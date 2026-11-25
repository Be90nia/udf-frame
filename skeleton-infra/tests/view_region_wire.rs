#![cfg(all(feature = "view", feature = "runtime"))]
//! 区域线格式 v1 与发布事务验收（#171 addendum / #222 §5.2 §B / #187 / #107 / #165 /
//! #186 / #154 / #153 / #141 / #142 / #172 / #168 / #185 / #188 / #211）。
//!
//! 三组断言：
//! ① **布局与版本闸**：Header 冻结偏移、序列 8 字节对齐、manifest/生成物对表、
//!    不支持的 major 在解释任何 v1 偏移之前被拒。
//! ② **读侧韧性**：odd 拒收、前后不等重试耗尽可见、结构损坏 typed、逐条目未知
//!    kind/tag 跳过 + 计数 + resync（禁 panic 禁整区失败）。
//! ③ **写侧事务**：确定性交错夹具（gate 条件变量握手，禁 sleep）证明写者在事务的
//!    任意停顿点都只让读者拿到「完整旧快照 / 重试耗尽 / 拒收」，绝不混合；编码器/
//!    背衬故障 contain 成 typed 失败且订阅存活；容量不足保持上一份已知良好区域。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use skeleton::view::region_layout::{
    ENTRY_LEN, ENTRY_OFFSET_BINDING_ID, ENTRY_OFFSET_KIND, ENTRY_OFFSET_LEN, ENTRY_OFFSET_OFFSET,
    HEADER_LEN, INDEX_OFFSET_V1, INITIAL_SEQ, MAGIC, OFFSET_BINDING_COUNT, OFFSET_FLAGS,
    OFFSET_INDEX_OFFSET, OFFSET_MAGIC, OFFSET_PROTOCOL, OFFSET_PUBLISH_SEQ, OFFSET_REGION_ID,
    SEG_ALIGN, SEQ_STEP, TAG_F64, TAG_NULL, TAG_U64, TLV_HEADER_LEN,
};
use skeleton::view::{BindingDecl, IntentMap, PROTOCOL_VERSION, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::read::{RegionRead, RegionReader, decode_batch};
use skeleton_infra::view::{
    BindingId, Coverage, Encoders, IntentError, JsonEncoder, RegionBacking, RegionEncoder,
    RegionError, RegionFailure, RegionWriter, SeqError, VecBacking, ViewBridge, WriterFault,
};

// ── 领域切片（单绑定：/n） ──

#[derive(Clone, Default)]
struct App {
    n: u32,
}

struct Red;

impl Reducer for Red {
    type State = App;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> App {
        App::default()
    }
    fn reduce(&self, s: &App, a: u32, _: &mut EffectBuf<u32, Infallible>) -> App {
        App { n: s.n + a }
    }
}

#[derive(Clone, PartialEq, Debug)]
enum V {
    N(u32),
}

struct Map;

const PATH: &str = "/n";
const KIND: u16 = 1;

impl ViewMap<App> for Map {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        &[BindingDecl {
            path: PATH,
            kind: PayloadTag(KIND),
        }]
    }
    fn project(&self, _: usize, s: &App) -> V {
        V::N(s.n)
    }
}

struct Json;

impl JsonEncoder for Json {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &V) -> Value {
        match v {
            V::N(n) => json!(n),
        }
    }
}

struct Intent;

impl IntentMap<u32> for Intent {
    type Wire = Value;
    type Err = IntentError;
    fn intent(&self, w: Value) -> Result<u32, IntentError> {
        w.as_u64()
            .map(|n| n as u32)
            .ok_or_else(|| IntentError::malformed("expect number"))
    }
}

/// 编码器行为开关（测试可在运行期翻面，验证「失败后恢复」）。
#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    U64,
    Empty,
    TwoTlv,
    PanicOnce,
    NonFinite,
    Bytes,
}

struct Enc {
    mode: Arc<Mutex<Mode>>,
}

fn enc(mode: Mode) -> (Enc, Arc<Mutex<Mode>>) {
    let cell = Arc::new(Mutex::new(mode));
    (
        Enc {
            mode: Arc::clone(&cell),
        },
        cell,
    )
}

fn set_mode(cell: &Mutex<Mode>, mode: Mode) {
    *cell.lock().expect("mode cell") = mode;
}

impl RegionEncoder for Enc {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &V, w: &mut RegionWriter) {
        let mode = *self.mode.lock().expect("mode cell");
        if mode == Mode::PanicOnce {
            set_mode(&self.mode, Mode::U64); // 下一次恢复正常（验证订阅存活）
            panic!("injected encoder panic");
        }
        let V::N(n) = v;
        match mode {
            Mode::U64 | Mode::PanicOnce => w.u64(u64::from(*n)),
            Mode::Empty => {}
            Mode::TwoTlv => {
                w.u64(u64::from(*n));
                w.u64(u64::from(*n));
            }
            Mode::NonFinite => w.f64(f64::NAN),
            Mode::Bytes => w.bytes(&vec![0u8; *n as usize]),
        }
    }
}

type Bridge = ViewBridge<App, u32, Map, Intent, Json, Enc>;

fn build(backing: Arc<dyn RegionBacking>, mode: Mode) -> (Arc<Bridge>, Arc<Mutex<Mode>>) {
    let store = SyncStore::new(Red, NoFx);
    let (encoder, cell) = enc(mode);
    let bridge = Arc::new(
        ViewBridge::mount(
            store,
            Map,
            Intent,
            Encoders {
                json: Some(Json),
                region: Some((encoder, backing)),
            },
        )
        .expect("static declarations are well-formed"),
    );
    (bridge, cell)
}

fn decls() -> Vec<BindingDecl> {
    Map.bindings().to_vec()
}

/// 不可变快照读（普通 load；entry 与 lock-free 入口分离）。
fn try_read(backing: &dyn RegionBacking, decls: &[BindingDecl]) -> Result<RegionRead, RegionError> {
    let mut slot = None;
    backing.with_bytes(&mut |bytes| {
        slot = Some(RegionReader::read_snapshot(bytes, decls));
    });
    slot.expect("with_bytes invoked exactly once")
}

fn read(backing: &dyn RegionBacking, decls: &[BindingDecl]) -> RegionRead {
    try_read(backing, decls).expect("valid committed region")
}

// ── ① 布局与版本闸 ──

#[test]
fn header_layout_is_frozen_and_artifacts_agree() {
    assert_eq!(HEADER_LEN, 32, "Header 32B 冻结");
    assert_eq!(ENTRY_LEN, 24, "Index 条目 24B 冻结");
    assert_eq!(
        OFFSET_PUBLISH_SEQ % 8,
        0,
        "序列必须 8 字节对齐（SAB 原子视图）"
    );
    assert_eq!(OFFSET_MAGIC, 0);
    assert_eq!(OFFSET_PROTOCOL, 4);
    assert_eq!(OFFSET_FLAGS, 6);
    assert_eq!(OFFSET_REGION_ID, 16);
    assert_eq!(OFFSET_BINDING_COUNT, 20);
    assert_eq!(OFFSET_INDEX_OFFSET, 24);
    assert_eq!(INDEX_OFFSET_V1 as usize, HEADER_LEN, "v1 索引紧随头");
    assert_eq!(INITIAL_SEQ % 2, 0, "已提交水位恒 even");
    assert_eq!(SEQ_STEP % 2, 0, "每次提交跨过 odd");

    // manifest 是唯一真源；生成物由 `view_constants_gen --check` 把闸，这里咬
    // Rust 常量 ↔ 清单 ↔ TS 生成物三处同值。
    let manifest = include_str!("../../docs/view-bridge-constants/manifest.toml");
    for (key, value) in [
        ("header_len", HEADER_LEN as u64),
        ("entry_len", ENTRY_LEN as u64),
        ("seg_align", SEG_ALIGN as u64),
        ("tlv_header_len", TLV_HEADER_LEN as u64),
        ("initial_seq", INITIAL_SEQ),
        ("seq_step", SEQ_STEP),
        ("offset_magic", OFFSET_MAGIC as u64),
        ("offset_protocol", OFFSET_PROTOCOL as u64),
        ("offset_flags", OFFSET_FLAGS as u64),
        ("offset_publish_seq", OFFSET_PUBLISH_SEQ as u64),
        ("offset_region_id", OFFSET_REGION_ID as u64),
        ("offset_binding_count", OFFSET_BINDING_COUNT as u64),
        ("offset_index_offset", OFFSET_INDEX_OFFSET as u64),
        ("index_offset_v1", INDEX_OFFSET_V1),
        ("entry_offset_binding_id", ENTRY_OFFSET_BINDING_ID as u64),
        ("entry_offset_kind", ENTRY_OFFSET_KIND as u64),
        ("entry_offset_offset", ENTRY_OFFSET_OFFSET as u64),
        ("entry_offset_len", ENTRY_OFFSET_LEN as u64),
    ] {
        assert!(
            manifest.contains(&format!("{key} = {value}")),
            "manifest drift: {key} = {value}"
        );
    }
    let ts = include_str!("../../docs/view-bridge-constants/view_bridge_constants.ts");
    assert!(
        ts.contains("export const OFFSET_PUBLISH_SEQ = 8 as const;"),
        "TS 生成物未与清单同步"
    );
}

// ── 夹具构造 ──

fn entry(id: u32, kind: u16, off: u64, len: u32) -> [u8; ENTRY_LEN] {
    let mut e = [0u8; ENTRY_LEN];
    e[ENTRY_OFFSET_BINDING_ID..ENTRY_OFFSET_KIND].copy_from_slice(&id.to_le_bytes());
    e[ENTRY_OFFSET_KIND..ENTRY_OFFSET_KIND + 2].copy_from_slice(&kind.to_le_bytes());
    e[ENTRY_OFFSET_OFFSET..ENTRY_OFFSET_LEN].copy_from_slice(&off.to_le_bytes());
    e[ENTRY_OFFSET_LEN..ENTRY_OFFSET_LEN + 4].copy_from_slice(&len.to_le_bytes());
    e
}

/// 原始布局（offset/len 由调用方给，含敌意值）。
fn region_raw(
    sequence: u64,
    region_id: u32,
    meta: &[(u32, u16, u64, u32)],
    arena: &[u8],
) -> Vec<u8> {
    let mut bytes = vec![0u8; HEADER_LEN + ENTRY_LEN * meta.len()];
    bytes[OFFSET_MAGIC..OFFSET_PROTOCOL].copy_from_slice(&MAGIC);
    bytes[OFFSET_PROTOCOL..OFFSET_PROTOCOL + 2].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    bytes[OFFSET_PUBLISH_SEQ..OFFSET_REGION_ID].copy_from_slice(&sequence.to_le_bytes());
    bytes[OFFSET_REGION_ID..OFFSET_BINDING_COUNT].copy_from_slice(&region_id.to_le_bytes());
    bytes[OFFSET_BINDING_COUNT..OFFSET_INDEX_OFFSET]
        .copy_from_slice(&(meta.len() as u32).to_le_bytes());
    bytes[OFFSET_INDEX_OFFSET..HEADER_LEN].copy_from_slice(&INDEX_OFFSET_V1.to_le_bytes());
    for (i, (id, kind, off, len)) in meta.iter().enumerate() {
        let at = HEADER_LEN + ENTRY_LEN * i;
        bytes[at..at + ENTRY_LEN].copy_from_slice(&entry(*id, *kind, *off, *len));
    }
    bytes.extend_from_slice(arena);
    bytes
}

/// 自动布局（段 8 字节对齐、紧随索引区）。
fn region_auto(sequence: u64, segs: &[(u32, u16, Vec<u8>)]) -> Vec<u8> {
    let index_end = HEADER_LEN + ENTRY_LEN * segs.len();
    let mut arena: Vec<u8> = Vec::new();
    let mut meta = Vec::new();
    for (id, kind, seg) in segs {
        let pad = (SEG_ALIGN - (arena.len() % SEG_ALIGN)) % SEG_ALIGN;
        arena.resize(arena.len() + pad, 0);
        let off = index_end + arena.len();
        arena.extend_from_slice(seg);
        meta.push((*id, *kind, off as u64, seg.len() as u32));
    }
    region_raw(sequence, 1, &meta, &arena)
}

fn tlv(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn unsupported_major_is_rejected_before_v1_offsets() {
    // v1 偏移处全是垃圾：若被当作 v1 解释，binding_count 会读出 0xFFFF_FFFF。
    let mut bytes = vec![0xFFu8; 64];
    bytes[OFFSET_MAGIC..OFFSET_PROTOCOL].copy_from_slice(&MAGIC);
    bytes[OFFSET_PROTOCOL..OFFSET_PROTOCOL + 2].copy_from_slice(&2u16.to_le_bytes());
    let err = RegionReader::read_snapshot(&bytes, &decls()).expect_err("major 2 unsupported");
    assert!(
        matches!(err, RegionError::UnsupportedVersion { major: 2 }),
        "{err}"
    );
}

#[test]
fn snapshot_reader_rejects_odd_and_flags() {
    let bytes = region_auto(3, &[]); // odd = 写事务进行中
    let err = RegionReader::read_snapshot(&bytes, &decls()).expect_err("odd must be rejected");
    assert!(
        matches!(err, RegionError::WriterInProgress { sequence: 3 }),
        "{err}"
    );

    let mut bytes = region_auto(INITIAL_SEQ, &[]);
    bytes[OFFSET_FLAGS..OFFSET_PUBLISH_SEQ].copy_from_slice(&1u16.to_le_bytes());
    let err = RegionReader::read_snapshot(&bytes, &decls()).expect_err("nonzero flags");
    assert!(matches!(err, RegionError::Malformed { .. }), "{err}");
}

#[test]
fn reader_rejects_structural_corruption_without_panicking() {
    let decls = decls();
    // 头不足
    assert!(matches!(
        RegionReader::read_snapshot(&[0u8; 8], &decls).expect_err("short"),
        RegionError::Truncated { .. }
    ));
    // magic 不符
    let mut bytes = region_auto(INITIAL_SEQ, &[]);
    bytes[0] = b'X';
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("magic"),
        RegionError::BadMagic
    ));
    // index_offset != 32
    let mut bytes = region_auto(INITIAL_SEQ, &[]);
    bytes[OFFSET_INDEX_OFFSET..HEADER_LEN].copy_from_slice(&40u64.to_le_bytes());
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("index_offset"),
        RegionError::Malformed { .. }
    ));
    // 索引进界（binding_count 巨大）
    let mut bytes = region_auto(INITIAL_SEQ, &[]);
    bytes[OFFSET_BINDING_COUNT..OFFSET_INDEX_OFFSET].copy_from_slice(&0xFFFFu32.to_le_bytes());
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("count"),
        RegionError::Truncated { .. }
    ));
    // 重复 BindingId
    let seg = tlv(TAG_U64, &7u64.to_le_bytes());
    let mut bytes = region_auto(
        INITIAL_SEQ,
        &[(0, KIND, seg.clone()), (0, KIND, seg.clone())],
    );
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("dup id"),
        RegionError::Malformed { .. }
    ));
    // 未对齐段偏移
    let seg = tlv(TAG_U64, &7u64.to_le_bytes());
    let off = (HEADER_LEN + ENTRY_LEN) as u64 + 1;
    bytes = region_raw(
        INITIAL_SEQ,
        1,
        &[(0, KIND, off, seg.len() as u32)],
        &[0u8; 32],
    );
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("unaligned"),
        RegionError::Malformed { .. }
    ));
    // 段落进 Header/Index
    let mut arena = vec![0u8; 16];
    arena[..seg.len()].copy_from_slice(&seg);
    bytes = region_raw(INITIAL_SEQ, 1, &[(0, KIND, 0, seg.len() as u32)], &arena);
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("overlaps header"),
        RegionError::Malformed { .. }
    ));
    // end 溢出（8 字节对齐的 usize::MAX-7）
    let huge = (usize::MAX - 7) as u64;
    assert_eq!(huge as usize % SEG_ALIGN, 0);
    bytes = region_raw(INITIAL_SEQ, 1, &[(0, KIND, huge, 16)], &[]);
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("overflow"),
        RegionError::Malformed { .. }
    ));
    // 段越界（尾部截断）
    let seg = tlv(TAG_U64, &7u64.to_le_bytes());
    let mut bytes = region_auto(INITIAL_SEQ, &[(0, KIND, seg)]);
    bytes.truncate(bytes.len() - 1);
    assert!(matches!(
        RegionReader::read_snapshot(&bytes, &decls).expect_err("truncated payload"),
        RegionError::Truncated { .. }
    ));
}

#[test]
fn reader_skips_unknown_kind_and_malformed_segments() {
    let good = tlv(TAG_U64, &42u64.to_le_bytes());
    let unknown_tag = tlv(0x7F, b"junk");
    let mut trailing = tlv(TAG_U64, &1u64.to_le_bytes());
    trailing.extend_from_slice(&[0, 0]); // 段内残留 1-4 字节 ⇒ 整段失效
    let non_finite = tlv(TAG_F64, &f64::NAN.to_le_bytes());
    let bad_null = tlv(TAG_NULL, &[1, 2, 3]);
    let unframed = vec![TAG_U64, 8, 0, 0]; // TLV 头截断

    let bytes = region_auto(
        INITIAL_SEQ,
        &[
            (0, KIND, good),
            (1, 0x7777, tlv(TAG_U64, &1u64.to_le_bytes())), // 未知 kind
            (2, KIND, unknown_tag),
            (3, KIND, trailing),
            (4, KIND, non_finite),
            (5, KIND, bad_null),
            (6, KIND, unframed),
            (7, 0, tlv(TAG_U64, &9u64.to_le_bytes())), // kind 0 = 未声明（自描述）
        ],
    );
    let read = RegionReader::read_snapshot(&bytes, &decls()).expect("结构合法（逐条目降级）");
    assert_eq!(
        read.values,
        vec![(BindingId(0), json!(42)), (BindingId(7), json!(9))],
        "独立有效条目必须保留"
    );
    assert_eq!(
        read.unknown_tags_skipped, 6,
        "未知 kind + 5 条畸形段（各整段失效）"
    );
    assert!(read.needs_resync, "降级必须显形");
    assert_eq!(read.sequence, INITIAL_SEQ);
    assert_eq!(read.region_id, 1);
}

// ── ②b 判定权：原子序列 vs header 字节（SAB 撕裂读） ──

/// 共享区形态：字节镜像与原子序列**可故意不一致**——模拟 SAB 上 header `8..16`
/// 的 8 字节普通读撕裂（低半旧/高半新）或陈旧镜像。判定权必须只在原子序列。
struct SplitBacking {
    bytes: Mutex<Vec<u8>>,
    seq: AtomicU64,
}

impl SplitBacking {
    fn new(bytes: Vec<u8>, seq: u64) -> Self {
        Self {
            bytes: Mutex::new(bytes),
            seq: AtomicU64::new(seq),
        }
    }
    fn bytes_copy(&self) -> Vec<u8> {
        self.bytes.lock().expect("split backing").clone()
    }
}

impl RegionBacking for SplitBacking {
    fn len(&self) -> usize {
        self.bytes.lock().expect("split backing").len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        write(&mut self.bytes.lock().expect("split backing"))
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        read(&self.bytes.lock().expect("split backing"))
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        self.seq.store(seq, Ordering::Release);
        Ok(())
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        Ok(self.seq.load(Ordering::Acquire))
    }
}

#[test]
fn lock_free_reader_takes_sequence_from_atomic_not_header_bytes() {
    let decls = decls();
    // 字节镜像 = 撕裂/陈旧残留（序列 odd，内容已是新值）；原子值 = 稳定 even。
    // 若拿字节当判据，这条稳定的已提交区域会被误判成「写者在途」。
    let bytes = region_auto(3, &[(0, KIND, tlv(TAG_U64, &42u64.to_le_bytes()))]);
    let backing = SplitBacking::new(bytes, INITIAL_SEQ);
    let read = RegionReader::read_lock_free(&backing, &decls, 4)
        .expect("一致性判定权在原子序列：不得因字节 odd 拒收");
    assert_eq!(read.sequence, INITIAL_SEQ, "上报的是原子值，不是字节值");
    assert_eq!(read.values, vec![(BindingId(0), json!(42))]);
    assert!(!read.needs_resync);

    // 变异证据：把判定改回读字节 8..16 ⇒ 上面那条必红（此处显形为 odd 拒收）。
    let snapshot = backing.bytes_copy();
    assert!(
        matches!(
            RegionReader::read_snapshot(&snapshot, &decls),
            Err(RegionError::WriterInProgress { sequence: 3 })
        ),
        "不可变快照入口仍以字节为真相（无并发写者）"
    );
}

#[test]
fn lock_free_reader_reports_writer_in_progress_from_odd_atomic() {
    let decls = decls();
    // 字节镜像 even（看起来已提交）；原子值 odd（写者真在途）⇒ 必须拒收。
    let bytes = region_auto(INITIAL_SEQ, &[(0, KIND, tlv(TAG_U64, &7u64.to_le_bytes()))]);
    let backing = SplitBacking::new(bytes, INITIAL_SEQ + 1);
    let err = RegionReader::read_lock_free(&backing, &decls, 3).expect_err("原子值 odd ⇒ 不得接受");
    assert!(
        matches!(err, RegionError::WriterInProgress { sequence } if sequence == INITIAL_SEQ + 1),
        "{err}"
    );
}

/// 每次 load 返回**不同的 even** 值（持续提交的写者）：前后永不相等 ⇒ 重试耗尽
/// （与「原子值一直是 odd」的 writer-in-progress 区分开）。
struct ChurnBacking {
    bytes: Mutex<Vec<u8>>,
    loads: AtomicU64,
}

impl ChurnBacking {
    fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes: Mutex::new(bytes),
            loads: AtomicU64::new(0),
        }
    }
}

impl RegionBacking for ChurnBacking {
    fn len(&self) -> usize {
        self.bytes.lock().expect("churn backing").len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        write(&mut self.bytes.lock().expect("churn backing"))
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        read(&self.bytes.lock().expect("churn backing"))
    }
    fn store_publish_seq(&self, _seq: u64) -> Result<(), SeqError> {
        Ok(())
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        // 2, 4, 6, …：恒 even 但每次都在变 ⇒ 没有任何一次读是稳定提交态。
        Ok(INITIAL_SEQ + SEQ_STEP * self.loads.fetch_add(1, Ordering::Acquire))
    }
}

#[test]
fn lock_free_reader_reports_retry_exhaustion_under_churn() {
    let decls = decls();
    let bytes = region_auto(INITIAL_SEQ, &[(0, KIND, tlv(TAG_U64, &7u64.to_le_bytes()))]);
    let backing = ChurnBacking::new(bytes);
    let err = RegionReader::read_lock_free(&backing, &decls, 4).expect_err("序列持续变化");
    assert!(
        matches!(err, RegionError::RetryExhausted { attempts: 4 }),
        "持续提交 ⇒ 重试耗尽（不是 writer-in-progress）: {err}"
    );
}

// ── ③ 写侧事务：确定性交错夹具 ──

#[derive(Clone, Copy, PartialEq, Debug)]
enum Pause {
    /// odd 之前：读者应拿到完整旧快照。
    BeforeOdd,
    /// odd 之后、任何字节之前。
    AfterOdd,
    /// 段字节已写、索引/头未写（多字段撕裂窗口）。
    AfterPayload,
    /// 全部字节已写、even 未提交。
    BeforeEven,
}

#[derive(Default)]
struct GateState {
    armed: Option<Pause>,
    hit: bool,
    released: bool,
}

/// 写者暂停夹具：条件变量握手（禁 sleep；有界等待把挂死变成红）。
struct Gate {
    state: Mutex<GateState>,
    cv: Condvar,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(GateState::default()),
            cv: Condvar::new(),
        })
    }
    fn arm(&self, at: Pause) {
        let mut s = self.state.lock().expect("gate");
        s.armed = Some(at);
        s.hit = false;
        s.released = false;
    }
    /// 写者侧：命中即阻塞到释放（只一次，释放后自动解除武装）。
    fn hit(&self, at: Pause) {
        let mut s = self.state.lock().expect("gate");
        if s.armed != Some(at) || s.released {
            return;
        }
        s.hit = true;
        self.cv.notify_all();
        while !s.released {
            s = self.cv.wait(s).expect("gate wait");
        }
        s.armed = None;
    }
    fn wait_hit(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut s = self.state.lock().expect("gate");
        while !s.hit {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "writer never reached the pause point");
            let (guard, _) = self.cv.wait_timeout(s, left).expect("gate wait");
            s = guard;
        }
    }
    fn release(&self) {
        let mut s = self.state.lock().expect("gate");
        s.released = true;
        self.cv.notify_all();
    }
}

/// 真实字节背衬 + 可在事务四个点暂停的包装（语义与 `VecBacking` 一致）。
struct Gated {
    inner: VecBacking,
    gate: Arc<Gate>,
}

impl Gated {
    fn new(capacity: usize, gate: Arc<Gate>) -> Arc<Self> {
        Arc::new(Self {
            inner: VecBacking::new(capacity),
            gate,
        })
    }
}

impl RegionBacking for Gated {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        self.inner.with_bytes_mut(write);
        self.gate.hit(Pause::AfterPayload);
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read)
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        if seq % 2 == 1 {
            self.gate.hit(Pause::BeforeOdd);
            self.inner.store_publish_seq(seq)?;
            self.gate.hit(Pause::AfterOdd);
            Ok(())
        } else {
            self.gate.hit(Pause::BeforeEven);
            self.inner.store_publish_seq(seq)
        }
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        self.inner.load_publish_seq()
    }
}

#[test]
fn seqlock_interleavings_never_accept_mixed_snapshots() {
    let decls = decls();
    for pause in [
        Pause::BeforeOdd,
        Pause::AfterOdd,
        Pause::AfterPayload,
        Pause::BeforeEven,
    ] {
        let gate = Gate::new();
        let backing = Gated::new(4096, Arc::clone(&gate));
        let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
        bridge.bind(PATH).unwrap();
        bridge.seal().expect("seal publishes the initial region");
        assert!(bridge.send_intent(json!(1)).unwrap(), "首发布");
        let before = try_read(backing.as_ref(), &decls).expect("stable before");
        assert_eq!(before.values, vec![(BindingId(0), json!(1))], "{pause:?}");
        assert_eq!(before.sequence, bridge.region_version(), "{pause:?}");

        gate.arm(pause);
        let writer = {
            let bridge = Arc::clone(&bridge);
            std::thread::spawn(move || bridge.send_intent(json!(2)).unwrap())
        };
        gate.wait_hit();

        if pause == Pause::BeforeOdd {
            // 事务尚未开始：旧快照完整可读（普通 load 与原子入口一致）。
            let snap = try_read(backing.as_ref(), &decls).expect("old snapshot accepted");
            assert_eq!(snap.values, vec![(BindingId(0), json!(1))]);
            assert_eq!(snap.sequence, bridge.region_version());
            let lf = RegionReader::read_lock_free(backing.as_ref(), &decls, 4)
                .expect("old snapshot accepted (atomic entry)");
            assert_eq!(lf.values, snap.values);
        } else {
            // 事务进行中：不可变快照入口读字节判 odd；原子入口只认原子值，同样
            // 拒收但按「写者在途」显形（耗尽时最后观测仍是 odd）。
            let err = try_read(backing.as_ref(), &decls).expect_err("odd must be rejected");
            assert!(
                matches!(err, RegionError::WriterInProgress { sequence } if sequence % 2 == 1),
                "{pause:?}: {err}"
            );
            let err = RegionReader::read_lock_free(backing.as_ref(), &decls, 4)
                .expect_err("writer busy must be visible");
            assert!(
                matches!(err, RegionError::WriterInProgress { sequence } if sequence % 2 == 1),
                "{pause:?}: {err}"
            );
        }

        gate.release();
        writer.join().unwrap();
        // 释放后：完整新快照（绝不混合）。
        let after = try_read(backing.as_ref(), &decls).expect("stable after");
        assert_eq!(after.values, vec![(BindingId(0), json!(3))], "{pause:?}");
        assert_eq!(after.sequence % 2, 0, "{pause:?}: 只有 even 被接受");
        assert_eq!(after.sequence, bridge.region_version(), "{pause:?}");
        assert_eq!(bridge.region_failure(), None, "{pause:?}");
    }
}

#[test]
fn arena_exhaustion_rolls_over_region_id_and_keeps_watermark() {
    // arena = 32B：封板初始段 16B + 一拍 16B 占满，第三段换区（版本延续）。
    let backing = Arc::new(VecBacking::new(HEADER_LEN + ENTRY_LEN + 32));
    let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
    bridge.bind(PATH).unwrap();
    bridge.seal().expect("seal publishes the initial region");
    for _ in 0..2 {
        assert!(bridge.send_intent(json!(1)).unwrap());
    }
    assert!(bridge.region_id() > 1, "arena 耗尽 ⇒ 换区握手");
    assert_eq!(bridge.region_id(), 2);
    let batch = bridge.take_patches();
    assert_eq!(batch.to_version, INITIAL_SEQ + 2 * SEQ_STEP, "水位单调延续");
    assert_eq!(bridge.region_version(), batch.to_version, "换区后仍同源");
    let read = read(backing.as_ref(), &decls());
    assert_eq!(read.region_id, bridge.region_id());
    assert_eq!(
        read.values,
        vec![(BindingId(0), json!(2))],
        "重映射后可读现值"
    );
    assert_eq!(bridge.region_failure(), None);
}

// ── 编码器 / 背衬故障边界 ──

#[test]
fn encoder_faults_are_typed_and_recoverable() {
    for (mode, fault) in [
        (Mode::Empty, WriterFault::TruncatedTlv),
        (Mode::TwoTlv, WriterFault::NotSingleTlv),
        (Mode::NonFinite, WriterFault::NonFiniteF64),
    ] {
        let backing = Arc::new(VecBacking::new(4096));
        let (bridge, cell) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
        bridge.bind(PATH).unwrap();
        bridge.seal().expect("seal publishes the initial region");
        // 封板事务成功；本条变更编码失败 ⇒ 水位不推进、已提交 region 保持有效。
        set_mode(&cell, mode);
        assert!(bridge.send_intent(json!(1)).unwrap());
        assert_eq!(bridge.region_version(), INITIAL_SEQ, "{mode:?}");
        assert_eq!(
            bridge.region_failure(),
            Some(RegionFailure::EncoderOutput { path: PATH, fault }),
            "{mode:?}"
        );
        assert_eq!(read(backing.as_ref(), &decls()).sequence, INITIAL_SEQ);

        // 恢复：编码器翻面后下一拍即成功发布，失败槽清空。
        set_mode(&cell, Mode::U64);
        assert!(bridge.send_intent(json!(1)).unwrap());
        let batch = bridge.take_patches();
        assert_eq!(
            bridge.region_version(),
            batch.to_version,
            "{mode:?}: 区域头必须等于桥水位"
        );
        assert!(bridge.region_version() > INITIAL_SEQ, "{mode:?}");
        assert_eq!(bridge.region_failure(), None, "{mode:?}");
        assert_eq!(
            read(backing.as_ref(), &decls()).values,
            vec![(BindingId(0), json!(2))],
            "{mode:?}"
        );
    }
}

#[test]
fn encoder_panic_is_contained_and_subscription_survives() {
    let backing = Arc::new(VecBacking::new(4096));
    let (bridge, cell) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
    bridge.bind(PATH).unwrap();
    bridge.seal().expect("seal publishes the initial region");

    set_mode(&cell, Mode::PanicOnce);
    assert!(bridge.send_intent(json!(1)).unwrap()); // 编码器 panic（首变更）
    assert_eq!(
        bridge.region_failure(),
        Some(RegionFailure::EncoderPanicked { path: PATH })
    );
    assert_eq!(bridge.region_version(), INITIAL_SEQ, "panic 不得推进水位");

    // 订阅存活：下一拍正常投影 + 发布。
    let runs = bridge.projection_runs();
    assert!(bridge.send_intent(json!(1)).unwrap());
    assert!(bridge.projection_runs() > runs, "订阅未被 SyncStore 摘除");
    let batch = bridge.take_patches();
    assert_eq!(
        bridge.region_version(),
        batch.to_version,
        "区域头 == 桥水位"
    );
    assert!(bridge.region_version() > INITIAL_SEQ);
    assert_eq!(bridge.region_failure(), None);
    assert_eq!(
        read(backing.as_ref(), &decls()).values,
        vec![(BindingId(0), json!(2))]
    );
}

/// 只实现字节读写（无原子序列能力）：lock-free 模式必须在装配期被拒绝。
struct CopyOnly {
    inner: VecBacking,
}

impl RegionBacking for CopyOnly {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        self.inner.with_bytes_mut(write)
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read)
    }
}

#[test]
fn backing_without_atomic_sequence_rejects_lock_free_mode() {
    let backing = Arc::new(CopyOnly {
        inner: VecBacking::new(4096),
    });
    let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
    bridge.bind(PATH).unwrap();
    assert_eq!(
        bridge.region_failure(),
        Some(RegionFailure::BackingUnsupported),
        "装配期即拒绝 lock-free 区域模式"
    );
    // 封板：区域初始事务失败必须显形（JSON 通道不受影响）。
    assert!(matches!(
        bridge.seal(),
        Err(skeleton_infra::view::SealError::Region(
            RegionFailure::BackingUnsupported
        ))
    ));
    // JSON 通道照常；区域永不对外宣称水位。
    assert!(bridge.send_intent(json!(1)).unwrap());
    let batch = bridge.take_patches();
    assert_eq!(batch.patches[0].value, json!(1));
    assert_eq!(bridge.region_version(), 0);
    assert_eq!(bridge.region_id(), 0);
}

#[test]
fn backing_too_small_for_header_and_index_fails_visibly() {
    let backing = Arc::new(VecBacking::new(16));
    let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
    bridge.bind(PATH).unwrap();
    assert!(matches!(
        bridge.seal(),
        Err(skeleton_infra::view::SealError::Region(
            RegionFailure::HeaderIndexTooLarge {
                required: _,
                capacity: 16
            }
        ))
    ));
    assert!(bridge.send_intent(json!(1)).unwrap());
    assert_eq!(
        bridge.region_failure(),
        Some(RegionFailure::HeaderIndexTooLarge {
            required: HEADER_LEN + ENTRY_LEN,
            capacity: 16,
        })
    );
    assert_eq!(bridge.region_version(), 0, "seal 失败不得宣称任何水位");
    // JSON 通道照常（区域失败不得拖垮补丁交付）。
    assert_eq!(bridge.take_patches().patches[0].value, json!(1));
}

#[test]
fn capacity_exhaustion_keeps_last_known_good_region() {
    // 32 头 + 24 索引 + 16 arena：封板初始段 8B + 10 字节载荷（5+10=15 → 填充
    // 16）各占一段刚好放下，12 字节载荷（17 → 24）连全量重编也放不下。
    let backing = Arc::new(VecBacking::new(HEADER_LEN + ENTRY_LEN + 16));
    let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::Bytes);
    bridge.bind(PATH).unwrap();
    bridge.seal().expect("seal publishes the initial region");
    assert!(bridge.send_intent(json!(10)).unwrap());
    let ok = read(backing.as_ref(), &decls());
    assert_eq!(ok.sequence, INITIAL_SEQ + SEQ_STEP);
    assert_eq!(ok.values.len(), 1);
    let ok_region_id = bridge.region_id();

    assert!(bridge.send_intent(json!(2)).unwrap()); // 12 字节载荷
    match bridge.region_failure() {
        Some(RegionFailure::CapacityExceeded {
            required,
            capacity,
            region_id,
        }) => {
            assert_eq!(required, HEADER_LEN + ENTRY_LEN + 24);
            assert_eq!(capacity, HEADER_LEN + ENTRY_LEN + 16);
            assert_eq!(region_id, ok_region_id, "换区失败：region_id 不得前进");
        }
        other => panic!("expect CapacityExceeded, got {other:?}"),
    }
    assert_eq!(bridge.region_version(), ok.sequence, "失败不得推进水位");
    let kept = read(backing.as_ref(), &decls());
    assert_eq!(
        kept.values, ok.values,
        "上一份已知良好区域保持可读（不是半成品）"
    );
    assert_eq!(kept.sequence, ok.sequence);

    // 恢复口：容量真的不够 ⇒ 重编也失败且仍显形（不静默）。
    assert!(matches!(
        bridge.reseed_region(),
        Err(RegionFailure::CapacityExceeded { .. })
    ));
}

/// 首次事务中途 panic 的背衬（之后恢复正常，验证恢复口）。
struct PanicBacking {
    inner: VecBacking,
    armed: AtomicBool,
}

impl PanicBacking {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            inner: VecBacking::new(capacity),
            armed: AtomicBool::new(false),
        })
    }
}

impl RegionBacking for PanicBacking {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        if self.armed.swap(false, Ordering::SeqCst) {
            panic!("injected backing panic");
        }
        self.inner.with_bytes_mut(write)
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read)
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        self.inner.store_publish_seq(seq)
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        self.inner.load_publish_seq()
    }
}

#[test]
fn backing_panic_marks_unavailable_until_explicit_reseed() {
    let backing = PanicBacking::new(4096);
    let (bridge, _) = build(Arc::clone(&backing) as Arc<dyn RegionBacking>, Mode::U64);
    bridge.bind(PATH).unwrap();
    bridge.seal().expect("seal publishes the initial region");
    assert!(bridge.send_intent(json!(1)).unwrap());
    let committed = bridge.region_version();
    assert_eq!(committed, INITIAL_SEQ + SEQ_STEP);

    backing.armed.store(true, Ordering::SeqCst);
    assert!(bridge.send_intent(json!(1)).unwrap()); // 事务中 panic
    assert_eq!(
        bridge.region_failure(),
        Some(RegionFailure::BackingPanicked {
            op: "with_bytes_mut"
        })
    );
    assert_eq!(bridge.region_version(), committed, "失败不得宣称新水位");
    // 订阅存活：JSON 照常交付（含本拍变更）。
    let batch = bridge.take_patches();
    assert_eq!(batch.patches[0].value, json!(2));
    assert_eq!(batch.to_version, committed + SEQ_STEP);

    // 通道已 unavailable：再拍不发布（也不 panic）。
    assert!(bridge.send_intent(json!(1)).unwrap());
    assert_eq!(bridge.region_version(), committed);

    // 显式全量重编 = 唯一恢复口。
    assert_eq!(bridge.reseed_region(), Ok(()));
    assert_eq!(bridge.region_failure(), None);
    assert!(bridge.region_version() > batch.to_version);
    let healed = read(backing.as_ref(), &decls());
    assert_eq!(healed.values, vec![(BindingId(0), json!(3))]);
    assert_eq!(healed.sequence, bridge.region_version());
}

#[test]
fn decode_batch_enforces_version_and_kind_vocabulary() {
    let decls = decls();
    // 不支持的 major：不解释任何条目。
    let read = decode_batch(
        &json!({"protocol_version": 9, "patches": [{"path": PATH, "kind": KIND, "value": 1}]}),
        &decls,
    );
    assert!(read.unsupported_version);
    assert!(read.needs_resync);
    assert!(read.patches.is_empty(), "未知信封不得硬解为 v1 条目");
    // 版本缺失：同上（不猜）。
    let read = decode_batch(&json!({"patches": []}), &decls);
    assert!(read.unsupported_version);

    // 畸形/越界 kind 与 kind 漂移都逐项跳过（禁 unwrap_or(0) 静默当未声明）。
    let read = decode_batch(
        &json!({"protocol_version": PROTOCOL_VERSION, "patches": [
            {"path": PATH, "kind": "1", "value": 1},
            {"path": PATH, "kind": 70000, "value": 1},
            {"path": PATH, "kind": 2, "value": 1},
            {"path": PATH, "kind": 1, "value": 7}
        ]}),
        &decls,
    );
    assert_eq!(read.patches.len(), 1);
    assert_eq!(read.patches[0].value, json!(7));
    assert_eq!(read.unknown_tags_skipped, 3);
    assert!(read.needs_resync);
    assert!(!read.unsupported_version);
}

/// #135/#166：缺失 / 负数 / 浮点 / 越界别名（`65536 ⇒ 0`）的 kind 一律**逐项跳过**
/// 并置 resync；显式 `kind: 0`（未声明形状）仍是合法词表值。
#[test]
fn decode_batch_rejects_missing_negative_float_and_aliased_kind() {
    let decls = decls();
    let read = decode_batch(
        &json!({"protocol_version": PROTOCOL_VERSION, "patches": [
            {"path": PATH, "value": 1},                  // kind 缺失
            {"path": PATH, "kind": -1, "value": 1},      // 负数
            {"path": PATH, "kind": 1.5, "value": 1},     // 浮点
            {"path": PATH, "kind": 65536, "value": 1},   // 旧实现 `as u16` 会截成 0（别名）
            {"path": PATH, "kind": 0, "value": 3}        // 显式 0 = 未声明形状，合法
        ]}),
        &decls,
    );
    assert_eq!(
        read.patches.len(),
        1,
        "只有显式 kind 0 通过：{:?}",
        read.patches
    );
    assert_eq!(read.patches[0].value, json!(3));
    assert_eq!(read.unknown_tags_skipped, 4);
    assert!(read.needs_resync, "降级必须显形");
    assert!(!read.unsupported_version);
}

// ── 初始区域全量发布（#155 / #190）：封板即可用，且未变的绑定也一定有值 ──────

/// 双绑定：`/n`（随状态变）+ `/fixed`（恒定 7，后续拍永不进 diff 集）。
struct TwoMap;

static TWO_DECLS: [BindingDecl; 2] = [
    BindingDecl {
        path: "/n",
        kind: PayloadTag(KIND),
    },
    BindingDecl {
        path: "/fixed",
        kind: PayloadTag(2),
    },
];

impl ViewMap<App> for TwoMap {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        &TWO_DECLS
    }
    fn project(&self, index: usize, s: &App) -> V {
        V::N(if index == 0 { s.n } else { 7 })
    }
}

struct TwoEnc;

impl RegionEncoder for TwoEnc {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &V, w: &mut RegionWriter) {
        let V::N(n) = v;
        w.u64(u64::from(*n));
    }
}

/// Region-only：`seal()` 必须发布全部已登记绑定的现值——Store 之后保持空闲也
/// 不会留下空区域（#155）；且只改一个绑定时，未变的绑定照样可读（#190）。
#[test]
fn region_only_seal_publishes_every_registered_binding() {
    let backing = Arc::new(VecBacking::new(4096));
    let bridge: ViewBridge<App, u32, TwoMap, Intent, Json, TwoEnc> = ViewBridge::mount(
        SyncStore::new(Red, NoFx),
        TwoMap,
        Intent,
        Encoders {
            json: None,
            region: Some((TwoEnc, Arc::clone(&backing) as Arc<dyn RegionBacking>)),
        },
    )
    .expect("valid declaration table");
    bridge.bind("/n").unwrap();
    bridge.bind("/fixed").unwrap();
    bridge.seal().expect("Region-only seal must publish values");

    let sealed = read(backing.as_ref(), &TWO_DECLS);
    assert_eq!(sealed.sequence, INITIAL_SEQ, "封板即首个已提交 even 水位");
    assert_eq!(
        sealed.values,
        vec![(BindingId(0), json!(0)), (BindingId(1), json!(7))],
        "全量初始发布：UI 无需等后续变更就能取到基线"
    );

    assert!(bridge.send_intent(json!(3)).unwrap());
    let read = read(backing.as_ref(), &TWO_DECLS);
    assert_eq!(read.region_id, bridge.region_id());
    assert_eq!(read.sequence, INITIAL_SEQ + SEQ_STEP);
    assert_eq!(
        read.values,
        vec![(BindingId(0), json!(3)), (BindingId(1), json!(7))],
        "未变的绑定不得退回 len=0（旧实现永不发布它）"
    );
    assert_eq!(bridge.region_version(), read.sequence);
}

/// 双通道：`snapshot_doc()` 之后只改一个绑定 ⇒ 区域里两条绑定都有值（#190 回归）。
#[test]
fn dual_channel_snapshot_seeds_every_covered_region_entry() {
    let backing = Arc::new(VecBacking::new(4096));
    let bridge: ViewBridge<App, u32, TwoMap, Intent, Json, TwoEnc> = ViewBridge::mount(
        SyncStore::new(Red, NoFx),
        TwoMap,
        Intent,
        Encoders {
            json: Some(Json),
            region: Some((TwoEnc, Arc::clone(&backing) as Arc<dyn RegionBacking>)),
        },
    )
    .expect("valid declaration table");
    bridge.bind("/n").unwrap();
    bridge.bind("/fixed").unwrap();
    bridge.seal().unwrap();

    let snap = bridge.snapshot_doc().expect("sealed committed snapshot");
    assert_eq!(snap.version, INITIAL_SEQ);
    assert_eq!(snap.doc.pointer("/n"), Some(&json!(0)));
    assert_eq!(snap.doc.pointer("/fixed"), Some(&json!(7)));

    assert!(bridge.send_intent(json!(1)).unwrap());
    let read = read(backing.as_ref(), &TWO_DECLS);
    assert_eq!(
        read.values,
        vec![(BindingId(0), json!(1)), (BindingId(1), json!(7))],
        "未变的 /fixed 仍带封板初值"
    );
    assert!(!read.needs_resync);
}
