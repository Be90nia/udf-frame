//! 语言无关视图数据面参考机器（ADR 0006；数据走向/线程模型/版本语义/毒化闭环
//! 的完整语义承诺见契约层 `skeleton::view` 与 `docs/view-region-spec.md`）。
//!
//! `ViewBridge` 以**纯订阅者**挂 `Store` 通知面（核心零改动）。线程模型：通知体
//! 在 dispatch 线程同步执行，pull 侧跨线程 ⇒ 桥内态单把粗锁成组迁移（last/脏位/
//! 毒化位一次动齐），投影在锁外，哨兵胞锁为外层短叶锁（先取先放零嵌套），绑定表
//! 首通知封板后只读。版本语义（同步档 = MVP 唯一范围）：出站版本 = 桥内单调消费
//! 代数，区域头与 JSON 批 `to_version` 同源；`delivered_epoch` 是交付游标——
//! `take_patches` 与 `snapshot_doc` 同为交付点（快照重同步后下批 `from_version ==
//! snapshot.version`）。装配纪律：mount → 全部 bind → 才允许 dispatch。
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;
use skeleton::view::{IntentMap, PayloadTag, ViewMap, PROTOCOL_VERSION};
use skeleton::{ActionSender, Store, Subscription};

pub mod read;

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（fx/pool 同纪律）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("view bridge lock poisoned: prior panic while holding the lock")
}

// ── 错误词表（手写 Display/Error，journal.rs 先例；#[non_exhaustive] M6 纪律）──

/// `#[non_exhaustive]`：下游 match 须带通配臂——未来加变体非破坏演进。
#[derive(Debug)]
#[non_exhaustive]
pub enum BindError {
    /// 未知路径（不在 `ViewMap::bindings()` 词表内，装配期 fail-visible）。
    UnknownPath { path: String },
    /// 首通知已封板：绑定表只读，运行期动态注册属二期断点。
    Sealed,
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPath { path } => write!(f, "unknown binding path: {path}"),
            Self::Sealed => write!(f, "binding table sealed after first notification"),
        }
    }
}

impl std::error::Error for BindError {}

/// 意图翻译失败词表（`IntentMap::Err` 的参考形态；机器以本类型统一出站错误面）。
#[derive(Debug)]
#[non_exhaustive]
pub enum IntentError {
    /// 未知意图（不在采用方翻译词表内）。
    Unknown { detail: String },
    /// 载荷畸形（缺字段/类型不符）。
    Malformed { detail: String },
}

impl IntentError {
    pub fn unknown(detail: impl Into<String>) -> Self {
        Self::Unknown {
            detail: detail.into(),
        }
    }
    pub fn malformed(detail: impl Into<String>) -> Self {
        Self::Malformed {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for IntentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { detail } => write!(f, "unknown intent: {detail}"),
            Self::Malformed { detail } => write!(f, "malformed intent payload: {detail}"),
        }
    }
}

impl std::error::Error for IntentError {}

/// 区域通道错误（协议外字节；「未知标签」兼容路径不走此处，见 `read` 模块）。
#[derive(Debug)]
#[non_exhaustive]
pub enum RegionError {
    /// 头 magic 非 "UVBR"：不是本协议区域字节。
    BadMagic,
    /// 字节不足（头/索引/段截断）。
    Truncated { need: usize, got: usize },
}

impl fmt::Display for RegionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => write!(f, "region magic mismatch (expect \"UVBR\")"),
            Self::Truncated { need, got } => write!(f, "region truncated: need {need}, got {got}"),
        }
    }
}

impl std::error::Error for RegionError {}

// ── 出站词汇 ──

/// 绑定句柄：`bind` 返回的内部索引号（区域索引条目 `binding_id` 同值）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BindingId(pub u32);

/// 单补丁（RFC 6902 `replace` 子集语义：按路径整值替换）。
#[derive(Debug)]
pub struct Patch {
    /// 声明静态串零拷贝（行级实例路径二期需换形，规格 §12-1 断点）。
    pub path: &'static str,
    /// 绑定级载荷 schema 标签（`0` = 未声明）。
    pub kind: PayloadTag,
    /// replace 值（交付点由 `JsonEncoder::to_json` 产出）。
    pub value: Value,
}

/// 出站批：latest-wins 折叠后的脏路径现值 + 完整性事实（`resync`）+ 水位。
/// `from_version` = 上一次交付的 `to_version`（UI 侧 `applied != from_version`
/// ⇒ 缺段 ⇒ 走 `snapshot_doc` 重同步）；`to_version` 与区域头同源同值。
#[derive(Debug)]
pub struct PatchBatch {
    pub patches: Vec<Patch>,
    /// 流完整性可疑（唯一因：投影 panic 毒化）⇒ UI 必须 `snapshot_doc` 重同步。
    pub resync: bool,
    pub from_version: u64,
    pub to_version: u64,
    /// 信封协议版本；未知版本读侧逐项按 kind 跳过，禁整批失败。
    pub protocol_version: u16,
}

/// 重同步快照：文档 + 同源版本（UI 据此重建 `applied` 基线）。
#[derive(Debug)]
pub struct Snapshot {
    pub version: u64,
    /// 全量文档（按绑定路径建树，RFC 6901 可寻址）。
    pub doc: Value,
}

// ── 编码器组件矩阵（可组合：只开一种 / 都开，采用方自选）──

/// 覆盖语义。`#[non_exhaustive]`：未来加变体非破坏演进。
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Coverage {
    /// 全部已绑定项。
    All,
    /// 路径清单（装配期对账，未知路径 = 装配面配置洞，debug_assert + 计数）。
    Only(&'static [&'static str]),
}

impl Coverage {
    fn covers(self, path: &str) -> bool {
        match self {
            Self::All => true,
            Self::Only(paths) => paths.contains(&path),
        }
    }
}

/// lazy JSON 通道：`take_patches`/`snapshot_doc` 交付时翻译。
pub trait JsonEncoder {
    type View;
    fn covers(&self) -> Coverage;
    /// 编码红线：穷尽 match 变体 + 全字段覆盖（漏字段 = UI 静默陈旧，闸咬不住，
    /// 只有每变体 round-trip 断言能咬）。
    fn to_json(&self, view: &Self::View) -> Value;
}

/// eager 区域通道：通知路径变更即编（共享区至最近一次消费点为止的最新；
/// 新鲜度判据 = 区域头 `version`）。
pub trait RegionEncoder {
    type View;
    fn covers(&self) -> Coverage;
    /// 展平写入（短批纪律：一次 encode 一段 TLV，见 [`RegionWriter`]）。
    fn encode(&self, view: &Self::View, w: &mut RegionWriter);
}

/// 编码矩阵注册表（mount 装配）：两槽独立开关，采用方自选组合。
pub struct Encoders<J, R> {
    pub json: Option<J>,
    /// `(编码器, 区域背衬)`——背衬固定容量、地址稳定（禁原地增长）。
    pub region: Option<(R, Arc<dyn RegionBacking>)>,
}

// ── 机制胞（L1 输入键等值记忆化，两处应用同一机制同一「input 覆盖实读」红线）──

/// 派生记忆化胞（ViewMap 内持字段使用）：`input(state)` 等值于缓存 ⇒ 返回缓存
/// （零重算）。
pub struct Derived<S, In, Out> {
    cell: Mutex<Option<(In, Out)>>,
    input: Box<dyn Fn(&S) -> In + Send + Sync>,
    compute: Box<dyn Fn(&In) -> Out + Send + Sync>,
}

impl<S, In: PartialEq, Out: Clone> Derived<S, In, Out> {
    pub fn new(
        input: impl Fn(&S) -> In + Send + Sync + 'static,
        compute: impl Fn(&In) -> Out + Send + Sync + 'static,
    ) -> Self {
        Self {
            cell: Mutex::new(None),
            input: Box::new(input),
            compute: Box::new(compute),
        }
    }
    /// 命中返回缓存；未命中重算并换缓存。
    pub fn get(&self, state: &S) -> Out {
        let key = (self.input)(state);
        let mut cell = lock(&self.cell);
        if let Some((cached_in, cached_out)) = cell.as_ref() {
            if *cached_in == key {
                return cached_out.clone();
            }
        }
        let out = (self.compute)(&key);
        *cell = Some((key, out.clone()));
        out
    }
}

/// 绑定级输入哨兵：[`Derived`] 的退化形（只记输入不记输出）。胞锁恒为桥 core
/// 锁的外层短叶锁（先取先放，零嵌套）。In 抽取纪律：零分配或近零；抽不廉价的
/// 绑定不设哨兵（缺省 `true` 形态），禁哈希指纹（碰撞 = 漏报 = 静默陈旧）。
pub struct Sentinel<S, In> {
    cell: Mutex<Option<In>>,
    extract: Box<dyn Fn(&S) -> In + Send + Sync>,
}

impl<S, In: PartialEq> Sentinel<S, In> {
    pub fn new(extract: impl Fn(&S) -> In + Send + Sync + 'static) -> Self {
        Self {
            cell: Mutex::new(None),
            extract: Box::new(extract),
        }
    }
    /// 抽取 + 等值；变更 ⇒ 换缓存返 `true`。
    pub fn changed(&self, state: &S) -> bool {
        let key = (self.extract)(state);
        let mut cell = lock(&self.cell);
        if cell.as_ref() == Some(&key) {
            return false;
        }
        *cell = Some(key);
        true
    }
}

// ── 区域线格式件（规范唯一权威 = docs/view-region-spec.md v1）──

/// TLV 段写入器：`tag:u8 | len:u32(LE) | payload`——长度前缀使未知 tag 可跳过
/// （协议兼容期不整批失败的机制前提）。桥内 scratch 累计，批内一次进背衬。
#[derive(Default)]
pub struct RegionWriter {
    out: Vec<u8>,
}

impl RegionWriter {
    pub fn new() -> Self {
        Self::default()
    }
    /// 清空复用（批内多段共享一个 writer，省分配）。
    pub fn clear(&mut self) {
        self.out.clear();
    }
    /// 累计字节数（含已写各段；段边界由调用方以 `len()` 差值记录）。
    pub fn len(&self) -> usize {
        self.out.len()
    }
    /// 空判（`len` 配对纪律）。
    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }
    fn tlv(&mut self, tag: u8, payload: &[u8]) {
        self.out.push(tag);
        self.out
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        self.out.extend_from_slice(payload);
    }
    /// tag 0。
    pub fn u64(&mut self, v: u64) {
        self.tlv(0, &v.to_le_bytes());
    }
    /// tag 1。
    pub fn i64(&mut self, v: i64) {
        self.tlv(1, &v.to_le_bytes());
    }
    /// tag 2（IEEE754 LE）。
    pub fn f64(&mut self, v: f64) {
        self.tlv(2, &v.to_le_bytes());
    }
    /// tag 3（len:u32 + UTF-8）。
    pub fn utf8(&mut self, s: &str) {
        self.tlv(3, s.as_bytes());
    }
    /// tag 4（len:u32 + raw）。
    pub fn bytes(&mut self, b: &[u8]) {
        self.tlv(4, b);
    }
    /// tag 5（len:u32 + 紧凑 JSON UTF-8）。复杂嵌套一律 json_blob——与
    /// `JsonEncoder::to_json` 同源取值（两编码语义一致性的机制保证）。
    pub fn json_blob(&mut self, v: &Value) {
        self.tlv(5, v.to_string().as_bytes());
    }
}

/// 区域背衬：固定容量、地址稳定的共享内存抽象——框架经回调安全写入，零 unsafe
/// 零裸指针留在框架侧（宿主实现自持 SAB/shm/Vec）。固定容量是地址稳定的硬前提。
pub trait RegionBacking: Send + Sync + 'static {
    fn len(&self) -> usize;
    /// 空判（`len` 配对纪律；固定容量背衬恒 `false`）。
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// 框架唯一写入口：把 `&mut [u8]` 借给回调（宿主实现内自理锁/Atomic）。
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8]));
    /// 框架读侧（镜像读取器/宿主拷贝出口）。
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8]));
}

/// 默认背衬：固定容量 Vec（拷贝出站姿态；Mutex 互斥下重置无撕裂窗口）。
pub struct VecBacking {
    buf: Mutex<Vec<u8>>,
}

impl VecBacking {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Mutex::new(vec![0u8; capacity]),
        }
    }
}

impl RegionBacking for VecBacking {
    fn len(&self) -> usize {
        self.buf.lock().expect("vec backing poisoned").len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        write(&mut self.buf.lock().expect("vec backing poisoned"));
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        read(&self.buf.lock().expect("vec backing poisoned"));
    }
}

// ── 机器 ──

const HEADER_LEN: usize = 32;
const ENTRY_LEN: usize = 24;

fn pad8(n: usize) -> usize {
    (n + 7) & !7
}

/// 封板后的绑定表条目（只读）。`index` = `bindings()` 序（ViewMap 调用序）；
/// 表位置 = 绑定注册序（last/脏位/区域条目寻址序）。
struct Bound {
    index: usize,
    id: BindingId,
    path: &'static str,
    kind: PayloadTag,
}

/// 桥内折叠态（单锁成组：last/脏位/毒化/版本/交付游标）。
struct Core<V> {
    bound: Option<Arc<[Bound]>>, // 首通知封板
    registered: Vec<usize>,      // 已登记 bindings() 序（重复 bind 幂等）
    last: Vec<Option<V>>,
    dirty: Vec<bool>,
    poisoned: bool,
    version: u64,
    delivered_epoch: u64,
}

/// 区域索引条目（背衬 Index 区的内存镜像）。
struct RegionEntry {
    binding_id: u32,
    kind: u16,
    offset: usize,
    len: u32,
    gen: u32,
}

/// 区域通道（eager）：背衬布局 = Header 32B + Index 24×n B + Arena。单写者 =
/// 排空环；Arena 耗尽 ⇒ 原地重置换区（region_id+1 + 版本延续 + 全量重编；默认
/// VecBacking 形互斥下无撕裂窗口；宿主 SAB 形的「新背衬」换法随宿主背衬管理，
/// 禁原地**增长**不变）。
struct RegionChannel<R> {
    encoder: Arc<R>,
    backing: Arc<dyn RegionBacking>,
    region_id: u32,
    version: u64,
    arena_start: usize,
    cursor: usize,
    entries: Vec<RegionEntry>,
    writer: RegionWriter,
}

/// 段落位计划（计算阶段产出，单回调内落盘）。
struct SegPlan {
    entry: usize,
    arena_off: usize,
    scratch_at: usize,
    len: usize,
}

impl<R: RegionEncoder> RegionChannel<R> {
    fn new(encoder: Arc<R>, backing: Arc<dyn RegionBacking>) -> Self {
        let ch = Self {
            encoder,
            backing,
            region_id: 1,
            version: 0,
            arena_start: HEADER_LEN, // 封板后按条目数重算
            cursor: HEADER_LEN,
            entries: Vec::new(),
            writer: RegionWriter::new(),
        };
        ch.write_header(0, 0);
        ch
    }

    /// 首通知封板：条目数定死、索引区清零、arena 游标就位。
    fn seal(&mut self, bound: &[Bound]) {
        self.entries = bound
            .iter()
            .map(|b| RegionEntry {
                binding_id: b.id.0,
                kind: b.kind.0,
                offset: 0,
                len: 0,
                gen: 0,
            })
            .collect();
        self.arena_start = HEADER_LEN + ENTRY_LEN * bound.len();
        self.cursor = self.arena_start;
        self.write_header(bound.len(), 0);
    }

    fn write_header(&self, binding_count: usize, version: u64) {
        // index_offset = 索引区起始（紧随头的固定布局，规范冻结）。
        let header = header_bytes(self.region_id, version, binding_count, HEADER_LEN);
        self.backing.with_bytes_mut(&mut |buf| {
            if buf.len() >= HEADER_LEN {
                buf[0..HEADER_LEN].copy_from_slice(&header);
            }
        });
    }

    /// eager 发布：段展平 → 落位计算 → 单回调写「段 → 索引 → 头（发布点，最后
    /// 写）」。`items` 已按编码器覆盖过滤；空批也推进头 version（跨通道同源）。
    /// 返回 `false` = arena 不足，调用方走换区。
    fn publish(&mut self, items: &[(usize, &R::View)], version: u64, counter: &AtomicU64) -> bool {
        self.writer.clear();
        let mut raw: Vec<(usize, usize, usize)> = Vec::with_capacity(items.len()); // (entry, scratch_at, len)
        for (entry, view) in items {
            let at = self.writer.len();
            Arc::clone(&self.encoder).encode(view, &mut self.writer);
            raw.push((*entry, at, self.writer.len() - at));
        }
        let need: usize = raw.iter().map(|(_, _, l)| pad8(*l)).sum();
        if self.cursor + need > self.backing.len() {
            return false;
        }
        let mut plans: Vec<SegPlan> = Vec::with_capacity(raw.len());
        let mut cur = self.cursor;
        for (entry, at, len) in &raw {
            let e = &mut self.entries[*entry];
            e.offset = cur;
            e.len = *len as u32;
            e.gen = e.gen.wrapping_add(1);
            plans.push(SegPlan {
                entry: *entry,
                arena_off: cur,
                scratch_at: *at,
                len: *len,
            });
            cur += pad8(*len);
        }
        self.cursor = cur;
        self.version = version;
        counter.fetch_add(raw.len() as u64, Ordering::Relaxed);
        let payload = &self.writer.out[..];
        let entries: Vec<(usize, [u8; ENTRY_LEN])> = plans
            .iter()
            .map(|p| (p.entry, entry_bytes(&self.entries[p.entry])))
            .collect();
        let header = header_bytes(self.region_id, version, self.entries.len(), HEADER_LEN);
        self.backing.with_bytes_mut(&mut |buf| {
            if buf.len() < self.arena_start {
                return;
            }
            for p in &plans {
                let end = p.arena_off + pad8(p.len);
                buf[p.arena_off..end]
                    .copy_from_slice(&zero_pad(&payload[p.scratch_at..p.scratch_at + p.len]));
            }
            for (ei, bytes) in &entries {
                let at = HEADER_LEN + ENTRY_LEN * ei;
                buf[at..at + ENTRY_LEN].copy_from_slice(bytes);
            }
            buf[0..HEADER_LEN].copy_from_slice(&header);
        });
        true
    }
}

fn header_bytes(
    region_id: u32,
    version: u64,
    binding_count: usize,
    index_offset: usize,
) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[0..4].copy_from_slice(b"UVBR");
    h[4..6].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    h[6..10].copy_from_slice(&region_id.to_le_bytes());
    h[10..18].copy_from_slice(&version.to_le_bytes());
    h[18..22].copy_from_slice(&(binding_count as u32).to_le_bytes());
    h[22..30].copy_from_slice(&(index_offset as u64).to_le_bytes());
    h
}

fn entry_bytes(e: &RegionEntry) -> [u8; ENTRY_LEN] {
    let mut b = [0u8; ENTRY_LEN];
    b[0..4].copy_from_slice(&e.binding_id.to_le_bytes());
    b[4..6].copy_from_slice(&e.kind.to_le_bytes());
    b[8..16].copy_from_slice(&(e.offset as u64).to_le_bytes());
    b[16..20].copy_from_slice(&e.len.to_le_bytes());
    b[20..24].copy_from_slice(&e.gen.to_le_bytes());
    b
}

fn zero_pad(seg: &[u8]) -> Vec<u8> {
    let mut v = seg.to_vec();
    v.resize(pad8(seg.len()), 0);
    v
}

/// 通知体共享态（订阅闭包捕获 `Arc<Self>`；不含 store/票据 ⇒ 无环）。
struct Notifier<S, M: ViewMap<S>, J: JsonEncoder, R: RegionEncoder> {
    map: Arc<M>,
    json: Option<Arc<J>>,
    region: Mutex<Option<RegionChannel<R>>>,
    core: Mutex<Core<M::View>>,
    projection_runs: AtomicU64,
    projection_panics: AtomicU64,
    region_encodes: AtomicU64,
    region_ver: AtomicU64,
    region_id_cur: AtomicU32,
    assembly_gaps: AtomicU64,
    _s: std::marker::PhantomData<fn(&S)>,
}

impl<S, M, J, R> Notifier<S, M, J, R>
where
    M: ViewMap<S> + Send + Sync + 'static,
    J: JsonEncoder<View = M::View> + Send + Sync + 'static,
    R: RegionEncoder<View = M::View> + Send + Sync + 'static,
{
    /// 绑定表封板快照（首通知时从 `map.bindings()` 封板，此后只读）。
    fn bound_of(&self, core: &mut Core<M::View>) -> Arc<[Bound]> {
        if let Some(b) = &core.bound {
            return Arc::clone(b);
        }
        let bound: Arc<[Bound]> = self
            .map
            .bindings()
            .iter()
            .enumerate()
            .map(|(index, d)| Bound {
                index,
                id: BindingId(index as u32),
                path: d.path,
                kind: d.kind,
            })
            .collect();
        core.last = vec![None; bound.len()];
        core.dirty = vec![false; bound.len()];
        if let Some(ch) = lock(&self.region).as_mut() {
            ch.seal(&bound);
            self.region_ver.store(ch.version, Ordering::Release);
            self.region_id_cur.store(ch.region_id, Ordering::Release);
        }
        core.bound = Some(Arc::clone(&bound));
        bound
    }

    fn on_notify(&self, state: &S) {
        let bound = {
            let mut core = lock(&self.core);
            self.bound_of(&mut core)
        };
        // 锁外：哨兵粗闸（跳投影）→ 单点投影（panic catch：三态提交唯一缺口）。
        let mut pending: Vec<(usize, M::View)> = Vec::new(); // (绑定注册序, view)
        let mut panicked = false;
        for (bi, b) in bound.iter().enumerate() {
            if !self.map.input_changed(b.index, state) {
                continue;
            }
            self.projection_runs.fetch_add(1, Ordering::Relaxed);
            match catch_unwind(AssertUnwindSafe(|| self.map.project(b.index, state))) {
                Ok(v) => pending.push((bi, v)),
                Err(_) => {
                    self.projection_panics.fetch_add(1, Ordering::Relaxed);
                    panicked = true;
                }
            }
        }
        // 锁内折叠：类型 eq 细闸（跳补丁）→ 移槽置脏（latest-wins）+ 版本推进 + 毒化。
        let version;
        let mut changed: Vec<(usize, M::View)> = Vec::new();
        {
            let mut core = lock(&self.core);
            core.version += 1;
            version = core.version;
            for (bi, v) in pending {
                if core.last[bi].as_ref() != Some(&v) {
                    changed.push((bi, v.clone()));
                    core.last[bi] = Some(v);
                    core.dirty[bi] = true;
                }
            }
            if panicked {
                core.poisoned = true;
            }
        }
        // eager 区域编码（锁外；单写者 = 排空环）。空批也推进头 version（同源）。
        let mut rg = lock(&self.region);
        if let Some(ch) = rg.as_mut() {
            let covers = ch.encoder.covers();
            let refs: Vec<(usize, &M::View)> = changed
                .iter()
                .filter(|(bi, _)| covers.covers(bound[*bi].path))
                .map(|(bi, v)| (*bi, v))
                .collect();
            if !ch.publish(&refs, version, &self.region_encodes) {
                // 换区：region_id+1 + 版本延续 + 全量重编（互斥下无撕裂窗口）。
                let full: Vec<(usize, M::View)> = {
                    let core = lock(&self.core);
                    bound
                        .iter()
                        .enumerate()
                        .filter(|(bi, _)| covers.covers(bound[*bi].path))
                        .filter_map(|(bi, _)| core.last[bi].as_ref().map(|v| (bi, v.clone())))
                        .collect()
                };
                ch.region_id = ch.region_id.wrapping_add(1);
                ch.cursor = ch.arena_start;
                for e in &mut ch.entries {
                    e.offset = 0;
                    e.len = 0;
                    e.gen = 0;
                }
                let refs: Vec<(usize, &M::View)> = full.iter().map(|(bi, v)| (*bi, v)).collect();
                let ok = ch.publish(&refs, version, &self.region_encodes);
                debug_assert!(
                    ok,
                    "region arena overflow after reseed: backing capacity too small"
                );
            }
            self.region_ver.store(ch.version, Ordering::Release);
            self.region_id_cur.store(ch.region_id, Ordering::Release);
        }
    }
}

/// 现态源（私有擦除面；`snapshot_doc` 冷路径专用——热路径投影吃 `&S` 借用，
/// 桥不持有 State 快照，不制造快照持有者深克隆成本）。
trait SnapshotSource<S>: Send + Sync {
    fn snapshot(&self) -> Arc<S>;
}

impl<S, T: Store<State = S> + Send + Sync> SnapshotSource<S> for T {
    fn snapshot(&self) -> Arc<S> {
        Store::snapshot(self)
    }
}

/// 桥内部件：通知态 + 现态源（store）+ 上行句柄 + 意图翻译。
struct Inner<S, A, M, I, J, R>
where
    M: ViewMap<S>,
    J: JsonEncoder,
    R: RegionEncoder,
{
    notify: Arc<Notifier<S, M, J, R>>,
    store: Arc<dyn SnapshotSource<S>>,
    sender: ActionSender<A>,
    intents: Arc<I>,
}

/// 语言边界视图桥（ADR 0006 参考机器）。装配纪律：**mount → 全部 `bind` 完成
/// 后才允许 dispatch**（首通知封板绑定表）。装配后句柄可跨线程共享（Send +
/// Sync）；票据归桥，桥 Drop = 注销订阅；store 所有权随桥（`snapshot_doc` 现态
/// 源），桥 Drop ⇒ store 随桥回收（上行句柄经 `send_intent` 透传闭门 false）。
pub struct ViewBridge<S, A, M, I, J, R>
where
    M: ViewMap<S>,
    J: JsonEncoder,
    R: RegionEncoder,
{
    inner: Arc<Inner<S, A, M, I, J, R>>,
    // 持有即订阅（Drop = 注销）；从不读取，仅供析构语义。
    _ticket: Subscription<S>,
}

impl<S, A, M, I, J, R> ViewBridge<S, A, M, I, J, R>
where
    S: Send + 'static,
    A: Send + 'static,
    M: ViewMap<S> + Send + Sync + 'static,
    I: IntentMap<A, Wire = Value, Err = IntentError> + Send + Sync + 'static,
    J: JsonEncoder<View = M::View> + Send + Sync + 'static,
    R: RegionEncoder<View = M::View> + Send + Sync + 'static,
{
    /// 装配：store.subscribe 挂桥（票据归桥所有，桥 Drop = 注销）。store 以
    /// 所有权移入（`snapshot_doc` 现态源）；投影/翻译/编码器由桥与通知闭包共享。
    pub fn mount<T>(store: T, map: M, intents: I, encoders: Encoders<J, R>) -> Self
    where
        T: Store<State = S, Action = A> + Send + Sync + 'static,
    {
        let region = encoders
            .region
            .map(|(enc, backing)| RegionChannel::new(Arc::new(enc), backing));
        let notify = Arc::new(Notifier {
            map: Arc::new(map),
            json: encoders.json.map(Arc::new),
            region: Mutex::new(region),
            core: Mutex::new(Core {
                bound: None,
                registered: Vec::new(),
                last: Vec::new(),
                dirty: Vec::new(),
                poisoned: false,
                version: 0,
                delivered_epoch: 0,
            }),
            projection_runs: AtomicU64::new(0),
            projection_panics: AtomicU64::new(0),
            region_encodes: AtomicU64::new(0),
            region_ver: AtomicU64::new(0),
            region_id_cur: AtomicU32::new(0),
            assembly_gaps: AtomicU64::new(0),
            _s: std::marker::PhantomData,
        });
        let cb = Arc::clone(&notify);
        let ticket = store.subscribe(move |state: &S| cb.on_notify(state));
        let sender = store.sender();
        let inner = Arc::new(Inner {
            notify,
            store: Arc::new(store),
            sender,
            intents: Arc::new(intents),
        });
        Self {
            inner,
            _ticket: ticket,
        }
    }

    /// 绑定注册（**装配期专用**）：路径校验（未知路径 Err）→ 内部索引句柄；首
    /// 通知封板后显式 `Err(BindError::Sealed)`；重复 bind 同路径返回既有句柄。
    pub fn bind(&self, path: &str) -> Result<BindingId, BindError> {
        let notify = &self.inner.notify;
        let mut core = lock(&notify.core);
        if core.bound.is_some() {
            return Err(BindError::Sealed);
        }
        let decls = notify.map.bindings();
        let index =
            decls
                .iter()
                .position(|d| d.path == path)
                .ok_or_else(|| BindError::UnknownPath {
                    path: path.to_string(),
                })?;
        if let Some(&seen) = core.registered.iter().find(|&&r| r == index) {
            return Ok(BindingId(seen as u32)); // 幂等
        }
        // 装配面配置洞：绑定项不被任何已挂编码器覆盖 ⇒ debug_assert + 计数留痕。
        let json_cover = notify.json.as_ref().is_some_and(|j| j.covers().covers(path));
        let region_cover =
            lock(&notify.region).as_ref().is_some_and(|ch| ch.encoder.covers().covers(path));
        if !json_cover && !region_cover {
            notify.assembly_gaps.fetch_add(1, Ordering::Relaxed);
            debug_assert!(
                false,
                "assembly gap: bound path {path} covered by no encoder"
            );
        }
        let id = BindingId(core.registered.len() as u32);
        core.registered.push(index);
        Ok(id)
    }

    /// 语言边界入站面：意图翻译 + dispatch（任意线程）。`Err` = 未知意图/载荷
    /// 畸形；`Ok(false)` = store 已闭门（must_use 语义透传，停机显式化不折损）。
    pub fn send_intent(&self, intent: Value) -> Result<bool, IntentError> {
        let action = self.inner.intents.intent(intent)?;
        Ok(self.inner.sender.dispatch(action))
    }

    /// 入站文本重载（宿主拿到的是 wire 文本时）：RFC 8259 严格解析 + 64 MiB
    /// 上限 + 行列错误（`std_ext::json` 纪律白得），解析失败 = 载荷畸形。
    pub fn send_intent_str(&self, wire: &str) -> Result<bool, IntentError> {
        let value =
            std_ext::json::decode_str(wire, std_ext::json::JsonMode::Strict).map_err(|e| {
                IntentError::malformed(format!("{}:{}: {}", e.line, e.column, e.message))
            })?;
        self.send_intent(value)
    }

    /// 出站批拉取（`&self`：宿主任意线程可调）。翻译发生在这里（冷路径）：脏
    /// 路径逐个 `to_json` → Value 补丁，取 last 现值（latest-wins），清脏位。
    pub fn take_patches(&self) -> PatchBatch {
        let notify = &self.inner.notify;
        let mut core = lock(&notify.core);
        let bound = match &core.bound {
            Some(b) => Arc::clone(b),
            None => return empty_batch(0, false),
        };
        let resync = core.poisoned;
        let from = core.delivered_epoch;
        core.delivered_epoch = core.version;
        let mut patches = Vec::new();
        if let Some(j) = &notify.json {
            let covers = j.covers();
            for (bi, b) in bound.iter().enumerate() {
                if !core.dirty[bi] {
                    continue;
                }
                if covers.covers(b.path) {
                    if let Some(v) = &core.last[bi] {
                        patches.push(Patch {
                            path: b.path,
                            kind: b.kind,
                            value: j.to_json(v),
                        });
                    }
                }
                core.dirty[bi] = false;
            }
        } else {
            core.dirty.iter_mut().for_each(|d| *d = false);
        }
        PatchBatch {
            patches,
            resync,
            from_version: from,
            to_version: core.version,
            protocol_version: PROTOCOL_VERSION,
        }
    }

    /// 重同步原语：**现态全量重投影**（不信缓存、哨兵旁路，O(view) 一次性）+
    /// 锁内一次完成「换 last + 清脏 + 复位毒化」。快照即交付：`delivered_epoch`
    /// 推进至当前消费代数 ⇒ 重同步后下一批 `from_version == snapshot.version`。
    pub fn snapshot_doc(&self) -> Snapshot {
        let notify = &self.inner.notify;
        let bound = {
            let mut core = lock(&notify.core);
            notify.bound_of(&mut core)
        };
        let state = self.inner.store.snapshot(); // Arc<S> 用完即弃，不驻留
        let mut views: Vec<M::View> = Vec::with_capacity(bound.len());
        let mut panicked = false;
        for b in bound.iter() {
            match catch_unwind(AssertUnwindSafe(|| notify.map.project(b.index, &state))) {
                Ok(v) => views.push(v),
                Err(_) => {
                    notify.projection_panics.fetch_add(1, Ordering::Relaxed);
                    panicked = true;
                }
            }
        }
        let doc;
        let version;
        {
            let mut core = lock(&notify.core);
            version = core.version;
            for (bi, v) in views.iter().enumerate() {
                core.last[bi] = Some(v.clone());
            }
            core.dirty.iter_mut().for_each(|d| *d = false);
            if !panicked {
                core.poisoned = false; // 成组复位（重同步以现态为准，哨兵旁路）
            }
            core.delivered_epoch = version; // 快照即交付（补审 N2）
            doc = match &notify.json {
                Some(j) => assemble_doc(&bound, &views, j.as_ref()),
                None => Value::Object(serde_json::Map::new()),
            };
        }
        Snapshot { version, doc }
    }

    /// 区域头版本（未挂区域通道 = 恒 0；读侧原子镜像，同源判据）。
    pub fn region_version(&self) -> u64 {
        self.inner.notify.region_ver.load(Ordering::Acquire)
    }

    /// 换区握手：id 变化 = UI 重映射。
    pub fn region_id(&self) -> u32 {
        self.inner.notify.region_id_cur.load(Ordering::Acquire)
    }

    /// eager 编码累计（成本可见；空批推进头 version 不计入）。
    pub fn region_encodes(&self) -> u64 {
        self.inner.notify.region_encodes.load(Ordering::Relaxed)
    }

    /// 单点投影累计次数（哨兵命中 ⇒ 不增）。
    pub fn projection_runs(&self) -> u64 {
        self.inner.notify.projection_runs.load(Ordering::Relaxed)
    }

    /// 投影 panic 跳过累计（对齐 subscriber_panics 先例：让降级显形）。
    pub fn projection_panics(&self) -> u64 {
        self.inner.notify.projection_panics.load(Ordering::Relaxed)
    }
}

fn empty_batch(version: u64, resync: bool) -> PatchBatch {
    PatchBatch {
        patches: Vec::new(),
        resync,
        from_version: version,
        to_version: version,
        protocol_version: PROTOCOL_VERSION,
    }
}

/// 全量文档组装：按绑定路径建树（RFC 6901 段直插；`~0`/`~1` 转义段名列二期）。
fn assemble_doc<J: JsonEncoder>(bound: &[Bound], views: &[J::View], json: &J) -> Value {
    let covers = json.covers();
    let mut root = Value::Object(serde_json::Map::new());
    for (bi, b) in bound.iter().enumerate() {
        if !covers.covers(b.path) {
            continue;
        }
        let leaf = json.to_json(&views[bi]);
        let segs: Vec<&str> = b.path.split('/').skip(1).collect();
        let Some((last, parents)) = segs.split_last() else {
            debug_assert!(false, "binding path must be absolute (leading '/')");
            continue;
        };
        let mut node = &mut root;
        let mut ok = true;
        for seg in parents {
            if !node.is_object() {
                debug_assert!(false, "binding path collides with non-object at {seg}");
                ok = false;
                break;
            }
            node = node
                .as_object_mut()
                .expect("checked is_object")
                .entry(seg.to_string())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));
        }
        if ok {
            node = node
                .as_object_mut()
                .expect("leaf parent is object")
                .entry(last.to_string())
                .or_insert(Value::Null);
            *node = leaf; // replace 语义
        }
    }
    root
}
