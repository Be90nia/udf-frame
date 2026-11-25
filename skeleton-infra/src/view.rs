//! 语言无关视图数据面参考机器（ADR 0006；数据走向/线程模型/版本语义/毒化闭环
//! 的完整语义承诺见契约层 `skeleton::view` 与 `docs/view-region-spec.md`）。
//!
//! `ViewBridge` 以**纯订阅者**挂 `Store` 通知面（核心零改动）。线程模型：通知体
//! 在 dispatch 线程同步执行，pull 侧跨线程 ⇒ 桥内态单把粗锁成组迁移（last/脏位/
//! 毒化位一次动齐），投影在锁外，哨兵胞锁为外层短叶锁（先取先放零嵌套），绑定表
//! 首通知封板后只读。版本语义（同步档 = MVP 唯一范围）：出站版本 = 桥内已提交
//! **even 序列**（区域头 publish-sequence 与 JSON 批 `to_version` 同源同值；
//! odd 只表示区域写事务进行中，永不作为已提交版本暴露）；`delivered_epoch` 是
//! 交付游标——`take_patches` 与 `snapshot_doc` 同为交付点（快照重同步后下批
//! `from_version == snapshot.version`）。装配纪律：mount → 全部 bind → 才允许
//! dispatch。
//!
//! 回调边界纪律（#140 区域侧 / #188 / #211）：`RegionEncoder::{covers,encode}` 与
//! `RegionBacking::{len,with_bytes,with_bytes_mut,store/load_publish_seq}` 均为
//! 采用方代码——**任何桥内互斥量下都不调用**；panic 在桥边界 contain 成 typed
//! 失败 + 计数，Store 订阅保持存活。
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;
use skeleton::view::region_layout::{
    ENTRY_LEN, ENTRY_OFFSET_BINDING_ID, ENTRY_OFFSET_GEN, ENTRY_OFFSET_KIND, ENTRY_OFFSET_LEN,
    ENTRY_OFFSET_OFFSET, HEADER_LEN, INDEX_OFFSET_V1, INITIAL_SEQ, MAGIC, OFFSET_BINDING_COUNT,
    OFFSET_FLAGS, OFFSET_INDEX_OFFSET, OFFSET_MAGIC, OFFSET_PROTOCOL, OFFSET_PUBLISH_SEQ,
    OFFSET_REGION_ID, SEG_ALIGN, SEQ_STEP, TAG_BYTES, TAG_F64, TAG_I64, TAG_JSON, TAG_NULL,
    TAG_U64, TAG_UTF8, TLV_HEADER_LEN,
};
use skeleton::view::{IntentMap, PROTOCOL_VERSION, PayloadTag, ViewMap};
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

/// 区域读侧错误（**协议外字节/结构损坏**；「兼容期未知标签」不走此处，见
/// `read` 模块——那是逐项跳过 + 计数 + resync，禁整区失败）。
#[derive(Debug)]
#[non_exhaustive]
pub enum RegionError {
    /// 头 magic 非 "UVBR"：不是本协议区域字节。
    BadMagic,
    /// `protocol_version` 非本机支持的 major：**未解释任何 v1 偏移/计数/条目**
    /// （#187）。这是协商/重同步边界，不是可跳过的未知标签。
    UnsupportedVersion { major: u16 },
    /// 字节不足（头/索引/段截断）。
    Truncated { need: usize, got: usize },
    /// 结构性损坏：偏移/长度/几何不可表示、重叠、越界、重复 BindingId。
    Malformed { detail: &'static str },
    /// 序列为 odd：写事务进行中，本次不得接受（不可变快照入口）。
    WriterInProgress { sequence: u64 },
    /// 前后两次序列不等且重试已耗尽（写者持续占用）：可见的 writer busy。
    RetryExhausted { attempts: u32 },
    /// backing 不提供对齐原子序列能力（SAB 原子读不可用）。
    SequenceUnavailable,
    /// backing 回调 panic（在 ViewBridge 边界 contain 成 typed 失败）。
    BackingPanicked { op: &'static str },
}

impl fmt::Display for RegionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => write!(f, "region magic mismatch (expect \"UVBR\")"),
            Self::UnsupportedVersion { major } => write!(
                f,
                "unsupported protocol major {major} (this reader supports {PROTOCOL_VERSION})"
            ),
            Self::Truncated { need, got } => {
                write!(f, "region truncated: need {need}, got {got}")
            }
            Self::Malformed { detail } => write!(f, "region malformed: {detail}"),
            Self::WriterInProgress { sequence } => {
                write!(
                    f,
                    "region publication in progress (odd sequence {sequence})"
                )
            }
            Self::RetryExhausted { attempts } => {
                write!(f, "region writer busy: {attempts} retries exhausted")
            }
            Self::SequenceUnavailable => write!(f, "region backing has no atomic sequence"),
            Self::BackingPanicked { op } => write!(f, "region backing panicked in `{op}`"),
        }
    }
}

impl std::error::Error for RegionError {}

/// 写侧编码故障（[`RegionWriter`] 自报；`fault` 非空 ⇒ 本批不可发布）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WriterFault {
    /// 载荷/段长超出 v1 的 `u32` 宽度域（#141）。
    PayloadTooLong,
    /// 非有限 `f64`（NaN/±Inf）：JSON 值模型无法表示，禁编码（#153）。
    NonFiniteF64,
    /// 不是恰好一段完整 TLV（零段/多段/段内残留——#168/#185）。
    NotSingleTlv,
    /// TLV 头或载荷截断。
    TruncatedTlv,
    /// 定宽 tag 的长度不符（tag 0/1/2 = 8 字节，tag 6 = 0 字节）。
    BadTagLength,
    /// tag 3 非 UTF-8 / tag 5 非合法 JSON。
    BadPayload,
}

impl fmt::Display for WriterFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLong => write!(f, "payload exceeds the v1 u32 length field"),
            Self::NonFiniteF64 => write!(f, "non-finite f64 is not representable in JSON"),
            Self::NotSingleTlv => write!(f, "encoder must emit exactly one complete TLV segment"),
            Self::TruncatedTlv => write!(f, "truncated TLV header/payload"),
            Self::BadTagLength => write!(f, "tag length violates the fixed-width rule"),
            Self::BadPayload => write!(f, "tag payload is not valid for its tag"),
        }
    }
}

/// 对齐原子 publish-sequence 操作结果（[`RegionBacking`] 能力契约）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SeqError {
    /// 该 backing 不提供 8 字节对齐的原子序列（lock-free 模式必须被拒绝——
    /// 字节拷贝**不等价**于原子写，禁作降级实现）。
    Unsupported,
    /// 宿主原子操作自报失败（平台/内存校验不通过等）。
    Failed,
}

impl fmt::Display for SeqError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => write!(f, "backing does not expose an aligned atomic sequence"),
            Self::Failed => write!(f, "backing atomic sequence operation failed"),
        }
    }
}

impl std::error::Error for SeqError {}

/// 区域发布失败（写侧，**release/debug 同口径的 fail-visible 结果**；禁
/// `debug_assert!` 作为唯一处理）。`Backing*` 类失败之后的通道一律标
/// unavailable——不得声称旧 region 仍有效，直到显式 `ViewBridge::reseed_region`
/// 成功。
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RegionFailure {
    /// 背衬不提供对齐原子发布序列能力：lock-free 区域通道在装配期即被拒绝。
    BackingUnsupported,
    /// 背衬容量连固定 `Header + Index` 都放不下（#142：seal 失败，不是 arena-full）。
    HeaderIndexTooLarge { required: usize, capacity: usize },
    /// 全量重编仍放不下 arena（#172：上一份已知良好元数据保持有效）。
    CapacityExceeded {
        required: usize,
        capacity: usize,
        region_id: u32,
    },
    /// `RegionEncoder::covers` panic（#188）。
    CoversPanicked,
    /// `RegionEncoder::encode` panic（#188）。
    EncoderPanicked { path: &'static str },
    /// 编码器输出不合法（#141/#153/#168/#185）。
    EncoderOutput {
        path: &'static str,
        fault: WriterFault,
    },
    /// backing 回调 panic（#211）。
    BackingPanicked { op: &'static str },
    /// 背衬报告容量与借出的切片长度不符（宿主违背固定容量契约）。
    BackingCapacityMismatch { reported: usize, actual: usize },
    /// backing 原子序列操作失败。
    Sequence { fault: SeqError },
    /// 序列/region_id 代次耗尽（**禁 wrapping 复用旧代次**）。
    GenerationExhausted,
    /// 另一发布者正在进行（`reseed_region` 与 dispatch 并发 = 宿主违约）。
    PublishBusy,
}

impl fmt::Display for RegionFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BackingUnsupported => write!(
                f,
                "region backing has no aligned atomic publish sequence (lock-free mode rejected)"
            ),
            Self::HeaderIndexTooLarge { required, capacity } => write!(
                f,
                "backing too small for header+index: need {required}, capacity {capacity}"
            ),
            Self::CapacityExceeded {
                required,
                capacity,
                region_id,
            } => write!(
                f,
                "region {region_id} arena capacity exceeded: need {required}, capacity {capacity}"
            ),
            Self::CoversPanicked => write!(f, "RegionEncoder::covers panicked"),
            Self::EncoderPanicked { path } => {
                write!(f, "RegionEncoder::encode panicked at {path}")
            }
            Self::EncoderOutput { path, fault } => {
                write!(f, "invalid encoder output at {path}: {fault}")
            }
            Self::BackingPanicked { op } => write!(f, "RegionBacking::{op} panicked"),
            Self::BackingCapacityMismatch { reported, actual } => write!(
                f,
                "backing capacity drift: reported {reported}, slice {actual}"
            ),
            Self::Sequence { fault } => write!(f, "publish sequence: {fault}"),
            Self::GenerationExhausted => {
                write!(f, "region sequence/region_id generation exhausted")
            }
            Self::PublishBusy => write!(f, "another region publication is in flight"),
        }
    }
}

impl std::error::Error for RegionFailure {}

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
/// ⇒ 缺段 ⇒ 走 `snapshot_doc` 重同步）；`to_version` 与区域头 publish-sequence
/// 同源同值（even）。
#[derive(Debug)]
pub struct PatchBatch {
    pub patches: Vec<Patch>,
    /// 流完整性可疑（投影 panic 毒化）⇒ UI 必须 `snapshot_doc` 重同步。
    pub resync: bool,
    pub from_version: u64,
    pub to_version: u64,
    /// 信封协议版本；不支持的 major 读侧整批协商失败，条目级未知 kind 逐项跳过。
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
/// 新鲜度判据 = 区域头 publish-sequence）。
///
/// 契约（#188/#168/#185）：**恰好一段非空、结构完整的 TLV**——零段/多段/段内
/// 残留都是编码器故障（`RegionFailure::EncoderOutput`），不会作为「无值」发布；
/// 空输出**不是**删除标记。panic 在桥边界 contain（订阅保持存活）。
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
///
/// 写入期自检（#141/#153）：宽度溢出与非有限 `f64` 置 [`WriterFault`]，此后本次
/// 累计不再追加——`fault()` 非空 ⇒ 整批不可发布（发布前显式拒绝，禁半成品入盘）。
#[derive(Default)]
pub struct RegionWriter {
    out: Vec<u8>,
    fault: Option<WriterFault>,
}

impl RegionWriter {
    pub fn new() -> Self {
        Self::default()
    }
    /// 清空复用（批内多段共享一个 writer，省分配）。
    pub fn clear(&mut self) {
        self.out.clear();
        self.fault = None;
    }
    /// 累计字节数（含已写各段；段边界由调用方以 `len()` 差值记录）。
    pub fn len(&self) -> usize {
        self.out.len()
    }
    /// 空判（`len` 配对纪律）。
    pub fn is_empty(&self) -> bool {
        self.out.is_empty()
    }
    /// 本次累计期间的写入故障（`None` = 全部合法）。
    pub fn fault(&self) -> Option<WriterFault> {
        self.fault
    }
    /// 已累计字节（发布前校验用；不对采用方开放改写）。
    fn as_slice(&self) -> &[u8] {
        &self.out
    }
    fn tlv(&mut self, tag: u8, payload: &[u8]) {
        if self.fault.is_some() {
            return;
        }
        let Ok(len) = u32::try_from(payload.len()) else {
            self.fault = Some(WriterFault::PayloadTooLong);
            return;
        };
        if self
            .out
            .len()
            .checked_add(TLV_HEADER_LEN + payload.len())
            .is_none()
        {
            self.fault = Some(WriterFault::PayloadTooLong);
            return;
        }
        self.out.push(tag);
        self.out.extend_from_slice(&len.to_le_bytes());
        self.out.extend_from_slice(payload);
    }
    /// tag 0。
    pub fn u64(&mut self, v: u64) {
        self.tlv(TAG_U64, &v.to_le_bytes());
    }
    /// tag 1。
    pub fn i64(&mut self, v: i64) {
        self.tlv(TAG_I64, &v.to_le_bytes());
    }
    /// tag 2（IEEE754 LE）。**非有限值拒收**（#153：JSON 值模型无法表示
    /// NaN/±Inf，静默映射成 null 是语言边界数据损坏）。
    pub fn f64(&mut self, v: f64) {
        if !v.is_finite() {
            if self.fault.is_none() {
                self.fault = Some(WriterFault::NonFiniteF64);
            }
            return;
        }
        self.tlv(TAG_F64, &v.to_le_bytes());
    }
    /// tag 3（len:u32 + UTF-8）。
    pub fn utf8(&mut self, s: &str) {
        self.tlv(TAG_UTF8, s.as_bytes());
    }
    /// tag 4（len:u32 + raw）。
    pub fn bytes(&mut self, b: &[u8]) {
        self.tlv(TAG_BYTES, b);
    }
    /// tag 5（len:u32 + 紧凑 JSON UTF-8）。复杂嵌套一律 json_blob——与
    /// `JsonEncoder::to_json` 同源取值（两编码语义一致性的机制保证）。
    pub fn json_blob(&mut self, v: &Value) {
        self.tlv(TAG_JSON, v.to_string().as_bytes());
    }
}

/// 校验一段编码器输出**恰好是一段完整 TLV**（#168/#185；发布前闸门，禁
/// 「空输出 = 无值」的静默降级）。
fn validate_single_tlv(seg: &[u8]) -> Result<(), WriterFault> {
    if seg.len() < TLV_HEADER_LEN {
        return Err(WriterFault::TruncatedTlv);
    }
    let tag = seg[0];
    let len = u32::from_le_bytes(seg[1..TLV_HEADER_LEN].try_into().expect("4 bytes")) as usize;
    let end = TLV_HEADER_LEN
        .checked_add(len)
        .ok_or(WriterFault::TruncatedTlv)?;
    if end != seg.len() {
        return Err(WriterFault::NotSingleTlv);
    }
    let payload = &seg[TLV_HEADER_LEN..];
    match tag {
        TAG_U64 | TAG_I64 | TAG_F64 if len != 8 => Err(WriterFault::BadTagLength),
        TAG_NULL if len != 0 => Err(WriterFault::BadTagLength),
        TAG_UTF8 => std::str::from_utf8(payload)
            .map(|_| ())
            .map_err(|_| WriterFault::BadPayload),
        TAG_JSON => serde_json::from_slice::<Value>(payload)
            .map(|_| ())
            .map_err(|_| WriterFault::BadPayload),
        _ => Ok(()),
    }
}

/// 区域背衬：固定容量、地址稳定的共享内存抽象——框架经回调安全写入，零 unsafe
/// 零裸指针留在框架侧（宿主实现自持 SAB/shm/Vec）。固定容量是地址稳定的硬前提。
///
/// **原子发布序列是必需能力**（`store/load_publish_seq`）：v1 的无锁承诺建立在
/// 「对 header 8..16 做 8 字节对齐的原子 release store / acquire load」之上；字节
/// 拷贝（哪怕地址对齐）**不等价**于原子写，因此默认实现返回
/// [`SeqError::Unsupported`] 而**不是**降级拷贝——不支持该能力的 backing 会让
/// 区域通道在装配期被拒绝（`region_version()` 恒 0，仅 JSON 通道可用）。
pub trait RegionBacking: Send + Sync + 'static {
    /// 固定容量（装配期读一次，此后必须恒定）。
    fn len(&self) -> usize;
    /// 空判（`len` 配对纪律；固定容量背衬恒 `false`）。
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// 框架唯一字节写入口：把 `&mut [u8]` 借给回调（宿主实现内自理锁/内存序）。
    /// 一次事务拆成两次调用（段字节 / 索引+头），**调用之间不是事务边界**——
    /// 事务边界是 publish-sequence 的 odd→even。
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8]));
    /// 框架读侧（镜像读取器/宿主拷贝出口）。
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8]));
    /// 对 header `8..16` 的**对齐原子**序列 store（**release** 语义）。
    /// 契约：序列字必须与 header 字节 8..16 是同一共享内存位置（SAB 形 = 同一
    /// `BigUint64Array` 视图；拷贝形则须在同一临界区内镜像进字节，使整读拷贝
    /// 读者看到同一序列）。
    fn store_publish_seq(&self, _seq: u64) -> Result<(), SeqError> {
        Err(SeqError::Unsupported)
    }
    /// 对 header `8..16` 的**对齐原子**序列 load（**acquire** 语义）。
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        Err(SeqError::Unsupported)
    }
}

/// 默认背衬：固定容量 Vec（进程内拷贝姿态）。原子序列字另持 `AtomicU64`，并在
/// `store_publish_seq` 的同一临界区内镜像进 header 字节 8..16——整读拷贝读者与
/// 原子读者看到同一序列。
pub struct VecBacking {
    buf: Mutex<Vec<u8>>,
    published: AtomicU64,
}

impl VecBacking {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Mutex::new(vec![0u8; capacity]),
            published: AtomicU64::new(0),
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
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        let mut buf = self.buf.lock().expect("vec backing poisoned");
        if let Some(slot) = buf.get_mut(OFFSET_PUBLISH_SEQ..OFFSET_REGION_ID) {
            slot.copy_from_slice(&seq.to_le_bytes());
        } else {
            return Err(SeqError::Failed);
        }
        self.published.store(seq, Ordering::Release);
        Ok(())
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        Ok(self.published.load(Ordering::Acquire))
    }
}

// ── 机器 ──

/// 段偏移/cursor 对齐（8 字节，填充 0）。
fn pad8(n: usize) -> usize {
    n.checked_add(SEG_ALIGN - 1)
        .map(|x| x & !(SEG_ALIGN - 1))
        .unwrap_or(usize::MAX)
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
    /// 已提交的 even 发布水位（与区域头 publish-sequence / `to_version` 同值）。
    version: u64,
    delivered_epoch: u64,
}

/// 区域索引条目（背衬 Index 区的内存镜像）。
#[derive(Clone)]
struct RegionEntry {
    binding_id: u32,
    kind: u16,
    offset: usize,
    len: u32,
    gen: u32,
}

/// 一段已编码且已对齐填充的段字节。
struct Seg {
    entry: usize,
    /// 未填充长度（Index `len` 字段；TLV 自描述，填充字节不可见）。
    len: u32,
    /// 已 8 字节对齐填充的段字节。
    padded: Vec<u8>,
}

/// 一次发布事务的计划（**纯计算产物**，不含任何采用方调用）。
struct PublishPlan {
    region_id: u32,
    /// 新 committed even 序列。
    sequence: u64,
    entries: Vec<RegionEntry>,
    cursor: usize,
    /// `(arena 偏移, 已填充段字节)`。
    segments: Vec<(usize, Vec<u8>)>,
    header: [u8; HEADER_LEN],
    capacity: usize,
}

/// 区域通道（eager）：背衬布局 = Header 32B + Index 24×n B + Arena。单写者 =
/// 排空环 + `publishing` 互斥旗；Arena 耗尽 ⇒ 全量重编后**一次事务**换区
/// （region_id+1 + 版本延续）。
struct RegionChannel<R> {
    encoder: Arc<R>,
    backing: Arc<dyn RegionBacking>,
    capacity: usize,
    region_id: u32,
    /// 已提交 even 序列（未提交过 = 0）。
    sequence: u64,
    arena_start: usize,
    cursor: usize,
    count: u32,
    entries: Vec<RegionEntry>,
    /// `Some` = 通道 unavailable（背衬失败/容量不足），`reseed_region` 是唯一恢复口。
    state: Option<RegionFailure>,
    /// 发布事务互斥旗（**不是互斥量**：adopter 代码在锁外，违反者得 typed 失败
    /// 而不是死锁）。
    publishing: AtomicBool,
}

impl<R: RegionEncoder> RegionChannel<R> {
    fn new(encoder: Arc<R>, backing: Arc<dyn RegionBacking>) -> Self {
        // 装配期一次能力探测（采用方代码，无锁）：不支持的 backing ⇒ lock-free
        // 区域模式被拒绝，`region_version()` 恒 0（仅 JSON 通道可用）。
        let (capacity, failure) = match catch_unwind(AssertUnwindSafe(|| backing.len())) {
            Ok(n) => (n, None),
            Err(_) => (0, Some(RegionFailure::BackingPanicked { op: "len" })),
        };
        let failure = failure.or_else(|| {
            match catch_unwind(AssertUnwindSafe(|| backing.load_publish_seq())) {
                Ok(Ok(seq)) => {
                    // 未发布的拷贝背衬序列为 0；非 0 说明复用了已发布的共享区。
                    let _ = seq;
                    None
                }
                Ok(Err(SeqError::Unsupported)) => Some(RegionFailure::BackingUnsupported),
                Ok(Err(e)) => Some(RegionFailure::Sequence { fault: e }),
                Err(_) => Some(RegionFailure::BackingPanicked {
                    op: "load_publish_seq",
                }),
            }
        });
        Self {
            encoder,
            backing,
            capacity,
            region_id: 1,
            sequence: 0,
            arena_start: HEADER_LEN,
            cursor: HEADER_LEN,
            count: 0,
            entries: Vec::new(),
            state: failure,
            publishing: AtomicBool::new(false),
        }
    }

    fn ready(&self) -> bool {
        self.state.is_none()
    }

    /// 首通知封板（**纯内存**）：条目定死 + 固定 `Header + Index` 容量校验。
    /// 放不进 = seal 失败（#142），不是 arena-full。
    fn begin_seal(&mut self, bound: &[Bound]) -> Result<(), RegionFailure> {
        if let Some(f) = &self.state {
            return Err(f.clone());
        }
        let count = u32::try_from(bound.len()).map_err(|_| RegionFailure::HeaderIndexTooLarge {
            required: usize::MAX,
            capacity: self.capacity,
        })?;
        let required = ENTRY_LEN
            .checked_mul(bound.len())
            .and_then(|n| n.checked_add(HEADER_LEN))
            .ok_or(RegionFailure::HeaderIndexTooLarge {
                required: usize::MAX,
                capacity: self.capacity,
            })?;
        if required > self.capacity {
            let f = RegionFailure::HeaderIndexTooLarge {
                required,
                capacity: self.capacity,
            };
            self.state = Some(f.clone());
            return Err(f);
        }
        self.count = count;
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
        self.arena_start = required;
        self.cursor = required;
        Ok(())
    }

    /// 封板事务计划：committed even = [`INITIAL_SEQ`]，全条目 `len == 0`
    /// （「未发布」——空输出**不是**删除标记）。
    fn plan_initial(&self) -> PublishPlan {
        PublishPlan {
            region_id: self.region_id,
            sequence: INITIAL_SEQ,
            entries: self.entries.clone(),
            cursor: self.arena_start,
            segments: Vec::new(),
            header: header_bytes(self.region_id, self.count),
            capacity: self.capacity,
        }
    }

    /// 增量计划；`None` = arena 不足（调用方先做全量重编，再走 [`Self::plan_reseed`]）。
    fn plan_incremental(&self, segs: &[Seg], sequence: u64) -> Option<PublishPlan> {
        let mut need = 0usize;
        for s in segs {
            need = need.checked_add(s.padded.len())?;
        }
        let end = self.cursor.checked_add(need)?;
        if end > self.capacity {
            return None;
        }
        self.place(segs, sequence, false, self.cursor)
    }

    /// 全量重编计划（换区）：先算总编码大小再动任何 live 状态；放不下 ⇒ typed
    /// `CapacityExceeded` 且**上一份已知良好元数据保持有效**（#172）。
    fn plan_reseed(&self, segs: &[Seg], sequence: u64) -> Result<PublishPlan, RegionFailure> {
        let mut need = 0usize;
        for s in segs {
            need = need
                .checked_add(s.padded.len())
                .ok_or(RegionFailure::CapacityExceeded {
                    required: usize::MAX,
                    capacity: self.capacity,
                    region_id: self.region_id,
                })?;
        }
        let required =
            self.arena_start
                .checked_add(need)
                .ok_or(RegionFailure::CapacityExceeded {
                    required: usize::MAX,
                    capacity: self.capacity,
                    region_id: self.region_id,
                })?;
        if required > self.capacity {
            return Err(RegionFailure::CapacityExceeded {
                required,
                capacity: self.capacity,
                region_id: self.region_id,
            });
        }
        let region_id = self
            .region_id
            .checked_add(1)
            .ok_or(RegionFailure::GenerationExhausted)?;
        // 换区从 arena 起点重排（`required` 是「总量校验」值，不是 cursor 起点）。
        let mut plan = self
            .place(segs, sequence, true, self.arena_start)
            .ok_or(RegionFailure::GenerationExhausted)?;
        plan.region_id = region_id;
        plan.header = header_bytes(region_id, self.count);
        Ok(plan)
    }

    /// 落位计算（纯算术）：`reset` = 换区（条目全清零、cursor 回 arena 起点）。
    fn place(
        &self,
        segs: &[Seg],
        sequence: u64,
        reset: bool,
        cursor_start: usize,
    ) -> Option<PublishPlan> {
        let mut entries = if reset {
            self.entries
                .iter()
                .map(|e| RegionEntry {
                    offset: 0,
                    len: 0,
                    gen: 0,
                    ..e.clone()
                })
                .collect::<Vec<_>>()
        } else {
            self.entries.clone()
        };
        let mut cursor = cursor_start;
        let mut segments = Vec::with_capacity(segs.len());
        for s in segs {
            let e = entries.get_mut(s.entry)?;
            e.offset = cursor;
            e.len = s.len;
            e.gen = e.gen.wrapping_add(1); // 条目逻辑修订号（非原子标记，见规格 §5.2）
            segments.push((cursor, s.padded.clone()));
            cursor = cursor.checked_add(s.padded.len())?;
        }
        Some(PublishPlan {
            region_id: self.region_id,
            sequence,
            header: header_bytes(self.region_id, self.count),
            entries,
            cursor,
            segments,
            capacity: self.capacity,
        })
    }

    /// 事务提交点（**背衬字节已全部落盘之后**才调用）：换内存镜像 + 推进水位。
    fn commit(&mut self, plan: PublishPlan) {
        self.region_id = plan.region_id;
        self.sequence = plan.sequence;
        self.cursor = plan.cursor;
        self.entries = plan.entries;
        self.state = None;
    }

    fn fail(&mut self, f: RegionFailure) {
        self.state = Some(f);
    }
}

/// Header 镜像（提交时只拷贝 `0..8` 与 `16..32`：`6..8` flags 恒 0，`8..16` 序列由
/// backing 原子操作独占——字节路径**不写**这两个区间）。
fn header_bytes(region_id: u32, binding_count: u32) -> [u8; HEADER_LEN] {
    let mut h = [0u8; HEADER_LEN];
    h[OFFSET_MAGIC..OFFSET_PROTOCOL].copy_from_slice(&MAGIC);
    h[OFFSET_PROTOCOL..OFFSET_FLAGS].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    h[OFFSET_REGION_ID..OFFSET_BINDING_COUNT].copy_from_slice(&region_id.to_le_bytes());
    h[OFFSET_BINDING_COUNT..OFFSET_INDEX_OFFSET].copy_from_slice(&binding_count.to_le_bytes());
    h[OFFSET_INDEX_OFFSET..HEADER_LEN].copy_from_slice(&INDEX_OFFSET_V1.to_le_bytes());
    h
}

fn entry_bytes(e: &RegionEntry) -> [u8; ENTRY_LEN] {
    let mut b = [0u8; ENTRY_LEN];
    b[ENTRY_OFFSET_BINDING_ID..ENTRY_OFFSET_KIND].copy_from_slice(&e.binding_id.to_le_bytes());
    b[ENTRY_OFFSET_KIND..ENTRY_OFFSET_KIND + 2].copy_from_slice(&e.kind.to_le_bytes());
    b[ENTRY_OFFSET_OFFSET..ENTRY_OFFSET_LEN].copy_from_slice(&(e.offset as u64).to_le_bytes());
    b[ENTRY_OFFSET_LEN..ENTRY_OFFSET_GEN].copy_from_slice(&e.len.to_le_bytes());
    b[ENTRY_OFFSET_GEN..ENTRY_LEN].copy_from_slice(&e.gen.to_le_bytes());
    b
}

fn store_seq(backing: &dyn RegionBacking, seq: u64) -> Result<(), RegionFailure> {
    match catch_unwind(AssertUnwindSafe(|| backing.store_publish_seq(seq))) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(SeqError::Unsupported)) => Err(RegionFailure::BackingUnsupported),
        Ok(Err(e)) => Err(RegionFailure::Sequence { fault: e }),
        Err(_) => Err(RegionFailure::BackingPanicked {
            op: "store_publish_seq",
        }),
    }
}

/// 执行一次事务：**odd → 段字节 → 索引+头（跳过序列字节）→ even**。
///
/// 序列是唯一事务边界（一次事务可拆成两次 backing 调用）：odd 之后任何失败都
/// 让区域留在「进行中」，读者必拒收；even 只在全部字节落盘后以 release 语义
/// store（提交点，最后写）。
fn execute_plan(backing: &dyn RegionBacking, plan: &PublishPlan) -> Result<(), RegionFailure> {
    // even 序列 ⇒ odd = seq - 1。计划只产出 even（禁把 odd 当已提交版本）。
    let odd = plan
        .sequence
        .checked_sub(1)
        .filter(|s| s % 2 == 1)
        .ok_or(RegionFailure::GenerationExhausted)?;
    store_seq(backing, odd)?;

    // ① 段字节（按计划偏移整段写入，禁部分段）。
    if !plan.segments.is_empty() {
        let mut short: Option<(usize, usize)> = None;
        match catch_unwind(AssertUnwindSafe(|| {
            backing.with_bytes_mut(&mut |buf| {
                for (off, bytes) in &plan.segments {
                    let Some(end) = off.checked_add(bytes.len()) else {
                        short = Some((usize::MAX, buf.len()));
                        return;
                    };
                    match buf.get_mut(*off..end) {
                        Some(slot) => slot.copy_from_slice(bytes),
                        None => short = Some((end, buf.len())),
                    }
                }
            })
        })) {
            Ok(()) => {}
            Err(_) => {
                return Err(RegionFailure::BackingPanicked {
                    op: "with_bytes_mut",
                });
            }
        }
        if let Some((need, got)) = short {
            return Err(RegionFailure::BackingCapacityMismatch {
                reported: plan.capacity,
                actual: got.min(need),
            });
        }
    }

    // ② 索引 + 头（序列字节 8..16 由原子操作独占，禁字节拷贝覆盖）。
    let mut mismatch: Option<(usize, usize)> = None;
    match catch_unwind(AssertUnwindSafe(|| {
        backing.with_bytes_mut(&mut |buf| {
            if buf.len() != plan.capacity {
                mismatch = Some((plan.capacity, buf.len()));
                return;
            }
            let Some(index_end) = ENTRY_LEN
                .checked_mul(plan.entries.len())
                .and_then(|n| n.checked_add(HEADER_LEN))
            else {
                mismatch = Some((plan.capacity, buf.len()));
                return;
            };
            if buf.len() < index_end {
                mismatch = Some((plan.capacity, buf.len()));
                return;
            }
            for (ei, e) in plan.entries.iter().enumerate() {
                let at = HEADER_LEN + ENTRY_LEN * ei;
                buf[at..at + ENTRY_LEN].copy_from_slice(&entry_bytes(e));
            }
            buf[OFFSET_MAGIC..OFFSET_PUBLISH_SEQ]
                .copy_from_slice(&plan.header[OFFSET_MAGIC..OFFSET_PUBLISH_SEQ]);
            buf[OFFSET_REGION_ID..HEADER_LEN]
                .copy_from_slice(&plan.header[OFFSET_REGION_ID..HEADER_LEN]);
        })
    })) {
        Ok(()) => {}
        Err(_) => {
            return Err(RegionFailure::BackingPanicked {
                op: "with_bytes_mut",
            });
        }
    }
    if let Some((reported, actual)) = mismatch {
        return Err(RegionFailure::BackingCapacityMismatch { reported, actual });
    }

    // ③ even = 提交点（最后写，release）。
    store_seq(backing, plan.sequence)
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
    /// 当前 unavailable 原因（`None` = ready；成功提交/重编后清空）。
    region_failure: Mutex<Option<RegionFailure>>,
    /// 失败累计（含瞬时失败，如 `PublishBusy`）。
    region_failures: AtomicU64,
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
    /// 返回 `true` = 本次刚封板（调用方须在**锁外**发布区域初始事务）。
    fn bound_of(&self, core: &mut Core<M::View>) -> (Arc<[Bound]>, bool) {
        if let Some(b) = &core.bound {
            return (Arc::clone(b), false);
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
        // 封板即首个 committed even 水位：JSON/快照/区域三面同源同值。
        core.version = INITIAL_SEQ;
        core.bound = Some(Arc::clone(&bound));
        (bound, true)
    }

    /// 区域封板事务（`core` 锁外调用）：建表（纯内存）→ 发布初始 even 水位。
    fn seal_region(&self, bound: &[Bound]) {
        let plan = {
            let mut rg = lock(&self.region);
            let Some(ch) = rg.as_mut() else {
                return;
            };
            if let Err(f) = ch.begin_seal(bound) {
                drop(rg);
                self.fail_region(f, true);
                return;
            }
            ch.plan_initial()
        };
        self.publish_plan(plan);
    }

    /// 取区域编码器（锁内只做 `Arc::clone`，无采用方调用）。通道 unavailable 时
    /// 也返回——`reseed_region` 是唯一恢复口，必须能拿到编码器。
    fn region_encoder(&self) -> Option<Arc<R>> {
        let rg = lock(&self.region);
        rg.as_ref().map(|ch| Arc::clone(&ch.encoder))
    }

    /// 通道是否健康（unavailable 时增量发布直接跳过，禁把失败当静默空发布）。
    fn region_ready(&self) -> bool {
        lock(&self.region).as_ref().is_some_and(|ch| ch.ready())
    }

    /// `covers()` 逐绑定求值（采用方代码，锁外；panic ⇒ 可见失败 #188）。
    fn coverage(&self, encoder: &R, bound: &[Bound]) -> Option<Vec<bool>> {
        match catch_unwind(AssertUnwindSafe(|| {
            let covers = encoder.covers();
            bound
                .iter()
                .map(|b| covers.covers(b.path))
                .collect::<Vec<bool>>()
        })) {
            Ok(v) => Some(v),
            Err(_) => {
                self.fail_region(RegionFailure::CoversPanicked, false);
                None
            }
        }
    }

    /// 逐绑定编码（采用方代码，锁外）：每绑定恰一段非空完整 TLV（#168/#185），
    /// 宽度/有限性由 writer 自报（#141/#153）。返回 `None` = 已记录可见失败。
    fn encode_segments(
        &self,
        encoder: &R,
        bound: &[Bound],
        items: &[(usize, M::View)],
        covered: &[bool],
    ) -> Option<Vec<Seg>> {
        let mut writer = RegionWriter::new();
        let mut segs: Vec<Seg> = Vec::new();
        for (bi, view) in items {
            if !covered.get(*bi).copied().unwrap_or(false) {
                continue;
            }
            writer.clear();
            if catch_unwind(AssertUnwindSafe(|| encoder.encode(view, &mut writer))).is_err() {
                self.fail_region(
                    RegionFailure::EncoderPanicked {
                        path: bound[*bi].path,
                    },
                    false,
                );
                return None;
            }
            let seg = writer.as_slice();
            let fault = writer.fault().or_else(|| validate_single_tlv(seg).err());
            if let Some(fault) = fault {
                self.fail_region(
                    RegionFailure::EncoderOutput {
                        path: bound[*bi].path,
                        fault,
                    },
                    false,
                );
                return None;
            }
            let Ok(len) = u32::try_from(seg.len()) else {
                self.fail_region(
                    RegionFailure::EncoderOutput {
                        path: bound[*bi].path,
                        fault: WriterFault::PayloadTooLong,
                    },
                    false,
                );
                return None;
            };
            let mut padded = seg.to_vec();
            padded.resize(pad8(seg.len()), 0);
            segs.push(Seg {
                entry: *bi,
                len,
                padded,
            });
        }
        Some(segs)
    }

    /// 区域增量发布：**锁外编码 → 锁内计划 → 锁外事务 → 锁内提交**。
    /// `changed` = `(绑定表位置, 视图值)`。
    fn publish_changes(&self, bound: &[Bound], changed: &[(usize, M::View)], sequence: u64) {
        let Some(encoder) = self.region_encoder() else {
            return;
        };
        if !self.region_ready() {
            return;
        }
        let Some(covered) = self.coverage(encoder.as_ref(), bound) else {
            return;
        };
        let Some(segs) = self.encode_segments(encoder.as_ref(), bound, changed, &covered) else {
            return;
        };
        // 计划：锁内纯算术（无采用方调用）。
        let plan = {
            let rg = lock(&self.region);
            let Some(ch) = rg.as_ref() else {
                return;
            };
            match ch.plan_incremental(&segs, sequence) {
                Some(p) => p,
                // 增量放不下 ⇒ 全量重编（先算总大小再动 live 状态）。
                None => {
                    drop(rg);
                    return self.reseed_with(bound, encoder.as_ref(), sequence);
                }
            }
        };
        self.region_encodes
            .fetch_add(segs.len() as u64, Ordering::Relaxed);
        self.publish_plan(plan);
    }

    /// 全量重编 + 换区（增量耗尽路径 / `reseed_region` 恢复口）。
    fn reseed_with(&self, bound: &[Bound], encoder: &R, sequence: u64) {
        let items = self.full_snapshot(bound);
        let Some(covered) = self.coverage(encoder, bound) else {
            return;
        };
        let Some(segs) = self.encode_segments(encoder, bound, &items, &covered) else {
            return;
        };
        let plan = {
            let rg = lock(&self.region);
            let Some(ch) = rg.as_ref() else {
                return;
            };
            match ch.plan_reseed(&segs, sequence) {
                Ok(p) => p,
                // 放不下 ⇒ 上一份已知良好元数据保持有效，失败可见（不再 debug_assert）。
                Err(f) => {
                    drop(rg);
                    return self.fail_region(f, false);
                }
            }
        };
        self.publish_plan(plan);
    }

    /// 现态全量视图快照（core 锁内捕获；未发布过的绑定缺席）。
    fn full_snapshot(&self, bound: &[Bound]) -> Vec<(usize, M::View)> {
        let core = lock(&self.core);
        bound
            .iter()
            .enumerate()
            .filter_map(|(bi, _)| core.last[bi].as_ref().map(|v| (bi, v.clone())))
            .collect()
    }

    /// 执行 + 提交（唯一入口：先锁外事务，成功才锁内换镜像并对外宣称新水位）。
    /// `publishing` 旗提供并发可见失败（`PublishBusy`）而不是死锁——它**不是**
    /// 互斥量：采用方代码（encode/backing）在锁外执行。
    fn publish_plan(&self, plan: PublishPlan) {
        let (backing, busy) = {
            let rg = lock(&self.region);
            let Some(ch) = rg.as_ref() else {
                return;
            };
            let busy = ch
                .publishing
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err();
            (Arc::clone(&ch.backing), busy)
        };
        if busy {
            return self.fail_region(RegionFailure::PublishBusy, false);
        }
        let outcome = execute_plan(backing.as_ref(), &plan);
        let committed = {
            let mut rg = lock(&self.region);
            let mut meta = None;
            if let Some(ch) = rg.as_mut() {
                ch.publishing.store(false, Ordering::Release);
                if outcome.is_ok() {
                    ch.commit(plan);
                    meta = Some((ch.sequence, ch.region_id));
                }
            }
            meta
        };
        match outcome {
            Ok(()) => {
                if let Some((sequence, region_id)) = committed {
                    self.region_ver.store(sequence, Ordering::Release);
                    self.region_id_cur.store(region_id, Ordering::Release);
                }
                *lock(&self.region_failure) = None;
            }
            // 失败：禁宣称旧 region 仍有效（事务可能已留下 odd）⇒ 通道 unavailable，
            // 直到显式成功全量 reseed。
            Err(f) => self.fail_region(f, true),
        }
    }

    /// 记录失败：`region_failure` 槽恒记最后一次失败（下一次成功发布/重编清空，
    /// 让「编码器抖动」与「通道不可用」都可观测）；`unavailable` 只额外把通道
    /// 转 unavailable（后续 publish 直接跳过，恢复只能走 `reseed_region`）。
    /// 锁序恒为「region_failure → region」，禁反序。
    fn fail_region(&self, failure: RegionFailure, unavailable: bool) {
        self.region_failures.fetch_add(1, Ordering::Relaxed);
        *lock(&self.region_failure) = Some(failure.clone());
        if unavailable {
            let mut rg = lock(&self.region);
            if let Some(ch) = rg.as_mut() {
                ch.fail(failure);
            }
        }
    }

    fn on_notify(&self, state: &S) {
        let (bound, first) = {
            let mut core = lock(&self.core);
            self.bound_of(&mut core)
        };
        if first {
            self.seal_region(&bound);
        }
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
        // 锁内折叠：类型 eq 细闸（跳补丁）→ 移槽置脏（latest-wins）+ 水位推进 + 毒化。
        let sequence;
        let mut changed: Vec<(usize, M::View)> = Vec::new();
        {
            let mut core = lock(&self.core);
            let Some(next) = core.version.checked_add(SEQ_STEP) else {
                // 代次耗尽：typed/可见降级（禁 wrapping 复用旧代次）。
                core.poisoned = true;
                return;
            };
            core.version = next;
            sequence = next;
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
        // eager 区域编码 + 事务（锁外编码；空批也推进头序列，跨通道同源）。
        self.publish_changes(&bound, &changed, sequence);
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
    /// 背衬不支持对齐原子发布序列 ⇒ 区域通道装配期即 unavailable（lock-free
    /// 模式被拒绝；`region_failure()` 显形），JSON 通道不受影响。
    pub fn mount<T>(store: T, map: M, intents: I, encoders: Encoders<J, R>) -> Self
    where
        T: Store<State = S, Action = A> + Send + Sync + 'static,
    {
        let (region, initial_failure) = match encoders.region {
            Some((enc, backing)) => {
                let ch = RegionChannel::new(Arc::new(enc), backing);
                let failure = ch.state.clone();
                (Some(ch), failure)
            }
            None => (None, None),
        };
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
            region_failure: Mutex::new(initial_failure),
            region_failures: AtomicU64::new(0),
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
        // 错误优先级保持：封板先于未知路径。
        if lock(&notify.core).bound.is_some() {
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
        // 覆盖判定是采用方代码（`covers()`），必须在锁外求值（#222 §2.1）。
        let json_cover = notify
            .json
            .as_ref()
            .is_some_and(|j| j.covers().covers(path));
        let region_cover = notify
            .region_encoder()
            .is_some_and(|e| e.covers().covers(path));
        if !json_cover && !region_cover {
            notify.assembly_gaps.fetch_add(1, Ordering::Relaxed);
            debug_assert!(
                false,
                "assembly gap: bound path {path} covered by no encoder"
            );
        }
        let mut core = lock(&notify.core);
        if core.bound.is_some() {
            return Err(BindError::Sealed); // 并发封板：注册面已关
        }
        if let Some(&seen) = core.registered.iter().find(|&&r| r == index) {
            return Ok(BindingId(seen as u32)); // 幂等
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
    /// `to_version` = 已提交 even 水位（与区域头 publish-sequence 同源同值）。
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
        let (bound, first) = {
            let mut core = lock(&notify.core);
            notify.bound_of(&mut core)
        };
        if first {
            notify.seal_region(&bound);
        }
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

    /// 显式全量重编（区域通道 unavailable 后的**唯一恢复口**，#172/#211）：以
    /// 桥内已提交现值为内容做一次全量换区事务；成功才清 failure 并推进
    /// `region_id`/序列（sequence 随桥水位 +[`SEQ_STEP`]，跨通道同源不变）。
    /// 宿主违约并发调用（与 dispatch 重叠）⇒ `PublishBusy`，不破坏已提交状态。
    pub fn reseed_region(&self) -> Result<(), RegionFailure> {
        let notify = &self.inner.notify;
        let (bound, first) = {
            let mut core = lock(&notify.core);
            notify.bound_of(&mut core)
        };
        if first {
            notify.seal_region(&bound);
        }
        let Some(encoder) = notify.region_encoder() else {
            return Err(RegionFailure::BackingUnsupported);
        };
        let sequence = {
            let mut core = lock(&notify.core);
            let next = core
                .version
                .checked_add(SEQ_STEP)
                .ok_or(RegionFailure::GenerationExhausted)?;
            core.version = next;
            next
        };
        notify.reseed_with(&bound, encoder.as_ref(), sequence);
        let failure = lock(&notify.region_failure).clone();
        match failure {
            None => Ok(()),
            // 恢复失败：原因继续显形（成功时提交点已清空）。
            Some(f) => Err(f),
        }
    }

    /// 区域头已提交 even 序列（未挂区域通道/通道 unavailable = 0；读侧原子镜像，
    /// 与 `PatchBatch::to_version` / `Snapshot::version` 同源判据）。
    pub fn region_version(&self) -> u64 {
        self.inner.notify.region_ver.load(Ordering::Acquire)
    }

    /// 换区握手：id 变化 = UI 重映射（未提交过 = 0）。
    pub fn region_id(&self) -> u32 {
        self.inner.notify.region_id_cur.load(Ordering::Acquire)
    }

    /// 最后一次区域失败（`None` = 自上次成功发布以来无失败）。事务内失败
    /// （背衬 panic / 容量漂移 / 原子序列失败）会把通道转 unavailable——
    /// **必须**经 [`Self::reseed_region`] 显式恢复；事务外失败（编码器故障 /
    /// 容量不足）不动已提交 region，下一次成功发布即清空本槽。
    pub fn region_failure(&self) -> Option<RegionFailure> {
        lock(&self.inner.notify.region_failure).clone()
    }

    /// 区域失败累计（含瞬时失败：panic / 容量 / busy）。
    pub fn region_failures(&self) -> u64 {
        self.inner.notify.region_failures.load(Ordering::Relaxed)
    }

    /// eager 编码累计（成本可见；空批推进头序列不计入）。
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
