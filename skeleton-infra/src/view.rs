//! 语言无关视图数据面参考机器（ADR 0006；数据走向/线程模型/版本语义/毒化闭环
//! 的完整语义承诺见契约层 `skeleton::view` 与 `docs/view-region-spec.md`）。
//!
//! `ViewBridge` 以**纯订阅者**挂 `Store` 通知面（核心零改动）。线程模型：通知体
//! 在 dispatch 线程同步执行，pull 侧跨线程 ⇒ 桥内态单把粗锁成组迁移（last/脏位/
//! 毒化位一次动齐），投影在锁外，哨兵胞锁为外层短叶锁（先取先放零嵌套），绑定表
//! 封板后只读。版本语义（同步档 = MVP 唯一范围）：出站版本 = 桥内已提交
//! **even 序列**（区域头 publish-sequence 与 JSON 批 `to_version` 同源同值；
//! odd 只表示区域写事务进行中，永不作为已提交版本暴露）；`delivered_epoch` 是
//! 交付游标——`take_patches` 与 `snapshot_doc` 同为交付点（快照重同步后下批
//! `from_version == snapshot.version`）。
//!
//! # 装配生命周期（#222 §B，检查型而非惯例）
//!
//! `mount`（**fallible**：捕获并编译 `bindings()` 声明、校验路径）→ `bind`（选取
//! 要发布的路径，`BindingId` = 登记序）→ `seal`（捕获**一次**稳定 Store 快照 →
//! 投影已登记绑定 → 提交初始已提交视图，水位 [`INITIAL_SEQ`] → 发布初始区域全量）
//! → 才允许 dispatch。封板前的通知只计数（[`ViewBridge::pre_seal_notifications`]）
//! 不投影，禁静默吞吐。**并发前置条件**：`mount`/`bind`/`seal` 必须在任何 dispatch
//! 之前完成（同线程装配）；`Store` 适配器做不到就须先提供「订阅 + 快照」原子设施，
//! 本桥不用两次独立读去掩盖这个竞态。
//!
//! 快照语义（#104）：`snapshot_doc` 从**已提交投影值 + 同一临界区捕获的桥水位**
//! 构造，不再独立采样 Store 后套新版本；只有「有绑定无已知良好值/毒化」时才走
//! 版本校验下的重投影修复。
//!
//! 回调边界纪律（#140 区域侧 / #188 / #211）：`RegionEncoder::encode` 与
//! `RegionBacking::{len,with_bytes,with_bytes_mut,store/load_publish_seq}` 均为
//! 采用方代码——**任何桥内互斥量下都不调用**；panic 在桥边界 contain 成 typed
//! 失败 + 计数，Store 订阅保持存活。两编码器的 `covers()` 只在装配期（`mount`，
//! 锁外）各求值一次并缓存，运行期零调用（#222 §2.1）。
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
use skeleton::view::{BindingDecl, IntentMap, PROTOCOL_VERSION, PayloadTag, ViewMap};
use skeleton::{ActionSender, Store, Subscription};

pub mod read;

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（fx/pool 同纪律）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    #[cfg(test)]
    lock_probe::ACQUISITIONS.with(|c| c.set(c.get() + 1));
    mutex
        .lock()
        .expect("view bridge lock poisoned: prior panic while holding the lock")
}

/// 单元测试专用的**线程局部**取锁计数（#140 热路径锁次数预算）。线程局部是必须的：
/// 进程级计数器会被同二进制里并发跑的其它测试污染（计数不变量只对本线程窗口成立）。
#[cfg(test)]
mod lock_probe {
    use std::cell::Cell;

    thread_local! {
        pub(super) static ACQUISITIONS: Cell<u64> = const { Cell::new(0) };
    }

    /// 本线程取锁次数的绝对值。
    pub(super) fn here() -> u64 {
        ACQUISITIONS.with(Cell::get)
    }
}

// ── 错误词表（手写 Display/Error，journal.rs 先例；#[non_exhaustive] M6 纪律）──

/// `#[non_exhaustive]`：下游 match 须带通配臂——未来加变体非破坏演进。
#[derive(Debug)]
#[non_exhaustive]
pub enum BindError {
    /// 未知路径（不在 `ViewMap::bindings()` 词表内，装配期 fail-visible）。
    UnknownPath { path: String },
    /// 该路径不被任何已配置编码器覆盖（#173：**全构建** typed 错误，不是
    /// `debug_assert!`）——没有输出通道能发布它，登记即静默缺值。
    UncoveredBinding { path: String },
    /// 已封板：绑定表只读，运行期动态注册属二期断点。
    Sealed,
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownPath { path } => write!(f, "unknown binding path: {path}"),
            Self::UncoveredBinding { path } => {
                write!(f, "binding covered by no configured encoder: {path}")
            }
            Self::Sealed => write!(f, "binding table sealed (seal() already ran)"),
        }
    }
}

impl std::error::Error for BindError {}

/// 装配（`mount`）失败词表：#222 §2.1/§2.2 的**一次性声明捕获 + 校验**——
/// 声明表非法时桥根本不被构造（不泄漏订阅、不暴露半装配句柄）。
#[derive(Debug)]
#[non_exhaustive]
pub enum MountError {
    /// `ViewMap::bindings()` panic（采用方代码在装配边界 contain）。
    DeclarationsPanicked,
    /// 声明数超出 `BindingId` 的 `u32` 表示域。
    TooManyBindings { count: usize },
    /// 非根绝对 RFC 6901 指针不合法：空/根指针、缺前导 `/`、非法 `~` 转义。
    InvalidPath {
        path: &'static str,
        detail: &'static str,
    },
    /// 两条声明解码到同一指针（含规范化后相同的原始拼写）。
    DuplicatePath { path: &'static str },
    /// 一条声明是另一条的严格前缀 ⇒ 不可表示为单棵 JSON 树（如 `/a` 与 `/a/b`）。
    PrefixCollision {
        path: &'static str,
        other: &'static str,
    },
    /// 编码器 `covers()` panic（采用方代码，装配边界 contain）。
    CoveragePanicked { channel: &'static str },
}

impl fmt::Display for MountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeclarationsPanicked => write!(f, "ViewMap::bindings panicked"),
            Self::TooManyBindings { count } => {
                write!(f, "too many binding declarations: {count}")
            }
            Self::InvalidPath { path, detail } => {
                write!(f, "invalid binding path {path:?}: {detail}")
            }
            Self::DuplicatePath { path } => {
                write!(f, "duplicate binding path {path:?}")
            }
            Self::PrefixCollision { path, other } => write!(
                f,
                "binding path {path:?} collides with prefix binding {other:?}"
            ),
            Self::CoveragePanicked { channel } => {
                write!(f, "{channel} encoder covers() panicked")
            }
        }
    }
}

impl std::error::Error for MountError {}

/// 封板（`seal`）失败词表。已封板后的重入是幂等成功，不是错误。
#[derive(Debug)]
#[non_exhaustive]
pub enum SealError {
    /// 现态快照入口 panic（`Store::snapshot`，采用方代码在边界 contain）。
    SnapshotPanicked,
    /// 初始区域事务失败：桥仍已封板、JSON 通道不受影响，但区域通道转 unavailable
    /// （恢复只能走 [`ViewBridge::reseed_region`]）。
    Region(RegionFailure),
}

impl fmt::Display for SealError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotPanicked => write!(f, "Store::snapshot panicked during seal"),
            Self::Region(failure) => {
                write!(f, "initial region publication failed: {failure}")
            }
        }
    }
}

impl std::error::Error for SealError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SnapshotPanicked => None,
            Self::Region(f) => Some(f),
        }
    }
}

/// 重同步快照失败词表（#104 req 3 / #222 §6）：**禁把部分树当完整快照交付**。
#[derive(Debug)]
#[non_exhaustive]
pub enum SnapshotError {
    /// 未封板：`seal()` 之前没有已提交视图基线（pre-seal 水位恒 0）。
    Unsealed,
    /// 修复重投影再次失败 ⇒ 文档不完整（原因见 `projection_panics()` / `resync`）。
    Incomplete,
    /// 修复期间水位持续推进，Store 快照与已提交水位无法配对（重试耗尽）。
    Raced { attempts: u32 },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsealed => write!(f, "bridge not sealed: call seal() first"),
            Self::Incomplete => write!(f, "snapshot incomplete: a bound value is missing/stale"),
            Self::Raced { attempts } => {
                write!(f, "snapshot repair lost the race after {attempts} attempts")
            }
        }
    }
}

impl std::error::Error for SnapshotError {}

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
    /// 序列为 odd：写事务进行中，本次不得接受。**两个入口都会产生**——不可变快照
    /// 入口在字节里读到 odd 即拒收；lock-free 入口在重试耗尽且**最后一次原子观测
    /// 仍为 odd** 时也归此处（判定权只在原子序列，不看头里的字节镜像）。
    WriterInProgress { sequence: u64 },
    /// 重试耗尽：序列在**两个 even 之间**持续变化（写者一直在提交），前后两次读数
    /// 无法配对——可见的 writer busy（不是「写者占用不放」，而是写入持续推进）。
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
    /// 未封板：区域通道的初始事务属 `seal()`（#222 §B），之前没有已提交水位。
    Unsealed,
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
            Self::Unsealed => write!(f, "region channel not initialized: call seal() first"),
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

/// 机制胞的锁内态（[`Derived`] / [`Sentinel`] 共用，#157 两阶段代际协议）。临界区内
/// **只有**句柄搬移与代际记账，不执行任何采用方代码。
///
/// 协议三步：①锁内快照（值句柄 + 代际）→ ②锁外跑采用方代码（`eq` / `Clone` /
/// `compute` / 旧值 `Drop`）→ ③重取锁，**仅当代际未变**（快照之后无人发布）才裁决
/// 或发布。值以 [`Arc`] 句柄共享：快照只做原子计数，旧值随最后一个句柄在锁外析构。
struct CellState<T> {
    /// 最近一次采纳的值；`None` = 从未发布。
    current: Option<Arc<T>>,
    /// 每次采纳 +1；两阶段协议的令牌。
    generation: u64,
}

impl<T> CellState<T> {
    /// 锁内快照：值句柄 + 代际（临界区内仅原子计数）。
    fn snapshot(cell: &Mutex<Self>) -> (Option<Arc<T>>, u64) {
        let state = lock(cell);
        (state.current.clone(), state.generation)
    }

    /// 锁内复核：自 `generation` 那次快照以来是否无人发布。
    fn is_current(cell: &Mutex<Self>, generation: u64) -> bool {
        lock(cell).generation == generation
    }

    /// 锁内发布：代际未变 ⇒ 换入 `next`、代际 +1，返回被换出的旧值；代际已变 ⇒
    /// 缓存不动，`next` 原样退回。返回的句柄由调用方在锁外析构。
    fn publish(
        cell: &Mutex<Self>,
        generation: u64,
        next: Arc<T>,
    ) -> Result<Option<Arc<T>>, Arc<T>> {
        let displaced = {
            let mut state = lock(cell);
            if state.generation != generation {
                return Err(next);
            }
            // 回绕需 2^64 次发布（不可达）；wrapping 只为不留溢出 panic 路径。
            state.generation = state.generation.wrapping_add(1);
            state.current.replace(next)
        };
        Ok(displaced)
    }
}

/// 派生记忆化胞（ViewMap 内持字段使用）：`input(state)` 等值于缓存 ⇒ 返回缓存
/// （零重算）。
///
/// # 锁边界与并发语义（#157）
///
/// **采用方代码全在胞锁之外**：`input`、`compute`、`In::eq`、`Out::clone` 与缓存
/// 旧值的 `Drop` 都不在内部互斥量下执行。它们可以重入本胞、也可以 panic——既不会
/// 自锁，也不会毒化胞：panic 原样穿出 `get`，胞此后照常可用（`input` / `compute` /
/// `eq` / `clone` 的 panic 都发生在发布之前，缓存不变）。临界区内只有句柄搬移与
/// 代际记账。重入须自带终止条件：对**尚未缓存**的键在 `compute` 里再 `get` 会无限
/// 递归。
///
/// **并发未命中允许重复计算**：算出的 `Out` 对本次请求的输入恒是正确结果。只有
/// 「快照以来无人发布」（代际未变）才写缓存；代际已变 ⇒ 结果照常返回但**不写
/// 缓存**、不重试——陈旧计算永不覆盖更新的已提交值，缓存里的 `(In, Out)` 恒为同
/// 一次计算的自洽配对。
///
/// **线程共享门槛**：`Derived<S, In, Out>: Sync` 需要 `In: Send + Sync` 与
/// `Out: Send + Sync`（`Send` 同）。这是「采用方代码移出胞锁」的直接后果——比对
/// 与克隆在锁外读快照句柄（`Arc`），多线程可同时持有缓存值的共享引用。`In` /
/// `Out` 为 `Send` 但非 `Sync` 的胞只能在单线程内使用。
///
/// 胞内互斥量只可能在框架自身 bug 下中毒；一旦中毒沿本模块 `lock()` 约定 fail-fast
/// （panic 带因），不做静默降级。
pub struct Derived<S, In, Out> {
    cell: Mutex<CellState<(In, Out)>>,
    input: Box<dyn Fn(&S) -> In + Send + Sync>,
    compute: Box<dyn Fn(&In) -> Out + Send + Sync>,
}

impl<S, In: PartialEq, Out: Clone> Derived<S, In, Out> {
    pub fn new(
        input: impl Fn(&S) -> In + Send + Sync + 'static,
        compute: impl Fn(&In) -> Out + Send + Sync + 'static,
    ) -> Self {
        Self {
            cell: Mutex::new(CellState {
                current: None,
                generation: 0,
            }),
            input: Box::new(input),
            compute: Box::new(compute),
        }
    }

    /// 命中返回缓存；未命中重算，且仅当快照以来无人发布才换缓存（并发语义见
    /// 类型文档）。
    pub fn get(&self, state: &S) -> Out {
        let key = (self.input)(state);
        let (generation, hit) = self.lookup(&key);
        if let Some(out) = hit {
            return out;
        }
        let out = (self.compute)(&key);
        // 缓存副本与句柄分配都在锁外；返回的旧值 / 未采纳值在此（锁外）析构。
        drop(CellState::publish(
            &self.cell,
            generation,
            Arc::new((key, out.clone())),
        ));
        out
    }

    /// 锁内快照 + 锁外等值比对：返回（代际，命中的缓存副本）。快照句柄只活到
    /// 这里——旧值不随其后的长计算滞留，其析构也在锁外。
    fn lookup(&self, key: &In) -> (u64, Option<Out>) {
        let (cached, generation) = CellState::snapshot(&self.cell);
        let hit = match cached.as_deref() {
            Some((cached_in, cached_out)) if *cached_in == *key => Some(cached_out.clone()),
            _ => None,
        };
        (generation, hit)
    }
}

/// 绑定级输入哨兵：[`Derived`] 的退化形（只记输入不记输出）。In 抽取纪律：零分配
/// 或近零；抽不廉价的绑定不设哨兵（缺省 `true` 形态），禁哈希指纹（碰撞 = 漏报 =
/// 静默陈旧）。
///
/// # 锁边界与并发语义（#157）
///
/// **采用方代码全在胞锁之外**：`extract`、`In::eq` 与缓存旧键的 `Drop` 都不在内部
/// 互斥量下执行——可重入本胞，panic 不毒化：panic 原样穿出 `changed`，胞此后照常
/// 可用（`extract` / `eq` 的 panic 都发生在发布之前，缓存不变）。临界区内只有句柄
/// 搬移与代际记账。重入须自带终止条件：在 `In::eq` 里对同一个胞再 `changed` 会再
/// 进 `eq`，须保证它能终止。
///
/// **裁决在线性化点复核**：返回 `false` 当且仅当最后一次持锁那一刻，缓存键确等于
/// 本次键且自快照以来无人发布——与「一把锁包住比对 + 换缓存」同一语义。撞上并发
/// 发布（代际已变）用新快照**重试一轮**；仍不一致 ⇒ 返回 `true` 且**不提交**：
/// 多报只是多做一次投影，漏报才是违约。
///
/// 相对桥 core 锁恒为外层叶锁（先取先放，零嵌套）——这是调用方的锁序义务，与
/// 临界区内跑什么无关。
///
/// **线程共享门槛**：`Sentinel<S, In>: Sync` 需要 `In: Send + Sync`（`Send` 同）。
/// 这是「采用方代码移出胞锁」的直接后果——比对在锁外读快照句柄（`Arc`），多线程
/// 可同时持有缓存键的共享引用。`In` 为 `Send` 但非 `Sync` 的哨兵只能在单线程内
/// 使用。
///
/// 胞内互斥量只可能在框架自身 bug 下中毒；一旦中毒沿本模块 `lock()` 约定 fail-fast
/// （panic 带因），不做静默降级。
pub struct Sentinel<S, In> {
    cell: Mutex<CellState<In>>,
    extract: Box<dyn Fn(&S) -> In + Send + Sync>,
}

impl<S, In: PartialEq> Sentinel<S, In> {
    pub fn new(extract: impl Fn(&S) -> In + Send + Sync + 'static) -> Self {
        Self {
            cell: Mutex::new(CellState {
                current: None,
                generation: 0,
            }),
            extract: Box::new(extract),
        }
    }

    /// 抽取 + 等值；变更 ⇒ 换缓存返 `true`（并发语义见类型文档）。
    pub fn changed(&self, state: &S) -> bool {
        let key = (self.extract)(state);
        let (generation, equal) = self.equals_cached(&key);
        let key = if equal {
            if CellState::is_current(&self.cell, generation) {
                return false;
            }
            Arc::new(key) // 撞上并发发布：句柄化后带进重试
        } else {
            match CellState::publish(&self.cell, generation, Arc::new(key)) {
                Ok(displaced) => {
                    drop(displaced);
                    return true;
                }
                Err(unadopted) => unadopted, // 撞上并发发布：原句柄带回重试
            }
        };
        // 终轮：复核通过才算未变；否则再发布一次，采纳与否都多报 `true`。
        let (generation, equal) = self.equals_cached(&key);
        if equal && CellState::is_current(&self.cell, generation) {
            return false;
        }
        drop(CellState::publish(&self.cell, generation, key));
        true
    }

    /// 锁内快照 + 锁外等值比对：返回（代际，是否与缓存键等值）。
    fn equals_cached(&self, key: &In) -> (u64, bool) {
        let (cached, generation) = CellState::snapshot(&self.cell);
        (generation, cached.as_deref() == Some(key))
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
    /// 当前段在 `out` 中的起点。外部使用者恒 0；桥内批缓冲复用由 [`Self::begin_segment`]
    /// 前移，让每段编码直接写进可复用批次缓冲（零每段临时分配）。
    start: usize,
    fault: Option<WriterFault>,
}

impl RegionWriter {
    pub fn new() -> Self {
        Self::default()
    }

    /// 以调用方给的（可复用）缓冲起一个空 writer——桥内批缓冲复用入口。
    fn with_buffer(buf: Vec<u8>) -> Self {
        Self {
            out: buf,
            start: 0,
            fault: None,
        }
    }

    /// 交出底层缓冲（段边界见 [`Self::segment_start`]）。
    fn into_buffer(self) -> Vec<u8> {
        self.out
    }

    /// 起新段：段起点前移到缓冲尾并复位故障标记（批内多段共用一块连续缓冲）。
    fn begin_segment(&mut self) {
        self.start = self.out.len();
        self.fault = None;
    }

    /// 当前段在底层缓冲中的起点。
    fn segment_start(&self) -> usize {
        self.start
    }

    /// 清空复用（批内多段共享一个 writer，省分配）。
    pub fn clear(&mut self) {
        self.out.clear();
        self.start = 0;
        self.fault = None;
    }
    /// 累计字节数（含已写各段；段边界由调用方以 `len()` 差值记录）。
    pub fn len(&self) -> usize {
        self.out.len() - self.start
    }
    /// 空判（`len` 配对纪律）。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// 本次累计期间的写入故障（`None` = 全部合法）。
    pub fn fault(&self) -> Option<WriterFault> {
        self.fault
    }
    /// 当前段已累计字节（发布前校验用；不对采用方开放改写）。
    fn as_slice(&self) -> &[u8] {
        &self.out[self.start..]
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
        // 直接序列化进缓冲（省 `to_string()` 的整段临时 String）：先占 TLV 头，写完回填
        // 长度。只有 io 层错误会失败，失败即整体回退到段前（不留半截 TLV）。
        if self.fault.is_some() {
            return;
        }
        let head_at = self.out.len();
        self.out.push(TAG_JSON);
        self.out.extend_from_slice(&[0u8; 4]);
        let payload_at = self.out.len();
        if serde_json::to_writer(&mut self.out, v).is_err() {
            self.out.truncate(head_at);
            self.fault = Some(WriterFault::BadPayload);
            return;
        }
        let Ok(len) = u32::try_from(self.out.len() - payload_at) else {
            self.out.truncate(head_at);
            self.fault = Some(WriterFault::PayloadTooLong);
            return;
        };
        self.out[payload_at - 4..payload_at].copy_from_slice(&len.to_le_bytes());
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

/// 装配期编译后的声明（`bind` 的查找表；`ViewMap::bindings()` 装配期只调用一次）。
#[derive(Clone)]
struct Decl {
    path: &'static str,
    kind: PayloadTag,
    /// RFC 6901 解码后的段序列（文档建树用；热路径零拆串零解码）。
    segments: Vec<String>,
}

/// 编译一条 RFC 6901 指针（#146 / #222 §2.2）。v1 视图桥支持**非根绝对指针**这个
/// 明确子集：空/根指针、缺前导 `/`、`~` 后非 `0`/`1` 全部在装配期 typed 拒绝。
fn compile_pointer(path: &'static str) -> Result<Vec<String>, MountError> {
    let Some(rest) = path.strip_prefix('/') else {
        return Err(MountError::InvalidPath {
            path,
            detail: "must be a non-root absolute pointer starting with '/'",
        });
    };
    let mut segments = Vec::new();
    let mut cur = String::new();
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        match c {
            '/' => segments.push(std::mem::take(&mut cur)),
            '~' => match chars.next() {
                Some('0') => cur.push('~'),
                Some('1') => cur.push('/'),
                _ => {
                    return Err(MountError::InvalidPath {
                        path,
                        detail: "only ~0 and ~1 escapes are valid",
                    });
                }
            },
            c => cur.push(c),
        }
    }
    segments.push(cur);
    Ok(segments)
}

fn is_strict_prefix(prefix: &[String], of: &[String]) -> bool {
    prefix.len() < of.len() && of[..prefix.len()] == *prefix
}

/// 一次性捕获 + 校验声明表（#222 §2.1）：路径编译、重复解码指针、前缀冲突、
/// 条数可表示性。未通过 ⇒ 桥不被构造。
fn compile_declarations(src: &[BindingDecl]) -> Result<Vec<Decl>, MountError> {
    u32::try_from(src.len()).map_err(|_| MountError::TooManyBindings { count: src.len() })?;
    let mut decls: Vec<Decl> = Vec::with_capacity(src.len());
    for d in src {
        let segments = compile_pointer(d.path)?;
        for prev in &decls {
            if prev.segments == segments {
                return Err(MountError::DuplicatePath { path: d.path });
            }
            if is_strict_prefix(&prev.segments, &segments)
                || is_strict_prefix(&segments, &prev.segments)
            {
                return Err(MountError::PrefixCollision {
                    path: d.path,
                    other: prev.path,
                });
            }
        }
        decls.push(Decl {
            path: d.path,
            kind: d.kind,
            segments,
        });
    }
    Ok(decls)
}

/// 封板后的绑定表条目（只读）。`index` = `bindings()` 序（ViewMap 调用序）；
/// 表位置 = 绑定**登记**序（`BindingId`、last/脏位/区域条目寻址序）。
#[derive(Clone)]
struct Bound {
    index: usize,
    id: BindingId,
    path: &'static str,
    kind: PayloadTag,
    /// 装配期编译的解码段（#146：运行期不拆串、不解码转义）。
    segments: Vec<String>,
    /// 覆盖判定（装配期缓存：热路径零 `covers()` 采用方调用，#222 §2.1）。
    json_cover: bool,
    region_cover: bool,
}

/// 桥内折叠态（单锁成组：last/脏位/毒化/版本/交付游标）。
struct Core<V> {
    /// 装配期编译的声明表（`bind` 查找面）。
    decls: Arc<[Decl]>,
    /// 已登记绑定（`bind` 登记序 = `BindingId` 序）。
    registered: Vec<Bound>,
    /// `Some` = 已封板（`seal` 之后只读）。
    bound: Option<Arc<[Bound]>>,
    /// 封板进行中（禁并发 `bind` 挤进尚未投影的绑定）。
    sealing: bool,
    /// 已提交投影值**句柄**（#140 两阶段：锁内只搬 `Arc`，采用方 `Clone`/`Drop`
    /// 一律在锁外——退役句柄由调用方带出临界区后析构）。
    last: Vec<Option<Arc<V>>>,
    dirty: Vec<bool>,
    /// 每绑定脏代次（JSON 交付的清零令牌：只有捕获到的那一代仍是当前才清脏位，
    /// 编码期间竞态的新脏写留着）。
    dirty_rev: Vec<u64>,
    poisoned: bool,
    /// 已提交的 even 发布水位（与区域头 publish-sequence / `to_version` 同值）。
    version: u64,
    delivered_epoch: u64,
    /// 提交代次（每次提交 `last`/`version` 都 +1；两阶段提交的校验令牌）。
    commit_gen: u64,
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

/// 一段已编码载荷在批缓冲中的位置（`len` = 未填充长度；填充在背衬目标区直接补零）。
struct Seg {
    entry: usize,
    /// 未填充长度（Index `len` 字段；TLV 自描述，填充字节不可见）。
    len: u32,
    /// 段字节在批缓冲中的起点。
    at: usize,
}

/// 一批段字节：连续缓冲 + 段描述。编码直接写进 `bytes`（[`RegionWriter::with_buffer`]），
/// 故每绑定零临时分配、最终只拷一次进背衬；跨 publish 复用（段描述只有偏移，无采用方值）。
#[derive(Default)]
struct SegBatch {
    bytes: Vec<u8>,
    segs: Vec<Seg>,
}

impl SegBatch {
    /// 复位为「空批」（保留两块容量）。
    fn clear(&mut self) {
        self.bytes.clear();
        self.segs.clear();
    }
}

/// 段落位占宽（8 字节对齐；Index `len` 记未填充长度）。
fn seg_total(s: &Seg) -> usize {
    pad8(s.len as usize)
}

/// 一批段的总落位字节（溢出 `None`）。
fn segs_need(segs: &[Seg]) -> Option<usize> {
    let mut need = 0usize;
    for s in segs {
        need = need.checked_add(seg_total(s))?;
    }
    Some(need)
}

/// 一次发布事务的计划（**纯计算产物**，不含任何采用方调用）。
struct PublishPlan {
    region_id: u32,
    /// 新 committed even 序列。
    sequence: u64,
    entries: Vec<RegionEntry>,
    cursor: usize,
    /// `(arena 偏移, 批缓冲偏移, 未填充长度)`——填充字节在目标区直接补零。
    segments: Vec<(usize, usize, u32)>,
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

    /// 封板（**纯内存**）：条目定死 + 固定 `Header + Index` 容量校验。
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

    /// 封板事务计划：committed even = [`INITIAL_SEQ`]，**初始全量区域发布**
    /// （#155/#190）：已登记绑定的现值各占一段，不等后续变更——Region-only 部署
    /// 封板即可用。放不下 ⇒ typed `CapacityExceeded`（封板失败，不是 arena-full）。
    fn plan_initial(&self, segs: &[Seg]) -> Result<PublishPlan, RegionFailure> {
        let need = segs_need(segs).ok_or(RegionFailure::CapacityExceeded {
            required: usize::MAX,
            capacity: self.capacity,
            region_id: self.region_id,
        })?;
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
        self.place(segs, INITIAL_SEQ, true, self.arena_start)
            .ok_or(RegionFailure::GenerationExhausted)
    }

    /// 增量计划；`None` = arena 不足（调用方先做全量重编，再走 [`Self::plan_reseed`]）。
    fn plan_incremental(&self, segs: &[Seg], sequence: u64) -> Option<PublishPlan> {
        let end = self.cursor.checked_add(segs_need(segs)?)?;
        if end > self.capacity {
            return None;
        }
        self.place(segs, sequence, false, self.cursor)
    }

    /// 全量重编计划（换区）：先算总编码大小再动任何 live 状态；放不下 ⇒ typed
    /// `CapacityExceeded` 且**上一份已知良好元数据保持有效**（#172）。
    fn plan_reseed(&self, segs: &[Seg], sequence: u64) -> Result<PublishPlan, RegionFailure> {
        let need = segs_need(segs).ok_or(RegionFailure::CapacityExceeded {
            required: usize::MAX,
            capacity: self.capacity,
            region_id: self.region_id,
        })?;
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
            segments.push((cursor, s.at, s.len));
            cursor = cursor.checked_add(seg_total(s))?;
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
fn execute_plan(
    backing: &dyn RegionBacking,
    plan: &PublishPlan,
    batch: &[u8],
) -> Result<(), RegionFailure> {
    // even 序列 ⇒ odd = seq - 1。计划只产出 even（禁把 odd 当已提交版本）。
    let odd = plan
        .sequence
        .checked_sub(1)
        .filter(|s| s % 2 == 1)
        .ok_or(RegionFailure::GenerationExhausted)?;
    store_seq(backing, odd)?;

    // ① 段字节（按计划偏移整段写入 + 目标区直接补零，禁部分段）。`batch` 是本批段
    // 字节的连续缓冲：每段只拷一次未填充载荷，填充字节在目标区就地清零。
    if !plan.segments.is_empty() {
        let mut short: Option<(usize, usize)> = None;
        match catch_unwind(AssertUnwindSafe(|| {
            backing.with_bytes_mut(&mut |buf| {
                for (off, at, len) in &plan.segments {
                    let len = *len as usize;
                    let Some(end) = off.checked_add(pad8(len)) else {
                        short = Some((usize::MAX, buf.len()));
                        return;
                    };
                    let Some(src) = at
                        .checked_add(len)
                        .and_then(|end_at| batch.get(*at..end_at))
                    else {
                        short = Some((end, buf.len()));
                        return;
                    };
                    match buf.get_mut(*off..end) {
                        Some(slot) => {
                            slot[..len].copy_from_slice(src);
                            slot[len..].fill(0);
                        }
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

/// 两阶段提交的校验重试上限（期间 `repair`/`seal` 提交过才用得上；有界，禁自旋）。
const COMMIT_ATTEMPTS: u32 = 3;

/// 通知路径 scratch（跨通知复用容量；单个排空回调**独占**持有，见
/// [`Notifier::take_notify`]）。只装句柄与容量，无采用方值语义。
///
/// 归还前必须在**锁外** `recycle()`：丢弃的 `Arc` 会触发采用方 `Drop`，禁在桥内锁里跑。
struct NotifyScratch<V> {
    /// 本拍投影结果 `(绑定表位置, 值句柄)`。
    pending: Vec<(usize, Arc<V>)>,
    /// 本拍真正变更的项（两阶段比较后 latest-wins）。
    changed: Vec<(usize, Arc<V>)>,
    /// 被换出的旧值句柄（锁外析构）。
    retired: Vec<Arc<V>>,
    /// `last` 句柄快照（只读比较面）。
    prev: Vec<Option<Arc<V>>>,
}

// 手写 Default：`derive` 会额外要求 `V: Default`（空 Vec 不需要）。
impl<V> Default for NotifyScratch<V> {
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            changed: Vec::new(),
            retired: Vec::new(),
            prev: Vec::new(),
        }
    }
}

impl<V> NotifyScratch<V> {
    /// 锁外丢弃全部句柄。
    fn recycle(&mut self) {
        self.pending.clear();
        self.changed.clear();
        self.retired.clear();
        self.prev.clear();
    }
}

/// 一条通知路径的全部 scratch（单个排空回调独占，见 [`Notifier::take_scratch`]）：
/// 投影/提交句柄表 + 区域发布批字节。`batch` 与通知共用一槽，故一次取/还即够。
struct Scratch<V> {
    notify: NotifyScratch<V>,
    batch: SegBatch,
}

// 手写 Default：`derive` 会额外要求 `V: Default`（空 Vec 不需要）。
impl<V> Default for Scratch<V> {
    fn default() -> Self {
        Self {
            notify: NotifyScratch::default(),
            batch: SegBatch::default(),
        }
    }
}

/// 一个待交付脏项：（绑定位置, 脏代次, 值句柄）。
type DirtyItem<V> = (usize, u64, Option<Arc<V>>);

/// 通知体共享态（订阅闭包捕获 `Arc<Self>`；不含 store/票据 ⇒ 无环）。
struct Notifier<S, M: ViewMap<S>, J: JsonEncoder, R: RegionEncoder> {
    map: Arc<M>,
    json: Option<Arc<J>>,
    region: Mutex<Option<RegionChannel<R>>>,
    core: Mutex<Core<M::View>>,
    /// 两编码器的覆盖词表（装配期求值一次，此后逐绑定查缓存，热路径零调用）。
    json_covers: Option<Coverage>,
    region_covers: Option<Coverage>,
    projection_runs: AtomicU64,
    projection_panics: AtomicU64,
    region_encodes: AtomicU64,
    region_ver: AtomicU64,
    region_id_cur: AtomicU32,
    /// 当前 unavailable 原因（`None` = ready；成功提交/重编后清空）。
    region_failure: Mutex<Option<RegionFailure>>,
    /// 失败累计（含瞬时失败，如 `PublishBusy`）。
    region_failures: AtomicU64,
    /// 封板前到达的通知（装配纪律违约：可见计数，不投影不发布）。
    pre_seal_notifications: AtomicU64,
    /// 通知 + 发布 scratch（独占式复用：取不到 = 并发通知在跑，临时新建）。
    scratch: Mutex<Option<Scratch<M::View>>>,
    _s: std::marker::PhantomData<fn(&S)>,
}

impl<S, M, J, R> Notifier<S, M, J, R>
where
    M: ViewMap<S> + Send + Sync + 'static,
    J: JsonEncoder<View = M::View> + Send + Sync + 'static,
    R: RegionEncoder<View = M::View> + Send + Sync + 'static,
{
    /// 已封板绑定表（`None` = 未封板——装配纪律要求先 `seal()`）。
    fn sealed_bound(&self) -> Option<Arc<[Bound]>> {
        lock(&self.core).bound.as_ref().map(Arc::clone)
    }

    /// 取本拍 scratch（独占；并发通知取不到 ⇒ 新建空的，行为不变只是多分配）。
    fn take_scratch(&self) -> Scratch<M::View> {
        lock(&self.scratch).take().unwrap_or_default()
    }

    /// 归还本拍 scratch：先在**锁外**丢句柄（采用方 `Drop` 不落锁内），再入槽。
    fn put_scratch(&self, mut scratch: Scratch<M::View>) {
        scratch.notify.recycle();
        *lock(&self.scratch) = Some(scratch);
    }

    /// 初始区域发布（#155/#190，`core` 锁外）：建表（纯内存）→ **全量**初始
    /// 段落位 → 一次事务提交。水位 = 封板 even（[`INITIAL_SEQ`]）。
    fn seal_region(
        &self,
        bound: &[Bound],
        items: &[(usize, Arc<M::View>)],
    ) -> Result<(), RegionFailure> {
        let Some(encoder) = self.region_encoder() else {
            return Ok(()); // 未挂区域通道
        };
        let mut batch = SegBatch::default();
        if let Err(f) = self.encode_into(encoder.as_ref(), bound, items, &mut batch) {
            self.fail_region(f.clone(), false);
            return Err(f);
        }
        let plan = {
            let mut rg = lock(&self.region);
            let Some(ch) = rg.as_mut() else {
                return Ok(());
            };
            let outcome = ch
                .begin_seal(bound)
                .and_then(|()| ch.plan_initial(&batch.segs));
            match outcome {
                Ok(p) => p,
                Err(f) => {
                    drop(rg);
                    self.fail_region(f.clone(), true);
                    return Err(f);
                }
            }
        };
        self.region_encodes
            .fetch_add(batch.segs.len() as u64, Ordering::Relaxed);
        self.publish_plan(plan, &batch.bytes);
        match lock(&self.region_failure).clone() {
            None => Ok(()),
            Some(f) => Err(f),
        }
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

    /// 逐绑定编码（采用方代码，锁外）：每绑定恰一段非空完整 TLV（#168/#185），
    /// 宽度/有限性由 writer 自报（#141/#153）。覆盖判定吃装配期缓存。
    ///
    /// 段字节**直接写进批缓冲**（`batch.bytes`，跨 publish 复用）：每绑定零临时分配，
    /// 最终由 [`execute_plan`] 一次拷入背衬。`batch` 由本调用独占持有。
    fn encode_into(
        &self,
        encoder: &R,
        bound: &[Bound],
        items: &[(usize, Arc<M::View>)],
        batch: &mut SegBatch,
    ) -> Result<(), RegionFailure> {
        batch.clear();
        let mut writer = RegionWriter::with_buffer(std::mem::take(&mut batch.bytes));
        for (bi, view) in items {
            if !bound[*bi].region_cover {
                continue;
            }
            writer.begin_segment();
            if catch_unwind(AssertUnwindSafe(|| encoder.encode(view, &mut writer))).is_err() {
                batch.bytes = writer.into_buffer();
                return Err(RegionFailure::EncoderPanicked {
                    path: bound[*bi].path,
                });
            }
            let seg = writer.as_slice();
            let fault = writer.fault().or_else(|| validate_single_tlv(seg).err());
            let len = u32::try_from(seg.len()).ok();
            if let Some(fault) = fault {
                batch.bytes = writer.into_buffer();
                return Err(RegionFailure::EncoderOutput {
                    path: bound[*bi].path,
                    fault,
                });
            }
            let Some(len) = len else {
                batch.bytes = writer.into_buffer();
                return Err(RegionFailure::EncoderOutput {
                    path: bound[*bi].path,
                    fault: WriterFault::PayloadTooLong,
                });
            };
            batch.segs.push(Seg {
                entry: *bi,
                len,
                at: writer.segment_start(),
            });
        }
        batch.bytes = writer.into_buffer();
        Ok(())
    }

    /// 区域增量发布：**锁外编码 → 锁内计划 → 锁外事务 → 锁内提交**。
    /// `changed` = `(绑定表位置, 视图值)`。
    fn publish_changes(
        &self,
        bound: &[Bound],
        changed: &[(usize, Arc<M::View>)],
        sequence: u64,
        batch: &mut SegBatch,
    ) {
        let Some(encoder) = self.region_encoder() else {
            return;
        };
        if !self.region_ready() {
            return;
        }
        if let Err(f) = self.encode_into(encoder.as_ref(), bound, changed, batch) {
            return self.fail_region(f, false);
        }
        // 计划：锁内纯算术（无采用方调用）。
        let plan = {
            let rg = lock(&self.region);
            let Some(ch) = rg.as_ref() else {
                return;
            };
            match ch.plan_incremental(&batch.segs, sequence) {
                Some(p) => p,
                // 增量放不下 ⇒ 全量重编（先算总大小再动 live 状态；同一批缓冲重编）。
                None => {
                    drop(rg);
                    return self.reseed_with(bound, encoder.as_ref(), sequence, batch);
                }
            }
        };
        self.region_encodes
            .fetch_add(batch.segs.len() as u64, Ordering::Relaxed);
        self.publish_plan(plan, &batch.bytes);
    }

    /// 全量重编 + 换区（增量耗尽路径 / `reseed_region` 恢复口）。`batch` 由调用方
    /// 独占持有——重编复用同一块批缓冲，容量不丢。
    fn reseed_with(&self, bound: &[Bound], encoder: &R, sequence: u64, batch: &mut SegBatch) {
        let items = self.full_snapshot(bound);
        if let Err(f) = self.encode_into(encoder, bound, &items, batch) {
            return self.fail_region(f, false);
        }
        let plan = {
            let rg = lock(&self.region);
            let Some(ch) = rg.as_ref() else {
                return;
            };
            match ch.plan_reseed(&batch.segs, sequence) {
                Ok(p) => p,
                // 放不下 ⇒ 上一份已知良好元数据保持有效，失败可见（不再 debug_assert）。
                Err(f) => {
                    drop(rg);
                    return self.fail_region(f, false);
                }
            }
        };
        self.publish_plan(plan, &batch.bytes);
    }

    /// 现态全量视图快照（core 锁内只搬 `Arc` 句柄，零采用方 `Clone`；未发布过的
    /// 绑定缺席）。
    fn full_snapshot(&self, bound: &[Bound]) -> Vec<(usize, Arc<M::View>)> {
        let core = lock(&self.core);
        bound
            .iter()
            .enumerate()
            .filter_map(|(bi, _)| core.last[bi].as_ref().map(|v| (bi, Arc::clone(v))))
            .collect()
    }

    /// 执行 + 提交（唯一入口：先锁外事务，成功才锁内换镜像并对外宣称新水位）。
    /// `publishing` 旗提供并发可见失败（`PublishBusy`）而不是死锁——它**不是**
    /// 互斥量：采用方代码（encode/backing）在锁外执行。
    fn publish_plan(&self, plan: PublishPlan, batch: &[u8]) {
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
        let outcome = execute_plan(backing.as_ref(), &plan, batch);
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

    /// 通知体（dispatch 线程同步）。封板前到达 = 装配纪律违约：只计数不投影
    /// （禁静默吞吐，也禁事后补投影掩盖边界不一致）。
    ///
    /// **最后一道闸**：任何采用方 panic（含本体内各 `Drop` 点：退役句柄、句柄表快照）
    /// 都不得穿出通知体——穿出会被 store 按「订阅者 panic」摘除整条桥订阅，桥此后
    /// 永远收不到通知而其失败面（`resync`/`projection_panics`）全不动。此处兜底只做
    /// 计数 + 毒化，让失败可见而不失联。
    fn on_notify(&self, state: &S) {
        if catch_unwind(AssertUnwindSafe(|| self.dispatch_notify(state))).is_err() {
            self.projection_panics.fetch_add(1, Ordering::Relaxed);
            lock(&self.core).poisoned = true;
        }
    }

    /// 通知体本体（两阶段提交见 [`Self::commit_two_phase`]）。
    ///
    /// 投影/提交/发布 scratch 由本帧**独占**（[`Self::take_scratch`]）：并发通知取不到
    /// 就新建一个（行为不变，只是多分配），故不需要别名证明，也没有 unsafe。取用是
    /// **惰性**的——哨兵全报未变的快路径不取、不放，一次锁都不多加。
    fn dispatch_notify(&self, state: &S) {
        let Some(bound) = self.sealed_bound() else {
            self.pre_seal_notifications.fetch_add(1, Ordering::Relaxed);
            return;
        };
        // 首个变更落地时才取 scratch（未变更快路径零分配、零新增取锁）；本帧全权拥有，
        // 收尾（含未提交的早退）在这里统一归还。
        let mut scratch: Option<Scratch<M::View>> = None;
        let sequence = self.project_into(state, &bound, &mut scratch);
        if let Some(sequence) = sequence {
            match scratch.as_mut() {
                Some(Scratch { notify, batch }) => {
                    // eager 区域编码 + 事务（锁外编码；空批也推进头序列，跨通道同源）。
                    self.publish_changes(&bound, &notify.changed, sequence, batch);
                }
                None => {
                    // 未变更拍的空批发布：零段 ⇒ 空批 scratch 不分配。
                    let mut empty = SegBatch::default();
                    self.publish_changes(&bound, &[], sequence, &mut empty);
                }
            }
        }
        if let Some(scratch) = scratch {
            self.put_scratch(scratch);
        }
    }

    /// 锁外投影（哨兵粗闸 → 单点投影，panic catch：三态提交唯一缺口）→ 提交；返回本拍
    /// 新 even 水位（`None` = 未提交且已毒化）。scratch 经 `scratch` 出入（取用与否由本
    /// 函数决定，归还归调用方）。
    fn project_into(
        &self,
        state: &S,
        bound: &[Bound],
        scratch: &mut Option<Scratch<M::View>>,
    ) -> Option<u64> {
        let mut panicked = false;
        for (bi, b) in bound.iter().enumerate() {
            // #152/#189：`input_changed` 与 `project` 同为**采用方代码**，panic 禁穿出
            // `on_notify`——否则 `SyncStore` 按「订阅者 panic」摘除整条桥订阅，桥从此
            // 静默不再收更新。判不出「未变」⇒ 按已变更 fail-visible 投影 + 计数 + 毒化
            // （与 project panic 同口径；禁把不确定性吞成「未变」而丢一拍）。
            let sentinel_changed =
                match catch_unwind(AssertUnwindSafe(|| self.map.input_changed(b.index, state))) {
                    Ok(changed) => changed,
                    Err(_) => {
                        self.projection_panics.fetch_add(1, Ordering::Relaxed);
                        panicked = true;
                        true
                    }
                };
            if !sentinel_changed {
                continue;
            }
            self.projection_runs.fetch_add(1, Ordering::Relaxed);
            match catch_unwind(AssertUnwindSafe(|| self.map.project(b.index, state))) {
                Ok(v) => scratch
                    .get_or_insert_with(|| self.take_scratch())
                    .notify
                    .pending
                    .push((bi, Arc::new(v))),
                Err(_) => {
                    self.projection_panics.fetch_add(1, Ordering::Relaxed);
                    panicked = true;
                }
            }
        }
        let Some(scratch) = scratch.as_mut() else {
            // 未变更快路径：只有一次 `core` 锁、零分配、零新增取锁。
            return self.advance_uncommitted(panicked);
        };
        self.commit_two_phase(&mut scratch.notify, panicked)
    }

    /// 未变更快路径的水位推进（锁内纯记账：无采用方代码、零分配）。
    ///
    /// **不推进 `commit_gen`**：提交令牌的含义是「`last` 自快照以来是否被换过」，本路径
    /// 不动 `last`，推进令牌只会让并发的变更提交白白重来（#169/#174 一旦放开并发通知，
    /// 就成了「重试轮次被无谓耗尽 ⇒ 整批丢弃 + 毒化」的放大器）。前置条件由此固定：
    /// **只有真正改写 `last` 的提交才 +1 令牌**。
    fn advance_uncommitted(&self, panicked: bool) -> Option<u64> {
        let mut core = lock(&self.core);
        let Some(next) = core.version.checked_add(SEQ_STEP) else {
            // 代次耗尽：typed/可见降级（禁 wrapping 复用旧代次）。
            core.poisoned = true;
            return None;
        };
        core.version = next;
        if panicked {
            core.poisoned = true;
        }
        Some(next)
    }

    /// 变更路径的两阶段提交：锁内捕获 `Arc` 句柄快照 + 提交代次 → **锁外**跑采用方
    /// `PartialEq` → 锁内校验代次后只换变更槽（latest-wins），退役旧句柄带出临界区
    /// 析构。快照只做原子计数，不跑 `View::clone`。
    ///
    /// 校验失败（期间 `repair`/`seal` 提交过 ⇒ 快照与现态不配）⇒ 重取快照重来，共
    /// [`COMMIT_ATTEMPTS`] 轮；仍不配 ⇒ **不提交** + 毒化可见：已提交值可能更新于本次
    /// 投影，禁以更旧值覆盖它（毒化强制 `snapshot_doc` 重同步，可见而非静默）。
    ///
    /// **采用方 `panic` 一律在桥边界 contain（#140 复审 P0）**：`PartialEq` 抛错 ⇒ 本批
    /// 不提交、`projection_panics` +1、毒化（经 `resync` / `SnapshotError::Incomplete`
    /// 可见）；退役值 `Drop` 抛错 ⇒ 提交已成立但同样计数 + 毒化。两者**都不得穿出**
    /// `on_notify`——穿出会被 store 按「订阅者 panic」摘除订阅，桥此后永远收不到通知。
    fn commit_two_phase(
        &self,
        scratch: &mut NotifyScratch<M::View>,
        panicked: bool,
    ) -> Option<u64> {
        // 投影结果与 `changed` 同住 scratch：移出以分离可变/不可变借用（零分配）。
        let pending = std::mem::take(&mut scratch.pending);
        let out = self.commit_attempts(&pending, scratch, panicked);
        // 句柄的析构留给归还时的锁外 `recycle`（不在任何桥内锁里跑采用方 Drop）。
        scratch.pending = pending;
        out
    }

    /// 有界提交本体（协议见 [`Self::commit_two_phase`]）。
    fn commit_attempts(
        &self,
        pending: &[(usize, Arc<M::View>)],
        scratch: &mut NotifyScratch<M::View>,
        panicked: bool,
    ) -> Option<u64> {
        let NotifyScratch {
            changed,
            retired,
            prev,
            ..
        } = scratch;
        for _ in 0..COMMIT_ATTEMPTS {
            // 上一轮快照的句柄先在此（锁外）丢；锁内只搬句柄快照 + 取代次令牌。
            prev.clear();
            let token = {
                let core = lock(&self.core);
                prev.clone_from(&core.last);
                core.commit_gen
            };
            // 锁外：采用方 `PartialEq`（句柄读只做原子计数）。
            changed.clear();
            let compared = catch_unwind(AssertUnwindSafe(|| {
                for (bi, v) in pending {
                    if prev[*bi].as_deref() != Some(&**v) {
                        changed.push((*bi, Arc::clone(v)));
                    }
                }
            }));
            if compared.is_err() {
                // 判不出「变没变」⇒ 禁当未变（静默陈旧），也禁提交半批：不推进水位，
                // 只显形（计数 + 毒化 ⇒ 下一批 resync，UI 走 snapshot_doc 重同步）。
                self.projection_panics.fetch_add(1, Ordering::Relaxed);
                lock(&self.core).poisoned = true;
                return None;
            }
            let mut core = lock(&self.core);
            if core.commit_gen != token {
                drop(core);
                continue;
            }
            let Some(next) = core.version.checked_add(SEQ_STEP) else {
                core.poisoned = true;
                return None;
            };
            core.version = next;
            core.commit_gen = core.commit_gen.wrapping_add(1);
            if panicked {
                core.poisoned = true;
            }
            for (bi, v) in changed.iter() {
                core.dirty[*bi] = true;
                core.dirty_rev[*bi] = core.dirty_rev[*bi].wrapping_add(1);
                if let Some(old) = core.last[*bi].replace(Arc::clone(v)) {
                    retired.push(old);
                }
            }
            drop(core);
            // 退役值在**锁外**析构（采用方 `Drop`）；析构 panic 同样 contain：提交已成立，
            // 但必须计数 + 毒化让「该批的完整性」在 resync 上可见。
            if catch_unwind(AssertUnwindSafe(|| retired.clear())).is_err() {
                self.projection_panics.fetch_add(1, Ordering::Relaxed);
                lock(&self.core).poisoned = true;
            }
            return Some(next);
        }
        lock(&self.core).poisoned = true;
        None
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

/// 快照完整性判据（#104 / #222 §6）：每个槽位都有已知良好投影且未毒化。
/// 不完整 ⇒ 走 `repair`，不许把部分树当完整快照交付。
fn bridge_complete<V>(core: &Core<V>) -> bool {
    !core.poisoned && core.last.iter().all(Option::is_some)
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

/// 语言边界视图桥（ADR 0006 参考机器）。装配纪律（#222 §B）：**mount（fallible）
/// → 全部 `bind` → `seal()` 完成后才允许 dispatch**（`seal` 封板绑定表并提交初始
/// 已提交视图 + 初始区域全量）。装配后句柄可跨线程共享（Send + Sync）；票据归桥，
/// 桥 Drop = 注销订阅；store 所有权随桥（`snapshot_doc` 修复路径的现态源），桥
/// Drop ⇒ store 随桥回收（上行句柄经 `send_intent` 透传闭门 false）。
///
/// # 采用方代码零锁内（#140 / #222 §3）
///
/// 已提交投影值以 [`Arc`] 句柄存放：锁内只搬句柄与代次，采用方
/// `JsonEncoder::{covers,to_json}`、`RegionEncoder::{covers,encode}`、
/// `RegionBacking::*` 与 `M::View` 的 `PartialEq`/`Clone`/`Drop` **一律不在桥内互斥量
/// 下执行**（两阶段：锁内捕获 → 锁外运行 → 锁内校验令牌提交；退役值在锁外析构）。
/// 两个 `covers()` 只在 `mount`（桥尚不存在的装配期）各求值一次并缓存，运行期零调用。
///
/// **门槛**（本桥新增，[`ViewMap`] 自身约束不变）：`M::View` 必须 `Send + Sync`。
/// 这是「句柄快照」的直接后果——`Arc<V>: Send` 要求 `V: Send + Sync`，而桥跨线程
/// 共享（`Sync` 订阅回调）。与 #157 机制胞同质的编译期门槛；`View` 若含
/// `Cell`/`RefCell`/裸指针包装（`Send + !Sync`）则无法挂在桥上。
///
/// **锁序**：`core` 与 `region` 从不同时持有（每次获取都是叶锁）；唯一嵌套是
/// `region_failure → region`。`bind` 只在 `core.bound.is_none()` 时继续，封板后
/// 直接 `Err(BindError::Sealed)` ⇒ `core→region`（bind/seal）与 `region→core`
/// （旧 fallback）被封板状态互斥。**封板后禁止 bind；若未来允许 sealed 后 bind 或
/// 改变封板顺序，必须重审此锁序**（#140 评论 7153375286 的状态机前提）。
pub struct ViewBridge<S, A, M, I, J, R>
where
    M: ViewMap<S>,
    M::View: Send + Sync,
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
    M::View: Send + Sync,
    I: IntentMap<A, Wire = Value, Err = IntentError> + Send + Sync + 'static,
    J: JsonEncoder<View = M::View> + Send + Sync + 'static,
    R: RegionEncoder<View = M::View> + Send + Sync + 'static,
{
    /// 装配（**fallible**，#222 §2.1/§B）：一次性捕获并编译 `ViewMap::bindings()`
    /// 声明（路径语法/重复解码指针/前缀冲突/条数可表示性），然后 `store.subscribe`
    /// 挂桥（票据归桥所有，桥 Drop = 注销）。声明非法 ⇒ `Err`，不泄漏订阅、不暴露
    /// 半装配句柄。背衬不支持对齐原子发布序列 ⇒ 区域通道装配期即 unavailable
    /// （lock-free 模式被拒绝；`region_failure()` 显形），JSON 通道不受影响。
    ///
    /// **并发前置条件**：`mount` → `bind` → [`Self::seal`] 必须在任何 dispatch 之前
    /// 完成（同线程装配）——封板前的通知只计数不投影。
    pub fn mount<T>(
        store: T,
        map: M,
        intents: I,
        encoders: Encoders<J, R>,
    ) -> Result<Self, MountError>
    where
        T: Store<State = S, Action = A> + Send + Sync + 'static,
    {
        let decls = match catch_unwind(AssertUnwindSafe(|| map.bindings())) {
            Ok(src) => compile_declarations(src)?,
            Err(_) => return Err(MountError::DeclarationsPanicked),
        };
        let json_covers = match encoders.json.as_ref() {
            Some(j) => match catch_unwind(AssertUnwindSafe(|| j.covers())) {
                Ok(c) => Some(c),
                Err(_) => return Err(MountError::CoveragePanicked { channel: "json" }),
            },
            None => None,
        };
        let region_covers = match encoders.region.as_ref() {
            Some((r, _)) => match catch_unwind(AssertUnwindSafe(|| r.covers())) {
                Ok(c) => Some(c),
                Err(_) => return Err(MountError::CoveragePanicked { channel: "region" }),
            },
            None => None,
        };
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
                decls: decls.into(),
                registered: Vec::new(),
                bound: None,
                sealing: false,
                last: Vec::new(),
                dirty: Vec::new(),
                dirty_rev: Vec::new(),
                poisoned: false,
                version: 0,
                delivered_epoch: 0,
                commit_gen: 0,
            }),
            json_covers,
            region_covers,
            projection_runs: AtomicU64::new(0),
            projection_panics: AtomicU64::new(0),
            region_encodes: AtomicU64::new(0),
            region_ver: AtomicU64::new(0),
            region_id_cur: AtomicU32::new(0),
            region_failure: Mutex::new(initial_failure),
            region_failures: AtomicU64::new(0),
            pre_seal_notifications: AtomicU64::new(0),
            scratch: Mutex::new(None),
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
        Ok(Self {
            inner,
            _ticket: ticket,
        })
    }

    /// 绑定注册（**装配期专用**）：`BindingId` = **登记序**（不是 `bindings()` 序），
    /// 未登记的声明不投影、不编码、不发布；重复 bind 同路径幂等返回既有 ID；未封板
    /// 前失败**不占 ID、不改登记态**。路径不被任何已配置编码器覆盖 ⇒ 全构建
    /// typed `Err(UncoveredBinding)`（#173）。封板后显式 `Err(BindError::Sealed)`。
    pub fn bind(&self, path: &str) -> Result<BindingId, BindError> {
        let notify = &self.inner.notify;
        let mut core = lock(&notify.core);
        if core.bound.is_some() || core.sealing {
            return Err(BindError::Sealed);
        }
        let Some(index) = core.decls.iter().position(|d| d.path == path) else {
            return Err(BindError::UnknownPath {
                path: path.to_string(),
            });
        };
        // 覆盖判定吃装配期缓存（#222 §2.1：热路径零 `covers()` 调用）。
        let json_cover = notify.json_covers.is_some_and(|c| c.covers(path));
        let region_cover = notify.region_covers.is_some_and(|c| c.covers(path));
        if !json_cover && !region_cover {
            return Err(BindError::UncoveredBinding {
                path: path.to_string(),
            });
        }
        if let Some(seen) = core.registered.iter().find(|b| b.index == index) {
            return Ok(seen.id); // 幂等
        }
        let decl = core.decls[index].clone();
        // mount 已校验声明数 ≤ u32::MAX，登记集是声明集的子集 ⇒ 不会截断。
        let id = BindingId(core.registered.len() as u32);
        core.registered.push(Bound {
            index,
            id,
            path: decl.path,
            kind: decl.kind,
            segments: decl.segments,
            json_cover,
            region_cover,
        });
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
        // ① 锁内捕获：绑定表 + 每个脏绑定的（位置, 脏代次, 值句柄）+ 水位/游标。
        // 句柄化 ⇒ 锁内零采用方代码（`to_json` 一律在锁外，#140）。
        let (bound, due, resync, from, to) = {
            let core = lock(&notify.core);
            let bound = match &core.bound {
                Some(b) => Arc::clone(b),
                None => return empty_batch(0),
            };
            let capture_views = notify.json.is_some();
            let mut due: Vec<DirtyItem<M::View>> = Vec::new();
            for bi in 0..core.dirty.len() {
                if !core.dirty[bi] {
                    continue;
                }
                let view = if capture_views {
                    core.last[bi].clone()
                } else {
                    None
                };
                due.push((bi, core.dirty_rev[bi], view));
            }
            (
                bound,
                due,
                core.poisoned,
                core.delivered_epoch,
                core.version,
            )
        };
        // ② 锁外：逐项编码（采用方代码，panic 在桥边界 contain）。
        let mut patches = Vec::with_capacity(due.len());
        if let Some(j) = &notify.json {
            for (bi, _, view) in &due {
                let b = &bound[*bi];
                let Some(view) = view else { continue };
                if !b.json_cover {
                    continue;
                }
                match catch_unwind(AssertUnwindSafe(|| j.to_json(view))) {
                    Ok(value) => patches.push(Patch {
                        path: b.path,
                        kind: b.kind,
                        value,
                    }),
                    Err(_) => {
                        // 采用方 panic ⇒ **整批原子失败**：不交付半成品、不清脏位、
                        // 不推进交付游标；毒化闩经 `resync` 显形（UI 走 snapshot_doc）。
                        lock(&notify.core).poisoned = true;
                        return PatchBatch {
                            patches: Vec::new(),
                            resync: true,
                            from_version: from,
                            to_version: from,
                            protocol_version: PROTOCOL_VERSION,
                        };
                    }
                }
            }
        }
        // ③ 锁内提交：只清**捕获到的那一代**脏位（编码期间竞态的新脏写留着）；交付游标
        // **只前进不后退**（并发拉取时不得把更早捕获的水位写回，否则 `from_version` 回退）。
        let mut core = lock(&notify.core);
        for (bi, rev, _) in &due {
            if core.dirty_rev[*bi] == *rev {
                core.dirty[*bi] = false;
            }
        }
        core.delivered_epoch = core.delivered_epoch.max(to);
        drop(core);
        PatchBatch {
            patches,
            resync,
            from_version: from,
            to_version: to,
            protocol_version: PROTOCOL_VERSION,
        }
    }

    /// 显式封板（#222 §B）：捕获**一次**稳定 Store 快照 → 投影全部已登记绑定 →
    /// 提交初始已提交视图（水位 [`INITIAL_SEQ`]）→ 发布初始区域**全量**
    /// （#155/#190：Region-only 部署封板即可用，不等后续变更）。
    ///
    /// 幂等：已封板再调 = `Ok(())`。初始投影 panic 被 contain（`projection_panics`
    /// 计数 + 毒化 ⇒ 快照走修复路径），仍算封板成功；区域事务失败返回
    /// [`SealError::Region`]——桥仍已封板、JSON 通道可用，区域通道转 unavailable
    /// 直到显式 [`Self::reseed_region`]。
    pub fn seal(&self) -> Result<(), SealError> {
        let notify = &self.inner.notify;
        let bound: Arc<[Bound]> = {
            let mut core = lock(&notify.core);
            if core.bound.is_some() {
                return Ok(());
            }
            core.sealing = true; // 关掉 bind 面，防止封板期间的登记漏投影
            core.registered.clone().into()
        };
        let state = match catch_unwind(AssertUnwindSafe(|| self.inner.store.snapshot())) {
            Ok(state) => state,
            Err(_) => {
                lock(&notify.core).sealing = false;
                return Err(SealError::SnapshotPanicked);
            }
        };
        let mut views: Vec<Option<Arc<M::View>>> = Vec::with_capacity(bound.len());
        let mut panicked = false;
        for b in bound.iter() {
            notify.projection_runs.fetch_add(1, Ordering::Relaxed);
            match catch_unwind(AssertUnwindSafe(|| notify.map.project(b.index, &state))) {
                Ok(v) => views.push(Some(Arc::new(v))),
                Err(_) => {
                    notify.projection_panics.fetch_add(1, Ordering::Relaxed);
                    panicked = true;
                    views.push(None);
                }
            }
        }
        // 区域初始事务的输入在锁外取（只搬句柄，不跑采用方 `Clone`）。
        let items: Vec<(usize, Arc<M::View>)> = views
            .iter()
            .enumerate()
            .filter_map(|(bi, v)| v.as_ref().map(|v| (bi, Arc::clone(v))))
            .collect();
        {
            let mut core = lock(&notify.core);
            // 与 `repair` 同款：旧表句柄带出临界区析构（采用方 `Drop` 不在锁内）。
            let retired = std::mem::replace(&mut core.last, views);
            core.dirty = vec![false; bound.len()];
            core.dirty_rev = vec![0; bound.len()];
            core.poisoned = panicked;
            core.version = INITIAL_SEQ;
            core.commit_gen = core.commit_gen.wrapping_add(1);
            core.bound = Some(Arc::clone(&bound));
            core.sealing = false;
            drop(core);
            drop(retired);
        }
        notify
            .seal_region(&bound, &items)
            .map_err(SealError::Region)
    }

    /// 重同步原语（#104）：**从已提交投影值 + 同一临界区捕获的桥水位**构造文档，
    /// 不再独立采样 Store 后套新版本。只有「有绑定无已知良好值 / 毒化」时才走
    /// 版本校验下的重投影修复。快照即交付：`delivered_epoch` 推进至该水位 ⇒
    /// 重同步后下一批 `from_version == snapshot.version`。
    ///
    /// 不完整（修复后仍有缺失/仍毒化）⇒ [`SnapshotError::Incomplete`]，禁把部分
    /// 树当完整快照交付。
    ///
    /// **判定与捕获同住一个临界区（#140 复审 D1）**：`bridge_complete` 与
    /// `(last, version, dirty_rev)` 在同一次取锁内取，两段之间不存在「他人 `repair`
    /// 让本次快照掉槽」的窗口；`repair` 之后的再判定同样在同一临界区内做（有界两轮）。
    pub fn snapshot_doc(&self) -> Result<Snapshot, SnapshotError> {
        let notify = &self.inner.notify;
        for _ in 0..2 {
            // ① 判定 + 捕获（同一临界区，锁内零采用方代码）。
            let (bound, complete, views, version, revs) = {
                let core = lock(&notify.core);
                let Some(bound) = &core.bound else {
                    return Err(SnapshotError::Unsealed);
                };
                (
                    Arc::clone(bound),
                    bridge_complete(&core),
                    core.last.clone(),
                    core.version,
                    core.dirty_rev.clone(),
                )
            };
            if !complete {
                self.repair(&bound)?;
                continue; // 修复后重新在同一临界区判定 + 捕获
            }
            // ② 锁外：逐叶 `to_json` 组装（采用方代码，panic 在桥边界 contain）。
            let doc = match &notify.json {
                Some(j) => {
                    match catch_unwind(AssertUnwindSafe(|| {
                        assemble_doc(&bound, &views, j.as_ref())
                    })) {
                        Ok(doc) => doc,
                        Err(_) => {
                            // 失败快照不清脏位、不推进交付基线；毒化闩显形（#140 §3.3）。
                            lock(&notify.core).poisoned = true;
                            return Err(SnapshotError::Incomplete);
                        }
                    }
                }
                None => Value::Object(serde_json::Map::new()),
            };
            // ③ 锁内提交：只清捕获代次仍匹配的脏位（期间竞态的新脏写留着）；交付游标
            // **只前进不后退**（并发拉取时不得把更早捕获的水位写回）。
            {
                let mut core = lock(&notify.core);
                for (bi, rev) in revs.iter().enumerate() {
                    if core.dirty[bi] && core.dirty_rev[bi] == *rev {
                        core.dirty[bi] = false;
                    }
                }
                core.delivered_epoch = core.delivered_epoch.max(version);
            }
            return Ok(Snapshot { version, doc });
        }
        Err(SnapshotError::Incomplete)
    }

    /// 修复重投影（#104 req 3）：Store 快照与已提交水位必须描述**同一逻辑边界**。
    ///
    /// 协议：读水位 `v0` → 采 Store 快照 → 锁外投影 → 复核水位仍为 `v0` 才提交
    /// （变了 = 期间确实有通知被消费，重试；有界 ⇒ 不无限自旋）。两个方向的
    /// 不对称是这个协议成立的关键：采样到的状态只可能**新于** `v0`（未消费的通知
    /// 尚未投影，其值随后由补丁重放同一值 ⇒ 幂等），绝不会**旧于** `v0`——旧状态
    /// 套新版本才是 #104 的真缺陷。
    ///
    /// `Ok(())` 的承诺即「每个槽位都是已知良好投影且毒化已复位」，是快照完整性的
    /// 唯一闸门（`snapshot_doc` 只在此处判不完整）。
    fn repair(&self, bound: &[Bound]) -> Result<(), SnapshotError> {
        const ATTEMPTS: u32 = 4;
        let notify = &self.inner.notify;
        for _ in 0..ATTEMPTS {
            let v0 = lock(&notify.core).version;
            let state = match catch_unwind(AssertUnwindSafe(|| self.inner.store.snapshot())) {
                Ok(state) => state,
                Err(_) => return Err(SnapshotError::Incomplete),
            };
            let mut views: Vec<Option<Arc<M::View>>> = Vec::with_capacity(bound.len());
            let mut panicked = false;
            for b in bound.iter() {
                notify.projection_runs.fetch_add(1, Ordering::Relaxed);
                match catch_unwind(AssertUnwindSafe(|| notify.map.project(b.index, &state))) {
                    Ok(v) => views.push(Some(Arc::new(v))),
                    Err(_) => {
                        notify.projection_panics.fetch_add(1, Ordering::Relaxed);
                        panicked = true;
                        views.push(None);
                    }
                }
            }
            let mut core = lock(&notify.core);
            if core.version != v0 {
                continue; // 期间消费过通知 ⇒ 边界不配，重试
            }
            let retired = std::mem::replace(&mut core.last, views);
            for bi in 0..core.dirty.len() {
                core.dirty[bi] = true;
                core.dirty_rev[bi] = core.dirty_rev[bi].wrapping_add(1);
            }
            core.poisoned = panicked;
            core.commit_gen = core.commit_gen.wrapping_add(1);
            drop(core);
            drop(retired); // 退役值在锁外析构（采用方 `Drop`，#140）
            return if panicked {
                Err(SnapshotError::Incomplete)
            } else {
                Ok(())
            };
        }
        Err(SnapshotError::Raced { attempts: ATTEMPTS })
    }

    /// 显式全量重编（区域通道 unavailable 后的**唯一恢复口**，#172/#211）：以
    /// 桥内已提交现值为内容做一次全量换区事务；成功才清 failure 并推进
    /// `region_id`/序列（sequence 随桥水位 +[`SEQ_STEP`]，跨通道同源不变）。
    /// 宿主违约并发调用（与 dispatch 重叠）⇒ `PublishBusy`，不破坏已提交状态；
    /// 未封板 ⇒ `Unsealed`。
    pub fn reseed_region(&self) -> Result<(), RegionFailure> {
        let notify = &self.inner.notify;
        let bound = match &lock(&notify.core).bound {
            Some(b) => Arc::clone(b),
            None => return Err(RegionFailure::Unsealed),
        };
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
        let mut batch = SegBatch::default();
        notify.reseed_with(&bound, encoder.as_ref(), sequence, &mut batch);
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

    /// 采用方代码 panic 的累计计数（对齐 `subscriber_panics` 先例：让降级显形）。
    ///
    /// **计什么**：`project` panic、`input_changed`（哨兵）panic、`M::View::PartialEq`
    /// panic，以及退役 `M::View` 的 `Drop` panic，全部计入本计数器（#140 两阶段提交
    /// 与 #167 的三态提交共用这一本账）。
    ///
    /// **不算「跳过」**：哨兵 panic 走 fail-open——判不出「未变」时当拍**仍尝试**
    /// `project`（禁把不确定性吞成「未变」而丢一拍）；「跳过」只描述 `project` 自己
    /// panic 的那一支（保留 last-known-good，标不完整）。`PartialEq` panic 则该批
    /// **不提交**（水位不推进）；退役 `Drop` panic 发生于提交之后，不改变已提交值。
    ///
    /// **可见面**：以上任何 panic 都置毒化 ⇒ `PatchBatch::resync` 为真、
    /// `snapshot_doc` 走修复路径（仍有缺失/仍毒化 ⇒ `SnapshotError::Incomplete`），
    /// 修复成功后毒化复位。计数 > 0 即「降级显形」——桥本身不失联（panic 一律在
    /// 桥边界 contain，不穿出订阅回调）。
    pub fn projection_panics(&self) -> u64 {
        self.inner.notify.projection_panics.load(Ordering::Relaxed)
    }

    /// 封板前到达的通知累计（装配纪律违约的可见面：这些通知没有投影也没有发布，
    /// 绑定的初始基线由 `seal()` 那一次快照决定）。
    pub fn pre_seal_notifications(&self) -> u64 {
        self.inner
            .notify
            .pre_seal_notifications
            .load(Ordering::Relaxed)
    }
}

fn empty_batch(version: u64) -> PatchBatch {
    PatchBatch {
        patches: Vec::new(),
        resync: false,
        from_version: version,
        to_version: version,
        protocol_version: PROTOCOL_VERSION,
    }
}

/// 逐段下钻到叶父对象（缺的中间层补空对象；非对象挡路 = `None`）。
fn doc_parent<'a>(
    root: &'a mut Value,
    parents: &[String],
) -> Option<&'a mut serde_json::Map<String, Value>> {
    let mut node = root;
    for seg in parents {
        let Value::Object(map) = node else {
            return None;
        };
        node = map
            .entry(seg.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    match node {
        Value::Object(map) => Some(map),
        _ => None,
    }
}

/// 全量文档组装：按**装配期编译的**路径段建树（#146：运行期零拆串、零转义解码；
/// 重复指针/前缀冲突已在 `mount` 拒绝 ⇒ 此处形状必然可表示）。覆盖判定吃缓存。
/// `None` 槽 = 该绑定无已知良好投影（调用方已判不完整，这里跳过该叶）。
fn assemble_doc<J: JsonEncoder>(
    bound: &[Bound],
    views: &[Option<Arc<J::View>>],
    json: &J,
) -> Value {
    let mut root = Value::Object(serde_json::Map::new());
    for (bi, b) in bound.iter().enumerate() {
        if !b.json_cover {
            continue;
        }
        let Some(view) = &views[bi] else {
            continue;
        };
        let leaf = json.to_json(view);
        // 非根绝对指针：编译期保证至少一段。
        let (last, parents) = b
            .segments
            .split_last()
            .expect("compiled pointer has at least one segment");
        // 不可达：前缀冲突在 mount 已拒。真撞上时只丢这一叶，不整份清空。
        if let Some(map) = doc_parent(&mut root, parents) {
            map.insert(last.clone(), leaf); // replace 语义
        }
    }
    root
}

#[cfg(all(test, feature = "runtime"))]
mod bridge_lock_boundary {
    //! #140 锁边界探针（同模块 ⇒ 可直取 `notify.core` / `notify.region`）。直接证据 =
    //! 采用方回调（`to_json` / `encode` / `RegionBacking::with_bytes_mut` / `View::eq` /
    //! 退役 `View::drop`）正在执行时，对**两个**桥内互斥量 `try_lock` 都成功（框架此刻
    //! 未持任何桥锁）；`View::clone` 更进一层：桥内**零调用**。
    //!
    //! 公共面（自死锁探测 / 脏代次 / panic 协议 / 并发发布）见
    //! `tests/view_bridge_two_phase.rs`。把任一处回调挪回锁内 ⇒ 本模块 `WouldBlock` 即红。

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Mutex, OnceLock, TryLockError, Weak};

    use serde_json::{Value, json};
    use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
    use skeleton::{EffectBuf, Infallible, NoFx, Reducer};

    use super::*;

    #[derive(Clone)]
    struct App {
        n: u32,
    }

    struct AppReducer;

    impl Reducer for AppReducer {
        type State = App;
        type Action = u32;
        type Op = Infallible;
        fn initial(&self) -> App {
            App { n: 0 }
        }
        fn reduce(&self, s: &App, a: u32, _: &mut EffectBuf<u32, Infallible>) -> App {
            App { n: s.n + a }
        }
    }

    static DECLS: [BindingDecl; 1] = [BindingDecl {
        path: "/n",
        kind: PayloadTag(1),
    }];

    type ProbeInner = Inner<App, u32, Map, Intent, Json, Region>;
    type ProbeBridge = ViewBridge<App, u32, Map, Intent, Json, Region>;
    /// 回指桥内部件（视图先于桥构造 ⇒ 建好后再注入 `Weak`）。
    type Link = Arc<OnceLock<Weak<ProbeInner>>>;

    /// 直接证据：此刻对桥内互斥量 `try_lock` 成功（`Poisoned` 也算未持有）。
    ///
    /// 违规**记录**而非 panic：桥已把采用方 panic 全 contain（#140 复审 P0），探针
    /// panic 会被当成编码器/析构故障吞掉 ⇒ 记录 + 用例末尾断言 `violations == 0`
    /// 才具有判别力。
    fn unlocked<T>(mutex: &Mutex<T>, site: &str, mark: &Mark) {
        match mutex.try_lock() {
            Ok(_) | Err(TryLockError::Poisoned(_)) => {}
            Err(TryLockError::WouldBlock) => {
                mark.violations.fetch_add(1, Ordering::SeqCst);
                eprintln!("VIOLATION：{site} 在桥锁内执行");
            }
        }
    }

    #[derive(Default)]
    struct Mark {
        eq: AtomicUsize,
        clone: AtomicUsize,
        drop: AtomicUsize,
        json: AtomicUsize,
        encode: AtomicUsize,
        backing_writes: AtomicUsize,
        /// 锁边界违规累计（探针只记不 panic，见 [`unlocked`]）。
        violations: AtomicUsize,
    }

    struct V {
        n: u32,
        link: Link,
        mark: Arc<Mark>,
    }

    impl V {
        /// 回调里同时探两个桥锁（`core` 与 `region` 都不许被持有）。
        fn probe(&self, site: &str, counter: &AtomicUsize) {
            counter.fetch_add(1, Ordering::SeqCst);
            if let Some(inner) = self.link.get().and_then(Weak::upgrade) {
                unlocked(&inner.notify.core, site, &self.mark);
                unlocked(&inner.notify.region, site, &self.mark);
            }
        }
    }

    impl PartialEq for V {
        fn eq(&self, other: &Self) -> bool {
            self.probe("View::eq", &self.mark.eq);
            self.n == other.n
        }
    }

    impl Clone for V {
        fn clone(&self) -> Self {
            self.mark.clone.fetch_add(1, Ordering::SeqCst);
            Self {
                n: self.n,
                link: Arc::clone(&self.link),
                mark: Arc::clone(&self.mark),
            }
        }
    }

    impl Drop for V {
        fn drop(&mut self) {
            self.probe("退役 View::drop", &self.mark.drop);
        }
    }

    struct Map {
        link: Link,
        mark: Arc<Mark>,
        /// `true` ⇒ 哨兵恒报未变（复现「未变更快路径」）。
        quiet: bool,
    }

    impl ViewMap<App> for Map {
        type View = V;
        fn bindings(&self) -> &[BindingDecl] {
            &DECLS
        }
        fn input_changed(&self, _: usize, _: &App) -> bool {
            !self.quiet
        }
        fn project(&self, _: usize, s: &App) -> V {
            V {
                n: s.n,
                link: Arc::clone(&self.link),
                mark: Arc::clone(&self.mark),
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

    struct Json {
        mark: Arc<Mark>,
    }

    impl JsonEncoder for Json {
        type View = V;
        fn covers(&self) -> Coverage {
            Coverage::All
        }
        fn to_json(&self, v: &V) -> Value {
            v.probe("JsonEncoder::to_json", &self.mark.json);
            json!(v.n)
        }
    }

    struct Region {
        mark: Arc<Mark>,
    }

    impl RegionEncoder for Region {
        type View = V;
        fn covers(&self) -> Coverage {
            Coverage::All
        }
        fn encode(&self, v: &V, w: &mut RegionWriter) {
            v.probe("RegionEncoder::encode", &self.mark.encode);
            w.u64(u64::from(v.n));
        }
    }

    struct Backing {
        inner: VecBacking,
        link: Link,
        mark: Arc<Mark>,
    }

    impl RegionBacking for Backing {
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
            self.mark.backing_writes.fetch_add(1, Ordering::SeqCst);
            if let Some(inner) = self.link.get().and_then(Weak::upgrade) {
                unlocked(
                    &inner.notify.core,
                    "RegionBacking::with_bytes_mut",
                    &self.mark,
                );
                unlocked(
                    &inner.notify.region,
                    "RegionBacking::with_bytes_mut",
                    &self.mark,
                );
            }
            self.inner.with_bytes_mut(write);
        }
        fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
            self.inner.with_bytes(read);
        }
        fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
            self.inner.store_publish_seq(seq)
        }
        fn load_publish_seq(&self) -> Result<u64, SeqError> {
            self.inner.load_publish_seq()
        }
    }

    /// 建桥 + 注入回指 + 封板。`region_capacity` = `None` ⇒ 只开 JSON 通道。
    fn fixture(region_capacity: Option<usize>, quiet: bool) -> (Arc<ProbeBridge>, Arc<Mark>) {
        let link: Link = Arc::new(OnceLock::new());
        let mark = Arc::new(Mark::default());
        let store = crate::runtime::SyncStore::new(AppReducer, NoFx);
        let bridge = Arc::new(
            ViewBridge::mount(
                store,
                Map {
                    link: Arc::clone(&link),
                    mark: Arc::clone(&mark),
                    quiet,
                },
                Intent,
                Encoders {
                    json: Some(Json {
                        mark: Arc::clone(&mark),
                    }),
                    region: region_capacity.map(|capacity| {
                        (
                            Region {
                                mark: Arc::clone(&mark),
                            },
                            Arc::new(Backing {
                                inner: VecBacking::new(capacity),
                                link: Arc::clone(&link),
                                mark: Arc::clone(&mark),
                            }) as Arc<dyn RegionBacking>,
                        )
                    }),
                },
            )
            .expect("static declarations are well-formed"),
        );
        let inner: Weak<ProbeInner> = Arc::downgrade(&bridge.inner);
        assert!(link.set(inner).is_ok(), "回指只注入一次");
        bridge.bind("/n").unwrap();
        bridge.seal().expect("seal before dispatch");
        (bridge, mark)
    }

    #[test]
    fn json_encoder_runs_outside_both_bridge_locks() {
        let (bridge, mark) = fixture(None, false);
        assert!(bridge.send_intent(json!(3)).unwrap());
        let batch = bridge.take_patches();
        assert_eq!(batch.patches.len(), 1);
        assert!(bridge.snapshot_doc().is_ok());
        assert!(
            mark.json.load(Ordering::SeqCst) >= 2,
            "to_json 至少跑了两条路径"
        );
        assert_eq!(
            mark.violations.load(Ordering::SeqCst),
            0,
            "to_json 在桥锁内执行"
        );
    }

    #[test]
    fn region_encoder_and_backing_run_outside_both_bridge_locks() {
        let (bridge, mark) = fixture(Some(4096), false);
        for i in 1..=4u32 {
            assert!(bridge.send_intent(json!(i)).unwrap());
        }
        // 增量耗尽 ⇒ 全量重编（fallback 路径）也过同一组探针。
        bridge.reseed_region().expect("reseed succeeds");
        assert!(mark.encode.load(Ordering::SeqCst) >= 4, "encode 确实跑了");
        assert!(
            mark.backing_writes.load(Ordering::SeqCst) >= 4,
            "with_bytes_mut 确实跑了"
        );
        assert_eq!(
            mark.violations.load(Ordering::SeqCst),
            0,
            "encode/with_bytes_mut 在桥锁内执行"
        );
    }

    #[test]
    fn view_eq_and_retired_drop_run_outside_both_bridge_locks() {
        let (bridge, mark) = fixture(None, false);
        assert!(bridge.send_intent(json!(1)).unwrap());
        assert!(bridge.send_intent(json!(1)).unwrap());
        let _ = bridge.take_patches();
        assert!(mark.eq.load(Ordering::SeqCst) >= 2, "采用方 eq 确实跑了");
        assert!(mark.drop.load(Ordering::SeqCst) >= 1, "退役值确实被析构");
        assert_eq!(
            mark.violations.load(Ordering::SeqCst),
            0,
            "eq/退役 Drop 在桥锁内执行"
        );
    }

    /// 桥内**零** `View::clone`：句柄快照取代值拷贝（旧实现每次变更在 core 锁内
    /// 复制完整视图）。变异「把 `Arc::clone(v)` 换成 `(**v).clone()`」⇒ 红。
    #[test]
    fn bridge_never_clones_views() {
        let (bridge, mark) = fixture(Some(4096), false);
        for i in 1..=4u32 {
            assert!(bridge.send_intent(json!(i)).unwrap());
            let _ = bridge.take_patches();
            let _ = bridge.snapshot_doc();
        }
        let _ = bridge.reseed_region();
        assert_eq!(
            mark.clone.load(Ordering::SeqCst),
            0,
            "桥内不得调用采用方 View::clone"
        );
        assert_eq!(
            mark.violations.load(Ordering::SeqCst),
            0,
            "采用方回调在桥锁内执行"
        );
    }

    /// 未变更快路径的取锁预算（线程局部计数，同二进制并发测试不污染）。两种部署各测
    /// 一次：仅 JSON 与 JSON+Region（区域通道的发布事务每拍办一次，锁数是部署事实，
    /// 不是回退）。两次都不复制值快照。
    #[test]
    fn hot_path_lock_budget() {
        let json_only = locks_per_notify(None);
        let with_region = locks_per_notify(Some(4096));
        eprintln!("#140 未变更快路径取锁/通知：仅 JSON = {json_only}，JSON+Region = {with_region}");
        assert!(json_only <= 3.0, "仅 JSON 取锁超预算：{json_only}");
        assert!(with_region <= 8.0, "JSON+Region 取锁超预算：{with_region}");
    }

    fn locks_per_notify(region_capacity: Option<usize>) -> f64 {
        let (bridge, _mark) = fixture(region_capacity, true);
        assert!(bridge.send_intent(json!(1)).unwrap()); // 预热
        let before = super::lock_probe::here();
        let rounds = 64u64;
        for _ in 0..rounds {
            assert!(bridge.send_intent(json!(1)).unwrap());
        }
        (super::lock_probe::here() - before) as f64 / rounds as f64
    }
}

#[cfg(test)]
mod cell_lock_boundary {
    //! #157 锁边界探针（同模块 ⇒ 可直取胞的私有 `cell`）。直接证据 = 采用方代码
    //! 正在执行时，对同一个胞 `try_lock` 成功（框架此刻未持胞锁）；同线程重入
    //! 不死锁且结果确定。公共面场景（并发未命中竞态 / 代际裁决 / panic 不毒化）
    //! 见 `tests/view_cells_two_phase.rs`。
    //!
    //! 闭包先于胞存在（构造器要 `'static` 闭包）⇒ 经共享槽位回指：胞建好后再注入
    //! `Weak`，回调里 upgrade 取回。重入一律「恰一次」（原子旗子自清）防无限递归。

    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::OnceLock;
    use std::sync::TryLockError;
    use std::sync::Weak;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{Derived, Sentinel};

    /// OS 时钟护栏：探针体在独立线程执行，死锁表现为有界失败而非挂死。
    const LIMIT: Duration = Duration::from_secs(20);

    type Link<T> = Arc<OnceLock<Weak<T>>>;

    fn linked<T>(slot: &Link<T>) -> Arc<T> {
        slot.get()
            .and_then(Weak::upgrade)
            .expect("cell linked before use")
    }

    /// 直接证据：调用方正跑在采用方代码里 ⇒ 胞锁此刻必须空闲。
    fn assert_unlocked<T>(cell: &Mutex<T>, site: &str) {
        match cell.try_lock() {
            // Poisoned 也算「未持有」：析构探针可能在另一次断言的回退期被调，二次
            // panic 会 abort 整个测试进程、吞掉首发失败的报文。
            Ok(_) | Err(TryLockError::Poisoned(_)) => {}
            Err(TryLockError::WouldBlock) => panic!("{site} 在胞锁内执行"),
        }
    }

    fn bounded<T: Send + 'static>(probe: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(probe());
        });
        match rx.recv_timeout(LIMIT) {
            Ok(value) => value,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("探针疑似死锁（OS 时钟上限 {LIMIT:?}）")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("探针线程在回报前 panic")
            }
        }
    }

    #[test]
    fn compute_runs_outside_the_cell_lock_and_may_reenter() {
        bounded(|| {
            let link: Link<Derived<u32, u32, u32>> = Arc::new(OnceLock::new());
            let hook = Arc::clone(&link);
            let reenter = Arc::new(AtomicBool::new(true));
            let first = Arc::clone(&reenter);
            let computes = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&computes);
            let cell: Arc<Derived<u32, u32, u32>> = Arc::new(Derived::new(
                |state: &u32| *state,
                move |key: &u32| {
                    let cell = linked(&hook);
                    assert_unlocked(&cell.cell, "compute");
                    seen.fetch_add(1, Ordering::SeqCst);
                    if first.swap(false, Ordering::SeqCst) {
                        assert_eq!(cell.get(key), key * 10, "同线程重入同一个胞");
                    }
                    key * 10
                },
            ));
            assert!(link.set(Arc::downgrade(&cell)).is_ok(), "回指只注入一次");
            assert_eq!(cell.get(&7), 70);
            assert_eq!(computes.load(Ordering::SeqCst), 2, "外层 + 重入各算一次");
            assert_eq!(cell.get(&7), 70);
            assert_eq!(
                computes.load(Ordering::SeqCst),
                2,
                "重入那次已发布，其后命中零重算"
            );
        });
    }

    /// 带回指的键：`PartialEq` 里做锁边界探针，并（恰一次）重入同一个胞。
    struct ProbeKey {
        id: u32,
        link: Link<Derived<u32, ProbeKey, u32>>,
        reenter: Arc<AtomicBool>,
    }

    impl PartialEq for ProbeKey {
        fn eq(&self, other: &Self) -> bool {
            let cell = linked(&self.link);
            assert_unlocked(&cell.cell, "In::eq");
            if self.reenter.swap(false, Ordering::SeqCst) {
                assert_eq!(cell.get(&8), 80, "重入时换一个键：触发重算并发布");
            }
            self.id == other.id
        }
    }

    #[test]
    fn partial_eq_runs_outside_the_cell_lock_and_may_reenter() {
        bounded(|| {
            let link: Link<Derived<u32, ProbeKey, u32>> = Arc::new(OnceLock::new());
            let key_link = Arc::clone(&link);
            let reenter = Arc::new(AtomicBool::new(true));
            let key_reenter = Arc::clone(&reenter);
            let computes = Arc::new(AtomicUsize::new(0));
            let seen = Arc::clone(&computes);
            let cell: Arc<Derived<u32, ProbeKey, u32>> = Arc::new(Derived::new(
                move |state: &u32| ProbeKey {
                    id: *state,
                    link: Arc::clone(&key_link),
                    reenter: Arc::clone(&key_reenter),
                },
                move |key: &ProbeKey| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    key.id * 10
                },
            ));
            assert!(link.set(Arc::downgrade(&cell)).is_ok(), "回指只注入一次");
            assert_eq!(cell.get(&7), 70, "首次：缓存空，eq 未跑");
            assert_eq!(
                cell.get(&7),
                70,
                "命中：eq 里重入并发布 8 之后，外层仍按自己的快照作答"
            );
            assert!(!reenter.load(Ordering::SeqCst), "重入恰发生一次");
            assert_eq!(computes.load(Ordering::SeqCst), 2, "首次 + eq 里的重入");
            assert_eq!(cell.get(&8), 80);
            assert_eq!(computes.load(Ordering::SeqCst), 2, "8 已在重入里发布，命中");
            assert_eq!(cell.get(&7), 70);
            assert_eq!(computes.load(Ordering::SeqCst), 3);
        });
    }

    /// 带回指的值：`Clone` 里做锁边界探针并（恰一次）重入；`Drop` 里复核析构
    /// 不在胞锁内（缓存旧值的析构同样是采用方代码）。胞已析构（回指失效）时跳过。
    struct ProbeVal {
        v: u32,
        link: Link<Derived<u32, u32, ProbeVal>>,
        reenter: Arc<AtomicBool>,
    }

    impl Clone for ProbeVal {
        fn clone(&self) -> Self {
            let cell = linked(&self.link);
            assert_unlocked(&cell.cell, "Out::clone");
            if self.reenter.swap(false, Ordering::SeqCst) {
                assert_eq!(cell.get(&8).v, 80, "重入时换一个键：触发重算并发布");
            }
            Self {
                v: self.v,
                link: Arc::clone(&self.link),
                reenter: Arc::clone(&self.reenter),
            }
        }
    }

    impl Drop for ProbeVal {
        fn drop(&mut self) {
            if let Some(cell) = self.link.get().and_then(Weak::upgrade) {
                assert_unlocked(&cell.cell, "缓存值 Drop");
            }
        }
    }

    #[test]
    fn out_clone_and_cached_drop_run_outside_the_cell_lock() {
        bounded(|| {
            let link: Link<Derived<u32, u32, ProbeVal>> = Arc::new(OnceLock::new());
            let val_link = Arc::clone(&link);
            let reenter = Arc::new(AtomicBool::new(true));
            let val_reenter = Arc::clone(&reenter);
            let cell: Arc<Derived<u32, u32, ProbeVal>> = Arc::new(Derived::new(
                |state: &u32| *state,
                move |key: &u32| ProbeVal {
                    v: key * 10,
                    link: Arc::clone(&val_link),
                    reenter: Arc::clone(&val_reenter),
                },
            ));
            assert!(link.set(Arc::downgrade(&cell)).is_ok(), "回指只注入一次");
            // 首次未命中：缓存副本的 clone 里重入（换键 8，抢先发布）。
            assert_eq!(cell.get(&7).v, 70);
            assert!(!reenter.load(Ordering::SeqCst), "重入恰发生一次");
            // 命中路径的 clone；换键（旧缓存值析构）；再命中。
            assert_eq!(cell.get(&8).v, 80);
            assert_eq!(cell.get(&7).v, 70);
            assert_eq!(cell.get(&7).v, 70);
        });
    }

    /// 哨兵键：`eq` 里做锁边界探针并（恰一次）重入；`Drop` 复核析构不在胞锁内。
    struct GuardKey {
        id: u32,
        link: Link<Sentinel<u32, GuardKey>>,
        reenter: Arc<AtomicBool>,
    }

    impl PartialEq for GuardKey {
        fn eq(&self, other: &Self) -> bool {
            let cell = linked(&self.link);
            assert_unlocked(&cell.cell, "In::eq");
            if self.reenter.swap(false, Ordering::SeqCst) {
                assert!(cell.changed(&8), "重入时换一个键：提交 8");
            }
            self.id == other.id
        }
    }

    impl Drop for GuardKey {
        fn drop(&mut self) {
            if let Some(cell) = self.link.get().and_then(Weak::upgrade) {
                assert_unlocked(&cell.cell, "缓存值 Drop");
            }
        }
    }

    #[test]
    fn sentinel_partial_eq_and_cached_drop_run_outside_the_cell_lock() {
        bounded(|| {
            let link: Link<Sentinel<u32, GuardKey>> = Arc::new(OnceLock::new());
            let key_link = Arc::clone(&link);
            let reenter = Arc::new(AtomicBool::new(true));
            let key_reenter = Arc::clone(&reenter);
            let cell: Arc<Sentinel<u32, GuardKey>> =
                Arc::new(Sentinel::new(move |state: &u32| GuardKey {
                    id: *state,
                    link: Arc::clone(&key_link),
                    reenter: Arc::clone(&key_reenter),
                }));
            assert!(link.set(Arc::downgrade(&cell)).is_ok(), "回指只注入一次");
            assert!(cell.changed(&7), "首次：缓存空，eq 未跑");
            assert!(
                cell.changed(&7),
                "命中但重入已提交 8：裁决须复核代际，相对最新提交 7 确实变了"
            );
            assert!(!reenter.load(Ordering::SeqCst), "重入恰发生一次");
            assert!(cell.changed(&8), "重入那轮之后缓存是 7");
            assert!(cell.changed(&7));
            assert!(!cell.changed(&7));
        });
    }
}
