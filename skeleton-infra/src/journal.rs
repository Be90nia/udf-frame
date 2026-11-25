//! 追加式真相源端口 + JSONL 默认实现（先落盘后 apply；schema 只加字段+default）。
//!
//! feature 门 `journal`（绑 serde+serde_json+契约 crate skeleton——JournalPort 兑现面）。契约要点：
//! - **先落盘后 apply**：`append` 契约 Err ⇒ 调用方禁 apply 该 Action；
//! - **单写者**：本件无跨进程锁，双进程挂载同一账本=交错损坏——多进程场景上层编排；
//! - **flush 点=持久点**：BufWriter 缓冲非逐行 flush；读侧可见性=已 flush 前缀；
//! - 行格式：版本头 `{"v":1}`（`deny_unknown_fields`——未知字段=协议不兼容 fail-closed
//!   拒挂载；`"seed"` 落点须同步扩 Header 字段集）；action 行/snapshot 行
//!   均为 `{"t":"a"|"s", ...payload}` 平铺——**`"t"` 为顶层保留键**（payload 携带顶层
//!   `"t"`=append 显式拒绝 Err(Serialize)；值语义仍不校验）；payload 须序列化为
//!   **JSON 对象**（tag 平铺的前提）——非对象载荷（`A=u64` 裸数字等）首条
//!   append 即 Err(Serialize)。
//!
//! - **记录上限**：[`DEFAULT_MAX_RECORD_BYTES`]（16 MiB，**含终止换行**）是头行/
//!   action 行/snapshot 行/[`Replay`] 逐行读/快照扫描的同一上限。写侧走有界
//!   序列化（超限=`RecordTooLarge`，writer 与文件保持健康、文件不变），读侧走有界
//!   读行（超限=`RecordTooLarge` 带物理行号，不物化巨串、不静默跳过）。更大载荷的
//!   应用用 [`JsonlJournal::create_with_limit`]/[`JsonlJournal::open_with_limit`]
//!   显式 opt in——安全 API 无「无上限」哨兵。
//! - **记录形状**：头行之后每行必须是 JSON 对象且顶层 `t` 为**字符串**——`"a"`
//!   反序列化、`"s"` 与未知字符串 tag 跳过计数（schema 演化）、其余（非对象/
//!   缺 tag/非字符串 tag/坏 JSON）fail-closed `Corrupt`（带物理行号）。头行由
//!   [`JsonlJournal::replay`] 单独消费校验，不计数。
//! - **文件权限**（#181）：Unix 新建账本 `0600`（`OpenOptionsExt::mode`，独立于
//!   umask）；[`JsonlJournal::open`] 校验已有文件无 group/other 位（有则
//!   `InsecurePermissions`，**不隐式 chmod**）；Windows 隐私靠父目录 ACL 继承，
//!   本件不声称自动加固。
//! - **写侧 fail-stop**（#106 ③）：任何 write/flush/sync 失败 ⇒ writer `poisoned`，
//!   后续 append/snapshot/flush 一律 `Err(Poisoned)` 直到 reopen/恢复——记录边界
//!   已不确定时继续追加=在残行后写史，宁停不猜。
//! - **create 失败不留下半成品**（#212）：同目录独占临时文件初始化 + 硬链接原子
//!   发布（目标已存在即 `EEXIST`，`create_new` 语义与「绝不覆盖已有文件」原样保留）；
//!   失败只删本次创建的临时文件并保留原始 IO 错误。
//!
//! 零依赖纪律（同 text.rs 先例）：thiserror 不入 manifest，手写同形
//! `Display`/`Error` impl（错误文案与 thiserror 派生形态逐字一致），引入
//! thiserror 依赖时可换回派生。
//!
//! 自动快照注记：「append_count 达 n → flush+snapshot 行」中 snapshot 需 `&S`，
//! 而 `append(&A)` 无状态可见——本件实现**自动 flush**（重开后可见），
//! 快照节奏归调用方 `snapshot()`。

use std::fmt;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

/// 写缓冲容量（64KiB，禁逐行 flush）。
const WRITE_BUF: usize = 64 * 1024;

/// 单条 JSONL 记录编码上限默认值：**16 MiB，含终止换行**（#217 R2 拍板值）。
///
/// 头行、action 行、snapshot 行、[`Replay`] 逐行读、[`Journal::restore_stream`]
/// 快照扫描共用同一上限（即 [`JsonlJournal`] 的实例上限）。更大载荷的应用用
/// [`JsonlJournal::create_with_limit`]/[`JsonlJournal::open_with_limit`] 显式上调。
pub const DEFAULT_MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;

/// 行缓冲容量回收阈值：单条记录达此量级（远高于常态小记录）后缓冲回收到
/// [`LINE_BUF_KEEP`]——超大行不得把峰值容量挂在读侧迭代器的生命周期上（#217 ④）。
const LINE_BUF_SHRINK_ABOVE: usize = 1024 * 1024;

/// 行缓冲回收后的保留容量（常态记录远小于它，回收后不再抖动）。
const LINE_BUF_KEEP: usize = 8 * 1024;

/// 版本头字节（create 写出、open/replay 校验的唯一定义点；`v` 升级时同步改）。
const HEADER_LINE: &[u8] = b"{\"v\":1}\n";

/// 账本错误（fail-visible 面：Display 含路径/行号，`source` 链保源头）。
#[derive(Debug)]
pub enum JournalError {
    /// 底层 IO 失败（含 create 撞已存在文件：kind=AlreadyExists）。携带目标文件
    /// 路径（多账本进程里「哪个文件撞了」不靠猜）。
    Io {
        /// 目标账本文件路径。
        path: PathBuf,
        /// 底层 io 错误。
        source: std::io::Error,
    },
    /// 序列化失败（先序列化后写，失败不产生残行）。
    Serialize(serde_json::Error),
    /// 文件中部畸形行（行号从 1 起；尾部残行由 open 截断，不经此路径）。
    Corrupt {
        /// 畸形行行号（从 1 起）。
        line: u64,
        /// 反序列化失败源头。
        source: serde_json::Error,
    },
    /// 版本头与期望不符。
    VersionMismatch {
        /// 文件头实际版本。
        found: u32,
        /// 调用方期望版本。
        expected: u32,
    },
    /// 单条记录编码字节（含终止换行）越过配置上限（#217）。
    ///
    /// 读侧带物理行号（`Some`）；写侧序列化时无行号（`None`）。越限记录既不被
    /// 截断也不被当未知 tag 跳过——fail-closed。
    RecordTooLarge {
        /// 越限记录所在物理行号（从 1 起；写侧序列化时 `None`）。
        line: Option<u64>,
        /// 配置的上限（字节，含终止换行）。
        limit: usize,
    },
    /// 版本头缺终止换行（#106 ②）：空文件或崩溃截断的头行。
    ///
    /// 接受无换行的头会让首条 append 直接粘在头上，破坏第一条记录边界。
    HeaderUnterminated,
    /// 已有账本文件带 group/other 权限位（#181：Unix only，**不隐式 chmod**）。
    InsecurePermissions {
        /// 账本文件路径。
        path: PathBuf,
        /// 实际权限位（`mode & 0o777`）。
        mode: u32,
    },
    /// 写侧已被前一次 write/flush/sync 失败置为 fail-stop（#106 ③）。
    ///
    /// 记录边界已不确定：继续追加=在残行后写史。reopen/恢复前一律拒绝。
    Poisoned {
        /// 账本文件路径。
        path: PathBuf,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "io at {}: {source}", path.display()),
            Self::Serialize(e) => write!(f, "serialize action/state: {e}"),
            Self::Corrupt { line, .. } => write!(f, "corrupt json at line {line}"),
            Self::VersionMismatch { found, expected } => {
                write!(f, "version mismatch: file={found}, expected={expected}")
            }
            Self::RecordTooLarge { line, limit } => match line {
                Some(line) => write!(
                    f,
                    "record at line {line} exceeds the {limit} byte record limit (incl. newline)"
                ),
                None => write!(
                    f,
                    "record exceeds the {limit} byte record limit (incl. newline)"
                ),
            },
            Self::HeaderUnterminated => {
                write!(
                    f,
                    "journal header is not newline-terminated (empty/truncated header)"
                )
            }
            Self::InsecurePermissions { path, mode } => write!(
                f,
                "insecure journal permissions at {}: mode {mode:o} grants group/other access; \
                 chmod 600 or use a private parent directory",
                path.display()
            ),
            Self::Poisoned { path } => write!(
                f,
                "journal at {} is poisoned by a previous write/flush/sync failure; reopen to recover",
                path.display()
            ),
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Serialize(e) => Some(e),
            Self::Corrupt { source, .. } => Some(source),
            Self::VersionMismatch { .. } => None,
            Self::RecordTooLarge { .. } | Self::HeaderUnterminated => None,
            Self::InsecurePermissions { .. } | Self::Poisoned { .. } => None,
        }
    }
}

/// Io 错误构造（统一携带文件路径；`From<std::io::Error>` 无法承载 path，
/// 故收口于此、各 io 调用点显式 `map_err`）。
fn io_err(path: &Path, source: std::io::Error) -> JournalError {
    JournalError::Io {
        path: path.to_path_buf(),
        source,
    }
}

impl From<serde_json::Error> for JournalError {
    fn from(e: serde_json::Error) -> Self {
        Self::Serialize(e)
    }
}

/// 回放迭代器错误（逐行懒解析的失败面）。
#[derive(Debug)]
pub enum ReplayError {
    /// 底层 IO 失败。
    Io(std::io::Error),
    /// 畸形 JSON 行（行号从 1 起，fail-closed 禁静默跳过）。
    Corrupt {
        /// 畸形行行号（从 1 起）。
        line: u64,
        /// 反序列化失败源头。
        source: serde_json::Error,
    },
    /// 单行编码字节（含终止换行）越过配置上限（#217）。该物理行剩余字节已丢弃
    /// 以保证后续行号正确；不保留超限行的缓冲容量。
    RecordTooLarge {
        /// 越限行所在物理行号（从 1 起）。
        line: u64,
        /// 配置的上限（字节，含终止换行）。
        limit: usize,
    },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Corrupt { line, .. } => write!(f, "corrupt json at line {line}"),
            Self::RecordTooLarge { line, limit } => write!(
                f,
                "record at line {line} exceeds the {limit} byte record limit (incl. newline)"
            ),
        }
    }
}

impl std::error::Error for ReplayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Corrupt { source, .. } => Some(source),
            Self::RecordTooLarge { .. } => None,
        }
    }
}

impl From<std::io::Error> for ReplayError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

/// 追加式真相源端口（热路径静态分发优先、禁新增 dyn；本 trait 的 dyn 只在
/// 组合根装配点出现一次，属冷边界常数豁免）。
///
/// **与 [`skeleton::JournalPort`] 分工**：本 trait 是全功能面（[`Replay`]
/// 懒回放逐行解析，不物化 Vec），方法名带实现层词形（[`Journal::append_action`]/
/// [`Journal::restore_stream`]/[`Journal::append_snapshot`]，[`Journal::replay`]
/// 为实现层独有）；契约词表（`append`/`restore`/`persist_snapshot`）归端口
/// 独占——两 trait 同屏 import 时直呼无歧义（旧三同名 E0034/E0283 双候选
/// 已结构性消灭）。
///
/// **状态面关联类型化（批 6A）**：状态类型由 [`Journal::State`] 承载、被
/// `Self` 钉死——方法调用零 turbofish 零显式 deref（`j.append_action(&a)`
/// 直呼，`A` 从 action 参数自然推断；`MutexGuard` 后 `g.append_action(&a)`
/// 同形），读侧 `restore_stream`/`replay` 返回值全推断免绑定标注。类型标注
/// 只剩构造一处（`JsonlJournal::<A, S>::create(..)`——税落最冷点；惯用形见
/// [`JsonlJournal`] 类型文档 doctest）。
pub trait Journal<A>
where
    A: Serialize + DeserializeOwned,
{
    /// 账本状态面（快照行与恢复语义的载荷类型）。
    type State: Serialize + DeserializeOwned;

    /// 追加一行 action。**契约：Err ⇒ 调用方禁 apply 该 Action**（先落盘后 apply 的另一半）。
    ///
    /// 行格式约束：action/state 必须序列化为 **JSON 对象**（tag `"t"` 与载荷
    /// 平铺同层）——非对象载荷（如 `A = u64` 序列化为裸数字）首条
    /// `append_action` 即 `Err(JournalError::Serialize)`；序列化先于任何字节落盘（零残行），
    /// 换对象形状重试无副作用。
    ///
    /// 挂点矩阵：外部动作=组合根包装层 append→dispatch；`Run`/`Cancel` 效果的
    /// 回灌动作=解释器标准形模板强制 append-before-send（成功才 back.send）；
    /// **Emit 回灌不入账**——派生动作由 store 核心直接入队（不经解释器，无挂点
    /// 可设），它是已落盘 action 经纯 reducer 的确定性派生（同 (state, action)
    /// 恒同输出）：重放=Restore 动作流经同一装配的 store 逐条 dispatch，派生链
    /// 由排空环再生、终态收敛（回归锚：`tests/journal_port.rs`
    /// emit_feedback_replay_converges_with_live——live==重放==审计深度链三方可
    /// 查）。需逐动作账本的宿主须在自设派生挂点上 append。核心不设钩子=有意
    /// （零依赖）。
    ///
    /// BufWriter 缓冲，非逐行 flush；`snapshot_every` 命中时自动 flush（见模块注）。
    ///
    /// 失败面（#217/#106③）：载荷编码越过实例记录上限 ⇒ `Err(RecordTooLarge)`；
    /// 前一次 write/flush/sync 失败 ⇒ `Err(Poisoned)`。两者都**不写字节**，但
    /// 前者 writer 保持健康、后者需 reopen/恢复。
    fn append_action(&mut self, action: &A) -> Result<(), JournalError>;

    /// 快照读侧（流式）：自最新 "s" 行起播——`Some((快照态, 后续 action 回放))`；
    /// 无快照行=`None`（调用方回退 initial 全量回放）。快照截断/压实留升级位。
    /// 可见性=已 flush 前缀（BufWriter 未 flush 尾部对读侧不可见）；实时面用 Store 快照。
    /// 动名 `restore_stream`：账本侧=流式回放（`Option<(Self::State, Replay<'_>)>`，
    /// 逐行懒解析迭代器），端口侧 [`skeleton::JournalPort::restore`]=三态物化
    /// （`Restore<A, S>`）——同名已消（E0034/E0283 治理，批 2A 改名消重），
    /// 两 trait 同屏直呼各达其面。
    fn restore_stream(&self) -> Result<SnapshotStream<'_, A, Self::State>, JournalError>;

    /// flush + 写快照行（周期位；语义可选——真相源恒为全量 action 流，快照只为
    /// 加速恢复）。动名 `append_snapshot`：写侧语义自明（追加快照行），与端口侧
    /// [`skeleton::JournalPort::persist_snapshot`] 同物异名已消（E0034/E0283
    /// 治理）；与 [`skeleton::Store::snapshot`] 的读侧快照反向前缀区分照旧
    ///（接口维 R1：账本侧=持久化写入，store 侧=拉取读取）。
    fn append_snapshot(&mut self, state: &Self::State) -> Result<(), JournalError>;

    /// 只读回放迭代器（逐行懒解析）。**双层 Result 语义**：外层 `Err`=调用级
    /// 失败（文件打不开/头行不可校验等，调用本身）；内层迭代 item 的
    /// `Result<A, ReplayError>`=行级——`"s"` 行与未知字符串 tag 行跳过+计数
    /// （schema 演化）；非对象/缺顶层 string `t`/坏 JSON：`Err(Corrupt)`；单行越过
    /// 记录上限：`Err(RecordTooLarge)`——一律 fail-closed（禁静默跳过）。
    ///
    /// 版本头由本方法单独消费校验（#177）：不计数，且首条 action 的物理行号仍为 2。
    /// 可见性=已 flush 前缀；实时面用 Store 快照。
    fn replay(&self) -> Result<Replay<'_, A>, JournalError>;
}

/// `(A, S)` 焊接别名（`dyn Journal<A, State = S>` 短形）：关联类型化后
/// trait-object 面仍一线可达——把 `State` 焊进对象类型后 dyn 上方法调用
/// 同样零标注（对象寿命取默认 `+ 'static`，`JsonlJournal` 全自有句柄恒满足）。
pub type DynJournal<A, S> = dyn Journal<A, State = S>;

/// 快照流读侧（[`Journal::restore_stream`] 返回的 `Ok` 载荷形）：
/// `Some((快照态, 增量回放))` 或 `None`（无快照行，调用方回退 initial 全量回放）。
pub type SnapshotStream<'j, A, S> = Option<(S, Replay<'j, A>)>;

/// JSONL 追加式账本（`Journal` 默认实现；单写者）。
///
/// # 契约要点
///
/// - **记录上限**：构造期钉死（默认 [`DEFAULT_MAX_RECORD_BYTES`]=16 MiB 含换行），
///   头行/action/snapshot/回放/快照扫描同用此值（#217）。
/// - **文件权限**（#181）：Unix 新建 `0600`（独立于 umask）；`open` 拒绝已有的
///   group/other 可读文件（`InsecurePermissions`，不隐式 chmod）。**Windows**：无
///   POSIX 权限位，隐私完全依赖父目录 ACL 继承——调用方须把账本放进私有目录，
///   本件不声称自动 ACL 加固。
/// - **写侧 fail-stop**（#106 ③）：write/flush/sync 失败即 `poisoned`，直到 reopen。
/// - **create-new 语义**（#212）：目标已存在 ⇒ `Err(AlreadyExists)`，绝不删除/
///   覆盖调用方已有文件；初始化失败只清本次创建的临时文件。
///
/// # Examples
///
/// ```no_run
/// use skeleton_infra::journal::{JsonlJournal, Journal};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Action { id: u64 }
/// #[derive(Serialize, Deserialize)]
/// struct State { count: u64 }
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // (A, S) 在构造期钉死（一次 turbofish，税落最冷点）；此后方法调用全直呼：
/// let mut j = JsonlJournal::<Action, State>::create("journal.jsonl")?;
/// j.append_action(&Action { id: 1 })?; // 先落盘：Err ⇒ 禁 apply（A 从参数推断）
/// let _restored = j.restore_stream()?; // 全推断：Some((快照态, 增量回放)) 或 None
/// j.append_snapshot(&State { count: 1 })?; // 周期位：flush+写快照行（S 由 Self 归一）
/// j.flush()?; // 宿主停机序的 flush 位：解释器 close → 排空 idle → flush → drop
/// # Ok(())
/// # }
/// ```
pub struct JsonlJournal<A, S> {
    writer: BufWriter<std::fs::File>,
    path: PathBuf,
    version: u32,
    snapshot_every: Option<u64>,
    append_count: u64,
    /// 单条记录编码上限（字节，含终止换行；构造期钉死，见 [`DEFAULT_MAX_RECORD_BYTES`]）。
    max_record_bytes: usize,
    /// 写侧 fail-stop 位（#106 ③）：任一 write/flush/sync 失败后置位。
    poisoned: bool,
    /// open 时截断的崩溃残行字节数（Debug 可见留痕——残行必未越过 flush 点=非持久）。
    truncated_tail: u64,
    /// A/S 类型标记（不进存储布局；`fn()` 形对 Send/Sync/变体中性）。
    _types: PhantomData<fn() -> (A, S)>,
}

impl<A, S> fmt::Debug for JsonlJournal<A, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JsonlJournal")
            .field("path", &self.path)
            .field("version", &self.version)
            .field("snapshot_every", &self.snapshot_every)
            .field("append_count", &self.append_count)
            .field("max_record_bytes", &self.max_record_bytes)
            .field("poisoned", &self.poisoned)
            .field("truncated_tail", &self.truncated_tail)
            .finish_non_exhaustive()
    }
}

/// 版本头（`deny_unknown_fields`：未知字段=协议不兼容，fail-closed 拒挂载禁静默吞；
/// 协议演化走 `v` 版本号显式升级）。
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    v: u32,
}

impl<A, S> JsonlJournal<A, S> {
    /// 新建文件+写版本头 `{"v":1}`（记录上限取 [`DEFAULT_MAX_RECORD_BYTES`]）；
    /// 文件已存在=Err(Io kind AlreadyExists，Display 含路径)。
    ///
    /// 初始化实现（#212）：**同目录独占临时文件**（`create_new`）写头 → `flush` +
    /// `sync_all`（#106：create 成功=头行已落盘）→ **硬链接原子发布**到目标路径。
    /// 硬链接在目标已存在时失败 `EEXIST`，故 `create_new` 语义与「绝不覆盖/删除
    /// 已有文件」由文件系统原子保证，不需要「先查后建」的竞态窗口。任一步失败：
    /// 关闭句柄 → best-effort 删除**本次创建的临时文件** → 保留原始 IO 错误
    /// （清理失败并入错误文案：`temp file … cleanup failed`）。目标路径不会出现
    /// 「看似已建但打不开」的半成品。
    ///
    /// `shortcut:` 依赖文件系统支持硬链接（NTFS/主流 Unix 均满足）；FAT/exFAT 等
    /// 不支持硬链接的卷上 create 直接报底层 IO 错误，需要时再补「独占目标+直写」退化路径。
    pub fn create(path: impl AsRef<Path>) -> Result<Self, JournalError> {
        Self::create_with_limit(path, DEFAULT_MAX_RECORD_BYTES)
    }

    /// 同 [`JsonlJournal::create`]，但显式覆盖单条记录编码上限（字节，含终止换行）。
    ///
    /// 仅当应用存在**已证实**更大的 action/snapshot 载荷时上调；上限同样作用于
    /// 头行/回放/快照扫描（#217）。上限小于头行字节数（9B）时立即 `RecordTooLarge`
    /// ——不建出自身不可 open 的账本。
    pub fn create_with_limit(
        path: impl AsRef<Path>,
        max_record_bytes: usize,
    ) -> Result<Self, JournalError> {
        let path = path.as_ref();
        // 上限必须容得下头行，否则建出来的账本自身不可 open（fail-visible，不留陷阱）。
        if HEADER_LINE.len() > max_record_bytes {
            return Err(JournalError::RecordTooLarge {
                line: None,
                limit: max_record_bytes,
            });
        }
        let tmp = temp_sibling(path);
        let file = open_new_private(&tmp).map_err(|e| io_err(path, e))?;
        let mut writer = BufWriter::with_capacity(WRITE_BUF, file);
        // 版本头写后即刷+落盘：open 往返与挂载校验都依赖头行对读侧可见（flush 点=持久点）。
        let init = (|| -> std::io::Result<()> {
            #[cfg(test)]
            fault::check("create-write")?;
            writer.write_all(HEADER_LINE)?;
            writer.flush()?;
            #[cfg(test)]
            fault::check("create-flush")?;
            writer.get_ref().sync_all()?;
            Ok(())
        })();
        if let Err(e) = init {
            // 句柄必须先关（Windows 上开着删不掉），再清临时文件。
            drop(writer);
            return Err(create_failed(path, &tmp, e));
        }
        // 原子发布：hard_link 不覆盖已存在目标（`EEXIST`），删除临时名退场。
        if let Err(e) = std::fs::hard_link(&tmp, path) {
            drop(writer);
            return Err(create_failed(path, &tmp, e));
        }
        // 临时名退场失败只留一个孤儿 tmp 名字（账本已可用），不升级为错误。
        let _ = std::fs::remove_file(&tmp);
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            version: 1,
            snapshot_every: None,
            append_count: 0,
            max_record_bytes,
            poisoned: false,
            truncated_tail: 0,
            _types: PhantomData,
        })
    }

    /// 追加挂载：读版本头校验 → 尾部完整性校验 → append 从截断点续写。
    /// 记录上限取 [`DEFAULT_MAX_RECORD_BYTES`]（覆盖用 [`JsonlJournal::open_with_limit`]）。
    ///
    /// 头行校验含**换行框架**（#106 ②）：头行必须以 `\n` 终止（空文件/崩溃截断的
    /// 头行 ⇒ `HeaderUnterminated`），否则首条 append 会直接粘在头上破坏记录边界。
    /// 头行超过记录上限 ⇒ `RecordTooLarge{line: Some(1)}`。
    ///
    /// 末行无 `\n` 结尾=崩溃残行 → 截断到上一完整行边界并计数留痕（`Debug` 可见）。
    /// 残行截断安全：残行必未完整越过 flush 点=非持久，截断不丢任何已持久 action；
    /// 拒绝挂载会令账本报废不可修，故取截断+fail-visible。
    pub fn open(path: impl AsRef<Path>, expected_version: u32) -> Result<Self, JournalError> {
        Self::open_with_limit(path, expected_version, DEFAULT_MAX_RECORD_BYTES)
    }

    /// 同 [`JsonlJournal::open`]，但显式覆盖单条记录编码上限（含头行与后续回放/扫描）。
    /// 须与创建该账本时的 [`JsonlJournal::create_with_limit`] 取同值，否则读写口径不一致。
    pub fn open_with_limit(
        path: impl AsRef<Path>,
        expected_version: u32,
        max_record_bytes: usize,
    ) -> Result<Self, JournalError> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|e| io_err(path, e))?;
        // #181：已有文件不得带 group/other 权限位（不隐式 chmod）。
        check_private(&file, path)?;
        let mut reader = BufReader::new(file);

        // ① 版本头校验（有界读行 + 换行框架 + deny_unknown_fields）。
        let mut header_line: Vec<u8> = Vec::new();
        let header_len = match read_line_limited(&mut reader, &mut header_line, max_record_bytes)
            .map_err(|e| io_err(path, e))?
        {
            LineRead::Eof => return Err(JournalError::HeaderUnterminated),
            LineRead::TooLarge => {
                return Err(JournalError::RecordTooLarge {
                    line: Some(1),
                    limit: max_record_bytes,
                });
            }
            LineRead::Line => header_line.len() as u64,
        };
        if !header_line.ends_with(b"\n") {
            return Err(JournalError::HeaderUnterminated);
        }
        let header: Header = serde_json::from_slice(&header_line)
            .map_err(|e| JournalError::Corrupt { line: 1, source: e })?;
        if header.v != expected_version {
            return Err(JournalError::VersionMismatch {
                found: header.v,
                expected: expected_version,
            });
        }

        // ② 尾部完整性：末行无 \n=崩溃残行 → 截断到上一完整行边界。
        let mut truncated_tail = 0u64;
        let size = reader.seek(SeekFrom::End(0)).map_err(|e| io_err(path, e))?;
        if size > header_len {
            let last = find_last_newline(reader.get_mut(), size).map_err(|e| io_err(path, e))?;
            if let Some(pos) = last {
                let complete_end = pos + 1;
                if complete_end < size {
                    truncated_tail = size - complete_end;
                    reader
                        .get_mut()
                        .set_len(complete_end)
                        .map_err(|e| io_err(path, e))?;
                }
            }
        }

        let mut file = reader.into_inner();
        file.seek(SeekFrom::End(0)).map_err(|e| io_err(path, e))?;
        Ok(Self {
            writer: BufWriter::with_capacity(WRITE_BUF, file),
            path: path.to_path_buf(),
            version: header.v,
            snapshot_every: None,
            append_count: 0,
            max_record_bytes,
            poisoned: false,
            truncated_tail,
            _types: PhantomData,
        })
    }

    /// 显式冲刷（宿主停机序的 flush 位：解释器 close → 排空 idle → **flush** →
    /// drop——shutdown 前把 BufWriter 尾部落盘，读侧可见性=已 flush 前缀）。
    ///
    /// flush 末尾 `sync_all`（杠精盲审 2026-10-17）：裸 flush 只到 OS 页缓存，
    /// 掉电面「flush 点=持久点」不成立——同 crate atomic_write 有完整 fsync
    /// 纪律而本件没有=双标。flush 是低频点（停机序/snapshot 自动位），补
    /// fsync 后掉电读侧可见性=真持久前缀。
    ///
    /// **「flush 点=持久点」精确语义**：flush=写进 OS+`sync_all` 落盘（掉电
    /// 真持久）。读侧可见性另有分界——未 flush 的缓冲在**干净 drop** 时由
    /// `BufWriter` 兜底写出（重开同样可见）；真正丢数据的分界是**进程崩溃/
    /// `mem::forget`**（缓冲未落盘），不是 drop。
    ///
    /// 失败面（#106 ③）：flush 或 `sync_all` 任一失败 ⇒ writer 置 `poisoned`，
    /// 后续 flush/append 一律 `Err(Poisoned)`（原 IO 错误随本次返回，记录是否
    /// 已落盘=提交歧义，由调用方按 fail-closed 处理）。
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.reject_if_poisoned()?;
        let flushed = self.writer.flush();
        self.guard_io(flushed)?;
        #[cfg(test)]
        let synced = fault::check("flush-sync").and_then(|()| self.writer.get_ref().sync_all());
        #[cfg(not(test))]
        let synced = self.writer.get_ref().sync_all();
        self.guard_io(synced)
    }

    /// 每 n 次 append 自动 flush 一次（0/None=关闭自动快照；见模块注）。
    /// flush 含 `sync_all`（掉电真持久）——n 设小（如 1）=每 append 一次
    /// fsync，µs→ms 级延迟跳变属预期；高频持久需求请改显式 `flush()` 位。
    pub fn with_snapshot_every(mut self, n: u64) -> Self {
        self.snapshot_every = if n == 0 { None } else { Some(n) };
        self
    }

    /// 写侧 fail-stop 闸（#106 ③）：poisoned 后一律拒绝，直到 reopen/恢复。
    fn reject_if_poisoned(&self) -> Result<(), JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned {
                path: self.path.clone(),
            });
        }
        Ok(())
    }

    /// 任一 write/flush/sync 失败 ⇒ 置 poisoned 并原样上抛（#106 ③：记录边界已不确定）。
    fn guard_io(&mut self, r: std::io::Result<()>) -> Result<(), JournalError> {
        match r {
            Ok(()) => Ok(()),
            Err(e) => {
                self.poisoned = true;
                Err(io_err(&self.path, e))
            }
        }
    }
}

/// 自 EOF 反向分块找最后一个 `\n` 的绝对字节位（残行长度有界：一条未完成行）。
fn find_last_newline(file: &mut std::fs::File, size: u64) -> std::io::Result<Option<u64>> {
    let mut chunk = [0u8; 8192];
    let mut end = size;
    while end > 0 {
        let start = end.saturating_sub(chunk.len() as u64);
        let len = (end - start) as usize;
        file.seek(SeekFrom::Start(start))?;
        file.read_exact(&mut chunk[..len])?;
        if let Some(pos) = chunk[..len].iter().rposition(|&b| b == b'\n') {
            return Ok(Some(start + pos as u64));
        }
        end = start;
    }
    Ok(None)
}

/// 有界读行的结果。
enum LineRead {
    /// 文件结束（未读到任何字节）。
    Eof,
    /// 读到一条完整行（含终止 `\n`；末行无换行时也算 `Line`）。
    Line,
    /// 读满 `limit` 字节仍未见换行 ⇒ 越过记录上限；该物理行剩余字节已丢弃。
    TooLarge,
}

/// 行缓冲容量回收（#217 ④）：超大记录处理完后不把峰值容量留在复用缓冲里。
fn release_oversize(buf: &mut Vec<u8>) {
    if buf.capacity() > LINE_BUF_SHRINK_ABOVE {
        buf.shrink_to(LINE_BUF_KEEP);
    }
}

/// 有界读一行到 `buf`（#217：读侧不得靠无界 `read_line` 先分配再判长）。
///
/// 最多读 `limit` 字节（含终止换行）；越限即刻停并把该物理行余量吞掉（不分配），
/// 使后续行号仍是物理行号。`buf` 的长度不会超过 `limit`。
fn read_line_limited<R: BufRead>(
    reader: &mut R,
    buf: &mut Vec<u8>,
    limit: usize,
) -> std::io::Result<LineRead> {
    buf.clear();
    let mut too_large = false;
    loop {
        let (take, done) = {
            let avail = reader.fill_buf()?;
            if avail.is_empty() {
                return Ok(if too_large {
                    LineRead::TooLarge
                } else if buf.is_empty() {
                    LineRead::Eof
                } else {
                    LineRead::Line
                });
            }
            let (take, done) = match avail.iter().position(|&b| b == b'\n') {
                Some(pos) => (pos + 1, true),
                None => (avail.len(), false),
            };
            if !too_large && buf.len() + take <= limit {
                buf.extend_from_slice(&avail[..take]);
            } else {
                too_large = true;
            }
            (take, done)
        };
        reader.consume(take);
        if done {
            return Ok(if too_large {
                LineRead::TooLarge
            } else {
                LineRead::Line
            });
        }
    }
}

/// 去掉行尾 ASCII 空白（`\n`/`\r`/空格/制表）——等价旧 `str::trim_end` 的切尾。
fn trim_ascii_end(bytes: &[u8]) -> &[u8] {
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(0, |i| i + 1);
    &bytes[..end]
}

/// 同目录独占临时文件名（`.{name}.{pid}.{seq}.tmp`）——同目录=同文件系统，
/// 硬链接发布才可能原子（#212）。落在目标同目录也便于残留识别与人工清理。
fn temp_sibling(path: &Path) -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let dir = path.parent().unwrap_or_else(|| Path::new(""));
    dir.join(format!(".{name}.{}.{seq}.tmp", std::process::id()))
}

/// 独占新建账本文件（#181：Unix `0600` 由 `mode` 直接给，`umask` 只能减位不能加位
/// ⇒ 独立于 umask；Windows 无 POSIX 权限位，隐私靠父目录 ACL 继承）。
fn open_new_private(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.mode(0o600);
    }
    opts.open(path)
}

/// 已有账本隐私校验（#181：Unix only，**不隐式 chmod**）。
#[cfg(unix)]
fn check_private(file: &std::fs::File, path: &Path) -> Result<(), JournalError> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = file
        .metadata()
        .map_err(|e| io_err(path, e))?
        .permissions()
        .mode()
        & 0o777;
    if mode & 0o077 != 0 {
        return Err(JournalError::InsecurePermissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    Ok(())
}

/// Windows/其他平台：无 POSIX 权限位可校验，隐私由父目录 ACL 继承承担（见类型文档）。
#[cfg(not(unix))]
fn check_private(_file: &std::fs::File, _path: &Path) -> Result<(), JournalError> {
    Ok(())
}

/// 初始化失败收尾（#212）：只删**本次创建的临时文件**，原始 IO 错误（含 kind）
/// 原样保留；清理失败并入错误文案（不吞、也不掩盖原始失败）。
fn create_failed(path: &Path, tmp: &Path, source: std::io::Error) -> JournalError {
    match std::fs::remove_file(tmp) {
        Ok(()) => io_err(path, source),
        Err(cleanup) => io_err(
            path,
            std::io::Error::new(
                source.kind(),
                format!(
                    "{source}; temp file {} cleanup failed: {cleanup}",
                    tmp.display()
                ),
            ),
        ),
    }
}

/// 有界序列化落点（#217 R2）：写满 `cap` 字节后再写即拒，**不先物化超限载荷**。
///
/// `Write::write` 在越界时一个字节都不写（先判后写），越界事实由 `overflowed`
/// 上报——`serde_json` 只把它当 io 错误，超限与真序列化失败靠这个标志区分。
struct BoundedSink {
    buf: Vec<u8>,
    cap: usize,
    overflowed: bool,
}

impl Write for BoundedSink {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() + data.len() > self.cap {
            self.overflowed = true;
            return Err(std::io::Error::other("journal record exceeds limit"));
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// 行格式打标：`{"t":"<tag>", ...payload}` 平铺同层，返回**完整行字节（含终止换行）**。
///
/// 有界序列化（#217 R2）：载荷写进 [`BoundedSink`]（容量 = `limit` − 帧开销），
/// 越限即拒写 ⇒ 不先物化超限载荷；`limit` 含 `{"t":"<tag>",` + `}` + `\n` 的帧开销。
/// serde 失败/非对象载荷/顶层 `"t"` 保留键都在**零字节落盘前**拒绝（返回 Err 时
/// 调用方文件未动、writer 仍健康）。
///
/// tag 置首、载荷保序（确定性）；payload 顶层 `"t"` 键=显式拒绝（Err(Serialize)）
/// ——平铺成重复键后 serde_json 后键覆盖前键，replay 整行被当未知 tag 静默跳过
/// =真相源丢史。
fn tagged_line<T: Serialize>(tag: &str, value: &T, limit: usize) -> Result<Vec<u8>, JournalError> {
    let prefix = format!("{{\"t\":\"{tag}\"");
    // 帧开销：prefix + `,` + `\n`（body 自带外层 `{}`，剥掉 `{`/`}` 后原位重建）。
    let mut sink = BoundedSink {
        buf: Vec::new(),
        cap: limit.saturating_sub(prefix.len() + 1),
        overflowed: false,
    };
    if let Err(e) = serde_json::to_writer(&mut sink, value) {
        return Err(if sink.overflowed {
            JournalError::RecordTooLarge { line: None, limit }
        } else {
            JournalError::Serialize(e)
        });
    }
    let body = sink.buf;
    if !(body.starts_with(b"{") && body.ends_with(b"}")) {
        return Err(JournalError::Serialize(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "action/state must serialize to a JSON object for tag flattening",
            ),
        )));
    }
    // 保留键探测（append 冷路径 IO 主导，成本可忽略；失败发生在写字节之前）。
    let probe: serde_json::Value = serde_json::from_slice(&body)?;
    if probe.get("t").is_some() {
        return Err(JournalError::Serialize(serde_json::Error::io(
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "payload must not contain a top-level \"t\" key (reserved for line tag)",
            ),
        )));
    }
    let mut line = Vec::with_capacity(prefix.len() + body.len() + 2);
    line.extend_from_slice(prefix.as_bytes());
    if body.len() > 2 {
        line.push(b',');
        line.extend_from_slice(&body[1..body.len() - 1]);
    }
    line.push(b'}');
    line.push(b'\n');
    Ok(line)
}

impl<A, S> Journal<A> for JsonlJournal<A, S>
where
    A: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    type State = S;

    fn append_action(&mut self, action: &A) -> Result<(), JournalError> {
        self.reject_if_poisoned()?;
        // 有界序列化先于任何字节落盘（#217 R2）：超限=Err 且 writer/文件保持不变。
        let line = tagged_line("a", action, self.max_record_bytes)?;
        #[cfg(test)]
        self.guard_io(fault::check("append-write"))?;
        let written = self.writer.write_all(&line);
        self.guard_io(written)?;
        self.append_count += 1;
        // 自动 flush（with_snapshot_every 命中；快照节奏归调用方 snapshot()，见模块注）。
        if let Some(n) = self.snapshot_every {
            if self.append_count.is_multiple_of(n) {
                self.flush()?;
            }
        }
        Ok(())
    }

    fn restore_stream(&self) -> Result<Option<(S, Replay<'_, A>)>, JournalError> {
        // 单遍扫描找最后一条 "s" 行；返回的 Replay 从该行下一字节起（快照行之前的行
        // 一律跳过且不解析——含更早快照行与畸形行；行后畸形由增量回放 fail-closed）。
        let file = std::fs::File::open(&self.path).map_err(|e| io_err(&self.path, e))?;
        let mut reader = BufReader::new(file);
        let mut line: Vec<u8> = Vec::new();
        let mut offset = 0u64;
        let mut line_no = 0u64;
        let mut latest: Option<(S, u64, u64)> = None; // (快照态, 行后字节偏移, 行号)
        loop {
            // 扫描面同一回收策略（#217 ④）：超大行不得把峰值容量挂在本调用上。
            release_oversize(&mut line);
            match read_line_limited(&mut reader, &mut line, self.max_record_bytes)
                .map_err(|e| io_err(&self.path, e))?
            {
                LineRead::Eof => break,
                LineRead::TooLarge => {
                    line_no += 1;
                    release_oversize(&mut line);
                    return Err(JournalError::RecordTooLarge {
                        line: Some(line_no),
                        limit: self.max_record_bytes,
                    });
                }
                LineRead::Line => {}
            }
            line_no += 1;
            let n = line.len() as u64;
            // 扫描只认 "s" 行：畸形行/其他 tag 行不在此处裁决（留给回放面）。
            // 快照探测=字节前缀短路：tagged_line 恒产 `{"t":"s"...}` tag 置首形，
            // 非 "s" 形行零 JSON 解析——此前逐行 Value 解析成本 ≈ 全量 typed 回放
            //（0.40 vs 0.43µs/行），叠加增量回放=快照加速收益反转（perf-journal
            // §3-C）。非 writer 形（tag 不置首）漏检只退化全量重放：真相源恒为
            // action 流，终态不变。
            let trimmed = trim_ascii_end(&line);
            let is_snapshot = if trimmed.starts_with(b"{\"t\":\"s\"") {
                serde_json::from_slice::<serde_json::Value>(trimmed)
                    .ok()
                    .and_then(|v| (v.get("t").and_then(|t| t.as_str()) == Some("s")).then_some(v))
            } else {
                None
            };
            if let Some(mut v) = is_snapshot {
                if let Some(m) = v.as_object_mut() {
                    m.remove("t");
                }
                match serde_json::from_value::<S>(v) {
                    Ok(state) => latest = Some((state, offset + n, line_no)),
                    // 账本声称的快照读不出来=fail-closed，禁静默回退旧快照。
                    Err(e) => {
                        return Err(JournalError::Corrupt {
                            line: line_no,
                            source: e,
                        });
                    }
                }
            }
            offset += n;
        }
        match latest {
            None => Ok(None),
            Some((state, start, snap_line)) => {
                let mut file = reader.into_inner();
                file.seek(SeekFrom::Start(start))
                    .map_err(|e| io_err(&self.path, e))?;
                Ok(Some((
                    state,
                    Replay {
                        reader: BufReader::new(file),
                        line_no: snap_line,
                        skipped: 0,
                        limit: self.max_record_bytes,
                        buf: Vec::new(),
                        _marker: PhantomData,
                        _anchor: PhantomData,
                    },
                )))
            }
        }
    }

    fn append_snapshot(&mut self, state: &S) -> Result<(), JournalError> {
        self.reject_if_poisoned()?;
        // 序列化先于任何字节落盘（#217 R2 有界 sink）：超限=快照未落盘、writer 健康。
        let line = tagged_line("s", state, self.max_record_bytes)?;
        // flush 先行：快照行必须落在全部已 append action 之后（先落盘后 apply 的持久点纪律）。
        self.flush()?;
        let written = self.writer.write_all(&line);
        self.guard_io(written)?;
        // 快照行即写即刷：读侧可见性=已 flush 前缀，不刷的快照对恢复无用。
        self.flush()?;
        Ok(())
    }

    fn replay(&self) -> Result<Replay<'_, A>, JournalError> {
        let file = std::fs::File::open(&self.path).map_err(|e| io_err(&self.path, e))?;
        let mut reader = BufReader::new(file);
        // #177：完整回放先消费并校验头行（**不计数**）——首条 action 的物理行号仍为 2。
        let mut header: Vec<u8> = Vec::new();
        match read_line_limited(&mut reader, &mut header, self.max_record_bytes)
            .map_err(|e| io_err(&self.path, e))?
        {
            LineRead::Eof => return Err(JournalError::HeaderUnterminated),
            LineRead::TooLarge => {
                return Err(JournalError::RecordTooLarge {
                    line: Some(1),
                    limit: self.max_record_bytes,
                });
            }
            LineRead::Line => {}
        }
        if !header.ends_with(b"\n") {
            return Err(JournalError::HeaderUnterminated);
        }
        let parsed: Header = serde_json::from_slice(&header)
            .map_err(|e| JournalError::Corrupt { line: 1, source: e })?;
        if parsed.v != self.version {
            return Err(JournalError::VersionMismatch {
                found: parsed.v,
                expected: self.version,
            });
        }
        Ok(Replay {
            reader,
            line_no: 1,
            skipped: 0,
            limit: self.max_record_bytes,
            buf: Vec::new(),
            _marker: PhantomData,
            _anchor: PhantomData,
        })
    }
}

/// 契约端口兑现：`skeleton::JournalPort`（零依赖端口，后端自换的稳定面）。
///
/// 后端自换路径：宿主面向 `skeleton::JournalPort` 三方法编程，换后端=另实现
/// 该端口（内存账本/SQLite/网络皆可，序列化词表自选）；本件为 JSONL 参考实现。
/// 本 crate 的 [`Journal`] trait 是全功能面（[`Replay`] 懒回放逐行解析，
/// 不物化 Vec）——宿主要增量流式恢复用 [`Journal`]，要端口可换性用
/// `JournalPort`。端口同款关联类型化（批 6A）：`State` 被 `Self` 钉死，
/// 端口方法直呼零标注。
impl<A, S> skeleton::JournalPort<A> for JsonlJournal<A, S>
where
    A: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    type Err = JournalError;
    type State = S;

    fn append(&mut self, action: &A) -> Result<(), JournalError> {
        self.append_action(action)
    }

    fn persist_snapshot(&mut self, state: &S) -> Result<(), JournalError> {
        self.append_snapshot(state)
    }

    fn restore(&self) -> Result<skeleton::Restore<A, S>, JournalError> {
        match self.restore_stream()? {
            Some((state, replay)) => Ok(skeleton::Restore::FromSnapshot(
                state,
                collect_replay(&self.path, replay)?,
            )),
            // 无快照行：全量动作流自 initial 起播；零动作=空账本
            None => {
                let actions = collect_replay(&self.path, self.replay()?)?;
                if actions.is_empty() {
                    Ok(skeleton::Restore::Empty)
                } else {
                    Ok(skeleton::Restore::FromInitial(actions))
                }
            }
        }
    }
}

/// 回放收编（懒迭代器 → Vec；错误如实上抛，Io 补路径）。
fn collect_replay<A: DeserializeOwned>(
    path: &Path,
    replay: Replay<'_, A>,
) -> Result<Vec<A>, JournalError> {
    replay
        .map(|a| {
            a.map_err(|e| match e {
                ReplayError::Io(source) => JournalError::Io {
                    path: path.to_path_buf(),
                    source,
                },
                ReplayError::Corrupt { line, source } => JournalError::Corrupt { line, source },
                ReplayError::RecordTooLarge { line, limit } => JournalError::RecordTooLarge {
                    line: Some(line),
                    limit,
                },
            })
        })
        .collect()
}

/// 回放迭代器：Item=`Result<A, ReplayError>`。
///
/// 记录形状契约（#178）：头行之后每行必须是 JSON 对象且顶层 `t` 为字符串——
/// `"a"` 反序列化，`"s"` 与未知字符串 tag 跳过并计入 [`Replay::skipped_unknown`]
/// （schema 演化），其余（非对象/缺 tag/非字符串 tag/坏 JSON）fail-closed `Corrupt`。
/// 单行越过记录上限 ⇒ `RecordTooLarge`（#217），且越限行不保留缓冲容量。
pub struct Replay<'j, A> {
    reader: BufReader<std::fs::File>,
    /// 当前行号（从 1 起；跨过跳过面后保持绝对行号语义）。
    line_no: u64,
    skipped: u64,
    /// 单行编码上限（含终止换行；与写侧同一值）。
    limit: usize,
    buf: Vec<u8>,
    _marker: PhantomData<fn() -> A>,
    _anchor: PhantomData<&'j ()>,
}

impl<'j, A: DeserializeOwned> Iterator for Replay<'j, A> {
    type Item = Result<A, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.step();
        // 出错/读尽即回收超大行缓冲（#217 ④）：峰值容量不留在迭代器生命周期上。
        if !matches!(item.as_ref(), Some(Ok(_))) {
            release_oversize(&mut self.buf);
        }
        item
    }
}

impl<'j, A: DeserializeOwned> Replay<'j, A> {
    fn step(&mut self) -> Option<Result<A, ReplayError>> {
        loop {
            // 上一条超大记录的峰值容量先回收（复用缓冲不长期膨胀）。
            release_oversize(&mut self.buf);
            match read_line_limited(&mut self.reader, &mut self.buf, self.limit) {
                Ok(LineRead::Eof) => return None,
                Ok(LineRead::Line) => {}
                Ok(LineRead::TooLarge) => {
                    self.line_no += 1;
                    return Some(Err(ReplayError::RecordTooLarge {
                        line: self.line_no,
                        limit: self.limit,
                    }));
                }
                Err(e) => return Some(Err(ReplayError::Io(e))),
            }
            self.line_no += 1;
            let value: serde_json::Value = match serde_json::from_slice(&self.buf) {
                Ok(v) => v,
                Err(e) => {
                    return Some(Err(ReplayError::Corrupt {
                        line: self.line_no,
                        source: e,
                    }));
                }
            };
            // #178：顶层 `t` 必须是字符串——非对象/缺 tag/非字符串 tag 都是
            // writer 产不出的形状，按损坏 fail-closed（禁当未知变体静默跳过）。
            let is_action = match value.get("t") {
                Some(serde_json::Value::String(tag)) => tag == "a",
                _ => {
                    return Some(Err(ReplayError::Corrupt {
                        line: self.line_no,
                        source: serde_json::Error::io(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "journal record must be a JSON object with a top-level string \"t\" tag",
                        )),
                    }));
                }
            };
            if is_action {
                // 摘掉 tag 后整行交给 A：payload 缺字段走 schema default（add-only 演化），
                // 真畸形=Err fail-closed（禁静默跳过）。
                let mut payload = value;
                if let Some(m) = payload.as_object_mut() {
                    m.remove("t");
                }
                return Some(A::deserialize(&payload).map_err(|e| ReplayError::Corrupt {
                    line: self.line_no,
                    source: e,
                }));
            }
            // "s" 行与未知 string tag 一律跳过+计数（schema 演化）。
            self.skipped += 1;
        }
    }
}

impl<'j, A> Replay<'j, A> {
    /// 未知变体/快照行跳过计数（fail-visible：>0 必显式报告，禁静默）。
    ///
    /// 版本头**不计入**（[`JsonlJournal::replay`] 单独消费校验，#177）：header-only
    /// 账本恒为 0，一条快照行或一条未知 string tag 各 +1。
    pub fn skipped_unknown(&self) -> u64 {
        self.skipped
    }
}

/// 故障注入（**仅测试编译**）：`cargo test` 的 lib 单元测试用它在 write/flush/sync
/// 处制造真实 IO 失败——磁盘满/只读盘在 CI 里不可复现，而 #212 的失败清理与
/// #106 ③ 的 poison 态必须被红过再绿。集成测试编的是非 `cfg(test)` lib，不含本模块。
#[cfg(test)]
mod fault {
    use std::cell::Cell;

    thread_local! {
        /// 待触发的故障点；命中一次即解除（重试路径不被连带打断）。
        static ARMED: Cell<Option<&'static str>> = const { Cell::new(None) };
    }

    /// 武装一个故障点（下一个命中该 stage 的检查返回 Err）。
    pub(super) fn arm(stage: &'static str) {
        ARMED.with(|a| a.set(Some(stage)));
    }

    /// 命中已武装的 stage ⇒ 模拟一次 IO 失败。
    pub(super) fn check(stage: &'static str) -> std::io::Result<()> {
        let armed = ARMED.with(Cell::get);
        if armed != Some(stage) {
            return Ok(());
        }
        ARMED.with(|a| a.set(None));
        Err(std::io::Error::other(format!("injected fault: {stage}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Act {
        id: u64,
        label: String,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct St {
        count: u64,
    }

    /// 用例独享临时目录（收在 crate 构建目录内，禁系统 TEMP/用户目录）：路径含
    /// **进程 id + 进程内序号** ⇒ 并发测试进程互不干扰（固定路径会被对方
    /// `remove_dir_all` 连坐：读侧 NotFound、写侧读到半截）。清理归 [`CaseDir`] 的
    /// Drop（含 panic 路径）；断言仍按真实文件内容判定，目录缺失不跳过。
    fn case_dir(name: &str) -> CaseDir {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static SEQ: AtomicUsize = AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target-iso/journal-tests")
            .join(format!("{name}-{}-{seq}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir); // 同 pid 重跑残留清理
        std::fs::create_dir_all(&dir).unwrap();
        CaseDir(dir)
    }

    /// 用例目录守卫：Drop 收尾清理（含 panic 路径）——断言失败也留不下垃圾。
    struct CaseDir(PathBuf);
    impl std::ops::Deref for CaseDir {
        type Target = Path;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Drop for CaseDir {
        fn drop(&mut self) {
            // 清理失败不掩盖真实断言：残留只占 target-iso（gitignore 覆盖）。
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// 批 6A 关联类型化后调用面：直呼零标注（A/S 被 Self 钉死）——helper 只
    /// 收敛 unwrap 噪声；dyn 焊接面由 tests/journal_port.rs 的 Port 别名钉。
    fn app(j: &mut JsonlJournal<Act, St>, a: &Act) {
        j.append_action(a).unwrap();
    }
    fn snap(j: &mut JsonlJournal<Act, St>, s: &St) {
        j.append_snapshot(s).unwrap();
    }
    fn rep(j: &JsonlJournal<Act, St>) -> Replay<'_, Act> {
        j.replay().unwrap()
    }
    fn rest(j: &JsonlJournal<Act, St>) -> Option<(St, Replay<'_, Act>)> {
        j.restore_stream().unwrap()
    }

    /// 手写账本夹具：Unix 上权限收紧到 `0600`——`open` 会拒 group/other 可读的已有
    /// 文件（#181），裸 `fs::write` 在 `umask 022` 下产 0644 会把夹具变成隐私错误。
    fn write_private(path: &Path, body: &str) {
        std::fs::write(path, body).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    /// 恒等序列：中文/引号/反斜杠/换行转义/空串。
    fn sample_acts() -> Vec<Act> {
        vec![
            Act {
                id: 1,
                label: "中文\"引号\"与\\反斜杠".into(),
            },
            Act {
                id: 2,
                label: "line\nbreak\ttab".into(),
            },
            Act {
                id: 3,
                label: String::new(),
            },
        ]
    }

    #[test]
    fn append_replay_identity() {
        let dir = case_dir("append-replay");
        let path = dir.join("j.jsonl");
        let acts = sample_acts();
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            for a in &acts {
                app(&mut j, a);
            }
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, acts);
            assert_eq!(r.skipped_unknown(), 0); // 版本头不计入（#177）
        }
    }

    #[test]
    fn version_header_and_mismatch() {
        let dir = case_dir("version-header");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(
                &mut j,
                &Act {
                    id: 1,
                    label: "a".into(),
                },
            );
        }
        // create→open 往返；VersionMismatch。
        JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        match JsonlJournal::<Act, St>::open(&path, 2) {
            Err(JournalError::VersionMismatch { found, expected }) => {
                assert_eq!((found, expected), (1, 2));
            }
            other => panic!("expected VersionMismatch, got {other:?}"),
        }
        // 版本头未知字段：deny_unknown_fields → fail-closed。
        let seeded = dir.join("seeded.jsonl");
        write_private(&seeded, "{\"v\":1,\"seed\":42}\n");
        match JsonlJournal::<Act, St>::open(&seeded, 1) {
            Err(JournalError::Corrupt { line: 1, .. }) => {}
            other => panic!("expected Corrupt at line 1, got {other:?}"),
        }
        // create 撞已存在=Err(Io kind AlreadyExists，Display 含路径)。
        match JsonlJournal::<Act, St>::create(&path) {
            Err(JournalError::Io { source: e, .. }) => {
                assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists);
            }
            other => panic!("expected Io(AlreadyExists), got {other:?}"),
        }
    }

    /// 版本头静默吞未知字段
    /// （serde 默认忽略）=语义漂移入口——`deny_unknown_fields` 与畸形行 fail-closed
    /// 语义对齐；头演化走 `v` 版本号（VersionMismatch 显式拒绝），不走静默加字段。
    #[test]
    fn header_unknown_field_rejected_fail_closed() {
        let dir = case_dir("header-unknown-field");
        let path = dir.join("j.jsonl");
        write_private(&path, "{\"v\":1,\"extra\":true}\n");
        match JsonlJournal::<Act, St>::open(&path, 1) {
            Err(JournalError::Corrupt { line, .. }) => assert_eq!(line, 1),
            other => panic!("expected Corrupt at line 1, got {other:?}"),
        }
    }

    /// Io 错误 Display 必须携带文件路径——多账本进程里「哪个文件撞了」不再靠猜
    /// （os error 80 的本地化文案本身无定位价值）。kind 分流可编程性保持。
    #[test]
    fn io_error_display_carries_path() {
        let dir = case_dir("io-error-carries-path");
        let path = dir.join("ledger.jsonl");
        JsonlJournal::<Act, St>::create(&path).unwrap();
        let second = JsonlJournal::<Act, St>::create(&path); // 撞已存在
        match second {
            Ok(_) => panic!("create 撞已存在文件必须 Err"),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("ledger.jsonl"),
                    "Io 错误 Display 必须携带文件路径，实际: {msg}"
                );
                match &e {
                    JournalError::Io { source, .. } => {
                        assert_eq!(source.kind(), std::io::ErrorKind::AlreadyExists);
                    }
                    other => panic!("expected Io, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn unknown_tag_lines_are_skipped_and_counted() {
        let dir = case_dir("unknown-tag");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        let a2 = Act {
            id: 2,
            label: "b".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            j.flush().unwrap();
            // 手工注入未知 string tag 行（合法的 schema 演化形）。
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"t\":\"future_tag\",\"x\":1}}").unwrap();
        }
        {
            let mut j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
            app(&mut j, &a2);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]);
            assert_eq!(r.skipped_unknown(), 1); // 仅 future_tag（版本头不计入）
        }
    }

    /// #178：无 tag 的对象是 writer 产不出的形状——不得当未知变体静默跳过，
    /// 必须带物理行号 fail-closed，且不计入 skipped_unknown。
    #[test]
    fn untagged_object_fails_closed_and_is_not_counted() {
        let dir = case_dir("untagged-object");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(
                &mut j,
                &Act {
                    id: 1,
                    label: "a".into(),
                },
            );
            j.flush().unwrap();
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"note\":\"no tag at all\"}}").unwrap();
        }
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        let mut r = rep(&j);
        assert!(r.next().unwrap().is_ok()); // 行 2 完整
        match r.next() {
            Some(Err(ReplayError::Corrupt { line, .. })) => assert_eq!(line, 3),
            other => panic!("expected Corrupt at line 3, got {other:?}"),
        }
        assert_eq!(r.skipped_unknown(), 0, "损坏行不计入未知变体计数");
    }

    #[test]
    fn corrupt_json_line_fails_closed_with_line_number() {
        let dir = case_dir("corrupt-line");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(
                &mut j,
                &Act {
                    id: 1,
                    label: "a".into(),
                },
            );
            j.flush().unwrap();
            // 注入畸形行（行 3）：header=1, a1=2。
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "这行不是 JSON").unwrap();
        }
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        let mut r = rep(&j);
        assert!(r.next().unwrap().is_ok()); // 行 2 完整
        match r.next() {
            Some(Err(ReplayError::Corrupt { line, .. })) => assert_eq!(line, 3),
            other => panic!("expected Corrupt at line 3, got {other:?}"),
        }
    }

    #[test]
    fn snapshot_lines_stay_out_of_action_stream() {
        let dir = case_dir("snapshot-stream");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        let a2 = Act {
            id: 2,
            label: "b".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            snap(&mut j, &St { count: 1 });
            app(&mut j, &a2);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]); // "s" 行不混入
            assert_eq!(r.skipped_unknown(), 1); // 仅 "s" 行（版本头不计入）
        }
    }

    #[test]
    fn with_snapshot_every_auto_flushes() {
        let dir = case_dir("auto-flush");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        let a2 = Act {
            id: 2,
            label: "b".into(),
        };
        let mut j = JsonlJournal::<Act, St>::create(&path)
            .unwrap()
            .with_snapshot_every(2);
        app(&mut j, &a1);
        app(&mut j, &a2); // 达 n=2 → 自动 flush，无手动 flush
        // j 保持存活（BufWriter drop 兜底 flush 会污染断言），用第二句柄验证重开可见。
        {
            let j2 = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
            let mut r = rep(&j2);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]);
        }
        drop(j);
    }

    /// 恒 Err 的序列化替身（保证 Err(Serialize) 路径确定性）。
    #[derive(Debug, Deserialize)]
    struct BoomAct;
    impl Serialize for BoomAct {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("boom always"))
        }
    }

    #[test]
    fn serialize_error_leaves_no_partial_line() {
        let dir = case_dir("serialize-err");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<BoomAct, St>::create(&path).unwrap();
            match j.append_action(&BoomAct) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        // 先序列化后写：Err 时文件只有版本头，无残行。
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"v\":1}\n");
    }

    /// 顶层 `"t"` 为行 tag
    /// 保留键——payload 携带同层 `"t"` 时平铺成重复键，serde_json 后键覆盖前键，
    /// replay 整行被当未知 tag 静默跳过（先落盘后 apply 契约下=真相源丢史）。
    /// fail-closed：append 即 Err(Serialize)，字节零落盘。触发形态常见：
    /// `#[serde(tag = "t")]` internally-tagged enum 或任意含顶层 t 字段的类型。
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TaggedAct {
        t: String,
        v: u64,
    }

    #[test]
    fn reserved_top_level_t_key_rejected_fail_closed() {
        let dir = case_dir("reserved-t-key");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<TaggedAct, St>::create(&path).unwrap();
            let a = TaggedAct {
                t: "a".into(),
                v: 1,
            };
            match j.append_action(&a) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        // 先序列化后写：Err 时文件只有版本头，无残行。
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, "{\"v\":1}\n");
    }

    #[test]
    fn large_action_crosses_write_buffer() {
        let dir = case_dir("large-action");
        let path = dir.join("j.jsonl");
        let big = "字".repeat(70_000); // 210KiB UTF-8，跨 64KiB 写缓冲
        let a_big = Act {
            id: 1,
            label: big.clone(),
        };
        let a_small = Act {
            id: 2,
            label: "s".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a_big);
            app(&mut j, &a_small);
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got.len(), 2);
            assert_eq!(got[0].label.len(), big.len());
            assert_eq!(got[1], a_small);
        }
    }

    #[test]
    fn crash_residual_line_truncated_on_open() {
        let dir = case_dir("crash-residual");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        let a2 = Act {
            id: 2,
            label: "after-crash".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            j.flush().unwrap();
        }
        {
            // 模拟崩溃残行：无 \n 的半行。
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            write!(f, "{{\"t\":\"a\",\"id\":99,\"label\":\"torn").unwrap();
        }
        {
            let mut j = JsonlJournal::<Act, St>::open(&path, 1).unwrap(); // 截断残行
            app(&mut j, &a2); // 从完整行边界续写
            j.flush().unwrap();
            let mut r = rep(&j);
            let got: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(got, vec![a1, a2]); // 恰见完整序列，无 Corrupt
        }
    }

    #[test]
    fn restore_identity_snapshot_plus_delta_equals_full_replay() {
        let dir = case_dir("restore");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        let a2 = Act {
            id: 2,
            label: "b".into(),
        };
        let a3 = Act {
            id: 3,
            label: "c".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            app(&mut j, &a2);
            snap(&mut j, &St { count: 3 }); // fold(a1,a2)
            app(&mut j, &a3);
            j.flush().unwrap(); // 可见性=已 flush 前缀：先 flush 再 restore
            // 有快照行：起点=快照态，续回放=快照行之后的增量。
            let (s0, mut r) = rest(&j).unwrap();
            assert_eq!(s0, St { count: 3 });
            let inc: Vec<Act> = r.by_ref().map(Result::unwrap).collect();
            assert_eq!(inc, vec![a3]);
            // 终态恒等：快照+增量 ≡ initial+全量回放（Q3 扩展断言）。
            let mut by_snapshot = s0;
            for a in &inc {
                by_snapshot.count += a.id;
            }
            let mut by_full = St { count: 0 };
            for a in rep(&j).by_ref() {
                by_full.count += a.unwrap().id;
            }
            assert_eq!(by_snapshot.count, by_full.count);
        }
        // 无快照行=None。
        let empty = dir.join("empty.jsonl");
        {
            let j0 = JsonlJournal::<Act, St>::create(&empty).unwrap();
            assert!(rest(&j0).is_none());
        }
    }

    /// 恒 Err 的状态序列化替身（零覆盖 #11：snapshot() 序列化失败面）。
    #[derive(Debug, serde::Deserialize)]
    struct BoomState;

    impl serde::Serialize for BoomState {
        fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("boom state"))
        }
    }

    /// 零覆盖 #11：snapshot() 序列化失败 → Err(Serialize) 且零字节落盘。
    /// 契约：flush 先行已把存量 action 落盘（先落盘后 apply 的持久点纪律），
    /// 失败只发生在快照行写出前——账本保持一致前缀（版本头 + action 流），
    /// 无半行残迹。变异检出：先写后序列化的形态在此留残行，立即红。
    #[test]
    fn snapshot_serialize_failure_returns_err_without_partial_line() {
        let dir = case_dir("snapshot-serialize-err");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<Act, BoomState>::create(&path).unwrap();
            j.append_action(&Act {
                id: 1,
                label: "a".into(),
            })
            .unwrap();
            match j.append_snapshot(&BoomState) {
                Err(JournalError::Serialize(_)) => {}
                other => panic!("expected Serialize error, got {other:?}"),
            }
        }
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            content.lines().count(),
            2,
            "版本头 + 已 append 的 action，无快照残行"
        );
        assert!(!content.contains("\"t\":\"s\""), "失败快照不得落盘");
    }

    /// 零覆盖 #10：restore 扫描面遇「账本声称的快照读不出」fail-closed——
    /// JSON 合法（t:"s" 可识别）但 S 反序列化失败 = Err(Corrupt{line})，
    /// 禁静默回退更早的好快照（静默回退=恢复出陈旧态还自认成功=数据丢失面）。
    #[test]
    fn restore_fails_closed_on_undeserializable_snapshot_row() {
        let dir = case_dir("restore-bad-snapshot");
        let path = dir.join("j.jsonl");
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(
                &mut j,
                &Act {
                    id: 1,
                    label: "a".into(),
                },
            ); // 行 2
            snap(&mut j, &St { count: 1 }); // 行 3：好快照
            app(
                &mut j,
                &Act {
                    id: 2,
                    label: "b".into(),
                },
            ); // 行 4
            j.flush().unwrap();
            // 行 5：JSON 合法、t:"s" 可识别，但 count 类型不符——S 反序列化必败
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            writeln!(f, "{{\"t\":\"s\",\"count\":\"not-a-number\"}}").unwrap();
        }
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        match j.restore_stream() {
            Ok(_) => panic!("坏快照必须 Err：静默回退旧快照=恢复陈旧态还自认成功"),
            Err(JournalError::Corrupt { line, .. }) => {
                assert_eq!(line, 5, "坏快照行号如实上报");
            }
            Err(e) => panic!("expected Corrupt at line 5, got {e:?}"),
        }
    }

    /// 恢复反转回归钉（perf-journal §3-C）：中点快照恢复必须不慢于全量重放。
    ///
    /// 证据分层（issue `udf-frame-so3`）：
    /// - **主证据（不依赖墙钟）**：中快照恢复的增量恰等于全量重放的后半段（逐条
    ///   相等）+ 快照态播种 + 行数恒等——正确性面，任何负载下都必须成立；
    /// - **墙钟仅作粗判**：`snap <= full*2 + 200ms`。`min-of-3` 只抗一次性 OS 停顿，
    ///   共享机器上并发 cargo 会瞬时抬高任一侧 ⇒ 紧上界/严格比较必假红（并行窗口
    ///   实测 snap=438ms vs full=317ms）。精确定量归 perf 面，这里只守「量级倒挂」
    ///   这一种反转回归。
    #[test]
    fn restore_with_mid_snapshot_not_slower_than_full_replay() {
        let dir = case_dir("restore-inversion");
        let path = dir.join("j.jsonl");
        const HALF: u64 = 50_000;
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            let a = Act {
                id: 7,
                label: "x".into(),
            };
            for _ in 0..HALF {
                app(&mut j, &a);
            }
            snap(&mut j, &St { count: HALF * 7 });
            for _ in 0..HALF {
                app(&mut j, &a);
            }
            j.flush().unwrap();
        }
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        // min-of-3：OS 停顿至多污染一轮，min 收敛到无噪声固有成本。
        let mut snap_ms = u128::MAX;
        let mut restored_inc = Vec::new();
        for _ in 0..3 {
            let t = std::time::Instant::now();
            let (s0, r) = rest(&j).unwrap();
            let inc: Vec<Act> = r.map(Result::unwrap).collect();
            let e = t.elapsed();
            assert_eq!(s0, St { count: HALF * 7 }, "快照态播种");
            assert_eq!(inc.len() as u64, HALF, "只重放快照后增量，历史行不得重出");
            snap_ms = snap_ms.min(e.as_millis());
            restored_inc = inc;
        }
        let mut full_ms = u128::MAX;
        let mut replayed = Vec::new();
        for _ in 0..3 {
            let t = std::time::Instant::now();
            let full: Vec<Act> = rep(&j).map(Result::unwrap).collect();
            let e = t.elapsed();
            assert_eq!(full.len() as u64, HALF * 2);
            full_ms = full_ms.min(e.as_millis());
            replayed = full;
        }
        // 主证据（与墙钟无关）：中快照恢复 = 快照态 + 恰为全量重放的后半段。
        assert_eq!(
            restored_inc,
            replayed[HALF as usize..],
            "中快照恢复的增量必须与全量重放的后半段逐条相等（快照被跳过/历史重出即红）"
        );
        eprintln!("restore(mid-snapshot) = {snap_ms}ms, full replay = {full_ms}ms");
        // 粗判：只钉「量级倒挂」，宽容量级容差吸收共享机器的并发抬高。
        assert!(
            snap_ms <= full_ms * 2 + 200,
            "快照恢复出现量级倒挂（反转回归）：snap={snap_ms}ms full={full_ms}ms（容差 2x+200ms）"
        );
    }

    // ── #212：create 阶段失败不得留下「看似已建但打不开」的半成品 ────────────

    /// 头行写失败：目标路径不存在、临时文件已清、原始 IO 错误如实上抛、重试可建成。
    #[test]
    fn create_write_failure_leaves_no_artifact_and_retry_succeeds() {
        let dir = case_dir("create-write-fail");
        let path = dir.join("j.jsonl");
        fault::arm("create-write");
        match JsonlJournal::<Act, St>::create(&path) {
            Err(JournalError::Io { path: p, source }) => {
                assert_eq!(p, path, "错误携带目标账本路径");
                assert!(source.to_string().contains("create-write"), "{source}");
            }
            other => panic!("expected Io(write fault), got {other:?}"),
        }
        assert!(!path.exists(), "失败后目标路径必须不存在（不留半成品）");
        assert_eq!(
            std::fs::read_dir(&*dir).unwrap().count(),
            0,
            "本次创建的临时文件必须被清掉"
        );
        // 重试：同一路径可建成并 open（此前失败不阻塞重试）。
        let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
        app(
            &mut j,
            &Act {
                id: 1,
                label: "a".into(),
            },
        );
        j.flush().unwrap();
        drop(j);
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        assert_eq!(rep(&j).count(), 1);
    }

    /// flush/sync 失败（发布之前）走同一条清理路径。
    #[test]
    fn create_flush_failure_leaves_no_artifact_and_retry_succeeds() {
        let dir = case_dir("create-flush-fail");
        let path = dir.join("j.jsonl");
        fault::arm("create-flush");
        match JsonlJournal::<Act, St>::create(&path) {
            Err(JournalError::Io { source, .. }) => {
                assert!(source.to_string().contains("create-flush"), "{source}");
            }
            other => panic!("expected Io(flush fault), got {other:?}"),
        }
        assert!(!path.exists());
        assert_eq!(std::fs::read_dir(&*dir).unwrap().count(), 0);
        JsonlJournal::<Act, St>::create(&path).unwrap();
    }

    /// create-new 语义不变：目标已存在 ⇒ AlreadyExists，且**不删不覆盖**调用方文件。
    #[test]
    fn create_never_touches_preexisting_file() {
        let dir = case_dir("create-existing");
        let path = dir.join("j.jsonl");
        write_private(&path, "caller-owned\n");
        match JsonlJournal::<Act, St>::create(&path) {
            Err(JournalError::Io { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::AlreadyExists);
            }
            other => panic!("expected Io(AlreadyExists), got {other:?}"),
        }
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "caller-owned\n",
            "已有文件内容不得被改动"
        );
        assert_eq!(
            std::fs::read_dir(&*dir).unwrap().count(),
            1,
            "失败路径不得残留临时文件"
        );
    }

    // ── #106 ③：写侧 fail-stop（poisoned） ───────────────────────────────

    /// 写失败 ⇒ poisoned；后续 append/flush 一律 Err(Poisoned)；reopen 后恢复，
    /// 且文件里没有半条记录（先序列化后写 + 残行截断）。
    #[test]
    fn append_write_failure_poisons_writer_until_reopen() {
        let dir = case_dir("poison-append");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            j.flush().unwrap();
            fault::arm("append-write");
            match j.append_action(&Act {
                id: 2,
                label: "b".into(),
            }) {
                Err(JournalError::Io { source, .. }) => {
                    assert!(source.to_string().contains("append-write"), "{source}");
                }
                other => panic!("expected Io, got {other:?}"),
            }
            match j.append_action(&Act {
                id: 3,
                label: "c".into(),
            }) {
                Err(JournalError::Poisoned { path: p }) => assert_eq!(p, path),
                other => panic!("expected Poisoned, got {other:?}"),
            }
            assert!(
                matches!(j.flush(), Err(JournalError::Poisoned { .. })),
                "poisoned 后 flush 同样拒绝（同一条 fail-stop 边界）"
            );
        }
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        let got: Vec<Act> = rep(&j).map(Result::unwrap).collect();
        assert_eq!(got, vec![a1], "poisoned 后不得有半条记录入账");
    }

    /// flush 成功但 `sync_all` 失败也 poison（#106 R2「poison on sync failure」）：
    /// 记录可能已成残行，继续追加=在残行后写史。
    #[test]
    fn sync_failure_poisons_writer_until_reopen() {
        let dir = case_dir("poison-sync");
        let path = dir.join("j.jsonl");
        let a1 = Act {
            id: 1,
            label: "a".into(),
        };
        {
            let mut j = JsonlJournal::<Act, St>::create(&path).unwrap();
            app(&mut j, &a1);
            fault::arm("flush-sync");
            match j.flush() {
                Err(JournalError::Io { source, .. }) => {
                    assert!(source.to_string().contains("flush-sync"), "{source}");
                }
                other => panic!("expected Io(sync fault), got {other:?}"),
            }
            match j.append_action(&Act {
                id: 2,
                label: "b".into(),
            }) {
                Err(JournalError::Poisoned { .. }) => {}
                other => panic!("expected Poisoned, got {other:?}"),
            }
        }
        // sync 失败≠记录不存在：flush 已把字节交给 OS，reopen 后可见。
        let j = JsonlJournal::<Act, St>::open(&path, 1).unwrap();
        let got: Vec<Act> = rep(&j).map(Result::unwrap).collect();
        assert_eq!(got, vec![a1]);
    }

    /// #217：写侧超限拒绝后 writer 保持健康（非 poisoned）+ 文件零变化。
    #[test]
    fn oversized_write_rejection_keeps_writer_healthy() {
        let dir = case_dir("oversize-write");
        let path = dir.join("j.jsonl");
        let limit = 128usize;
        let mut j = JsonlJournal::<Act, St>::create_with_limit(&path, limit).unwrap();
        app(
            &mut j,
            &Act {
                id: 1,
                label: "a".into(),
            },
        );
        j.flush().unwrap();
        let before = std::fs::read(&path).unwrap();
        match j.append_action(&Act {
            id: 2,
            label: "x".repeat(limit),
        }) {
            Err(JournalError::RecordTooLarge {
                line: None,
                limit: l,
            }) => assert_eq!(l, limit),
            other => panic!("expected RecordTooLarge, got {other:?}"),
        }
        match j.append_snapshot(&St { count: u64::MAX }) {
            // 快照行 `{"t":"s","count":18446744073709551615}` = 37B < 128：正常写入
            Ok(()) => {}
            other => panic!("小快照不应被拒: {other:?}"),
        }
        j.flush().unwrap();
        // 超限 action 零字节落盘（快照行是之后正常写的，比对只针对 action 前缀）。
        let after = std::fs::read(&path).unwrap();
        assert!(
            after.starts_with(&before),
            "超限拒绝不得改动既有前缀（writer 健康、非 poisoned）"
        );
        assert!(
            String::from_utf8_lossy(&after[before.len()..]).starts_with("{\"t\":\"s\""),
            "超限拒绝后下一条正常写入照常落盘"
        );
    }
}
