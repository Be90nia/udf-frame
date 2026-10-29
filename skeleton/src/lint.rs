//! 配置后处理阶段编排：注册期登记的 lint 阶段表，按注册序一次跑完。
//!
//! 形状 = `register_stage`（登记）→ `post_process`（快照 + 锁外按序执行 + 短路）→
//! `registered_stages`（诊断名单）/ `clear_stages`（清场）。**键 = 配置类型**：
//! 每个 `C` 一张独立阶段表，不同 `C` 互不可见。
//!
//! # 为什么住契约层（`skeleton`）而不是 `skeleton_infra`
//!
//! 本件只依赖 std（`TypeId`/`Arc`/`RwLock`/`OnceLock`），零 feature 门、零第三方；
//! 而消费形态是「拿到配置结构体后跑校验」——**不需要任何机器**（无 IO、无线程池、
//! 无计时器），与 [`config`](crate::config) 同性质：启动编排住零依赖 crate，
//! 采用方单开 `udf-skeleton` 即可用，不必为配置校验把运行时拖进依赖树。
//!
//! # 一次调用做什么（快照语义）
//!
//! ```text
//! 1. 查表：REGISTRY 未初始化 → 直接 Ok（零注册的进程连锁都不碰，零分配）
//! 2. 快照：锁内 get + Arc::clone（仅原子自增，零堆分配）→ 立即放锁
//! 3. 执行：锁外按注册序跑阶段——阶段可 &mut C 改写、可再注册、可递归 post_process
//! 4. 短路：首个 Err 立即返回，归属由编排器补（已带归属的不覆盖）
//! ```
//!
//! 第 3 步的可见性是**快照**：本次调用跑的是第 2 步取到的那份表——阶段体内新
//! [`register_stage`] 的项**下一次调用**才生效（同一次调用里注册不会半路插队，
//! 也就不会出现「数组在遍历中被改」的未定义序列）。
//!
//! # 坑面 armor（生态事实逐条对表，11 条，`tests/lint.rs` + 本文件 `mod tests` 钉死）
//!
//! - **锁内跑用户回调 = 真实死锁**（tauri-apps/plugins-workspace#3394；std 的
//!   `Mutex`/`RwLock` 文档都警告非重入死锁）→ 快照后放锁，阶段在锁外执行；阶段体内
//!   [`register_stage`]/递归 [`post_process`] 因此合法。鉴别证据是**体内要拿锁**的那两条：
//!   `reentrant_registration_is_snapshot_scoped`（阶段体内注册 = 写锁）与
//!   `stage_drop_can_reenter_registry`（阶段 `Drop` 体内注册）；锁内执行必自死锁。
//!   `stages_are_not_serialized_by_an_exclusive_lock` 只证「读侧不被独占锁串行化」——
//!   读锁共享，即便锁持在快照期也凑得齐 barrier，故它**不**鉴别「锁外执行」。
//! - **析构也是用户代码**（阶段对象的 `Drop` 由采用方书写，可再入注册表）→ [`clear_stages`]
//!   两步式：`remove` 的返回值先出锁再 `drop`，最后一个阶段的 `Drop` 不在写锁内执行
//!   （`stage_drop_can_reenter_registry`）；`Drop` 内 panic 原样传播且不污染注册表
//!   （`stage_drop_panic_leaves_registry_usable`）。
//! - **每次调用 clone 整表**（生态里 `get_cloned` 形态）→ 每拍一次堆分配（本仓
//!   `notify` 曾因每拍 `collect::<Vec<_>>()` 被修，CHANGELOG 5753834）→ 本件注册表
//!   槽位存 `Arc<Vec<..>>`，改写走 `Arc::make_mut`（COW 只发生在**注册期**、且只在
//!   有在飞快照时才克隆），[`post_process`] 侧只 `Arc::clone` = 原子自增 → 稳态
//!   **零堆分配**（`tests/lint_alloc.rs` 事件计数断言 0）。
//! - **`TypeId` 要求 `'static` 且跨编译不稳定**（std 文档：同一 crate 不同编译单元
//!   亦不保证相等）→ 所有入口 `C: 'static`，且**键仅进程内有效**：禁持久化、禁
//!   跨进程传输、禁塞进任何序列化面。
//! - **全局可变状态 + 并行测试线程 = flaky**（Rust Internals「Global Registration」；
//!   `serial_test` 存在本身即证据；arXiv 2502.02760）→ 测试隔离靠**不同 `C` 类型**
//!   （每个测试私有 newtype）+ 同类型内 `clear_stages`：注册表按类型分槽，测试仍可
//!   并行跑，不需要串行闸。
//! - **锁中毒即注册表永久不可用**（生态 `global_registry` 直接返回 `LockError`，
//!   = 静默退化）→ 本件锁内只做 map 查找 / `Arc` 克隆 / `Vec` 追加，**不跑用户
//!   代码**，不存在被 panic 撕裂的中间态，故用
//!   `lock().unwrap_or_else(PoisonError::into_inner)` 恢复而非报废注册表
//!   （真毒化的覆盖在本文件 `#[cfg(test)] mod tests::registry_recovers_from_a_poisoned_lock`：
//!   持写 guard 后 panic 把锁刷成中毒态，再断言两条恢复分支仍可读写）。
//!   注：`stage_panic_propagates_and_registry_survives` 恰恰**不**毒化——锁不在执行期
//!   持有，故它证的是「panic 不污染注册表」，不是毒化恢复。
//! - **阶段错归属丢失**（错误只带 message → 排障不知哪个阶段）→ [`LintError`] 的
//!   `stage` 由编排器按失败阶段的 `name()` 补齐（`orchestrator_fills_missing_stage_attribution`）。
//! - **运行期注册的可见性**（不回写明 → 使用方以为立即生效）→ 见上「快照语义」，
//!   文档与测试双处声明。
//! - **编译期/链接期注册**（inventory / linkme）需要 `#[ctor]` 式构造：链接顺序
//!   不可控、无运行期动态注册能力、且各加一份三方依赖 → 本件选**运行期注册表**
//!   （`OnceLock` + `RwLock`），代价只是每次 `post_process` 一次全局读锁 + 一次原子
//!   自增。
//! - **锁类型是实测选出来的，不是口味**：完整对照与前值留档在
//!   `verify/2026-10-29-lint-lock-showdown/`（含 Mutex 版源码 `lint.rs.mutex.bak`；该目录
//!   里的 Mops/s 前值**非当前树可复跑**，只作历史对照）。结论：空阶段 4 线程吞吐 `RwLock`
//!   明显更好（约 **+25%~27%**），未争用 ns 锚（0/1/5 阶段）两侧差 ≤4%（判据 <10%）→ 取
//!   `RwLock`：快照路径走**共享读锁**（并发读互不阻塞），注册/清场走**独占写锁**，两侧
//!   毒化恢复同走 `PoisonError::into_inner`。本树读数 = **单批运行**（2026-10-29，本机
//!   16 核，std `Instant` 粗测；原文 `verify/2026-10-29-lint-fix-round/example.txt`）：
//!   0/1/5 阶段 **17.0 / 28.0 / 38.3 ns**，空阶段 1→4 线程 32.8 → 9.98 Mops/s（**0.30x**），
//!   ~20 µs 计算阶段 4 线程 **4.12x**。曾评估的第三条路（TLS per-线程表缓存 /
//!   generation 计数器）**不采用**：旧快照会滞留每线程、内存维度受损，而调用点是
//!   配置装载期冷路径，不值那个复杂度。
//! - **阶段 panic**：不 `catch_unwind`、不吞——panic 原样传播给调用方；注册表与
//!   全局锁不受影响（锁不在执行期持有）。
//!
//! # 收窄声明
//!
//! - 无卸载语义：阶段一旦注册只能整表 [`clear_stages`]（「谁注册谁负责」；需要
//!   单点撤销的形态请自带 `Arc` 句柄与开关位，本件不做引用计数式的点名摘除）。
//! - 无优先级/分组/依赖排序：只有一种顺序——**注册序**（先注册先跑）。
//! - **同类型递归无防护**：阶段体内 `post_process::<C>` 合法（不死锁）但会重跑本阶段，
//!   终止条件由阶段自设（无界递归 = 栈溢出，不是 `Err`）。
//! - `name()` 不做唯一性校验：重复名合法（诊断面而已），排障时按注册序列数位。
//! - 线程安全边界：注册表自身线程安全（`RwLock` + `Arc`）；**`C` 不必 `Send`**，
//!   跨线程并发 [`post_process`] 时由调用方保证各自 `&mut C` 独占。
//! - **无阶段时的多线程吞吐是显式登记的上限**：读路径仍是一次全局锁（`RwLock` 读侧与
//!   一次原子自增），绝对成本 ~17 ns/拍（本批 17.0）；0 阶段下 4 线程只有 ~0.30x（全局锁
//!   争用退化）→ 高吞吐采用方两条路：①各自 config 类型分槽（表按 `TypeId` 天然隔离，
//!   不同 `C` 不争同一把锁，本件已支持零成本）；②每拍不必重复 lint（配置装载一次跑一次，
//!   别塞进每帧/每请求热路径）——本件不提供 per-线程表缓存：TLS 缓存让旧快照滞留每线程，
//!   内存维度受损，配置装载期冷路径不值这个复杂度。
//!
//! # Examples
//!
//! ```
//! use skeleton::lint::{
//!     clear_stages, post_process, register_stage, registered_stages, LintError, LintStage,
//! };
//! use std::sync::Arc;
//!
//! // 私有 newtype 作 C：注册表按类型分槽，示例/测试互不干扰。
//! struct AppConfig {
//!     retries: u32,
//!     log_level: String,
//! }
//!
//! struct ClampRetries;
//! impl LintStage<AppConfig> for ClampRetries {
//!     fn name(&self) -> &'static str {
//!         "clamp_retries"
//!     }
//!     fn run(&self, cfg: &mut AppConfig) -> Result<(), LintError> {
//!         cfg.retries = cfg.retries.min(10);
//!         Ok(())
//!     }
//! }
//!
//! struct RequireLogLevel;
//! impl LintStage<AppConfig> for RequireLogLevel {
//!     fn name(&self) -> &'static str {
//!         "require_log_level"
//!     }
//!     fn run(&self, cfg: &mut AppConfig) -> Result<(), LintError> {
//!         if cfg.log_level.is_empty() {
//!             return Err(LintError::new("log_level 必填"));
//!         }
//!         Ok(())
//!     }
//! }
//!
//! register_stage::<AppConfig>(Arc::new(ClampRetries));
//! register_stage::<AppConfig>(Arc::new(RequireLogLevel));
//! assert_eq!(
//!     registered_stages::<AppConfig>(),
//!     ["clamp_retries", "require_log_level"]
//! );
//!
//! let mut cfg = AppConfig { retries: 99, log_level: String::new() };
//! let err = post_process(&mut cfg).unwrap_err();
//! assert_eq!(err.stage(), Some("require_log_level")); // 编排器补齐归属
//! assert_eq!(err.to_string(), "require_log_level: log_level 必填");
//! assert_eq!(cfg.retries, 10); // 短路：已发生的改动保留
//!
//! cfg.log_level = "info".into();
//! assert_eq!(post_process(&mut cfg), Ok(()));
//! clear_stages::<AppConfig>();
//! assert!(registered_stages::<AppConfig>().is_empty());
//! ```

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

/// 一个配置 lint 阶段：名字（排障归属）+ 在 `&mut C` 上校验/改写。
///
/// 语义契约（由 [`post_process`] 兑现）：
///
/// - [`run`](LintStage::run) 在**注册表锁之外**执行——阶段体内可以
///   [`register_stage`]、可以递归 [`post_process`]（含本类型以外的类型）；
/// - 拿到 `&mut C`：阶段**可改写**配置，不是纯校验面；
/// - 返回 `Err` 即终止本拍（后续阶段不跑，已发生的改动保留）；错误
///   [`LintError::stage`] 为空时由编排器补本阶段 [`name`](LintStage::name)；
/// - panic 原样传播（编排器不 `catch_unwind`、不吞）；
/// - 生命周期：`Arc<dyn LintStage<C>>` 默认 `'static`，阶段项须自持全部数据
///   （配置 `C` 本身只需 `C: 'static`）。
///
/// `Send + Sync` 是硬约束：登记表被跨线程共享，阶段项可能被任一线程执行。
///
/// # Examples
///
/// ```
/// use skeleton::lint::{clear_stages, post_process, register_stage, LintError, LintStage};
/// use std::sync::Arc;
///
/// struct AppConfig {
///     port: u16,
/// }
///
/// struct PortPresent;
/// impl LintStage<AppConfig> for PortPresent {
///     fn name(&self) -> &'static str {
///         "port_present"
///     }
///     fn run(&self, cfg: &mut AppConfig) -> Result<(), LintError> {
///         if cfg.port == 0 {
///             return Err(LintError::new("port 必填"));
///         }
///         Ok(())
///     }
/// }
///
/// register_stage::<AppConfig>(Arc::new(PortPresent));
/// let mut cfg = AppConfig { port: 0 };
/// assert_eq!(
///     post_process(&mut cfg).unwrap_err().stage(),
///     Some("port_present")
/// );
/// clear_stages::<AppConfig>();
/// ```
pub trait LintStage<C>: Send + Sync {
    /// 阶段名（稳定的 `'static` 字面量）：诊断名单与错误归属都用它。
    ///
    /// 不要求唯一——重复名合法，排障时按注册序列数位（见模块收窄声明）。
    fn name(&self) -> &'static str;

    /// 对一个配置执行本阶段：`Ok(())` 放行，`Err` 终止本拍。
    ///
    /// 契约：锁外执行、可改写 `cfg`、不得依赖「之后的阶段是否跑」——被短路
    /// 时后续阶段根本不会被调用。
    ///
    /// **同类型递归**：体内 `post_process::<C>(cfg)` 是合法的（锁不在手，不会死锁），
    /// 但本件**不做任何防护**——那会再跑一遍本阶段本身，无自设终止条件即无界递归
    /// （栈溢出 = 进程中止，不是可捕获的 `Err`）。阶段须自带终止条件（如 `cfg` 上的
    /// 深度/标志位），见 `same_type_recursion_is_allowed_and_self_limited`。
    fn run(&self, cfg: &mut C) -> Result<(), LintError>;
}

/// 阶段失败：`message`（发生了什么）+ `stage`（哪个阶段，编排器补）。
///
/// 归属的两种来源：
///
/// - 阶段内 [`LintError::new`] 构造 → `stage` 为空，[`post_process`] 补上**失败
///   阶段**的 [`LintStage::name`]（排障无需瞎猜是谁报的错）；
/// - 错误本身来自嵌套的 [`post_process`]（已带归属）→ 外层**不覆盖**（否则外层
///   替内层的错背锅，归属链断掉）。
///
/// 手写 `Display`/`Error`（同 `config`/`journal` 先例：thiserror 不入 manifest，
/// 零依赖）。`Display`：带归属时 `"{stage}: {message}"`，否则 `"{message}"`。
///
/// # Examples
///
/// ```
/// use skeleton::lint::LintError;
///
/// let err = LintError::new("port 越界");
/// assert_eq!(err.stage(), None); // 阶段外构造：归属由编排器补齐
/// assert_eq!(err.message(), "port 越界");
/// assert_eq!(err.to_string(), "port 越界");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintError {
    stage: Option<&'static str>,
    message: String,
}

impl LintError {
    /// 阶段内构造一个失败（`stage` 留空，由编排器补）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::lint::LintError;
    ///
    /// let err = LintError::new(String::from("层是加载链第 3 层"));
    /// assert_eq!(err.message(), "层是加载链第 3 层");
    /// assert_eq!(err.stage(), None);
    /// ```
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            stage: None,
            message: message.into(),
        }
    }

    /// 失败阶段名；`None` = 尚未经过编排器补齐（阶段外构造的原始错误）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::lint::LintError;
    ///
    /// assert_eq!(LintError::new("x").stage(), None);
    /// ```
    pub fn stage(&self) -> Option<&'static str> {
        self.stage
    }

    /// 失败原因原文。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::lint::LintError;
    ///
    /// assert_eq!(LintError::new("x").message(), "x");
    /// ```
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for LintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.stage {
            Some(stage) => write!(f, "{stage}: {}", self.message),
            None => f.write_str(&self.message),
        }
    }
}

impl std::error::Error for LintError {}

/// 全局注册表：`TypeId(C)` → boxed `Arc<Vec<Arc<dyn LintStage<C>>>>`。
///
/// `OnceLock` 懒建：**没有任何注册**的进程里 [`post_process`] 走
/// `REGISTRY.get() == None` 直接返回，连锁都不碰（快路径零成本）；也免了静态
/// 初始化顺序问题。
///
/// 槽位类型由 `TypeId` 决定，故 downcast 必然命中——不命中即内部不变量破坏
/// （见 `stage_table` 的 `expect`）。
static REGISTRY: OnceLock<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>> = OnceLock::new();

/// 一个配置类型的阶段表（`Arc` 快照单元：clone = 原子自增）。
type Stages<C> = Arc<Vec<Arc<dyn LintStage<C>>>>;

/// 注册表内层形状。
type Registry = HashMap<TypeId, Box<dyn Any + Send + Sync>>;

/// 读侧锁（快照路径）：并发读取互不阻塞。
///
/// 锁中毒恢复（理由见模块 armor「锁中毒」条：锁内不跑用户代码，无可撕裂状态）：
/// 毒化即 `into_inner` 恢复，不报废注册表。
fn read(registry: &RwLock<Registry>) -> RwLockReadGuard<'_, Registry> {
    registry.read().unwrap_or_else(PoisonError::into_inner)
}

/// 写侧锁（注册/清场路径）：同一时刻独占。
fn write(registry: &RwLock<Registry>) -> RwLockWriteGuard<'_, Registry> {
    registry.write().unwrap_or_else(PoisonError::into_inner)
}

/// 取 `C` 的阶段表快照：`None` = 注册表未初始化 **或** 该类型零注册。
///
/// 读锁内只做 `get` + `Arc::clone`（原子自增，零堆分配），返回即放锁——阶段在锁外
/// 执行（模块 armor 第 1 条）。共享读锁意味着多个线程的 `post_process` 可同时取快照。
fn snapshot<C: 'static>() -> Option<Stages<C>> {
    let registry = REGISTRY.get()?;
    let map = read(registry);
    Some(Arc::clone(stage_table::<C>(map.get(&TypeId::of::<C>())?)))
}

/// 槽位类型不符的 panic 文案（`TypeId` 键唯一决定槽位类型，故不可能发生）。
const SLOT_MISMATCH: &str =
    "lint 注册表槽位必须存 Arc<Vec<Arc<dyn LintStage<C>>>>（TypeId 键唯一，违反即不变量破坏）";

/// 槽位 downcast：`TypeId` 键唯一决定槽位类型，不命中 = 内部不变量破坏。
fn stage_table<C: 'static>(slot: &Box<dyn Any + Send + Sync>) -> &Stages<C> {
    (**slot).downcast_ref::<Stages<C>>().expect(SLOT_MISMATCH)
}

/// 槽位 downcast（可变侧）：同 [`stage_table`]。
fn stage_table_mut<C: 'static>(slot: &mut Box<dyn Any + Send + Sync>) -> &mut Stages<C> {
    (**slot).downcast_mut::<Stages<C>>().expect(SLOT_MISMATCH)
}

/// 登记一个阶段到 `C` 的阶段表尾（注册序 = 执行序）。
///
/// 语义：
///
/// - 反复登记同一个 `Arc` 即反复执行（**无去重**）；
/// - 阶段项在 `post_process` 进行中选择登记 → **下一次**调用生效（快照语义）；
/// - 同一 `C` 可跨线程并发登记（内部 `RwLock` 写锁串行化，见
///   `concurrent_registration_keeps_every_stage`）；
/// - 注册期会分配（建槽位/追加表项，COW 只在有在飞快照时克隆 Vec）——
///   **热路径纪律只覆盖 [`post_process`]**，不在注册期。
///
/// # Examples
///
/// ```
/// use skeleton::lint::{clear_stages, register_stage, registered_stages, LintError, LintStage};
/// use std::sync::Arc;
///
/// struct Cfg;
///
/// struct Named(&'static str);
/// impl LintStage<Cfg> for Named {
///     fn name(&self) -> &'static str {
///         self.0
///     }
///     fn run(&self, _cfg: &mut Cfg) -> Result<(), LintError> {
///         Ok(())
///     }
/// }
///
/// register_stage::<Cfg>(Arc::new(Named("a")));
/// register_stage::<Cfg>(Arc::new(Named("b")));
/// assert_eq!(registered_stages::<Cfg>(), ["a", "b"]); // 注册序
/// clear_stages::<Cfg>();
/// ```
pub fn register_stage<C: 'static>(stage: Arc<dyn LintStage<C>>) {
    let registry = REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
    let mut map = write(registry);
    let slot = map.entry(TypeId::of::<C>()).or_insert_with(|| {
        let stages: Stages<C> = Arc::new(Vec::new());
        Box::new(stages)
    });
    // COW：仅当已存在在飞快照时才克隆 Vec（注册期，不在 post_process 期）。
    // 锁内析构复核：`make_mut` 克隆的那一拍只 drop 旧 `Arc<Vec>` 的一份引用（另一份在
    // 在飞快照里活着），Vec 缓冲区与阶段对象都不析构 → 写锁内不跑用户 `Drop`（另一处会
    // 析构阶段对象的地方是 `clear_stages`，已改两步式）。
    Arc::make_mut(stage_table_mut::<C>(slot)).push(stage);
}

/// 清掉 `C` 的阶段表（槽位登记项**立即**脱离注册表；底层阶段表与阶段对象随最后一份
/// 在飞快照析构）。
///
/// 语义：只影响**后续**调用——已在飞的 [`post_process`] 持有自己的 `Arc` 快照，会把自己
/// 那拍跑完（契约：注册期与执行期解耦，`clear_stages_does_not_cancel_the_in_flight_tick`）。
/// 内存是**分层**释放的：登记项（`TypeId` → 槽位）这一拍就脱离注册表，后续调用看不到；
/// 而 `Arc<Vec<..>>` 与阶段对象要等最后一份在飞快照放掉才析构，且该析构发生在**锁外**
/// （阶段 `Drop` 可再入注册表，见模块 armor）。
///
/// # Examples
///
/// ```
/// use skeleton::lint::{clear_stages, register_stage, registered_stages, LintError, LintStage};
/// use std::sync::Arc;
///
/// struct Cfg;
///
/// struct S;
/// impl LintStage<Cfg> for S {
///     fn name(&self) -> &'static str {
///         "s"
///     }
///     fn run(&self, _cfg: &mut Cfg) -> Result<(), LintError> {
///         Ok(())
///     }
/// }
///
/// register_stage::<Cfg>(Arc::new(S));
/// assert_eq!(registered_stages::<Cfg>(), ["s"]);
/// clear_stages::<Cfg>();
/// assert!(registered_stages::<Cfg>().is_empty());
/// ```
pub fn clear_stages<C: 'static>() {
    let Some(registry) = REGISTRY.get() else {
        return; // 从未注册过任何类型：无表可清
    };
    // 两步式（不是口味）：`write(..).remove(..)` 单语句的临时量逆序析构会把槽位的
    // 释放（→ 最后一个阶段对象的 `Drop`）拖进写锁内——`Drop` 是用户代码，可再入注册表
    // 就自死锁。先出锁再 drop。
    let removed = write(registry).remove(&TypeId::of::<C>());
    drop(removed);
}

/// `C` 的阶段名单（**注册序**；重复名原样保留）。
///
/// 诊断面专用（日志/断言/运维自检）：返回值的 `Vec` 属诊断分配，**不**算进
/// [`post_process`] 的稳态零分配口径。
///
/// # Examples
///
/// ```
/// use skeleton::lint::{clear_stages, register_stage, registered_stages, LintError, LintStage};
/// use std::sync::Arc;
///
/// struct Cfg;
///
/// struct Named(&'static str);
/// impl LintStage<Cfg> for Named {
///     fn name(&self) -> &'static str {
///         self.0
///     }
///     fn run(&self, _cfg: &mut Cfg) -> Result<(), LintError> {
///         Ok(())
///     }
/// }
///
/// assert!(registered_stages::<Cfg>().is_empty());
/// register_stage::<Cfg>(Arc::new(Named("dup")));
/// register_stage::<Cfg>(Arc::new(Named("dup"))); // 重复名合法：不做唯一性校验
/// assert_eq!(registered_stages::<Cfg>(), ["dup", "dup"]);
/// clear_stages::<Cfg>();
/// ```
#[must_use = "阶段名单是诊断读数：忽略返回值通常意味着漏了断言/日志"]
pub fn registered_stages<C: 'static>() -> Vec<&'static str> {
    match snapshot::<C>() {
        Some(stages) => stages.iter().map(|stage| stage.name()).collect(),
        None => Vec::new(),
    }
}

/// 对 `cfg` 跑完 `C` 的整张阶段表（注册序），首个 `Err` 短路。
///
/// 语义（`tests/lint.rs` 逐条钉死）：
///
/// 1. **空表 → `Ok(())`**；注册表从未初始化时连锁都不碰。
/// 2. **按注册序执行**；阶段在**锁外**跑（体内可 [`register_stage`]、可递归
///    [`post_process`]）。
/// 3. **短路**：任一阶段 `Err` → 后续阶段不跑，`cfg` 保留已发生的改动；错误的
///    [`LintError::stage`] 为空则由编排器补该阶段名（已带归属的不覆盖）。
/// 4. **可改写**：阶段拿 `&mut C`，不只校验。
/// 5. **类型隔离**：只跑 `C` 自己的阶段表，不同 `C` 互不可见。
///
/// 性能口径：稳态（表已注册、无注册并发）下每次调用 = 一次全局读锁 + 一次
/// `Arc::clone`（原子自增）+ 各阶段自身开销——**零堆分配**（`tests/lint_alloc.rs`
/// 事件计数断言 0）。实测 = **单批运行**（`lint_multidim`，release，2026-10-29，本机 16 核，
/// std `Instant` 粗测；原文 `verify/2026-10-29-lint-fix-round/example.txt`）：0/1/5 阶段
/// **17.0 / 28.0 / 38.3 ns/拍**；空阶段 4 线程 **≈0.30x** —— 全局读锁的争用上限，见模块
/// 「收窄声明」末条。panic 不捕获，原样传播。
///
/// # Panics
///
/// 阶段 panic 原样传播（这是契约，不是缺陷：吞 panic 会让「配置没校验」静默过关）。
///
/// # Examples
///
/// ```
/// use skeleton::lint::{clear_stages, post_process, register_stage, LintError, LintStage};
/// use std::sync::Arc;
///
/// #[derive(Default)]
/// struct Cfg {
///     retries: u32,
/// }
///
/// struct Clamp;
/// impl LintStage<Cfg> for Clamp {
///     fn name(&self) -> &'static str {
///         "clamp"
///     }
///     fn run(&self, cfg: &mut Cfg) -> Result<(), LintError> {
///         cfg.retries = cfg.retries.min(3); // 阶段可改写，不只校验
///         Ok(())
///     }
/// }
///
/// // 零注册的进程：空表直接 Ok（连注册表都不初始化）。
/// let mut blank = Cfg { retries: 9 };
/// assert_eq!(post_process::<Cfg>(&mut blank), Ok(()));
///
/// register_stage::<Cfg>(Arc::new(Clamp));
/// let mut cfg = Cfg { retries: 9 };
/// assert_eq!(post_process(&mut cfg), Ok(()));
/// assert_eq!(cfg.retries, 3);
/// clear_stages::<Cfg>();
/// ```
///
/// `C` 必须 `'static`：注册表键是 `TypeId`，借用型配置拿不到进程级稳定身份——
/// 编译期拒绝（不是运行期检查；`E0310` = 类型可能活不够久，即 `&'a str: 'static`
/// 不成立）：
///
/// ```compile_fail,E0310
/// use skeleton::lint::post_process;
///
/// fn borrowed<'a>(cfg: &mut &'a str) {
///     // C = &'a str 未满足 C: 'static → 编译失败（TypeId 键要求进程内恒定身份）。
///     let _ = post_process(cfg);
/// }
/// ```
pub fn post_process<C: 'static>(cfg: &mut C) -> Result<(), LintError> {
    let Some(stages) = snapshot::<C>() else {
        return Ok(());
    };
    for stage in stages.iter() {
        if let Err(mut err) = stage.run(cfg) {
            err.stage.get_or_insert_with(|| stage.name());
            return Err(err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! 私有面测试：锁中毒恢复的**真**覆盖。
    //!
    //! 公开 API 造不出毒化（锁内不跑用户代码，无 panic 窗口），故「`PoisonError::into_inner`
    //! 恢复而不报废注册表」这条 armor 只能在本模块内验证——同 crate 才够得着 `REGISTRY`。

    use super::{
        clear_stages, post_process, register_stage, registered_stages, LintError, LintStage,
        REGISTRY,
    };
    use std::collections::HashMap;
    use std::panic::catch_unwind;
    use std::sync::{Arc, RwLock};

    struct PoisonCfg;

    struct Noop;

    impl LintStage<PoisonCfg> for Noop {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn run(&self, _cfg: &mut PoisonCfg) -> Result<(), LintError> {
            Ok(())
        }
    }

    #[test]
    fn registry_recovers_from_a_poisoned_lock() {
        // 真毒化：持写 guard 时 panic —— `into_inner` 分支的唯一入口。
        let poisoned = catch_unwind(|| {
            let registry = REGISTRY.get_or_init(|| RwLock::new(HashMap::new()));
            let _guard = registry.write().expect("首次取写锁不会中毒");
            panic!("故意毒化 lint 注册表写锁");
        });
        assert!(poisoned.is_err(), "毒化必须由真 panic 造出来");
        let registry = REGISTRY.get().expect("OnceLock 已初始化");
        assert!(registry.is_poisoned(), "写 guard 期间 panic 必须留中毒标记");

        // 两条恢复分支都要真被走到：clear/register 走 write()，post_process 走 read()。
        clear_stages::<PoisonCfg>();
        register_stage::<PoisonCfg>(Arc::new(Noop));
        assert_eq!(post_process(&mut PoisonCfg), Ok(()));
        assert_eq!(registered_stages::<PoisonCfg>(), ["noop"]);
        assert!(
            registry.is_poisoned(),
            "恢复不是清毒：标记仍在，只是不再报废注册表"
        );
        clear_stages::<PoisonCfg>();
    }
}
