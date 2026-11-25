//! shutdown 域：跨组件优雅停机编排器（std-only 常驻，零 feature）。
//!
//! 三现状停机面各管各家——`crate::runtime::SyncStore::close`（关闸+清 pending+
//! 在途排空，幂等）、[`std_ext::scheduler::Scheduler::close`]（关闸+标 Cancelled+
//! 唤醒驱动）、tokio feature 的 `crate::fx::FxScope::shutdown`（drain+超时显式+
//! abort 兜底）；本域把它们按**注册序串行**编排：单组件 panic/超时 fail-visible
//! 上报、不拖垮整体，最终 [`ShutdownReport`] 全量如实（零组件=空报告合法）。
//!
//! **停机序 = 注册序**（逐条串行）。本域不持有依赖图、无法自动排序 ⇒ **依赖方必须先
//! 于被依赖方注册**（先关上层、后关底层；Erlang/OTP 逆启动序口径，外部对标
//! F-11-1）：注册序与依赖序同向（先注册底层）会先关掉别人还依赖的组件。
//!
//! 组件接入形状：`register(名, 停机闭包)`——闭包内自行调用组件自己的停机 API：
//!
//! ```no_run
//! use skeleton_infra::shutdown::ShutdownCoordinator;
//!
//! let coord = std::sync::Arc::new(ShutdownCoordinator::new());
//! coord.register("store", || { /* store.close() */ }).unwrap();
//! coord.register("scheduler", || { /* scheduler.close() */ }).unwrap();
//! let report = coord.shutdown();
//! assert!(report.is_clean());
//! ```
//!
//! sync 局限如实声明：组件闭包无中断面——超时=**放弃等待**（watchdog 线程照跑、
//! detach，报告 `TimedOut`），不是中断。有中断面的组件（如
//! `FxScope::shutdown_timeout`）在闭包内自行组合超时；协调器超时是防单组件挂死
//! 拖垮整体报告的兜底防线。`Duration::MAX` 超时=等到底。
//!
//! watchdog 容量闸：带超时组件的闭包跑在 watchdog 线程上，超时=放弃等待（线程
//! 照跑、detach）——组件大量挂死时「仍在跑的 watchdog 闭包数」若无上界即随组件数
//! 线性增长。`with_max_watchdogs(n)` 给硬上界：许可 spawn 前占用、线程退出时归还
//! （超时后仍在跑的**仍占着**许可）；许可耗尽 → 该组件**未启动**（不 spawn 线程、
//! 不内联跑闭包），如实记 `Failed`——非静默丢弃停机回调。`live_watchdogs()`/
//! `peak_watchdogs()` 给运营读数（区分 `TimedOut` 与仍在跑的 detached 工作）。
//! 默认不限（`None`）。
//!
//! 坑面装甲（每坑→堵法→测试，`tests/shutdown.rs` 红绿；对照表见
//! shutdown 域性能锚（本地留存，未入库））：
//!
//! | 坑 | 堵法 |
//! |---|---|
//! | 组件闭包 panic 炸掉整体 | 内联/线程双路 `catch_unwind` → `Failed{载荷消息}`，后续组件照跑 |
//! | 单组件挂死拖垮整体 | 可配超时（协调器默认+逐组件覆盖），watchdog 线程+`recv_timeout` |
//! | 超时被误读为已停机 | `TimedOut` 显式变体，本文档声明线程照跑（非中断） |
//! | 停机中/后注册静默丢失 | phase 闸，`Err(RegisterError::Closed)` fail-visible |
//! | 重名静默覆盖/双注册 | `Err(RegisterError::Duplicate)` |
//! | 零组件误判失败 | 空报告合法，`is_clean()`=true |
//! | 组件闭包内重入 shutdown 自锁（HANG 无留痕） | 双锚 `ThreadId` 检查：驱动线程（内联闭包）+役中 watchdog 线程集（带超时组件闭包），重入就地 panic → 被编排面捕获记 `Failed`；无关线程并发叫仍=阻塞等首叫同报告 |
//! | watchdog 线程创建失败/上报通道断连被静默 | 显式 `Failed`（fail-visible，`await_watchdog` 四分支单测钉死） |
//! | watchdog 执行数无上界（超时 abandon 后照跑，线程随组件数线性增长） | 容量闸 `with_max_watchdogs` 许可上限 + live/peak 读数；许可耗尽 → 组件**未启动**显式 `Failed` |
//! | 持锁发埋点（订阅器重入 `register`/`is_shutdown` 自锁、panic 毒化注册锁） | 事件字段锁内取定、**提交后放锁再发**（#121） |
//! | 报告埋点先于终态（订阅器 panic/阻塞 ⇒ 已跑完的停机滞留 `Running`，并发叫者永久等 `Done`） | 先提交报告+`Done`+唤醒+放锁，**再发**报告埋点（#193） |
//!
//! log-embed 埋点（级别规范：正常流转=info / 异常终态=warn；target
//! `skeleton::shutdown[::phase]`，字段化纪律见 lib.rs `embed_event!` 注）：
//! registry=注册成功 info（名/注册后计数/超时；注册失败走 Err 返回面不埋）；
//! run=逐组件开始 info + 终态（Stopped=info，TimedOut/Failed=warn）；
//! report=整体报告 info（组件数/三态计数）。事件全在调用方线程发出
//!（watchdog 线程上的组件闭包体零埋点），线程局部订阅器即可捕获。

use std::any::Any;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::mpsc::{Receiver, RecvTimeoutError, channel};
use std::sync::{Arc, Condvar, LazyLock, Mutex, MutexGuard, PoisonError};
use std::thread::{self, ThreadId};
use std::time::Duration;

/// 注册失败（fail-visible，无静默）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    /// 停机已启动/已完成——注册窗口已关。
    Closed,
    /// 同名组件已注册。
    Duplicate(String),
}

impl fmt::Display for RegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RegisterError::Closed => f.write_str("shutdown 已启动/已完成，注册窗口已关"),
            RegisterError::Duplicate(name) => write!(f, "组件名重复: {name}"),
        }
    }
}

impl std::error::Error for RegisterError {}

/// 单组件停机结论。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutcomeKind {
    /// 干净停机。
    Stopped,
    /// 组件闭包 panic / watchdog 异常——原因进 `reason`。
    Failed { reason: String },
    /// 超时放弃等待。**组件线程可能仍在运行**（sync 闭包无中断面，非中断语义）。
    TimedOut,
}

/// 报告行：组件名 + 结论（名字随行——报告自足，调用方无需 zip 注册清单）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentOutcome {
    pub name: String,
    pub kind: OutcomeKind,
}

/// 停机完成报告（注册序排列，全量如实）。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ShutdownReport {
    pub per_component: Vec<ComponentOutcome>,
}

impl ShutdownReport {
    /// 全部 [`OutcomeKind::Stopped`]（零组件=true——无组件无可失败）。
    pub fn is_clean(&self) -> bool {
        self.per_component
            .iter()
            .all(|o| matches!(o.kind, OutcomeKind::Stopped))
    }
}

struct Component {
    name: String,
    timeout: Option<Duration>,
    f: Box<dyn FnOnce() + Send>,
}

/// 容量闸耗尽的失败原因（固定文案：组件**未启动**，非超时、非静默丢弃）。
const WATCHDOG_CAPACITY_EXHAUSTED: &str = "watchdog capacity exhausted; component not started";

/// 役中 watchdog 账本状态（重入装甲第二锚 + 容量闸计数）。
struct WatchdogState {
    /// 役中 watchdog 线程 id（重入装甲第二锚）：带超时组件的闭包跑在协调器自
    /// spawn 的 watchdog 线程上，ThreadId 不等于驱动线程——只查驱动线程锚时其
    /// 重入落并发等待分支（等 Done），timeout=MAX 档双线程互等永久死锁零留痕。
    marks: Vec<ThreadId>,
    /// 已放行但尚未收口的 watchdog 执行数（含超时放弃等待后仍在跑的）。
    live: usize,
    /// `live` 历史峰值（诊断用：区分「曾堆积」与「此刻仍在跑」）。
    peak: usize,
}

/// 役中 watchdog 账本（共享 Arc，watchdog 线程与协调器共写同一份）：惰性建
/// （仅超时组件路径解引用时分配），零构造成本——不并入 `Inner` 以免把 Arc
/// 共享需求传染到构造/注册热路径（register 冷/热分配锚 2.00/1.00 事件）。
type WatchdogLedger = Arc<Mutex<WatchdogState>>;

/// watchdog 执行许可（RAII）：放行时占用（spawn 前），归还于**持有者消亡**
/// ——watchdog 线程退出（正常完成/panic）或**起线程失败**（std 丢弃闭包 ⇒ 闭包
/// 内的许可随之消亡）时自动归还，不靠手写每条分支。
struct WatchdogPermit {
    state: Arc<Mutex<WatchdogState>>,
}

impl WatchdogPermit {
    /// 占用许可：容量闸通过 → Some(许可)；已满 → None（**不 spawn、不内联跑**）。
    fn acquire(ledger: &WatchdogLedger, max_watchdogs: Option<usize>) -> Option<Self> {
        let mut g = lock(ledger);
        if let Some(cap) = max_watchdogs {
            if g.live >= cap {
                return None;
            }
        }
        g.live += 1;
        g.peak = g.peak.max(g.live);
        drop(g);
        Some(Self {
            state: Arc::clone(ledger),
        })
    }
}

impl Drop for WatchdogPermit {
    fn drop(&mut self) {
        let mut g = lock(&self.state);
        g.live -= 1;
    }
}

/// watchdog 役中守卫（RAII）：摘掉役中线程牌（重入装甲第二锚）——每次退出路径
/// 都摘（含 panic），不靠手写每条分支。许可归还由 [`WatchdogPermit`] 承担。
struct WatchdogGuard {
    state: Arc<Mutex<WatchdogState>>,
    id: ThreadId,
    /// 许可随守卫同寿：线程退出（守卫析构）即归还。
    _permit: WatchdogPermit,
}

impl Drop for WatchdogGuard {
    fn drop(&mut self) {
        let mut g = lock(&self.state);
        g.marks.retain(|&id| id != self.id);
    }
}

impl Component {
    /// 单组件执行：无超时=驱动线程内联跑（零 watchdog 线程开销，panic 同样
    /// 就地捕获）；有超时=watchdog 线程+通道等待。
    ///
    /// watchdog 路径先登记线程 id 再跑闭包（执行窗），闭包结束即摘牌——此后
    /// 闭包内任何 `shutdown()` 调用必能在装甲检查中看到本线程，登记先于执行
    /// 无逃逸窗。
    ///
    /// 容量闸（`max_watchdogs`）：watchdog 执行许可在 spawn 前占用、线程退出时
    /// 归还（超时放弃等待后仍在跑的**仍占着**许可）——许可耗尽则该组件
    /// **未启动**，如实记 `Failed`（不 spawn 线程、不内联跑闭包）。
    fn run(
        self,
        watchdogs: &LazyLock<WatchdogLedger>,
        max_watchdogs: Option<usize>,
    ) -> ComponentOutcome {
        let Component { name, timeout, f } = self;
        // 埋点「逐组件开始」：注册序串行执行每组件一条 info。
        embed_event!(
            info,
            target: "skeleton::shutdown::run",
            name = %name,
            timeout_ms = timeout.map(|d| d.as_millis() as u64)
        );
        let outcome = match timeout {
            None => match catch_unwind(AssertUnwindSafe(f)) {
                Ok(()) => ComponentOutcome {
                    name,
                    kind: OutcomeKind::Stopped,
                },
                Err(payload) => ComponentOutcome {
                    name,
                    kind: OutcomeKind::Failed {
                        reason: panic_reason(payload),
                    },
                },
            },
            Some(d) => run_timed(name, d, f, watchdogs, max_watchdogs),
        };
        // 埋点「终态」：Stopped=info，TimedOut/Failed=warn（异常路径留痕）。
        #[cfg(feature = "log-embed")]
        match &outcome.kind {
            OutcomeKind::Stopped => embed_event!(
                info,
                target: "skeleton::shutdown::run",
                name = %outcome.name,
                kind = "stopped"
            ),
            OutcomeKind::TimedOut => embed_event!(
                warn,
                target: "skeleton::shutdown::run",
                name = %outcome.name,
                kind = "timed_out"
            ),
            OutcomeKind::Failed { reason } => embed_event!(
                warn,
                target: "skeleton::shutdown::run",
                name = %outcome.name,
                kind = "failed",
                reason = %reason
            ),
        }
        outcome
    }
}

/// watchdog 路径（带超时组件）：容量闸 → spawn watchdog 线程 → 等结果
/// （超时=放弃等待，线程照跑、detach）。
fn run_timed(
    name: String,
    timeout: Duration,
    f: Box<dyn FnOnce() + Send>,
    watchdogs: &LazyLock<WatchdogLedger>,
    max_watchdogs: Option<usize>,
) -> ComponentOutcome {
    let (tx, rx) = channel();
    let ledger = Arc::clone(watchdogs);
    match WatchdogPermit::acquire(&ledger, max_watchdogs) {
        // 许可耗尽：不 spawn 线程、不内联跑闭包——组件**未启动**，显式 Failed
        // 上报（fail-visible，禁静默丢弃停机回调）。
        None => ComponentOutcome {
            name,
            kind: OutcomeKind::Failed {
                reason: WATCHDOG_CAPACITY_EXHAUSTED.into(),
            },
        },
        Some(permit) => {
            let state = Arc::clone(&ledger);
            // 线程名带组件名：超时 detach 后宿主仍可归因谁在照跑。
            let spawn = thread::Builder::new()
                .name(format!("shutdown-{name}"))
                .spawn(move || {
                    let me = thread::current().id();
                    // 守卫先建后跑：任何退出路径（含 panic）都归还许可。
                    let guard = WatchdogGuard {
                        state: Arc::clone(&state),
                        id: me,
                        _permit: permit,
                    };
                    lock(&state).marks.push(me);
                    let result = catch_unwind(AssertUnwindSafe(f)).map_err(panic_reason);
                    drop(guard); // 先摘牌归还许可再上报：观察者确定性看到归还
                    let _ = tx.send(result);
                });
            match spawn {
                // 起线程失败（OS 资源面）：如实 Failed，不静默跳过该组件；线程从未
                // 启动 ⇒ std 丢弃闭包 ⇒ 闭包内的许可随之归还。
                Err(e) => ComponentOutcome {
                    name,
                    kind: OutcomeKind::Failed {
                        reason: format!("watchdog 线程创建失败: {e}"),
                    },
                },
                Ok(handle) => await_watchdog(name, timeout, rx, handle),
            }
        }
    }
}

/// watchdog 等待收口（独立成函数：四分支在 `#[cfg(test)]` 直接钉死）。
fn await_watchdog(
    name: String,
    timeout: Duration,
    rx: Receiver<Result<(), String>>,
    handle: thread::JoinHandle<()>,
) -> ComponentOutcome {
    let kind = match rx.recv_timeout(timeout) {
        Ok(Ok(())) => OutcomeKind::Stopped,
        Ok(Err(reason)) => OutcomeKind::Failed { reason },
        // 超时=放弃等待：watchdog 线程照跑（detach），迟到的 send 打到已弃收端
        // 被忽略——`let _` 即本语义，非遗漏。
        Err(RecvTimeoutError::Timeout) => OutcomeKind::TimedOut,
        // 通道先断=watchdog 未上报即终：join 收口归因（panic 载荷或不可达路径
        // 如实报 Failed，禁谎报 Stopped）。
        Err(RecvTimeoutError::Disconnected) => match handle.join() {
            Err(payload) => OutcomeKind::Failed {
                reason: panic_reason(payload),
            },
            Ok(()) => OutcomeKind::Failed {
                reason: "watchdog 线程未上报即干净退出（不可达路径，如实上报）".into(),
            },
        },
    };
    ComponentOutcome { name, kind }
}

/// panic 载荷 → 人读消息（`&'static str`/`String` 常见两形，其余如实兜底）。
fn panic_reason(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "panic（非字符串载荷）".into()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// 注册窗开。
    Open,
    /// 编排进行中（`driver`=驱动线程——重入装甲锚）。
    Running { driver: ThreadId },
    /// 已完成（`report` 就位，二次叫取克隆）。
    Done,
}

struct Inner {
    components: Vec<Component>,
    phase: Phase,
    report: Option<ShutdownReport>,
}

/// 跨组件优雅停机编排器（`Arc` 共享线程安全；注册窗与编排窗互斥）。
pub struct ShutdownCoordinator {
    inner: Mutex<Inner>,
    done: Condvar,
    default_timeout: Option<Duration>,
    /// 役中 watchdog 账本（重入装甲第二锚 + 容量闸计数，见 [`WatchdogLedger`]）。
    watchdogs: LazyLock<WatchdogLedger>,
    /// watchdog 执行许可上限（`None`=不限；见 [`Self::with_max_watchdogs`]）。
    max_watchdogs: Option<usize>,
}

/// 取锁：毒锁恢复继续（`PoisonError::into_inner`，scheduler/clock/ratelimit 同款）
/// ——重入装甲的 panic 发生在持锁点且状态未变异，恢复继续不株连其他线程。
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ShutdownCoordinator {
    /// 新协调器：注册窗开，无默认超时（无超时组件等到底）。
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                components: Vec::new(),
                phase: Phase::Open,
                report: None,
            }),
            done: Condvar::new(),
            default_timeout: None,
            watchdogs: LazyLock::new(|| {
                Arc::new(Mutex::new(WatchdogState {
                    marks: Vec::new(),
                    live: 0,
                    peak: 0,
                }))
            }),
            max_watchdogs: None,
        }
    }

    /// 设协调器默认超时（普通注册继承；逐组件可覆盖）。builder 沿
    /// `FxScope::with_concurrency` 形。
    pub fn with_default_timeout(mut self, d: Duration) -> Self {
        self.default_timeout = Some(d);
        self
    }

    /// 设 watchdog 容量上限（同时在跑的 watchdog 闭包数硬上界；**按协调器计**——
    /// 各协调器的许可池彼此独立）。
    ///
    /// 口径**不是并发池**：上限计的是「已放行但尚未收口」的 watchdog 执行数
    /// ——含超时放弃等待后仍在跑（detach）的那些。协调器串行执行组件，故 N 的
    /// 实际含义=「最多允许 N 个 watchdog 闭包同时存在（含已放弃等待仍在跑的）」。
    ///
    /// 许可耗尽时：不 spawn 线程、不内联跑闭包，该组件**未启动**，如实记
    /// `Failed{reason}`（fail-visible，禁静默丢弃停机回调）。默认不限（`None`）。
    pub fn with_max_watchdogs(mut self, max: usize) -> Self {
        self.max_watchdogs = Some(max);
        self
    }

    /// 注册组件（名 + 停机闭包；超时=协调器默认，无默认=等到底）。
    ///
    /// 停机启动后/完成后=Err([`RegisterError::Closed`])；重名=
    /// Err([`RegisterError::Duplicate`])。名字一次分配随组件存续到报告行
    /// （move，零二次分配）。
    ///
    /// 成功注册的埋点**在提交并放锁之后**发出（#121）：订阅器是同步用户回调，
    /// 其内重入 `register`/`is_shutdown` 不会自锁、其 panic 也不会毒化注册锁；
    /// 事件描述的是已提交的注册（名+提交后计数），被拒注册不发事件。
    pub fn register<F>(&self, name: impl Into<String>, shutdown: F) -> Result<(), RegisterError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.register_inner(name.into(), self.default_timeout, Box::new(shutdown))
    }

    /// 同 [`Self::register`]，逐组件显式超时（覆盖协调器默认）。
    pub fn register_with_timeout<F>(
        &self,
        name: impl Into<String>,
        shutdown: F,
        timeout: Duration,
    ) -> Result<(), RegisterError>
    where
        F: FnOnce() + Send + 'static,
    {
        self.register_inner(name.into(), Some(timeout), Box::new(shutdown))
    }

    fn register_inner(
        &self,
        name: String,
        timeout: Option<Duration>,
        f: Box<dyn FnOnce() + Send>,
    ) -> Result<(), RegisterError> {
        let mut g = lock(&self.inner);
        if !matches!(g.phase, Phase::Open) {
            return Err(RegisterError::Closed);
        }
        if g.components.iter().any(|c| c.name == name) {
            return Err(RegisterError::Duplicate(name));
        }
        // 提交组件并取定事件字段（#121）：埋点在**锁外**发——tracing 订阅器是
        // 同步用户回调，持锁发会让其重入 register/is_shutdown 自锁、panic 还会
        // 毒化注册锁。事件字段全是锁内取定的不可变值（名 + 已提交后的计数），
        // 故锁外发出的事件描述的是**已提交**的注册；被拒注册不发事件（早返）。
        #[cfg(feature = "log-embed")]
        let event_name = name.clone();
        g.components.push(Component { name, timeout, f });
        #[cfg(feature = "log-embed")]
        let count = g.components.len() as u64;
        drop(g);
        // 埋点「注册」：成功注册一条 info（名/注册后计数/超时毫秒；None=等到底，
        // Option None 字段缺席=tracing 语义）。
        #[cfg(feature = "log-embed")]
        embed_event!(
            info,
            target: "skeleton::shutdown::registry",
            name = %event_name,
            count = count,
            timeout_ms = timeout.map(|d| d.as_millis() as u64)
        );
        Ok(())
    }

    /// 编排停机：注册序串行执行全部组件，返回全量报告。幂等——二次叫（含并发
    /// 叫）阻塞等首叫完成，返回同一报告。组件闭包内重入（内联驱动线程/役中
    /// watchdog 线程双锚）=panic 装甲（被编排面捕获记 `Failed`，非 HANG 无
    /// 留痕）；无关线程的并发叫=合法等待者，等首叫同报告。
    ///
    /// **诊断在终态之后**（#193）：报告先提交、phase→`Done`、唤醒全部等待者并
    /// 放锁，之后才发 `skeleton::shutdown::report` 埋点。订阅器是同步用户回调
    /// （可阻塞/可重入/可 panic）——放在提交前会让已跑完的停机滞留 `Running`、
    /// 并发叫者永久等 `Done`。panic 由 embed 边界就地 contain（#225），不回滚
    /// 终态；故首叫之后（含并发叫）恒取到同一份已提交报告。
    pub fn shutdown(&self) -> ShutdownReport {
        let mut g = lock(&self.inner);
        match g.phase {
            Phase::Done => {
                return g
                    .report
                    .clone()
                    .expect("phase=Done 时首叫报告必已就位（Done 只在 report 赋值后置位）");
            }
            // 重入装甲：组件闭包在本线程内联跑时再叫 shutdown=自等自锁。
            // fail-fast 成显式 panic → 编排面 catch_unwind → Failed 上报。
            Phase::Running { driver } if driver == thread::current().id() => panic!(
                "ShutdownCoordinator::shutdown 重入：组件闭包内禁止再次调用（自锁防线）——需要子编排请新起独立协调器"
            ),
            Phase::Running { .. } => {
                // 重入装甲第二锚：役中 watchdog 线程=带超时组件闭包的执行窗，
                // 其 shutdown 调用是闭包重入而非合法并发等待者——落等待分支
                // 即双线程互等（驱动等 watchdog 收口、watchdog 等 Done），
                // timeout=MAX 档永久 HANG 零留痕（盲评 F-1 实弹）。就地 panic
                // → 该线程 catch_unwind → Failed 上报。
                let me = thread::current().id();
                let in_window = lock(&*self.watchdogs).marks.contains(&me);
                if in_window {
                    panic!(
                        "ShutdownCoordinator::shutdown 重入：watchdog 线程上的组件闭包内禁止再次调用（自锁防线）——需要子编排请新起独立协调器"
                    );
                }
                // 并发第二叫：阻塞等首叫完成拿同一报告（不与首叫竞争执行）。
                while !matches!(g.phase, Phase::Done) {
                    g = self.done.wait(g).unwrap_or_else(PoisonError::into_inner);
                }
                return g
                    .report
                    .clone()
                    .expect("phase=Done 时首叫报告必已就位（Done 只在 report 赋值后置位）");
            }
            Phase::Open => {}
        }
        g.phase = Phase::Running {
            driver: thread::current().id(),
        };
        let components = std::mem::take(&mut g.components);
        drop(g); // 编排全程不持内锁：组件闭包内 register/is_shutdown 合法（重入除外）

        let per_component = components
            .into_iter()
            .map(|c| c.run(&self.watchdogs, self.max_watchdogs))
            .collect();
        let report = ShutdownReport { per_component };
        // 事件字段先取定（#193）：报告要随终态提交给等待者，之后才发诊断。
        #[cfg(feature = "log-embed")]
        let (components, stopped, timed_out, failed) = {
            let (mut stopped, mut timed_out, mut failed) = (0u64, 0u64, 0u64);
            for o in &report.per_component {
                match o.kind {
                    OutcomeKind::Stopped => stopped += 1,
                    OutcomeKind::TimedOut => timed_out += 1,
                    OutcomeKind::Failed { .. } => failed += 1,
                }
            }
            (
                report.per_component.len() as u64,
                stopped,
                timed_out,
                failed,
            )
        };

        let mut g = lock(&self.inner);
        g.report = Some(report.clone());
        g.phase = Phase::Done;
        self.done.notify_all(); // 锁内改状态后唤醒并发第二叫，无丢唤窗
        drop(g);
        // 埋点「整体报告」在**终态提交 + 唤醒 + 放锁之后**发（#193）：订阅器是
        // 同步用户回调（可阻塞/可重入/可 panic）——放在提交前会让已完成停机滞留
        // `Running`、并发叫者永久等 Done。此刻并发第二叫已能取到已提交报告；
        // panic 由 embed 边界就地 contain（#225 计数可见），即使逃逸也不回滚终态。
        #[cfg(feature = "log-embed")]
        embed_event!(
            info,
            target: "skeleton::shutdown::report",
            components = components,
            stopped = stopped,
            timed_out = timed_out,
            failed = failed
        );
        report
    }

    /// shutdown 已启动（含进行中）或已完成（闸语义同 `SyncStore::is_closed`）。
    pub fn is_shutdown(&self) -> bool {
        !matches!(lock(&self.inner).phase, Phase::Open)
    }

    /// 当前在跑的 watchdog 闭包数（已放行未收口；含超时放弃等待后仍在跑的）。
    /// ——运营读数：区分「已放弃等待（`TimedOut`）」与「仍在跑的 detached 工作」。
    pub fn live_watchdogs(&self) -> usize {
        lock(&*self.watchdogs).live
    }

    /// [`Self::live_watchdogs`] 历史峰值（诊断用：区分「曾堆积」与「此刻仍在跑」）。
    pub fn peak_watchdogs(&self) -> usize {
        lock(&*self.watchdogs).peak
    }
}

impl Default for ShutdownCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // panic 载荷三形：&'static str / String / 非字符串兜底。
    #[test]
    fn panic_reason_extracts_payloads() {
        assert_eq!(panic_reason(Box::new("静态串")), "静态串");
        assert_eq!(panic_reason(Box::new("拥有串".to_string())), "拥有串");
        assert_eq!(panic_reason(Box::new(42u32)), "panic（非字符串载荷）");
    }

    // watchdog 收口四分支，逐支钉死（公开面经 tests/shutdown.rs，此为分支面）。
    #[test]
    fn await_watchdog_reports_stopped() {
        let (tx, rx) = channel();
        let h = thread::spawn(move || tx.send(Ok(())).unwrap());
        let o = await_watchdog("a".into(), Duration::from_secs(5), rx, h);
        assert_eq!(o.name, "a");
        assert_eq!(o.kind, OutcomeKind::Stopped);
    }

    #[test]
    fn await_watchdog_reports_failed_payload() {
        let (tx, rx) = channel();
        let h = thread::spawn(move || tx.send(Err("组件炸了".into())).unwrap());
        let o = await_watchdog("a".into(), Duration::from_secs(5), rx, h);
        assert_eq!(
            o.kind,
            OutcomeKind::Failed {
                reason: "组件炸了".into()
            }
        );
    }

    #[test]
    fn await_watchdog_reports_timed_out() {
        let (_tx, rx) = channel::<Result<(), String>>();
        let h = thread::spawn(|| thread::sleep(Duration::from_millis(500)));
        let o = await_watchdog("a".into(), Duration::from_millis(20), rx, h);
        assert_eq!(o.kind, OutcomeKind::TimedOut);
    }

    // Disconnected+join Err（watchdog 未上报即 panic）：panic 载荷归因进 Failed。
    // 装甲注：必须 drop(tx) 而非 let _ = tx——edition 2021 disjoint capture 下
    // `let _ = place` 是无绑定求值=不捕获，tx 会留在测试帧里，线程收不到发送端
    // （recv 必 Timeout——本测试曾因此误判 std 断连失效）。
    #[test]
    fn await_watchdog_disconnected_join_err_becomes_failed() {
        let (tx, rx) = channel::<Result<(), String>>();
        let h = thread::spawn(move || {
            drop(tx); // 显式弃发送端 → 收端见断连
            panic!("watchdog 本体炸了");
        });
        let o = await_watchdog("a".into(), Duration::from_secs(5), rx, h);
        match o.kind {
            OutcomeKind::Failed { reason } => assert!(reason.contains("watchdog 本体炸了")),
            k => panic!("须 Failed，实为 {k:?}"),
        }
    }

    // Disconnected+join Ok（不可达路径）：如实 Failed，禁谎报 Stopped。
    #[test]
    fn await_watchdog_disconnected_join_ok_reports_honest_failure() {
        let (tx, rx) = channel::<Result<(), String>>();
        drop(tx); // 提前弃发送端：收端即见 Disconnected
        let h = thread::spawn(|| {});
        let o = await_watchdog("a".into(), Duration::from_secs(5), rx, h);
        assert!(
            matches!(o.kind, OutcomeKind::Failed { .. }),
            "不可达路径须如实报 Failed，实为 {:?}",
            o.kind
        );
    }

    // 容量闸记账（白盒单测）：许可 acquire/release 语义——达上限则 acquire 返
    // None（不占许可）；持有者消亡即归还（与「起线程失败时 std 丢弃闭包 ⇒ 闭包
    // 内许可随之消亡」同一机制，故该路径的归还亦被此单测钉住）。
    #[test]
    fn watchdog_permit_accounting_releases_on_drop() {
        let ledger: WatchdogLedger = Arc::new(Mutex::new(WatchdogState {
            marks: Vec::new(),
            live: 0,
            peak: 0,
        }));
        let permit = WatchdogPermit::acquire(&ledger, Some(1)).expect("首个许可应放行");
        assert_eq!(lock(&ledger).live, 1);
        assert_eq!(lock(&ledger).peak, 1);
        assert!(
            WatchdogPermit::acquire(&ledger, Some(1)).is_none(),
            "达上限须拒发许可"
        );
        drop(permit); // 持有者消亡 → 归还
        assert_eq!(lock(&ledger).live, 0);
        let permit = WatchdogPermit::acquire(&ledger, Some(1)).expect("归还后应可再放行");
        assert_eq!(lock(&ledger).live, 1);
        drop(permit);
        assert_eq!(lock(&ledger).live, 0);
    }
}
