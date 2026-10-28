//! shutdown 域：跨组件优雅停机编排器（std-only 常驻，零 feature）。
//!
//! 三现状停机面各管各家——[`crate::runtime::SyncStore::close`]（关闸+清 pending+
//! 在途排空，幂等）、[`std_ext::scheduler::Scheduler::close`]（关闸+标 Cancelled+
//! 唤醒驱动）、tokio feature 的 [`crate::fx::FxScope::shutdown`]（drain+超时显式+
//! abort 兜底）；本域把它们按**注册序串行**编排：单组件 panic/超时 fail-visible
//! 上报、不拖垮整体，最终 [`ShutdownReport`] 全量如实（零组件=空报告合法）。
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
//!
//! log-embed 埋点（级别规范：正常流转=info / 异常终态=warn；target
//! `skeleton::shutdown[::phase]`，字段化纪律见 lib.rs `embed_event!` 注）：
//! registry=注册成功 info（名/注册后计数/超时；注册失败走 Err 返回面不埋）；
//! run=逐组件开始 info + 终态（Stopped=info，TimedOut/Failed=warn）；
//! report=整体报告 info（组件数/三态计数）。事件全在调用方线程发出
//!（watchdog 线程上的组件闭包体零埋点），线程局部订阅器即可捕获。

use std::any::Any;
use std::fmt;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
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

/// 役中 watchdog 线程集（重入装甲第二锚）：带超时组件的闭包跑在协调器自
/// spawn 的 watchdog 线程上，ThreadId 不等于驱动线程——只查驱动线程锚时其
/// 重入落并发等待分支（等 Done），timeout=MAX 档双线程互等永久死锁零留痕。
/// 惰性建（仅超时组件路径解引用时分配），零构造成本——不并入 `Inner` 以免
/// 把 Arc 共享需求传染到构造/注册热路径（register 冷/热分配锚 2.00/1.00 事件）。
type WatchdogSet = Arc<Mutex<Vec<ThreadId>>>;

impl Component {
    /// 单组件执行：无超时=驱动线程内联跑（零 watchdog 线程开销，panic 同样
    /// 就地捕获）；有超时=watchdog 线程+通道等待。
    ///
    /// watchdog 路径先登记线程 id 再跑闭包（执行窗），闭包结束即摘牌——此后
    /// 闭包内任何 `shutdown()` 调用必能在装甲检查中看到本线程，登记先于执行
    /// 无逃逸窗。
    fn run(self, watchdogs: &LazyLock<WatchdogSet>) -> ComponentOutcome {
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
            Some(d) => {
                let (tx, rx) = channel();
                let marks = Arc::clone(watchdogs);
                // 线程名带组件名：超时 detach 后宿主仍可归因谁在照跑。
                let spawn = thread::Builder::new()
                    .name(format!("shutdown-{name}"))
                    .spawn(move || {
                        let me = thread::current().id();
                        lock(&marks).push(me);
                        let result = catch_unwind(AssertUnwindSafe(f)).map_err(panic_reason);
                        lock(&marks).retain(|&id| id != me);
                        let _ = tx.send(result);
                    });
                match spawn {
                    // 起线程失败（OS 资源面）：如实 Failed，不静默跳过该组件。
                    Err(e) => ComponentOutcome {
                        name,
                        kind: OutcomeKind::Failed {
                            reason: format!("watchdog 线程创建失败: {e}"),
                        },
                    },
                    Ok(handle) => await_watchdog(name, d, rx, handle),
                }
            }
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
    /// 役中 watchdog 线程集（重入装甲第二锚，见 [`WatchdogSet`]）。
    watchdogs: LazyLock<WatchdogSet>,
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
            watchdogs: LazyLock::new(|| Arc::new(Mutex::new(Vec::new()))),
        }
    }

    /// 设协调器默认超时（普通注册继承；逐组件可覆盖）。builder 沿
    /// `FxScope::with_concurrency` 形。
    pub fn with_default_timeout(mut self, d: Duration) -> Self {
        self.default_timeout = Some(d);
        self
    }

    /// 注册组件（名 + 停机闭包；超时=协调器默认，无默认=等到底）。
    ///
    /// 停机启动后/完成后=Err([`RegisterError::Closed`])；重名=
    /// Err([`RegisterError::Duplicate`])。名字一次分配随组件存续到报告行
    /// （move，零二次分配）。
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
        // 埋点「注册」：成功注册一条 info（名/注册后计数/超时毫秒；None=等到底，
        // Option None 字段缺席=tracing 语义）。
        embed_event!(
            info,
            target: "skeleton::shutdown::registry",
            name = %name,
            count = g.components.len() as u64 + 1,
            timeout_ms = timeout.map(|d| d.as_millis() as u64)
        );
        g.components.push(Component { name, timeout, f });
        Ok(())
    }

    /// 编排停机：注册序串行执行全部组件，返回全量报告。幂等——二次叫（含并发
    /// 叫）阻塞等首叫完成，返回同一报告。组件闭包内重入（内联驱动线程/役中
    /// watchdog 线程双锚）=panic 装甲（被编排面捕获记 `Failed`，非 HANG 无
    /// 留痕）；无关线程的并发叫=合法等待者，等首叫同报告。
    pub fn shutdown(&self) -> ShutdownReport {
        let mut g = lock(&self.inner);
        match g.phase {
            Phase::Done => {
                return g
                    .report
                    .clone()
                    .expect("phase=Done 时首叫报告必已就位（Done 只在 report 赋值后置位）")
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
                let in_window = lock(&*self.watchdogs).contains(&me);
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
            .map(|c| c.run(&self.watchdogs))
            .collect();
        let report = ShutdownReport { per_component };
        // 埋点「整体报告」：编排收口一条 info（组件数/三态计数）。
        #[cfg(feature = "log-embed")]
        {
            let (mut stopped, mut timed_out, mut failed) = (0u64, 0u64, 0u64);
            for o in &report.per_component {
                match o.kind {
                    OutcomeKind::Stopped => stopped += 1,
                    OutcomeKind::TimedOut => timed_out += 1,
                    OutcomeKind::Failed { .. } => failed += 1,
                }
            }
            embed_event!(
                info,
                target: "skeleton::shutdown::report",
                components = report.per_component.len() as u64,
                stopped = stopped,
                timed_out = timed_out,
                failed = failed
            );
        }

        let mut g = lock(&self.inner);
        g.report = Some(report.clone());
        g.phase = Phase::Done;
        self.done.notify_all(); // 锁内改状态后唤醒并发第二叫，无丢唤窗
        report
    }

    /// shutdown 已启动（含进行中）或已完成（闸语义同 `SyncStore::is_closed`）。
    pub fn is_shutdown(&self) -> bool {
        !matches!(lock(&self.inner).phase, Phase::Open)
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
}
