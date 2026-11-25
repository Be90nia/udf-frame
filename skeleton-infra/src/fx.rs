//! 托管 spawn 唯一合法口：`spawn_tracked`（JoinSet 生命周期集 + oneshot 结果旁路，
//! scope drop ⇒ 全组 abort）与 `spawn_detached`（显式脱管，drop 语义相反的另一半）
//! 双命名 + accepting 闸 + 取消令牌；两套语义对照见 [`FxScope::spawn_tracked`] 文档。
//!
//! feature 门 `tokio`。绑 tokio（JoinSet/oneshot/watch/time::timeout/
//! Semaphore）；CancellationToken 树/tokio-util 不引入（语义零增量+父 drop 陷阱）。
//!
//! 零依赖纪律（同 journal.rs/text.rs 先例）：`thiserror::Error` 派生
//! 以手写同形 `Display`/`Error` impl 落地（错误文案与派生形态逐字一致）；
//! 引入 thiserror 依赖时统一换回派生。
//!
//! log-embed 埋点（级别规范：异常路径=warn）：任务 panic 且句柄已被丢
//! （结果无处投递）=warn 一行，target `skeleton::fx`。宏无订阅者时 no-op
//! ——不 panic/不锁/不阻塞，logx 未初始化即静默退让（tracing 语义）。
//! **成因不可被遥测顶替（issue#201）**：事件在 panic 结果已成、投递已定之后才发；
//! 订阅者 panic 被宏的 catch_unwind 边界吞掉并计入 `embedded_event_panic_count`，
//! 生命周期集收到的仍是原任务的 panic，而非订阅者的。
//!
//! 自停机禁令（issue#133）：本 scope 的托管任务**不得**调用本 scope 的
//! `shutdown`/`shutdown_timeout`——它自身就在待 abort 的生命周期集里。任务体 poll
//! 期挂 scope 标记（RAII 还原，见 `ScopeScoped`），停机口在赢 `Open → Draining`
//! **之前**判定并 fail-fast panic（不关闸、不改排空状态）；外部调用者语义不变。
//! 详见 [`FxScope::shutdown`] 的「谁可以调用」。
//!
//! 增量收割面（issue#13）：完成条目滞留集合到 [`FxScope::try_reap`] 显式收割；
//! `shutdown`/`shutdown_timeout` 排空产出 [`ShutdownReport`]（终局五桶 completed/
//! cancelled/panicked/failed/still_running + 截止计数 timed_out + 中断标记
//! drain_interrupted；超时不等于已终止，中断也不等于已终止），并发停机共享一次
//! 排空（owner/等待者/已结算三角色，issue#70），panic 载荷 join 与收割双重可见
//! （诊断面语义非 exactly-once）。
//!
//! 三资源轴（issue#39）：`max_concurrency`（并发许可）/ `max_pending`（已准入
//! 但等待许可）/ `max_tracked`（集合保留条目，含已完成未收割）互相独立——
//! `with_concurrency` **不是**内存/保留量上界（保留上界是 `with_max_tracked`）；
//! 有界准入口 `try_spawn_tracked` 过载时 typed 拒绝（`AdmissionError`），
//! 不阻塞不 panic；计数面见 `FxScope::counts`。
//!
//! # 执行模型与阻塞边界（issue#94）
//!
//! 本域只托管**协作式** async：契约是被托管的 future 在有限时间内返回 poll，长工作段
//! 定期让渡。三类「阻塞」必须分清，只有第一类属于本域能保证的范围：
//!
//! | 类别 | 形态 | 对 executor 的影响 |
//! |---|---|---|
//! | ① 协作式 async 等待 | `.await` 未就绪的 IO/timer/通道 | 返回 `Poll::Pending`，executor 照常 poll 其他任务与计时器 |
//! | ② 同步阻塞 / 长时不 yield | poll 内忙等、同步锁等待、同步析构 | 占住 executor 线程；current-thread runtime 上 timer、取消观察、停机等待全部停摆 |
//! | ③ 真正的 blocking work | 文件 IO、CPU 密集、阻塞系统调用 | 应移出 async executor（`tokio::task::spawn_blocking` 或消费方自备 `ThreadPool`），再 await / 收集结果 |
//!
//! 框架**不做** blocking adapter：不承诺能强制终止任意 Rust 代码，也不在 `FxScope`
//! 内部再造 watchdog 线程伪装可抢占（#94 设计决策）——②③ 的适配责任在消费方。
//!
//! ## 截止不是硬墙钟上限
//!
//! [`FxScope::shutdown_timeout`] 与 [`Spawned::join_timeout`] 的 `d` 依赖 tokio timer：
//! 只在 **executor 可继续调度**时才保证「到期触发并请求 abort」。某个 poll 卡在②的
//! 同步段里时，截止即便已过，也只能等该段返回、executor 重新拿到执行权后才被观察到
//! ⇒ `d` 不是对同步阻塞代码的硬上限。abort 是取消请求，任务在下一个让渡点才终止；
//! 无 await 的同步段内 abort 排不上。故 [`TaskError::TimedOut`]、
//! [`ShutdownReport::timed_out`]、[`ShutdownReport::still_running`] 的口径恒为
//! 「截止已过 / 已请求 abort / 终止未观测」，**不是**「已终止」。
//!
//! ## runtime profile 选择
//!
//! 纯 async、无阻塞任务可用 current-thread runtime；含 blocking work 的应用必须
//! multi-thread runtime（或独立 worker）。UDF 不替消费方创建/切换 runtime；
//! current-thread 上**不要**用 `block_in_place`（该 API 不适用于 current-thread）。
//! 也不要指望在被②占住的 worker 上推进 timer/截止。两侧由 `tests/fx_lifecycle.rs`
//! 钉住：`current_thread_blocked_poll_cannot_observe_shutdown_deadline`（阻塞期间截止
//! 无法被观察，放行后只能看到「已过期」）与
//! `multi_worker_observes_shutdown_deadline_while_one_worker_is_blocked`（另一 worker
//! 照常观察截止，且被阻塞任务不被标成已终止）。

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot, watch};
use tokio::task::{AbortHandle, JoinError, JoinSet};

/// std Mutex 取锁：中毒 = 持锁方已 panic、状态不可信——fail-fast 传播 panic
/// （核心壳 `lock` 同形，禁静默吞毒锁）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("mutex poisoned: prior panic while holding the lock")
}

/// runtime 上下文预检（issue#92）：**必须在取任何内部锁（`set` / `caps`）之前**调用。
///
/// 环境错误先于准入判定 = 宿主 Future 不会在锁持有期析构（其 Drop 可重入本 scope，
/// 持锁即自锁），环境错误也不会以 panic 形态从锁内炸出。
fn runtime_handle() -> Result<Handle, AdmissionError> {
    Handle::try_current().map_err(|_| AdmissionError::NoRuntime)
}

/// 任务结局（闭合 enum；Panicked 经 catch_unwind 归一）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    /// 发送端消亡（abort / scope drop）时 recv 侧的映射。
    Cancelled,
    /// panic 消息（payload downcast &str/String，否则哨兵文案）。
    Panicked(String),
    /// join 等待超时：截止已过且**已请求 abort**——**未**确认任务体终止。
    ///
    /// tokio 的 abort 是协作式取消请求：任务体在下一个让渡点才被销毁 ⇒ 收到本变体
    /// 时任务体**可能仍在执行**。终止确认走 scope 级观察面（[`FxScope::in_flight`] /
    /// [`FxScope::expect_body_idle`]），口径同 [`Spawned::join_timeout`]。
    TimedOut,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("task cancelled before completion"),
            Self::Panicked(msg) => write!(f, "task panicked: {msg}"),
            Self::TimedOut => {
                f.write_str("join timed out; abort requested (task termination not confirmed)")
            }
        }
    }
}

impl std::error::Error for TaskError {}

/// accepting 闸已关：spawn 拒新（fail-closed 显式，禁静默丢弃 future）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScopeClosed;

impl fmt::Display for ScopeClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fx scope closed for new spawns (accepting gate)")
    }
}

impl std::error::Error for ScopeClosed {}

/// 有界准入的 typed 拒绝原因（issue#39）：过载**不** panic、**不**阻塞调用方。
///
/// 非 `#[non_exhaustive]`（0.1.0 未发布，按 ADR 0005 先例直迁）：新增变体对跨 crate
/// 穷尽 `match` 属破坏性变更——[`Self::NoRuntime`]（issue#92）即此形态，采用方补一条臂。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// accepting 闸已关（scope 关闭中）——与 [`ScopeClosed`] 同义。
    Closed,
    /// `max_pending` 触顶：已准入但等待并发许可的任务数达上界。
    PendingFull,
    /// `max_tracked` 触顶：生命周期集合保留的条目数（含已完成未收割）达上界。
    TrackedFull,
    /// 调用线程不在 Tokio runtime 上下文（`Handle::try_current` 失败）：spawn 的
    /// 环境前置条件不成立（issue#92）。**不入容量账**——不是过载也不是闸关：
    /// `admitted` / `tracked` / `active` / `waiting` / `rejected` 全不变，宿主
    /// Future 不入集。
    NoRuntime,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("fx scope closed for new spawns (accepting gate)"),
            Self::PendingFull => {
                f.write_str("fx scope pending capacity exhausted (max_pending reached)")
            }
            Self::TrackedFull => {
                f.write_str("fx scope tracked capacity exhausted (max_tracked reached)")
            }
            Self::NoRuntime => f.write_str(
                "no Tokio runtime context: spawn must be called from within a Tokio runtime",
            ),
        }
    }
}

impl std::error::Error for AdmissionError {}

/// fx 域三资源轴的占用快照（issue#39 可观测面）。
///
/// 三轴互相独立：`active` 对应 `max_concurrency`（并发许可）、`waiting` 对应
/// `max_pending`（已准入但等待许可）；`completed_unreaped` 对应 `max_tracked`
/// （集合保留但任务体已终局的条目数——**含**已完成未收割者）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FxCounts {
    /// 持有并发许可的任务数（准入即取许可 ⇒ 含已准入未开跑者）。
    pub active: usize,
    /// 已准入但等待并发许可的任务数。
    ///
    /// **不保证按准入先后获得许可**——见 [`FxScope::with_max_pending`] 的公平性
    /// 说明：新准入者的 `try_acquire` 不看等待队列，可越过既有等待者。
    pub waiting: usize,
    /// 已完成但尚未从生命周期集合收割的条目数（含 panic / abort 终局条目）。
    pub completed_unreaped: usize,
    /// 因容量上界被拒的准入次数（累计；闸关拒发不计入）。
    pub rejected: usize,
}

/// 取消源（fx 域停机三段的② abort 协作取消通知面，三段定义见
/// [`FxScope::shutdown`] 文档）；幂等 cancel。
///
/// **取消是粘性的**：状态存进 watch 值本身，不依赖此刻是否有接收者——取消之后
/// 才订阅的观察者（如 [`FxScope::with_source`] 新建的 scope token）同样看到
/// `true`。无接收者期间取消不得静默丢失（issue#68）。
///
/// **Clone = 同一取消态的第二句柄**（不是独立源）：所有句柄共享同一个 watch
/// 值，任一句柄 `cancel()` 全体可见——宿主把源按值交给
/// [`FxScope::with_source`] 后，用预先 clone 的句柄照样能触发取消。取消幂等、
/// 单向（`false → true`，不回落）。
#[derive(Clone)]
pub struct CancelSource {
    tx: watch::Sender<bool>,
}

impl CancelSource {
    /// 触发取消（幂等；单向 `false → true`）。
    ///
    /// 用 `send_if_modified` 而非 `send`：`send` 在**没有接收者**时不写入值
    /// （tokio watch 语义）⇒ 取消会静默丢失，之后 `with_source` 的订阅者观察到
    /// `false`（issue#68）。`send_if_modified` 无接收者也把值存下，且只在真实
    /// 转换时唤醒当前接收者（重复 cancel 不产生冗余版本通知）；watch 值是唯一
    /// 真源，不另设原子标志。
    pub fn cancel(&self) {
        let _ = self.tx.send_if_modified(|cancelled| {
            if *cancelled {
                false
            } else {
                *cancelled = true;
                true
            }
        });
    }

    /// 取消态读侧（非阻塞快照；读的是存下的 watch 值，与接收者是否在场无关）。
    pub fn is_cancelled(&self) -> bool {
        *self.tx.borrow()
    }
}

/// 取消令牌（观察面，Clone=同一取消态）。
#[derive(Clone)]
pub struct CancelToken {
    rx: watch::Receiver<bool>,
}

impl CancelToken {
    /// 取消态读侧（非阻塞快照）。
    pub fn is_cancelled(&self) -> bool {
        *self.rx.borrow()
    }

    /// cancel-safe（`false→true` 才返回）。
    ///
    /// 取消态是粘性的：**取消发生在订阅之前**（含无接收者窗口里的取消）⇒ 立即
    /// 返回；源已丢且从未取消 ⇒ 永不到来。
    pub async fn cancelled(&self) {
        let mut rx = self.rx.clone();
        if rx.wait_for(|v| *v).await.is_err() {
            // 源已丢：取消永不到来（false→true 才返回）——park 而非忙等或伪装已取消。
            std::future::pending::<()>().await;
        }
    }

    /// 取消=None 形状。
    pub async fn run_until_cancelled<T>(&self, fut: impl Future<Output = T>) -> Option<T>
    where
        T: Send,
    {
        tokio::pin!(fut);
        // biased=取消臂优先裁决（取消就绪即 None，「取消=None」形状；否则双臂同就绪
        // 时随机裁决会让已完成的 fut 抢跑）。
        tokio::select! {
            biased;
            _ = self.cancelled() => None,
            out = &mut fut => Some(out),
        }
    }
}

/// 构造点唯一：watch::channel(false) 一处成对产出。
pub fn cancel_pair() -> (CancelSource, CancelToken) {
    let (tx, rx) = watch::channel(false);
    (CancelSource { tx }, CancelToken { rx })
}

/// 在途计数守卫：abort=future 被 drop，守卫随 drop 递减——「abort 生效即归零」
/// 要求递减发生在取消瞬间，而 JoinSet 条目要等 join_next 收割才从集内移除，故 in_flight
/// 用独立计数而非 `JoinSet::len`。
struct FlightGuard(Arc<AtomicUsize>);

impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// scope 身份发号器（issue#133）：只用于「本 scope 的托管任务 vs 外部调用者」判定，
/// 进程内唯一即可（不跨进程比较、不进任何对外契约）。
static NEXT_SCOPE_ID: AtomicUsize = AtomicUsize::new(1);

thread_local! {
    /// 当前 poll 段里正在跑的托管任务所属 scope（issue#133 自证面）。
    ///
    /// **只在 poll 段内可见**：await 让出即恢复前值 ⇒ 同线程随后轮转 poll 的其他
    /// 任务不会被误标。存前值（RAII 还原）而非布尔 ⇒ 嵌套 poll（任务里 `block_on`）
    /// 也按 LIFO 正确还原。
    static ACTIVE_SCOPE: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// 标记守卫：装/还原 [`ACTIVE_SCOPE`]（panic 展开路径同样还原）。
struct ScopeMark(Option<usize>);

impl ScopeMark {
    fn install(scope: usize) -> Self {
        Self(ACTIVE_SCOPE.with(|cell| cell.replace(Some(scope))))
    }
}

impl Drop for ScopeMark {
    fn drop(&mut self) {
        ACTIVE_SCOPE.with(|cell| cell.set(self.0));
    }
}

/// 当前 poll 段是否属于 `scope` 的托管任务（[`FxScope::shutdown`] 的自停机判定）。
fn polling_in_scope(scope: usize) -> bool {
    ACTIVE_SCOPE.with(|cell| cell.get() == Some(scope))
}

/// 任务体 poll 面标记（issue#133）：把「此刻在跑哪个 scope 的托管任务」挂在线程
/// 局部，供停机口在赢 `Open → Draining` **之前**拒绝自停机。零分配，每 poll 一次
/// TLS 读写；标记在 [`CatchUnwind`] 之内 ⇒ 用户 panic 不影响还原。
struct ScopeScoped<'a, F> {
    fut: Pin<&'a mut F>,
    scope: usize,
}

impl<F: Future> Future for ScopeScoped<'_, F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut(); // Self: Unpin（字段是 Pin<&mut F>）
        let _mark = ScopeMark::install(this.scope);
        this.fut.as_mut().poll(cx)
    }
}

/// future 轮询面 catch_unwind（std 版只包同步段）：panic payload 旁路给 task_body
/// 归一成 TaskError::Panicked。包 `Pin<&mut F>`（Unpin）——投影全走
/// 安全 API，零 unsafe。
struct CatchUnwind<'a, F>(Pin<&'a mut F>);

impl<F: Future> Future for CatchUnwind<'_, F> {
    type Output = Result<F::Output, Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut(); // Self: Unpin（字段是 Pin<&mut F>）
        match catch_unwind(AssertUnwindSafe(|| this.0.as_mut().poll(cx))) {
            Ok(poll) => poll.map(Ok),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

/// panic payload → 消息：downcast &str/String，否则哨兵文案。
fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_owned()
    }
}

/// 任务包装体：permit-in-future（abort 连带回收 permit）→ 本 scope 标记 →
/// catch_unwind 归一 → oneshot 发送。接收端已亡（Spawned drop/abort）= 结果无处
/// 投递即弃（panic 结果留一行 warn 痕防静默丢失，见模块头注），任务本体仍归生命
/// 周期集托管。
///
/// `scope` = 本 scope 身份（issue#133）：每次 poll 用户 future 期间挂进线程局部
/// 标记，[`FxScope::shutdown`] / [`FxScope::shutdown_timeout`] 据此拒绝自停机。
///
/// 返回值 = 生命周期集条目载荷（issue#13）：panic 归一为 `Err(Panicked)` 进集合
/// ——句柄被丢也不丢 panic；[`Spawned::join`] 与 [`FxScope::try_reap`] 双重可见
/// （同一 panic 两条路都可能报出，诊断面语义非 exactly-once）。其余结局集合面
/// 统一 `Ok(())`——真实值只走 oneshot 旁路，集合条目仅承载生命周期与 panic 面。
async fn task_body<T, F>(
    scope: usize,
    sem: Option<Arc<Semaphore>>,
    permit: Option<OwnedSemaphorePermit>,
    fut: F,
    tx: oneshot::Sender<Result<T, TaskError>>,
    mut slot: SlotGuard,
) -> Result<(), TaskError>
where
    F: Future<Output = T>,
{
    // 参数声明序 = 析构序（逆序）：`slot`（终局记账守卫）先于 `tx` 析构 ⇒
    // join 侧被唤醒时终局记账已落（无观测竞窗）。
    let _permit = match permit {
        // 准入时已取到许可（持有档）：许可随 future 析构回收（abort 连带回收）。
        Some(permit) => Some(permit),
        // 等待档：许可在任务体内部取得（此时才迁移 waiting → active）。
        None => match sem {
            None => None,
            Some(sem) => match sem.acquire_owned().await {
                Ok(permit) => {
                    slot.promote();
                    Some(permit)
                }
                // 信号量关闭在本 API 面不存在（从不 close）；显式归 Cancelled 禁吞。
                Err(_) => {
                    let _ = tx.send(Err(TaskError::Cancelled));
                    return Ok(());
                }
            },
        },
    };
    let mut fut = std::pin::pin!(fut);
    let mut marked = ScopeScoped {
        fut: fut.as_mut(),
        scope,
    };
    let result = match CatchUnwind(Pin::new(&mut marked)).await {
        Ok(value) => Ok(value),
        Err(payload) => Err(TaskError::Panicked(panic_message(payload))),
    };
    // 终局记账先于投递：join 侧返回时「已完成未收割」计数已可见。
    slot.report_terminal();
    // panic 载荷双投（issue#13）：oneshot 先行（句柄在 → join 收原值），集合收
    // clone——句柄被丢也不丢。句柄已亡（send 失败）留一行 warn 痕（tracing 无
    // 订阅者 no-op；宏关=默认，零增量）。clone 只在 panic 异常路径付一次。
    match result {
        Err(te @ TaskError::Panicked(_)) => {
            let send_failed = tx.send(Err(te.clone())).is_err();
            // 结果已成（`te` 在栈上、投递已定），诊断才发：订阅者 panic 只被宏的
            // catch_unwind 边界吞掉并计数，顶不掉下面返回给生命周期集的原 panic
            // 成因（issue#201）。
            #[cfg(feature = "log-embed")]
            if send_failed {
                if let TaskError::Panicked(msg) = &te {
                    embed_event!(warn, target: "skeleton::fx", msg = msg.as_str(),
                        "managed task panicked; handle dropped before join");
                }
            }
            #[cfg(not(feature = "log-embed"))]
            let _ = send_failed;
            Err(te)
        }
        result => {
            let _ = tx.send(result);
            Ok(())
        }
    }
}

/// 停机排空报告（issue#13 缺口二 / 蓝图 §4）：停机排空不再静默丢结果——终局按桶
/// 归集，超时只表达「调用方停止等待」，仍在运行的任务显式申报。
///
/// 五桶（`completed` / `cancelled` / `panicked` / `failed` / `still_running`）互斥
/// 且穷尽：本次排空观测到的每条任务终局恰落一桶；`timed_out` 是截止事件的计数，
/// 与终局桶正交（可覆盖截止瞬间已终局但尚未收割的条目）。失败桶另带 `errors`
/// 逐条明细（计数与文案同一处写入）。
///
/// `drain_interrupted` 不是桶而是**报告来源**标记：排空 owner 的 future 被 drop
/// 或 panic 展开时置 true——本报告是中断结果，不是整 scope 的 canonical 终局
/// （issue#70）。
///
/// `#[non_exhaustive]`：字段按真实采用面渐进扩，加字段属非破坏演进——**跨 crate
/// 禁用 struct literal**（含 `..Default::default()` 形态，rustc 一律 E0639），采用
/// 方走 `ShutdownReport::default()` 起手再按需赋值/读侧访问（同 crate 豁免，纪律
/// 同 [`skeleton::Effect`] 词表的加变体面）。
#[must_use]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ShutdownReport {
    /// 正常终局（任务体返回 `Ok(())`）的条数——含此前已终局、滞留生命周期集至
    /// 本次排空才收割的条目。
    pub completed: usize,
    /// 已确认被中止的条数（abort 在任务下一 await 点生效后，生命周期集报
    /// cancelled）。**只数已观测到的终止**：abort 已发而终止未观测者进
    /// [`Self::still_running`]。
    pub cancelled: usize,
    /// 排空中收割到的任务 panic 消息（含句柄已被丢的任务——载荷入集合不丢，
    /// 见 [`FxScope::spawn_tracked`] 的双重可见语义）。
    pub panicked: Vec<String>,
    /// 非 panic 的失败终局条数（任务体错误 `TaskError::TimedOut` 等，以及未知
    /// join 变体的兜底落账）；逐条文案与 [`Self::errors`] 对齐。任务包装体当前只
    /// 把 panic 归一成终局错误，故常态 0。
    ///
    /// [`TaskError::TimedOut`] 的语义是「截止已过 + 已请求 abort」，**不**表示
    /// 任务体已终止——与 [`Self::timed_out`] / [`Self::still_running`] 的区分同口径。
    pub failed: usize,
    /// [`Self::failed`] 的逐条诊断文案（与计数同点写入，`errors.len() == failed`）；
    /// 未知 join 变体在此留 `Display` 原文，禁静默吞。
    pub errors: Vec<String>,
    /// 截止时未完成、调用方停止等待的条数（仅 [`FxScope::shutdown_timeout`] 超时
    /// 路径 >0；[`FxScope::shutdown`] 恒 0）。**语义边界**：超时只表示调用方不再
    /// 等待，不表示工作已终止——此数含截止瞬间已终局但尚未收割的条目。
    pub timed_out: usize,
    /// 报告返回时终止**未被观测到**的条数（仍在生命周期集内，剩余集 Drop=abort
    /// 已排队但尚未生效）。**语义边界**：不宣称这些任务仍在执行，只申报「终止未
    /// 确认」；超时路径上 ≤ [`Self::timed_out`]（截止后的首轮收割只会减少）。
    pub still_running: usize,
    /// 本报告来自**被中断的排空**（排空 owner 的 future 被 drop 或 panic 展开，
    /// issue#70）：置 true ⇒ 这是中断结果，不是整 scope 的 canonical 终局。
    ///
    /// 此时各终局桶只统计中断前已收割的条目，[`Self::still_running`] 为中断时集内
    /// 条数。中断只**请求** abort（JoinSet drop 语义），**不**确认任务终止——任务
    /// 守卫与 [`FxScope::in_flight`] 在任务真正退出前具权威。中断后排空已结算：
    /// 后续 [`FxScope::shutdown`] 调用返回**该次报告的副本**（canonical 可见面，
    /// 不降级为空报告——审计 F3）。
    pub drain_interrupted: bool,
}

impl ShutdownReport {
    /// 单条终局归类（`join_next` / `try_join_next` 同形，排空两面共用一张表——
    /// 桶互斥且穷尽）。
    fn record(&mut self, outcome: Result<Result<(), TaskError>, JoinError>) {
        match outcome {
            Ok(Ok(())) => self.completed += 1,
            Ok(Err(TaskError::Panicked(msg))) => self.panicked.push(msg),
            Ok(Err(TaskError::Cancelled)) => self.cancelled += 1,
            Ok(Err(err)) => {
                self.failed += 1;
                self.errors.push(err.to_string());
            }
            Err(err) if err.is_cancelled() => self.cancelled += 1,
            // `is_panic` 显式守卫：`into_panic` 在非 panic 时自身 panic，而本路径
            // 是停机收尾——报告代码不得因 JoinError 将来新增变体而炸。
            Err(err) if err.is_panic() => self.panicked.push(panic_message(err.into_panic())),
            // 兜底：未知 join 变体按失败落账并留 `Display` 原文（禁 `_ => {}` 静默吞）。
            Err(err) => {
                self.failed += 1;
                self.errors.push(format!("unknown join failure: {err}"));
            }
        }
    }
}

/// 准入计数（issue#39 三轴内部账）：并发许可占用 + 集合保留条目 + 终局记账。
///
/// 锁序铁律：`set` 锁 → `caps` 锁（准入/收割/排空三处同序，禁反序取锁）。
#[derive(Debug)]
struct Capacity {
    /// 持有并发许可的任务数。
    active: usize,
    /// 已准入但等待并发许可的任务数。
    waiting: usize,
    /// 生命周期集合保留的条目数（含已完成未收割）。
    tracked: usize,
    /// 已完成但尚未收割的条目数（终局记账 +1，收割 -1）。
    completed_unreaped: usize,
    /// 因容量上界被拒的准入次数（累计）。
    rejected: usize,
    /// 累计准入任务数（终局记账的对照面）。
    admitted: usize,
    /// 累计终局任务数（每个任务恰 +1）。
    terminal: usize,
    /// 跟踪记账是否开启：停机排空取走集合后置 false（scope 不再保留任何条目）。
    tracking: bool,
}

impl Default for Capacity {
    fn default() -> Self {
        Self {
            active: 0,
            waiting: 0,
            tracked: 0,
            completed_unreaped: 0,
            rejected: 0,
            admitted: 0,
            terminal: 0,
            tracking: true,
        }
    }
}

/// 并发槽守卫：任务终局（正常完成 / panic / 取消 / abort）**恰好**释放一次许可
/// ——RAII 单一 Drop 落点，不多（多放会让并发度越界）不少（少放＝泄漏）。
///
/// `holds_slot` = 持有许可档（计入 `active`）/ 等待档（计入 `waiting`）；
/// `terminal_reported` 防任务体与 Drop 双记终局。
struct SlotGuard {
    caps: Arc<Mutex<Capacity>>,
    /// true = 计入 `active`（持有许可）；false = 计入 `waiting`（等待许可）。
    holds_slot: bool,
    /// 终局是否已记账。
    terminal_reported: bool,
}

impl SlotGuard {
    /// 取得并发许可时的迁移：等待档 → 持有档（`waiting` 减一、`active` 加一）。
    fn promote(&mut self) {
        let mut caps = lock(&self.caps);
        caps.waiting -= 1;
        caps.active += 1;
        self.holds_slot = true;
    }

    /// 终局记账：任务体已终局 ⇒ 条目仍在集合内 ⇒ 未收割计数加一（收割时再减一）。
    ///
    /// 在**投递结果之前**调用：join 侧被唤醒时计数已落（无观测竞窗）。
    fn report_terminal(&mut self) {
        if self.terminal_reported {
            return;
        }
        self.terminal_reported = true;
        let mut caps = lock(&self.caps);
        caps.terminal += 1;
        if caps.tracking {
            caps.completed_unreaped += 1;
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        // 终局记账兜底：准入后未跑到收尾（被 abort）时由 Drop 代记——恰好一次。
        self.report_terminal();
        let mut caps = lock(&self.caps);
        if self.holds_slot {
            caps.active -= 1;
        } else {
            caps.waiting -= 1;
        }
    }
}

/// 排空共享状态（issue#70）：`Open` → `Draining` → `Settled`。
///
/// watch 值是**唯一真源**：`Settled` 的写入本身就携带 canonical 报告，故不存在
/// 「通知了但谓词未落」的窗——等待者被唤醒时谓词必已成立。
#[derive(Clone)]
enum DrainState {
    /// 无人在排空（含从未调用过停机的 scope）。
    Open,
    /// 已有 owner 取得排空权；该发布**先于**取走 JoinSet。
    Draining,
    /// 排空已结算：载荷 = 该次排空的 canonical 报告（正常完成 / 截止期 / 中断）。
    Settled(ShutdownReport),
}

/// [`FxScope::begin_drain`] 的角色判定（一次调用恰得一种）。
enum DrainSlot {
    /// 本次调用赢得排空权（唯一 owner）。
    Owner,
    /// 已有 owner 在排空：等它的结算（共享 canonical 报告）。
    Waiter(watch::Receiver<DrainState>),
    /// 排空早已结算：返回该次排空的 canonical 报告副本（顺序语义；并发晚到者与
    /// owner/等待者所见一致——审计 F3：旧实现的两次读会让结算瞬间到达的调用拿到
    /// 默认报告，panic 载荷就此静默丢失）。
    Settled(ShutdownReport),
}

/// 排空 owner 守卫（issue#70，RAII）：JoinSet 留在**唯一** owner 手里
/// （take-ownership 保持 [`FxScope::try_reap`] 同步、临界区不跨 await），共享状态
/// 的结算由本守卫的每条退出路径负责。
///
/// 结算顺序铁律：先发布共享谓词（`DrainState::Settled`），再让 `set` 字段 drop
/// ——JoinSet drop 会**请求** abort，其任务 future 的析构（用户代码）因此发生在
/// 结算之后，且全程不持有本 scope 的任何锁。取消 / panic 路径由 [`Drop`] 兜底，
/// 永不把 `Draining` 留在原地。
struct DrainOwner<'a> {
    scope: &'a FxScope,
    report: ShutdownReport,
    set: JoinSet<Result<(), TaskError>>,
    settled: bool,
}

impl<'a> DrainOwner<'a> {
    /// 建守卫（调用前 `begin_drain` 已发布 `Draining`）：`set` 先为占位空集，
    /// 真实集合由 [`Self::attach`] 接管——接管阶段即使 panic（毒锁等）也已有守卫
    /// 在栈上，[`Drop`] 会发布中断结果，绝不把 `Draining` 留在原地。
    fn new(scope: &'a FxScope) -> Self {
        Self {
            scope,
            report: ShutdownReport::default(),
            set: JoinSet::new(),
            settled: false,
        }
    }

    /// 接管真实生命周期集（close + abort_all + take；与 `new` 之间无 await）。
    fn attach(&mut self) {
        self.set = self.scope.close_and_take();
    }

    /// 排空至空集（正常完成路径的 await 段）。
    async fn drain_all(&mut self) {
        while let Some(outcome) = self.set.join_next().await {
            self.report.record(outcome);
        }
    }

    /// 截止结算（owner 的排空策略）：先记截止时未完成数，再收割此刻已终局者
    /// （panic 载荷不丢），剩余 = 终止未观测数。
    fn settle_deadline(&mut self) -> ShutdownReport {
        self.report.timed_out = self.set.len();
        while let Some(outcome) = self.set.try_join_next() {
            self.report.record(outcome);
        }
        self.report.still_running = self.set.len();
        self.publish()
    }

    /// 结算并发布本次排空结果（正常完成 / 截止期 / 中断共用同一发布口）。
    ///
    /// watch 写入即通知 ⇒ **谓词先落、等待者后醒**；锁内只有 [`ShutdownReport`]
    /// （自有 `Vec<String>`，无用户载荷）——用户代码与用户析构一律不在锁内运行。
    fn publish(&mut self) -> ShutdownReport {
        self.settled = true;
        let report = std::mem::take(&mut self.report);
        self.scope
            .drain
            .send_replace(DrainState::Settled(report.clone()));
        report
    }
}

impl Drop for DrainOwner<'_> {
    /// owner future 被 drop（select! 竞败 / 超时分支弃置）或 panic 展开 ⇒ 发布
    /// **中断结果**（含中断时已知计数），再让 JoinSet drop 请求 abort。绝不留在
    /// `Draining`，也绝不让等待者等一个永远不会产生的报告。
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        self.report.drain_interrupted = true;
        // 中断前已终局的条目同步收割（不 await）：panic 载荷不静默丢。
        while let Some(outcome) = self.set.try_join_next() {
            self.report.record(outcome);
        }
        self.report.still_running = self.set.len();
        let _ = self.publish(); // 报告只投给等待者（owner 已被弃置，无返回方）
    }
}

/// 等排空 owner 结算（issue#70）：订阅者以共享 canonical 报告醒来。
///
/// `wait_for` 先查当前值 ⇒ owner 已结算时立即返回（无丢失唤醒）。发送端消亡
/// （scope 先于等待者消失）按中断结果申报，禁挂死。
async fn await_settled(mut rx: watch::Receiver<DrainState>) -> ShutdownReport {
    match rx
        .wait_for(|state| matches!(state, DrainState::Settled(_)))
        .await
    {
        Ok(state) => match &*state {
            DrainState::Settled(report) => report.clone(),
            // `wait_for` 的谓词只放行 `Settled`，其余臂不可达。
            _ => unreachable!("wait_for predicate only admits DrainState::Settled"),
        },
        Err(_closed) => ShutdownReport {
            drain_interrupted: true,
            ..ShutdownReport::default()
        },
    }
}

/// 托管 scope：内部 = `Mutex<JoinSet<Result<(), TaskError>>>`（生命周期集，短临界区
/// 零 await；条目载荷 = panic 归一面——完成条目滞留至 [`Self::try_reap`] 或停机排空
/// 显式收割，见 issue#13）+ AtomicBool accepting 闸 + `Option<Arc<Semaphore>>` 并发闸
/// + 取消源/令牌 + 排空共享状态（issue#70：`Open`/`Draining`/`Settled`）。
///
/// 通过 `Arc<FxScope>` 跨任务共享（spawn/join 均 &self）。
pub struct FxScope {
    set: Mutex<JoinSet<Result<(), TaskError>>>,
    accepting: AtomicBool,
    sem: Option<Arc<Semaphore>>,
    caps: Arc<Mutex<Capacity>>,
    /// `max_pending` 上界（`usize::MAX` = 不限；默认不限 ⇒ 兼容既有消费方）。
    max_pending: usize,
    /// `max_tracked` 上界（`usize::MAX` = 不限；默认不限 ⇒ 兼容既有消费方）。
    max_tracked: usize,
    in_flight: Arc<AtomicUsize>,
    /// 取消源：`new()` 建**内部源**（首次进入停机与 Drop 时由 scope 自己触发
    /// 取消，issue#119），`with_source` 换成**外部源**（取消权归外部 owner）。
    source: CancelSource,
    token: CancelToken,
    /// 内部源所有权（issue#119）：`true` = `new()` 自建（停机首转与 Drop 调
    /// `cancel()` 让克隆出去的 token 观察到取消）；`false` = `with_source` 注入
    /// 的外部源（scope 生命周期**不**改写其取消态）。
    owns_source: bool,
    /// scope 身份（issue#133）：任务体 poll 期把它挂进线程局部标记，供停机口判定
    /// 「调用者是否本 scope 的托管任务」。进程内唯一即可（不跨进程比较）。
    id: usize,
    /// 排空共享状态（issue#70）：`Open`/`Draining`/`Settled`——watch 值为唯一真源，
    /// 并发停机共享同一次排空。
    drain: watch::Sender<DrainState>,
}

impl FxScope {
    /// 新建即 accepting（可 spawn）；并发闸与外接取消源经 builder 链显式加挂。
    ///
    /// 本构造建**内部取消源**（issue#119）：[`Self::token`] 取出的令牌在首次
    /// 进入停机（`shutdown` / `shutdown_timeout`）或 scope `Drop` 时转为已取消。
    /// 要让取消权留在外部，用 [`Self::with_source`] 注入（此后 scope 的停机与
    /// Drop 都不改写该源的取消态）。
    pub fn new() -> Self {
        let (source, token) = cancel_pair();
        // 初始接收者即刻丢弃：`send_replace` / `send_if_modified` 无接收者也写入值
        // （#68 的教训），等待者按需 `subscribe`（watch 的共享状态由发送侧持有）。
        let (drain, _initial_receiver) = watch::channel(DrainState::Open);
        Self {
            set: Mutex::new(JoinSet::new()),
            accepting: AtomicBool::new(true),
            sem: None,
            caps: Arc::new(Mutex::new(Capacity::default())),
            max_pending: usize::MAX,
            max_tracked: usize::MAX,
            in_flight: Arc::new(AtomicUsize::new(0)),
            source,
            token,
            owns_source: true,
            id: NEXT_SCOPE_ID.fetch_add(1, Ordering::Relaxed),
            drain,
        }
    }

    /// 并发闸（`max_concurrency` 轴）：限制**同时持有并发许可的任务体**数量。
    ///
    /// **语义边界——本口不是内存/保留量上界**：本闸只约束「同时执行的任务体数」，
    /// **不**约束生命周期集合保留的条目数 ⇒ 已准入而未收割的条目照样滞留集合。
    /// 保留量上界是 [`Self::with_max_tracked`]。
    ///
    /// 许可在**准入时**取走（`try_acquire`）：取到即计入 `active`，取不到即落等待
    /// 档（计入 `waiting`，任务体内部 `acquire` 排队等待）——许可由准入时的槽位
    /// 守卫持有，abort / 任务体终局时连带回收。
    pub fn with_concurrency(mut self, max: usize) -> Self {
        assert!(max > 0, "fx scope concurrency cap must be > 0");
        self.sem = Some(Arc::new(Semaphore::new(max)));
        self
    }

    /// `max_pending` 轴上界：已准入但**等待并发许可**的任务数上限。
    ///
    /// 只约束有界准入口 [`Self::try_spawn_tracked`]（触顶 ⇒
    /// [`AdmissionError::PendingFull`]，typed 拒绝、不阻塞不 panic）；
    /// [`Self::spawn_tracked`] 是无界旧口，不受此约束。未设
    /// [`Self::with_concurrency`] 时无任务等许可 ⇒ 本轴恒不触发（与并发轴独立）。
    ///
    /// **许可分配不保证 FIFO（依 tokio 1.53.3 源码核实）**：取不到许可的任务体在
    /// 内部 `acquire()` 排队——该路径是 FIFO 公平的；但**新准入者用
    /// `try_acquire` 取许可，它不查看等待队列**，因此可能抢在既有等待者之前拿到
    /// 刚释放的许可。后果：**并发上限仍然严格**（信号量是唯一闸，许可总数受限），
    /// 但 `waiting` 中的任务**不保证按准入先后获得许可**，极端情况下存在饥饿可能。
    /// 需要严格 FIFO 时，不要依赖本轴的排队顺序——改用不设上界的口，让许可由
    /// 任务体自行 `acquire` 获取。
    pub fn with_max_pending(mut self, max: usize) -> Self {
        assert!(max > 0, "fx scope pending cap must be > 0");
        self.max_pending = max;
        self
    }

    /// `max_tracked` 轴上界：生命周期集合保留的**全部**条目数上限——**含**已完成
    /// 未收割者（[`Spawned::join`] 只消费结果、**不**移除集合条目 ⇒ 出集合只能靠
    /// [`Self::try_reap`] 或停机排空）。
    ///
    /// 显式 opt-in：默认不限 ⇒ 兼容既有消费方。本轴只约束有界准入口
    /// [`Self::try_spawn_tracked`]（触顶 ⇒ [`AdmissionError::TrackedFull`]）。
    pub fn with_max_tracked(mut self, max: usize) -> Self {
        assert!(max > 0, "fx scope tracked cap must be > 0");
        self.max_tracked = max;
        self
    }

    /// 外接取消源（scope 内部 token 与外部源同 channel）。
    ///
    /// 外部源已取消（含取消发生时**没有任何接收者**）⇒ 本 scope 的 token 立即
    /// 观察到 `true`（粘性取消，issue#68）。
    pub fn with_source(mut self, src: CancelSource) -> Self {
        self.token = CancelToken {
            rx: src.tx.subscribe(),
        };
        self.source = src;
        // 外部源：取消权归外部 owner，scope 停机/Drop 不改写它（issue#119）。
        self.owns_source = false;
        self
    }

    /// 取观察面令牌（Clone=同一取消态；任务内携带判停）。
    ///
    /// **默认 scope（`new()`）的 token 生命周期（issue#119）**：首次进入停机
    /// （[`Self::shutdown`] / [`Self::shutdown_timeout`] 赢得 `Open → Draining`
    /// 的那一次）与 scope **Drop** 时，scope 触发内部源的粘性取消 ⇒ 此前取出
    /// 的 token 全部观察到 `true`。`close()` **不算**停机（只关接受闸，不取消
    /// ——同 `conformance_lifecycle` 的「close 不是 cancel」契约）。
    ///
    /// **注入外部源（[`Self::with_source`]）时**取消态由外部源决定（含取消先于
    /// 接线，issue#68）；scope 的停机与 Drop 都不改写它。
    pub fn token(&self) -> CancelToken {
        self.token.clone()
    }

    /// accepting 闸：拒新 spawn，放行在途（kameo 语义；fx 域停机三段的①段
    /// ——闸已关、② abort_all 未发的窗，三段定义见 [`Self::shutdown`] 文档）。
    pub fn close(&self) {
        // CAS 失败=已关幂等（close 侧显式）。
        let _ = self
            .accepting
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire);
    }

    /// accepting 闸读侧（true=可 spawn）。
    pub fn is_closed(&self) -> bool {
        !self.accepting.load(Ordering::Acquire)
    }

    /// 生命周期集未净数（LeakGuard 断言锚；泄漏判据≠0）。
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// 托管 spawn（tracked）：任务进 JoinSet 生命周期集——过闸任务必被停机三段
    /// 覆盖，scope drop ⇒ 全组 abort。结果走 oneshot 旁路（生命周期集条目载 panic 归一面，
    /// 规避 JoinSet 单类型限制）；[`Spawned`] Drop 不 abort（忘 join 也不泄漏，
    /// JoinSet Drop 兜底）。闸关=Err(ScopeClosed)。
    ///
    /// **无界旧口**（issue#39）：本口不施加 `max_pending` / `max_tracked` 上界
    /// （并发闸仍生效：许可取不到时任务在内部排队，不阻塞调用方）。需要保留量 /
    /// 准入上界保证的调用方必须走 [`Self::try_spawn_tracked`]。
    ///
    /// # 阻塞边界（issue#94）
    ///
    /// 本口只承载**非阻塞 async orchestration**：被托管 future 必须在有限时间内返回
    /// poll、长工作段定期让渡。CPU 密集 / 阻塞 IO / 阻塞系统调用**不得**放在 poll 内
    /// ——同步段占住 executor 线程期间，timer、取消观察与停机等待全部停摆
    /// （current-thread runtime 尤甚）。这类工作交给 `tokio::task::spawn_blocking` 或
    /// 独立 `ThreadPool`，本任务只 await / 收集结果。三类阻塞的划分、`d` 的保证边界
    /// 与 runtime profile 见模块文档「执行模型与阻塞边界」。
    ///
    /// # 两套 drop 语义对照（选口先选语义）
    ///
    /// tokio 两套原生语义相反且类型上无区分：`JoinHandle` Drop = **detach**
    /// （任务继续跑，"its return value is lost"）；`JoinSet` Drop = **立即 abort
    /// 全部**。本 API 以命名分开，禁混用：
    ///
    /// | 口 | 任务归宿 | scope `shutdown()` / drop | 返回句柄 Drop |
    /// |---|---|---|---|
    /// | [`Self::spawn_tracked`] | JoinSet 生命周期集（计 in_flight） | 等净 / abort 全组 | 无影响（任务归集托管） |
    /// | [`Self::spawn_detached`] | 完全脱离 scope（不计 in_flight） | 不等待、不中止 | detach，任务自生自灭 |
    ///
    /// # `'static + Send` 标准写法
    ///
    /// `'static` = future 不得借用任务外的拥有数据（不是「活 forever」）；`Send` =
    /// 多线程 runtime 工作窃取所需。**持外部句柄须先 `Arc` 化 move 进 future**
    /// （裸引用/`Rc` 编译错是 armor；`Arc` 的分配+原子计数是必要代价。可变共享面
    /// 锁选型：短临界区无 await 持锁用 `std::sync::Mutex`，持锁跨 await 用
    /// `tokio::sync::Mutex`）：
    ///
    /// ```ignore
    /// let ledger: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::new()));
    /// let task_ledger = Arc::clone(&ledger);
    /// // &ledger 裸借用进 'static future 编译不过——Arc clone 后 move：
    /// scope.spawn_tracked(async move {
    ///     task_ledger.lock().expect("poisoned").insert(1, 41);
    /// }).unwrap();
    /// ```
    ///
    /// # cancel-safe 组合清单
    ///
    /// scope 暴露的取消面全部 cancel-safe：[`CancelToken::cancelled`]（watch
    /// false→true 恰一次）、[`CancelToken::run_until_cancelled`]（biased select，
    /// 取消臂优先）。任务体内自写 `select!` 时非 cancel-safe 分支（`write_all`、
    /// `recv_many` 等）被轮转 drop 会丢半截操作——分支选择对照 tokio `select!`
    /// 官方 cancel-safety 表，不安全组合属文档红线（本 API 不代管）。
    ///
    /// # LocalSet 边界
    ///
    /// 本口只收 `Send` future；`!Send` future 走 `tokio::task::spawn_local`
    /// （须 LocalSet 同线程驱动），其句柄禁跨 runtime/LocalSet await（契约外行为）。
    ///
    /// # Panics
    ///
    /// **调用方必须处于 Tokio runtime 上下文**（issue#92）。本口是 fail-visible 旧签名
    /// ——错误类型仍是 [`ScopeClosed`]，加不了 typed 变体 ⇒ 环境错误以带因 panic 表达：
    /// 没有当前 runtime 时在**取任何内部锁之前** panic（消息含 runtime 前置条件与指向
    /// [`Self::try_spawn_tracked`] 的指引），宿主 Future 随之在锁外析构一次。需要
    /// [`AdmissionError::NoRuntime`] typed 结果的调用方走 [`Self::try_spawn_tracked`]。
    ///
    /// 这只是「完全没有 runtime」一条。另一条残差（Handle 所属 runtime 恰在停机）**不**
    /// panic：实测 tokio 1.53 把该任务直接丢弃、句柄落 [`TaskError::Cancelled`]，记账
    /// 仍自洽（`in_flight` 由 FlightGuard 归零，集合条目由收割/停机排空闲清）。
    pub fn spawn_tracked<T>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<Spawned<T>, ScopeClosed>
    where
        T: Send + 'static,
    {
        // runtime 预检在全函数第一句：环境错误不得在 `set` / `caps` 锁内炸开（issue#92）。
        let rt = match Handle::try_current() {
            Ok(rt) => rt,
            Err(err) => panic!(
                "FxScope::spawn_tracked requires a Tokio runtime context: {err}; \
                 use FxScope::try_spawn_tracked for a typed AdmissionError::NoRuntime"
            ),
        };
        let mut set = self.admit_gate()?;
        // 无界档准入（legacy 口）：不施加 `max_pending` / `max_tracked` 上界 ⇒
        // 预留恒成功（触顶分支不可达）。
        let (permit, slot) = self
            .reserve_slot(None)
            .expect("unbounded admission must not hit a limit (no limits configured)");
        Ok(self.spawn_reserved(&mut set, fut, permit, slot, &rt))
    }

    /// 有界准入口（issue#39）：typed 准入结果——成功 / 闸关 / 过载。
    ///
    /// 与 [`Self::spawn_tracked`] 的差别：本口施加两轴上界——`max_pending`
    /// （已准入但等待许可的任务数）与 `max_tracked`（集合保留的条目数，含已完成
    /// 未收割者），触顶即 **typed 拒绝**（[`AdmissionError::PendingFull`] /
    /// [`AdmissionError::TrackedFull`]）——过载用返回值表达，既不阻塞调用方也不
    /// panic。
    ///
    /// 其余语义与 [`Self::spawn_tracked`] 一致（生命周期集托管、oneshot 旁路、
    /// `Spawned::join()` 只消费结果、**不**移除集合条目）。阻塞边界同
    /// [`Self::spawn_tracked`]：有界准入不改变「只承载非阻塞 async」的前提。
    ///
    /// **runtime 上下文前置条件**（issue#92）：没有当前 Tokio runtime 时返回
    /// [`AdmissionError::NoRuntime`]——该判定发生在**取 `set` / `caps` 锁之前**，
    /// 不动 accepting 闸、不动容量账（`admitted` / `tracked` / `active` / `waiting`
    /// / `rejected` 全不变），宿主 Future 不入集（随调用返回在锁外析构一次）。
    pub fn try_spawn_tracked<T>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<Spawned<T>, AdmissionError>
    where
        T: Send + 'static,
    {
        // runtime 预检先于一切内部锁（issue#92）：环境错误 ≠ 过载 ≠ 闸关。
        let rt = runtime_handle()?;
        // 闸关与 tracked 侧同义（`ScopeClosed` 无载荷 ⇒ 1:1 映射，无信息丢失）。
        let mut set = self.admit_gate().map_err(|_| AdmissionError::Closed)?;
        let (permit, slot) = self.reserve_slot(Some((self.max_pending, self.max_tracked)))?;
        Ok(self.spawn_reserved(&mut set, fut, permit, slot, &rt))
    }

    /// 取集合锁并核验 accepting 闸（闸核验 = 原子 RMW 而非 plain load：close
    /// 并发下准入的线性化点）。**锁内核验** ⇒ 与 shutdown 的 `abort_all` 串行
    /// ——过闸任务必被 shutdown 覆及（不存在「过闸却逃过停机」的窗）。
    fn admit_gate(&self) -> Result<MutexGuard<'_, JoinSet<Result<(), TaskError>>>, ScopeClosed> {
        let set = lock(&self.set);
        if self
            .accepting
            .compare_exchange(true, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ScopeClosed);
        }
        Ok(set)
    }

    /// 槽位预留（闸核验通过、`set` 锁已持时调用）。
    ///
    /// `limits` = `Some((max_pending, max_tracked))` 时施加两轴上界，触顶返回
    /// typed 拒绝（不阻塞、不 panic）；`None` = 无界档（恒成功）。
    ///
    /// 分类口径：准入时能立刻取到并发许可 ⇒ 持有档（计入 `active`）；取不到 ⇒
    /// 等待档（计入 `waiting`，许可在任务体内部取得时才迁移到持有档）。
    fn reserve_slot(
        &self,
        limits: Option<(usize, usize)>,
    ) -> Result<(Option<OwnedSemaphorePermit>, SlotGuard), AdmissionError> {
        let (max_pending, max_tracked) = limits.unwrap_or((usize::MAX, usize::MAX));
        let mut caps = lock(&self.caps);
        // tracked 轴：集合保留条目数（含已完成未收割）达上界 ⇒ 拒发。
        if caps.tracked >= max_tracked {
            caps.rejected += 1;
            return Err(AdmissionError::TrackedFull);
        }
        let (permit, holds_slot) = match &self.sem {
            Some(sem) => match sem.clone().try_acquire_owned() {
                Ok(permit) => {
                    caps.active += 1;
                    (Some(permit), true)
                }
                Err(_) => {
                    // pending 轴：等待许可的任务数达上界 ⇒ 拒发。
                    if caps.waiting >= max_pending {
                        caps.rejected += 1;
                        return Err(AdmissionError::PendingFull);
                    }
                    caps.waiting += 1;
                    (None, false)
                }
            },
            None => {
                // 无并发闸：任务体不受许可限制，占用视图仍计入 active。
                caps.active += 1;
                (None, true)
            }
        };
        caps.tracked += 1;
        caps.admitted += 1;
        Ok((
            permit,
            SlotGuard {
                caps: Arc::clone(&self.caps),
                holds_slot,
                terminal_reported: false,
            },
        ))
    }

    /// 入集：把已预留的槽位与任务装进生命周期集（`set` 锁已持；`rt` = 准入前预检
    /// 取得的显式 Handle，不读隐式当前位置）。
    fn spawn_reserved<T>(
        &self,
        set: &mut JoinSet<Result<(), TaskError>>,
        fut: impl Future<Output = T> + Send + 'static,
        permit: Option<OwnedSemaphorePermit>,
        slot: SlotGuard,
        rt: &Handle,
    ) -> Spawned<T>
    where
        T: Send + 'static,
    {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let sem = self.sem.clone();
        // 身份按值取（future 须 'static，不能捕获 &self；issue#133）。
        let scope_id = self.id;
        let flight = FlightGuard(Arc::clone(&self.in_flight));
        // tokio ≥1.53：`JoinSet::spawn_on` 用显式 Handle 入集，同样返回 AbortHandle。
        //
        // 入集本身不失败（预检之后的唯一残差是「Handle 所属 runtime 恰在停机」，实测
        // tokio 1.53 把该任务直接丢弃、句柄落 cancelled 而不 panic）⇒ 不需要 spawn 失败
        // 回滚：记账由 FlightGuard/SlotGuard 与集合收割自然闭合，预留账即为真实入集账。
        let abort = set.spawn_on(
            async move {
                let _flight = flight; // abort/panic/完成三路必经 drop——归零契约的落点
                task_body(scope_id, sem, permit, fut, tx, slot).await
            },
            rt,
        );
        Spawned { handle: abort, rx }
    }

    /// 脱管 spawn（detached）：任务不进 JoinSet 生命周期集——shutdown/close 不等
    /// 它、scope drop 不 abort 它；返回裸 tokio `JoinHandle`，其 Drop=detach（任务
    /// 自生自灭）。不计 in_flight（等净断言/泄漏判据均不含它，`expect_body_idle` 对它
    /// 沉默）；不吃并发闸（permit 语义属托管集）。accepting 闸仍生效——scope 已关
    /// 拒发任何新工作（含脱管的）。
    ///
    /// 用途边界：仅限明确要求活得比 scope 久的背景任务（日志 flush/指标上报形），
    /// 泄漏排查与停机契约都不覆盖它，须自行持 JoinHandle 等待或让任务自终止。
    /// 「只是不想等」不是用它的理由——[`Self::spawn_tracked`] 的 [`Spawned`]
    /// Drop 已不 abort，等净面还覆盖它。
    ///
    /// **runtime 上下文前置条件**（issue#92）：没有当前 Tokio runtime 时返回
    /// [`AdmissionError::NoRuntime`]（与 tracked 侧同口：判定在任何内部锁与
    /// accepting 闸核验之前；宿主 Future 随返回析构一次，不入集）。脱管语义不变。
    pub fn spawn_detached<T>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<tokio::task::JoinHandle<T>, AdmissionError>
    where
        T: Send + 'static,
    {
        // runtime 预检在闸核验之前（issue#92）：环境错误不带崩 accepting 闸语义。
        let rt = runtime_handle()?;
        // 闸核验同 tracked 的原子 RMW 口径（close 并发下 spawn 的线性化点）；
        // 不进锁——detached 不与 abort_all 串行（语义上就不被 shutdown 覆及）。
        if self
            .accepting
            .compare_exchange(true, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(AdmissionError::Closed);
        }
        // 显式 Handle 而非 `tokio::spawn`（issue#92）：隐式当前位置读取在预检之后
        // 已无必要，显式句柄把「用哪个 runtime」摆在调用点。`tokio::spawn` 的
        // clippy.toml 禁令仍拦其它直调点（本函数本就是命名闸内唯一脱管口）。
        Ok(rt.spawn(fut))
    }

    /// 自停机检测（issue#133）：**本 scope 的托管任务不得发起本 scope 的停机**。
    ///
    /// 判定必须早于 [`Self::begin_drain`]——命中即 fail-fast panic（签名/返回面保持
    /// 不变，typed 结果留待 API 演进），**不关 accepting 闸、不改排空状态**。
    /// 状态无关：owner 已存在时同样是 panic，不会退化成「成为等待者再被 owner 的
    /// abort 掉」。
    fn reject_self_shutdown(&self, op: &str) {
        assert!(
            !polling_in_scope(self.id),
            "FxScope::{op} 由本 scope 的托管任务发起（issue#133）：该任务就在待 abort 的 \
             生命周期集里，自任 owner 时会被自己的 abort 取消 ⇒ 排空记为中断、调用方拿不到 \
             报告。停机须由外部 owner/协调者发起；任务内请用 CancelToken 自停，或让外部 \
             调用方 await 它。"
        );
    }

    /// 内部取消源的一次性取消（issue#119）：只在**首次进入停机**（`Open →
    /// Draining` 的唯一赢家）与 Drop 时触发；`with_source` 注入的外部源不动
    /// （取消权归外部 owner）。取消是粘性 + 幂等，重复调用无副作用。
    fn cancel_owned_source(&self) {
        if self.owns_source {
            self.source.cancel();
        }
    }

    /// 尝试成为排空 owner（issue#70）：`Open → Draining` 的原子转换**先于**取走
    /// JoinSet——否则并发调用者会把「集合已被取走」误读成「排空已完成」。
    ///
    /// owner 身份 = 赢得这次转换的调用者（本地知识，无需可观测载荷）；发布
    /// `Draining` 的作用是让并发调用者看到「排空在途」而非「空集 = 完成」。
    fn begin_drain(&self) -> DrainSlot {
        let won = self.drain.send_if_modified(|state| match state {
            DrainState::Open => {
                *state = DrainState::Draining;
                true
            }
            _ => false,
        });
        if won {
            // 停机首转的唯一赢家：一次性取消内部源（issue#119）。协作取消通知
            // 与随后的 abort_all **同批**下发，不给「协作任务先反应」的宽限期。
            self.cancel_owned_source();
            return DrainSlot::Owner;
        }
        // 未赢：**一次** borrow 同时取状态与载荷（线性化点 = 本次调用）。旧实现是
        // 「先判 Draining、再 borrow」两次读 ⇒ 恰在结算瞬间到达的调用落进 Settled
        // 分支却只拿到默认报告（审计 F3：并发停机时 panic 载荷静默丢失）。
        match &*self.drain.borrow() {
            DrainState::Settled(report) => DrainSlot::Settled(report.clone()),
            // `Open` 不可达（`send_if_modified` 未命中即已离开 `Open`）；保守回退为
            // 等待，与 Draining 同形——绝不返回伪造的默认报告。
            DrainState::Open | DrainState::Draining => DrainSlot::Waiter(self.drain.subscribe()),
        }
    }

    /// fx 域停机三段一次收拢：① close（accepting 闸关，拒新 spawn）→
    /// ② 取消 + abort_all（**先**触发内部取消源的粘性取消——[`CancelSource`]
    /// 协作取消通知面，issue#119；**同批**随即 abort_all）→ ③ 等净（join_next
    /// 循环至任务集清空）。锁内取消 + abort_all + mem::take(JoinSet)，take 后的
    /// set 在锁外 await join_next 循环（禁持 std 锁跨 await）。
    ///
    /// **取消只是通知，不是宽限期**（issue#119）：`token` 变 `true` 与 abort 请求
    /// 同批下发——协作任务若想在停机时做收尾，必须**自己**在 `cancelled()` 之后
    /// 立刻完成，不能指望「cancel 之后还有时间等 abort」。
    ///
    /// **取消的一次性**：取消挂在 `Open → Draining` 的唯一赢家上（[`Self::shutdown`]
    /// 与 [`Self::shutdown_timeout`] 共用同一转换），重复/并发停机不再重复触发
    /// （取消本身亦幂等）；`with_source` 注入的外部源不在此列（取消权归外部
    /// owner，scope 停机不改写它）。`close()` 不是停机、不取消。
    ///
    /// 排空即收割（issue#13）：终局按 [`ShutdownReport`] 五桶归集（panic 载荷入
    /// 清单，不静默丢弃）；正常等净至空集 ⇒ `timed_out` / `still_running` 恒 0。
    ///
    /// # 并发调用契约（issue#70）
    ///
    /// 一次 scope 的排空是**共享操作**：owner 身份由 `Open → Draining` 的原子转换
    /// 唯一产生，该发布**先于**取走 JoinSet。到达时的状态决定本次调用的角色：
    ///
    /// | 到达时 | 行为 |
    /// |---|---|
    /// | 无人在排空 | 成为 owner：close + abort_all + take，排空至净 |
    /// | 有 owner 在排空 | **等待共享结果**：返回 owner 的 canonical 报告 |
    /// | 排空已结算 | 顺序语义：返回该次排空的 canonical 报告副本（晚到者亦可见） |
    ///
    /// owner 的 future 被 drop 或 panic 展开 ⇒ 守卫先发布**中断结果**
    /// （[`ShutdownReport::drain_interrupted`]）再让 JoinSet drop：全部等待者随之
    /// 醒来，绝不停在「排空中」。中断只**请求** abort，不确认任务终止 ⇒ 报告如实
    /// 保留 `still_running` 读数。排空完成后再次调用仍走顺序语义，但**返回的是该次
    /// 排空的 canonical 报告副本**（非默认报告）——并发与顺序调用者的可见面一致
    /// （审计 F3 修正；`ShutdownReport: Clone`，代价一次浅拷贝）。
    ///
    /// # 谁可以调用（issue#133）
    ///
    /// **本 scope 的托管任务不得调用本 scope 的 `shutdown` / `shutdown_timeout`**
    /// （自停机）：它本身就在待 abort 的生命周期集里，成为 owner 后会被自己的
    /// `abort_all` 取消 ⇒ 共享排空被记成中断、调用方永远拿不到 canonical 报告。
    /// 任务体 poll 期挂着 scope 标记（RAII），停机口在赢 `Open → Draining` **之前**
    /// 判定，命中即 fail-fast panic（签名与返回面保持不变）——本判定**不关
    /// accepting 闸、不改排空状态**，scope 之后仍可被外部正常停机。该判定与排空
    /// 状态无关：owner 已在时也 panic，不会退化成「成为等待者再被 owner 的 abort
    /// 掉」。
    ///
    /// 外部调用者语义完全不变（owner/等待者/已结算三角色照旧）：协调者任务、本
    /// scope 的 detached 任务、**其他** scope 的托管任务都可以调用。任务内想停机
    /// 请改用 [`CancelToken`] 自停，或让外部调用方 await 它。
    ///
    /// # Panics
    ///
    /// 本 scope 的托管任务内调用（见上「谁可以调用」）。
    #[must_use = "shutdown 报告含终局归集与 panic 载荷清单，丢弃即静默丢失（纯收尾用 `let _ =` 显式让渡）"]
    pub async fn shutdown(&self) -> ShutdownReport {
        self.reject_self_shutdown("shutdown");
        match self.begin_drain() {
            DrainSlot::Owner => {
                let mut owner = DrainOwner::new(self);
                owner.attach();
                owner.drain_all().await;
                owner.publish()
            }
            DrainSlot::Waiter(rx) => await_settled(rx).await,
            DrainSlot::Settled(report) => report,
        }
    }

    /// 同上+超时：先 close+abort_all；超时不再返回 `Err`，改 [`ShutdownReport`]
    /// 的 `timed_out > 0` 表达（单任务面对应 [`TaskError::TimedOut`]——同一口径：
    /// 「截止已过 + 已请求 abort」，**不**表示任务体已终止）。
    /// abortable 任务零残留（剩余集 Drop=abort 兜底）；不可 abort 段依赖自终止
    /// 契约。
    ///
    /// 超时即返前先记截止计数、再增量收割已完结条目——终局不因超时丢失。截止时
    /// 未完成数计 [`ShutdownReport::timed_out`]，报告返回时终止仍未观测到者计
    /// [`ShutdownReport::still_running`]：超时路径**不得**把仍在运行记成已完成或
    /// 已终止。
    ///
    /// # 并发语义与截止仲裁（issue#70）
    ///
    /// 排空权与共享结果同 [`Self::shutdown`]：成为 owner 时本截止决定**owner 的
    /// 排空策略**，其截止期报告即该次排空的 canonical 结果（等待者共享它）。
    ///
    /// **截止仲裁**：以等待者身份到达时，本截止只约束**自己的等待**——到期即按
    /// 自己的读数返回（`timed_out` / `still_running` = 此刻 [`Self::in_flight`]），
    /// **不**取消、**不**改写 owner 的排空操作；owner 之后照样发布它的 canonical
    /// 报告供其余等待者。
    ///
    /// **该报告不是整 scope 的 canonical 结果**（终局桶恒空，只含自己的截止读数）：
    /// 判断「整 scope 已停机完成」请以共享排空已结算（再调用 `shutdown` 返回该次
    /// 排空的 canonical 报告副本）或 [`Self::in_flight`] 归零为准，勿把等待者的到期
    /// 报告当终局账。
    ///
    /// # 截止的保证边界（issue#94）
    ///
    /// `d` 依赖 tokio timer：只在 **executor 可继续调度**时才保证「到期触发并请求
    /// abort」。若有任务卡在不返回 poll 的同步段（忙等 / 同步锁等待 / 同步析构），
    /// 截止即使已过也要等该段返回、executor 重新拿到执行权后才被观察到 ⇒ `d`
    /// **不是**对同步阻塞代码的硬墙钟上限，外层 CI job timeout 也不改变本域语义。
    /// `timed_out` = 截止已过计数，`still_running` = 报告返回时终止仍未观测到——
    /// 两者都**不**表示已终止。含 blocking work 的消费方须选 multi-thread runtime
    /// 并把 blocking 工作移出 executor（见模块文档「执行模型与阻塞边界」）。
    ///
    /// # Panics
    ///
    /// 本 scope 的托管任务内调用（自停机，同 [`Self::shutdown`] 的「谁可以调用」）；
    /// 另需**启用 time driver 的 Tokio runtime 上下文**（issue#92）：`tokio::time::timeout`
    /// 的截止句柄在构造时向当前 runtime 的 time driver 注册。用没开 `.enable_time()`
    /// 的合法 runtime（如 `Builder::new_current_thread().build()`）调用即 panic，而不是
    /// 返回结构化的超时结果；完全没有 runtime 时同样 panic。这属环境前置条件、不是
    /// 可恢复错误面——签名与返回面（[`ShutdownReport`]）按 #92 取舍保持不变。
    pub async fn shutdown_timeout(&self, d: Duration) -> ShutdownReport {
        self.reject_self_shutdown("shutdown_timeout");
        match self.begin_drain() {
            DrainSlot::Owner => {
                let mut owner = DrainOwner::new(self);
                owner.attach();
                let outcome = tokio::time::timeout(d, owner.drain_all()).await;
                match outcome {
                    Ok(()) => owner.publish(),
                    Err(_elapsed) => owner.settle_deadline(),
                }
            }
            DrainSlot::Waiter(rx) => {
                let waited = tokio::time::timeout(d, await_settled(rx)).await;
                match waited {
                    Ok(report) => report,
                    // 自己的截止到期：只停止自己的等待，不触碰 owner 的排空。
                    Err(_elapsed) => {
                        let unconfirmed = self.in_flight();
                        ShutdownReport {
                            timed_out: unconfirmed,
                            still_running: unconfirmed,
                            ..ShutdownReport::default()
                        }
                    }
                }
            }
            DrainSlot::Settled(report) => report,
        }
    }

    /// 增量收割口（issue#13 缺口一）：锁内同步扫集合中**已完成**条目——正常完成
    /// 丢弃、panic 载荷归集返回、abort 条目映射 [`TaskError::Cancelled`]。不动
    /// 在途任务、不改 `in_flight` 计数（FlightGuard 独立计数，与集合条目无关）；
    /// 空集零分配（`Vec::new`），幂等可反复调。
    ///
    /// 锁内禁 await：`try_join_next` 同步不挂起，临界区保持短（与 spawn 同口径）。
    ///
    /// **锁边界（issue#91）**：`self.set` 锁内只把条目**搬出**集合（`JoinError` 原样
    /// 带出，不归一）；`into_panic` / `panic_message` / downcast / 字符串分配，以及
    /// panic payload 的最终 Drop 全在锁外。payload 是宿主提供的任意 `Any + Send`，
    /// 其 Drop 可重入本 scope（持锁则自锁）或自身 panic（持锁则毒化集合锁）。
    ///
    /// 防御分支：包装体自带 catch_unwind，poll 期 panic 理论到不了 JoinSet 层；若
    /// 出现（包装被绕过 / panic 发生在 future 自身的 Drop），按载荷映射
    /// [`TaskError::Panicked`] 显式上报，禁静默丢条目。
    pub fn try_reap(&self) -> Vec<TaskError> {
        let mut reaped = Vec::new();
        // `JoinError` 承载宿主 payload：只搬出，不在此析构（见「锁边界」段）。
        let mut join_failures = Vec::new();
        {
            let mut set = lock(&self.set);
            let mut harvested = 0usize;
            while let Some(outcome) = set.try_join_next() {
                harvested += 1;
                match outcome {
                    Ok(Ok(())) => {}
                    Ok(Err(te)) => reaped.push(te),
                    Err(je) => join_failures.push(je),
                }
            }
            // 收割记账：条目出集合 ⇒ 保留数与未收割数各减一（锁序 set → caps）。
            // 记账留在 `set` 锁内：与 `close_and_take` 的「取走集合 + 记账归零」互斥，
            // 否则「先取集、后减账」的窗会让归零之后的减法下溢（低估 ⇒ 准入超上界）。
            // 锁内只做短同步账目变更，不触碰任何宿主载荷。
            //
            // 安全性依赖**外部**不变量：tokio 保证任务析构先于 JoinError 观测 ⇒ 出集合
            // 的条目必然已计入这两项。release 下 usize 减法无下溢保护（wrap 成巨值），
            // 故 debug 构建显式锚住该前提（审计 F4；zero-cost in release）。
            if harvested > 0 {
                let mut caps = lock(&self.caps);
                debug_assert!(
                    caps.tracked >= harvested && caps.completed_unreaped >= harvested,
                    "卡账不变量破损：tracked={} completed_unreaped={} harvested={}（tokio 任务析构先于 JoinError 观测之前提被破坏）",
                    caps.tracked,
                    caps.completed_unreaped,
                    harvested
                );
                caps.tracked -= harvested;
                caps.completed_unreaped -= harvested;
            }
        }
        // 锁外归一：downcast、字符串分配、payload 的最终 Drop 都在集合锁之外。
        for je in join_failures {
            reaped.push(if je.is_cancelled() {
                TaskError::Cancelled
            } else {
                TaskError::Panicked(panic_message(je.into_panic()))
            });
        }
        reaped
    }

    fn close_and_take(&self) -> JoinSet<Result<(), TaskError>> {
        self.close();
        let mut set = lock(&self.set);
        set.abort_all();
        let taken = std::mem::take(&mut *set);
        // 记账收口：集合已被取走 ⇒ scope 不再保留任何条目（跟踪面归零；此后终局
        // 不再计入未收割数——条目已不在本 scope 的集合里）。
        {
            let mut caps = lock(&self.caps);
            caps.tracking = false;
            caps.tracked = 0;
            caps.completed_unreaped = 0;
        }
        taken
    }

    /// 三资源轴占用快照（issue#39 可观测面）：`active`（并发许可占用）/
    /// `waiting`（等待许可的已准入任务）/ `completed_unreaped`（已完成未收割的
    /// 集合条目）/ `rejected`（因上界被拒的准入次数）。
    pub fn counts(&self) -> FxCounts {
        let caps = lock(&self.caps);
        FxCounts {
            active: caps.active,
            waiting: caps.waiting,
            completed_unreaped: caps.completed_unreaped,
            rejected: caps.rejected,
        }
    }

    /// 任务体已结束断言（body-idle）：所有已准入任务的任务体都已终局（完成 /
    /// panic / 取消 / abort）——**不**要求生命周期集合已收割。
    ///
    /// 与 [`Self::expect_fully_reaped_idle`] 分工：后者要求集合条目已全部收割
    /// （更强的判据）。两个名字各自精确，禁互相误读。
    pub fn expect_body_idle(&self) {
        // 断言在锁外：锁内 panic 会毒化 caps 锁（后续观测全被毒化掩盖）。
        let (admitted, terminal) = {
            let caps = lock(&self.caps);
            (caps.admitted, caps.terminal)
        };
        assert!(
            terminal == admitted,
            "fx scope not body-idle: {} task body(ies) still running",
            admitted - terminal
        );
    }

    /// 跟踪集合已完全收割断言（fully-reaped-idle）：生命周期集合无滞留条目
    /// （全部已收割，或已随停机排空被取走）——比 [`Self::expect_body_idle`] 更强：
    /// 任务体结束**不**蕴含条目已出集合（`Spawned::join()` 不移除条目）。
    pub fn expect_fully_reaped_idle(&self) {
        // 断言在锁外：锁内 panic 会毒化 caps 锁（后续观测全被毒化掩盖）。
        let retained = { lock(&self.caps).tracked };
        assert!(
            retained == 0,
            "fx scope not fully-reaped-idle: {retained} entry(ies) retained in the lifecycle set"
        );
    }
}

impl Default for FxScope {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for FxScope {
    /// scope 消失 = 本域生命周期的终点：**先**触发内部源的粘性取消（issue#119）
    /// ——早已克隆出去的 token 随之观察到 `true`，禁让默认 token 成为永久 `false`
    /// 的假观察面；随后字段 drop 让 `JoinSet` 请求 abort 全部托管任务（既有
    /// 「scope drop ⇒ 全组 abort」语义不变，本 impl 不 join、不等）。
    ///
    /// **注入外部源（`with_source`）时不动它**：取消权在外部 owner。
    fn drop(&mut self) {
        self.cancel_owned_source();
    }
}

/// spawn 句柄：结果走 oneshot 旁路（生命周期集条目载 panic 归一面，规避 JoinSet 单类型限制）。
/// Drop 不 abort——任务归生命周期集托管（忘 join 也不泄漏，JoinSet Drop 兜底）。
pub struct Spawned<T> {
    handle: AbortHandle,
    rx: oneshot::Receiver<Result<T, TaskError>>,
}

impl<T: Send + 'static> Spawned<T> {
    /// recv 映射：Ok(v)→v；recv Err（发送端随 abort 消亡）→TaskError::Cancelled。
    ///
    /// **只消费结果、不移除生命周期集合条目**：join 之后该任务的集合条目仍滞留
    /// （含已完成未收割者）⇒ 保留量计数 [`FxScope::counts`] 的
    /// `completed_unreaped` 不因 join 而减少。出集合只能靠 [`FxScope::try_reap`]
    /// 或停机排空（[`FxScope::shutdown`]）。保留量上界见
    /// [`FxScope::with_max_tracked`]。
    ///
    /// 本 await 同样是**协作式**的（issue#94）：对端任务停在同步段内不返回 poll 时，
    /// `join` 也只能等它返回——`join` 不抢占、不推进也不终止对端。
    pub async fn join(self) -> Result<T, TaskError> {
        match self.rx.await {
            Ok(result) => result,
            Err(_dropped) => Err(TaskError::Cancelled),
        }
    }

    /// 超时 join：期限内完成⇒直传结果；逾期限⇒**请求 abort** 并回
    /// [`TaskError::TimedOut`]。
    ///
    /// **超时路径不确认任务体已终止**：tokio 的 abort 是取消请求——任务体在下一个
    /// 让渡点才被销毁 ⇒ 返回时任务体**可能仍在执行**（如正停在非让渡 poll 段内）。
    /// [`TaskError::TimedOut`] 只表达「截止已过 + 已请求 abort」，与
    /// [`ShutdownReport::timed_out`]（截止计数）/ [`ShutdownReport::still_running`]
    /// （终止未观测）同一口径：**截止 ≠ 终止**。
    ///
    /// **终止确认走 scope 级观察面**（本方法消费句柄，调用方不再持有它）：
    /// [`FxScope::expect_body_idle`] 通过 ⇒ 任务体已终局（在途计数与终局记账都在
    /// 任务体真正终止后才更新）；[`FxScope::expect_fully_reaped_idle`] 通过 ⇒ 集合
    /// 条目已收割（**终局 ≠ 已收割**，两层分开）。消费句柄**不**取消 scope 的生命
    /// 周期跟踪——任务仍归 scope 生命周期集托管。
    ///
    /// # 截止的保证边界（issue#94）
    ///
    /// `d` 依赖 tokio timer：executor 可继续调度时才保证「到期触发并请求 abort」。
    /// 任务若停在不返回 poll 的同步段内，本方法也只能在该段返回、executor 恢复后
    /// 才拿到 `TimedOut` ⇒ `d` **不是**硬墙钟上限。CPU / 阻塞 IO 应移出 executor
    /// （`tokio::task::spawn_blocking` 或独立 `ThreadPool`），见模块文档「执行模型与
    /// 阻塞边界」。
    ///
    /// # Panics
    ///
    /// 需要**启用 time driver 的 Tokio runtime 上下文**（issue#92）：截止句柄在构造
    /// 时向当前 runtime 的 time driver 注册——没开 `.enable_time()` 的合法 runtime
    /// （如 `Builder::new_current_thread().build()`）下调用即 panic，而不是返回
    /// [`TaskError::TimedOut`]；完全没有 runtime 时同样 panic。环境前置条件而非可恢复
    /// 错误面：签名与 [`TaskError`] 返回面按 #92 取舍保持不变。
    pub async fn join_timeout(self, d: Duration) -> Result<T, TaskError> {
        match tokio::time::timeout(d, self.rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_dropped)) => Err(TaskError::Cancelled),
            Err(_elapsed) => {
                self.handle.abort();
                Err(TaskError::TimedOut)
            }
        }
    }

    /// 中止任务（幂等；join 侧见 `TaskError::Cancelled`）。
    ///
    /// **取消请求，非同步终止**：任务体在下一个让渡点才被销毁⇒调用返回时任务体
    /// 可能仍在执行（与 [`TaskError::TimedOut`] 同一口径：abort 已请求 ≠ 已终止）。
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// 任务是否已终局（完成/中止/panic 皆真；在途=false）。
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::guard::{bounded, kick_scheduler, wait_until};

    /// abort 的取消 poll 异步生效：等计数归零（1ms 实时钟轮询，5s 墙钟上限，防挂死）。
    /// 不能用纯让渡预算：让渡不释放 OS 线程，wrapper drop 序列（tx drop 唤醒在前、
    /// FlightGuard drop 在后）被 OS 量子级抢占时，10⁴ 让渡毫秒级耗尽而对方几十毫秒
    /// 后才被调度回来（CI 2 核饱和实测）——真实 sleep 让核给 drop 执行线程。
    async fn wait_idle(scope: &FxScope) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if scope.in_flight() == 0 {
                return;
            }
            if std::time::Instant::now() >= deadline {
                panic!(
                    "fx scope not idle: {} task(s) still in flight",
                    scope.in_flight()
                );
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// ①spawn/join 值直传 Ok。
    #[tokio::test]
    async fn spawn_join_passes_value_ok() {
        let scope = FxScope::new();
        let handle = scope.spawn_tracked(async { 41 + 1 }).unwrap();
        assert_eq!(handle.join().await, Ok(42));
        scope.expect_body_idle();
    }

    /// ②abort→join=Cancelled。
    #[tokio::test]
    async fn abort_yields_cancelled() {
        let scope = FxScope::new();
        let handle = scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                1
            })
            .unwrap();
        handle.abort();
        assert_eq!(handle.join().await, Err(TaskError::Cancelled));
        wait_idle(&scope).await;
        scope.expect_body_idle();
    }

    /// ③任务 panic（含非 String payload）→join=Panicked(msg)。
    #[tokio::test]
    async fn panic_maps_to_panicked_with_message() {
        let scope = FxScope::new();
        let handle = scope.spawn_tracked(async { panic!("boom-42") }).unwrap();
        assert_eq!(
            handle.join().await,
            Err(TaskError::Panicked("boom-42".to_owned()))
        );
        let handle = scope
            .spawn_tracked(async { std::panic::panic_any(7_i32) })
            .unwrap();
        assert_eq!(
            handle.join().await,
            Err(TaskError::Panicked("non-string panic payload".to_owned()))
        );
        scope.expect_body_idle();
    }

    /// ④join_timeout 超时→TimedOut（**abort 请求已发**；本任务停在 await 点 ⇒
    /// abort 立即生效）且 in_flight 归零。
    ///
    /// 终止未确认的路径（任务体停在非让渡段）见集成回归
    /// `tests/fx_join_timeout_semantics.rs`——本例只覆盖「可 abort 段」。
    #[tokio::test(start_paused = true)]
    async fn join_timeout_aborts_and_reports_timed_out() {
        let scope = FxScope::new();
        let handle = scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                1
            })
            .unwrap();
        assert_eq!(
            bounded(
                "join_timeout 有界返回",
                handle.join_timeout(Duration::from_secs(1)),
            )
            .await,
            Err(TaskError::TimedOut)
        );
        wait_idle(&scope).await;
        scope.expect_body_idle();
    }

    /// ⑤close 后 spawn=Err(ScopeClosed)，close 前 spawn 正常、在途可 join。
    #[tokio::test]
    async fn close_rejects_new_spawns_but_allows_in_flight() {
        let scope = FxScope::new();
        let handle = scope.spawn_tracked(async { 5 }).unwrap();
        scope.close();
        assert!(scope.is_closed());
        assert!(matches!(scope.spawn_tracked(async { 6 }), Err(ScopeClosed)));
        assert_eq!(handle.join().await, Ok(5)); // 在途放行
        assert!(matches!(scope.spawn_tracked(async { 7 }), Err(ScopeClosed))); // 幂等仍拒
    }

    /// ⑥shutdown 等净（in_flight==0，报告零事件）+ shutdown_timeout 超时路径
    /// report.timed_out>0（超时不再返回 Err——报告形，issue#13）。
    /// 超时路径按「不可 abort 段」语义：abort 只在下一 await 生效，任务 poll 不返回
    /// 则 drain 真超时——需多线程 rt + 实时钟。确定性构造：hold 闸时序由测试持有
    /// ——holding 握手确认任务已滞留 poll（无可让渡 await 点）后才起 50ms 计时，
    /// 忙段持续到 hold 放开，慢机调度延迟无法击穿（旧实现靠「200ms spin 段 > 50ms
    /// 超时」固定余量，2 核 CI 握手延迟 >150ms 即翻成 Ok(())）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_and_shutdown_timeout_reports_timed_out() {
        let scope = FxScope::new();
        for i in 0..4u32 {
            scope.spawn_tracked(async move { i }).unwrap();
        }
        let report = scope.shutdown().await;
        // abort 与任务 poll 竞速：四条任务可能落 completed 或 cancelled，但必各落
        // 一桶（桶穷尽面）；截止/未观测两桶恒 0（排空至空集）。
        assert_eq!(
            report.completed + report.cancelled,
            4,
            "四条任务各落一桶（排空收割到空集）"
        );
        assert!(report.panicked.is_empty(), "无 panic 载荷");
        assert_eq!(report.timed_out, 0, "shutdown 无截止 ⇒ 恒 0");
        assert_eq!(report.still_running, 0, "等净至空集 ⇒ 无未观测终局");
        scope.expect_body_idle();

        use std::time::Instant;
        let hold = Arc::new(AtomicBool::new(false)); // 测试→任务：留在 poll 内不让渡
        let holding = Arc::new(AtomicBool::new(false)); // 任务→测试：已进不可让渡段
        let stop = Arc::new(AtomicBool::new(false));
        let scope = FxScope::new();
        let hold_task = Arc::clone(&hold);
        let holding_task = Arc::clone(&holding);
        let stop_task = Arc::clone(&stop);
        let spinner = scope
            .spawn_tracked(async move {
                while !stop_task.load(Ordering::Relaxed) {
                    if hold_task.load(Ordering::Relaxed) {
                        holding_task.store(true, Ordering::Relaxed); // 已进 poll 无 await 点，abort 只能等让渡
                        while hold_task.load(Ordering::Relaxed)
                            && !stop_task.load(Ordering::Relaxed)
                        {
                            std::hint::spin_loop();
                        }
                        holding_task.store(false, Ordering::Relaxed);
                    }
                    tokio::task::yield_now().await; // 唯一 abort 点
                }
            })
            .unwrap();
        // 握手：任务确实滞留在 poll 段内（未启动任务的 abort=免运行即撤，模拟不了该场景）。
        hold.store(true, Ordering::Relaxed);
        wait_until(
            "自旋任务已进入不可让渡段（留在 poll 内不让渡）",
            || holding.load(Ordering::Relaxed),
        )
        .await;
        // 自旋段占住原 driver 持有者：kick 一台普通休眠 worker 重新 park，让它以
        // driver 身份 park 并处理时间轮——否则 50ms 截止永不到期（issue#102 机理）。
        kick_scheduler(
            "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
            FxScope::new()
                .spawn_detached(async {})
                .expect("一次性 kick scope 新开 ⇒ 闸必开"),
        )
        .await;
        let started = Instant::now();
        let report = bounded(
            "shutdown_timeout 有界返回",
            scope.shutdown_timeout(Duration::from_millis(50)),
        )
        .await;
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "超时必达：提前返回即守卫失效"
        );
        assert_eq!(
            report.timed_out, 1,
            "超时收割：截止时未完成任务数显式计数（Drop=abort 兜底）"
        );
        hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一 await 点生效
        stop.store(true, Ordering::Relaxed); // 放行自终止，运行时干净退出
        assert_eq!(
            spinner.join().await,
            Err(TaskError::Cancelled),
            "超时收割：abort 在任务下一 await 点生效（事件化等待其消亡）"
        );
        // join 返回≠guard 已归零：tx drop（唤醒 join）与 wrapper 内 FlightGuard drop
        // 是相邻语句，跨线程可被抢占观测（macos 2 核 CI flake 实证）——有界让渡等真实归零。
        wait_idle(&scope).await;
        scope.expect_body_idle();
    }

    /// ⑦with_concurrency(max)=并发度实测 ≤max 且 permit 随 abort 回收（满载 abort 后
    /// 新任务可进）；构造期 max=0 assert fail-fast。
    #[tokio::test(start_paused = true)]
    async fn concurrency_cap_permit_recycle_and_zero_max_fail_fast() {
        assert!(catch_unwind(|| FxScope::new().with_concurrency(0)).is_err());

        let live = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let scope = FxScope::new().with_concurrency(2);
        let mut handles = Vec::new();
        for _ in 0..6 {
            let live = Arc::clone(&live);
            let peak = Arc::clone(&peak);
            handles.push(
                scope
                    .spawn_tracked(async move {
                        let now = live.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        live.fetch_sub(1, Ordering::SeqCst);
                    })
                    .unwrap(),
            );
        }
        for handle in handles {
            handle.join().await.unwrap();
        }
        let observed = peak.load(Ordering::SeqCst);
        assert!(observed >= 1);
        assert!(observed <= 2, "observed concurrency {observed} > cap 2");

        // permit 随 abort 回收：满载 1 槽，堵死在 Notify；排队的任务在 abort 后放行。
        let scope = FxScope::new().with_concurrency(1);
        let blocker = scope
            .spawn_tracked(async {
                tokio::sync::Notify::default().notified().await;
            })
            .unwrap();
        let queued = scope.spawn_tracked(async { 99u8 }).unwrap();
        assert!(!queued.is_finished()); // 卡在 acquire，未执行
        blocker.abort();
        assert_eq!(queued.join().await, Ok(99));
        scope.expect_body_idle();
    }

    /// ⑧Drop Spawned 不 abort（忘 join 任务仍跑完且 in_flight 归零）。
    #[tokio::test]
    async fn dropping_spawned_does_not_abort_task() {
        let ran = Arc::new(AtomicUsize::new(0));
        let scope = FxScope::new();
        let ran_task = Arc::clone(&ran);
        let handle = scope
            .spawn_tracked(async move {
                ran_task.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        drop(handle); // 忘 join
        wait_idle(&scope).await;
        assert_eq!(ran.load(Ordering::SeqCst), 1); // 任务仍跑完
        scope.expect_body_idle();
    }

    /// ⑨cancel_pair 传播（cancel→token.is_cancelled/run_until_cancelled=None）+
    /// 外接源同 channel（cancel 先于接线，状态经 subscribe 可见）。
    #[tokio::test]
    async fn cancel_pair_propagates_and_external_source_shares_channel() {
        let (source, token) = cancel_pair();
        assert!(!token.is_cancelled());
        assert_eq!(token.run_until_cancelled(async { 7 }).await, Some(7));
        source.cancel();
        assert!(source.is_cancelled());
        assert!(token.is_cancelled());
        token.cancelled().await;
        assert_eq!(token.run_until_cancelled(async { 7 }).await, None);

        let (source, _observer) = cancel_pair();
        source.cancel();
        let scope = FxScope::new().with_source(source);
        assert!(scope.token().is_cancelled());
    }

    /// ⑩跨任务共享 Arc<FxScope> 并发 spawn 无死锁（多任务压力 spawn-join 循环）。
    #[tokio::test]
    async fn shared_scope_concurrent_spawn_join_no_deadlock() {
        let scope = Arc::new(FxScope::new());
        let mut outers = Vec::new();
        for _ in 0..8 {
            let caller = Arc::clone(&scope);
            let runner = Arc::clone(&scope);
            outers.push(
                caller
                    .spawn_tracked(async move {
                        let mut acc = 0u32;
                        for _ in 0..16 {
                            let inner = runner.spawn_tracked(async { 3u32 }).unwrap();
                            acc += inner.join().await.unwrap_or(0);
                        }
                        acc
                    })
                    .unwrap(),
            );
        }
        let mut total = 0u32;
        for outer in outers {
            total += outer.join().await.unwrap_or(0);
        }
        assert_eq!(total, 8 * 16 * 3);
        scope.expect_body_idle();
    }

    /// 零覆盖 #14：shutdown 超时路径对在途任务的 abort 收割——超时返回
    /// report.timed_out>0 后，遗留 JoinSet Drop=abort 兜底；abort 在任务下一 await
    /// 点生效，join 即收割事件：join=Err(Cancelled) 且 in_flight 归零（旧 ⑥
    /// 只断言超时本身，未直接断言 abort 语义与 join 的组合）。确定性构造同 ⑥：
    /// hold 闸由测试持有（holding 握手确认滞留 poll 后才计时），不用固定 spin 段
    /// 余量赌慢机调度延迟。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_timeout_aborts_inflight_and_join_yields_cancelled() {
        let scope = FxScope::new();
        let hold = Arc::new(AtomicBool::new(false));
        let holding = Arc::new(AtomicBool::new(false));
        let hold_task = Arc::clone(&hold);
        let holding_task = Arc::clone(&holding);
        let handle = scope
            .spawn_tracked(async move {
                loop {
                    if hold_task.load(Ordering::Relaxed) {
                        holding_task.store(true, Ordering::Relaxed); // 已进 poll 无 await 点，abort 只能等让渡
                        while hold_task.load(Ordering::Relaxed) {
                            std::hint::spin_loop();
                        }
                        holding_task.store(false, Ordering::Relaxed);
                    }
                    tokio::task::yield_now().await; // 唯一 abort 点
                }
            })
            .unwrap();
        // 握手：任务确实滞留在 poll 段内（免运行即撤模拟不了该场景）。
        hold.store(true, Ordering::Relaxed);
        wait_until(
            "自旋任务已进入不可让渡段（留在 poll 内不让渡）",
            || holding.load(Ordering::Relaxed),
        )
        .await;
        // 自旋段占住原 driver 持有者：kick 一台普通休眠 worker 重新 park，让它以
        // driver 身份 park 并处理时间轮——否则 50ms 截止永不到期（issue#102 机理）。
        kick_scheduler(
            "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
            FxScope::new()
                .spawn_detached(async {})
                .expect("一次性 kick scope 新开 ⇒ 闸必开"),
        )
        .await;
        let report = bounded(
            "shutdown_timeout 有界返回",
            scope.shutdown_timeout(Duration::from_millis(50)),
        )
        .await;
        assert_eq!(report.timed_out, 1, "超时收割：截止时未完成任务数显式计数");
        hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一 await 点生效
        assert_eq!(
            handle.join().await,
            Err(TaskError::Cancelled),
            "超时遗留任务被 abort 收割"
        );
        // join 返回≠guard 已归零（tx drop 唤醒在前、FlightGuard drop 在后的跨线程
        // 观测窗，见 ⑥）——有界让渡等真实归零。
        wait_idle(&scope).await;
        scope.expect_body_idle(); // abort 即刻回收（FlightGuard Drop）——零 sleep、事件化
    }

    /// ⑪两套 drop 语义·tracked 侧：scope drop ⇒ 生命周期集（JoinSet）drop ⇒ 全组
    /// abort——abort 使 future 被 drop，Drop 哨兵置位（armor 表
    /// tracked_drop_aborts_all）。
    #[tokio::test]
    async fn tracked_drop_aborts_all() {
        struct AbortLatch(Arc<AtomicBool>);
        impl Drop for AbortLatch {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }
        let aborted = Arc::new(AtomicBool::new(false));
        let scope = FxScope::new();
        {
            let latch = AbortLatch(Arc::clone(&aborted));
            scope
                .spawn_tracked(async move {
                    let _latch = latch; // future 被 drop（abort）即置位
                    tokio::time::sleep(Duration::from_secs(60)).await;
                })
                .unwrap();
        }
        drop(scope); // JoinSet drop = abort 全部

        // abort 异步生效：有界实钟轮询等哨兵（同 wait_idle 惯例，禁纯让渡——CI 抢占实证）。
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !aborted.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < deadline,
                "tracked task not aborted after scope drop"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// ⑫两套 drop 语义·detached 侧：shutdown 只等生命周期集——detached 任务不被
    /// 等待也不被中止，shutdown 返回后仍活到完成（armor 表
    /// detached_outlives_scope_documented）。
    #[tokio::test]
    async fn detached_outlives_scope_documented() {
        let started = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let release = Arc::new(tokio::sync::Notify::new());
        let scope = FxScope::new();
        let (started_task, done_task) = (Arc::clone(&started), Arc::clone(&done));
        let release_task = Arc::clone(&release);
        let handle = scope
            .spawn_detached(async move {
                started_task.store(true, Ordering::Release);
                release_task.notified().await; // 等 release 信号（可控完成时刻）
                done_task.store(true, Ordering::Release);
            })
            .unwrap();
        // 握手：任务已启动并挂进 notified（此后 shutdown 返回时必然未终局）。
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        // tracked 面空集 ⇒ shutdown 等净即时返回；detached 不被等待、不被中止。
        let _ = scope.shutdown().await;
        assert!(
            !handle.is_finished(),
            "shutdown must not wait/abort detached tasks"
        );
        // detach 后任务照跑：放行并等其完成（返回面=裸 JoinHandle，可直接 await）。
        release.notify_one();
        handle.await.expect("detached task runs to completion");
        assert!(done.load(Ordering::Acquire));
    }

    /// ⑬文档范式①标准写法的运行版（issue#1-5 验收场景）：外部 sync 句柄
    /// Arc 化 move 进 future——多任务并发写共享账本，join 后断言汇总。
    #[tokio::test]
    async fn arc_handle_borrowed_across_tasks() {
        use std::collections::HashMap;
        let ledger: Arc<Mutex<HashMap<u32, u32>>> = Arc::new(Mutex::new(HashMap::new()));
        let scope = FxScope::new();
        let mut handles = Vec::new();
        for k in 0..8u32 {
            let task_ledger = Arc::clone(&ledger); // 持外部句柄须先 Arc 化 move 进 future
            handles.push(
                scope
                    .spawn_tracked(async move {
                        // std Mutex 短临界区（无 await 持锁）：sync 句柄直接借用
                        task_ledger.lock().expect("poisoned").insert(k, k * k);
                    })
                    .unwrap(),
            );
        }
        for handle in handles {
            handle.join().await.expect("task ok");
        }
        scope.expect_body_idle();
        let map = ledger.lock().expect("poisoned");
        assert_eq!(map.len(), 8);
        for k in 0..8u32 {
            assert_eq!(map[&k], k * k);
        }
    }

    /// 收割·panic 不丢（issue#13 验收①）：panic 任务句柄被丢 → oneshot 旁路
    /// 无人接，载荷只可能从集合收割——try_reap 返回 Panicked 载荷（「不丢」形态）。
    #[tokio::test]
    async fn try_reap_recovers_panicked_payload_with_dropped_handle() {
        let scope = FxScope::new();
        {
            let handle = scope.spawn_tracked(async { panic!("boom-reap") }).unwrap();
            drop(handle); // 句柄即刻丢：join 面消失，集合面是唯一观测口
        }
        wait_idle(&scope).await; // 任务已被 poll 且 panic 落载（FlightGuard 归零）
        let reaped = scope.try_reap();
        assert_eq!(reaped.len(), 1, "panic 载荷滞留集合至收割");
        assert!(
            matches!(&reaped[0], TaskError::Panicked(msg) if msg.contains("boom-reap")),
            "载荷含 panic 消息: {reaped:?}"
        );
    }

    /// 收割·滞留复现+幂等（issue#13 验收②）：N=100 即刻完成任务 wait_idle 后
    /// 条目滞留集合（JoinSet 无 len 观测面，滞留可见性经同集 panic 条目钉死：
    /// 若条目不滞留，panic 载荷早就无处可收）→ try_reap 清空，二次 reap 空且
    /// 零分配（`Vec::new` 形态，capacity==0 钉死）。
    #[tokio::test]
    async fn try_reap_drains_retained_entries_and_is_idempotent() {
        let scope = FxScope::new();
        {
            let handle = scope.spawn_tracked(async { panic!("boom-stay") }).unwrap();
            drop(handle);
        }
        for i in 0..100u32 {
            scope.spawn_tracked(async move { i }).unwrap();
        }
        wait_idle(&scope).await; // 101 条目全部已终局，滞留集合
        let reaped = scope.try_reap();
        assert_eq!(
            reaped.len(),
            1,
            "panic 条目滞留至收割（Ok 条目同集一并清空）"
        );
        assert!(matches!(&reaped[0], TaskError::Panicked(msg) if msg.contains("boom-stay")));
        let again = scope.try_reap();
        assert!(again.is_empty(), "二次收割空（幂等）");
        assert_eq!(again.capacity(), 0, "空收割零分配（Vec::new 形态）");
        scope.expect_body_idle();
    }

    /// 停机报告收 panic（issue#13 验收③）：shutdown_timeout 窗口内 panic 的任务
    /// → report.panicked 含载荷、timed_out==0（窗口内完成，未触发超时）。
    #[tokio::test]
    async fn shutdown_timeout_report_collects_panicked_payload() {
        let scope = FxScope::new();
        {
            let handle = scope
                .spawn_tracked(async { panic!("boom-report") })
                .unwrap();
            drop(handle);
        }
        wait_idle(&scope).await;
        let report = bounded(
            "shutdown_timeout 有界返回",
            scope.shutdown_timeout(Duration::from_secs(1)),
        )
        .await;
        assert_eq!(
            report,
            ShutdownReport {
                panicked: vec!["boom-report".to_owned()],
                ..ShutdownReport::default()
            }
        );
    }

    /// 收割·在途不动（P1）：try_reap 只扫已完成条目——在途任务不被收割、不受
    /// 影响，join 照常收值；Ok 条目收割为空（丢弃面）。
    #[tokio::test(start_paused = true)]
    async fn try_reap_leaves_inflight_tasks_alone() {
        let scope = FxScope::new();
        let handle = scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                7
            })
            .unwrap();
        assert!(scope.try_reap().is_empty(), "在途任务不被收割");
        assert!(!handle.is_finished());
        assert_eq!(handle.join().await, Ok(7), "reap 后在途任务照常完成可 join");
        assert!(scope.try_reap().is_empty(), "Ok 条目收割为空（丢弃面）");
        scope.expect_body_idle();
    }

    /// 收割·abort 条目（P1）：abort 在下一 await 点生效后，条目由 try_reap 收割
    /// 映射 Cancelled（集合面与 join 面同语义）；二次收割空（幂等）。
    #[tokio::test]
    async fn try_reap_maps_aborted_entries_to_cancelled() {
        let scope = FxScope::new();
        let handle = scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .unwrap();
        handle.abort();
        wait_idle(&scope).await; // abort 生效（FlightGuard 归零 = 任务体已 drop）
        assert_eq!(scope.try_reap(), vec![TaskError::Cancelled]);
        assert!(scope.try_reap().is_empty(), "二次收割空（幂等）");
    }

    /// 锁边界（issue#91 验收①②）：panic payload 的 downcast / 摘要分配 / 最终 Drop
    /// 全在 `set` 锁外。
    ///
    /// 构造：future 里挂一枚 panic 炸弹，abort 时 tokio 在 `cancel_task` 的
    /// catch_unwind 内析构 future ⇒ 炸弹 panic 成 `JoinError` 的负载（poll 期 panic
    /// 会被包装体的 catch_unwind 吃掉，到不了集合层）。炸弹 panic 的**负载本身**是
    /// 探针：它的 Drop 只在 `into_panic()` 之后被裁掉时运行。
    ///
    /// 探针只做 `try_lock`（阻塞式 `lock` 在缺陷态会挂死而非报红）与受控重入。
    #[tokio::test]
    async fn try_reap_drops_payload_outside_set_lock() {
        struct Probe {
            scope: std::sync::Weak<FxScope>,
            lock_free: Arc<AtomicBool>,
            reentered: Arc<AtomicBool>,
        }

        impl Drop for Probe {
            fn drop(&mut self) {
                let Some(scope) = self.scope.upgrade() else {
                    return; // 观察位留 false 由断言报红（禁静默通过）
                };
                let free = match scope.set.try_lock() {
                    Ok(guard) => {
                        drop(guard);
                        true
                    }
                    Err(_) => false,
                };
                self.lock_free.store(free, Ordering::SeqCst);
                if free {
                    // 受控重入：探针已证明锁空闲 ⇒ 需要 set 锁的公开入口不会阻塞。
                    let inner = scope.try_reap();
                    self.reentered.store(inner.is_empty(), Ordering::SeqCst);
                }
            }
        }

        struct PanicOnDrop {
            scope: std::sync::Weak<FxScope>,
            lock_free: Arc<AtomicBool>,
            reentered: Arc<AtomicBool>,
        }

        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                // panic 发生在 future 自身析构（不是 poll）：负载进 JoinError。
                std::panic::panic_any(Probe {
                    scope: self.scope.clone(),
                    lock_free: Arc::clone(&self.lock_free),
                    reentered: Arc::clone(&self.reentered),
                });
            }
        }

        let scope = Arc::new(FxScope::new());
        let lock_free = Arc::new(AtomicBool::new(false));
        let reentered = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let bomb = PanicOnDrop {
            scope: Arc::downgrade(&scope),
            lock_free: Arc::clone(&lock_free),
            reentered: Arc::clone(&reentered),
        };
        let started_in_task = Arc::clone(&started);
        let handle = scope
            .spawn_tracked(async move {
                let _bomb = bomb;
                started_in_task.store(true, Ordering::SeqCst);
                std::future::pending::<()>().await;
            })
            .unwrap();
        wait_until("任务体已开跑", || started.load(Ordering::SeqCst)).await;
        handle.abort();

        let mut reaped = Vec::new();
        wait_until("abort 条目可收割", || {
            reaped = scope.try_reap();
            !reaped.is_empty()
        })
        .await;
        assert_eq!(
            reaped,
            vec![TaskError::Panicked("non-string panic payload".to_owned())],
            "自定义负载仍走非字符串占位摘要（错误分类不变）"
        );
        assert!(
            lock_free.load(Ordering::SeqCst),
            "payload 析构期间 set 锁必须空闲（锁内析构即 issue#91 缺陷）"
        );
        assert!(
            reentered.load(Ordering::SeqCst),
            "锁外 payload Drop 可受控重入 try_reap（不死锁且收割为空）"
        );
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();
    }

    /// 并发准入 + 收割下的记账（issue#91 验收③）：`completed_unreaped` 既不下溢
    /// （wrap 成巨值）也不越过 `max_tracked`；收割后容量回落，重复收割不重复计数。
    ///
    /// 载入器走 [`FxScope::spawn_detached`]（脱管 ⇒ 不进生命周期集，不干扰被测记账；
    /// 也算框架唯一合法的 spawn 口，`tokio::spawn` 直调被 clippy.toml 拦下）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn try_reap_accounting_holds_under_concurrent_admission() {
        const SPAWNERS: usize = 4;
        const ROUNDS: usize = 200;
        const MAX_TRACKED: usize = 4;
        let scope = Arc::new(FxScope::new().with_max_tracked(MAX_TRACKED));
        let mut spawners = Vec::new();
        for _ in 0..SPAWNERS {
            let worker = Arc::clone(&scope);
            let handle = scope
                .spawn_detached(async move {
                    let (mut admitted, mut rejected) = (0usize, 0usize);
                    for round in 0..ROUNDS {
                        match worker.try_spawn_tracked(async {}) {
                            // 句柄即丢：条目滞留集合至收割（不入 oneshot 旁路）。
                            Ok(handle) => {
                                admitted += 1;
                                drop(handle);
                            }
                            Err(_) => rejected += 1,
                        }
                        if round % 16 == 0 {
                            tokio::task::yield_now().await; // 与收割循环交错
                        }
                    }
                    (admitted, rejected)
                })
                .expect("脱管载入器启动");
            spawners.push(handle);
        }
        while spawners.iter().any(|handle| !handle.is_finished()) {
            let unreaped = scope.counts().completed_unreaped;
            assert!(
                unreaped <= MAX_TRACKED,
                "未收割数越界（记账下溢 wrap 或准入超上限）：{unreaped} > {MAX_TRACKED}"
            );
            let _ = scope.try_reap();
            tokio::task::yield_now().await;
        }
        let (mut admitted, mut rejected) = (0usize, 0usize);
        for handle in spawners {
            let (a, r) = handle.await.expect("载入器正常返回");
            admitted += a;
            rejected += r;
        }
        assert_eq!(
            admitted + rejected,
            SPAWNERS * ROUNDS,
            "每次准入尝试要么成功要么 typed 拒绝（结果守恒）"
        );
        assert!(admitted > 0, "至少有一次准入成功");
        // 先等任务体全部终局：`report_terminal`（completed_unreaped += 1）**先于**
        // JoinSet 条目可收割 ⇒ 只等「收割返回空」会与在途终局记账竞速（满载下偶发
        // 1 ≠ 0）。in_flight 归零 ⇒ 所有终局记账已落，断言才是确定性的。
        wait_until("托管任务体全部终局", || scope.in_flight() == 0).await;
        wait_until("集合收割净", || scope.try_reap().is_empty()).await;
        assert_eq!(
            scope.counts().completed_unreaped,
            0,
            "收割后未收割数归零（容量回落）"
        );
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();
        assert!(scope.try_reap().is_empty(), "重复收割不重复计数");
        assert_eq!(scope.counts().completed_unreaped, 0);
    }

    /// future 析构探针（issue#92 锁边界）：记录析构次数，以及析构瞬间 `set` / `caps`
    /// 两把内部锁是否都空闲。锁内析构（其 Drop 可重入本 scope ⇒ 自锁）会把它压成 false。
    struct DropProbe {
        scope: std::sync::Weak<FxScope>,
        locks_free: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            let Some(scope) = self.scope.upgrade() else {
                return; // 观察位留 false 由断言报红（禁静默通过）
            };
            // try_lock 而非 lock：缺陷态（探针在持锁期析构）要报红，不要挂死。
            let set_free = scope.set.try_lock().is_ok();
            let caps_free = scope.caps.try_lock().is_ok();
            self.locks_free
                .store(set_free && caps_free, Ordering::SeqCst);
        }
    }

    /// 带探针的 pending future：永不就绪，析构时运行探针。
    fn probed_pending(
        scope: &Arc<FxScope>,
        locks_free: Arc<AtomicBool>,
        drops: Arc<AtomicUsize>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let probe = DropProbe {
            scope: Arc::downgrade(scope),
            locks_free,
            drops,
        };
        async move {
            let _probe = probe;
            std::future::pending::<()>().await
        }
    }

    /// runtime 上下文预检（issue#92 验收①）：无 runtime ⇒ typed `NoRuntime`——不 panic、
    /// 不消费 Future、不触碰任何内部锁、不动任何计数（tracked / admitted / active /
    /// waiting / rejected 全不变）。
    #[test]
    fn no_runtime_admission_is_typed_and_lock_free() {
        let scope = Arc::new(FxScope::new());
        let locks_free = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));

        let outcome = scope.try_spawn_tracked(probed_pending(
            &scope,
            Arc::clone(&locks_free),
            Arc::clone(&drops),
        ));
        assert!(
            matches!(outcome, Err(AdmissionError::NoRuntime)),
            "无 runtime ⇒ typed NoRuntime（不 panic）"
        );
        assert_eq!(
            scope.counts(),
            FxCounts::default(),
            "NoRuntime 不入容量账（active/waiting/completed_unreaped/rejected 全 0）"
        );
        assert_eq!(scope.in_flight(), 0, "NoRuntime 不动 in_flight");
        assert!(!scope.is_closed(), "NoRuntime 不触碰 accepting 闸");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "宿主 Future 恰析构一次");
        assert!(
            locks_free.load(Ordering::SeqCst),
            "Future 析构不得发生在 set / caps 锁持有期（锁内析构 = 自锁面）"
        );

        // 脱管口同一前置条件（typed 错误，不 panic）。
        let outcome = scope.spawn_detached(probed_pending(
            &scope,
            Arc::clone(&locks_free),
            Arc::clone(&drops),
        ));
        assert!(
            matches!(outcome, Err(AdmissionError::NoRuntime)),
            "spawn_detached 无 runtime ⇒ 明确 typed 错误"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 2, "每个入口恰析构一次");
        assert!(locks_free.load(Ordering::SeqCst));
        assert_eq!(scope.in_flight(), 0, "两套入口都不动 in_flight");
        assert_eq!(scope.counts(), FxCounts::default());
    }

    /// 锁边界直证（issue#92 验收②的确定性形态）：另一线程持着 `set` 锁时，无 runtime
    /// 的拒绝仍须**立刻**返回。实现若先取锁再预检，这次调用会排在锁上 ⇒ 本测试超时判红。
    #[test]
    fn no_runtime_rejection_does_not_wait_for_the_set_lock() {
        let scope = Arc::new(FxScope::new());
        let held = lock(&scope.set); // 同模块直取私有锁：模拟「锁已被人持有」的窗口
        let (tx, rx) = std::sync::mpsc::channel();
        let caller_scope = Arc::clone(&scope);
        let caller = std::thread::spawn(move || {
            let outcome = caller_scope.try_spawn_tracked(std::future::pending::<()>());
            let _ = tx.send(matches!(outcome, Err(AdmissionError::NoRuntime)));
        });
        let typed = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("无 runtime 的拒绝必须发生在取 set 锁之前（持锁时仍须立即返回，而非排队等锁）");
        drop(held);
        assert!(typed);
        caller.join().expect("调用线程正常结束");
    }

    /// legacy `spawn_tracked` 无 runtime ⇒ 带因 panic（`# Panics` 契约，指向 typed
    /// 入口），且容量账零残留、Future 在锁外析构。
    #[test]
    fn legacy_spawn_tracked_without_runtime_panics_with_cause() {
        let scope = Arc::new(FxScope::new());
        let locks_free = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        let fut = probed_pending(&scope, Arc::clone(&locks_free), Arc::clone(&drops));
        let payload = match catch_unwind(AssertUnwindSafe(|| scope.spawn_tracked(fut))) {
            Ok(_) => panic!(
                "无 runtime 的 legacy 入口必须 fail-visible panic（返回 Ok 即静默丢环境错误）"
            ),
            Err(payload) => payload,
        };
        let msg = panic_message(payload);
        assert!(msg.contains("Tokio runtime context"), "panic 带因: {msg}");
        assert!(
            msg.contains("try_spawn_tracked"),
            "panic 指向 typed 入口: {msg}"
        );
        assert_eq!(scope.counts(), FxCounts::default(), "panic 后容量账零残留");
        assert_eq!(scope.in_flight(), 0, "panic 后 in_flight 零残留");
        assert!(!scope.is_closed(), "panic 不动 accepting 闸");
        assert_eq!(drops.load(Ordering::SeqCst), 1, "宿主 Future 恰析构一次");
        assert!(locks_free.load(Ordering::SeqCst), "Future 在锁外析构");
    }

    /// 停机竞态面（issue#92 第 6 条实证）：预检通过但 Handle 所属 runtime 已停机时，
    /// tokio 1.53 **不** panic——任务入集即被丢弃、句柄落 `Cancelled`，记账仍自洽
    /// （无 `tracked` / `in_flight` 残留）。这条钉住「预检之后无需 spawn 失败回滚」。
    #[tokio::test]
    async fn shutdown_runtime_admission_yields_cancelled_without_residue() {
        let dead = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .expect("multi-thread runtime");
        let dead_handle = dead.handle().clone();
        dead.shutdown_background();
        let scope = FxScope::new();
        let spawned = {
            // 进入已停机的 handle 上下文：`try_current` 成功 ⇒ 预检放行（真实竞态面）。
            let _entered = dead_handle.enter();
            scope
                .try_spawn_tracked(std::future::ready(9u8))
                .expect("停机中的 runtime 入集不 panic（tokio 丢弃任务、句柄落 cancelled）")
        };
        assert_eq!(
            spawned.join().await,
            Err(TaskError::Cancelled),
            "入集即被丢弃 ⇒ 句柄落 Cancelled"
        );
        wait_until("停机任务条目可收割", || {
            !scope.try_reap().is_empty()
        })
        .await;
        assert_eq!(scope.counts().completed_unreaped, 0, "收割后未收割数归零");
        assert_eq!(scope.in_flight(), 0, "FlightGuard 随任务丢弃归零");
        scope.expect_body_idle();
        scope.expect_fully_reaped_idle();
    }

    /// runtime 上下文内的入口照常（issue#92 回归面）：typed 入场成功且 join 语义不变，
    /// detached 仍脱管（不计 in_flight、不被 shutdown 等待）。
    #[tokio::test]
    async fn runtime_context_admission_paths_unchanged() {
        let scope = FxScope::new();
        let handle = scope
            .try_spawn_tracked(async { 7u8 })
            .expect("runtime 内 typed 准入成功");
        assert_eq!(handle.join().await, Ok(7));
        let detached = scope
            .spawn_detached(std::future::ready(()))
            .expect("runtime 内脱管口成功");
        detached.await.expect("脱管任务正常结束");
        assert_eq!(scope.in_flight(), 0, "detached 不计 in_flight");
        scope.expect_body_idle();
    }

    /// 环境故障分离（issue#92 评论 1）：**没有 time driver** 的合法 runtime 下，两个
    /// timeout API 按 `# Panics` 契约 panic（而不是返回结构化超时结果）；随后用开
    /// `.enable_time()` 的 runtime 跑通原分类，证明约束只在时间驱动前置条件上。
    #[test]
    fn timeout_apis_panic_without_time_driver_and_keep_classification_with_it() {
        // 完全合法但未 enable_time 的 current-thread runtime。
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime（未 enable_time）");
        let scope = FxScope::new();
        assert!(
            catch_unwind(AssertUnwindSafe(
                || rt.block_on(scope.shutdown_timeout(Duration::from_millis(10)))
            ))
            .is_err(),
            "无 time driver ⇒ shutdown_timeout 必须 panic（不是结构化超时报告）"
        );
        drop(rt);

        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime（未 enable_time）");
        let scope = FxScope::new();
        let spawned = rt.block_on(async { scope.spawn_tracked(async { 1u8 }).unwrap() });
        assert!(
            catch_unwind(AssertUnwindSafe(|| rt.block_on(async move {
                spawned.join_timeout(Duration::from_millis(10)).await
            })))
            .is_err(),
            "无 time driver ⇒ join_timeout 必须 panic（不是 TaskError::TimedOut）"
        );
        drop(rt);

        // 开了 time driver：原超时分类与返回面不变。
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime（enable_time）");
        let scope = FxScope::new();
        let report = rt.block_on(scope.shutdown_timeout(Duration::from_millis(10)));
        assert_eq!(report, ShutdownReport::default(), "空 scope：无事件报告");
        // shutdown_timeout 关闸（close 是停机①段）⇒ join 面另起 scope 测。
        let scope = FxScope::new();
        let handle = rt.block_on(async { scope.spawn_tracked(async { 5u8 }).unwrap() });
        assert_eq!(
            rt.block_on(handle.join_timeout(Duration::from_secs(1))),
            Ok(5),
            "time driver 就位 ⇒ 正常直传结果（分类不变）"
        );
    }

    /// issue#133：自停机拒绝与排空状态**无关**——别人已是 owner（`Draining`）时，
    /// 本 scope 的托管任务调用仍 fail-fast，不会退化成「成为等待者再被 owner 的
    /// abort 掉」；且拒绝路径不动闸、不改排空状态。
    #[test]
    fn self_shutdown_rejection_is_independent_of_drain_state() {
        let scope = FxScope::new();
        assert!(
            matches!(scope.begin_drain(), DrainSlot::Owner),
            "本测试需先让 scope 进入 Draining"
        );
        let rejected = catch_unwind(AssertUnwindSafe(|| {
            let _mark = ScopeMark::install(scope.id);
            scope.reject_self_shutdown("shutdown");
        }));
        assert!(rejected.is_err(), "Draining 下自停机仍必须 fail-fast");
        assert!(
            matches!(&*scope.drain.borrow(), DrainState::Draining),
            "拒绝不得改排空状态（仍是 Draining，未被结算）"
        );
        assert!(!scope.is_closed(), "拒绝不得关 accepting 闸");
    }

    /// 对照：标记不在本 scope 身份上（外部调用者 / 其他 scope 的任务）时不拒。
    #[test]
    fn self_shutdown_rejection_only_matches_own_scope_marker() {
        let scope = FxScope::new();
        let other = FxScope::new();
        let external = catch_unwind(AssertUnwindSafe(|| scope.reject_self_shutdown("shutdown")));
        assert!(external.is_ok(), "无标记（外部调用者）必须放行");
        let sibling = catch_unwind(AssertUnwindSafe(|| {
            let _mark = ScopeMark::install(other.id);
            scope.reject_self_shutdown("shutdown");
        }));
        assert!(sibling.is_ok(), "其他 scope 的托管任务必须放行");
        assert!(!scope.is_closed(), "未被拒绝 ⇒ 状态未变");
    }
}
