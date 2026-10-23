//! 托管 spawn 唯一合法口：JoinSet 生命周期集 + oneshot 结果旁路 + accepting 闸 + 取消令牌。
//!
//! feature 门 `tokio`。绑 tokio（JoinSet/oneshot/watch/time::timeout/
//! Semaphore）；CancellationToken 树/tokio-util 不引入（语义零增量+父 drop 陷阱）。
//!
//! 零依赖纪律（同 journal.rs/text.rs 先例）：`thiserror::Error` 派生
//! 以手写同形 `Display`/`Error` impl 落地（错误文案与派生形态逐字一致）；
//! 引入 thiserror 依赖时统一换回派生。

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::{oneshot, watch, Semaphore};
use tokio::task::{AbortHandle, JoinSet};

/// std Mutex 取锁：中毒 = 持锁方已 panic、状态不可信——fail-fast 传播 panic
/// （核心壳 `lock` 同形，禁静默吞毒锁）。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("mutex poisoned: prior panic while holding the lock")
}

/// 任务结局（闭合 enum；Panicked 经 catch_unwind 归一）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskError {
    /// 发送端消亡（abort / scope drop）时 recv 侧的映射。
    Cancelled,
    /// panic 消息（payload downcast &str/String，否则哨兵文案）。
    Panicked(String),
    /// join 超时；任务已 abort（禁留活口）。
    TimedOut,
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("task cancelled before completion"),
            Self::Panicked(msg) => write!(f, "task panicked: {msg}"),
            Self::TimedOut => f.write_str("join timed out; task aborted"),
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

/// 取消源（fx 域停机三段的② abort 协作取消通知面，三段定义见
/// [`FxScope::shutdown`] 文档）；幂等 cancel。
pub struct CancelSource {
    tx: watch::Sender<bool>,
}

impl CancelSource {
    /// 触发取消（幂等；观察面 `CancelToken` 恒见 `false→true` 恰一次）。
    pub fn cancel(&self) {
        // 幂等：重复 cancel 语义恒真；接收端全部丢弃时 send 失败可忽略（无观察者）。
        let _ = self.tx.send(true);
    }

    /// 取消态读侧（非阻塞快照）。
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

    /// cancel-safe（false→true 才返回）。
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

/// 任务包装体：permit-in-future（abort 连带回收 permit）→ catch_unwind 归一
/// → oneshot 发送。接收端已亡（Spawned drop/abort）= 结果无处投递即弃，任务本体仍归
/// 生命周期集托管。
async fn task_body<T, F>(
    sem: Option<Arc<Semaphore>>,
    fut: F,
    tx: oneshot::Sender<Result<T, TaskError>>,
) where
    F: Future<Output = T>,
{
    let _permit = match sem {
        None => None,
        Some(sem) => match sem.acquire_owned().await {
            Ok(permit) => Some(permit),
            // 信号量关闭在本 API 面不存在（从不 close）；显式归 Cancelled 禁吞。
            Err(_) => {
                let _ = tx.send(Err(TaskError::Cancelled));
                return;
            }
        },
    };
    let fut = std::pin::pin!(fut);
    let result = match CatchUnwind(fut).await {
        Ok(value) => Ok(value),
        Err(payload) => Err(TaskError::Panicked(panic_message(payload))),
    };
    let _ = tx.send(result);
}

/// 托管 scope：内部 = `Mutex<JoinSet<()>>`（生命周期集，短临界区零 await）+ AtomicBool
/// accepting 闸 + `Option<Arc<Semaphore>>` 并发闸 + 取消源/令牌。通过 `Arc<FxScope>` 跨任务
/// 共享（spawn/join 均 &self）。
pub struct FxScope {
    set: Mutex<JoinSet<()>>,
    accepting: AtomicBool,
    sem: Option<Arc<Semaphore>>,
    in_flight: Arc<AtomicUsize>,
    source: CancelSource,
    token: CancelToken,
}

impl FxScope {
    /// 新建即 accepting（可 spawn）；并发闸与外接取消源经 builder 链显式加挂。
    pub fn new() -> Self {
        let (source, token) = cancel_pair();
        Self {
            set: Mutex::new(JoinSet::new()),
            accepting: AtomicBool::new(true),
            sem: None,
            in_flight: Arc::new(AtomicUsize::new(0)),
            source,
            token,
        }
    }

    /// 并发闸：每任务 future 内 acquire（permit-in-future——abort 连带回收 permit）。
    pub fn with_concurrency(mut self, max: usize) -> Self {
        assert!(max > 0, "fx scope concurrency cap must be > 0");
        self.sem = Some(Arc::new(Semaphore::new(max)));
        self
    }

    /// 外接取消源（scope 内部 token 与外部源同 channel）。
    pub fn with_source(mut self, src: CancelSource) -> Self {
        self.token = CancelToken {
            rx: src.tx.subscribe(),
        };
        self.source = src;
        self
    }

    /// 取观察面令牌（Clone=同一取消态；任务内携带判停）。
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

    /// 唯一合法 spawn 口。闸关=Err(ScopeClosed)；并发闸不在 spawn 点排队（permit 在
    /// future 内）。
    pub fn spawn<T>(
        &self,
        fut: impl Future<Output = T> + Send + 'static,
    ) -> Result<Spawned<T>, ScopeClosed>
    where
        T: Send + 'static,
    {
        let mut set = lock(&self.set);
        // 闸核验=原子 RMW 而非 plain load：close 并发下 spawn 的线性化点。
        // 插入在锁内，与 shutdown 的 abort_all 串行——过闸任务必被 shutdown 覆及。
        if self
            .accepting
            .compare_exchange(true, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(ScopeClosed);
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let sem = self.sem.clone();
        let flight = FlightGuard(Arc::clone(&self.in_flight));
        // tokio ≥1.53：JoinSet::spawn 直接返回 AbortHandle（旧版 JoinHandle::abort_handle 已废）。
        let abort = set.spawn(async move {
            let _flight = flight; // abort/panic/完成三路必经 drop——归零契约的落点
            task_body(sem, fut, tx).await;
        });
        Ok(Spawned { handle: abort, rx })
    }

    /// fx 域停机三段一次收拢：① close（accepting 闸关，拒新 spawn）→
    /// ② abort_all（在途任务收取消通知——[`CancelSource`] 协作取消面）→
    /// ③ 等净（join_next 循环至任务集清空）。锁内 abort_all + mem::take(JoinSet)，
    /// take 后的 set 在锁外 await join_next 循环（禁持 std 锁跨 await）。
    pub async fn shutdown(&self) {
        let mut set = self.close_and_take();
        while set.join_next().await.is_some() {}
    }

    /// 同上+超时：先 close+abort_all，超时回 Err(TimedOut)。abortable 任务零残留
    /// （剩余集 Drop=abort 兜底）；不可 abort 段依赖自终止契约。
    pub async fn shutdown_timeout(&self, d: Duration) -> Result<(), TaskError> {
        let mut set = self.close_and_take();
        let drain = async { while set.join_next().await.is_some() {} };
        if tokio::time::timeout(d, drain).await.is_err() {
            // 超时即返回：set 在此 drop = 剩余任务 abort 兜底（零残留）。
            return Err(TaskError::TimedOut);
        }
        Ok(())
    }

    fn close_and_take(&self) -> JoinSet<()> {
        self.close();
        let mut set = lock(&self.set);
        set.abort_all();
        std::mem::take(&mut *set)
    }
}

impl Default for FxScope {
    fn default() -> Self {
        Self::new()
    }
}

/// spawn 句柄：结果走 oneshot 旁路（生命周期集统一产出 ()，规避 JoinSet 单类型限制）。
/// Drop 不 abort——任务归生命周期集托管（忘 join 也不泄漏，JoinSet Drop 兜底）。
pub struct Spawned<T> {
    handle: AbortHandle,
    rx: oneshot::Receiver<Result<T, TaskError>>,
}

impl<T: Send + 'static> Spawned<T> {
    /// recv 映射：Ok(v)→v；recv Err（发送端随 abort 消亡）→TaskError::Cancelled。
    pub async fn join(self) -> Result<T, TaskError> {
        match self.rx.await {
            Ok(result) => result,
            Err(_dropped) => Err(TaskError::Cancelled),
        }
    }

    /// 超时先 abort 再回 TimedOut（禁留活口——C 类收口）。
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
    pub fn abort(&self) {
        self.handle.abort();
    }

    /// 任务是否已终局（完成/中止/panic 皆真；在途=false）。
    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }
}

/// 泄漏断言（memgov LeakGuard 的 fx 侧锚点；放 fx 因依赖 FxScope 类型）。
pub fn expect_idle(scope: &FxScope) {
    let in_flight = scope.in_flight();
    assert!(
        in_flight == 0,
        "fx scope not idle: {in_flight} task(s) still in flight"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let handle = scope.spawn(async { 41 + 1 }).unwrap();
        assert_eq!(handle.join().await, Ok(42));
        expect_idle(&scope);
    }

    /// ②abort→join=Cancelled。
    #[tokio::test]
    async fn abort_yields_cancelled() {
        let scope = FxScope::new();
        let handle = scope
            .spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                1
            })
            .unwrap();
        handle.abort();
        assert_eq!(handle.join().await, Err(TaskError::Cancelled));
        wait_idle(&scope).await;
        expect_idle(&scope);
    }

    /// ③任务 panic（含非 String payload）→join=Panicked(msg)。
    #[tokio::test]
    async fn panic_maps_to_panicked_with_message() {
        let scope = FxScope::new();
        let handle = scope.spawn(async { panic!("boom-42") }).unwrap();
        assert_eq!(
            handle.join().await,
            Err(TaskError::Panicked("boom-42".to_owned()))
        );
        let handle = scope.spawn(async { std::panic::panic_any(7_i32) }).unwrap();
        assert_eq!(
            handle.join().await,
            Err(TaskError::Panicked("non-string panic payload".to_owned()))
        );
        expect_idle(&scope);
    }

    /// ④join_timeout 超时→TimedOut 且 in_flight 归零（abort 生效断言）。
    #[tokio::test(start_paused = true)]
    async fn join_timeout_aborts_and_reports_timed_out() {
        let scope = FxScope::new();
        let handle = scope
            .spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                1
            })
            .unwrap();
        assert_eq!(
            handle.join_timeout(Duration::from_secs(1)).await,
            Err(TaskError::TimedOut)
        );
        wait_idle(&scope).await;
        expect_idle(&scope);
    }

    /// ⑤close 后 spawn=Err(ScopeClosed)，close 前 spawn 正常、在途可 join。
    #[tokio::test]
    async fn close_rejects_new_spawns_but_allows_in_flight() {
        let scope = FxScope::new();
        let handle = scope.spawn(async { 5 }).unwrap();
        scope.close();
        assert!(scope.is_closed());
        assert!(matches!(scope.spawn(async { 6 }), Err(ScopeClosed)));
        assert_eq!(handle.join().await, Ok(5)); // 在途放行
        assert!(matches!(scope.spawn(async { 7 }), Err(ScopeClosed))); // 幂等仍拒
    }

    /// ⑥shutdown 等净（in_flight==0）+ shutdown_timeout 超时路径 Err(TimedOut)。
    /// 超时路径按「不可 abort 段」语义：abort 只在下一 await 生效，任务 poll 不返回
    /// 则 drain 真超时——需多线程 rt + 实时钟。确定性构造：hold 闸时序由测试持有
    /// ——holding 握手确认任务已滞留 poll（无可让渡 await 点）后才起 50ms 计时，
    /// 忙段持续到 hold 放开，慢机调度延迟无法击穿（旧实现靠「200ms spin 段 > 50ms
    /// 超时」固定余量，2 核 CI 握手延迟 >150ms 即翻成 Ok(())）。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_drains_and_shutdown_timeout_reports_timed_out() {
        let scope = FxScope::new();
        for i in 0..4u32 {
            scope.spawn(async move { i }).unwrap();
        }
        scope.shutdown().await;
        expect_idle(&scope);

        use std::time::Instant;
        let hold = Arc::new(AtomicBool::new(false)); // 测试→任务：留在 poll 内不让渡
        let holding = Arc::new(AtomicBool::new(false)); // 任务→测试：已进不可让渡段
        let stop = Arc::new(AtomicBool::new(false));
        let scope = FxScope::new();
        let hold_task = Arc::clone(&hold);
        let holding_task = Arc::clone(&holding);
        let stop_task = Arc::clone(&stop);
        let spinner = scope
            .spawn(async move {
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
        while !holding.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        let started = Instant::now();
        assert_eq!(
            scope.shutdown_timeout(Duration::from_millis(50)).await,
            Err(TaskError::TimedOut)
        );
        assert!(started.elapsed() >= Duration::from_millis(50));
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
        expect_idle(&scope);
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
                    .spawn(async move {
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
            .spawn(async {
                tokio::sync::Notify::default().notified().await;
            })
            .unwrap();
        let queued = scope.spawn(async { 99u8 }).unwrap();
        assert!(!queued.is_finished()); // 卡在 acquire，未执行
        blocker.abort();
        assert_eq!(queued.join().await, Ok(99));
        expect_idle(&scope);
    }

    /// ⑧Drop Spawned 不 abort（忘 join 任务仍跑完且 in_flight 归零）。
    #[tokio::test]
    async fn dropping_spawned_does_not_abort_task() {
        let ran = Arc::new(AtomicUsize::new(0));
        let scope = FxScope::new();
        let ran_task = Arc::clone(&ran);
        let handle = scope
            .spawn(async move {
                ran_task.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        drop(handle); // 忘 join
        wait_idle(&scope).await;
        assert_eq!(ran.load(Ordering::SeqCst), 1); // 任务仍跑完
        expect_idle(&scope);
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
                    .spawn(async move {
                        let mut acc = 0u32;
                        for _ in 0..16 {
                            let inner = runner.spawn(async { 3u32 }).unwrap();
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
        expect_idle(&scope);
    }

    /// 零覆盖 #14：shutdown 超时路径对在途任务的 abort 收割——超时返回
    /// Err(TimedOut) 后，遗留 JoinSet Drop=abort 兜底；abort 在任务下一 await
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
            .spawn(async move {
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
        while !holding.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            scope.shutdown_timeout(Duration::from_millis(50)).await,
            Err(TaskError::TimedOut)
        );
        hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一 await 点生效
        assert_eq!(
            handle.join().await,
            Err(TaskError::Cancelled),
            "超时遗留任务被 abort 收割"
        );
        // join 返回≠guard 已归零（tx drop 唤醒在前、FlightGuard drop 在后的跨线程
        // 观测窗，见 ⑥）——有界让渡等真实归零。
        wait_idle(&scope).await;
        expect_idle(&scope); // abort 即刻回收（FlightGuard Drop）——零 sleep、事件化
    }
}
