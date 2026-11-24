#![cfg(feature = "tokio")]
//! #67：`Spawned::join_timeout` 超时语义——「已请求 abort」≠「已确认终止」。
//!
//! tokio 的 `AbortHandle::abort()` 是**取消请求**：正在执行非让渡 poll 的任务体要
//! 到下一次让渡点才被销毁。`join_timeout` 在 `abort()` 之后**立即**返回，既不等
//! oneshot 也不等任何收割确认 ⇒ 返回时任务体**可能仍在执行**。
//!
//! 本文件钉四条边界：
//! ① 超时错误的措辞只申报「已请求 abort」，不申报「已终止」（旧文案
//!    「join timed out; task aborted」把取消请求说成已终止——#67 的核心）；
//! ② 任务体停在非让渡段时，超时返回的瞬间 `in_flight` 仍非零（终止未确认）；
//! ③ 句柄被消费（`join_timeout` 取走 `self`）**不**取消 scope 的生命周期跟踪——
//!    终局条目仍留在集合里（`completed_unreaped` 可见），收割后才归零；
//! ④ 期限内正常完成、首次 poll 前 abort 两条既有语义不变。
//!
//! 确定性构造：任务体进入**无 await 的自旋段**（进入点用 oneshot 握手确认）⇒
//! abort 的生效点被推迟到闸门放行；等待一律「有界超时 + 让渡」，禁 sleep。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use skeleton_infra::fx::{FxScope, Spawned, TaskError};
use tokio::sync::oneshot;

#[path = "../src/test_support/guard.rs"]
mod guards;

use guards::{bounded, kick_scheduler, wait_until};

/// 自旋段放行闸的 RAII 兜底：断言 panic 时也必须放开自旋段——否则 worker 线程
/// 滞留在无 await 的 poll 里，运行时拆除期等它 ⇒ 测试挂死（CI 上表现成 job 超时，
/// 比一条红更难诊断）。
struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

// 有界等待条件成立见 `src/test_support/guard.rs`（OS 时钟 + 进程级兜底，不依赖
// tokio 计时器）。

/// 等任务体终局可见（在途计数归零 **且** 终局记账已落）⇒ body-idle 断言可安全执行。
///
/// 两个条件**都**看：`in_flight` 由 FlightGuard 的 Drop 递减，`terminal` 记账由
/// SlotGuard 的 Drop 代记——两者的先后在语言层无保证 ⇒ 只等其中一个会让
/// `expect_body_idle()` 偶发假红（记账尚未落时 terminal < admitted）。
async fn wait_body_idle(scope: &FxScope) {
    wait_until(
        "任务体终局（在途归零 + 终局记账可见）",
        || scope.in_flight() == 0 && scope.counts().completed_unreaped == 1,
    )
    .await;
    scope.expect_body_idle();
}

/// 非让渡段闸门：任务体进入自旋段后不再 await ⇒ abort 只能等下一次让渡才生效。
///
/// `release` = 放行闸（测试→任务，走原子量：自旋段内不可 await，只能轮询）。
/// 「任务体已进入自旋段」的握手走 oneshot（await 侧挂起⇒运行时可驱动定时器）。
struct SpinGate {
    release: Arc<AtomicBool>,
    _release_on_drop: ReleaseOnDrop,
}

impl SpinGate {
    /// 造闸门 + 「任务体进入自旋段」的握手收发端。
    fn new() -> (Self, oneshot::Sender<()>, oneshot::Receiver<()>) {
        let (entered_tx, entered_rx) = oneshot::channel();
        let release = Arc::new(AtomicBool::new(false));
        let gate = Self {
            release: Arc::clone(&release),
            _release_on_drop: ReleaseOnDrop(Arc::clone(&release)),
        };
        (gate, entered_tx, entered_rx)
    }

    fn release(&self) {
        self.release.store(true, Ordering::Release);
    }
}

/// 造一条「停在非让渡自旋段」的任务：任务体进入自旋段后不再 await ⇒ abort 排不上
/// 生效，直到闸门放行。
///
/// 返回 (句柄, 闸门, 「已进入自旋段」握手接收端)——握手接收端 await 完即证明任务体
/// 确已进入自旋段（否则后续「超时返回时它仍在跑」的断言无意义）。
fn spawn_spinning_task(scope: &FxScope) -> (Spawned<i32>, SpinGate, oneshot::Receiver<()>) {
    let (gate, entered_tx, entered_rx) = SpinGate::new();
    let release = Arc::clone(&gate.release);
    let handle = scope
        .spawn_tracked(async move {
            // 握手：告知测试侧「我已进入自旋段」。发送失败 = 测试侧已弃听 ⇒ 显式 panic
            // （吞掉会让「握手其实没发生」静默通过）。
            entered_tx
                .send(())
                .expect("任务体须能把「已进入自旋段」信号送达测试侧");
            while !release.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            7
        })
        .expect("任务体须能入生命周期集");
    (handle, gate, entered_rx)
}

/// ①超时错误只申报「已请求 abort」，不申报「已终止」。
///
/// 旧文案「join timed out; task aborted」把**取消请求**说成**已终止**——tokio 的
/// abort 在任务体下一次让渡前不生效 ⇒ 该措辞强于实现保证（#67 的核心）。
#[test]
fn timed_out_reports_abort_request_not_confirmed_termination() {
    let text = TaskError::TimedOut.to_string();
    assert!(
        text.contains("abort requested"),
        "超时错误须申报「已请求 abort」：{text}"
    );
    assert!(
        !text.contains("task aborted"),
        "超时错误不得申报「任务已 abort」（abort 只是请求，终止未确认）：{text}"
    );
    assert!(
        text.contains("not confirmed"),
        "超时错误须明示终止未确认：{text}"
    );
}

/// ②任务体停在非让渡段时，超时返回瞬间任务体仍在执行（终止未确认）。
///
/// 这是 #67 的行为证据：旧文档承诺「超时路径不留活口」，而实现给不出该保证——
/// 任务体在自旋段里没有让渡点，abort 请求排不上生效。
///
/// 断言不恒真：`in_flight` 由 FlightGuard 的 Drop 递减，而该 Drop 只在任务体真正
/// 终止时才发生——任务体停在非让渡段 ⇒ 它**不可能**已经终止 ⇒ `in_flight > 0`
/// 恒真于本场景（反之，若 join_timeout 改成「超时后等终止确认再返回」，本场景的
/// 任务体永不让渡 ⇒ 该实现会挂死在 join_timeout 内，不会静默通过本断言）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_timeout_returns_while_task_body_is_still_running() {
    let scope = FxScope::new();
    let (handle, gate, entered_rx) = spawn_spinning_task(&scope);

    // 握手：任务体确已进入自旋段（否则下面的 in_flight 断言无意义）。
    entered_rx.await.expect("任务体须进入非让渡段");
    // 自旋段占住原 driver 持有者：kick 一台普通休眠 worker 重新 park（接手 driver
    // 处理时间轮），否则 50ms 截止永不到期（issue#102 机理）。
    kick_scheduler(
        "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
        FxScope::new()
            .spawn_detached(async {})
            .expect("一次性 kick scope 新开 ⇒ 闸必开"),
    )
    .await;

    let outcome = bounded(
        "join_timeout 有界返回",
        handle.join_timeout(Duration::from_millis(50)),
    )
    .await;
    assert_eq!(outcome, Err(TaskError::TimedOut));

    // 终止**未**确认：任务体仍在自旋段里跑。
    assert!(
        scope.in_flight() > 0,
        "超时返回时任务体仍可能在执行（abort 只是请求，不是终止）"
    );

    // 放行 ⇒ 任务体让渡 ⇒ 终局可见。
    gate.release();
    wait_body_idle(&scope).await;
}

/// ③句柄被消费不取消 scope 的生命周期跟踪：终局条目仍留集合，收割后才归零。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn consumed_handle_keeps_scope_lifecycle_tracking() {
    let scope = FxScope::new();
    let (handle, gate, entered_rx) = spawn_spinning_task(&scope);

    entered_rx.await.expect("任务体须进入非让渡段");
    // 自旋段占住原 driver 持有者：kick 一台普通休眠 worker 重新 park（接手 driver
    // 处理时间轮），否则 50ms 截止永不到期（issue#102 机理）。
    kick_scheduler(
        "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
        FxScope::new()
            .spawn_detached(async {})
            .expect("一次性 kick scope 新开 ⇒ 闸必开"),
    )
    .await;
    assert_eq!(
        bounded(
            "join_timeout 有界返回",
            handle.join_timeout(Duration::from_millis(50)),
        )
        .await,
        Err(TaskError::TimedOut)
    );

    gate.release();
    wait_body_idle(&scope).await;

    // 终局条目仍在生命周期集合里（未收割）⇒ scope 仍在跟踪这条任务。
    assert_eq!(
        scope.counts().completed_unreaped,
        1,
        "终局条目仍留集合（句柄被消费不取消 scope 的生命周期跟踪）"
    );

    // 收割后才归零（集合保留量归零 = 跟踪面收口）。
    //
    // 收割出的**载荷**故意不断言：abort 请求已在任务体自旋段中发出，放行后该条目
    // 落「cancelled」（JoinError）或「正常完成」（自旋段放行后返回值的竞态窗）两形
    // 都合法（tokio 内部竞窗）⇒ 断言载荷即 flaky。本测试断言的是**稳定的计数面**：
    // 收割动作发生 ⇒ completed_unreaped 归零 + 集合保留量归零。
    wait_until("集合条目收割（completed_unreaped 归零）", || {
        let _reaped = scope.try_reap();
        scope.counts().completed_unreaped == 0
    })
    .await;
    scope.expect_fully_reaped_idle();
}

/// ④期限内正常完成：结果直传，超时面不误报（既有语义不变）。
#[tokio::test]
async fn completion_before_deadline_returns_value() {
    let scope = FxScope::new();
    let handle = scope.spawn_tracked(async { 42 }).unwrap();
    assert_eq!(
        handle.join_timeout(Duration::from_secs(5)).await,
        Ok(42),
        "期限内完成 ⇒ 结果直传（不落 TimedOut）"
    );
    wait_body_idle(&scope).await;
}

/// ⑤abort 发生在首次 poll 之前：join 侧见 Cancelled，任务体从未执行（既有语义不变）。
#[tokio::test]
async fn abort_before_first_poll_yields_cancelled() {
    let scope = FxScope::new();
    let ran = Arc::new(AtomicBool::new(false));
    let task_ran = Arc::clone(&ran);
    let handle = scope
        .spawn_tracked(async move {
            task_ran.store(true, Ordering::SeqCst);
            1
        })
        .unwrap();

    // current_thread 运行时：任务体要等当前任务让渡才开跑 ⇒ 此处 abort 必在首次 poll 之前。
    handle.abort();
    assert_eq!(handle.join().await, Err(TaskError::Cancelled));
    assert!(
        !ran.load(Ordering::SeqCst),
        "首次 poll 前 abort ⇒ 任务体从未执行"
    );
    wait_body_idle(&scope).await;
}
