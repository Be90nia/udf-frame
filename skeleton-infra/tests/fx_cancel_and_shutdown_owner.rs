#![cfg(feature = "tokio")]
//! #68 / #70：取消态的粘性存储 + FxScope 并发停机的共享排空语义。
//!
//! #68（取消跨「无接收者窗口」留存）：tokio `watch::Sender::send` 在**没有接收者**
//! 时不写入值 ⇒ 取消会静默丢失，之后 `with_source` 订阅到的是 `false`。本文件钉
//! 「丢弃全部接收者 → cancel → 之后接入 scope 仍观察到 true」，以及既有唤醒/幂等。
//!
//! #70（并发停机的共享排空）：`shutdown` 的集合 take-ownership 会移走 JoinSet，
//! 并发到达的第二个调用者曾把「空集」误读成「整 scope 停机完成」，返回空报告。
//! 本文件钉：等待者共享 owner 的 canonical 报告 / 截止更短的等待者只约束自己的等待
//! 而不改写 owner 策略 / owner future 被 drop 时所有等待者以文档化的中断结果醒来。
//!
//! 确定性构造：任务体进入**无 await 的自旋段**（握手确认入段）⇒ abort 只在下一次
//! 让渡点生效；等待一律「有界超时 + 让渡」，禁 sleep。

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use skeleton_infra::fx::{CancelToken, FxScope, cancel_pair};

#[path = "common/guards.rs"]
mod guards;

use guards::{bounded, wait_until};

/// 把 future 轮询**一次**：`biased` 保证先 poll 目标、再落到让渡分支。
///
/// 返回 `Some(output)` = 目标已就绪；`None` = 目标被 poll 过一次后挂起。用它把
/// 「owner 已 take 集合并挂在 join_next / 等待者已挂进共享等待」变成确定性事实，
/// 免去 sleep 或调度假设。
async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Option<F::Output> {
    tokio::select! {
        biased;
        out = &mut *fut => Some(out),
        _ = tokio::task::yield_now() => None,
    }
}

/// 自旋段放行闸的 RAII 兜底：断言 panic 时也必须放开自旋段——否则 worker 线程
/// 滞留在无 await 的 poll 里，运行时拆除期等它 ⇒ 测试挂死（比一条红更难诊断）。
struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// 停在**非让渡自旋段**的任务：入段后不再 await ⇒ abort 只能等下一次让渡生效。
///
/// 放行后循环回到 `yield_now().await` ⇒ 已请求的 abort 确定生效（条目必落
/// `cancelled` 桶，不与「已完成」赛跑）。
struct Spinner {
    hold: Arc<AtomicBool>,
    entered: Arc<AtomicBool>,
    _release_on_drop: ReleaseOnDrop,
}

impl Spinner {
    /// 握手：任务体确已进入自旋段（此后 abort 不会即时生效）。
    async fn enter(&self) {
        self.hold.store(true, Ordering::Release);
        let entered = Arc::clone(&self.entered);
        wait_until("任务体进入非让渡自旋段", || {
            entered.load(Ordering::Acquire)
        })
        .await;
    }

    fn release(&self) {
        self.hold.store(false, Ordering::Release);
    }
}

/// 造一条自旋任务（句柄即刻丢弃——`Spawned` Drop 不 abort，任务归生命周期集托管）；
/// `drop_probe` 随任务 future 一起析构（用户析构探针）。
fn spawn_spinner_with(scope: &FxScope, drop_probe: Option<ObserveOnDrop>) -> Spinner {
    let hold = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let (hold_task, entered_task) = (Arc::clone(&hold), Arc::clone(&entered));
    let handle = scope
        .spawn_tracked(async move {
            let _probe = drop_probe;
            loop {
                if hold_task.load(Ordering::Acquire) {
                    entered_task.store(true, Ordering::Release);
                    while hold_task.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    entered_task.store(false, Ordering::Release);
                }
                tokio::task::yield_now().await; // 唯一 abort 点
            }
        })
        .expect("自旋任务须能入生命周期集");
    drop(handle);
    Spinner {
        _release_on_drop: ReleaseOnDrop(Arc::clone(&hold)),
        hold,
        entered,
    }
}

/// 造一条无析构探针的自旋任务。
fn spawn_spinner(scope: &FxScope) -> Spinner {
    spawn_spinner_with(scope, None)
}

/// 用户析构探针：任务 future 被 abort 时在**用户析构**里重入 scope 观测面。
///
/// 若排空路径在持有 scope 锁（生命周期集 / 计数面）时跑用户析构，这里会自锁 ——
/// 测试以有界等待暴露成红，而不是静默挂死。
struct ObserveOnDrop {
    scope: Arc<FxScope>,
    observed: Arc<AtomicBool>,
}

impl Drop for ObserveOnDrop {
    fn drop(&mut self) {
        let _ = self.scope.counts();
        let _ = self.scope.in_flight();
        let _ = self.scope.try_reap();
        self.observed.store(true, Ordering::Release);
    }
}

// ---------------------------------------------------------------------------
// #68：取消态跨无接收者窗口留存
// ---------------------------------------------------------------------------

/// #68①：唯一接收者先被丢弃 → cancel → 之后接一个 scope ⇒ source 与新 token
/// **都**观察到 true（取消状态存进 watch 值本身，不依赖接收者在场）。
#[tokio::test]
async fn cancel_survives_receiver_free_interval() {
    let (source, token) = cancel_pair();
    drop(token);
    source.cancel();
    assert!(
        source.is_cancelled(),
        "#68：发送侧读侧必须看到自己设下的取消态（无接收者时 send 不写值）"
    );
    let scope = FxScope::new().with_source(source);
    assert!(
        scope.token().is_cancelled(),
        "#68：取消之后才订阅的 scope token 必须观察到 true"
    );
}

/// #68②：多个接收者克隆全部丢弃后同样留存；重复 cancel 幂等（单向 false→true，
/// 不回落、不发散）。
#[tokio::test]
async fn cancel_survives_all_clones_dropped_and_stays_idempotent() {
    let (source, token) = cancel_pair();
    let clones: Vec<CancelToken> = (0..3).map(|_| token.clone()).collect();
    drop(clones);
    drop(token);

    source.cancel();
    source.cancel(); // 幂等：重复 cancel 不产生状态回退
    assert!(source.is_cancelled(), "#68：无接收者时取消态必须被存下");

    let scope = FxScope::new().with_source(source);
    assert!(
        scope.token().is_cancelled(),
        "#68：全部克隆丢弃后取消仍粘性"
    );
}

/// #68③：有活跃等待者时 cancel 立即唤醒它（既有唤醒语义不变）。
#[tokio::test(start_paused = true)]
async fn cancel_wakes_parked_waiter() {
    let (source, token) = cancel_pair();
    let scope = FxScope::new();
    let waiter = token.clone();
    // 走脱管口（本 crate 对 `tokio::spawn` 的唯一承载点）——等待任务只需等取消。
    let task = scope
        .spawn_detached(async move { waiter.cancelled().await })
        .expect("scope 未关闸 ⇒ 脱管 spawn 可用");
    // 单线程 runtime：让渡一次即轮到等待任务跑到 wait_for 的挂起点。
    tokio::task::yield_now().await;
    assert!(!source.is_cancelled());

    source.cancel();
    let woken = bounded("等待者被 cancel 唤醒", task).await;
    woken.expect("等待任务不得 panic");
    assert!(token.is_cancelled());
}

// ---------------------------------------------------------------------------
// #70：并发停机的共享排空语义
// ---------------------------------------------------------------------------

/// #70①：排空期间的第二个调用者必须等到**共享**排空结果，不得把「集合已被 owner
/// 取走」误读成「scope 停机已完成」。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_waiter_shares_canonical_report() {
    let scope = FxScope::new();
    let spinner = spawn_spinner(&scope);
    spinner.enter().await;

    // owner 首 poll：close + abort_all + take 完成，join_next 挂在自旋任务上。
    let mut owner = Box::pin(scope.shutdown());
    let early = poll_once(&mut owner).await;
    assert!(
        early.is_none(),
        "自旋段未放行前 owner 不得完成排空（实得 {early:?}）"
    );

    // 等待者首 poll：必须停在共享等待上。
    let mut waiter = Box::pin(scope.shutdown());
    let early = poll_once(&mut waiter).await;
    assert!(
        early.is_none(),
        "#70：排空进行中，并发调用者不得先返回报告（实得 {early:?}）"
    );

    spinner.release();
    let owner_report = owner.await;
    let waiter_report = waiter.await;
    assert_eq!(
        owner_report.cancelled, 1,
        "自旋段放行后 abort 确定生效 ⇒ 条目落 cancelled"
    );
    assert_eq!(
        waiter_report, owner_report,
        "#70：等待者共享 owner 的 canonical 报告，而不是空报告"
    );
}

/// #70②：截止更短的等待者只约束**自己的等待**——按自己的截止返回自己的读数，
/// 既不取消也不改写 owner 的排空策略。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shorter_deadline_waiter_does_not_rewrite_owner_policy() {
    let scope = FxScope::new();
    let spinner = spawn_spinner(&scope);
    spinner.enter().await;

    let mut owner = Box::pin(scope.shutdown());
    let early = poll_once(&mut owner).await;
    assert!(early.is_none(), "owner 必须停在自旋任务的 join_next 上");

    // 等待者自己的（更短）截止到期：只停止自己的等待。
    let waiter_report = scope.shutdown_timeout(Duration::from_millis(50)).await;
    assert_eq!(
        waiter_report.timed_out, 1,
        "#70：等待者按自己的截止如实申报仍在排空中的条数"
    );
    assert_eq!(
        waiter_report.still_running, 1,
        "终止未确认 ⇒ still_running 申报，不得记成已完成/已终止"
    );
    assert_eq!(waiter_report.cancelled, 0, "等待者不得声称任务已终止");

    // owner 策略未被改写：放行后它照样等净并给出 canonical 报告。
    spinner.release();
    let owner_report = owner.await;
    assert_eq!(owner_report.cancelled, 1);
    assert_eq!(
        owner_report.timed_out, 0,
        "#70：owner 无截止 ⇒ 不得继承等待者的截止"
    );
    assert_eq!(owner_report.still_running, 0);
}

/// #70③：排空 owner future 被 drop（select! 竞败/超时分支弃置的常态）⇒ **所有**
/// 等待者以文档化的中断结果醒来（有界，不得停在 Draining 永等）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_drain_owner_wakes_all_waiters_with_interrupted_outcome() {
    let scope = Arc::new(FxScope::new());
    let probe_ran = Arc::new(AtomicBool::new(false));
    let spinner = spawn_spinner_with(
        &scope,
        Some(ObserveOnDrop {
            scope: Arc::clone(&scope),
            observed: Arc::clone(&probe_ran),
        }),
    );
    spinner.enter().await;

    let mut owner = Box::pin(scope.shutdown());
    let early = poll_once(&mut owner).await;
    assert!(early.is_none(), "owner 必须停在对自旋任务的 join_next 上");
    let mut first = Box::pin(scope.shutdown());
    let early = poll_once(&mut first).await;
    assert!(early.is_none(), "等待者不得在 owner 结算前返回");
    let mut second = Box::pin(scope.shutdown());
    let early = poll_once(&mut second).await;
    assert!(early.is_none(), "等待者不得在 owner 结算前返回");

    drop(owner); // 排空 owner 被弃置：其 JoinSet drop 只「请求」abort

    let first_report = bounded("#70：owner 被 drop 后等待者必须有界醒来", first).await;
    let second_report = bounded("#70：owner 被 drop 后等待者必须有界醒来", second).await;
    assert_eq!(first_report, second_report, "所有等待者看到同一份中断结果");
    assert!(
        first_report.drain_interrupted,
        "#70：owner 被 drop ⇒ 必须发布**独立**的中断结果（不得伪装成 canonical 终局）"
    );
    assert_eq!(
        first_report.cancelled + first_report.completed,
        0,
        "中断时不得把未确认终止的任务记成已取消/已完成"
    );
    assert_eq!(
        first_report.still_running, 1,
        "#70：中断结果含中断时已知的条数（终止未确认）"
    );
    assert_eq!(
        first_report.timed_out, 0,
        "中断不是截止事件（不得借用 timed_out 表达）"
    );

    // 中断只**请求** abort（报告不声称已终止）：放行后任务真正退出，在途归零。
    spinner.release();
    wait_until("中断后 abort 生效（任务真正退出）", || {
        scope.in_flight() == 0
    })
    .await;
    // 用户析构（被 abort 的任务 future）里重入 scope 观测面不得自锁——锁内禁跑
    // 用户析构；探针置位即证该路径已在锁外跑完。
    wait_until("用户析构探针执行完毕", || {
        probe_ran.load(Ordering::Acquire)
    })
    .await;

    // 中断不停在 Draining：后续调用走「已结算」顺序语义，但**返回该次排空的
    // canonical 报告副本**（审计 F3：旧实现返回空默认报告 ⇒ 并发停机时 panic
    // 载荷被静默丢；此处仍须可重试、不挂死）。
    assert_eq!(
        scope.shutdown().await,
        first_report,
        "已结算的排空：后续调用返回该次排空的 canonical 报告副本（不得静默降级为空报告）"
    );
}

/// #70④：超时 owner 的**截止期报告**就是等待者共享的 canonical 结果（等待者拿到
/// owner 的 timed_out/still_running，而不是空报告）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_owner_publishes_deadline_report_to_waiters() {
    let scope = FxScope::new();
    let spinner = spawn_spinner(&scope);
    spinner.enter().await;

    let mut owner = Box::pin(scope.shutdown_timeout(Duration::from_millis(50)));
    let early = poll_once(&mut owner).await;
    assert!(early.is_none(), "自旋段未放行 ⇒ owner 必在截止处返回");
    let mut waiter = Box::pin(scope.shutdown());
    let early = poll_once(&mut waiter).await;
    assert!(early.is_none(), "等待者不得在 owner 结算前返回");

    let owner_report = owner.await;
    assert_eq!(owner_report.timed_out, 1, "owner 的截止期报告");
    let waiter_report = waiter.await;
    assert_eq!(
        waiter_report, owner_report,
        "#70：等待者拿 owner 的截止期报告（canonical），而非空报告"
    );

    spinner.release();
}
