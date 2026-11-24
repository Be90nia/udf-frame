#![cfg(feature = "tokio")]
//! 停机报告契约（蓝图 §4 / §11 第 3 项）的生命周期回归。
//!
//! 报告五个终局桶互斥且穷尽：`completed` / `cancelled` / `panicked` / `failed` /
//! `still_running` 中每任务恰落一桶；`timed_out` 是**截止事件**的计数（调用方停止
//! 等待），`still_running` 是**终止未观测**的计数——二者都不宣称工作已终止。
//!
//! 确定性分工：deadline 路径只放「终局类与 abort 无竞态」的任务（不可让渡自旋 /
//! 长期存活）；正常完成与 panic 的逐类落账走无 deadline 路径——`close_and_take` 的
//! abort 与 tokio「poll 返回 Ready → 标记 COMPLETE」之间存在内部竞窗，CPU 抢占即可
//! 命中（详见 `timeout_reports_survivors_without_claiming_termination` 的文档）。
//!
//! 另钉三条既有语义：close ≠ cancel（闸关不连带取消在途）、close/cancel/shutdown
//! 幂等可重入、无超时路径在任务全完成时 `timed_out == 0`。

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use skeleton_infra::fx::{FxScope, ShutdownReport, TaskError, cancel_pair};

#[path = "../src/test_support/guard.rs"]
mod guards;

use guards::{bounded, kick_scheduler, wait_until};

/// 自旋段放行闸的 RAII 兜底：断言 panic 时也必须放开自旋段——否则 worker 线程
/// 滞留在无 await 的 poll 里，运行时拆除期等它 ⇒ 测试挂死（CI 上表现成 job 超时，
/// 比一条红更难诊断）。
struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

// 有界等待条件 / future 完成见 `src/test_support/guard.rs`（OS 时钟 + 进程级兜底，
// 不依赖 tokio 计时器）。

/// 正常路径（e2e 对照跑法②）：任务在期限内全部退出 → `completed` 计满、
/// `timed_out` / `still_running` 归零。current_thread 运行时保证 join 返回时条目
/// 终局已落定（无跨线程窗口）。
#[tokio::test]
async fn shutdown_counts_completed_and_reports_no_timeout() {
    let scope = FxScope::new();
    let mut handles = Vec::new();
    for i in 0..5u32 {
        handles.push(scope.spawn_tracked(async move { i * 2 }).unwrap());
    }
    for handle in handles {
        bounded("托管任务 join 完成", handle.join())
            .await
            .expect("task ok");
    }
    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    println!("all-complete report: {report:?}");
    assert_eq!(report.completed, 5, "任务全完成 ⇒ completed 计满");
    assert_eq!(report.cancelled, 0);
    assert!(report.panicked.is_empty());
    assert_eq!(report.failed, 0);
    assert_eq!(report.timed_out, 0, "无超时路径恒 0");
    assert_eq!(report.still_running, 0, "排空至空集 ⇒ 无未观测终局");
}

/// 超时路径（deadline 面）：只放**终局类与 abort 无竞态**的任务——不可让渡自旋段
/// （abort 只能等下一 await）与长期存活任务（只会被 abort）——故断言与 CPU 负载
/// 无关：`timed_out` 是截止事件计数、`still_running` 是终止未观测计数，二者都不得
/// 把幸存者记成 `completed`。
///
/// 为什么「正常完成 / panic」不在这里：`close_and_take` 的 abort 与「任务 poll 返回
/// Ready → tokio 标记 COMPLETE」之间有内部竞窗（cancel 落进该窗即丢输出、条目记
/// cancelled），CPU 抢占即可命中——那是时序事实，不是可断言的语义。逐类落账改由
/// `mixed_outcomes_classified_without_deadline` 在无 deadline 路径上确定化钉住。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_reports_survivors_without_claiming_termination() {
    let started = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(AtomicBool::new(false));
    let holding = Arc::new(AtomicBool::new(false));
    // panic 路径也放开自旋段：否则 worker 在无 await 的 poll 里滞留，运行时拆除
    // 期等它 ⇒ 测试挂死（CI 上表现为 job 超时）。
    let _release = ReleaseOnDrop(Arc::clone(&hold));
    let scope = FxScope::new();

    // ① 两条长期存活：入口先发「已进等待」信号再睡——启动是可判定事实，不靠时间
    // 窗猜（abort 后要么落 cancelled、要么未收割进 still_running，都是确定类）。
    for _ in 0..2 {
        let started_task = Arc::clone(&started);
        scope
            .spawn_tracked(async move {
                started_task.fetch_add(1, Ordering::Release);
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .unwrap();
    }

    // ② 一条不可让渡自旋段：abort 只在下一次 await 生效 ⇒ 超时必达。
    let (hold_task, holding_task) = (Arc::clone(&hold), Arc::clone(&holding));
    let spinner = scope
        .spawn_tracked(async move {
            loop {
                if hold_task.load(Ordering::Relaxed) {
                    holding_task.store(true, Ordering::Relaxed);
                    while hold_task.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                    holding_task.store(false, Ordering::Relaxed);
                }
                tokio::task::yield_now().await;
            }
        })
        .unwrap();

    // 握手（有界等待，不变量被破坏时诊断失败而非挂死）：长期存活任务已真的进入
    // 等待、自旋任务已滞留在无 await 的 poll 段内。
    wait_until("两条长期存活任务已进入等待", || {
        started.load(Ordering::Acquire) >= 2
    })
    .await;
    hold.store(true, Ordering::Relaxed);
    wait_until("自旋任务已进入不可让渡段", || {
        holding.load(Ordering::Relaxed)
    })
    .await;
    // 自旋段占住原 driver 持有者：kick 一台普通休眠 worker 重新 park（接手 driver
    // 处理时间轮），否则 50ms 截止永不到期（issue#102 机理）。
    kick_scheduler(
        "唤醒在 driver 上 park 的 worker（自旋段占住原 driver 持有者）",
        FxScope::new()
            .spawn_detached(async {})
            .expect("一次性 kick scope 新开 ⇒ 闸必开"),
    )
    .await;

    scope.close(); // 闸关：拒新 spawn，不取消在途
    assert!(!scope.token().is_cancelled(), "close 不是 cancel");
    let waited = Instant::now();
    let report = bounded(
        "shutdown_timeout 有界返回",
        scope.shutdown_timeout(Duration::from_millis(50)),
    )
    .await;
    println!("timeout report: {report:?}");

    assert!(
        waited.elapsed() >= Duration::from_millis(50),
        "超时必达：提前返回即守卫失效"
    );
    // 终局面：幸存者不得被记成已完成 / 已取消。
    assert_eq!(report.completed, 0, "超时路径不得把未观测终止记成已完成");
    assert!(report.panicked.is_empty(), "本场景无 panic 终局");
    assert_eq!(report.failed, 0, "本场景无非 panic 失败终局");
    // deadline 面：截止时未完成 ≥ 自旋段；报告返回时终止未观测 ≥ 自旋段。
    assert!(report.timed_out >= 1, "自旋段把截止推过期限");
    assert!(
        report.still_running >= 1,
        "截止时未终局的任务进 still_running"
    );
    assert!(
        report.still_running <= report.timed_out,
        "still_running 只减不增：截止后首轮收割可减少、不会新增"
    );
    assert_eq!(
        report.completed
            + report.cancelled
            + report.panicked.len()
            + report.failed
            + report.still_running,
        3,
        "桶互斥且穷尽：每条任务恰落一桶"
    );
    assert!(
        !spinner.is_finished(),
        "报告不宣称超时任务已终止：自旋任务此刻仍在跑"
    );

    hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一 await 点生效
    assert_eq!(
        bounded("超时遗留任务被 abort 收割", spinner.join()).await,
        Err(TaskError::Cancelled),
        "超时遗留任务由 abort 在下一 await 点收割"
    );
}
/// 混合场景（无 deadline 路径，逐类落账确定化）：正常完成 / panic / 长期存活三类
/// 各落各桶。`shutdown()` 等净至空集 ⇒ `timed_out` / `still_running` 恒 0，且
/// current_thread 调度下单线程「poll 返回 Ready ⇒ 条目终局」先于测试线程恢复，
/// 故每条任务的终局都是已确定事实，无需与 abort 赛跑。
///
/// 与 [`timeout_reports_survivors_without_claiming_termination`] 分工：deadline
/// 路径只钉幸存者面（不可让渡自旋 + 长期存活），逐类落账钉在本测试——multi_thread
/// 上让「已完成 / panic」与 abort 赛跑属 tokio 内部竞窗（cancel 落进 poll 完成窗
/// 即丢输出、条目记 cancelled），CPU 抢占即可命中，不是可断言的语义。
#[tokio::test]
async fn mixed_outcomes_classified_without_deadline() {
    let scope = FxScope::new();

    // ① 两条正常完成。
    let mut quick = Vec::new();
    for i in 0..2u8 {
        quick.push(scope.spawn_tracked(async move { i }).unwrap());
    }
    // ② 一条 panic（句柄不丢：载荷先经 join 面证一次，再由报告面落账）。
    let boom = scope.spawn_tracked(async { panic!("boom-mixed") }).unwrap();
    // ③ 两条长期存活（只会被 abort ⇒ 必落 cancelled）。
    for _ in 0..2 {
        scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .unwrap();
    }

    for (expected, handle) in quick.into_iter().enumerate() {
        assert_eq!(
            bounded("正常任务 join 收值", handle.join()).await,
            Ok(expected as u8),
            "正常任务 join 收值"
        );
    }
    assert_eq!(
        bounded("panic 任务 join 收割", boom.join()).await,
        Err(TaskError::Panicked("boom-mixed".to_owned())),
        "panic 载荷经 join 面可见"
    );

    scope.close();
    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    println!("mixed report (no deadline): {report:?}");
    assert_eq!(report.completed, 2, "正常完成的任务进 completed");
    assert_eq!(
        report.panicked,
        vec!["boom-mixed".to_owned()],
        "panic 载荷进 panicked"
    );
    assert_eq!(report.cancelled, 2, "长期存活任务被 abort ⇒ cancelled");
    assert_eq!(report.failed, 0, "本场景无失败的终局");
    assert_eq!(report.timed_out, 0, "无 deadline 路径恒 0");
    assert_eq!(report.still_running, 0, "排空至空集 ⇒ 无未观测终局");
    assert_eq!(
        report.completed
            + report.cancelled
            + report.panicked.len()
            + report.failed
            + report.still_running,
        5,
        "桶互斥且穷尽：每任务恰落一桶"
    );
}

/// 幂等：`close()` / `cancel()` / `shutdown()`（含超时形）重复调用安全；已结算的
/// 重复调用返回**该次排空的 canonical 报告副本**（审计 F3；旧实现返回空默认报告），
/// 且 `ShutdownReport::default()` 仍可用（向后兼容面）。
#[tokio::test]
async fn close_cancel_and_shutdown_are_idempotent() {
    // cancel 幂等（观察面恒 true，重复取消不 panic 不改语义）。
    let (source, token) = cancel_pair();
    source.cancel();
    source.cancel();
    assert!(token.is_cancelled(), "重复 cancel 幂等");
    assert!(source.is_cancelled());

    let scope = FxScope::new().with_source(source);
    let handle = scope.spawn_tracked(async { 7u8 }).unwrap();
    scope.close();
    scope.close(); // 幂等
    assert!(scope.is_closed());
    assert!(
        scope.spawn_tracked(async { 8u8 }).is_err(),
        "闸关后拒新 spawn"
    );
    assert_eq!(
        bounded("close 不取消在途任务", handle.join()).await,
        Ok(7),
        "close 不取消在途任务"
    );

    let first = bounded("首次 shutdown 返回", scope.shutdown()).await;
    assert_eq!(first.completed, 1, "在途任务已终局 ⇒ 报告可见");
    assert_eq!(first.timed_out, 0);
    // 审计 F3 后的语义：已结算的重复调用返回**该次排空的 canonical 报告副本**
    // （旧实现返回空默认报告 ⇒ 并发停机时 panic 载荷静默丢）。
    let second = bounded("二次 shutdown 返回", scope.shutdown()).await;
    assert_eq!(second, first, "二次 shutdown 返回 canonical 报告副本");
    let third = bounded(
        "空集超时路径返回",
        scope.shutdown_timeout(Duration::from_millis(50)),
    )
    .await;
    assert_eq!(third.completed, 1, "已结算路径不得声称超时或丢失终局桶");
    assert_eq!(third.timed_out, 0, "已结算路径不声称超时");
}

/// close ≠ cancel：闸关后在途任务照常跑到完成（既有语义，用测试钉死）。
#[tokio::test(start_paused = true)]
async fn close_gate_does_not_cancel_in_flight_work() {
    let gate = Arc::new(AtomicBool::new(false));
    let gate_task = Arc::clone(&gate);
    let scope = FxScope::new();
    let handle = scope
        .spawn_tracked(async move {
            while !gate_task.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            9u8
        })
        .unwrap();

    scope.close();
    assert!(scope.is_closed());
    assert!(!scope.token().is_cancelled(), "close 不等于 cancel");

    gate.store(true, Ordering::Release);
    assert_eq!(
        bounded("close 后任务照常完成", handle.join()).await,
        Ok(9),
        "close 后任务照常完成"
    );
    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    assert_eq!(report.completed, 1);
    assert_eq!(report.cancelled, 0, "不得把在途任务记成已取消");
}

/// 交叉面（issue#39 F3）：`shutdown_timeout` 遇**非空已完成集**——排空前已完成
/// 的条目（滞留集合、未收割）与一条不可让渡自旋段（终止未观测）同时在场。
///
/// 两侧各落各桶：已完成者进 `completed`（由停机排空面收割，**不**依赖
/// `try_reap`）；幸存者进 `timed_out` / `still_running`（终止未观测，**不得**被
/// 记成已完成）。停机前的可观测面：已完成未收割条目数 > 0（`completed_unreaped`
/// ——`join` 只消费结果、**不**移除集合条目）。
///
/// 确定性分工：已完成的两条在停机前**已彻底终结**（future 已析构 ⇒ 运行时已完成
/// COMPLETE 记账），故 `close_and_take` 的 abort 打不到它们 ⇒ 必落 `completed`
/// （与 `timeout_reports_survivors_without_claiming_termination` 里「终局类与
/// abort 无竞态」的分工同源）；幸存者用不可让渡自旋段（abort 只在下一次 await
/// 生效）⇒ 超时必达。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_timeout_with_non_empty_completed_set_classifies_both_faces() {
    let hold = Arc::new(AtomicBool::new(false));
    let holding = Arc::new(AtomicBool::new(false));
    // panic 路径也放开自旋段：否则 worker 在无 await 的 poll 里滞留 ⇒ 运行时拆除
    // 期等它 ⇒ 测试挂死（CI 上表现为 job 超时）。
    let _release = ReleaseOnDrop(Arc::clone(&hold));
    let scope = FxScope::new();

    // ① 两条已完成任务：条目滞留集合 ⇒ 非空已完成集。
    let mut quick = Vec::new();
    for i in 0..2u8 {
        quick.push(scope.spawn_tracked(async move { i }).unwrap());
    }
    for (expected, handle) in quick.into_iter().enumerate() {
        assert_eq!(
            bounded("已完成任务 join 收值", handle.join()).await,
            Ok(expected as u8),
            "已完成任务 join 收值"
        );
    }
    // 任务体已终结（future 已析构 ⇒ 运行时完成 COMPLETE 记账，abort 打不到）。
    wait_until("已完成任务体终结", || scope.in_flight() == 0).await;
    // 已完成未收割条目可观测（join 不移除集合条目 ⇒ 保留量计数不因 join 而减）。
    wait_until("已完成条目滞留集合", || {
        scope.counts().completed_unreaped == 2
    })
    .await;

    // ② 一条不可让渡自旋段：abort 只在下一次 await 生效 ⇒ 超时必达。
    let (hold_task, holding_task) = (Arc::clone(&hold), Arc::clone(&holding));
    let spinner = scope
        .spawn_tracked(async move {
            loop {
                if hold_task.load(Ordering::Relaxed) {
                    holding_task.store(true, Ordering::Relaxed);
                    while hold_task.load(Ordering::Relaxed) {
                        std::hint::spin_loop();
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .unwrap();

    // 握手（有界等待，不变量被破坏时诊断失败而非挂死）：自旋任务已滞留在无
    // await 的 poll 段内（免运行即撤模拟不了该场景）。
    hold.store(true, Ordering::Relaxed);
    wait_until("自旋任务已进入不可让渡段", || {
        holding.load(Ordering::Relaxed)
    })
    .await;
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
    println!("timeout report with non-empty completed set: {report:?}");

    assert_eq!(
        report.completed, 2,
        "停机前已完成的条目进 completed（排空面收割，不依赖 try_reap）"
    );
    assert_eq!(report.cancelled, 0, "已完成条目不得被误记为取消");
    assert!(report.panicked.is_empty(), "本场景无 panic 终局");
    assert_eq!(report.failed, 0, "本场景无非 panic 失败终局");
    assert_eq!(report.timed_out, 1, "截止时未完成 = 不可让渡自旋段");
    assert_eq!(
        report.still_running, 1,
        "终止未观测 = 自旋段（不宣称已终止）"
    );
    assert_eq!(
        report.completed
            + report.cancelled
            + report.panicked.len()
            + report.failed
            + report.still_running,
        3,
        "桶互斥且穷尽：每条任务恰落一桶"
    );

    hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一 await 点生效
    assert_eq!(
        bounded("超时遗留任务被 abort 收割", spinner.join()).await,
        Err(TaskError::Cancelled),
        "超时遗留任务由 abort 在下一 await 点收割"
    );
}

// ── #94「设计验证」─ current-thread 限制证明 / multi-thread 观察证明 ─────────────

/// 把 future 轮询**一次**：`Some(v)` = 已就绪；`None` = 已被 poll 一次后挂起。
///
/// 用它把「50ms 截止已在时间轮上登记」「排空权已发布」变成确定性事实，免去任何
/// 调度顺序假设（首次 poll 序 = spawn 序只是 tokio 的实现巧合，不是契约）。
async fn poll_once<F: Future + Unpin>(fut: &mut F) -> Option<F::Output> {
    std::future::poll_fn(|cx| match Pin::new(&mut *fut).poll(cx) {
        Poll::Ready(value) => Poll::Ready(Some(value)),
        Poll::Pending => Poll::Ready(None),
    })
    .await
}

/// #94 设计验证·multi-thread 观察证明（构造 A：被阻塞任务首次 poll 即进不可让渡段，
/// 无 yield 握手）。
///
/// 一个 worker 被**同步阻塞**占用时，另一个 worker 上的 shutdown timer 仍按期被观察：
/// `timed_out` 记截止时未完成数、`still_running` 记终止未观测数，且被阻塞任务**不得**
/// 被标成已终止。两 worker 下阻塞者占用其一、测试任务占用另一（它 park 时即 driver
/// 持有者）⇒ 时间轮照常推进，故本证明不需要 kick。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_worker_observes_shutdown_deadline_while_one_worker_is_blocked() {
    let scope = FxScope::new();
    let entered = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    // 断言 panic 也必须放开同步段，否则 worker 滞留在无 await 的 poll 里。
    let _release_guard = ReleaseOnDrop(Arc::clone(&release));
    let (entered_task, release_task) = (Arc::clone(&entered), Arc::clone(&release));
    let blocked = scope
        .spawn_tracked(async move {
            entered_task.store(true, Ordering::Release); // 首次 poll 即入同步段
            while !release_task.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            tokio::task::yield_now().await; // 放行后的 abort 点
        })
        .unwrap();

    wait_until("阻塞任务已进入同步段", || {
        entered.load(Ordering::Acquire)
    })
    .await;

    let started = Instant::now();
    let report = bounded(
        "shutdown_timeout 有界返回",
        scope.shutdown_timeout(Duration::from_millis(50)),
    )
    .await;
    let elapsed = started.elapsed();
    println!("multi-worker blocked report: {report:?}（实钟 {elapsed:?}）");

    assert_eq!(report.timed_out, 1, "截止时未完成 = 被阻塞任务");
    assert_eq!(report.still_running, 1, "终止未观测 = 被阻塞任务");
    assert_eq!(report.completed, 0, "阻塞任务不得被记成已完成");
    assert_eq!(report.cancelled, 0, "阻塞任务不得被记成已取消");
    assert!(
        !blocked.is_finished(),
        "报告不宣称阻塞任务已终止（abort 只能等下一次让渡）"
    );
    assert!(
        elapsed >= Duration::from_millis(50),
        "按期观察：截止不得提前返回（实钟 {elapsed:?}）"
    );

    release.store(true, Ordering::Release); // 放行 ⇒ abort 在下一让渡点生效
    assert_eq!(
        bounded("被阻塞任务被 abort 收割", blocked.join()).await,
        Err(TaskError::Cancelled),
        "放行后被 abort 收割"
    );
    wait_until("in_flight 归零", || scope.in_flight() == 0).await;
    scope.expect_body_idle();
}

/// #94 设计验证·current-thread 限制证明：受控 Future 在 poll 中通知外部线程后**同步
/// 阻塞**唯一 executor 线程 ⇒ 同一 runtime 的 timer / shutdown waiter 在阻塞期间无法
/// 前进；外部线程放行后 executor 恢复并观察到**已过期**的 timeout（迟于截止 ≥ 300ms）。
///
/// 确定性构造（不依赖「首次 poll 序 = spawn 序」这类实现巧合）：
/// - **T**（托管）= `pending`：排空只能停在它上面；
/// - **owner**（本测试持有）= `scope.shutdown()` 首 poll 后被**冻结**（此后不再 poll）
///   ⇒ 排空权已发布 `Draining`、排空永不结算 ⇒ 后续 `shutdown_timeout` 走**等待者**
///   分支（自带 50ms tokio 截止）；
/// - **W**（非托管）= `shutdown_timeout(50ms)`：体内用 [`poll_once`] 把内部 timeout
///   首 poll 掉（截止即刻登记在时间轮上）后才置 `w_registered` ⇒ 主测试看到该位即
///   确定「截止已在时间轮上」；
/// - **B**（非托管，受控 Future）= 受控阻塞：poll#1 发 `started` 后等 `go`；poll#2 发
///   `entered` 后在 std channel 上**同步阻塞**（占死唯一 executor 线程），放行即返回。
///   B 必须不托管：托管任务的 `close+abort_all` 会把尚未 poll 的任务「免运行即撤」。
///
/// 为何不断言 `report.timed_out == 1`：current_thread 上不可能有「截止时仍未完成且
/// 未被收割」的任务——截止只能用 executor 重新拿到执行权后读取，而那一刻 abort 早就
/// 被处理（`in_flight` 已归零）。这正是本证明要钉的限制（#94「timer/waiter 在阻塞
/// 期间无法前进」）。截止分支的身份由「owner 全程冻结 ⇒ `await_settled` 不可能结算」
/// 结构保证，故断言报告**不含任何已结算终局桶**。
#[tokio::test]
async fn current_thread_blocked_poll_cannot_observe_shutdown_deadline() {
    let scope = Arc::new(FxScope::new());
    let t0 = Instant::now();

    // ① 托管任务 T：永不就绪 ⇒ 排空只能停在它上面（句柄丢弃不影响，Drop 不 abort）。
    scope.spawn_tracked(std::future::pending::<()>()).unwrap();

    // ② 非托管 B：受控 Future（poll#2 里同步阻塞唯一 executor 线程）。
    let go = Arc::new(tokio::sync::Notify::new());
    let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let go_task = Arc::clone(&go);
    scope
        .spawn_detached(async move {
            let _ = started_tx.send(());
            go_task.notified().await;
            let _ = entered_tx.send(());
            let _ = release_rx.recv(); // 同步阻塞：占死唯一 executor 线程
        })
        .expect("闸开 ⇒ 脱管 spawn 可用");

    // ③ 非托管 W：等待者分支的 shutdown_timeout（体内首 poll 登记 50ms 截止）。
    let w_registered = Arc::new(AtomicBool::new(false));
    let waiter_returned = Arc::new(AtomicBool::new(false));
    let waiter_at = Arc::new(Mutex::new(None::<Instant>));
    let waiter_report = Arc::new(Mutex::new(None::<ShutdownReport>));
    let (w_scope, w_registered_task, w_returned, w_at, w_report) = (
        Arc::clone(&scope),
        Arc::clone(&w_registered),
        Arc::clone(&waiter_returned),
        Arc::clone(&waiter_at),
        Arc::clone(&waiter_report),
    );
    scope
        .spawn_detached(async move {
            let mut fut = Box::pin(w_scope.shutdown_timeout(Duration::from_millis(50)));
            assert!(
                poll_once(&mut fut).await.is_none(),
                "等待者不得在阻塞前返回（排空 owner 未结算）"
            );
            // 此刻 50ms 截止已在时间轮上（内部 timeout 已首 poll）。
            w_registered_task.store(true, Ordering::Release);
            let report = fut.await;
            *w_at.lock().expect("poisoned") = Some(Instant::now());
            *w_report.lock().expect("poisoned") = Some(report);
            w_returned.store(true, Ordering::Release);
        })
        .expect("闸开 ⇒ 脱管 spawn 可用");

    // ④ 排空 owner：首 poll 发布 Draining（顺带 close+abort_all+take）后**冻结**——
    //    此后不再 poll 它 ⇒ 排空永不结算，等待者的唯一出路是自己的截止。
    let mut owner = Box::pin(scope.shutdown());
    assert!(
        poll_once(&mut owner).await.is_none(),
        "排空不得在阻塞前结算（T 仍在途）"
    );

    // ⑤ 外部 OS 观察线程（全用 OS 时钟有界等待）：确认受控任务进入同步段 → 睡 300ms
    //    （≫ 50ms 截止）→ 采样「等待者是否已前进」→ 放行 → 等它返回。
    let (observe_tx, observe_rx) = std::sync::mpsc::channel::<()>();
    let (result_tx, result_rx) = tokio::sync::oneshot::channel::<(bool, Instant)>();
    let observer = std::thread::spawn(move || {
        // 握手/等待上限一律 30s（issue `udf-frame-so3`）：本证明的本质是「阻塞期
        // timer/waiter 不前进」，不依赖紧上界；紧上界在共享机器上会假红。进程级
        // `bounded` 护栏（10s）仍在 ⇒ 外部线程永不放行时仍具名失败而非挂死。
        observe_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("观察线程须收到开始观察指令");
        go.notify_one();
        entered_rx
            .recv_timeout(Duration::from_secs(30))
            .expect("受控任务须进入同步段（不得静默跳过）");
        std::thread::sleep(Duration::from_millis(300));
        let progressed_during_block = waiter_returned.load(Ordering::Acquire);
        let release_at = Instant::now();
        release_tx.send(()).expect("受控任务须仍在等放行");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !waiter_returned.load(Ordering::Acquire) {
            if Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let _ = result_tx.send((progressed_during_block, release_at));
    });

    // ⑥ 等「截止已登记」，再交给外部线程观察；整体包在 OS 护栏里：外部线程永不放行
    //    时必须具名失败而不是挂死。
    wait_until("等待者的 50ms 截止已在时间轮上", || {
        w_registered.load(Ordering::Acquire)
    })
    .await;
    started_rx.await.expect("受控任务须发出 started");
    let (progressed_during_block, release_at) = bounded("current-thread 限制证明", async {
        observe_tx.send(()).expect("观察线程须存活");
        result_rx.await.expect("观察线程须回传观测结果")
    })
    .await;

    let waited_until = waiter_at.lock().expect("poisoned").expect("W 须已返回");
    let report = waiter_report
        .lock()
        .expect("poisoned")
        .take()
        .expect("W 须已写报告");
    println!(
        "current-thread report: {report:?}（放行于 T0+{:?}，W 返回于 T0+{:?}）",
        release_at.duration_since(t0),
        waited_until.duration_since(t0)
    );

    assert!(
        !progressed_during_block,
        "阻塞期间 timer / shutdown waiter 不得前进（实得已前进）"
    );
    assert!(
        release_at.duration_since(t0) >= Duration::from_millis(300),
        "放行时 50ms 截止早已过期（放行于 T0+{:?}）",
        release_at.duration_since(t0)
    );
    assert!(
        waited_until >= release_at,
        "等待者只能在放行后才观察到已过期 timeout（返回于 T0+{:?}，放行于 T0+{:?}）",
        waited_until.duration_since(t0),
        release_at.duration_since(t0)
    );
    // 截止分支身份：owner 全程冻结 ⇒ `await_settled` 不可能结算 ⇒ 报告不得含终局桶。
    assert_eq!(report.cancelled, 0, "截止分支不得含已结算终局桶");
    assert_eq!(report.completed, 0, "截止分支不得含已结算终局桶");
    assert!(report.panicked.is_empty(), "截止分支不得含 panic 载荷");
    assert_eq!(report.failed, 0, "截止分支不得含失败终局");
    assert!(!report.drain_interrupted, "不是中断结果，是截止读数");
    assert_eq!(
        report.timed_out, report.still_running,
        "截止分支两读数同源（in_flight）"
    );

    observer.join().expect("观察线程须正常退出");
    drop(owner); // 冻结的排空 owner 到此弃置（守卫发布中断结果，无等待者）
}
