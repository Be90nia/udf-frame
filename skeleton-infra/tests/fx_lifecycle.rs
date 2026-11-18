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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use skeleton_infra::fx::{FxScope, ShutdownReport, TaskError, cancel_pair};

/// 自旋段放行闸的 RAII 兜底：断言 panic 时也必须放开自旋段——否则 worker 线程
/// 滞留在无 await 的 poll 里，运行时拆除期等它 ⇒ 测试挂死（CI 上表现成 job 超时，
/// 比一条红更难诊断）。
struct ReleaseOnDrop(Arc<AtomicBool>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// 有界等待握手条件（5s 抖动余量，远小于 job 超时）：不变量被破坏时以诊断 panic
/// 收场，禁无限轮转成挂死。
async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    const LIMIT: Duration = Duration::from_secs(5);
    let reached = tokio::time::timeout(LIMIT, async {
        while !ready() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "等待 {}s 仍未见「{what}」：不变量可能已被破坏（有界等待，禁挂死）",
        LIMIT.as_secs()
    );
}

/// 有界等待 future 完成（任务终止 / 停机返回）：超时即诊断 panic，禁挂死。
async fn bounded<F: std::future::Future>(what: &str, fut: F) -> F::Output {
    const LIMIT: Duration = Duration::from_secs(10);
    match tokio::time::timeout(LIMIT, fut).await {
        Ok(output) => output,
        Err(_) => panic!(
            "等待 {}s 仍未见「{what}」：不变量可能已被破坏（有界等待，禁挂死）",
            LIMIT.as_secs()
        ),
    }
}

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

/// 幂等：`close()` / `cancel()` / `shutdown()`（含超时形）重复调用安全；空集
/// 报告恒为 `default`，且 `ShutdownReport::default()` 可用（向后兼容面）。
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
    let second = bounded("二次 shutdown 返回", scope.shutdown()).await;
    assert_eq!(second, ShutdownReport::default(), "二次 shutdown 空报告");
    let third = bounded(
        "空集超时路径返回",
        scope.shutdown_timeout(Duration::from_millis(50)),
    )
    .await;
    assert_eq!(third, ShutdownReport::default(), "空集超时路径不声称超时");
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
