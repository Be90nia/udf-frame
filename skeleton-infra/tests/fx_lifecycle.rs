#![cfg(feature = "tokio")]
//! 停机报告契约（蓝图 §4 / §11 第 3 项）的生命周期回归。
//!
//! 报告六桶互斥且穷尽：`completed` / `cancelled` / `panicked` / `failed` /
//! `still_running` 中每任务恰落一桶；`timed_out` 是**截止事件**的计数（调用方停止
//! 等待），`still_running` 是**终止未观测**的计数——二者都不宣称工作已终止。
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

/// e2e（跑法①）：正常完成 / panic / 长期存活 / 不可让渡自旋混合场景 →
/// `close()` → `shutdown_timeout(短期限)`。三类结局各落各桶，且报告不把超时任务
/// 记成已终止（自旋任务在报告返回时仍在跑，abort 在其下一个 await 点才生效）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_outcomes_land_in_distinct_buckets_without_claiming_termination() {
    let done = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(AtomicBool::new(false));
    let holding = Arc::new(AtomicBool::new(false));
    // panic 路径也放开自旋段：否则 worker 在无 await 的 poll 里滞留，运行时拆除
    // 期等它 ⇒ 测试挂死（CI 上表现为 job 超时）。
    let _release = ReleaseOnDrop(Arc::clone(&hold));
    let scope = FxScope::new();

    // ① 两个正常完成。
    for _ in 0..2 {
        let done_task = Arc::clone(&done);
        scope
            .spawn_tracked(async move {
                done_task.fetch_add(1, Ordering::Release);
            })
            .unwrap();
    }

    // ② 一个 panic（句柄丢弃：载荷只走集合面，报告是唯一观测口）。
    {
        let handle = scope.spawn_tracked(async { panic!("boom-e2e") }).unwrap();
        drop(handle);
    }

    // ③ 两个长期存活（abort 在 sleep await 点即生效 ⇒ 应进 cancelled）。
    for _ in 0..2 {
        scope
            .spawn_tracked(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            })
            .unwrap();
    }

    // ④ 一个不可让渡自旋段：abort 只在下一 await 生效 ⇒ 超时必达，报告只能报
    // still_running（构造同 fx.rs 单测的 hold/holding 握手）。
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

    // 握手（有界等待：不变量被破坏时诊断失败而非挂死）。
    wait_until("两个正常任务已终局", || {
        done.load(Ordering::Acquire) >= 2
    })
    .await;
    hold.store(true, Ordering::Relaxed);
    wait_until("自旋任务已进入不可让渡段", || {
        holding.load(Ordering::Relaxed)
    })
    .await;

    scope.close(); // 闸关：拒新 spawn，不取消在途
    assert!(!scope.token().is_cancelled(), "close 不是 cancel");
    let started = Instant::now();
    let report = bounded(
        "shutdown_timeout 有界返回",
        scope.shutdown_timeout(Duration::from_millis(50)),
    )
    .await;
    println!("mixed report (timeout path): {report:?}");

    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "超时必达：提前返回即守卫失效"
    );
    assert_eq!(report.completed, 2, "正常完成的任务进 completed");
    assert_eq!(
        report.panicked,
        vec!["boom-e2e".to_owned()],
        "panic 载荷进 panicked"
    );
    assert_eq!(report.failed, 0, "本场景无失败的终局");
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
        6,
        "桶互斥且穷尽：每任务恰落一桶"
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
