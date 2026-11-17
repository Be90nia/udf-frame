#![cfg(all(feature = "tokio", feature = "pool"))]
//! 生命周期 / 资源 conformance 套件（蓝图 §4 生命周期模型 + §5 三轴资源模型）。
//!
//! 定位：把**已定义**的语义写成跨实现的对外可观察断言——只读报告字段与
//! `metrics()` 读数，不触内部字段。语义被改坏 ⇒ 本文件变红。
//!
//! 分工：逐实现的细粒度回归在 `tests/fx_lifecycle.rs`（停机报告各桶）、
//! `tests/object_pool.rs`（借出四路径 / reset / resize）、`tests/memory_pool.rs`
//! （借还机器）里；本文件不替代它们，只钉「换实现也必须成立」的那一层。
//!
//! | 契约 | 断言 |
//! |---|---|
//! | close ≠ cancel | 闸关后在途任务照常完成、令牌未取消、拒新 spawn |
//! | shutdown 等净 | 全终局 ⇒ `timed_out == 0 && still_running == 0` |
//! | shutdown_timeout 超时 | `still_running <= timed_out`；未观测终止不得进 `completed`/`cancelled` |
//! | 幂等 | 重复 close/cancel/shutdown 安全；空集二次报告为 `default` |
//! | 桶互斥穷尽 | 各桶之和 == 任务总数 |
//! | `max` 轴 | `idle + 正常借出 <= max`，降级件在宇宙外 |
//! | 字节轴 | 只计 idle 常驻字节，借出即回卷 |
//! | 降级轴 | 不入宇宙、归还即释放、不占字节预算 |

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use skeleton_infra::fx::{FxScope, ShutdownReport, TaskError, cancel_pair};
use skeleton_infra::objpool::ObjectPool;

// ── 共享判别式（真实实现与「错误语义桩」走同一条，见文末变异守卫）─────────

/// 桶互斥且穷尽：本次排空观测到的每条任务终局恰落一桶（报告对外可观察的唯一
/// 不变量：`completed` / `cancelled` / `panicked` / `failed` / `still_running`
/// 之和 == 任务总数；`timed_out` 是截止事件计数，与终局桶正交）。
fn assert_buckets_exhaustive(report: &ShutdownReport, total: usize) {
    let reported = report.completed
        + report.cancelled
        + report.panicked.len()
        + report.failed
        + report.still_running;
    assert_eq!(reported, total, "桶和必须等于任务总数（互斥且穷尽）");
    assert_eq!(
        report.errors.len(),
        report.failed,
        "失败桶的计数与明细同点写入（errors.len() == failed）"
    );
}

/// 超时路径的可观察契约（真实场景与错误语义桩共用）：
/// ① 每任务恰落一桶；② 已终局者按类落账；③ 不可让渡的自旋任务此刻仍在跑 ⇒
/// 必须出现在 `still_running` 且 `still_running <= timed_out`（截止计数只减不增）；
/// ④ 不得把它记成 `completed` / `cancelled`——超时只表示调用方停止等待。
fn assert_timeout_outcomes(report: &ShutdownReport, finished: usize, spinning: usize) {
    assert_buckets_exhaustive(report, finished + spinning);
    assert_eq!(report.completed, finished, "已终局者按类落 completed");
    assert!(report.panicked.is_empty(), "本场景无 panic 终局");
    assert_eq!(report.failed, 0, "本场景无非 panic 失败终局");
    assert_eq!(report.cancelled, 0, "超时不得把未观测终止记成已取消");
    assert!(
        report.still_running >= spinning,
        "自旋任务在报告返回时仍在跑 ⇒ 必申报 still_running"
    );
    assert!(
        report.timed_out >= spinning,
        "截止瞬间这些任务未完成 ⇒ timed_out 至少覆盖它们"
    );
    assert!(
        report.still_running <= report.timed_out,
        "still_running 只减不增（截止后首轮收割只会减少）"
    );
}

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

// ── 生命周期：close ≠ cancel，且最终等净 ────────────────────────────────

#[tokio::test]
async fn close_keeps_in_flight_work_running_and_shutdown_drains_clean() {
    let gate = Arc::new(AtomicBool::new(false));
    let gate_task = Arc::clone(&gate);
    let scope = FxScope::new();
    let work = scope
        .spawn_tracked(async move {
            while !gate_task.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            7u8
        })
        .unwrap();

    scope.close();
    assert!(scope.is_closed(), "close 关 accepting 闸");
    assert!(!scope.token().is_cancelled(), "close 不是 cancel");
    assert!(
        scope.spawn_tracked(async {}).is_err(),
        "闸关后拒发新工作（含托管/脱管）"
    );

    gate.store(true, Ordering::Release);
    assert_eq!(
        bounded("close 后在途任务照常完成", work.join()).await,
        Ok(7),
        "close 后已在途的任务照常完成"
    );

    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    assert_buckets_exhaustive(&report, 1);
    assert_eq!(report.completed, 1, "等净路径：已终局者进 completed");
    assert_eq!(
        (report.timed_out, report.still_running),
        (0, 0),
        "等净至空集 ⇒ 无超时、无未观测终局"
    );
    assert_eq!(report.cancelled, 0, "close 不产生取消终局");
}

/// 无超时路径：`shutdown` 返回即全终局（`timed_out` / `still_running` 恒 0），
/// 且已终局条目在排空时被收割进 `completed`（不因停机而丢账）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_reports_full_drain_without_timeout() {
    const TOTAL: usize = 8;
    let scope = FxScope::new();
    let mut handles = Vec::new();
    for _ in 0..TOTAL {
        handles.push(scope.spawn_tracked(async {}).unwrap());
    }
    for handle in handles {
        bounded("托管任务 join 完成", handle.join())
            .await
            .expect("task ok");
    }

    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    assert_buckets_exhaustive(&report, TOTAL);
    assert_eq!(
        report.completed, TOTAL,
        "已终局条目在排空时收割进 completed"
    );
    assert_eq!(
        (report.timed_out, report.still_running),
        (0, 0),
        "no-timeout 路径不得申报超时或未观测终局"
    );
}

/// 停机先于首轮 poll：任务只能落 `cancelled`（abort 的诚实归宿），不得被伪造成
/// `completed`——单线程运行时下 abort_all 发生在任何 yield 之前，故确定性。
#[tokio::test]
async fn unpolled_tasks_are_reported_cancelled_never_completed() {
    const TOTAL: usize = 4;
    let scope = FxScope::new();
    for _ in 0..TOTAL {
        scope.spawn_tracked(async {}).unwrap();
    }

    let report = bounded("shutdown 有界返回", scope.shutdown()).await;
    assert_buckets_exhaustive(&report, TOTAL);
    assert_eq!(report.completed, 0, "未及 poll 的任务不得记成已完成");
    assert_eq!(report.cancelled, TOTAL, "abort 后的归宿是 cancelled");
    assert_eq!((report.timed_out, report.still_running), (0, 0));
}

/// 超时路径：自旋段（不可让渡）把截止推过期限 ⇒ 只能申报 still_running，
/// 不得把未观测终止计入 completed / cancelled。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_reports_survivors_without_claiming_termination() {
    let done = Arc::new(AtomicUsize::new(0));
    let hold = Arc::new(AtomicBool::new(false));
    let holding = Arc::new(AtomicBool::new(false));
    // panic 路径也放开自旋段：否则 worker 在无 await 的 poll 里滞留，运行时拆除
    // 期等它 ⇒ 测试挂死（CI 上表现为 job 超时）。
    let _release = ReleaseOnDrop(Arc::clone(&hold));
    let scope = FxScope::new();

    // ① 两条正常完成的任务。
    for _ in 0..2 {
        let done_task = Arc::clone(&done);
        scope
            .spawn_tracked(async move {
                done_task.fetch_add(1, Ordering::Release);
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

    // 握手：正常任务已终局、自旋任务已滞留在无 await 的 poll 段内（有界等待，
    // 不变量被破坏时诊断失败而非挂死）。
    wait_until("两条正常任务已终局", || {
        done.load(Ordering::Acquire) >= 2
    })
    .await;
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
    assert_timeout_outcomes(&report, 2, 1);
    assert!(
        !spinner.is_finished(),
        "报告不宣称超时任务已终止：自旋任务此刻仍在跑"
    );

    hold.store(false, Ordering::Relaxed); // 放行让渡，abort 在下一个 await 点生效
    assert_eq!(
        bounded("超时遗留任务被 abort 收割", spinner.join()).await,
        Err(TaskError::Cancelled),
        "超时遗留任务由 abort 在下一 await 点收割"
    );
}

/// 幂等：重复 close / cancel / shutdown 安全；空集二次报告恒 `default`。
#[tokio::test]
async fn close_cancel_and_shutdown_are_idempotent() {
    let (source, token) = cancel_pair();
    source.cancel();
    source.cancel();
    assert!(
        token.is_cancelled(),
        "重复 cancel：观察面恒 true，不 panic 不改语义"
    );

    let scope = FxScope::new();
    assert!(!scope.token().is_cancelled(), "新建 scope 未取消");
    let work = scope.spawn_tracked(async { 5u8 }).unwrap();

    scope.close();
    scope.close();
    assert_eq!(
        bounded("在途任务不受重复 close 影响", work.join()).await,
        Ok(5),
        "在途任务不受重复 close 影响"
    );

    let first = bounded("首次 shutdown 返回", scope.shutdown()).await;
    assert_eq!(first.completed, 1, "首次 shutdown 收割已终局任务");
    let second = bounded("空集二次 shutdown 返回", scope.shutdown()).await;
    assert_eq!(
        second,
        ShutdownReport::default(),
        "空集二次 shutdown 恒空报告"
    );
    let third = bounded(
        "空集超时路径返回",
        scope.shutdown_timeout(Duration::from_millis(10)),
    )
    .await;
    assert_eq!(
        third,
        ShutdownReport::default(),
        "空集超时路径不得凭空申报超时"
    );
}

// ── 资源三轴（ObjectPool / 蓝图 §5）────────────────────────────────────

/// 析构可观测件：`drops` 计数是「降级件归还即释放」的对外观察面。
struct Tracked {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Tracked {
    fn factory(drops: Arc<AtomicUsize>) -> impl Fn() -> Tracked + Send + Sync + 'static {
        move || Tracked {
            data: Vec::with_capacity(512),
            drops: Arc::clone(&drops),
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 三轴同池可观察：`max` 约束宇宙（idle + 正常借出）、降级件在宇宙外、字节预算
/// 只计 idle 常驻字节、降级件归还即释放。
#[test]
fn max_axis_bounds_universe_and_degradation_stays_outside() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::with_byte_quota(
        1,
        1,
        Tracked::factory(Arc::clone(&drops)),
        4096,
        |t: &Tracked| t.data.capacity(),
    );

    let normal = pool.acquire();
    let degraded = pool.acquire(); // 宇宙 1 == max 1 ⇒ 降级直构（两轴皆不计）

    let m = pool.metrics();
    assert_eq!(m.core.max, 1);
    assert_eq!(m.degraded, 1, "宇宙已满 ⇒ 降级直构留痕");
    assert_eq!(
        (m.core.idle, m.core.borrowed),
        (0, 2),
        "borrowed 含降级借出"
    );
    // `degraded` 此刻 == 在借降级件数（借后未归还）⇒ 扣除即「宇宙内件数」。
    assert_eq!(
        m.core.idle + m.core.borrowed - m.degraded as usize,
        m.core.max,
        "max 约束 idle + 正常借出；降级件在宇宙之外"
    );
    assert_eq!(m.idle_bytes, 0, "借出件不占字节预算（降级件同样不占）");

    drop(degraded);
    let m = pool.metrics();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "降级件归还即释放（不进 idle、不被池留住）"
    );
    assert_eq!(
        (m.core.idle, m.idle_bytes),
        (0, 0),
        "降级件不入宇宙、不认领字节"
    );
    assert_eq!(
        m.core.dropped_overflow, 0,
        "降级释放 ≠ 溢出丢弃（两本账不混）"
    );

    drop(normal);
    let m = pool.metrics();
    assert_eq!(
        (m.core.idle, m.idle_bytes),
        (1, 512),
        "正常件归还 ⇒ 常驻 idle 并认领其字节"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1, "正常件被池留住（未析构）");
    assert_eq!(
        m.core.idle + m.core.borrowed,
        m.core.max,
        "终态宇宙仍不超 max"
    );
}

/// 字节轴：只计 idle 常驻字节；借出即回卷（对称配平，借出件不占预算）。
#[test]
fn byte_axis_counts_only_idle_and_rolls_back_on_borrow() {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::with_byte_quota(
        4,
        2,
        Tracked::factory(Arc::clone(&drops)),
        2048,
        |t: &Tracked| t.data.capacity(),
    );

    let borrowed = pool.acquire();
    assert_eq!(pool.metrics().idle_bytes, 0, "借出中的对象不占字节预算");
    drop(borrowed);
    let m = pool.metrics();
    assert_eq!(
        (m.byte_budget, m.idle_bytes),
        (2048, 512),
        "归还 ⇒ 认领常驻字节（held == 单件驻留）"
    );

    let reused = pool.acquire();
    let m = pool.metrics();
    assert_eq!(m.idle_bytes, 0, "借出对称回卷：held 随件离开 idle 归零");
    assert_eq!(m.reused, 1, "读数面证明这轮走的是复用而非新建");
    drop(reused);
    assert_eq!(
        pool.metrics().idle_bytes,
        512,
        "再归还 ⇒ 重新认领（每轮完整往返都配平）"
    );
}

// ── 变异守卫：套件有鉴别力（错误语义必须被判红）─────────────────────────

/// 把「仍在运行」伪造进 `completed`（正是契约禁止的那种改坏）的报告必须被
/// 同一条判别式拒绝。
///
/// 证明范围：只覆盖**报告语义判别式**的鉴别力（真实实现改坏时本文件走的就是
/// 这条判别式）；不覆盖计时/abort 时序本身——那由 `timeout_reports_survivors_
/// without_claiming_termination` 的真实场景钉住。产品代码禁为本单改动，故以
/// 可控桩模拟该错误语义。
#[test]
#[should_panic(expected = "已终局者按类落 completed")]
fn predicate_rejects_still_running_counted_as_completed() {
    // `#[non_exhaustive]`：跨 crate 禁 struct literal（含 `..Default::default()`
    // 形态，rustc 一律 E0639）——外部构造面 = `default()` 起手再按需赋值。
    let mut genuine = ShutdownReport::default();
    genuine.completed = 2;
    genuine.timed_out = 1;
    genuine.still_running = 1;
    assert_timeout_outcomes(&genuine, 2, 1);

    let mut forged = genuine;
    forged.completed += forged.still_running; // 错误语义：未观测终止记成已完成
    forged.still_running = 0;
    assert_timeout_outcomes(&forged, 2, 1);
}
