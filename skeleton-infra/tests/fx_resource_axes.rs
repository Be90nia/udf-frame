#![cfg(feature = "tokio")]
//! fx 三资源维度分离（GitHub issue#39）的验收回归。
//!
//! 三轴互相独立：`max_concurrency`（并发槽 = 信号量许可）/ `max_pending`（已准入
//! 但等待许可）/ `max_tracked`（生命周期集合保留的**全部**条目，含已完成未收割）。
//!
//! 钉死的语义：
//! - 准入过载 ⇒ typed 拒绝（不阻塞、不 panic）；
//! - `Spawned::join()` 只消费结果、**不**移除集合条目 ⇒ 从不 reap 时未收割计数
//!   可观测且随条目增长；
//! - 六条终态路径（正常完成 / panic / 取消 / abort / shutdown / 接收端丢弃）
//!   各释放并发槽**恰好一次**（既不漏放也不多放——多放会让并发度越界）；
//! - 三条上界各自独立（只设一条不影响另两条的语义）；
//! - 收割口语义精确拆分：任务体已结束 ≠ 跟踪集合已完全收割。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use skeleton_infra::fx::{AdmissionError, FxScope, TaskError};

/// 有界等待（5s 上限）：不变量被破坏时以诊断 panic 收场，禁挂死。
async fn wait_for(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "timeout waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// 并发峰值探针：任务体进入 +1 / 退出 -1，峰值即实测并发度。
#[derive(Clone)]
struct ConcurrencyProbe {
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl ConcurrencyProbe {
    fn new() -> Self {
        Self {
            live: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 进入任务体：返回离开守卫（退出即 -1）。
    fn enter(&self) -> ConcurrencyExit {
        let now = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        ConcurrencyExit {
            live: Arc::clone(&self.live),
        }
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }
}

struct ConcurrencyExit {
    live: Arc<AtomicUsize>,
}

impl Drop for ConcurrencyExit {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
    }
}

/// 准入过载：pending 容量填满 ⇒ 超出部分 typed 拒绝（不阻塞、不 panic）。
#[tokio::test]
async fn pending_overflow_is_typed_rejection() {
    let scope = FxScope::new().with_concurrency(1).with_max_pending(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let release_task = Arc::clone(&release);

    // 第一条：准入即取走唯一并发槽（active=1），任务体停在 release 上。
    let holder = scope
        .try_spawn_tracked(async move {
            release_task.notified().await;
            1u8
        })
        .expect("first admission fits");
    // 第二条：槽已占 ⇒ 落 pending 档（waiting=1），pending 上限内准入。
    let pending = scope
        .try_spawn_tracked(async { 2u8 })
        .expect("second admission fits pending capacity");
    // 第三条：pending 容量已满 ⇒ typed 拒绝（既不阻塞也不 panic）。
    assert!(
        matches!(
            scope.try_spawn_tracked(async { 3u8 }),
            Err(AdmissionError::PendingFull)
        ),
        "pending 触顶必须 typed 拒绝（不阻塞、不 panic）"
    );

    let counts = scope.counts();
    assert_eq!(counts.active, 1, "唯一并发槽被占用");
    assert_eq!(counts.waiting, 1, "第二条在等许可");
    assert_eq!(counts.rejected, 1, "过载计数可见");

    release.notify_one();
    assert_eq!(holder.join().await, Ok(1));
    assert_eq!(pending.join().await, Ok(2));
    wait_for("all slots released", || scope.counts().active == 0).await;
    assert_eq!(scope.counts().waiting, 0, "等待数归零");
}

/// 从不 reap ⇒ 未收割计数可观测且随条目增长；保留上界触顶 ⇒ typed 拒绝。
#[tokio::test]
async fn never_reaped_entries_are_observable_and_tracked_bound_rejects() {
    let scope = FxScope::new().with_max_tracked(4);
    for i in 0..4u8 {
        let handle = scope
            .try_spawn_tracked(async move { i })
            .expect("within tracked bound");
        // join 只消费结果，**不**移除集合条目 ⇒ 条目滞留。
        assert_eq!(handle.join().await, Ok(i));
    }
    let counts = scope.counts();
    assert_eq!(
        counts.completed_unreaped, 4,
        "join 不移除集合条目 ⇒ 已完成未收割计数可见且在增长"
    );

    assert!(
        matches!(
            scope.try_spawn_tracked(async { 9u8 }),
            Err(AdmissionError::TrackedFull)
        ),
        "保留上界触顶必须 typed 拒绝"
    );
    assert_eq!(scope.counts().rejected, 1);
    assert_eq!(scope.counts().completed_unreaped, 4, "被拒准入不新增条目");

    // 显式收割 ⇒ 条目出集合 ⇒ 未收割计数归零 ⇒ 可再准入。
    assert!(scope.try_reap().is_empty(), "正常完成条目收割面为空");
    assert_eq!(scope.counts().completed_unreaped, 0);
    assert!(
        scope.try_spawn_tracked(async { 9u8 }).is_ok(),
        "收割后释放保留容量"
    );
}

/// 六条终态路径各释放并发槽恰好一次：既不漏放（槽归还）也不多放（并发度不越界）。
#[tokio::test]
async fn each_terminal_path_releases_capacity_exactly_once() {
    // 路径①正常完成。
    let scope = FxScope::new().with_concurrency(1);
    let handle = scope.try_spawn_tracked(async { 1u8 }).expect("admitted");
    assert_eq!(handle.join().await, Ok(1));
    assert_capacity_released_once(&scope, "normal completion").await;

    // 路径②panic（归一为 TaskError::Panicked）。
    let scope = FxScope::new().with_concurrency(1);
    let handle = scope
        .try_spawn_tracked(async { panic!("boom-axis") })
        .expect("admitted");
    assert!(matches!(handle.join().await, Err(TaskError::Panicked(_))));
    assert_capacity_released_once(&scope, "panic").await;

    // 路径③取消（协作取消面：scope 的取消源已触发，任务体观察取消后返回）。
    let (source, _token) = skeleton_infra::fx::cancel_pair();
    source.cancel();
    let scope = FxScope::new().with_concurrency(1).with_source(source);
    let token = scope.token();
    let handle = scope
        .try_spawn_tracked(async move {
            token.cancelled().await;
            7u8
        })
        .expect("admitted");
    assert_eq!(handle.join().await, Ok(7));
    assert_capacity_released_once(&scope, "cancel").await;

    // 路径④abort（句柄中止）。
    let scope = FxScope::new().with_concurrency(1);
    let handle = scope
        .try_spawn_tracked(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            1u8
        })
        .expect("admitted");
    handle.abort();
    assert_eq!(handle.join().await, Err(TaskError::Cancelled));
    assert_capacity_released_once(&scope, "abort").await;

    // 路径⑤shutdown（停机排空：全组 abort）。
    let scope = FxScope::new().with_concurrency(1);
    let _handle = scope
        .try_spawn_tracked(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
            1u8
        })
        .expect("admitted");
    let _report = scope.shutdown().await;
    assert_capacity_released_once(&scope, "shutdown").await;

    // 路径⑥接收端丢弃（句柄被丢、无人 join：任务体照常跑完并释放槽）。
    let scope = FxScope::new().with_concurrency(1);
    let done = Arc::new(AtomicUsize::new(0));
    let done_task = Arc::clone(&done);
    let handle = scope
        .try_spawn_tracked(async move {
            done_task.fetch_add(1, Ordering::SeqCst);
            1u8
        })
        .expect("admitted");
    drop(handle); // 接收端先亡
    wait_for("task body finished", || done.load(Ordering::SeqCst) == 1).await;
    assert_capacity_released_once(&scope, "receiver drop").await;
}

/// 终局后容量校验：槽归零（不漏放）+ 并发度不越界（不多放）。
async fn assert_capacity_released_once(scope: &FxScope, path: &str) {
    // ① 不漏放：并发槽与等待数归零（终局恰释放一次 ⇒ 计数不多不少；多一次会
    //    让计数下溢——debug 构建直接 panic，测试即红）。
    wait_for("slot released", || scope.counts().active == 0).await;
    assert_eq!(scope.counts().active, 0, "{path}: 并发槽须归还");
    assert_eq!(scope.counts().waiting, 0, "{path}: 等待数须归零");

    // ② 不多放：并发上限 1 时，两条并发任务实测并发度仍须 ≤1（多放会让它变 2）。
    //    停机路径的 scope 已关闸（不能再准入）⇒ 该面由①的计数归零 + 无下溢
    //    panic 钉住（闸关面的并发度无从再测——闸关后本就不许新准入）。
    if scope.is_closed() {
        return;
    }
    let probe = ConcurrencyProbe::new();
    let mut handles = Vec::new();
    for _ in 0..2 {
        let probe = probe.clone();
        handles.push(
            scope
                .try_spawn_tracked(async move {
                    let _exit = probe.enter();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                })
                .expect("admitted"),
        );
    }
    for handle in handles {
        let _ = handle.join().await;
    }
    assert!(
        probe.peak() <= 1,
        "{path}: 并发槽被多放（实测并发度 {} > 上限 1）",
        probe.peak()
    );
}

/// 三条上界各自独立：只设一条不影响另两条的语义。
#[tokio::test]
async fn limits_are_independent() {
    // 只设 max_concurrency：pending 不设限 ⇒ 等许可的任务照常准入（不拒）。
    let scope = FxScope::new().with_concurrency(1);
    let release = Arc::new(tokio::sync::Notify::new());
    let release_task = Arc::clone(&release);
    let _holder = scope
        .try_spawn_tracked(async move {
            release_task.notified().await;
            1u8
        })
        .expect("admitted");
    for _ in 0..3 {
        assert!(
            scope.try_spawn_tracked(async { 2u8 }).is_ok(),
            "未设 max_pending ⇒ 等待许可不构成拒绝理由"
        );
    }
    assert_eq!(scope.counts().waiting, 3, "等待数只受 max_concurrency 影响");

    // 只设 max_pending：无并发闸 ⇒ 无任务等许可 ⇒ 上界不触发（不误拒）。
    let scope = FxScope::new().with_max_pending(1);
    for _ in 0..3 {
        assert!(
            scope.try_spawn_tracked(async { 2u8 }).is_ok(),
            "未设 max_concurrency ⇒ 无等待 ⇒ max_pending 不触发"
        );
    }
    assert_eq!(scope.counts().waiting, 0);

    // 只设 max_tracked：pending/并发语义不受影响，且触顶轴是 tracked。
    let scope = FxScope::new().with_max_tracked(2);
    assert!(scope.try_spawn_tracked(async { 1u8 }).is_ok());
    assert!(scope.try_spawn_tracked(async { 2u8 }).is_ok());
    assert!(
        matches!(
            scope.try_spawn_tracked(async { 3u8 }),
            Err(AdmissionError::TrackedFull)
        ),
        "只设 max_tracked ⇒ 触顶轴是 tracked"
    );
    assert_eq!(scope.counts().waiting, 0, "max_tracked 不影响等待语义");
}

/// 收割口语义精确拆分：任务体已结束（body-idle）≠ 跟踪集合已完全收割
/// （fully-reaped-idle）——两个名字不得互相误读。
#[tokio::test]
async fn body_idle_and_fully_reaped_idle_are_distinct() {
    let scope = FxScope::new();
    let handle = scope.try_spawn_tracked(async { 1u8 }).expect("admitted");
    assert_eq!(handle.join().await, Ok(1));

    // 任务体已结束，但条目仍滞留集合 ⇒ body-idle 成立、fully-reaped 不成立。
    wait_for("body finished", || scope.counts().completed_unreaped == 1).await;
    scope.expect_body_idle();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            scope.expect_fully_reaped_idle()
        }))
        .is_err(),
        "条目未收割时 fully-reaped-idle 必须不成立"
    );

    // 收割后两者同时成立。
    assert!(scope.try_reap().is_empty());
    scope.expect_body_idle();
    scope.expect_fully_reaped_idle();
}
