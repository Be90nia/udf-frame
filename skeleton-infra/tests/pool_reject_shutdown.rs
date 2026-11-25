#![cfg(feature = "pool")]
//! issue#118：`DiscardOldestPolicy` 不得删除 shutdown 后**已接收**的排队任务。
//!
//! 修复前的缺陷形状：`submit` 见 `shutdown` 仍走拒绝策略 → 策略先 `pop_oldest()`
//! 把已接收任务摘掉，再 `offer()` 因 shutdown 失败 → **既丢了既有工作又丢了新
//! 任务**，而策略回报 `Ok(())`。修复后停机拒绝在单临界区内判定，pop/替换只发生在
//! 「队列满且未停机」的正常换血路径。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use skeleton_infra::pool::{DiscardOldestPolicy, RejectCause, ThreadPool, ThreadPoolTuning};

/// 确定性的阻塞闸：worker 被第一件任务占住，后续提交才会落进队列。
fn occupy_only_worker(pool: &ThreadPool) -> mpsc::Sender<()> {
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    pool.execute(move || {
        started_tx.send(()).expect("started 信号通道");
        let _ = release_rx.recv();
    })
    .expect("首件任务必被接收");
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker 必须开跑阻塞任务");
    release_tx
}

/// 主场景：队列里有一条**已接收**任务 → shutdown → 再提交一次。
///
/// 断言三件事：① 新任务退还调用方（cause = Policy）；② 已接收任务仍在队列并在
/// 放行后**恰好执行一次**；③ 指标把「新提交被拒」与「已接收任务被弃」分账。
#[test]
fn discard_oldest_never_eats_accepted_tasks_after_shutdown() {
    let pool = ThreadPool::with_policy(
        ThreadPoolTuning::new(1, 1, 1),
        Arc::new(DiscardOldestPolicy),
    );
    let release = occupy_only_worker(&pool);

    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_task = Arc::clone(&accepted);
    pool.execute(move || {
        accepted_task.fetch_add(1, Ordering::SeqCst);
    })
    .expect("队列有位 ⇒ 接收");
    assert_eq!(pool.metrics().queue_len, 1, "T1 必须已入队");

    pool.shutdown();

    let rejected = Arc::new(AtomicUsize::new(0));
    let rejected_task = Arc::clone(&rejected);
    let returned = pool
        .execute(move || {
            rejected_task.fetch_add(1, Ordering::SeqCst);
        })
        .expect_err("停机后的提交必须退还调用方，不得被策略当作已消化");
    assert!(matches!(returned.cause(), RejectCause::Policy));

    let metrics = pool.metrics();
    assert_eq!(metrics.rejected, 1, "新提交被拒计入 rejected");
    assert_eq!(metrics.discarded, 0, "停机拒绝不得动已接收任务");
    assert_eq!(
        metrics.queue_len, 1,
        "已接收任务必须留在队列等 graceful drain"
    );

    release.send(()).expect("放行 worker");
    pool.join();

    assert_eq!(accepted.load(Ordering::SeqCst), 1, "已接收任务恰好执行一次");
    assert_eq!(rejected.load(Ordering::SeqCst), 0, "退还的任务不由池执行");
}

/// 对照组：**未停机且队列满**时换血语义照旧（弃队头、本任务入队），并且
/// `discarded` 把「已接收任务被策略弃掉」显式记账。
#[test]
fn discard_oldest_still_swaps_head_when_queue_is_full_before_shutdown() {
    let pool = ThreadPool::with_policy(
        ThreadPoolTuning::new(1, 1, 1),
        Arc::new(DiscardOldestPolicy),
    );
    let release = occupy_only_worker(&pool);

    let dropped = Arc::new(AtomicUsize::new(0));
    let dropped_task = Arc::clone(&dropped);
    pool.execute(move || {
        dropped_task.fetch_add(1, Ordering::SeqCst);
    })
    .expect("队列有位 ⇒ 接收");

    let swapped_in = Arc::new(AtomicUsize::new(0));
    let swapped_task = Arc::clone(&swapped_in);
    pool.execute(move || {
        swapped_task.fetch_add(1, Ordering::SeqCst);
    })
    .expect("队列满 ⇒ DiscardOldest 弃队头换入（Ok）");

    let metrics = pool.metrics();
    assert_eq!(metrics.discarded, 1, "被策略弃掉的已接收任务单独记账");
    assert_eq!(metrics.rejected, 1, "策略触发即事件（新提交被拒）");
    assert_eq!(metrics.queue_len, 1);

    pool.shutdown();
    release.send(()).expect("放行 worker");
    pool.join();

    assert_eq!(dropped.load(Ordering::SeqCst), 0, "队头已被替换，不再执行");
    assert_eq!(swapped_in.load(Ordering::SeqCst), 1, "换入的任务执行一次");
}
