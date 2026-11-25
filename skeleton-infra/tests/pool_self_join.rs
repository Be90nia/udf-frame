#![cfg(feature = "pool")]
//! issue#132：worker 任务内的**自 join** 必须显式拒绝，而不是永久死锁。
//!
//! 自 join 的 worker 无法返回 `worker_loop` 去递减自己的退役计数 ⇒ 等自己 = 挂死。
//! 契约面：`try_join()` 返回 [`SelfJoinError`]（可判定），`join()` 走 fail-fast
//! panic（既有调用点签名不变，panic 由 worker 摘除并计入 `task_panics`）；外部
//! 所有者 `shutdown(); join();` 语义完全不变。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use skeleton_infra::pool::{SelfJoinError, ThreadPool, ThreadPoolTuning};

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let start = Instant::now();
    while !cond() {
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "timeout waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// `try_join()`：任务内可判定地拿到 `SelfJoinError`，且任务内 `shutdown()` 不阻塞。
///
/// 修复前本用例的等待会在 5s 后报红（任务永久阻塞在 join 上）——不挂死 CI。
#[test]
fn worker_task_self_join_is_rejected_instead_of_deadlocking() {
    let pool = Arc::new(ThreadPool::new(ThreadPoolTuning::new(1, 1, 4)));
    let (tx, rx) = mpsc::channel();
    let inner = Arc::clone(&pool);
    pool.execute(move || {
        inner.shutdown(); // 任务内停机：只发信号，不阻塞（契约要求）
        tx.send(inner.try_join()).expect("verdict 通道");
    })
    .expect("首件任务入队");

    let verdict = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("自 join 不得挂死");
    assert_eq!(verdict, Err(SelfJoinError));

    // 外部所有者的收尾语义不变。
    pool.shutdown();
    assert_eq!(pool.try_join(), Ok(()), "外部等待全部 worker 退役");
    assert_eq!(
        pool.metrics().task_panics,
        0,
        "try_join 路径不产生任务 panic"
    );
}

/// `join()`：自 join fail-fast panic，panic 被 worker 摘除记账，池继续服务。
#[test]
fn worker_task_join_panics_fail_fast_and_pool_keeps_serving() {
    let pool = Arc::new(ThreadPool::new(ThreadPoolTuning::new(2, 2, 4)));
    let inner = Arc::clone(&pool);
    pool.execute(move || inner.join()).expect("首件任务入队");

    wait_until("自 join 任务被摘除记账", || {
        pool.metrics().task_panics == 1
    });

    // 池未砖：另一个 worker 照常执行后续任务。
    let served = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&served);
    pool.execute(move || {
        counter.fetch_add(1, Ordering::SeqCst);
    })
    .expect("池仍接收任务");
    wait_until("池继续服务", || served.load(Ordering::SeqCst) == 1);

    pool.shutdown();
    pool.join();
    assert_eq!(pool.metrics().workers, 0, "外部收尾等全部 worker 退役");
}

/// 跨池不受影响：另一个池的 worker 可以 join 一个已退役的池（身份判定按池，
/// 不按线程名/逻辑 id）。
#[test]
fn worker_of_another_pool_may_join_a_retired_pool() {
    let other = Arc::new(ThreadPool::new(ThreadPoolTuning::new(1, 1, 1)));
    let host = Arc::new(ThreadPool::new(ThreadPoolTuning::new(1, 1, 1)));
    other.shutdown();
    other.join(); // 外部所有者先把它收干净

    let (tx, rx) = mpsc::channel();
    let target = Arc::clone(&other);
    host.execute(move || tx.send(target.try_join()).expect("verdict 通道"))
        .expect("host 任务入队");
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5))
            .expect("跨池 join 不得挂死"),
        Ok(())
    );

    host.shutdown();
    host.join();
}
