#![cfg(all(feature = "memgov", feature = "pool"))]
//! issue#116：`MemoryPool::acquire` 的超时**表示范围**边界。
//!
//! 修复前 `acquire` 在取任何锁、看任何空闲件之前先算 `Instant::now() + timeout`，
//! 超出平台单调时钟可表示范围的 `Duration`（如 `Duration::MAX`）直接 panic——
//! 连「池里就有空闲件」的路径都走不到。修复后语义 = **无实际期限**（不 panic、
//! 不截短成更短的期限），并仍受归还/扩容的 notify 驱动。

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use skeleton_infra::memgov::MemoryPool;

/// 空闲件直取：超大超时不得让「本来立刻可满足」的借出失败。
#[test]
fn huge_timeout_takes_the_idle_object_without_panicking() {
    let pool: MemoryPool<Vec<u8>> = MemoryPool::new(1, 1, || vec![7u8]);
    {
        let first = pool
            .acquire(Duration::from_millis(10))
            .expect("工厂新建必成功");
        assert_eq!(&*first, &[7]);
    } // Drop 即还 ⇒ 池里恰有一件闲置

    let reused = pool
        .acquire(Duration::MAX)
        .expect("空闲件必须直取（超大超时既不得 panic，也不得把路径变成拒绝）");
    assert_eq!(&*reused, &[7]);
    assert_eq!(pool.metrics().core.borrowed, 1);
}

/// 不可表示期限 ≠ 立即超时：配额用尽时必须**等**到归还才返回。
///
/// 这条钉住「无实际期限」的另一半语义：若把不可表示期限退化成 0（或截短成某个
/// 小期限），本用例会拿到 `Err(PoolAcquireTimeout)` 而不是归还后的对象。
#[test]
fn huge_timeout_waits_until_an_object_returns() {
    let pool = Arc::new(MemoryPool::new(1, 1, || vec![1u8]));
    let held = pool
        .acquire(Duration::from_millis(10))
        .expect("工厂新建必成功");

    let (started_tx, started_rx) = mpsc::channel();
    let waiter_pool = Arc::clone(&pool);
    let waiter = std::thread::spawn(move || {
        started_tx.send(()).expect("started 信号通道");
        waiter_pool.acquire(Duration::MAX).map(|guard| guard.len())
    });
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("等待线程必须启动");

    // 归还唯一一件 ⇒ 等待中的 acquire 被 notify 唤醒（无实际期限的等待仍受
    // 谓词复判驱动，不是忙等也不是永久挂起）。
    drop(held);
    let outcome = waiter.join().expect("等待线程不得 panic");
    assert_eq!(outcome.expect("归还后必须取到"), 1);
}
