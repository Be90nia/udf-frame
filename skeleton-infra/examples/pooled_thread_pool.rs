//! `PooledThreadPool` 组合类最小示例：ObjectPool × ThreadPool 互喂时序封装。
//!
//! 提交闭包在 worker 线程内借缓冲→跑→句柄 Drop RAII 还（panic unwind 同覆盖
//! ——句柄是任务闭包栈上局部量）；借空=降级直构（永不丢任务，`degraded` 计数
//! 留痕）。完整语义与 panic/饱和/CallerRuns 档见 `tests/pooled_thread_pool.rs`。

use std::sync::Arc;

use skeleton_infra::objpool::ObjectPool;
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use skeleton_infra::pooled_thread_pool::PooledThreadPool;

fn main() {
    let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(4, 2, Vec::new));
    let ptp = PooledThreadPool::new(
        Arc::clone(&buffers),
        ThreadPool::new(ThreadPoolTuning::new(1, 2, 8)),
    );
    for i in 0..4u8 {
        ptp.submit(move |buf: &mut Vec<u8>| buf.push(i)).unwrap();
    }
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = buffers.metrics();
    assert_eq!(m.degraded, 0, "池深≥并发，零降级");
    assert_eq!(m.core.borrowed, 0, "任务结束缓冲必已归还");
    assert_eq!(m.core.idle, 1, "同一缓冲全程复用，留存闲槽");
    assert_eq!(m.core.dropped_overflow, 0);
    assert_eq!(ptp.threads().metrics().completed, 4);
    println!("pooled_thread_pool: 组合类最小例全断言通过");
}
