//! `PooledThreadPool` 组合类最小示例：MemoryPool × ThreadPool 互喂时序封装。
//!
//! 提交闭包在 worker 线程内借缓冲→跑→guard Drop RAII 还（panic unwind 同
//! 覆盖——guard 是任务闭包栈上局部量）；借空=Discard 等价 + 独立计数。完整
//! 语义与 panic/借空/缩容路径见 `tests/pooled_thread_pool.rs`。

use std::sync::Arc;
use std::time::Duration;

use skeleton_infra::memgov::MemoryPool;
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use skeleton_infra::pooled_thread_pool::PooledThreadPool;

fn main() {
    let mem = Arc::new(MemoryPool::<Vec<u8>>::new(4, 2, Vec::new));
    let ptp = PooledThreadPool::new(
        Arc::clone(&mem),
        ThreadPool::new(ThreadPoolTuning::new(1, 2, 8)),
        Duration::from_millis(100),
    );
    for i in 0..4u8 {
        ptp.submit(move |buf: &mut Vec<u8>| buf.push(i)).unwrap();
    }
    ptp.threads().shutdown();
    ptp.threads().join();

    let m = ptp.mem_pool().metrics();
    assert_eq!(ptp.borrow_rejected(), 0, "池深≥并发，零借空");
    assert_eq!(m.core.borrowed, 0, "任务结束缓冲必已归还");
    assert_eq!(m.core.idle, 1, "同一缓冲全程复用，留存闲槽");
    assert_eq!(m.core.dropped_overflow, 0);
    assert_eq!(ptp.threads().metrics().completed, 4);
    println!("pooled_thread_pool: 组合类最小例全断言通过");
}
