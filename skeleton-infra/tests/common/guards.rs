//! 有界等待护栏（OS 时钟，**不用 tokio 计时器**）。被集成测试
//! `#[path = "common/guards.rs"] mod guards;` 引入：每个测试目标独立编译一份
//! 本文件，未用面按目标各异，故整文件 allow(dead_code)。
//!
//! 为什么不 `tokio::time::timeout`：被测任务可能在 poll 内**无 await 忙等**
//! （自旋段），它会饿死 tokio 的时间驱动 ⇒ 计时器护栏自身无界 ⇒ 测试挂死（CI 上
//! 表现成 job 超时，比一条红更难诊断）。OS 时钟与时驱动解耦，护栏恒有界。

#![allow(dead_code)]

use std::future::Future;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::time::{Duration, Instant};

/// 等待条件成立的上限（远小于 job 超时，远大于正常抖动）。
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// 等待 future 完成的上限（任务终止 / 停机返回）。
const CALL_LIMIT: Duration = Duration::from_secs(10);

/// 有界等待条件成立：OS 时钟截止 + 让渡轮询（禁 sleep 定时猜）。
pub async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT_LIMIT;
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "等待 {WAIT_LIMIT:?} 仍未见「{what}」：不变量可能已被破坏，或运行时已被自旋饿死（有界等待，禁挂死）"
        );
        tokio::task::yield_now().await;
    }
}

/// 有界等待 future 完成：独立 OS 线程用可取消的 OS 等待计时，`select!` 抢答。
///
/// future 提前完成或调用方取消本护栏时，丢弃取消发送端会立即唤醒计时线程；
/// 不再让每次正常测试都遗留一个最长睡眠 10s 的 detached 线程。
pub async fn bounded<F: Future>(what: &str, fut: F) -> F::Output {
    let (fire, fired) = tokio::sync::oneshot::channel::<()>();
    let (cancel_timer, timer_cancelled) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        if matches!(
            timer_cancelled.recv_timeout(CALL_LIMIT),
            Err(RecvTimeoutError::Timeout)
        ) {
            let _ = fire.send(());
        }
    });
    // 保留发送端至目标 future 或超时分支结束；离开函数时 drop 会取消 OS 等待。
    let _cancel_timer = cancel_timer;
    tokio::select! {
        biased;
        output = fut => output,
        _ = fired => panic!(
            "等待 {CALL_LIMIT:?} 仍未见「{what}」：不变量可能已被破坏，或运行时已被自旋饿死（有界等待，禁挂死）"
        ),
    }
}
