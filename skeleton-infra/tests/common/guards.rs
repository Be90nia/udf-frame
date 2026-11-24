//! 有界等待护栏（OS 时钟，**不用 tokio 计时器**）。被集成测试
//! `#[path = "common/guards.rs"] mod guards;` 引入：每个测试目标独立编译一份
//! 本文件，未用面按目标各异，故整文件 allow(dead_code)。
//!
//! 为什么不 `tokio::time::timeout`：被测任务可能在 poll 内**无 await 忙等**
//! （自旋段），它会饿死 tokio 的时间驱动 ⇒ 计时器护栏自身无界 ⇒ 测试挂死（CI 上
//! 表现成 job 超时，比一条红更难诊断）。OS 时钟与时驱动解耦，护栏恒有界。

#![allow(dead_code)]

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// 可取消的 OS 截止线程：目标 future 提前结束时立即唤醒线程，不让它白睡满 10s。
struct DeadlineWake {
    worker: Option<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
}

impl Drop for DeadlineWake {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            // 线程只执行截止等待与 oneshot send；取消后立即退出，join 确保不遗留线程。
            let _ = worker.join();
        }
    }
}

fn spawn_deadline(fire: tokio::sync::oneshot::Sender<()>) -> DeadlineWake {
    let cancelled = Arc::new(AtomicBool::new(false));
    let worker_cancelled = Arc::clone(&cancelled);
    let worker = thread::Builder::new()
        .name("udf-test-deadline".into())
        .spawn(move || {
            let deadline = Instant::now() + CALL_LIMIT;
            loop {
                if worker_cancelled.load(Ordering::Acquire) {
                    break;
                }
                let now = Instant::now();
                if now >= deadline {
                    let _ = fire.send(());
                    break;
                }
                // park_timeout 允许虚假唤醒；用绝对截止时间循环，不能把虚假唤醒当超时。
                thread::park_timeout(deadline.saturating_duration_since(now));
            }
        })
        .expect("创建 OS 截止线程失败");
    DeadlineWake {
        worker: Some(worker),
        cancelled,
    }
}

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

/// 有界等待 future 完成：独立 OS 线程睡满期限后点火，`select!` 抢答。
///
/// 代价：每次调用起一个 detached OS 线程，正常路径下它也白睡到期限才自退——
/// 测试面可接受，换来的正是「护栏不依赖 tokio 时间驱动」。
pub async fn bounded<F: Future>(what: &str, fut: F) -> F::Output {
    let (fire, fired) = tokio::sync::oneshot::channel::<()>();
    let deadline = spawn_deadline(fire);
    let output = tokio::select! {
        biased;
        output = fut => output,
        _ = fired => panic!(
            "等待 {CALL_LIMIT:?} 仍未见「{what}」：不变量可能已被破坏，或运行时已被自旋饿死（有界等待，禁挂死）"
        ),
    };
    // 提前完成时取消 OS 截止线程；超时 panic 路径则由 Drop 同样清理。
    drop(deadline);
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_worker_is_joined_when_guard_drops_early() {
        let (fire, _fired) = tokio::sync::oneshot::channel::<()>();
        let started = Instant::now();
        drop(spawn_deadline(fire));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "提前完成后应取消并 join 截止线程，而不是等待完整 10s"
        );
    }
}
