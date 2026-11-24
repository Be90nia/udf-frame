//! 有界等待护栏（OS 时钟，**不用 tokio 计时器**）——crate 内单元测试与集成测试
//! 共用同一份实现：crate 内走 `crate::test_support::guard`，集成测试走
//! `#[path = "../src/test_support/guard.rs"] mod guards;`（每个测试目标独立编译
//! 一份，未用面按目标各异，故整文件 allow(dead_code)）。本文件自包含：只用
//! std + tokio，不引用 crate 内部符号。
//!
//! 为什么不 `tokio::time::timeout`：被测任务可能在 poll 内**无 await 忙等**
//! （自旋段），它会饿死 tokio 的时间驱动 ⇒ 计时器护栏自身无界 ⇒ 测试挂死（CI 上
//! 表现成 20 分钟 job 超时，比一条红更难诊断）。
//!
//! 兜底层级（issue#102）：独立 OS 线程按 OS 截止时间等 → 到期 `fire`（唤醒
//! `bounded` 的 `select!`）→ 最多 [`ABORT_GRACE`] 宽限轮询完成位 → 仍未完成则
//! stderr 具名诊断 + `std::process::abort()`。完成位由活在**被守护线程栈上**的
//! drop 守卫置位：正常返回、断言 panic 展开、future 被弃置三条路径都置位 ⇒ 零误杀。
//! 该兜底只存在于测试面（`cfg(test)` / 集成测试模块），库内无 watchdog 线程。

#![allow(dead_code)]

use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// 等待条件成立的上限（远小于 job 超时，远大于正常抖动）。
const WAIT_LIMIT: Duration = Duration::from_secs(5);
/// 等待 future 完成的上限（任务终止 / 停机返回）。
const CALL_LIMIT: Duration = Duration::from_secs(10);
/// 截止后的宽限：给「刚好走在收尾路径上」的测试留出置完成位的时间，仍不完成才 abort。
const ABORT_GRACE: Duration = Duration::from_secs(2);

/// 可取消的 OS 截止线程 + 进程级兜底：目标完成时立即唤醒并 join，不让它白睡满期限。
struct DeadlineGuard {
    worker: Option<JoinHandle<()>>,
    finished: Arc<AtomicBool>,
}

impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            // 线程只做截止等待与兜底；置位后立即退出，join 确保不遗留线程。
            // 结果无需传播：该线程不产生错误（出口只有 break / fire / abort）。
            let _ = worker.join();
        }
    }
}

/// 被守护线程的自我描述（诊断里指出是哪个测试线程卡住）。
fn describe_current_thread() -> String {
    let current = thread::current();
    let name = current.name().unwrap_or("<unnamed>").to_owned();
    format!("{name} ({:?})", current.id())
}

/// 起 OS 截止线程：`limit` 到期后 `fire`（`bounded` 的唤醒面），再宽限
/// [`ABORT_GRACE`] 等完成位；仍未完成 ⇒ 具名诊断 + 进程 abort。
fn spawn_deadline(
    what: &str,
    limit: Duration,
    fire: Option<tokio::sync::oneshot::Sender<()>>,
) -> DeadlineGuard {
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    let what = what.to_owned();
    let guarded_thread = describe_current_thread();
    let worker = thread::Builder::new()
        .name("udf-test-deadline".into())
        .spawn(move || {
            let started = Instant::now();
            let deadline = started + limit;
            // ① 等截止：park_timeout 允许虚假唤醒 ⇒ 用绝对截止时间循环，不把虚假唤醒当到期。
            while !worker_finished.load(Ordering::Acquire) {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                thread::park_timeout(deadline.saturating_duration_since(now));
            }
            if worker_finished.load(Ordering::Acquire) {
                return;
            }
            if let Some(fire) = fire {
                // 接收端已弃置（select! 竞败分支）也不影响兜底。
                let _ = fire.send(());
            }
            // ② 宽限：轮询完成位，让「正在收尾 / 正在 panic 展开」的测试先退出。
            let grace_end = Instant::now() + ABORT_GRACE;
            while !worker_finished.load(Ordering::Acquire) {
                let now = Instant::now();
                if now >= grace_end {
                    break;
                }
                // 10ms 粒度轮询：正常完成由守卫 unpark 立即叫醒，无需等满这一段。
                thread::park_timeout(grace_end.saturating_duration_since(now).min(Duration::from_millis(10)));
            }
            if worker_finished.load(Ordering::Acquire) {
                return;
            }
            // ③ 仍未完成：具名诊断（本线程的 stderr 不被 libtest 捕获）后终止进程。
            eprintln!(
                "\n[udf-test-guard] 有界护栏超时：{what}\n  \
                 上限: {limit:?}  宽限: {ABORT_GRACE:?}  已耗时: {:?}\n  \
                 被守护线程: {guarded_thread}\n  \
                 诊断: 截止后仍未完成 ⇒ 运行时线程可能被同步阻塞，或全部 worker 被占满；\n        tokio 时间驱动/调度无法推进，故护栏本身也拿不到 poll 机会。\n  \
                 处置: 进程 abort（把 job 级 20 分钟超时变成 ≈{:?} 的具名失败）。\n",
                started.elapsed(),
                limit + ABORT_GRACE,
            );
            std::process::abort();
        })
        .expect("创建 OS 截止线程失败");
    DeadlineGuard {
        worker: Some(worker),
        finished,
    }
}

/// 有界等待条件成立：OS 时钟截止 + 让渡轮询（禁 sleep 定时猜）。
///
/// 兜底：截止后若本函数所在线程始终拿不到 poll 机会（运行时被同步阻塞/占满），
/// 由其 OS 截止线程在 [`ABORT_GRACE`] 后终止进程——纯让渡轮询在这里无效，因为
/// 轮询代码本身跑不到。
pub async fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
    if ready() {
        return;
    }
    let _guard = spawn_deadline(what, WAIT_LIMIT, None);
    let deadline = Instant::now() + WAIT_LIMIT;
    while !ready() {
        assert!(
            Instant::now() < deadline,
            "等待 {WAIT_LIMIT:?} 仍未见「{what}」：不变量可能已被破坏，或运行时已被自旋饿死（有界等待，禁挂死）"
        );
        tokio::task::yield_now().await;
    }
}

/// 有界等待 future 完成：独立 OS 线程按 OS 截止时间 park，`select!` 抢答。
///
/// 代价：每次调用创建一个短生命周期 OS 线程；完成/弃置时 RAII 守卫取消并 join，
/// 不留下睡满期限的后台线程。护栏不依赖 tokio 时间驱动。
///
/// 兜底：截止 fire 后 `select!` 成功即 panic 成具名失败；若整个 runtime 已无
/// worker 能 poll 本 `select!`，则由截止线程宽限 [`ABORT_GRACE`] 后终止进程。
pub async fn bounded<F: Future>(what: &str, fut: F) -> F::Output {
    let (fire, fired) = tokio::sync::oneshot::channel::<()>();
    let guard = spawn_deadline(what, CALL_LIMIT, Some(fire));
    let output = tokio::select! {
        biased;
        output = fut => output,
        _ = fired => panic!(
            "等待 {CALL_LIMIT:?} 仍未见「{what}」：不变量可能已被破坏，或运行时已被自旋饿死（有界等待，禁挂死）"
        ),
    };
    // 提前完成：取消并 join 截止线程（超时 panic 路径同样由 Drop 清理）。
    drop(guard);
    output
}

/// 唤醒一台「无 IO driver 的普通休眠」worker 重新 park——它下次 park 会接手 IO
/// driver 并处理 tokio 时间轮。没有这一记 kick 时，自旋段占住原 driver 持有者后，
/// 全 runtime 无人在 driver 上 park，注册的 timer 永不到期（issue#102 机理）。
///
/// 只收句柄：调用方传 `FxScope::spawn_detached(async {})` 的返回值（一次性 scope，
/// 闸必开 ⇒ 不依赖被测 scope 的关闸时序），本文件因此不引用任何 crate 符号。
pub async fn kick_scheduler(what: &str, handle: tokio::task::JoinHandle<()>) {
    if let Err(err) = bounded(what, handle).await {
        panic!("scheduler kick 任务未能完成（{what}）：{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 完成位一置即取消并 join 截止线程——不让它白睡满上限（零遗留线程 / 零等待）。
    #[test]
    fn deadline_worker_is_joined_when_guard_drops_early() {
        let (fire, _fired) = tokio::sync::oneshot::channel::<()>();
        let started = Instant::now();
        drop(spawn_deadline(
            "单测：提前完成即取消截止线程",
            Duration::from_secs(10),
            Some(fire),
        ));
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "提前完成后应取消并 join 截止线程，而不是等待完整期限"
        );
    }
}
