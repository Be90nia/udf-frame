//! 句柄生命周期集成测试：#55（detach 泄漏 TaskCell）+ #72（丢 WorkerHandle
//! 不关闸 ⇒ 驱动线程与调度器双泄漏）。
//!
//! 时间线：确定性面走 [`MockClock`] + [`Scheduler::tick`]（零真实 sleep）；
//! 驱动线程面走 [`SystemClock`] 真实时钟，判定用**握手**（channel）或
//! **带上限轮询**（禁 sleep 定时猜）。
//!
//! feature 面：scheduler 域为 std-ext 十三域零门常驻件（本文件只触及
//! scheduler 面，无 json/log-embed/random 依赖）⇒ 无需文件头 feature gate。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use skeleton::clock::{MockClock, SystemClock};
use std_ext::scheduler::Scheduler;

/// 析构探针：回调捕获图随 TaskCell 回收而析构的**外部可观测量**
/// （TaskCell 私有，集成面靠捕获的析构次数反证分配是否回收）。
struct DropProbe {
    drops: Arc<AtomicU64>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 带上限轮询（禁 sleep 定时猜）：条件成立即返真，超时判负。
fn wait_until(timeout: Duration, mut cond: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    loop {
        if cond() {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        std::thread::yield_now();
    }
}

// ── #55：detach 抑制 cancel，但不得泄漏 TaskCell 分配 ──

#[test]
fn detach_does_not_leak_task_cell_allocation() {
    let drops = Arc::new(AtomicU64::new(0));
    let probe = Arc::new(DropProbe {
        drops: Arc::clone(&drops),
    });
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    let handle = sched
        .after(Duration::from_secs(3600), move || {
            let _ = &*probe; // 捕获探针（任务远期，点火前不析构）
        })
        .expect("未关闸注册必成");
    handle.detach();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "任务未点火：捕获仍在（断言非恒真）"
    );
    sched.close();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "detach 释放句柄后 TaskCell 必被回收（回调捕获随之析构一次）"
    );
}

#[test]
fn detach_does_not_leak_periodic_task_capture() {
    let drops = Arc::new(AtomicU64::new(0));
    let probe = Arc::new(DropProbe {
        drops: Arc::clone(&drops),
    });
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    let handle = sched
        .every(Duration::from_secs(3600), move || {
            let _ = &*probe; // 周期任务的回调与捕获图
        })
        .expect("未关闸注册必成");
    handle.detach();
    sched.close();
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "周期任务的回调与捕获图必随 TaskCell 回收（不得永久留存）"
    );
}

#[test]
fn detach_does_not_cancel_the_task() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .after(Duration::from_millis(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    clock.advance(Duration::from_millis(10));
    assert_eq!(sched.tick(), 1, "detach 不触发取消：任务照常点火");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn dropping_handle_still_cancels_the_task() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    let handle = sched
        .after(Duration::from_millis(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成");
    drop(handle);
    clock.advance(Duration::from_millis(10));
    assert_eq!(sched.tick(), 0, "句柄 Drop = cancel（RAII 语义不回归）");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

// ── #72：丢弃 WorkerHandle 必须关闸，驱动线程随之退出 ──

#[test]
fn dropping_worker_handle_closes_scheduler_and_releases_it() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let weak = Arc::downgrade(&sched);
    let worker = sched.spawn_worker();
    drop(worker);
    assert!(
        sched.is_closed(),
        "丢弃 WorkerHandle 必关闸（否则再无公开句柄能关它）"
    );
    drop(sched);
    assert!(
        wait_until(Duration::from_secs(5), || weak.upgrade().is_none()),
        "驱动线程退出并释放其 Arc<Scheduler>（否则线程+调度器双泄漏）"
    );
}

#[test]
fn dropping_worker_handle_inside_callback_does_not_deadlock() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let weak = Arc::downgrade(&sched);
    let worker = sched.spawn_worker();
    let (tx, rx) = mpsc::channel::<()>();
    sched
        .after(Duration::ZERO, move || {
            drop(worker); // 回调内丢句柄：禁自 join 死锁
            let _ = tx.send(());
        })
        .expect("未关闸注册必成")
        .detach();
    rx.recv_timeout(Duration::from_secs(5))
        .expect("回调应执行（丢句柄不得自等死锁）");
    drop(sched);
    assert!(
        wait_until(Duration::from_secs(5), || weak.upgrade().is_none()),
        "回调内丢句柄后驱动线程退出并释放调度器"
    );
}

#[test]
fn dropping_worker_handle_during_inflight_callback_stops_worker() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let weak = Arc::downgrade(&sched);
    let (started_tx, started_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    // Receiver 非 Sync ⇒ 入回调需包 Mutex（回调要求 Send + Sync）。
    let release_rx = Arc::new(std::sync::Mutex::new(release_rx));
    let worker = sched.spawn_worker();
    sched
        .after(Duration::ZERO, move || {
            let _ = started_tx.send(());
            let _ = release_rx.lock().expect("test gate").recv(); // 在途回调占住 worker
        })
        .expect("未关闸注册必成")
        .detach();
    started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("在途回调应进入");
    drop(worker);
    assert!(sched.is_closed(), "丢句柄即关闸");
    assert!(
        sched.after(Duration::ZERO, || {}).is_err(),
        "关闸后注册显式拒绝（fail-visible）"
    );
    release_tx.send(()).expect("放行在途回调");
    drop(sched);
    assert!(
        wait_until(Duration::from_secs(5), || weak.upgrade().is_none()),
        "在途回调返回后驱动线程退出并释放调度器"
    );
}

#[test]
fn explicit_join_still_closes_and_waits_for_worker_exit() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let worker = sched.spawn_worker();
    worker.join();
    assert!(sched.is_closed(), "join = close + join（语义不回归）");
    assert!(
        sched.after(Duration::ZERO, || {}).is_err(),
        "join 后关闸，注册显式拒绝"
    );
}
