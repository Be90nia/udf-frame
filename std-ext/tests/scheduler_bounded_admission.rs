//! 有界准入（#58）+ 远期取消条目回收（#52）集成测试。
//!
//! 确定性：全部走 [`MockClock`] 虚拟时刻 + [`Scheduler::tick`]（零真实
//! sleep）。远期 deadline 取 3600s 虚拟时刻且**虚拟钟从不前进**⇒「回收
//! 发生在原 deadline 之前」由「回收已发生 ∧ 虚拟钟未动」两条断言合证。
//! 跨线程面用 channel 握手 + 带上限等待（`recv_timeout`），不用 sleep
//! 定时猜。
//!
//! feature 面：scheduler 域为 std-ext 零门常驻件（本文件只触及 scheduler
//! 面，无 json/log-embed/random 依赖）⇒ 无需文件头 feature gate。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

use skeleton::clock::{Clock, MockClock, SystemClock};
use std_ext::scheduler::{AdmissionError, Scheduler};

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

/// 捕获图析构探针：任务胞回收 ⇒ 回调捕获随之析构（集成面靠析构次数
/// 反证分配是否回收）。
struct DropProbe {
    drops: Arc<AtomicU64>,
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 远期 deadline（虚拟时刻）：远期任务在原 deadline 前被回收 ⇒ 断言
/// 「已回收」时虚拟钟必未到点。
fn far() -> Duration {
    Duration::from_secs(3600)
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ── #52：远期取消条目在原 deadline 之前被回收 ──

/// issue#52 验收：大量远期 `after` 任务挂 Drop 探针 ⇒ 取消并丢句柄 ⇒ 触发
/// 清扫 ⇒ **已取消的捕获被回收、pending 任务仍在**。
#[test]
fn cancelled_far_future_entries_are_reclaimed_before_deadline() {
    let clock = Arc::new(MockClock::new());
    let t0 = clock.now_instant();
    let sched = Scheduler::new(clock.clone());
    let drops = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicU64::new(0));
    const TOTAL: usize = 64;
    const CANCELLED: usize = 48;

    let mut weak = Vec::with_capacity(TOTAL);
    let mut pending = Vec::with_capacity(TOTAL - CANCELLED);
    for i in 0..TOTAL {
        let probe = Arc::new(DropProbe {
            drops: Arc::clone(&drops),
        });
        weak.push(Arc::downgrade(&probe));
        let hits2 = Arc::clone(&hits);
        let handle = sched
            .after(far(), move || {
                let _ = &*probe; // 捕获探针（远期任务，点火前不析构）
                hits2.fetch_add(1, Ordering::SeqCst);
            })
            .expect("未关闸注册必成");
        if i < CANCELLED {
            assert!(handle.cancel(), "首次取消成功");
            drop(handle); // 取消并丢句柄 ⇒ 堆条目是捕获的唯一持有者
        } else {
            pending.push(handle);
        }
    }
    // 断言非恒真：取消前捕获全部在（回收面真生效时此断言会失败）。
    assert!(
        weak.iter().all(|w| w.upgrade().is_some()),
        "取消前：全部捕获在堆内（断言非恒真）"
    );

    sched.tick(); // 触发清扫（虚拟钟未动 ⇒ 无到期条目可点火）

    assert_eq!(
        weak.iter()
            .take(CANCELLED)
            .filter(|w| w.upgrade().is_none())
            .count(),
        CANCELLED,
        "已取消的远期条目必在原 deadline 之前被回收（不等惰性弹出）"
    );
    assert!(
        weak.iter().skip(CANCELLED).all(|w| w.upgrade().is_some()),
        "未取消的 pending 条目必仍在堆内（清扫不得误删活条目）"
    );
    assert_eq!(
        drops.load(Ordering::SeqCst),
        CANCELLED as u64,
        "被回收条目的捕获恰各析构一次（不多不少）"
    );
    assert_eq!(
        clock.now_instant(),
        t0,
        "虚拟钟未前进 ⇒ 回收发生在原 deadline 之前"
    );

    // 清扫后 pending 条目仍照常点火（堆重建未破坏活条目）。
    clock.advance(far());
    assert_eq!(sched.tick(), TOTAL - CANCELLED, "pending 条目照常点火");
    assert_eq!(hits.load(Ordering::SeqCst), (TOTAL - CANCELLED) as u64);
}

/// issue#52 验收：取消当前最早 deadline ⇒ 驱动重算 park 锚（剪掉堆顶
/// 墓碑），不等陈旧 deadline。
#[test]
fn worker_recalculates_park_deadline_after_earliest_cancelled() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    // 最早 deadline 在 5s 之后（陈旧锚）；测试窗口 1s 内必须剪掉 ⇒ 证明
    // 驱动没有「睡满陈旧 deadline」。
    let handle = sched
        .after(Duration::from_secs(5), || {})
        .expect("未关闸注册必成");
    let worker = sched.spawn_worker();
    assert!(handle.cancel(), "取消最早 deadline");
    let pruned = wait_until(Duration::from_secs(1), || sched.next_deadline().is_none());
    worker.join();
    assert!(
        pruned,
        "驱动须重算 park 锚：堆顶墓碑被剪除（不等陈旧 deadline）"
    );
}

// ── #58：有界准入 ──

/// issue#58 验收：容量 N 恰好收 N 条活跃注册，第 N+1 条 typed `Full` 拒绝
/// （不部分插入）。
#[test]
fn capacity_accepts_exactly_n_then_rejects_with_typed_full() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    assert_eq!(sched.capacity(), 0, "默认无界（不盲设有界默认值）");
    sched.set_capacity(3);
    let mut held = Vec::new();
    for i in 0..3 {
        let handle = sched
            .try_after(far(), || {})
            .unwrap_or_else(|e| panic!("第 {i} 条必收（容量内）：{e:?}"));
        held.push(handle); // 句柄须存活：Drop = cancel（RAII）⇒ 会释放额度
    }
    assert_eq!(sched.live_registrations(), 3, "活跃注册数 = 已收条数");
    assert!(
        matches!(sched.try_after(far(), || {}), Err(AdmissionError::Full)),
        "第 4 条 typed Full（显式拒绝，不静默丢）"
    );
    assert_eq!(
        sched.live_registrations(),
        3,
        "被拒注册不占额度（不部分插入）"
    );
    assert_eq!(sched.rejected_registrations(), 1, "拒绝次数可观测");
    assert!(
        sched.try_every(far(), || {}).is_err(),
        "周期注册同样受上界约束"
    );
}

/// issue#58 验收：一次性任务点火 ⇒ 额度释放；周期任务重臂 ⇒ 额度保留。
#[test]
fn firing_one_shot_releases_slot_and_periodic_retains_it() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched.set_capacity(1);
    let once = sched.try_after(ms(10), || {}).expect("第一条必收");
    assert_eq!(sched.live_registrations(), 1);
    assert!(
        matches!(sched.try_after(ms(10), || {}), Err(AdmissionError::Full)),
        "额度占满 ⇒ 拒绝"
    );
    clock.advance(ms(10));
    assert_eq!(sched.tick(), 1, "一次性点火");
    assert_eq!(sched.live_registrations(), 0, "一次性点火 ⇒ 额度释放");
    drop(once); // 终态句柄 Drop-cancel 无效（不重复释放）

    let periodic = sched.try_every(ms(10), || {}).expect("额度释放后可注册");
    assert_eq!(sched.live_registrations(), 1);
    clock.advance(ms(10));
    assert_eq!(sched.tick(), 1, "周期首火并重臂");
    assert_eq!(sched.live_registrations(), 1, "周期任务重臂 ⇒ 额度保留");
    assert!(
        matches!(sched.try_after(ms(10), || {}), Err(AdmissionError::Full)),
        "周期任务占额度 ⇒ 拒绝"
    );
    drop(periodic);
    assert_eq!(sched.live_registrations(), 0, "周期任务取消 ⇒ 额度释放");
}

/// issue#58 验收：执行中取消的周期任务**恰好释放一次**额度且不再重臂。
#[test]
fn periodic_cancelled_while_running_releases_slot_once_and_is_not_rearmed() {
    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    sched.set_capacity(1);
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let handle = sched
        .try_every(ms(10), move || {
            let _ = entered_tx.send(());
            let _ = release_rx.lock().expect("test gate").recv();
        })
        .expect("第一条必收");
    clock.advance(ms(10));
    let s = Arc::clone(&sched);
    let ticker = std::thread::spawn(move || s.tick());
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("回调应入场");
    assert_eq!(
        sched.live_registrations(),
        1,
        "执行中的周期任务仍占额度（它可能被重臂）"
    );
    assert!(handle.cancel(), "执行中取消：CAS 成功");
    release_tx.send(()).expect("放行在途回调");
    ticker.join().expect("tick 线程无 panic");
    assert_eq!(
        sched.live_registrations(),
        0,
        "执行中取消 ⇒ 额度恰好释放一次（不重复释放）"
    );
    clock.advance(ms(10));
    assert_eq!(sched.tick(), 0, "取消后不再重臂");
    assert_eq!(sched.live_registrations(), 0, "不重臂 ⇒ 不再占额度");
}

/// issue#58 验收：close 释放全部堆内注册额度，句柄侧仍见 Cancelled。
#[test]
fn close_releases_all_slots_and_marks_handles_cancelled() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched.set_capacity(4);
    let once = sched.try_after(far(), || {}).expect("必收");
    let periodic = sched.try_every(far(), || {}).expect("必收");
    assert_eq!(sched.live_registrations(), 2);
    sched.close();
    assert_eq!(sched.live_registrations(), 0, "close 释放全部堆内注册额度");
    assert!(
        !once.cancel(),
        "close 已把 pending 标 Cancelled（句柄侧可见）"
    );
    assert!(
        !periodic.cancel(),
        "close 已把 pending 标 Cancelled（句柄侧可见）"
    );
    assert!(
        matches!(sched.try_after(far(), || {}), Err(AdmissionError::Closed)),
        "关闸后注册 typed Closed"
    );
}

/// issue#58 验收：并发注册/取消下活跃数**不越上界**且计数精确。
#[test]
fn concurrent_registrations_never_exceed_capacity() {
    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    sched.set_capacity(8);
    const THREADS: usize = 4;
    const PER_THREAD: usize = 16;
    let accepted = Arc::new(AtomicU64::new(0));

    let mut joiners = Vec::new();
    for _ in 0..THREADS {
        let sched = Arc::clone(&sched);
        let accepted = Arc::clone(&accepted);
        joiners.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            for _ in 0..PER_THREAD {
                match sched.try_after(far(), || {}) {
                    Ok(handle) => {
                        accepted.fetch_add(1, Ordering::SeqCst);
                        assert!(sched.live_registrations() <= 8, "活跃注册数不得越过上界");
                        local.push(handle);
                    }
                    Err(AdmissionError::Full) => {} // 过载显式拒绝（预期）
                    Err(other) => panic!("意外拒绝：{other:?}"),
                }
            }
            local
        }));
    }
    let mut all = Vec::new();
    for joiner in joiners {
        all.extend(joiner.join().expect("注册线程无 panic"));
    }
    let accepted_total = accepted.load(Ordering::SeqCst);
    assert!(
        accepted_total <= 8,
        "并发注册下收下的条数不越上界（实收 {accepted_total}）"
    );
    assert_eq!(
        sched.live_registrations() as u64,
        accepted_total,
        "活跃数 = 已收条数（此刻均未退役）"
    );
    assert_eq!(
        sched.rejected_registrations(),
        (THREADS * PER_THREAD) as u64 - accepted_total,
        "拒绝计数 = 尝试数 - 收下数"
    );
    for handle in all {
        handle.cancel();
    }
    assert_eq!(sched.live_registrations(), 0, "全部取消 ⇒ 额度归零");
}

/// issue#58 验收：被拒回调的捕获在**堆锁外**析构（析构里再取状态锁 ⇒ 若
/// 在锁内则同线程重入 std 锁 ⇒ 死锁；用带上限等待把死锁判负）。
#[test]
fn rejected_callback_capture_is_dropped_outside_heap_mutex() {
    /// 析构里再取一次状态锁的探针。
    struct ReentrantProbe {
        sched: Arc<Scheduler>,
    }
    impl Drop for ReentrantProbe {
        fn drop(&mut self) {
            let _ = self.sched.live_registrations();
        }
    }

    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    sched.set_capacity(1);
    let _keep = sched.try_after(far(), || {}).expect("第一条必收");
    let (tx, rx) = mpsc::channel::<()>();
    let sched2 = Arc::clone(&sched);
    let joiner = std::thread::spawn(move || {
        let probe = ReentrantProbe {
            sched: Arc::clone(&sched2),
        };
        let result = sched2.try_after(far(), move || {
            let _ = &probe;
        });
        assert!(matches!(result, Err(AdmissionError::Full)), "第二条必被拒");
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(Duration::from_secs(5)).is_ok(),
        "被拒回调的捕获析构不得持堆锁（否则同线程重入死锁）"
    );
    joiner.join().expect("注册线程无 panic");
}

/// issue#58 验收：拒绝不得静默丢任务——被拒回调既不入堆也不点火，且既有
/// `after`/`every` 触顶时 fail-visible panic。
#[test]
fn rejected_task_never_fires_and_legacy_api_fails_visibly() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched.set_capacity(1);
    let _keep = sched.try_after(far(), || {}).expect("第一条必收");
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    assert!(
        matches!(
            sched.try_after(far(), move || {
                hits2.fetch_add(1, Ordering::SeqCst);
            }),
            Err(AdmissionError::Full)
        ),
        "触顶 ⇒ typed Full"
    );
    clock.advance(far());
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "被拒任务不入堆 ⇒ 永不点火（拒绝显式，非静默丢）"
    );
}

/// 既有 `after` 的 fail-visible 面：触顶 ⇒ panic（禁静默丢回调）。
#[test]
#[should_panic = "scheduler registration capacity exhausted"]
fn legacy_after_panics_when_capacity_reached() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched.set_capacity(1);
    let _keep = sched.after(far(), || {}).expect("第一条必收");
    let _ = sched.after(far(), || {}); // 触顶 ⇒ panic
}
