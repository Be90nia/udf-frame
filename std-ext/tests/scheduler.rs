//! scheduler 域集成测试：坑面 armor 逐条钉死。
//!
//! 时间线全部经 [`MockClock`] 虚拟推进 + [`Scheduler::tick`] 同步点火（零真实
//! sleep，全套毫秒级完成——若实现误走真实时钟，虚拟钟不前进，逐毫秒断言即
//! 红；确定性不等式是真实 sleep 通路的证伪器，同 `tests/retry.rs` 先例）。
//! 唯 [`Scheduler::spawn_worker`] 驱动线程面用 [`SystemClock`] 真实等待——该
//! 面是真实时钟的生产形态，虚拟钟不流动 park 它无意义（模块头注「双层」节）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use skeleton::clock::{Clock, MockClock, SystemClock};
use std_ext::scheduler::Scheduler;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ---- 驱动线程面专用（SystemClock 真实等待，有界超时）----

/// 轮询等原子计数到位（1ms 步进，超时判负——测试骨架，非产品路径）。
fn wait_hits(flag: &AtomicU64, want: u64, timeout: Duration) -> bool {
    let start = Instant::now();
    while flag.load(Ordering::SeqCst) < want {
        if start.elapsed() >= timeout {
            return false;
        }
        std::thread::sleep(ms(1));
    }
    true
}

type Gate = Arc<(Mutex<bool>, Condvar)>;

fn gate() -> Gate {
    Arc::new((Mutex::new(false), Condvar::new()))
}

fn open(g: &Gate) {
    let mut b = g.0.lock().expect("test gate");
    *b = true;
    g.1.notify_all();
}

fn wait_open(g: &Gate, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let mut b = g.0.lock().expect("test gate");
    while !*b {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        let (nb, _) = g.1.wait_timeout(b, deadline - now).expect("test gate");
        b = nb;
    }
    true
}

// ---- after：一次性任务（确定性面）----

#[test]
fn after_fires_once_exactly_at_virtual_deadline() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    clock.advance(ms(99));
    assert_eq!(sched.tick(), 0, "差 1ms 不火（无提前触发）");
    clock.advance(ms(1));
    assert_eq!(sched.tick(), 1, "到点即火");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(sched.tick(), 0, "一次性任务不复火");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn zero_delay_fires_on_next_tick() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .after(Duration::ZERO, move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    assert_eq!(sched.tick(), 1, "delay=0 = 下个 tick 即火（合法语义）");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn same_deadline_fires_fifo_by_registration_seq() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    for id in 1..=3u64 {
        let order2 = Arc::clone(&order);
        sched
            .after(ms(100), move || {
                order2.lock().expect("test vec").push(id);
            })
            .expect("未关闸注册必成")
            .detach();
    }
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 3);
    assert_eq!(
        *order.lock().expect("test vec"),
        vec![1, 2, 3],
        "同 deadline 按 seq FIFO"
    );
}

#[test]
fn multiple_timers_fire_in_timeline_order() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let order: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    for (delay, id) in [(ms(50), 1), (ms(100), 2), (ms(20), 3)] {
        let order2 = Arc::clone(&order);
        sched
            .after(delay, move || {
                order2.lock().expect("test vec").push(id);
            })
            .expect("未关闸注册必成")
            .detach();
    }
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 3);
    assert_eq!(
        *order.lock().expect("test vec"),
        vec![3, 1, 2],
        "跨时间线按 deadline 升序"
    );
}

#[test]
fn next_deadline_tracks_heap_min() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    assert_eq!(sched.next_deadline(), None, "空堆无 deadline");
    let before = clock.now_instant();
    sched.after(ms(100), || {}).expect("未关闸注册必成");
    assert_eq!(sched.next_deadline(), Some(before + ms(100)));
    sched.after(ms(50), || {}).expect("未关闸注册必成");
    assert_eq!(sched.next_deadline(), Some(before + ms(50)), "取堆顶最小");
}

// ---- every：周期任务（锚定 + 超载退化固定延迟，无补发）----

#[test]
fn every_anchors_to_plan_without_drift() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .every(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "首次点火在 period 后（非注册即火）"
    );
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 1);
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 1);
    clock.advance(ms(150)); // 200→350：只到 300 一拍
    assert_eq!(sched.tick(), 1);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        3,
        "锚定 100/200/300 三拍，零漂移"
    );
}

#[test]
fn every_overrun_postpones_without_burst() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let clock2 = Arc::clone(&clock);
    let hits2 = Arc::clone(&hits);
    sched
        .every(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
            clock2.advance(ms(250)); // 回调自身超载：单次执行吃掉 2.5 个周期
        })
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 1, "t=100 首火；回调推进到 t=350");
    assert_eq!(
        sched.tick(),
        0,
        "无补发：跳空的 200/300 两拍不追帧（D14 防线）"
    );
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 1, "t=450 照常一拍（完成时刻 350 + 100 改锚）");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

#[test]
fn every_huge_forward_jump_fires_once_and_reanchors() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .every(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(1000)); // 时钟前跳（advance 大步/Instant 前跳同型）
    assert_eq!(sched.tick(), 1, "跳空 10 拍只火一次（无补发）");
    assert_eq!(sched.tick(), 0);
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 0, "重锚到跳后时刻 + period（非旧锚）");
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 1, "t=1100 照常");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
}

// ---- cancel：RAII/幂等/竞态面 ----

#[test]
fn cancel_before_deadline_never_fires_and_is_idempotent() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    let handle = sched
        .after(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成");
    assert!(handle.cancel(), "首次取消成功");
    assert!(!handle.cancel(), "幂等：二次取消 no-op 返 false");
    clock.advance(ms(500));
    assert_eq!(sched.tick(), 0);
    assert_eq!(hits.load(Ordering::SeqCst), 0);
}

#[test]
fn cancel_after_fire_returns_false() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let handle = sched.after(ms(50), || {}).expect("未关闸注册必成");
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 1);
    assert!(!handle.cancel(), "已点火（Done 终态）取消无效");
    drop(handle); // RAII drop 对终态任务 no-op
}

#[test]
fn cancel_during_execution_stops_periodic_without_rearm() {
    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    let entered = gate();
    let release = gate();
    let entered2 = Arc::clone(&entered);
    let release2 = Arc::clone(&release);
    let handle = sched
        .every(ms(50), move || {
            open(&entered2);
            wait_open(&release2, Duration::from_secs(5)); // 模拟长回调（测试骨架放行）
        })
        .expect("未关闸注册必成");
    clock.advance(ms(50));
    let t = {
        let s = Arc::clone(&sched);
        std::thread::spawn(move || s.tick())
    };
    assert!(wait_open(&entered, Duration::from_secs(5)), "回调未入场");
    assert!(handle.cancel(), "执行中取消：标志位 CAS 应成功");
    open(&release);
    t.join().expect("tick 线程无 panic");
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 0, "执行中取消后周期任务不再重臂");
}

#[test]
fn cancel_far_future_leaves_lazy_tombstone_semantics_pinned() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let handle = sched
        .after(Duration::from_secs(3600), || {})
        .expect("未关闸注册必成");
    assert!(handle.cancel());
    // 惰性墓碑（文档化语义）：死亡条目到点弹出时摘除，堆顶仍可见——
    // 驱动线程会为此多醒一拍（一次空醒，语义正确）。
    assert!(sched.next_deadline().is_some());
    assert_eq!(sched.tick(), 0);
}

// ---- close：关闸 + 清 pending + 在途跑完 ----

#[test]
fn close_rejects_registration_and_clears_pending() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    let handle = sched
        .after(ms(100), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成");
    assert!(!sched.is_closed());
    sched.close();
    assert!(sched.is_closed());
    sched.close(); // 幂等：二次 close no-op 不 panic
    assert!(
        matches!(sched.after(ms(50), || {}), Err(std_ext::scheduler::Closed)),
        "关闸后注册显式 Err（禁静默）"
    );
    clock.advance(ms(500));
    assert_eq!(sched.tick(), 0, "close 清 pending：不再点火");
    assert_eq!(hits.load(Ordering::SeqCst), 0);
    assert!(
        !handle.cancel(),
        "close 已把 pending 标 Cancelled（句柄侧可见）"
    );
}

#[test]
fn close_from_within_callback_is_reentrant_safe() {
    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    let s2 = Arc::clone(&sched);
    sched
        .every(ms(50), move || {
            let r = s2.after(ms(200), || {});
            assert!(r.is_ok(), "close 前回调内注册应成功");
            s2.close(); // 点火在锁外 ⇒ 回调内 close 不自死锁
        })
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 1, "回调跑完（含 close）");
    assert!(sched.is_closed());
    assert_eq!(sched.tick(), 0, "close 清 pending：回调内注册的任务被摘");
    clock.advance(ms(200));
    assert_eq!(sched.tick(), 0, "close 后周期任务不再重臂");
}

#[test]
fn reentrant_registration_from_callback_fires_later() {
    let clock = Arc::new(MockClock::new());
    let sched = Arc::new(Scheduler::new(clock.clone()));
    let hits = Arc::new(AtomicU64::new(0));
    let s2 = Arc::clone(&sched);
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(0), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
            s2.after(ms(100), || {}).expect("未关闸注册必成").detach();
        })
        .expect("未关闸注册必成")
        .detach();
    assert_eq!(sched.tick(), 1, "点火回调本身（锁外执行 ⇒ 重入注册不死锁）");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    clock.advance(ms(100));
    assert_eq!(sched.tick(), 1, "回调内注册的任务随后照常火");
    assert_eq!(hits.load(Ordering::SeqCst), 1, "第二次点火是空闭包不计数");
}

// ---- panic 摘除：驱动存活 ----

#[test]
fn task_panic_contained_and_counted_scheduler_survives() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(10), || panic!("boom"))
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(10));
    assert_eq!(sched.tick(), 1, "panic 回调计一次点火");
    assert_eq!(sched.panic_count(), 1, "fail-visible 留痕");
    sched
        .after(ms(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(10));
    assert_eq!(sched.tick(), 1, "panic 后调度器照常可用");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
    assert_eq!(sched.panic_count(), 1, "正常点火不追加");
}

#[test]
fn periodic_task_panic_is_removed_not_rearmed() {
    let clock = Arc::new(MockClock::new());
    let sched = Scheduler::new(clock.clone());
    sched
        .every(ms(50), || panic!("boom"))
        .expect("未关闸注册必成")
        .detach();
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 1);
    assert_eq!(sched.panic_count(), 1);
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 0, "panic 后周期任务摘除不再臂（JDK 语义）");
    clock.advance(ms(50));
    assert_eq!(sched.tick(), 0);
}

#[test]
fn worker_survives_task_panic_and_keeps_firing() {
    // worker 面的 panic 摘除：点火 catch_unwind 计数留痕，驱动线程存活续跑。
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let hits = Arc::new(AtomicU64::new(0));
    sched
        .after(ms(5), || panic!("worker-fire boom"))
        .expect("未关闸注册必成")
        .detach();
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(10), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    let worker = sched.spawn_worker();
    assert!(
        wait_hits(&hits, 1, Duration::from_secs(2)),
        "panic 后续任务照常点火（worker 存活）"
    );
    worker.join();
    assert_eq!(sched.panic_count(), 1);
}

// ---- 构造期 fail-closed ----

#[test]
#[should_panic = "溢出 Instant 表示范围"]
fn register_overflow_fails_closed() {
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    let _ = sched.after(Duration::MAX, || {});
}

#[test]
#[should_panic = "忙等风暴"]
fn every_zero_period_fails_closed() {
    let sched = Scheduler::new(Arc::new(MockClock::new()));
    let _ = sched.every(Duration::ZERO, || {});
}

// ---- spawn_worker：驱动线程（SystemClock 真实时钟面）----

#[test]
fn worker_fires_after_delay_and_joins_on_close() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let hits = Arc::new(AtomicU64::new(0));
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(20), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    let worker = sched.spawn_worker();
    assert!(
        wait_hits(&hits, 1, Duration::from_secs(2)),
        "worker 20ms 内点火"
    );
    worker.join();
    assert!(sched.is_closed(), "join = close + join");
    assert_eq!(hits.load(Ordering::SeqCst), 1);
}

#[test]
fn worker_wakes_early_on_earlier_registration() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let hits = Arc::new(AtomicU64::new(0));
    sched
        .after(Duration::from_secs(1), || {})
        .expect("未关闸注册必成")
        .detach();
    let worker = sched.spawn_worker();
    std::thread::sleep(ms(30)); // worker 已 park 到 1s 后
    let hits2 = Arc::clone(&hits);
    sched
        .after(ms(20), move || {
            hits2.fetch_add(1, Ordering::SeqCst);
        })
        .expect("未关闸注册必成")
        .detach();
    assert!(
        wait_hits(&hits, 1, Duration::from_millis(500)),
        "新注册更早 deadline 即时唤醒 worker（非睡满 1s）"
    );
    worker.join();
}

#[test]
fn worker_idle_parks_without_spin() {
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let worker = sched.spawn_worker();
    std::thread::sleep(ms(150)); // 空堆 park
    let p1 = sched.worker_passes();
    std::thread::sleep(ms(100));
    let p2 = sched.worker_passes();
    worker.join();
    assert!(
        p2 - p1 <= 2,
        "空堆 idle 100ms 驱动循环零自旋（passes 增量 {}，park 等待不应轮询）",
        p2 - p1
    );
}
