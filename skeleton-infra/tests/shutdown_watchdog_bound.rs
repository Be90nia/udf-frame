//! shutdown 域 watchdog 容量闸回归（issue #54）：超时 abandon 后仍在跑的
//! watchdog 闭包数有硬上界；许可耗尽时该组件**未启动**显式上报（不 spawn 线程、
//! 不内联跑闭包）；许可随线程退出归还（正常完成/panic/超时各按真实线程寿命）。
//!
//! 观测纪律（禁「sleep 固定时长后假定状态」的猜时序）：容量计数直接读协调器账本
//! （live/peak，账本写序在 spawn 前、归还先于上报）；阻塞闭包用闸门（gate）握手
//! 放行；只有「等许可归还」用**有界轮询**（事件必到 + deadline 兜底，超窗即失败
//! 不挂死测试套）。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use skeleton_infra::shutdown::{OutcomeKind, ShutdownCoordinator};

// ---- 助手 ----

/// 闸门：闭包在其上阻塞，测试显式开闸放行（确定性握手）。
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    /// 阻塞直到开闸（未开闸则永不返回——模拟挂死的组件闭包）。
    fn wait(&self) {
        let mut g = self.open.lock().unwrap();
        while !*g {
            g = self.cv.wait(g).unwrap();
        }
    }

    /// 开闸放行全部等待者。
    fn release(&self) {
        let mut g = self.open.lock().unwrap();
        *g = true;
        self.cv.notify_all();
    }
}

/// 有界轮询：等条件成立（事件必到，deadline 兜底——超窗返回 false ⇒ 测试失败，
/// 不挂死）。禁「sleep 固定时长后假定状态已变」的写法。
fn wait_until(mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if cond() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// 捕获值守卫：析构时回调协调器——验证捕获值析构**不在协调器锁持有期内**发生
/// （锁持有则 `is_shutdown()` 自死锁，测试以有界等待判失败，不挂死测试套）。
struct DropSpy {
    coord: Arc<ShutdownCoordinator>,
    dropped: Arc<AtomicBool>,
}

impl Drop for DropSpy {
    fn drop(&mut self) {
        let _ = self.coord.is_shutdown(); // 协调器持锁则此处自死锁 → 测出即失败
        self.dropped.store(true, Ordering::SeqCst);
    }
}

// ---- 容量闸：许可耗尽 → 组件未启动 ----

// 坑面「watchdog 执行数无上界」：上限 N 时前 N 个超时组件（线程照跑、仍在跑）
// 占满许可，第 N+1 个组件**未启动**显式 Failed（fail-visible，非静默丢弃、非
// 内联跑、非谎报 TimedOut）；被拒组件的闭包从未执行（没 spawn 线程）。
#[test]
fn watchdog_cap_rejects_component_when_capacity_exhausted() {
    let gate = Gate::new();
    let started = Arc::new(AtomicUsize::new(0));
    let coord = ShutdownCoordinator::new().with_max_watchdogs(2);

    for i in 0..2 {
        let g = Arc::clone(&gate);
        let s = Arc::clone(&started);
        coord
            .register_with_timeout(
                format!("blocked-{i}"),
                move || {
                    s.fetch_add(1, Ordering::SeqCst);
                    g.wait(); // 永久挂起（闸不开）
                },
                Duration::from_millis(30),
            )
            .unwrap();
    }
    // 第三个：许可已满 → 未启动。
    let s = Arc::clone(&started);
    coord
        .register_with_timeout(
            "rejected",
            move || {
                s.fetch_add(1, Ordering::SeqCst);
            },
            Duration::from_millis(30),
        )
        .unwrap();

    let report = coord.shutdown();

    // 前两个：超时=放弃等待（线程照跑，仍占许可）。
    assert!(matches!(
        report.per_component[0].kind,
        OutcomeKind::TimedOut
    ));
    assert!(matches!(
        report.per_component[1].kind,
        OutcomeKind::TimedOut
    ));
    // 第三个：显式未启动（fail-visible）。
    match &report.per_component[2].kind {
        OutcomeKind::Failed { reason } => assert!(
            reason.contains("watchdog capacity exhausted"),
            "许可耗尽须显式上报未启动，实为: {reason}"
        ),
        k => panic!("许可耗尽须记 Failed（组件未启动），实为 {k:?}"),
    }
    // 两个阻塞闭包确已启动（否则「第三个没跑」不成立）。
    assert!(
        wait_until(|| started.load(Ordering::SeqCst) >= 2),
        "两个阻塞闭包未启动"
    );
    // 被拒组件的闭包从未执行（既没 spawn 线程、也没内联跑）。
    assert_eq!(
        started.load(Ordering::SeqCst),
        2,
        "被拒组件的闭包被执行了（应未启动）"
    );
    // 账本读数：两个超时后仍在跑的 watchdog 仍计入 live，未超上限。
    assert_eq!(coord.live_watchdogs(), 2);
    assert_eq!(coord.peak_watchdogs(), 2);

    // 放行后许可归还（线程退出 → 守卫 Drop）。
    gate.release();
    assert!(
        wait_until(|| coord.live_watchdogs() == 0),
        "阻塞闭包结束后许可未归还"
    );
}

// ---- 容量闸：许可归还 → 后续组件可启动 ----

// 许可随线程退出归还：放行阻塞闭包后许可归还，后续组件可正常启动（watchdog
// 路径 Stopped）——容量闸不误伤后续组件。
#[test]
fn watchdog_permit_returned_after_blocked_closure_ends_and_next_component_starts() {
    let gate = Gate::new();
    let coord = Arc::new(ShutdownCoordinator::new().with_max_watchdogs(2));

    for i in 0..2 {
        let g = Arc::clone(&gate);
        coord
            .register_with_timeout(
                format!("blocked-{i}"),
                move || g.wait(),
                Duration::from_millis(30),
            )
            .unwrap();
    }
    // 第三个：许可已满 → 未启动。
    coord
        .register_with_timeout("rejected", || {}, Duration::from_millis(30))
        .unwrap();
    // 第四个：内联组件（不占 watchdog 许可）——开闸放行并等许可归还。
    let freed = Arc::new(AtomicBool::new(false));
    let c = Arc::clone(&coord);
    let g = Arc::clone(&gate);
    let f = Arc::clone(&freed);
    coord
        .register("release", move || {
            g.release();
            f.store(wait_until(|| c.live_watchdogs() == 0), Ordering::SeqCst);
        })
        .unwrap();
    // 第五个：许可已归还 → 正常启动（watchdog 路径 Stopped）。
    coord
        .register_with_timeout("after", || {}, Duration::from_millis(500))
        .unwrap();

    let report = coord.shutdown();

    assert!(matches!(
        report.per_component[0].kind,
        OutcomeKind::TimedOut
    ));
    assert!(matches!(
        report.per_component[1].kind,
        OutcomeKind::TimedOut
    ));
    match &report.per_component[2].kind {
        OutcomeKind::Failed { reason } => assert!(
            reason.contains("watchdog capacity exhausted"),
            "许可耗尽须显式上报未启动，实为: {reason}"
        ),
        k => panic!("许可耗尽须记 Failed（组件未启动），实为 {k:?}"),
    }
    assert!(
        freed.load(Ordering::SeqCst),
        "开闸后许可未归还（live 未归零）"
    );
    assert_eq!(report.per_component[3].kind, OutcomeKind::Stopped);
    assert_eq!(report.per_component[4].kind, OutcomeKind::Stopped);
    assert_eq!(coord.live_watchdogs(), 0);
    assert_eq!(coord.peak_watchdogs(), 2);
}

// ---- 许可按真实线程寿命释放/保留 ----

// 正常完成与 panic 都归还许可（线程退出即归还，先于上报）；串行执行下同一时刻
// 只有一个 watchdog 在跑 → 峰值 1。
#[test]
fn watchdog_permit_released_on_normal_completion_and_panic() {
    let coord = ShutdownCoordinator::new().with_max_watchdogs(1);
    coord
        .register_with_timeout("ok", || {}, Duration::from_millis(500))
        .unwrap();
    coord
        .register_with_timeout("boom", || panic!("炸了"), Duration::from_millis(500))
        .unwrap();

    let report = coord.shutdown();

    assert_eq!(report.per_component[0].kind, OutcomeKind::Stopped);
    match &report.per_component[1].kind {
        OutcomeKind::Failed { reason } => assert!(
            reason.contains("炸了"),
            "panic 载荷须进报告，实为: {reason}"
        ),
        k => panic!("panic 须记 Failed，实为 {k:?}"),
    }
    assert_eq!(coord.live_watchdogs(), 0, "许可未归还");
    assert_eq!(coord.peak_watchdogs(), 1, "峰值应=串行执行下的 1");
}

// 被拒组件（未启动）的捕获值析构发生在协调器锁释放之后（锁持有则析构里的
// `is_shutdown()` 自死锁 → 有界等待判失败，不挂死测试套）。
#[test]
fn rejected_component_capture_dropped_without_coordinator_lock_held() {
    let coord = Arc::new(ShutdownCoordinator::new().with_max_watchdogs(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let spy = DropSpy {
        coord: Arc::clone(&coord),
        dropped: Arc::clone(&dropped),
    };
    coord
        .register_with_timeout(
            "never-started",
            move || drop(spy),
            Duration::from_millis(30),
        )
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let driver = {
        let c = Arc::clone(&coord);
        thread::spawn(move || {
            let report = c.shutdown();
            tx.send(report).unwrap();
        })
    };
    let report = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown 未在有界窗内完成（疑似持协调器锁析构 → 自死锁）");
    driver.join().unwrap();

    assert!(dropped.load(Ordering::SeqCst), "被拒组件的捕获值未析构");
    match &report.per_component[0].kind {
        OutcomeKind::Failed { reason } => assert!(
            reason.contains("watchdog capacity exhausted"),
            "许可耗尽须显式上报未启动，实为: {reason}"
        ),
        k => panic!("许可耗尽须记 Failed（组件未启动），实为 {k:?}"),
    }
    // 上限 0：全程未放行任何 watchdog（无线程被 spawn）。
    assert_eq!(coord.live_watchdogs(), 0);
    assert_eq!(coord.peak_watchdogs(), 0);
}

// ---- 容量闸不破坏既有重入装甲 ----

// watchdog 线程上的闭包重入 shutdown 仍就地 panic 装甲（记 Failed，非 HANG）——
// 容量闸不得改写重入判定。
#[test]
fn watchdog_cap_keeps_reentrancy_armor() {
    let coord = Arc::new(ShutdownCoordinator::new().with_max_watchdogs(2));
    let c_in = Arc::clone(&coord);
    coord
        .register_with_timeout(
            "reentrant",
            move || {
                let _ = c_in.shutdown(); // watchdog 线程上重入 → 就地 panic 装甲
            },
            Duration::from_secs(5),
        )
        .unwrap();

    let report = coord.shutdown();

    match &report.per_component[0].kind {
        OutcomeKind::Failed { reason } => assert!(
            reason.contains("重入"),
            "装甲消息须点名重入，实为: {reason}"
        ),
        k => panic!("watchdog 线程重入须记 Failed，实为 {k:?}"),
    }
    assert_eq!(coord.live_watchdogs(), 0, "许可未归还");
}
