//! shutdown 域集成测试：坑面红绿（坑|堵法对照表见 shutdown 域性能锚（本地留存，未入库））。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use skeleton_infra::shutdown::{OutcomeKind, RegisterError, ShutdownCoordinator, ShutdownReport};

// ---- 助手 ----

type Log = Arc<Mutex<Vec<&'static str>>>;

fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn push(log: &Log, tag: &'static str) {
    log.lock().unwrap().push(tag);
}

fn order_of(log: &Log) -> Vec<&'static str> {
    log.lock().unwrap().clone()
}

fn flat(report: &ShutdownReport) -> Vec<(&str, &OutcomeKind)> {
    report
        .per_component
        .iter()
        .map(|o| (o.name.as_str(), &o.kind))
        .collect()
}

// ---- 编排序与报告完整性 ----

// 拍板「注册序串行」：执行序=注册序（依赖序常见；优先级编排不在本域）。
#[test]
fn shutdown_runs_components_in_registration_order() {
    let log = log();
    let c = ShutdownCoordinator::new();
    let l = Arc::clone(&log);
    c.register("a", move || push(&l, "a")).unwrap();
    let l = Arc::clone(&log);
    c.register("b", move || push(&l, "b")).unwrap();
    let l = Arc::clone(&log);
    c.register("c", move || push(&l, "c")).unwrap();

    let report = c.shutdown();

    assert_eq!(order_of(&log), vec!["a", "b", "c"]);
    assert_eq!(
        flat(&report),
        vec![
            ("a", &OutcomeKind::Stopped),
            ("b", &OutcomeKind::Stopped),
            ("c", &OutcomeKind::Stopped),
        ]
    );
    assert!(report.is_clean());
}

// 坑面「零组件报告完整性」：空报告合法且 is_clean（无组件无可失败）；is_shutdown 启动前假、完成后真。
#[test]
fn zero_components_yields_clean_empty_report() {
    let c = ShutdownCoordinator::new();
    assert!(!c.is_shutdown());
    let report = c.shutdown();
    assert!(report.per_component.is_empty());
    assert!(report.is_clean());
    assert!(c.is_shutdown());
}

// ---- 坑面：组件 panic ----

// 坑面「组件 panic 炸掉整体」：panic 就地捕获记 Failed（载荷消息进报告），
// 后续组件照跑（fail-visible 不传染）。
#[test]
fn component_panic_becomes_failed_and_does_not_stop_the_rest() {
    let log = log();
    let c = ShutdownCoordinator::new();
    c.register("db", || {}).unwrap();
    c.register("boom", || panic!("仓库炸了")).unwrap();
    let l = Arc::clone(&log);
    c.register("after", move || push(&l, "after")).unwrap();

    let report = c.shutdown();

    assert_eq!(order_of(&log), vec!["after"]);
    let rows = flat(&report);
    assert_eq!(rows[0], ("db", &OutcomeKind::Stopped));
    match &report.per_component[1].kind {
        OutcomeKind::Failed { reason } => {
            assert!(
                reason.contains("仓库炸了"),
                "panic 载荷须进报告，实为: {reason}"
            )
        }
        k => panic!("boom 须记 Failed，实为 {k:?}"),
    }
    assert_eq!(rows[2], ("after", &OutcomeKind::Stopped));
    assert!(!report.is_clean());
}

// ---- 坑面：超时语义 ----

// 坑面「单组件慢/挂拖垮整体」：超时=放弃等待记 TimedOut（sync 无中断面——组件
// 线程照跑，如实上报非静默跳过），后续组件照跑。
#[test]
fn timed_out_component_does_not_block_the_rest() {
    let c = ShutdownCoordinator::new();
    c.register_with_timeout(
        "stuck",
        || thread::sleep(Duration::from_millis(500)),
        Duration::from_millis(30),
    )
    .unwrap();
    c.register("quick", || {}).unwrap();

    let report = c.shutdown();

    assert_eq!(
        flat(&report),
        vec![
            ("stuck", &OutcomeKind::TimedOut),
            ("quick", &OutcomeKind::Stopped)
        ]
    );
    assert!(!report.is_clean());
}

// 超时充足时 watchdog 路径正常收口 Stopped（超时不是失败源，只是放弃等待）。
#[test]
fn generous_timeout_path_reports_stopped() {
    let c = ShutdownCoordinator::new();
    c.register_with_timeout(
        "quick",
        || thread::sleep(Duration::from_millis(1)),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(flat(&c.shutdown()), vec![("quick", &OutcomeKind::Stopped)]);
}

// 坑面「默认超时未生效」：with_default_timeout 对普通注册生效。
#[test]
fn default_timeout_applies_to_plain_register() {
    let c = ShutdownCoordinator::new().with_default_timeout(Duration::from_millis(30));
    c.register("stuck", || thread::sleep(Duration::from_millis(500)))
        .unwrap();
    assert_eq!(flat(&c.shutdown()), vec![("stuck", &OutcomeKind::TimedOut)]);
}

// 逐组件超时显式覆盖默认值（覆盖生效方向钉死）。
#[test]
fn per_component_timeout_overrides_default() {
    let c = ShutdownCoordinator::new().with_default_timeout(Duration::from_millis(30));
    c.register_with_timeout(
        "quick",
        || thread::sleep(Duration::from_millis(1)),
        Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(flat(&c.shutdown()), vec![("quick", &OutcomeKind::Stopped)]);
}

// ---- 坑面：注册守卫 ----

// 坑面「停机中注册静默丢失」：停机启动后注册显式 Err(Closed)（fail-visible）。
#[test]
fn register_during_shutdown_is_rejected() {
    let c = Arc::new(ShutdownCoordinator::new());
    let seen: Arc<Mutex<Option<Result<(), RegisterError>>>> = Arc::new(Mutex::new(None));
    let seen_in = Arc::clone(&seen);
    let c_in = Arc::clone(&c);
    c.register("first", move || {
        *seen_in.lock().unwrap() = Some(c_in.register("late", || {}));
    })
    .unwrap();

    let report = c.shutdown();

    assert_eq!(
        seen.lock().unwrap().take(),
        Some(Err(RegisterError::Closed))
    );
    assert_eq!(flat(&report), vec![("first", &OutcomeKind::Stopped)]);
}

// 坑面「close 后注册」：完成后注册同样 Err(Closed)。
#[test]
fn register_after_shutdown_is_rejected() {
    let c = ShutdownCoordinator::new();
    c.shutdown();
    assert_eq!(c.register("late", || {}), Err(RegisterError::Closed));
}

// 坑面「重名覆盖」：重名显式 Err(Duplicate)，禁静默双注册或覆盖。
#[test]
fn duplicate_name_is_rejected() {
    let c = ShutdownCoordinator::new();
    c.register("db", || {}).unwrap();
    assert_eq!(
        c.register("db", || {}),
        Err(RegisterError::Duplicate("db".into()))
    );
}

// ---- 幂等与并发 ----

// 拍板「二次 shutdown 返回首报告」：报告逐字段相等（失败结论不漂移）。
#[test]
fn second_shutdown_returns_first_report() {
    let c = ShutdownCoordinator::new();
    c.register("ok", || {}).unwrap();
    c.register("boom", || panic!("x")).unwrap();
    let first = c.shutdown();
    let second = c.shutdown();
    assert_eq!(first, second);
}

// 并发第二叫：阻塞等首叫完成拿同一报告（不与首叫竞争执行，零组件漏跑）。
#[test]
fn concurrent_shutdown_waits_for_first_and_shares_report() {
    let c = Arc::new(ShutdownCoordinator::new());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    c.register("slow", move || {
        started_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(200));
    })
    .unwrap();

    let driver = {
        let c = Arc::clone(&c);
        thread::spawn(move || c.shutdown())
    };
    started_rx.recv().unwrap(); // 首叫已在跑组件（phase=Running）
    let second = {
        let c = Arc::clone(&c);
        thread::spawn(move || c.shutdown()).join().unwrap()
    };
    let first = driver.join().unwrap();

    assert_eq!(first, second);
    assert_eq!(second.per_component.len(), 1);
}

// ---- 坑面：重入 ----

// 坑面「组件闭包内重入 shutdown 自锁」：同线程重入就地 panic 装甲，被编排面
// 捕获记 Failed（自锁变显式失败，非 HANG 无留痕），后续组件照跑。
#[test]
fn reentrant_shutdown_from_component_becomes_failed_not_deadlock() {
    let c = Arc::new(ShutdownCoordinator::new());
    let c_in = Arc::clone(&c);
    c.register("reentrant", move || {
        let _ = c_in.shutdown();
    })
    .unwrap();
    c.register("after", || {}).unwrap();

    let report = c.shutdown();

    match &report.per_component[0].kind {
        OutcomeKind::Failed { reason } => {
            assert!(
                reason.contains("重入"),
                "装甲消息须点名重入，实为: {reason}"
            )
        }
        k => panic!("重入须记 Failed，实为 {k:?}"),
    }
    assert_eq!(report.per_component[1].kind, OutcomeKind::Stopped);
}

// 坑面「watchdog 线程上组件闭包内重入 shutdown 自锁」（F-1，实弹证据
// verify/2026-10-16-b4-blind/hang-evidence.txt）：带超时组件的闭包跑在协调器
// 自 spawn 的 watchdog 线程，ThreadId 不等于驱动线程——重入落并发等待分支
// （等 Done）而非 panic；驱动在 recv_timeout 等该线程收口，timeout=MAX 档
// 双线程互等永久死锁零报告（盲评 8s 外部击杀复现）。装甲=役中 watchdog
// 线程集第二锚：重入就地 panic → 该线程 catch_unwind → Failed 上报。
// 观察窗判定（2s）防回归挂死测试套：超窗即 HANG 复现，测试失败留痕。
#[test]
fn watchdog_thread_reentrant_shutdown_becomes_failed_not_deadlock() {
    let c = Arc::new(ShutdownCoordinator::new());
    let c_in = Arc::clone(&c);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    c.register_with_timeout(
        "reentrant",
        move || {
            entered_tx.send(()).unwrap();
            let _ = c_in.shutdown(); // watchdog 线程上重入
        },
        Duration::MAX, // 等到底档：修前重入落等待分支即永久死锁
    )
    .unwrap();
    c.register("after", || {}).unwrap();

    let (tx, rx) = std::sync::mpsc::channel();
    let _driver = {
        let c = Arc::clone(&c);
        thread::spawn(move || {
            let _ = tx.send(c.shutdown());
        })
    };
    entered_rx.recv().unwrap(); // 组件闭包已在 watchdog 线程内开跑

    let report = rx.recv_timeout(Duration::from_secs(2)).unwrap_or_else(|_| {
        panic!("HANG 复现：timeout=MAX + watchdog 重入双线程死锁，2s 观察窗零报告")
    });

    match &report.per_component[0].kind {
        OutcomeKind::Failed { reason } => {
            assert!(
                reason.contains("重入"),
                "装甲消息须点名重入，实为: {reason}"
            )
        }
        k => panic!("watchdog 重入须记 Failed，实为 {k:?}"),
    }
    assert_eq!(report.per_component[1].kind, OutcomeKind::Stopped);
}

// ---- is_shutdown 语义 ----

// 启动即真（Running 含进行中——闸语义同 SyncStore::is_closed），完成仍真。
#[test]
fn is_shutdown_true_from_start_of_shutdown() {
    let c = Arc::new(ShutdownCoordinator::new());
    let seen = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&seen);
    let c_in = Arc::clone(&c);
    c.register("probe", move || {
        s.store(c_in.is_shutdown(), Ordering::SeqCst);
    })
    .unwrap();

    assert!(!c.is_shutdown());
    c.shutdown();

    assert!(seen.load(Ordering::SeqCst), "组件执行时 is_shutdown 须已真");
    assert!(c.is_shutdown());
}

// ── log-embed 埋点断言由 tests/log_embed.rs 承载（registry/run/report 全流程
// + 超时 warn 四断言齐，set_default 全局 dispatcher 形态）。本文件原
// with_default 版已删：libtest 并行下无 capture 的同 callsite 测试抢先首评估
// → tracing callsite interest 缓存 Never → with_default（不重建缓存）恒捕 0 条
// ——CI 三平台确定性红、多核本机调度序侥幸绿的脆弱形态，集中一份稳版即够。
