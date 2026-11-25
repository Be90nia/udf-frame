//! log-embed 埋点验收（TDD；`--features log-embed` 开跑）：
//!
//! ① feature 开：shutdown 域事件断言（target 精确 + level 精确 + 字段子串）——
//!    本文件 `#![cfg(feature = "log-embed")]` 门控，feature 关时整文件为空，
//!    `cargo test` 默认面零负担；
//! ② feature 关：零 tracing 依赖是构建面事实（cargo tree 证明），不进
//!    单测（测试无法观测「依赖不存在」）。
//!
//! backoff/fsx/iter 三域断言随批 5-1、scheduler/ratelimit/retry/cache 四域
//! 断言随批 5-2 拆至 std-ext/tests/log_embed.rs（本 crate 埋点面=
//! shutdown 一域 6 调用点 + fx 一域 1 调用点：panic 且句柄已被丢=warn 留痕）。
//!
//! 捕获订阅器：手写最小 `Subscriber`（std-ext 同款形态），线程作用域
//! `set_default` 挂载不动全局默认；shutdown 埋点全在调用方线程发出，无跨线程
//! 捕获面；fx 埋点由托管任务发出——current-thread rt 下任务即在测试线程被
//! 轮询，捕获面同线程可达。

#![cfg(feature = "log-embed")]

use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

#[cfg(feature = "tokio")]
use skeleton_infra::fx::FxScope;
use skeleton_infra::shutdown::{OutcomeKind, ShutdownCoordinator};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Metadata, Subscriber};

// ---- 捕获订阅器 ----

#[derive(Debug)]
struct Captured {
    target: &'static str,
    level: Level,
    fields: String,
}

#[derive(Default)]
struct Capture {
    events: Mutex<Vec<Captured>>,
}

impl Capture {
    /// 断言存在一条 target/level 精确匹配且字段全部命中的事件，否则
    /// panic 并附实际捕获面（测试自证失败原因，不留「没断到」悬案）。
    /// 匹配三分（审计 F5 消除裸 contains 的 `seq=1`⊂`seq=12` 假阳性面，
    /// 同时保留动态值字段的存在性断言）：f 含空格=多词片段 contains；
    /// f 以 `=` 结尾=字段存在性（token 前缀，值任意）；否则 token 精确等值。
    fn assert_event(&self, target: &str, level: Level, fields_contains: &[&str]) {
        let hit = |fields: &str, f: &str| {
            if f.contains(' ') {
                fields.contains(f)
            } else if f.ends_with('=') {
                fields.split(' ').any(|tok| tok.starts_with(f))
            } else {
                fields.split(' ').any(|tok| tok == f)
            }
        };
        let events = self.events.lock().expect("capture mutex").iter().any(|e| {
            e.target == target
                && e.level == level
                && fields_contains.iter().all(|f| hit(&e.fields, f))
        });
        assert!(
            events,
            "未捕获 target={target} level={level:?} fields~{fields_contains:?}；实际={:?}",
            self.events.lock().expect("capture mutex")
        );
    }
}

struct FieldWriter(String);

impl Visit for FieldWriter {
    // 静态串字段原样落（默认 record_str 转发 record_debug 会被 Debug 加引号）。
    fn record_str(&mut self, field: &Field, value: &str) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(&mut self.0, "{}={}", field.name(), value);
    }

    // 数值/布尔字段经 Visit 默认实现转发到 record_debug，字符串化统一在此。
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if !self.0.is_empty() {
            self.0.push(' ');
        }
        let _ = write!(&mut self.0, "{}={:?}", field.name(), value);
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut writer = FieldWriter(String::new());
        event.record(&mut writer);
        self.events.lock().expect("capture mutex").push(Captured {
            target: event.metadata().target(),
            level: *event.metadata().level(),
            fields: writer.0,
        });
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

type SharedCapture = Arc<Capture>;

fn capture() -> SharedCapture {
    Arc::new(Capture::default())
}

// ---- shutdown 域（target skeleton::shutdown[::phase]；流转=info / 异常终态
// =warn；事件全在调用方线程=线程作用域捕获可及）----

#[test]
fn shutdown_registry_run_outcome_report_events() {
    let cap = capture();
    let c = ShutdownCoordinator::new();
    let _guard = tracing::subscriber::set_default(cap.clone());

    // 注册：成功 info（名/注册后计数；显式超时入 timeout_ms 字段）。
    c.register_with_timeout("db", || {}, Duration::from_secs(1))
        .unwrap();
    cap.assert_event(
        "skeleton::shutdown::registry",
        Level::INFO,
        &["name=db", "count=1", "timeout_ms=1000"],
    );
    c.register("boom", || panic!("仓库炸了")).unwrap();
    cap.assert_event(
        "skeleton::shutdown::registry",
        Level::INFO,
        &["name=boom", "count=2"],
    );

    let report = c.shutdown();
    assert_eq!(report.per_component.len(), 2);
    // 逐组件开始 info（db 带超时 → timeout_ms 入字段；boom 无超时字段缺席）。
    cap.assert_event(
        "skeleton::shutdown::run",
        Level::INFO,
        &["name=db", "timeout_ms=1000"],
    );
    cap.assert_event("skeleton::shutdown::run", Level::INFO, &["name=boom"]);
    // 终态：Stopped=info，Failed=warn（reason 原文入字段）。
    cap.assert_event(
        "skeleton::shutdown::run",
        Level::INFO,
        &["name=db", "kind=stopped"],
    );
    cap.assert_event(
        "skeleton::shutdown::run",
        Level::WARN,
        &["name=boom", "kind=failed", "reason="],
    );
    // 整体报告 info（组件数/三态计数）。
    cap.assert_event(
        "skeleton::shutdown::report",
        Level::INFO,
        &["components=2", "stopped=1", "timed_out=0", "failed=1"],
    );
}

/// issue#149：`timeout_ms` 越过 `u64::MAX` 毫秒 ⇒ 饱和到 `u64::MAX`（裸 `as u64`
/// 会把 2^64 ms 回绕成 0，遥测谎报「零超时」）。注册与逐组件开始两处同口径。
#[test]
fn shutdown_timeout_ms_saturates_at_u64_max() {
    let cap = capture();
    let c = ShutdownCoordinator::new();
    let _guard = tracing::subscriber::set_default(cap.clone());
    // 2^64 ms：恰越 u64::MAX（裸转换回绕成 0），仍远在 Instant 表示范围内。
    let huge = Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1));
    let field = format!("timeout_ms={}", u64::MAX);

    c.register_with_timeout("huge", || {}, huge).unwrap();
    cap.assert_event(
        "skeleton::shutdown::registry",
        Level::INFO,
        &["name=huge", field.as_str()],
    );
    let report = c.shutdown();
    assert_eq!(
        report.per_component[0].kind,
        OutcomeKind::Stopped,
        "饱和只改遥测值：等待仍用原 Duration（组件立即完成，不因超大超时延后）"
    );
    cap.assert_event(
        "skeleton::shutdown::run",
        Level::INFO,
        &["name=huge", field.as_str()],
    );
}

#[test]
fn shutdown_timed_out_outcome_warns() {
    let cap = capture();
    let c = ShutdownCoordinator::new();
    let _guard = tracing::subscriber::set_default(cap.clone());
    c.register_with_timeout(
        "stuck",
        || thread::sleep(Duration::from_millis(400)),
        Duration::from_millis(30),
    )
    .unwrap();
    let report = c.shutdown();
    assert_eq!(report.per_component[0].kind, OutcomeKind::TimedOut);
    cap.assert_event(
        "skeleton::shutdown::run",
        Level::WARN,
        &["name=stuck", "kind=timed_out"],
    );
}

// ---- 顺序契约探针（#121 / #193）：订阅器=同步用户回调，其内重入协调器不得被挡 ----

type ProbeFn = Arc<dyn Fn() -> String + Send + Sync>;

/// 只在一个 target 上动手的探针：记录事件字段，首次命中时执行一次探针闭包并
/// 保存人读结论（闭包内自起线程 + 有界超时——探针挂死时测试在 2s 内红，不挂 CI）。
struct Probe {
    target: &'static str,
    once: AtomicBool,
    probe: ProbeFn,
    seen: Mutex<Vec<String>>,
    outcome: Mutex<Option<String>>,
}

impl Probe {
    fn seen(&self) -> Vec<String> {
        self.seen.lock().expect("probe mutex").clone()
    }

    fn verdict(&self) -> String {
        self.outcome
            .lock()
            .expect("probe mutex")
            .clone()
            .expect("探针未执行（事件未命中 target 或不可达）")
    }
}

fn probe_for(
    target: &'static str,
    probe: impl Fn() -> String + Send + Sync + 'static,
) -> Arc<Probe> {
    Arc::new(Probe {
        target,
        once: AtomicBool::new(false),
        probe: Arc::new(probe),
        seen: Mutex::new(Vec::new()),
        outcome: Mutex::new(None),
    })
}

impl Subscriber for Probe {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() != self.target {
            return;
        }
        let mut writer = FieldWriter(String::new());
        event.record(&mut writer);
        self.seen.lock().expect("probe mutex").push(writer.0);
        if self.once.swap(true, Ordering::SeqCst) {
            return;
        }
        let verdict = (self.probe)();
        *self.outcome.lock().expect("probe mutex") = Some(verdict);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

/// #121：注册埋点在**提交并放锁之后**发——订阅器内另起线程重入 `register`
/// 不得被注册锁挡死，且事件描述的是已提交的注册（`count`=提交后计数）。
#[test]
fn shutdown_registry_event_emitted_after_commit_and_unlock() {
    let c = Arc::new(ShutdownCoordinator::new());
    let probe = probe_for("skeleton::shutdown::registry", {
        let c = Arc::clone(&c);
        move || {
            let c = Arc::clone(&c);
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let _ = tx.send(c.register("from-subscriber", || {}));
            });
            match rx.recv_timeout(Duration::from_secs(2)) {
                Ok(Ok(())) => "registered".to_string(),
                Ok(Err(e)) => format!("register error: {e:?}"),
                Err(_) => {
                    "blocked：注册埋点发出时协调器锁仍被持有（重入 register 自锁）".to_string()
                }
            }
        }
    });
    let _guard = tracing::subscriber::set_default(Arc::clone(&probe));

    c.register("first", || {}).unwrap();

    assert_eq!(probe.verdict(), "registered");
    let seen = probe.seen();
    assert_eq!(seen.len(), 1, "只有成功注册发事件；实际捕获={seen:?}");
    assert!(
        seen[0].contains("name=first") && seen[0].contains("count=1"),
        "事件须描述已提交的注册（名+提交后计数），实际={:?}",
        seen[0]
    );

    // 被拒注册不发成功事件（重名 = 提交前早返）。
    assert!(c.register("first", || {}).is_err());
    assert_eq!(probe.seen().len(), 1, "被拒注册不得发事件");
}

/// #193：报告埋点在**报告提交 + `Done` + 唤醒 + 放锁之后**发——订阅器内另起
/// 线程做第二次 `shutdown()` 必须立刻拿到同一份已提交报告（而不是等本栈把
/// `Done` 置位 ⇒ 自等挂死）。
#[test]
fn shutdown_report_event_emitted_after_terminal_state() {
    let c = Arc::new(ShutdownCoordinator::new());
    c.register("only", || {}).unwrap();
    let probe = probe_for("skeleton::shutdown::report", {
        let c = Arc::clone(&c);
        move || {
            let c = Arc::clone(&c);
            let (tx, rx) = mpsc::channel();
            thread::spawn(move || {
                let _ = tx.send(c.shutdown());
            });
            match rx.recv_timeout(Duration::from_secs(2)) {
                Ok(report) => format!("second-caller-report:{}", report.per_component.len()),
                Err(_) => "blocked：报告埋点发出时终态未提交（并发叫者等 Done 挂死）".to_string(),
            }
        }
    });
    let _guard = tracing::subscriber::set_default(Arc::clone(&probe));

    let report = c.shutdown();

    assert_eq!(report.per_component.len(), 1);
    assert_eq!(
        probe.verdict(),
        "second-caller-report:1",
        "报告埋点必须在终态提交+唤醒之后发"
    );
    assert!(c.is_shutdown());
}

/// #193 第 4 条：订阅器 panic 由 embed 边界就地 contain（#225）——首叫照常拿到
/// 报告、状态机已终态、失败在计数器上可见、后续叫者拿同一份报告而不挂死。
#[test]
fn report_event_subscriber_panic_cannot_strand_terminal_state() {
    let c = Arc::new(ShutdownCoordinator::new());
    c.register("only", || {}).unwrap();
    let before = skeleton_infra::embedded_event_panic_count();
    let probe = probe_for("skeleton::shutdown::report", || {
        panic!("injected subscriber panic")
    });
    let _guard = tracing::subscriber::set_default(Arc::clone(&probe));

    let report = c.shutdown();

    assert_eq!(report.per_component.len(), 1, "首叫照常拿到报告");
    assert!(
        skeleton_infra::embedded_event_panic_count() > before,
        "订阅器 panic 必须被计数（可观察失败通道）"
    );
    assert!(c.is_shutdown(), "诊断 panic 不得回滚终态");
    assert_eq!(c.shutdown(), report, "后续叫者拿同一份已提交报告，不挂死");
}

// ---- fx 域（target skeleton::fx；panic 且句柄已被丢=异常路径 warn）----

/// 等 scope 归零（1ms 实钟轮询 + 5s 墙钟上限，同 src/fx.rs 库内 wait_idle 形——
/// current-thread rt 下 sleep 即让位驱动任务跑完，禁忙等）。
#[cfg(feature = "tokio")]
async fn wait_idle(scope: &FxScope) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if scope.in_flight() == 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "fx scope not idle: {} task(s) still in flight",
            scope.in_flight()
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// spawn panic → drop 句柄 → warn 留痕一行：panic 结果无人消费不可静默丢失
/// （任务在 wait_idle 轮询期跑完 panic 归一+投递失败，埋点与捕获同线程）。
#[cfg(feature = "tokio")]
#[tokio::test]
async fn fx_panic_with_dropped_handle_leaves_warn_trace() {
    let cap = capture();
    let scope = FxScope::new();
    let _guard = tracing::subscriber::set_default(cap.clone());
    let handle = scope
        .spawn_tracked(async { panic!("boom-unconsumed") })
        .unwrap();
    drop(handle); // 句柄先亡：结果无处投递
    wait_idle(&scope).await;
    cap.assert_event("skeleton::fx", Level::WARN, &["msg=boom-unconsumed"]);
}
