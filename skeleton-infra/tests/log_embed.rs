//! log-embed 埋点验收（TDD；`--features log-embed` 开跑）：
//!
//! ① feature 开：shutdown 域事件断言（target 精确 + level 精确 + 字段子串）——
//!    本文件 `#![cfg(feature = "log-embed")]` 门控，feature 关时整文件为空，
//!    `cargo test` 默认面零负担；
//! ② feature 关：零 tracing 依赖是构建面事实（cargo tree 证明），不进
//!    单测（测试无法观测「依赖不存在」）。
//!
//! backoff/fsx/iter 三域断言随批 5-1、scheduler/ratelimit/retry/cache 四域
//! 断言随批 5-2 拆至 std-ext/tests/log_embed.rs（本 crate 埋点面收窄为
//! shutdown 一域 6 调用点）。
//!
//! 捕获订阅器：手写最小 `Subscriber`（std-ext 同款形态），线程作用域
//! `set_default` 挂载不动全局默认；shutdown 埋点全在调用方线程发出，无跨线程
//! 捕获面。

#![cfg(feature = "log-embed")]

use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

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
