//! log-embed 埋点断言基建：手写最小捕获 Subscriber（只用 `tracing` 核心接口，
//! 零新依赖——tracing-subscriber 的 fmt/JSON 面不在断言路径）。被各域集成
//! 测试 `#[path = "common/log_capture.rs"] mod log_capture;` 引入：每个测试
//! 目标独立编译一份本文件，未用面按目标各异，故整文件 allow(dead_code)。
//!
//! 注：`with_default` 为线程局部作用域——被测埋点须在被测线程上发出
//! （shutdown watchdog 线程上的组件闭包不可见；组件终态/报告事件在调用方
//! 线程发出，可捕获）。无 span 面（`new_span` 恒 Id(1)，本基建只收事件）。

#![allow(dead_code)]

use std::fmt;
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Record};
use tracing::{Event, Id, Level, Metadata, Subscriber};

/// 单条捕获事件：target/级别/message/字段表（值一律字符串化=断言面）。
#[derive(Debug, Clone)]
pub struct CapturedEvent {
    pub target: String,
    pub level: Level,
    pub message: String,
    pub fields: Vec<(String, String)>,
}

impl CapturedEvent {
    /// 取字段值（数值字段经 record_debug 字符串化，如 `Some("1")`）。
    pub fn field(&self, name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// 事件汇：Clone 句柄在 `with_default` 作用域外读取。
#[derive(Debug, Clone, Default)]
pub struct Capture(Arc<Mutex<Vec<CapturedEvent>>>);

impl Capture {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscriber(&self) -> CaptureSubscriber {
        CaptureSubscriber {
            events: Arc::clone(&self.0),
        }
    }

    pub fn events(&self) -> Vec<CapturedEvent> {
        self.0.lock().unwrap().clone()
    }
}

/// 断言助手：恰有一条 `target` 事件并返回之（多条/零条都算测试失败）。
pub fn only(capture: &Capture, target: &str) -> CapturedEvent {
    let events: Vec<CapturedEvent> = capture
        .events()
        .into_iter()
        .filter(|e| e.target == target)
        .collect();
    assert_eq!(
        events.len(),
        1,
        "target {target:?} 应恰一条事件，实为 {}: {:?}",
        events.len(),
        capture.events()
    );
    events.into_iter().next().unwrap()
}

/// 捕获 Subscriber：全量感兴趣、无 span 状态、事件字符串化入汇。
#[derive(Debug)]
pub struct CaptureSubscriber {
    events: Arc<Mutex<Vec<CapturedEvent>>>,
}

struct FieldVisitor {
    message: String,
    fields: Vec<(String, String)>,
}

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields
                .push((field.name().to_owned(), value.to_owned()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields
                .push((field.name().to_owned(), format!("{value:?}")));
        }
    }
}

impl Subscriber for CaptureSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _attrs: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut visitor = FieldVisitor {
            message: String::new(),
            fields: Vec::new(),
        };
        event.record(&mut visitor);
        let metadata = event.metadata();
        self.events.lock().unwrap().push(CapturedEvent {
            target: metadata.target().to_owned(),
            level: *metadata.level(),
            message: visitor.message,
            fields: visitor.fields,
        });
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}
