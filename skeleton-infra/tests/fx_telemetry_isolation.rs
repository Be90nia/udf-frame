//! issue#201：托管任务 panic 的成因不得被 tracing 订阅者顶替。
//!
//! 场景：`Spawned` 句柄已被丢（oneshot 接收端随之消失）⇒ `task_body` 走 log-embed
//! 的 warn 分支。该事件的宿主订阅者 panic 时，生命周期集收到的必须**仍是原任务的
//! panic 文案**——否则停机报告把订阅者 panic 记成任务成因（原始 panic 已无处投递，
//! 从此永久丢失）。

#![cfg(all(feature = "tokio", feature = "log-embed"))]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use skeleton_infra::fx::FxScope;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

const PANIC: &str = "managed-task-original-panic";

/// 只在 `skeleton::fx` 事件上 panic 的订阅者；命中计数用来证明该事件分支**真的**
/// 被走到（否则「报告里有原 panic」可能根本没经过遥测路径 = 假绿）。
struct PanicOnFx {
    hits: Arc<AtomicUsize>,
}

impl Subscriber for PanicOnFx {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        if event.metadata().target() == "skeleton::fx" {
            self.hits.fetch_add(1, Ordering::SeqCst);
            panic!("注入：宿主订阅者在 skeleton::fx 事件上 panic");
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

#[tokio::test]
async fn lifecycle_report_keeps_original_panic_when_subscriber_panics() {
    let hits = Arc::new(AtomicUsize::new(0));
    let _subscriber = tracing::subscriber::set_default(PanicOnFx {
        hits: Arc::clone(&hits),
    });

    let scope = FxScope::new();
    let gate = Arc::new(tokio::sync::Notify::new());
    let task_gate = Arc::clone(&gate);
    let handle = scope
        .spawn_tracked(async move {
            task_gate.notified().await;
            panic!("{PANIC}");
        })
        .expect("spawn tracked");
    // 先丢句柄（oneshot 接收端消失），再放行任务：panic 结果无处投递 ⇒ 走 warn 分支。
    drop(handle);
    gate.notify_one();

    // 等任务体终局（in_flight 归零 = 包装体已结束，事件已发或已展开）。
    let deadline = Instant::now() + Duration::from_secs(5);
    while scope.in_flight() > 0 {
        assert!(Instant::now() < deadline, "任务体未在期限内终局");
        tokio::task::yield_now().await;
    }
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "句柄被丢的 panic 必须走到 skeleton::fx 的 warn 分支（否则本测试不覆盖 `#201` 场景）"
    );

    let report = scope.shutdown().await;
    assert!(!report.drain_interrupted, "外部停机仍是 canonical 报告");
    assert_eq!(
        report.panicked,
        vec![PANIC.to_owned()],
        "生命周期集必须保留原任务 panic 成因，而不是订阅者 panic"
    );
}
