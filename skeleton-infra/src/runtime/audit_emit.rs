//! 审计事件流发射面：发射唯一收口 + 实时回调在途防护。
//!
//! 从 runtime 主文件切出的关注面缝（可观测域）：发射点随排空环/入队路径走
//! （调用方在 [`super`]），本域承载 gate 快路径、有界环入账、锁外实时回调与
//! 「回调内重入 dispatch 只录环不再回调」的自指递归封死。事件值面在
//! [`crate::audit`]（宿主可消费的 `AuditEvent`/`AuditEventKind`）。

use std::cell::Cell;
use std::panic::{catch_unwind, AssertUnwindSafe};

use super::lock;
use super::panic_cause_summary;
use super::{ActionOrigin, StoreInner};
use crate::audit::{AuditEvent, AuditEventKind};
use std::sync::atomic::Ordering;

thread_local! {
    /// 审计回调在途标记（键 = store_id）：回调内重入 dispatch 的事件只录
    /// 环不再进回调——封死「回调→dispatch→回调」自指递归（栈爆），与回灌深度
    /// 预算同一病理类别。嵌套回调（另一 store）按栈配对（prev 恢复）。
    static AUDIT_CB_IN_FLIGHT: Cell<Option<u64>> = const { Cell::new(None) };
}

/// 审计回调在途守卫：进入置位、离开（含 panic 栈展开）恢复原值。
struct AuditCallbackGuard {
    prev: Option<u64>,
}

impl AuditCallbackGuard {
    fn enter(store_id: u64) -> Self {
        Self {
            prev: AUDIT_CB_IN_FLIGHT.with(|cell| cell.replace(Some(store_id))),
        }
    }
}

impl Drop for AuditCallbackGuard {
    fn drop(&mut self) {
        AUDIT_CB_IN_FLIGHT.with(|cell| cell.set(self.prev.take()));
    }
}

/// 入队事件发射（[`audit_emit`] 的 Enqueued 收口：来源标签 → 回灌深度）。
pub(super) fn audit_enqueued<S, A, Op>(inner: &StoreInner<S, A, Op>, origin: ActionOrigin) {
    let feedback_depth = match origin {
        ActionOrigin::External => 0,
        ActionOrigin::Feedback(depth) => depth,
    };
    audit_emit(inner, AuditEventKind::Enqueued { feedback_depth });
}

/// 审计发射唯一收口：未订阅 = 一次 gate load 即返；订阅态 = 有界环入账
/// （溢出丢最旧）+ 实时回调（锁外调用——回调可重入本 store 的一切入口，
/// 持锁即死锁；take/归还窗口内订阅被替换则宁弃旧回调禁覆盖新槽）。
pub(super) fn audit_emit<S, A, Op>(inner: &StoreInner<S, A, Op>, kind: AuditEventKind) {
    if !inner.audit_gate.load(Ordering::Acquire) {
        return;
    }
    let event = AuditEvent {
        seq: inner.audit_seq.fetch_add(1, Ordering::Relaxed) + 1,
        kind,
    };
    let mut notify = {
        let mut guard = lock(&inner.audit);
        match guard.as_mut() {
            Some(sink) => {
                if sink.ring.len() >= sink.cap {
                    sink.ring.pop_front();
                }
                sink.ring.push_back(event);
                sink.notify.take()
            }
            None => return,
        }
    };
    if let Some(f) = notify.as_mut() {
        if AUDIT_CB_IN_FLIGHT.with(Cell::get) != Some(inner.store_id) {
            let _guard = AuditCallbackGuard::enter(inner.store_id);
            match catch_unwind(AssertUnwindSafe(|| f(&event))) {
                Ok(()) => {}
                // 回调 panic：fail-closed 摘除（环照录、dispatch 行为不变——观测
                // 面禁改变被观测者），成因留痕与订阅回调同一纪律（审计回调也是
                // 订阅回调：subscriber_panics / last_cause）。
                Err(payload) => {
                    inner.subscriber_panics.fetch_add(1, Ordering::Release);
                    *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
                    notify = None;
                }
            }
        }
    }
    if let Some(f) = notify {
        let mut guard = lock(&inner.audit);
        if let Some(sink) = guard.as_mut() {
            if sink.notify.is_none() {
                sink.notify = Some(f);
            }
        }
    }
}
