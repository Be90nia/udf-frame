//! 审计事件流（时序事后取证域）：事件值 + 汇聚槽 + 订阅票据。
//!
//! 发射点与 gate 快路径在 [`crate::runtime`]（随实现走）；本域承载可被消费的
//! 值面：`tracingx` 桥吃 [`AuditEvent`]，宿主经
//! [`crate::runtime::SyncStore::subscribe_events`] 开流、票据 Drop 关流。
//!
//! 最小 API 面：一订阅点（SyncStore 固有方法）+ 一事件枚举
//! （[`AuditEvent`]/[`AuditEventKind`]）+ 一读侧（`SyncStore::take_events`）。
//! 默认关闭零开销：未订阅路径 = 每 emit 点一次 gate load 即返，零分配零锁；
//! 不搞 serde/异步/过滤 DSL。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard};

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（同 runtime::lock 纪律）。
fn lock_audit<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("audit sink lock poisoned: prior panic while holding the lock")
}

/// 审计事件：单笔 dispatch 生命周期的一个节点。
///
/// 生命周期重建协议：一笔 dispatch = `Enqueued(depth) → Reduced →
/// [EffectForwarded* | EffectInterrupted | Enqueued(depth+1) → …] → Notified?`；`Notified` 仅当拍
/// 确有订阅者收到快照时记录。`seq` 单调；订阅窗内**连续**，空洞 = 有界环溢出
/// 丢最旧的显式证据（禁静默重写历史的对账键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEvent {
    /// 进程内单调序号（store 级计数器，跨订阅窗不重复）。
    pub seq: u64,
    /// 生命周期节点。
    pub kind: AuditEventKind,
}

/// 生命周期节点枚举（七节点：入队/reduce 完成/效果转发/效果中断/通知/拒绝/回灌超限）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditEventKind {
    /// action 入队（dispatch / [`skeleton::ActionSender::dispatch`] / Emit 回灌三路同闸）。
    /// `feedback_depth`: 0 = 外部流量；>0 = 同栈回灌链深（链上逐级 +1，回灌
    /// 环结构与预算止步点可重建，见 [`AuditEventKind::FeedbackBudgetExceeded`]）。
    Enqueued {
        /// 回灌深度（0 = 外部流量）
        feedback_depth: u64,
    },
    /// reduce 完成（状态迁移已产出，效果在缓冲）。
    Reduced,
    /// 一条 `Run`/`Cancel` 效果已转发解释器（[`skeleton::Effect::Emit`] 不经此处——
    /// 回灌以 [`AuditEventKind::Enqueued`] 入账）。
    EffectForwarded,
    /// 一条效果转发解释器时解释器 panic：本条效果随转发丢弃，未转发的残留由
    /// 下一任排空续转（续转仍以 [`AuditEventKind::EffectForwarded`] 入账）——
    /// 时间线上「谁 panic、什么被续转」可归因。
    EffectInterrupted,
    /// 状态通知已发出（本地或跨线程至少一个订阅者收到当拍快照）。
    Notified,
    /// 闭门拒绝（dispatch no-op / sender dispatch `false` 的共享计数路径）。
    Rejected,
    /// 回灌深度预算止步（自指回灌环在时间线的显式终点）。
    FeedbackBudgetExceeded,
}

/// 审计回调闭包（crate 内别名收口 type_complexity，零公共面增量）。
pub(crate) type AuditCallback = Box<dyn FnMut(&AuditEvent) + Send>;

/// 审计汇聚槽（唯一订阅点的单槽内容）：有界环 + 实时回调 + 订阅代次。
pub(crate) struct AuditSink {
    pub(crate) ring: VecDeque<AuditEvent>,
    pub(crate) cap: usize,
    /// 订阅代次：票据 Drop 凭 gen 对账，禁误杀后继订阅流。
    pub(crate) gen: u64,
    pub(crate) notify: Option<AuditCallback>,
}

/// 审计订阅票据：Drop = 关闭事件流（落闸 + 清槽，零开销恢复）。
/// `#[must_use]` 拦截裸调用（丢弃即静默关闭，与状态订阅票据同一纪律）。
#[must_use = "审计票据被丢弃即立即关闭事件流——`store.subscribe_events(..);` 裸调用 = 静默关闭；绑定持有才持续记录"]
pub struct AuditTicket {
    pub(crate) gate: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) audit: Arc<Mutex<Option<AuditSink>>>,
    pub(crate) gen: u64,
}

impl Drop for AuditTicket {
    fn drop(&mut self) {
        // 代次对账在锁内完成：现役票据才落闸清槽；旧票据（已被后订覆盖）禁动。
        let mut guard = lock_audit(&self.audit);
        if guard.as_ref().is_some_and(|s| s.gen == self.gen) {
            *guard = None;
            drop(guard);
            use std::sync::atomic::Ordering;
            self.gate.store(false, Ordering::Release);
        }
    }
}
