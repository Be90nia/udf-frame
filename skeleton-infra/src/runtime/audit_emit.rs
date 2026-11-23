//! 审计事件流发射面：发射唯一收口 + 实时回调串行派发器（issue#65 主路线①）。
//!
//! 从 runtime 主文件切出的关注面缝（可观测域）：发射点随排空环/入队路径走
//! （调用方在 [`super`]），本域承载 gate 快路径、有界环入账、锁外实时回调与
//! 「回调内重入 dispatch 只录环不再回调」的自指递归封死。事件值面在
//! [`crate::audit`]（宿主可消费的 `AuditEvent`/`AuditEventKind`）。
//!
//! # 实时回调的投递语义（issue#65 主路线①）
//!
//! 实时回调 = **显式串行派发器**：任一时刻至多一个派发者在调用回调（回调闭包
//! `FnMut`，并发调用即数据竞争）。并发 emitter 的事件**入队而非跳过**——回调
//! 在途期间的事件进**有界待投递 FIFO**（上限 `AUDIT_PENDING_CAP`），由在途者
//! 按 `seq` 序在**所有内部锁之外**补投。
//!
//! 两类抑制**意图不同、分别文档化**（禁合并成一句话）：
//!
//! - **并发投递抑制**（本域解决）：回调在途 ⇒ 事件入队待补投，**不丢**；
//!   队列有界 ⇒ 溢出丢最旧并计入**投递丢弃计数**（实时流背压因）。
//! - **同 store 重入抑制**（`AUDIT_CB_IN_FLIGHT`）：回调内重入 dispatch 本
//!   store ⇒ 事件**只录环**、不进回调——封死「回调→dispatch→回调」自指递归
//!   （栈爆），与回灌深度预算同一病理类别。重入事件**不进待投递队列**（补投
//!   它 = 把递归禁令变成排队禁令）。
//!
//! 权威历史仍是环 + `seq`（取证走 `take_events`）；实时流与历史两套丢失证据
//! 分账（环溢出 = 容量因；投递丢弃 = 背压因）。

use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};

use super::lock;
use super::panic_cause_summary;
use super::{ActionOrigin, ActionRecord, StoreInner, record_depth, temp_drain_frame};
use crate::audit::{AuditCallback, AuditEvent, AuditEventKind, AuditSink};

/// 待投递 FIFO 上限（issue#65 主路线①）：回调在途窗口内的并发 emit 排队上限。
/// 溢出丢**最旧**并计入**投递丢弃计数**（与环溢出分账：环溢出丢历史侧，
/// 投递丢弃丢实时流）。
pub(super) const AUDIT_PENDING_CAP: usize = 1024;

/// 审计实时回调派发状态（issue#65 主路线①）：在途标志 + 有界待投递 FIFO。
///
/// **与审计环分开**：环 = 权威历史（容量因，溢出丢最旧）；本状态 = 实时投递流
/// （背压因，溢出丢最旧并计数）。两套状态互不替代。
///
/// 归属**订阅代次**（`gen`）：订阅被替换即换代次 ⇒ 旧代次排队事件作废
/// （禁泄漏进替换订阅），作废件数计入投递丢弃计数。
pub(super) struct AuditDispatch {
    /// 归属订阅代次（与槽内 `AuditSink::gen` 同步；换代次即作废旧队列）。
    gen: u64,
    /// 在途派发标记：`true` = 有线程持有本代次回调并在**锁外**调用中——此窗口
    /// 内的并发 emit 入 `pending` FIFO（不丢事件）。`false` 且回调已被摘除
    /// （panic fail-closed）⇒ 只录环不投递。
    inflight: bool,
    /// 待投递 FIFO：回调在途期间的并发 emit 事件（**有界**，按 `seq` 序在所有
    /// 内部锁之外补投）。
    pending: VecDeque<AuditEvent>,
}

impl AuditDispatch {
    /// 构造空派发状态（未订阅态：无代次、队列空、无人在途）。
    pub(super) fn new() -> Self {
        Self {
            gen: 0,
            inflight: false,
            pending: VecDeque::new(),
        }
    }
}

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
    let feedback_depth = record_depth(origin);
    audit_emit(inner, AuditEventKind::Enqueued { feedback_depth });
}

/// 订阅换代次（issue#65 主路线①）：派发状态随新代次复位——旧代次排队事件
/// 作废（禁泄漏进替换订阅，作废件数计入投递丢弃计数），在途标记复位（新代次
/// 可自行派发；旧代次在途者凭代次对账自行作废，不覆盖新代次状态）。
pub(super) fn audit_dispatch_reset<S, A, Op>(inner: &StoreInner<S, A, Op>, gen: u64) {
    let mut disp = lock(&inner.audit_dispatch);
    if disp.gen == gen {
        return;
    }
    abandon(&inner.audit_delivery_drops, &mut disp.pending);
    disp.gen = gen;
    disp.inflight = false;
}

/// 审计发射收口（issue#65 主路线①：显式串行派发器）。唯一实现在
/// [`audit_emit_prepared`]；夺租约路径的 `Enqueued` 走
/// [`audit_enqueued_as_lease_winner`]（多一次临时排空帧，见 issue#59）。
///
/// 未订阅 = 一次 gate load 即返；订阅态 = 有界环入账（`seq` 在入环临界区内
/// 分配 ⇒ 环序 = `seq` 序；溢出丢最旧）+ 实时回调**串行派发**：
///
/// - 回调不在途 ⇒ 本线程成为派发者（独占回调，**锁外**调用）；
/// - 回调在途（他线程持有）⇒ 事件**入待投递 FIFO**（有界，溢出丢最旧并计入
///   投递丢弃计数），由在途者按 `seq` 序补投（并发 emit 不丢事件）；
/// - 同 store 重入（回调内 dispatch 本 store）⇒ **只录环**（自指递归封死——
///   与「并发投递抑制」是两种不同意图，见模块文档）。
///
/// 回调**锁外**调用（回调可重入本 store 的一切入口，持锁即死锁）；禁以阻塞锁
/// 替代回调（同 store 重入会死锁）。take/归还窗口内订阅被替换则宁弃旧回调禁
/// 覆盖新槽。
pub(super) fn audit_emit<S, A, Op>(inner: &StoreInner<S, A, Op>, kind: AuditEventKind) {
    audit_emit_prepared(inner, kind, None);
}

/// 夺租约路径的 `Enqueued` 投递（issue#59）：首条被 pop 的记录在**其**
/// `Enqueued` 回调投递期间即构成同栈重入上下文——帧安装与闸判是同一次 load，
/// 故不存在「判闸 false 之后订阅并发装上、回调却无帧」的窗口。归属规则见
/// [`super::temp_drain_frame`]。
pub(super) fn audit_enqueued_as_lease_winner<S, A, Op>(
    inner: &StoreInner<S, A, Op>,
    origin: ActionOrigin,
    first: Option<&ActionRecord<A>>,
) {
    audit_emit_prepared(
        inner,
        AuditEventKind::Enqueued {
            feedback_depth: record_depth(origin),
        },
        first,
    );
}

/// 闸判之后的唯一投递实现：`first = Some` ⇒ 先装临时排空帧（issue#59）再跑
/// 实时回调；帧随本函数返回（含 panic 展开）弹出。
fn audit_emit_prepared<S, A, Op>(
    inner: &StoreInner<S, A, Op>,
    kind: AuditEventKind,
    first: Option<&ActionRecord<A>>,
) {
    if !inner.audit_gate.load(Ordering::Acquire) {
        // 无订阅 = 无回调可跑 ⇒ 不装帧（零订阅稳态 dispatch 热路径零 TLS 操作）。
        return;
    }
    let _temp_frame = first.map(|first| temp_drain_frame(inner.store_id, first));
    // 同 store 重入（回调在本线程在途）：只录环不投递——封死自指递归。
    if AUDIT_CB_IN_FLIGHT.with(Cell::get) == Some(inner.store_id) {
        record(inner, kind);
        return;
    }
    let mut taken: Option<(u64, AuditCallback, AuditEvent)> = None;
    {
        let mut guard = lock(&inner.audit);
        let Some(sink) = guard.as_mut() else {
            return;
        };
        // `seq` 分配与入环同一临界区：并发 emitter 不会出现「先取得 seq、后
        // 入环」的反序；环序 = `seq` 序。原子仍取 Relaxed——序由锁临界区给。
        let seq = inner.audit_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let event = record_into(sink, seq, kind);
        // 派发状态（锁序：审计环锁 → 派发状态锁，全程一致 ⇒ 无死锁）。
        let mut disp = lock(&inner.audit_dispatch);
        if disp.gen != sink.gen {
            // 派发状态与订阅槽不同步（代次更换）⇒ 旧代次排队事件作废
            // （禁泄漏进替换订阅），作废件数计入投递丢弃计数。
            abandon(&inner.audit_delivery_drops, &mut disp.pending);
            disp.gen = sink.gen;
            disp.inflight = false;
        }
        if disp.inflight {
            // 回调在途（他线程持有）⇒ 事件入待投递 FIFO ⇒ 由在途者补投
            // （并发 emit 不丢事件）。FIFO 满 ⇒ 丢最旧 + 计数。
            if disp.pending.len() >= AUDIT_PENDING_CAP {
                disp.pending.pop_front();
                inner.audit_delivery_drops.fetch_add(1, Ordering::Relaxed);
            }
            disp.pending.push_back(event);
            return;
        }
        if let Some(cb) = sink.notify.take() {
            // 本线程成为在途派发者（独占回调 ⇒ 回调串行调用）。
            disp.inflight = true;
            taken = Some((sink.gen, cb, event));
        }
        // notify 为 None 且非在途 ⇒ 回调已摘除（panic fail-closed）：只录环。
    }
    if let Some((gen, cb, event)) = taken {
        dispatch(inner, gen, cb, event);
    }
}

/// 只录环（不投递）：同 store 重入路径——历史完整、实时流不递归。
fn record<S, A, Op>(inner: &StoreInner<S, A, Op>, kind: AuditEventKind) {
    let mut guard = lock(&inner.audit);
    let Some(sink) = guard.as_mut() else {
        return;
    };
    let seq = inner.audit_seq.fetch_add(1, Ordering::Relaxed) + 1;
    record_into(sink, seq, kind);
}

/// 入环记账（`seq` 已在环临界区内分配 ⇒ 环序 = `seq` 序；溢出丢最旧）。
fn record_into(sink: &mut AuditSink, seq: u64, kind: AuditEventKind) -> AuditEvent {
    let event = AuditEvent { seq, kind };
    if sink.ring.len() >= sink.cap {
        sink.ring.pop_front();
    }
    sink.ring.push_back(event);
    event
}

/// 在途派发者：持回调（独占）**锁外**逐件投递，并按 `seq` 序补投待投递 FIFO
/// （issue#65 主路线①：并发 emit 不丢事件）。
///
/// 回调 panic ⇒ **fail-close 本代次**：回调不归还（摘除）、本代次待投递队列
/// 作废（禁泄漏进替换订阅），计数留痕（禁静默摘除）。
fn dispatch<S, A, Op>(
    inner: &StoreInner<S, A, Op>,
    gen: u64,
    mut cb: AuditCallback,
    mut next: AuditEvent,
) {
    loop {
        // 回调调用在锁外（回调可重入本 store 的一切入口，持锁即死锁）。
        let result = {
            let _guard = AuditCallbackGuard::enter(inner.store_id);
            catch_unwind(AssertUnwindSafe(|| cb(&next)))
        };
        if let Err(payload) = result {
            // 回调 panic：fail-closed 摘除（环照录、dispatch 行为不变——观测面
            // 禁改变被观测者），成因留痕与订阅回调同一纪律（审计回调也是订阅
            // 回调：subscriber_panics / last_cause）。回调**不归还** = 摘除。
            inner.subscriber_panics.fetch_add(1, Ordering::Release);
            *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
            // 本代次 fail-close：回调**不归还**（= 摘除）+ 待投递队列作废
            // （禁泄漏进替换订阅）。代次已换 ⇒ 不动新代次状态。
            fail_close(inner, gen);
            return;
        }
        // 取下一件待投递事件（锁内）；空 ⇒ 交还回调并结束在途。
        let mut guard = lock(&inner.audit);
        let Some(sink) = guard.as_mut() else {
            // 订阅已关闭（票据 Drop）⇒ 本代次队列作废（禁静默丢）。
            fail_close(inner, gen);
            return;
        };
        if sink.gen != gen {
            // 订阅已被替换：本代次队列随旧代次作废（不投递到替换订阅；旧队列
            // 的作废计数由换代次方记）。
            return;
        }
        let mut disp = lock(&inner.audit_dispatch);
        if disp.gen != gen {
            return;
        }
        match disp.pending.pop_front() {
            Some(ev) => {
                next = ev;
            }
            None => {
                disp.inflight = false;
                if sink.notify.is_none() {
                    sink.notify = Some(cb);
                }
                return;
            }
        }
    }
}

/// 本代次 fail-close：在途标记落闸 + 待投递队列作废（禁泄漏进替换订阅）。
/// 代次已换 ⇒ 不动新代次状态（凭代次对账）。
fn fail_close<S, A, Op>(inner: &StoreInner<S, A, Op>, gen: u64) {
    let mut disp = lock(&inner.audit_dispatch);
    if disp.gen != gen {
        return;
    }
    disp.inflight = false;
    abandon(&inner.audit_delivery_drops, &mut disp.pending);
}

/// 作废待投递队列（禁泄漏进替换订阅）：件数计入投递丢弃计数（禁静默丢）。
fn abandon(drops: &AtomicU64, pending: &mut VecDeque<AuditEvent>) {
    let n = pending.len() as u64;
    pending.clear();
    if n > 0 {
        drops.fetch_add(n, Ordering::Relaxed);
    }
}
