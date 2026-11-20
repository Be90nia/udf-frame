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

use skeleton::{EffectId, EffectKind};

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（同 runtime::lock 纪律）。
fn lock_audit<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("audit sink lock poisoned: prior panic while holding the lock")
}

/// 审计事件：单笔 dispatch 生命周期的一个节点。
///
/// 生命周期重建协议：记账单位是 **action 而非 dispatch**——一笔 dispatch 的
/// 主 action 走 `Enqueued(depth) → Reduced → [EffectForwarded* |
/// EffectInterrupted] → Notified?`；Emit 回灌派生的每个动作**各走一条同形
/// 链**，交错嵌进主链时间线（派生 `Enqueued(depth+1)` 在外层 `Reduced` 后
/// 入账，外层 `Notified` 先于派生 `Reduced`），如带状态订阅者的实测形
/// `Enqueued(0)→Reduced→Enqueued(1)→Notified→Reduced→Notified`（回归锚：
/// `tests/journal_port.rs` snapshot_restore_seeds_* 交错链断言）。多订阅者
/// 之间的逐字交错顺序不可预推（与订阅/转发时点耦合）；单订阅者内仅
/// per-action `seq` 递增确定——时间线断言用语义不变量（深度链/节点计数），
/// 勿钉逐字全局顺序。`Notified`
/// 仅当拍确有**状态快照订阅者**收到快照时记录（事件订阅不开此闸）。病理终点
/// 两类：`ReducerInterrupted`（reduce 中断，
/// 后继无 Reduced）与 `FeedbackBudgetExceeded`（回灌环预算止步）。`seq` 单调；
/// 订阅窗内**连续**，空洞 = 有界环溢出丢最旧的显式证据（禁静默重写历史的对账键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEvent {
    /// 进程内单调序号（store 级计数器，跨订阅窗不重复）。
    pub seq: u64,
    /// 生命周期节点。
    pub kind: AuditEventKind,
}

/// 生命周期节点枚举（八节点：入队/reduce 完成/reduce 中断/效果转发/效果中断/
/// 通知/拒绝/回灌超限）。
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
    /// reduce 执行中断（reducer panic）：状态迁移未完成，本 action 已出队、
    /// 随栈展开丢失（不回队——出队即离队是排空环的所有权契约）；**本次调用
    /// 已产效果随未提交迁移一并丢弃（不执行、不转发）**，而更早已提交待转发
    /// 的效果原样留存、由后续排空续转（issue#44 的事务边界：只有已提交的
    /// 效果才会被执行）。panic 本体照常传播出 dispatch
    /// （fail-visible 传播语义不变），计数读侧
    /// [`crate::runtime::SyncStore::reducer_panic_count`]。
    ReducerInterrupted,
    /// 一条 `Run`/`Cancel` 效果已转发解释器（[`skeleton::Effect::Emit`] 不经此处——
    /// 回灌以 [`AuditEventKind::Enqueued`] 入账）。
    EffectForwarded,
    /// 一条效果转发解释器时解释器 panic：本条效果随转发丢弃，未转发的残留由
    /// 下一任排空续转（续转仍以 [`AuditEventKind::EffectForwarded`] 入账）——
    /// 时间线上「谁 panic、什么被续转」可归因。
    ///
    /// **携带结构化效果身份**（GitHub issue#48）：消费方据此定位「**哪一个**
    /// 效果处于 indeterminate 态（外部结果不确定）」，并把它与失败回执对齐。
    /// 五个字段缺一不可：两个 action 序号回答「谁触发、谁产出」，`effect_index`
    /// 回答「该 action 产出的第几条」，`effect_kind` + `effect_id` 回答「哪种
    /// 效果、哪个逻辑句柄」。
    ///
    /// **两个序号的语义**（store 内单调序号，**入队线性化点**分配——⚠️
    /// **序号序与全局 FIFO 序不保证恒等**：外部入队路径的序号在 `queue` 锁内
    /// 分配，而 `Effect::Emit` 回灌路径的序号在取锁**之前**分配 ⇒ 理论上可能
    /// 与并发入队的序号**交错** ⇒ 详见 `docs/reference.md` 的「已知张力」登记）：
    ///
    /// - `dispatch_action_seq`：**本次触发转发、并承接失败回执**的 action 的
    ///   入队序号（= 效果转发失败发生在哪个 action 的转发段内）。
    /// - `origin_action_seq`：**实际产出该效果**的 action 的入队序号——可能
    ///   **早于** `dispatch_action_seq`（pending 效果跨 action 存活：action A
    ///   产出的效果后缀在 A 的转发 panic 后留存，后来由 action B 的 dispatch
    ///   触发续转并再次 panic ⇒ 被中断效果的来源是 A，触发者是 B）。
    ///   同一次 action 内两者相等；跨 action 续转时**通常** `origin < dispatch`
    ///   （**不保证**恒成立——见「已知张力」）。
    ///
    /// ⚠️ 这是**入队序号**，不是「dispatch 调用发生的时刻序」——外部调用方看到
    /// 的是「我这次调用排在队列第几位」，并发下与调用时刻序不同。
    ///
    /// **不携带** `Op` / Action / payload（可能很大、不可 Clone、可能含敏感
    /// 业务数据）——事件面只留定位所需的身份信息。
    ///
    /// [`skeleton::Effect::Emit`] 不经解释器 ⇒ 不产生本事件。
    EffectInterrupted {
        /// 本次触发转发、并承接失败回执的 action 的**入队序号**。
        dispatch_action_seq: u64,
        /// 实际产出该效果的 action 的**入队序号**（可能早于
        /// `dispatch_action_seq`）。
        origin_action_seq: u64,
        /// 该效果在产出它的 action 的 reducer 输出序列中的 **0-based** 位置
        /// （计数**包含** [`skeleton::Effect::Emit`]——Emit 也占位）——它定位的是
        /// 「该 action 产出的**第几条**效果」，与效果种类无关。
        /// （[`skeleton::EffectId`] 在一个 store 内**全局唯一**，见
        /// [`skeleton::EffectId`] 的唯一性纪律。）
        effect_index: u64,
        /// 效果的解释器侧种类（[`skeleton::EffectKind`]）。
        effect_kind: EffectKind,
        /// 效果的**逻辑句柄**（[`skeleton::EffectId`]）——当前 `Run`/`Cancel`
        /// 恒 `Some`；未来无句柄的效果种类为 `None`。
        /// ⚠️ **语义随种类而变**：`Run` 的 `effect_id` 是**该效果自己的**句柄；
        /// `Cancel` 的 `effect_id` 是**被取消目标**的句柄（Cancel 效果自身没有
        /// 句柄）⇒ 因此 `effect_id` **不可**单独当作「效果身份」用——同一 store
        /// 内 `Run(id=N)` 与 `Cancel(id=N)` 会发出**相同**的 `effect_id`；两条
        /// 指向同一目标的 `Cancel` 效果也会发出**相同**的 `effect_id`（合法时序，
        /// 非误用）。
        /// **唯一标识一个效果的作用域**：在**同一个产出 action 内**，`(effect_kind,
        /// effect_id, effect_index)` 三元组唯一标识一个效果；
        /// **跨 action** 时唯一标识一个效果的是 **`(origin_action_seq, effect_index)`**
        /// （= 哪个 action 产出的第几条）——`effect_index` 是**产出 action 内**
        /// 的序号，不跨 action 唯一。
        effect_id: Option<EffectId>,
    },
    /// 状态通知已发出（本地或跨线程至少一个订阅者收到当拍快照）。
    Notified,
    /// 闭门拒绝（dispatch no-op / sender dispatch `false` 的共享计数路径）。
    Rejected,
    /// 回灌深度预算止步（自指回灌环在时间线的显式终点）。轮询式对照读侧：
    /// [`crate::runtime::SyncStore::feedback_budget_exceeded`]（计数与事件同一
    /// 触发点各计一次——环结构重建走事件流，快速巡检走计数）。
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
