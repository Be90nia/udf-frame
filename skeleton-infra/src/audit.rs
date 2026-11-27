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
/// 三类：`ReducerInterrupted`（reduce 中断，
/// 后继无 Reduced）、`FeedbackBudgetExceeded`（回灌环预算止步）与
/// `SegmentEffectOverflow`（暂存超界 ⇒ 整次转换被拒，后继无 Reduced）。`seq` 单调；
/// 订阅窗内**连续**，空洞 = 有界环溢出丢最旧的显式证据（禁静默重写历史的对账键）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEvent {
    /// 进程内单调序号（store 级计数器，跨订阅窗不重复）。
    pub seq: u64,
    /// 生命周期节点。
    pub kind: AuditEventKind,
}

/// 生命周期节点枚举（九节点：入队/reduce 完成/reduce 中断/效果转发/效果中断/
/// 通知/拒绝/回灌超限/暂存超界拒绝）。
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
    /// **两个序号的语义**（store 内单调序号，**队列插入线性化点**分配——外部
    /// 准入与 `Effect::Emit` 回灌**同在 `queue` 临界区内取号**（issue#158，方案 2
    /// 拍板）⇒ **序号序 = 队列插入序**，出队（= 执行）序按插入序 ⇒ 消费方可按
    /// 序号重排 action 时间线。停放后经 `resume_deferred` 入队的记录保留**取号
    /// 时**的序号（不回溯重编号））：
    ///
    /// - `dispatch_action_seq`：**本次触发转发、并承接失败回执**的 action 的
    ///   入队序号（= 效果转发失败发生在哪个 action 的转发段内）。
    /// - `origin_action_seq`：**实际产出该效果**的 action 的入队序号——可能
    ///   **早于** `dispatch_action_seq`（pending 效果跨 action 存活：action A
    ///   产出的效果后缀在 A 的转发 panic 后留存，后来由 action B 的 dispatch
    ///   触发续转并再次 panic ⇒ 被中断效果的来源是 A，触发者是 B）。
    ///   同一次 action 内两者相等；跨 action 续转时**未停放路径**恒有
    ///   `origin < dispatch`（#158：取号序 = 队列插入序 ⇒ 先插入者序号必小）。
    ///   **例外**：停放后经 `resume_deferred` 入队的记录保留**取号时**的序号
    ///   （不回溯重编号），它与后续准入记录的「插入序 vs 序号序」可相反 ⇒
    ///   跨「停放 → 恢复」的续转仍**不保证** `origin < dispatch`。
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
    /// reducer 转换因暂存超界被**整次拒绝**（ADR-0011 §2.2）：状态未发布、该
    /// segment 的效果零转发、超量 Emit 不入停放队列（后继无 `Reduced`——本事件
    /// 就是该 action 生命周期在时间线上的终点）。轮询式对照读侧：
    /// [`crate::runtime::SyncStore::segment_effect_overflow_count`]（计数与事件
    /// 同一触发点各记一次）。
    SegmentEffectOverflow,
}

/// 审计回调闭包（crate 内别名收口 type_complexity，零公共面增量）。
pub(crate) type AuditCallback = Box<dyn FnMut(&AuditEvent) + Send>;

/// 审计汇聚槽（唯一订阅点的单槽内容）：有界环 + 实时回调 + 订阅代次。
///
/// **所有权与析构边界（issue#88）**：`notify` 里的用户回调闭包捕获的对象可自带
/// `Drop`，其析构是宿主代码——因此槽的退役**以整个槽为单位**（`Option::take` /
/// `Option::replace` 整槽移入局部变量），且在审计 mutex **外**析构（票据 Drop 与
/// 订阅替换两条路径同款）。只 take `notify` 而把其余字段留在锁内析构是同一缺陷
/// 的变体；本槽新增字段时整槽退役的纪律照样覆盖。
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
        //
        // **落闸也在锁内**（issue#60 线性化点）：槽摘除与 `gate=false` 是**一次**
        // 状态变更。旧的「先放锁再落闸」会让并发 `subscribe_events` 在窗口内装入
        // 新 sink 并开闸，随后旧票据的延迟 `false` 把新订阅静默关掉（闸 false 时
        // 发射在环入账**之前**就返回 ⇒ 实时回调与取证环一起失声）。
        // 总序论证：两侧都在这把 mutex 上 ⇒ 落闸先则新订阅随后重新开闸；装槽先
        // 则本票据代次失配、根本不碰闸。无第三种交错。热路径不变（未订阅 =
        // 一次 Acquire load 即返）；代价只有关闭路径多持一拍锁。
        //
        // **退役以整槽为单位 + 锁外析构**（issue#88）：`AuditSink` 是用户回调
        // 闭包的唯一宿主，闭包捕获对象可自带 `Drop`（宿主代码，能重入
        // `subscribe_events`/`take_events`）⇒ 锁内析构即自锁。锁内只 detach，
        // 代次对账、暂停点与落闸的线性化（#60）原样留在临界区内。
        let retired = {
            let mut guard = lock_audit(&self.audit);
            if guard.as_ref().is_some_and(|s| s.gen == self.gen) {
                let retired = guard.take();
                #[cfg(test)]
                drop_pause::pause(std::sync::Arc::as_ptr(&self.gate), self.gen);
                self.store_gate_under_slot_lock();
                retired
            } else {
                None
            }
        };
        drop(retired);
    }
}

impl AuditTicket {
    /// 落闸（issue#60 线性化点的机械锚）：**必须在订阅槽锁内调用**——槽摘除与
    /// `gate=false` 是一次状态变更。debug 构建用 `try_lock` 断言调用点确实持锁：
    /// 若有人把落闸挪出临界区（#60 的缺陷形状），测试立刻红，鉴别力不再依赖测试
    /// 暂停钩子恰好落在窗口内（审计 F1）。release 下 `debug_assert` 编译为空。
    #[inline]
    fn store_gate_under_slot_lock(&self) {
        debug_assert!(
            self.audit.try_lock().is_err(),
            "审计闸落必须在订阅槽锁内（#60）：陈旧票据的落闸与新订阅装槽未串行"
        );
        self.gate.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// 测试专用暂停点（`cfg(test)`，**不进公共面**）：让「槽已摘除、闸未落」这一
/// 窗口可被确定性地插进来（GitHub issue#60 的回归夹具——不暂停就只能靠概率
/// 竞速）。键 = (闸 Arc 身份, 订阅代次)：闸 Arc 每 store 一个，故跨测试并行
/// 运行时不会误命中别人的票据。
#[cfg(test)]
pub(crate) mod drop_pause {
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;

    pub(crate) struct Hook {
        pub(crate) gate: *const AtomicBool,
        pub(crate) gen: u64,
        pub(crate) reached: mpsc::Sender<()>,
        pub(crate) resume: mpsc::Receiver<()>,
    }

    // SAFETY（Mutex 包裹）：`Hook` 只被一次取走并在测试线程间移动（Sender/
    // Receiver 均 Send）；`*const AtomicBool` 只作身份比较，不解引用。
    unsafe impl Send for Hook {}

    static SLOT: Mutex<VecDeque<Hook>> = Mutex::new(VecDeque::new());

    pub(crate) fn install(hook: Hook) {
        SLOT.lock()
            .expect("drop_pause 槽中毒：测试线程已 panic")
            .push_back(hook);
    }

    /// 命中（闸身份 + 代次）则取走该钩子并阻塞到测试放行——一次性。
    pub(crate) fn pause(gate: *const AtomicBool, gen: u64) {
        let hook = {
            let mut slot = SLOT.lock().expect("drop_pause 槽中毒：测试线程已 panic");
            match slot.iter().position(|h| h.gate == gate && h.gen == gen) {
                Some(idx) => slot.remove(idx),
                None => return,
            }
        };
        if let Some(hook) = hook {
            // 通道失败 = 测试线程已退出（测试本来就失败了）：此处**不可** panic
            // ——本函数在票据 Drop（可能在 unwind 路径上）里被调用，二次 panic 会
            // 直接把进程 abort，掩盖真正的失败原因。
            let _ = hook.reached.send(());
            let _ = hook.resume.recv();
        }
    }
}

/// issue#60 回归：陈旧票据落闸 vs 替换订阅的竞态（确定性交错，非概率竞速）。
///
/// 白盒测试住本模块：夹具要 `cfg(test)` 暂停点与票据的 `gen`/闸身份（crate 内
/// 可见、**不扩公共面**）。公共面的两种顺序与被取代票据 drop 见
/// `tests/audit_gate_generation.rs`。
#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SyncStore;
    use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::thread;
    use std::time::Duration;

    /// 计数 reducer（零效果）：只为驱动 dispatch 产审计事件。
    struct Tick;

    impl Reducer for Tick {
        type State = u64;
        type Action = u64;
        type Op = Infallible;

        fn initial(&self) -> u64 {
            0
        }

        fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
            s + a
        }
    }

    /// 强制交错的观测面（三块断言各取一处，互不掩盖）。
    struct ForcedRace {
        /// 新订阅是否在「旧票据已摘槽、闸未落」的暂停窗内装入完成。
        installed_during_pause: bool,
        /// 随后一个事件的环历史。
        events: Vec<AuditEvent>,
        /// 随后一个事件命中**当前代次**回调的次数。
        current_hits: usize,
        /// 旧票据回调的命中次数（它已被摘除，必须 0）。
        stale_hits: usize,
    }

    /// 序列：旧票据 Drop 暂停在「槽已摘、闸未落」→ 新订阅抢在暂停窗内装入 →
    /// 放行旧 Drop → 打一个事件。
    fn force_stale_drop_before_new_subscription() -> ForcedRace {
        let store = Arc::new(SyncStore::new(Tick, NoFx));
        let stale_hits = Arc::new(AtomicUsize::new(0));
        let ticketing = {
            let hits = Arc::clone(&stale_hits);
            store.subscribe_events(16, move |_| {
                hits.fetch_add(1, Ordering::SeqCst);
            })
        };
        let gen_a = ticketing.gen;
        let gate_ptr = Arc::as_ptr(&ticketing.gate);

        let (reached_tx, reached_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        drop_pause::install(drop_pause::Hook {
            gate: gate_ptr,
            gen: gen_a,
            reached: reached_tx,
            resume: resume_rx,
        });

        let dropper = thread::spawn(move || drop(ticketing));
        reached_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("旧票据 Drop 未进入暂停点");

        // 替换订阅：修复后（落闸在审计 mutex 内）会被挡到 Drop 结束；竞态版
        // 会直接装入并开闸——故此处**有界**观察即可，无需 sleep。
        let current_hits = Arc::new(AtomicUsize::new(0));
        let (installed_tx, installed_rx) = mpsc::channel();
        let subscriber = {
            let store = Arc::clone(&store);
            let hits = Arc::clone(&current_hits);
            thread::spawn(move || {
                let ticket = store.subscribe_events(16, move |_| {
                    hits.fetch_add(1, Ordering::SeqCst);
                });
                installed_tx.send(()).expect("install 信号通道");
                ticket
            })
        };
        let installed_during_pause = installed_rx
            .recv_timeout(Duration::from_millis(500))
            .is_ok();

        resume_tx.send(()).expect("放行旧票据 Drop");
        dropper.join().expect("dropper 线程");
        let ticket_b = subscriber.join().expect("subscriber 线程");

        store.dispatch(1u64);
        let race = ForcedRace {
            installed_during_pause,
            events: store.take_events(),
            current_hits: current_hits.load(Ordering::SeqCst),
            stale_hits: stale_hits.load(Ordering::SeqCst),
        };
        drop(ticket_b);
        race
    }

    /// ① 陈旧旧票据**绝不能**禁用更新的 sink（实时回调面）。
    #[test]
    fn stale_ticket_drop_never_disables_newer_callback() {
        let race = force_stale_drop_before_new_subscription();
        assert!(
            race.current_hits > 0,
            "陈旧票据的落闸禁用了当前代次的实时回调（#60 竞态）"
        );
        assert_eq!(race.stale_hits, 0, "已被摘除的旧回调不得再收到事件");
    }

    /// ② 被禁用的闸**不得**意外压制环历史（取证面，与实时流分账）。
    #[test]
    fn stale_ticket_drop_never_suppresses_ring_history() {
        let race = force_stale_drop_before_new_subscription();
        assert!(
            !race.events.is_empty(),
            "陈旧票据的落闸压制了环历史（随后一个事件零记录，取证面静默）"
        );
    }

    /// ③ 落闸与装槽必须**串行在同一把审计 mutex 上**（竞态被结构性消除，而
    /// 不只是「碰巧没撞上」）：暂停窗内新订阅装不进来。
    #[test]
    fn stale_ticket_drop_serializes_with_new_subscription() {
        let race = force_stale_drop_before_new_subscription();
        assert!(
            !race.installed_during_pause,
            "旧票据落闸与新订阅装槽未串行（暂停窗内装入成功 ⇒ #60 窗口仍在）"
        );
    }

    /// ④ 反序守卫：新订阅先装入、旧票据后 Drop（代次失配）——旧票据不得动新流。
    #[test]
    fn superseded_ticket_drop_leaves_current_stream_live() {
        let store = SyncStore::new(Tick, NoFx);
        let stale_hits = Arc::new(AtomicUsize::new(0));
        let stale = {
            let hits = Arc::clone(&stale_hits);
            store.subscribe_events(16, move |_| {
                hits.fetch_add(1, Ordering::SeqCst);
            })
        };
        let current_hits = Arc::new(AtomicUsize::new(0));
        let _current = {
            let hits = Arc::clone(&current_hits);
            store.subscribe_events(16, move |_| {
                hits.fetch_add(1, Ordering::SeqCst);
            })
        };
        drop(stale); // 代次失配：既不清槽也不落闸

        store.dispatch(1u64);
        assert!(
            !store.take_events().is_empty(),
            "环历史被被取代票据的 drop 压制"
        );
        assert!(
            current_hits.load(Ordering::SeqCst) > 0,
            "被取代票据的 drop 禁用了当前代次的回调"
        );
        assert_eq!(
            stale_hits.load(Ordering::SeqCst),
            0,
            "被取代票据的回调不得再收到事件"
        );
    }

    /// ⑤ 顺序守卫：现役票据 Drop（无人接手）后事件流确实关闭——闸与槽同生死。
    #[test]
    fn dropping_current_ticket_without_replacement_closes_the_stream() {
        let store = SyncStore::new(Tick, NoFx);
        let ticket = store.subscribe_events(16, |_| {});
        store.dispatch(1u64);
        assert!(!store.take_events().is_empty(), "订阅期事件应入环");
        drop(ticket);
        store.dispatch(1u64);
        assert!(
            store.take_events().is_empty(),
            "无订阅者时环历史必须为空（闸 = 槽，禁残留记录）"
        );
    }

    /// 析构锁边界探针（issue#88）：记录「用户回调闭包析构时审计槽锁是否空闲」。
    ///
    /// `Drop` 里只做 `try_lock`（不阻塞、不 sleep）：拿到锁 ⇒ 析构发生在锁外；
    /// 失败 ⇒ 仍有线程持锁（= 锁内析构的缺陷形状）。持 `Weak` 而非 `Arc`：探针
    /// 不得延长槽的寿命，否则观测行为本身改变了被测时点。
    struct DropProbe {
        audit: std::sync::Weak<Mutex<Option<AuditSink>>>,
        observed: Arc<Mutex<Option<LockBoundary>>>,
        /// 析构次数（「恰好一次」断言：在途回调不得被新路径重复销毁）。
        drops: Arc<AtomicUsize>,
    }

    /// 探针读数。
    #[derive(Debug, PartialEq, Eq, Clone, Copy)]
    enum LockBoundary {
        /// `try_lock` 成功：审计锁空闲 ⇒ 回调闭包在锁外析构。
        Free,
        /// `try_lock` 失败：有线程持锁 ⇒ 回调闭包在锁内析构。
        Held,
    }

    impl DropProbe {
        fn new(audit: &Arc<Mutex<Option<AuditSink>>>) -> Self {
            Self {
                audit: Arc::downgrade(audit),
                observed: Arc::new(Mutex::new(None)),
                drops: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            let boundary = match self.audit.upgrade() {
                Some(audit) => match audit.try_lock() {
                    Ok(_) => LockBoundary::Free,
                    Err(_) => LockBoundary::Held,
                },
                // 槽本身已随 store 析构：锁不复存在，谈不上「锁内析构」。
                None => LockBoundary::Free,
            };
            *self.observed.lock().expect("探针结果槽中毒") = Some(boundary);
        }
    }

    /// 取 store 的审计槽句柄：借一次性票据拿到 `audit` Arc 后释放该票据（清空
    /// 槽）⇒ 其后的订阅是「从空槽装入」的唯一现役订阅。
    fn audit_slot<S, A, Op>(store: &SyncStore<S, A, Op>) -> Arc<Mutex<Option<AuditSink>>>
    where
        S: Clone + Send + Sync + 'static,
        A: Send + 'static,
        Op: Send + 'static,
    {
        let placeholder = store.subscribe_events(4, |_| {});
        let audit = Arc::clone(&placeholder.audit);
        drop(placeholder);
        audit
    }

    /// ⑥ issue#88：现役票据 Drop 的整槽退役必须在 audit 锁**外**析构用户回调。
    #[test]
    fn ticket_drop_retires_callback_outside_audit_lock() {
        let store = SyncStore::new(Tick, NoFx);
        let audit = audit_slot(&store);
        let probe = DropProbe::new(&audit);
        let observed = Arc::clone(&probe.observed);
        let ticket = store.subscribe_events(16, move |_| {
            let _ = &probe; // 闭包持有探针：闭包析构即观测点
        });
        assert_eq!(
            *observed.lock().expect("探针结果槽中毒"),
            None,
            "订阅期的回调闭包不得被析构"
        );

        drop(ticket);
        assert_eq!(
            *observed.lock().expect("探针结果槽中毒"),
            Some(LockBoundary::Free),
            "票据 Drop 在 audit 锁内析构了用户回调闭包（#88 自锁形状）"
        );
    }

    /// ⑦ issue#88：新订阅覆盖旧订阅时，被覆盖整槽的析构同样必须在锁外。
    #[test]
    fn resubscribe_retires_old_callback_outside_audit_lock() {
        let store = SyncStore::new(Tick, NoFx);
        let audit = audit_slot(&store);
        let probe = DropProbe::new(&audit);
        let observed = Arc::clone(&probe.observed);
        let stale = store.subscribe_events(16, move |_| {
            let _ = &probe;
        });
        let current = store.subscribe_events(16, |_| {});

        assert_eq!(
            *observed.lock().expect("探针结果槽中毒"),
            Some(LockBoundary::Free),
            "覆盖订阅在 audit 锁内析构了旧回调闭包（#88 自锁形状）"
        );
        drop(stale); // 代次失配：不碰新流
        drop(current);
    }

    /// ⑧ 控制臂：同一探针在「锁被持有」时必须读到 `Held`——证明上面的 `Free`
    /// 不是恒真断言，探针确有鉴别力。
    ///
    /// 持锁方是**另一线程**：std 未规定同线程对自己的锁 `try_lock` 的结果，跨
    /// 线程才有平台无关的「必然失败」。
    #[test]
    fn drop_probe_control_arm_reports_held_lock() {
        let store = SyncStore::new(Tick, NoFx);
        let audit = audit_slot(&store);
        let probe = DropProbe::new(&audit);
        let observed = Arc::clone(&probe.observed);

        let (locked_tx, locked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder = {
            let audit = Arc::clone(&audit);
            thread::spawn(move || {
                let _guard = lock_audit(&audit);
                locked_tx.send(()).expect("持锁信号通道");
                release_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("持锁方未收到放行");
            })
        };
        locked_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("持锁线程未就绪");

        drop(probe);
        assert_eq!(
            *observed.lock().expect("探针结果槽中毒"),
            Some(LockBoundary::Held),
            "控制臂失效：探针在锁被持有时仍报 Free"
        );

        release_tx.send(()).expect("放行持锁方");
        holder.join().expect("持锁线程");
    }
    /// ⑨ issue#88：替换/退订发生在**回调已被 dispatcher 取走**的窗口内（槽内
    /// `notify == None`）——旧回调只由在途派发者持有：锁外析构、且恰好一次。
    #[test]
    fn inflight_callback_is_destroyed_once_outside_lock_on_replace() {
        let store = Arc::new(SyncStore::new(Tick, NoFx));
        let audit = audit_slot(&store);
        let probe = DropProbe::new(&audit);
        let observed = Arc::clone(&probe.observed);
        let drops = Arc::clone(&probe.drops);
        let (in_cb_tx, in_cb_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let stale = store.subscribe_events(16, move |_| {
            let _ = &probe; // 探针随闭包析构
            in_cb_tx.send(()).expect("回调信号通道");
            release_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("回调未收到放行");
        });

        let emitter = {
            let store = Arc::clone(&store);
            thread::spawn(move || store.dispatch(1u64))
        };
        in_cb_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("回调未进入在途窗口（槽内 notify 未被取走）");

        // 在途窗口内替换订阅 + 释放旧票据：两条路径都不得析构在途回调。
        let current = store.subscribe_events(16, |_| {});
        drop(stale);
        assert_eq!(
            drops.load(Ordering::SeqCst),
            0,
            "在途回调被替换/退订路径提前或重复析构"
        );

        release_tx.send(()).expect("放行回调");
        emitter.join().expect("dispatch 线程");
        assert_eq!(
            drops.load(Ordering::SeqCst),
            1,
            "在途回调必须恰好析构一次（重复销毁 = 双重释放）"
        );
        assert_eq!(
            *observed.lock().expect("探针结果槽中毒"),
            Some(LockBoundary::Free),
            "在途回调在 audit 锁内析构（#88 自锁形状）"
        );
        drop(current);
    }
}
