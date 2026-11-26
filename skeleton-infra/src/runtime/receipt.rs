//! 回执账本（per-action 序列回执 + 因果后代屏障，GitHub issue#40）。
//!
//! 每个**外部根** action 分配一个账本（[`Ledger`]：`Mutex` + `Condvar`）。
//! 同栈回灌的**因果后代**不各自分配账本——它们共享根账本（`Arc` 克隆，只是
//! 引用计数流量），并在根账本上累加「未终局后代计数」。
//!
//! 终局谓词：`root_done && outstanding == 0`。
//! - 后代计数在 queue 锁内与入队原子增减（锁序 queue → ledger），
//!   根账本不可能在后代被接受之前发布终局（无「已接受但未计数」的缝）。
//! - 终局结果一经发布即不可变（`Completed` / `Failed` / `Deferred` 三者互斥）。
//!
//! **复用生命周期（issue#84）**：`Arc` 克隆可在飞行中保留到**全部因果后代
//! 结算**；账本进复用池只在**终局发布**且**指定 owner 让渡**之后开始——
//! 非等待型根由框架在终局发布点归还（`claim_release`），等待型根由外部等待者
//! 消费 `Outcome` 后归还（`waiter_release`）。终局发布前入池 = 在途后代的
//! 结算被记到下一个根的账上（跨归属），故 `reset` 在 debug/test 构建对
//! 「已终局 + 无在途后代 + 已 claim」做硬断言。
//!
//! 阶段（[`DispatchPhase`]）只在真实提交边界推进：reducer 前 `Reducing`、快照
//! 替换后 `StateCommitted`、效果转发前 `ForwardingEffects`、转发毕
//! `EffectsForwarded`、通知段 `Notifying`——失败分类据此区分「提交前失败」与
//! 「提交后部分失败」。
//!
//! **停放计数（issue#57）**：`Deferred` 携带两个独立计数——`retained`（已留存
//! 到停放队列、可被 [`super::SyncStore::resume_deferred`] 恢复）与 `rejected`
//! （停放队列满、续接无法留存）。两者不合并：被拒记录**不是**「已停放」。

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// action 处理阶段（终局分类用；[`DispatchFailure`] 的公开字段）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DispatchPhase {
    /// 已入队未开始（被排空中断的队列残留）。
    Queued,
    /// reducer 执行中（此刻 panic = 未提交状态）。
    Reducing,
    /// 状态已发布（快照替换完成，旧快照析构之前）。
    StateCommitted,
    /// 效果转发中（此刻 panic = 状态已提交、外部效果可能部分生效）。
    ForwardingEffects,
    /// 效果全部转发完毕。
    EffectsForwarded,
    /// 通知段（订阅回调 panic 被摘除归一，不算排空中断）。
    Notifying,
}

/// 终局失败分类（bounded：单条失败摘要，禁留 action 载荷）。
///
/// 公开面（issue#57）：[`super::SyncStore::dispatch_with_receipt`] 的回执以值
/// 形式携带它，而**不是**像 legacy `dispatch` 那样 panic——同一条失败信息，
/// 两种消费形态。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DispatchFailure {
    /// 失败 action 的序号（store 内单调）。
    pub seq: u64,
    /// 失败发生时的阶段。
    pub phase: DispatchPhase,
    /// 失败时状态是否已提交（true = 部分完成，外部效果可能已部分生效）。
    pub state_committed: bool,
    /// 成因摘要（短文本）。
    pub cause: &'static str,
}

/// 账本终局结果（三者互斥，发布后不可变）。
#[derive(Clone, Debug)]
pub(crate) enum Outcome {
    /// 自身各阶段完成且全部因果后代终局成功。
    Completed,
    /// 自身或某个因果后代失败。
    Failed(DispatchFailure),
    /// 因果后代被回灌预算脱钩续接——本次等待边界以「未完成」收口。
    ///
    /// `retained` = 已留存到停放队列（可被恢复）的记录数；`rejected` = 停放
    /// 队列满、续接无法留存的记录数。**两者都是非成功**，且不互相折算
    /// （被拒记录不是「已停放」）。
    Deferred { retained: u64, rejected: u64 },
}

/// 账本状态（锁内）。字段纪律：只留聚合计数与首条失败摘要，不留 action 载荷、
/// 不留无界的后代清单。
#[derive(Default)]
struct LedgerState {
    /// 未终局的因果后代数（外部根本身不计入）。
    outstanding: usize,
    /// 本根**累计已接受**的因果后代数（issue#105/#144 per-root 因果工作量预算的
    /// 用量；单调不回落，随账本复位清零）。与 `outstanding` 分立：后者是终局谓词
    /// 的在途计数，前者是准入闸的用量。
    work: usize,
    /// 根 action 自身阶段是否已完成。
    root_done: bool,
    /// 已发布终局（发布后不可变）。
    terminal: Option<Outcome>,
    /// 首条失败摘要（先到先得，后续失败只计数）。
    first_failure: Option<DispatchFailure>,
    /// 失败总数（bounded 计数，禁留载荷）。
    failure_count: usize,
    /// 被回灌预算脱钩且**已留存**到停放队列的因果后代数（终局取 `Deferred`）。
    deferred: u64,
    /// 脱钩但**停放队列满、续接无法留存**的记录数（issue#57：独立非成功计数，
    /// 不并入 `deferred`——被拒记录不是「已停放」）。
    deferred_rejected: u64,
    /// 是否已被归还到复用池（首次归还者胜——防同一账本被归还两次）。
    released: bool,
    /// 归属 token（issue#84）：本账本属于**等待型根**（`true`）= 外部等待者
    /// 持有归还权（消费终局后归还）；`false` = 非等待型根，框架在其终局发布
    /// 时归还。显式落在账本状态里——不由任何后代记录的 `waiter` 推断（后代
    /// 记录的该字段恒 false）。
    waiter: bool,
}

/// 完成账本：`Mutex<LedgerState>` + `Condvar`（等待面与发布面同一把锁——
/// 禁「裸原子终局旗 + 无关 Condvar」的丢唤醒形态）。
pub(crate) struct Ledger {
    state: Mutex<LedgerState>,
    cv: Condvar,
}

impl Ledger {
    /// 新建账本（外部根 action 的唯一分配点）。
    ///
    /// `waiter` = 本根是否有外部等待者：等待型根的 owner 是外部等待者（它消费
    /// 终局后自行归还）；非等待型根由框架在其**终局发布**时归还（issue#84——
    /// 归属 token 落在账本状态里显式化，不由任何后代记录的字段推断）。
    pub(crate) fn new(waiter: bool) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LedgerState {
                waiter,
                ..LedgerState::default()
            }),
            cv: Condvar::new(),
        })
    }

    /// 因果后代的**预算预留 + 未终局计数**（queue 锁内调用：锁序 queue → ledger；
    /// issue#105/#144）：`budget > 0` 且本根已接受后代数达上限 ⇒ 拒绝预留并返回
    /// `false`（两个计数都不动——调用方不得让该子动作进入主队列）。
    pub(crate) fn try_add_descendant(&self, budget: usize) -> bool {
        let mut st = self.lock();
        if budget > 0 && st.work >= budget {
            return false;
        }
        st.work += 1;
        st.outstanding += 1;
        true
    }

    /// 未获预算预留的子动作去向（issue#105/#144）：它从未计入 `outstanding`，只把
    /// 「已停放（可恢复）/ 被停放容量拒收」记入原账本——根回执据此以 `Deferred` /
    /// `CapacityRejected` 收口。调用时产出它的 action 仍在途（账本不可能已终局）。
    pub(crate) fn note_unadmitted(&self, retained: bool) {
        let mut st = self.lock();
        debug_assert!(
            st.terminal.is_none(),
            "未获预留子动作记账时原账本已终局：产出它的 action 不该仍在途"
        );
        if retained {
            st.deferred += 1;
        } else {
            st.deferred_rejected += 1;
        }
    }

    /// 本账本的因果子树是否已无在途后代（终局已发布，或 `outstanding` 为 0）。
    /// 排空让出点用它判定「非等待型入口 action 的完成边界是否已过」。
    pub(crate) fn has_no_inflight(&self) -> bool {
        let st = self.lock();
        st.terminal.is_some() || st.outstanding == 0
    }

    /// 后代终局（成功 / 失败 / 脱钩后另行记账）。
    pub(crate) fn descendant_done(&self, failure: Option<DispatchFailure>) {
        let mut st = self.lock();
        // 记账恒减：后代终局与脱钩各减一次（脱钩走 detach_deferred）。
        release_descendant_slot(&mut st);
        if let Some(f) = failure {
            st.failure_count += 1;
            if st.first_failure.is_none() {
                st.first_failure = Some(f);
            }
        }
        self.publish(&mut st);
    }

    /// 根 action 自身阶段完成（正常路径）。
    pub(crate) fn finish_root(&self, failure: Option<DispatchFailure>) {
        let mut st = self.lock();
        st.root_done = true;
        if let Some(f) = failure {
            st.failure_count += 1;
            if st.first_failure.is_none() {
                st.first_failure = Some(f);
            }
        }
        self.publish(&mut st);
    }

    /// 因果后代被回灌预算脱钩（迁往续接账本）：本账本记「未完成」并减计数。
    ///
    /// `is_root` = 被脱钩者是否本账本的根（外部根恒 false——外部流量深度恒 0
    /// 不可能超预算；续接账本的根可能再次超预算）。`retained` = 脱钩记录是否
    /// **成功留存**到停放队列：`false` = 停放队列满被容量拒收（issue#57 的
    /// 独立非成功计数，不并入 `deferred`）。
    pub(crate) fn detach_deferred(&self, is_root: bool, retained: bool) {
        let mut st = self.lock();
        if is_root {
            st.root_done = true;
        } else {
            release_descendant_slot(&mut st);
        }
        if retained {
            st.deferred += 1;
        } else {
            st.deferred_rejected += 1;
        }
        self.publish(&mut st);
    }

    /// 恢复路径的等待者认领（issue#49/#57）：停放记录尚未被任何排空者处理 ⇒
    /// 终局未发布、无归还竞争 ⇒ 此刻把归属 token 翻到等待者一侧（恢复调用者在
    /// 消费终局后归还，见 [`Self::waiter_release`]）。
    pub(crate) fn claim_as_waiter(&self) {
        let mut st = self.lock();
        debug_assert!(
            st.terminal.is_none() && !st.released,
            "恢复的停放记录必须尚未终局、尚未归还（否则账本已被复用）"
        );
        st.waiter = true;
    }

    /// 等待终局（外部调用者的同步等待面）。中毒锁按 `into_inner` 恢复——
    /// 账本锁内不跑用户代码，中毒只可能来自框架自身的记账 panic。
    pub(crate) fn wait(&self) -> Outcome {
        let mut st = self.lock();
        loop {
            if let Some(outcome) = st.terminal.clone() {
                return outcome;
            }
            st = self
                .cv
                .wait(st)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// 终局判定与发布（锁内调用）。终局结果一经发布不可变：重复发布被拒。
    fn publish(&self, st: &mut LedgerState) {
        if st.terminal.is_some() {
            return;
        }
        if !(st.root_done && st.outstanding == 0) {
            return;
        }
        let outcome = if let Some(f) = st.first_failure {
            Outcome::Failed(f)
        } else if st.deferred > 0 || st.deferred_rejected > 0 {
            Outcome::Deferred {
                retained: st.deferred,
                rejected: st.deferred_rejected,
            }
        } else {
            Outcome::Completed
        };
        st.terminal = Some(outcome);
        // 通知在解锁后由调用方发出（终局谓词与结果已在锁内发布，等待者要么
        // 已在等（被唤醒），要么尚未睡（醒来即见终局）——无丢唤醒）。
        self.cv.notify_all();
    }

    fn lock(&self) -> MutexGuard<'_, LedgerState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 复位到空闲态（复用池取出时调用）：清空终局、计数与失败摘要。
    /// 复位在账本锁内完成——复用方拿到的账本必是干净态。
    ///
    /// **不变式（issue#84）**：只允许复位**已终局且已 claim 归还**的账本。
    /// 活账本被复位 = 在途后代把结算记到下一个根的账上（跨归属）；debug/test
    /// 构建把违反现场直接咬住（release 构建保持零开销）。
    pub(crate) fn reset(&self, waiter: bool) {
        let mut st = self.lock();
        debug_assert!(
            st.terminal.is_some(),
            "复位非终局账本：账本在终局发布前被放回复用池（在途后代会被记到下一个根上）"
        );
        debug_assert_eq!(
            st.outstanding, 0,
            "复位仍有在途后代的账本（终局谓词未成立）"
        );
        debug_assert!(st.released, "复位未被 claim 归还的账本");
        st.outstanding = 0;
        st.work = 0;
        st.root_done = false;
        st.terminal = None;
        st.first_failure = None;
        st.failure_count = 0;
        st.deferred = 0;
        st.deferred_rejected = 0;
        st.released = false;
        st.waiter = waiter;
    }

    /// 框架侧复用资格判定与 claim：**终局发布是唯一可复用边界**。
    ///
    /// 全部成立才 claim（`released` 保证每个账本恰好一次入池）：
    /// - 终局已发布（`root_done && outstanding == 0` 曾成立）；
    /// - 本账本属于**非等待型根**——等待型根的 owner 是外部等待者，由
    ///   [`Self::waiter_release`] 归还。
    ///
    /// 判定在账本锁内完成；调用方放锁后才取 `ledger_pool`（禁同时持两把锁）。
    pub(crate) fn claim_release(&self) -> bool {
        let mut st = self.lock();
        if st.released || st.waiter || st.terminal.is_none() {
            return false;
        }
        st.released = true;
        true
    }

    /// 等待者收口后的归还 claim：等待型根的 owner 在 `wait()` 返回终局后调用
    /// （`wait()` 返回即终局已发布）。与 [`Self::claim_release`] 共用
    /// `released` 的恰好一次守卫——两条路径合起来每个账本只进池一次。
    ///
    /// 终局未发布即归还（回执句柄被提前丢弃，issue#57）⇒ **不归还**：活账本
    /// 绝不许进池（`reset` 的不变式），账本随最后一个 `Arc` 消亡——少一次复用，
    /// 不产生跨归属记账。
    pub(crate) fn waiter_release(&self) -> bool {
        let mut st = self.lock();
        if st.released || st.terminal.is_none() {
            return false;
        }
        st.released = true;
        true
    }
}

/// 后代计数递减（后代终局与脱钩共用的唯一记账点）。
///
/// issue#84：**不用 `saturating_sub` 掩盖**重复/迟到结算——debug/test 构建在
/// 计数已为 0 时直接咬住（正是「账本被提前复用」的现场）；release 构建取饱和
/// 语义保持零开销。
fn release_descendant_slot(st: &mut LedgerState) {
    debug_assert!(
        st.outstanding > 0,
        "后代结算/脱钩时未终局后代计数已为 0：重复结算或账本在终局前被复用"
    );
    st.outstanding = st.outstanding.saturating_sub(1);
}

#[cfg(test)]
impl Ledger {
    /// 测试读侧：终局是否已发布（复用池不变式断言用）。
    pub(crate) fn terminal_published(&self) -> bool {
        self.lock().terminal.is_some()
    }

    /// 测试读侧：累计已接受后代数 / 在途后代数（预留原子性探针用）。
    pub(crate) fn work_counts(&self) -> (usize, usize) {
        let st = self.lock();
        (st.work, st.outstanding)
    }
}
