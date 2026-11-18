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
//! 阶段（[`Phase`]）只在真实提交边界推进：reducer 前 `Reducing`、快照替换后
//! `StateCommitted`、效果转发前 `ForwardingEffects`、转发毕 `EffectsForwarded`、
//! 通知段 `Notifying`——失败分类据此区分「提交前失败」与「提交后部分失败」。

use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// action 处理阶段（终局分类用）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Phase {
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
#[derive(Clone, Debug)]
pub(crate) struct Failure {
    /// 失败 action 的序号（store 内单调）。
    pub seq: u64,
    /// 失败发生时的阶段。
    pub phase: Phase,
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
    Failed(Failure),
    /// 因果后代被回灌预算脱钩续接——本次等待边界以「未完成」收口。
    Deferred,
}

/// 账本状态（锁内）。字段纪律：只留聚合计数与首条失败摘要，不留 action 载荷、
/// 不留无界的后代清单。
#[derive(Default)]
struct LedgerState {
    /// 未终局的因果后代数（外部根本身不计入）。
    outstanding: usize,
    /// 根 action 自身阶段是否已完成。
    root_done: bool,
    /// 已发布终局（发布后不可变）。
    terminal: Option<Outcome>,
    /// 首条失败摘要（先到先得，后续失败只计数）。
    first_failure: Option<Failure>,
    /// 失败总数（bounded 计数，禁留载荷）。
    failure_count: usize,
    /// 是否有因果后代被回灌预算脱钩（终局取 `Deferred`）。
    deferred: bool,
    /// 是否已被归还到复用池（首次归还者胜——防同一账本被归还两次）。
    released: bool,
}

/// 完成账本：`Mutex<LedgerState>` + `Condvar`（等待面与发布面同一把锁——
/// 禁「裸原子终局旗 + 无关 Condvar」的丢唤醒形态）。
pub(crate) struct Ledger {
    state: Mutex<LedgerState>,
    cv: Condvar,
}

impl Ledger {
    /// 新建账本（外部根 action 的唯一分配点）。
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LedgerState::default()),
            cv: Condvar::new(),
        })
    }

    /// 接受一个因果后代（queue 锁内调用：锁序 queue → ledger）。
    pub(crate) fn add_descendant(&self) {
        let mut st = self.lock();
        st.outstanding += 1;
    }

    /// 后代终局（成功 / 失败 / 脱钩后另行记账）。
    pub(crate) fn descendant_done(&self, failure: Option<Failure>) {
        let mut st = self.lock();
        // 记账恒减：后代终局与脱钩各减一次（脱钩走 detach_deferred）。
        st.outstanding = st.outstanding.saturating_sub(1);
        if let Some(f) = failure {
            st.failure_count += 1;
            if st.first_failure.is_none() {
                st.first_failure = Some(f);
            }
        }
        self.publish(&mut st);
    }

    /// 根 action 自身阶段完成（正常路径）。
    pub(crate) fn finish_root(&self, failure: Option<Failure>) {
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
    /// 不可能超预算；续接账本的根可能再次超预算）。
    pub(crate) fn detach_deferred(&self, is_root: bool) {
        let mut st = self.lock();
        if is_root {
            st.root_done = true;
        } else {
            st.outstanding = st.outstanding.saturating_sub(1);
        }
        st.deferred = true;
        self.publish(&mut st);
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
        let outcome = if let Some(f) = st.first_failure.clone() {
            Outcome::Failed(f)
        } else if st.deferred {
            Outcome::Deferred
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
    pub(crate) fn reset(&self) {
        let mut st = self.lock();
        st.outstanding = 0;
        st.root_done = false;
        st.terminal = None;
        st.first_failure = None;
        st.failure_count = 0;
        st.deferred = false;
        st.released = false;
    }

    /// 归还标记：首次归还者返回 `true`（账本进复用池），其后一律 `false`
    /// ——同一账本只可能进池一次（双重归还 = 两个使用者共用一个账本 = 状态互毁）。
    pub(crate) fn mark_released(&self) -> bool {
        let mut st = self.lock();
        if st.released {
            return false;
        }
        st.released = true;
        true
    }
}
