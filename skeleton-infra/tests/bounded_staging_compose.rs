#![cfg(all(feature = "runtime", feature = "testing", feature = "compose"))]
//! `combine!` 委托与 `TestStore` 路径的**同款边界**（ADR-0011 §2.2，issue#233 票①）。
//!
//! 钉两件事：
//!
//! 1. `combine!` 委托链产出的效果与父自己的输出共用**同一个**暂存位 ⇒ 上限按
//!    「整次转换的总条数」计，超界即整次被拒（不是按片段各计一份）。
//! 2. `TestStore` 走 legacy `dispatch` 路径 ⇒ 预算拒绝**不 panic**、状态不前进、
//!    无残留效果（测试面不得把被拒转换伪装成一次成功迁移）。

use skeleton::{Effect, EffectBuf, EffectId, EffectRuntime, Fragment, Reducer, Store as _};
use skeleton_infra::runtime::{DispatchOutcome, SyncStore};
use skeleton_infra::testing::TestStore;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

/// 默认 per-reducer 分段预算（`DEFAULT_SEGMENT_EFFECT_BUDGET` 是私有常量；
/// 测试面按 `docs/reference.md` §资源上限与默认值的公开口径钉 4096）。
const DEFAULT_SEGMENT_EFFECT_BUDGET: u32 = 4096;

#[derive(Clone, Debug, PartialEq)]
struct ChildState {
    count: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum ChildAction {
    /// 产 `n` 条 `Cancel` 效果（零 `op` 载荷 ⇒ `Op = Infallible` 可测）
    Cuts(u32),
}

struct ChildFeature;

impl Reducer for ChildFeature {
    type State = ChildState;
    type Action = ChildAction;
    type Op = Infallible;

    fn initial(&self) -> ChildState {
        ChildState { count: 0 }
    }

    fn reduce(
        &self,
        s: &ChildState,
        a: ChildAction,
        out: &mut EffectBuf<ChildAction, Infallible>,
    ) -> ChildState {
        match a {
            ChildAction::Cuts(n) => {
                for i in 0..n {
                    out.push(Effect::Cancel {
                        id: EffectId::new(u64::from(i) + 1),
                    });
                }
            }
        }
        ChildState { count: s.count + 1 }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ParentState {
    child: ChildState,
}

#[derive(Clone, Debug, PartialEq)]
enum ParentAction {
    Child(ChildAction),
}

/// 组合根：`combine!` 显式 `lift`（子效果上提到父词表）。
///
/// 短形态省略 `lift` 会走零效果闸（子效果被证明可丢）⇒ 本票要测「委托链输出
/// 计入同一暂存位」，必须显式上提。
fn parent_fragment() -> impl Fragment<State = ParentState, Action = ParentAction, Op = Infallible> {
    skeleton_infra::combine!(
        ChildFeature,
        ParentState.child,
        ParentAction::Child,
        lift: |effect: Effect<ChildAction, Infallible>,
               out: &mut EffectBuf<ParentAction, Infallible>| {
            // 子词表只产 `Cancel`（`Op = Infallible` ⇒ 无 `Run`；本夹具无 `Emit`）。
            if let Effect::Cancel { id } = effect {
                out.push(Effect::Cancel { id });
            }
        }
    )
}

struct Parent;

impl Reducer for Parent {
    type State = ParentState;
    type Action = ParentAction;
    type Op = Infallible;

    fn initial(&self) -> ParentState {
        ParentState {
            child: ChildState { count: 0 },
        }
    }

    fn reduce(
        &self,
        s: &ParentState,
        a: ParentAction,
        out: &mut EffectBuf<ParentAction, Infallible>,
    ) -> ParentState {
        parent_fragment().reduce(s, a, out)
    }
}

/// 解释器：只记条数（`Cancel` 无 `op` 载荷）。
#[derive(Default)]
struct Counting {
    cancelled: Arc<Mutex<u32>>,
}

impl EffectRuntime<ParentAction, Infallible> for Counting {
    fn run(
        &mut self,
        effect: Effect<ParentAction, Infallible>,
        _back: &skeleton::ActionSender<ParentAction>,
    ) {
        if let Effect::Cancel { .. } = effect {
            *self.cancelled.lock().expect("cancelled 锁") += 1;
        }
    }
}

/// 委托链的输出按**整次转换**计上限：父 + 子合计 3 条 > 上限 2 ⇒ 整次被拒；
/// 恰在上限则照常提交。判别：按片段各计一份（3 条也过）即红。
#[test]
fn combined_reducer_output_is_bounded_as_one_transition() {
    let cancelled = Arc::new(Mutex::new(0));
    let store = SyncStore::new(
        Parent,
        Counting {
            cancelled: Arc::clone(&cancelled),
        },
    );
    store.set_segment_effect_budget(2);

    let over = store
        .dispatch_with_receipt(ParentAction::Child(ChildAction::Cuts(3)))
        .expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(
        store.snapshot().child.count,
        0,
        "被拒转换不发布 next state（含 combine! 写回的切片）"
    );
    assert_eq!(*cancelled.lock().expect("cancelled 锁"), 0, "效果零转发");

    let at = store
        .dispatch_with_receipt(ParentAction::Child(ChildAction::Cuts(2)))
        .expect("准入");
    assert_eq!(at.wait(), DispatchOutcome::Completed, "恰在上限：照常提交");
    assert_eq!(store.snapshot().child.count, 1);
    assert_eq!(*cancelled.lock().expect("cancelled 锁"), 2);
}

/// `TestStore` 路径（legacy `dispatch` + idle 快照）：默认上限下超量 ⇒ 不 panic、
/// 状态不前进、无残留效果。判别：预算拒绝走 panic 口径即红；状态被提交即红。
#[test]
fn teststore_path_rejects_over_default_cap_without_panic_or_state_change() {
    let mut ts = TestStore::new(Parent);
    ts.dispatch(ParentAction::Child(ChildAction::Cuts(
        DEFAULT_SEGMENT_EFFECT_BUDGET + 1,
    )));
    assert_eq!(
        ts.state().child.count,
        0,
        "被拒转换不发布 next state（TestStore 的 idle 快照 = 旧状态）"
    );
    ts.finish();

    // 恰在上限的转换照常过（片段真被调用、状态照常写回；效果进了脚本队列）。
    let mut at_cap = TestStore::new(Parent);
    at_cap.dispatch(ParentAction::Child(ChildAction::Cuts(2)));
    assert_eq!(at_cap.state().child.count, 1, "上限内：接受");
    assert_eq!(at_cap.pending_effects(), 2, "上限内：效果照常产出");
}

// ── 全链 live staging（#235 / ADR-0011 §2.2）：嵌套深度 2 的额度共享与溢出传播 ──

/// 叶子：reduce 停靠点记录它拿到的**暂存额度**（子缓冲上限），再按需产效果。
#[derive(Clone, Debug, PartialEq, Default)]
struct LeafState {
    n: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum LeafAction {
    Cuts(u32),
}

struct Leaf {
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl Reducer for Leaf {
    type State = LeafState;
    type Action = LeafAction;
    type Op = Infallible;

    fn initial(&self) -> LeafState {
        LeafState { n: 0 }
    }

    fn reduce(
        &self,
        s: &LeafState,
        a: LeafAction,
        out: &mut EffectBuf<LeafAction, Infallible>,
    ) -> LeafState {
        self.seen
            .lock()
            .expect("seen 锁")
            .push(out.remaining_limit());
        let LeafAction::Cuts(n) = a;
        for i in 0..n {
            out.push(Effect::Cancel {
                id: EffectId::new(u64::from(i) + 1),
            });
        }
        LeafState { n: s.n + 1 }
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct MidState {
    leaf: LeafState,
}

#[derive(Clone, Debug, PartialEq)]
enum MidAction {
    Leaf(LeafAction),
}

struct Mid {
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl Reducer for Mid {
    type State = MidState;
    type Action = MidAction;
    type Op = Infallible;

    fn initial(&self) -> MidState {
        MidState::default()
    }

    fn reduce(
        &self,
        s: &MidState,
        a: MidAction,
        out: &mut EffectBuf<MidAction, Infallible>,
    ) -> MidState {
        // 自己先占一个槽 ⇒ 叶子拿到的额度应比根预算少 1（嵌套扣除祖先存活缓冲）。
        out.push(Effect::Cancel {
            id: EffectId::new(100),
        });
        // `combine!` 的 `Fragment::reduce` 返回**父层**状态（`PS`），直接就是本层结果。
        skeleton_infra::combine!(
            Leaf {
                seen: Arc::clone(&self.seen),
            },
            MidState.leaf,
            MidAction::Leaf,
            lift: |effect: Effect<LeafAction, Infallible>,
                   out: &mut EffectBuf<MidAction, Infallible>| {
                if let Effect::Cancel { id } = effect {
                    out.push(Effect::Cancel { id });
                }
            }
        )
        .reduce(s, a, out)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct RootState {
    mid: MidState,
}

#[derive(Clone, Debug, PartialEq)]
enum RootAction {
    Mid(MidAction),
}

struct Root {
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl Reducer for Root {
    type State = RootState;
    type Action = RootAction;
    type Op = Infallible;

    fn initial(&self) -> RootState {
        RootState::default()
    }

    fn reduce(
        &self,
        s: &RootState,
        a: RootAction,
        out: &mut EffectBuf<RootAction, Infallible>,
    ) -> RootState {
        skeleton_infra::combine!(
            Mid {
                seen: Arc::clone(&self.seen),
            },
            RootState.mid,
            RootAction::Mid,
            lift: |effect: Effect<MidAction, Infallible>,
                   out: &mut EffectBuf<RootAction, Infallible>| {
                if let Effect::Cancel { id } = effect {
                    out.push(Effect::Cancel { id });
                }
            }
        )
        .reduce(s, a, out)
    }
}

/// 解释器：只记条数（根词表）。
#[derive(Default)]
struct RootCounting {
    cancelled: Arc<Mutex<u32>>,
}

impl EffectRuntime<RootAction, Infallible> for RootCounting {
    fn run(
        &mut self,
        effect: Effect<RootAction, Infallible>,
        _back: &skeleton::ActionSender<RootAction>,
    ) {
        if let Effect::Cancel { .. } = effect {
            *self.cancelled.lock().expect("cancelled 锁") += 1;
        }
    }
}

/// 嵌套组合链（深度 2）共用**同一份**暂存预算：叶子 reduce 停靠点观测到的额度 =
/// 根预算 − 祖先已占槽位 ⇒ 全链 live staging 受同一预算约束；超出预算 ⇒ 子缓冲
/// 溢出、沿父链传回、整段原子拒绝（不静默截断）。
///
/// 判别：① 去掉子缓冲额度继承 ⇒ 叶子观测到 `None`（不限）即红；② 去掉溢出回传
/// ⇒ 截断后的前缀被当成功提交（`BudgetRejected` 变成 `Completed`）即红；③ 去掉
/// 动态额度回收 ⇒ 上限内的转换也会被拒即红。
#[test]
fn nested_compose_chain_shares_one_staging_budget() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let cancelled = Arc::new(Mutex::new(0));
    let store = SyncStore::new(
        Root {
            seen: Arc::clone(&seen),
        },
        RootCounting {
            cancelled: Arc::clone(&cancelled),
        },
    );
    store.set_segment_effect_budget(6);

    // 上限内：中间自己 1 + 叶子 2 = 3 ≤ 6 ⇒ 接受。
    let ok = store
        .dispatch_with_receipt(RootAction::Mid(MidAction::Leaf(LeafAction::Cuts(2))))
        .expect("准入");
    assert_eq!(ok.wait(), DispatchOutcome::Completed);
    assert_eq!(store.snapshot().mid.leaf.n, 1);
    assert_eq!(
        *cancelled.lock().expect("cancelled 锁"),
        3,
        "1 + 2 条照常转发"
    );
    assert_eq!(
        *seen.lock().expect("seen 锁"),
        vec![Some(5)],
        "叶子的暂存额度必须是「根预算 6 − 中间已占 1」= 5（缺继承则是 None = 不限）"
    );

    // 超预算：叶子想产 9 条但只有 5 额度 ⇒ 子缓冲溢出 ⇒ 沿父链传回 ⇒ 整段拒绝。
    let over = store
        .dispatch_with_receipt(RootAction::Mid(MidAction::Leaf(LeafAction::Cuts(9))))
        .expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(store.snapshot().mid.leaf.n, 1, "被拒转换不发布 next state");
    assert_eq!(
        *cancelled.lock().expect("cancelled 锁"),
        3,
        "被拒转换零转发（截断前缀也不得外泄）"
    );
}
