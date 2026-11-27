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
