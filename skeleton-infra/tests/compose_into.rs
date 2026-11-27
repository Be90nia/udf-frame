#![cfg(all(feature = "runtime", feature = "testing", feature = "compose"))]
//! 融合写入（ADR-0011 §2.2 / issue#235 二段）的公共面验收。
//!
//! 钉四件事：
//!
//! 1. 全链共用**一份**暂存预算：子层在 `reduce_into` 停靠点观测到的额度 = 父层
//!    预算 − 祖先已写条数（`Some(0)` = 精确零，不是「不限」）。
//! 2. 溢出**沿链传播**且整段原子拒绝（不静默截断、零转发、状态不前进）。
//! 3. `lift` 的 0/1/多输出都成立（无临时 Vec 的形态由调用点自选）。
//! 4. 旧 `Fragment` 经 [`Buffered`] 适配器可进融合树（保留旧回调时点）。

use skeleton::{
    Buffered, Effect, EffectBuf, EffectId, EffectRuntime, EffectSink, Fragment, FragmentInto,
    Infallible, Reducer, Store as _,
};
use skeleton_infra::runtime::{DispatchOutcome, SyncStore};
use std::sync::{Arc, Mutex};

/// 叶子：融合写法——效果**直接写 sink**（无中间缓冲），停靠点记录观测到的额度。
struct Leaf {
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl FragmentInto for Leaf {
    type State = u32;
    type Action = u32;
    type Op = Infallible;

    fn reduce_into<S>(&self, s: &u32, a: u32, out: &mut S) -> u32
    where
        S: EffectSink<Effect<u32, Infallible>>,
    {
        self.seen.lock().expect("seen 锁").push(out.remaining());
        for i in 0..a {
            out.push(Effect::Cancel {
                id: EffectId::new(u64::from(i) + 1),
            });
        }
        s + 1
    }
}

/// 旧写法叶子（只实现 `Fragment`），用于验证 `Buffered` 适配器互操作。
struct OldLeaf;

impl Reducer for OldLeaf {
    type State = u32;
    type Action = u32;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: u32, out: &mut EffectBuf<u32, Infallible>) -> u32 {
        for i in 0..a {
            out.push(Effect::Cancel {
                id: EffectId::new(u64::from(i) + 1),
            });
        }
        s + 1
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct MidState {
    leaf: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum MidAction {
    Leaf(u32),
}

struct Mid {
    seen: Arc<Mutex<Vec<Option<usize>>>>,
}

impl FragmentInto for Mid {
    type State = MidState;
    type Action = MidAction;
    type Op = Infallible;

    fn reduce_into<S>(&self, s: &MidState, a: MidAction, out: &mut S) -> MidState
    where
        S: EffectSink<Effect<MidAction, Infallible>>,
    {
        // 自己先占一个槽：紧接着的叶子必须在**同一**预算里看到它被扣掉。
        out.push(Effect::Cancel {
            id: EffectId::new(100),
        });
        skeleton_infra::combine_into!(
            Leaf {
                seen: Arc::clone(&self.seen),
            },
            MidState.leaf,
            MidAction::Leaf,
            lift: |e: Effect<u32, Infallible>| match e {
                // 1 → 2 条：fan-out > 1 也受同一预算约束。
                Effect::Cancel { id } => vec![Effect::Cancel { id }, Effect::Cancel { id }],
                _ => Vec::new(),
            }
        )
        .reduce_into(s, a, out)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
struct RootState {
    mid: MidState,
    old: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum RootAction {
    Mid(MidAction),
    /// 走旧 `Fragment` + `Buffered` 适配器（互操作臂）。
    Old(u32),
}

#[derive(Clone)]
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
        match a {
            RootAction::Mid(a) => skeleton_infra::combine_into!(
                Mid {
                    seen: Arc::clone(&self.seen),
                },
                RootState.mid,
                RootAction::Mid,
                lift: |e: Effect<MidAction, Infallible>| match e {
                    // 根层 1:1（放大只发生在叶子层，便于逐项验算）。
                    Effect::Cancel { id } => vec![Effect::Cancel { id }],
                    _ => Vec::new(),
                }
            )
            .reduce(s, RootAction::Mid(a), out),
            RootAction::Old(n) => skeleton_infra::combine_into!(
                Buffered::new(OldLeaf),
                RootState.old,
                RootAction::Old,
                lift: |e: Effect<u32, Infallible>| match e {
                    Effect::Cancel { id } => vec![Effect::Cancel { id }],
                    _ => Vec::new(),
                }
            )
            .reduce(s, RootAction::Old(n), out),
        }
    }
}

#[derive(Default)]
struct Counting {
    cancelled: Arc<Mutex<u32>>,
}

impl EffectRuntime<RootAction, Infallible> for Counting {
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

fn store(
    seen: &Arc<Mutex<Vec<Option<usize>>>>,
    cancelled: &Arc<Mutex<u32>>,
    budget: usize,
) -> SyncStore<RootState, RootAction> {
    let s = SyncStore::new(
        Root {
            seen: Arc::clone(seen),
        },
        Counting {
            cancelled: Arc::clone(cancelled),
        },
    );
    s.set_segment_effect_budget(budget);
    s
}

/// 融合链共用一份预算：叶子在 `reduce_into` 停靠点观测到 `Some(预算 − 祖先已写)`；
/// fan-out 3 条/子效果也受同一上限约束（超界整段拒绝）。
#[test]
fn fused_chain_shares_one_budget_across_levels() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let cancelled = Arc::new(Mutex::new(0));
    // 预算 6：中间自己先写 1 条（根层 1:1）⇒ 叶子看到 5；叶子每条 ×2（中间层 lift）。
    let s = store(&seen, &cancelled, 6);

    // 超界：1（中间）+ 3×2（叶子 ×2 fan-out）= 7 > 6 ⇒ 整段拒绝、零转发。
    let over = s
        .dispatch_with_receipt(RootAction::Mid(MidAction::Leaf(3)))
        .expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(s.snapshot().mid.leaf, 0, "被拒转换不发布 next state");
    assert_eq!(*cancelled.lock().expect("cancelled 锁"), 0, "零转发");
    assert_eq!(
        *seen.lock().expect("seen 锁"),
        vec![Some(5)],
        "叶子看到的额度必须是「预算 6 − 中间已写 1」= 5（精确值，不是 None）"
    );

    // 上限内：1 + 2×2 = 5 ≤ 6 ⇒ 接受。
    let ok = s
        .dispatch_with_receipt(RootAction::Mid(MidAction::Leaf(2)))
        .expect("准入");
    assert_eq!(ok.wait(), DispatchOutcome::Completed);
    assert_eq!(s.snapshot().mid.leaf, 1);
    assert_eq!(
        *cancelled.lock().expect("cancelled 锁"),
        5,
        "1 + 2×2 条转发"
    );
    assert_eq!(
        *seen.lock().expect("seen 锁"),
        vec![Some(5), Some(5)],
        "第二次仍是同一预算下的 5"
    );
}

/// 精确零额度：父已写满预算 ⇒ 叶子看到 `Some(0)`，任何 push 都被拒并把溢出沿链传回。
#[test]
fn fused_chain_with_exhausted_budget_is_rejected_atomically() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let cancelled = Arc::new(Mutex::new(0));
    let s = store(&seen, &cancelled, 1);
    // 中间自己写 1 条就把预算用满 ⇒ 叶子看到 Some(0)。
    let over = s
        .dispatch_with_receipt(RootAction::Mid(MidAction::Leaf(1)))
        .expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(
        *seen.lock().expect("seen 锁"),
        vec![Some(0)],
        "用满 = 精确零额度（若退化成 None = 不限，越界就会静默放行）"
    );
    assert_eq!(*cancelled.lock().expect("cancelled 锁"), 0);
}

/// 旧 `Fragment` 经 `Buffered` 适配器进融合树：效果照常产出、预算照常约束。
#[test]
fn buffered_adapter_keeps_old_fragment_usable_in_fused_tree() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let cancelled = Arc::new(Mutex::new(0));
    let s = store(&seen, &cancelled, 4);

    let ok = s.dispatch_with_receipt(RootAction::Old(3)).expect("准入");
    assert_eq!(ok.wait(), DispatchOutcome::Completed);
    assert_eq!(
        *cancelled.lock().expect("cancelled 锁"),
        3,
        "旧路径 3 条照常转发"
    );
    assert!(seen.lock().expect("seen 锁").is_empty(), "未走叶子");

    // 超界仍整段拒绝（适配器走同一份有界兼容缓冲）。
    let over = s.dispatch_with_receipt(RootAction::Old(9)).expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(*cancelled.lock().expect("cancelled 锁"), 3, "被拒零转发");
}
