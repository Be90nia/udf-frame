#![cfg(all(feature = "testing", feature = "compose"))]
//! combine 组合器 + TestStore 测试面验收。
//!
//! 覆盖：combine 段（lift_fx 同形出参）与
//! TestStore 验收形状（send/receive/state + ExpectedEffect）。
//! tests 先行（TDD）：先有本文件失败用例，再有 lib.rs 实现。

use skeleton::{Effect, EffectBuf, EffectId, Fragment, Reducer};
use skeleton_infra::compose::combine;
use skeleton_infra::testing::{ExpectedEffect, TestStore};
use std::convert::Infallible;
use std::panic::{catch_unwind, AssertUnwindSafe};

// ── 固定装置：父 Feature 委托子切片 ──────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
struct ChildState {
    count: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum ChildAction {
    Inc,
}

#[derive(Debug, PartialEq)]
enum ChildOp {
    Notify,
}

/// 子 Feature：Inc 产 Run 效果（词表=本 feature 要世界做的事）
struct ChildFeature;

impl Reducer for ChildFeature {
    type State = ChildState;
    type Action = ChildAction;
    type Op = ChildOp;
    fn initial(&self) -> ChildState {
        ChildState { count: 0 }
    }
    fn reduce(
        &self,
        s: &ChildState,
        a: ChildAction,
        out: &mut EffectBuf<ChildAction, ChildOp>,
    ) -> ChildState {
        match a {
            ChildAction::Inc => {
                out.push(Effect::Run {
                    id: EffectId::new(1),
                    op: ChildOp::Notify,
                });
                ChildState { count: s.count + 1 }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ParentState {
    child: ChildState,
    done: bool,
}

#[derive(Clone, Debug, PartialEq)]
enum ParentAction {
    Child(ChildAction),
    Finish,
}

#[derive(Debug, PartialEq)]
enum ParentOp {
    ChildNotify,
    /// 不被任何分支产出：receive 负例的可构造「错值」
    Audit,
}

/// 子切片分片（组合根显式构造）：get/set 镜头 + wrap/unwrap 路由 + lift_fx 效果上提
fn parent_child_fragment(
) -> impl Fragment<State = ParentState, Action = ParentAction, Op = ParentOp> {
    combine(
        |ps: &ParentState| &ps.child,
        |ps: &mut ParentState, cs: ChildState| ps.child = cs,
        ParentAction::Child,
        |pa: &ParentAction| match pa {
            ParentAction::Child(ca) => Some(ca.clone()),
            ParentAction::Finish => None,
        },
        ChildFeature,
        |fx, out: &mut EffectBuf<ParentAction, ParentOp>| match fx {
            Effect::Run {
                id,
                op: ChildOp::Notify,
            } => {
                out.push(Effect::Run {
                    id,
                    op: ParentOp::ChildNotify,
                });
            }
            // Emit 不达 lift_fx（核心经 wrap 上浮）；穷尽性占位
            Effect::Emit(_) => {}
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
            // Effect 词表 non_exhaustive（跨 crate match 须通配）：未知新变体
            // fail-closed 崩溃带因，禁静默丢弃效果
            other => unreachable!("skeleton::Effect 新增变体未适配 lift_fx: {other:?}"),
        },
    )
}

/// 父 Feature：子路由 action 委托分片；自有 action 自理（Finish 产 Cancel）
struct ParentFeature;

impl Reducer for ParentFeature {
    type State = ParentState;
    type Action = ParentAction;
    type Op = ParentOp;
    fn initial(&self) -> ParentState {
        ParentState {
            child: ChildFeature.initial(),
            done: false,
        }
    }
    fn reduce(
        &self,
        s: &ParentState,
        a: ParentAction,
        out: &mut EffectBuf<ParentAction, ParentOp>,
    ) -> ParentState {
        match a {
            ParentAction::Child(_) => parent_child_fragment().reduce(s, a, out),
            ParentAction::Finish => {
                out.push(Effect::Cancel {
                    id: EffectId::new(9),
                });
                ParentState {
                    done: true,
                    ..s.clone()
                }
            }
        }
    }
}

// ── combine ─────────────────────────────────────────────────────────────

/// 分片语义 1：路由命中的子 action → 子切片迁移 + 子效果上提为父词表。
#[test]
fn combine_routes_child_action_and_lifts_effect() {
    let mut ts = TestStore::new(ParentFeature);
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    assert_eq!(
        ts.state(),
        &ParentState {
            child: ChildState { count: 1 },
            done: false
        },
        "子 action 经 unwrap 路由 → set 写回子切片"
    );
    ts.receive(ExpectedEffect::Run {
        id: Some(EffectId::new(1)),
        op: ParentOp::ChildNotify,
    });
}

/// 分片语义 2：未路由的父 action → 本层不动子切片（克隆返回、零效果），
/// 父 Feature 自有分支照常自理（含自有效果）。
#[test]
fn combine_unrouted_action_leaves_child_slice() {
    let mut ts = TestStore::new(ParentFeature);
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    ts.dispatch(ParentAction::Finish);
    assert_eq!(ts.state().child.count, 1, "父 action 不路由 → 子切片不动");
    assert!(ts.state().done, "父自有分支自理");
    // FIFO：先清第一条 send 的子效果，再验父自有 Cancel
    ts.receive(ExpectedEffect::Run {
        id: Some(EffectId::new(1)),
        op: ParentOp::ChildNotify,
    });
    ts.receive(ExpectedEffect::Cancel {
        id: Some(EffectId::new(9)),
    });
}

// ── combine! 宏：六闭包生成（人体工学验收：变体短形态组合面一行）────────

/// 宏显式形态（有 lift）：与 parent_child_fragment 逐参同义。
fn macro_fragment() -> impl Fragment<State = ParentState, Action = ParentAction, Op = ParentOp> {
    skeleton_infra::combine!(
        ChildFeature,
        ParentState.child,
        wrap: ParentAction::Child,
        unwrap: |pa: &ParentAction| match pa {
            ParentAction::Child(ca) => Some(ca.clone()),
            ParentAction::Finish => None,
        },
        lift: |fx, out: &mut EffectBuf<ParentAction, ParentOp>| match fx {
            Effect::Run { id, op: ChildOp::Notify } => {
                out.push(Effect::Run { id, op: ParentOp::ChildNotify });
            }
            Effect::Emit(_) => {}
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
            // Effect 词表 non_exhaustive（跨 crate match 须通配）：未知新变体
            // fail-closed 崩溃带因，禁静默丢弃效果
            other => unreachable!("skeleton::Effect 新增变体未适配 lift_fx: {other:?}"),
        },
    )
}

/// 宏产物与函数产物对拍：同输入恒同状态、同效果序列（路由命中/未路由两路）。
#[test]
fn combine_macro_fragment_matches_function_semantics() {
    let s0 = ParentState {
        child: ChildState { count: 0 },
        done: false,
    };
    let cases = [ParentAction::Child(ChildAction::Inc), ParentAction::Finish];
    for a in cases {
        let mut o_macro = EffectBuf::new();
        let mut o_fn = EffectBuf::new();
        let sm = macro_fragment().reduce(&s0, a.clone(), &mut o_macro);
        let sf = parent_child_fragment().reduce(&s0, a.clone(), &mut o_fn);
        assert_eq!(sm, sf, "{a:?}: 宏产物与函数产物状态同义");
        let em: Vec<_> = std::iter::from_fn(|| o_macro.pop_front()).collect();
        let ef: Vec<_> = std::iter::from_fn(|| o_fn.pop_front()).collect();
        assert_eq!(format!("{em:?}"), format!("{ef:?}"), "{a:?}: 效果序列同形");
    }
}

/// 变体短形态（lift 省略）：组合面一行——切片表达式 + 变体构造子即完装配
///（零效果词表的正形；对齐 compose 文档的 counter_fragment 例）。
#[test]
fn combine_macro_short_form_one_line_assembly() {
    #[derive(Clone, Debug, PartialEq)]
    struct P {
        total: u32,
        child: u32,
    }
    #[derive(Clone, Debug, PartialEq)]
    enum A {
        Child(u32),
        Touch,
    }

    struct Counter;
    impl Reducer for Counter {
        type State = u32;
        type Action = u32;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
            s + a
        }
    }

    fn short_fragment() -> impl Fragment<State = P, Action = A, Op = Infallible> {
        skeleton_infra::combine!(Counter, P.child, A::Child)
    }

    let f = short_fragment();
    let s0 = P {
        total: 10,
        child: 0,
    };
    let mut out = EffectBuf::new();
    let s1 = f.reduce(&s0, A::Child(7), &mut out);
    assert_eq!(
        s1,
        P {
            total: 10,
            child: 7
        },
        "短形态：子迁移+切片写回"
    );
    let mut out2 = EffectBuf::new();
    let s2 = f.reduce(&s1, A::Touch, &mut out2);
    assert_eq!(s2, s1, "短形态：未路由零漂移");
    assert!(out.is_empty() && out2.is_empty(), "零效果词表无效果可产");
}

// ── TestStore 验收形状 ───────────────────────────────────────────────────

/// 验收纪律 1+2：逐动作断言 + 效果回填断言（原样流程）。
#[test]
fn teststore_tracks_state_and_effects() {
    let mut ts = TestStore::new(ParentFeature);
    assert_eq!(ts.state().child.count, 0, "new 即 initial 态");
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    assert_eq!(ts.state().child.count, 2, "逐动作推进");
    ts.receive(ExpectedEffect::Run {
        id: None,
        op: ParentOp::ChildNotify,
    });
    ts.receive(ExpectedEffect::Run {
        id: None,
        op: ParentOp::ChildNotify,
    });
    assert!(matches!(ts.state(), ParentState { .. }));
}

/// 验收纪律 2 负例：期望 op 与实际不符 = panic（禁静默吞）。
#[test]
#[should_panic(expected = "effect mismatch")]
fn teststore_receive_op_mismatch_panics() {
    let mut ts = TestStore::new(ParentFeature);
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    ts.receive(ExpectedEffect::Run {
        id: None,
        op: ParentOp::Audit,
    });
}

/// 效果寻址校验：id: Some 不匹配 = panic。
#[test]
#[should_panic(expected = "effect mismatch")]
fn teststore_receive_id_mismatch_panics() {
    let mut ts = TestStore::new(ParentFeature);
    ts.dispatch(ParentAction::Child(ChildAction::Inc));
    ts.receive(ExpectedEffect::Run {
        id: Some(EffectId::new(2)),
        op: ParentOp::ChildNotify,
    });
}

/// 队列空时 receive = panic（效果缺失不可静默）。
#[test]
#[should_panic(expected = "no effect")]
fn teststore_receive_empty_queue_panics() {
    let mut ts: TestStore<u32, LoopAction, Infallible> = TestStore::new(QuietFeature);
    ts.dispatch(LoopAction::Start);
    ts.receive(ExpectedEffect::Cancel { id: None });
}

/// Emit 回灌由 store 核心以 Action 形态重入队列，从不进入效果转发路径——
/// 回环推进状态但不产生可 receive 的效果（脚本化语义的边界钉死）。
#[test]
#[should_panic(expected = "no effect")]
fn emit_loopback_never_enters_teststore_effect_queue() {
    let mut ts: TestStore<u32, LoopAction, Infallible> = TestStore::new(LoopOnceFeature);
    ts.dispatch(LoopAction::Start);
    assert_eq!(*ts.state(), 2, "Emit 回灌被排空：两次 reduce");
    ts.receive(ExpectedEffect::Cancel { id: None });
}

// ── 固定装置（零效果 / 单次 Emit 回环）───────────────────────────────────

#[derive(Debug)]
enum LoopAction {
    Start,
}

struct QuietFeature;

impl Reducer for QuietFeature {
    type State = u32;
    type Action = LoopAction;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: LoopAction, _out: &mut EffectBuf<LoopAction, Infallible>) -> u32 {
        s + 1
    }
}

struct LoopOnceFeature;

impl Reducer for LoopOnceFeature {
    type State = u32;
    type Action = LoopAction;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: LoopAction, out: &mut EffectBuf<LoopAction, Infallible>) -> u32 {
        if *s == 0 {
            out.push(Effect::Emit(LoopAction::Start));
        }
        s + 1
    }
}

// ── Emit 情形的 receive 消息如实归因 ───────────────────────────────────

/// panic payload 转消息（子串断言防 catch_unwind 假绿，同 robust_panic_recovery 纪律）。
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else {
        payload
            .downcast_ref::<String>()
            .cloned()
            .unwrap_or_else(|| "<non-string payload>".to_string())
    }
}

/// reducer 产过回灌 Emit 时，receive 空队列禁再报「no effect produced (queue
/// empty)」文不对题：消息必须如实陈述「N 个 Emit 已重入队列、永不进
/// receive 队列」并指正解（send + state 断言回环）。
#[test]
fn emit_feedback_receive_message_reports_reenqueued_truthfully() {
    let mut ts: TestStore<u32, LoopAction, Infallible> = TestStore::new(LoopOnceFeature);
    ts.dispatch(LoopAction::Start);
    let m = catch_unwind(AssertUnwindSafe(|| {
        ts.receive(ExpectedEffect::Cancel { id: None });
    }))
    .err()
    .map(|p| panic_message(p.as_ref()))
    .unwrap_or_else(|| "<no panic>".to_string());
    assert!(m.contains("no effect for consumer"), "消息本体：{m:?}");
    assert!(
        m.contains("1 emitted action(s) were re-enqueued as feedback"),
        "回灌事实计数必须在消息内：{m:?}"
    );
    assert!(
        !m.contains("queue empty"),
        "Emit 情形禁再报队列空（文不对题本体）：{m:?}"
    );
}
