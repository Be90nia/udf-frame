//! 最小运转环验收：
//! Op = Infallible 的零效果程序，编译期证明 Run 不可构造（Cancel 恒可构造、无注册时空转）。

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store};
use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Count(u32);

enum CounterAction {
    Initial,
    Inc,
}

// 非法状态无处构造：无 -1、无 bool 旗子（机制 #8）
struct CounterFeature;

impl Reducer for CounterFeature {
    type State = Count;
    type Action = CounterAction;
    type Op = Infallible;
    fn initial(&self) -> Count {
        Count(0)
    }
    fn reduce(
        &self,
        s: &Count,
        a: CounterAction,
        _out: &mut EffectBuf<CounterAction, Infallible>,
    ) -> Count {
        match a {
            CounterAction::Initial => s.clone(),
            CounterAction::Inc => Count(s.0 + 1), // match 穷尽免费
        }
    }
}

struct NullRuntime;

impl EffectRuntime<CounterAction, Infallible> for NullRuntime {
    fn run(&mut self, _effect: Effect<CounterAction, Infallible>, _back: &ActionSender<CounterAction>) {
    }
}

/// 零效果最小环（new → dispatch → snapshot）。
#[test]
fn minimal_loop_op_infallible() {
    let store = Store::new(CounterFeature, NullRuntime); // Op=Infallible → 无效果可运行
    store.dispatch(CounterAction::Initial);
    store.dispatch(CounterAction::Inc);
    assert_eq!(store.snapshot().0, 1);
}

/// 编译期证明：Op = Infallible 时 `Run` 的 op 字段无构造值，`match op {}` 编译通过
/// 即证明零效果程序在类型层结构性排除 Run（Cancel 恒可构造、无注册时空转）。
#[test]
fn infallible_op_structurally_excludes_run() {
    fn unwrap_emitted(a: Effect<&'static str, Infallible>) -> Option<&'static str> {
        match a {
            Effect::Emit(a) => Some(a),
            Effect::Run { op, .. } => match op {}, // Infallible 无值：不可达分支零成本消除
            Effect::Cancel { .. } => None,
        }
    }
    assert_eq!(unwrap_emitted(Effect::Emit("x")), Some("x"));
    assert_eq!(unwrap_emitted(Effect::Cancel { id: EffectId(0) }), None);
}

/// 环闭合最小语义：Emit 回灌重入队列被排空（0.3 完整环时序的核心骨架）。
#[test]
fn emit_effect_loops_back_through_queue() {
    enum Echo {
        Ping,
        Pong,
    }
    struct EchoFeature;
    impl Reducer for EchoFeature {
        type State = u32;
        type Action = Echo;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, a: Echo, out: &mut EffectBuf<Echo, Infallible>) -> u32 {
            match a {
                Echo::Ping => {
                    out.push(Effect::Emit(Echo::Pong));
                    s + 1
                }
                Echo::Pong => s + 1,
            }
        }
    }
    let store = Store::new(EchoFeature, NullRuntimeFor::<Echo>::default());
    store.dispatch(Echo::Ping);
    assert_eq!(*store.snapshot(), 2); // Ping + Emit(Pong) 回灌均被排空
}

struct NullRuntimeFor<A> {
    _marker: std::marker::PhantomData<fn() -> A>,
}

impl<A> Default for NullRuntimeFor<A> {
    fn default() -> Self {
        Self { _marker: std::marker::PhantomData }
    }
}

impl<A: 'static> EffectRuntime<A, Infallible> for NullRuntimeFor<A> {
    fn run(&mut self, _effect: Effect<A, Infallible>, _back: &ActionSender<A>) {}
}

/// reducer 多效果按写入顺序转发——解释器收序 = EffectBuf 写序。
#[test]
fn reducer_effects_forward_in_push_order() {
    enum Burst {
        Burst,
    }
    struct BurstFeature;
    impl Reducer for BurstFeature {
        type State = u32;
        type Action = Burst;
        type Op = u32;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: Burst, out: &mut EffectBuf<Burst, u32>) -> u32 {
            out.push(Effect::Run { id: EffectId(1), op: 1 });
            out.push(Effect::Run { id: EffectId(2), op: 2 });
            out.push(Effect::Run { id: EffectId(3), op: 3 });
            s + 1
        }
    }
    struct Recorder(Arc<Mutex<Vec<u32>>>);
    impl EffectRuntime<Burst, u32> for Recorder {
        fn run(&mut self, effect: Effect<Burst, u32>, _back: &ActionSender<Burst>) {
            if let Effect::Run { op, .. } = effect {
                self.0.lock().unwrap().push(op);
            }
        }
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = Store::new(BurstFeature, Recorder(Arc::clone(&seen)));
    store.dispatch(Burst::Burst);
    assert_eq!(*store.snapshot(), 1);
    assert_eq!(*seen.lock().unwrap(), vec![1, 2, 3], "转发顺序必须等于写入顺序");
}

/// 空效果 reducer 不触碰解释器（Run/Cancel 零转发）。
#[test]
fn reducer_without_effects_never_invokes_runtime() {
    struct Counting(Arc<AtomicUsize>);
    impl EffectRuntime<CounterAction, Infallible> for Counting {
        fn run(&mut self, _: Effect<CounterAction, Infallible>, _: &ActionSender<CounterAction>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let runs = Arc::new(AtomicUsize::new(0));
    let store = Store::new(CounterFeature, Counting(Arc::clone(&runs)));
    store.dispatch(CounterAction::Initial);
    store.dispatch(CounterAction::Inc);
    assert_eq!(store.snapshot().0, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 0, "零效果：解释器零调用");
}
