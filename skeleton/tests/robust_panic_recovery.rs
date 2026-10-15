//! 健壮性修复回归：解释器 panic / S::Clone panic / 效果吞没 / 回灌活锁四景。
//!
//! - 解释器 panic、效果续转、回灌预算三测为红测转绿（修复前必败）；
//! - S::Clone 锁内 panic 为文档化显式契约（必须 panic-free，违者实例报废），
//!   行为前后不变——本测为契约金丝，钉死「报废」语义防未来漂移。

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store};
use std::convert::Infallible;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

/// panic payload 转消息（断言消息子串，防 catch_unwind 假绿）。
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic>".to_string()
    }
}

/// 零副作用解释器（Infallible 词表程序用）。
struct NopFx<A> {
    _m: PhantomData<fn() -> A>,
}

impl<A: 'static> EffectRuntime<A, Infallible> for NopFx<A> {
    fn run(&mut self, _: Effect<A, Infallible>, _: &ActionSender<A>) {}
}

/// 对任何 Run 效果先「记录」后 panic 的解释器（转发 panic 配方本体）。
struct BoomRuntime<A, Op> {
    _m: PhantomData<fn() -> (A, Op)>,
}

impl<A: 'static, Op: 'static> EffectRuntime<A, Op> for BoomRuntime<A, Op> {
    fn run(&mut self, _: Effect<A, Op>, _: &ActionSender<A>) {
        panic!("interpreter boom (drill)");
    }
}

// ── 解释器 panic 毒锁恢复 ────────────────────────────────────────────────

enum DemoAction {
    Run1,
    Noop,
}
enum BoomOp {
    Boom,
}

struct PanicRunFeature;

impl Reducer for PanicRunFeature {
    type State = u32;
    type Action = DemoAction;
    type Op = BoomOp;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: DemoAction, out: &mut EffectBuf<DemoAction, BoomOp>) -> u32 {
        match a {
            // 配方：解释器对 Effect::Run 先记录后 panic!
            DemoAction::Run1 => {
                out.push(Effect::Run { id: EffectId(1), op: BoomOp::Boom });
                s + 1
            }
            DemoAction::Noop => *s,
        }
    }
}

/// 解释器 panic 毒化 runtime mutex 后，实例不得半砖——第二次产效果
/// dispatch 不得报 "mutex poisoned"（毒锁恢复），无效果路径与 snapshot 照常，
/// 状态前进如实保留（两次 reduce 均已落账）。
#[test]
fn interpreter_panic_does_not_brick_runtime_mutex() {
    let store = Store::new(PanicRunFeature, BoomRuntime::<DemoAction, BoomOp> { _m: PhantomData });
    let r1 = catch_unwind(AssertUnwindSafe(|| store.dispatch(DemoAction::Run1)));
    assert!(r1.is_err(), "解释器 panic 必须如实传播（fail-visible）");
    // 修复前：runtime mutex 中毒，本 dispatch 恒 panic "mutex poisoned"（半砖）
    let r2 = catch_unwind(AssertUnwindSafe(|| store.dispatch(DemoAction::Run1)));
    let msg = r2.as_ref().err().map(|b| panic_message(b.as_ref())).unwrap_or_default();
    assert!(!msg.contains("poisoned"), "毒锁必须恢复、实例不得半砖（实际 panic：{msg}）");
    // 计数上浮：恢复过一次 = 第一个 panic 已计入（第二个 panic 待下次效果转发恢复）
    assert_eq!(store.runtime_panic_count(), 1, "panic 事件必须经计数显式上浮");
    store.dispatch(DemoAction::Noop);
    assert_eq!(*store.snapshot(), 2, "两次 reduce 已落账：状态前进必须如实保留");
}

// ── S::Clone 锁内 panic 整体变砖（文档化契约）────────────────────────────

struct CloneBoomState {
    gate: Arc<AtomicBool>,
}

impl Clone for CloneBoomState {
    fn clone(&self) -> Self {
        if self.gate.load(Ordering::SeqCst) {
            panic!("S::clone boom (drill)");
        }
        CloneBoomState { gate: Arc::clone(&self.gate) }
    }
}

struct CloneBoomFeature {
    gate: Arc<AtomicBool>,
}

impl Reducer for CloneBoomFeature {
    type State = CloneBoomState;
    type Action = DemoAction;
    type Op = Infallible;
    fn initial(&self) -> CloneBoomState {
        CloneBoomState { gate: Arc::clone(&self.gate) }
    }
    // 直构不走 trait clone：clone 闸只由 make_mut 克隆分支触发
    fn reduce(
        &self,
        s: &CloneBoomState,
        _: DemoAction,
        _: &mut EffectBuf<DemoAction, Infallible>,
    ) -> CloneBoomState {
        CloneBoomState { gate: Arc::clone(&s.gate) }
    }
}

/// 契约金丝（行为前后不变）：外部持有快照时状态落位走 make_mut 克隆
/// 分支，`S::clone` 在 state 锁内 panic = 毒锁 = 实例整体报废（snapshot/dispatch
/// 从此恒 panic，状态不可取出）。此为文档化显式契约（Store doc「Panic 契约」节），
/// 非可恢复路径——钉死语义防未来实现漂移。
#[test]
fn clone_panic_inside_state_lock_bricks_instance_by_contract() {
    let store = Store::new(
        CloneBoomFeature { gate: Arc::new(AtomicBool::new(false)) },
        NopFx::<DemoAction> { _m: PhantomData },
    );
    let held = store.snapshot(); // 外部持有：强计数 ≥2 → make_mut 必走克隆分支
    held.gate.store(true, Ordering::SeqCst);
    let r = catch_unwind(AssertUnwindSafe(|| store.dispatch(DemoAction::Noop)));
    assert!(r.is_err(), "make_mut 克隆分支的 S::clone panic 必须如实传播");
    let brick1 = catch_unwind(AssertUnwindSafe(|| store.snapshot()));
    let msg1 = brick1.as_ref().err().map(|b| panic_message(b.as_ref())).unwrap_or_default();
    assert!(msg1.contains("poisoned"), "契约：state 毒锁 → snapshot 报废（实际：{msg1}）");
    drop(held);
    let brick2 = catch_unwind(AssertUnwindSafe(|| store.snapshot()));
    assert!(brick2.is_err(), "F2 契约：报废不因外部持有释放而解除（毒锁粘滞）");
}

/// 毒锁消息必须自含——子系统名（skeleton Store state lock）+ 契约指位
/// （Store「Panic 契约」文档节）+ 严重度（实例整体报废、状态不可取出），
/// 禁裸 std PoisonError 让排查者再翻文档。
#[test]
fn poisoned_lock_message_names_subsystem_and_contract() {
    let store = Store::new(
        CloneBoomFeature { gate: Arc::new(AtomicBool::new(false)) },
        NopFx::<DemoAction> { _m: PhantomData },
    );
    let held = store.snapshot(); // 外部持有：make_mut 必走克隆分支
    held.gate.store(true, Ordering::SeqCst);
    let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(DemoAction::Noop)));
    let brick = catch_unwind(AssertUnwindSafe(|| store.snapshot()));
    let msg = brick.as_ref().err().map(|b| panic_message(b.as_ref())).unwrap_or_default();
    assert!(
        msg.contains("skeleton Store state lock poisoned"),
        "子系统名必须在消息内（非自含缺陷本体）：{msg}"
    );
    assert!(msg.contains("Panic 契约"), "必须指向 Store Panic 契约文档节：{msg}");
    assert!(msg.contains("状态不可取出"), "严重度判断必须在消息内：{msg}");
}

// ── 部分转发失败静默吞效果 ───────────────────────────────────────────────

enum TrioAction {
    Noop,
    Inc,
    EmitRunEmit,
}

struct TrioFeature;

impl Reducer for TrioFeature {
    type State = u32;
    type Action = TrioAction;
    type Op = BoomOp;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: TrioAction, out: &mut EffectBuf<TrioAction, BoomOp>) -> u32 {
        match a {
            TrioAction::Noop => *s,
            TrioAction::Inc => s + 1,
            // 配方：缓冲 [Emit(Noop), Run, Emit(Inc)]，Run 转发即 panic
            TrioAction::EmitRunEmit => {
                out.push(Effect::Emit(TrioAction::Noop));
                out.push(Effect::Run { id: EffectId(2), op: BoomOp::Boom });
                out.push(Effect::Emit(TrioAction::Inc));
                *s
            }
        }
    }
}

/// 转发中途 panic 时未转发效果不得被下一次 dispatch 的 clear() 无声吞掉——
/// 残留 Emit(Inc) 必须在恢复排空续转，终态 1（健康语义；修复前实测终态 0）。
#[test]
fn residual_effects_forwarded_after_forward_panic() {
    let store = Store::new(TrioFeature, BoomRuntime::<TrioAction, BoomOp> { _m: PhantomData });
    let r = catch_unwind(AssertUnwindSafe(|| store.dispatch(TrioAction::EmitRunEmit)));
    assert!(r.is_err(), "转发 panic 必须如实传播");
    store.dispatch(TrioAction::Noop);
    assert_eq!(
        *store.snapshot(),
        1,
        "panic 前已产效果禁吞——残留 Emit(Inc) 必须续转（修复前被 clear 吞掉终态 0）"
    );
}

// ── 自指 Emit 回灌活锁无看门狗 ───────────────────────────────────────────

enum SpinAction {
    Spin,
}

struct SpinFeature;

impl Reducer for SpinFeature {
    type State = u32;
    type Action = SpinAction;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: SpinAction, out: &mut EffectBuf<SpinAction, Infallible>) -> u32 {
        // 自指回灌：恒 Emit 自身——修复前排空环永不达空
        out.push(Effect::Emit(SpinAction::Spin));
        *s
    }
}

/// 自指回灌活锁必须在回灌深度预算内止步——dispatch 返回（非 panic 非静默
/// no-op，结构化错误经计数上浮）。修复前 dispatch 永不返回，recv_timeout 必超时。
#[test]
fn self_emit_feedback_livelock_budget_trips() {
    let store = Arc::new(Store::new(SpinFeature, NopFx::<SpinAction> { _m: PhantomData }));
    let (tx, rx) = mpsc::channel();
    let s2 = Arc::clone(&store);
    std::thread::spawn(move || {
        s2.dispatch(SpinAction::Spin);
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_millis(500))
        .expect("dispatch 必须在回灌深度预算内返回（修复前活锁永挂）");
    assert_eq!(
        store.feedback_budget_exceeded(),
        1,
        "超限必须结构化上浮（计数读侧可见，非 panic 非静默 no-op）"
    );
    assert_eq!(*store.snapshot(), 0, "自指 Spin 不推进状态");
}

// ── 解释器 back 通道回灌绕预算 ─────────────────────────────────────────
// 复现配方：reducer 对每个 action 产 Run 效果，解释器 run 内无条件
// back.send(...)——同栈回灌曾以 External 标签入队、深度每次清零，预算永不
// 触发，dispatch 永挂且 feedback_budget_exceeded 恒 0（无任何计数留痕）。

/// Spin 走 Run 效果载荷（解释器回灌），Bump 为外部流量探针。
enum LoopbackAction {
    Spin,
    Bump,
}

#[derive(Debug, PartialEq)]
enum LoopbackOp {
    Spin,
}

struct LoopbackFeature;

impl Reducer for LoopbackFeature {
    type State = u32;
    type Action = LoopbackAction;
    type Op = LoopbackOp;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: LoopbackAction, out: &mut EffectBuf<LoopbackAction, LoopbackOp>) -> u32 {
        match a {
            LoopbackAction::Spin => {
                out.push(Effect::Run { id: EffectId(9), op: LoopbackOp::Spin });
                *s
            }
            LoopbackAction::Bump => s + 1,
        }
    }
}

/// 复现配方解释器：每条 Run 无条件同栈回灌 Spin。
struct BackSpinFx;

impl EffectRuntime<LoopbackAction, LoopbackOp> for BackSpinFx {
    fn run(&mut self, effect: Effect<LoopbackAction, LoopbackOp>, back: &ActionSender<LoopbackAction>) {
        if let Effect::Run { .. } = effect {
            let _ = back.send(LoopbackAction::Spin);
        }
    }
}

/// 红绿：back 通道同栈回灌必须计入深度预算——dispatch 必返回（看门狗：
/// 修复前此测死挂，由 recv_timeout 击杀转红），65th 回灌结构化上浮。
#[test]
fn back_channel_feedback_counts_toward_budget() {
    let store = Arc::new(Store::new(LoopbackFeature, BackSpinFx));
    let (tx, rx) = mpsc::channel();
    let s2 = Arc::clone(&store);
    std::thread::spawn(move || {
        s2.dispatch(LoopbackAction::Spin);
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_millis(500))
        .expect("back 通道回灌必须计入预算，dispatch 必返回（修复前同栈回灌深度清零永挂）");
    assert_eq!(
        store.feedback_budget_exceeded(),
        1,
        "65th 回灌必须结构化上浮（计数读侧可见，非静默非 panic）"
    );
    assert_eq!(*store.snapshot(), 0, "Spin 不推进状态");
    assert_eq!(store.pending_actions(), 1, "预算点回灌残留零丢失");
}

/// 伴生回归：预算止步的残留链不得吞掉后续外部流量，也不得丢 action。
/// 残留退队尾后外部 action 照常推进（每任排空在撞上残留前先处理既有外部
/// 流量；病理链被限流是预算的职责，饿死健康流量不是）。
#[test]
fn budget_residual_does_not_starve_external_traffic() {
    let store = Arc::new(Store::new(LoopbackFeature, BackSpinFx));
    let (tx, rx) = mpsc::channel();
    let s2 = Arc::clone(&store);
    std::thread::spawn(move || {
        s2.dispatch(LoopbackAction::Spin);
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_millis(500)).expect("预算必须止步");
    assert_eq!(store.pending_actions(), 1, "前置：残留尚在队列");
    let mut n = 0;
    while *store.snapshot() < 200 {
        store.dispatch(LoopbackAction::Bump);
        n += 1;
        assert!(n <= 400, "外部流量被残留链饿死（200 个 Bump 未在 400 次 dispatch 内完成）");
    }
    assert_eq!(*store.snapshot(), 200, "外部流量零丢失");
    assert!(store.feedback_budget_exceeded() >= 1, "预算必须曾触发");
}

/// 伴生回归：同线程嵌套排空的深度守卫按栈配对——A 排空中（解释器内）
/// 重入 dispatch B，B 的嵌套排空正常完成且不串改 A 的在途回灌深度（守卫
/// save/restore）；A 的链照常在 65th 回灌止步，后续外部 action 不因线程局部
/// 状态泄漏被误标回灌而瞬间触顶。
struct PlainB;

impl Reducer for PlainB {
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

struct NestedThenSpin {
    b: Arc<Store<u32, u32, Infallible>>,
}

impl EffectRuntime<LoopbackAction, LoopbackOp> for NestedThenSpin {
    fn run(&mut self, effect: Effect<LoopbackAction, LoopbackOp>, back: &ActionSender<LoopbackAction>) {
        if let Effect::Run { .. } = effect {
            self.b.dispatch(1); // 嵌套排空 B（同线程，A 的深度账在途）
            let _ = back.send(LoopbackAction::Spin);
        }
    }
}

#[test]
fn nested_store_drain_keeps_depth_accounting_paired() {
    let b = Arc::new(Store::new(PlainB, NopFx::<u32> { _m: PhantomData }));
    let store = Arc::new(Store::new(LoopbackFeature, NestedThenSpin { b: Arc::clone(&b) }));
    let (tx, rx) = mpsc::channel();
    let s2 = Arc::clone(&store);
    std::thread::spawn(move || {
        s2.dispatch(LoopbackAction::Spin);
        let _ = tx.send(());
    });
    rx.recv_timeout(Duration::from_millis(500)).expect("A 的预算必须正常止步");
    // 65 个 Spin（Ext + FB1..64）各嵌套排空 B 一次；65th 回灌止步
    assert_eq!(*b.snapshot(), 65, "嵌套排空的 B 必须完整处理，不受 A 的预算影响");
    assert_eq!(store.feedback_budget_exceeded(), 1, "A 的深度账不因嵌套排空串账");
    // 泄漏哨兵：泄漏的 (A_id, 大深度) 会把后续 Bump 误标回灌而瞬间触顶，
    // 状态永不前进——能推进即证明守卫按栈恢复干净
    let mut n = 0;
    while *store.snapshot() < 1 {
        store.dispatch(LoopbackAction::Bump);
        n += 1;
        assert!(n <= 400, "泄漏：外部 action 被误标回灌深度，永远触顶");
    }
}
