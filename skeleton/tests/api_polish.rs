//! 公共 API 面回归。
//!
//! 覆盖：`Infallible` 根再导出 / `NoFx` 零效果缺省件 /
//! 具名 `Combine` 产物 / `subscribe_local` 单线程订阅。
//! `send` 之 `#[must_use]` 的红绿由 lib.rs `send` 文档的
//! `compile_fail` doctest 承载（裸调用必须被 `unused_must_use` 拦下），
//! 运行时断言无法表达 lint 契约，故不在本文件。
//!
//! `subscribe_local` 的线程纪律断言：本地订阅只在注册线程触发——他线程 dispatch
//! 不回调本地订阅；回调 panic 被 fail-closed 摘除并计入计数。

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use skeleton::{Combine, EffectBuf, Infallible, NoFx, Reducer, Store};

// ── 共用根件：零效果计数器 ─────────────────────────────────────────────

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

// ── 契约类型根再导出 ─────────────────────────────────────────────────────

/// `skeleton::Infallible` 必须与 `std::convert::Infallible` 同型：fn 指针
/// 双向强转只有同型才编译通过。缺再导出时本测试文件整体编译失败（红）。
#[test]
fn infallible_reexported_at_crate_root() {
    // 同型才能直传/互换：两条跨路径 fn 绑定各自即一次类型恒等检查
    fn std_to_crate(x: std::convert::Infallible) -> Infallible {
        x
    }
    fn crate_to_std(x: Infallible) -> std::convert::Infallible {
        x
    }
    let _fwd: fn(std::convert::Infallible) -> Infallible = std_to_crate;
    let _rev: fn(Infallible) -> std::convert::Infallible = crate_to_std;
}

// ── NoFx 消灭手写空运行时 ────────────────────────────────────────────────

/// 零效果最小环不再需要手写含死分支的运行时：`Store::new` 直配 `NoFx`。
#[test]
fn nofx_drives_zero_effect_store() {
    let store = Store::new(Counter, NoFx);
    store.dispatch(2);
    store.dispatch(3);
    assert_eq!(*store.snapshot(), 5, "同步排空：dispatch 返回即 idle");
    store.close();
    assert!(store.is_closed());
}

// ── combine 产物具名（Combine<A…>）──────────────────────────────────────

#[derive(Clone, PartialEq, Debug)]
struct ParentState {
    total: i32,
    child: i32,
}

#[derive(Clone, PartialEq, Debug)]
enum ParentAction {
    Bump,
    FromChild(ChildAction),
}

struct ChildCounter;

impl Reducer for ChildCounter {
    type State = i32;
    type Action = ChildAction;
    type Op = Infallible;
    fn initial(&self) -> i32 {
        0
    }
    fn reduce(&self, s: &i32, a: ChildAction, _out: &mut EffectBuf<ChildAction, Infallible>) -> i32 {
        match a {
            ChildAction::Add(n) => s + n,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
enum ChildAction {
    Add(i32),
}

/// 父件以**具名类型注解**持有 combine 产物（12 个闭包/子件参数以 `_` 推断）——
/// 匿名 `impl Reducer` 时代不可表达，具名化后采用方方可注解与别名。
struct Parent<R> {
    shard: R,
}

impl<R> Reducer for Parent<R>
where
    R: Reducer<State = ParentState, Action = ParentAction, Op = Infallible>,
{
    type State = ParentState;
    type Action = ParentAction;
    type Op = Infallible;
    fn initial(&self) -> ParentState {
        // 分片无合法 initial（恒 panic）：子切片初值由父件显式给出
        ParentState { total: 0, child: 0 }
    }
    fn reduce(
        &self,
        s: &ParentState,
        a: ParentAction,
        out: &mut EffectBuf<ParentAction, Infallible>,
    ) -> ParentState {
        match a {
            ParentAction::Bump => {
                let mut next = s.clone();
                next.total += 1;
                next
            }
            // 子路由动作整委托分片：镜头/上浮在片内自理（combine 的职责面）
            routed => self.shard.reduce(s, routed, out),
        }
    }
}

#[test]
#[allow(clippy::type_complexity)] // 卡点 7 验收物本身：具名 12 参注解（6 泛型+6 闭包）可写出即卡点的证明，复杂度是特性不是坏味道
fn combine_product_is_nameable_and_reducible() {
    let shard: Combine<_, _, Infallible, _, _, Infallible, _, _, _, _, _, _> = skeleton::combine(
        |s: &ParentState| &s.child,
        |s: &mut ParentState, v: i32| s.child = v,
        |ca: ChildAction| ParentAction::FromChild(ca),
        |pa: &ParentAction| match pa {
            ParentAction::FromChild(ca) => Some(ca.clone()),
            _ => None,
        },
        ChildCounter,
        |_fx: skeleton::Effect<ChildAction, Infallible>,
         _out: &mut EffectBuf<ParentAction, Infallible>| {},
    );
    let store = Store::new(Parent { shard }, NoFx);
    store.dispatch(ParentAction::FromChild(ChildAction::Add(5)));
    store.dispatch(ParentAction::Bump);
    let state = store.snapshot();
    assert_eq!(
        (state.child, state.total),
        (5, 1),
        "子片经具名分片委托迁移，父件自有分支照常"
    );
    store.close();
}

// ── subscribe_local 单线程订阅 ──────────────────────────────────────────

/// 回调免 Send：`Rc<RefCell<…>>` 直收快照（匿名 `impl Reducer` … 不——
/// 这正是免 Send 设计要放行的形态）。本地订阅不计入
/// `subscriber_count`（该计数只看跨线程订阅），关系在 doc 对照表。
#[test]
fn subscribe_local_accepts_non_send_callback() {
    let store = Store::new(Counter, NoFx);
    let seen = Rc::new(RefCell::new(Vec::<u32>::new()));
    let sink = Rc::clone(&seen);
    let sub = store.subscribe_local(move |s: &u32| sink.borrow_mut().push(*s));
    assert_eq!(store.subscriber_count(), 0, "本地订阅不入跨线程订阅计数");
    store.dispatch(1);
    store.dispatch(2);
    assert_eq!(*seen.borrow(), vec![1, 3], "注册线程 dispatch 同步触发本地回调（通知态=迁移后新态，与跨线程订阅一致）");
    drop(sub);
    store.dispatch(3);
    assert_eq!(*seen.borrow(), vec![1, 3], "票据 Drop = 注销");
}

/// 线程纪律：本地订阅只在注册线程触发。他线程 dispatch 不回调（其线程
/// 无本地登记），注册线程 dispatch 正常触发。
#[test]
fn subscribe_local_notifies_only_on_registering_thread() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let seen = Rc::new(RefCell::new(0u32));
    let sink = Rc::clone(&seen);
    let _sub = store.subscribe_local(move |s: &u32| *sink.borrow_mut() += *s);
    let foreign = Arc::clone(&store);
    std::thread::spawn(move || foreign.dispatch(10))
        .join()
        .expect("他线程 dispatch 不得 panic（本地订阅不得被跨界触达）");
    assert_eq!(*seen.borrow(), 0, "他线程 dispatch 不触发本地订阅");
    store.dispatch(5);
    assert_eq!(*seen.borrow(), 15, "注册线程 dispatch 正常触发（5+10 累计后新态）");
}

/// 回调 panic 的 fail-closed 摘除与共享订阅同一纪律：摘除 + 计数，
/// 健康订阅者不受染，已摘除者不重复计。
#[test]
fn subscribe_local_panic_removal_is_counted() {
    let store = Store::new(Counter, NoFx);
    let fired = Rc::new(RefCell::new(0u32));
    let _poison = store.subscribe_local(|_s: &u32| panic!("local subscriber bug"));
    let healthy = Rc::clone(&fired);
    let _keep = store.subscribe_local(move |s: &u32| *healthy.borrow_mut() += *s);
    store.dispatch(1);
    assert_eq!(*fired.borrow(), 1, "毒订阅者摘除后健康者照常通知");
    assert_eq!(store.subscriber_panics(), 1, "panic 摘除显式留痕");
    store.dispatch(2);
    assert_eq!(*fired.borrow(), 4, "已摘除者不再触发（健康者累计新态 1+3）");
    assert_eq!(store.subscriber_panics(), 1, "已摘除者不重复计数");
}

// ── EffectId 可作 Hash 键（宿主寻址表承诺落地）─────────────────────────

/// doc 许诺「宿主寻址表用值比对」——`HashMap` 键位直接可用（缺 `Hash` 时本
/// 文件整体编译失败，即红）。
#[test]
fn effect_id_keys_hash_maps() {
    use std::collections::HashMap;
    let mut inflight: HashMap<skeleton::EffectId, &'static str> = HashMap::new();
    inflight.insert(skeleton::EffectId(1), "timer");
    inflight.insert(skeleton::EffectId(2), "http");
    assert_eq!(inflight.get(&skeleton::EffectId(1)), Some(&"timer"));
    assert_eq!(inflight.len(), 2);
}

// ── 订阅回调 panic 的成因摘要留痕 ──────────────────────────────────────

/// 摘除计数回答「谁死了」，成因摘要回答「为什么」——payload 为字符串时存
/// 原文（截断），读侧可查；无 panic 时读侧恒 `None`。
#[test]
fn subscriber_panic_cause_is_recorded_and_readable() {
    let store = Store::new(Counter, NoFx);
    assert_eq!(store.subscriber_panic_last_cause(), None, "无 panic 无成因");
    let _poison = store.subscribe(|_| panic!("subscriber bug: watched value went negative"));
    let _keep = store.subscribe(|_| {});
    store.dispatch(1);
    assert_eq!(store.subscriber_panics(), 1);
    let cause = store
        .subscriber_panic_last_cause()
        .expect("panic 摘除必须留成因摘要");
    assert!(
        cause.contains("watched value went negative"),
        "摘要须含 payload 原文，实际: {cause}"
    );
}

/// 本地订阅回调 panic 同样留成因（两条摘除路径同一纪律）。
#[test]
fn local_subscriber_panic_cause_is_recorded_too() {
    let store = Store::new(Counter, NoFx);
    let _poison = store.subscribe_local(|_s: &u32| panic!("local watcher exploded"));
    store.dispatch(1);
    assert_eq!(store.subscriber_panics(), 1);
    let cause = store.subscriber_panic_last_cause().expect("本地路径同样留痕");
    assert!(cause.contains("local watcher exploded"), "实际: {cause}");
}

// ── 闭门后登记的订阅显形 ───────────────────────────────────────────────

/// 闭门后 subscribe/subscribe_local 照常登记但永无通知（已知取舍）——本计数
/// 让「登记生效但通知永不达」从观测死角显形。与 rejected_after_close 分立：
/// 登记不是拒绝（票据有效、可注销、count 可查）。
#[test]
fn subscriptions_after_close_are_counted() {
    let store = Store::new(Counter, NoFx);
    let _before = store.subscribe(|_| {});
    store.dispatch(1);
    assert_eq!(store.subscriptions_after_close(), 0, "开门登记不计");
    store.close();
    let _a = store.subscribe(|_| {});
    let _b = store.subscribe_local(|_| {});
    assert_eq!(
        store.subscriptions_after_close(),
        2,
        "闭门登记显形（跨线程+本地同计）"
    );
}
