//! Store/combine 行为面补测。每测守一条可观察契约；共享收集走 mpsc channel +
//! 原子量——测试零锁零毒化面。

use std::convert::Infallible;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};

use skeleton::{
    combine, ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, ExpectedEffect, Reducer,
    Store, TestStore,
};

// ── 小固定装置：计数器（订阅/并发/send/重入面共用）────────────────────────

/// Bump +1；Ping +1 且产 Cancel 效果（Infallible 下唯一可构造效果形，触发
/// 解释器转发）；Bonus +10。
enum Msg {
    Bump,
    Ping,
    Bonus,
}

struct Counter;

impl Reducer for Counter {
    type State = u32;
    type Action = Msg;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: Msg, out: &mut EffectBuf<Msg, Infallible>) -> u32 {
        match a {
            Msg::Bump => s + 1,
            Msg::Ping => {
                out.push(Effect::Cancel { id: EffectId(1) });
                s + 1
            }
            Msg::Bonus => s + 10,
        }
    }
}

struct NoFx;

impl EffectRuntime<Msg, Infallible> for NoFx {
    fn run(&mut self, _: Effect<Msg, Infallible>, _: &ActionSender<Msg>) {}
}

// ── combine 固定装置：父状态 { child: u32 }，子自增/自产 Emit/产 Cancel ──

#[derive(Clone, Debug, PartialEq)]
struct ParentState {
    child: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum ChildAct {
    Inc,
    /// 子 reducer 自产 Emit：核心经 wrap 上浮回灌（不经 lift_fx）
    EmitSelf,
    /// 子 reducer 产 Cancel 效果：经 lift_fx 显式上提
    Abort,
}

#[derive(Clone, Debug, PartialEq)]
enum ParentAct {
    Child(ChildAct),
}

#[derive(Debug, PartialEq)]
enum ParentOp {
    Notified,
}

struct Child;

impl Reducer for Child {
    type State = u32;
    type Action = ChildAct;
    type Op = ();
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: ChildAct, out: &mut EffectBuf<ChildAct, ()>) -> u32 {
        match a {
            ChildAct::Inc => s + 1,
            ChildAct::EmitSelf => {
                out.push(Effect::Emit(ChildAct::Inc));
                s + 1
            }
            ChildAct::Abort => {
                out.push(Effect::Cancel { id: EffectId(9) });
                s + 1
            }
        }
    }
}

fn fragment() -> impl Reducer<State = ParentState, Action = ParentAct, Op = ParentOp> {
    combine(
        |ps: &ParentState| &ps.child,
        |ps: &mut ParentState, child: u32| ps.child = child,
        |ca: ChildAct| ParentAct::Child(ca),
        |pa: &ParentAct| match pa {
            ParentAct::Child(ca) => Some(ca.clone()),
        },
        Child,
        |fx: Effect<ChildAct, ()>, out: &mut EffectBuf<ParentAct, ParentOp>| match fx {
            Effect::Run { id, op: () } => out.push(Effect::Run { id, op: ParentOp::Notified }),
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
            // Emit 由核心经 wrap 上浮，到不了 lift_fx——到达即核心分支断裂（fail-visible）
            Effect::Emit(_) => unreachable!("Emit 必须经核心 wrap 上浮，不经 lift_fx"),
        },
    )
}

/// 根 Feature：initial 显式构造含子切片初值的完整根状态（combine 产物无合法
/// initial），reduce 全量委托分片。
struct Root<F> {
    fragment: F,
}

impl<F> Reducer for Root<F>
where
    F: Reducer<State = ParentState, Action = ParentAct, Op = ParentOp>,
{
    type State = ParentState;
    type Action = ParentAct;
    type Op = ParentOp;
    fn initial(&self) -> ParentState {
        ParentState { child: 0 }
    }
    fn reduce(
        &self,
        ps: &ParentState,
        a: ParentAct,
        out: &mut EffectBuf<ParentAct, ParentOp>,
    ) -> ParentState {
        self.fragment.reduce(ps, a, out)
    }
}

/// 零覆盖 #2：子 reducer 自产的 Emit 不经 lift_fx——由核心经 wrap 上浮为父
/// Emit 回灌（Combined::reduce 的 Emit 分支，此前固定装置只产 Run，分支零覆盖）。
/// 可观察：一次 Child(EmitSelf) 后子切片 = 2（本 action +1、上浮回灌再 +1）；
/// 效果队列零残留（Emit 走核心回灌面，从不进入可 receive 面）。
/// 变异检出：Emit 被吞→子切片=1 红；误入 lift_fx→unreachable 红；wrap 产
/// 不可路由 action→回灌无效=1 红。
#[test]
fn combine_child_emitted_action_wraps_and_reenters_parent() {
    let mut ts = TestStore::new(Root { fragment: fragment() });
    ts.send(ParentAct::Child(ChildAct::EmitSelf));
    assert_eq!(ts.state().child, 2, "子产 Emit 必须经 wrap 上浮回灌：+1+1");
    // Emit 不留效果队列：receive 空队必 panic，且 panic 文案必须是「无效果」
    let leaked = std::panic::catch_unwind(AssertUnwindSafe(|| {
        ts.receive(ExpectedEffect::Cancel { id: None });
    }));
    let payload = match leaked {
        Err(payload) => payload,
        Ok(()) => panic!("子产 Emit 不得留效果队列（receive 必空）"),
    };
    let msg = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        msg.contains("no effect"),
        "receive 空队 panic 才是队面为空的证据，实际 panic: {msg}"
    );
}

/// 零覆盖 #3：子效果的 Cancel 经 lift_fx 显式上提（此前 Cancel 臂无子效果
/// 到达过——ParentFeature::Finish 的 Cancel 是父层自产，不经 combine）。
/// 可观察：Child(Abort) 产 Cancel{id=9} 逐值上提到父词表，子切片照常迁移。
#[test]
fn combine_child_cancel_lifts_through_lift_fx() {
    let mut ts = TestStore::new(Root { fragment: fragment() });
    ts.send(ParentAct::Child(ChildAct::Abort));
    ts.receive(ExpectedEffect::Cancel { id: Some(EffectId(9)) });
    assert_eq!(ts.state().child, 1, "子切片照常迁移（Cancel 上提不改迁移）");
}

/// 解释器回灌通道的重入 dispatch——排空持旗期间 send 只入队、
/// 由同一排空循环接管（重入语义的 fx 侧闸；订阅回调侧闸另测覆盖）。
/// 可观察：send 返回 true；外层 dispatch 返回即排至 idle（快照=11、队列=0）；
/// 通知序 = reduce 序 [1, 11]（重入不在效果转发内内联执行）。
struct ReentryFx {
    fired: bool,
    accepted: Arc<AtomicBool>,
}

impl EffectRuntime<Msg, Infallible> for ReentryFx {
    fn run(&mut self, _: Effect<Msg, Infallible>, back: &ActionSender<Msg>) {
        if !self.fired {
            self.fired = true;
            self.accepted.store(back.send(Msg::Bonus), Ordering::SeqCst);
        }
    }
}

#[test]
fn interpreter_backchannel_reentry_drains_to_idle_within_outer_dispatch() {
    let accepted = Arc::new(AtomicBool::new(false));
    let store = Arc::new(Store::new(
        Counter,
        ReentryFx { fired: false, accepted: Arc::clone(&accepted) },
    ));
    let (tx, rx) = mpsc::channel();
    let _sub = store.subscribe(move |s: &u32| {
        let _ = tx.send(*s);
    });
    store.dispatch(Msg::Ping);
    assert!(accepted.load(Ordering::SeqCst), "排空中的重入 send 必须被接受（true）");
    assert_eq!(*store.snapshot(), 11, "回灌由同一排空循环执行至 idle");
    assert_eq!(store.pending_actions(), 0, "dispatch 返回即队列空");
    assert_eq!(
        rx.try_iter().collect::<Vec<u32>>(),
        vec![1, 11],
        "通知序 = reduce 序：重入不在效果转发内内联执行"
    );
}

/// 零覆盖 #5（白盒 dead_subscription_slots_pruned_on_drain 的黑盒孪生）：
/// 票据丢弃后广播可见性立即成立——后续 dispatch 不再通知该回调；同表其他
/// 订阅者照常；下一次 dispatch 后计数读侧回落（死槽精确摘除）。
#[test]
fn dropped_subscription_stops_receiving_notifications() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (ta, ra) = mpsc::channel();
    let (tb, rb) = mpsc::channel();
    let _keep_a = store.subscribe(move |s: &u32| {
        let _ = ta.send(*s);
    });
    let ticket_b = store.subscribe(move |s: &u32| {
        let _ = tb.send(*s);
    });
    store.dispatch(Msg::Bump);
    drop(ticket_b);
    store.dispatch(Msg::Bump);
    assert_eq!(ra.try_iter().collect::<Vec<u32>>(), vec![1, 2], "在册订阅者照常通知");
    assert_eq!(rb.try_iter().collect::<Vec<u32>>(), vec![1], "票据丢弃后不再通知");
    assert_eq!(store.subscriber_count(), 1, "死槽在下一次 dispatch 后摘除");
}

/// 零覆盖 #6：订阅回调收到的 &S 是本次 reduce 后的新态（时序=reduce→快照
/// 落位→效果转发→通知）。帧序 [1, 2]——若回调拿到旧态则为 [0, 1]（红）。
#[test]
fn subscriber_receives_post_reduce_state_per_dispatch() {
    let (tx, rx) = mpsc::channel();
    let store = Store::new(Counter, NoFx);
    let _sub = store.subscribe(move |s: &u32| {
        let _ = tx.send(*s);
    });
    store.dispatch(Msg::Bump);
    store.dispatch(Msg::Bump);
    assert_eq!(rx.try_iter().collect::<Vec<u32>>(), vec![1, 2], "回调实参 = reduce 后新态");
    assert_eq!(*store.snapshot(), 2, "与最新快照一致");
}

/// 零覆盖 #7：共享句柄跨线程并发 send——「dispatch 与 send 同闸同窗」的 send
/// 路径此前只有单线程面，并发压力全走 dispatch。契约：4 线程 × 2500 次 send
/// 全部送达（零丢失零双计，终态 = N×K）；未闭门 send 恒 true。
#[test]
fn shared_sender_concurrent_send_delivers_every_action() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let sender = store.sender();
    let mut joins = Vec::new();
    for _ in 0..4 {
        let sender = sender.clone();
        joins.push(std::thread::spawn(move || {
            for _ in 0..2500 {
                assert!(sender.send(Msg::Bump), "未闭门 send 必须返回 true");
            }
        }));
    }
    for j in joins {
        j.join().expect("send 线程不得 panic");
    }
    assert_eq!(*store.snapshot(), 10_000, "零丢失零双计：终态 = 发送总量");
}

/// 白盒孪生功能面（skeleton lib 白盒 sender_handles_share_cached_arc 的黑盒
/// 孪生之二）：sender() 多克隆共享同一回灌通道——各克隆全部送达同一 store。
/// 分配面孪生（克隆零新增分配）在 skeleton-infra tests/memgov.rs。
#[test]
fn sender_clones_deliver_to_the_same_store() {
    let store = Store::new(Counter, NoFx);
    let a = store.sender();
    let b = store.sender();
    let c = a.clone();
    assert!(a.send(Msg::Bump) && b.send(Msg::Bump) && c.send(Msg::Bump));
    assert_eq!(*store.snapshot(), 3, "三个克隆句柄驱动同一 store");
}

/// `subscriber_count` 读侧实时判活——票据 drop 后读数立即回落，无「下一次
/// dispatch 的一拍滞后」。关键序：先 dispatch 让通知快照缓存装载槽位强引用
/// （旧实现的「续命」窗），再 drop 票据即时读——修复前此处读出旧值（缓存
/// 强引用撑着 upgrade 成功）。
#[test]
fn subscriber_count_is_live_immediately_after_ticket_drop() {
    let store = Store::new(Counter, NoFx);
    let _keep = store.subscribe(|_| {});
    let dropped = store.subscribe(|_| {});
    assert_eq!(store.subscriber_count(), 2);
    store.dispatch(Msg::Bump); // 通知缓存装载（旧实现死槽续命窗的前置）
    assert_eq!(store.subscriber_count(), 2);
    drop(dropped);
    assert_eq!(
        store.subscriber_count(),
        1,
        "票据 drop 后读数必须立即回落（读侧实时判活，无一拍滞后）"
    );
    store.dispatch(Msg::Bump);
    assert_eq!(store.subscriber_count(), 1, "dispatch 后仍为 1（不回归滞后语义）");
}
