#![cfg(all(feature = "runtime", feature = "compose"))]
//! Store/combine 行为面补测。每测守一条可观察契约；共享收集走 mpsc channel +
//! 原子量——测试零锁零毒化面。

use std::convert::Infallible;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::compose::combine;
use skeleton_infra::runtime::SyncStore as Store;
use skeleton_infra::testing::{ExpectedEffect, TestStore};

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
                out.push(Effect::Cancel {
                    id: EffectId::new(1),
                });
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
                out.push(Effect::Cancel {
                    id: EffectId::new(9),
                });
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
            Effect::Run { id, op: () } => out.push(Effect::Run {
                id,
                op: ParentOp::Notified,
            }),
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
            // Emit 由核心经 wrap 上浮，到不了 lift_fx——到达即核心分支断裂（fail-visible）
            Effect::Emit(_) => unreachable!("Emit 必须经核心 wrap 上浮，不经 lift_fx"),
            // Effect 词表 non_exhaustive（跨 crate match 须通配）：未知新变体
            // fail-closed 崩溃带因，禁静默丢弃效果
            other => unreachable!("skeleton::Effect 新增变体未适配 lift_fx: {other:?}"),
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
    let mut ts = TestStore::new(Root {
        fragment: fragment(),
    });
    ts.dispatch(ParentAct::Child(ChildAct::EmitSelf));
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
    let mut ts = TestStore::new(Root {
        fragment: fragment(),
    });
    ts.dispatch(ParentAct::Child(ChildAct::Abort));
    ts.receive(ExpectedEffect::Cancel {
        id: Some(EffectId::new(9)),
    });
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
            self.accepted
                .store(back.dispatch(Msg::Bonus), Ordering::SeqCst);
        }
    }
}

#[test]
fn interpreter_backchannel_reentry_drains_to_idle_within_outer_dispatch() {
    let accepted = Arc::new(AtomicBool::new(false));
    let store = Arc::new(Store::new(
        Counter,
        ReentryFx {
            fired: false,
            accepted: Arc::clone(&accepted),
        },
    ));
    let (tx, rx) = mpsc::channel();
    let _sub = store.subscribe(move |s: &u32| {
        let _ = tx.send(*s);
    });
    store.dispatch(Msg::Ping);
    assert!(
        accepted.load(Ordering::SeqCst),
        "排空中的重入 dispatch 必须被接受（true）"
    );
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
    assert_eq!(
        ra.try_iter().collect::<Vec<u32>>(),
        vec![1, 2],
        "在册订阅者照常通知"
    );
    assert_eq!(
        rb.try_iter().collect::<Vec<u32>>(),
        vec![1],
        "票据丢弃后不再通知"
    );
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
    assert_eq!(
        rx.try_iter().collect::<Vec<u32>>(),
        vec![1, 2],
        "回调实参 = reduce 后新态"
    );
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
                assert!(sender.dispatch(Msg::Bump), "未闭门 dispatch 必须返回 true");
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
    assert!(a.dispatch(Msg::Bump) && b.dispatch(Msg::Bump) && c.dispatch(Msg::Bump));
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
    assert_eq!(
        store.subscriber_count(),
        1,
        "dispatch 后仍为 1（不回归滞后语义）"
    );
}

// ── 闭门 dispatch 的审计现场（可观测 R1：拒绝不止轮询计数，订阅者有节点可查）──

/// 闭门后 dispatch 的两条 push 路径（no-op 与 sender false）在审计时间线上
/// 同形落 `Rejected` 节点——审计订阅者对每次拒绝有现场可查，与轮询读侧
/// [`Store::rejected_after_close`] 互为对照（同一触发点各计一次）。
#[test]
fn closed_gate_dispatch_leaves_rejected_audit_scene() {
    use skeleton_infra::audit::AuditEventKind;

    let store = Store::new(Counter, NoFx);
    let _ticket = store.subscribe_events(64, |_| {});
    store.close();
    store.dispatch(Msg::Bump); // 早闸位
    assert!(!store.sender().dispatch(Msg::Bump), "sender 闭门显式 false");
    store.dispatch(Msg::Bump); // enqueue 复检闸位（闭门后早闸必先命中，此行只加固计数）
                               // 订阅窗内全部事件即三次拒绝（闭门后无其他节点），seq 单调连续。
    let events = store.take_events();
    assert_eq!(
        events.len(),
        3,
        "三次拒绝 = 三个 Rejected 现场，实际: {events:?}"
    );
    assert!(events.iter().all(|e| e.kind == AuditEventKind::Rejected));
    let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "取走序即发射序（seq 单调）");
    assert_eq!(
        store.rejected_after_close(),
        3,
        "审计事件与拒绝计数同一触发点对账"
    );
}

// ── 静默面显形四件（对齐 subscriber_panics 先例：计数+读侧+一测一档）──────

/// F-04：路由 miss（`unwrap` 返 `None`）不再纯静默——状态克隆原样返回的
/// 语义不变，但每次 miss 计入 `route_misses()`。「父层送错地方」与「父层
/// 自理」共享同一 `None`，计数是唯一区分线索。变异检出：miss 分支丢计数→
/// 断言 2 红；命中路径误计数→断言 4 红。
#[test]
fn combine_route_miss_is_counted_not_silent() {
    #[derive(Clone, Debug, PartialEq)]
    struct S {
        child: u32,
    }

    #[derive(Clone, Debug, PartialEq)]
    enum A {
        Child(u32),
        Unrouted,
    }

    struct ChildAdd;

    impl Reducer for ChildAdd {
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

    // 分片装配一次持有（计数随实例：按调用重建则读数随新实例清零）
    let frag = skeleton_infra::combine!(ChildAdd, S.child, A::Child);
    let s0 = S { child: 0 };
    let mut out: EffectBuf<A, Infallible> = EffectBuf::new();

    // miss：Unrouted 不在 unwrap 词表——状态原样克隆、零效果、计数 +1
    let s1 = frag.reduce(&s0, A::Unrouted, &mut out);
    assert_eq!(s1, s0, "miss 语义不变：状态克隆原样返回");
    assert_eq!(frag.route_misses(), 1, "路由 miss 必须显式留痕");

    // 命中：正常下沉路由，计数不动
    let s2 = frag.reduce(&s1, A::Child(5), &mut out);
    assert_eq!(s2.child, 5);
    assert_eq!(frag.route_misses(), 1, "命中路径不计数");
}

/// F-09：本地订阅只在注册线程触发（语义不变），但跨线程 dispatch 的 miss
/// 不再零信号：闸>0 而 dispatch 线程表查无登记即计入
/// `local_missed_notifications()`——宿主据此发现「本地档挂在了非 dispatch
/// 线程」。变异检出：miss 分支丢计数→断言 3 红；本地触发语义被误改→
/// 断言 1/末断言红。
#[test]
fn local_subscription_misses_are_counted_on_cross_thread_dispatch() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let hits = Arc::new(AtomicU64::new(0));

    // 基线：注册线程自 dispatch 正常触发，miss 零计
    let h = Arc::clone(&hits);
    let ticket = store.subscribe_local(move |_| {
        h.fetch_add(1, Ordering::SeqCst);
    });
    store.dispatch(Msg::Bump);
    assert_eq!(hits.load(Ordering::SeqCst), 1, "注册线程 dispatch 必触发");
    assert_eq!(store.local_missed_notifications(), 0);
    drop(ticket);

    // 跨线程：A 线程持有本地订阅，主线程 dispatch → 零触发 + miss 显形
    let (registered_tx, registered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let h = Arc::clone(&hits);
    let store_for_a = Arc::clone(&store);
    let handle = std::thread::spawn(move || {
        let h = Arc::clone(&h);
        let _ticket = store_for_a.subscribe_local(move |_| {
            h.fetch_add(1, Ordering::SeqCst);
        });
        registered_tx.send(()).expect("注册信号");
        release_rx.recv().expect("释放信号");
        store_for_a.dispatch(Msg::Bump); // 注册线程自 dispatch：本地档照常触发
    });
    registered_rx.recv().expect("等待 A 线程登记完成");
    store.dispatch(Msg::Bump);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "他线程 dispatch 不触达本地订阅（语义不变）"
    );
    assert_eq!(
        store.local_missed_notifications(),
        1,
        "跨线程 dispatch 的 miss 必须显式留痕"
    );
    release_tx.send(()).expect("放行 A 线程");
    handle.join().expect("A 线程不得 panic");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "注册线程自 dispatch 照常触发"
    );
    assert_eq!(
        store.local_missed_notifications(),
        1,
        "命中线程的 dispatch 不误计 miss"
    );
}

/// F-10：`try_subscribe` 闭门显式 Err——停机序写反（先 close 后 subscribe）
/// 在调用点显形；Err 路径登记未发生，`subscriptions_after_close` 保持
/// 「已登记死订阅」口径不掺水；旧 `subscribe` 保持签名 + 计数面（迁移期
/// 不静默）。变异检出：Err 路径误登记/误计数→断言 3 红；旧面计数丢失→
/// 末断言红。
#[test]
fn try_subscribe_after_close_is_explicit_err() {
    use skeleton_infra::runtime::StoreClosed;

    let store = Store::new(Counter, NoFx);
    let (tx, rx) = mpsc::channel();
    let ticket = store
        .try_subscribe(move |_| {
            tx.send(()).expect("通知信号");
        })
        .expect("未闭门登记必成功");
    store.dispatch(Msg::Bump);
    rx.recv().expect("订阅者必须收到通知");
    assert_eq!(store.subscriptions_after_close(), 0);

    store.close();
    assert!(
        matches!(store.try_subscribe(|_| {}), Err(StoreClosed)),
        "闭门后 try_subscribe 必须显式 Err"
    );
    assert_eq!(
        store.subscriptions_after_close(),
        0,
        "Err 路径登记未发生，死订阅计数不掺水"
    );

    // 旧面（迁移期兼容）：照常登记 + 计数，不静默
    let _late = store.subscribe(|_| {});
    assert_eq!(store.subscriptions_after_close(), 1, "旧面闭门登记照常计数");
    drop(ticket);
    drop(_late);
}

/// F-13：审计事件流单槽二次订阅显形——覆盖语义不变（仍是唯一订阅点），
/// 但覆盖留痕 `audit_preempted()`，「tracing 桥被自定义消费者静默顶掉」
/// 从此可查。变异检出：覆盖丢计数→断言 3 红；前订未被终止（误改多槽）→
/// 断言 4 红；新流未接通→断言 5 红。
#[test]
fn second_event_stream_subscription_preempts_and_leaves_trace() {
    let store = Store::new(Counter, NoFx);
    let first_hits = Arc::new(AtomicU64::new(0));
    let h = Arc::clone(&first_hits);
    let first = store.subscribe_events(16, move |_| {
        h.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(store.audit_preempted(), 0, "首订无覆盖");
    store.dispatch(Msg::Bump);
    let before = first_hits.load(Ordering::SeqCst);
    assert!(before > 0, "首订正常收事件");

    let _second = store.subscribe_events(16, |_| {});
    assert_eq!(store.audit_preempted(), 1, "二次订阅覆盖前订必须显式留痕");

    store.dispatch(Msg::Bump);
    assert_eq!(
        first_hits.load(Ordering::SeqCst),
        before,
        "被覆盖的前订不再收到事件（单槽语义不变）"
    );
    assert!(!store.take_events().is_empty(), "新订阅的事件流照常接通");
    assert_eq!(
        store.audit_preempted(),
        1,
        "覆盖一次计一次，后续 dispatch 不误计"
    );
    drop(first);
    drop(_second);
}
