#![cfg(all(feature = "runtime", feature = "compose", feature = "log"))]
//! 可观测计数读面的并发精确性：≥4 线程并发触发同一 miss/覆盖/拒绝路径，
//! join 后读数必须**精确等于**触发次数——计数丢失（fetch_add 竞态）与重复
//! 计数一并显形（「≥」式断言放不过丢计）。
//!
//! 可见性依据：`thread::join` 返回与被 join 线程的全部写入建立
//! happens-before——join 后读侧无论 `Relaxed`/`Acquire`/`SeqCst` 都可见计数
//! 终值。读侧内存序无需为此加强：原子性防撕读、join 边缘防不可见，本文件
//! 即该契约的回归面；产品读侧维持原序不动。
//!
//! 单线程双向断言（基线/触发/未触发不涨）由 store_behavior_gaps（F-04/F-09/
//! F-10/F-13）、api_polish（cancel_unknown_id/effect_id）、async_notify
//! （notify_published）、logx_channels（dropped）承载；本文件每测仍先取
//! 基线再触发，不依赖「新实例必零」的隐含前提。

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc};

use skeleton::notify::{Notify as _, NotifySink};
use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Fragment, Reducer, Store as _,
};
use skeleton_infra::logx::{Channel, ChannelRegistry, ChannelSink};
use skeleton_infra::runtime::{StoreClosed, SyncStore as Store};

const THREADS: usize = 4;
const PER_THREAD: u64 = 64;

// ── 固定装置 ────────────────────────────────────────────────────────────────

struct Counter;

impl Reducer for Counter {
    type State = u64;
    type Action = u64;
    type Op = Infallible;
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        s + a
    }
}

struct NoFx;

impl EffectRuntime<u64, Infallible> for NoFx {
    fn run(&mut self, _: Effect<u64, Infallible>, _: &ActionSender<u64>) {}
}

/// Run1 登记 id 1；Cancel1 引用已见 id；Cancel9 引用悬空 id。
enum CMsg {
    Run1,
    Cancel1,
    Cancel9,
}

struct RunCancel;

impl Reducer for RunCancel {
    type State = u32;
    type Action = CMsg;
    type Op = ();
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: CMsg, out: &mut EffectBuf<CMsg, ()>) -> u32 {
        match a {
            CMsg::Run1 => {
                out.push(Effect::Run {
                    id: EffectId::new(1),
                    op: (),
                });
                s + 1
            }
            CMsg::Cancel1 => {
                out.push(Effect::Cancel {
                    id: EffectId::new(1),
                });
                *s
            }
            CMsg::Cancel9 => {
                out.push(Effect::Cancel {
                    id: EffectId::new(9),
                });
                *s
            }
        }
    }
}

struct NoFxC;

impl EffectRuntime<CMsg, ()> for NoFxC {
    fn run(&mut self, _: Effect<CMsg, ()>, _: &ActionSender<CMsg>) {}
}

/// 路由 miss 面固定装置：父状态 `{ child: u32 }`，`Stray` 是本分片不认识的
/// action（unwrap 返 None ⇒ miss）。combine! 内联装配保持具名类型——
/// `route_misses` 是 `Combine` 固有方法，`impl Fragment` 返回形态会把它擦掉。
#[derive(Clone, Debug, PartialEq)]
struct ParentState {
    child: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum ParentAct {
    Child(u32),
    Stray,
}

struct ChildFragment;

impl Reducer for ChildFragment {
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

// ── ① route_misses：并发 miss 精确计数 ─────────────────────────────────────

/// 每线程 PER_THREAD 次 `Stray`（unwrap 返 None）直接打 `Fragment::reduce`
/// （&self 并发合法），join 后读数 = 单发 1 + THREADS×PER_THREAD，精确等值；
/// 命中路径不涨。
#[test]
fn route_misses_concurrent_misses_are_exact() {
    let frag = Arc::new(skeleton_infra::combine!(
        ChildFragment,
        ParentState.child,
        wrap: |ca: u32| ParentAct::Child(ca),
        unwrap: |pa: &ParentAct| match pa {
            ParentAct::Child(ca) => Some(*ca),
            ParentAct::Stray => None,
        },
    ));
    let s0 = ParentState { child: 0 };
    assert_eq!(frag.route_misses(), 0, "基线");
    let mut out: EffectBuf<ParentAct, Infallible> = EffectBuf::new();
    let _ = frag.reduce(&s0, ParentAct::Stray, &mut out);
    assert_eq!(frag.route_misses(), 1, "单发 miss 留痕");

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let frag = Arc::clone(&frag);
            let s0 = s0.clone();
            std::thread::spawn(move || {
                let mut out = EffectBuf::new();
                for _ in 0..PER_THREAD {
                    let _ = frag.reduce(&s0, ParentAct::Stray, &mut out);
                }
            })
        })
        .collect();
    for j in joins {
        j.join().expect("并发 reduce 不得 panic");
    }
    assert_eq!(
        frag.route_misses(),
        1 + THREADS as u64 * PER_THREAD,
        "并发 miss 必须精确留痕（丢失/重复计即红）"
    );

    let hit = frag.reduce(&s0, ParentAct::Child(5), &mut out);
    assert_eq!(hit.child, 5, "命中路径语义不变");
    assert_eq!(
        frag.route_misses(),
        1 + THREADS as u64 * PER_THREAD,
        "命中不计数"
    );
}

// ── ② audit_preempted：并发二次订阅精确计数 ─────────────────────────────────

/// 首订占槽后 THREADS 线程并发再订：槽是 Mutex 串行检查-置放，每订必见
/// Some ⇒ 恰 +1/订。票据经 channel 回收保活，防「票先 drop 清槽」让后续
/// 订阅落进 None 窗。
#[test]
fn audit_preempted_concurrent_subscribes_are_exact() {
    let store = Arc::new(Store::new(Counter, NoFx));
    assert_eq!(store.audit_preempted(), 0, "基线");
    let first = store.subscribe_events(16, |_| {});
    assert_eq!(store.audit_preempted(), 0, "首订无覆盖");

    let (tx, rx) = mpsc::channel();
    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            let tx = tx.clone();
            std::thread::spawn(move || {
                let ticket = store.subscribe_events(16, |_| {});
                tx.send(ticket).expect("票据回收");
            })
        })
        .collect();
    drop(tx);
    let mut tickets = vec![first];
    while let Ok(ticket) = rx.recv() {
        tickets.push(ticket);
    }
    for j in joins {
        j.join().expect("并发订阅不得 panic");
    }
    assert_eq!(
        store.audit_preempted(),
        THREADS as u64,
        "并发二次订阅每订必覆盖一次（Mutex 串行检查-置放，不丢不重）"
    );

    store.dispatch(1);
    store.dispatch(2);
    assert_eq!(
        store.audit_preempted(),
        THREADS as u64,
        "后续 dispatch 不误计"
    );
}

// ── ③ cancel_unknown_id：并发悬空 Cancel 精确计数 ───────────────────────────

#[test]
fn cancel_unknown_id_concurrent_dangling_cancels_are_exact() {
    let store = Arc::new(Store::new(RunCancel, NoFxC));
    assert_eq!(store.cancel_unknown_id(), 0, "基线");
    store.dispatch(CMsg::Run1);
    assert_eq!(store.cancel_unknown_id(), 0, "Run 登记不计数");

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    store.dispatch(CMsg::Cancel9);
                }
            })
        })
        .collect();
    for j in joins {
        j.join().expect("并发 dispatch 不得 panic");
    }
    assert_eq!(
        store.cancel_unknown_id(),
        THREADS as u64 * PER_THREAD,
        "每条悬空 Cancel 恰计一次（转发环逐条留痕）"
    );

    store.dispatch(CMsg::Cancel1);
    assert_eq!(
        store.cancel_unknown_id(),
        THREADS as u64 * PER_THREAD,
        "已登记 id 的 Cancel 不误计"
    );
}

// ── ④ local_missed_notifications：并发他线程 dispatch 精确计数 ──────────────

/// 本地订阅挂在注册线程（thread-local 表），THREADS 个他线程并发 dispatch：
/// 每条 action 恰一拍、闸>0 且 dispatch 线程表查无登记 ⇒ 恰 +1/拍。注册线
/// 程静止期自 dispatch 命中（matched=1）不涨；票据注销后闸=0 短路不涨。
#[test]
fn local_missed_notifications_concurrent_foreign_dispatches_are_exact() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let hits = Arc::new(AtomicU64::new(0));
    assert_eq!(store.local_missed_notifications(), 0, "基线");

    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (report_tx, report_rx) = mpsc::channel::<(u64, u64)>();
    let registrar = {
        let store = Arc::clone(&store);
        let hits = Arc::clone(&hits);
        let hits_cb = Arc::clone(&hits);
        std::thread::spawn(move || {
            let ticket = store.subscribe_local(move |_| {
                hits_cb.fetch_add(1, Ordering::SeqCst);
            });
            ready_tx.send(()).expect("ready 信号");
            release_rx.recv().expect("放行信号");
            store.dispatch(7); // 静止期注册线程 dispatch：本线程表命中
            let hits_now = hits.load(Ordering::SeqCst);
            let missed_now = store.local_missed_notifications();
            report_tx.send((hits_now, missed_now)).expect("回读上报");
            drop(ticket);
        })
    };
    ready_rx.recv().expect("本地订阅已登记");
    assert_eq!(store.local_missed_notifications(), 0, "注册本身不计数");

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    store.dispatch(1);
                }
            })
        })
        .collect();
    for j in joins {
        j.join().expect("并发 dispatch 不得 panic");
    }
    assert_eq!(
        store.local_missed_notifications(),
        THREADS as u64 * PER_THREAD,
        "他线程每拍 dispatch 恰计一次 miss"
    );

    release_tx.send(()).expect("放行注册线程");
    let (hits_now, missed_now) = report_rx.recv().expect("注册线程回读");
    registrar.join().expect("注册线程不得 panic");
    assert_eq!(hits_now, 1, "注册线程 dispatch 恰触发本地订阅一次");
    assert_eq!(
        missed_now,
        THREADS as u64 * PER_THREAD,
        "命中线程的 dispatch 不误计 miss"
    );

    store.dispatch(8); // 票据已注销：闸=0 短路
    assert_eq!(
        store.local_missed_notifications(),
        THREADS as u64 * PER_THREAD,
        "零本地订阅后 dispatch 不再计 miss"
    );
}

// ── ⑤ notify_published：并发拍投递精确计数 ──────────────────────────────────

#[derive(Clone)]
struct CountingSink(Arc<AtomicU64>);

impl NotifySink for CountingSink {
    fn publish(&self, _version: u64) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

/// 单投递口在册，THREADS 线程并发 dispatch：每条 action 恰一拍、每拍恰一
/// 投递 ⇒ 读数 = 拍数，精确等值；票据注销后门控短路零投递。
#[test]
fn notify_published_concurrent_beats_are_exact() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let published = Arc::new(AtomicU64::new(0));
    assert_eq!(store.notify_published(), 0, "基线");
    let ticket = store.subscribe_notify(CountingSink(Arc::clone(&published)));
    store.dispatch(1);
    assert_eq!(store.notify_published(), 1, "单拍单投递");

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    store.dispatch(1);
                }
            })
        })
        .collect();
    for j in joins {
        j.join().expect("并发 dispatch 不得 panic");
    }
    assert_eq!(
        store.notify_published(),
        1 + THREADS as u64 * PER_THREAD,
        "每拍恰一投递，累计精确"
    );
    assert_eq!(
        published.load(Ordering::Relaxed),
        1 + THREADS as u64 * PER_THREAD,
        "读侧与投递口两侧计数一致"
    );

    drop(ticket);
    store.dispatch(2);
    assert_eq!(
        store.notify_published(),
        1 + THREADS as u64 * PER_THREAD,
        "无人订阅的门控拍零投递"
    );
}

// ── ⑥ dropped(channel)：并发写失败精确自计数 ────────────────────────────────

/// 契约测试替身（同 logx_channels::CaptureSink 形态）：fail=true 时写恒失败
/// 并自计数（失败记账在 sink 内——registry 无从知晓单次写成败的契约面）。
struct FailingSink {
    channel: Channel,
    fail: bool,
    dropped: AtomicU64,
}

impl FailingSink {
    fn new(channel: Channel, fail: bool) -> Self {
        Self {
            channel,
            fail,
            dropped: AtomicU64::new(0),
        }
    }
}

impl ChannelSink for FailingSink {
    fn channel(&self) -> Channel {
        self.channel
    }
    fn level(&self) -> log::LevelFilter {
        log::LevelFilter::Trace
    }
    fn write(&self, _level: log::Level, _target: &str, _message: &str) {
        if self.fail {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn dropped_lines(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

/// THREADS 线程并发向失败通道各写 PER_THREAD 条：读数 = THREADS×PER_THREAD
/// 精确；同接口的正常通道并发写零计数（未触发时读数不变的反向面）。
#[test]
fn dropped_channel_concurrent_failed_writes_are_exact() {
    let registry = Arc::new(ChannelRegistry::new());
    let failing = Arc::new(FailingSink::new(Channel::Access, true));
    let healthy = Arc::new(FailingSink::new(Channel::General, false));
    registry.register(failing).expect("access 首注");
    registry.register(healthy).expect("general 首注");
    assert_eq!(registry.dropped(Channel::Access), Some(0), "基线");
    assert_eq!(registry.dropped(Channel::General), Some(0), "基线");

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let registry = Arc::clone(&registry);
            std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    registry.emit(Channel::Access, log::Level::Info, "conc::t", "lost");
                    registry.emit(Channel::General, log::Level::Info, "conc::t", "kept");
                }
            })
        })
        .collect();
    for j in joins {
        j.join().expect("并发 emit 不得 panic");
    }
    assert_eq!(
        registry.dropped(Channel::Access),
        Some(THREADS as u64 * PER_THREAD),
        "失败写每条恰计一次"
    );
    assert_eq!(
        registry.dropped(Channel::General),
        Some(0),
        "正常通道并发写零计数（反向面）"
    );
    assert_eq!(
        registry.dropped_total(),
        THREADS as u64 * PER_THREAD,
        "dropped_total 与逐通道读数一致"
    );
}

// ── ⑦ try_subscribe：close 边缘两阶段并发拒绝 ───────────────────────────────

/// open 态 THREADS 线程并发 try_subscribe 全 Ok（登记面可用）；close 后并发
/// try_subscribe 全 Err(StoreClosed)（拒绝面显式）——两阶段各自 join 后再进
/// 入下一阶段，不制造 check/close 竞态（契约：close **后**调用才 Err）。
#[test]
fn try_subscribe_concurrent_phases_around_close() {
    let store = Arc::new(Store::new(Counter, NoFx));

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || store.try_subscribe(|_| {}))
        })
        .collect();
    let mut open_tickets = Vec::new();
    for j in joins {
        open_tickets.push(
            j.join()
                .expect("并发登记不得 panic")
                .expect("open 态并发 try_subscribe 必全 Ok"),
        );
    }
    assert_eq!(open_tickets.len(), THREADS, "四票全登记");

    store.close();

    let joins: Vec<_> = (0..THREADS)
        .map(|_| {
            let store = Arc::clone(&store);
            std::thread::spawn(move || store.try_subscribe(|_| {}))
        })
        .collect();
    for j in joins {
        let outcome = j.join().expect("并发登记不得 panic");
        assert!(
            matches!(outcome, Err(StoreClosed)),
            "close 后并发 try_subscribe 必全显式 Err"
        );
    }
}
