#![cfg(feature = "runtime")]
//! panic 变砖（旗守卫+订阅者摘除）与 send 显式失效的回归。
//! 死 Weak 回收与并发孤儿压力测试需私有态断言，在 src/runtime.rs 单测模块
//! （不变量监视线程 + 去 barrier 自由竞争）。

use skeleton::{ActionSender, Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::AuditEventKind;
use skeleton_infra::runtime::SyncStore as Store;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

enum Act {
    Inc,
}

struct Counting;

impl Reducer for Counting {
    type State = u32;
    type Action = Act;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: Act, _out: &mut EffectBuf<Act, Infallible>) -> u32 {
        match a {
            Act::Inc => s + 1,
        }
    }
}

enum Boom {
    Fine,
    Detonate,
}

struct BoomFeature;

impl Reducer for BoomFeature {
    type State = u32;
    type Action = Boom;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: Boom, _out: &mut EffectBuf<Boom, Infallible>) -> u32 {
        match a {
            Boom::Fine => s + 1,
            Boom::Detonate => panic!("reducer detonation (regression test)"),
        }
    }
}

struct Nop<A> {
    _m: PhantomData<fn() -> A>,
}

impl<A: 'static> EffectRuntime<A, Infallible> for Nop<A> {
    fn run(&mut self, _: Effect<A, Infallible>, _: &ActionSender<A>) {}
}

/// drain 内 panic 不再静默变砖——修复前 `draining` 旗无守卫，栈展开跳过复位后
/// 后续 dispatch 只入队不执行且零报错（Fine 静默滞留，终态 0）；修复后旗由 Drop
/// 守卫复位。reduce 期间不持任何锁（state 写在 reduce 之后），panic 后状态一致、
/// store 存活、后续 dispatch 正常排空。
#[test]
fn drain_panic_resets_flag_not_brick() {
    let store = Store::new(BoomFeature, Nop::<Boom> { _m: PhantomData });
    let r1 = catch_unwind(AssertUnwindSafe(|| store.dispatch(Boom::Detonate)));
    assert!(r1.is_err(), "reducer panic 必须外溢（fail-fast 不吞）");
    let r2 = catch_unwind(AssertUnwindSafe(|| store.dispatch(Boom::Fine)));
    assert!(r2.is_ok(), "旗已守卫复位：后续 dispatch 正常排空");
    // 修复前此处为 0：旗卡死 → Fine 只入队静默滞留
    assert_eq!(*store.snapshot(), 1);
}

/// 订阅回调 panic 归一为「摘除该订阅者+继续」——中毒 notify 锁
/// 永久再 panic，留表即每次 dispatch 必 panic 的双重变砖；摘除后 store 存活、
/// 后继订阅者照常。
#[test]
fn subscriber_panic_removed_and_store_survives() {
    let store = Store::new(Counting, Nop::<Act> { _m: PhantomData });
    // 两张票据均须持活：丢弃即注销，收不到通知则测试失真
    let _bad = store.subscribe(|_| panic!("subscriber detonation (regression test)"));
    let hits = Arc::new(AtomicUsize::new(0));
    let h2 = Arc::clone(&hits);
    let _good = store.subscribe(move |_| {
        h2.fetch_add(1, Ordering::SeqCst);
    });
    assert_eq!(store.subscriber_count(), 2, "订阅计数读侧可见");
    let r1 = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Inc)));
    assert!(
        r1.is_ok(),
        "订阅回调 panic 必须被归一（摘除+继续），不外溢 dispatch"
    );
    assert_eq!(hits.load(Ordering::SeqCst), 1, "后继订阅者照常收到通知");
    assert_eq!(
        store.subscriber_count(),
        1,
        "panic 摘除经计数可观测——摘除不再纯静默（r2 minor-2）"
    );
    let r2 = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Inc)));
    assert!(r2.is_ok(), "中毒 slot 已摘除：后续通知不再撞毒锁");
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    assert_eq!(
        *store.snapshot(),
        2,
        "store 存活：reduce 链未受订阅 panic 影响"
    );
}

/// send 显式化——store 存活返回 true；store 丢弃后返回 false（停机窗显式
/// 拒绝，禁静默丢弃）。
#[test]
fn send_reports_delivery_explicitly() {
    let store = Store::new(Counting, Nop::<Act> { _m: PhantomData });
    let sender = store.sender();
    assert!(sender.dispatch(Act::Inc));
    assert_eq!(*store.snapshot(), 1, "回灌已入队并排空");
    drop(store);
    assert!(
        !sender.dispatch(Act::Inc),
        "store 丢弃后 dispatch 必须显式返回 false"
    );
}

/// close 对并发 push 的
/// TOCTOU——旧代码 dispatch 过闸后在 queue 锁等待窗被 close 抢跑（store+drain+
/// 返回全完成），迟到 push 在闭门后自排空执行：停机闸「闭门后无新入队」前提被
/// 静默绕过。修复=push 路径持 queue 锁后复检 closed（与 close 的 drain 经 queue
/// 锁线性化合拢）。本测以 4 线程风暴持续踩踏该竞窗，断言两条可观测契约：
/// ①守恒——每个已发 action 恰落一侧（已执行计入终态 / 显式拒绝计入计数），
/// 零丢失零双计；②闭门终局——全量 join（drain 同步内嵌于 dispatch，join 即排空
/// 静止）后再 dispatch 恒拒且快照冻结。注：竞窗的时序表象（迟到 push 在 close
/// 返回后执行）不可黑盒断言——close 交接语义下，在途 drainer 排完闭门前已
/// 入队 action 属合法推迟，与违规形态同貌；守恒+终局是该修复的不变式锚点。
#[test]
fn close_vs_dispatch_race_conserves_and_freezes() {
    /// 快照静止判定：连续 unchanged_required 次读数不变即视为排空静止
    /// （在途他线程 drain 经 close 交接仍可能未完，数值有限必收敛）。
    fn settle(store: &Store<u32, Act, Infallible>) -> u32 {
        const UNCHANGED_REQUIRED: usize = 2_000;
        let mut last = *store.snapshot();
        let mut unchanged = 0;
        while unchanged < UNCHANGED_REQUIRED {
            let now = *store.snapshot();
            if now == last {
                unchanged += 1;
            } else {
                unchanged = 0;
                last = now;
            }
        }
        last
    }

    for _ in 0..96 {
        let store = Arc::new(Store::new(Counting, Nop::<Act> { _m: PhantomData }));
        let stop = Arc::new(AtomicBool::new(false));
        let mut joins = Vec::new();
        for _ in 0..4 {
            let (store, stop) = (Arc::clone(&store), Arc::clone(&stop));
            joins.push(std::thread::spawn(move || {
                let mut sent = 0u64;
                while !stop.load(Ordering::SeqCst) {
                    store.dispatch(Act::Inc);
                    sent += 1;
                }
                sent
            }));
        }
        // flaky 治理：定长 250µs
        // 猜窗决定风暴时长——慢 CI 上可能返回于任何线程开跑前（竞窗空转、覆盖
        // 静默降级）。事件化握手：等首个 dispatch 真实落地（快照推进）再 close，
        // 1s 有界上限防挂死；不变式判定（守恒+冻结）与窗口长短无关。
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
        while *store.snapshot() == 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        store.close();
        stop.store(true, Ordering::SeqCst);
        let mut total = 0u64;
        for j in joins {
            total += j.join().unwrap();
        }
        // drain 同步内嵌于 dispatch：全量 join 后队列必空（交接协议），
        // 快照即终态。
        let final_state = settle(&store);
        let rejected = store.rejected_after_close();
        assert_eq!(
            total,
            u64::from(final_state) + rejected,
            "守恒：每 action 恰落 执行|显式拒绝 一侧，零丢失零双计"
        );
        // 闭门终局：静止后再 dispatch 恒拒（计数推进）且快照冻结。
        for _ in 0..3 {
            store.dispatch(Act::Inc);
        }
        assert_eq!(
            store.rejected_after_close(),
            rejected + 3,
            "闭门后 dispatch 恒显式拒绝"
        );
        assert_eq!(*store.snapshot(), final_state, "闭门后快照冻结（零新执行）");
    }
}

// ── reducer panic 计数留痕（可观测对称：四条 panic/异常路径全有读侧）──────

/// reducer panic 与其余三路（解释器 panic / 订阅回调 panic / 回灌超限）同一
/// fail-visible 留痕纪律：catch_unwind 只观察（计数+审计现场）后 resume_unwind
/// 原样重抛——传播语义不变，本测钉的是读侧面。动作本体已出队随栈展开丢失
/// （已知语义，见 Store doc「Panic 契约」），状态不前进、实例可复用。
#[test]
fn reducer_panic_is_counted_and_audited() {
    let store = Store::new(BoomFeature, Nop::<Boom> { _m: PhantomData });
    let _ticket = store.subscribe_events(64, |_| {});
    let r1 = catch_unwind(AssertUnwindSafe(|| store.dispatch(Boom::Detonate)));
    assert!(r1.is_err(), "panic 传播语义不变（fail-fast 不吞）");
    assert_eq!(
        store.reducer_panic_count(),
        1,
        "reducer panic 必须计数留痕（四路唯一无读侧者收口）"
    );
    assert_eq!(store.runtime_panic_count(), 0, "解释器计数不得串线");
    assert_eq!(store.subscriber_panics(), 0, "订阅回调计数不得串线");
    // 审计现场：ReducerInterrupted 在时间线上，且 panic 拍不记 Reduced
    //（reduce 未完成——时间线如实）。
    let kinds: Vec<_> = store.take_events().iter().map(|e| e.kind).collect();
    assert!(
        kinds.contains(&AuditEventKind::ReducerInterrupted),
        "审计流必须有病理现场，实际: {kinds:?}"
    );
    assert!(
        !kinds.contains(&AuditEventKind::Reduced),
        "panic 拍不得记 Reduced（状态迁移未完成），实际: {kinds:?}"
    );
    // 实例照常复用，计数不重复上浮。
    store.dispatch(Boom::Fine);
    assert_eq!(*store.snapshot(), 1);
    assert_eq!(
        store.reducer_panic_count(),
        1,
        "正常 dispatch 不得推进 panic 计数"
    );
}
