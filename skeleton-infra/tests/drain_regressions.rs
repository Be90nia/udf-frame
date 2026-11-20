#![cfg(feature = "runtime")]
//! panic 变砖（旗守卫+订阅者摘除）与 send 显式失效的回归。
//! 死 Weak 回收与并发孤儿压力测试需私有态断言，在 src/runtime.rs 单测模块
//! （不变量监视线程 + 去 barrier 自由竞争）。

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::AuditEventKind;
use skeleton_infra::runtime::SyncStore as Store;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

/// 一次性信号（确定性握手，**禁 sleep 编排**）：`set()` 置位并唤醒全部等待者；
/// `wait_set(timeout)` 在**有界超时**内等待置位（超时 ⇒ 返回 false，由调用方判红）。
struct Latch {
    set: Mutex<bool>,
    cv: Condvar,
}

impl Latch {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            set: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    /// 置位（幂等）。
    fn set(&self) {
        let mut set = self.set.lock().expect("latch lock");
        *set = true;
        self.cv.notify_all();
    }

    /// 有界等待置位（超时 ⇒ false；**禁无限等待**——失败必须表现为红而非挂死）。
    fn wait_set(&self, timeout: Duration) -> bool {
        let mut set = self.set.lock().expect("latch lock");
        let deadline = Instant::now() + timeout;
        while !*set {
            let remain = deadline.saturating_duration_since(Instant::now());
            if remain.is_zero() {
                return false;
            }
            let (guard, _) = self.cv.wait_timeout(set, remain).expect("latch wait");
            set = guard;
        }
        true
    }
}

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
///
/// 注：本测断言的「回灌已入队**并排空**」是**静默态**形态（无在任排空者 ⇒
/// sender 夺得排空权并同步排空至 idle ⇒ 返回时该 action 已被 reduce）；框架
/// 提供的 sender 在**竞争态**下则是纯准入——**返回瞬间该 action 是否已被 reduce，
/// 框架不作任何保证**（取决于在任排空者的推进速度：可能尚未被 reduce，也可能已被
/// reduce 甚至已完成）⇒ 下游**不得**据「竞争态」推断「此刻读状态仍是旧值」。
/// 两条路径的条件化语义见 `skeleton::ActionSender::dispatch` 的契约与
/// `framework_sender_admission_is_conditional_on_drain_state`。
#[test]
fn send_reports_delivery_explicitly() {
    let store = Store::new(Counting, Nop::<Act> { _m: PhantomData });
    let sender = store.sender();
    assert!(sender.dispatch(Act::Inc));
    assert_eq!(
        *store.snapshot(),
        1,
        "静默态：回灌已入队并同步排空（已被 reduce）"
    );
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

// ── 条件性准入闸：框架提供的 sender 语义按「调用瞬间的排空状态」分两态 ──────
//
// 契约（skeleton::ActionSender::dispatch）：框架提供的 sender（Store::sender()）
// 的语义**按调用瞬间有无在任排空者分两条路径**——
//   静默态（无在任排空者）：夺得排空权并**同步排空至 idle** ⇒ 返回时该 action
//     **已被 reduce**、完成边界已达成；
//   竞争态（已有在任排空者）：**纯准入** ⇒ 立即返回。**在本闸的编排下**（排空环被
//   drain 回调同步等待挡住、同一线程不可能前进）该 action **尚未被 reduce**——这是
//   **本编排内的确定事实**，**不是**框架对竞争态的无条件保证（框架只保证「不等该
//   action 的完成边界」；返回瞬间是否已被 reduce 不作保证，见
//   `skeleton::ActionSender::dispatch`）。
// 本闸同时钉住两态（任一态退化 ⇒ 立即报红），是契约表述的机械闸（rustdoc 只查
// 链接与编译，不查语义真伪）。
//
// 确定性编排（禁 sleep）：竞争态窗口用「drain 回调内派生 worker」——回调运行在
// 排空环内 ⇒ 排空权在排空线程 ⇒ worker 的 sender 必处竞争态；回调**同步等待**
// worker 的完成信号后才返回 ⇒ worker 观察 reducer 计数的瞬间排空环不可能前进
// （同一线程）⇒ 「尚未 reduce」是确定判据而非竞速读数。
// 失败不挂死：worker 完成信号经 channel + `recv_timeout`（10s）。

/// 条件性准入闸的动作词表：`Start` 产一条效果（打开竞争态窗口）；`Follow` 由
/// worker 经框架提供的 sender 提交；`Bump` 收尾（排在 `Follow` 之后）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CondAct {
    Start,
    Follow,
    Bump,
}

/// 条件性准入闸的效果载荷（解释器只用它触发派生 worker）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CondOp {
    Work,
}

/// 计数 reducer：每个 action 计数一次（计数即「是否已被 reduce」的读侧）。
struct CondCounter {
    processed: Arc<AtomicUsize>,
}

impl Reducer for CondCounter {
    type State = usize;
    type Action = CondAct;
    type Op = CondOp;

    fn initial(&self) -> usize {
        0
    }

    fn reduce(&self, s: &usize, a: CondAct, out: &mut EffectBuf<CondAct, CondOp>) -> usize {
        self.processed.fetch_add(1, Ordering::SeqCst);
        if let CondAct::Start = a {
            out.push(Effect::Run {
                id: EffectId::new(1),
                op: CondOp::Work,
            });
        }
        s + 1
    }
}

/// drain 回调内派生 worker：worker 用**框架提供的** sender 提交 `Follow`，并在
/// dispatch 返回的瞬间记录 reducer 计数。
///
/// ⚠️ 该计数读数是**本编排下**的事实（drain 回调在此同步等待 worker 完成 ⇒ 排空环
/// 在该窗口内不可能推进 ⇒ `Follow` 此刻未被 reduce），**不是**框架对「竞争态 ⇒
/// 未被 reduce」的无条件保证——框架对**返回瞬间是否已被 reduce 不作任何保证**
/// （契约见 `skeleton::ActionSender::dispatch`；反向闸见同文件
/// `competitive_state_does_not_guarantee_reduction_timing`）。
struct CondSpawner {
    processed: Arc<AtomicUsize>,
    observed: Arc<AtomicUsize>,
}

impl EffectRuntime<CondAct, CondOp> for CondSpawner {
    fn run(&mut self, _effect: Effect<CondAct, CondOp>, back: &ActionSender<CondAct>) {
        let sender = back.clone();
        let processed = Arc::clone(&self.processed);
        let observed = Arc::clone(&self.observed);
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let ok = sender.dispatch(CondAct::Follow);
            // dispatch 返回瞬间的 reducer 计数——**本编排内**（排空环被 drain 回调的
            // 同步等待挡住、同一线程不可能前进）`Follow` 尚未被 reduce；框架对竞争态
            // **不**保证这一点。
            observed.store(processed.load(Ordering::SeqCst), Ordering::SeqCst);
            let _ = tx.send(ok);
        });
        // 带超时的完成握手（禁裸 join：裸 join 会把死锁形态变成挂死的测试进程）。
        let ok = rx.recv_timeout(Duration::from_secs(10)).expect(
            "worker 未在 10s 内完成——竞争态下框架提供的 sender 必须纯准入（立即返回）；\
             若它改为等完成边界即死锁形态",
        );
        assert!(ok, "worker 的动作必须被准入（框架提供的 sender 返回 true）");
        worker.join().expect("worker 线程");
    }
}

/// 条件性准入闸：静默态 sender 返回时该 action **已被 reduce**；竞争态 sender
/// 返回时该 action **尚未被 reduce**——⚠️ 后者是**本闸编排下**的读数（drain 回调
/// 同步等待 worker 完成 ⇒ 排空环在该窗口内不可能推进），**不是**框架的无条件
/// 保证；框架对**返回瞬间是否已被 reduce 不作任何保证**（反向闸见同文件
/// `competitive_state_does_not_guarantee_reduction_timing`）。
#[test]
fn framework_sender_admission_is_conditional_on_drain_state() {
    // ── 静默态：无在任排空者 ⇒ sender 夺得排空权并同步排空 ⇒ 返回时已 reduce。
    let quiet_processed = Arc::new(AtomicUsize::new(0));
    let quiet_store = Store::new(
        CondCounter {
            processed: Arc::clone(&quiet_processed),
        },
        CondSpawner {
            processed: Arc::clone(&quiet_processed),
            observed: Arc::new(AtomicUsize::new(0)),
        },
    );
    let sender = quiet_store.sender();
    assert!(
        sender.dispatch(CondAct::Follow),
        "静默态：框架提供的 sender 必须接受 action（bool = 被接受）"
    );
    assert_eq!(
        quiet_processed.load(Ordering::SeqCst),
        1,
        "静默态：sender 返回时该 action 必须已被 reduce（完成边界已达成）"
    );

    // ── 竞争态：已有在任排空者（drain 回调内）⇒ 纯准入 ⇒ **本编排内**返回时尚未
    // reduce（编排限定：排空环被同步等待挡住、同一线程不可能前进；框架对竞争态
    // **不**保证「尚未 reduce」——见 `skeleton::ActionSender::dispatch`）。
    let processed = Arc::new(AtomicUsize::new(0));
    let observed = Arc::new(AtomicUsize::new(0));
    let store = Store::new(
        CondCounter {
            processed: Arc::clone(&processed),
        },
        CondSpawner {
            processed: Arc::clone(&processed),
            observed: Arc::clone(&observed),
        },
    );
    store.dispatch(CondAct::Start); // 排空环转发效果 ⇒ 回调内派生 worker（竞争态窗口）
    assert_eq!(
        observed.load(Ordering::SeqCst),
        1,
        "竞争态（本编排：drain 回调同步等待 worker 完成 ⇒ 排空环无法推进）：\
         sender 返回瞬间该 action 尚未被 reduce（reducer 计数仍只有 Start）——\
         注意这是**本编排**的读数，不是框架的无条件保证"
    );
    store.dispatch(CondAct::Bump); // 排在 Follow 之后 ⇒ 其完成边界覆盖 Follow 的处理
    assert_eq!(
        processed.load(Ordering::SeqCst),
        3,
        "Start、Follow、Bump 三个动作各被处理恰好一次"
    );
    assert_eq!(store.pending_actions(), 0, "队列不得残留");
}

// ── 反向机械闸：竞争态「不保证尚未 reduce」 ──────────────────────────────
//
// 契约（skeleton::ActionSender::dispatch，2026-11-20 第三轮对抗审计更正）：
// 竞争态（调用瞬间已有在任排空者）= **纯准入**——框架**只保证**该 action 已入队、
// **不等**它的完成边界；**返回瞬间该 action 是否已被 reduce，框架不作任何保证**
// （取决于在任排空者的推进速度：可能尚未被 reduce，也可能已被 reduce 甚至已完成）
// ⇒ 下游**不得**据「竞争态」推断「此刻读状态仍是旧值」。
//
// 本闸**刻意不断言**「返回瞬间该 action 是否已被 reduce」：那是**竞态窗口内的读数**
// （第三轮对抗审计的外部 crate 视角编排式探针实测：真竞争态 8/8 轮次中该 action 在
// dispatch **返回之前**就已被他人 reduce ⇒「竞争态 ⇒ 尚未 reduce」**不是**框架保证）。
// 把该读数写进断言 = 把竞态写成契约 ⇒ 那正是本闸要避免的。
//
// 本闸断言**只**针对框架**无条件保证**的事实：
//   ① 该 action 最终被 reduce 且**恰好一次**（不丢不重）；
//   ② 竞争态下 sender 是**纯准入**：dispatch 在有界超时内返回（不等完成边界）——
//      退化为「同步等待完成边界」即与在任排空者互等（死锁形态）⇒ 超时判红。
//
// 确定性编排（禁 sleep）：排空线程在 reducer 内持闸（`Hold`）⇒ 排空权在他手里 ⇒
// 发送方 enqueue 必见 `draining == true` ⇒ 必走 `Enqueue::Queued` 分支（真竞争态）；
// 闸在**发送方返回之后**才开 ⇒ 发送方若等完成边界即与排空者互等（死锁形态）。
// 有界超时：发送方完成信号走 channel + `recv_timeout`（10s）——失败不挂死。

/// 反向闸的动作词表：`Hold` 在 reducer 内持闸（占住排空权）；`Probe` 是被观测的
/// action（reduce 时计数）；`Fence` 是收尾屏障（其完成边界 ⇒ 排在其前的 `Probe`
/// 必已被 reduce ⇒ 观测**无竞态**）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RevAct {
    Hold,
    Probe,
    Fence,
}

/// 反向闸的 reducer：`Hold` 阻塞在闸上（排空权在他手里 ⇒ 发送方必走竞争态分支）；
/// `Probe` 只计数（reduce 计数 = 「已被 reduce」的读侧）；`Fence` 空转。
struct RevReducer {
    /// 「排空线程已进入 reducer」信号（证明排空权已在他手里）。
    entered: Arc<Latch>,
    /// 放行闸（**在发送方返回之后**才开 ⇒ 竞争态窗口成立）。
    release: Arc<Latch>,
    /// `Probe` 被 reduce 的次数（终局读侧）。
    probed: Arc<AtomicUsize>,
}

impl Reducer for RevReducer {
    type State = usize;
    type Action = RevAct;
    type Op = Infallible;

    fn initial(&self) -> usize {
        0
    }

    fn reduce(&self, s: &usize, a: RevAct, _out: &mut EffectBuf<RevAct, Infallible>) -> usize {
        match a {
            RevAct::Hold => {
                self.entered.set();
                // 有界等待（超时即返回——闸本就该由测试线程在发送方返回后才开）。
                let _ = self.release.wait_set(Duration::from_secs(30));
            }
            RevAct::Probe => {
                self.probed.fetch_add(1, Ordering::SeqCst);
            }
            RevAct::Fence => {}
        }
        s + 1
    }
}

/// 反向机械闸：竞争态下 sender 是**纯准入**（不等该 action 的完成边界），且该
/// action **最终被 reduce 且恰好一次**。刻意**不**断言「返回瞬间是否已被 reduce」。
#[test]
fn competitive_state_does_not_guarantee_reduction_timing() {
    let entered = Latch::new();
    let release = Latch::new();
    let probed = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(Store::new(
        RevReducer {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            probed: Arc::clone(&probed),
        },
        Nop::<RevAct> { _m: PhantomData },
    ));

    // 排空线程：dispatch(Hold) ⇒ 夺得排空权并停在 reducer 内（持闸）。
    let drainer = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.dispatch(RevAct::Hold))
    };
    assert!(
        entered.wait_set(Duration::from_secs(10)),
        "排空线程未在 10s 内进入 reducer（编排失效 ⇒ 竞争态窗口未成立）"
    );

    // 发送方：真竞争态（排空权在排空线程手里）⇒ 纯准入 ⇒ 有界超时内返回。
    let sender = store.sender();
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let admitted = sender.dispatch(RevAct::Probe);
        let _ = tx.send(admitted);
    });
    let admitted = match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(admitted) => admitted,
        Err(_) => {
            // 放闸让排空线程收尾（避免残留阻塞线程），再判红。
            release.set();
            panic!(
                "竞争态：sender 必须在 10s 内返回（纯准入、不等完成边界）——\
                 若它同步等待完成边界即与在任排空者互等（死锁形态）"
            );
        }
    };
    assert!(
        admitted,
        "竞争态：sender 必须接受 action（bool = 准入结果，不承载完成状态）"
    );
    // ⚠️ 此处**刻意不**读 `probed`：返回瞬间该 action 是否已被 reduce 属竞态窗口
    // 读数，框架不作保证（外部 crate 视角探针实测真竞争态 8/8 轮次返回前已被 reduce）。
    worker.join().expect("发送线程");

    // 放闸 ⇒ 排空线程续排 ⇒ `Probe` 被 reduce；`Fence` 排在其后 ⇒ `Fence` 的完成
    // 边界达成时 `Probe` 必已被 reduce（观测无竞态）。
    release.set();
    store.dispatch(RevAct::Fence);
    drainer.join().expect("排空线程");
    assert_eq!(
        probed.load(Ordering::SeqCst),
        1,
        "该 action 最终必须被 reduce 且恰好一次（不丢不重）"
    );
}
