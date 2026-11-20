#![cfg(feature = "runtime")]
//! per-action 序列回执（GitHub issue#40）：外部并发 dispatch 必须等自己的
//! action 到达完成边界才返回；同栈回灌仅入队不等待（禁自死锁）；FIFO 保持；
//! panic/close 让每个等待者恰好结算一次（无人悬挂）。

use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, NoFx, Reducer, Store as _,
};
use skeleton_infra::runtime::SyncStore;
use std::convert::Infallible;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak, mpsc};
use std::thread;
use std::time::Duration;

/// 两段闸：reducer 进入点 + 放行点（确定性并发编排，禁 sleep 竞速）。
struct Gate {
    entered: (Mutex<bool>, Condvar),
    release: (Mutex<bool>, Condvar),
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: (Mutex::new(false), Condvar::new()),
            release: (Mutex::new(false), Condvar::new()),
        })
    }

    fn mark_entered(&self) {
        let mut g = self.entered.0.lock().unwrap();
        *g = true;
        self.entered.1.notify_all();
    }

    fn wait_entered(&self) {
        let mut g = self.entered.0.lock().unwrap();
        while !*g {
            g = self.entered.1.wait(g).unwrap();
        }
    }

    fn open(&self) {
        let mut g = self.release.0.lock().unwrap();
        *g = true;
        self.release.1.notify_all();
    }

    fn wait_release(&self) {
        let mut g = self.release.0.lock().unwrap();
        while !*g {
            g = self.release.1.wait(g).unwrap();
        }
    }
}

enum Act {
    /// 在 reducer 内阻塞（占住排空权），放行后计 +1。
    Block,
    /// 普通动作（计 +1）。
    Inc,
}

struct Gated {
    gate: Arc<Gate>,
}

impl Reducer for Gated {
    type State = u32;
    type Action = Act;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: Act, _out: &mut EffectBuf<Act, Infallible>) -> u32 {
        match a {
            Act::Block => {
                self.gate.mark_entered();
                self.gate.wait_release();
                s + 1
            }
            Act::Inc => s + 1,
        }
    }
}

/// issue#40 核心契约：并发外部 dispatch 必须等**自己的** action 到达完成边界。
///
/// 修复前：B 的 dispatch 在 A 持排空权时只入队即返（`Enqueue::Queued` 分支），
/// B 返回后 B 的 action 尚未被 reduce——B 读到的是旧状态。
#[test]
fn external_dispatch_waits_for_own_action_boundary() {
    let gate = Gate::new();
    let store = Arc::new(SyncStore::new(
        Gated {
            gate: Arc::clone(&gate),
        },
        NoFx,
    ));

    // A：夺排空权并在 reducer 内阻塞。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(Act::Block));
    gate.wait_entered();

    // B：并发外部 dispatch。
    let (tx, rx) = mpsc::channel();
    let store_b = Arc::clone(&store);
    let handle_b = thread::spawn(move || {
        store_b.dispatch(Act::Inc);
        tx.send(()).unwrap();
    });

    // B 不得在自己的 action 到达边界前返回（A 仍卡在 reducer 里）。
    assert!(
        rx.recv_timeout(Duration::from_millis(200)).is_err(),
        "外部 dispatch 必须等自己的 action 到达完成边界才返回（不得只入队即返）"
    );

    gate.open();
    handle_a.join().unwrap();
    handle_b.join().unwrap();

    // B 返回后其 action 必已 reduce（状态含 A、B 两次 +1）。
    assert_eq!(*store.snapshot(), 2, "B 返回时 B 的 action 必须已被 reduce");
}

// ── 因果后代屏障（#40）：外部 dispatch 必须等自己的因果后代终局 ────────────

/// 因果后代场景的 action：
/// - `Hold`：占住排空权（reducer 内等闸），放行后继续排空；
/// - `Root`：外部根，reduce 时产出一条 `Emit` 后代；
/// - `Child`：因果后代，reduce 时 panic（后代失败 ⇒ 根回执必须结算为失败）。
enum DescendantAct {
    Hold,
    Root,
    Child,
}

struct Descendant {
    gate: Arc<Gate>,
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl Reducer for Descendant {
    type State = u32;
    type Action = DescendantAct;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(
        &self,
        s: &u32,
        a: DescendantAct,
        out: &mut EffectBuf<DescendantAct, Infallible>,
    ) -> u32 {
        match a {
            DescendantAct::Hold => {
                self.gate.mark_entered();
                self.gate.wait_release();
                *s
            }
            DescendantAct::Root => {
                // 派生因果后代：它共享本根的账本 ⇒ 根的完成边界覆盖它。
                out.push(Effect::Emit(DescendantAct::Child));
                *s
            }
            DescendantAct::Child => {
                self.events.lock().unwrap().push("child");
                panic!("descendant reducer detonation (barrier test)");
            }
        }
    }
}

/// issue#40 因果后代屏障：**外部 dispatch 必须等自己的因果后代终局才返回**。
///
/// 构造（跨线程，禁同栈平凡满足）：
/// - A 线程 `dispatch(Hold)`：夺排空权后在 reducer 内等闸；
/// - B 线程 `dispatch(Root)`（外部根）：A 持排空权 ⇒ B 只入队 ⇒ B 在账本上等；
/// - `Root` 的 reducer 产 `Emit(Child)` ⇒ `Child` 是 `Root` 的因果后代
///   （共享 `Root` 的账本、后代计数 +1）；
/// - `Child` 的 reducer panic ⇒ 后代失败。
///
/// 判据（确定性，禁 sleep 竞速）：后代失败必须结算进**根的**回执 ⇒ B 的 dispatch
/// 以「dispatch 失败」panic 上浮。若根账本在后代终局前就发布终局（终局谓词
/// `root_done && outstanding == 0` 被削弱），B 会拿到「成功」回执并正常返回 ⇒ 判红。
#[test]
fn external_dispatch_waits_for_causal_descendant_failure() {
    let gate = Gate::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(SyncStore::new(
        Descendant {
            gate: Arc::clone(&gate),
            events: Arc::clone(&events),
        },
        NoFx,
    ));

    // A：夺排空权并在 reducer 内阻塞（B 的入队因此只入队、不排空）。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(DescendantAct::Hold));
    gate.wait_entered();

    // B：外部 dispatch（外部根）⇒ 产出一条因果后代。B 的 dispatch 调用结束
    // （正常返回或异常展开）后立刻记一笔 —— 与后代 reducer 的「终局」记一笔进
    // 同一日志 ⇒ 「返回序」可断言（屏障：外部 dispatch 的返回必须晚于后代终局）。
    let store_b = Arc::clone(&store);
    let events_b = Arc::clone(&events);
    let handle_b = thread::spawn(move || {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            store_b.dispatch(DescendantAct::Root);
        }));
        events_b.lock().unwrap().push("b-ended");
        if let Err(payload) = outcome {
            std::panic::resume_unwind(payload);
        }
    });

    // B 的记录确已入队（轮询可观测的队列深；禁 sleep 竞速）——否则 B 可能晚于
    // A 放行才入队并自己夺排空权 ⇒ 屏障被平凡满足（本用例空转）。
    wait_pending(&store, 1);

    // 放行 A：A 继续排空 ⇒ 处理 `Root`（派生后代）⇒ 处理 `Child`（后代 reduce panic）。
    gate.open();

    // B 的回执必须结算为失败：后代失败上浮到根回执（因果后代屏障的可观测面）。
    let panic_b = handle_b
        .join()
        .expect_err("外部 dispatch 必须等因果后代终局，并把后代失败结算进自己的回执（屏障被绕过）");
    let text_b = panic_text(panic_b);
    assert!(
        text_b.contains("dispatch 失败"),
        "B 必须收到显式失败结算（后代失败上浮）：{text_b}"
    );

    // A 的排空被后代 reducer 的 panic 打断（原样上浮，禁吞）。
    let panic_a = handle_a
        .join()
        .expect_err("后代 reducer 的 panic 必须照常上浮到排空者");
    assert!(
        panic_text(panic_a).contains("descendant reducer detonation"),
        "A 必须收到后代 reducer 的原始 panic 载荷"
    );

    // 后代确实被 reduce 过，且**先于** B 的 dispatch 结束抵达终局
    // （防空转：后代没跑过 ⇒ 本用例无鉴别力；顺序倒置 ⇒ 屏障被绕过）。
    assert_eq!(
        *events.lock().unwrap(),
        vec!["child", "b-ended"],
        "因果后代必须先于外部 dispatch 的结束抵达终局（屏障：返回 = 后代终局后）"
    );
}

// ── 同栈回灌（重入）：仅入队不等待 ──────────────────────────────────────

/// 重入 reducer：首次 reduce 时把自身重入 dispatch 回同一 store。
struct Reentrant;

type ReentrantSlot = LazyLock<Mutex<Option<Weak<SyncStore<u32, u32, Infallible>>>>>;
static REENTRANT_SLOT: ReentrantSlot = LazyLock::new(|| Mutex::new(None));
static REENTRANT_DEPTH: AtomicU32 = AtomicU32::new(0);

impl Reducer for Reentrant {
    type State = u32;
    type Action = u32;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, depth: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
        // 只重入一次（depth 0 → 1），避免自指环把测试拖成预算止步。
        if depth == 0 {
            REENTRANT_DEPTH.fetch_add(1, Ordering::SeqCst);
            let slot = REENTRANT_SLOT.lock().unwrap();
            if let Some(weak) = slot.as_ref() {
                if let Some(store) = weak.upgrade() {
                    // 同栈重入：必须只入队即返（等待 = 自死锁）。
                    store.dispatch(1);
                }
            }
        }
        s + 1
    }
}

/// 同栈回灌（reducer 内重入 dispatch 同一 store）：仅入队不等待——等待即自死锁。
#[test]
fn reentrant_dispatch_does_not_self_deadlock() {
    let store = Arc::new(SyncStore::new(Reentrant, NoFx));
    REENTRANT_SLOT
        .lock()
        .unwrap()
        .replace(Arc::downgrade(&store));

    store.dispatch(0);
    assert_eq!(
        REENTRANT_DEPTH.load(Ordering::SeqCst),
        1,
        "重入 dispatch 必须已发生"
    );
    assert_eq!(*store.snapshot(), 2, "重入 action 必须被外层排空环消费");
}
// ── FIFO ─────────────────────────────────────────────────────────────────

/// FIFO reducer：把被 reduce 的 action 记进共享日志（验证处理序）。
struct Fifo {
    order: Arc<Mutex<Vec<u32>>>,
}

impl Reducer for Fifo {
    type State = u32;
    type Action = u32;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
        self.order.lock().unwrap().push(a);
        s + 1
    }
}

/// 单生产者：action 的处理序 = 准入序（FIFO）。
#[test]
fn single_producer_actions_reduce_in_admission_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Fifo {
            order: Arc::clone(&order),
        },
        NoFx,
    );
    for i in 0..5u32 {
        store.dispatch(i);
    }
    assert_eq!(
        *order.lock().unwrap(),
        vec![0, 1, 2, 3, 4],
        "单生产者的 action 必须按准入序处理（FIFO）"
    );
}

/// 多生产者：每个生产者自己的 action 序必须保持（全局序由准入序决定）。
#[test]
fn concurrent_producers_keep_per_producer_order() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(SyncStore::new(
        Fifo {
            order: Arc::clone(&order),
        },
        NoFx,
    ));
    let mut handles = Vec::new();
    for producer in 0..4u32 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..5u32 {
                store.dispatch(producer * 100 + i);
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let log = order.lock().unwrap().clone();
    assert_eq!(log.len(), 20, "全部 action 必须被 reduce（无丢失）");
    for producer in 0..4u32 {
        let mine: Vec<u32> = log
            .iter()
            .copied()
            .filter(|id| id / 100 == producer)
            .collect();
        assert_eq!(
            mine,
            (0..5).map(|i| producer * 100 + i).collect::<Vec<u32>>(),
            "每生产者的 action 序必须保持（FIFO）"
        );
    }
}

// ── panic / close 结算（#40 终局 + #41 中断批次） ───────────────────────

enum PanicAct {
    /// 占住排空权（等闸），放行后 panic。
    Boom,
    /// 普通动作。
    Inc,
}

struct PanicGated {
    gate: Arc<Gate>,
    reduced: Arc<Mutex<Vec<&'static str>>>,
}

impl Reducer for PanicGated {
    type State = u32;
    type Action = PanicAct;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: PanicAct, _out: &mut EffectBuf<PanicAct, Infallible>) -> u32 {
        match a {
            PanicAct::Boom => {
                self.gate.mark_entered();
                self.gate.wait_release();
                self.reduced.lock().unwrap().push("boom");
                panic!("reducer detonation (receipt settlement test)");
            }
            PanicAct::Inc => {
                self.reduced.lock().unwrap().push("inc");
                s + 1
            }
        }
    }
}

/// panic 载荷取文本（断言失败消息用）。
fn panic_text(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else {
        "<非字符串 panic 载荷>".to_string()
    }
}

/// 有界等待队列深度达 `want`（编排用；超时即失败，禁无限等）。
fn wait_pending<S, A, Op>(store: &SyncStore<S, A, Op>, want: usize)
where
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
{
    for _ in 0..5000 {
        if store.pending_actions() >= want {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("队列深度未达期望值 {want}");
}

/// 排空者 panic：并发排队的生产者**不被搁浅**——其回执恰好结算一次（失败），
/// 队列不残留，panic 的 action 不重放，store 仍可用（close 能终止）。
#[test]
fn panic_settles_every_waiter_exactly_once() {
    let gate = Gate::new();
    let reduced = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(SyncStore::new(
        PanicGated {
            gate: Arc::clone(&gate),
            reduced: Arc::clone(&reduced),
        },
        NoFx,
    ));

    // A：夺排空权，等闸后 panic。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(PanicAct::Boom));
    gate.wait_entered();

    // B：并发外部 dispatch（其 action 排在 A 之后）。
    let store_b = Arc::clone(&store);
    let handle_b = thread::spawn(move || store_b.dispatch(PanicAct::Inc));
    wait_pending(&store, 1);

    gate.open();

    // A 的原始 panic 照常上浮（fail-visible 传播语义不变）。
    let panic_a = handle_a
        .join()
        .expect_err("排空者的原始 panic 必须照常上浮");
    assert!(
        panic_text(panic_a).contains("reducer detonation"),
        "必须上浮原始 panic 载荷（不得被回执结算替换）"
    );

    // B 的回执被结算为失败（显式失败，不静默成功、不悬挂）。
    let panic_b = handle_b
        .join()
        .expect_err("被中断批次的等待者必须收到失败结算");
    let text_b = panic_text(panic_b);
    assert!(
        text_b.contains("dispatch 失败"),
        "等待者必须收到显式失败结算：{text_b}"
    );

    // 无搁浅：队列空、无重复执行、状态未前进。
    assert_eq!(store.pending_actions(), 0, "被中断批次不得留在队列里无主");
    assert_eq!(
        *reduced.lock().unwrap(),
        vec!["boom"],
        "被中断的 action 不得重放（且 B 的 action 不得被执行）"
    );
    assert_eq!(*store.snapshot(), 0, "reducer panic ⇒ 状态不前进");

    // store 仍可用（排空权已复位），close 能终止。
    store.dispatch(PanicAct::Inc);
    assert_eq!(*store.snapshot(), 1);
    store.close();
    assert!(store.is_closed());
}

// ── 解释器 panic：提交后失败（状态可见） ───────────────────────────────

enum Mixed {
    /// 占住排空权（等闸）。
    Hold,
    /// 产出一条 Run 效果（解释器会 panic）。
    Run,
}

struct MixedReducer {
    gate: Arc<Gate>,
}

impl Reducer for MixedReducer {
    type State = u32;
    type Action = Mixed;
    type Op = u64;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: Mixed, out: &mut EffectBuf<Mixed, u64>) -> u32 {
        match a {
            Mixed::Hold => {
                self.gate.mark_entered();
                self.gate.wait_release();
                *s
            }
            Mixed::Run => {
                out.push(Effect::Run {
                    id: EffectId::new(1),
                    op: 7u64,
                });
                s + 1
            }
        }
    }
}

struct PanicRuntime;

impl EffectRuntime<Mixed, u64> for PanicRuntime {
    fn run(&mut self, _: Effect<Mixed, u64>, _: &ActionSender<Mixed>) {
        panic!("runtime detonation (post-commit failure test)");
    }
}

/// 解释器 panic 发生在状态提交**之后**：等待者收到的失败必须标注
/// `state_committed=true` 与效果转发阶段（不得伪报成功），状态快照可见。
#[test]
fn effect_runtime_panic_reports_post_commit_failure() {
    let gate = Gate::new();
    let store = Arc::new(SyncStore::new(
        MixedReducer {
            gate: Arc::clone(&gate),
        },
        PanicRuntime,
    ));

    // H：占住排空权。
    let store_h = Arc::clone(&store);
    let handle_h = thread::spawn(move || store_h.dispatch(Mixed::Hold));
    gate.wait_entered();

    // W：并发 dispatch（其 action 由 H 的排空者处理 → 解释器在 W 的 action 上 panic）。
    let store_w = Arc::clone(&store);
    let handle_w = thread::spawn(move || store_w.dispatch(Mixed::Run));
    wait_pending(&store, 1);

    gate.open();

    // 排空者的原始 panic 照常上浮。
    handle_h
        .join()
        .expect_err("解释器 panic 必须照常上浮出 dispatch");

    // 等待者收到「提交后失败」的显式结算。
    let panic_w = handle_w.join().expect_err("等待者必须收到失败结算");
    let text = panic_text(panic_w);
    assert!(
        text.contains("state_committed=true"),
        "提交后失败必须标注 state_committed=true：{text}"
    );
    assert!(
        text.contains("ForwardingEffects"),
        "失败阶段必须定位到效果转发：{text}"
    );

    // 状态已提交 ⇒ 快照可见（提交后失败 ≠ 未提交）。
    assert_eq!(*store.snapshot(), 1, "状态已提交必须可见");
    assert_eq!(store.pending_actions(), 0, "队列不得残留");
    store.close();
}

// ── 订阅回调 panic（被摘除归一）：不得搁浅回执 ─────────────────────────

struct Counting;

impl Reducer for Counting {
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

/// 订阅回调 panic 被摘除归一（非排空中断）：dispatch 正常返回，回执结算完成。
#[test]
fn caught_subscriber_panic_does_not_strand_receipt() {
    let store = SyncStore::new(Counting, NoFx);
    let _ticket = store.subscribe(|_| panic!("subscriber detonation (caught)"));
    store.dispatch(1);
    assert_eq!(*store.snapshot(), 1, "状态照常推进");
    assert_eq!(store.subscriber_count(), 0, "中毒订阅者必须被摘除");
    assert_eq!(store.subscriber_panics(), 1, "摘除必须留痕");
}

// ── 正向场景（GitHub issue#46）：drain 回调里 join 的 worker 用 ActionSender ─────
//
// 契约面：回调派生并 join 的 worker **必须**用**框架提供的** sender（本例 = 回调
// 收到的 `back`；回调运行在排空环内 ⇒ 该 sender 必处**竞争态** = 纯准入、不等
// 回执）的 `dispatch`——若改用阻塞式 `Store::dispatch`
// 回调同一 store，则 worker 等回执、排空环等 worker ⇒ 确定性互等死锁。（**自建**
// sender 的语义由实现方决定，不可假定框架提供的 sender 的任一条路径语义。）负向复现（外部超时 + 状态探针，
// 六场景矩阵）留在 `verify/2026-11-20-residual-join-risk/`，**仓内不复制**：观察
// 死锁要求测试**挂死**，而挂死的测试不是闸（会卡住 CI）；故仓内只保本正向场景 +
// 超时保护。
//
// 本测钉住**安全写法**的三件事：① worker 的动作被**接受**（`bool` = 被接受，非完成）；
// ② 回调**返回**（不死锁）；③ 排队的 action 被**恰好处理一次**。
//
// **超时保护**：worker 的完成信号经 channel 回传，回调用 `recv_timeout` 而非裸
// `join`——若误写成阻塞式 dispatch（死锁形态），worker 永不完成 ⇒ 本测**超时失败**
// 而不是挂死整个测试进程。

/// 正向场景的 action 词表：`Start` 产一条效果；`Follow` 由 worker 提交；`Bump` 收尾。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinAct {
    Start,
    Follow,
    Bump,
}

/// 效果载荷（解释器只用它触发派生）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinOp {
    Work,
}

/// 计数器 reducer：每个 action 计数一次（证明「恰好处理一次」）。
struct JoinCounter {
    processed: Arc<AtomicU32>,
}

impl Reducer for JoinCounter {
    type State = u64;
    type Action = JoinAct;
    type Op = JoinOp;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, _s: &u64, action: JoinAct, out: &mut EffectBuf<JoinAct, JoinOp>) -> u64 {
        self.processed.fetch_add(1, Ordering::SeqCst);
        if let JoinAct::Start = action {
            out.push(Effect::Run {
                id: EffectId::new(1),
                op: JoinOp::Work,
            });
        }
        0
    }
}

/// 解释器：派生 worker 并等它完成——worker 只**提交**动作（框架提供的 sender 在
/// drain 回调内 = **竞争态** ⇒ 纯准入、不等完成）。
struct JoinSpawner {
    admitted: Arc<AtomicU32>,
}

impl EffectRuntime<JoinAct, JoinOp> for JoinSpawner {
    fn run(&mut self, _effect: Effect<JoinAct, JoinOp>, back: &ActionSender<JoinAct>) {
        let sender = back.clone();
        let admitted = Arc::clone(&self.admitted);
        let (tx, rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let ok = sender.dispatch(JoinAct::Follow);
            if ok {
                admitted.fetch_add(1, Ordering::SeqCst);
            }
            let _ = tx.send(ok);
        });
        // 用带超时的完成握手代替裸 join（裸 join 会把死锁形态变成挂死的测试进程）。
        let ok = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("worker 未在 10s 内完成——若 worker 改用阻塞式 Store::dispatch，即死锁形态");
        assert!(
            ok,
            "worker 的动作必须被准入（框架提供的 sender 的 dispatch 返回 true）"
        );
        worker.join().expect("worker 线程"); // 已收到完成信号 ⇒ worker 随即退出
    }
}

/// 正向场景：回调派生的 worker 用 `ActionSender::dispatch` ⇒ 回调不死锁，排队的
/// action 恰好处理一次。
#[test]
fn joined_worker_using_sender_does_not_deadlock_and_is_processed_once() {
    let processed = Arc::new(AtomicU32::new(0));
    let admitted = Arc::new(AtomicU32::new(0));
    let store = SyncStore::new(
        JoinCounter {
            processed: Arc::clone(&processed),
        },
        JoinSpawner {
            admitted: Arc::clone(&admitted),
        },
    );

    // ① 外部 dispatch 正常返回（回调派生并等 worker ⇒ 若误用阻塞式 dispatch 则挂死）。
    store.dispatch(JoinAct::Start);
    assert_eq!(
        admitted.load(Ordering::SeqCst),
        1,
        "worker 的动作必须被准入"
    );

    // ② 再排空一次 ⇒ worker 提交的动作被处理（Start + Follow + Bump 各一次）。
    store.dispatch(JoinAct::Bump);
    assert_eq!(
        processed.load(Ordering::SeqCst),
        3,
        "Start、Follow、Bump 三个动作各被处理恰好一次"
    );
    assert_eq!(store.pending_actions(), 0, "队列不得残留");
    store.close();
}
