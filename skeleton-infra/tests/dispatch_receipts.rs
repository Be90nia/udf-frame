#![cfg(feature = "runtime")]
//! per-action 序列回执（GitHub issue#40）：外部并发 dispatch 必须等自己的
//! action 到达完成边界才返回；同栈回灌仅入队不等待（禁自死锁）；FIFO 保持；
//! panic/close 让每个等待者恰好结算一次（无人悬挂）。

use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, NoFx, Reducer, Store as _,
};
use skeleton_infra::runtime::SyncStore;
use std::convert::Infallible;
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
