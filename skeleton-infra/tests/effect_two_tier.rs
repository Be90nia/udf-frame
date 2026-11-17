#![cfg(all(feature = "runtime", feature = "tokio"))]
//! 效果执行两档（蓝图 §11 项 6）：同步档（排空环内内联）+ 托管档（`FxScope`）。
//!
//! 每测钉一条可观察契约，语义口径见 `docs/reference.md` 的
//! 「effect 执行两档」节与 `skeleton::EffectRuntime` 契约文档：
//!
//! ① 同步档：效果在 `dispatch` 调用栈内、排空环转发段执行完——dispatch 返回即
//!    「效果已执行 + 同栈回灌的动作已排空」；
//! ② 同步档失败面：解释器 panic 原样从 `dispatch` 上浮（fail-visible），实例不
//!    报废（毒锁下一次效果转发时恢复并计数）；
//! ③ 托管档：效果交 `FxScope` 后 `dispatch` **不**等它完成（核心永不阻塞等异步）；
//! ④ 托管档失败面：任务失败经 `Spawned::join` / `ShutdownReport` 可见，store 照常可用；
//! ⑤ 两档语义一致面：同一 action 序列在两档下状态终值相同。

use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::fx::{FxScope, Spawned, TaskError};
use skeleton_infra::runtime::SyncStore;
use tokio::sync::oneshot;

// ── 固定装置：action/op 词表 + 计数器 reducer ───────────────────────────────

/// `Add(n)` = 外部动作（加 n，并发一条 `Load(n)` 效果）；
/// `Ack(n)` = 效果结果的回灌动作（再加 n）；
/// `Bump` = 零效果动作（收尾期证明 store 本身仍可用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Act {
    Add(u64),
    Ack(u64),
    Bump,
}

/// 效果载荷：加载 n 并由解释器回灌 `Ack(n)`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Load(u64),
}

struct Counter;

impl Reducer for Counter {
    type State = u64;
    type Action = Act;
    type Op = Op;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, action: Act, out: &mut EffectBuf<Act, Op>) -> u64 {
        match action {
            Act::Add(n) => {
                out.push(Effect::Run {
                    id: EffectId::new(1),
                    op: Op::Load(n),
                });
                s + n
            }
            Act::Ack(n) => s + n,
            Act::Bump => s + 1,
        }
    }
}

// ── 同步档解释器：排空环内联执行 + 同栈回灌 ────────────────────────────────

/// 取出效果载荷（reducer 只产 `Run`；`Emit` 由核心直接回灌，不进解释器）。
fn load_payload(effect: Effect<Act, Op>) -> u64 {
    let Effect::Run {
        op: Op::Load(n), ..
    } = effect
    else {
        unreachable!("reducer 只产 Run：Emit 由核心直接回灌，不进解释器");
    };
    n
}

struct InlineFx {
    seen: Arc<Mutex<Vec<Op>>>,
}

impl EffectRuntime<Act, Op> for InlineFx {
    fn run(&mut self, effect: Effect<Act, Op>, back: &ActionSender<Act>) {
        let n = load_payload(effect);
        self.seen.lock().unwrap().push(Op::Load(n));
        assert!(back.dispatch(Act::Ack(n)), "回灌被拒：store 已闭门");
    }
}

/// 首轮 panic、次轮正常解释——钉「解释器 panic 不报废实例」的恢复面。
struct PanicOnceFx {
    panicked: bool,
    seen: Arc<Mutex<Vec<Op>>>,
}

impl EffectRuntime<Act, Op> for PanicOnceFx {
    fn run(&mut self, effect: Effect<Act, Op>, back: &ActionSender<Act>) {
        if !self.panicked {
            self.panicked = true;
            panic!("boom-sync-fx");
        }
        let n = load_payload(effect);
        self.seen.lock().unwrap().push(Op::Load(n));
        assert!(back.dispatch(Act::Ack(n)), "回灌被拒：store 已闭门");
    }
}

// ── 托管档解释器：效果交 FxScope（可选闸门观察中间态）─────────────────────

/// `gate`/`started` 是测试观察钩子：`started` 在任务内先发（证明任务起跑），
/// `gate` 由测试释放（在此之前效果不可能完成）。`None` = 无闸直跑。
struct ManagedFx {
    scope: Arc<FxScope>,
    gate: Option<oneshot::Receiver<()>>,
    started: Option<oneshot::Sender<()>>,
}

impl EffectRuntime<Act, Op> for ManagedFx {
    fn run(&mut self, effect: Effect<Act, Op>, back: &ActionSender<Act>) {
        let n = load_payload(effect);
        let gate = self.gate.take();
        let started = self.started.take();
        let back = back.clone();
        self.scope
            .spawn_tracked(async move {
                if let Some(started) = started {
                    let _ = started.send(());
                }
                if let Some(gate) = gate {
                    let _ = gate.await; // 发送端丢弃 = 开闸（测试收尾兜底）
                }
                assert!(back.dispatch(Act::Ack(n)), "回灌被拒：store 已闭门");
            })
            .expect("fx scope 关闸时 spawn 失败（测试期不应发生）");
    }
}

/// 任务内 panic，句柄留给测试 join（失败可见面）。
struct PanicFx {
    scope: Arc<FxScope>,
    handles: Arc<Mutex<Vec<Spawned<()>>>>,
}

impl EffectRuntime<Act, Op> for PanicFx {
    fn run(&mut self, _effect: Effect<Act, Op>, _back: &ActionSender<Act>) {
        let handle: Spawned<()> = self
            .scope
            .spawn_tracked(async { panic!("boom-async-fx") })
            .expect("fx scope 关闸时 spawn 失败（测试期不应发生）");
        self.handles.lock().unwrap().push(handle);
    }
}

/// 等状态收敛（托管档回灌在另一任务里，需让出执行权）：5s 墙钟上限。
async fn wait_state(store: &SyncStore<u64, Act, Op>, want: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let got = *store.snapshot();
        if got == want {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "状态未收敛：want {want}, got {got}"
        );
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

// ── ①同步档：dispatch 返回时效果已执行、同栈回灌已排空 ────────────────────

#[test]
fn sync_tier_runs_effect_inline_before_dispatch_returns() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Counter,
        InlineFx {
            seen: Arc::clone(&seen),
        },
    );

    store.dispatch(Act::Add(5));

    // dispatch 返回即 idle：解释器已在本线程排空环内跑完，回灌的 Ack(5) 也已排空
    assert_eq!(*seen.lock().unwrap(), vec![Op::Load(5)]);
    assert_eq!(*store.snapshot(), 10);
    assert_eq!(store.pending_actions(), 0);
    store.close();
}

// ── ②同步档失败面：panic 上浮 + 实例恢复（计数只在恢复时 +1）──────────────

#[test]
fn sync_tier_effect_panic_propagates_and_instance_recovers() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Counter,
        PanicOnceFx {
            panicked: false,
            seen: Arc::clone(&seen),
        },
    );

    let first = std::panic::catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Add(5))));
    let payload = first.expect_err("解释器 panic 必须原样从 dispatch 上浮（fail-visible）");
    assert_eq!(
        payload.downcast_ref::<&str>().copied(),
        Some("boom-sync-fx"),
        "panic 载荷原样传播，不被吞改"
    );
    // 管线序 reduce → 快照落位 → 效果转发：状态已前进，效果 panic 不改写这一点
    assert_eq!(*store.snapshot(), 5);
    // 毒位未被观察前不计：计数在下一次效果转发的恢复路径 +1（见 runtime_panic_count doc）
    assert_eq!(store.runtime_panic_count(), 0);
    assert!(seen.lock().unwrap().is_empty(), "panic 的那条效果未被执行");

    // 实例不报废：下一次产效果的 dispatch 恢复毒锁、计数、正常解释
    store.dispatch(Act::Add(3));
    assert_eq!(store.runtime_panic_count(), 1);
    assert_eq!(*seen.lock().unwrap(), vec![Op::Load(3)]);
    assert_eq!(*store.snapshot(), 11); // 5 + 3（Add）+ 3（回灌 Ack）
    store.close();
}

// ── ③托管档：dispatch 不等效果完成（核心永不阻塞等异步）──────────────────

#[tokio::test]
async fn async_tier_dispatch_returns_before_effect_completes() {
    let scope = Arc::new(FxScope::new());
    let (gate_tx, gate_rx) = oneshot::channel();
    let (started_tx, started_rx) = oneshot::channel();
    let store = SyncStore::new(
        Counter,
        ManagedFx {
            scope: Arc::clone(&scope),
            gate: Some(gate_rx),
            started: Some(started_tx),
        },
    );

    store.dispatch(Act::Add(5));
    assert_eq!(*store.snapshot(), 5, "dispatch 返回时只有 reducer 结果");

    // 任务确已起跑且卡在闸门：dispatch 返回 ≠ 效果完成（核心没等它）
    started_rx.await.expect("效果任务应已起跑");
    assert_eq!(*store.snapshot(), 5, "任务在途未回灌，状态不动");

    gate_tx.send(()).expect("效果任务仍存活");
    wait_state(&store, 10).await;

    // 任务终局进停机报告（失败/完成可观察面）
    let report = scope.shutdown().await;
    assert_eq!(report.completed, 1);
    store.close();
}

// ── ④托管档失败面：join 与 ShutdownReport 可见，store 不受影响 ─────────────

#[tokio::test]
async fn async_tier_task_failure_is_visible_and_isolated() {
    let scope = Arc::new(FxScope::new());
    let handles = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Counter,
        PanicFx {
            scope: Arc::clone(&scope),
            handles: Arc::clone(&handles),
        },
    );

    store.dispatch(Act::Add(1));
    assert_eq!(
        *store.snapshot(),
        1,
        "dispatch 不等效果完成，终态停在 reducer 结果"
    );

    let handle = handles
        .lock()
        .unwrap()
        .pop()
        .expect("run 内同步登记句柄，dispatch 返回时必已入表");
    assert_eq!(
        handle.join().await,
        Err(TaskError::Panicked("boom-async-fx".to_owned()))
    );

    // 同一 panic 在停机报告里也可见（句柄已 join，生命周期集条目照样收割）
    let report = scope.shutdown().await;
    assert_eq!(report.panicked, vec!["boom-async-fx".to_owned()]);

    // 效果失败不拖垮 store：零效果动作照常排空
    store.dispatch(Act::Bump);
    assert_eq!(*store.snapshot(), 2);
    store.close();
}

// ── ⑤两档语义一致面：同一 action 序列 → 同一状态终值 ──────────────────────

#[tokio::test]
async fn both_tiers_converge_to_same_terminal_state() {
    let seq = [Act::Add(5), Act::Add(3)];

    let sync_store = SyncStore::new(
        Counter,
        InlineFx {
            seen: Arc::new(Mutex::new(Vec::new())),
        },
    );
    for &action in &seq {
        sync_store.dispatch(action);
        // --nocapture 观察面：同步档 dispatch 返回即终态（回灌已排空）
        println!("[sync ] after {action:?}: state={}", *sync_store.snapshot());
    }
    assert_eq!(*sync_store.snapshot(), 16);

    let scope = Arc::new(FxScope::new());
    let async_store = SyncStore::new(
        Counter,
        ManagedFx {
            scope: Arc::clone(&scope),
            gate: None,
            started: None,
        },
    );
    for &action in &seq {
        async_store.dispatch(action);
        // current_thread：同步段内 spawn 的任务不会被抢占，读数是确定的中间态
        println!(
            "[async] after {action:?}: state={}（回灌尚未发生）",
            *async_store.snapshot()
        );
    }
    wait_state(&async_store, 16).await;
    println!("[async] converged: state={}", *async_store.snapshot());

    assert_eq!(*async_store.snapshot(), *sync_store.snapshot());
    let report = scope.shutdown().await;
    assert_eq!(report.completed, seq.len());
    sync_store.close();
    async_store.close();
}
