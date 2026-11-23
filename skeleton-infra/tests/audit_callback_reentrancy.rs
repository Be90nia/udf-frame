#![cfg(feature = "runtime")]
//! issue#59 回归：**夺得排空租约**的那次调用，其首个 `Enqueued` 实时回调内
//! 可以重入同 store 的 `dispatch`（契约承诺「回调锁外调用、可重入本 store」）。
//!
//! 修复前：`enqueue` 已置 `draining=true` 并 pop 出首条记录，但排空帧只在
//! `drain_owned` 内登记 ⇒ 回调期间 `admission_context` 看不到同栈上下文，把
//! 嵌套 dispatch 判为外部根（wait=true）⇒ `enqueue` 返回 `Queued` ⇒ 调用方在
//! **无界**的 `Ledger::wait` 上等自己 = 确定性单线程死锁。
//!
//! 编排纪律：每条用例的重入调用都跑在独立线程上、由主线程**有界超时**收信号
//! ——死锁表现为一条有界的红（而不是把 CI 挂死）。三条夺租约路径各一条：
//! `Store::dispatch` / `SyncStore::try_dispatch` / 框架 `ActionSender::dispatch`。
//! 另加一条纯回调控制测试钉住 `Enqueued`-before-`Reduced` 顺序。

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{ActionSender, Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::{AuditEvent, AuditEventKind};
use skeleton_infra::runtime::{AdmissionOutcome, SyncStore as Store};

/// 有界握手超时（禁无界等待：死锁应表现为一条红，而非 CI 挂死）。
const BOUND: Duration = Duration::from_secs(10);

// ── 固定装置：只自增的计数器（无效果 ⇒ 事件面只有 Enqueued/Reduced）──────

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

/// 有界执行：闭包跑在独立线程上，主线程只按超时收结果——死锁（或线程内
/// panic）以明确的失败信息收口，绝不无限阻塞测试线程。
fn run_bounded<R: Send + 'static>(what: &str, f: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(f());
    });
    match rx.recv_timeout(BOUND) {
        Ok(out) => out,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!(
            "{what}：{}s 内有界窗口未返回——回调内同 store dispatch 在等自己（issue#59 死锁回归）",
            BOUND.as_secs()
        ),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("{what}：调用线程在返回前 panic")
        }
    }
}

/// 首个 `Enqueued` 事件内重入同 store `dispatch` **恰好一次**（其余事件直通）。
///
/// 「恰好一次」由计数原子保证：嵌套 dispatch 自己引发的 Enqueued 事件被
/// 同 store 重入抑制只录环、不进回调，`taken` 的先行判足以封死递归，双保险。
fn reentrant_on_first_enqueued(
    store: Arc<Store<u64, u64, Infallible>>,
    nested_calls: Arc<AtomicUsize>,
    taken: Arc<AtomicUsize>,
) -> impl FnMut(&AuditEvent) + Send + 'static {
    move |event: &AuditEvent| {
        if matches!(event.kind, AuditEventKind::Enqueued { .. })
            && taken.fetch_add(1, Ordering::SeqCst) == 0
        {
            nested_calls.fetch_add(1, Ordering::SeqCst);
            store.dispatch(1);
        }
    }
}

/// 环里全部 `Enqueued` 事件的回灌深度（升序即发生序）。
fn enqueued_depths(events: &[AuditEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e.kind {
            AuditEventKind::Enqueued { feedback_depth } => Some(feedback_depth),
            _ => None,
        })
        .collect()
}

// ── ① Store::dispatch 夺租约路径 ────────────────────────────────────────

#[test]
fn winning_dispatch_enqueued_callback_reenters_same_store_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let nested_calls = Arc::new(AtomicUsize::new(0));
    let _ticket = store.subscribe_events(
        64,
        reentrant_on_first_enqueued(
            Arc::clone(&store),
            Arc::clone(&nested_calls),
            Arc::new(AtomicUsize::new(0)),
        ),
    );

    let st = Arc::clone(&store);
    run_bounded("Store::dispatch 夺租约路径", move || st.dispatch(1));

    assert_eq!(
        nested_calls.load(Ordering::SeqCst),
        1,
        "首个 Enqueued 回调内的同 store dispatch 必须恰好执行一次并返回"
    );
    assert_eq!(
        *store.snapshot(),
        2,
        "外层 action 与回调内的嵌套 action 都必须被 reduce（嵌套者由外层排空环覆盖）"
    );
    // 深度归属钉：嵌套 dispatch 是**首条记录**（深度 0）的因果后代 ⇒ 深度 1。
    // 漏装临时帧 ⇒ 0（外部根）；重复计首条自身深度 ⇒ 2。
    assert_eq!(
        enqueued_depths(&store.take_events()),
        vec![0, 1],
        "嵌套回调 dispatch 的深度 = 首条记录深度 + 1（不重复计首条自身深度）"
    );
}

// ── ② SyncStore::try_dispatch 夺租约路径 ────────────────────────────────

#[test]
fn winning_try_dispatch_enqueued_callback_reenters_same_store_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let nested_calls = Arc::new(AtomicUsize::new(0));
    let _ticket = store.subscribe_events(
        64,
        reentrant_on_first_enqueued(
            Arc::clone(&store),
            Arc::clone(&nested_calls),
            Arc::new(AtomicUsize::new(0)),
        ),
    );

    let st = Arc::clone(&store);
    let outcome = run_bounded("SyncStore::try_dispatch 夺租约路径", move || {
        st.try_dispatch(1)
    });

    assert_eq!(outcome, AdmissionOutcome::Accepted, "静默态夺租约 ⇒ 已准入");
    assert_eq!(
        nested_calls.load(Ordering::SeqCst),
        1,
        "首个 Enqueued 回调内的同 store dispatch 必须恰好执行一次并返回"
    );
    assert_eq!(
        *store.snapshot(),
        2,
        "嵌套 action 由外层排空环覆盖（排空至 idle）"
    );
}

// ── ③ 框架 ActionSender::dispatch 夺租约路径 ────────────────────────────

#[test]
fn winning_sender_dispatch_enqueued_callback_reenters_same_store_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let nested_calls = Arc::new(AtomicUsize::new(0));
    let _ticket = store.subscribe_events(
        64,
        reentrant_on_first_enqueued(
            Arc::clone(&store),
            Arc::clone(&nested_calls),
            Arc::new(AtomicUsize::new(0)),
        ),
    );

    let sender = store.sender();
    let accepted = run_bounded("ActionSender::dispatch 夺租约路径", move || {
        sender.dispatch(1)
    });

    assert!(accepted, "静默态夺租约 ⇒ 框架 sender 返回 true");
    assert_eq!(
        nested_calls.load(Ordering::SeqCst),
        1,
        "首个 Enqueued 回调内的同 store dispatch 必须恰好执行一次并返回"
    );
    assert_eq!(
        *store.snapshot(),
        2,
        "嵌套 action 由外层排空环覆盖（排空至 idle）"
    );
}

// ── ④ 纯回调控制：Enqueued-before-Reduced（事件序不因 #59 修复而漂移）────

#[test]
fn enqueued_precedes_reduced_for_the_same_action() {
    let store = Store::new(Counter, NoFx);
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&kinds);
    let _ticket = store.subscribe_events(64, move |event: &AuditEvent| {
        recorded.lock().expect("kinds lock").push(event.kind);
    });

    store.dispatch(1);

    let kinds = kinds.lock().expect("kinds lock").clone();
    let enqueued = kinds
        .iter()
        .position(|k| matches!(k, AuditEventKind::Enqueued { .. }))
        .expect("Enqueued 必须投递进实时回调");
    let reduced = kinds
        .iter()
        .position(|k| matches!(k, AuditEventKind::Reduced))
        .expect("Reduced 必须投递进实时回调");
    assert!(
        enqueued < reduced,
        "Enqueued 必须先于 Reduced 投递（事件序契约）：{kinds:?}"
    );
}

// ── ⑤ 回调 panic：临时帧随展开弹出，排空租约与回执失败策略不变 ──────────

#[test]
fn panic_in_first_enqueued_callback_still_drains_and_pops_temp_frame() {
    let store = Store::new(Counter, NoFx);
    let panicked = Arc::new(AtomicUsize::new(0));
    let fired = Arc::new(AtomicUsize::new(0));
    let p = Arc::clone(&panicked);
    let f = Arc::clone(&fired);
    let _ticket = store.subscribe_events(64, move |event: &AuditEvent| {
        if matches!(event.kind, AuditEventKind::Enqueued { .. })
            && f.fetch_add(1, Ordering::SeqCst) == 0
        {
            p.fetch_add(1, Ordering::SeqCst);
            panic!("首条 Enqueued 回调 panic（fail-close 路径）");
        }
    });

    store.dispatch(1);
    assert_eq!(
        *store.snapshot(),
        1,
        "回调 panic 不得阻断排空租约（dispatch 仍完成 reduce 与本 action 的完成边界）"
    );
    assert_eq!(
        store.subscriber_panics(),
        1,
        "回调 panic = fail-closed 摘除并计数（既有策略不变）"
    );

    // 临时帧必须已随展开弹出：下一次同线程 dispatch 仍是外部根（深度 0）。
    // 残留的临时帧会把它误判为同栈回灌（深度 1）。
    store.dispatch(1);
    assert_eq!(*store.snapshot(), 2);
    assert_eq!(
        enqueued_depths(&store.take_events()),
        vec![0, 0],
        "回调 panic 后临时帧不得残留（否则后续 dispatch 被误判为同栈回灌）"
    );
}
