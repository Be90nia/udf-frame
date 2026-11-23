#![cfg(feature = "pool")]
//! 池域审计流并发投递（issue#75）：回调在途窗口内的并发 emit 不得静默丢事件。
//!
//! 编排（全程 channel 握手 + 有界轮询，禁 sleep 定时）：
//! 1. 线程 1 触发事件 A ⇒ 实时回调在门闩上阻塞（回调**锁外**调用，不持池锁）；
//! 2. 主线程并发触发事件 B（A 的回调仍在途）；
//! 3. 放行 A ⇒ 在途者串行补投 B；
//! 4. 断言 A、B **各投递恰好一次**且按序（环照录 + 实时回调流都完整）。
//!
//! 附带回归：回调 panic ⇒ fail-closed 摘除（计数留痕），该代次积压的事件
//! **不投递到后继订阅**（issue#75「panic 与并发 emit 竞态」验收项）。
//!
//! 覆盖面：`PoolBase` 与 `ObjectPool` 共用 `PoolAuditSink`（公共逻辑写一次），
//! 两池各跑一遍同一编排。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use skeleton::pool::{PoolAuditEvent, PoolAuditKind, PoolManaged};
use skeleton_infra::objpool::ObjectPool;
use skeleton_infra::pool::PoolBase;

/// 有界握手超时（禁无界等待：卡死应表现为一条红，而非 CI 挂死）。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 造一个「首件阻塞、其余直通」的回调（记录投递次数与事件种类）。
fn gated_callback(
    entered: Sender<()>,
    release: Receiver<()>,
    calls: Arc<AtomicUsize>,
    kinds: Arc<Mutex<Vec<PoolAuditKind>>>,
) -> impl FnMut(&PoolAuditEvent) + Send + 'static {
    let mut release = Some(release);
    move |e: &PoolAuditEvent| {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        kinds.lock().expect("kinds lock").push(e.kind);
        if n == 0 {
            entered
                .send(())
                .expect("entered receiver alive (test drives the gate)");
            release
                .take()
                .expect("release armed")
                .recv()
                .expect("release sent by test");
        }
    }
}

/// 有界轮询等待条件成立（禁固定 sleep 断言）。
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = std::time::Instant::now();
    while !f() {
        assert!(
            start.elapsed() < HANDSHAKE_TIMEOUT,
            "timeout waiting for {what}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

// ── PoolBase：并发 emit 不丢事件 ────────────────────────────────────────

#[test]
fn pool_base_concurrent_emit_during_inflight_callback_is_not_lost() {
    let pool = Arc::new(PoolBase::new(4, 2));
    let (entered_tx, entered_rx) = channel();
    let (release_tx, release_rx) = channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let _ticket = pool.subscribe_audit(
        8,
        gated_callback(
            entered_tx,
            release_rx,
            Arc::clone(&calls),
            Arc::clone(&kinds),
        ),
    );

    // ① 线程 1：事件 A ⇒ 实时回调在门闩上阻塞（窗口打开）。
    let emitter = Arc::clone(&pool);
    let thread_a = thread::spawn(move || {
        emitter.note_borrow();
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (A in flight)");

    // ② 主线程：事件 B（A 的回调仍在途 ⇒ 旧实现里 B 只入环、不进回调）。
    pool.note_borrow();

    // ③ 放行 A ⇒ 在途者补投 B。
    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    // ④ 断言：两件各投递恰好一次（不丢、不重），且环照录两件。
    let seen = kinds.lock().expect("kinds lock").clone();
    assert_eq!(
        seen.len(),
        2,
        "并发 emit 不得丢事件（实时回调流必须完整）：{seen:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "每事件恰好投递一次（禁重复投递）"
    );
    assert!(
        seen.iter().all(|k| *k == PoolAuditKind::Borrowed),
        "两件都是 Borrowed：{seen:?}"
    );
    assert_eq!(pool.take_audit_events().len(), 2, "环照录两件（历史完整）");
}

// ── ObjectPool：同一编排（共用抽取件，行为一致） ────────────────────────

#[test]
fn object_pool_concurrent_emit_during_inflight_callback_is_not_lost() {
    let pool = Arc::new(ObjectPool::new(4, 2, || 0u32));
    let (entered_tx, entered_rx) = channel();
    let (release_tx, release_rx) = channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let _ticket = PoolManaged::subscribe_audit(
        &*pool,
        gated_callback(
            entered_tx,
            release_rx,
            Arc::clone(&calls),
            Arc::clone(&kinds),
        ),
    );

    // ① 线程 1：借出一件 ⇒ Borrowed 事件 ⇒ 回调在门闩上阻塞。
    let emitter = Arc::clone(&pool);
    let thread_a = thread::spawn(move || {
        let _held = emitter.acquire();
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (A in flight)");

    // ② 主线程：再借一件 ⇒ 第二件 Borrowed（A 的回调仍在途）。
    let _held_b = pool.acquire();

    // ③ 放行 A ⇒ 在途者补投 B。
    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    // ④ 断言：两件都恰好投递一次（不丢、不重）。
    let seen = kinds.lock().expect("kinds lock").clone();
    let borrowed = seen
        .iter()
        .filter(|k| **k == PoolAuditKind::Borrowed)
        .count();
    assert_eq!(
        borrowed, 2,
        "并发 emit 不得丢事件（ObjectPool 面同款）：{seen:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        seen.len(),
        "每事件恰好投递一次（禁重复投递）：{seen:?}"
    );
}

// ── 回归：回调 panic ⇒ fail-close 本代次，排队事件不泄漏进替换订阅 ────────

#[test]
fn callback_panic_discards_pending_events_and_keeps_generation_isolated() {
    let pool = Arc::new(PoolBase::new(4, 2));
    let (entered_tx, entered_rx) = channel();
    let (release_tx, release_rx) = channel();
    let calls = Arc::new(AtomicUsize::new(0));
    // 回调：首件阻塞到放行 ⇒ 放行后 panic（fail-closed 摘除）。
    let _ticket = pool.subscribe_audit(8, move |_e| {
        calls.fetch_add(1, Ordering::SeqCst);
        entered_tx.send(()).expect("entered receiver alive");
        release_rx.recv().expect("release sent by test");
        panic!("boom");
    });

    // ① 线程 1：事件 A ⇒ 回调在门闩上阻塞（窗口打开）。
    let emitter = Arc::clone(&pool);
    let thread_a = thread::spawn(move || {
        emitter.note_borrow();
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (A in flight)");

    // ② 主线程：事件 B（在途窗口内 ⇒ B 排在回调侧的待投递队列里）。
    pool.note_borrow();

    // ③ 放行 A ⇒ 回调 panic ⇒ fail-close 本代次（回调摘除 + 排队事件作废）。
    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");
    assert_eq!(pool.audit_callback_panics(), 1, "回调 panic 留痕且只摘一次");

    // ④ 后继订阅：只收自己窗内的新事件，旧代次排队事件**不**泄漏进来。
    let successor_calls = Arc::new(AtomicUsize::new(0));
    let successor_kinds = Arc::new(Mutex::new(Vec::new()));
    let sc = Arc::clone(&successor_calls);
    let sk = Arc::clone(&successor_kinds);
    let _next = pool.subscribe_audit(8, move |e| {
        sc.fetch_add(1, Ordering::SeqCst);
        sk.lock().expect("successor kinds lock").push(e.kind);
    });
    pool.note_borrow();
    wait_until("successor callback delivered", || {
        successor_calls.load(Ordering::SeqCst) >= 1
    });
    assert_eq!(
        successor_calls.load(Ordering::SeqCst),
        1,
        "后继订阅只收自己窗内的事件（旧代次积压禁泄漏）"
    );
    assert_eq!(
        successor_kinds.lock().expect("successor kinds lock").len(),
        1,
        "后继订阅恰好一件"
    );
}
