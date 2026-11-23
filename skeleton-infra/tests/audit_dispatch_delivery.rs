#![cfg(feature = "runtime")]
//! 审计实时回调的串行派发（issue#65 主路线①）：回调在途窗口内的并发 emit
//! 不得静默丢事件——入队待补投、按 `seq` 序补投、每事件恰好一次。
//!
//! 编排（全程 channel 握手，禁 sleep 定时）：
//! 1. 线程 A dispatch ⇒ 实时回调在门闩上阻塞（窗口打开）；
//! 2. 主线程并发 emit（sender 面 = 竞争态纯准入，不等完成边界）⇒ 事件 B；
//! 3. 放行 A ⇒ 在途者串行补投 B；
//! 4. 断言 B **确实进了回调且恰好一次**（修复前 B 只入环、不进回调 ⇒ 真红）。
//!
//! 附带回归：回调 panic ⇒ fail-close 本代次（排队事件作废、不泄漏进替换
//! 订阅）；同 store 重入仍只录环不递归回调；待投递 FIFO 溢出 ⇒ 独立的
//! **投递丢弃计数**（与环溢出分账）。

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{ActionSender, Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::{AuditEvent, AuditEventKind};
use skeleton_infra::runtime::SyncStore as Store;

/// 有界握手超时（禁无界等待：卡死应表现为一条红，而非 CI 挂死）。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// 待投递 FIFO 上限（镜像 `runtime::audit_emit::AUDIT_PENDING_CAP`；该常量是
/// crate 私有，外部测试只能按值镜像——上限变更会让本文件的溢出断言变红）。
const PENDING_CAP: usize = 1024;

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

/// 首件阻塞、其余直通的回调（进入/放行两段握手；记录投递事件与次数）。
fn gated_callback(
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<AuditEvent>>>,
) -> impl FnMut(&AuditEvent) + Send + 'static {
    move |e: &AuditEvent| {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        seen.lock().expect("seen lock").push(*e);
        if n == 0 {
            entered.send(()).expect("entered receiver alive");
            release.recv().expect("release sent by test");
        }
    }
}

/// 取回调见到的 `Enqueued` 事件序号（升序即投递序）。
fn enqueued_seqs(seen: &[AuditEvent]) -> Vec<u64> {
    seen.iter()
        .filter(|e| matches!(e.kind, AuditEventKind::Enqueued { .. }))
        .map(|e| e.seq)
        .collect()
}

// ── ① 并发 emit 在回调在途窗口内不得丢（修复前真红）──────────────────────

#[test]
fn concurrent_emit_during_inflight_callback_is_delivered_exactly_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _ticket = store.subscribe_events(
        64,
        gated_callback(
            entered_tx,
            release_rx,
            Arc::clone(&calls),
            Arc::clone(&seen),
        ),
    );

    // ① 线程 A：dispatch ⇒ 首个事件进回调并在门闩上阻塞（窗口打开）。
    let st = Arc::clone(&store);
    let thread_a = thread::spawn(move || {
        st.dispatch(1);
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (first event in flight)");

    // ② 主线程：并发 emit（sender 面 = 竞争态纯准入，不等完成边界）⇒ 事件 B。
    let sender = store.sender();
    assert!(sender.dispatch(2), "第二件已入队");

    // ③ 放行 A ⇒ 在途者补投 B。
    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    // ④ 断言：两件 Enqueued **各进回调恰好一次**（修复前 B 只入环、不进回调）。
    let seen = seen.lock().expect("seen lock").clone();
    let seqs = enqueued_seqs(&seen);
    assert_eq!(
        seqs.len(),
        2,
        "并发 emit 的事件必须补投进回调（实时回调流不得缺件）：{seqs:?}"
    );
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "补投按 seq 序：{seqs:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        seen.len(),
        "每事件恰好投递一次（禁重复投递）：{seqs:?}"
    );
    // 环照录两件（权威历史独立于实时回调流）。
    let ring = store.take_events();
    assert!(
        ring.iter()
            .filter(|e| matches!(e.kind, AuditEventKind::Enqueued { .. }))
            .count()
            >= 2,
        "环照录两件（历史完整）：{ring:?}"
    );
}

// ── ② 待投递队列按 seq 序补投、每事件恰好一次 ───────────────────────────

#[test]
fn pending_events_are_replayed_in_seq_order_exactly_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _ticket = store.subscribe_events(
        64,
        gated_callback(
            entered_tx,
            release_rx,
            Arc::clone(&calls),
            Arc::clone(&seen),
        ),
    );

    let st = Arc::clone(&store);
    let thread_a = thread::spawn(move || {
        st.dispatch(1);
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (first event in flight)");

    // 并发三件（竞争态纯准入 ⇒ 三件都在回调在途窗口内入队）。
    let sender = store.sender();
    for action in 2..5 {
        assert!(sender.dispatch(action), "并发件已入队");
    }

    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    let seen = seen.lock().expect("seen lock").clone();
    let seqs = enqueued_seqs(&seen);
    assert_eq!(seqs.len(), 4, "四件 Enqueued 各进回调一次：{seqs:?}");
    assert!(
        seqs.windows(2).all(|w| w[0] < w[1]),
        "补投严格按 seq 升序（禁乱序）：{seqs:?}"
    );
    let mut dedup = seqs.clone();
    dedup.sort_unstable();
    dedup.dedup();
    assert_eq!(
        dedup.len(),
        seqs.len(),
        "每事件恰好一次（禁重复投递）：{seqs:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        seen.len(),
        "回调次数 = 投递件数（禁重复投递）：{seqs:?}"
    );
}

// ── ③ 回调 panic ⇒ fail-close 本代次，排队事件不泄漏进替换订阅 ──────────

#[test]
fn callback_panic_discards_pending_events_and_isolates_generation() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let pc = Arc::clone(&panic_calls);
    // 回调：首件阻塞到放行 ⇒ 放行后 panic（fail-closed 摘除）。
    let _ticket = store.subscribe_events(64, move |_e: &AuditEvent| {
        pc.fetch_add(1, Ordering::SeqCst);
        entered_tx.send(()).expect("entered receiver alive");
        release_rx.recv().expect("release sent by test");
        panic!("boom");
    });

    let st = Arc::clone(&store);
    let thread_a = thread::spawn(move || {
        st.dispatch(1);
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (first event in flight)");

    // 并发件 B（在途窗口内 ⇒ 排在回调侧的待投递队列里，seq = 2）。
    let sender = store.sender();
    assert!(sender.dispatch(2), "并发件已入队");

    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    assert_eq!(panic_calls.load(Ordering::SeqCst), 1, "回调只被调用一次");
    assert_eq!(store.subscriber_panics(), 1, "回调 panic 留痕且只摘一次");
    assert!(
        store.audit_delivery_drops() >= 1,
        "本代次排队事件作废须计入投递丢弃计数（禁静默丢）：{}",
        store.audit_delivery_drops()
    );

    // 替换订阅：只收自己窗内的新事件，旧代次排队事件（seq = 2）**不**泄漏进来。
    let succ_seen = Arc::new(Mutex::new(Vec::<AuditEvent>::new()));
    let ss = Arc::clone(&succ_seen);
    let _next = store.subscribe_events(64, move |e: &AuditEvent| {
        ss.lock().expect("successor seen lock").push(*e);
    });
    store.dispatch(3);

    let succ = succ_seen.lock().expect("successor seen lock").clone();
    assert!(
        succ.iter().all(|e| e.seq > 2),
        "旧代次排队事件（seq = 2）禁泄漏进替换订阅：{succ:?}"
    );
    assert_eq!(succ.len(), 2, "替换订阅只收自己窗内的两件：{succ:?}");
}

// ── ④ 同 store 重入仍只录环、不递归回调（既有行为不回归）────────────────

#[test]
fn same_store_reentrant_emit_is_recorded_but_not_delivered_to_callback() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let seen = Arc::new(Mutex::new(Vec::<AuditEvent>::new()));
    let calls = Arc::new(AtomicUsize::new(0));
    let reentered = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&seen);
    let c = Arc::clone(&calls);
    let r = Arc::clone(&reentered);
    // 弱引用：回调内重入 dispatch 本 store（避免 store↔闭包引用环）。
    // 用 sender 面（竞争态纯准入，不等完成边界）：`store.dispatch` 在回调内
    // 会等完成边界，而该边界要等外层排空推进 ⇒ 回调内自等 = 死锁（与被测
    // 行为无关，是编排面的坑）。
    let weak = Arc::downgrade(&store);
    let _ticket = store.subscribe_events(64, move |e: &AuditEvent| {
        c.fetch_add(1, Ordering::SeqCst);
        s.lock().expect("seen lock").push(*e);
        if !r.swap(true, Ordering::SeqCst) {
            if let Some(st) = weak.upgrade() {
                assert!(st.sender().dispatch(2), "同 store 重入件已入队");
            }
        }
    });

    store.dispatch(1);

    let seen = seen.lock().expect("seen lock").clone();
    let delivered: Vec<u64> = seen.iter().map(|e| e.seq).collect();
    // 两笔 action = 四个事件（Enqueued×2 + Reduced×2）；回调内重入的那件
    // Enqueued（seq = 2）只录环、不递归进回调 ⇒ 回调只见 [1, 3, 4]。
    assert_eq!(
        delivered,
        vec![1, 3, 4],
        "同 store 重入事件只录环、不递归进回调：{delivered:?}"
    );
    assert!(
        calls.load(Ordering::SeqCst) <= 8,
        "回调次数有界（禁自指递归栈爆）：{}",
        calls.load(Ordering::SeqCst)
    );
    let ring: Vec<u64> = store.take_events().iter().map(|e| e.seq).collect();
    assert_eq!(
        ring,
        vec![1, 2, 3, 4],
        "重入事件照录环（历史完整）：{ring:?}"
    );
}

// ── ⑤ 待投递 FIFO 溢出 ⇒ 独立的投递丢弃计数（与环溢出分账）─────────────

#[test]
fn pending_overflow_counts_delivery_drops_separately_from_ring_overflow() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    // 环容量 8（远小于并发件数 ⇒ 环溢出与投递丢弃两套证据同时出现）。
    let _ticket = store.subscribe_events(
        8,
        gated_callback(
            entered_tx,
            release_rx,
            Arc::clone(&calls),
            Arc::clone(&seen),
        ),
    );

    let st = Arc::clone(&store);
    let thread_a = thread::spawn(move || {
        st.dispatch(1);
    });
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("callback entered (first event in flight)");

    // 并发件数 = 上限 + 76 ⇒ 待投递 FIFO 溢出丢 76 件（丢最旧）。
    let extra = PENDING_CAP + 76;
    let sender = store.sender();
    for action in 2..(2 + extra as u64) {
        assert!(sender.dispatch(action), "并发件已入队");
    }

    release_tx.send(()).expect("release receiver alive");
    thread_a.join().expect("emitter thread joined");

    assert_eq!(
        store.audit_delivery_drops(),
        extra as u64 - PENDING_CAP as u64,
        "投递丢弃计数 = FIFO 溢出件数（与环溢出分账）：{}",
        store.audit_delivery_drops()
    );
    let seen = seen.lock().expect("seen lock").clone();
    assert_eq!(
        enqueued_seqs(&seen).len(),
        1 + PENDING_CAP,
        "实时流只补投 FIFO 容量内的件数（溢出件丢弃）：{}",
        enqueued_seqs(&seen).len()
    );
    // 环溢出独立留证（环容量 8 ⇒ 环内只剩最后几件，历史侧另有一套丢失）。
    let ring = store.take_events();
    assert!(
        ring.len() <= 8,
        "环容量独立于待投递 FIFO（两套上限不混账）：{}",
        ring.len()
    );
    assert!(
        ring.first().is_some_and(|e| e.seq > 1),
        "环溢出独立留证（seq 空洞 = 历史侧丢失）：{ring:?}"
    );
}
