#![cfg(feature = "runtime")]
//! issue#73 / issue#74：票据注销后不得再收到投递/回调（注销线性化边界）。
//!
//! 两票的复现窗口都用「握手」精确构造，零 sleep：
//!
//! - #73（异步投递口）：`notify_async` 先把在册投递口收集进复用缓冲，再逐口
//!   `publish`。第一个投递口的 `publish` 内阻塞（宿主代码），此刻第二个投递口
//!   的票据被注销——收集窗已过、投递窗未到。修复后第二个投递口不得被调用。
//! - #74（跨线程同步订阅）：排空环先刷新订阅快照缓存（持强 `Arc`），再逐槽
//!   调回调。第一个订阅者的回调内阻塞，此刻第二个订阅者的票据被注销——快照
//!   已捕获、回调未到。修复后第二个订阅者不得被回调。
//!
//! 断言均在「能失败的输入」上：注销方在另一线程先落地（握手同步），被观察方
//! 的计数必须为 0（修复前为 1）。

use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

use skeleton::notify::{Notify, NotifySink};
use skeleton::{Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;

// ── 固定装置：计数器 reducer + 空解释器 ─────────────────────────────────────

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
    fn run(&mut self, _: Effect<u64, Infallible>, _: &skeleton::ActionSender<u64>) {}
}

// ── 投递口 / 订阅者 ──────────────────────────────────────────────────────────

/// 阻塞投递口：`publish` 内握手放行——制造「收集已过、投递未到」的窗口。
struct BlockingSink {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    calls: AtomicU64,
}

impl NotifySink for BlockingSink {
    fn publish(&self, _version: u64) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered.send(());
        let _ = self.release.lock().expect("release 通道未毒").recv();
    }
}

/// 计数投递口：只记投递次数。
struct CountingSink {
    calls: Arc<AtomicU64>,
}

impl NotifySink for CountingSink {
    fn publish(&self, _version: u64) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// 毒投递口：每次 publish 先计数再 panic（fail-closed 摘除对象）。
struct PoisonSink {
    calls: Arc<AtomicU64>,
}

impl NotifySink for PoisonSink {
    fn publish(&self, _version: u64) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        panic!("boom: poison sink");
    }
}

// ── issue#73：投递口票据注销后不得再收到 publish ────────────────────────────

/// #73 验收：注销（票据 drop / panic 摘除）后不再收到任何投递。
///
/// 修复前：`notify_async` 收集窗内把 weak 升级成强引用存进复用缓冲，票据 drop
/// 只把表项摘掉、闸计数回落——缓冲里的强引用仍在，投递窗照旧 `publish`。
#[test]
fn notify_sink_ticket_dropped_before_publish_is_not_published() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let blocking = BlockingSink {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        calls: AtomicU64::new(0),
    };
    let counted = Arc::new(AtomicU64::new(0));
    let counting = CountingSink {
        calls: Arc::clone(&counted),
    };

    // 登记序 = 投递序：阻塞口在前，计数口在后。
    let _blocking_ticket = store.subscribe_notify(blocking);
    let counting_ticket = store.subscribe_notify(counting);

    let dispatch_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.dispatch(1);
        })
    };

    // 第一个投递口已进入 publish ⇒ 收集窗已过（第二个口已在缓冲里）。
    entered_rx.recv().expect("阻塞投递口未进入 publish");
    // 此刻注销第二个投递口：表项摘除 + 闸回落，但快照缓冲仍持强引用。
    drop(counting_ticket);
    release_tx.send(()).expect("放行失败");
    dispatch_thread.join().expect("dispatch 线程 panic");

    assert_eq!(
        counted.load(Ordering::SeqCst),
        0,
        "票据注销后不得再收到 publish（issue#73）"
    );
}

// ── issue#74：订阅者票据注销后不得再收到回调 ────────────────────────────────

/// #74 验收：注销后本次 dispatch 的缓存快照不得再回调该订阅者。
///
/// 修复前：排空环刷新订阅快照缓存（持强 `Arc<SubscriptionSlot>`），票据 drop
/// 只把 weak 表项摘掉 + 版本自增——本次 dispatch 的快照仍持强引用，回调照旧。
#[test]
fn sync_subscriber_ticket_dropped_before_callback_is_not_invoked() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let counted = Arc::new(AtomicU64::new(0));

    // 登记序 = 回调序：阻塞订阅者在前，计数订阅者在后。
    let _blocking_ticket = store.subscribe({
        let entered_tx = entered_tx.clone();
        let release = Mutex::new(release_rx);
        move |_: &u64| {
            let _ = entered_tx.send(());
            let _ = release.lock().expect("release 通道未毒").recv();
        }
    });
    let counted_ticket = store.subscribe({
        let counted = Arc::clone(&counted);
        move |_: &u64| {
            counted.fetch_add(1, Ordering::SeqCst);
        }
    });

    let dispatch_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.dispatch(1);
        })
    };

    // 第一个订阅者的回调已进入 ⇒ 快照缓存已捕获两个订阅者。
    entered_rx.recv().expect("阻塞订阅者回调未进入");
    // 此刻注销第二个订阅者：表项摘除 + 版本自增，但快照缓存仍持强引用。
    drop(counted_ticket);
    release_tx.send(()).expect("放行失败");
    dispatch_thread.join().expect("dispatch 线程 panic");

    assert_eq!(
        counted.load(Ordering::SeqCst),
        0,
        "票据注销后不得再收到订阅回调（issue#74）"
    );
}

// ── issue#73：publish 阻塞中并发 Drop（in-flight 语义 + 不死锁）──────────────

/// #73 验收：publish 在途时并发 Drop 票据——通过最后一道 alive 检查的 publish
/// 视为 in-flight，**允许在票据 Drop 返回后完成**（窄契约：不承诺 quiescent
/// 退订）；但不得死锁，且此后不再开始新投递。
#[test]
fn notify_sink_in_flight_publish_completes_after_ticket_drop() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let ticket = store.subscribe_notify(BlockingSink {
        entered: entered_tx,
        release: Mutex::new(release_rx),
        calls: AtomicU64::new(0),
    });

    let dispatch_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.dispatch(1);
        })
    };

    // publish 已在途（宿主代码执行中）⇒ 此时 Drop 票据。
    entered_rx.recv().expect("阻塞投递口未进入 publish");
    drop(ticket);
    // in-flight publish 允许完成 ⇒ 放行后 dispatch 必返回（不死锁）。
    release_tx.send(()).expect("放行失败");
    dispatch_thread
        .join()
        .expect("dispatch 线程 panic（in-flight publish 完成后应正常收尾）");

    // 此后不得再开始新投递（票据已注销 ⇒ 闸归零 ⇒ 空窗拍零投递）。
    // 计数口径：in-flight 那次 publish 正常返回（窄契约允许完成）⇒ 计入成功投递；
    // 第二次 dispatch 不得再产生新投递 ⇒ 计数不再增长。
    let published_before = store.notify_published();
    store.dispatch(2);
    assert_eq!(
        store.notify_published(),
        published_before,
        "票据注销后不得再开始新投递（in-flight 那次允许完成并计入）"
    );
}

// ── issue#73：sink panic 摘除与票据 Drop 竞争（闸计数恰好一次递减）──────────

/// #73 验收：毒投递口 panic 摘除与票据 Drop 竞争——闸计数恰好一次递减
/// （`alive.swap` 单点裁决），此后不再 publish，且不下溢（闸不再前进）。
#[test]
fn notify_sink_panic_removal_racing_ticket_drop_removes_exactly_once() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let calls = Arc::new(AtomicU64::new(0));
    let healthy = Arc::new(AtomicU64::new(0));

    let ticket = store.subscribe_notify(PoisonSink {
        calls: Arc::clone(&calls),
    });
    let _healthy_ticket = store.subscribe_notify(CountingSink {
        calls: Arc::clone(&healthy),
    });

    // 毒口 panic ⇒ fail-closed 摘除（闸回落一次）；票据 Drop 并发抵达 ⇒
    // `alive.swap(false)` 单点裁决：第二个到达者不得再回落闸（不下溢）。
    store.dispatch(1);
    drop(ticket);
    store.dispatch(2);

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "毒口只被调用一次即摘除（panic 后不再投递）"
    );
    assert_eq!(
        store.subscriber_panics(),
        1,
        "panic 摘除恰好一次（票 Drop 与 panic 竞争不重复计数）"
    );
    assert_eq!(
        healthy.load(Ordering::SeqCst),
        2,
        "健康口逐拍照常（摘除只影响毒口）"
    );
    assert_eq!(
        store.notify_published(),
        2,
        "panic 的投递不计入成本读侧（健康口两拍）"
    );
}

// ── issue#74：回调执行中 Drop（in-flight 语义 + 不死锁）─────────────────────

/// #74 验收：回调在途时并发 Drop 票据——已开始的回调允许跑完（in-flight 语义），
/// 但不得死锁；此后不得再开始新回调。
#[test]
fn sync_subscriber_in_flight_callback_completes_after_ticket_drop() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();

    let ticket = store.subscribe({
        let entered_tx = entered_tx.clone();
        let release = Mutex::new(release_rx);
        move |_: &u64| {
            let _ = entered_tx.send(());
            let _ = release.lock().expect("release 通道未毒").recv();
        }
    });

    let dispatch_thread = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            store.dispatch(1);
        })
    };

    // 回调已在途 ⇒ 此时 Drop 票据。
    entered_rx.recv().expect("阻塞订阅者回调未进入");
    drop(ticket);
    // in-flight 回调允许完成 ⇒ 放行后 dispatch 必返回（不死锁）。
    release_tx.send(()).expect("放行失败");
    dispatch_thread
        .join()
        .expect("dispatch 线程 panic（in-flight 回调完成后应正常收尾）");

    // 此后不得再开始新回调（票据已注销）。
    store.dispatch(2);
    assert_eq!(store.subscriber_count(), 0, "票据注销后订阅表无在册订阅者");
}
