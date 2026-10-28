#![cfg(feature = "runtime")]
//! ADR 0008 §5.3 六项断言的机械化：异步（信号式）通知档行为面。
//! 每测守一条可观察契约；投递口用共享原子/通道收集，零锁零毒化面。

use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

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

/// 收集型投递口：记录收到的版本号序列（测试观察面）。
struct RecordingSink(Arc<Mutex<Vec<u64>>>);

impl NotifySink for RecordingSink {
    fn publish(&self, version: u64) {
        self.0.lock().unwrap().push(version);
    }
}

/// 毒投递口：每次 publish 先计数再 panic（fail-closed 摘除对象）。
struct PoisonSink {
    calls: Arc<AtomicU64>,
}

impl NotifySink for PoisonSink {
    fn publish(&self, _version: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        panic!("boom: poison sink");
    }
}

/// 通道投递口：publish 只做 O(1) 入队，重活在宿主消费线程——契约义务的
/// 示范形态（无界通道 send 断连仅发生在接收端消亡后，宿主停机自查 is_closed）。
struct ChannelSink(mpsc::Sender<u64>);

impl NotifySink for ChannelSink {
    fn publish(&self, version: u64) {
        let _ = self.0.send(version); // 断连 = 接收端已消亡，Q3：不另加信号
    }
}

/// 投递序观察口：与同步订阅回调共用序号器，验证异步先于同步（决策 4）。
struct OrderedSink {
    order: Arc<Mutex<Vec<String>>>,
    seq: Arc<AtomicUsize>,
}

impl NotifySink for OrderedSink {
    fn publish(&self, _version: u64) {
        self.order.lock().unwrap().push(format!(
            "async#{}",
            self.seq.fetch_add(1, Ordering::Relaxed)
        ));
    }
}

// ── §5.3 ①：版本号逐拍单调（在册期间逐拍对应）──────────────────────────────

#[test]
fn versions_monotonic_per_beat() {
    let store = Store::new(Counter, NoFx);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _t = store.subscribe_notify(RecordingSink(Arc::clone(&seen)));
    for i in 1..=5 {
        store.dispatch(i);
    }
    let got = seen.lock().unwrap().clone();
    assert_eq!(
        got,
        vec![1, 2, 3, 4, 5],
        "在册期间逐拍对应：每拍恰一投递、版本 +1"
    );
    assert!(
        got.windows(2).all(|w| w[0] < w[1]),
        "版本严格单调（M5 水位：跳跃/回退即可检出异常）"
    );
}

// ── §5.3 ②：投递次数与订阅数一致 ───────────────────────────────────────────

#[test]
fn published_count_matches_subscription_count() {
    let store = Store::new(Counter, NoFx);
    let a = Arc::new(Mutex::new(Vec::new()));
    let b = Arc::new(Mutex::new(Vec::new()));
    let _ta = store.subscribe_notify(RecordingSink(Arc::clone(&a)));
    let _tb = store.subscribe_notify(RecordingSink(Arc::clone(&b)));
    for i in 1..=3 {
        store.dispatch(i);
    }
    assert_eq!(*a.lock().unwrap(), vec![1, 2, 3]);
    assert_eq!(
        *b.lock().unwrap(),
        vec![1, 2, 3],
        "同拍同版本，两投递口各收全 3 拍"
    );
    assert_eq!(
        store.notify_published(),
        6,
        "累计投递 = 订阅数 × 拍数（成本可见读侧）"
    );
}

// ── §5.3 ③：投递口 panic ⇒ 摘除 + 计数 + 成因（fail-closed）────────────────

#[test]
fn sink_panic_removed_with_count_and_cause() {
    let store = Store::new(Counter, NoFx);
    let calls = Arc::new(AtomicU64::new(0));
    let healthy = Arc::new(Mutex::new(Vec::new()));
    let _tp = store.subscribe_notify(PoisonSink {
        calls: Arc::clone(&calls),
    });
    let _th = store.subscribe_notify(RecordingSink(Arc::clone(&healthy)));
    store.dispatch(1);
    store.dispatch(2);
    assert_eq!(calls.load(Ordering::Relaxed), 1, "毒口只被调用一次即摘除");
    assert_eq!(
        store.subscriber_panics(),
        1,
        "panic 计数留痕（同订阅回调纪律）"
    );
    assert_eq!(
        store.subscriber_panic_last_cause().as_deref(),
        Some("boom: poison sink"),
        "成因摘要留痕（回答「为什么」）"
    );
    assert_eq!(
        *healthy.lock().unwrap(),
        vec![1, 2],
        "健康口逐拍照常（同拍同版本）"
    );
    assert_eq!(store.notify_published(), 2, "panic 的投递不计入成本读侧");
}

// ── §5.3 ④：闭门后登记计数（登记照常、永不发信号）──────────────────────────

#[test]
fn register_after_close_counted_and_silent() {
    let store = Store::new(Counter, NoFx);
    store.close();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let _t = store.subscribe_notify(RecordingSink(Arc::clone(&seen)));
    assert_eq!(store.subscriptions_after_close(), 1, "闭门登记显形");
    store.dispatch(1); // 闭门后恒 no-op
    assert!(seen.lock().unwrap().is_empty(), "闭门后永不发信号");
    assert_eq!(store.notify_published(), 0);
}

// ── §5.3 ⑤：票 Drop ⇒ 不再投递；门控：无人订阅版本冻结、不回退 ─────────────

#[test]
fn ticket_drop_stops_delivery_and_version_freezes() {
    let store = Store::new(Counter, NoFx);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let ticket = store.subscribe_notify(RecordingSink(Arc::clone(&seen)));
    store.dispatch(1);
    assert_eq!(*seen.lock().unwrap(), vec![1]);
    drop(ticket);
    store.dispatch(2);
    store.dispatch(3);
    assert_eq!(*seen.lock().unwrap(), vec![1], "票据 Drop 即注销，不再投递");
    assert_eq!(store.notify_published(), 1, "空窗拍零投递（门控快路径）");
    // 门控语义（ADR §5.2）：无人订阅期间版本冻结；重开后从 1 → 2 继续
    // （不回退、不补涨空窗拍）。
    let seen2 = Arc::new(Mutex::new(Vec::new()));
    let _t2 = store.subscribe_notify(RecordingSink(Arc::clone(&seen2)));
    store.dispatch(4);
    assert_eq!(
        *seen2.lock().unwrap(),
        vec![2],
        "版本续走下一位：冻结不回退、空窗不补涨"
    );
}

// ── §4 决策 4：异步投递排在同步回调之前 ─────────────────────────────────────

#[test]
fn async_delivery_precedes_sync_callbacks() {
    let store = Store::new(Counter, NoFx);
    let order = Arc::new(Mutex::new(Vec::new()));
    let seq = Arc::new(AtomicUsize::new(0));
    let _t = store.subscribe_notify(OrderedSink {
        order: Arc::clone(&order),
        seq: Arc::clone(&seq),
    });
    let cb_order = Arc::clone(&order);
    let cb_seq = Arc::clone(&seq);
    let _sync = store.subscribe(move |_s: &u64| {
        cb_order
            .lock()
            .unwrap()
            .push(format!("sync#{}", cb_seq.fetch_add(1, Ordering::Relaxed)));
    });
    store.dispatch(1);
    assert_eq!(
        *order.lock().unwrap(),
        vec!["async#0", "sync#1"],
        "同一拍内：异步投递先于同步回调（决策 4）"
    );
}

// ── 延迟测量（dispatch_profile.rs 同款口径：batch mean + black_box，三批中位）─

fn median3<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let mut xs = [0.0f64; 3];
    for x in &mut xs {
        let t = Instant::now();
        for _ in 0..n {
            std::hint::black_box(op());
        }
        *x = t.elapsed().as_nanos() as f64 / n as f64;
    }
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[1]
}

// ── §5.3 ⑥：零异步订阅者时 dispatch 延迟不回退（77ns 锚的结构守卫）──────────

#[test]
fn zero_subscriber_dispatch_not_regressed() {
    let store = Store::new(Counter, NoFx);
    for _ in 0..10_000 {
        store.dispatch(1);
    }
    let ns = median3(|| store.dispatch(1), 200_000);
    println!(
        "zero_notify_dispatch={ns:.1}ns（77ns 锚对齐判定以 dispatch_profile 原始输出为准，±10ns）"
    );
    // 宽松结构闸：门控快路径每拍只许一次 Acquire load（~1ns 量级）。绝对锚
    // 判定归 dispatch_profile（CI 噪声下绝对值断言恒抖）；本闸只拦结构性
    // 回退（每拍上锁/建表/触投递口）。
    assert!(ns < 5_000.0, "零异步订阅 dispatch 结构性回退：{ns}ns");
}

// ── §9 第 5 条：50ms 慢消费者挂上后 dispatch 保持 ~100ns 量级 ────────────────

#[test]
fn slow_consumer_does_not_block_dispatch() {
    let store = Store::new(Counter, NoFx);
    let (tx, rx) = mpsc::channel::<u64>();
    let stop = Arc::new(AtomicBool::new(false));
    let worker_stop = Arc::clone(&stop);
    // 消费线程：每条信号后睡 50ms（模拟 IO/渲染重活——重活全在宿主侧）。
    let worker = std::thread::spawn(move || {
        while !worker_stop.load(Ordering::Relaxed) {
            match rx.recv_timeout(Duration::from_millis(5)) {
                Ok(_version) => std::thread::sleep(Duration::from_millis(50)),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });
    let _t = store.subscribe_notify(ChannelSink(tx));
    for _ in 0..10_000 {
        store.dispatch(1);
    }
    let ns = median3(|| store.dispatch(1), 200_000);
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    println!("slow_consumer_dispatch={ns:.1}ns（50ms 消费者在途；§9 第 5 条 ~100ns 量级实证）");
    // publish 仅 O(1) 通道入队：消费端 50ms 重活与 dispatch 完全解耦。50ms
    // 若被传导进排空环，此处读数是 5e7 ns——5µs 闸与之隔 4 个数量级。
    assert!(
        ns < 5_000.0,
        "慢消费者传导进 dispatch：{ns}ns（重活进了排空环）"
    );
}
