#![cfg(feature = "runtime")]
//! 投递面契约回归锚（issue#145 / #161 / #162）：
//!
//! - **#145**：审计 `Notified` 只在真有订阅回调被调用时记录——「在册 / 闸 >0」
//!   不等于「被调用」（本地订阅在他线程 dispatch 查不到；快照后才注销的
//!   跨线程槽位不被调用）。
//! - **#161**：退役的异步投递口槽位在每轮投递末尾释放——票据 Drop 后再无
//!   dispatch 也不得滞留 sink 的捕获图。
//! - **#162**：本地订阅 panic 摘除后闸计数回落——不再虚高、不再误记跨线程
//!   miss，票据 Drop 也不会二次回落。

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use skeleton::notify::{Notify, NotifySink};
use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _, Subscription,
};
use skeleton_infra::audit::AuditEventKind;
use skeleton_infra::runtime::SyncStore as Store;

// ── 固定装置 ───────────────────────────────────────────────────────────────

/// 只自增的计数器（无效果 ⇒ 每拍事件面只有 `Enqueued`/`Reduced`[+`Notified`]）。
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

/// 计数审计环里的 `Notified` 出现次数（取证走环，与实时回调同源）。
fn notified_count<S, A, Op>(store: &Store<S, A, Op>) -> usize
where
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
{
    store
        .take_events()
        .iter()
        .filter(|e| e.kind == AuditEventKind::Notified)
        .count()
}

/// 捕获图探针：析构即计数（投递口槽位退役的观察面）。
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}

/// 静默投递口（持有探针；投递本体与断言无关）。
struct ProbeSink {
    probe: DropProbe,
}

impl NotifySink for ProbeSink {
    fn publish(&self, _version: u64) {
        let _ = &self.probe;
    }
}

/// 投递即 panic 的投递口（fail-closed 摘除对象），同样持有探针。
struct PoisonSink {
    probe: DropProbe,
}

impl NotifySink for PoisonSink {
    fn publish(&self, _version: u64) {
        let _ = &self.probe;
        panic!("boom: poison sink");
    }
}

// ── issue#145：Notified 只认「回调被调用」──────────────────────────────────

/// 假阳性路径①：本地订阅登记在注册线程的 TLS 表里，他线程 dispatch 查不到它
/// （`matched == 0`，已计入跨线程 miss），而 store 闸 >0。修复前据此记
/// `Notified`，把「没投出去」写成投递成功。
#[test]
fn local_subscription_on_other_thread_emits_no_notified() {
    let store = Arc::new(Store::new(Counter, NoFx));
    let _local = store.subscribe_local(|_| {});
    let _audit = store.subscribe_events(64, |_| {});
    let worker = Arc::clone(&store);
    std::thread::spawn(move || worker.dispatch(1))
        .join()
        .expect("dispatch 线程不得 panic");
    assert_eq!(
        store.local_missed_notifications(),
        1,
        "跨线程 miss 照常留痕（诊断不因本修复消失）"
    );
    assert_eq!(
        notified_count(&store),
        0,
        "本地回调未被调用 ⇒ 不得记 Notified（issue#145）"
    );
}

/// 每个 action 产一条 `Run` 效果（把注销点放进「通知段之前」）。
struct EmitRun;

impl Reducer for EmitRun {
    type State = u64;
    type Action = u64;
    type Op = ();
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, a: u64, out: &mut EffectBuf<u64, ()>) -> u64 {
        out.push(Effect::Run {
            id: EffectId::new(1),
            op: (),
        });
        s + a
    }
}

/// 转发期注销订阅的解释器（确定性入口：效果转发在通知段之前，缓存未及冷重建）。
struct DropTicketFx(Arc<Mutex<Option<Subscription<u64>>>>);

impl EffectRuntime<u64, ()> for DropTicketFx {
    fn run(&mut self, _: Effect<u64, ()>, _: &ActionSender<u64>) {
        drop(self.0.lock().expect("ticket slot").take());
    }
}

/// 假阳性路径②：跨线程槽位在通知窗内已失活（同一 dispatch 内注销，缓存仍
/// 持有强引用）——本拍所有槽位都被跳过，没有任何回调被调用。修复前
/// `notified_next.is_some()` 仍把本拍记成投递成功。
#[test]
fn unsubscribed_before_notify_pass_emits_no_notified() {
    let slot: Arc<Mutex<Option<Subscription<u64>>>> = Arc::new(Mutex::new(None));
    let store = Store::new(EmitRun, DropTicketFx(Arc::clone(&slot)));
    *slot.lock().expect("ticket slot") = Some(store.subscribe(|_| {}));
    let _audit = store.subscribe_events(64, |_| {});
    store.dispatch(1);
    assert_eq!(store.subscriber_count(), 0, "票据已在效果转发期注销");
    assert_eq!(
        notified_count(&store),
        0,
        "通知窗内已失活 ⇒ 无回调被调用 ⇒ 不得记 Notified（issue#145）"
    );
}

// ── issue#161：退役投递口槽位的强引用随投递轮释放 ─────────────────────────

/// 修复前：复用缓冲在投递末尾不清，票据 Drop 后它仍是最后一个强持有者——
/// 长闲 store 把 sink 捕获图滞留到下一次 dispatch。本测试刻意**不再 dispatch**。
#[test]
fn retired_notify_sink_releases_capture_without_further_dispatch() {
    let drops = Arc::new(AtomicUsize::new(0));
    let store = Store::new(Counter, NoFx);
    let ticket = store.subscribe_notify(ProbeSink {
        probe: DropProbe(Arc::clone(&drops)),
    });
    store.dispatch(1);
    assert_eq!(drops.load(Ordering::Acquire), 0, "在册期间捕获图存活");
    drop(ticket);
    assert_eq!(
        drops.load(Ordering::Acquire),
        1,
        "退订后必须立即释放（不得等下一次 dispatch，issue#161）"
    );
}

/// panic 摘除的槽位：表里已摘、票据仍在 ⇒ 捕获图随票据 Drop 释放，缓冲不得
/// 留最后一个强引用。
#[test]
fn panic_removed_notify_sink_releases_capture_on_ticket_drop() {
    let drops = Arc::new(AtomicUsize::new(0));
    let store = Store::new(Counter, NoFx);
    let ticket = store.subscribe_notify(PoisonSink {
        probe: DropProbe(Arc::clone(&drops)),
    });
    store.dispatch(1); // 投递口 panic ⇒ fail-closed 摘除
    assert_eq!(store.subscriber_panics(), 1, "毒投递口摘除留痕");
    assert_eq!(drops.load(Ordering::Acquire), 0, "票据仍在 ⇒ 槽位仍活");
    drop(ticket);
    assert_eq!(
        drops.load(Ordering::Acquire),
        1,
        "摘除后票据 Drop 即释放槽位（无需再 dispatch，issue#161）"
    );
}

// ── issue#162：本地闸计数在 panic 摘除后保持一致 ───────────────────────────

/// 修复前：本地回调 panic 被摘除，但闸计数不减——后续每拍都进入本地路径、
/// 查无登记、误记「跨线程 miss」（真实原因是回调已被摘除）。票据 Drop 的
/// 无条件 `fetch_sub` 还会二次回落（下溢 = 门控恒真）。
#[test]
fn local_panic_removal_stops_reporting_cross_thread_misses() {
    let store = Store::new(Counter, NoFx);
    let ticket = store.subscribe_local(|_| panic!("boom: local sub"));
    store.dispatch(1); // 同线程：回调被调用后 panic ⇒ fail-closed 摘除
    assert_eq!(store.subscriber_panics(), 1, "毒本地回调摘除留痕");
    assert_eq!(
        store.local_missed_notifications(),
        0,
        "摘除与 miss 是两回事：摘除不计 miss"
    );
    store.dispatch(1);
    assert_eq!(
        store.local_missed_notifications(),
        0,
        "闸已回落 ⇒ 不再进入本地路径、不误记跨线程 miss（issue#162）"
    );
    drop(ticket); // 已被摘除的票据：不得二次回落
    store.dispatch(1);
    assert_eq!(
        store.local_missed_notifications(),
        0,
        "票据 Drop 后闸仍为 0（下溢会让门控恒真、再次误记 miss）"
    );
    assert_eq!(*store.snapshot(), 3, "三笔 dispatch 照常推进状态");
}
