//! #137 参考实现面：订阅回调内注销自己（Drop 自己的票据）不得自等死锁，且此后
//! 不再触达——「Drop 不等待在途回调」契约的端到端证据。
//!
//! 判定用握手 + 有界超时（禁 sleep 猜时序）：若实现改成「注销时等在途回调结束」，
//! 本测试在超时内显式失败而不是把 CI 挂死。

#![cfg(feature = "runtime")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _, Subscription};
use skeleton_infra::runtime::SyncStore;

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

#[test]
fn subscriber_dropping_its_own_ticket_inside_its_callback_does_not_self_wait() {
    let store = SyncStore::new(Counter, NoFx);
    let slot: Arc<Mutex<Option<Subscription<u64>>>> = Arc::new(Mutex::new(None));
    let notified = Arc::new(AtomicUsize::new(0));

    let slot_in_cb = Arc::clone(&slot);
    let notified_in_cb = Arc::clone(&notified);
    let ticket = store.subscribe(move |_state| {
        notified_in_cb.fetch_add(1, Ordering::SeqCst);
        // 通知栈内注销自己：注销=失活+摘表，不等待本回调返回。
        if let Some(own) = slot_in_cb.lock().expect("slot mutex").take() {
            drop(own);
        }
    });
    *slot.lock().expect("slot mutex") = Some(ticket);

    let (done_tx, done_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        store.dispatch(1); // 第一次通知：回调内自注销
        store.dispatch(2); // 第二次通知：不得再触达
        let _ = done_tx.send(());
    });

    done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("自注销在通知栈内挂死（Drop 等待了在途回调 = 自等）");
    worker.join().expect("dispatch 线程 panic");

    assert_eq!(
        notified.load(Ordering::SeqCst),
        1,
        "自注销后不得再收到通知（in-flight 语义：本次通知照常完成，之后静默）"
    );
}
