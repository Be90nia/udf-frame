//! 停机等待面（issue#64）：`close_wait` / `close_wait_timeout` 的核心谓词。
//!
//! 全部用握手（channel）驱动，禁 sleep——阻塞中/已返回的判定都靠有界超时。

#![cfg(feature = "runtime")]

use std::sync::{Arc, Mutex, Weak, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, CloseWaitOutcome, SyncStore};

type GatedStore = Arc<SyncStore<u64, u64, Infallible>>;
/// reducer 内的自我句柄槽（构造期注入：SyncStore 需要先存在才能被 reducer 拿到）。
type StoreSlot = Arc<Mutex<Option<Weak<SyncStore<u64, u64, Infallible>>>>>;

/// action 1 进临界区即发「已进场」并阻塞到 release；其余动作直通。
struct Gated {
    entered: Arc<Mutex<mpsc::Sender<()>>>,
    release: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl Reducer for Gated {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        if a == 1 {
            self.entered.lock().unwrap().send(()).unwrap();
            self.release.lock().unwrap().recv().unwrap();
        }
        s + a
    }
}

fn gated_store() -> (GatedStore, mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let store = SyncStore::new(
        Gated {
            entered: Arc::new(Mutex::new(entered_tx)),
            release: Arc::new(Mutex::new(release_rx)),
        },
        NoFx,
    );
    (Arc::new(store), entered_rx, release_tx)
}

/// ① 静默态 + 竞争态：close_wait 必须阻塞到在跑的 gated reducer 退出为止。
#[test]
fn close_wait_blocks_until_gated_reducer_exits() {
    let (store, entered, release) = gated_store();
    let worker = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.dispatch(1))
    };
    entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");

    let (done_tx, done_rx) = mpsc::channel();
    let waiter = {
        let store = Arc::clone(&store);
        thread::spawn(move || done_tx.send(store.close_wait()).unwrap())
    };

    // reducer 未退 ⇒ 不得报 Drained（这是本条测试的鉴别力点）。
    assert!(
        done_rx.recv_timeout(Duration::from_millis(150)).is_err(),
        "close_wait 在 reducer 仍在跑时提前返回"
    );

    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("close_wait 未返回"),
        CloseWaitOutcome::Drained
    );
    waiter.join().unwrap();
    assert_eq!(*store.snapshot(), 1);
}

/// ② timeout 只界调用方等待：返回 TimedOutStillRunning 且 reducer 未被取消。
#[test]
fn close_wait_timeout_reports_still_running_without_cancelling_reducer() {
    let (store, entered, release) = gated_store();
    let worker = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.dispatch(1))
    };
    entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");

    assert_eq!(
        store.close_wait_timeout(Duration::from_millis(120)),
        CloseWaitOutcome::TimedOutStillRunning
    );

    // 未取消：放行后 reducer 照常提交，随后静止可再观测。
    release.send(()).unwrap();
    worker.join().unwrap();
    assert_eq!(*store.snapshot(), 1, "reducer 被取消或未提交");
    assert_eq!(store.close_wait(), CloseWaitOutcome::Drained);
}

/// ③ 同栈重入：本 store 的 reducer 帧内调用不得自等。
struct SelfWaiter {
    store: StoreSlot,
    seen: Arc<Mutex<Option<CloseWaitOutcome>>>,
}

impl Reducer for SelfWaiter {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        if a == 1 {
            let store = self
                .store
                .lock()
                .unwrap()
                .as_ref()
                .and_then(Weak::upgrade)
                .expect("store 未注入");
            *self.seen.lock().unwrap() = Some(store.close_wait());
        }
        s + a
    }
}

#[test]
fn close_wait_from_own_drain_frame_is_reentrant() {
    let slot: StoreSlot = Arc::new(Mutex::new(None));
    let seen = Arc::new(Mutex::new(None));
    let store = Arc::new(SyncStore::new(
        SelfWaiter {
            store: Arc::clone(&slot),
            seen: Arc::clone(&seen),
        },
        NoFx,
    ));
    *slot.lock().unwrap() = Some(Arc::downgrade(&store));

    store.dispatch(1); // 若自等，本条测试挂死
    assert_eq!(*seen.lock().unwrap(), Some(CloseWaitOutcome::Reentrant));
    assert_eq!(*store.snapshot(), 1);

    // 闭门先于重入判定（#64 06:59 定稿）：拿到 `Reentrant` 时准入**已关**——
    // 重入只跳过自等，不跳过闭门。
    assert_eq!(
        store.try_dispatch(2),
        AdmissionOutcome::Closed,
        "Reentrant 的语义是「准入已关、但不能自等」"
    );
}

/// ④ panic unwind 臂是租约释放的第二点：不 notify 会让等待面永久悬挂。
struct Panicky;

impl Reducer for Panicky {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        assert_ne!(a, 2, "reducer 主动 panic（模拟排空环内展开）");
        s + a
    }
}

#[test]
fn close_wait_after_reducer_panic_is_drained() {
    let store = SyncStore::new(Panicky, NoFx);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.dispatch(2)));
    assert!(caught.is_err(), "reducer panic 应原样传播");

    // 用有界等待而非无界：实现若漏了 unwind 臂的 notify，这里 5s 内失败而不是挂死 CI。
    assert_eq!(
        store.close_wait_timeout(Duration::from_secs(5)),
        CloseWaitOutcome::Drained
    );
}
