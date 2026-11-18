#![cfg(feature = "runtime")]
//! 有界准入（GitHub issue#38）：typed 准入结果（Accepted / Full / Closed）、
//! 队列容量上限（只约束外部准入）、close 与准入的线性化、重入在容量满时
//! 不自死锁、过载显式化（禁静默弃 action）。

use skeleton::{EffectBuf, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, SyncStore};
use std::convert::Infallible;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, LazyLock, Mutex, Weak};
use std::thread;
use std::time::Duration;

/// 两段闸：reducer 进入点 + 放行点（确定性并发编排）。
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
    /// 在 reducer 内阻塞（占住排空权）。
    Block,
    /// 普通动作。
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

/// 有界等待队列深度达 `want`（编排用；超时即失败，禁无限等）。
fn wait_pending(store: &SyncStore<u32, Act, Infallible>, want: usize) {
    for _ in 0..5000 {
        if store.pending_actions() >= want {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("队列深度未达期望值 {want}");
}

/// 容量满 = typed reject（`Full`）：不部分插入、不阻塞、计数留痕。
#[test]
fn try_dispatch_reports_full_without_partial_insert() {
    let gate = Gate::new();
    let store = Arc::new(SyncStore::new(
        Gated {
            gate: Arc::clone(&gate),
        },
        NoFx,
    ));
    store.set_capacity(2);

    // 占住排空权。
    let store_h = Arc::clone(&store);
    let handle_h = thread::spawn(move || store_h.dispatch(Act::Block));
    gate.wait_entered();

    // 两条外部准入入队（容量 2）。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(Act::Inc));
    let store_b = Arc::clone(&store);
    let handle_b = thread::spawn(move || store_b.dispatch(Act::Inc));
    wait_pending(&store, 2);

    // 第三条：typed reject（不阻塞——阻塞会与回调重入死锁）。
    assert_eq!(store.try_dispatch(Act::Inc), AdmissionOutcome::Full);
    assert_eq!(store.pending_actions(), 2, "队列不得超过容量上限");
    assert_eq!(store.admission_full_count(), 1, "过载必须留痕");

    gate.open();
    handle_h.join().unwrap();
    handle_a.join().unwrap();
    handle_b.join().unwrap();
    assert_eq!(*store.snapshot(), 3, "已准入的 action 必须照常被 reduce");
}

/// 闭门后准入 = typed reject（`Closed`），且不部分插入。
#[test]
fn try_dispatch_reports_closed_after_close() {
    let store = SyncStore::new(Gated { gate: Gate::new() }, NoFx);
    store.close();
    assert_eq!(store.try_dispatch(Act::Inc), AdmissionOutcome::Closed);
    assert_eq!(store.pending_actions(), 0, "闭门后准入不得入队");
}

/// legacy `dispatch` 无失败面 ⇒ 过载显式失败（禁静默弃 action）。
#[test]
fn legacy_dispatch_overload_fails_visibly() {
    let gate = Gate::new();
    let store = Arc::new(SyncStore::new(
        Gated {
            gate: Arc::clone(&gate),
        },
        NoFx,
    ));
    store.set_capacity(1);

    let store_h = Arc::clone(&store);
    let handle_h = thread::spawn(move || store_h.dispatch(Act::Block));
    gate.wait_entered();

    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(Act::Inc));
    wait_pending(&store, 1);

    let result = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Inc)));
    assert!(
        result.is_err(),
        "容量满时 legacy dispatch 必须显式失败（不得静默丢弃 action）"
    );
    assert_eq!(store.admission_full_count(), 1, "过载必须留痕");
    assert_eq!(store.pending_actions(), 1, "被拒 action 不得部分插入");

    gate.open();
    handle_h.join().unwrap();
    handle_a.join().unwrap();
}

// ── 重入在容量满时不自死锁 ──────────────────────────────────────────────

/// 重入 reducer：action 0 等闸后重入 dispatch（同栈 → 仅入队不等待）。
struct Reenter {
    gate: Arc<Gate>,
}

type ReenterSlot = LazyLock<Mutex<Option<Weak<SyncStore<u32, u32, Infallible>>>>>;
static REENTER_SLOT: ReenterSlot = LazyLock::new(|| Mutex::new(None));

impl Reducer for Reenter {
    type State = u32;
    type Action = u32;
    type Op = Infallible;

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
        if a == 0 {
            self.gate.mark_entered();
            self.gate.wait_release();
            let slot = REENTER_SLOT.lock().unwrap();
            if let Some(weak) = slot.as_ref() {
                if let Some(store) = weak.upgrade() {
                    // 同栈重入：队列此刻已满（容量 1），但回灌不受容量约束
                    // ——挡住它 = 排空环自锁。
                    store.dispatch(9);
                }
            }
        }
        s + 1
    }
}

/// 同栈重入不受容量约束：容量满时重入 dispatch 不自死锁（且不被拒）。
#[test]
fn reentrant_dispatch_at_capacity_does_not_deadlock() {
    let gate = Gate::new();
    let store = Arc::new(SyncStore::new(
        Reenter {
            gate: Arc::clone(&gate),
        },
        NoFx,
    ));
    REENTER_SLOT.lock().unwrap().replace(Arc::downgrade(&store));
    store.set_capacity(1);

    // 占住排空权。
    let store_h = Arc::clone(&store);
    let handle_h = thread::spawn(move || store_h.dispatch(0));
    gate.wait_entered();

    // 一条外部准入把队列填满（容量 1）。
    let store_a = Arc::clone(&store);
    let handle_a = thread::spawn(move || store_a.dispatch(1));
    wait_pending_u32(&store, 1);

    // 放行 → 重入 dispatch 在「队列已满」的时刻发生。
    gate.open();

    handle_h.join().unwrap();
    handle_a.join().unwrap();
    assert_eq!(
        *store.snapshot(),
        3,
        "重入 action 必须被排空环消费（容量满不得把它挡掉）"
    );
}

/// 有界等待队列深度达 `want`（u32 action 版）。
fn wait_pending_u32(store: &SyncStore<u32, u32, Infallible>, want: usize) {
    for _ in 0..5000 {
        if store.pending_actions() >= want {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("队列深度未达期望值 {want}");
}
