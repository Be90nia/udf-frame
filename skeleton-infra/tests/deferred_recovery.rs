#![cfg(feature = "runtime")]
//! 显式有界恢复（GitHub issue#49）：`resume_deferred(limit)` 按 FIFO 取最老的
//! 停放记录、追加在已准入的健康外部工作之后、每条开**新回灌深度 epoch**；
//! 原根回执保持终态不可变，恢复出的工作拿自己的（分离的）续接回执。
//!
//! 并发编排一律确定性握手（闸/屏障），禁 sleep 竞速。

use skeleton::{Effect, EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{DispatchOutcome, SyncStore};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread;

/// 回灌深度预算（`runtime` 内 const 64）+ 本条链长 ⇒ 首次派发恰好停在一条
/// 停放记录上（载荷 hops = 1）。
const CHAIN: u64 = 66;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    /// 无条件自指回灌：每次恢复都会再次停放。
    Loop,
    /// 定长回灌链：hops 递减到 0 才停（恢复后能从 depth 0 顺利跑完）。
    Chain { tag: u64, hops: u64 },
    /// 占住排空权的闸（确定性并发编排）。
    Block,
}

/// 两段闸：reducer 进入点 + 放行点（禁 sleep 竞速）。
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

    fn wait_entered(&self) {
        let mut g = self.entered.0.lock().unwrap();
        while !*g {
            g = self.entered.1.wait(g).unwrap();
        }
    }

    /// 占住排空权：先报「已进场」，再等放行（确定性握手，禁 sleep）。
    fn block(&self) {
        {
            let mut g = self.entered.0.lock().unwrap();
            *g = true;
            self.entered.1.notify_all();
        }
        self.wait_release();
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

/// reduce 序观察（FIFO / 公平性 / 新 epoch 的证据面）。
struct Fixture {
    seen: Arc<Mutex<Vec<Act>>>,
    gate: Option<Arc<Gate>>,
}

impl Fixture {
    fn plain() -> (Self, Arc<Mutex<Vec<Act>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                seen: Arc::clone(&seen),
                gate: None,
            },
            seen,
        )
    }
}

impl Reducer for Fixture {
    type State = u64;
    type Action = Act;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: Act, out: &mut EffectBuf<Act, Infallible>) -> u64 {
        self.seen.lock().unwrap().push(a);
        match a {
            Act::Loop => out.push(Effect::Emit(Act::Loop)),
            Act::Chain { tag, hops } if hops > 0 => out.push(Effect::Emit(Act::Chain {
                tag,
                hops: hops - 1,
            })),
            Act::Chain { .. } => {}
            Act::Block => self
                .gate
                .as_ref()
                .expect("Block 只在带闸的夹具里使用")
                .block(),
        }
        s + 1
    }
}

fn parked(store: &SyncStore<u64, Act>) -> usize {
    store.deferred_actions()
}

/// ① FIFO + limit + remaining：最老的先恢复，`limit` 只放行那么些条。
#[test]
fn resumes_oldest_first_and_honours_limit() {
    let (fixture, seen) = Fixture::plain();
    let store = SyncStore::new(fixture, NoFx);
    for tag in [1u64, 2, 3] {
        let receipt = store
            .dispatch_with_receipt(Act::Chain { tag, hops: CHAIN })
            .expect("准入");
        assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });
    }
    assert_eq!(parked(&store), 3, "前置：三条停放记录");

    seen.lock().unwrap().clear();
    let resumed = store.resume_deferred(2);
    assert_eq!(resumed.resumed, 2, "limit 上界生效");
    assert_eq!(resumed.remaining, 1, "剩下的仍停放");
    assert_eq!(resumed.deferred_again, 0, "恢复的链跑完了（没有再次停放）");
    assert_eq!(resumed.capacity_rejected, 0);

    let order: Vec<u64> = seen
        .lock()
        .unwrap()
        .iter()
        .filter_map(|a| match a {
            Act::Chain { tag, .. } => Some(*tag),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        vec![1, 2, 1, 2],
        "FIFO：最老的两条先被 reduce（各自的回灌子代排在其后，不插队），tag=3 未跑"
    );

    let resumed = store.resume_deferred(10);
    assert_eq!(
        (resumed.resumed, resumed.remaining, resumed.deferred_again),
        (1, 0, 0),
        "剩余一条最终被恢复并跑完"
    );
}

/// ② 新回灌深度 epoch：恢复记录从 depth 0 起重计，否则会立即再次停放。
#[test]
fn resumed_record_starts_a_fresh_depth_epoch() {
    let (fixture, seen) = Fixture::plain();
    let store = SyncStore::new(fixture, NoFx);
    let receipt = store
        .dispatch_with_receipt(Act::Chain {
            tag: 7,
            hops: CHAIN,
        })
        .expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });

    seen.lock().unwrap().clear();
    let resumed = store.resume_deferred(1);
    assert_eq!(
        resumed.deferred_again, 0,
        "新 epoch ⇒ 不再撞预算（沿用旧深度会立刻再次停放）"
    );
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            Act::Chain { tag: 7, hops: 1 },
            Act::Chain { tag: 7, hops: 0 }
        ],
        "恢复记录及其同栈子代都被 reduce（载荷与序号原样保留）"
    );
    assert_eq!(parked(&store), 0, "记录已离开停放队列（population 迁移）");
}

/// ③ 原根回执终态不可变：恢复给的是**分离的**续接回执，不改写原根。
#[test]
fn recovery_never_rewrites_the_original_root_receipt() {
    let (fixture, _seen) = Fixture::plain();
    let store = SyncStore::new(fixture, NoFx);
    let root = store
        .dispatch_with_receipt(Act::Chain {
            tag: 5,
            hops: CHAIN,
        })
        .expect("准入");
    assert_eq!(root.wait(), DispatchOutcome::Deferred { retained: 1 });

    let resumed = store.resume_deferred(1);
    assert_eq!((resumed.resumed, resumed.deferred_again), (1, 0));

    assert_eq!(
        root.wait(),
        DispatchOutcome::Deferred { retained: 1 },
        "恢复后原根回执仍报 Deferred（终局不可变）"
    );
    assert_eq!(root.seq(), 0, "回执身份 = 原根 action 的入队序号");
}

/// ④ 公平性：恢复记录**追加在已准入的健康外部工作之后**（不插队）。
#[test]
fn resumed_work_runs_behind_already_admitted_external_work() {
    let gate = Gate::new();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(SyncStore::new(
        Fixture {
            seen: Arc::clone(&seen),
            gate: Some(Arc::clone(&gate)),
        },
        NoFx,
    ));
    let receipt = store
        .dispatch_with_receipt(Act::Chain {
            tag: 1,
            hops: CHAIN,
        })
        .expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });
    seen.lock().unwrap().clear();

    // 外部根占住排空权，并在其阻塞期间再准入一条健康外部工作。
    let blocked = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.dispatch(Act::Block))
    };
    gate.wait_entered();
    assert_eq!(
        store.try_dispatch(Act::Chain { tag: 2, hops: 0 }),
        skeleton_infra::runtime::AdmissionOutcome::Accepted,
        "在任排空 ⇒ 准入即返（排队在 Block 之后）"
    );

    let resumer = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.resume_deferred(1))
    };
    gate.open();
    blocked.join().expect("阻塞根不得 panic");
    let resumed = resumer.join().expect("恢复线程不得 panic");

    assert_eq!((resumed.resumed, resumed.deferred_again), (1, 0));
    assert_eq!(
        *seen.lock().unwrap(),
        vec![
            Act::Block,
            Act::Chain { tag: 2, hops: 0 },
            Act::Chain { tag: 1, hops: 1 },
            Act::Chain { tag: 1, hops: 0 },
        ],
        "已准入的外部工作先跑，恢复的记录追加在后（不插队）"
    );
}

/// ⑤ 恢复侧再次停放 × 停放队列满：容量拒收单独计数，不重标成「已停放」。
#[test]
fn recovery_capacity_rejection_is_counted_separately() {
    let (fixture, _seen) = Fixture::plain();
    let store = SyncStore::new(fixture, NoFx);
    for _ in 0..2 {
        let receipt = store.dispatch_with_receipt(Act::Loop).expect("准入");
        assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });
    }
    assert_eq!(parked(&store), 2, "前置：两条停放记录");
    // 容量收紧到 1：第一条恢复记录的再次停放占满，第二条被容量拒收。
    store.set_deferred_capacity(1);

    let resumed = store.resume_deferred(2);
    assert_eq!(resumed.resumed, 2);
    assert_eq!(resumed.deferred_again, 1, "一条再次停放且被留存");
    assert_eq!(resumed.capacity_rejected, 1, "一条续接无法留存（非成功）");
    assert_eq!(parked(&store), 1, "被拒记录不计入留存/停放");
    assert_eq!(
        store.deferred_capacity_rejected(),
        1,
        "专用计数 fail-visible"
    );
}

/// ⑥ 与 close 串行：闭门后拒绝恢复；竞争下记录恰属一个 population。
#[test]
fn recovery_is_serialized_with_close() {
    let (fixture, _seen) = Fixture::plain();
    let store = SyncStore::new(fixture, NoFx);
    let receipt = store
        .dispatch_with_receipt(Act::Chain {
            tag: 9,
            hops: CHAIN,
        })
        .expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });
    store.close();
    let resumed = store.resume_deferred(1);
    assert_eq!(
        (resumed.resumed, resumed.remaining),
        (0, 1),
        "闭门后不恢复（记录仍归 store，不丢）"
    );

    // 竞争态：恢复与 close 任一先后 ⇒ 记录不丢不重（恰属一个 population）。
    let (fixture, _seen) = Fixture::plain();
    let store = Arc::new(SyncStore::new(fixture, NoFx));
    let receipt = store
        .dispatch_with_receipt(Act::Chain {
            tag: 9,
            hops: CHAIN,
        })
        .expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });

    let start = Arc::new(Barrier::new(2));
    let resumer = {
        let store = Arc::clone(&store);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            store.resume_deferred(1)
        })
    };
    start.wait();
    store.close();
    let resumed = resumer.join().expect("恢复线程不得 panic");
    assert_eq!(
        resumed.resumed + resumed.remaining,
        1,
        "记录要么被恢复、要么仍停放——不丢不重"
    );
}
