#![cfg(feature = "runtime")]
//! per-drain 租约交接（ADR-0011 §2.4，issue#234）：到界让出**所有权**，等入口账本
//! 完成**不持租约**，无人接手时让出者自己续排（禁搁浅）。
//!
//! 白盒编排（让出闸停靠、账本终局判据、让出后的执行线程）在
//! `src/runtime.rs` 的单测模块；本文件只钉**公开面**可观测的三件事：
//! ① 无竞争生产者时到界仍推进到完成；② 静默态 sender / `try_dispatch` 的返回
//! 仍是本 action 的完成边界；③ 并发生产者在交接下每条 action 恰好一次。
//!
//! 编排纪律：有界等待（OS 时钟截止）+ mpsc 握手，禁 sleep 竞速。

use skeleton::{Effect, EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, DispatchOutcome, SyncStore};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

// ── 夹具 ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    /// 扇出 `n` 个 `Leaf(i)`（回灌子代，挂在本根的账本上）。
    Fan(u32),
    /// 零效果叶子。
    Leaf(u32),
    /// 外部唯一标签（压力用例的「恰好一次」判据）。
    Tagged(u32),
}

struct Fixture {
    seen: Arc<Mutex<Vec<Act>>>,
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
        if let Act::Fan(n) = a {
            for i in 0..n {
                out.push(Effect::Emit(Act::Leaf(i)));
            }
        }
        s + 1
    }
}

/// reduce 序观测面（减 type_complexity，不压制 lint）。
type Obs = Arc<Mutex<Vec<Act>>>;

fn store() -> (SyncStore<u64, Act, Infallible>, Obs) {
    let seen: Obs = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Fixture {
            seen: Arc::clone(&seen),
        },
        NoFx,
    );
    (store, seen)
}

/// reduce 序快照。
fn seen(seen: &Mutex<Vec<Act>>) -> Vec<Act> {
    seen.lock().unwrap().clone()
}

fn fan_order(n: u32) -> Vec<Act> {
    std::iter::once(Act::Fan(n))
        .chain((0..n).map(Act::Leaf))
        .collect()
}

// ── ① 无竞争生产者：到界无人接手 ⇒ 仍推进到完成（禁搁浅）────────────────

/// 单线程、无其他生产者：`try_dispatch` 在静默态夺得租约，每次到界都无人接手
/// ⇒ 必须自己重新竞争并续排到队列空。判别：让出后直接返回（不重新竞争）⇒
/// 快照停在到界处、`pending_actions() != 0`，红。
#[test]
fn single_thread_at_budget_drains_to_completion() {
    let (store, obs) = store();
    store.set_drain_work_budget(3);

    assert_eq!(
        store.try_dispatch(Act::Fan(10)),
        AdmissionOutcome::Accepted,
        "静默态必被准入"
    );

    // try_dispatch 的静默态契约：返回即本 action 的完成边界（让出后不持租约，
    // 但不放松等待）⇒ 11 条全部已 reduce、队列无搁浅。
    assert_eq!(*store.snapshot(), 11, "无竞争生产者时到界仍推进到完成");
    assert_eq!(store.pending_actions(), 0, "无队列搁浅");
    assert_eq!(
        store.drain_lease_yields(),
        3,
        "W=3：首条 + 10 子代 ⇒ 3 次到界让出（各自都由本线程重新竞争续排）"
    );
    assert_eq!(seen(&obs), fan_order(10), "FIFO 不变、无丢失、无重复");
    assert_eq!(store.causal_work_budget_exceeded(), 0);
    assert_eq!(store.segment_effect_overflow_count(), 0);
}

// ── ② 静默态 sender 的 bool 语义：返回值仍是完成边界 ──────────────────

/// 框架提供的 sender 在静默态夺得租约并同步排空至 idle（`bool` 只承载准入，
/// 不承载完成状态）——本测钉的是那个**完成边界**在多次到界让出之后依然成立：
/// 返回时本 action 的因果子树全部已 reduce。判别：让出后不等待即返回 ⇒ 快照
/// 停在到界处，红。
#[test]
fn sender_silent_state_returns_at_completion_under_yields() {
    let (store, obs) = store();
    store.set_drain_work_budget(2);

    assert!(store.sender().dispatch(Act::Fan(6)), "静默态必须被接受");
    assert_eq!(
        *store.snapshot(),
        7,
        "静默态：返回时本 action 的完成边界已达成（Fan + 6 个叶子）"
    );
    assert_eq!(store.pending_actions(), 0);
    assert_eq!(store.drain_lease_yields(), 3, "W=2：7 条 ⇒ 3 次到界让出");
    assert_eq!(seen(&obs), fan_order(6));
}

// ── ③ 有竞争生产者：每条 action 恰好一次、根回执全部完成 ────────────────

/// 4 个回执根（各 21 条记录，单根必跨多次到界让出）与 2 路 `try_dispatch` 外部
/// 生产者并发：交接全程 FIFO 单队列，每条 action 恰 reduce 一次（唯一标签集合 =
/// 全量），根回执全部 `Completed`，无队列搁浅。判别：交接丢记录 / 重复处理 /
/// 自死锁（有界 `recv_timeout` 转成失败）即红。
#[test]
fn competing_producers_complete_every_action_exactly_once() {
    const ROOTS: u32 = 4;
    const PINGS_PER_PRODUCER: u32 = 200;
    let (store, obs) = store();
    let store = Arc::new(store);
    store.set_drain_work_budget(8);

    let mut roots = Vec::new();
    for _ in 0..ROOTS {
        let store = Arc::clone(&store);
        roots.push(thread::spawn(move || {
            store
                .dispatch_with_receipt(Act::Fan(20))
                .expect("准入")
                .wait()
        }));
    }
    let mut pings = Vec::new();
    for producer in 0..2u32 {
        let store = Arc::clone(&store);
        pings.push(thread::spawn(move || {
            for i in 0..PINGS_PER_PRODUCER {
                let tag = producer * PINGS_PER_PRODUCER + i;
                if store.try_dispatch(Act::Tagged(tag)) != AdmissionOutcome::Accepted {
                    return false;
                }
            }
            true
        }));
    }

    let (done, finished) = mpsc::channel();
    let joiner = thread::spawn(move || {
        let outcomes: Vec<DispatchOutcome> = roots
            .into_iter()
            .map(|h| h.join().expect("根生产者不得 panic"))
            .collect();
        let pings_ok = pings
            .into_iter()
            .all(|h| h.join().expect("tag 生产者不得 panic"));
        let _ = done.send((outcomes, pings_ok));
    });
    let (outcomes, pings_ok) = finished
        .recv_timeout(Duration::from_secs(30))
        .expect("全部生产者在 30s 内返回（交接不得自死锁）");
    joiner.join().expect("joiner 不得 panic");

    assert!(pings_ok, "外部健康流量全部被准入");
    assert!(
        outcomes.iter().all(|o| *o == DispatchOutcome::Completed),
        "根回执全部 Completed：{outcomes:?}"
    );
    let leaves = ROOTS as u64 * 20;
    let tags = 2 * PINGS_PER_PRODUCER;
    assert_eq!(
        *store.snapshot(),
        ROOTS as u64 + leaves + tags as u64,
        "每条 action 恰 reduce 一次"
    );
    // 恰好一次：每个唯一标签出现且仅出现一次（无丢失、无重复）。
    let mut tagged: Vec<u32> = seen(&obs)
        .into_iter()
        .filter_map(|a| match a {
            Act::Tagged(tag) => Some(tag),
            _ => None,
        })
        .collect();
    tagged.sort_unstable();
    assert_eq!(
        tagged,
        (0..tags).collect::<Vec<u32>>(),
        "每个外部 action 恰好一次（无丢失、无重复）"
    );
    assert_eq!(store.pending_actions(), 0, "无队列搁浅");
    // 让出计数只证明「到界让出发生了」，**不**证明「有人接手」（让出后自己重夺同样
    // 计数 +1）。交接本身的证据在内核白盒探针：`runtime::tests::handoff_wait_survives_pool_reuse`
    // 与 `handoff_waits_entry_ledger_without_holding_lease` 断言 `handoffs == 1`
    // （重新竞争失败 = 他人真的接手）。本测只管公开面守恒。
    assert!(store.drain_lease_yields() > 0, "并发负载下必然发生到界让出");
    assert_eq!(store.causal_work_budget_exceeded(), 0);
    assert_eq!(store.segment_effect_overflow_count(), 0);
}
