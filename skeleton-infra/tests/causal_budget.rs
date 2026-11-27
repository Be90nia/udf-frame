#![cfg(feature = "runtime")]
//! 因果工作量预算（GitHub issue#105 + #144）：回灌深度预算只约束链深；本文件钉住
//! 另外三个**独立计数**的界——per-root 因果工作量、per-reducer 分段效果、
//! per-drain 租约工作量——各自的耗尽语义、typed outcome、计数，以及 `0 = 不限`。
//!
//! 编排纪律：并发用例一律确定性握手 / 有界等待（OS 时钟截止），禁 sleep 竞速。
//! 分段预算的「暂存条数不过上限」判据落在 `skeleton` 的 `EffectBuf` 单测（直接观测
//! `effects.len()`）；本文件只钉运行时面的边界、结局、计数与 `0 = 不限`。

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, DispatchOutcome, SyncStore};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

// ── 夹具 ──────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    /// 产 `n` 个 `Leaf(i)` 回灌子代（扇出）。
    Fan(u32),
    /// 二叉扇出：深度 `d > 0` 时产两个 `Tree(d - 1)`，`d == 0` 零效果。
    Tree(u32),
    /// 产 `n` 条 `Run`（载荷 = 段内序号）。
    Runs(u32),
    /// 产一条 `Run`（载荷 0）。
    Run,
    /// 零效果叶子。
    Leaf(u32),
    /// 零效果占位（健康外部流量）。
    Ping,
}

struct Fixture {
    seen: Arc<Mutex<Vec<Act>>>,
}

impl Reducer for Fixture {
    type State = u64;
    type Action = Act;
    type Op = u32;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: Act, out: &mut EffectBuf<Act, u32>) -> u64 {
        self.seen.lock().unwrap().push(a);
        match a {
            Act::Fan(n) => {
                for i in 0..n {
                    out.push(Effect::Emit(Act::Leaf(i)));
                }
            }
            Act::Tree(depth) if depth > 0 => {
                for _ in 0..2 {
                    out.push(Effect::Emit(Act::Tree(depth - 1)));
                }
            }
            Act::Runs(n) => {
                for i in 0..n {
                    out.push(Effect::Run {
                        id: EffectId::new(u64::from(i) + 1),
                        op: i,
                    });
                }
            }
            Act::Run => out.push(Effect::Run {
                id: EffectId::new(1),
                op: 0,
            }),
            Act::Tree(_) | Act::Leaf(_) | Act::Ping => {}
        }
        s + 1
    }
}

/// 观测面：reduce 序、解释器实收的 Run 载荷、同栈回灌的返回值。
#[derive(Default)]
struct Obs {
    seen: Arc<Mutex<Vec<Act>>>,
    runs: Arc<Mutex<Vec<u32>>>,
    back_feed: Arc<Mutex<Vec<Act>>>,
    back_results: Arc<Mutex<Vec<bool>>>,
}

/// 解释器：记录 Run 载荷；`back_feed` 非空时在回调内**同栈回灌**（每条 Run 一次）。
#[derive(Default)]
struct Recorder {
    runs: Arc<Mutex<Vec<u32>>>,
    back_feed: Arc<Mutex<Vec<Act>>>,
    back_results: Arc<Mutex<Vec<bool>>>,
}

impl EffectRuntime<Act, u32> for Recorder {
    fn run(&mut self, effect: Effect<Act, u32>, back: &ActionSender<Act>) {
        if let Effect::Run { op, .. } = effect {
            self.runs.lock().unwrap().push(op);
        }
        let feed = std::mem::take(&mut *self.back_feed.lock().unwrap());
        for act in feed {
            let accepted = back.dispatch(act);
            self.back_results.lock().unwrap().push(accepted);
        }
    }
}

fn store() -> (SyncStore<u64, Act, u32>, Obs) {
    let obs = Obs::default();
    let fx = Recorder {
        runs: Arc::clone(&obs.runs),
        back_feed: Arc::clone(&obs.back_feed),
        back_results: Arc::clone(&obs.back_results),
    };
    let store = SyncStore::new(
        Fixture {
            seen: Arc::clone(&obs.seen),
        },
        fx,
    );
    (store, obs)
}

fn seen(obs: &Obs) -> Vec<Act> {
    obs.seen.lock().unwrap().clone()
}

// ── per-root 因果工作量预算 ────────────────────────────────────────────

/// 单根扇出超界 ⇒ 恰 `预算` 条子代被 reduce，其余停放；outcome `Deferred`、计数 =
/// 超出条数、深度预算计数不动、读数与实际入队条数一致；停放不自动重试。
/// 判别：去掉预留检查即红（全部子代被 reduce、无停放）。
#[test]
fn per_root_budget_parks_excess_and_reports_deferred() {
    let (store, obs) = store();
    store.set_causal_work_budget(4);
    let receipt = store.dispatch_with_receipt(Act::Fan(10)).expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 6 });
    assert_eq!(
        seen(&obs),
        vec![
            Act::Fan(10),
            Act::Leaf(0),
            Act::Leaf(1),
            Act::Leaf(2),
            Act::Leaf(3)
        ],
        "根 + 恰 4 个获准预留的子代"
    );
    assert_eq!(*store.snapshot(), 5);
    assert_eq!(store.causal_work_budget_exceeded(), 6);
    assert_eq!(
        store.deferred_actions(),
        6,
        "超出子动作全部留存（停放区默认容量内）"
    );
    assert_eq!(store.pending_actions(), 6, "读数 = 主队列 0 + 停放 6");
    assert_eq!(
        store.feedback_budget_exceeded(),
        0,
        "深度预算计数不被工作量预算推进"
    );
    assert_eq!(store.deferred_capacity_rejected(), 0);
    assert_eq!(store.causal_work_budget(), 4, "读侧回显配置");

    // 无自动重试：另一条外部 action 照常处理，停放记录原地不动。
    assert_eq!(store.try_dispatch(Act::Ping), AdmissionOutcome::Accepted);
    assert_eq!(store.deferred_actions(), 6, "停放记录只能经显式恢复离开");

    let resumed = store.resume_deferred(10);
    assert_eq!(
        (
            resumed.resumed,
            resumed.remaining,
            resumed.deferred_again,
            resumed.capacity_rejected
        ),
        (6, 0, 0, 0)
    );
    assert_eq!(*store.snapshot(), 12, "5 + Ping + 恢复的 6 条");
    assert_eq!(
        store.causal_work_budget_exceeded(),
        6,
        "恢复的续接不重复计拒绝"
    );
}

/// 预算按**整棵因果树**累计（不只是第一层扇出）：二叉树深度 3 共 14 个后代，
/// 预算 7 ⇒ 恰 7 个被接受（第 3 层只进 1 个），其余 7 个停放。
#[test]
fn per_root_budget_counts_every_level_of_the_tree() {
    let (store, obs) = store();
    store.set_causal_work_budget(7);
    let receipt = store.dispatch_with_receipt(Act::Tree(3)).expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 7 });
    assert_eq!(
        seen(&obs),
        vec![
            Act::Tree(3),
            Act::Tree(2),
            Act::Tree(2),
            Act::Tree(1),
            Act::Tree(1),
            Act::Tree(1),
            Act::Tree(1),
            Act::Tree(0),
        ],
        "前 7 个后代获准预留（广度优先序），第 8 个起停放"
    );
    assert_eq!(*store.snapshot(), 8);
    assert_eq!(store.causal_work_budget_exceeded(), 7);
    assert_eq!(store.deferred_actions(), 7);
}

/// 停放区满 ⇒ 超出子动作被容量拒收：outcome 是独立的 `CapacityRejected`
/// （retained / rejected 分列），专用计数与拒绝计数各自对账——不报 `Completed`，
/// 也不把被拒记录混进「已停放」。判别：把拒收并进 retained 即红。
#[test]
fn per_root_budget_rejects_when_parking_is_full() {
    let (store, obs) = store();
    store.set_causal_work_budget(4);
    store.set_deferred_capacity(2);
    let receipt = store.dispatch_with_receipt(Act::Fan(10)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::CapacityRejected {
            retained: 2,
            rejected: 4
        }
    );
    assert_eq!(*store.snapshot(), 5);
    assert_eq!(store.deferred_actions(), 2, "被拒记录不计入停放");
    assert_eq!(store.deferred_capacity_rejected(), 4);
    assert_eq!(store.causal_work_budget_exceeded(), 6, "已停放 2 + 被拒 4");
    assert_eq!(store.pending_actions(), 2);

    // legacy void dispatch 同路径不 panic：停放 / 拒收由计数与停放区承载。
    store.dispatch(Act::Fan(10));
    assert_eq!(*store.snapshot(), 10);
    assert_eq!(
        store.deferred_actions(),
        2,
        "停放区已满：这次超出的 6 条全部被拒"
    );
    assert_eq!(store.deferred_capacity_rejected(), 10);
    assert_eq!(store.causal_work_budget_exceeded(), 12);
    assert_eq!(seen(&obs).len(), 10, "被拒子动作从不进主队列");
}

/// 同栈回灌（解释器经 `back` 通道）同样消耗根预算：`sender.dispatch` 对已停放返回
/// `true`（仍归本 store）、对被拒收返回 `false`；根回执如实分列 retained / rejected。
/// 判别：只拦 Emit 分支、不拦 `enqueue` 的同栈回灌分支即红（三条全部 accepted）。
#[test]
fn reentrant_children_also_consume_the_root_budget() {
    let (store, obs) = store();
    store.set_causal_work_budget(1);
    store.set_deferred_capacity(1);
    *obs.back_feed.lock().unwrap() = vec![Act::Leaf(0), Act::Leaf(1), Act::Leaf(2)];
    let receipt = store.dispatch_with_receipt(Act::Run).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::CapacityRejected {
            retained: 1,
            rejected: 1
        }
    );
    assert_eq!(
        *obs.back_results.lock().unwrap(),
        vec![true, true, false],
        "获准 = true；已停放 = true（仍归本 store）；被拒收 = false"
    );
    assert_eq!(
        seen(&obs),
        vec![Act::Run, Act::Leaf(0)],
        "只有获准预留的那条被 reduce"
    );
    assert_eq!(store.causal_work_budget_exceeded(), 2);
    assert_eq!(store.deferred_actions(), 1);
    assert_eq!(store.deferred_capacity_rejected(), 1);
}

/// 同栈回灌的 `try_dispatch` 面（订阅回调内重入）：已停放 ⇒ `Accepted`（结果由根
/// 账本承担），被停放容量拒收 ⇒ `Full`（未留存，显式拒绝）。
#[test]
fn reentrant_try_dispatch_reports_full_only_when_rejected() {
    let (store, obs) = store();
    let store = Arc::new(store);
    store.set_causal_work_budget(2);
    store.set_deferred_capacity(1);
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let once = Arc::new(AtomicBool::new(false));
    let _ticket = {
        let inner = Arc::clone(&store);
        let outcomes = Arc::clone(&outcomes);
        let once = Arc::clone(&once);
        store.subscribe(move |state: &u64| {
            // 只在根落位的那一拍重入一次（此刻根的 2 个子代已获准预留）。
            if *state == 1 && !once.swap(true, Ordering::SeqCst) {
                outcomes
                    .lock()
                    .unwrap()
                    .push(inner.try_dispatch(Act::Leaf(7)));
                outcomes
                    .lock()
                    .unwrap()
                    .push(inner.try_dispatch(Act::Leaf(8)));
            }
        })
    };
    let receipt = store.dispatch_with_receipt(Act::Fan(2)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::CapacityRejected {
            retained: 1,
            rejected: 1
        }
    );
    assert_eq!(
        *outcomes.lock().unwrap(),
        vec![AdmissionOutcome::Accepted, AdmissionOutcome::Full]
    );
    assert_eq!(seen(&obs), vec![Act::Fan(2), Act::Leaf(0), Act::Leaf(1)]);
    assert_eq!(store.causal_work_budget_exceeded(), 2);
}

/// `0` = 不限是显式 opt-in：默认值在位时超出一格即停放一条；置 0 后同样的扇出
/// 全部接受。判别：把 0 当成「立即耗尽」、或默认不限，即红。
#[test]
fn zero_budget_is_explicit_opt_in_unbounded() {
    let (store, obs) = store();
    store.set_segment_effect_budget(0); // 只隔离 per-root 这一个界
    let default = store.causal_work_budget();
    assert!(default > 0, "默认启用：0 必须显式置");
    let over = u32::try_from(default + 1).expect("默认值在 u32 内");

    let receipt = store.dispatch_with_receipt(Act::Fan(over)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::Deferred { retained: 1 },
        "默认预算在位：恰超出一格"
    );
    assert_eq!(store.causal_work_budget_exceeded(), 1);
    let accepted = seen(&obs).len();
    assert_eq!(accepted, default + 1, "根 + 恰 default 个子代");

    store.set_causal_work_budget(0);
    assert_eq!(store.causal_work_budget(), 0);
    let receipt = store.dispatch_with_receipt(Act::Fan(over)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::Completed,
        "0 = 不限：同样的扇出全部接受"
    );
    assert_eq!(
        seen(&obs).len(),
        accepted + 1 + usize::try_from(over).expect("over 在 usize 内")
    );
    assert_eq!(store.causal_work_budget_exceeded(), 1, "置 0 后不再有拒绝");
}

/// 停放续接拿**新预算**：`resume_deferred` 后被停放的中间节点照常扇出（若沿用它
/// 停放前已耗尽的根预算，会立刻再次停放）。判别：预算跟着原账本走即红。
#[test]
fn resumed_continuation_gets_a_fresh_budget() {
    let (store, _obs) = store();
    store.set_causal_work_budget(2);
    let receipt = store.dispatch_with_receipt(Act::Tree(3)).expect("准入");
    // 根 → 2 个 Tree(2)（预算用尽）→ 它们各产的 Tree(1) 共 4 条被停放。
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 4 });
    assert_eq!(store.causal_work_budget_exceeded(), 4);

    let resumed = store.resume_deferred(10);
    assert_eq!(
        (
            resumed.resumed,
            resumed.remaining,
            resumed.deferred_again,
            resumed.capacity_rejected
        ),
        (4, 0, 0, 0),
        "续接各有新预算 2：每个 Tree(1) 的两个子代全部获准"
    );
    assert_eq!(*store.snapshot(), 15, "满二叉树 1 + 2 + 4 + 8 全部 reduce");
}

// ── per-reducer 分段效果预算 ───────────────────────────────────────────

/// 单个 reducer 一次吐 8 条、上限 3 ⇒ **整次转换被拒**（ADR-0011 §2.2 全有或全无）：
/// 无子代被回灌、状态不发布、超量 Emit 不入停放队列、根预算不被动用。判别：把
/// 上限检查去掉（8 条全部回灌 ⇒ `Deferred`）或退回「提交点截断」（前缀照跑、
/// outcome `Deferred{5}`）即红。
#[test]
fn segment_budget_rejects_whole_transition_over_cap() {
    let (store, obs) = store();
    store.set_segment_effect_budget(3);
    store.set_causal_work_budget(3);
    let receipt = store.dispatch_with_receipt(Act::Fan(8)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::BudgetRejected { rejected: 1 }
    );
    assert_eq!(seen(&obs), vec![Act::Fan(8)], "整段作废：子代一条都没跑");
    assert_eq!(*store.snapshot(), 0, "被拒转换的 next state 未发布");
    assert_eq!(store.segment_effect_overflow_count(), 1);
    assert_eq!(store.segment_effect_overflow_count(), 1);
    assert_eq!(
        store.causal_work_budget_exceeded(),
        0,
        "拒绝在入库前：不消耗根预算、不进停放路径"
    );
    assert_eq!(store.deferred_actions(), 0, "超量 Emit 不入停放队列");
    assert_eq!(store.pending_actions(), 0);
    assert_eq!(store.segment_effect_budget(), 3);

    // 主队列继续处理其他工作；实例不报废。
    assert_eq!(store.try_dispatch(Act::Ping), AdmissionOutcome::Accepted);
    assert_eq!(seen(&obs).last(), Some(&Act::Ping));
    assert_eq!(*store.snapshot(), 1);
}

/// 边界三档：cap-1 与 cap 都被接受，cap+1 被整次拒绝（状态与效果零进展）。
/// 判别：上限判定写成 `>` 而非 `>=`（cap+1 也过）或根本不判定（三档全过）即红。
#[test]
fn segment_budget_boundary_cap_minus_one_cap_cap_plus_one() {
    let (store, obs) = store();
    store.set_segment_effect_budget(4);

    let under = store.dispatch_with_receipt(Act::Fan(3)).expect("准入");
    assert_eq!(under.wait(), DispatchOutcome::Completed, "cap-1：接受");
    assert_eq!(seen(&obs).len(), 4, "根 + 3 子代都跑了");

    let at = store.dispatch_with_receipt(Act::Fan(4)).expect("准入");
    assert_eq!(at.wait(), DispatchOutcome::Completed, "cap：恰满仍接受");
    assert_eq!(seen(&obs).len(), 9, "根 + 4 子代");
    assert_eq!(store.segment_effect_overflow_count(), 0);

    let before = *store.snapshot();
    let over = store.dispatch_with_receipt(Act::Fan(5)).expect("准入");
    assert_eq!(over.wait(), DispatchOutcome::BudgetRejected { rejected: 1 });
    assert_eq!(*store.snapshot(), before, "cap+1：状态零进展");
    assert_eq!(seen(&obs).len(), 10, "cap+1 的子代一条都没跑");
    assert_eq!(store.segment_effect_overflow_count(), 1);
    assert_eq!(store.deferred_actions(), 0);
}

/// 溢出 ⇒ 该 segment 的任何效果都不转发（包括预算内的前缀——旧「提交点截断」
/// 语义下前缀会照常转发）；legacy `dispatch` 在同一路径上**不 panic**（ADR-0011
/// §2.5），只增加专属计数。判别：转发前缀、或 legacy 抛 panic 即红。
#[test]
fn segment_budget_rejection_forwards_nothing_and_legacy_does_not_panic() {
    let (store, obs) = store();
    store.set_segment_effect_budget(3);
    let receipt = store.dispatch_with_receipt(Act::Runs(5)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::BudgetRejected { rejected: 1 }
    );
    assert!(
        obs.runs.lock().unwrap().is_empty(),
        "整段作废：预算内前缀也不得转发"
    );
    assert_eq!(*store.snapshot(), 0, "状态未提交");
    assert_eq!(store.segment_effect_overflow_count(), 1);
    assert_eq!(store.pending_actions(), 0);

    let legacy = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Runs(5))));
    assert!(legacy.is_ok(), "预算拒绝不升级为 panic（资源保护拒绝）");
    assert_eq!(
        store.segment_effect_overflow_count(),
        2,
        "legacy 也计入专属计数"
    );
    assert!(
        obs.runs.lock().unwrap().is_empty(),
        "两次超限转换一条效果都没转发"
    );
    assert_eq!(*store.snapshot(), 0, "两次都没发布状态");

    // 实例不报废：后续 action 照常。
    assert_eq!(store.try_dispatch(Act::Ping), AdmissionOutcome::Accepted);
    assert_eq!(*store.snapshot(), 1);
}

// ── per-drain 租约工作量预算 ───────────────────────────────────────────

/// W = 4、根 + 10 子代 ⇒ 让出 2 次（4 条、再 4 条到界，剩 2 条不到界）；无人接手
/// 时原线程续排——FIFO 不变、无丢失无重复、outcome `Completed`。判别：去掉让出点
/// （计数恒 0）或让出后丢记录 / 重放即红。
#[test]
fn drain_budget_yields_and_continues_in_fifo_order() {
    let (store, obs) = store();
    store.set_drain_work_budget(4);
    assert_eq!(store.drain_work_budget(), 4);
    let receipt = store.dispatch_with_receipt(Act::Fan(10)).expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Completed, "让出不是未完成");
    assert_eq!(store.drain_lease_yields(), 2);
    let expected: Vec<Act> = std::iter::once(Act::Fan(10))
        .chain((0..10).map(Act::Leaf))
        .collect();
    assert_eq!(seen(&obs), expected, "FIFO 不变、无丢失、无重复");
    assert_eq!(*store.snapshot(), 11);
    assert_eq!(store.pending_actions(), 0);
    assert_eq!(store.causal_work_budget_exceeded(), 0);
}

/// 让出与运行时无关：排空让出点只用 `std::thread::yield_now`，在**没有 tokio
/// 运行时**的普通线程上触发 per-drain 让出必须照常返回。判别：换成需要 reactor
/// 的 tokio yield 即 panic「no reactor running」。护栏 = OS 时钟截止（不挂死）。
#[test]
fn yield_on_plain_thread_without_tokio_runtime() {
    let (done, finished) = mpsc::channel();
    // 普通 std 线程（非 #[tokio::test]、从未进入过任何运行时）：同步入口走排空环。
    let worker = thread::spawn(move || {
        let (store, obs) = store();
        store.set_drain_work_budget(3);
        let outcome = store
            .dispatch_with_receipt(Act::Fan(10))
            .expect("准入")
            .wait();
        let _ = done.send((outcome, store.drain_lease_yields(), seen(&obs).len()));
    });
    let (outcome, yields, reduced) = finished
        .recv_timeout(Duration::from_secs(10))
        .expect("纯 std 线程上的让出必须返回（不依赖任何异步运行时）");
    worker.join().expect("工作线程不得 panic");
    assert_eq!(outcome, DispatchOutcome::Completed);
    assert_eq!(yields, 3, "W=3：首条 + 10 子代 ⇒ 3 次到界让出");
    assert_eq!(reduced, 11);
}

/// 让出交接下外部健康流量照常推进：4 个扇出根（各 21 条记录，单根必跨多次让出）
/// 与 2 路纯 `try_dispatch` 生产者并发，全部返回（无自死锁），每条 action 恰
/// reduce 一次、根回执全部 `Completed`。判别：交接丢记录 / 重复结算 / 死锁即红
/// （死锁由 OS 时钟护栏转成失败）。
#[test]
fn external_traffic_progresses_across_yields() {
    const ROOTS: usize = 4;
    const PINGS_PER_PRODUCER: usize = 200;
    let (store, _obs) = store();
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
    for _ in 0..2 {
        let store = Arc::clone(&store);
        pings.push(thread::spawn(move || {
            for _ in 0..PINGS_PER_PRODUCER {
                if store.try_dispatch(Act::Ping) != AdmissionOutcome::Accepted {
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
            .all(|h| h.join().expect("ping 生产者不得 panic"));
        let _ = done.send((outcomes, pings_ok));
    });
    let (outcomes, pings_ok) = finished
        .recv_timeout(Duration::from_secs(30))
        .expect("全部生产者在 30s 内返回（让出交接不得自死锁）");
    joiner.join().expect("joiner 不得 panic");

    assert!(pings_ok, "外部健康流量全部被准入");
    assert_eq!(outcomes.len(), ROOTS);
    assert!(
        outcomes.iter().all(|o| *o == DispatchOutcome::Completed),
        "根回执全部 Completed：{outcomes:?}"
    );
    let total = (ROOTS * 21 + 2 * PINGS_PER_PRODUCER) as u64;
    assert_eq!(*store.snapshot(), total, "每条 action 恰 reduce 一次");
    assert_eq!(store.pending_actions(), 0);
    // 让出次数与调度耦合：并发下他人可接手租约，单根不再自累积 ⇒ 下界不可断言
    // （负载下偶红，见 verify/2026-11-26-wave7/V2/pm-test-infra.log）。改钉负载无关
    // 的守恒式：`work` 到界才让出且让出即清零 ⇒ 每次让出至少消化一整个预算，故
    // 「让出次数 × 预算 ≤ 总记录数」恒成立（判别：让出计数凭空增长/重复计数即红）。
    // 单根让出次数由 `yield_on_plain_thread_without_tokio_runtime`（W=3，恰 3 次）
    // 确定性钉住。
    let budget = store.drain_work_budget() as u64;
    assert!(
        store.drain_lease_yields() * budget <= total,
        "让出必须由足量工作量背书：{} 次 × {budget} > {total}",
        store.drain_lease_yields()
    );
    assert_eq!(store.causal_work_budget_exceeded(), 0);
    assert_eq!(store.segment_effect_overflow_count(), 0);
}
