#![cfg(feature = "runtime")]
//! ADR-0011 §2.2（issue#233 票①）：reducer 转换**全有或全无**。
//!
//! 钉五件事：
//!
//! 1. **拒绝不发布任何东西**：next state 不发布、该 segment 效果零转发、超量
//!    `Emit` 不入 deferred 队列（`causal_work_budget_exceeded` 不动 = 没走回灌 /
//!    停放路径），审计时间线里只有 `SegmentEffectOverflow`、没有 `Reduced`。
//! 2. **此前已提交的 segments 原样存活**：解释器 panic 留下的未转发后缀，不因后续
//!    action 被拒而被牵连（关闭报告的 `pending_effects` 仍精确报出它）。
//! 3. **三类结局可区分**：`BudgetRejected`（分段预算）/ `CapacityRejected`（停放
//!    容量）/ `Failed`（解释器失败）各走各的结局与计数，互不串账。
//! 4. **legacy `dispatch` 不 panic**：预算拒绝是资源保护拒绝，只从专属计数与审计
//!    事件显形（ADR-0011 §2.5）。
//! 5. 拒绝后实例仍可用（不报废）。

use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectKind, EffectRuntime, Reducer, Store as _,
};
use skeleton_infra::audit::{AuditEvent, AuditEventKind, AuditTicket};
use skeleton_infra::runtime::{
    CloseReport, CloseReportOutcome, DispatchOutcome, DispatchPhase, PendingSegmentSummary,
    SyncStore,
};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

/// 解释器在这条 `Run` 的 `op` 上 panic（制造「已提交未转发」后缀）。
const POISON_OP: u32 = 99;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Act {
    /// 产 `n` 条 `Emit(Leaf)` 回灌子代。
    Fan(u32),
    /// 产 `n` 条 `Run`（`op` = 段内序号）。
    Runs(u32),
    /// 产两条 `Run`：第一条让解释器 panic，第二条留在后缀里。
    PoisonRun,
    /// 产 `n` 个 `Emit(Runs(3))` 子代（用来制造「被停放的后代」）。
    Burst(u32),
    /// 零效果叶子。
    Leaf(u32),
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
        self.seen.lock().expect("seen 锁").push(a);
        match a {
            Act::Fan(n) => {
                for i in 0..n {
                    out.push(Effect::Emit(Act::Leaf(i)));
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
            Act::PoisonRun => {
                out.push(Effect::Run {
                    id: EffectId::new(9),
                    op: POISON_OP,
                });
                out.push(Effect::Run {
                    id: EffectId::new(10),
                    op: 1,
                });
            }
            Act::Burst(n) => {
                for _ in 0..n {
                    out.push(Effect::Emit(Act::Runs(3)));
                }
            }
            Act::Leaf(_) => {}
        }
        s + 1
    }
}

/// 解释器：记录成功转发的 `Run` 载荷；`POISON_OP` 直接 panic（fail-visible）。
struct Runner {
    runs: Arc<Mutex<Vec<u32>>>,
}

impl EffectRuntime<Act, u32> for Runner {
    fn run(&mut self, effect: Effect<Act, u32>, _back: &ActionSender<Act>) {
        if let Effect::Run { op, .. } = effect {
            if op == POISON_OP {
                panic!("udf-frame test: interpreter exploded on op 99");
            }
            self.runs.lock().expect("runs 锁").push(op);
        }
    }
}

/// 夹具句柄：store + reduce 序（解释器的 `Run` 载荷只由夹具内部观察）。
struct Harness {
    store: SyncStore<u64, Act, u32>,
    seen: Arc<Mutex<Vec<Act>>>,
}

fn store() -> Harness {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let runs = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        Fixture {
            seen: Arc::clone(&seen),
        },
        Runner {
            runs: Arc::clone(&runs),
        },
    );
    Harness { store, seen }
}

fn collect_events(
    store: &SyncStore<u64, Act, u32>,
) -> (AuditTicket, Arc<Mutex<Vec<AuditEventKind>>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&seen);
    let ticket = store.subscribe_events(256, move |event: &AuditEvent| {
        sink.lock().expect("事件槽锁").push(event.kind);
    });
    (ticket, seen)
}

fn drained(outcome: CloseReportOutcome) -> CloseReport {
    match outcome {
        CloseReportOutcome::Drained(report) => report,
        other => panic!("期望 Drained，实得 {other:?}"),
    }
}

/// ① 拒绝不发布状态 / 不转发效果 / 不入 deferred 队列；审计面只有
/// `SegmentEffectOverflow`（后继无 `Reduced`、无回灌 `Enqueued`）。
/// 判别：把上限检查去掉（子代照跑 ⇒ 状态前进、有 `Enqueued`）即红。
#[test]
fn rejection_publishes_nothing_and_leaves_no_queue_trace() {
    let Harness { store, seen, .. } = store();
    store.set_segment_effect_budget(2);
    // 根预算收紧到 1：超量 Emit 若真进入回灌路径，会立刻撞根预算被停放 / 拒收
    // ⇒ 这是「不入队列」的鉴别器（不是靠 deferred 计数恰好为 0）。
    store.set_causal_work_budget(1);
    let (_ticket, kinds) = collect_events(&store);

    let receipt = store.dispatch_with_receipt(Act::Fan(5)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::BudgetRejected { rejected: 1 }
    );
    assert_eq!(
        *seen.lock().expect("seen 锁"),
        vec![Act::Fan(5)],
        "子代一条都没跑"
    );
    assert_eq!(*store.snapshot(), 0, "next state 未发布");
    assert_eq!(
        store.causal_work_budget_exceeded(),
        0,
        "超量 Emit 未进回灌 / 停放路径"
    );
    assert_eq!(store.deferred_actions(), 0, "不入停放队列");
    assert_eq!(store.pending_actions(), 0);
    assert_eq!(store.segment_effect_overflow_count(), 1);

    let kinds = kinds.lock().expect("事件槽锁").clone();
    assert!(
        kinds.contains(&AuditEventKind::SegmentEffectOverflow),
        "拒绝必须有审计节点：{kinds:?}"
    );
    assert!(
        !kinds.contains(&AuditEventKind::Reduced),
        "被拒转换不得报 Reduced：{kinds:?}"
    );
    assert!(
        !kinds.iter().any(
            |k| matches!(k, AuditEventKind::Enqueued { feedback_depth } if *feedback_depth > 0)
        ),
        "超量 Emit 不得入队（无回灌 Enqueued）：{kinds:?}"
    );
}

/// ② 此前已提交的 segments 原样存活：解释器 panic 留下的未转发后缀，不因下一个
/// action 被拒而被牵连（也不被顺手转发）。
/// 判别：拒绝分支顺手清空 / 触动 pending（旧「提交点截断」在转发环里干活）即红。
#[test]
fn rejection_leaves_previously_committed_segments_alive() {
    let Harness { store, .. } = store();
    // ① 首个 action：状态已提交、解释器在第 0 条 Run 上 panic ⇒ 第 1 条留作后缀。
    let boom = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::PoisonRun)));
    assert!(boom.is_err(), "解释器 panic 必须上浮（fail-visible）");
    assert_eq!(
        *store.snapshot(),
        1,
        "状态已提交（解释器 panic 发生在提交之后）"
    );

    // ② 第二个 action：暂存超界 ⇒ 整次被拒（不转发、不发布）。
    store.set_segment_effect_budget(1);
    let receipt = store.dispatch_with_receipt(Act::Fan(3)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::BudgetRejected { rejected: 1 }
    );
    assert_eq!(*store.snapshot(), 1, "只有首个 action 的状态被发布");

    // ③ 关闭报告：① 的后缀原样存活（1 条、段级身份不变），② 没留下任何段。
    let report = drained(store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(report.queued_actions, 0);
    assert_eq!(report.deferred_actions, 0);
    assert_eq!(report.pending_effects.effects, 1, "① 的后缀必须原样存活");
    assert_eq!(
        report.pending_effects.segments,
        vec![PendingSegmentSummary {
            origin_action_seq: 0,
            next_effect_index: 1,
            effects: 1,
        }],
        "段级身份 = 产出 action 的入队序号 + 段内起点"
    );
    assert_eq!(
        report.interrupted_effect.expect("中断归属").effect_kind,
        EffectKind::Run
    );
}

/// ③ pre 恢复记录被拒（ADR-0011 §2.2 + `resume_deferred` 文档）：被停放的后代
/// 恢复时暂存超界 ⇒ 整次被拒——状态不前进、**不重新停放**、`DeferredResume`
/// 任何批次计数都不动，只留全局专属计数 + 审计事件（该记录无 `Reduced`）。
/// 判别：把拒绝当成「再次停放」记账（`Deferred { retained: 1 }` ⇒ `deferred_again`
/// 加一）即红；把拒绝当成成功提交（状态前进）也红。
#[test]
fn resumed_record_rejected_by_staging_cap_is_visible_and_not_reparked() {
    let Harness { store, seen } = store();
    // 根预算 1 + 停放容量 4：root 产 2 个 `Runs(3)` 子代 ⇒ 恰 1 个获准、1 个被停放。
    store.set_causal_work_budget(1);
    store.set_deferred_capacity(4);
    let receipt = store.dispatch_with_receipt(Act::Burst(2)).expect("准入");
    assert_eq!(receipt.wait(), DispatchOutcome::Deferred { retained: 1 });
    assert_eq!(store.deferred_actions(), 1, "前置：恰一条被停放");
    let before = *store.snapshot();
    assert_eq!(before, 2, "root + 获准的那一个子代");

    // 收紧分段预算到 1：恢复的 `Runs(3)` 要吐 3 条 ⇒ 整次被拒。
    store.set_segment_effect_budget(1);
    let (_ticket, kinds) = collect_events(&store);
    let resumed = store.resume_deferred(1);

    assert_eq!(resumed.resumed, 1);
    assert_eq!(resumed.remaining, 0, "被拒记录不得重新停放");
    assert_eq!(
        (resumed.deferred_again, resumed.capacity_rejected),
        (0, 0),
        "拒绝不计入任何批次计数（只算全局专属计数）"
    );
    assert_eq!(store.deferred_actions(), 0);
    assert_eq!(*store.snapshot(), before, "被拒转换不发布 next state");
    assert_eq!(store.segment_effect_overflow_count(), 1);
    assert_eq!(
        *seen
            .lock()
            .expect("seen 锁")
            .last()
            .expect("至少一次 reduce"),
        Act::Runs(3),
        "恢复记录确实跑过 reduce（拒绝发生在提交之前）"
    );

    let kinds = kinds.lock().expect("事件槽锁").clone();
    assert!(
        kinds.contains(&AuditEventKind::SegmentEffectOverflow),
        "恢复记录的拒绝必须上审计面：{kinds:?}"
    );
    assert!(
        !kinds.contains(&AuditEventKind::Reduced),
        "被拒的恢复记录不得报 Reduced：{kinds:?}"
    );
}

/// ③ 三类非成功结局与计数互不串账（各自独立 store，读数干净）。
#[test]
fn three_outcomes_and_counters_stay_separate() {
    // (a) 分段预算拒绝
    let Harness { store: budget, .. } = store();
    budget.set_segment_effect_budget(1);
    let receipt = budget.dispatch_with_receipt(Act::Runs(2)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::BudgetRejected { rejected: 1 }
    );
    assert_eq!(budget.segment_effect_overflow_count(), 1);
    assert_eq!(budget.causal_work_budget_exceeded(), 0);
    assert_eq!(budget.deferred_capacity_rejected(), 0);

    // (b) 停放容量拒收（撞根预算且停放队列满）
    let Harness {
        store: capacity, ..
    } = store();
    capacity.set_causal_work_budget(1);
    capacity.set_deferred_capacity(1);
    let receipt = capacity.dispatch_with_receipt(Act::Fan(4)).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::CapacityRejected {
            retained: 1,
            rejected: 2
        }
    );
    assert_eq!(capacity.causal_work_budget_exceeded(), 3);
    assert_eq!(capacity.deferred_capacity_rejected(), 2);
    assert_eq!(capacity.segment_effect_overflow_count(), 0);

    // (c) 解释器失败（提交后部分失败；panic 上浮，计数各自独立）
    let Harness { store: failed, .. } = store();
    let boom = catch_unwind(AssertUnwindSafe(|| {
        failed
            .dispatch_with_receipt(Act::PoisonRun)
            .expect("准入")
            .wait()
    }));
    assert!(boom.is_err(), "解释器 panic 必须上浮（fail-visible）");
    assert_eq!(failed.segment_effect_overflow_count(), 0);
    assert_eq!(failed.causal_work_budget_exceeded(), 0);
    assert_eq!(failed.deferred_capacity_rejected(), 0);
}

/// ③b 「解释器失败」的 typed 结局仍是 `Failed`（与 `BudgetRejected` 分得开）：
/// 在任排空者 panic ⇒ 排队中的记录由排空守卫以 `Queued` / `state_committed=false`
/// 结算，等待方拿到 `Failed`（不是 `BudgetRejected`、不计预算账）。
/// 判别：把预算拒绝也塞进 `Failed`（旧口径）即红——本用例的 `phase` /
/// `state_committed` 与被拒转换根本不同形。
#[test]
fn drain_interruption_reports_failed_outcome_to_waiting_receipt() {
    struct GatedBoom {
        entered: Mutex<mpsc::Sender<()>>,
        release: Mutex<mpsc::Receiver<()>>,
    }
    struct AlwaysPanics;
    impl Reducer for GatedBoom {
        type State = u64;
        type Action = Act;
        type Op = u32;
        fn initial(&self) -> u64 {
            0
        }
        fn reduce(&self, s: &u64, a: Act, out: &mut EffectBuf<Act, u32>) -> u64 {
            if a == Act::PoisonRun {
                self.entered
                    .lock()
                    .expect("entered 锁")
                    .send(())
                    .expect("进场");
                self.release
                    .lock()
                    .expect("release 锁")
                    .recv()
                    .expect("放行");
            }
            out.push(Effect::Run {
                id: EffectId::new(1),
                op: 0,
            });
            s + 1
        }
    }
    impl EffectRuntime<Act, u32> for AlwaysPanics {
        fn run(&mut self, _effect: Effect<Act, u32>, _back: &ActionSender<Act>) {
            panic!("udf-frame test: interpreter always panics");
        }
    }

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let store = Arc::new(SyncStore::new(
        GatedBoom {
            entered: Mutex::new(entered_tx),
            release: Mutex::new(release_rx),
        },
        AlwaysPanics,
    ));
    let worker = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::PoisonRun))).is_err()
        })
    };
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");
    // 竞争态：本条入队，由在任排空者（worker）消费 ⇒ 只拿到回执，不自己排空。
    let receipt = store.dispatch_with_receipt(Act::Leaf(0)).expect("准入");
    release_tx.send(()).expect("放行 reducer");
    assert!(worker.join().expect("worker 线程"), "解释器 panic 必须上浮");

    match receipt.wait() {
        DispatchOutcome::Failed(failure) => {
            assert_eq!(failure.phase, DispatchPhase::Queued, "排队中被中断");
            assert!(!failure.state_committed, "该 action 从未提交");
            assert_eq!(failure.cause, "drain interrupted by panic");
        }
        other => panic!("期望 Failed，实得 {other:?}"),
    }
    assert_eq!(
        store.segment_effect_overflow_count(),
        0,
        "解释器失败不计入预算账（三类计数互不串账）"
    );
}
