//! issue#61 契约（定稿 B + C）：关闭期**已提交未转发效果后缀**的报告与显式丢弃。
//!
//! 钉四件事（对应 issue 的 Acceptance）：
//!
//! 1. **quiescence 闸**：活跃排空（gated reducer / 效果）仍在跑时，waiter **不能**
//!    报 `Drained`——超时返回 `TimedOutStillRunning`，且该变体**不携带**快照
//!    （类型层：超时/重入都不带「看起来终态」的计数）。
//! 2. **终态报告含该后缀**：放行后 `Drained(report)` 的
//!    `pending_effects` 精确报出条数与段级身份（producer 的入队序号 + 段内位置），
//!    `interrupted_effect` 报出被中断那条的归属（与 #48 五字段同值）。
//! 3. **重入不暴露假终态**：本 store 排空帧内调用返回 `Reentrant`，准入已关。
//! 4. **显式丢弃**：返回精确 count/identities，**不跑宿主 `EffectRuntime`**；
//!    丢弃后再报告为 0，被中断归属仍保留（它是历史，不是后缀）。
//!
//! 另钉「四个 population 分离」：停放的 deferred action 非零也**不影响**
//! `Drained`（它不在 #64 谓词里），且不与效果后缀合并成同一个计数。
//!
//! 全程握手/有界超时驱动，零 sleep。

#![cfg(feature = "runtime")]

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectKind, EffectRuntime, Reducer, Store as _,
};
use skeleton_infra::runtime::{CloseReport, CloseReportOutcome, PendingSegmentSummary, SyncStore};

type UnderTest = Arc<SyncStore<u64, u64, u64>>;
/// reducer 内的自我句柄槽（构造期注入：SyncStore 需要先存在）。
type StoreSlot = Arc<Mutex<Option<std::sync::Weak<SyncStore<u64, u64, u64>>>>>;

/// 动作 1 = 受闸动作：进 reduce 即发「已进场」并阻塞到放行；其余直通。
const GATED: u64 = 1;

struct Gate {
    entered: Mutex<mpsc::Sender<()>>,
    release: Mutex<mpsc::Receiver<()>>,
}

/// 产效果器：`state == 0` 的那一拍产 `per_action` 条 `Run`（id = 1..，op = 10000+i）。
/// 之后各拍不产效果 ⇒ 第二拍只用来驱动「续转」，断言不会混淆新产与续转。
struct Producer {
    per_action: u64,
    gate: Option<Gate>,
    /// 非 `None` 时：本 reducer 在 reduce 内调用 `close_wait_with_report`（重入面）。
    reentrant: Option<(StoreSlot, Arc<Mutex<Option<CloseReportOutcome>>>)>,
}

impl Reducer for Producer {
    type State = u64;
    type Action = u64;
    type Op = u64;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, state: &u64, action: u64, out: &mut EffectBuf<u64, u64>) -> u64 {
        if let Some(gate) = &self.gate {
            if action == GATED {
                gate.entered
                    .lock()
                    .expect("entered 通道")
                    .send(())
                    .expect("entered 发送");
                gate.release
                    .lock()
                    .expect("release 通道")
                    .recv()
                    .expect("release 接收");
            }
        }
        if let Some((slot, seen)) = &self.reentrant {
            let store = slot
                .lock()
                .expect("store 槽")
                .as_ref()
                .and_then(std::sync::Weak::upgrade)
                .expect("store 未注入");
            let outcome = store.close_wait_with_report(Duration::from_secs(5));
            *seen.lock().expect("outcome 槽") = Some(outcome);
        }
        if *state == 0 {
            for i in 0..self.per_action {
                out.push(Effect::Run {
                    id: EffectId::new(i + 1),
                    op: 10_000 + i,
                });
            }
        }
        *state + action
    }
}

/// 解释器：第 0 次调用 panic（模拟 `EffectRuntime::run` 中断），其余记录收到的 op。
struct PanicOnFirst {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<u64>>>,
}

impl EffectRuntime<u64, u64> for PanicOnFirst {
    fn run(&mut self, effect: Effect<u64, u64>, _back: &ActionSender<u64>) {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            panic!("第 0 个效果转发中断（测试注入：提交后缀必须留存）");
        }
        if let Effect::Run { op, .. } = effect {
            self.seen.lock().expect("recorder").push(op);
        }
    }
}

struct Fixture {
    store: UnderTest,
    entered: mpsc::Receiver<()>,
    release: mpsc::Sender<()>,
    calls: Arc<AtomicUsize>,
}

/// 受闸 store：`per_action` 条效果，第 0 条转发中断。
fn gated_fixture(per_action: u64) -> Fixture {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(SyncStore::new(
        Producer {
            per_action,
            gate: Some(Gate {
                entered: Mutex::new(entered_tx),
                release: Mutex::new(release_rx),
            }),
            reentrant: None,
        },
        PanicOnFirst {
            calls: Arc::clone(&calls),
            seen: Arc::new(Mutex::new(Vec::new())),
        },
    ));
    Fixture {
        store,
        entered: entered_rx,
        release: release_tx,
        calls,
    }
}

/// 在 worker 线程 dispatch 受闸动作（其 panic 必须原样上浮 = fail-visible）。
fn spawn_gated_dispatch(store: &UnderTest) -> thread::JoinHandle<bool> {
    let store = Arc::clone(store);
    thread::spawn(move || catch_unwind(AssertUnwindSafe(|| store.dispatch(GATED))).is_err())
}

fn drained(outcome: CloseReportOutcome) -> CloseReport {
    match outcome {
        CloseReportOutcome::Drained(report) => report,
        other => panic!("期望 Drained，实得 {other:?}"),
    }
}

/// ① waiter 在活跃排空提交最终后缀前**不能**报告；放行后的终态报告**含**该后缀。
#[test]
fn report_waits_for_active_drain_and_includes_its_committed_suffix() {
    let fx = gated_fixture(3);
    let worker = spawn_gated_dispatch(&fx.store);
    fx.entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");

    // 排空仍在跑（reducer 阻塞在提交点之前）⇒ 不得报 Drained，且超时变体不带快照。
    assert_eq!(
        fx.store.close_wait_with_report(Duration::from_millis(120)),
        CloseReportOutcome::TimedOutStillRunning,
        "活跃排空未结束时不得报终态"
    );

    fx.release.send(()).expect("放行 reducer");
    assert!(
        worker.join().expect("worker 线程"),
        "效果转发 panic 必须原样上浮"
    );

    let report = drained(fx.store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(report.queued_actions, 0, "Drained 时活跃队列必空");
    assert_eq!(report.deferred_actions, 0, "本例无停放记录");
    assert_eq!(
        report.pending_effects.effects, 2,
        "3 条效果中第 0 条已被中断 ⇒ 后缀是 1..3 共 2 条"
    );
    assert_eq!(
        report.pending_effects.segments,
        vec![PendingSegmentSummary {
            origin_action_seq: 0, // 首个 action 的入队序号（store 内单调，锁内分配）
            next_effect_index: 1,
            effects: 2,
        }],
        "段级身份 = 产出 action 的入队序号 + 段内起点（禁伪造 dispatch seq）"
    );

    let interrupted = report.interrupted_effect.expect("必须留下中断归属");
    assert_eq!(interrupted.effect_index, 0, "中断的是第 0 条效果");
    assert_eq!(interrupted.effect_kind, EffectKind::Run);
    assert_eq!(interrupted.effect_id, Some(EffectId::new(1)));
    assert_eq!(
        interrupted.origin_action_seq, interrupted.dispatch_action_seq,
        "同一 action 内产者/触发者序号相等"
    );
    assert_eq!(interrupted.origin_action_seq, 0);

    // 报告只读：后缀仍被持有、可再次检视（不因一次报告消失）。
    let again = drained(fx.store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(again, report, "报告必须是幂等的只读快照");
    assert_eq!(
        fx.calls.load(Ordering::SeqCst),
        1,
        "报告/关闭不得执行宿主效果代码"
    );
}

/// ② 重入：本 store 排空帧内调用返回 `Reentrant`（不带快照），且准入**已关**。
#[test]
fn reentrant_close_report_claims_no_final_snapshot_and_closes_admission() {
    let slot: StoreSlot = Arc::new(Mutex::new(None));
    let seen = Arc::new(Mutex::new(None));
    let store = Arc::new(SyncStore::new(
        Producer {
            per_action: 0,
            gate: None,
            reentrant: Some((Arc::clone(&slot), Arc::clone(&seen))),
        },
        PanicOnFirst {
            calls: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(Vec::new())),
        },
    ));
    *slot.lock().expect("slot") = Some(Arc::downgrade(&store));

    store.dispatch(GATED); // 若自等，本条测试挂死
    assert_eq!(
        *seen.lock().expect("seen"),
        Some(CloseReportOutcome::Reentrant),
        "排空帧内必须报 Reentrant（无快照）"
    );

    // 闭门先于重入判定：Reentrant 之后准入确已关闭。
    let before = store.rejected_after_close();
    store.dispatch(7);
    assert_eq!(
        store.rejected_after_close(),
        before + 1,
        "Reentrant 的语义是「准入已关、但不能自等」"
    );
}

/// ③ 显式丢弃：精确 count/identities，**不跑宿主效果代码**；丢弃后再报告为 0。
#[test]
fn discard_reports_exact_identities_and_runs_no_effect_code() {
    let fx = gated_fixture(3);
    let worker = spawn_gated_dispatch(&fx.store);
    fx.entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");
    fx.release.send(()).expect("放行 reducer");
    assert!(worker.join().expect("worker 线程"));

    let report = drained(fx.store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(report.pending_effects.effects, 2);
    let calls_before = fx.calls.load(Ordering::SeqCst);
    assert_eq!(calls_before, 1, "前置：只有那条被中断的调用");

    let discarded = fx
        .store
        .discard_pending_effects()
        .expect("关闭且静止后必须可显式丢弃");
    assert_eq!(
        discarded, report.pending_effects,
        "丢弃报告必须与终态报告逐字一致（精确 count + identities）"
    );
    assert_eq!(
        fx.calls.load(Ordering::SeqCst),
        calls_before,
        "丢弃绝不执行宿主 EffectRuntime 代码"
    );

    let after = drained(fx.store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(after.pending_effects.effects, 0, "丢弃后后缀为空");
    assert!(after.pending_effects.segments.is_empty());
    assert_eq!(
        after.interrupted_effect, report.interrupted_effect,
        "被中断效果是历史归属，不随后缀丢弃而消失"
    );
    assert!(
        fx.store.discard_pending_effects().is_some(),
        "空后缀的丢弃是幂等的空操作（仍可调用）"
    );
}

/// ④ 未关闭的 store 上显式丢弃 = 拒绝（`None`），不偷偷丢活工作。
#[test]
fn discard_on_a_running_store_is_refused() {
    let store = SyncStore::new(Counter, MustNotRun);
    assert!(
        store.discard_pending_effects().is_none(),
        "未关闭的 store 不得被偷偷丢弃（显式丢弃只属于关闭序）"
    );
    // 对照：关闭 + 静止后同一个调用是可用的（拒绝不是永久态）。
    store.close();
    assert!(store.discard_pending_effects().is_some());
}

/// 不产效果的计数器（用于只关心关闭语义的用例）。
struct Counter;

impl Reducer for Counter {
    type State = u64;
    type Action = u64;
    type Op = u64;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, u64>) -> u64 {
        s + a
    }
}

/// 解释器哨兵：本例任何效果被转发到这里都是失败（= 框架跑了不该跑的用户代码）。
struct MustNotRun;

impl EffectRuntime<u64, u64> for MustNotRun {
    fn run(&mut self, _effect: Effect<u64, u64>, _back: &ActionSender<u64>) {
        panic!("本例不应转发任何效果（宿主 EffectRuntime 被意外执行）");
    }
}

/// ⑤ legacy `close()` **不是**效果 flush：它是准入关闭 + best-effort 排空，
///    提交后缀原样留存（本票的观察面才把它显形）。
#[test]
fn legacy_close_leaves_committed_suffix_untouched() {
    let fx = gated_fixture(3);
    let worker = spawn_gated_dispatch(&fx.store);
    fx.entered
        .recv_timeout(Duration::from_secs(5))
        .expect("reducer 未进场");
    fx.release.send(()).expect("放行 reducer");
    assert!(worker.join().expect("worker 线程"));

    fx.store.close(); // 主队列已空 ⇒ 不进转发环（这正是 #61 的洞）
    let report = drained(fx.store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(
        report.pending_effects.effects, 2,
        "legacy close 不 flush 后缀（报告面把它暴露出来）"
    );
    assert_eq!(
        fx.calls.load(Ordering::SeqCst),
        1,
        "close 绝不因收到停机请求就执行宿主效果代码"
    );
}

/// ⑥ population 分离：停放的 deferred action（#49）非零也**不影响** Drained，
///    且不并进效果后缀计数。
struct EmitSelf;

impl Reducer for EmitSelf {
    type State = u64;
    type Action = u64;
    type Op = u64;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, state: &u64, _action: u64, out: &mut EffectBuf<u64, u64>) -> u64 {
        // 无条件回灌自身 ⇒ 深度预算（64）止步 ⇒ 停放一条 deferred record。
        out.push(Effect::Emit(1));
        *state + 1
    }
}

#[test]
fn deferred_actions_are_a_separate_population_from_pending_effects() {
    let store = SyncStore::new(EmitSelf, MustNotRun);
    store.dispatch(1);
    assert!(
        store.feedback_budget_exceeded() > 0,
        "前置：回灌环必须在深度预算处止步"
    );

    let report = drained(store.close_wait_with_report(Duration::from_secs(5)));
    assert_eq!(report.queued_actions, 0);
    assert!(
        report.deferred_actions > 0,
        "停放记录必须显形（不并进活跃队列，也不并进效果后缀）"
    );
    assert_eq!(
        report.pending_effects.effects, 0,
        "停放记录不是效果后缀（两个 population 分账）"
    );
    assert!(report.interrupted_effect.is_none(), "本例无转发中断");
}
