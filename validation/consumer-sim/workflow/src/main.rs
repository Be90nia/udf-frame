//! 第二消费方（#56 Stage A）：CLI/workflow 型宿主骨架——**sync-only 面**。
//!
//! 结构相异点（对照 `../src/main.rs` 的全 API 面扫街）：本件是真实宿主形态，
//! 只经**公开 API** 走一条最小 workflow 生命线，不 require 内部 cfg、不触私有模块：
//!
//! 1. 自定义 [`EffectRuntime`] 实现：把执行过的 `EffectId`/效果形态记进 `Vec`
//!    （不接真实 I/O、不 spawn、不 `await`）。
//! 2. 有界准入：`SyncStore::set_capacity` + `try_dispatch -> AdmissionOutcome`
//!    （后者非 `non_exhaustive` ⇒ 跨 crate 可穷尽 match，编译期即证）。
//! 3. graceful shutdown：`Store::close` / `is_closed` 停机闸 + 闭门后 typed 拒绝。
//!
//! 边界：本切片交付**编译级证据 + 依赖树证据**；并发下的 `Full` 路径、停机排空
//! 时序等 runtime 行为属 **#56 Stage B**。

use std::sync::{Arc, Mutex};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, SyncStore};

// ── 宿主词表：workflow 动作 / 副作用 / 状态 ──────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkflowAction {
    Submit { job: u32 },
    JobDone { job: u32 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JobOp {
    Launch { job: u32 },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct WorkflowState {
    submitted: Vec<u32>,
    done: Vec<u32>,
}

/// 根 Feature（完整 `Reducer`：有 initial，可作 `SyncStore::new` 的根）。
struct Workflow;

impl Reducer for Workflow {
    type State = WorkflowState;
    type Action = WorkflowAction;
    type Op = JobOp;

    fn initial(&self) -> WorkflowState {
        WorkflowState::default()
    }

    fn reduce(
        &self,
        state: &WorkflowState,
        action: WorkflowAction,
        out: &mut EffectBuf<WorkflowAction, JobOp>,
    ) -> WorkflowState {
        let mut next = state.clone();
        match action {
            WorkflowAction::Submit { job } => {
                next.submitted.push(job);
                // 副作用出参化：交给解释器 launch（解释器不在此处执行）。
                out.push(Effect::Run {
                    id: EffectId::new(u64::from(job)),
                    op: JobOp::Launch { job },
                });
            }
            WorkflowAction::JobDone { job } => next.done.push(job),
        }
        next
    }
}

// ── 自定义解释器：只落记录，不接真实 I/O ─────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
struct Recorded {
    kind: String,
    id: Option<u64>,
    op: Option<JobOp>,
}

#[derive(Clone, Default)]
struct RecordingFx {
    executed: Arc<Mutex<Vec<Recorded>>>,
}

impl RecordingFx {
    fn executed(&self) -> Vec<Recorded> {
        self.executed.lock().expect("recorder 锁未被毒化").clone()
    }
}

impl EffectRuntime<WorkflowAction, JobOp> for RecordingFx {
    fn run(&mut self, effect: Effect<WorkflowAction, JobOp>, _back: &ActionSender<WorkflowAction>) {
        // `Effect` 是 `#[non_exhaustive]`：未知变体显式留痕，不静默丢。
        let record = match &effect {
            Effect::Run { id, op } => Recorded {
                kind: String::from("run"),
                id: Some(id.get()),
                op: Some(*op),
            },
            Effect::Cancel { id } => Recorded {
                kind: String::from("cancel"),
                id: Some(id.get()),
                op: None,
            },
            _ => Recorded {
                kind: format!("{effect:?}"),
                id: None,
                op: None,
            },
        };
        self.executed
            .lock()
            .expect("recorder 锁未被毒化")
            .push(record);
    }
}

/// `AdmissionOutcome` **非** `non_exhaustive` ⇒ 外部 crate 可穷尽 match（无通配臂
/// 即编译通过 = 该面的编译级证据；加变体将在此处报 E0004）。
fn describe_admission(outcome: AdmissionOutcome) -> &'static str {
    match outcome {
        AdmissionOutcome::Accepted => "accepted",
        AdmissionOutcome::Full => "full",
        AdmissionOutcome::Closed => "closed",
    }
}

fn main() {
    let fx = RecordingFx::default();
    let observed = fx.clone();
    let store = SyncStore::new(Workflow, fx);

    // 有界准入配置：容量只约束「等待被 reduce 的 action 数」（0 = 不限）。
    store.set_capacity(4);

    assert_eq!(
        describe_admission(store.try_dispatch(WorkflowAction::Submit { job: 1 })),
        "accepted"
    );
    // 静默态：try_dispatch 夺得排空权并同步排空至 idle ⇒ 返回时效果已被记录。
    assert_eq!(
        observed.executed(),
        vec![Recorded {
            kind: String::from("run"),
            id: Some(1),
            op: Some(JobOp::Launch { job: 1 }),
        }]
    );
    assert_eq!(store.snapshot().submitted, vec![1]);

    // graceful shutdown：单向停机闸，置位后排空至 idle；此后入口 typed 拒绝。
    store.close();
    assert!(store.is_closed());
    assert_eq!(
        describe_admission(store.try_dispatch(WorkflowAction::JobDone { job: 1 })),
        "closed"
    );
    assert!(store.rejected_after_close() >= 1);

    // 容量读数（`Full` 路径是并发行为，属 #56 Stage B）。
    assert_eq!(store.admission_full_count(), 0);

    println!("workflow_consumer: OK");
}
