//! 第二消费方（#56 **Stage B**）：任务调度 CLI 宿主——**真实运行**的消费方。
//!
//! 与既有两件的结构差异（刻意不复刻）：
//!
//! - `../src/main.rs` = 全 API 面扫街（编译夹具，逐个 API 走一遍）；
//! - `../workflow/src/main.rs` = 单 store 最小生命线（submit → close，无并发）；
//! - **本件** = 任务调度宿主三段：`submit` / `cancel` / `shutdown`，每段是独立
//!   进程内的一次真实装配 + 运行，带自定义解释器、并发有界准入、组合根状态。
//!
//! # 六条硬要求 → 本文件落点
//!
//! 1. 结构相异：CLI 三段 [`run_cli`] + 每段独立装配（非既有两件的复制）。
//! 2. 自定义解释器：[`RecordingFx`] 实现 [`EffectRuntime`]，覆盖 `Run`/`Cancel`
//!    两形并按转发序落账（[`Executed`]）；未知词表变体显式留痕，不静默丢。
//! 3. 有界准入真运行时：[`DrainGate`] 把排空解释器钉在效果转发点（确定性握手，
//!    无 sleep），并发下 `set_capacity` 打满 ⇒ [`AdmissionOutcome::Full`]。
//! 4. 取消/停机：`SyncStore::close_wait_with_report` 停机身 + 闭门 typed 拒绝；
//!    `tokio` feature 面走 `fx::cancel_pair` / `FxScope`（见 [`async_host`]）。
//! 5. 状态形状：根 [`HostState`] 内 `queue` 切片由 `combine!` 产出的 Fragment
//!    组合承载（[`queue_fragment`]），父层自有 `heartbeats` 分支；快照语义与
//!    `Arc::make_mut` 克隆分支由集成测试用 `Arc::strong_count` 观测。
//! 6. 自带集成测试：`tests/host.rs`（`cargo test --manifest-path …`）。

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Fragment, Reducer, Store as _,
};
use skeleton_infra::runtime::{
    AdmissionOutcome, CloseReportOutcome, CloseWaitOutcome, SyncStore,
};

/// 有界准入并发用例的握手 job：解释器在它的 `Run` 上停住，钉住排空租约。
pub const GATE_JOB: JobId = 0;

/// 任务标识（全局单调，effect id 直接取同值）。
pub type JobId = u64;

// ── 宿主词表：动作 / 副作用 / 状态 ──────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobAction {
    Submit { job: JobId },
    Cancel { job: JobId },
    Finished { job: JobId },
}

/// 根动作：`Job` 路由进分片；`Heartbeat` 是父层自有分支（证明组合路由）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostAction {
    Job(JobAction),
    Heartbeat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobOp {
    Launch { job: JobId },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueueState {
    pub submitted: Vec<JobId>,
    pub cancelled: Vec<JobId>,
    pub finished: Vec<JobId>,
}

/// 根状态：`queue` 是组合分片切片，`heartbeats` 是父层自有切片。
///
/// 组合装配只经 [`queue_fragment`]（`combine!` 产物），分片本身不能作根
/// （`Fragment` 无 `initial`，作根 = E0277）——根契约由 [`HostFeature`] 承担。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostState {
    pub queue: QueueState,
    pub heartbeats: u64,
}

/// 分片 reducer：只服务组合根（`initial` 由父层显式装配时调用）。
struct QueueSlice;

impl Reducer for QueueSlice {
    type State = QueueState;
    type Action = JobAction;
    type Op = JobOp;

    fn initial(&self) -> QueueState {
        QueueState::default()
    }

    fn reduce(
        &self,
        state: &QueueState,
        action: JobAction,
        out: &mut EffectBuf<JobAction, JobOp>,
    ) -> QueueState {
        let mut next = state.clone();
        match action {
            JobAction::Submit { job } => {
                next.submitted.push(job);
                // 效果出参化：解释器不在这里执行，只按转发序收到 Run。
                out.push(Effect::Run {
                    id: EffectId::new(job),
                    op: JobOp::Launch { job },
                });
            }
            JobAction::Cancel { job } => {
                next.cancelled.push(job);
                out.push(Effect::Cancel {
                    id: EffectId::new(job),
                });
            }
            JobAction::Finished { job } => next.finished.push(job),
        }
        next
    }
}

/// 组合分片：`queue` 切片 + 子动作上浮/下沉 + 子效果词表显式上提。
///
/// 子 `Run`/`Cancel` 必须逐条重建到父词表（`Effect<A, Op>` 的 `A` 变了）；
/// 漏掉 lift = 子效果静默丢弃，故这里不省略、也不留空转。
fn queue_fragment() -> impl Fragment<State = HostState, Action = HostAction, Op = JobOp> {
    skeleton_infra::combine!(
        QueueSlice,
        HostState.queue,
        wrap: HostAction::Job,
        unwrap: |action: &HostAction| match action {
            HostAction::Job(job) => Some(job.clone()),
            HostAction::Heartbeat => None,
        },
        lift: |effect, out| match effect {
            Effect::Run { id, op } => out.push(Effect::Run { id, op }),
            Effect::Cancel { id } => out.push(Effect::Cancel { id }),
            _ => {}
        },
    )
}

/// 根 Feature：`initial` 显式装配组合分片初值；`Job` 整条委托分片，
/// `Heartbeat` 走父层自有分支。
pub struct HostFeature;

impl Reducer for HostFeature {
    type State = HostState;
    type Action = HostAction;
    type Op = JobOp;

    fn initial(&self) -> HostState {
        HostState {
            queue: QueueSlice.initial(),
            heartbeats: 0,
        }
    }

    fn reduce(
        &self,
        state: &HostState,
        action: HostAction,
        out: &mut EffectBuf<HostAction, JobOp>,
    ) -> HostState {
        match action {
            HostAction::Heartbeat => HostState {
                queue: state.queue.clone(),
                heartbeats: state.heartbeats.saturating_add(1),
            },
            routed @ HostAction::Job(_) => queue_fragment().reduce(state, routed, out),
        }
    }
}

// ── 自定义解释器：Run + Cancel 两形按转发序落账 ─────────────────────────

/// 解释器执行序的一条记录（`Effect` 非穷尽：未知词表形态走 `Unknown` 留痕）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Executed {
    Run { id: u64, job: JobId },
    Cancel { id: u64 },
    Unknown { debug: String },
}

#[derive(Clone, Default)]
pub struct RecordingFx {
    executed: Arc<Mutex<Vec<Executed>>>,
    gate: Option<Arc<DrainGate>>,
}

impl RecordingFx {
    pub fn new() -> Self {
        Self::default()
    }

    /// 带排空门闩的解释器：`GATE_JOB` 的 `Run` 在效果转发点停住，钉住排空租约。
    pub fn gated(gate: Arc<DrainGate>) -> Self {
        Self {
            executed: Arc::default(),
            gate: Some(gate),
        }
    }

    pub fn executed(&self) -> Vec<Executed> {
        self.executed
            .lock()
            .expect("解释器记录锁未被毒化")
            .clone()
    }
}

impl EffectRuntime<HostAction, JobOp> for RecordingFx {
    fn run(&mut self, effect: Effect<HostAction, JobOp>, _back: &ActionSender<HostAction>) {
        let record = match effect {
            Effect::Run { id, op } => {
                let job = match op {
                    JobOp::Launch { job } => job,
                };
                if let Some(gate) = &self.gate {
                    if job == GATE_JOB {
                        gate.enter_and_wait();
                    }
                }
                Executed::Run { id: id.get(), job }
            }
            Effect::Cancel { id } => Executed::Cancel { id: id.get() },
            // `Effect` 是 `#[non_exhaustive]`：未知变体显式留痕，不静默丢。
            other => Executed::Unknown {
                debug: format!("{other:?}"),
            },
        };
        self.executed
            .lock()
            .expect("解释器记录锁未被毒化")
            .push(record);
    }
}

/// 排空门闩：`enter_and_wait` 在解释器线程宣布「已进入」并等 `release`；
/// 测试侧 `wait_until_entered` 用 condvar 握手（非 sleep），保证 Full 用例
/// 在「排空租约确已被钉住」之后才发并发准入。
#[derive(Default)]
pub struct DrainGate {
    entered: Mutex<bool>,
    entered_cv: Condvar,
    released: Mutex<bool>,
    released_cv: Condvar,
}

impl DrainGate {
    fn enter_and_wait(&self) {
        *self.entered.lock().expect("gate entered 锁未被毒化") = true;
        self.entered_cv.notify_all();

        let mut released = self.released.lock().expect("gate released 锁未被毒化");
        while !*released {
            released = self
                .released_cv
                .wait(released)
                .expect("gate released 等待未被毒化");
        }
    }

    /// 等解释器进入（有限时兜底：握手失败时测试报错而非挂死 CI）。
    pub fn wait_until_entered(&self) {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut entered = self.entered.lock().expect("gate entered 锁未被毒化");
        while !*entered {
            let now = Instant::now();
            assert!(
                now < deadline,
                "30s 内解释器未进入排空门闩（确定性握手失败）"
            );
            // condvar 允许虚假唤醒：回环复检谓词，不把一次唤醒当成功。
            let (guard, timeout) = self
                .entered_cv
                .wait_timeout(entered, deadline - now)
                .expect("gate entered 等待未被毒化");
            entered = guard;
            assert!(
                !timeout.timed_out() || *entered,
                "30s 内解释器未进入排空门闩（确定性握手失败）"
            );
        }
    }

    pub fn release(&self) {
        *self.released.lock().expect("gate released 锁未被毒化") = true;
        self.released_cv.notify_all();
    }
}

// ── CLI 宿主：提交 / 取消 / 停机三段 ────────────────────────────────────

pub const USAGE: &str =
    "usage: cli-host <submit|cancel> <job-id> | shutdown | (--features tokio: cancel-async)";

/// 一进程一命令：每段独立装配 store（真实宿主形态，不藏全局单例）。
pub fn run_cli(args: &[String]) -> Result<String, String> {
    match args {
        [cmd, job] if cmd == "submit" => submit(job),
        [cmd, job] if cmd == "cancel" => cancel(job),
        [cmd] if cmd == "shutdown" => shutdown_phase(),
        _ => Err(String::from(USAGE)),
    }
}

fn parse_job(raw: &str) -> Result<JobId, String> {
    raw.parse::<JobId>()
        .map_err(|err| format!("job id 非法 {raw:?}: {err}"))
}

fn admit(store: &SyncStore<HostState, HostAction, JobOp>, action: HostAction) -> Result<(), String> {
    match store.try_dispatch(action) {
        AdmissionOutcome::Accepted => Ok(()),
        outcome => Err(format!("准入被拒: {outcome:?}")),
    }
}

fn submit(raw: &str) -> Result<String, String> {
    let job = parse_job(raw)?;
    let fx = RecordingFx::new();
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(16);

    admit(
        &store,
        HostAction::Job(JobAction::Submit { job }),
    )?;
    let snapshot = store.snapshot();
    match store.close_wait() {
        CloseWaitOutcome::Drained => {}
        other => return Err(format!("submit {job} 停机未静止: {other:?}")),
    }
    Ok(format!(
        "submit {job}: state={snapshot:?} effects={:?}",
        observed.executed()
    ))
}

fn cancel(raw: &str) -> Result<String, String> {
    let job = parse_job(raw)?;
    let fx = RecordingFx::new();
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(16);

    // 一进程一命令：先登记在途 Run 再发 Cancel（Cancel 寻址需先见 Run id）。
    for action in [JobAction::Submit { job }, JobAction::Cancel { job }] {
        admit(&store, HostAction::Job(action))?;
    }
    let snapshot = store.snapshot();
    match store.close_wait() {
        CloseWaitOutcome::Drained => {}
        other => return Err(format!("cancel {job} 停机未静止: {other:?}")),
    }
    Ok(format!(
        "cancel {job}: state={snapshot:?} effects={:?} unknown_cancel={}",
        observed.executed(),
        store.cancel_unknown_id()
    ))
}

fn shutdown_phase() -> Result<String, String> {
    let fx = RecordingFx::new();
    let observed = fx.clone();
    let store = SyncStore::new(HostFeature, fx);
    store.set_capacity(4);
    admit(&store, HostAction::Job(JobAction::Submit { job: 99 }))?;

    let report = match store.close_wait_with_report(Duration::from_secs(5)) {
        CloseReportOutcome::Drained(report) => report,
        other => return Err(format!("shutdown 未达 Drained: {other:?}")),
    };
    // 闭门后入场必须 typed 拒绝（不是 no-op 静默吞）。
    let post_close = store.try_dispatch(HostAction::Job(JobAction::Finished { job: 99 }));
    if post_close != AdmissionOutcome::Closed {
        return Err(format!("闭门后准入未报 Closed: {post_close:?}"));
    }
    Ok(format!(
        "shutdown: report={report:?} rejected_after_close={} post_close_admission={post_close:?} effects={:?}",
        store.rejected_after_close(),
        observed.executed()
    ))
}

// ── async-enabled 面（tokio feature）：fx 取消可见 ──────────────────────

#[cfg(feature = "tokio")]
pub mod async_host;
