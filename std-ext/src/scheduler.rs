//! 同步任务调度器：延迟（after）/周期（every）任务排程，Clock 注入。
//!
//! 常驻零依赖件：纯 std，无 feature 门。**双层形态**（拍板点，论证见
//! scheduler 域性能锚（本地留存，未入库））：
//!
//! - **确定性内核（线程无关）**：最小堆（`BinaryHeap`+`Reverse`）按注入
//!   [`Clock`] 的单调面（`now_instant`）排程；[`Scheduler::tick`] 在调用
//!   线程同步点火全部到期任务并返回实际点火数。测试配 [`skeleton::clock::MockClock`]：
//!   `advance`+`tick` 零真实 sleep，时间线逐毫秒可断言（retry 域 Sleeper
//!   同哲学的调度版）；
//! - **驱动线程（生产外挂）**：[`Scheduler::spawn_worker`] 起单线程驱动，
//!   condvar park 到最近 deadline（零自旋），注册/关闭即时唤醒。驱动线程
//!   属真实时钟面——MockClock 时间只在显式 `advance` 时流动，park 它无
//!   意义（tokio auto-advance 陷阱的反向对表：零自动前进，确定性面走
//!   tick）。OS 线程创建失败可观察：走 [`Scheduler::try_spawn_worker`] 的
//!   `io::Error`（失败零残留、调度器仍开放）；[`Scheduler::spawn_worker`]
//!   是失败带因 panic 的便利入口（issue#87）。
//!
//! 姿态（骨架哲学：fail-visible/显式/禁静默；语义对标 JDK
//! ScheduledThreadPoolExecutor 双形态、tokio `MissedTickBehavior`（gotchas
//! D14：默认 Burst 连发追赶是坑，默认必须不补发）、Linux timerfd 的
//! overrun 补发计数语义——本件显式不采纳补发）：
//!
//! - **every 语义 = 锚定周期，超载退化固定延迟**（拍板点）：重臂锚
//!   `prev_deadline + period`（零漂移）；回调超载（完成时刻已越过下锚）
//!   则改锚 `完成时刻 + period`——无补发无追帧，持续超载退化为 JDK
//!   scheduleWithFixedDelay 形态，绝不连发追赶；
//! - **cancel 线性化点 = 状态原子位**：CAS 置位成功即承诺「此后不再点火」；
//!   已出堆条目的检查-执行窗口内落地的取消不可撤（本次照跑，文档化
//!   语义；单线程 tick 确定性面无此窗口）；执行中取消 ⇒ 周期任务不再
//!   重臂。`cancel` 幂等；句柄 RAII（Drop 即**静默** cancel：取消语义相同、
//!   不发 tracing 事件——宿主 subscriber 回调不得在析构里同步执行），
//!   [`TaskHandle::detach`]
//!   是 fire-and-forget 逃生门（只解除取消、不接管所有权，TaskCell 随之
//!   回收——#55）；
//! - **close = 关闸 + 清 pending + 在途跑完**（拍板点）：注册显式
//!   `Err(Closed)` 禁静默；pending 全部标 Cancelled（句柄侧可见）；在途
//!   回调跑完为止（同步代码无中断面），驱动线程随后退出
//!   （[`WorkerHandle::join`] = close + join；丢弃句柄（Drop）= 只停该
//!   worker 的 per-worker 停信标——不关闸、不 join，其余 worker 与既有登记
//!   不受影响——#72）；
//! - **回调 panic 不杀驱动**：点火 catch_unwind 摘除 + [`Scheduler::panic_count`]
//!   留痕（池审计回调同款纪律）；周期任务 panic 后摘除不再臂（JDK 语义）；
//!   驱动循环体另有兜底 catch（正常零命中）——「驱动线程 panic 静默死亡 =
//!   全部任务静默停摆」的坑被结构性排除；
//! - **时钟回拨**：deadline 全程走单调面（SystemClock=Instant 不回拨；
//!   MockClock 后退在类型层不可表达）——回拨坑不存在；**前跳**（advance
//!   大步 / Instant 前跳）= 到期即火 + 周期任务按超载规则重锚（不补跳空
//!   期，测试钉死）；
//! - **重入安全**：点火在堆锁外——回调内 after/every/cancel/close 合法
//!   （无自死锁），测试钉死；
//! - **溢出 fail-closed**：注册/重臂恒 `checked_add`（clock.rs armor：
//!   Instant + 巨大 Duration 平台 panic 坑），溢出带因 panic；
//! - **有界准入**（#58）：[`Scheduler::set_capacity`] 设活跃注册数上限
//!   （0=无界；默认不盲设有界默认值——#56 已评审并拍板保持无界
//!   2026-11-23，见 `docs/reference.md` §资源上限与默认值的决定记录）；
//!   触顶 ⇒ typed 拒绝（[`AdmissionError::Full`]，任务**不入堆**、不部分
//!   插入、禁静默丢回调）；额度随注册退役（点火/取消/关闸/panic）**恰好
//!   一次**释放；既有 `after`/`every` 触顶时 panic（fail-visible），typed
//!   拒绝面走 `try_after`/`try_every`；
//! - **惰性墓碑 + 阈值清扫**（#52）：cancel 不摘堆（O(1)），死亡条目到点
//!   弹出时摘除；驱动 park 锚跳过堆顶墓碑（墓碑不是有效 deadline ⇒ 驱动
//!   不为它多醒一拍）；墓碑数越过「绝对下限 + 比例阈值」时清扫堆（只留
//!   PENDING 条目，被摘条目移出锁外析构）⇒ 已取消的远期条目在原 deadline
//!   之前回收；执行语义不变（已出堆检查-执行窗口内落地的取消仍不可撤、
//!   仍照跑）。
//!
//! log-embed 埋点（`log-embed` feature，默认关=宏消除零开销）：target
//! `skeleton::scheduler::{register,cancel,close,fire,worker}`。级别规范
//! （骨架默认：正常流转=debug / 异常路径=warn / 终态=info）：注册/取消/
//! 点火/墓碑跳过=debug；关闸拒绝注册、容量触顶拒绝注册、回调 panic、
//! 超载退化重臂、驱动内部 panic=warn；close（清 pending）与 worker 退出=info。seq（堆条目
//! 序号）进字段承载 register↔fire 跨事件关联；埋点全部在状态锁外发射
//!（订阅方 IO 不持锁，坑面同「睡眠持锁」）。
//!
//! # Examples
//!
//! MockClock 确定性时间线（零真实 sleep）：
//!
//! ```
//! use std::sync::atomic::{AtomicU32, Ordering};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use skeleton::clock::MockClock;
//! use std_ext::scheduler::Scheduler;
//!
//! let clock = Arc::new(MockClock::new());
//! let sched = Scheduler::new(clock.clone());
//! let hits = Arc::new(AtomicU32::new(0));
//! let hits2 = Arc::clone(&hits);
//! sched
//!     .after(Duration::from_millis(100), move || {
//!         hits2.fetch_add(1, Ordering::SeqCst);
//!     })
//!     .unwrap()
//!     .detach();
//! clock.advance(Duration::from_millis(100));
//! assert_eq!(sched.tick(), 1);
//! assert_eq!(hits.load(Ordering::SeqCst), 1);
//! assert_eq!(sched.tick(), 0); // 一次性任务不复火
//! ```

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering as AtomicOrd};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use skeleton::clock::Clock;

// ── 任务态原子词（终态不可逆；cancel 的线性化点）──

const PENDING: u8 = 0;
const CANCELLED: u8 = 1;
const DONE: u8 = 2;

/// 关闸后注册的显式拒绝（fail-visible 禁静默）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Closed;

impl fmt::Display for Closed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "scheduler closed: registration rejected（关闸后注册显式拒绝）"
        )
    }
}

impl std::error::Error for Closed {}

/// 有界准入的 typed 拒绝（issue#58）：过载**不** panic、**不**静默丢回调。
///
/// 与既有 admission 面同族：`SyncStore::try_dispatch` 的
/// `AdmissionOutcome::{Accepted, Full, Closed}`、`FxScope` 的
/// `AdmissionError::{Closed, PendingFull, TrackedFull}`——本域只取关闸与
/// 触顶两个成因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// 关闸后注册（与 [`Closed`] 同义）。
    Closed,
    /// 活跃注册数达上界（[`Scheduler::set_capacity`]）——任务**未入堆**
    /// （不部分插入）；拒绝计数见 [`Scheduler::rejected_registrations`]。
    Full,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(
                f,
                "scheduler closed: registration rejected（关闸后注册显式拒绝）"
            ),
            Self::Full => write!(
                f,
                "scheduler registration capacity exhausted（活跃注册数达上界）"
            ),
        }
    }
}

impl std::error::Error for AdmissionError {}

/// 墓碑清扫阈值（#52）——绝对下限：墓碑数达此值才可能触发清扫（少量取消
/// 不触发 O(n) 堆重建）。
const TOMBSTONE_FLOOR: usize = 16;

/// 墓碑清扫阈值（#52）——比例阈值：墓碑数 × 此值 ≥ 堆条目数 ⇒ 触发清扫
/// （大堆里的少量墓碑不长期滞留，也不频繁重建）。
const TOMBSTONE_RATIO: usize = 4;
/// 任务胞：状态词 + 任务体（cancel 的线性化点就是 `state` 上的 CAS）。
struct TaskCell {
    state: AtomicU8,
    kind: TaskKind,
}

enum TaskKind {
    /// 一次性：点火即终态（Done）。`take` 语义（stable 上面板：`&Box<dyn
    /// FnOnce>` 不可 move 调用，Mutex<Option<_>> 是 tokio/async-std 同款
    /// 内部解法）——回调体只可能被取出执行一次，双执行结构性排除。
    Once(std::sync::Mutex<Option<Box<dyn FnOnce() + Send + Sync + 'static>>>),
    /// 周期：锚定重臂（超载退化固定延迟）；panic 摘除不再臂。
    Every {
        period: Duration,
        fire: Box<dyn Fn() + Send + Sync + 'static>,
    },
}

/// 堆条目：deadline 升序 + seq 升序（同 deadline 按注册序 FIFO 平局）。
struct Entry {
    deadline: Instant,
    seq: u64,
    task: Arc<TaskCell>,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.deadline == other.deadline && self.seq == other.seq
    }
}

impl Eq for Entry {}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        // 包 Reverse 入最大堆后 = deadline 早 / seq 小者先出。
        self.deadline
            .cmp(&other.deadline)
            .then(self.seq.cmp(&other.seq))
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct HeapState {
    heap: BinaryHeap<Reverse<Entry>>,
    closed: bool,
    next_seq: u64,
}

/// 驱动线程名（诊断面：线程列表可归因谁在跑；多 worker 共用同名——worker
/// 数不计数，无编号可加，#87 明确不引入 worker 账本）。
const WORKER_THREAD_NAME: &str = "scheduler-worker";

/// OS 线程创建注入缝（issue#87，仅测试替换）：生产恒 [`spawn_worker_thread`]。
///
/// `fn` 指针而非 `dyn Fn`：开销只在起 worker 时一次间接调用（起 worker 是
/// 冷路径；注册/点火热路径零成本）。
type SpawnFn = fn(&str, Box<dyn FnOnce() + Send>) -> std::io::Result<JoinHandle<()>>;

/// 生产 spawner（issue#87）：`Builder::spawn` 把 OS 线程创建失败作为
/// `io::Result` 返回（裸 `thread::spawn` 会 panic ⇒ 调用方无从恢复）。
fn spawn_worker_thread(
    name: &str,
    body: Box<dyn FnOnce() + Send>,
) -> std::io::Result<JoinHandle<()>> {
    thread::Builder::new().name(name.to_owned()).spawn(body)
}

/// 容量发布窗口停车闸（**仅 `cfg(test)` 构建存在**，issue#95）：把 `register`
/// 停在「已按旧 cap 裁决、尚未入堆」的状态锁内窗口里，供 `set_capacity` 交错的
/// 确定性测试。生产构建无本类型、热路径无任何检查。
#[cfg(test)]
struct CapacitySettle {
    state: Mutex<SettleState>,
    cv: Condvar,
}

#[cfg(test)]
struct SettleState {
    armed: bool,
    entered: bool,
}

#[cfg(test)]
impl CapacitySettle {
    fn new() -> Self {
        Self {
            state: Mutex::new(SettleState {
                armed: false,
                entered: false,
            }),
            cv: Condvar::new(),
        }
    }

    /// 停靠点调用（**持 `shared.state` 锁时进入**）：布防则置进入位并 park 到放行。
    fn park(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.armed {
            return;
        }
        state.entered = true;
        self.cv.notify_all();
        while state.armed {
            state = self.cv.wait(state).expect("capacity gate poisoned");
        }
    }

    /// 测试面：布防（清上一轮进入位）。
    fn arm(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.armed = true;
        state.entered = false;
    }

    /// 测试面：有界等待停靠线程进入窗口（OS 时钟 Condvar 超时，非 sleep）。
    fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        while !state.entered {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, _) = self
                .cv
                .wait_timeout(state, deadline - now)
                .expect("capacity gate poisoned");
            state = next;
        }
        true
    }

    /// 测试面：放行并解除布防（后续注册不再停靠）。
    fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.armed = false;
        self.cv.notify_all();
    }
}

struct Shared {
    clock: Arc<dyn Clock>,
    // ponytail: 单全局状态锁，注册竞争下吞吐负扩展（multidim ③ 实测
    // 6.2M→2.9M ops/s @1→8 线程）——deadline 全序要求堆顶唯一，分片破坏
    // 语义；驱动线程单线程形态下注册竞争非热路径，分片锁/无锁堆留待真实
    // 多注册方负载出现再升。
    state: Mutex<HeapState>,
    /// 注册/关闭/取消唤醒（驱动线程 park 早醒源；取消唤醒只为墓碑清扫后
    /// 重算 park 锚——#52）。
    signal: Condvar,
    panics: AtomicU64,
    worker_passes: AtomicU64,
    /// 活跃注册数（#58）：未退役的注册数——堆内待执行条目 + **执行中**的
    /// 周期任务（它仍持有一条注册，除非被取消否则会被重臂）。
    ///
    /// **不等于 `heap.len()`**：堆内还含已取消的惰性墓碑（#52 清扫前）。
    live: AtomicUsize,
    /// 活跃注册数上限（#58）：0 = 无界（默认形态——不盲设有界默认值；#56
    /// 已评审并拍板保持无界 2026-11-23，见 `docs/reference.md` §资源上限与
    /// 默认值的决定记录）。与 `SyncStore::set_capacity` 同款口径。
    capacity: AtomicUsize,
    /// 因容量上界被拒的注册次数（#58；闸关拒发不计入，同 `FxCounts::rejected`）。
    rejected: AtomicU64,
    /// 注册停靠闸（**仅 `cfg(test)`**，issue#95）：`register` 在「已按旧 cap
    /// 裁决、尚未入堆」的状态锁窗口停靠——`set_capacity` 此刻必被同一把锁挡住。
    #[cfg(test)]
    register_gate: CapacitySettle,
    /// 已取消但可能仍在堆内的条目数（#52，近似计数）：取消 O(1) 递增，
    /// 清扫后归零——漂移至多「清扫期间落地的那次取消」，良性。
    tombstones: AtomicUsize,
    /// OS 线程创建注入缝（issue#87）：生产恒 [`spawn_worker_thread`]，测试换
    /// 成失败桩以确定性模拟创建失败。`fn` 指针 ⇒ 只在起 worker 时一次间接
    /// 调用（热路径零成本）。
    spawn: SpawnFn,
}

/// 埋点 kind 字段值（公开入口名：Once=after / Every=every）。log-embed 门内
/// 纯函数，feature 关时整体不存在。
#[cfg(feature = "log-embed")]
fn task_kind_name(kind: &TaskKind) -> &'static str {
    match kind {
        TaskKind::Once(_) => "after",
        TaskKind::Every { .. } => "every",
    }
}

/// 状态锁：中毒恢复（`into_inner`）——锁内零用户代码（回调点火/堆摘除全
/// 在锁外），毒位只可能来自框架自身 bug；状态无跨调用不变量可破坏
/// （close 后 heap 恒空 + closed 恒真），恢复继续（clock.rs 毒锁纪律同款）。
fn lock(state: &Mutex<HeapState>) -> MutexGuard<'_, HeapState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Shared {
    /// 退役一次注册（#58）：CAS `PENDING`→终态；**CAS 赢家释放额度**
    /// （恰好一次——取消 / 点火 / 关闸 / panic 四条退役路径共用此闸门，
    /// 杜绝重复释放与漏释放）。
    fn retire(&self, cell: &TaskCell, terminal: u8) -> bool {
        let won = cell
            .state
            .compare_exchange(PENDING, terminal, AtomicOrd::SeqCst, AtomicOrd::SeqCst)
            .is_ok();
        if won {
            self.live.fetch_sub(1, AtomicOrd::SeqCst);
        }
        won
    }
}
/// 同步任务调度器（`Arc` 内脏，[`Clone`] 廉价共享；注册/点火/关闭线程安全）。
///
/// 见[模块文档](self)双层形态与语义拍板。
///
/// **生产驱动启动失败可观察**（issue#87）：[`Scheduler::try_spawn_worker`]
/// 把 OS 线程创建失败作为 `io::Error` 原样返回（失败零残留、调度器保持
/// 开放）；[`Scheduler::spawn_worker`] 是 fail-visible 便利入口（失败带因
/// panic）。启动失败后确定性内核不受影响——注册照常受理，显式
/// [`Scheduler::tick`] 仍按 deadline 点火。
#[derive(Clone)]
pub struct Scheduler {
    shared: Arc<Shared>,
}

impl fmt::Debug for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scheduler")
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl Scheduler {
    /// 新调度器：时间源注入（生产 [`SystemClock`](skeleton::clock::SystemClock)，
    /// 测试 [`MockClock`](skeleton::clock::MockClock) 确定性时间线）。
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self::build(clock, spawn_worker_thread)
    }

    /// 注入缝构造（issue#87，仅测试）：换掉 OS 线程创建实现，确定性模拟创建
    /// 失败（不依赖真实 OS 线程资源耗尽）。
    #[cfg(test)]
    fn with_spawn(clock: Arc<dyn Clock>, spawn: SpawnFn) -> Self {
        Self::build(clock, spawn)
    }

    fn build(clock: Arc<dyn Clock>, spawn: SpawnFn) -> Self {
        Self {
            shared: Arc::new(Shared {
                clock,
                state: Mutex::new(HeapState {
                    heap: BinaryHeap::new(),
                    closed: false,
                    next_seq: 0,
                }),
                signal: Condvar::new(),
                panics: AtomicU64::new(0),
                worker_passes: AtomicU64::new(0),
                live: AtomicUsize::new(0),
                capacity: AtomicUsize::new(0), // 0 = 无界（默认）
                rejected: AtomicU64::new(0),
                #[cfg(test)]
                register_gate: CapacitySettle::new(),
                tombstones: AtomicUsize::new(0),
                spawn,
            }),
        }
    }

    /// 登记一次性任务：`delay` 后点火一次。`delay=0` 合法（下个 tick 即火）。
    ///
    /// **点火前提（防静默）**：注册仅入堆——生产形态必须 [`Scheduler::spawn_worker`]
    /// 起驱动线程（或确定性测试面显式 [`Scheduler::tick`]），否则任务**永不点火**
    /// 且注册返回 `Ok` 无提示。
    ///
    /// # Panics
    ///
    /// `delay` 过大致 `now + delay` 溢出 Instant 表示范围（clock.rs armor
    /// 同款 fail-closed 带因 panic）。
    ///
    /// 触顶（[`Scheduler::set_capacity`] 配了上界且活跃注册数达上界）：
    /// **panic**（fail-visible，禁静默丢回调）——需要 typed 拒绝的调用方走
    /// [`Scheduler::try_after`]。
    pub fn after(
        &self,
        delay: Duration,
        task: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<TaskHandle, Closed> {
        match self.try_after(delay, task) {
            Ok(handle) => Ok(handle),
            Err(AdmissionError::Closed) => Err(Closed),
            // 触顶 = 过载（fail-visible）：禁静默丢回调（issue#58）——typed
            // 拒绝面走 `try_after`/`try_every`。
            Err(AdmissionError::Full) => self.panic_capacity_full(),
        }
    }

    /// 登记周期任务：首火在 `period` 后（非注册即火），此后按锚定周期重臂
    /// （超载退化固定延迟，无补发——模块头注「every 语义」节）。
    ///
    /// **点火前提（防静默）**：同 [`Scheduler::after`]——注册仅入堆，需
    /// [`Scheduler::spawn_worker`]（生产）或 [`Scheduler::tick`]（确定性测试），
    /// 否则**永不点火**且 `Ok` 无提示。
    ///
    /// # Panics
    ///
    /// `period` 为零：零周期=忙等风暴，fail-closed 拒绝（RetryPolicy 零退避
    /// 同款）。
    ///
    /// 触顶（[`Scheduler::set_capacity`] 配了上界且活跃注册数达上界）：
    /// **panic**（fail-visible，禁静默丢回调）——需要 typed 拒绝的调用方走
    /// [`Scheduler::try_every`]。
    pub fn every(
        &self,
        period: Duration,
        task: impl Fn() + Send + Sync + 'static,
    ) -> Result<TaskHandle, Closed> {
        match self.try_every(period, task) {
            Ok(handle) => Ok(handle),
            Err(AdmissionError::Closed) => Err(Closed),
            // 触顶 = 过载（fail-visible）：禁静默丢回调（issue#58）——typed
            // 拒绝面走 `try_after`/`try_every`。
            Err(AdmissionError::Full) => self.panic_capacity_full(),
        }
    }
    /// 登记一次性任务（typed 拒绝面，issue#58）：容量触顶 ⇒
    /// [`AdmissionError::Full`]（任务**不入堆**，不部分插入，禁静默丢回调）；
    /// 关闸 ⇒ [`AdmissionError::Closed`]。
    ///
    /// 与 [`Scheduler::after`] 的差别只在拒绝面：`after` 的错误类型是既有
    /// 的 `Closed`（公开面不变），触顶时 **panic**（fail-visible）⇒ 需要
    /// typed 拒绝的调用方走本方法。
    pub fn try_after(
        &self,
        delay: Duration,
        task: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<TaskHandle, AdmissionError> {
        self.register(
            delay,
            TaskKind::Once(std::sync::Mutex::new(Some(Box::new(task)))),
        )
    }

    /// 登记周期任务（typed 拒绝面，issue#58）：语义同 [`Scheduler::try_after`]。
    ///
    /// # Panics
    ///
    /// `period` 为零：零周期=忙等风暴，fail-closed 拒绝。
    pub fn try_every(
        &self,
        period: Duration,
        task: impl Fn() + Send + Sync + 'static,
    ) -> Result<TaskHandle, AdmissionError> {
        assert!(
            period > Duration::ZERO,
            "every 零周期=忙等风暴，fail-closed 拒绝（RetryPolicy 零退避同款）"
        );
        self.register(
            period,
            TaskKind::Every {
                period,
                fire: Box::new(task),
            },
        )
    }
    /// 触顶 fail-visible（issue#58）：既有 `after`/`every` 的错误类型是
    /// `Closed`（公开面不变）⇒ 触顶只能 panic（禁静默丢回调）；typed 拒绝
    /// 面走 `try_after`/`try_every`。
    fn panic_capacity_full(&self) -> ! {
        panic!(
            "scheduler registration capacity exhausted (capacity={} live={})：\
             需要 typed 拒绝请用 try_after/try_every",
            self.shared.capacity.load(AtomicOrd::SeqCst),
            self.shared.live.load(AtomicOrd::SeqCst),
        );
    }

    /// 活跃注册数上限（issue#58）：`cap` = 未退役注册数上限；**0 = 无界**
    /// （默认形态——不盲设有界默认值；#56 已评审并拍板保持无界 2026-11-23，
    /// 见 `docs/reference.md` §资源上限与默认值的决定记录）。
    ///
    /// 计数口径（含什么、不含什么）：
    ///
    /// - **含**：堆内待执行条目；**执行中**的周期任务（它仍持有一条注册，
    ///   除非被取消否则会被重臂）；
    /// - **不含**：已点火 / 已取消 / 已 panic 退役的条目（额度已释放）；
    /// - **≠ `heap.len()`**：堆内还含已取消的惰性墓碑（#52 清扫前）。
    ///
    /// 与 `SyncStore::set_capacity` 同款口径（0 = 无界）；调低到当前活跃数
    /// 以下不驱逐既有注册，只让后续注册被拒。
    ///
    /// # 与注册的线性化（issue#95）
    ///
    /// **线性化点 = `state` 锁内的发布**：本方法先取状态锁再写新值，而
    /// `Scheduler::register` 在**同一把锁**内读 cap 并完成检查 + 入堆 ⇒ 二者
    /// 只有两种合法先后（不存在「注册读了旧 cap、本方法先返回、该注册随后仍按
    /// 旧 cap 成功」的交错）：
    ///
    /// - 注册先得锁：该次按**旧** cap 判定并入堆，本方法随后返回——它线性化在
    ///   本方法之前，**不被回溯取消/驱逐**（即使此刻 `live_registrations() > cap`）。
    /// - 本方法先得锁：其后所有注册按**新** cap 判定。
    ///
    /// **扩容与 `0`（无界）对后续注册立即生效**。退役 / 取消路径仍经 `live` 的
    /// 原子 CAS 释放额度、**不取状态锁**，故本方法不新增锁序边（状态锁零嵌套）。
    pub fn set_capacity(&self, cap: usize) {
        let _state = lock(&self.shared.state);
        self.shared.capacity.store(cap, AtomicOrd::SeqCst);
    }

    /// 当前活跃注册数上限（0 = 无界）。
    pub fn capacity(&self) -> usize {
        self.shared.capacity.load(AtomicOrd::SeqCst)
    }

    /// 活跃注册数（issue#58 诊断面）：未退役的注册数（口径见
    /// [`Scheduler::set_capacity`]）。
    pub fn live_registrations(&self) -> usize {
        self.shared.live.load(AtomicOrd::SeqCst)
    }

    /// 因容量上界被拒的注册次数（issue#58 诊断面；累计；闸关拒发不计入）。
    pub fn rejected_registrations(&self) -> u64 {
        self.shared.rejected.load(AtomicOrd::SeqCst)
    }

    /// 注册内窥（after/every 共用）：deadline 锁外合成（时钟锁与状态锁零
    /// 嵌套，序=state→clock 单向），入堆+序号在锁内。
    fn register(&self, delay: Duration, kind: TaskKind) -> Result<TaskHandle, AdmissionError> {
        // deadline 锁外合成：时钟锁与状态锁零嵌套（序=state→clock 单向）。
        let now = self.shared.clock.now_instant();
        let deadline = now
            .checked_add(delay)
            .expect("注册 deadline 溢出 Instant 表示范围（clock.rs armor 同款 fail-closed）");
        #[cfg(feature = "log-embed")]
        let kind_name = task_kind_name(&kind);
        let cell = Arc::new(TaskCell {
            state: AtomicU8::new(PENDING),
            kind,
        });
        let seq;
        {
            let mut s = lock(&self.shared.state);
            if s.closed {
                // 异常路径（拒绝）：先放锁再发事件（订阅方 IO 不持锁）。
                drop(s);
                embed_event!(warn, target: "skeleton::scheduler::register", kind = kind_name, reason = "closed");
                return Err(AdmissionError::Closed);
            }
            // 有界准入（#58）：活跃注册数达上界 ⇒ 显式拒绝，任务**不入堆**
            // （不部分插入）。检查与计数同在状态锁内 ⇒ 并发注册不越上界。
            let cap = self.shared.capacity.load(AtomicOrd::SeqCst);
            // 测试停靠点（issue#95）：已按旧 cap 裁决、入堆尚未提交——
            // `set_capacity` 此刻只能被状态锁挡住。
            #[cfg(test)]
            self.shared.register_gate.park();
            if cap > 0 && self.shared.live.load(AtomicOrd::SeqCst) >= cap {
                drop(s);
                self.shared.rejected.fetch_add(1, AtomicOrd::SeqCst);
                embed_event!(warn, target: "skeleton::scheduler::register", kind = kind_name, reason = "capacity_full");
                return Err(AdmissionError::Full);
            }
            seq = s.next_seq;
            s.next_seq += 1;
            s.heap.push(Reverse(Entry {
                deadline,
                seq,
                task: Arc::clone(&cell),
            }));
            // 额度占用：注册成功即占一条（退役时恰好释放一次，#58）。
            self.shared.live.fetch_add(1, AtomicOrd::SeqCst);
        }
        // 唤醒驱动线程：更早 deadline 的注册即时生效（非睡满旧锚）。
        // Telemetry must not sit before this mandatory wakeup.
        self.shared.signal.notify_all();
        embed_event!(debug, target: "skeleton::scheduler::register", seq = seq, kind = kind_name, delay_ms = delay.as_millis() as u64);
        Ok(TaskHandle {
            cell,
            armed: true,
            sched: Arc::downgrade(&self.shared),
        })
    }

    /// 确定性内核：点火全部到期任务（`deadline <= now`，按注入钟口径），
    /// 返回实际点火数（惰性墓碑不计）。回调在调用线程**锁外**执行（重入
    /// 安全）；回调内推进虚拟钟（MockClock）⇒ 后续到期条目同轮继续火。
    pub fn tick(&self) -> usize {
        // 墓碑清扫（#52）：取消密集时把堆内已取消条目提前回收（不等原
        // deadline）；阈值未达 ⇒ O(1) 早退。
        self.reclaim_cancelled();
        let mut fired = 0;
        loop {
            // 每轮重读 now：回调内推进虚拟钟（MockClock）⇒ 后续到期条目同轮续火。
            let now = self.shared.clock.now_instant();
            let entry = {
                let mut s = lock(&self.shared.state);
                match s.heap.peek() {
                    Some(Reverse(e)) if e.deadline <= now => {
                        s.heap.pop().expect("peek 刚确认非空").0
                    }
                    _ => return fired,
                }
            };
            if self.fire(entry, now) {
                fired += 1;
            }
        }
    }

    /// 点火单条目（调用方已出堆）：锁外执行回调，锁内做终态/重臂裁决。
    /// 返回是否实际执行（出堆前已 cancel 的惰性墓碑=静默丢弃不执行）。
    fn fire(&self, entry: Entry, _now: Instant) -> bool {
        // 惰性墓碑：出堆前已 cancel（state 终态）⇒ 静默丢弃不执行。
        if entry.task.state.load(AtomicOrd::SeqCst) != PENDING {
            embed_event!(debug, target: "skeleton::scheduler::fire", seq = entry.seq, skipped = true);
            return false;
        }
        #[cfg(feature = "log-embed")]
        let kind_name = task_kind_name(&entry.task.kind);
        embed_event!(debug, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name);
        match &entry.task.kind {
            TaskKind::Once(run) => {
                // take 后锁外执行：回调体已消费，双执行结构性排除。
                let taken = run.lock().unwrap_or_else(PoisonError::into_inner).take();
                let callback_panicked = if let Some(run) = taken {
                    if catch_unwind(AssertUnwindSafe(run)).is_err() {
                        self.shared.panics.fetch_add(1, AtomicOrd::SeqCst);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                };
                // 终态 DONE：此后 cancel 返 false（测试 cancel_after_fire）。
                // 退役赢家释放额度（#58，恰好一次）：CAS 失败 = 检查-执行
                // 窗口内被 cancel ⇒ 额度已由取消方释放。
                self.shared.retire(&entry.task, DONE);
                if callback_panicked {
                    // The live slot is released before invoking the host subscriber.
                    embed_event!(warn, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name, reason = "panic");
                }
                // CAS 失败=检查-执行窗口内被 cancel：本次照跑（文档化语义）。
            }
            TaskKind::Every { period, fire } => {
                if catch_unwind(AssertUnwindSafe(fire)).is_err() {
                    // panic 摘除不再臂（JDK 语义）；留痕 fail-visible。
                    self.shared.panics.fetch_add(1, AtomicOrd::SeqCst);
                    // 退役（不再臂）⇒ 释放额度（#58，恰好一次）。
                    self.shared.retire(&entry.task, DONE);
                    embed_event!(warn, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name, reason = "panic");
                    return true;
                }
                // 重臂锚取回调完成后的当前时刻：超载（完成时已越下锚）⇒
                // 改锚退化固定延迟，无补发（D14 防线）。
                let now = self.shared.clock.now_instant();
                let next = rearm_deadline(entry.deadline, *period, now);
                // 异常路径（超载退化重臂）：下锚已被完成时刻越过（或锚溢出）
                // ⇒ 周期语义降级为固定延迟，留痕。
                #[cfg(feature = "log-embed")]
                let overloaded = entry
                    .deadline
                    .checked_add(*period)
                    .is_none_or(|anchor| anchor <= now);
                // 锁内裁决重臂：close 或执行中 cancel 赢了 ⇒ 摘除。
                // 残余窗口（裁决后 cancel 落地）由惰性墓碑兜底：取消成功，
                // 该条目到点被墓碑逻辑吃掉。
                let mut s = lock(&self.shared.state);
                if s.closed || entry.task.state.load(AtomicOrd::SeqCst) != PENDING {
                    drop(s);
                    // 退役（关闸或执行中取消 ⇒ 不再重臂）⇒ 释放额度（#58）：
                    // CAS 赢家释放（取消已赢 ⇒ 额度已由取消方释放）。
                    self.shared.retire(&entry.task, DONE);
                    return true;
                }
                s.heap.push(Reverse(Entry {
                    deadline: next,
                    seq: entry.seq,
                    task: Arc::clone(&entry.task),
                }));
                drop(s);
                // 不 notify：重臂只会把 deadline 推后，park 锚提前无害。
                // Publish the new heap entry before invoking the host subscriber.
                #[cfg(feature = "log-embed")]
                if overloaded {
                    embed_event!(warn, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name, reason = "overload_rearm", period_ms = period.as_millis() as u64);
                }
            }
        }
        true
    }

    /// 最近到期时刻（堆顶，**含惰性墓碑**——公共面口径：堆顶原样可见，
    /// 空堆 None）。
    ///
    /// 驱动面用「跳过堆顶墓碑」的口径（内部 park 锚计算）：墓碑不是有效
    /// deadline，驱动不该为它多醒一拍——两口径的差异是刻意的（公共面答
    /// 「堆顶是什么」，驱动面答「下一个要醒的 deadline」）。
    pub fn next_deadline(&self) -> Option<Instant> {
        let s = lock(&self.shared.state);
        s.heap.peek().map(|Reverse(e)| e.deadline)
    }
    /// 驱动 park 锚（#52）：堆顶若是被取消的墓碑 ⇒ 跳过并即时回收——墓碑
    /// 不是有效 deadline，驱动不该为它多醒一拍（取消最早 deadline 后驱动
    /// 须重算 park 锚）。
    ///
    /// 与公共面 [`Scheduler::next_deadline`] 的口径差异是刻意的：公共面
    /// 答「堆顶是什么」（含惰性墓碑），本方法答「下一个要醒的 deadline」。
    fn next_pending_deadline(&self) -> Option<Instant> {
        loop {
            let tombstone = {
                let mut s = lock(&self.shared.state);
                let is_tombstone = s
                    .heap
                    .peek()
                    .is_some_and(|Reverse(e)| e.task.state.load(AtomicOrd::SeqCst) == CANCELLED);
                if is_tombstone {
                    s.heap.pop().map(|Reverse(e)| e)
                } else {
                    None
                }
            };
            match tombstone {
                // 锁外析构：捕获闭包的析构不得持堆锁。
                Some(entry) => drop(entry),
                None => break,
            }
        }
        let s = lock(&self.shared.state);
        s.heap.peek().map(|Reverse(e)| e.deadline)
    }

    /// 关闸：此后注册显式 `Err(Closed)`；清空 pending（全部标 Cancelled，
    /// 句柄侧可见）；在途回调跑完为止。幂等。驱动线程随之退出
    /// （[`WorkerHandle::join`]）。
    ///
    /// 回调内调用合法（重入安全，测试钉死）。
    pub fn close(&self) {
        let drained = {
            let mut s = lock(&self.shared.state);
            if s.closed {
                return; // 幂等
            }
            s.closed = true;
            // 清 pending：堆内全部标 Cancelled（句柄侧可见）；已出堆在途条目
            // 不在堆里，回调跑完为止（同步代码无中断面）。
            let drained = std::mem::take(&mut s.heap);
            // 堆已清空 ⇒ 无墓碑残留（#52 计数归零）。
            self.shared.tombstones.store(0, AtomicOrd::SeqCst);
            drained
        };
        #[cfg(feature = "log-embed")]
        let cleared = drained.len() as u64;
        for Reverse(e) in drained {
            // 全部标 Cancelled（句柄侧可见）；退役赢家释放额度（#58，
            // 恰好一次）。退役与析构均在状态锁外。
            self.shared.retire(&e.task, CANCELLED);
        }
        // 唤醒驱动线程：close 即退（park 中早醒重算）。必须先于同步 subscriber。
        self.shared.signal.notify_all();
        // 终态（close）：清 pending 计数留痕；幂等重入（已 closed 早退）不发。
        embed_event!(info, target: "skeleton::scheduler::close", cleared = cleared);
    }
    /// 墓碑清扫（#52）：已取消但仍在堆内的条目在原 deadline 之前被回收。
    ///
    /// 触发条件：墓碑计数同时越过**绝对下限**（[`TOMBSTONE_FLOOR`]）与
    /// **比例阈值**（墓碑数 × [`TOMBSTONE_RATIO`] ≥ 堆条目数）——少量取消
    /// 不触发 O(n) 堆重建；大堆里的少量墓碑也不长期滞留。
    ///
    /// 清扫在状态锁内重建堆（只保留 PENDING 条目），被摘条目**移出锁外**
    /// 析构（回调捕获的析构不得在堆锁内跑）。返回被回收的条目数。
    fn reclaim_cancelled(&self) -> usize {
        let removed = {
            let mut s = lock(&self.shared.state);
            let tombstones = self.shared.tombstones.load(AtomicOrd::SeqCst);
            if tombstones < TOMBSTONE_FLOOR
                || tombstones.saturating_mul(TOMBSTONE_RATIO) < s.heap.len()
            {
                return 0;
            }
            // 重建堆：只保留 PENDING 条目（取消/终态条目即刻回收）。
            let old = std::mem::take(&mut s.heap);
            let mut kept = Vec::with_capacity(old.len());
            let mut removed = Vec::new();
            for Reverse(e) in old {
                if e.task.state.load(AtomicOrd::SeqCst) == PENDING {
                    kept.push(Reverse(e));
                } else {
                    removed.push(e);
                }
            }
            s.heap = BinaryHeap::from(kept);
            removed
        };
        let reclaimed = removed.len();
        // 锁外析构：捕获闭包的析构不得持堆锁（issue#52 硬要求）。
        drop(removed);
        // 堆内已无墓碑 ⇒ 计数归零（清扫期间落地的那次取消可能漂移，良性）。
        self.shared.tombstones.store(0, AtomicOrd::SeqCst);
        if reclaimed > 0 {
            // 唤醒驱动线程：park 锚（堆顶 deadline）可能提前 ⇒ 重算。
            self.shared.signal.notify_all();
            embed_event!(debug, target: "skeleton::scheduler::fire", reclaimed = reclaimed as u64);
        }
        reclaimed
    }

    pub fn is_closed(&self) -> bool {
        lock(&self.shared.state).closed
    }

    /// 点火 catch_unwind 命中计数（fail-visible 留痕读侧；含驱动循环体
    /// 兜底命中的框架内部 panic——正常零）。
    pub fn panic_count(&self) -> u64 {
        self.shared.panics.load(AtomicOrd::SeqCst)
    }

    /// 驱动线程主循环走过圈数（零自旋证据读侧：idle park 时圈数静止）。
    pub fn worker_passes(&self) -> u64 {
        self.shared.worker_passes.load(AtomicOrd::SeqCst)
    }

    /// 起驱动线程（生产形态）：park 到最近 deadline（零自旋）→ 到期点火；
    /// 注册（更早 deadline）即时唤醒重算。可多起（互抢 pop，典型 1 个）：
    /// 每个 worker 有自己的停信标——丢弃一个 [`WorkerHandle`] 只停它自己，
    /// 不关闸、不影响其余 worker 与既有登记（#72）。
    ///
    /// **无 worker 态**：所有句柄都丢弃而调度器仍开放时，注册继续入堆但不再
    /// 点火，直到再起 worker 或显式 [`Scheduler::close`]。
    ///
    /// MockClock 时间不自动流动，驱动线程对其无意义——确定性测试走
    /// [`Scheduler::tick`]。
    ///
    /// # Panics
    ///
    /// OS 线程创建失败（PID/线程数限额、内存压力等）：**带因 panic**，消息含
    /// 底层 `io::Error` 及其 `kind`。需要恢复面（失败后退回显式
    /// [`Scheduler::tick`] 确定性内核，或把错误传播给宿主启动流程）的调用方走
    /// [`Scheduler::try_spawn_worker`]。
    pub fn spawn_worker(self: &Arc<Self>) -> WorkerHandle {
        self.try_spawn_worker().unwrap_or_else(|err| {
            panic!(
                "scheduler worker thread spawn failed (kind: {:?}): {err}——生产驱动未启动；\
                 需要可恢复错误面请用 try_spawn_worker",
                err.kind()
            )
        })
    }

    /// 起驱动线程（错误面可观察版，issue#87）：语义同
    /// [`Scheduler::spawn_worker`]，但 OS 线程创建失败**原样返回**
    /// `io::Error`（不改写 `kind`、不 panic）。
    ///
    /// 失败**零残留**：只有 OS 线程创建成功才构造 [`WorkerHandle`]——不遗留
    /// 停信标、`JoinHandle` 或对 `Scheduler` 的额外所有权；调度器保持开放、
    /// 注册照常受理、显式 [`Scheduler::tick`] 的确定性内核照常点火（启动失败
    /// 不污染内核，也绝不假装「生产调度已启动」）。
    ///
    /// 典型处理：记录错误后退回 `tick()` 驱动，或向宿主启动流程传播。
    pub fn try_spawn_worker(self: &Arc<Self>) -> std::io::Result<WorkerHandle> {
        // 闭包只持 `Arc<Scheduler>` + 本 worker 停信标（#72）；spawn 失败时
        // std 丢弃闭包 ⇒ 两个克隆随闭包消失（不留残余所有权）。
        let stop = Arc::new(AtomicBool::new(false));
        let body = Box::new({
            let sched = Arc::clone(self);
            let stop = Arc::clone(&stop);
            move || worker_loop(sched, stop)
        });
        let thread = (self.shared.spawn)(WORKER_THREAD_NAME, body)?;
        Ok(WorkerHandle {
            thread: Some(thread),
            sched: Arc::clone(self),
            stop,
        })
    }
}

/// 周期重臂锚：`prev + period`（锚定零漂移）；已越锚（超载）则 `now +
/// period`（退化固定延迟，无补发——D14 防线）。
fn rearm_deadline(prev: Instant, period: Duration, now: Instant) -> Instant {
    match prev.checked_add(period) {
        // 锚定周期（零漂移）：下锚尚未被完成时刻越过。
        Some(next) if next > now => next,
        // 超载（已越锚/溢出）：改锚完成时刻 + period，退化固定延迟。
        _ => now
            .checked_add(period)
            .expect("周期重臂溢出 Instant 表示范围（clock.rs armor 同款 fail-closed）"),
    }
}

/// 任务句柄：RAII cancel（Drop 即静默取消——语义同 [`TaskHandle::cancel`] 但不发
/// 任何 tracing 事件）+ 幂等。
///
/// 非 `Clone`（Clone + Drop-cancel = 意外取消陷阱）；fire-and-forget 走
/// [`TaskHandle::detach`]。`Send + Sync`（任意线程取消）。
pub struct TaskHandle {
    cell: Arc<TaskCell>,
    /// Drop-cancel 开关（#55）：detach 置 false ⇒ Drop 只释放 Arc 不
    /// 取消——「抑制 cancel 语义」与「释放所有权」解耦，TaskCell 不泄漏。
    armed: bool,
    /// 调度器弱引用（#52）：取消路径据此释放额度（#58）、登记墓碑并唤醒
    /// 驱动线程——弱引用 ⇒ 不构成引用环（调度器释放后任务胞随之回收）。
    sched: Weak<Shared>,
}

impl TaskHandle {
    /// 取消：线性化点=状态 CAS。返回是否由本次调用完成取消（幂等：已
    /// Cancelled/Done 返 false）。已出堆条目检查-执行窗口内落地的取消不可
    /// 撤（本次照跑，模块头注「cancel 线性化点」节）；执行中取消 ⇒ 周期
    /// 任务不再重臂。
    ///
    /// 显式调用发 `skeleton::scheduler::cancel` 埋点事件；[`Drop`] 走同一取消
    /// 语义但**不发任何事件**（宿主 subscriber 回调不得在析构里同步执行）。
    pub fn cancel(&self) -> bool {
        self.cancel_inner(true)
    }

    /// 取消主体：唯一区别是 `emit`——`false` 为 Drop 静默路径。事件在状态
    /// 锁外发射（订阅方 IO 不持锁）。
    fn cancel_inner(&self, emit: bool) -> bool {
        // feature 关时宏消除，emit 无读者（别用 allow 压，显式消费）。
        #[cfg(not(feature = "log-embed"))]
        let _ = emit;

        let cancelled = match self.sched.upgrade() {
            Some(shared) => {
                // 退役（PENDING→CANCELLED）：CAS 赢家释放额度（#58，恰好一次）。
                let won = shared.retire(&self.cell, CANCELLED);
                if won {
                    // 墓碑计数（#52）：取消 O(1) 递增（不摘堆、不扫堆）。
                    shared.tombstones.fetch_add(1, AtomicOrd::SeqCst);
                    // 唤醒驱动线程：park 锚可能需重算（墓碑被清扫后锚提前）。
                    shared.signal.notify_all();
                }
                won
            }
            None => {
                // 调度器已释放（无共享状态可退）⇒ 仅置位（额度随调度器消失）。
                self.cell
                    .state
                    .compare_exchange(PENDING, CANCELLED, AtomicOrd::SeqCst, AtomicOrd::SeqCst)
                    .is_ok()
            }
        };
        if emit {
            embed_event!(debug, target: "skeleton::scheduler::cancel", cancelled = cancelled);
        }
        cancelled
    }

    /// 消费句柄且不触发 Drop-cancel（fire-and-forget 逃生门）。
    ///
    /// **不泄漏**（#55）：取消语义与所有权释放解耦——只置 `armed=false`
    /// （Drop 不再 cancel），`Arc<TaskCell>` 照常释放。此前用
    /// `mem::forget` 抑制 cancel，连带永久泄漏 TaskCell 分配（周期任务的
    /// 回调与捕获图随之永久留存）。
    pub fn detach(mut self) {
        self.armed = false;
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        // 仅 armed 才取消：detach 置 armed=false ⇒ 只释放 Arc，不取消。
        // 析构路径不发 tracing 事件（#221 §2.5）：宿主 subscriber 回调不得在
        // Drop 内同步执行。
        if self.armed {
            let _ = self.cancel_inner(false);
        }
    }
}

/// 驱动线程句柄：[`WorkerHandle::join`] = close（scheduler-wide 关闸）+ 等线程
/// 退出（优雅停机）；丢弃句柄（Drop）= 只请求**本 worker** 停机、不 join、
/// 不关闸（#72）。
///
/// 多 worker 下丢一个句柄只停它自己：其余 worker 与既有登记照常。所有句柄都
/// 丢弃而调度器仍开着时，注册继续入堆但不再点火，直到再
/// [`Scheduler::spawn_worker`] 或显式 [`Scheduler::close`]（显式状态，不是
/// 泄漏）。
///
/// 句柄**只在 OS 线程创建成功时存在**（issue#87）：[`Scheduler::try_spawn_worker`]
/// 失败原样返回 `io::Error`、不产出半构造句柄，故不存在「有句柄没线程」的态。
pub struct WorkerHandle {
    /// Option 化：`join` 需把 JoinHandle 移出（类型有 Drop impl 时禁止
    /// 移出字段）；`None` = 已 join。
    thread: Option<JoinHandle<()>>,
    sched: Arc<Scheduler>,
    /// per-worker 停信标（#72）：置位 + 唤醒共享 condvar ⇒ 本 worker 退出；
    /// 与调度器的 `closed` 闸解耦（丢句柄不得停无关 worker/取消登记）。
    stop: Arc<AtomicBool>,
}

impl WorkerHandle {
    /// 优雅停机：close（关闸+清 pending）+ 等驱动线程退出（在途回调跑完）。
    /// worker 侧 panic 原样重抛（fail-visible，禁静默吞 JoinError）。
    ///
    /// 禁在驱动回调内 join 同一 worker（自等死锁）。
    pub fn join(mut self) {
        self.sched.close();
        if let Some(thread) = self.thread.take() {
            if let Err(panic) = thread.join() {
                // worker 侧 panic 原样重抛（fail-visible，禁静默吞 JoinError）。
                std::panic::resume_unwind(panic);
            }
        }
    }
}

impl Drop for WorkerHandle {
    /// drop = 只请求**本 worker** 停机（per-worker 停信标），**不 join、不关闸**
    /// （#72）：关闸是 scheduler-wide 操作（[`Scheduler::close`]），丢一个句柄不得
    /// 停无关 worker、不得取消既有登记。
    ///
    /// 信标在状态锁内置位、解锁后再 `notify_all`：worker 的 park 判定（含
    /// 信标）同在这把锁内 ⇒ 无丢失唤醒（worker 不会在「判定后、wait 前」
    /// 错过置位）。唤醒走共享 condvar 的 `notify_all`——无关 worker 虚假醒来
    /// 重算后重 park（worker 数典型 1，不到可测成本不做 per-worker condvar）。
    ///
    /// 禁 join：句柄可能在 worker 自身回调内被 drop ⇒ 自等死锁。
    ///
    /// 丢弃路径**不确认**线程终止、**不传播** worker panic（仅
    /// [`WorkerHandle::join`] 确认终止并原样重抛 panic）。
    fn drop(&mut self) {
        let s = lock(&self.sched.shared.state);
        self.stop.store(true, AtomicOrd::SeqCst);
        drop(s);
        self.sched.shared.signal.notify_all();
        // thread（Option<JoinHandle>）随之 drop ⇒ std 语义 = detach（非阻塞）。
    }
}

/// 驱动主循环：park 到最近 deadline（condvar，零自旋），到期点火；本 worker
/// 的停信标置位（[`WorkerHandle`] Drop）或调度器关闸（[`Scheduler::close`] /
/// [`WorkerHandle::join`]）即退。
fn worker_loop(sched: Arc<Scheduler>, stop: Arc<AtomicBool>) {
    loop {
        sched.shared.worker_passes.fetch_add(1, AtomicOrd::SeqCst);
        if stop.load(AtomicOrd::SeqCst) {
            // 终态（worker 退出）：只停本 worker（信标），不关闸。
            // No synchronous telemetry on a bounded-progress teardown path.
            return;
        }
        if sched.is_closed() {
            // 终态（worker 退出）：close 是唯一正常出口。
            return;
        }
        // 墓碑清扫（#52）：取消后尽快回收，避免驱动为墓碑多醒一拍。
        sched.reclaim_cancelled();
        // park 锚 = 最近**有效** deadline（跳过堆顶墓碑——墓碑不是有效
        // deadline，驱动不该为它多醒一拍，#52）。
        let park_until = sched.next_pending_deadline();
        let now = sched.shared.clock.now_instant();
        match park_until {
            Some(d) if d <= now => {
                // 到期：确定性内核点火（回调在驱动线程锁外跑）。
                // 循环体兜底 catch：正常零命中（回调 panic 已被 fire 层
                // catch_unwind 摘除）；命中 = 框架自身 bug，留痕后退出
                // （不静默带病续跑）。
                if catch_unwind(AssertUnwindSafe(|| sched.tick())).is_err() {
                    sched.shared.panics.fetch_add(1, AtomicOrd::SeqCst);
                    // 异常路径（驱动内部 panic）：留痕后退出，不静默带病续跑。
                    // Do not invoke a synchronous host subscriber while terminating the worker.
                    return;
                }
            }
            Some(d) => {
                // park 到最近 deadline；注册（更早锚）/close/停信标即时唤醒
                // 重算。虚假唤醒醒来重算剩余时间继续 park——非自旋（每次醒
                // 必 park）。信标判定在状态锁内：置位（Drop）同持此锁 ⇒
                // 「判定后、wait 前」的窗口不会错过唤醒。
                //
                // 剩余时长用本轮**锁外**样本（上方 `now`，issue#101）：注入 Clock
                // 不得在 state mutex 下执行 ⇒ 这里取锁只为「再检查 + 等」，
                // 而检查在本样本之后、且在持锁内完成 ⇒ 取样窗口内入堆的更早
                // deadline / 关闸 / 停信标都会被看见（丢唤醒防护）。
                let s = lock(&sched.shared.state);
                if stop.load(AtomicOrd::SeqCst) || s.closed {
                    return;
                }
                // 双检（与 None 分支对称）：拿锁前可能有更早 deadline 注册入堆
                // ——按旧锚 park 会迟到（有界自愈不丢任务，但迟到无谓）；回主
                // 循环重算锚。
                if s.heap.peek().is_some_and(|Reverse(e)| e.deadline < d) {
                    drop(s);
                    continue;
                }
                // 剩余时长来自本轮锁外样本 `now`（issue#101：注入 Clock 不在
                // state mutex 下执行）；`d > now` 由外层匹配臂保证（`d <= now`
                // 已走点火分支）。样本可能略旧 ⇒ 至多晚醒一次持锁等待的时长，
                // 不会漏醒（复检均在此样本之后）。
                let guard = sched
                    .shared
                    .signal
                    .wait_timeout(s, d - now)
                    .unwrap_or_else(PoisonError::into_inner);
                drop(guard);
            }
            None => {
                // 空堆：无限期 park，等 close/注册/停信标唤醒（零自旋的关键面）。
                let s = lock(&sched.shared.state);
                if stop.load(AtomicOrd::SeqCst) || s.closed {
                    return;
                }
                // 双检：拿锁前可能有注册入堆——有活就回主循环重算锚。
                if s.heap.peek().is_some() {
                    drop(s);
                    continue;
                }
                let guard = sched
                    .shared
                    .signal
                    .wait(s)
                    .unwrap_or_else(PoisonError::into_inner);
                drop(guard);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use skeleton::clock::MockClock;

    use super::*;

    // ── 容量 setter 与注册共锁线性化（issue#95）──────────────────────────
    //
    // `set_capacity` 与 `register` 共用 `shared.state`；本区用 `CapacitySettle`
    // 把注册线程停在「已按旧 cap 裁决、尚未入堆」的状态锁窗口里，确定性构造
    // setter 想插在裁决与提交之间的交错。禁 sleep 竞速。

    /// setter 的反向有界等待上限：正确实现下 setter 必被状态锁挡住 ⇒ 必然超时。
    const SETTER_BLOCK_LIMIT: Duration = Duration::from_millis(300);

    /// issue#95：注册已按旧 cap 裁决（仍持状态锁）时 setter **不可能返回**；
    /// 其后所有注册按新 cap 判定，旧 cap 接纳的注册不被回溯驱逐。
    ///
    /// 变异判别：若 setter 退回「锁外原子 store」，它会在注册停靠期间立即返回
    /// ⇒ 本条红（本测试即该改动的变异锚）。
    #[test]
    fn set_capacity_cannot_linearize_inside_register() {
        let sched = Scheduler::new(Arc::new(MockClock::new()));
        sched.shared.register_gate.arm();

        let registering = {
            let sched = sched.clone();
            thread::spawn(move || sched.try_after(Duration::from_secs(60), || {}))
        };
        assert!(
            sched
                .shared
                .register_gate
                .wait_entered(Duration::from_secs(10)),
            "注册必须停靠在「已读 cap、尚未入堆」的状态锁窗口里"
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let setting = {
            let sched = sched.clone();
            thread::spawn(move || {
                sched.set_capacity(1);
                let _ = done_tx.send(());
            })
        };
        // 负向断言（OS 时钟有界等待）：setter 不得在注册尚未提交时返回。
        let setter_returned_early = done_rx.recv_timeout(SETTER_BLOCK_LIMIT).is_ok();
        // 先放行再断言：断言失败时也不得把停靠线程留在锁里（否则是挂死而非失败）。
        sched.shared.register_gate.release();
        let handle = registering
            .join()
            .expect("注册线程不得 panic")
            .expect("停靠中的注册按旧 cap（0 = 无界）成功");
        setting.join().expect("setter 线程不得 panic");
        assert!(
            !setter_returned_early,
            "setter 在注册已按旧 cap 裁决、尚未入堆时返回 ⇒ 容量发布未与注册共锁（issue#95）"
        );

        assert_eq!(
            sched.live_registrations(),
            1,
            "旧 cap 接纳的注册必须保留（缩容不回溯驱逐）"
        );
        assert_eq!(
            sched.try_after(Duration::from_secs(60), || {}).err(),
            Some(AdmissionError::Full),
            "setter 之后的新注册按新 cap=1 拒绝"
        );
        assert_eq!(sched.live_registrations(), 1, "拒绝不占额度");
        assert_eq!(sched.rejected_registrations(), 1, "拒绝必须留痕");

        handle.detach();
        sched.close();
    }

    /// issue#95：缩容只挡后续注册；扩容与 `0`（无界）对后续注册立即生效。
    #[test]
    fn set_capacity_shrink_and_expand_apply_to_later_registrations() {
        let sched = Scheduler::new(Arc::new(MockClock::new()));
        sched.set_capacity(1);
        let first = sched
            .try_after(Duration::from_secs(60), || {})
            .expect("空位按 cap=1 接纳");
        assert_eq!(
            sched.try_after(Duration::from_secs(60), || {}).err(),
            Some(AdmissionError::Full),
            "活跃数达 cap=1 ⇒ 后续注册拒绝"
        );

        sched.set_capacity(2);
        let second = sched
            .try_after(Duration::from_secs(60), || {})
            .expect("扩容立即生效");
        sched.set_capacity(1);
        assert_eq!(
            sched.live_registrations(),
            2,
            "缩容到活跃数以下不驱逐既有注册"
        );
        assert_eq!(
            sched.try_after(Duration::from_secs(60), || {}).err(),
            Some(AdmissionError::Full),
            "缩容后新注册按新 cap=1 拒绝"
        );

        sched.set_capacity(0);
        let third = sched
            .try_after(Duration::from_secs(60), || {})
            .expect("0 = 无界：立即生效");
        assert_eq!(sched.live_registrations(), 3);
        assert_eq!(sched.rejected_registrations(), 2, "两次触顶各计一次");

        first.detach();
        second.detach();
        third.detach();
        sched.close();
    }

    /// #55：detach 抑制 cancel 但**不得**泄漏 TaskCell 分配——句柄释放后
    /// 调度器清堆 ⇒ 弱引用升级失败（TaskCell 已回收）。
    ///
    /// 断言非恒真：清堆前弱引用必可升级（堆条目持强引用）。
    #[test]
    fn detach_releases_task_cell_after_scheduler_drains_entry() {
        let sched = Scheduler::new(Arc::new(skeleton::clock::MockClock::new()));
        let handle = sched
            .after(Duration::from_secs(3600), || {})
            .expect("未关闸注册必成");
        let weak = Arc::downgrade(&handle.cell);
        handle.detach();
        assert!(
            weak.upgrade().is_some(),
            "堆条目持强引用：此时 TaskCell 必在（断言非恒真）"
        );
        sched.close();
        assert!(
            weak.upgrade().is_none(),
            "detach 后 TaskCell 必被回收（不泄漏分配）"
        );
    }

    /// #87：注入的 OS 创建失败 ⇒ `try_spawn_worker` 原样返回 `io::Error` 的
    /// kind，不 panic、不关闸、不留对 `Scheduler` 的残余所有权。
    ///
    /// 断言非恒真：注入桩必返回 `Err(WouldBlock)`——若实现改回裸
    /// `thread::spawn`（panic）或吞掉错误，本测试红。
    #[test]
    fn try_spawn_worker_returns_original_io_error_without_residue() {
        let sched = Arc::new(Scheduler::with_spawn(
            Arc::new(MockClock::new()),
            |_name, _body| Err(io::Error::from(io::ErrorKind::WouldBlock)),
        ));
        let before = Arc::strong_count(&sched);
        let err = sched
            .try_spawn_worker()
            .err()
            .expect("注入 OS 创建失败必返回 Err（不得 panic）");
        assert_eq!(
            err.kind(),
            io::ErrorKind::WouldBlock,
            "原样返回 OS 错误的 kind（不包装、不改写）"
        );
        assert_eq!(
            Arc::strong_count(&sched),
            before,
            "失败零残留：闭包持有的 Arc<Scheduler> 克隆随闭包丢弃（无泄漏 owner）"
        );
        assert!(!sched.is_closed(), "启动失败不关闸（调度器仍开放）");
        assert_eq!(sched.worker_passes(), 0, "无后台线程 ⇒ 驱动圈数恒 0");
    }

    /// #87：启动失败不污染确定性内核——注册照常受理，显式 `tick()` 仍按
    /// deadline 点火。
    #[test]
    fn failed_spawn_leaves_tick_kernel_intact() {
        let clock = Arc::new(MockClock::new());
        let sched = Arc::new(Scheduler::with_spawn(clock.clone(), |_name, _body| {
            Err(io::Error::from(io::ErrorKind::Other))
        }));
        assert!(sched.try_spawn_worker().is_err(), "注入创建失败必返回 Err");

        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::clone(&hits);
        sched
            .after(Duration::from_millis(100), move || {
                hits2.fetch_add(1, AtomicOrd::SeqCst);
            })
            .expect("启动失败不关闸：注册照常受理")
            .detach();
        assert_eq!(sched.live_registrations(), 1, "注册入堆（注册行为不变）");
        assert_eq!(sched.tick(), 0, "未到期不点火");
        clock.advance(Duration::from_millis(100));
        assert_eq!(sched.tick(), 1, "失败后确定性内核照常按 deadline 点火");
        assert_eq!(hits.load(AtomicOrd::SeqCst), 1);
        assert_eq!(
            sched.live_registrations(),
            0,
            "点火即退役（无虚构运行状态）"
        );
        assert_eq!(sched.worker_passes(), 0, "无后台线程：无人推进驱动圈数");
        assert!(!sched.is_closed());
    }

    /// #87：兼容入口 `spawn_worker` 失败带因 panic（消息含 io::Error 的 kind
    /// 并指向 `try_spawn_worker`），且失败路径同样零残留。
    #[test]
    fn spawn_worker_panics_with_io_error_cause_on_failed_os_spawn() {
        let sched = Arc::new(Scheduler::with_spawn(
            Arc::new(MockClock::new()),
            |_name, _body| Err(io::Error::from(io::ErrorKind::WouldBlock)),
        ));
        let before = Arc::strong_count(&sched);
        let payload = catch_unwind(AssertUnwindSafe(|| {
            let _ = sched.spawn_worker();
        }))
        .expect_err("fail-visible 便利入口：OS 创建失败必 panic");
        let msg = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .expect("panic 载荷为格式化字符串");
        assert!(
            msg.contains("WouldBlock"),
            "panic 带因：消息含 io::Error 的 kind（实际：{msg}）"
        );
        assert!(
            msg.contains("try_spawn_worker"),
            "panic 指向恢复面（实际：{msg}）"
        );
        assert_eq!(
            Arc::strong_count(&sched),
            before,
            "panic 前的失败路径同样零残留"
        );
    }

    /// 锁外 Clock（issue#101）：worker 的 park 路径不得在 `shared.state` mutex 下
    /// 调注入时钟。三面各一测：try_lock 探针（每次取样都看到锁可用）、可重入钟
    /// （取样内调 `is_closed`/`next_deadline` 不自锁）、取样窗口内插入更早
    /// deadline（复检 ⇒ 不丢唤醒、不按旧样本长时间晚醒）。
    mod clock_outside_state_lock {
        use super::*;
        use std::sync::TryLockError;
        use std::time::SystemTime;

        /// 轮询等条件（1ms 步进、2s 上限——驱动线程是真实时钟面，测试骨架）。
        fn wait_until(what: &str, mut ready: impl FnMut() -> bool) {
            let deadline = Instant::now() + Duration::from_secs(2);
            while !ready() {
                assert!(Instant::now() < deadline, "{what}：等超时（2s）");
                std::thread::sleep(Duration::from_millis(1));
            }
        }

        /// 探针钟：取值委托 `MockClock`；每次取样先对 scheduler 的 `state` mutex
        /// 做 `try_lock`（`WouldBlock` = 本次取样在临界区内 ⇒ 回归信号）。
        struct ProbeClock {
            mock: Arc<MockClock>,
            target: Mutex<Weak<Scheduler>>,
            calls: AtomicU64,
            under_lock: AtomicU64,
        }

        impl ProbeClock {
            fn new(mock: Arc<MockClock>) -> Arc<Self> {
                Arc::new(Self {
                    mock,
                    target: Mutex::new(Weak::new()),
                    calls: AtomicU64::new(0),
                    under_lock: AtomicU64::new(0),
                })
            }

            fn observe(&self) {
                self.calls.fetch_add(1, AtomicOrd::SeqCst);
                let weak = self.target.lock().expect("probe 目标调度器锁").clone();
                if let Some(sched) = weak.upgrade() {
                    match sched.shared.state.try_lock() {
                        Ok(_) | Err(TryLockError::Poisoned(_)) => {}
                        Err(TryLockError::WouldBlock) => {
                            self.under_lock.fetch_add(1, AtomicOrd::SeqCst);
                        }
                    }
                }
            }
        }

        impl Clock for ProbeClock {
            fn now_instant(&self) -> Instant {
                self.observe();
                self.mock.now_instant()
            }

            fn now_system(&self) -> SystemTime {
                self.mock.now_system()
            }
        }

        /// #101：worker 的 park 路径（含注册/关闸唤醒后的重算）每次时钟取样都在
        /// `state` mutex 之外。
        #[test]
        fn worker_park_reads_clock_outside_state_mutex() {
            let mock = Arc::new(MockClock::new());
            let clock = ProbeClock::new(Arc::clone(&mock));
            let sched = Arc::new(Scheduler::new(Arc::clone(&clock) as Arc<dyn Clock>));
            *clock.target.lock().expect("probe 目标调度器锁") = Arc::downgrade(&sched);

            // 冻结仿钟上的 +1ms deadline：worker 反复 park（真实 1ms/圈）⇒
            // park 分支的取样必然走过（mock 不推进，任务不点火）。
            let handle = sched
                .after(Duration::from_millis(1), || {})
                .expect("未关闸注册必成");
            let mut worker = sched.spawn_worker();
            wait_until("worker 跑过三圈", || sched.worker_passes() >= 3);
            let calls = clock.calls.load(AtomicOrd::SeqCst);
            // Stop this worker without Scheduler::close(): close() takes the same
            // state mutex from the test thread and can make the probe's try_lock
            // observe unrelated cross-thread contention as a false "clock under
            // lock" positive. Mirror WorkerHandle::drop's lock/flag/wakeup protocol,
            // then join explicitly so the test still proves worker termination.
            let state = lock(&sched.shared.state);
            worker.stop.store(true, AtomicOrd::SeqCst);
            drop(state);
            sched.shared.signal.notify_all();
            if let Some(thread) = worker.thread.take() {
                thread.join().expect("worker exits after stop signal");
            }
            drop(worker);
            handle.detach();

            assert!(calls >= 3, "park 路径确实取过样（实际 {calls} 次）");
            assert_eq!(
                clock.under_lock.load(AtomicOrd::SeqCst),
                0,
                "worker 的 Clock 取样不得在 state mutex 内"
            );
        }

        /// 探针鉴别力对照：**故意**持 state 锁取样 ⇒ 探针必须记到 `WouldBlock`
        /// （证明上面那个「0 次」不是探针失效的空断言）。
        #[test]
        fn probe_detects_clock_read_inside_state_mutex() {
            let mock = Arc::new(MockClock::new());
            let clock = ProbeClock::new(Arc::clone(&mock));
            let sched = Arc::new(Scheduler::new(Arc::clone(&clock) as Arc<dyn Clock>));
            *clock.target.lock().expect("probe 目标调度器锁") = Arc::downgrade(&sched);

            let g = lock(&sched.shared.state);
            let _ = clock.now_instant();
            drop(g);

            assert_eq!(clock.calls.load(AtomicOrd::SeqCst), 1);
            assert_eq!(
                clock.under_lock.load(AtomicOrd::SeqCst),
                1,
                "对照：锁内取样必须被探针抓到（否则上面的 0 次无意义）"
            );
        }

        /// 可重入钟：取样内调同一调度器的 `is_closed`/`next_deadline`——park 路径
        /// 不得自锁。先 `try_lock` 判可用性：锁内取样在这里确定性失败，而不是
        /// 挂死测试进程。
        struct ReentrantClock {
            mock: Arc<MockClock>,
            target: Mutex<Weak<Scheduler>>,
        }

        impl ReentrantClock {
            fn new(mock: Arc<MockClock>) -> Arc<Self> {
                Arc::new(Self {
                    mock,
                    target: Mutex::new(Weak::new()),
                })
            }
        }

        impl Clock for ReentrantClock {
            fn now_instant(&self) -> Instant {
                let weak = self.target.lock().expect("probe 目标调度器锁").clone();
                if let Some(sched) = weak.upgrade() {
                    if let Err(TryLockError::WouldBlock) = sched.shared.state.try_lock() {
                        panic!(
                            "Clock 在 scheduler state mutex 内执行（issue#101 回归：重入自锁面）"
                        );
                    }
                    let _ = sched.is_closed();
                    let _ = sched.next_deadline();
                }
                self.mock.now_instant()
            }

            fn now_system(&self) -> SystemTime {
                self.mock.now_system()
            }
        }

        #[test]
        fn reentrant_clock_can_query_same_scheduler() {
            let mock = Arc::new(MockClock::new());
            let clock = ReentrantClock::new(Arc::clone(&mock));
            let sched = Arc::new(Scheduler::new(Arc::clone(&clock) as Arc<dyn Clock>));
            *clock.target.lock().expect("probe 目标调度器锁") = Arc::downgrade(&sched);

            sched
                .after(Duration::from_millis(1), || {})
                .expect("注册必成")
                .detach();
            let worker = sched.spawn_worker();
            let deadline = Instant::now() + Duration::from_secs(2);
            while sched.worker_passes() < 2 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            worker.join(); // worker 侧 panic 原样重抛（fail-visible）
            assert!(
                sched.worker_passes() >= 2,
                "worker 应在取样处不自锁地跑过两圈"
            );
        }

        /// 闸钟：委托 `MockClock`；`armed` 时在取样处闸住 worker 并通知测试，由
        /// 测试放行——精确构造「worker 已取到时间样本、尚未拿 state 锁」的窗口
        /// （不用 sleep 控交错）。
        struct GateClock {
            mock: Arc<MockClock>,
            armed: AtomicBool,
            at_gate: Arc<(Mutex<bool>, Condvar)>,
            release: Arc<(Mutex<bool>, Condvar)>,
        }

        impl GateClock {
            fn new(mock: Arc<MockClock>) -> Arc<Self> {
                Arc::new(Self {
                    mock,
                    armed: AtomicBool::new(false),
                    at_gate: Arc::new((Mutex::new(false), Condvar::new())),
                    release: Arc::new((Mutex::new(false), Condvar::new())),
                })
            }

            fn arm(&self) {
                self.armed.store(true, AtomicOrd::SeqCst);
            }

            fn wait_at_gate(&self) {
                let deadline = Instant::now() + Duration::from_secs(2);
                let mut at = self.at_gate.0.lock().expect("gate");
                while !*at {
                    let left = deadline.saturating_duration_since(Instant::now());
                    assert!(!left.is_zero(), "worker 未在取样处闸住（等超时）");
                    let (g, _) = self.at_gate.1.wait_timeout(at, left).expect("gate");
                    at = g;
                }
            }

            fn release(&self) {
                *self.release.0.lock().expect("gate") = true;
                self.release.1.notify_all();
            }
        }

        impl Clock for GateClock {
            fn now_instant(&self) -> Instant {
                if self.armed.swap(false, AtomicOrd::SeqCst) {
                    *self.at_gate.0.lock().expect("gate") = true;
                    self.at_gate.1.notify_all();
                    let mut rel = self.release.0.lock().expect("gate");
                    while !*rel {
                        rel = self.release.1.wait(rel).expect("gate");
                    }
                }
                self.mock.now_instant()
            }

            fn now_system(&self) -> SystemTime {
                self.mock.now_system()
            }
        }

        /// #101 丢唤醒交错：worker 取到旧样本后、拿锁前插入更早 deadline ⇒ 复检
        /// 必须看见并回主循环（否则按旧锚 park：既丢这次唤醒又长时间晚醒）。
        /// 放行后**不再有任何 notify**——能按期点火只能靠复检。
        #[test]
        fn earlier_deadline_inserted_while_sampling_is_not_missed() {
            let mock = Arc::new(MockClock::new());
            let clock = GateClock::new(Arc::clone(&mock));
            let sched = Arc::new(Scheduler::new(Arc::clone(&clock) as Arc<dyn Clock>));

            sched
                .after(Duration::from_secs(5), || {})
                .expect("注册必成")
                .detach();
            let fired = Arc::new(AtomicU64::new(0));
            let fired2 = Arc::clone(&fired);
            clock.arm();
            let worker = sched.spawn_worker();

            clock.wait_at_gate(); // worker 已取到样本，尚未拿 state 锁
            sched
                .after(Duration::from_millis(1), move || {
                    fired2.fetch_add(1, AtomicOrd::SeqCst);
                })
                .expect("注册必成")
                .detach();
            clock.release();
            // 虚拟钟越过更早 deadline；此后**不 notify**（丢唤醒面全靠复检）。
            mock.advance(Duration::from_millis(2));

            let deadline = Instant::now() + Duration::from_millis(500);
            while fired.load(AtomicOrd::SeqCst) == 0 && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            worker.join();
            assert_eq!(
                fired.load(AtomicOrd::SeqCst),
                1,
                "取样窗口内入堆的更早 deadline 必须被复检看见（缺复检 ⇒ 只能等满 5s 锚）"
            );
        }

        /// panic 钟：在取样处 panic——锁外展开 ⇒ 不投毒状态锁；panic 原样传播
        /// 出 worker（`join` 重抛），调度器仍可注册/点火（确定性内核不受影响）。
        struct PanicClock {
            mock: Arc<MockClock>,
            target: Mutex<Weak<Scheduler>>,
            armed: AtomicBool,
            panicked: AtomicBool,
            panicked_under_lock: AtomicU64,
        }

        impl PanicClock {
            fn new(mock: Arc<MockClock>) -> Arc<Self> {
                Arc::new(Self {
                    mock,
                    target: Mutex::new(Weak::new()),
                    armed: AtomicBool::new(false),
                    panicked: AtomicBool::new(false),
                    panicked_under_lock: AtomicU64::new(0),
                })
            }
        }

        impl Clock for PanicClock {
            fn now_instant(&self) -> Instant {
                if self.armed.swap(false, AtomicOrd::SeqCst) {
                    let weak = self.target.lock().expect("probe 目标调度器锁").clone();
                    if let Some(sched) = weak.upgrade() {
                        if matches!(sched.shared.state.try_lock(), Err(TryLockError::WouldBlock)) {
                            self.panicked_under_lock.fetch_add(1, AtomicOrd::SeqCst);
                        }
                    }
                    self.panicked.store(true, AtomicOrd::SeqCst);
                    panic!("clock boom（锁外展开）");
                }
                self.mock.now_instant()
            }

            fn now_system(&self) -> SystemTime {
                self.mock.now_system()
            }
        }

        #[test]
        fn clock_panic_expands_outside_state_mutex_and_scheduler_stays_usable() {
            let mock = Arc::new(MockClock::new());
            let clock = PanicClock::new(Arc::clone(&mock));
            let sched = Arc::new(Scheduler::new(Arc::clone(&clock) as Arc<dyn Clock>));
            *clock.target.lock().expect("probe 目标调度器锁") = Arc::downgrade(&sched);

            sched
                .after(Duration::from_secs(60), || {})
                .expect("注册必成")
                .detach();
            clock.armed.store(true, AtomicOrd::SeqCst);
            let worker = sched.spawn_worker();

            let deadline = Instant::now() + Duration::from_secs(2);
            while !clock.panicked.load(AtomicOrd::SeqCst) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(
                clock.panicked.load(AtomicOrd::SeqCst),
                "worker 应在取样处 panic"
            );
            assert_eq!(
                clock.panicked_under_lock.load(AtomicOrd::SeqCst),
                0,
                "clock panic 必须在 state mutex 之外展开"
            );

            // 调度器仍可用：注册 + 显式点火照常（panic 不破坏确定性内核）。
            let fired = Arc::new(AtomicU64::new(0));
            let fired2 = Arc::clone(&fired);
            sched
                .after(Duration::ZERO, move || {
                    fired2.fetch_add(1, AtomicOrd::SeqCst);
                })
                .expect("panic 后调度器仍开放")
                .detach();
            assert_eq!(sched.tick(), 1, "panic 后显式点火照常");
            assert_eq!(fired.load(AtomicOrd::SeqCst), 1);

            let joined = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.join()));
            assert!(
                joined.is_err(),
                "worker 侧 clock panic 原样传播（join 重抛）"
            );
        }
    }
}
