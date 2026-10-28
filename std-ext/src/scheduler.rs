//! 同步任务调度器：延迟（after）/周期（every）任务排程，Clock 注入。
//!
//! 常驻零依赖件：纯 std，无 feature 门。**双层形态**（拍板点，论证见
//! scheduler 域性能锚（本地留存，未入库））：
//!
//! - **确定性内核（线程无关）**：最小堆（`BinaryHeap`+`Reverse`）按注入
//!   [`Clock`] 的单调面（`now_instant`）排程；[`Scheduler::tick`] 在调用
//!   线程同步点火全部到期任务并返回实际点火数。测试配 [`MockClock`]：
//!   `advance`+`tick` 零真实 sleep，时间线逐毫秒可断言（retry 域 Sleeper
//!   同哲学的调度版）；
//! - **驱动线程（生产外挂）**：[`Scheduler::spawn_worker`] 起单线程驱动，
//!   condvar park 到最近 deadline（零自旋），注册/关闭即时唤醒。驱动线程
//!   属真实时钟面——MockClock 时间只在显式 `advance` 时流动，park 它无
//!   意义（tokio auto-advance 陷阱的反向对表：零自动前进，确定性面走
//!   tick）。
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
//!   重臂。`cancel` 幂等；句柄 RAII（Drop 即 cancel），[`TaskHandle::detach`]
//!   是 fire-and-forget 逃生门；
//! - **close = 关闸 + 清 pending + 在途跑完**（拍板点）：注册显式
//!   `Err(Closed)` 禁静默；pending 全部标 Cancelled（句柄侧可见）；在途
//!   回调跑完为止（同步代码无中断面），驱动线程随后退出
//!   （[`WorkerHandle::join`] = close + join）；
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
//! - **惰性墓碑**：cancel 不摘堆（O(1)），死亡条目到点弹出时摘除——远期
//!   取消的墓碑会让驱动多醒一拍（语义正确，代价一次空醒）。
//!
//! log-embed 埋点（`log-embed` feature，默认关=宏消除零开销）：target
//! `skeleton::scheduler::{register,cancel,close,fire,worker}`。级别规范
//! （骨架默认：正常流转=debug / 异常路径=warn / 终态=info）：注册/取消/
//! 点火/墓碑跳过=debug；关闸拒绝注册、回调 panic、超载退化重臂、驱动
//! 内部 panic=warn；close（清 pending）与 worker 退出=info。seq（堆条目
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
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering as AtomicOrd};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
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

struct Shared {
    clock: Arc<dyn Clock>,
    // ponytail: 单全局状态锁，注册竞争下吞吐负扩展（multidim ③ 实测
    // 6.2M→2.9M ops/s @1→8 线程）——deadline 全序要求堆顶唯一，分片破坏
    // 语义；驱动线程单线程形态下注册竞争非热路径，分片锁/无锁堆留待真实
    // 多注册方负载出现再升。
    state: Mutex<HeapState>,
    /// 注册/关闭唤醒（驱动线程 park 唯一早醒源；cancel 不唤醒=惰性墓碑）。
    signal: Condvar,
    panics: AtomicU64,
    worker_passes: AtomicU64,
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

/// 同步任务调度器（`Arc` 内脏，[`Clone`] 廉价共享；注册/点火/关闭线程安全）。
///
/// 见[模块文档](self)双层形态与语义拍板。
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
    pub fn after(
        &self,
        delay: Duration,
        task: impl FnOnce() + Send + Sync + 'static,
    ) -> Result<TaskHandle, Closed> {
        self.register(
            delay,
            TaskKind::Once(std::sync::Mutex::new(Some(Box::new(task)))),
        )
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
    pub fn every(
        &self,
        period: Duration,
        task: impl Fn() + Send + Sync + 'static,
    ) -> Result<TaskHandle, Closed> {
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

    /// 注册内窥（after/every 共用）：deadline 锁外合成（时钟锁与状态锁零
    /// 嵌套，序=state→clock 单向），入堆+序号在锁内。
    fn register(&self, delay: Duration, kind: TaskKind) -> Result<TaskHandle, Closed> {
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
                return Err(Closed);
            }
            seq = s.next_seq;
            s.next_seq += 1;
            s.heap.push(Reverse(Entry {
                deadline,
                seq,
                task: Arc::clone(&cell),
            }));
        }
        embed_event!(debug, target: "skeleton::scheduler::register", seq = seq, kind = kind_name, delay_ms = delay.as_millis() as u64);
        // 唤醒驱动线程：更早 deadline 的注册即时生效（非睡满旧锚）。
        self.shared.signal.notify_all();
        Ok(TaskHandle { cell })
    }

    /// 确定性内核：点火全部到期任务（`deadline <= now`，按注入钟口径），
    /// 返回实际点火数（惰性墓碑不计）。回调在调用线程**锁外**执行（重入
    /// 安全）；回调内推进虚拟钟（MockClock）⇒ 后续到期条目同轮继续火。
    pub fn tick(&self) -> usize {
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
                if let Some(run) = taken {
                    if catch_unwind(AssertUnwindSafe(run)).is_err() {
                        self.shared.panics.fetch_add(1, AtomicOrd::SeqCst);
                        embed_event!(warn, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name, reason = "panic");
                    }
                }
                // 终态 DONE：此后 cancel 返 false（测试 cancel_after_fire）。
                let _ = entry.task.state.compare_exchange(
                    PENDING,
                    DONE,
                    AtomicOrd::SeqCst,
                    AtomicOrd::SeqCst,
                );
                // CAS 失败=检查-执行窗口内被 cancel：本次照跑（文档化语义）。
            }
            TaskKind::Every { period, fire } => {
                if catch_unwind(AssertUnwindSafe(fire)).is_err() {
                    // panic 摘除不再臂（JDK 语义）；留痕 fail-visible。
                    self.shared.panics.fetch_add(1, AtomicOrd::SeqCst);
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
                if entry
                    .deadline
                    .checked_add(*period)
                    .is_none_or(|anchor| anchor <= now)
                {
                    embed_event!(warn, target: "skeleton::scheduler::fire", seq = entry.seq, kind = kind_name, reason = "overload_rearm", period_ms = period.as_millis() as u64);
                }
                // 锁内裁决重臂：close 或执行中 cancel 赢了 ⇒ 摘除。
                // 残余窗口（裁决后 cancel 落地）由惰性墓碑兜底：取消成功，
                // 该条目到点被墓碑逻辑吃掉。
                let mut s = lock(&self.shared.state);
                if s.closed || entry.task.state.load(AtomicOrd::SeqCst) != PENDING {
                    return true;
                }
                s.heap.push(Reverse(Entry {
                    deadline: next,
                    seq: entry.seq,
                    task: Arc::clone(&entry.task),
                }));
                drop(s);
                // 不 notify：重臂只会把 deadline 推后，park 锚提前无害。
            }
        }
        true
    }

    /// 最近到期时刻（堆顶，**含惰性墓碑**——驱动 park 依据，空堆 None）。
    pub fn next_deadline(&self) -> Option<Instant> {
        let s = lock(&self.shared.state);
        s.heap.peek().map(|Reverse(e)| e.deadline)
    }

    /// 关闸：此后注册显式 `Err(Closed)`；清空 pending（全部标 Cancelled，
    /// 句柄侧可见）；在途回调跑完为止。幂等。驱动线程随之退出
    /// （[`WorkerHandle::join`]）。
    ///
    /// 回调内调用合法（重入安全，测试钉死）。
    pub fn close(&self) {
        {
            let mut s = lock(&self.shared.state);
            if s.closed {
                return; // 幂等
            }
            s.closed = true;
            // 清 pending：堆内全部标 Cancelled（句柄侧可见）；已出堆在途条目
            // 不在堆里，回调跑完为止（同步代码无中断面）。
            let drained = std::mem::take(&mut s.heap);
            drop(s); // CAS 是原子操作，锁外逐个标记不持锁
            #[cfg(feature = "log-embed")]
            let cleared = drained.len() as u64;
            for Reverse(e) in drained {
                let _ = e.task.state.compare_exchange(
                    PENDING,
                    CANCELLED,
                    AtomicOrd::SeqCst,
                    AtomicOrd::SeqCst,
                );
            }
            // 终态（close）：清 pending 计数留痕；幂等重入（已 closed 早退）不发。
            embed_event!(info, target: "skeleton::scheduler::close", cleared = cleared);
        }
        // 唤醒驱动线程：close 即退（park 中早醒重算）。
        self.shared.signal.notify_all();
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
    /// 注册（更早 deadline）即时唤醒重算，close 即退。可多起（互抢 pop，
    /// 典型 1 个）；MockClock 时间不自动流动，驱动线程对其无意义——确定性
    /// 测试走 [`Scheduler::tick`]。
    pub fn spawn_worker(self: &Arc<Self>) -> WorkerHandle {
        let thread = thread::spawn({
            let sched = Arc::clone(self);
            move || worker_loop(sched)
        });
        WorkerHandle {
            thread,
            sched: Arc::clone(self),
        }
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

/// 任务句柄：RAII cancel（Drop 即 [`TaskHandle::cancel`]）+ 幂等。
///
/// 非 `Clone`（Clone + Drop-cancel = 意外取消陷阱）；fire-and-forget 走
/// [`TaskHandle::detach`]。`Send + Sync`（任意线程取消）。
pub struct TaskHandle {
    cell: Arc<TaskCell>,
}

impl TaskHandle {
    /// 取消：线性化点=状态 CAS。返回是否由本次调用完成取消（幂等：已
    /// Cancelled/Done 返 false）。已出堆条目检查-执行窗口内落地的取消不可
    /// 撤（本次照跑，模块头注「cancel 线性化点」节）；执行中取消 ⇒ 周期
    /// 任务不再重臂。
    pub fn cancel(&self) -> bool {
        let cancelled = self
            .cell
            .state
            .compare_exchange(PENDING, CANCELLED, AtomicOrd::SeqCst, AtomicOrd::SeqCst)
            .is_ok();
        embed_event!(debug, target: "skeleton::scheduler::cancel", cancelled = cancelled);
        cancelled
    }

    /// 消费句柄且不触发 Drop-cancel（fire-and-forget 逃生门）。
    pub fn detach(self) {
        std::mem::forget(self);
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        let _ = self.cancel();
    }
}

/// 驱动线程句柄：[`WorkerHandle::join`] = close + 等线程退出（优雅停机）。
pub struct WorkerHandle {
    thread: JoinHandle<()>,
    sched: Arc<Scheduler>,
}

impl WorkerHandle {
    /// 优雅停机：close（关闸+清 pending）+ 等驱动线程退出（在途回调跑完）。
    /// worker 侧 panic 原样重抛（fail-visible，禁静默吞 JoinError）。
    ///
    /// 禁在驱动回调内 join 同一 worker（自等死锁）。
    pub fn join(self) {
        self.sched.close();
        if let Err(panic) = self.thread.join() {
            // worker 侧 panic 原样重抛（fail-visible，禁静默吞 JoinError）。
            std::panic::resume_unwind(panic);
        }
    }
}

/// 驱动主循环：park 到最近 deadline（condvar，零自旋），到期点火，关闭即退。
fn worker_loop(sched: Arc<Scheduler>) {
    loop {
        sched.shared.worker_passes.fetch_add(1, AtomicOrd::SeqCst);
        if sched.is_closed() {
            // 终态（worker 退出）：close 是唯一正常出口。
            embed_event!(info, target: "skeleton::scheduler::worker", reason = "closed");
            return;
        }
        // park 锚 = 堆顶（含惰性墓碑——远期取消多醒一拍，语义正确）。
        let park_until = sched.next_deadline();
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
                    embed_event!(warn, target: "skeleton::scheduler::worker", reason = "internal_panic");
                    return;
                }
            }
            Some(d) => {
                // park 到最近 deadline；注册（更早锚）/close 即时唤醒重算。
                // 虚假唤醒醒来重算剩余时间继续 park——非自旋（每次醒必 park）。
                let s = lock(&sched.shared.state);
                if s.closed {
                    return;
                }
                // 双检（与 None 分支对称）：拿锁前可能有更早 deadline 注册入堆
                // ——按旧锚 park 会迟到（有界自愈不丢任务，但迟到无谓）；回主
                // 循环重算锚。
                if s.heap.peek().is_some_and(|Reverse(e)| e.deadline < d) {
                    drop(s);
                    continue;
                }
                let now = sched.shared.clock.now_instant();
                if d > now {
                    let guard = sched
                        .shared
                        .signal
                        .wait_timeout(s, d - now)
                        .unwrap_or_else(PoisonError::into_inner);
                    drop(guard);
                }
            }
            None => {
                // 空堆：无限期 park，等 close/注册唤醒（零自旋的关键面）。
                let s = lock(&sched.shared.state);
                if s.closed {
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
