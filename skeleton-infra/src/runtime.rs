//! # skeleton-infra :: runtime
//!
//! [`SyncStore`]：契约 trait `skeleton::Store` 的 std-only 参考实现（单写者
//! 队列 + 快照广播 + 同步排空环）。契约承诺（同步排空/快照一致性/订阅/停机闸/
//! fail-visible/回灌有界）见 `skeleton::Store` 类型文档；本模块承载兑现承诺的
//! 机器：CoW 快照落位、锁消融排空环、回灌深度预算、本地订阅档、异步信号
//! 通知档（ADR 0008）、审计事件流发射点（值面在 [`crate::audit`]）与取证八读数。
//!
//! std-only：`runtime` feature 零三方依赖（cargo tree 证明口径见 crate 文档）。

use std::cell::{RefCell, UnsafeCell};
use std::collections::{HashSet, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use skeleton::notify::{Notify, NotifySink};
use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, EffectKind, EffectRuntime, Infallible, Reducer,
    Restore, Store, Subscription,
};

use crate::audit::{AuditEvent, AuditEventKind, AuditSink, AuditTicket};

// 关注面切缝（可维护性 R1）：排空环主文件只留 store 机器；本地订阅 TLS 机制
// 与审计发射沿自然缝独立成子模块（对内 pub(super)，公共面经本模块 re-export
// 原路不变）。
mod audit_emit;
mod close_wait;
mod local_subs;
mod notify;
mod receipt;
// 蓝图 §9 证据面：排空权独占载体的 Mutex 对照实现（默认构建不编译，见
// `ExclusiveCell` 与 `tests/unsafe_evidence.rs` 的差分测试）。
#[cfg(feature = "unsafe-evidence")]
mod evidence;

use audit_emit::{
    AuditDispatch, audit_dispatch_reset, audit_emit, audit_enqueued, audit_enqueued_as_lease_winner,
};
pub use close_wait::CloseWaitOutcome;
pub use local_subs::LocalSubscription;
use local_subs::{local_notify, next_local_sub_id};
use notify::{NotifySinkSlot, notify_async};
pub use receipt::{DispatchFailure, DispatchPhase};
use receipt::{Ledger, Outcome};

/// std Mutex 取锁：中毒 = 持锁方已 panic、状态不可信——按 fail-fast 纪律传播
/// panic，禁静默吞掉毒锁继续跑。（crate 内共享：testing 域 TestStore 同纪律取锁。）
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect(
        "skeleton Store state lock poisoned（S::Clone/守卫 panic 违约——实例整体报废且状态不可取出，见 Store 文档「Panic 契约」节）: prior panic while holding the lock",
    )
}

/// 构造回灌句柄：仅装配构造器（[`SyncStore::new`] / [`SyncStore::restore_from`]
/// 共用装配段）经
/// [`Arc::new_cyclic`] 调用一次，产物缓存为 [`StoreInner::back`]——`sender()`
/// 克隆复用、排空循环借用，禁每 action 新建 Arc 闭包（稳态零新增分配的构成部分）。
fn back_sender<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    weak: &Weak<StoreInner<S, A, Op>>,
) -> ActionSender<A> {
    let weak = weak.clone();
    ActionSender::new(move |action| match weak.upgrade() {
        Some(inner) => {
            // 与 dispatch 同闸——闭门 sender dispatch 显式 false 并计数；
            // push/夺权/首 pop 同临界区（锁消融，论证同 enqueue/dispatch）。
            // 同栈判定经 admission_context——排空线程在 run 内回灌计深，
            // 他线程异步回灌=外部流量（环防护责任边界见 EffectRuntime::run）。
            let admission = admission_context(inner.store_id, false);
            let origin = admission.origin;
            match enqueue(&inner, action, admission) {
                Enqueue::Rejected => {
                    audit_emit(&inner, AuditEventKind::Rejected);
                    false
                }
                Enqueue::Full => {
                    // 容量满（issue#38）：显式拒绝（false）+ 计数留痕，不阻塞。
                    audit_emit(&inner, AuditEventKind::Rejected);
                    false
                }
                Enqueue::Queued(_, _) => {
                    // 竞争态（已有在任排空者）：纯准入——框架只保证该 action 已入队、
                    // 不等它的完成边界。返回瞬间该 action 是否已被 reduce 不作任何
                    // 保证（取决于在任排空者的推进速度）⇒ 下游不得据「竞争态」推断
                    // 「此刻读状态仍是旧值」。契约口径见 skeleton::ActionSender::dispatch。
                    audit_enqueued(&inner, origin);
                    true
                }
                Enqueue::Draining(_, _, first) => {
                    // 静默态（无在任排空者）：本线程夺得排空权 ⇒ 同步排空至 idle
                    // ⇒ 返回时该 action 已被 reduce、其完成边界必然已达成。此路径
                    // 强于 Store::dispatch 的 per-action 完成边界契约——后者不承诺
                    // 队列为空。契约（两条路径 + bool 的唯一含义）见
                    // skeleton::ActionSender::dispatch 的 rustdoc。
                    audit_enqueued_as_lease_winner(&inner, origin, first.as_ref());
                    drain_owned(&inner, first);
                    true
                }
            }
        }
        None => false,
    })
}

/// 订阅回调闭包（私有别名收口 type_complexity，零公共面增量）
type SubscriberCallback<S> = Box<dyn FnMut(&S) + Send>;

struct SubscriptionSlot<S> {
    notify: Mutex<SubscriberCallback<S>>,
    /// 存活闸（issue#74）：票据 Drop / panic 摘除先原子失活（幂等），缓存快照
    /// 回调前复查——失活槽不再被调用。单一真源，不另立第二套登记表。
    alive: AtomicBool,
}

/// 排空权独占字段：仅持排空权（[`StoreInner::draining`] 旗互斥下的 drain 循环）
/// 触及的可变态。锁消融的载体——`effect_buf`、订阅集快照缓存与异步投递收集缓冲
/// 全 dispatch 生命周期内只有排空者读写（重入 dispatch 只入队即返，到不了这三处），
/// 互斥由排空旗外部同步，无需各自上锁：原 `Mutex` 的 lock/unlock（每 action 3 对）
/// 与毒锁面一并消融。
struct DrainExclusive<T> {
    value: UnsafeCell<T>,
}

// SAFETY（建立者）：排空权互斥由排空旗 `StoreInner::draining` 的 swap 建立——
// `enqueue` 在 queue 锁内 `swap(true)` 判首、`drain` 同法判首，RMW 恒见修改序
// 最新值，故同一时刻至多一个线程持权（push 与夺权同临界区合拢，见 enqueue 注）。
// SAFETY（维持者）：三处独占字段（`effect_buf`/`notify_cache`/`notify_sink_buf`）
// 的每次读写都必须经 [`ExclusiveCell::with_mut`]，且调用点必须在
// [`drain_owned`] 的 `DrainGuard` armed 区间内；重入路径（reducer/订阅回调/
// 解释器内 dispatch 或 sender dispatch）只经 `enqueue` 入队即返，不入 with_mut。
// **唯一例外 = 关闭路径的静止窗**（issue#61 `with_quiescent_effect_bufs`：显式
// 丢弃与终态报告）：调用方已持 `queue` 锁且复核「无排空租约 + 活跃队列空」——
// 闭门后无生产者、排空者无记录可处理（不会触达本 cell）、`Effect::Emit` 回灌只
// 在排空者的转发环内发生；读者经 `queue` 锁串行 ⇒ 至多一个 `&mut`。热路径
// （dispatch 排空环）的访问点与开销零变化。
// `effect_buf` 一个字段内含暂存区与已提交待转发队列两段（见 [`EffectBufs`]）：
// 一次 with_mut 取两段的 &mut——不新增独占字段。
// 「调用点全集」即本约束的检查清单：排空环三处（`effect_buf`/`notify_cache`/
// `notify_sink_buf`）+ 关闭路径静止窗一处（`effect_buf`，见上）；新增访问点必须
// 同时扩清单。
// SAFETY（违反后果）：两线程同时拿到 `&mut T`（排空旗旁路，或 `&mut` 活到排空权
// 释放之后）= `UnsafeCell` 别名写，Vec 扩容/清空撕裂、迭代器失效，属未定义行为
// 而非可兜底的 panic；`tests/unsafe_evidence.rs` 的不变量测试与其变异验证是该
// 论断的可执行面。`T: Send` 使独占访问可跨线程交接（排空权可被任意线程夺得）。
unsafe impl<T: Send> Sync for DrainExclusive<T> {}

impl<T> DrainExclusive<T> {
    fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    /// 独占区间唯一入口：`f` 执行期间本 cell 的 `&mut T` 唯一有效，且不逃逸
    /// `f`（不跨排空权释放点存活）。默认构建在 `ExclusiveCell` 单变体上直通，
    /// 无 tag、无分支开销。
    ///
    /// SAFETY: 调用方必须在排空权区间内（`DrainGuard` armed）**或**关闭路径的
    /// 静止窗内（持 `queue` 锁且已复核无租约/队列空，见类型级安全纪要的例外）；
    /// 建立者/维持者/违反后果见上方类型级安全纪要。
    fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        // SAFETY: 调用方持排空权（DrainGuard armed）或处于关闭路径静止窗，
        // 见类型级安全纪要
        f(unsafe { self.get_mut() })
    }

    /// SAFETY: 只由 [`Self::with_mut`] 在同一排空权区间内借用；调用方必须持
    /// 排空权，且不得让返回的 `&mut T` 活到排空权释放之后。
    // clippy::mut_from_ref：本函数的不安全**就是**设计本体——外部同步（排空旗
    // 互斥）替代互斥锁，见类型级文档。allow 收口在唯一不安全面上。
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut T {
        &mut *self.value.get()
    }
}

/// 排空权独占载体（[`DrainExclusive`] 的类型选择层）：默认构建只有生产变体
/// （单变体枚举 = 无判别位、无分支，`with_mut` 直通）；`unsafe-evidence` 构建
/// 并列编入 `Mutex` 对照变体，仅 [`SyncStore::new_mutex_reference`] 选它——
/// 差分测试同进程对照两实现，生产装配路径（[`SyncStore::new`] /
/// [`SyncStore::restore_from`]）恒选生产变体，默认路径零开销。
enum ExclusiveCell<T> {
    /// 生产实现：`UnsafeCell` + 排空旗外部同步（默认构建唯一变体）。
    Unsynchronized(DrainExclusive<T>),
    /// 对照实现：`Mutex<T>` 把独占区间换成锁区间（蓝图 §9 证据面，默认不编译）。
    #[cfg(feature = "unsafe-evidence")]
    Locked(evidence::MutexExclusive<T>),
}

impl<T> ExclusiveCell<T> {
    /// 生产载体构造（装配默认面的唯一入口）。
    fn unsynchronized(value: T) -> Self {
        Self::Unsynchronized(DrainExclusive::new(value))
    }

    /// 对照载体构造（证据面唯一入口，默认构建不存在）。
    #[cfg(feature = "unsafe-evidence")]
    fn locked(value: T) -> Self {
        Self::Locked(evidence::MutexExclusive::new(value))
    }

    /// 独占区间：语义与调用点约束同 [`DrainExclusive::with_mut`]（排空权区间
    /// 内调用），对照变体额外由锁自证互斥。
    fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        match self {
            Self::Unsynchronized(cell) => cell.with_mut(f),
            #[cfg(feature = "unsafe-evidence")]
            Self::Locked(cell) => cell.with_mut(f),
        }
    }
}

// 零开销证据（默认构建）：单变体枚举不带判别位，布局与生产载体逐字节相同。
#[cfg(all(test, not(feature = "unsafe-evidence")))]
const _: () =
    assert!(std::mem::size_of::<ExclusiveCell<u64>>() == std::mem::size_of::<UnsafeCell<u64>>());

/// 三处排空权独占载体的装配组：构造入口按实现整组选择（生产 vs `Mutex` 对照），
/// 使 `StoreInner` 只留一处字面量（[`SyncStore::assemble`]）——对照装配与生产
/// 装配的其余字段因此不可能漂移。
/// 效果缓冲两段式（issue#44 事务边界）：暂存区（本次 reducer 调用的未提交
/// 输出）与已提交待转发队列（状态已发布的迁移的效果，跨 action 存活）。
///
/// 两段同属一个排空权独占载体（ExclusiveCell）——SAFETY 纪要的「三处独占字段」
/// 清单不变（effect_buf 一个字段、一次 with_mut 取两段的 &mut）。
struct EffectBufs<A, Op> {
    /// 暂存区：每次 reducer 调用独享（入口清零 ⇒ 跨调用不串味）；reduce
    /// panic ⇒ 整段丢弃（未提交的迁移零外部副作用）。
    staged: EffectBuf<A, Op>,
    /// 已提交待转发队列：提交点把暂存区整段并入队尾（保持 action 序）；解释器
    /// panic ⇒ 只留未转发的后缀（已转发前缀不重放），下一任成功排空续转。
    pending: VecDeque<PendingSegment<A, Op>>,
}

/// 已提交待转发**段**（j9q 零搬移移交）：一个 action 在提交点产出的全部效果。
///
/// 来源元数据在**段级固化**（issue#48 语义不变）：段内每条效果的
/// `origin_action_seq` = 产出该段的 action 的入队序号（store 内单调，队列锁内
/// 分配）；`effect_index` = **段内偏移**（一个 action 恰提交一段、reducer 输出序
/// 从 0 连续 ⇒ 偏移即「该 action 产出的第几条效果」，含 `Effect::Emit` 占位，
/// 与效果种类无关）。来源在提交点写入——不可在 panic 时反推（pending 效果
/// **跨 action 存活**：A 产出的段在 A 的转发 panic 后留存，后来由 B 的 dispatch
/// 触发续转并再次 panic ⇒ 被中断效果的来源是 A，而触发者是 B；只记「当前
/// drain 的 action」= 错误归因）。
/// （`EffectId` 在一个 store 内**全局唯一**，见 `skeleton::EffectId` 的唯一性
/// 纪律；它是逻辑句柄 ≠ 投递身份，见 `EffectKind` 文档。）
struct PendingSegment<A, Op> {
    /// 产出该段全部效果的 action 的入队序号（store 内单调，队列锁内分配）。
    origin_action_seq: u64,
    /// 段内下一条待转发效果的 0-based 位置（含回灌变体占位）。转发逐条推进，
    /// panic 时随段留存 ⇒ 续转的后缀从断点继续编号（与逐条包装版同值）。
    next_index: u64,
    /// 段内待转发效果。缓冲本体在 `staged` ↔ 段之间 `mem::swap` 循环
    /// （提交点移交、摘壳点回收）⇒ 稳态零新增分配（分配锚见 `tests/memgov.rs`）。
    buf: EffectBuf<A, Op>,
}

impl<A, Op> EffectBufs<A, Op> {
    /// 两段皆空（装配期构造一次，此后各自复用容量）。
    fn new() -> Self {
        Self {
            staged: EffectBuf::new(),
            pending: VecDeque::new(),
        }
    }

    /// 提交点：暂存区**整段并入**已提交待转发队列（j9q：`mem::swap` O(1) 三
    /// 指针整体移交，替代逐条包装——旧版每条一次 move，成本 O(n·sizeof(Op))，
    /// 512B/4096B 载荷实测约 +50%；来源元数据固化在段级，逐条版的
    /// `(origin_action_seq, effect_index)` 语义零变化——审计事件/失败回执/
    /// 探针全部同值）。swap 后 `staged` 留空壳（容量 0），摘壳点把满缓冲换回
    /// （稳态零新增分配，分配锚见 `tests/memgov.rs`）。
    fn commit(&mut self, origin_action_seq: u64) {
        if self.staged.is_empty() {
            return;
        }
        let mut seg = PendingSegment {
            origin_action_seq,
            next_index: 0,
            buf: EffectBuf::new(),
        };
        std::mem::swap(&mut seg.buf, &mut self.staged);
        self.pending.push_back(seg);
    }
}

struct ExclusiveCells<S, A, Op> {
    effect_buf: ExclusiveCell<EffectBufs<A, Op>>,
    notify_cache: ExclusiveCell<NotifyCache<S>>,
    notify_sink_buf: ExclusiveCell<Vec<Arc<NotifySinkSlot>>>,
}

impl<S, A, Op> ExclusiveCells<S, A, Op> {
    /// 生产装配（默认公共面的唯一路径）。
    fn unsynchronized() -> Self {
        Self {
            effect_buf: ExclusiveCell::unsynchronized(EffectBufs::new()),
            notify_cache: ExclusiveCell::unsynchronized(NotifyCache {
                version: 0,
                slots: Vec::new(),
            }),
            notify_sink_buf: ExclusiveCell::unsynchronized(Vec::new()),
        }
    }

    /// 对照装配（证据面唯一路径，默认构建不存在）。
    #[cfg(feature = "unsafe-evidence")]
    fn locked() -> Self {
        Self {
            effect_buf: ExclusiveCell::locked(EffectBufs::new()),
            notify_cache: ExclusiveCell::locked(NotifyCache {
                version: 0,
                slots: Vec::new(),
            }),
            notify_sink_buf: ExclusiveCell::locked(Vec::new()),
        }
    }
}

/// 订阅集快照缓存（锁消融：稳态 dispatch 对订阅广播零锁读）。
/// `version` 与 [`StoreInner::subs_version`] 对账：订阅增删/票据注销/回调 panic
/// 摘除都自增版本，排空者版本不匹配才冷重建（重建顺带代答回收死 Weak）；
/// 匹配则直接复用 `slots`——稳态（订阅集不变）单次 Acquire load + 遍历，零锁。
struct NotifyCache<S> {
    version: usize,
    slots: Vec<Arc<SubscriptionSlot<S>>>,
}

/// Run-ID 跟踪视界的保守默认容量（issue#62）。**数值已拍板 keep current**
/// （2026-11-23，正典见 `docs/reference.md` §资源上限与默认值 的决定记录）——
/// 改动走该表修订，别自选数字。
pub const DEFAULT_RUN_ID_TRACKING_CAPACITY: usize = 1024;

/// Run-ID 三态分类（issue#62）：消费方据此区分「确定未知的 Cancel」与
/// 「不可判定」——**被驱逐的 id 绝不判成 [`RunIdClass::DefinitelyUnknown`]**。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunIdClass {
    /// 保留视界内见过：该 id 的 `Run` 在跟踪窗内发出过。
    Known,
    /// 保留视界内**确定**未见过（诊断启用、视界完整且从未发生驱逐）。
    DefinitelyUnknown,
    /// 不可判定：诊断未启用 / 视界残缺（启用晚于首个准入）/ 已饱和驱逐
    /// （该 id 可能已被驱逐）。**不承诺**该 id 存在或不存在。
    NotTracked,
}

/// 历史 Run-ID 诊断（issue#62）：**默认关闭**的窄口径 runtime 诊断置
/// （[`SyncStore::enable_run_id_diagnostics`]）——不是通用策略/配置框架。
///
/// 契约（维护者定稿）：
/// - 默认关闭；`Run`/`Cancel` 的转发路径与诊断是否启用**无关**（诊断只观测）；
/// - 禁用路径零锁零分配：一次 `OnceLock::get` 即返（不登记、不分配）；
/// - 启用后是有界最近集：容量满即驱逐最旧，驱逐计数显式上浮；
/// - 三态分类见 [`RunIdClass`]；旧 [`SyncStore::cancel_unknown_id`] 只数
///   **视界完整且无驱逐时的精确 miss**（不静默改义——见该读侧文档）。
struct RunIdDiag {
    /// `None` = 未启用。`OnceLock` 的读侧是一次原子 load（禁用路径的全部成本）。
    track: OnceLock<Mutex<RunIdTrack>>,
}

/// 跟踪窗状态（只在启用后存在）。
struct RunIdTrack {
    /// 有界最近集容量（≥ 1）。
    capacity: usize,
    /// 视界是否完整：必须在**首个准入之前**启用——否则早先的 `Run` 未被登记，
    /// miss 只能判 `NotTracked`（保守方向：不把可能存在的 id 判成未知）。
    horizon_complete: bool,
    /// 最近 ID（插入序 = 驱逐序）。
    order: VecDeque<u64>,
    /// 精确成员表（与 `order` 同步维护；驱逐时两处同删）。
    ids: HashSet<u64>,
    /// 饱和驱逐计数（> 0 ⇒ 之后一切 miss 均为 `NotTracked`）。
    evicted: u64,
    /// 精确 miss 计数：[`SyncStore::cancel_unknown_id`] 的唯一来源。
    exact_unknown: u64,
}

impl RunIdDiag {
    const fn new() -> Self {
        Self {
            track: OnceLock::new(),
        }
    }

    /// 门闸：禁用路径的全部成本 = 这一次原子 load。
    fn is_enabled(&self) -> bool {
        self.track.get().is_some()
    }

    /// 首次启用生效；已启用时是 no-op（容量与视界在首次启用时固定）。
    fn enable(&self, capacity: usize, horizon_complete: bool) {
        let _ = self.track.set(Mutex::new(RunIdTrack {
            capacity: capacity.max(1),
            horizon_complete,
            order: VecDeque::new(),
            ids: HashSet::new(),
            evicted: 0,
            exact_unknown: 0,
        }));
    }

    fn note_run(&self, id: u64) {
        let Some(track) = self.track.get() else {
            return;
        };
        let mut t = lock(track);
        if t.ids.contains(&id) {
            return;
        }
        while t.order.len() >= t.capacity {
            let Some(oldest) = t.order.pop_front() else {
                break;
            };
            t.ids.remove(&oldest);
            t.evicted += 1;
        }
        t.ids.insert(id);
        t.order.push_back(id);
    }

    fn note_cancel(&self, id: u64) {
        let Some(track) = self.track.get() else {
            return;
        };
        let mut t = lock(track);
        // 只有「视界完整 + 从未驱逐」的 miss 才是**精确**未知（旧计数口径）。
        if t.ids.contains(&id) || !t.horizon_complete || t.evicted > 0 {
            return;
        }
        t.exact_unknown += 1;
    }

    fn classify(&self, id: u64) -> RunIdClass {
        let Some(track) = self.track.get() else {
            return RunIdClass::NotTracked;
        };
        let t = lock(track);
        if t.ids.contains(&id) {
            RunIdClass::Known
        } else if t.horizon_complete && t.evicted == 0 {
            RunIdClass::DefinitelyUnknown
        } else {
            RunIdClass::NotTracked
        }
    }

    fn tracked_count(&self) -> usize {
        self.track.get().map_or(0, |t| lock(t).ids.len())
    }

    fn capacity(&self) -> usize {
        self.track.get().map_or(0, |t| lock(t).capacity)
    }

    fn evicted_count(&self) -> u64 {
        self.track.get().map_or(0, |t| lock(t).evicted)
    }

    fn cancel_unknown_count(&self) -> u64 {
        self.track.get().map_or(0, |t| lock(t).exact_unknown)
    }
}

/// 容量发布窗口停车闸（**仅 `cfg(test)` 构建存在**，issue#95）：把准入 / 停放
/// 线程停在「已按旧 cap 裁决、尚未提交」的临界区里，供容量 setter 交错的
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

    /// 停靠点调用（**持对应 population 锁时进入**）：布防则置进入位并 park 到放行。
    fn park(&self) {
        let mut state = lock(&self.state);
        if !state.armed {
            return;
        }
        state.entered = true;
        self.cv.notify_all();
        while state.armed {
            state = self
                .cv
                .wait(state)
                .expect("capacity gate poisoned (prior holder panicked)");
        }
    }

    /// 测试面：布防（清上一轮进入位）。
    fn arm(&self) {
        let mut state = lock(&self.state);
        state.armed = true;
        state.entered = false;
    }

    /// 测试面：有界等待停靠线程进入窗口（OS 时钟 Condvar 超时，非 sleep）。
    fn wait_entered(&self, timeout: Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = lock(&self.state);
        while !state.entered {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, _) = self
                .cv
                .wait_timeout(state, deadline - now)
                .expect("capacity gate poisoned (prior holder panicked)");
            state = next;
        }
        true
    }

    /// 测试面：放行并解除布防（后续准入 / 停放不再停靠）。
    fn release(&self) {
        let mut state = lock(&self.state);
        state.armed = false;
        self.cv.notify_all();
    }
}

struct StoreInner<S, A, Op> {
    reducer: Arc<dyn Reducer<State = S, Action = A, Op = Op> + Send + Sync>,
    state: Mutex<Arc<S>>,
    runtime: Mutex<Box<dyn EffectRuntime<A, Op> + Send>>,
    queue: Mutex<VecDeque<ActionRecord<A>>>,
    /// 入队序号计数器（store 内单调递增）：**外部入队**路径在 queue 锁内分配，
    /// **`Effect::Emit` 回灌**路径在取锁之前分配 ⇒ 序号序与全局 FIFO 序**不保证**
    /// 恒等（已知张力，见 `docs/reference.md`）；回执按序号定位失败 action。
    next_seq: AtomicU64,
    /// 队列容量上限（issue#38）：等待被 reduce 的 action 数上限；`0` = 不限
    /// （默认）。正在执行的 action 已出队 → 不计入。容量满 = typed reject
    /// （[`SyncStore::try_dispatch`] 的 `Full`），绝不阻塞。
    capacity: AtomicUsize,
    /// 准入停靠闸（**仅 `cfg(test)`**，issue#95）：`enqueue` 在「已按旧 cap
    /// 裁决、尚未 push」的 queue 锁窗口停靠——容量 setter 此刻必被同一把锁挡住。
    #[cfg(test)]
    ingress_gate: CapacitySettle,
    /// 停放停靠闸（**仅 `cfg(test)`**，issue#95）：停放路径在「已按旧 cap
    /// 裁决、尚未 retain/reject」的 deferred_queue 锁窗口停靠。
    #[cfg(test)]
    deferred_gate: CapacitySettle,
    /// 容量满拒绝计数（fail-visible）：读侧 [`SyncStore::admission_full_count`]。
    admission_full: AtomicU64,
    /// 完成账本复用池（issue#40 性能协议：稳态零分配；issue#84 生命周期）：
    /// 空闲账本待取用，取出即复位、归还者唯一（防双重归还）。池容量 = 并发
    /// 等待者数（有界）。**池内不变量**：池内每个账本必是「终局已发布 + 无在途
    /// 后代 + 已 claim 归还」（`Ledger::reset` 在 debug/test 构建硬断言）——
    /// 终局发布前入池会让在途后代的结算记到下一个根的账上（跨归属）。
    ledger_pool: Mutex<Vec<Arc<Ledger>>>,
    /// 回灌预算止步的**停放队列**（issue#40）：超预算的 action 脱钩（续接
    /// 账本）后停放在此，**不留在主队列**——留在主队列会让它顶在队首把外部
    /// 流量反复顶住（饿死），且每任排空退出时都要重新脱钩（死循环）。
    /// 停放记录保留序号与载荷（零丢失），等显式恢复
    /// （[`SyncStore::resume_deferred`]，issue#49）；容量见
    /// `StoreInner::deferred_capacity`。
    deferred_queue: Mutex<VecDeque<ActionRecord<A>>>,
    /// 停放队列容量（issue#49）：**独立于外部准入容量**
    /// （`StoreInner::capacity`）——外部 ingress 与内部 feedback 不共用闸。
    /// `0` = 不限；默认 `DEFAULT_DEFERRED_CAPACITY`（保守值，**数值已拍板
    /// keep current**，2026-11-23，见 `docs/reference.md` §资源上限与默认值的
    /// 决定记录）。满 ⇒ 续接拿显式非成功终局（不消失、不标成停放）。
    deferred_capacity: AtomicUsize,
    /// 停放容量拒收计数（fail-visible）：读侧
    /// [`SyncStore::deferred_capacity_rejected`]。与
    /// `StoreInner::feedback_exceeded` 分账：后者数「撞预算」，本计数数
    /// 「撞预算且续接无法留存」。
    deferred_capacity_rejected: AtomicU64,
    /// 订阅集（Arc 包裹：票据 Drop 经共享句柄即时摘表）
    subs: Arc<Mutex<Vec<Weak<SubscriptionSlot<S>>>>>,
    /// 订阅闸（issue#63）：闭门翻转（closed=false→true）与可失败订阅登记
    /// （`try_subscribe`）共用的一把小锁——二者互斥 ⇒ 登记与闭门线性化。
    /// 只在登记/闭门路径取用，热 dispatch 路径不取（零热路径开销）。
    sub_gate: Mutex<()>,
    /// 回灌句柄缓存：每 store 构造一次；`sender()` 克隆、排空循环借用。
    back: ActionSender<A>,
    /// 效果缓冲（锁消融修订）：**两段式**——暂存区（每次 reducer 调用的未提交
    /// 输出）+ 已提交待转发队列（状态已发布的迁移的效果，跨 action 存活，见
    /// [`EffectBufs`]）；clear 保容量跨 dispatch 复用。
    /// 排空权独占（[`DrainExclusive`]）：原 `Mutex` 的 take/还 两对锁消融——
    /// reducer 期间不持任何锁的契约不变（UnsafeCell 无锁可毒），panic 路径
    /// 连容量都不再丢（缓冲留在原位，下次 clear 复用）。
    effect_buf: ExclusiveCell<EffectBufs<A, Op>>,
    /// 订阅集快照缓存（锁消融修订，见 [`NotifyCache`]）：稳态零锁读，
    /// 冷路径（订阅集变更后的首个 dispatch）才锁 `subs` 重建+代答回收死 Weak。
    notify_cache: ExclusiveCell<NotifyCache<S>>,
    /// 订阅集版本号：订阅增删（[`SyncStore::subscribe`]）、票据注销（[`Subscription`] Drop）、
    /// 回调 panic 摘除三路自增，快照缓存失效依据。Arc 装载使注销票据持句柄可 bump。
    subs_version: Arc<AtomicUsize>,
    /// 本地订阅闸（[`SyncStore::subscribe_local`]）：零本地订阅时通知路径一次
    /// relaxed 读即短路，不触达任何线程本地表。只作闸，不作精确计数读侧。
    local_subs: Arc<AtomicUsize>,
    /// 本地订阅跨线程 miss 计数（读侧 [`SyncStore::local_missed_notifications`]）：
    /// 通知段闸>0 但本线程表查无该 store 登记即 +1——「本地档挂注册线程、
    /// dispatch 来自他线程」的显形。
    local_missed: AtomicU64,
    /// 异步投递口注册表（ADR 0008）：表存 Weak、票据 Drop 经共享句柄即时摘表
    /// （与 `subs` 同构）；槽位与投递机制在 `notify` 子模块。
    notify_sinks: Arc<Mutex<Vec<Weak<NotifySinkSlot>>>>,
    /// 异步订阅闸（门控快路径，投递段一次 Acquire load 即返）：零异步订阅者时
    /// `state_version` 不自增——77ns 无订阅锚的守卫。`NotifySinkSlot::alive`
    /// 裁决保证摘除（panic 摘除 / 票据 Drop 两路竞争）只回落一次。
    notify_subs: Arc<AtomicUsize>,
    /// 状态版本水位（ADR 0008 §5.2 门控语义）：仅在异步订阅在册的落位拍自增
    /// （版本号 = 状态代数），无人订阅期间冻结、增减订不回退。经 publish 出站，
    /// 无独立读侧（本 ADR 唯一新读侧是 `notify_published`）。
    state_version: AtomicU64,
    /// 异步投递累计次数（成功投递逐口逐拍 +1，成本可见）。读侧
    /// [`SyncStore::notify_published`]。
    notify_published: AtomicU64,
    /// 异步投递收集缓冲（复用容量，消除每拍堆分配）：原实现每次 `collect` 新建
    /// Vec，有订阅者时每拍一次分配（PerfProbe 实测 total = 1.00 alloc/拍，见
    /// `verify/perf-probe/`）。排空权独占（同 `effect_buf`/`notify_cache`）：只在
    /// 排空环通知段读写，无需自锁。容量保留到订阅数峰值（每槽一针宽）。
    notify_sink_buf: ExclusiveCell<Vec<Arc<NotifySinkSlot>>>,
    /// store 身份号（本地订阅在注册线程 TLS 表中的键）：全局单调，进程内唯一。
    store_id: u64,
    /// 单写者排空守卫（确定性：同序 action 序列恒同终态；重入回灌只入队）
    draining: AtomicBool,
    /// 停机闸：置位后 push 路径（dispatch 与 sender().dispatch 同闸）恒拒——
    /// dispatch no-op、sender dispatch 显式 false，拒绝计数 fail-visible。Emit 回灌是已
    /// 接受 action 的内部延续（确定性迁移的收尾），不过闸。push
    /// 复检在 queue 锁内，置位与 push 先后经锁线性化——闭门后零静默执行。
    closed: AtomicBool,
    rejected_after_close: AtomicU64,
    /// 订阅回调 panic 摘除的成因留痕计数（摘除留痕+显式计数读侧可见）：`subscriber_count` 下跌只表明有人没了，本计数回答「为什么」。
    /// 毒订阅者摘除即 +1，已摘除者不重复计入。
    subscriber_panics: AtomicU64,
    /// 最近一次订阅回调 panic 的成因摘要：
    /// payload 为 `&str`/`String` 时存原文（200 字符截断），其余载荷类型不可
    /// 静态提取，存占位说明。只存最近一次；跨线程与本地两条摘除路径同一纪律。
    subscriber_panic_cause: Mutex<Option<String>>,
    /// 闭门后登记的订阅笔数：
    /// 登记照常、永不触发——本计数让该状态显形。与 [`StoreInner::rejected_after_close`]
    /// 分立：登记不是拒绝（票据有效、可注销、count 可查），并数会污染拒绝语义。
    subscriptions_after_close: AtomicU64,
    /// 解释器 panic 毒锁恢复计数：每次 into_inner 恢复自增（= 此前恰一次
    /// 解释器 panic，恢复后 clear_poison 复位毒位）。读侧 [`SyncStore::runtime_panic_count`]。
    runtime_panics: AtomicU64,
    /// reducer panic 计数：排空环 catch_unwind 观察点计数后原样 resume（传播
    /// 语义不变，只加留痕）。读侧 [`SyncStore::reducer_panic_count`]——四条
    /// panic/异常路径（解释器/订阅回调/回灌超限/reducer）的最后一处读侧收口。
    reducer_panics: AtomicU64,
    /// 回灌深度预算触发计数：单任排空连续回灌超 [`FEEDBACK_DEPTH_BUDGET`]
    /// 即停排+自增。读侧 [`SyncStore::feedback_budget_exceeded`]。
    feedback_exceeded: AtomicU64,
    /// 历史 Run-ID 诊断（issue#62）：**默认关闭**。启用后是有界最近集 + 三态
    /// 分类（known / definitely-unknown / not-tracked）；禁用路径零锁零分配
    /// （`Run`/`Cancel` 只多一次门闸 load，转发语义与诊断无关）。
    run_id_diag: RunIdDiag,
    /// Emit 回灌累计重入计数：`Effect::Emit` 永不
    /// 进效果转发路径（回环以 Action 重入队列），本计数供 [`TestStore::receive`]
    /// 空队列时如实归因「本轮产过 N 个回灌 Emit」——区别于「真没产效果」。
    /// 白盒读侧（TestStore 同模块、同线程读），零公共面增量。
    feedback_emitted: AtomicU64,
    /// 审计事件流闸（时序事后取证面）：false = 默认关闭——emit 点一次
    /// Acquire load 即返，零分配零锁（dispatch 锚不回退的依据）。Release/Acquire
    /// 配对 subscribe_events 的开闸与票据 Drop 的落闸。
    audit_gate: Arc<AtomicBool>,
    /// 审计汇聚槽（唯一订阅点的单槽内容）：有界环 + 实时回调 + 订阅代次。
    /// None = 未订阅。锁内只做数据搬运、回调锁外调用（禁持锁重入）。
    audit: Arc<Mutex<Option<AuditSink>>>,
    /// 审计事件单调序号计数器（store 级，跨订阅窗不重复；空洞 = 环溢出证据）。
    audit_seq: AtomicU64,
    /// 审计订阅代次计数器（票据 Drop 对账键，防旧票据误杀后继订阅流）。
    audit_gen: AtomicU64,
    /// 审计事件流前订被覆盖计数（读侧 [`SyncStore::audit_preempted`]）：
    /// `subscribe_events` 落槽时槽内已有订阅即 +1——单槽二次订阅静默杀掉
    /// 前订的显形。
    audit_preempted: AtomicU64,
    /// 审计实时投递丢弃计数（读侧 [`SyncStore::audit_delivery_drops`]）：
    /// 实时回调**未发生投递**的次数（待投递 FIFO 溢出 / 回调 panic 后该代次
    /// 队列作废 / 订阅替换时旧代次队列作废）。**与环溢出分账**：环溢出丢历史侧
    /// （容量因），本计数丢实时流（背压因）；两套证据互不替代。
    audit_delivery_drops: AtomicU64,
    /// 审计实时回调派发状态（issue#65 主路线①）：在途标志 + 有界待投递 FIFO
    /// （实时投递专用，**与审计环分开**）。锁序：审计环锁 → 本状态锁（全程
    /// 一致 ⇒ 无死锁）；回调调用一律在两把锁之外。
    audit_dispatch: Mutex<AuditDispatch>,
    /// 最近一次效果转发中断的完整归属（issue#61 close 报告的第四个 population；
    /// 与 [`AuditEventKind::EffectInterrupted`] 同一组五字段）。只在解释器 panic
    /// 路径写入 ⇒ dispatch 热路径零开销；`None` = 本 store 从未中断过转发。
    /// **与已提交后缀分账**：被中断的那条效果不在后缀里（已随转发丢弃、不重放
    /// ——#45 at-most-once），它只是后缀被留下的原因。
    interrupted_effect: Mutex<Option<InterruptedEffect>>,
    /// 停机等待广播（配对 `queue` 锁）：排空租约释放的两个点各 notify 一次
    /// （正常出口 [`DrainGuard::disarm`] + unwind 臂 [`DrainGuard::drop`]）。
    /// 谓词读的「租约已释放 + 队列空」两点同受 `queue` 锁保护，故与本锁配对；
    /// 本字段只做通知，不持状态——禁用作状态来源（防轮询旗复活）。
    stop_wait: Condvar,
    /// 测试专用：在途 dispatch 计数。0=无任何
    /// dispatch 在执行，此刻「draining=false 且队列非空」即孤儿 action（无人
    /// 排空、无人即将排空）。push 方先 +1 再入队、返回前 -1，使监视线程可用
    /// in_flight==0 排除「push 已入队、swap 未达」的正常瞬态窗。非 Drop 守卫：
    /// reducer panic 路径会残留计数，但仅影响同实例后续测试监视精度，零发布
    /// 产物开销。
    #[cfg(test)]
    in_flight: AtomicUsize,
}

/// Store：单写者队列 + 快照广播。`Op` 缺省 [`Infallible`] → [`Effect::Run`] 不可
/// 构造（[`Effect::Cancel`] 恒可构造、无注册时空转），零效果程序编译期证明无
/// 副作用（最小环直接配 [`NoFx`](skeleton::NoFx)，见其文档示例）。
///
/// # 闭门后的入口对照（停机闸）
///
/// | 入口 | 闭门后行为 | fail-visible 面 |
/// |---|---|---|
/// | [`Self::dispatch`] | 恒 no-op（签名不引入 Err） | 计入 [`Self::rejected_after_close`] |
/// | [`Self::try_dispatch`] | 返回 [`AdmissionOutcome::Closed`]（显式拒绝、不入队） | 返回值即信号；计入 [`Self::rejected_after_close`] |
/// | [`Self::dispatch_with_receipt`] | 返回 `Err(AdmissionOutcome::Closed)`（显式拒绝、无回执） | 返回值即信号；同一计数 |
/// | [`Self::resume_deferred`] | 返回 `resumed == 0`，停放记录仍归 store（不偷偷放行闭门后的工作） | 返回值即信号 |
/// | [`ActionSender::dispatch`] | 返回 `false`（显式拒绝，`#[must_use]` 兜底） | 同一计数 |
/// | [`Self::subscribe`] / [`Self::subscribe_local`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍，见下） | 计入 [`Self::subscriptions_after_close`]；`subscriber_count` 仍可查 |
/// | [`Self::try_subscribe`] | 返回 `Err(StoreClosed)`——显式拒绝、不登记（推荐停机序主路径） | 返回值即信号；登记未发生，[`Self::subscriptions_after_close`] 不计 |
/// | [`Notify::subscribe_notify`] | 同上行：登记照常、闭门后永无落位拍——信号静默 | 计入 [`Self::subscriptions_after_close`] |
///
/// dispatch 的静默 no-op 与 sender dispatch 的显式 `false` 是**保签名**与**停机显式化**
/// 之间的显式取舍：dispatch 面向动作源（不感染调用面），sender dispatch 面向异步回灌
/// （停机窗必须可观测）；两者闭门后共用同一拒绝计数，无观测死角。闭门
/// subscribe 照常登记是「票据即注销」模型的自然推论——通知静默可经
/// `subscriber_count` 交叉查证（已知语义，非缺陷）。
///
/// # 订阅面三档（ADR 0008）
///
/// - **跨线程同步**：[`Self::subscribe`]——回调在 dispatch 线程同步执行，需 `Send`；
/// - **本地线程亲和**：[`Self::subscribe_local`]——免 `Send`，只在注册线程触发；
/// - **异步信号**：[`Notify::subscribe_notify`]——投递口每拍收一个单调版本号
///   （水位信号，非事件流），消费侧见版本即 [`Store::snapshot`] 拉取现态。
///
/// 投递序：**异步先行、同步回调随后**（异步投递排在通知段开头，同步档内部
/// 既有顺序不动）。**异步档不救同步档**：慢同步订阅者的耗时照常计入 dispatch；
/// 慢消费者靠投递口内自备通道/工作线程解耦（O(1) 是宿主契约义务，框架无法
/// 检测也无法阻止阻塞）。
///
/// # 效果执行两档（同步档 / 托管档）
///
/// 同一「两档并存 + 同一语义契约」在效果执行面的落法（蓝图 §11 项 6，承继
/// ADR 0008 的通知面先例）：`EffectRuntime` 只有一种实现形态要求——`run` 是
/// 同步方法（[`EffectRuntime::run`]），排空环在
/// `dispatch` 调用栈上内联调用它，**核心从不 `await`、从不等待任务句柄**。
///
/// - **同步档**：`run` 内联把活干完（有界、非阻塞、不 await）。耗时直接计入
///   dispatch；`run` panic 原样从 [`Self::dispatch`] 上浮（fail-visible，毒锁
///   在下一次效果转发时恢复并计入 [`Self::runtime_panic_count`]），实例不报废。
/// - **托管档**：`run` 只把工作 `spawn_tracked` 进 `crate::fx::FxScope` 的
///   生命周期集便返回——`dispatch` 返回 ≠ 效果完成。失败/取消经
///   `crate::fx::Spawned::join` 的 `crate::fx::TaskError` 与
///   `crate::fx::ShutdownReport` 分桶面可见；结果必须经 `back`
///   （[`ActionSender`]）回灌成普通 action——状态仍只由
///   reducer 推进，跨线程回灌按外部流量计（不受回灌深度预算约束，环防护责任
///   在解释器自身，见 [`EffectRuntime`] 契约注）。
///
/// 两档共用的边界规则：核心永不阻塞等异步完成；异步结果只经 `ActionSender`
/// 消息边界回灌；同步档内禁 `await`/禁阻塞 IO（阻塞 `run` = 阻塞整个状态机）。
/// 行为面与逐步坐标见 `docs/reference.md` 的「effect 执行两档」节与
/// `tests/effect_two_tier.rs`。
///
/// # Panic 契约
///
/// `S::Clone` 必须 panic-free：外部持有快照（强计数 ≥2）时，状态落位走
/// `Arc::make_mut` 克隆分支，`S::clone` 在 state 锁内执行——clone panic 即毒锁，
/// snapshot/dispatch 从此恒 panic，**实例整体报废且状态不可取出**。这是文档化
/// 显式契约而非可恢复路径：state 锁内「只剩 memcpy 与计数」的形态纪律不为用户
/// State 的 panic-free 性兜底。对照：`S::Drop` 的 panic 已由「旧快照锁外析构」
/// 排除（见 `drain_owned` 落位段）；解释器 panic 不报废实例（毒锁恢复+计数，
/// 见 [`Self::runtime_panic_count`]）；reducer panic 同不报废（计数+审计现场，
/// 见 [`Self::reducer_panic_count`]——action 本体已出队随栈展开丢失，状态不前进；
/// 本次调用已产的暂存效果随未提交迁移一并丢弃，不执行）。
///
/// ## Action 析构义务（unwind 清理期，issue#66）
///
/// 两件事分开说，别混成一条全局要求：
///
/// - **正常运行期**：`A` 的析构同样发生在所有内部锁之外（与 `S::Drop` 的
///   「旧快照锁外析构」同款纪律：锁内只剩搬移与计数）。本库**不**对一般运行
///   路径提出 panic-free 的 `Drop` 要求——`Drop` panic 与任何用户代码 panic
///   同形上浮，不做特殊处理。
/// - **排空环 panic 的展开清理期**（`DrainGuard` 的 `Drop`）：队列残留被整批
///   摘下、按值结算为失败，随后各条 `A` 载荷**在构造该 panic 的展开栈上**析构。
///   框架拥有并按值丢弃这些载荷 ⇒ 无法阻止其 `Drop` 执行，而 `catch_unwind`
///   隔离不是答案（捕得住 abort、挡不住重入，且把诊断策略塞进 `Drop` 是更差的
///   取舍——同 #46 的先例：宿主代码形状义务写进契约，不做框架侧探测）。⇒
///   **义务在宿主**：这类 `Drop` **必须 panic-free**（展开中二次 panic =
///   进程 abort），且**不得重入同一 Store**（放旗已在 `Drop` 前半完成 ⇒ 重入
///   会夺得新排空租约、在展开栈上跑完整个排空环）；只读面（[`Self::snapshot`] /
///   [`Self::pending_actions`] / [`Self::subscriber_count`] 等）安全。
///
/// 覆盖范围与暴露面：本段直接覆盖排空环 panic 的展开清理期（上面的 `DrainGuard`
/// 清理：先结算收据，再析构载荷）。同一义务在另一条路径上同样成立、框架同样
/// 无法兜底：**store 自身在既有展开中被析构**（栈展开带走 `SyncStore`）时，
/// 队列与停放队列的残留载荷也在展开栈上析构——差别只在它只剩弃队留痕，没有
/// 逐条收据结算。在途记录的完成元信息（内部 `CurrentMeta`）不含载荷：载荷已
/// 交给 reducer 按值消费，其析构不在框架手里。结算顺序不因本契约而变：
/// queue / ledger 锁的释放在前，收据结算与载荷析构在后。被中断的 action
/// **绝不重放**（#45 at-most-once），终局回执照常 fail-visible 结算
/// （#41 语义不变）。
pub struct SyncStore<S, A, Op = Infallible> {
    inner: Arc<StoreInner<S, A, Op>>,
}

/// 停机闸已闭：订阅登记窗口已关（[`SyncStore::try_subscribe`] 的显式失败面）。
/// 与 `crate::fx::ScopeClosed`、`RegisterError::Closed` 同族停机语义——
/// 框架其余停机入口全都显式失败，本类型补齐 subscribe 面的缺口。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreClosed;

impl std::fmt::Display for StoreClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("store closed for new subscriptions (accepting gate)")
    }
}

impl std::error::Error for StoreClosed {}

/// 有界准入结果（[`SyncStore::try_dispatch`] 的 typed 面，issue#38）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOutcome {
    /// 已准入（action 已入队；完成边界的同步面见 [`skeleton::Store::dispatch`]）。
    Accepted,
    /// 队列容量已满——action **未入队**（不部分插入）。过载显式化，绝不阻塞。
    Full,
    /// store 已闭门——action **未入队**。
    Closed,
}

/// 回灌深度预算（const 64）：单任排空内连续处理的回灌链深度上限。
/// 有界链（Undo→Replay、表单回写）深度个位数；自指回灌环（reducer 恒 Emit
/// 自身、解释器经 [`ActionSender::dispatch`] 同栈回灌）属用户程序病理，预算点
/// 止步+计数上浮（[`SyncStore::feedback_budget_exceeded`]）。
const FEEDBACK_DEPTH_BUDGET: u64 = 64;

/// 根因果回执的终局（issue#57：结果面的公开观测点）。
///
/// [`Self::Completed`] 与 [`Self::Deferred`] 的区分正是 legacy void
/// [`skeleton::Store::dispatch`] 无法表达的那条信息：某个因果后代撞上回灌深度
/// 预算（[`SyncStore::feedback_budget_exceeded`]）时，**状态与已转发效果都已
/// 生效，但被停放的子树没跑**——那是「未完成」，不是成功完成。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOutcome {
    /// 根 action 与全部因果后代终局成功（完整因果完成）。
    Completed,
    /// 至少一个因果后代被回灌深度预算**停放**（`retained` = 留存的停放记录数）。
    ///
    /// 不是成功完成：恢复走 [`SyncStore::resume_deferred`]（**无**自动重试）。
    Deferred { retained: u64 },
    /// 停放队列满：`rejected` 条续接**无法留存**（`retained` 条仍留存）。
    ///
    /// 与 [`Self::Deferred`] 分开表达——被拒记录不是「已停放待恢复」，而是本应
    /// 停放却被容量拒收；专用计数见 [`SyncStore::deferred_capacity_rejected`]。
    CapacityRejected { retained: u64, rejected: u64 },
    /// 根或某个因果后代失败（legacy [`skeleton::Store::dispatch`] 在同一路径上
    /// panic；结果面以值返回同一份失败摘要）。
    Failed(DispatchFailure),
}

/// 根 action 的回执句柄（issue#57）：结果面的可等待观测点。
///
/// 由 [`SyncStore::dispatch_with_receipt`] 产出。[`Self::wait`] 可**重复**调用
/// ——终局发布后不可变：后续恢复操作（[`SyncStore::resume_deferred`]）给的是
/// **分离的**续接回执，绝不改写本回执。
///
/// # 禁止在排空回调内等待
///
/// [`Self::wait`] 在排空回调（reducer / 解释器 / 订阅回调 / 审计回调）内调用
/// **panic**（fail-visible）：回调跑在排空环上，等自己的根回执 = 等排空环
/// ——自死锁（同 #46 sender 收据教训）。回调内派生工作请用 [`ActionSender`]
/// （纯准入）。
pub struct DispatchReceipt<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>
{
    seq: u64,
    ledger: Arc<Ledger>,
    inner: Arc<StoreInner<S, A, Op>>,
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>
    DispatchReceipt<S, A, Op>
{
    /// 根 action 的入队序号（store 内单调；回执身份，可与审计/trace 时间线对齐）。
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// 等待（终局已发布则直接读）本根的终局；可重复调用，结果不可变。
    pub fn wait(&self) -> DispatchOutcome {
        assert!(
            reentrant_context(self.inner.store_id).is_none(),
            "udf-frame: 禁止在排空回调内等待根回执（回调在排空环上，等它 = 自死锁）"
        );
        public_outcome(self.ledger.wait())
    }
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Drop
    for DispatchReceipt<S, A, Op>
{
    fn drop(&mut self) {
        // 等待型根的 owner 归还账本（issue#84）：终局未发布时**不**归还——活账本
        // 绝不进池（见 `Ledger::waiter_release`），随最后一个 Arc 消亡。
        release_ledger_by_waiter(&self.inner, &self.ledger);
    }
}

/// [`SyncStore::resume_deferred`] 的结构化结果（issue#49：无隐藏自动重试）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeferredResume {
    /// 本次送进主队列的记录数（`limit` 上界内）。
    pub resumed: usize,
    /// 本次返回时仍停放的记录数（含本次再次停放的；**不含**被容量拒收的）。
    pub remaining: usize,
    /// 本次恢复的工作里有几条以**再次停放**（并成功留存）收口。
    pub deferred_again: usize,
    /// 本次恢复的工作里有几条**因停放队列满被容量拒收**（非成功终局，不计入
    /// `deferred_again`）。
    pub capacity_rejected: usize,
}

/// 账本终局 → 公开结果面（`Deferred` 的两个计数分流成两个变体，issue#57）。
fn public_outcome(outcome: Outcome) -> DispatchOutcome {
    match outcome {
        Outcome::Completed => DispatchOutcome::Completed,
        Outcome::Failed(failure) => DispatchOutcome::Failed(failure),
        Outcome::Deferred { retained, rejected } if rejected > 0 => {
            DispatchOutcome::CapacityRejected { retained, rejected }
        }
        Outcome::Deferred { retained, .. } => DispatchOutcome::Deferred { retained },
    }
}

/// 停放队列的默认容量（issue#49；朴素有界 + 满则显式拒收）。
///
/// **数值已拍板 keep current**（2026-11-23，正典见 `docs/reference.md`
/// §资源上限与默认值的决定记录）：取保守值并保证可配
/// （[`SyncStore::set_deferred_capacity`]）可观测
/// （[`SyncStore::deferred_capacity`]）；改动走该表修订。
const DEFAULT_DEFERRED_CAPACITY: usize = 64;

/// 入队来源标签：外部流量 = [`ActionOrigin::External`]（深度清零）；
/// 同栈回灌重入 = [`ActionOrigin::Feedback`]（携带入队时深度）；
/// 显式恢复的续接 = [`ActionOrigin::Resumed`]（**新回灌 epoch**：深度清零，且
/// 携带它停放时的深度作停放来源，issue#49）。
///
/// 「同栈」判定不看调用者身份，只看排空帧栈（TLS）：排空线程
/// 处理某 action 期间（store_id 匹配），该线程上发生的一切入队——Emit 回灌、
/// 解释器 `back.dispatch`、订阅回调重入 dispatch——一律计深（预算按
/// dispatch 调用栈深度统一计数，不分来源标签，back 通道不再绕预算）；他线程
/// 或静止态的入队 = 外部流量，永不计深。
#[derive(Clone, Copy, PartialEq, Eq)]
enum ActionOrigin {
    External,
    /// 同栈回灌重入，值为入队时的回灌深度（= 产生本回灌的 action 深度 + 1）
    Feedback(u64),
    /// 被 [`SyncStore::resume_deferred`] 显式恢复的停放记录：本记录开**新回灌
    /// 深度 epoch**（自身深度 0，见 [`record_depth`]），`prior_depth` 是它**停放
    /// 时**的深度（停放来源留痕，issue#49）。
    Resumed {
        prior_depth: u64,
    },
}

thread_local! {
    /// 排空帧栈（TLS）：本线程当前正在处理的 action 上下文（store_id、回灌
    /// 深度、因果账本）。**栈**而非单槽——嵌套排空（A→B→A）时按 store_id
    /// 自内向外查最近匹配帧：单槽形态会把 A 的同栈回灌误判为外部流量（因果
    /// 父丢失 + 回灌深度被清零，预算失效）。RAII 出栈，panic 展开亦然。
    static DRAIN_FRAMES: RefCell<Vec<DrainFrame>> = const { RefCell::new(Vec::new()) };
}

/// 排空帧：当前 action 的处理上下文。
struct DrainFrame {
    /// 所属 store（帧与 store 一一对应）。
    store_id: u64,
    /// 当前 action 的回灌深度（其同栈子代深度 = 本值 + 1）。
    depth: u64,
    /// 当前 action 所属账本（同栈回灌子代共享此账本 = 因果父）。
    ledger: Arc<Ledger>,
}

/// 排空帧守卫：出栈（panic 展开亦然）——TLS 栈不跨 action 泄漏。
struct DrainFrameGuard;

impl Drop for DrainFrameGuard {
    fn drop(&mut self) {
        DRAIN_FRAMES.with(|cell| {
            cell.borrow_mut().pop();
        });
    }
}

/// 同栈回灌上下文：本线程正在排空同 store → (当前深度, 因果父账本)；
/// 否则 → None（外部流量：自开新根账本，无因果父）。
fn reentrant_context(store_id: u64) -> Option<(u64, Arc<Ledger>)> {
    DRAIN_FRAMES.with(|cell| {
        cell.borrow()
            .iter()
            .rev()
            .find(|frame| frame.store_id == store_id)
            .map(|frame| (frame.depth, Arc::clone(&frame.ledger)))
    })
}

/// 记录的回灌深度：外部 action = 0；同栈回灌 = 产生它的 action 深度 + 1；
/// 恢复的续接 = 0（新 epoch，否则立即再次撞预算——issue#49）。
fn record_depth(origin: ActionOrigin) -> u64 {
    match origin {
        ActionOrigin::External | ActionOrigin::Resumed { .. } => 0,
        ActionOrigin::Feedback(depth) => depth,
    }
}

/// 首条竞争记录的**临时排空帧**（issue#59 定稿：临时帧方案，非延迟投递）。
///
/// 三条夺租约路径（[`SyncStore::dispatch`] / [`SyncStore::try_dispatch`] /
/// 框架 [`ActionSender`]）在投递首条 pop 记录的 `Enqueued` 回调**之前**安装它
/// ⇒ 回调期间 `admission_context` 看到同栈上下文 ⇒ 回调内的同 store `dispatch`
/// 归为**因果后代**（共享本帧账本、计深 +1、`wait=false` 只入队即返），不会
/// 同步等外层排空。缺这一帧 = 嵌套 dispatch 被判为外部根（`wait=true`），而此
/// 刻 `draining` 已置 ⇒ `enqueue` 返回 `Queued` ⇒ 调用方在无界的 `Ledger::wait`
/// 上等自己 = 确定性单线程死锁。
///
/// **归属规则**：嵌套 action 的因果父 = **正在投递其 `Enqueued` 回调的首条
/// 记录**，不一定是恰好夺得排空权的外部调用点（回调跑在夺租约者的
/// [`drain_owned`] 之前，首条记录甚至可能不是它的 action）。
///
/// 守卫 RAII 弹出（panic 展开亦然），且返回即弹出——**先于** [`drain_owned`]
/// 安装该记录的真排空帧 ⇒ 首条记录自身的回灌深度不被重复计。
fn temp_drain_frame<A>(store_id: u64, first: &ActionRecord<A>) -> DrainFrameGuard {
    DRAIN_FRAMES.with(|cell| {
        cell.borrow_mut().push(DrainFrame {
            store_id,
            depth: record_depth(first.origin),
            ledger: Arc::clone(&first.ledger),
        });
    });
    DrainFrameGuard
}

/// 已准入的 action 记录（队列元素）：序号 + 载荷 + 来源 + 账本。
///
/// 序号 store 内单调；**外部入队**路径在 queue 锁内分配（入队线性化点），
/// **`Effect::Emit` 回灌**路径在取锁之前分配 ⇒ 序号序与全局 FIFO 序**不保证**
/// 恒等（已知张力，见 `docs/reference.md`）。
/// `ledger` 是完成账本：外部根自持新账本；同栈回灌**共享**根账本（因果父）
/// ——共享而非各持一份，是因果后代屏障（根账本在全部因果后代终局前不发布
/// 终局）的实现基础。
struct ActionRecord<A> {
    /// store 内单调序号。
    seq: u64,
    /// action 载荷。
    action: A,
    /// 来源标签（外部流量 / 同栈回灌深度）。
    origin: ActionOrigin,
    /// 完成账本。
    ledger: Arc<Ledger>,
    /// 是否本账本的根（外部根 / 续接根 = true；因果后代 = false）。
    root: bool,
}

/// push 四态结果（锁消融：push/夺权/首 pop 同临界区一次获取）。
enum Enqueue<A> {
    /// 闭门拒绝（已计入 [`SyncStore::rejected_after_close`]）
    Rejected,
    /// 容量满拒绝（已计入 [`SyncStore::admission_full_count`]；action 未入队）
    Full,
    /// 已入队，排空权归在任排空者（其循环必覆盖本 push，论证见 [`enqueue`]）；
    /// 载荷为入队序号 + 完成账本（外部根据此等自己的完成边界）。
    Queued(u64, Arc<Ledger>),
    /// 已入队且夺得排空权：序号 + 账本 + 同临界区内 pop 出的首个记录。
    Draining(u64, Arc<Ledger>, Option<ActionRecord<A>>),
}

/// 准入上下文：来源标签 + 因果父账本（`None` = 外部根，自开新账本）。
struct Admission {
    /// 来源标签。
    origin: ActionOrigin,
    /// 因果父账本（同栈回灌共享根账本）；`None` = 外部根。
    parent: Option<Arc<Ledger>>,
    /// 是否等待完成边界（外部 dispatch = true；sender / try_dispatch = false）。
    wait: bool,
}

/// 准入上下文判定：本线程正在排空同 store → 同栈回灌（共享父账本 + 计深）；
/// 否则 → 外部根（新账本 + 深度清零）。
fn admission_context(store_id: u64, wait: bool) -> Admission {
    match reentrant_context(store_id) {
        Some((depth, ledger)) => Admission {
            origin: ActionOrigin::Feedback(depth + 1),
            parent: Some(ledger),
            // 同栈回灌永不等待（等待 = 自死锁）；其结果由根账本承担。
            wait: false,
        },
        None => Admission {
            origin: ActionOrigin::External,
            parent: None,
            wait,
        },
    }
}

/// 账本复用池取用（issue#40 性能协议：稳态零分配）：池空则新建；取出即复位
/// （终局 / 计数 / 失败摘要清零）。锁序 queue → pool（与入队同临界区）。
///
/// `waiter` = 新根的归属 token（issue#84）：等待型根由外部等待者归还，非等待
/// 型根由框架在其终局发布时归还——token 随取用写进账本状态。池内账本必经
/// **已终局 + 已 claim 归还**（`reset` 的不变式断言兜底）。
fn acquire_ledger<S, A, Op>(inner: &Arc<StoreInner<S, A, Op>>, waiter: bool) -> Arc<Ledger> {
    let mut pool = lock(&inner.ledger_pool);
    match pool.pop() {
        Some(ledger) => {
            ledger.reset(waiter);
            ledger
        }
        None => Ledger::new(waiter),
    }
}

/// 账本归还复用池（issue#84）：**终局发布是唯一可复用边界**。资格判定与 claim
/// 在账本锁内完成（`released` 保证恰好一次入池），放锁后才取 `ledger_pool`
/// 入池——禁同时持账本锁与池锁。
///
/// 归还时机：任何可能使账本终局的转移之后（`finish_root` / `descendant_done`
/// / `detach_deferred`）都尝试一次——非等待型根的最后一个后代结算时正是终局
/// 发布点。等待型根不走本路径（其 owner 是外部等待者，见
/// [`release_ledger_by_waiter`]）。
fn release_ledger<S, A, Op>(inner: &Arc<StoreInner<S, A, Op>>, ledger: &Arc<Ledger>) {
    if ledger.claim_release() {
        lock(&inner.ledger_pool).push(Arc::clone(ledger));
    }
}

/// 等待者归还（等待型根的 owner）：`wait()` 返回终局后调用——**消费** `Outcome`
/// 之后账本才可复用（issue#84：后代结算绝不把等待型根的账本放进池子）。
fn release_ledger_by_waiter<S, A, Op>(inner: &Arc<StoreInner<S, A, Op>>, ledger: &Arc<Ledger>) {
    if ledger.waiter_release() {
        lock(&inner.ledger_pool).push(Arc::clone(ledger));
    }
}

/// 共享 push 路径（[`SyncStore::dispatch`] / [`SyncStore::try_dispatch`] 与
/// [`ActionSender::dispatch`] 同闸同窗）。
/// 临界区一次获取：容量闸、序号、账本、因果计数、push、排空权 swap、首 pop
/// 全在 queue 锁内完成——准入的线性化点在此（序 → 计数 → 入队 → 夺权不可分）。
///
/// 交接论证（M1 语义保持）：swap 在 queue 锁内判定——
/// - 输给在任排空者：其尚未到出口（出口=持锁 pop None 后放旗），本 push 或被其
///   后续循环 pop 覆盖，或在其持锁出口判定之后才入队（此时本线程 swap 读到的是
///   disarm 后的 false，走赢家路径自排空）。零孤儿。
/// - 赢家 pop_front 取的是队首（可能非本 action）：全局 FIFO 由单队列单排空者
///   保证，先入先出序不因合并而变。
///
/// 容量闸（issue#38）：容量只约束**外部**准入；同栈回灌不受约束（回灌是已
/// 接受 action 的内部延续，挡住它 = 排空环自锁）。容量满 = typed reject，
/// 绝不阻塞（阻塞会与回调重入死锁）。
fn enqueue<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    action: A,
    admission: Admission,
) -> Enqueue<A> {
    let mut queue = lock(&inner.queue);
    // 持 queue 锁后复检 closed——close 的 store 与本复检经 queue 锁
    // 线性化合拢：复检先于 store 则 close 的 drain 必覆盖本 push；后于 store 则
    // 显式拒绝计数。封死「过闸后在锁等待窗被 close 抢跑、闭门后静默执行」的
    // TOCTOU。序论证：closed 单调，stale false 是唯一可能的方向偏（锁+drain
    // 兜底，见 close 注释），Acquire 配 close 的 Release 即足。
    if inner.closed.load(Ordering::Acquire) {
        inner.rejected_after_close.fetch_add(1, Ordering::SeqCst);
        return Enqueue::Rejected;
    }
    // 容量闸（外部准入）：队列中等待被 reduce 的 action 数达上限即拒（不部分
    // 插入）。正在执行的 action 已出队 → 不计入（口径见 [`SyncStore::set_capacity`]）。
    if admission.parent.is_none() {
        let cap = inner.capacity.load(Ordering::Relaxed);
        if cap > 0 && queue.len() >= cap {
            inner.admission_full.fetch_add(1, Ordering::SeqCst);
            return Enqueue::Full;
        }
        // 测试停靠点（issue#95）：已按旧 cap 裁决、push 尚未提交——容量 setter
        // 此刻只能被 queue 锁挡住（共锁线性化的可观测窗口）。
        #[cfg(test)]
        inner.ingress_gate.park();
    }
    // 序号 + 账本 + 因果计数：同一临界区内完成（入队即线性化点）。
    let seq = inner.next_seq.fetch_add(1, Ordering::SeqCst);
    let (ledger, root) = match &admission.parent {
        Some(parent) => {
            // 因果后代：共享父账本并累加未终局后代计数（锁序 queue → ledger）。
            // 计数先于 push：父账本在后代可见前不可能发布终局。
            parent.add_descendant();
            (Arc::clone(parent), false)
        }
        None => {
            // 外部根：自持账本（复用池取用——稳态零分配）。归属 token 随取用
            // 固化进账本状态（issue#84）：等待型根由等待者归还。
            (acquire_ledger(inner, admission.wait), true)
        }
    };
    let record = ActionRecord {
        seq,
        action,
        origin: admission.origin,
        ledger: Arc::clone(&ledger),
        root,
    };
    queue.push_back(record);
    // AcqRel：RMW 恒见修改序最新值；Acquire 侧同步前任 Release 放旗，Release 侧
    // 发布本任排空权。
    if inner.draining.swap(true, Ordering::AcqRel) {
        return Enqueue::Queued(seq, ledger);
    }
    let first = queue.pop_front();
    Enqueue::Draining(seq, ledger, first)
}

// S: Sync：ActionSender 的回灌闭包跨线程共享（Send+Sync），Weak<StoreInner> 的
// Send/Sync 要求 StoreInner 整体 Send+Sync——其中 Mutex<Arc<S>> 传导出 S: Sync。
impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Store
    for SyncStore<S, A, Op>
{
    type State = S;
    type Action = A;
    type Op = Op;

    /// 唯一写入口；**同步到本 action 的完成边界**（issue#40）。
    ///
    /// 完成边界 = 本 action 自身各阶段完成（状态提交 → 效果转发 → 通知段返回）
    /// **且**其全部因果后代（同栈回灌引发的子动作）终局。并发外部调用者不再
    /// 只入队即返——返回即代表本 action 已生效（后继 `snapshot()` 可见新状态）。
    ///
    /// **例外一（同栈重入）**：本线程正在排空**同一** store 时（含嵌套排空
    /// A→B→A 的 A 帧命中），dispatch 只入队不等待——等待即自死锁；其结果由
    /// 外层根调用者的账本承担（因果后代屏障）。
    ///
    /// **例外二（回灌预算停放，Deferred）**：某个因果后代撞上回灌深度预算
    /// （[`Self::feedback_budget_exceeded`]）时，根回执以 `Deferred` 收口 ⇒
    /// **本方法的 void 返回不证明完整因果完成**（状态与已转发效果都已生效，
    /// 但被停放的子树没跑）。要拿到那个终局请用结果面
    /// [`Self::dispatch_with_receipt`]（本方法签名不变、行为不变）。
    ///
    /// 闭门后恒 no-op 并计入 [`Self::rejected_after_close`]，签名不变（取舍
    /// 对照 [`ActionSender::dispatch`] 的显式 `false`——见类型文档「闭门后的
    /// 入口对照」表）。容量满（配了 [`Self::set_capacity`]）按过载显式失败
    /// ——typed 面见 [`Self::try_dispatch`]。
    fn dispatch(&self, action: A) {
        match self.admit(action) {
            // 过载显式失败（issue#38）：legacy dispatch 无失败面 ⇒ 禁静默弃 action
            //（typed 面见 [`Self::try_dispatch`]）。
            Err(AdmissionOutcome::Full) => panic!(
                "udf-frame: SyncStore dispatch 过载——队列容量已满（typed 面见 SyncStore::try_dispatch）"
            ),
            // 已闭门：no-op（拒绝计数在准入段已记）。
            Err(_) => {}
            // 同栈回灌：只入队不等待（自死锁回避），结果由外层根账本承担。
            Ok((_, false, _)) => {}
            Ok((_, true, ledger)) => {
                let outcome = public_outcome(ledger.wait());
                // 等待型根的 owner 在消费终局后归还账本（issue#84：后代结算的归还
                // 资格被归属 token 排除，不会把它放进池子）。
                release_ledger_by_waiter(&self.inner, &ledger);
                match outcome {
                    // 回灌预算脱钩（Deferred）/ 完整完成：本方法无返回值，两者同形
                    // ——差别只在 [`Self::dispatch_with_receipt`] 的结果面。
                    DispatchOutcome::Completed | DispatchOutcome::Deferred { .. } => {}
                    DispatchOutcome::CapacityRejected { .. } => {}
                    // fail-visible：已接受但失败的 action 禁伪装成成功。
                    DispatchOutcome::Failed(failure) => panic!(
                        "udf-frame: SyncStore dispatch 失败——seq={} phase={:?} state_committed={} cause={}",
                        failure.seq, failure.phase, failure.state_committed, failure.cause
                    ),
                }
            }
        }
    }

    /// 拉取式快照。
    fn snapshot(&self) -> Arc<S> {
        Arc::clone(&lock(&self.inner.state))
    }

    /// 订阅状态通知；Drop=注销，漏注销不可表达（订阅表存 Weak，票据丢弃即失效）。
    /// 「失效」的精确语义：票据 drop 后**即时生效**——表项即时摘除（广播可见性：
    /// 注销票据不再收到通知；计数读侧 [`Self::subscriber_count`] 实时判活、读数
    /// 立即回落，无一拍滞后，见其时序节）。
    /// 通知缓存失效依据：push 后 Release 自增订阅集版本——稳态广播零锁读
    /// （顺序：push 先于 bump；快照重建载入版本 v 即保证 ≥v 前的全部入表可见，
    /// 论证见 drain 通知段）。票据持版本句柄，注销时自增（见 [`Subscription`]）。
    ///
    /// # Examples
    ///
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（[`Subscription`] 恒
    /// `#[must_use]`）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// store.subscribe(|_| {}); // ERROR: unused `Subscription` that must be used——丢弃即死订阅
    /// ```
    ///
    /// 绑定持有即持续收通知：
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// let _ticket = store.subscribe(|_| {}); // 绑定即持有，Drop 才注销
    /// store.dispatch(1);
    /// store.close();
    /// ```
    ///
    /// 停机序推荐走 [`SyncStore::try_subscribe`]（闭门显式 `Err`，见其文档）；
    /// 本方法保留签名作迁移期兼容面——闭门登记照常 + 计数
    /// （[`SyncStore::subscriptions_after_close`]），不静默。
    fn subscribe(&self, f: impl FnMut(&S) + Send + 'static) -> Subscription<S> {
        // 闸内「闭门观测 + 登记」（issue#63）：与 close 的闭门翻转互斥 ⇒
        // 闭门观测与登记同一线性化点（legacy 语义不变：闭门后仍登记并计数）。
        let _gate = lock(&self.inner.sub_gate);
        // 闭门登记显形（登记照常生效，但永无通知——见类型文档对照表）
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        self.insert_subscription(f)
    }

    /// 可克隆回灌句柄（解释器异步回灌的唯一通道）。克隆 StoreInner 缓存句柄
    /// （Arc 引用计数），不重建闭包。
    fn sender(&self) -> ActionSender<A> {
        self.inner.back.clone()
    }

    /// 停机闸：置 closed 后 best-effort 排空（与宿主装配序「解释器 close→排空
    /// idle→flush→drop」对齐）——drop 不静默弃队，但**不承诺 quiescence**。
    /// 本调用夺得排空租约时同步排至 idle（静默态，最常见路径）；他线程已持
    /// 租约时仅完成交接（闭门后 push 恒拒，其循环必达空）即返——**该路返回时
    /// 排空仍在进行**，要等静止请用 [`Self::close_wait`] /
    /// [`Self::close_wait_timeout`]（#64 拍板：close 保持现状，等待面独立）。
    ///
    /// **`close` 不是效果 flush**（issue#61 定稿）：主队列空时排空环不进转发环，
    /// 故解释器 panic 遗留的**已提交未转发效果后缀**会原样留存，且本方法**不
    /// 承诺**任何终态 pending 快照、**绝不**因收到停机请求就执行宿主
    /// `EffectRuntime`（效果可能做 I/O / 改外部系统，框架无法判定重放安全）。
    /// 要「等静止 + 读终态报告」用 [`Self::close_wait_with_report`]，要显式
    /// 清掉留存后缀用 [`Self::discard_pending_effects`]。
    /// 幂等。闭门后 push 恒拒：dispatch no-op、send 显式 `false`、subscribe
    /// 照常登记但永不触发——三入口对照见类型文档「闭门后的入口对照」表。
    fn close(&self) {
        // Release 配对 push 路径的 Acquire 复检。线性化不靠原子序：close 的
        // drain 与 dispatch 的 push 在 queue 锁上串行——stale false（迟见的
        // 闭门）只会让 push 照常入队，随后 close 的 drain 必经锁覆盖该 push；
        // 反向（未见 store 即读到 true）单变量一致性上不可能。Release 只保证
        // 「读到 true 时闸前所有写可见」，单调闸下这是唯一的错误方向防护。
        // 闭门翻转与订阅登记互斥（issue#63）：closed=false→true 的翻转取
        // 同一把订阅闸 ⇒ try_subscribe 与本翻转线性化互斥（不会「已登记
        // 但 store 已关」）。闸在排空前释放：排空/回调期间不持闸。
        {
            let _gate = lock(&self.inner.sub_gate);
            self.inner.closed.store(true, Ordering::Release);
        }
        drain(&self.inner);
    }

    /// 停机闸状态。
    fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }
}

/// 异步（信号式）通知档的登记面实现（ADR 0008）：与 [`Store`] 正交的独立端口
/// ——「六方法即全契约」纪律不因此扩员。语义细则（版本号水位/门控/失败面）见
/// `skeleton::notify` 模块文档；投递在排空环通知段、同步回调**之前**（异步档
/// 不救同步档：同步订阅者耗时照常计入 dispatch）。
impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> Notify<S>
    for SyncStore<S, A, Op>
{
    /// 登记投递口（信号式异步档）。载荷 = 单调状态版本号：在册期间逐拍 +1、
    /// 无人订阅期间冻结、增减订不回退；消费侧见版本变化即 [`Store::snapshot`]
    /// 拉取现态，版本跳跃 = 中间拍被合并（重取现态自愈，撕裂判定依据）。
    ///
    /// 失败面（复用既有读侧，ADR §5.5，不造第二套）：投递口 panic ⇒
    /// `catch_unwind` 捕获 + fail-closed 摘除 + [`Self::subscriber_panics`] /
    /// [`Self::subscriber_panic_last_cause`] 留痕（同订阅回调纪律）；闭门后
    /// 登记照常、永不发信号 ⇒ [`Self::subscriptions_after_close`]；投递成本 ⇒
    /// [`Self::notify_published`]。投递口 O(1) 是宿主契约义务（
    /// `skeleton::notify::NotifySink` 文档），框架无法检测也无法阻止阻塞
    /// ——重活请在投递口内自备通道/工作线程移出。
    ///
    /// # Examples
    ///
    /// 合并式投递口挂载后，dispatch 每拍投递一位版本号（水位从 1 起步）；
    /// 成本读侧同步累计：
    ///
    /// ```
    /// use skeleton::notify::{Notify, NotifySink};
    /// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::sync::atomic::{AtomicU64, Ordering};
    /// use std::sync::Arc;
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u64; type Action = u64; type Op = Infallible;
    ///     fn initial(&self) -> u64 { 0 }
    ///     fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 { s + a }
    /// }
    ///
    /// #[derive(Clone)]
    /// struct Latest(Arc<AtomicU64>);
    /// impl NotifySink for Latest {
    ///     fn publish(&self, version: u64) { self.0.store(version, Ordering::Relaxed); }
    /// }
    ///
    /// let store = SyncStore::new(Counter, NoFx);
    /// let sink = Latest(Arc::new(AtomicU64::new(0)));
    /// let _ticket = store.subscribe_notify(sink.clone()); // 绑定持有，Drop 才注销
    /// store.dispatch(7);
    /// assert_eq!(sink.0.load(Ordering::Relaxed), 1); // 首拍版本 = 1
    /// store.dispatch(2);
    /// assert_eq!(*store.snapshot(), 9);
    /// assert_eq!(store.notify_published(), 2);
    /// # store.close();
    /// ```
    fn subscribe_notify(&self, sink: impl NotifySink) -> Subscription<S> {
        // 闭门登记显形（与 subscribe 同一纪律）：登记照常生效，但闭门后
        // 排空环不再产生落位拍——永无信号。
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        notify::register(
            &self.inner.notify_sinks,
            &self.inner.notify_subs,
            Box::new(sink),
        )
    }
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> SyncStore<S, A, Op> {
    /// 装配最小运转环：`root` 是**根** Reducer（[`Reducer::initial`] 产出完整根
    /// 状态），`fx` 是效果解释器（核心只转发，永不解释）。
    ///
    /// 与 `compose` 域的 `combine` 的关系：`combine` 产物是**状态分片不是根**
    /// （只实现 `Fragment`，无 `initial`），不能作 `root` 传入——分片作根即
    /// **E0277 编译期拒绝**（`Fragment` ≠ `Reducer`，错误信息自带修复指引）。
    /// 正确用法：父 Feature 的 `initial` 显式构造含子切片初值的根状态、
    /// `reduce` 内委托分片，父 Feature 本体作 `root`（完整形态见
    /// `tests/combine_teststore.rs` 固定装置）。
    ///
    /// 构造即接线：回灌句柄经 `Arc::new_cyclic` 缓存（[`SyncStore::sender`] 克隆复用），
    /// 初始状态 = `root.initial()`；此后 [`SyncStore::dispatch`] / `sender().dispatch` 驱动。
    ///
    /// # 界不对称（有意，接口维 R1）
    ///
    /// 根 Reducer 要 `Send + Sync`、解释器只要求 `Send`，依据在 std 类型律：
    /// store 部件经 `Arc<StoreInner>` 跨线程共享，`Arc<T>: Sync` 要求 `T: Sync`，
    /// 故 reducer 的 trait 对象必须挂全 `Send + Sync`；解释器装箱在 `Mutex`
    /// 之后（`Mutex<T>: Sync` 只要求 `T: Send`）——锁后独占访问、从不并发
    /// 共享，能安全移动即足，多要 `Sync` 是白缴的税。
    ///
    /// # 解释器观测的惯用形（fx 按值收走后）
    ///
    /// `new` 按值拿走解释器——产品代码要观测解释器行为（处理过哪些 op、
    /// 何时处理）时，把观测面放进共享句柄：解释器持 `Arc<Mutex<…>>` 克隆份，
    /// 宿主持原份，装配后随处可读（测试语境的收口见 `testing::TestStore`；
    /// 本例是产品语境的手写形）：
    ///
    /// ```
    /// use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::convert::Infallible;
    /// use std::sync::{Arc, Mutex};
    ///
    /// #[derive(Debug, PartialEq)]
    /// enum Op { Ping }
    ///
    /// struct Pinger;
    /// impl Reducer for Pinger {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = Op;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, _a: u32, out: &mut EffectBuf<u32, Op>) -> u32 {
    ///         out.push(Effect::Run { id: EffectId::new(1), op: Op::Ping });
    ///         *s
    ///     }
    /// }
    ///
    /// /// 解释器外壳：执行逻辑外的观测面（此处记录处理序；真实宿主在同一位置执行）。
    /// struct Observed {
    ///     seen: Arc<Mutex<Vec<Op>>>,
    /// }
    /// impl EffectRuntime<u32, Op> for Observed {
    ///     fn run(&mut self, effect: Effect<u32, Op>, _back: &ActionSender<u32>) {
    ///         if let Effect::Run { op, .. } = effect {
    ///             self.seen.lock().unwrap().push(op); // 解释后记录（时机自选）
    ///         }
    ///     }
    /// }
    ///
    /// let seen = Arc::new(Mutex::new(Vec::new()));
    /// let store = SyncStore::new(Pinger, Observed { seen: Arc::clone(&seen) });
    /// store.dispatch(1);
    /// assert_eq!(*seen.lock().unwrap(), vec![Op::Ping]); // 装配后随处可读
    /// # store.close();
    /// ```
    ///
    /// # drain 回调里 join 的 worker ⇒ 用框架提供的 sender（回调内 = 竞争态）
    ///
    /// 回调（[`skeleton::EffectRuntime::run`]）运行在排空环内：若它 join 的 worker
    /// 用**阻塞式** `Store::dispatch` 回调同一 store，则 worker 等回执、排空环等
    /// worker ⇒ **确定性互等死锁**。安全写法是 worker 用**框架提供的** sender
    /// （本例即回调收到的 `back`）：**回调运行在排空环内 ⇒ 该 sender 必处竞争态**
    /// ⇒ **纯准入**（立即返回、不等该 action 的完成边界）⇒ 回调可以 join 该 worker
    /// （worker 不与排空环互等），排队的 action 在**回调返回后**按 FIFO 处理。
    ///
    /// 语义**条件性**（勿当无条件「准入-only」）：框架提供的 sender 只在**竞争态**
    /// （已有在任排空者）纯准入；**静默态**（无在任排空者）它会夺得排空权并
    /// **同步排空至 idle**——此时返回即该 action 的完成边界。逐条规格见
    /// `skeleton::ActionSender::dispatch` 的契约。
    ///
    /// ```
    /// use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::sync::atomic::{AtomicUsize, Ordering};
    /// use std::sync::{Arc, mpsc};
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// #[derive(Debug, PartialEq)]
    /// enum Op { Work }
    ///
    /// /// 每个 action 计数一次（证明「恰好处理一次」）；只有动作 1 产一条效果。
    /// struct Start {
    ///     processed: Arc<AtomicUsize>,
    /// }
    /// impl Reducer for Start {
    ///     type State = u64; type Action = u64; type Op = Op;
    ///     fn initial(&self) -> u64 { 0 }
    ///     fn reduce(&self, s: &u64, a: u64, out: &mut EffectBuf<u64, Op>) -> u64 {
    ///         self.processed.fetch_add(1, Ordering::SeqCst);
    ///         if a == 1 {
    ///             out.push(Effect::Run { id: EffectId::new(1), op: Op::Work });
    ///         }
    ///         *s
    ///     }
    /// }
    ///
    /// /// 解释器：派生 worker 并 join 它——worker 只**提交**动作（不等待完成）。
    /// struct Spawner {
    ///     admitted: Arc<AtomicUsize>,
    /// }
    /// impl EffectRuntime<u64, Op> for Spawner {
    ///     fn run(&mut self, _effect: Effect<u64, Op>, back: &ActionSender<u64>) {
    ///         let sender = back.clone();
    ///         let admitted = Arc::clone(&self.admitted);
    ///         let (tx, rx) = mpsc::channel();
    ///         let worker = thread::spawn(move || {
    ///             // 本回调运行在排空环内 ⇒ 竞争态 ⇒ 纯准入：立即返回，不等该 action 的完成边界。
    ///             if sender.dispatch(7) {
    ///                 admitted.fetch_add(1, Ordering::SeqCst);
    ///             }
    ///             let _ = tx.send(());
    ///         });
    ///         // 带超时的完成握手（禁裸 join：裸 join 会把死锁形态变成挂死的 doctest，
    ///         // 而带超时握手让同一退化**报红**而非卡住 CI）。
    ///         rx.recv_timeout(Duration::from_secs(10))
    ///             .expect("worker 未在 10s 内完成——若 worker 改用阻塞式 Store::dispatch，即死锁形态");
    ///         worker.join().expect("worker 线程");
    ///     }
    /// }
    ///
    /// let processed = Arc::new(AtomicUsize::new(0));
    /// let admitted = Arc::new(AtomicUsize::new(0));
    /// let store = SyncStore::new(
    ///     Start { processed: Arc::clone(&processed) },
    ///     Spawner { admitted: Arc::clone(&admitted) },
    /// );
    /// store.dispatch(1); // 排空环转发效果 ⇒ 回调派生并 join worker（不死锁）
    /// assert_eq!(admitted.load(Ordering::SeqCst), 1, "worker 的动作被准入");
    /// store.dispatch(2); // 再排空一次 ⇒ worker 提交的动作被处理
    /// assert_eq!(processed.load(Ordering::SeqCst), 3, "1、7、2 三个动作各处理一次");
    /// # store.close();
    /// ```
    pub fn new(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
    ) -> Self {
        Self::restore_from(root, fx, Restore::Empty)
    }

    /// 恢复装配（接口维 R1 落点）：自 [`Restore`] 三态播种新 store——
    /// [`skeleton::JournalPort::restore`] 的返回值直传本构造，`FromSnapshot`
    /// 由此获得第一类落点（采用方不再自造协议级合成 action 污染词表）。
    ///
    /// 播种语义（契约②「恢复完备」的兑现）：
    ///
    /// - [`Restore::Empty`]：取 [`Reducer::initial`]，零 apply（[`Self::new`] 即本态）；
    /// - [`Restore::FromInitial`]：自 initial 起步，动作全量经 [`Store::dispatch`] 重放；
    /// - [`Restore::FromSnapshot`]：**快照态直入**（不经 `initial`——快照即真相），
    ///   增量动作经 [`Store::dispatch`] 重放。
    ///
    /// 重放走 dispatch 而非裸 reduce 是契约而非实现细节：效果环照常运转，
    /// `Effect::Emit` 回灌等派生动作由排空环再生，终态与 live 收敛（裸 reduce
    /// 丢派生链）；收敛回归锚见 `tests/journal_port.rs`。
    ///
    /// # Examples
    ///
    /// 快照态 100 + 增量 [10, 5]：快照直入、增量重放，重建后照常 live：
    ///
    /// ```
    /// use skeleton::{EffectBuf, NoFx, Reducer, Restore, Store as _};
    /// use skeleton_infra::runtime::SyncStore;
    /// use std::convert::Infallible;
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u64;
    ///     type Action = u64;
    ///     type Op = Infallible;
    ///     fn initial(&self) -> u64 { 0 }
    ///     fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 { s + a }
    /// }
    ///
    /// let store = SyncStore::restore_from(
    ///     Counter,
    ///     NoFx,
    ///     Restore::FromSnapshot(100u64, vec![10u64, 5]),
    /// );
    /// assert_eq!(*store.snapshot(), 115);
    /// store.dispatch(1);
    /// assert_eq!(*store.snapshot(), 116);
    /// # store.close();
    /// ```
    // `#[track_caller]`：播种期 `initial()`（`Restore::Empty`/`FromInitial` 两态）的
    // panic 归采用方装配行——分片误用作根已在类型层拒绝（E0277），此处兜的是
    // 用户 `initial` 自身的 panic。
    #[track_caller]
    pub fn restore_from(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
        restore: Restore<A, S>,
    ) -> Self {
        let (state, actions) = match restore {
            Restore::Empty => (root.initial(), Vec::new()),
            Restore::FromInitial(actions) => (root.initial(), actions),
            // 快照态直入：不经 initial（快照即真相），增量留给下方 dispatch 重放
            Restore::FromSnapshot(state, actions) => (state, actions),
            // Restore 词表 non_exhaustive（跨 crate match 须通配）：未知恢复
            // 形态 fail-closed 崩溃带因——恢复语义核心不懂的态不能静默当
            // Empty/FromInitial 处理（吞掉快照或重放即状态损坏）
            _ => panic!(
                "skeleton::Restore 新增变体未适配：SyncStore::restore_from 无法 \
                 解释该恢复形态（fail-closed），须在实现侧补臂定义其播种语义"
            ),
        };
        Self::assemble(root, fx, state, actions, ExclusiveCells::unsynchronized())
    }

    /// 装配尾段（[`Self::restore_from`] 与证据专用构造共用）：`StoreInner` 的
    /// 唯一字面量——构造入口只差排空权独占载体的实现（生产 vs `Mutex` 对照），
    /// 其余字段不可能漂移。
    fn assemble(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
        state: S,
        actions: Vec<A>,
        cells: ExclusiveCells<S, A, Op>,
    ) -> Self {
        // new_cyclic：back 缓存句柄持 Weak 指回 inner，环构造一步完成
        let inner = Arc::new_cyclic(|weak| StoreInner {
            reducer: Arc::new(root),
            state: Mutex::new(Arc::new(state)),
            runtime: Mutex::new(Box::new(fx)),
            queue: Mutex::new(VecDeque::new()),
            next_seq: AtomicU64::new(0),
            capacity: AtomicUsize::new(0),
            #[cfg(test)]
            ingress_gate: CapacitySettle::new(),
            #[cfg(test)]
            deferred_gate: CapacitySettle::new(),
            admission_full: AtomicU64::new(0),
            ledger_pool: Mutex::new(Vec::new()),
            deferred_queue: Mutex::new(VecDeque::new()),
            deferred_capacity: AtomicUsize::new(DEFAULT_DEFERRED_CAPACITY),
            deferred_capacity_rejected: AtomicU64::new(0),
            subs: Arc::new(Mutex::new(Vec::new())),
            sub_gate: Mutex::new(()),
            back: back_sender(weak),
            effect_buf: cells.effect_buf,
            notify_cache: cells.notify_cache,
            subs_version: Arc::new(AtomicUsize::new(0)),
            local_subs: Arc::new(AtomicUsize::new(0)),
            local_missed: AtomicU64::new(0),
            notify_sinks: Arc::new(Mutex::new(Vec::new())),
            notify_subs: Arc::new(AtomicUsize::new(0)),
            state_version: AtomicU64::new(0),
            notify_published: AtomicU64::new(0),
            notify_sink_buf: cells.notify_sink_buf,
            store_id: NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed),
            draining: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            rejected_after_close: AtomicU64::new(0),
            subscriber_panics: AtomicU64::new(0),
            subscriber_panic_cause: Mutex::new(None),
            subscriptions_after_close: AtomicU64::new(0),
            runtime_panics: AtomicU64::new(0),
            reducer_panics: AtomicU64::new(0),
            feedback_exceeded: AtomicU64::new(0),
            feedback_emitted: AtomicU64::new(0),
            run_id_diag: RunIdDiag::new(),
            audit_gate: Arc::new(AtomicBool::new(false)),
            audit: Arc::new(Mutex::new(None)),
            audit_seq: AtomicU64::new(0),
            audit_gen: AtomicU64::new(0),
            audit_preempted: AtomicU64::new(0),
            audit_delivery_drops: AtomicU64::new(0),
            audit_dispatch: Mutex::new(AuditDispatch::new()),
            interrupted_effect: Mutex::new(None),
            stop_wait: Condvar::new(),
            #[cfg(test)]
            in_flight: AtomicUsize::new(0),
        });
        let store = SyncStore { inner };
        // 重放协议（契约②）：动作流经同一装配的 store 逐条 dispatch——效果环
        // 照常运转，Emit 回灌等派生动作由排空环再生；裸 reduce 会丢派生链。
        for action in actions {
            store.dispatch(action);
        }
        store
    }

    /// 证据专用装配（`unsafe-evidence` 构建，蓝图 §9）：与 [`Self::new`] 逐字
    /// 同装（同一个私有 `assemble`），唯一差异是排空权独占载体换成 `Mutex<T>`
    /// 对照实现——差分测试因此能在同一进程内对同一 dispatch 序列对照两实现的
    /// 可观察面（状态终值/订阅触达/效果缓冲清空/panic 计数），而不必重写第二套
    /// `SyncStore`。默认构建不存在本入口（对照变体不进默认公共面）。
    #[cfg(feature = "unsafe-evidence")]
    #[track_caller]
    pub fn new_mutex_reference(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
    ) -> Self {
        let state = root.initial();
        Self::assemble(root, fx, state, Vec::new(), ExclusiveCells::locked())
    }

    /// 订阅登记（issue#63 私有 helper）：**假定调用方已持订阅闸** `sub_gate`
    /// ——闸把「闭门观测 + 登记」与 close 的闭门翻转变为同一线性化点：登记要么
    /// 整体在闭门前，要么整体在闭门后（不会出现「已登记但 store 已关」）。
    ///
    /// 闸内纪律（issue#63 锁序）：不跑用户回调、不丢用户闭包、不排空队列——
    /// 本 helper 只做「建槽 → 入表 → 版本自增 → 造票据」。
    fn insert_subscription(&self, f: impl FnMut(&S) + Send + 'static) -> Subscription<S> {
        let slot = Arc::new(SubscriptionSlot {
            notify: Mutex::new(Box::new(f)),
            alive: AtomicBool::new(true),
        });
        lock(&self.inner.subs).push(Arc::downgrade(&slot));
        self.inner.subs_version.fetch_add(1, Ordering::Release);
        let dead = Arc::downgrade(&slot);
        let subs = Arc::clone(&self.inner.subs);
        let version = Arc::clone(&self.inner.subs_version);
        // 存活令牌随闭包持有：表项存 Weak，强引用在票据 Drop（闭包释放）前
        // 保 slot 存活——旧形态 `_slot: Arc<SubscriptionSlot>` 字段的等价物。
        let keep_alive = Arc::clone(&slot);
        Subscription::new(move || {
            // 失活优先（issue#74）：alive=false 先于摘表 + 递增版本——缓存
            // 快照回调前复查 alive，失活槽不再被调用（幂等：panic 摘除与
            // 票据 Drop 竞争时只有一方做实）。
            keep_alive.alive.store(false, Ordering::Release);
            lock(&subs).retain(|w| !Weak::ptr_eq(w, &dead));
            version.fetch_add(1, Ordering::Release);
            drop(keep_alive);
        })
    }

    /// 登记跨线程订阅，闭门显式拒绝（推荐停机序主路径）：登记路径与
    /// [`Store::subscribe`] 完全一致，唯 `close()` 之后返回 `Err(StoreClosed)`
    /// 而非登记一张永不触发的死票据——停机序写反（先 close 后 subscribe）
    /// 在调用点即显形。与 `ActionSender::dispatch` 的显式 `false`、
    /// `crate::fx::FxScope::spawn_tracked` 的 `Err(ScopeClosed)` 同族：其余
    /// 停机入口全都显式失败，本方法补齐 subscribe 面的缺口。
    ///
    /// 旧 [`Store::subscribe`] 保持签名不变（迁移期兼容面）：照常登记 +
    /// [`Self::subscriptions_after_close`] 计数，不静默。Err 路径登记未发生，
    /// 不计入任何计数（返回值即信号，`subscriptions_after_close` 保持
    /// 「已登记的死订阅笔数」口径不掺水）。
    ///
    /// # Examples
    ///
    /// ```
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::{StoreClosed, SyncStore};
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// let store = SyncStore::new(Counter, NoFx);
    /// let ticket = store.try_subscribe(|_| {}).expect("未闭门登记必成功");
    /// store.close();
    /// assert!(matches!(store.try_subscribe(|_| {}), Err(StoreClosed)));
    /// drop(ticket);
    /// ```
    pub fn try_subscribe(
        &self,
        f: impl FnMut(&S) + Send + 'static,
    ) -> Result<Subscription<S>, StoreClosed> {
        // 闸内「检查 closed + 登记」原子化（issue#63）：close 的
        // closed=false→true 翻转取同一把闸 ⇒ 本调用与 close 互斥——结果
        // 只能是「Ok 且登记线性化在 close 之前」或「Err(StoreClosed) 且
        // close 已线性化」；绝不会「已登记但 store 已关」。
        let _gate = lock(&self.inner.sub_gate);
        if self.is_closed() {
            // 闸在抛错前释放：用户闭包随参数析构，不在闸内（禁持闸丢闭包）。
            drop(_gate);
            return Err(StoreClosed);
        }
        Ok(self.insert_subscription(f))
    }

    /// 单线程订阅：回调免 `Send` 约束——闭包体内可用 `Rc`/`RefCell` 等线程
    /// 本地设施（跨线程档逼着采用方 `Arc<AtomicU64>`/`Mutex` 的痛点在此收口）。
    /// 与 [`Self::subscribe`] 的四点差异：
    ///
    /// 1. **只在注册线程触发**：本地回调登记在注册线程的 thread-local 表，
    ///    store 侧从不持有闭包。他线程 dispatch 不触达本地订阅（其线程表中
    ///    无此 store）；注册线程的 dispatch / `sender().dispatch` 正常触发；
    /// 2. **通知路径免锁免原子量**：回调经 thread-local 直达；store 侧只存一个
    ///    计数闸，零本地订阅时一次 relaxed 读即短路；
    /// 3. **不计入 [`Self::subscriber_count`]**（该计数只看跨线程订阅）；回调
    ///    panic 与跨线程订阅同一纪律：fail-closed 摘除 + [`Self::subscriber_panics`]
    ///    计数。
    /// 4. **跨线程 miss 显形**：他线程 dispatch 不触达本档——每次「闸>0 而
    ///    dispatch 线程表查无本 store 登记」计入 [`Self::local_missed_notifications`]
    ///    （「挂错档」检测；触发语义不变，只留痕）。
    ///
    /// 两 API 的关系：`subscribe` 是跨线程档（回调需 `Send`，槽位 `Mutex`
    /// 串行化）；本 API 是单线程档（回调免 `Send`，线程亲和即安全依据）。
    /// 闭门（[`Self::close`]）后登记照常、永不触发——与 `subscribe` 一致，
    /// 对照见 [`SyncStore`] 类型文档「闭门后的入口对照」。同一 dispatch 内通知序：
    /// 本地订阅先行，跨线程订阅随后（确定性；本地闸先判读，零共享订阅不短路
    /// 本地通知）。
    ///
    /// 票据 [`LocalSubscription`]：Drop = 注销（从注册线程表摘除 + 计数回落）；
    /// 票据本身 `!Send + !Sync`（线程亲和的类型化表达），跨线程移动即编译错。
    /// 丢弃票据 = 立即注销（死订阅），类型层拦截（与 [`Self::subscribe`] 同款）：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct Counter;
    /// # impl Reducer for Counter {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(Counter, NoFx);
    /// store.subscribe_local(|_| {}); // ERROR: unused `LocalSubscription` that must be used——丢弃即死订阅
    /// ```
    pub fn subscribe_local(&self, f: impl FnMut(&S) + 'static) -> LocalSubscription {
        // 闭门登记显形（与 subscribe 同一纪律）
        if self.is_closed() {
            self.inner
                .subscriptions_after_close
                .fetch_add(1, Ordering::SeqCst);
        }
        let sub_id = next_local_sub_id();
        let store_id = self.inner.store_id;
        local_subs::register(store_id, sub_id, Box::new(f));
        self.inner.local_subs.fetch_add(1, Ordering::Release);
        LocalSubscription::new(store_id, sub_id, Arc::clone(&self.inner.local_subs))
    }

    /// 活跃订阅者数（跨线程订阅；本地订阅不入此表，见 [`Self::subscribe_local`]）。
    /// 只读：不触发回收；订阅回调 panic 被摘除后计数可观测下降（成因另见
    /// [`Self::subscriber_panics`] 与 [`Self::subscriber_panic_last_cause`]）——
    /// 订阅者丢失不再纯静默。
    ///
    /// # 时序（如实）
    ///
    /// 读数即时精确：票据 drop 即时摘表（[`Subscription`] Drop 经共享订阅表
    /// 句柄 retain 摘除本表项），本读侧直查订阅表 Weak 判活——无
    /// 「下一次 dispatch 的一拍滞后」（旧文档曾声称滞后：旧实现依赖排空冷重建
    /// 代笔回收，通知快照缓存持强引用为死槽「续命」，实测证伪后修复为即时摘表；
    /// 「先 dispatch 后 drop 再读」序下滞后实测可复现，见
    /// tests/store_behavior_gaps.rs 的即时性回归）。
    ///
    /// 口径：**仅计 `subscribe` 订阅**——`subscribe_local`（TLS 亲和档）不在
    /// 本计数内（两档订阅面不对称，设计见 subscribe_local 文档）。
    pub fn subscriber_count(&self) -> usize {
        lock(&self.inner.subs)
            .iter()
            .filter(|w| w.upgrade().is_some())
            .count()
    }

    /// 闭门后拒绝计数（fail-visible）：dispatch 拒收与 sender dispatch false 均计入。
    /// store 已丢弃后的 sender dispatch false 不可计（计数器随实例消亡），停机窗语义不变。
    pub fn rejected_after_close(&self) -> u64 {
        self.inner.rejected_after_close.load(Ordering::SeqCst)
    }

    /// 未处理 action 的队列深（状态可查读侧）。
    ///
    /// 口径（issue#40）：`dispatch` 的同步面是**本 action 的完成边界**，不是
    /// 全局队列空——返回时本 action 已被 reduce（且其引发的因果后代已终局），
    /// 但其后被其他生产者准入的 action 可能仍在队（并发生产者的队列深非 0
    /// 是正常态，不是「未排空」的证据）。采样即时值 = 主队列 + 停放队列
    /// （回灌预算止步的残留，零丢失留痕）。
    pub fn pending_actions(&self) -> usize {
        // 主队列（等待被 reduce）+ 停放队列（预算止步的残留，零丢失留痕）
        lock(&self.inner.queue).len() + lock(&self.inner.deferred_queue).len()
    }

    /// 结果面（issue#57）：准入当前 action 并交还**根因果回执**——与 legacy
    /// [`Store::dispatch`] 走同一条准入/排空/完成路径，唯一差别是终局以值返回
    /// （「未完成」与「失败」都不再被 void 返回/panic 掩盖）。
    ///
    /// 回执的 [`DispatchReceipt::wait`] 返回 [`DispatchOutcome`]：
    ///
    /// - `Completed`：根及其全部因果后代终局成功；
    /// - `Deferred { retained }`：至少一个因果后代被回灌深度预算停放——**不是**
    ///   完整因果完成，恢复走 [`Self::resume_deferred`]；
    /// - `CapacityRejected { retained, rejected }`：停放队列满，`rejected` 条续接
    ///   无法留存（专用计数 [`Self::deferred_capacity_rejected`]）；
    /// - `Failed(..)`：根或某个因果后代失败（legacy 路径同处 panic）。
    ///
    /// `Err(..)` = **未准入**（[`AdmissionOutcome::Closed`] / `Full`）：action 未
    /// 入队、无回执（`Accepted` 不会出现在 `Err` 位）。与
    /// [`Self::try_dispatch`] 的差别：本方法返回的是**回执**（可等终局），而
    /// 不是准入判定。
    ///
    /// # 禁止在排空回调内调用
    ///
    /// 排空回调（reducer / 解释器 / 订阅回调 / 审计回调）内调用本方法
    /// **panic**（fail-visible）：回执身份只属于外部根，且在该处等待自己的回执
    /// 必然死锁（排空环被本次调用卡住）。回调内派生工作请用 [`ActionSender`]
    /// （纯准入，无需回执）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// use skeleton_infra::runtime::{DispatchOutcome, SyncStore};
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = Infallible;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32>) -> u32 { s + a }
    /// }
    ///
    /// let store = SyncStore::new(Counter, NoFx);
    /// let receipt = store.dispatch_with_receipt(2).expect("静默态必被准入");
    /// assert_eq!(receipt.wait(), DispatchOutcome::Completed);
    /// assert_eq!(*store.snapshot(), 2);
    /// ```
    pub fn dispatch_with_receipt(
        &self,
        action: A,
    ) -> Result<DispatchReceipt<S, A, Op>, AdmissionOutcome> {
        assert!(
            reentrant_context(self.inner.store_id).is_none(),
            "udf-frame: 禁止在排空回调内取根回执（回执身份只属于外部根；在此等待即自死锁）"
        );
        match self.admit(action)? {
            (seq, true, ledger) => Ok(DispatchReceipt {
                seq,
                ledger,
                inner: Arc::clone(&self.inner),
            }),
            // 排空回调内已被上面的断言排除 ⇒ 同栈回灌路径不可达。
            (_, false, _) => unreachable!("排空回调内已由断言排除：回调内入队只会是因果后代"),
        }
    }

    /// [`Store::dispatch`] / [`Self::dispatch_with_receipt`] 共用准入段：闸判 →
    /// 入队（必要时夺排空权并同步排空首段）→ 交还 `(入队序号, 是否外部根, 账本)`。
    ///
    /// `is_root == false` = 同栈回灌（共享父账本）：调用方**不得**等待该账本。
    fn admit(&self, action: A) -> Result<(u64, bool, Arc<Ledger>), AdmissionOutcome> {
        // 闸序（锁消融批论证）：closed 单调 false→true，load Acquire——见
        // [`SyncStore::close`] 的 Release 配对与锁内复检论证。
        if self.is_closed() {
            self.inner
                .rejected_after_close
                .fetch_add(1, Ordering::SeqCst);
            // 取证面：早闸拒绝与 enqueue 复检拒绝同一事件节点（两条 push
            // 路径 + 两个闸位的拒绝在时间线上同形）。
            audit_emit(&self.inner, AuditEventKind::Rejected);
            return Err(AdmissionOutcome::Closed);
        }
        #[cfg(test)]
        self.inner.in_flight.fetch_add(1, Ordering::SeqCst);
        // 准入上下文：同栈回灌（共享父账本、不等待）vs 外部根（新账本、等待）。
        let admission = admission_context(self.inner.store_id, true);
        let is_root = admission.parent.is_none();
        let origin = admission.origin;
        let admitted = match enqueue(&self.inner, action, admission) {
            Enqueue::Rejected => Err(AdmissionOutcome::Closed),
            // 容量满（issue#38）：typed reject 不阻塞（阻塞会与回调重入死锁）。
            Enqueue::Full => Err(AdmissionOutcome::Full),
            Enqueue::Queued(seq, ledger) => {
                audit_enqueued(&self.inner, origin); // 在任排空者覆盖（交接论证见 enqueue）
                Ok((seq, is_root, ledger))
            }
            Enqueue::Draining(seq, ledger, first) => {
                audit_enqueued_as_lease_winner(&self.inner, origin, first.as_ref());
                drain_owned(&self.inner, first);
                Ok((seq, is_root, ledger))
            }
        };
        if admitted.is_err() {
            audit_emit(&self.inner, AuditEventKind::Rejected);
        }
        #[cfg(test)]
        self.inner.in_flight.fetch_sub(1, Ordering::SeqCst);
        admitted
    }

    /// 有界准入（issue#38）：typed 准入判定面。
    ///
    /// 返回 [`AdmissionOutcome::Accepted`] = 已入队（可能由在任排空者消费，
    /// 也可能稍后由后续排空消费）；[`AdmissionOutcome::Full`] = 队列容量已满，
    /// action **未入队**（不部分插入）；[`AdmissionOutcome::Closed`] = 已闭门。
    ///
    /// 本方法**不阻塞等待**完成边界——但**不等待**≠**未达成**：语义**按调用瞬间的
    /// 排空状态分两条路径**——**竞争态**（调用瞬间已有在任排空者）= **纯准入**——
    /// 只保证该 action 已入队、**不等**它的完成边界；**返回瞬间该 action 是否已被
    /// reduce，框架不作任何保证**（取决于在任排空者的推进速度：可能尚未被 reduce，
    /// 也可能已被 reduce 甚至已完成）⇒ 下游**不得**据「竞争态」推断「此刻读状态
    /// 仍是旧值」。**静默态**（调用瞬间无在任排空者）= 本线程夺得排空权并**同步排空
    /// 至 idle** ⇒ 返回时该 action **已被 reduce、其完成边界必然已达成**（此路径
    /// **强于** [`Store::dispatch`] 的 per-action 完成边界契约——后者**不**承诺队列
    /// 为空）。**等待面见 [`Store::dispatch`]**——它在返回前保证本 action 到达完成
    /// 边界。容量满绝不阻塞：阻塞会与回调重入（reducer / 解释器 / 订阅回调内
    /// dispatch）死锁。
    ///
    /// 静默态那句「完成边界必然已达成」**不含**被回灌预算停放的子树：某个因果
    /// 后代撞上预算时根回执以 `Deferred` 收口（issue#57），准入面本来就不承载
    /// 终局信息——要终局用 [`Self::dispatch_with_receipt`]。
    pub fn try_dispatch(&self, action: A) -> AdmissionOutcome {
        if self.is_closed() {
            self.inner
                .rejected_after_close
                .fetch_add(1, Ordering::SeqCst);
            audit_emit(&self.inner, AuditEventKind::Rejected);
            return AdmissionOutcome::Closed;
        }
        let admission = admission_context(self.inner.store_id, false);
        let origin = admission.origin;
        match enqueue(&self.inner, action, admission) {
            Enqueue::Rejected => {
                audit_emit(&self.inner, AuditEventKind::Rejected);
                AdmissionOutcome::Closed
            }
            Enqueue::Full => {
                // 过载显式化：计数 + 审计（不部分插入、不阻塞）。
                audit_emit(&self.inner, AuditEventKind::Rejected);
                AdmissionOutcome::Full
            }
            Enqueue::Queued(_, _) => {
                audit_enqueued(&self.inner, origin);
                AdmissionOutcome::Accepted
            }
            Enqueue::Draining(_, _, first) => {
                audit_enqueued_as_lease_winner(&self.inner, origin, first.as_ref());
                drain_owned(&self.inner, first);
                AdmissionOutcome::Accepted
            }
        }
    }

    /// 队列容量上限（issue#38）：`cap` = 等待被 reduce 的 action 数上限；
    /// `0` = 不限（默认）。正在执行的 action 已出队 → **不计入**。
    ///
    /// 只约束**外部**准入（dispatch / try_dispatch / sender 的外部流量）；
    /// 同栈回灌不受约束（回灌是已接受 action 的内部延续，挡住它 = 排空环
    /// 自锁）。容量满 = typed reject（[`Self::try_dispatch`] 的 `Full`），
    /// 绝不阻塞。
    ///
    /// # 与准入的线性化（issue#95）
    ///
    /// **线性化点 = `queue` 锁内的发布**：本方法先取 `queue` 锁再写新值，而每次
    /// 外部准入在**同一把锁**内读 cap 并完成 push ⇒ 二者只有两种合法先后（不存在
    /// 「准入读了旧 cap、setter 先返回、准入随后仍按旧 cap 入队」的交错）：
    ///
    /// - 准入先得锁：该条按**旧** cap 判定并入队，本方法随后返回——它线性化在
    ///   本方法之前，**不被回溯删除/驱逐**（即使此刻 `pending_actions() > cap`）。
    /// - 本方法先得锁：其后所有外部准入按**新** cap 判定。
    ///
    /// **扩容与 `0`（不限）对后续准入立即生效**；缩容只影响后续准入，已排队的
    /// 工作照常被 reduce，另有独立计数记录拒绝。
    pub fn set_capacity(&self, cap: usize) {
        let _queue = lock(&self.inner.queue);
        self.inner.capacity.store(cap, Ordering::Relaxed);
    }

    /// 容量满拒绝计数（fail-visible）：读侧。
    pub fn admission_full_count(&self) -> u64 {
        self.inner.admission_full.load(Ordering::SeqCst)
    }

    /// 停放队列容量（issue#49）：`cap` = 可留存的停放记录数上限；`0` = 不限。
    ///
    /// **与外部准入容量分立**（[`Self::set_capacity`]）：外部 ingress 与内部
    /// feedback 不共用一个闸。默认 `DEFAULT_DEFERRED_CAPACITY`——**数值已拍板
    /// keep current**（2026-11-23，见 `docs/reference.md` §资源上限与默认值的
    /// 决定记录；只保证可配 + 可观测）。满 ⇒ 新续接拿显式非成功
    /// 终局（[`DispatchOutcome::CapacityRejected`]）并计入
    /// [`Self::deferred_capacity_rejected`]，绝不静默消失、绝不标成「已停放」。
    ///
    /// # 与停放写入的线性化（issue#95）
    ///
    /// **线性化点 = `deferred_queue` 锁内的发布**：本方法先取该锁再写新值，而
    /// 停放路径在**同一把锁**内读 cap 并执行 retain / reject ⇒ 二者只有两种合法
    /// 先后：
    ///
    /// - 停放先得锁：该条按**旧** cap 留存或拒收，本方法随后返回；已留存的停放
    ///   记录**不被回溯删除**（即使此刻 `deferred_actions() > cap`）。
    /// - 本方法先得锁：其后所有停放按**新** cap 判定。
    ///
    /// **扩容与 `0`（不限）对后续停放立即生效**。锁序不变：本方法只取
    /// `deferred_queue`（与既有 `queue → deferred_queue` 单向序一致，不新增反向
    /// 边，故与 `close` / `resume_deferred` 并发不产生死锁）。
    pub fn set_deferred_capacity(&self, cap: usize) {
        let _parked = lock(&self.inner.deferred_queue);
        self.inner.deferred_capacity.store(cap, Ordering::Relaxed);
    }

    /// 停放队列容量读侧（`0` = 不限）。
    pub fn deferred_capacity(&self) -> usize {
        self.inner.deferred_capacity.load(Ordering::Relaxed)
    }

    /// 停放队列当前的记录数（独立可观测：不与主队列合并计数）。
    pub fn deferred_actions(&self) -> usize {
        lock(&self.inner.deferred_queue).len()
    }

    /// 停放容量拒收计数（fail-visible 读侧）：撞预算但续接**无法留存**的累计
    /// 次数。与 [`Self::feedback_budget_exceeded`] 分账（后者数「撞预算」，
    /// 含成功留存的停放）。
    pub fn deferred_capacity_rejected(&self) -> u64 {
        self.inner.deferred_capacity_rejected.load(Ordering::SeqCst)
    }

    /// 显式有界恢复（issue#49）：把停放队列里**最老的**至多 `limit` 条记录按
    /// FIFO 送回主队列——**追加在已准入的健康外部工作之后**（不插队、不垄断
    /// 排空），每条记录开**新回灌深度 epoch**（`ActionOrigin::Resumed`：自身
    /// 深度 0），保留原 action 序号与停放来源。
    ///
    /// # 归属规则
    ///
    /// - **原根回执保持终态不可变**（通常是 [`DispatchOutcome::Deferred`]）；
    /// - 恢复出的工作拿**自己的**（分离的）续接回执：本方法等到每条记录的终局
    ///   才返回，据此给出 [`DeferredResume`] 的计数；
    /// - **无隐藏重试**：恢复后再次撞预算就再次停放（计入 `deferred_again`），
    ///   停放队列满则拿显式非成功容量终局（计入 `capacity_rejected` + 专用计数）。
    ///
    /// # 串行化
    ///
    /// 与 close、队列准入共用 `queue` 锁：一条记录任何时刻只属于一个 population
    /// （主队列 / 停放队列 / 活动排空 / 终态拒收或丢弃）。**闭门后拒绝恢复**
    /// （返回 `resumed == 0`，记录仍归 store——不偷偷放行闭门后的工作）。
    ///
    /// # 禁止在排空回调内调用
    ///
    /// 本方法等恢复记录的终局 ⇒ 排空回调内调用必然自死锁，故 **panic**
    /// （fail-visible；同 [`Self::dispatch_with_receipt`] 的等待禁令）。
    pub fn resume_deferred(&self, limit: usize) -> DeferredResume {
        assert!(
            reentrant_context(self.inner.store_id).is_none(),
            "udf-frame: 禁止在排空回调内恢复停放记录（等待自己所在的排空环 = 自死锁）"
        );
        let mut resume = DeferredResume::default();
        // 与 close / 准入串行（同一把 queue 锁）：闭门复检与搬运同临界区。
        let mut queue = lock(&self.inner.queue);
        if limit == 0 || self.inner.closed.load(Ordering::Acquire) {
            drop(queue);
            resume.remaining = self.deferred_actions();
            return resume;
        }
        let mut ledgers: Vec<Arc<Ledger>> = Vec::new();
        {
            // 锁序 queue → deferred（停放路径只取 deferred，无反向持有 ⇒ 无环）。
            let mut parked = lock(&self.inner.deferred_queue);
            let take = limit.min(parked.len());
            ledgers.reserve(take);
            for _ in 0..take {
                let Some(mut record) = parked.pop_front() else {
                    break;
                };
                // 停放来源留痕（`Resumed` 的深度恒 0 ⇒ 新 epoch，见 record_depth）。
                record.origin = ActionOrigin::Resumed {
                    prior_depth: match record.origin {
                        ActionOrigin::Resumed { prior_depth } => prior_depth,
                        origin => record_depth(origin),
                    },
                };
                // 续接回执的等待者认领：本调用持有归还权（issue#84 的归属 token）。
                record.ledger.claim_as_waiter();
                ledgers.push(Arc::clone(&record.ledger));
                queue.push_back(record);
            }
            resume.resumed = ledgers.len();
        }
        if ledgers.is_empty() {
            // 无记录可恢复：不夺排空权（本方法不该顺带排空别人的主队列）。
            drop(queue);
            resume.remaining = self.deferred_actions();
            return resume;
        }
        // 夺排空权（与 enqueue 同协议：swap 在 queue 锁内，出口持锁放旗 ⇒ 无孤儿）。
        let drained = !self.inner.draining.swap(true, Ordering::AcqRel);
        drop(queue);
        if drained {
            drain_owned(&self.inner, None);
        }
        // 每条恢复记录等自己的续接回执终局（Completed / 再次停放 / 容量拒收）。
        // 不等待 ⇒ 三项计数只能是猜测；等待边界 = 这些记录各自的因果子树终局。
        for ledger in ledgers {
            match ledger.wait() {
                Outcome::Completed => {}
                Outcome::Deferred { retained, rejected } => {
                    resume.capacity_rejected += rejected as usize;
                    if retained > 0 {
                        resume.deferred_again += 1;
                    }
                }
                // 与 legacy dispatch 同一 fail-visible 口径（已接受但失败禁伪装成
                // 成功）。同线程排空时 panic 已经过排空环上浮到这里，此臂只见
                // 「他线程排空」的路径。
                Outcome::Failed(failure) => panic!(
                    "udf-frame: resume_deferred 恢复的记录失败——seq={} phase={:?} state_committed={} cause={}",
                    failure.seq, failure.phase, failure.state_committed, failure.cause
                ),
            }
            release_ledger_by_waiter(&self.inner, &ledger);
        }
        resume.remaining = self.deferred_actions();
        resume
    }

    /// 订阅回调 panic 摘除的累计次数（成因留痕读侧：`subscriber_count` 下跌
    /// 只表明订阅者消失，本计数回答「因 panic 被摘」）。摘除留痕
    /// +显式计数读侧可见的成因面；只读，不触发回收。
    pub fn subscriber_panics(&self) -> u64 {
        self.inner.subscriber_panics.load(Ordering::Acquire)
    }

    /// 最近一次订阅回调 panic 的成因摘要（成因面读侧；摘除计数见
    /// [`Self::subscriber_panics`]——计数回答「谁死了」，本读侧回答「为什么」）。
    /// `None` = 从无 panic 摘除；`Some` = payload 为 `&str`/`String` 的原文
    /// （200 字符截断）或非字符串载荷的占位说明。只保留最近一次，多次摘除以
    /// 最后为准；跨线程与本地订阅两条摘除路径同一纪律。
    pub fn subscriber_panic_last_cause(&self) -> Option<String> {
        lock(&self.inner.subscriber_panic_cause).clone()
    }

    /// 闭门后登记的订阅笔数：登记照常生效（票据可注销、
    /// [`Self::subscriber_count`] 可查）但闭门后永无 dispatch 触发——本计数让
    /// 该状态从观测死角显形。与 [`Self::rejected_after_close`] 分立：登记不是
    /// 拒绝，并数会污染拒绝语义（闭门对照见类型文档）。
    pub fn subscriptions_after_close(&self) -> u64 {
        self.inner.subscriptions_after_close.load(Ordering::SeqCst)
    }

    /// 本地订阅跨线程 miss 计数（fail-visible 读侧）：dispatch 通知段「本地
    /// 订阅闸>0 但 dispatch 线程的 TLS 表查无本 store 登记」即 +1——本地订阅
    /// 挂在 A 线程、dispatch 来自 B 线程时，A 的订阅者收不到 B 的任何 dispatch
    /// 通知（触发语义不变，只从「零触发零信号」变「miss 几次看得见」）。
    /// 非零读数 = 本地档挂在非 dispatch 线程（「挂错档」）：改用
    /// [`Self::subscribe`] 跨线程档，或把 dispatch 收敛到注册线程。口径按
    /// dispatch 时点的闸快照：票据注销与闸回落之间的瞬态窗可能多计一次
    /// （无害偏粗）。
    pub fn local_missed_notifications(&self) -> u64 {
        self.inner.local_missed.load(Ordering::Relaxed)
    }

    /// 审计事件流前订被覆盖计数（fail-visible 读侧）：[`Self::subscribe_events`]
    /// 落槽时槽内已有订阅即 +1——单槽二次订阅会终止前订的事件流（典型：
    /// 框架自带 tracing 桥被自定义消费者顶掉且无任何信号），本计数让覆盖
    /// 显形。旧票据 Drop 照常安全（代次对账不误杀后继流），但其事件流已被
    /// 覆盖终止。
    pub fn audit_preempted(&self) -> u64 {
        self.inner.audit_preempted.load(Ordering::Relaxed)
    }

    /// 审计实时投递丢弃计数（fail-visible 读侧，issue#65 主路线①）：实时回调
    /// **未发生投递**的次数——待投递 FIFO 溢出（丢最旧）/ 回调 panic 后该代次
    /// 队列作废 / 订阅替换时旧代次队列作废。
    ///
    /// **与环溢出分账**：环溢出丢历史侧（容量因，`seq` 空洞即显式证据），
    /// 本计数丢实时流（背压因）；两套证据互不替代（词表见
    /// `docs/reference.md`「投递语义词汇（订阅面通用）」）。
    pub fn audit_delivery_drops(&self) -> u64 {
        self.inner.audit_delivery_drops.load(Ordering::Relaxed)
    }

    /// 异步投递累计次数（ADR 0008 唯一新增读侧，成本可见）：投递口成功收到
    /// 信号的累计次数——逐口逐拍累计（panic 被摘除的投递不计；闭门/票 Drop 后
    /// 不再产生）。乘以单次投递成本即异步档对 dispatch 的总税额，是投递口
    /// 「O(1)」契约义务的核对依据。只读。
    pub fn notify_published(&self) -> u64 {
        self.inner.notify_published.load(Ordering::Acquire)
    }

    /// 解释器 panic 毒锁恢复计数（fail-visible 上浮）。每次 into_inner 恢复自增 = 此前恰
    /// 发生一次解释器 panic（毒位随即 clear_poison 复位，实例不报废）；读数不含
    /// 尚未恢复的最近一次 panic——它在下一次效果转发恢复时才计入。持续上升的
    /// 读数 = 解释器 `run` 存在 panic 路径，需在宿主侧修复。
    pub fn runtime_panic_count(&self) -> u64 {
        self.inner.runtime_panics.load(Ordering::SeqCst)
    }

    /// reducer panic 计数（fail-visible 留痕读侧）：reduce 的 panic 在排空环
    /// catch_unwind 观察点计数后 `resume_unwind` 原样重抛——传播语义不变，
    /// panic 照常在 dispatch 调用点上浮。动作本体已出队、以值传入 reduce，
    /// panic 即丢失（状态不前进、实例可复用；**本次调用已产的暂存效果随未提交
    /// 迁移一并丢弃，不计数**——未提交的效果从未成为已提交效果，另立计数会把
    /// 「失败迁移的副作用」混进已提交效果的账；更早已提交、只等续转的效果原样
    /// 留存，不受牵连）；
    /// 审计时间线现场 = [`AuditEventKind::ReducerInterrupted`]。至此四条
    /// panic/异常路径读侧齐全：解释器 [`Self::runtime_panic_count`]、订阅回调
    /// [`Self::subscriber_panics`]、回灌超限 [`Self::feedback_budget_exceeded`]、
    /// reducer 本计数。持续上升的读数 = 用户 reducer 存在 panic 路径。
    pub fn reducer_panic_count(&self) -> u64 {
        self.inner.reducer_panics.load(Ordering::SeqCst)
    }

    /// 回灌深度预算触发计数（fail-visible 上浮）：单任排空内连续处理的
    /// 回灌链深度超过 `FEEDBACK_DEPTH_BUDGET`（64）即停排并自增。触发的记录
    /// 脱钩后**停放到 deferred 队列**（保留序号与载荷，零丢失；留在主队列会
    /// 把外部流量反复顶住 ⇒ 饿死健康流量），本任排空继续处理主队列——非 panic
    /// 非静默 no-op。持续上升的读数 = 用户程序存在自指回灌环（reducer 恒 Emit
    /// 自身、解释器经 `back.dispatch` 同栈回灌），需在宿主侧修复；读数不升的
    /// 正常流量不受预算影响（外部 action 深度恒 0）。停放记录等**显式**恢复
    /// （[`Self::resume_deferred`]，无自动重试）；续接无法留存（停放队列满）
    /// 同样计入本计数，并另计入 [`Self::deferred_capacity_rejected`]。store
    /// drop 时队列仍滞留的弃队数一并计入本计数留痕（禁静默弃队）——**但两个
    /// population 混在同一读口**：弃队快照可能含普通未处理 action（句柄可在排空
    /// 进行中被丢弃），且最后一次留痕在唯一句柄析构时写入、此后不可读 ⇒ 读到
    /// 「预算触发」与「停机弃队」不可区分（bd udf-frame-8ad 显式承认）。时间线现场
    /// = [`AuditEventKind::FeedbackBudgetExceeded`]（`subscribe_events` 订阅窗内可查——环结构重建走事件流，快速巡检走本
    /// 计数）。
    pub fn feedback_budget_exceeded(&self) -> u64 {
        self.inner.feedback_exceeded.load(Ordering::SeqCst)
    }

    /// Cancel 寻址校验读数：**精确**的「Cancel 引用了保留视界内从未发出的
    /// `Run` id」次数（只计数不拦截——迟到 Cancel 是合法时序，见
    /// [`Self::classify_run_id`]）。
    ///
    /// **口径（issue#62，显式化，不静默改义）**：
    /// - 只数**诊断启用**（[`Self::enable_run_id_diagnostics`]）且**视界完整、
    ///   从未发生饱和驱逐**时的 miss。默认关闭 ⇒ 本读数恒 0；
    /// - 一旦发生驱逐（[`Self::run_id_evicted_count`] > 0），之后的 miss 可能是
    ///   合法迟到的 Cancel ⇒ 本计数**停止增长**（不再声称精确），改由三态分类的
    ///   [`RunIdClass::NotTracked`] 承载不可判定的那部分；
    /// - 视界残缺（诊断启用晚于首个准入）时同样不计数。
    ///
    /// 非零读数 = id 纪律漂移的直接证据：Cancel 打错数字、多效果域撞 id 后
    /// 引用了他域 id、或 Cancel 早于 Run 的时序 bug。持续上升的读数需在
    /// reducer 侧修 id 指派（唯一性纪律见 [`EffectId`] 文档）。
    pub fn cancel_unknown_id(&self) -> u64 {
        self.inner.run_id_diag.cancel_unknown_count()
    }

    /// 启用历史 Run-ID 诊断（issue#62）：**默认关闭**，本方法是唯一开关。
    ///
    /// - 窄口径 runtime 诊断设置——不是通用策略/配置框架；`Run`/`Cancel` 的
    ///   转发语义与是否启用无关（诊断只观测）。
    /// - `capacity`：有界最近 ID 集容量（`0` 视作 1）。**默认值已拍板
    ///   keep current**（2026-11-23，见 `docs/reference.md` §资源上限与默认值
    ///   的决定记录）：拿不准时用 [`DEFAULT_RUN_ID_TRACKING_CAPACITY`]。
    /// - **必须在首个准入之前调用**：晚于任何已准入 action 启用 ⇒ 早先的 `Run`
    ///   未被登记，视界残缺 ⇒ miss 只能判 [`RunIdClass::NotTracked`]（保守方向：
    ///   不把可能存在的 id 判成确定未知）。
    /// - 幂等：已启用时再调用不改动（容量与视界在首次启用时固定）。
    /// - 禁用路径零锁零分配（`Run` 效果不登记、不分配；见
    ///   `tests/run_id_diagnostics.rs` 的零分配锚）。
    pub fn enable_run_id_diagnostics(&mut self, capacity: usize) {
        let first_admission_seen = self.inner.next_seq.load(Ordering::SeqCst) > 0;
        self.inner
            .run_id_diag
            .enable(capacity, !first_admission_seen);
    }

    /// Run-ID 三态分类读数（issue#62）：`known` / `definitely-unknown`（保留
    /// 视界内确定未见过）/ `not-tracked`（未启用、视界残缺或已发生驱逐——
    /// **被驱逐的 id 绝不判成确定未知**）。诊断默认关闭 ⇒ 恒
    /// [`RunIdClass::NotTracked`]。
    pub fn classify_run_id(&self, id: u64) -> RunIdClass {
        self.inner.run_id_diag.classify(id)
    }

    /// 已跟踪的最近 Run-ID 数（诊断未启用 = 0）。
    pub fn run_id_tracked_count(&self) -> usize {
        self.inner.run_id_diag.tracked_count()
    }

    /// 配置的跟踪容量（诊断未启用 = 0；默认值已拍板 keep current，2026-11-23，
    /// 见 [`DEFAULT_RUN_ID_TRACKING_CAPACITY`]）。
    pub fn run_id_tracking_capacity(&self) -> usize {
        self.inner.run_id_diag.capacity()
    }

    /// 饱和驱逐计数：每次容量满驱逐最旧 +1。> 0 ⇒ 保留视界已不全，miss 只能
    /// 判 [`RunIdClass::NotTracked`]（旧 [`Self::cancel_unknown_id`] 同时停止增长）。
    pub fn run_id_evicted_count(&self) -> u64 {
        self.inner.run_id_diag.evicted_count()
    }

    /// Emit 回灌累计重入计数（crate 内读侧：testing 域 TestStore 归因用，
    /// 零公共面增量）。
    #[cfg(feature = "testing")]
    pub(crate) fn feedback_emitted_total(&self) -> u64 {
        self.inner.feedback_emitted.load(Ordering::Relaxed)
    }

    /// 唯一订阅点：开启审计事件流。默认关闭零开销；
    /// 本调用是事件流的唯一开关（再次订阅覆盖前订，单槽；覆盖留痕
    /// [`Self::audit_preempted`]——前订被终止从此可查）。
    ///
    /// - `capacity`：有界环容量（0 视作 1）。溢出丢最旧——最新生命周期优先；
    ///   丢失无新计数，`seq` 空洞即显式证据（见 [`AuditEvent`]）。
    /// - `on_event`：实时回调（**串行派发**：任一时刻至多一个派发者在调用回
    ///   调）。回调在 emit 调用方线程内同步调用（慢回调拖慢 emit 调用方的时序
    ///   ——已知观测面张力）。**回调在途期间**并发发射的事件**入队待补投**（有
    ///   界 FIFO，按 `seq` 序，由在途者在所有内部锁之外补投）——并发发射**不
    ///   丢事件**；队列有界 ⇒ 溢出丢最旧并计入 [`Self::audit_delivery_drops`]
    ///   （实时流背压因，与环溢出分账）。**同 store 重入**（回调内 dispatch 本
    ///   store）的事件只录环、不再回调（TLS 在途标记封自指递归）——该抑制与
    ///   「并发投递入队」是两种不同意图。权威历史仍是环 + `seq`（取证走
    ///   [`Self::take_events`]；订阅面通用词汇见 `docs/reference.md`「投递语义词
    ///   汇」）。纯环录模式传 `|_| {}`。回调 panic = fail-closed 摘除（环照录、
    ///   dispatch 行为不变——观测面禁改变被观测者），该代次排队事件作废（不投
    ///   递给替换订阅）并计数，成因计入 [`Self::subscriber_panics`] /
    ///   [`Self::subscriber_panic_last_cause`]。
    /// - **回调内 dispatch 的因果归属**（issue#59）：**夺得排空租约**的那次
    ///   调用在投递首条 pop 记录的 `Enqueued` 回调**之前**即建立临时同栈帧 ⇒
    ///   回调内的同 store dispatch 归为**因果后代**（共享该首条记录的账本、
    ///   计深 +1、只入队即返，不同步等外层排空）。因果父 = **正在投递其
    ///   `Enqueued` 回调的首条记录**，不必然是夺得排空权的外部调用点（回调先于
    ///   外层 action 的 reduce 投递，首条记录甚至可能不是调用者的 action）。
    /// - 票据 Drop = 关闭；被覆盖的旧票据 Drop 凭代次对账，禁误杀后继流。
    ///
    /// # Examples
    ///
    /// 一笔 dispatch 的生命周期从事件流重建（取证锚）：
    ///
    /// ```
    /// use skeleton::{Effect, EffectBuf, NoFx, Reducer, Store as _};
    /// use skeleton_infra::audit::AuditEventKind;
    /// use skeleton_infra::runtime::SyncStore;
    ///
    /// struct PingOnce;
    /// impl Reducer for PingOnce {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = skeleton::Infallible;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, a: u32, out: &mut EffectBuf<u32>) -> u32 {
    ///         let n = s + a;
    ///         if n == 8 { out.push(Effect::Emit(4)); } // 单次回灌
    ///         n
    ///     }
    /// }
    ///
    /// let store = SyncStore::new(PingOnce, NoFx);
    /// let _ticket = store.subscribe_events(64, |_| {});
    /// store.dispatch(8);
    /// let kinds: Vec<_> = store.take_events().iter().map(|e| e.kind).collect();
    /// // 外部入队 → reduce → 回灌入队(深 1) → reduce
    /// assert_eq!(kinds, [
    ///     AuditEventKind::Enqueued { feedback_depth: 0 },
    ///     AuditEventKind::Reduced,
    ///     AuditEventKind::Enqueued { feedback_depth: 1 },
    ///     AuditEventKind::Reduced,
    /// ]);
    /// # store.close();
    /// ```
    ///
    /// 票据丢弃即静默关闭，类型层拦截：
    ///
    /// ```compile_fail
    /// # #![deny(unused_must_use)]
    /// # use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// # use skeleton_infra::runtime::SyncStore;
    /// # struct T;
    /// # impl Reducer for T {
    /// #     type State = u32; type Action = u32; type Op = Infallible;
    /// #     fn initial(&self) -> u32 { 0 }
    /// #     fn reduce(&self, s: &u32, a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + a }
    /// # }
    /// # let store = SyncStore::new(T, NoFx);
    /// store.subscribe_events(16, |_| {}); // ERROR: unused `AuditTicket`——丢弃即静默关闭事件流
    /// ```
    pub fn subscribe_events(
        &self,
        capacity: usize,
        on_event: impl FnMut(&AuditEvent) + Send + 'static,
    ) -> AuditTicket {
        let gen = self.inner.audit_gen.fetch_add(1, Ordering::AcqRel) + 1;
        let cap = capacity.max(1);
        {
            let mut guard = lock(&self.inner.audit);
            // 前订被覆盖留痕（读侧 audit_preempted）：单槽二次订阅会终止前订
            // 的事件流（典型：tracing 桥被自定义消费者顶掉），覆盖必须显形——
            // 语义不变（仍覆盖），只留痕。
            if guard.is_some() {
                self.inner.audit_preempted.fetch_add(1, Ordering::Relaxed);
            }
            *guard = Some(AuditSink {
                ring: VecDeque::with_capacity(cap),
                cap,
                gen,
                notify: Some(Box::new(on_event)),
            });
            // 派发状态换代次（issue#65 主路线①）：旧代次排队事件作废（禁泄漏
            // 进替换订阅，作废件数计入投递丢弃计数）；在途标记复位（新代次可
            // 自行派发；旧代次在途者凭代次对账自行作废，不覆盖新代次状态）。
            audit_dispatch_reset(&self.inner, gen);
            // 开闸与装槽同一临界区（issue#60 线性化点的对侧）：票据 Drop 的落闸
            // 也在本锁内 ⇒ 装槽/摘槽与闸翻转串行；且「已装槽、闸未开」的窗口
            // 不存在（该窗口内的发射会因闸 false 在环入账**之前**返回 = 静默丢
            // 取证）。热发射路径仍是一次 Acquire load。
            self.inner.audit_gate.store(true, Ordering::Release);
        }
        AuditTicket {
            gate: Arc::clone(&self.inner.audit_gate),
            audit: Arc::clone(&self.inner.audit),
            gen,
        }
    }

    /// 读侧：整体取走缓冲事件（取证消费；环容量保留复用）。未订阅 = 空 Vec。
    pub fn take_events(&self) -> Vec<AuditEvent> {
        let mut guard = lock(&self.inner.audit);
        match guard.as_mut() {
            Some(sink) => sink.ring.drain(..).collect(),
            None => Vec::new(),
        }
    }
}

// ── 关闭终态报告（issue#61 定稿 B + C）──────────────────────────────────────
//
// `close` 是「准入关闭 + best-effort 排空/交接」，**不是效果 flush**：主队列空
// 时排空环 `pop_front` 到 `None` 即返回，不进转发环 ⇒ 解释器 panic 遗留的
// 已提交后缀会原地留存，直到宿主显式处理或 store 析构。本域补的是**观察与显式
// 解决面**（不是转发面）：一个受 quiescence 闸控的报告操作 + 一个显式有计数的
// 丢弃操作。post-close 转发（含 Emit 回灌的 drain-to-idle continuation epoch 与
// 回执归属）是**另立规格**的扩展，本版不给（也不给「弹一条」式的 API —— 那会
// 搁浅出新的后缀）。

/// 单个已提交段的后缀摘要（身份词汇与 #48/#49/#57 共享）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingSegmentSummary {
    /// 产出该段全部效果的 action 的**入队序号**（producer/origin；可能早于任何
    /// 触发续转的 action）。
    pub origin_action_seq: u64,
    /// 该段下一条待转发效果的 0-based 位置（= #48 的 `effect_index` 起点；计数
    /// 含 [`Effect::Emit`] 占位，与效果种类无关）。
    pub next_effect_index: u64,
    /// 该段剩余待转发条数。
    pub effects: usize,
}

/// 已提交、未转发的效果后缀摘要（issue#61 的第三个 population）。
///
/// **不是**「pending 总数」：活跃队列、停放 deferred action（#49）、本后缀、被
/// 中断效果归属四类**分列**（见 [`CloseReport`]），禁合并成一个计数。段级
/// producer 身份保留 ⇒ 消费方可按 `(origin_action_seq, next_effect_index)` 定位
/// 剩余效果的来源（#48 词汇），且**绝不为**它们伪造普通 dispatch action seq。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PendingEffectsSummary {
    /// 待转发效果总条数（所有段合计）。
    pub effects: usize,
    /// 段级归属（FIFO 序，每段一项；空壳段不计）。
    pub segments: Vec<PendingSegmentSummary>,
}

/// 被中断效果的归属（issue#61 的第四个 population；与
/// [`AuditEventKind::EffectInterrupted`] 同词汇、同值）。
///
/// 该效果**不在** [`PendingEffectsSummary`] 里：它已随转发丢弃、不回填、不自动
/// 重放（#45 拍板 at-most-once）。它只是后缀被留下的原因，且两个序号分开保存
/// ⇒ 「谁产出、谁触发」可追（pending 效果跨 action 存活）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InterruptedEffect {
    /// 触发转发、并承接失败回执的 action 的入队序号。
    pub dispatch_action_seq: u64,
    /// 实际产出该效果的 action 的入队序号（可能早于 `dispatch_action_seq`）。
    pub origin_action_seq: u64,
    /// 该效果在产出 action 输出序列中的 0-based 位置（含 [`Effect::Emit`] 占位）。
    pub effect_index: u64,
    /// 解释器侧效果种类（[`EffectKind`]）。
    pub effect_kind: EffectKind,
    /// 效果逻辑句柄（`Run` 自身 / `Cancel` 的目标；语义随种类而变，见
    /// [`AuditEventKind::EffectInterrupted`] 的字段文档）。
    pub effect_id: Option<EffectId>,
}

/// 关闭后的终态报告（issue#61 定稿 B）：**只报告**，不改任何归属。
///
/// 四类未完成工作**分列**（禁合并成一个 pending 计数）：
///
/// 1. **活跃队列**（[`CloseReport::queued_actions`]）——`Drained` 时恒 0（它就是
///    quiescence 谓词的一半）；
/// 2. **停放 deferred action**（#49，[`CloseReport::deferred_actions`]）——不是
///    活跃排空工作，#64 谓词不计数 ⇒ 可非零；
/// 3. **已提交未转发的效果后缀**（本票，[`CloseReport::pending_effects`]）——可
///    非零；报告之后仍被 store 持有、可再报告检视，直到显式丢弃；
/// 4. **被中断效果的归属**（[`CloseReport::interrupted_effect`]）——已不在后缀里。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloseReport {
    /// 活跃队列残留（`Drained` 恒 0）。
    pub queued_actions: usize,
    /// 停放的 deferred action 数（#49；不影响 `Drained` 判定）。
    pub deferred_actions: usize,
    /// 已提交、未转发的效果后缀（本票）。
    pub pending_effects: PendingEffectsSummary,
    /// 最近一次被中断效果的归属；`None` = 本 store 从未中断过转发。
    pub interrupted_effect: Option<InterruptedEffect>,
}

/// [`SyncStore::close_wait_with_report`] 的结果。**只有 `Drained` 携带快照**：
/// 超时/重入若携带报告，调用方会把它当终态（这正是要禁的假终态快照）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloseReportOutcome {
    /// 已闭门且排空至静止：报告是本关闭 store 的**终态**（闭门后无生产者、无
    /// 排空者 ⇒ 后缀不再变化）。
    Drained(CloseReport),
    /// 超时仍有排空在跑：**不声称**任何终态——活跃 reducer/效果路径可能继续
    /// 提交工作（含新的效果后缀）。
    TimedOutStillRunning,
    /// 本线程正在本 store 的排空帧内（reducer/订阅回调/效果转发）：等待即自等。
    /// 准入**已关**（闭门先于本判定），但不声称终态。
    Reentrant,
}

/// 一段被摘出的后缀（段元数据 + 效果本体）：报告路径用后原序放回，丢弃路径
/// 让它随返回值在锁外析构。
struct PendingSuffixSegment<A, Op> {
    origin_action_seq: u64,
    next_index: u64,
    effects: VecDeque<Effect<A, Op>>,
}

/// 摘出各段待转发效果（段壳与段元数据**原位不动**；`next_index` 不推进——本
/// 操作不是转发）。
///
/// 代价：O(段内条数) 次值搬移 + 每段一个临时 FIFO。**只在关闭路径**（报告/显式
/// 丢弃）发生 ⇒ dispatch 热路径零改动。`EffectBuf` 无 `len` 面（契约 crate 不
/// 扩面），故计数必须摘出来数。
fn take_pending_suffix<A, Op>(bufs: &mut EffectBufs<A, Op>) -> Vec<PendingSuffixSegment<A, Op>> {
    let mut out = Vec::with_capacity(bufs.pending.len());
    for seg in bufs.pending.iter_mut() {
        let mut effects = VecDeque::new();
        while let Some(effect) = seg.buf.pop_front() {
            effects.push_back(effect);
        }
        out.push(PendingSuffixSegment {
            origin_action_seq: seg.origin_action_seq,
            next_index: seg.next_index,
            effects,
        });
    }
    out
}

fn summarize_pending<A, Op>(segments: &[PendingSuffixSegment<A, Op>]) -> PendingEffectsSummary {
    let mut summary = PendingEffectsSummary::default();
    for seg in segments {
        // 空壳段（中断恰在段末）没有身份可报——不计段、不计条。
        if seg.effects.is_empty() {
            continue;
        }
        summary.effects += seg.effects.len();
        summary.segments.push(PendingSegmentSummary {
            origin_action_seq: seg.origin_action_seq,
            next_effect_index: seg.next_index,
            effects: seg.effects.len(),
        });
    }
    summary
}

/// 按 FIFO 原序放回（报告路径：读而不动）。
fn restore_pending_suffix<A, Op>(
    bufs: &mut EffectBufs<A, Op>,
    segments: Vec<PendingSuffixSegment<A, Op>>,
) {
    debug_assert_eq!(bufs.pending.len(), segments.len());
    for (seg, suffix) in bufs.pending.iter_mut().zip(segments) {
        debug_assert!(seg.buf.is_empty(), "静止窗内段壳必须是空壳");
        for effect in suffix.effects {
            seg.buf.push(effect);
        }
    }
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> SyncStore<S, A, Op> {
    /// 关闭准入 → 等 #64 的静止谓词 → **仅在 `Drained` 之后**快照终态报告
    /// （issue#61 定稿 B）。
    ///
    /// 与 legacy [`Store::close`] 的差别：`close` 只
    /// best-effort 排空、**不是**效果 flush（主队列空时根本不进转发环）；本方法
    /// **等**到「排空租约释放 + 活跃队列空」，然后才读终态。等待与快照在**一次
    /// 调用内**完成——故调用方不必（也不该）用 `close_wait()` + 另一次报告拼，
    /// 那会留下「两次调用之间有别的生命周期操作插进来」的 TOCTOU 组合。
    ///
    /// 保证：
    ///
    /// - **超时/重入不携带快照**（[`CloseReportOutcome`] 的类型层保证）——活跃
    ///   reducer/效果路径可能继续提交工作，「看起来终态」的计数会误导读方；
    /// - **绝不执行宿主 `EffectRuntime`**：收到停机请求不是重放已提交效果的理由
    ///   （效果可能做 I/O 或改外部系统，框架无法判定重放安全）；
    /// - 报告不改任何归属：保留的后缀仍被 store 持有，直到
    ///   [`Self::discard_pending_effects`] 显式丢弃；本版**不提供** post-close
    ///   转发面（Emit 回灌在闭门后仍可入队 ⇒ 需要独立规格的 continuation epoch）；
    /// - 幂等：已闭门再调用只重新等 + 重新报告。
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
    /// use skeleton_infra::runtime::{CloseReportOutcome, SyncStore};
    ///
    /// struct Counter;
    /// impl Reducer for Counter {
    ///     type State = u32;
    ///     type Action = u32;
    ///     type Op = Infallible;
    ///     fn initial(&self) -> u32 { 0 }
    ///     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32>) -> u32 { s + a }
    /// }
    ///
    /// let store = SyncStore::new(Counter, NoFx);
    /// store.dispatch(1);
    /// match store.close_wait_with_report(Duration::from_secs(5)) {
    ///     CloseReportOutcome::Drained(report) => {
    ///         assert_eq!(report.queued_actions, 0);
    ///         assert_eq!(report.deferred_actions, 0);
    ///         assert_eq!(report.pending_effects.effects, 0); // 零效果 reducer
    ///         assert!(report.interrupted_effect.is_none());
    ///     }
    ///     other => panic!("静止 store 必须报 Drained：{other:?}"),
    /// }
    /// ```
    pub fn close_wait_with_report(&self, timeout: Duration) -> CloseReportOutcome {
        // 闭门先于重入判定（定稿条目 2/3）：`Reentrant` 的语义是「准入**已关**、
        // 但当前排空帧不能等自己」。与 `Store::close` 取同一把订阅闸 ⇒ 与
        // `try_subscribe` 同一线性化点（闸在排空前释放）。
        {
            let _gate = lock(&self.inner.sub_gate);
            self.inner.closed.store(true, Ordering::Release);
        }
        match self.close_wait_timeout(timeout) {
            CloseWaitOutcome::Drained => match self.close_report() {
                Some(report) => CloseReportOutcome::Drained(report),
                // 静止窗复核失败 = 又有排空工作出现（闭门后无生产者 ⇒ 理论不可达）：
                // 宁报「仍在跑」也不给一个看起来终态的快照。
                None => CloseReportOutcome::TimedOutStillRunning,
            },
            CloseWaitOutcome::TimedOutStillRunning => CloseReportOutcome::TimedOutStillRunning,
            CloseWaitOutcome::Reentrant => CloseReportOutcome::Reentrant,
        }
    }

    /// 显式丢弃已提交、未转发的效果后缀（issue#61 定稿 C）：返回**精确**摘要
    /// （条数 + 段级身份）。
    ///
    /// `None` = 未关闭或未静止（有排空在跑）——本次**什么都没丢**，也不声称终态。
    /// 推荐时机：[`Self::close_wait_with_report`] 返回 `Drained` 之后（那里的
    /// 计数即本次将要丢弃的内容）。
    ///
    /// **不跑宿主代码**：只摘缓冲并让效果载荷在内部锁外析构——绝不调用
    /// `EffectRuntime::run`（含 `Effect::Emit`：丢弃 = 不回灌）。丢弃后后缀为空
    /// （再报告可见 0），store 其余状态不受影响。
    pub fn discard_pending_effects(&self) -> Option<PendingEffectsSummary> {
        if !self.is_closed() {
            return None;
        }
        let (summary, discarded) = self.with_quiescent_effect_bufs(|bufs| {
            let segments = take_pending_suffix(bufs);
            let summary = summarize_pending(&segments);
            // 段壳一并出队：丢弃 = 后缀整体消失（不留下可续转的半段）。
            bufs.pending.clear();
            (summary, segments)
        })?;
        // 载荷析构在内部锁之外（宿主 `Drop` 可能加锁或 panic，禁在临界区内跑）。
        drop(discarded);
        Some(summary)
    }

    /// 终态报告快照（`None` = 复核发现仍有排空工作）。
    fn close_report(&self) -> Option<CloseReport> {
        // 被中断效果与 deferred 队列各自独立取锁（不同 population，禁合并计数）。
        let interrupted_effect = *lock(&self.inner.interrupted_effect);
        let deferred_actions = lock(&self.inner.deferred_queue).len();
        let pending_effects = self.with_quiescent_effect_bufs(|bufs| {
            let segments = take_pending_suffix(bufs);
            let summary = summarize_pending(&segments);
            // 报告只读：原序放回（后缀仍归 store 所有，等显式丢弃）。
            restore_pending_suffix(bufs, segments);
            summary
        })?;
        // 活跃队列：静止窗内已复核为空；这里**如实采样**（不是 0 常量）——
        // 闭门后无生产者，读数必为 0。
        let queued_actions = lock(&self.inner.queue).len();
        Some(CloseReport {
            queued_actions,
            deferred_actions,
            pending_effects,
            interrupted_effect,
        })
    }

    /// 静止窗专属访问（issue#61）：持 `queue` 锁复核 #64 谓词（无排空租约 +
    /// 活跃队列空），成立后才触达效果缓冲。
    ///
    /// SAFETY（[`ExclusiveCell::with_mut`] 的第四处访问点，安全纪要见
    /// [`DrainExclusive`]）：静止窗内本 cell **无写者**——闭门后无生产者（push 在
    /// `queue` 锁内复检 `closed`），排空者要有记录才会进 `with_mut`（队列空 ⇒ 它
    /// `pop_front` 到 `None` 即放旗返回，不触达本 cell），`Effect::Emit` 回灌也只
    /// 发生在排空者的转发环内；读者经本函数的 `queue` 锁串行 ⇒ 同一时刻至多一个
    /// `&mut`。锁序 `queue → effect_buf`（与排空环的 `queue` 先于 `effect_buf`
    /// 同向；对照实现的 `Mutex` 变体亦无反转）。
    fn with_quiescent_effect_bufs<R>(
        &self,
        f: impl FnOnce(&mut EffectBufs<A, Op>) -> R,
    ) -> Option<R> {
        let queue = lock(&self.inner.queue);
        if self.inner.draining.load(Ordering::Acquire) || !queue.is_empty() {
            return None;
        }
        let out = self.inner.effect_buf.with_mut(f);
        drop(queue);
        Some(out)
    }
}

/// 残留弃队留痕：**快照式**，随 store 句柄退出做一次计数。单线程常规路径 drop
/// 恒 idle（dispatch/close 同步排空至空）；但**句柄可在排空进行中被丢弃**（另一线程
/// 持 sender / inner Arc 仍活着），此时快照到的队列可能是**普通未处理 action**，
/// 不限于预算止步残留 ⇒ 弃队数并入 `feedback_exceeded` 同账，两个 population
/// **不可区分**（bd udf-frame-8ad：显式承认的口径混同，不静默改义）。
/// 读窗 = inner Arc 存续期（SyncStore 已弃而 inner 仍被持有，如测试白盒）；
/// **最后一份句柄析构后本留痕不可读**（累加点是析构，读口在实例上）⇒ 本路径只服务
/// 白盒/仍有 inner 持有者的场景，**不是**对外可观测面；对外可观测的是停机路径的
/// `ShutdownReport` / `rejected_after_close`。
impl<S, A, Op> Drop for SyncStore<S, A, Op> {
    fn drop(&mut self) {
        let discarded = match self.inner.queue.lock() {
            Ok(queue) => queue.len(),
            // 毒锁 = 此前已有 panic 上浮；取内层只读长度，drop 内不二次爆炸
            Err(poisoned) => poisoned.into_inner().len(),
        }
        // 停放队列（预算止步的残留）同样留痕——禁静默弃队。
        + match self.inner.deferred_queue.lock() {
            Ok(parked) => parked.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        if discarded > 0 {
            self.inner
                .feedback_exceeded
                .fetch_add(discarded as u64, Ordering::SeqCst);
        }
    }
}

/// 排空权守卫：正常出口**持锁放旗**（[`DrainGuard::disarm`]——调用点必持
/// queue 锁，旗与队列状态的交接在同一临界区）；异常出口（panic 展开）只做
/// **记账**：结算当前记录与队列残留的完成回执、摘队、放旗。
///
/// 展开路径纪律（issue#41）：Drop 内**绝不调用** reducer / 解释器 / 订阅回调 /
/// 审计回调——展开中二次 panic = 进程 abort。
///
/// **但展开清理期必有用户代码**（issue#66）：被摘下的队列记录按值持有 `A`，
/// 结算之后其析构在**所有内部锁之外**发生（锁内只搬指针）。框架拥有并按值丢弃
/// 这些载荷 ⇒ 既无法阻止其 `Drop` 运行，也不做 `catch_unwind` 隔离（捕得住
/// abort、挡不住重入）。义务因此落在宿主：该 Drop **必须 panic-free**，且
/// **不得重入同一 Store**（放旗已在本 Drop 前半完成 ⇒ 重入会夺得新租约、在
/// 展开栈上跑完整个排空环）。契约正文见 [`SyncStore`]「Action 析构义务」。
///
/// 摘队与放旗同一临界区：生产者要么进本批（被结算为失败），要么在放旗之后
/// 夺下新租约（不搁浅）。
struct DrainGuard<'a, S, A, Op> {
    inner: &'a Arc<StoreInner<S, A, Op>>,
    /// 是否仍持排空权（正常出口 disarm 后为 false → Drop 不动旗）。
    armed: bool,
    /// 当前在途记录（结算用；动作完成即清空）。
    current: Option<CurrentMeta>,
    /// 当前阶段（终局分类：提交前 / 提交后）。
    phase: DispatchPhase,
    /// 当前记录的状态是否已提交（单调 false→true）。
    committed: bool,
}

/// 在途记录元信息（结算回执用；不含 action 载荷——载荷随记录走）。
struct CurrentMeta {
    /// 在途记录序号。
    seq: u64,
    /// 完成账本。
    ledger: Arc<Ledger>,
    /// 是否账本的根。
    root: bool,
}

impl<S, A, Op> DrainGuard<'_, S, A, Op> {
    /// 正常出口放旗（调用点必持 queue 锁——放旗与队列空判定同临界区，
    /// 晚到的 push 由其排空者自排空）。
    fn disarm(&mut self) {
        self.inner.draining.store(false, Ordering::Release);
        self.armed = false;
        // 状态（放旗）已发布，且调用点持 queue 锁——等待者被唤醒后重取锁即可见
        // 「队列空 + 无租约」的终局谓词。
        self.inner.stop_wait.notify_all();
    }
}

impl<S, A, Op> Drop for DrainGuard<'_, S, A, Op> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // 展开路径：只做记账（禁调任何回调；载荷析构在锁外进行——其 Drop 的
        // 义务见类型文档）。
        // 1. 当前在途记录：按最后已知阶段结算失败（状态已提交则如实标注——
        //    提交后失败 ≠ reducer 失败）。
        if let Some(meta) = self.current.take() {
            let failure = DispatchFailure {
                seq: meta.seq,
                phase: self.phase,
                state_committed: self.committed,
                cause: "drain interrupted by panic",
            };
            if meta.root {
                meta.ledger.finish_root(Some(failure));
            } else {
                meta.ledger.descendant_done(Some(failure));
            }
            // 终局发布才是可复用边界：资格由账本判定（等待型根/仍有后代 ⇒ 不入池）。
            release_ledger(self.inner, &meta.ledger);
        }
        // 2. 队列残留：整批摘下并结算失败（不留在队列里无主，也不重放——
        //    被中断的 action 可能已有部分外部副作用，重放会重复副作用）。
        let detached: Vec<ActionRecord<A>> = {
            let mut queue = match self.inner.queue.lock() {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
            let detached: Vec<ActionRecord<A>> = queue.drain(..).collect();
            // 摘队与放旗同一临界区：生产者要么进本批（被结算），要么在放旗后
            // 夺下新租约（不搁浅）。
            self.inner.draining.store(false, Ordering::Release);
            // unwind 臂是放旗的第二点：不放通知会让 close_wait 永久等一个已
            // 不存在的租约持有者。
            self.inner.stop_wait.notify_all();
            detached
        };
        // 3. 逐条结算；载荷在本步末尾的锁外析构——该 Drop 必须 panic-free 且
        //    不得重入同一 Store（宿主义务，见类型文档）。
        for record in detached {
            let failure = DispatchFailure {
                seq: record.seq,
                phase: DispatchPhase::Queued,
                state_committed: false,
                cause: "drain interrupted by panic",
            };
            if record.root {
                record.ledger.finish_root(Some(failure));
            } else {
                record.ledger.descendant_done(Some(failure));
            }
            release_ledger(self.inner, &record.ledger);
        }
    }
}

/// 排空入口（close 路径与重入兜底）：夺排空权后交 [`drain_owned`]。
/// dispatch 的常规路径不经过此函数——其夺权已并入 push 临界区
/// （[`enqueue`]），同窗免二次加锁。
fn drain<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
) {
    // 已在排空（重入回灌）：只入队，最外层循环排空。AcqRel：RMW 恒见修改序最新
    // 值（无 stale 可言）；Acquire 侧同步前任 Release 放旗，Release 侧发布本任。
    if inner.draining.swap(true, Ordering::AcqRel) {
        return;
    }
    drain_owned(inner, None);
}

/// 持排空权排空至 idle：出队→reduce→快照替换→效果转发（Emit 回灌入队，
/// Run/Cancel 交解释器）→通知订阅。
///
/// 交接语义（锁消融批修订）：持有排空权=同步排空至队列空；否则=入队保证
/// 序。出口协议为**持锁放旗**：pop 得 None 时仍持 queue 锁内完成放旗再返回——
/// push 全部被挡在锁外，此刻的空判定即终局判定（原「放旗→复检」两步的交错窗在锁
/// 内不可插入，复检退化为同锁内的已见空）；晚到的 push 由后到者自己的夺权排空。
/// 「重夺排空权」分支随窗口封死而不可达，rearm 机制随之删除（不变量由孤儿
/// 监视测试与守恒竞速持续咬住）。
fn drain_owned<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    first: Option<ActionRecord<A>>,
) {
    let mut guard = DrainGuard {
        inner,
        armed: true,
        current: None,
        phase: DispatchPhase::Queued,
        committed: false,
    };
    let mut first = first;
    loop {
        let mut record = match first.take() {
            Some(record) => record,
            None => {
                // let-else 绑定：queue 守卫先于 action 使用释放，Emit 回灌再锁
                // queue 不会自死锁
                let mut queue = lock(&inner.queue);
                let Some(record) = queue.pop_front() else {
                    // 出口交接（M1）：持锁放旗——push 被锁外挡，锁内空即终局空；
                    // queue 守卫随 return 释放，晚到 push 由其排空者自排空
                    guard.disarm();
                    return;
                };
                drop(queue);
                record
            }
        };
        // 回灌深度随 action 携带（入队时定深，见 [`ActionOrigin`]）——
        // 外部 action 深度 0，同栈回灌 = 产生它的 action 深度 + 1。自指回灌环
        // 在预算点止步，dispatch 必返回（修复前排空环永不达空，dispatch 永挂；
        // 修复前 back 通道同栈回灌标 External 深度清零，预算永不触发）。
        let feedback_depth = record_depth(record.origin);
        if feedback_depth > FEEDBACK_DEPTH_BUDGET {
            // 预算止步：脱钩（续接账本）+ **停放**到 deferred 队列。
            // 不留在主队列——留在主队列会让它顶在队首把外部流量反复顶住
            // （饿死健康流量），且每任排空都要重新脱钩（链停不下来）。
            // 停放 = 保留序号与载荷（零丢失）+ 留痕，等待显式恢复
            // （[`SyncStore::resume_deferred`]，issue#49）。
            // 停放来源（issue#49）：记录以 `Resumed` 携带**停放时的深度**，恢复时
            // 由 [`record_depth`] 清零 ⇒ 新 epoch。
            record.origin = ActionOrigin::Resumed {
                prior_depth: feedback_depth,
            };
            let origin_ledger = Arc::clone(&record.ledger);
            // 停放队列容量（issue#49/#57）：**独立于外部准入容量**、可配可观测。
            // 满 ⇒ 续接无法留存 = 显式非成功终局 + 专用计数（不消失、不标成
            // 「已停放」）。只有本排空者会 push ⇒ 判长与 push 之间无 push 竞争
            // （恢复路径只 pop）。
            let mut parked = lock(&inner.deferred_queue);
            let cap = inner.deferred_capacity.load(Ordering::Relaxed);
            let retained = cap == 0 || parked.len() < cap;
            // 测试停靠点（issue#95）：停放已按旧 cap 裁决、留存/拒收尚未提交——
            // 容量 setter 此刻只能被 deferred_queue 锁挡住。
            #[cfg(test)]
            inner.deferred_gate.park();
            let continuation = detach_deferred(record, retained);
            if retained {
                parked.push_back(continuation);
            } else {
                drop(parked);
                // 续接拿自己的非成功终局（不进任何 population），原账本记容量拒收。
                continuation.ledger.finish_root(Some(DispatchFailure {
                    seq: continuation.seq,
                    phase: DispatchPhase::Queued,
                    state_committed: false,
                    cause: "deferred queue full: continuation not retained",
                }));
                release_ledger(inner, &continuation.ledger);
                inner
                    .deferred_capacity_rejected
                    .fetch_add(1, Ordering::SeqCst);
            }
            // 脱钩把**原账本**以 Deferred 收口：它可能在此刻终局 ⇒ 按同一归还
            // 规则尝试一次（续接账本是独立账本，不随原账本入池——issue#84）。
            release_ledger(inner, &origin_ledger);
            inner.feedback_exceeded.fetch_add(1, Ordering::SeqCst);
            // 取证时间线的病理终点（R1-2）：预算止步以事件收尾，环结构可重建。
            audit_emit(inner, AuditEventKind::FeedbackBudgetExceeded);
            // 主队列继续排空（外部流量照常推进——停放不吞健康流量）。
            continue;
        }
        // 本 action 处理期间（reduce/效果转发/订阅回调）本线程的一切入队
        // 一律计深 feedback_depth+1；迭代末守卫恢复原值，panic 栈展开亦然。
        // 排空帧（TLS 栈）：本 action 处理期间（reduce/效果转发/订阅回调）本线程
        // 的一切入队 = 同栈回灌（共享本 action 的账本 = 因果父 + 计深 +1）。
        let _frame = {
            DRAIN_FRAMES.with(|cell| {
                cell.borrow_mut().push(DrainFrame {
                    store_id: inner.store_id,
                    depth: feedback_depth,
                    ledger: Arc::clone(&record.ledger),
                });
            });
            DrainFrameGuard
        };
        // 当前在途记录登记（排空中断时按此结算回执）。
        guard.current = Some(CurrentMeta {
            seq: record.seq,
            ledger: Arc::clone(&record.ledger),
            root: record.root,
        });
        guard.phase = DispatchPhase::Reducing;
        guard.committed = false;
        let current = Arc::clone(&lock(&inner.state));
        // 效果缓冲排空权独占（[`ExclusiveCell`]）：take/还两对锁消融——reducer
        // 期间不持任何锁的契约不变：无锁可毒、reducer panic 时栈展开零残留，
        // 缓冲容量连丢一次都不丢。
        // 两段式缓冲（issue#44 事务边界，见 [`EffectBufs`]）：**暂存区**每次
        // reducer 调用独享（入口清零），**已提交待转发队列**跨 action 存活。
        // reducer 正常返回 ⇒ 提交点把暂存区整段并入已提交队列（按 action 序）；
        // reducer panic ⇒ 本次暂存区整段丢弃（未提交的迁移零外部副作用），
        // 更早已提交、只等续转的效果原样留存（禁无差别清空：否则「上次解释器
        // panic 遗留的效果丢失」这个已有保障回归）。解释器 panic ⇒ 状态已发布，
        // 只留**未转发**的后缀（已转发前缀不重放），下一任成功排空续转。
        // 转发改为逐条摘除（见转发环注释），成功路径转发即摘除，缓冲照常回空，
        // 保容量语义不变，稳态零新增分配不受影响。
        // reducer panic 与解释器 panic 同形留痕（与全链计数纪律对称）：catch 只
        // 观察（计数+审计现场）后把 payload 带出区间；`resume_unwind` 落在独占
        // 区间之外——传播语义不变（panic 照常在 dispatch 调用点上浮），且对照实现
        // （Mutex）的锁不会在展开途中被毒化。动作本体已出队随栈展开丢失（已知
        // 契约）；本次调用已产的暂存效果随未提交迁移一并丢弃（**不计数**，见
        // [`SyncStore::reducer_panic_count`]）。
        // SAFETY: 持排空权（DrainGuard armed 区间）；重入 dispatch 只入队即返，
        // 到不了此处——独占访问成立，见 DrainExclusive 安全纪要
        let reduced = inner.effect_buf.with_mut(|bufs| {
            // 暂存区入口清零：每次 reducer 调用独享（跨调用不串味）。
            bufs.staged.clear();
            match catch_unwind(AssertUnwindSafe(|| {
                inner
                    .reducer
                    .reduce(&current, record.action, &mut bufs.staged)
            })) {
                Ok(next) => {
                    // 提交点（issue#44）：reducer 正常返回 ⇒ 本次调用的暂存输出
                    // 整段并入已提交待转发队列（按 action 序）。提交点落在状态
                    // 发布之前——旧快照析构 panic（状态已发布）时，已提交效果
                    // 不得随之丢失。**来源在此固化**（issue#48）：产出这些效果
                    // 的 action = 当前被 reduce 的 `record` ⇒ 其入队序号。
                    bufs.commit(record.seq);
                    Ok(next)
                }
                Err(payload) => {
                    // 失败调用：本次暂存输出整段丢弃（未提交的迁移零外部副作用）
                    // ——更早已提交、只等续转的效果原样留存（禁无差别清空）。
                    bufs.staged.clear();
                    inner.reducer_panics.fetch_add(1, Ordering::SeqCst);
                    audit_emit(inner, AuditEventKind::ReducerInterrupted);
                    Err(payload)
                }
            }
        });
        let next_val = match reduced {
            Ok(next) => next,
            Err(payload) => std::panic::resume_unwind(payload),
        };
        audit_emit(inner, AuditEventKind::Reduced);
        // CoW 前置：reduce 后立即放手 current——不放
        // 手则 state 强计数恒 2，下方 make_mut 永走伪克隆分支，稳态零分配落空。
        drop(current);
        // 订阅广播（锁消融修订）：稳态版本匹配=单次 Acquire load 直接
        // 复用快照，零锁零 upgrade；版本不匹配才冷重建（锁 subs 单遍 upgrade
        // 收集+代笔回收死 Weak，Stamp 用载入时版本——竞态多出的重建无害）。
        // 快照落位（消融：原「再锁 state 再 Arc::clone」的快照预取一对锁+一对
        // 引用计数整对删除）：快照即 `next` 本体。零订阅稳态把 `next` 直接移入
        // state（省 clone 对）；有订阅者才 clone 留本体给回调。管线序：
        // reduce（含提交点：暂存区→已提交待转发队列）→快照替换→效果转发→通知
        // （异步投递先行、同步回调随后）。
        // SAFETY: 持排空权（DrainGuard armed 区间）；通知段仅在排空权下进入，
        // 独占访问成立，见 DrainExclusive 安全纪要
        // 冷重建与「有无订阅者」判读在同一次独占区间内完成；落位/转发不触达该
        // 缓存，故与下方通知段分两次取用（不把整段管线塞进闭包）。
        let has_subscribers = inner.notify_cache.with_mut(|cache| {
            let version = inner.subs_version.load(Ordering::Acquire);
            if cache.version != version {
                let mut subs = lock(&inner.subs);
                cache.slots.clear();
                subs.retain(|w| match w.upgrade() {
                    Some(slot) => {
                        cache.slots.push(slot);
                        true
                    }
                    None => false,
                });
                cache.version = version;
            }
            !cache.slots.is_empty()
        });
        // CoW 快照落位（快照分配税消除；析构在锁外——契约细节见 helper 文档）。
        // 退役快照交还调用方：阶段先推进到「状态已提交」，再析构旧值——旧值
        // 析构 panic = 提交后失败（不是 reducer 失败）。
        let retired = place_snapshot(inner, next_val);
        guard.phase = DispatchPhase::StateCommitted;
        guard.committed = true;
        drop(retired);
        // 通知句柄：订阅态在落位后补一次 state 锁克隆新快照（回调锁外借用）；
        // 句柄随迭代末消亡，稳态计数回落 1，下一 action 的 make_mut 仍原地命中。
        let notified_next = if has_subscribers {
            Some(Arc::clone(&lock(&inner.state)))
        } else {
            None
        };
        // 转发环（禁吞语义见 helper 文档）：Emit 回灌入队、Run/Cancel 交解释器。
        // `dispatch_action_seq` = 本次触发转发的 action（= 承接失败回执的那个
        // action）的入队序号——与 pending 效果各自的来源序号（可能更早）区分
        // （issue#48）。
        guard.phase = DispatchPhase::ForwardingEffects;
        inner.effect_buf.with_mut(|bufs| {
            forward_effects(inner, bufs, feedback_depth, &record.ledger, record.seq)
        });
        guard.phase = DispatchPhase::EffectsForwarded;
        // 异步投递段（ADR 0008）：排在同步回调之前——否则同步档慢订阅者会把
        // 异步消费者的可见延迟一并拖后。投递只发版本号：O(1)、门控短路
        // （零异步订阅者一次 Acquire load 即返）、锁外调用宿主代码。
        guard.phase = DispatchPhase::Notifying;
        notify_async(inner);
        // 通知段（本地先行、跨线程随后；返回当拍是否有订阅者收到快照）。
        let snapshot_notified = inner
            .notify_cache
            .with_mut(|cache| notify_snapshot_consumers(inner, cache, &notified_next));
        // 通知节点（R1-2）：只在当拍确有订阅者收到快照时记录——零订阅者的
        // dispatch 无 Notified 事件（时间线如实，不造心跳噪声）。
        if snapshot_notified {
            audit_emit(inner, AuditEventKind::Notified);
        }
        // 结算本记录：自身阶段完成 → 账本终局判定（根账本在全部因果后代终局
        // 前不发布终局——因果后代屏障：根调用者的同步边界覆盖其引发的子动作）。
        if record.root {
            record.ledger.finish_root(None);
        } else {
            record.ledger.descendant_done(None);
        }
        guard.current = None;
        // 每个可能使账本终局的转移后都尝试归还：非等待型根的**最后一个后代**
        // 结算即终局发布点（issue#84——终局发布是唯一可复用边界）。
        release_ledger(inner, &record.ledger);
    }
}

/// 脱钩一条超预算记录：原账本以非成功收口（`retained` 决定记「已停放」还是
/// 「容量拒收」），记录改挂**独立**的续接账本（非等待型根：归属框架，终局发布
/// 时归还）——原账本的终局一经发布不可变，续接记录的后续结果不得改写它
/// （issue#84 ⑥ / #49 定稿条目 3）。
fn detach_deferred<A: Send + 'static>(
    mut record: ActionRecord<A>,
    retained: bool,
) -> ActionRecord<A> {
    let origin_ledger = Arc::clone(&record.ledger);
    let continuation = Ledger::new(false);
    origin_ledger.detach_deferred(record.root, retained);
    record.ledger = continuation;
    record.root = true;
    record
}

/// CoW 快照落位（快照分配税消除）：稳态
/// （current 已放手、无外部 snapshot() 持有者）强计数=1 → make_mut 原地
/// 写入零分配；有外部持有者时自动克隆新分配、旧分配冻结——持有者永见旧
/// 快照，快照一致性逐比特保持（CoW 即快照语义的业界标准实现）。
/// **mem::replace 把旧 S 挪出锁外析构是本形态最大风险点**：原地赋值会在
/// state 锁内 drop 旧值，S:Drop panic 即毒锁变砖——锁内必须
/// 只剩 memcpy 与计数原子，析构在本函数返回后的 drop(old_snapshot) 处（锁外）发生。
fn place_snapshot<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    next_val: S,
) -> S {
    let old_snapshot = {
        let mut slot = lock(&inner.state);
        std::mem::replace(Arc::make_mut(&mut slot), next_val)
    };
    // 退役快照交还调用方：析构在锁外，且**在调用方推进阶段之后**——旧值析构
    // panic 是「状态已提交」之后的事，不得记成 reducer 失败。
    old_snapshot
}

/// 转发环：逐条借出转发（只转发**已提交**的效果——暂存区在提交点已并入队列）。
/// 成功才从段内摘除——panic 时正在转发的一条已离段（随帧丢弃，不回填），
/// 未转发后缀原地留在段内、段原地留在队首，由下一任成功排空续转（禁吞：
/// 已提交效果零丢失；已转发前缀不重放）。不能用 `drain(..)`：其 Drop 语义是
/// 「完成排空」（未消费元素被 drop，drain_keep_rest 才回填且 unstable）——
/// `drain(..)` 的静默蒸发本体在此，「clear 吞掉」是次生路径。段壳只在段空时
/// 出队（j9q 段化）⇒ panic 路径段带后缀留存，语义与逐条包装版等价。
/// 回灌句柄为 StoreInner 缓存字段，禁每 action 新建 Arc 闭包。
///
/// **来源元数据**（issue#48）：每段自带「产出它的 action 的入队序号」，段内
/// 偏移即 `effect_index`（见 `PendingSegment`——j9q 段化后逐条元数据收进段级，
/// 逐条 `(origin, index)` 语义零变化）。`dispatch_action_seq` = **本次触发转发**
/// 的 action 的入队序号（调用方传入，与失败回执的 action 同一个）；
/// `origin_action_seq` = 该效果**产出者**的序号（可能更早——pending 效果跨
/// action 存活）。
///
/// **缓冲回收**（j9q）：段空摘壳时把满缓冲 `mem::swap` 回 `staged`（稳态零
/// 新增分配）；panic 路径不经过摘壳臂 ⇒ 不影响该不变量（毒化半砖后的一次性
/// 分配不在稳态口径内）。
fn forward_effects<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static>(
    inner: &Arc<StoreInner<S, A, Op>>,
    bufs: &mut EffectBufs<A, Op>,
    feedback_depth: u64,
    parent: &Arc<Ledger>,
    dispatch_action_seq: u64,
) {
    // 单步转发决策：把「段内弹出一条」与「段空摘壳」从借用区带出——
    // `bufs.pending` 与 `bufs.staged` 的 &mut 互斥 ⇒ 决策与执行分段借用。
    enum Step<A, Op> {
        Forward {
            effect: Effect<A, Op>,
            origin_action_seq: u64,
            effect_index: u64,
        },
        Retire,
    }
    loop {
        let step = {
            let seg = match bufs.pending.front_mut() {
                Some(seg) => seg,
                None => break,
            };
            match seg.buf.pop_front() {
                Some(effect) => {
                    let effect_index = seg.next_index;
                    seg.next_index += 1;
                    Step::Forward {
                        effect,
                        origin_action_seq: seg.origin_action_seq,
                        effect_index,
                    }
                }
                None => Step::Retire,
            }
        };
        let (origin_action_seq, effect_index, effect) = match step {
            Step::Retire => {
                // 段空摘壳：满缓冲换回 staged（稳态零新增分配），空壳出队 drop。
                let mut seg = bufs.pending.pop_front().expect("front_mut 判空后队首必存");
                std::mem::swap(&mut seg.buf, &mut bufs.staged);
                continue;
            }
            Step::Forward {
                effect,
                origin_action_seq,
                effect_index,
            } => (origin_action_seq, effect_index, effect),
        };
        match effect {
            Effect::Emit(a) => {
                inner.feedback_emitted.fetch_add(1, Ordering::Relaxed);
                // 回灌入队 = 因果后代：共享父账本 + 未终局后代计数 +1（计数与入队
                // 同一 queue 临界区，锁序 queue → ledger）——父账本在后代可见前
                // 不可能发布终局。
                let seq = inner.next_seq.fetch_add(1, Ordering::SeqCst);
                let mut queue = lock(&inner.queue);
                parent.add_descendant();
                queue.push_back(ActionRecord {
                    seq,
                    action: a,
                    origin: ActionOrigin::Feedback(feedback_depth + 1),
                    ledger: Arc::clone(parent),
                    root: false,
                });
                drop(queue);
                // 回灌入队与外部入队同一事件节点（深度链 = 环结构重建键）
                audit_emit(
                    inner,
                    AuditEventKind::Enqueued {
                        feedback_depth: feedback_depth + 1,
                    },
                );
            }
            effect => {
                // 效果身份（kind + 逻辑 id）：**单一映射点** `EffectKind::of`
                // （skeleton crate 内穷尽匹配 Effect 词表 ⇒ 词表演进时编译期
                // 强制同步）。非 Emit ⇒ 恒 `Some`（`of` 仅对 Emit 返回 `None`，
                // 而 Emit 已在上方分支分流）。
                let (effect_kind, effect_id) = EffectKind::of(&effect)
                    .expect("非 Emit 效果必经解释器（EffectKind::of 仅对 Emit 返回 None）");
                // 历史 Run-ID 诊断（issue#62，**默认关闭**）：门闸不成立时只花
                // 一次原子 load——不登记、不分配、不取锁；Run/Cancel 的转发
                // 语义与诊断是否启用无关（诊断只观测，不改解释器责任边界：
                // Cancel 语义由解释器自证，迟到 Cancel 是合法时序）。
                if inner.run_id_diag.is_enabled() {
                    match &effect {
                        Effect::Run { id, .. } => inner.run_id_diag.note_run(id.get()),
                        Effect::Cancel { id } => inner.run_id_diag.note_cancel(id.get()),
                        // Effect 词表 non_exhaustive（跨 crate match 须通配）：
                        // 未知新变体与寻址校验无关，原样落下方解释器转发
                        _ => {}
                    }
                }
                // 解释器 panic 毒化 runtime 锁，此后每个产效果 dispatch
                // 恒 panic（半砖：reduce 照常执行、状态持续前进，效果路径
                // 原地无恢复 API）。修法 = PoisonError into_inner 恢复守卫 +
                // clear_poison 复位毒位 + 计数上浮（runtime_panic_count），
                // panic 本体照常传播（禁 catch_unwind，fail-visible）。恢复
                // 的 Box 可能已被 panic 打断在任意中间态——run panic 后解释器
                // 自身一致性自证（宿主适配层责任，见 EffectRuntime 契约注）。
                let mut runtime = match inner.runtime.lock() {
                    Ok(guard) => guard,
                    Err(poisoned) => {
                        inner.runtime_panics.fetch_add(1, Ordering::SeqCst);
                        let guard = poisoned.into_inner();
                        inner.runtime.clear_poison();
                        guard
                    }
                };
                // 解释器 panic 在时间线留痕（EffectInterrupted）——catch_unwind
                // 只观察不吞：发完事件原样 resume（payload/传播面不变；runtime
                // 守卫在闭包外存活，随外层展开照常毒化，恢复路径与 fail-visible
                // 均不受影响）。事件携带结构化效果身份：消费方据此定位「**哪
                // 一个**效果处于 indeterminate 态」并与失败回执对齐
                // （issue#48）。
                let run = catch_unwind(AssertUnwindSafe(|| runtime.run(effect, &inner.back)));
                if let Err(payload) = run {
                    // 中断归属留痕（issue#61 close 报告读侧）：与下方审计事件
                    // 同一组值——「谁产出、谁触发、第几条、哪个句柄」。
                    *lock(&inner.interrupted_effect) = Some(InterruptedEffect {
                        dispatch_action_seq,
                        origin_action_seq,
                        effect_index,
                        effect_kind,
                        effect_id,
                    });
                    audit_emit(
                        inner,
                        AuditEventKind::EffectInterrupted {
                            dispatch_action_seq,
                            origin_action_seq,
                            effect_index,
                            effect_kind,
                            effect_id,
                        },
                    );
                    std::panic::resume_unwind(payload);
                }
                audit_emit(inner, AuditEventKind::EffectForwarded);
            }
        }
    }
}

/// 通知段：本地订阅先行、跨线程订阅随后（确定性），返回当拍是否有订阅者
/// 收到快照（审计 `Notified` 节点的判据）。
fn notify_snapshot_consumers<
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
>(
    inner: &Arc<StoreInner<S, A, Op>>,
    cache: &NotifyCache<S>,
    notified_next: &Option<Arc<S>>,
) -> bool {
    // 本地订阅（单线程档）：先于跨线程广播判读——零本地订阅一次 relaxed
    // 读即短路；有本地订阅时零跨线程订阅也必触发（快照按需取，本地闭包
    // panic 摘除计入 subscriber_panics，fail-closed）。同一 dispatch 通知
    // 序：本地先行、跨线程随后（确定性）。
    let local_notified = inner.local_subs.load(Ordering::Relaxed) > 0;
    if local_notified {
        let snapshot = Arc::clone(&lock(&inner.state));
        let (matched, poisoned, cause) = local_notify(inner.store_id, &*snapshot);
        // 跨线程 miss 留痕（读侧 local_missed_notifications）：闸>0 说明有线程
        // 持有本地订阅，本线程表却查无该 store——本拍 dispatch 到不了任何
        // 本地订阅（触发语义不变，只从「零触发零信号」变「miss 看得见」）。
        if matched == 0 {
            inner.local_missed.fetch_add(1, Ordering::Relaxed);
        }
        if poisoned > 0 {
            inner
                .subscriber_panics
                .fetch_add(poisoned, Ordering::Release);
            *lock(&inner.subscriber_panic_cause) = cause;
        }
    }
    if let Some(next) = notified_next {
        for slot in cache.slots.iter() {
            // 订阅回调 panic 归一：摘除该订阅者后继续——中毒 slot.notify
            // 永久再 panic，留表即「每次 dispatch 必 panic」的双重变砖。摘除自增
            // 版本，快照缓存下一次 dispatch 冷重建时收敛。
            // 缓存快照存活复查（issue#74）：快照持强 Arc，票据 Drop 后本次
            // dispatch 仍可能回调已注销槽——alive=false 即跳过（已开始的
            // 回调可完成：in-flight 语义，不承诺 quiescent 退订）。
            if !slot.alive.load(Ordering::Acquire) {
                continue;
            }
            let notified = catch_unwind(AssertUnwindSafe(|| {
                let mut notify = lock(&slot.notify);
                notify(next);
            }));
            if let Err(payload) = notified {
                // panic 摘除：先失活（幂等）再摘表 + 递增版本。
                slot.alive.store(false, Ordering::Release);
                let dead = Arc::downgrade(slot);
                lock(&inner.subs).retain(|w| !Weak::ptr_eq(w, &dead));
                inner.subs_version.fetch_add(1, Ordering::Release);
                inner.subscriber_panics.fetch_add(1, Ordering::Release);
                // 成因摘要留痕（回答「为什么」，计数只答「谁死了」）
                *lock(&inner.subscriber_panic_cause) = Some(panic_cause_summary(&*payload));
            }
        }
    }
    local_notified || notified_next.is_some()
}

// ── 本地订阅：线程本地登记机制在 runtime/local_subs.rs（关注面切缝）────────
// 排空环的通知段经 local_notify 触发本线程登记的回调；store 侧只持闸计数。

/// store 身份号：本地订阅 TLS 表的键。全局单调、进程内唯一。
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// panic payload 成因摘要：str/String 原文截断 200 字符；其余
/// 载荷类型不可静态提取（`Box<dyn Any>` 无 Debug 面），存占位说明。
fn panic_cause_summary(payload: &(dyn std::any::Any + Send)) -> String {
    let raw = payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string());
    if raw.chars().count() > 200 {
        format!("{}…", raw.chars().take(200).collect::<String>())
    } else {
        raw
    }
}

// ── 审计事件流发射面：runtime/audit_emit.rs（关注面切缝）────────────────────
// 发射点（值面 crate::audit）与 gate 快路径随排空环走，本文件只持调用点。

#[cfg(test)]
mod tests {
    use super::*;

    // ── 所有权分界：本 mod 为白盒回归监视区；产品面与新 API 测试归
    // tests/ 下各文件，互不越界。──

    enum Tick {
        Tick,
    }

    struct TickFeature;

    impl Reducer for TickFeature {
        type State = u32;
        type Action = Tick;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: Tick, _out: &mut EffectBuf<Tick, Infallible>) -> u32 {
            s + 1
        }
    }

    struct NopRuntime;

    impl EffectRuntime<Tick, Infallible> for NopRuntime {
        fn run(&mut self, _: Effect<Tick, Infallible>, _: &ActionSender<Tick>) {}
    }

    /// 死 Weak 就地回收：票据丢弃后下一次排空的通知收集阶段摘除，
    /// 订阅表不随订阅增删无界增长（修复前表长只增不减）。内部状态断言，须与
    /// 实现同模块。
    #[test]
    fn dead_subscription_slots_pruned_on_drain() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        let _keep = store.subscribe(|_| {});
        let dropped = store.subscribe(|_| {});
        drop(dropped);
        store.dispatch(Tick::Tick);
        assert_eq!(lock(&store.inner.subs).len(), 1);
    }

    /// 多线程竞争回归：自由竞争 dispatch +
    /// 不变量监视线程。原版在 tests/ 且 barrier 每轮同步，push 永远落在退出
    /// 窗口（放旗→复检）之外，修复前代码上也绿（green theater）。强化三件：
    /// ①挪入 src 暴露私有态；②去 barrier 让 push 可落进退出窗口；③监视线程
    /// 连续断言「无在途 dispatch ⇒ 队列空」。
    ///
    /// 判孤儿取持锁全程核查：监视线程握住 queue 锁的整个核查期间，任何在途
    /// dispatch 都无法完成排空或返回（pop/出口复检都要这把锁），因此
    /// `draining=false && 队列非空 && in_flight==0`（push 方先 +1 再入队、返回
    /// 前 -1）在修法代码中不可达——swap 失败有在任排空者出口复检兜底；修复前
    /// 出口 store(false) 无复检，孤儿可滞留，即被咬住。检出置旗标不 panic，
    /// 免毒锁噪声；终态由测试线程显式断言。
    #[test]
    fn concurrent_dispatch_never_orphans_actions() {
        const THREADS: usize = 8;
        const ITERS: usize = 10000;
        let store = Arc::new(SyncStore::new(TickFeature, NopRuntime));
        let inner = Arc::clone(&store.inner);
        let done = Arc::new(AtomicBool::new(false));
        let orphan = Arc::new(AtomicBool::new(false));
        let monitor = {
            let inner = Arc::clone(&inner);
            let done = Arc::clone(&done);
            let orphan = Arc::clone(&orphan);
            std::thread::spawn(move || {
                let mut iter = 0usize;
                while !done.load(Ordering::SeqCst) && !orphan.load(Ordering::SeqCst) {
                    let queue = lock(&inner.queue);
                    if !queue.is_empty()
                        && !inner.draining.load(Ordering::SeqCst)
                        && inner.in_flight.load(Ordering::SeqCst) == 0
                    {
                        orphan.store(true, Ordering::SeqCst);
                    }
                    drop(queue);
                    iter += 1;
                    if iter.is_multiple_of(64) {
                        std::thread::yield_now(); // 防独占：周期性让出，其余迭代贴身采样
                    }
                }
            })
        };
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let store = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for _ in 0..ITERS {
                    store.dispatch(Tick::Tick);
                }
            }));
        }
        for h in handles {
            h.join().expect("dispatch 线程不得 panic");
        }
        // 静止态终检（确定性判据）：全部 dispatch 已返回，滞留孤儿必在此现形
        assert!(
            !orphan.load(Ordering::SeqCst),
            "孤儿 action：无在途 dispatch 且排空旗已放，队列仍滞留 action（监视线程持锁核实）"
        );
        assert_eq!(inner.in_flight.load(Ordering::SeqCst), 0);
        assert!(
            !inner.draining.load(Ordering::SeqCst),
            "静止态排空旗必须已放"
        );
        assert!(
            lock(&inner.queue).is_empty(),
            "静止态队列必须空：滞留即孤儿"
        );
        // 全部 dispatch 返回 ⇒ 各自的 action 均已终局（滞留即少计）
        assert_eq!(*store.snapshot(), (THREADS * ITERS) as u32);
        done.store(true, Ordering::SeqCst);
        monitor.join().expect("监视线程不得 panic");
    }

    // ── 停机闸（close 语义）──────────

    /// close 后 dispatch 恒 no-op——状态不再前进；dispatch 签名不变，
    /// 无 Err 感染调用面。
    #[test]
    fn close_makes_dispatch_noop() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
    }

    /// 闸在共享 push 路径——闭门后 dispatch 返回 false 且计数，
    /// bool 契约不撒谎（不报「已入队」实为弃）。
    #[test]
    fn closed_sender_returns_false_and_counts() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        let sender = store.sender();
        assert!(sender.dispatch(Tick::Tick), "闭门前 dispatch 照常入队");
        store.close();
        assert!(
            !sender.dispatch(Tick::Tick),
            "闭门后 dispatch 必须显式 false"
        );
        assert_eq!(store.rejected_after_close(), 1);
    }

    /// 闭门计数 fail-visible，dispatch 与 send 两条 push 路径的拒绝都计入。
    #[test]
    fn rejection_counter_covers_both_push_paths() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        let sender = store.sender();
        store.close();
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        let _ = sender.dispatch(Tick::Tick);
        assert_eq!(store.rejected_after_close(), 5);
        assert_eq!(*store.snapshot(), 0, "被拒 action 不得推进状态");
    }

    /// close() 内同步排空至 idle——关门时点的存量队列被执行而非
    /// 随 drop 静默弃队（与 fx 停机序「fx.close→await idle→flush→drop」对齐）。
    /// 白盒注入：单线程下队列仅在排空中非空，直推 queue 复现「存量未排」瞬态。
    #[test]
    fn close_drains_stock_queue_to_idle() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        for _ in 0..3 {
            // 白盒注入：直推队列须带来源标签（回灌深度预算的入队纪律）
            // 白盒注入：直推队列须带完整记录（序号 + 账本 + 来源标签——回灌深度
            // 预算与回执结算的入队纪律）
            let ledger = Ledger::new(false);
            lock(&store.inner.queue).push_back(ActionRecord {
                seq: store.inner.next_seq.fetch_add(1, Ordering::SeqCst),
                action: Tick::Tick,
                origin: ActionOrigin::External,
                ledger,
                root: true,
            });
        }
        store.close();
        assert_eq!(*store.snapshot(), 3, "存量 action 必须被 close 排空执行");
        assert!(lock(&store.inner.queue).is_empty(), "close 返回即 idle");
        assert_eq!(store.rejected_after_close(), 0, "存量执行不是拒绝");
    }

    /// close 幂等——重复关门只多一次空排空，状态与计数不受扰。
    #[test]
    fn close_is_idempotent() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.close();
        store.close();
        assert!(store.is_closed());
        store.dispatch(Tick::Tick);
        assert_eq!(*store.snapshot(), 0);
        assert_eq!(store.rejected_after_close(), 1);
    }

    // ── make_mut CoW 快照落位 ────────

    /// CoW 分支语义（快照一致性自证）：外部持有者存续期间旧快照冻结——持有者
    /// 永见其取得时点的自洽状态，后续 action 的落位不得波及（快照即模型）。
    #[test]
    fn make_mut_cow_freezes_held_snapshot() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.dispatch(Tick::Tick);
        let held = store.snapshot();
        store.dispatch(Tick::Tick);
        assert_eq!(*held, 1, "外部持有者必须永见旧快照（CoW 冻结）");
        assert_eq!(*store.snapshot(), 2, "新快照照常前进");
    }

    /// 原地分支命中（稳态零分配自证）：无外部持有者时连续 dispatch 复用同一
    /// 分配（指针不变）且稳态强计数恒 1——快照分配税消除的直接断言。
    #[test]
    fn make_mut_inplace_steady_state_reuses_allocation() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        store.dispatch(Tick::Tick);
        let p1 = Arc::as_ptr(&*lock(&store.inner.state));
        store.dispatch(Tick::Tick);
        let p2 = Arc::as_ptr(&*lock(&store.inner.state));
        assert_eq!(
            p1, p2,
            "稳态无外部持有者：make_mut 必须原地写入，禁每 action 换分配"
        );
        assert_eq!(
            Arc::strong_count(&*lock(&store.inner.state)),
            1,
            "稳态强计数必须回落 1"
        );
    }

    /// 旧 S 析构必须发生在 state 锁外（锁消融形态回归）：
    /// ——S:Drop panic 不得毒化 state mutex（毒锁 = store 永久变砖）。
    #[derive(Clone)]
    struct PanicOnDrop {
        armed: Arc<AtomicBool>,
    }
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            // 首次析构（=落位时被换出的旧快照）panic；其后实例静默，保测试
            // 自身与 store drop 不被噪声引爆
            if !self.armed.swap(true, Ordering::SeqCst) {
                panic!("S:Drop drill：旧快照析构必须发生在 state 锁外（毒锁契约）");
            }
        }
    }

    struct PanicDropFeature {
        armed: Arc<AtomicBool>,
    }
    impl Reducer for PanicDropFeature {
        type State = PanicOnDrop;
        type Action = Tick;
        type Op = Infallible;
        fn initial(&self) -> PanicOnDrop {
            PanicOnDrop {
                armed: Arc::clone(&self.armed),
            }
        }
        fn reduce(
            &self,
            s: &PanicOnDrop,
            _: Tick,
            _out: &mut EffectBuf<Tick, Infallible>,
        ) -> PanicOnDrop {
            PanicOnDrop {
                armed: Arc::clone(&s.armed),
            }
        }
    }

    #[test]
    fn snapshot_replacement_drops_old_value_outside_state_lock() {
        let store = SyncStore::new(
            PanicDropFeature {
                armed: Arc::new(AtomicBool::new(false)),
            },
            NopRuntime,
        );
        let first = std::panic::catch_unwind(AssertUnwindSafe(|| store.dispatch(Tick::Tick)));
        assert!(
            first.is_err(),
            "旧快照析构 panic 必须如实传播（fail-visible）"
        );
        // 析构在锁外 ⇒ state mutex 未中毒 ⇒ store 继续可用（毒锁形态下此处必 panic）
        store.dispatch(Tick::Tick);
        store.dispatch(Tick::Tick);
    }

    // ── 残留弃队留痕 ────────────────────

    enum DropAction {
        Spin,
    }

    struct DropSpinFeature;

    impl Reducer for DropSpinFeature {
        type State = u32;
        type Action = DropAction;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(
            &self,
            s: &u32,
            _: DropAction,
            out: &mut EffectBuf<DropAction, Infallible>,
        ) -> u32 {
            out.push(Effect::Emit(DropAction::Spin));
            *s
        }
    }

    struct DropNopFx;

    impl EffectRuntime<DropAction, Infallible> for DropNopFx {
        fn run(&mut self, _: Effect<DropAction, Infallible>, _: &ActionSender<DropAction>) {}
    }

    /// 预算止步残留 × 直接 drop：弃队必须留痕（并入预算计数），禁静默。
    /// 读窗 = inner Arc 存续期（与 rejected_after_close 的停机窗同类语义）——
    /// 白盒持 inner 引用跨越 Store drop 验证计数上浮。
    #[test]
    fn budget_residual_discard_is_counted_on_store_drop() {
        let store = Arc::new(SyncStore::new(DropSpinFeature, DropNopFx));
        store.dispatch(DropAction::Spin);
        assert_eq!(store.feedback_budget_exceeded(), 1, "前置：预算已止步");
        assert_eq!(store.pending_actions(), 1, "前置：残留尚在队列");
        let inner = Arc::clone(&store.inner);
        drop(store);
        assert_eq!(
            inner.feedback_exceeded.load(Ordering::SeqCst),
            2,
            "drop 弃队必须留痕（1 预算触发 + 1 弃队），禁静默"
        );
    }

    // ── 可观测读侧 ─────────────────────────────────────────────────────

    /// 队列深公开读侧：状态可查。同步排空语义下 dispatch
    /// 返回即 idle（恒 0），非 0 值只在排空循环内可见：经订阅回调重入 dispatch
    /// 验证「入队未排空」的窗口深度可读。
    #[test]
    fn pending_actions_visible_from_reentrant_dispatch() {
        let store = Arc::new(SyncStore::new(TickFeature, NopRuntime));
        let observed = Arc::new(AtomicUsize::new(0));
        let observer = Arc::clone(&observed);
        let reentrant = Arc::clone(&store);
        let fired = Arc::new(AtomicBool::new(false));
        let guard = Arc::clone(&fired);
        let _keep = store.subscribe(move |_| {
            // 只在首个通知里重入一次，否则重入 action 的通知再触发重入 = 无限环
            if !guard.swap(true, Ordering::SeqCst) {
                reentrant.dispatch(Tick::Tick); // 本任排空循环持旗，重入只入队
                observer.store(reentrant.pending_actions(), Ordering::SeqCst);
            }
        });
        assert_eq!(store.pending_actions(), 0, "静止态队列深为 0");
        store.dispatch(Tick::Tick);
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "回调内重入入队后队列深必须可见为 1"
        );
        assert_eq!(
            store.pending_actions(),
            0,
            "dispatch 返回后队列深为 0（本例单动作 ⇒ 完成边界与排空至 idle 同形）"
        );
        assert_eq!(*store.snapshot(), 2, "重入 action 被同一排空循环执行");
    }

    /// 订阅回调 panic 摘除的成因留痕计数（摘除留痕+显式
    /// 计数读侧可见）：摘除只剩 subscriber_count 下跌可查时无成因，
    /// 本计数补齐成因面。摘除一次计 1；已摘除者不再重复计入。
    #[test]
    fn subscriber_panic_removal_is_counted() {
        let store = SyncStore::new(TickFeature, NopRuntime);
        assert_eq!(store.subscriber_panics(), 0);
        let _poison = store.subscribe(|_| panic!("subscriber bug"));
        let _healthy = store.subscribe(|_| {});
        store.dispatch(Tick::Tick); // 毒订阅者 panic → 摘除 + 留痕
        assert_eq!(store.subscriber_panics(), 1, "panic 摘除必须显式留痕");
        assert_eq!(store.subscriber_count(), 1, "毒订阅者已摘除");
        store.dispatch(Tick::Tick); // 健康者照常通知
        assert_eq!(store.subscriber_panics(), 1, "已摘除者不得重复计数");
    }

    /// 两段闸（白盒编排用）：进入点 + 放行点（确定性握手，禁 sleep 竞速）。
    struct PoolGate {
        entered: (Mutex<bool>, Condvar),
        release: (Mutex<bool>, Condvar),
    }

    impl PoolGate {
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

    enum PoolAct {
        Hold,
        Root,
        Child,
    }

    struct PoolFixture {
        hold: Arc<PoolGate>,
        child: Arc<PoolGate>,
    }

    impl Reducer for PoolFixture {
        type State = u32;
        type Action = PoolAct;
        type Op = Infallible;

        fn initial(&self) -> u32 {
            0
        }

        fn reduce(&self, s: &u32, a: PoolAct, out: &mut EffectBuf<PoolAct, Infallible>) -> u32 {
            match a {
                PoolAct::Hold => {
                    self.hold.mark_entered();
                    self.hold.wait_release();
                    s + 1
                }
                PoolAct::Root => {
                    out.push(Effect::Emit(PoolAct::Child));
                    s + 1
                }
                PoolAct::Child => {
                    self.child.mark_entered();
                    self.child.wait_release();
                    s + 1
                }
            }
        }
    }

    fn pooled(store: &SyncStore<u32, PoolAct, Infallible>) -> Vec<Arc<Ledger>> {
        lock(&store.inner.ledger_pool).clone()
    }

    /// issue#84（①）：非等待型根的账本在**终局发布前**绝不进复用池——最后一个
    /// 后代结算 ⇒ 终局发布 ⇒ **恰好一次**归还，且池内账本必是终局态（`reset`
    /// 的不变式断言兜底：活账本被复位即在 debug 构建直接咬住）。
    #[test]
    fn non_waiting_root_ledger_pooled_only_after_terminal() {
        let hold = PoolGate::new();
        let child = PoolGate::new();
        let store = Arc::new(SyncStore::new(
            PoolFixture {
                hold: Arc::clone(&hold),
                child: Arc::clone(&child),
            },
            skeleton::NoFx,
        ));

        // A：等待型根持有排空权，并闸在 reducer 内（其账本由 A 在消费终局后
        // 归还 ⇒ 断言窗内池只可能来自 Root）。
        let store_a = Arc::clone(&store);
        let handle_a = std::thread::spawn(move || store_a.dispatch(PoolAct::Hold));
        hold.wait_entered();

        // 非等待型根：只入队（其后代在途期间正是旧实现入池的窗口）。
        assert_eq!(
            store.try_dispatch(PoolAct::Root),
            AdmissionOutcome::Accepted,
            "前置：非等待型根必须被接受"
        );
        hold.open();
        child.wait_entered();

        // Root 已结算（root_done + 1 个在途后代）⇒ 不得入池。
        assert!(
            pooled(&store).is_empty(),
            "终局发布前账本不得进复用池（在途后代会把结算记到被复用的账本上）"
        );

        child.open();
        handle_a.join().expect("排空者不得 panic");

        // 每个账本恰好一次入池：Hold 的根账本 + Root 的根账本 = 2（重复 push 会更多）。
        let ledgers = pooled(&store);
        assert_eq!(ledgers.len(), 2, "每个终局的非等待型根账本恰好一次归还");
        assert!(
            ledgers.iter().all(|l| l.terminal_published()),
            "池内账本必须已发布终局"
        );
    }

    /// issue#84（④ unwind）：排空展开路径不得把非终局账本入池——当前记录在
    /// 解释器 panic 时仍有排队后代（`outstanding == 1`），摘队 sweep 结算后才
    /// 终局；池内只能有终局账本。
    #[test]
    fn unwind_path_pools_only_terminal_ledgers() {
        enum UnwindAct {
            Root,
            Child,
        }

        enum UnwindOp {
            Tick,
        }

        struct PanicFx;

        impl EffectRuntime<UnwindAct, UnwindOp> for PanicFx {
            fn run(&mut self, effect: Effect<UnwindAct, UnwindOp>, _: &ActionSender<UnwindAct>) {
                if let Effect::Run { .. } = effect {
                    panic!("interpreter detonation (unwind path)");
                }
            }
        }

        struct UnwindFixture;

        impl Reducer for UnwindFixture {
            type State = u32;
            type Action = UnwindAct;
            type Op = UnwindOp;

            fn initial(&self) -> u32 {
                0
            }

            fn reduce(
                &self,
                s: &u32,
                a: UnwindAct,
                out: &mut EffectBuf<UnwindAct, UnwindOp>,
            ) -> u32 {
                match a {
                    UnwindAct::Root => {
                        // 先产出回灌后代（入队后 outstanding=1），再让解释器 panic。
                        out.push(Effect::Emit(UnwindAct::Child));
                        out.push(Effect::Run {
                            id: EffectId::new(1),
                            op: UnwindOp::Tick,
                        });
                        s + 1
                    }
                    UnwindAct::Child => s + 1,
                }
            }
        }

        let store = SyncStore::new(UnwindFixture, PanicFx);
        let outcome = catch_unwind(AssertUnwindSafe(|| store.try_dispatch(UnwindAct::Root)));
        assert!(outcome.is_err(), "解释器 panic 必须照常上浮（禁吞）");

        let ledgers = lock(&store.inner.ledger_pool).clone();
        assert!(!ledgers.is_empty(), "终局账本必须被归还（不得静默漏归还）");
        assert!(
            ledgers.iter().all(|l| l.terminal_published()),
            "展开路径不得把非终局账本放进复用池"
        );
    }

    // ── 容量 setter 与 population 的共锁线性化（issue#95）───────────────
    //
    // 三个 setter（ingress / deferred）与各自 population 共用一把锁；本区用
    // `CapacitySettle` 把触发线程停靠在「已按旧 cap 裁决、尚未提交」的临界区里，
    // 确定性构造「setter 想插在同一准入/停放的裁决与提交之间」的交错——
    // 合法结果只有两种：触发方先得锁（setter 被挡住）或 setter 先得锁（触发方
    // 按新 cap 判定）。禁 sleep 竞速。

    /// setter 的反向有界等待上限：正确实现下 setter 必被锁挡住 ⇒ 必然超时。
    const SETTER_BLOCK_LIMIT: Duration = Duration::from_millis(300);

    enum CapAct {
        /// 占住排空权（在 reduce 内阻塞）——使后续准入记录留在队列里。
        Hold,
        /// 普通外部准入。
        Inc,
        /// 无条件自指回灌：撞回灌预算即在 deferred 队列停一条。
        Loop,
    }

    struct CapFixture {
        hold: Arc<PoolGate>,
    }

    impl Reducer for CapFixture {
        type State = u32;
        type Action = CapAct;
        type Op = Infallible;

        fn initial(&self) -> u32 {
            0
        }

        fn reduce(&self, s: &u32, a: CapAct, out: &mut EffectBuf<CapAct, Infallible>) -> u32 {
            match a {
                CapAct::Hold => {
                    self.hold.mark_entered();
                    self.hold.wait_release();
                }
                CapAct::Inc => {}
                CapAct::Loop => out.push(Effect::Emit(CapAct::Loop)),
            }
            s + 1
        }
    }

    /// ingress 容量 setter 与准入共锁（issue#95）：准入已按旧 cap 裁决（仍持
    /// `queue` 锁）时 setter **不可能返回**；其后所有准入按新 cap 判定，且旧 cap
    /// 接纳的记录不被回溯删除。
    ///
    /// 变异判别：若 setter 退回「锁外原子 store」，它会在准入停靠期间立即返回
    /// ⇒ 本条红（本测试即该改动的变异锚）。
    #[test]
    fn ingress_capacity_setter_cannot_linearize_inside_admission() {
        let hold = PoolGate::new();
        let store = Arc::new(SyncStore::new(
            CapFixture {
                hold: Arc::clone(&hold),
            },
            skeleton::NoFx,
        ));
        // 占住排空权：让后续准入的记录留在队列里（容量闸才有可观测的队列深）。
        let store_hold = Arc::clone(&store);
        let holder = std::thread::spawn(move || store_hold.dispatch(CapAct::Hold));
        hold.wait_entered();

        store.inner.ingress_gate.arm();
        let store_a = Arc::clone(&store);
        let admission = std::thread::spawn(move || store_a.try_dispatch(CapAct::Inc));
        assert!(
            store
                .inner
                .ingress_gate
                .wait_entered(Duration::from_secs(10)),
            "准入必须停靠在「已读 cap、尚未 push」的 queue 锁窗口里"
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let store_b = Arc::clone(&store);
        let setter = std::thread::spawn(move || {
            store_b.set_capacity(1);
            let _ = done_tx.send(());
        });
        // 负向断言（OS 时钟有界等待）：setter 不得在准入尚未提交时返回。
        let setter_returned_early = done_rx.recv_timeout(SETTER_BLOCK_LIMIT).is_ok();
        // 先放行再断言：断言失败时也不得把停靠线程留在锁里（否则是挂死而非失败）。
        store.inner.ingress_gate.release();
        assert_eq!(
            admission.join().expect("准入线程不得 panic"),
            AdmissionOutcome::Accepted,
            "停靠中的准入按旧 cap（0 = 不限）接纳"
        );
        setter.join().expect("setter 线程不得 panic");
        assert!(
            !setter_returned_early,
            "setter 在准入已按旧 cap 裁决、尚未 push 时返回 ⇒ 容量发布未与准入共锁（issue#95）"
        );

        assert_eq!(
            store.pending_actions(),
            1,
            "旧 cap 接纳的那一条必须仍在队（缩容不回溯删除已接纳工作）"
        );
        assert_eq!(
            store.try_dispatch(CapAct::Inc),
            AdmissionOutcome::Full,
            "setter 之后的新准入按新 cap=1 拒绝"
        );
        assert_eq!(store.admission_full_count(), 1, "拒绝必须留痕");
        assert_eq!(store.pending_actions(), 1, "拒绝不得部分插入");

        hold.open();
        holder.join().expect("排空者不得 panic");
        assert_eq!(*store.snapshot(), 2, "已接纳的 action 照常被 reduce");
    }

    /// ingress 容量：缩容只挡后续准入；扩容与 `0`（不限）对后续准入立即生效。
    #[test]
    fn ingress_capacity_expand_and_unbounded_apply_immediately() {
        let hold = PoolGate::new();
        let store = Arc::new(SyncStore::new(
            CapFixture {
                hold: Arc::clone(&hold),
            },
            skeleton::NoFx,
        ));
        let store_hold = Arc::clone(&store);
        let holder = std::thread::spawn(move || store_hold.dispatch(CapAct::Hold));
        hold.wait_entered();

        store.set_capacity(1);
        assert_eq!(
            store.try_dispatch(CapAct::Inc),
            AdmissionOutcome::Accepted,
            "空队按 cap=1 接纳"
        );
        assert_eq!(
            store.try_dispatch(CapAct::Inc),
            AdmissionOutcome::Full,
            "队列已到 cap=1 ⇒ 拒绝"
        );

        store.set_capacity(4);
        assert_eq!(
            store.try_dispatch(CapAct::Inc),
            AdmissionOutcome::Accepted,
            "扩容立即生效（不受此前拒绝影响）"
        );
        assert_eq!(
            store.try_dispatch(CapAct::Inc),
            AdmissionOutcome::Accepted,
            "扩容后队列深 2 < 4"
        );

        store.set_capacity(0);
        for _ in 0..3 {
            assert_eq!(
                store.try_dispatch(CapAct::Inc),
                AdmissionOutcome::Accepted,
                "0 = 不限：后续准入一律立即生效"
            );
        }
        assert_eq!(store.admission_full_count(), 1, "只有 cap=1 那次被拒");
        assert_eq!(store.pending_actions(), 6, "被拒记录不占位");

        hold.open();
        holder.join().expect("排空者不得 panic");
        assert_eq!(*store.snapshot(), 7);
    }

    /// deferred 容量 setter 与停放写入共锁（issue#95）：停放已按旧 cap 裁决
    /// （仍持 `deferred_queue` 锁）时 setter 不可能返回；其后停放按新 cap 判定，
    /// 既有停放记录不被回溯删除，拒收计数准确。
    #[test]
    fn deferred_capacity_setter_cannot_linearize_inside_park() {
        let store = Arc::new(SyncStore::new(
            CapFixture {
                hold: PoolGate::new(),
            },
            skeleton::NoFx,
        ));
        store.set_deferred_capacity(0); // 旧 cap = 不限

        store.inner.deferred_gate.arm();
        let store_a = Arc::clone(&store);
        let parker = std::thread::spawn(move || store_a.try_dispatch(CapAct::Loop));
        assert!(
            store
                .inner
                .deferred_gate
                .wait_entered(Duration::from_secs(10)),
            "停放必须停靠在「已读 cap、尚未 retain/reject」的 deferred_queue 锁窗口里"
        );

        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let store_b = Arc::clone(&store);
        let setter = std::thread::spawn(move || {
            store_b.set_deferred_capacity(1);
            let _ = done_tx.send(());
        });
        let setter_returned_early = done_rx.recv_timeout(SETTER_BLOCK_LIMIT).is_ok();
        store.inner.deferred_gate.release();
        assert_eq!(
            parker.join().expect("停放线程不得 panic"),
            AdmissionOutcome::Accepted,
            "停放只是内部续接，准入面照常 Accepted"
        );
        setter.join().expect("setter 线程不得 panic");
        assert!(
            !setter_returned_early,
            "setter 在停放已按旧 cap 裁决、尚未提交时返回 ⇒ 容量发布未与停放共锁（issue#95）"
        );

        assert_eq!(
            store.deferred_actions(),
            1,
            "停放先得锁 ⇒ 按旧 cap（0 = 不限）留存，且不被回溯删除"
        );
        assert_eq!(store.deferred_capacity_rejected(), 0, "旧 cap 下无拒收");

        assert_eq!(
            store.try_dispatch(CapAct::Loop),
            AdmissionOutcome::Accepted,
            "外部准入容量不受影响"
        );
        assert_eq!(store.deferred_actions(), 1, "缩容不回溯删除既有停放记录");
        assert_eq!(
            store.deferred_capacity_rejected(),
            1,
            "新停放按新 cap=1 拒收且计数准确"
        );
    }

    /// deferred 容量：扩容与 `0`（不限）对后续停放立即生效。
    #[test]
    fn deferred_capacity_expand_and_unbounded_apply_immediately() {
        let store = SyncStore::new(
            CapFixture {
                hold: PoolGate::new(),
            },
            skeleton::NoFx,
        );
        store.set_deferred_capacity(1);
        store.dispatch(CapAct::Loop);
        assert_eq!(store.deferred_actions(), 1, "cap=1 接纳首条停放");
        store.dispatch(CapAct::Loop);
        assert_eq!(store.deferred_actions(), 1, "cap=1 拒绝第二条停放");
        assert_eq!(store.deferred_capacity_rejected(), 1);

        store.set_deferred_capacity(2);
        store.dispatch(CapAct::Loop);
        assert_eq!(store.deferred_actions(), 2, "扩容立即生效");
        assert_eq!(store.deferred_capacity_rejected(), 1, "扩容后不再拒收");

        store.set_deferred_capacity(0);
        store.dispatch(CapAct::Loop);
        assert_eq!(store.deferred_actions(), 3, "0 = 不限：立即生效");
        assert_eq!(store.deferred_capacity_rejected(), 1);
    }

    /// 两个 setter 与 `close` / `resume_deferred` / 准入并发：锁序保持
    /// （`queue → deferred_queue` 未被破坏）、无死锁、无丢无重。
    ///
    /// 计数口径：本次只跑 `Inc`（无停放记录）⇒ 每条 Accepted 恰好一次 reduce，
    /// 终态 `snapshot == accepted` 即「无丢无重」。容量判定的精确交错已由上面
    /// 三条确定性用例覆盖，本用例只咬锁序与存活。
    #[test]
    fn capacity_setters_concurrent_with_close_and_resume_preserve_records() {
        let store = Arc::new(SyncStore::new(
            CapFixture {
                hold: PoolGate::new(),
            },
            skeleton::NoFx,
        ));

        let admit = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                let mut accepted = 0u32;
                for _ in 0..200 {
                    if store.try_dispatch(CapAct::Inc) == AdmissionOutcome::Accepted {
                        accepted += 1;
                    }
                }
                accepted
            })
        };
        let setter = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for i in 0..200usize {
                    store.set_capacity(i % 5);
                    store.set_deferred_capacity(i % 3);
                }
            })
        };
        let resumer = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || {
                for _ in 0..200 {
                    let _ = store.resume_deferred(1).resumed;
                }
            })
        };
        let closer = {
            let store = Arc::clone(&store);
            std::thread::spawn(move || store.close())
        };

        let accepted = admit.join().expect("准入线程不得 panic");
        setter.join().expect("setter 线程不得 panic");
        resumer.join().expect("恢复线程不得 panic");
        closer.join().expect("闭门线程不得 panic");

        assert!(store.is_closed(), "close 必须生效");
        assert_eq!(
            *store.snapshot(),
            accepted,
            "每条 Accepted 恰好一次 reduce（无丢无重）"
        );
        assert_eq!(store.pending_actions(), 0, "闭门排空后主队列为空");
        assert_eq!(store.deferred_actions(), 0, "本次无停放记录");
    }
}
