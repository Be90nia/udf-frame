//! 池化管理组合件：公共理念逻辑写一次（校验/水位线/审计/计数），双池特化面留白。
//!
//! [`PoolBase`] 是**组合件不是池**：不碰对象、不管槽位存储、不跑调度循环——
//! 它承载任何池都要一遍的管理面账本与纪律（armor #26：借出句柄 RAII 归还 /
//! 池深+借出数可观测 fail-visible / 跨线程竞争面单权威）。内存池/线程池各自
//! 持有一个 `PoolBase` 字段，特化面（借还路径/worker 调度）回调本件记账 +
//! 发射审计；Wave2 双池实现单背性能锚，本件只守契约面纪律。
//!
//! # 性能纪律（2026-10-16 拍板的契约面落位）
//!
//! - **校验与状态变更分离**：入口校验是纯函数（`validate`，`pub(crate)`——
//!   `ObjectPool` 同源复用），独立于任何锁——Wave2 换无锁/分片账本时校验面原样保留。
//! - **读侧 O(1) 零分配**：[`PoolBase::metrics`] 单次取锁 + `Copy` 读数；
//!   错误面 [`PoolResizeError`] 为 `Copy`（拒绝路径不构造 `String`/`Vec`）。
//! - **热路径禁格式化**：审计发射 = 一次 gate load 快路径（未订阅零分配零锁）
//!   + 容量环入账 + seq 单调，全程无格式化（`Display` 只在拒绝路径触发）。
//! - **锁策略**：单权威 `Mutex`（排空权同款）起步；账本锁与审计锁分离，回调
//!   锁外调用——接口形态（方法面不变）不堵无锁演进。锁获取收口 `lock`
//!   helper（fail-fast，同 `runtime::lock` 纪律）。
//!
//! feature `pool`（纯 std + 契约词汇，零三方增量）；由 lib.rs 以
//! `#[cfg(feature = "pool")] pub mod pool;` 接线。

use std::borrow::Borrow;
use std::cell::Cell;
use std::collections::VecDeque;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;

use skeleton::pool::{
    PoolAuditEvent, PoolAuditKind, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
};

/// 审计环默认容量：实现方 [`skeleton::pool::PoolManaged::subscribe_audit`]
/// 委托用（`PoolBase::subscribe_audit` 的订阅面可显式给 cap）。
pub const AUDIT_RING_DEFAULT_CAP: usize = 1024;

/// 入口校验（事前校验纪律，纯函数零锁零分配）：max==0 / idle_cap==0 /
/// idle_cap>max 三类恒不合法配置。构造期（[`PoolBase::new`]）同规则 panic
/// fail-fast，运行时（[`PoolBase::resize`]）显式 Err——同一套规则两档出口；
/// `pub(crate)` 后 [`crate::objpool::ObjectPool`] 同源复用本函数（公共规则
/// 写一次，禁复制第二套）。
pub(crate) fn validate(max: usize, idle_cap: usize) -> Result<(), PoolResizeError> {
    if max == 0 {
        return Err(PoolResizeError::ZeroMax);
    }
    if idle_cap == 0 {
        return Err(PoolResizeError::IdleCapZero);
    }
    if idle_cap > max {
        return Err(PoolResizeError::IdleCapExceedsMax { idle_cap, max });
    }
    Ok(())
}

/// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（同 runtime::lock
/// 纪律）。收口一处，调用点只见加锁。
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .expect("pool lock poisoned (prior holder panicked)")
}

/// 核心账本（单权威锁内）：边界 + 计数。seq/代次走独立原子（发射路径与账本
/// 锁分离——回调锁外可重入一切读入口，持账本锁发射即死锁）。
struct PoolCore {
    max: usize,
    idle_cap: usize,
    idle: usize,
    borrowed: usize,
    dropped_overflow: u64,
}

/// 审计回调闭包（模块内别名收口 type_complexity，同 `audit::AuditCallback` 先例）。
type AuditCallback = Box<dyn FnMut(&PoolAuditEvent) + Send>;

/// 审计槽（订阅态的单槽内容）：容量环 + 实时回调 + 订阅代次（形态同
/// `audit::AuditSink`）。
struct AuditSlot {
    ring: VecDeque<PoolAuditEvent>,
    cap: usize,
    gen: u64,
    notify: Option<AuditCallback>,
}

/// 池域审计环（`PoolBase` 与 [`crate::objpool::ObjectPool`] 共用的抽取件）：
/// gate 快路径 + 订阅槽（容量环 + 实时回调 + 代次）+ seq 单调 + 回调 panic
/// 摘除计数 + TLS 重入封死。公共逻辑写一次——两池各持一个 sink 字段，各自
/// 呈现自己的公开面（[`PoolBase`] 公开 API 与行为零变化）。
pub(crate) struct PoolAuditSink {
    gate: Arc<AtomicBool>,
    audit: Arc<Mutex<Option<AuditSlot>>>,
    seq: AtomicU64,
    gen: AtomicU64,
    audit_callback_panics: AtomicU64,
}

impl PoolAuditSink {
    /// 构造空环（未订阅态：gate 落闸，发射 = 一次 load 即返）。
    pub(crate) fn new() -> Self {
        Self {
            gate: Arc::new(AtomicBool::new(false)),
            audit: Arc::new(Mutex::new(None)),
            seq: AtomicU64::new(0),
            gen: AtomicU64::new(0),
            audit_callback_panics: AtomicU64::new(0),
        }
    }

    /// 开审计流：落闸 + 建槽（容量环 `cap` 钳到 ≥1——恒保最新一件）。再次订阅
    /// = 替换现役槽，被替换票据凭代次对账 inert（Drop 不误杀新流）。
    pub(crate) fn subscribe_audit(
        &self,
        cap: usize,
        f: impl FnMut(&PoolAuditEvent) + Send + 'static,
    ) -> PoolAuditTicket {
        let gen = self.gen.fetch_add(1, Ordering::Relaxed) + 1;
        {
            let mut guard = lock(&self.audit);
            *guard = Some(AuditSlot {
                ring: VecDeque::new(),
                cap: cap.max(1),
                gen,
                notify: Some(Box::new(f)),
            });
        }
        self.gate.store(true, Ordering::Release);
        let gate = Arc::clone(&self.gate);
        let audit = Arc::clone(&self.audit);
        PoolAuditTicket::new(move || {
            let mut guard = lock(&audit);
            // 代次对账在锁内完成：现役票据才落闸清槽；已被后订替换的旧票据禁动。
            if guard.as_ref().is_some_and(|slot| slot.gen == gen) {
                *guard = None;
                drop(guard);
                gate.store(false, Ordering::Release);
            }
        })
    }

    /// 取走环内事件（事后取证读侧；环随取随清——实时回调是持续观测面，本
    /// 方法是批量对账面，形态同 `SyncStore::take_events`）。
    pub(crate) fn take_events(&self) -> Vec<PoolAuditEvent> {
        match lock(&self.audit).as_mut() {
            Some(slot) => slot.ring.drain(..).collect(),
            None => Vec::new(),
        }
    }

    /// 审计回调 panic 摘除计数（fail-visible 留痕：回调 panic 后该回调被摘除、
    /// 环照录、池行为不变——观测面禁改变被观测者，摘除必留痕禁静默）。
    pub(crate) fn audit_callback_panics(&self) -> u64 {
        self.audit_callback_panics.load(Ordering::Relaxed)
    }

    /// 审计发射唯一收口：未订阅 = 一次 gate load 即返（热路径零分配零锁）；
    /// 订阅态 = seq 单调入账（容量环溢出丢最旧）+ **锁外**实时回调（回调可
    /// 重入本池一切入口，持锁即死锁）。热路径禁格式化。
    pub(crate) fn emit(&self, kind: PoolAuditKind) {
        if !self.gate.load(Ordering::Acquire) {
            return;
        }
        let event = PoolAuditEvent {
            seq: self.seq.fetch_add(1, Ordering::Relaxed) + 1,
            kind,
        };
        let mut notify = {
            let mut guard = lock(&self.audit);
            match guard.as_mut() {
                Some(slot) => {
                    if slot.ring.len() >= slot.cap {
                        slot.ring.pop_front();
                    }
                    slot.ring.push_back(event);
                    slot.notify.take()
                }
                None => return,
            }
        };
        if let Some(f) = notify.as_mut() {
            if !EMIT_CB_IN_FLIGHT.with(Cell::get) {
                EMIT_CB_IN_FLIGHT.with(|cell| cell.set(true));
                match catch_unwind(AssertUnwindSafe(|| f(&event))) {
                    Ok(()) => {}
                    // 回调 panic：fail-closed 摘除（环照录、池行为不变——观测面
                    // 禁改变被观测者），计数留痕（禁静默摘除）。
                    Err(_) => {
                        self.audit_callback_panics.fetch_add(1, Ordering::Relaxed);
                        notify = None;
                    }
                }
                EMIT_CB_IN_FLIGHT.with(|cell| cell.set(false));
            }
        }
        if let Some(f) = notify {
            let mut guard = lock(&self.audit);
            if let Some(slot) = guard.as_mut() {
                // 归还窗口内订阅被替换则宁弃旧回调，禁覆盖新槽。
                if slot.notify.is_none() {
                    slot.notify = Some(f);
                }
            }
        }
    }
}

thread_local! {
    /// 审计回调在途标记：回调内**同线程任一池**的 emit 只录环不再进回调——封死
    /// 「回调→调参→回调」自指递归（栈爆），与骨架 audit 纪律同款。**本标记为
    /// `PoolBase` 与 `ObjectPool` 两池共用**（`PoolAuditSink` 是同一抽取件）：
    /// 在 A 池回调体内触发的 B 池 emit 同样只录环。
    static EMIT_CB_IN_FLIGHT: Cell<bool> = const { Cell::new(false) };
}

/// 池化管理组合件（公共理念逻辑写一次）：边界账本 + 水位线读数 + 池域审计
/// 事件流 + 溢出丢弃计数。**不碰对象**：留存/丢弃的裁决返回给实现方执行
/// （组合件不是池——槽位存储与对象生命周期归特化面）。
///
/// 借出侧职责分工（RAII 语义，armor #26）：实现方的 guard `Drop` 路径调用
/// [`Self::note_return`]，按返回值留存（放回闲槽）或丢弃——归还永不阻塞
/// 永不报错，槽满 = 丢弃 + [`PoolMetrics::dropped_overflow`] 计数（fail-visible）。
pub struct PoolBase {
    core: Mutex<PoolCore>,
    /// 审计环（[`PoolAuditSink`] 共用抽取件：gate/订阅槽/seq/代次/回调 panic
    /// 计数/TLS 重入封死——与 [`crate::objpool::ObjectPool`] 同源）。
    sink: PoolAuditSink,
}

impl PoolBase {
    /// 构造池基座。非法参数 panic fail-fast（恒不合法的配置必是 bug——同
    /// `BoundedRegistry::new` 纪律）；运行时调参走 [`Self::resize`] 的显式 Err。
    ///
    /// # Panics
    ///
    /// `max == 0` / `idle_cap == 0` / `idle_cap > max`。
    pub fn new(max: usize, idle_cap: usize) -> Self {
        if let Err(e) = validate(max, idle_cap) {
            panic!("PoolBase::new: {e}");
        }
        Self {
            core: Mutex::new(PoolCore {
                max,
                idle_cap,
                idle: 0,
                borrowed: 0,
                dropped_overflow: 0,
            }),
            sink: PoolAuditSink::new(),
        }
    }

    /// 运行时调参（美团「动态化」）：事前校验非法参数入口拒绝（不改任何状态）；
    /// 生效发射 `Resized`、拒绝发射 `ResizeRejected`——两条都进审计留痕。
    ///
    /// 缩容不夺借出：只调边界，已借出句柄不受影响；归还时 [`Self::note_return`]
    /// 按新边界裁决（槽满 = 丢弃 + 计数）。
    ///
    /// 性能：O(1)；校验纯函数零锁，合法路径单次取锁 + 两次原子发射。
    pub fn resize(&self, max: usize, idle_cap: usize) -> Result<(), PoolResizeError> {
        // 校验与状态变更分离（性能纪律）：拒绝路径不碰锁。
        if let Err(e) = validate(max, idle_cap) {
            self.emit(PoolAuditKind::ResizeRejected);
            return Err(e);
        }
        {
            let mut core = lock(&self.core);
            core.max = max;
            core.idle_cap = idle_cap;
        }
        self.emit(PoolAuditKind::Resized);
        Ok(())
    }

    /// 指标快照（可观测）：值语义 `Copy` 读数，含水位线（`borrowed / max`，
    /// 可 >1.0 = 缩容后借出数尚高于新池深——合法中间态，读数如实呈现不钳位）。
    ///
    /// 性能：O(1) 单次取锁 + Copy（契约面纪律；无锁演进时预期更低）。
    pub fn metrics(&self) -> PoolMetrics {
        let core = lock(&self.core);
        PoolMetrics {
            max: core.max,
            idle: core.idle,
            borrowed: core.borrowed,
            dropped_overflow: core.dropped_overflow,
            watermark: core.borrowed as f64 / core.max as f64,
        }
    }

    /// 借出记账（实现方 acquire 成功后调用）：借出数 +1，发射 `Borrowed`。
    /// 性能：O(1)；未订阅时发射 = 一次 gate load 即返。
    pub fn note_borrow(&self) {
        lock(&self.core).borrowed += 1;
        self.emit(PoolAuditKind::Borrowed);
    }

    /// 闲槽记账（实现方维护 idle 读数与现实的同步）：正 = 入闲（预热/创建），
    /// 负 = 离闲（自闲槽借出/缩容弃闲）。不发事件——时刻已被 `Borrowed`/
    /// `Resized` 标记，逐件弃闲事件是噪音。
    ///
    /// # Panics
    ///
    /// 计算后闲槽为负 = 调用方记账 bug（fail-fast，release 同判——计数错账
    /// 禁静默钳位）。
    pub fn note_idle_delta(&self, delta: isize) {
        let mut core = lock(&self.core);
        let next = core.idle as isize + delta;
        assert!(
            next >= 0,
            "PoolBase note_idle_delta: idle would go negative (bookkeeping bug: idle={}, delta={delta})",
            core.idle
        );
        core.idle = next as usize;
    }

    /// 归还裁决（实现方的 RAII guard `Drop` 路径调用）：`true` = 留存（调用方
    /// 把对象放回闲槽存储），`false` = 丢弃（槽满：缩容后超池深，或闲槽上限
    /// 已满——对象直接丢弃，`dropped_overflow` +1，发射 `OverflowDropped`）。
    ///
    /// RAII 语义唯一自洽：归还永不阻塞、永不报错；缩容窗内多余对象无处安放，
    /// 丢弃即计数（fail-visible），禁阻塞归还禁静默。
    ///
    /// 性能：O(1) 单次取锁 + 至多一次发射（gate 快路径）。
    ///
    /// # Panics
    ///
    /// 借出数为 0 时调用 = 记账 bug（fail-fast，release 同判）。
    pub fn note_return(&self) -> bool {
        let keep = {
            let mut core = lock(&self.core);
            assert!(
                core.borrowed > 0,
                "PoolBase note_return: borrowed == 0 (bookkeeping bug)"
            );
            core.borrowed -= 1;
            let keep = core.idle < core.idle_cap && core.idle + core.borrowed < core.max;
            if keep {
                core.idle += 1;
            } else {
                core.dropped_overflow += 1;
            }
            keep
        };
        self.emit(if keep {
            PoolAuditKind::Returned
        } else {
            PoolAuditKind::OverflowDropped
        });
        keep
    }

    /// 开审计流：落闸 + 建槽（容量环 `cap` 钳到 ≥1——恒保最新一件）。再次订阅
    /// = 替换现役槽，被替换票据凭代次对账 inert（Drop 不误杀新流）。
    pub fn subscribe_audit(
        &self,
        cap: usize,
        f: impl FnMut(&PoolAuditEvent) + Send + 'static,
    ) -> PoolAuditTicket {
        self.sink.subscribe_audit(cap, f)
    }

    /// 取走环内事件（事后取证读侧；环随取随清——实时回调是持续观测面，本
    /// 方法是批量对账面，形态同 `SyncStore::take_events`）。
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent> {
        self.sink.take_events()
    }

    /// 审计回调 panic 摘除计数（fail-visible 留痕：回调 panic 后该回调被摘除、
    /// 环照录、池行为不变——观测面禁改变被观测者，摘除必留痕禁静默）。
    pub fn audit_callback_panics(&self) -> u64 {
        self.sink.audit_callback_panics()
    }

    /// 审计发射唯一收口：未订阅 = 一次 gate load 即返（热路径零分配零锁）；
    /// 订阅态 = seq 单调入账（容量环溢出丢最旧）+ **锁外**实时回调（回调可
    /// 重入本池一切入口，持锁即死锁）。热路径禁格式化。
    fn emit(&self, kind: PoolAuditKind) {
        self.sink.emit(kind)
    }
}

// ═══ ThreadPool：动态线程池特化实现（纯 std 调度循环）═══════════════════
// 定位：运行时可调并行度的同步 CPU 密集任务池（调研档 §4.1 拍板：异步 IO 归
// fx，同步 CPU 密集动态并行归 pool，零 tokio）。
//
// 与 PoolBase 的职责切分（§4.4.2 映射表线程池列）：基座承载**管理面**——
// core/max 校验（validate 同源双出口）、resize 审计（Resized/ResizeRejected）、
// 审计流基建（环/seq/票据/重入封死）。任务读侧走池自有计数：`note_borrow`/
// `note_return` 是对象池语义（note_return 的 keep/drop 裁决 = 归还入闲槽 vs
// 销毁，对「worker 闲了是驻留」不成立，混用 = 计数漂移），故不接；线程池
// 可观测 = 队列深/活跃 worker/完成数，全在 ThreadPoolMetrics。
//
// 调度形态（已拍板）：中央队列（Mutex+VecDeque+Condvar，不搞工作窃取——理论
// 射程不覆盖）；零自旋（idle worker condvar park，CPU 静默）；优雅缩容（显式
// 退出令牌，手上无任务时认领即退，执行中任务必跑完，零空转自灭）；resize 补建
// 至新 core（返回即生效），需求性扩容只在队列满且 workers<max（新 worker
// first-task 交接直接接管，队列不越限）；三参封顶 core/max/queue_cap。
//
// 核心形映射：max=worker 上限 / idle=驻留数 / borrowed=活跃数（水位线 =
// busy/max，美团活跃度公式）/ dropped_overflow=被拒任务累计。
//
// worker 创建失败（§7 Runtime/infrastructure 类）：OS 线程创建失败 = 环境
// 故障 → **typed 错误**（[`ThreadSpawnError`]），禁 panic、禁静默降级为「少
// 一个 worker 也能用」。记账发布点 = **席位预留 → 创建失败即回滚席位**：
// 存活计数恒等于真实存在的 worker（幻影 worker 消除），`join` 的等待条件
// （workers>0）因此永不指向不存在的 worker（不挂死）。失败注入闸（[`SpawnGate`]）
// 是类型参数——生产实例化零大小闸门后单态化消除（生产构建零开销）。
// ═══════════════════════════════════════════════════════════════════════

/// 类型擦除任务（提交路径恰一次 Box 分配——类型擦除不可避免，如实报数）。
pub type Task = Box<dyn FnOnce() + Send + 'static>;

/// worker 线程创建失败（§7 Runtime/infrastructure 类：运行期可恢复的 typed
/// operational error——环境资源耗尽，**不是**构造期非法参数）。
///
/// 保留底层 OS 错误现场（[`ThreadSpawnError::io_error`]）：调用方据此判
/// 可重试性（资源耗尽 = 退避后重试；权限/平台限制 = 不可重试）。**绝不**折
/// 叠进 panic，也**绝不**与「策略拒绝」混账——两者恢复语义不同。
#[derive(Debug)]
pub struct ThreadSpawnError {
    source: std::io::Error,
}

impl ThreadSpawnError {
    /// 底层 OS 错误现场（`io::ErrorKind` 面判可重试性）。
    pub fn io_error(&self) -> &std::io::Error {
        &self.source
    }
}

impl From<std::io::Error> for ThreadSpawnError {
    fn from(source: std::io::Error) -> Self {
        Self { source }
    }
}

impl std::fmt::Display for ThreadSpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "thread pool worker spawn failed: {}", self.source)
    }
}

impl std::error::Error for ThreadSpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// 提交失败成因（[`RejectedTask`] 的诊断面）：**策略拒绝**与 **worker 创建
/// 失败**分账——前者可等容量释放，后者是环境故障（恢复路径不同，禁混账）。
#[derive(Debug)]
pub enum RejectCause {
    /// 队列满 / 已 shutdown → 拒绝策略处置（任务原样退还调用方）。
    Policy,
    /// worker 线程创建失败（§7 Runtime/infrastructure 类 typed error）。
    SpawnFailed(ThreadSpawnError),
}

/// worker 创建闸（可插 seam）：生产恒放行（[`AllowAllSpawns`]），测试/仿真
/// 注入**确定性**创建失败。
///
/// **零开销纪律**：闸门是**类型参数**（`ThreadPool<G>`），生产实例化
/// `G = AllowAllSpawns` 后经单态化消除——生产构建既无 vtable 间接调用，也无
/// 额外分配/锁/分支（闸门恒 `Ok`，分支被编译器删掉）。
///
/// 闸门语义：**创建前**裁决。`Ok(())` = 允许本次创建；`Err(e)` = 本次创建
/// 失败（池据此回滚 worker 席位并把失败上抛给调用方——禁静默降级）。
pub trait SpawnGate: Send + Sync + 'static {
    /// 裁决一次 worker 创建（`worker_id` = 本次创建的 worker 序号，单调）。
    fn check_spawn(&self, worker_id: u64) -> Result<(), ThreadSpawnError>;
}

/// 生产闸门：恒放行（零大小类型——生产池的闸门字段不占空间）。
#[derive(Debug, Clone, Copy, Default)]
pub struct AllowAllSpawns;

impl SpawnGate for AllowAllSpawns {
    fn check_spawn(&self, _worker_id: u64) -> Result<(), ThreadSpawnError> {
        Ok(())
    }
}

/// 被拒任务的退还封装（[`ThreadPool::execute`] 的 Err 面）：Box 不实现
/// Debug，包一层让调用方 `unwrap()`/`expect()` 可用。
///
/// 成因随任务一起退还（[`RejectedTask::cause`]）：调用方据此区分「策略拒绝」
/// 与「worker 创建失败」——后者携带 typed 现场（[`ThreadSpawnError`]），任务
/// 原样交还（失败路径禁吞任务）。
pub struct RejectedTask {
    task: Task,
    cause: RejectCause,
}

impl std::fmt::Debug for RejectedTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RejectedTask(<task>)")
    }
}

impl RejectedTask {
    /// 取回原任务（Abort 场景：调用方自行执行或另行处置）。
    pub fn into_task(self) -> Task {
        self.task
    }

    /// 失败成因（区分「策略拒绝」与「worker 创建失败」）。
    pub fn cause(&self) -> &RejectCause {
        &self.cause
    }

    /// worker 创建失败现场（`None` = 策略拒绝，非环境故障）。
    pub fn spawn_error(&self) -> Option<&ThreadSpawnError> {
        match &self.cause {
            RejectCause::Policy => None,
            RejectCause::SpawnFailed(error) => Some(error),
        }
    }
}

/// 三参封顶调参形状（Less is More，§4.4.2）：core 保活下限 / max worker 上限 /
/// queue_cap 排队上限。非法组合入口拒绝（见 [`ThreadPool::resize`]）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadPoolTuning {
    /// 保活 worker 数（恒驻，park 零 CPU）。`core ≥ 1` 且 `core ≤ max`。
    pub core: usize,
    /// worker 上限（含 core）。`max ≥ 1`。
    pub max: usize,
    /// 中央队列容量。`queue_cap ≥ 1`（0 = 提交即拒的直通管道 = 池语义自毁，
    /// 复用 [`PoolResizeError::IdleCapZero`] 语义拒绝；rendezvous 交接不做——
    /// 出现真实采用方再议）。
    queue_cap: usize,
}

impl ThreadPoolTuning {
    /// 构造调参（字段私有化防乱序：非法组合在 [`ThreadPool`] 入口统一拒绝）。
    pub fn new(core: usize, max: usize, queue_cap: usize) -> Self {
        Self {
            core,
            max,
            queue_cap,
        }
    }
}

/// 拒绝策略契约（池满/shutdown 时对提交任务的处置）：四件套默认 +
/// 自定义可插（§4.4.1 A 表「拒绝策略 trait 化」落地）。
///
/// `on_reject` 在**锁外**调用（CallerRuns 可能内联执行任意用户代码）；返回
/// `Err(task)` = 任务原样退还调用方（[`ThreadPool::execute`] 透传）。
/// 无论返回什么，`rejected` 计数在策略触发时已 +1（策略触发即事件）。
pub trait RejectPolicy: Send + Sync + 'static {
    /// 处置被拒任务。`Ok(())` = 策略已消化（内联执行/丢弃/换血入队）；
    /// `Err(task)` = 退还调用方。
    fn on_reject(&self, task: Task, pool: &PoolHandle) -> Result<(), Task>;
}

/// 策略面对池的只读/受限操作柄（策略不能触达调度内核）：窥队列、弃最旧、
/// 补入队。拒绝路径专用，不暴露 execute（封死「策略再提交」自指递归）。
pub struct PoolHandle {
    shared: Arc<Shared>,
}

impl PoolHandle {
    /// 当前排队深。
    pub fn queue_len(&self) -> usize {
        lock(&self.shared.inner).queue.len()
    }

    /// 池是否已 shutdown。
    pub fn is_shutdown(&self) -> bool {
        lock(&self.shared.inner).shutdown
    }

    /// 弃最旧（FIFO 队头）：[`DiscardOldestPolicy`] 的积木，自定义策略可复用。
    pub fn pop_oldest(&self) -> Option<Task> {
        lock(&self.shared.inner).queue.pop_front()
    }

    /// 补入队（有位且未 shutdown 才收）：[`DiscardOldestPolicy`] 的积木。
    pub fn offer(&self, task: Task) -> bool {
        let mut g = lock(&self.shared.inner);
        if g.shutdown || g.queue.len() >= g.queue_cap {
            return false;
        }
        g.queue.push_back(task);
        drop(g);
        self.shared.work.notify_one();
        true
    }
}

/// 默认拒绝四件套（Java TPE 同名语义）：
/// Abort 退还调用方；CallerRuns 提交线程内联执行（任务 panic 按普通调用
/// 传播——调用方自己的线程，自然语义）；Discard 静默丢弃（计数留痕）；
/// DiscardOldest 弃队头换本任务入队（pop 与 offer 之间的并发空位被抢时
/// 弃新任务——不循环重试，见 [`PoolHandle::offer`]）。
#[derive(Debug, Clone, Copy, Default)]
pub struct AbortPolicy;

#[derive(Debug, Clone, Copy, Default)]
pub struct CallerRunsPolicy;

#[derive(Debug, Clone, Copy, Default)]
pub struct DiscardPolicy;

#[derive(Debug, Clone, Copy, Default)]
pub struct DiscardOldestPolicy;

impl RejectPolicy for AbortPolicy {
    fn on_reject(&self, task: Task, _pool: &PoolHandle) -> Result<(), Task> {
        Err(task)
    }
}

impl RejectPolicy for CallerRunsPolicy {
    fn on_reject(&self, task: Task, _pool: &PoolHandle) -> Result<(), Task> {
        task();
        Ok(())
    }
}

impl RejectPolicy for DiscardPolicy {
    fn on_reject(&self, _task: Task, _pool: &PoolHandle) -> Result<(), Task> {
        Ok(())
    }
}

impl RejectPolicy for DiscardOldestPolicy {
    fn on_reject(&self, task: Task, pool: &PoolHandle) -> Result<(), Task> {
        let _ = pool.pop_oldest();
        let _ = pool.offer(task);
        Ok(())
    }
}

/// 调度内核（单权威锁 + 双 condvar）。worker 生命周期状态机全在此锁内裁决。
struct Shared {
    inner: Mutex<Inner>,
    /// worker 队空等待（零自旋 park 点）。
    work: Condvar,
    /// join 等待面：worker 退役（workers==0）通知。
    retired: Condvar,
}

struct Inner {
    queue: VecDeque<Task>,
    core: usize,
    max: usize,
    queue_cap: usize,
    workers: usize,
    busy: usize,
    idle: usize,
    /// 待退令牌数（缩容差额）。worker 手上无任务时认领即退；回扩时重算可撤销。
    shrinking: usize,
    completed: u64,
    rejected: u64,
    task_panics: u64,
    /// worker 创建失败累计（fail-visible：环境故障留痕；与策略拒绝 `rejected`
    /// 分账——两者恢复语义不同，禁混账）。
    spawn_failures: u64,
    next_id: u64,
    shutdown: bool,
}

/// 预留一个 worker 席位（**锁内调用**）：计数 +1 并分配 worker id。
///
/// 席位先于创建发布——保证 worker 退出时的递减不会先于计数（禁下溢）；创建
/// 窗口内计数含该席位（保守上界：join 因此不会漏等在途创建）；创建失败由
/// [`ThreadPool::rollback_spawn`] 回滚（幻影 worker 消除）。
fn reserve_worker(g: &mut Inner) -> u64 {
    g.workers += 1;
    let id = g.next_id;
    g.next_id += 1;
    id
}

/// 线程池指标（§4.4.2 可观测行：队列深/活跃 worker/完成数 + 治理计数）。
/// 经 [`Borrow`] 可达核心形 [`PoolMetrics`]（映射见模块文档表）。derive 面
/// 与 [`MemoryPoolMetrics`](crate::memgov::MemoryPoolMetrics) 同一读侧纪律
/// （快照整体可等值比较——拒绝路径「零状态变更」断言的统一写法）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ThreadPoolMetrics {
    /// 保活下限（resize 目标）。
    pub core: usize,
    /// 存活 worker 数（恒等于真实存在的 worker——创建失败不留幻影计数）。
    pub workers: usize,
    /// 执行中 worker 数。
    pub busy: usize,
    /// 驻留（parked）worker 数。
    pub idle: usize,
    /// 当前排队深。
    pub queue_len: usize,
    /// 排队上限。
    pub queue_cap: usize,
    /// 正常跑完的任务累计（panic 任务另册 `task_panics`）。
    pub completed: u64,
    /// 拒绝策略触发累计（含 CallerRuns 内联）。
    pub rejected: u64,
    /// 任务 panic 摘除累计（fail-visible：worker 存活但 panic 禁静默）。
    pub task_panics: u64,
    /// worker 创建失败累计（fail-visible：环境故障留痕；与策略拒绝 `rejected`
    /// 分账——恢复语义不同，禁混账）。
    pub spawn_failures: u64,
    /// 共享核心形（可见性与 [`MemoryPoolMetrics`](crate::memgov::MemoryPoolMetrics)
    /// 的 `pub core` 同一形态；名不从 `core`——本池 `core` 已承载保活数语义，
    /// 三池命名不对称的唯一处）。
    pub shape: PoolMetrics,
}

impl Borrow<PoolMetrics> for ThreadPoolMetrics {
    fn borrow(&self) -> &PoolMetrics {
        &self.shape
    }
}

/// 动态线程池：[`PoolManaged`] 的 worker 调度特化实现（管理面委托
/// [`PoolBase`]，任务面自有计数——切分理由见模块文档）。
///
/// 泛型参数 `G` 是**创建闸**（[`SpawnGate`]）：生产用 [`AllowAllSpawns`]
/// （默认类型参数，单态化后零开销），测试/仿真注入确定性创建失败。
///
/// # Examples
///
/// ```
/// use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
/// use std::sync::Arc;
///
/// let pool = ThreadPool::new(ThreadPoolTuning::new(1, 2, 8));
/// let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
/// let n2 = Arc::clone(&n);
/// pool.execute(move || {
///     n2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
/// })
/// .unwrap();
/// pool.shutdown();
/// pool.join();
/// assert_eq!(n.load(std::sync::atomic::Ordering::Relaxed), 1);
/// ```
pub struct ThreadPool<G: SpawnGate = AllowAllSpawns> {
    base: PoolBase,
    policy: Arc<dyn RejectPolicy>,
    shared: Arc<Shared>,
    /// 创建闸（可插 seam）：生产为 [`AllowAllSpawns`]（零大小，单态化后零开销）。
    gate: G,
}

/// worker 创建失败现场（模块私有）：typed 错误 + 未能交付的任务（交还调用方）。
struct SpawnFailure {
    error: ThreadSpawnError,
    task: Option<Task>,
}

impl ThreadPool<AllowAllSpawns> {
    /// 构造（core 保活层即建）。非法配置 panic fail-fast（恒不合法 = bug，
    /// 同 [`PoolBase::new`] 纪律）；默认拒绝策略 Abort。
    ///
    /// # Panics
    ///
    /// `core == 0` / `max == 0` / `core > max` / `queue_cap == 0`；
    /// **worker 创建失败**（环境资源耗尽）亦 panic——本入口是 fail-fast 快捷
    /// 面；要 typed 错误用 [`Self::try_new`]。
    pub fn new(tuning: ThreadPoolTuning) -> Self {
        Self::try_new(tuning).expect("thread pool worker spawn failed (thread resource exhaustion)")
    }

    /// 构造并指定拒绝策略（策略构造期定死——三参封顶不纳入策略动态面）。
    ///
    /// # Panics
    ///
    /// 同 [`Self::new`]。
    pub fn with_policy(t: ThreadPoolTuning, policy: Arc<dyn RejectPolicy>) -> Self {
        Self::try_with_policy(t, policy)
            .expect("thread pool worker spawn failed (thread resource exhaustion)")
    }

    /// 构造（typed 失败面）：非法配置 panic fail-fast（同 [`Self::new`]），
    /// **worker 创建失败返回 typed 错误**（§7 Runtime/infrastructure 类）——
    /// 返回前已建 worker 已收到停机信号（自行退役；**不 join**——join 等「存活计数归零」，计数不变量被破坏时会永久挂起）。
    pub fn try_new(tuning: ThreadPoolTuning) -> Result<Self, ThreadSpawnError> {
        Self::try_with_policy(tuning, Arc::new(AbortPolicy))
    }

    /// 构造并指定拒绝策略（typed 失败面）：同 [`Self::try_new`]。
    pub fn try_with_policy(
        t: ThreadPoolTuning,
        policy: Arc<dyn RejectPolicy>,
    ) -> Result<Self, ThreadSpawnError> {
        Self::try_with_gate(t, policy, AllowAllSpawns)
    }
}

impl<G: SpawnGate> ThreadPool<G> {
    /// 构造（可插创建闸 + 指定拒绝策略，typed 失败面）。
    ///
    /// 非法配置 panic fail-fast（恒不合法 = bug，§7 构造期纪律）；worker 创建
    /// 失败返回 [`ThreadSpawnError`]——**已创建的 worker 在返回前收到停机信号
    /// 并自行退役**（不留悬挂线程；计数不留幻影）。
    pub fn try_with_gate(
        tuning: ThreadPoolTuning,
        policy: Arc<dyn RejectPolicy>,
        gate: G,
    ) -> Result<Self, ThreadSpawnError> {
        // 线程池词汇的前置 fail-fast（可读消息）；随后 PoolBase::new 以同一套
        // 规则二次闸（契约同源双出口，校验必过）。
        if tuning.core == 0 {
            panic!("ThreadPool::new: core == 0 (no permanent workers is a bug)");
        }
        if tuning.max == 0 {
            panic!("ThreadPool::new: max == 0 (a pool that cannot run is a bug)");
        }
        if tuning.core > tuning.max {
            panic!(
                "ThreadPool::new: core {} exceeds max {} (illegal tuning)",
                tuning.core, tuning.max
            );
        }
        if tuning.queue_cap == 0 {
            panic!("ThreadPool::new: queue cap == 0 (hand-off pipe is a bug)");
        }
        let base = PoolBase::new(tuning.max, tuning.core);
        let shared = Arc::new(Shared {
            inner: Mutex::new(Inner {
                queue: VecDeque::with_capacity(tuning.queue_cap),
                core: tuning.core,
                max: tuning.max,
                queue_cap: tuning.queue_cap,
                // 计数随 worker 真实创建发布（禁预挂幻影 worker——join 会等它）。
                workers: 0,
                busy: 0,
                idle: 0,
                shrinking: 0,
                completed: 0,
                rejected: 0,
                task_panics: 0,
                spawn_failures: 0,
                next_id: 0,
                shutdown: false,
            }),
            work: Condvar::new(),
            retired: Condvar::new(),
        });
        let pool = Self {
            base,
            policy,
            shared,
            gate,
        };
        // core 保活层逐个创建：任一失败 → 回滚该席位 + 停机等已建 worker 退役
        // + 上抛 typed 错误（调用方拿到的是错误，不是半成品池）。
        for _ in 0..tuning.core {
            let worker_id = {
                let mut g = lock(&pool.shared.inner);
                reserve_worker(&mut g)
            };
            if let Err(failure) = pool.create_worker(None, worker_id) {
                // 停机信号下发即返回（**不 join**——join 等「存活计数归零」，
                // 计数不变量被破坏时会永久挂起；已建 worker 收到停机信号后
                // 自行退役，池对象随即 drop 兜底重复下发该信号）。
                pool.shutdown();
                return Err(failure.error);
            }
        }
        Ok(pool)
    }

    /// 提交任务（拒绝时按策略处置）。队列有位即入队（FIFO）；队列满且
    /// workers<max 时新建 worker 直接接管本任务（first-task 交接，队列不越限）；
    /// 队列满且到 max（或已 shutdown）走拒绝策略。
    ///
    /// 每次提交恰一次 `Box` 分配（类型擦除不可避免）；CallerRuns 内联执行时
    /// 任务 panic 在提交线程自然传播。
    ///
    /// **worker 创建失败**：返回 [`RejectedTask`]，其
    /// [`RejectCause::SpawnFailed`] 带 typed 现场，任务原样退还（调用方可
    /// 自行执行或稍后重试）——禁 panic、禁静默吞任务。
    pub fn execute(&self, task: impl FnOnce() + Send + 'static) -> Result<(), RejectedTask> {
        self.submit(Box::new(task))
    }

    fn submit(&self, task: Task) -> Result<(), RejectedTask> {
        let spawn_id;
        {
            let mut g = lock(&self.shared.inner);
            if g.shutdown || (g.queue.len() >= g.queue_cap && g.workers >= g.max) {
                g.rejected += 1;
                spawn_id = None; // 拒绝：锁外交策略（CallerRuns 可能跑任意用户代码）
            } else if g.queue.len() < g.queue_cap {
                g.queue.push_back(task);
                drop(g);
                self.shared.work.notify_one();
                return Ok(());
            } else {
                // 队列满、workers<max：新 worker first-task 交接（不越限排队）
                // ——席位预留与「是否可扩容」的裁决同锁完成（禁拆开并发超建）。
                spawn_id = Some(reserve_worker(&mut g));
            }
        }
        match spawn_id {
            Some(worker_id) => match self.create_worker(Some(task), worker_id) {
                Ok(()) => Ok(()),
                Err(failure) => Err(RejectedTask {
                    // 创建失败必把任务原样交还（内部不变式：失败路径不吞任务）。
                    task: failure
                        .task
                        .expect("worker creation failure must hand the task back"),
                    cause: RejectCause::SpawnFailed(failure.error),
                }),
            },
            None => {
                let handle = PoolHandle {
                    shared: Arc::clone(&self.shared),
                };
                self.policy
                    .on_reject(task, &handle)
                    .map_err(|task| RejectedTask {
                        task,
                        cause: RejectCause::Policy,
                    })
            }
        }
    }

    /// 运行时调参（[`PoolManaged`] 契约）：core/max 校验委托 [`PoolBase::resize`]
    /// （同源规则 + Resized/ResizeRejected 审计白送）；queue_cap 零值在进基座
    /// 前拒绝（直通管道语义复用 `IdleCapZero`——此路径不进审计流，非法参数
    /// 病理面如实降档）。
    ///
    /// 生效语义：扩容补建至新 core（返回即到数）；缩容发退出令牌（差额 worker
    /// 处理完当前任务即退）；回扩重算待退令牌（撤销未消费的缩容）；queue_cap
    /// 只拦未来入队（既有积压照常排空）。
    ///
    /// **worker 创建失败**（§7 Runtime/infrastructure 类）：扩容补建时 OS 线程
    /// 创建失败 → **typed Err**（[`PoolResizeError::SpawnFailed`]）——错误对象
    /// 自带「部分生效」状态（目标 core / 实际存活 worker 数），调用方不必回查
    /// 指标即可判断池处于什么状态（已建 worker 继续服务，可重试 resize 补齐）。
    /// 失败另经 [`ThreadPoolMetrics::spawn_failures`] 计数留痕（fail-visible）。
    pub fn resize(&self, t: ThreadPoolTuning) -> Result<(), PoolResizeError> {
        if t.queue_cap == 0 {
            return Err(PoolResizeError::IdleCapZero);
        }
        self.base.resize(t.max, t.core)?;
        let mut spawn_ids = Vec::new();
        let shrink_pending;
        {
            let mut g = lock(&self.shared.inner);
            g.core = t.core;
            g.max = t.max;
            g.queue_cap = t.queue_cap;
            g.shrinking = g.workers.saturating_sub(t.core);
            shrink_pending = g.shrinking > 0;
            let deficit = t.core.saturating_sub(g.workers);
            for _ in 0..deficit {
                spawn_ids.push(reserve_worker(&mut g));
            }
        }
        if shrink_pending {
            self.shared.work.notify_all();
        }
        let mut spawn_failure = None;
        for (index, worker_id) in spawn_ids.iter().enumerate() {
            if let Err(failure) = self.create_worker(None, *worker_id) {
                // 补建在失败处停止；未尝试的预留一并回滚（幻影 worker 消除，
                // join 不会等一个不存在的 worker）。
                self.rollback_reservations(spawn_ids.len() - index - 1);
                spawn_failure = Some(failure.error);
                break;
            }
        }
        match spawn_failure {
            None => Ok(()),
            Some(error) => {
                // 部分生效报告：调参已落地但 worker 未建够——错误对象自带生效
                // 状态（目标 core / 实际存活数），池保持有效（已建 worker 继续
                // 服务，计数准确）。
                let workers = lock(&self.shared.inner).workers;
                Err(PoolResizeError::SpawnFailed {
                    kind: error.io_error().kind(),
                    target_core: t.core,
                    workers,
                })
            }
        }
    }

    /// 指标快照（值语义 `Copy`，O(1) 单次取锁）。
    pub fn metrics(&self) -> ThreadPoolMetrics {
        let g = lock(&self.shared.inner);
        ThreadPoolMetrics {
            core: g.core,
            workers: g.workers,
            busy: g.busy,
            idle: g.idle,
            queue_len: g.queue.len(),
            queue_cap: g.queue_cap,
            completed: g.completed,
            rejected: g.rejected,
            task_panics: g.task_panics,
            spawn_failures: g.spawn_failures,
            shape: PoolMetrics {
                max: g.max,
                idle: g.idle,
                borrowed: g.busy,
                dropped_overflow: g.rejected,
                watermark: g.busy as f64 / g.max as f64,
            },
        }
    }

    /// 关闭入口：停止接收新提交（后续提交走拒绝策略），已入队任务排空后
    /// worker 退役。幂等。
    pub fn shutdown(&self) {
        {
            let mut g = lock(&self.shared.inner);
            g.shutdown = true;
        }
        self.shared.work.notify_all();
    }

    /// 等待全部 worker 退役（shutdown 或缩容到 0 后才有确定终点；在活跃池上
    /// 调用会阻塞到池关闭）。幂等。
    ///
    /// 存活计数只含**真实存在**的 worker（创建失败不留幻影）——本方法不会因
    /// 创建失败而永久等待。
    pub fn join(&self) {
        let mut g = lock(&self.shared.inner);
        while g.workers > 0 {
            g = self
                .shared
                .retired
                .wait(g)
                .expect("thread pool lock poisoned (prior holder panicked)");
        }
    }

    /// 开审计流（委托 [`PoolBase::subscribe_audit`]：容量环/seq/票据/重入封死
    /// 基建全复用）。线程池域的审计时刻 = 管理面（resize 生效/拒绝）。
    pub fn subscribe_audit(
        &self,
        f: impl FnMut(&PoolAuditEvent) + Send + 'static,
    ) -> PoolAuditTicket {
        self.base.subscribe_audit(AUDIT_RING_DEFAULT_CAP, f)
    }

    /// 取走环内审计事件（委托基座，随取随清）。
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent> {
        self.base.take_audit_events()
    }

    /// 创建 worker（**调用方须已在锁内预留席位**——见 [`reserve_worker`]）：
    /// 闸门裁决 → OS 创建 → 失败回滚席位（幻影 worker 消除）。
    ///
    /// 失败路径三件事同做：回滚席位（计数回到实际存活数）+ 失败计数
    /// （`spawn_failures`）+ 唤醒 join（否则 join 会等一个不存在的 worker）。
    fn create_worker(&self, first: Option<Task>, worker_id: u64) -> Result<(), SpawnFailure> {
        // ① 闸门裁决（生产恒放行；单态化后本分支被编译器删掉）。
        if let Err(error) = self.gate.check_spawn(worker_id) {
            return Err(self.rollback_spawn(error, first));
        }
        // ② OS 创建。任务经一次性槽交接：创建失败时原样取回交还调用方
        //    （失败路径禁吞任务——任务归属调用方）。
        let slot = Arc::new(Mutex::new(first));
        let worker_slot = Arc::clone(&slot);
        let worker_shared = Arc::clone(&self.shared);
        let spawned = thread::Builder::new()
            .name(format!("pool-worker-{worker_id}"))
            .spawn(move || {
                let mut guard = lock(&worker_slot);
                let first = guard.take();
                drop(guard);
                worker_loop(worker_shared, first);
            });
        match spawned {
            Ok(_handle) => Ok(()),
            Err(e) => {
                let mut guard = lock(&slot);
                let task = guard.take();
                drop(guard);
                Err(self.rollback_spawn(ThreadSpawnError::from(e), task))
            }
        }
    }

    /// 创建失败回滚：撤销预留席位（幻影 worker 消除）+ 失败计数 + 唤醒 join。
    fn rollback_spawn(&self, error: ThreadSpawnError, task: Option<Task>) -> SpawnFailure {
        {
            let mut g = lock(&self.shared.inner);
            g.workers -= 1;
            g.spawn_failures += 1;
        }
        // join 在 `retired` 上等 worker 退役：回滚后必须唤醒，否则 join 会
        // 永远等一个不存在的 worker。
        self.shared.retired.notify_all();
        SpawnFailure { error, task }
    }

    /// 回滚未尝试的预留席位（resize 补建中断时用——幻影 worker 消除）。
    fn rollback_reservations(&self, count: usize) {
        if count == 0 {
            return;
        }
        {
            let mut g = lock(&self.shared.inner);
            g.workers -= count;
        }
        self.shared.retired.notify_all();
    }
}

impl<G: SpawnGate> PoolManaged for ThreadPool<G> {
    type Tuning = ThreadPoolTuning;
    type Metrics = ThreadPoolMetrics;

    fn resize(&self, tuning: Self::Tuning) -> Result<(), PoolResizeError> {
        ThreadPool::<G>::resize(self, tuning)
    }

    fn metrics(&self) -> Self::Metrics {
        ThreadPool::<G>::metrics(self)
    }

    fn subscribe_audit(&self, f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket {
        ThreadPool::<G>::subscribe_audit(self, f)
    }
}

impl<G: SpawnGate> Drop for ThreadPool<G> {
    fn drop(&mut self) {
        // Drop = 发优雅关闭信号（排空后退役），不 join——worker 持 Shared Arc
        // 存活到排空完成，池亡线程不悬空；确定性收尾用 shutdown()+join()。
        {
            let mut g = lock(&self.shared.inner);
            g.shutdown = true;
        }
        self.shared.work.notify_all();
    }
}

/// worker 主循环：零自旋（condvar park）；退出令牌优先于取任务（缩容语义 =
/// 处理完当前任务即退）；first-task 交接先跑再入环。
fn worker_loop(shared: Arc<Shared>, mut first: Option<Task>) {
    let mut g = lock(&shared.inner);
    loop {
        let next = if let Some(task) = first.take() {
            g.busy += 1;
            Some(task)
        } else if g.shrinking > 0 {
            g.shrinking -= 1;
            g.workers -= 1;
            shared.retired.notify_all();
            return;
        } else if let Some(task) = g.queue.pop_front() {
            g.busy += 1;
            Some(task)
        } else if g.shutdown {
            g.workers -= 1;
            shared.retired.notify_all();
            return;
        } else {
            // 零自旋：队空且无退出令牌 → condvar park（OS 挂起，CPU 静默）。
            g.idle += 1;
            g = shared
                .work
                .wait(g)
                .expect("thread pool lock poisoned (prior holder panicked)");
            g.idle -= 1;
            continue;
        };
        drop(g);
        // 锁外执行；任务 panic 摘除（worker 存活接续，池不砖），另册留痕。
        let mut slot = next;
        let panicked = catch_unwind(AssertUnwindSafe(|| (slot.take().unwrap())())).is_err();
        g = lock(&shared.inner);
        g.busy -= 1;
        if panicked {
            g.task_panics += 1;
        } else {
            g.completed += 1;
        }
    }
}
