//! 组合件：`PooledThreadPool<T>`——`ObjectPool` × `ThreadPool` 互喂时序封装
//! （第三个可选池类型，纯 infra 侧组合层）。
//!
//! # 为什么借还发生在任务闭包内（worker 线程）
//!
//! [`ObjectPool::acquire`] 返回的 [`Pooled`](crate::objpool::Pooled) 是借用池的
//! 句柄（非 `'static`）——无法在提交线程借出后把句柄 move 进 `'static` 任务闭包
//! （自引用结构不可安全构造）。因此时序封装为：提交时只入队闭包，worker 认领
//! 任务后才借缓冲 → 跑 `f` → 句柄离开作用域 Drop 即还（RAII）。**panic unwind
//! 同样覆盖**：句柄是任务闭包的栈上局部量，unwind 展开路径照常 Drop，宇宙记账
//! 必回卷、池可继续用（`ObjectPool` 的 unwind 防御：panic 中对象不变量可能已破
//! ⇒ 销毁不回池 + `panic_dropped` 留痕，见
//! `tests/object_pool.rs::unwind_return_destroys_object_and_does_not_repool`；本件
//! 测试 `tests/pooled_thread_pool.rs::task_panic_returns_buffer_and_pool_stays_usable`
//! 钉住「panic 后零在借 + 池可继续用」）。
//!
//! # 借空语义 = 降级直构（永不丢任务）
//!
//! [`ObjectPool::acquire`] **永不阻塞、永不失败**：idle 有货即复用 → 宇宙
//! （idle + 正常借出，硬上界 `max`）未满即工厂新建 → **宇宙已满则降级直构**
//! （不入宇宙、归还即释放）。所以任务总能拿到缓冲执行完，本件**不存在「借不到
//! 缓冲就丢工作」的路径**——旧实现的 `acquire(borrow_timeout)` 超时丢弃 +
//! `borrow_rejected` 计数已退役。
//!
//! 可观测性不丢：降级面由 [`ObjectPool`] 自身的 `metrics()` 表达——`degraded`
//! （降级直构次数，宇宙饱和的直接读数）与 `constructed`（工厂调用总数，含降级）
//! 是两个独立计数，经 [`PooledThreadPool::buffer_pool`] 读。三轴口径（蓝图 §5）
//! 由此落地：alive 轴 = idle + 正常借出受 `max`；字节轴 = 仅 idle 常驻；降级轴 =
//! 宇宙满时直构、两轴皆不计。
//!
//! # 提交侧拒绝档：默认 Abort / 可选 CallerRuns
//!
//! 缓冲侧永不拒绝后，**唯一还会丢工作的地方是提交侧**（队列满且 worker 到
//! `max`）：默认 `Abort` 把任务退还调用方（[`PooledThreadPool::submit`] 的
//! `Err`），[`PooledThreadPool::with_caller_runs`] 档在**提交线程内联执行**
//! （不丢、当代跑）。策略机制复用 [`ThreadPool`] 的 `RejectPolicy` /
//! [`CallerRunsPolicy`]，本件不造第二套；内联任务的缓冲照常从同一池借（宇宙满
//! 则降级直构）。**两个计数不得混账**：`threads().metrics().rejected`（提交侧策略
//! 触发）与 `buffer_pool().metrics().degraded`（缓冲侧降级）是两条独立路径的留痕。
//!
//! # 与 `MemoryPool` 组合件的差异（0.1.0 未发布，直迁）
//!
//! 缓冲域从 [`MemoryPool`](crate::memgov::MemoryPool) 换成 [`ObjectPool`]：`new`
//! 不再收 `borrow_timeout`（新语义无等待点，参数失去意义，已移除）；`mem_pool()`
//! 更名 [`PooledThreadPool::buffer_pool`]；`borrow_rejected()` 退役（读数改由
//! `buffer_pool().metrics().degraded` 承担）。
//!
//! # 文件位置
//!
//! 独立新文件而非并入 `pool.rs`：后者已 848 行（超单文件 800 行上限），且本
//! 件是跨 `objpool` + `pool` 两域的可选组合层，独立生命周期。零核心耦合：契约
//! 层零动，两池类型签名零变，两池彼此仍不认识。

use std::sync::Arc;

use crate::objpool::ObjectPool;
use crate::pool::{CallerRunsPolicy, RejectedTask, ThreadPool, ThreadPoolTuning};

/// 对象池 × 线程池组合：提交 `FnOnce(&mut T)` 任务，缓冲从 [`ObjectPool`] 借、
/// 任务结束（含 panic）自动还。
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use skeleton_infra::objpool::ObjectPool;
/// use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
/// use skeleton_infra::pooled_thread_pool::PooledThreadPool;
///
/// let buffers = Arc::new(ObjectPool::<Vec<u8>>::new(4, 2, Vec::new));
/// let ptp = PooledThreadPool::new(buffers, ThreadPool::new(ThreadPoolTuning::new(1, 2, 8)));
/// ptp.submit(|buf: &mut Vec<u8>| buf.push(7)).unwrap();
/// ptp.threads().shutdown();
/// ptp.threads().join();
/// assert_eq!(ptp.buffer_pool().metrics().core.idle, 1);
/// ```
pub struct PooledThreadPool<T> {
    buffers: Arc<ObjectPool<T>>,
    threads: ThreadPool,
}

impl<T> PooledThreadPool<T>
where
    T: Send + 'static,
{
    /// 组装。`buffers` 以 `Arc` 接入（宇宙/降级计数归池自身，组合件不重记）；
    /// `threads` 取所有权（Drop 时随件优雅关闭——确定性收尾用
    /// [`Self::threads`] 的 `shutdown()+join()`）。提交侧拒绝策略由传入的
    /// [`ThreadPool`] 决定（CallerRuns 档见 [`Self::with_caller_runs`]）。
    pub fn new(buffers: Arc<ObjectPool<T>>, threads: ThreadPool) -> Self {
        Self { buffers, threads }
    }

    /// CallerRuns 档（显式可选拒绝策略）：池饱和（队列满且 worker 到 `max`）时
    /// **提交线程代跑**被拒任务，不丢工作。策略复用
    /// [`ThreadPool::with_policy`] + [`CallerRunsPolicy`]（不新造第二套机制）；
    /// 内联任务在提交线程借缓冲，宇宙满则降级直构。
    ///
    /// # Panics
    ///
    /// 同 [`ThreadPool::with_policy`]（非法调参 / worker 创建失败 fail-fast）。
    pub fn with_caller_runs(buffers: Arc<ObjectPool<T>>, tuning: ThreadPoolTuning) -> Self {
        Self::new(
            buffers,
            ThreadPool::with_policy(tuning, Arc::new(CallerRunsPolicy)),
        )
    }

    /// 提交任务：worker 认领后从缓冲池借 `T` → 跑 `f(&mut buf)` → 句柄 Drop
    /// 自动还池（panic unwind 同覆盖，见模块文档）。
    ///
    /// 返回值只反映**提交侧**拒绝（队列满/shutdown，透传 [`ThreadPool`] 的拒绝
    /// 策略）；执行期借空**不再丢任务**——[`ObjectPool::acquire`] 永不失败（宇宙
    /// 满降级直构），降级面经 [`Self::buffer_pool`] 的 `metrics().degraded` 可见。
    pub fn submit(&self, f: impl FnOnce(&mut T) + Send + 'static) -> Result<(), RejectedTask> {
        let buffers = Arc::clone(&self.buffers);
        self.threads.execute(move || {
            let mut buf = buffers.acquire();
            f(&mut buf);
        })
    }

    /// 缓冲池管理面入口（resize / metrics / prune_idle / 直接借还）。
    pub fn buffer_pool(&self) -> &ObjectPool<T> {
        &self.buffers
    }

    /// 线程池管理面入口（resize / metrics / shutdown+join / 审计订阅）。
    pub fn threads(&self) -> &ThreadPool {
        &self.threads
    }
}
