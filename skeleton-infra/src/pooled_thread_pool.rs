//! 组合件：`PooledThreadPool<T>`——`MemoryPool` × `ThreadPool` 互喂时序封装
//! （第三个可选池类型，纯 infra 侧组合层）。
//!
//! # 为什么借还发生在任务闭包内（worker 线程）
//!
//! [`MemoryPool::acquire`] 返回的 [`PoolGuard`](crate::memgov::PoolGuard) 持有
//! 池的借用引用，**不是 `'static`**——无法在提交线程借出后把 guard move 进
//! `'static` 任务闭包（自引用结构不可安全构造）。因此时序封装为：提交时只
//! 入队闭包，worker 认领任务后才借缓冲 → 跑 `f` → guard 离开作用域 Drop 即
//! 还（RAII）。**panic unwind 同样覆盖**：guard 是任务闭包的栈上局部量，
//! unwind 展开路径照常 Drop，缓冲必回池（`worker_loop` 的 catch_unwind 在
//! unwind 完成后才摘除任务 panic——测试
//! `tests/pooled_thread_pool.rs::task_panic_returns_buffer_and_pool_stays_usable`
//! 钉死该路径）。
//!
//! # 借空语义 = Discard 等价（计数留痕，非静默）
//!
//! worker 执行期 [`MemoryPool::acquire`] 超过借用超时仍无缓冲时，任务体丢弃
//! 并计入 [`PooledThreadPool::borrow_rejected`]。这是队列满四策略中唯一在
//! 异步执行边界语义自洽的对齐：Abort 无法把任务退还调用方（提交早已返回）；
//! CallerRuns 无法凭空造出缓冲（造缓冲=绕开内存治理上限，负载下无界分配，
//! 与组合内存池的目的相反）；DiscardOldest 换血入队后队头任务依旧无缓冲可借。
//! 与提交侧拒绝（`ThreadPool` 队列满 → `rejected` 计数）是**两个独立计数**，
//! 不得混账。
//!
//! # 借用超时构造期必填（D21 同源）
//!
//! 无默认无限等：构造时显式给 `borrow_timeout`，借不到=显式丢弃+计数。
//!
//! # 文件位置
//!
//! 独立新文件而非并入 `pool.rs`：后者已 848 行（超单文件 800 行上限），且本
//! 件是跨 `memgov` + `pool` 两域的可选组合层，独立生命周期。零核心耦合：契约
//! 层零动，两池类型签名零变，两池彼此仍不认识。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::memgov::MemoryPool;
use crate::pool::{RejectedTask, ThreadPool};

/// 内存池 × 线程池组合：提交 `FnOnce(&mut T)` 任务，缓冲从 [`MemoryPool`] 借、
/// 任务结束（含 panic）自动还。
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use skeleton_infra::memgov::MemoryPool;
/// use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
/// use skeleton_infra::pooled_thread_pool::PooledThreadPool;
///
/// let mem = Arc::new(MemoryPool::<Vec<u8>>::new(4, 2, Vec::new));
/// let ptp = PooledThreadPool::new(
///     mem,
///     ThreadPool::new(ThreadPoolTuning::new(1, 2, 8)),
///     Duration::from_millis(100),
/// );
/// ptp.submit(|buf: &mut Vec<u8>| buf.push(7)).unwrap();
/// ptp.threads().shutdown();
/// ptp.threads().join();
/// assert_eq!(ptp.mem_pool().metrics().core.idle, 1);
/// ```
pub struct PooledThreadPool<T, F = fn() -> T> {
    mem: Arc<MemoryPool<T, F>>,
    threads: ThreadPool,
    borrow_timeout: Duration,
    /// 借空丢弃计数（fail-visible：丢弃必留痕，禁静默）。
    borrow_rejected: Arc<AtomicU64>,
}

impl<T, F> PooledThreadPool<T, F>
where
    T: Send + 'static,
    F: Fn() -> T + Send + Sync + 'static,
{
    /// 组装。`mem` 以 `Arc` 接入（共享同一内存池的其他消费方不受影响）；
    /// `threads` 取所有权（组合件的 worker 集自洽，Drop 时随件优雅关闭——
    /// 确定性收尾用 [`Self::threads`] 的 `shutdown()+join()`）。借用超时
    /// 构造期必填（D21 同源，无默认无限等）。
    pub fn new(mem: Arc<MemoryPool<T, F>>, threads: ThreadPool, borrow_timeout: Duration) -> Self {
        Self {
            mem,
            threads,
            borrow_timeout,
            borrow_rejected: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 提交任务：worker 认领后从内存池借 `T` → 跑 `f(&mut buf)` → guard Drop
    /// 自动还池（panic unwind 同覆盖，见模块文档）。
    ///
    /// 返回值只反映**提交侧**拒绝（队列满/shutdown，透传 [`ThreadPool`] 的
    /// 拒绝策略）；执行期借空走 Discard 等价（任务体丢弃 +
    /// [`Self::borrow_rejected`] 计数），见模块文档。
    pub fn submit(&self, f: impl FnOnce(&mut T) + Send + 'static) -> Result<(), RejectedTask> {
        let mem = Arc::clone(&self.mem);
        let borrow_rejected = Arc::clone(&self.borrow_rejected);
        let borrow_timeout = self.borrow_timeout;
        self.threads
            .execute(move || match mem.acquire(borrow_timeout) {
                Ok(mut buf) => f(&mut buf),
                // 借空=Discard 等价：任务体丢弃、计数留痕（两个独立计数的后者）。
                Err(_) => {
                    borrow_rejected.fetch_add(1, Ordering::Relaxed);
                }
            })
    }

    /// 内存池管理面入口（resize / metrics / 审计订阅 / 直接借还）。
    pub fn mem_pool(&self) -> &MemoryPool<T, F> {
        &self.mem
    }

    /// 线程池管理面入口（resize / metrics / shutdown+join / 审计订阅）。
    pub fn threads(&self) -> &ThreadPool {
        &self.threads
    }

    /// 借空丢弃累计（worker 执行期超借用超时无缓冲的任务数；与线程池提交侧
    /// `rejected` 计数独立）。
    pub fn borrow_rejected(&self) -> u64 {
        self.borrow_rejected.load(Ordering::Relaxed)
    }
}
