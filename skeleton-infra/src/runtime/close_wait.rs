//! 停机等待面（issue#64 拍板）：[`Store::close`](skeleton::Store::close) 只 best-effort 排空，
//! 本域提供显式「等静止」。
//!
//! 谓词（quiescence）= **排空租约已释放 且 活跃队列已空**——两点同受 `queue`
//! 锁保护，故 [`Condvar`](std::sync::Condvar) 与 `queue` 锁配对。**禁读
//! `pending_actions()`**：#49 的 deferred 停放记录不是活跃排空工作，计入会让
//! 「已静止」在预算止步后永假。
//!
//! 租约释放的两个点（[`super::DrainGuard`] 的正常出口 disarm 与 unwind 臂）
//! 放旗后各 notify 一次——等待面不轮询旗，也没有第二份状态。

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use super::{SyncStore, drain, lock, reentrant_context};

/// [`SyncStore::close_wait`] / [`SyncStore::close_wait_timeout`] 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseWaitOutcome {
    /// 排空至静止：排空租约已释放且活跃队列已空。
    Drained,
    /// 超时仍有排空在跑。**只界调用方等待**——不取消同步 reducer、不声称已停
    /// （超时后那些工作可能仍在推进并提交状态）。
    TimedOutStillRunning,
    /// 本线程正处在**本 store** 的排空帧内（reducer / 订阅回调 / 效果转发中）
    /// ——等待即自等（排空环在等本次调用返回），故立即返回、不等待。
    Reentrant,
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> SyncStore<S, A, Op> {
    /// 关闭准入并等待排空至静止（无超时）。语义见 [`CloseWaitOutcome`]。
    ///
    /// 与 [`Store::close`](skeleton::Store::close) 的差别只有等待：两者都置停机闸并做
    /// best-effort 排空，本方法**额外**等到「租约释放 + 队列空」为止，故他线程
    /// 持租约的竞争路径返回时排空确已结束（那条路 `close` 只是交接）。
    pub fn close_wait(&self) -> CloseWaitOutcome {
        self.wait_quiescent(None)
    }

    /// 关闭准入并等待排空至静止，至多 `timeout`。超时只结束**本次等待**，不
    /// 取消在跑的同步 reducer、也不声称已停。
    pub fn close_wait_timeout(&self, timeout: Duration) -> CloseWaitOutcome {
        self.wait_quiescent(Instant::now().checked_add(timeout))
    }

    fn wait_quiescent(&self, deadline: Option<Instant>) -> CloseWaitOutcome {
        // 同栈重入：本线程正在排空本 store（TLS 排空帧栈），等待即自死锁。
        if reentrant_context(self.inner.store_id).is_some() {
            return CloseWaitOutcome::Reentrant;
        }
        // 闭门翻转与订阅登记互斥（issue#63）：与 `Store::close` 取同一把订阅
        // 闸 ⇒ try_subscribe 与本翻转线性化互斥（不会「已登记但 store 已关」）。
        // 闸在排空前释放：排空/回调期间不持闸。
        {
            let _gate = lock(&self.inner.sub_gate);
            self.inner.closed.store(true, Ordering::Release);
        }
        // 静默态：本调用夺租约并排至 idle，随后谓词立即可满足；竞争态：只交接
        // 即返（他线程的循环必达空），本方法接着进等待。
        drain(&self.inner);
        let mut queue = lock(&self.inner.queue);
        loop {
            if !self.inner.draining.load(Ordering::Acquire) && queue.is_empty() {
                return CloseWaitOutcome::Drained;
            }
            queue = match deadline {
                None => self
                    .inner
                    .stop_wait
                    .wait(queue)
                    .expect(
                        "queue 锁中毒（与本模块 lock() 同纪律：毒锁不静默吞，见 Store 文档「Panic 契约」）",
                    ),
                Some(end) => {
                    let now = Instant::now();
                    if now >= end {
                        return CloseWaitOutcome::TimedOutStillRunning;
                    }
                    let (queue, _) = self
                        .inner
                        .stop_wait
                        .wait_timeout(queue, end - now)
                        .expect(
                        "queue 锁中毒（与本模块 lock() 同纪律：毒锁不静默吞，见 Store 文档「Panic 契约」）",
                    );
                    // 超时也回环复检谓词一次（放旗可能恰在最后一刻发生）。
                    queue
                }
            };
        }
    }
}
