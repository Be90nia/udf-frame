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
    ///
    /// **准入已关**：闭门先于重入判定，本结果只表示「不能等」，不表示「没关门」
    /// ——此后外部准入一律得 `AdmissionOutcome::Closed`。
    Reentrant,
}

impl<S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static> SyncStore<S, A, Op> {
    /// 关闭准入并等待排空至静止（无超时）。语义见 [`CloseWaitOutcome`]。
    ///
    /// **闭门先于重入判定**：即使同栈重入返回 [`CloseWaitOutcome::Reentrant`]，
    /// 准入也已关闭——重入只跳过自等，不跳过闭门。
    ///
    /// 与 [`Store::close`](skeleton::Store::close) 的差别只有等待：两者都置停机闸并做
    /// best-effort 排空，本方法**额外**等到「租约释放 + 队列空」为止，故他线程
    /// 持租约的竞争路径返回时排空确已结束（那条路 `close` 只是交接）。
    pub fn close_wait(&self) -> CloseWaitOutcome {
        self.wait_quiescent(WaitDeadline::Forever)
    }

    /// 关闭准入并等待排空至静止，至多 `timeout`。超时只结束**本次等待**，不
    /// 取消在跑的同步 reducer、也不声称已停。
    ///
    /// 与 [`Self::close_wait`] 同序：**闭门先于重入判定**（`Reentrant` 时准入已关）。
    ///
    /// **有限超时恒有限**（#122）：`timeout` 大到 `Instant` 表示不了（如
    /// `Duration::MAX`）时饱和到可达的最远截止时刻（`saturating_deadline`），
    /// 不退化成 [`Self::close_wait`] 的无界等待。零 `timeout` = 立即做一次谓词
    /// 检查（已静止即 `Drained`，否则 `TimedOutStillRunning`）。
    pub fn close_wait_timeout(&self, timeout: Duration) -> CloseWaitOutcome {
        self.wait_quiescent(finite_deadline(timeout))
    }

    fn wait_quiescent(&self, deadline: WaitDeadline) -> CloseWaitOutcome {
        // 闭门翻转与订阅登记互斥（issue#63）：与 `Store::close` 取同一把订阅
        // 闸 ⇒ try_subscribe 与本翻转线性化互斥（不会「已登记但 store 已关」）。
        // 闸在排空前释放：排空/回调期间不持闸。
        {
            let _gate = lock(&self.inner.sub_gate);
            self.inner.closed.store(true, Ordering::Release);
        }
        // 闭门**先于**重入判定（#64 06:59 定稿）：同栈重入只跳过自等（排空环在
        // 等本次调用返回，等待即死锁），不跳过闭门——`Reentrant` 是合法返回值，
        // 不能把「未闭门」静默留给调用方。
        if reentrant_context(self.inner.store_id).is_some() {
            return CloseWaitOutcome::Reentrant;
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
                WaitDeadline::Forever => self
                    .inner
                    .stop_wait
                    .wait(queue)
                    .expect(
                        "queue 锁中毒（与本模块 lock() 同纪律：毒锁不静默吞，见 Store 文档「Panic 契约」）",
                    ),
                WaitDeadline::Until(end) => {
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

/// 等待上限（#122）：两种状态在类型上分开——`None` 不再同时表示「调用方要
/// 无限等」与「deadline 算术溢出」。
#[derive(Clone, Copy)]
enum WaitDeadline {
    /// 调用方明确无超时（[`SyncStore::close_wait`] 专有）。
    Forever,
    /// 有限超时的截止时刻，**必可表示**。
    Until(Instant),
}

/// 有限超时（[`SyncStore::close_wait_timeout`]）的等待上限：**恒为 `Until`**
/// ——`Forever` 只能由 [`SyncStore::close_wait`] 构造（#122：`None` 一个值不再
/// 同时表示「调用方要无限等」与「deadline 算术溢出」）。
fn finite_deadline(timeout: Duration) -> WaitDeadline {
    WaitDeadline::Until(saturating_deadline(timeout))
}

/// 有限超时的截止时刻；溢出时**饱和**到可表示的最远点（#122）——宁可等到
/// 平台时间轴尽头，也不能把「有限」静默变成「无限」。
///
/// `Instant` 未公开上界 ⇒ 折半逼近：`now + lo` 可表示、`now + timeout` 不可
/// 表示，每轮把区间折半；`Duration` 纳秒量级 ≤2^64 ⇒ 64 轮必然收到 1ns 内。
/// 只在溢出臂付这点算术（常见路径 `checked_add` 成功即返回）。
fn saturating_deadline(timeout: Duration) -> Instant {
    let now = Instant::now();
    if let Some(exact) = now.checked_add(timeout) {
        return exact;
    }
    // 不变量：`now + lo` 可表示、`now + hi` 不可表示。
    let (mut lo, mut hi) = (Duration::ZERO, timeout);
    for _ in 0..64 {
        if hi - lo <= Duration::from_nanos(1) {
            break;
        }
        let mid = lo + (hi - lo) / 2;
        if now.checked_add(mid).is_some() {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    now.checked_add(lo)
        .expect("lo 全程只取 checked_add 可表示的值（循环不变量）")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #122：有限超时分支**恒**产出可表示截止——`Duration::MAX` 不得落回
    /// 「无截止」（旧实现 `checked_add` 溢出 → `None` → 永久等待）。
    #[test]
    fn runtime_finite_timeout_never_yields_forever() {
        assert!(matches!(
            finite_deadline(Duration::MAX),
            WaitDeadline::Until(_)
        ));
        assert!(matches!(
            finite_deadline(Duration::ZERO),
            WaitDeadline::Until(_)
        ));
    }

    /// #122：有限超时恒给出可表示截止——`Duration::MAX` 不得落回「无截止」。
    #[test]
    fn runtime_finite_timeout_saturates_instead_of_going_unbounded() {
        assert!(
            Instant::now().checked_add(Duration::MAX).is_none(),
            "前置：Duration::MAX 在平台 Instant 上必须不可表示，否则本测试无鉴别力"
        );
        let now = Instant::now();
        let deadline = saturating_deadline(Duration::MAX);
        // 有截止 ⇒ 语义是「有限」；且远在未来 ⇒ 不退化成「立即超时」。
        assert!(
            deadline - now >= Duration::from_secs(3600),
            "饱和值必须是远期截止时刻，不得退化成立即超时"
        );
    }

    /// 常见路径不被折半污染：可表示的超时原样给出。
    #[test]
    fn runtime_representable_timeout_is_exact() {
        let now = Instant::now();
        assert!(saturating_deadline(Duration::from_secs(5)) >= now + Duration::from_secs(5));
    }
}
