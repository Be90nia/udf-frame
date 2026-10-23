//! 令牌桶限流：突发预算 + 恒速回填，Clock 注入。
//!
//! 常驻零依赖件：纯 std，无 feature 门。限流域对标 Guava `RateLimiter`
//!（SmoothBursty）的「突发许可 + 恒速补充」语义面——warmup 平滑
//!（SmoothWarmingUp）不做（YAGNI 最小面，契约拍板）。时间面复用
//! [`crate::clock`] 单调面，阻塞等待复用 [`crate::retry`] 的 [`Sleeper`]
//!（测量与睡眠同钟绑定：等待时长从注入钟测得、再交同一钟睡眠，测试
//! 错配在类型层不可表达——retry 预算闸同款 armor）。
//!
//! 姿态（骨架哲学 fail-visible/显式/禁静默）：
//!
//! - **满桶启动（拍板点）**：构造即满 `capacity` 个令牌——教材标准令牌桶
//!   语义，`capacity` 就是突发预算上限。Guava 构造后 `storedPermits = 0`
//!   且首次 acquire 走「免费票 + 未来债」（nextFreeTicket 语义），为其
//!   warmup 家族设计的一部分；本件无 warmup，故取可预测的标准语义
//!   （`capacity=n` → 构造后立即可 `try_acquire(n)`），与 Guava 的差异
//!   在此文档化；
//! - **try_acquire / acquire 共享同一结算内核**：锁内惰性回填 + 判定 +
//!   扣减一次完成（线性化点 = 临界区内回填后判定通过那一刻），两入口
//!   判定零漂移；
//! - **acquire 阻塞形态 = 锁外睡到再查（拍板点）**：缺口时长锁内算完
//!   即放锁，[`Sleeper::sleep`] 足额睡到令牌应足时刻，醒来重查循环——
//!   **零自旋**（每轮真实 park，睡粒度下限 1ns 防忙转）；不用 Condvar：
//!   令牌无归还语义，无外部回填源，「到点醒」就是主唤醒路径，notify
//!   面纯属多余（std 文档坑：notify 与 wait 竞态丢失需超时兜底——兜底
//!   即本体的设计里这个坑结构性不存在）；
//! - **acquire(0) = 恒成功 no-op**（拍板点）：零令牌请求无资源语义，
//!   零等待零消耗。Guava `acquire(0)` 可能阻塞到下一免费票（其债务票
//!   实现的泄漏），本件不采纳；
//! - **acquire(n>capacity) 立即拒绝**（fail-visible）：等待也无法满足，
//!   静默无限等 = 活锁面；阻塞版与快路径同判 `Err(ExceedsCapacity)`；
//! - **公平性文档化**：多 acquirer 竞争非严格 FIFO（无排队序），活性
//!   保证——令牌持续供给下每个 waiter 每轮睡满自身缺口时长，缺口
//!   单调收敛，有限 acquirer 集合内无永久饥饿；严格 FIFO 预约队列
//!   （Guava 单线程票序的多线程化）YAGNI 不做。
//!
//! 坑面 armor（每坑一测，见 `tests/ratelimit.rs`）：
//!
//! | 坑 | 来源 | 堵法 | 测试名 |
//! |---|---|---|---|
//! | 首桶不满 / 首票免费语义混淆 | Guava SmoothRateLimiter 构造注释（storedPermits=0） | 满桶启动，文档化差异 | `initial_bucket_is_full_up_to_capacity` |
//! | 时间回拨 | NTP 挂钟回拨 | 只消费单调面 `now_instant`（clock armor #19；MockClock 后退类型层不可表达） | 结构性排除 |
//! | 回填精度积累 | IEEE754 f64 结转雪球 | 回填 = 从 `last` 起点乘法重算 `min(cap, t + e·rate)`，单步误差 ≤1ulp 不跨步积累；禁 `tokens +=` 增量漂移写法 | `fractional_refill_accumulates_exactly` |
//! | 巨步回填溢出 | clock.rs armor：Instant+巨大 Duration 平台 panic | 回填走差值 `elapsed`，从不对时刻做加法；封顶 `min` 吸收任意大步 | `huge_advance_saturates_at_capacity` |
//! | 睡眠持锁 | 并发常识（阻塞他人 try_acquire） | 缺口时长锁内算完即 drop guard，sleep 在锁外 | `blocked_acquire_does_not_hold_lock` |
//! | 醒后忙转（wait 下溢 0 → sleep(0) 自旋） | std `from_secs_f64` 小值截断 | 睡粒度下限 1ns（`wait.max(1ns)`） | `acquire_recheck_rounds_are_bounded` |
//! | acquire(n>cap) 无限等 | Guava permits 无上限面 | 立即 `Err(ExceedsCapacity)` | `acquire_above_capacity_rejects_immediately` |
//! | acquire(0) 阻塞 | Guava `acquire(0)` 文档 | 恒 Ok no-op | `acquire_zero_is_noop` |
//! | 非法构造参数 | fail-closed 纪律（RetryPolicy 同款） | capacity/rate ≤0、NaN、inf 构造期 panic | `invalid_constructor_args_panic` |
//! | 阻塞等待真实 sleep 不可测 | tokio auto-advance 反面教训 | [`Sleeper`] 注入，MockClock 实现=advance：acquire 自动推进虚拟钟，时间线零 sleep 逐纳秒断言 | `acquire_sleeps_exactly_until_tokens_sufficient` |
//! | 测量与睡眠异钟漂移 | retry 预算漏计先例 | [`Sleeper`] 超 trait 绑 [`Clock`]，单实例单时间线 | 类型层排除 |
//!
//! # Examples
//!
//! MockClock 确定性令牌时间线（零真实 sleep）：
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use skeleton::clock::MockClock;
//! use std_ext::ratelimit::TokenBucket;
//!
//! let clock = Arc::new(MockClock::new());
//! // 桶容量 2、每秒回填 1：构造即满桶，突发 2 立即可取。
//! let bucket = TokenBucket::new(clock.clone(), 2.0, 1.0);
//! bucket.try_acquire(2).expect("首桶满：突发预算立即可取");
//!
//! // 空桶后取 1 需恰好 1s 回填——虚拟推进，真实零等待。
//! bucket.acquire(1).expect("acquire 阻塞到令牌足额");
//! assert_eq!(clock.unix_offset(), Duration::from_secs(1));
//!
//! // 半秒只回填 0.5 个：不足额，快路径拒绝并给出可满足时刻。
//! clock.advance(Duration::from_millis(500));
//! let err = bucket.try_acquire(1).unwrap_err();
//! assert_eq!(err.wait(), Duration::from_millis(500)); // 缺 0.5 ÷ 1/s
//! ```
//!
//! log-embed 埋点（`log-embed` feature，默认关=宏消除零开销）：target
//! `skeleton::ratelimit::{try_acquire,acquire}`。级别规范（骨架默认：正常
//! 流转=debug / 异常路径=warn / 终态=info）：快路径拒绝与超容量拒绝（异常
//! 路径「拒因」）=warn（available/wait_ns/capacity 字段带拒因细节）；阻塞
//! 等待分段与醒后重查（正常流转）=debug，round 字段（第几轮等待）+deficit/
//! wait_ns 承载分段与跨事件关联；成功扣减零事件（只埋决策点）。

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::retry::Sleeper;

/// 快路径拒绝：令牌不足额（等待 `wait` 后可满足——供调用方安排重试时刻）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TryAcquireError {
    /// 请求超过桶容量：等待也无法满足，重试无意义。
    ExceedsCapacity(ExceedsCapacity),
    /// 令牌暂不足：现有 `available` 个，约 `wait` 后可达 `requested`。
    Insufficient {
        /// 判定时刻的现有令牌数（回填后、扣减前口径）。
        available: f64,
        /// 令牌足额的可满足时刻（按恒速回填估算；≥1ns）。
        wait: Duration,
    },
}

impl TryAcquireError {
    /// 令牌足额前还需等待的时长（`ExceedsCapacity` 无此语义，返回 `None`）。
    #[must_use]
    pub fn wait(&self) -> Duration {
        match *self {
            Self::Insufficient { wait, .. } => wait,
            Self::ExceedsCapacity(_) => Duration::ZERO,
        }
    }
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExceedsCapacity(e) => write!(f, "{e}"),
            Self::Insufficient { available, wait } => write!(
                f,
                "令牌不足：现有 {available}，约 {} 后可满足（fail-visible 提示，非错误详情）",
                crate::clock::format_duration(*wait)
            ),
        }
    }
}

impl std::error::Error for TryAcquireError {}

/// 请求超过桶容量：等待也无法满足（阻塞版立即拒绝，禁静默无限等）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExceedsCapacity {
    /// 请求的令牌数。
    pub requested: u32,
    /// 桶容量（突发预算上限）。
    pub capacity: f64,
}

impl fmt::Display for ExceedsCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "请求 {} 超过桶容量 {}：等待也无法满足，立即拒绝（fail-visible）",
            self.requested, self.capacity
        )
    }
}

impl std::error::Error for ExceedsCapacity {}

/// 桶状态胞：令牌余额 + 上次结算时刻（回填起算点）。
struct BucketState {
    tokens: f64,
    last: std::time::Instant,
}

/// 令牌桶限流器：突发预算 `capacity` + 恒速回填 `refill_rate`（个/秒）。
///
/// 线程安全（`Send + Sync`）：状态在 `Mutex` 内，快路径单次临界区。
/// 多线程共享经 `Arc<TokenBucket>`。
pub struct TokenBucket {
    clock: Arc<dyn Sleeper>,
    capacity: f64,
    refill_rate: f64,
    state: Mutex<BucketState>,
}

impl fmt::Debug for TokenBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 时间源不打（MockClock 面含构造锚真实时刻，Debug 输出须确定性）。
        f.debug_struct("TokenBucket")
            .field("capacity", &self.capacity)
            .field("refill_rate", &self.refill_rate)
            .finish_non_exhaustive()
    }
}

impl TokenBucket {
    /// 新桶：容量 `capacity`、回填速率 `refill_rate` 个/秒，**构造即满桶**。
    ///
    /// # Panics
    ///
    /// 构造期 fail-closed（测试写错了就炸，不静默；RetryPolicy 同款纪律）：
    ///
    /// - `capacity <= 0`、NaN、inf：零容量桶任何请求都不可满足，静默通过
    ///   = 死桶；
    /// - `refill_rate <= 0`、NaN、inf：零速率=永不回填的死桶；无穷速率
    ///   =「无限流」，不该经限流器表达。
    #[must_use]
    pub fn new(clock: Arc<dyn Sleeper>, capacity: f64, refill_rate: f64) -> Self {
        assert!(
            capacity.is_finite() && capacity > 0.0,
            "capacity 必须为正有限值（零/负容量=死桶，NaN/inf 非法），得 {capacity}"
        );
        assert!(
            refill_rate.is_finite() && refill_rate > 0.0,
            "refill_rate 必须为正有限值（零速率=永不回填死桶，inf=无限流不该走限流器），得 {refill_rate}"
        );
        let anchor = clock.now_instant();
        Self {
            clock,
            capacity,
            refill_rate,
            state: Mutex::new(BucketState {
                tokens: capacity,
                last: anchor,
            }),
        }
    }

    /// 桶容量（突发预算上限，个）。
    #[must_use]
    pub fn capacity(&self) -> f64 {
        self.capacity
    }

    /// 回填速率（个/秒）。
    #[must_use]
    pub fn refill_rate(&self) -> f64 {
        self.refill_rate
    }

    /// 非阻塞快路径：取 `permits` 个令牌，不足额立即拒绝（含可满足时刻）。
    ///
    /// `permits == 0` 恒成功（no-op）。线性化点 = 临界区内回填后判定通过
    /// 那一刻。
    pub fn try_acquire(&self, permits: u32) -> Result<(), TryAcquireError> {
        if permits == 0 {
            return Ok(());
        }
        let n = f64::from(permits);
        if n > self.capacity {
            embed_event!(warn, target: "skeleton::ratelimit::try_acquire", permits = permits, capacity = self.capacity, reason = "exceeds_capacity");
            return Err(TryAcquireError::ExceedsCapacity(ExceedsCapacity {
                requested: permits,
                capacity: self.capacity,
            }));
        }
        let available = self.settle_and_take(n);
        match available {
            Ok(()) => Ok(()),
            Err(tokens) => {
                let wait = Self::wait_for(n - tokens, self.refill_rate);
                embed_event!(warn, target: "skeleton::ratelimit::try_acquire", permits = permits, available = tokens, wait_ns = wait.as_nanos() as u64, reason = "insufficient");
                Err(TryAcquireError::Insufficient {
                    available: tokens,
                    wait,
                })
            }
        }
    }

    /// 阻塞版：取 `permits` 个令牌，不足额则睡到可满足时刻再查（零自旋：
    /// 每轮真实 park，睡粒度下限 1ns）。
    ///
    /// 睡眠绝不持锁——缺口时长锁内算完即放锁，他人 try_acquire 不被阻塞。
    ///
    /// # Errors
    ///
    /// 仅 `permits > capacity` 时返回 [`ExceedsCapacity`]（等待也无法满足，
    /// 立即拒绝）。
    pub fn acquire(&self, permits: u32) -> Result<(), ExceedsCapacity> {
        if permits == 0 {
            return Ok(());
        }
        let n = f64::from(permits);
        if n > self.capacity {
            embed_event!(warn, target: "skeleton::ratelimit::acquire", permits = permits, capacity = self.capacity, reason = "exceeds_capacity");
            return Err(ExceedsCapacity {
                requested: permits,
                capacity: self.capacity,
            });
        }
        // 睡到再查：缺口单调收敛（无归还语义下令牌只增不减，减只发生在
        // 自己扣减），每轮睡满自身缺口，有限 acquirer 集合内必终止。
        #[cfg(feature = "log-embed")]
        let mut round = 0u64;
        loop {
            let deficit = match self.settle_and_take(n) {
                Ok(()) => return Ok(()),
                Err(tokens) => n - tokens,
            };
            // guard 已 drop：睡眠在锁外。
            let wait = Self::wait_for(deficit, self.refill_rate);
            // 阻塞等待分段（正常流转 debug）：round=第几轮等待，deficit/
            // wait_ns 带缺口与睡眠时长（醒后重查即下一段的唤醒留痕）。
            #[cfg(feature = "log-embed")]
            {
                round += 1;
                embed_event!(debug, target: "skeleton::ratelimit::acquire", permits = permits, round = round, deficit = deficit, wait_ns = wait.as_nanos() as u64);
            }
            self.clock.sleep(wait);
        }
    }

    /// 结算内核（try/acquire 共享，判定零漂移）：锁内惰性回填后判定，
    /// 足额即扣减返回 `Ok(())`；不足额放锁并返回现有余数供上游报错。
    fn settle_and_take(&self, n: f64) -> Result<(), f64> {
        let mut s = self.lock();
        let now = self.clock.now_instant();
        // 回填 = 从上次结算点乘法重算（误差 ≤1ulp 不跨步积累），封顶吸收
        // 任意大步。单调钟上 now >= last 恒成立（checked 显式化，clock armor）。
        let elapsed = now
            .checked_duration_since(s.last)
            .expect("单调钟上 now >= last 恒成立（MockClock 后退类型层不可表达）");
        if !elapsed.is_zero() {
            s.tokens = (s.tokens + elapsed.as_secs_f64() * self.refill_rate).min(self.capacity);
            s.last = now;
        }
        if s.tokens >= n {
            s.tokens -= n;
            Ok(())
        } else {
            Err(s.tokens)
        }
    }

    /// 缺口时长：`deficit / rate` 秒，下限 1ns（`from_secs_f64` 小值截断为
    /// 0 → sleep(0) 忙转；下限保底真实 park，SystemClock 面平台粒度过睡
    /// 无害——重查时余量只多不少）。
    fn wait_for(deficit: f64, refill_rate: f64) -> Duration {
        Duration::from_secs_f64(deficit / refill_rate).max(Duration::from_nanos(1))
    }

    /// 毒锁恢复：状态胞在持锁内无 panic 面（回填纯算术、扣减无越界），
    /// 毒化仅可能来自调用方自带的 op panic——状态无不变量可破坏，直接
    /// into_inner 与 clock.rs/scheduler 先例同款。
    fn lock(&self) -> MutexGuard<'_, BucketState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
