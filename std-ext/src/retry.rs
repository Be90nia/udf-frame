//! 重试编排：政策显式、确定性退避、fail-visible 耗尽回灌；同步/异步双面。
//!
//! 常驻零依赖件：纯 std，无 feature 门。行为面的「编排半」——退避计算在
//! [`crate::backoff`]（只算延迟），时间源在 [`skeleton::clock`]，本件把三者接
//! 成循环。同步面 [`retry`]/[`retry_if`]，异步面 [`retry_async`]/
//! [`retry_if_async`]——调用形 `retry::retry(...)` 等，函数与类型均经模块
//! 路径使用，不进 crate 根 re-export（避免与模块名撞名）。
//!
//! 姿态（骨架哲学 fail-visible/显式策略/禁静默）：
//!
//! - **政策显式**：次数/退避/预算在 [`RetryPolicy`] 构造期一次定死，非法值
//!   构造期 fail-closed panic（测试写错了就炸，不静默）——零退避=忙等风暴，
//!   零次数=静默不执行，都不放行；
//! - **可重试判定显式可选**：[`retry`] 默认全可重试；按错误类分流走
//!   [`retry_if`]（判定器独立参数，不藏进政策对象——政策保持 `Copy`、
//!   判定器不装箱）；
//! - **耗尽回灌标准形**：[`RetryError`] 带 `attempts` + `last`（backoff.rs
//!   预告的标准形），末次错误原样在手，禁包装吞类型；
//! - **panic 非 error**：op panic 原样传播（同步同线程无任务边界，转 Err
//!   反而污染语义）；panic 不计次、不重试、不睡眠；
//! - **full jitter 内建禁关**（issue#1-2 armor ①；AWS "Exponential Backoff
//!   and Jitter"：无抖动的同步节拍=重试风暴）：退避值算出后经政策内抖动源
//!   映射再睡，默认 [`full_jitter`]（std `RandomState` 派生，非密码学熵面）；
//!   [`RetryPolicy::with_jitter`] 注入恒等源换回确定性回放（MockClock 测试
//!   口径）。[`crate::backoff`] 仍纯确定性——抖动只发生在编排层；
//! - **异步取消安全**：[`AsyncSleeper`] 零三方（RPITIT+Send，不留 tokio），
//!   运行时定时器由调用方对接；sleep 内联 await——外层 future drop 即全停
//!   （在途 sleep future 连带 drop，无 spawn 逃逸，armor 表钉死）；
//! - **幂等假定红线**：retry 默认假定 op 幂等；非幂等操作必须自备显式 ack
//!   路径（如 idempotency key 重签，Stripe 先例），否则重试=双扣/双写。
//!
//! 坑面 armor（每坑一测，见 `tests/retry.rs`）：
//!
//! | 坑 | armor |
//! |---|---|
//! | 无限重试 | 次数闸构造期必填 ≥1，先于睡眠裁决 |
//! | 预算漏计 | 预算测量与退避睡眠同钟（[`Sleeper`] 超 trait 绑 [`Clock`]，错配在类型层不可表达）；异步面预算口径差异见 [`retry_async`] 文档 |
//! | 不可重试错误重试 | 判定器先于睡眠，拒绝即停 |
//! | 退避为零风暴 | base=0 构造期拒绝 |
//! | 只配次数不配预算=风暴面 | 文档红线（[`RetryPolicy::with_budget`]）；双闸先到先停（`deadline_and_attempts_both_bound_retries`） |
//! | 无 jitter 同步节拍→重试风暴 | full jitter 内建禁关、源=RandomState 派生（`full_jitter_within_half_open_range` + `retry_delays_are_jittered`） |
//! | 取消后仍重试 | sleep 内联 await，外层 future drop 即全停（`dropped_retry_stops_entirely`）；sync 循环被次数/预算双闸强制有界 |
//! | 真实 sleep 不可测 | [`Sleeper`] 注入，[`MockClock`] 实现=虚拟推进，时间线零 sleep 逐毫秒断言 |
//! | panic 当错误重试 | panic 原样传播（见上） |
//! | 非幂等操作被盲重试 | 文档红线：retry 默认假定幂等，非幂等需显式 ack 路径（issue#1-2 定案：本批落文档，不落类型门） |
//! | 末次错误被吞 | Exhausted/OutOfBudget 原样回灌 last + into_last（`last_error_is_reported_not_swallowed`） |
//!
//! 裁决序（每次失败后、睡眠前）：次数 → 可重试 → 预算；不为不会发生的
//! 重试付出等待。
//!
//! log-embed 埋点（`log-embed` feature，默认关=宏消除零开销）：target
//! `skeleton::retry::{attempt,verdict,backoff}`。级别规范（骨架默认：正常
//! 流转=debug / 异常路径=warn / 终态=info）：尝试开始=debug；重试放行
//! （异常路径「重试发生」）=warn，backoff_ms 字段带退避时长；停机终态
//! （次数到顶/判定拒绝/预算耗尽）=info；退避截平（base>max 线性值越过
//! 上限）=warn（target `...::backoff`，linear_ms→capped_ms）。attempt 序号
//! 进字段承载循环内跨事件关联，不起 span。
//!
//! 性能锚（2026-10-16 批 2-b 定性，release/std Instant 粗测，详见
//! reports/2026-10-16-b2-retry.md）：不可重试快路径零分配零睡眠（判定即返，
//! 循环体无 Box/Vec/String）；首试成功编排税 ~11ns/次、不可重试快路径
//! ~11ns/次、单次重试（含一次 MockClock 虚拟睡眠）~25ns/次；④-⑦ n/a
//! （纯 std 同步域，无并发/内存维度）。

use std::collections::hash_map::RandomState;
use std::fmt;
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::thread;
use std::time::{Duration, Instant};

use crate::backoff::backoff_delay;
use crate::clock::format_duration;
use skeleton::clock::{Clock, MockClock, SystemClock};

/// 睡眠口：退避等待唯一通道，与读钟同源绑定（超 trait [`Clock`]——预算
/// 测量与退避睡眠恒同一时间线，测试错配在类型层不可表达）。
///
/// 生产用 [`SystemClock`]（线程阻塞），测试注入 [`MockClock`]（虚拟推进，
/// 零真实 sleep，时间线逐纳秒可断言）。
pub trait Sleeper: Clock {
    /// 足额等待 `d`（欠睡=退避语义失真；实现禁静默缩短）。
    fn sleep(&self, d: Duration);
}

impl Sleeper for SystemClock {
    fn sleep(&self, d: Duration) {
        thread::sleep(d);
    }
}

impl Sleeper for MockClock {
    fn sleep(&self, d: Duration) {
        self.advance(d);
    }
}

/// 抖动源：把政策退避值映射为实际睡眠时长（重试函数睡前调用一次）。
///
/// 非捕获闭包与 `fn` 项自动 coercing——测试注入恒等源写 `.with_jitter(|d| d)`。
pub type JitterFn = fn(Duration) -> Duration;

/// full jitter（默认抖动源）：均匀映射进 `[0, d)` 半开区间（AWS "Exponential
/// Backoff and Jitter" 公式的 `sleep = random(0, d)` 形）。
///
/// 熵源 = std `RandomState`（每实例随机种子，零三方）；**非密码学熵面**，
/// 勿用于任何安全用途。`d` 为零返回零；纳秒收窄按 Duration 不变量恒无损
/// （内部即 u64 纳秒存储，`try_from` 失败分支不可达）。
///
/// ```
/// use std::time::Duration;
/// use std_ext::retry::full_jitter;
///
/// let d = Duration::from_millis(100);
/// let j = full_jitter(d);
/// assert!(j < d, "full jitter 恒在 [0, d) 半开区间内");
/// assert_eq!(full_jitter(Duration::ZERO), Duration::ZERO);
/// ```
#[must_use]
pub fn full_jitter(d: Duration) -> Duration {
    let nanos = u64::try_from(d.as_nanos()).unwrap_or(u64::MAX);
    if nanos == 0 {
        return Duration::ZERO;
    }
    let r = RandomState::new().build_hasher().finish();
    Duration::from_nanos(r % nanos)
}

/// 重试政策：次数/退避/预算三闸，构造期定死、运行期只读（`Copy`）。
///
/// 第 N 次失败后（且裁决放行）睡 `jitter([`backoff_delay`]`(N, base,
/// max_delay)`))`：线性退避饱和截平，再经政策内抖动源映射（默认
/// [`full_jitter`]，内建禁关；[`with_jitter`][RetryPolicy::with_jitter]
/// 注入恒等源=确定性回放）。`base > max_delay` 合法：退避值静默截平到
/// `max_delay`（`backoff_delay` 的 saturating 语义，无害）。
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    max_attempts: u32,
    base: Duration,
    max_delay: Duration,
    budget: Option<Duration>,
    jitter: JitterFn,
}

impl RetryPolicy {
    /// 新政策：最多 `max_attempts` 次执行（**含首试**——首试不是重试）。
    ///
    /// # Panics
    ///
    /// - `max_attempts == 0`：零次执行不是重试语义，静默通过=调用方以为试过；
    /// - `base` 为零：零退避=忙等风暴。
    ///
    /// 构造期 fail-closed（测试写错了就炸，不静默）。
    #[must_use]
    pub fn new(max_attempts: u32, base: Duration, max_delay: Duration) -> Self {
        assert!(
            max_attempts >= 1,
            "max_attempts 必须 ≥ 1（首试非重试；零次执行请勿调用 retry）"
        );
        assert!(
            !base.is_zero(),
            "退避 base 必须非零（零退避=忙等风暴，fail-closed 拒绝）"
        );
        Self {
            max_attempts,
            base,
            max_delay,
            budget: None,
            jitter: full_jitter,
        }
    }

    /// 总预算闸：累计耗时（注入钟口径）≥ `budget` 即停
    /// （[`RetryError::OutOfBudget`]，带命中时实测 elapsed）。
    ///
    /// 预算闸在每次退避睡眠**之前**检查——超预算不再白睡一觉才停。
    /// `budget = 0` 合法且语义明确 = 失败即停（文档化退化形，不是错误）。
    ///
    /// # 红线
    ///
    /// **只配次数不配预算 = 风暴面**：次数闸只封单次调用的重试上界，预算
    /// 闸才封长尾挂死（退避被上层 timeout 提前打断时次数闸形同虚设）——
    /// 生产政策两者都配（backoff crate 的 max_elapsed_time 与次数闸并存即
    /// 双闸先例），先到先停。
    #[must_use]
    pub fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = Some(budget);
        self
    }

    /// 注入抖动源（默认 [`full_jitter`]，无「关闭」开关——注入恒等源
    /// `|d| d` 即确定性回放，供 MockClock 测试口径使用）。
    #[must_use]
    pub fn with_jitter(mut self, jitter: JitterFn) -> Self {
        self.jitter = jitter;
        self
    }
}

/// 耗尽回灌标准形（backoff.rs 预告）：终未成功时次数与末次错误原样在手。
///
/// [`RetryError::Exhausted`] 覆盖「政策决定不再重试」两因——次数到顶
/// （`attempts == max_attempts`）与判定器拒绝（`attempts` = 拒绝时的执行
/// 次数）；两者调用方处置相同（上抛 last），attempts 字段自带区分。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryError<E> {
    /// 次数到顶或判定器拒绝：`attempts` = 实际执行次数，`last` = 末次错误。
    Exhausted {
        /// 实际执行次数。
        attempts: u32,
        /// 末次错误原样回灌（禁包装吞类型）。
        last: E,
    },
    /// 预算闸命中（未到次数上限）：`elapsed` 为命中时累计耗时（注入钟口径）。
    OutOfBudget {
        /// 实际执行次数。
        attempts: u32,
        /// 末次错误原样回灌。
        last: E,
        /// 命中时的累计耗时。
        elapsed: Duration,
    },
}

impl<E> RetryError<E> {
    /// 取出末次错误（最常见的上抛形状）。
    #[must_use]
    pub fn into_last(self) -> E {
        match self {
            Self::Exhausted { last, .. } | Self::OutOfBudget { last, .. } => last,
        }
    }
}

impl<E: fmt::Display> fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { attempts, last } => {
                write!(f, "retry exhausted after {attempts} attempt(s): {last}")
            }
            Self::OutOfBudget {
                attempts,
                last,
                elapsed,
            } => write!(
                f,
                "retry budget exhausted after {attempts} attempt(s), elapsed {}: {last}",
                format_duration(*elapsed)
            ),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for RetryError<E> {}

/// 同步重试（默认全可重试）：按政策执行 `op` 至成功或闸停。
///
/// 按错误类分流用 [`retry_if`]。op panic 原样传播（不转 Err、不计次）。
///
/// # 红线
///
/// retry **默认假定 op 幂等**：非幂等操作必须自备显式 ack 路径（如
/// idempotency key 重签）再交给 retry，否则重试=双扣/双写。
///
/// # Examples
///
/// MockClock 确定性时间线：两次失败后成功，退避 100+200ms 全经虚拟推进，
/// 零真实 sleep，逐毫秒可断言（真实 `thread::sleep` 不推进虚拟钟，此断言
/// 即 sleep 通路的证伪器）。断言逐毫秒确定值须注入恒等抖动源（默认
/// [`full_jitter`] 会把实际睡眠摊进 `[0, d)`）：
///
/// ```
/// use std::time::Duration;
/// use skeleton::clock::MockClock;
/// use std_ext::retry::{retry, RetryPolicy};
///
/// let clock = MockClock::new();
/// let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10))
///     .with_jitter(|d| d);
/// let mut remaining = 2u32;
/// let op = || {
///     if remaining > 0 {
///         remaining -= 1;
///         Err("transient")
///     } else {
///         Ok(7u32)
///     }
/// };
/// assert_eq!(retry(op, &policy, &clock).unwrap(), 7);
/// assert_eq!(clock.unix_offset(), Duration::from_millis(300)); // delay(1)+delay(2)
/// ```
pub fn retry<T, E, F, S>(op: F, policy: &RetryPolicy, sleeper: &S) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Result<T, E>,
    S: Sleeper,
{
    retry_if(op, policy, sleeper, |_| true)
}

/// 同步重试（判定器分流）：`retryable(&err)` 为 false 的错误立即停
/// （快路径：零睡眠，[`RetryError::Exhausted`] 记拒绝时执行次数）。
///
/// 幂等假定与抖动语义同 [`retry`]（默认 [`full_jitter`]，判定放行的重试
/// 才会睡眠）。
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use skeleton::clock::MockClock;
/// use std_ext::retry::{retry_if, RetryError, RetryPolicy};
///
/// let clock = MockClock::new();
/// let policy = RetryPolicy::new(5, Duration::from_millis(10), Duration::from_secs(1));
/// let op = || Err::<u32, &str>("fatal");
/// let res = retry_if(op, &policy, &clock, |e: &&str| *e == "transient");
/// assert!(matches!(res, Err(RetryError::Exhausted { attempts: 1, .. })));
/// assert_eq!(clock.unix_offset(), Duration::ZERO); // 判定器拒绝：零睡眠立停
/// ```
pub fn retry_if<T, E, F, P, S>(
    mut op: F,
    policy: &RetryPolicy,
    sleeper: &S,
    mut retryable: P,
) -> Result<T, RetryError<E>>
where
    F: FnMut() -> Result<T, E>,
    P: FnMut(&E) -> bool,
    S: Sleeper,
{
    let start = sleeper.now_instant();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        embed_event!(debug, target: "skeleton::retry::attempt", attempt = attempts);
        match op() {
            Ok(v) => return Ok(v),
            Err(e) => {
                // 裁决序：次数 → 可重试 → 预算；睡前裁决，不为不会发生的重试等待。
                if attempts >= policy.max_attempts {
                    embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_max_attempts");
                    return Err(RetryError::Exhausted { attempts, last: e });
                }
                if !retryable(&e) {
                    embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_non_retryable");
                    return Err(RetryError::Exhausted { attempts, last: e });
                }
                if let Some(budget) = policy.budget {
                    let elapsed = sleeper
                        .now_instant()
                        .checked_duration_since(start)
                        .expect("单调钟上 start 恒不晚于 now（Sleeper: Clock 契约）");
                    if elapsed >= budget {
                        embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_out_of_budget", elapsed_ms = elapsed.as_millis() as u64);
                        return Err(RetryError::OutOfBudget {
                            attempts,
                            last: e,
                            elapsed,
                        });
                    }
                }
                let delay = backoff_delay(attempts, policy.base, policy.max_delay);
                embed_event!(warn, target: "skeleton::retry::verdict", attempt = attempts, decision = "retry", backoff_ms = delay.as_millis() as u64);
                // 截平（base 线性值 > max）才发：saturating 语义下的静默截平
                // 是异常路径留痕点（契约「base>cap 截平发生」）。
                #[cfg(feature = "log-embed")]
                if policy.base.saturating_mul(attempts) > delay {
                    embed_event!(warn, target: "skeleton::retry::backoff", attempt = attempts, linear_ms = policy.base.saturating_mul(attempts).as_millis() as u64, capped_ms = delay.as_millis() as u64);
                }
                // 抖动在睡前应用（full jitter 内建禁关）：backoff_ms 字段
                // 记政策退避值（确定性观测口径），实际睡眠=抖动映射值。
                sleeper.sleep((policy.jitter)(delay));
            }
        }
    }
}

/// 异步睡眠口：退避等待唯一通道（issue#1-2）。**零三方，不留 tokio**——
/// 运行时定时器由调用方对接（如 tokio 门内包一层 `tokio::time::sleep`）。
///
/// 契约：返回的 future 必须**真异步挂起**（轮询驱动型阻塞实现会拖死执行
/// 器且破坏取消安全，禁）；实现禁静默缩短时长（欠睡=退避语义失真）。
pub trait AsyncSleeper {
    /// 异步足额等待 `d`。
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send;
}

/// 异步重试（默认全可重试）：语义与同步 [`retry`] **逐字同**——同一
/// [`RetryPolicy`]、同一裁决序（次数 → 可重试 → 预算）、同一退避映射
/// （含默认 [`full_jitter`]）、同一耗尽回灌 [`RetryError`] 形（into_last
/// 在手）。op panic 同样原样传播（不转 Err、不计次）。
///
/// 与同步面的三处差异，各自有因：
///
/// - **op 收 attempt 序号**（`FnMut(u32)`，自 1 起=首试）：重试侧可按轮次
///   换凭据（如 idempotency key 重签——幂等红线同 [`retry`]，非幂等操作
///   必须自备显式 ack 路径）；
/// - **取消安全**：退避 sleep 内联 await 在本函数返回的 future 里——外层
///   future drop 即全停（在途 sleep future 连带 drop，之后不再睡眠不再
///   重试），无 spawn 逃逸；
/// - **预算闸读真实墙钟**（`std::time::Instant`）：[`AsyncSleeper`] 契约只
///   注入睡眠不含读钟，预算口径=真实耗时（同步面的注入钟口径此处不可达；
///   预算红线同 [`RetryPolicy::with_budget`]：只配次数不配预算=风暴面）。
///
/// # Examples
///
/// ```no_run
/// use std::future::{ready, Future};
/// use std::time::Duration;
/// use std_ext::retry::{retry_async, AsyncSleeper, RetryPolicy};
///
/// // 零依赖演示面：即刻就绪的睡眠口（真实运行时请对接其定时器）。
/// struct ReadySleeper;
/// impl AsyncSleeper for ReadySleeper {
///     fn sleep(&self, _d: Duration) -> impl Future<Output = ()> + Send {
///         ready(())
///     }
/// }
///
/// # async fn demo() {
/// let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10));
/// let mut remaining = 2u32;
/// let op = |attempt: u32| {
///     let _ = attempt; // 首试=1；可按轮次换凭据
///     let done = remaining == 0;
///     if !done { remaining -= 1; }
///     async move { if done { Ok(7u32) } else { Err("transient") } }
/// };
/// let v = retry_async(&policy, op, ReadySleeper).await.unwrap();
/// assert_eq!(v, 7);
/// # }
/// ```
pub async fn retry_async<F, Fut, T, E, S>(
    policy: &RetryPolicy,
    op: F,
    sleep: S,
) -> Result<T, RetryError<E>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    S: AsyncSleeper,
{
    retry_if_async(policy, op, sleep, |_| true).await
}

/// 异步重试（判定器分流）：`retryable(&err)` 为 false 的错误立即停（快路径：
/// 零睡眠）。语义与同步 [`retry_if`] 逐字同；差异面（attempt 序号/取消安全/
/// 预算口径）见 [`retry_async`]。
///
/// # Examples
///
/// ```no_run
/// use std::future::{ready, Future};
/// use std::time::Duration;
/// use std_ext::retry::{retry_if_async, AsyncSleeper, RetryError, RetryPolicy};
///
/// struct ReadySleeper;
/// impl AsyncSleeper for ReadySleeper {
///     fn sleep(&self, _d: Duration) -> impl Future<Output = ()> + Send {
///         ready(())
///     }
/// }
///
/// # async fn demo() {
/// let policy = RetryPolicy::new(5, Duration::from_millis(10), Duration::from_secs(1));
/// let op = |_| async { Err::<u32, &str>("fatal") };
/// let res = retry_if_async(&policy, op, ReadySleeper, |e: &&str| *e == "transient").await;
/// assert!(matches!(res, Err(RetryError::Exhausted { attempts: 1, .. })));
/// # }
/// ```
pub async fn retry_if_async<F, Fut, T, E, P, S>(
    policy: &RetryPolicy,
    mut op: F,
    sleep: S,
    mut retryable: P,
) -> Result<T, RetryError<E>>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, E>>,
    P: FnMut(&E) -> bool,
    S: AsyncSleeper,
{
    let start = Instant::now();
    let mut attempts = 0u32;
    loop {
        attempts += 1;
        embed_event!(debug, target: "skeleton::retry::attempt", attempt = attempts);
        match op(attempts).await {
            Ok(v) => return Ok(v),
            Err(e) => {
                // 裁决序与同步面逐字同：次数 → 可重试 → 预算；睡前裁决。
                if attempts >= policy.max_attempts {
                    embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_max_attempts");
                    return Err(RetryError::Exhausted { attempts, last: e });
                }
                if !retryable(&e) {
                    embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_non_retryable");
                    return Err(RetryError::Exhausted { attempts, last: e });
                }
                if let Some(budget) = policy.budget {
                    let elapsed = Instant::now()
                        .checked_duration_since(start)
                        .expect("单调钟上 start 恒不晚于 now（std Instant 单调契约）");
                    if elapsed >= budget {
                        embed_event!(info, target: "skeleton::retry::verdict", attempt = attempts, decision = "stop_out_of_budget", elapsed_ms = elapsed.as_millis() as u64);
                        return Err(RetryError::OutOfBudget {
                            attempts,
                            last: e,
                            elapsed,
                        });
                    }
                }
                let delay = backoff_delay(attempts, policy.base, policy.max_delay);
                embed_event!(warn, target: "skeleton::retry::verdict", attempt = attempts, decision = "retry", backoff_ms = delay.as_millis() as u64);
                // 截平（base 线性值 > max）才发：与同步面同口径（抖动前值）。
                #[cfg(feature = "log-embed")]
                if policy.base.saturating_mul(attempts) > delay {
                    embed_event!(warn, target: "skeleton::retry::backoff", attempt = attempts, linear_ms = policy.base.saturating_mul(attempts).as_millis() as u64, capped_ms = delay.as_millis() as u64);
                }
                // 取消安全锚点：sleep 内联 await，不 spawn 不装箱——外层
                // future drop 即连带 drop 在途 sleep future，全停。
                sleep.sleep((policy.jitter)(delay)).await;
            }
        }
    }
}
