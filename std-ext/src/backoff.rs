//! 确定性退避：attempt 序号线性偏移，禁 jitter。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零时钟零随机——sleep 归调用方
//!（fx 侧 `tokio::time` / sync 侧 `std::thread::sleep`）；耗尽语义的标准形
//!（`RetryExhausted{attempts, last}` 回灌）归调用方，本件只算延迟不做重试循环。
//!
//! log-embed 埋点（`log-embed` feature，默认关=宏消除零开销）：target
//! `skeleton::backoff`——计算入参/结果（attempt/base_ms/max_ms/delay_ms），
//! debug 级（派单契约显式点：纯函数域唯一埋点，勿在其上加事件）。

use std::time::Duration;

/// 计算第 `attempt` 次重试前的确定性退避延迟。
///
/// `attempt` 从 1 起（0 视为未重试，返回 [`Duration::ZERO`]）；饱和乘法防溢出；
/// 结果截到 `max`。禁 jitter（确定性回放要求）。
///
/// 性能锚（2026-10-16 批 D 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-perf-batchd.md）：纯算术常数级，~3ns/次
///（`saturating_mul`+`min`，零分配零时钟）。
///
/// # Examples
///
/// ```
/// use std_ext::backoff::backoff_delay;
/// use std::time::Duration;
///
/// let base = Duration::from_millis(100);
/// assert_eq!(backoff_delay(1, base, Duration::from_secs(10)), base);
/// assert_eq!(backoff_delay(3, base, Duration::from_millis(250)), Duration::from_millis(250));
/// ```
pub fn backoff_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    let delay = base.saturating_mul(attempt).min(max);
    embed_event!(
        debug,
        target: "skeleton::backoff",
        attempt = attempt,
        base_ms = base.as_millis() as u64,
        max_ms = max.as_millis() as u64,
        delay_ms = delay.as_millis() as u64
    );
    delay
}

#[cfg(test)]
mod tests {
    use super::*;

    // ① 线性序列：base=100ms → 100/200/300ms。
    #[test]
    fn linear_sequence_is_attempt_times_base() {
        let base = Duration::from_millis(100);
        let cap = Duration::from_secs(10);
        assert_eq!(backoff_delay(1, base, cap), Duration::from_millis(100));
        assert_eq!(backoff_delay(2, base, cap), Duration::from_millis(200));
        assert_eq!(backoff_delay(3, base, cap), Duration::from_millis(300));
    }

    // ② max 饱和：线性增长越过上限后截平。
    #[test]
    fn delay_saturates_at_max() {
        let base = Duration::from_millis(100);
        let max = Duration::from_millis(250);
        assert_eq!(backoff_delay(2, base, max), Duration::from_millis(200)); // 未及上限
        assert_eq!(backoff_delay(3, base, max), max); // 300ms → 截到 250ms
        assert_eq!(backoff_delay(10, base, max), max); // 持续截平
    }

    // ③ attempt=0 = ZERO（未重试语义）。
    #[test]
    fn attempt_zero_is_zero() {
        assert_eq!(
            backoff_delay(0, Duration::from_secs(1), Duration::from_secs(10)),
            Duration::ZERO
        );
    }

    // ④ u32::MAX attempt 饱和不 panic：2^33s × u32::MAX 超出 u64 秒字段上限，
    //    saturating_mul 饱和到 Duration::MAX（误用会 panic 的 Mul 则此处红）。
    #[test]
    fn u32_max_attempt_saturates_without_panic() {
        let saturated = backoff_delay(u32::MAX, Duration::from_secs(1 << 33), Duration::MAX);
        assert_eq!(saturated, Duration::MAX);
    }
}
