//! 确定性退避：attempt 序号线性偏移，禁 jitter。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零时钟零随机——sleep 归调用方
//!（fx 侧 `tokio::time` / sync 侧 `std::thread::sleep`）；耗尽语义的标准形
//!（`RetryExhausted{attempts, last}` 回灌）归调用方，本件只算延迟不做重试循环。

use std::time::Duration;

/// 计算第 `attempt` 次重试前的确定性退避延迟。
///
/// `attempt` 从 1 起（0 视为未重试，返回 [`Duration::ZERO`]）；饱和乘法防溢出；
/// 结果截到 `max`。禁 jitter（确定性回放要求）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::backoff::backoff_delay;
/// use std::time::Duration;
///
/// let base = Duration::from_millis(100);
/// assert_eq!(backoff_delay(1, base, Duration::from_secs(10)), base);
/// assert_eq!(backoff_delay(3, base, Duration::from_millis(250)), Duration::from_millis(250));
/// ```
pub fn backoff_delay(attempt: u32, base: Duration, max: Duration) -> Duration {
    base.saturating_mul(attempt).min(max)
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
