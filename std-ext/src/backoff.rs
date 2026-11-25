//! 确定性退避：attempt 序号线性偏移，禁 jitter。
//!
//! 常驻零依赖件：纯 std，无 feature 门。零时钟零随机——sleep 归调用方
//!（fx 侧 `tokio::time` / sync 侧 `std::thread::sleep`）；耗尽语义的标准形
//!（`RetryExhausted{attempts, last}` 回灌）归调用方，本件只算延迟不做重试循环。
//!
//! **零可观测副作用（issue#202）**：本件不调用宿主回调——`log-embed` 也不在这里
//! 发事件。原先 target `skeleton::backoff` 的计算入参事件已上移到编排层调用点
//! （[`crate::retry`] 的 `skeleton::retry::verdict` / `skeleton::retry::backoff`，
//! 字段含 attempt/base_ms/max_ms/backoff_ms），因为「纯算术件」的契约是零时钟
//! 零随机零分配**零回调**：订阅者 panic/阻塞不得让一次有效计算变成一次外部调用，
//! 也不得改变结果的可用性。

use std::time::Duration;

/// 计算第 `attempt` 次重试前的确定性退避延迟。
///
/// `attempt` 从 1 起（0 视为未重试，返回 [`Duration::ZERO`]）；饱和乘法防溢出；
/// 结果截到 `max`。禁 jitter（确定性回放要求）。
///
/// **无副作用**：不读钟、不分配、不调用宿主订阅者（`log-embed` 也不在此发事件，
/// issue#202）——订阅者 panic/阻塞与计算结果完全无关。
///
/// 性能锚（2026-10-16 批 D 定性，release/std Instant 粗测，详见
/// 批 D 性能锚（本地留存，未入库））：纯算术常数级，~3ns/次
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
    // 纯算术：不读钟、不分配、不碰宿主订阅者（issue#202——埋点在编排层）。
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

// issue#202：纯算术件零事件路径——退回到「计算里发事件」时本测试红
//（计数非 0）。用计数而非 panic 订阅者：宏的 catch_unwind 边界会把 panic 吞成
// 计数增量，panic 形式在此测不出「有没有调过订阅者」。
#[cfg(all(test, feature = "log-embed"))]
mod purity {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    #[derive(Default)]
    struct Counting(std::sync::Arc<AtomicUsize>);

    impl Subscriber for Counting {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, _: &Event<'_>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    #[test]
    fn backoff_delay_never_calls_host_subscriber() {
        let counter = Counting::default();
        let tally = std::sync::Arc::clone(&counter.0);
        let base = Duration::from_millis(100);
        let delay = tracing::subscriber::with_default(counter, || {
            backoff_delay(3, base, Duration::from_millis(250))
        });
        assert_eq!(delay, Duration::from_millis(250), "计算结果不因遥测面改变");
        assert_eq!(
            tally.load(Ordering::Relaxed),
            0,
            "纯算术件不得调用宿主订阅者（issue#202）"
        );
    }
}
