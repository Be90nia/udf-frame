//! ratelimit 域集成测试：坑面 armor 逐条钉死。
//!
//! 时间线全部经 [`MockClock`] 虚拟推进（Sleeper 注入，实现=advance）：
//! 若阻塞版误走真实 `thread::sleep`，虚拟钟不前进，逐毫秒断言即红——
//! 确定性不等式是 sleep 通路的证伪器。标「真睡」的两个测试用
//! SystemClock 短时长（50ms 级），覆盖锁空闲面与并发活性。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use skeleton::clock::{Clock, MockClock, SystemClock};
use std_ext::ratelimit::{ExceedsCapacity, TokenBucket, TryAcquireError};
use std_ext::retry::Sleeper;

/// 计数次 sleeper：包装 MockClock，每次 sleep 计数后虚拟推进——
/// 轮次可数是「零自旋/轮次有界」断言的观测面。
struct CountingSleeper {
    clock: MockClock,
    sleeps: AtomicU32,
}

impl CountingSleeper {
    fn new() -> Self {
        Self {
            clock: MockClock::new(),
            sleeps: AtomicU32::new(0),
        }
    }

    fn sleep_count(&self) -> u32 {
        self.sleeps.load(Ordering::Relaxed)
    }
}

impl Clock for CountingSleeper {
    fn now_instant(&self) -> Instant {
        self.clock.now_instant()
    }

    fn now_system(&self) -> SystemTime {
        self.clock.now_system()
    }
}

impl Sleeper for CountingSleeper {
    fn sleep(&self, d: Duration) {
        self.sleeps.fetch_add(1, Ordering::Relaxed);
        self.clock.advance(d);
    }
}

fn offset_of(clock: &CountingSleeper) -> Duration {
    clock.clock.unix_offset()
}

// ---- 构造不变量（fail-closed 纪律，RetryPolicy 同款）----

#[test]
fn invalid_constructor_args_panic() {
    let clock: Arc<dyn Sleeper> = Arc::new(SystemClock);
    for bad_cap in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let r = catch_unwind(AssertUnwindSafe(|| {
            TokenBucket::new(Arc::clone(&clock), bad_cap, 1.0)
        }));
        assert!(r.is_err(), "capacity={bad_cap} 必须构造期拒绝");
    }
    for bad_rate in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let r = catch_unwind(AssertUnwindSafe(|| {
            TokenBucket::new(Arc::clone(&clock), 2.0, bad_rate)
        }));
        assert!(r.is_err(), "refill_rate={bad_rate} 必须构造期拒绝");
    }
}

// ---- 坑面「首桶不满/首票免费」：满桶启动，capacity=突发预算 ----

#[test]
fn initial_bucket_is_full_up_to_capacity() {
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock, 2.0, 1.0);
    bucket.try_acquire(2).expect("构造即满桶：突发预算立即可取");
    // 满桶清空后零时间流逝，再取任何正数立即拒（Insufficient 而非等回填）。
    let err = bucket.try_acquire(1).unwrap_err();
    assert_eq!(
        err,
        TryAcquireError::Insufficient {
            available: 0.0,
            wait: Duration::from_secs(1)
        }
    );
}

#[test]
fn fractional_capacity_accommodates_integer_requests() {
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock, 2.5, 1.0);
    bucket.try_acquire(2).expect("2.5 容量容纳 2 个整数请求");
    assert!(bucket.try_acquire(1).is_err(), "剩 0.5 不足 1");
}

// ---- 坑面「acquire(0) 阻塞」：恒成功 no-op，零等待零消耗 ----

#[test]
fn acquire_zero_is_noop() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 1.0, 1.0);
    bucket.try_acquire(0).expect("try_acquire(0) 恒成功");
    bucket.acquire(0).expect("acquire(0) 恒成功");
    bucket.try_acquire(1).unwrap(); // 清桶
    bucket
        .try_acquire(0)
        .expect("空桶上 try_acquire(0) 仍恒成功");
    bucket
        .acquire(0)
        .expect("空桶上 acquire(0) 仍恒成功、零阻塞");
    assert_eq!(sleeper.sleep_count(), 0, "零请求零睡眠");
    assert_eq!(offset_of(&sleeper), Duration::ZERO, "零请求零时间流逝");
}

// ---- 坑面「acquire(n>cap) 无限等」：立即拒绝，fail-visible ----

#[test]
fn acquire_above_capacity_rejects_immediately() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 3.0, 1.0);
    let expected = ExceedsCapacity {
        requested: 4,
        capacity: 3.0,
    };
    assert_eq!(
        bucket.try_acquire(4),
        Err(TryAcquireError::ExceedsCapacity(expected))
    );
    assert_eq!(bucket.acquire(4), Err(expected), "阻塞版同判");
    assert_eq!(sleeper.sleep_count(), 0, "不可满足请求禁进入等待循环");
    assert_eq!(offset_of(&sleeper), Duration::ZERO);
}

// ---- MockClock 确定性令牌时间线：阻塞版自动推进虚拟钟，零真实 sleep ----

#[test]
fn acquire_sleeps_exactly_until_tokens_sufficient() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 2.0, 1.0);
    bucket.try_acquire(2).unwrap(); // 清空
    bucket.acquire(1).expect("第一个：睡到回填 1 个");
    assert_eq!(offset_of(&sleeper), Duration::from_secs(1));
    bucket.acquire(1).expect("第二个：再睡 1s");
    assert_eq!(
        offset_of(&sleeper),
        Duration::from_secs(2),
        "时间线逐秒可断言"
    );
}

#[test]
fn try_and_block_share_one_ledger() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 2.0, 1.0);
    bucket.try_acquire(2).unwrap(); // try 清空
    bucket.acquire(1).unwrap(); // 阻塞版在回填面上续接
    assert_eq!(offset_of(&sleeper), Duration::from_secs(1));
    // 刚睡满的 1 个已被取走，回填从睡醒时刻起算：零流逝再取 1 不足额。
    assert!(matches!(
        bucket.try_acquire(1),
        Err(TryAcquireError::Insufficient { available, .. }) if (available - 0.0).abs() < 1e-9
    ));
    // 半秒后回填 0.5 仍不足；整秒足额（同桶账本，两入口零漂移）。
    assert_eq!(
        bucket.try_acquire(1).unwrap_err().wait(),
        Duration::from_secs(1)
    );
}

// ---- 坑面「回填精度积累」：fractional 结转精确，误差不跨步积累 ----

#[test]
fn fractional_refill_accumulates_exactly() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 2.0, 1.0);
    bucket.try_acquire(2).unwrap(); // 清空
    bucket.acquire(1).unwrap(); // 睡 1s，账本归零+1-1=0
                                // 两个半秒各回填 0.5：try 之间无整秒，fractional 结转必须精确积累。
    sleeper.clock.advance(Duration::from_millis(500));
    assert!(bucket.try_acquire(1).is_err(), "0.5 不足 1");
    sleeper.clock.advance(Duration::from_millis(500));
    bucket.try_acquire(1).expect("0.5+0.5=1.0：结转后足额");
    assert_eq!(offset_of(&sleeper), Duration::from_secs(2));
}

// ---- 坑面「巨步回填溢出/爆桶」：封顶 min 吸收任意大步 ----

#[test]
fn huge_advance_saturates_at_capacity() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 2.0, 1.0);
    bucket.try_acquire(2).unwrap(); // 清空
    sleeper.clock.advance(Duration::from_secs(10_000)); // 巨步
    bucket.try_acquire(2).expect("回填封顶：恰满桶不爆");
    assert!(bucket.try_acquire(1).is_err(), "封顶后不超 capacity");
}

// ---- try_acquire 的 Insufficient 载荷：available + 可满足时刻 ----

#[test]
fn insufficient_reports_available_and_wait() {
    let clock = Arc::new(MockClock::new());
    let bucket = TokenBucket::new(clock.clone(), 5.0, 2.0); // rate=2/s
    bucket.try_acquire(5).unwrap(); // 清空
    clock.advance(Duration::from_millis(500)); // 回填 1.0
    let err = bucket.try_acquire(3).unwrap_err();
    match err {
        TryAcquireError::Insufficient { available, wait } => {
            assert!(
                (available - 1.0).abs() < 1e-9,
                "回填 0.5s×2/s=1.0，得 {available}"
            );
            assert_eq!(wait, Duration::from_secs(1), "缺口 2 ÷ 2/s = 1s");
        }
        other => panic!("预期 Insufficient，实得 {other:?}"),
    }
}

// ---- 坑面「醒后忙转」：轮次有界——睡满缺口一次到位，零自旋 ----

#[test]
fn acquire_recheck_rounds_are_bounded() {
    let sleeper = Arc::new(CountingSleeper::new());
    let bucket = TokenBucket::new(sleeper.clone(), 10.0, 5.0);
    bucket.try_acquire(10).unwrap(); // 清空
    for _ in 0..3 {
        bucket.acquire(1).unwrap();
    }
    assert_eq!(offset_of(&sleeper), Duration::from_millis(600));
    assert_eq!(
        sleeper.sleep_count(),
        3,
        "每次 acquire 恰一轮睡眠（睡满缺口一次到位，忙转=轮数暴涨）"
    );
}

// ---- 坑面「睡眠持锁」：acquire 睡眠期间锁必须空闲（真睡，短时长）----

#[test]
fn blocked_acquire_does_not_hold_lock() {
    let bucket = Arc::new(TokenBucket::new(Arc::new(SystemClock), 1.0, 20.0)); // 50ms/token
    bucket.try_acquire(1).unwrap(); // 清桶
    let b2 = Arc::clone(&bucket);
    let waiter = thread::spawn(move || b2.acquire(1).expect("回填后成功"));
    thread::sleep(Duration::from_millis(5)); // 让 waiter 进入锁外睡眠
    let t0 = Instant::now();
    // waiter 睡满 50ms；期间反复进锁判定（恒 Err 但走临界区）：
    // 若实现持锁睡眠，这里第一趟就卡 50ms，100 趟=5s 超时量级可辨。
    for _ in 0..100 {
        assert!(bucket.try_acquire(1).is_err());
    }
    let guard_free = t0.elapsed();
    assert!(
        guard_free < Duration::from_millis(25),
        "acquire 睡眠期间锁必须空闲（100 趟临界区耗时 {guard_free:?}，持锁睡眠面即红）"
    );
    waiter.join().unwrap();
}

// ---- 并发活性：有限 acquirer 集合内无永久饥饿（真睡，短时长）----

#[test]
fn concurrent_acquire_all_succeed() {
    // 突发 10 + 100/s 回填：4 线程 × 5 = 20 个，理论供给 100ms。
    let bucket = Arc::new(TokenBucket::new(Arc::new(SystemClock), 10.0, 100.0));
    let t0 = Instant::now();
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let b = Arc::clone(&bucket);
            thread::spawn(move || b.acquire(5).expect("活性：令牌持续供给必达成"))
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let elapsed = t0.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "20 个令牌（10 突发 + 100ms 回填）2s 内必清账，实耗 {elapsed:?}"
    );
}
