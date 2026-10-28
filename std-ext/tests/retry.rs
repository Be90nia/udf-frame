//! retry 编排域集成测试：坑面 armor 逐条钉死。
//!
//! 同步面时间线全部经 [`MockClock`] 虚拟推进（Sleeper 注入，实现=advance）：
//! 若实现误走真实 `thread::sleep`，虚拟钟不前进，逐毫秒断言即红——确定性
//! 不等式是 sleep 通路的证伪器。零真实 sleep，全套毫秒级完成。
//!
//! 断言逐毫秒确定值的政策一律注入恒等抖动源（`with_jitter(identity)`）；
//! 默认 full-jitter 的行为面（区间/非恒值）由专门测试钉死。异步面零三方
//! 手动驱动：RecordingSleeper 即刻就绪记录睡眠序列（Pending=非确定挂起面
//! 即红），取消安全经悬停探针观测。

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use skeleton::clock::{MockClock, SystemClock};
use std_ext::retry::{
    full_jitter, retry, retry_async, retry_if, retry_if_async, AsyncSleeper, RetryError,
    RetryPolicy,
};

/// 脚本化 op：按序吐结果并计数；脚本耗尽 = retry 越权多调（测试即红）。
fn scripted(
    outcomes: Vec<Result<u32, &'static str>>,
) -> (impl FnMut() -> Result<u32, &'static str>, Arc<AtomicU32>) {
    let calls = Arc::new(AtomicU32::new(0));
    let calls_in = Arc::clone(&calls);
    let mut script = outcomes.into_iter();
    (
        move || {
            calls_in.fetch_add(1, Ordering::Relaxed);
            script.next().expect("脚本耗尽：retry 越权多调 op")
        },
        calls,
    )
}

fn calls_of(calls: &Arc<AtomicU32>) -> u32 {
    calls.load(Ordering::Relaxed)
}

/// 恒等抖动源：确定性回放口径（MockClock 逐毫秒断言的前提）。
fn identity(d: Duration) -> Duration {
    d
}

/// 异步面脚本化 op：与同步面 `scripted` 同约（脚本耗尽=越权多调即红）。
fn scripted_async(
    outcomes: Vec<Result<u32, &'static str>>,
) -> (
    impl FnMut(u32) -> std::future::Ready<Result<u32, &'static str>>,
    Arc<AtomicU32>,
) {
    let calls = Arc::new(AtomicU32::new(0));
    let calls_in = Arc::clone(&calls);
    let mut script = outcomes.into_iter();
    (
        move |_attempt| {
            calls_in.fetch_add(1, Ordering::Relaxed);
            std::future::ready(script.next().expect("脚本耗尽：retry_async 越权多调 op"))
        },
        calls,
    )
}

/// 记录型异步睡眠口：调用即记下请求时长、即刻就绪——异步面「同一时间线」
/// 断言的观测面（对齐同步面 MockClock 口径）。Clone 共享记录（传入 future
/// 后仍可读取）。
#[derive(Clone, Default)]
struct RecordingSleeper(Arc<Mutex<Vec<Duration>>>);

impl AsyncSleeper for RecordingSleeper {
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send {
        self.0.lock().expect("记录锁未毒化").push(d);
        std::future::ready(())
    }
}

impl RecordingSleeper {
    fn delays(&self) -> Vec<Duration> {
        self.0.lock().expect("记录锁未毒化").clone()
    }
}

/// 确定性驱动：sleeper 即刻就绪，整个 future 应单次 poll 完成——Pending =
/// 实现引入了非确定挂起面（测试即红）。
fn run_to_ready<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("确定性 mock（sleeper 即刻就绪）下不得 Pending"),
    }
}

/// 取消探针状态：睡眠计数 + 在途 sleep future 的 drop 留痕（Arc 供
/// PendingSleep 与测试断言共享同一面）。
struct CancelProbeState {
    sleeps: AtomicU32,
    in_flight_dropped: Arc<AtomicBool>,
}

/// 悬停探针睡眠口：sleep 计数后返回恒 Pending 的 future，Drop 留痕——
/// 外层 future drop 是否连带 drop 在途 sleep future 的观测面。
#[derive(Clone)]
struct CancelProbe(Arc<CancelProbeState>);

struct PendingSleep {
    dropped: Arc<AtomicBool>,
}

impl Future for PendingSleep {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

impl Drop for PendingSleep {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Relaxed);
    }
}

impl AsyncSleeper for CancelProbe {
    fn sleep(&self, _d: Duration) -> impl Future<Output = ()> + Send {
        self.0.sleeps.fetch_add(1, Ordering::Relaxed);
        PendingSleep {
            dropped: Arc::clone(&self.0.in_flight_dropped),
        }
    }
}

const BASE: Duration = Duration::from_millis(100);
const CAP: Duration = Duration::from_secs(10);

// 坑面「clock 缺失下真实 sleep 不可测」：时间线经注入 Sleeper 推进，逐毫秒断言。
#[test]
fn timeline_two_retries_then_success_is_deterministic() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, BASE, CAP).with_jitter(identity);
    let (op, calls) = scripted(vec![Err("e1"), Err("e2"), Ok(7)]);
    assert_eq!(retry(op, &policy, &clock).expect("第三次成功"), 7);
    assert_eq!(calls_of(&calls), 3);
    // delay(1)+delay(2)=100ms+200ms；真实 sleep 不推进虚拟钟 → 此断言必红。
    assert_eq!(clock.unix_offset(), Duration::from_millis(300));
}

#[test]
fn first_success_makes_one_call_and_zero_sleep() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, BASE, CAP);
    let (op, calls) = scripted(vec![Ok(1)]);
    assert_eq!(retry(op, &policy, &clock).unwrap(), 1);
    assert_eq!(calls_of(&calls), 1);
    assert_eq!(clock.unix_offset(), Duration::ZERO);
}

// 坑面「无限重试」：次数闸先于睡眠裁决——末次失败后不为不会发生的重试等待。
#[test]
fn exhaustion_reports_attempts_and_last_without_trailing_sleep() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, BASE, CAP).with_jitter(identity);
    let (op, calls) = scripted(vec![Err("boom"), Err("boom"), Err("boom")]);
    match retry(op, &policy, &clock) {
        Err(RetryError::Exhausted { attempts, last }) => {
            assert_eq!(attempts, 3);
            assert_eq!(last, "boom");
        }
        other => panic!("预期 Exhausted，实得 {other:?}"),
    }
    assert_eq!(calls_of(&calls), 3);
    assert_eq!(clock.unix_offset(), Duration::from_millis(300)); // 仅 delay(1)+delay(2)
}

// 坑面「不可重试错误重试」：判定器拒绝 → 立停，零睡眠（快路径）。
#[test]
fn non_retryable_error_stops_immediately() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(5, BASE, CAP);
    let (op, calls) = scripted(vec![Err("fatal")]);
    let res = retry_if(op, &policy, &clock, |e: &&str| *e != "fatal");
    match res {
        Err(RetryError::Exhausted { attempts, last }) => {
            assert_eq!(attempts, 1);
            assert_eq!(last, "fatal");
        }
        other => panic!("预期 Exhausted，实得 {other:?}"),
    }
    assert_eq!(calls_of(&calls), 1);
    assert_eq!(clock.unix_offset(), Duration::ZERO);
}

// 判定器按错误类分流：transient 重试一次，fatal 即停（fatal 败后不睡眠）。
#[test]
fn predicate_gates_per_error_class() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(5, BASE, CAP).with_jitter(identity);
    let (op, calls) = scripted(vec![Err("transient"), Err("fatal")]);
    let res = retry_if(op, &policy, &clock, |e: &&str| *e == "transient");
    match res {
        Err(RetryError::Exhausted { attempts, last }) => {
            assert_eq!(attempts, 2);
            assert_eq!(last, "fatal");
        }
        other => panic!("预期 Exhausted，实得 {other:?}"),
    }
    assert_eq!(calls_of(&calls), 2);
    assert_eq!(clock.unix_offset(), Duration::from_millis(100)); // 仅首败后的 delay(1)
}

// 坑面「预算漏计」：预算测量与退避睡眠同钟（Sleeper: Clock 超参绑定，
// 错配在类型层不可表达），elapsed 逐毫秒断言。
#[test]
fn budget_stop_reports_exact_elapsed() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(10, BASE, CAP)
        .with_budget(Duration::from_millis(250))
        .with_jitter(identity);
    let (op, calls) = scripted(vec![Err("e"); 5]);
    let res = retry(op, &policy, &clock);
    match res {
        Err(RetryError::OutOfBudget {
            attempts,
            last,
            elapsed,
        }) => {
            // 时间线：t=0 败→睡 delay(1)=100；t=100 败→睡 delay(2)=200；t=300 败时
            // elapsed=300 ≥ 250 命中（线性退避 ×attempt，非平推）。
            assert_eq!(attempts, 3);
            assert_eq!(last, "e");
            assert_eq!(elapsed, Duration::from_millis(300));
        }
        other => panic!("预期 OutOfBudget，实得 {other:?}"),
    }
    assert_eq!(calls_of(&calls), 3);
    assert_eq!(clock.unix_offset(), Duration::from_millis(300));
}

// 预算边界：budget=0 语义明确 = 失败即停（文档化退化形，不静默歧义）。
#[test]
fn zero_budget_stops_after_first_failure() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(5, BASE, CAP).with_budget(Duration::ZERO);
    let (op, calls) = scripted(vec![Err("e")]);
    match retry(op, &policy, &clock) {
        Err(RetryError::OutOfBudget {
            attempts, elapsed, ..
        }) => {
            assert_eq!(attempts, 1);
            assert_eq!(elapsed, Duration::ZERO);
        }
        other => panic!("预期 OutOfBudget，实得 {other:?}"),
    }
    assert_eq!(calls_of(&calls), 1);
    assert_eq!(clock.unix_offset(), Duration::ZERO);
}

// 退避上限：cap 截平时间线 100/150/150。
#[test]
fn delay_cap_saturates_timeline() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(4, BASE, Duration::from_millis(150)).with_jitter(identity);
    let (op, calls) = scripted(vec![Err("e"); 4]);
    let res = retry(op, &policy, &clock);
    assert!(matches!(
        res,
        Err(RetryError::Exhausted { attempts: 4, .. })
    ));
    assert_eq!(calls_of(&calls), 4);
    assert_eq!(clock.unix_offset(), Duration::from_millis(400)); // 100+150+150
}

// 坑面「退避为零风暴」：构造期 fail-closed，不静默通过。
#[test]
#[should_panic(expected = "退避 base 必须非零")]
fn zero_base_rejected_at_construction() {
    let _ = RetryPolicy::new(3, Duration::ZERO, Duration::from_secs(1));
}

// 坑面「无限重试」前置闸：零次执行不是重试语义，构造期拒绝。
#[test]
#[should_panic(expected = "max_attempts 必须 ≥ 1")]
fn zero_attempts_rejected_at_construction() {
    let _ = RetryPolicy::new(0, Duration::from_millis(1), Duration::from_secs(1));
}

// 坑面「panic 当错误重试污染语义」：panic 原样传播，不转 Err、不计数、不重试。
#[test]
fn panic_propagates_untouched() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, BASE, CAP);
    let calls = Arc::new(AtomicU32::new(0));
    let calls_in = Arc::clone(&calls);
    let op = move || -> Result<u32, &'static str> {
        calls_in.fetch_add(1, Ordering::Relaxed);
        panic!("依赖炸了");
    };
    let res = catch_unwind(AssertUnwindSafe(|| retry(op, &policy, &clock)));
    assert!(res.is_err(), "panic 必须原样传播，禁转 Err/吞/重试");
    assert_eq!(calls_of(&calls), 1);
    assert_eq!(clock.unix_offset(), Duration::ZERO);
}

// SystemClock（thread::sleep 实路）端到端冒烟：ns 级退避，生产睡眠路径真实可达。
#[test]
fn system_clock_smoke_end_to_end() {
    let policy = RetryPolicy::new(3, Duration::from_nanos(1), Duration::from_nanos(10));
    let (op, calls) = scripted(vec![Err("x"), Ok(9)]);
    assert_eq!(retry(op, &policy, &SystemClock).expect("第二次成功"), 9);
    assert_eq!(calls_of(&calls), 2);
}

// armor「无 jitter 同步节拍→重试风暴」边界：full_jitter 恒在 [0, d) 半开
// 区间（含零退避与 1ns 退化值域）。
#[test]
fn full_jitter_within_half_open_range() {
    let values = [
        Duration::from_nanos(1),
        Duration::from_nanos(999),
        Duration::from_millis(1),
        Duration::from_millis(250),
        Duration::from_secs(1),
    ];
    for d in values {
        for _ in 0..64 {
            let j = full_jitter(d);
            assert!(j < d, "抖动值 {j:?} 越出 [0, {d:?}) 半开区间");
        }
    }
    assert_eq!(full_jitter(Duration::ZERO), Duration::ZERO);
    assert_eq!(
        full_jitter(Duration::from_nanos(1)),
        Duration::ZERO,
        "值域 [0, 1ns) 仅含零"
    );
}

// armor「无 jitter 同步节拍→重试风暴」行为面：默认政策（full jitter 禁关）
// 实际睡眠严格小于线性退避值，且多次运行非恒值（恒值=抖动未生效/线性回潮）。
#[test]
fn retry_delays_are_jittered() {
    let policy = RetryPolicy::new(2, Duration::from_millis(1), CAP); // 默认=full jitter
    let mut observed = std::collections::BTreeSet::new();
    for _ in 0..16 {
        let clock = MockClock::new();
        let (op, _calls) = scripted(vec![Err("e"), Ok(1)]);
        let _ = retry(op, &policy, &clock);
        let slept = clock.unix_offset();
        assert!(
            slept < Duration::from_millis(1),
            "full jitter 必须严格小于线性退避值，实得 {slept:?}"
        );
        observed.insert(slept);
    }
    assert!(observed.len() > 1, "16 次全同值 {observed:?}：抖动未生效");
}

// armor「只设次数无 deadline（长尾挂死）或反之」：双闸先到先停。
#[test]
fn deadline_and_attempts_both_bound_retries() {
    // 预算先到：次数宽松、预算紧 → OutOfBudget（恒等源确定时间线 100+200）。
    let clock = MockClock::new();
    let tight = RetryPolicy::new(10, BASE, CAP)
        .with_budget(Duration::from_millis(150))
        .with_jitter(identity);
    let (op, calls) = scripted(vec![Err("e"); 5]);
    assert!(matches!(
        retry(op, &tight, &clock),
        Err(RetryError::OutOfBudget { attempts: 3, .. })
    ));
    assert_eq!(calls_of(&calls), 3);
    assert_eq!(clock.unix_offset(), Duration::from_millis(300));

    // 次数先到：预算宽松 → Exhausted{attempts:3}。
    let clock2 = MockClock::new();
    let loose = RetryPolicy::new(3, BASE, CAP)
        .with_budget(Duration::from_secs(1))
        .with_jitter(identity);
    let (op2, calls2) = scripted(vec![Err("e"); 3]);
    assert!(matches!(
        retry(op2, &loose, &clock2),
        Err(RetryError::Exhausted { attempts: 3, .. })
    ));
    assert_eq!(calls_of(&calls2), 3);
    assert_eq!(clock2.unix_offset(), Duration::from_millis(300));
}

// armor「末次失败原因被 retry 错误吞掉」：into_last 两种形态都原样取回。
#[test]
fn last_error_is_reported_not_swallowed() {
    let exhausted: RetryError<&str> = RetryError::Exhausted {
        attempts: 3,
        last: "boom",
    };
    assert_eq!(exhausted.into_last(), "boom");
    let budgeted: RetryError<&str> = RetryError::OutOfBudget {
        attempts: 2,
        last: "late",
        elapsed: Duration::from_millis(300),
    };
    assert_eq!(budgeted.into_last(), "late");
}

// armor 验收「同步/异步同语义对照」：同一 policy、同一脚本、恒等抖动源下
// 同一时间线——调用次数/睡眠序列/钟面推进逐项对齐。
#[test]
fn sync_and_async_share_policy_and_timeline() {
    let policy = RetryPolicy::new(3, BASE, CAP).with_jitter(identity);

    let clock = MockClock::new();
    let (op_s, calls_s) = scripted(vec![Err("e1"), Err("e2"), Ok(7)]);
    assert_eq!(retry(op_s, &policy, &clock).expect("第三次成功"), 7);

    let (op_a, calls_a) = scripted_async(vec![Err("e1"), Err("e2"), Ok(7)]);
    let recorder = RecordingSleeper::default();
    let driver = recorder.clone();
    assert_eq!(
        run_to_ready(retry_async(&policy, op_a, driver)).expect("异步面第三次成功"),
        7
    );

    assert_eq!(calls_of(&calls_s), calls_of(&calls_a));
    assert_eq!(recorder.delays(), vec![BASE, Duration::from_millis(200)]);
    assert_eq!(clock.unix_offset(), Duration::from_millis(300));
    assert_eq!(
        recorder.delays().iter().sum::<Duration>(),
        clock.unix_offset(),
        "异步睡眠序列与同步钟面推进同一时间线"
    );
}

// 异步面耗尽回灌与同步面逐字同：attempts/last 原样在手，into_last 可取，
// 末次失败后无 trailing sleep。
#[test]
fn async_exhaustion_reports_attempts_last_and_into_last() {
    let policy = RetryPolicy::new(3, BASE, CAP).with_jitter(identity);
    let (op, calls) = scripted_async(vec![Err("boom"); 3]);
    let recorder = RecordingSleeper::default();
    let driver = recorder.clone();
    let res = run_to_ready(retry_async(&policy, op, driver));
    match res {
        Err(e) => {
            assert!(matches!(&e, RetryError::Exhausted { attempts: 3, .. }));
            assert_eq!(e.into_last(), "boom");
        }
        Ok(_) => panic!("预期 Exhausted"),
    }
    assert_eq!(calls_of(&calls), 3);
    assert_eq!(
        recorder.delays(),
        vec![Duration::from_millis(100), Duration::from_millis(200)]
    );
}

// 异步面判定器拒绝：零睡眠立停（快路径与同步面逐字同）。
#[test]
fn async_non_retryable_stops_without_sleep() {
    let policy = RetryPolicy::new(5, BASE, CAP).with_jitter(identity);
    let (op, calls) = scripted_async(vec![Err("fatal")]);
    let recorder = RecordingSleeper::default();
    let driver = recorder.clone();
    let res = run_to_ready(retry_if_async(&policy, op, driver, |e: &&str| {
        *e == "transient"
    }));
    assert!(matches!(
        res,
        Err(RetryError::Exhausted { attempts: 1, .. })
    ));
    assert_eq!(calls_of(&calls), 1);
    assert!(recorder.delays().is_empty(), "判定拒绝必须零睡眠");
}

// 异步面预算闸（真实墙钟口径，见 retry_async 文档）：budget=0 = 失败即停，
// 语义与同步面 zero_budget 测试对齐。
#[test]
fn async_zero_budget_stops_after_first_failure() {
    let policy = RetryPolicy::new(5, BASE, CAP).with_budget(Duration::ZERO);
    let (op, calls) = scripted_async(vec![Err("e")]);
    let res = run_to_ready(retry_async(&policy, op, RecordingSleeper::default()));
    assert!(matches!(
        res,
        Err(RetryError::OutOfBudget { attempts: 1, .. })
    ));
    assert_eq!(calls_of(&calls), 1);
}

// armor「drop 后后台仍在睡/仍重试」：sleep 内联 await，外层 future drop 即
// 连带 drop 在途 sleep future——全停，无 spawn 逃逸。
#[test]
fn dropped_retry_stops_entirely() {
    let policy = RetryPolicy::new(3, BASE, CAP);
    let probe = CancelProbe(Arc::new(CancelProbeState {
        sleeps: AtomicU32::new(0),
        in_flight_dropped: Arc::new(AtomicBool::new(false)),
    }));
    let (op, calls) = scripted_async(vec![Err("boom"), Ok(1)]);
    // Box::pin 而非 pin!：armor 观测面要求「drop 外层句柄 = drop 在途 sleep
    // future」——pin! 的所有权绑定活到块尾，drop Pin 引用不触发内层 Drop。
    let mut fut = Box::pin(retry_async(&policy, op, probe.clone()));

    // 首试失败后应悬停于退避睡眠（sleeper 已被调、future 未决）。
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Pending => {}
        Poll::Ready(_) => panic!("首试失败后应悬停于退避睡眠"),
    }
    assert_eq!(probe.0.sleeps.load(Ordering::Relaxed), 1);
    assert_eq!(calls_of(&calls), 1);
    assert!(!probe.0.in_flight_dropped.load(Ordering::Relaxed));

    // drop 外层 future：在途 sleep future 必连带 drop（全停锚点）。
    drop(fut);
    assert!(
        probe.0.in_flight_dropped.load(Ordering::Relaxed),
        "外层 future drop 必须连带 drop 在途 sleep future（无 spawn 逃逸）"
    );
}
