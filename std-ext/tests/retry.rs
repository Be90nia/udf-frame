//! retry 编排域集成测试：坑面 armor 逐条钉死。
//!
//! 时间线全部经 [`MockClock`] 虚拟推进（Sleeper 注入，实现=advance）：
//! 若实现误走真实 `thread::sleep`，虚拟钟不前进，逐毫秒断言即红——确定性
//! 不等式是 sleep 通路的证伪器。零真实 sleep，全套毫秒级完成。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::{MockClock, SystemClock};
use std_ext::retry::{retry, retry_if, RetryError, RetryPolicy};

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

const BASE: Duration = Duration::from_millis(100);
const CAP: Duration = Duration::from_secs(10);

// 坑面「clock 缺失下真实 sleep 不可测」：时间线经注入 Sleeper 推进，逐毫秒断言。
#[test]
fn timeline_two_retries_then_success_is_deterministic() {
    let clock = MockClock::new();
    let policy = RetryPolicy::new(3, BASE, CAP);
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
    let policy = RetryPolicy::new(3, BASE, CAP);
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
    let policy = RetryPolicy::new(5, BASE, CAP);
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
    let policy = RetryPolicy::new(10, BASE, CAP).with_budget(Duration::from_millis(250));
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
    let policy = RetryPolicy::new(4, BASE, Duration::from_millis(150));
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
