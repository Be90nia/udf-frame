//! 遥测隔离回归（issue#202/#204/#205/#206）：埋点失败或阻塞不得改变控制流与结果。
//!
//! 判据两轴：
//!
//! ① **顺序**：必需动作（op 调用 / 注入钟 `sleep` / typed 错误成形）先于事件——
//!    探针订阅者与注入钟共写一份顺序日志，退回「事件在前」即红；
//! ② **成因**：订阅者恒 panic 时，结果、调用次数、睡眠量与 typed 错误字段全部不变
//!    （宏的 catch_unwind 边界吞掉订阅者 panic 并计数）。
//!
//! `backoff_delay` 的纯性另有专门断言（`std_ext/src/backoff.rs` 的 `purity` 单测 +
//! 本文件 `backoff_delay_is_not_an_event_site`）。

#![cfg(feature = "log-embed")]

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant, SystemTime};

use skeleton::clock::Clock;
use std_ext::backoff::backoff_delay;
use std_ext::iter::process_results;
use std_ext::ratelimit::{ExceedsCapacity, TokenBucket, TryAcquireError};
use std_ext::retry::{AsyncSleeper, RetryError, RetryPolicy, Sleeper, retry_async, retry_if};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

type Log = Arc<Mutex<Vec<String>>>;

fn fresh_log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn mark(log: &Log, entry: impl Into<String>) {
    log.lock().expect("顺序日志").push(entry.into());
}

fn take(log: &Log) -> Vec<String> {
    std::mem::take(&mut *log.lock().expect("顺序日志"))
}

/// 探针订阅者：把 target 记进共享顺序日志；`panic_target` 命中即 panic（订阅者
/// 故障注入——宏边界把它吞成计数，调用方结果不受影响）。
struct Probe {
    log: Log,
    panic_target: Option<&'static str>,
}

impl Probe {
    fn new(log: &Log, panic_target: Option<&'static str>) -> Self {
        Self {
            log: Arc::clone(log),
            panic_target,
        }
    }
}

impl Subscriber for Probe {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let target = event.metadata().target();
        mark(&self.log, format!("ev:{target}"));
        assert!(
            Some(target) != self.panic_target,
            "注入：订阅者在 {target} 上 panic"
        );
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// 记录型注入钟：`sleep` 推进虚拟时间并把 `sleep` 记进顺序日志——事件出现在
/// sleep 之前即违约（注入钟是等待语义的唯一权威）。
struct RecordingClock {
    base: Instant,
    offset: Mutex<Duration>,
    log: Log,
}

impl RecordingClock {
    fn new(log: &Log) -> Self {
        Self {
            base: Instant::now(),
            offset: Mutex::new(Duration::ZERO),
            log: Arc::clone(log),
        }
    }

    fn slept(&self) -> Duration {
        *self.offset.lock().expect("虚拟钟")
    }
}

impl Clock for RecordingClock {
    fn now_instant(&self) -> Instant {
        self.base + self.slept()
    }

    fn now_system(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH
    }
}

impl Sleeper for RecordingClock {
    fn sleep(&self, d: Duration) {
        *self.offset.lock().expect("虚拟钟") += d;
        mark(&self.log, "sleep");
    }
}

/// 即刻就绪的记录型异步睡眠口（零运行时依赖；`sleep` 被调用即记账）。
struct RecordingAsyncSleeper(Arc<RecordingClock>);

impl AsyncSleeper for RecordingAsyncSleeper {
    fn sleep(&self, d: Duration) -> impl Future<Output = ()> + Send {
        self.0.sleep(d);
        std::future::ready(())
    }
}

/// 确定性驱动：睡眠口即刻就绪 ⇒ 单次 poll 必完成（Pending 即说明实现挂错）。
fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("即时完成的睡眠口下不得 Pending"),
    }
}

// ---- #202：纯算术件不是事件站点 ----

#[test]
fn backoff_delay_is_not_an_event_site() {
    let log = fresh_log();
    let delay = tracing::subscriber::with_default(Probe::new(&log, None), || {
        backoff_delay(3, Duration::from_millis(100), Duration::from_millis(250))
    });
    assert_eq!(delay, Duration::from_millis(250), "截平结果不变");
    assert!(take(&log).is_empty(), "纯算术件不得调用宿主订阅者");
}

// ---- #204：retry 控制流与订阅者解耦 ----

#[test]
fn retry_sync_attempt_and_sleep_precede_events() {
    let log = fresh_log();
    let clock = RecordingClock::new(&log);
    let policy =
        RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10)).with_jitter(|d| d);
    let mut remaining = 2u32;
    let mut calls = 0u32;
    let res = tracing::subscriber::with_default(Probe::new(&log, None), || {
        retry_if(
            || {
                calls += 1;
                mark(&log, format!("op{calls}"));
                if remaining > 0 {
                    remaining -= 1;
                    Err("transient")
                } else {
                    Ok(7u32)
                }
            },
            &policy,
            &clock,
            |_| true,
        )
    });
    assert_eq!(res.expect("两次失败后成功"), 7);
    assert_eq!(calls, 3, "每次尝试都真的调了 op");
    assert_eq!(clock.slept(), Duration::from_millis(300), "退避 100+200ms");
    assert_eq!(
        take(&log),
        vec![
            "op1",
            "ev:skeleton::retry::attempt",
            "sleep",
            "ev:skeleton::retry::verdict",
            "op2",
            "ev:skeleton::retry::attempt",
            "sleep",
            "ev:skeleton::retry::verdict",
            "op3",
            "ev:skeleton::retry::attempt",
        ],
        "op 与 sleep 均先于其事件；退避诊断在睡眠之后"
    );
}

#[test]
fn retry_sync_panicking_subscriber_keeps_operation_count_and_typed_error() {
    // ① 判定拒绝（verdict 事件 panic）：typed 错误与「零睡眠」不变。
    let log = fresh_log();
    let clock = RecordingClock::new(&log);
    let policy =
        RetryPolicy::new(5, Duration::from_millis(10), Duration::from_secs(1)).with_jitter(|d| d);
    let mut calls = 0u32;
    let res = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::retry::verdict")),
        || {
            retry_if(
                || {
                    calls += 1;
                    Err::<u32, &str>("fatal")
                },
                &policy,
                &clock,
                |_| false,
            )
        },
    );
    match res {
        Err(RetryError::Exhausted { attempts, last }) => {
            assert_eq!((attempts, last), (1, "fatal"), "typed 错误与末次错误原样");
        }
        other => panic!("订阅者 panic 不得顶替 typed 错误：{other:?}"),
    }
    assert_eq!(calls, 1, "订阅者 panic 不得阻止尝试");
    assert_eq!(clock.slept(), Duration::ZERO, "判定拒绝：零睡眠");

    // ② 重试放行（attempt 事件 panic）：重试仍全部发生。
    let log = fresh_log();
    let clock = RecordingClock::new(&log);
    let policy =
        RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10)).with_jitter(|d| d);
    let mut remaining = 2u32;
    let mut calls = 0u32;
    let res = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::retry::attempt")),
        || {
            retry_if(
                || {
                    calls += 1;
                    if remaining > 0 {
                        remaining -= 1;
                        Err("transient")
                    } else {
                        Ok(7u32)
                    }
                },
                &policy,
                &clock,
                |_| true,
            )
        },
    );
    assert_eq!(res.expect("重试照旧跑到成功"), 7);
    assert_eq!(calls, 3, "订阅者 panic 不得跳过任何一次尝试");
    assert_eq!(
        clock.slept(),
        Duration::from_millis(300),
        "调度好的退避照旧睡眠"
    );
}

#[test]
fn retry_async_attempt_and_sleep_precede_events() {
    let log = fresh_log();
    let clock = Arc::new(RecordingClock::new(&log));
    let policy =
        RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10)).with_jitter(|d| d);
    let mut remaining = 2u32;
    let mut calls = 0u32;
    let res = tracing::subscriber::with_default(Probe::new(&log, None), || {
        block_on(retry_async(
            &policy,
            |_attempt: u32| {
                calls += 1;
                mark(&log, format!("op{calls}"));
                let done = remaining == 0;
                if !done {
                    remaining -= 1;
                }
                async move { if done { Ok(7u32) } else { Err("transient") } }
            },
            RecordingAsyncSleeper(Arc::clone(&clock)),
        ))
    });
    assert_eq!(res.expect("两次失败后成功"), 7);
    assert_eq!(calls, 3, "每次尝试都真的调了 op");
    assert_eq!(clock.slept(), Duration::from_millis(300));
    assert_eq!(
        take(&log),
        vec![
            "op1",
            "ev:skeleton::retry::attempt",
            "sleep",
            "ev:skeleton::retry::verdict",
            "op2",
            "ev:skeleton::retry::attempt",
            "sleep",
            "ev:skeleton::retry::verdict",
            "op3",
            "ev:skeleton::retry::attempt",
        ],
        "异步面与同步面同序：op 与 sleep 先于其事件"
    );
}

#[test]
fn retry_async_panicking_subscriber_keeps_retry_sequence() {
    let log = fresh_log();
    let clock = Arc::new(RecordingClock::new(&log));
    let policy =
        RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10)).with_jitter(|d| d);
    let mut remaining = 2u32;
    let mut calls = 0u32;
    let res = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::retry::verdict")),
        || {
            block_on(retry_async(
                &policy,
                |_attempt: u32| {
                    calls += 1;
                    let done = remaining == 0;
                    if !done {
                        remaining -= 1;
                    }
                    async move { if done { Ok(7u32) } else { Err("transient") } }
                },
                RecordingAsyncSleeper(Arc::clone(&clock)),
            ))
        },
    );
    assert_eq!(res.expect("重试照旧跑到成功"), 7);
    assert_eq!(calls, 3);
    assert_eq!(clock.slept(), Duration::from_millis(300));
}

// ---- #205：限流结果与等待与订阅者解耦 ----

#[test]
fn ratelimit_try_acquire_typed_rejections_survive_panicking_subscriber() {
    let log = fresh_log();
    let clock = Arc::new(RecordingClock::new(&log));
    let bucket = TokenBucket::new(clock, 2.0, 1.0);
    let (over, short) = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::ratelimit::try_acquire")),
        || {
            let over = bucket.try_acquire(5).expect_err("超容量拒绝");
            assert!(bucket.try_acquire(2).is_ok(), "满桶突发 2");
            let short = bucket.try_acquire(1).expect_err("不足额拒绝");
            (over, short)
        },
    );
    assert_eq!(
        over,
        TryAcquireError::ExceedsCapacity(ExceedsCapacity {
            requested: 5,
            capacity: 2.0,
        }),
        "typed 拒绝不被订阅者 panic 顶替"
    );
    match short {
        TryAcquireError::Insufficient { available, wait } => {
            assert_eq!(available, 0.0);
            assert_eq!(wait, Duration::from_secs(1), "可满足时刻不变");
        }
        other => panic!("应为 Insufficient：{other:?}"),
    }
}

#[test]
fn ratelimit_acquire_waits_on_injected_clock_before_events() {
    let log = fresh_log();
    let clock = Arc::new(RecordingClock::new(&log));
    let bucket = TokenBucket::new(clock.clone(), 1.0, 1.0);
    assert!(bucket.try_acquire(1).is_ok(), "清空满桶");
    assert!(take(&log).is_empty(), "成功扣减零事件");
    tracing::subscriber::with_default(Probe::new(&log, None), || {
        bucket.acquire(1).expect("睡到回填足额");
    });
    assert_eq!(clock.slept(), Duration::from_secs(1), "等待由注入钟决定");
    assert_eq!(
        take(&log),
        vec!["sleep", "ev:skeleton::ratelimit::acquire"],
        "一轮等待先睡后埋点"
    );
}

#[test]
fn ratelimit_acquire_outcomes_survive_panicking_subscriber() {
    let log = fresh_log();
    let clock = Arc::new(RecordingClock::new(&log));
    let bucket = TokenBucket::new(clock.clone(), 1.0, 1.0);
    let over = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::ratelimit::acquire")),
        || {
            let over = bucket.acquire(5).expect_err("超容量拒绝");
            assert!(bucket.try_acquire(1).is_ok(), "清空满桶");
            bucket.acquire(1).expect("等待轮事件 panic 后仍须取到令牌");
            over
        },
    );
    assert_eq!(
        over,
        ExceedsCapacity {
            requested: 5,
            capacity: 1.0,
        }
    );
    assert_eq!(
        clock.slept(),
        Duration::from_secs(1),
        "等待量不受订阅者影响"
    );
}

// ---- #206：process_results 首 Err 短路与订阅者解耦 ----

#[test]
fn process_results_next_returns_original_error_under_panicking_subscriber() {
    let log = fresh_log();
    let got: Result<Vec<i32>, &str> = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::iter::process_results")),
        || process_results(vec![Ok(1), Ok(2), Err("boom"), Ok(4)], |it| it.collect()),
    );
    assert_eq!(got, Err("boom"), "首 Err 原样返回，不被订阅者 panic 顶替");
}

#[test]
fn process_results_fold_returns_accumulator_under_panicking_subscriber() {
    let log = fresh_log();
    let seen = std::cell::Cell::new(None);
    let got: Result<usize, &str> = tracing::subscriber::with_default(
        Probe::new(&log, Some("skeleton::iter::process_results")),
        || {
            process_results(vec![Ok::<i32, &str>(1), Err("boom"), Ok(3)], |it| {
                // 计数 fold（非求和）：既走 try_fold 快路径又不踩 unnecessary_fold。
                let acc = it.fold(0usize, |acc, _| acc + 1);
                seen.set(Some(acc));
                0usize
            })
        },
    );
    assert_eq!(got, Err("boom"));
    assert_eq!(
        seen.get(),
        Some(1),
        "fold 短路后仍把累加器交回处理器（Err(acc) 分支完整走完）"
    );
}

#[test]
fn process_results_warns_after_result_is_formed() {
    let log = fresh_log();
    let next_got: Result<Vec<i32>, &str> =
        tracing::subscriber::with_default(Probe::new(&log, None), || {
            process_results(vec![Ok(1), Err("boom")], |it| {
                let v: Vec<i32> = it.collect();
                mark(&log, "processed");
                v
            })
        });
    assert_eq!(next_got, Err("boom"));
    assert_eq!(
        take(&log),
        vec!["processed", "ev:skeleton::iter::process_results"],
        "next 通路：Result 在处理器内成形后才有事件"
    );

    let fold_got: Result<usize, &str> =
        tracing::subscriber::with_default(Probe::new(&log, None), || {
            process_results(vec![Ok::<i32, &str>(1), Err("boom")], |it| {
                let n = it.fold(0usize, |acc, _| acc + 1);
                mark(&log, "processed");
                n
            })
        });
    assert_eq!(fold_got, Err("boom"));
    assert_eq!(
        take(&log),
        vec!["processed", "ev:skeleton::iter::process_results"],
        "fold 通路：累加器交回后才有事件"
    );
}
