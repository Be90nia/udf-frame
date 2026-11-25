//! #129：`ChannelSink` 的 trait 方法一律在槽位锁外调用。
//!
//! `ChannelSink` 是公开 trait、实现是消费方代码——`level()` / `dropped_lines()`
//! 都不是类型层可证的纯 getter，重入 registry 会拿同一槽位的锁。故：
//!
//! ① `register` 先取 `channel()`/`level()`，再进槽位写锁（重入 `level()` 不死锁）；
//! ② `dropped` / `dropped_total` 锁内只克隆 `Arc`，`dropped_lines()` 锁外调用。
//!
//! 每个断言都放在「带超时的独立线程」里：旧实现（锁内调 trait 方法）会自死锁，
//! 于是测试**红得干净**而不是把 CI 挂死。旧实现的死锁机制：同一线程持有槽位读锁
//! （`dropped`）或写锁（`register`）时，`dropped_lines()`/`level()` 重入
//! `register` 同通道需要同一槽位的写锁 ⇒ 自等 ⇒ 永不返回。

#![cfg(feature = "log")]

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Weak, mpsc};
use std::time::Duration;

use skeleton_infra::logx::{Channel, ChannelError, ChannelRegistry, ChannelSink};

/// 超时守卫：动作在独立线程跑，超时即判「trait 方法在槽位锁内被调用」。
/// （挂起的线程随进程结束丢弃，不会阻塞测试二进制退出。）
fn within<T: Send + 'static>(what: &str, action: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(action());
    });
    match rx.recv_timeout(Duration::from_secs(10)) {
        Ok(value) => value,
        Err(_) => panic!("{what}：10s 内未完成——ChannelSink 方法在槽位锁内被调用（#129 自死锁）"),
    }
}

/// 普通旁路 sink（只用于占位/探针，不参与断言）。
struct NullSink(Channel);

impl ChannelSink for NullSink {
    fn channel(&self) -> Channel {
        self.0
    }

    fn level(&self) -> log::LevelFilter {
        log::LevelFilter::Trace
    }

    fn write(&self, _level: log::Level, _target: &str, _message: &str) {}

    fn dropped_lines(&self) -> u64 {
        0
    }
}

/// `level()` 里重入 `register` 同通道的实现：旧实现在槽位写锁内调 `level()`，
/// 该重入会等同一把写锁 ⇒ 自死锁。
struct ReentrantLevelSink {
    registry: Weak<ChannelRegistry>,
    /// 0=未执行 1=内层 Ok 2=内层 Err。
    nested: AtomicU8,
}

impl ChannelSink for ReentrantLevelSink {
    fn channel(&self) -> Channel {
        Channel::General
    }

    fn level(&self) -> log::LevelFilter {
        if let Some(registry) = self.registry.upgrade() {
            let nested = registry.register(Arc::new(NullSink(Channel::General)));
            self.nested
                .store(if nested.is_ok() { 1 } else { 2 }, Ordering::SeqCst);
        }
        log::LevelFilter::Trace
    }

    fn write(&self, _level: log::Level, _target: &str, _message: &str) {}

    fn dropped_lines(&self) -> u64 {
        0
    }
}

/// `dropped_lines()` 里重入 `register` 同通道的实现：旧实现在槽位读锁内调它，
/// 重入要写锁 ⇒ 写等读 ⇒ 自死锁。
struct ReentrantDroppedSink {
    registry: Weak<ChannelRegistry>,
    calls: AtomicU64,
}

impl ChannelSink for ReentrantDroppedSink {
    fn channel(&self) -> Channel {
        Channel::General
    }

    fn level(&self) -> log::LevelFilter {
        log::LevelFilter::Trace
    }

    fn write(&self, _level: log::Level, _target: &str, _message: &str) {}

    fn dropped_lines(&self) -> u64 {
        if let Some(registry) = self.registry.upgrade() {
            // 已注册 ⇒ 新实现下返回 Err(AlreadyRegistered) 即可；旧实现先死锁。
            let _ = registry.register(Arc::new(NullSink(Channel::General)));
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        7
    }
}

/// ① `register` 在进锁之前求值 `level()`：其中重入 `register` 同通道必须能完成
/// （外层随后因同通道已占用而 fail-visible）。
#[test]
fn register_evaluates_level_before_taking_the_slot_lock() {
    let registry = Arc::new(ChannelRegistry::new());
    let sink = Arc::new(ReentrantLevelSink {
        registry: Arc::downgrade(&registry),
        nested: AtomicU8::new(0),
    });

    let outcome = within("register", {
        let registry = Arc::clone(&registry);
        let sink = Arc::clone(&sink);
        move || {
            registry
                .register(sink)
                .map_err(|err| matches!(err, ChannelError::AlreadyRegistered { .. }))
        }
    });

    assert_eq!(
        outcome,
        Err(true),
        "外层 register 必须 fail-visible（内层重入已占用同通道）"
    );
    assert_eq!(
        sink.nested.load(Ordering::SeqCst),
        1,
        "level() 中的重入 register 必须成功完成——证明它运行在槽位锁之外"
    );
}

/// ② `dropped` 锁内只克隆 `Arc`：`dropped_lines()` 重入 `register` 同通道必须能
/// 完成（旧实现持读锁时该重入等写锁 ⇒ 自死锁）。
#[test]
fn dropped_calls_sink_counter_outside_the_slot_lock() {
    let registry = Arc::new(ChannelRegistry::new());
    let sink = Arc::new(ReentrantDroppedSink {
        registry: Arc::downgrade(&registry),
        calls: AtomicU64::new(0),
    });
    registry
        .register(Arc::clone(&sink) as Arc<dyn ChannelSink>)
        .expect("首注恒 Ok");

    let value = within("dropped", {
        let registry = Arc::clone(&registry);
        move || registry.dropped(Channel::General)
    });
    assert_eq!(value, Some(7), "读数来自 sink 自计数");
    assert_eq!(
        sink.calls.load(Ordering::SeqCst),
        1,
        "dropped_lines() 恰好被调用一次且已完成"
    );
}

/// ③ `dropped_total` 同样逐槽克隆 `Arc` 后调用（三槽求和语义不变）。
#[test]
fn dropped_total_calls_sink_counters_outside_slot_locks() {
    let registry = Arc::new(ChannelRegistry::new());
    let sink = Arc::new(ReentrantDroppedSink {
        registry: Arc::downgrade(&registry),
        calls: AtomicU64::new(0),
    });
    registry
        .register(Arc::clone(&sink) as Arc<dyn ChannelSink>)
        .expect("首注恒 Ok");

    let total = within("dropped_total", {
        let registry = Arc::clone(&registry);
        move || registry.dropped_total()
    });
    assert_eq!(total, 7, "未注册通道不计数，注册通道取 sink 自计数");
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
}
