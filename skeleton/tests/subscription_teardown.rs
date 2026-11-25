//! `Subscription` 注销契约（#137）：Drop 恰好一次、panic 就地 contain 并计入
//! 可观察计数器、显式 `unsubscribe` 把原始 panic 载荷交给调用方。
//!
//! 契约文本见 `skeleton::Subscription` 类型文档「注销回调契约」。

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use skeleton::{Subscription, subscription_teardown_panic_count};

fn counter() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

#[test]
fn drop_runs_unsubscribe_exactly_once() {
    let calls = counter();
    let c = Arc::clone(&calls);
    let ticket = Subscription::<u64>::new(move || {
        c.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(calls.load(Ordering::SeqCst), 0, "构造不得触发注销");
    drop(ticket);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn drop_contains_callback_panic_and_records_it_once() {
    let calls = counter();
    let c = Arc::clone(&calls);
    let before = subscription_teardown_panic_count();

    // 不 unwind 出 Drop：旧实现在这里会把 panic 抛给调用方，unwind 期即 abort。
    drop(Subscription::<u64>::new(move || {
        c.fetch_add(1, Ordering::SeqCst);
        panic!("injected unsubscribe panic");
    }));

    assert_eq!(calls.load(Ordering::SeqCst), 1, "panic 前回调已执行一次");
    assert!(
        subscription_teardown_panic_count() >= before.saturating_add(1),
        "Drop 面 panic 必须是可观察失败信号（计数器允许被并发测试抬高，但本次必 +1）"
    );
}

#[test]
fn explicit_unsubscribe_returns_original_panic_payload() {
    let calls = counter();
    let c = Arc::clone(&calls);
    let before = subscription_teardown_panic_count();

    let result = Subscription::<u64>::new(move || {
        c.fetch_add(1, Ordering::SeqCst);
        panic!("explicit unsubscribe failure");
    })
    .unsubscribe();

    let payload = result.expect_err("显式注销必须把 panic 交给调用方");
    assert_eq!(
        payload.downcast_ref::<&'static str>(),
        Some(&"explicit unsubscribe failure"),
        "原始载荷不得被格式化或替换"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "回调恰好执行一次");
    assert_eq!(
        subscription_teardown_panic_count(),
        before,
        "显式面的失败已交给调用方，不再计入 Drop 计数器"
    );
}

#[test]
fn explicit_unsubscribe_is_not_replayed_by_drop() {
    let calls = counter();
    let c = Arc::clone(&calls);

    let ok = Subscription::<u64>::new(move || {
        c.fetch_add(1, Ordering::SeqCst);
    })
    .unsubscribe();

    assert!(ok.is_ok(), "无 panic 的显式注销应成功");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "票据已被 unsubscribe 消费，Drop 不得重跑回调"
    );
}

#[test]
fn drop_takes_the_callback_so_a_panic_never_repeats() {
    // panic 发生后票据 Drop 已把回调 take 走：二次 Drop 路径（字段已空）不再执行。
    let calls = counter();
    let c = Arc::clone(&calls);
    drop(Subscription::<u64>::new(move || {
        c.fetch_add(1, Ordering::SeqCst);
        panic!("once only");
    }));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
