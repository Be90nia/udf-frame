#![cfg(feature = "tokio")]
//! issue#119：默认 `FxScope::new()` 的 token 生命周期契约（Option A —— scope
//! 生命周期拥有取消权）。
//!
//! 修复前：`new()` 私有持有内部 `CancelSource` 却**从不**调 `cancel()`，也没有
//! `Drop for FxScope` ⇒ 默认 token 永远为 `false`（连停机期间也不变），与
//! `with_source` 的可见面实质不同。修复后：**首次进入停机**（`shutdown` /
//! `shutdown_timeout` 共用 `Open → Draining` 唯一转换）与 scope **Drop** 触发
//! 内部源的粘性取消；`close()` 不取消；注入外部源时取消权仍归外部。

use std::time::Duration;

use skeleton_infra::fx::{FxScope, cancel_pair};

/// ① 首次停机取消默认 token；重复停机幂等且 canonical 报告不变；`close()` 不算。
#[tokio::test]
async fn default_token_is_cancelled_by_first_shutdown_only() {
    let scope = FxScope::new();
    let token = scope.token();

    scope.close();
    assert!(!token.is_cancelled(), "close 只关接受闸，不是取消");

    let report = scope.shutdown().await;
    assert!(token.is_cancelled(), "首次停机必须取消默认 token");
    assert!(
        scope.token().is_cancelled(),
        "停机后再取的 token 同样看到粘性取消"
    );

    let again = scope.shutdown().await;
    assert_eq!(again, report, "重复停机返回同一次排空的 canonical 报告");
    assert!(token.is_cancelled());
}

/// ② `shutdown_timeout` 走同一次取消转换（两条停机口不各造一套）。
#[tokio::test]
async fn shutdown_timeout_cancels_default_token_too() {
    let scope = FxScope::new();
    let token = scope.token();
    let _report = scope.shutdown_timeout(Duration::from_millis(50)).await;
    assert!(token.is_cancelled(), "两条停机口共用同一次取消转换");
}

/// ③ scope Drop 也通知克隆出去的 token，并唤醒等在 `cancelled()` 上的任务。
#[tokio::test]
async fn scope_drop_cancels_default_token_and_wakes_waiters() {
    let scope = FxScope::new();
    let token = scope.token();
    let waiter = token.clone();
    let handle = scope
        .spawn_detached(async move { waiter.cancelled().await })
        .expect("接受闸开着 ⇒ 脱管 spawn 可用");
    // 让等待者先跑到 `wait_for` 的挂起点（单线程 runtime 让渡一次即可）。
    tokio::task::yield_now().await;

    drop(scope);

    assert!(
        token.is_cancelled(),
        "scope 消失后 token 必须看到取消（禁永久 false）"
    );
    tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("Drop 必须唤醒等待取消的任务")
        .expect("等待任务不得 panic");
}

/// ④ 注入外部源时取消权归外部：scope 停机与 Drop 都不得改写它，外部取消照常传导。
#[tokio::test]
async fn injected_source_stays_externally_owned() {
    let (source, _observer) = cancel_pair();
    let control = source.clone();
    let token = {
        let scope = FxScope::new().with_source(source);
        let token = scope.token();
        let _report = scope.shutdown().await;
        assert!(!token.is_cancelled(), "scope 停机不得取消外部注入的源");
        token
    }; // scope drop

    assert!(!token.is_cancelled(), "scope Drop 也不得改写外部源的取消态");
    assert!(!control.is_cancelled());

    control.cancel();
    assert!(token.is_cancelled(), "外部取消照常传导给已克隆的 token");
}
