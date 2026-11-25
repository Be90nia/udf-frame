//! issue#133：本 scope 的托管任务不得发起**本 scope** 的停机。
//!
//! 理由：该任务自身就在待 abort 的生命周期集里，成为 owner 后会被自己的
//! `abort_all` 取消 ⇒ 共享排空记成中断、调用方永远拿不到 canonical 报告。判定在
//! 赢 `Open → Draining` **之前**做，命中即 fail-fast panic 且**不动**闸/排空状态。
//!
//! 边界：外部协调者、本 scope 的 detached 任务、**其他** scope 的托管任务照旧可停机。

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::Duration;

use skeleton_infra::fx::{FxScope, TaskError};

fn self_shutdown_message(err: TaskError) -> String {
    match err {
        TaskError::Panicked(msg) => {
            assert!(
                msg.contains("issue#133"),
                "自停机拒绝必须具名（含票号）：{msg}"
            );
            msg
        }
        other => panic!("自停机必须 fail-fast panic，实际 {other:?}"),
    }
}

#[tokio::test]
async fn tracked_task_cannot_shutdown_its_own_scope() {
    let scope = Arc::new(FxScope::new());
    let inside = Arc::clone(&scope);
    let handle = scope
        .spawn_tracked(async move {
            let _ = inside.shutdown().await;
        })
        .expect("spawn tracked");
    let msg = self_shutdown_message(handle.join().await.expect_err("自停机必须被拒"));
    assert!(msg.contains("shutdown"), "文案指出被拒的入口：{msg}");

    // 拒绝不改状态：闸仍开、排空未被启动 ⇒ 外部照常收尾并拿到 canonical 报告。
    assert!(!scope.is_closed(), "自停机拒绝不得关 accepting 闸");
    let ok = scope.spawn_tracked(async { 7u32 }).expect("闸仍开");
    assert_eq!(ok.join().await, Ok(7));
    let report = scope.shutdown().await;
    assert!(
        !report.drain_interrupted,
        "外部停机仍是 canonical 报告（未被自停机污染）"
    );
    assert_eq!(
        report.panicked.len(),
        1,
        "自停机的 fail-fast panic 如实入账"
    );
    assert_eq!(report.completed, 1);
}

#[tokio::test]
async fn tracked_task_cannot_shutdown_timeout_its_own_scope() {
    let scope = Arc::new(FxScope::new());
    let inside = Arc::clone(&scope);
    let handle = scope
        .spawn_tracked(async move {
            let _ = inside.shutdown_timeout(Duration::from_millis(10)).await;
        })
        .expect("spawn tracked");
    let msg = self_shutdown_message(handle.join().await.expect_err("自停机必须被拒"));
    assert!(
        msg.contains("shutdown_timeout"),
        "文案指出被拒的入口：{msg}"
    );
    assert!(!scope.is_closed(), "自停机拒绝不得关 accepting 闸");
}

#[tokio::test]
async fn tracked_task_may_shutdown_another_scope() {
    let victim = Arc::new(FxScope::new());
    let host = Arc::new(FxScope::new());
    let target = Arc::clone(&victim);
    let handle = host
        .spawn_tracked(async move { target.shutdown().await })
        .expect("spawn tracked");
    let report = handle.join().await.expect("跨 scope 停机照旧允许");
    assert!(!report.drain_interrupted);
}

#[tokio::test]
async fn detached_task_may_initiate_shutdown() {
    let scope = Arc::new(FxScope::new());
    let inside = Arc::clone(&scope);
    let handle = scope
        .spawn_detached(async move { inside.shutdown().await })
        .expect("spawn detached");
    let report = handle.await.expect("detached 任务不被停机排空 abort");
    assert!(!report.drain_interrupted, "detached 任务仍是合法 owner");
}
