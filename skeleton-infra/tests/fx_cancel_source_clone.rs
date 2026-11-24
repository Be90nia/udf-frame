#![cfg(feature = "tokio")]
//! #56 Stage B G1：`CancelSource` 可 Clone（**同一**取消态，不是独立源）——宿主把
//! 源按值交给 `with_source` 之后仍持有触发取消的句柄。
//!
//! 验收面：
//! ① 源按值移入 scope 后，**预先 clone 的句柄**触发取消 ⇒ scope token 观察到，
//!    等待取消的托管任务正常终局（in_flight 归零）；
//! ② clone 是同一取消态的第二句柄：原句柄全部丢弃后仍能取消，取消幂等，且取消
//!    发生在订阅之前时之后接入的 scope 也观察到 `true`（与 #68 粘性叠加）。
//!
//! 一致性：有界等待（让渡 + 超时），禁 sleep。

use skeleton_infra::fx::{FxScope, cancel_pair};

#[path = "../src/test_support/guard.rs"]
mod guards;

use guards::{bounded, wait_until};

/// G1①：`with_source` 取走原句柄后，clone 句柄仍能触发取消——观察面与在途面同时收敛。
#[tokio::test]
async fn cloned_source_cancels_after_source_moves_into_scope() {
    let (source, _) = cancel_pair();
    let handle = source.clone();
    let scope = FxScope::new().with_source(source);

    let token = scope.token();
    let task = scope
        .spawn_tracked(async move { token.cancelled().await })
        .expect("accepting 闸开着 ⇒ 托管 spawn 可用");
    // 单线程 runtime：让渡一次即轮到等待任务跑到 wait_for 的挂起点。
    tokio::task::yield_now().await;
    assert!(!scope.token().is_cancelled(), "取消之前不得误报取消态");

    handle.cancel();
    assert!(
        scope.token().is_cancelled(),
        "clone 出的句柄必须与移入 scope 的源共享同一取消态"
    );
    bounded("cancel 唤醒等待取消的托管任务", task.join())
        .await
        .expect("等待取消的任务正常返回，不得是 Cancelled/Panicked");
    // join 只消费结果，in_flight 由任务 future 的 Drop 归零 ⇒ 有界等待其收敛。
    wait_until("in_flight 归零", || scope.in_flight() == 0).await;
}

/// G1②：clone 是**同一取消态**的第二句柄，不是独立源——原句柄丢光后仍能取消，
/// 取消幂等、单向，且取消之后才接入的 scope 同样观察到 `true`（#68 粘性叠加）。
#[tokio::test]
async fn cloned_handle_stays_usable_after_originals_dropped() {
    let (source, token) = cancel_pair();
    let only = source.clone();
    drop(source); // 原句柄丢弃 ⇒ 唯一可触发句柄 = clone
    drop(token); // #68：此刻没有任何接收者

    only.cancel();
    only.cancel(); // 幂等：重复 cancel 不回退（单向 false → true）
    assert!(only.is_cancelled(), "clone 句柄自身必须看到取消态");

    // #68 叠加：取消发生在订阅之前 ⇒ 之后接入的 scope 立即观察到粘性 true；
    // 两个 scope 共享同一取消态 = clone 不是独立源。
    let first = FxScope::new().with_source(only.clone());
    let second = FxScope::new().with_source(only.clone());
    assert!(
        first.token().is_cancelled(),
        "取消之后才接入的 scope 必须观察到既有取消态"
    );
    assert!(
        second.token().is_cancelled(),
        "clone 共享同一取消态，不是独立源"
    );
}
