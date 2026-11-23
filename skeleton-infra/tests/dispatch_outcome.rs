#![cfg(feature = "runtime")]
//! 结果面（GitHub issue#57）：void `dispatch` 在回灌预算停放（`Deferred`）时
//! **不证明**完整因果完成——那条信息必须经由根回执逃逸到调用方。
//!
//! 钉住三件事：
//! 1. 正常根（因果子树跑完）⇒ `Completed`；
//! 2. 后代被回灌深度预算停放 ⇒ `Deferred`（**不是**成功完成，虽然 void
//!    `dispatch` 在同一路径上照常返回）；
//! 3. 停放队列满 ⇒ 独立的非成功容量终局 + 专用计数（既不报 `Completed`，
//!    也不把被拒记录算成已停放）。
//!
//! 外加：无关并发生产者不得改变哪个根回执报出结果（归属 = 各自账本）。

use skeleton::{Effect, EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::{AdmissionOutcome, DispatchOutcome, SyncStore};
use std::sync::{Arc, Barrier};
use std::thread;

/// 无效果计数器（正常完成路径）。
struct Counter;

impl Reducer for Counter {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        s + a
    }
}

/// 无条件自指回灌：深度预算（64）止步后恰好停放一条记录。
struct EmitSelf;

impl Reducer for EmitSelf {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, _a: u64, out: &mut EffectBuf<u64, Infallible>) -> u64 {
        out.push(Effect::Emit(1));
        s + 1
    }
}

/// 混合根：`0` 自指回灌（必被停放），其余自增（必正常完成）。
struct Mixed;

impl Reducer for Mixed {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, a: u64, out: &mut EffectBuf<u64, Infallible>) -> u64 {
        if a == 0 {
            out.push(Effect::Emit(0));
        }
        s + 1
    }
}

/// 正常根：回执报 `Completed`，且回执身份 = 根 action 的入队序号。
#[test]
fn completed_when_causal_subtree_finishes() {
    let store = SyncStore::new(Counter, NoFx);
    let receipt = store.dispatch_with_receipt(3).expect("静默态必被准入");
    assert_eq!(receipt.seq(), 0, "回执身份 = 根 action 的入队序号");
    assert_eq!(receipt.wait(), DispatchOutcome::Completed);
    assert_eq!(*store.snapshot(), 3, "完成 ⇒ 状态已提交");
}

/// 预算停放：void `dispatch` 照常返回，但结果面如实报「未完成」。
#[test]
fn deferred_is_not_reported_as_completed() {
    let store = SyncStore::new(EmitSelf, NoFx);
    let receipt = store.dispatch_with_receipt(1).expect("准入");
    assert_eq!(
        receipt.wait(),
        DispatchOutcome::Deferred { retained: 1 },
        "后代被预算停放 ⇒ 非成功完成"
    );
    assert!(store.feedback_budget_exceeded() >= 1, "预算止步留痕");
    assert_eq!(store.deferred_actions(), 1, "被停放的记录已留存（可恢复）");

    // 对照：legacy void 路径在同一病理上照常返回——信息只能经结果面逃逸。
    store.dispatch(1);
    assert_eq!(store.deferred_actions(), 2, "void 路径同样停放、同样不可见");
}

/// 停放队列满：独立的非成功容量终局 + 专用计数（不重标成已停放）。
#[test]
fn deferred_capacity_rejection_is_a_distinct_non_success() {
    let store = SyncStore::new(EmitSelf, NoFx);
    store.set_deferred_capacity(1);

    let first = store.dispatch_with_receipt(1).expect("准入");
    assert_eq!(first.wait(), DispatchOutcome::Deferred { retained: 1 });
    assert_eq!(store.deferred_actions(), 1, "前置：停放队列已满");

    let second = store.dispatch_with_receipt(1).expect("准入");
    assert_eq!(
        second.wait(),
        DispatchOutcome::CapacityRejected {
            retained: 0,
            rejected: 1
        },
        "容量拒收既不是 Completed 也不是「已停放」"
    );
    assert_eq!(
        store.deferred_capacity_rejected(),
        1,
        "专用计数 fail-visible"
    );
    assert_eq!(
        store.deferred_actions(),
        1,
        "被拒记录不得计入留存/停放（population 不变）"
    );
    assert_eq!(store.deferred_capacity(), 1, "容量可独立观测");
}

/// 无关并发生产者不得改变哪个根回执报出结果（归属 = 各自账本）。
#[test]
fn unrelated_concurrent_root_cannot_steal_the_outcome() {
    let store = Arc::new(SyncStore::new(Mixed, NoFx));
    let start = Arc::new(Barrier::new(2));
    let looping = {
        let store = Arc::clone(&store);
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            store.dispatch_with_receipt(0).expect("准入").wait()
        })
    };
    start.wait();
    let plain = store.dispatch_with_receipt(7).expect("准入").wait();
    let looping = looping.join().expect("回灌根线程不得 panic");

    assert_eq!(
        plain,
        DispatchOutcome::Completed,
        "无关生产者的停放不得改写本根回执"
    );
    assert_eq!(
        looping,
        DispatchOutcome::Deferred { retained: 1 },
        "回灌根源头的停放只记在它自己的账本上"
    );
}

/// 未准入（闭门）走 typed 拒绝面，不产生回执。
#[test]
fn closed_store_has_no_receipt() {
    let store = SyncStore::new(Counter, NoFx);
    store.close();
    assert_eq!(
        store.dispatch_with_receipt(1).err(),
        Some(AdmissionOutcome::Closed)
    );
}
