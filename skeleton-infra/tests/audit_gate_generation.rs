//! issue#60（订阅代次/闸正确性）：陈旧票据的 Drop **绝不能**禁用更新的 sink，
//! 且两种顺序下随后一个事件都必须同时证明「环历史」与「当前代次回调」仍活跃。
//!
//! 强制交错（暂停在「槽已摘、闸未落」窗口内插队）的确定性夹具在
//! `src/audit.rs` 的白盒测试里（`cfg(test)` 暂停点，不扩公共面）；本文件覆盖
//! **公共面**的顺序语义：两种交替顺序、被取代票据的 drop、多代次叠替。
//!
//! 词表（与环/投递分账同一套）：
//! - **环历史** = `take_events()`（取证面，容量因丢最旧）；
//! - **实时回调** = 当前代次 `subscribe_events` 的回调命中（背压因）。

#![cfg(feature = "runtime")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;

/// 计数 reducer（零效果）：只为驱动 dispatch 产审计事件。
struct Tick;

impl Reducer for Tick {
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

fn counting_subscription(
    store: &SyncStore<u64, u64, Infallible>,
) -> (skeleton_infra::audit::AuditTicket, Arc<AtomicUsize>) {
    let hits = Arc::new(AtomicUsize::new(0));
    let ticket = {
        let hits = Arc::clone(&hits);
        store.subscribe_events(64, move |_| {
            hits.fetch_add(1, Ordering::SeqCst);
        })
    };
    (ticket, hits)
}

/// 顺序 ①：先丢旧票据（无人接手）→ 再订阅新代次。随后一个事件必须同时进入
/// 环历史与当前代次回调（旧票据的落闸不得越过这次开闸）。
#[test]
fn drop_then_subscribe_keeps_ring_and_current_callback_live() {
    let store = SyncStore::new(Tick, NoFx);
    let (stale, stale_hits) = counting_subscription(&store);
    drop(stale);

    let (_current, current_hits) = counting_subscription(&store);
    store.dispatch(1u64);

    assert!(
        !store.take_events().is_empty(),
        "随后一个事件必须入环（取证面活跃）"
    );
    assert!(
        current_hits.load(Ordering::SeqCst) > 0,
        "随后一个事件必须投递到当前代次回调（实时流活跃）"
    );
    assert_eq!(
        stale_hits.load(Ordering::SeqCst),
        0,
        "已摘除的旧回调不得再收到事件"
    );
}

/// 顺序 ②：先订阅新代次（旧票据被覆盖）→ 再丢旧票据（代次失配）。旧票据既
/// 不得清槽、也不得落闸。
#[test]
fn subscribe_then_drop_stale_ticket_keeps_ring_and_current_callback_live() {
    let store = SyncStore::new(Tick, NoFx);
    let (stale, stale_hits) = counting_subscription(&store);
    let (_current, current_hits) = counting_subscription(&store);
    assert_eq!(
        store.audit_preempted(),
        1,
        "单槽二次订阅覆盖前订必须留痕（fail-visible）"
    );

    drop(stale); // 代次失配：禁动槽与闸
    store.dispatch(1u64);

    assert!(
        !store.take_events().is_empty(),
        "被取代票据的 drop 不得压制环历史"
    );
    assert!(
        current_hits.load(Ordering::SeqCst) > 0,
        "被取代票据的 drop 不得禁用当前代次回调"
    );
    assert_eq!(stale_hits.load(Ordering::SeqCst), 0);
}

/// 多代次叠替：只有**当前**代次可以关闭事件流；任一陈旧票据的 drop 都不得。
#[test]
fn only_the_current_generation_may_close_the_stream() {
    let store = SyncStore::new(Tick, NoFx);
    let (gen1, hits1) = counting_subscription(&store);
    let (gen2, _hits2) = counting_subscription(&store);
    let (gen3, hits3) = counting_subscription(&store);

    drop(gen1);
    drop(gen2);
    store.dispatch(1u64);
    assert!(
        !store.take_events().is_empty(),
        "陈旧代次的 drop 压制了当前代的环历史"
    );
    assert!(
        hits3.load(Ordering::SeqCst) > 0,
        "陈旧代次的 drop 禁用了当前代回调"
    );
    assert_eq!(hits1.load(Ordering::SeqCst), 0);

    // 当前代次 Drop = 关流（闸与槽同生死）：此后不再入环。
    drop(gen3);
    store.dispatch(1u64);
    assert!(
        store.take_events().is_empty(),
        "当前代次 drop 后事件流必须关闭（无订阅者不得残留记录）"
    );
}

/// 关流后重新订阅：闸必须重新打开（落闸不得粘在新订阅上）。
#[test]
fn resubscribe_after_close_reopens_the_gate() {
    let store = SyncStore::new(Tick, NoFx);
    let (first, _hits_first) = counting_subscription(&store);
    drop(first);
    store.dispatch(1u64);
    assert!(store.take_events().is_empty(), "前置：关流态不记录");

    let (_second, hits_second) = counting_subscription(&store);
    store.dispatch(1u64);
    assert!(
        hits_second.load(Ordering::SeqCst) > 0,
        "重新订阅必须重新开闸（实时流恢复）"
    );
    assert!(!store.take_events().is_empty(), "重新订阅后环历史恢复");
}
