#![cfg(feature = "runtime")]
//! issue#42 回归：本地订阅回调重入不得被误判为用户 panic。
//!
//! 修复前 `local_notify` 持表 `borrow_mut` 跑用户回调，回调内任何再入
//! （跨 store dispatch 的嵌套通知、callback-time subscribe/unregister、
//! 自注销）都会炸 `BorrowMutError`，被外层 `catch_unwind` 吞成「用户回调
//! panic」→ 健康订阅被静默摘除。修复后：快照句柄、不持表借用调回调。

use skeleton::{ActionSender, Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::{LocalSubscription, SyncStore as Store};
use std::cell::{Cell, RefCell};
use std::convert::Infallible;
use std::rc::Rc;

#[derive(Clone)]
struct Count(u32);

enum CounterAction {
    Initial,
    Inc,
}

struct CounterFeature;

impl Reducer for CounterFeature {
    type State = Count;
    type Action = CounterAction;
    type Op = Infallible;
    fn initial(&self) -> Count {
        Count(0)
    }
    fn reduce(
        &self,
        s: &Count,
        a: CounterAction,
        _out: &mut EffectBuf<CounterAction, Infallible>,
    ) -> Count {
        match a {
            CounterAction::Initial => s.clone(),
            CounterAction::Inc => Count(s.0 + 1),
        }
    }
}

struct NullRuntime;

impl EffectRuntime<CounterAction, Infallible> for NullRuntime {
    fn run(
        &mut self,
        _effect: Effect<CounterAction, Infallible>,
        _back: &ActionSender<CounterAction>,
    ) {
    }
}

fn hits() -> Rc<Cell<u32>> {
    Rc::new(Cell::new(0))
}

/// 回调内 dispatch 另一个 store（跨 store 嵌套）：嵌套通知必须完整触达
/// B 的本地订阅，且 A 的健康订阅不得被误摘（修复前 BorrowMutError 被
/// 吞成 A 的用户 panic → A 静默摘除 + 嵌套通知 abort）。
#[test]
fn cross_store_reentrant_dispatch_not_misjudged_as_user_panic() {
    let a = Store::new(CounterFeature, NullRuntime);
    let a_hits = hits();
    let b_hits = hits();
    let ah = Rc::clone(&a_hits);
    let bh = Rc::clone(&b_hits);
    let _ta = a.subscribe_local(move |_| {
        ah.set(ah.get() + 1);
        let b = Store::new(CounterFeature, NullRuntime);
        let bh2 = Rc::clone(&bh);
        // 每轮 A 的回调都新建 B 并登记，再嵌套 dispatch B：B 的本地订阅
        // 必须在本轮嵌套通知中被触发。
        let _tb = b.subscribe_local(move |_| bh2.set(bh2.get() + 1));
        b.dispatch(CounterAction::Inc);
    });
    a.dispatch(CounterAction::Initial);
    assert_eq!(a_hits.get(), 1);
    assert_eq!(b_hits.get(), 1, "嵌套 dispatch 应触达 B 的本地订阅");
    assert_eq!(a.subscriber_panics(), 0, "A 的健康订阅不得被误摘");
    a.dispatch(CounterAction::Inc);
    assert_eq!(a_hits.get(), 2, "A 的订阅应持续存活");
    assert_eq!(b_hits.get(), 2);
}

thread_local! {
    static LATE_STORE: RefCell<Option<Store<Count, CounterAction>>> = const { RefCell::new(None) };
}

/// 回调内 subscribe_local：新注册本轮不可见（快照确定性），下一轮可见；
/// 注册本身不得 panic、不得误摘在触回调。
#[test]
fn callback_time_subscribe_visible_next_round() {
    let a = Store::new(CounterFeature, NullRuntime);
    LATE_STORE.with(|slot| *slot.borrow_mut() = Some(a));
    let first_hits = hits();
    let late_hits = hits();
    let tickets: Rc<RefCell<Vec<LocalSubscription>>> = Rc::new(RefCell::new(Vec::new()));
    let fh = Rc::clone(&first_hits);
    let lh = Rc::clone(&late_hits);
    let tk = Rc::clone(&tickets);
    let _t = LATE_STORE.with(|slot| {
        slot.borrow().as_ref().unwrap().subscribe_local(move |_| {
            fh.set(fh.get() + 1);
            if tk.borrow().is_empty() {
                let lh2 = Rc::clone(&lh);
                let late = LATE_STORE.with(|slot| {
                    slot.borrow()
                        .as_ref()
                        .unwrap()
                        .subscribe_local(move |_| lh2.set(lh2.get() + 1))
                });
                tk.borrow_mut().push(late);
            }
        })
    });
    let dispatch = |act| LATE_STORE.with(|slot| slot.borrow().as_ref().unwrap().dispatch(act));
    dispatch(CounterAction::Initial);
    assert_eq!(first_hits.get(), 1);
    assert_eq!(late_hits.get(), 0, "回调期间新注册的本轮不可见");
    assert_eq!(
        LATE_STORE.with(|slot| slot.borrow().as_ref().unwrap().subscriber_panics()),
        0,
        "callback-time 注册不得被误判为用户 panic"
    );
    dispatch(CounterAction::Inc);
    assert_eq!(first_hits.get(), 2);
    assert_eq!(late_hits.get(), 1, "新注册下一轮可见");
    LATE_STORE.with(|slot| *slot.borrow_mut() = None);
}

/// 回调内 drop 排位靠后的另一票据：本轮剩余通知里该订阅跳过（Drop=退订
/// 即时生效，不复活死订阅），且不得 panic / 误摘在触回调。
#[test]
fn callback_time_unsubscribe_of_later_sub_skips_it_same_round() {
    let a = Store::new(CounterFeature, NullRuntime);
    let a_hits = hits();
    let b_hits = hits();
    let ticket_b: Rc<RefCell<Option<LocalSubscription>>> = Rc::new(RefCell::new(None));
    let ah = Rc::clone(&a_hits);
    let holder = Rc::clone(&ticket_b);
    // A 先登记（通知序在前），其回调注销排位靠后的 B。
    let _ta = a.subscribe_local(move |_| {
        ah.set(ah.get() + 1);
        *holder.borrow_mut() = None;
    });
    let bh = Rc::clone(&b_hits);
    *ticket_b.borrow_mut() = Some(a.subscribe_local(move |_| bh.set(bh.get() + 1)));
    a.dispatch(CounterAction::Initial);
    assert_eq!(a_hits.get(), 1);
    assert_eq!(b_hits.get(), 0, "本轮内已注销的排位靠后订阅应被跳过");
    assert_eq!(
        a.subscriber_panics(),
        0,
        "callback-time 注销不得被误判为用户 panic"
    );
    a.dispatch(CounterAction::Inc);
    assert_eq!(a_hits.get(), 2);
    assert_eq!(b_hits.get(), 0, "已注销订阅此后不再触发");
}

/// 回调内 drop 自己：正常注销路径（非 panic），计数不得上浮，此后不再触发。
#[test]
fn callback_dropping_own_ticket_unregisters_without_panic_count() {
    let a = Store::new(CounterFeature, NullRuntime);
    let hits_cnt = hits();
    let self_ticket: Rc<RefCell<Option<LocalSubscription>>> = Rc::new(RefCell::new(None));
    let h = Rc::clone(&hits_cnt);
    let holder = Rc::clone(&self_ticket);
    *self_ticket.borrow_mut() = Some(a.subscribe_local(move |_| {
        h.set(h.get() + 1);
        *holder.borrow_mut() = None;
    }));
    a.dispatch(CounterAction::Initial);
    assert_eq!(hits_cnt.get(), 1);
    assert_eq!(a.subscriber_panics(), 0, "自注销不是 panic，不得计数");
    a.dispatch(CounterAction::Inc);
    assert_eq!(hits_cnt.get(), 1, "注销后不再触发");
}

/// 真用户 panic 仍按文档 fail-closed 摘除 + 计数 + 成因留痕（不回归）。
#[test]
fn real_user_panic_still_fail_closed_removal() {
    let a = Store::new(CounterFeature, NullRuntime);
    let hits_cnt = hits();
    let h = Rc::clone(&hits_cnt);
    let _t = a.subscribe_local(move |_| {
        h.set(h.get() + 1);
        panic!("boom-user");
    });
    a.dispatch(CounterAction::Initial);
    assert_eq!(hits_cnt.get(), 1);
    assert_eq!(a.subscriber_panics(), 1);
    assert!(
        a.subscriber_panic_last_cause()
            .unwrap()
            .contains("boom-user"),
        "成因摘要应留痕"
    );
    a.dispatch(CounterAction::Inc);
    assert_eq!(hits_cnt.get(), 1, "毒回调摘除后不再触发");
}
