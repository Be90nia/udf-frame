//! issue#60 验收第 4 条机械锚：**未订阅审计**时 dispatch 热路径零分配。
//!
//! 口径（与 `tests/run_id_diagnostics.rs::allocs` 同一件仪器、同一口径）：
//! 本线程事件计数分配器，窗内 `alloc`/`alloc_zeroed`/`realloc` 各计 1 事件，
//! `dealloc` 不计；只计本线程窗内（libtest 并发线程的分配不进窗）。故
//! **净分配 = 窗内分配事件数**——分配事件为 0 时净（分配−释放）必为 0；若
//! 「先分配后释放」也算分配事件，本断言因此**强于**净 0（无瞬时分配）。
//!
//! 断言不恒真（鉴别力）：
//! - 仪器自检：窗内一次 `Vec::with_capacity` 必须计 1（分配器挂了/门没开 ⇒ 红）；
//! - 审计路径正向控制：订阅激活（环预分配 + 回调装箱）在**同一个窗**内必须
//!   产生大于 0 的事件，且订阅后让回调收件（`Vec::push` 首次增长）的 dispatch
//!   也大于 0——证明同一窗口确实覆盖审计路径上真实发生的分配，未订阅路径的 0
//!   不是仪器失明。
//!
//! 「一次 gate load、无锁」是**结构**保证（`runtime::audit_emit` 的闸判先于任何
//! 状态访问，闸 false 即返）；分配面才是本文件可机械测量的部分。

#![cfg(feature = "runtime")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::AuditEvent;
use skeleton_infra::runtime::SyncStore;

/// 效果计数（转发路径不依赖审计的直接可观测面）。
#[derive(Default)]
struct Tally {
    runs: AtomicU32,
}

struct Sink {
    tally: Arc<Tally>,
}

impl EffectRuntime<Msg, ()> for Sink {
    fn run(&mut self, effect: Effect<Msg, ()>, _: &ActionSender<Msg>) {
        if matches!(effect, Effect::Run { .. }) {
            self.tally.runs.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Msg {
    Run(u64),
}

struct Fixture;

impl Reducer for Fixture {
    type State = u32;
    type Action = Msg;
    type Op = ();

    fn initial(&self) -> u32 {
        0
    }

    fn reduce(&self, s: &u32, a: Msg, out: &mut EffectBuf<Msg, ()>) -> u32 {
        match a {
            Msg::Run(id) => out.push(Effect::Run {
                id: EffectId::new(id),
                op: (),
            }),
        }
        s + 1
    }
}

fn store() -> (SyncStore<u32, Msg, ()>, Arc<Tally>) {
    let tally = Arc::new(Tally::default());
    let store = SyncStore::new(
        Fixture,
        Sink {
            tally: Arc::clone(&tally),
        },
    );
    (store, tally)
}

// ---- 本线程事件计数分配器（形态照 tests/run_id_diagnostics.rs / alloc_site.rs）----

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、
// 无析构注册，在分配器内读取不会递归进分配。
thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: 纯转发 System，仅在窗内转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// op 执行期的分配事件数（瞬时生灭也计入；只计本线程窗内）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    IN_WINDOW.with(|w| w.set(true));
    let before = EVENTS.load(Ordering::Relaxed);
    let out = op();
    black_box(&out);
    let events = EVENTS.load(Ordering::Relaxed) - before;
    IN_WINDOW.with(|w| w.set(false));
    events
}

/// 仪器自检：窗内分配必须可见——防「分配器没挂上/门没开」的假绿。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

/// 未订阅审计：同一 store 连续产 `Run` 效果的 dispatch 净分配 == 0。
///
/// 伴随断言（防假零）：效果确实到了解释器、审计环确实为空（未订阅 ⇒ 无入账）。
#[test]
fn unsubscribed_dispatch_with_effects_allocates_nothing() {
    let (store, tally) = store();

    // 预热：队列/效果缓冲/账本池达稳态（首轮分配不属于被观测面）。
    for id in 0..64 {
        store.dispatch(Msg::Run(id));
    }
    let runs_before = tally.runs.load(Ordering::SeqCst);

    let events = allocs(|| {
        for id in 1000..1032 {
            store.dispatch(Msg::Run(id));
        }
    });

    assert_eq!(events, 0, "未订阅审计：产 Run 效果的 dispatch 必须零分配");
    assert_eq!(
        tally.runs.load(Ordering::SeqCst),
        runs_before + 32,
        "路径真跑到：效果全部到达解释器（防「dispatch 没干活」的假零）"
    );
    assert!(
        store.take_events().is_empty(),
        "未订阅 ⇒ 审计环无入账（gate 快路径未落到 record 分支）"
    );
}

/// 正向控制（鉴别力）：同一件仪器在**路径真的做事**时必须亮。三条控制臂都断
/// 大于 0：
/// 1. 冷启动 dispatch（活跃队列/效果缓冲首次增长）= 框架侧分配；
/// 2. 订阅激活本身（环预分配 + 回调装箱）= 审计设施的真实分配；
/// 3. 订阅后 dispatch 到达回调（回调收件 `Vec::push` 首次增长）——证明窗口覆盖
///    整条 dispatch 调用（不是只覆盖了某个无分配的外壳）。
///
/// 若把「未订阅 dispatch 零分配」的断言写坏（例如仪器失明恒 0），本控制红。
#[test]
fn audit_path_allocations_are_visible_to_the_same_counter() {
    // 冷启动对照：同一 fixture 的**首次** dispatch（活跃队列/效果缓冲首次增长）
    // 是框架侧真实分配 ⇒ 同一窗口必亮（证明仪器覆盖 dispatch 内部，而不是只看
    // 一个无分配的外壳）。
    let (cold_store, _cold_tally) = store();
    let cold = allocs(|| cold_store.dispatch(Msg::Run(1)));
    assert!(
        cold > 0,
        "冷启动 dispatch 的框架侧分配必须可观测（实得 {cold} 事件）"
    );

    let (store, _tally) = store();

    let seen = Arc::new(Mutex::new(Vec::<AuditEvent>::new()));
    let seen_cb = Arc::clone(&seen);
    // 票据绑到窗外（窗内只测订阅激活的分配；票据 Drop 即关流，不能随窗析构）。
    let mut ticket = None;
    let subscribe = allocs(|| {
        ticket = Some(store.subscribe_events(16, move |e| {
            seen_cb.lock().expect("seen lock").push(*e);
            // 每次回调都确定性分配一次：让「回调被执行」在分配面上不可隐藏，
            // 弃票后的 0 才能证明 gate 真的关断（不是「回调跑了但恰好没分配」）。
            black_box(Vec::<u8>::with_capacity(8));
        }));
    });
    let _ticket = ticket.expect("订阅必须成功装槽");
    assert!(
        subscribe > 0,
        "订阅激活必须可观测到框架侧分配（实得 {subscribe} 事件）"
    );

    let events = allocs(|| store.dispatch(Msg::Run(1)));
    assert!(
        events > 0,
        "订阅态的 dispatch 必须可观测到审计路径分配（实得 {events} 事件）"
    );
    assert!(
        !seen.lock().expect("seen lock").is_empty(),
        "回调确实收到事件（控制臂真的走到了审计路径）"
    );

    // 关流恢复快路径：同一 store 弃票后再测必须回到 0——证明计数器按路径
    // 区分（不是「一旦亮过就恒亮」），未订阅断言与订阅态控制互为对照。
    drop(_ticket);
    let after_close = allocs(|| store.dispatch(Msg::Run(2)));
    assert_eq!(
        after_close, 0,
        "弃票关流后 dispatch 必须回到零分配（gate 落闸恢复快路径）"
    );
}
