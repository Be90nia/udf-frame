#![cfg(feature = "runtime")]
//! 历史 Run-ID 诊断（GitHub issue#62）：**默认关闭**的窄口径 runtime 诊断设置。
//!
//! 定稿口径（维护者）：
//! - 默认关闭；`Run`/`Cancel` 的转发路径与诊断是否启用**无关**；
//! - 启用后是有界最近集 + 三态分类（known / definitely-unknown / not-tracked）；
//!   **被驱逐的 id 绝不判成 definitely-unknown**；
//! - 暴露已跟踪数 / 配置容量 / 饱和驱逐计数；
//! - 旧 `cancel_unknown_id` 的含义显式化：**只数视界完整且无驱逐时的精确 miss**，
//!   默认关闭 ⇒ 恒 0（不静默改义，见其 rustdoc）；
//! - 禁用路径零锁零分配（本文件用本线程事件计数分配器实测分配面）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::{DEFAULT_RUN_ID_TRACKING_CAPACITY, RunIdClass, SyncStore};

/// 效果计数（转发路径不依赖诊断的直接可观测面）。
#[derive(Default)]
struct Tally {
    runs: AtomicU32,
    cancels: AtomicU32,
}

struct Sink {
    tally: Arc<Tally>,
}

impl EffectRuntime<Msg, ()> for Sink {
    fn run(&mut self, effect: Effect<Msg, ()>, _: &ActionSender<Msg>) {
        match effect {
            Effect::Run { .. } => {
                self.tally.runs.fetch_add(1, Ordering::SeqCst);
            }
            Effect::Cancel { .. } => {
                self.tally.cancels.fetch_add(1, Ordering::SeqCst);
            }
            Effect::Emit(_) => {}
            _ => {}
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum Msg {
    Run(u64),
    Cancel(u64),
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
            Msg::Cancel(id) => out.push(Effect::Cancel {
                id: EffectId::new(id),
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

/// 默认关闭（①）：无 opt-in ⇒ 不登记、不声称精确、分类只能 not-tracked；
/// `Run`/`Cancel` 的转发照常（诊断不得改变被观测者）。
///
/// 修复前：`run_ids` 无条件登记、未知 Cancel 无条件计数 ⇒ 本测判红。
#[test]
fn diagnostics_are_off_by_default() {
    let (store, tally) = store();
    store.dispatch(Msg::Run(1));
    store.dispatch(Msg::Cancel(9));

    assert_eq!(
        store.cancel_unknown_id(),
        0,
        "默认关闭：没有跟踪视界 ⇒ 不得声称「精确未知」"
    );
    assert_eq!(store.run_id_tracked_count(), 0, "默认关闭不得登记 Run id");
    assert_eq!(store.run_id_tracking_capacity(), 0, "未配置容量 = 0");
    assert_eq!(store.run_id_evicted_count(), 0);
    assert_eq!(
        store.classify_run_id(1),
        RunIdClass::NotTracked,
        "默认关闭下 known 也不可判定"
    );
    assert_eq!(store.classify_run_id(9), RunIdClass::NotTracked);

    // 转发路径不依赖诊断：Run 与 Cancel 都照常到解释器。
    assert_eq!(tally.runs.load(Ordering::SeqCst), 1, "Run 必须照常转发");
    assert_eq!(
        tally.cancels.load(Ordering::SeqCst),
        1,
        "Cancel 必须照常转发"
    );
}

/// 启用后（未饱和）：三态分类与精确 miss 计数（旧 `cancel_unknown_id` 的正
/// 语义保留在 opt-in 之后）。
#[test]
fn enabled_diagnostics_classify_exactly_before_saturation() {
    let (mut store, tally) = store();
    store.enable_run_id_diagnostics(DEFAULT_RUN_ID_TRACKING_CAPACITY);
    assert_eq!(
        store.run_id_tracking_capacity(),
        DEFAULT_RUN_ID_TRACKING_CAPACITY
    );

    store.dispatch(Msg::Cancel(9));
    assert_eq!(store.cancel_unknown_id(), 1, "视界完整时的 miss 是精确未知");
    assert_eq!(store.classify_run_id(9), RunIdClass::DefinitelyUnknown);

    store.dispatch(Msg::Run(1));
    assert_eq!(store.run_id_tracked_count(), 1);
    assert_eq!(store.classify_run_id(1), RunIdClass::Known);
    assert_eq!(store.cancel_unknown_id(), 1, "已发出 id 的 Cancel 不计数");

    store.dispatch(Msg::Cancel(1));
    assert_eq!(store.cancel_unknown_id(), 1, "known 的 Cancel 不得计数");

    store.dispatch(Msg::Cancel(9));
    assert_eq!(store.cancel_unknown_id(), 2, "悬空 Cancel 累计");

    assert_eq!(tally.cancels.load(Ordering::SeqCst), 3, "Cancel 照常转发");
}

/// 饱和驱逐（②）：**被驱逐的 id 绝不判成 definitely-unknown**；驱逐后 miss
/// 只能 not-tracked，旧精确计数停止增长（显式降级，不静默改义）。
#[test]
fn saturation_never_reports_evicted_ids_as_definitely_unknown() {
    let (mut store, _tally) = store();
    store.enable_run_id_diagnostics(2);
    store.dispatch(Msg::Run(1));
    store.dispatch(Msg::Run(2));
    assert_eq!(store.run_id_tracked_count(), 2);
    assert_eq!(store.run_id_evicted_count(), 0);
    assert_eq!(store.classify_run_id(9), RunIdClass::DefinitelyUnknown);

    // 容量满 ⇒ 驱逐最旧（id 1）。
    store.dispatch(Msg::Run(3));
    assert_eq!(store.run_id_evicted_count(), 1, "驱逐必须显式计数");
    assert_eq!(store.run_id_tracked_count(), 2, "有界：跟踪数不超容量");
    assert_eq!(store.classify_run_id(3), RunIdClass::Known);
    assert_eq!(
        store.classify_run_id(1),
        RunIdClass::NotTracked,
        "被驱逐的 id 绝不判成 definitely-unknown（它可能合法存在）"
    );
    assert_eq!(
        store.classify_run_id(9),
        RunIdClass::NotTracked,
        "驱逐后任何 miss 都可能撞上被驱逐的 id ⇒ 只能 not-tracked"
    );

    // 旧精确计数在驱逐后停止增长（不得把不可判定当精确 miss 计）。
    let before = store.cancel_unknown_id();
    store.dispatch(Msg::Cancel(9));
    assert_eq!(
        store.cancel_unknown_id(),
        before,
        "驱逐后的 miss 不是精确未知 ⇒ 精确计数不得增长"
    );
}

/// 晚启用（③）：视界残缺（早先的 Run 未登记）⇒ miss 一律 not-tracked，
/// 精确计数不增长（保守方向：不把可能存在的 id 判成未知）。
#[test]
fn late_enable_leaves_horizon_incomplete() {
    let (mut store, _tally) = store();
    store.dispatch(Msg::Run(1)); // 启用前已发出的 Run：未被登记
    store.enable_run_id_diagnostics(DEFAULT_RUN_ID_TRACKING_CAPACITY);
    assert_eq!(store.run_id_tracked_count(), 0);
    assert_eq!(
        store.classify_run_id(1),
        RunIdClass::NotTracked,
        "视界残缺 ⇒ 已知的 id 也不可判定"
    );
    assert_eq!(store.classify_run_id(9), RunIdClass::NotTracked);
    assert_eq!(store.cancel_unknown_id(), 0, "视界残缺时不得声称精确");
}

// ---- 禁用路径零分配锚（本线程事件计数分配器；形态照 tests/alloc_site.rs）----

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、
// 无析构注册，在分配器内读取不会递归进分配。
thread_local! {
    static WINDOW: Cell<(bool, usize)> = const { Cell::new((false, 0)) };
}

// 窗内计数：开关与计数同住 TLS（旧版进程级 AtomicUsize + TLS 开关会被同二进制
// 其他线程重叠的窗口污染 ⇒ 偶发假红；bd udf-frame-2h7 统一整改）。
fn note_event() {
    let _ = WINDOW.try_with(|c| {
        let (open, n) = c.get();
        if open {
            c.set((true, n + 1));
        }
    });
}

// SAFETY: 纯转发 System，仅在窗内转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note_event();
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note_event();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note_event();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// op 执行期的分配事件数（瞬时生灭也计入；只计本线程窗内）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    WINDOW.with(|c| c.set((true, 0)));
    let out = op();
    black_box(&out);
    let events = WINDOW.with(|c| c.get().1);
    WINDOW.with(|c| c.set((false, 0)));
    events
}

/// 仪器自检：窗内分配必须可见——防「分配器没挂上/门没开」的假绿。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

/// 禁用路径零分配（④）：诊断默认关闭时，连续产**新 id** 的 `Run` 效果不得不
/// 分配（不被登记）；若被登记（修复前的无条件 `run_ids` 插入），新 id 必然
/// 触发哈希表增长 ⇒ 判红。零锁为结构保证：门闸（一次原子 load）先于任何状态
/// 访问，禁用路径没有可取的锁。
#[test]
fn disabled_path_registers_nothing_and_allocates_nothing() {
    let (store, _tally) = store();

    // 预热：队列/效果缓冲/账本池达稳态（首轮分配不属于被观测面）。
    for id in 0..64 {
        store.dispatch(Msg::Run(id));
    }
    assert_eq!(
        store.run_id_tracked_count(),
        0,
        "禁用路径不得登记任何 Run id（零锁零分配的行为面）"
    );

    let events = allocs(|| {
        for id in 1000..1032 {
            store.dispatch(Msg::Run(id));
        }
    });
    assert_eq!(events, 0, "禁用路径：产 Run 效果的 dispatch 必须零分配");
    assert_eq!(store.cancel_unknown_id(), 0, "禁用路径不计精确 miss");
}
