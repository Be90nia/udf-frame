//! `skeleton::lint::post_process` 稳态零分配锚（事件计数口径）。
//!
//! 口径（与 `std-ext/tests/alloc_anchors.rs` 同一测量纪律）：本文件自带**事件计数**
//! 分配器（lib 测试二进制禁挂全局分配器，集成测试自带），`alloc`/`alloc_zeroed`/
//! `realloc` 各计 1 次事件，只计**本线程窗内**（`IN_WINDOW` 门 + `EVENTS` 计数同为本
//! 线程 TLS，const 初始化免分配）——他线程的分配既不进窗、也不动本线程计数，
//! **跨线程互不污染**（门与计数同域），故无需串行闸。
//!
//! 为何事件计 0 同时满足「无事件、无 churn、无净增」：事件就是分配动作本身；无
//! 事件即无瞬时生灭（比只比 live 净额严格——瞬时生灭在净额口径下看不见）。
//!
//! 被测口径：`post_process` 稳态（表已注册、无并发注册）= 一次锁 + 一次 `Arc::clone`
//! （原子自增）+ 各阶段自身开销。注册动作**在窗外**做（建槽位/追加表项会分配，属预期）。
//! 依据：`tests/lint_alloc.rs` 断言 0 事件，example `lint_multidim` 维① 复现同一读数。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use skeleton::lint::{clear_stages, post_process, register_stage, LintError, LintStage};

/// 转发 `System` 并统计窗内分配事件的分配器。
struct EventCounting;

// 计数窗门 + 事件计数：两者都是本线程 TLS（进程全局 static 会让两个窗口重叠的测试
// 线程互相累加 = 随机假红）。const 初始化无惰性分配、无析构注册，在分配器内读取
// 不会递归进分配。
thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
    static EVENTS: Cell<usize> = const { Cell::new(0) };
}

/// 窗内分配事件 +1。
fn record_event() {
    // try_with：线程 TLS 析构期访问会 panic（分配器内 panic=abort），取不到就不计这一次。
    let _ = EVENTS.try_with(|count| count.set(count.get() + 1));
}

// SAFETY: 纯转发 System，仅在窗内转发前累加本线程事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // try_with: 线程 TLS 析构期访问会 panic（分配器内 panic=abort），取不到就当不在窗内。
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            record_event();
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            record_event();
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            record_event();
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// 本线程累计事件数。
fn current_events() -> usize {
    EVENTS.with(Cell::get)
}

/// op 执行期的分配事件数（瞬时生灭也计入；结果活到函数尾，drop 不进窗；
/// 只计本线程，其他线程的并发分配既不进窗也不进本线程计数）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    let before = current_events();
    IN_WINDOW.with(|w| w.set(true));
    let out = op();
    black_box(&out);
    IN_WINDOW.with(|w| w.set(false));
    current_events() - before
}

/// 仪器自检：窗内分配必须可见——防「分配器没挂上 / 门没开」导致的假绿
/// （本文件其余断言全是「0 事件」，仪器坏了照样绿）。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

/// 空阶段：只记一次命中（证明阶段真的跑了），且自身零分配。
///
/// `PhantomData<fn(&mut C)>` 而非 `PhantomData<C>`：函数指针形状恒 `Send + Sync`，
/// 不必给 `C` 加线程约束（契约里 `C` 不要求 `Send`）。
struct HitStage<C> {
    name: &'static str,
    hits: Arc<AtomicUsize>,
    marker: PhantomData<fn(&mut C)>,
}

impl<C> LintStage<C> for HitStage<C> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, _cfg: &mut C) -> Result<(), LintError> {
        self.hits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn hit_stage<C: 'static>(name: &'static str, hits: &Arc<AtomicUsize>) -> Arc<dyn LintStage<C>> {
    Arc::new(HitStage {
        name,
        hits: Arc::clone(hits),
        marker: PhantomData,
    })
}

const ITERATIONS: u64 = 1_000;

// ═══ 锚 1：已注册 1 阶段，稳态 post_process 零事件 ═══

struct OneStageCfg;

#[test]
fn post_process_single_stage_steady_state_zero_alloc() {
    clear_stages::<OneStageCfg>();
    let hits = Arc::new(AtomicUsize::new(0));
    register_stage::<OneStageCfg>(hit_stage("one", &hits));

    // 预热（注册表已由注册动作初始化，第一拍也可能触及哈希表内部状态）：不进窗。
    let mut cfg = OneStageCfg;
    post_process(&mut cfg).expect("阶段恒 Ok");
    hits.store(0, Ordering::Relaxed);

    let events = allocs(|| {
        for _ in 0..ITERATIONS {
            post_process(&mut cfg).expect("阶段恒 Ok");
        }
    });
    assert_eq!(events, 0, "稳态 post_process（1 阶段）必须零分配事件");
    assert_eq!(
        hits.load(Ordering::Relaxed),
        ITERATIONS as usize,
        "阶段必须每拍都跑到（防「快照为空」的假零分配）"
    );
}

// ═══ 锚 2：已注册 3 阶段，稳态 post_process 零事件 ═══

struct ThreeStageCfg;

#[test]
fn post_process_three_stages_steady_state_zero_alloc() {
    clear_stages::<ThreeStageCfg>();
    let hits = Arc::new(AtomicUsize::new(0));
    for name in ["a", "b", "c"] {
        register_stage::<ThreeStageCfg>(hit_stage(name, &hits));
    }

    let mut cfg = ThreeStageCfg;
    post_process(&mut cfg).expect("阶段恒 Ok");
    hits.store(0, Ordering::Relaxed);

    let events = allocs(|| {
        for _ in 0..ITERATIONS {
            post_process(&mut cfg).expect("阶段恒 Ok");
        }
    });
    assert_eq!(events, 0, "稳态 post_process（3 阶段）必须零分配事件");
    assert_eq!(hits.load(Ordering::Relaxed), 3 * ITERATIONS as usize);
}

// ═══ 锚 3：零注册类型（空表快路径）零事件 ═══

struct UnregisteredCfg;

#[test]
fn post_process_unregistered_type_zero_alloc() {
    clear_stages::<UnregisteredCfg>();
    let mut cfg = UnregisteredCfg;

    let events = allocs(|| {
        for _ in 0..ITERATIONS {
            post_process(&mut cfg).expect("空表恒 Ok");
        }
    });
    assert_eq!(events, 0, "空表快路径不得有分配事件");
}
