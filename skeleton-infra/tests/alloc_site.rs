//! alloc_site 语义测试 + 事件计数零分配锚。
//!
//! 口径：本文件自带**事件计数**分配器（形态照 `std-ext/tests/alloc_anchors.rs`
//! ——lib 测试二进制禁挂全局分配器，集成测试自带；alloc/realloc/alloc_zeroed
//! 各计 1 次事件，只计**窗内本线程**，libtest 并发线程的分配不进窗）。语义
//! 测试共享进程级全局闸（`GLOBAL_ENABLED`），以文件内串行闸互斥（SERIAL，
//! 形态同 alloc_anchors）。
//!
//! armor↔测试对表（模块 doc 八条）：1 原子序=doc 声明；2 开关检查序=doc +
//! multidim dim2/dim5；3 禁用零写入=global_off/site_off 两测 + dim5；4 枚举
//! 序=table_order；5 重名=find_returns_first_duplicate；6 伪共享=doc 声明
//! （升级路径登记）；7 零分配=hot_paths_zero_alloc 锚；8 分工=doc 声明。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use skeleton_infra::alloc_site::{find, global_enabled, set_global_enabled, AllocSite};
use skeleton_infra::alloc_sites;

// 两张独立宏表（site 标识符跨表不得重名——每次宏调用生成独立 static 集）。
alloc_sites! {
    pub static MAIN_SITES = {
        MAIN_APPEND,
        MAIN_SPAWN,
    };
}

alloc_sites! {
    pub static SIDE_SITES = {
        SIDE_APPEND,
        SIDE_FLUSH,
    };
}

// armor 5 夹具：手工重名表（find 返回第一个）。
static DUP_FIRST: AllocSite = AllocSite::new("dup");
static DUP_SECOND: AllocSite = AllocSite::new("dup");
static DUP_TABLE: &[&AllocSite] = &[&DUP_FIRST, &DUP_SECOND];

// ---- 文件内串行闸（语义测试拨全局闸，必须互斥）----

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// 测试前复位：两级开关全开 + 目标站点计数/开关归位（串行闸内调用）。
fn fresh(site: &AllocSite) {
    set_global_enabled(true);
    site.reset();
    site.set_enabled(true);
}

// ---- 语义测试 ----

/// bump 递增；宏生成站点名 = stringify! 标识符。
#[test]
fn bump_increments_count() {
    let _serial = serial();
    fresh(&MAIN_APPEND);

    MAIN_APPEND.bump();
    MAIN_APPEND.bump();
    MAIN_APPEND.bump();
    assert_eq!(MAIN_APPEND.count(), 3);
    assert_eq!(MAIN_APPEND.name, "MAIN_APPEND");
}

/// 全局关 ⇒ 不计数；重开 ⇒ 恢复（armor 3「开关 ≠ 清零」：历史保留）。
#[test]
fn global_off_stops_counting_and_reenable_recovers() {
    let _serial = serial();
    fresh(&MAIN_APPEND);

    MAIN_APPEND.bump();
    assert_eq!(MAIN_APPEND.count(), 1);

    set_global_enabled(false);
    MAIN_APPEND.bump();
    MAIN_APPEND.bump();
    assert_eq!(MAIN_APPEND.count(), 1, "全局关 ⇒ 零写入");

    set_global_enabled(true);
    MAIN_APPEND.bump();
    assert_eq!(MAIN_APPEND.count(), 2, "重开恢复，关闸期历史计数保留");
}

/// 站点关（全局开）⇒ 不计数；重开恢复。
#[test]
fn site_off_stops_counting_while_global_on() {
    let _serial = serial();
    fresh(&MAIN_SPAWN);

    MAIN_SPAWN.bump();
    assert_eq!(MAIN_SPAWN.count(), 1);

    MAIN_SPAWN.set_enabled(false);
    assert!(global_enabled(), "前置：全局闸保持开");
    MAIN_SPAWN.bump();
    assert_eq!(MAIN_SPAWN.count(), 1, "站点关 ⇒ 零写入");

    MAIN_SPAWN.set_enabled(true);
    MAIN_SPAWN.bump();
    assert_eq!(MAIN_SPAWN.count(), 2, "重开恢复");
}

/// 枚举序 = 宏声明序，且表内即站点本身（armor 4）。
#[test]
fn table_order_and_names_match_declaration() {
    let names: Vec<_> = MAIN_SITES.iter().map(|site| site.name).collect();
    assert_eq!(names, ["MAIN_APPEND", "MAIN_SPAWN"]);
    assert!(std::ptr::eq(MAIN_SITES[0], &MAIN_APPEND));
    assert!(std::ptr::eq(MAIN_SITES[1], &MAIN_SPAWN));
}

/// find 命中与未命中；跨表名字空间不串。
#[test]
fn find_hit_and_miss() {
    assert!(std::ptr::eq(
        find(MAIN_SITES, "MAIN_SPAWN").expect("命中"),
        &MAIN_SPAWN
    ));
    assert!(find(MAIN_SITES, "NO_SUCH_SITE").is_none());
    assert!(
        find(MAIN_SITES, "SIDE_APPEND").is_none(),
        "另一张宏表的站点不进本表"
    );
}

/// armor 5：重名返回第一个（先声明者胜）。
#[test]
fn find_returns_first_duplicate() {
    let _serial = serial();
    fresh(&DUP_SECOND);

    DUP_SECOND.bump();
    let found = find(DUP_TABLE, "dup").expect("重名表命中");
    assert!(std::ptr::eq(found, &DUP_FIRST), "先声明者胜");
    assert_eq!(found.count(), 0, "第二个同名站点的计数不串入");
}

/// 独立两张宏表互不干扰（计数与开关都独立）。
#[test]
fn two_macro_tables_are_independent() {
    let _serial = serial();
    fresh(&MAIN_APPEND);
    fresh(&SIDE_APPEND);

    MAIN_APPEND.bump();
    MAIN_APPEND.bump();
    SIDE_APPEND.bump();
    assert_eq!(MAIN_APPEND.count(), 2);
    assert_eq!(SIDE_APPEND.count(), 1, "同角色站点随表独立");

    MAIN_APPEND.set_enabled(false);
    SIDE_APPEND.bump();
    assert_eq!(SIDE_APPEND.count(), 2, "A 表站点关不拦 B 表计数");
    assert_eq!(MAIN_APPEND.count(), 2, "A 表关闸期零写入");
    MAIN_APPEND.set_enabled(true);
}

/// reset 只清计数，不动开关。
#[test]
fn reset_clears_count_only() {
    let _serial = serial();
    fresh(&MAIN_APPEND);

    MAIN_APPEND.bump();
    MAIN_APPEND.reset();
    assert_eq!(MAIN_APPEND.count(), 0);
    assert!(MAIN_APPEND.is_enabled(), "reset 不动站点开关");
    assert!(global_enabled(), "reset 不动全局闸");
}

// ---- 事件计数分配器 + 零分配锚（armor 7；形态照 std-ext/tests/alloc_anchors.rs）----

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
        // try_with：线程 TLS 析构期访问会 panic（分配器内 panic=abort），取不到当不在窗内。
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

/// 仪器自检：窗内分配必须可见——防「分配器没挂上/门没开」的假绿
/// （其余锚全是「0 事件」，仪器坏了照样绿）。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let _serial = serial();
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

/// armor 7：find 命中/未命中、bump 三态（开/全局关/站点关）、count、表枚举
/// 全路径零分配；伴随计数断言防「假零」（路径必须真跑到）。
#[test]
fn hot_paths_zero_alloc() {
    let _serial = serial();
    fresh(&MAIN_APPEND);
    fresh(&MAIN_SPAWN); // 表内另一站点同步复位（静态计数跨测试残留）

    // find 命中/未命中（纯查询，结果须 black_box 防消除）
    let hit = allocs(|| black_box(find(MAIN_SITES, "MAIN_APPEND")));
    assert_eq!(hit, 0, "find 命中零分配");
    assert!(find(MAIN_SITES, "MAIN_APPEND").is_some(), "路径真跑到");
    let miss = allocs(|| black_box(find(MAIN_SITES, "NO_SUCH_SITE")));
    assert_eq!(miss, 0, "find 未命中零分配");

    // bump 开：真计数
    let on = allocs(|| MAIN_APPEND.bump());
    assert_eq!(on, 0, "bump（开）零分配");
    assert_eq!(MAIN_APPEND.count(), 1, "开路径真计数（防假零）");

    // bump 全局关：零写入 ⇒ 计数不动
    set_global_enabled(false);
    let off_global = allocs(|| MAIN_APPEND.bump());
    assert_eq!(off_global, 0, "bump（全局关）零分配");
    assert_eq!(MAIN_APPEND.count(), 1, "关闸零写入");
    set_global_enabled(true);

    // bump 站点关：同上
    MAIN_APPEND.set_enabled(false);
    let off_site = allocs(|| MAIN_APPEND.bump());
    assert_eq!(off_site, 0, "bump（站点关）零分配");
    assert_eq!(MAIN_APPEND.count(), 1, "关闸零写入");
    MAIN_APPEND.set_enabled(true);

    // count 读
    let read = allocs(|| black_box(MAIN_APPEND.count()));
    assert_eq!(read, 0, "count 零分配");

    // 表枚举（遍历 + 逐站点读；值断言先于窗，窗内只证零事件）
    let sum: u64 = MAIN_SITES.iter().map(|site| site.count()).sum();
    assert_eq!(sum, 1, "枚举路径真跑到（MAIN_APPEND 计 1）");
    let walk = allocs(|| {
        let mut total = 0u64;
        for site in MAIN_SITES {
            total = total.wrapping_add(site.count());
            black_box(site.name);
        }
        total
    });
    assert_eq!(walk, 0, "表枚举零分配");
}
