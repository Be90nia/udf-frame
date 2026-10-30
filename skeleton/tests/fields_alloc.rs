//! `skeleton::fields` 零分配锚（事件计数口径）。
//!
//! 口径（与 `tests/lint_alloc.rs` 同一测量纪律）：本文件自带**事件计数**分配器
//! （lib 测试二进制禁挂全局分配器，集成测试自带），`alloc`/`alloc_zeroed`/
//! `realloc` 各计 1 次事件，只计**本线程窗内**（`IN_WINDOW` 门 + `EVENTS` 计数
//! 同为本线程 TLS，const 初始化免分配）——他线程的分配既不进窗、也不动本线程
//! 计数，跨线程互不污染，无需串行闸。
//!
//! 被测口径：`new`/`Default` 空 bag 零分配（EMPTY 静态懒建的那一次在预热拍）；
//! `clone` = 一次原子自增零事件（与字段数无关）；`iter` 借用遍历零事件；`insert`
//! 首字段落表必须发生分配（≥1 事件，如实断言不硬编码——EMPTY 共享 `Arc` 逃逸 +
//! `Vec` 首推）；同名覆盖稳态（独占 bag 原地改）零事件。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use skeleton::fields::{FieldBag, FieldValue};

/// 转发 `System` 并统计窗内分配事件的分配器。
struct EventCounting;

// 计数窗门 + 事件计数：两者都是本线程 TLS（进程全局 static 会让两个窗口重叠的
// 测试线程互相累加 = 随机假红）。const 初始化无惰性分配、无析构注册，在分配器
// 内读取不会递归进分配。
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
/// 只计本线程）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    let before = current_events();
    IN_WINDOW.with(|w| w.set(true));
    let out = op();
    black_box(&out);
    IN_WINDOW.with(|w| w.set(false));
    current_events() - before
}

/// 仪器自检：窗内分配必须可见——防「分配器没挂上 / 门没开」导致的假绿
/// （本文件其余断言含多个「0 事件」，仪器坏了照样绿）。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

/// 预建 n 字段 bag（窗外构造；名字取自常量表，`&'static str` 零分配纪律）。
const NAMES: [&str; 8] = ["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"];

fn filled(n: usize) -> FieldBag {
    let mut bag = FieldBag::new();
    for (i, name) in NAMES.iter().take(n).enumerate() {
        bag.insert(name, FieldValue::Count(i as u64));
    }
    bag
}

// ═══ 锚 1：new / Default 空 bag 零事件 ═══

#[test]
fn new_and_default_are_zero_alloc() {
    black_box(FieldBag::new()); // 预热：EMPTY 静态懒建的一次分配不进计数窗
    assert_eq!(allocs(FieldBag::new), 0);
    assert_eq!(allocs(FieldBag::default), 0);
}

// ═══ 锚 2：clone 零事件（与字段数无关，O(1) 原子自增） ═══

#[test]
fn clone_is_zero_alloc_regardless_of_size() {
    for n in [0usize, 1, 8] {
        let bag = filled(n);
        assert_eq!(allocs(|| bag.clone()), 0, "{n} 字段 bag 的克隆必须零事件");
        black_box(bag);
    }
}

// ═══ 锚 3：iter 借用遍历零事件 ═══

#[test]
fn iter_is_zero_alloc() {
    let bag = filled(8);
    let events = allocs(|| {
        for (name, value) in bag.iter() {
            black_box((name, value));
        }
    });
    assert_eq!(events, 0);
}

// ═══ 锚 4：insert 首字段 ≥1 事件（如实断言，不硬编码精确值） ═══

#[test]
fn first_insert_allocates_at_least_one_event() {
    let events = allocs(|| {
        let mut bag = FieldBag::new();
        bag.insert("k", FieldValue::Count(1));
        bag
    });
    assert!(
        events >= 1,
        "首字段落表必须发生分配（EMPTY 共享 Arc 逃逸 + Vec 首推），实测 {events} 事件"
    );
}

// ═══ 锚 5：同名覆盖稳态（独占 bag 原地改）零事件 ═══

#[test]
fn overwrite_existing_key_is_zero_alloc() {
    let mut bag = filled(4);
    let events = allocs(|| bag.insert("f0", FieldValue::Count(99)));
    assert_eq!(events, 0, "独占 bag 同名覆盖走 make_mut 原地改，必须零事件");
}
