#![cfg(feature = "pool")]
//! `SizeClassPool` 组合层语义契约：向上取整选档 / 归还按档归位（不按对象当前
//! 容量反推）/ 超最大档降级直构且可观察 / 非法档表构造期 fail-fast / 按档读数。
//!
//! 分工：单档借还四路径（复用/扫片/新建/池内降级）、reset 钩子、prune/resize
//! 已在 `tests/object_pool.rs` 覆盖——本文件只测**组合层**（多档路由 + 档位
//! 归属），与单池交汇处按端到端断言不复测单池内部。

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use skeleton_infra::SizeClassPool;

// ── 固定装置：容量可读出、析构可计数的借出对象 ──

struct Buf {
    data: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Buf {
    fn capacity(&self) -> usize {
        self.data.capacity()
    }
}

impl Drop for Buf {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 构造器：`size` = 档字节（池化路）或请求字节（超最大档直构路）。
fn factory(drops: &Arc<AtomicUsize>) -> impl Fn(usize) -> Buf + Send + Sync + 'static {
    let drops = Arc::clone(drops);
    move |size| Buf {
        data: Vec::with_capacity(size),
        drops: Arc::clone(&drops),
    }
}

fn pool_with_tuning(
    sizes: impl IntoIterator<Item = usize>,
    max: usize,
    idle_cap: usize,
) -> (SizeClassPool<Buf>, Arc<AtomicUsize>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = SizeClassPool::new(sizes, max, idle_cap, factory(&drops));
    (pool, drops)
}

// ── 选档：向上取整到最小满足档 ──

#[test]
fn acquire_rounds_up_to_the_smallest_satisfying_class() {
    let (pool, _drops) = pool_with_tuning([64, 1024, 8192, 131_072], 4, 4);

    // (请求字节, 期望档位下标, 期望档字节)
    let cases = [
        (0, 0, 64),
        (1, 0, 64),
        (64, 0, 64),
        (65, 1, 1024),
        (1024, 1, 1024),
        (1025, 2, 8192),
        (5000, 2, 8192),
        (8193, 3, 131_072),
        (65_536, 3, 131_072), // 请求 64KB 拿 128KB 块：允许
        (131_072, 3, 131_072),
    ];
    for (need, index, class) in cases {
        let handle = pool.acquire(need);
        assert_eq!(handle.class_index(), Some(index), "need {need}");
        assert_eq!(handle.class_bytes(), class, "need {need}");
        assert!(!handle.is_over_max_degraded(), "need {need}");
        assert!(
            handle.capacity() >= need,
            "need {need} got capacity {}",
            handle.capacity()
        );
    }
}

#[test]
fn lent_capacity_never_falls_below_the_request() {
    let (pool, _drops) = pool_with_tuning([64, 1024, 8192, 131_072], 3, 3);
    let sizes = pool.class_bytes().to_vec();

    for need in (0..140_000).step_by(997) {
        let handle = pool.acquire(need);
        assert!(handle.capacity() >= need, "need {need} < capacity");
        if handle.is_over_max_degraded() {
            assert_eq!(handle.class_bytes(), need, "档外直构按请求字节");
            continue;
        }
        let index = handle
            .class_index()
            .expect("pooled handle carries its class");
        assert!(sizes[index] >= need);
        // 最小满足性：前一档必须不够（否则等于向下取整）
        assert!(
            index == 0 || sizes[index - 1] < need,
            "need {need} not minimal"
        );
    }
}

// ── 归还：回借出时的档，不按对象当前容量反推 ──

#[test]
fn return_goes_back_to_the_class_it_was_lent_from() {
    let (pool, _drops) = pool_with_tuning([64, 1024, 8192], 2, 2);

    let small = pool.acquire(600); // → 1024 档
    let mid = pool.acquire(5000); // → 8192 档
    let borrowed = pool.metrics();
    assert_eq!(borrowed.classes[1].pool.core.borrowed, 1);
    assert_eq!(borrowed.classes[2].pool.core.borrowed, 1);

    drop(small);
    drop(mid);

    let returned = pool.metrics();
    assert_eq!(returned.classes[1].pool.core.idle, 1, "600B 件回 1024 档");
    assert_eq!(returned.classes[2].pool.core.idle, 1, "5KB 件回 8192 档");
    assert_eq!(returned.classes[0].pool.core.idle, 0, "无错档归还");

    let again = pool.acquire(600);
    assert_eq!(again.class_bytes(), 1024);
    let reused = pool.metrics();
    assert_eq!(reused.classes[1].pool.constructed, 1, "复用不新造");
    assert_eq!(reused.classes[1].pool.reused, 1);
}

#[test]
fn grown_object_still_returns_to_its_origin_class() {
    let (pool, _drops) = pool_with_tuning([64, 1024, 8192], 2, 2);

    let mut handle = pool.acquire(600); // → 1024 档
    handle.data.reserve(8192); // 对象容量涨到 8192（下一档）以上
    assert!(handle.capacity() >= 8192);
    drop(handle);

    // 按档位归位（借出时记的档），不按涨大后的容量反推：8192 档必须仍是空的。
    let metrics = pool.metrics();
    assert_eq!(metrics.classes[1].pool.core.idle, 1, "回到借出时的 1024 档");
    assert_eq!(metrics.classes[2].pool.core.idle, 0, "未飘到 8192 档");
    assert_eq!(metrics.classes[2].pool.constructed, 0);
}

// ── 超最大档：档外降级直构，不入宇宙、归还即释放、可观察 ──

#[test]
fn request_above_the_max_class_degrades_without_touching_tiers() {
    let (pool, drops) = pool_with_tuning([64, 1024], 2, 2);

    let handle = pool.acquire(1 << 20);
    assert!(handle.is_over_max_degraded());
    assert_eq!(handle.class_index(), None);
    assert_eq!(handle.class_bytes(), 1 << 20, "按请求直构，不回退到最大档");
    assert!(handle.capacity() >= 1 << 20);

    let held = pool.metrics();
    assert_eq!(held.over_max_degraded, 1);
    assert_eq!(held.idle(), 0);
    assert_eq!(held.borrowed(), 0, "降级件不计入任何档的宇宙");
    assert_eq!(held.class_degraded(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);

    drop(handle);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "归还即释放");

    let after = pool.metrics();
    assert_eq!(after.idle(), 0, "永不进 idle");

    let second = pool.acquire(64);
    assert_eq!(second.class_bytes(), 64, "档内请求不受超档降级影响");
    drop(second);
    assert_eq!(pool.metrics().classes[0].pool.core.idle, 1);
    assert_eq!(pool.metrics().over_max_degraded, 1);
}

// ── 小档容量紧张：池内降级（与档外降级分账）可观察 ──

#[test]
fn small_class_capacity_tension_is_observable_per_class() {
    let (pool, drops) = pool_with_tuning([64, 1024], 1, 1); // 每档只容一件正常借出

    let first = pool.acquire(600);
    let second = pool.acquire(600);
    assert_eq!(first.class_index(), Some(1));
    assert_eq!(second.class_index(), Some(1));
    assert!(!first.is_over_max_degraded() && !second.is_over_max_degraded());

    let tense = pool.metrics();
    assert_eq!(tense.classes[1].pool.degraded, 1, "同档第二件走池内降级");
    assert_eq!(
        tense.classes[1].pool.core.borrowed, 2,
        "borrowed 含降级借出"
    );
    assert_eq!(tense.classes[1].pool.core.idle, 0);
    assert_eq!(tense.over_max_degraded, 0, "与档外降级分账");

    drop(first);
    drop(second);
    let relaxed = pool.metrics();
    assert_eq!(relaxed.classes[1].pool.core.idle, 1, "只有正常件被留存");
    assert_eq!(relaxed.classes[1].pool.core.borrowed, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1, "降级件归还即释放");
}

// ── 非法档表：构造期 fail-fast ──

#[test]
fn invalid_class_tables_fail_fast_at_construction() {
    let bad: [Vec<usize>; 4] = [
        vec![],          // 空表
        vec![64, 0],     // 含 0
        vec![1024, 64],  // 递减
        vec![64, 64, 1], // 非严格递增（重复档）
    ];
    for sizes in bad {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            let _ = SizeClassPool::new(sizes.clone(), 2, 2, Vec::<u8>::with_capacity);
        }));
        assert!(outcome.is_err(), "{sizes:?} 应被拒绝");
    }

    let ok = catch_unwind(AssertUnwindSafe(|| {
        let _ = SizeClassPool::new([64, 128], 2, 2, Vec::<u8>::with_capacity);
    }));
    assert!(ok.is_ok(), "严格递增表应被接受");
}

// ── 消费方场景端到端：混合尺寸 + 小档紧张 + 超档降级 ──

const DEMO_SIZES: [usize; 4] = [64, 1024, 8192, 131_072];

fn dump(label: &str, pool: &SizeClassPool<Buf>) {
    let metrics = pool.metrics();
    println!(
        "[{label}] aggregate idle={} borrowed={} class_degraded={} over_max_degraded={}",
        metrics.idle(),
        metrics.borrowed(),
        metrics.class_degraded(),
        metrics.over_max_degraded
    );
    for (index, class) in metrics.classes.iter().enumerate() {
        println!(
            "  class#{index} {:>7}B idle={} borrowed={} degraded={} constructed={} reused={}",
            class.class_bytes,
            class.pool.core.idle,
            class.pool.core.borrowed,
            class.pool.degraded,
            class.pool.constructed,
            class.pool.reused
        );
    }
}

#[test]
fn consumer_mixed_size_scenario() {
    // 消费方形态：600B / 5KB / 70KB 混合请求，小档池深 2 制造容量紧张，
    // 另加 2MB 超最大档请求。
    let (pool, drops) = pool_with_tuning(DEMO_SIZES, 2, 2);
    dump("fresh", &pool);

    let small_a = pool.acquire(600);
    let small_b = pool.acquire(600);
    let small_c = pool.acquire(600); // 1024 档第 3 件：池内降级
    let mid = pool.acquire(5 * 1024);
    let big = pool.acquire(70 * 1024);
    assert_eq!(
        [
            small_a.class_bytes(),
            small_b.class_bytes(),
            small_c.class_bytes(),
            mid.class_bytes(),
            big.class_bytes()
        ],
        [1024, 1024, 1024, 8192, 131_072]
    );
    dump("holding 600B x3 + 5KB + 70KB", &pool);

    let huge = pool.acquire(2 << 20);
    assert!(huge.is_over_max_degraded());
    assert!(huge.capacity() >= (2 << 20));
    dump("plus 2MB (over max class)", &pool);

    // 对照验证：「请求 64KB 拿 128KB」允许；反向（128KB 请求拿 64KB）不可能。
    let sixty_four_k = pool.acquire(65_536);
    assert_eq!(sixty_four_k.class_bytes(), 131_072);
    assert!(sixty_four_k.capacity() >= 65_536);
    let round_trip = pool.acquire(131_072);
    assert!(
        round_trip.capacity() >= 131_072,
        "请求 128KB 不可能拿 64KB 块"
    );

    drop((small_a, small_b, small_c, mid, big));
    let returned = pool.metrics();
    assert_eq!(
        returned.classes[1].pool.core.idle, 2,
        "1024 档留两件正常件（池深 2；第 3 件是池内降级，归还即释放）"
    );
    assert_eq!(returned.classes[2].pool.core.idle, 1);
    assert_eq!(returned.classes[3].pool.core.idle, 1);
    dump("after dropping pooled handles (2MB still held)", &pool);

    let drops_before = drops.load(Ordering::SeqCst);
    drop((huge, sixty_four_k, round_trip));
    let settled = pool.metrics();
    assert_eq!(settled.borrowed(), 0);
    assert_eq!(settled.over_max_degraded, 1);
    assert_eq!(
        drops.load(Ordering::SeqCst) - drops_before,
        2,
        "2MB 档外直构件 + 池内降级件归还即释放；正常件留池"
    );
    dump("settled", &pool);

    // 复用回流：再借 600B 命中 1024 档 idle 件（不新造）。
    let reuse = pool.acquire(600);
    assert_eq!(pool.metrics().classes[1].pool.reused, 1);
    assert_eq!(
        pool.metrics().classes[1].pool.constructed,
        3,
        "两件正常 + 一件池内降级，复用不再新造"
    );
    drop(reuse);
}
