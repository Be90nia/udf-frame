//! iter 域七维性能矩阵采集器（收窄声明：①适配器零分配面+collect 一次分配
//! 兑现 ②ns 锚（interleave/zip_longest/minmax/dedup 四件 vs 手写等价循环）③
//! 长度梯度 100/1k/10k；④⑤⑥⑦ 收窄——纯惰性组合子：无运行时状态故无调参
//! 面（④），无锁无线程故无自旋（⑥）与共享热词（⑦），延迟分布（⑤）对代表
//! 件 interleave 采样 p50/p99 承载）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-std-ext --example iter_multidim
//! ```
//!
//! 口径注（沿 text_multidim 惯例）：
//! ① **事件计数**（只增不减）——本地 EventCounting 采集器自持，零 feature 门；
//! ② 锚随 src/iter.rs doc 声明（iter 域性能锚（本地留存，未入库））；粗测
//!     （std Instant）非 criterion——数字供判级，不供微优化基线；
//! ③ black_box 防 LLVM 消除；每个锚三连跑取中位。
//!
//! 报告：iter 域性能锚（本地留存，未入库）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::iter::zip;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use std_ext::iter::{
    dedup, interleave, minmax, process_results, zip_longest, EitherOrBoth, MinMaxResult,
};

/// 分配事件计数器（text_multidim 同款：alloc/alloc_zeroed/realloc 各计一次
/// 事件；dealloc 不计——事件面只增不减，瞬时生灭全可见）。
struct EventCounting;

static EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

// ---- 测量助手（text_multidim 同款）----

/// op 跑 n 次的分配事件数/次（事件只增不减，无净额相抵问题）。
fn events_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op()); // 预热：懒初始化不进计数
    let before = EVENTS.load(Ordering::Relaxed);
    for _ in 0..n {
        black_box(op());
    }
    (EVENTS.load(Ordering::Relaxed) - before) as f64 / n as f64
}

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3(a: f64, b: f64, c: f64) -> f64 {
    let mut v = [a, b, c];
    v.sort_by(|x, y| x.partial_cmp(y).unwrap());
    v[1]
}

/// 输入向量：n 个 i32，值域 n/4（保证 dedup 有真实重复面、minmax 无序）。
fn input(n: usize) -> Vec<i64> {
    (0..n)
        .map(|i| ((i as u64).wrapping_mul(2654435761) % ((n / 4).max(1) as u64)) as i64)
        .collect()
}

fn main() {
    // ⓪ 计数器自检（采集器可信面，fail-visible）：String 堆分配必须计 1 事件。
    let probe = events_per(|| String::with_capacity(64).len(), 1_000);
    println!("⓪ 计数器自检：String::with_capacity(64) = {probe:.2} events/call（应 =1）");
    assert!(
        (probe - 1.0).abs() < 1e-9,
        "EventCounting 未计到堆分配事件，本次采集读数不可信"
    );

    perf_alloc_face();
    perf_ns_anchors();
    perf_gradient();
    perf_latency_dist();
}

// ---- ① 分配事件面（events/call：惰性组合子构型零分配，collect 时一次分配如实）----

fn perf_alloc_face() {
    let a: Vec<i64> = input(1_000);
    let b: Vec<i64> = input(1_000);

    // 构型（不消费）必须零事件：惰性纪律的可观测兑现
    let zip_cfg = events_per(|| zip_longest(a.iter(), b.iter()), 10_000);
    let il_cfg = events_per(|| interleave(a.iter(), b.iter()), 10_000);
    let dd_cfg = events_per(|| dedup(a.iter()), 10_000);

    // 消费进 Vec：双 ExactSize 底源 → 预分配 max(a,b) 一次到位（坑面 #4 兑现）
    let zip_collect = events_per(
        || zip_longest(0..1_000i64, 0..1_000i64).collect::<Vec<_>>(),
        1_000,
    );
    // 非 ExactSize 面（上界未知）：Vec 倍增策略 ~log2(n) 次，std 同款非本域税
    let il_collect = events_per(|| interleave(a.iter(), b.iter()).collect::<Vec<_>>(), 1_000);

    // 全件适配器链消费零新增分配（collect 已计入目标 Vec）
    let chain = events_per(
        || a.iter().copied().map(|x| x * 2).collect::<Vec<_>>().len(),
        1_000,
    );

    println!(
        "① 分配事件面：构型 zip_longest {zip_cfg:.2} / interleave {il_cfg:.2} / dedup {dd_cfg:.2}（应全=0）；collect：zip_longest 双 Range {zip_collect:.2}（应=1，ExactSize 预分配兑现）/ interleave Vec 底源 {il_collect:.2}（倍增策略如实）；纯 map 链 {chain:.2}"
    );
}

// ---- ② ns 锚（median3；vs 手写等价循环对照，随 src/iter.rs doc 声明判级）----

/// 手写 interleave 等价循环（轮转 push，长者尾部追加）。
fn hand_interleave(a: &[i64], b: &[i64]) -> Vec<i64> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let mut ai = a.iter().copied();
    let mut bi = b.iter().copied();
    loop {
        match ai.next() {
            Some(x) => out.push(x),
            None => {
                out.extend(bi);
                return out;
            }
        }
        match bi.next() {
            Some(y) => out.push(y),
            None => {
                out.extend(ai);
                return out;
            }
        }
    }
}

/// 手写 zip_longest 等价循环（产出同型 EitherOrBoth）。
fn hand_zip_longest(a: &[i64], b: &[i64]) -> Vec<EitherOrBoth<i64, i64>> {
    let mut out = Vec::with_capacity(a.len().max(b.len()));
    let mut ai = a.iter().copied();
    let mut bi = b.iter().copied();
    loop {
        match (ai.next(), bi.next()) {
            (None, None) => return out,
            (Some(x), None) => {
                out.push(EitherOrBoth::Left(x));
                out.extend(ai.map(EitherOrBoth::Left));
                return out;
            }
            (None, Some(y)) => {
                out.push(EitherOrBoth::Right(y));
                out.extend(bi.map(EitherOrBoth::Right));
                return out;
            }
            (Some(x), Some(y)) => out.push(EitherOrBoth::Both(x, y)),
        }
    }
}

/// std 两遍 min/max（通用采用方现实基线）。
fn hand_minmax_two_pass(v: &[i64]) -> (i64, i64) {
    (*v.iter().min().unwrap(), *v.iter().max().unwrap())
}

/// 手写 dedup 等价循环（连续去重保首现）。
fn hand_dedup(v: &[i64]) -> Vec<i64> {
    let mut out: Vec<i64> = Vec::with_capacity(v.len());
    for x in v.iter().copied() {
        if out.last() != Some(&x) {
            out.push(x);
        }
    }
    out
}

fn perf_ns_anchors() {
    let v: Vec<i64> = input(10_000);
    let w: Vec<i64> = input(10_000);

    let il_ours = median3(
        batch_mean(|| interleave(v.iter(), w.iter()).sum::<i64>(), 200),
        batch_mean(|| interleave(v.iter(), w.iter()).sum::<i64>(), 200),
        batch_mean(|| interleave(v.iter(), w.iter()).sum::<i64>(), 200),
    );
    let il_hand = median3(
        batch_mean(|| hand_interleave(&v, &w).len(), 200),
        batch_mean(|| hand_interleave(&v, &w).len(), 200),
        batch_mean(|| hand_interleave(&v, &w).len(), 200),
    );

    let zl_ours = median3(
        batch_mean(
            || zip_longest(v.iter(), w.iter()).fold(0usize, |s, _| s + 1),
            200,
        ),
        batch_mean(
            || zip_longest(v.iter(), w.iter()).fold(0usize, |s, _| s + 1),
            200,
        ),
        batch_mean(
            || zip_longest(v.iter(), w.iter()).fold(0usize, |s, _| s + 1),
            200,
        ),
    );
    let zl_hand = median3(
        batch_mean(|| hand_zip_longest(&v, &w).len(), 200),
        batch_mean(|| hand_zip_longest(&v, &w).len(), 200),
        batch_mean(|| hand_zip_longest(&v, &w).len(), 200),
    );

    let mm_ours = median3(
        batch_mean(|| minmax(v.iter().copied()), 200),
        batch_mean(|| minmax(v.iter().copied()), 200),
        batch_mean(|| minmax(v.iter().copied()), 200),
    );
    let mm_hand = median3(
        batch_mean(|| hand_minmax_two_pass(&v), 200),
        batch_mean(|| hand_minmax_two_pass(&v), 200),
        batch_mean(|| hand_minmax_two_pass(&v), 200),
    );

    let dd_ours = median3(
        batch_mean(|| dedup(v.iter()).sum::<i64>(), 200),
        batch_mean(|| dedup(v.iter()).sum::<i64>(), 200),
        batch_mean(|| dedup(v.iter()).sum::<i64>(), 200),
    );
    let dd_hand = median3(
        batch_mean(|| hand_dedup(&v).len(), 200),
        batch_mean(|| hand_dedup(&v).len(), 200),
        batch_mean(|| hand_dedup(&v).len(), 200),
    );

    // process_results：短路提升税（全 Ok 流 collect 对照裸 Vec 收集）
    let oks: Vec<Result<i64, ()>> = v.iter().map(|x| Ok(*x)).collect();
    let pr = median3(
        batch_mean(
            || process_results(oks.iter().copied(), |it| it.sum::<i64>()),
            200,
        ),
        batch_mean(
            || process_results(oks.iter().copied(), |it| it.sum::<i64>()),
            200,
        ),
        batch_mean(
            || process_results(oks.iter().copied(), |it| it.sum::<i64>()),
            200,
        ),
    );
    let bare = median3(
        batch_mean(|| oks.iter().map(|r| r.unwrap()).sum::<i64>(), 200),
        batch_mean(|| oks.iter().map(|r| r.unwrap()).sum::<i64>(), 200),
        batch_mean(|| oks.iter().map(|r| r.unwrap()).sum::<i64>(), 200),
    );

    // 黑箱消费自检：语义正确性钉住（防锚面跑在错误输出上）
    assert_eq!(
        interleave(&[1i64, 2, 3][..], &[10, 20][..]).sum::<i64>(),
        36
    );
    assert_eq!(minmax([3i64, 1, 2]), MinMaxResult::MinMax(1, 3));
    assert_eq!(hand_zip_longest(&[1], &[2, 3]).len(), 2);
    assert_eq!(hand_dedup(&[1, 1, 2]), vec![1, 2]);

    println!(
        "② ns 锚 @10k（域版 vs 手写，median3）：interleave {il_ours:.0} vs {il_hand:.0}（{:.2}x）/ zip_longest {zl_ours:.0} vs {zl_hand:.0}（{:.2}x）/ minmax {mm_ours:.0} vs 两遍 {mm_hand:.0}（{:.2}x，单遍 3n/2 比较 vs 2n 遍历）/ dedup {dd_ours:.0} vs {dd_hand:.0}（{:.2}x）/ process_results {pr:.0} vs 裸 unwrap 流 {bare:.0}（{:.2}x）",
        il_ours / il_hand,
        zl_ours / zl_hand,
        mm_ours / mm_hand,
        dd_ours / dd_hand,
        pr / bare,
    );
}

// ---- ③ 长度梯度（代表件 interleave + minmax：100/1k/10k）----

fn perf_gradient() {
    for n in [100usize, 1_000, 10_000] {
        let v = input(n);
        let w = input(n);
        let il = batch_mean(|| interleave(v.iter(), w.iter()).sum::<i64>(), 500);
        let mm = batch_mean(|| minmax(v.iter().copied()), 500);
        println!("③ 梯度 n={n}：interleave {il:.0} ns（2n 元素）/ minmax {mm:.0} ns");
    }
}

// ---- ⑤ 延迟分布（代表件 interleave@1k 单发采样 p50/p99）----

fn perf_latency_dist() {
    let v = input(1_000);
    let w = input(1_000);
    let mut samples: Vec<u64> = Vec::with_capacity(1_000);
    for _ in 0..1_000 {
        let t = Instant::now();
        black_box(interleave(v.iter(), w.iter()).sum::<i64>());
        samples.push(t.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    let p50 = samples[samples.len() / 2];
    let p99 = samples[samples.len() * 99 / 100];
    println!("⑤ interleave@1k 单发采样（n=1000）：p50 {p50} ns / p99 {p99} ns");

    // 坑面回归面烟测（Fail-visible：短路语义真值锚）
    let mixed = [Ok::<i64, &str>(1), Err("boom"), Ok(3)];
    assert!(process_results(mixed, |it| it.sum::<i64>()).is_err());
    assert_eq!(zip(1..3, 10..20).count(), 2); // std zip 截断语义对照（本域反面教材）
}
