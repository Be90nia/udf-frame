//! text 域七维性能矩阵采集器（收窄声明：①分配事件面 ②ns 锚（五向转换 /
//! snake-pascal @64B / truncate / decode Strict）③距离 O(min) 行宽结构证据；
//! ④⑤⑥⑦ n/a——纯函数无状态：无运行时调参面、无阻塞无自旋、无共享可变
//! 状态故无伪共享面，延迟面由②中位锚承载）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-std-ext --example text_multidim
//! ```
//!
//! 口径注（沿 cache_multidim 惯例）：
//! ① **事件计数**（只增不减）——本地 EventCounting 采集器自持，零 feature 门；
//! ② 锚随 src/text.rs doc 声明（text 域性能锚（本地留存，未入库））；粗测
//!     （std Instant）非 criterion——数字供判级，不供微优化基线；
//! ③ black_box 防 LLVM 消除；每个锚三连跑取中位（扁平三次调用，无嵌套闭包）。
//!
//! 报告：text 域性能锚（本地留存，未入库）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use std_ext::text::{
    decode_bytes, edit_distance, to_camel_case, to_kebab_case, to_pascal_case,
    to_screaming_snake_case, to_snake_case, truncate_chars, DecodeStrategy,
};

/// 分配事件计数器（cache_multidim 同款：alloc/alloc_zeroed/realloc 各计一次
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

// ---- 测量助手（cache_multidim 同款）----

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

/// 14B 缩略词（doc 五向锚输入；XMLHttpRequest 实为 14 字节）。
const S13: &str = "XMLHttpRequest";

/// 30B ASCII（doc truncate 30字符截16 锚输入）。
const S30: &str = "XMLHttpRequest a1B2 v2Long xyz";

/// 64B 混合形态（doc snake/pascal 锚输入；长度改动须同步锚口径）。
const S64: &str = "XMLHttpRequest fooBARbaz get2JSONValues HTTP2Server v2Long fGeHi";

/// ~50B 合法 UTF-8（doc decode 锚输入）。
const S50: &str = "XMLHttpRequest fooBARbaz HTTP2Server v2Long a1B2c";

fn main() {
    // ⓪ 计数器自检（采集器可信面，fail-visible）：String 堆分配必须计 1 事件。
    let probe = events_per(|| String::with_capacity(64).len(), 1_000);
    println!("⓪ 计数器自检：String::with_capacity(64) = {probe:.2} events/call（应 =1）");
    assert!(
        (probe - 1.0).abs() < 1e-9,
        "EventCounting 未计到堆分配事件，本次采集读数不可信"
    );
    assert_eq!(S13.len(), 14);
    assert_eq!(S30.len(), 30);
    assert_eq!(S64.len(), 64);
    assert_eq!(S50.len(), 49);

    perf_alloc_face();
    perf_ns_anchors();
    perf_distance();
}

// ---- ① 分配事件面（events/call：有效路径分配数随 doc 声明）----

fn perf_alloc_face() {
    let five = events_per(|| to_snake_case(S13), 10_000);
    let trunc = events_per(|| truncate_chars(S64, 64), 10_000);
    let dec = events_per(
        || decode_bytes(S50.as_bytes(), DecodeStrategy::Strict).unwrap(),
        10_000,
    );
    let (a, b) = dist_inputs_100();
    let dist = events_per(|| edit_distance(&a, &b), 1_000);
    println!("① 分配事件面（events/call）：五向转换 {five:.2}（输出 String 一次精确容量）/ truncate {trunc:.2}（零分配字符前缀扫描）/ decode Strict 有效 {dec:.2} / 距离 100×100 {dist:.2}（单行 DP）");
}

// ---- ② ns 锚（median3；随 src/text.rs doc 声明判级）----

fn perf_ns_anchors() {
    let five_way = |op: &dyn Fn(&str) -> String| -> f64 {
        median3(
            batch_mean(|| black_box(op(S13)), 100_000),
            batch_mean(|| black_box(op(S13)), 100_000),
            batch_mean(|| black_box(op(S13)), 100_000),
        )
    };
    let s = five_way(&to_snake_case);
    let c = five_way(&to_camel_case);
    let p = five_way(&to_pascal_case);
    let k = five_way(&to_kebab_case);
    let sc = five_way(&to_screaming_snake_case);
    println!("②′ 五向 @XMLHttpRequest(14B) ns（doc ≈174-178）：snake {s:.0} / camel {c:.0} / pascal {p:.0} / kebab {k:.0} / screaming {sc:.0}");

    let sn64 = median3(
        batch_mean(|| to_snake_case(S64), 20_000),
        batch_mean(|| to_snake_case(S64), 20_000),
        batch_mean(|| to_snake_case(S64), 20_000),
    );
    let pa64 = median3(
        batch_mean(|| to_pascal_case(S64), 20_000),
        batch_mean(|| to_pascal_case(S64), 20_000),
        batch_mean(|| to_pascal_case(S64), 20_000),
    );
    println!("②″ snake/pascal @64B ns（doc ≈679-691）：snake {sn64:.0} / pascal {pa64:.0}");

    // truncate 扫描线性面：30 字符截 16 与 130 字符截 64（doc 锚两档）。
    let s130 = "abcdefghij".repeat(13);
    assert_eq!(s130.len(), 130);
    assert_eq!(S30.len(), 30);
    let t30 = median3(
        batch_mean(|| truncate_chars(S30, 16), 500_000),
        batch_mean(|| truncate_chars(S30, 16), 500_000),
        batch_mean(|| truncate_chars(S30, 16), 500_000),
    );
    let t130 = median3(
        batch_mean(|| truncate_chars(&s130, 64), 500_000),
        batch_mean(|| truncate_chars(&s130, 64), 500_000),
        batch_mean(|| truncate_chars(&s130, 64), 500_000),
    );
    let t_all = median3(
        batch_mean(|| truncate_chars(S64, 10_000), 500_000),
        batch_mean(|| truncate_chars(S64, 10_000), 500_000),
        batch_mean(|| truncate_chars(S64, 10_000), 500_000),
    );
    println!("②‴ truncate ns（30字符截16 / 130字符截64 / 越界全扫 64B）：{t30:.0} / {t130:.0} / {t_all:.0}");

    let dec = median3(
        batch_mean(
            || decode_bytes(S50.as_bytes(), DecodeStrategy::Strict).unwrap(),
            200_000,
        ),
        batch_mean(
            || decode_bytes(S50.as_bytes(), DecodeStrategy::Strict).unwrap(),
            200_000,
        ),
        batch_mean(
            || decode_bytes(S50.as_bytes(), DecodeStrategy::Strict).unwrap(),
            200_000,
        ),
    );
    println!("②⁗ decode Strict 有效 @49B ns（doc 48-71 @30-130B）：{dec:.0}");
}

// ---- ③ 距离面：ns/格 + O(min) 行宽结构证据（10k×100 ≈ 1k×1k 同耗）----

/// 100B 对（10 处替换，距离=10；ns/格 = ns/(100×100)）。
fn dist_inputs_100() -> (String, String) {
    ("abcdefghij".repeat(10), "Xbcdefghij".repeat(10))
}

fn once_distance(a: &str, b: &str) -> f64 {
    let t = Instant::now();
    black_box(edit_distance(a, b));
    t.elapsed().as_nanos() as f64
}

fn perf_distance() {
    let (a, b) = dist_inputs_100();
    assert_eq!(a.len(), 100);
    let ns = median3(
        once_distance(&a, &b),
        once_distance(&a, &b),
        once_distance(&a, &b),
    );
    println!(
        "③′ 距离 100B 对：{ns:.0} ns / 100 格 = {:.2} ns/格（doc ≈1.1）",
        ns / 10_000.0
    );

    // O(min) 同耗证据（差异内容，格数同 1M）：行宽走短侧的结构面跨形状成立。
    // ③‴ 观察项：同串纯内容在极端长宽比下分支预测可退化（实测 ~10x）——
    // 判级用差异内容对，不用纯重复串。
    let long = "abcdefghij".repeat(1_000);
    let wide = "abcdefghij".repeat(100);
    let mixed_wide = "Xbcdefghij".repeat(10); // 100B，1 处替换/10B
    let plain_1k = "abcdefghij".repeat(100);
    let mixed_1k = "Xbcdefghij".repeat(100);
    let half = "abcdefghij".repeat(500);
    let mixed_half = "Xbcdefghij".repeat(20); // 200B
    let shape_10k = median3(
        once_distance(&long, &mixed_wide),
        once_distance(&long, &mixed_wide),
        once_distance(&long, &mixed_wide),
    );
    let shape_5k = median3(
        once_distance(&half, &mixed_half),
        once_distance(&half, &mixed_half),
        once_distance(&half, &mixed_half),
    );
    let shape_1k = median3(
        once_distance(&plain_1k, &mixed_1k),
        once_distance(&plain_1k, &mixed_1k),
        once_distance(&plain_1k, &mixed_1k),
    );
    let degenerate = median3(
        once_distance(&long, &wide),
        once_distance(&long, &wide),
        once_distance(&long, &wide),
    );
    println!(
        "③″ O(min) 同耗证据（差异内容，格数同 1M）：10k×100 {shape_10k:.0} / 5k×200 {shape_5k:.0} / 1k×1k {shape_1k:.0} ns（行宽走短侧，ns/格跨形状持平）"
    );
    println!("③‴ 观察项：同串纯内容 10k×100 = {degenerate:.0} ns（重复串极端长宽比分支预测退化可 ~10x，非遗漏——判级用差异内容对）");
}
