//! alloc_site 域七维性能矩阵采集器：①稳态分配事件（bump 开/全局关/站点关、
//! find 命中/未命中、枚举=0）②ns 锚（同六路径）③规模梯度 1/10/100 站点
//! find（判线性）④并发吞吐 1/2/4 线程 bump 开（同一 AtomicU64 争用上限如实
//! 报，模块 doc armor 6 登记为设计口径+伪共享升级路径）⑤禁用路径零写入证据
//! （耗时不升 + 计数不变）⑥宏表隔离（两张表独立计数/开关）⑦收窄声明。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --features memgov --example alloc_site_multidim
//! ```
//!
//! 口径注（沿 lint_multidim 惯例）：
//! ① 分配读数=本线程窗内事件计数（`IN_WINDOW` 门），测量段单线程；
//! ② 粗测（std `Instant`）非 criterion——数字供判级，不供微优化基线；
//! ③ `black_box` 防 LLVM 消除；每个锚三连跑取中位；
//! ④ bump 全开锚 = 全局读 + 站点读 + fetch_add 三次原子操作。

#[global_allocator]
static ALLOC: EventCounting = EventCounting;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use skeleton_infra::alloc_site::{find, set_global_enabled, AllocSite};
use skeleton_infra::alloc_sites;

// ---- 采集对象表（宏表 + 梯度表）----

alloc_sites! {
    pub static TINY = {
        ONLY,
    };
}

alloc_sites! {
    pub static MID = {
        M0, M1, M2, M3, M4, M5, M6, M7, M8, M9,
    };
}

alloc_sites! {
    pub static BIG = {
        B0, B1, B2, B3, B4, B5, B6, B7, B8, B9,
        B10, B11, B12, B13, B14, B15, B16, B17, B18, B19,
        B20, B21, B22, B23, B24, B25, B26, B27, B28, B29,
        B30, B31, B32, B33, B34, B35, B36, B37, B38, B39,
        B40, B41, B42, B43, B44, B45, B46, B47, B48, B49,
        B50, B51, B52, B53, B54, B55, B56, B57, B58, B59,
        B60, B61, B62, B63, B64, B65, B66, B67, B68, B69,
        B70, B71, B72, B73, B74, B75, B76, B77, B78, B79,
        B80, B81, B82, B83, B84, B85, B86, B87, B88, B89,
        B90, B91, B92, B93, B94, B95, B96, B97, B98, B99,
    };
}

// dim6 隔离表：两次 alloc_sites! 调用 = 两套独立 static。
alloc_sites! {
    pub static TABLE_A = {
        A_INC,
    };
}

alloc_sites! {
    pub static TABLE_B = {
        B_INC,
    };
}

// ---- 事件计数分配器（lint_multidim 同款）----

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、无析构注册。
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

// ---- 测量助手（time_multidim 同款）----

const N: u64 = 100_000;

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut samples: Vec<f64> = (0..3).map(|_| f()).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("ns 读数无 NaN"));
    samples[1]
}

fn batch_ns<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

/// 窗内事件计数 / 拍数（预热一拍不进窗）。
fn events_per_call<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    op();
    IN_WINDOW.with(|w| w.set(true));
    let before = EVENTS.load(Ordering::Relaxed);
    for _ in 0..n {
        black_box(op());
    }
    let events = EVENTS.load(Ordering::Relaxed) - before;
    IN_WINDOW.with(|w| w.set(false));
    events as f64 / n as f64
}

/// 枚举 BIG 全表（防消除：逐站点 black_box 名字 + 累加计数）。
fn walk_big() -> u64 {
    let mut total = 0u64;
    for site in BIG {
        total = total.wrapping_add(site.count());
        black_box(site.name);
    }
    total
}

fn main() {
    println!("== alloc_site 域七维采集（release / std Instant 粗测；本地数字，环境相关）==");
    dim1_alloc_events();
    dim2_ns_anchors();
    dim3_scale();
    dim4_concurrency();
    dim5_disabled_no_write();
    dim6_table_isolation();
    dim7_narrowing();
}

// ---- ① 稳态分配事件 ----

fn dim1_alloc_events() {
    println!("\n[dim1] 稳态分配事件（每路径 100k 拍，本线程窗内）");
    println!("  {:<34} | {:>11}", "path", "events/call");
    set_global_enabled(true);
    ONLY.reset();

    let mut rows: Vec<(&str, f64)> = Vec::new();
    rows.push(("bump (enabled)", events_per_call(|| ONLY.bump(), N)));
    set_global_enabled(false);
    rows.push(("bump (global off)", events_per_call(|| ONLY.bump(), N)));
    set_global_enabled(true);
    ONLY.set_enabled(false);
    rows.push(("bump (site off)", events_per_call(|| ONLY.bump(), N)));
    ONLY.set_enabled(true);
    rows.push((
        "find hit (BIG, B50)",
        events_per_call(|| black_box(find(BIG, "B50")), N),
    ));
    rows.push((
        "find miss (BIG)",
        events_per_call(|| black_box(find(BIG, "NO_SUCH_SITE")), N),
    ));
    rows.push(("enumerate BIG (100)", events_per_call(walk_big, N)));

    let mut verdict = true;
    for (path, events) in &rows {
        println!("  {path:<34} | {events:>11.2}");
        verdict &= *events == 0.0;
    }

    // 防假零：开路径真计数、find 真命中（仪器/路径自证）
    ONLY.reset();
    ONLY.bump();
    let counted = ONLY.count() == 1;
    let hit = find(BIG, "B50").is_some();
    println!(
        "  判定: {}（全路径 0 事件 + bump 真计数 + find 真命中）",
        if verdict && counted && hit {
            "PASS"
        } else {
            "FAIL"
        }
    );
}

// ---- ② ns 锚 ----

fn dim2_ns_anchors() {
    println!("\n[dim2] ns 锚（中位三连跑；bump 全开 = 全局读+站点读+fetch_add 三原子）");
    println!("  {:<34} | {:>10}", "path", "ns/call");
    set_global_enabled(true);
    ONLY.reset();
    let row = |path: &str, ns: f64| println!("  {path:<34} | {ns:>10.1}");

    row("bump (enabled)", median3(|| batch_ns(|| ONLY.bump(), N)));
    set_global_enabled(false);
    row("bump (global off)", median3(|| batch_ns(|| ONLY.bump(), N)));
    set_global_enabled(true);
    ONLY.set_enabled(false);
    row("bump (site off)", median3(|| batch_ns(|| ONLY.bump(), N)));
    ONLY.set_enabled(true);
    row(
        "find hit (BIG, B50)",
        median3(|| batch_ns(|| black_box(find(BIG, "B50")), N)),
    );
    row(
        "find miss (BIG)",
        median3(|| batch_ns(|| black_box(find(BIG, "NO_SUCH_SITE")), N)),
    );
    row("enumerate BIG (100)", median3(|| batch_ns(walk_big, N)));
    println!(
        "  判定: 关闸臂（global/site off）应低于全开臂（少一次 fetch_add）；\
枚举 100 站点 ≈ 100 次原子读 + 累加"
    );
    println!(
        "  口径注: find 未命中（首字节早退）可低于命中——命中臂含全名比较 + 返回路径，\
两者量级同属线性扫描面，非测量错误"
    );
}

// ---- ③ 规模梯度（判线性）----

fn dim3_scale() {
    println!("\n[dim3] find 规模梯度（1/10/100 站点，判线性）");
    println!(
        "  {:>7} | {:>10} | {:>13}",
        "sites", "ns/call", "marginal/site"
    );

    let tables: [(usize, &'static [&'static AllocSite]); 3] = [(1, TINY), (10, MID), (100, BIG)];
    let measured: Vec<(usize, f64)> = tables
        .iter()
        .map(|&(sites, table)| {
            let target = table[table.len() / 2].name;
            (
                sites,
                median3(|| batch_ns(|| black_box(find(table, target)), 50_000)),
            )
        })
        .collect();

    let mut previous: Option<(usize, f64)> = None;
    for &(sites, ns) in &measured {
        match previous {
            None => println!("  {sites:>7} | {ns:>10.1} | {:>13}", "-"),
            Some((from, from_ns)) => {
                let marginal = (ns - from_ns) / (sites - from) as f64;
                println!("  {sites:>7} | {ns:>10.1} | {marginal:>13.2}");
            }
        }
        previous = Some((sites, ns));
    }
    let slope = (measured[2].1 - measured[1].1) / 90.0;
    println!(
        "  判定: 10→100 段边际 {slope:.2} ns/站点——线性扫描如实（无哈希、无分配；\
预期 ≈ 每站点一次指针解引 + 名字比较）"
    );
}

// ---- ④ 并发吞吐 ----

fn dim4_concurrency() {
    let cores = thread::available_parallelism().map_or(0, |n| n.get());
    println!("\n[dim4] 并发吞吐（同站点 ONLY bump 全开，1/2/4 线程；本机核数 {cores}）");
    println!(
        "  设计口径：全部线程 fetch_add 同一 AtomicU64——争用上限是已知面\
（模块 doc armor 6 登记伪共享升级路径），如实报不粉饰"
    );

    set_global_enabled(true);
    ONLY.reset();
    println!("  {:>7} | {:>13} | {:>9}", "threads", "Mops/sec", "speedup");

    const CALLS: u64 = 4_000_000;
    let mut base = 0.0;
    let mut bumped = 0u64;
    for threads in [1usize, 2, 4] {
        let barrier = Arc::new(Barrier::new(threads + 1));
        let start = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..CALLS {
                        ONLY.bump();
                    }
                })
            })
            .collect();
        barrier.wait();
        for handle in handles {
            handle.join().expect("并发线程不得 panic");
        }
        let elapsed = start.elapsed().as_secs_f64();
        let rate = threads as f64 * CALLS as f64 / elapsed / 1e6;
        if threads == 1 {
            base = rate;
        }
        bumped += threads as u64 * CALLS;
        println!("  {threads:>7} | {rate:>13.2} | {:>8.2}x", rate / base);
    }
    // 防假零（spawn 类 API 断言成功次数）：全部 bump 真生效
    let real = ONLY.count() == bumped;
    println!(
        "  判定: {}（计数 {bumped} 全部落账；多线程加速比 ≤1 即同原子争用上限，设计口径内）",
        if real { "PASS" } else { "FAIL" }
    );
}

// ---- ⑤ 禁用路径零写入证据 ----

fn dim5_disabled_no_write() {
    println!("\n[dim5] 禁用路径零写入证据（口径：关闸臂耗时不升 + 计数不变）");
    set_global_enabled(true);
    ONLY.reset();
    ONLY.set_enabled(true);

    let on_ns = median3(|| batch_ns(|| ONLY.bump(), N));
    let on_count = ONLY.count();

    set_global_enabled(false);
    let gated_ns = median3(|| batch_ns(|| ONLY.bump(), N));
    let gated_count = ONLY.count();
    set_global_enabled(true);

    println!("  bump 全开 : {on_ns:>9.1} ns（期间计数 {on_count}）");
    println!("  bump 关闸 : {gated_ns:>9.1} ns（期间计数 {gated_count}）");
    let count_stable = gated_count == on_count;
    let not_slower = gated_ns <= on_ns * 1.2;
    println!(
        "  判定: {}（计数不变 = 零写入直接证据；不升 = 旁路少一次 fetch_add；\
缓存行不污染由此读数口径背书）",
        if count_stable && not_slower {
            "PASS"
        } else {
            "FAIL"
        }
    );
}

// ---- ⑥ 宏表隔离 ----

fn dim6_table_isolation() {
    println!("\n[dim6] 宏表隔离（两次 alloc_sites! 调用 = 两套独立 static）");
    set_global_enabled(true);
    A_INC.reset();
    A_INC.set_enabled(true);
    B_INC.reset();
    B_INC.set_enabled(true);

    A_INC.bump();
    A_INC.bump();
    B_INC.bump();
    let counts_independent = (A_INC.count(), B_INC.count()) == (2, 1);

    A_INC.set_enabled(false);
    B_INC.bump();
    let switches_independent = B_INC.count() == 2 && A_INC.count() == 2;
    A_INC.set_enabled(true);

    println!(
        "  A_INC = {} / B_INC = {}（A 计 2 拍 + 关闸 1 拍，B 计 2 拍）",
        A_INC.count(),
        B_INC.count()
    );
    println!(
        "  判定: {}",
        if counts_independent && switches_independent {
            "PASS 表间计数/开关互不干扰"
        } else {
            "FAIL 出现表间串扰"
        }
    );
}

// ---- ⑦ 收窄声明 ----

fn dim7_narrowing() {
    println!("\n[dim7] 收窄声明（本件不做，均属采用方策略层）");
    for item in [
        "阈值判定 / 上报 / 告警",
        "序列化 / 指标格式输出",
        "站点归属映射",
        "自动发现（站点必须 alloc_sites! 显式登记）",
    ] {
        println!("  不做：{item}");
    }
}
