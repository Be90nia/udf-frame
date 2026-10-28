//! fsx 域性能七维采集器（单线程 IO 域收窄：①遍历分配事件锚②文件数梯度
//! 100/1k/10k③深度梯度 4/16/64；④并发曲线⑤锁⑥内存水位⑦伪共享=n/a——
//! 无锁无线程面。热缓存口径：先建树后多轮采样取中位；IO 吞吐不做平台间
//! 绝对值对标，只报本机锚。报告：fsx 域性能锚（本地留存，未入库））。
//!
//! 沿 multidim 模式纯 std，零新依赖。运行：
//! `cargo run -p udf-std-ext --example fsx_multidim --release`

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use std_ext::fsx::{temp_dir_in, walk, Sort, TempDir};

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// SAFETY: 纯转发 System，仅在转发前累加事件计数；null 原样透传（alloc 契约）。
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

/// op 执行期的分配事件数（瞬时生灭计入；对齐 tests/paths_alloc.rs 口径）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    let before = EVENTS.load(Ordering::Relaxed);
    let out = op();
    black_box(&out);
    EVENTS.load(Ordering::Relaxed) - before
}

/// 中位数采样（奇数次取中位，对齐既有 multidim 先例）；返回 (ns, 条目数)。
fn median_ns(iters: u32, mut f: impl FnMut() -> usize) -> (u128, usize) {
    let mut samples = Vec::with_capacity(iters as usize);
    let mut entries = 0;
    for _ in 0..iters {
        let t0 = Instant::now();
        entries = f();
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    (samples[samples.len() / 2], entries)
}

fn walk_entries(root: &Path) -> usize {
    let mut n = 0;
    for e in walk(root) {
        black_box(e.unwrap());
        n += 1;
    }
    n
}

fn build_flat(root: &Path, n: usize) {
    for i in 0..n {
        fs::write(root.join(format!("f{i:06}")), b"").unwrap();
    }
}

fn build_deep(root: &Path, depth: usize) {
    let mut cur = root.to_path_buf();
    for i in 0..depth {
        cur.push(format!("d{i:03}"));
        fs::create_dir(&cur).unwrap();
        fs::write(cur.join("leaf"), b"").unwrap();
    }
}

fn main() {
    // 落点=target/（CARGO_TARGET_DIR 或缺省 target），域件 RAII 自清（禁系统 TEMP）
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let scratch: TempDir = temp_dir_in(&base, "fsx-multidim-").expect("scratch under target/");

    println!("## fsx 域锚（release/热缓存/Instant 中位数/7 次采样）");
    println!();
    println!("### ② 文件数梯度（平铺 Sort::None）");
    println!("| 条目数 | ns/entry | 完整遍历 ns |");
    println!("|---|---|---|");
    for n in [100usize, 1_000, 10_000] {
        let root = scratch.path().join(format!("flat{n}"));
        fs::create_dir(&root).unwrap();
        build_flat(&root, n);
        let (ns, entries) = median_ns(7, || walk_entries(&root));
        assert_eq!(entries, n + 1, "平铺 {n}：产出=文件+根本身");
        println!("| {n} | {} | {ns} |", ns / entries as u128);
        fs::remove_dir_all(&root).unwrap();
    }

    println!();
    println!("### ②' 排序模式代价（10k 平铺 Sort::ByName，+1/entry 收集缓冲声明）");
    println!("| 条目数 | ns/entry | 完整遍历 ns |");
    println!("|---|---|---|");
    let root = scratch.path().join("flat10k");
    fs::create_dir(&root).unwrap();
    build_flat(&root, 10_000);
    let (ns, entries) = median_ns(7, || {
        let mut n = 0;
        for e in walk(&root).sort(Sort::ByName) {
            black_box(e.unwrap());
            n += 1;
        }
        n
    });
    println!("| {entries} | {} | {ns} |", ns / entries as u128);
    fs::remove_dir_all(&root).unwrap();

    println!();
    println!("### ③ 深度梯度（逐层 1 文件）");
    println!("| 深度 | 条目数 | ns/entry | 完整遍历 ns |");
    println!("|---|---|---|---|");
    for depth in [4usize, 16, 64] {
        let root = scratch.path().join(format!("deep{depth}"));
        fs::create_dir(&root).unwrap();
        build_deep(&root, depth);
        let (ns, entries) = median_ns(7, || walk_entries(&root));
        assert_eq!(
            entries,
            2 * depth + 1,
            "深度 {depth}：产出=每层（目录+文件）+根本身"
        );
        println!("| {depth} | {entries} | {} | {ns} |", ns / entries as u128);
        fs::remove_dir_all(&root).unwrap();
    }

    println!();
    println!("### ① 分配事件锚（1k 平铺完整遍历窗）");
    let root = scratch.path().join("alloc1k");
    fs::create_dir(&root).unwrap();
    build_flat(&root, 1_000);
    black_box(walk_entries(&root)); // 预热：懒初始化出窗
    let events = allocs(|| {
        let n = walk_entries(&root);
        assert_eq!(n, 1_001);
    });
    println!();
    println!(
        "events/entry = {:.2}（{} events / 1001 条目；含 yield 路径本体",
        events as f64 / 1001.0,
        events
    );
    println!("clone+push 容量事件与 std `file_name` 复制——自有面=路径本体 1 个 owned PathBuf，");
    println!("std 内部件不归本域，声明见 fsx.rs 模块头注①）。");
}
