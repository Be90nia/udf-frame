//! log 文件落盘多维实测采集器（拍板性能矩阵的文件面：①分配事件逐路径
//! counting allocator 实证——行写入/换日首开/格式化三径报数，禁只写「零分配」
//! 声明；②uncontended ns 锚与吞吐；③竞争扩展性曲线（1/2/4/8 线程共享单
//! sink）；⑤延迟分布（p50/p99/p99.9，行写入与事件全径双臂）。⑥零自旋=std
//! Mutex park 结构性 n/a（本域无自旋循环可测，竞争面形状由③留痕）；⑦伪共享
//! n/a=单权威单缓冲结构（无 per-线程热点计数可 pad）；④运行时调参=每行
//! flush 为拍板语义、BufWriter 容量未暴露（升级面见 logx.rs ponytail 注）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --features log,memgov --example log_multidim
//! ```
//!
//! 口径注（沿 memory_pool_profile 先例）：
//! ① CountingAllocator 常驻本二进制；分配计数=前后差分，仅串行窗内有效；
//! ② std Instant 粗测——数字供判级留痕，不供微优化基线，结论看量级与曲线形状；
//! ③ scratch 落 CARGO_TARGET_DIR 或 target/ 回退（禁系统 TEMP），RAII 自清。

use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use skeleton::logpath::LogPathResolver;
use skeleton_infra::logx::{init_with_file, FileSinkWriter};
use skeleton_infra::memgov::CountingAllocator;
use std_ext::fsx::temp_dir_in;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// 契约替身：任意根目录注入。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

fn scratch(tag: &str) -> std_ext::fsx::TempDir {
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    temp_dir_in(&base, &format!("logx-{tag}-")).expect("scratch under target/")
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

fn main() {
    let scratch = scratch("multidim");
    let root = scratch.path().join("logs");
    let sink = Arc::new(skeleton_infra::logx::FileSink::new(&root).expect("root creatable"));
    let line = Arc::new(Vec::from(
        b"2026-10-17 12:34:56.789 INFO test_app::worker warm body for profile write\n" as &[u8],
    ));
    let mut w = FileSinkWriter::new(Arc::clone(&sink));
    println!(
        "=== FileSink multidim profile (release, {}B lines) ===",
        line.len()
    );

    // 预热：首行走首开分支（与换日同分支），create_dir_all/open/BufWriter
    // 容量分配挪出热窗；stdout LineWriter 缓冲同步预热。
    w.write_all(&line).expect("warm line");
    init_with_file(&FixedRoot(root.join("fmt")), log::LevelFilter::Info).expect("collector init");
    tracing::info!(target: "profile::fmt", seq = 0u64, "format path warmup");

    // ── ①a 稳态行写入分配事件（诊断热路径，期望 0）────────────────────────
    let before = CountingAllocator::live_allocs();
    for _ in 0..10_000 {
        w.write_all(&line).expect("line");
    }
    let delta = CountingAllocator::live_allocs() - before;
    println!("①a steady-state line write: {delta} allocs per 10k lines (expect 0)");
    // live_allocs=存活集 gauge，对「即用即还」分配恒 0——用峰值差分补证
    // 窗口内确无任何分配事件发生（retained 与 transient 双口径）。
    CountingAllocator::reset_peak();
    for _ in 0..1_000 {
        w.write_all(&line).expect("line");
    }
    println!(
        "①a' steady-state window peak delta: {}B (expect 0 = zero allocation events)",
        CountingAllocator::peak_bytes()
    );

    // ── ①b 换日/首开分配事件（create_dir_all+open+BufWriter 容量）─────────
    // 换日与首行走同一分支（writer.is_none() || day 变更），成本同径。
    let mut cold = FileSinkWriter::new(Arc::new(
        skeleton_infra::logx::FileSink::new(root.join("cold")).expect("cold root"),
    ));
    let before = CountingAllocator::live_allocs();
    let bytes_before = CountingAllocator::live_bytes();
    cold.write_all(&line).expect("cold line");
    let delta = CountingAllocator::live_allocs() - before;
    let net = CountingAllocator::live_bytes() as isize - bytes_before as isize;
    println!("①b rollover/first-open path: +{delta} allocs, net retained {net}B (path String + PathBuf + BufWriter cap)");

    // ── ①c 格式化路径分配事件（init_with_file 双 sink，各一份格式串）──────
    let before = CountingAllocator::live_allocs();
    const EVENTS: u64 = 10_000;
    for i in 0..EVENTS {
        tracing::info!(target: "profile::fmt", seq = i, "format path allocation probe");
    }
    let delta = CountingAllocator::live_allocs() - before;
    println!(
        "①c format path (console+file dual sink): {delta} allocs per {EVENTS} events (~{:.1}/event; two layers by construction)",
        delta as f64 / EVENTS as f64
    );
    // live 差分=0 只说明临时串即用即还——峰值差分证明事件内确有分配发生。
    // peak_bytes 是高水位（含 reset 时既有存活集），口径=peak−live0=窗口内
    // 最大瞬时分配占用（transient 上界）。
    CountingAllocator::reset_peak();
    let live0 = CountingAllocator::live_bytes();
    tracing::info!(target: "profile::fmt", seq = 0u64, "peak probe single event");
    println!(
        "①c' single event max transient footprint: {}B (peak−live0; freed within the event, retained 0)",
        CountingAllocator::peak_bytes().saturating_sub(live0)
    );

    // ── ② uncontended 行写入锚 + 吞吐 ─────────────────────────────────────
    let n = 100_000u32;
    let t0 = Instant::now();
    for _ in 0..n {
        w.write_all(&line).expect("line");
    }
    let wall = t0.elapsed();
    let secs = wall.as_secs_f64();
    println!(
        "② uncontended line write: {:.0} ns/line | {:.2} M lines/s | {:.0} MB/s (batch mean over {n})",
        wall.as_nanos() as f64 / n as f64,
        n as f64 / secs / 1e6,
        n as f64 * line.len() as f64 / secs / 1e6,
    );

    // ── ⑤a 行写入延迟分布 ──────────────────────────────────────────────────
    let n5 = 50_000usize;
    let mut lat = Vec::with_capacity(n5);
    for _ in 0..n5 {
        let t = Instant::now();
        w.write_all(&line).expect("line");
        lat.push(t.elapsed().as_nanos() as u64);
    }
    lat.sort_unstable();
    println!(
        "⑤a line write latency p50={}ns p99={}ns p99.9={}ns (n={n5})",
        pct(&lat, 50),
        pct(&lat, 99),
        pct(&lat, 999),
    );

    // ── ⑤b 事件全径延迟分布（format+双 sink 落盘，经全局订阅者）───────────
    let n5b = 20_000usize;
    let mut lat2 = Vec::with_capacity(n5b);
    for i in 0..n5b as u64 {
        let t = Instant::now();
        tracing::info!(target: "profile::fmt", seq = i, "format latency probe");
        lat2.push(t.elapsed().as_nanos() as u64);
    }
    lat2.sort_unstable();
    println!(
        "⑤b full event latency (format+console+file): p50={}ns p99={}ns p99.9={}ns (n={n5b})",
        pct(&lat2, 50),
        pct(&lat2, 99),
        pct(&lat2, 999),
    );

    // ── ③ 竞争扩展性：1/2/4/8 线程共享单 sink（单锁单文件）────────────────
    println!("③ contended throughput (shared single FileSink, 400ms window):");
    for threads in [1usize, 2, 4, 8] {
        let stop = Arc::new(AtomicU64::new(0));
        let total = Arc::new(AtomicU64::new(0));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let w = FileSinkWriter::new(Arc::clone(&sink));
            let line = Arc::clone(&line);
            let stop = Arc::clone(&stop);
            let total = Arc::clone(&total);
            handles.push(thread::spawn(move || {
                let mut w = w;
                let mut ops = 0u64;
                while stop.load(Ordering::Relaxed) == 0 {
                    w.write_all(&line).expect("line");
                    ops += 1;
                }
                total.fetch_add(ops, Ordering::Relaxed);
            }));
        }
        thread::sleep(Duration::from_millis(400));
        stop.store(1, Ordering::Relaxed);
        for h in handles {
            h.join().expect("collector worker");
        }
        let ops = total.load(Ordering::Relaxed);
        println!(
            "   threads={threads}: {:.2} M lines/s | {:.0} MB/s aggregate",
            ops as f64 / 0.4 / 1e6,
            ops as f64 * line.len() as f64 / 0.4 / 1e6,
        );
    }

    println!("=== notes: ⑥ zero-spin = std Mutex park (no spin loop); ⑦ n/a single-authority single-buffer; ④ per-line flush is the approved semantic (BufWriter cap not exposed)");
}
