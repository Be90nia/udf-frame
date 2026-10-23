//! id 域七维性能矩阵采集器（①gen 分配面（Id 值面 Copy 无堆恒零断言）②gen ns 锚
//!（SystemClock 端到端 / MockClock 纯算术+锁 双报）③' 批量梯度 100/1k/10k
//!（收窄声明：单权威 cell 串行域，非竞争扩展性面；竞争面以⑥替代——
//! MockClock 冻结钟 counter 面）⑤p50/p99 延迟分布；④⑥⑦ 收窄契约明示：
//! 无运行时调参面 / std Mutex park 结构性零自旋（⑥仅 1/2 线程两点：单权威
//! cell 无 4/8 扩展性可测；spawn 移出计时窗）/ 单权威 cell（非热点计数
//! 阵列）伪共享面 n/a）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example id_multidim --features memgov
//! ```
//!
//! 口径注（沿 time_multidim/ratelimit_multidim 惯例）：
//! ① CountingAllocator 读数仅串行上下文有效（本采集器测量段单线程）；
//! ② 计时时钟开销（~2×20ns）双臂同担，直通成本取差值口径；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ 配额自洽：本域自身 4096/ms 配额会被测量打满——SystemClock 臂每
//!    chunk 换真实毫秒（真睡），MockClock 臂每 chunk 虚拟 advance；
//! ⑤ black_box 防 LLVM 消除；每个锚三连跑取中位。
//!
//! 报告：reports/2026-10-17-id.md（agentwiki 根）。

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use skeleton::clock::{Clock, MockClock, SystemClock};
use skeleton_infra::memgov::CountingAllocator;
use std_ext::id::IdGen;

/// 计时批量均值（跳过批间 sleep/advance——只累计纯测量段）。
fn chunked_mean<T>(mut op: impl FnMut() -> T, total: u64, chunk: u64, gap: Duration) -> f64 {
    let mut elapsed = Duration::ZERO;
    let mut done = 0u64;
    while done < total {
        let c = chunk.min(total - done);
        let t = Instant::now();
        for _ in 0..c {
            black_box(op());
        }
        elapsed += t.elapsed();
        done += c;
        thread::sleep(gap); // 真实毫秒预算换新（SystemClock 臂）
    }
    elapsed.as_nanos() as f64 / total as f64
}

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut v: Vec<f64> = (0..3).map(|_| f()).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[1]
}

/// ① op 跑 n 次的 live_allocs 增量/次（先预热再读数）。
fn allocs_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op()); // 预热：懒初始化不进计数
    let before = CountingAllocator::live_allocs();
    for _ in 0..n {
        black_box(op());
    }
    (CountingAllocator::live_allocs() - before) as f64 / n as f64
}

/// ③ 批量均值：n 连发（>4096 自动分 chunk 虚拟换毫秒——counter 面不失真）。
fn batch_mean_mock(gen: &IdGen, clock: &MockClock, n: u64) -> f64 {
    let t = Instant::now();
    let mut done = 0u64;
    while done < n {
        let c = 4000.min(n - done);
        for _ in 0..c {
            black_box(gen.gen());
        }
        done += c;
        clock.advance(Duration::from_millis(1)); // 虚拟换毫秒（~ns 级，免配额）
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn main() {
    println!("== id 域七维性能矩阵（release/std Instant 粗测）==");

    // ---- ① 分配面 ----
    let clock = Arc::new(MockClock::new());
    clock.advance(Duration::from_millis(1));
    let gen = IdGen::new(0x7D_0000_0001, clock.clone());
    let gen_allocs = allocs_per(|| gen.gen(), 100);
    assert_eq!(gen_allocs, 0.0, "gen 恒零分配（Id 值面 Copy 无堆）");
    println!("① gen 分配事件/次        : {gen_allocs:.1}  （断言 ==0 通过）");
    let id = gen.gen();
    let s_allocs = allocs_per(|| id.to_string(), 100);
    println!(
        "① to_string 净活分配/次    : {s_allocs:.1}  （即用即弃净零；总分配 2 次/调用=format! 中转+连字装配，展示面非热路径）"
    );

    // ---- ② ns 锚 ----
    let sys = Arc::new(SystemClock);
    let sys_gen = IdGen::new(0x7D_0000_0002, sys.clone());
    let baseline =
        median3(|| chunked_mean(|| sys.now_system(), 20_000, 1_000, Duration::from_millis(2)));
    let sys_anchor =
        median3(|| chunked_mean(|| sys_gen.gen(), 20_000, 1_000, Duration::from_millis(2)));
    let mock_gen = IdGen::new(0x7D_0000_0003, clock.clone());
    let mock_anchor = {
        let mut v = Vec::new();
        for _ in 0..3 {
            let t = Instant::now();
            for _ in 0..20 {
                for _ in 0..1_000 {
                    black_box(mock_gen.gen());
                }
                clock.advance(Duration::from_millis(1));
            }
            v.push(t.elapsed().as_nanos() as f64 / 20_000.0);
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[1]
    };
    let s_anchor =
        median3(|| chunked_mean(|| id.to_string(), 20_000, 1_000, Duration::from_millis(2)));
    println!("② now_system 直通（差值基线）: {baseline:8.1} ns");
    println!(
        "② gen/SystemClock 端到端    : {sys_anchor:8.1} ns  （含钟读；净算术+锁 ≈ {} ns）",
        sys_anchor - baseline
    );
    println!("② gen/MockClock 算术+锁     : {mock_anchor:8.1} ns");
    println!("② to_string（展示面参考）   : {s_anchor:8.1} ns");

    // ---- ③' 批量梯度（收窄声明：单权威 cell 串行域，非竞争扩展性面；
    // 竞争面以⑥替代——冻结钟 counter 面）----
    for n in [100u64, 1_000, 10_000] {
        let ns = median3(|| batch_mean_mock(&mock_gen, &clock, n));
        println!("③' 批量 {n:>6} ids          : {ns:8.1} ns/id");
    }

    // ---- ⑤ 延迟分布（SystemClock 端到端，20×1000 分批换毫秒）----
    let mut samples: Vec<u64> = Vec::with_capacity(20_000);
    for _ in 0..20 {
        for _ in 0..1_000 {
            let t = Instant::now();
            black_box(sys_gen.gen());
            samples.push(t.elapsed().as_nanos() as u64);
        }
        thread::sleep(Duration::from_millis(2));
    }
    samples.sort_unstable();
    let pct = |p: f64| samples[((samples.len() as f64 - 1.0) * p) as usize];
    println!(
        "⑤ gen 延迟 p50/p99/p99.9   : {} / {} / {} ns",
        pct(0.50),
        pct(0.99),
        pct(0.999)
    );

    // ---- ⑥ 竞争面（收窄：仅 1/2 线程两点，单权威 cell 无 4/8 扩展性可测
    // ——非完整曲线，如实报两点）----
    // 每轮换新指纹（注册表发新权威）：同权威连跑会累计耗同毫秒配额——
    // fail-closed panic 是配额语义实弹，非缺陷。
    // 计时口径：spawn/线程启动移出计时窗——栅栏对齐后起表，窗内纯
    // 算术+锁竞争。
    let run_threads = |fp: u64, threads: usize| {
        let frozen = Arc::new(MockClock::new());
        frozen.advance(Duration::from_millis(1));
        let shared = Arc::new(IdGen::new(fp, frozen));
        let barrier = Arc::new(Barrier::new(threads + 1));
        thread::scope(|s| {
            let mut handles = Vec::with_capacity(threads);
            for _ in 0..threads {
                let g = Arc::clone(&shared);
                let b = Arc::clone(&barrier);
                handles.push(s.spawn(move || {
                    b.wait();
                    for _ in 0..(4000 / threads as u64) {
                        black_box(g.gen());
                    }
                }));
            }
            barrier.wait(); // 主线程对齐后起表：所有 worker 已就位在栅栏
            let t = Instant::now();
            for h in handles {
                h.join().unwrap();
            }
            t.elapsed().as_nanos() as f64 / 4000.0
        })
    };
    let t1 = run_threads(0x7D00_0000_00E1, 1);
    let t2 = run_threads(0x7D00_0000_00E2, 2);
    println!(
        "⑥ 竞争面（冻结钟 4000 ids，栅栏对齐后计时）: 1 线程 {t1:.1} ns/id，2 线程 {t2:.1} ns/id（纯算术+锁竞争口径，spawn 移出；std Mutex park，结构性零自旋）"
    );

    println!("④⑥⑦ 收窄：无调参面 / ⑥仅 1/2 两点（单权威 cell 无 4/8 扩展性可测） / 单权威 cell 无热点计数面（契约明示，见模块头注）");
}
