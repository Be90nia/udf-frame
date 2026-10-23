//! 组合类七维性能矩阵采集器（`PooledThreadPool<T>` 组合端到端口径；拍板
//! 2026-10-16：组合态全链路借→submit→执行→自动还，补齐七维终表第三列）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example pooled_thread_pool_multidim --features pool
//! ```
//!
//! 七维对表（派单契约）：
//! ①全链零分配（ZST 任务 end-to-end alloc=0；Box 类型擦除包装单列如实报数）
//! ②组合往返锚（提交含借还的完整周期）③N 并发提交者吞吐曲线（1/2/4/8）
//! ④组合 resize 代价（压场调参 + 缩容/借空联合语义溢出/拒绝行为锚）
//! ⑤组合延迟分布 p50/p99 ⑥等待者零自旋确认（借空等待+队列等待两处）
//! ⑦组合态伪共享（双池计数隔离 + 组合计数热点）
//!
//! 口径注（沿 multidim 惯例）：
//! ① CountingAllocator 常驻本二进制（内存维前提），计时含每分配 2 次 Relaxed
//!    原子加噪声（量级 ns）；
//! ② 粗测 std Instant 非 criterion——数字供判级留痕，不供微优化基线；
//! ③ 并发段只计吞吐不计内存；`live_allocs` 是净存活计数（闸门隔离释放窗才可见
//!    分配，同 thread_pool_multidim ①先例）；逐段 shutdown+join 清场。

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton_infra::memgov::{CountingAllocator, MemoryPool, MemoryTuning};
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use skeleton_infra::pooled_thread_pool::PooledThreadPool;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// 组合借用超时（D21 同源；零自旋臂单发另配）。
const BORROW_TIMEOUT: Duration = Duration::from_millis(100);

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// 轮询至条件成立（bench 清场/排空用，禁裸 sleep 定时）。
fn wait_until(deadline: Duration, mut f: impl FnMut() -> bool) -> bool {
    let start = Instant::now();
    while !f() {
        if start.elapsed() > deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

fn combo(
    core: usize,
    max: usize,
    queue_cap: usize,
    mem_max: usize,
    mem_idle: usize,
    borrow_timeout: Duration,
) -> PooledThreadPool<Vec<u8>> {
    PooledThreadPool::new(
        Arc::new(MemoryPool::<Vec<u8>>::new(mem_max, mem_idle, Vec::new)),
        ThreadPool::new(ThreadPoolTuning::new(core, max, queue_cap)),
        borrow_timeout,
    )
}

/// 预热内存池（工厂/容量分配挪出热窗，沿 memory_pool_profile 惯例）。
fn prewarm(mem: &MemoryPool<Vec<u8>>, n: usize) {
    let warm: Vec<_> = (0..n)
        .filter_map(|_| mem.acquire(BORROW_TIMEOUT).ok())
        .collect();
    drop(warm);
}

/// 固定量提交风暴：`submitters` 线程各提交 n/submitters 件（任务=借缓冲→碰一件
/// →计数完成）。返回 (完成数, 受理数, 提交拒绝数, 借空弃件数, 风暴耗时)。
/// 排空判据 = 账目恒等式：完成 + 拒绝 + 借弃 ≥ 受理。
fn storm(
    ptp: &PooledThreadPool<Vec<u8>>,
    submitters: usize,
    n: u64,
) -> (u64, u64, u64, u64, Duration) {
    let done = Arc::new(AtomicU64::new(0));
    let accepted = Arc::new(AtomicU64::new(0));
    let refused = Arc::new(AtomicU64::new(0));
    let br0 = ptp.borrow_rejected();
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..submitters {
            let (done, accepted, refused) = (
                Arc::clone(&done),
                Arc::clone(&accepted),
                Arc::clone(&refused),
            );
            s.spawn(move || {
                let mine = n / submitters as u64;
                for _ in 0..mine {
                    let d = Arc::clone(&done);
                    match ptp.submit(move |buf: &mut Vec<u8>| {
                        black_box(buf.is_empty());
                        d.fetch_add(1, Ordering::Relaxed);
                    }) {
                        Ok(()) => {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(rejected) => {
                            drop(rejected);
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });
    let elapsed = t0.elapsed();
    wait_until(Duration::from_secs(30), || {
        done.load(Ordering::Relaxed)
            + refused.load(Ordering::Relaxed)
            + (ptp.borrow_rejected() - br0)
            >= accepted.load(Ordering::Relaxed)
    });
    (
        done.load(Ordering::Relaxed),
        accepted.load(Ordering::Relaxed),
        refused.load(Ordering::Relaxed),
        ptp.borrow_rejected() - br0,
        elapsed,
    )
}

fn main() {
    println!("== PooledThreadPool 组合七维矩阵（release, 纯 std）==\n");
    dim1_combo_allocs();
    dim2_roundtrip();
    dim3_throughput();
    dim4_resize_combo();
    dim5_latency_dist();
    dim6_zero_spin();
    dim7_false_sharing();
    println!("\n== 采集完毕 ==");
}

/// ①全链零分配：闸门隔离释放窗（同 thread_pool_multidim ①先例）——worker 全部
/// 闸住后整批入队，live 差=队列中存活的分配数。ZST 任务全链恰 1/提交（Box 类型
/// 擦除包装；借还+执行 0，对照臂同二进制直借直还 10k=0 佐证）。
fn dim1_combo_allocs() {
    let ptp = combo(2, 2, 32_768, 64, 64, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 64);
    // 闸住两个 worker（无锁 AtomicBool 闸门——零 Mutex/unwrap，worker 自旋占位，
    // 被测 N 件任务的 Box 分配存活在队列里，闸门隔离释放窗）
    let gate = Arc::new(AtomicBool::new(false));
    for _ in 0..2 {
        let g = Arc::clone(&gate);
        ptp.submit(move |buf: &mut Vec<u8>| {
            while !g.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            black_box(buf);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || ptp.threads().metrics().busy == 2);
    const N: u64 = 20_000;
    let submit_one = |ptp: &PooledThreadPool<Vec<u8>>, done: &Arc<AtomicU64>| {
        let d = Arc::clone(done);
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap()
    };
    let done = Arc::new(AtomicU64::new(0));
    // ZST 任务臂：f 无捕获，包装闭包仍携带 mem/计数 Arc——量的是组合全链下限。
    let before = CountingAllocator::live_allocs();
    for _ in 0..N {
        submit_one(&ptp, &done);
    }
    let allocs = CountingAllocator::live_allocs() - before;
    // 对照臂：同池直借直还（无提交无 Box），稳态应=0
    let p0 = CountingAllocator::live_allocs();
    for _ in 0..10_000 {
        drop(ptp.mem_pool().acquire(BORROW_TIMEOUT).unwrap());
    }
    let pool_only = CountingAllocator::live_allocs() - p0;
    assert_eq!(allocs, N as usize, "ZST 任务全链每提交恰 1 Box，禁其他分配");
    assert_eq!(pool_only, 0, "借还路径稳态零分配");
    println!(
        "①全链分配/ZST任务: {allocs} allocs / {N} submits = {:.2}/提交（恰 1 Box=类型擦除包装，单列；借还路径对照臂 {pool_only}/10k=0）",
        allocs as f64 / N as f64
    );
    // 放闸排空 ZST 臂（队列容量有限，两臂间必须清场重闸）
    gate.store(true, Ordering::Release);
    wait_until(Duration::from_secs(10), || {
        done.load(Ordering::Relaxed) >= N
    });
    let gate2 = Arc::new(AtomicBool::new(false));
    for _ in 0..2 {
        let g = Arc::clone(&gate2);
        ptp.submit(move |buf: &mut Vec<u8>| {
            while !g.load(Ordering::Acquire) {
                std::hint::spin_loop();
            }
            black_box(buf);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || ptp.threads().metrics().busy == 2);
    // 捕获任务臂：f 捕获一枚 Arc——包装闭包仍携带 mem/计数 Arc，量 f 捕获的增量。
    let done2 = Arc::new(AtomicU64::new(0));
    let before = CountingAllocator::live_allocs();
    for _ in 0..N {
        let d = Arc::clone(&done2);
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    let allocs = CountingAllocator::live_allocs() - before;
    assert_eq!(allocs, N as usize, "捕获任务全链仍恰 1 Box（f 捕获零增量）");
    println!(
        "①全链分配/捕获任务: {allocs} allocs / {N} submits = {:.2}/提交（f 捕获一枚 Arc 零增量——包装闭包本携 Arc 句柄）",
        allocs as f64 / N as f64
    );
    // 放闸清场
    gate2.store(true, Ordering::Release);
    wait_until(Duration::from_secs(10), || {
        done2.load(Ordering::Relaxed) >= N
    });
    ptp.threads().shutdown();
    ptp.threads().join();
}

/// ②组合往返锚：1 worker 空闲，submit→borrow→exec→return 全程批均。
fn dim2_roundtrip() {
    let ptp = combo(1, 1, 32_768, 64, 64, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 64);
    let done = Arc::new(AtomicU64::new(0));
    wait_until(Duration::from_secs(5), || ptp.threads().metrics().idle == 1);
    let n = 20_000u64;
    let t0 = Instant::now();
    for _ in 0..n {
        let d = Arc::clone(&done);
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    while done.load(Ordering::Acquire) < n {
        std::hint::spin_loop();
    }
    let mean = t0.elapsed().as_nanos() as f64 / n as f64;
    println!(
        "②组合往返锚（提交→借→执行→自动还，1 worker）: {mean:.1} ns/op（batch mean over {n}）"
    );
    ptp.threads().shutdown();
    ptp.threads().join();
}

/// ③N 并发提交者吞吐曲线：core=2/max=8（需求性扩容可上台），1/2/4/8 提交者。
fn dim3_throughput() {
    println!("③N 并发提交者吞吐（每臂 100k 任务，core=2 max=8 queue=32k，mem 64 预热）:");
    for submitters in [1usize, 2, 4, 8] {
        let ptp = combo(2, 8, 32_768, 64, 64, BORROW_TIMEOUT);
        prewarm(ptp.mem_pool(), 64);
        let (done, _accepted, refused, _br, elapsed) = storm(&ptp, submitters, 100_000);
        let tps = done as f64 / elapsed.as_secs_f64();
        println!(
            "    submitters={submitters:>2}  completed={done}  tps={:>10.0}  提交拒绝={refused}  借弃={_br}  wall={:.0}ms",
            tps,
            elapsed.as_millis(),
        );
        ptp.threads().shutdown();
        ptp.threads().join();
    }
}

/// ④组合 resize 代价：4a 压场调参调用时延；4b 缩容/越界归还确定性语义锚；
/// 4c 借空 Discard 等价 + shutdown Abort 锚（两独立计数不混账）。
fn dim4_resize_combo() {
    // 4a 压场调参：4 提交者风暴中 200 次交替（tp 缩/扩 + mem 缩/扩各一拍/次）
    let ptp = combo(2, 8, 32_768, 64, 64, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 64);
    let stop = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicU64::new(0));
    std::thread::scope(|s| {
        for _ in 0..4 {
            let (stop, accepted) = (Arc::clone(&stop), Arc::clone(&accepted));
            let ptp = &ptp;
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let _ = ptp.submit(|buf: &mut Vec<u8>| {
                        black_box(buf.is_empty());
                    });
                    accepted.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        std::thread::sleep(Duration::from_millis(50));
        let mut samples: Vec<u64> = Vec::with_capacity(200);
        for i in 0..200u32 {
            let t0 = Instant::now();
            if i % 2 == 0 {
                ptp.threads()
                    .resize(ThreadPoolTuning::new(1, 2, 32_768))
                    .unwrap();
                ptp.mem_pool()
                    .resize(MemoryTuning {
                        max: 8,
                        idle_cap: 8,
                    })
                    .unwrap();
            } else {
                ptp.threads()
                    .resize(ThreadPoolTuning::new(2, 8, 32_768))
                    .unwrap();
                ptp.mem_pool()
                    .resize(MemoryTuning {
                        max: 64,
                        idle_cap: 64,
                    })
                    .unwrap();
            }
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        let mean = samples.iter().sum::<u64>() as f64 / samples.len() as f64;
        samples.sort_unstable();
        println!(
            "④a 组合压场调参（tp+mem 各一拍/次）: mean={mean:.0}ns p99={}ns（4 提交者风暴中 200 次）",
            pct(&samples, 99),
        );
        stop.store(true, Ordering::Relaxed);
    });
    ptp.threads().shutdown();
    ptp.threads().join();

    // 4b 缩容+越界归还联合语义（确定性序列，断言计数账目；queue=512 容下 100 任务突发）
    let ptp = combo(2, 2, 512, 4, 4, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 4);
    let h1 = ptp.mem_pool().acquire(BORROW_TIMEOUT).unwrap();
    let h2 = ptp.mem_pool().acquire(BORROW_TIMEOUT).unwrap();
    ptp.mem_pool()
        .resize(MemoryTuning {
            max: 4,
            idle_cap: 1,
        })
        .unwrap();
    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.idle, 1, "缩容弃超额闲置");
    assert_eq!(m.core.borrowed, 2, "缩容不夺借出");
    drop(h1);
    drop(h2);
    let m = ptp.mem_pool().metrics();
    assert_eq!(m.core.dropped_overflow, 2, "越界归还=溢出丢弃计数");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1);
    // 缩容不丢任务：worker 2→1，100 任务全完成零拒绝
    ptp.threads()
        .resize(ThreadPoolTuning::new(1, 1, 512))
        .unwrap();
    let done = Arc::new(AtomicU64::new(0));
    for _ in 0..100u64 {
        let d = Arc::clone(&done);
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(10), || {
        done.load(Ordering::Relaxed) == 100
    });
    let tm = ptp.threads().metrics();
    assert_eq!(tm.rejected, 0, "缩容窗零提交拒绝");
    ptp.threads().shutdown();
    ptp.threads().join();
    println!(
        "④b 缩容联合语义锚: 弃超额闲置 1 件 / 借出保留 2 件 / 越界归还溢出丢弃 2 件 / 缩容后 100 任务零丢失零拒绝 ✓"
    );

    // 4c 借空=Discard 等价（20ms 借用超时）+ shutdown 后提交=Abort 退还
    let ptp = combo(1, 1, 64, 2, 2, Duration::from_millis(20));
    prewarm(ptp.mem_pool(), 2);
    let held: Vec<_> = (0..2)
        .filter_map(|_| ptp.mem_pool().acquire(Duration::from_millis(50)).ok())
        .collect();
    for _ in 0..5u64 {
        ptp.submit(|buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || ptp.borrow_rejected() == 5);
    let tm = ptp.threads().metrics();
    assert_eq!(tm.rejected, 0, "借空不污染提交侧计数（两独立计数）");
    drop(held);
    let done = Arc::new(AtomicU64::new(0));
    for _ in 0..5u64 {
        let d = Arc::clone(&done);
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || done.load(Ordering::Relaxed) == 5);
    ptp.threads().shutdown();
    let err = ptp
        .submit(|buf: &mut Vec<u8>| {
            black_box(buf);
        })
        .unwrap_err();
    drop(err);
    let tm = ptp.threads().metrics();
    assert_eq!(tm.rejected, 1, "shutdown 后提交=Abort 拒绝计数");
    ptp.threads().join();
    println!(
        "④c 借空/拒绝行为锚: 借空弃件 borrow_rejected=5（提交侧 rejected=0）→ 归还后 5 任务全完成 → shutdown 后提交 Err+rejected=1 ✓（账目恒等式 完成5+借弃5=受理10）"
    );
}

/// ⑤组合延迟分布：submit→完成 e2e 单发采样 p50/p99/max。
fn dim5_latency_dist() {
    let ptp = combo(1, 1, 32_768, 64, 64, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 64);
    let done = Arc::new(AtomicU64::new(0));
    wait_until(Duration::from_secs(5), || ptp.threads().metrics().idle == 1);
    let n = 5000u64;
    let mut samples: Vec<u64> = Vec::with_capacity(n as usize);
    for i in 0..n {
        let d = Arc::clone(&done);
        let t0 = Instant::now();
        ptp.submit(move |buf: &mut Vec<u8>| {
            black_box(buf.is_empty());
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
        while done.load(Ordering::Acquire) != i + 1 {
            std::hint::spin_loop();
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    println!(
        "⑤组合 e2e 延迟（submit→借→执行→还 全程）: p50={}ns p99={}ns max={}ns（n={n}, 逐发采样）",
        pct(&samples, 50),
        pct(&samples, 99),
        samples[samples.len() - 1],
    );
    ptp.threads().shutdown();
    ptp.threads().join();
}

/// ⑥等待者零自旋确认两处：6a 队列等待（worker park 在池调度 condvar）；
/// 6b 借空等待（worker park 在内存池 condvar）。park 判据：µs 级=OS 挂起被唤醒；
/// ns 级即自旋。
fn dim6_zero_spin() {
    // 6a 队列等待：深 park 200ms 后单发任务
    let ptp = combo(1, 1, 64, 8, 8, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 8);
    let warm = Arc::new(AtomicU64::new(0));
    let w = Arc::clone(&warm);
    ptp.submit(move |buf: &mut Vec<u8>| {
        black_box(buf.is_empty());
        w.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    wait_until(Duration::from_secs(5), || warm.load(Ordering::Relaxed) == 1);
    std::thread::sleep(Duration::from_millis(200));
    let done = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let d = Arc::clone(&done);
    ptp.submit(move |buf: &mut Vec<u8>| {
        black_box(buf.is_empty());
        d.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    while done.load(Ordering::Acquire) != 1 {
        std::hint::spin_loop();
    }
    println!(
        "⑥a 队列深park(200ms)后唤醒往返={}µs（park 判据：µs 级=OS 挂起；ns 级即自旋）",
        t0.elapsed().as_micros()
    );
    ptp.threads().shutdown();
    ptp.threads().join();

    // 6b 借空等待：唯一缓冲被持有，worker 在借处 park；持有者 200ms 后 RAII 归还
    let ptp = combo(1, 1, 64, 1, 1, Duration::from_secs(5));
    let holder = ptp.mem_pool().acquire(Duration::from_millis(100)).unwrap();
    let done = Arc::new(AtomicU64::new(0));
    let d = Arc::clone(&done);
    ptp.submit(move |buf: &mut Vec<u8>| {
        black_box(buf.is_empty());
        d.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(200)); // worker 已 park 在内存池 condvar
    let t0 = Instant::now();
    drop(holder); // RAII 归还 → notify_one → worker 借到并完成
    while done.load(Ordering::Acquire) != 1 {
        std::hint::spin_loop();
    }
    println!(
        "⑥b 借空深park(200ms)后归还唤醒至完成={}µs（borrow-park 判据同上）",
        t0.elapsed().as_micros()
    );
    ptp.threads().shutdown();
    ptp.threads().join();
}

/// ⑦组合态伪共享：7a 双池计数隔离（2 线程各撞各池 vs 共撞一池——≈2×=池间零线
/// 干扰，池内衰减=单权威锁竞争）；7b 组合计数热点（借空风暴每任务烧共享
/// borrow_rejected 原子 vs 正常借还稳态）。
fn dim7_false_sharing() {
    // 7a 双池隔离
    let pa = Arc::new(MemoryPool::<Vec<u8>>::new(64, 64, Vec::new));
    let pb = Arc::new(MemoryPool::<Vec<u8>>::new(64, 64, Vec::new));
    prewarm(&pa, 64);
    prewarm(&pb, 64);
    let hammer = |pool: Arc<MemoryPool<Vec<u8>>>, stop: Arc<AtomicBool>| -> u64 {
        let mut ops = 0u64;
        while !stop.load(Ordering::Relaxed) {
            if let Ok(g) = pool.acquire(Duration::from_millis(10)) {
                black_box(g.is_empty());
                drop(g);
                ops += 1;
            }
        }
        ops
    };
    let run2 = |a: Arc<MemoryPool<Vec<u8>>>, b: Arc<MemoryPool<Vec<u8>>>| -> u64 {
        let stop = Arc::new(AtomicBool::new(false));
        let (s1, s2) = (Arc::clone(&stop), Arc::clone(&stop));
        let h1 = std::thread::spawn(move || hammer(a, s1));
        let h2 = std::thread::spawn(move || hammer(b, s2));
        std::thread::sleep(Duration::from_millis(400));
        stop.store(true, Ordering::Relaxed);
        (h1.join().unwrap() + h2.join().unwrap()) * 1_000 / 400
    };
    let dual = run2(Arc::clone(&pa), Arc::clone(&pb));
    let shared = run2(Arc::clone(&pa), Arc::clone(&pa));
    println!(
        "⑦a 双池计数隔离: 各撞各池={dual} ops/s vs 共撞一池={shared} ops/s ratio={:.2}x（≈2x=池间零线干扰；衰减部分=池内单权威锁竞争，见 MemoryPool ③）",
        dual as f64 / shared.max(1) as f64
    );

    // 7b 组合计数热点：借空风暴（borrow_timeout=ZERO 非阻塞尝试，每任务烧共享
    // borrow_rejected 原子）vs 正常借还稳态（该原子零触碰=冷路径设计）。
    let ptp = combo(4, 4, 32_768, 4, 4, Duration::ZERO);
    let held: Vec<_> = (0..4)
        .filter_map(|_| ptp.mem_pool().acquire(Duration::from_millis(50)).ok())
        .collect();
    let (_d, accepted, refused, br, elapsed) = storm(&ptp, 4, 40_000);
    assert_eq!(refused, 0);
    assert_eq!(br, accepted, "借空风暴：每受理任务恰一次 borrow_rejected");
    let storm_tps = br as f64 / elapsed.as_secs_f64();
    drop(held);
    ptp.threads().shutdown();
    ptp.threads().join();

    let ptp = combo(4, 4, 32_768, 64, 64, BORROW_TIMEOUT);
    prewarm(ptp.mem_pool(), 64);
    let (done, _accepted, refused, br, elapsed) = storm(&ptp, 4, 40_000);
    assert_eq!(refused, 0);
    assert_eq!(br, 0, "正常借还稳态零 borrow_rejected 触碰");
    let normal_tps = done as f64 / elapsed.as_secs_f64();
    ptp.threads().shutdown();
    ptp.threads().join();
    println!(
        "⑦b 组合计数热点: 借空风暴（每任务烧共享原子）={storm_tps:.0} /s vs 正常借还（原子零触碰）={normal_tps:.0} /s",
    );
    println!("    （设计注: borrow_rejected 为弃件路径冷计数——稳态借还零共享原子写，热点只在持续借空故障态）");

    let m = ptp.mem_pool().metrics();
    println!(
        "final sanity: borrowed={} (expect 0) idle={} dropped_overflow={} borrow_rejected={}",
        m.core.borrowed,
        m.core.idle,
        m.core.dropped_overflow,
        ptp.borrow_rejected(),
    );
}
