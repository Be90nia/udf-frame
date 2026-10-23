//! 线程池七维性能矩阵采集器（拍板 2026-10-16：多维拉满，multidim 模式纯 std）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example thread_pool_multidim --features pool,memgov
//! ```
//!
//! 七维对表（派单契约）：
//! ①提交热路径分配（ZST 任务=0 / 捕获任务=1 Box，类型擦除不可避免如实报数）
//! ②uncontended 提交→完成往返锚 ③竞争 1/2/4/8 worker 吞吐曲线（饱和不倒退）
//! ④resize 生效延迟（扩容上线/缩容令牌收口）⑤提交延迟分布 p50/p99
//! ⑥零自旋（park 后唤醒延迟留痕——自旋者呈现 ns 级往返且 CPU 忙，park 呈 µs 级）
//! ⑦伪共享防（单权威锁设计=计数器不跨线程原子弹跳；审计开/关吞吐差为代理留痕）
//!
//! 口径注：
//! ① CountingAllocator 常驻（内存维前提），计时含每分配 2 次 Relaxed 原子加噪声；
//! ② Instant 时钟对开销（~2×20ns）双臂同担；粗测 std 分位数非 criterion——数字供判级；
//! ③ 并发段只测吞吐不计内存（计数器仅串行上下文有效）；池逐段 shutdown+join 清场。

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton::pool::PoolAuditEvent;
use skeleton_infra::memgov::CountingAllocator;
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn tuning(core: usize, max: usize, queue_cap: usize) -> ThreadPoolTuning {
    ThreadPoolTuning::new(core, max, queue_cap)
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

fn collect_samples(mut op: impl FnMut() -> u64, n: usize, v: &mut Vec<u64>) {
    v.clear();
    for _ in 0..n {
        v.push(op());
    }
    v.sort_unstable();
}

/// 轮询至条件成立（bench 清场用，禁裸 sleep 定时）。
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

/// CPU 密集负载（~5µs：工作远大于锁流量，饱和曲线的诚实口径）。
fn burn() {
    for i in 0u64..16384 {
        black_box(i.wrapping_mul(0x9E37_79B9_7F4A_7C15));
    }
}

fn main() {
    println!("== thread_pool 七维矩阵（release, 纯 std）==\n");
    dim1_submit_allocs();
    dim2_uncontended_roundtrip();
    dim3_contention_curve();
    dim4_resize_latency();
    dim5_submit_distribution();
    dim6_zero_spin();
    dim7_false_sharing();
    println!("\n== 采集完毕 ==");
}

/// ①提交热路径分配：ZST 任务=0 分配；捕获任务=恰 1 Box（类型擦除）。
fn dim1_submit_allocs() {
    // ZST 任务：闭包无捕获 → Box<ZST> 悬垂指针不进分配器。cap 须容下整批突发。
    {
        let pool = ThreadPool::new(tuning(1, 1, 32_768));
        for _ in 0..1000 {
            pool.execute(|| {}).unwrap();
        }
        wait_until(Duration::from_secs(5), || pool.metrics().completed >= 1000);
        let before = CountingAllocator::live_allocs();
        let n = 20_000;
        for _ in 0..n {
            pool.execute(|| {}).unwrap();
        }
        wait_until(Duration::from_secs(5), || {
            pool.metrics().completed as usize >= 21_000
        });
        let allocs = CountingAllocator::live_allocs() - before;
        println!(
            "①提交分配/ZST任务: {} allocs / {} submits = {:.2}/提交（稳态零分配）",
            allocs,
            n,
            allocs as f64 / n as f64
        );
        pool.shutdown();
        pool.join();
    }
    // 捕获任务：闸住 worker 后整批入队（零释放窗口），live 差=净增 Box 数。
    {
        let pool = ThreadPool::new(tuning(1, 1, 32_768));
        let (gate, release) = {
            let open = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
            let g = open.clone();
            let rel = move || {
                let (m, c) = &*open;
                *m.lock().unwrap() = true;
                c.notify_all();
            };
            (g, rel)
        };
        {
            let open = gate;
            pool.execute(move || {
                let (m, c) = &*open;
                let mut o = m.lock().unwrap();
                while !*o {
                    o = c.wait(o).unwrap();
                }
            })
            .unwrap();
        }
        wait_until(Duration::from_secs(5), || pool.metrics().busy == 1);
        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..1000 {
            let d = Arc::clone(&done);
            pool.execute(move || {
                d.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }
        let before = CountingAllocator::live_allocs();
        let n = 20_000;
        for _ in 0..n {
            let d = Arc::clone(&done);
            pool.execute(move || {
                d.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }
        let allocs = CountingAllocator::live_allocs() - before;
        release();
        wait_until(Duration::from_secs(5), || {
            done.load(Ordering::Relaxed) == 21_000
        });
        println!(
            "①提交分配/捕获任务: {} allocs / {} submits = {:.2}/提交（恰 1 Box=类型擦除下限，闸门隔离释放窗）",
            allocs,
            n,
            allocs as f64 / n as f64
        );
        pool.shutdown();
        pool.join();
    }
}

/// ②uncontended 提交→执行完成往返（1 worker 空闲，spin-wait 完成）。
fn dim2_uncontended_roundtrip() {
    let pool = ThreadPool::new(tuning(1, 1, 4096));
    let done = Arc::new(AtomicU64::new(0));
    let n = 3000;
    let mut samples: Vec<u64> = Vec::with_capacity(n);
    // 预热。
    for _ in 0..200 {
        let d = Arc::clone(&done);
        pool.execute(move || {
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || {
        done.load(Ordering::Relaxed) == 200
    });
    let mut expected = done.load(Ordering::Acquire);
    for _ in 0..n {
        let d = Arc::clone(&done);
        let t0 = Instant::now();
        pool.execute(move || {
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
        expected += 1;
        while done.load(Ordering::Acquire) < expected {
            std::hint::spin_loop();
        }
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    let mean = samples.iter().sum::<u64>() / samples.len() as u64;
    println!(
        "②uncontended往返: mean={}ns p50={}ns p99={}ns（n={}）",
        mean,
        pct(&samples, 50),
        pct(&samples, 99),
        n
    );
    pool.shutdown();
    pool.join();
}

/// ③竞争吞吐曲线：W worker × W 提交线程，CPU 密集任务，饱和不倒退。
/// 口径：耗时含提交+排空全程（提交后等待 queue_len==0 && busy==0）；任务零捕获
/// （fn 指针 ZST，零分配零共享计数——被测对象是池不是基准自身的伪共享热点）。
fn dim3_contention_curve() {
    println!("③竞争吞吐（burn≈5µs/任务，含排空全程）:");
    let mut prev = 0.0;
    for w in [1usize, 2, 4, 8] {
        let pool = Arc::new(ThreadPool::new(tuning(w, w, 8192)));
        let per = 10_000usize;
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..w {
                let p = Arc::clone(&pool);
                s.spawn(move || {
                    for _ in 0..per {
                        // 拒绝重试=背压（任务取回再投，零丢失）。
                        let mut retry: Option<skeleton_infra::pool::Task> = None;
                        loop {
                            let t: skeleton_infra::pool::Task =
                                retry.take().unwrap_or_else(|| Box::new(burn));
                            match p.execute(t) {
                                Ok(()) => break,
                                Err(rej) => {
                                    retry = Some(rej.into_task());
                                    std::hint::spin_loop();
                                }
                            }
                        }
                    }
                });
            }
        });
        let ok = wait_until(Duration::from_secs(60), || {
            let m = pool.metrics();
            m.queue_len == 0 && m.busy == 0
        });
        let secs = t0.elapsed().as_secs_f64();
        let total = (w * per) as f64;
        let tps = total / secs;
        let verdict = if w == 1 {
            "基线"
        } else if tps >= prev * 0.95 {
            "饱和不倒退"
        } else {
            "⚠倒退>5%"
        };
        println!(
            "   workers={w}: {} tasks/s（{:.3}s, 提交器={w}）[{verdict}]{}",
            format_num(tps),
            secs,
            if ok { "" } else { " ⚠未在期限内收齐" }
        );
        prev = tps;
        pool.shutdown();
        pool.join();
    }
}

fn format_num(v: f64) -> String {
    let k = v / 1000.0;
    format!("{k:.0}K")
}

/// ④resize 生效延迟：扩容（补建 spawn 计入调用内）/ 缩容（令牌消费=worker 退）。
fn dim4_resize_latency() {
    let pool = ThreadPool::new(tuning(1, 8, 1024));
    wait_until(Duration::from_secs(5), || pool.metrics().idle == 1);
    // 扩容：调用返回即补建到位（workers 计数同步生效）。
    let t0 = Instant::now();
    pool.resize(tuning(4, 8, 1024)).unwrap();
    let grow_call = t0.elapsed();
    assert_eq!(pool.metrics().workers, 4);
    wait_until(Duration::from_secs(5), || pool.metrics().idle == 4);
    // 缩容：从调用返回到差额 worker 全退（令牌消费完毕）。
    let t1 = Instant::now();
    pool.resize(tuning(1, 8, 1024)).unwrap();
    let shrink_ok = wait_until(Duration::from_secs(5), || pool.metrics().workers == 1);
    let shrink = t1.elapsed();
    println!(
        "④resize延迟: 扩容调用={}µs（返回即 workers=4）/ 缩容令牌收口={}µs（workers→1, {}）",
        grow_call.as_micros(),
        shrink.as_micros(),
        if shrink_ok { "ok" } else { "⚠超时" }
    );
    pool.shutdown();
    pool.join();
}

/// ⑤提交延迟分布：空闲池 enqueue+notify 单发采样。
fn dim5_submit_distribution() {
    let pool = ThreadPool::new(tuning(1, 1, 16384));
    let done = Arc::new(AtomicU64::new(0));
    wait_until(Duration::from_secs(5), || pool.metrics().idle == 1);
    let n = 5000;
    let mut samples: Vec<u64> = Vec::with_capacity(n);
    collect_samples(
        || {
            let d = Arc::clone(&done);
            let t0 = Instant::now();
            pool.execute(move || {
                d.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
            t0.elapsed().as_nanos() as u64
        },
        n,
        &mut samples,
    );
    wait_until(Duration::from_secs(5), || {
        done.load(Ordering::Relaxed) == 5000
    });
    println!(
        "⑤提交延迟: p50={}ns p99={}ns max={}ns（n={n}, enqueue+notify 不等完成）",
        pct(&samples, 50),
        pct(&samples, 99),
        samples[samples.len() - 1]
    );
    pool.shutdown();
    pool.join();
}

/// ⑥零自旋留痕：park 后唤醒延迟——park 语义下应呈 µs 级（自旋则 ns 级且 CPU 忙）。
/// 结构性证据：worker_loop 队空即 Condvar::wait（OS 挂起），全文件无忙等循环。
fn dim6_zero_spin() {
    let pool = ThreadPool::new(tuning(1, 1, 64));
    let done = Arc::new(AtomicU64::new(0));
    for _ in 0..10 {
        let d = Arc::clone(&done);
        pool.execute(move || {
            d.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    wait_until(Duration::from_secs(5), || {
        done.load(Ordering::Relaxed) == 10
    });
    // 深度 park：静置 200ms 后单发任务，测 唤醒+完成 往返。
    std::thread::sleep(Duration::from_millis(200));
    let t0 = Instant::now();
    let d = Arc::clone(&done);
    pool.execute(move || {
        d.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    let target = 11;
    while done.load(Ordering::Acquire) != target {
        std::hint::spin_loop();
    }
    let wake = t0.elapsed();
    println!(
        "⑥零自旋: 深park(200ms)后唤醒往返={}µs（park 判据：µs 级=OS 挂起被唤醒；ns 级即自旋）",
        wake.as_micros()
    );
    pool.shutdown();
    pool.join();
}

/// ⑦伪共享防代理：审计开/关热路径吞吐差（关=一次 gate load 快路径）。
/// 设计面：单权威锁内计数器不形成跨线程原子弹跳，无可填充热点—— CachePad
/// 不适用（兄弟件 memgov 的原子计数器形态），留痕=审计开销差+设计注。
fn dim7_false_sharing() {
    let run = |audit: bool| -> f64 {
        // cap 容下全量突发（100k），两臂同条件对齐——只比审计开关差。
        let pool = Arc::new(ThreadPool::new(tuning(4, 4, 131_072)));
        let _ticket = if audit {
            Some(pool.subscribe_audit(|_: &PoolAuditEvent| {}))
        } else {
            None
        };
        let done = Arc::new(AtomicU64::new(0));
        let n = 100_000u64;
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for _ in 0..4 {
                let p = Arc::clone(&pool);
                let d = Arc::clone(&done);
                s.spawn(move || {
                    for _ in 0..n / 4 {
                        let d = Arc::clone(&d);
                        p.execute(move || {
                            d.fetch_add(1, Ordering::Relaxed);
                        })
                        .unwrap();
                    }
                });
            }
        });
        wait_until(Duration::from_secs(30), || {
            done.load(Ordering::Relaxed) >= n
        });
        let tps = n as f64 / t0.elapsed().as_secs_f64();
        pool.shutdown();
        pool.join();
        tps
    };
    // 交替三轮取中位（消跑序偏热——首轮冷缓存会伪装成「审计更快」）。
    let mut offs: Vec<u64> = Vec::new();
    let mut ons: Vec<u64> = Vec::new();
    for round in 0..3 {
        if round % 2 == 0 {
            offs.push(run(false) as u64);
            ons.push(run(true) as u64);
        } else {
            ons.push(run(true) as u64);
            offs.push(run(false) as u64);
        }
    }
    offs.sort_unstable();
    ons.sort_unstable();
    let off = offs[1] as f64;
    let on = ons[1] as f64;
    println!(
        "⑦审计关（gate load 快路径）: {} tasks/s（三轮中位）",
        format_num(off)
    );
    println!(
        "⑦审计开（环+回调在环）: {} tasks/s（三轮中位）",
        format_num(on)
    );
    println!(
        "⑦审计开销差: {:+.1}%（设计注: 热路径零跨线程原子写；计数器全在单权威锁内=无伪共享面，无可填充热点）",
        (on - off) / off * 100.0
    );
}
