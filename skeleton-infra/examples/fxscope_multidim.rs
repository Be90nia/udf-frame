//! FxScope 多维性能矩阵采集器（fx 域性能锚补齐批 A-2；沿 thread_pool_multidim/
//! memory_pool_profile 模式纯 std 计时，零新依赖）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example fxscope_multidim --features tokio,memgov
//! ```
//!
//! 采集面对表（派单契约，七维映射 ①②③⑤⑦ + 停机面；④ n/a=FxScope 无 resize
//! 语义，⑥ 缺维未采）：
//! ①稳态分配：spawn 热路径未执行净增（current_thread 隔离执行窗）/捕获任务单列/
//!   并发闸 spawn 侧增量/闸排队态等待者足迹/排空后 live 回基线
//! ③并发 spawn 曲线：1/2/4/8 提交线程吞吐（无闸 vs with_concurrency 同并发度；
//!   提交者=OS 线程经 handle.block_on，执行者=8 tokio worker——争用面=scope 共享态）
//! ②严格往返锚：current_thread 单线程 uncontended 单发 spawn→完成（每样本任务体
//!   done 正信号排空后才进下一样本=排空窗口隔离；与⑤的 8-worker 连发采样口径区分）
//! ⑤延迟分布：spawn→执行完成单发采样 p50/p99/max（对照 criterion fxscope_spawn_join
//!   0.95µs——彼含每迭代 block_on 进出，本采集器 block_on 常驻，口径更净）
//! ⑦伪共享对照（严格口径=隔离增益规范测法）：in_flight 形计数器 adjacent vs
//!   align(64) 垫片（2 线程各 5M 自增）+计数器/闸 CAS 混合对撞；屏障同步起跑+
//!   双线程各自计时取较慢者+3 轮交替跑序取中位（memory_pool_profile ⑦布局的升级件）
//! 停机面：abort 响应/协作取消传播/accepting 闸单发/shutdown 排空已完结/shutdown abort 在途/shutdown_timeout 守卫触发
//!
//! 口径注（沿兄弟件惯例）：
//! ① CountingAllocator 常驻本二进制（内存维前提），计时含每分配 2 次 Relaxed 原子加
//!    噪声（ns 量级）；
//! ② 粗测 std Instant 非 criterion——数字供判级留痕不供微优化基线；tokio 多线程调度
//!    噪声敏感，结论看量级与曲线形状；
//! ③ 分配段用 current_thread 单线程隔离执行/释放窗（spawn 后不让出，任务永不执行，
//!    净增=spawn 调用面真实分配）；闸排队态例外（须真执行到 acquire），就绪判据=
//!    live 读数稳定；
//! ④ 「已到达挂起点」判据一律用任务体 mpsc 正信号（in_flight 在 spawn 调用内同步
//!    置位，不能作「已被 poll」证据）；black_box 防消除；时钟地板双臂同担。

use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton_infra::fx::{cancel_pair, FxScope, TaskError};
use skeleton_infra::memgov::CountingAllocator;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// 轮询至条件成立（清场用，禁裸 sleep 定时）。
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

/// live 分配读数连续 `reads` 次不变 = 排队态到位（闸排队臂专用粗判据）。
fn live_stable(reads: usize) {
    let mut last = CountingAllocator::live_allocs();
    let mut same = 0;
    while same < reads {
        std::thread::sleep(Duration::from_micros(200));
        let cur = CountingAllocator::live_allocs();
        if cur == last {
            same += 1;
        } else {
            same = 0;
            last = cur;
        }
    }
}

fn multi_rt(workers: usize) -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime")
}

fn main() {
    println!("== FxScope 多维矩阵（release, 纯 std 计时 + 8 worker 多线程运行时）==\n");
    dim1_allocs();
    dim2_latency_distribution();
    dim2b_uncontended_roundtrip();
    let rt = multi_rt(8);
    dim3_spawn_curve(rt.handle());
    drop(rt);
    dim7_false_sharing();
    dim5_shutdown();
    println!("\n== 采集完毕 ==");
    println!(
        "终检: live_allocs={}（采集器自身稳态残存，含全部运行时已 drop 后的测量面残留定性）",
        CountingAllocator::live_allocs()
    );
}

/// ①稳态分配：spawn 热路径分配足迹。
fn dim1_allocs() {
    // a) ZST 任务无闸：oneshot 通道 + JoinSet 任务分配，类型擦除形态如实报数。
    let zst = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let n = 2000usize;
        let delta = rt.block_on(async {
            let scope = FxScope::new();
            for _ in 0..100 {
                scope.spawn(async {}).unwrap();
            }
            let before = CountingAllocator::live_allocs();
            for _ in 0..n {
                scope.spawn(async {}).unwrap();
            }
            CountingAllocator::live_allocs() - before
        });
        delta as f64 / n as f64
    };
    println!(
        "①a spawn净增/ZST任务: {zst:.2} allocs/任务（oneshot+JoinSet 类型擦除形态，未执行隔离窗）"
    );

    // b) 捕获任务（Arc+u64）：对比 a 判定捕获是否内联进任务分配（无独立 Box）。
    let captured = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let n = 2000usize;
        let done = Arc::new(AtomicU64::new(0));
        let delta = rt.block_on(async {
            let scope = FxScope::new();
            for _ in 0..100 {
                let dc = Arc::clone(&done);
                scope
                    .spawn(async move {
                        dc.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
            let before = CountingAllocator::live_allocs();
            for _ in 0..n {
                let dc = Arc::clone(&done);
                scope
                    .spawn(async move {
                        dc.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
            CountingAllocator::live_allocs() - before
        });
        delta as f64 / n as f64
    };
    let verdict = if captured > zst + 0.5 {
        "捕获含独立 Box"
    } else {
        "捕获无独立 Box（future 内联任务分配）"
    };
    println!("①b spawn净增/捕获任务: {captured:.2} allocs/任务（{verdict}）");

    // c) 并发闸 spawn 侧：with_concurrency(8) 同法——闸挂载后 spawn 调用面增量
    //    （acquire 在任务体内，未执行=只测 spawn 面；Semaphore 构建不在 spawn 路）。
    let gated_spawn = {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("rt");
        let n = 2000usize;
        let delta = rt.block_on(async {
            let scope = FxScope::new().with_concurrency(8);
            for _ in 0..100 {
                scope.spawn(async {}).unwrap();
            }
            let before = CountingAllocator::live_allocs();
            for _ in 0..n {
                scope.spawn(async {}).unwrap();
            }
            CountingAllocator::live_allocs() - before
        });
        delta as f64 / n as f64
    };
    println!(
        "①c 闸spawn侧/ZST任务: {gated_spawn:.2} allocs/任务（增量 {:+.2} vs ①a；目标=0 即持平）",
        gated_spawn - zst
    );

    // d) 闸排队态：cap=1 持闸者先占满 permit（mpsc 正信号=已被 poll=permit 已持），
    //    2000 任务全部排队于 acquire——每任务总足迹-①a=等待者注册面增量。
    let (queued, drain_ok) = {
        let rt = multi_rt(4);
        let n = 2000u64;
        let done = Arc::new(AtomicU64::new(0));
        let mut ok = false;
        let footprint = rt.block_on(async {
            let scope = Arc::new(FxScope::new().with_concurrency(1));
            let notify = Arc::new(tokio::sync::Notify::new());
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let holder = scope
                .spawn({
                    let notify = Arc::clone(&notify);
                    async move {
                        tx.send(()).unwrap(); // 已被 poll ⇒ permit 已在其手
                        notify.notified().await;
                    }
                })
                .unwrap();
            rx.recv().expect("holder parked");
            let before = CountingAllocator::live_allocs();
            for _ in 0..n {
                let d = Arc::clone(&done);
                scope
                    .spawn(async move {
                        d.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
            live_stable(50); // 等全部到达 acquire 挂起态
            let per = (CountingAllocator::live_allocs() - before) as f64 / n as f64;
            notify.notify_waiters(); // 放行：permit 逐个流转
            drop(holder);
            // 先排空后 shutdown——shutdown=close+abort_all，会中止仍未跑的排队任务。
            for _ in 0..4_000_000 {
                if done.load(Ordering::Relaxed) >= n {
                    ok = true;
                    break;
                }
                tokio::task::yield_now().await;
            }
            let _ = scope.shutdown().await;
            per
        });
        (footprint, ok)
    };
    let inc = (queued - zst).abs();
    println!(
        "①d 闸排队态/ZST任务: {queued:.2} allocs/任务（|增量| {inc:.2} vs ①a；0=等待者注册零分配（内联任务分配形态）；排空{}）",
        if drain_ok { "ok" } else { "⚠超时" }
    );

    // e) 稳态回基线：5000 即完结任务全排空后 live 回落（无泄漏定性）。
    // 双 scope：预热批先 shutdown 收割（完结任务条目要到 join_next 才释放，单 scope
    // 会把预热批 100 条记录算进基线 → 排空后假负 Δ）。
    {
        let rt = multi_rt(4);
        let mut baseline = 0usize;
        rt.block_on(async {
            let done = Arc::new(AtomicU64::new(0));
            // 预热 scope：跑+收割一整轮，把运行时惰性内部件也建齐。
            {
                let warm = Arc::new(FxScope::new());
                for _ in 0..100 {
                    let d = Arc::clone(&done);
                    warm.spawn(async move {
                        d.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
                }
                wait_until(Duration::from_secs(5), || {
                    done.load(Ordering::Relaxed) >= 100
                });
                let _ = warm.shutdown().await;
            }
            live_stable(30); // 释放落账后再取基线
            baseline = CountingAllocator::live_allocs();
            let scope = Arc::new(FxScope::new());
            let n = 5000u64;
            for _ in 0..n {
                let d = Arc::clone(&done);
                scope
                    .spawn(async move {
                        d.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
            }
            wait_until(Duration::from_secs(10), || {
                done.load(Ordering::Relaxed) >= 100 + n
            });
            let _ = scope.shutdown().await;
        });
        std::thread::sleep(Duration::from_millis(100)); // worker 侧异步释放落账
        let live = CountingAllocator::live_allocs();
        println!(
            "①e 排空后live: {live} allocs（预热后基线 {baseline}，Δ={}；≈0=无泄漏，正小值=运行时内部残差）",
            live as i64 - baseline as i64
        );
    }
}

/// ⑤延迟分布：spawn→执行完成单发采样（任务体置 done 旗=「执行完成」判据；
/// block_on 主线程 spin 旗不占 worker——与 criterion 含 block_on 进出口径区分）。
fn dim2_latency_distribution() {
    let rt = multi_rt(8);
    rt.block_on(async {
        let scope = Arc::new(FxScope::new());
        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..200 {
            let d = Arc::clone(&done);
            scope
                .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
                .unwrap();
        }
        let mut expected = done.load(Ordering::Acquire);
        wait_until(Duration::from_secs(5), || {
            done.load(Ordering::Acquire) >= expected
        });
        let n = 5000usize;
        let mut samples: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            let d = Arc::clone(&done);
            let t0 = Instant::now();
            scope
                .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
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
            "⑤延迟分布 spawn→完成: mean={mean}ns p50={}ns p99={}ns max={}ns（n={n}, 无闸无捕获; 对照 criterion 0.95µs=含 block_on 进出）",
            pct(&samples, 50),
            pct(&samples, 99),
            samples[samples.len() - 1]
        );
        let _ = scope.shutdown().await;
    });
}

/// ②严格口径 uncontended 往返锚：current_thread 单线程运行时（提交者与执行者同
/// 线程，结构性零跨 worker 争用），单发 spawn→完成采样；每样本等任务体 done 正
/// 信号（已执行完）后才进下一样本=排空窗口隔离。让出用 yield_now 轮转（阻塞式
/// spin 会饿死单线程执行者）——与⑤的 8-worker 连发采样口径严格区分。
fn dim2b_uncontended_roundtrip() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("rt");
    rt.block_on(async {
        let scope = Arc::new(FxScope::new());
        let done = Arc::new(AtomicU64::new(0));
        for _ in 0..200 {
            let d = Arc::clone(&done);
            scope
                .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
                .unwrap();
        }
        let mut expected = done.load(Ordering::Acquire);
        while done.load(Ordering::Acquire) < expected {
            tokio::task::yield_now().await;
        }
        let n = 3000usize;
        let mut samples: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            let d = Arc::clone(&done);
            let t0 = Instant::now();
            scope
                .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
                .unwrap();
            expected += 1;
            while done.load(Ordering::Acquire) < expected {
                tokio::task::yield_now().await;
            }
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        samples.sort_unstable();
        let mean = samples.iter().sum::<u64>() / samples.len() as u64;
        println!(
            "②严格uncontended往返: mean={mean}ns p50={}ns p99={}ns max={}ns（n={n}, current_thread 单发每样本排空; 对照⑤=8-worker 连发分布口径）",
            pct(&samples, 50),
            pct(&samples, 99),
            samples[samples.len() - 1]
        );
        let _ = scope.shutdown().await;
    });
}

/// ③并发 spawn 曲线：c 提交线程 × 10k 任务/线程，吞吐含全程（提交+执行+done 收齐）。
/// 两臂：无闸（纯共享态争用面）/ with_concurrency(c)（permit 流转）。
/// 每点 3 连跑取中位；前置冷启动预热臂（首跑受 worker 冷启动污染，旧版实测 ~3× 偏慢）。
fn dim3_spawn_curve(handle: &tokio::runtime::Handle) {
    println!("③并发spawn曲线（10k 任务/提交线程, 含执行+排空全程, 每点3连跑取中位）:");
    let per = 10_000usize;
    // 冷启动预热：worker 刚建、首次重载（建任务存储/唤醒路径）偏慢，不入表。
    {
        let scope = Arc::new(FxScope::new());
        let done = Arc::new(AtomicU64::new(0));
        handle.block_on(async {
            for _ in 0..2000 {
                let d = Arc::clone(&done);
                scope
                    .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
                    .unwrap();
            }
            let mut spins = 0u64;
            while done.load(Ordering::Relaxed) < 2000 {
                tokio::task::yield_now().await;
                spins += 1;
                if spins > 200_000_000 {
                    break;
                }
            }
            let _ = scope.shutdown().await;
        });
    }
    for gated in [false, true] {
        for c in [1usize, 2, 4, 8] {
            let mut runs: Vec<f64> = Vec::with_capacity(3);
            for _ in 0..3 {
                let scope = Arc::new(if gated {
                    FxScope::new().with_concurrency(c)
                } else {
                    FxScope::new()
                });
                let done = Arc::new(AtomicU64::new(0));
                let total = (c * per) as u64;
                let t0 = Instant::now();
                std::thread::scope(|s| {
                    for _ in 0..c {
                        let (h, scope, done) =
                            (handle.clone(), Arc::clone(&scope), Arc::clone(&done));
                        s.spawn(move || {
                            h.block_on(async {
                                for _ in 0..per {
                                    let d = Arc::clone(&done);
                                    scope
                                        .spawn(async move {
                                            black_box(d.fetch_add(1, Ordering::Relaxed))
                                        })
                                        .unwrap();
                                }
                            });
                        });
                    }
                });
                // 收齐判据：done 达 total（提交线程已全退出，执行面在 worker 上继续）。
                let mut spins = 0u64;
                while done.load(Ordering::Acquire) < total {
                    std::hint::spin_loop();
                    spins += 1;
                    if spins > 8_000_000_000 {
                        break;
                    }
                }
                let secs = t0.elapsed().as_secs_f64();
                runs.push(total as f64 / secs);
                // 清场：收割完结条目（JoinSet 无限增长防）。
                let scope2 = Arc::clone(&scope);
                handle.block_on(async move {
                    let _ = scope2.shutdown().await;
                });
            }
            runs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            println!(
                "   {}c={c}: {} tasks/s（3连跑 {:.0}K/{:.0}K/{:.0}K）",
                if gated { "闸" } else { "无闸" },
                format_num(runs[1]),
                runs[0] / 1000.0,
                runs[1] / 1000.0,
                runs[2] / 1000.0
            );
        }
    }
}

fn format_num(v: f64) -> String {
    if v >= 1_000_000.0 {
        format!("{:.2}M", v / 1_000_000.0)
    } else {
        format!("{:.0}K", v / 1000.0)
    }
}

/// ⑦伪共享对照（严格口径=隔离增益的规范测法）：真实争用面对 =
/// in_flight 计数器（fetch_add）与 accepting 闸（CAS）——单结构邻接布局下热词
/// 是否同线弹跳的机制锚。adjacent vs align(64) 垫片隔离，2 线程各 5M 次。
/// 严格化三件（升级自 memory_pool_profile ⑦ 的墙钟摊账法）：屏障同步起跑
/// （spawn 延迟/启动偏斜出计时窗）、双线程各自计时取较慢者摊 ns/op（隔离增益
/// 判据=较慢线程加速比，不被快线程空闲稀释）、3 轮交替跑序取中位（消跑序偏热）。
fn dim7_false_sharing() {
    mod share {
        use std::sync::atomic::AtomicU64;
        // repr(C)：被测机制就是字段邻接/垫片隔离，布局必须按声明落。
        #[repr(C)]
        pub struct Adjacent(pub AtomicU64, pub AtomicU64);
        #[repr(align(64))]
        pub struct Pad(pub ());
        #[repr(C)]
        pub struct Isolated(pub AtomicU64, pub Pad, pub AtomicU64, pub Pad);
    }
    let iters = 5_000_000u64;
    let rounds = 3usize;
    /// 三轮中位（跑序已交替，中位再压单轮调度噪声）。
    fn median3(mut v: Vec<f64>) -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    }
    /// 严格口径单轮对撞：屏障同步起跑后双线程各跑各的访问循环并各自计时，
    /// 取较慢线程摊 ns/op（thread::scope 免 Arc——线程借用手柄内状态）。
    fn bounce_strict<R>(iters: u64, state: &R, fa: impl Fn(&R) + Send, fb: impl Fn(&R)) -> f64
    where
        R: Sync,
    {
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|s| {
            let other = s.spawn({
                let b = Arc::clone(&barrier);
                move || {
                    b.wait();
                    let t0 = Instant::now();
                    for _ in 0..iters {
                        fa(state);
                    }
                    t0.elapsed()
                }
            });
            barrier.wait();
            let t0 = Instant::now();
            for _ in 0..iters {
                fb(state);
            }
            let mine = t0.elapsed();
            mine.max(other.join().unwrap()).as_nanos() as f64 / iters as f64
        })
    }

    // ⑦a 纯计数器对撞（in_flight 形，Relaxed fetch_add）。
    // 3 轮交替跑序：偶轮先邻接后隔离，奇轮反序（thread_pool_multidim ⑦先例）。
    let (adj_c, iso_c) = {
        let mut ar = Vec::with_capacity(rounds);
        let mut ir = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let adj = share::Adjacent(AtomicU64::new(0), AtomicU64::new(0));
            let iso = share::Isolated(
                AtomicU64::new(0),
                share::Pad(()),
                AtomicU64::new(0),
                share::Pad(()),
            );
            let run_adj = || {
                bounce_strict(
                    iters,
                    &adj,
                    |s: &share::Adjacent| {
                        s.0.fetch_add(1, Ordering::Relaxed);
                    },
                    |s: &share::Adjacent| {
                        s.1.fetch_add(1, Ordering::Relaxed);
                    },
                )
            };
            let run_iso = || {
                bounce_strict(
                    iters,
                    &iso,
                    |s: &share::Isolated| {
                        s.0.fetch_add(1, Ordering::Relaxed);
                    },
                    |s: &share::Isolated| {
                        s.2.fetch_add(1, Ordering::Relaxed);
                    },
                )
            };
            let (a, i) = if round % 2 == 0 {
                (run_adj(), run_iso())
            } else {
                (run_iso(), run_adj())
            };
            ar.push(a);
            ir.push(i);
        }
        (median3(ar), median3(ir))
    };
    println!(
        "⑦a 计数器对撞（2线程×5M fetch_add, 屏障同步起跑, 3轮交替中位）: adjacent={adj_c:.1} ns/op  isolated(pad)={iso_c:.1} ns/op  隔离增益={:.2}×",
        adj_c / iso_c
    );

    // ⑦b 计数器×闸 CAS 混合对撞（spawn 路真实访问对：fetch_add(in_flight) ‖ CAS(accepting)）。
    let (adj_m, iso_m) = {
        let mut ar = Vec::with_capacity(rounds);
        let mut ir = Vec::with_capacity(rounds);
        for round in 0..rounds {
            let adj = share::Adjacent(AtomicU64::new(0), AtomicU64::new(1));
            let iso = share::Isolated(
                AtomicU64::new(0),
                share::Pad(()),
                AtomicU64::new(1),
                share::Pad(()),
            );
            let run_adj = || {
                bounce_strict(
                    iters,
                    &adj,
                    // 闸形：同值 CAS（accepting 快路径核验形态）。
                    |s: &share::Adjacent| {
                        let _ =
                            s.1.compare_exchange(1, 1, Ordering::AcqRel, Ordering::Acquire);
                    },
                    |s: &share::Adjacent| {
                        s.0.fetch_add(1, Ordering::Relaxed);
                    },
                )
            };
            let run_iso = || {
                bounce_strict(
                    iters,
                    &iso,
                    |s: &share::Isolated| {
                        let _ =
                            s.2.compare_exchange(1, 1, Ordering::AcqRel, Ordering::Acquire);
                    },
                    |s: &share::Isolated| {
                        s.0.fetch_add(1, Ordering::Relaxed);
                    },
                )
            };
            let (a, i) = if round % 2 == 0 {
                (run_adj(), run_iso())
            } else {
                (run_iso(), run_adj())
            };
            ar.push(a);
            ir.push(i);
        }
        (median3(ar), median3(ir))
    };
    println!(
        "⑦b 计数器×闸CAS混合（屏障同步起跑, 3轮交替中位）: adjacent={adj_m:.1} ns/op  isolated(pad)={iso_m:.1} ns/op  隔离增益={:.2}×",
        adj_m / iso_m
    );
    println!(
        "⑦设计注: FxScope 单结构邻接布局（set/in_flight/accepting 同 struct），多线程 spawn \
         争用时热词同线弹跳风险由 ⑦ab 机制锚量化；③曲线形状为真实面背书。零产品改动（只报告）。"
    );
}

/// 停机面代价锚：abort 响应 / 协作取消传播 / accepting 闸单发 / shutdown 三态。
fn dim5_shutdown() {
    let rt = multi_rt(8);
    rt.block_on(async {
        // a) abort 响应：park 任务（Notify 挂起，mpsc 正信号=已达挂起点），abort→计数归零。
        let n = 200;
        let mut samples: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            let notify = Arc::new(tokio::sync::Notify::new());
            let scope = FxScope::new();
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let h = scope
                .spawn({
                    let notify = Arc::clone(&notify);
                    async move {
                        tx.send(()).unwrap();
                        notify.notified().await;
                    }
                })
                .unwrap();
            rx.recv().expect("task parked");
            let t0 = Instant::now();
            h.abort();
            while scope.in_flight() != 0 {
                std::hint::spin_loop();
            }
            samples.push(t0.elapsed().as_nanos() as u64);
            let _ = scope.shutdown().await;
        }
        samples.sort_unstable();
        let mean = samples.iter().sum::<u64>() / samples.len() as u64;
        println!(
            "⑤停机a abort响应: mean={mean}ns p99={}ns（park 任务强制中止→计数归零, n={n}）",
            pct(&samples, 99)
        );

        // b) 协作取消传播：任务自带外部取消对令牌（watch 旗），cancel()→任务醒来。
        //    口径：触发面=watch 旗传播+任务唤醒（与 scope 内部 token 同 channel 机制）。
        let n = 200;
        let mut samples: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            let (source, token) = cancel_pair();
            let scope = FxScope::new();
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            let done = Arc::new(AtomicU64::new(0));
            let h = scope
                .spawn({
                    let done = Arc::clone(&done);
                    async move {
                        tx.send(()).unwrap();
                        token.cancelled().await;
                        done.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .unwrap();
            rx.recv().expect("task parked");
            let t0 = Instant::now();
            source.cancel();
            while done.load(Ordering::Acquire) == 0 {
                std::hint::spin_loop();
            }
            samples.push(t0.elapsed().as_nanos() as u64);
            let _ = h;
            let _ = scope.shutdown().await;
        }
        samples.sort_unstable();
        let mean = samples.iter().sum::<u64>() / samples.len() as u64;
        println!(
            "⑤停机b 协作取消传播: mean={mean}ns p99={}ns（watch 旗→任务醒来, n={n}）",
            pct(&samples, 99)
        );

        // c) accepting 闸单发（close 本体 = 单 CAS）。
        let n = 1000;
        let mut samples: Vec<u64> = Vec::with_capacity(n);
        for _ in 0..n {
            let scope = FxScope::new();
            let t0 = Instant::now();
            scope.close();
            samples.push(t0.elapsed().as_nanos() as u64);
        }
        samples.sort_unstable();
        let mean = samples.iter().sum::<u64>() / samples.len() as u64;
        println!(
            "⑤停机c close闸单发: mean={mean}ns p99={}ns（accepting CAS 本体, n={n}）",
            pct(&samples, 99)
        );

        // d) shutdown 排空已完结：10k 即完结任务收齐后 shutdown（join_next 收割 10k 条目）。
        {
            let scope = Arc::new(FxScope::new());
            let done = Arc::new(AtomicU64::new(0));
            let n = 10_000u64;
            for _ in 0..n {
                let d = Arc::clone(&done);
                scope
                    .spawn(async move { black_box(d.fetch_add(1, Ordering::Relaxed)) })
                    .unwrap();
            }
            wait_until(Duration::from_secs(10), || done.load(Ordering::Acquire) >= n);
            let t0 = Instant::now();
            scope.shutdown().await;
            let d = t0.elapsed();
            println!(
                "⑤停机d shutdown排空已完结: {:.1}µs 总计 / {:.0}ns每条目（10k 完结条目 join_next 收割）",
                d.as_micros(),
                d.as_nanos() as f64 / n as f64
            );
        }

        // e) shutdown abort 在途：100 park 任务收齐挂起信号后 close+abort_all+收割。
        {
            let scope = Arc::new(FxScope::new());
            let notify = Arc::new(tokio::sync::Notify::new());
            let parked = Arc::new(AtomicU64::new(0));
            let n = 100u64;
            for _ in 0..n {
                let (notify, parked) = (Arc::clone(&notify), Arc::clone(&parked));
                scope
                    .spawn(async move {
                        parked.fetch_add(1, Ordering::Relaxed);
                        notify.notified().await;
                    })
                    .unwrap();
            }
            wait_until(Duration::from_secs(5), || parked.load(Ordering::Relaxed) >= n);
            let t0 = Instant::now();
            scope.shutdown().await;
            let d = t0.elapsed();
            println!(
                "⑤停机e shutdown中止在途: {}µs（100 park 任务 close+abort_all+收割, in_flight={}）",
                d.as_micros(),
                scope.in_flight()
            );
        }

        // f) shutdown_timeout 守卫：不可协作任务（单 poll 内自旋 30ms，abort 在 poll 边界
        //    前无法落地），timeout=5ms → TimedOut @≈5ms（守卫触发时延≈设定值）。
        {
            let mut elapsed_ms: Vec<u128> = Vec::new();
            let mut hit = 0;
            for _ in 0..3 {
                let scope = Arc::new(FxScope::new());
                let (tx, rx) = std::sync::mpsc::channel::<()>();
                scope
                    .spawn(async move {
                        tx.send(()).unwrap(); // 已被 poll ⇒ 正在自旋
                        let end = Instant::now() + Duration::from_millis(30);
                        while Instant::now() < end {
                            std::hint::spin_loop();
                        }
                    })
                    .unwrap();
                rx.recv().expect("spinner running");
                let t0 = Instant::now();
                let r = scope.shutdown_timeout(Duration::from_millis(5)).await;
                elapsed_ms.push(t0.elapsed().as_millis());
                if r == Err(TaskError::TimedOut) {
                    hit += 1;
                }
                // 自旋任务 30ms 后自然终局；留出收尾再进下一轮。
                std::thread::sleep(Duration::from_millis(40));
            }
            elapsed_ms.sort_unstable();
            println!(
                "⑤停机f shutdown_timeout守卫: 中位={}ms（设定 5ms, TimedOut 命中 {hit}/3；超出设定值部分=Windows 计时器粒度+传播开销，语义=超时必达必返）",
                elapsed_ms[1]
            );
        }
    });
}
