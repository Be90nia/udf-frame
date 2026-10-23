//! scheduler 域七维性能矩阵采集器（收窄声明：①注册/next_deadline/tick 空转
//! 分配面 ②注册 uncontended ns 锚 ③并发注册扩展性曲线（1/2/4/8 线程）④cancel
//! 延迟锚（CAS O(1)）⑤worker 驱动触发精度双报（p50/p99 偏差）⑥idle park 零自旋
//! （passes 静止证据）⑦ n/a——任务胞各自独立 Arc 堆分配无共享计数阵列，无伪
//! 共享面）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example scheduler_multidim --features memgov
//! ```
//!
//! 口径注（沿 time_multidim/perf_batchc 惯例）：
//! ① CountingAllocator 读数仅串行上下文有效（①④串行；③⑤并发段不计分配）；
//! ② 计时时钟自身开销（~20ns/读）双臂同担，取差值口径；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ worker 精度段是真实时钟面（SystemClock），数字含 OS 调度抖动，供量级
//!    判级非 SLO 承诺；⑤ black_box 防 LLVM 消除；batch_mean 三连跑取中位。
//!
//! 报告：reports/2026-10-16-b3-scheduler.md（agentwiki 根）。

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use skeleton::clock::SystemClock;
use skeleton_infra::memgov::CountingAllocator;
use std_ext::scheduler::Scheduler;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ---- 测量助手（perf_batchc 同款）----

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut v: Vec<f64> = (0..3).map(|_| f()).collect();
    v.sort_by(f64::total_cmp);
    v[1]
}

fn allocs_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op());
    let before = CountingAllocator::live_allocs();
    for _ in 0..n {
        black_box(op());
    }
    (CountingAllocator::live_allocs() - before) as f64 / n as f64
}

/// p50/p99：样本排序取位（ns）。
fn pct(sorted: &mut [f64], p: f64) -> f64 {
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx]
}

fn main() {
    perf_alloc_surface();
    perf_register_anchor();
    perf_register_scaling();
    perf_cancel_anchor();
    perf_trigger_precision();
    perf_idle_park();
    println!("⑦: n/a（TaskCell 各自独立 Arc 堆分配，无共享计数阵列——伪共享面不存在）");
}

// ---- ① 分配面 ----

fn perf_alloc_surface() {
    println!("== ① 分配面（allocs/call，CountingAllocator）==");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let reg = allocs_per(
        || {
            sched.after(ms(3_600_000), || {}).expect("未关闸").cancel();
        },
        10_000,
    );
    println!("register+cancel（1 alloc = Arc<TaskCell>；Mutex 内嵌零额外；无捕获闭包零 Box 分配）: {reg:.2}");
    let scan = allocs_per(|| sched.next_deadline(), 100_000);
    println!("next_deadline（锁内 peek）           : {scan:.2}（期望 0）");
    let empty_tick = allocs_per(|| sched.tick(), 100_000);
    println!("tick 空堆（锁内 peek 即返）          : {empty_tick:.2}（期望 0）");
}

// ---- ② 注册 uncontended 锚 ----

fn perf_register_anchor() {
    println!("== ② 注册 uncontended ns 锚（含 deadline 合成+入堆+唤醒，即注册即 cancel 回收）==");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let reg = median3(|| {
        batch_mean(
            || {
                sched.after(ms(3_600_000), || {}).expect("未关闸").cancel();
            },
            10_000,
        )
    });
    println!("after+cancel 不计回收: {reg:.0} ns/op（Mutex 非竞争 + BinaryHeap push O(log n)）");
}

// ---- ③ 并发注册扩展性 ----

fn perf_register_scaling() {
    println!("== ③ 并发注册吞吐（ops/s，4 任务/线程 × 1/2/4/8 线程，竞争扩展性曲线）==");
    for threads in [1usize, 2, 4, 8] {
        let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
        let per_thread = 20_000u64;
        let t0 = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let s = Arc::clone(&sched);
                thread::spawn(move || {
                    for _ in 0..per_thread {
                        s.after(ms(3_600_000), || {}).expect("未关闸").cancel();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().expect("worker 线程");
        }
        let elapsed = t0.elapsed().as_secs_f64();
        let total = (threads as u64 * per_thread) as f64;
        println!(
            "{threads:>1} 线程: {:.0} ops/s（含 cancel 回收）",
            total / elapsed
        );
        sched.close();
    }
}

// ---- ④ cancel 延迟锚 ----

fn perf_cancel_anchor() {
    println!("== ④ cancel 延迟锚（CAS O(1)，堆内 1k 条目语境）==");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    // 预置 1k 长期条目垫堆深（cancel 不摘堆=O(1) 的语境证据）。
    for _ in 0..1_000 {
        sched.after(ms(3_600_000), || {}).expect("未关闸").detach();
    }
    let cancel = median3(|| {
        batch_mean(
            || {
                let h = sched.after(ms(3_600_000), || {}).expect("未关闸");
                black_box(h.cancel());
            },
            10_000,
        )
    });
    println!("cancel（1k 堆深）: {cancel:.0} ns/op（单 CAS，与堆深无关）");
    sched.close();
}

// ---- ⑤ worker 驱动触发精度双报（真实时钟面）----

fn perf_trigger_precision() {
    println!("== ⑤ 触发精度双报（SystemClock worker 面，20 个 50ms 错峰 after 偏差 ms，含 OS 调度抖动）==");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let observed: Arc<std::sync::Mutex<Vec<f64>>> = Arc::default();
    // 采样锚：20 个 20ms+i*50ms 错峰一次性任务。错峰间隔取 50ms ≫ Windows
    // 时钟粒度（~15.6ms）：间隔小于粒度时多任务同拍醒来串行点火，偏差被
    // 积压污染（测的是后尾延迟而非调度精度）。
    for i in 0..20u64 {
        let obs = Arc::clone(&observed);
        let t0 = Instant::now();
        let target = 20 + i * 50;
        sched
            .after(ms(target), move || {
                let dev_ms = t0.elapsed().as_secs_f64() * 1e3 - target as f64;
                obs.lock().expect("test vec").push(dev_ms);
            })
            .expect("未关闸")
            .detach();
    }
    let worker = sched.spawn_worker();
    // 等全部点火（超时判负）。
    let deadline = Instant::now() + Duration::from_secs(10);
    while observed.lock().expect("test vec").len() < 20 {
        if Instant::now() >= deadline {
            panic!("触发精度采样未在 10s 内收齐（worker 面）");
        }
        thread::sleep(ms(5));
    }
    worker.join();
    let mut samples = observed.lock().expect("test vec").clone();
    let p50 = pct(&mut samples, 0.50);
    let p99 = pct(&mut samples, 0.99);
    println!("偏差 p50 = {p50:.2} ms / p99 = {p99:.2} ms（相对各自 deadline；含 Windows 时钟粒度 ~15.6ms 与 OS 调度抖动）");
}

// ---- ⑥ idle park 零自旋 ----

fn perf_idle_park() {
    println!("== ⑥ idle park 零自旋（空堆 200ms，驱动循环 passes 增量）==");
    let sched = Arc::new(Scheduler::new(Arc::new(SystemClock)));
    let worker = sched.spawn_worker();
    thread::sleep(ms(100)); // 等 worker 入 park
    let p1 = sched.worker_passes();
    thread::sleep(ms(200));
    let p2 = sched.worker_passes();
    worker.join();
    println!(
        "200ms idle 窗口 passes 增量: {}（0 = 零自旋 park 实证）",
        p2 - p1
    );
}
