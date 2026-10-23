//! ratelimit 域七维性能矩阵采集器（收窄声明：①try_acquire 分配面
//! ②try_acquire ns 锚 ⑤阻塞版延迟分布（MockClock 虚拟 + SystemClock 实测
//! 双报）⑥零自旋（sleep 轮次计数）；③竞争扩展性收窄（单 Mutex 临界区
//! ~20ns，竞争面在锁域不在此域，契约明示）④⑦ n/a——无 resize 面/无
//! 缓存行热点计数）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example ratelimit_multidim --features memgov
//! ```
//!
//! 口径注（沿 time_multidim 惯例）：
//! ① CountingAllocator 读数仅串行上下文有效（本采集器测量段单线程）；
//! ② 计时时钟对开销（~2×20ns）双臂同担——直通成本取差值口径；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ black_box 防 LLVM 消除；每个锚三连跑取中位。
//!
//! 报告：reports/2026-10-16-b3-ratelimit.md（agentwiki 根）。

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

use std::hint::black_box;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use skeleton::clock::{Clock, MockClock, SystemClock};
use skeleton_infra::memgov::CountingAllocator;
use std_ext::ratelimit::TokenBucket;
use std_ext::retry::Sleeper;

// ---- 测量助手（time_multidim 同款）----

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut v: Vec<f64> = (0..3).map(|_| f()).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[1]
}

/// ① 分配计数：op 跑 n 次的 live_allocs 增量/次（先预热再读数）。
fn allocs_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op()); // 预热：懒初始化不进计数
    let before = CountingAllocator::live_allocs();
    for _ in 0..n {
        black_box(op());
    }
    (CountingAllocator::live_allocs() - before) as f64 / n as f64
}

/// ⑥ 观测面：包装 MockClock 计 sleep 轮次（测试替身 CountingSleeper 的
/// 采集器版——acquire 每次应恰一轮睡眠，轮数暴涨 = 忙转）。
struct SpinProbe {
    clock: MockClock,
    sleeps: AtomicU32,
}

impl SpinProbe {
    fn new() -> Self {
        Self {
            clock: MockClock::new(),
            sleeps: AtomicU32::new(0),
        }
    }

    fn resets(&self) {
        self.sleeps.store(0, Ordering::Relaxed);
    }

    fn count(&self) -> u32 {
        self.sleeps.load(Ordering::Relaxed)
    }
}

impl Clock for SpinProbe {
    fn now_instant(&self) -> Instant {
        self.clock.now_instant()
    }

    fn now_system(&self) -> SystemTime {
        self.clock.now_system()
    }
}

impl Sleeper for SpinProbe {
    fn sleep(&self, d: Duration) {
        self.sleeps.fetch_add(1, Ordering::Relaxed);
        self.clock.advance(d);
    }
}

/// ⑤ p50/p99：样本排序后取分位（采样数节制，粗测口径）。
fn pctl(mut samples: Vec<f64>, q: f64) -> f64 {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let idx = ((samples.len() as f64 - 1.0) * q).round() as usize;
    samples[idx]
}

fn main() {
    perf_try_acquire_alloc();
    perf_try_acquire_anchor();
    latency_blocking_virtual();
    latency_blocking_real();
    spin_rounds();
    println!("③ 竞争扩展性：收窄（单 Mutex ~20ns 临界区，竞争面归锁域，契约明示）");
    println!("④⑦: n/a（无 resize 面/无伪共享热点计数）");
}

// ---- ① try_acquire 快路径分配面 ----

fn perf_try_acquire_alloc() {
    println!("== ① try_acquire 分配面（allocs/call，期望 0）==");
    const N: u64 = 100_000;
    // 满桶成功路径 + 不足额拒绝路径，两臂都须零分配。
    let full = TokenBucket::new(Arc::new(SystemClock), 1e9, 1e9);
    let drain = TokenBucket::new(Arc::new(SystemClock), 1.0, 1.0);
    drain.try_acquire(1).unwrap(); // 清桶 → 之后恒 Insufficient
    println!(
        "try_acquire(1) 足额成功 : {:.2}",
        allocs_per(|| full.try_acquire(1), N)
    );
    println!(
        "try_acquire(1) 不足拒绝 : {:.2}",
        allocs_per(|| drain.try_acquire(1), N)
    );
}

// ---- ② try_acquire ns 锚 ----

fn perf_try_acquire_anchor() {
    println!("== ② try_acquire ns 锚（release/std Instant 粗测）==");
    const N: u64 = 100_000;
    let full = TokenBucket::new(Arc::new(SystemClock), 1e9, 1e9);
    let drain = TokenBucket::new(Arc::new(SystemClock), 1.0, 1.0);
    drain.try_acquire(1).unwrap();
    let ok = median3(|| batch_mean(|| full.try_acquire(1), N));
    let reject = median3(|| batch_mean(|| drain.try_acquire(1), N));
    let clock_only = median3(|| batch_mean(|| SystemClock.now_instant(), N));
    println!("try_acquire 成功（含锁+钟）: {ok:.0} ns/次");
    println!("try_acquire 拒绝（含报错载荷）: {reject:.0} ns/次");
    println!("SystemClock::now_instant 直通 : {clock_only:.0} ns/次（差值口径基线）");
}

// ---- ⑤ 阻塞版延迟分布·虚拟臂（MockClock：确定性，零真睡）----

fn latency_blocking_virtual() {
    println!("== ⑤ 阻塞版延迟·虚拟臂（MockClock：wait 序列确定性）==");
    let probe = Arc::new(SpinProbe::new());
    let bucket = TokenBucket::new(probe.clone(), 0.0 + 10.0, 5.0); // 200ms/token
    bucket.try_acquire(10).unwrap(); // 清空
    let t = Instant::now();
    for _ in 0..5 {
        bucket.acquire(1).unwrap();
    }
    let wall = t.elapsed();
    println!(
        "5 次 acquire(1)（200ms 间距）：虚拟总时长 {:?}（恰 5×200ms=1s），真实墙钟 {wall:?}（零真睡证明）",
        probe.clock.unix_offset()
    );
}

// ---- ⑤ 阻塞版延迟分布·实测臂（SystemClock 真睡，样本节制）----

fn latency_blocking_real() {
    println!("== ⑤ 阻塞版延迟·实测臂（SystemClock，64 样本 × 1ms/token）==");
    let bucket = TokenBucket::new(Arc::new(SystemClock), 1.0, 1000.0); // 1ms/token
    bucket.try_acquire(1).unwrap(); // 清桶
    let mut samples = Vec::with_capacity(64);
    for _ in 0..64 {
        let t = Instant::now();
        bucket.acquire(1).unwrap();
        samples.push(t.elapsed().as_nanos() as f64);
    }
    println!(
        "acquire(1) 真睡：p50 = {:.0}µs、p99 = {:.0}µs（理论 1ms；thread::sleep 平台粒度过睡为已知面）",
        pctl(samples.clone(), 0.50) / 1e3,
        pctl(samples, 0.99) / 1e3,
    );
}

// ---- ⑥ 零自旋：sleep 轮次计数 ----

fn spin_rounds() {
    println!("== ⑥ 零自旋（acquire 每次恰一轮睡眠）==");
    let probe = Arc::new(SpinProbe::new());
    let bucket = TokenBucket::new(probe.clone(), 100.0, 100.0); // 10ms/token
    bucket.try_acquire(100).unwrap(); // 清空
    probe.resets();
    for _ in 0..50 {
        bucket.acquire(1).unwrap();
    }
    println!(
        "50 次 acquire(1)：sleep 轮次 = {}（期望恰 50；>50 即忙转）",
        probe.count()
    );
    probe.resets();
    bucket.acquire(0).unwrap();
    println!("acquire(0)：sleep 轮次 = {}（期望 0）", probe.count());
}
