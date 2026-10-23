//! time 域七维性能矩阵采集器（收窄声明：①now()/advance/format 分配面 ②now()
//! ns 锚 ③格式化输入规模梯度；④⑤⑥⑦ n/a——纯函数域+时钟锚，无锁竞争曲线/
//! 无并发扩展面/无伪共享/无 IO）。2026-10-17 增 RFC3339 双向面（分配/锚/
//! 长度梯度，humantime dev-dep 对照基线），报告 reports/2026-10-17-rfc3339.md。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example time_multidim --features memgov
//! ```
//!
//! 口径注（沿 perf_batchc/multidim 惯例）：
//! ① CountingAllocator 读数仅串行上下文有效（本采集器单线程）；
//! ② 计时时钟对开销（~2×20ns）双臂同担——直通成本取差值口径；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ black_box 防 LLVM 消除；每个 batch_mean 三连跑取中位。
//!
//! 报告：reports/2026-10-16-b1-time.md（agentwiki 根）。

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

use std::hint::black_box;
use std::time::{Duration, Instant, SystemTime};

use skeleton::clock::{Clock, MockClock, SystemClock};
use skeleton_infra::memgov::CountingAllocator;
use std_ext::clock::format_duration;
use std_ext::clock::rfc3339::{format_rfc3339, parse_rfc3339, parse_rfc3339_weak};

// ---- 测量助手（perf_batchc 同款）----

/// 大批量均值：ns/op。返回值黑箱化后弃置，防 LLVM 消除被测 op。
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

fn main() {
    perf_now_alloc();
    perf_now_anchor();
    perf_format_gradient();
    perf_rfc3339();
    println!("④⑤⑥⑦: n/a（纯函数域+时钟锚；无锁竞争/并发扩展/伪共享/IO 面）");
}

// ---- ① 分配面 ----

fn perf_now_alloc() {
    println!("== ① now()/advance/format 分配面（allocs/call）==");
    const N: u64 = 100_000;
    let real = SystemClock;
    let mock = MockClock::new();
    println!(
        "SystemClock::now_instant : {:.2}（期望 0）",
        allocs_per(|| real.now_instant(), N)
    );
    println!(
        "SystemClock::now_system  : {:.2}（期望 0）",
        allocs_per(|| real.now_system(), N)
    );
    println!(
        "MockClock::now_instant   : {:.2}（期望 0）",
        allocs_per(|| mock.now_instant(), N)
    );
    println!(
        "MockClock::now_system    : {:.2}（期望 0）",
        allocs_per(|| mock.now_system(), N)
    );
    println!(
        "MockClock::advance(1ms)  : {:.2}（期望 0）",
        allocs_per(|| mock.advance(Duration::from_millis(1)), N)
    );
    // String 返回值存活口径：预扩 Vec 承接返回值再统一 drop——`black_box(op());`
    // 即弃会让 alloc+dealloc 抵消成净 0（live_allocs 净存活计数的固有盲区，
    // journal_multidim 盲区声明同款；承接口径测得真实 alloc 次数）。
    let mut sink: Vec<String> = Vec::with_capacity(N as usize);
    let before = CountingAllocator::live_allocs();
    for _ in 0..N {
        sink.push(black_box(format_duration(Duration::from_secs(90))));
    }
    let format_allocs = (CountingAllocator::live_allocs() - before) as f64 / N as f64;
    drop(sink);
    println!(
        "format_duration(90s)     : {format_allocs:.2}（期望 1=返回 String 本体，存活承接口径）"
    );
}

// ---- ② ns 锚（std 直通成本）----

fn perf_now_anchor() {
    println!("== ② now() ns 锚（直通成本=与 std 直调差值）==");
    const N: u64 = 1_000_000;
    let real = SystemClock;
    let mock = MockClock::new();

    let raw_instant = median3(|| batch_mean(Instant::now, N));
    let wrap_instant = median3(|| batch_mean(|| real.now_instant(), N));
    let raw_system = median3(|| batch_mean(SystemTime::now, N));
    let wrap_system = median3(|| batch_mean(|| real.now_system(), N));
    let mock_instant = median3(|| batch_mean(|| mock.now_instant(), N));
    let mock_system = median3(|| batch_mean(|| mock.now_system(), N));

    println!("std Instant::now          : {raw_instant:6.1} ns/call");
    println!(
        "SystemClock::now_instant  : {wrap_instant:6.1} ns/call（直通 Δ={:+.1}）",
        wrap_instant - raw_instant
    );
    println!("std SystemTime::now       : {raw_system:6.1} ns/call");
    println!(
        "SystemClock::now_system   : {wrap_system:6.1} ns/call（直通 Δ={:+.1}）",
        wrap_system - raw_system
    );
    println!("MockClock::now_instant    : {mock_instant:6.1} ns/call（Mutex 锁+checked_add）");
    println!("MockClock::now_system     : {mock_system:6.1} ns/call（Mutex 锁+checked_add）");
}

// ---- ③ 格式化输入规模梯度（纯函数域）----

fn perf_format_gradient() {
    println!("== ③ format_duration 输入规模梯度（ns/format）==");
    const N: u64 = 200_000;
    let cases: [(&str, Duration); 9] = [
        ("1ns", Duration::from_nanos(1)),
        ("1us", Duration::from_micros(1)),
        ("1ms", Duration::from_millis(1)),
        ("1s", Duration::from_secs(1)),
        ("1m", Duration::from_secs(60)),
        ("1h", Duration::from_secs(3600)),
        ("1day", Duration::from_secs(86_400)),
        ("365d", Duration::from_secs(86_400 * 365)),
        ("MAX", Duration::MAX),
    ];
    for (name, d) in cases {
        let ns = median3(|| batch_mean(|| format_duration(d), N));
        let out = format_duration(d);
        println!(
            "{name:>4}: {ns:>6.0} ns/format  out_len={:>2}  \"{out}\"",
            out.len()
        );
    }
}

// ---- RFC 3339 面（2026-10-17 增：分配面 + ns 锚 humantime 对照 + 长度梯度）----

fn perf_rfc3339() {
    println!("== RFC3339 ① 分配面（allocs/call，存活承接口径）==");
    const N: u64 = 100_000;
    let epoch = SystemTime::UNIX_EPOCH;
    // String 返回值存活承接口径（format_duration 同款：即弃会让 alloc/dealloc
    // 在 live_allocs 净计数下抵消）。
    let mut sink: Vec<String> = Vec::with_capacity(N as usize);
    let before = CountingAllocator::live_allocs();
    for _ in 0..N {
        sink.push(black_box(format_rfc3339(epoch)));
    }
    let fmt_allocs = (CountingAllocator::live_allocs() - before) as f64 / N as f64;
    drop(sink);
    println!("format_rfc3339(整秒)      : {fmt_allocs:.2}（期望 1=返回 String 本体）");
    println!(
        "parse_rfc3339(strict)     : {:.2}（期望 0=纯值返回）",
        allocs_per(|| parse_rfc3339("1985-04-12T23:20:50.52Z"), N)
    );
    println!(
        "parse_rfc3339_weak(weak)  : {:.2}（期望 0）",
        allocs_per(|| parse_rfc3339_weak("2018-02-14 00:28:07+0800"), N)
    );

    println!("== RFC3339 ② ns 锚（humantime dev-dep 对照基线）==");
    const M: u64 = 500_000;
    let frac_t = epoch + Duration::new(1, 123_456_789);
    let whole = format_rfc3339(epoch);
    let full = format_rfc3339(frac_t);
    println!(
        "format 整秒(20B)          : {:6.1} ns",
        median3(|| batch_mean(|| format_rfc3339(epoch), M))
    );
    println!(
        "format .123456789(30B)    : {:6.1} ns",
        median3(|| batch_mean(|| format_rfc3339(frac_t), M))
    );
    println!(
        "humantime 整秒(20B)       : {:6.1} ns（对照）",
        median3(|| batch_mean(|| humantime::format_rfc3339(epoch).to_string(), M))
    );
    println!(
        "humantime .123000000(30B) : {:6.1} ns（对照）",
        median3(|| batch_mean(|| humantime::format_rfc3339(frac_t).to_string(), M))
    );
    println!(
        "parse strict 整秒         : {:6.1} ns",
        median3(|| batch_mean(|| parse_rfc3339(&whole), M))
    );
    println!(
        "parse strict 9位frac      : {:6.1} ns",
        median3(|| batch_mean(|| parse_rfc3339(&full), M))
    );
    println!(
        "parse weak ±HHMM          : {:6.1} ns",
        median3(|| batch_mean(|| parse_rfc3339_weak("2018-02-14 00:28:07+0800"), M))
    );
    println!(
        "humantime parse strict    : {:6.1} ns（对照）",
        median3(|| batch_mean(|| humantime::parse_rfc3339(&whole), M))
    );

    println!("== RFC3339 ③ 输出长度梯度（宽度面）==");
    let cases: [(&str, SystemTime); 3] = [
        ("整秒", epoch),
        (".52", epoch + Duration::new(482_196_050, 520_000_000)),
        ("9位frac", frac_t),
    ];
    for (name, t) in cases {
        let out = format_rfc3339(t);
        println!("{name:>6}: out_len={:>2}  \"{out}\"", out.len());
    }
}
