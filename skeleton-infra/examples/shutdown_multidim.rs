//! shutdown 域七维性能矩阵采集器（收窄声明：①register 分配面 ②编排 ns 锚
//! （N=0 驱动路径 / Done 克隆路径 / watchdog 线程路径）③组件数梯度 10/100/1000
//! ⑤N=0 编排延迟分布 p50/p99；④n/a——唯一可调面=超时值，无内部运行时调参；
//! ⑥n/a-by-construction——等待面全为阻塞原语（Condvar park / mpsc recv），
//! 无轮询环可计；⑦n/a——无热共享计数器，Phase 单 Mutex 低频转换，无伪共享面）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example shutdown_multidim --features memgov
//! ```
//!
//! 口径注（沿 time_multidim 惯例）：
//! ① CountingAllocator 读数仅串行上下文有效（本采集器单线程驱动）；
//! ② 组件闭包为瞬时 mock——数字是**编排开销**锚，不含真实组件停机耗时；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ black_box 防 LLVM 消除；每个锚三连跑取中位。
//!
//! 报告：shutdown 域性能锚（本地留存，未入库）。

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

use std::hint::black_box;
use std::time::{Duration, Instant};

use skeleton_infra::memgov::CountingAllocator;
use skeleton_infra::shutdown::ShutdownCoordinator;

// ---- 测量助手（time_multidim 同款）----

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
    perf_register_alloc();
    perf_shutdown_anchor();
    perf_gradient();
    perf_latency_dist();
    println!("④运行时调参/⑥零自旋/⑦伪共享: n/a（收窄声明见文件头）");
}

// ---- ① register 分配面 ----

fn perf_register_alloc() {
    println!("== ① register 分配面（allocs/call，存活承接口径）==");
    const N: u64 = 10_000;
    // 冷协调器：每 op 新建+注册；协调器们承接进 sink 到读数后才弃——
    // live_allocs 净额口径下 alloc+dealloc 抵消成 0，必须承接存活测真实次数
    // （journal_multidim 承接口径同款）。
    let mut sink: Vec<ShutdownCoordinator> = Vec::with_capacity(N as usize);
    let before = CountingAllocator::live_allocs();
    for _ in 0..N {
        let c = ShutdownCoordinator::new();
        black_box(c.register("a", || {})).ok();
        sink.push(c);
    }
    let cold = (CountingAllocator::live_allocs() - before) as f64 / N as f64;
    drop(sink);
    println!("register（冷协调器）      : {cold:.2}（实测=name String 1 + ZST 闭包 Box 0 + 注册表 Vec 首推 1；协调器本体零堆；非 ZST 闭包 Box +1）");

    // 热注册表：单协调器顺序注册唯一名，Vec 增长摊销≈0；组件存活到 c 弃置。
    // 名字池 N+1 条：allocs_per 先预热一次再读数。
    let names: Vec<String> = (0..=N).map(|i| format!("c{i}")).collect();
    let c = ShutdownCoordinator::new();
    let mut i = 0usize;
    let warm = allocs_per(
        || {
            let name = names[i].as_str();
            i += 1;
            black_box(c.register(name, || {})).ok();
        },
        N,
    );
    drop(c);
    println!("register（热注册表）      : {warm:.2}（实测=name String 1 + ZST 闭包 Box 0；Vec 增长摊销≈0；非 ZST 闭包 Box +1）");
    println!("（名字进报告行经 move 零二次分配——String 一次分配声明）");
}

// ---- ② 编排 ns 锚 ----

fn perf_shutdown_anchor() {
    println!("== ② shutdown 编排 ns 锚（mock 瞬时闭包）==");
    const N: u64 = 200_000;
    // N=0 首叫驱动路径：锁转相+空报告装配（含协调器构造，栈上零堆）。
    let drive0 = median3(|| batch_mean(|| black_box(ShutdownCoordinator::new()).shutdown(), N));
    println!("首叫驱动路径 N=0          : {drive0:6.1} ns/op（含 new）");

    // Done 二次叫克隆路径：phase 判定+空报告 clone。
    let c = ShutdownCoordinator::new();
    c.shutdown();
    let done = median3(|| batch_mean(|| c.shutdown(), N));
    println!("二次叫幂等路径（Done）    : {done:6.1} ns/op（报告克隆）");

    // watchdog 线程路径单组件锚：超时挂件+瞬时闭包=起停一线程+通道。
    let wd = median3(|| {
        batch_mean(
            || {
                let c = ShutdownCoordinator::new();
                c.register_with_timeout("w", || {}, Duration::from_secs(1))
                    .ok();
                black_box(c.shutdown())
            },
            1_000,
        )
    });
    println!("watchdog 线程路径 N=1     : {wd:6.0} ns/op（含 new+register；线程起停主导）");
}

// ---- ③ 组件数梯度 ----

fn perf_gradient() {
    println!("== ③ 组件数梯度（无超时内联 mock 闭包；phase 单向，总时长口径）==");
    for n in [10usize, 100, 1000] {
        let names: Vec<String> = (0..n).map(|i| format!("g{i}")).collect();
        let c = ShutdownCoordinator::new();
        for name in &names {
            c.register(name.as_str(), || {}).ok();
        }
        let t = Instant::now();
        let report = c.shutdown();
        let took = t.elapsed();
        assert_eq!(report.per_component.len(), n);
        println!(
            "N={n:<5} 总时长 {took:>8.1?}  ={:.0} ns/组件",
            took.as_nanos() as f64 / n as f64
        );
    }
}

// ---- ⑤ N=0 编排延迟分布 ----

fn perf_latency_dist() {
    println!("== ⑤ N=0 首叫编排延迟分布（20k 样本，含 new）==");
    const N: usize = 20_000;
    let mut xs: Vec<u128> = (0..N)
        .map(|_| {
            let t = Instant::now();
            black_box(ShutdownCoordinator::new()).shutdown();
            t.elapsed().as_nanos()
        })
        .collect();
    xs.sort_unstable();
    println!("p50={:.0} ns  p99={:.0} ns", xs[N / 2], xs[N * 99 / 100]);
}
