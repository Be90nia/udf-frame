//! lint 域七维性能矩阵采集器：①稳态分配事件（0/1/3/5 阶段）②ns 锚（空表 /
//! 1 阶段 / 5 阶段 / register_stage / registered_stages）③规模梯度 1/10/100 阶段
//! （判线性）④并发吞吐 1/2/4 线程（锁只在快照持有的吞吐证据）⑤panic 后可用性
//! ⑥阶段体内注册的快照语义 ⑦收窄声明。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton --example lint_multidim
//! ```
//!
//! 口径注（沿 ratelimit_multidim / time_multidim 惯例）：
//! ① 分配读数=本线程窗内事件计数（`IN_WINDOW` 门），本采集器测量段单线程；
//! ② 粗测（std `Instant`）非 criterion——**数字供判级，不供微优化基线**；
//! ③ `black_box` 防 LLVM 消除；每个锚三连跑取中位；
//! ④ 阶段用装箱闭包实现：每阶段含一次动态调用（真实阶段的量级），故 ns 锚是
//!    「编排器开销 + 一次间接调用」而非纯编排器开销；
//! ⑤ 注册表是进程级全局态，故各维用**各自的私有配置类型**，维间不串味。

#[global_allocator]
static ALLOC: EventCounting = EventCounting;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use skeleton::lint::{
    clear_stages, post_process, register_stage, registered_stages, LintError, LintStage,
};

// ---- ① 分配事件计数（同 tests/lint_alloc.rs 口径）----

/// 分配事件累计（窗内 + 本线程，见 [`IN_WINDOW`]）。
static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、无析构注册。
thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: 纯转发 System，仅在窗内转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // try_with：线程 TLS 析构期访问会 panic（分配器内 panic=abort），取不到当不在窗内。
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

// ---- 阶段替身 ----

/// 装箱闭包阶段：名字 + 一次动态调用（真实阶段的调用形态）。
struct FnStage<C> {
    name: &'static str,
    run: Box<dyn Fn(&mut C) + Send + Sync>,
}

impl<C: 'static> FnStage<C> {
    fn arc(
        name: &'static str,
        run: impl Fn(&mut C) + Send + Sync + 'static,
    ) -> Arc<dyn LintStage<C>> {
        Arc::new(Self {
            name,
            run: Box::new(run),
        })
    }
}

impl<C> LintStage<C> for FnStage<C> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, cfg: &mut C) -> Result<(), LintError> {
        (self.run)(cfg);
        Ok(())
    }
}

/// 空阶段（只排队、不做事）：量编排器开销，不含用户逻辑。
fn stage_noop<C: 'static>(name: &'static str) -> Arc<dyn LintStage<C>> {
    FnStage::arc(name, |_: &mut C| {})
}

/// 命中阶段：自增一个外部计数器（证明阶段真的跑到，且不引入堆分配）。
fn stage_hit<C: 'static>(name: &'static str, hits: &Arc<AtomicUsize>) -> Arc<dyn LintStage<C>> {
    let hits = Arc::clone(hits);
    FnStage::arc(name, move |_: &mut C| {
        hits.fetch_add(1, Ordering::Relaxed);
    })
}

// ---- 测量助手（time_multidim 同款）----

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut samples: Vec<f64> = (0..3).map(|_| f()).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("ns 读数无 NaN"));
    samples[1]
}

fn batch_ns<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

/// 窗内事件计数 / 调用次数（预热一拍不进窗），并回传阶段命中数（防假零）。
fn measure_alloc_events<C: 'static>(cfg: &mut C, stages: usize) -> (f64, usize) {
    const N: u64 = 100_000;
    clear_stages::<C>();
    let hits = Arc::new(AtomicUsize::new(0));
    for _ in 0..stages {
        register_stage::<C>(stage_hit("hit", &hits));
    }
    post_process(cfg).expect("阶段恒 Ok"); // 预热：懒初始化不进计数
    hits.store(0, Ordering::Relaxed);

    IN_WINDOW.with(|w| w.set(true));
    let before = EVENTS.load(Ordering::Relaxed);
    for _ in 0..N {
        black_box(post_process(cfg)).expect("阶段恒 Ok");
    }
    let events = EVENTS.load(Ordering::Relaxed) - before;
    IN_WINDOW.with(|w| w.set(false));
    (events as f64 / N as f64, hits.load(Ordering::Relaxed))
}

/// `post_process` 的 ns/次（表先清再按 `stages` 重建）。
fn measure_ns<C: 'static>(cfg: &mut C, stages: usize, n: u64) -> f64 {
    clear_stages::<C>();
    for _ in 0..stages {
        register_stage::<C>(stage_noop("noop"));
    }
    post_process(cfg).expect("阶段恒 Ok");
    median3(|| batch_ns(|| post_process(cfg).expect("阶段恒 Ok"), n))
}

fn main() {
    println!("== lint 域七维采集（release / std Instant 粗测；本地数字，环境相关）==");
    dim1_alloc();
    dim2_ns();
    dim3_scale();
    dim4_concurrency();
    dim5_panic_recovery();
    dim6_reentrant();
    dim7_narrowing();
}

// ---- ① 稳态分配事件 ----

struct AllocCfg;

fn dim1_alloc() {
    println!("\n[dim1] 稳态分配事件（预热后 100k 拍，本线程窗内）");
    println!(
        "  {:<6} | {:>12} | {:>10}",
        "stages", "events/call", "stage hits"
    );
    let mut cfg = AllocCfg;
    let mut verdict = true;
    for stages in [0usize, 1, 3, 5] {
        let (events, hits) = measure_alloc_events(&mut cfg, stages);
        println!("  {:<6} | {:>12.2} | {:>10}", stages, events, hits);
        verdict &= events == 0.0 && hits == stages * 100_000;
    }
    println!(
        "  判定: {}（快照路径 Arc::clone 仅原子自增，稳态零堆分配；hits 证明阶段真跑到）",
        if verdict {
            "PASS 全档 0 事件"
        } else {
            "FAIL 出现分配事件或阶段未跑"
        }
    );
}

// ---- ② ns 锚 ----

struct NsCfg;

struct RegCfg;

fn dim2_ns() {
    println!("\n[dim2] ns 锚（每拍含一次间接调用；中位三连跑）");
    println!("  {:<32} | {:>10}", "path", "ns/call");
    let mut cfg = NsCfg;
    let empty = measure_ns(&mut cfg, 0, 100_000);
    let one = measure_ns(&mut cfg, 1, 100_000);
    let five = measure_ns(&mut cfg, 5, 100_000);
    println!("  {:<32} | {:>10.1}", "post_process (0 stages)", empty);
    println!("  {:<32} | {:>10.1}", "post_process (1 stage)", one);
    println!("  {:<32} | {:>10.1}", "post_process (5 stages)", five);

    // register_stage：Arc 构造在计时外，只量注册表路径（锁 + entry + COW + push）。
    const REG: usize = 10_000;
    let prepared: Vec<Arc<dyn LintStage<RegCfg>>> =
        (0..REG).map(|_| stage_noop::<RegCfg>("noop")).collect();
    clear_stages::<RegCfg>();
    let register_ns = median3(|| {
        clear_stages::<RegCfg>();
        let t = Instant::now();
        for stage in prepared.iter() {
            register_stage::<RegCfg>(Arc::clone(stage));
        }
        t.elapsed().as_nanos() as f64 / REG as f64
    });
    println!(
        "  {:<32} | {:>10.1}",
        "register_stage (no Arc build)", register_ns
    );

    let diag = median3(|| {
        let t = Instant::now();
        let names = registered_stages::<RegCfg>();
        let ns = t.elapsed().as_nanos() as f64;
        black_box(names);
        ns
    });
    println!("  {:<32} | {:>10.1}", "registered_stages (10k names)", diag);
    println!(
        "  判定: 编排器固定成本 = 一次全局读锁（RwLock 读侧）+ 一次原子自增；每阶段 ~一次间接调用"
    );
}

// ---- ③ 规模梯度（判线性）----

struct ScaleCfg;

fn dim3_scale() {
    println!("\n[dim3] 规模梯度（post_process ns/call 随阶段数；批拍数按阶段数反比配平）");
    let mut cfg = ScaleCfg;
    let plan: [(usize, u64); 4] = [(1, 200_000), (10, 200_000), (100, 20_000), (1_000, 2_000)];
    let measured: Vec<(usize, f64)> = plan
        .into_iter()
        .map(|(stages, n)| (stages, measure_ns(&mut cfg, stages, n)))
        .collect();

    println!(
        "  {:>7} | {:>10} | {:>12}",
        "stages", "ns/call", "marginal/st"
    );
    let mut previous: Option<(usize, f64)> = None;
    for &(stages, ns) in &measured {
        match previous {
            None => println!("  {stages:>7} | {ns:>10.1} | {:>12}", "-"),
            Some((from_stages, from_ns)) => {
                let marginal = (ns - from_ns) / (stages - from_stages) as f64;
                println!("  {stages:>7} | {ns:>10.1} | {marginal:>12.2}");
            }
        }
        previous = Some((stages, ns));
    }

    // 线性判据看**边际**单阶段成本：每拍固定成本（一次锁 + 一次原子自增）在每个读数里
    // 都一样，作差即消掉；1→10 段只剩两三个阶段成本的量级，噪声占比大，故判据取
    // 10→100 与 100→1000 两段斜率（后者固定成本占比 <1%）。
    let slopes: Vec<f64> = measured
        .windows(2)
        .map(|pair| (pair[1].1 - pair[0].1) / (pair[1].0 - pair[0].0) as f64)
        .collect();
    let low = slopes[slopes.len() - 2];
    let high = slopes[slopes.len() - 1];
    let drift = (high - low).abs() / low * 100.0;
    let ns1 = measured[0].1;
    println!(
        "  判定: 1 阶段整体 {ns1:.1} ns（固定成本+1 阶段）；每阶段 {low:.2}→{high:.2} ns\
（10→100 段 vs 100→1000 段，漂移 {drift:.0}%）——{}",
        if drift < 50.0 {
            "近线性：边际成本不随阶段数放大（无每拍分配、无逐阶段缓存失效）"
        } else {
            "偏离线性，需查缓存/分配面"
        }
    );
}

// ---- ④ 并发吞吐 ----

#[derive(Default)]
struct TrivialCfg {
    hits: u64,
}

#[derive(Default)]
struct WorkCfg {
    acc: u64,
}

/// 并发吞吐表：同一 `C` 的表、各线程私有 cfg 实例，1/2/4 线程各跑 `calls` 拍；
/// 返回 4 线程相对 1 线程的加速比。
fn concurrency_table<C: Default + 'static>(calls: u64) -> f64 {
    println!("  {:>7} | {:>13} | {:>9}", "threads", "Mops/sec", "speedup");
    let mut base = 0.0;
    let mut last = 0.0;
    for threads in [1usize, 2, 4] {
        let barrier = Arc::new(Barrier::new(threads + 1));
        let start = Instant::now();
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut cfg = C::default();
                    barrier.wait();
                    for _ in 0..calls {
                        post_process(&mut cfg).expect("阶段恒 Ok");
                    }
                })
            })
            .collect();
        barrier.wait();
        for handle in handles {
            handle.join().expect("并发线程不得 panic");
        }
        let elapsed = start.elapsed().as_secs_f64();
        let rate = (threads as f64 * calls as f64) / elapsed / 1e6;
        if threads == 1 {
            base = rate;
        }
        last = rate / base;
        println!("  {threads:>7} | {rate:>13.2} | {last:>8.2}x");
    }
    last
}

fn dim4_concurrency() {
    let cores = thread::available_parallelism().map_or(0, |n| n.get());
    println!("\n[dim4] 并发吞吐（同类型表，各线程私有 cfg 实例；本机可用核数 {cores}）");

    clear_stages::<TrivialCfg>();
    register_stage::<TrivialCfg>(FnStage::arc("bump", |cfg: &mut TrivialCfg| {
        cfg.hits += 1;
    }));
    println!("  -- 空阶段（只 bump 一个字段）--");
    let trivial = concurrency_table::<TrivialCfg>(2_000_000);

    clear_stages::<WorkCfg>();
    register_stage::<WorkCfg>(FnStage::arc("work", |cfg: &mut WorkCfg| {
        let mut acc = cfg.acc;
        for round in 0..200_000u64 {
            acc = acc
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(round);
        }
        cfg.acc = acc;
    }));
    println!("  -- ~20µs 计算阶段（200k 轮 LCG；每拍锁成本占比 <1%）--");
    let work = concurrency_table::<WorkCfg>(2_000);

    // 已知取舍（ponytail: 每拍一次全局读锁）：阶段比锁还便宜时（30ns 级任务）争用即
    // 退化——读锁换来的是稳态零分配与锁外执行。锁类型是实测选的，完整对照与前值留档在
    // `verify/2026-10-29-lint-lock-showdown/`（含 Mutex 版源码 `lint.rs.mutex.bak`）；
    // 本行**只印 live 读数**，不在源码里复述历史数字（单批运行，粗测）。瓶颈是同一缓存行
    // 上的原子 RMW 争用（2 线程还略差）：再要高吞吐走「各自 config 类型分槽」或别每拍
    // lint，不做 per-线程表缓存（旧快照会滞留每线程）。
    println!(
        "  判定: 空阶段 4 线程 {trivial:.2}x（每拍一次全局读锁 → 30ns 级任务的争用上限；\
RwLock 选型依据见 verify/2026-10-29-lint-lock-showdown/）；\
计算阶段 {work:.2}x ——{}",
        if work > 1.5 {
            "锁不持在阶段执行期：用户代码真并行"
        } else {
            "计算阶段也未见并行（核数不足/调度干扰，需复测）"
        }
    );
}

// ---- ⑤ panic 后可用性 ----

#[derive(Default)]
struct PanicCfg {
    runs: u32,
}

fn dim5_panic_recovery() {
    println!("\n[dim5] panic 后注册表可用性（不 catch_unwind 吞错，panic 原样传播）");
    clear_stages::<PanicCfg>();
    register_stage::<PanicCfg>(FnStage::arc("boom", |_: &mut PanicCfg| {
        panic!("stage panic（采集器故意触发）")
    }));
    let mut cfg = PanicCfg::default();
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {})); // 采集器自己吞掉 panic 打印，保持表格干净
    let caught = catch_unwind(AssertUnwindSafe(|| post_process(&mut cfg)));
    std::panic::set_hook(hook);
    println!(
        "  panic 传播到调用方: {}",
        if caught.is_err() { "是" } else { "否" }
    );

    clear_stages::<PanicCfg>();
    register_stage::<PanicCfg>(FnStage::arc("good", |cfg: &mut PanicCfg| {
        cfg.runs += 1;
    }));
    let ns = median3(|| batch_ns(|| post_process(&mut cfg).expect("清场后恒 Ok"), 10_000));
    println!(
        "  清场后一次正常调用: {ns:.1} ns（cfg.runs={}，注册表未被 panic 毒化）",
        cfg.runs
    );
    println!(
        "  判定: {}",
        if caught.is_err() && cfg.runs > 0 {
            "PASS 锁不在执行期持有 → panic 不毒化全局态"
        } else {
            "FAIL panic 被吞或注册表失效"
        }
    );
}

// ---- ⑥ 重入注册的快照语义 ----

#[derive(Default)]
struct ReentrantCfg {
    trace: Vec<&'static str>,
}

fn dim6_reentrant() {
    println!("\n[dim6] 阶段体内注册的快照语义（锁外执行 → 不死锁）");
    clear_stages::<ReentrantCfg>();
    register_stage::<ReentrantCfg>(FnStage::arc("a", |cfg: &mut ReentrantCfg| {
        cfg.trace.push("a");
        register_stage::<ReentrantCfg>(FnStage::arc("b", |cfg: &mut ReentrantCfg| {
            cfg.trace.push("b");
        }));
    }));
    let mut cfg = ReentrantCfg::default();
    post_process(&mut cfg).expect("阶段恒 Ok");
    let first = cfg.trace.clone();
    post_process(&mut cfg).expect("阶段恒 Ok");
    println!("  第 1 拍 trace: {first:?}（本次只跑快照内的 a）");
    println!("  第 2 拍 trace: {:?}（a 再跑 + 首次执行 b）", cfg.trace);
    println!(
        "  名单: {:?}（a 每拍都登记一次 b，重复名合法）",
        registered_stages::<ReentrantCfg>()
    );
    println!(
        "  判定: {}",
        if first == ["a"] && cfg.trace == ["a", "a", "b"] {
            "PASS 运行期注册下一拍生效（快照语义）"
        } else {
            "FAIL 可见性与文档不符"
        }
    );
}

// ---- ⑦ 收窄声明 ----

fn dim7_narrowing() {
    println!("\n[dim7] 收窄声明（本采集器不测的面）");
    println!("  无 resize 面: 阶段表只有 push/remove，无容量策略可调（n/a）");
    println!("  无阻塞面:    post_process 从不 park/sleep；锁只在快照期持有（n/a）");
    println!("  无自旋面:    无忙等、无重试循环（n/a）");
    println!("  无 IO/无异步: 纯 std 同步路径，零文件/网络面（n/a）");
    println!("  未测: 阶段自身耗时（取决于采用方逻辑，不属于编排器口径）");
}
