//! 骨架多维实测数据采集器（性能/内存/并发三维）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example multidim_eval --features memgov,testing
//! ```
//!
//! 口径注：
//! ① CountingAllocator 常驻本二进制（内存维前提），计时含每分配 2 次 Relaxed 原子加
//!    噪声（benches/infra.rs 同款声明，量级 ns）；
//! ② Instant 时钟对开销（~2×20ns）双臂同担——抽象税主口径=大批量均值摊薄时钟噪声，
//!    p50/p99 为单发采样分布辅证；
//! ③ 粗测（std Instant 分位数）非 criterion——数字供判级，不供微优化基线；
//! ④ 内存段全程串行（计数器仅串行上下文有效），并发段只断言不变量不计内存。

// 裸基线用 std::sync::Mutex+unwrap：①本 crate 零依赖纪律（parking_lot 不入树，
// criterion dev-dep 是唯一豁免）；②被测壳本身即 std Mutex 毒锁 fail-fast
// 语义（skeleton lib.rs `lock()` helper）——裸基线须同族对照才公平。
use std::collections::VecDeque;
use std::convert::Infallible;
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use skeleton::{ActionSender, Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
use skeleton_infra::memgov::CountingAllocator;
use skeleton_infra::runtime::SyncStore as Store;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

// ---- 被测形态：计数器 Feature（零效果，Op=Infallible）----

struct Count;

impl Reducer for Count {
    type State = u64;
    type Action = ();
    type Op = Infallible;
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, (): (), _out: &mut EffectBuf<(), Infallible>) -> u64 {
        s + 1
    }
}

struct Nop;

impl EffectRuntime<(), Infallible> for Nop {
    fn run(&mut self, _: Effect<(), Infallible>, _: &ActionSender<()>) {}
}

// ---- 裸基线（任务口径：手写 mutex+VecDeque 单写者队列，push→同步排空→状态更新）----

struct RawQueue {
    queue: Mutex<VecDeque<()>>,
    state: Mutex<u64>,
}

impl RawQueue {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            state: Mutex::new(0),
        }
    }
    /// 与 Store::dispatch 同形：入队→同步排空至 idle→状态更新。
    fn dispatch(&self) {
        self.queue.lock().unwrap().push_back(());
        while self.queue.lock().unwrap().pop_front().is_some() {
            *self.state.lock().unwrap() += 1;
        }
    }
    fn state(&self) -> u64 {
        *self.state.lock().unwrap()
    }
}

/// 裸基线+快照分配等价物：分离「锁机制税」与「快照即模型本体的 Arc::new 成本」。
struct RawSnap {
    queue: Mutex<VecDeque<()>>,
    state: Mutex<Arc<u64>>,
}

impl RawSnap {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            state: Mutex::new(Arc::new(0)),
        }
    }
    fn dispatch(&self) {
        self.queue.lock().unwrap().push_back(());
        while self.queue.lock().unwrap().pop_front().is_some() {
            let next = **self.state.lock().unwrap() + 1;
            *self.state.lock().unwrap() = Arc::new(next);
        }
    }
    fn state(&self) -> u64 {
        **self.state.lock().unwrap()
    }
}

// ---- 测量助手 ----

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// 单发采样分布：(p50, p99) ns。样本向量预分配，环内零新增分配。
fn sample_dist<T>(op: impl FnMut() -> T, n: usize) -> (u64, u64) {
    let mut v = Vec::with_capacity(n);
    collect_samples(op, n, &mut v);
    v.sort_unstable();
    (pct(&v, 50), pct(&v, 99))
}

fn collect_samples<T>(mut op: impl FnMut() -> T, n: usize, v: &mut Vec<u64>) {
    for _ in 0..n {
        let t = Instant::now();
        black_box(op());
        v.push(t.elapsed().as_nanos() as u64);
    }
}

/// 大批量均值（抽象税主口径）：ns/op。返回值黑箱化后弃置，防 LLVM 消除被测 op。
fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn main() {
    // 时钟对开销探针：双臂同担的口径注数字
    let n = 200_000u64;
    let mut acc = 0u64;
    let t = Instant::now();
    for _ in 0..n {
        let a = Instant::now();
        acc = acc.wrapping_add(a.elapsed().as_nanos() as u64);
    }
    black_box(acc);
    println!(
        "clock_pair_ns_mean={:.1}",
        t.elapsed().as_nanos() as f64 / n as f64
    );

    perf_dispatch_idle();
    perf_broadcast_1k();
    perf_snapshot();
    mem_store_subs_peak();
    concurrency_storm();
    perf_subs_contention_decay();
    perf_false_sharing_gates();
}

fn perf_dispatch_idle() {
    println!("== perf: dispatch_idle（空载 dispatch，零订阅零效果）==");
    const SAMPLES: usize = 20_000;
    const BATCH: u64 = 1_000_000;

    let store = Store::new(Count, Nop);
    for _ in 0..10_000 {
        store.dispatch(());
    }
    let (p50, p99) = sample_dist(|| store.dispatch(()), SAMPLES);
    let mean = batch_mean(|| store.dispatch(()), BATCH);
    assert_eq!(
        *store.snapshot(),
        10_000 + SAMPLES as u64 + BATCH,
        "dispatch 计数守恒"
    );
    println!("skeleton_dispatch_p50_ns={p50} skeleton_dispatch_p99_ns={p99} skeleton_dispatch_batch_mean_ns={mean:.1}");

    let raw = RawQueue::new();
    for _ in 0..10_000 {
        raw.dispatch();
    }
    let (rp50, rp99) = sample_dist(|| raw.dispatch(), SAMPLES);
    let rmean = batch_mean(|| raw.dispatch(), BATCH);
    assert_eq!(raw.state(), 10_000 + SAMPLES as u64 + BATCH);
    println!("raw_queue_p50_ns={rp50} raw_queue_p99_ns={rp99} raw_queue_batch_mean_ns={rmean:.1}");

    let raws = RawSnap::new();
    for _ in 0..10_000 {
        raws.dispatch();
    }
    let (sp50, sp99) = sample_dist(|| raws.dispatch(), SAMPLES);
    let smean = batch_mean(|| raws.dispatch(), BATCH);
    assert_eq!(raws.state(), 10_000 + SAMPLES as u64 + BATCH);
    println!("rawsnap_p50_ns={sp50} rawsnap_p99_ns={sp99} rawsnap_batch_mean_ns={smean:.1}");

    let tax_raw = (mean - rmean) / rmean * 100.0;
    let tax_snap = (mean - smean) / smean * 100.0;
    println!("tax_vs_raw_pct={tax_raw:.1} tax_vs_rawsnap_pct={tax_snap:.1}");
}

fn perf_broadcast_1k() {
    println!("== perf: broadcast_1k（1000 订阅者广播摊销，空回调）==");
    const SUBS: usize = 1_000;
    const BATCH: u64 = 2_000;

    let store = Store::new(Count, Nop);
    // 0 订阅对照（同店同期，消跨实例漂移）
    for _ in 0..100 {
        store.dispatch(());
    }
    let idle_mean = batch_mean(|| store.dispatch(()), 500_000);
    println!("idle_ref_batch_mean_ns={idle_mean:.1}");

    let mut tickets = Vec::with_capacity(SUBS);
    for _ in 0..SUBS {
        tickets.push(store.subscribe(|_| {}));
    }
    assert_eq!(store.subscriber_count(), SUBS);
    for _ in 0..100 {
        store.dispatch(());
    }
    let mean = batch_mean(|| store.dispatch(()), BATCH);
    let per_notify = mean / SUBS as f64;
    let marginal = (mean - idle_mean) / SUBS as f64;
    assert_eq!(*store.snapshot(), 200 + 500_000 + BATCH, "广播段计数守恒");
    println!(
        "dispatch_with_1k_subs_batch_mean_ns={mean:.1} per_notify_ns={per_notify:.3} marginal_per_sub_ns={marginal:.3}"
    );
    drop(tickets);
}

fn perf_snapshot() {
    println!("== perf: snapshot_share（快照拉取 = 锁+Arc::clone）==");
    let store = Store::new(Count, Nop);
    store.dispatch(());
    let (p50, p99) = sample_dist(|| black_box(store.snapshot()), 50_000);
    const BATCH: u64 = 1_000_000;
    let mean = batch_mean(|| black_box(store.snapshot()), BATCH);
    println!("snapshot_p50_ns={p50} snapshot_p99_ns={p99} snapshot_batch_mean_ns={mean:.1}");

    let snap = store.snapshot();
    let arc_mean = batch_mean(|| black_box(Arc::clone(&snap)), BATCH);
    println!("arc_clone_drop_batch_mean_ns={arc_mean:.1}");
}

fn mem_store_subs_peak() {
    println!("== mem: per-store / per-subscriber / 100k peak（串行上下文）==");
    let base_bytes = CountingAllocator::live_bytes();
    let base_allocs = CountingAllocator::live_allocs();

    let mstore = Store::new(Count, Nop);
    let per_store = CountingAllocator::live_bytes() - base_bytes;
    let per_store_allocs = CountingAllocator::live_allocs() - base_allocs;
    println!("per_store_resident_bytes={per_store} per_store_allocs_delta={per_store_allocs}");

    // 每订阅者增量：先建表防 Vec 扩容计入读数
    let mut tickets = Vec::with_capacity(1_000);
    let s0 = CountingAllocator::live_bytes();
    for _ in 0..1_000 {
        tickets.push(mstore.subscribe(|_| {}));
    }
    let per_sub_empty = (CountingAllocator::live_bytes() - s0) as f64 / 1_000.0;

    let mut tickets2 = Vec::with_capacity(1_000);
    let s1 = CountingAllocator::live_bytes();
    let shared = Arc::new(AtomicU64::new(0));
    for _ in 0..1_000 {
        let sh = Arc::clone(&shared);
        tickets2.push(mstore.subscribe(move |_| {
            sh.fetch_add(1, Ordering::Relaxed);
        }));
    }
    let per_sub_capture = (CountingAllocator::live_bytes() - s1) as f64 / 1_000.0;
    assert_eq!(mstore.subscriber_count(), 2_000);
    println!(
        "per_subscriber_empty_bytes={per_sub_empty:.1} per_subscriber_capturing_bytes={per_sub_capture:.1}"
    );

    // 注销回落与整店回落（泄漏判据的静态面）
    drop(tickets);
    drop(tickets2);
    drop(shared);
    let after_unsub = CountingAllocator::live_bytes() as isize - s0 as isize;
    // 死 Weak 代答回收验证（摘除发生在下次 dispatch 的通知收集阶段）：
    // 一次 dispatch 后死槽位必须归零回收——deferment 是有界簿记，非泄漏
    mstore.dispatch(());
    let after_prune = CountingAllocator::live_bytes() as isize - s0 as isize;
    assert_eq!(mstore.subscriber_count(), 0, "死 Weak 摘除后计数归零");
    drop(mstore);
    let after_drop = CountingAllocator::live_bytes() as isize - base_bytes as isize;
    println!(
        "unsub_reclaim_delta_bytes={after_unsub} after_prune_delta_bytes={after_prune} full_drop_reclaim_delta_bytes={after_drop}"
    );

    // 100k action 峰值+归零回落（泄漏判据的动态面）
    let store = Store::new(Count, Nop);
    for _ in 0..10_000 {
        store.dispatch(()); // 预热：VecDeque 容量等稳态就位
    }
    let live0 = CountingAllocator::live_bytes();
    CountingAllocator::reset_peak();
    let mut mid = [0isize; 3];
    for i in 0..100_000u64 {
        store.dispatch(());
        if i == 24_999 {
            mid[0] = CountingAllocator::live_bytes() as isize - live0 as isize;
        }
        if i == 49_999 {
            mid[1] = CountingAllocator::live_bytes() as isize - live0 as isize;
        }
        if i == 74_999 {
            mid[2] = CountingAllocator::live_bytes() as isize - live0 as isize;
        }
    }
    let peak_delta = CountingAllocator::peak_bytes() as isize - live0 as isize;
    let after_delta = CountingAllocator::live_bytes() as isize - live0 as isize;
    assert_eq!(*store.snapshot(), 110_000);
    println!(
        "peak_100k_delta_bytes={peak_delta} mid_live_delta_bytes=[{}, {}, {}] live_after_delta_bytes={after_delta}",
        mid[0], mid[1], mid[2]
    );
}

fn concurrency_storm() {
    println!("== conc: 8 线程 × 10k dispatch 风暴 × 20 轮（pass^k）==");
    const THREADS: usize = 8;
    const PER_THREAD: u64 = 10_000;
    const ROUNDS: usize = 20;
    const EXPECTED: u64 = THREADS as u64 * PER_THREAD;

    let mut pass = 0usize;
    let mut storm_elapsed_ns = 0u128;
    for round in 0..ROUNDS {
        let store = Arc::new(Store::new(Count, Nop));
        let notified = Arc::new(AtomicU64::new(0));
        let ticket = {
            let n = Arc::clone(&notified);
            store.subscribe(move |s| {
                n.fetch_add(1, Ordering::Relaxed);
                black_box(s);
            })
        };

        // 壳不变量监视线程：快照单调非降 + 值域 + 订阅表规模
        let stop = Arc::new(AtomicBool::new(false));
        let violations = Arc::new(AtomicU64::new(0));
        let monitor = {
            let st = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            let viol = Arc::clone(&violations);
            std::thread::spawn(move || {
                let mut last = 0u64;
                let mut iters = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let v = *st.snapshot();
                    if v < last || v > EXPECTED {
                        viol.fetch_add(1, Ordering::Relaxed);
                    }
                    if st.subscriber_count() != 1 {
                        viol.fetch_add(1, Ordering::Relaxed);
                    }
                    last = v;
                    iters += 1;
                    if iters.is_multiple_of(8) {
                        std::thread::yield_now();
                    }
                }
                let v = *st.snapshot();
                if v < last {
                    viol.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        let t0 = Instant::now();
        let mut hs = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let st = Arc::clone(&store);
            hs.push(std::thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    st.dispatch(());
                }
            }));
        }
        for h in hs {
            h.join().expect("storm thread panicked");
        }
        storm_elapsed_ns += t0.elapsed().as_nanos();
        stop.store(true, Ordering::Relaxed);
        monitor.join().unwrap();

        let final_state = *store.snapshot();
        let notify_total = notified.load(Ordering::Relaxed);
        let viol = violations.load(Ordering::Relaxed);
        if final_state == EXPECTED && notify_total == EXPECTED && viol == 0 {
            pass += 1;
        } else {
            println!("ROUND {round} FAIL: final={final_state} notify={notify_total} viol={viol}");
        }
        drop(ticket);

        // 闭门账目（双路径）：close 后 100 dispatch + 10 send 恒拒、全量入账、快照冻结
        store.close();
        let frozen = *store.snapshot();
        let mut hs = Vec::with_capacity(4);
        for _ in 0..4 {
            let st = Arc::clone(&store);
            hs.push(std::thread::spawn(move || {
                for _ in 0..25 {
                    st.dispatch(());
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let sender = store.sender();
        let mut send_rejected = 0;
        for _ in 0..10 {
            if !sender.dispatch(()) {
                send_rejected += 1;
            }
        }
        assert_eq!(*store.snapshot(), frozen, "闭门后快照冻结");
        assert_eq!(send_rejected, 10, "send 闭门显式 false");
        assert_eq!(
            store.rejected_after_close(),
            100 + send_rejected as u64,
            "闭门拒绝全量入账（dispatch+send 双路径）"
        );
    }
    let throughput = ROUNDS as f64 * EXPECTED as f64 / (storm_elapsed_ns as f64 / 1e9);
    println!(
        "storm_rounds_pass={pass}/{ROUNDS} storm_throughput_ops_per_s={throughput:.0} close_gate_ok=true"
    );
    assert_eq!(pass, ROUNDS, "pass^k：全轮零违例才达标");
}

// ---- runtime 域扩段（perf-audit P1-3：③ 订阅争用衰减 + ⑦ 伪共享专项）----

/// ③ 扩展性：多订阅者争用吞吐衰减曲线。8 线程争用 dispatch（concurrency_storm
/// 同形 harness），订阅者 K∈{0,1,2,4,8}（空回调，隔离「通知面成本」单变量），
/// 5 轮取中位吞吐。口径沿用本文件口径注①-④；并发段只断言计数守恒不计内存。
fn perf_subs_contention_decay() {
    println!("== conc: 订阅争用衰减（8 线程 × 10k dispatch × K 订阅者，5 轮中位吞吐）==");
    const THREADS: usize = 8;
    const PER_THREAD: u64 = 10_000;
    const ROUNDS: usize = 5;
    let expected = THREADS as u64 * PER_THREAD;

    let mut base_ops_per_s = 0.0f64;
    let mut last_per_dispatch_ns = 0.0f64;
    let mut base_per_dispatch_ns = 0.0f64;
    for subs in [0usize, 1, 2, 4, 8] {
        let mut rounds: Vec<u64> = Vec::with_capacity(ROUNDS);
        for _ in 0..ROUNDS {
            let store = Arc::new(Store::new(Count, Nop));
            let mut tickets = Vec::with_capacity(subs);
            for _ in 0..subs {
                tickets.push(store.subscribe(|_| {}));
            }
            assert_eq!(store.subscriber_count(), subs);
            let t0 = Instant::now();
            let mut hs = Vec::with_capacity(THREADS);
            for _ in 0..THREADS {
                let st = Arc::clone(&store);
                hs.push(std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        st.dispatch(());
                    }
                }));
            }
            for h in hs {
                h.join().expect("decay thread panicked");
            }
            rounds.push(t0.elapsed().as_nanos() as u64);
            assert_eq!(*store.snapshot(), expected, "衰减段计数守恒");
            drop(tickets);
        }
        rounds.sort_unstable();
        let med = rounds[ROUNDS / 2];
        let ops_per_s = expected as f64 * 1e9 / med as f64;
        let per_dispatch_ns = 1e9 / ops_per_s;
        let decay_pct = if subs == 0 {
            0.0
        } else {
            (base_ops_per_s - ops_per_s) / base_ops_per_s * 100.0
        };
        println!(
            "subs={subs:>2} throughput_ops_per_s={ops_per_s:.0} per_dispatch_ns={per_dispatch_ns:.1} decay_vs_0_pct={decay_pct:.1}"
        );
        if subs == 0 {
            base_ops_per_s = ops_per_s;
            base_per_dispatch_ns = per_dispatch_ns;
        }
        last_per_dispatch_ns = per_dispatch_ns;
    }
    let marginal = (last_per_dispatch_ns - base_per_dispatch_ns) / 8.0;
    println!("implied_marginal_per_sub_ns(0→8, contended)={marginal:.1}");
}

/// ⑦ 伪共享代理形：StoreInner 内联热词对（runtime.rs 声明序 draining/closed
/// 紧邻，gate 镜像 local_subs 闸计数——真实布局 gate 经 Arc 独立分配，此臂
/// 并入内联是「最坏打包」对照）。真实布局中 subs_version/local_subs/audit_gate/
/// subs/audit 五个热词均 Arc 装载（独立分配，结构性不与内联字段同线），不测。
/// queue/state 两把 Mutex 的 CAS 争用是有意争用（锁本体语义），不入本对照。
mod gateshapes {
    use std::hint::black_box;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    // repr(C)：被测机制就是字段邻接/垫片隔离，布局必须按声明落。
    #[repr(C)]
    pub struct Adjacent {
        pub closed: AtomicBool, // 读热：dispatch is_closed 闸 + 锁内复检（Acquire load）
        pub draining: AtomicBool, // 写热：夺权 swap(AcqRel) + 出口放旗 store(Release)
        pub gate: AtomicUsize,  // 读热：local_subs 闸（drain 每轮 Relaxed load）
    }

    #[repr(align(64))]
    pub struct Pad(pub ());

    #[repr(C)]
    pub struct Isolated {
        pub closed: AtomicBool,
        pub _p0: Pad,
        pub draining: AtomicBool,
        pub _p1: Pad,
        pub gate: AtomicUsize,
        pub _p2: Pad,
    }

    /// 写混：夺权/放旗（DrainGuard 生命周期镜像：swap 夺权、出口 Release 放旗）。
    #[inline]
    pub fn write_mix(draining: &AtomicBool) {
        if !draining.swap(true, Ordering::AcqRel) {
            draining.store(false, Ordering::Release);
        }
    }

    /// 读混：停机闸 + 本地闸（dispatch/drain 读侧镜像，黑箱防消除）。
    #[inline]
    pub fn read_mix(closed: &AtomicBool, gate: &AtomicUsize) {
        black_box(closed.load(Ordering::Acquire));
        black_box(gate.load(Ordering::Relaxed));
    }

    /// dispatch 热路径原子面镜像：读闸 + 夺权/放旗 + 本地闸读。
    #[inline]
    pub fn dispatch_atomic_mix(closed: &AtomicBool, draining: &AtomicBool, gate: &AtomicUsize) {
        read_mix(closed, gate);
        write_mix(draining);
    }
}

/// ⑦ 伪共享防：热词对相邻 vs align(64) 垫片隔离（沿 memory_pool_profile ⑦
/// 对照法）。两臂：2 线程不对称对撞（写混×读闸）+ 8 线程全混 hammer（风暴形态
/// 镜像）。纯 std 原子，零分配热窗，内存段不适用。
fn perf_false_sharing_gates() {
    println!("== perf: false_sharing_gates（⑦ 内联热词 draining/closed/gate：相邻 vs 垫片隔离）==");
    use gateshapes::{dispatch_atomic_mix, Adjacent, Isolated};

    /// 双线程对撞：一线程写混（夺权/放旗），一线程读闸（closed+gate）。
    /// ns/op = 总墙钟摊到每 op（两线程等 op 数，口径同 memory_pool_profile ⑦）。
    fn bounce(write: impl Fn() + Send + 'static, read: impl Fn() + 'static, iters: u64) -> f64 {
        let t0 = Instant::now();
        let h = std::thread::spawn(move || {
            for _ in 0..iters {
                write();
            }
        });
        for _ in 0..iters {
            read();
        }
        h.join().unwrap();
        t0.elapsed().as_nanos() as f64 / iters as f64
    }

    /// N 线程同撞完整 dispatch 原子面（读闸+夺权/放旗+本地闸读），ns/op。
    fn hammer(n: usize, iters_per: u64, mix: impl Fn() + Send + Sync + 'static) -> f64 {
        let mix = Arc::new(mix);
        let t0 = Instant::now();
        let mut hs = Vec::with_capacity(n);
        for _ in 0..n {
            let m = Arc::clone(&mix);
            hs.push(std::thread::spawn(move || {
                for _ in 0..iters_per {
                    m();
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        t0.elapsed().as_nanos() as f64 / (n as u64 * iters_per) as f64
    }

    const ITERS: u64 = 5_000_000;
    const HAMMER_PER: u64 = 1_000_000;

    let adj = Arc::new(Adjacent {
        closed: AtomicBool::new(false),
        draining: AtomicBool::new(false),
        gate: AtomicUsize::new(0),
    });
    let adjacent_bounce = {
        let w = Arc::clone(&adj);
        let r = Arc::clone(&adj);
        bounce(
            move || gateshapes::write_mix(&w.draining),
            move || gateshapes::read_mix(&r.closed, &r.gate),
            ITERS,
        )
    };
    let adj8 = {
        let a = Arc::clone(&adj);
        hammer(8, HAMMER_PER, move || {
            dispatch_atomic_mix(&a.closed, &a.draining, &a.gate)
        })
    };

    let iso = Arc::new(Isolated {
        closed: AtomicBool::new(false),
        _p0: gateshapes::Pad(()),
        draining: AtomicBool::new(false),
        _p1: gateshapes::Pad(()),
        gate: AtomicUsize::new(0),
        _p2: gateshapes::Pad(()),
    });
    let isolated_bounce = {
        let w = Arc::clone(&iso);
        let r = Arc::clone(&iso);
        bounce(
            move || gateshapes::write_mix(&w.draining),
            move || gateshapes::read_mix(&r.closed, &r.gate),
            ITERS,
        )
    };
    let iso8 = {
        let a = Arc::clone(&iso);
        hammer(8, HAMMER_PER, move || {
            dispatch_atomic_mix(&a.closed, &a.draining, &a.gate)
        })
    };

    println!(
        "⑦ gate bounce (2thr × {ITERS}, write-mix × read-gate): adjacent={adjacent_bounce:.1} ns/op isolated(pad)={isolated_bounce:.1} ns/op ratio={:.2}x",
        adjacent_bounce / isolated_bounce
    );
    println!(
        "⑦ gate hammer (8thr × {HAMMER_PER}, full atomic mix): adjacent={adj8:.1} ns/op isolated(pad)={iso8:.1} ns/op ratio={:.2}x",
        adj8 / iso8
    );
    println!("⑦ product 布局=adjacent 形（StoreInner 内联紧邻）；若风暴吞吐需此余量，热词拆线（Pad/Arc 化）为候选改法——本采集器只留痕不改布局。");
}
