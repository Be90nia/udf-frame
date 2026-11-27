//! infra bench：执行/账本三组（fxscope_spawn_join / journal_append_line /
//! effect_steady_state）+ 池维度四组（蓝图 §11 第 9 项：借还热路径、饱和拒绝、
//! 字节预算争用）。
//!
//! 注意：CountingAllocator 由下方 `#[global_allocator]` 声明为**本二进制的**全局
//! 分配器（effect_steady_state 预算闸与全部 churn 读数的前提）——组计时同受每
//! 分配 2 次原子加的噪声；预算门为 10µs 量级，余量充足。**声明此前缺失**：计数
//! 恒 0 ⇒ 两组预算闸退化成 `0 - 0 == 0` 恒真（2026-11-23 补上，并由
//! `assert_allocator_instrumented` 在首个布局组 setup 把前提钉住）。
//!
//! State 布局指导证据（issue#53 / bd udf-frame-68t）三组：
//! `state_layout_update`（整体 vs 分片 × 总量 × 留存）、
//! `state_layout_update_scope`（选择性 vs 整体更新 + 分片粒度）、
//! `state_layout_retention`（陈旧长持 vs 滚动窗口 + 内存增长）。
//!
//! **只产数据**：池组不设阈值、不写机器相关基线注释（§9 性能实践——基线走
//! `perf.yml` 滚动 + artifacts）；本机实测数字落在 verify 报告里，不进源码。
//!
//! 多生产者准入/完成面（issue#96 / bd udf-frame-7is）两组：
//! `multi_producer_admission`（`try_dispatch` 纯准入）与
//! `multi_producer_dispatch`（`dispatch_with_receipt(..).wait()` 端到端）——
//! 矩阵（baseline / contention / bounded / slow reducer / causal feedback）与
//! 装置纪律见下方该节头注。
//!
//! 跑法（required-features 并集，见 Cargo.toml）：
//! `cargo bench -p udf-skeleton-infra --all-features --bench infra`

use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

use skeleton::notify::{Notify as _, NotifySink};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{Effect, EffectBuf, Infallible, Reducer, Store as _};
use skeleton_infra::fx::FxScope;
use skeleton_infra::memgov::{CountingAllocator, MemoryPool};
use skeleton_infra::objpool::{ByteBudget, ObjectPool};
use skeleton_infra::pool::{
    AbortPolicy, CallerRunsPolicy, RejectPolicy, ThreadPool, ThreadPoolTuning,
};
use skeleton_infra::runtime::{AdmissionOutcome, DispatchOutcome, SyncStore};
use skeleton_infra::view::{
    Coverage, Encoders, IntentError, JsonEncoder, RegionBacking, RegionEncoder, RegionWriter,
    VecBacking, ViewBridge,
};
use skeleton_infra::{Journal, JournalError, JsonlJournal};

// 分配面计量前提：CountingAllocator 必须是本二进制的全局分配器，否则
// live_*/total_* 恒 0（预算闸与 churn 表全部失真）。
#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// 计量前提闸：一次真实分配必须被记到。**不是**阈值——是「表在不在」的开关。
fn assert_allocator_instrumented() {
    let before = CountingAllocator::total_allocs();
    let probe = Arc::new(vec![0u8; 4096]);
    black_box(&probe);
    assert!(
        CountingAllocator::total_allocs() > before,
        "CountingAllocator 未装配为全局分配器：分配计量恒 0，预算闸 / churn 读数全部失真"
    );
}

// ---- effect_steady_state 的最小效果域（单效果 dispatch 稳态，零订阅者）----

#[derive(Clone, Debug)]
struct Hits(u64);

enum Act {
    Tick,
}

enum Beep {
    One,
}

struct Producer;

impl skeleton::Reducer for Producer {
    type State = Hits;
    type Action = Act;
    type Op = Beep;

    fn initial(&self) -> Hits {
        Hits(0)
    }

    fn reduce(&self, s: &Hits, _a: Act, out: &mut skeleton::EffectBuf<Act, Beep>) -> Hits {
        out.push(Effect::Run {
            id: skeleton::EffectId::new(s.0 + 1),
            op: Beep::One,
        });
        Hits(s.0 + 1)
    }
}

struct Sink;

impl skeleton::EffectRuntime<Act, Beep> for Sink {
    fn run(&mut self, _effect: Effect<Act, Beep>, _back: &skeleton::ActionSender<Act>) {}
}

fn effect_steady_state(c: &mut Criterion) {
    let store = SyncStore::new(Producer, Sink);
    // 预热：队列/EffectBuf/通知缓冲到稳态容量（此后 dispatch 恒复用）。
    for _ in 0..1_000u64 {
        store.dispatch(Act::Tick);
    }
    // 预算闸：稳态每 dispatch 净新增分配=0——快照
    // `Arc::new(next)` 一配一释自抵消（快照即模型本体，口径扣除后=零新增）。
    // 口径注：live_allocs() 为净存活计数，净零无法区分「除快照外零分配」与
    // 「同量 churn」——churn 对账面已出厂（`CountingAllocator::total_allocs`/
    // `total_deallocs`，churn=total 差值）；本闸仍用 live 净额（预算=无净增量，
    // churn 辨识归采集器内存锚段双口径）。
    let before = CountingAllocator::live_allocs();
    store.dispatch(black_box(Act::Tick));
    assert_eq!(
        CountingAllocator::live_allocs() - before,
        0,
        "effect path steady state exceeded budget: nonzero net allocations per dispatch"
    );
    c.bench_function("effect_steady_state", |b| {
        b.iter(|| store.dispatch(black_box(Act::Tick)))
    });
}

// ---- effect_batch_forward：批量效果转发（每拍 k 条效果）----
//
// 每拍 reducer 产 k 条 Run 效果，转发环逐条摘除。旧形态 `Vec::remove(0)`
// 摘队首把尾部整体前移一格 ⇒ 排空 k 条共 k(k-1)/2 次元素搬移（O(k²)）；
// 环形缓冲摘队首零搬移（issue#43）。
//
// 本组**只产墙钟读数**：这条路径的墙钟被布局噪声主导，纳秒级读数不作证据；
// 元素搬移数与分配面的计数证据见 skeleton crate 单元测（元素搬移计数）与
// `tests/effect_fifo.rs`（分配事件面）。同码 dup 臂作噪声锚（byte-budget 臂
// 同款纪律：同 run 内两者一致 ⇒ 读数可信）。
struct BatchProducer {
    per_action: u64,
}

impl skeleton::Reducer for BatchProducer {
    type State = u64;
    type Action = u64;
    type Op = Beep;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, _a: u64, out: &mut skeleton::EffectBuf<u64, Beep>) -> u64 {
        for i in 0..self.per_action {
            out.push(Effect::Run {
                // id 每拍复用：历史 Run-ID 诊断默认关闭（issue#62）⇒ 本臂不登记
                // 任何 run id；启用后才是「有界最近集」（不再是只增集合）。
                id: skeleton::EffectId::new(i + 1),
                op: Beep::One,
            });
        }
        *s + 1
    }
}

fn effect_batch_forward(c: &mut Criterion) {
    let mut group = c.benchmark_group("effect_batch_forward");
    for k in [1u64, 4, 16, 64, 256, 1024, 4096] {
        let store = SyncStore::new(BatchProducer { per_action: k }, NullFx);
        // 预热：缓冲/队列到稳态容量（此后 dispatch 恒复用，无新增分配）。
        for _ in 0..10 {
            store.dispatch(1u64);
        }
        group.bench_function(format!("k{k}"), |b| {
            b.iter(|| store.dispatch(black_box(1u64)))
        });
        // 同码 dup 臂作噪声锚（byte-budget 臂同款纪律）。
        group.bench_function(format!("k{k}_dup"), |b| {
            b.iter(|| store.dispatch(black_box(1u64)))
        });
    }
    group.finish();
}

// ---- journal_append_line：BufWriter 缓冲态单行 append（无逐行 flush）----

#[derive(Debug, Serialize, Deserialize)]
struct BenchAction {
    id: u64,
    label: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct BenchState {
    seen: u64,
}

// 批 6A 关联类型化：A/S 由 Self 钉死，append 直呼零标注。
fn append_one(
    journal: &mut JsonlJournal<BenchAction, BenchState>,
    action: &BenchAction,
) -> Result<(), JournalError> {
    journal.append_action(action)
}

fn journal_append_line(c: &mut Criterion) {
    // 临时件归 crate 构建隔离目录，禁系统 TEMP/用户目录。
    let dir = Path::new("../../target-iso/bench-journal");
    std::fs::create_dir_all(dir).expect("mkdir target-iso/bench-journal");
    let path = dir.join("bench.jsonl");
    let _ = std::fs::remove_file(&path); // create_new 撞残留=Err，先清
    let mut journal =
        JsonlJournal::<BenchAction, BenchState>::create(&path).expect("create journal");
    let action = BenchAction {
        id: 1,
        label: "bench".to_string(),
    };

    c.bench_function("journal_append_line", |b| {
        b.iter(|| append_one(&mut journal, black_box(&action)).expect("append ok"))
    });

    journal.flush().expect("flush ok");
    drop(journal);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir(dir);
}

fn fxscope_spawn_join(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    let scope = FxScope::new();
    c.bench_function("fxscope_spawn_join", |b| {
        // JoinSet::spawn 需运行时上下文（裸调 panic）——spawn+join 同闭包内 block_on，
        // 基线含每迭代一次 block_on 进出开销（current_thread 亚 µs 级）
        b.iter(|| {
            rt.block_on(async {
                let handle = scope.spawn_tracked(async { 1u8 }).expect("scope open");
                handle.join().await.expect("task ok")
            })
        })
    });
}

// ---- dispatch 每拍：命中 vs Cancel(unknown id) ----
//
// 对照 PerfProbe（`verify/perf-probe/` 的 `dispatch` 组，同机同树）：criterion
// 默认采样口径取数，与 probe 的 7 轮中位量级可比、绝对值随机器/负载漂移。
// 本机实测（i9-9900K@3.60GHz / Win 10.0.26200，criterion 默认采样中位）：
//   hit ≈ 82.0 ns/拍；cancel_unknown_id ≈ 114.8 ns/拍（差 ~33 ns = run_ids
//   查表 + miss 留痕）。两臂之差即效果转发路径的每拍成本。

/// 命中热路径：零效果、（下方 notify 组里另有零订阅/1 订阅两臂）。
struct Hit;

impl skeleton::Reducer for Hit {
    type State = u64;
    type Action = u64;
    type Op = Beep;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, _a: u64, _out: &mut skeleton::EffectBuf<u64, Beep>) -> u64 {
        s + 1
    }
}

/// Cancel 每拍：未知 id ⇒ 每次 dispatch 走 run_ids 查表 + miss 留痕路径。
struct CancelEachBeat;

impl skeleton::Reducer for CancelEachBeat {
    type State = u64;
    type Action = u64;
    type Op = Beep;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, s: &u64, _a: u64, out: &mut skeleton::EffectBuf<u64, Beep>) -> u64 {
        out.push(Effect::Cancel {
            id: skeleton::EffectId::new(999),
        });
        s + 1
    }
}

/// 空解释器（`Hit`/`CancelEachBeat` 的动作域）。
struct NullFx;

impl skeleton::EffectRuntime<u64, Beep> for NullFx {
    fn run(&mut self, _effect: Effect<u64, Beep>, _back: &skeleton::ActionSender<u64>) {}
}

/// 投递口：只落一个原子（O(1) 契约的最省形态，不引入测量噪声）。
struct Latest(Arc<AtomicU64>);

impl NotifySink for Latest {
    fn publish(&self, version: u64) {
        self.0.store(version, Ordering::Relaxed);
    }
}

fn dispatch_per_beat(c: &mut Criterion) {
    let mut group = c.benchmark_group("dispatch_per_beat");

    let hit = SyncStore::new(Hit, NullFx);
    for _ in 0..1_000u64 {
        hit.dispatch(0); // 预热：队列/缓冲到稳态容量
    }
    group.bench_function("hit", |b| b.iter(|| hit.dispatch(black_box(0))));
    hit.close();

    let cancel = SyncStore::new(CancelEachBeat, NullFx);
    for _ in 0..1_000u64 {
        cancel.dispatch(0);
    }
    // ⚠️ 2026-11-23（issue#62）语义变更：历史 Run-ID 诊断**默认关闭** ⇒ 本臂
    // 现在测的是「Cancel 投递的门闸-only 路径」（不再有 run_ids 查表 + miss
    // 留痕）。ID 保留以延续基线曲线，但**跨该日期的基线不可直接对比**；
    // 启用诊断的路径**未加臂**（未覆盖）。
    group.bench_function("cancel_unknown_id", |b| {
        b.iter(|| cancel.dispatch(black_box(0)))
    });
    cancel.close();

    group.finish();
}

// ---- notify 异步档：0 / 1 订阅者 ----
//
// 对照 PerfProbe（`verify/perf-probe/` 的 `notify` 组）：零异步订阅者 = 一次
// Acquire load 即返（与命中同量级）；1 订阅者 = 表锁收集 + 锁外 publish，
// 增量即投递成本。**分配面**：修复（2026-10-28）前每拍 `collect` 新建 Vec，
// total = 1.00 alloc/拍——下方预算闸钉住「有订阅者稳态零净分配」。
// 本机实测（同机同树，criterion 默认采样中位）：no_subscriber ≈ 86.0 ns/拍
// （= 命中 + 一次门控 load）；one_subscriber ≈ 114.5 ns/拍（增量 ~28 ns =
// 表锁收集 + publish）。
fn notify_dispatch(c: &mut Criterion) {
    let mut group = c.benchmark_group("notify_dispatch");

    let none = SyncStore::new(Hit, NullFx);
    for _ in 0..1_000u64 {
        none.dispatch(0);
    }
    group.bench_function("no_subscriber", |b| b.iter(|| none.dispatch(black_box(0))));
    none.close();

    let one = SyncStore::new(Hit, NullFx);
    let seen = Arc::new(AtomicU64::new(0));
    let _ticket = one.subscribe_notify(Latest(Arc::clone(&seen)));
    for _ in 0..1_000u64 {
        one.dispatch(0); // 预热：收集缓冲首撞容量（此后跨拍复用）
    }
    let before = CountingAllocator::live_allocs();
    one.dispatch(black_box(0));
    assert_eq!(
        CountingAllocator::live_allocs() - before,
        0,
        "1 订阅者稳态 dispatch 出现净分配：收集缓冲复用退化了"
    );
    group.bench_function("one_subscriber", |b| b.iter(|| one.dispatch(black_box(0))));
    drop(_ticket);
    one.close();

    group.finish();
}

// ---- 池维度（蓝图 §11 第 9 项）：借还热路径 / 饱和拒绝 / 字节预算争用 ----
//
// 统一口径：`Throughput::Elements(1)` 让输出同时给吞吐与单次成本；闭包内显式
// `drop(句柄)`，量的是「借出 + RAII 归还」一次完整往返。本节只产数据——阈值与
// 回归判据归 perf.yml 的滚动基线与 conformance 套件，不写进 bench。

// ---- 池维度①：ObjectPool 借还热路径（借用句柄，复用件）----

fn object_pool_acquire_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("object_pool_acquire_drop");
    group.throughput(Throughput::Elements(1));

    let pool = ObjectPool::new(16, 8, || Vec::<u8>::with_capacity(256));
    // 预热：留一件常驻 idle ⇒ 计时落在「pop 闲槽 + 归还留槽」的复用路径
    //（工厂新建/扫片/降级三路另有 tests/object_pool.rs 的语义闸，这里不混）。
    drop(pool.acquire());

    group.bench_function("reuse_round_trip", |b| {
        b.iter(|| {
            let buf = pool.acquire();
            black_box(&*buf);
            drop(buf);
        })
    });

    group.finish();
}

// ---- 池维度②：MemoryPool 借还（闲槽命中 vs 空池新建）----

fn memory_pool_acquire_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pool_acquire_drop");
    group.throughput(Throughput::Elements(1));
    let timeout = Duration::from_millis(50);

    // 闲槽命中：先还一件让其常驻 idle（此后每轮都命中同一槽）。
    let warm: MemoryPool<Vec<u8>> = MemoryPool::new(8, 4, Vec::new);
    drop(warm.acquire(timeout).expect("bench setup: warm idle slot"));
    group.bench_function("idle_hit_round_trip", |b| {
        b.iter(|| {
            let buf = warm.acquire(timeout).expect("idle slot available");
            black_box(&*buf);
            drop(buf);
        })
    });

    // 空池新建：setup 建池（不计时），计时 = 空池首借（工厂新建 + 配额记账）+
    // 归还——池每轮是新的，故恒走新建路径，无闲槽可命中。
    group.bench_function("empty_pool_create_round_trip", |b| {
        b.iter_batched(
            || MemoryPool::<Vec<u8>>::new(8, 4, Vec::new),
            |pool| {
                let buf = pool.acquire(Duration::ZERO).expect("fresh pool has quota");
                black_box(&*buf);
                drop(buf);
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

// ---- 池维度③：饱和 / 拒绝路径（只测「提交被拒」的处置成本）----

/// 饱和池装置：core = max = 1 + queue_cap = 1——一件闸任务占住唯一 worker
/// （sleep 等放行，不烧 CPU）、队列再塞一件 ⇒ 后续 `execute` 必走拒绝策略
/// （队列满且 `workers == max`，不会走 first-task 交接扩容）。
///
/// 返回放行闸：置 true 让闸任务退出，再由 [`drain_pool`] 收干净。
fn saturated_pool(policy: Arc<dyn RejectPolicy>) -> (ThreadPool, Arc<AtomicBool>) {
    let pool = ThreadPool::with_policy(ThreadPoolTuning::new(1, 1, 1), policy);
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));

    let (started_task, release_task) = (Arc::clone(&started), Arc::clone(&release));
    pool.execute(move || {
        started_task.store(true, Ordering::Release);
        while !release_task.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    })
    .expect("bench setup: gate task queued");

    while !started.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
    pool.execute(|| {})
        .expect("bench setup: filler task queued");

    (pool, release)
}

/// 拒绝路径守卫（**非性能阈值**）：饱和池上首轮提交必须让 `rejected` 前进一格。
/// 不设此闸则「池其实没饱和」时测的是入队路径，数字看着更快且完全失真。
fn expect_rejection(pool: &ThreadPool) {
    let before = pool.metrics().rejected;
    let _ = pool.execute(|| {});
    assert_eq!(
        pool.metrics().rejected,
        before + 1,
        "bench setup: saturated pool must reject (queue full + workers == max)"
    );
}

/// 收干净：放行闸任务 → shutdown → join（worker 不悬空）。
fn drain_pool(pool: &ThreadPool, release: &AtomicBool) {
    release.store(true, Ordering::Release);
    pool.shutdown();
    pool.join();
}

fn thread_pool_reject(c: &mut Criterion) {
    let mut group = c.benchmark_group("thread_pool_reject");
    group.throughput(Throughput::Elements(1));

    // Abort：拒绝 = 任务原样退还 + 拒收句柄析构（Box 擦除与锁内裁决都已计入）。
    let (abort_pool, abort_release) = saturated_pool(Arc::new(AbortPolicy));
    expect_rejection(&abort_pool);
    group.bench_function("abort", |b| {
        b.iter(|| {
            drop(
                abort_pool
                    .execute(|| {})
                    .expect_err("saturated pool rejects"),
            )
        })
    });
    drain_pool(&abort_pool, &abort_release);

    // CallerRuns：拒绝 = 提交线程内联执行（任务体是空闭包 ⇒ 只剩策略调用本身，
    // 用户任务体的成本不在本臂读数内）。
    let (caller_pool, caller_release) = saturated_pool(Arc::new(CallerRunsPolicy));
    expect_rejection(&caller_pool);
    group.bench_function("caller_runs", |b| {
        b.iter(|| {
            caller_pool
                .execute(|| {})
                .expect("caller-runs digests the task")
        })
    });
    drain_pool(&caller_pool, &caller_release);

    group.finish();
}

// ---- 池维度④：字节预算争用（共享 Arc<ByteBudget> 的账本锁成本）----
//
// 双臂 × 两线程度的 2×2 对照——「预算 vs 无预算」差价 = 归还 `try_take` / 借出
// `give_back` 的互斥量成本，「1 线程 vs 4 线程」差价 = 该锁的争用放大。预算
// 64KiB 容纳全部 idle 槽（16 × 256B），故本组不走超预算丢弃路径。
// 争用臂的吞吐列是「争用下的单线程成本」，不是聚合吞吐（陪跑线程不计入）。

fn byte_budget_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("byte_budget_contention");
    group.throughput(Throughput::Elements(1));

    let budget = Arc::new(ByteBudget::new(64 * 1024));
    let plain = ObjectPool::new(64, 16, || Vec::<u8>::with_capacity(256));
    let budgeted = ObjectPool::with_shared_byte_quota(
        64,
        16,
        || Vec::<u8>::with_capacity(256),
        Arc::clone(&budget),
        |buf: &Vec<u8>| buf.capacity(),
    );
    drop(plain.acquire());
    drop(budgeted.acquire());

    // 四臂同一往返形态，避免形态差异混进对照。
    let round_trip = |pool: &ObjectPool<Vec<u8>>| {
        let buf = pool.acquire();
        black_box(&*buf);
        drop(buf);
    };

    group.bench_function("unbudgeted_single_thread", |b| {
        b.iter(|| round_trip(&plain))
    });
    group.bench_function("budgeted_single_thread", |b| {
        b.iter(|| round_trip(&budgeted))
    });

    let stop_plain = AtomicBool::new(false);
    std::thread::scope(|s| {
        for _ in 0..3 {
            s.spawn(|| {
                while !stop_plain.load(Ordering::Relaxed) {
                    round_trip(&plain);
                }
            });
        }
        group.bench_function("unbudgeted_four_threads", |b| b.iter(|| round_trip(&plain)));
        // 同码 dup 臂作噪声锚：同 run 内两者一致 ⇒ 读数可信；跨 run 同一臂跳动 ⇒ 噪声在 run 之间（线程落点），
        // 不是稳定代码属性。判别「噪声 vs 效应」的实测依据（bd udf-frame-p6x）。
        group.bench_function("unbudgeted_four_threads_dup", |b| {
            b.iter(|| round_trip(&plain))
        });
        stop_plain.store(true, Ordering::Release);
    });

    let stop_budgeted = AtomicBool::new(false);
    std::thread::scope(|s| {
        for _ in 0..3 {
            s.spawn(|| {
                while !stop_budgeted.load(Ordering::Relaxed) {
                    round_trip(&budgeted);
                }
            });
        }
        group.bench_function("budgeted_four_threads", |b| {
            b.iter(|| round_trip(&budgeted))
        });
        // 同码 dup 臂作噪声锚（同上）。
        group.bench_function("budgeted_four_threads_dup", |b| {
            b.iter(|| round_trip(&budgeted))
        });
        stop_budgeted.store(true, Ordering::Release);
    });

    group.finish();
}

// ---- State 布局指导证据（issue#53 / bd udf-frame-68t）----
//
// 维护者定稿（issue#53 评论 2026-11-23）：本组只产**决策证据**——布局指导是给
// 消费方的通用建议（更新局部性 / 快照留存 / Fragment 组合），不是框架强制的
// 唯一表示形态。矩阵覆盖会改变决策的维度：整体 vs 分片、总量与分片粒度、
// 选择性 vs 整体更新、留存形态与内存增长。**含碎片化开销占优的反例**（小状态、
// 过度分片、整片更新三处整体态更快），禁读成「分片总是更快」。
//
// 装置口径（跨臂一致）：
// ① reducer 恒做一次状态构造（整体态 = 全量 `Vec` 拷贝；分片态 = 根
//    `Vec<Arc>` 浅拷贝 + 仅被改动分片的 `Arc::make_mut`）——值语义 reducer 的
//    固有税，与框架侧 `Arc::make_mut` 发布克隆分开读；
// ② 「发布前持有一份当前快照」= 一位读者在发布窗内持态 ⇒ 发布走 `make_mut`
//    克隆分支（README 大状态锚的最坏情形）；陈旧长持只在首拍触发；
// ③ 分配面读 `CountingAllocator` 累计面（churn）：**局限** = 分配器只见分配
//    事件、不见 memcpy 字节数（拷贝成本只体现在墙钟），realloc 计作换块
//    （alloc/dealloc 各 1）；`live_bytes` 是请求字节净额、非 RSS，同机同 run
//    内可比、跨平台不可比。**读数里还有一层分配器类别效应**：单次分配跨过
//    malloc 的 mmap 阈值后每次都要重新缺页+零页——本机实测每 MiB 拷贝 ≈ 210 µs
//    （~5 GB/s），而 64 KiB 拷贝 ≈ 2.3 µs（~28 GB/s）；故「整体态 vs 分片态」
//    的倍数里含这一段，不是纯 memcpy 带宽差;
// ④ 分片态与 `compose` 的 Fragment 对齐是**布局层面**的等价（分片边界 =
//    Fragment 切片边界）；本文件不 import `compose` 域，避免把 `compose` 塞进
//    bench 的 required-features（Cargo.toml 归 PM）。

const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;
/// 分片态默认分片数（1 MiB / 16 = 64 KiB）。
const PARTITIONS: usize = 16;

/// 单拍触碰范围：改 1 字节（选择性）vs 改全部字节（整体更新，两布局同工作量）。
#[derive(Clone, Copy)]
enum UpdateScope {
    One,
    All,
}

/// 整体式状态：单块连续缓冲，`Clone` = 全量深拷贝。
#[derive(Clone, Debug)]
struct MonoState {
    bytes: Vec<u8>,
}

/// 分片状态：根 `Clone` 只拷 `Arc` 句柄；被改动且被快照钉住的分片才深拷贝。
#[derive(Clone, Debug)]
struct PartitionedState {
    partitions: Vec<Arc<Vec<u8>>>,
}

struct MonoReducer {
    total: usize,
    scope: UpdateScope,
}

impl skeleton::Reducer for MonoReducer {
    type State = MonoState;
    type Action = u8;
    type Op = skeleton::Infallible;

    fn initial(&self) -> MonoState {
        MonoState {
            bytes: vec![0u8; self.total],
        }
    }

    fn reduce(
        &self,
        s: &MonoState,
        a: u8,
        _out: &mut skeleton::EffectBuf<u8, skeleton::Infallible>,
    ) -> MonoState {
        let mut next = s.clone(); // 整体态固有税：全量拷贝
        match self.scope {
            UpdateScope::One => next.bytes[0] = a,
            UpdateScope::All => next.bytes.fill(a),
        }
        next
    }
}

struct PartitionedReducer {
    partitions: usize,
    partition_bytes: usize,
    scope: UpdateScope,
}

impl skeleton::Reducer for PartitionedReducer {
    type State = PartitionedState;
    type Action = u8;
    type Op = skeleton::Infallible;

    fn initial(&self) -> PartitionedState {
        PartitionedState {
            partitions: (0..self.partitions)
                .map(|_| Arc::new(vec![0u8; self.partition_bytes]))
                .collect(),
        }
    }

    /// 两段税：根浅拷贝（逐片 `Arc` 计数 +1）恒发生；深拷贝落在被 `Arc::make_mut`
    /// 触碰的分片上——**但 `s.clone()` 已使每片计数 ≥2 ⇒ 被碰分片必深拷贝，与是否
    /// 持快照无关**（`examples/state_layout.rs` 实测：持/不持快照同为 65,536 B）。
    /// 快照只决定旧世代是否继续存活（留存成本），不决定这一拍拷不拷。
    fn reduce(
        &self,
        s: &PartitionedState,
        a: u8,
        _out: &mut skeleton::EffectBuf<u8, skeleton::Infallible>,
    ) -> PartitionedState {
        let mut next = s.clone();
        match self.scope {
            UpdateScope::One => Arc::make_mut(&mut next.partitions[0])[0] = a,
            UpdateScope::All => {
                for p in &mut next.partitions {
                    Arc::make_mut(p).fill(a);
                }
            }
        }
        next
    }
}

fn mono_store(total: usize, scope: UpdateScope) -> SyncStore<MonoState, u8> {
    let store = SyncStore::new(MonoReducer { total, scope }, skeleton::NoFx);
    // 预热：队列/缓冲到稳态容量（此后每拍只剩状态拷贝）。
    for _ in 0..64 {
        store.dispatch(1);
    }
    store
}

fn partitioned_store(
    total: usize,
    partitions: usize,
    scope: UpdateScope,
) -> SyncStore<PartitionedState, u8> {
    let store = SyncStore::new(
        PartitionedReducer {
            partitions,
            partition_bytes: total / partitions,
            scope,
        },
        skeleton::NoFx,
    );
    for _ in 0..64 {
        store.dispatch(1);
    }
    store
}

/// 留存窗口：滚动持有最近 `len` 份快照（`len = 0` = 不持，发布原地写）。
/// 每拍先拉一份**当前**快照再发布 ⇒ 发布时当前世代恒被钉住（活跃读者形态）。
struct Window<S> {
    snaps: VecDeque<Arc<S>>,
    len: usize,
}

impl<S> Window<S> {
    fn new(len: usize) -> Self {
        Self {
            snaps: VecDeque::with_capacity(len),
            len,
        }
    }

    fn rotate(&mut self, current: Arc<S>) {
        if self.len == 0 {
            return;
        }
        self.snaps.push_back(current);
        if self.snaps.len() > self.len {
            self.snaps.pop_front();
        }
    }
}

/// 单拍：`window.len == 0` 时不拉快照（保护无留存臂的读数），否则在发布窗内
/// 持一份当前快照 ⇒ 发布走 `Arc::make_mut` 克隆分支。
fn beat<S, A, Op>(store: &SyncStore<S, A, Op>, window: &mut Window<S>, action: A)
where
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
{
    if window.len == 0 {
        store.dispatch(action);
        return;
    }
    window.rotate(store.snapshot());
    store.dispatch(action);
}

/// 每拍分配 / 释放事件数（churn 口径：live 净额对「即用即还」恒 0）。
fn churn(beats: u32, mut once: impl FnMut()) -> (f64, f64) {
    let allocs = CountingAllocator::total_allocs();
    let deallocs = CountingAllocator::total_deallocs();
    for _ in 0..beats {
        once();
    }
    let n = f64::from(beats);
    (
        (CountingAllocator::total_allocs() - allocs) as f64 / n,
        (CountingAllocator::total_deallocs() - deallocs) as f64 / n,
    )
}

struct ChurnRow {
    arm: String,
    allocs: f64,
    frees: f64,
}

/// 登记一条臂：先测 churn，再注册 Criterion 用例（同装置同窗口 ⇒ 可比）。
fn push_arm<S, A, Op>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    rows: &mut Vec<ChurnRow>,
    arm: String,
    store: &SyncStore<S, A, Op>,
    window: &mut Window<S>,
    action: A,
) where
    S: Clone + Send + Sync + 'static,
    A: Copy + Send + 'static,
    Op: Send + 'static,
{
    let (allocs, frees) = churn(16, || beat(store, window, action));
    rows.push(ChurnRow {
        arm: arm.clone(),
        allocs,
        frees,
    });
    group.bench_function(arm, |b| b.iter(|| beat(store, window, black_box(action))));
}

fn print_churn(title: &str, rows: &[ChurnRow]) {
    println!("\n== {title} ==");
    println!("{:<32} {:>12} {:>12}", "arm", "allocs/beat", "frees/beat");
    for r in rows {
        println!("{:<32} {:>12.2} {:>12.2}", r.arm, r.allocs, r.frees);
    }
}

/// 留存形态（④ 的自变量）。
#[derive(Clone, Copy)]
enum RetainShape {
    /// 不持快照（根发布原地写；分片深拷贝规则见 `PartitionedReducer::reduce`）
    None,
    /// 一份快照长持不刷新（陈旧世代；只有首拍触发发布克隆）
    Stale,
    /// 滚动窗口 `k`：每拍发布前拉一份当前快照，保留最近 `k` 份
    Window(usize),
}

impl RetainShape {
    fn label(self) -> &'static str {
        match self {
            RetainShape::None => "none",
            RetainShape::Stale => "stale1",
            RetainShape::Window(1) => "window1",
            RetainShape::Window(4) => "window4",
            RetainShape::Window(16) => "window16",
            RetainShape::Window(_) => "windowN",
        }
    }
}

/// 留存净字节：(live_bytes 增量, 每份留存快照钉住的净字节)。
/// 窗口按 2k 拍测（前 k 拍填窗、后 k 拍滚动稳态）；陈旧长持按 8 拍测。
/// **留存物必须活到读数之后**——放在 match 臂内的局部会在臂尾析构，量到的
/// 「增长」恒 0（本函数 2026-11-23 初版即栽在此，被 `state_layout_retention`
/// 的主张闸抓住）。
fn retention_bytes<S, A, Op>(
    store: &SyncStore<S, A, Op>,
    shape: RetainShape,
    action: A,
) -> (i64, i64)
where
    S: Clone + Send + Sync + 'static,
    A: Copy + Send + 'static,
    Op: Send + 'static,
{
    let (delta, retained) = match shape {
        RetainShape::None => {
            let before = CountingAllocator::live_bytes() as i64;
            for _ in 0..8 {
                store.dispatch(action);
            }
            (CountingAllocator::live_bytes() as i64 - before, 0)
        }
        RetainShape::Stale => {
            let stale = store.snapshot();
            let before = CountingAllocator::live_bytes() as i64;
            for _ in 0..8 {
                store.dispatch(action);
            }
            let delta = CountingAllocator::live_bytes() as i64 - before;
            black_box(&stale);
            (delta, 1)
        }
        RetainShape::Window(k) => {
            let mut window = Window::new(k);
            let before = CountingAllocator::live_bytes() as i64;
            for _ in 0..(2 * k as u32) {
                beat(store, &mut window, action);
            }
            let delta = CountingAllocator::live_bytes() as i64 - before;
            black_box(&window.snaps);
            (delta, k)
        }
    };
    (
        delta,
        if retained == 0 {
            0
        } else {
            delta / retained as i64
        },
    )
}

struct RetRow {
    layout: &'static str,
    shape: &'static str,
    delta: i64,
    per_snap: i64,
}

fn print_retention(rows: &[RetRow]) {
    println!("\n== retention growth (1 MiB total, 16 x 64 KiB shards) ==");
    println!(
        "{:<24} {:>16} {:>18}",
        "arm", "live delta B", "per snapshot B"
    );
    for r in rows {
        println!(
            "{:<24} {:>16} {:>18}",
            format!("{}/{}", r.layout, r.shape),
            r.delta,
            r.per_snap
        );
    }
}

fn per_snap_of(rows: &[RetRow], layout: &str, shape: &str) -> i64 {
    rows.iter()
        .find(|r| r.layout == layout && r.shape == shape)
        .expect("retention row present")
        .per_snap
}

/// ① 整体 vs 分片 × 总量 × 留存：交叉区（分片何时开始赢）与「无留存时整体态
/// 只剩 reducer 一次拷贝」的反例都在本组。16 臂含 16 MiB 臂，故测量窗收窄到
/// 2s/50 样本（臂间差距 ≫ 采样噪声）。
fn state_layout_update(c: &mut Criterion) {
    assert_allocator_instrumented();
    let mut group = c.benchmark_group("state_layout_update");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);
    let mut rows = Vec::new();

    for (label, total) in [
        ("1k", KIB),
        ("64k", 64 * KIB),
        ("1m", MIB),
        ("16m", 16 * MIB),
    ] {
        for retain in [false, true] {
            let tag = if retain { "retain" } else { "noretain" };
            let window_len = if retain { 1 } else { 0 };

            let store = mono_store(total, UpdateScope::One);
            let mut window = Window::new(window_len);
            push_arm(
                &mut group,
                &mut rows,
                format!("mono_{label}_{tag}"),
                &store,
                &mut window,
                2,
            );

            let store = partitioned_store(total, PARTITIONS, UpdateScope::One);
            let mut window = Window::new(window_len);
            push_arm(
                &mut group,
                &mut rows,
                format!("sharded_{label}_{tag}"),
                &store,
                &mut window,
                2,
            );
        }
    }
    group.finish();
    print_churn("state_layout_update: per-beat allocation events", &rows);
}

/// ③⑤ 选择性 vs 整体更新 + 分片粒度（固定 1 MiB 总量；末两臂 1 KiB 总量）。
/// 反例臂：单片分装（同样字节、只多一层 `Arc` + 根 `Vec`）、触全部分片（同
/// 字节工作量、分片态多出 16 倍分配事件）、过度分片（1 KiB / 256 片：逐片
/// `Arc` 计数压过 4 字节拷贝）。
fn state_layout_update_scope(c: &mut Criterion) {
    assert_allocator_instrumented();
    let mut group = c.benchmark_group("state_layout_update_scope");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);
    let mut rows = Vec::new();

    for retain in [false, true] {
        let tag = if retain { "retain" } else { "noretain" };
        let window_len = if retain { 1 } else { 0 };

        // 整体态：任何更新都要全量拷贝（无选择性可言）。
        let store = mono_store(MIB, UpdateScope::All);
        let mut window = Window::new(window_len);
        push_arm(
            &mut group,
            &mut rows,
            format!("mono_whole_{tag}"),
            &store,
            &mut window,
            2,
        );

        for (name, total, parts, scope) in [
            ("sharded_single", MIB, 1, UpdateScope::One),
            ("sharded_16parts_one", MIB, PARTITIONS, UpdateScope::One),
            ("sharded_16parts_all", MIB, PARTITIONS, UpdateScope::All),
            ("sharded_256parts_1k_one", KIB, 256, UpdateScope::One),
            ("sharded_256parts_1k_all", KIB, 256, UpdateScope::All),
        ] {
            let store = partitioned_store(total, parts, scope);
            let mut window = Window::new(window_len);
            push_arm(
                &mut group,
                &mut rows,
                format!("{name}_{tag}"),
                &store,
                &mut window,
                2,
            );
        }
    }
    group.finish();
    print_churn(
        "state_layout_update_scope: per-beat allocation events",
        &rows,
    );
}

/// ④ 留存形态 × 内存增长：陈旧长持（快照只在首拍触发发布克隆）vs 滚动窗口
/// （每拍发布都撞上当前世代）——「持有一份快照」这句话的两种读法在这里分叉。
fn state_layout_retention(c: &mut Criterion) {
    assert_allocator_instrumented();
    let mut group = c.benchmark_group("state_layout_retention");
    group.throughput(Throughput::Elements(1));
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(50);

    // 墙钟臂：陈旧长持 vs 16 份滚动窗口（同状态同总量，只差留存形态）。
    {
        let store = mono_store(MIB, UpdateScope::One);
        let stale = store.snapshot();
        group.bench_function("mono_stale1", |b| {
            b.iter(|| {
                black_box(&stale);
                store.dispatch(black_box(2))
            })
        });
        let mut window = Window::new(16);
        group.bench_function("mono_window16", |b| {
            b.iter(|| beat(&store, &mut window, black_box(2)))
        });
    }
    {
        let store = partitioned_store(MIB, PARTITIONS, UpdateScope::One);
        let stale = store.snapshot();
        group.bench_function("sharded_stale1", |b| {
            b.iter(|| {
                black_box(&stale);
                store.dispatch(black_box(2))
            })
        });
        let mut window = Window::new(16);
        group.bench_function("sharded_window16", |b| {
            b.iter(|| beat(&store, &mut window, black_box(2)))
        });
    }

    // 内存面：每形态独立 store（上一形态的窗口快照不污染下一形态的基线）。
    let shapes = [
        RetainShape::None,
        RetainShape::Stale,
        RetainShape::Window(1),
        RetainShape::Window(4),
        RetainShape::Window(16),
    ];
    let mut rows = Vec::new();
    for shape in shapes {
        let store = mono_store(MIB, UpdateScope::One);
        let (delta, per_snap) = retention_bytes(&store, shape, 2);
        rows.push(RetRow {
            layout: "mono",
            shape: shape.label(),
            delta,
            per_snap,
        });
    }
    for shape in shapes {
        let store = partitioned_store(MIB, PARTITIONS, UpdateScope::One);
        let (delta, per_snap) = retention_bytes(&store, shape, 2);
        rows.push(RetRow {
            layout: "sharded",
            shape: shape.label(),
            delta,
            per_snap,
        });
    }
    group.finish();
    print_retention(&rows);

    // 布局主张闸：字节数由请求大小决定（非机器相关），同 run 内可比——分片态
    // + 选择性更新时留存一份快照应≈单分片字节，整体态≈全量。若哪天根克隆变深
    // 或更新绕过 `Arc::make_mut`，本闸即红。
    let mono_per_snap = per_snap_of(&rows, "mono", "window16");
    let sharded_per_snap = per_snap_of(&rows, "sharded", "window16");
    assert!(
        mono_per_snap >= (MIB / 2) as i64,
        "整体态留存一份快照应钉住约全量状态（实测 {mono_per_snap} 字节）"
    );
    assert!(
        sharded_per_snap <= (MIB / 8) as i64,
        "分片态 + 选择性更新留存一份快照应只钉住被改分片（实测 {sharded_per_snap} 字节）；\
         达到全量说明根克隆变深或更新绕过 Arc::make_mut"
    );
}

// ---- multi-producer 准入 / 完成面（issue#96 / bd udf-frame-7is）----
//
// 矩阵：baseline / contention(2·4·8·16·32) / bounded admission(small·saturated) /
// slow reducer / causal feedback。issue 的「try_dispatch 与 dispatch 不可混成一个
// 吞吐数」落在 **ID 面**（两个 group）：
//
// - `multi_producer_admission`：`try_dispatch` 纯准入（不等待完成边界）；
// - `multi_producer_dispatch`：`dispatch_with_receipt(..).wait()` 端到端完成边界
//   （与 `Store::dispatch` 同一完成承诺；用回执面是因为有界臂的 `Full` 在 legacy
//   `dispatch` 上是 panic——那是刻意选的 fail-visible 面，不是本组要测的东西）。
//
// 装置纪律（issue#96 实验实现约束 1–4）：
// ① 时序协调只用 `Barrier` + `mpsc`（启动 / 排空者入场 / 受控停顿），无 sleep；
// ② 生产者与排空者线程在**样本内创建一次**（`thread::scope`），计时窗只覆盖
//    「生产者开闸 → 全部返回」，线程创建不在窗内；
// ③ 每样本新建 Store（队列 / 账本池 / 订阅者从零起），warm-up 与测量由
//    criterion 分开；
// ④ 计时窗内**无 per-op 计时器** ⇒ 吞吐读数干净；p50/p95/p99 只在首个样本的
//    首轮采样（criterion 的 warm-up 轮），读数含每 op 两次 `Instant::now` 开销
//    （~20-30 ns/op，Windows）——量级判读用，绝对值不作 SLA。
//
// 指标口径：
// - 吞吐 = 元素/秒，元素 = **提交尝试数**（含 Full 拒绝）⇒ bounded 臂的吞吐是
//   「尝试率」，不是完成率；完成数另列；
// - 队列深 max/avg 由**生产者侧**抽样（每 1/8 提交量读一次 `pending_actions()`，
//   该读取两次队列锁，抽样成本 ~1/125 op）；正在 reduce 的 action 已出队 ⇒
//   不计入队列深（issue 明确要求）；
// - bounded saturated 臂：排空者持租约阻塞在 `MpAct::Pause` 的 channel 令牌上，
//   队列填满后剩余提交走 `Full`；`release_depth` = **放行前真实排队 population**
//   = 一次排空批的大小（唯一可直读 batch 的形态）+ `放行→排空` 墙钟；
// - slow reducer 臂 = reducer-heavy 控制组（定长确定性自旋，`black_box` 防折叠），
//   使「准入 / 排队 / 服务」三段可分离；
// - causal feedback 臂 = 每条外部 action 在 reducer 内 `Effect::Emit` 一条子动作，
//   与同 producer 数的 contention 臂之差即回灌成本。
//
// 两处非对称，都是装置可运行性要求，不是被测语义差异：
// ① **放行点**：admission 组生产者不阻塞 ⇒ 全批跑完再放行（提交期全程饱和，
//    便于量 Full 率与拒绝成本）；dispatch 组生产者阻塞在回执上 ⇒ 必须由主线程
//    观测到 `admission_full_count() > 0` 后放行（等全批跑完 = 死锁）。
// ② **公平性非保证**：并发生产者之间没有跨线程公平 FIFO（契约只承诺单生产者
//    程序序与入队线性化序）。本组按生产者统计完成数以**观测**饥饿，不做公平性
//    断言、也不暗示 mutex 提供严格公平。
//
// 不可直读项：contention 臂的「一次排空处理的批大小」无读侧计数（本票不改 src）；
// 只能由队列深抽样与尾延迟形态间接判读，见报告「未覆盖」段。

/// 生产者 / 排空者的动作域。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MpAct {
    /// 轻量外部动作。
    Nop,
    /// 因果回灌：reducer 内 Emit 一条子动作（同栈续转，不经解释器）。
    EmitChild,
    /// 受控停顿（只由排空者提交）：reducer 内等 channel 令牌，不用 sleep。
    Pause,
    /// reducer-heavy：定长确定性自旋（服务时间控制组）。
    Heavy,
}

/// 两个 API 面 = 两个 group（issue 约束 3）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MpMode {
    /// `try_dispatch`：只测准入，不等完成边界。
    Admission,
    /// `dispatch_with_receipt(..).wait()`：端到端完成边界。
    Dispatch,
}

#[derive(Clone, Copy, Debug)]
struct MpState {
    reduced: u64,
}

/// 受控停顿通道 + 服务时间控制。每次样本新建 store 时随之新建（无跨样本残留）。
struct MpReducer {
    /// 停顿放行通道（`MpAct::Pause` 的 reduce 里 recv 一条令牌）。
    gate: Mutex<mpsc::Receiver<()>>,
    /// 停顿入场信号：阻塞**之前**发一条 ⇒ 保证「排空者已持租约」先于生产者开闸，
    /// 否则生产者会自己夺租约、把停顿搬到自己的调用栈上。
    entered: Mutex<mpsc::Sender<()>>,
    /// `MpAct::Heavy` 的自旋轮数（0 = 不适用）。
    spin: u32,
}

impl skeleton::Reducer for MpReducer {
    type State = MpState;
    type Action = MpAct;
    type Op = std::convert::Infallible;

    fn initial(&self) -> MpState {
        MpState { reduced: 0 }
    }

    fn reduce(&self, s: &MpState, action: MpAct, out: &mut skeleton::EffectBuf<MpAct>) -> MpState {
        match action {
            MpAct::Pause => {
                // 先报到场（主线程据此开闸），再阻塞等放行令牌。
                let _ = self.entered.lock().expect("entered 锁").send(());
                let _ = self.gate.lock().expect("gate 锁").recv();
            }
            MpAct::EmitChild => out.push(Effect::Emit(MpAct::Nop)),
            MpAct::Heavy => {
                let mut acc = s.reduced;
                for _ in 0..self.spin {
                    acc = acc
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                }
                black_box(acc);
            }
            MpAct::Nop => {}
        }
        MpState {
            reduced: s.reduced + 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct MpPlan {
    producers: usize,
    per_producer: u64,
    action: MpAct,
    mode: MpMode,
    /// 0 = unbounded（外部 ingress 不限）。
    capacity: usize,
    /// 排空者是否先进入受控停顿（bounded saturated 形态）。
    pause: bool,
    spin: u32,
}

#[derive(Clone, Copy, Debug, Default)]
struct MpProducerStats {
    submitted: u64,
    accepted: u64,
    full: u64,
    completed: u64,
    depth_max: usize,
    depth_sum: u64,
    depth_n: u64,
}

#[derive(Default)]
struct MpSample {
    producers: Vec<MpProducerStats>,
    /// 首个样本首轮采样（含 per-op 计时开销）。
    latencies: Vec<u64>,
    /// 放行前排队 population（= 一次排空批大小）。
    release_depth: usize,
    /// 放行 → 排空完成的墙钟。
    recovery_ns: u64,
    /// 样本结束时状态里的 reduce 计数（装置闸：应与「准入 + 排空者自提交」相符）。
    reduced: u64,
    /// 外部准入 Full 计数（store 读侧，fail-visible 面）。
    full_counter: u64,
}

fn mp_store(
    plan: &MpPlan,
) -> (
    SyncStore<MpState, MpAct>,
    mpsc::Sender<()>,
    mpsc::Receiver<()>,
) {
    let (release_tx, gate) = mpsc::channel();
    let (entered_tx, entered_rx) = mpsc::channel();
    let store = SyncStore::new(
        MpReducer {
            gate: Mutex::new(gate),
            entered: Mutex::new(entered_tx),
            spin: plan.spin,
        },
        skeleton::NoFx,
    );
    if plan.capacity > 0 {
        store.set_capacity(plan.capacity);
    }
    (store, release_tx, entered_rx)
}

/// 一个样本 = 一个新建 Store + 一轮（`iters` 次重复）。返回样本墙钟与统计。
///
/// 线程模型：N 个生产者 + 1 个排空者 + 本线程（计时与放行）。四个 Barrier 对应
/// 「开闸 → 排空者就位 → 生产者返回 → 本轮排空完成」四相位。
fn mp_sample(plan: &MpPlan, iters: u64, collect_lat: bool) -> (Duration, MpSample) {
    let (store, release_tx, entered_rx) = mp_store(plan);
    let store = Arc::new(store);
    let n = plan.producers;
    let start = Barrier::new(n + 2);
    let armed = Barrier::new(n + 1);
    let finished = Barrier::new(n + 1);
    let drained = Barrier::new(2);
    let mut residue = 0u64;
    // 装置闸的违规先记下来、**出计时窗后**再断言：在 scope 内 panic 会卡在
    // thread::scope 的 join 上（worker 还挂在 Barrier 上）⇒ 进程挂住、exe 被占。
    let mut saturated_missing = false;
    let mut residue_stuck = false;
    let drainer_own = AtomicU64::new(0);
    let (stats_tx, stats_rx) = mpsc::channel::<(MpProducerStats, Vec<u64>)>();
    let depth_every = (plan.per_producer / 8).max(1);
    let mut total = Duration::ZERO;
    let mut sample = MpSample::default();

    thread::scope(|scope| {
        // 引用（Copy）捕获：N 个生产者闭包共享同一组 Barrier / 原子，不各自持一份。
        let (start, armed, finished, drained) = (&start, &armed, &finished, &drained);
        let drainer_own = &drainer_own;
        for _ in 0..n {
            let store = Arc::clone(&store);
            let stats_tx = stats_tx.clone();
            scope.spawn(move || {
                let mut st = MpProducerStats::default();
                let mut lats: Vec<u64> = Vec::new();
                for round in 0..iters {
                    start.wait();
                    armed.wait();
                    let timed = collect_lat && round == 0;
                    for i in 0..plan.per_producer {
                        let t0 = Instant::now();
                        match plan.mode {
                            MpMode::Admission => match store.try_dispatch(plan.action) {
                                AdmissionOutcome::Accepted => st.accepted += 1,
                                AdmissionOutcome::Full => st.full += 1,
                                AdmissionOutcome::Closed => panic!("bench store 不闭门"),
                            },
                            MpMode::Dispatch => match store.dispatch_with_receipt(plan.action) {
                                Ok(receipt) => {
                                    st.accepted += 1;
                                    if receipt.wait() == DispatchOutcome::Completed {
                                        st.completed += 1;
                                    }
                                }
                                Err(AdmissionOutcome::Full) => st.full += 1,
                                Err(AdmissionOutcome::Closed) => panic!("bench store 不闭门"),
                                Err(AdmissionOutcome::Accepted) => {
                                    unreachable!("Accepted 不出现在 Err 位（回执面契约）")
                                }
                            },
                        }
                        if timed {
                            lats.push(t0.elapsed().as_nanos() as u64);
                        }
                        if i % depth_every == 0 {
                            let d = store.pending_actions();
                            st.depth_max = st.depth_max.max(d);
                            st.depth_sum += d as u64;
                            st.depth_n += 1;
                        }
                    }
                    st.submitted += plan.per_producer;
                    finished.wait();
                }
                let _ = stats_tx.send((st, lats));
            });
        }
        {
            let store = Arc::clone(&store);
            scope.spawn(move || {
                for _ in 0..iters {
                    start.wait();
                    if plan.pause {
                        // 夺租约 → 阻塞在受控停顿 → 放行后同一排空环把队列排空；
                        // 该调用返回即本轮的恢复点。
                        store.dispatch(MpAct::Pause);
                        drainer_own.fetch_add(1, Ordering::SeqCst);
                        drained.wait();
                    } else {
                        // 每轮只提**一条**自身动作：占住「无孤儿排空」的线性化点，
                        // 又不对扩展曲线造成固定份额的偏置。**不做**「排空到 idle」
                        // 的循环——那会与本轮生产者互相喂活（一方不断提交、另一方
                        // 的排空环永不空）⇒ 活锁 + 队列无界增长（2026-11-24 实测
                        // 撞到 20 GiB 分配失败）。单租约是结构不变量，不由本线程独享：
                        // 谁在无租约时准入，谁就排空（store 的既有语义）。
                        if let Ok(receipt) = store.dispatch_with_receipt(MpAct::Nop) {
                            let _ = receipt.wait();
                            drainer_own.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            });
        }
        for _ in 0..iters {
            start.wait();
            if plan.pause {
                entered_rx.recv().expect("排空者必须进入受控停顿");
            }
            armed.wait();
            let t0 = Instant::now();
            if plan.pause && plan.mode == MpMode::Dispatch {
                // 完成边界面：生产者阻塞在各自回执上 ⇒ 必须观测到「队列已满」后
                // 放行；等全批返回 = 死锁。
                let mut guard = 0u64;
                while guard < 50_000_000 && store.admission_full_count() == 0 {
                    guard += 1;
                    std::hint::spin_loop();
                }
                if store.admission_full_count() == 0 {
                    // 观测不到 Full = 形态没成立；**照样放行**让本轮收敛，出窗后报红。
                    saturated_missing = true;
                }
                sample.release_depth = sample.release_depth.max(store.pending_actions());
                let _ = release_tx.send(());
            }
            finished.wait();
            total += t0.elapsed();
            if plan.pause {
                if plan.mode == MpMode::Admission {
                    // 准入面生产者不阻塞 ⇒ 全批跑完再放行（提交期全程饱和）。
                    sample.release_depth = sample.release_depth.max(store.pending_actions());
                    let _ = release_tx.send(());
                }
                let t1 = Instant::now();
                drained.wait();
                sample.recovery_ns = sample.recovery_ns.max(t1.elapsed().as_nanos() as u64);
            } else {
                // 本轮残留在**计时窗外**收干净：否则它会被算进下一轮，破坏
                // 「每样本独立」口径（守卫防装置失真导致的静默挂死）。
                let mut guard = 0u32;
                while guard < 10_000 && store.pending_actions() > 0 {
                    guard += 1;
                    if let Ok(receipt) = store.dispatch_with_receipt(MpAct::Nop) {
                        let _ = receipt.wait();
                        residue += 1;
                    }
                }
                if store.pending_actions() > 0 {
                    residue_stuck = true;
                }
            }
        }
    });
    drop(stats_tx);
    for (st, lats) in stats_rx.iter() {
        sample.producers.push(st);
        sample.latencies.extend(lats);
    }
    sample.reduced = store.snapshot().reduced;
    sample.full_counter = store.admission_full_count();
    assert!(
        !saturated_missing,
        "bounded saturated 臂未观测到 Full（装置失真：饱和形态不成立）"
    );
    assert!(!residue_stuck, "残留排空未收敛（装置失真）");
    mp_check(plan, &sample, drainer_own.load(Ordering::SeqCst) + residue);
    (total, sample)
}

/// 装置闸（**非性能阈值**）：读数若成立不了这些结构性事实，就说明装置失真、
/// 数字无意义，宁可红。
fn mp_check(plan: &MpPlan, sample: &MpSample, own_reduces: u64) {
    assert_eq!(
        sample.producers.len(),
        plan.producers,
        "生产者统计缺失：有生产者线程未交付读数"
    );
    let accepted: u64 = sample.producers.iter().map(|p| p.accepted).sum();
    let expected = accepted
        * if plan.action == MpAct::EmitChild {
            2
        } else {
            1
        }
        + own_reduces;
    assert_eq!(
        sample.reduced, expected,
        "reduce 计数与准入统计不符（accepted={accepted} + 排空侧自提交/残留={own_reduces}，\
         feedback 臂应翻倍）：有 action 丢失或被重复处理"
    );
    if plan.pause {
        assert_eq!(
            sample.release_depth, plan.capacity,
            "bounded saturated 臂放行前队列应恰好填满容量（实测 {} / 容量 {}）",
            sample.release_depth, plan.capacity
        );
        assert!(
            sample.full_counter > 0,
            "bounded saturated 臂未产生任何 Full：饱和形态未成立"
        );
    }
}

fn mp_print(id: &str, plan: &MpPlan, sample: &MpSample, iters: u64) {
    let per: Vec<String> = sample
        .producers
        .iter()
        .map(|p| format!("{}/{}/{}/{}", p.submitted, p.accepted, p.full, p.completed))
        .collect();
    let depth_max = sample
        .producers
        .iter()
        .map(|p| p.depth_max)
        .max()
        .unwrap_or(0);
    let depth_sum: u64 = sample.producers.iter().map(|p| p.depth_sum).sum();
    let depth_n: u64 = sample.producers.iter().map(|p| p.depth_n).sum();
    let avg = if depth_n == 0 {
        0.0
    } else {
        depth_sum as f64 / depth_n as f64
    };
    let mut lat = sample.latencies.clone();
    lat.sort_unstable();
    let pct = |q: usize| {
        if lat.is_empty() {
            0
        } else {
            lat[(lat.len() - 1) * q / 100]
        }
    };
    eprintln!(
        "[multi-producer] {id} —— 生产者={} 每生产者提交/轮={} action={:?} 模式={:?} 容量={} 轮数={iters}",
        plan.producers, plan.per_producer, plan.action, plan.mode, plan.capacity
    );
    eprintln!("  每生产者 提交/准入/Full/完成 = {}", per.join(" | "));
    eprintln!(
        "  full_counter={} reduced={} 队列深 max/avg={}/{}（生产者侧抽样 {} 次）",
        sample.full_counter, sample.reduced, depth_max, avg, depth_n
    );
    if plan.pause {
        eprintln!(
            "  release_depth(一次排空批)={} 放行→排空={:.1} µs",
            sample.release_depth,
            sample.recovery_ns as f64 / 1000.0
        );
    }
    eprintln!(
        "  p50/p95/p99 = {} / {} / {} ns（首轮样本，含 per-op 计时开销）",
        pct(50),
        pct(95),
        pct(99)
    );
    // 饥饿观测：已准入但一次都没完成的生产者（公平性非 API 保证，只如实报告）。
    // 准入面不等待完成边界 ⇒ 该面的 completed 恒 0，只在完成边界面对照。
    let starved: Vec<usize> = if plan.mode == MpMode::Dispatch {
        sample
            .producers
            .iter()
            .enumerate()
            .filter(|(_, p)| p.accepted > 0 && p.completed == 0)
            .map(|(i, _)| i)
            .collect()
    } else {
        Vec::new()
    };
    if !starved.is_empty() {
        eprintln!("  WARN 生产者 {starved:?} 有准入但零完成（观测到饥饿，非 API 保证）");
    }
}

fn mp_id(plan: &MpPlan) -> String {
    let n = plan.producers;
    match plan.action {
        MpAct::Heavy => format!("slow_reducer_p{n}"),
        MpAct::EmitChild => format!("causal_feedback_p{n}"),
        MpAct::Nop if plan.pause => format!("bounded_saturated_p{n}"),
        MpAct::Nop if plan.capacity > 0 => format!("bounded_small_p{n}"),
        MpAct::Nop if n == 1 => "baseline_p1".to_string(),
        MpAct::Nop => format!("contention_p{n}"),
        MpAct::Pause => unreachable!("Pause 只由排空者提交"),
    }
}

fn mp_plans(mode: MpMode) -> Vec<MpPlan> {
    let mut plans = Vec::new();
    let mut push = |producers, per_producer, action, capacity, pause, spin| {
        plans.push(MpPlan {
            producers,
            per_producer,
            action,
            mode,
            capacity,
            pause,
            spin,
        })
    };
    push(1, 2000, MpAct::Nop, 0, false, 0);
    for n in [2, 4, 8, 16, 32] {
        push(n, 1000, MpAct::Nop, 0, false, 0);
    }
    for n in [2, 4, 8, 16, 32] {
        push(n, 1000, MpAct::Nop, 64, false, 0);
    }
    for n in [4, 8, 16, 32] {
        // 饱和形态：容量 2 **必须**小于生产者数——完成边界面每个生产者同时只
        // 有 1 条在途（阻塞在自己的回执上），队列占用的上界 = min(n, cap)；
        // n ≤ cap 时永远填不满 ⇒ 观测不到 Full（实测 n=2/cap=8 即此形态）。
        push(n, 5000, MpAct::Nop, 2, true, 0);
    }
    for n in [1, 2, 4, 8, 16] {
        push(n, 100, MpAct::Heavy, 0, false, 1000);
    }
    push(4, 1000, MpAct::EmitChild, 0, false, 0);
    plans
}

fn mp_group(c: &mut Criterion, mode: MpMode) {
    let name = match mode {
        MpMode::Admission => "multi_producer_admission",
        MpMode::Dispatch => "multi_producer_dispatch",
    };
    let mut group = c.benchmark_group(name);
    // 臂多（22）× 每臂都要起线程，故把单臂测量窗与样本数收窄（臂间差距远大于
    // 采样噪声；完整矩阵的绝对值判读留给 perf.yml 的滚动基线）。
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(1));
    for plan in mp_plans(mode) {
        let id = mp_id(&plan);
        group.throughput(Throughput::Elements(
            plan.producers as u64 * plan.per_producer,
        ));
        group.bench_function(&id, |b| {
            // 每臂独立的统计闸：延迟采样只在**本臂**的首个样本（warm-up 轮）做，
            // 统计块每臂打一次——共用变量会让第二个臂起全部静默。
            let mut lat_done = false;
            let mut printed = false;
            b.iter_custom(|iters| {
                let collect_lat = !lat_done;
                let (spent, sample) = mp_sample(&plan, iters, collect_lat);
                lat_done |= collect_lat;
                if !printed {
                    printed = true;
                    mp_print(&id, &plan, &sample, iters);
                }
                spent
            });
        });
    }
    group.finish();
}

fn multi_producer_admission(c: &mut Criterion) {
    mp_group(c, MpMode::Admission);
}

fn multi_producer_dispatch(c: &mut Criterion) {
    mp_group(c, MpMode::Dispatch);
}

/// 一次 `fetch_add(1, Relaxed)` 的成本——`CountingAllocator` 干扰的**上界**估计量。
///
/// 用途：把 `state_layout_*` 臂每拍的分配事件数换算成「时间上界」，判断实测差
/// 距能否由 instrumentation 的原子开销解释。**上界而非精确值**，因为：
/// ① 分配器包装本身的间接成本（栈帧 / 分支 / TLS 查找）两侧共有，本臂不含，
///    这部分会被算进「上界」之外 ⇒ 只覆盖原子面；
/// ② 生产里的计数原子与业务数据同处 hot 进程，cache line 竞争下单点成本通常
///    高于本臂的独占计数器；
/// ③ 不同计数点可能是 `fetch_add` 之外的形态（`load` 更便宜），故按最贵形态取。
static MICRO_RMW_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn alloc_atomics(c: &mut Criterion) {
    let mut group = c.benchmark_group("alloc_atomics");
    group.throughput(Throughput::Elements(1));
    group.bench_function("relaxed_rmw", |b| {
        b.iter(|| {
            let prev = MICRO_RMW_COUNTER.fetch_add(1, Ordering::Relaxed);
            black_box(prev)
        })
    });
    group.finish();
}

// ---- ViewBridge 发布 / 通知热路径（#169 / #174）----
//
// 口径：单线程、预热后稳态；吞吐只作参考（环境相关，跨会话单点不可比）。判别力
// 证据在 tests/view_bridge_test.rs 的确定性分配锚（每拍 N 次 / 每绑定 1 次）。
// arena 满时的换区全量重编是稳态的一部分（长跑会周期性出现）。

#[derive(Clone, Default)]
struct ViewApp {
    count: u32,
}

struct ViewReducer;

impl Reducer for ViewReducer {
    type State = ViewApp;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> ViewApp {
        ViewApp::default()
    }
    fn reduce(&self, s: &ViewApp, a: u32, _: &mut EffectBuf<u32, Infallible>) -> ViewApp {
        ViewApp { count: s.count + a }
    }
}

/// N 绑定宽面： = 哨兵过滤 workload（每拍只有 1 个绑定变更）。
struct ViewMapN {
    decls: &'static [BindingDecl],
    filtered: bool,
}

impl ViewMap<ViewApp> for ViewMapN {
    type View = u32;
    fn bindings(&self) -> &[BindingDecl] {
        self.decls
    }
    fn project(&self, index: usize, s: &ViewApp) -> u32 {
        s.count.wrapping_mul(1_000).wrapping_add(index as u32)
    }
    fn input_changed(&self, index: usize, s: &ViewApp) -> bool {
        !self.filtered || (s.count as usize) % self.decls.len() == index
    }
}

struct VJson;

impl JsonEncoder for VJson {
    type View = u32;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &u32) -> serde_json::Value {
        serde_json::json!(v)
    }
}

struct VRegion;

impl RegionEncoder for VRegion {
    type View = u32;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &u32, w: &mut RegionWriter) {
        w.u64(u64::from(*v));
    }
}

type ViewBench = ViewBridge<ViewApp, u32, ViewMapN, ViewIntent, VJson, VRegion>;

struct ViewIntent;

impl IntentMap<u32> for ViewIntent {
    type Wire = serde_json::Value;
    type Err = IntentError;
    fn intent(&self, w: serde_json::Value) -> Result<u32, IntentError> {
        w.as_u64()
            .map(|n| n as u32)
            .ok_or_else(|| IntentError::malformed("expect number"))
    }
}

fn view_decls(n: usize) -> &'static [BindingDecl] {
    let decls: Vec<BindingDecl> = (0..n)
        .map(|i| BindingDecl {
            path: Box::leak(format!("/b{i}").into_boxed_str()),
            kind: PayloadTag(1),
        })
        .collect();
    Box::leak(decls.into_boxed_slice())
}

fn view_bridge(
    decls: &'static [BindingDecl],
    filtered: bool,
    region_capacity: Option<usize>,
) -> ViewBench {
    let bridge = ViewBridge::mount(
        SyncStore::new(ViewReducer, skeleton::NoFx),
        ViewMapN { decls, filtered },
        ViewIntent,
        Encoders {
            json: Some(VJson),
            region: region_capacity.map(|capacity| {
                (
                    VRegion,
                    Arc::new(VecBacking::new(capacity)) as Arc<dyn RegionBacking>,
                )
            }),
        },
    )
    .expect("static declarations are well-formed");
    for decl in decls {
        bridge.bind(decl.path).expect("declared + covered");
    }
    bridge.seal().expect("seal before dispatch");
    bridge
}

/// #169：N 个变更绑定的一拍（JSON + Region 部署，含发布事务）。
fn view_publish(c: &mut Criterion) {
    let mut group = c.benchmark_group("view_publish");
    for bindings in [1usize, 16, 256] {
        let bridge = view_bridge(view_decls(bindings), false, Some(1 << 20));
        for _ in 0..64u32 {
            bridge.send_intent(serde_json::json!(1)).unwrap(); // 预热：scratch 容量
        }
        group.throughput(Throughput::Elements(bindings as u64));
        group.bench_function(format!("changed_{bindings}"), |b| {
            b.iter(|| bridge.send_intent(black_box(serde_json::json!(1))).unwrap())
        });
    }
    group.finish();
}

/// #174：一拍通知（仅 JSON 部署 ⇒ 只量投影 + 两阶段提交面），两种 workload。
fn view_notify(c: &mut Criterion) {
    let mut group = c.benchmark_group("view_notify");
    for filtered in [false, true] {
        for bindings in [1usize, 16, 64, 256] {
            let bridge = view_bridge(view_decls(bindings), filtered, None);
            for _ in 0..64u32 {
                bridge.send_intent(serde_json::json!(1)).unwrap();
            }
            let tag = if filtered { "filtered" } else { "all" };
            group.throughput(Throughput::Elements(bindings as u64));
            group.bench_function(format!("{tag}_{bindings}"), |b| {
                b.iter(|| bridge.send_intent(black_box(serde_json::json!(1))).unwrap())
            });
        }
    }
    group.finish();
}

// ---- effect_push_forms：单条 `push` vs 批量 `push_batch` 的逐条成本（issue#237）----
//
// 两臂在**同一二进制、同一次运行**内对照（对照的是两条代码路径，不是历史基线
// ⇒ 无跨版本漂移）。两臂写进同一个**有限上限**的缓冲（上限远大于批量 ⇒ 只测
// 边界判断路径，不测溢出分支），`Throughput::Elements(k)` 把读数折算成「每条」。
// 期望：批量臂抹掉「每条一次额度判断」，逐条成本随 k 增大而明显下降；单条臂
// 保持票① 的即时硬界（对任意旧 reducer 不可省）。
fn effect_push_forms(c: &mut Criterion) {
    const CAP: usize = 1 << 20;
    let mut group = c.benchmark_group("effect_push_forms");
    for k in [1usize, 4, 16, 64, 256, 1024, 4096] {
        group.throughput(Throughput::Elements(k as u64));
        let mut buf: skeleton::EffectBuf<u64, Infallible> = skeleton::EffectBuf::new();
        group.bench_function(format!("push/k{k}"), |b| {
            b.iter(|| {
                buf.clear();
                buf.set_effect_cap(CAP);
                for _ in 0..k {
                    buf.push(black_box(Effect::Emit(0u64)));
                }
                black_box(buf.len())
            })
        });
        group.bench_function(format!("batch/k{k}"), |b| {
            b.iter(|| {
                buf.clear();
                buf.set_effect_cap(CAP);
                buf.push_batch(black_box((0..k).map(|_| Effect::Emit(0u64))));
                black_box(buf.len())
            })
        });
        // 对照臂：**无上限**的单条 push（其余形状完全相同）⇒ 两臂之差就是
        // 「每次插入一次额度判断」本身的成本（若被编译器提到循环外则≈0）。
        group.bench_function(format!("push_nocap/k{k}"), |b| {
            b.iter(|| {
                buf.clear();
                buf.set_effect_cap(0); // 0 = 不限
                for _ in 0..k {
                    buf.push(black_box(Effect::Emit(0u64)));
                }
                black_box(buf.len())
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    fxscope_spawn_join,
    journal_append_line,
    effect_steady_state,
    effect_batch_forward,
    effect_push_forms,
    dispatch_per_beat,
    notify_dispatch,
    object_pool_acquire_drop,
    memory_pool_acquire_drop,
    thread_pool_reject,
    byte_budget_contention,
    state_layout_update,
    state_layout_update_scope,
    state_layout_retention,
    multi_producer_admission,
    multi_producer_dispatch,
    alloc_atomics,
    view_publish,
    view_notify
);
criterion_main!(benches);
