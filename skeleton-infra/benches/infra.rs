//! infra bench：执行/账本三组（fxscope_spawn_join / journal_append_line /
//! effect_steady_state）+ 池维度四组（蓝图 §11 第 9 项：借还热路径、饱和拒绝、
//! 字节预算争用）。
//!
//! 注意：CountingAllocator 常驻本二进制（effect_steady_state 的测量前提）——全部
//! 组计时同受每分配 2 次原子加的噪声；预算门为 10µs 量级，余量充足。
//!
//! **只产数据**：池组不设阈值、不写机器相关基线注释（§9 性能实践——基线走
//! `perf.yml` 滚动 + artifacts）；本机实测数字落在 verify 报告里，不进源码。
//!
//! 跑法（required-features 并集，见 Cargo.toml）：
//! `cargo bench -p udf-skeleton-infra --all-features --bench infra`

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, black_box, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

use skeleton::notify::{Notify as _, NotifySink};
use skeleton::{Effect, Store as _};
use skeleton_infra::fx::FxScope;
use skeleton_infra::memgov::{CountingAllocator, MemoryPool};
use skeleton_infra::objpool::{ByteBudget, ObjectPool};
use skeleton_infra::pool::{
    AbortPolicy, CallerRunsPolicy, RejectPolicy, ThreadPool, ThreadPoolTuning,
};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::{Journal, JournalError, JsonlJournal};

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
        stop_budgeted.store(true, Ordering::Release);
    });

    group.finish();
}

criterion_group!(
    benches,
    fxscope_spawn_join,
    journal_append_line,
    effect_steady_state,
    dispatch_per_beat,
    notify_dispatch,
    object_pool_acquire_drop,
    memory_pool_acquire_drop,
    thread_pool_reject,
    byte_budget_contention
);
criterion_main!(benches);
