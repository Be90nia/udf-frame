//! infra bench 三组：fxscope_spawn_join /
//! journal_append_line / effect_steady_state。
//!
//! 注意：CountingAllocator 常驻本二进制（effect_steady_state 的测量前提）——三组
//! 计时同受每分配 2 次原子加的噪声；预算门为 10µs 量级，余量充足。
//!
//! 跑法（required-features 并集，见 Cargo.toml）：
//! `cargo bench -p udf-skeleton-infra --all-features --bench infra`

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};

use skeleton::notify::{Notify as _, NotifySink};
use skeleton::{Effect, Store as _};
use skeleton_infra::fx::FxScope;
use skeleton_infra::memgov::CountingAllocator;
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

criterion_group!(
    benches,
    fxscope_spawn_join,
    journal_append_line,
    effect_steady_state,
    dispatch_per_beat,
    notify_dispatch
);
criterion_main!(benches);
