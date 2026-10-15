//! infra bench 三组：fxscope_spawn_join /
//! journal_append_line / effect_steady_state。
//!
//! 注意：CountingAllocator 常驻本二进制（effect_steady_state 的测量前提）——三组
//! 计时同受每分配 2 次原子加的噪声；预算门为 10µs 量级，余量充足。
//!
//! 跑法（required-features 并集，见 Cargo.toml）：
//! `cargo bench -p skeleton-infra --all-features --bench infra`

use std::path::Path;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use serde::{Deserialize, Serialize};

use skeleton::{Effect, Store as _};
use skeleton_infra::fx::FxScope;
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::memgov::CountingAllocator;
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
        out.push(Effect::Run { id: skeleton::EffectId(s.0 + 1), op: Beep::One });
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
    // 口径注：live_allocs() 为净存活计数（memgov pub 面无累积分配调用计数），
    // 净零无法区分「除快照外零分配」与「同量 churn」——盲区与泄漏判据
    // 同口径诚实分级（alloc 调用计数口径留 memgov 升级位）。
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

// Journal<A, S> 的 S 与 append 无关，具体化以便方法解析。
fn append_one(journal: &mut JsonlJournal, action: &BenchAction) -> Result<(), JournalError> {
    Journal::<BenchAction, BenchState>::append(journal, action)
}

fn journal_append_line(c: &mut Criterion) {
    // 临时件归 crate 构建隔离目录，禁系统 TEMP/用户目录。
    let dir = Path::new("../../target-iso/bench-journal");
    std::fs::create_dir_all(dir).expect("mkdir target-iso/bench-journal");
    let path = dir.join("bench.jsonl");
    let _ = std::fs::remove_file(&path); // create_new 撞残留=Err，先清
    let mut journal = JsonlJournal::create(&path).expect("create journal");
    let action = BenchAction { id: 1, label: "bench".to_string() };

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
                let handle = scope.spawn(async { 1u8 }).expect("scope open");
                handle.join().await.expect("task ok")
            })
        })
    });
}

criterion_group!(benches, fxscope_spawn_join, journal_append_line, effect_steady_state);
criterion_main!(benches);
