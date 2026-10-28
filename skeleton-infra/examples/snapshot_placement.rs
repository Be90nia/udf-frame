//! 快照落位策略实测采集器：(a) `Arc::make_mut` 就地改写 vs (b) 每拍新 `Arc`。
//!
//! 口径（沿 dispatch_profile / perf_batchc 惯例）：
//! ① 纯计时（std Instant 批均摊薄时钟地板），每锚三连跑取中位；black_box 防消除；
//! ② **clone 计数分离框架税与 reducer 自身税**：小状态 reducer 不克隆（只算
//!    `v+1`）⇒ 全局 clone 计数即框架侧 `make_mut` 克隆分支次数；大状态 reducer
//!    手写字段克隆（`payload.clone()` 走 Vec clone，不经 `Big::clone`）⇒ 计数
//!    仍只记框架侧克隆，两臂口径同源；
//! ③ 分配面用 memgov CountingAllocator 的 **total_allocs 累计差值**（churn 口径：
//!    (b) 每拍 alloc+free ⇒ live 净额恒 0，只能看 churn）；
//! ④ 语义探针：跨拍 `Arc::as_ptr` 是否同址（(a) 稳态同址 / (b) 逐拍换址）、
//!    外部持有者所见快照是否冻结（CoW 语义两策略同）；
//! ⑤ (a) 臂跑在主仓（现行实现）；(b) 臂跑在打了策略补丁的源码副本（同一采集器
//!    同口径），补丁与复现命令见 reports/2026-10-27-snapshot-placement-bench.md。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example snapshot_placement
//! ```

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::memgov::CountingAllocator;
use skeleton_infra::runtime::SyncStore as Store;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 全局 clone 计数：只统计 `State::clone`（框架 `make_mut` 克隆分支触发面）——
/// reducer 侧克隆走字段级 `Vec::clone`，不进本计数 ⇒ 计数即框架税。
static CLONES: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

// ── 测量助手（dispatch_profile 同款）──

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let mut xs = [
        batch_mean(&mut op, n),
        batch_mean(&mut op, n),
        batch_mean(&mut op, n),
    ];
    xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    xs[1]
}

// ── 小状态：reducer 不克隆 ⇒ clone 计数即框架侧 make_mut 克隆分支次数 ──

#[derive(Debug)]
struct Small {
    v: u64,
}

impl Clone for Small {
    fn clone(&self) -> Self {
        CLONES.fetch_add(1, Ordering::Relaxed);
        Small { v: self.v }
    }
}

enum Tick {
    Tick,
}

struct SmallReducer;

impl Reducer for SmallReducer {
    type State = Small;
    type Action = Tick;
    type Op = Infallible;

    fn initial(&self) -> Small {
        Small { v: 0 }
    }

    fn reduce(&self, s: &Small, _: Tick, _: &mut EffectBuf<Tick>) -> Small {
        Small { v: s.v + 1 }
    }
}

// ── 大状态（1 MiB payload）：reducer 手写字段克隆，框架税与 reducer 税可分离 ──

const BIG_BYTES: usize = 1024 * 1024;

#[derive(Debug)]
struct Big {
    payload: Vec<u8>,
    v: u64,
}

impl Clone for Big {
    fn clone(&self) -> Self {
        CLONES.fetch_add(1, Ordering::Relaxed);
        Big {
            payload: self.payload.clone(),
            v: self.v,
        }
    }
}

struct BigReducer;

impl Reducer for BigReducer {
    type State = Big;
    type Action = Tick;
    type Op = Infallible;

    fn initial(&self) -> Big {
        Big {
            payload: vec![0u8; BIG_BYTES],
            v: 0,
        }
    }

    fn reduce(&self, s: &Big, _: Tick, _: &mut EffectBuf<Tick>) -> Big {
        // reducer 自身克隆（字段级 Vec::clone，不经 Big::clone ⇒ 不进 CLONES 计数）
        Big {
            payload: s.payload.clone(),
            v: s.v + 1,
        }
    }
}

/// 一臂的实测读数。
struct Arm {
    label: String,
    ns: f64,
    clones_per_dispatch: f64,
    allocs_per_dispatch: f64,
}

/// 小状态臂：无外部持有者 vs 有外部持有者（跨拍持有 `Arc<S>`）。
fn small_arms() -> Vec<Arm> {
    let mut arms = Vec::new();

    // 无外部持有者：make_mut 强计数=1 ⇒ 原地写入，零克隆零分配（现行实现的稳态）。
    {
        let store = Store::new(SmallReducer, NoFx);
        for _ in 0..10_000 {
            store.dispatch(Tick::Tick);
        }
        let clones_before = CLONES.load(Ordering::Relaxed);
        let allocs_before = CountingAllocator::total_allocs();
        let ns = median3(|| store.dispatch(Tick::Tick), 200_000);
        let clones = CLONES.load(Ordering::Relaxed) - clones_before;
        let allocs = CountingAllocator::total_allocs() - allocs_before;
        arms.push(Arm {
            label: "small/无持有者".to_string(),
            ns,
            clones_per_dispatch: clones as f64 / 600_000.0,
            allocs_per_dispatch: allocs as f64 / 600_000.0,
        });
    }

    // 有外部持有者：强计数≥2 ⇒ make_mut 必走克隆分支（框架深克隆税）。
    {
        let store = Store::new(SmallReducer, NoFx);
        for _ in 0..10_000 {
            store.dispatch(Tick::Tick);
        }
        let held = store.snapshot();
        let clones_before = CLONES.load(Ordering::Relaxed);
        let allocs_before = CountingAllocator::total_allocs();
        let ns = median3(|| store.dispatch(Tick::Tick), 200_000);
        let clones = CLONES.load(Ordering::Relaxed) - clones_before;
        let allocs = CountingAllocator::total_allocs() - allocs_before;
        black_box(&held);
        arms.push(Arm {
            label: "small/有持有者".to_string(),
            ns,
            clones_per_dispatch: clones as f64 / 600_000.0,
            allocs_per_dispatch: allocs as f64 / 600_000.0,
        });
    }

    arms
}

/// 大状态臂（1 MiB payload）：dispatch 延迟含 reducer 自身克隆 + 框架侧克隆。
fn big_arms() -> Vec<Arm> {
    let mut arms = Vec::new();
    const N: u64 = 200;

    {
        let store = Store::new(BigReducer, NoFx);
        for _ in 0..20 {
            store.dispatch(Tick::Tick);
        }
        let clones_before = CLONES.load(Ordering::Relaxed);
        let allocs_before = CountingAllocator::total_allocs();
        let ns = median3(|| store.dispatch(Tick::Tick), N);
        let clones = CLONES.load(Ordering::Relaxed) - clones_before;
        let allocs = CountingAllocator::total_allocs() - allocs_before;
        arms.push(Arm {
            label: "big(1MiB)/无持有者".to_string(),
            ns,
            clones_per_dispatch: clones as f64 / (N * 3) as f64,
            allocs_per_dispatch: allocs as f64 / (N * 3) as f64,
        });
    }

    {
        let store = Store::new(BigReducer, NoFx);
        for _ in 0..20 {
            store.dispatch(Tick::Tick);
        }
        let held = store.snapshot();
        let clones_before = CLONES.load(Ordering::Relaxed);
        let allocs_before = CountingAllocator::total_allocs();
        let ns = median3(|| store.dispatch(Tick::Tick), N);
        let clones = CLONES.load(Ordering::Relaxed) - clones_before;
        let allocs = CountingAllocator::total_allocs() - allocs_before;
        black_box(&held);
        arms.push(Arm {
            label: "big(1MiB)/有持有者".to_string(),
            ns,
            clones_per_dispatch: clones as f64 / (N * 3) as f64,
            allocs_per_dispatch: allocs as f64 / (N * 3) as f64,
        });
    }

    arms
}

/// 语义探针：跨拍 `Arc::as_ptr` 同址性（(a) 稳态同址 / (b) 逐拍换址）+ 外部持有者
/// 所见快照冻结性（CoW 语义两策略同）。
fn semantic_probes() {
    // 小状态跨拍指针：快照 Arc 必须**持有**（持有即强计数≥2 ⇒ 下一拍走克隆分支，
    // 与「无持有者稳态原地写入」不同臂）——故此处只读地址不保留，
    // 释放后的地址可能被下一拍复用 ⇒ 指针同址只作**弱信号**，以分配/克隆计数为准。
    let store = Store::new(SmallReducer, NoFx);
    for _ in 0..10 {
        store.dispatch(Tick::Tick);
    }
    let s1 = store.snapshot();
    let p1 = Arc::as_ptr(&s1);
    drop(s1);
    store.dispatch(Tick::Tick);
    let s2 = store.snapshot();
    let p2 = Arc::as_ptr(&s2);
    drop(s2);
    store.dispatch(Tick::Tick);
    let s3 = store.snapshot();
    let p3 = Arc::as_ptr(&s3);
    drop(s3);
    println!(
        "跨拍 Arc 指针同址（小状态，无持有者）: p1==p2 {} / p2==p3 {}（弱信号：释放后地址可复用）",
        p1 == p2,
        p2 == p3
    );

    // 持有者所见快照冻结（外部持有跨拍 ⇒ 旧值必须不变）
    let store = Store::new(SmallReducer, NoFx);
    store.dispatch(Tick::Tick);
    let held = store.snapshot();
    let held_v = held.v;
    for _ in 0..5 {
        store.dispatch(Tick::Tick);
    }
    println!(
        "外部持有者快照冻结: 持有时 v={held_v} → 现在 v={}（应保持 {held_v}）",
        held.v
    );
    println!("当前快照 v={}（5 拍后应=6）", store.snapshot().v);
}

fn main() {
    println!("== 快照落位策略实测（(a)=Arc::make_mut 就地改写 / (b)=每拍新 Arc）==");
    println!(
        "clone 计数口径：仅框架侧 State::clone（小状态 reducer 不克隆；大状态 reducer \
         走字段级 Vec::clone 不入计数）⇒ clones/dispatch = 框架 make_mut 克隆分支次数"
    );

    semantic_probes();

    let mut arms = small_arms();
    arms.extend(big_arms());
    println!();
    println!(
        "{:<24} {:>14} {:>18} {:>20}",
        "臂", "ns/dispatch", "clones/dispatch", "allocs/dispatch"
    );
    for a in &arms {
        println!(
            "{:<24} {:>14.1} {:>18.4} {:>20.4}",
            a.label, a.ns, a.clones_per_dispatch, a.allocs_per_dispatch
        );
    }
}
