//! #53 验收第 1 条的可运行载体：同一份逻辑数据（3 × 64 KiB）在整体布局与
//! 分片布局下的写入代价对比。可观测面 = 一次 dispatch 里状态被**深拷贝**的
//! 字节数：整体布局恒全量，分片布局只深拷贝被 `Arc::make_mut` 触到的那一片。
//! 测不了墙钟、分配事件数、留存增长——那些是 bench 的活，见
//! `skeleton-infra/benches/infra.rs` 的 `state_layout_update` /
//! `state_layout_update_scope` / `state_layout_retention` 三组。
//! 持一份快照的边际代价：整体布局再付一遍全量，分片布局只付根句柄拷贝
//! （不计入本计数器）⇒ 每份留存快照钉住的 ≈ 单片字节。
//!
//! 跑法：`cargo run -p udf-skeleton-infra --example state_layout`

use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 状态数据的最小复制单位；只统计它的 `Clone`（= 真实深拷贝字节）。
static COPIED: AtomicUsize = AtomicUsize::new(0);

struct Payload(Vec<u8>);

impl Clone for Payload {
    fn clone(&self) -> Self {
        COPIED.fetch_add(self.0.len(), Ordering::Relaxed);
        Self(self.0.clone())
    }
}

const PART_BYTES: usize = 64 * 1024;
const PARTS: usize = 3;

/// 整体布局：同样 3 × 64 KiB 逻辑数据挤成单块，根 `Clone` = 全量深拷贝
/// （整体提交无法选择性重建）。
#[derive(Clone)]
struct Mono(Payload);

/// 分片布局：根 `Clone` 只拷 `Arc` 句柄；只有被 `Arc::make_mut` 触到的片
/// 深拷贝，其余片继续与旧状态共享。
#[derive(Clone)]
struct Sharded(Vec<Arc<Payload>>);

enum Msg {
    Touch,
}

struct MonoReducer;

impl Reducer for MonoReducer {
    type State = Mono;
    type Action = Msg;
    type Op = Infallible;

    fn initial(&self) -> Mono {
        Mono(Payload(vec![0; PARTS * PART_BYTES]))
    }

    fn reduce(&self, s: &Mono, _a: Msg, _out: &mut EffectBuf<Msg, Infallible>) -> Mono {
        let mut next = s.clone(); // 整体布局固有税：全量深拷贝
        next.0.0[0] = 1;
        next
    }
}

struct ShardedReducer;

impl Reducer for ShardedReducer {
    type State = Sharded;
    type Action = Msg;
    type Op = Infallible;

    fn initial(&self) -> Sharded {
        Sharded(
            (0..PARTS)
                .map(|_| Arc::new(Payload(vec![0; PART_BYTES])))
                .collect(),
        )
    }

    fn reduce(&self, s: &Sharded, _a: Msg, _out: &mut EffectBuf<Msg, Infallible>) -> Sharded {
        let mut next = s.clone(); // 根浅拷贝：只加 `Arc` 计数
        Arc::make_mut(&mut next.0[0]).0[0] = 1; // 只深拷贝被碰到的片
        next
    }
}

/// 一次 dispatch 的状态深拷贝字节；`retain` = 是否持一份当前快照再发布。
fn copied<S, A, Op>(store: &Store<S, A, Op>, action: A, retain: bool) -> usize
where
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
{
    let _snap = retain.then(|| store.snapshot()); // 钉住当前世代 ⇒ 发布走克隆分支
    let before = COPIED.load(Ordering::Relaxed);
    store.dispatch(action);
    COPIED.load(Ordering::Relaxed) - before
}

fn main() {
    let mono = Store::new(MonoReducer, NoFx);
    let sharded = Store::new(ShardedReducer, NoFx);
    println!(
        "状态总量 {} B；每行 = 一次 dispatch 的状态深拷贝",
        PARTS * PART_BYTES
    );
    let mono_held = copied(&mono, Msg::Touch, true);
    let mono_free = copied(&mono, Msg::Touch, false);
    let sharded_held = copied(&sharded, Msg::Touch, true);
    let sharded_free = copied(&sharded, Msg::Touch, false);
    // 主张闸：整体布局恒拷全量；分片布局恒只拷被碰的一片（口径漂移即红）。
    assert_eq!(mono_free, PARTS * PART_BYTES);
    assert_eq!(sharded_held, PART_BYTES);
    assert_eq!(sharded_free, PART_BYTES);
    for (label, bytes) in [
        ("整体布局 + 持快照", mono_held),
        ("整体布局 无快照  ", mono_free),
        ("分片布局 + 持快照", sharded_held),
        ("分片布局 无快照  ", sharded_free),
    ] {
        println!("{label}: {bytes:>6} B");
    }

    let before = sharded.snapshot(); // 旧快照被钉住时，未碰的片与它共享同一份数据
    sharded.dispatch(Msg::Touch);
    let after = sharded.snapshot();
    assert!(Arc::ptr_eq(&before.0[1], &after.0[1]) && Arc::ptr_eq(&before.0[2], &after.0[2]));
    assert!(!Arc::ptr_eq(&before.0[0], &after.0[0]));
    println!("未碰片 1/2 与旧快照同址；碰过的片 0 已分叉，旧快照未被就地改写");

    mono.close();
    sharded.close();
}
