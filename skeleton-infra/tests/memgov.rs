#![cfg(feature = "memgov")]
//! memgov 集成测试：声明 [`CountingAllocator`] 的泄漏断言面。
//!
//! 本二进制与 lib 测试二进制的分工：
//! - 泄漏断言依赖全局挂载 CountingAllocator，只能在集成测试二进制内跑（lib 挂载会让
//!   降级断言失真，且 delta 隔离要求专用二进制）；
//! - CI 侧必须 `-- --test-threads=1` 串行（泄漏 delta 被同二进制并发分配污染
//!   = flaky-by-construction）；本文件另以互斥锁（隔离测试间并发）+ `settle()`（把基线
//!   读取挪出 libtest 主线程打印前序结果的分配窗口）兜底，使裸 `cargo test`（默认并行）
//!   在本二进制内同样确定；CI 确定口径仍以串行闸为准。

use std::alloc::{GlobalAlloc, Layout};
use std::sync::{Mutex, MutexGuard};

use skeleton_infra::memgov::{
    expect_empty, BoundedRegistry, CountingAllocator, Eviction, LeakGuard,
};

/// 计数分配器挂全局（本二进制专用；lib 测试二进制不挂——测试 ⑩ 的降级前提）。
#[global_allocator]
static COUNTING: CountingAllocator = CountingAllocator;

/// 文件内串行闸（见模块文档）。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    // 测试 panic 会毒化互斥锁；毒化不影响串行语义，直接复用。
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// 等 libtest 主线程打印完前序测试结果（并发模式下「完成事件 → format+打印 ok 行」的
/// 分配窗口与持锁在跑测试的全局读数窗口物理重叠，互斥锁隔离不了主线程——实测 8 跑 2 挂）。
/// 基线读取前 settle，把读数窗口挪出打印窗口；CI 确定口径仍是串行闸。
///
/// flaky 治理：定长 2ms 是猜窗——重负载下
/// 打印窗未排空时读数仍会撞（历史 8 跑 2 挂的残余面）。改为**静止检测**：轮询至全局
/// 计数器相邻两次读数一致（1ms 间隔）才认基线成立——打印窗真实排空的时刻收敛，
/// 不赌固定时长；500 次有界上限永不挂死，超限放行最后读数、由断言定成败（fail-visible）。
fn settle() {
    let mut prev = (
        CountingAllocator::live_allocs(),
        CountingAllocator::live_bytes(),
    );
    for _ in 0..500 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        let now = (
            CountingAllocator::live_allocs(),
            CountingAllocator::live_bytes(),
        );
        if now == prev {
            return;
        }
        prev = now;
    }
}

/// ⑥ CountingAllocator 计数：分配/释放/峰值/reset；alloc 失败（null）不计数。
#[test]
fn counting_allocator_counts_alloc_dealloc_peak_and_reset() {
    let _serial = serial();
    settle();

    // 失败分配不计数：System 对超大 Layout 返回 null（8 EiB 在任何真实宿主上都失败）。
    let huge = Layout::from_size_align(isize::MAX as usize, 1).expect("layout is valid");
    let before_allocs = CountingAllocator::live_allocs();
    let before_bytes = CountingAllocator::live_bytes();
    // SAFETY: layout 由 from_size_align 构造、约束已满足；仅探测失败路径，
    // 返回值断言必为 null，不存在需配对 dealloc 的指针。
    let null_ptr = unsafe { COUNTING.alloc(huge) };
    assert!(null_ptr.is_null(), "huge alloc must fail on any real host");
    assert_eq!(CountingAllocator::live_allocs(), before_allocs);
    assert_eq!(CountingAllocator::live_bytes(), before_bytes);

    let v = Vec::<u8>::with_capacity(1024);
    assert_eq!(CountingAllocator::live_allocs(), before_allocs + 1);
    assert_eq!(CountingAllocator::live_bytes(), before_bytes + 1024);
    drop(v);
    assert_eq!(CountingAllocator::live_allocs(), before_allocs);
    assert_eq!(CountingAllocator::live_bytes(), before_bytes);

    CountingAllocator::reset_peak();
    assert_eq!(CountingAllocator::peak_bytes(), 0);
    let big = Vec::<u8>::with_capacity(4096);
    assert!(CountingAllocator::peak_bytes() >= 4096);
    CountingAllocator::reset_peak();
    assert_eq!(CountingAllocator::peak_bytes(), 0);
    drop(big);
}

/// ⑥b churn 累计计数（终盲测槽点 3）：total_allocs/total_deallocs 只增不减，
/// churn 窗口 = total 差值——live 净额「即用即还恒 0」盲区（同量 churn 不可辨）
/// 的对账面。分配 N 次 total 至少 +N；drop M 个 total_allocs 不减、deallocs 至少 +M。
#[test]
fn total_alloc_dealloc_counters_track_churn_monotonically() {
    let _serial = serial();
    settle();

    // 分配 N=8 次（每 Vec 精确一次 alloc；外层预容纳扩容不扰计数窗）。
    let a0 = CountingAllocator::total_allocs();
    let d0 = CountingAllocator::total_deallocs();
    let mut keep: Vec<Vec<u8>> = Vec::with_capacity(8);
    for i in 0..8u8 {
        keep.push(vec![i; 16]);
    }
    let a1 = CountingAllocator::total_allocs();
    assert!(
        a1 >= a0 + 8,
        "分配 N 次后 total_allocs 至少 +N（实际 +{}）",
        a1 - a0
    );

    // drop M=5 个：total_allocs 不减（累计语义单调），total_deallocs 至少 +M。
    let before_drop = a1;
    keep.truncate(3);
    let d1 = CountingAllocator::total_deallocs();
    assert!(
        CountingAllocator::total_allocs() >= before_drop,
        "dealloc 不得回拨 total_allocs（累计计数单调不减）"
    );
    assert!(
        d1 >= d0 + 5,
        "drop M 个后 total_deallocs 至少 +M（实际 +{}）",
        d1 - d0
    );
    drop(keep);
}

/// ⑦ LeakGuard 干净路径 Ok。
#[test]
fn leak_guard_reports_clean_scope_as_ok() {
    let _serial = serial();
    settle();
    let guard = LeakGuard::arm();
    let v = Vec::<u8>::with_capacity(128);
    drop(v);
    guard.check().expect("freed everything: no leak");
}

/// ⑧ LeakGuard 泄漏路径 Err 且 delta 正确（分配后不释放）。
#[test]
fn leak_guard_reports_leak_with_exact_delta() {
    let _serial = serial();
    settle();
    let guard = LeakGuard::arm();
    let leaked = Vec::<u8>::with_capacity(256);
    std::mem::forget(leaked);
    let report = guard.check().expect_err("forgotten allocation is a leak");
    assert_eq!(report.allocs_delta, 1);
    assert_eq!(report.bytes_delta, 256);
    // guard Drop 的兜底 eprintln 在本测结束后预期出现——尽力而为语义，不参与断言。
}

/// ⑨ expect_empty panic 路径（带 len/capacity 现场）。
#[test]
#[should_panic(expected = "registry not drained")]
fn expect_empty_panics_with_diagnostics() {
    let _serial = serial();
    let mut reg = BoundedRegistry::new(2, Eviction::Reject);
    reg.insert(1, "a").expect("within capacity");
    expect_empty(&reg);
}

/// 零覆盖 #9：realloc 增/缩记账分支——grow 记 (new−old)、shrink 记 (old−new)，
/// live_bytes 恒等于存活布局尺寸之和；realloc 原子替换不新增分配计数，整块
/// 释放后回基线。容量断言全用观测值（v.capacity()），不硬编码 std 增长策略。
#[test]
fn realloc_grow_and_shrink_track_live_bytes() {
    let _serial = serial();
    settle();
    let base = (
        CountingAllocator::live_allocs(),
        CountingAllocator::live_bytes(),
    );

    let mut v = vec![0u8; 64]; // len=cap=64：顶满构造，shrink_to_fit 走缩分支
    v.reserve(2048); // grow：realloc 增分支
    let cap_grown = v.capacity();
    assert!(cap_grown > 64, "reserve 必须扩容");
    assert_eq!(
        CountingAllocator::live_bytes(),
        base.1 + cap_grown,
        "grow 后 live_bytes = 基线 + 当前容量"
    );

    v.shrink_to_fit(); // shrink：realloc 缩分支
    let cap_shrunk = v.capacity();
    assert_eq!(cap_shrunk, v.len(), "shrink_to_fit 收缩到 len");
    assert_eq!(
        CountingAllocator::live_bytes(),
        base.1 + cap_shrunk,
        "shrink 后 live_bytes = 基线 + 收缩后容量"
    );

    drop(v);
    assert_eq!(
        CountingAllocator::live_allocs(),
        base.0,
        "realloc 往返不改变净分配计数"
    );
    assert_eq!(CountingAllocator::live_bytes(), base.1, "全释放回基线");
}

/// cqb⑥ Borrow 泛型查找热路径零分配（armor 性能锚：借用查找禁退化成
/// `to_string` 分配查找）：借用形 get/get_mut 查找窗 live/total 双平。
/// live 净额对「即用即还」恒 0，total 面抓临时分配 churn——两面对账才是真
/// 零分配（口径同 total_allocs 文档）。
///
/// 含分配路径的代价口径（旧文「10×+ 慢」不实，按实测改写）：PerfProbe
/// （`verify/perf-probe/` 的 `registry` 组，借用查找 vs `to_string` 三口径
/// 10k/44c/100 键，7 轮中位）实测 **1.5–1.9×**——差 ~50ns ≈ 一次 alloc +
/// 拷贝。本锚守的是「不分配」这一结构性质，不依赖倍数大小。
#[test]
fn borrow_lookup_allocates_nothing() {
    let _serial = serial();
    let mut reg: BoundedRegistry<String, u32> = BoundedRegistry::new(4, Eviction::Reject);
    reg.insert("alpha".to_owned(), 1).expect("within capacity");
    settle();
    let base = (
        CountingAllocator::live_allocs(),
        CountingAllocator::live_bytes(),
        CountingAllocator::total_allocs(),
    );

    for _ in 0..16 {
        assert_eq!(reg.get("alpha"), Some(&1));
        assert_eq!(reg.get("zeta"), None);
        assert!(reg.get_mut("alpha").is_some());
    }

    assert_eq!(
        CountingAllocator::live_allocs(),
        base.0,
        "借用形查找必须零新增分配"
    );
    assert_eq!(CountingAllocator::live_bytes(), base.1);
    assert_eq!(
        CountingAllocator::total_allocs(),
        base.2,
        "total 面抓即用即还型临时分配（to_string 退化即红）"
    );
}

/// borrow-remove 语义①：`&str` 摘 `String` 键——值正确返回、len 递减、
/// iter 仍升序确定序（乱序插入后摘中间键断言序）。
#[test]
fn borrow_remove_by_str_returns_value_and_shrinks_in_order() {
    let _serial = serial();
    let mut reg: BoundedRegistry<String, u32> = BoundedRegistry::new(4, Eviction::Reject);
    reg.insert("delta".to_owned(), 4).expect("within capacity");
    reg.insert("alpha".to_owned(), 1).expect("within capacity");
    reg.insert("charlie".to_owned(), 3)
        .expect("within capacity");
    reg.insert("bravo".to_owned(), 2).expect("within capacity");

    assert_eq!(reg.remove("bravo"), Some(2));
    assert_eq!(reg.len(), 3);
    let keys: Vec<&str> = reg.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, ["alpha", "charlie", "delta"]);
    assert_eq!(reg.remove("alpha"), Some(1));
    let rest: Vec<&str> = reg.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(rest, ["charlie", "delta"]);
}

/// borrow-remove 语义②：未命中=`None`（不 panic）；摘除腾位后同键 re-insert
/// 正常，且满表 Reject 的 fail-closed 语义不被 remove 改写。
#[test]
fn remove_missing_is_none_and_reinsert_after_remove_works() {
    let _serial = serial();
    let mut reg: BoundedRegistry<String, u8> = BoundedRegistry::new(1, Eviction::Reject);
    reg.insert("solo".to_owned(), 1).expect("within capacity");

    assert_eq!(reg.remove("ghost"), None);
    assert_eq!(reg.remove("solo"), Some(1));
    assert!(reg.is_empty());

    reg.insert("solo".to_owned(), 2).expect("within capacity");
    assert_eq!(reg.get("solo"), Some(&2));
    assert!(reg.insert("other".to_owned(), 3).is_err(), "真满仍 Reject");
}

/// cqb⑦ borrow-remove 零分配锚 + 差分对照：`String` 键表窗内借用形
/// `remove("key")` 分配事件面（total）精确平——摘除释放节点不进 total；
/// live 面只降不增（释放 ≥1 个存活分配，证明摘除真发生而非空转）。
/// 同表临时键路径 `remove(&key.clone())` 必须 ≥1 事件——差分即仪器自检
/// （证明锚窗内分配可见），借用形省掉的正是这一次分配。与
/// `borrow_lookup_allocates_nothing` 的口径差异：remove 是结构性收缩，
/// live 净额方向向下，故 live 面锚上界「不增」而非精确平；total 面才是
/// 「零分配」的硬断言（同源同仪两面对账）。
#[test]
fn borrow_remove_allocates_nothing() {
    let _serial = serial();
    let mut reg: BoundedRegistry<String, u32> = BoundedRegistry::new(4, Eviction::Reject);
    reg.insert("alpha".to_owned(), 1).expect("within capacity");
    settle();
    let base = (
        CountingAllocator::live_allocs(),
        CountingAllocator::live_bytes(),
        CountingAllocator::total_allocs(),
    );

    assert_eq!(reg.remove("alpha"), Some(1));
    assert!(
        CountingAllocator::live_allocs() < base.0,
        "摘除必须释放 ≥1 个存活分配（键缓冲/节点），否则锚空转"
    );
    assert!(
        CountingAllocator::live_bytes() <= base.1,
        "摘除禁净增存活字节"
    );
    assert_eq!(
        CountingAllocator::total_allocs(),
        base.2,
        "total 面：借用摘除零分配事件（禁临时键 churn）"
    );

    // 差分对照：先回填，再走临时键路径——&key.clone() 构造 owned 临时，窗内
    // 必须 ≥1 分配事件；0 即仪器不可见/锚空转，红。
    reg.insert("alpha".to_owned(), 1).expect("within capacity");
    settle();
    let diff_base = CountingAllocator::total_allocs();
    let key = "alpha".to_owned();
    assert_eq!(reg.remove(&key), Some(1));
    let diff = CountingAllocator::total_allocs() - diff_base;
    assert!(
        diff >= 1,
        "临时键路径必须 ≥1 分配事件（差分对照+仪器自检），实测 {diff}"
    );
}

/// skeleton store 分配契约的黑盒孪生（skeleton lib 白盒测的边界测量面）。
/// skeleton 依赖走 testing feature 门；--all-features 下激活。
#[cfg(feature = "testing")]
mod skeleton_alloc_twin {
    use std::convert::Infallible;
    use std::hint::black_box;

    use super::*;
    use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
    use skeleton_infra::runtime::SyncStore as Store;

    enum Bump {
        Bump,
    }

    struct Counter;

    impl Reducer for Counter {
        type State = u32;
        type Action = Bump;
        type Op = Infallible;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: Bump, out: &mut EffectBuf<Bump, Infallible>) -> u32 {
            // 恒产一条 Cancel 效果：把效果缓冲的复用面一并纳入测量
            out.push(Effect::Cancel {
                id: EffectId::new(1),
            });
            s.wrapping_add(1)
        }
    }

    struct NoFx;

    impl EffectRuntime<Bump, Infallible> for NoFx {
        fn run(&mut self, _: Effect<Bump, Infallible>, _: &ActionSender<Bump>) {}
    }

    /// sender() 克隆零新增分配：每次调用重建 Arc 闭包（修复前的形态）
    /// 即每调用 +1 alloc，立即红。功能面孪生（多克隆同达一 store）在
    /// skeleton tests/store_behavior_gaps.rs。
    #[test]
    fn sender_clones_are_allocation_free() {
        let _serial = serial();
        settle();
        let store = Store::new(Counter, NoFx);
        let mut handles = Vec::with_capacity(8);
        let base = (
            CountingAllocator::live_allocs(),
            CountingAllocator::live_bytes(),
        );

        for _ in 0..8 {
            handles.push(store.sender());
        }
        black_box(&handles);

        assert_eq!(
            CountingAllocator::live_allocs(),
            base.0,
            "sender() 克隆必须零新增分配（缓存句柄复用）"
        );
        assert_eq!(CountingAllocator::live_bytes(), base.1);
    }

    /// 白盒孪生并集（notify_cache_persists / effect_buf 保容量 / make_mut 原地
    /// 三个白盒的黑盒并集）：稳态 dispatch 零新增分配——订阅快照缓存重建、
    /// 效果缓冲重分配、快照落位伪克隆任一退化立即红。
    #[test]
    fn steady_state_dispatches_are_allocation_free() {
        let _serial = serial();
        settle();
        let store = Store::new(Counter, NoFx);
        let _keep = store.subscribe(|_| {});
        for _ in 0..8 {
            store.dispatch(Bump::Bump); // 预热：队列/缓存/缓冲的首撞分配
        }
        let base = (
            CountingAllocator::live_allocs(),
            CountingAllocator::live_bytes(),
        );

        for _ in 0..64 {
            store.dispatch(Bump::Bump);
        }
        black_box(store.snapshot());

        assert_eq!(
            CountingAllocator::live_allocs(),
            base.0,
            "稳态 dispatch 必须零新增分配（快照缓存/效果缓冲/原地落位任一退化即红）"
        );
        assert_eq!(CountingAllocator::live_bytes(), base.1);
        assert_eq!(*store.snapshot(), 8 + 64, "预热+测量段全部送达");
    }
}

/// PerfProbe 锚回归（双口径：live 净增 + total 累计）：dispatch 命中 /
/// dispatch route-miss 稳态 / notify 单订阅者 三锚，每锚窗内 live=0 **且** total=0。
///
/// 依据：`verify/perf-probe/`（2026-10-28 全量测量；口径 = 分配事件次数；复跑
/// 命令见其 README）。live 净额对「即用即还」恒 0（churn 盲区），total 面抓窗内
/// 临时分配——两面同时为 0 才是真零分配。与 `skeleton_alloc_twin` 的分工：那边测
/// live 净额（预算闸），这边加 total 面（churn 闸），且覆盖异步通知段。
#[cfg(feature = "runtime")]
mod perf_anchor_alloc {
    use std::convert::Infallible;
    use std::hint::black_box;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use super::*;
    use skeleton::notify::{Notify as _, NotifySink};
    use skeleton::{Effect, EffectBuf, EffectRuntime, Reducer, Store as _};
    use skeleton_infra::runtime::SyncStore as Store;

    /// 测量窗读数：(live 净增, live 字节净增, total 累计)。
    fn window(op: impl FnOnce()) -> (i64, i64, usize) {
        let live = CountingAllocator::live_allocs() as i64;
        let bytes = CountingAllocator::live_bytes() as i64;
        let total = CountingAllocator::total_allocs();
        op();
        (
            CountingAllocator::live_allocs() as i64 - live,
            CountingAllocator::live_bytes() as i64 - bytes,
            CountingAllocator::total_allocs() - total,
        )
    }

    /// 命中热路径最小域：零效果「+1」reducer（probe `dispatch/命中` 同形）。
    struct Bump;

    impl Reducer for Bump {
        type State = u32;
        type Action = u32;
        type Op = Infallible;

        fn initial(&self) -> u32 {
            0
        }

        fn reduce(&self, s: &u32, _a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
            s + 1
        }
    }

    struct NoFx;

    impl EffectRuntime<u32, Infallible> for NoFx {
        fn run(&mut self, _: Effect<u32, Infallible>, _: &skeleton::ActionSender<u32>) {}
    }

    /// 投递口只落一个原子（O(1) 契约的最省形态；不引入测量噪声）。
    struct Latest(Arc<AtomicU64>);

    impl NotifySink for Latest {
        fn publish(&self, version: u64) {
            self.0.store(version, Ordering::Relaxed);
        }
    }

    /// 锚①：命中热路径（零效果、零订阅者）稳态 dispatch 双口径零分配。
    /// probe 依据：`dispatch/命中` [mem] total 0.00/次（skeleton-infra 侧稳态零分配，
    /// 快照 make_mut 原地落位 + EffectBuf 保容量后成立）。
    #[test]
    fn dispatch_hit_zero_alloc_dual_caliber() {
        let _serial = super::serial();
        super::settle();
        let store = Store::new(Bump, NoFx);
        for _ in 0..8 {
            store.dispatch(1u32); // 预热：队列/缓冲首撞容量
        }
        let (live, bytes, total) = window(|| {
            for _ in 0..64 {
                store.dispatch(black_box(1u32));
            }
        });
        assert_eq!(
            (live, bytes, total),
            (0, 0, 0),
            "命中稳态 dispatch 必须双口径零分配（live 净增 / total churn 各 0）"
        );
        assert_eq!(*store.snapshot(), 8 + 64, "预热+测量段全部送达");
    }

    /// 锚③：有 1 个异步订阅者时，投递增量零分配（Phase 1 钉子）。
    /// 依据：PerfProbe `notify/dispatch 1 异步订阅者` [mem] total = 1.00/次——
    /// 每拍 `collect::<Vec<_>>()` 一次堆分配（修复前）。修复后收集缓冲挂
    /// StoreInner 跨拍复用，本锚转绿；回退成每拍新建即红。
    #[test]
    fn notify_single_subscriber_zero_alloc_dual_caliber() {
        let _serial = super::serial();
        super::settle();
        let store = Store::new(Bump, NoFx);
        let seen = Arc::new(AtomicU64::new(0));
        let _ticket = store.subscribe_notify(Latest(Arc::clone(&seen)));
        for _ in 0..8 {
            store.dispatch(1u32); // 预热：收集缓冲首撞容量
        }
        let published0 = store.notify_published();
        let (live, bytes, total) = window(|| {
            for _ in 0..64 {
                store.dispatch(black_box(1u32));
            }
        });
        assert_eq!(
            (live, bytes, total),
            (0, 0, 0),
            "1 订阅者时投递稳态必须双口径零分配（每拍 collect 回退即红）"
        );
        assert_eq!(
            store.notify_published() - published0,
            64,
            "每拍恰一次投递（语义不变）"
        );
        assert_eq!(
            seen.load(Ordering::Relaxed),
            8 + 64,
            "版本号经 publish 出站（订阅后逐拍 +1）"
        );
    }

    /// 锚②：route-miss 稳态 dispatch 双口径零分配（compose 域）。
    /// miss 路径 = `route_misses` 计数 + 状态克隆原样返回；状态是纯标量
    /// （Clone = memcpy）时零分配——probe `bc-route/miss reduce` 口径同源。
    #[cfg(feature = "compose")]
    #[test]
    fn dispatch_route_miss_zero_alloc_dual_caliber() {
        use skeleton::Fragment;

        let _serial = super::serial();
        super::settle();

        #[derive(Clone, Debug, PartialEq)]
        struct Parent {
            total: u32,
            child: u32,
        }

        #[derive(Clone, Debug, PartialEq)]
        enum PAct {
            Child(u32),
            Miss,
        }

        struct ChildAdd;

        impl Reducer for ChildAdd {
            type State = u32;
            type Action = u32;
            type Op = Infallible;

            fn initial(&self) -> u32 {
                0
            }

            fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
                s + a
            }
        }

        /// 装配一次持有的委托父 reducer（compose.rs 正典形态）。
        struct Delegate<F> {
            frag: F,
        }

        impl<F> Reducer for Delegate<F>
        where
            F: Fragment<State = Parent, Action = PAct, Op = Infallible> + Send + Sync,
        {
            type State = Parent;
            type Action = PAct;
            type Op = Infallible;

            fn initial(&self) -> Parent {
                Parent { total: 0, child: 0 }
            }

            fn reduce(&self, s: &Parent, a: PAct, out: &mut EffectBuf<PAct, Infallible>) -> Parent {
                self.frag.reduce(s, a, out)
            }
        }

        struct NoFx2;

        impl EffectRuntime<PAct, Infallible> for NoFx2 {
            fn run(&mut self, _: Effect<PAct, Infallible>, _: &skeleton::ActionSender<PAct>) {}
        }

        let store = Store::new(
            Delegate {
                frag: skeleton_infra::combine!(ChildAdd, Parent.child, PAct::Child),
            },
            NoFx2,
        );
        // 命中一次：证明路由臂在工作（miss 断言才有鉴别力）
        store.dispatch(PAct::Child(1));
        assert_eq!(store.snapshot().child, 1, "命中臂路由到子切片");
        for _ in 0..8 {
            store.dispatch(PAct::Miss); // 预热：miss 路径首次落位
        }
        let (live, bytes, total) = window(|| {
            for _ in 0..64 {
                store.dispatch(black_box(PAct::Miss));
            }
        });
        assert_eq!(
            (live, bytes, total),
            (0, 0, 0),
            "route-miss 稳态必须双口径零分配（状态为纯标量，clone = memcpy）"
        );
        assert_eq!(
            *store.snapshot(),
            Parent { total: 0, child: 1 },
            "miss 只计数留痕、状态克隆原样返回（语义不变）"
        );
    }
}
