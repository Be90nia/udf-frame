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
    let mut prev = (CountingAllocator::live_allocs(), CountingAllocator::live_bytes());
    for _ in 0..500 {
        std::thread::sleep(std::time::Duration::from_millis(1));
        let now = (CountingAllocator::live_allocs(), CountingAllocator::live_bytes());
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
    let mut reg = BoundedRegistry::bounded(2, Eviction::Reject);
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
    let base = (CountingAllocator::live_allocs(), CountingAllocator::live_bytes());

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
    assert_eq!(CountingAllocator::live_allocs(), base.0, "realloc 往返不改变净分配计数");
    assert_eq!(CountingAllocator::live_bytes(), base.1, "全释放回基线");
}

/// skeleton store 分配契约的黑盒孪生（skeleton lib 白盒测的边界测量面）。
/// skeleton 依赖走 testing feature 门；--all-features 下激活。
#[cfg(feature = "testing")]
mod skeleton_alloc_twin {
    use std::convert::Infallible;
    use std::hint::black_box;

    use super::*;
    use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store};

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
            out.push(Effect::Cancel { id: EffectId(1) });
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
        let base = (CountingAllocator::live_allocs(), CountingAllocator::live_bytes());

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
        let base = (CountingAllocator::live_allocs(), CountingAllocator::live_bytes());

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
