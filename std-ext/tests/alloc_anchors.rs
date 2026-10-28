//! PerfProbe 锚回归（std-ext 侧双口径）：cache get+insert / retry_async 首试 /
//! RandomId 三处热路径的「零分配」断言。
//!
//! 口径：本文件自带**事件计数**分配器（形态照 `tests/paths_alloc.rs`——同一
//! 测量纪律：lib 测试二进制禁挂全局分配器，集成测试自带）。alloc/realloc/
//! alloc_zeroed 各计 1 次事件，只计**本线程窗内**（IN_WINDOW 门，const TLS 免
//! 分配）——libtest 并发线程的分配不进窗，整跑免疫污染。
//!
//! 为何事件计 0 同时满足「live 净增 = 0 且 total 累计 = 0」：total 就是事件
//! 累计，无事件即无 churn；无分配即无净增。反过来事件计数比 `memgov::
//! CountingAllocator` 的 live 净额严格（能抓窗内生灭的瞬时件），故本文件只报
//! 事件数，断言 0 等价于双口径同时为 0。
//!
//! 依据：`verify/perf-probe/`（2026-10-28 全量测量，release profile，7 轮中位；
//! 复跑命令见其 README；组名见各测试注释）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// 分配事件累计（窗内 + 本线程，见 [`IN_WINDOW`]）。
static EVENTS: AtomicUsize = AtomicUsize::new(0);

/// 文件内串行闸：setenv/时基类全局态在本文件暂不涉及，但串行闸保留
/// （读数窗已按线程归属，不依赖串行；与 paths_alloc 同形，便于对齐口径）。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、
// 无析构注册，在分配器内读取不会递归进分配。
thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: 纯转发 System，仅在窗内转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // try_with: 线程 TLS 析构期访问会 panic（分配器内 panic=abort），
        // 取不到就当不在窗内。
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

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// op 执行期的分配事件数（瞬时生灭也计入；结果活到函数尾，drop 不进窗；
/// 只计本线程，其他线程的并发分配不进窗）。
fn allocs<T>(op: impl FnOnce() -> T) -> usize {
    IN_WINDOW.with(|w| w.set(true));
    let before = EVENTS.load(Ordering::Relaxed);
    let out = op();
    black_box(&out);
    let events = EVENTS.load(Ordering::Relaxed) - before;
    IN_WINDOW.with(|w| w.set(false));
    events
}

/// 仪器自检：窗内分配必须可见——防「分配器没挂上 / 门没开」导致的假绿
/// （本文件其余断言全是「0 事件」，仪器坏了照样绿）。
#[test]
fn alloc_counter_sees_allocations_in_window() {
    let _serial = serial();
    let events = allocs(|| Vec::<u8>::with_capacity(32));
    assert_eq!(events, 1, "窗内一次分配必须计 1 事件（仪器在位）");
}

// ═══ cache 域：SyncLruCache / SyncTtlCache get+insert ═══
//
// 依据：PerfProbe `cache` 组（SyncLru/SyncTtl 分配事件面，逐路径计事件）——
// get 命中与已有键覆盖插入都不新分配；只有**新键**插入需要哈希表扩容/建表项
// （本锚只用已有键，故窗内恒 0）。

use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::cache::{SyncLruCache, SyncTtlCache};

/// SyncLruCache 稳态 get 命中 + 已有键覆盖插入：零事件。
#[test]
fn sync_lru_cache_get_insert_zero_alloc() {
    let _serial = serial();
    let cache: SyncLruCache<u64, u64> = SyncLruCache::new(64);
    // 预热：建表项 + 触及 LRU 结点分配
    for k in 0..64u64 {
        cache.insert(k, k);
        black_box(cache.get(&k));
    }

    let events = allocs(|| {
        for k in 0..64u64 {
            black_box(cache.get(&k));
            cache.insert(k, k + 1);
        }
    });
    assert_eq!(
        events, 0,
        "SyncLruCache get 命中 + 已有键覆盖插入不得分配（新键扩容才分配，不属稳态）"
    );
    assert_eq!(cache.get(&7), Some(8), "覆盖插入生效");
}

/// SyncTtlCache 稳态 get 命中（惰性过期检查走注入钟）+ 已有键覆盖插入：零事件。
#[test]
fn sync_ttl_cache_get_insert_zero_alloc() {
    let _serial = serial();
    let clock = Arc::new(MockClock::new());
    let cache: SyncTtlCache<u64, u64> = SyncTtlCache::new(clock.clone(), 64);
    let ttl = Duration::from_secs(60);
    // 预热：建表项/过期元数据
    for k in 0..64u64 {
        cache.insert(k, k, ttl);
        black_box(cache.get(&k));
    }

    let events = allocs(|| {
        for k in 0..64u64 {
            black_box(cache.get(&k));
            cache.insert(k, k + 1, ttl);
        }
    });
    assert_eq!(
        events, 0,
        "SyncTtlCache get 命中 + 已有键覆盖插入不得分配（过期钟读不分配）"
    );
    assert_eq!(cache.get(&7), Some(8), "覆盖插入生效");
}

// ═══ retry 域：retry_async 首试 ═══
//
// 依据：PerfProbe `retry` 组（retry_async 适配层，ReadySleeper 排除 sleep 成本）
// ——首试成功路径不睡眠、不装箱、不收集。

use std::future::{ready, Future};
use std::task::{Context, Poll, Waker};

use std_ext::retry::{retry_async, AsyncSleeper, RetryPolicy};

/// 即刻就绪的睡眠口（首试成功路径压根不会走到它）。
struct ReadySleeper;

impl AsyncSleeper for ReadySleeper {
    fn sleep(&self, _d: Duration) -> impl Future<Output = ()> + Send {
        ready(())
    }
}

/// 最小阻塞驱动：ReadySleeper 下 future 必然一步内就绪；有界循环防挂死。
fn run_to_ready<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    let mut cx = Context::from_waker(Waker::noop());
    for _ in 0..64 {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
    panic!("future 在 64 次 poll 内未就绪（ReadySleeper 应立即完成）");
}

/// retry_async 首试成功：零事件（无装箱、无重试簿记分配）。
#[test]
fn retry_async_first_attempt_zero_alloc() {
    let _serial = serial();
    let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_secs(1));
    // 预热：常量折叠/内联判定的首次落地
    run_to_ready(retry_async(
        &policy,
        |_attempt| async { Ok::<u32, &'static str>(7) },
        ReadySleeper,
    ))
    .expect("首试成功");

    let events = allocs(|| {
        run_to_ready(retry_async(
            &policy,
            |_attempt| async { Ok::<u32, &'static str>(7) },
            ReadySleeper,
        ))
    });
    assert_eq!(events, 0, "retry_async 首试成功路径不得分配");
}

// ═══ random 域：RandomId v4 ═══
//
// 依据：PerfProbe `id` 组（v4 单枚 + fill 批量，值面恒零分配；OS CSPRNG 直写
// 调用方栈/切片缓冲）。首调用可能触发 OS 层一次性初始化，故预热后计。

#[cfg(feature = "random")]
mod random_id {
    use super::*;
    use std_ext::id::RandomId;

    /// 单枚 v4：零事件（16 字节栈缓冲直填）。
    #[test]
    fn random_id_single_zero_alloc() {
        let _serial = serial();
        black_box(RandomId::new().expect("OS CSPRNG 可用"));

        let events = allocs(|| RandomId::new().expect("OS CSPRNG 可用"));
        assert_eq!(events, 0, "RandomId::new 不得分配（栈缓冲直填）");
    }

    /// 批量 fill：零事件（调用方缓冲，框架不中转拷贝）。
    #[test]
    fn random_id_fill_batch_zero_alloc() {
        let _serial = serial();
        let mut buf = [0u8; 64];
        RandomId::fill(&mut buf[..8]).expect("OS CSPRNG 可用");

        let events = allocs(|| {
            for chunk in buf.chunks_mut(8) {
                RandomId::fill(chunk).expect("OS CSPRNG 可用");
            }
        });
        assert_eq!(events, 0, "RandomId::fill 批量不得分配");
    }
}
