//! std-ext bench：cache get/insert / retry_async 首试 / RandomId v4 / scheduler 调度器。
//!
//! 跑法：`cargo bench -p udf-std-ext --bench std_ext`（RandomId 组需 `--features
//! random`，默认关闭时本组空转不报错）。
//!
//! 取数口径对齐 PerfProbe（`verify/perf-probe/` 的 `cache`/`retry`/`id` 组）：
//! criterion 默认采样取中位，与 probe 的 7 轮中位量级可比；绝对值随机器漂移。
//! 既有组（cache/retry/random）：分配面由集成测试钉住（`tests/alloc_anchors.rs`），
//! bench 只测时间；**scheduler 组另做分配器字节计量**（见下「字节计量」段）。
//!
//! scheduler 组（issue#81）：注册 / 取消 / 取消+回收 / pending 对照 @1k/10k/100k。
//! 每次迭代用**全新调度器**（不继承上一轮的墓碑/堆容量）；注册段在 setup（不计时）、
//! 取消/回收段在 routine（计时）⇒ 成本可归因；远期 deadline（1h）+ MockClock 冻结
//! 时间 ⇒ 测量期间**无任务点火**。回收段走 #52 阈值触发压缩路径（cancel 造墓碑
//! → tick 清扫）；drop 探针计数证明被取消的捕获**在 deadline 前确已释放**，pending
//! 对照则仍存活。
//!
//! 字节计量：进程级计数分配器（每块 16B 头部），**只在计量区段内计数** ⇒ 区段差值
//! = 该区段净保留字节（scheduler 拥有的部分）；**不是 RSS、不是进程级噪声峰值**。
//! 代价：非计量区段每块分配多一次头部写/读（对既有 cache/retry/random 组的影响
//! 见本文件末尾「分配器代价实测」段）。
//!
//! 本机实测（i9-9900K@3.60GHz / Win 10.0.26200，criterion 默认采样中位）：
//! lru_get_hit 28.1 ns / lru_insert_existing_key 24.7 ns / ttl_get_hit 55.1 ns /
//! ttl_insert_existing_key 51.6 ns（TTL 多一次注入钟读）/ retry_async_first_attempt
//! 31.4 ns / random_id_v4 60.0 ns / random_fill_8x8 364.9 ns（8 次 8 字节 fill）。
//! （以上为**接入计数分配器前**的读数；接入后既有组漂移见「分配器代价实测」段。）
//!
//! scheduler 组实测（2026-11-23 同机；每次迭代 = 一个完整批次，criterion 中位）：
//! register 1000/10000/100000 = 144.8 µs / 1.347 ms / 15.34 ms（≈131/141/157 ns/op）；
//! cancel = 39.2 µs / 409.1 µs / 4.589 ms（≈38/41/51 ns/op ⇒ O(1) 快路径成立）；
//! cancel+reclaim = 130.5 µs / 1.348 ms / 17.04 ms；pending_control tick =
//! 103 ns / 156 ns / 569 ns（无墓碑 ⇒ 阈值早退）。
//!
//! 字节读数（计数分配器，计量区段净字节；非 RSS）：基线 192 B；1k/10k/100k 的
//! 峰值（= pending 对照保留量，即「堆构造成本」）94.5 KiB / 1.11 MiB / 10.10 MiB；
//! 取消后**不变**（捕获仍在堆里，cancel 不释放）；清扫后保留 31.25 KiB /
//! 312.5 KiB / 3.05 MiB（= 堆条目数组容量仍在：Entry 32 B/条）；清扫释放
//! 63.25 KiB / 824.5 KiB / 7.05 MiB；drop 探针 = N（取消后的捕获在 deadline 前
//! **确已释放**），pending 对照 drop = 0（任务仍存活）。
//!
//! 分配器代价实测：既有 cache/retry 组热路径**零堆分配**（`scheduler alloc probe`
//! 计量区段净字节差 = 0）⇒ 每块 16B 头部开销不落在其运行时路径上；接入后既有组
//! 在多次全量跑之间于 −5% ~ +16% 区间摆动（同一二进制连跑两遍也摆 ±10% ⇒ 机器
//! 负载 + 二进制布局共同作用，非分配器运行时开销）。PM 建新基线时须知此漂移。

use std::alloc::{GlobalAlloc, Layout, System};
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};

use skeleton::clock::MockClock;
use std_ext::cache::{SyncLruCache, SyncTtlCache};
use std_ext::retry::{AsyncSleeper, RetryPolicy, retry_async};
use std_ext::scheduler::{Scheduler, TaskHandle};

// ---- cache：get 命中 + insert（已有键覆盖）----

fn cache_get_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("sync_cache");

    let lru: SyncLruCache<u64, u64> = SyncLruCache::new(64);
    for k in 0..64u64 {
        lru.insert(k, k);
    }
    group.bench_function("lru_get_hit", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            lru.get(black_box(&k))
        })
    });
    group.bench_function("lru_insert_existing_key", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            lru.insert(black_box(k), k + 1)
        })
    });

    let clock = Arc::new(MockClock::new());
    let ttl: SyncTtlCache<u64, u64> = SyncTtlCache::new(clock, 64);
    let ttl_dur = Duration::from_secs(60);
    for k in 0..64u64 {
        ttl.insert(k, k, ttl_dur);
    }
    group.bench_function("ttl_get_hit", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            ttl.get(black_box(&k))
        })
    });
    group.bench_function("ttl_insert_existing_key", |b| {
        let mut k = 0u64;
        b.iter(|| {
            k = (k + 1) % 64;
            ttl.insert(black_box(k), k + 1, ttl_dur)
        })
    });

    group.finish();
}

// ---- retry：retry_async 首试成功（ReadySleeper，排除 sleep 成本）----

/// 即刻就绪的睡眠口：首试成功路径不会走到它。
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

fn retry_first_attempt(c: &mut Criterion) {
    let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_secs(1));
    c.bench_function("retry_async_first_attempt", |b| {
        b.iter(|| {
            run_to_ready(retry_async(
                &policy,
                |_attempt| async { Ok::<u32, &'static str>(7) },
                ReadySleeper,
            ))
            .expect("首试成功")
        })
    });
}

// ---- random：RandomId v4 单枚 / fill 批量（random feature 门）----

fn random_id(c: &mut Criterion) {
    #[cfg(feature = "random")]
    {
        use std_ext::id::RandomId;

        c.bench_function("random_id_v4", |b| {
            b.iter(|| RandomId::new().expect("OS CSPRNG 可用"))
        });

        let mut buf = [0u8; 64];
        c.bench_function("random_fill_8x8", |b| {
            b.iter(|| {
                for chunk in buf.chunks_mut(8) {
                    RandomId::fill(chunk).expect("OS CSPRNG 可用");
                }
                black_box(&buf);
            })
        });
    }
    // 默认 feature 集下 random 门关闭：本组空转（criterion 照常跑其余组）。
    #[cfg(not(feature = "random"))]
    black_box(c);
}

// ---- 字节计量：进程级计数分配器（计量区段隔离） ----
//
// 口径：`LIVE_BYTES` 只计「计量区段内分配、且尚未释放」的块字节。区段外分配的块
// 即使在区段内被释放也不冲减 ⇒ 区段差值 = 该区段**净保留**字节（scheduler 拥有
// 的部分）。因此本读数不是 RSS、不是进程级噪声峰值。
//
// 块布局：payload 之前放 [size: usize, counted: bool] 头部（16B）；头部偏移按
// payload 对齐向上取整（`16.next_multiple_of(align)`）⇒ payload 对齐不变。

/// 头部字节数（size + counted 标记）。
const HEADER_BYTES: usize = 16;

/// 计量开关：非计量区段的分配不计入字节数。
static ARMED: AtomicBool = AtomicBool::new(false);
/// 计量区段内分配且尚未释放的字节数。
static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

/// 分配头部（payload 之前）：块大小 + 是否计入计量。
struct Header {
    size: usize,
    counted: bool,
}

/// 计数分配器：委托 [`System`]，附每块头部。
struct CountingAlloc;

/// payload 之前的头部偏移（按 payload 对齐向上取整 ⇒ payload 对齐不变）。
fn header_offset(layout: Layout) -> usize {
    HEADER_BYTES.next_multiple_of(layout.align())
}

/// 分配块对齐（≥ `align_of::<Header>()` ⇒ 头部对齐写）。
fn block_align(layout: Layout) -> usize {
    layout.align().max(std::mem::align_of::<Header>())
}

// SAFETY: 头部与 payload 同属一次分配（偏移按 align 向上取整 ⇒ payload 不越界、
// 头部对齐可写）；dealloc/realloc 用同一 layout 重算偏移 ⇒ 与分配侧一致。
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some(total) = header_offset(layout).checked_add(layout.size()) else {
            return std::ptr::null_mut();
        };
        let Ok(block) = Layout::from_size_align(total, block_align(layout)) else {
            return std::ptr::null_mut();
        };
        // SAFETY: block 布局合法（size ≤ isize::MAX、对齐为 2 的幂）。
        let base = unsafe { System.alloc(block) };
        if base.is_null() {
            return base;
        }
        let counted = ARMED.load(Ordering::Relaxed);
        // SAFETY: base 对齐 ≥ align_of::<Header>()（block_align 取 max）。
        unsafe {
            (base as *mut Header).write(Header {
                size: layout.size(),
                counted,
            })
        };
        if counted {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        // SAFETY: offset + size ≤ total ⇒ payload 在块内。
        unsafe { base.add(header_offset(layout)) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let offset = header_offset(layout);
        // SAFETY: ptr 指向本分配器分配的 payload；头部在其前 offset 字节处。
        let base = unsafe { ptr.sub(offset) };
        // SAFETY: base 对齐满足 Header。
        let header = unsafe { (base as *const Header).read() };
        if header.counted {
            LIVE_BYTES.fetch_sub(header.size, Ordering::Relaxed);
        }
        // SAFETY: base/total/align 与分配侧一致（同一 layout 重算）。
        unsafe {
            System.dealloc(
                base,
                Layout::from_size_align_unchecked(layout.size() + offset, block_align(layout)),
            )
        };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            // SAFETY: 委托 dealloc（同一 layout）。
            unsafe { self.dealloc(ptr, layout) };
            return std::ptr::null_mut();
        }
        let offset = header_offset(layout);
        // SAFETY: 同 dealloc（ptr 属本分配器）。
        let base = unsafe { ptr.sub(offset) };
        // SAFETY: 同 dealloc。
        let header = unsafe { (base as *const Header).read() };
        if header.counted {
            LIVE_BYTES.fetch_sub(header.size, Ordering::Relaxed);
        }
        let total = offset.checked_add(new_size);
        let block = total.and_then(|t| Layout::from_size_align(t, block_align(layout)).ok());
        let Some(block) = block else {
            // 失败：原块仍有效 ⇒ 恢复计量。
            if header.counted {
                LIVE_BYTES.fetch_add(header.size, Ordering::Relaxed);
            }
            return std::ptr::null_mut();
        };
        // SAFETY: base 属本分配器；block 布局合法。
        let new_base = unsafe { System.realloc(base, block, total.expect("total 已算出")) };
        if new_base.is_null() {
            // 失败：原块仍有效 ⇒ 恢复计量。
            if header.counted {
                LIVE_BYTES.fetch_add(header.size, Ordering::Relaxed);
            }
            return std::ptr::null_mut();
        }
        let counted = ARMED.load(Ordering::Relaxed);
        // SAFETY: new_base 对齐满足 Header。
        unsafe {
            (new_base as *mut Header).write(Header {
                size: new_size,
                counted,
            })
        };
        if counted {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
        }
        // SAFETY: offset + new_size ≤ total ⇒ payload 在块内。
        unsafe { new_base.add(offset) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 委托 alloc。
        let ptr = unsafe { self.alloc(layout) };
        if !ptr.is_null() {
            // SAFETY: payload 区域 [ptr, ptr+size) 在块内。
            unsafe { std::ptr::write_bytes(ptr, 0, layout.size()) };
        }
        ptr
    }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

// ---- scheduler：注册/取消/取消+回收吞吐 + 墓碑/live/drop/字节报告 ----
//
// 口径（issue#81）：
// - **每次迭代用全新调度器**（不继承上一轮的墓碑/堆容量）；
// - **注册段在 setup（不计时）**、取消/回收段在 routine（计时）⇒ 成本可归因；
// - 远期 deadline（1h）+ MockClock 冻结时间 ⇒ 测量期间**无任务点火**；
// - drop 探针：闭包捕获被析构时计数 ⇒ 证明取消后的捕获**确已释放**；
// - 回收段走 #52 阈值触发压缩路径（cancel 造墓碑 → tick → reclaim_cancelled）。

/// 远期 deadline：远到测量期间绝不可能点火。
const FAR_FUTURE: Duration = Duration::from_secs(3600);

/// 捕获析构探针：闭包被释放时计数（证明取消后捕获确已析构，而非只置了状态位）。
struct DropProbe(Arc<AtomicUsize>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// 全新调度器（每次迭代新建 ⇒ 不继承墓碑/堆容量）。
fn fresh_scheduler() -> Scheduler {
    Scheduler::new(Arc::new(MockClock::new()))
}

/// 注册阶段：注册 n 个远期任务（捕获探针）⇒ 句柄入 `handles`（持有 ⇒ 任务 live）。
fn phase_register(
    sched: &Scheduler,
    n: usize,
    probe: &Arc<AtomicUsize>,
    handles: &mut Vec<TaskHandle>,
) {
    for _ in 0..n {
        let p = DropProbe(Arc::clone(probe));
        handles.push(
            sched
                .after(FAR_FUTURE, move || {
                    // 闭包体永不到点（远期 deadline）：捕获只随闭包析构释放 ⇒
                    // 探针计数即「捕获确已释放」的证据。
                    drop(p);
                })
                .expect("未关闸 ⇒ 注册成功"),
        );
    }
}

/// 取消阶段：逐个取消（O(1) 快路径，不摘堆）⇒ 返回本次成功取消数。
fn phase_cancel(handles: &[TaskHandle]) -> usize {
    let mut cancelled = 0;
    for h in handles {
        if h.cancel() {
            cancelled += 1;
        }
    }
    cancelled
}

fn scheduler(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler");
    // 重 workload（1k/10k/100k）：样本数压到 10、测量窗 2s ⇒ criterion 按单次
    // 迭代耗时自适应迭代数（每次迭代 = 一个完整批次）；预热窗压到 1s（默认 3s
    // × 12 个用例太贵）。2s 覆盖 100k 档单样本所需 ~1.7s（否则 criterion 报
    // 「Unable to complete 10 samples」警告）。
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));

    for size in [1_000usize, 10_000, 100_000] {
        // ① 注册吞吐：setup 建全新调度器（不计时），routine 只做注册（计时）。
        group.bench_function(format!("register/{size}"), |b| {
            b.iter_batched(
                || {
                    (
                        fresh_scheduler(),
                        Arc::new(AtomicUsize::new(0)),
                        Vec::with_capacity(size),
                    )
                },
                |(sched, probe, mut handles)| {
                    phase_register(&sched, size, &probe, &mut handles);
                    // 句柄与调度器的析构落在计时区段外（输出值在计时结束后析构）。
                    (sched, probe, handles)
                },
                BatchSize::PerIteration,
            )
        });

        // ② 取消吞吐：setup 注册（不计时），routine 取消（计时；O(1) 快路径，
        //    不摘堆 ⇒ 捕获仍在堆里）。
        group.bench_function(format!("cancel/{size}"), |b| {
            b.iter_batched(
                || {
                    let sched = fresh_scheduler();
                    let probe = Arc::new(AtomicUsize::new(0));
                    let mut handles = Vec::with_capacity(size);
                    phase_register(&sched, size, &probe, &mut handles);
                    (sched, probe, handles)
                },
                |(sched, probe, handles)| {
                    let cancelled = phase_cancel(&handles);
                    // 取消不解构捕获（墓碑仍在堆里）⇒ 探针计数应为 0。
                    let dropped = probe.load(Ordering::SeqCst);
                    assert_eq!(cancelled, size, "首次取消应全部成功");
                    assert_eq!(dropped, 0, "取消阶段不解构捕获（墓碑仍在堆里）");
                    (sched, probe, handles)
                },
                BatchSize::PerIteration,
            )
        });

        // ③ 取消 + 回收：走 #52 阈值触发压缩路径（cancel 造墓碑 → tick 清扫）。
        group.bench_function(format!("cancel_reclaim/{size}"), |b| {
            b.iter_batched(
                || {
                    let sched = fresh_scheduler();
                    let probe = Arc::new(AtomicUsize::new(0));
                    let mut handles = Vec::with_capacity(size);
                    phase_register(&sched, size, &probe, &mut handles);
                    (sched, probe, handles)
                },
                |(sched, probe, handles)| {
                    let cancelled = phase_cancel(&handles);
                    // 释放句柄 ⇒ 堆内条目成为捕获的唯一持有者 ⇒ 此后捕获的释放
                    // 只可能来自清扫路径。
                    drop(handles);
                    // 清扫：tick 触发 #52 阈值清扫（墓碑数越下限且占比达阈值）。
                    let fired = sched.tick();
                    let live = sched.live_registrations();
                    let heap_empty = sched.next_deadline().is_none();
                    let dropped = probe.load(Ordering::SeqCst);
                    assert_eq!(cancelled, size, "首次取消应全部成功");
                    assert_eq!(fired, 0, "远期 deadline ⇒ 测量期间不得点火");
                    assert_eq!(live, 0, "取消后活跃注册数应归零");
                    assert!(heap_empty, "清扫后堆应为空");
                    assert_eq!(dropped, size, "清扫应在 deadline 前释放全部捕获");
                    (sched, probe)
                },
                BatchSize::PerIteration,
            )
        });

        // ④ pending 对照：远期但未取消 ⇒ 无墓碑 ⇒ 阈值早退、任务仍存活。
        group.bench_function(format!("pending_control/{size}"), |b| {
            b.iter_batched(
                || {
                    let sched = fresh_scheduler();
                    let probe = Arc::new(AtomicUsize::new(0));
                    let mut handles = Vec::with_capacity(size);
                    phase_register(&sched, size, &probe, &mut handles);
                    (sched, probe, handles)
                },
                |(sched, probe, handles)| {
                    let fired = sched.tick(); // 无墓碑 ⇒ 阈值早退（O(1)）
                    let live = sched.live_registrations();
                    let heap_empty = sched.next_deadline().is_none();
                    let dropped = probe.load(Ordering::SeqCst);
                    assert_eq!(fired, 0, "远期 deadline ⇒ 不得点火");
                    assert_eq!(live, size, "pending 任务应仍活跃");
                    assert!(!heap_empty, "pending 堆应非空");
                    assert_eq!(dropped, 0, "pending 捕获不应被析构");
                    (sched, probe, handles)
                },
                BatchSize::PerIteration,
            )
        });
    }
    group.finish();

    // 附加读数（不参与 criterion 计时）：ns/op + ops/s 汇总、字节计量、分配探针。
    print_ops_report();
    print_bytes_report();
    print_alloc_probe();
}

/// ns/op + ops/s 汇总（自测重复 5 次取中位；独立于 criterion 计时区段）。
fn print_ops_report() {
    const REPS: usize = 5;
    for size in [1_000usize, 10_000, 100_000] {
        let mut reg = Vec::with_capacity(REPS);
        let mut can = Vec::with_capacity(REPS);
        let mut rec = Vec::with_capacity(REPS);
        let mut pend = Vec::with_capacity(REPS);
        for _ in 0..REPS {
            let sched = fresh_scheduler();
            let probe = Arc::new(AtomicUsize::new(0));
            let mut handles = Vec::with_capacity(size);

            let t0 = Instant::now();
            phase_register(&sched, size, &probe, &mut handles);
            reg.push(t0.elapsed());

            let t1 = Instant::now();
            let cancelled = phase_cancel(&handles);
            can.push(t1.elapsed());
            assert_eq!(cancelled, size, "首次取消应全部成功");

            // 回收：释放句柄 + 阈值清扫（#52 压缩路径）。
            drop(handles);
            let t2 = Instant::now();
            let fired = sched.tick();
            rec.push(t2.elapsed());
            assert_eq!(fired, 0, "远期 deadline ⇒ 不得点火");
            assert_eq!(probe.load(Ordering::SeqCst), size, "清扫应释放全部捕获");

            // pending 对照：远期未取消 ⇒ tick 早退、任务仍存活。
            let sched2 = fresh_scheduler();
            let probe2 = Arc::new(AtomicUsize::new(0));
            let mut handles2 = Vec::with_capacity(size);
            phase_register(&sched2, size, &probe2, &mut handles2);
            let t3 = Instant::now();
            let fired2 = sched2.tick();
            pend.push(t3.elapsed());
            assert_eq!(fired2, 0, "远期 deadline ⇒ 不得点火");
            assert_eq!(sched2.live_registrations(), size, "pending 任务应仍活跃");
            assert_eq!(probe2.load(Ordering::SeqCst), 0, "pending 捕获不应被析构");
            drop(handles2);
        }
        let reg_ns = median_ns(reg);
        let can_ns = median_ns(can);
        let rec_ns = median_ns(rec);
        let pend_ns = median_ns(pend);
        eprintln!(
            "scheduler ops | size={size} | register={:.1} ns/op ({:.0} ops/s) | \
             cancel={:.1} ns/op ({:.0} ops/s) | cancel+reclaim={:.1} ns/op ({:.0} ops/s) | \
             pending_control_tick={:.1} ns",
            ns_per_op(reg_ns, size),
            ops_per_sec(reg_ns, size),
            ns_per_op(can_ns, size),
            ops_per_sec(can_ns, size),
            ns_per_op(rec_ns, size),
            ops_per_sec(rec_ns, size),
            pend_ns,
        );
    }
}

/// 中位耗时（纳秒）。
fn median_ns(mut xs: Vec<Duration>) -> f64 {
    assert!(!xs.is_empty(), "中位数需要至少一个样本");
    xs.sort();
    xs[xs.len() / 2].as_nanos() as f64
}

/// 每操作纳秒（批次耗时 ÷ 规模）。
fn ns_per_op(total_ns: f64, size: usize) -> f64 {
    total_ns / size as f64
}

/// 每秒操作数。
fn ops_per_sec(total_ns: f64, size: usize) -> f64 {
    size as f64 * 1e9 / total_ns
}

/// 字节计量（隔离区段 + 基线扣减）：注册 → 取消 → 清扫，逐段取差值。
///
/// 返回元组：`(基线, 峰值(pending 对照保留), 取消后保留, 清扫后保留, 清扫释放,
/// 被析构捕获数)`。
fn measure_scheduler_bytes(size: usize) -> (usize, usize, usize, usize, usize, usize) {
    // —— 区段 0：空调度器基线（扣减用）——
    let base_before = LIVE_BYTES.load(Ordering::Relaxed);
    ARMED.store(true, Ordering::Relaxed);
    let sched = fresh_scheduler();
    let baseline = LIVE_BYTES
        .load(Ordering::Relaxed)
        .saturating_sub(base_before);
    ARMED.store(false, Ordering::Relaxed);

    // 夹具（bench 侧，不属 scheduler）：区段外分配 ⇒ 不计入字节。
    let probe = Arc::new(AtomicUsize::new(0));
    let mut handles: Vec<TaskHandle> = Vec::with_capacity(size);

    // —— 区段 1：注册 size 个远期任务（捕获计入）——
    ARMED.store(true, Ordering::Relaxed);
    phase_register(&sched, size, &probe, &mut handles);
    let peak_total = LIVE_BYTES.load(Ordering::Relaxed);
    ARMED.store(false, Ordering::Relaxed);

    // —— 区段 2：取消 size 个（O(1) 快路径；捕获仍在堆里 ⇒ 字节不减）——
    ARMED.store(true, Ordering::Relaxed);
    let cancelled = phase_cancel(&handles);
    assert_eq!(cancelled, size, "首次取消应全部成功");
    let after_cancel_total = LIVE_BYTES.load(Ordering::Relaxed);
    ARMED.store(false, Ordering::Relaxed);

    // —— 区段 3：释放句柄 + 阈值清扫（#52 压缩路径）——
    ARMED.store(true, Ordering::Relaxed);
    drop(handles);
    let fired = sched.tick();
    let after_reclaim_total = LIVE_BYTES.load(Ordering::Relaxed);
    ARMED.store(false, Ordering::Relaxed);

    assert_eq!(fired, 0, "远期 deadline ⇒ 不得点火");
    let dropped = probe.load(Ordering::SeqCst);
    assert_eq!(dropped, size, "清扫应在 deadline 前释放全部捕获");

    let net_base = base_before + baseline;
    (
        baseline,
        peak_total.saturating_sub(net_base),
        after_cancel_total.saturating_sub(net_base),
        after_reclaim_total.saturating_sub(net_base),
        after_cancel_total.saturating_sub(after_reclaim_total),
        dropped,
    )
}

/// 字节报告打印（bench 输出可见；非 RSS、非进程级峰值）。
fn print_bytes_report() {
    for size in [1_000usize, 10_000, 100_000] {
        let (baseline, peak, after_cancel, after_reclaim, reclaimed, dropped) =
            measure_scheduler_bytes(size);
        eprintln!(
            "scheduler bytes | size={size} | baseline={} | peak_pending_control={} | \
             after_cancel={} | after_reclaim={} | reclaimed={} | drops={dropped}",
            human_bytes(baseline),
            human_bytes(peak),
            human_bytes(after_cancel),
            human_bytes(after_reclaim),
            human_bytes(reclaimed),
        );
    }
}

/// 分配探针：既有 cache/retry 组热路径在计量区内是否触发堆分配。
///
/// 计数分配器的每块头部开销只落在**真的分配**的路径上；本探针给出实测证据
/// （计量区段内字节差 = 0 ⇒ 该热路径零堆分配 ⇒ 头部开销不落在该路径）。
fn print_alloc_probe() {
    let lru: SyncLruCache<u64, u64> = SyncLruCache::new(64);
    for k in 0..64u64 {
        lru.insert(k, k);
    }
    let clock = Arc::new(MockClock::new());
    let ttl: SyncTtlCache<u64, u64> = SyncTtlCache::new(clock, 64);
    let ttl_dur = Duration::from_secs(60);
    for k in 0..64u64 {
        ttl.insert(k, k, ttl_dur);
    }
    let policy = RetryPolicy::new(3, Duration::from_millis(10), Duration::from_secs(1));
    const ITERS: usize = 10_000;

    ARMED.store(true, Ordering::Relaxed);
    let mut k = 0u64;
    let before = LIVE_BYTES.load(Ordering::Relaxed);
    for _ in 0..ITERS {
        k = (k + 1) % 64;
        black_box(lru.get(black_box(&k)));
    }
    let lru_get = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);

    let before = LIVE_BYTES.load(Ordering::Relaxed);
    let mut k = 0u64;
    for _ in 0..ITERS {
        k = (k + 1) % 64;
        lru.insert(black_box(k), k + 1);
    }
    let lru_insert = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);

    let before = LIVE_BYTES.load(Ordering::Relaxed);
    let mut k = 0u64;
    for _ in 0..ITERS {
        k = (k + 1) % 64;
        ttl.get(black_box(&k));
    }
    let ttl_get = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);

    let before = LIVE_BYTES.load(Ordering::Relaxed);
    for _ in 0..ITERS {
        black_box(
            run_to_ready(retry_async(
                &policy,
                |_attempt| async { Ok::<u32, &'static str>(7) },
                ReadySleeper,
            ))
            .expect("首试成功"),
        );
    }
    let retry = LIVE_BYTES.load(Ordering::Relaxed).saturating_sub(before);
    ARMED.store(false, Ordering::Relaxed);

    eprintln!(
        "scheduler alloc probe | lru_get={lru_get} B | lru_insert={lru_insert} B | \
         ttl_get={ttl_get} B | retry_first_attempt={retry} B \
         （计量区段内净字节差；0 ⇒ 该热路径零堆分配）"
    );
}

/// 字节数 → 人类可读（二进制单位 KiB/MiB）。
fn human_bytes(bytes: usize) -> String {
    const KI: f64 = 1024.0;
    const MI: f64 = 1024.0 * 1024.0;
    let b = bytes as f64;
    if b >= MI {
        format!("{:.2} MiB", b / MI)
    } else if b >= KI {
        format!("{:.2} KiB", b / KI)
    } else {
        format!("{bytes} B")
    }
}

criterion_group!(
    benches,
    cache_get_insert,
    retry_first_attempt,
    random_id,
    scheduler
);
criterion_main!(benches);
