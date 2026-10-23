//! cache 域七维性能矩阵采集器（收窄声明：①get/peek/insert 分配事件面
//! ②get/peek/insert ns 锚 ③容量梯度 100/1k/10k 稳态混合作业；④resize 代价
//! n/a（容量构造期定死）⑤⑥ n/a（无阻塞无自旋面——单线程 &mut 契约明示）
//! ⑦ n/a（无热点计数跨缓存行面））。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-std-ext --example cache_multidim
//! ```
//!
//! 口径注：
//! ① **事件计数**（对齐 fsx_multidim/paths_alloc 口径：只增不减，瞬时生灭
//!     计入）——memgov `live_allocs` 是净存活计数，稳态插拔循环进出相抵恒
//!     0.00，不适用缓存稳态面（首版踩坑，报告 §6 有案）；本地 EventCounting
//!     采集器自持，零 feature 门；
//! ①② 键取 u64（Clone=memcpy 零分配）隔离缓存机制自身面；String 键的
//!     insert 键 clone 分配如实展示（K: Clone 契约已知代价）；get 热路径恒
//!     零 clone 零分配（&K 借用查询）；
//! ③ 稳态口径：先灌满容量（arena/map 全建立），再测混合作业——首灌的
//!     Vec 扩容/HashMap 建表不进稳态读数；
//! ④ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ⑤ black_box 防 LLVM 消除；每个锚三连跑取中位（扁平三次调用，无嵌套闭包）。
//!
//! 报告：reports/2026-10-16-b4-cache.md（agentwiki 根）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton::clock::MockClock;
use std_ext::cache::{LruCache, TtlCache};

/// 分配事件计数器（fsx_multidim 同款：alloc/alloc_zeroed/realloc 各计一次
/// 事件；dealloc 不计——事件面只增不减，瞬时生灭全可见）。
struct EventCounting;

static EVENTS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

// ---- 测量助手 ----

/// op 跑 n 次的分配事件数/次（事件只增不减，无净额相抵问题）。
fn events_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op()); // 预热：懒初始化不进计数
    let before = EVENTS.load(Ordering::Relaxed);
    for _ in 0..n {
        black_box(op());
    }
    (EVENTS.load(Ordering::Relaxed) - before) as f64 / n as f64
}

fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3(a: f64, b: f64, c: f64) -> f64 {
    let mut v = [a, b, c];
    v.sort_by(|x, y| x.partial_cmp(y).unwrap());
    v[1]
}

fn main() {
    // ⓪ 计数器自检（采集器可信面，fail-visible）：String 堆分配必须计 1 事件。
    let probe = events_per(|| String::with_capacity(64).len(), 1_000);
    println!("⓪ 计数器自检：String::with_capacity(64) = {probe:.2} events/call（应 =1）");
    assert!(
        (probe - 1.0).abs() < 1e-9,
        "EventCounting 未计到堆分配事件，本次采集读数不可信"
    );
    perf_alloc_face();
    perf_ns_anchors();
    perf_ttl_anchor();
    perf_capacity_gradient();
}

/// ② TTL 面读锚：TtlCache.get = 内核 get（touch）+ 单调钟读 + 过期比较
/// （MockClock 冻结钟=活条目路径；钟读成本即 TtlCache 对 LruCache 的净税）。
fn perf_ttl_anchor() {
    let clock = Arc::new(MockClock::new());
    let mut c = TtlCache::new(clock.clone(), 1024);
    for i in 0..1024u64 {
        c.insert(i, i, Duration::from_secs(3600));
    }
    let hit = median3(
        batch_mean(|| c.get(&7u64).copied(), 100_000),
        batch_mean(|| c.get(&7u64).copied(), 100_000),
        batch_mean(|| c.get(&7u64).copied(), 100_000),
    );
    let ev = events_per(|| c.get(&7u64).copied(), 10_000);
    println!("②′ TtlCache.get 命中（MockClock 注入）：{hit:.0} ns / {ev:.2} events（内核 14ns + 钟读净税）");
}

// ---- ① 分配事件面（稳态：命中/未命中/peek/新键插入-逐尾循环）----

fn perf_alloc_face() {
    let mut c = LruCache::new(1024);
    for i in 0..1024u64 {
        c.insert(i, i);
    }
    // 稳态插拔循环键集：1024..（插入即逐尾，容量恒满，arena 槽全复用）
    // 测量面返回属主值（FnMut 闭包禁返捕获引用；查找+touch 照跑，仅拷走返回值）
    let hit = events_per(|| c.get(&7u64).copied(), 10_000);
    let miss = events_per(|| c.get(&u64::MAX).copied(), 10_000);
    let peek = events_per(|| c.peek(&7u64).copied(), 10_000);
    let mut k = 1024u64;
    let mut insert_u64 = || {
        k += 1;
        c.insert(k, k);
    };
    let insert = events_per(&mut insert_u64, 10_000);
    println!("① 分配事件面（u64 键稳态，events/call）：get 命中 {hit:.2} / get 未命中 {miss:.2} / peek {peek:.2} / insert 新键(逐尾循环) {insert:.2}");

    // String 键对照：insert 的键 clone 事件如实展示（K: Clone 契约代价：
    // to_string 1 + 键 clone 1 = 2 事件/次，键值落 map+槽各一份）
    let mut cs = LruCache::new(1024);
    for i in 0..1024u64 {
        cs.insert(i.to_string(), i);
    }
    let mut n = 1024u64;
    let mut insert_s = || {
        n += 1;
        cs.insert(n.to_string(), n);
    };
    let insert_str = events_per(&mut insert_s, 10_000);
    let ks = "777".to_string();
    let get_str = events_per(|| cs.get(&ks).is_some(), 10_000);
    println!("  String 键对照：insert 新键 {insert_str:.2}（键两份=to_string+clone）/ get 命中 {get_str:.2}（热路径仍零事件）");
}

// ---- ② ns 锚（u64 键；HashMap 查找为底盘）----

fn perf_ns_anchors() {
    let mut c = LruCache::new(1024);
    for i in 0..1024u64 {
        c.insert(i, i);
    }
    let hit = median3(
        batch_mean(|| c.get(&7u64).copied(), 100_000),
        batch_mean(|| c.get(&7u64).copied(), 100_000),
        batch_mean(|| c.get(&7u64).copied(), 100_000),
    );
    let miss = median3(
        batch_mean(|| c.get(&u64::MAX).copied(), 100_000),
        batch_mean(|| c.get(&u64::MAX).copied(), 100_000),
        batch_mean(|| c.get(&u64::MAX).copied(), 100_000),
    );
    let peek = median3(
        batch_mean(|| c.peek(&7u64).copied(), 100_000),
        batch_mean(|| c.peek(&7u64).copied(), 100_000),
        batch_mean(|| c.peek(&7u64).copied(), 100_000),
    );
    let mut k = 1024u64;
    let mut insert_once = || {
        k += 1;
        c.insert(k, k);
    };
    let insert = median3(
        batch_mean(&mut insert_once, 100_000),
        batch_mean(&mut insert_once, 100_000),
        batch_mean(&mut insert_once, 100_000),
    );
    println!("② ns 锚（u64 键，median3）：get 命中 {hit:.0} / get 未命中 {miss:.0} / peek {peek:.0} / insert 新键 {insert:.0}");
}

// ---- ③ 容量梯度 100/1k/10k：稳态混合作业应平坦（O(1) 判据）----

fn mixed_workload(cap: u64, ops: u64) -> f64 {
    let mut c = LruCache::new(cap as usize);
    for i in 0..cap {
        c.insert(i, i);
    }
    let mut k = cap;
    let mut getk = 0u64;
    let t = Instant::now();
    for i in 0..ops {
        // 确定性混合作业：3 读 1 写；读键周期扫（局部性中性），写键循环逐尾
        match i % 4 {
            0..=2 => {
                getk = getk.wrapping_add(1);
                black_box(c.get(&(getk % cap)).copied());
            }
            _ => {
                k += 1;
                c.insert(k, k);
            }
        }
    }
    t.elapsed().as_nanos() as f64 / ops as f64
}

fn perf_capacity_gradient() {
    let c100 = median3(
        mixed_workload(100, 300_000),
        mixed_workload(100, 300_000),
        mixed_workload(100, 300_000),
    );
    let c1k = median3(
        mixed_workload(1_000, 300_000),
        mixed_workload(1_000, 300_000),
        mixed_workload(1_000, 300_000),
    );
    let c10k = median3(
        mixed_workload(10_000, 300_000),
        mixed_workload(10_000, 300_000),
        mixed_workload(10_000, 300_000),
    );
    println!("③ 容量梯度稳态混合作业 ns/op：cap=100: {c100:.0}  cap=1k: {c1k:.0}  cap=10k: {c10k:.0}（平坦=O(1) 判据）");
}
