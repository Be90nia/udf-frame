//! MemoryPool 内存池多维实测采集器（拍板性能矩阵的池侧六维 ①②③④⑤⑦；
//! ⑥零自旋为线程池专属维度——本池 condvar park 结构性留痕见 ④b/⑤b）。
//! 补臂（2026-10-16 拍板）：③b 无锁对照臂（Treiber stack 纯 std，**只在采集器
//! 内实现不落产品**——量化单权威锁衰减换来的简单性值多少）；⑤ 延迟分布专项
//! （借还 p50/p99/p99.9 + 池空等待者唤醒分布）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example memory_pool_profile
//! ```
//!
//! 口径注（沿 multidim_eval 先例）：
//! ① CountingAllocator 常驻本二进制（内存维前提），计时含每分配 2 次 Relaxed
//!    原子加噪声（量级 ns）；
//! ② 粗测 std Instant 非 criterion——数字供判级留痕，不供微优化基线；机器噪声
//!    敏感，结论看量级与曲线形状，不看个位数；
//! ③ 并发段只断言不变量不计内存；热路径 = 闲槽直取（LIFO），本采集器先预热
//!    把工厂/容量分配挪出热窗。

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use skeleton_infra::memgov::{CountingAllocator, MemoryPool};

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

const ACQUIRE_TIMEOUT: Duration = Duration::from_millis(100);

/// 压测窗内每线程循环：借出→碰一件→归还，计成功次数。
fn hammer(pool: &MemoryPool<Vec<u8>>, stop: &AtomicBool) -> u64 {
    let mut ops = 0u64;
    while !stop.load(Ordering::Relaxed) {
        if let Ok(g) = pool.acquire(Duration::from_millis(10)) {
            black_box(g.is_empty());
            drop(g);
            ops += 1;
        }
    }
    ops
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

fn main() {
    let pool = Arc::new(MemoryPool::<Vec<u8>>::new(64, 64, Vec::new));
    let warm: Vec<_> = (0..64)
        .filter_map(|_| pool.acquire(ACQUIRE_TIMEOUT).ok())
        .collect();
    for g in warm {
        drop(g);
    }
    println!("=== MemoryPool multidim profile (release, 64-deep pool prewarmed) ===");

    // ── ① 稳态零分配：借/还热路径 10k 次 alloc delta 必须为 0 ──────────────
    // 双口径（total 联用判级，memgov CountingAllocator::total_allocs 口径注）：
    // live delta=0 且 churn=0 → 窗口真零分配；live delta=0 且 churn>0 → 高 churn
    // 零泄漏（净额盲区的辨识面）。
    let before = CountingAllocator::live_allocs();
    let churn_before = CountingAllocator::total_allocs();
    for _ in 0..10_000 {
        drop(pool.acquire(ACQUIRE_TIMEOUT).unwrap());
    }
    let delta = CountingAllocator::live_allocs() - before;
    let churn = CountingAllocator::total_allocs() - churn_before;
    println!("① steady-state allocs per 10k borrow/return: {delta} (expect 0; churn={churn} — 0/0=真零分配, 0/n>0=高churn零泄漏)");

    // ── ② uncontended 借还往返锚（单线程闲槽直取）──────────────────────────
    let n = 100_000u64;
    let t0 = Instant::now();
    for _ in 0..n {
        drop(pool.acquire(ACQUIRE_TIMEOUT).unwrap());
    }
    let mean = t0.elapsed().as_nanos() as f64 / n as f64;
    println!("② uncontended borrow+return roundtrip: {mean:.1} ns/op (batch mean over {n})");

    // ── ③ 竞争扩展性：1/2/4/8 线程吞吐曲线 ─────────────────────────────────
    println!("③ contended throughput (400ms window, timeout=10ms):");
    for threads in [1usize, 2, 4, 8] {
        let stop = Arc::new(AtomicBool::new(false));
        let mut handles = Vec::new();
        for _ in 0..threads {
            let (pool, stop) = (Arc::clone(&pool), Arc::clone(&stop));
            handles.push(thread::spawn(move || hammer(&pool, &stop)));
        }
        thread::sleep(Duration::from_millis(400));
        stop.store(true, Ordering::Relaxed);
        let total: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        println!(
            "    threads={threads:>2}  ops/s={:>10}",
            total * 1_000 / 400
        );
    }

    // ── ③b 无锁对照臂：同形 hammer（取件→碰一件→还件），量化锁形态差距 ─────
    // Treiber stack 只在采集器内实现（不落产品）：若差距 <2× 维持单权威锁的
    // 简单性；>2× 无锁升级位提级——数字供拍板，不改任何产品语义。
    println!("③b lock-free comparison (Treiber stack vs single-authority lock, 400ms, 64 slots):");
    let stack = Arc::new(TreiberStack::new(64));
    for threads in [1usize, 2, 4, 8] {
        let pool_op: Arc<dyn Fn() -> bool + Send + Sync> = {
            let pool = Arc::clone(&pool);
            Arc::new(move || {
                if let Ok(g) = pool.acquire(Duration::from_millis(10)) {
                    black_box(g.is_empty());
                    true
                } else {
                    false
                }
            })
        };
        let stack_op: Arc<dyn Fn() -> bool + Send + Sync> = {
            let stack = Arc::clone(&stack);
            Arc::new(move || match stack.pop() {
                Some(i) => {
                    // SAFETY: 槽位数据仅在 pop→push 所有权窗口内被持有者触碰，
                    // 单一所有权转移由栈序保证（bench-only，见 TreiberStack 注）。
                    black_box(unsafe { (*stack.slot(i)).is_empty() });
                    stack.push(i);
                    true
                }
                None => false,
            })
        };
        let lock_tps = run_window(threads, 400, pool_op) * 1_000 / 400;
        let free_tps = run_window(threads, 400, stack_op) * 1_000 / 400;
        println!(
            "    threads={threads:>2}  lock={:>10} ops/s  lockfree={:>10} ops/s  ratio={:.2}x",
            lock_tps,
            free_tps,
            free_tps as f64 / lock_tps.max(1) as f64
        );
    }
    println!("    (判据：ratio <2x 维持单权威锁；>2x 无锁升级位提级——简单性价格单列供拍板)");

    // ── ④ resize 代价：调用自身耗时 + 扩容唤醒等待者延迟 ────────────────────
    // 4a. 调参调用自身（扩 64→128 / 缩 128→4，各 200 次均值/p99——调参暂停窗）
    let mut samples: Vec<u64> = Vec::with_capacity(400);
    for i in 0..400u32 {
        let t = if i % 2 == 0 {
            let t0 = Instant::now();
            pool.resize(skeleton_infra::memgov::MemoryTuning {
                max: 128,
                idle_cap: 128,
            })
            .unwrap();
            t0.elapsed()
        } else {
            let t0 = Instant::now();
            pool.resize(skeleton_infra::memgov::MemoryTuning {
                max: 64,
                idle_cap: 64,
            })
            .unwrap();
            t0.elapsed()
        };
        samples.push(t.as_nanos() as u64);
    }
    samples.sort_unstable();
    println!(
        "④a resize() call duration: mean={:.0}ns p99={}ns (200 expand + 200 shrink)",
        samples.iter().sum::<u64>() as f64 / samples.len() as f64,
        pct(&samples, 99),
    );

    // 4b. 扩容唤醒等待者：max=1 满借 → resize 扩容 → 等待者被即时唤醒
    //     （延迟 = resize 返回 → 等待者 acquire 返回；零阻塞=远小于其自身超时）
    let small = Arc::new(MemoryPool::<Vec<u8>>::new(1, 1, Vec::new));
    let mut wake: Vec<u64> = Vec::with_capacity(100);
    for _ in 0..100 {
        let holder = small.acquire(Duration::from_millis(100)).unwrap();
        let (tx, rx) = mpsc::channel::<Instant>();
        let p = Arc::clone(&small);
        let waiter = thread::spawn(move || {
            let g = p.acquire(Duration::from_millis(5_000)).unwrap();
            tx.send(Instant::now()).unwrap();
            drop(g);
        });
        thread::sleep(Duration::from_millis(2)); // 等待者入 park
        let t0 = Instant::now();
        small
            .resize(skeleton_infra::memgov::MemoryTuning {
                max: 2,
                idle_cap: 1,
            })
            .unwrap();
        let got = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        wake.push(got.duration_since(t0).as_micros() as u64);
        waiter.join().unwrap();
        drop(holder);
        // 回到 (1,1) 基态：多余件随溢出归还离开池宇宙
        small
            .resize(skeleton_infra::memgov::MemoryTuning {
                max: 1,
                idle_cap: 1,
            })
            .unwrap();
    }
    wake.sort_unstable();
    println!(
        "④b expand unblocks parked waiter: mean={:.1}µs p99={}µs (n=100)",
        wake.iter().sum::<u64>() as f64 / wake.len() as f64,
        pct(&wake, 99),
    );

    // 4c. 缩容零等待：锤线程压场中调参调用照常即时返回（不被借出侧阻塞）
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let (pool, stop) = (Arc::clone(&pool), Arc::clone(&stop));
        handles.push(thread::spawn(move || hammer(&pool, &stop)));
    }
    thread::sleep(Duration::from_millis(50));
    let t0 = Instant::now();
    for _ in 0..50 {
        pool.resize(skeleton_infra::memgov::MemoryTuning {
            max: 4,
            idle_cap: 1,
        })
        .unwrap();
        pool.resize(skeleton_infra::memgov::MemoryTuning {
            max: 64,
            idle_cap: 64,
        })
        .unwrap();
    }
    let per_call = t0.elapsed().as_nanos() as f64 / 100.0;
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    println!(
        "④c shrink/expand under 4-thread contention: {per_call:.0} ns/call (zero-wait evidence)"
    );

    // ── ⑤ 延迟分布专项：借还 p50/p99/p99.9 + 池空等待者唤醒分布 ─────────────
    // 5a uncontended 单线程逐发采样（同②闲槽直取路径，分位数口径）。
    let n5 = 50_000usize;
    let mut samples: Vec<u64> = Vec::with_capacity(n5);
    for _ in 0..n5 {
        let t0 = Instant::now();
        drop(pool.acquire(ACQUIRE_TIMEOUT).unwrap());
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    println!(
        "⑤a uncontended borrow+return latency: p50={}ns p99={}ns p99.9={}ns max={}ns (n={n5})",
        pct(&samples, 50),
        pct(&samples, 99),
        permille(&samples, 999),
        samples[n5 - 1],
    );

    // 5b 池空等待：max=4/idle_cap=2，主线程借走 2 件（配额占 2），4 等待线程
    // ×250 次「借→即还」——时延=acquire 进入到拿到件的全程（park 唤醒与竞争
    // 快路径混合分布；2 件在 4 线程间循环，等待者必然经历 condvar park）。
    let sat = Arc::new(MemoryPool::<Vec<u8>>::new(4, 2, Vec::new));
    let held: Vec<_> = (0..2)
        .filter_map(|_| sat.acquire(Duration::from_millis(100)).ok())
        .collect();
    let per = 250usize;
    let mut handles = Vec::new();
    for _ in 0..4 {
        let sat = Arc::clone(&sat);
        handles.push(thread::spawn(move || {
            let mut v = Vec::with_capacity(per);
            for _ in 0..per {
                let t0 = Instant::now();
                let g = sat.acquire(Duration::from_secs(5)).unwrap();
                v.push(t0.elapsed().as_nanos() as u64);
                drop(g);
            }
            v
        }));
    }
    let mut wake: Vec<u64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    drop(held);
    wake.sort_unstable();
    println!(
        "⑤b waiter latency (saturated pool, 4 waiters, 2 circulating buffers): p50={}ns p99={}ns p99.9={}ns max={}ns (n={})",
        permille(&wake, 500),
        permille(&wake, 990),
        permille(&wake, 999),
        wake[wake.len() - 1],
        wake.len(),
    );

    // ── ⑦ 伪共享机制演示：相邻计数 vs align(64) 垫片隔离（2 线程各 5M 自增）──
    mod share {
        use std::sync::atomic::AtomicU64;
        // repr(C)：被测机制就是字段邻接/垫片隔离，布局必须按声明落。
        #[repr(C)]
        pub struct Adjacent(pub AtomicU64, pub AtomicU64);
        #[repr(align(64))]
        pub struct Pad(pub ());
        #[repr(C)]
        pub struct Isolated(pub AtomicU64, pub Pad, pub AtomicU64, pub Pad);
    }
    /// 双线程各撞一个计数器，总耗时摊到每次自增（ns/op）。
    fn bounce(iters: u64, other: impl FnOnce() + Send + 'static, mut mine: impl FnMut()) -> f64 {
        let t0 = Instant::now();
        let h = thread::spawn(other);
        mine();
        h.join().unwrap();
        t0.elapsed().as_nanos() as f64 / iters as f64
    }
    let iters = 5_000_000u64;
    let adj = Arc::new(share::Adjacent(AtomicU64::new(0), AtomicU64::new(0)));
    let adjacent = {
        let a = Arc::clone(&adj);
        bounce(
            iters,
            move || {
                for _ in 0..iters {
                    a.0.fetch_add(1, Ordering::Relaxed);
                }
            },
            || {
                for _ in 0..iters {
                    adj.1.fetch_add(1, Ordering::Relaxed);
                }
            },
        )
    };
    let iso = Arc::new(share::Isolated(
        AtomicU64::new(0),
        share::Pad(()),
        AtomicU64::new(0),
        share::Pad(()),
    ));
    let isolated = {
        let i = Arc::clone(&iso);
        bounce(
            iters,
            move || {
                for _ in 0..iters {
                    i.0.fetch_add(1, Ordering::Relaxed);
                }
            },
            || {
                for _ in 0..iters {
                    iso.2.fetch_add(1, Ordering::Relaxed);
                }
            },
        )
    };
    println!("⑦ false-sharing bounce (2 threads × 5M inc on adjacent counters):");
    println!("    adjacent={adjacent:.1} ns/op  isolated(pad)={isolated:.1} ns/op");
    println!("    (MemoryPool 布局恒为 isolated 形：pad0/pad1 把账本锁、槽位锁、condvar 分线)");

    let m = pool.metrics();
    println!(
        "final sanity: borrowed={} (expect 0) idle={} dropped_overflow={}（④c 缩容竞争窗的溢出裁决累计）",
        m.core.borrowed, m.core.idle, m.core.dropped_overflow
    );
}

/// 千分位分位（p50/p99/p99.9 口径；`p`∈[0,1000]）。
fn permille(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 1000).min(sorted.len() - 1)]
}

/// 固定窗吞吐 harness（③b 两臂同形）：`op` 返回 true=成功一件（失败重试不计
/// 成功数——与池 acquire 超时不计数的口径对齐）。
fn run_window(threads: usize, ms: u64, op: Arc<dyn Fn() -> bool + Send + Sync>) -> u64 {
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..threads {
        let (stop, op) = (Arc::clone(&stop), Arc::clone(&op));
        handles.push(thread::spawn(move || {
            let mut ops = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if op() {
                    ops += 1;
                }
            }
            ops
        }));
    }
    thread::sleep(Duration::from_millis(ms));
    stop.store(true, Ordering::Relaxed);
    handles.into_iter().map(|h| h.join().unwrap()).sum()
}

// ═══ Treiber stack（③b 无锁对照专用，采集器内实现不落产品）══════════════════
// 纯 std CAS 无锁栈：head 打包 (tag:u32 << 32) | idx:u32，head 每次成功 CAS
// tag 自增——同一槽位被弹了又压回（ABA）时 tag 已变，CAS 必败重读。
// SAFETY（bench-only）：槽位数据仅在「pop 成功→push 归还」窗口内被持有者触碰
// （单一所有权转移由栈序保证），`UnsafeCell` 不引入数据竞争；空栈 pop 返回
// None 由调用方重试。本类型不入产品树，不承担产品级 panic-safety/析构审计。
const TREIBER_EMPTY: u32 = u32::MAX;

struct TreiberSlot {
    next: AtomicU64,
    buf: std::cell::UnsafeCell<Vec<u8>>,
}

struct TreiberStack {
    head: AtomicU64,
    slots: Vec<TreiberSlot>,
}

// SAFETY: 槽位数据（UnsafeCell）仅在 pop 成功后的所有权窗口内被持有者触碰，
// 跨线程共享面只有 AtomicU64 域；单一所有权转移由栈序 CAS 保证（bench-only）。
unsafe impl Sync for TreiberStack {}

fn pack(tag: u32, idx: u32) -> u64 {
    ((tag as u64) << 32) | idx as u64
}

fn low(h: u64) -> u32 {
    h as u32
}

fn tag(h: u64) -> u32 {
    (h >> 32) as u32
}

impl TreiberStack {
    fn new(n: usize) -> Self {
        let slots: Vec<TreiberSlot> = (0..n)
            .map(|_| TreiberSlot {
                next: AtomicU64::new(pack(0, TREIBER_EMPTY)),
                buf: std::cell::UnsafeCell::new(Vec::new()),
            })
            .collect();
        for (i, slot) in slots.iter().enumerate().take(n - 1) {
            slot.next.store(pack(0, i as u32 + 1), Ordering::Relaxed);
        }
        Self {
            head: AtomicU64::new(pack(0, 0)),
            slots,
        }
    }

    /// 弹出槽位下标（空栈=None；tag 防 ABA）。next 域用 u64 打包同款 tag——
    /// push 写入的 next 即当时 head 快照，CAS 成功即代表快照仍新鲜。
    fn pop(&self) -> Option<usize> {
        loop {
            let h = self.head.load(Ordering::Acquire);
            let idx = low(h);
            if idx == TREIBER_EMPTY {
                return None;
            }
            let next = self.slots[idx as usize].next.load(Ordering::Acquire);
            let new_head = pack(tag(h).wrapping_add(1), low(next));
            if self
                .head
                .compare_exchange_weak(h, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some(idx as usize);
            }
        }
    }

    /// 归还槽位（压栈；tag 自增防 ABA）。
    fn push(&self, idx: usize) {
        loop {
            let h = self.head.load(Ordering::Acquire);
            self.slots[idx].next.store(h, Ordering::Relaxed);
            let new_head = pack(tag(h).wrapping_add(1), idx as u32);
            if self
                .head
                .compare_exchange_weak(h, new_head, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    /// 槽位数据引用（仅 pop→push 所有权窗口内调用，SAFETY 见类型注）。
    fn slot(&self, idx: usize) -> *mut Vec<u8> {
        self.slots[idx].buf.get()
    }
}
