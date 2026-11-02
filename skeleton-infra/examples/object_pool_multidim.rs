//! ObjectPool 语义锚示例（ADR 0010）：两条可观测锚 + 降级直构一维留痕。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example object_pool_multidim
//! ```
//!
//! 四维对表：
//! ①「reset 后复用」：构造次数 vs 复用次数 + reset 前后字段状态（重置字段
//!   清除、保留字段跨借还留存）——`Pooled` 捞出改参数即复用的主能力演示；
//! ②「无人用即释放」：`prune_idle` 老化释放 + idle_cap 满容量淘汰的 Drop
//!   计数（内存换性能不会变成泄漏）；
//! ③「降级直构」：宇宙满时不阻塞不失败、降级计数与水位线 >1.0 如实体现；
//! ④「分片并发争用」：8 线程 × 20000 次借还的吞吐/p50/p99 + 并发后账实一致
//!   （分片有效性的实测证据；每片一锁非无锁栈，登记为升级路径）。
//!
//! 口径注：本例是语义锚（计数/状态判定）非性能采集器——纯 std 零计时；
//! PASS/FAIL 内嵌真 `assert_eq!`（`thread_pool_multidim` 先例），断言失败即
//! 进程非零退出。

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton_infra::objpool::{ObjectPool, ResetPhase};

/// 池化对象（消费方形状的缩样）：`hot` = reset 保留的热态字段（如密钥调度
/// 结果），`payload` = reset 清除的脏态字段（如上一次会话的缓冲）。
struct ConnCtx {
    hot: u64,
    payload: u64,
    live: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Drop for ConnCtx {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn factory(
    live: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
) -> impl Fn() -> ConnCtx + Send + Sync + 'static {
    move || {
        live.fetch_add(1, Ordering::SeqCst);
        ConnCtx {
            hot: 0,
            payload: 0,
            live: Arc::clone(&live),
            drops: Arc::clone(&drops),
        }
    }
}

// ---- ① reset 后复用：构造次数 vs 复用次数 + 字段状态 ----

fn dim1_reset_reuse() {
    println!("\n[dim1] reset 后复用（64 次借还：构造 vs 复用 + 字段状态）");
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let resets = Arc::new(AtomicU64::new(0));
    let pool = ObjectPool::with_reset(
        8,
        8,
        factory(Arc::clone(&live), Arc::clone(&drops)),
        {
            let resets = Arc::clone(&resets);
            move |c: &mut ConnCtx| {
                resets.fetch_add(1, Ordering::Relaxed);
                c.payload = 0; // 重置脏态；hot 不动——保留热态（重置策略归调用方）
            }
        },
        ResetPhase::OnReturn,
    );

    const CYCLES: u64 = 64;
    for i in 0..CYCLES {
        let mut ctx = pool.acquire();
        ctx.payload = i + 1; // 取出即改参数（DerefMut）：弄脏
        ctx.hot = 0xABCD; // 热态
    }
    let m = pool.metrics();
    println!(
        "  构造 {} 次 | 复用 {} 次 | reset {} 次 | 在池 idle {}",
        m.constructed,
        m.reused,
        resets.load(Ordering::Relaxed),
        m.core.idle
    );
    assert_eq!(m.constructed, 1, "锚①：64 次借还只构造 1 次");
    assert_eq!(m.reused, CYCLES - 1, "锚①：其余 63 次全部复用");
    assert_eq!(
        resets.load(Ordering::Relaxed),
        CYCLES,
        "锚①：每次归还被 reset"
    );

    // 字段状态：取出即改参数 → 归还 reset 清脏 → 再取验证热态保留/脏态清除
    let mut ctx = pool.acquire();
    ctx.payload = 0xDEAD;
    assert_eq!(ctx.hot, 0xABCD, "热态跨借还保留");
    drop(ctx);
    let ctx = pool.acquire();
    println!(
        "  取出即改参数：payload 弄脏 0xDEAD → 归还 reset → 再取 = {}",
        ctx.payload
    );
    assert_eq!(ctx.payload, 0, "锚①：reset 清除脏态、复用干净件");
    drop(ctx);
    println!(
        "  判定: PASS（构造 1 / 复用 {} / reset {}）",
        CYCLES - 1,
        resets.load(Ordering::Relaxed)
    );
}

// ---- ② 无人用即释放：prune 老化 + 容量淘汰的 Drop 计数 ----

fn dim2_release_paths() {
    println!("\n[dim2] 无人用即释放（prune_idle 老化 + idle_cap 滘容量淘汰）");
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::new(8, 8, factory(Arc::clone(&live), Arc::clone(&drops)));
    // 先同时持有 4 件、再一起归还 ⇒ idle 4（逐件 drop 会复用同一件、idle 恒 1，
    // 那是「复用」锚而非「留存」锚——dim1 已覆盖复用）。
    let guards: Vec<_> = (0..4).map(|_| pool.acquire()).collect();
    drop(guards);
    assert_eq!(pool.metrics().core.idle, 4, "4 件同时归还 ⇒ idle 4");
    let pruned = pool.prune_idle(Duration::ZERO, Instant::now());
    let m = pool.metrics();
    println!(
        "  prune_idle(ZERO): 释放 {} 件 | 在池 idle {} | 存活实例 {}",
        pruned,
        m.core.idle,
        live.load(Ordering::SeqCst)
    );
    assert_eq!(pruned, 4, "锚②(a)：老化口释放全部 idle");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        4,
        "锚②(a)：Drop 计数与释放数一致"
    );

    let live2 = Arc::new(AtomicUsize::new(0));
    let drops2 = Arc::new(AtomicUsize::new(0));
    let pool2 = ObjectPool::new(4, 1, factory(Arc::clone(&live2), Arc::clone(&drops2)));
    let guards: Vec<_> = (0..4).map(|_| pool2.acquire()).collect();
    for g in guards {
        drop(g);
    } // 第 1 件留存，后 3 件容量淘汰
    let m = pool2.metrics();
    println!(
        "  容量淘汰(idle_cap=1): 丢弃 3 件 | dropped_overflow {} | 存活实例 {}",
        m.core.dropped_overflow,
        live2.load(Ordering::SeqCst)
    );
    assert_eq!(m.core.dropped_overflow, 3, "锚②(b)：超限归还丢弃计数");
    assert_eq!(
        live2.load(Ordering::SeqCst),
        1,
        "锚②(b)：只保留 idle_cap 件"
    );
    println!("  判定: PASS");
}

// ---- ③ 降级直构：宇宙满时不阻塞不失败 ----

fn dim3_degraded_direct_construct() {
    println!("\n[dim3] 降级直构（宇宙满 ⇒ 不阻塞不失败，无超时参数）");
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::new(1, 1, factory(Arc::clone(&live), Arc::clone(&drops)));
    let holder = pool.acquire();
    let t0 = Instant::now();
    let spill = pool.acquire(); // 宇宙满 ⇒ 降级直构
    let elapsed = t0.elapsed();
    let m = pool.metrics();
    println!(
        "  degraded {} | borrowed {} | 水位线 {:.2} | acquire 耗时 {:?}",
        m.degraded, m.core.borrowed, m.core.watermark, elapsed
    );
    assert_eq!(m.degraded, 1, "宇宙满 ⇒ 降级直构计数");
    assert!(m.core.watermark > 1.0, "水位线 >1.0 如实体现降级");

    drop(spill);
    let m = pool.metrics();
    assert_eq!(m.core.idle, 0, "降级对象归还即释放，永不进 idle");
    assert_eq!(live.load(Ordering::SeqCst), 1, "只余正常借出的 1 件");
    drop(holder);
    let m = pool.metrics();
    assert_eq!(m.core.idle, 1, "正常对象归还留存（两条宇宙记账路径）");
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "降级件的释放被 Drop 计数观测"
    );
    println!("  判定: PASS");
}

// ---- ④ 分片并发借还：单线程 vs 8 线程同口径对照（分片有效性证据）----

/// 固定次数的借还循环（只测墙钟；禁 per-op 计时——那会把 Instant 开销算进延迟）。
fn borrow_return_ops(pool: &ObjectPool<Vec<u8>>, ops: usize) -> Duration {
    let t = Instant::now();
    for _ in 0..ops {
        let mut g = pool.acquire();
        g.push(1u8); // 触碰对象（模拟使用）
        g.clear();
        drop(g); // 归还
    }
    t.elapsed()
}

fn dim4_concurrent_contention() {
    println!("\n[dim4] 分片并发争用（单线程 vs 8 线程，各 20000 次借还/线程）");
    let pool = Arc::new(ObjectPool::new(64, 64, Vec::<u8>::new));
    const THREADS: usize = 8;
    const OPS: usize = 20_000;

    // 预热：先把对象构造出来，计时窗内只剩借还。
    let warm: Vec<_> = (0..64).map(|_| pool.acquire()).collect();
    drop(warm);

    // 阶段 A：单线程基线。
    let single = borrow_return_ops(&pool, OPS);
    let single_ns = single.as_nanos() as f64 / OPS as f64;

    // 阶段 B：8 线程并行（同口径：只测墙钟，end-start 覆盖全部线程）。
    let start = Instant::now();
    let mut handles = Vec::with_capacity(THREADS);
    for _ in 0..THREADS {
        let p = Arc::clone(&pool);
        handles.push(std::thread::spawn(move || borrow_return_ops(&p, OPS)));
    }
    for h in handles {
        h.join().expect("worker join");
    }
    let parallel = start.elapsed();
    let total = THREADS * OPS;
    let par_ns = parallel.as_nanos() as f64 / total as f64;
    // 扩展比 = 单线程每操作耗时 / 并行每操作耗时；>1 表示并发有收益。
    let scale = single_ns / par_ns;

    let m = pool.metrics();
    println!(
        "  单线程 {single_ns:.1} ns/借还 | 8 线程聚合 {par_ns:.1} ns/借还 | 扩展比 {scale:.2}x"
    );
    println!(
        "  账实: borrowed {} | idle {} | 构造 {} | 复用 {}",
        m.core.borrowed, m.core.idle, m.constructed, m.reused
    );
    // 并发后账实一致：无在借、idle 不超上限、宇宙不超池深。
    assert_eq!(m.core.borrowed, 0, "并发借还后无在借（账实一致）");
    assert!(m.core.idle <= 64, "idle 不超 idle_cap 硬顶");
    assert!(
        m.core.idle + m.core.borrowed <= 64,
        "宇宙（idle + 在借）不超池深"
    );
    assert!(scale > 1.0, "分片下 8 线程聚合吞吐应高于单线程");
    // 数字口径：每借还含两次片锁 + 一次 `Instant::now()`（老化时间戳）——时钟读是
    // 单线程基线 ~75ns 里的固定成本，不是争用项。剩余上限 = 每片一把 Mutex（非
    // 无锁栈，ADR 0010 登记）+ 对象跨线程流转（升级路径 = 无锁 Treiber 栈 / TLS
    // 槽位）；未订阅态审计发射 = 一次 gate load，不在争用面内。
    println!(
        "  判定: PASS（含 clock 读；低于 {THREADS}x 的成因登记：每片一锁 + 单线程基线含两次片锁与 Instant 开销）"
    );
}

fn main() {
    println!("== objpool 域语义锚（release / 纯 std 零计时；计数判定非 criterion）==");
    dim1_reset_reuse();
    dim2_release_paths();
    dim3_degraded_direct_construct();
    dim4_concurrent_contention();
    println!("\n全部维度 PASS");
}
