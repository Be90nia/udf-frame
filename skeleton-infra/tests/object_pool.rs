#![cfg(feature = "pool")]
//! ObjectPool 语义契约端到端：借出四路径（复用/扫片/新建/降级）、reset 两相位
//! 钩子纪律（含 panic 兜底与 unwind 感知归还）、detach 所有权移交、prune_idle
//! 老化、resize 三律、审计流、并发账实一致。
//!
//! 分工：PoolBase 公共逻辑（校验/水位线/审计环/溢出计数）已在 `tests/pool_base.rs`
//! 逐件覆盖、审计环抽取件 `PoolAuditSink` 行为随其回归；`MemoryPool` 借还机器
//! 已在 `tests/memory_pool.rs` 覆盖——本文件只测 `ObjectPool` **特化面**，与基座
//! 交汇处按端到端断言不复测基座内部。

use std::{
    borrow::Borrow,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Barrier, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};

use skeleton::clock::SystemClock;
use skeleton::pool::{PoolAuditEvent, PoolAuditKind, PoolManaged, PoolMetrics, PoolResizeError};
use skeleton_infra::objpool::{ObjectPool, ObjectPoolTuning, Pooled, ResetPhase};

// ── 固定装置：析构可观测对象（hot = reset 保留字段，payload = reset 清除字段）──

#[derive(Debug)]
struct Tracked {
    hot: u64,
    payload: Option<&'static str>,
    live: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}

impl Tracked {
    fn factory(
        live: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    ) -> impl Fn() -> Tracked + Send + Sync + 'static {
        move || {
            live.fetch_add(1, Ordering::SeqCst);
            Tracked {
                hot: 0,
                payload: None,
                live: Arc::clone(&live),
                drops: Arc::clone(&drops),
            }
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::SeqCst);
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

/// 无钩子池装配（每测独立固定装置）。
fn tracked_pool(
    max: usize,
    idle_cap: usize,
) -> (ObjectPool<Tracked>, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::new(
        max,
        idle_cap,
        Tracked::factory(Arc::clone(&live), Arc::clone(&drops)),
    );
    (pool, live, drops)
}

/// 带钩子池装配（reset 计数器 + live/drops 一并交还，保证断言读的是同一计数源）。
fn resettable_pool(
    reset: impl Fn(&mut Tracked) + Send + Sync + 'static,
    phase: ResetPhase,
) -> (
    ObjectPool<Tracked>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let resets = Arc::new(AtomicUsize::new(0));
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = ObjectPool::with_reset(
        2,
        2,
        Tracked::factory(Arc::clone(&live), Arc::clone(&drops)),
        {
            let resets = Arc::clone(&resets);
            move |t: &mut Tracked| {
                resets.fetch_add(1, Ordering::SeqCst);
                reset(t);
            }
        },
        phase,
    );
    (pool, resets, live, drops)
}

// ── reset 钩子两相位：保留字段 / 重置字段区分 + 新对象跳过 reset ──────────

#[test]
fn reset_on_return_restores_fields_keeps_hot_state() {
    let (p, resets, _live, _drops) = resettable_pool(
        |t: &mut Tracked| t.payload = None, // 重置字段：脏态清除（hot 不动——保留字段）
        ResetPhase::OnReturn,
    );

    let mut g = p.acquire();
    assert_eq!(
        resets.load(Ordering::SeqCst),
        0,
        "新构造对象不调 reset（OnReturn 相位同跳）"
    );
    g.hot = 42;
    g.payload = Some("dirty");
    drop(g); // 归还 → OnReturn reset

    assert_eq!(resets.load(Ordering::SeqCst), 1, "归还路径 reset 恰一次");
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "reset 后留存入 idle");
    assert_eq!(m.reused, 0);
    assert_eq!(m.constructed, 1, "复用件不重复构造");

    let g2 = p.acquire();
    assert_eq!(g2.payload, None, "重置字段：脏态已清除");
    assert_eq!(g2.hot, 42, "保留字段：热态跨借还保留");
    let m = p.metrics();
    assert_eq!(m.reused, 1, "复用计数 +1");
    drop(g2);
    assert_eq!(resets.load(Ordering::SeqCst), 2, "第二次归还 reset 恰一次");
}

#[test]
fn reset_on_acquire_restores_fields_keeps_hot_state() {
    let (p, resets, _live, _drops) = resettable_pool(
        |t: &mut Tracked| t.payload = None, // 重置字段
        ResetPhase::OnAcquire,
    );

    let mut g = p.acquire();
    assert_eq!(
        resets.load(Ordering::SeqCst),
        0,
        "新构造对象不调 reset（OnAcquire 相位跳过）"
    );
    g.hot = 7;
    g.payload = Some("dirty");
    drop(g);
    assert_eq!(
        resets.load(Ordering::SeqCst),
        0,
        "OnAcquire 相位：归还不调 reset"
    );

    let g2 = p.acquire();
    assert_eq!(
        resets.load(Ordering::SeqCst),
        1,
        "取出时 reset 恰一次（仅复用件）"
    );
    assert_eq!(g2.payload, None, "重置字段：取出时清除");
    assert_eq!(g2.hot, 7, "保留字段：热态跨借还保留");
    drop(g2);
    assert_eq!(resets.load(Ordering::SeqCst), 1, "归还路径不调 reset");
}

#[test]
fn without_reset_state_persists_across_cycles() {
    // 对照组：无 reset 钩子（`new`）⇒ 状态跨借还原样保留
    let (p, _live, _drops) = tracked_pool(2, 2);
    {
        let mut g = p.acquire();
        g.hot = 99;
        g.payload = Some("keep");
    }
    let g = p.acquire();
    assert_eq!(g.hot, 99, "无 reset：hot 保留");
    assert_eq!(g.payload, Some("keep"), "无 reset：payload 保留");
    let m = p.metrics();
    assert_eq!(m.reused, 1);
    assert_eq!(m.constructed, 1);
    drop(g);
}

// ── 降级直构：宇宙满 ⇒ 不入池宇宙、归还即释放（两独立计数口径）────────────

#[test]
fn degraded_when_universe_full_released_on_return() {
    let (p, _live, _drops) = tracked_pool(1, 1);
    let g1 = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 1);
    assert_eq!(m.degraded, 0);

    let g2 = p.acquire(); // 宇宙 1 == max 1 ⇒ 降级直构
    let m = p.metrics();
    assert_eq!(m.degraded, 1, "宇宙已满 ⇒ 降级直构计数");
    assert_eq!(
        m.constructed, 2,
        "降级直构同时是一次工厂调用（两独立计数各自留痕）"
    );
    assert_eq!(m.core.borrowed, 2, "borrowed = 正常 + 降级都在内");
    assert!(
        (m.core.watermark - 2.0).abs() < 1e-9,
        "水位线 = borrowed/max = 2/1 > 1.0（降级如实体现）"
    );

    drop(g2); // 降级对象归还 ⇒ 直接释放
    let m = p.metrics();
    assert_eq!(m.core.idle, 0, "降级对象永不进 idle");
    assert_eq!(m.core.borrowed, 1);
    assert_eq!(
        m.core.dropped_overflow, 0,
        "降级释放 ≠ 溢出丢弃（两独立计数不得混账）"
    );

    drop(g1); // 正常对象归还 ⇒ 留存
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "正常对象宇宙记账是另一条路径");
    assert_eq!(m.core.borrowed, 0);
    assert!(m.core.idle <= m.core.max, "池宇宙不超 max");
}

// ── detach：所有权移交调用方，池不钉住（宇宙配额随件回吐）─────────────────

#[test]
fn detach_hands_ownership_out_pool_does_not_retain() {
    let (p, live, drops) = tracked_pool(2, 2);
    let g = p.acquire();
    assert_eq!(p.metrics().constructed, 1);

    let obj = g.detach();
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0, "detach 即借出数回吐");
    assert_eq!(m.core.idle, 0, "池不持有 detach 件");
    drop(obj);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "析构归调用方（最后一个持有者放手即析构）"
    );
    assert_eq!(live.load(Ordering::SeqCst), 0);
    assert_eq!(p.metrics().core.idle, 0, "池不回收已移交对象");

    let g2 = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 2, "宇宙已回吐 ⇒ 新建而非降级");
    assert_eq!(m.degraded, 0);
    drop(g2);
}

#[test]
fn detach_broken_object_does_not_poison_pool() {
    // 坏对象处置 = detach 带走（r2d2 has_broken / Commons Pool2 invalidateObject
    // 的调用方判定权）：池宇宙回吐，后续借还路径不受污染。
    let (p, _live, drops) = tracked_pool(2, 2);
    let mut g = p.acquire();
    g.payload = Some("corrupted"); // 视为坏
    let obj = g.detach();
    drop(obj);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 0, "坏对象未回池");

    let g2 = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 2, "宇宙回吐 ⇒ 新建");
    assert_eq!(m.degraded, 0);
    assert_eq!(m.reused, 0, "reused 计数不受 detach 污染");
    drop(g2);
    assert_eq!(p.metrics().core.idle, 1, "后续正常归还照常留存");
}

// ── 归还丢弃：idle_cap 满 ⇒ 丢弃 + dropped_overflow（Drop 计数观测）────────

#[test]
fn return_drops_when_idle_cap_full() {
    let (p, live, drops) = tracked_pool(2, 1);
    let a = p.acquire();
    let b = p.acquire();
    assert_eq!(live.load(Ordering::SeqCst), 2);

    drop(a); // idle 0 < 1 ⇒ 留存
    let m = p.metrics();
    assert_eq!(m.core.idle, 1);
    assert_eq!(live.load(Ordering::SeqCst), 2);

    drop(b); // idle 已满 ⇒ 丢弃
    let m = p.metrics();
    assert_eq!(live.load(Ordering::SeqCst), 1, "槽满对象被析构");
    assert_eq!(drops.load(Ordering::SeqCst), 1, "Drop 计数观测到释放");
    assert_eq!(m.core.dropped_overflow, 1, "dropped_overflow 计数留痕");
    assert_eq!(m.core.idle, 1);
}

// ── prune_idle：按龄释放 + ZERO 全释放 + pruned 计数 ─────────────────────

#[test]
fn prune_idle_releases_by_age_and_counts() {
    let (p, live, _drops) = tracked_pool(2, 2);
    let mut a = p.acquire();
    a.hot = 1;
    let mut b = p.acquire(); // 与 a 同时在借 ⇒ 两件独立构造（借用期互不复用）
    b.hot = 2;
    drop(a); // idle 1（老）
    thread::sleep(Duration::from_millis(120));
    drop(b); // idle 2（新——last_used 晚于 a 约 120ms）

    let pruned = p.prune_idle_with_clock(Duration::from_millis(50), &SystemClock);
    assert_eq!(pruned, 1, "超龄释放恰一件");
    let m = p.metrics();
    assert_eq!(m.pruned, 1, "pruned 计数留痕");
    assert_eq!(m.core.idle, 1, "未超龄保留");
    assert_eq!(live.load(Ordering::SeqCst), 1);

    let pruned_again = p.prune_idle_with_clock(Duration::from_millis(50), &SystemClock);
    assert_eq!(pruned_again, 0, "未超龄不释放");
    assert_eq!(p.metrics().pruned, 1);
}

#[test]
fn prune_zero_age_releases_all_idle() {
    let (p, live, _drops) = tracked_pool(2, 2);
    {
        let _a = p.acquire();
        let _b = p.acquire();
    }
    assert_eq!(p.metrics().core.idle, 2);

    let pruned = p.prune_idle_with_clock(Duration::ZERO, &SystemClock);
    assert_eq!(pruned, 2, "Duration::ZERO ⇒ 释放全部 idle");
    let m = p.metrics();
    assert_eq!(m.core.idle, 0);
    assert_eq!(m.pruned, 2);
    assert_eq!(live.load(Ordering::SeqCst), 0, "全部析构");
}

// ── resize：事前校验拒绝不改状态 / 缩容裁剪 idle ──────────────────────────

#[test]
fn resize_rejects_illegal_params_without_state_change() {
    let (p, _live, _drops) = tracked_pool(4, 2);
    let before = p.metrics();
    assert_eq!(
        PoolManaged::resize(
            &p,
            ObjectPoolTuning {
                max: 0,
                idle_cap: 1
            }
        ),
        Err(PoolResizeError::ZeroMax),
        "max==0 必须 fail-visible 拒绝"
    );
    assert_eq!(
        PoolManaged::resize(
            &p,
            ObjectPoolTuning {
                max: 4,
                idle_cap: 0
            }
        ),
        Err(PoolResizeError::IdleCapZero),
        "idle_cap==0 必须拒绝"
    );
    assert_eq!(
        PoolManaged::resize(
            &p,
            ObjectPoolTuning {
                max: 2,
                idle_cap: 3
            }
        ),
        Err(PoolResizeError::IdleCapExceedsMax {
            idle_cap: 3,
            max: 2
        }),
        "idle_cap>max 必须拒绝"
    );
    assert_eq!(
        p.metrics(),
        before,
        "拒绝路径不得改任何状态（含 panic_dropped）"
    );
}

#[test]
fn resize_shrink_prunes_excess_idle() {
    let (p, live, _drops) = tracked_pool(4, 2);
    {
        let _a = p.acquire();
        let _b = p.acquire();
    }
    assert_eq!(p.metrics().core.idle, 2);

    PoolManaged::resize(
        &p,
        ObjectPoolTuning {
            max: 4,
            idle_cap: 1,
        },
    )
    .expect("合法缩容");
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "缩容裁剪到新 idle_cap");
    assert_eq!(m.core.dropped_overflow, 1, "缩容丢弃走 dropped_overflow");
    assert_eq!(
        m.pruned, 0,
        "resize 丢弃与 prune_idle 是两个独立计数，不得混账"
    );
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

#[test]
fn resize_shrink_does_not_revoke_borrowed() {
    // 缩容不夺借出（PoolManaged 语义契约 2）：在借句柄不受影响，归还时按新
    // 边界裁决（终态收敛到新 idle_cap）。
    let (p, live, _drops) = tracked_pool(4, 4);
    let a = p.acquire();
    let b = p.acquire();
    PoolManaged::resize(
        &p,
        ObjectPoolTuning {
            max: 4,
            idle_cap: 1,
        },
    )
    .expect("缩容生效");
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 2, "缩容不夺借出：在借句柄不受影响");
    assert_eq!(m.core.idle, 0);
    assert_eq!(live.load(Ordering::SeqCst), 2);

    drop(a); // idle 0→1 留存
    drop(b); // idle 已满 ⇒ 溢出丢弃（归还时按新边界裁决）
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "终态收敛到新 idle_cap");
    assert_eq!(m.core.dropped_overflow, 1);
    assert_eq!(live.load(Ordering::SeqCst), 1);
}

// ── 并发借还：容量硬上界 + 终态账实一致（范式同 tests/memory_pool.rs）──────

#[test]
fn concurrent_acquire_return_keeps_ledger_consistent() {
    let (p, live, _drops) = tracked_pool(8, 4);
    let p = Arc::new(p);
    let mut handles = Vec::new();
    for _ in 0..8 {
        let p = Arc::clone(&p);
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                let g = p.acquire(); // 永不阻塞永不失败——无超时参数
                std::hint::black_box(&*g);
                drop(g);
            }
        }));
    }
    for h in handles {
        h.join().expect("负载线程不得 panic");
    }
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0, "全部归还后借出数归零");
    assert!(m.core.idle <= 4, "idle ≤ idle_cap");
    assert!(m.core.idle <= m.core.max, "终态宇宙（= idle）≤ max");
    assert_eq!(
        live.load(Ordering::SeqCst),
        m.core.idle,
        "账实一致：存活实例数 == idle 数"
    );
    assert_eq!(m.panic_dropped, 0, "正常路径不得有 unwind 丢弃");
    assert_eq!(m.reset_panicked, 0, "无钩子 ⇒ 不得有钩子因丢弃");
    assert_eq!(m.core.watermark, 0.0);
    // 反假绿：若配额逻辑坏到每次 acquire 都降级，`idle`/`borrowed`/`live` 全归零
    // 上面几条照样成立。8 线程各持 1 件（≤ max=8）⇒ 正确实现零降级、工厂至多被
    // 调用 max 次。
    assert_eq!(m.degraded, 0, "8 线程各持 1 件 ≤ max=8 ⇒ 正确实现下零降级");
    assert!(
        m.constructed <= 8,
        "工厂调用次数 ≤ max（无降级直构、无对象泄漏式重建）：{}",
        m.constructed
    );
}

// ── 分片均分余数：无降级 + idle 上界（max/ idle_cap 不整除分片数）──────────

#[test]
fn uneven_share_has_zero_degradation_and_bounded_idle() {
    // max=10 / idle_cap=4 ⇒ 4 片，片配额按 share 分：max 侧 3,3,2,2（10 % 4 = 2），
    // idle_cap 侧 1,1,1,1。宇宙恰好 10 件可同时借出 ⇒ 零降级；10 件全归还后只有
    // 4 个 idle 名额（每片 1）⇒ 恰好 4 留存、6 按容量因丢弃。
    let (p, live, _drops) = tracked_pool(10, 4);
    let p = Arc::new(p);
    let gate = Arc::new(Barrier::new(11));
    let release = Arc::new(Barrier::new(11));
    let mut handles = Vec::new();
    for _ in 0..10 {
        let p = Arc::clone(&p);
        let gate = Arc::clone(&gate);
        let release = Arc::clone(&release);
        handles.push(thread::spawn(move || {
            let g = p.acquire();
            gate.wait(); // 峰值窗口：主线程在此读数
            release.wait();
            std::hint::black_box(&*g);
            drop(g);
        }));
    }
    gate.wait();
    let at_peak = p.metrics();
    assert_eq!(at_peak.core.borrowed, 10, "10 线程各持 1 件");
    assert_eq!(
        at_peak.degraded, 0,
        "宇宙恰好满 ≠ 降级（余数份额也认领得到）"
    );
    assert_eq!(
        at_peak.constructed, 10,
        "工厂调用 == 借出件数（零降级直构）"
    );
    assert_eq!(at_peak.core.idle, 0);
    release.wait();

    for h in handles {
        h.join().expect("负载线程不得 panic");
    }
    let m = p.metrics();
    assert_eq!(m.core.idle, 4, "idle 上界 = idle_cap（每片 1）");
    assert_eq!(m.core.dropped_overflow, 6, "10 还 4 留 ⇒ 6 按容量因丢弃");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.reset_panicked, 0, "容量因不进钩子因计数");
    assert_eq!(live.load(Ordering::SeqCst), 4, "存活实例数 == idle 数");
}

// ── 宇宙恰好达 max：水位线 1.0 且零降级 ─────────────────────────────────

#[test]
fn universe_exactly_at_max_stays_undegraded() {
    let (p, live, _drops) = tracked_pool(8, 8);
    let p = Arc::new(p);
    let gate = Arc::new(Barrier::new(9));
    let release = Arc::new(Barrier::new(9));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let p = Arc::clone(&p);
        let gate = Arc::clone(&gate);
        let release = Arc::clone(&release);
        handles.push(thread::spawn(move || {
            let g = p.acquire();
            gate.wait();
            release.wait();
            std::hint::black_box(&*g);
            drop(g);
        }));
    }
    gate.wait();
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 8, "宇宙满（= max）");
    assert!((m.core.watermark - 1.0).abs() < 1e-9, "水位线 = 1.0");
    assert_eq!(m.degraded, 0, "达 max 但未越界 ⇒ 零降级");
    assert_eq!(m.constructed, 8);
    assert_eq!(live.load(Ordering::SeqCst), 8);
    release.wait();

    for h in handles {
        h.join().expect("负载线程不得 panic");
    }
    let m = p.metrics();
    assert_eq!(m.core.idle, 8, "8 片各 1 留存");
    assert_eq!(m.core.dropped_overflow, 0, "无溢出：每片恰好 1");
    assert_eq!(m.degraded, 0);
}

// ── 跨片复用：异线程借到同片留存件（复用命中、不新建）─────────────────────

#[test]
fn cross_shard_reuse_is_counted_without_new_construction() {
    let (p, _live, _drops) = tracked_pool(4, 4); // 4 片
    let p = Arc::new(p);
    {
        // 首个领号者（本线程）home = 0 号片：借出即归还 ⇒ 0 号片留 1 件。
        let g = p.acquire();
        std::hint::black_box(&*g);
    }
    let m = p.metrics();
    assert_eq!((m.constructed, m.reused), (1, 0), "首次借出必新建");

    let p2 = Arc::clone(&p);
    let m = thread::spawn(move || {
        // 新线程 home = 1 号片：本片空 ⇒ 轮转扫到 0 号片的留存件。
        let g = p2.acquire();
        std::hint::black_box(&*g);
        drop(g);
        p2.metrics()
    })
    .join()
    .expect("异线程借还不得 panic");

    assert_eq!(m.reused, 1, "跨片复用命中（非本片空 ⇒ 轮转扫到持有片）");
    assert_eq!(m.constructed, 1, "跨片复用不触发工厂调用");
    assert_eq!(m.degraded, 0);
    assert_eq!(m.core.borrowed, 0);
}

// ── 同线程多池亲和：A/B 交替各 200 轮，两池各自稳定复用 ──────────────────

#[test]
fn multi_pool_affinity_is_stable_in_same_thread() {
    // 宿主真实形态：同一线程同时用加解密 ctx 池与连接池。亲和表按池地址分槽 ⇒
    // 两池互不覆盖（单槽实现下每切换一次池都要重领号，亲和失效）。
    let (a, a_live, _a_drops) = tracked_pool(4, 4);
    let (b, b_live, _b_drops) = tracked_pool(4, 4);
    for _ in 0..200 {
        let mut ga = a.acquire();
        ga.hot += 1; // 复用同一件：状态跨借还累加（证明是同一件而非重建）
        std::hint::black_box(&*ga);
        drop(ga);
        let gb = b.acquire();
        std::hint::black_box(&*gb);
        drop(gb);
    }
    let ma = a.metrics();
    let mb = b.metrics();
    assert_eq!(ma.constructed, 1, "A 池 200 轮只造 1 件（亲和不漂移）");
    assert_eq!(ma.reused, 199, "A 池 199 次命中复用");
    assert_eq!(ma.core.idle, 1);
    assert_eq!(ma.degraded, 0);
    assert_eq!(a_live.load(Ordering::SeqCst), 1, "A 池账实一致");
    assert_eq!(mb.constructed, 1, "B 池同款：不被 A 挤掉亲和");
    assert_eq!(mb.reused, 199, "B 池 199 次命中复用");
    assert_eq!(b_live.load(Ordering::SeqCst), 1, "B 池账实一致");
    // 鉴别力锚（审计 F4-复核）：借还/构造计数在「单槽亲和」回退实现下同样正确
    // （`pop_idle` 会轮转把唯一那件找回来）——唯「每 (线程,池) 只领一次号」这条
    // 能区分。单槽回退时该值会涨到 ≈ 循环次数（200）。
    assert_eq!(
        a.affinity_issuances(),
        1,
        "A 池每线程只领一次号（单槽回退会涨）"
    );
    assert_eq!(
        b.affinity_issuances(),
        1,
        "B 池每线程只领一次号（单槽回退会涨）"
    );
}

// ── F1 回归：resize 缩 cap × 多线程归还交错，终态 idle ≤ 新 idle_cap ───────

#[test]
fn resize_shrink_racing_returns_never_breaches_new_idle_cap() {
    // 配额若读在片锁外：归还读到旧 cap（宽）→ resize 已 store 新 cap 并裁过该片
    // → 归还的 push 落在新上界之外，且**无任何再裁剪路径**（永久留存）。本测试
    // 反复「放开 → 缩到 1」让窗口高频出现，终态断言硬上界。
    //
    // 口径：该窗口是 ns 级（须「load 配额 → 取片锁」之间恰好被 resize 整段插队），
    // 故本条是**不变式守卫**而非确定性复现——击穿一旦发生即永久留存，join 后断言
    // 必中的；确定性收敛路径的对照见 `resize_shrink_does_not_revoke_borrowed`。
    const ROUNDS: usize = 3;
    for round in 1..=ROUNDS {
        let (p, live, _drops) = tracked_pool(32, 8);
        let p = Arc::new(p);
        let gate = Arc::new(Barrier::new(9));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let p = Arc::clone(&p);
            let gate = Arc::clone(&gate);
            handles.push(thread::spawn(move || {
                gate.wait();
                for _ in 0..400 {
                    let g = p.acquire();
                    std::hint::black_box(&*g);
                    drop(g); // 归还与 resize 交错
                }
            }));
        }
        gate.wait();
        for _ in 0..128 {
            p.resize(ObjectPoolTuning {
                max: 32,
                idle_cap: 8,
            })
            .expect("放开 cap 合法");
            p.resize(ObjectPoolTuning {
                max: 32,
                idle_cap: 1,
            })
            .expect("缩 cap 合法");
        }
        for h in handles {
            h.join().expect("负载线程不得 panic");
        }
        // 最后一次 resize 是缩小 ⇒ 无需再收敛动作，终态必须已在界内。
        let m = p.metrics();
        assert!(
            m.core.idle <= 1,
            "round {round}: 终态 idle {} > 新 idle_cap 1（配额读在片锁外 ⇒ 永久击穿）",
            m.core.idle
        );
        assert_eq!(m.core.borrowed, 0, "round {round}: 全部归还");
        assert_eq!(m.reset_panicked, 0);
        assert_eq!(
            live.load(Ordering::SeqCst),
            m.core.idle,
            "round {round}: 账实一致（未留孤儿实例）"
        );
    }
}

// ── metrics：共享核心形经 Borrow 可达 + 特有读数 ──────────────────────────

#[test]
fn metrics_expose_core_via_borrow() {
    let (p, _live, _drops) = tracked_pool(4, 2);
    let m = p.metrics();
    let core: &PoolMetrics = m.borrow();
    assert_eq!(core.max, 4);
    assert_eq!(core.idle, 0);
    assert_eq!(core.borrowed, 0);
    assert_eq!(core.dropped_overflow, 0);
    assert_eq!(core.watermark, 0.0);
    assert_eq!(m.degraded, 0);
    assert_eq!(m.reused, 0);
    assert_eq!(m.constructed, 0);
    assert_eq!(m.pruned, 0);
    assert_eq!(m.panic_dropped, 0, "panic_dropped 纳入 metrics 一致性");
    assert_eq!(m.reset_panicked, 0, "reset_panicked 纳入 metrics 一致性");

    let g = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 1);
    assert!((m.core.watermark - 0.25).abs() < 1e-9, "水位线 = 1/4");
    drop(g);
    assert_eq!(p.metrics().core.idle, 1);
}

// ── 审计流：seq 单调 + 种类齐全（借出/归还/调参/拒绝/溢出；实时回调收集面）──

#[test]
fn audit_events_cover_lifecycle_with_monotonic_seq() {
    let (p, _live, _drops) = tracked_pool(2, 1);
    let got: Arc<Mutex<Vec<PoolAuditEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&got);
    let _ticket = PoolManaged::subscribe_audit(&p, move |e| {
        sink.lock().expect("审计槽未毒化").push(*e);
    });

    let a = p.acquire(); // Borrowed（新建）
    drop(a); // Returned
    let b = p.acquire(); // Borrowed（复用）
    let c = p.acquire(); // Borrowed（新建，宇宙 2/2）
    PoolManaged::resize(
        &p,
        ObjectPoolTuning {
            max: 1,
            idle_cap: 1,
        },
    )
    .expect("缩容生效"); // Resized
    drop(b); // Returned（idle 0→1）
    drop(c); // OverflowDropped（idle 已满）
    assert!(
        PoolManaged::resize(
            &p,
            ObjectPoolTuning {
                max: 0,
                idle_cap: 1
            }
        )
        .is_err(),
        "拒绝也进审计（事前校验留痕）"
    ); // ResizeRejected

    let events = got.lock().expect("审计槽未毒化").clone();
    assert!(events.len() >= 2, "生命周期必须产生事件流");
    for w in events.windows(2) {
        assert!(w[0].seq < w[1].seq, "seq 必须单调：{events:?}");
    }
    let kinds: Vec<PoolAuditKind> = events.iter().map(|e| e.kind).collect();
    for expected in [
        PoolAuditKind::Borrowed,
        PoolAuditKind::Returned,
        PoolAuditKind::Resized,
        PoolAuditKind::ResizeRejected,
        PoolAuditKind::OverflowDropped,
    ] {
        assert!(
            kinds.contains(&expected),
            "生命周期事件缺 {expected:?}：实际 {kinds:?}"
        );
    }
    drop(_ticket);
}

// ── 审计回调 panic：摘除留痕（audit_callback_panics）+ 环照录 + 池行为不变 ──

#[test]
fn audit_callback_panic_is_counted_and_pool_unaffected() {
    let (p, _live, _drops) = tracked_pool(2, 2);
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let _ticket = PoolManaged::subscribe_audit(&p, move |_e| {
        seen.fetch_add(1, Ordering::SeqCst);
        panic!("audit boom");
    });

    let g = p.acquire(); // Borrowed ⇒ 回调 panic ⇒ fail-closed 摘除
    drop(g); // Returned ⇒ 已摘除，不再进回调
    assert_eq!(p.audit_callback_panics(), 1, "回调 panic 留痕且只摘一次");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "摘除后不再回调");

    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0, "观测面 panic 不得改变池行为");
    assert_eq!(m.core.idle, 1, "归还照常留存");
    assert_eq!(m.reset_panicked, 0, "回调 panic 不是钩子因");
    // 环照录：观测面摘除不影响事后取证面（读侧同 `PoolBase` 口径）。
    let events = p.take_audit_events();
    assert_eq!(events.len(), 2, "Borrowed + Returned 均入环：{events:?}");
    assert!(p.take_audit_events().is_empty(), "随取随清");
}

// ── 工厂 panic：宇宙配额回滚，池继续可用 ──────────────────────────────────

#[test]
fn factory_panic_rolls_back_universe_and_pool_stays_usable() {
    let boom = Arc::new(AtomicBool::new(false));
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let make = {
        let boom = Arc::clone(&boom);
        let live = Arc::clone(&live);
        let drops = Arc::clone(&drops);
        move || {
            if boom.swap(false, Ordering::SeqCst) {
                panic!("factory boom");
            }
            live.fetch_add(1, Ordering::SeqCst);
            Tracked {
                hot: 0,
                payload: None,
                live: Arc::clone(&live),
                drops: Arc::clone(&drops),
            }
        }
    };
    let p = ObjectPool::new(1, 1, make);

    boom.store(true, Ordering::SeqCst);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = p.acquire();
    }));
    assert!(result.is_err(), "工厂 panic 必须如实传播");
    let m = p.metrics();
    assert_eq!(m.constructed, 1, "工厂调用即留痕（panic 时已计数）");
    assert_eq!(m.core.borrowed, 0, "配额回滚：借出数未虚增");
    assert_eq!(live.load(Ordering::SeqCst), 0);

    boom.store(false, Ordering::SeqCst);
    let g = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 2);
    assert_eq!(m.degraded, 0, "宇宙配额已回滚 ⇒ 新建而非降级");
    drop(g);
    assert_eq!(p.metrics().core.idle, 1, "panic 回滚后池必须可用");
}

// ── reset panic 兜底（非 unwind 路径）：丢弃对象，进程不 abort，池仍可用 ───

#[test]
fn reset_panic_on_return_discards_object_pool_survives() {
    const MARK: u64 = u64::MAX;
    let (p, resets, live, _drops) = resettable_pool(
        |t: &mut Tracked| {
            if t.hot == MARK {
                panic!("reset boom");
            }
            t.payload = None;
        },
        ResetPhase::OnReturn,
    );

    let mut g = p.acquire();
    g.hot = MARK; // 标记为 reset 时爆炸
    drop(g); // reset panic ⇒ catch_unwind 兜底：丢弃不入 idle，进程不 abort

    let m = p.metrics();
    assert_eq!(resets.load(Ordering::SeqCst), 1, "钩子确实运行过");
    assert_eq!(
        m.reset_panicked, 1,
        "reset panic ⇒ 丢弃 + 钩子因计数 reset_panicked"
    );
    assert_eq!(
        m.core.dropped_overflow, 0,
        "钩子因不得混进容量口径（dropped_overflow）"
    );
    assert_eq!(
        m.panic_dropped, 0,
        "catch_unwind 兜底路径 ≠ unwind 路径（两独立计数）"
    );
    assert_eq!(m.core.idle, 0, "状态不可信对象不入 idle");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(live.load(Ordering::SeqCst), 0, "对象已销毁");

    let g2 = p.acquire(); // 池仍可用
    let m = p.metrics();
    assert_eq!(m.constructed, 2, "宇宙配额已回吐 ⇒ 新建");
    drop(g2); // reset 正常路径（hot=0 ≠ MARK）
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "reset 后正常留存");
    assert_eq!(resets.load(Ordering::SeqCst), 2);
}

#[test]
fn reset_on_acquire_panic_propagates_and_pool_stays_consistent() {
    const MARK: u64 = u64::MAX;
    let (p, resets, live, _drops) = resettable_pool(
        |t: &mut Tracked| {
            if t.hot == MARK {
                panic!("acquire reset boom");
            }
            t.payload = None;
        },
        ResetPhase::OnAcquire,
    );

    let mut g1 = p.acquire(); // 新对象：reset 跳过
    g1.hot = MARK;
    drop(g1); // OnAcquire：归还不调 reset ⇒ 留存 idle
    assert_eq!(p.metrics().core.idle, 1);

    let result = catch_unwind(AssertUnwindSafe(|| {
        let _ = p.acquire(); // 取出复用件 → reset panic ⇒ 原样传播（不吞）
    }));
    assert!(result.is_err(), "OnAcquire reset panic 必须原样传播");

    let m = p.metrics();
    assert_eq!(resets.load(Ordering::SeqCst), 1);
    assert_eq!(m.core.idle, 0, "复用件已消费销毁");
    assert_eq!(m.core.borrowed, 0, "借出数未虚增");
    assert_eq!(live.load(Ordering::SeqCst), 0, "宇宙配额已回吐");

    let g3 = p.acquire(); // 新建对象：reset 跳过 ⇒ 无 panic
    let m = p.metrics();
    assert_eq!(m.constructed, 2);
    assert_eq!(
        resets.load(Ordering::SeqCst),
        1,
        "新对象跳过 reset（复证跳过语义）"
    );
    drop(g3);
    assert_eq!(p.metrics().core.idle, 1);
}

// ── 语义 11：unwind 感知归还（panic 中析构 = 销毁不回池，r2d2#31 防御）────

#[test]
fn unwind_return_destroys_object_and_does_not_repool() {
    let (p, live, drops) = tracked_pool(2, 2);
    let result = catch_unwind(AssertUnwindSafe(|| {
        let mut g = p.acquire();
        g.hot = 5;
        g.payload = Some("dirty");
        panic!("boom inside unwind window");
    }));
    assert!(result.is_err(), "panic 必须如实传播");

    let m = p.metrics();
    assert_eq!(m.panic_dropped, 1, "unwind 归还计入 panic_dropped");
    assert_eq!(
        m.core.dropped_overflow, 0,
        "panic_dropped 与 dropped_overflow 两独立计数不得混账"
    );
    assert_eq!(m.core.idle, 0, "脏对象禁回池");
    assert_eq!(m.core.borrowed, 0, "借出数已回吐");
    assert_eq!(live.load(Ordering::SeqCst), 0, "对象已销毁");
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let g = p.acquire();
    let m = p.metrics();
    assert_eq!(m.constructed, 2, "对象未回池 ⇒ 下一个借出是新建");
    assert_eq!(m.reused, 0, "复用计数不得被 unwind 归还污染");
    drop(g);
    assert_eq!(p.metrics().core.idle, 1, "池后续可用（正常归还回池）");
}

#[test]
fn unwind_return_control_group_normal_return_repools() {
    // 对照组：非 unwind 正常归还回池（复用计数 +1）——与 unwind 路径对照显形
    let (p, _live, _drops) = tracked_pool(2, 2);
    {
        let mut g = p.acquire();
        g.hot = 1;
        g.payload = Some("dirty");
    } // 正常 Drop ⇒ 回池
    let m = p.metrics();
    assert_eq!(m.panic_dropped, 0);
    assert_eq!(m.core.idle, 1, "正常归还回池");

    let g = p.acquire();
    let m = p.metrics();
    assert_eq!(m.reused, 1, "复用计数 +1（对照组）");
    assert_eq!(m.constructed, 1);
    drop(g);
}

// ── reset 重入检出：可观察失败（丢弃留痕），非静默跳过、非死锁、非 abort ──

#[test]
fn reset_reentrant_acquire_is_detected_and_discarded() {
    // 钩子体内重入**同一池** acquire + 归还 = 契约违规（无限递归面）——
    // thread_local 在途标记检出 ⇒ panic ⇒ 归还路径 catch_unwind 兜底：重入件
    // 丢弃 + `reset_panicked` 留痕（钩子因，**不进容量口径** `dropped_overflow`），
    // 外层对象照常留存（池不死锁不 abort）。
    // 池句柄：钩子闭包读它拿池；收尾置 None 摘除强引用（断环）。
    type PoolHandle = Arc<Mutex<Option<Arc<ObjectPool<Vec<u8>>>>>>;
    let holder: PoolHandle = Arc::new(Mutex::new(None));
    let hook_pool = Arc::clone(&holder);
    let pool = Arc::new(ObjectPool::with_reset(
        4,
        4,
        Vec::<u8>::new,
        move |v: &mut Vec<u8>| {
            if let Some(p) = hook_pool.lock().expect("holder 锁未毒化").as_ref() {
                let inner = p.acquire(); // 重入同一池（检出点在其归还路径）
                drop(inner); // 归还 → 重置重入检出 ⇒ panic ⇒ 兜底丢弃重入件
            }
            v.clear();
        },
        ResetPhase::OnReturn,
    ));
    *holder.lock().expect("holder 锁未毒化") = Some(Arc::clone(&pool));

    let mut g = pool.acquire(); // 新对象：reset 跳过（无重入）
    g.push(1);
    drop(g); // 归还 → 钩子 → 重入 acquire → 重入件归还 → 检出 ⇒ 丢弃

    let m = pool.metrics();
    assert_eq!(
        m.reset_panicked, 1,
        "归还路径重入 ⇒ 兜底丢弃重入件（fail-visible，非静默跳过）"
    );
    assert_eq!(m.core.dropped_overflow, 0, "重入检出是钩子因，不进容量口径");
    assert_eq!(
        m.panic_dropped, 0,
        "重入检出走 catch_unwind 兜底 ≠ unwind 路径（两独立计数）"
    );
    assert_eq!(m.core.idle, 1, "外层对象照常留存（兜底只丢弃重入件）");
    assert_eq!(m.constructed, 2, "重入件被实际构造（调用留痕口径）");
    assert_eq!(m.core.borrowed, 0);

    // 池仍可用（复用外层件）；本测试二段归还再检出一次——每归还周期至多
    // 检出一层（有界，非无限递归）。
    let g2 = pool.acquire();
    assert_eq!(pool.metrics().reused, 1, "池仍可用");
    drop(g2);
    let m = pool.metrics();
    assert_eq!(m.reset_panicked, 2, "每归还周期检出恰一次（有界）");
    assert_eq!(m.core.dropped_overflow, 0, "钩子因始终不进容量口径");
    assert_eq!(m.core.idle, 1);

    // 断环收尾：清空 holder 摘除闭包侧的池强引用，整链可析构。
    *holder.lock().expect("holder 锁未毒化") = None;
    drop(pool);
}

// ── reset 重入的跨池面：单标志保守语义（跨池嵌套同样被拦）───────────────

#[test]
fn reset_hook_cross_pool_nesting_is_also_detected() {
    // 实现为进程内单标志（无法区分池身份）：A 池钩子体内触发 B 池的 reset
    // 钩子同样被判重入 ⇒ B 的对象被丢弃（fail-visible）。红线见模块文档
    // 「重入」节：钩子体内不得触发**任何**池的 reset 钩子。
    type PoolHandle = Arc<Mutex<Option<Arc<ObjectPool<Tracked>>>>>;
    let b_holder: PoolHandle = Arc::new(Mutex::new(None));
    let b_for_a = Arc::clone(&b_holder);

    // A 池：钩子体内借一件 B 的对象并立即归还（触发 B 的 reset）。
    let (a_pool, _a_resets, _a_live, _a_drops) = resettable_pool(
        move |t: &mut Tracked| {
            if let Some(b) = b_for_a.lock().expect("holder 锁未毒化").as_ref() {
                drop(b.acquire()); // 归还 B ⇒ B 的钩子被单标志拦
            }
            t.hot = 0;
        },
        ResetPhase::OnReturn,
    );

    // B 池：独立计数，便于断言其对象被丢弃。
    let (b_pool, _b_resets, b_live, _b_drops) =
        resettable_pool(|t: &mut Tracked| t.hot = 0, ResetPhase::OnReturn);
    let b_pool = Arc::new(b_pool);
    *b_holder.lock().expect("holder 锁未毒化") = Some(Arc::clone(&b_pool));

    {
        let mut g = a_pool.acquire();
        g.hot = 7;
    } // A 归还 ⇒ A 钩子 ⇒ 借 B ⇒ 归还 B ⇒ B 钩子被拦 ⇒ B 对象丢弃

    assert_eq!(
        b_pool.metrics().reset_panicked,
        1,
        "跨池嵌套同样被检出：B 的对象丢弃留痕（非静默回池）"
    );
    assert_eq!(
        b_pool.metrics().core.dropped_overflow,
        0,
        "钩子因不进容量口径"
    );
    assert_eq!(b_live.load(Ordering::SeqCst), 0, "B 的对象被销毁");
    assert_eq!(
        a_pool.metrics().core.idle,
        1,
        "A 池不受影响：外层对象照常留存"
    );

    // 断环收尾：摘除 holder 侧强引用，整链可析构。
    *b_holder.lock().expect("holder 锁未毒化") = None;
    drop(b_pool);
}

// ── 构造期非法配置 fail-fast（同 PoolBase::new 纪律）─────────────────────
#[test]
#[should_panic(expected = "ObjectPool")]
fn new_rejects_illegal_config_fail_fast() {
    let _ = ObjectPool::<Vec<u8>>::new(0, 1, Vec::new);
}

#[test]
#[should_panic(expected = "ObjectPool")]
fn with_reset_rejects_illegal_config_fail_fast() {
    let _ = ObjectPool::with_reset(4, 0, Vec::<u8>::new, |_| {}, ResetPhase::OnAcquire);
}

// ── auto traits：ObjectPool Send+Sync（T: Send 即可），Pooled T: Send 时 Send ─

#[test]
fn pool_is_send_sync_and_handle_is_send() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<ObjectPool<Vec<u8>>>();
    assert_send_sync::<ObjectPool<Tracked>>();
    assert_send::<Pooled<'static, Vec<u8>>>();
}

// ── 按值插入（insert）：外部对象入 idle / 双配额拒收 / 记账 / reset 交互 ────

/// 池外构造的对象（不经池工厂——`insert` 的前提就是「对象不是本池造的」）；
/// 自带 live/drops 观测弧，与池工厂的计数不混账。
fn external(
    hot: u64,
    payload: Option<&'static str>,
) -> (Tracked, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    live.fetch_add(1, Ordering::SeqCst);
    (
        Tracked {
            hot,
            payload,
            live: Arc::clone(&live),
            drops: Arc::clone(&drops),
        },
        live,
        drops,
    )
}

#[test]
fn insert_puts_external_object_into_idle_and_acquire_reuses_it() {
    let (p, _live, _drops) = tracked_pool(2, 2);
    let (ext, ext_live, _ext_drops) = external(7, Some("ext"));
    assert!(p.insert(ext).is_none(), "None = 池已接管（入 idle）");
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "插入件进 idle");
    assert_eq!(m.constructed, 0, "插入不走工厂");
    assert_eq!(m.core.borrowed, 0, "插入不动 borrowed（从未借出）");

    let mut g = p.acquire();
    assert_eq!(g.hot, 7, "下一次 acquire 拿到同一件");
    assert_eq!(g.payload, Some("ext"), "无钩子池：状态原样");
    let m = p.metrics();
    assert_eq!(m.reused, 1, "插入件走复用路径借出");
    assert_eq!(m.constructed, 0, "全程零构造");
    assert_eq!(ext_live.load(Ordering::SeqCst), 1, "池持有的是插入的那件");

    g.hot = 9;
    drop(g); // 归还
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "归还回 idle");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.constructed, 0);
}

#[test]
fn insert_rejects_when_idle_cap_full_and_counts_capacity_cause() {
    let (p, _live, _drops) = tracked_pool(4, 1); // 1 片、idle_cap=1
    let (a, _a_live, _a_drops) = external(1, None);
    assert!(p.insert(a).is_none());

    let (b, _b_live, b_drops) = external(2, None);
    let rejected = p.insert(b);
    assert!(
        rejected.is_some(),
        "idle 满 ⇒ 对象交回调用方（fail-closed）"
    );
    assert_eq!(
        rejected.as_ref().expect("刚断言 is_some").hot,
        2,
        "交回的是原对象（内容未变）"
    );
    drop(rejected);
    assert_eq!(b_drops.load(Ordering::SeqCst), 1, "调用方放手才析构");

    let m = p.metrics();
    assert_eq!(m.core.dropped_overflow, 1, "容量因留痕");
    assert_eq!(m.reset_panicked, 0, "容量因不进钩子账");
    assert_eq!(m.core.idle, 1, "拒收不占槽");
    assert_eq!(m.core.borrowed, 0);
}

#[test]
fn insert_rejects_when_universe_full_despite_idle_room() {
    // max=2 / idle_cap=1（1 片）：借出两件把宇宙占满，idle 仍有空位 ⇒ 插入必须
    // 拒收——宇宙（idle + 正常借出）硬上界 max 不得被插入击穿。
    let (p, _live, _drops) = tracked_pool(2, 1);
    let h1 = p.acquire();
    let h2 = p.acquire();
    assert_eq!(p.metrics().core.idle, 0);

    let (ext, ext_live, _drops) = external(3, None);
    assert!(
        p.insert(ext).is_some(),
        "宇宙满：idle 有空位也拒收（否则 alive 越过 max）"
    );
    assert_eq!(p.metrics().core.dropped_overflow, 1, "拒收记一笔容量因");
    assert_eq!(p.metrics().core.idle, 0);
    assert_eq!(ext_live.load(Ordering::SeqCst), 0, "拒收件由调用方析构");

    drop(h1);
    drop(h2);
    assert_eq!(p.metrics().core.idle, 1, "idle_cap=1 ⇒ 只留一件");
    assert_eq!(
        p.metrics().core.dropped_overflow,
        2,
        "第二笔来自归还路径（单片 idle 满，既有语义，非插入）"
    );
    // 腾出宇宙余量后（prune 释放 idle 且回卷 alive）插入才成功。
    assert_eq!(p.prune_idle_with_clock(Duration::ZERO, &SystemClock), 1);
    let (ext2, _l2, _d2) = external(4, None);
    assert!(p.insert(ext2).is_none(), "宇宙余量恢复后插入成功");
    assert_eq!(p.metrics().core.idle, 1);
    assert_eq!(p.metrics().core.dropped_overflow, 2, "成功插入不记容量因");
}

#[test]
fn insert_runs_on_return_reset_hook() {
    let (p, resets, _live, _drops) = resettable_pool(
        |t: &mut Tracked| t.payload = None, // 重置字段
        ResetPhase::OnReturn,
    );
    let (ext, _ext_live, _drops_ext) = external(5, Some("dirty"));

    assert!(p.insert(ext).is_none());
    assert_eq!(
        resets.load(Ordering::SeqCst),
        1,
        "插入 = 归还语义（OnReturn 相位必须跑钩子，否则异形对象污染下一个借出方）"
    );

    let g = p.acquire();
    assert_eq!(g.payload, None, "重置字段已清");
    assert_eq!(g.hot, 5, "保留字段在");
    assert_eq!(p.metrics().constructed, 0, "插入件走复用，不构造");
    assert_eq!(p.metrics().reused, 1);
}

#[test]
fn insert_skips_reset_in_on_acquire_phase() {
    let (p, resets, _live, _drops) =
        resettable_pool(|t: &mut Tracked| t.payload = None, ResetPhase::OnAcquire);
    let (ext, _ext_live, _drops_ext) = external(6, Some("dirty"));

    assert!(p.insert(ext).is_none());
    assert_eq!(
        resets.load(Ordering::SeqCst),
        0,
        "OnAcquire 相位：插入不触发钩子（时点在取出，同归还路径）"
    );

    let g = p.acquire();
    assert_eq!(resets.load(Ordering::SeqCst), 1, "取出时才重置");
    assert_eq!(g.payload, None);
}

#[test]
fn insert_reset_panic_discards_object_and_pool_survives() {
    const MARK: u64 = u64::MAX;
    let (p, resets, _live, _drops) = resettable_pool(
        |t: &mut Tracked| {
            if t.hot == MARK {
                panic!("insert reset boom");
            }
            t.payload = None;
        },
        ResetPhase::OnReturn,
    );

    let (ext, ext_live, ext_drops) = external(MARK, Some("dirty"));
    let out = p.insert(ext); // 钩子 panic ⇒ 捕获兜底（禁 abort）
    assert!(out.is_none(), "钩子失败件已被销毁，不再交回调用方");
    assert_eq!(resets.load(Ordering::SeqCst), 1, "钩子确实运行过");
    assert_eq!(ext_drops.load(Ordering::SeqCst), 1, "对象已销毁");
    assert_eq!(ext_live.load(Ordering::SeqCst), 0);

    let m = p.metrics();
    assert_eq!(m.reset_panicked, 1, "钩子因计数留痕");
    assert_eq!(m.core.dropped_overflow, 0, "钩子因不得混进容量口径");
    assert_eq!(m.core.idle, 0, "状态不可信对象不入 idle");
    assert_eq!(m.constructed, 0, "插入失败不构造（宇宙未被占用）");

    let (ok, _l, _d) = external(1, None); // 池仍可用（宇宙配额未漏计）
    assert!(p.insert(ok).is_none());
    assert_eq!(p.metrics().core.idle, 1);
}

#[test]
fn inserted_object_is_pruned_and_alive_reclaimed() {
    // 插入认领 `alive` 的硬证据：`prune_idle` 会回卷 alive（`take` 在账面不准时
    // panic）——插入若不认领，prune 立刻炸「universe alive would go negative」。
    let (p, _live, _drops) = tracked_pool(2, 2);
    let (a, _al, ad) = external(1, None);
    assert!(p.insert(a).is_none());

    assert_eq!(p.prune_idle_with_clock(Duration::ZERO, &SystemClock), 1);
    assert_eq!(ad.load(Ordering::SeqCst), 1, "老化释放即析构");
    let m = p.metrics();
    assert_eq!(m.pruned, 1);
    assert_eq!(m.core.idle, 0);
    assert_eq!(m.core.dropped_overflow, 0, "老化走 pruned，不混容量因");

    // 回收后宇宙余量恢复：再插一次仍成功，再 prune 仍不 panic。
    let (b, _bl, _bd) = external(2, None);
    assert!(p.insert(b).is_none());
    assert_eq!(p.prune_idle_with_clock(Duration::ZERO, &SystemClock), 1);
    assert_eq!(p.metrics().pruned, 2);
}

#[test]
fn insert_audit_emits_returned_and_overflow_dropped() {
    let (p, _live, _drops) = tracked_pool(2, 1);
    let got: Arc<Mutex<Vec<PoolAuditEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&got);
    let _ticket = PoolManaged::subscribe_audit(&p, move |e| {
        sink.lock().expect("审计槽未毒化").push(*e);
    });

    let (a, _al, _ad) = external(1, None);
    assert!(p.insert(a).is_none()); // Returned
    let (b, _bl, _bd) = external(2, None);
    assert!(p.insert(b).is_some()); // OverflowDropped

    let events = got.lock().expect("审计槽未毒化").clone();
    let kinds: Vec<PoolAuditKind> = events.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![PoolAuditKind::Returned, PoolAuditKind::OverflowDropped],
        "插入两路各发一次事件（借用/构造面零事件——插入不借出）"
    );
    for w in events.windows(2) {
        assert!(w[0].seq < w[1].seq, "seq 单调：{events:?}");
    }
}

#[test]
fn insert_follows_resized_idle_cap() {
    // idle_cap=2 ⇒ 2 片、片配额 [1,1]：两次插入分别落两片。
    let (p, _live, _drops) = tracked_pool(4, 2);
    let (a, _al, _ad) = external(1, None);
    let (b, _bl, _bd) = external(2, None);
    assert!(p.insert(a).is_none());
    assert!(p.insert(b).is_none());
    assert_eq!(p.metrics().core.idle, 2);

    PoolManaged::resize(
        &p,
        ObjectPoolTuning {
            max: 4,
            idle_cap: 1,
        },
    )
    .expect("合法缩容"); // 片配额 [1,0] ⇒ 裁掉 1 件
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "缩容裁剪后 idle 收敛到新配额");
    assert_eq!(m.core.dropped_overflow, 1, "裁剪走容量因");

    let (c, _cl, _cd) = external(3, None);
    assert!(
        p.insert(c).is_some(),
        "新配额下无空位 ⇒ 拒收（不残留旧额度）"
    );
    assert_eq!(p.metrics().core.idle, 1);
}

// ── 拥有式取用（acquire_owned）：跨 'static 边界 + Drop 归还 ───────────────

#[test]
fn acquire_owned_crosses_static_boundary_and_returns_on_drop() {
    let (p, _live, _drops) = tracked_pool(2, 2);
    let p = Arc::new(p);
    let mut g = p.acquire_owned();
    g.hot = 11;
    assert_eq!(p.metrics().core.borrowed, 1);

    // 句柄是 'static + Send：move 进线程并在**另一个线程**析构 ⇒ 归还回池。
    let moved = g;
    thread::spawn(move || {
        assert_eq!(moved.hot, 11);
        drop(moved);
    })
    .join()
    .expect("归还线程不 panic");

    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "跨线程 Drop 归还到池");
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.constructed, 1, "全程一件（借还复用，无再构造）");

    let again = p.acquire_owned();
    assert_eq!(again.hot, 11, "复用同一件");
    assert_eq!(p.metrics().reused, 1);

    fn takes_static<T: 'static>(_: T) {}
    takes_static(again); // 类型级证据：句柄自身满足 'static
    assert_eq!(
        p.metrics().core.idle,
        1,
        "takes_static 里的句柄已随作用域归还"
    );
}

#[test]
fn acquire_owned_detach_does_not_repool_and_does_not_leak_pool() {
    let (p, live, _drops) = tracked_pool(2, 2);
    let p = Arc::new(p);
    let obj = p.acquire_owned().detach();
    let m = p.metrics();
    assert_eq!(m.core.idle, 0, "detach 不回池");
    assert_eq!(m.core.borrowed, 0, "借出数与宇宙一并回吐");
    drop(obj);
    assert_eq!(live.load(Ordering::SeqCst), 0, "detach 件由调用方析构");

    let again = p.acquire_owned(); // 池仍可用
    assert_eq!(p.metrics().constructed, 2);
    drop(again);
    assert_eq!(p.metrics().core.idle, 1);

    // detach 不得漏掉 `Arc` 强引用（否则池永不析构）：最后一个 Arc 放手即析构。
    let weak = Arc::downgrade(&p);
    drop(p);
    assert!(weak.upgrade().is_none(), "池已析构（Arc 计数干净）");
}

#[test]
fn acquire_owned_runs_on_return_reset_hook() {
    let (p, resets, _live, _drops) =
        resettable_pool(|t: &mut Tracked| t.payload = None, ResetPhase::OnReturn);
    let p = Arc::new(p);
    let mut g = p.acquire_owned();
    g.payload = Some("dirty");
    drop(g);
    assert_eq!(
        resets.load(Ordering::SeqCst),
        1,
        "Owned 与 Pooled 共用归还路径（含 reset 相位语义）"
    );
    assert_eq!(p.metrics().core.idle, 1);

    let g2 = p.acquire_owned();
    assert_eq!(g2.payload, None);
}

// ── 字节配额（jovl4 缺口 1）：借还配平 + 超限丢弃 + 共享预算仲裁 ─────────

/// 装置：8KiB 块池（借出时填充到恰 8192 容量，weight = `capacity()`）。
fn quota_pool(budget: usize) -> ObjectPool<Vec<u8>> {
    ObjectPool::with_byte_quota(8, 8, Vec::<u8>::new, budget, |v: &Vec<u8>| v.capacity())
}

/// 借一块并填充到恰 8192 字节（首次分配的 `capacity() == len == 8192`）。
fn fill_8k(pool: &ObjectPool<Vec<u8>>) -> Pooled<'_, Vec<u8>> {
    let mut buf = pool.acquire();
    buf.resize(8192, 0u8);
    buf
}

#[test]
fn byte_quota_drops_return_over_budget_and_keeps_ledger_balanced() {
    let pool = quota_pool(3 * 8192);
    let m = pool.metrics();
    assert_eq!(
        (m.byte_budget, m.idle_bytes),
        (3 * 8192, 0),
        "构造期读数：预算满额、零认领"
    );

    // 4 块**同时在借**（字节守恒：借出回卷、归还认领——超限只可能发生在
    // 「idle 常驻已占满预算 + 池外还有一块要还」）。
    let mut bufs: Vec<_> = (0..4).map(|_| fill_8k(&pool)).collect();
    let m = pool.metrics();
    assert_eq!(m.idle_bytes, 0, "借出中的对象不占预算（只记常驻 idle）");

    drop(bufs.split_off(1)); // 先还 3 块 → 占满预算
    let m = pool.metrics();
    assert_eq!(
        (m.core.idle, m.idle_bytes, m.core.dropped_overflow),
        (3, 3 * 8192, 0),
        "预算内归还全收"
    );

    // 第 4 块（池外最后一块）：字节超限 ⇒ 丢弃 + dropped_overflow（容量因）。
    drop(bufs);
    let m = pool.metrics();
    assert_eq!(
        (m.core.idle, m.idle_bytes),
        (3, 3 * 8192),
        "超预算归还不得入池"
    );
    assert_eq!(m.core.dropped_overflow, 1, "字节超限 = 容量因丢弃");
    assert_eq!(m.idle_bytes, m.byte_budget, "配平：held == total（满水位）");

    // 借还配平：借出对称回卷、归还再认领，held 始终 = idle 字节合计。
    let again: Vec<_> = (0..3).map(|_| pool.acquire()).collect();
    let m = pool.metrics();
    assert_eq!((m.core.idle, m.idle_bytes), (0, 0), "借出对称回卷字节额度");
    drop(again);
    assert_eq!(pool.metrics().idle_bytes, 3 * 8192, "归还再认领");
}

#[test]
fn byte_quota_releases_bytes_on_prune_and_resize() {
    let pool = quota_pool(4 * 8192);
    // 两块同时在借再归还 ⇒ idle 常驻 2 块（逐块借还会命中复用、字节守恒）。
    let bufs: Vec<_> = (0..2).map(|_| fill_8k(&pool)).collect();
    drop(bufs);
    assert_eq!(pool.metrics().idle_bytes, 2 * 8192);

    // prune（ZERO 特判 = 全释放）：字节额度随之回卷归零。
    assert_eq!(pool.prune_idle_with_clock(Duration::ZERO, &SystemClock), 2);
    let m = pool.metrics();
    assert_eq!((m.core.idle, m.idle_bytes), (0, 0), "prune 后字节归零");

    // resize 缩容裁剪：字节随裁剪回卷。
    let bufs: Vec<_> = (0..2).map(|_| fill_8k(&pool)).collect();
    drop(bufs);
    assert_eq!(pool.metrics().idle_bytes, 2 * 8192);
    PoolManaged::resize(
        &pool,
        ObjectPoolTuning {
            max: 8,
            idle_cap: 1,
        },
    )
    .expect("合法缩容");
    let m = pool.metrics();
    assert_eq!(
        m.idle_bytes, 8192,
        "裁剪 1 件 ⇒ 字节回卷到剩余驻留（idle=1）"
    );
    assert_eq!(m.core.idle, 1);
    assert_eq!(m.core.dropped_overflow, 1, "裁剪走容量因");
}

#[test]
fn shared_byte_budget_is_arbitrated_across_pools() {
    let budget = Arc::new(skeleton_infra::objpool::ByteBudget::new(2 * 8192));
    let mk = || {
        ObjectPool::with_shared_byte_quota(
            4,
            4,
            Vec::<u8>::new,
            Arc::clone(&budget),
            |v: &Vec<u8>| v.capacity(),
        )
    };
    let a = mk();
    let b = mk();
    assert_eq!(budget.available(), 2 * 8192);

    // A 池 2 块同时在借、一起归还 ⇒ 占满共享预算。
    let bufs: Vec<_> = (0..2).map(|_| fill_8k(&a)).collect();
    drop(bufs);
    assert_eq!(budget.available(), 0, "A 池占满共享预算");

    // B 池归还同规格块 ⇒ 余额为零被拒（共享仲裁：超预算方丢弃）。
    drop(fill_8k(&b));
    let m = b.metrics();
    assert_eq!(m.core.idle, 0, "B 池零余额不得入池");
    assert_eq!(
        m.idle_bytes, 16384,
        "共享预算 held 不变——读数是共享余额的 held（含 A 池认领），单一账本不双记"
    );
    assert_eq!(m.core.dropped_overflow, 1, "共享预算超限 = 容量因丢弃");

    // A 池借出（持有不还）回卷 8192 后，B 池归还即被接纳（同一份余额双向流动）。
    let _held = a.acquire(); // 持有至测试尾：借出期间余额回卷，给 B 池腾位
    assert_eq!(budget.available(), 8192);
    drop(fill_8k(&b));
    let m = b.metrics();
    assert_eq!(m.core.idle, 1, "余额腾出后 B 池归还入池");
    assert_eq!(m.idle_bytes, 16384);
    // 测试尾自然收尾：`_held` 归还时共享余额已满（B 池的 8192 认领）⇒ 丢弃 +
    // dropped_overflow，无后续断言依赖。
}

#[test]
fn byte_budget_fail_fast_on_zero_total_and_underflow() {
    // 公开构造器（with_byte_quota / with_shared_byte_quota）成对收参 ⇒
    // 「半配置」在类型上不可表达；build 的成对 assert 只防未来新增构造器。
    // 本测试钉 ByteBudget 自身的 fail-fast 面：
    let zero = catch_unwind(AssertUnwindSafe(|| {
        let _ = skeleton_infra::objpool::ByteBudget::new(0);
    }));
    assert!(
        zero.is_err(),
        "ByteBudget::new(0) 必须 panic（零预算池无从起用）"
    );

    let budget = skeleton_infra::objpool::ByteBudget::new(100);
    assert!(budget.try_take(60), "余额足 ⇒ 认领");
    assert!(!budget.try_take(60), "余额不足 ⇒ false（不部分认领）");
    assert_eq!(budget.held(), 60);
    assert_eq!(budget.available(), 40);
    budget.give_back(60);
    assert_eq!(budget.held(), 0);
    let underflow = catch_unwind(AssertUnwindSafe(|| budget.give_back(1)));
    assert!(underflow.is_err(), "下溢 = 记账 bug，fail-fast panic");
    assert!(budget.try_take(0), "零字节认领恒真（未计量面调用点短路）");
}

// ── 强制清零（jovl4 缺口 2）：with_zeroing_reset 跨借还串味防护 ──────────

#[test]
fn zeroing_reset_erases_payload_across_borrows() {
    // A 用后 B 取，B 读不到 A 字节（明文驻留面归零）。
    let pool = ObjectPool::with_zeroing_reset(4, 4, Vec::<u8>::new, |v: &mut Vec<u8>| {
        v.fill(0);
    });
    {
        let mut a = pool.acquire();
        a.clear();
        a.extend_from_slice(b"SECRET-PAYLOAD-BYTES");
        assert_eq!(a.len(), 20);
    } // 归还 ⇒ OnReturn 相位清零 ⇒ 入 idle

    let b = pool.acquire();
    assert!(
        b.iter().all(|&x| x == 0),
        "B 借出读不到 A 的任何字节（串味防护）"
    );
    assert_eq!(
        b.len(),
        20,
        "长度形态由 zero 钩子决定（本钩子保留 len 清值，容量归一）"
    );
}

#[test]
fn zeroing_reset_pool_reports_zeroing_intent_in_metrics_shape() {
    // 普通池与清零池的 metrics 形状一致（清零是构造路语义，不改变读数面）；
    // 本测试钉「构造路存在且可用」——串味语义见上一测试。
    let pool = ObjectPool::with_zeroing_reset(4, 2, Vec::<u8>::new, |v: &mut Vec<u8>| {
        v.clear();
    });
    drop(pool.acquire());
    let m = pool.metrics();
    assert_eq!(m.core.idle, 1);
    assert_eq!(
        (m.byte_budget, m.idle_bytes),
        (0, 0),
        "未配字节预算的池读数恒 0"
    );
}

// ── 自动老化（jovl4 缺口 3）：prune_idle_with_clock + MockClock 确定性推进 ─

#[test]
fn prune_idle_with_mock_clock_advances_deterministically() {
    use skeleton::clock::MockClock;

    let clock = MockClock::new();
    let pool = ObjectPool::new(4, 4, Vec::<u8>::new);
    drop(pool.acquire()); // 归还 1 件入 idle（last_used = 构造期附近真实时刻）
    assert_eq!(pool.metrics().core.idle, 1);

    // 未推进虚拟时间：不老化（0 龄 < 30s 窗）。
    assert_eq!(
        pool.prune_idle_with_clock(Duration::from_secs(30), &clock),
        0,
        "时钟未推进 ⇒ 无超龄对象"
    );

    // 注入时钟一次性推进 60s（无需真实等待）⇒ 超龄释放。
    clock.advance(Duration::from_secs(60));
    assert_eq!(
        pool.prune_idle_with_clock(Duration::from_secs(30), &clock),
        1,
        "虚拟时间推进 ⇒ 超龄对象确定性释放"
    );
    assert_eq!(pool.metrics().core.idle, 0);
    assert_eq!(pool.metrics().pruned, 1);
}

// ── weight 钩子重入防呆（issue#21）：debug 构建 fail-fast 检出 ──────────────

/// 钩子体内经共享槽延迟拿到池句柄再 acquire（消费方误用形态——钩子构造期
/// 拿不到池自身，运行期才可达）。debug 构建在片锁取锁点 fail-fast（检出先于
/// 取锁 ⇒ 无锁毒化）；release 无防呆但同样禁（doc 红线，本测试 cfg 出）。
#[cfg(debug_assertions)]
#[test]
fn weight_hook_reentry_is_detected_in_debug() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let slot: Arc<Mutex<Option<Arc<ObjectPool<Tracked>>>>> = Arc::new(Mutex::new(None));
    let hook_slot = Arc::clone(&slot);
    let live = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let pool = Arc::new(ObjectPool::with_byte_quota(
        4,
        2,
        Tracked::factory(Arc::clone(&live), Arc::clone(&drops)),
        1024,
        move |t: &Tracked| {
            // 误用：钩子体内借还同一池（延迟句柄）。
            if let Some(p) = hook_slot.lock().expect("slot unpoisoned").clone() {
                let _guard = p.acquire();
            }
            t.hot as usize
        },
    ));
    *slot.lock().expect("slot unpoisoned") = Some(Arc::clone(&pool));

    // insert 的计量点（片锁外调 weight）触发钩子 → 重入 acquire → 取片锁时
    // debug_assert 翻牌。
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let (ext, _l, _d) = external(1, None);
        let _ = pool.insert(ext);
    }));
    assert!(
        outcome.is_err(),
        "debug 构建须 fail-fast 检出 weight 钩子重入（同线程双重加锁死锁面）"
    );

    // 池仍干净可用：对象未入槽（计量即拒）、零构造、账面无变动。
    let m = pool.metrics();
    assert_eq!(m.core.idle, 0);
    assert_eq!(m.constructed, 0, "插入件未入槽（计量点即检出）");
}
