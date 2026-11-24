#![cfg(all(feature = "memgov", feature = "pool"))]
//! MemoryPool 内存池特化端到端：借还语义（RAII Drop 即还 / 获取超时必填 / 容量
//! 有界）+ resize（扩容即时唤醒 / 缩容只弃 idle）+ 审计事件全断言 + 稳态零分配
//! （性能矩阵①，counting allocator 本线程净额口径）。
//!
//! 分工：PoolBase 公共逻辑（校验/水位线/审计/溢出计数）已在 `tests/pool_base.rs`
//! 逐件覆盖——本文件只测**特化面**（槽位借还/等待/resize 联动/工厂 panic 恢复），
//! 与基座交汇处按端到端断言不复测基座内部。

use std::alloc::{GlobalAlloc, Layout};
use std::borrow::Borrow;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use skeleton::pool::{PoolAuditKind, PoolManaged, PoolResizeError};
use skeleton_infra::memgov::{CountingAllocator, MemoryPool, MemoryTuning};

// 本线程存活分配净额（alloc/alloc_zeroed 成功 +1、dealloc −1；realloc 换块
// 旧死新生对消，净额不动）。稳态窗只读本线程净额：同二进制并发测试线程/
// libtest 主线程的分配不进窗，并行整跑免疫污染（旧版全局 live 净额被并发
// 净漂移打穿，CI windows 实测 64→57）。
thread_local! {
    static OWN_LIVE: Cell<isize> = const { Cell::new(0) };
}

struct TlLive;

// SAFETY: 转发 CountingAllocator（全局五计数器照常累计），外加本线程归属净额；
// null 原样透传（alloc 契约）。const 初始化 TLS 无惰性分配、无析构注册，
// 分配器内读写不递归进分配。
unsafe impl GlobalAlloc for TlLive {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { CountingAllocator.alloc(layout) };
        if !ptr.is_null() {
            // try_with:线程 TLS 析构期访问会 panic(分配器内 panic=abort),取不到就不计
            OWN_LIVE.try_with(|c| c.set(c.get() + 1)).ok();
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { CountingAllocator.alloc_zeroed(layout) };
        if !ptr.is_null() {
            OWN_LIVE.try_with(|c| c.set(c.get() + 1)).ok();
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        OWN_LIVE.try_with(|c| c.set(c.get() - 1)).ok();
        unsafe { CountingAllocator.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        unsafe { CountingAllocator.realloc(ptr, layout, new_size) }
    }
}

/// 计数分配器挂全局（本二进制专用；同 tests/memgov.rs 纪律——lib 二进制不挂）。
#[global_allocator]
static COUNTING: TlLive = TlLive;

/// 本线程当前存活分配净额。
fn own_live() -> isize {
    OWN_LIVE.with(Cell::get)
}

fn pool(max: usize, idle_cap: usize) -> MemoryPool<Vec<u8>, fn() -> Vec<u8>> {
    MemoryPool::new(max, idle_cap, Vec::new)
}

// ── 借还语义：RAII Drop 即还 + 同件复用（工厂不再调用）──────────────────

#[test]
fn acquire_creates_then_reuses_same_object() {
    let created = AtomicUsize::new(0);
    let make = || {
        created.fetch_add(1, Ordering::SeqCst);
        Vec::<u8>::new()
    };
    let p = MemoryPool::new(2, 2, make);
    assert_eq!(created.load(Ordering::SeqCst), 0);

    {
        let mut g = p.acquire(Duration::from_millis(100)).unwrap();
        g.push(7);
        assert_eq!(created.load(Ordering::SeqCst), 1);
        assert_eq!(p.metrics().core.borrowed, 1);
    } // Drop 即还（RAII，禁手动还）
    assert_eq!(p.metrics().core.borrowed, 0);
    assert_eq!(p.metrics().core.idle, 1);
    assert_eq!(created.load(Ordering::SeqCst), 1);

    let g = p.acquire(Duration::from_millis(100)).unwrap();
    assert_eq!(
        created.load(Ordering::SeqCst),
        1,
        "闲置件复用，工厂不得再调"
    );
    assert_eq!(&*g, &[7], "同一件对象（状态随件保留）");
}

#[test]
fn new_rejects_illegal_config_fail_fast() {
    let result = std::panic::catch_unwind(|| {
        let _ = MemoryPool::new(0, 1, Vec::<u8>::new);
    });
    assert!(
        result.is_err(),
        "max==0 必须 fail-fast（同 PoolBase::new 纪律）"
    );
}

// ── 获取超时必填（D21）：拿不到=显式错，禁无限等 ─────────────────────────

#[test]
fn acquire_times_out_when_exhausted() {
    let p = pool(1, 1);
    let g = p.acquire(Duration::from_millis(100)).unwrap();

    let t0 = Instant::now();
    let err = p.acquire(Duration::from_millis(30));
    let elapsed = t0.elapsed();
    assert!(err.is_err(), "池耗尽必须显式超时错");
    assert!(
        elapsed >= Duration::from_millis(15),
        "实测 {elapsed:?}——未等待即返回说明超时形同虚设"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "实测 {elapsed:?}——超时不许远超请求值"
    );
    drop(g);
}

#[test]
fn zero_timeout_is_nonblocking_try_acquire() {
    let p = pool(1, 1);
    let _g = p.acquire(Duration::from_millis(100)).unwrap();
    let t0 = Instant::now();
    assert!(p.acquire(Duration::ZERO).is_err());
    assert!(
        t0.elapsed() < Duration::from_secs(1),
        "零超时=立即尝试，不得阻塞"
    );
}

// ── 容量有界：并发竞争下工厂调用数永不越 max ─────────────────────────────

#[test]
fn max_capacity_is_hard_bound_under_contention() {
    let p = Arc::new(pool(4, 4));
    let acquires = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (p, acquires) = (Arc::clone(&p), Arc::clone(&acquires));
        handles.push(thread::spawn(move || {
            for _ in 0..200 {
                if let Ok(g) = p.acquire(Duration::from_millis(500)) {
                    acquires.fetch_add(1, Ordering::SeqCst);
                    drop(g);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let m = p.metrics();
    assert!(
        acquires.load(Ordering::SeqCst) > 0,
        "负载窗内必须有成功借出"
    );
    assert!(m.core.borrowed == 0, "全部归还后借出数必须归零");
    assert!(m.core.idle <= 4 && m.core.idle >= 1);
    assert_eq!(m.core.dropped_overflow, 0, "未缩容不得有溢出丢弃");
    assert!(m.core.watermark <= 1.0);
}

// ── resize：事前校验拒绝不改状态 / 扩容即时唤醒 / 缩容只弃 idle ──────────

#[test]
fn resize_rejects_illegal_params_without_state_change() {
    let p = pool(4, 2);
    let before = p.metrics();
    assert_eq!(
        PoolManaged::resize(
            &p,
            MemoryTuning {
                max: 0,
                idle_cap: 1
            }
        ),
        Err(PoolResizeError::ZeroMax)
    );
    assert_eq!(
        PoolManaged::resize(
            &p,
            MemoryTuning {
                max: 4,
                idle_cap: 0
            }
        ),
        Err(PoolResizeError::IdleCapZero)
    );
    assert_eq!(
        PoolManaged::resize(
            &p,
            MemoryTuning {
                max: 2,
                idle_cap: 3
            }
        ),
        Err(PoolResizeError::IdleCapExceedsMax {
            idle_cap: 3,
            max: 2
        })
    );
    assert_eq!(p.metrics(), before, "拒绝路径不得改任何状态");
}

#[test]
fn resize_expand_unblocks_waiter_without_full_timeout() {
    let p = Arc::new(pool(1, 1));
    let holder = p.acquire(Duration::from_millis(100)).unwrap();
    let (tx, rx) = mpsc::channel();
    let waiter = {
        let p = Arc::clone(&p);
        thread::spawn(move || {
            let g = p.acquire(Duration::from_millis(5_000)).unwrap();
            tx.send(()).unwrap();
            drop(g); // guard 借用线程内 p——就地归还，不外传
        })
    };
    thread::sleep(Duration::from_millis(100)); // 让等待者真正入 park

    let t0 = Instant::now();
    p.resize(MemoryTuning {
        max: 3,
        idle_cap: 2,
    })
    .unwrap();
    rx.recv_timeout(Duration::from_secs(1))
        .expect("扩容必须即时唤醒等待者（远早于其 5s 超时）");
    assert!(t0.elapsed() < Duration::from_secs(1));
    waiter.join().unwrap();
    drop(holder);
}

#[test]
fn resize_shrink_truncates_idle_and_created_quota_follows() {
    let created = AtomicUsize::new(0);
    let make = || {
        created.fetch_add(1, Ordering::SeqCst);
        Vec::<u8>::new()
    };
    let p = MemoryPool::new(4, 4, make);
    let g1 = p.acquire(Duration::from_millis(100)).unwrap();
    let g2 = p.acquire(Duration::from_millis(100)).unwrap();
    drop(g1);
    drop(g2); // idle=2 borrowed=0 created=2 factory=2

    PoolManaged::resize(
        &p,
        MemoryTuning {
            max: 2,
            idle_cap: 1,
        },
    )
    .unwrap();
    let m = p.metrics();
    assert_eq!(m.core.idle, 1, "缩容只弃 idle 槽：2→1");
    assert_eq!(m.core.borrowed, 0);

    let g3 = p.acquire(Duration::from_millis(100)).unwrap(); // 复用留存件（idle→0）
    let g4 = p
        .acquire(Duration::from_millis(100))
        .expect("配额回吐证明：弃 idle 必须同步递减 created，否则 created=2 堵死新建路径");
    assert_eq!(
        created.load(Ordering::SeqCst),
        3,
        "g4 走工厂新建（留存件已借出）"
    );
    drop(g3); // idle(0)<1 && 0+1<2 → 留存
    drop(g4); // idle(1)<1 不成立 → 溢出丢弃+计数
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0);
    assert_eq!(m.core.idle, 1);
    assert_eq!(m.core.dropped_overflow, 1);
}

#[test]
fn return_after_shrink_full_slot_drops_and_counts() {
    let p = pool(3, 3);
    let g1 = p.acquire(Duration::from_millis(100)).unwrap();
    let g2 = p.acquire(Duration::from_millis(100)).unwrap();
    let g3 = p.acquire(Duration::from_millis(100)).unwrap();

    PoolManaged::resize(
        &p,
        MemoryTuning {
            max: 1,
            idle_cap: 1,
        },
    )
    .unwrap();
    drop(g1); // borrowed=2 > max=1：保留判据 idle+borrowed<max 不成立 → 丢弃
    drop(g2); // borrowed=1：0+1<1 不成立 → 丢弃
    assert_eq!(
        p.metrics().core.dropped_overflow,
        2,
        "缩容后归还槽满=丢弃+计数"
    );
    drop(g3); // borrowed=0：0<1 && 0+0<1 → 留存
    let m = p.metrics();
    assert_eq!(m.core.dropped_overflow, 2);
    assert_eq!(m.core.idle, 1);
    assert_eq!(m.core.borrowed, 0);
}

// ── 审计：池域事件流经 trait 挂点逐事件断言 ─────────────────────────────

#[test]
fn audit_events_cover_full_lifecycle_with_monotonic_seq() {
    let p = pool(2, 2);
    let ticket = PoolManaged::subscribe_audit(&p, |e| {
        let _ = e; // 实时回调面；对账走 take 侧
    });
    let _keep_alive = ticket;

    let g = p.acquire(Duration::from_millis(100)).unwrap();
    drop(g);
    PoolManaged::resize(
        &p,
        MemoryTuning {
            max: 2,
            idle_cap: 1,
        },
    )
    .unwrap();
    assert!(
        PoolManaged::resize(
            &p,
            MemoryTuning {
                max: 0,
                idle_cap: 1
            }
        )
        .is_err(),
        "拒绝也进审计（事前校验留痕）"
    );

    // 制造一次溢出归还：借满→缩到 (1,1)→双还
    let a = p.acquire(Duration::from_millis(100)).unwrap();
    let b = p.acquire(Duration::from_millis(100)).unwrap();
    PoolManaged::resize(
        &p,
        MemoryTuning {
            max: 1,
            idle_cap: 1,
        },
    )
    .unwrap();
    drop(a);
    drop(b);

    let events = p.take_audit_events();
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
}

// ── 工厂 panic：容量配额回滚，池继续可用（fail-visible 不留半死态）────────

#[test]
fn factory_panic_restores_capacity_and_pool_stays_usable() {
    let boom = Arc::new(AtomicBool::new(false));
    let make = {
        let boom = Arc::clone(&boom);
        move || {
            if boom.swap(false, Ordering::SeqCst) {
                panic!("factory boom");
            }
            Vec::<u8>::new()
        }
    };
    let p = MemoryPool::new(2, 2, make);
    let g1 = p.acquire(Duration::from_millis(100)).unwrap();

    boom.store(true, Ordering::SeqCst);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = p.acquire(Duration::from_millis(100)).unwrap();
    }));
    assert!(result.is_err(), "工厂 panic 必须如实传播");

    // 配额已回滚：这次借出成功（若 created 泄漏，max=2 已耗尽会超时）
    let g2 = p
        .acquire(Duration::from_millis(100))
        .expect("panic 回滚后池必须可用");
    drop(g1);
    drop(g2);
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0);
    assert!(m.core.idle <= 2);
}

// ── Metrics 共享核心形经 Borrow 可达 + 特有读数 idle_cap ─────────────────

#[test]
fn metrics_expose_core_via_borrow_and_idle_cap() {
    let p = pool(4, 2);
    let m = p.metrics();
    assert_eq!(m.idle_cap, 2, "内存池特有读数：闲槽上限");
    let core: &skeleton::pool::PoolMetrics = m.borrow();
    assert_eq!(core.max, 4);
    assert_eq!(core.idle, 0);
    assert_eq!(core.borrowed, 0);
    assert_eq!(core.dropped_overflow, 0);
    assert_eq!(core.watermark, 0.0);

    let g = p.acquire(Duration::from_millis(100)).unwrap();
    let m = p.metrics();
    assert!((m.core.watermark - 0.25).abs() < 1e-9);
    drop(g);
}

// ── 缩容与借还并发：无死锁、无 panic、终态账实一致 ───────────────────────

#[test]
fn concurrent_resize_and_hammering_stays_consistent() {
    let p = Arc::new(pool(8, 8));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for _ in 0..4 {
        let (p, stop) = (Arc::clone(&p), Arc::clone(&stop));
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                if let Ok(g) = p.acquire(Duration::from_millis(50)) {
                    drop(g);
                }
            }
        }));
    }
    let resizer = {
        let p = Arc::clone(&p);
        thread::spawn(move || {
            for i in 0..50 {
                let t = if i % 2 == 0 {
                    MemoryTuning {
                        max: 2,
                        idle_cap: 2,
                    }
                } else {
                    MemoryTuning {
                        max: 8,
                        idle_cap: 8,
                    }
                };
                PoolManaged::resize(&*p, t).unwrap();
                thread::sleep(Duration::from_micros(200));
            }
        })
    };
    resizer.join().unwrap();
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    PoolManaged::resize(
        &*p,
        MemoryTuning {
            max: 8,
            idle_cap: 8,
        },
    )
    .unwrap();
    let m = p.metrics();
    assert_eq!(m.core.borrowed, 0, "终态必须无借出");
    assert!(m.core.idle <= 8, "终态 idle 不得越界：实际 {}", m.core.idle);
}

// ── 性能矩阵①：稳态借/还热路径零分配（本线程 live 净额断言=0）─────────────

#[test]
fn steady_state_borrow_return_is_zero_alloc() {
    // 稳态复用路径：预热一件，借/还各 1000 次。读数窗只认本线程净额，
    // 并发分配本就不进窗——无需 settle/串行闸。
    let p = pool(2, 2);
    drop(p.acquire(Duration::from_millis(100)).unwrap()); // 预热：工厂/Vec 容量分配在窗外
    let own_before = own_live();
    for _ in 0..1000 {
        drop(p.acquire(Duration::from_millis(100)).unwrap());
    }
    assert_eq!(
        own_live(),
        own_before,
        "稳态借/还热路径零分配（1000 次往返本线程 live 净增必须为 0）"
    );

    // 溢出丢弃归还路径（丢弃+计数）：同样零分配。
    // 形态 (2,1)：每轮 借闲件+借新件 → 还第一件留存、还第二件溢出丢弃——
    // 单件自还永不判溢出（0+0<max 恒真），两件轮转才持续走到丢弃分支。
    let p = MemoryPool::<Vec<u8>>::new(2, 1, Vec::new);
    drop(p.acquire(Duration::from_millis(100)).unwrap()); // 预热 idle=1
    let own_before = own_live();
    for _ in 0..1000 {
        let a = p.acquire(Duration::from_millis(100)).unwrap();
        let b = p.acquire(Duration::from_millis(100)).unwrap();
        drop(a); // 留存（idle 未满）
        drop(b); // 溢出丢弃（idle_cap=1 已满）
    }
    assert_eq!(own_live(), own_before, "溢出丢弃归还路径零分配");
    assert!(
        p.metrics().core.dropped_overflow > 0,
        "溢出路径确实走到了丢弃分支"
    );
}

// ── issue#98：归还裁决与槽位提交的跨锁原子性（core 临界区内挂住归还线程）───
//
// 旧实现把「core 账本裁决」与「inner.idle push / created 回吐」分成两个临界区：
// 一次按旧边界批准的归还可以在缩容完成后才真正入槽（越过新 cap），Returned /
// OverflowDropped 事件也早于槽位提交（回调重入看到中间态）。以下测试用
// `feature = "testing"` 的归还窗闸把归还线程挂在 **core 临界区内的 pre-commit
// 窗**，确定性复现/钉死该窗口（禁 sleep 控交错）。

#[cfg(feature = "testing")]
mod return_atomicity {
    use std::sync::{Mutex, OnceLock, Weak};

    use skeleton_infra::memgov::ReturnGate;

    use super::*;

    type PoolFn = fn() -> Vec<u8>;
    type SharedPool = Arc<MemoryPool<Vec<u8>, PoolFn>>;

    /// 放行闸的 RAII 兜底：断言 panic 时不留 park 住的归还线程。
    struct ReleaseOnDrop<'a>(&'a ReturnGate);

    impl Drop for ReleaseOnDrop<'_> {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    /// 越界丢弃的对象（析构次数可观测——「锁外析构恰好一次」的对账面）。
    struct Tracked(Arc<AtomicUsize>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn shared_pool(max: usize, idle_cap: usize) -> SharedPool {
        Arc::new(MemoryPool::new(max, idle_cap, Vec::new))
    }

    /// 静止点对账（resize 已返回、无在途借还）：账本读数 == 真实槽位、边界不破、
    /// 存活件数与 idle+borrowed 对得上。
    fn assert_quiescent<T, F: Fn() -> T>(p: &MemoryPool<T, F>, what: &str) {
        let m = p.metrics();
        let (slots, created) = p.slot_snapshot();
        assert_eq!(m.core.borrowed, 0, "{what}: 静止点不得有借出");
        assert_eq!(m.core.idle, slots, "{what}: 账本 idle 与真实槽数必须一致");
        assert!(
            slots <= m.idle_cap,
            "{what}: 真实闲槽 {slots} 越过 idle_cap {}",
            m.idle_cap
        );
        assert!(
            m.core.idle <= m.idle_cap,
            "{what}: 账本 idle {} 越过 idle_cap {}",
            m.core.idle,
            m.idle_cap
        );
        assert!(
            created <= m.core.max,
            "{what}: 存活件 {created} 越过 max {}",
            m.core.max
        );
        assert_eq!(
            created,
            slots + m.core.borrowed,
            "{what}: created 与 idle+borrowed 对账失配"
        );
    }

    /// 确定性交错：归还线程在「core 已裁决、槽位未提交」窗内 park，主线程发起
    /// shrink 并观察它能否在该窗内完成。`true` = resize 穿过了未提交的归还（旧
    /// 实现的竞态）；`false` = resize 被 core 挡住直到提交完成（新实现的原子性）。
    fn resize_inside_return_window(p: &SharedPool, warm: usize, tuning: MemoryTuning) -> bool {
        // 同时持有 warm 件再一并归还：造出 warm 个**不同**对象进闲槽（逐个借还
        // 只会复用同一件，inner.idle 永远到不了新 cap，复现不出越界）。
        let warm_guards: Vec<_> = (0..warm)
            .map(|_| p.acquire(Duration::ZERO).expect("warm slot"))
            .collect();
        drop(warm_guards);
        p.return_gate().arm();
        let gate = ReleaseOnDrop(p.return_gate());

        let returner = {
            let p = Arc::clone(p);
            thread::spawn(move || {
                let g = p.acquire(Duration::ZERO).expect("borrow a prewarmed slot");
                drop(g); // Drop ⇒ 归还路径：在 core 临界区内 park
            })
        };
        assert!(
            p.return_gate().wait_entered(Duration::from_secs(5)),
            "归还线程必须进入 core 临界区内的 pre-commit 窗"
        );

        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let resizer = {
            let p = Arc::clone(p);
            thread::spawn(move || {
                started_tx.send(()).expect("started signal");
                p.resize(tuning).expect("legal tuning");
                done_tx.send(()).expect("done signal");
            })
        };
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("resizer 必须起跑");
        let finished_in_window = done_rx.recv_timeout(Duration::from_millis(500)).is_ok();
        gate.0.release();
        returner.join().expect("returner");
        resizer.join().expect("resizer");
        finished_in_window
    }

    /// 静止点读数一行（断言消息里带上具体越界数字）。
    fn quiescent_report<T, F: Fn() -> T>(p: &MemoryPool<T, F>) -> String {
        let m = p.metrics();
        let (slots, created) = p.slot_snapshot();
        format!(
            "账本 idle {} / 真实闲槽 {slots} / idle_cap {} / created {created} / max {} / borrowed {}",
            m.core.idle, m.idle_cap, m.core.max, m.core.borrowed
        )
    }

    #[test]
    fn shrink_idle_cap_waits_for_borrowed_return_commit() {
        let p = shared_pool(4, 4);
        let early = resize_inside_return_window(
            &p,
            2,
            MemoryTuning {
                max: 4,
                idle_cap: 1,
            },
        );
        assert!(
            !early,
            "resize 在归还 pre-commit 窗内完成 ⇒ core 账本与 idle 槽位非原子（issue#98）；{}",
            quiescent_report(&p)
        );
        assert_quiescent(&p, "只缩 idle_cap");
    }

    #[test]
    fn shrink_max_and_idle_cap_waits_for_borrowed_return_commit() {
        let p = shared_pool(4, 4);
        let early = resize_inside_return_window(
            &p,
            2,
            MemoryTuning {
                max: 1,
                idle_cap: 1,
            },
        );
        assert!(
            !early,
            "resize 在归还 pre-commit 窗内完成 ⇒ 旧边界下批准的留存越过新 max（issue#98）；{}",
            quiescent_report(&p)
        );
        assert_quiescent(&p, "同时缩 max 与 idle_cap");
    }

    #[test]
    fn expand_after_atomic_shrink_keeps_ledger_consistent() {
        let p = shared_pool(4, 4);
        let early = resize_inside_return_window(
            &p,
            2,
            MemoryTuning {
                max: 1,
                idle_cap: 1,
            },
        );
        assert!(
            !early,
            "缩容必须等归还提交（issue#98）；{}",
            quiescent_report(&p)
        );
        assert_quiescent(&p, "缩容后静止点");
        p.resize(MemoryTuning {
            max: 8,
            idle_cap: 8,
        })
        .expect("legal expand");
        assert_quiescent(&p, "缩容后扩容静止点");
    }

    /// 借出件跨缩容存活：resize 先赢，归还按**新**边界裁决——越界丢弃恰好一次
    /// （`dropped_overflow` + `created` 回吐 + 锁外析构），留存件在提交后唤醒
    /// 等待者，等待者复用该件而不越过新池深新建。
    #[test]
    fn borrowed_return_after_shrink_decides_under_new_bounds() {
        let created = Arc::new(AtomicUsize::new(0));
        let destructed = Arc::new(AtomicUsize::new(0));
        let p = {
            let (created, destructed) = (Arc::clone(&created), Arc::clone(&destructed));
            MemoryPool::new(2, 2, move || {
                created.fetch_add(1, Ordering::SeqCst);
                Tracked(Arc::clone(&destructed))
            })
        };

        let a = p.acquire(Duration::ZERO).expect("slot a");
        let b = p.acquire(Duration::ZERO).expect("slot b");
        assert_eq!(created.load(Ordering::SeqCst), 2);
        p.resize(MemoryTuning {
            max: 1,
            idle_cap: 1,
        })
        .expect("legal shrink");
        // 缩容后借出件仍在外：合法中间态（borrowed 可 > 新 max）
        assert_eq!(p.metrics().core.borrowed, 2);
        assert_eq!(p.slot_snapshot(), (0, 2));

        drop(a); // 新边界下 0+1<1 不成立 ⇒ 越界丢弃
        assert_eq!(
            destructed.load(Ordering::SeqCst),
            1,
            "越界丢弃的对象必须在锁外恰好析构一次"
        );
        assert_eq!(p.metrics().core.dropped_overflow, 1, "溢出计数恰好一次");
        assert_eq!(p.slot_snapshot(), (0, 1), "丢弃必须回吐 create 配额一次");

        // 等待者此刻拿不到件（created == max 且 idle 空）：必须由留存件提交后的
        // 唤醒接手，而不是靠新建越过新池深。
        let (borrowed_tx, borrowed_rx) = mpsc::channel();
        thread::scope(|s| {
            let waiter = s.spawn(|| {
                let g = p.acquire(Duration::from_secs(5)).expect("留存件唤醒等待者");
                borrowed_tx.send(()).expect("borrowed signal");
                drop(g); // 归还自留（新边界内）
            });
            assert!(
                borrowed_rx
                    .recv_timeout(Duration::from_millis(200))
                    .is_err(),
                "缩容后无件可借，等待者不得提前完成"
            );
            drop(b); // 留存（新边界内）→ 提交后唤醒
            borrowed_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("等待者必须被留存件唤醒");
            waiter.join().expect("waiter");
        });

        assert_eq!(
            created.load(Ordering::SeqCst),
            2,
            "唤醒的等待者必须复用留存件，不得新建越过新池深"
        );
        let (slots, live) = p.slot_snapshot();
        assert_eq!((slots, live), (1, 1));
        let m = p.metrics();
        assert_eq!(m.core.idle, slots);
        assert_eq!(m.core.borrowed, 0);
    }

    /// 审计事件描述**已提交状态**：`Returned` / `OverflowDropped` 回调里重入
    /// 公开入口必须看到「账本 idle == 真实槽位」「created == idle + borrowed」
    /// 且能直接借到已提交的件（旧实现事件早于槽位提交/配额回吐）。
    #[test]
    fn audit_return_events_expose_committed_state() {
        let p = shared_pool(2, 1);
        let violations = Arc::new(Mutex::new(Vec::<String>::new()));
        let ticket = {
            let inside = Arc::clone(&p);
            let violations = Arc::clone(&violations);
            PoolManaged::subscribe_audit(&*p, move |e| {
                if !matches!(
                    e.kind,
                    PoolAuditKind::Returned | PoolAuditKind::OverflowDropped
                ) {
                    return;
                }
                let m = inside.metrics();
                let (slots, created) = inside.slot_snapshot();
                let push = |msg: String| violations.lock().expect("violations").push(msg);
                if m.core.idle != slots {
                    push(format!(
                        "{:?}: 账本 idle {} != 真实槽位 {slots}",
                        e.kind, m.core.idle
                    ));
                }
                if created != slots + m.core.borrowed {
                    push(format!(
                        "{:?}: created {created} != idle {slots} + borrowed {}",
                        e.kind, m.core.borrowed
                    ));
                }
                if slots > m.idle_cap {
                    push(format!(
                        "{:?}: 真实槽位 {slots} 越过 idle_cap {}",
                        e.kind, m.idle_cap
                    ));
                }
                if inside.acquire(Duration::ZERO).is_err() {
                    push(format!(
                        "{:?}: 回调重入 acquire 看不到已提交的槽位/配额",
                        e.kind
                    ));
                }
            })
        };

        // Returned：借一件即还（提交后事件才发）
        drop(p.acquire(Duration::from_millis(100)).expect("borrow"));
        // OverflowDropped：借满两件后双还——第一件留存、第二件溢出丢弃
        let a = p.acquire(Duration::from_millis(100)).expect("a");
        let b = p.acquire(Duration::from_millis(100)).expect("b");
        drop(a);
        drop(b);
        drop(ticket);

        let violations = violations.lock().expect("violations");
        assert!(
            violations.is_empty(),
            "审计回调看到未提交的中间态：{violations:#?}"
        );
    }

    // ── 用户对象析构不得落在内部闸内（Drop 可重入公开入口）──────────────────

    trait ResizeProbe: Send + Sync {
        fn probe(&self);
    }

    /// 缩容弃置的用户对象：`Drop` 里重入同一池的公开入口（本仓契约：用户类型的
    /// `Drop` 可以回调池公开 API）。
    struct ReentrantItem(Weak<dyn ResizeProbe>);

    impl Drop for ReentrantItem {
        fn drop(&mut self) {
            if let Some(probe) = self.0.upgrade() {
                probe.probe();
            }
        }
    }

    type ReentrantPool = MemoryPool<ReentrantItem, Box<dyn Fn() -> ReentrantItem + Send + Sync>>;

    /// 布防时在 `Drop` 里对同一池发起一次 `resize`：若析构发生在 `resizing`
    /// 闸内，该调用会在同线程上自等闸（std Mutex 非重入）而卡死。
    struct RealProbe {
        pool: OnceLock<Weak<ReentrantPool>>,
        armed: AtomicBool,
        completed: AtomicUsize,
    }

    impl ResizeProbe for RealProbe {
        fn probe(&self) {
            if !self.armed.load(Ordering::SeqCst) {
                return;
            }
            let pool = self
                .pool
                .get()
                .and_then(Weak::upgrade)
                .expect("probe pool alive");
            pool.resize(MemoryTuning {
                max: 2,
                idle_cap: 1,
            })
            .expect("reentrant resize is legal");
            self.completed.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// 缩容弃置的对象必须在 `resizing` 串行闸**之外**析构：闸内析构 ⇒ 用户
    /// `Drop` 重入 `resize` 自死锁（同线程等非重入锁）。用外层有界等待捕获，
    /// 挂死记成红而不是真的挂死。
    #[test]
    fn shrink_drops_user_objects_outside_the_serial_gate() {
        let probe = Arc::new(RealProbe {
            pool: OnceLock::new(),
            armed: AtomicBool::new(false),
            completed: AtomicUsize::new(0),
        });
        let probe_handle: Arc<dyn ResizeProbe> = Arc::clone(&probe) as Arc<dyn ResizeProbe>;
        let weak_probe: Weak<dyn ResizeProbe> = Arc::downgrade(&probe_handle);
        let p: Arc<ReentrantPool> = {
            let handle = weak_probe.clone();
            Arc::new(MemoryPool::new(
                2,
                2,
                Box::new(move || ReentrantItem(handle.clone()))
                    as Box<dyn Fn() -> ReentrantItem + Send + Sync>,
            ))
        };
        assert!(
            probe.pool.set(Arc::downgrade(&p)).is_ok(),
            "probe 只接线一次"
        );

        // 同时借出 2 件再归还（逐个借还只会复用同一件，闲槽到不了裁剪线）
        let held: Vec<_> = (0..2)
            .map(|_| p.acquire(Duration::ZERO).expect("slot"))
            .collect();
        drop(held);
        assert_eq!(p.slot_snapshot(), (2, 2));

        probe.armed.store(true, Ordering::SeqCst);
        let (done_tx, done_rx) = mpsc::channel();
        let resizer = {
            let p = Arc::clone(&p);
            thread::spawn(move || {
                p.resize(MemoryTuning {
                    max: 1,
                    idle_cap: 1,
                })
                .expect("legal shrink");
                done_tx.send(()).expect("done signal");
            })
        };
        let outcome = done_rx.recv_timeout(Duration::from_secs(5));
        probe.armed.store(false, Ordering::SeqCst);
        assert!(
            outcome.is_ok(),
            "缩容弃置的用户对象在 resizing 闸内析构 ⇒ 其 Drop 重入 resize 自死锁（析构必须在闸外）"
        );
        resizer.join().expect("resizer");
        assert_eq!(
            probe.completed.load(Ordering::SeqCst),
            1,
            "用户对象 Drop 内的重入 resize 必须成功返回恰好一次"
        );
        assert_quiescent(&p, "缩容弃置");
    }
}

// ── issue#99：resize 审计事件在跨层事务提交、全部内部锁释放后发射 ─────────
//
// 旧实现持 `resizing` 闸调 `PoolBase::resize`，后者同步发 `Resized`/
// `ResizeRejected` ⇒ 回调重入 `resize` 在同线程等非重入闸（确定性自锁），且回调
// 能看到「base 新值 + inner 旧值」。以下测试用 `feature = "testing"` 的锁探针 +
// 有界通道把两种失效记成红而不是挂死（禁 sleep 控交错）。

#[cfg(feature = "testing")]
mod resize_event_timing {
    use std::sync::Mutex;

    use super::*;

    const HANDOFF: Duration = Duration::from_secs(5);

    type SharedPool = Arc<MemoryPool<Vec<u8>, fn() -> Vec<u8>>>;

    fn shared_pool(max: usize, idle_cap: usize) -> SharedPool {
        Arc::new(pool(max, idle_cap))
    }

    /// 外层 resize 放到独立线程：回调若自等内部锁，本测试以超时记红（禁挂死）。
    fn resize_bounded(p: &SharedPool, tuning: MemoryTuning) -> Result<(), PoolResizeError> {
        let (done_tx, done_rx) = mpsc::channel();
        let p = Arc::clone(p);
        let handle = thread::spawn(move || {
            done_tx.send(p.resize(tuning)).expect("send resize result");
        });
        let result = done_rx
            .recv_timeout(HANDOFF)
            .expect("resize 不得卡在内部锁（审计回调重入路径）");
        handle.join().expect("resize thread");
        result
    }

    /// `Resized` 回调内重入同一个池的 `resize`：外层必须完成返回、内层必须有
    /// 确定结果（旧实现在 `resizing` 闸上确定性自锁）。
    #[test]
    fn resized_callback_reenters_resize_without_self_lock() {
        let p = shared_pool(4, 2);
        let nested = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&p);
            let nested = Arc::clone(&nested);
            PoolManaged::subscribe_audit(&*p, move |e| {
                if e.kind != PoolAuditKind::Resized {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                let result = inner.resize(MemoryTuning {
                    max: 8,
                    idle_cap: 4,
                });
                nested.lock().expect("nested results").push(result);
            })
        };

        resize_bounded(
            &p,
            MemoryTuning {
                max: 6,
                idle_cap: 3,
            },
        )
        .expect("outer resize");
        drop(ticket);

        let nested = nested.lock().expect("nested results");
        assert_eq!(nested.len(), 1, "一次成功 resize 恰触发一次 Resized 回调");
        assert!(
            nested[0].is_ok(),
            "回调内重入 resize 必须有确定结果：{nested:?}"
        );
        drop(nested);
        let m = p.metrics();
        assert_eq!(
            (m.core.max, m.idle_cap),
            (8, 4),
            "内层（后发起）目标必须生效"
        );
    }

    /// `ResizeRejected` 同款：拒绝事件也在锁外发射（旧实现在 `resizing` 闸内发）。
    #[test]
    fn rejected_callback_reenters_resize_without_self_lock() {
        let p = shared_pool(4, 2);
        let nested = Arc::new(Mutex::new(Vec::new()));
        let locks_free = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&p);
            let nested = Arc::clone(&nested);
            let locks_free = Arc::clone(&locks_free);
            PoolManaged::subscribe_audit(&*p, move |e| {
                if e.kind != PoolAuditKind::ResizeRejected {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                locks_free
                    .lock()
                    .expect("locks")
                    .push(inner.resize_locks_free());
                let result = inner.resize(MemoryTuning {
                    max: 4,
                    idle_cap: 2,
                });
                nested.lock().expect("nested results").push(result);
            })
        };

        let error = resize_bounded(
            &p,
            MemoryTuning {
                max: 0,
                idle_cap: 1,
            },
        )
        .unwrap_err();
        assert_eq!(error, PoolResizeError::ZeroMax);
        drop(ticket);

        let locks = locks_free.lock().expect("locks");
        assert_eq!(locks.len(), 1, "拒绝路径恰一次 ResizeRejected 回调");
        assert_eq!(
            locks[0],
            (true, true, true),
            "ResizeRejected 回调运行时 resizing/inner/core 均须空闲"
        );
        drop(locks);
        let nested = nested.lock().expect("nested results");
        assert!(
            nested[0].is_ok(),
            "拒绝事件在锁外发射 ⇒ 回调内 resize 必须成功：{nested:?}"
        );
        drop(nested);
        let m = p.metrics();
        assert_eq!((m.core.max, m.idle_cap), (4, 2), "拒绝路径零状态变更");
    }

    /// `Resized` 回调看到的是 base 边界 + inner 槽位的**同一已提交配置**，且能
    /// 重入 `acquire`/`metrics`（旧实现会看到 inner 旧值、锁被占用）。
    #[test]
    fn resized_callback_observes_committed_config_and_can_acquire() {
        let p = shared_pool(4, 2);
        // 预热两件闲槽（逐个借还只会复用同一件，闲槽到不了裁剪线）。
        let warm: Vec<_> = (0..2)
            .map(|_| p.acquire(Duration::ZERO).expect("warm slot"))
            .collect();
        drop(warm);
        assert_eq!(p.slot_snapshot(), (2, 2));

        let observed = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&p);
            let observed = Arc::clone(&observed);
            PoolManaged::subscribe_audit(&*p, move |e| {
                if e.kind != PoolAuditKind::Resized {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                let locks = inner.resize_locks_free();
                let m = inner.metrics();
                // 回调内借一件再还：必须能拿到已提交进槽位的件。
                let acquired = inner.acquire(Duration::ZERO).is_ok();
                observed.lock().expect("observed").push((
                    m.core.max,
                    m.idle_cap,
                    m.core.idle,
                    locks,
                    acquired,
                ));
            })
        };

        p.resize(MemoryTuning {
            max: 3,
            idle_cap: 1,
        })
        .expect("legal shrink");
        drop(ticket);

        let observed = observed.lock().expect("observed");
        assert_eq!(observed.len(), 1, "一次成功 resize 恰一次 Resized 回调");
        let (max, idle_cap, idle, locks, acquired) = observed[0];
        assert_eq!(
            (max, idle_cap),
            (3, 1),
            "回调必须看到 base 与 inner 同一已提交配置"
        );
        assert_eq!(idle, 1, "账本 idle 必须已是裁剪后的值（旧实现读到 2）");
        assert_eq!(
            locks,
            (true, true, true),
            "回调运行时 resizing/inner/core 均须空闲"
        );
        assert!(acquired, "回调内 acquire 必须拿到已提交的闲槽");
        drop(observed);
        let m = p.metrics();
        assert_eq!(
            (m.core.max, m.idle_cap, m.core.idle, m.core.borrowed),
            (3, 1, 1, 0)
        );
        assert_eq!(p.slot_snapshot(), (1, 1));
    }

    /// 并发 resize（`_serial` 串行化 base+inner 成对写）：每个回调读到的
    /// `(core.max, idle_cap)` 必须成对来自某次提交（禁撕裂），且最后发射的事件
    /// 描述最终提交配置。
    #[test]
    fn concurrent_resizes_never_expose_a_torn_config() {
        let p = shared_pool(4, 2);
        let observed = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&p);
            let observed = Arc::clone(&observed);
            PoolManaged::subscribe_audit(&*p, move |e| {
                if e.kind != PoolAuditKind::Resized {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                let locks = inner.resize_locks_free();
                let m = inner.metrics();
                observed
                    .lock()
                    .expect("observed")
                    .push(((m.core.max, m.idle_cap), locks));
            })
        };

        let spawn = |tuning: MemoryTuning| {
            let p = Arc::clone(&p);
            thread::spawn(move || p.resize(tuning).expect("legal tuning"))
        };
        let a = spawn(MemoryTuning {
            max: 6,
            idle_cap: 3,
        });
        let b = spawn(MemoryTuning {
            max: 5,
            idle_cap: 4,
        });
        a.join().expect("resize A");
        b.join().expect("resize B");
        drop(ticket);

        let observed = observed.lock().expect("observed");
        assert!(!observed.is_empty(), "并发 resize 至少一个提交者发事件");
        for ((max, idle_cap), locks) in observed.iter() {
            assert!(
                (*max, *idle_cap) == (6, 3) || (*max, *idle_cap) == (5, 4),
                "回调读到撕裂的 base/inner 配置：({max}, {idle_cap})"
            );
            assert_eq!(*locks, (true, true, true), "回调运行时不得持有内部锁");
        }
        let m = p.metrics();
        let last = observed.last().expect("非空");
        assert_eq!(
            last.0,
            (m.core.max, m.idle_cap),
            "最后发射的事件必须描述最终提交配置"
        );
    }
}
