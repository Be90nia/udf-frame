#![cfg(all(feature = "memgov", feature = "pool"))]
//! MemoryPool 内存池特化端到端：借还语义（RAII Drop 即还 / 获取超时必填 / 容量
//! 有界）+ resize（扩容即时唤醒 / 缩容只弃 idle）+ 审计事件全断言 + 稳态零分配
//! （性能矩阵①，counting allocator）。
//!
//! 分工：PoolBase 公共逻辑（校验/水位线/审计/溢出计数）已在 `tests/pool_base.rs`
//! 逐件覆盖——本文件只测**特化面**（槽位借还/等待/resize 联动/工厂 panic 恢复），
//! 与基座交汇处按端到端断言不复测基座内部。

use std::borrow::Borrow;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use skeleton::pool::{PoolAuditKind, PoolManaged, PoolResizeError};
use skeleton_infra::memgov::{CountingAllocator, MemoryPool, MemoryTuning};

/// 计数分配器挂全局（本二进制专用；同 tests/memgov.rs 纪律——lib 二进制不挂）。
#[global_allocator]
static COUNTING: CountingAllocator = CountingAllocator;

/// 稳态零分配断言的读数窗口隔离（同 tests/memgov.rs：libtest 主线程打印前序
/// 结果的分配窗与读数窗物理重叠——静止检测收敛真实排空时刻，不赌固定时长）。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    match SERIAL.lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    }
}

fn settle() {
    let mut last = CountingAllocator::live_allocs();
    for _ in 0..500 {
        thread::sleep(Duration::from_millis(1));
        let now = CountingAllocator::live_allocs();
        if now == last {
            return;
        }
        last = now;
    }
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

// ── 性能矩阵①：稳态借/还热路径零分配（counting allocator 断言=0）─────────

#[test]
fn steady_state_borrow_return_is_zero_alloc() {
    let _guard = serial();
    settle();

    // 稳态复用路径：预热一件，借/还各 1000 次
    let p = pool(2, 2);
    drop(p.acquire(Duration::from_millis(100)).unwrap()); // 预热：工厂/Vec 容量分配在窗外
    settle();
    let allocs_before = CountingAllocator::live_allocs();
    for _ in 0..1000 {
        drop(p.acquire(Duration::from_millis(100)).unwrap());
    }
    assert_eq!(
        CountingAllocator::live_allocs(),
        allocs_before,
        "稳态借/还热路径零分配（1000 次往返 alloc delta 必须为 0）"
    );

    // 溢出丢弃归还路径（丢弃+计数）：同样零分配。
    // 形态 (2,1)：每轮 借闲件+借新件 → 还第一件留存、还第二件溢出丢弃——
    // 单件自还永不判溢出（0+0<max 恒真），两件轮转才持续走到丢弃分支。
    let p = MemoryPool::<Vec<u8>>::new(2, 1, Vec::new);
    drop(p.acquire(Duration::from_millis(100)).unwrap()); // 预热 idle=1
    settle();
    let allocs_before = CountingAllocator::live_allocs();
    for _ in 0..1000 {
        let a = p.acquire(Duration::from_millis(100)).unwrap();
        let b = p.acquire(Duration::from_millis(100)).unwrap();
        drop(a); // 留存（idle 未满）
        drop(b); // 溢出丢弃（idle_cap=1 已满）
    }
    assert_eq!(
        CountingAllocator::live_allocs(),
        allocs_before,
        "溢出丢弃归还路径零分配"
    );
    assert!(
        p.metrics().core.dropped_overflow > 0,
        "溢出路径确实走到了丢弃分支"
    );
}
