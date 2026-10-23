#![cfg(feature = "pool")]
//! ThreadPool（动态线程池）端到端实弹测试：提交/执行/关闭生命周期、resize
//! 扩缩语义（扩容补建/缩容退出令牌优雅退）、拒绝策略四件套+自定义可插、
//! 审计事件流、任务 panic 不砖池——全部真实线程，轮询等待（禁裸 sleep 定时）。
//!
//! 锁风格：std::sync + 裸 unwrap 与仓内既有测试同款（本 crate 零依赖纪律，
//! parking_lot 不入树；产品面毒锁 fail-fast 收口 `lock()` helper）。

use std::borrow::Borrow;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use skeleton::pool::{PoolAuditKind, PoolManaged, PoolMetrics, PoolResizeError};
use skeleton_infra::pool::{
    CallerRunsPolicy, DiscardOldestPolicy, DiscardPolicy, PoolHandle, RejectPolicy, Task,
    ThreadPool, ThreadPoolTuning,
};

/// 轮询至条件成立（或超时 panic 附现场）——真实线程的异步过渡统一走这里，
/// 禁裸 sleep 定时断言。
fn wait_until(what: &str, deadline: Duration, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < deadline,
            "timeout waiting for {what} (elapsed {:?})",
            start.elapsed()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 门闩任务：阻塞到 release 被调（模拟长任务，确定性编排饱和态）。
#[derive(Clone)]
struct Gate {
    open: Arc<(Mutex<bool>, Condvar)>,
}

impl Gate {
    fn new() -> (Self, impl Fn() + Send + Sync + 'static) {
        let open = Arc::new((Mutex::new(false), Condvar::new()));
        let g = Self {
            open: Arc::clone(&open),
        };
        let release = move || {
            let (m, c) = &*open;
            *m.lock().unwrap() = true;
            c.notify_all();
        };
        (g, release)
    }

    /// 任务体：等放行（测试负载；测试自身路径允许裸 unwrap）。
    fn blocker(&self) -> impl FnOnce() + Send + 'static {
        let open = Arc::clone(&self.open);
        move || {
            let (m, c) = &*open;
            let mut o = m.lock().unwrap();
            while !*o {
                o = c.wait(o).unwrap();
            }
        }
    }
}

fn tuning(core: usize, max: usize, queue_cap: usize) -> ThreadPoolTuning {
    ThreadPoolTuning::new(core, max, queue_cap)
}

// ── 构造期事前校验：恒不合法配置 fail-fast ──────────────────────────────

#[test]
#[should_panic(expected = "core")]
fn new_rejects_zero_core() {
    let _ = ThreadPool::new(tuning(0, 4, 8));
}

#[test]
#[should_panic(expected = "max")]
fn new_rejects_zero_max() {
    let _ = ThreadPool::new(tuning(1, 0, 8));
}

#[test]
#[should_panic(expected = "core")]
fn new_rejects_core_above_max() {
    let _ = ThreadPool::new(tuning(5, 4, 8));
}

#[test]
#[should_panic(expected = "queue cap")]
fn new_rejects_zero_queue_cap() {
    let _ = ThreadPool::new(tuning(1, 4, 0));
}

// ── 生命周期：提交/执行/关闭排空/join ───────────────────────────────────

#[test]
fn submit_execute_join_lifecycle() {
    let pool = ThreadPool::new(tuning(2, 2, 16));
    let n = Arc::new(AtomicUsize::new(0));
    for _ in 0..16 {
        let n = Arc::clone(&n);
        pool.execute(move || {
            n.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    pool.shutdown();
    pool.join();
    let m = pool.metrics();
    assert_eq!(m.workers, 0, "join 后 worker 全退");
    assert_eq!(m.completed, 16);
    assert_eq!(n.load(Ordering::Relaxed), 16);
    pool.join(); // join 幂等：二次 join 不挂不炸
}

#[test]
fn shutdown_drains_queued_tasks() {
    let pool = ThreadPool::new(tuning(1, 1, 32));
    let n = Arc::new(AtomicUsize::new(0));
    for _ in 0..32 {
        let n = Arc::clone(&n);
        pool.execute(move || {
            n.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap();
    }
    pool.shutdown();
    pool.join();
    assert_eq!(n.load(Ordering::Relaxed), 32, "shutdown 必须排空已入队任务");
}

#[test]
fn execute_after_shutdown_is_rejected() {
    let pool = ThreadPool::new(tuning(1, 1, 4));
    pool.shutdown();
    pool.join();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    let err = pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    });
    assert!(err.is_err(), "shutdown 后提交必须被拒");
    assert_eq!(pool.metrics().rejected, 1);
    assert_eq!(ran.load(Ordering::Relaxed), 0, "被拒任务不得执行");
}

#[test]
fn drop_initiates_drain_and_workers_outlive_pool() {
    let ran = Arc::new(AtomicUsize::new(0));
    {
        let pool = ThreadPool::new(tuning(1, 1, 8));
        for _ in 0..4 {
            let r = Arc::clone(&ran);
            pool.execute(move || {
                r.fetch_add(1, Ordering::Relaxed);
            })
            .unwrap();
        }
        // 未显式 shutdown 直接 drop：worker 持 Shared Arc 存活排空，不悬挂不 UB。
    }
    wait_until("drop-drain completes", Duration::from_secs(5), || {
        ran.load(Ordering::Relaxed) == 4
    });
}

// ── resize：扩容补建 / 缩容退出令牌优雅退 / 拒绝不改状态 ────────────────

#[test]
fn resize_rejects_illegal_tuning_without_state_change() {
    let pool = ThreadPool::new(tuning(2, 4, 8));
    let before = pool.metrics();
    assert_eq!(
        pool.resize(tuning(0, 4, 8)),
        Err(PoolResizeError::IdleCapZero)
    );
    assert_eq!(
        pool.resize(tuning(5, 4, 8)),
        Err(PoolResizeError::IdleCapExceedsMax {
            idle_cap: 5,
            max: 4
        })
    );
    assert_eq!(pool.resize(tuning(2, 0, 8)), Err(PoolResizeError::ZeroMax));
    assert_eq!(
        pool.resize(tuning(2, 4, 0)),
        Err(PoolResizeError::IdleCapZero)
    );
    let after = pool.metrics();
    assert_eq!(
        (before.core, before.workers),
        (after.core, after.workers),
        "拒绝不改状态"
    );
}

#[test]
fn resize_up_spawns_workers_immediately() {
    let pool = ThreadPool::new(tuning(1, 8, 8));
    assert_eq!(pool.metrics().workers, 1, "构造即建 core 保活层");
    pool.resize(tuning(4, 8, 8)).unwrap();
    assert_eq!(
        pool.metrics().workers,
        4,
        "扩容补建：resize 返回即到新 core"
    );
}

#[test]
fn resize_down_retires_idle_workers_via_exit_tokens() {
    let pool = ThreadPool::new(tuning(4, 4, 8));
    wait_until("initial workers", Duration::from_secs(5), || {
        pool.metrics().workers == 4
    });
    pool.resize(tuning(1, 4, 8)).unwrap();
    // 优雅退：差额 worker 收退出令牌后退役（异步生效，轮询等待）。
    wait_until("shrink to 1 worker", Duration::from_secs(5), || {
        pool.metrics().workers == 1
    });
}

#[test]
fn shrink_lets_busy_worker_finish_current_task() {
    let pool = ThreadPool::new(tuning(2, 2, 8));
    wait_until("workers up", Duration::from_secs(5), || {
        pool.metrics().workers == 2
    });
    let (gate, release) = Gate::new();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        gate.blocker()();
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    wait_until("task picked up", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    // worker 忙碌中缩容：当前任务必须跑完（处理完当前任务即退，禁弃任务）。
    pool.resize(tuning(1, 2, 8)).unwrap();
    release();
    pool.shutdown();
    pool.join();
    assert_eq!(ran.load(Ordering::Relaxed), 1, "缩容不弃执行中任务");
}

#[test]
fn resize_down_then_up_cancels_pending_shrink() {
    let pool = ThreadPool::new(tuning(4, 4, 8));
    wait_until("workers up", Duration::from_secs(5), || {
        pool.metrics().workers == 4
    });
    pool.resize(tuning(1, 4, 8)).unwrap();
    // 未等令牌消费完立刻回扩：待退令牌重算（清零），不发生过冲退役。
    pool.resize(tuning(4, 4, 8)).unwrap();
    wait_until("workers back to 4", Duration::from_secs(5), || {
        pool.metrics().workers == 4
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(pool.metrics().workers, 4, "回扩后不得有 worker 退役");
}

// ── 拒绝策略四件套 + 自定义可插 ─────────────────────────────────────────

/// 饱和池：1 worker 忙于门闩，队列 cap=1 已填满（默认 Abort 策略）。
fn saturated_pool() -> (ThreadPool, impl Fn() + Send + Sync + 'static) {
    let pool = ThreadPool::new(tuning(1, 1, 1));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until("worker busy", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    pool.execute(|| {}).unwrap(); // 队列 1/1 满
    (pool, release)
}

#[test]
fn abort_policy_hands_task_back() {
    let (pool, release) = saturated_pool();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    let err = pool
        .execute(move || {
            r.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap_err();
    assert_eq!(pool.metrics().rejected, 1);
    release();
    pool.shutdown();
    pool.join();
    // 被退回的任务完好：调用方自行执行仍可运行。
    assert_eq!(ran.load(Ordering::Relaxed), 0);
    err.into_task()();
    assert_eq!(ran.load(Ordering::Relaxed), 1);
}

#[test]
fn caller_runs_policy_executes_inline() {
    let pool = ThreadPool::with_policy(tuning(1, 1, 1), Arc::new(CallerRunsPolicy));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until("worker busy", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    pool.execute(|| {}).unwrap(); // 队列 1/1 满
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    // 提交时 worker 仍被门闩堵着：probe 若被执行只能发生在提交线程内联。
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    assert_eq!(
        ran.load(Ordering::Relaxed),
        1,
        "CallerRuns 在提交线程内联执行"
    );
    assert_eq!(pool.metrics().rejected, 1, "策略触发即计数");
    release();
    pool.shutdown();
    pool.join();
}

#[test]
fn discard_policy_drops_and_counts() {
    let pool = ThreadPool::with_policy(tuning(1, 1, 1), Arc::new(DiscardPolicy));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until("worker busy", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    pool.execute(|| {}).unwrap(); // 队列满
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    release();
    pool.shutdown();
    pool.join();
    assert_eq!(ran.load(Ordering::Relaxed), 0, "Discard 静默丢弃不入队");
    assert_eq!(pool.metrics().rejected, 1);
}

#[test]
fn discard_oldest_policy_drops_head_keeps_newest() {
    let pool = ThreadPool::with_policy(tuning(1, 1, 2), Arc::new(DiscardOldestPolicy));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until("worker busy", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    let tag = Arc::new(AtomicUsize::new(0));
    let t1 = Arc::clone(&tag);
    pool.execute(move || {
        t1.fetch_or(1, Ordering::Relaxed);
    })
    .unwrap();
    let t2 = Arc::clone(&tag);
    pool.execute(move || {
        t2.fetch_or(2, Ordering::Relaxed);
    })
    .unwrap();
    let t3 = Arc::clone(&tag);
    pool.execute(move || {
        t3.fetch_or(4, Ordering::Relaxed);
    })
    .unwrap();
    release();
    pool.shutdown();
    pool.join();
    assert_eq!(
        tag.load(Ordering::Relaxed),
        0b110,
        "最旧的 t1 被弃，t2/t3 执行"
    );
}

#[test]
fn custom_policy_is_pluggable() {
    struct Record(Arc<AtomicUsize>);
    impl RejectPolicy for Record {
        fn on_reject(&self, _task: Task, _pool: &PoolHandle) -> Result<(), Task> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = ThreadPool::with_policy(tuning(1, 1, 1), Arc::new(Record(Arc::clone(&calls))));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until("worker busy", Duration::from_secs(5), || {
        pool.metrics().busy == 1
    });
    pool.execute(|| {}).unwrap(); // 队列满
    pool.execute(|| {}).unwrap(); // 触发自定义策略
    assert_eq!(calls.load(Ordering::Relaxed), 1, "自定义策略被调");
    assert_eq!(pool.metrics().rejected, 1);
    release();
    pool.shutdown();
    pool.join();
}

// ── 指标 + PoolManaged 契约面 + 审计流 ──────────────────────────────────

#[test]
fn metrics_track_lifecycle_readouts() {
    let pool = ThreadPool::new(tuning(2, 4, 8));
    wait_until("core workers parked", Duration::from_secs(5), || {
        let m = pool.metrics();
        m.workers == 2 && m.idle == 2
    });
    let m = pool.metrics();
    assert_eq!((m.core, m.idle, m.busy, m.queue_len), (2, 2, 0, 0));
    // 双 worker 全堵门闩（确定性饱和），第三件必排队——禁假设单任务调度交错。
    let (g1, r1) = Gate::new();
    let (g2, r2) = Gate::new();
    pool.execute(g1.blocker()).unwrap();
    pool.execute(g2.blocker()).unwrap();
    wait_until("both busy", Duration::from_secs(5), || {
        pool.metrics().busy == 2
    });
    pool.execute(|| {}).unwrap();
    let m = pool.metrics();
    assert_eq!((m.busy, m.queue_len, m.idle), (2, 1, 0));
    r1();
    r2();
    pool.shutdown();
    pool.join();
    let m = pool.metrics();
    assert_eq!((m.completed, m.busy, m.queue_len), (3, 0, 0));
    // 水位线（借出率）= busy/max，Meituan 活跃度公式（核形字段走 Borrow 面）。
    let shape: &PoolMetrics = m.borrow();
    assert!(shape.watermark >= 0.0 && shape.watermark <= 1.0);
}

#[test]
fn poolmanaged_trait_core_shape_via_borrow() {
    // 契约可泛型消费：任何 PoolManaged 都能被宿主代码零特化驱动。
    fn resize_and_read<P: PoolManaged>(
        pool: &P,
        t: P::Tuning,
    ) -> Result<P::Metrics, PoolResizeError> {
        pool.resize(t)?;
        Ok(pool.metrics())
    }
    let pool = ThreadPool::new(tuning(2, 4, 8));
    let m = resize_and_read(&pool, tuning(3, 4, 8)).unwrap();
    assert_eq!(m.core, 3);
    // idle 随真实 park 异步生效（workers 计数同步、parking 异步），轮询等待。
    wait_until("workers parked to core", Duration::from_secs(5), || {
        pool.metrics().idle == 3
    });
    let m = pool.metrics();
    let core: &PoolMetrics = m.borrow();
    assert_eq!(core.max, 4);
    assert_eq!(core.idle, 3);
    assert_eq!(core.borrowed, 0);
    assert_eq!(core.dropped_overflow, 0);
    pool.shutdown();
    pool.join();
}

#[test]
fn audit_stream_records_resize_and_closes_on_ticket_drop() {
    let pool = ThreadPool::new(tuning(1, 4, 8));
    {
        let ticket = pool.subscribe_audit(|_| {});
        pool.resize(tuning(2, 4, 8)).unwrap();
        pool.resize(tuning(0, 2, 8)).unwrap_err(); // 拒绝也留痕
        let events = pool.take_audit_events();
        let kinds: Vec<_> = events.iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            [PoolAuditKind::Resized, PoolAuditKind::ResizeRejected]
        );
        for w in events.windows(2) {
            assert!(w[0].seq < w[1].seq, "seq 单调");
        }
        drop(ticket); // 票据 Drop = 关流
    }
    pool.resize(tuning(3, 4, 8)).unwrap();
    assert!(pool.take_audit_events().is_empty(), "关流后不再记录");
    pool.shutdown();
    pool.join();
}

#[test]
fn task_panic_does_not_brick_pool() {
    let pool = ThreadPool::new(tuning(1, 1, 8));
    pool.execute(|| panic!("boom")).unwrap();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    pool.shutdown();
    pool.join();
    assert_eq!(
        ran.load(Ordering::Relaxed),
        1,
        "panic 后 worker 存活接续后续任务"
    );
    assert_eq!(pool.metrics().task_panics, 1, "panic 摘除必留痕");
    assert_eq!(
        pool.metrics().completed,
        1,
        "panic 任务不计完成账（另册 task_panics）"
    );
}

// ── 并发实弹 ────────────────────────────────────────────────────────────

#[test]
fn concurrent_submitters_stress() {
    let pool = ThreadPool::new(tuning(4, 4, 1024));
    let n = Arc::new(AtomicUsize::new(0));
    std::thread::scope(|s| {
        for _ in 0..4 {
            let n = Arc::clone(&n);
            let p = &pool;
            s.spawn(move || {
                for _ in 0..250 {
                    let n = Arc::clone(&n);
                    p.execute(move || {
                        n.fetch_add(1, Ordering::Relaxed);
                    })
                    .unwrap();
                }
            });
        }
    });
    pool.shutdown();
    pool.join();
    assert_eq!(n.load(Ordering::Relaxed), 1000);
    assert_eq!(pool.metrics().completed, 1000);
}

// ── 读侧收口（Wave3）：快照等值比较 + 共享核心形公开直达 ────────────────

#[test]
fn metrics_read_side_shape() {
    // derive 面与 MemoryPoolMetrics 同一纪律（快照整体等值比较——拒绝路径
    // 「零状态变更」断言的统一写法）；shape 公开直达与 pub core 同一形态。
    let pool = ThreadPool::new(tuning(1, 2, 8));
    let m = pool.metrics();
    assert_eq!(m, m, "快照 Copy 自反相等（PartialEq derive 不回归）");
    assert_eq!(m.shape.borrowed, m.busy, "核心形映射：borrowed=busy");
    assert_eq!(
        m.shape.dropped_overflow, m.rejected,
        "核心形映射：溢出=rejected"
    );
    pool.shutdown();
    pool.join();
}
