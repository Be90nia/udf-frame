#![cfg(feature = "pool")]
//! ThreadPool（动态线程池）端到端实弹测试：提交/执行/关闭生命周期、resize
//! 扩缩语义（扩容补建/缩容退出令牌优雅退）、拒绝策略四件套+自定义可插、
//! 审计事件流、任务 panic 不砖池——全部真实线程，轮询等待（禁裸 sleep 定时）。
//!
//! 锁风格：std::sync + 裸 unwrap 与仓内既有测试同款（本 crate 零依赖纪律，
//! parking_lot 不入树；产品面毒锁 fail-fast 收口 `lock()` helper）。

use std::borrow::Borrow;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use skeleton::pool::{PoolAuditKind, PoolManaged, PoolMetrics, PoolResizeError};
use skeleton_infra::pool::{
    AbortPolicy, CallerRunsPolicy, DiscardOldestPolicy, DiscardPolicy, PoolHandle, RejectCause,
    RejectPolicy, SpawnGate, Task, ThreadPool, ThreadPoolTuning, ThreadSpawnError,
};
use std::sync::mpsc;

/// 有界等待 + 现场诊断：超时 panic 并打印当时的快照（直接看出卡在什么状态）。
///
/// 真实线程的异步过渡统一走这里：**禁无界等待**（不变量被破坏时无界等待 =
/// 挂死，CI 上表现为 job 超时，比一条红更难诊断）；**禁固定 sleep + 断言**
/// （概率性）。
fn wait_until_diag(
    what: &str,
    deadline: Duration,
    mut f: impl FnMut() -> bool,
    snapshot: impl Fn() -> String,
) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < deadline,
            "timeout waiting for {what} (elapsed {:?})\n  现场快照：{}",
            start.elapsed(),
            snapshot()
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 有界 join：先有界轮询「存活 worker 归零」（超时 panic 并打印 metrics 快照
/// ——直接看出 workers 卡在几），再调 [`ThreadPool::join`]（此时必然立即返回）。
///
/// `join()` 本身是阻塞 API：若存活计数含幻影 worker（计数 > 实际存活），它会
/// 永久等待 ⇒ 测试变挂死而非变红。本辅助把该情形转成**带诊断的红**。
fn join_bounded<G: SpawnGate>(pool: &ThreadPool<G>, what: &str) {
    wait_until_diag(
        &format!("{what}：存活 worker 归零"),
        Duration::from_secs(5),
        || pool.metrics().workers == 0,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.join();
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
    join_bounded(&pool, "线程池停机排空");
    let m = pool.metrics();
    assert_eq!(m.workers, 0, "join 后 worker 全退");
    assert_eq!(m.completed, 16);
    assert_eq!(n.load(Ordering::Relaxed), 16);
    join_bounded(&pool, "线程池停机排空"); // join 幂等：二次 join 不挂不炸
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
    join_bounded(&pool, "线程池停机排空");
    assert_eq!(n.load(Ordering::Relaxed), 32, "shutdown 必须排空已入队任务");
}

#[test]
fn execute_after_shutdown_is_rejected() {
    let pool = ThreadPool::new(tuning(1, 1, 4));
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
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
    wait_until_diag(
        "drop-drain completes",
        Duration::from_secs(5),
        || ran.load(Ordering::Relaxed) == 4,
        || format!("已执行任务数={}", ran.load(Ordering::Relaxed)),
    );
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
    wait_until_diag(
        "initial workers",
        Duration::from_secs(5),
        || pool.metrics().workers == 4,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.resize(tuning(1, 4, 8)).unwrap();
    // 优雅退：差额 worker 收退出令牌后退役（异步生效，轮询等待）。
    wait_until_diag(
        "shrink to 1 worker",
        Duration::from_secs(5),
        || pool.metrics().workers == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
}

#[test]
fn shrink_lets_busy_worker_finish_current_task() {
    let pool = ThreadPool::new(tuning(2, 2, 8));
    wait_until_diag(
        "workers up",
        Duration::from_secs(5),
        || pool.metrics().workers == 2,
        || format!("metrics={:?}", pool.metrics()),
    );
    let (gate, release) = Gate::new();
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        gate.blocker()();
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    wait_until_diag(
        "task picked up",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    // worker 忙碌中缩容：当前任务必须跑完（处理完当前任务即退，禁弃任务）。
    pool.resize(tuning(1, 2, 8)).unwrap();
    release();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
    assert_eq!(ran.load(Ordering::Relaxed), 1, "缩容不弃执行中任务");
}

#[test]
fn resize_down_then_up_cancels_pending_shrink() {
    let pool = ThreadPool::new(tuning(4, 4, 8));
    wait_until_diag(
        "workers up",
        Duration::from_secs(5),
        || pool.metrics().workers == 4,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.resize(tuning(1, 4, 8)).unwrap();
    // 未等令牌消费完立刻回扩：待退令牌重算（清零），不发生过冲退役。
    pool.resize(tuning(4, 4, 8)).unwrap();
    wait_until_diag(
        "workers back to 4",
        Duration::from_secs(5),
        || pool.metrics().workers == 4,
        || format!("metrics={:?}", pool.metrics()),
    );
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(pool.metrics().workers, 4, "回扩后不得有 worker 退役");
}

// ── 拒绝策略四件套 + 自定义可插 ─────────────────────────────────────────

/// 饱和池：1 worker 忙于门闩，队列 cap=1 已填满（默认 Abort 策略）。
fn saturated_pool() -> (ThreadPool, impl Fn() + Send + Sync + 'static) {
    let pool = ThreadPool::new(tuning(1, 1, 1));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
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
    join_bounded(&pool, "线程池停机排空");
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
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
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
    join_bounded(&pool, "线程池停机排空");
}

#[test]
fn discard_policy_drops_and_counts() {
    let pool = ThreadPool::with_policy(tuning(1, 1, 1), Arc::new(DiscardPolicy));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap(); // 队列满
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    release();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
    assert_eq!(ran.load(Ordering::Relaxed), 0, "Discard 静默丢弃不入队");
    assert_eq!(pool.metrics().rejected, 1);
}

#[test]
fn discard_oldest_policy_drops_head_keeps_newest() {
    let pool = ThreadPool::with_policy(tuning(1, 1, 2), Arc::new(DiscardOldestPolicy));
    let (gate, release) = Gate::new();
    pool.execute(gate.blocker()).unwrap();
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
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
    join_bounded(&pool, "线程池停机排空");
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
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap(); // 队列满
    pool.execute(|| {}).unwrap(); // 触发自定义策略
    assert_eq!(calls.load(Ordering::Relaxed), 1, "自定义策略被调");
    assert_eq!(pool.metrics().rejected, 1);
    release();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
}

// ── 指标 + PoolManaged 契约面 + 审计流 ──────────────────────────────────

#[test]
fn metrics_track_lifecycle_readouts() {
    let pool = ThreadPool::new(tuning(2, 4, 8));
    wait_until_diag(
        "core workers parked",
        Duration::from_secs(5),
        || {
            let m = pool.metrics();
            m.workers == 2 && m.idle == 2
        },
        || format!("metrics={:?}", pool.metrics()),
    );
    let m = pool.metrics();
    assert_eq!((m.core, m.idle, m.busy, m.queue_len), (2, 2, 0, 0));
    // 双 worker 全堵门闩（确定性饱和），第三件必排队——禁假设单任务调度交错。
    let (g1, r1) = Gate::new();
    let (g2, r2) = Gate::new();
    pool.execute(g1.blocker()).unwrap();
    pool.execute(g2.blocker()).unwrap();
    wait_until_diag(
        "both busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 2,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap();
    let m = pool.metrics();
    assert_eq!((m.busy, m.queue_len, m.idle), (2, 1, 0));
    r1();
    r2();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
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
    wait_until_diag(
        "workers parked to core",
        Duration::from_secs(5),
        || pool.metrics().idle == 3,
        || format!("metrics={:?}", pool.metrics()),
    );
    let m = pool.metrics();
    let core: &PoolMetrics = m.borrow();
    assert_eq!(core.max, 4);
    assert_eq!(core.idle, 3);
    assert_eq!(core.borrowed, 0);
    assert_eq!(core.dropped_overflow, 0);
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
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
    join_bounded(&pool, "线程池停机排空");
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
    join_bounded(&pool, "线程池停机排空");
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
    join_bounded(&pool, "线程池停机排空");
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
    join_bounded(&pool, "线程池停机排空");
}

// ── worker 创建失败：typed 错误 + 计数一致 + join 不挂 ─────────────────────
//
// 失败注入闸（[`SpawnGate`]）：第 `fail_at` 次 worker 创建失败（序号从 1 计）。
// 闸门归池所有，测试经 Arc 读数句柄观测尝试次数与失败次数。

/// 确定性创建失败注入闸（第 N 次创建失败）。
struct FailNthSpawn {
    fail_at: usize,
    seen: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}

impl FailNthSpawn {
    /// 建闸（`fail_at` = 第几次创建失败；1 = 首次即失败）。返回闸门 + 读数句柄。
    fn new(fail_at: usize) -> (Self, Arc<AtomicUsize>, Arc<AtomicUsize>) {
        let seen = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));
        (
            Self {
                fail_at,
                seen: Arc::clone(&seen),
                failed: Arc::clone(&failed),
            },
            seen,
            failed,
        )
    }
}

impl SpawnGate for FailNthSpawn {
    fn check_spawn(&self, _worker_id: u64) -> Result<(), ThreadSpawnError> {
        let n = self.seen.fetch_add(1, Ordering::SeqCst) + 1;
        if n == self.fail_at {
            self.failed.fetch_add(1, Ordering::SeqCst);
            Err(ThreadSpawnError::from(io::Error::other(
                "injected worker spawn failure",
            )))
        } else {
            Ok(())
        }
    }
}

/// 建带注入闸的池（首 worker 创建必成功；失败即测试前置条件破裂）。
fn pool_with_gate(
    t: ThreadPoolTuning,
    gate: FailNthSpawn,
) -> Result<ThreadPool<FailNthSpawn>, ThreadSpawnError> {
    ThreadPool::<FailNthSpawn>::try_with_gate(t, Arc::new(AbortPolicy), gate)
}

#[test]
fn spawn_failure_at_construction_returns_typed_error() {
    let (gate, seen, failed) = FailNthSpawn::new(1);
    let built = pool_with_gate(tuning(1, 2, 8), gate);
    let err = match built {
        Ok(_) => panic!("首 worker 创建失败必须返回 typed 错误（禁 panic）"),
        Err(e) => e,
    };
    assert!(
        matches!(err.io_error().kind(), io::ErrorKind::Other),
        "typed 现场保留底层 OS 错误面"
    );
    assert_eq!(seen.load(Ordering::SeqCst), 1, "首 worker 创建即失败");
    assert_eq!(failed.load(Ordering::SeqCst), 1);
}

#[test]
fn resize_partial_spawn_failure_keeps_pool_valid() {
    let (gate, seen, failed) = FailNthSpawn::new(2); // 构造第 1 次成功；resize 补建首个失败
    let pool = match pool_with_gate(tuning(1, 3, 8), gate) {
        Ok(p) => p,
        Err(e) => panic!("首 worker 创建应成功: {e}"),
    };
    assert_eq!(pool.metrics().workers, 1, "构造即建 core 保活层");

    // 补建 2 个：首个失败 → 停止补建 → **typed Err**（部分生效状态随错误给出）。
    let err = pool.resize(tuning(3, 3, 8)).unwrap_err();
    assert_eq!(
        err,
        PoolResizeError::SpawnFailed {
            kind: io::ErrorKind::Other,
            target_core: 3,
            workers: 1,
        },
        "resize 部分生效：错误对象自带目标 core 与实际存活 worker 数"
    );
    assert!(
        err.to_string().contains("partially applied"),
        "错误文案自述部分生效: {err}"
    );

    let m = pool.metrics();
    assert_eq!(m.workers, 1, "计数 = 实际存活 worker（幻影已回滚）");
    assert_eq!(m.spawn_failures, 1, "创建失败留痕（fail-visible）");
    assert_eq!(failed.load(Ordering::SeqCst), 1);
    assert_eq!(seen.load(Ordering::SeqCst), 2, "失败后不再尝试补建");

    // 池仍是有效池：任务照跑、排空、停机。
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
    assert_eq!(ran.load(Ordering::Relaxed), 1, "部分失败后池仍可服务");
}

#[test]
fn resize_spawn_failure_reports_partial_effect_state() {
    // 构造 1 个 worker；resize 到 core=3 需补建 2 个：第 1 个建成、第 2 个失败
    // → 池处于「部分生效」：已建成的 worker 仍存活，错误对象给出缺口面。
    let (gate, seen, failed) = FailNthSpawn::new(3);
    let pool = match pool_with_gate(tuning(1, 3, 8), gate) {
        Ok(p) => p,
        Err(e) => panic!("首 worker 创建应成功: {e}"),
    };
    let err = pool.resize(tuning(3, 3, 8)).unwrap_err();
    assert_eq!(
        err,
        PoolResizeError::SpawnFailed {
            kind: io::ErrorKind::Other,
            target_core: 3,
            workers: 2,
        },
        "部分生效：补建第 1 个已落地（workers=2），第 2 个失败 → 缺口 = target_core - workers"
    );
    assert_eq!(
        pool.metrics().workers,
        2,
        "计数 = 实际存活 worker（已建成的仍存活）"
    );
    assert_eq!(pool.metrics().spawn_failures, 1);
    assert_eq!(failed.load(Ordering::SeqCst), 1);
    assert_eq!(seen.load(Ordering::SeqCst), 3, "失败后不再尝试补建");

    // 部分生效的池仍可服务；join 能返回（无幻影 worker 等待）。
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    pool.execute(move || {
        r.fetch_add(1, Ordering::Relaxed);
    })
    .unwrap();
    let start = Instant::now();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
    assert!(start.elapsed() < Duration::from_secs(5), "join 必返回");
    assert_eq!(ran.load(Ordering::Relaxed), 1);
    assert_eq!(pool.metrics().workers, 0);
}

#[test]
fn submit_spawn_failure_returns_task_with_typed_cause() {
    let (gate, _seen, failed) = FailNthSpawn::new(2); // 构造成功；提交触发的创建失败
    let pool = match pool_with_gate(tuning(1, 2, 1), gate) {
        Ok(p) => p,
        Err(e) => panic!("首 worker 创建应成功: {e}"),
    };
    let (gate_h, release) = Gate::new();
    pool.execute(gate_h.blocker()).unwrap(); // worker 忙
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap(); // 队列 1/1 满

    // 本次提交：队列满 + workers(1) < max(2) → 需新建 worker → 创建失败。
    let ran = Arc::new(AtomicUsize::new(0));
    let r = Arc::clone(&ran);
    let err = pool
        .execute(move || {
            r.fetch_add(1, Ordering::Relaxed);
        })
        .unwrap_err();
    assert!(
        matches!(err.cause(), RejectCause::SpawnFailed(_)),
        "成因必须是 worker 创建失败（与策略拒绝分账）"
    );
    assert!(err.spawn_error().is_some(), "typed 现场随任务交还");
    let m = pool.metrics();
    assert_eq!(m.workers, 1, "幻影 worker 已回滚（计数 = 实际存活）");
    assert_eq!(m.spawn_failures, 1);
    assert_eq!(m.rejected, 0, "创建失败不是策略拒绝（两账分开）");
    assert_eq!(failed.load(Ordering::SeqCst), 1);

    // 任务原样交还调用方（失败路径禁吞任务）。
    err.into_task()();
    assert_eq!(ran.load(Ordering::Relaxed), 1);

    release();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
}

#[test]
fn join_returns_after_partial_spawn_failure() {
    let (gate, _seen, _failed) = FailNthSpawn::new(2);
    let pool = match pool_with_gate(tuning(1, 2, 8), gate) {
        Ok(p) => p,
        Err(e) => panic!("首 worker 创建应成功: {e}"),
    };
    // 补建失败 → typed Err（部分生效：目标 core 2 / 实际存活 1）。
    assert_eq!(
        pool.resize(tuning(2, 2, 8)).unwrap_err(),
        PoolResizeError::SpawnFailed {
            kind: io::ErrorKind::Other,
            target_core: 2,
            workers: 1,
        }
    );
    let start = Instant::now();
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空"); // 禁等幻影 worker 永久挂起
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "join 必须在有限时间内返回（elapsed {:?}）",
        start.elapsed()
    );
    assert_eq!(pool.metrics().workers, 0, "join 后 worker 全退");
}

#[test]
fn worker_count_matches_live_workers_after_spawn_failures() {
    let (gate, seen, failed) = FailNthSpawn::new(3); // 构造 2 次成功；resize 第 3 次失败
    let pool = match pool_with_gate(tuning(2, 4, 8), gate) {
        Ok(p) => p,
        Err(e) => panic!("首 worker 创建应成功: {e}"),
    };
    assert_eq!(pool.metrics().workers, 2);
    // 补建：第 3 次失败 → 停止 → typed Err（部分生效：目标 core 4 / 存活 2）。
    assert_eq!(
        pool.resize(tuning(4, 4, 8)).unwrap_err(),
        PoolResizeError::SpawnFailed {
            kind: io::ErrorKind::Other,
            target_core: 4,
            workers: 2,
        }
    );

    let m = pool.metrics();
    let created = seen.load(Ordering::SeqCst) - failed.load(Ordering::SeqCst);
    assert_eq!(
        m.workers, created,
        "计数 = 实际成功创建的 worker 数（禁幻影）"
    );
    assert_eq!(m.spawn_failures, 1);
    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
}
// ── issue#34（bd udf-frame-5zv）：spawn 回滚残留 stale shrinking token 把
//    core worker 退役 ─────────────────────────────────────────────────────
//
// 根因：席位预约发布进 `workers` 口径，并发 resize 的缩容差额按 `workers`
// 计算会把「尚未创建成功的在途席位」误当可退役 worker 发令牌；spawn 失败
// 回滚席位后令牌不回收，唯一的 real worker 完成任务即认领退出 → workers
// 跌破 core、队列任务无人接。修复面：真实存活与在途席位分册 + 缩容差额只
// 按真实存活计 + 退出守卫（真实存活不得跌破 core）。

/// 阻塞闸（issue#34 复现专用）：`check_spawn` 先通知「已入闸」（此刻席位已
/// 预约发布），再等测试放行，然后注入创建失败——给并发 resize 留出确定的
/// 交错窗口（修复前 shrinking 恰在此窗口内误发）。构造期保活层创建（id=0）
/// 直接放行，否则池构造本身就死锁在闸内。
struct BlockingFailGate {
    entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl SpawnGate for BlockingFailGate {
    fn check_spawn(&self, worker_id: u64) -> Result<(), ThreadSpawnError> {
        if worker_id == 0 {
            // 构造期保活层创建：放行（本闸只拦测试编排触发的扩容/补建）。
            return Ok(());
        }
        self.entered
            .send(())
            .expect("测试线程存活（recv 有界兜底）");
        self.release
            .lock()
            .unwrap()
            .recv()
            .expect("测试线程放行信号");
        Err(ThreadSpawnError::from(io::Error::other(
            "injected worker spawn failure (issue#34)",
        )))
    }
}

/// issue#34 主交错（rollback_spawn 路径）：占忙 + 队列满 → 提交触发预约第 2
/// 席位 → 闸内悬停 → 并发 resize（原参数）→ 放行闸门 spawn 失败 → 席位回滚
/// 后 stale 令牌不得退役唯一的 real worker；队列任务必须被继续完成。
#[test]
fn spawn_rollback_must_not_retire_core_workers_via_stale_shrink_token() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate = BlockingFailGate {
        entered: entered_tx,
        release: Mutex::new(release_rx),
    };
    let pool = Arc::new(
        match ThreadPool::<BlockingFailGate>::try_with_gate(
            tuning(1, 2, 1),
            Arc::new(AbortPolicy),
            gate,
        ) {
            Ok(p) => p,
            Err(e) => panic!("首 worker 创建应成功: {e}"),
        },
    );

    // ① 唯一 real worker 占忙；② 队列填满（cap=1）。
    let (busy_gate, release_busy) = Gate::new();
    pool.execute(busy_gate.blocker()).unwrap();
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap();
    assert_eq!(pool.metrics().queue_len, 1, "前置：队列满");

    // ③ 独立线程提交：队列满 + workers(1) < max(2) → 预约第 2 席位 → 卡闸。
    let submitter = {
        let pool = Arc::clone(&pool);
        std::thread::spawn(move || pool.execute(|| {}))
    };
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("spawn 触发提交未进入闸门（席位预约未发布）");

    // ④ 并发 resize（原参数）：修复前 workers 口径含在途席位 → shrinking 误发 1。
    pool.resize(tuning(1, 2, 1)).unwrap();

    // ⑤ 放行闸门 → 注入失败 → rollback_spawn 撤销席位。
    release_tx.send(()).expect("gate 线程存活");
    let submitted = submitter.join().unwrap().unwrap_err();
    assert!(
        matches!(submitted.cause(), RejectCause::SpawnFailed(_)),
        "spawn 失败必须按 typed 成因退还任务"
    );

    // ⑥ 原 worker 完成当前任务（修复前此处认领 stale 令牌退出）。
    release_busy();
    wait_until_diag(
        "worker 处理完当前任务",
        Duration::from_secs(5),
        || pool.metrics().busy == 0,
        || format!("metrics={:?}", pool.metrics()),
    );

    // ⑦ 断言：workers 不得跌破 core；队列任务必须被做完（不只数 worker）。
    let m = pool.metrics();
    assert!(
        m.workers >= m.core,
        "issue#34: stale shrinking token 把 core worker 退役了 \
         (workers={} < core={}; queue_len={}; completed={})",
        m.workers,
        m.core,
        m.queue_len,
        m.completed
    );
    wait_until_diag(
        "队列任务被继续执行",
        Duration::from_secs(5),
        || pool.metrics().completed >= 2,
        || format!("metrics={:?}", pool.metrics()),
    );
    assert_eq!(pool.metrics().queue_len, 0, "队列任务不得滞留");

    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
}

/// issue#34 第二交错面（rollback_reservations 路径）：resize 补建 2 席在途时
/// 并发收缩——修复前缩容差额按含席位的 workers 口径误发 2 枚令牌；补建首个
/// 失败走 rollback_spawn + rollback_reservations 后令牌仍残留，同样退役
/// real worker。
#[test]
fn resize_rollback_must_not_retire_core_workers_via_stale_shrink_token() {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let gate = BlockingFailGate {
        entered: entered_tx,
        release: Mutex::new(release_rx),
    };
    let pool = Arc::new(
        match ThreadPool::<BlockingFailGate>::try_with_gate(
            tuning(1, 4, 4),
            Arc::new(AbortPolicy),
            gate,
        ) {
            Ok(p) => p,
            Err(e) => panic!("首 worker 创建应成功: {e}"),
        },
    );

    // ① worker 占忙 + 队列放一个任务（断言「工作真的被做了」的探针）。
    let (busy_gate, release_busy) = Gate::new();
    pool.execute(busy_gate.blocker()).unwrap();
    wait_until_diag(
        "worker busy",
        Duration::from_secs(5),
        || pool.metrics().busy == 1,
        || format!("metrics={:?}", pool.metrics()),
    );
    pool.execute(|| {}).unwrap();

    // ② 补建 2 席（deficit=2）：首个创建卡在闸内（已预约，未建成）。
    let resizer = {
        let pool = Arc::clone(&pool);
        std::thread::spawn(move || pool.resize(tuning(3, 4, 4)))
    };
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("补建提交未进入闸门（席位预约未发布）");

    // ③ 并发收缩 resize(core=1)：修复前 shrinking = workers(3) - 1 = 2（stale）。
    pool.resize(tuning(1, 4, 4)).unwrap();

    // ④ 放行 → create#1 失败 → rollback_spawn + rollback_reservations(1)。
    release_tx.send(()).expect("gate 线程存活");
    assert_eq!(
        resizer.join().unwrap(),
        Err(PoolResizeError::SpawnFailed {
            kind: io::ErrorKind::Other,
            target_core: 3,
            workers: 1,
        }),
        "补建失败必须 typed 报错（部分生效状态自带）"
    );

    // ⑤ 原 worker 完成当前任务。
    release_busy();
    wait_until_diag(
        "worker 处理完当前任务",
        Duration::from_secs(5),
        || pool.metrics().busy == 0,
        || format!("metrics={:?}", pool.metrics()),
    );

    // ⑥ 断言同主交错：core 保活 + 队列排空 + 任务被做完。
    let m = pool.metrics();
    assert!(
        m.workers >= m.core,
        "issue#34(reservations 路径): stale shrinking token 把 core worker 退役了 \
         (workers={} < core={}; queue_len={}; completed={})",
        m.workers,
        m.core,
        m.queue_len,
        m.completed
    );
    assert_eq!(m.spawn_failures, 1, "创建失败留痕");
    wait_until_diag(
        "队列任务被继续执行",
        Duration::from_secs(5),
        || pool.metrics().completed >= 2,
        || format!("metrics={:?}", pool.metrics()),
    );
    assert_eq!(pool.metrics().queue_len, 0, "队列任务不得滞留");

    pool.shutdown();
    join_bounded(&pool, "线程池停机排空");
}

// ── issue#99：ThreadPool resize 的事件时序与代次窗口 ─────────────────────
//
// 旧实现先 `base.resize`（同步发 `Resized`）再领 epoch ⇒ 回调内重入 resize 的
// 目标会被外层迟到代次覆盖（「最后发起的目标生效」反转），回调也能看到 base
// 新值 + worker 旧目标。以下测试用 `feature = "testing"` 的锁探针与一次性代次
// 钩子确定性复现（禁 sleep 控交错）。

#[cfg(feature = "testing")]
mod resize_event_timing {
    use super::*;

    const HANDOFF: Duration = Duration::from_secs(5);

    /// `Resized` 回调内重入 `resize`：外层必须返回、内层（后发起）目标生效且
    /// 不被外层迟到代次覆盖；回调读到的 core/max 必须是外层已提交配置。
    #[test]
    fn resized_callback_reenters_resize_and_last_initiated_target_wins() {
        let pool = Arc::new(ThreadPool::new(tuning(1, 8, 8)));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let nested = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&pool);
            let seen = Arc::clone(&seen);
            let nested = Arc::clone(&nested);
            pool.subscribe_audit(move |e| {
                if e.kind != PoolAuditKind::Resized {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                let (inner_free, core_free) = inner.resize_locks_free();
                let m = inner.metrics();
                seen.lock()
                    .expect("seen")
                    .push(((m.core, m.shape.max), inner_free && core_free));
                // 后发起的内层目标：必须赢过外层的迟到代次。
                nested
                    .lock()
                    .expect("nested results")
                    .push(inner.resize(tuning(4, 8, 8)));
            })
        };

        let (done_tx, done_rx) = mpsc::channel();
        let outer = {
            let p = Arc::clone(&pool);
            std::thread::spawn(move || {
                done_tx
                    .send(p.resize(tuning(2, 8, 8)))
                    .expect("send result");
            })
        };
        done_rx
            .recv_timeout(HANDOFF)
            .expect("Resized 回调内重入 resize 不得卡在内部锁")
            .expect("outer resize");
        outer.join().expect("outer thread");
        drop(ticket);

        let seen = seen.lock().expect("seen");
        assert_eq!(seen.len(), 1, "一次成功 resize 恰一次 Resized 回调");
        assert_eq!(
            seen[0].0,
            (2, 8),
            "回调必须看到外层已提交的 core/max（不得是 base 新值 + worker 旧目标）"
        );
        assert!(seen[0].1, "回调运行时调度锁与 base 账本锁均须空闲");
        drop(seen);
        let nested = nested.lock().expect("nested results");
        assert!(nested[0].is_ok(), "内层 resize 必须成功：{nested:?}");
        drop(nested);

        wait_until_diag(
            "内层目标 core=4 生效（外层迟到代次不得覆盖）",
            Duration::from_secs(5),
            || {
                let m = pool.metrics();
                m.core == 4 && m.workers == 4
            },
            || format!("metrics={:?}", pool.metrics()),
        );
        pool.shutdown();
        join_bounded(&pool, "线程池停机排空");
    }

    /// 确定性交错：首个 resize 停在「领号 → 目标提交」窗，后发起者必须拿到
    /// 更高代次并最终生效（#69 的「last initiated wins」在该窗口的遗漏）。
    #[test]
    fn later_initiated_resize_wins_when_earlier_is_paused_after_epoch_claim() {
        let pool = Arc::new(ThreadPool::new(tuning(1, 8, 8)));
        let (paused_tx, paused_rx) = mpsc::channel::<u64>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        pool.set_resize_epoch_hook(move |epoch| {
            paused_tx.send(epoch).expect("report paused epoch");
            // 阻塞到测试放行：确定性暂停在「领号 → 目标提交」窗（禁 sleep）。
            release_rx.recv().expect("release signal");
        });

        let (done_tx, done_rx) = mpsc::channel();
        let a = {
            let p = Arc::clone(&pool);
            std::thread::spawn(move || {
                done_tx
                    .send(p.resize(tuning(2, 8, 8)))
                    .expect("send result");
            })
        };
        assert_eq!(
            paused_rx.recv_timeout(HANDOFF).expect("A 必须停在领号后"),
            1,
            "首个 resize 领取代次 1"
        );

        // B 后发起：领更高代次并完整提交。
        pool.resize(tuning(5, 8, 8)).expect("B resize");
        assert_eq!(pool.metrics().core, 5, "后发起的 B 先提交目标");

        release_tx.send(()).expect("release A");
        done_rx
            .recv_timeout(HANDOFF)
            .expect("A 必须返回")
            .expect("A resize");
        a.join().expect("A thread");

        let m = pool.metrics();
        assert_eq!(m.core, 5, "外层迟到代次不得覆盖后发起的目标");
        assert_eq!(m.shape.max, 8);
        assert_eq!(m.workers, 5);
        pool.shutdown();
        join_bounded(&pool, "线程池停机排空");
    }

    /// `ResizeRejected` 回调内重入 resize：拒绝事件也在锁外发射。
    #[test]
    fn rejected_callback_reenters_resize_without_self_lock() {
        let pool = Arc::new(ThreadPool::new(tuning(1, 4, 8)));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let nested = Arc::new(Mutex::new(Vec::new()));
        let ticket = {
            let weak = Arc::downgrade(&pool);
            let seen = Arc::clone(&seen);
            let nested = Arc::clone(&nested);
            pool.subscribe_audit(move |e| {
                if e.kind != PoolAuditKind::ResizeRejected {
                    return;
                }
                let Some(inner) = weak.upgrade() else { return };
                let (inner_free, core_free) = inner.resize_locks_free();
                seen.lock().expect("seen").push(inner_free && core_free);
                nested
                    .lock()
                    .expect("nested results")
                    .push(inner.resize(tuning(3, 4, 8)));
            })
        };

        let (done_tx, done_rx) = mpsc::channel();
        let outer = {
            let p = Arc::clone(&pool);
            std::thread::spawn(move || {
                // core(5) > max(4)：纯校验拒绝。
                done_tx
                    .send(p.resize(tuning(5, 4, 8)))
                    .expect("send result");
            })
        };
        let error = done_rx
            .recv_timeout(HANDOFF)
            .expect("ResizeRejected 回调内重入 resize 不得卡在内部锁")
            .expect_err("illegal tuning");
        outer.join().expect("outer thread");
        drop(ticket);

        assert!(
            matches!(error, PoolResizeError::IdleCapExceedsMax { .. }),
            "拒绝成因：{error:?}"
        );
        let seen = seen.lock().expect("seen");
        assert_eq!(seen.len(), 1, "拒绝路径恰一次 ResizeRejected 回调");
        assert!(seen[0], "Rejected 回调运行时调度锁与 base 账本锁均须空闲");
        drop(seen);
        let nested = nested.lock().expect("nested results");
        assert!(
            nested[0].is_ok(),
            "拒绝事件在锁外发射 ⇒ 回调内 resize 必须成功：{nested:?}"
        );
        drop(nested);

        wait_until_diag(
            "回调内 resize 的目标 core=3 生效",
            Duration::from_secs(5),
            || {
                let m = pool.metrics();
                m.core == 3 && m.workers == 3
            },
            || format!("metrics={:?}", pool.metrics()),
        );
        pool.shutdown();
        join_bounded(&pool, "线程池停机排空");
    }
}
