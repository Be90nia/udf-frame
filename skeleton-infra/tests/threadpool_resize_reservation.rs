#![cfg(feature = "pool")]
//! ThreadPool 并发 resize 的在途预留对账（issue#69）：并发 resize 不得重复
//! 预留同一 core 缺口 ⇒ `workers`（真实存活 + 在途席位）恒 ≤ `max`。
//!
//! 确定性编排（channel 握手 + 条件变量，禁 sleep 定时）：
//! 1. 池 core=1/max=8，注入**脚本化创建闸**（`SpawnGate`）：指定 worker_id 的
//!    创建裁决阻塞到放行 / 或确定性失败（裁决点在状态锁外，闸内阻塞不持池锁）；
//! 2. 线程 A 调 `resize`（大 core）⇒ 预留若干席位后卡在创建闸；
//! 3. 主线程确认 A 已进闸 ⇒ 线程 B 调并发 `resize`（同目标 / 不同目标）；
//! 4. 放闸 ⇒ 断言（存活数 ≤ max、恰好收敛到 core / 收敛到最新目标 / 缩容不
//!    过冲 / 注入失败后无幽灵预留且可补齐）。
//!
//! 旧实现的复现（同一编排）：`live = workers - reservations` 算增长缺口 ⇒
//! 看不见 A 的在途预留 ⇒ 又预留 3 个 ⇒ workers = 7 > max = 4（超限）。

use std::io;
use std::sync::mpsc::{Sender, channel};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use skeleton::pool::PoolResizeError;
use skeleton_infra::pool::{
    AbortPolicy, SpawnGate, ThreadPool, ThreadPoolTuning, ThreadSpawnError,
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// 有界轮询等待条件成立（禁固定 sleep 断言）。
fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(
            start.elapsed() < HANDSHAKE_TIMEOUT,
            "timeout waiting for {what}"
        );
        thread::sleep(Duration::from_millis(1));
    }
}

/// 脚本化创建闸：指定 worker_id 的创建裁决阻塞到放行；另一些 id 确定性失败。
/// 裁决点 = `create_worker` 内（**状态锁外**）⇒ 闸内阻塞不持池锁。
struct ScriptedGate {
    /// id ≥ 此值的创建裁决阻塞到放行（usize::MAX = 不阻塞）。
    block_from: u64,
    /// 这些 id 的创建裁决确定性失败（注入环境故障）。
    fail_ids: Vec<u64>,
    /// 进入闸（阻塞中）的 worker_id 通知（测试据此确认窗口打开）。
    entered: Sender<u64>,
    release: Arc<(Mutex<bool>, Condvar)>,
}

impl ScriptedGate {
    fn new(
        block_from: u64,
        fail_ids: Vec<u64>,
        entered: Sender<u64>,
        release: Arc<(Mutex<bool>, Condvar)>,
    ) -> Self {
        Self {
            block_from,
            fail_ids,
            entered,
            release,
        }
    }
}

/// 放行闸门（测试持有独立句柄——闸本体已移入池内）。
fn release_gate(release: &Arc<(Mutex<bool>, Condvar)>) {
    let (m, cv) = &**release;
    let mut released = m.lock().expect("release lock");
    *released = true;
    cv.notify_all();
}

impl SpawnGate for ScriptedGate {
    fn check_spawn(&self, worker_id: u64) -> Result<(), ThreadSpawnError> {
        if self.fail_ids.contains(&worker_id) {
            return Err(ThreadSpawnError::from(io::Error::other(
                "scripted spawn failure",
            )));
        }
        if worker_id >= self.block_from {
            self.entered
                .send(worker_id)
                .expect("entered receiver alive (test drives the gate)");
            let (m, cv) = &*self.release;
            let mut released = m.lock().expect("release lock");
            while !*released {
                released = cv.wait(released).expect("release wait");
            }
        }
        Ok(())
    }
}

/// 造一个池（脚本化创建闸 + 默认拒绝策略）。
fn pool_with_gate(tuning: ThreadPoolTuning, gate: ScriptedGate) -> ThreadPool<ScriptedGate> {
    ThreadPool::try_with_gate(tuning, Arc::new(AbortPolicy), gate).expect("pool construction")
}

// ── 验收 1：两个并发 resize 到同一更大 core ⇒ 创建数不超 max，且精确收敛 ──

#[test]
fn concurrent_resize_to_same_core_does_not_double_reserve() {
    let (entered_tx, entered_rx) = channel();
    // 扩容用的 worker id 从 1 起（core=1 的保活 worker 是 id 0）⇒ id ≥ 1 阻塞。
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = ScriptedGate::new(1, Vec::new(), entered_tx, Arc::clone(&release));
    let pool = pool_with_gate(ThreadPoolTuning::new(1, 4, 8), gate);
    let pool = Arc::new(pool);
    wait_until("initial core worker live", || pool.metrics().workers == 1);

    // ② 线程 A：扩到 core=4 ⇒ 预留 3 席后卡在创建闸（id=1）。
    let pool_a = Arc::clone(&pool);
    let resize_a = thread::spawn(move || pool_a.resize(ThreadPoolTuning::new(4, 4, 8)));
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("resize A blocked in spawn gate");

    // ③ 线程 B：同一目标的并发 resize（A 的 3 个席位仍在途）。
    let pool_b = Arc::clone(&pool);
    let resize_b = thread::spawn(move || pool_b.resize(ThreadPoolTuning::new(4, 4, 8)));
    let _ = resize_b.join().expect("resize B joined");

    // ④ 放闸 ⇒ A 的在途创建兑现。
    release_gate(&release);
    let _ = resize_a.join().expect("resize A joined");

    // ⑤ 断言：存活 worker ≤ max 且恰好到 core（无重复预留 ⇒ 无超限）。
    wait_until("workers settled", || {
        let m = pool.metrics();
        m.workers >= 4
    });
    let m = pool.metrics();
    assert!(
        m.workers <= m.shape.max,
        "存活 worker 不得超 max（并发 resize 重复预留）：workers={} max={}",
        m.workers,
        m.shape.max
    );
    assert_eq!(
        m.workers, 4,
        "并发 resize 到同一 core：存活数恰为 core（不得重复预留）：{m:?}"
    );
}

// ── 验收 2：两个并发 resize 到不同 core 目标 ⇒ 收敛到最新目标 ──────────

#[test]
fn concurrent_resize_to_different_targets_converges_to_latest() {
    let (entered_tx, entered_rx) = channel();
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = ScriptedGate::new(1, Vec::new(), entered_tx, Arc::clone(&release));
    let pool = pool_with_gate(ThreadPoolTuning::new(1, 8, 8), gate);
    let pool = Arc::new(pool);
    wait_until("initial core worker live", || pool.metrics().workers == 1);

    // ② 线程 A：扩到 core=4 ⇒ 预留 3 席后卡在创建闸。
    let pool_a = Arc::clone(&pool);
    let resize_a = thread::spawn(move || pool_a.resize(ThreadPoolTuning::new(4, 8, 8)));
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("resize A blocked in spawn gate");

    // ③ 线程 B（后取得状态锁 ⇒ 目标更新在后）：缩到 core=2。
    let pool_b = Arc::clone(&pool);
    let resize_b = thread::spawn(move || pool_b.resize(ThreadPoolTuning::new(2, 8, 8)));
    let _ = resize_b.join().expect("resize B joined");

    // ④ 放闸 ⇒ A 的 3 个席位兑现（为旧目标预留 ⇒ 超出最新 core 者退役）。
    release_gate(&release);
    let _ = resize_a.join().expect("resize A joined");

    // ⑤ 断言：收敛到最新目标（2），不为旧目标（4）多留 worker。
    wait_until("converged to latest target", || pool.metrics().workers == 2);
    let m = pool.metrics();
    assert_eq!(
        m.workers, 2,
        "并发 resize 到不同目标：收敛到最新 core（旧目标的在途席位不得多留）：{m:?}"
    );
    assert!(m.workers <= m.shape.max, "存活 worker 不得超 max：{m:?}");
}

// ── 验收 3：缩容与被阻塞的增长预留竞争 ⇒ 事后不得过度增长 ──────────────

#[test]
fn shrink_racing_blocked_growth_reservation_does_not_overgrow() {
    let (entered_tx, entered_rx) = channel();
    // 池 core=4/max=8：保活 worker 是 id 0..3；扩容 worker id ≥ 4 阻塞。
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = ScriptedGate::new(4, Vec::new(), entered_tx, Arc::clone(&release));
    let pool = pool_with_gate(ThreadPoolTuning::new(4, 8, 8), gate);
    let pool = Arc::new(pool);
    wait_until("initial core workers live", || pool.metrics().workers == 4);

    // ② 线程 A：扩到 core=8 ⇒ 预留 4 席后卡在创建闸。
    let pool_a = Arc::clone(&pool);
    let resize_a = thread::spawn(move || pool_a.resize(ThreadPoolTuning::new(8, 8, 8)));
    entered_rx
        .recv_timeout(HANDSHAKE_TIMEOUT)
        .expect("resize A blocked in spawn gate");

    // ③ 线程 B：缩到 core=2（与 A 的在途增长预留竞争）。
    let pool_b = Arc::clone(&pool);
    let resize_b = thread::spawn(move || pool_b.resize(ThreadPoolTuning::new(2, 8, 8)));
    let _ = resize_b.join().expect("resize B joined");

    // ④ 放闸 ⇒ A 的在途席位兑现（超出最新 core ⇒ 退役）。
    release_gate(&release);
    let _ = resize_a.join().expect("resize A joined");

    // ⑤ 断言：最终不超 max，且收敛到最新目标（2）——不过冲。
    wait_until("converged after shrink race", || {
        pool.metrics().workers == 2
    });
    let m = pool.metrics();
    assert!(
        m.workers <= m.shape.max,
        "缩容与增长预留竞争后不得超 max：{m:?}"
    );
    assert_eq!(
        m.workers, 2,
        "缩容与增长预留竞争：收敛到最新 core（不按旧目标过冲）：{m:?}"
    );
}

// ── 验收 4：注入 spawn 失败 ⇒ 回滚无幽灵预留 / 无陈旧令牌 / 可补齐 core ────

#[test]
fn failed_spawn_rolls_back_cleanly_and_core_can_be_refilled() {
    let (entered_tx, _entered_rx) = channel();
    // id 1 的创建确定性失败；id ≥ 2 阻塞（先失败再观察回滚）。
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let gate = ScriptedGate::new(2, vec![1], entered_tx, Arc::clone(&release));
    let pool = pool_with_gate(ThreadPoolTuning::new(1, 8, 8), gate);
    let pool = Arc::new(pool);
    wait_until("initial core worker live", || pool.metrics().workers == 1);

    // ② 扩到 core=4：id 1 失败 ⇒ resize 报 typed Err（部分生效）。
    let pool_a = Arc::clone(&pool);
    let resize_a = thread::spawn(move || pool_a.resize(ThreadPoolTuning::new(4, 8, 8)));
    let outcome = resize_a.join().expect("resize A joined");
    assert!(
        matches!(outcome, Err(PoolResizeError::SpawnFailed { .. })),
        "创建失败须报 typed Err：{outcome:?}"
    );

    // ③ 回滚后：无幽灵预留（存活数与 metrics 一致）、无陈旧退役令牌
    //    （池仍可按需补建，不被幽灵席位卡住）。
    let m = pool.metrics();
    assert_eq!(
        m.workers, 1,
        "回滚后存活数=真实 worker 数（无幽灵预留）：{m:?}"
    );

    // ④ 再次 resize（同目标）⇒ 失败的那批补齐 ⇒ 收敛到 core（不留永久偏小的 core）。
    release_gate(&release);
    pool.resize(ThreadPoolTuning::new(4, 8, 8))
        .expect("refill resize succeeds after gate released");
    wait_until("core refilled", || pool.metrics().workers == 4);
    assert_eq!(
        pool.metrics().workers,
        4,
        "失败回滚后可补齐到 core（不留永久偏小的 core）"
    );
}
