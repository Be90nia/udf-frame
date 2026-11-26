#![cfg(feature = "view")]
//! #157 机制胞（`Derived` / `Sentinel`）两阶段代际协议的公共面验收：并发未命中
//! 竞态下缓存 `(In, Out)` 配对自洽且全程无死锁 / 陈旧计算不覆盖更新的已提交值 /
//! 哨兵裁决在线性化点复核代际、至多重试一轮 / 采用方代码 panic 不毒化胞。锁边界
//! 的直接证据（回调里对胞锁 `try_lock`、同线程重入）在 `src/view.rs` 的单元探针里。
//!
//! 时序一律用握手钉死（通道 + OS 时钟截止），不靠 sleep 猜时序。

use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use skeleton_infra::view::{Derived, Sentinel};

/// OS 时钟护栏：握手等待与线程收拢的上限，超时 = 有界失败（疑似死锁），不挂死。
const LIMIT: Duration = Duration::from_secs(15);

/// 带编号的输出：`id` 恒等于算出它的输入键 ⇒ 缓存配对自洽可断言。
#[derive(Clone, Debug, PartialEq)]
struct Val {
    id: u32,
}

/// OS 时钟截止收拢工作线程。
fn join_bounded<T>(workers: Vec<thread::JoinHandle<T>>, phase: &str) -> Vec<T> {
    let deadline = Instant::now() + LIMIT;
    while workers.iter().any(|worker| !worker.is_finished()) {
        assert!(
            Instant::now() < deadline,
            "{phase}：并发调用疑似死锁（OS 时钟截止）"
        );
        thread::sleep(Duration::from_millis(1));
    }
    workers
        .into_iter()
        .map(|worker| worker.join().expect("worker panicked"))
        .collect()
}

/// OS 时钟截止执行一次调用（旧实现把采用方代码关在胞锁内时，这里会有界失败）。
fn bounded<T: Send + 'static>(call: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(call());
    });
    match rx.recv_timeout(LIMIT) {
        Ok(value) => value,
        Err(mpsc::RecvTimeoutError::Timeout) => panic!("调用疑似死锁（OS 时钟截止）"),
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("调用在回报前 panic"),
    }
}

// ── Derived ────────────────────────────────────────────────────────────────────

/// compute 钩子：计数；`gate` 置位时在栅栏等齐；`boom` 置位时 panic（都在锁外）。
struct ComputeHook {
    calls: AtomicUsize,
    gate: AtomicBool,
    barrier: Barrier,
    boom: AtomicBool,
}

impl ComputeHook {
    fn new(parties: usize) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            gate: AtomicBool::new(true),
            barrier: Barrier::new(parties),
            boom: AtomicBool::new(false),
        }
    }

    fn compute(&self, key: &u32) -> Val {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.boom.load(Ordering::SeqCst), "injected compute panic");
        if self.gate.load(Ordering::SeqCst) {
            self.barrier.wait();
        }
        Val { id: *key }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

fn derived(hook: Arc<ComputeHook>) -> Arc<Derived<u32, u32, Val>> {
    Arc::new(Derived::new(
        |state: &u32| *state,
        move |key: &u32| hook.compute(key),
    ))
}

/// 非破坏式缓存探针：compute 置为 panic 后逐键 `get`——命中（`Ok`）的键就是缓存
/// 现存配对的输入；未命中（`Err`：重算 panic ⇒ 不发布）不动缓存。panic 在锁外 ⇒
/// 胞不毒化，探针可反复使用。
fn cached_now(cell: &Derived<u32, u32, Val>, hook: &ComputeHook, keys: &[u32]) -> Vec<(u32, Val)> {
    hook.boom.store(true, Ordering::SeqCst);
    let hits = keys
        .iter()
        .filter_map(|key| {
            catch_unwind(AssertUnwindSafe(|| cell.get(key)))
                .ok()
                .map(|out| (*key, out))
        })
        .collect();
    hook.boom.store(false, Ordering::SeqCst);
    hits
}

fn assert_paired(hits: &[(u32, Val)]) {
    assert_eq!(hits.len(), 1, "缓存恰持一对");
    assert!(
        hits.iter().all(|(key, out)| out.id == *key),
        "out.id == in.id：{hits:?}"
    );
}

#[test]
fn view_cells_concurrent_miss_race_keeps_cached_pair_coherent() {
    // 同 key / 不同 key 混用（前两个线程同键）。
    let keys = [7, 7, 8, 9];
    let hook = Arc::new(ComputeHook::new(keys.len()));
    let cell = derived(Arc::clone(&hook));

    // 第一轮：四个线程都在 compute 的栅栏上等齐 ⇒ 都已快照过空缓存，随后各自
    // 发布（同一代际）——必出三个陈旧发布，缓存恰留一对。
    let workers: Vec<_> = keys
        .iter()
        .map(|key| {
            let (cell, key) = (Arc::clone(&cell), *key);
            thread::spawn(move || {
                let out = cell.get(&key);
                assert_eq!(out.id, key, "每个请求拿到的都是自己键的结果");
                out
            })
        })
        .collect();
    let first = join_bounded(workers, "栅栏轮");
    assert_eq!(hook.calls(), 4);
    assert!(first.iter().all(|out| keys.contains(&out.id)));
    assert_paired(&cached_now(&cell, &hook, &[7, 8, 9]));

    // 第二轮：撤掉栅栏自由竞争，每次结果仍须对得上自己的键。
    hook.gate.store(false, Ordering::SeqCst);
    let workers: Vec<_> = keys
        .iter()
        .map(|key| {
            let (cell, key) = (Arc::clone(&cell), *key);
            thread::spawn(move || {
                for _ in 0..3_000 {
                    let out = cell.get(&key);
                    assert_eq!(out.id, key, "每个请求拿到的都是自己键的结果");
                }
            })
        })
        .collect();
    join_bounded(workers, "自由竞争轮");
    assert_paired(&cached_now(&cell, &hook, &[7, 8, 9]));
    assert_eq!(cell.get(&7), Val { id: 7 });
}

#[test]
fn view_cells_stale_computation_never_overwrites_a_newer_commit() {
    let (arrived_tx, arrived_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let slow = Arc::new(AtomicBool::new(true));
    let calls = Arc::new(AtomicUsize::new(0));
    let cell: Arc<Derived<u32, u32, Val>> = Arc::new(Derived::new(|state: &u32| *state, {
        let slow = Arc::clone(&slow);
        let calls = Arc::clone(&calls);
        move |key: &u32| {
            calls.fetch_add(1, Ordering::SeqCst);
            if *key == 1 && slow.swap(false, Ordering::SeqCst) {
                // 握手：键 1 已快照（缓存空）且正在算，等主线用键 2 发布完再收尾。
                arrived_tx.send(()).expect("coordinator alive");
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(LIMIT)
                    .expect("released");
            }
            Val { id: *key }
        }
    }));
    let racer = {
        let cell = Arc::clone(&cell);
        thread::spawn(move || cell.get(&1))
    };
    arrived_rx
        .recv_timeout(LIMIT)
        .expect("slow compute arrived");

    // 慢计算尚在进行：键 2 的请求先发布。
    let publisher = Arc::clone(&cell);
    assert_eq!(bounded(move || publisher.get(&2)).id, 2);
    release_tx.send(()).expect("racer alive");
    assert_eq!(
        join_bounded(vec![racer], "慢计算").remove(0),
        Val { id: 1 },
        "陈旧计算的结果对本次请求仍正确"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);

    // 陈旧结果没写缓存：键 2 仍命中（零重算），键 1 需重算。
    assert_eq!(cell.get(&2), Val { id: 2 });
    assert_eq!(calls.load(Ordering::SeqCst), 2, "键 2 仍是缓存现值");
    assert_eq!(cell.get(&1), Val { id: 1 });
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[test]
fn view_cells_derived_memoizes_by_input_equality() {
    let calls = Arc::new(AtomicUsize::new(0));
    let cell: Derived<u32, u32, Val> = Derived::new(|state: &u32| state / 2, {
        let calls = Arc::clone(&calls);
        move |key: &u32| {
            calls.fetch_add(1, Ordering::SeqCst);
            Val { id: *key }
        }
    });
    assert_eq!(cell.get(&2), Val { id: 1 }, "算");
    assert_eq!(cell.get(&2), Val { id: 1 }, "命中");
    assert_eq!(
        cell.get(&3),
        Val { id: 1 },
        "等值输入（3/2 == 2/2）同样命中"
    );
    assert_eq!(cell.get(&9), Val { id: 4 }, "换键重算");
    assert_eq!(cell.get(&2), Val { id: 1 }, "旧键不再命中");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

// ── Sentinel ───────────────────────────────────────────────────────────────────

thread_local! {
    /// 本线程接下来要在前 N 次 `eq` 上进握手点；只有被指定的线程置数。
    static EQ_CHECKPOINTS: Cell<u32> = const { Cell::new(0) };
}

/// 握手通道：键侧（`eq` 内，即胞锁外）报「已到检查点」并等放行；协调端收/放。
struct Handshake {
    check_rx: Mutex<mpsc::Receiver<()>>,
    go_tx: mpsc::Sender<()>,
    check_tx: mpsc::Sender<()>,
    go_rx: Mutex<mpsc::Receiver<()>>,
}

impl Handshake {
    fn new() -> Self {
        let (check_tx, check_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        Self {
            check_rx: Mutex::new(check_rx),
            go_tx,
            check_tx,
            go_rx: Mutex::new(go_rx),
        }
    }

    /// 键侧：预算内则报到了并等放行。
    fn checkpoint(&self) {
        if EQ_CHECKPOINTS.with(|budget| {
            let left = budget.get();
            budget.set(left.saturating_sub(1));
            left > 0
        }) {
            self.check_tx.send(()).expect("coordinator alive");
            self.go_rx
                .lock()
                .unwrap()
                .recv_timeout(LIMIT)
                .expect("released");
        }
    }

    /// 协调端：等键侧进检查点 / 放行。
    fn wait_checkpoint(&self) {
        self.check_rx
            .lock()
            .unwrap()
            .recv_timeout(LIMIT)
            .expect("eq reached checkpoint");
    }

    fn release(&self) {
        self.go_tx.send(()).expect("key side alive");
    }
}

/// 握手键：`eq` 里过握手点。旧实现的 eq 在胞锁内 ⇒ 键侧一阻塞，协调端的另一次
/// `changed` 就被卡死——这正是护栏要抓的形态。
struct EqKey {
    id: u32,
    handshake: Arc<Handshake>,
}

impl PartialEq for EqKey {
    fn eq(&self, other: &Self) -> bool {
        self.handshake.checkpoint();
        self.id == other.id
    }
}

fn sentinel(handshake: Arc<Handshake>) -> Arc<Sentinel<u32, EqKey>> {
    Arc::new(Sentinel::new(move |state: &u32| EqKey {
        id: *state,
        handshake: Arc::clone(&handshake),
    }))
}

fn spawn_changed(
    cell: Arc<Sentinel<u32, EqKey>>,
    key: u32,
    checkpoints: u32,
) -> thread::JoinHandle<bool> {
    thread::spawn(move || {
        EQ_CHECKPOINTS.with(|budget| budget.set(checkpoints));
        cell.changed(&key)
    })
}

#[test]
fn view_cells_sentinel_rechecks_generation_before_reporting_unchanged() {
    let handshake = Arc::new(Handshake::new());
    let cell = sentinel(Arc::clone(&handshake));
    assert!(cell.changed(&1), "缓存 ← 1");

    let racer = spawn_changed(Arc::clone(&cell), 1, 1);
    // racer 已比对出「未变」，停在裁决前。
    handshake.wait_checkpoint();
    let publisher = Arc::clone(&cell);
    assert!(bounded(move || publisher.changed(&2)), "主线趁隙发布 2");
    handshake.release();
    assert!(
        join_bounded(vec![racer], "命中复核").remove(0),
        "裁决前复核代际：有线程抢先发布，相对最新提交 1 确实变了"
    );
    assert!(!cell.changed(&1), "重试那轮把 1 提交了");
    assert!(cell.changed(&2));
}

#[test]
fn view_cells_sentinel_overreports_without_committing_when_still_raced_after_one_retry() {
    let handshake = Arc::new(Handshake::new());
    let cell = sentinel(Arc::clone(&handshake));
    assert!(cell.changed(&0), "缓存 ← 0");

    let racer = spawn_changed(Arc::clone(&cell), 1, 2);
    // 第一轮：racer 比对出「变了」，停在发布前。
    handshake.wait_checkpoint();
    let publisher = Arc::clone(&cell);
    assert!(bounded(move || publisher.changed(&2)));
    handshake.release();
    // 第二轮（终轮）：racer 用新快照重新比对，又停在发布前。
    handshake.wait_checkpoint();
    let publisher = Arc::clone(&cell);
    assert!(bounded(move || publisher.changed(&3)));
    handshake.release();
    assert!(
        join_bounded(vec![racer], "终轮").remove(0),
        "两轮都撞上并发发布：多报"
    );
    assert!(!cell.changed(&3), "且不提交：缓存仍是 3");
    assert!(cell.changed(&1));
}

#[test]
fn view_cells_sentinel_reports_input_changes() {
    let cell: Sentinel<u32, u32> = Sentinel::new(|state: &u32| state / 2);
    assert!(cell.changed(&2));
    assert!(!cell.changed(&3), "等值输入（3/2 == 2/2）⇒ 未变");
    assert!(cell.changed(&9));
    assert!(cell.changed(&2));
    assert!(!cell.changed(&2));
}

#[test]
fn view_cells_sentinel_concurrent_calls_never_deadlock() {
    let handshake = Arc::new(Handshake::new());
    let cell = sentinel(Arc::clone(&handshake));
    let keys = [1, 1, 2, 3];
    let workers: Vec<_> = keys
        .iter()
        .map(|key| {
            let (cell, key) = (Arc::clone(&cell), *key);
            thread::spawn(move || {
                for _ in 0..3_000 {
                    cell.changed(&key);
                }
            })
        })
        .collect();
    join_bounded(workers, "哨兵自由竞争轮");
    // 静止后的终局：同一键连报两次必为「变 → 未变」。
    assert!(cell.changed(&5));
    assert!(!cell.changed(&5));
}

// ── panic 不毒化 ────────────────────────────────────────────────────────────────

#[test]
fn view_cells_compute_panic_leaves_the_cell_usable() {
    let boom = Arc::new(AtomicBool::new(true));
    let calls = Arc::new(AtomicUsize::new(0));
    let cell: Derived<u32, u32, Val> = Derived::new(|state: &u32| *state, {
        let boom = Arc::clone(&boom);
        let calls = Arc::clone(&calls);
        move |key: &u32| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert!(!boom.load(Ordering::SeqCst), "injected compute panic");
            Val { id: *key }
        }
    });
    assert!(catch_unwind(AssertUnwindSafe(|| cell.get(&1))).is_err());
    boom.store(false, Ordering::SeqCst);
    assert_eq!(cell.get(&1), Val { id: 1 }, "panic 原样穿出后胞照常可用");
    assert_eq!(calls.load(Ordering::SeqCst), 2, "panic 那次没发布");
}

/// 带「可武装 panic」钩子的输出值：两条 `Out::clone` 路径（缓存副本 / 命中返回）。
struct FragileVal {
    id: u32,
    boom: Arc<AtomicBool>,
}

impl Clone for FragileVal {
    fn clone(&self) -> Self {
        assert!(!self.boom.load(Ordering::SeqCst), "injected clone panic");
        Self {
            id: self.id,
            boom: Arc::clone(&self.boom),
        }
    }
}

#[test]
fn view_cells_out_clone_panic_leaves_the_cell_usable() {
    let boom = Arc::new(AtomicBool::new(true));
    let calls = Arc::new(AtomicUsize::new(0));
    let cell: Derived<u32, u32, FragileVal> = Derived::new(|state: &u32| *state, {
        let boom = Arc::clone(&boom);
        let calls = Arc::clone(&calls);
        move |key: &u32| {
            calls.fetch_add(1, Ordering::SeqCst);
            FragileVal {
                id: *key,
                boom: Arc::clone(&boom),
            }
        }
    });
    // 未命中路径：缓存副本的 clone panic ⇒ 不发布，原样穿出。
    assert!(catch_unwind(AssertUnwindSafe(|| cell.get(&1))).is_err());
    boom.store(false, Ordering::SeqCst);
    assert_eq!(cell.get(&1).id, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "上次没发布，这次重算");
    // 命中路径：返回值的 clone panic ⇒ 缓存不动。
    boom.store(true, Ordering::SeqCst);
    assert!(catch_unwind(AssertUnwindSafe(|| cell.get(&1))).is_err());
    boom.store(false, Ordering::SeqCst);
    assert_eq!(cell.get(&1).id, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "缓存仍在，命中零重算");
}

/// 带「可武装 panic」钩子的键：两条胞的命中路径都会跑 `In::eq`。
struct FragileKey {
    id: u32,
    boom: Arc<AtomicBool>,
}

impl PartialEq for FragileKey {
    fn eq(&self, other: &Self) -> bool {
        assert!(!self.boom.load(Ordering::SeqCst), "injected eq panic");
        self.id == other.id
    }
}

fn fragile_keys(boom: &Arc<AtomicBool>) -> impl Fn(&u32) -> FragileKey + Send + Sync + 'static {
    let boom = Arc::clone(boom);
    move |state: &u32| FragileKey {
        id: *state,
        boom: Arc::clone(&boom),
    }
}

#[test]
fn view_cells_in_eq_panic_leaves_both_cells_usable() {
    let boom = Arc::new(AtomicBool::new(true));
    let derived: Derived<u32, FragileKey, Val> =
        Derived::new(fragile_keys(&boom), |key: &FragileKey| Val { id: key.id });
    let sentinel: Sentinel<u32, FragileKey> = Sentinel::new(fragile_keys(&boom));
    // 先各发布一次（缓存空时 eq 不跑）。
    assert_eq!(derived.get(&1).id, 1);
    assert!(sentinel.changed(&1));

    // 命中路径的 eq panic：原样穿出，缓存不动，胞不毒化。
    assert!(catch_unwind(AssertUnwindSafe(|| derived.get(&2))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| sentinel.changed(&2))).is_err());
    boom.store(false, Ordering::SeqCst);
    assert_eq!(derived.get(&1).id, 1, "缓存仍是键 1 的结果");
    assert!(sentinel.changed(&2));
    assert!(!sentinel.changed(&2));
}
