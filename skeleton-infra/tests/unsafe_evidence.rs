#![cfg(feature = "unsafe-evidence")]
//! 蓝图 §9「Unsafe implementation evidence」的可执行面：把 `DrainExclusive`
//! 的 SAFETY 注释从文档变成断言。
//!
//! 被测论断（`runtime.rs` 类型级安全纪要的三段）：
//!
//! 1. **建立者**：排空旗 `StoreInner::draining` 的 swap 互斥 ⇒ 同一时刻至多一个
//!    线程持排空权 ⇒ 三处独占字段（`effect_buf`/`notify_cache`/`notify_sink_buf`）
//!    的 `&mut` 永不并存；
//! 2. **维持者**：独占访问只在 `drain_owned` 的 `DrainGuard` armed 区间内进行，
//!    重入（reducer/订阅回调内 dispatch）只入队即返，到不了独占区间；
//! 3. **违反后果**：别名写 = `UnsafeCell` 数据竞争（Vec 扩容/清空撕裂、迭代器
//!    失效），不可兜底——故必须有可执行断言，而非仅注释。
//!
//! 三个不变量测试只经公开 API（`SyncStore::new`/`dispatch`/`snapshot`/
//! `subscribe`/`close` 与读数侧）驱动，不触私有字段；差分测试把同一 dispatch
//! 序列跑在「生产载体（UnsafeCell + 排空旗）」与「对照载体（`Mutex<T>`）」两个
//! 装配上，逐项比对可观察面（蓝图 §9：对照变体只针对该类型，禁重写 SyncStore）。
//!
//! 运行：`cargo +1.96.0 test -p udf-skeleton-infra --all-features --test unsafe_evidence`

use std::convert::Infallible;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use skeleton::{ActionSender, Effect, EffectBuf, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;

type TestStore = SyncStore<Tally, Cmd, Infallible>;

// ── 固定装置 ────────────────────────────────────────────────────────────────

/// 状态：`total` = 累加值，`applied` = reduce 次数——两者都由 action 值唯一
/// 决定，故任何顺序下的终值都是确定量（并发下可作精确断言）。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Tally {
    total: u64,
    applied: u64,
}

#[derive(Clone, Copy, Debug)]
enum Cmd {
    Add(u64),
    /// 产一条 `Emit(Add(n))` 再正常返回：效果缓冲 + 回灌环在并发下被持续读写，
    /// 且终值仍精确（自身 +n，回灌再把 +n 走一遍）。
    AddAndEmit(u64),
    /// 先产一条 `Emit(Add(n))` 再 panic：panic 前已产效果必须原地留缓冲，
    /// 由下一任排空的转发段续转（禁吞）。
    EmitThenPanic(u64),
    Panic,
}

struct TallyReducer;

impl Reducer for TallyReducer {
    type State = Tally;
    type Action = Cmd;
    type Op = Infallible;

    fn initial(&self) -> Tally {
        Tally::default()
    }

    fn reduce(&self, state: &Tally, action: Cmd, out: &mut EffectBuf<Cmd, Infallible>) -> Tally {
        match action {
            Cmd::Add(n) => Tally {
                total: state.total + n,
                applied: state.applied + 1,
            },
            Cmd::AddAndEmit(n) => {
                out.push(Effect::Emit(Cmd::Add(n)));
                Tally {
                    total: state.total + n,
                    applied: state.applied + 1,
                }
            }
            Cmd::EmitThenPanic(n) => {
                out.push(Effect::Emit(Cmd::Add(n)));
                panic!("reducer boom (EmitThenPanic)")
            }
            Cmd::Panic => panic!("reducer boom (Panic)"),
        }
    }
}

/// 订阅触达计数器（跨线程订阅档）。
fn counting_subscriber(hits: &Arc<AtomicU64>) -> impl FnMut(&Tally) + Send + 'static {
    let hits = Arc::clone(hits);
    move |_| {
        hits.fetch_add(1, Ordering::Relaxed);
    }
}

/// 有界等待：线程跑飞（死锁）时测试在超时点显式失败，而不是把 CI 挂死。
/// 必须在 `join` 之前调用——`thread::scope` 的收尾会无条件等线程，超时断言
/// 到不了（故本文件一律手 `spawn` + 完成信号）。
fn expect_within(receiver: &mpsc::Receiver<()>, count: usize, what: &str) {
    for i in 0..count {
        receiver
            .recv_timeout(Duration::from_secs(60))
            .unwrap_or_else(|e| {
                panic!("{what}: 第 {i} 个工作线程未在 60s 内完成（排空权未交接 = 死锁？）: {e}")
            });
    }
}

// ── ① 并发 dispatch × 快照 clone/drop × 订阅 register/unregister 交错 ───────

/// 断言 SAFETY 论断 1+2：排空旗互斥下三处独占字段永不并存写 ⇒
/// 状态终值精确（无丢/重拍）、固定订阅者触达数 == reduce 拍数（通知缓存不错位、
/// 不重复广播）、快照单调（无撕裂读）、无死锁无 panic。
#[test]
fn concurrent_dispatch_snapshot_and_subscription_interleaving_is_exact() {
    const DISPATCHERS: usize = 4;
    const PER_THREAD: u64 = 200;
    const PER_KIND: u64 = PER_THREAD / 2; // 每线程 Add/AddAndEmit 各半（奇偶交替）

    let store = Arc::new(TestStore::new(TallyReducer, NoFx));
    let hits = Arc::new(AtomicU64::new(0));
    let churn_hits = Arc::new(AtomicU64::new(0));

    // 固定订阅者：登记后不注销 ⇒ 它的回调数就是「reduce 拍数」的无损计数面。
    let permanent = store.subscribe(counting_subscriber(&hits));

    let barrier = Arc::new(Barrier::new(DISPATCHERS + 1)); // 4 派发 + 1 搅动
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let mut handles = Vec::new();

    // 派发线程：Add / AddAndEmit 交替——效果缓冲与回灌环同步受压。
    for t in 0..DISPATCHERS {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let done_tx = done_tx.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..PER_THREAD {
                if (i + t as u64).is_multiple_of(2) {
                    store.dispatch(Cmd::Add(1));
                } else {
                    store.dispatch(Cmd::AddAndEmit(2));
                }
            }
            let _ = done_tx.send(());
        }));
    }
    // 搅动线程：快照 clone/drop + 订阅 register/unregister 与派发交错。
    {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let done_tx = done_tx.clone();
        let churn_hits = Arc::clone(&churn_hits);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let mut last_total = 0u64;
            for _ in 0..PER_THREAD {
                // 快照 clone/drop：`Arc` 强引用存活期会强制 CoW 克隆新分配
                // （旧快照冻结）——与排空者的 make_mut 竞争的最强压法。
                let snap = store.snapshot();
                assert!(
                    snap.total >= last_total,
                    "快照回退：{} < {}（撕裂读 = 快照替换非原子）",
                    snap.total,
                    last_total
                );
                last_total = snap.total;
                drop(snap);
                // 短命订阅：登记即注销，逼通知缓存走冷重建路径（版本失配）。
                let ticket = store.subscribe(counting_subscriber(&churn_hits));
                drop(ticket);
            }
            let _ = done_tx.send(());
        }));
    }
    drop(barrier);
    drop(done_tx);

    expect_within(&done_rx, DISPATCHERS + 1, "并发交错");
    for handle in handles {
        handle.join().expect("派发/搅动线程不得 panic");
    }

    // 期望值：每线程 PER_KIND 次 Add(1) + PER_KIND 次 AddAndEmit(2)，后者各产一条
    // 回灌 Add(2) ⇒ total = PER_KIND*(1 + 2 + 2)；reduce 拍数 = PER_KIND*3。
    let expected_total = DISPATCHERS as u64 * PER_KIND * 5;
    let expected_applied = DISPATCHERS as u64 * PER_KIND * 3;

    assert_eq!(
        *store.snapshot(),
        Tally {
            total: expected_total,
            applied: expected_applied
        },
        "状态终值必须精确（丢一拍或多一拍都会显形）"
    );
    assert_eq!(
        hits.load(Ordering::Relaxed),
        expected_applied,
        "固定订阅者触达数必须等于 reduce 拍数：少了 = 通知缓存错位，多了 = 同一 slot 被广播两次"
    );
    assert!(
        churn_hits.load(Ordering::Relaxed) <= expected_applied,
        "短命订阅者的触达总数不可能超过 reduce 拍数（超了即重复广播）"
    );
    assert_eq!(
        store.pending_actions(),
        0,
        "同步排空：dispatch 全返回即 idle"
    );
    assert_eq!(store.reducer_panic_count(), 0);
    assert_eq!(store.subscriber_panics(), 0);
    assert_eq!(
        store.subscriber_count(),
        1,
        "搅动票据全部注销后仅留固定订阅者"
    );

    // 停机闸：并发热路径收尾后 close 仍幂等可用（排空旗一致）。
    drop(permanent);
    store.close();
    assert!(store.is_closed());
    store.dispatch(Cmd::Add(1));
    assert_eq!(
        store.rejected_after_close(),
        1,
        "闭门后 dispatch 必须显式拒绝并计数"
    );
    assert_eq!(store.snapshot().total, expected_total, "闭门后状态不再前进");
}

// ── ② panic/unwind：排空旗一致 + 已产效果零丢失 ────────────────────────────

/// 断言 SAFETY 论断 2 的异常路径：`DrainGuard` 在栈展开时复位排空旗，且
/// `effect_buf` 的 panic 后状态仍自洽（容量与未转发内容都在）——旗若残留 true，
/// 后续 dispatch 只入队不执行（静默变砖），本测试立即显形。
#[test]
fn reducer_panic_keeps_drain_flag_consistent_and_effects_retained() {
    let store = TestStore::new(TallyReducer, NoFx);
    let hits = Arc::new(AtomicU64::new(0));
    let _ticket = store.subscribe(counting_subscriber(&hits));

    // panic 在 dispatch 调用点上浮（传播语义不变，只加计数与审计现场）。
    let failed = catch_unwind(AssertUnwindSafe(|| store.dispatch(Cmd::EmitThenPanic(5))));
    assert!(failed.is_err(), "reducer panic 必须上浮到 dispatch 调用点");
    assert_eq!(store.reducer_panic_count(), 1);
    assert_eq!(
        *store.snapshot(),
        Tally::default(),
        "panic 的 action 不前进状态（本体已出队）"
    );
    assert_eq!(store.pending_actions(), 0, "队列无残留");
    assert!(!store.is_closed(), "panic 不触发停机闸");
    assert_eq!(hits.load(Ordering::Relaxed), 0, "panic 拍无通知");

    // 排空旗已复位：下一次 dispatch 必须真的执行（若旗残留 true 则只入队、
    // 状态不动——这正是「旗恒 true → 静默变砖」的可执行检测面）。
    store.dispatch(Cmd::Add(1));
    // panic 前已产的 Emit(Add(5)) 原地留缓冲，本任排空的转发段先续转：
    // total = 1（Add(1)）+ 5（残留回灌）= 6；reduce 拍数 = 2。
    assert_eq!(
        *store.snapshot(),
        Tally {
            total: 6,
            applied: 2
        },
        "panic 前已产效果必须续转（禁吞），且排空旗已放行"
    );
    assert_eq!(hits.load(Ordering::Relaxed), 2, "两拍各一次通知");

    store.dispatch(Cmd::Add(1));
    assert_eq!(store.snapshot().total, 7, "实例可继续复用（无中毒）");
    assert_eq!(store.reducer_panic_count(), 1, "计数不重复");

    // 纯 panic（不产效果）同款：旗与计数纪律一致。
    let failed = catch_unwind(AssertUnwindSafe(|| store.dispatch(Cmd::Panic)));
    assert!(failed.is_err());
    assert_eq!(store.reducer_panic_count(), 2);
    store.dispatch(Cmd::Add(1));
    assert_eq!(store.snapshot().total, 8);
    assert_eq!(store.pending_actions(), 0);
}

/// 断言通知段的 panic 纪律（`notify_cache` 快照在 panic 后仍一致）：中毒订阅者
/// 被 fail-closed 摘除、其余订阅者照常触达、后续 dispatch 不再 panic。
#[test]
fn subscriber_panic_is_quarantined_without_poisoning_drain() {
    let store = TestStore::new(TallyReducer, NoFx);
    let boom = store.subscribe(|_| panic!("subscriber boom"));
    let survivor_hits = Arc::new(AtomicU64::new(0));
    let survivor = store.subscribe(counting_subscriber(&survivor_hits));

    store.dispatch(Cmd::Add(1));
    assert_eq!(store.subscriber_panics(), 1);
    assert_eq!(
        store.subscriber_panic_last_cause().as_deref(),
        Some("subscriber boom")
    );
    assert_eq!(store.subscriber_count(), 1, "中毒订阅者已摘除");

    // 摘除后不再有 panic 路径：状态照常前进，存活订阅者每拍一次。
    store.dispatch(Cmd::Add(1));
    assert_eq!(store.snapshot().total, 2);
    assert_eq!(survivor_hits.load(Ordering::Relaxed), 2);
    assert_eq!(store.pending_actions(), 0);
    drop((boom, survivor));
}

// ── ③ 重入 dispatch：只入队不越界 ───────────────────────────────────────────

/// reducer 内的同栈 dispatch：`handle` 由装配后的 `store.sender()` 注入（与
/// `SyncStore::new` 文档的「解释器观测惯用形」同款共享句柄）。
struct Reentrant {
    handle: Arc<Mutex<Option<ActionSender<Cmd>>>>,
    remaining: AtomicU64,
}

impl Reducer for Reentrant {
    type State = Tally;
    type Action = Cmd;
    type Op = Infallible;

    fn initial(&self) -> Tally {
        Tally::default()
    }

    fn reduce(&self, state: &Tally, action: Cmd, _out: &mut EffectBuf<Cmd, Infallible>) -> Tally {
        // 重入：reducer 内同栈 dispatch。此时排空权已在本线程手里，`enqueue` 的
        // swap 见 true ⇒ 只入队即返，故不会二次进入独占区间——这正是
        // 「重入路径到不了三处可变面」的断言面。
        if self.remaining.load(Ordering::Relaxed) > 0 {
            self.remaining.fetch_sub(1, Ordering::Relaxed);
            let sender = {
                let guard = self.handle.lock().expect("handle 锁不中毒");
                guard.clone().expect("装配后已注入 sender")
            };
            assert!(sender.dispatch(Cmd::Add(1)), "排空期内回灌必须被接受");
        }
        let n = match action {
            Cmd::Add(n) | Cmd::AddAndEmit(n) => n,
            _ => 0,
        };
        Tally {
            total: state.total + n,
            applied: state.applied + 1,
        }
    }
}

/// 断言 SAFETY 论断 2 的重入面：并发 dispatch 下 reducer 内同栈 dispatch 只入队，
/// 由最外层排空环继续处理（不越界进入独占区间）；终值与拍数仍精确。
#[test]
fn reentrant_dispatch_from_reducer_only_enqueues() {
    const DISPATCHERS: usize = 4;
    const PER_THREAD: u64 = 100;
    // 远低于 FEEDBACK_DEPTH_BUDGET(64)：本测试压的是重入交接，不是预算点。
    const REENTRANT_BUDGET: u64 = 32;

    let handle = Arc::new(Mutex::new(None));
    let store = Arc::new(TestStore::new(
        Reentrant {
            handle: Arc::clone(&handle),
            remaining: AtomicU64::new(REENTRANT_BUDGET),
        },
        NoFx,
    ));
    *handle.lock().expect("handle 锁不中毒") = Some(store.sender());

    let hits = Arc::new(AtomicU64::new(0));
    let _ticket = store.subscribe(counting_subscriber(&hits));

    let barrier = Arc::new(Barrier::new(DISPATCHERS));
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let mut handles = Vec::new();
    for _ in 0..DISPATCHERS {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        let done_tx = done_tx.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..PER_THREAD {
                store.dispatch(Cmd::Add(1));
            }
            let _ = done_tx.send(());
        }));
    }
    drop(barrier);
    drop(done_tx);

    expect_within(&done_rx, DISPATCHERS, "重入并发");
    for handle in handles {
        handle.join().expect("派发线程不得 panic");
    }

    // 精确值：派发 4×100 次 + 回灌预算恰好耗尽 REENTRANT_BUDGET 次重入
    // （reduce 串行化 ⇒ `remaining` 的读改写无竞态，触发次数精确）。
    let expected_applied = DISPATCHERS as u64 * PER_THREAD + REENTRANT_BUDGET;
    assert_eq!(
        *store.snapshot(),
        Tally {
            total: expected_applied,
            applied: expected_applied
        },
        "重入 action 只入队、由最外层排空环处理：既不丢也不重复"
    );
    assert_eq!(
        hits.load(Ordering::Relaxed),
        expected_applied,
        "每拍一次通知"
    );
    assert_eq!(store.pending_actions(), 0);
    assert_eq!(store.feedback_budget_exceeded(), 0, "未触及回灌预算点");
    assert_eq!(store.reducer_panic_count(), 0);
}

// ── 差分：生产载体 vs `Mutex<T>` 对照载体 ───────────────────────────────────

/// 可观察面（两实现必须逐项一致）：状态终值、通知触达、panic 计数、队列余量、
/// 订阅数，以及「效果缓冲已清空」的间接证明（空 action 只推进一拍）。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Observables {
    total: u64,
    applied: u64,
    notified: u64,
    subscriber_panics: u64,
    reducer_panics: u64,
    subscribers: usize,
    pending: usize,
    noop_applied_delta: u64,
}

fn observe(store: &TestStore, notified: u64) -> Observables {
    let before = store.snapshot().applied;
    // 空 action：若效果缓冲残留未转发的效果，本拍会多 reduce 若干次。
    store.dispatch(Cmd::Add(0));
    let snap = store.snapshot();
    Observables {
        total: snap.total,
        applied: before,
        notified,
        subscriber_panics: store.subscriber_panics(),
        reducer_panics: store.reducer_panic_count(),
        subscribers: store.subscriber_count(),
        pending: store.pending_actions(),
        noop_applied_delta: snap.applied - before,
    }
}

/// 确定性序列（含 Emit 回灌、reducer panic、订阅注销后的冷重建）。
fn run_sequence(store: &TestStore) -> Observables {
    let hits = Arc::new(AtomicU64::new(0));
    let permanent = store.subscribe(counting_subscriber(&hits));
    let boom = store.subscribe(|_| panic!("differential subscriber boom"));

    store.dispatch(Cmd::Add(1));
    store.dispatch(Cmd::AddAndEmit(2));
    let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(Cmd::EmitThenPanic(5))));
    store.dispatch(Cmd::Add(3));
    drop(boom);
    drop(permanent);
    let notified = hits.load(Ordering::Relaxed);
    store.dispatch(Cmd::Add(1));
    observe(store, notified)
}

/// 并发序列（同一派发脚本，两实现各自的计数必须一致）。
fn run_concurrent(store: &Arc<TestStore>) -> Observables {
    const THREADS: usize = 3;
    const PER_THREAD: u64 = 60;
    let hits = Arc::new(AtomicU64::new(0));
    let ticket = store.subscribe(counting_subscriber(&hits));

    let barrier = Arc::new(Barrier::new(THREADS));
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let store = Arc::clone(store);
        let barrier = Arc::clone(&barrier);
        let done_tx = done_tx.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..PER_THREAD {
                if (i + t as u64).is_multiple_of(2) {
                    store.dispatch(Cmd::Add(1));
                } else {
                    store.dispatch(Cmd::AddAndEmit(2));
                }
            }
            let _ = done_tx.send(());
        }));
    }
    drop(barrier);
    drop(done_tx);

    expect_within(&done_rx, THREADS, "差分并发序列");
    for handle in handles {
        handle.join().expect("派发线程不得 panic");
    }
    let notified = hits.load(Ordering::Relaxed);
    drop(ticket);
    observe(store, notified)
}

/// 同一 dispatch 序列跑在两实现上：可观察面逐项相等，且等于契约期望值
/// （常量同时兜住「两实现一起错」的情形）。
#[test]
fn mutex_reference_and_production_cells_observe_identically() {
    let prod = run_sequence(&TestStore::new(TallyReducer, NoFx));
    let refr = run_sequence(&TestStore::new_mutex_reference(TallyReducer, NoFx));
    assert_eq!(prod, refr, "确定性序列：两实现可观察面必须逐项一致");
    // 契约期望值（手算）：total = 1 + 2 + 2(回灌) + 0(panic 拍) + 3 + 5(残留续转)
    // + 1 = 14；reduce 拍数 = Add1/Add2/回灌Add2/Add3/回灌Add5/Add1 = 6，其中
    // 最后一拍发生在订阅注销后故不计通知 ⇒ notified = 5。
    let expected = Observables {
        total: 14,
        applied: 6,
        notified: 5,
        subscriber_panics: 1,
        reducer_panics: 1,
        subscribers: 0,
        pending: 0,
        noop_applied_delta: 1,
    };
    assert_eq!(prod, expected, "生产载体必须命中契约期望值");
    assert_eq!(refr, expected, "对照载体必须命中同一契约期望值");

    let prod = run_concurrent(&Arc::new(TestStore::new(TallyReducer, NoFx)));
    let refr = run_concurrent(&Arc::new(TestStore::new_mutex_reference(
        TallyReducer,
        NoFx,
    )));
    assert_eq!(prod, refr, "并发序列：两实现可观察面必须逐项一致");
    // 3 线程 × 60 次：每线程 30 Add(1) + 30 AddAndEmit(2) + 30 回灌 Add(2)。
    assert_eq!(prod.total, 3 * 30 * 5);
    assert_eq!(prod.applied, 3 * 30 * 3);
    assert_eq!(prod.notified, prod.applied);
    assert_eq!(prod.noop_applied_delta, 1, "效果缓冲已清空");
}

/// 对照载体的 panic 恢复语义同样自洽：旗/锁都不残留，实例可继续复用。
#[test]
fn reference_store_keeps_same_public_semantics_under_panics() {
    let reference = TestStore::new_mutex_reference(TallyReducer, NoFx);
    let failed = catch_unwind(AssertUnwindSafe(|| reference.dispatch(Cmd::Panic)));
    assert!(failed.is_err());
    assert_eq!(reference.reducer_panic_count(), 1);
    reference.dispatch(Cmd::Add(2));
    assert_eq!(
        *reference.snapshot(),
        Tally {
            total: 2,
            applied: 1
        },
        "对照载体的 panic 恢复语义与生产载体一致（独占锁不残留）"
    );
    assert_eq!(reference.pending_actions(), 0);
}
