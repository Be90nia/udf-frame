//! `skeleton::lint` 语义 / 并发 / panic 行为测试。
//!
//! 隔离纪律：**每个测试用私有 newtype 作 `C`**——注册表按 `TypeId` 分槽，不同测试
//! 的槽互不可见，故 libtest 并行跑不串味（同类型内多段场景靠 `clear_stages` 清场）。
//! 这条纪律本身是 armor 的一部分（全局可变状态 + 并行测试线程 = flaky）。
//!
//! 死锁不表现为挂死：凡「锁内跑用户代码就会死锁」的场景（阶段体内注册 / 阶段 `Drop`
//! 内重入 / 清场与在飞快照相遇）都用 `mpsc::recv_timeout(5s)` 收口——持锁跑用户代码
//! 会超时判败而不是把整跑拖死。

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use skeleton::lint::{
    clear_stages, post_process, register_stage, registered_stages, LintError, LintStage,
};

/// 测试替身阶段的闭包面（复杂类型起别名，避免类型膨胀）。
type StageRun<C> = Box<dyn Fn(&mut C) -> Result<(), LintError> + Send + Sync>;

/// 测试替身阶段：名字 + 闭包（省去每个测试手写一个 `impl`；真实采用方直接实现 trait）。
struct FnStage<C> {
    name: &'static str,
    run: StageRun<C>,
}

impl<C: 'static> FnStage<C> {
    fn arc(
        name: &'static str,
        run: impl Fn(&mut C) -> Result<(), LintError> + Send + Sync + 'static,
    ) -> Arc<dyn LintStage<C>> {
        Arc::new(Self {
            name,
            run: Box::new(run),
        })
    }
}

impl<C> LintStage<C> for FnStage<C> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn run(&self, cfg: &mut C) -> Result<(), LintError> {
        (self.run)(cfg)
    }
}

// ═══ 语义 1：空表 → Ok ═══

#[derive(Default)]
struct EmptyCfg {
    runs: u32,
}

#[test]
fn empty_registry_is_ok() {
    clear_stages::<EmptyCfg>();
    let mut cfg = EmptyCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 0);
    assert!(registered_stages::<EmptyCfg>().is_empty());
}

// ═══ 语义 2：按注册序执行；名单同序；重复名原样 ═══

#[derive(Default)]
struct OrderCfg {
    trace: Vec<&'static str>,
}

#[test]
fn stages_run_in_registration_order() {
    clear_stages::<OrderCfg>();
    for name in ["a", "b", "c"] {
        register_stage::<OrderCfg>(FnStage::arc(name, |cfg: &mut OrderCfg| {
            cfg.trace.push(name);
            Ok(())
        }));
    }
    let mut cfg = OrderCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.trace, ["a", "b", "c"]);
    assert_eq!(registered_stages::<OrderCfg>(), ["a", "b", "c"]);
}

#[derive(Default)]
struct DupNameCfg {
    runs: u32,
}

#[test]
fn duplicate_names_are_reported_verbatim() {
    clear_stages::<DupNameCfg>();
    register_stage::<DupNameCfg>(FnStage::arc("same", |cfg: &mut DupNameCfg| {
        cfg.runs += 1;
        Ok(())
    }));
    register_stage::<DupNameCfg>(FnStage::arc("same", |cfg: &mut DupNameCfg| {
        cfg.runs += 1;
        Ok(())
    }));
    let mut cfg = DupNameCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 2); // 无去重：同一阶段登记两次就执行两次
    assert_eq!(registered_stages::<DupNameCfg>(), ["same", "same"]);
}

// ═══ 语义 3：短路（后续不跑，已发生的改动保留） ═══

#[derive(Default)]
struct ShortCircuitCfg {
    trace: Vec<&'static str>,
}

#[test]
fn first_error_short_circuits_and_keeps_prior_mutations() {
    clear_stages::<ShortCircuitCfg>();
    register_stage::<ShortCircuitCfg>(FnStage::arc("ok", |cfg: &mut ShortCircuitCfg| {
        cfg.trace.push("ok");
        Ok(())
    }));
    register_stage::<ShortCircuitCfg>(FnStage::arc("bad", |cfg: &mut ShortCircuitCfg| {
        cfg.trace.push("bad");
        Err(LintError::new("boom"))
    }));
    register_stage::<ShortCircuitCfg>(FnStage::arc("never", |cfg: &mut ShortCircuitCfg| {
        cfg.trace.push("never");
        Ok(())
    }));

    let mut cfg = ShortCircuitCfg::default();
    let err = post_process(&mut cfg).unwrap_err();
    assert_eq!(cfg.trace, ["ok", "bad"]); // 第三个没跑，第一个的改动留着
    assert_eq!(err.message(), "boom");
}

// ═══ 语义 4：阶段可改写配置 ═══

#[derive(Default)]
struct MutateCfg {
    tag: String,
    limit: u32,
}

#[test]
fn stages_can_rewrite_config() {
    clear_stages::<MutateCfg>();
    register_stage::<MutateCfg>(FnStage::arc("rewrite", |cfg: &mut MutateCfg| {
        cfg.tag = "tuned".into();
        cfg.limit = 7;
        Ok(())
    }));
    let mut cfg = MutateCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!((cfg.tag.as_str(), cfg.limit), ("tuned", 7));
}

// ═══ 语义 5：类型隔离 ═══

#[derive(Default)]
struct IsoA {
    runs: u32,
}

#[derive(Default)]
struct IsoB {
    runs: u32,
}

#[test]
fn registries_are_isolated_per_config_type() {
    clear_stages::<IsoA>();
    clear_stages::<IsoB>();
    register_stage::<IsoA>(FnStage::arc("only-a", |cfg: &mut IsoA| {
        cfg.runs += 1;
        Ok(())
    }));

    let mut a = IsoA::default();
    let mut b = IsoB::default();
    assert_eq!(post_process(&mut a), Ok(()));
    assert_eq!(post_process(&mut b), Ok(()));
    assert_eq!((a.runs, b.runs), (1, 0));
    assert_eq!(registered_stages::<IsoA>(), ["only-a"]);
    assert!(registered_stages::<IsoB>().is_empty());
}

// ═══ 语义 6：错误归属（编排器补 / 已带归属不覆盖） ═══

struct AttrCfg;

#[test]
fn orchestrator_fills_missing_stage_attribution() {
    clear_stages::<AttrCfg>();
    register_stage::<AttrCfg>(FnStage::arc("attr-stage", |_: &mut AttrCfg| {
        Err(LintError::new("x"))
    }));
    let err = post_process(&mut AttrCfg).unwrap_err();
    assert_eq!(err.stage(), Some("attr-stage"));
    assert_eq!(err.to_string(), "attr-stage: x");
}

struct OuterNestedCfg;

struct InnerNestedCfg;

#[test]
fn nested_error_keeps_its_own_stage_attribution() {
    clear_stages::<InnerNestedCfg>();
    clear_stages::<OuterNestedCfg>();
    register_stage::<InnerNestedCfg>(FnStage::arc("inner-bad", |_: &mut InnerNestedCfg| {
        Err(LintError::new("deep"))
    }));
    register_stage::<OuterNestedCfg>(FnStage::arc("outer-wraps", |_: &mut OuterNestedCfg| {
        post_process(&mut InnerNestedCfg)
    }));

    let err = post_process(&mut OuterNestedCfg).unwrap_err();
    // 内层编排器已填好归属，外层不得覆盖（否则外层替内层的错背锅）。
    assert_eq!(err.stage(), Some("inner-bad"));
    assert_eq!(err.to_string(), "inner-bad: deep");
}

// ═══ 增补：阶段体内注册（快照语义）+ 递归 post_process ═══

#[derive(Default)]
struct ReentrantCfg {
    trace: Vec<&'static str>,
}

#[test]
fn reentrant_registration_is_snapshot_scoped() {
    clear_stages::<ReentrantCfg>();
    // 阶段 A 在自己体内注册 B：若 post_process 持锁跑阶段，这里立刻死锁。
    register_stage::<ReentrantCfg>(FnStage::arc("a", |cfg: &mut ReentrantCfg| {
        cfg.trace.push("a");
        register_stage::<ReentrantCfg>(FnStage::arc("b", |cfg: &mut ReentrantCfg| {
            cfg.trace.push("b");
            Ok(())
        }));
        Ok(())
    }));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut cfg = ReentrantCfg::default();
        let first = post_process(&mut cfg);
        let after_first = cfg.trace.clone();
        let second = post_process(&mut cfg);
        let after_second = cfg.trace.clone();
        tx.send((first, after_first, second, after_second))
            .expect("主线程必须在等这一拍");
    });
    // 死锁必须表现为失败而非挂死（5s 上限）。
    let (first, after_first, second, after_second) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("阶段体内注册不得死锁：5s 内必须返回");
    handle.join().expect("执行线程不得 panic");

    assert_eq!(first, Ok(()));
    assert_eq!(after_first, ["a"]); // 本拍快照只有 A：B 下一拍才生效
    assert_eq!(second, Ok(()));
    assert_eq!(after_second, ["a", "a", "b"]); // 第二拍：A 又跑一次 + B 首次执行
    let names = registered_stages::<ReentrantCfg>();
    assert_eq!(&names[..2], ["a", "b"]); // A 每拍都登记一次 B（重复名允许，仅诊断）
}

#[derive(Default)]
struct OuterCfg {
    inner_note: Option<&'static str>,
}

#[derive(Default)]
struct InnerCfg {
    touched: bool,
}

#[test]
fn stage_can_recursively_post_process_other_types() {
    clear_stages::<OuterCfg>();
    clear_stages::<InnerCfg>();
    register_stage::<InnerCfg>(FnStage::arc("inner", |cfg: &mut InnerCfg| {
        cfg.touched = true;
        Ok(())
    }));
    register_stage::<OuterCfg>(FnStage::arc("outer", |cfg: &mut OuterCfg| {
        let mut inner = InnerCfg::default();
        post_process(&mut inner)?;
        cfg.inner_note = inner.touched.then_some("inner ran");
        Ok(())
    }));

    let mut outer = OuterCfg::default();
    assert_eq!(post_process(&mut outer), Ok(()));
    assert_eq!(outer.inner_note, Some("inner ran"));
}

// ═══ 增补：panic 传播 + 注册表存活 ═══

#[derive(Default)]
struct PanicCfg {
    runs: u32,
}

#[test]
fn stage_panic_propagates_and_registry_survives() {
    clear_stages::<PanicCfg>();
    register_stage::<PanicCfg>(FnStage::arc("boom", |_: &mut PanicCfg| {
        panic!("stage panic")
    }));

    let mut cfg = PanicCfg::default();
    let caught = catch_unwind(AssertUnwindSafe(|| post_process(&mut cfg)));
    assert!(
        caught.is_err(),
        "阶段 panic 必须直接传播（不 catch_unwind、不吞）"
    );

    // 锁不在执行期持有 → panic 不毒化注册表；清场后照常工作。
    clear_stages::<PanicCfg>();
    register_stage::<PanicCfg>(FnStage::arc("good", |cfg: &mut PanicCfg| {
        cfg.runs += 1;
        Ok(())
    }));
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 1);
    assert_eq!(registered_stages::<PanicCfg>(), ["good"]);
}

// ═══ 增补：并发（不同 C 实例并发 post_process / 阶段在锁外执行 / 并发注册） ═══

#[derive(Default)]
struct ConcurrentCfg {
    hits: u32,
}

#[test]
fn concurrent_post_process_over_distinct_configs() {
    clear_stages::<ConcurrentCfg>();
    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    register_stage::<ConcurrentCfg>(FnStage::arc("count", move |cfg: &mut ConcurrentCfg| {
        cfg.hits += 1;
        counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }));

    let handles: Vec<_> = (0..4)
        .map(|_| {
            thread::spawn(|| {
                for _ in 0..200 {
                    let mut cfg = ConcurrentCfg::default();
                    post_process(&mut cfg).expect("并发 post_process 必须全成功");
                    assert_eq!(cfg.hits, 1);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("并发线程不得 panic");
    }
    assert_eq!(runs.load(Ordering::Relaxed), 800);
}

#[derive(Default)]
struct OverlapCfg {
    hits: u32,
}

/// 证的是**读侧不被独占锁串行化**（`RwLock` 读锁共享）——不是「阶段在锁外执行」：
/// 读锁即便持在快照期，两线程照样凑得齐 barrier。锁外执行的鉴别证据是
/// `reentrant_registration_is_snapshot_scoped` 与 `stage_drop_can_reenter_registry`
/// （体内要拿锁，锁内执行必自死锁）。
#[test]
fn stages_are_not_serialized_by_an_exclusive_lock() {
    clear_stages::<OverlapCfg>();
    let barrier = Arc::new(Barrier::new(2));
    let inside = barrier.clone();
    register_stage::<OverlapCfg>(FnStage::arc("overlap", move |cfg: &mut OverlapCfg| {
        cfg.hits += 1;
        // 两线程必须在阶段体内同时在场：编排器若用独占锁（Mutex 或 RwLock 写侧）包住
        // 执行期，这里永远凑不齐。
        inside.wait();
        Ok(())
    }));

    let (tx, rx) = mpsc::channel();
    for _ in 0..2 {
        let tx = tx.clone();
        thread::spawn(move || {
            let mut cfg = OverlapCfg::default();
            tx.send(post_process(&mut cfg))
                .expect("主线程必须在等这两拍");
        });
    }
    drop(tx);
    for _ in 0..2 {
        let result = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("阶段必须在锁外执行：5s 内两线程都要进场");
        assert_eq!(result, Ok(()));
    }
}

#[derive(Default)]
struct RegisterCfg {
    runs: u32,
}

static REGISTER_NAMES: [&str; 8] = ["s0", "s1", "s2", "s3", "s4", "s5", "s6", "s7"];

#[test]
fn concurrent_registration_keeps_every_stage() {
    clear_stages::<RegisterCfg>();
    let handles: Vec<_> = (0..8)
        .map(|index| {
            thread::spawn(move || {
                let name = REGISTER_NAMES[index];
                register_stage::<RegisterCfg>(FnStage::arc(name, |cfg: &mut RegisterCfg| {
                    cfg.runs += 1;
                    Ok(())
                }));
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("注册线程不得 panic");
    }

    let mut names = registered_stages::<RegisterCfg>();
    assert_eq!(names.len(), 8, "并发注册不得丢阶段");
    names.sort_unstable();
    assert_eq!(names, REGISTER_NAMES);

    let mut cfg = RegisterCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 8);
}

// ═══ 增补：clear_stages 后名单空且不再跑 ═══

#[derive(Default)]
struct ClearCfg {
    runs: u32,
}

#[test]
fn clear_stages_empties_registry_and_stops_running() {
    clear_stages::<ClearCfg>();
    register_stage::<ClearCfg>(FnStage::arc("gone", |cfg: &mut ClearCfg| {
        cfg.runs += 1;
        Ok(())
    }));
    let mut cfg = ClearCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 1);

    clear_stages::<ClearCfg>();
    assert!(registered_stages::<ClearCfg>().is_empty());
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 1); // 不再跑阶段
    clear_stages::<ClearCfg>(); // 幂等：无表可清也不 panic
}

// ═══ 增补：同类型递归（合法但本件不防，终止条件由阶段自设） ═══

#[derive(Default)]
struct RecursiveCfg {
    depth: u32,
    trace: Vec<&'static str>,
}

#[test]
fn same_type_recursion_is_allowed_and_self_limited() {
    clear_stages::<RecursiveCfg>();
    // 体内 post_process::<C> 合法（锁不在手），但会重跑本阶段本身：此处靠 cfg.depth
    // 自设终止条件（本件不做防护，见 LintStage::run doc）。
    register_stage::<RecursiveCfg>(FnStage::arc("recurse", |cfg: &mut RecursiveCfg| {
        cfg.depth += 1;
        cfg.trace.push("nested");
        if cfg.depth < 2 {
            post_process(cfg)?;
        }
        Ok(())
    }));

    let mut cfg = RecursiveCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.depth, 2, "递归一层后由阶段自设条件收住");
    assert_eq!(cfg.trace, ["nested", "nested"]);
    clear_stages::<RecursiveCfg>();
}

// ═══ 增补：clear_stages 的析构面（锁外析构 / 析构 panic / 在飞快照） ═══

#[derive(Default)]
struct DropReentryCfg;

/// `Drop` 体内重入注册表：若槽位析构发生在写锁内，同线程重入 `RwLock` 写锁即自死锁。
struct ReentrantDropStage {
    reentered: Arc<AtomicUsize>,
}

impl LintStage<DropReentryCfg> for ReentrantDropStage {
    fn name(&self) -> &'static str {
        "reentrant-drop"
    }

    fn run(&self, _cfg: &mut DropReentryCfg) -> Result<(), LintError> {
        Ok(())
    }
}

impl Drop for ReentrantDropStage {
    fn drop(&mut self) {
        // 写路径（register_stage）+ 读路径（post_process）都得能进。
        register_stage::<DropReentryCfg>(FnStage::arc("born-in-drop", |_: &mut DropReentryCfg| {
            Ok(())
        }));
        let mut cfg = DropReentryCfg;
        post_process(&mut cfg).expect("析构内 post_process 必须成功");
        self.reentered.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn stage_drop_can_reenter_registry() {
    clear_stages::<DropReentryCfg>();
    let reentered = Arc::new(AtomicUsize::new(0));
    // 阶段对象唯一强引用在注册表里：clear_stages 移除槽位 → 它析构。
    register_stage::<DropReentryCfg>(Arc::new(ReentrantDropStage {
        reentered: Arc::clone(&reentered),
    }));

    // 在独立线程里清场：锁内析构 → 自死锁 → 5s 超时判败，而不是拖死整跑。
    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        clear_stages::<DropReentryCfg>();
        tx.send(()).expect("主线程必须在等这一拍");
    });
    rx.recv_timeout(Duration::from_secs(5))
        .expect("阶段 Drop 必须在锁外执行：5s 内必须返回");
    handle.join().expect("清场线程不得 panic");

    assert_eq!(
        reentered.load(Ordering::Relaxed),
        1,
        "阶段 Drop 必须真的跑到"
    );
    // Drop 内注册的那条生效：证明重入的写路径真的成功，而不只是「没死锁」。
    assert_eq!(registered_stages::<DropReentryCfg>(), ["born-in-drop"]);
    clear_stages::<DropReentryCfg>();
}

#[derive(Default)]
struct DropPanicCfg {
    runs: u32,
}

struct PanickingDropStage;

impl LintStage<DropPanicCfg> for PanickingDropStage {
    fn name(&self) -> &'static str {
        "panicking-drop"
    }

    fn run(&self, _cfg: &mut DropPanicCfg) -> Result<(), LintError> {
        Ok(())
    }
}

impl Drop for PanickingDropStage {
    fn drop(&mut self) {
        panic!("阶段 Drop 故意 panic");
    }
}

#[test]
fn stage_drop_panic_leaves_registry_usable() {
    clear_stages::<DropPanicCfg>();
    // 登记项先一步脱离注册表，随后 drop 阶段对象 → panic 从 clear_stages 冒出。
    register_stage::<DropPanicCfg>(Arc::new(PanickingDropStage));
    let caught = catch_unwind(clear_stages::<DropPanicCfg>);
    assert!(caught.is_err(), "阶段 Drop 的 panic 必须原样传播（不吞）");

    // 注册表不因析构 panic 报废：后续读写照常。
    assert!(registered_stages::<DropPanicCfg>().is_empty());
    register_stage::<DropPanicCfg>(FnStage::arc(
        "after-drop-panic",
        |cfg: &mut DropPanicCfg| {
            cfg.runs += 1;
            Ok(())
        },
    ));
    let mut cfg = DropPanicCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 1);
    assert_eq!(registered_stages::<DropPanicCfg>(), ["after-drop-panic"]);
    clear_stages::<DropPanicCfg>();
}

#[derive(Default)]
struct ClearInFlightCfg {
    runs: u32,
}

#[test]
fn clear_stages_does_not_cancel_the_in_flight_tick() {
    clear_stages::<ClearInFlightCfg>();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    // `Receiver` 不是 `Sync`，阶段要 `Send + Sync`，故套一层 `Mutex`（只有执行线程会锁它）。
    let release_rx = Arc::new(Mutex::new(release_rx));
    register_stage::<ClearInFlightCfg>(FnStage::arc(
        "inflight",
        move |cfg: &mut ClearInFlightCfg| {
            cfg.runs += 1;
            entered_tx.send(()).expect("主线程必须在等入场");
            // 等主线程清场完再返回：本拍跑的是清场前那份快照。
            release_rx
                .lock()
                .expect("放行闸不得中毒")
                .recv_timeout(Duration::from_secs(5))
                .expect("主线程必须 5s 内放行在飞那一拍");
            Ok(())
        },
    ));

    let (tx, rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let mut cfg = ClearInFlightCfg::default();
        let result = post_process(&mut cfg);
        tx.send((result, cfg.runs)).expect("主线程必须在等这一拍");
    });

    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("阶段必须在 5s 内进到场（快照已在手）");
    clear_stages::<ClearInFlightCfg>();
    assert!(
        registered_stages::<ClearInFlightCfg>().is_empty(),
        "清场后名单必须空"
    );
    release_tx.send(()).expect("在飞那拍必须在等放行");

    let (result, runs) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("在飞那拍必须 5s 内跑完");
    handle.join().expect("执行线程不得 panic");
    assert_eq!(result, Ok(()), "clear_stages 不得取消在飞那一拍");
    assert_eq!(runs, 1, "在飞快照里的阶段必须跑完（且只跑一次）");

    // 下一拍不再执行该阶段。
    let mut cfg = ClearInFlightCfg::default();
    assert_eq!(post_process(&mut cfg), Ok(()));
    assert_eq!(cfg.runs, 0);
}
