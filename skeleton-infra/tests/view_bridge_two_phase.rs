#![cfg(all(feature = "view", feature = "runtime"))]
//! #140 公共面验收：ViewBridge 的采用方回调（`JsonEncoder::to_json`、
//! `RegionEncoder::encode`、`RegionBacking::with_bytes_mut`、`M::View` 的
//! `PartialEq`/`Clone`/`Drop`）**不得在桥内部锁内执行**，以及两阶段提交的代次语义
//! （脏位只清捕获到的那一代 / 编码器 panic 不 poison 桥 / 区域单写者写序不回退）。
//!
//! 锁边界的直接证据（回调里对 `notify.core`/`notify.region` `try_lock`）在
//! `src/view.rs` 的单元探针 `bridge_lock_boundary` 里（集成测试拿不到私有字段）。
//! 本文件用**自死锁探测**做公共面判别力：旧实现（回调在锁内）下，回调里重入桥的
//! 公开面会挂死 ⇒ 有界失败；新实现下必须正常返回。时序一律握手 + OS 时钟截止，
//! 不靠 sleep 猜时序。

use std::panic;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier, OnceLock, Weak};
use std::thread;
use std::time::Duration;

use serde_json::{Value, json};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    Coverage, Encoders, IntentError, JsonEncoder, RegionBacking, RegionEncoder, RegionFailure,
    RegionWriter, SeqError, VecBacking, ViewBridge,
};

/// OS 时钟护栏：回调里重入桥的公开面，旧实现（持锁）会有界失败而非挂死。
const LIMIT: Duration = Duration::from_secs(15);

/// 用例护栏：探针体在独立线程执行，死锁表现为有界 panic。
fn bounded<T: Send + 'static>(probe: impl FnOnce() -> T + Send + 'static) -> T {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(probe());
    });
    match rx.recv_timeout(LIMIT) {
        Ok(value) => value,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("采用方回调疑似在桥锁内执行并重入（OS 时钟截止 {LIMIT:?}）")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("探针线程在回报前 panic"),
    }
}

// ── 应用底座 ──────────────────────────────────────────────────────────────────

#[derive(Clone, Default)]
struct App {
    count: u32,
    other: u32,
}

/// 动作载荷 = 状态增量（两个字段同步前进，任一绑定都在变）。
struct AppReducer;

impl Reducer for AppReducer {
    type State = App;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> App {
        App::default()
    }
    fn reduce(&self, s: &App, a: u32, _: &mut EffectBuf<u32, Infallible>) -> App {
        App {
            count: s.count + a,
            other: s.other + a,
        }
    }
}

struct Intent;

impl IntentMap<u32> for Intent {
    type Wire = Value;
    type Err = IntentError;
    fn intent(&self, w: Value) -> Result<u32, IntentError> {
        w.as_u64()
            .map(|n| n as u32)
            .ok_or_else(|| IntentError::malformed("expect number"))
    }
}

// ── 探针钩子：计数 + （恰一次）重入桥 ────────────────────────────────────────

/// 重入面（`dyn` 擦除：钩子先于桥存在，桥建好后再注入）。
trait Reenter: Send + Sync {
    fn reenter(&self, site: &'static str);
}

/// 探针臂位：哪个回调里重入/panic。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    None,
    PartialEq,
    RetiredDrop,
    ToJson,
    ToJsonDispatch,
}

impl Arm {
    fn code(self) -> u8 {
        match self {
            Self::None => 0,
            Self::PartialEq => 1,
            Self::RetiredDrop => 2,
            Self::ToJson => 3,
            Self::ToJsonDispatch => 4,
        }
    }
    fn from(code: u8) -> Self {
        match code {
            1 => Self::PartialEq,
            2 => Self::RetiredDrop,
            3 => Self::ToJson,
            4 => Self::ToJsonDispatch,
            _ => Self::None,
        }
    }
}

#[derive(Default)]
struct Hook {
    arm: AtomicU64,
    /// 「恰一次」自清旗：重入一次后立刻失效，防无限递归。
    spent: AtomicBool,
    /// 回指桥（`Weak`：桥先于视图析构，析构尾段的重入自动跳过，不留 Arc 环）。
    reentry: OnceLock<Weak<dyn Reenter>>,
    eqs: AtomicUsize,
    clones: AtomicUsize,
    drops: AtomicUsize,
    json_covers: AtomicUsize,
    region_covers: AtomicUsize,
    boom_to_json: AtomicBool,
    /// 注入 `View::eq` panic（P0-a）。
    boom_eq: AtomicBool,
    /// 注入退役 `View::drop` panic（P0-b）。
    boom_drop: AtomicBool,
    /// `project` 抖动：每 N 次 panic 一次（0 = 不抖，D1 守卫用）。
    boom_every: AtomicUsize,
    project_calls: AtomicUsize,
    /// `to_json` 里握手卡点（P1 并发拉取用）。
    block_json: AtomicBool,
    gate_in: OnceLock<Arc<Barrier>>,
    gate_out: OnceLock<Arc<Barrier>>,
}

impl Hook {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            spent: AtomicBool::new(false),
            ..Self::default()
        })
    }

    fn arm(&self, arm: Arm) {
        self.spent.store(false, Ordering::SeqCst);
        self.arm.store(arm.code() as u64, Ordering::SeqCst);
    }

    /// 若本次臂位匹配且尚未用过 ⇒ 执行一次重入（钩子未注入则视为已用过）。
    fn fire(&self, site: Arm, label: &'static str) {
        if Arm::from(self.arm.load(Ordering::SeqCst) as u8) != site {
            return;
        }
        if self.spent.swap(true, Ordering::SeqCst) {
            return;
        }
        if let Some(reenter) = self.reentry.get().and_then(Weak::upgrade) {
            reenter.reenter(label);
        }
    }
}

/// 视图值：`PartialEq` / `Clone` / `Drop` 全部是**采用方代码**。
struct V {
    id: u32,
    hook: Arc<Hook>,
}

impl PartialEq for V {
    fn eq(&self, other: &Self) -> bool {
        self.hook.eqs.fetch_add(1, Ordering::SeqCst);
        self.hook.fire(Arm::PartialEq, "View::eq");
        assert!(
            !self.hook.boom_eq.load(Ordering::SeqCst),
            "injected View::eq panic"
        );
        self.id == other.id
    }
}

impl Clone for V {
    fn clone(&self) -> Self {
        self.hook.clones.fetch_add(1, Ordering::SeqCst);
        Self {
            id: self.id,
            hook: Arc::clone(&self.hook),
        }
    }
}

impl Drop for V {
    fn drop(&mut self) {
        self.hook.drops.fetch_add(1, Ordering::SeqCst);
        self.hook.fire(Arm::RetiredDrop, "View::drop");
        assert!(
            !self.hook.boom_drop.load(Ordering::SeqCst),
            "injected View::drop panic"
        );
    }
}

// ── 映射与编码器 ─────────────────────────────────────────────────────────────

static DECLS: [BindingDecl; 2] = [
    BindingDecl {
        path: "/count",
        kind: PayloadTag(1),
    },
    BindingDecl {
        path: "/other",
        kind: PayloadTag(1),
    },
];

struct ProbeMap {
    hook: Arc<Hook>,
    /// `true` ⇒ 哨兵恒报未变（复现「未变更快路径」的分配/锁预算口径）。
    quiet: bool,
}

impl ViewMap<App> for ProbeMap {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        &DECLS
    }
    fn input_changed(&self, _: usize, _: &App) -> bool {
        !self.quiet
    }
    fn project(&self, index: usize, s: &App) -> V {
        let nth = self.hook.project_calls.fetch_add(1, Ordering::SeqCst);
        let every = self.hook.boom_every.load(Ordering::SeqCst);
        assert!(
            every == 0 || !nth.is_multiple_of(every),
            "injected projection panic (every {every})"
        );
        V {
            id: if index == 0 { s.count } else { s.other },
            hook: Arc::clone(&self.hook),
        }
    }
}

struct Json {
    hook: Arc<Hook>,
}

impl JsonEncoder for Json {
    type View = V;
    fn covers(&self) -> Coverage {
        self.hook.json_covers.fetch_add(1, Ordering::SeqCst);
        Coverage::All
    }
    fn to_json(&self, v: &V) -> Value {
        self.hook.fire(Arm::ToJson, "to_json");
        self.hook.fire(Arm::ToJsonDispatch, "to_json-dispatch");
        if self.hook.block_json.swap(false, Ordering::SeqCst) {
            if let Some(gate) = self.hook.gate_in.get() {
                gate.wait();
            }
            if let Some(gate) = self.hook.gate_out.get() {
                gate.wait();
            }
        }
        assert!(
            !self.hook.boom_to_json.load(Ordering::SeqCst),
            "injected JsonEncoder::to_json panic"
        );
        json!(v.id)
    }
}

struct Region {
    hook: Arc<Hook>,
}

impl RegionEncoder for Region {
    type View = V;
    fn covers(&self) -> Coverage {
        self.hook.region_covers.fetch_add(1, Ordering::SeqCst);
        Coverage::All
    }
    fn encode(&self, v: &V, w: &mut RegionWriter) {
        w.u64(u64::from(v.id));
    }
}

/// 背衬探针：字节写入口是采用方代码（宿主实现控制同步回调边界）。
struct ProbeBacking {
    inner: VecBacking,
    hook: Arc<Hook>,
    writes: AtomicUsize,
}

impl ProbeBacking {
    fn new(capacity: usize, hook: Arc<Hook>) -> Arc<Self> {
        Arc::new(Self {
            inner: VecBacking::new(capacity),
            hook,
            writes: AtomicUsize::new(0),
        })
    }
}

impl RegionBacking for ProbeBacking {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        self.writes.fetch_add(1, Ordering::SeqCst);
        let _ = &self.hook; // 写入口探针（锁边界见 in-module 探针）
        self.inner.with_bytes_mut(write);
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read);
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        self.inner.store_publish_seq(seq)
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        self.inner.load_publish_seq()
    }
}

type TestBridge = ViewBridge<App, u32, ProbeMap, Intent, Json, Region>;

impl Reenter for TestBridge {
    /// 重入桥的公开面。`to_json` 臂位跑真交付面（`take_patches` + `snapshot_doc`）；
    /// `eq`/`drop` 臂位只做**纯取锁**探针（`bind` 走到 `core` 锁后立刻返回错，无
    /// 副作用），以免重入把本批脏位消费掉、混淆判别力。
    fn reenter(&self, site: &'static str) {
        match site {
            "to_json-dispatch" => {
                self.send_intent(json!(1)).unwrap();
            }
            "to_json" => {
                let batch = self.take_patches();
                assert_eq!(batch.patches.len(), 1, "重入交付本批复读同一代脏位");
                self.snapshot_doc().expect("重入快照");
            }
            _ => {
                let _ = self.bind("/__probe__");
            }
        }
    }
}

/// 建桥并注入重入回指（`Arc` 持有：视图里的回指是 `Weak`，不留环）。
fn build(hook: Arc<Hook>, with_region: bool) -> Arc<TestBridge> {
    build_with(hook, with_region, false)
}

fn build_with(hook: Arc<Hook>, with_region: bool, quiet: bool) -> Arc<TestBridge> {
    let store = SyncStore::new(AppReducer, NoFx);
    let backing = ProbeBacking::new(4096, Arc::clone(&hook));
    let bridge = Arc::new(
        ViewBridge::mount(
            store,
            ProbeMap {
                hook: Arc::clone(&hook),
                quiet,
            },
            Intent,
            Encoders {
                json: Some(Json {
                    hook: Arc::clone(&hook),
                }),
                region: with_region.then(|| {
                    (
                        Region {
                            hook: Arc::clone(&hook),
                        },
                        backing as Arc<dyn RegionBacking>,
                    )
                }),
            },
        )
        .expect("static declarations are well-formed"),
    );
    let erased: Arc<dyn Reenter> = bridge.clone();
    let weak: Weak<dyn Reenter> = Arc::downgrade(&erased);
    assert!(hook.reentry.set(weak).is_ok(), "重入回指只注入一次");
    bridge.bind("/count").unwrap();
    if with_region {
        bridge.bind("/other").unwrap();
    }
    bridge.seal().expect("seal before dispatch");
    bridge
}

// ── T2：锁外跑 `to_json`（自死锁探测） ──────────────────────────────────────

/// `take_patches` 在**锁外**调用 `JsonEncoder::to_json`：编码器里重入
/// `take_patches`/`snapshot_doc` 必须正常返回。旧实现（core 锁内编码）⇒ 自死锁 ⇒
/// 有界失败。
#[test]
fn json_encoder_may_reenter_the_bridge() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        assert!(bridge.send_intent(json!(3)).unwrap());
        hook.arm(Arm::ToJson);
        let batch = bridge.take_patches();
        assert!(hook.spent.load(Ordering::SeqCst), "重入必须先真的发生");
        assert_eq!(
            batch.patches.len(),
            1,
            "重入之后本批仍需交付本次补丁: {:?}",
            batch.patches
        );
        assert_eq!(batch.patches[0].value, json!(3));
    });
}

// ── T3：锁外跑 `PartialEq` 与退役值 `Drop` ──────────────────────────────────

/// `on_notify` 热路径的比较在锁外：`View::eq` 里重入桥必须正常返回（旧实现
/// 在 core 锁内比较 ⇒ 自死锁）。
#[test]
fn view_partial_eq_may_reenter_the_bridge() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        hook.arm(Arm::PartialEq);
        assert!(bridge.send_intent(json!(5)).unwrap());
        assert!(hook.spent.load(Ordering::SeqCst), "重入必须先真的发生");
        assert!(
            hook.eqs.load(Ordering::SeqCst) >= 1,
            "比较确实跑了采用方 eq"
        );
        let batch = bridge.take_patches();
        assert_eq!(batch.patches.len(), 1);
        assert_eq!(batch.patches[0].value, json!(5));
    });
}

/// 退役 `View` 的析构在锁外：`View::drop` 里重入桥必须正常返回（旧实现把旧值
/// 在 core 锁内换出并析构 ⇒ 自死锁）。
#[test]
fn retired_view_drop_may_reenter_the_bridge() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        assert!(bridge.send_intent(json!(2)).unwrap());
        assert_eq!(bridge.take_patches().patches.len(), 1);
        let drops_before = hook.drops.load(Ordering::SeqCst);
        hook.arm(Arm::RetiredDrop);
        // 下一次通知把旧值换出 ⇒ 退役句柄在锁外析构。
        assert!(bridge.send_intent(json!(1)).unwrap());
        assert!(hook.spent.load(Ordering::SeqCst), "重入必须先真的发生");
        assert!(
            hook.drops.load(Ordering::SeqCst) > drops_before,
            "退役值确实被析构"
        );
        let batch = bridge.take_patches();
        assert_eq!(batch.patches.len(), 1);
        assert_eq!(batch.patches[0].value, json!(3));
    });
}

// ── T4：两阶段提交只确认捕获到的那一代脏位 ─────────────────────────────────

/// 编码期间（锁外）再触发一次通知：竞态写入的新脏位**不得**被本批清掉，下一批
/// 必须仍交付它。变异「提交时无条件清脏」⇒ 第二批空 ⇒ 红。
#[test]
fn racing_notification_keeps_its_own_dirty_generation() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        assert!(bridge.send_intent(json!(2)).unwrap());
        // 第一批：编码 `/count` 时从编码器里再派发一拍（值 1 ⇒ count = 3）。
        hook.arm(Arm::ToJsonDispatch);
        let first = bridge.take_patches();
        assert!(hook.spent.load(Ordering::SeqCst), "竞态通知必须先真的发生");
        assert_eq!(first.patches.len(), 1, "本批交付捕获到的那一代: {first:?}");
        assert_eq!(
            first.patches[0].value,
            json!(2),
            "捕获代次的值，不是竞态后的"
        );
        // 竞态那一代的脏位必须留着（否则这次更新永久丢失）。
        let second = bridge.take_patches();
        assert_eq!(
            second.patches.len(),
            1,
            "竞态写入的脏位不得被清: {second:?}"
        );
        assert_eq!(second.patches[0].value, json!(3));
        assert_eq!(second.from_version, first.to_version, "交付游标连续");
        // 再拉一次：没有新变更 ⇒ 空批。
        assert!(bridge.take_patches().patches.is_empty());
    });
}

// ── T6：编码器 panic 的失败协议 ─────────────────────────────────────────────

/// `to_json` panic：桥不得被裸 mutex poison（后续调用照常可用）、本批不推进交付
/// 游标、不丢脏位；取消注入后同一批补丁仍到达。
#[test]
fn json_encoder_panic_keeps_the_bridge_usable() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        assert!(bridge.send_intent(json!(4)).unwrap());
        hook.boom_to_json.store(true, Ordering::SeqCst);
        let failed = bridge.take_patches();
        assert!(failed.patches.is_empty(), "失败批不交付半成品补丁");
        assert!(failed.resync, "编码器 panic 必须经 resync 可见");
        assert_eq!(failed.from_version, failed.to_version, "交付游标不推进");
        // 桥仍可用（旧实现：panic 逃出 take_patches 且 core 被 poison ⇒ 这里 panic）。
        hook.boom_to_json.store(false, Ordering::SeqCst);
        let snap = bridge
            .snapshot_doc()
            .expect("panic 未毒化 mutex ⇒ 快照修复路径照常可用");
        assert_eq!(snap.doc.pointer("/count"), Some(&json!(4)));
        // 毒化由快照修复复位；快照本身即交付点 ⇒ 那一代脏位被全量文档覆盖。
        assert!(
            bridge.take_patches().patches.is_empty(),
            "快照即交付：全量文档已覆盖该代脏位"
        );
        assert!(!bridge.take_patches().resync, "成功快照复位毒化闩");
        assert!(bridge.send_intent(json!(1)).unwrap());
        let after = bridge.take_patches();
        assert_eq!(after.patches.len(), 1, "panic 之后桥照常交付新变更");
        assert_eq!(after.patches[0].value, json!(5));
    });
}

// ── T8：`covers()` 只在装配期求值一次 ──────────────────────────────────────

/// 两个编码器的 `covers()` 都在 mount 期**锁外**各求值一次并缓存：`bind`、一拍
/// dispatch（哨兵变更/不变两种）、`take_patches`、`snapshot_doc` 之后计数不再增长。
#[test]
fn covers_is_evaluated_only_at_mount() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), true);
        assert_eq!(hook.json_covers.load(Ordering::SeqCst), 1, "装配期一次");
        assert_eq!(hook.region_covers.load(Ordering::SeqCst), 1, "装配期一次");
        assert!(bridge.send_intent(json!(1)).unwrap());
        let _ = bridge.take_patches();
        let _ = bridge.snapshot_doc();
        let _ = bridge.reseed_region();
        assert_eq!(
            hook.json_covers.load(Ordering::SeqCst),
            1,
            "运行期零 covers"
        );
        assert_eq!(
            hook.region_covers.load(Ordering::SeqCst),
            1,
            "运行期零 covers"
        );
    });
}

/// 「第二次调用即 panic」的编码器：mount 成功即证明 `covers()` 恰好被调用一次
/// （运行期任何一处再调 ⇒ 桥在后续操作里 panic ⇒ 红）。
#[test]
fn second_covers_call_panics_and_never_happens() {
    bounded(|| {
        struct Once(AtomicUsize);
        impl JsonEncoder for Once {
            type View = V;
            fn covers(&self) -> Coverage {
                assert_eq!(
                    self.0.fetch_add(1, Ordering::SeqCst),
                    0,
                    "covers() 只允许在装配期被调用一次"
                );
                Coverage::All
            }
            fn to_json(&self, v: &V) -> Value {
                json!(v.id)
            }
        }
        struct OnceRegion(AtomicUsize);
        impl RegionEncoder for OnceRegion {
            type View = V;
            fn covers(&self) -> Coverage {
                assert_eq!(
                    self.0.fetch_add(1, Ordering::SeqCst),
                    0,
                    "covers() 只允许在装配期被调用一次"
                );
                Coverage::All
            }
            fn encode(&self, v: &V, w: &mut RegionWriter) {
                w.u64(u64::from(v.id));
            }
        }
        let hook = Hook::new();
        let store = SyncStore::new(AppReducer, NoFx);
        let bridge = ViewBridge::<App, u32, ProbeMap, Intent, Once, OnceRegion>::mount(
            store,
            ProbeMap {
                hook: Arc::clone(&hook),
                quiet: false,
            },
            Intent,
            Encoders {
                json: Some(Once(AtomicUsize::new(0))),
                region: Some((
                    OnceRegion(AtomicUsize::new(0)),
                    ProbeBacking::new(4096, Arc::clone(&hook)) as Arc<dyn RegionBacking>,
                )),
            },
        )
        .expect("covers() 恰一次的编码器可以装配成功");
        bridge.bind("/count").unwrap();
        bridge.seal().unwrap();
        assert!(bridge.send_intent(json!(1)).unwrap());
        let _ = bridge.take_patches();
        let _ = bridge.snapshot_doc();
        let _ = panic::catch_unwind(panic::AssertUnwindSafe(|| bridge.reseed_region()));
    });
}

// ── T7：区域发布的单写者写序不回退 ─────────────────────────────────────────

/// 并发 dispatch + pull：区域序列恒为偶（odd 只在事务内可见）、严格递增、
/// 最终与桥水位同源；读者接受最后一次快照。
#[test]
fn region_publication_stays_serialized() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), true);
        let mut last_seq = bridge.region_version();
        for i in 1..=16u32 {
            assert!(bridge.send_intent(json!(i)).unwrap());
            let seq = bridge.region_version();
            assert_eq!(seq % 2, 0, "已提交序列恒为偶");
            assert!(seq > last_seq, "序列严格递增: {last_seq} → {seq}");
            last_seq = seq;
            let _ = bridge.take_patches();
        }
        assert_eq!(
            bridge.region_version(),
            bridge.take_patches().to_version,
            "区域序列与 JSON 水位同源"
        );
    });
}

// ── T5：bind × 首次通知 × region-full fallback 的锁序状态机 ────────────────

/// 并发 `bind` × 首通知 × **region-full fallback** 不得互锁（#140 复审 D2：真的构造
/// 出三条路径——小背衬让增量几拍后耗尽，每次发布走全量重编；`seal` 与 bind/dispatch
/// 三方在栅栏上同时起跑）。封板后 `bind` 恒 `Sealed`（`core.bound` 互斥两条静态锁序
/// 路径的状态机前提）。
#[test]
fn bind_races_first_notification_with_region_fallback() {
    bounded(|| {
        use skeleton::view::region_layout::{ENTRY_LEN, HEADER_LEN};

        let hook = Hook::new();
        let store = SyncStore::new(AppReducer, NoFx);
        // 初始全量放得下、增量很快耗尽 ⇒ 真正的全量重编（fallback）路径。
        let capacity = HEADER_LEN + 2 * ENTRY_LEN + 160;
        let bridge = Arc::new(
            ViewBridge::mount(
                store,
                ProbeMap {
                    hook: Arc::clone(&hook),
                    quiet: false,
                },
                Intent,
                Encoders {
                    json: Some(Json {
                        hook: Arc::clone(&hook),
                    }),
                    region: Some((
                        Region {
                            hook: Arc::clone(&hook),
                        },
                        ProbeBacking::new(capacity, Arc::clone(&hook)) as Arc<dyn RegionBacking>,
                    )),
                },
            )
            .expect("static declarations are well-formed"),
        );

        // 三段式状态机（#236）：① 准备（两个**不同** binding 已登记；dispatch 与
        // seal 不重叠）② bind × seal 并发（dispatch worker 已退出并被 join ⇒ 无在途
        // 发布）③ sealed 后验证（增量耗尽 ⇒ 全量重编 fallback）。
        //
        // 原用例让 dispatch 洪流与 seal 并发并断言封板必成功——但 `PublishBusy` 的
        // 契约原文是「另一发布者正在进行（reseed_region 与 dispatch 并发 = 宿主
        // 违约）」，而 `seal()` 先置 `core.bound` 再发布初始区域 ⇒ 中间到达的通知
        // 会与它抢发布，输者拿到 `PublishBusy`。合法并发只有 bind × seal（`bind`
        // 不发布）；违约形态由 `concurrent_publisher_gets_typed_publish_busy...`
        // 专测，不再要求封板在宿主违约下成功。
        let bound_mask = Arc::new(AtomicU8::new(0));
        let stop_dispatch = Arc::new(AtomicBool::new(false));
        let sealed = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(3));

        // ① bind worker：封板前持续登记两条**不同**路径（bit0=/count、bit1=/other）。
        // 判据是「两个不同 binding 已登记」，不是成功调用次数（幂等成功会重复计）。
        let binder = {
            let (bridge, bound_mask, sealed, barrier) = (
                Arc::clone(&bridge),
                Arc::clone(&bound_mask),
                Arc::clone(&sealed),
                Arc::clone(&barrier),
            );
            thread::spawn(move || {
                barrier.wait();
                while !sealed.load(Ordering::SeqCst) {
                    if bound_mask.load(Ordering::SeqCst) & 1 == 0 && bridge.bind("/count").is_ok() {
                        bound_mask.fetch_or(1, Ordering::SeqCst);
                    }
                    if bound_mask.load(Ordering::SeqCst) & 2 == 0 && bridge.bind("/other").is_ok() {
                        bound_mask.fetch_or(2, Ordering::SeqCst);
                    }
                }
            })
        };
        // ② dispatch worker：只负责封板前的通知洪流；收到停止信号后必须先从当前
        // `send_intent` 返回并退出（封板后的 12 拍移到协调线程，避免与 seal 抢发布）。
        let dispatcher = {
            let (bridge, stop_dispatch, barrier) = (
                Arc::clone(&bridge),
                Arc::clone(&stop_dispatch),
                Arc::clone(&barrier),
            );
            thread::spawn(move || {
                barrier.wait();
                while !stop_dispatch.load(Ordering::SeqCst) {
                    let _ = bridge.send_intent(json!(1));
                }
            })
        };
        // ③ 协调线程（本线程）：等两个不同 binding 登记 → 停 dispatch → join（join
        // 是「无在途 dispatch publication」的确定性握手，禁 sleep / 固定延时）→
        // 封板（bind worker 仍在并发 ⇒ bind × seal 锁序覆盖保留）→ 置 sealed →
        // 打满 12 拍验证 fallback。
        barrier.wait();
        while bound_mask.load(Ordering::SeqCst) != 0b11 {
            thread::yield_now();
        }
        stop_dispatch.store(true, Ordering::SeqCst);
        dispatcher.join().expect("dispatch worker panicked");
        bridge.seal().expect("initial region publication fits");
        sealed.store(true, Ordering::SeqCst);
        binder.join().expect("bind worker panicked");
        for i in 1..=12u32 {
            assert!(bridge.send_intent(json!(i)).unwrap());
        }
        assert!(
            bridge.region_id() >= 2,
            "增量耗尽 ⇒ 必须真的走到过全量重编（region_id={}）",
            bridge.region_id()
        );
        assert!(
            bridge.region_failure().is_none(),
            "并发窗口内不得有发布失败"
        );
        assert!(
            bridge.bind("/count").is_err(),
            "封板后 bind 必须直接 Sealed（锁序前提）"
        );
        let _ = bridge.take_patches();
    });
}

// ── PublishBusy 的 fail-closed 契约（确定性并发，不靠压力测试碰巧撞上） ────────

/// 发布阻塞背衬：`armed` 后进入 `with_bytes_mut` 时置 `entered` 并等到 `release`
/// ——把「发布在途」窗口做成确定性的（`publish_plan` 已置 `publishing` 才走到这里，
/// 故阻塞期间第二个发布者必然看到争用）。
struct BlockingBacking {
    inner: VecBacking,
    armed: AtomicBool,
    entered: AtomicBool,
    release: AtomicBool,
}

impl RegionBacking for BlockingBacking {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        if self.armed.load(Ordering::SeqCst) {
            self.entered.store(true, Ordering::SeqCst);
            while !self.release.load(Ordering::SeqCst) {
                thread::yield_now();
            }
        }
        self.inner.with_bytes_mut(write);
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read);
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        self.inner.store_publish_seq(seq)
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        self.inner.load_publish_seq()
    }
}

/// 两个发布者重叠：后者必须拿到 typed `PublishBusy`（fail-closed，不死锁、不静默
/// 成功），失败计数 +1；放行前者后其发布正常完成，桥仍可经 `reseed_region` 恢复并
/// 继续处理合法发布。
#[test]
fn concurrent_publisher_gets_typed_publish_busy_and_bridge_stays_usable() {
    bounded(|| {
        let hook = Hook::new();
        let backing = Arc::new(BlockingBacking {
            inner: VecBacking::new(4096),
            armed: AtomicBool::new(false),
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
        });
        let store = SyncStore::new(AppReducer, NoFx);
        let bridge = Arc::new(
            ViewBridge::mount(
                store,
                ProbeMap {
                    hook: Arc::clone(&hook),
                    quiet: false,
                },
                Intent,
                Encoders {
                    json: Some(Json {
                        hook: Arc::clone(&hook),
                    }),
                    region: Some((
                        Region {
                            hook: Arc::clone(&hook),
                        },
                        Arc::clone(&backing) as Arc<dyn RegionBacking>,
                    )),
                },
            )
            .expect("static declarations are well-formed"),
        );
        bridge.bind("/count").unwrap();
        bridge.bind("/other").unwrap();
        bridge.seal().expect("initial region publication fits"); // 未武装：不阻塞

        // ① 发布者 A：进入发布临界区后阻塞（`publishing` 已置位 ⇒ 发布在途）。
        backing.armed.store(true, Ordering::SeqCst);
        let publisher = {
            let bridge = Arc::clone(&bridge);
            thread::spawn(move || bridge.send_intent(json!(1)))
        };
        while !backing.entered.load(Ordering::SeqCst) {
            thread::yield_now();
        }

        // ② 发布者 B：A 未完成时再发布 ⇒ typed PublishBusy（fail-closed）。
        let before = bridge.region_failures();
        let busy = bridge.reseed_region();
        assert!(
            matches!(busy, Err(RegionFailure::PublishBusy)),
            "并发发布者必须 fail-closed 为 PublishBusy：{busy:?}"
        );
        assert_eq!(bridge.region_failures(), before + 1, "失败计数按契约 +1");

        // ③ 放行 A：A 正常完成；桥经显式 `reseed_region` 恢复（契约的恢复口）后
        // 失败槽清空、后续合法发布照常。
        backing.release.store(true, Ordering::SeqCst);
        assert!(publisher.join().expect("publisher panicked").unwrap());
        assert!(
            bridge.reseed_region().is_ok(),
            "违约并发后显式重编必须可恢复"
        );
        assert!(bridge.region_failure().is_none(), "成功发布清空失败槽");
        assert!(
            bridge.send_intent(json!(2)).unwrap(),
            "违约并发之后桥仍处理合法发布"
        );
        let batch = bridge.take_patches();
        // 补丁值 = 投影后的状态（AppReducer：count += 动作）；A 的 +1 与本次 +2 ⇒ 3。
        assert_eq!(
            batch.patches.last().map(|p| p.value.clone()),
            Some(json!(3))
        );
    });
}

// ── P0：采用方 `PartialEq` / 退役 `Drop` 的 panic 必须在桥边界 contain ──────

/// `View::eq` panic：不得穿出 `dispatch`（穿出会被 store 摘订阅 ⇒ 桥永久失联）、
/// 计数 + 毒化（`resync` 可见）、本批不提交；解除注入后后续拍照常投影交付。
#[test]
fn view_eq_panic_is_contained_and_keeps_the_subscription() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        let panics = bridge.projection_panics();
        hook.boom_eq.store(true, Ordering::SeqCst);
        assert!(
            bridge.send_intent(json!(3)).unwrap(),
            "View::eq panic 不得穿出 dispatch"
        );
        hook.boom_eq.store(false, Ordering::SeqCst);
        assert!(
            bridge.projection_panics() > panics,
            "eq panic 必须在 projection_panics 上计数"
        );
        assert!(bridge.take_patches().resync, "eq panic 必须经 resync 可见");
        assert!(bridge.send_intent(json!(1)).unwrap(), "订阅仍在");
        let after = bridge.take_patches();
        assert_eq!(after.patches.len(), 1, "桥未失联: {after:?}");
        assert_eq!(after.patches[0].value, json!(4));
    });
}

/// 退役 `View` 的 `Drop` panic：同样不得穿出 `dispatch`，且该批以可见 `resync` 收口。
#[test]
fn retired_drop_panic_is_contained_and_keeps_the_subscription() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        assert!(bridge.send_intent(json!(2)).unwrap());
        assert_eq!(bridge.take_patches().patches.len(), 1);
        let panics = bridge.projection_panics();
        hook.boom_drop.store(true, Ordering::SeqCst); // 下一拍把旧值换出 ⇒ 析构 panic
        assert!(
            bridge.send_intent(json!(1)).unwrap(),
            "退役值 Drop panic 不得穿出 dispatch"
        );
        hook.boom_drop.store(false, Ordering::SeqCst);
        assert!(
            bridge.projection_panics() > panics,
            "退役值 Drop panic 必须计数"
        );
        assert!(bridge.take_patches().resync, "该批必须以便可见 resync 收口");
        assert!(bridge.send_intent(json!(1)).unwrap(), "订阅仍在");
        let after = bridge.take_patches();
        assert_eq!(after.patches.len(), 1, "桥未失联: {after:?}");
        assert_eq!(after.patches[0].value, json!(4));
    });
}

// ── P1：并发拉取时 `from_version` 单调不回退 ───────────────────────────────

/// 慢拉取（`to_json` 里被握手卡住，捕获 to=4）与快拉取（to=6）交错：慢的那次提交
/// **不得**把更早捕获的水位写回交付游标，否则后续 `from_version` 回退。
/// 变异「改回 `delivered_epoch = to`」⇒ 红。
#[test]
fn concurrent_pulls_keep_from_version_monotonic() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), false);
        let gate_in = Arc::new(Barrier::new(2));
        let gate_out = Arc::new(Barrier::new(2));
        assert!(hook.gate_in.set(Arc::clone(&gate_in)).is_ok());
        assert!(hook.gate_out.set(Arc::clone(&gate_out)).is_ok());

        assert!(bridge.send_intent(json!(1)).unwrap()); // 水位 4
        hook.block_json.store(true, Ordering::SeqCst);
        let slow_bridge = Arc::clone(&bridge);
        // 慢拉取：捕获 to=4，卡在锁外的 to_json 里（此刻未提交游标）。
        let slow = thread::spawn(move || slow_bridge.take_patches());
        gate_in.wait(); // 慢拉取确实进到编码（锁外）了

        assert!(bridge.send_intent(json!(1)).unwrap()); // 水位 6，又置脏
        let fresh = bridge.take_patches(); // to=6，提交游标 6
        assert_eq!(fresh.to_version, 6);

        gate_out.wait(); // 放慢拉取提交
        let slow_batch = slow.join().expect("slow pull panicked");
        assert_eq!(slow_batch.to_version, 4, "慢拉取捕获的是更早的水位");

        let after = bridge.take_patches();
        assert!(
            after.from_version >= fresh.to_version.max(slow_batch.to_version),
            "from_version 不得回退：{} < max({}, {})",
            after.from_version,
            fresh.to_version,
            slow_batch.to_version
        );
        assert!(after.patches.is_empty(), "两代脏位都已交付: {after:?}");
    });
}

// ── D1：快照完整性守卫（判定与捕获同临界区后不可能返回缺叶 Ok） ─────────────

/// `project` 抖动（每 3 次 panic 一次）+ 反复重同步：`Ok` 的文档必须含**每个已绑定
/// 叶子**；不完整只能以 `Err` 显形。判定与捕获合并前，两段取锁之间他人 `repair`
/// 掉槽会让这里出现「Ok 但缺叶」。
#[test]
fn snapshot_never_returns_a_doc_missing_a_bound_leaf() {
    bounded(|| {
        let hook = Hook::new();
        let bridge = build(Arc::clone(&hook), true); // /count + /other
        hook.boom_every.store(3, Ordering::SeqCst);
        for _ in 0..48 {
            let _ = bridge.send_intent(json!(1));
            if let Ok(snap) = bridge.snapshot_doc() {
                assert!(
                    snap.doc.pointer("/count").is_some(),
                    "Ok 快照缺 /count 叶: {:?}",
                    snap.doc
                );
                assert!(
                    snap.doc.pointer("/other").is_some(),
                    "Ok 快照缺 /other 叶: {:?}",
                    snap.doc
                );
            }
        }
    });
}

// ── T9：未变更快路径的分配预算 ─────────────────────────────────────────────

/// 事件计数分配器（形态照 `tests/alloc_site.rs`：lib 测试二进制禁挂全局分配器，
/// 集成测试自带；只计**窗内本线程**的分配，libtest 并发线程的分配不进窗）。
mod alloc_probe {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;
    use std::hint::black_box;

    struct EventCounting;

    // 开关与计数同住 TLS：进程级计数器会被同二进制其它线程的重叠窗污染。
    thread_local! {
        static WINDOW: Cell<(bool, u64, u64)> = const { Cell::new((false, 0, 0)) };
    }

    fn note_event(bytes: usize) {
        let _ = WINDOW.try_with(|c| {
            let (open, n, b) = c.get();
            if open {
                c.set((true, n + 1, b + bytes as u64));
            }
        });
    }

    // SAFETY: 纯转发 System，仅在窗内转发前累加计数；null 原样透传（alloc 契约）。
    unsafe impl GlobalAlloc for EventCounting {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            note_event(layout.size());
            unsafe { System.alloc(layout) }
        }
        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            note_event(layout.size());
            unsafe { System.alloc_zeroed(layout) }
        }
        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            unsafe { System.dealloc(ptr, layout) }
        }
        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            note_event(new_size);
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static COUNTING: EventCounting = EventCounting;

    /// 窗内执行一次，返回（分配次数, 分配字节数, 返回值）。
    pub(super) fn allocs<T>(op: impl FnOnce() -> T) -> (u64, u64, T) {
        WINDOW.with(|c| c.set((true, 0, 0)));
        let out = op();
        black_box(&out);
        let (_, n, b) = WINDOW.with(Cell::get);
        WINDOW.with(|c| c.set((false, 0, 0)));
        (n, b, out)
    }

    /// 仪器自检：窗内分配必须可见（防「分配器没挂上/门没开」的假绿）。
    pub(super) fn self_check() -> u64 {
        let (n, _, v) = allocs(|| Vec::<u8>::with_capacity(32));
        black_box(v);
        n
    }
}

/// 未变更快路径（所有哨兵报未变）每次通知的分配数必须与「订阅者什么都不做」的
/// store 地面值相等——桥侧多一次分配（例如又去复制值快照）即红。
#[test]
fn hot_path_allocation_budget_matches_a_noop_subscriber() {
    bounded(|| {
        assert!(alloc_probe::self_check() >= 1, "仪器自检：窗内分配必须可见");
        // 桥：哨兵恒报未变 ⇒ 不投影、不比较、不装箱。
        let hook = Hook::new();
        let bridge = build_with(Arc::clone(&hook), false, true);
        for _ in 0..4 {
            assert!(bridge.send_intent(json!(1)).unwrap()); // 预热（一次性扩容）
        }
        let (fast, fast_bytes, ()) = alloc_probe::allocs(|| {
            assert!(bridge.send_intent(json!(1)).unwrap());
        });

        // 地面值：同一 dispatch 走「空订阅者」store（桥侧零成本对照）。
        let floor_store = SyncStore::new(AppReducer, NoFx);
        let _sub = floor_store.subscribe(|_: &App| {});
        let sender = floor_store.sender();
        for _ in 0..4 {
            assert!(sender.dispatch(1));
        }
        let (floor, floor_bytes, ()) = alloc_probe::allocs(|| {
            assert!(sender.dispatch(1));
        });

        assert_eq!(
            fast, floor,
            "仅 JSON 的未变更快路径不得比空订阅者多分配（{fast} vs {floor}）"
        );

        // 同类实测：**挂区域通道**的未变更快路径（每拍办一次空事务：计划复制索引镜像
        // + 背衬写入）。这是部署事实而非回退，故只给读数与上限锚。
        let region_hook = Hook::new();
        let with_region = build_with(Arc::clone(&region_hook), true, true);
        for _ in 0..4 {
            assert!(with_region.send_intent(json!(1)).unwrap());
        }
        let (region_n, region_bytes, ()) = alloc_probe::allocs(|| {
            assert!(with_region.send_intent(json!(1)).unwrap());
        });
        eprintln!(
            "#140 未变更快路径分配：仅 JSON {fast} 次/{fast_bytes}B（地面值 {floor} 次/{floor_bytes}B），JSON+Region {region_n} 次/{region_bytes}B"
        );
        assert!(
            region_n <= floor + 4,
            "JSON+Region 未变更快路径分配超上限：{region_n}（地面值 {floor}）"
        );
    });
}
