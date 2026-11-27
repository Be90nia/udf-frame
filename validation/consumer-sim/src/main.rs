//! 模拟下游采用方（external adopter）——编译夹具。
//!
//! 本工程独立于 udf-frame workspace（自带空 `[workspace]` 根），只以 path 依赖
//! 指向仓库三 crate，因此**编译视角 = 外部 crate 视角**：凡「同 crate 豁免」的
//! 规则在此一律生效（`#[non_exhaustive]` 通配臂、私有字段封闭构造等）。
//!
//! 覆盖面（= 本轮三处破坏面 + 今日新增面）：
//! 1. `EffectId` 跨 crate 受控构造（只用 `EffectId::new` / `get`）。
//! 2. `Effect` / `Restore` 跨 crate match（必须带通配臂）。
//! 3. 跨 crate 持有 `impl Fragment<…>`（ADR 0009 迁移后的返回类型形态）。
//! 4. 完整 Feature（`impl Reducer`）作根挂 `SyncStore`。
//! 5. 今日新增面：`Store` 六方法 / `notify` 异步信号档 / `FxScope` 效果作用域。
//! 6. `lint` 阶段编排：外部 crate 实现 `LintStage<C>` + 全局注册表 + `post_process`
//!    （单开 `udf-skeleton` 即可用，不拉 skeleton-infra feature 矩阵）。
//! 7. `fields` 可观测字段值面：FieldValue/FieldBag 值语义袋（COW 克隆/同名覆盖/稳定迭代）。
//! 8. `alloc_site` 分配站点目录：alloc_sites! 宏编译期注册 + 原子计数/两级开关（memgov feature）。

use std::borrow::Borrow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use skeleton::clock::SystemClock;
use skeleton::fields::{FieldBag, FieldValue};
use skeleton::lint::{
    LintError, LintStage, clear_stages, post_process, register_stage, registered_stages,
};
use skeleton::notify::{Notify, NotifySink};
use skeleton::pool::{PoolManaged, PoolMetrics};
use skeleton::{
    ActionSender, Effect, EffectBuf, EffectId, Fragment, Infallible, NoFx, Reducer, Restore, Store,
};
use skeleton_infra::compose::combine;
use skeleton_infra::fx::{AdmissionError, FxScope};
use skeleton_infra::runtime::{AdmissionOutcome, SyncStore};
use skeleton_infra::{ObjectPool, ObjectPoolTuning, ResetPhase};

// ── 1. EffectId：跨 crate 受控构造 ────────────────────────────────────────
// 裸元组语法 `EffectId(1)` 在外部 crate 不可用（字段私有）——唯一构造入口
// `EffectId::new`，唯一读出口 `get`。
fn effect_id_construction() -> u64 {
    let id = EffectId::new(7);
    assert_eq!(id.get(), 7);
    id.get()
}

// ── 2. Effect / Restore：跨 crate match 必须带通配臂 ──────────────────────
// `Effect` / `Restore` 均 `#[non_exhaustive]`：外部 crate 的 match 必须留
// 通配臂（未知词表形态走 fail-closed 分支，不得静默漏配）。
fn describe_effect(effect: &Effect<u64, Infallible>) -> &'static str {
    match effect {
        Effect::Emit(_) => "emit",
        Effect::Run { id, .. } => {
            assert!(id.get() > 0);
            "run"
        }
        Effect::Cancel { id } => {
            assert!(id.get() > 0);
            "cancel"
        }
        _ => "unknown-effect-variant",
    }
}

fn describe_restore(restore: &Restore<u64, u64>) -> &'static str {
    match restore {
        Restore::Empty => "empty",
        Restore::FromInitial(actions) => {
            assert_eq!(actions.len(), 1);
            "from-initial"
        }
        Restore::FromSnapshot(state, actions) => {
            assert_eq!(*state, 41);
            assert!(actions.is_empty());
            "from-snapshot"
        }
        _ => "unknown-restore-variant",
    }
}

// ── 3. 状态分片：跨 crate 持有 `impl Fragment<…>` ─────────────────────────
// ADR 0009 后 combine 产物只实现 `Fragment`（reduce-only 分片），返回类型注解
// 随之从 `impl Reducer<…>` 迁到 `impl Fragment<…>`。
#[derive(Clone, Debug, PartialEq)]
struct ParentState {
    total: u64,
    child: u64,
}

#[derive(Clone, Debug, PartialEq)]
enum ParentAction {
    Child(u64),
    Touch,
}

struct ChildCounter;

impl Reducer for ChildCounter {
    type State = u64;
    type Action = u64;
    type Op = Infallible;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, state: &u64, action: u64, _out: &mut EffectBuf<u64, Infallible>) -> u64 {
        state + action
    }
}

/// 分片工厂：返回类型即迁移面本体（`impl Fragment<…>`，非 `impl Reducer<…>`）。
fn child_fragment() -> impl Fragment<State = ParentState, Action = ParentAction, Op = Infallible> {
    combine(
        |p: &ParentState| &p.child,
        |p: &mut ParentState, c: u64| p.child = c,
        ParentAction::Child,
        |p: &ParentAction| match p {
            ParentAction::Child(c) => Some(*c),
            ParentAction::Touch => None,
        },
        ChildCounter,
        |_, _| {},
    )
}

// ── 4. 完整 Feature（impl Reducer）作根 ───────────────────────────────────
// 根契约 = `Reducer`（有 initial）；分片无 initial，不能作根。
struct ParentFeature;

impl Reducer for ParentFeature {
    type State = ParentState;
    type Action = ParentAction;
    type Op = Infallible;

    fn initial(&self) -> ParentState {
        ParentState {
            total: 100,
            child: 0,
        }
    }

    fn reduce(
        &self,
        state: &ParentState,
        action: ParentAction,
        out: &mut EffectBuf<ParentAction, Infallible>,
    ) -> ParentState {
        match action {
            ParentAction::Child(_) => child_fragment().reduce(state, action, out),
            ParentAction::Touch => ParentState {
                total: state.total + 1,
                child: state.child,
            },
        }
    }
}

// ── 5. notify：宿主实现投递口 + 登记面 ───────────────────────────────────
#[derive(Clone)]
struct LatestVersion(Arc<AtomicU64>);

impl NotifySink for LatestVersion {
    fn publish(&self, version: u64) {
        self.0.store(version, Ordering::Relaxed);
    }
}

// ── 6. lint：配置后处理阶段编排（外部 crate 视角）─────────────────────────
// 契约全在 `skeleton`（零依赖）：实现方只需实现 `LintStage<C>`（C 是自家配置
// 结构体）、在 init() 登记、装载后调 `post_process`。件内不认任何领域类型。
struct LintConfig {
    retries: u32,
    log_level: String,
}

struct ClampRetries;

impl LintStage<LintConfig> for ClampRetries {
    fn name(&self) -> &'static str {
        "clamp_retries"
    }

    fn run(&self, cfg: &mut LintConfig) -> Result<(), LintError> {
        cfg.retries = cfg.retries.min(3); // 阶段可改写，不只校验
        Ok(())
    }
}

struct RequireLogLevel;

impl LintStage<LintConfig> for RequireLogLevel {
    fn name(&self) -> &'static str {
        "require_log_level"
    }

    fn run(&self, cfg: &mut LintConfig) -> Result<(), LintError> {
        if cfg.log_level.is_empty() {
            return Err(LintError::new("log_level 必填")); // stage 由编排器补齐
        }
        Ok(())
    }
}

/// 注册 → 按序执行 → 短路 + 归属 → 可改写 → 清场。
fn lint_roundtrip() {
    register_stage::<LintConfig>(Arc::new(ClampRetries));
    register_stage::<LintConfig>(Arc::new(RequireLogLevel));
    assert_eq!(
        registered_stages::<LintConfig>(),
        ["clamp_retries", "require_log_level"] // 注册序
    );

    let mut cfg = LintConfig {
        retries: 9,
        log_level: String::new(),
    };
    let err = post_process(&mut cfg).unwrap_err();
    assert_eq!(err.stage(), Some("require_log_level")); // 编排器补归属
    assert_eq!(err.message(), "log_level 必填");
    assert_eq!(cfg.retries, 3); // 短路：先前阶段的改写保留

    cfg.log_level = "info".into();
    assert_eq!(post_process(&mut cfg), Ok(())); // 空错误面
    clear_stages::<LintConfig>();
    assert!(registered_stages::<LintConfig>().is_empty());
}

// ── 7. fields：可观测字段值面（外部 crate 视角）───────────────────────────
// 值面全在 `skeleton`（零依赖）：袋构造/同名覆盖/COW 克隆/稳定迭代，导出归消费方。
fn fields_roundtrip() {
    let mut bag = FieldBag::new();
    bag.insert("http.method", FieldValue::Str(String::from("GET")));
    bag.insert("http.status", FieldValue::Int(503));
    bag.insert("http.status", FieldValue::Int(200)); // 同名覆盖：值换、位置不动
    assert_eq!(bag.len(), 2); // 构造即去重

    // COW 克隆：enriched 的后续改写不影响原袋
    let mut enriched = bag.clone();
    enriched.insert("peer", FieldValue::Str(String::from("edge-7")));
    assert_eq!(bag.get("peer"), None);
    enriched.merge(&bag); // 已有键覆盖、新键按 other 序追加
    assert_eq!(
        enriched.get("peer"),
        Some(&FieldValue::Str(String::from("edge-7")))
    );

    // 稳定迭代序 + 值词表跨 crate match（通配臂必需）
    let names: Vec<&'static str> = enriched.iter().map(|(n, _)| n).collect();
    assert_eq!(names, ["http.method", "http.status", "peer"]);
    for (name, value) in enriched.iter() {
        let tag = match value {
            FieldValue::Int(_) => "int",
            FieldValue::Float(_) => "float",
            FieldValue::Bool(_) => "bool",
            FieldValue::Str(_) => "str",
            FieldValue::Duration(_) => "duration",
            FieldValue::Count(_) => "count",
            _ => "future-variant",
        };
        assert!(!name.is_empty() && !tag.is_empty());
    }
}

// ── 8. alloc_site：分配站点目录（外部 crate 自声明表）─────────────────────
use skeleton_infra::alloc_site::{find, global_enabled, set_global_enabled};
use skeleton_infra::alloc_sites;

alloc_sites! {
    pub static ADOPTER_SITES = {
        CONN_ACCEPT,
        CONN_RECYCLE,
    };
}

fn alloc_sites_roundtrip() {
    assert!(global_enabled(), "两级开关默认全开");
    CONN_ACCEPT.bump();
    CONN_ACCEPT.bump();
    CONN_RECYCLE.bump();
    assert_eq!(CONN_ACCEPT.count(), 2);
    assert_eq!(
        find(ADOPTER_SITES, "CONN_RECYCLE").map(|s| s.count()),
        Some(1)
    );

    // 枚举序 = 宏声明序
    let names: Vec<&'static str> = ADOPTER_SITES.iter().map(|s| s.name).collect();
    assert_eq!(names, ["CONN_ACCEPT", "CONN_RECYCLE"]);

    // 站点开关：关 ⇒ 零写入，历史计数保留
    CONN_RECYCLE.set_enabled(false);
    CONN_RECYCLE.bump();
    assert_eq!(CONN_RECYCLE.count(), 1, "关 ⇒ 旁路");
    CONN_RECYCLE.set_enabled(true);

    // 全局闸：一刀切
    set_global_enabled(false);
    CONN_ACCEPT.bump();
    assert_eq!(CONN_ACCEPT.count(), 2, "全局关 ⇒ 零写入");
    set_global_enabled(true);
}

// ── 9. 通用对象池 ObjectPool（issue#15）：外部采用方消费形态 ───────────────
// 覆盖外部视角的四个可达性：crate 根 re-export 路径 / `PoolManaged` 契约三挂点
// / `Borrow<PoolMetrics>` 核心形 / `Pooled` 的 DerefMut（捞出改参数即复用）。
fn object_pool_roundtrip() {
    // 池化**有状态**对象：连接 wrapper 形态（捞出来改参数即用）。
    struct Conn {
        endpoint: String,
        buf: Vec<u8>,
    }

    let pool: ObjectPool<Conn> = ObjectPool::with_reset(
        4,
        2,
        || Conn {
            endpoint: String::new(),
            buf: Vec::with_capacity(64 * 1024),
        },
        |c: &mut Conn| {
            c.endpoint.clear();
            c.buf.clear();
        },
        ResetPhase::OnReturn,
    );

    // 借出 → 改参数 → 用 → 归还（RAII）。
    {
        let mut conn = pool.acquire();
        conn.endpoint = "ws://example/1".to_string();
        conn.buf.extend_from_slice(b"hello");
        assert_eq!(conn.buf.len(), 5);
    }
    assert_eq!(pool.metrics().core.borrowed, 0);
    assert_eq!(pool.metrics().core.idle, 1);

    // 复用同一件（构造次数不增）+ reset 钩子已把状态重置。
    let again = pool.acquire();
    assert_eq!(again.endpoint, ""); // reset 生效：参数已清空
    assert!(again.buf.is_empty());
    assert_eq!(pool.metrics().reused, 1);
    assert_eq!(pool.metrics().constructed, 1);

    // detach：交出所有权 → 对象脱离池生命周期（最后一个持有者放手即析构）。
    let owned: Conn = again.detach();
    assert!(owned.endpoint.is_empty());

    // `PoolManaged` 三挂点（外部 crate 调用面）。
    pool.resize(ObjectPoolTuning {
        max: 8,
        idle_cap: 4,
    })
    .expect("valid resize");
    let mut kinds = Vec::new();
    let ticket = pool.subscribe_audit(move |e| kinds.push(e.kind));
    drop(ticket);

    // `Borrow<PoolMetrics>` 核心形：外部代码对任何池取同一组核心读数。
    let snapshot = pool.metrics();
    let core: &PoolMetrics = snapshot.borrow();
    assert_eq!(core.max, 8);
    assert!(core.watermark >= 0.0);

    // 老化口：宿主自调（池不起后台线程）——detach 已把对象带走，先放一件回池再老化。
    // Clock 注入版 = 正典口径（issue#22 弃用 Instant 版；外部 crate 同款写法）。
    drop(pool.acquire());
    let pruned = pool.prune_idle_with_clock(Duration::ZERO, &SystemClock);
    assert_eq!(pruned, 1, "Duration::ZERO ⇒ 释放全部 idle");

    // `Pooled` 只能经 `acquire` 取得（字段私有，外部 crate 写不出裸守卫——
    // 见反例 E）。
}

fn main() {
    assert_eq!(effect_id_construction(), 7);

    // 跨 crate match：通配臂存在性由此编译（无通配臂 = E0004）。
    let cancel = Effect::<u64, Infallible>::Cancel {
        id: EffectId::new(1),
    };
    assert_eq!(describe_effect(&cancel), "cancel");
    assert_eq!(describe_restore(&Restore::Empty), "empty");
    assert_eq!(
        describe_restore(&Restore::FromInitial(vec![9])),
        "from-initial"
    );
    assert_eq!(
        describe_restore(&Restore::FromSnapshot(41, vec![])),
        "from-snapshot"
    );

    // 完整 Feature 作根挂载：Store 六方法全走 trait 面。
    let store = SyncStore::new(ParentFeature, NoFx);
    let seen = Arc::new(AtomicU64::new(0));
    let _ticket = store.subscribe({
        let seen = Arc::clone(&seen);
        move |s: &ParentState| {
            seen.store(s.total, Ordering::Relaxed);
        }
    });
    store.dispatch(ParentAction::Child(2));
    store.dispatch(ParentAction::Touch);
    assert_eq!(seen.load(Ordering::Relaxed), 101);
    assert_eq!(
        *store.snapshot(),
        ParentState {
            total: 101,
            child: 2
        }
    );

    // notify 异步信号档：版本号单调，落位拍 +1。
    let sink = LatestVersion(Arc::new(AtomicU64::new(0)));
    let _notify_ticket = store.subscribe_notify(sink.clone());
    store.dispatch(ParentAction::Touch);
    assert_eq!(sink.0.load(Ordering::Relaxed), 1);
    assert_eq!(store.notify_published(), 1);

    // ActionSender 回灌句柄（外部采用方自持的回灌口）。
    let sender: ActionSender<ParentAction> = store.sender();
    assert!(sender.dispatch(ParentAction::Touch));

    // issue#38：有界准入的 typed 面（外部 crate 视角）。AdmissionOutcome 非
    // non_exhaustive ⇒ 跨 crate 穷尽 match 合法（漏变体即编译错）。
    store.set_capacity(8);
    match store.try_dispatch(ParentAction::Touch) {
        AdmissionOutcome::Accepted => {}
        AdmissionOutcome::Full => panic!("容量 8 不会满"),
        AdmissionOutcome::Closed => panic!("store 尚未关闭"),
    }
    assert_eq!(store.admission_full_count(), 0, "未被拒过");

    store.close();
    assert!(store.is_closed());
    assert!(!sender.dispatch(ParentAction::Touch));

    // FxScope 效果作用域（tokio feature）。
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async {
        let scope = FxScope::new();
        let handle = scope
            .spawn_tracked(async { 41 + 1 })
            .expect("scope accepting");
        assert_eq!(handle.join().await, Ok(42));
        // issue#39：歧义名 expect_idle 已拆为两个口径的断言（外部 crate 视角）。
        scope.expect_body_idle(); // 任务体结束
        let counts = scope.counts();
        assert_eq!(counts.active, 0, "任务体结束 ⇒ active 归零");
        assert_eq!(counts.waiting, 0, "无等待者");
        let _reaped = scope.try_reap(); // 收割已完结条目
        scope.expect_fully_reaped_idle(); // 集合已完全收割（更强口径）
    });

    // issue#39：有界准入的外部 crate 消费形态——两个独立上界 + typed 拒绝。
    // 本夹具是**编译夹具**：此处只钉 API 的外部可达性与类型面（穷尽 match
    // 合法、计数可读），行为语义由仓库内单测钉。
    rt.block_on(async {
        let scope = FxScope::new().with_max_pending(1).with_max_tracked(4);
        // AdmissionError 非 non_exhaustive ⇒ 跨 crate 穷尽 match 合法
        // （漏一个变体即编译错，这就是本面的鉴别力）。
        match scope.try_spawn_tracked(async { 1u32 }) {
            Ok(handle) => {
                let _ = handle;
            }
            Err(AdmissionError::PendingFull) => {}
            Err(AdmissionError::TrackedFull) => {}
            Err(AdmissionError::Closed) => {}
            // issue#92：runtime 上下文前置条件不成立（本处已在 block_on 内，不可达）。
            Err(AdmissionError::NoRuntime) => {}
        }
        // 三轴计数从外部 crate 可读（字段全 pub，非 non_exhaustive）。
        let counts = scope.counts();
        let _ = (
            counts.active,
            counts.waiting,
            counts.completed_unreaped,
            counts.rejected,
        );
    });

    // issue#92：`AdmissionError::NoRuntime` 是环境前置条件（调用线程不在 Tokio runtime
    // 上下文），不是过载也不是闸关。本夹具只钉三件事：typed 拒绝、不 panic、不入容量账
    // （不计 `rejected`）。真实采用方应把它当编程错误带因上报，而不是当「丢弃负载」吞掉。
    // block_on 之外（主线程）调用即从外部 crate 触达该变体。
    {
        let scope = FxScope::new();
        match scope.try_spawn_tracked(async { 1u32 }) {
            Err(AdmissionError::NoRuntime) => {}
            Err(other) => panic!("runtime 之外应得 NoRuntime，实得 {other}"),
            Ok(_) => panic!("runtime 之外不应被接纳"),
        }
        assert_eq!(scope.counts().rejected, 0, "NoRuntime 不入容量账");
    }

    // lint 阶段编排：外部 crate 自己实现阶段，注册表按 C 类型分槽。
    lint_roundtrip();

    // fields 值面 + alloc_site 站点目录（外部 crate 消费形态）。
    fields_roundtrip();
    alloc_sites_roundtrip();

    // 通用对象池 ObjectPool（issue#15，外部 crate 消费形态）。
    object_pool_roundtrip();

    // std-ext 公共面（第三 crate 的 path 依赖可达性）。
    assert_eq!(std_ext::text::truncate_chars("abcdef", 3), "abc");
    let h = std_ext::hash::stable64(b"consumer-sim");
    assert_ne!(h, 0);
    assert_eq!(h, std_ext::hash::stable64(b"consumer-sim")); // 确定性

    println!("consumer-sim: OK");
}

// ── 反例（鉴别力证明）────────────────────────────────────────────────────
// 把下面任一反例取消注释后 `cargo check` 必红——证明本夹具对三处破坏面
// 具备鉴别力（而非「恒绿的空壳」）。验证后必须恢复注释态。
//
// 反例 A（EffectId 裸元组构造 → E0423/E0603：字段私有，构造入口只有 new）：
//   let _id = EffectId(1);
//
// 反例 B（Effect 跨 crate match 漏通配臂 → E0004 non-exhaustive patterns）：
//   fn bad(e: &Effect<u64, Infallible>) {
//       match e {
//           Effect::Emit(_) => {}
//           Effect::Run { .. } => {}
//           Effect::Cancel { .. } => {}
//       }
//   }
//
// 反例 C（分片作根 → E0277：`Combine` 只实现 Fragment，不实现 Reducer）：
//   let _store = SyncStore::new(child_fragment(), NoFx);
//
// 反例 D（lint 错误必须经受控构造 → E0451：`LintError` 字段私有，stage 归属
// 只能由编排器补，外部 crate 写不出「无归属但谎称有归属」的错误）：
//   let _err = LintError { stage: None, message: String::from("x") };
//
// 反例 E（借出守卫 `Pooled` 只能经 `acquire` 取得 → E0451：字段私有，外部
// crate 写不出裸守卫——裸守卫 = 绕过 RAII 归还 = 池语义自毁）：
//   let _p = Pooled { pool: &pool, home: 0, slot: None, degraded: false };
//
// 反例 F（issue#39 破坏面：歧义名 `expect_idle` 已删除，拆为两个口径的方法
// → E0432/E0425 找不到该函数/该方法）：
//   use skeleton_infra::fx::expect_idle;
//   scope.expect_idle();
