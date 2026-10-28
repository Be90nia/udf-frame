#![cfg(feature = "view")]
//! ViewBridge 行为面验收（规格 §8 验收②载体）：panic 毒化-重同步闭环（四步断
//! 言）/ 绑定封板 / 跨线程 pull / 桥 Drop / 闭门 send_intent 显式 `Ok(false)`
//! / 「input 覆盖实读」红线的最小反例（漏报可检出）
//! ——并发与毒化场景 demo 装不下，在此机械化（断言链①②）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    Coverage, Encoders, IntentError, JsonEncoder, RegionEncoder, RegionWriter, Sentinel, ViewBridge,
};

#[derive(Clone, Default)]
struct App {
    count: u32,
}

#[derive(Default)]
struct AppReducer;

impl Reducer for AppReducer {
    type State = App;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> App {
        App::default()
    }
    fn reduce(&self, s: &App, a: u32, _: &mut EffectBuf<u32, Infallible>) -> App {
        App { count: s.count + a }
    }
}

#[derive(Clone, PartialEq)]
enum V {
    C(u32),
}

/// 毒化注入：`arm` 置位时 project panic（三态提交缺口：哨兵已提交、last/脏未更新）。
struct BoomMap {
    arm: Arc<AtomicBool>,
}

impl ViewMap<App> for BoomMap {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        &[BindingDecl {
            path: "/count",
            kind: PayloadTag(1),
        }]
    }
    fn project(&self, _: usize, s: &App) -> V {
        assert!(
            !self.arm.load(Ordering::SeqCst),
            "injected projection panic"
        );
        V::C(s.count)
    }
}

struct Json;

impl JsonEncoder for Json {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &V) -> Value {
        match v {
            V::C(n) => json!(n),
        }
    }
}

struct Region;

impl RegionEncoder for Region {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, _: &V, _: &mut RegionWriter) {}
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

type TestBridge = ViewBridge<App, u32, BoomMap, Intent, Json, Region>;

fn build(arm: Arc<AtomicBool>) -> TestBridge {
    let store = SyncStore::new(AppReducer, NoFx);
    ViewBridge::mount(
        store,
        BoomMap { arm },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    )
}

/// 毒化-重同步闭环四步断言（规格 §8②）：panic ⇒ 计数+闩 ⇒ 次批 resync ⇒
/// snapshot_doc 全量且复位 ⇒ 后续变更补丁恢复到达。
#[test]
fn poison_resync_cycle() {
    let arm = Arc::new(AtomicBool::new(false));
    let bridge = build(Arc::clone(&arm));
    bridge.bind("/count").unwrap();

    assert!(bridge.send_intent(json!(2)).unwrap());
    arm.store(true, Ordering::SeqCst); // 下一拍注入 panic
    assert!(bridge.send_intent(json!(3)).unwrap());

    // ① 计数 + 闩：panic 被桥内 catch，禁裸传禁吞
    assert_eq!(bridge.projection_panics(), 1);
    // ② 次批携带 resync == true（完整性事实必达 UI）
    let batch = bridge.take_patches();
    assert!(batch.resync, "毒化必须经返回值显形");
    assert_eq!(batch.to_version, 2);
    // ③ snapshot_doc 现态全量重投影（哨兵旁路）且毒化复位
    arm.store(false, Ordering::SeqCst);
    let snap = bridge.snapshot_doc();
    assert_eq!(
        snap.doc.pointer("/count"),
        Some(&json!(5)),
        "现态重建（不信缓存）"
    );
    assert!(!bridge.take_patches().resync, "重同步后毒化闩复位");
    // ④ 后续变更补丁恢复到达
    assert!(bridge.send_intent(json!(1)).unwrap());
    let batch = bridge.take_patches();
    assert!(!batch.resync);
    assert_eq!(batch.patches.len(), 1);
    assert_eq!(batch.patches[0].value, json!(6));
}

/// 绑定封板（F1）：首通知后 bind ⇒ 显式 Err（只读无锁保证）。
#[test]
fn bind_sealed_after_first_notify() {
    let bridge = build(Arc::new(AtomicBool::new(false)));
    bridge.bind("/count").unwrap();
    assert!(bridge.send_intent(json!(1)).unwrap()); // 首通知封板
    let err = bridge.bind("/count").unwrap_err();
    assert!(err.to_string().contains("sealed"), "封板错误须显形: {err}");
}

/// 跨线程 pull（F1）：桥 Send+Sync，宿主线程 take_patches；快照即交付（N2）。
#[test]
fn cross_thread_pull() {
    let bridge = build(Arc::new(AtomicBool::new(false)));
    bridge.bind("/count").unwrap();
    assert!(bridge.send_intent(json!(4)).unwrap());
    let snapshot = bridge.snapshot_doc();
    std::thread::spawn(move || {
        let batch = bridge.take_patches();
        assert_eq!(batch.to_version, 1);
        assert_eq!(
            snapshot.version, batch.from_version,
            "N2：快照重同步后 from == snapshot.version"
        );
    })
    .join()
    .unwrap();
}

/// 桥 Drop：store 随桥回收（mount 取所有权），通路闭门显形（ActionSender false）。
#[test]
fn drop_releases_cleanly() {
    let arm = Arc::new(AtomicBool::new(false));
    let store = SyncStore::new(AppReducer, NoFx);
    let sender = skeleton::Store::sender(&store); // mount 前克隆上行句柄
    let bridge: TestBridge = ViewBridge::mount(
        store,
        BoomMap { arm },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    );
    bridge.bind("/count").unwrap();
    assert!(sender.dispatch(1));
    drop(bridge);
    assert!(
        !sender.dispatch(1),
        "store 所有权随桥回收 ⇒ 闭门显形（非静默悬挂）"
    );
}

/// 闭门 store：send_intent 显式 `Ok(false)`（ActionSender must_use 语义透传）。
#[test]
fn closed_store_yields_ok_false() {
    let store = SyncStore::new(AppReducer, NoFx);
    store.close(); // 闭门后挂桥：订阅照常登记、永无通知（契约④已知取舍）
    let bridge: TestBridge = ViewBridge::mount(
        store,
        BoomMap {
            arm: Arc::new(AtomicBool::new(false)),
        },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    );
    bridge.bind("/count").unwrap();
    assert!(
        matches!(bridge.send_intent(json!(1)), Ok(false)),
        "闭门显式拒绝"
    );
}

// ── G5：「input 覆盖实读」红线的最小可验证反例（报告 G5 建议处置） ──────────
//
// 红线原文：`input_changed` 声明的 `In` 必须覆盖 `project` 的实读面——漏报 =
// 静默陈旧（框架零宏，咬不住采用方的代码）。本组用同一份 `project` 配两种
// 哨兵，把「漏报确实存在」与「它在哪几个观测面上留下签名」都变成可执行断言。

/// 两字段状态：`b` 只被 `project` 实读；哨兵声明不声明它由映射自选。
#[derive(Clone, Default)]
struct Pair {
    a: u32,
    b: u32,
}

enum PairAct {
    A,
    B,
}

struct PairReducer;

impl Reducer for PairReducer {
    type State = Pair;
    type Action = PairAct;
    type Op = Infallible;
    fn initial(&self) -> Pair {
        Pair::default()
    }
    fn reduce(&self, s: &Pair, a: PairAct, _: &mut EffectBuf<PairAct, Infallible>) -> Pair {
        match a {
            PairAct::A => Pair { a: s.a + 1, b: s.b },
            PairAct::B => Pair { a: s.a, b: s.b + 1 },
        }
    }
}

/// 视图词表 = 互斥状态单变体 enum（`ViewMap` 词表纪律的落地形态）。
#[derive(Clone, PartialEq)]
enum Sum {
    Total(u32),
}

/// 哨兵键类型 = 映射**声明**的输入面：`u32`（只 `a`，窄）/ `(u32, u32)`（覆盖
/// 实读）。`project` 恒读 `a + b`——两者只差声明面。
struct SumMap<In> {
    sentinel: Sentinel<Pair, In>,
}

impl<In: PartialEq + Send + 'static> ViewMap<Pair> for SumMap<In> {
    type View = Sum;
    fn bindings(&self) -> &[BindingDecl] {
        &[BindingDecl {
            path: "/sum",
            kind: PayloadTag(1),
        }]
    }
    fn input_changed(&self, _: usize, s: &Pair) -> bool {
        self.sentinel.changed(s)
    }
    fn project(&self, _: usize, s: &Pair) -> Sum {
        Sum::Total(s.a + s.b)
    }
}

struct SumJson;

impl JsonEncoder for SumJson {
    type View = Sum;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &Sum) -> Value {
        match v {
            Sum::Total(n) => json!(n),
        }
    }
}

struct SumRegion;

impl RegionEncoder for SumRegion {
    type View = Sum;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, _: &Sum, _: &mut RegionWriter) {}
}

struct PairIntent;

impl IntentMap<PairAct> for PairIntent {
    type Wire = Value;
    type Err = IntentError;
    fn intent(&self, w: Value) -> Result<PairAct, IntentError> {
        match w.as_str() {
            Some("a") => Ok(PairAct::A),
            Some("b") => Ok(PairAct::B),
            _ => Err(IntentError::malformed("expect \"a\" or \"b\"")),
        }
    }
}

/// 「只动 b」实验的观测面（全部是采用方可读的量）。
struct Observed {
    /// 该拍到达的补丁数。
    patches: usize,
    /// 该批水位（版本与状态变更逐拍一一对应，同步档）。
    to_version: u64,
    /// 累计投影次数（哨兵跳过的拍不计）。
    runs: u64,
    /// 快照真值（哨兵旁路，不增 `runs`）。
    doc: Value,
}

/// 单次实验：先动 `a`（两种哨兵都声明了它 ⇒ 必成一拍），再**只**动 `b`（差异
/// 所在）。
fn bump_b_with<In: PartialEq + Send + 'static>(sentinel: Sentinel<Pair, In>) -> Observed {
    let store = SyncStore::new(PairReducer, NoFx);
    let bridge: ViewBridge<Pair, PairAct, SumMap<In>, PairIntent, SumJson, SumRegion> =
        ViewBridge::mount(
            store,
            SumMap { sentinel },
            PairIntent,
            Encoders {
                json: Some(SumJson),
                region: None,
            },
        );
    bridge.bind("/sum").unwrap();
    assert!(bridge.send_intent(json!("a")).unwrap());
    assert_eq!(bridge.take_patches().patches.len(), 1, "首拍必须成补丁");
    assert!(bridge.send_intent(json!("b")).unwrap());
    let batch = bridge.take_patches();
    Observed {
        patches: batch.patches.len(),
        to_version: batch.to_version,
        runs: bridge.projection_runs(),
        doc: bridge.snapshot_doc().doc,
    }
}

/// 漏报反例：哨兵声明窄于实读 ⇒ `b` 变更被跳过、无补丁；三处签名一起构成
/// 「可检出」——「静默陈旧」不是真的静默。
#[test]
fn narrow_input_leaks_stale_and_is_detectable() {
    // 正例：哨兵覆盖实读 ⇒ 两拍都投影、都成补丁。
    let cover = bump_b_with(Sentinel::new(|s: &Pair| (s.a, s.b)));
    assert_eq!(
        (cover.patches, cover.runs, cover.to_version),
        (1, 2, 2),
        "覆盖实读：b 变更必须被哨兵看见"
    );
    assert_eq!(cover.doc.pointer("/sum"), Some(&json!(2)));

    // 反例（违约形态）：哨兵只声明 `a` ⇒ 漏报。
    let leak = bump_b_with(Sentinel::new(|s: &Pair| s.a));
    assert_eq!(leak.patches, 0, "漏报 = 静默陈旧：b 变了但无补丁到达");
    assert_eq!(
        leak.runs, 1,
        "签名①：projection_runs 停在 1（第 2 拍没投影）"
    );
    // 签名②：水位照常推进（2）而补丁为空——UI 侧唯一可见的「状态变了」信号。
    assert_eq!(leak.to_version, 2, "版本推进与投影解耦：空批也推进水位");
    // 签名③：快照真值 = 2，而最后交付的补丁值仍是 1 ⇒ 不一致可被检出。
    assert_eq!(
        leak.doc.pointer("/sum"),
        Some(&json!(2)),
        "实读宽于声明 In：真值只在哨兵旁路的面（快照）上可见"
    );
}
