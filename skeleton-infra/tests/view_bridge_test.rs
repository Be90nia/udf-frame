#![cfg(feature = "view")]
//! ViewBridge 行为面验收（规格 §8 验收②载体）：panic 毒化-重同步闭环（四步断
//! 言）/ 绑定封板 / 跨线程 pull / 桥 Drop / 闭门 send_intent 显式 `Ok(false)`
//! ——并发与毒化场景 demo 装不下，在此机械化（断言链①②）。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    Coverage, Encoders, IntentError, JsonEncoder, RegionEncoder, RegionWriter, ViewBridge,
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
