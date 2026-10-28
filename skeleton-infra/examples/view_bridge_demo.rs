//! ViewBridge 正典验收环（ADR 0006 / 规格 §8 demo 行为脚本 + 断言链①②③）：
//! 矩阵行为（只开 Json / 只开 Region / 都开）+ 双编码一致性 + 镜像读取器 +
//! 换区握手 + 水位自洽 + 未知标签跳过 + manifest `include_str!` 逐字对表。
//! 断言内嵌，红即退出码非 0；语言无关性：断言全程只经 `serde_json::Value`
//! 与 `&str` 进出（无任何 UI 栈类型参与）。
use std::sync::Arc;

use serde_json::{json, Value};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap, PROTOCOL_VERSION};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    read::{decode_batch, RegionReader},
    Coverage, Derived, Encoders, IntentError, JsonEncoder, RegionBacking, RegionEncoder,
    RegionWriter, Sentinel, VecBacking, ViewBridge,
};

// ── 领域切片（不出 Rust）──
#[derive(Clone, Default)]
struct App {
    count: u32,
    todos: Vec<bool>, // done 旗标（demo 不读标题，YAGNI）
    noise: u32,       // Touch 拍的无关切片
}

enum Msg {
    Add(u32),
    PushTodo,
    Flip(u32),
    Touch,
}

struct AppReducer;

impl Reducer for AppReducer {
    type State = App;
    type Action = Msg;
    type Op = Infallible;
    fn initial(&self) -> App {
        App::default()
    }
    fn reduce(&self, s: &App, a: Msg, _out: &mut EffectBuf<Msg, Infallible>) -> App {
        match a {
            Msg::Add(n) => App {
                count: s.count + n,
                todos: s.todos.clone(),
                noise: s.noise,
            },
            Msg::PushTodo => {
                let mut todos = s.todos.clone();
                todos.push(false);
                App {
                    count: s.count,
                    todos,
                    noise: s.noise,
                }
            }
            Msg::Flip(i) => {
                let mut todos = s.todos.clone();
                if let Some(t) = todos.get_mut(i as usize) {
                    *t = !*t;
                }
                App {
                    count: s.count,
                    todos,
                    noise: s.noise,
                }
            }
            Msg::Touch => App {
                count: s.count,
                todos: s.todos.clone(),
                noise: s.noise + 1,
            },
        }
    }
}

// ── 类型化视图词表（一绑定一变体）+ 派生/哨兵 ──
#[derive(Clone, PartialEq)]
enum DemoView {
    Count(u32),
    Remaining(u32),
}

/// remaining 的 L1 输入切片（忠实标量元组：(长度, 已完成数)）。
type RemainingInput = (usize, usize);

struct DemoMap {
    count_input: Sentinel<App, u32>,
    rem_input: Sentinel<App, RemainingInput>,
    remaining: Derived<App, RemainingInput, u32>,
}

const PATH_COUNT: &str = "/count";
const PATH_REMAINING: &str = "/derived/remaining";
const FIELD_REMAINING: &str = "remaining";
const KIND_COUNT: u16 = 1;
const KIND_REMAINING: u16 = 2;

impl DemoMap {
    fn new() -> Self {
        let rem_input = |s: &App| (s.todos.len(), s.todos.iter().filter(|d| **d).count());
        Self {
            count_input: Sentinel::new(|s: &App| s.count),
            rem_input: Sentinel::new(rem_input),
            remaining: Derived::new(rem_input, |(len, done): &RemainingInput| {
                (*len - *done) as u32
            }),
        }
    }
}

impl ViewMap<App> for DemoMap {
    type View = DemoView;
    fn bindings(&self) -> &[BindingDecl] {
        &[
            BindingDecl {
                path: PATH_COUNT,
                kind: PayloadTag(KIND_COUNT),
            },
            BindingDecl {
                path: PATH_REMAINING,
                kind: PayloadTag(KIND_REMAINING),
            },
        ]
    }
    fn input_changed(&self, index: usize, state: &App) -> bool {
        match index {
            0 => self.count_input.changed(state),
            1 => self.rem_input.changed(state),
            _ => true, // 越界 = 缺省无哨兵形（误报仅性能损失）
        }
    }
    fn project(&self, index: usize, state: &App) -> DemoView {
        match index {
            0 => DemoView::Count(state.count),
            _ => DemoView::Remaining(self.remaining.get(state)),
        }
    }
}

// ── 双编码器（穷尽 match + 全字段；复杂子树 json_blob 与 to_json 同源取值）──
struct DemoJson;

impl JsonEncoder for DemoJson {
    type View = DemoView;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &DemoView) -> Value {
        match v {
            DemoView::Count(n) => Value::from(*n),
            DemoView::Remaining(r) => json!({ FIELD_REMAINING: r }),
        }
    }
}

struct DemoRegion;

impl RegionEncoder for DemoRegion {
    type View = DemoView;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &DemoView, w: &mut RegionWriter) {
        match v {
            DemoView::Count(n) => w.u64(*n as u64),
            DemoView::Remaining(r) => w.json_blob(&json!({ FIELD_REMAINING: r })),
        }
    }
}

struct DemoIntent;

impl IntentMap<Msg> for DemoIntent {
    type Wire = Value;
    type Err = IntentError;
    fn intent(&self, wire: Value) -> Result<Msg, IntentError> {
        let op = wire
            .get("op")
            .and_then(Value::as_str)
            .ok_or_else(|| IntentError::malformed("missing `op`"))?;
        match op {
            "add" => {
                let n = wire
                    .get("n")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| IntentError::malformed("add: missing `n`"))?;
                Ok(Msg::Add(n as u32))
            }
            "push_todo" => Ok(Msg::PushTodo),
            "flip" => {
                let i = wire
                    .get("i")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| IntentError::malformed("flip: missing `i`"))?;
                Ok(Msg::Flip(i as u32))
            }
            "touch" => Ok(Msg::Touch),
            other => Err(IntentError::unknown(other)),
        }
    }
}

/// M3 断言③：demo 不引常量工件——`include_str!` 清单逐字对表（写下的路径/
/// 字段名/kind 与清单内容逐字一致，不一致即红）。
const MANIFEST: &str = include_str!("../../docs/view-bridge-constants/manifest.toml");

fn assert_manifest_matches_literals() {
    for needle in [
        "protocol_version = 1",
        &format!("path = \"{PATH_COUNT}\""),
        &format!("kind = {KIND_COUNT}"),
        &format!("path = \"{PATH_REMAINING}\""),
        &format!("kind = {KIND_REMAINING}"),
        &format!("fields = [\"{FIELD_REMAINING}\"]"),
    ] {
        assert!(
            MANIFEST.contains(needle),
            "manifest literal drift: {needle}"
        );
    }
}

fn main() {
    assert_manifest_matches_literals();
    assert_eq!(PROTOCOL_VERSION, 1);

    // ── 都开：水位自洽（断言链①）+ latest-wins + Touch 零补丁 + 跨通道同源 ──
    let store = SyncStore::new(AppReducer, NoFx);
    let bridge: ViewBridge<App, Msg, DemoMap, DemoIntent, DemoJson, DemoRegion> = ViewBridge::mount(
        store,
        DemoMap::new(),
        DemoIntent,
        Encoders {
            json: Some(DemoJson),
            region: Some((DemoRegion, Arc::new(VecBacking::new(8192)))),
        },
    );
    assert!(bridge.bind("/nope").is_err(), "unknown path must Err");
    assert_eq!(
        bridge.bind(PATH_COUNT).unwrap(),
        skeleton_infra::view::BindingId(0)
    );
    bridge.bind(PATH_REMAINING).unwrap();

    // 首挂快照：全量 + 投影数 == 绑定数 + version == 桥内消费代数（0 拍）。
    let snap = bridge.snapshot_doc();
    assert_eq!(snap.version, 0);
    assert_eq!(snap.doc.pointer(PATH_COUNT), Some(&json!(0)));
    assert_eq!(
        snap.doc.pointer(PATH_REMAINING),
        Some(&json!({ FIELD_REMAINING: 0 }))
    );

    // 语言边界入站：Value 进（未知意图 Err 显形）。
    assert!(bridge.send_intent(json!({"op": "nope"})).is_err());
    assert!(bridge.send_intent(json!({"op": "add", "n": 2})).unwrap());
    assert!(bridge.send_intent(json!({"op": "touch"})).unwrap());
    // todos 切片三拍（派生 remaining 的真实变更路径：2 条待办、1 条完成 ⇒ 1）。
    bridge.send_intent(json!({"op": "push_todo"})).unwrap();
    bridge.send_intent(json!({"op": "push_todo"})).unwrap();
    bridge.send_intent(json!({"op": "flip", "i": 0})).unwrap();

    // 折叠首取：5 拍未拉取 ⇒ latest-wins 一次交付两绑定现值。
    {
        let batch = bridge.take_patches();
        assert_eq!(batch.from_version, 0, "from == 上一批 to（首次 0）");
        assert_eq!(batch.to_version, 5);
        assert_eq!(batch.patches.len(), 2, "折叠：每脏绑定一条现值");
        assert_eq!(batch.patches[0].value, json!(2));
        assert_eq!(batch.patches[1].value, json!({ FIELD_REMAINING: 1 }));
        assert_eq!(bridge.region_version(), batch.to_version, "跨通道同源");
    }

    // 逐拍水位：to_version 逐拍 +1、from == 上一批 to、区域头 == 同拍 to_version。
    let mut applied = 5u64;
    for i in 1..=3u64 {
        bridge.send_intent(json!({"op": "add", "n": 1})).unwrap();
        let batch = bridge.take_patches();
        assert_eq!(batch.protocol_version, PROTOCOL_VERSION);
        assert_eq!(batch.from_version, applied, "from == 上一批 to");
        assert_eq!(batch.to_version, applied + 1, "to_version 逐拍 +1");
        assert_eq!(batch.patches.len(), 1, "latest-wins：仅 /count 现值");
        assert_eq!(batch.patches[0].value, json!(2 + i));
        assert_eq!(bridge.region_version(), batch.to_version, "跨通道同源");
        applied = batch.to_version;
    }
    assert_eq!(
        bridge.snapshot_doc().version,
        applied,
        "快照版本 == 桥内消费代数"
    );

    // 人为倒退（UI applied 落后）⇒ 缺段判定 ⇒ snapshot_doc 重同步（路径被走到）。
    applied = 1;
    let batch = bridge.take_patches();
    assert_ne!(batch.from_version, applied);
    let snap = bridge.snapshot_doc();
    applied = snap.version;
    assert_eq!(applied, bridge.region_version());

    // 快照即交付（补审 N2）：重同步后下一批 from_version == snapshot.version。
    bridge.send_intent(json!({"op": "add", "n": 1})).unwrap();
    let batch = bridge.take_patches();
    assert_eq!(
        batch.from_version, snap.version,
        "N2：快照重同步后 from == snapshot.version"
    );

    // Touch（无关切片）：零补丁、region_encodes 不变、projection_runs 不变。
    let encodes = bridge.region_encodes();
    let runs = bridge.projection_runs();
    bridge.send_intent(json!({"op": "touch"})).unwrap();
    let batch = bridge.take_patches();
    assert!(batch.patches.is_empty());
    assert_eq!(bridge.region_encodes(), encodes, "无关拍零编码");
    assert_eq!(bridge.projection_runs(), runs, "哨兵命中零投影");

    // ── 只开 Json：区域通道不存在 ──
    let store = SyncStore::new(AppReducer, NoFx);
    let only_json: ViewBridge<App, Msg, DemoMap, DemoIntent, DemoJson, DemoRegion> =
        ViewBridge::mount(
            store,
            DemoMap::new(),
            DemoIntent,
            Encoders {
                json: Some(DemoJson),
                region: None,
            },
        );
    only_json.bind(PATH_COUNT).unwrap();
    only_json.send_intent(json!({"op": "add", "n": 1})).unwrap();
    assert_eq!(only_json.region_version(), 0, "只开 Json：区域通道不存在");
    assert_eq!(only_json.region_id(), 0);

    // ── 只开 Region：take_patches 空批且区域照常更新 ──
    let store = SyncStore::new(AppReducer, NoFx);
    let backing = Arc::new(VecBacking::new(8192));
    let only_region: ViewBridge<App, Msg, DemoMap, DemoIntent, DemoJson, DemoRegion> =
        ViewBridge::mount(
            store,
            DemoMap::new(),
            DemoIntent,
            Encoders {
                json: None,
                region: Some((DemoRegion, backing.clone())),
            },
        );
    only_region.bind(PATH_COUNT).unwrap();
    only_region.bind(PATH_REMAINING).unwrap();
    only_region
        .send_intent(json!({"op": "add", "n": 7}))
        .unwrap();
    assert!(
        only_region.take_patches().patches.is_empty(),
        "只开 Region：JSON 批空"
    );
    assert_eq!(only_region.region_version(), 1);
    let rid = only_region.region_id();

    // 镜像读取器回读 + 双编码一致性（区域段回读 == to_json 语义）。
    let mut values = read_region(&backing);
    assert_eq!(
        values.remove(&skeleton_infra::view::BindingId(0)),
        Some(json!(7))
    );
    assert_eq!(
        values.remove(&skeleton_infra::view::BindingId(1)),
        Some(json!({ FIELD_REMAINING: 0 })),
        "json_blob 与 to_json 同源"
    );

    // ── 换区握手：小容量挤爆 arena ⇒ region_id 变化 + 版本延续 + 重映射可读 ──
    let store = SyncStore::new(AppReducer, NoFx);
    let small = Arc::new(VecBacking::new(192)); // header 32 + index 48 + arena 112
    let swapper: ViewBridge<App, Msg, DemoMap, DemoIntent, DemoJson, DemoRegion> =
        ViewBridge::mount(
            store,
            DemoMap::new(),
            DemoIntent,
            Encoders {
                json: None,
                region: Some((DemoRegion, small.clone())),
            },
        );
    swapper.bind(PATH_COUNT).unwrap();
    swapper.bind(PATH_REMAINING).unwrap();
    for _ in 0..8 {
        swapper.send_intent(json!({"op": "add", "n": 1})).unwrap();
    }
    assert!(swapper.region_id() != rid, "换区握手：region_id 变化");
    assert_eq!(swapper.region_version(), 8, "版本延续（单调不回退）");
    let values = read_region(&small);
    assert_eq!(
        values.get(&skeleton_infra::view::BindingId(0)),
        Some(&json!(8)),
        "重映射后可读现值"
    );

    // ── 未知标签跳过（断言链②）：手造含 tag=0x7F 的区域字节 ──
    let mut bytes = vec![0u8; 32 + 24 * 2];
    bytes[0..4].copy_from_slice(b"UVBR");
    bytes[4..6].copy_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    bytes[18..22].copy_from_slice(&2u32.to_le_bytes());
    bytes[22..30].copy_from_slice(&32u64.to_le_bytes()); // index_offset = 索引起始（紧随头）
    let arena_at = 32 + 48;
    let mut arena: Vec<u8> = Vec::new();
    let known_at = arena_at;
    arena.push(0); // tag 0 = u64
    arena.extend_from_slice(&8u32.to_le_bytes());
    arena.extend_from_slice(&42u64.to_le_bytes()); // 段长 13（5+8）
    let unknown_at = known_at + 16;
    arena.resize(16, 0); // known 段 8 字节对齐填充（offset 8 字节对齐纪律）
    arena.extend_from_slice(&[0x7F, 4, 0, 0, 0, b'j', b'u', b'n', b'k']); // 未知 tag + len 前缀
    let entry = |id: u32, kind: u16, off: u64, len: u32| {
        let mut e = [0u8; 24];
        e[0..4].copy_from_slice(&id.to_le_bytes());
        e[4..6].copy_from_slice(&kind.to_le_bytes());
        e[8..16].copy_from_slice(&off.to_le_bytes());
        e[16..20].copy_from_slice(&len.to_le_bytes());
        e
    };
    bytes[32..56].copy_from_slice(&entry(0, 1, known_at as u64, 13));
    bytes[56..80].copy_from_slice(&entry(1, 2, unknown_at as u64, 9));
    bytes.extend_from_slice(&arena);
    let read = RegionReader::read(&bytes).expect("结构字节必须可解");
    assert_eq!(read.values.len(), 1, "已知段照常解出");
    assert_eq!(
        read.values[0],
        (skeleton_infra::view::BindingId(0), json!(42))
    );
    assert_eq!(read.unknown_tags_skipped, 1, "未知 tag 计数");
    assert!(read.needs_resync, "resync 提示置位（禁整区失败、禁 panic）");

    // JSON 通道同款：未知 kind 项跳过 + 计数，其余项照常。
    let decls = DemoMap::new().bindings().to_vec();
    let batch_json = json!({
        "protocol_version": PROTOCOL_VERSION,
        "patches": [
            {"path": PATH_COUNT, "kind": KIND_COUNT, "value": 1},
            {"path": "/ghost", "kind": 0x7FFF, "value": 2}
        ]
    });
    let decoded = decode_batch(&batch_json, &decls);
    assert_eq!(decoded.patches.len(), 1);
    assert_eq!(decoded.unknown_tags_skipped, 1);
    assert!(decoded.needs_resync);

    println!("view_bridge_demo: all assertions passed");
}

/// 镜像读取器：背衬整读 → 规范读器 → BindingId → Value 映射。
fn read_region(
    backing: &Arc<VecBacking>,
) -> std::collections::HashMap<skeleton_infra::view::BindingId, Value> {
    let mut map = std::collections::HashMap::new();
    backing.with_bytes(&mut |bytes| {
        let read = RegionReader::read(bytes).expect("self-written region must parse");
        for (id, v) in read.values {
            map.insert(id, v);
        }
    });
    map
}
