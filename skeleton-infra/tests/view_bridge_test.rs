#![cfg(feature = "view")]
//! ViewBridge 行为面验收（规格 §8 验收②载体）：panic 毒化-重同步闭环（四步断
//! 言）/ 绑定封板 / 跨线程 pull / 桥 Drop / 闭门 send_intent 显式 `Ok(false)`
//! / 「input 覆盖实读」红线的最小反例（漏报可检出）/ 装配期校验（RFC 6901 路径
//! 编译 #146、登记身份 #104、覆盖缺口 #173、不完整快照拒绝 #104/#190）
//! ——并发与毒化场景 demo 装不下，在此机械化（断言链①②）。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::{Value, json};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    BindError, BindingId, Coverage, Encoders, IntentError, JsonEncoder, MountError, RegionBacking,
    RegionEncoder, RegionWriter, Sentinel, SnapshotError, VecBacking, ViewBridge,
    read::RegionReader,
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
    .expect("static declarations are well-formed")
}

/// #152/#189：采用方 `input_changed` panic 必须在桥边界收容——不得穿出
/// `dispatch`（否则 `SyncStore` 按订阅者 panic 摘除整条桥订阅），且计数 + 毒化
/// 显形，解除注入后后续拍仍到达（订阅未被摘）。
#[test]
fn sentinel_panic_is_contained_and_keeps_the_subscription() {
    static DECLS: [BindingDecl; 1] = [BindingDecl {
        path: "/count",
        kind: PayloadTag(1),
    }];

    struct SentinelMap {
        arm: Arc<AtomicBool>,
    }

    impl ViewMap<App> for SentinelMap {
        type View = V;
        fn bindings(&self) -> &[BindingDecl] {
            &DECLS
        }
        fn project(&self, _: usize, s: &App) -> V {
            V::C(s.count)
        }
        fn input_changed(&self, _: usize, _: &App) -> bool {
            assert!(!self.arm.load(Ordering::SeqCst), "injected sentinel panic");
            true
        }
    }

    let arm = Arc::new(AtomicBool::new(false));
    let store = SyncStore::new(AppReducer, NoFx);
    let bridge = ViewBridge::<App, u32, SentinelMap, Intent, Json, Region>::mount(
        store,
        SentinelMap {
            arm: Arc::clone(&arm),
        },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    )
    .expect("static declarations are well-formed");
    bridge.bind("/count").unwrap();
    bridge.seal().expect("seal before dispatch");

    assert!(bridge.send_intent(json!(1)).unwrap());
    arm.store(true, Ordering::SeqCst); // 下一拍哨兵 panic
    assert!(
        bridge.send_intent(json!(2)).unwrap(),
        "哨兵 panic 不得穿出 dispatch（否则订阅被摘）"
    );
    assert_eq!(bridge.projection_panics(), 1, "哨兵 panic 必须计数");
    assert!(bridge.take_patches().resync, "毒化必须经返回值显形");

    // 订阅未被摘除：解除注入后下一拍仍产出补丁（被摘则这里恒空）。
    arm.store(false, Ordering::SeqCst);
    assert!(bridge.send_intent(json!(4)).unwrap());
    let batch = bridge.take_patches();
    assert_eq!(
        batch.patches.len(),
        1,
        "后续拍必须仍到达：{:?}",
        batch.patches
    );
}

/// 毒化-重同步闭环四步断言（规格 §8②）：panic ⇒ 计数+闩 ⇒ 次批 resync ⇒
/// snapshot_doc 全量且复位 ⇒ 后续变更补丁恢复到达。
#[test]
fn poison_resync_cycle() {
    let arm = Arc::new(AtomicBool::new(false));
    let bridge = build(Arc::clone(&arm));
    bridge.bind("/count").unwrap();
    bridge.seal().expect("seal before dispatch");

    assert!(bridge.send_intent(json!(2)).unwrap());
    arm.store(true, Ordering::SeqCst); // 下一拍注入 panic
    assert!(bridge.send_intent(json!(3)).unwrap());

    // ① 计数 + 闩：panic 被桥内 catch，禁裸传禁吞
    assert_eq!(bridge.projection_panics(), 1);
    // ② 次批携带 resync == true（完整性事实必达 UI）
    let batch = bridge.take_patches();
    assert!(batch.resync, "毒化必须经返回值显形");
    assert_eq!(batch.to_version, 6, "封板 2 + 两拍 ×2");
    // ③ snapshot_doc 版本校验下的重投影修复且毒化复位
    arm.store(false, Ordering::SeqCst);
    let snap = bridge
        .snapshot_doc()
        .expect("repair succeeds once project stops panicking");
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

/// 绑定封板（F1）：`seal` 之后 bind ⇒ 显式 Err（只读无锁保证）。
#[test]
fn bind_sealed_after_seal() {
    let bridge = build(Arc::new(AtomicBool::new(false)));
    bridge.bind("/count").unwrap();
    bridge.seal().unwrap();
    let err = bridge.bind("/count").unwrap_err();
    assert!(err.to_string().contains("sealed"), "封板错误须显形: {err}");
}

/// 跨线程 pull（F1）：桥 Send+Sync，宿主线程 take_patches；快照即交付（N2）。
#[test]
fn cross_thread_pull() {
    let bridge = build(Arc::new(AtomicBool::new(false)));
    bridge.bind("/count").unwrap();
    bridge.seal().unwrap();
    assert!(bridge.send_intent(json!(4)).unwrap());
    let snapshot = bridge.snapshot_doc().expect("sealed committed snapshot");
    std::thread::spawn(move || {
        let batch = bridge.take_patches();
        assert_eq!(batch.to_version, 4, "封板 2 + 一拍 ×2");
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
    )
    .expect("static declarations are well-formed");
    bridge.bind("/count").unwrap();
    bridge.seal().unwrap();
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
    )
    .expect("static declarations are well-formed");
    bridge.bind("/count").unwrap();
    bridge.seal().expect("闭门 store 的快照仍可读");
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
        )
        .expect("static declarations are well-formed");
    bridge.bind("/sum").unwrap();
    bridge.seal().expect("seal before dispatch");
    assert!(bridge.send_intent(json!("a")).unwrap());
    assert_eq!(bridge.take_patches().patches.len(), 1, "首拍必须成补丁");
    assert!(bridge.send_intent(json!("b")).unwrap());
    let batch = bridge.take_patches();
    Observed {
        patches: batch.patches.len(),
        to_version: batch.to_version,
        runs: bridge.projection_runs(),
        doc: bridge.snapshot_doc().expect("committed snapshot").doc,
    }
}

/// 漏报反例：哨兵声明窄于实读 ⇒ `b` 变更被跳过、无补丁；两处签名一起构成
/// 「可检出」——「静默陈旧」不是真的静默。
#[test]
fn narrow_input_leaks_stale_and_is_detectable() {
    // 正例：哨兵覆盖实读 ⇒ 两拍都投影、都成补丁（+1 次封板投影）。
    let cover = bump_b_with(Sentinel::new(|s: &Pair| (s.a, s.b)));
    assert_eq!(
        (cover.patches, cover.runs, cover.to_version),
        (1, 3, 6),
        "覆盖实读：b 变更必须被哨兵看见"
    );
    assert_eq!(cover.doc.pointer("/sum"), Some(&json!(2)));

    // 反例（违约形态）：哨兵只声明 `a` ⇒ 漏报。
    let leak = bump_b_with(Sentinel::new(|s: &Pair| s.a));
    assert_eq!(leak.patches, 0, "漏报 = 静默陈旧：b 变了但无补丁到达");
    assert_eq!(
        leak.runs, 2,
        "签名①：projection_runs 停在 2（封板 1 + 首拍 1，第 2 拍没投影）"
    );
    // 签名②：水位照常推进（2）而补丁为空——UI 侧唯一可见的「状态变了」信号。
    assert_eq!(leak.to_version, 6, "版本推进与投影解耦：空批也推进水位");
    // #104 之后快照吃**已提交投影值**（不再独立采 Store 套新版本），所以它也停在
    // 陈旧值 1：快照是交付基线，不是真相判决器。真值分歧只能靠采用方自查 Store。
    assert_eq!(
        leak.doc.pointer("/sum"),
        Some(&json!(1)),
        "快照 = 已提交投影值（#104：同一临界区的内容与版本）"
    );
}

// ── 区域通道挂载缺口（#171）：真实 `VecBacking` 跑 RegionChannel → publish → 读器 ──
//
// 旧版 `build()` 一律 `region: None`，region 行为只由 example（CI 不跑 main）承载。
// 本组把「区段编码 → 索引/头发布 → 规范读器回读」放进 integration test。

/// `V` 的区域编码（一绑定恰一段非空完整 TLV）。
struct RegionEnc;

impl RegionEncoder for RegionEnc {
    type View = V;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &V, w: &mut RegionWriter) {
        match v {
            V::C(n) => w.u64(u64::from(*n)),
        }
    }
}

fn decls() -> Vec<BindingDecl> {
    BoomMap {
        arm: Arc::new(AtomicBool::new(false)),
    }
    .bindings()
    .to_vec()
}

/// 背衬整读拷贝 → 规范快照读器（不可变快照入口，普通 load）。
fn read_backing(
    backing: &VecBacking,
    decls: &[BindingDecl],
) -> skeleton_infra::view::read::RegionRead {
    let mut slot = None;
    backing.with_bytes(&mut |bytes| {
        slot = Some(RegionReader::read_snapshot(bytes, decls).expect("self-written region parses"));
    });
    slot.expect("with_bytes invoked exactly once")
}

#[test]
fn region_mount_publishes_and_round_trips() {
    use skeleton::view::region_layout::{INITIAL_SEQ, SEQ_STEP};

    let backing = Arc::new(VecBacking::new(4096));
    let store = SyncStore::new(AppReducer, NoFx);
    let bridge: ViewBridge<App, u32, BoomMap, Intent, Json, RegionEnc> = ViewBridge::mount(
        store,
        BoomMap {
            arm: Arc::new(AtomicBool::new(false)),
        },
        Intent,
        Encoders {
            json: Some(Json),
            region: Some((RegionEnc, Arc::clone(&backing) as Arc<dyn RegionBacking>)),
        },
    )
    .expect("static declarations are well-formed");
    bridge.bind("/count").unwrap();
    bridge.seal().expect("seal publishes the initial region");
    assert_eq!(bridge.region_failure(), None, "装配期无失败");

    // 封板即初始区域**全量**发布（#155/#190：不等首条变更）。
    assert_eq!(bridge.region_version(), INITIAL_SEQ);
    let decls = decls();
    let sealed = read_backing(&backing, &decls);
    assert_eq!(sealed.sequence, INITIAL_SEQ);
    assert_eq!(
        sealed.values,
        vec![(BindingId(0), json!(0))],
        "封板全量：已登记绑定立即有值"
    );

    // 一拍：区域 eager 发布 → 回读 == JSON 语义（双编码一致性）。
    assert!(bridge.send_intent(json!(2)).unwrap());
    assert_eq!(bridge.region_version(), INITIAL_SEQ + SEQ_STEP);
    let read = read_backing(&backing, &decls);
    assert_eq!(read.sequence, bridge.region_version(), "区域头 == 桥水位");
    assert_eq!(read.region_id, bridge.region_id());
    assert_eq!(read.values, vec![(BindingId(0), json!(2))]);
    assert!(!read.needs_resync);
    assert_eq!(bridge.region_failure(), None);
}

// ── 装配期校验（#146 路径编译 / #104 登记身份与快照完整性 / #173 覆盖缺口） ──

const fn decl(path: &'static str) -> BindingDecl {
    BindingDecl {
        path,
        kind: PayloadTag(1),
    }
}

/// 纯声明载体：装配期校验的各种（非法）静态表都由它喂进 `mount`。
struct DeclMap(&'static [BindingDecl]);

impl ViewMap<App> for DeclMap {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        self.0
    }
    fn project(&self, index: usize, s: &App) -> V {
        V::C(s.count + index as u32)
    }
}

static NO_SLASH: [BindingDecl; 1] = [decl("count")];
static ROOT_POINTER: [BindingDecl; 1] = [decl("")];
static BAD_ESCAPE: [BindingDecl; 1] = [decl("/a~2b")];
static TRAILING_TILDE: [BindingDecl; 1] = [decl("/a~")];
static DUPLICATE: [BindingDecl; 2] = [decl("/a"), decl("/a")];
static PREFIX_COLLISION: [BindingDecl; 2] = [decl("/a"), decl("/a/b")];
static ESCAPED: [BindingDecl; 3] = [decl("/a~1b"), decl("/t~0x"), decl("/plain")];
static TWO: [BindingDecl; 2] = [decl("/count"), decl("/other")];

fn mount_of_decls(
    decls: &'static [BindingDecl],
    region: Option<Arc<VecBacking>>,
) -> Result<ViewBridge<App, u32, DeclMap, Intent, Json, RegionEnc>, MountError> {
    ViewBridge::mount(
        SyncStore::new(AppReducer, NoFx),
        DeclMap(decls),
        Intent,
        Encoders {
            json: Some(Json),
            region: region.map(|b| (RegionEnc, b as Arc<dyn RegionBacking>)),
        },
    )
}

/// （声明表, 期望的装配期错误判定）。
type PathCase = (&'static [BindingDecl], fn(&MountError) -> bool);

/// #146：非根绝对指针是明确子集——缺前导 `/`、根指针、非法 `~` 转义、重复解码
/// 指针、前缀冲突全部在**装配期** typed 拒绝（不是 debug 断言，也不是装配后崩）。
#[test]
fn mount_rejects_invalid_and_colliding_pointer_tables() {
    let cases: [PathCase; 6] = [
        (&NO_SLASH, |e| {
            matches!(e, MountError::InvalidPath { path: "count", .. })
        }),
        (&ROOT_POINTER, |e| {
            matches!(e, MountError::InvalidPath { path: "", .. })
        }),
        (&BAD_ESCAPE, |e| {
            matches!(e, MountError::InvalidPath { path: "/a~2b", .. })
        }),
        (&TRAILING_TILDE, |e| {
            matches!(e, MountError::InvalidPath { path: "/a~", .. })
        }),
        (&DUPLICATE, |e| {
            matches!(e, MountError::DuplicatePath { path: "/a" })
        }),
        (&PREFIX_COLLISION, |e| {
            matches!(
                e,
                MountError::PrefixCollision {
                    path: "/a/b",
                    other: "/a"
                }
            )
        }),
    ];
    for (decls, want) in cases {
        let err = mount_of_decls(decls, None)
            .err()
            .expect("must fail assembly");
        assert!(want(&err), "unexpected mount error: {err}");
    }
}

/// #146：`~1`/`~0` 在装配期解码——文档键是解码后的段名，原始拼写不得成为字面键。
#[test]
fn compiled_pointers_decode_escapes_into_document_keys() {
    let bridge = mount_of_decls(&ESCAPED, None).expect("valid pointer table");
    for path in ["/a~1b", "/t~0x", "/plain"] {
        bridge.bind(path).unwrap();
    }
    bridge.seal().unwrap();
    let doc = bridge.snapshot_doc().expect("committed snapshot").doc;
    assert_eq!(doc.pointer("/a~1b"), Some(&json!(0)), "键 = a/b");
    assert_eq!(doc.pointer("/t~0x"), Some(&json!(1)), "键 = t~x");
    assert_eq!(doc.pointer("/plain"), Some(&json!(2)));
    assert!(doc.get("a~1b").is_none(), "原始拼写不得成为字面键");
    assert!(doc.get("a").is_none(), "段名不得被拆成嵌套对象");
}

/// #104 req 1：`BindingId` = **登记序**（不是 `bindings()` 序）；未登记声明不投影、
/// 不进 JSON 文档、不进区域索引。
#[test]
fn binding_identity_follows_registration_order_only() {
    let backing = Arc::new(VecBacking::new(4096));
    let bridge = mount_of_decls(&TWO, Some(Arc::clone(&backing))).expect("valid pointer table");
    assert_eq!(
        bridge.bind("/other").unwrap(),
        BindingId(0),
        "ID 跟随登记序，不是声明序"
    );
    bridge.seal().expect("seal publishes the initial region");

    let doc = bridge.snapshot_doc().expect("committed snapshot").doc;
    assert_eq!(doc.pointer("/other"), Some(&json!(1)), "登记绑定照常投影");
    assert!(doc.get("count").is_none(), "未登记声明不投影不发布");

    let read = read_backing(&backing, &TWO);
    assert_eq!(
        read.values,
        vec![(BindingId(0), json!(1))],
        "区域条目 id = 登记序，未登记声明无条目"
    );
}

/// #104 req 2/3：投影 panic 不得让后续值挤进前一个路径；边界不完整的快照必须
/// 显式失败，修复后内容与水位同一一致性点。
struct PairMap {
    boom: Arc<AtomicBool>,
}

impl ViewMap<App> for PairMap {
    type View = V;
    fn bindings(&self) -> &[BindingDecl] {
        &TWO
    }
    fn project(&self, index: usize, s: &App) -> V {
        if index == 0 {
            assert!(
                !self.boom.load(Ordering::SeqCst),
                "injected projection panic"
            );
            V::C(s.count)
        } else {
            V::C(s.count + 100)
        }
    }
}

#[test]
fn snapshot_never_shifts_values_after_a_projection_panic() {
    let boom = Arc::new(AtomicBool::new(false));
    let bridge: ViewBridge<App, u32, PairMap, Intent, Json, RegionEnc> = ViewBridge::mount(
        SyncStore::new(AppReducer, NoFx),
        PairMap {
            boom: Arc::clone(&boom),
        },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    )
    .expect("valid pointer table");
    bridge.bind("/count").unwrap();
    bridge.bind("/other").unwrap();
    bridge.seal().unwrap();

    boom.store(true, Ordering::SeqCst);
    assert!(bridge.send_intent(json!(5)).unwrap());
    assert_eq!(bridge.projection_panics(), 1);
    // 旧实现把 /other 的成功投影挤到 /count 的路径上；新契约直接判不完整。
    assert!(
        matches!(bridge.snapshot_doc(), Err(SnapshotError::Incomplete)),
        "残缺快照禁当完整文档交付"
    );

    boom.store(false, Ordering::SeqCst);
    let snap = bridge
        .snapshot_doc()
        .expect("repair once projection recovers");
    assert_eq!(snap.version, 4, "修复与水位同一一致性点：封板 2 + 一拍 2");
    assert_eq!(snap.doc.pointer("/count"), Some(&json!(5)));
    assert_eq!(snap.doc.pointer("/other"), Some(&json!(105)));
    assert_eq!(bridge.projection_panics(), 2, "修复那次也 panic 过一次");
}

/// 未被任何编码器覆盖的绑定：全构建 typed 失败，且失败的 bind 不占 ID、不改登记态
/// （#173）。
#[test]
fn uncovered_binding_fails_visibly_and_consumes_no_id() {
    struct OnlyCount;

    impl JsonEncoder for OnlyCount {
        type View = V;
        fn covers(&self) -> Coverage {
            Coverage::Only(&["/count"])
        }
        fn to_json(&self, v: &V) -> Value {
            match v {
                V::C(n) => json!(n),
            }
        }
    }

    let bridge: ViewBridge<App, u32, DeclMap, Intent, OnlyCount, Region> = ViewBridge::mount(
        SyncStore::new(AppReducer, NoFx),
        DeclMap(&TWO),
        Intent,
        Encoders {
            json: Some(OnlyCount),
            region: None,
        },
    )
    .expect("valid pointer table");
    let err = bridge.bind("/other").unwrap_err();
    assert!(
        matches!(&err, BindError::UncoveredBinding { path } if path.as_str() == "/other"),
        "{err}"
    );
    assert_eq!(
        bridge.bind("/count").unwrap(),
        BindingId(0),
        "失败的 bind 不得占 ID"
    );
}
