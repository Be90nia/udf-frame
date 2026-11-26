#![cfg(feature = "view")]
//! ViewBridge 行为面验收（规格 §8 验收②载体）：panic 毒化-重同步闭环（四步断
//! 言）/ 绑定封板 / 跨线程 pull / 桥 Drop / 闭门 send_intent 显式 `Ok(false)`
//! / 「input 覆盖实读」红线的最小反例（漏报可检出）/ 装配期校验（RFC 6901 路径
//! 编译 #146、登记身份 #104、覆盖缺口 #173、不完整快照拒绝 #104/#190）
//! ——并发与毒化场景 demo 装不下，在此机械化（断言链①②）。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use serde_json::{Value, json};
use skeleton::view::region_layout::{
    ENTRY_LEN, ENTRY_OFFSET_LEN, ENTRY_OFFSET_OFFSET, HEADER_LEN, INITIAL_SEQ,
    OFFSET_BINDING_COUNT, SEG_ALIGN,
};
use skeleton::view::{BindingDecl, IntentMap, PayloadTag, ViewMap};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::view::{
    BindError, BindingId, Coverage, Encoders, IntentError, JsonEncoder, MountError, RegionBacking,
    RegionEncoder, RegionWriter, Sentinel, SeqError, SnapshotError, VecBacking, ViewBridge,
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
    // 「判不出未变 ⇒ 按已变更投影」（#167 Round 4）：panic 那一拍必须**真的**投影。
    // 只毒化不投影的变异 ⇒ runs 少 1、补丁为空（两条断言各自变红）。
    assert_eq!(
        bridge.projection_runs(),
        3,
        "seal 1 + 正常拍 1 + 毒化拍 1：panic 拍不得跳过投影"
    );
    let batch = bridge.take_patches();
    assert!(batch.resync, "毒化必须经返回值显形");
    assert_eq!(
        batch.patches.len(),
        1,
        "毒化拍仍交付已变更补丁：{:?}",
        batch.patches
    );
    assert_eq!(
        batch.patches[0].value,
        json!(3),
        "fail-open 投影读的是恐慌拍的真实状态（1 + 2）"
    );

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
///
/// `Sync` 上界：#157 之后 `Sentinel<Pair, In>: Sync` 需要 `In: Send + Sync`，而桥的
/// `mount` 要求 `M: Send + Sync`。
fn bump_b_with<In: PartialEq + Send + Sync + 'static>(sentinel: Sentinel<Pair, In>) -> Observed {
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

/// #167 req 1（#222 §2.1）：`bindings()` 只在**装配期**捕获一次——运行期
/// `bind` / 封板 / 通知 / 快照重投影一律读冻结表，零调用（判别：把任一条运行期
/// 路径塞回 `map.bindings()` 即红）。
#[test]
fn declarations_are_captured_once_at_mount_time() {
    static COUNT_ONLY: [BindingDecl; 1] = [decl("/count")];

    struct CountingMap {
        calls: Arc<AtomicU32>,
    }

    impl ViewMap<App> for CountingMap {
        type View = V;
        fn bindings(&self) -> &[BindingDecl] {
            self.calls.fetch_add(1, Ordering::SeqCst);
            &COUNT_ONLY
        }
        fn project(&self, _: usize, s: &App) -> V {
            V::C(s.count)
        }
    }

    let calls = Arc::new(AtomicU32::new(0));
    let bridge: ViewBridge<App, u32, CountingMap, Intent, Json, Region> = ViewBridge::mount(
        SyncStore::new(AppReducer, NoFx),
        CountingMap {
            calls: Arc::clone(&calls),
        },
        Intent,
        Encoders {
            json: Some(Json),
            region: None,
        },
    )
    .expect("valid pointer table");
    assert_eq!(calls.load(Ordering::SeqCst), 1, "装配期恰捕获一次");

    bridge.bind("/count").unwrap();
    bridge.seal().unwrap();
    assert!(bridge.send_intent(json!(3)).unwrap());
    let _ = bridge.take_patches();
    bridge.snapshot_doc().expect("committed snapshot");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "运行期 bind/seal/通知/快照重投影都不许再调用 bindings()"
    );
}

/// #167 req 4/5：`bindings()` panic 在**装配边界**换成 typed 错误——不裸 panic、
/// 不留半装配桥（Err 路径 Store 随 `mount` 弃置 ⇒ 上行句柄闭门）。判别：删
/// `mount` 里 `catch_unwind(AssertUnwindSafe(|| map.bindings()))` 即红。
#[test]
fn declarations_panic_is_a_typed_mount_error() {
    struct PanicDecls;

    impl ViewMap<App> for PanicDecls {
        type View = V;
        fn bindings(&self) -> &[BindingDecl] {
            panic!("injected bindings panic");
        }
        fn project(&self, _: usize, _: &App) -> V {
            V::C(0)
        }
    }

    let store = SyncStore::new(AppReducer, NoFx);
    let sender = skeleton::Store::sender(&store); // mount 前克隆上行句柄
    let mounted: Result<ViewBridge<App, u32, PanicDecls, Intent, Json, Region>, MountError> =
        ViewBridge::mount(
            store,
            PanicDecls,
            Intent,
            Encoders {
                json: Some(Json),
                region: None,
            },
        );
    let err = mounted.err().expect("panic 不得穿出 mount");
    assert_eq!(
        err.to_string(),
        "ViewMap::bindings panicked",
        "typed 错误的文案必须可读"
    );
    assert!(matches!(err, MountError::DeclarationsPanicked), "{err}");
    assert!(!sender.dispatch(1), "Err 路径不得留下仍在接单的半装配句柄");
}

/// #167 req 4（同一收容口径的另一半）：编码器 `covers()` panic 也是装配期 typed
/// 错误，两通道各自点名（json / region）。
#[test]
fn coverage_panic_is_a_typed_mount_error_per_channel() {
    struct PanicCoversJson;

    impl JsonEncoder for PanicCoversJson {
        type View = V;
        fn covers(&self) -> Coverage {
            panic!("injected json covers panic");
        }
        fn to_json(&self, _: &V) -> Value {
            json!(0)
        }
    }

    struct PanicCoversRegion;

    impl RegionEncoder for PanicCoversRegion {
        type View = V;
        fn covers(&self) -> Coverage {
            panic!("injected region covers panic");
        }
        fn encode(&self, _: &V, _: &mut RegionWriter) {}
    }

    let json_err = ViewBridge::<App, u32, DeclMap, Intent, PanicCoversJson, Region>::mount(
        SyncStore::new(AppReducer, NoFx),
        DeclMap(&TWO),
        Intent,
        Encoders {
            json: Some(PanicCoversJson),
            region: None,
        },
    )
    .err()
    .expect("json covers panic 不得穿出 mount");
    assert_eq!(json_err.to_string(), "json encoder covers() panicked");
    assert!(matches!(
        json_err,
        MountError::CoveragePanicked { channel: "json" }
    ));

    let region_err = ViewBridge::<App, u32, DeclMap, Intent, Json, PanicCoversRegion>::mount(
        SyncStore::new(AppReducer, NoFx),
        DeclMap(&TWO),
        Intent,
        Encoders {
            json: None,
            region: Some((
                PanicCoversRegion,
                Arc::new(VecBacking::new(4096)) as Arc<dyn RegionBacking>,
            )),
        },
    )
    .err()
    .expect("region covers panic 不得穿出 mount");
    assert_eq!(region_err.to_string(), "region encoder covers() panicked");
    assert!(matches!(
        region_err,
        MountError::CoveragePanicked { channel: "region" }
    ));
}

// ── #169 / #174：发布与通知热路径的分配面（确定性事件计数）+ 发布顺序/补零 ──────
//
// 判别力口径 = **预热后**窗内分配事件数：每绑定只剩 V2 的投影值句柄分配
// （`Arc::new`）；发布面（段字节/填充/索引镜像）与通知 scratch（pending / changed /
// retired / prev）都必须与绑定数无关。回退成 `to_vec() + clone()` ⇒ 每绑定 +2；
// 去掉 scratch 复用 ⇒ 每拍常数项 +4。

/// 事件计数分配器（形态照 `tests/view_bridge_two_phase.rs`：集成测试自带全局分配器；
/// 只计**窗内本线程**的分配，libtest 并发线程的分配不进窗）。
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

/// N 绑定的宽面夹具：每绑定投影一个 u32（值与位置绑定，`PartialEq` 可分辨）。
/// `filtered` = 哨兵过滤 workload：每拍只有 `count % N` 那个绑定报变更。
struct Wide {
    decls: &'static [BindingDecl],
    filtered: bool,
}

impl ViewMap<App> for Wide {
    type View = u32;
    fn bindings(&self) -> &[BindingDecl] {
        self.decls
    }
    fn project(&self, index: usize, s: &App) -> u32 {
        s.count.wrapping_mul(1_000).wrapping_add(index as u32)
    }
    fn input_changed(&self, index: usize, s: &App) -> bool {
        !self.filtered || (s.count as usize) % self.decls.len() == index
    }
}

struct WideJson;

impl JsonEncoder for WideJson {
    type View = u32;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &u32) -> Value {
        json!(v)
    }
}

struct WideRegion;

impl RegionEncoder for WideRegion {
    type View = u32;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &u32, w: &mut RegionWriter) {
        w.u64(u64::from(*v));
    }
}

type WideBridge = ViewBridge<App, u32, Wide, Intent, WideJson, WideRegion>;

/// `n` 条声明（`BindingDecl.path` 要求 `&'static str`，测试进程内一次性泄漏即可）。
fn wide_decls(n: usize) -> &'static [BindingDecl] {
    let decls: Vec<BindingDecl> = (0..n)
        .map(|i| BindingDecl {
            path: Box::leak(format!("/b{i}").into_boxed_str()),
            kind: PayloadTag(1),
        })
        .collect();
    Box::leak(decls.into_boxed_slice())
}

/// 建桥 → 全绑定 → 封板。`region_capacity` = `None` ⇒ 只开 JSON 通道。
fn wide_bridge(
    decls: &'static [BindingDecl],
    filtered: bool,
    region_capacity: Option<usize>,
) -> WideBridge {
    let bridge = ViewBridge::mount(
        SyncStore::new(AppReducer, NoFx),
        Wide { decls, filtered },
        Intent,
        Encoders {
            json: Some(WideJson),
            region: region_capacity.map(|capacity| {
                (
                    WideRegion,
                    Arc::new(VecBacking::new(capacity)) as Arc<dyn RegionBacking>,
                )
            }),
        },
    )
    .expect("static declarations are well-formed");
    for decl in decls {
        bridge.bind(decl.path).expect("declared + covered");
    }
    bridge.seal().expect("seal before dispatch");
    bridge
}

/// #169：稳态 publish 的**每变更绑定**分配为 0（预热后）。
///
/// 断言 `a(N) − a(1) == N − 1`：每多一个变更绑定只多一次分配（投影值 `Arc::new`），
/// 发布面（段字节 / 填充 / 索引镜像 / 偏移表）是每拍常数。
#[test]
fn region_publish_has_no_per_binding_allocation() {
    assert!(alloc_probe::self_check() >= 1, "仪器自检：窗内分配必须可见");
    let intent = json!(1);
    let mut per = Vec::new();
    for bindings in [1usize, 16, 256] {
        let bridge = wide_bridge(wide_decls(bindings), false, Some(1 << 20));
        for _ in 0..6 {
            assert!(bridge.send_intent(intent.clone()).unwrap()); // 预热：scratch 容量 + 缓冲生长
        }
        let (allocs, bytes, ()) = alloc_probe::allocs(|| {
            assert!(bridge.send_intent(intent.clone()).unwrap());
        });
        eprintln!("#169 变更绑定 {bindings}：{allocs} 次分配 / {bytes}B");
        // 实测 = N + 2（索引镜像 1 + 段落位表 1，均为每拍常数）；去掉批 scratch 复用
        // ⇒ +2、段字节回退成 `to_vec()+clone()` ⇒ 再 +2N，两者都会被这一条咬住。
        assert!(
            allocs <= bindings as u64 + 3,
            "每拍分配超出「N + 2」预算（绑定 {bindings}：{allocs} 次）"
        );
        per.push((bindings as u64, allocs));
    }
    for pair in per.windows(2) {
        let (n0, a0) = pair[0];
        let (n1, a1) = pair[1];
        assert_eq!(
            a1 - a0,
            n1 - n0,
            "每多一个变更绑定只许多 1 次分配（{n0}→{n1}：{a0}→{a1}）"
        );
    }
}

/// #174：通知路径的每拍分配在两种 workload 下都与绑定数无关（scratch 复用）。
///
/// 默认 `input_changed`（全绑定投影）⇒ 每拍 = N + 常数；哨兵过滤（每拍只 1 个绑定变更）
/// ⇒ 每拍 = 1 + 常数。常数项跨 N 恒定 = scratch 容量被复用（去掉复用 ⇒ +4）。
#[test]
fn notify_scratch_reuse_keeps_per_action_cost_flat() {
    assert!(alloc_probe::self_check() >= 1, "仪器自检：窗内分配必须可见");
    let intent = json!(1);
    for filtered in [false, true] {
        let mut per = Vec::new();
        for bindings in [1usize, 16, 64, 256] {
            let bridge = wide_bridge(wide_decls(bindings), filtered, None);
            for _ in 0..6 {
                assert!(bridge.send_intent(intent.clone()).unwrap());
            }
            let (allocs, bytes, ()) = alloc_probe::allocs(|| {
                assert!(bridge.send_intent(intent.clone()).unwrap());
            });
            eprintln!("#174 filtered={filtered} 绑定 {bindings}：{allocs} 次分配 / {bytes}B");
            let per_binding = if filtered { 1 } else { bindings as u64 };
            // 实测 = 每绑定 1 次（投影值装箱）+ 0 常数（scratch 全复用）；去掉
            // pending/changed/retired/prev 任一复用 ⇒ 常数项 +1，四条全去 ⇒ +4。
            assert!(
                allocs <= per_binding + 1,
                "每拍分配超出「每绑定 1 次」预算（filtered={filtered} 绑定 {bindings}：{allocs} 次）"
            );
            per.push((bindings, allocs - per_binding));
        }
        // 常数项（每拍非绑定面分配）跨 N 恒定。
        for pair in per.windows(2) {
            let (n0, k0) = pair[0];
            let (n1, k1) = pair[1];
            assert_eq!(
                k0, k1,
                "filtered={filtered} 每拍常数分配随绑定数变化（{n0}→{n1}：{k0}→{k1}）"
            );
        }
    }
}

// ── 发布事务：顺序（odd → 段字节 → 索引+头 → even）、单批边界、目标区补零 ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Seq(u64),
    Bytes,
}

/// 录制背衬：记 op 序列，字节与原子序列委托 [`VecBacking`]；整块预置 0xAA ⇒ 漏补零的
/// 填充字节会残留 0xAA 被下面的断言咬住。
struct Recording {
    inner: VecBacking,
    ops: std::sync::Mutex<Vec<Op>>,
}

impl Recording {
    fn new(capacity: usize) -> Self {
        let inner = VecBacking::new(capacity);
        inner.with_bytes_mut(&mut |buf| buf.fill(0xAA));
        Self {
            inner,
            ops: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn take_ops(&self) -> Vec<Op> {
        std::mem::take(&mut *self.ops.lock().unwrap())
    }

    fn bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.inner.with_bytes(&mut |b| out.extend_from_slice(b));
        out
    }
}

impl RegionBacking for Recording {
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8])) {
        self.ops.lock().unwrap().push(Op::Bytes);
        self.inner.with_bytes_mut(write);
    }
    fn with_bytes(&self, read: &mut dyn FnMut(&[u8])) {
        self.inner.with_bytes(read);
    }
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError> {
        self.ops.lock().unwrap().push(Op::Seq(seq));
        self.inner.store_publish_seq(seq)
    }
    fn load_publish_seq(&self) -> Result<u64, SeqError> {
        self.inner.load_publish_seq()
    }
}

/// 三种载荷长度（三种 tag，长度都不是 8 的倍数 ⇒ 填充真被走到）。
struct MixedMap {
    decls: &'static [BindingDecl],
}

impl ViewMap<App> for MixedMap {
    type View = u32;
    fn bindings(&self) -> &[BindingDecl] {
        self.decls
    }
    fn project(&self, index: usize, s: &App) -> u32 {
        // `% 3` 恒等于 `index % 3`：每绑定固定一种 tag（值仍随状态变化）。
        s.count.wrapping_mul(3).wrapping_add(index as u32)
    }
}

struct MixedRegion;

impl RegionEncoder for MixedRegion {
    type View = u32;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &u32, w: &mut RegionWriter) {
        match v % 3 {
            0 => w.u64(u64::from(*v)),
            1 => w.utf8("hello"),
            _ => w.json_blob(&json!({"k": [*v, 1, 2, 3]})),
        }
    }
}

/// 发布顺序 + 单批边界 + 目标区补零：一条测试咬住 #169 的三条「不变项」。
///
/// 变异：交换两次 `with_bytes_mut` / 提前写 even / 去掉补零 ⇒ 各自变红。
#[test]
fn region_publish_order_and_padding_are_unchanged() {
    let decls = wide_decls(3);
    let backing = Arc::new(Recording::new(1 << 16));
    let bridge = ViewBridge::<App, u32, MixedMap, Intent, WideJson, MixedRegion>::mount(
        SyncStore::new(AppReducer, NoFx),
        MixedMap { decls },
        Intent,
        Encoders {
            json: None,
            region: Some((MixedRegion, Arc::clone(&backing) as Arc<dyn RegionBacking>)),
        },
    )
    .expect("static declarations are well-formed");
    for decl in decls {
        bridge.bind(decl.path).expect("region-covered");
    }

    bridge
        .seal()
        .expect("seal publishes the initial full region");
    let seal_ops = backing.take_ops();
    assert_eq!(
        seal_ops.len(),
        4,
        "一次事务 = 1 次 odd + 2 次批量写（段字节 / 索引+头）+ 1 次 even：{seal_ops:?}"
    );
    assert_eq!(seal_ops[1], Op::Bytes, "odd 之后先写段字节：{seal_ops:?}");
    assert_eq!(seal_ops[2], Op::Bytes, "再写索引+头：{seal_ops:?}");
    match (seal_ops[0], seal_ops[3]) {
        (Op::Seq(odd), Op::Seq(even)) => {
            assert_eq!(odd % 2, 1, "odd 必须是奇序列：{odd}");
            assert_eq!(even, odd + 1, "even 紧跟 odd：{odd} → {even}");
            assert_eq!(even, INITIAL_SEQ, "封板水位：{even}");
        }
        other => panic!("首尾必须是 publish-sequence：{other:?}"),
    }

    assert!(bridge.send_intent(json!(1)).unwrap());
    let inc_ops = backing.take_ops();
    assert_eq!(
        inc_ops.len(),
        4,
        "增量事务同一形状（单批边界不变）：{inc_ops:?}"
    );
    assert_eq!(inc_ops[1], Op::Bytes);
    assert_eq!(inc_ops[2], Op::Bytes);
    match (inc_ops[0], inc_ops[3]) {
        (Op::Seq(odd), Op::Seq(even)) => {
            assert_eq!(odd % 2, 1);
            assert_eq!(even, odd + 1);
        }
        other => panic!("首尾必须是 publish-sequence：{other:?}"),
    }

    // 读侧：三段不同 tag 都能解回同源值（索引/头/段字节都写对了）。
    let read = RegionReader::read_lock_free(backing.as_ref(), decls, 8).expect("region readable");
    assert_eq!(read.sequence % 2, 0, "读到的序列必须是已提交 even");
    assert_eq!(read.values.len(), 3, "三个绑定都有值：{:?}", read.values);
    let values: Vec<Value> = read.values.iter().map(|(_, v)| v.clone()).collect();
    assert_eq!(
        values,
        vec![json!(3), json!("hello"), json!({"k": [5, 1, 2, 3]})]
    );

    // 目标区补零：填充字节必须为 0（预置 0xAA 未补零 ⇒ 残留）。
    let bytes = backing.bytes();
    let count = u32::from_le_bytes(
        bytes[OFFSET_BINDING_COUNT..OFFSET_BINDING_COUNT + 4]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    assert_eq!(count, 3);
    for i in 0..count {
        let at = HEADER_LEN + ENTRY_LEN * i;
        let offset = u64::from_le_bytes(
            bytes[at + ENTRY_OFFSET_OFFSET..at + ENTRY_OFFSET_OFFSET + 8]
                .try_into()
                .expect("8 bytes"),
        ) as usize;
        let len = u32::from_le_bytes(
            bytes[at + ENTRY_OFFSET_LEN..at + ENTRY_OFFSET_LEN + 4]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let padded = len.div_ceil(SEG_ALIGN) * SEG_ALIGN;
        assert!(padded > len, "段 {i} 长度 {len} 恰好对齐 ⇒ 该用例没走填充");
        let pad = &bytes[offset + len..offset + padded];
        assert!(
            pad.iter().all(|b| *b == 0),
            "段 {i} 的填充字节必须为零，实际 {pad:?}"
        );
    }
}

// ── 延迟分布采集（#174 / #169 的 p50/p99；诊断用，不设阈值） ─────────────────

/// 通知（仅 JSON）与发布（JSON + Region）两种部署的每拍延迟分布。
///
/// 只打印、不设阈值：读数是环境相关的（含每样本 2 次 `Instant::now()` 开销，
/// Windows 上约 20–30 ns）；判别力证据是上面的确定性分配锚。
/// 跑法：`cargo test --all-features --test view_bridge_test -- --ignored --nocapture latency_profile`
#[test]
#[ignore = "measurement harness: run with --ignored --nocapture"]
fn view_hot_path_latency_profile() {
    fn profile(label: &str, bridge: &dyn Fn() -> bool, rounds: usize) {
        for _ in 0..64 {
            assert!(bridge());
        }
        let mut samples: Vec<u128> = Vec::with_capacity(rounds);
        for _ in 0..rounds {
            let started = std::time::Instant::now();
            assert!(bridge());
            samples.push(started.elapsed().as_nanos());
        }
        samples.sort_unstable();
        let at = |p: f64| samples[((samples.len() as f64 * p) as usize).min(samples.len() - 1)];
        eprintln!(
            "{label}: p50={} ns  p99={} ns  max={} ns（{} 拍）",
            at(0.50),
            at(0.99),
            samples[samples.len() - 1],
            samples.len()
        );
    }

    let intent = json!(1);
    for bindings in [1usize, 16, 64, 256] {
        for filtered in [false, true] {
            let bridge = wide_bridge(wide_decls(bindings), filtered, None);
            let op = || bridge.send_intent(intent.clone()).unwrap();
            profile(
                &format!("#174 notify filtered={filtered} bindings={bindings}"),
                &op,
                2_000,
            );
        }
        let bridge = wide_bridge(wide_decls(bindings), false, Some(1 << 22));
        let op = || bridge.send_intent(intent.clone()).unwrap();
        profile(
            &format!("#169 publish+notify bindings={bindings}"),
            &op,
            2_000,
        );
    }
}

/// #169/#174：两个线程同时通知（scratch 必被抢 ⇒ 抢不到的那拍临时新建）后，
/// 区域仍是**最后一拍**的值（不得残留陈旧段），通道也不得转 unavailable。
#[test]
fn region_publish_survives_scratch_contention() {
    let decls = wide_decls(16);
    let backing = Arc::new(VecBacking::new(1 << 20));
    let bridge = Arc::new(
        ViewBridge::<App, u32, Wide, Intent, WideJson, WideRegion>::mount(
            SyncStore::new(AppReducer, NoFx),
            Wide {
                decls,
                filtered: false,
            },
            Intent,
            Encoders {
                json: Some(WideJson),
                region: Some((WideRegion, Arc::clone(&backing) as Arc<dyn RegionBacking>)),
            },
        )
        .expect("static declarations are well-formed"),
    );
    for decl in decls {
        bridge.bind(decl.path).expect("declared + covered");
    }
    bridge.seal().expect("seal before dispatch");

    let rounds = 200u32;
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let handle = Arc::clone(&bridge);
            std::thread::spawn(move || {
                for _ in 0..rounds {
                    assert!(handle.send_intent(json!(1)).unwrap());
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("并发通知不得 panic");
    }

    // 收尾一拍（单线程 ⇒ 不会被 PublishBusy 丢掉）：区域必须落到现态。
    assert!(bridge.send_intent(json!(1)).unwrap());
    assert!(
        bridge.region_failure().is_none(),
        "并发通知把通道打成 unavailable：{:?}",
        bridge.region_failure()
    );
    let count = 2 * rounds + 1;
    let read = RegionReader::read_lock_free(backing.as_ref(), decls, 8).expect("region readable");
    let mut got: Vec<u64> = read.values.iter().filter_map(|(_, v)| v.as_u64()).collect();
    let mut want: Vec<u64> = (0..16).map(|i| u64::from(count) * 1_000 + i).collect();
    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want, "区域里出现了陈旧/缺失的段：{:?}", read.values);
}

// ── scratch 归还路径：采用方 `Drop` 不得在桥内锁内跑（#174 第 4 条） ──────────

type ReenterBridge = ViewBridge<App, u32, ReenterMap, Intent, ReenterJson, ReenterRegion>;

#[derive(Clone)]
struct ReenterView {
    n: u32,
    link: Arc<std::sync::OnceLock<std::sync::Weak<ReenterBridge>>>,
    armed: Arc<AtomicBool>,
    drops: Arc<AtomicU32>,
}

// 值语义只看 `n`：桥的 latest-wins 比较不得被探针句柄干扰。
impl PartialEq for ReenterView {
    fn eq(&self, other: &Self) -> bool {
        self.n == other.n
    }
}

struct ReenterJson;

impl JsonEncoder for ReenterJson {
    type View = ReenterView;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn to_json(&self, v: &ReenterView) -> Value {
        json!(v.n)
    }
}

struct ReenterRegion;

impl RegionEncoder for ReenterRegion {
    type View = ReenterView;
    fn covers(&self) -> Coverage {
        Coverage::All
    }
    fn encode(&self, v: &ReenterView, w: &mut RegionWriter) {
        w.u64(u64::from(v.n));
    }
}

impl Drop for ReenterView {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if !self.armed.load(Ordering::SeqCst) {
            return;
        }
        // 退役/scratch 回收期间的析构若发生在桥内锁里，这里会自死锁（OS 时限兜底）。
        if let Some(bridge) = self.link.get().and_then(std::sync::Weak::upgrade) {
            let _ = bridge.take_patches();
            let _ = bridge.projection_panics();
        }
    }
}

struct ReenterMap {
    link: Arc<std::sync::OnceLock<std::sync::Weak<ReenterBridge>>>,
    armed: Arc<AtomicBool>,
    drops: Arc<AtomicU32>,
}

impl ViewMap<App> for ReenterMap {
    type View = ReenterView;
    fn bindings(&self) -> &[BindingDecl] {
        wide_decls(2)
    }
    fn project(&self, index: usize, s: &App) -> ReenterView {
        ReenterView {
            n: s.count.wrapping_mul(10).wrapping_add(index as u32),
            link: Arc::clone(&self.link),
            armed: Arc::clone(&self.armed),
            drops: Arc::clone(&self.drops),
        }
    }
}

/// 退役值（`retired`）与 scratch 归还（`recycle`）两处析构都必须在桥锁外：探针在
/// `Drop` 里重入公共面，持锁即自死锁（独立线程 + OS 时限兜底）。
#[test]
fn scratch_recycle_runs_user_drop_outside_bridge_locks() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let link: Arc<std::sync::OnceLock<std::sync::Weak<ReenterBridge>>> =
            Arc::new(std::sync::OnceLock::new());
        let armed = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicU32::new(0));
        let bridge = Arc::new(
            ViewBridge::mount(
                SyncStore::new(AppReducer, NoFx),
                ReenterMap {
                    link: Arc::clone(&link),
                    armed: Arc::clone(&armed),
                    drops: Arc::clone(&drops),
                },
                Intent,
                Encoders {
                    json: Some(ReenterJson),
                    region: None,
                },
            )
            .expect("static declarations are well-formed"),
        );
        assert!(link.set(Arc::downgrade(&bridge)).is_ok());
        for decl in wide_decls(2) {
            bridge.bind(decl.path).expect("covered");
        }
        bridge.seal().expect("seal before dispatch");
        armed.store(true, Ordering::SeqCst);
        for _ in 0..4 {
            assert!(bridge.send_intent(json!(1)).unwrap()); // 每拍换一批句柄 ⇒ 退役析构
        }
        let _ = bridge.take_patches();
        let _ = tx.send((drops.load(Ordering::SeqCst), bridge.projection_panics()));
    });
    let (drops, panics) = rx
        .recv_timeout(std::time::Duration::from_secs(20))
        .expect("探针疑似死锁：采用方 Drop 落在桥内锁里");
    assert!(drops >= 2, "退役值确实被析构过（{drops} 次）");
    assert_eq!(panics, 0, "重入式 Drop 不得被记成投影 panic");
}
