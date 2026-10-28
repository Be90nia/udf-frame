# Changelog

本文件记录 udf-frame 的对外可见变更（API/feature/文档/修复）。格式参照 Keep a Changelog；版本号语义遵循 SemVer。

## [Unreleased]

### Added（消费方反馈六件套 + 通知面异步化，2026-10-28；GitHub issue#1 / ADR 0008）
- **BoundedRegistry**（memgov）：`get<Q: Borrow<Q>>` 零分配泛型查找、`get_mut`（只回值面——键面类型级不可达，compile_fail doctest 钉死）、`Clone`（deep-copy 值容器；运行期只读共享用 `Arc<BoundedRegistry>` 外包）。
- **retry**：`retry_async`/`retry_if_async` + `AsyncSleeper` 注入式异步面（零运行时依赖，下游 tokio 自接线）；同步/异步同 policy 同语义（对照测试钉死）；取消安全（sleep 内联 await，future drop 即全停）。
- **cache**：`SyncLruCache`/`SyncTtlCache` 并发薄包装（`&self` + 内部 Mutex 短临界区，语义与单线程版一致；poison=fail-fast panic 带因；分片留待命中率证据）。
- **id**：`RandomId`（UUIDv4 凭据级，OS CSPRNG 直通 fail-closed 禁弱熵回退）——`random = ["dep:getrandom"]` 第三道依赖隔离门，默认树保持零三方；文档双用途口径：v7=排序友好事件 id / v4=凭据。
- **logx**：每通道独立 level（写路径 AtomicU8 原子过滤，`set_level` 运行期即时生效——结构性免疫 tracing callsite interest 缓存坑，tracing#2874/#2400）+ 逐通道丢弃计数读数 `dropped(channel)`。
- **FxScope**：`spawn` 更名 `spawn_tracked` + 新增 `spawn_detached`（显式脱管口，不计 in_flight、不被 shutdown 等待；全仓唯一 tokio::spawn allow 豁免）+ 文档范式（Arc 化标准写法/cancel-safe 清单/LocalSet 边界/两套 drop 语义对照）。
- **通知面异步化**（ADR 0008 案 C 两档并存）：`skeleton::notify`（`NotifySink` 宿主投递口 + `Notify<S>` 登记面，不扩 Store 六方法）+ SyncStore 实现（排空环内 O(1) 版本投递、先于同步回调；state_version 零异步订阅者不自增）+ `notify_published()` 读侧。锚：无订阅 dispatch 77→78.4ns（±10ns 容忍内）；50ms 慢消费者隔离实证 ~172ns。既有通知/审计/回灌测试零改动。
- **fx 域埋点**（`skeleton-infra`）：托管任务 panic 归一为 `Panicked` 后，若句柄已被丢（结果无人消费）留一行 warn 痕——target `skeleton::fx`、msg=panic 文案，消除「panic 结果静默丢失」；`Ok`/`Cancelled` 无人接属设计内不埋；`log-embed` 默认关=原静默语义零增量。埋点面口径：infra 七点（shutdown 6 + fx 1）+ std-ext 七域 35 点 = 九域 42 点。
- 六域 armor 逐坑一测（调研：外部踩坑调研（本地留存，未入库）——外部踩坑面/性能模式/生态先例，T1/T2 出处全覆盖）。

### Changed（retry 默认 jitter + FxScope spawn 双命名，破坏性）
- retry：退避默认 **full jitter**（禁关——无 jitter 同拍风暴，AWS 一手实测；确定性回放用 `with_jitter` 恒等源）；`with_budget` 红线升级「只配次数不配预算=风暴面」；retry 默认假定操作幂等（文档红线）。
- fx：`FxScope::spawn` → `spawn_tracked`（语义显名）；调用点全仓机械迁移（42 处）。
- （破坏性直迁：0.1.0 未发布无外部采用方，按 ADR 0005 先例无 shim。）

### Changed（fail-visible 封口：静默行为显形四件 + 契约词表封口，破坏性）
- `skeleton`：`EffectId` 构造封口——`EffectId(pub u64)` → 私字段 + `pub const fn new(u64)` / `get()`（std `NonZeroU64` 同形），裸元组构造由 compile_fail doctest 冻结；id 唯一性纪律（store 内全局唯一、多效果域不重叠区间）入文档。**破坏性**：`EffectId(id)` → `EffectId::new(id)`。
- `skeleton`：`Effect` / `Restore` 加 `#[non_exhaustive]`（与 `parse` 词表同口径，ADR 0007 协同）——跨 crate match 编译期强制带通配臂面对未知变体；`skeleton-infra` 内 7 处 match 站点按 fail-closed 语义补臂（panic 带因 / `unreachable!` 带因）。
- `skeleton-infra`：静默行为显形四件（对齐 `subscriber_panics` 计数先例）——`Combine::route_misses()`（combine 路由 miss 不再零计数）；`SyncStore::local_missed_notifications()`（跨线程 dispatch 下本地订阅零触达可观测）；`SyncStore::try_subscribe`（闭门后显式 `Err(StoreClosed)`，旧 `subscribe` 照常登记并计入闭门计数）；`SyncStore::audit_preempted()`（审计事件流二次订阅的静默覆盖留痕）。另 `SyncStore::cancel_unknown_id()`：Cancel 指向从未发出的 Run id 计数留痕（不拦截——迟到 Cancel 属合法时序，核心无生命周期面无法区分漂移）。
- 依据：架构审计（本地留存，未入库）F-04/07/08/09/10/13（B/C 桶残余清零）。

### Changed（combine 分片作根编译期拒绝：Fragment/Reducer 契约拆分，破坏性）
- `skeleton` 新增 `Fragment` trait（reduce-only 状态分片契约）+ blanket `impl<T: Reducer> Fragment for T`（每个根都是合法子件）；`Reducer` 语义收窄为**根契约**（initial+reduce，trait 本体与既有 `impl Reducer` 零变化）。
- `combine`/`combine!` 产物 `Combine<…>` 由 `impl Reducer` 改为 `impl Fragment`：分片作根（`SyncStore::new(分片, …)`）从「装配期 panic」升级为 **E0277 编译期拒绝**（`#[diagnostic::on_unimplemented]` 错误信息自带「分片≠根」修复指引）；`combine` 的子件 bound 由 `Reducer` 放宽为 `Fragment`（完整 Feature 经 blanket 仍可作子件）。随拆分删除误用 panic 与三处孤儿 `#[track_caller]`；两处分片误用 panic 测试由 compile_fail doctest 接替。决策依据 ADR 0009。（**破坏性直迁**：0.1.0 未发布无外部采用方，按 ADR 0005 先例无 shim。）
- **下游迁移说明（combine 分片：作根从 panic 变编译错）**：只写 Feature（`impl Reducer` 含 initial+reduce，`SyncStore::new(feature, fx)` 挂载）**零改动**；持有分片并注解返回类型（`-> impl Reducer<…>` 接 `combine`/`combine!` 产物）改为 `-> impl Fragment<…>`、use 行加 `skeleton::Fragment`（委托调用 `.reduce(...)` 写法不变）；曾把分片传给 `SyncStore::new`/`TestStore::new` 的：此前装配期 panic，现在编译期 E0277 即修复指引——根状态由父 Feature 的 `initial` 显式构造（含子切片初值），父 Feature 本体才是根（`skeleton-infra/tests/combine_teststore.rs` 固定装置为标准形）；少见形态：同文件 import `Fragment` 且直接裸调完整 Feature 的 `.reduce()` 会报 E0034 二义——用 `Reducer::reduce(&feature, …)` 显式分发（框架树内无此形态）。

### Added（ViewBridge MVP：语言无关视图数据面，ADR 0006 波 3）
- 契约词汇 `skeleton::view`（零依赖）：`BindingDecl`/`ViewMap<S>`（纯投影 + 缺省哨兵）/`IntentMap<A>` + `PayloadTag` + `PROTOCOL_VERSION`；`ViewMap` 方法集冻结与 bound 永不落领域 `S`（M6 冻结条款入 doc）。
- 机器 `skeleton_infra::view`（feature `view = ["dep:serde_json", "udf-std-ext/json"]`，ADR 0007 §4.2-5 跨 crate 开门；默认树零三方不变）：`ViewBridge` 纯订阅者挂 SyncStore 通知面（核心零改动）——类型化投影 + 哨兵粗闸/类型 eq 细闸 + latest-wins 折叠 + 版本水位（`PatchBatch.from/to_version`、`Snapshot.version`，桥内单调消费代数与区域头同源）+ 毒化-重同步闭环（投影 panic catch + 计数 + resync 位 ⇒ `snapshot_doc` 收口）+ 可组合双编码矩阵（`JsonEncoder` lazy / `RegionEncoder` eager + `Coverage` + `RegionWriter`/`RegionBacking`/`VecBacking` 固定容量区域通道 v1）+ `Derived`/`Sentinel` L1 记忆化胞。
- 读侧参考实现 `view::read`（M2 实现一次）：`RegionReader`（TLV 解段 + 未知 tag 跳过 + `unknown_tags_skipped` 计数 + `needs_resync` 提示）+ `decode_batch`（JSON 通道同款语义）；规范 `docs/view-region-spec.md` v1 + TS 镜像读器 `docs/view-region-reader.ts`。
- 命名工件链（M3）：真源 `docs/view-bridge-constants/manifest.toml` + 生成器 `examples/view_constants_gen.rs`（零三方纯 std，`--check` CI 断言）+ TS/Python/C 生成物入库；demo `include_str!` 清单逐字对表。
- 正典验收环 `examples/view_bridge_demo.rs`（矩阵行为/双编码一致性/换区握手/水位自洽/未知标签跳过，断言内嵌）+ 行为面 `tests/view_bridge_test.rs`（毒化闭环/封板/跨线程/闭门）。
- 采用方纪律与边界收口（外部踩坑扫描 G4-G8）：视图词表 = enum 表达互斥状态、禁布尔旗子（`ViewMap` doc 立纪律 + `bindings()` 语义给正反例）；「input 覆盖实读」红线补最小可执行反例 `tests/view_bridge_test.rs::narrow_input_leaks_stale_and_is_detectable`（漏报三签名：`projection_runs` 停滞 / 水位推进而补丁为空 / 快照真值与最后交付补丁不等）；ADR 0006 后果段补 M6 冻结记账（方法集冻结 + bound 不落 `S`）+ 后续工作段登记桥层内省面（按绑定维度的投影抓手，本次只登记不实现）；`docs/reference.md` view 章节写明边界「桥保证同一进程内自洽与重同步，跨进程/跨重启复现归 store/持久化层」。

### Changed（json 域门化：默认树恢复零三方，破坏性）
- `std-ext` 的 `json` 域门化为 `json` feature（ADR 0007）：`serde_json` 转 optional（`json = ["dep:serde_json"]`），`json` 域默认不进树——`std-ext` 与 `skeleton-infra` 的默认树**双双恢复零三方**（`cargo tree -p udf-std-ext -e normal` 仅 `skeleton`；`cargo tree -p udf-skeleton-infra -e normal` 仅 skeleton + std-ext，一行可复核）；开 `json` 门时 `serde_json` 及其五子节点（itoa/memchr/serde_core/zmij…）按需进树。域数口径由「十四域零门常驻」改为「**十三域零门常驻 + `json` 域门控**」。（**破坏性**：`std_ext::json` 默认不可用，使用者加 `features = ["json"]`；`pub use serde_json` 再导出经 PM 评审否决，不提供；0.1.0 未发布故无外部采用方迁移面，按 ADR 0005 直迁先例无 shim。）四文档同步终态口径：`README.md` / `docs/reference.md` / `std-ext/README.md` / `skeleton-infra/README.md` 的「除 `json` 域外零三方」过渡态限定（本区块下方 Fixed 首条落下的）全部翻转为终态表述。顺带清零三条 clippy warning：`config.rs` 的 `use std::fmt` 门由 `config-json` 修正到真实消费者门 `config-toml`（单开 config-json 时 unused import）；`logx.rs` 两处 doc list 误判（段落续行行首 `+` 被 markdown 当列表标记）调整断行消除。

### Fixed（对外口径诚实化 + 域数对齐 + CI 门整改）
- 「零三方」口径修正：`std-ext` 的 `json` 域引 `serde_json` 普通依赖（零 feature 常开，默认树即含 `serde_json` 子树）——此前 `README.md` / `docs/reference.md` / `std-ext/README.md` / `skeleton-infra/README.md` 仍写「零三方」，与实测依赖树矛盾；现四处统一为限定表述「除 `json` 域外零三方」，`skeleton-infra` 默认树随之同口径（`json` 域依赖形态的决策见 ADR 0007）。
- 域数口径对齐：`std-ext` 为**十四域**（除 `json` 域外各域零门常驻、本体纯 std + `json` 域带 `serde_json`）——`README.md`（crate 表 + 常驻域行 + 「只要散件」段）、`docs/reference.md`（头部 + feature 矩阵 + 域索引 + 速查表）、`skeleton-infra/README.md` 此前写「十三域」且漏 `json`；`docs/reference.md` 补 `## json` 域章节（`JsonMode`/`JsonParseError`/64MiB `MAX_INPUT_BYTES`/`decode`/`decode_str`/`decode_slice`，API 逐字对 `std-ext/src/json.rs`）与域索引 json 行。
- 性能锚口径诚实化：dispatch 锚（无订阅 ~77ns / 订阅态 ~142ns）此前无适用边界，现补「锚仅在小 State 成立」——外部持有快照时状态落位走 `Arc::make_mut` 克隆分支，每次 dispatch 额外付一次 `S::clone`（锁内），实测 1MiB State ≈210µs/dispatch；`README.md` 埋点规范段 + `docs/reference.md` runtime 锚段同口径（审计报告 F-05）。
- CI 门整改：`.github/workflows/ci.yml` 的 std-ext 步去掉全局 `--skip anchors --skip verbatim`——该字面量匹配连带屏蔽 13 个真实测试（含 `hash` 的 XXH64 官方向量锚、`paths` 的 Windows verbatim 路径语义测试、`scheduler` 的 every_anchors_* 等），属 hollow gate；并行污染根因已在 fe5728a 根治（计数窗加 IN_WINDOW 线程归属门），主步改全量跑，`paths_alloc serial` 单列步保留作双保险。
- 孤儿源文件处置：删 `skeleton-infra/src/text.rs`、`skeleton-infra/src/backoff.rs`（ADR 0004 迁移后残留的无 `mod` 声明副本，不参与编译、不产文档，且 backoff 双份实现已漂移——infra 副本无 `log-embed` 埋点）；内容已在 `std-ext`，无 shim 需求。
- **notify 每拍堆分配修复**（性能修复）：`notify_async` 对有订阅者的每次 dispatch 原走 `collect::<Vec<_>>()` 新建收集缓冲（PerfProbe 实测 total = 1.00 alloc/拍）；改为排空权独占的复用缓冲（`StoreInner::notify_sink_buf`，每 store 独立、跨拍 `clear` 保容量）。语义零变化：每订阅者每拍恰一次投递、投递仍在表锁外、`state_version` 门控与冻结（无人订阅不自增/增减订不回退）原样；钉住锚 `tests/memgov.rs::perf_anchor_alloc::notify_single_subscriber_zero_alloc_dual_caliber`（回退成每拍新建即红）。
- 分配锚注释口径修正：`skeleton-infra/tests/memgov.rs` 的 Borrow 查找锚称含分配路径「10×+ 慢」，PerfProbe 实测仅 **1.5–1.9×**（差 ~50ns ≈ 一次 alloc + copy）——按实测值改写并注明口径与 `verify/perf-probe/`（`registry` 组，10k/44c/100 键三口径）。

### Tests（分配锚与 bench 目标补齐，2026-10-28）
- 新增双口径（live 净增 + total 累计）分配锚：`skeleton-infra` 的 dispatch 命中 / route-miss 稳态 / notify 单订阅者（`tests/memgov.rs::perf_anchor_alloc`），`std-ext` 的 `SyncLruCache`/`SyncTtlCache` get+insert、`retry_async` 首试、`RandomId` 单枚与 fill 批量（`tests/alloc_anchors.rs`，事件计数 + 仪器自检）；补 `std-ext` 的 bench 目标 `std_ext`（cache/retry/id）与 `infra` bench 两组（`dispatch_per_beat` 命中 vs Cancel、`notify_dispatch` 0/1 订阅者）。

## [0.1.0] — 2026-10-18

> 0.1.0 首发准备态：三 crate（`skeleton` / `std-ext` / `skeleton-infra`）同版 0.1.0 首发；
> crate 名冲突处置见发布检查单（本地留存，未入库）。以下为 0.1.0 累计对外可见变更。

### Changed（批 6A Journal 状态面关联类型化，破坏性）
- `Journal<A, S>` → `Journal<A>` + `type State`（ADR 0005）：`JsonlJournal` 同步泛型化（`JsonlJournal<A, S>`，`PhantomData<fn() -> (A, S)>` 标记，零布局成本）——构造期一次定型（`JsonlJournal::<A, S>::create(..)`）后方法调用零 turbofish 零 UFCS 零绑定标注。旧世界 `S` 不被方法参数约束，每调用点 E0283 强迫 `Journal::<A, S>::` 全限定 + `MutexGuard` 场景再加 `&mut *guard` 显式 deref（终评三槽点之三），税清零。`skeleton::JournalPort<A, S>` → `JournalPort<A>` + `type State` 同款（`restore` 返回 `Restore<A, Self::State>`）。dyn 焊接面保留：`DynJournal<A, S>` = `dyn Journal<A, State = S>`。（**破坏性**：`JsonlJournal` 具体类型携带 (A, S) 参数，构造点需类型标注；外部实现方在 impl 块补 `type State = S;` 一行。方案对比与三探针证据：ADR 0005。）
- 文档伺候面全撤（E0283 专伺候教行清零）：教程驻足节 UFCS 全限定改为「构造期定型、调用期直呼」；cheatsheet §1 惯用形行与坑 4 重写（新坑=定型账本塞异型 payload 的 E0308）；README 第一天提示同步。教程坑 A 教学指令修正：删 `#[serde(tag)]` 不复现（struct variant 外部 tag 恰好是 object）→ 改为「给 Clear（unit variant）入账」真实命中路径，重跑报错逐字命中。README「只要散件」最小 Cargo.toml 组合示例补齐。埋点宏 `embed_event!` 单源化评估：`#[macro_export]` 单源可行但劣化面更大（公开 API 面+feature 耦合+cfg 复杂度 > 14 行冻结宏同步成本），维持双副本+两侧同步责任注释（ADR 0005 §埋点宏）。

### Added（批5-2 std-ext 七域迁移收口）
- std-ext 扩至十三常驻域全量：`paths`/`clock`（含 `clock::rfc3339` 行为件）/`retry`/`scheduler`/`ratelimit`/`cache`/`id` 自 `skeleton-infra` 物理迁入（各域集成测试随迁；自持采集器 `cache_multidim` 迁 std-ext/examples；memgov 依赖的五采集器留守 infra examples 改测 `std_ext::` 面；`humantime` dev-dep 随 RFC3339 对照测试迁 std-ext，infra 侧留守 bench 仍用）（**破坏性**：`skeleton_infra::{paths,clock,retry,scheduler,ratelimit,cache,id}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim）。`log-embed` 对账新基线 41=std-ext 七域 35 + infra shutdown 6。`shutdown` 留守 infra（胶水件反向依赖 `std_ext::scheduler`）。ADR 0004 补终版切面表。

### Added（批5-1 std-ext 拆分）
- 新子 crate `std-ext`：六个纯 std 叶子域自 `skeleton-infra` 物理迁入——`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`（含各自内联测试/集成测试/示例/doctest 随域迁移；`log-embed` 埋点三域 9 调用点由 std-ext 自有同名 feature 承载，宏为同款文本作用域副本，target `skeleton::<domain>` 观测契约不变）。分层 `skeleton` ← `std-ext` ← `skeleton-infra`（反向依赖）；`cargo tree -p udf-std-ext` 默认树零三方（**破坏性**：`skeleton_infra::{text,encode,hash,iter,backoff,fsx}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim；见 ADR 0004）。

### Added（批1A 契约下沉 + log 拆分）
- 契约层真下沉：`Clock`/`SystemClock`/`MockClock` → `skeleton::clock`；`EnvSource`/`StdEnv`/`MockEnv`/`ConfigPort`/`ConfigError` → `skeleton::config`；`LogPathResolver`（trait）→ `skeleton::logpath`。实现件留 `skeleton-infra`，无 re-export shim（**破坏性**：import 路径迁移，见 ADR 0001）。
- 新 feature `log-fs`：`logx` 文件落盘面（密钥掩码/路径解析/FileSink，纯 std 零三方依赖）；`log` 变为 `log-fs` + tracing 四件套门面绑定层（见 ADR 0002）。
- ADR 三篇落盘：ADR 0001、ADR 0002、ADR 0003（paths 域 dunce/XDG 拍板补档）。

### Changed（批2A API 形状四件，破坏性）
- `EnvSource::get` → `Result<String, EnvError>`；新增 `EnvError { Missing, NotUnicode(OsString) }`——键存在但值非 Unicode 不再谎报 Missing（`ConfigError` 相应新增 `NotUnicode` 变体）。
- id 类型化：新增 `Id(u128)` 值类型（`Display`/`FromStr`，RFC 9562 连字形）；删除 `to_string` 自由函数。
- `relative_to(path, base)`：参数序对齐 pathdiff 生态惯例（原 `(base, path)`）。
- JournalPort 双 trait 同名消歧：实现层方法改名 `append_action`/`restore_stream`/`append_snapshot`（端口词表不变），双 trait 同屏直呼零 turbofish。

### Fixed
- journal flush 补 `sync_all`（掉电真持久；`with_snapshot_every` 高频档有 µs→ms 成本警示）。
- 根 CI 修复：udf-frame 迁独立 workflow（`.github/workflows/udf-frame.yml`），根 ci.yml 收缩 harness-only；删除 inert 子目录 workflow。
- 性能口径诚实化：id_multidim ③位收窄声明 + ⑥竞争面 Barrier 栅栏对齐（spawn 移出计时窗；旧锚「无悬崖」结论作废）；reference.md logx 锚补 sync_all 换算警示。
- 文档精度：README 常驻域清单/埋点口径/default 漂移/debug 臂采用方警示/第一天提示（盲测 6 坑就近化）。

### Tests
- 全量 682 passed / 0 failed（`--test-threads=1 --no-fail-fast` 正典口径）；skeleton 契约层 `--no-default-features` 自持绿；clippy `--workspace --all-features --all-targets` 零警告；`cargo fmt --all` 清账。

### 独立盲测
- 采用方视角全流程盲测 4.35/5（有限采用）：UDF 核与池化三件值得采用；性能维 5.0（dispatch 80.3ns，契约 ≤100ns 达标）；稳态零分配亲手复现 PASS。报告：采用方盲测（本地留存，未入库）。
