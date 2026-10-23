# Changelog

本文件记录 udf-frame 的对外可见变更（API/feature/文档/修复）。格式参照 Keep a Changelog；版本号语义遵循 SemVer。

## [Unreleased]

## [0.1.0] — 2026-10-18

> 0.1.0 首发准备态：三 crate（`skeleton` / `std-ext` / `skeleton-infra`）同版 0.1.0 首发；
> 发布检查单与 crate 名冲突处置见 `docs/PUBLISHING.md`。以下为 0.1.0 累计对外可见变更。

### Changed（批 6A Journal 状态面关联类型化，破坏性）
- `Journal<A, S>` → `Journal<A>` + `type State`（ADR 0005）：`JsonlJournal` 同步泛型化（`JsonlJournal<A, S>`，`PhantomData<fn() -> (A, S)>` 标记，零布局成本）——构造期一次定型（`JsonlJournal::<A, S>::create(..)`）后方法调用零 turbofish 零 UFCS 零绑定标注。旧世界 `S` 不被方法参数约束，每调用点 E0283 强迫 `Journal::<A, S>::` 全限定 + `MutexGuard` 场景再加 `&mut *guard` 显式 deref（终评三槽点之三），税清零。`skeleton::JournalPort<A, S>` → `JournalPort<A>` + `type State` 同款（`restore` 返回 `Restore<A, Self::State>`）。dyn 焊接面保留：`DynJournal<A, S>` = `dyn Journal<A, State = S>`。（**破坏性**：`JsonlJournal` 具体类型携带 (A, S) 参数，构造点需类型标注；外部实现方在 impl 块补 `type State = S;` 一行。方案对比与三探针证据：`docs/adr/0005-journal-reshape.md`。）
- 文档伺候面全撤（E0283 专伺候教行清零）：教程驻足节 UFCS 全限定改为「构造期定型、调用期直呼」；cheatsheet §1 惯用形行与坑 4 重写（新坑=定型账本塞异型 payload 的 E0308）；README 第一天提示同步。教程坑 A 教学指令修正：删 `#[serde(tag)]` 不复现（struct variant 外部 tag 恰好是 object）→ 改为「给 Clear（unit variant）入账」真实命中路径，重跑报错逐字命中。README「只要散件」最小 Cargo.toml 组合示例补齐。埋点宏 `embed_event!` 单源化评估：`#[macro_export]` 单源可行但劣化面更大（公开 API 面+feature 耦合+cfg 复杂度 > 14 行冻结宏同步成本），维持双副本+两侧同步责任注释（ADR 0005 §埋点宏）。

### Added（批5-2 std-ext 七域迁移收口）
- std-ext 扩至十三常驻域全量：`paths`/`clock`（含 `clock::rfc3339` 行为件）/`retry`/`scheduler`/`ratelimit`/`cache`/`id` 自 `skeleton-infra` 物理迁入（各域集成测试随迁；自持采集器 `cache_multidim` 迁 std-ext/examples；memgov 依赖的五采集器留守 infra examples 改测 `std_ext::` 面；`humantime` dev-dep 随 RFC3339 对照测试迁 std-ext，infra 侧留守 bench 仍用）（**破坏性**：`skeleton_infra::{paths,clock,retry,scheduler,ratelimit,cache,id}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim）。`log-embed` 对账新基线 41=std-ext 七域 35 + infra shutdown 6。`shutdown` 留守 infra（胶水件反向依赖 `std_ext::scheduler`）。ADR 0004 补终版切面表。

### Added（批5-1 std-ext 拆分）
- 新子 crate `std-ext`：六个纯 std 叶子域自 `skeleton-infra` 物理迁入——`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`（含各自内联测试/集成测试/示例/doctest 随域迁移；`log-embed` 埋点三域 9 调用点由 std-ext 自有同名 feature 承载，宏为同款文本作用域副本，target `skeleton::<domain>` 观测契约不变）。分层 `skeleton` ← `std-ext` ← `skeleton-infra`（反向依赖）；`cargo tree -p udf-std-ext` 默认树零三方（**破坏性**：`skeleton_infra::{text,encode,hash,iter,backoff,fsx}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim；见 `docs/adr/0004-std-ext-split.md`）。

### Added（批1A 契约下沉 + log 拆分）
- 契约层真下沉：`Clock`/`SystemClock`/`MockClock` → `skeleton::clock`；`EnvSource`/`StdEnv`/`MockEnv`/`ConfigPort`/`ConfigError` → `skeleton::config`；`LogPathResolver`（trait）→ `skeleton::logpath`。实现件留 `skeleton-infra`，无 re-export shim（**破坏性**：import 路径迁移，见 `docs/adr/0001-contract-sink.md`）。
- 新 feature `log-fs`：`logx` 文件落盘面（密钥掩码/路径解析/FileSink，纯 std 零三方依赖）；`log` 变为 `log-fs` + tracing 四件套门面绑定层（见 `docs/adr/0002-log-feature-split.md`）。
- ADR 三篇落盘：`docs/adr/0001-contract-sink.md`、`0002-log-feature-split.md`、`0003-paths-dunce-xdg.md`（paths 域 dunce/XDG 拍板补档）。

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
- 采用方视角全流程盲测 4.35/5（有限采用）：UDF 核与池化三件值得采用；性能维 5.0（dispatch 80.3ns，契约 ≤100ns 达标）；稳态零分配亲手复现 PASS。报告：`reports/2026-10-17-adopters-blind-eval.md`。
