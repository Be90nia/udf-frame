# 蓝图断言台账：59 条一致性核对 + 9 条不可机检项复核（2026-11-23 审计，2026-11-25 入库）

> **这是什么**：2026-11-23 层边界审计里「蓝图 59 条可机判断言逐条核对」的**受控副本**，加上 2026-11-25 对其中 9 条 `Not-verifiable` 的逐条复核（为何不能机械化 / 人工证据 / 复核频率 / owner / 可重复步骤）。
>
> **来源与可追溯性**
> - 原始报告：《层 × 职责 × 承载者 × 依据：边界点清单与蓝图一致性核对（2026-11-23）》，原存于作者工作区 `reports/2026-11-23-layer-boundaries.md`（`/reports/` 被 `.gitignore` 忽略，clean checkout 不可见）。本文件是其 §2 一致性核对表**逐字**副本，由原报告生成者提供原文（GitHub issue #86 评论记录恢复过程），**未重建、未回填、未按蓝图 §13 反推**。
> - 原始快照 commit：`d2824db`（表格内 `file:line` 全部为该快照的行号；仓库自述「行号随重构漂移，符号名恒准」）。修订 commit（C1–C9 文档矛盾修正）：`52057f87`。本轮复核基线：`23b0a42`。
> - **本文件只加了三样东西**：① 59 行逐字搬入（ID / 断言原文 / 实现落点 / 原判定，一字未改）；② §2 的 9 条逐项复核；③ §3/§4 的本轮新增检查与分类变更记录。原始判定列不做任何静默改写。
> - **逐字搬入的唯一例外（逐字比对可验）**：行 #16 的证据列原文写「代码锚见 §1.1」，而原报告的 §1.1（资源上限表 9 行）未随 59 行一并搬入本文件，故改为「代码锚见**原报告** §1.1」以防悬空引用。除该处外，59 行与原文逐字节相同。
>
> **编号纪律（防混淆）**：`N-03` 这类编号 = **本台账行号**（`N-03 ≡ 行 #3`），**不是 GitHub issue 号**。同号 GitHub issue 只是相关历史工作项；「同号 GH issue 已关闭」**不**等于「该断言成立」。
> 特别提醒：issue #86 评论中以 N-32 / N-35 / N-36 / N-44 / N-59 开头的那张自动化建议表，逐项描述的是**同号 GitHub issue 的主题**（GH #32 依赖解耦、GH #35 weight-hook panic、GH #36 状态漂移、GH #44 reducer panic、GH #59 audit callback 重入），与本台账同名行的断言原文**不是同一命题**。本文件一律以台账原文为准——这也是 issue 自己立的规则（「不能把同号 Issue 已关闭当成证据」）。

## 0. 判定规则与统计

| 判定 | 定义 |
|---|---|
| **Conform** | 实现或 CI 与蓝图断言一致，且仓库内有可定位落点 |
| **Diverged** | 实现 / 计数 / 坐标与蓝图矛盾 |
| **Not-verifiable** | 仓库内无可机检落点：过程性、仓外、证据只在 gitignored 目录、或需重跑才能证 |

**统计有两个口径，都必须读**：

| 口径 | 合计 | Conform | Diverged | Not-verifiable |
|---|---|---|---|---|
| **历史快照**（`d2824db`，§1 表格逐字保留） | 59 | 48 | 2 | 9 |
| **现行复核**（`23b0a42`） | 59 | **50** | **0** | **9** |

现行口径与历史口径的差异**只有 2 行**（#12 / #38，均已在 `52057f87` 修复，证据见 §4）。9 条 `Not-verifiable` 的**判定本轮一律不改**（派单硬约束：不把 `Not-verifiable` 写成 `Conform`），只在其上新增一条正交的「自动化状态」轴（§2）：

- `AUTO` — 已落成确定性检查并接入 CI；
- `MANUAL` — 保留人工验收项，带可重复步骤；
- `OUT-OF-REPO` — 断言的证据主体在本仓之外（Xray 消费仓）。

现行口径下 9 条的自动化状态：**AUTO 1 条（N-03）/ MANUAL 5 条（N-10、N-17、N-19、N-35、N-44）/ OUT-OF-REPO 3 条（N-32、N-36、N-59）**。

## 1. 59 行断言台账（逐字，含原判定）

判定口径同上。行内 `§x` 指 `docs/architecture/target-architecture.md` 的章节；`file:line` 为 `d2824db` 快照行号。

| # | 蓝图断言（位置） | 实现 / CI 落点 | 判定 |
|---|---|---|---|
| 1 | §13 行 1：sync-only 三条依赖图（infra 默认 / `--no-default-features --features runtime` / std-ext）无 tokio | `ci.yml` step `Sync-only dependency tree (no Tokio)`（三条 `cargo tree -e normal`）；+ feature-isolation 的 `Runtime face is Tokio-free` 步；+ consumer-sim 的 sync 面断言 | Conform |
| 2 | §13 行 1：fail-closed（tree 失败与检出 tokio 都 exit 1） | `ci.yml` 同一步：`if ! tree=...` ⇒ `fail=1`；检出 tokio ⇒ `fail=1`；`exit "$fail"`（无 `continue-on-error`、无失败吞掉的兜底） | Conform |
| 3 | §13 行 1：反向控制（`--features tokio`、`--all-features` 实测失败） | CI 只有 consumer-sim 的正向反向控制（async 面必须含 tokio，`ci.yml` Stage B 步）；infra feature 面的「断言在 tokio 面必红」无 CI 步 | Not-verifiable |
| 4 | §13 行 2：`skeleton::Store` 契约可独立实现 | `skeleton/src/lib.rs:45-78` toy store doctest（`Test skeleton` CI 步跑 doctest）；`Store` `:717`；`SyncStore` 只是实现 | Conform |
| 5 | §13 行 3：Tokio 集成 feature 门控 | `skeleton-infra/Cargo.toml:46` `tokio=["dep:tokio"]`；`src/lib.rs:83-84` `#[cfg(feature="tokio")] pub mod fx`；CI feature-isolation `tokio` 面 | Conform |
| 6 | §13 行 4：quickstart / driver tour / end-to-end / tutorial 存在且可编译 | `skeleton-infra/examples/{quickstart,driver_tour,end_to_end}.rs`；`docs/tutorials/{thirty-min,cheatsheet}.md`；CI `Clippy --workspace --all-features --all-targets` 编译 examples | Conform |
| 7 | §13 行 5：close 与 cancel 互异 + 重复 close/cancel/shutdown 幂等 | `tests/fx_lifecycle.rs:266 close_cancel_and_shutdown_are_idempotent`；`tests/conformance_lifecycle.rs:272` 同名；`close_keeps_in_flight_work_running_and_shutdown_drains_clean` | Conform |
| 8 | §13 行 6：`ShutdownReport` 携六桶字段（completed/cancelled/panicked/failed(+errors)/timed_out/still_running） | `fx.rs:355-393` 字段逐一存在（另加 `drain_interrupted` 来源标记） | Conform |
| 9 | §13 行 6：`timed_out` 在截止点采样、`still_running` 在收割后 | `fx.rs:355-393` 文档 + `shutdown_timeout` `/1070` 实现；`docs/reference.md:2193` 同口径 | Conform |
| 10 | §13 行 6：变异下两测试 ~0.06s 内变红而非挂死 | 仓库内无变异记录/CI 步；本单未重跑 | Not-verifiable |
| 11 | §13 行 7：spawn 失败不污染 worker 账本、不挂 join；typed 错误 + `reservations` 分账 | `pool.rs:554 ThreadSpawnError`、`:586 RejectCause::SpawnFailed`、`:813-822` 预留、`:1105 resize`、`:1221 shutdown`、`:1285-1334` 回滚；`skeleton/src/pool.rs:90 SpawnFailed{kind,target_core,workers}`；`tests/thread_pool.rs` 故障注入族 | Conform |
| 12 | §13 行 7：`tests/thread_pool.rs`「30 total」 | 实测 `#[test]` **32**（本单计数，见 evidence）；spawn/resize 失败相关 8 个 | **Diverged** |
| 13 | §13 行 8：`unsafe_evidence.rs` 6 测试（并发交错/panic/重入/生产 vs Mutex 差分 8 面） | `tests/unsafe_evidence.rs` 实测 6 个 `#[test]`；`runtime/evidence.rs:12 MutexExclusive`；`runtime.rs:1679 new_mutex_reference` | Conform |
| 14 | §13 行 8：Miri 纯 std 单线程子集、记录结果 | `CHANGELOG.md:425-427` 记录 3 测试与 `cargo +nightly miri test … -- panic` exit 0；明示不声称并发 | Conform |
| 15 | §13 行 8：`unsafe-evidence` 默认关，默认 rlib 符号级无 `MutexExclusive` | `Cargo.toml:44 unsafe-evidence=[]` 不在 `full`；`CHANGELOG.md:423` 记录 `strings` 符号级证明（手工，非 CI） | Conform |
| 16 | §13 行 9：资源上限逐池记录覆盖群体/满容量/超配/读数（7 面含 ObjectPool 三轴） | `docs/reference.md:453-520`（PoolBase/ThreadPool/PooledThreadPool）、`:590-700`（ObjectPool 三轴/SizeClassPool）、`:745-790`（memgov 三件）；代码锚见原报告 §1.1 | Conform |
| 17 | §13 行 9：外部探针 crate 断言全部文档声明（`ALL DOC CLAIMS HOLD`） | `verify/2026-11-17-p2-resource-axes/probe/src/main.rs:22` + `receipt.md`（**`verify/` 被 `.gitignore` 忽略** ⇒ clean checkout 不可复现） | Not-verifiable |
| 18 | §13 行 10：池维度 bench 组（ObjectPool/MemoryPool/ThreadPool/共享 ByteBudget） | `benches/infra.rs` 组名 `object_pool_acquire_drop`/`memory_pool_acquire_drop`/`thread_pool_reject`/`byte_budget_contention`；CI `Bench compile gate` | Conform |
| 19 | §13 行 10：具体 ns 数字（958-1024ns@4t 等） | 数据仅在蓝图正文；本单未重跑（§9 禁止把机器相关数字当通用基线） | Not-verifiable |
| 20 | §13 行 11：生命周期/资源 conform 语义落地 | `tests/conformance_lifecycle.rs` 实测 7 测试（六桶 + 三资源轴跨实现断言） | Conform |
| 21 | §13 行 11：`ThreadPool::drain(deadline)` 仍 deferred | `skeleton-infra/src/pool.rs` 无 `drain`（仅 `shutdown` `:1221`）；`docs/reference.md` 同 | Conform |
| 22 | §13 行 12：facade/policy profiles 未被多消费方需要，不得预建 | 3 member workspace；无 facade 模块/feature；无 Balanced/LowLatency 等 profile | Conform |
| 23 | §13 行 13：第二非代理应用通过 conform 仍是 future 触发项 | `validation/consumer-sim` 是**模拟夹具**（README 自述 Stage A/B），不冒充真实第二应用；未预拆 crate | Conform |
| 24 | §11 项 1（P0）：spawn 可失败、发布点后移、submit/resize 传播、确定性故障注入 | 同 #11；`tests/thread_pool.rs` 的 `spawn_failure_at_construction_returns_typed_error` / `submit_spawn_failure_returns_task_with_typed_cause` / `join_returns_after_partial_spawn_failure` 等 | Conform |
| 25 | §11 项 2（P0）：DrainExclusive 差分 harness + 不变量 + panic/unwind + Miri 子集 | 同 #13/#14；`runtime.rs:122 DrainExclusive`、`:189 Locked(evidence)` | Conform |
| 26 | §11 项 3（P1）：生命周期报告契约 + close/cancel 幂等测试 | 同 #7/#8；`fx.rs:355` | Conform |
| 27 | §11 项 4（P1）：剩余错误分类对齐 §7；不得把构造期 panic 改 Result | 构造期 panic：`pool.rs:353/970-982`、`objpool.rs:419/835`、`memgov.rs:64`；运行期 typed：`AdmissionOutcome`/`AdmissionError`/`RejectedTask`/`TaskError`/`StoreClosed`/`RegisterError` | Conform |
| 28 | §11 项 5（P1）：cargo tree 架构回归 + feature 隔离 + 公共 Store 契约保持 | `ci.yml` sync-only 步 + feature-isolation（14 叶+full）+ zero-and-default-face job；`Store` 签名未变 | Conform |
| 29 | §11 项 6（P1）：以 ADR-0008 两档为precedent，定义 effect 侧两档并测试 | `skeleton/src/lib.rs:433` `EffectRuntime` 文档「执行两档（同步档/托管档）」；`tests/effect_two_tier.rs` 5 测试（含 `async_tier_dispatch_returns_before_effect_completes`） | Conform |
| 30 | §11 项 7（Deferred）：CancelToken parent/child 决策未拍板前不得引 tokio-util/watch-per-child | `fx.rs:6` 明示不引入；`Cargo.toml`/`Cargo.lock` 无 `tokio-util`；`CancelSource/CancelToken` 为 watch 单值（`:150/181`） | Conform |
| 31 | §11 项 8（P2）：资源语义文档（逐池覆盖群体/超配/读数） | 同 #16 | Conform |
| 32 | §11 项 8：Xray buffer pool 的 check-then-act 在 Xray 仓修，不在本仓加未用子系统 | 本仓无 Xray 代码；无法从本仓验证 | Not-verifiable |
| 33 | §11 项 9（P2）：池 acquire/release、饱和、竞争 bench + 契约先定的 conformance | 同 #18/#20 | Conform |
| 34 | §11 项 10（Deferred）：facade/profiles/独立 adapter-testing-resource crate/workspace rename 等稳定边界+第二消费方 | 3 crate 未拆未改名；无 facade/profile（同 #22） | Conform |
| 35 | §11 项 10：架构 owner 每 release cut/至少 90 天复核并把结论记入 #25 | 过程项，仓库内无记录（`reports/` 有评审报告但不等于 #25 记录） | Not-verifiable |
| 36 | §11 项 11：Xray ConnScope/ArmScope/缓冲池准入/协议数据面在消费仓实现 | 仓外 | Not-verifiable |
| 37 | §4：close ≠ cancel ≠ drain ≠ abort；cancellation signal 不是停止证明；timeout 不是终止 | `skeleton/src/lib.rs:786` `Store::close` 文档；`close_wait*`（`runtime/close_wait.rs:19/42/50`）；`fx.rs:52 TaskError::TimedOut` 文档；`spawn_timeout`/`abort`（`:1259`） | Conform |
| 38 | §4：`CancelToken` 是 UDF 自有 watch 实现，位置 `fx.rs:78-135` | 实际 `CancelSource` 在 `fx.rs:150`、`CancelToken` `:181`、`cancel_pair` `:220`；`:78-135` 现在是 `ScopeClosed`/`AdmissionError`/`FxCounts` | **Diverged** |
| 39 | §5：ObjectPool 三独立轴（alive/idle 字节/降级）不同覆盖群体 | `objpool.rs:1325 claim_growth`（片锁 `alive < share(max)`，`share` `:603`）、`:450 try_take`、`:909 pop_or_build`（`:943 degraded+1`）；`docs/reference.md:590-660` 三轴表 | Conform |
| 40 | §5：ByteBudget 只约束 idle 常驻字节；借出回卷；池析构回卷 | `objpool.rs:406` 类型文档、`:450 try_take`、`:1462 impl Drop`（按 `IdleSlot.weight` 回卷，含中毒片 `into_inner`） | Conform |
| 41 | §5：`weight(&T)` 每次准入测一次并存槽，释放不再调用户钩子 | `objpool.rs:498-502 IdleSlot.weight`、`:1021` 入槽写值、`:1072` 注释、`:1431 weight_of`；释放路径读存储值 | Conform |
| 42 | §5：硬上限必须是锁内 check-and-act 或原子 CAS；不得引入通用 ResourcePermit | 三处 check-and-act（`memgov.rs:568`、`objpool.rs:1325`、`objpool.rs:450`）；无 permit 模块；`docs/reference.md:487-500` 明示「观测面≠准入面，把 metrics 当放行判据=无锁 check-then-act」；蓝图 §13 同段记「No generic permit layer introduced」 | Conform |
| 43 | §5：lease/drop 路径避免 per-acquire 堆分配；优先具体 lease 类型而非 boxed FnOnce | `objpool::Pooled`（`:1534`）/`Owned`（`:1608`）为具体类型；`PooledThreadPool` 互喂；`ThreadPool::Task` 的 Box 被明确声明为类型擦除不可避免（`pool.rs:546`）；`tests/alloc_site.rs` 有分配面锚 | Conform |
| 44 | §6：Effects 应声明 intent/policy（Immediate/Async/Blocking/CpuBound/Deferred/Batch）而非隐含选执行器 | 代码无该策略词表：`Effect` 只有 `Emit/Run/Cancel`（`skeleton/src/lib.rs:255`），执行由宿主 `EffectRuntime` 决定；`fx` 无 `spawn_blocking`；§11 项 6 把可机检范围收窄为「两档并存 + 文档/测试」并已落地 | Not-verifiable |
| 45 | §6：每条排队执行路径需容量+溢出策略（wait/reject/drop-newest/oldest/caller-runs） | `runtime.rs:2026` ingress capacity（typed `Full`）、`:2042` deferred（`CapacityRejected`）、`audit_emit.rs:40` 待投递（丢最旧+计数）、`fx.rs:712/724`（typed 拒绝）、`pool.rs:657` queue_cap（扩容/拒绝/CallerRuns）、`objpool` idle/byte（丢弃+计数/降级） | Conform |
| 46 | §7：错误分类显式（配置/准入/执行/取消/超时/运行时/panic） | `AdmissionOutcome`（`runtime.rs:753`）、`AdmissionError`（`fx.rs:93`）、`RejectedTask`/`RejectCause`（`pool.rs:623/586`）、`TaskError`（`fx.rs:52`）、`StoreClosed`（`runtime.rs:741`）、`RegisterError`（`shutdown.rs:70`）、`OutcomeKind`（`:90`） | Conform |
| 47 | §7：拒绝必须是对提交者可见的结果，不是隐形指标 | `ActionSender::dispatch` 返回 bool + `#[must_use]`（`skeleton/src/lib.rs:612`）；`try_dispatch` typed；`try_spawn_tracked` typed；`execute` 返 `Err(RejectedTask)` | Conform |
| 48 | §7：构造期恒非法参数 panic fail-fast，不批量改 Result | 同 #27 构造器清单（`PoolBase::new` panic 带原因、`ByteBudget::new(total==0)` panic、`ObjectPool::new` 校验 panic、`ThreadPool::new` 四条件 panic） | Conform |
| 49 | §8：保持 skeleton/std-ext/skeleton-infra 三包边界 | `Cargo.toml:3` 三 member；`skeleton` 零依赖；`std-ext` 仅 skeleton；`infra` 依赖两者 | Conform |
| 50 | §8：测试工具与故障注入不得进生产依赖路径 | `testing`/`parse_mock` 在 `#[cfg(feature="testing")]` 后；dev-dependencies 隔离；`SpawnGate` 是公开泛型 seam（生产单态化零开销、非 test-only 依赖） | Conform |
| 51 | §8：Cargo features 是编译期能力选择，不是运行时动态插件 | 无 dylib/plugin 加载面；所有门是 `cfg(feature)` + optional deps | Conform |
| 52 | §9：为 DrainExclusive 建 feature-gated Mutex 对照实现，而非第二套 SyncStore | `runtime/evidence.rs:12 MutexExclusive<T>`；`runtime.rs:1679 new_mutex_reference`（与生产共用 `assemble()`）；`unsafe-evidence` 门 | Conform |
| 53 | §9：四类独立闸（正确性/压力并发/bench 编译/定时性能跟踪） | `ci.yml`（fmt/test/clippy/docs/bench compile/feature 矩阵/MSRV/consumer-sim）+ `perf.yml`（workflow_dispatch+schedule、`continue-on-error`、告警不改退出码） | Conform |
| 54 | §2：只允许一个 Clock；不得建并行契约 crate | `skeleton/src/clock.rs:55` 唯一 `pub trait Clock`（仓内其余命中都在 gitignored `verify/` 快照）；无 `udf-contracts` crate | Conform |
| 55 | §2：ADR-0001..0010 + publishing checklist 是版本化决策记录 | `docs/adr/0001..0010` 10 文件 + `docs/PUBLISHING.md`；`.gitignore` 2026-11-23 改判注释 | Conform |
| 56 | §2：crate-splitting rule——现步骤不改包名/路径/公共 API，不建 facade/adapters/testing/resource crate | 实际三包名/路径未变；无 facade；ADR-0004/0007 为当前布局依据 | Conform |
| 57 | §3：Reducer 恒同步、确定性、无副作用（所有拓扑） | `Reducer::reduce`（`skeleton/src/lib.rs:107-137`）非 async 且 crate 零依赖 ⇒ 无 IO/时钟/随机可达；`docs/reference.md:82` 同口径 | Conform |
| 58 | §12：非目标——不得为概念盒子建 crate、不得运行时动态加载、不得无消费者造 profile | 同 #22/#56；无 `dylib`/profile 面 | Conform |
| 59 | §10：Xray 拥有协议/套接字/缓冲分配；UDF 不替换 xray-buf、不把协议热路径走 Action/Reducer/Effect | 本仓无协议/套接字/数据面代码；无法从本仓验证（应查 xray-core-rust） | Not-verifiable |

## 2. 9 条 Not-verifiable：逐条复核台账（2026-11-25）

每条给齐五项：**为什么不能机械化 / 现在能机械化吗 / 人工证据及其位置 / 复核频率 / 责任边界与 owner**，末尾是可重复验收步骤。
编号换算：`N-03 ≡ 行 #3`，依此类推。

### N-03（行 #3）§13 行 1：反向控制——`--features tokio` / `--all-features` 面必须报红

- **自动化状态**：`AUTO`（本轮落成 CI 硬闸；判定列仍保留原 `Not-verifiable`，改判见 §4）。
- **为什么原来是 Not-verifiable**：蓝图要求「sync-only 断言在 tokio 面必须失败」，这只被一次性人工实验验证过；仓库里没留下任何持久断言，「该闸有鉴别力」只能靠口供。
- **为什么现在能**：反向控制本质是对**探测器本身**的断言，而探测器就是 `cargo tree` + `grep tokio`。同一函数在必须含 tokio 的面上必须回红 ⇒ 完全确定、无环境依赖、毫秒级。
- **落地位置**：`ci.yml` 步骤 `Sync-only dependency tree (no Tokio)`，新增 `assert_tokio_present`（定义 `ci.yml:83-98`，调用 `:99-100`）：`-p udf-skeleton-infra --features tokio` 与 `-p udf-skeleton-infra --all-features` 两面包必须检出 tokio，否则 `::error::` + `exit 1`。与原三条 `assert_no_tokio` 同 job、同 shell、同 `fail` 账本。
- **人工证据**：负向自测（把一条**无 tokio** 的面喂给同一函数 ⇒ 必须报红）本轮实跑通过，输出见 §3。
- **复核频率 / 触发**：每次 push / PR（CI 运行）；人为改动该步逻辑时按「正向 3 条 + 反向 2 条 + 自测」三件套重跑。
- **责任边界 / owner**：依赖图与 CI 闸 —— CI / 架构 owner。
- **可重复验收步骤**：① `cargo tree -p udf-skeleton-infra --features tokio -e normal | grep -q tokio` 必须为真；② 同一命令把 `--features tokio` 换成 `--no-default-features --features runtime` 必须为假；③ CI 红/绿以 `Sync-only dependency tree (no Tokio)` 步为准。

### N-10（行 #10）§13 行 6：变异下两测试 ~0.06s 内变红而非挂死

- **自动化状态**：`MANUAL`（发布前）。
- **为什么不能机械化**：断言说的是「**变异体**下测试会快速失败」——确定性可观测的前提是先注入 mutant（改 `src/` 再还原）。仓库无 mutation runner，把 mutation campaign 塞进 PR CI = 新 job + 工具链不稳定（cargo-mutants 类工具），成本远超收益；issue #86 亦明确「不接受『曾手工变异过』当持续 CI 证据」但不要求把它变成持续闸。
- **人工证据 / 位置**：mutant 补丁 + 命令 + 实测耗时 + 工具版本，贴到 GitHub issue 评论（tracking issue，#86 或其继任）并在 §5 复核日志留一行。候选 mutant（本轮未执行，仅给模板）：把 `fx::shutdown_timeout` 的 `timed_out` 计数改为恒 0（或在收割后才采 `still_running`）⇒ 期望 `tests/fx_lifecycle.rs::timeout_reports_survivors_without_claiming_termination` 与 `tests/conformance_lifecycle.rs::timeout_reports_survivors_without_claiming_termination` 在亚秒级转红。**注意**：本轮未重跑变异（需改 `src/`，不在本单文件域），因此这两个测试名是**候选**而非已证实的杀死对。
- **复核频率 / 触发**：每次 release cut；或改动 `fx.rs` 停机/采样路径、`tests/fx_lifecycle.rs`、`tests/conformance_lifecycle.rs` 时。
- **责任边界 / owner**：fx 生命周期面 —— 运行时 owner。
- **可重复验收步骤**：① 备份 `skeleton-infra/src/fx.rs`；② 施加候选 mutant（单点改动）；③ `CARGO_TARGET_DIR=<隔离目录> cargo +1.96.0 test -p udf-skeleton-infra --all-features --test fx_lifecycle --test conformance_lifecycle -- timeout_reports_survivors`；④ 记录是否在 <1s 内失败（挂死 = 断言不成立，因为护栏应把挂死变成失败）；⑤ 还原并 `git diff --stat` 确认干净。

### N-17（行 #17）§13 行 9：外部探针 crate 断言全部文档声明（`ALL DOC CLAIMS HOLD`）

- **自动化状态**：`MANUAL`（可跑，但**证据不在 clean checkout**）。
- **为什么不能机械化**：探针 crate 实体在 `verify/2026-11-17-p2-resource-axes/probe/`，而 `/verify/` 被 `.gitignore` 忽略 ⇒ fresh clone 既没有探针也没有其 `Cargo.lock`，CI 无从执行。要机械化必须新建受控夹具（`validation/resource-contract/`，含独立 `[workspace]` + 自有 lock）——**新增 crate 超出本单文件域**，需 PM 立项。
- **人工证据 / 位置**：探针源码 `verify/2026-11-17-p2-resource-axes/probe/src/main.rs`、结果 `verify/.../e2e-probe-final.log`、说明 `verify/.../receipt.md`（均为作者工作区，不可移植）；本轮实跑记录见 §5。断言与 `docs/reference.md` 资源表（`reference.md` §资源上限四问）逐项对应。
- **复核频率 / 触发**：改动 `docs/reference.md` 资源表、`pool.rs`/`objpool.rs`/`memgov.rs` 准入路径时**必跑**；否则每 90 天与 N-35 同批。
- **责任边界 / owner**：资源表面语义 —— 池/内存 owner（+ PM 立项迁移到 `validation/`）。
- **可重复验收步骤**：`cargo +1.96.0 run --manifest-path verify/2026-11-17-p2-resource-axes/probe/Cargo.toml` ⇒ 末行必须为 `ALL DOC CLAIMS HOLD`、exit 0（2026-11-25 实测通过）。

### N-19（行 #19）§13 行 10：具体 ns 数字（958-1024ns@4t 等）

- **自动化状态**：`MANUAL`（data-only，按 §9 刻意不设硬阈值）。
- **为什么不能机械化**：这些数字是**某台机器某次采样**的结果，蓝图 §9 明令「数据不作阈值」；共享 runner 上 ±20% 的会话间漂移会让任何绝对阈值变成随机红（该仓已有此教训）。可机械化的是「采集链」（`perf.yml`：显式 bench ID 清单 + rolling baseline + artifact），不是「数字本身」。
- **人工证据 / 位置**：perf workflow run 的 artifact + 报告行；必须绑定 commit / OS / CPU / Rust 工具链 / feature / 采样参数。历史数字只在蓝图 §13 行 10 正文。
- **复核频率 / 触发**：每次触碰 `objpool` ByteBudget 记账或 `benches/infra.rs` 时；发布前一次。
- **责任边界 / owner**：性能面 —— perf.yml owner。
- **可重复验收步骤**：① 本地 `CARGO_TARGET_DIR=<隔离目录> cargo +1.96.0 bench -p udf-skeleton-infra --all-features -- byte_budget_contention`（criterion 按 ID 子串过滤）；② 或 `gh workflow run perf.yml` 后取 artifact；③ 判定口径是「同会话 OLD↔NEW 交替配对」，不是「与历史单点比」——后者在 ±20% 内没有判别力。

### N-32（行 #32）§11 项 8：Xray buffer pool 的 check-then-act 在 Xray 仓修，本仓不加未用子系统

- **自动化状态**：`OUT-OF-REPO`（本仓侧负向可核）。
- **为什么不能机械化**：命题的两半性质不同。**仓外半**（Xray buffer pool 改 check-and-act）只能在 `xray-core-rust` 验证，本仓无该代码。**本仓负向半**（「不新增未用子系统」）今天成立，但它的可机检形式只是「依赖树里没有 xray、没有第二套 buffer pool」，属于**不存在的证明**——写成一个永远为真的 grep 闸没有鉴别力。
- **人工证据 / 位置**：xray 仓对应 PR/issue（buffer pool 准入改造：锁内 check-and-act 或单字原子 CAS）+ 本仓 `docs/reference.md` 中 xray 相关段的「不在本仓实现」声明。
- **复核频率 / 触发**：每次 UDF↔Xray 边界变更（新适配层/新池化件）时；否则 90 天批次。
- **责任边界 / owner**：Xray 集成 owner（仓外）+ UDF 池化 owner（本仓负向）。
- **可重复验收步骤**：① 本仓侧：`cargo tree -p udf-skeleton-infra --all-features -e normal | grep -i xray` 必须为空；`cargo tree -p udf-std-ext --all-features -e normal | grep -i xray` 必须为空。② 仓外侧：在 `xray-core-rust` 检 `buffer` 池准入点是否为单临界区 check-and-act，并附该仓 CI 结果链接。

### N-35（行 #35）§11 项 10：架构 owner 每 release cut / 至少 90 天复核并记入 #25

- **自动化状态**：`MANUAL`（过程项）。
- **为什么不能机械化**：断言的对象是**人的复核行为与记录**，不是代码性质。给它写 CI 步只能检查「日志里有没有新行」，那是把治理伪装成断言（且日期比较会随 CI 时区/节假日失真）。可机械化的只有它要复核的**子项**（下条 checklist），不是「复核发生过」。
- **人工证据 / 位置**：GitHub #25 的架构复核评论（权威记录）+ 本文件 §5 复核日志一行（日期 / 复核人 / 结论 / 子项差异）。
- **复核频率 / 触发**：**每次 release cut，或距上次复核 ≥90 天，取先到者**。
- **责任边界 / owner**：架构 owner（维护者）。
- **可重复验收步骤**（6 项 checklist，逐项记 yes/no）：① 依赖方向仍为 `skeleton ← std-ext ← skeleton-infra`（`cargo tree` + 反向边零命中）；② feature 表（`Cargo.toml` `[features]`）与 `docs/reference.md` 矩阵键集合一致；③ 资源上限表仍完整（含 `SyncStore::set_capacity` ingress 行，C5）；④ `fx::ShutdownReport` 字段表与 `docs/reference.md` 代码块一致（C2）；⑤ 测试计数类表述仍标「快照」（#12 的 30→32→当前 35 漂移即教训）；⑥ ADR-0001..0010 无未 supersede 的冲突。

### N-36（行 #36）§11 项 11：Xray ConnScope/ArmScope/缓冲池准入/协议数据面在消费仓实现

- **自动化状态**：`OUT-OF-REPO`。
- **为什么不能机械化**：断言的主语是**另一个仓库的模块**（ConnScope/ArmScope/协议数据面）。本仓只能验证「自己没有实现它们」这一负向面。
- **人工证据 / 位置**：`xray-core-rust` 中 `ConnScope`/`ArmScope` 的实现与其测试；UDF 侧只提供 `Store`/`EffectRuntime` 契约（本仓 `skeleton/src/lib.rs`）。
- **复核频率 / 触发**：UDF↔Xray 边界的契约变更时；90 天批次。
- **责任边界 / owner**：Xray 集成 owner（仓外）；UDF 侧契约 owner 只负责「契约足够 hosts 自装 scope」。
- **可重复验收步骤**：① 在 `xray-core-rust` grep `ConnScope`/`ArmScope` 并跑其相关测试；② 本仓侧确认协议/套接字代码零命中（`cargo tree` 无 xray、`skeleton-infra/src` 无 listener/accept 类数据面）；③ 把两侧结果一起贴到 tracking issue。

### N-44（行 #44）§6：Effects 应声明 intent/policy 而非隐含选执行器

- **自动化状态**：`MANUAL`（**待裁定的语义项**——见下方风险）。
- **为什么不能机械化**：代码里没有这套词表（`Effect` 只有 `Emit`/`Run`/`Cancel`，执行档由宿主 `EffectRuntime` 决定），所以既不能断言它存在，也不能断言它被违反。原审计把可机检范围收窄为「两档并存 + 文档/测试」（§11 项 6），并由 `skeleton/src/lib.rs` 的 `EffectRuntime` 文档 + `tests/effect_two_tier.rs` 落地——那部分已由行 #29 覆盖为 Conform，本条剩下的纯是设计意图陈述。
- **风险与不冒充**：这一行最接近 `Diverged`（蓝图说「应」，实现没有）。本轮**不改判**：判定变更须维护者裁定「是否要引入策略词表」（= 公共 API 变更，需 ADR）。若裁定「不引入」，应在蓝图 §6 把「should」改成「不是当前设计目标」；若裁定「引入」，另开实现票。
- **人工证据 / 位置**：`Effect` 枚举定义（`skeleton/src/lib.rs`）+ `EffectRuntime` rustdoc + `tests/effect_two_tier.rs`；裁定结论落 GitHub #25 / tracking issue。
- **复核频率 / 触发**：改动 `Effect` 枚举、`EffectRuntime` 契约、蓝图 §6 或 §11 项 6 时。
- **责任边界 / owner**：架构 owner（决策人）。
- **可重复验收步骤**：① 确认 `Effect` 仍只有三个变体且无策略字段；② 确认 `tests/effect_two_tier.rs` 仍覆盖「同步档/托管档」两档；③ 在 tracking issue 记录本轮结论「维持设计意图陈述 / 已收窄为 §11 项 6」。

### N-59（行 #59）§10：Xray 拥有协议/套接字/缓冲分配；UDF 不替换 xray-buf、协议热路径不走 Action/Reducer/Effect

- **自动化状态**：`OUT-OF-REPO`（本仓侧负向可核）。
- **为什么不能机械化**：同 N-32/N-36：主语是 xray 的所有权边界。「协议热路径不走 UDF 环」只能在 xray 仓的调用图里看。本仓侧的负向面（无协议/套接字/数据面代码）由既有闸间接覆盖：行 #1 的 sync-only 依赖树（无 tokio/网络栈）+ 默认面零三方依赖 ⇒ 本仓不可能承载协议数据面。
- **人工证据 / 位置**：`xray-core-rust` 的协议/缓冲分配模块与热路径调用图；本仓 `docs/reference.md` 的非目标声明。
- **复核频率 / 触发**：UDF↔Xray 集成变更时；90 天批次。
- **责任边界 / owner**：Xray 集成 owner（仓外）+ UDF 架构 owner（负向声明）。
- **可重复验收步骤**：① 本仓确认未替换 `xray-buf`：`cargo tree --all-features -e normal | grep -i "xray"` 为空（三 crate 各跑一次）；② 确认协议热路径不经 UDF：在 xray 仓追踪一次请求，检查 Action/Reducer/Effect 是否出现在配额/协议热路径上（应为 0 次）；③ 结果贴 tracking issue。

## 3. 本轮新增的确定性检查（CI）

**只加了一条**，落在既有步骤内（无新依赖、无新 job、无脚本文件）：

| 项 | 内容 |
|---|---|
| 断言 | 行 #3（N-03）反向控制 |
| 位置 | `ci.yml` → job `udf-frame` → step `Sync-only dependency tree (no Tokio)`；新增 `assert_tokio_present`（`ci.yml:83-98`）+ 两条调用（`:99-100`） |
| 为什么加在既有步 | 它要证的正是这一步的**鉴别力**：同一步里三条 `assert_no_tokio` 只有在该探测器「遇到 tokio 会报红」时才不是恒绿闸。放同 job 同 shell，失败原因直接显示在原始闸旁边 |
| 为什么不是新 job | 新 job 只增加排队时间；断言与被验证闸同居一处才可读 |
| 网络依赖 | 无新增类别：该步原本就执行 `cargo tree`；新增两条只是同一命令加 feature 面（`--features tokio` / `--all-features`），首次运行会把 registry 拉取提前到本步（CI 本就需要联网构建） |
| 成本 | 本机实测两条合计 ≈0.25s（`cargo tree` 不编译） |

**负向验证（本轮实跑 —— 正向全绿不能证明闸有鉴别力）**：

```
$ bash <从 ci.yml 原样抽出的 step 脚本>     # 抽取方式：见下，45 行
ok: udf-skeleton-infra (default features) dependency graph is tokio-free
ok: udf-skeleton-infra --no-default-features --features runtime dependency graph is tokio-free
ok: udf-std-ext (default features) dependency graph is tokio-free
ok (reverse control): udf-skeleton-infra --features tokio exposes tokio
ok (reverse control): udf-skeleton-infra --all-features exposes tokio
EXIT=0

# 注入法（把一条必须为假的面喂给同一函数）：
$ assert_tokio_present "NEGATIVE SAMPLE" -p udf-skeleton-infra --no-default-features --features runtime
::error::reverse control failed: NEGATIVE SAMPLE dependency graph has no tokio — the tokio-free assertion above is not discriminating
fail=1        # ⇒ 探测器确实会红；不是恒绿闸
```

抽取方式（CI 步骤文本即被测脚本，避免「文档写的命令」与 CI 实际执行漂移）：
`python -c "…读 .github/workflows/ci.yml，取该 step 的 run: | 块，去 10 空格缩进…" | bash`

**刻意没加的三项**（各有理由，不是遗漏）：

1. **mutation smoke（N-10）**：需要 cargo-mutants 类工具或手写 patch runner ⇒ 新 job + 工具链不稳定，收益低于维护成本。留 `MANUAL`，步骤见 §2-N-10。
2. **`cargo metadata` 反向边 DAG 闸（issue 建议）**：与既有 `Sync-only dependency tree` 高度重叠，且需要解析器/内联 python；当前反向依赖在代码级零命中（原报告 §0），先按 90 天 checklist 第 ① 项人工核（§2-N-35）。若 PM 要求硬闸，建议单独票（含负向自测）。
3. **资源探针迁移（N-17）**：需要新建 `validation/resource-contract/` crate ⇒ 超出本单文件域。

## 4. 分类变更记录

| 行 | 原判定（`d2824db`） | 现判定（`23b0a42`） | 变更理由 | 新增证据 |
|---|---|---|---|---|
| #12 | **Diverged**（蓝图「30 total」，实测 32） | **Conform** | 蓝图已删除固定计数，改为「当前树快照，数字会增长」⇒ 「数值矛盾」这一矛盾源消失（`52057f87` / C8） | `docs/architecture/target-architecture.md:330`：`… (32 tests in the current tree — counted as #[test] attributes; the number grows, so treat it as a snapshot, not an API contract)` |
| #38 | **Diverged**（蓝图写作 `fx.rs:78-135`） | **Conform** | 蓝图改用符号锚 + 快照区间，收敛行号漂移（`52057f87` / C7） | `docs/architecture/target-architecture.md:120`：`… (fx::CancelSource / fx::CancelToken, whose only constructor is fx::cancel_pair(); symbol anchors, not line numbers — 2026-11-23 snapshot: fx.rs:150-223 …)` |
| #3 (N-03) | Not-verifiable | **维持 Not-verifiable**（自动化状态 → `AUTO`） | 派单硬约束：不得把 Not-verifiable 写成 Conform。本轮只把检查落地并留档；正式改判留给下一轮审计（届时判定依据 = §3 的 CI 步与负向验证） | `ci.yml:83-100` + §3 实跑输出 |

**当前合计**：50 Conform / 0 Diverged / 9 Not-verifiable（历史快照 48 / 2 / 9 见 §0）。
**残留小漂移（不构成 Diverged，仅记录）**：行 #12 引用的蓝图括号内数字现为 32，而当前树 `#[test]` 实测 **35**（2026-11-25 计数）。蓝图已自述「数字会增长、按快照读」，故不改判；建议下次改文档时直接去掉该数字，避免每年一次同一处漂移。

## 5. 复核日志（人工项的证据落点）

人工项的证据一律**双重落盘**：权威记录在 GitHub（tracking issue / #25 评论），可读副本在此表。禁止只写在 gitignored 的 `verify/`、`reports/`。

| 日期 | 项 | 执行人 | 结论 | 证据位置 / 命令 |
|---|---|---|---|---|
| 2026-11-25 | N-17 | BlueprintLedger | 通过：探针可跑，输出 `ALL DOC CLAIMS HOLD`，exit 0（`CARGO_TARGET_DIR` 隔离，工具链 1.96.0） | `cargo +1.96.0 run --manifest-path verify/2026-11-17-p2-resource-axes/probe/Cargo.toml`（证据在作者工作区，clean checkout 不可复现） |
| 2026-11-25 | N-03 | BlueprintLedger | 通过：反向控制两条绿 + 注入无 tokio 面必红 | §3 实跑输出；`ci.yml:83-100` |
| — | N-10 / N-19 / N-35 / N-44 / N-32 / N-36 / N-59 | — | 待下一次 release cut / 90 天批次 | 见 §2 各条步骤 |

## 6. 缺口与需 PM 同步的条目

1. **CHANGELOG 死链（PM 的文件，本单未改）**：`CHANGELOG.md:169` 的条目尾部指向 `reports/2026-11-23-layer-boundaries.md`（gitignored ⇒ clean checkout 打不开）。应改为本文件：`docs/audit/2026-11-23-blueprint-assertions.md`。注意 issue #86 评论 3 建议的目录是 `docs/audits/`（复数），本单契约指定 `docs/audit/`（单数）——以本单为准，改 CHANGELOG 时勿写错。
2. **N-17 探针迁移**：建议新建 `validation/resource-contract/`（独立 `[workspace]` + 自有 `Cargo.lock`，只依赖公开 API），CI 用 `--locked` 执行；否则本条永远只能靠作者工作区复现。需要新建 crate ⇒ 需 PM 立项。
3. **N-10 变异 smoke**：是否值得一个轻量 CI job，属成本/收益决策（见 §3「刻意没加」第 1 项）。
4. **N-44 待裁定**：蓝图 §6 的策略词表是「落实」还是「改成非目标」，需维护者裁定（会动公共 API）。
5. **行 #12 的 32→35 漂移**：建议下次改蓝图顺手去掉括号内数字。
6. **证据可移植性总则**：`.gitignore` 忽略 `/verify/` 与 `/reports/`，因此**任何**以这两处为唯一载体的结论都不是 clean-checkout 可复核证据。本文件把 59 行判定与 9 条理由搬进 tracked tree，正是为消除这一缺口；后续复核记录请按 §5 表格式追加。

