# udf-frame

最小单向数据流（UDF / Elm 式架构）Rust 框架：状态迁移是纯函数，副作用是描述式数据，解释器在边缘执行。

```
action ──▶ Store ──▶ Reducer（纯函数）──▶ 新状态 + 效果描述 ──▶ EffectRuntime（解释器）──▶ 回灌
                │                                                    │
                └──────────────── 订阅通知（快照） ◀─────────────────┘
```

## Crate 结构（三步分层）

| crate | 层 | 内容 | 依赖 |
|---|---|---|---|
| [`skeleton`](skeleton) | 契约层 | trait（`Reducer`/`Store`/`EffectRuntime`/`Driver`/`JournalPort`/`PoolManaged`/`JsonPort`/`SqlDialectPort`/`MarkdownPort`）+ 数据词汇（`Effect`/`EffectBuf`/`EffectId`/`ActionSender`/`Subscription`/`NoFx`/`Restore`/`PoolMetrics`/`Json`/`MdTag`/`MdEvent`）+ 语义契约文本 | **零依赖** |
| [`std-ext`](std-ext) | 散件层 | 十三个纯 std 常驻域（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`）+ `log-embed` 埋点（七域 35 调用点） | 仅 skeleton 路径依赖，零三方 |
| [`skeleton-infra`](skeleton-infra) | 实现层 | `runtime::SyncStore`（契约参考实现：同步排空环 + 快照广播）+ `audit` 审计事件流 + `pool` 池化管理组合件 + `compose` 组合器 + `testing` 测试替身（含 `parse_mock` 解析面 mock）+ `shutdown` 停机编排 + 生态绑定（tokio fx / journal / memgov / logx / tracingx / config） | runtime（默认）std-only + skeleton/std-ext 路径依赖，零三方 |

契约（约束后面代码长法的形状）与实现（机器）分家：`skeleton` 十分钟读完，`skeleton-infra` 按域查典。核心无 IO 依赖 → reducer「禁 IO」是编译期事实，不是评审约定；副作用只能以 `Effect::Run { op }` 描述值离场，由宿主提供的解释器执行。

## 快起

```sh
cargo run -p udf-skeleton-infra --example quickstart
```

最小环（完整代码见 [skeleton-infra/examples/quickstart.rs](skeleton-infra/examples/quickstart.rs)，逐字同步）：

```rust
let store = Store::new(Counter, NoFx); // skeleton_infra::runtime::SyncStore
store.dispatch(Msg::Inc);
store.dispatch(Msg::Inc);
assert_eq!(store.snapshot().0, 2); // 同步排空：dispatch 返回即 idle
store.close();
```

**自建项目抄这段时**（`Cargo.toml` 补三行——仓库外建项目，见 [cheatsheet 坑 7](docs/tutorials/cheatsheet.md)；`[workspace]` 装甲行防被上层 workspace 吸收）：

```toml
[workspace]

[dependencies]
udf-skeleton = { path = "<UDF_ROOT>/skeleton" }
udf-skeleton-infra = { path = "<UDF_ROOT>/skeleton-infra" }
```

契约即可实现：十几行即可实现 `skeleton::Store`（见 [skeleton](skeleton) 的 crate 文档 toy 环）。

**串联教程**：[docs/tutorials/thirty-min.md](docs/tutorials/thirty-min.md)——30 分钟走完非零效果路径（journal 持久化 → FxScope 效果作用域 → combine 分片 → TestStore 验收），每块代码真实编译运行过。

**第一天提示**（盲测实测 8 坑的高频五处，全表见 [cheatsheet](docs/tutorials/cheatsheet.md)）：
- `FxScope::spawn` 需要 tokio reactor 上下文——同步 `main` 直接用会 panic，demo 从 `#[tokio::main]` 起步；
- `JsonlJournal` 的 payload 必须序列化为 JSON object（enum 记得内部 tag，如 `#[serde(tag = "kind")]`——tag 键任取，`"t"` 是 journal 行 tag 顶层保留键会被拒；unit variant 裸字符串会被拒）；`replay` 前先 `flush`——flush=fsync 掉电真持久（干净 drop 亦有 BufWriter 兜底，真丢数据分界是进程崩溃）；
- 账本调用惯用形：`JsonlJournal::<A, S>::create(..)` 构造期一次定型，此后全直呼零标注——`j.append_action(&a)`、`MutexGuard` 后 `g.append_action(&a)`、`j.replay()` 返回值全推断；`combine` 两形态都只收 **tuple variant** 构造器（unit variant 路由用显式闭包 `|_: ()| Act::Ping`）；
- demo 项目**建在 udf-frame 仓库外**——树内 `cargo new` 会被根 workspace 吸收损坏仓库；
- `Scheduler::after/every` 注册仅入堆——生产面需 `spawn_worker()`（或确定性测试 `tick()`）才有点火驱动。

## skeleton-infra 的 feature 门

| feature | 域 |
|---|---|
| `runtime`（**默认**） | `SyncStore`：`skeleton::Store` 参考实现（std-only）+ `audit` 审计事件值面 |
| `tokio` | `FxScope` 效果作用域：解释器在 tokio 任务里执行，回灌经 `ActionSender::dispatch` 重入 |
| `journal` | `JsonlJournal`（`JournalPort` 契约端口参考实现）：JSONL 追加式日志 + 快照行 + 回放（serde） |
| `memgov` | 计数分配器 + 有界注册表（淘汰策略必选：满则报错或逐最旧） |
| `pool`（**默认**） | `PoolBase` 池化管理组合件：resize 校验/水位线/审计/溢出计数公共逻辑写一次，`PoolManaged` 契约（skeleton）+ 双池特化（`MemoryPool`/`ThreadPool`）+ `PooledThreadPool` 互喂组合件（任务闭包内借缓冲→跑→RAII 还） |
| `testing` | `TestStore` 逐动作断言 + `ScriptedRuntime` 脚本化解释器替身 + `parse_mock` 解析端口三 mock（`MockJson`/`MockSqlDialect`/`MockMarkdown`） |
| `compose` | `combine`/`combine!` 父子分片组合器：宏一行装配（只依赖契约词汇） |
| `log-fs` | `logx` 文件落盘面（纯 std 零三方）：密钥掩码 + 路径解析（契约 `LogPathResolver` 住 `skeleton::logpath`；`DefaultPathResolver` debug=编译期锚**仅框架自身 dogfood 用**——采用方 debug 态必须注入自有 resolver）+ `FileSink` 按日滚动 |
| `log` | `logx` 门面绑定层（=`log-fs`+log+tracing+tracing-subscriber+tracing-log）：init 系（`init_tracing_from_env`/`init_with_file` console+file 双 sink） |
| `tracing` | `tracingx`：审计事件流 → tracing 桥 |
| `log-embed` | 行为+数据域 tracing 埋点（本 crate：shutdown 一域 6 调用点；`std-ext` 自有同款 feature：七域 35 调用点；合计八域 41，target `skeleton::<domain>[::<phase>]`，级别规范见各模块头注；默认关=宏消除，tracing 不进树） |
| （常驻，零 feature） | **std-ext 十三域**（`text` `encode` `hash` `iter` `backoff` `fsx` `paths` `clock` `retry` `scheduler` `ratelimit` `cache` `id`，随依赖树常驻，逐域职责见 `std-ext` crate 文档与 `docs/reference.md` 域索引）+ 本 crate `shutdown`（行为件面；时间源/配置/日志路径契约已下沉 `skeleton::clock`/`skeleton::config`/`skeleton::logpath`） |

`cargo tree -p udf-skeleton-infra -e normal`：默认 feature 下仅 skeleton + std-ext（workspace 路径依赖），零三方；`cargo tree -p udf-std-ext -e normal` 仅 skeleton，零三方。

**只要散件**（不需要状态环/运行时）：十三域散件单依赖即取即用，纯 std 零三方——限流+重试+缓存+调度这类组合一个 dependencies 块就够：

```toml
[dependencies]
udf-std-ext = "0.1.0"   # lib 名 std_ext；use std_ext::{ratelimit, retry, cache, scheduler, ...}

# 可选：行为域 tracing 埋点（默认关=宏消除零开销、tracing 不进树）
# udf-std-ext = { version = "0.1.0", features = ["log-embed"] }
```

## 池化域（`pool` feature）

管理面框架写一次，池只写特化面——「约束后面代码长法」的池化版：后面任何人写的池都不会乱。

| 件 | 定位 | 特化面 |
|---|---|---|
| `skeleton::pool::PoolManaged` | 契约层基座 trait：resize/metrics/审计三挂点，`Tuning`/`Metrics` 关联类型，读侧经 `Borrow<PoolMetrics>` 统一核心形 | — |
| `pool::PoolBase` | 组合件：校验/水位线/审计/溢出计数公共逻辑写一次 | — |
| `memgov::MemoryPool<T>` | 内存池：有界槽 + 获取超时必填 + RAII 归还 | 槽位借还 |
| `pool::ThreadPool` | 动态线程池：worker 调度 + 拒绝策略四件套 + 优雅缩容 | worker 调度 |
| `pooled_thread_pool::PooledThreadPool<T>` | 组合类：双池互喂时序封装（worker 线程内借缓冲→跑→panic 亦自动还） | 借还时序 |

第三者接入只需写特化面（[examples/connection_pool.rs](skeleton-infra/examples/connection_pool.rs)）：
`ConnectionPool<T>` 只接 `PoolManaged` + `PoolBase` 两件，动态调参/水位线/审计/溢出计数
全套自动到手——总 203 行，其中管理面 25 行（全一行委托、零逻辑重写）、特化面 95 行，
约为双池体量的 1/5（MemoryPool 池化段 473 行 / ThreadPool 段 520 行，各含超时、
调度循环、拒绝策略等机器；实测对表 [reports/2026-10-16-pool-wave3.md](../reports/2026-10-16-pool-wave3.md)）。
组合类最小例见 [examples/pooled_thread_pool.rs](skeleton-infra/examples/pooled_thread_pool.rs)：

```sh
cargo run -p udf-skeleton-infra --example connection_pool
cargo run -p udf-skeleton-infra --example pooled_thread_pool
```

## 生态缝地图（宿主接入点）

框架留口、实现自换——每一缝都是稳定端口，随附实现可整体替换：

| 缝 | 端口 | 随附实现（skeleton-infra） | 用途 |
|---|---|---|---|
| 效果托管 | `EffectRuntime`（skeleton） | `NoFx` 缺省件 / `fx::FxScope`（tokio feature） | 副作用以描述值在边缘解释，经 `ActionSender` 回灌闭环 |
| 状态持久化 | `JournalPort`（skeleton，零依赖） | `journal::JsonlJournal`（journal feature） | 先落盘后 apply；后端自换 = 另实现端口（SQLite/网络皆可） |
| 观测 | 审计事件值面（runtime） | `audit::AuditEvent` + `tracingx`（tracing feature） | 事件流桥入 tracing，观测栈由宿主自选 |
| 测试替身 | `Reducer` 契约直测 | `testing::{TestStore, ScriptedRuntime}`（testing feature） | 逐动作断言 + 脚本化解释器 |
| 解析面 | `JsonPort`/`SqlDialectPort`/`MarkdownPort`（skeleton，零依赖） | `parse_mock` 三脚本化 mock（testing feature）；真实实现=宿主 optional dep（serde_json 等）或采用方自供 | 框架定义形状，生态提供实现（journal 先例推广） |
| Feature 组合 | `Reducer` 纯函数自然组合 | `compose::{combine, combine!}`（compose feature） | 父子分片：镜头+路由+效果上提，宏一行装配 |

## 埋点规范（观测面）

框架自身埋点走统一规范，宿主用一条 `RUST_LOG` 控制粒度：

| 维度 | 规范 |
|---|---|
| target 命名 | `skeleton::<域>`——`::` 分层对齐 EnvFilter 前缀匹配，单域开关=一个前缀指令（既有**非埋点** target：`skeleton::audit`=tracingx 审计桥，与埋点八域互不重叠） |
| 级别 | 正常流转=debug / 异常=warn / 终态=info |
| 不埋点 | 纯函数域（paths/clock/encode/hash/iter/config）：无行为决策点，埋点零诊断价值（拍板归档 [reports/2026-10-17-log-embed-c.md](../reports/2026-10-17-log-embed-c.md)） |

`init_tracing_from_env` 的默认预设=`<default_level>,skeleton=off`——框架默认静默，宿主日志零打扰；`RUST_LOG` 已设且合法时逐字生效（显式值恒赢预设）。三种开启姿势：

```sh
RUST_LOG=debug cargo run -p udf-skeleton-infra --example quickstart            # 全开
RUST_LOG=warn,skeleton::ratelimit=debug cargo run -p udf-skeleton-infra --example quickstart   # 单域：基线 warn + 该域 debug
RUST_LOG=skeleton::scheduler=trace cargo run -p udf-skeleton-infra --example quickstart        # 单流程：只这一个子系统
```

`RUST_LOG` 语法非法时 fail-visible：stderr 报非法语法与回退结果，禁静默降级成另一档级别（配置错误必须可见）。

零开销：过滤关闭时 `event!` 只剩一次兴趣判定（无订阅者零输出面）。实测锚（`skeleton-infra/src/tracingx.rs` 模块文档，采集器 `examples/perf_batchc.rs`，报告 [reports/2026-10-16-perf-batchc.md](../reports/2026-10-16-perf-batchc.md)）：审计桥常挂净转译 ~10ns/事件，订阅态 dispatch 全含 ~142ns（无订阅 ~77ns），有订阅者实收再 +~11ns——桥常挂成本集中在宿主订阅者本体。

## 构建与测试

```sh
cargo build --workspace --all-features
cargo test --workspace --all-features
```

采用方一键全开（tokio 先例）：`features = ["full"]` = 本 crate 全部子开关并集，等价本 crate `--all-features`；默认集仍最小（零三方树不变），子开关=进阶裁剪。`std-ext` 的七域 35 埋点（`log-embed`）由其自有同款 feature 承载，不随本 crate `full` 点亮（ADR 0005：跨 crate 强制耦合否决）——workspace 全量埋点=两 crate 各自 `--all-features`（CI 同款）。

## License

MIT
