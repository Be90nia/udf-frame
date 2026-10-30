# skeleton-infra — UDF 骨架基础设施层

契约实现 + 生态标准件绑定 + 防坑门面。契约（trait 与数据词汇）在姊妹 crate
[`skeleton`](../skeleton)（零依赖），十四个散件域（十三域零门常驻、
本体纯 std，`json` 域门控 `json` feature）在
[`std-ext`](../std-ext)（批 5-1/5-2 物理拆分），本 crate 兑现契约参考实现与
核+胶水件。crate 级文档（`lib.rs`）为权威域典，本 README 是浏览器视角的域清单。

## 域清单（feature 门）

| feature | 域 | 内容 |
|---|---|---|
| `runtime`（**默认**） | `runtime` | `SyncStore`：`skeleton::Store` 契约参考实现（单写者队列 + 同步排空环 + 快照广播，std-only）；`LocalSubscription` 单线程订阅档；`audit` 审计事件值面（`AuditEvent`/`AuditEventKind`/`AuditTicket`） |
| `pool`（**默认**） | `pool` | `PoolBase` 池化管理组合件：resize 事前校验 / 水位线读数 / 池域审计事件流（seq 单调+容量环）/ `dropped_overflow` 溢出计数——公共理念逻辑写一次；双池特化（`memgov::MemoryPool` / `pool::ThreadPool`）+ `pooled_thread_pool::PooledThreadPool` 互喂组合件（借还时序封装，armor #26 一等件） |
| （常驻，零 feature） | `shutdown` | 跨组件优雅停机编排器：注册序串行；组件 panic/超时 fail-visible 上报不拖垮整体；超时=放弃等待非中断（watchdog 线程照跑如实声明） |
| `config-json` | `ConfigFile` | 分层装配（default→file→层叠合并→load）+ `save` 原子落盘（serde_json::Value 统一值面；serde/serde_json 仅此 feature 可达；配置契约 `ConfigPort`/`EnvSource`/`ConfigError` 已下沉 `skeleton::config`） |
| `config-toml` | `load_toml`/`save_toml` | TOML 类型化直读直写 + `file_layer_toml`（TOML↔值面翻译；Datetime 层面显式拒；toml 仅此 feature 可达） |
| `tokio` | `fx` | `FxScope` 异步并发托管（JoinSet 生命周期集 + accepting 闸 + 取消令牌；`tokio::spawn` 直调经 clippy 闸封死） |
| `journal` | `journal` | `JsonlJournal`：`JournalPort` 契约端口参考实现（JSONL 追加式真相源 + 快照行 + 懒回放；serde 仅此 feature 可达） |
| `testing` | `testing` | `TestStore` 逐动作断言 + `ScriptedRuntime` 脚本化解释器替身 |
| `memgov` | `memgov` | `CountingAllocator` 计数分配器 + `BoundedRegistry` 有界注册表（淘汰策略必选；`get`/`get_mut`/`remove` 同款 `Borrow` 泛形借用查找）+ `LeakGuard` 泄漏断言 + `alloc_site` 分配站点目录（`alloc_sites!` 编译期静态表 + 每站点原子计数/两级开关 + 查表零分配） |
| `log-fs` | `logx` | 文件落盘面（纯 std 零三方：`log-fs` 面自身口径，crate 树见下方零依赖口径段）：密钥掩码 + `DefaultPathResolver` 双构建默认根（debug=编译期锚/release=`~/.<项目>/log`；契约 `LogPathResolver` 住 `skeleton::logpath`）+ `FileSink` 按日滚动追加 |
| `log` | `logx` | 门面绑定层（=`log-fs`+log/tracing/tracing-subscriber/tracing-log）：`init_tracing_from_env`（EnvFilter）+ `init_with_file` console/file 双 sink + `FileSinkWriter` |
| `tracing` | `tracingx` | 审计事件流 → tracing 桥 |
| `log-embed` | `shutdown` + `fx` | 行为+数据域 tracing 埋点（本 crate：shutdown 一域 6 + fx 一域 1 = 7 调用点）：shutdown 注册/逐组件开始/终态（Stopped/TimedOut/Failed）/整体报告；fx 托管任务 panic 且句柄已被丢（结果无人消费）=warn，msg=panic 文案（target `skeleton::<domain>[::<phase>]`，级别=正常流转 debug/异常 warn/终态 info；默认关=宏消除零依赖零开销）。批 5-1/5-2 起其余七域 35 调用点由 `std-ext` 自有同款 feature 承载（宏同款文本作用域副本） |
| `compose` | `compose` | `combine`/`combine!` 父子分片组合器（镜头+路由+效果上提，宏一行装配） |

零依赖口径：`cargo tree -p udf-skeleton-infra -e normal`——默认 feature 下仅
`skeleton` + `std-ext`（均 workspace 路径依赖）——**真正零三方**（连
`serde_json` 也不进树：它已随 `std-ext` 的 `json` 域门控 `json` feature）。
各生态 feature 的三方依赖经 feature 门控（serde/tokio/tracing 等），
不随默认面进树。

## 最小示例指引

```sh
# 零效果最小环（默认 feature 即可跑）
cargo run -p udf-skeleton-infra --example quickstart

# 全 feature 端到端（combine + TestStore + fx/journal 解释器标准形）
cargo run -p udf-skeleton-infra --features tokio,journal,testing,compose --example end_to_end
```

其余示例（`examples/`）：`counter_local_runtime`（订阅断言 + Emit 回灌链）、
`driver_tour`（Driver 端口）、`dispatch_profile`（dispatch 热路径分段归因）、
`multidim_eval`（多维实测数据采集器——评测基建非产品面）。

构建与测试（CI 闸同款）：`cargo build --workspace --all-features` /
`cargo test --workspace --all-features`。

## License

MIT
