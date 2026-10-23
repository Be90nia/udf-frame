# ADR-0001：契约层真下沉（Clock/ConfigPort/EnvSource/LogPathResolver → skeleton）

- 状态：已采纳（2026-10-18，udf-frame 批 1）
- 决策人：用户拍板（三档拉满批次·批 1，授权 API 破坏性变更）
- 关联：ADR-0002（log feature 拆分）

## 背景（杠精杠点）

杠精二轮审计（agentwiki 根 reports/2026-10-17-devils-advocate-2.md，架构维 2/5）判双层 crate 只兑现 1/3：**被多域消费的核心契约全部住在 skeleton-infra 与实现同床**——

- `Clock`（含 SystemClock/MockClock）：scheduler/ratelimit/retry/cache/id 五域消费，却住在 infra 的常驻模块里；infra 的 `skeleton` 依赖反而是 optional（runtime/pool/journal/compose 各自叠 `dep:skeleton` 门）。契约层名不副实：`skeleton` 只住 UDF 核三件（Store/Effect 族 + parse 三端口 + PoolManaged）。
- `ConfigPort`/`EnvSource`（含 StdEnv/MockEnv/ConfigError）：配置域规范接口，同床于 infra（config-json/config-toml 行为件同文件）。
- `LogPathResolver`：日志路径契约混在 `logx`（tracing 门面绑定 + FileSink 行为件同文件，挂 `log` feature 门）。

契约与实现同床的结构性后果：契约层零依赖纪律被实现件拖着走（消费者为拿一个 trait 被迫依赖整个 infra crate）；infra 的 feature 矩阵被迫为契约维持 optional 依赖叠门（`dep:skeleton` × 4）。

## 决策

1. **迁移清单**（trait+词汇迁 skeleton，实现件留 infra）：
   - `Clock` + `SystemClock` + `MockClock` → `skeleton::clock`（纯 std 零门；时长格式化 `format_duration` 与 `clock::rfc3339` 是行为件，留 infra）；
   - `EnvSource` + `StdEnv` + `MockEnv` + `ConfigPort` + `ConfigError` → `skeleton::config`（纯 std 零门；`ConfigFile`/`load_toml`/`save_toml` 留 infra 的 config-json/config-toml 门后；`ConfigError` 全枚举随迁——infra 文件面复用同一错误类型，避免错误分叉）；
   - `LogPathResolver`（仅 trait）→ `skeleton::logpath`；`DefaultPathResolver`+`HomeDirUnresolvable` 是行为件（含 debug/release 分支与 `paths::home_dir` 依赖），留 infra `logx`。
2. **破坏性直接迁移不留 re-export shim**（0.1.0 未发布，无兼容负担）。
3. **`skeleton` 依赖转常驻非 optional**：常驻域 cache/id/retry/scheduler 直接消费 `skeleton::clock`，故 infra 的 `skeleton` 依赖去 optional；原 `runtime`/`pool`/`journal`/`compose` 的 `dep:skeleton` 叠门移除（workspace path 依赖，非三方——`cargo tree -e normal` 零三方口径不变）。
4. **TDD**：迁移前先在 skeleton 新位置落契约层测试（Clock 假推进/EnvSource 覆盖语义/LogPathResolver 契约），红→迁移实现→绿。

## 后果

**正向**：

- 契约层名副其实：`skeleton::clock`/`skeleton::config`/`skeleton::logpath` + 原 UDF 核三件，被多域消费的契约全部住契约层（杠点清零）；
- 消费者可以零 infra 依赖直接取契约与测试替身（MockClock/MockEnv 随契约走）；
- infra feature 矩阵简化（`dep:skeleton` × 4 叠门消失）。

**破坏性变更清单**（调用方迁移指南）：

| 旧路径 | 新路径 |
|---|---|
| `skeleton_infra::clock::{Clock, SystemClock, MockClock}` | `skeleton::clock::{Clock, SystemClock, MockClock}` |
| `skeleton_infra::config::{EnvSource, StdEnv, MockEnv, ConfigPort, ConfigError}` | `skeleton::config::{…}` |
| `skeleton_infra::logx::LogPathResolver` | `skeleton::logpath::LogPathResolver` |

`skeleton_infra::clock::format_duration`、`clock::rfc3339::*`、`config::{ConfigFile, load_toml, save_toml}`、`logx` 行为件路径不变。本仓内已迁移全部调用点（src 五域 + tests 7 文件 + examples 6 文件 + docs 三份）。

**测试面变化**：契约测试随迁（skeleton 24 单测含 MockClock 确定性 10 项 + EnvSource/ConfigPort 9 项 + logpath 契约 2 项；infra 对应项摘除）；全量 678 passed / 0 failed（--all-features），skeleton `--no-default-features` 自持全绿。

**验证锚**：`cargo test -p skeleton --no-default-features` 全绿（契约层零门自持）；`cargo clippy --workspace --all-features --all-targets` 零警告。
