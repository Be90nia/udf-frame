# ADR-0002：log feature 拆分（`log-fs` 纯 std 底座 + `log` 门面绑定层）

- 状态：已采纳（2026-10-18，udf-frame 批 1）
- 决策人：用户拍板（三档拉满批次·批 1，授权 API 破坏性变更）
- 关联：ADR-0001（契约下沉——`LogPathResolver` trait 迁出由本拆分承载）

## 背景（杠精杠点）

杠精二轮审计（agentwiki 根 reports/2026-10-17-devils-advocate-2.md，架构维 2/5）指出 **log feature 门粒度绑死**：纯 std 的文件日志面（`FileSink` 按日滚动 / `mask_secrets` 密钥掩码 / 路径解析）被整门绑在 tracing 四件套之后——

```toml
log = ["dep:log", "dep:tracing", "dep:tracing-subscriber", "dep:tracing-log"]
```

后果：只想把日志写文件的采用方被迫拉入 log+tracing+tracing-subscriber+tracing-log 四个三方依赖（含 EnvFilter 正则引擎等重物）；而这套文件面本体（std::fs 追加写 + SystemTime 换日 + 字符串替换）零三方可行。

## 决策

1. **新增纯 std feature `log-fs`（零三方依赖）**，承载文件落盘面：
   - `mask_secrets`（密钥掩码）；
   - `DefaultPathResolver` + `HomeDirUnresolvable` + `release_root`（路径解析实现件；契约 `LogPathResolver` 同批下沉 `skeleton::logpath`，见 ADR-0001）；
   - `FileSink`（+ SinkState/utc_epoch_days/civil_from_days/day_file_name）。
2. **`log` 变为叠门**：`log = ["log-fs", "dep:log", "dep:tracing", "dep:tracing-subscriber", "dep:tracing-log"]`——只承载门面绑定层：`init_tracing_default`/`init_tracing_from_env`/`env_filter_or`/`to_tracing_filter`/`AlreadyInit`/`LogInitError`/`FileSinkWriter`（MakeWriter 适配壳）/`LineFormat`/`LineFields`/`init_with_file`。
3. **单开 `log-fs` 即可用文件日志**：`FileSink` 实现 `std::io::Write`（软失败面：丢弃计数+stderr 一次告警），宿主不经 tracing 直接写入。
4. required-features 并集矩阵同步：`perf_batchc` = `["compose", "tracing", "log-fs"]`（mask 实际用量面）；`log_multidim` 保持 `["log", "memgov"]`（用 init_with_file）。

## 后果

**正向**：

- 文件日志零三方可达（`cargo tree -p skeleton-infra --no-default-features --features log-fs -e normal` 树内仅 `skeleton`，workspace path 非三方）；
- FileSink/mask/路径解析在 `--features log-fs` 单开下 8 项具名单测全绿（rollover/软失败/日期锚/mask 三态/debug 编译期锚）；
- feature 矩阵语义清晰：`log-fs`=数据面，`log`=绑定面，依赖单调递增无交叉。

**破坏性变更清单**：

| 变更 | 迁移 |
|---|---|
| 仅用文件日志的采用方 | feature `log` → `log-fs`（API 名不变，`skeleton_infra::logx::{FileSink, mask_secrets, DefaultPathResolver}` 路径不变） |
| `skeleton_infra::logx::LogPathResolver` | `skeleton::logpath::LogPathResolver`（ADR-0001） |
| `log` feature 依赖树 | 多出 `log-fs`（空 feature，零成本） |

**门控细节**：logx 模块声明门 `log` → `log-fs`；tracing 绑定件在模块内 `#[cfg(feature = "log")]` 二级门控（含 Display/Error impl 与 LineFormat 行格式器）；`format_utc_timestamp` 属绑定层（LineFormat 行时间戳），log-fs 单开时门出、其测试同门。

**验证锚**：`cargo test -p skeleton-infra --no-default-features --features log-fs` 全绿（175 lib 单测 + FileSink 面全过）；`--all-features` 全量 678 passed / 0 failed；clippy `--workspace --all-features --all-targets` 零警告。
