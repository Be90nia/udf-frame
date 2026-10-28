# ADR-0004：十三常驻域物理拆分第一步——`std-ext` 子 crate（六叶子域迁移）

- 状态：已采纳（2026-10-18，udf-frame 批 5-1）
- 决策人：用户拍板（十三常驻域拆独立子 crate；架构时机窗口逻辑：初期拆便宜）
- 关联：批 5-2（paths/clock/retry/scheduler/ratelimit/cache/id 七域后续处理）

> **口径修正指针（2026-10-27，PM 加；本 ADR 正文为历史记录不改）**：本文「`std-ext` 零三方」
> 口径（§零三方口径段、批 5-1/5-2 完成态段）**已被 ADR 0007 收窄并部分纠正**——① `json` 域
> （2026-10-23 由 P1-9 自 xray-conf 上浮）引 `serde_json` 且为**普通依赖零 feature 常开**，
> 使 `std-ext` 与 `skeleton-infra` 的默认树「零三方」承诺同时失真；② ADR 0007 决定将 `json`
> 域门化为 `json` feature（`serde_json` 转 optional），**默认树恢复零三方**，终态措辞以
> ADR 0007 §3/§4.3 为准（十三域零门常驻 + `json` 域门控）；③ 本文「域可独立取用」的诉求
> **已由物理拆分闭合**（`std-ext` 拆出即闭合），ADR 0007 实测证伪「未用域拖全部域进二进制」
> （null 探针与无依赖基线逐字节相同），其口径按 ADR 0007 §2.1 收窄。
> 决策与论证全文见 `docs/adr/0007-std-ext-packaging-granularity.md`；落地执行见 bd 单
> `udf-frame-zex`。

## 背景（盲测槽点）

十三常驻域全部住在 `skeleton-infra` 单 crate：契约实现 + 生态绑定 + 防坑门面 + 散件标准件混居，
体量耦合（src 目录单层 20+ 模块文件）。盲测审计口径：域边界只存在于文档与 feature 表，
物理上不可单独取用——采用方要一个纯 std 的 `hex_encode` 也得面对整个 infra 的 feature 矩阵。

批 5-1 开工前依赖网盘点（PM 已验证 + 本批逐域复核一致）：

- `runtime`/`journal`/`pool`/`fx` 对十三散件**零直接依赖**；
- 六叶子域（text/encode/hash/iter/backoff/fsx）互相之间与对 infra 其余域**零依赖**（逐域
  `use`/`crate::` 引用闭合验证），本体纯 std；
- infra 内部对六域的引用面收口在 5 处：config（atomic_write ×2+doc link）、logx（doctest+
  测试 mod）、retry（backoff_delay）、id（doc link）；
- `backoff` 对 `clock` **零依赖**（纯 std Duration 算术，sleep 归调用方）——不触发「backoff 留
  给批 5-2 与 clock 同批」的条件分支；
- 唯一隐藏共享面：`embed_event!` 埋点宏（文本作用域，infra lib.rs 定义）被 backoff(1)/fsx(6)/
  iter(2) 共 9 个调用点使用（`log-embed` feature，默认关）。

## 决策

**子 crate 物理拆分，而非 feature 门**。理由：

1. **消费目标不同**。feature 门解决「依赖可选」，解决不了「域可独立取用」：feature 关掉后域
   代码仍在 infra 包里，采用方无法只拿散件。子 crate 让叶子标准件成为独立发布单元。
2. **依赖方向必须成层**。拍板分层 `skeleton`（最底层契约）← `std-ext`（散件）←
   `skeleton-infra`（核+胶水，反向依赖 std-ext）。feature 门做不出 crate 间分层（infra 内部
   域间引用仍走 `crate::`，层边界永远靠约定）。
3. **物理拆分迫使依赖网显式化**。拆分过程强制把六域的全部引用面（5 处 infra 引用+埋点宏+集成
   测试+示例）清点改写，依赖闭合由编译器背书，不再靠盘点文档。

**std-ext 零三方口径**：六域本体纯 std；唯一例外是 `log-embed` 埋点的 optional tracing（默认
关=不进树，`cargo tree -p std-ext` 零三方证明口径与 infra 一致）。埋点宏为 infra lib.rs 同款
文本作用域副本（宏未导出，两 crate 各自一份）；**target 前缀沿用 `skeleton::<domain>` 不改**——
观测契约（EnvFilter 表达式与测试断言字符串）跨 crate 稳定，不随物理拆分更名。

## 切面表

| 处置 | 域 | 依赖网证据 |
|---|---|---|
| **迁 std-ext（本批）** | text / encode / hash / iter / backoff / fsx | 六域互依赖与对 infra 其余域依赖均零（逐域闭合验证）；backoff 对 clock 零依赖；纯 std 本体 |
| **留 skeleton-infra（核+胶水）** | runtime / journal / pool / memgov / testing / compose / logx / tracingx / config / shutdown / fx | 对散件零直接依赖（PM 盘点）；shutdown 依赖 runtime+scheduler+fx=胶水件 |
| **待批 5-2** | paths / clock / retry / scheduler / ratelimit / cache / id | retry→backoff+clock；ratelimit→retry+clock；scheduler/ratelimit/cache 消费 skeleton::clock 契约+MockClock；id 推荐 stable64 派生（doc 引用）；clock 域含 rfc3339 行为件 |

## 后果

**正向**：

- `std-ext` 独立可取用：六叶子域零 feature 门常驻可用，`cargo tree -p std-ext` 默认树零三方；
- infra 瘦身 ~4.3k 行源码（六域 src+fsx 子模块）+ ~1.9k 行测试/示例，feature 表减 5 个常驻行；
- 埋点计数精确对账：原八域 41 调用点 = infra 留守五域 32（scheduler 11/ratelimit 4/retry 6/
  cache 5/shutdown 6）+ std-ext 三域 9（backoff 1/fsx 6/iter 2）。

**破坏性变更清单（无 shim，调用方直迁）**：

| 变更 | 迁移 |
|---|---|
| `skeleton_infra::text::*` | `std_ext::text::*` |
| `skeleton_infra::encode::*` | `std_ext::encode::*` |
| `skeleton_infra::hash::*` | `std_ext::hash::*` |
| `skeleton_infra::iter::*` | `std_ext::iter::*` |
| `skeleton_infra::backoff::backoff_delay` | `std_ext::backoff::backoff_delay` |
| `skeleton_infra::fsx::*` | `std_ext::fsx::*` |
| infra 根再导出（`skeleton_infra::{backoff_delay, iter::*, text::*, encode::*, hash::*}`） | 已删除；std-ext 按模块路径消费（不设根再导出） |

**遗留**：`docs/reference.md` 六域条目仍写 `skeleton_infra::` 旧路径（docs 批同步，本批不动
README/docs/tutorials）。

**验证锚**：正典口径 `cargo test --workspace --all-features --no-fail-fast -- --test-threads=1`
全绿（计数见批 5-1 报告）；std-ext 单 crate 默认 156 / all-features 170，infra all-features
467，均 0 failed；clippy `--workspace --all-features --all-targets` 零警告；
`cargo tree -p std-ext` 零三方。

## 批 5-2 完成态（2026-10-18，终版切面表）

七域（paths/clock/retry/scheduler/ratelimit/cache/id）迁入后，十三常驻域全量住
`std-ext`，拆分收口。终版切面：

| 处置 | 域 | 说明 |
|---|---|---|
| **std-ext（终版十三域）** | text / encode / hash / iter / backoff / fsx（批 5-1）+ paths / clock（含 `clock::rfc3339`）/ retry / scheduler / ratelimit / cache / id（批 5-2） | std-ext 内部自闭合：retry→`crate::backoff`+`crate::clock`；ratelimit→`crate::retry`+`crate::clock`；scheduler/cache/id→`skeleton::clock` 契约；id doc 推荐 `crate::hash::stable64`。零三方口径不变（tracing 仅 log-embed optional；humantime dev-dep 随 RFC3339 对照测试迁入，不进 normal 树） |
| **skeleton-infra（核+胶水）** | runtime / audit / journal / pool / memgov / testing / compose / logx / tracingx / config / fx / shutdown / parse_mock / pooled_thread_pool | shutdown 留守：胶水件（依赖 runtime+`std_ext::scheduler`+fx，方向 infra→std-ext 合法）；logx 消费 `std_ext::paths::home_dir` |

**迁移细则**：各域集成测试随迁（paths_alloc/paths_io/paths_xdg/retry/scheduler/ratelimit/
cache/id 八文件）；`log_embed.rs` 断言按域拆分（std-ext 七域 35 断言 / infra shutdown 2 断言，
捕获订阅器同款各一份）；自持采集器 `cache_multidim` 迁 std-ext/examples，memgov 依赖的五采集器
（time/id/paths/ratelimit/scheduler_multidim）留守 infra examples 改测 `std_ext::` 面（测量基建
贴 memgov，被测域路径随域迁）；埋点宏同款副本+target 前缀 `skeleton::<domain>` 稳定契约不变。

**埋点对账（批 5-2 新基线）**：41 = std-ext 35（backoff 1 / fsx 6 / iter 2 / scheduler 11 /
ratelimit 4 / retry 6 / cache 5）+ skeleton-infra 6（shutdown）。
