# std-ext — 常驻生态标准件库

自 `skeleton-infra` 物理拆出的叶子域（P1-9 起十四个）：`text` / `encode` / `hash` / `iter` /
`backoff` / `fsx` / `paths` / `clock`（含 `clock::rfc3339` 行为件）/ `retry` /
`scheduler` / `ratelimit` / `cache` / `id` / `json`。

**依赖纪律（ADR 0007 终态）**：十四域：**十三域零门常驻**（本体纯 std）+
**`json` 域门控**（`json` feature，引 `serde_json`）。默认树
（`cargo tree -p udf-std-ext -e normal`）仅 `skeleton`（workspace 路径依赖）——
**零三方**；开 `json` 门时 `serde_json` 子树按需进树。可选 `log-embed` feature
提供 tracing 埋点（默认关——埋点宏展开为空，`tracing` 不进树）。

分层（ADR 0004）：`skeleton`（契约层，最底层）← **本 crate（散件）** ←
`skeleton-infra`（核+胶水，反向依赖本 crate）。

各域 API 全键参考见 [`docs/reference.md`](../docs/reference.md)；拆分决策与终版
切面表见 [`docs/adr/0004-std-ext-split.md`](../docs/adr/0004-std-ext-split.md)。
crate 命名与发布现状见 [`docs/PUBLISHING.md`](../docs/PUBLISHING.md)。
