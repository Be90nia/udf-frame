# udf-frame 发布检查单（PUBLISHING）

适用对象：本 workspace 三 crate——`udf-skeleton` / `udf-std-ext` / `udf-skeleton-infra`。
真发布（`cargo publish` 去 flag、crates.io token）归用户执行；本文是执行前的完整清单。

## 0. 命名与版本策略（拍板 2026-10-18）

**package/lib 名分离**：crates.io 发布名带 `udf-` 前缀，`[lib] name` 保持
`skeleton` / `std_ext` / `skeleton_infra` 不变——`use skeleton::` 等全部 import
路径零改动，采用方代码与本文档代码块无需手术。

| crate 目录 | 发布名（package） | lib 名（use 用） | 版本 |
|---|---|---|---|
| `skeleton/` | `udf-skeleton` | `skeleton` | 0.1.0 |
| `std-ext/` | `udf-std-ext` | `std_ext` | 0.1.0 |
| `skeleton-infra/` | `udf-skeleton-infra` | `skeleton_infra` | 0.1.0 |

改前缀原因：crates.io 上 `skeleton`（2017 年休眠项目）与 `std-ext`（活跃：
try-box/rust-box）均已被占（2026-10-18 API 实测，见 §3）。同版首发：三 crate
锁同版本号，家族内 path 依赖随版本同步升级。

## 1. 发布前置（全部勾完才进 §2）

- [ ] **repository URL**：本仓 `git remote` 未配置，三 manifest 的 `[package]` 段
  repository 字段留白（`skeleton/Cargo.toml:11`、`std-ext/Cargo.toml:11`、
  `skeleton-infra/Cargo.toml:11` 各有一行占位注释）。用户配好 remote 后各补一行
  `repository = "<URL>"`——否则 crates.io 页面无仓库链接，cargo 打包有
  "no documentation, homepage or repository" 警告（repository 项）。
- [ ] **LICENSE 现状**：workspace 根 `LICENSE` = MIT（Copyright 2026 Be90nia）；
  三 crate `license = "MIT"`（SPDX ID）已对齐，无需拍板。可选动作：`.crate` 包内
  实测不带仓根 LICENSE 文本（2026-10-18 tar 核验；SPDX ID 下 crates.io 按 ID
  展示条款）；如需包内携带，把 LICENSE 复制进各 crate 根（cargo 自动打包
  crate 根 LICENSE）。
- [ ] **树净**：commit 全部待发内容后再 publish——`cargo package`/`publish` 硬拒
  未提交变更（error: …contain changes that were not yet committed）。
- [ ] **打包试跑 + 体积审查**（首 crate 发布前即可全跑；两个依赖件受下条约束）：

  ```sh
  cargo package -p udf-skeleton --no-verify
  ls -la target/package/*.crate   # 单包上限 10 MiB；关注 examples/ 是否虚胖
  ```

- [ ] **依赖件干跑的时序约束**（cargo 设计，2026-10-18 实测）：带 `version` 的
  path 依赖在 package/publish 准备期按 version 查 crates.io index——依赖未上库时
  依赖件的 `cargo package`/`--dry-run` 必失败（`no matching package named ...
  found`，`--offline` 不豁免）。故 udf-std-ext / udf-skeleton-infra 的打包与干跑
  只能在前一 crate 上库且索引可见后逐级执行（见 §2）。

- [ ] **干跑**（不消耗 token；本地全链含 verify 编译，止于
  `warning: aborting upload due to dry run`）：

  ```sh
  cargo publish -p udf-skeleton --dry-run
  ```

- [ ] **README 在位**：三 crate 均有（`std-ext/README.md` 2026-10-18 补齐）。

## 2. 发布顺序与 path→version 编辑点

**顺序**：`udf-skeleton` → `udf-std-ext` → `udf-skeleton-infra`（依赖链序；
crates.io 要求被依赖方先在库。索引传播有 30s–数分钟延迟，逐个发）。

```sh
cargo publish -p udf-skeleton
# 等索引可见后：
cargo publish -p udf-std-ext
cargo publish -p udf-skeleton-infra
```

path→version 替换：manifest 中 path 依赖已带 `version = "0.1.0"`
（`std-ext/Cargo.toml:35`；`skeleton-infra/Cargo.toml:64,67`）。发布时 cargo
**自动**把「path+version」重写为纯 version 依赖打进包内，path 仅本地生效——
发布无需手工编辑。未来版本升级编辑点（三处同改）：
① 三 manifest `[package] version`；② 上述两文件三行 path dep 的 `version`。

逐级节奏：`cargo publish` 每步自带 package+verify+上传，无需独立预打包；依赖件的
打包/干跑在本依赖 publish 且索引可见（30s–数分钟）后才可执行（§1 时序约束）。

## 3. crate 名冲突与备选

**已占名实测（2026-10-18，crates.io API）**：

| 名 | 状态 | 占用方 |
|---|---|---|
| `skeleton` | 200 已占 | ntzwrk/skeleton，2017 年起休眠（newest 0.3.3） |
| `std-ext` | 200 已占 | try-box/rust-box 子件，活跃（2026-05 仍在更） |
| `skeleton-infra` | 404 可用 | — |
| `udf-skeleton` / `udf-std-ext` / `udf-skeleton-infra` | 404 可用 | —（本策略采用） |

`std-ext` 活跃无索回可能；`skeleton` 休眠多年理论上可联系原作者（ntzwrk）索回，
但等待期不可控——已按前缀方案绕开，不依赖索回。

**发布前复查指令**（名字是否仍可用，逐个跑）：

```sh
curl -s -o /dev/null -w "%{http_code}\n" \
  -H "User-Agent: udf-frame-publish-check (github.com/<org>/agentwiki)" \
  https://crates.io/api/v1/crates/<name>
```

200=已占，404=可发布。若 `udf-*` 再撞，按 `udf-<名>` 模式扩备选（如
`udf-skeleton-core`），新名需同步 `[package] name`、两处 path dep key、教程第 0 步。

**改名牵动面（本次已完成，改名时复查）**：三 `[package] name` + 三 `[lib] name`
（lib 名不动）、`std-ext/Cargo.toml:35` 与 `skeleton-infra/Cargo.toml:64,67`
dep key、教程 `docs/tutorials/thirty-min.md` 第 0 步 dependencies。注意：仓内
README/docs/CHANGELOG 行文与 CI 中 `-p <旧名>` 式 cargo 命令引用**未**随本次
改名批量更新，采用 `-p udf-*` 前先 grep 全仓校准。

## 4. 发布后动作

- [ ] **git tag**：同版首发建议单 tag `udf-v0.1.0`（三 crate 分 tag
  `udf-skeleton-v0.1.0` 等亦可，仓库惯例二选一后保持）。
- [ ] **CHANGELOG**：`udf-frame/CHANGELOG.md` 的 `[0.1.0]` 节已定版（2026-10-18）；
  发布后可补 compare 链接行（`[0.1.0]: <repo>/compare/<prev>...udf-v0.1.0`，
  本仓现无链接区，可选）。
- [ ] **docs.rs 构建验证**：发布 10–20 分钟后逐个访问
  `https://docs.rs/<crate>/0.1.0`。重点核验 feature 门控模块
  （journal/tokio/log/tracing/compose/config-*）文档是否可见——docs.rs 默认按
  default features 构建；如需全 feature 文档，发布前三 manifest 各加：

  ```toml
  [package.metadata.docs.rs]
  all-features = true
  ```

- [ ] **教程兑现**：`docs/tutorials/thirty-min.md` 第 0 步「首次发布后可换版本号」
  ——把 `<UDF_ROOT>` path 依赖换为 `udf-skeleton = "0.1"` 版本写法并复跑教程。
