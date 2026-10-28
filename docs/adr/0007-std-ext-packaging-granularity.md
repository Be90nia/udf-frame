# ADR 0007：`std-ext` 打包粒度与「零三方」对外口径

- 状态：已采纳（2026-10-27，批 C-5；落地执行见 bd 单 `udf-frame-zex`）
- 决策人：用户批「C 桶六项全动」授权 F-01/F-15 由本 ADR 定案；本 ADR 的机制选择由架构面实证拍板，PM 归口
- 关联：ADR `0004-std-ext-split.md`（三 crate 分层，本 ADR 不推翻其分层结论，只收窄其「域可独立取用」的口径）
  · ADR `0006`（视图桥，草案见 `docs/research/mvi-view-bridge-research.md` §10，尚未落盘）
  · `reports/2026-10-27-udf-architecture-audit.md`（F-01 / F-15 原始 finding，本 ADR 的输入）
  · `docs/PUBLISHING.md`（发布顺序约束，本 ADR 采信其依赖件索引可见性事实）

---

## 1. 背景：两个 finding，一个被误述的痛点

审计给出两条相关但不等价的结论：

| # | finding | 严重度 | 断言 |
|---|---|---|---|
| F-01 | 「零三方」承诺已破 | blocker | `std-ext/Cargo.toml` 的 `serde_json = "1"` 是**普通依赖（零 feature 常开）**，默认树含 `serde_json → itoa/memchr/serde_core/zmij` 五节点；而 README/reference 仍称「仅 skeleton 路径依赖，零三方」 |
| F-15 | 十四域聚合成单 crate，粒度过粗 | 吹毛 | 「采用方想只要一个 `hex` 也得拖 `cache`/`scheduler`/`ratelimit` 全部编译进二进制」；14 域共享一个版本号 |

F-15 的**原始措辞经实测不成立**——本 ADR 的第一件事是把它测准，再决定粒度。

### 1.1 实测：未用域不进最终二进制

一次性探针 crate（`target/` 下 scratch，已删）依赖 `udf-std-ext` 后按使用面链接，release 构建实测字节数：

| 探针 | 依赖 | 触达域 | 二进制字节 | 相对「无依赖基线」 |
|---|---|---|---|---|
| base | 无 | — | 125,440 | — |
| null | `udf-std-ext` | **0** | 125,440 | **±0** |
| small | `udf-std-ext` | 1（`text`） | 129,536 | +4,096 |
| big | `udf-std-ext` | 12 | 206,848 | +81,408 |
| json | `udf-std-ext`（`json` feature） | 1（`json`） | 220,672 | +95,232 |

**null 探针与无依赖基线逐字节相同**：依赖 `std-ext` 却一个域都不用，二进制增量为零。
机制是 rustc 默认 `link-dead-code = no`（移除不被链接的死代码，官方 codegen-options 文档
「remove dead code (the default)」）——未引用的域不进最终二进制。

→ **F-15 的「拖全部域编译进二进制」是错的**。聚合的真实代价只有两项：① 整 crate 冷编译时间；
② 14 域共享一个版本号。

### 1.2 实测：真实代价的量级

- `std-ext` 整 crate 冷编译（dev，14 域全在）：**3.16 s**（一次性，此后增量缓存）。
- `serde_json` 子树冷编译增量（release，scratch 同 workspace 对照）：**≈ 4.4 s**
  （gated 无 serde_json 1.97 s vs gated 开 `json` 6.41 s；差值即 `serde_json`+`serde_core`+`itoa`+`memchr`+`zmij` 五 crate 冷编译）。

### 1.2b 实测：拆簇的编译收益上限

一次性簇探针（`target/` 下 scratch，已删）实测 dev 冷编译：

| 探针 | 含域 | 冷编译 |
|---|---|---|
| 整 `std-ext` | 14 域全在 | **3.16 s** |
| 单簇 `text`+`hash` | 2 域 | **0.27 s** |
| 单簇「时间族」`clock`(含 `rfc3339`)+`backoff`+`retry`+`ratelimit`+`scheduler`+`id` | 6 域 | **0.80 s** |

⇒ 拆簇的编译收益**理论上限 ≈ 2.9 s**（只取最轻簇时）；真实采纳方通常跨 2-3 簇 ⇒ 实际落点
**1-2 s 一次性**冷编译。相对 §2.1 方案 B 的发布链代价（每多一层依赖多一道 30 s～数分钟的
索引可见性闸）不构成理由。

### 1.3 实测：F-01 的破坏面比 finding 写的大一格

`serde_json` 常开不仅破 `std-ext` 一家的「零三方」，**同时破了 `skeleton-infra` 的**——
`cargo tree -p udf-skeleton-infra -e normal` 实测同样含 `serde_json` 子树，
而 README 声称 infra 默认树「仅 skeleton + std-ext（均 workspace 路径依赖），零三方」。

→ 三个 crate 里**两个**的「默认树零三方」承诺同时失真。

### 1.4 未被记录的空白：`P1-9` 自身没有 ADR

审计指出 P1-9（`json` 域自 `xray-conf` 上浮）**零 ADR 记录**。查其判据原文
（`std-ext/src/json.rs:1-3`）：P1-9 的提炼判据是「**零 xray 类型**（全泛型 reader + `serde_json::Value`）」
——即判据落在**域纯度**（不残留 xray 专有类型），**并未就「serde_json 该不该常开」表态**。
`std-ext/Cargo.toml:17-19` 的注释只记录了结果（常开），未记录理由。

→ 本 ADR 补记的正是这个空白：**P1-9 的上浮判断成立，其未审视的副作用（serde_json 变常开）在此纠正**。

---

## 2. 方案对比（实证）

### 2.1 F-15：打包粒度

| 方案 | 内容 | 实测代价 | 判定 |
|---|---|---|---|
| **A 维持单 crate** | 十四域同 crate，零门常驻（现状） | 冷编译 3.16 s；版本号共享 | **采纳**（附复评触发条件，见 §4.5） |
| B 按耦合簇拆 2-3 crate | `udf-std-text`/`-time`/`-fs` | 二进制收益 **0**（§1.1）；编译收益实测**上限 ≈ 2.9 s**（整 crate 3.16 s → 单簇 0.27 s，见 §1.2b），且真实采纳方多用 2-3 簇 ⇒ 落点 1-2 s **一次性**冷编译；**发布链 3→5 crate**：`PUBLISHING.md §1` 实测「带 version 的 path 依赖在 package/publish 准备期按 version 查 crates.io index——依赖未上库时依赖件的 `cargo package`/`--dry-run` 必失败」，即每多一层依赖多一道「已上库 + 索引可见（30 s～数分钟）」的发布闸 | 否决 |
| C 每域一个 feature 门 | 14 门 | 2^14 组合面 = 审计 F-14 在 infra 13 门上已记的组合爆炸再放大；且**推翻 ADR 0004 的「零门常驻」契约**；门组合还需逐一切片测（infra 现状 13 门仅 CI 测 5 片，F-14） | 否决 |

**关键澄清（重估 ADR 0004「域可独立取用」诉求）**：ADR 0004 当时的痛点是「采用方要一个纯 std 的
`hex_encode` 也得面对整个 infra 的 feature 矩阵」——**该诉求已由物理拆分闭合**（`std-ext`
的 feature 表在本 ADR 落地前只有 `log-embed` 一项）。F-15 把「代码仍在包里」与「编译成本」混为一谈（审计自己的措辞），
实测证明前者对二进制不成立、对编译只余 3.16 s 级一次性成本。**故 ADR 0004 的结论保留，其论证口径按本 ADR 收窄。**

### 2.2 F-01：`json` 域的 `serde_json` 依赖形态

| 方案 | 内容 | 代价 | 判定 |
|---|---|---|---|
| **A 门化为 `json` feature** | `serde_json` 转 optional，`json = ["dep:serde_json"]` | 13 个纯 std 域使用者：默认树恢复零三方，冷编译省 ≈ 4.4 s，供应链面少 5 crate；`json` 使用者：多写一个 feature（其 API 返回 `serde_json::Value`，本就离不开 serde_json） | **采纳** |
| B 保持常开 + 文档诚实化 | 承认 `serde_json` 常开 | 零三方的承诺**永久作废**（两个 crate 同时），且变成散文约定而非可验事实；与仓库自身成例相悖：`serde_json` 在 `skeleton-infra` 的 `journal`/`config-json` 门后、`tracing` 在 `std-ext` 的 `log-embed` 门后——**仓库既有纪律就是「三方依赖走 feature 门」** | 否决 |
| C `json` 域迁出 `std-ext` | 回 `skeleton-infra` | 破坏 ADR 0004 的分层（`json` 是叶子行为件不是胶水件）；「只要散件」路线（`std-ext` 单依赖即取即用）的用户为一份 JSON 解码器拖进整个 infra 实现层；`infra` 侧已有 `config-json` 绑 serde_json（配置读写值面），再放一份解码器语义相邻 | 否决 |

**决策的理论依据与仓库自身哲学同构**：README 明写「核心无 IO 依赖 → reducer「禁 IO」是**编译期事实，不是评审约定**」。
「零三方」同理应当是 `cargo tree` 可一行复核的**事实**，而不是需要散文加注的承诺。

### 2.3 外部证据

- Cargo 官方 Features 文档：`dep:` 前缀会关闭同名隐式 feature（故门名取 `json` 而非 `serde_json`）；
  官方可选依赖章节确认 optional 依赖默认不编译 — https://doc.rust-lang.org/cargo/reference/features.html
- Cargo 官方 SemVer 文档：**移除一个 feature** 属 major 变更 — https://doc.rust-lang.org/cargo/reference/semver.html
  （本变更属「新增门 + 移除隐式常开」，因 crate 从未发布（§4.1）故无兼容负担）
- rustc codegen-options：`link-dead-code` 默认 `no`（死代码不进二进制）— https://doc.rust-lang.org/rustc/codegen-options/index.html
- 生态先例：`reqwest` 把 JSON 支持门在 `json` feature 后（`RequestBuilder::json` 需显式开该门）—
  https://docs.rs/reqwest/latest/reqwest/

---

## 3. 决策

1. **打包粒度：维持 `std-ext` 单 crate、十四域同住**（方案 A）。不按耦合簇拆 crate，不引入每域 feature 门。
   未用域不进最终二进制（实测 ±0 字节），聚合的真实代价仅为 3.16 s 级一次性冷编译与版本号共享；
   拆 crate 的编译收益实测上限 ≈ 2.9 s（真实落点 1-2 s）却把发布链从 3 拉到 5，收益/代价比不成立。
2. **唯一新增的 feature 门是 `json`，且它的理由不是粒度而是依赖隔离**：
   `serde_json` 转 optional，`json = ["dep:serde_json"]`，`json` 域默认**不**进树。
   这一条**不改**「十三域零门常驻」——被门化的只有唯一带三方依赖的那个域。
3. **「零三方」对外口径改为可证伪的三层表述**（默认树层，不再是「crate 全域零三方」）：
   - `skeleton`：**零依赖**（不变，`[dependencies]` 段不存在）。
   - `std-ext` **默认树**：仅 `skeleton`（workspace 路径依赖）——**零三方**。
   - `skeleton-infra` **默认树**：仅 `skeleton` + `std-ext`（均 workspace 路径依赖）——**零三方**。
   - 开 `json` 门时 `serde_json` 子树按需进树（**明示例外**，不再假装不存在）。
4. **`json` 域的门化不豁免「十三域零门常驻」契约的其余部分**；`json` 域自此是「十四域中唯一门控域」。
   「十四域零门常驻」这一表述本身作废，改为「十四域：十三域零门常驻 + `json` 域门控」。

---

## 4. 落地清单（bd 单 `udf-frame-zex`，波 2 执行）

### 4.1 前置事实

- 三 crate 均**未发布**：crates.io API 实测 `udf-skeleton` / `udf-std-ext` / `udf-skeleton-infra` 全 404
  （2026-10-27）→ 本变更**无外部兼容负担**，SemVer major 条款不触发。
- 仓库**未提交**状态：`docs/adr/0004-std-ext-split.md` 正文**不动**（历史 ADR），口径修正走本 ADR +
  **PM 在 ADR 0004 加指针注释**（指向本 ADR 与本节落地清单）。

### 4.2 源码改动（最小集，共 5 处）

| # | 文件 | 改动 |
|---|---|---|
| 1 | `std-ext/Cargo.toml` | `[features]` 增 `json = ["dep:serde_json"]`（用 `dep:` 前缀，门名不暴露为 `serde_json`）；`serde_json = { version = "1", optional = true }` |
| 2 | `std-ext/src/lib.rs` | `pub mod json;` → `#[cfg(feature = "json")] pub mod json;`；域表 `json` 行由「常驻，零 feature」改为「门控 `json`」；feature 指南段「默认零 feature 全开；`--no-default-features` 同面」需改为「默认面 = 十三域零门常驻（`json` 域需显式开门）」 |
| 3 | `std-ext/tests/json.rs` | 加 `#![cfg(feature = "json")]`（否则门关时该测试目标编译失败） |
| 4 | `std-ext/README.md` | 依赖纪律段与域清单同步 |
| 5 | `skeleton-infra/Cargo.toml` | **仅当 ADR 0006 的 `view` feature 落地时**：其 `view` 门需 `["dep:serde_json", "udf-std-ext/json"]`（跨 crate 开门机制已实证：scratch 中 `view-json = ["dep:serde_json", "udf-std-ext/json"]` → 开则 `serde_json` 进 infra 树、关则不进） |

**可选（未列入必须）**：`std-ext/src/json.rs` 内 `pub use serde_json;` 再导出——便于 `json` 使用者
直接命名 `Value` 而不必在自己 manifest 里再写一条 `serde_json`。会扩大 std-ext 公开 API 面，
交 PM 定。

### 4.3 文档同步（三处同措辞 + ADR 0004 指针）

`README.md`（crate 表 std-ext 行 / feature 矩阵「常驻，零 feature」行 / `cargo tree` 段 / 「只要散件」段）、
`docs/reference.md`（§零依赖口径 / §feature 矩阵 std-ext 行 / §域索引表 / `## json` 章节的「feature」行）、
`std-ext/README.md`——**统一终态措辞**：

> `std-ext` 十四域：**十三域零门常驻**（本体纯 std）+ **`json` 域门控**（`json` feature，引 `serde_json`）。
> 默认树（`cargo tree -p udf-std-ext -e normal`）仅 `skeleton`（workspace 路径依赖）——**零三方**；
> 开 `json` 门时 `serde_json` 子树按需进树。

`skeleton-infra` 侧同款限定（默认树零三方；生态三方经各自 feature 门控：`journal`/`config-json`→serde+serde_json、
`config-toml`→toml、`log`→log+tracing 四件、`tokio`→tokio、`tracing`→tracing+subscriber）。

`docs/adr/0004-std-ext-split.md`：**正文不改**，PM 加一行指针注释指向本 ADR（口径修正的权威文本在本 ADR）。

### 4.4 CI 闸（必做，否则门化即造新空心门）

`std-ext` 现有 CI 步是 `cargo test -p udf-std-ext -- --skip anchors --skip verbatim`（**默认 feature**）。
`json` 门化后，该步将**静默跳过 `json` 域 9 条断言**（实测：无门时该测试目标 `0 tests` 且 exit 0）——
这正是审计 F-11 记的「空心门」形态。落地须把 `std-ext` 测试步改为
`cargo test -p udf-std-ext --all-features`（实测 all-features 全绿：385 条，含 `json` 9 条 + `log-embed` 35 条）。

**顺带闭合一个既有空心门（本次实测新发现，非审计 F-11 的 13 条）**：CI 的 `std-ext` 步跑默认 feature，
而 `std-ext/tests/log_embed.rs` 整文件 `#![cfg(feature = "log-embed")]` 门控 ⇒
**35 条 `log-embed` 埋点断言（backoff 1 / fsx 6 / iter 2 / scheduler 11 / ratelimit 4 / retry 6 / cache 5）
在 CI 中从未执行**（实测 `cargo test -p udf-std-ext -- --list` = 350，`--features log-embed` = 385，差 35，
逐条名核对全部属 `log_embed` 文件）。本地 all-features 实跑全绿（35 passed）⇒ 门本身不是坏的，是 CI 没开。
故本节的 `--all-features` 一并闭合此洞。

### 4.5 复评触发条件（聚合决策非教条）

出现任一条即重开粒度决策：
1. `std-ext` 冷编译时长超过采纳方机器 10 s 量级；
2. 某域需独立破坏性演进而其余域已在生产使用（版本号共享开始产生实际阻塞）；
3. 发布节奏出现分叉诉求（三 crate 同版锁定的 `PUBLISHING.md §0` 策略开始别扭）。

---

## 5. 后果

**正向**

- `std-ext` 与 `skeleton-infra` 的默认树**双双恢复零三方**，且是 `cargo tree` 一行可验的事实
  （scratch 实测：门化后 `cargo tree -p udf-std-ext -e normal` = 仅 `udf-skeleton`；
  `cargo tree -p udf-skeleton-infra -e normal` = 仅 `skeleton` + `std-ext`）。
- 十三纯 std 域使用者冷编译省 ≈ 4.4 s、供应链面少 5 crate（`serde_json`+`serde_core`+`itoa`+`memchr`+`zmij`）。
- 与仓库既有纪律同形：`serde_json` 在 infra 的 `journal`/`config-json` 门后、`tracing` 在 std-ext 的
  `log-embed` 门后——本 ADR 消灭最后一个例外。
- 审计 F-15 的「二进制膨胀」断言被实测证伪并留档（null 探针与无依赖基线逐字节相同），后续不再有人
  据此主张拆 crate。

**破坏性（无 shim 直迁）**

| 变更 | 迁移 |
|---|---|
| `std_ext::json` 域默认不可用 | 使用者加 `features = ["json"]`（`udf-std-ext = { version = "0.1.0", features = ["json"] }`）；0.1.0 未发布故无外部采用方迁移面 |
| 「十四域零门常驻」表述作废 | 改为「十三域零门常驻 + `json` 域门控」 |
| `std-ext` 默认树不再含 `serde_json` | 需 `serde_json` 类型的使用者经 `json` 门按需取；跨 crate 使用者（如 ADR 0006 的 `view` feature）按 §4.2 第 5 项开门 |

**遗留 / 交回 PM**

- §4.2 的 `pub use serde_json;` 可选项（扩大公开 API 面 vs 使用者便利）未决。
- `docs/adr/0006`（视图桥）尚未落盘；其 `view` feature 的 `serde_json` 依赖形态须与本 ADR 的
  `json` 门协同（§4.2 第 5 项），落 ADR 0006 时需回读本节。
- `CHANGELOG.md` 需记一条破坏性变更（`json` 域门化 + 口径修正）。

---

## 6. 验证锚

| 断言 | 命令 / 位置 | 实测 |
|---|---|---|
| 门化后 std-ext 默认树零三方 | `cargo tree -p udf-std-ext -e normal` | 仅 `udf-skeleton`（scratch 实测） |
| 门化后 infra 默认树零三方 | `cargo tree -p udf-skeleton-infra -e normal` | 仅 `skeleton` + `std-ext`（scratch 实测） |
| 开 `json` 门时 serde_json 按需进树 | `cargo tree -p udf-std-ext -e normal --features json` | `serde_json`→`itoa/memchr/serde_core/zmij`（scratch 实测） |
| 跨 crate 开门可用 | infra `view-json = ["dep:serde_json", "udf-std-ext/json"]` | 开则进树、关则不进（scratch 实测） |
| 未用域不进二进制 | null 探针 vs 无依赖基线 | 125,440 = 125,440（逐字节相同） |
| `json` 域测试在门关闭时静默为 0 | `cargo test -p udf-std-ext --test json`（无门） | `0 tests`，exit 0 ⇒ 必须配 all-features CI 步 |
| all-features 全绿 | `cargo test -p udf-std-ext --all-features` | 385 条，0 failed（本地实测） |
| 三 crate 未发布 | crates.io API | 三个全 404（2026-10-27） |
