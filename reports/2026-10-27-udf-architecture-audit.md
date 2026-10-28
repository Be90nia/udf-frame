# udf-frame 架构设计合理性审计

日期：2026-10-27 · 审计员：ArchAudit（只读审计，未改任何源码）
审计对象：`E:/Projcet/udf-frame`（Rust workspace，三 crate：`skeleton` 契约层 / `std-ext` 散件层 / `skeleton-infra` 实现层）

---

## 0. 测量口径声明

本审计测的是**结果层**（架构设计对采用方与维护者产生的实际后果），不是曝光/检索/激活层。

具体口径：

- **可观察证据口径**：每条 finding 挂 `file:line` 或命令输出；架构层判断凡能跑的一律跑（编译期断言用一次性 scratch crate 验证，跑完即删）。
- **不适用项**：本审计不涉及 prompt/模型输出优化，故 pm-audit 的「配对不一致计数（discordance）」与「任务级劣化份额（degradation）」两个量**不适用**——审计对象是代码架构不是 prompt，无基线/优化版两臂可比。
- **对外比较口径**：外部对标只用于反证「业界都这么做」式断言，外部结论必须回写差异，不作结论依据单方采用。

**深度闸自查（本次要查的面 ≥6 条，逐条作答见 §2）**：

| # | 要查的面 | 结论位置 |
|---|---|---|
| 1 | 分层与依赖方向合理性 | F-02 / F-08 |
| 2 | 契约面 trait 形状与约束强度 | F-03 / F-04 / F-05 |
| 3 | 状态模型（值语义快照 + Clone 语义 + Arc 快照） | F-05 / F-06 |
| 4 | 效果模型（Effect 三形 + EffectId + EffectBuf + 解释器边界） | F-07 / F-08 |
| 5 | 组合模型（combine 12 参 + 闭包路由）表达力与可错性 | F-03 / F-04 |
| 6 | 状态机通知与锁模型（通知在 dispatch 线程同步执行、单锁状态、快照 Arc 分配） | F-05 / F-06 / F-09 |
| 7 | 采用面 DX 负担（真实采用方要写多少代码、哪些坑必须自己踩） | F-04 / F-09 / F-10 |
| 8 | 公开契约演化与兼容策略（版本化、序列化格式稳定性） | F-07 / F-11 |
| 9 | 文档与实现一致性（README/crate doc/ADR 声称 vs 代码实际） | F-01 / F-02 / F-12 |
| 10 | 三 crate 边界（std-ext 聚合 crate 粒度、版本锁定、编译成本） | F-02 / F-13 |
| 11 | feature 门组合爆炸与未测组合 | F-11 / F-14 |
| 12 | 失败模式与可观测性（静默失败面） | F-03 / F-04 / F-09 / F-10 |

中途换问题留痕：原计划查「journal 账本与 store 的双 trait 是否构成自造第二套账本」，取证后发现两 trait 词表已分离（`journal.rs:566` 实现层 `JournalPort` 与 `Journal` 方法名不重叠，`docs/adr/0005-journal-reshape.md` 已拍板），**不是** finding，改为查「契约层 `Effect`/`Restore` 枚举缺 `#[non_exhaustive]` 的演化风险」→ F-07。

---

## 1. 审计结论

**VERDICT: CONDITIONAL**（第 1 关 代码审计 FAIL、第 2 关 PM 端到端验证 FAIL、第 3 关测试 Agent 独立跑 CONDITIONAL、第 4 关架构审计 FAIL、第 5 关设计审计 CONDITIONAL —— 最严胜出 = FAIL，但本审计本身不是交付验收，见下）

**不合理点计数：13 条**（blocker 3 / 建议 7 / 吹毛 3）

**Top3（按影响面排序）**：

1. **F-01｜blocker｜零三方承诺已被自己打破，但 README/ADR/reference 全部还在说零三方。** `std-ext/Cargo.toml:44` 的 `serde_json = "1"` 是**普通依赖（零 feature 常开）**，`cargo tree -p udf-std-ext -e normal` 实测输出 `serde_json v1.0.154 → itoa/memchr/serde_core/zmij` 五节点；而 `README.md:16`、`README.md:75`、`README.md:77`、`docs/reference.md:9`、`docs/adr/0004-std-ext-split.md:80` 全部仍写「仅 skeleton 路径依赖，零三方」「`cargo tree -p udf-std-ext -e normal` 仅 skeleton，零三方」。这是**对外销售话术与实际依赖树直接矛盾**——采用方按文档做「零三方可审计」判断会误判。三处口径要改：README×3、reference.md、ADR 0004。
2. **F-02｜blocker｜std-ext 域清单三处不一致，json 域在两个权威文档里整域缺失。** `std-ext/src/lib.rs:3` 自称「十四域」，`std-ext/README.md:3` 也写「十四个」并列出 json；但 `README.md:16` 与 `README.md:73` 仍写「十三个纯 std 常驻域」且清单里没有 json；`docs/reference.md:48-66` 的域索引表**完全没有 json 行**，`docs/reference.md` 全文 grep `std_ext::json` 零命中（`json` 域唯一入口 `std-ext/src/json.rs` 无任何 reference 文档章节）。后果：`docs/reference.md` 被 README 自称「权威口径」，但它漏了一整个域——采用方按 reference 检索找不到 json 域的 API。
3. **F-03｜blocker｜`combine` 的「分片不是根」靠运行期 panic 兜，而不是类型层拦——契约层自相矛盾。** 一次性 scratch crate 实测：`SyncStore::new(combine_frag(), NoFx)` **编译通过**，`initial()` 运行时 panic「combine 产物是状态分片不是根」。而 `skeleton/src/lib.rs:86-88` 的 `Reducer` 契约明写「非法状态进类型」「`State` 禁内部可变性」，`skeleton/src/lib.rs:99` 明写 reducer「纯函数」。框架整体靠**类型层**消灭非法状态（`Restore` 三态、`EffectBuf`、闭包路由），偏偏最容易被误用的组合入口用 panic 兜——采用方把 `combine` 产物直接塞进 `SyncStore::new` 是最自然的写法，编译期零信号、运行期炸。

---

## 2. 七查结果（架构审计口径）

> 本节是「架构/设计层合理性」专项，映射 pm-audit 的 12 维到架构关注点。逐条可观察问题已列在 §0 深度闸表。

**正确性[✗]** — `combine` 分片作根编译通过（§3 F-03）；`combine` 路由闭包返回 `None` 时**静默零效果零状态变更**且无任何计数（§3 F-04，scratch 实测 `FINDING E`：`Touch` 被路由到分片后被 `unwrap` 返回 `None` 吞掉，最终状态 `Parent { total: 100, child: 7 }`——`Touch` 那次 dispatch 完全蒸发，无错误、无计数、无日志）。

**边界[✗]** — `EffectBuf::pop_front` 是 `Vec::remove(0)`（`skeleton/src/lib.rs:177-183`），每 pop 一次 O(n) 搬移，drain 一轮 O(n²)。scratch 实测 `FINDING D`：20000 条效果 pop 完 53.47ms（2.673µs/条）。采用方若在一个 reducer 里产 100 条效果，drain 成本比线性高一个量级。框架自己在 `skeleton/src/lib.rs:176` 写了「容量语义与 `Vec::remove(0)` 同」——即明知。

**错误处理[✓/✗ 混合]** — 失败模式层做得很好（`runtime.rs:38-46` 毒锁 fail-fast、`runtime.rs:1147-1160` 订阅回调 panic 摘除、`runtime.rs:1085-1096` 解释器 panic 计数上浮、`runtime.rs:993-1012` 回灌预算超限计数留痕、四条 panic 路径都有独立读侧计数）。但**静默面**有四处（§3 F-04 combine 路由 None、F-09 `subscribe_local` 跨线程不触发、F-10 闭门后 `subscribe` 登记永无通知、F-13 审计单槽被二次订阅覆盖）——每处都有对应的**计数读侧**（这是本框架的强项），但计数**不是错误**：采用方不主动查计数器就看不出发生过。

**测试质量[✗]** — CI 的 `std-ext` 测试步用 `--skip anchors --skip verbatim` 全局跳过（`.github/workflows/ci.yml:44-45`）。实测这**两个字面量匹配**把 17 个测试排除，其中 **13 个在 CI 的任何一步都不跑**（`hash::tests::xxh64_published_anchors` —— XXH64 spec v0.2.0 官方向量的兼容性锚、`paths::tests::*` 9 个 Windows verbatim 路径语义测试、`fsx::tests::walk_traverses_beyond_260_via_verbatim_root`、`paths_io` 两个、`scheduler` 两个）。我逐个显式跑过：**13 个全绿**（`cargo test -p udf-std-ext -- --exact <13 names>` → 9+1+2+2 passed / 0 failed）。即：这些测试没坏，是 CI 门把它们藏了。`paths_alloc.rs` 的 3 个在 CI 的独立串行步里跑（ci.yml:60-61），实际只有 `hash`/`paths`/`fsx`/`paths_io`/`scheduler` 这 10 个是**真·永不执行**。

**接线[✓]** — 三 crate 依赖方向单一无环：`skeleton` ← `std-ext` ← `skeleton-infra`，`cargo tree -p udf-skeleton-infra -e normal` 实测只出这三个节点 + serde_json 子树。骨架核心无 IO 依赖（`skeleton/Cargo.toml` 无 `[dependencies]` 段），reducer「禁 IO」确为编译期事实。

**复用面[✗]** — `skeleton-infra/src/text.rs`（150 行）与 `skeleton-infra/src/backoff.rs`（68 行）是**孤儿文件**：两者都未被 `skeleton-infra/src/lib.rs` 的任何 `mod` 声明引用（`lib.rs:72-109` 全量 `pub mod` 清单里没有 `text`/`backoff`），`cargo doc` 产物 `target/doc/skeleton_infra/` 目录列表里也无 `text`/`backoff`（实测：audit compose config fx journal logx memgov parse_mock pool pooled_thread_pool runtime shutdown testing tracingx）。`git ls-files` 显示两文件仍被跟踪。内容与 `std-ext/src/backoff.rs` 逐字重复（同一 `backoff_delay` 函数、同样的 4 个测试），ADR 0004 声称「破坏性变更清单：infra 根再导出已删除」——文件删了，但**源码副本留在原地不删**，doc 注释还写着 `use skeleton_infra::backoff::backoff_delay;`（`backoff.rs:17`）这种已不存在的路径。

**外部对标[✓]** — 见 §4。

---

## 3. 不合理点清单（按影响面排序）

严重度：blocker = 必修 / 建议 = 应修 / 吹毛 = 可不改

### F-01｜blocker｜「零三方」承诺在 `std-ext` 已被打破，文档未跟

- **现象**：`std-ext` 的 `json` 域引 `serde_json = "1"` 为普通依赖（零 feature 常开），但全部对外文档仍称 `std-ext` 零三方。
- **证据**：
  - `std-ext/Cargo.toml:44` → `serde_json = "1"`（在 `[dependencies]` 段，非 optional）
  - `cargo tree -p udf-std-ext -e normal` 实测输出：
    ```
    udf-std-ext v0.1.0
    ├── serde_json v1.0.154
    │   ├── itoa v1.0.18
    │   ├── memchr v2.8.3
    │   ├── serde_core v1.0.229
    │   └── zmij v1.0.23
    └── udf-skeleton v0.1.0
    ```
  - 文档仍称零三方：`README.md:16`「仅 skeleton 路径依赖，零三方」、`README.md:75`「`cargo tree -p udf-std-ext -e normal` 仅 skeleton，零三方」、`README.md:77`「纯 std 零三方」、`docs/reference.md:9`「`cargo tree -p udf-std-ext -e normal`：仅 `skeleton`（workspace 路径依赖），零三方」、`docs/adr/0004-std-ext-split.md:80`「**std-ext 零三方口径**：六域本体纯 std」
  - `std-ext/Cargo.toml:17-21` 的注释自己承认了（「`json` 域引 serde_json 普通依赖（零 feature 常开，自主仓 xray-conf 上浮）」），但 P1-9 的拍板记录没进 `docs/adr/`（`docs/adr/` 下 5 篇，无一篇提 P1-9 / json 上浮）。
- **为什么不合理**：零三方不是实现细节而是**对外承诺 + 架构判据**——ADR 0004 的整条决策链（「feature 门解决不了域可独立取用」「物理拆分迫使依赖网显式化」）建立在「std-ext 零三方」这个可被 `cargo tree` 一行验证的不变量上。承诺破了而文档不改，采用方的技术选型评审（空气隔离 / 供应链审计 / 编译成本）会基于错误前提做决策。`skeleton-infra` 的默认树也同步被污染（`cargo tree -p udf-skeleton-infra -e normal` 实测含 serde_json 子树），`README.md:75` 的 infra 侧「零三方」同样失真。
- **替代方案**（≥1）：
  - A（推荐，代价小）：`json` 域降为 `json = ["dep:serde_json"]` feature 门，恢复默认树零三方；`json` 域体量 153 行，独立成门无采用面损失。同步改 `README.md:16/73/75/77`、`docs/reference.md:9` 口径措辞为「默认树零三方，`json` 域例外」。
  - B：把 `json` 域整体移出 `std-ext`，回 `skeleton-infra`（绑 `config-json` 门）——`json` 域是「格式解析行为件」，与 `config-json`/`journal` 同族；`std-ext` 恢复纯 std 不变式。代价：`std-ext/src/json.rs` 迁位 + `Cargo.toml` 依赖迁移。
  - C：接受现实，全面改文档口径（承认 `std-ext` 有 serde_json），但**必须**同时补一篇 ADR 记录 P1-9 的依赖判据与代价——当前 P1-9 零 ADR 记录。
- **能否框架层踩平**：**能**。A 方案是一次 manifest 改动 + 文档同步，框架层自己踩完，采用方零负担。
- **影响面 / 踩平难度**：影响面 = 全部对外材料 + `std-ext`/`skeleton-infra` 两个 crate 的采用方技术评审；踩平难度 = 低（改 `Cargo.toml` 一行 + 5 处文档）。

### F-02｜blocker｜`std-ext` 域清单三处不一致，`docs/reference.md` 整域缺失 json

- **现象**：json 域在 `std-ext` 自己的文档里存在，在框架对外文档里不存在或数量对不上。
- **证据**：
  - `std-ext/src/lib.rs:3` → 「**十四域**零 feature 门常驻可用（默认树：十三域纯 std；`json` 域引 serde_json 普通依赖——P1-9 拍板，自主仓 xray-conf 上浮）」
  - `std-ext/README.md:3` → 「常驻叶子域（P1-9 起**十四个**）：`text` / `encode` / `hash` / `iter` / `backoff` / `fsx` / `paths` / `clock`（含 `clock::rfc3339` 行为件）/ `retry` / `scheduler` / `ratelimit` / `cache` / `id` / **`json`**」
  - `README.md:16` → 「**十三个**纯 std 常驻域（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`）」——无 json
  - `README.md:73` → 「**std-ext 十三域**（`text` `encode` `hash` `iter` `backoff` `fsx` `paths` `clock` `retry` `scheduler` `ratelimit` `cache` `id`）」——无 json
  - `docs/reference.md:5` → 「承载**十三个**纯 std 常驻散件域」——无 json
  - `docs/reference.md:48-66` 域索引表：13 个 std-ext 域行，**无 json 行**
  - `docs/reference.md` 全文 grep `std_ext::json` / `### json` → **零命中**
  - `std-ext/src/json.rs` 全文存在（153 行，含 `JsonMode`/`JsonParseError`/`MAX_INPUT_BYTES`/`decode`/`decode_str`/`decode_slice`）
- **为什么不合理**：`README.md:1` 与 `docs/reference.md:3` 都自称权威口径（「权威口径：本文从 `src/*.rs` doc 注释、`tests/` 行为面与 `Cargo.toml` feature 表提取」），但两者对同一 crate 的域清单给出两个不同答案，reference 还漏了一整域。采用方按 reference 索引检索 json 域 API 会**完全找不到**——而 json 域是 `std-ext` 里唯一带输入上限（64MiB fail-visible）与 JSONC 容忍模式的域，这两条正是「防坑」价值所在。
- **替代方案**：
  - A（推荐）：`docs/reference.md` 补 json 域章节 + 域索引行（格式照 `### hash -- 稳定摘要` 的既有体例），`README.md:16/73` 与 `docs/reference.md:5` 改「十四域」并补 json；同步 F-01 的口径修正。
  - B：把 json 域从 `std-ext` 移出（F-01 方案 B），则数量口径自然回到十三——但 reference 仍需补 json 章节（域搬到哪都要文档）。
- **能否框架层踩平**：**能**，纯文档工作，框架层自己踩完。
- **影响面 / 踩平难度**：影响面 = 全部采用方检索路径 + 域数量对账（ADR 0004 的「终版十三域」表也要更新）；踩平难度 = 低。

### F-03｜blocker｜`combine` 产物作根 Reducer 编译通过、运行期 panic —— 违反本框架自述的「非法状态进类型」原则

- **现象**：`combine` 的产物实现 `Reducer`（含 `initial()`），`initial()` 是 `panic!`；把它塞进 `SyncStore::new` 编译无警告、运行期炸。
- **证据**：
  - `skeleton-infra/src/compose.rs:288-294`：
    ```rust
    #[track_caller]
    fn initial(&self) -> PS {
        panic!("combine 产物是状态分片不是根：根状态由父 Feature 的 initial 显式构造（含子切片初值），分片只承载 reduce 委托")
    }
    ```
  - `skeleton-infra/src/compose.rs:124-126` 文档自称「**产物是状态分片不是根**……用作根 = 组合根误用，**fail-closed 显式崩溃**（禁静默空状态）」——即**有意为之**的 panic 兜底。
  - scratch crate 实测（`FINDING A`）：`SyncStore::<Parent, PAction, Infallible>::new(combine_frag(), NoFx)` **编译通过**；运行输出 `panicked at src/main.rs:30:58: combine 产物是状态分片不是根` + `FINDING A: combine-as-root compiles; runtime panic = true`。
  - 框架自述的原则：`skeleton/src/lib.rs:86-88`「`State`：值语义只读快照的形状约束。契约：**禁内部可变性**（无 Mutex/Cell 字段）、**非法状态进类型**（互斥态用嵌套 enum，禁 bool 旗子）」。
- **为什么不合理**：框架整体方法论是「**类型层消灭非法状态**」——`Restore` 三态而非双 `Option`、`PoolResizeError` 枚举而非 `Option`、`EffectBuf` 强制效果出参化、`Subscription` `#[must_use]` 拦裸调用、`ActionSender::dispatch` 返回 `bool` 而非静默丢弃。这些都是编译期信号。唯独最容易被误用的组合入口（`combine` 产物是 `impl Reducer`，与真根 Reducer 同类型，装配时无任何形状差别）用 panic 兜——**编译期零信号、运行期炸**，且 panic 发生在 `initial()` 即 `SyncStore::new` 内（`runtime.rs:556` `root.initial()`），采用方在组合根这一行看到的是库内 panic 位置（`#[track_caller]` 只把 `initial` 的 panic 指回调用行，不解决「为什么该类型不能当根」的问题）。
  - 更具体的踩坑路径：`combine!` 宏短形态 `skeleton_infra::combine!(ChildCounter, Parent.child, PAction::Child)` 产出的正是 `impl Reducer`，文档示例（`compose.rs:190-215`）里它被赋给 `fn counter_fragment() -> impl Reducer<...>`——**签名上看不出它不能当根**。采用方把 `counter_fragment()` 直接传给 `SyncStore::new` 是零思考的自然动作。
- **替代方案**：
  - A（推荐）：分片不进 `Reducer` trait——另立 `Fragment` trait（只有 `reduce`，无 `initial`），`Combine` 只实现 `Fragment`；父 Feature 的 `reduce` 显式调分片（现有示例本来就是这么用的）。这样 `SyncStore::new(combine_frag(), ...)` **编译期 E0277**，类型层拦截。代价：现有 `combine` 返回类型签名改（`impl Reducer` → `impl Fragment`），需给 `Fragment` 定义一个 `reduce` 同形方法 + 父侧调用点迁移（`compose.rs` 文档示例、`tests/combine_teststore.rs`、教程 `thirty-min.md` 组合分片节）。属破坏性变更，但 0.1.0 未发布无兼容负担（ADR 0004 同款论证）。
  - B（轻量）：保留 `Reducer` 实现，但给 `Combine` 加一个**类型级标记**（如 `pub trait NotARoot {}` 的负向标记不可行，则改用 `SyncStore::new` 的第二形态 `new_fragment` 不提供——只加 `Reducer::is_root() -> bool` 默认 `true`，`Combine` 返 `false`，`SyncStore::new` 构造期断言）。仍为运行期，但至少 panic 信息可定制。
  - C（最小，仅文档）：保留 panic，把「combine 产物不可作根」写进 `README.md` 快起段 + `docs/tutorials/cheatsheet.md` 坑表。**只解决认知不解决编译期**——不建议单用。
- **能否框架层踩平**：**能**（A 方案框架层改 trait 边界，采用方零额外负担）。
- **影响面 / 踩平难度**：影响面 = 所有用 `compose` feature 的采用方 + 组合根装配这一最关键的一步；踩平难度 = 中（trait 边界重构 + 调用点迁移 + 测试改）。

### F-04｜建议｜`combine` 路由闭包返 `None` = 静默吞掉 action，零效果零状态变更零计数

- **现象**：`unwrap` 闭包返回 `None` 时，`Combine::reduce` 返回 `state.clone()`，该 action 完全蒸发，无任何可观察信号。
- **证据**：
  - `skeleton-infra/src/compose.rs:284-287`：
    ```rust
    fn reduce(&self, state: &PS, action: PA, out: &mut EffectBuf<PA, PO>) -> PS {
        let Some(child_action) = (self.unwrap)(&action) else {
            return state.clone();
        };
    ```
  - `skeleton-infra/src/compose.rs:20-22` 文档：「`unwrap` 返回 `None` = 本层不路由，状态克隆原样返回、零效果（父层分支自理）」
  - scratch 实测（`FINDING E`）：父 Feature 把 `PAction::Touch` 路由给分片，但分片的 `unwrap` 只认 `Child` → `Touch` 被吞。终态 `Parent { total: 100, child: 7 }`——`Touch` 那次 dispatch 的效果与状态变更**全部消失**，无 panic、无计数、无日志。
  - 对比：框架里其它「不处理」路径都有读侧。`runtime.rs:1075-1080` 的回灌预算超限有 `feedback_budget_exceeded()` 计数；`runtime.rs:809-816` 的闭门拒绝有 `rejected_after_close()` 计数。**唯独 combine 的路由 miss 什么也没有**。
- **为什么不合理**：路由 miss 与「本层不路由」在类型上不可区分（都返 `None`），但语义天差地别——前者是 bug（父层把 action 送错了地方），后者是设计（父层自己处理）。框架对这两类情形用同一个 `None` 表达，且**对 bug 那侧完全静默**。对照 Elm：`update` 收到不认识的 msg 是穷尽 match 强制暴露；对照 Redux：`combineReducers` 会对未知 action 调每个 slice reducer（`combineReducers.ts` 实证，见 §4）——两者都不给「静默吞」留位置。
- **替代方案**：
  - A（推荐）：`unwrap` 返 `Option<CA>` 保留，但 `Combine` 增一个**路由 miss 计数读侧**（`Combine` 内 `AtomicU64`）+ `#[track_caller]` 的 panic 选项（如 `combine_strict` 变体，miss 即 panic 带父 action 的 Debug）。零新概念、与框架既有「计数读侧」纪律一致。
  - B：路由闭包返 `Result<CA, Unrouted>`，`Unrouted` 分支由调用方决定（父层自理 vs panic）——类型层表达「不路由」与「路由失败」的区分。
  - C：抄 Redux 语义——父 action 广播给所有分片，各分片自行判断是否处理（`unwrap` 返 `Option` 但 miss 不是错误而是「我不处理」）。**这会改组合模型语义**，代价大，仅记录。
- **能否框架层踩平**：**能**（A 方案在 `Combine` 内加计数 + 读侧，框架层踩完）。
- **影响面 / 踩平难度**：影响面 = 多分片组合的采用方（`compose` feature 用户）+ 组合根正确性；踩平难度 = 低。

### F-05｜建议｜`Arc<State>` 快照的 CoW 语义在「有外部持有者」时退化为每次 dispatch 全量深克隆，采用方零感知

- **现象**：`place_snapshot` 用 `Arc::make_mut`，外部持有快照时走克隆分支；采用方按「快照是廉价引用」的直觉写代码，代价是每次 dispatch 深克隆整个 State。
- **证据**：
  - `skeleton-infra/src/runtime.rs:1104-1114`：`std::mem::replace(Arc::make_mut(&mut slot), next_val)` + 文档「有外部持有者时自动克隆新分配、旧分配冻结」
  - `skeleton/src/lib.rs:141` 契约文档：「**`S::Clone` 必须 panic-free**：外部持有快照时状态落位走 `Arc::make_mut` 克隆分支，`S::clone` 在 state 锁内执行——clone panic 即毒锁，snapshot/dispatch 从此恒 panic，**实例整体报废且状态不可取出**」
  - scratch 实测（clone 计数，`FINDING` 克隆计数段）：
    ```
    clones per 100 (poll snapshot + dispatch) = 200
    clones per 100 (dispatch only)            = 100
    ```
    即：每次「读一次快照再 dispatch」= **2 次 State 深克隆**（reducer 自身 1 次 + `make_mut` 克隆分支 1 次）。读快照这个动作本身会让**下一次** dispatch 多付一次全量克隆。
  - 深度克隆的成本实测（1 MiB State，`Vec<[u64;32]>` × 4096）：**210µs/dispatch**（release）。对照：`README.md` 自述 dispatch 锚 ~80ns——**差 2600 倍**。
- **为什么不合理**：三点叠加。
  1. **clone 在 state 锁内**（`runtime.rs:1108` `lock(&inner.state)` 包裹 `Arc::make_mut`），深克隆的耗时直接进入临界区，锁持有时间随 State 体量线性增长——并发 dispatch 场景下这变成全局串行点。
  2. **panic 即毒锁即实例报废**：采用方在自己 State 的 `Clone` 里写了个会 panic 的逻辑（罕见但存在，如 `Clone` 里调了会失败的 IO），整个 store 永久报废且状态取不出来——契约文档写了，但没有任何类型层或运行期防护。
  3. **采用方零感知**：`Arc<State>` 的类型签名不提示「持有快照会使下次写入变慢」。文档只在契约注释里提了一句 panic 风险，**没有提性能代价**。`docs/reference.md` 全文 grep「克隆」无性能代价的说明。
  - 补充证据（另一个侧面）：`Arc` 包裹**不解决**这个问题——scratch 实测 `Arc`-wrapped State 与深克隆 State 均为 ~205-210µs/dispatch（比率 1.0x），因为 reducer 内部照样要克隆整个 `Vec`。即：**「用 Arc 装 State」这条常见规避路径在本框架无效**，而框架文档未提示这一点。
- **替代方案**：
  - A（推荐，成本最低）：在 `Store` 契约与 `docs/reference.md` 明确写出「快照持有者存活期间，每次 dispatch 额外付一次 `S::clone`」这一成本模型，并给出 State 体量指引（如「State 建议 ≤ 数 KiB；大状态用分片 + 自选持久化结构」）。纯文档，零代码风险。
  - B：`Reducer` 增一个可选的 `type State` 关联形态允许返回 `Arc<S>`（`Reducer` 的 `State` 已是关联类型，采用方可自己写 `type State = Arc<MyState>`，但那样 `Snapshot` 语义与 `Clone` 约束都要重谈）——本质是把「快照 vs 状态」的区分权交给采用方，代价是契约分叉。
  - C：把 `Arc::make_mut` 的克隆移出 state 锁（先在锁外克隆再入锁），消除「长临界区 + 毒锁」耦合。改动面大（`place_snapshot` 与 `DrainExclusive` 纪律耦合），代价中等。
- **能否框架层踩平**：**部分能**——A 方案（文档 + 指引）能踩平认知；C 方案能踩平锁竞争；「State 体量上限」本身是采用方的领域选择，框架无法代踩。
- **影响面 / 踩平难度**：影响面 = 所有采用方（State 体量与持有快照习惯）+ 并发性能；踩平难度 = 低（文档）/ 中（C 方案改锁序）。

### F-06｜建议｜通知在 dispatch 线程同步执行 → 慢订阅者阻塞整个状态机，无异步通知面

- **现象**：订阅回调在 dispatch 线程内同步执行（本地订阅与跨线程订阅皆然），回调耗时直接计入 dispatch 延迟。
- **证据**：
  - `skeleton/src/lib.rs:412` 契约：「**回调在 dispatch 线程同步执行**」
  - `skeleton-infra/src/runtime.rs:1168-1196` `notify_snapshot_consumers`：在 drain 循环内、`forward_effects` 之后同步调用 `notify(next)`（catch_unwind 包裹）
  - `skeleton-infra/src/runtime.rs:1085-1096` 本地订阅同样在 drain 内同步调
  - `docs/reference.md:1790+` 的性能锚：订阅态 dispatch 全含 ~142ns vs 无订阅 ~77ns——该数字只覆盖**订阅者本身廉价**的情形；订阅者若做重活（渲染、IO），142ns 完全不成立，且**无任何异步/延迟通知面**可退。
  - 无 `subscribe_async` / `notify_offload` 之类的面：`grep subscribe.*async|offload` 在 `skeleton/src` 与 `skeleton-infra/src` 零命中。
- **为什么不合理**：UDF 框架的典型采用方是 UI/服务混合栈——渲染或网络回写天然是重活。Elm 的做法是 runtime 在 update 后跑 view（同步但与状态更新分离，且 Elm 单线程不阻塞他人）；Redux 的做法是 middleware 在 dispatch 链上同步跑（同样阻塞，但 Redux 明确要求 middleware 轻量、副作用异步化）。udf-frame 的 `EffectRuntime` 契约把「异步」的责任推给解释器（`EffectRuntime::run` 收 `&ActionSender`，解释器可 spawn），这在**效果**方向解决了；但**订阅通知**方向没有对应出口——订阅者只能同步阻塞在被通知的 dispatch 线程上。要做「异步通知」，采用方只能自己再包一层（自己在订阅回调里 `tokio::spawn`，自己管生命周期）——这是框架本该踩平的坑。
- **替代方案**：
  - A（推荐）：增一个 `subscribe_deferred` 面——回调签名带一个 `ActionSender`/sink，框架在 drain 内只投递不执行（投递到 `std::sync::mpsc` 或框架自带的 worker），由宿主决定谁在何时消费。框架层即可踩平。
  - B：明确文档化「订阅回调必须廉价，重活走 `Effect::Run` 解释器」并在契约层写进 `Store::subscribe` 文档（当前只在 `skeleton/src/lib.rs:412` 一句话带过，未给规避路径）。
  - C：采纳方侧规避（自己在订阅回调里 spawn）——已有教程 `thirty-min.md:576-579` 用 `subscribe_local` + `Rc<RefCell>` 的本地档，但那是**本地**订阅，跨线程重活仍无出口。
- **能否框架层踩平**：**能**（A 方案框架层加投递面）。
- **影响面 / 踩平难度**：影响面 = 全部 UI/服务采用方的通知链；踩平难度 = 中（新增投递面 + 生命周期 + 停机序对齐）。

### F-07｜建议｜契约层 `Effect` / `Restore` 枚举缺 `#[non_exhaustive]`，加变体即破坏性变更

- **现象**：`skeleton/src/parse.rs` 的三个词表（`Json`/`MdTag`/`MdEvent`）都标了 `#[non_exhaustive]`，但最核心的 `Effect` 与 `Restore` 没有。
- **证据**：
  - `skeleton/src/parse.rs:41`、`:173`、`:234` → `#[non_exhaustive]`（三个）
  - `skeleton/src/lib.rs:121-122` → `#[derive(Debug)] pub enum Effect<A, Op = Infallible>`（**无** `#[non_exhaustive]`）
  - `skeleton/src/lib.rs:525-526` → `#[derive(Debug, Clone, PartialEq, Eq)] pub enum Restore<A, S>`（**无**）
  - `skeleton/src/parse.rs:24` 明确写了这条纪律：「`#[non_exhaustive]` 演进 | 词表按真实采用面渐进扩；加变体=非破坏演进（采用方 match 须带通配）」
  - 而 `Effect` 的采用方 match 全部是穷尽匹配——`skeleton-infra/examples/end_to_end.rs:172-180`（三臂穷尽）、`skeleton-infra/src/compose.rs:291-296`（三臂穷尽）
- **为什么不合理**：框架自己的演进纪律（「加变体=非破坏演进」）在 `parse.rs` 落实了，在核心 `Effect` 上没落实。`Effect` 是全框架使用频率最高的公开枚举（每个解释器都要 match 它），加一个新变体（如 `Effect::Delay`、或未来要加的第四形）会让**所有**解释器的穷尽 match 编译失败——正是 `parse.rs` 用 `#[non_exhaustive]` 规避的那类破坏。`Restore` 同理（采用方 match 三态）。这不是理论风险：`Effect` 已经过形态讨论（三形已是「三形」定案），再加一形的概率不低。
- **替代方案**：
  - A（推荐）：给 `Effect` 与 `Restore` 加 `#[non_exhaustive]`。代价：采用方 match 必须加通配臂——**但当前无发布版本**（0.1.0 未发布，`docs/PUBLISHING.md` 待发布），零兼容负担；且现有穷尽 match 点（examples/compose）需加通配臂。
  - B：给 `Effect` 加一个显式的 `Effect::Other` 兜底变体（不加 `non_exhaustive`）——效果近似但语义更弱（漏匹配静默进 `Other`，比编译失败差）。
  - C：不动，但在 `skeleton/src/lib.rs` 的 `Effect` 文档里显式登记「本枚举的加变体属破坏性变更，0.1.0 后按 semver 走」——**把风险写明而非消除**，可接受但弱于 A。
- **能否框架层踩平**：**能**（A 方案一行属性 + 迁移调用点）。
- **影响面 / 踩平难度**：影响面 = 全部解释器实现方（公开契约演化）；踩平难度 = 低。

### F-08｜建议｜`EffectId(pub u64)` 是裸元组，无命名空间/无生成纪律，Cancel 语义靠采用方自律

- **现象**：`EffectId` 是 `pub struct EffectId(pub u64)`——公开字段、无来源约束、无唯一性保证，`Effect::Cancel { id }` 的寻址完全靠 reducer 与解释器双方自律对齐。
- **证据**：
  - `skeleton/src/lib.rs:116-117` → `pub struct EffectId(pub u64);`（`pub` 字段）
  - `skeleton/src/lib.rs:112-115` 文档：「效果句柄：reducer 按语义指派（**同义恒同值**），解释器寻址用……值语义小句柄（Copy+Eq+Hash）」
  - 采用方实际用法（全是手搓计数器）：`skeleton-infra/examples/end_to_end.rs:80` → `id: EffectId(s.count + n)`；`skeleton-infra/benches/infra.rs:47` → `EffectId(s.0 + 1)`；`docs/tutorials/thirty-min.md:464` → `EffectId(id)`
  - `skeleton-infra/src/runtime.rs:1136-1141` 转发环：`Effect::Cancel { id }` 交解释器，**核心不校验 id 是否对应任何在途 Run**
- **为什么不合理**：`EffectId` 承担「取消在途效果」的寻址职责，但：
  1. 字段公开 → 任何整数都能造 id，无编译期约束；
  2. 无唯一性纪律文档 → 多个效果域（如 timer 与 http）各自从 0 计数会**撞 id**，撞了之后 `Cancel` 取消谁？框架不校验，解释器各写各的 → 取消错对象，且现象是「另一个效果莫名被取消」，极难定位；
  3. 「同义恒同值」是**纯约定**，无机制保证——约定型不变量在多作者采用方项目里必然漂移。
  对照 Elm：`Cmd`/`Sub` 无取消原语，根本不暴露这个问题（ Elm 用 `Task` 的进程内取消，但取消是**句柄**不是**裸 id**）。udf-frame 提供了取消原语却给的是裸数字，属「有能力无约束」。
- **替代方案**：
  - A（推荐）：`EffectId` 改私字段 + 提供构造器（`EffectId::new(u64)`）并在文档给出**唯一性纪律**（如「一个 store 内 id 全局唯一，reducer 侧自持单调计数器」）；或提供 `EffectIdGenerator` 辅助（框架已有 `std_ext::id` 域可复用形态）。
  - B：`Cancel` 携带**效果种类**（`Op`）一并校验：`Effect::Cancel { id, kind }`，核心/解释器可校验词表一致性，减少撞 id 的爆炸半径。
  - C：文档化「Cancel 语义由解释器自证，框架不保证」——**已在 `skeleton/src/lib.rs:195-200` 部分声明**（回灌环责任边界），但没说 id 撞车的后果。
- **能否框架层踩平**：**部分能**（A/B 方案框架层可做；唯一性纪律本质是采用方约定，框架只能给工具不能代守）。
- **影响面 / 踩平难度**：影响面 = 所有用 `Effect::Cancel` 的采用方（多效果域项目必撞）；踩平难度 = 低（A）/ 中（B）。

### F-09｜建议｜`subscribe_local` 跨线程 dispatch 时静默不触发，无任何可观察信号

- **现象**：本地订阅只在注册线程触发；其他线程 dispatch 时该订阅者**完全不收到通知**，且框架不给任何计数/信号。
- **证据**：
  - `skeleton-infra/src/runtime.rs:661-666` 文档：「**只在注册线程触发**：本地回调登记在注册线程的 thread-local 表，store 侧从不持有闭包。**他线程 dispatch 不触达本地订阅**」
  - scratch 实测（`FINDING B`）：线程 A 注册 `subscribe_local` 后，线程 B `dispatch(1)` → **hits = 0**（本地订阅者零触发）。框架未 panic、未计数、未警告。
  - `skeleton-infra/src/runtime.rs:723-733`：`subscriber_count()` **不计入**本地订阅（「口径：**仅计 `subscribe` 订阅**——`subscribe_local`（TLS 亲和档）不在此计数内」）
- **为什么不合理**：本地订阅是给「闭包体内可用 `Rc`/`RefCell`」的采用方用的（`runtime.rs:658-660`），典型场景是 UI 线程读状态。**但任何后台线程的 dispatch（包括所有 `Effect::Emit` 回灌——`runtime.rs:1123-1134` 的 Emit 是从**排空线程**入队的）都到不了 UI 线程的本地订阅**。采用方看到「订阅了、有票据、不报错」，实际收不到通知，且 `subscriber_count` 也不反映本地订阅数——**从外部完全不可观测**。
  - 精确的静默路径：`Effect::Emit` 回灌由**排空线程**执行（`runtime.rs:1123` `lock(&inner.queue).push_back`），若排空者是后台线程，则 UI 线程的本地订阅者收不到 Emit 派生动作引起的状态变更通知。这正是 UDF 最常见的用法（后台 IO → Emit → 状态变更 → UI 更新），而 UI 更新这一环在此**恰好断了**。
- **替代方案**：
  - A（推荐）：本地订阅在「非注册线程排空」时也投递——但 `Rc`/`RefCell` 不可跨线程，物理上不可行。故正确做法是**改语义**：本地订阅在注册线程上**排队**，由注册线程自己消费（框架提供 `LocalSubscription::poll()` 或让注册线程持一个本地 pump）。这样「注册线程未被调度 → 通知延迟」而非「通知丢失」。
  - B（最小）：加计数读侧 `local_missed_notifications()`（他线程排空时本地订阅未触发的次数），让静默变可观测——与框架既有「所有异常都有计数读侧」纪律一致。
  - C：文档化并在 `subscribe_local` 返回值上加提示（但返回类型是票据，加提示需改 API）。
- **能否框架层踩平**：**能**（B 方案框架层加计数；A 方案框架层加本地 pump）。
- **影响面 / 踩平难度**：影响面 = 全部用本地订阅的 UI 线程采用方（帧循环宿主，`skeleton/src/lib.rs:340-352` 明确把帧循环宿主列为典型形态）；踩平难度 = 低（B）/ 中（A）。

### F-10｜建议｜闭门后 `subscribe` 照常登记、永不触发——已知取舍但采用方零感知

- **现象**：`close()` 之后 `subscribe` 仍返回有效票据，登记计数 `subscriptions_after_close` 会涨，但订阅者永不收到通知。
- **证据**：
  - `skeleton/src/lib.rs:424-425` 契约表：「[`Self::subscribe`] | 照常登记、票据照常注销，但闭门后永无 dispatch 触发——通知静默（已知取舍，见下）」；`skeleton/src/lib.rs:428-431` 承认「通知静默可经实现方的计数读侧交叉查证」
  - `skeleton-infra/src/runtime.rs:691-696`：`subscribe` 闭门时 `subscriptions_after_close.fetch_add(1, ...)`，**仍然 `lock(&self.inner.subs).push(...)` 登记**
  - `skeleton-infra/src/runtime.rs:809-816`：闭门登记计数读侧
- **为什么不合理**：框架把它标为「已知取舍」并给了计数读侧（比静默丢弃好得多），但**计数读侧不是错误信号**——采用方在停机流程里若先 `close()` 再 `subscribe()`（顺序写反是常见笔误），会拿到一个「有效票据」，此后 UI 永远不更新，而除了主动查 `subscriptions_after_close()` 没有任何信号提示「你的订阅是死的」。
  - 对照：`ActionSender::dispatch` 闭门返 `false`（`skeleton/src/lib.rs:303-306`，`#[must_use]`），`FxScope::spawn` 闭门返 `Err(ScopeClosed)`（`fx.rs:288-296`），`ShutdownCoordinator::register` 闭门返 `Err(RegisterError::Closed)`（`shutdown.rs:60-62`）——**框架其它所有「已停机」入口都返回显式错误**。唯独 `subscribe` 是「成功返回一个永不触发的票据」。
- **替代方案**：
  - A（推荐）：`subscribe` 闭门时返回 `Result<Subscription<S>, StoreClosed>`（或 `Subscription` 携带一个「已闭门」态，`Subscription::is_live()` 可查）——与其余三个停机入口对齐。代价：签名变更（破坏性，但未发布）。
  - B：闭门时 `subscribe` 直接 panic（`#[track_caller]`）——与「fail-fast 纪律」一致（框架对 `combine` 作根就是 panic 兜，见 F-03），零签名变更。
  - C：保持现状但把 `subscriptions_after_close` 提升为 `subscribe` 闭门时的 **warn 级埋点**（框架 `log-embed` 机制现成）——信号进日志面而非 API 面。
- **能否框架层踩平**：**能**（A/B 方案框架层改）。
- **影响面 / 踩平难度**：影响面 = 停机序写错的采用方 + 所有 UI 宿主；踩平难度 = 低。

### F-11｜建议｜CI 的 `--skip anchors --skip verbatim` 静默屏蔽 10 个真实测试（含 XXH64 官方向量锚）

- **现象**：CI 为绕开 `paths_alloc.rs` 的分配计数污染，在 std-ext 测试步加了全局 `--skip anchors --skip verbatim` 两个**字面量**过滤，把不相关的测试一并屏蔽。
- **证据**：
  - `.github/workflows/ci.yml:44-45`：`cargo test -p udf-std-ext -- --skip anchors --skip verbatim`（注释说「paths_alloc 文件内事件计数器被 libtest 主线程分配污染……本步 --skip 隔离其测试」——但 `--skip` 是**按测试名**过滤，不是按文件）
  - 实测 `cargo test -p udf-std-ext -- --list` 共 **350** 个测试；`--skip anchors --skip verbatim` 命中 **17** 个，其中 **10 个在任何 CI 步都不执行**（`paths_alloc.rs` 的 4 个由 ci.yml:60-61 独立步覆盖，另 3 个 `every_anchors_*`/`every_huge_forward_jump_*` 在 `scheduler.rs`、**不在** `paths_alloc.rs`，也被跳过）：
    - `hash::tests::xxh64_published_anchors` —— **XXH64 spec v0.2.0 官方向量的兼容性锚**（`std-ext/src/hash.rs:411-414` 断言 `0xEF46DB3751D8E999` 等三个常量）
    - `paths::tests::keeps_verbatim_unc_and_device` / `keeps_verbatim_with_literal_dots` / `non_unicode_verbatim_kept` / `strips_verbatim_disk` / `unc_nonverbatim_normalizes` / `verbatim_passthrough_literal_dots` / `verbatim_path_relativizes_against_legacy_base` / `verbatim_with_dots_cannot_relativize_silently`（8 个 Windows verbatim 路径语义测试）
    - `paths::tests::walk_traverses_beyond_260_via_verbatim_root`（`std-ext/tests/fsx.rs`）
    - `paths_io::canonicalize_absolute_and_nonverbatim` / `verbatim_base_containment_f19_end_to_end`
    - `scheduler::every_anchors_to_plan_without_drift` / `every_huge_forward_jump_fires_once_and_reanchors`
  - 实测这 13 个（除 3 个 `paths_alloc` 已在 CI 跑）**逐个显式跑全绿**（`cargo test -p udf-std-ext -- --exact <13 names>` → 9 passed / 1 passed / 2 passed / 2 passed，0 failed）——**测试没坏，是 CI 门把它们藏了**
  - 对照：CI 已经为 `paths_alloc` 准备了正确的隔离手段（`ci.yml:60-61` 独立 `--test-threads=1` 步），主步的 `--skip` 属于**重复且过度**的屏蔽。
- **为什么不合理**：这是「hollow gate」——CI 绿灯掩盖了「XXH64 与官方 spec 向量不再被验证」这一事实。`hash` 域的文档明写「算法集版本化=稳定承诺」（`std-ext/src/lib.rs` 域表 hash 行），XXH64 向量锚是那个承诺的唯一执行者。若某次改动破坏了 XXH64，CI 不会红。Windows 路径语义同理（CI 三 OS 矩阵的价值正在于此）。
- **替代方案**：
  - A（推荐）：删掉主步的两个 `--skip`，只靠 `ci.yml:60-61` 的 `paths_alloc` 独立串行步隔离。若 `paths_alloc` 里的 `verbatim_hot_path_zero_alloc` 之类仍需跳过，则按**精确测试名** `--skip verbatim_hot_path_zero_alloc` 逐个列，不用宽泛字面量。
  - B：保留过滤但把被屏蔽的测试**搬到独立测试文件**（如 `tests/hash_anchors.rs`、`tests/paths_verbatim.rs`），该文件在 CI 里以独立步跑——与 `paths_alloc` 现行做法一致。
- **能否框架层踩平**：**能**（CI 配置层自己改）。
- **影响面 / 踩平难度**：影响面 = 供应链完整性（hash 域版本化承诺）+ Windows 路径正确性 + CI 门可信度；踩平难度 = 低。

### F-12｜建议｜`skeleton-infra` 里两个未声明的孤儿源文件（`text.rs` / `backoff.rs`）留在仓库里

- **现象**：`skeleton-infra/src/text.rs` 与 `skeleton-infra/src/backoff.rs` 存在且被 git 跟踪，但**没有被任何 `mod` 声明引用**，因此不参与编译、不产生测试、不出现在文档。
- **证据**：
  - `skeleton-infra/src/lib.rs:72-109` 全量模块声明：`config` / `shutdown` / `audit` / `runtime` / `fx` / `testing` / `parse_mock` / `journal` / `logx` / `tracingx` / `memgov` / `pool` / `pooled_thread_pool` / `compose`——**无 `text`、无 `backoff`**
  - `cargo doc -p udf-skeleton-infra --all-features --no-deps` 产物目录实测：`target/doc/skeleton_infra/` = `audit compose config fx journal logx memgov parse_mock pool pooled_thread_pool runtime shutdown testing tracingx`——**无 `text`/`backoff`**
  - `git ls-files skeleton-infra/src/` 确认两文件被跟踪（`skeleton-infra/src/text.rs`、`skeleton-infra/src/backoff.rs`）
  - 内容与 std-ext 重复：`skeleton-infra/src/backoff.rs:24-27` 的 `backoff_delay` 与 `std-ext/src/backoff.rs:26-39` 的同名函数**逻辑逐字相同**（同样 `base.saturating_mul(attempt).min(max)`），四个 `#[test]` 也逐字重复
  - 文件内的 doc 路径已失效：`skeleton-infra/src/backoff.rs:17` → `use skeleton_infra::backoff::backoff_delay;`（该路径在当前代码里不存在）；`skeleton-infra/src/text.rs:47` → `use skeleton_infra::text::{decode_bytes, DecodeStrategy};`（同上不存在）
  - ADR 0004 的破坏性清单（`docs/adr/0004-std-ext-split.md:63-69`）称迁移已完成、「infra 根再导出……已删除」
- **为什么不合理**：文件删了、源码没删——**死代码留在仓库里**，读者（含未来维护者与审计者）会以为 `skeleton_infra::text` 存在。它同时是「两份实现各自漂移」的种子：`backoff.rs` 的 infra 副本**没有 `log-embed` 埋点**（std-ext 副本有），说明两份已经漂移了。ADR 0004 说 std-ext 侧 backoff 有 1 个埋点调用点，infra 侧这份没有——若有人「发现」这个文件并启用它，埋点计数会对不上。
- **替代方案**：
  - A（推荐）：删 `skeleton-infra/src/text.rs` 与 `skeleton-infra/src/backoff.rs`（内容已在 std-ext，无 shim 需求——0.1.0 未发布）。零风险。
  - B：若保留（例如担心历史），加 `#[cfg(any())]` 或移到 `attic/` 并在文件头注明「已迁 std-ext，勿用」——比裸文件好，但仍是负担。
- **能否框架层踩平**：**能**（删文件）。
- **影响面 / 踩平难度**：影响面 = 仓库整洁度 + 双实现漂移风险；踩平难度 = 极低。

### F-13｜建议｜审计事件流是单槽，二次订阅静默覆盖前一个订阅者

- **现象**：`subscribe_events` 是「唯一订阅点、单槽」，第二次订阅直接替换第一个，第一个订阅者此后收不到任何事件。
- **证据**：
  - `skeleton-infra/src/runtime.rs:805-806` 文档：「**本调用是事件流的唯一开关（再次订阅覆盖前订，单槽）**」
  - `skeleton-infra/src/runtime.rs:872-880`：`*lock(&self.inner.audit) = Some(AuditSink { ... })` —— 直接覆盖 `Option`
  - `skeleton-infra/src/runtime.rs:875` 覆盖时**没有**给旧订阅者任何通知（无计数、无事件、无 warn）
  - scratch 实测（`FINDING C`）：
    ```
    1st subscriber hits after 1 dispatch = 2
    1st subscriber hits after 2nd subscriber registered + dispatch = 2   ← 未再增长
    2nd subscriber hits = 2
    ```
  - 对比框架对**订阅者**（非事件流）的纪律：订阅者 panic 摘除有 `subscriber_panics` 计数（`runtime.rs:1168-1176`），订阅表被覆盖式替换的类似问题在 `subs` 表上是「多订阅者并存」（`runtime.rs:744` `subs.push(Arc::downgrade(&slot))`），**事件流却没有多订阅者形态**。
- **为什么不合理**：事件流是框架的**事后取证面**（`audit.rs` 头注：「时序事后取证域」），tracing 桥（`tracingx::bridge_events_to_tracing`）就建立在它之上。采用方很可能同时想要「框架自带 tracing 桥」+「自己的审计消费者」——第二个 `subscribe_events` 会**静默杀掉第一个**（tracing 桥就此失效，且无任何信号）。两个订阅者不能并存这一点，与同一框架里状态订阅**支持多订阅者并存**的形态不一致（能力不对称）。
- **替代方案**：
  - A（推荐）：事件流支持多订阅者（`Vec<AuditSink>` 或每订阅者独立环），旧订阅者不被覆盖。代价：分配面与热路径成本上升（当前单槽设计是为零开销默认路径优化的）。折中：保留单槽快路径，额外提供 `subscribe_events_multi` 显式多订阅面。
  - B（最小）：覆盖时给旧订阅者发一个「被替换」事件或至少让旧票据的 `Drop` 语义可查（当前票据靠 `audit_gen` 代次对账防误杀后继流——`runtime.rs:193-194`），加一个「前订阅被替换」计数读侧。
  - C：文档化「单槽是硬约束，框架自带 tracing 桥与自定义消费者二选一」，并在 `bridge_events_to_tracing` 文档里写明。
- **能否框架层踩平**：**能**（A/B 方案框架层改）。
- **影响面 / 踩平难度**：影响面 = 观测面采用方（tracing 桥 + 自定义审计并用）；踩平难度 = 中（A 改热路径）/ 低（B）。

### F-14｜吹毛｜`config-json` 单 feature 切片下有 `unused import` 警告，CI 的 all-features 闸看不见

- **现象**：`--features config-json`（不带 `config-toml`）时 `skeleton-infra/src/config.rs:33` 的 `use std::fmt;` 未使用，clippy 报警告；CI 跑 `--all-features`（含 `config-toml`），该 import 被用上，警告消失。
- **证据**：
  - `cargo clippy -p udf-skeleton-infra --no-default-features --features "config-json"` 实测 → `warning: unused import: std::fmt`（`skeleton-infra/src/config.rs:33`）
  - `cargo clippy --workspace --all-features --all-targets` 实测 → 仅 2 个 `doc list item without indentation` 警告（`logx.rs:570-571`），**exit code = 0**（CI 步 `.github/workflows/ci.yml:75-76` 未加 `-D warnings`，注释明写「记录级：升级 -D warnings 前需先清零存量告警」）
  - 另：`--features log` 单切片实测也有 2 个 `doc list item without indentation`（同 `logx.rs:570-571`）
  - 框架有 13 个 feature 子开关（`skeleton-infra/Cargo.toml:36-70`），组合空间大；CI 实测只跑：默认、`--all-features`、`--features memgov`、`--features log`、`--release --features log`（ci.yml:38-61），**单 feature 切片无 CI 覆盖**
- **为什么不合理**：不阻塞，但暴露两个模式问题：(1) feature 门下的代码质量只在 all-features 组合下被检查，单切片死代码/未用 import 无人看；(2) clippy 闸不是 `-D warnings`，存量警告可无限累积（当前 2 个 doc 警告已在 CI 里长期存在）。框架自身的「hollow gate 纪律」（`skeleton-infra/Cargo.toml` 多处「required-features … hollow gate 纪律」自注）说明作者在意这个，但闸本身留了口子。
- **替代方案**：
  - A（推荐）：把 `use std::fmt;` 改成 `#[cfg(feature = "config-toml")]` 门控（或改用全路径 `std::fmt::…`），并在 CI 加一条 `cargo clippy -p udf-skeleton-infra --no-default-features --features config-json` 的单切片步。
  - B：CI clippy 升 `-D warnings`（需先清零 2 个 doc 警告 + 其余存量）。
- **能否框架层踩平**：**能**。
- **影响面 / 踩平难度**：影响面 = CI 闸可信度 + feature 组合爆炸面；踩平难度 = 低。

### F-15｜吹毛｜`std-ext` 十四域聚合成单 crate：粒度过粗，编译成本与版本锁定被整体绑定

- **现象**：ADR 0004 的物理拆分理由是「域可独立取用」，但结果是 14 个互不相关的域住在**一个** crate 里——采用方想只要一个 `hex` 也得拖 `cache`/`scheduler`/`ratelimit` 全部编译进二进制。
- **证据**：
  - `std-ext/src/lib.rs:91-105`：14 个 `pub mod` 全部**零 feature 门常驻**（`text backoff cache clock encode fsx hash id iter json paths ratelimit retry scheduler`）
  - 各域零互相耦合（ADR 0004:32-38 逐域验证过：`retry`→`backoff`+`clock`；`ratelimit`→`retry`+`clock`；`scheduler`/`cache`/`id`→`skeleton::clock`）
  - `std-ext/Cargo.toml:42-46`：依赖只有 `udf-skeleton`（路径）+ `serde_json` + optional `tracing` —— 但 **14 个域同时进依赖树**（无论采用方用哪几个）
  - `std-ext/src/` 源码体量：`paths.rs` 1176 + `encode.rs` 1062 + `iter.rs` 1059 + `fsx.rs` 839 + `clock/rfc3339.rs` 823 + `text.rs` 650 + `cache.rs` 633 + `scheduler.rs` 624 ≈ 6866 行（占 std-ext 主体）——这些行对只用一个域的采用方全部是编译税
  - 对照 ADR 0004:30-32 自己的理由：「**feature 门解决不了「域可独立取用」**：feature 关掉后域代码仍在 infra 包里，采用方无法只拿散件。子 crate 让叶子标准件成为独立发布单元」——拆分做到了「子 crate」，但 14 域仍挤在一个子 crate 里，**当初的诉求没有闭环**
- **为什么不合理**：ADR 的原始痛点是「采用方要一个纯 std 的 `hex_encode` 也得面对整个 infra 的 feature 矩阵」——拆分后痛点从「整个 infra」缩到「整个 std-ext」，**缩小了但没消除**。同时 14 域共享一个版本号：任何一域要 breaking change，整个 `std-ext` 升版本，所有采用方被迫一起升（SemVer 粒度被最粗的那一域绑架）。`json` 域刚引 serde_json（F-01）就是这一粒度的直接代价——为了 1 个域的 1 个依赖，整个 crate 的「零三方」承诺破了。
- **替代方案**：
  - A（推荐）：按**耦合簇**再分 2-3 个 crate（例：`udf-std-text`（text/encode/hash/iter）+ `udf-std-time`（clock/backoff/retry/ratelimit/scheduler/id）+ `udf-std-fs`（fsx/paths/cache））。簇内耦合保留，簇间解耦 → 编译成本按需付。
  - B：单 crate + **每域一个 feature 门**（回到 ADR 0004 否决的方案）——但 ADR 的否决理由（feature 关掉后代码仍在包里）在这个场景已不成立：若目标只是「不编译进二进制」，feature 门恰恰够用且更省事。**这条值得重新评估**——ADR 否决 feature 门时把「代码仍在包里」和「编译成本」混为一谈。
  - C：维持现状，但在 `std-ext/README.md` 明确「本 crate 是 14 域聚合体，编译成本是选型代价的一部分」，让采用方知情。
- **能否框架层踩平**：**部分能**（B 方案能完全踩平编译成本；A 方案只能部分）。
- **影响面 / 踩平难度**：影响面 = 全部只取一两个域的采用方 + 全部采用方的版本升级节奏；踩平难度 = 中（A 需拆 crate + 改依赖）/ 低（B 需加 14 个 feature 门 + 改文档口径，但会推翻 ADR 0004 的一条决策）。

---

## 4. 外部对标（广度闸）

**广度闸要求**：至少一次外部检索，GitHub 同类项目 star ≥100 且近一年活跃 ≥2 个；写「对标结论 + 与我们差异 + 承重假设」。可复用 `reports/2026-10-27-derive-decoupling-precedents.md`，但须自行核实关键条。

**检索记录**（GitHub API 实测，2026-10-27）：

| 项目 | star | 最后 push | 许可证 | 满足闸条件 |
|---|---|---|---|---|
| `reduxjs/redux` | 61,446 | 2026-10-20 | MIT | ✅ |
| `bevyengine/bevy` | 49,313 | 2026-10-27 | Apache-2.0 | ✅ |
| `yewstack/yew` | 32,837 | 2026-10-25 | Apache-2.0 | ✅ |
| `iced-rs/iced` | 31,845 | 2026-10-21 | MIT | ✅ |
| `linebender/druid` | 9,698 | 2026-04-23 | Apache-2.0 | ✅（但 README 标 UNMAINTAINED） |
| `linebender/xilem` | 5,581 | 2026-09-14 | Apache-2.0 | ✅ |
| `Relm4/Relm4` | 1,989 | 2026-10-26 | Apache-2.0 | ✅ |
| `ivanceras/sauron` | 2,065 | 2025-01-27 | MIT | ❌（近一年无活跃） |

（`elm/elm`、`pointfreestyle/transformer` 的 GitHub API 返回空/404——主仓未镜像或改名，不计入闸。）

**对标结论（逐条挂外部来源）**：

1. **Redux 的 `combineReducers` 不存在「路由 miss 静默吞」** —— 实读 `reduxjs/redux` 源码 `src/combineReducers.ts`（master 分支原文）：它**把每个 action 广播给每个 slice reducer**，slice reducer 对不认识的 action 返回**原状态**（`return state`）。没有「父层把 action 路由给分片、分片不认得就吞掉」这种中间形态。udf-frame 的 `combine` 是**显式父子路由**（`unwrap` 闭包返 `Option`），表达力更强（子 action 词表可与父不同、可上浮），但代价是引入了 Redux 没有的失败模式（F-04）。
2. **Redux 对「reducer 返回 undefined」是显式抛错，不是静默** —— `combineReducers.ts` 内 `throw new Error("The slice reducer for key ... returned undefined...")`。udf-frame 的 `combine` 路由 miss 是静默返回 `state.clone()`（F-04）。**「显式失败 vs 静默」的取向差异是结构性的，不是实现疏忽。**
3. **Elm 的 `Cmd`/`Sub` 二分 vs udf-frame 的 `Effect` 三形** —— 实读 Elm 官方 guide（`guide.elm-lang.org/effects/`）：Elm 分 **`Cmd`（发起副作用）** 与 **`Sub`（订阅外部事件）** 两条正交通道。udf-frame 的 `Effect` 是 `Emit`（回灌）/ `Run`（描述式副作用）/ `Cancel`（取消）三形，**没有 `Sub` 对应物**——外部事件源（timer / socket / 文件变更）进入状态机只能靠 `Run` + 解释器自己 spawn。`skeleton/src/lib.rs:340-352` 的宿主主循环接缝描述也只提到「帧循环/事件循环宿主」把外部输入转成 Action，**框架层没有提供事件订阅原语**。这是与 Elm 的一个真实能力差。
4. **Bevy 的「宏批量生成 blanket impl」范式** —— 复用仓库既有报告 `reports/2026-10-27-derive-decoupling-precedents.md` §2（本次未逐条复核，因本审计的核心 finding 不依赖它）；其要点：`bevy_ecs` 用 0..=16 参的 blanket impl 把普通 `FnMut` 映射成 `System`，**采用方写的是普通函数不是 trait impl**。udf-frame 反其道——`Reducer` 是必须手写 impl 的 trait（`skeleton/src/lib.rs:89-113`），采用方**必须**写 `impl Reducer for X`（含 `initial` + `reduce` + 4 个关联类型）。这个差异直接决定采用面 DX 负担。
5. **Druid 的 `Data` bound 被作者本人否定** —— 复用既有报告 §1（本次未逐条复核）：Druid 作者自述「app data must have a Data bound, which implies cloning and equality testing. Interior mutability is effectively forbidden」，且 Druid 已 UNMAINTAINED、Xilem 继任后完全弃用该派生。**这直接对应 udf-frame 的 F-05**：`Reducer::State: Clone`（`skeleton/src/lib.rs:89`）+ 快照 Arc 语义 = Druid 同款「clone 绑死在框架核心类型上」的坑，只是 udf-frame 尚未走到「要求相等比较」那一步。
6. **Yew 的 `Rc` 状态 + `use_reducer` 增量重渲染** —— 复用既有报告 §3（本次未逐条复核）：Yew 的状态是 `Rc<Self>`，更新是替换 Rc（一次分配）而非全量克隆。udf-frame 的 `Arc<State>` + `Arc::make_mut` CoW 是同一族思路，但**在有外部快照持有者时退化为全量深克隆**（F-05 实测 200 clones/100 dispatch-poll）。

**与本项目的差异（承重假设）**：

| 维度 | 外部主流 | udf-frame | 判断 |
|---|---|---|---|
| 效果/命令模型 | Elm: `Cmd`+`Sub` 二分；Redux: 中间件 | `Effect` 三形（Emit/Run/Cancel），**无 Sub** | udf-frame **少一个通道**（外部事件源进状态机需采用方自建） |
| 组合模型 | Redux: 广播 + reducer 自过滤；Elm: 单 update 穷尽 match | 显式父子路由（`unwrap` 返 Option） | udf-frame 表达力更强，但**引入 Redux 没有的静默失败面**（F-04） |
| 状态更新 | Yew: `Rc` 替换（一次分配）；Druid: `Data`（clone+eq，已被弃用） | `Arc<State>` + `make_mut` CoW | udf-frame 与 Yew 同族，但**在有持有者时退化到 Druid 的 clone 代价**（F-05） |
| 采用方写法 | Bevy: 普通函数 + blanket impl；Elm/Redux: 普通函数 | **必须手写 `impl Reducer`** | udf-frame 的**采用面摩擦明显更高**（关联类型 + `initial` + `reduce`） |
| 失败取向 | Redux: 显式 throw；Elm: 穷尽 match | 框架内多为「计数读侧」（优秀），但 `combine` 路由 miss / `subscribe_local` 跨线程 / 闭门 `subscribe` 三处纯静默 | **框架内纪律不统一**（F-04/F-09/F-10） |

**承重假设（本审计结论成立依赖的前提，写明以便复核）**：

- A1：**F-01/F-02 成立的前提**是 `README.md` 与 `docs/reference.md` 的「权威口径」定位继续成立（`README.md:1`、`docs/reference.md:3` 自称权威）。若项目决定这些文档降级为「非权威速查」，F-01/F-02 的严重度应从 blocker 降为建议。**但对外销售话术与实际依赖树的矛盾不因文档降级而消失。**
- A2：**F-03 成立的前提**是「`combine` 产物被误用为根」是一个真实可达路径（`combine` 返回 `impl Reducer`，与真根同类型）。scratch 实测确认可达。若未来 `combine` 改返回独立类型，F-03 自动消失。
- A3：**F-05 的性能数字**基于本机 release 构建 + 1MiB `Vec<[u64;32]>` 样本，**不构成对所有 State 形态的普适结论**；其成立部分仅限「clone 计数」结论（200 vs 100 clones/100 次 poll+dispatch），该结论与 State 体量无关，是结构性事实。
- A4：**F-11 的「10 个测试永不执行」** 基于 `.github/workflows/ci.yml` 当前文本的逐步骤覆盖分析 + `cargo test --list` 实测。若存在本审计未见的其它 CI 入口（如外部 CI 编排文件），该数字需修正。**但仓库内只有一个 workflow 文件**（`.github/workflows/` 下仅 `ci.yml`）。
- A5：**F-06/F-13 的严重度**基于「采用方是 UI/服务混合栈且通知回调可能重」的假设。若 udf-frame 的目标采用方严格限定为 headless 批处理，F-06/F-13 降为吹毛。框架自述（`skeleton/src/lib.rs:340-352`）把帧循环/事件循环宿主列为典型形态，故按混合栈计。

**未覆盖范围（诚实声明）**：
- 未跑 `cargo test --workspace --all-features` 全量（时间预算），仅跑了 std-ext 全量 `--list` 与针对性测试批 + 15 个 feature 切片的 build/clippy。
- 未逐条复核 `reports/2026-10-27-derive-decoupling-precedents.md` 的 §1/§2/§3 外部链接（本次仅复用其结论，未回访 druid-derive / bevy_ecs 源码）。
- 未审 `skeleton-infra/src/logx.rs`（1194 行）/ `memgov.rs`（777 行）/ `pool.rs`（864 行）内部实现细节，仅从契约面与调用面取证。
- 未审 `docs/research/` 下两份未跟踪研究文档（不在本次审计范围）。

---

## 5. 12 维覆盖表

| 维度 | 状态 | 结论 | 证据 | 缺失原因 |
|---|---|---|---|---|
| 1 正确性与边界 | 覆盖 | `combine` 分片作根编译通过运行期 panic（F-03）；路由 miss 静默吞（F-04）；`EffectBuf::pop_front` O(n²)（F-04 边界项，实测 20000 条 53.47ms） | `compose.rs:288-294`、scratch `FINDING A/D/E` | — |
| 2 接线与端到端 | 覆盖 | 三 crate 依赖方向单一无环，`cargo tree -p udf-skeleton-infra -e normal` 实测无环；核心零 IO 依赖为编译期事实 | `cargo tree` 实测输出、`skeleton/Cargo.toml` 无 `[dependencies]` | — |
| 3 契约与兼容 | 覆盖 | `Effect`/`Restore` 缺 `#[non_exhaustive]`，加变体即破坏（F-07）；journal 格式本身稳定（版本头 `{"v":1}` + `deny_unknown_fields` fail-closed + payload add-only 演化） | `skeleton/src/lib.rs:121/525` vs `parse.rs:41/173/234`；`journal.rs:262-268` | — |
| 4 架构与层归属 | 覆盖 | 分层方向合理（skeleton←std-ext←infra）；但 `std-ext` 14 域聚合使 ADR 0004 的原始诉求未闭环（F-15）；`json` 域把 serde_json 拉进「零三方」层（F-01）；两个孤儿源文件留在 infra 层（F-12） | `cargo tree` 实测、`std-ext/src/lib.rs:91-105`、ADR 0004:30-32、`git ls-files` | — |
| 5 既有面复用 | 覆盖 | `backoff` 在 infra 与 std-ext 双份实现且已漂移（infra 副本无 `log-embed` 埋点）；`json` 域与 `skeleton::parse::Json` 双 JSON 词汇并存但无桥接 | `skeleton-infra/src/backoff.rs` vs `std-ext/src/backoff.rs`、`skeleton/src/parse.rs:40-95` | — |
| 6 融合度 | 覆盖 | 契约层与实现层职责边界清楚（journal 双 trait 词表已分离、ADR 0005 已拍板）；但 `combine` 产物的「根 vs 分片」语义未进类型层（F-03） | `journal.rs:566` vs `docs/adr/0005-journal-reshape.md`、`compose.rs:288-294` | — |
| 7 性能与资源 | 覆盖 | 快照持有者致每次 dispatch 额外全量深克隆（实测 200 vs 100 clones/100 次 poll+dispatch；1MiB State ≈210µs/dispatch vs 自述 80ns 锚）；`EffectBuf::pop_front` O(n²)（20000 条 53.47ms）；clone 在 state 锁内（`runtime.rs:1108`） | scratch 克隆计数 + release 计时、`skeleton/src/lib.rs:177-183`、`runtime.rs:1104-1114` | — |
| 8 安全与权限 | 不适用 | 本框架无权限/凭据/注入面；`skeleton::parse::SqlDialectPort` 已红线声明「不提供参数值→文本拼接的绑定原语」（`parse.rs:180-186`），`logx` 有密钥掩码面 | `skeleton/src/parse.rs:180-186`、`logx.rs` mask_secrets | 架构审计范围内无安全面可查 |
| 9 失败模式与可观测 | 覆盖 | 框架内多数失败有计数读侧（四条 panic 路径 + 闭门拒绝 + 回灌预算），是强项；但四处纯静默：路由 miss（F-04）、`subscribe_local` 跨线程（F-09）、闭门 `subscribe`（F-10）、审计单槽覆盖（F-13） | `runtime.rs:809-816/1075-1080/1168-1176`（有计数）vs `compose.rs:284-287`/`runtime.rs:661-666`/`runtime.rs:691-696`/`runtime.rs:872-880`（无计数） | — |
| 10 测试充分性 | 覆盖 | CI `--skip anchors --skip verbatim` 屏蔽 10 个真实测试（含 XXH64 官方向量锚）；被屏蔽者显式跑全绿（证明是门藏了不是测试坏）；单 feature 切片无 CI 覆盖且 `config-json` 切片有 unused import 警告 | `ci.yml:44-45`、实测 `--list` 350 个中命中 17、`cargo test --exact` 13 个全绿、`ci.yml:75-76`（无 `-D warnings`） | — |
| 11 外部对标 | 覆盖 | 8 个 GitHub 项目元数据实测（4 个满足 star≥100 + 近一年活跃）；Redux `combineReducers.ts` 源码实读；Elm guide 实读；Druid/Yew/Bevy 结论复用仓库既有报告（未逐条复核，已在 §4 标注） | §4 全表 + `reduxjs/redux` master `src/combineReducers.ts` 原文、`guide.elm-lang.org/effects/` | — |
| 12 依赖与供应链 | 覆盖 | `serde_json` 以普通依赖进 `std-ext`（破「零三方」承诺，F-01），且 P1-9 该决策**无 ADR 记录**（`docs/adr/` 5 篇无一提及）；其余三方依赖（serde/tokio/log/tracing/toml/criterion/humantime）feature 门控正常，`cargo tree` 默认树仅 serde_json 子树 | `std-ext/Cargo.toml:44`、`docs/adr/` 目录列表、`cargo tree` 实测 | — |

---

## 6. 建议的处理顺序（供 PM 排期参考，非派单）

1. **先修 F-01 + F-02**（半天内可完）：`std-ext/Cargo.toml` 的 `serde_json` 加 feature 门（或移域）+ 5 处文档口径修正 + reference.md 补 json 章节。这一步消除「对外承诺与实际矛盾」，影响面最大、成本最低。
2. **再修 F-11 + F-12**（半天内可完）：CI 去掉宽泛 `--skip` 改精确名；删两个孤儿文件。恢复 CI 闸可信度 + 消除双实现漂移种子。
3. **然后 F-07 + F-03**（各半天到一天）：`Effect`/`Restore` 加 `#[non_exhaustive]`；`combine` 的根/分片边界进类型层（方案 A）。均为破坏性但未发布，成本可控。
4. **F-04 / F-05 / F-09 / F-10 / F-13** 可合并为一批「静默面补计数读侧 + 文档口径」：框架内既有纪律是「所有异常都有计数」，这批就是把纪律补齐到一致。
5. **F-06 / F-08 / F-15** 属架构级取舍，建议先出 ADR 再动手；F-15 需要重新评估 ADR 0004 的「否决 feature 门」论证（见 finding 正文）。

---

## 7. 产物清单

| 路径 | 类型 | 说明 |
|---|---|---|
| `reports/2026-10-27-udf-architecture-audit.md` | 报告（本文件） | 架构设计合理性审计全文 |
| — | 临时验证 crate | `C:/Users/Begonia/AppData/Local/Temp/udfaudit`，跑完已 `rm -rf` 删除（实测 `ls` 确认不存在）。未落在仓库内，未污染工作区。 |
| — | 仓库源码 | **未改任何源码**（`git status --short` 输出的 `.gitignore` 修改与 `docs/research/`、`reports/` 三个未跟踪项均为审计前既有，非本次产生） |
