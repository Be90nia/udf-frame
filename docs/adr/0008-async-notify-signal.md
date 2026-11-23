# ADR 0008：通知面异步化——两档并存 + 状态水位信号（`NotifySink` / `Notify`）

- 状态：**已采纳**（2026-10-27，用户拍板 F-06 = 案 C；Q1–Q5 同批拍板）
- 决策人：用户拍板（F-06 通知面异步化；关联 issue `udf-frame-18g` 设计稿 → `udf-frame-f5y` 实现）
- 关联：
  - 设计稿 `docs/research/async-notification-design.md`（架构岗产出，VERDICT PASS；四案与语义公理的完整论证在彼，本文不复述全文）
  - 审计 `reports/2026-10-27-udf-architecture-audit.md`（F-06 通知同步执行 / F-05 快照深克隆，原始 finding）
  - ADR `0007-std-ext-packaging-granularity.md`（零三方口径；本 ADR 不触依赖树）
  - ADR `0004-std-ext-split.md`（三 crate 分层纪律：契约层 `skeleton` 零依赖）
  - ADR `0005-journal-reshape.md`（破坏性直迁先例：0.1.0 未发布，无 shim 负担）
  - ADR `0006`（视图桥，**尚未落盘**；本 ADR 对桥侧的三处约束见 §6，落 ADR 0006 时须回读本节）
  - `docs/research/mvi-view-bridge-research.md`（ViewBridge 方案）

---

## 1. 背景：慢订阅者阻塞整个状态机，且现有同步语义被三处钉死

F-06 的现象是「订阅回调在 dispatch 线程内同步执行，回调耗时直接计入 dispatch 延迟」。但这**不是**
「加一个异步 API」就能解的问题——现有通知语义被三处钉死，任何改动都要同时裁决这三条。

**树内事实（逐条可复核，行号对当前源码）**：

| # | 事实 | 证据 |
|---|---|---|
| F1 | 契约层承诺「回调在 dispatch 线程同步执行」 | `skeleton/src/lib.rs:373`（`Store` 语义契约第 3 条）；`skeleton/src/lib.rs:429`（`fn subscribe`）；`skeleton/src/lib.rs:363`（「六方法即全契约」） |
| F2 | 通知调用点在**排空环体内**（每拍一次） | `skeleton-infra/src/runtime.rs:1081`（`notify_snapshot_consumers(inner, cache, &notified_next)`），位于 `drain_owned`（`:967`）的循环体内 |
| F3 | 排空环管线序 = reduce → 快照落位 → 效果转发 → 通知 | `runtime.rs:1033`（reduce）→ `:1070`（`place_snapshot`）→ `:1079`（`forward_effects`）→ `:1081`（通知） |
| F4 | 通知体执行用户闭包 | `runtime.rs:1168`（`notify_snapshot_consumers`）→ `:1197-1200`（`catch_unwind` 包裹 `notify(next)`） |
| F5 | 订阅回调内 dispatch 被计为**回灌深度**（受 64 预算约束） | `runtime.rs:241`（`FEEDBACK_DEPTH_BUDGET = 64`）、`:286`（`reentrant_origin`）、`:267`（`ReentrantDepthGuard`） |
| F6 | 存在**两档**订阅面（跨线程 / 本地线程亲和），本地档他线程 dispatch **不触达** | `runtime.rs:691`（`subscribe_local`）、`skeleton-infra/src/runtime/local_subs.rs`（TLS 表）、`runtime.rs:670-672`（通知序文档：本地先行、跨线程随后） |
| F7 | 通知顺序对订阅者可观测 | `skeleton-infra/tests/store_behavior_gaps.rs:237-240`（一 dispatch 内回灌 ⇒ 通知序 `[1, 11]`） |
| F8 | 审计事件链在通知点交织（`Notified` 与 `Reduced`/`Enqueued` 逐拍交错），有测试逐字钉死 | `runtime.rs:1084-1085`（发 `Notified`）、`skeleton-infra/tests/journal_port.rs:309-320`（交错链断言） |
| F9 | 快照落位用 `Arc::make_mut` **就地改写**（外部持有者 ⇒ 每拍深克隆 `S`） | `runtime.rs:1097-1106`（`place_snapshot`；`:1103` = `mem::replace(Arc::make_mut(...))`） |
| F10 | 性能锚：无订阅 ≈ **77 ns**、订阅态全含 ≈ **142 ns** | `README.md:144`、`docs/reference.md:201`（2026-10-16 批 C 实测） |
| F11 | `runtime` feature **零三方** | `skeleton-infra/Cargo.toml:32`（`runtime = []`）、`skeleton/Cargo.toml`（不设 `[dependencies]`） |
| F12 | 深克隆的实测代价：1 MiB State ≈ **210 µs/dispatch** | 审计 F-05（`reports/2026-10-27-udf-architecture-audit.md:178`） |

**影响面**：① 一个做 IO/渲染的订阅者，让后续 action 的排空（连同 Emit 回灌链）全部排队等待；② dispatch
调用方无法区分「状态机慢」与「订阅者慢」（F10 的 142 ns 只在订阅者**廉价**时成立）；③ 异步化不是加一个
API 就能解决——语义被 F5/F7/F8 三处钉死。

**外部判据**（来自 `reports/2026-10-27-mvi-mvvm-coupling-pitfalls.md` 的 M1/M5 抄录清单）：

- **M1（事件规约成状态）**：Android 官方 UI 架构指南原文——「UI actions that originate in the
  ViewModel—ViewModel events—should always result in a **UI state** update. This complies with the
  principles of Unidirectional Data Flow (UDF). It makes events reproducible after configuration
  changes and guarantees that UI actions won't be lost.」
  — https://developer.android.com/topic/architecture/ui-layer/events
  ⇒ 载荷必须**状态化**，不做「每拍一条事件」的有界队列。
- **M5（帧/拍级一致的水位与版本）**：React 官方 `useSyncExternalStore` 参考页原文——订阅回调被唤起后
  React 重新调用 `getSnapshot`；且「for every Transition update, React will call `getSnapshot` a
  second time just before applying changes to the DOM. If it returns a different value than when it
  was called originally, React will restart the update from scratch, this time applying it as a
  blocking update, to ensure that every component on screen is reflecting the same version of the store」
  — https://react.dev/reference/react/useSyncExternalStore
  ⇒ 出站带**单调版本号**，消费侧见版本跳跃即重取（resync），绝不撕裂。

---

## 2. 语义公理：异步通知必须满足的六条判据

任何候选方案逐条对表，对不上即出局（推导过程见设计稿 §3）：

| # | 公理 | 含义 |
|---|---|---|
| A1 | **投递不得执行用户重活** | 排空环内用户代码执行时间 = 0（结构性，非统计） |
| A2 | **投递必须 O(1) 且非阻塞** | 投递成本与订阅者快慢无关 |
| A3 | **顺序可恢复** | 任一消费侧能从投递物恢复「先后」 |
| A4 | **合并合法但丢失可检出** | 中间拍可丢，但必须能检出丢了 |
| A5 | **失败可见** | 投递失败/panic/丢弃均有计数或成因面 |
| A6 | **关闭语义显式** | 闭门后登记/投递行为可查 |

---

## 3. 方案对比：四案与否决理由

| 判据 | A 排空后统一派发 | B 每订阅者队列+框架泵线程 | **C 两档并存 + 状态水位信号（采纳）** | D 桥侧自管 |
|---|---|---|---|---|
| 排空环内用户代码执行时间 | 0（派发移出环） | 0 | **0（结构性）** | O(1) 自增 |
| dispatch 延迟受订阅者支配 | **是**（仍等全部回调） | 否 | 否（同步档除外） | 否 |
| 破坏既有契约/测试 | **是**（审计链 + 通知时刻 + 新预算） | 是（新票据/停机序） | **否** | 否 |
| 违 M1（事件流）风险 | 中 | **高**（队列=事件流） | 低（信号非事件） | 低 |
| M5 版本水位 | 需补 | 需补 | **内建** | 需框架补 |
| 新增失败面种类 | +1（派发轮次预算） | ≥+4（线程/队列/溢出/停机） | **+0**（复用既有计数） | +0（框架） |
| 新增第三方依赖 | 无 | 无 | 无 | 无 |
| 回滚成本 | 中 | 高 | **低** | 最低 |
| 框架层踩平 F-06 | 半 | 是 | **是** | **否** |
| 线程归属 | 框架不新增线程 | **框架新增线程** | **框架不新增线程** | 宿主 |

**否决理由（各自的具体破面，不作「不够优雅」式泛评）**：

- **A 否决**——移出环体 ≠ 不阻塞：dispatch 返回仍被订阅者时长支配。且派发发生在
  `ReentrantDepthGuard` 恢复之后 ⇒ 订阅者回调内 dispatch 被判为 `External`（`runtime.rs:286`）⇒
  F5 的回灌预算**不再约束订阅者驱动的环** ⇒ 需另加「派发轮次预算」这一新失败面。破坏面（F8 的审计
  交错链断言）换不来收益。
- **B 否决**——异步 ≠ 线程 + 队列：逐拍快照队列即事件流（违 M1），且把背压/丢弃/停机/未消费四类问题
  变成框架的新责任（在「状态信号」语义下这四类问题**根本不存在**）；框架新增线程与「框架不拥有主循环」
  的既有接缝张力最大。
- **D 否决**——只让桥不堵 drain，框架层未踩平 F-06：外部采用方拿不到桥的代码，只会拿到「回调在 dispatch
  线程同步执行」这句契约，每个采用方各写一遍线程与生命周期（重复实现）。D 与 C 不冲突（D 是 C 的
  「只加版本号」子集），可作过渡止血，不作终态。

---

## 4. 决策

1. **两档并存**：现有 `Store::subscribe`（跨线程同步档）与 `SyncStore::subscribe_local`（本地线程亲和档）
   **语义一字不改**（零破坏 ⇒ 零回滚成本）。
2. **新增独立契约端口**（`skeleton/src/notify.rs`，契约层零依赖）：`NotifySink`（宿主实现的投递口）+
   `Notify<S>`（实现方实现的登记面）；票据**复用** `Subscription<S>`（`skeleton/src/lib.rs:314-336`，
   `Subscription::new` 已是公开构造器，**不新增票据类型**）。新端口**不扩** `Store` 的六方法（F1 的
   「六方法即全契约」纪律）。
   签名级（草案，非最终措辞）：

   ```rust
   /// 状态变更信号投递口：**宿主实现**。框架只调用，绝不在调用内执行用户重活。
   pub trait NotifySink: Send + Sync + 'static {
       /// 投递一次「状态已变」的信号。契约：O(1) 返回；允许合并；
       /// panic 由框架捕获并摘除该投递口（fail-closed）。
       fn publish(&self, version: u64);
   }

   /// 异步（信号式）通知端口：**实现方**实现。与 `Store` 正交。
   pub trait Notify<S> {
       /// 登记投递口。票据 Drop = 注销（复用 `Subscription<S>` 语义）。
       fn subscribe_notify(&self, sink: impl NotifySink) -> Subscription<S>;
   }
   ```

   （`subscribe_notify` 含 `impl` 泛型参数 ⇒ 与既有 `Store::subscribe` 同款非 dyn-safe；不新增新规则。）
3. **载荷 = 单调状态版本号**（Q1 拍板：版本号 + 拉取）。消费侧按需 `store.snapshot()`
   （`skeleton/src/lib.rs:406`，既有拉取面）拉取现态。**不提供事件流形态**（M1）。
4. **排空环内对异步档只做 O(1) 投递，不执行任何用户重活**；**异步投递排在同步回调之前**——否则同步档里的
   慢订阅者会把异步消费者的可见延迟一并拖后。异步投递不影响同步档内部既有顺序（本地先行、跨线程随后，
   `runtime.rs:670-672`），也不影响审计链（F8 的 `Notified` 仍在通知段末尾）。
5. **失败面复用既有读侧，不造第二套**：投递口 panic ⇒ 捕获 + 摘除该投递口 + `subscriber_panics` +
   `subscriber_panic_last_cause`（`runtime.rs:744` / `:753`）；闭门后登记 ⇒ `subscriptions_after_close`
   （`runtime.rs:761`）；**唯一新增读侧** = `notify_published()`（累计投递次数，用于成本可见）。
6. **快照落位策略本期不变**（Q2 拍板：先 (a)）：维持 `Arc::make_mut` 就地改写（F9）；「消费侧跨拍持有
   `Arc<S>` ⇒ 深克隆税」的升级路径（每拍新 `Arc`）写为触发式后续项，触发条件见 §8。
7. **断连不另加信号**（Q3 拍板）：投递口断连/store 提前 drop 后，宿主经 `is_closed()`
   （`skeleton/src/lib.rs:435`）自查；框架不新增 closed 信号语义。
8. **同步档不加「回调耗时」观测面**（Q4 拍板）：为一个猜测预付埋点；二期再说。
9. **本地档不异步化**（Q5 拍板）：本地档的价值正是「免 `Send` + 线程亲和」（F6），异步化会摧毁它的存在
   理由；异步档是**跨线程档**的补充。
10. **依赖树不变**（F11）：`runtime` feature 仍零三方；契约层新增模块仍是纯 std。

---

## 5. 落地面

### 5.1 契约层（`skeleton`，零依赖）

- 新增 `skeleton/src/notify.rs`：`NotifySink` + `Notify<S>`；复用 `Subscription<S>` 票据。
- `skeleton/src/lib.rs` 的 `Store` 类型文档加一行指针（**六方法承诺不动**）。

### 5.2 实现层（`skeleton-infra`，`runtime` feature，零三方）

- 新增 `skeleton-infra/src/runtime/notify.rs`（与 `local_subs.rs` 同款「关注面切缝」体例；
  `runtime.rs` 已 1600+ 行不宜再涨）：投递口注册表 + 版本计数器 + 门控计数。
- `SyncStore` 实现 `Notify<S>`；通知段新增「异步投递」分支，**排在同步回调之前**。
- **版本号推进与落位一一对应**（落位是状态唯一变化的时刻 ⇒ 版本号 = 状态代数）；**零异步订阅者时不自增**
  （门控，照抄 `audit_emit.rs:56-58` 的「一次 Acquire load 即返」快路径）⇒ F10 的 77 ns 锚不受影响。
  门控只影响「无人订阅期间是否推进」，**不影响订阅者在册期间的逐拍对应**；版本号在订阅者增减时**不得回退**。
- 投递口调用**不持锁**（照抄 `audit_emit.rs` 的「取出 → 锁外调用 → 归还」范式：`:63-71` 取出回调、
  `:79-89` 锁外 `catch_unwind` 调用、`:92-100` 归还槽位——持锁调用户代码即死锁）。
- 内存增量 = O(异步订阅数)，**无队列、无缓冲**。

### 5.3 测试（实现期）

- 行为面 `skeleton-infra/tests/async_notify.rs`：① 版本号逐拍单调；② 投递次数与订阅数一致；
  ③ 投递口 panic ⇒ 摘除 + 计数 + 成因；④ 闭门后登记计数；⑤ 票 Drop ⇒ 不再投递；
  ⑥ **零订阅者时 dispatch 延迟不回退**（对齐 F10 的 77 ns 锚，走 `skeleton-infra/examples/dispatch_profile.rs`
  同款口径）。
- `compile_fail` 断言裸调用被 `unused_must_use` 拦（同步档已有同款 doctest 范式）。
- **零破坏的证明方式**：既有通知/审计/回灌测试文件**零改动**（`tests/journal_port.rs`、
  `tests/store_behavior_gaps.rs` 的断言逐字不动）。

### 5.4 文档（实现期同步）

`docs/reference.md` 新增「异步（信号式）通知档」小节；`skeleton-infra/src/runtime.rs` 类型文档「订阅面」
段落扩为三档说明（跨线程同步 / 本地同步 / 异步信号）并写明「**异步档不救同步档**」；`README.md` /
`docs/reference.md` / `docs/adr/` 三处口径逐条对账（本仓已存在文档口径漂移，审计 F-01/F-02）。

### 5.5 失败面与读侧映射（不新增异常种类）

| 失败模式 | 框架行为 | 读侧 |
|---|---|---|
| 投递口 `publish` panic | `catch_unwind` 捕获 → 摘除该投递口（fail-closed，同同步档纪律） | `subscriber_panics` + `subscriber_panic_last_cause` |
| 投递口 `publish` 阻塞/极慢 | 框架**无法检测也无法阻止**（O(1) 是契约义务） | 无（宿主侧自律；文档明示） |
| 消费侧合并丢拍 | 框架不感知（合并是宿主侧行为） | 版本号让消费侧检出跳拍 → 重取现态 |
| 闭门后登记投递口 | 登记照常，**永不发信号** | `subscriptions_after_close` |
| store 提前 drop | 投递口随注册表消亡 | 宿主自查 `is_closed()`（Q3：不另加信号） |
| 订阅者回调内 dispatch 成环 | **不受影响**（同步档语义与回灌预算完全不动） | 既有 `feedback_budget_exceeded` + 64 预算 |
| 消费侧跨拍持有 `Arc<S>` | 框架不感知；下一拍落位走 `Arc::make_mut` 克隆分支 | 无（升级路径见 §8 第 1 条） |
| 投递成本可见 | 累计投递次数 | **新增唯一读侧** `notify_published()` |

### 5.6 可替换组件留位（契约不换，机器可换）

| 组件 | 留位方式 | 何时填 |
|---|---|---|
| 投递口实现 | `NotifySink` 由宿主实现 | 立即（宿主自选） |
| tokio 版投递口 | infra `tokio` feature 下提供 `NotifySink` 的 tokio 实现（`tokio::sync::Notify` / `mpsc::Sender::try_send`） | 宿主为 tokio 生态时（契约面不变） |
| 快照载荷变体 | `publish` 增参 `Option<Arc<S>>` 或另开 trait | §8 第 2 条证伪条件触发时 |

---

## 6. 与 ViewBridge 的交叉（三处硬约束，不得含糊）

桥以 `store.subscribe` 挂载，通知体内做投影 + eager 区域编码 ⇒ 桥是 F-06 的**第一个受害者**。ADR 0006
落盘时必须显式记账以下三条：

1. **eager 最新性语义重述**：「共享区始终最新」改为「**至最近一次消费点为止**的最新」+ 版本号进区域头
   （`Header.version` 已有版本位）供 UI 判新鲜度。前提：C 案下投影/编码搬到宿主消费线程。
2. **两处版本号同源**：桥的 `PatchBatch` 版本（M5 要求带水位）与异步档的 `state_version` **必须同源**，
   否则撕裂判定失真。单一来源：框架侧的状态版本计数。
3. **通知线程归属改写**：桥的「通知体在 dispatch 线程」前提失效 ⇒ 投影从「dispatch 线程内」变为
   「宿主消费线程内」；`core` 锁竞争方从「dispatch 线程 vs pull 线程」变为「消费线程 vs pull 线程」，
   锁窗声明相应改写。桥的投影 panic 毒化位仍在桥内，但「毒化发生在消费线程」须写明。

**审计 finding F-09（`subscribe_local` 跨线程 dispatch 时静默不触发）**不在本 ADR 修复范围（对应上表 F6）——
本次不修，如实记账。

---

## 7. 后果

**正向**：

- 排空环内用户代码执行时间结构性归零（A1）；投递 O(1) ⇒ 慢订阅者不再传导到 dispatch 延迟。
- 既有契约、审计链、通知序、回灌预算**零改动**（F5/F7/F8 三处钉死全部不动）⇒ 既有测试全部不动，
  回滚成本 = 删新增文件。
- 载荷状态化 ⇒ 与 M1 同向；版本号内建 ⇒ 与 M5 同源（ViewBridge 的 `PatchBatch`/区域头版本可取同一来源）。
- 框架不新增线程 ⇒ 不引入线程生命周期 / join 序 / 队列背压 / 未消费丢弃四类新失败面；守「框架不拥有
  主循环」的接缝纪律。
- 契约层词汇零膨胀：`Store` 仍是六方法，`NotifySink` 只有一个方法。

**负向 / 代价（如实声明）**：

- 消费侧要自己写「信号 → 拉取 → 处理」循环（采用方多写几行）；框架**不提供**「替你推确切快照」。
- 合并丢拍对消费侧是常态（须靠版本号检出）——不懂这一点的采用方会看到「少了几次更新」。
- **同步档不受益**：慢同步订阅者照样堵 drain（须靠契约文案引导迁移）。
- 版本号与桥/其他模块的代数必须同源，否则撕裂判定失真。
- 投递口「O(1) 且不阻塞」是**契约义务**，框架无法检测也无法阻止（FM2：宿主侧自律）。

**破坏性变更清单（0.1.0 未发布，零兼容负担，无 shim）**：

| 项 | 旧 | 新 |
|---|---|---|
| `skeleton` 公共面 | 无异步通知端口 | 新增 `notify` 模块（`Notify` / `NotifySink`），`Store` 不变 |
| `SyncStore` | 无异步通知面 | 实现 `Notify<S>`；新增 `notify_published()` 读侧；新增内部版本计数器 |
| 既有订阅语义 | 同步回调在 dispatch 线程 | **不变** |
| 依赖树 | `runtime` 零三方 | **不变** |

---

## 8. 升级与复评触发条件

1. **落位策略 (a) → (b)（每拍新 `Arc`）**：触发条件 = 实测证实「消费侧跨拍持有 `Arc<S>` ⇒ 每拍全量深
   克隆」在真实负载下显形（审计 F-05 已测出 1 MiB State ≈ 210 µs/dispatch 的量级）。执行 (b) 前**必须重测
   无订阅 dispatch 锚**（77 ns 量级）；若锚回退超过 10 ns（与设计稿 §5.1 同一热路径容忍度），则 (b) 不落地，
   改走「消费侧自律 + 文档口径」路径。
   **【2026-10-27 回填：实测完成，(b) 不落地，(a) 维持】**采集器
   `skeleton-infra/examples/snapshot_placement.rs`（release 实测，语义探针 + 四臂基准）：
   ① 小状态无/有持有者 79.2 / 80.9 ns/dispatch——对齐 77 ns 锚（±3 ns 噪声内），同步档热路径零回退；
   ② 1 MiB 大状态 ~230 µs/dispatch（236.8k 无持有者 / 227.6k 有持有者）——与审计 F12 的 ~210 µs
   同量级（独立复核成立），大 State 每拍拷贝税实锤，「大 State 是反模式」文档红线获第二次实测支撑；
   ③ **持有者差异不可分辨**（差 <5%，被大状态臂 ~230 µs 地板淹没；clones/dispatch 仅 0.0017，
   远低于 F9 理论预期的「每拍 1 次」——疑似 harness 的持有 Arc 未跨拍存活，**该项标待专项探针复核**，
   不影响结论：(b) 每拍强制新分配只会 ≥ (a)，无任何臂显示 (b) 有收益）。
   复评触发条件不变；专项探针（O(1) reducer + 持有者跨拍存活的隔离测量）列为低优先级尾巴，不阻塞实现。
2. **证伪条件（出现任一即重开本决策）**：
   - ≥3 个真实采用方要求「框架把确切快照推给我」（不接受拉取）⇒ 载荷变体升级为「版本号 + `Arc<S>`」，
     并同步联裁第 1 条落位策略；
   - 消费侧普遍为「单次动作后立即取一次」的低频模式且延迟预算 < 100 µs ⇒ 重评「框架单泵线程 + 每订阅者
     单槽 latest-value」变体（须先证明单槽为状态化、非事件流）；
   - 宿主全为 tokio 生态 ⇒ 在 infra 侧提供 tokio 版投递口实现，**契约面不变**；
   - 实测异步档让无订阅 dispatch 从 77 ns 退化 > 10 ns ⇒ 复核门控实现（版本自增与投递同闸）。

---

## 9. 验证锚（实现期可执行）

| 断言 | 手段 |
|---|---|
| 既有语义零破坏 | `tests/journal_port.rs` / `tests/store_behavior_gaps.rs` 等通知/审计/回灌测试文件**零改动**（diff 实证） |
| 异步档行为面 | `skeleton-infra/tests/async_notify.rs` 六项断言（§5.3） |
| 无订阅 dispatch 不回退 | `cargo run -p udf-skeleton-infra --example dispatch_profile --features runtime`（对齐 77 ns 锚） |
| 依赖树不变 | `cargo tree -p udf-skeleton-infra -e normal` 零三方口径不变 |
| 排空环内零用户重活 | 「回调 sleep 50 ms」的**异步**投递口挂上后，dispatch 延迟保持 ~100 ns 量级 |

---

## 10. 遗留 / 交回 PM

- 实现尚未开始（下游 `udf-frame-f5y` 承接拍板 → 实现）；本 ADR 的 §5 落地面为实现契约，非既成事实。
- `CHANGELOG.md` 记一条「Added（异步信号式通知档）」待实现落地时补（对齐 ADR 0005/0007 的记法）。
- ADR `0006`（视图桥）尚未落盘；其 eager 最新性措辞、版本同源、通知线程归属三处须回读 §6。
- 落位策略 (b) 的实测在另一单进行中，结论回填 §8 第 1 条。
- 审计 finding F-09（本地档跨线程不触达）不在本次范围，如实记账。
