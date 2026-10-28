# udf 语言无关视图数据面（MVI View Bridge）——架构方案

- 日期：2026-10-27（**v6.1 = 封版候选（v6 复审修订轮）**：按 momus FAIL 报告 `reports/2026-10-27-mvi-view-bridge-v6-review.md`（3 Major / 7 Minor / 1 Trivial）修订 + **PM 三项 Major 裁决落档**——①版本来源两档分写（同步档 MVP = 桥内单调消费代数，异步档切框架 `state_version`，消费入口不在 0006 MVP 范围）；②行预算重定基 ~840（= ~740 截至 v6 + ~100 M3 执行件增项；PM 预批，补审 N1 更新批准数）；③M3 执行件最小形态（manifest 真源 + `view_constants_gen` 生成器 + `--check` CI 断言，Rust demo `include_str!` 对表）。前版：**v6 = 封版候选（踩平轮）**：外部踩坑扫描（`reports/2026-10-27-mvi-mvvm-coupling-pitfalls.md`）+ 架构审计（`reports/2026-10-27-udf-architecture-audit.md`）输入 → **机制化落点**：版本序列单源 + 出站水位（`from_version`/`to_version`）· 信封协议版本 + 逐项载荷标签 + 未知标签跳过（计数 + resync 提示）· 命名工件（manifest 单一真源 → Rust/TS/Python/C 常量）· 锚口径诚实化 + 大 State 成本 · 通知线程成本入账（诚实声明不解决核心问题）· 公开词表冻结 + `#[non_exhaustive]`。v5 = 可组合编码矩阵 + 二进制区域（固定容量、索引、版本、eager 变更即编）；轮 3 = 内部载体类型化/边界 wire 化；轮 2 = 绑定级输入哨兵；v2 = momus 终审轮 1 修订 F1-F5）· 状态：**提案（v6.1 封版候选，待 momus 补审：范围 = 复审报告末节「通过条件」所列段落——§1 两行 / §2-2 / §3.2-v6(a)(e) / §8 / §9 四行 / §10 状态与⑩ / §12-12；未拍板；封版后方案冻结，后续改动一律走 ADR 修订）** · 作者：ViewBridgeArch（架构岗，只出方案不动码）
- 关联：`docs/research/mvi-view-bridge-clarify.md`（用户原话+事实账本）· `reports/2026-10-27-derive-decoupling-precedents.md`（T2 对标）· ADR 0004（分层）/ 0005（关联类型化先例）
- 本文是 ADR 0006 草稿的承载文件（拍板前不建 docs/adr/ 文件，见 §10）

---

## 1. 驱动因素（决定结构的力）

| 驱动力 | 证据 | 对结构的约束 |
|---|---|---|
| UI 与领域类型必须彻底隔离 | 用户原话③：映射/派生在 Rust 内做好，UI「绑定好要获取的数据就能拿到」 | 出站面只能是数据（JSON 值），领域 `S`/`A` 类型与字段形状**不出 Rust**——比 Tauri「emit 整个 State」更彻底：连序列化领域类型都不发生 |
| 三处既有耦合 | 账本：Driver::render 下行全量 `&S`；UI 手拼嵌套 Action 树（combine 六参 wrap/unwrap）；无 diff 闸全量重渲 | 三处都要有对应解法：投影（下行）、意图映射（上行）、diff 闸（通知面） |
| 性能与内存 | 用户原话①；现有锚：无订阅 dispatch ~77ns / 订阅态全含 ~142ns（§2.4） | 桥不得触碰 dispatch 排空环本体；diff 成本按绑定收敛不按视图收敛 |
| 零新依赖 + 三 crate 纪律 | ADR 0004；skeleton 零 `[dependencies]`；serde_json 已按 ADR 0007 门化为 std-ext `json` feature（optional，默认树零三方）+ infra optional（journal/config-json 先例） | 契约词汇进 skeleton（零依赖可编译）；机器进 infra（feature 门控 serde_json）；禁 proc-macro（T2 实证成本） |
| 字符串路由否决不翻案 | compose.rs「字符串路由组合的封死形态」 | 数据寻址用路径串与 action 路由的张力必须显式划界（§7） |
| 失败可见（fail-visible） | 全库纪律：订阅回调 panic 摘除留痕、回灌有界止步计数、闭门显式 false | 未知路径/未知意图显式 Err；投影 panic 毒化 + 计数 + resync 重同步契约（v4 起 latest-wins 折叠 ⇒ 出站箱溢出结构性退役，折叠只合并不丢弃，§6） |
| **协议兼容期不整批失败（v6 新增驱动）** | 踩坑扫描 §6：serde 未知变体 = 整条 Err、protobuf 旧解析器跳过未知字段、`#[non_exhaustive]` 给「新增变体不破下游」官方出口；审计 F-07 同型风险（core `Effect`/`Restore` 加变体即破坏） | 「只加不改」从**文档纪律**升为**机制**：信封带 protocol version + 每项载荷标签；读侧遇未知标签 ⇒ 跳过该项 + 计数 + resync 提示，**禁整条失败**（M2） |
| **跨语言视图一致性（v6 新增驱动）** | 踩坑扫描 §9：React `useSyncExternalStore` 的三级支持阶梯（撕裂 = 外部 store 在渲染中途变化）；§8：Tauri 顶层 camelCase / 嵌套 snake_case ⇒ 前端 `undefined` | 两通道共享**同一版本序列**（单源），出站带水位 ⇒ UI 侧可判「缺段/倒退」并自愈重同步（M1）；路径/字段名以**命名工件**单一真源生成（M3） |

---

## 2. 既有面核对（派单指定四项）

1. **std-ext json 域编码能力**：`std-ext/src/json.rs` 确为 decode-only（`decode`/`decode_str` 双模 + 64 MiB 上限 + 行列错误）。**出站编码判断成立**：`serde_json::Value` 自带 `Serialize` 实现，`serde_json::to_string(&Value)` 无需 derive、无需新依赖——桥的出站面 = 直接构造/持有 `Value`，序列化免费（T1：serde_json 官方文档）。入站解析复用 `std_ext::json::decode_str`（Strict 模式 + 上限纪律白得）。
2. **SyncStore 通知面够不够挂 diff 闸**：够，且**核心零改动**。`runtime.rs` 排空环管线序 = reduce→快照落位→效果转发→通知；通知为**逐 action** 每拍携带新 `Arc<S>`（`drain_owned` 迭代末 `notify_snapshot_consumers`），零订阅者时 `notified_next=None` 连 clone 都省。`NotifyCache.version` 是**订阅集版本**（增删/摘除才自增），不是状态版本。**出站版本语义（v6.1 定案，两档统一表述，与 §3.2-v6 (a) 逐字一致）**：**版本号生成者 = 该档下能保证无丢失单源的那一位：同步档是桥（逐拍无丢失），异步档是框架（合并可丢，须框架单源）**。同步档：`store.subscribe` 逐拍通知无丢失 ⇒ 桥内单调消费代数「本桥见过的第 N 次状态变更」与状态变更一一对应 ⇒ 撕裂结构性不可能；同 store 多通道同源 = 同一桥实例同一代数；跨桥/跨进程不同源，§12。异步档：ADR 0008 消费可合并丢拍 ⇒ 须框架 `state_version` 单源；该档消费入口 API + mount 变体不在 0006 MVP 范围，异步档另立后续。diff 闸与版本号正交：桥内逐绑定值等值比较、派生记忆化用输入等值（§5），两档同形。桥 = 普通订阅者（`store.subscribe` 持票据），票据语义（Drop=注销、must_use）原样继承。
3. **journal feature 的 serde 形态无冲突**：`JsonlJournal<A, S>` 要求 `A/S: Serialize + DeserializeOwned`（行格式对象平铺、`"t"` 保留键）。桥**不要求 `S`/`A` 实现 serde**——投影/意图映射是手写 `Value` 翻译，两 feature 正交：journal 消费 dispatch 侧（JournalPort 包装），桥消费 notify 侧，同 store 可并存互不触达。
4. **性能锚同源三证**：`README.md:144`、`docs/reference.md:201`、`skeleton-infra/src/tracingx.rs:40-44`（2026-10-16 批 C 实测的树内记录；所指 reports/2026-10-16-\* 报告文件已不在工作区，树内三处数字互相咬合，引用以树内为准）：订阅态 dispatch 全含 ~142ns 中订阅分发本体 ~63-72ns、无订阅 ~77ns；`end_to_end_dispatch ≤100ns` 契约锚以无订阅口径定义（`examples/dispatch_profile.rs`）。
5. **〔v6 增补〕审计 F-05 对第 4 项的限定**（架构审计实测）：上锚**只在小 State 下成立**——外部持有 `snapshot()` 者存活时，`Arc::make_mut` 走克隆分支，每次 dispatch 额外一次 `S::clone` 全量深克隆（**在 state 锁内**），1 MiB State 实测 **~210µs/dispatch**（vs 锚 ~80ns，差 ~2600 倍）；且**单纯把 State 包成 `Arc` 无效**（实测比率 1.0x，因 reducer 内部仍克隆整个 `Vec`）。⇒ 本方案的处置：**锚的口径在 §6 诚实化**（M4）、**采用面建议 `State = Arc<Inner>` 且 reducer 内 CoW**、**桥不承诺大 State 下的锚**（桥吃 `&S` 借用、不持快照 ⇒ 桥不制造该成本）。**桥不修改 `Reducer` 契约**——这是采用面写法建议。
6. **〔v6 增补〕审计 F-06 与 ADR 0008 的接口面**：通知在 dispatch 线程同步执行（`skeleton/src/lib.rs:373` 契约③ + `runtime.rs:1168` 调用点）⇒ 桥的投影 + eager 编码成本直接进排空环。**用户已拍板 = 案 C**（新增独立契约端口 `NotifySink`/`Notify`，载荷 = 单调状态版本号，消费侧拉取快照；`Store` 六方法零改动，`subscribe`/`subscribe_local` 一字不改）。⇒ 桥侧三处措辞按已定改写（§3.2-v6 (e)），**桥不预付异步通知面之外的任何新面**（不造第二套通知语义）。

---

## 3. 分层设计（契约 / 机器 / 复用面）

依赖方向单向：`skeleton::view`（零依赖词汇）← `skeleton_infra::view`（机器，`view` feature 门控 serde_json）← 宿主适配层（任意语言边界的 Rust 侧胶水）。std-ext 只被复用不被修改。

### 3.1 skeleton 契约词汇（签名级，零依赖下可编译）

```rust
// skeleton/src/view.rs —— 新域 `view`；词表三件，语义承诺入 doc（同 Reducer 体例）

/// 载荷 schema 标签（v6）：**绑定级**载荷形状的身份号（JSON 通道每补丁项
/// 一枚、区域通道每索引条目一枚）。编号空间分段保留（protobuf reserved 同款
/// 纪律，T1）：0 = **未声明**（读侧按 JSON 自描述处理 = 兼容期最宽松）；
/// 0x0001-0x7FFF = 采用方扩展段（形状版本号，采用方自管）；0x8000-0xFFFF =
/// 框架保留段。**只增不改、退役号不复用**。
/// 注：区域段的**值类型**标签（u64/i64/f64/…）是另一张 u8 空间（§3.6 规范），
/// 二者不混用——前者判「形状变没变」，后者判「字节怎么读」。
pub struct PayloadTag(pub u16);

/// 出站协议版本（v6）：信封级版本号，两通道共用（区域头 / JSON 补丁批各一份）。
/// 读者据此判定「这份数据用的是哪代格式」；**未知版本不整条失败**——逐项按
/// 标签跳过（§3.2 读侧契约 + §3.6 规范）：计数 + 置 resync 提示（禁 panic、
/// 禁整批 Err、禁「解不出就崩」）。
pub const PROTOCOL_VERSION: u16 = 1;

/// 绑定声明：纯数据。路径是**生产方发布的协议常量**，UI 只读消费。
pub struct BindingDecl {
    /// RFC 6901 JSON Pointer 形（如 "/todos"、"/count"）；装配期由桥校验并
    /// 编译为内部索引，运行期不回字符串（§7 划界）。契约：声明后只加不改。
    pub path: &'static str,
    /// v6：绑定级载荷 schema 身份（标签）。0 = 未声明（reader 按 JSON 自描述
    /// 处理 = 兼容期最宽松）；非 0 = 该绑定载荷形状的版本号——**形状变更必须
    /// 换新标签**（旧标签退役不复用）；reader 见未知标签 ⇒ 跳过该项 + 计数 +
    /// 置降级提示（**禁整条失败**，M2 机制，§3.6 读侧协议）。
    pub kind: PayloadTag,
}

/// 视图映射（纯函数契约，采用方按 Feature 实现——与 Reducer 对称）：
/// State → **类型化**视图值集合（按项）。v5 起 ViewMap **只承担投影**——
/// 编码义务整体迁出为可注册编码器组件（§3.1 末段/§3.2；用户澄清 supersedes
/// v4 的 ViewMap::to_wire 形，留档见下段）。
pub trait ViewMap<S> {
    /// 类型化视图词表：**一绑定一变体的采用方 enum**（`enum MyView {
    /// Count(u32), Todos(Vec<TodoRow>) }`）。bound 全部 std 内建 derive
    ///（Clone/PartialEq）可满足——零 proc-macro 零三方；`Send + 'static`
    /// 是跨线程 core 存储（F1）的要求。**bound 只落在本关联类型上，领域
    /// State 零新增 bound**（Druid Data 教训不重演，T2）。异构 View 论证
    /// 同哨兵（见 §3.1 末段）：逐绑定异构类型必须擦除，单关联类型 + enum
    /// 变体零擦除；变体尺寸悬殊时大变体 Box 由采用方自定（布局自由度）。
    type View: Clone + PartialEq + Send + 'static;
    /// 绑定词表：装配期定死；返回序即 BindingId 序（桥装配期对账，错位 =
    /// debug_assert + 计数留痕，fail-visible）。
    fn bindings(&self) -> &[BindingDecl];
    /// 绑定级输入哨兵（L1 记忆化的绑定级应用；机制胞 `Sentinel` 在 infra §3.2，
    /// 与 Derived 同一机制同一条红线）：index 绑定的输入切片是否变更。
    /// **false ⇒ 桥跳过该绑定的单点投影与比较**（承诺：project(index) 输出与
    /// 上拍恒等——「input 覆盖实读」红线，误报仅性能损失，漏报 = 静默陈旧违约）。
    /// 缺省 true = 无哨兵（升级可选、语义不翻面）。
    fn input_changed(&self, index: usize, state: &S) -> bool { let _ = (index, state); true }
    /// 单点投影：只投影 index 一项，产**类型化**视图值（内部无 Value 树）。
    /// 同 state 恒同输出（确定性）；禁 IO/时钟/随机/写共享态（同 reducer 纯度
    /// 纪律）；panic 不跨边界裸传（桥内 catch + 计数 + 毒化闩收口，§3.2/§6）。
    fn project(&self, index: usize, state: &S) -> Self::View;
}

/// 意图映射（纯函数契约）：语言边界词 → 领域 action。
pub trait IntentMap<A> {
    /// 入站载体（infra 以 serde_json::Value 钉死）。
    type Wire;
    /// 翻译失败词表（未知意图/载荷畸形——fail-visible，禁 panic 禁吞）。
    type Err;
    fn intent(&self, wire: Self::Wire) -> Result<A, Self::Err>;
}
```

**为什么进 skeleton 而不是全塞 infra**（备选已否决）：投影/意图映射是采用方每个 Feature 都要写的**采用面纯函数**，与 Reducer 同地位——契约层承载「约束后面代码长法的形状」，机器可换、契约不换（框架哲学本体）。若只做 infra 闭包注册，采用面代码与机器焊死，桥实现失去可替换性；成本仅三个类型，不构成契约通胀。派生不进 skeleton：它是机器侧记忆化胞 + 用户双闭包的组合件（§5），无跨实现互操作需求，进 skeleton 属投机词汇（YAGNI）。

**哨兵为何不是 BindingDecl 字段（修订令点 1「或等价形态」的论证）**：`bindings()` 返回切片，切片元素同型——异构 `In` 塞进字段必须擦除（`Box<dyn Fn>` 抽取 + dyn 等值），Rust 里擦除等值只能 `Any` 下转或装箱比较——**跳过路径（最热路径）反被加上每次装箱/下转的分配与间接**，与哨兵的存在理由自相矛盾。等价形态把**义务**放契约（`input_changed` 方法，可测：投影计数断言）、**机制胞**放 infra（`Sentinel<S, In>` 与 Derived 同型，In 全程具体类型，零擦除零装箱）；`BindingDecl` 因此保持纯数据（仅 path），形状不动。与 Derived 的关系：**同一 L1 输入键等值机制两处应用**——Derived 记「输入→派生输出」，Sentinel 只记「输入」一位结论（退化形：无 Out）；「input 覆盖实读」红线共用一条，§5 的对比与证伪对两者同样生效。

**〔v6 增补·契约面冻结条款（M6）〕**：两条**面向未来的承诺**，写进本契约的 doc 注释（落地即成为骨架 crate 的公开语义）：
- **`ViewMap` 方法集冻结**：v6 之后**不得**给 `ViewMap` 增加新的必实现方法。新能力一律走二选一——（i）**带默认实现的新方法**（语义向后兼容，如 `input_changed` 的缺省 true 形：升级可选、语义不翻面）；或（ii）**独立可选 trait**（采用方按需实现，不实现即零负担）。**理由**：采用方升级成本 = 绑定点数量 × 每绑定点的侵入深度；每次加必实现方法都是全体采用方的一次强制返工。**代价与边界**（诚实记账）：框架侧每个新能力都要设计扩展点，且可选 trait 数量本身会膨胀（§12-14）。**只冻结 `ViewMap`**——`IntentMap`/`JsonEncoder`/`RegionEncoder` **当前不冻结**（尚无第二个实现，冻结为时过早）。
- **bound 永不落在采用方的领域 `S` 上**：框架的 bound 只落在**框架自己的词表**上（`ViewMap::View` 的 `Clone + PartialEq + Send + 'static`、`PayloadTag` 的 `Copy` 等）。**理由**（T2 一手证据）：Druid 的 `Data` bound 被作者本人否定——「app data 必须实现 `Data` ⇒ 强制克隆 + 等值比较 + 事实上禁止内部可变性」，且 Druid 项目已 UNMAINTAINED、继任者 Xilem 改用 plain struct + 普通函数（框架升级不波及采用方状态类型）。**含义**：框架演进不得以「给 `S` 加约束」为代价；`S` 是采用方的领域类型，框架无权改写其形状。**注**：`Reducer::State: Clone` 是**既有 core 契约**（早于本方案，非本方案引入），本方案不改动它——但**新增**能力不得再往 `S` 上叠 bound。

**〔v5 修订留档〕wire 义务两案对比（轮 3 拍板，v5 supersedes）**：轮 3 择「ViewMap 关联 `type Wire` + `to_wire`」对称案，理由 = 与 `IntentMap::Wire` 入站对称、翻译与词表同 impl（穷尽 match 编译期咬住）；否决 mount 注入闭包（翻译与词表分离、「哪个视图配哪个翻译」退化为装配点位置记忆）。v5 用户澄清（编码 = 可组合矩阵）supersedes 该形：`to_wire`/`Wire` 整体迁出 ViewMap 进 `JsonEncoder` 组件（下段）；原对称论证由「两编码器与 View 同型钉死、穷尽 match 编译期咬住」继承，原闭包否决理由由「编码器覆盖语义需独立类型承载」继承并加重。**类型化 View 的异构坑**（v3 发现，仍有效）：v3 的 `View = serde_json::Value` 掩盖了「各绑定 View 类型不同」的问题，单关联类型逼出**采用方 enum 词表**（一绑定一变体）——现在是两编码器穷尽 match 的共同载体，与 Reducer 单 Action enum 同构，非缺陷而是纪律。

**编码器组件矩阵（v5 用户澄清 supersedes「规范形态 + JSON 可选」双轨初形）**：用户拍板原话——两种模式可任选、可同开、可组合，「这是用户自己选的，用户可以选择都开或者只开一种」。契约落点：**ViewMap 只留类型化投影**；编码义务拆为**两个独立编码器组件、mount 注册 0..2 个**：

- `JsonEncoder`（**lazy**：take_patches/snapshot_doc 交付时翻译——拉取频率低时最省）；
- `RegionEncoder`（**eager**：通知路径变更即编入区域——共享区必须始终最新，否则 UI 读到旧值）。

每编码器声明**覆盖**（`Coverage::All | Only(&[路径])`）+ 按变体穷尽分派；桥持注册表，发布面 = **绑定 × 编码器矩阵**（只开一种 / 都开 / 按绑定混搭，采用方自选）。硬约束（v4 教训沿用于新组件）：编码器**不得引入类型擦除**（不许 `Box<dyn Fn>` + Any 下转把编码路径变装箱）——异构 View 沿用 enum 词表按变体分派，桥侧泛型 `Option<J>`/`Option<R>` 单调化分发（零擦除零虚调用，Bevy 静态分发先例 T2）；两编码器 `View` 同型由 mount bound 钉死。绑定已 bind 却不被任何编码器覆盖 = 装配面配置洞 → 首次交付计数留痕 + debug_assert（fail-visible，对账纪律同 v4）。区域格式不进骨架的理由不变（RegionWriter 是 serde_json 味 infra 类型；journal 先例：骨架载端口语义、实现层载具体格式）。

**〔v6 增补·eager 最新性语义的重述与指针〕**：上段「共享区必须始终最新，否则 UI 读到旧值」是 **v5 同步档**的措辞，**原文不动**；但该措辞在 ADR 0008 案 C（异步档）下**不再精确**——投影/编码搬到宿主消费线程后，共享区的新鲜度只到「**至最近一次消费点为止**」。**重述与判据**（据 F-06 三处措辞第 1 项，按已定案 C 写入）：区域头 `version` 即新鲜度判据，UI 凭它判「我读到的是不是当前消费点的值」。**该重述的完整形态、版本同源要求与线程归属改写见 §3.2-v6 (e)**。

### 3.2 skeleton-infra 机器（签名级，feature `view = ["dep:serde_json", "udf-std-ext/json"]`）

```rust
// skeleton-infra/src/view.rs —— 数据走向见 §3.4；错误类型手写 Display/Error（journal.rs 先例）
//
// 线程模型（F1）：通知体在 dispatch 线程**同步**执行（skeleton/src/lib.rs:373-429
// `Store` 契约③「回调在 dispatch 线程同步执行」（:373）+ `subscribe` 签名（:429）；通知段调用点
// runtime.rs:1081 → notify_snapshot_consumers:1168），而任意语言宿主的 pull 侧必然
// 跨线程——桥内态因此**显式同步**，`ViewBridge: Send + Sync`，句柄可克隆分持
// 通知侧与宿主侧。
pub struct ViewBridge<S, A, M, I, J, R> {
    /* 订阅票据（归桥，Drop=注销）
     + 绑定表：装配期填、首通知封板（Arc<[Binding]>；封板后只读，通知体逐拍无锁读）
     + core: Mutex<BridgeCore>（last_values: Vec<M::View> 类型槽 + dirty 位图
       + 毒化位——单锁成组；latest-wins 折叠 = 置脏，无独立出站箱，O(绑定数) 恒定）
     + region: Mutex<Option<RegionChannel>>（v5 eager 区域通道：背衬 + arena 追加
       游标 + region_id + 头版本写点；单写者 = 排空环，读侧走头内版本原子）
     + json: Option<J>（lazy JSON 编码器，take_patches/snapshot_doc 调用）
     + projection_panics / projection_runs / region_encodes: AtomicU64（累计只增）
     + sealed: AtomicBool（绑定表封板闸） */
}
// 同步形态选择（Mutex vs 原子分段，F1 要求写明理由）：出站箱/上次值/毒化位必须
// **成组一致迁移**——snapshot_doc 的重同步一次动三者，原子分段做不成组迁移且会把
// 一致性推给调用方；竞争方只有两个（dispatch 线程通知体推补丁、pull 侧排空/重同步），
// 临界区皆为短批操作——单把粗锁是最小正确解。持锁窗口如实声明：通知体在锁内一次完成
// 类型 eq + 移槽置脏（O(Σ哨兵变更项) 比较 + O(值变更) 搬移），投影在锁外，与 pull
// 侧互斥但**不与 reduce/排空环互斥**（锁只归桥，dispatch 主体不触）；绑定表封板后
// 只读，才是真无锁。
//
// 投影管线（v4 类型化载体）：每通知逐【已绑定】项——
//   ① 哨兵（Sentinel::changed：廉价抽取+eq）命中 ⇒ 跳过（零投影零比较零分配）；
//   ② 变更 ⇒ 单点 project 产**类型化** View（**锁外**纯函数）→ 类型 PartialEq（细闸）
//   → 变更者移入 last_values 槽 + 置 dirty 位（**latest-wins 折叠**：中间变更自动
//   合并，UI 只取每路径现值——无箱可溢，无丢弃计数）。
// 哨兵粗闸（跳投影）在前，类型 eq 细闸（跳补丁）在后：粗闸漏报 = 红线违约（静默
// 陈旧），粗闸误报仅浪费一次投影。三态提交的唯一缺口 = project panic（哨兵胞已
// 提交而 last/dirty 未更新）：catch + projection_panics 计数 + 置毒化闩，由
// PatchBatch.resync 位统一收口到 snapshot_doc 现态全量重投影——哨兵缓存与
// last_values 不可能静默分叉（重同步以现态为准重建，哨兵旁路）。
// **JSON 通道（lazy）**：出站翻译只在交付点——take_patches/snapshot_doc 对脏
// 路径调 to_json → Value 补丁（冷路径）；每通知零 Value 构造零树分配。
// **区域通道（v5，eager）**：通知路径对变更绑定**变更即编**——RegionEncoder 展平
// 追加进背衬 arena（批内一次 with_bytes_mut 回调，单写者 = 排空环保证），段写毕
// 更新该绑定 Index 条目、Header.version += 1（发布点）——共享区始终最新，无发布
// 调用无水线；每通知增量 = 变更绑定的编码 + 一次版本原子写。两通道成本独立可测
//（projection/encode 计数分读侧），**都开 = 两条都付**（采用方选择，非隐藏税，§6）。
// write_wire（JSON 直写字节）留位不预付；区域格式规范见 §3.6。

impl<..> ViewBridge<..>
where M: ViewMap<S> + Send + Sync + 'static,
      M::View: Clone + PartialEq + Send + 'static,
      J: JsonEncoder<View = M::View> + Send + Sync + 'static,   // lazy JSON 通道（Option 可关）
      R: RegionEncoder<View = M::View> + Send + Sync + 'static, // eager 区域通道（Option 可关）
      I: IntentMap<A, Wire = serde_json::Value> + Send + Sync + 'static,
{
    /// 装配：store.subscribe 挂桥（票据归桥所有，桥 Drop = 注销）。
    /// encoders = 编码矩阵注册表：`Encoders { json: Option<J>,
    /// region: Option<(R, Arc<dyn RegionBacking>)> }`——只开一种 / 都开 /
    /// 按 Coverage 混搭，采用方自选（泛型 Option 单调化，零类型擦除）。
    /// 线程归属：装配线程调用（与 bind 同线程）；装配后句柄可克隆跨线程。
    pub fn mount(store: &(impl skeleton::Store<State = S, Action = A>),
                 map: M, intents: I, encoders: Encoders<J, R>) -> Self;

    /// 语言边界出站面 ─────────────────────────────
    /// 绑定注册（**装配期专用**）：路径校验（未知路径 Err，fail-visible）→ 内部索引句柄。
    /// 首个通知即封板：封板后调用显式 `Err(BindError::Sealed)`——封板保证绑定表
    /// 只读无锁；运行期动态注册随行级通配二期另议（§12-1 断点）。
    /// 封板前窗口的并发纪律（v6.1 钉死，上轮 R1）：bind 的 sealed 检查与登记走
    /// core 锁（与通知体同一把桥锁）；**首通知前 `send_intent` 未定义**——装配纪律
    /// 为「mount → 全部 bind 完成后才允许 dispatch」，装配未完成即发意图 = 装配面 bug，
    /// 不在桥的保证面内（封版后改动一律走 ADR 修订）。
    pub fn bind(&self, path: &str) -> Result<BindingId, BindError>;
    /// 重同步原语：**现态全量重投影 + 逐项 to_wire 组装全量文档**（不信缓存、
    /// 哨兵旁路，O(view) 一次性）+ 锁内一次完成「换 last_values + 清 dirty 位 +
    /// 复位毒化」（§6 状态机的毒化→正常迁移边）。以现态为准是哨兵形的必然：
    /// 重同步正是哨兵缓存可能失真的收口点，不能拿可能失真的 last_values 当真相。
    pub fn snapshot_doc(&self) -> serde_json::Value;
    /// 出站批拉取（`&self`：与通知体跨线程共存，宿主任意线程可调）。
    /// **翻译发生在这里**（冷路径）：对脏路径逐个 `to_wire` → Value 补丁，取
    /// last 现值（latest-wins：同路径多次变更一次交付现值），清脏位。
    /// 返回批携带完整性事实：`resync == true` ⇒ 本批之后流不可信，UI 必须
    /// snapshot_doc 重同步——检测义务在返回值里，不靠文档自觉。
    pub fn take_patches(&self) -> PatchBatch;

    /// 语言边界入站面 ─────────────────────────────
    /// 意图翻译 + dispatch（任意线程；ActionSender 本就 Send+Sync）。
    /// Err = 未知意图/载荷畸形；Ok(false) = store 已闭门
    ///（ActionSender must_use 语义透传，停机显式化不折损）。
    /// 入站保持 Value/&str（轮 3 拍板）：意图频率 ≤ 人交互速率，单次解析+丢弃
    /// 的盒子税在此数量级可忽略——类型化收益集中在每 action 必走的通知路径。
    pub fn send_intent(&self, intent: serde_json::Value) -> Result<bool, IntentError>;

    /// 可插拔编码层（v5 矩阵）──────────────────────────
    /// 区域通道为 **eager**：无发布调用——通知路径变更即编，共享区始终最新。
    /// 读侧（背衬映射方直接读内存头，不过桥 API；桥面只留计数与身份读侧）：
    /// UI 以 version 判新旧、以 region_id 变化识别重映射握手（§3.6 规范）。
    pub fn region_version(&self) -> u64;   // 头版本（未挂区域通道 = 恒 0）
    pub fn region_id(&self) -> u32;        // 换区握手：id 变化 = UI 重映射
    pub fn region_encodes(&self) -> u64;   // eager 编码累计（成本可见，§6/§12-10）

    /// 读侧计数（**累计**，只增不减；对齐 subscriber_panics 先例：让降级显形）。
    // projection_runs 选型（轮 2 令点 2）：**单只累计**，不选逐绑定——逐绑定
    // 计数 = 每通知 O(绑定数) 原子写加热路径，而读侧消费者（断言/降级显形）
    // 只需要总量；「哪个哨兵失效」的定位维度归 multidim 采集器（memgov 先例：
    // 测量基建不进产品面）。
    // 注（v4）：`patches_dropped()` 随 latest-wins 折叠**退役**——折叠只合并不
    // 丢弃，溢出结构性不可能（O(绑定数) 恒定槽位）；F2 的「丢弃计数」职责消灭，
    // 完整性事实由 `resync` 位独担（论证见 §6 状态机 / §12-6）。
    pub fn projection_panics(&self) -> u64;    // 投影 panic 跳过累计（因读）
    pub fn projection_runs(&self) -> u64;      // 单点投影累计次数（哨兵命中⇒不增）
}

pub struct Patch {
    pub path: &'static str,        // 声明静态串零拷贝（行级实例路径二期需换形，§12-1 断点）
    pub kind: PayloadTag,          // v6：绑定级载荷 schema 标签（形状身份号；0=未声明）
    pub value: serde_json::Value,  // replace 语义（RFC 6902 replace 子集）；**交付点由 to_wire 产出**
}

pub struct PatchBatch {
    pub patches: Vec<Patch>,       // replace 补丁批（每脏路径一条、取现值；latest-wins）
    pub resync: bool,              // 流完整性可疑（唯一因：投影 panic 毒化）⇒ snapshot_doc
    // v4 退役字段：dropped_since_last——折叠只合并不丢弃（PM 轮 3 内核点 3），
    // 语义演化论证见 §3.2 / §6 / §12-6。
    // ── v6 水位（与区域头 version 同源，见「版本序列单源」段）────────────────
    /// 本批**起**版本：上一次交付的 to_version（= UI 侧已应用版本的期望值）。
    /// UI 侧 `applied != from_version` ⇒ 缺段 ⇒ 走 snapshot_doc 重同步。
    pub from_version: u64,
    /// 本批**止**版本：本次交付所反映的状态版本（= 区域头 version 同源计数）。
    pub to_version: u64,
    /// 信封协议版本（v6）：读侧凭它判定格式代次；未知版本 ⇒ 逐项按 kind 跳过
    /// + 计数 + 置 resync 提示，**禁整批失败**。
    pub protocol_version: u16,
}

/// 重同步快照（v6：resync 原语返回值升为「文档 + 同源版本」——机制不变，
/// 只多带一个版本号，使 UI 侧能重建 applied 基线，见 §3.2 水位段）。
pub struct Snapshot {
    pub version: u64,              // 与 doc 同源的版本号（取自同一 state_version）
    pub doc: serde_json::Value,
}

/// 派生组合件（§5 推荐 L1）：记忆化胞，ViewMap 内持字段使用。
pub struct Derived<S, In, Out> { /* Mutex<Option<(In, Out)>> + input/compute 双纯闭包 */ }
impl<S, In: PartialEq, Out: Clone> Derived<S, In, Out> {
    pub fn new(input: impl Fn(&S) -> In + Send + Sync + 'static,
               compute: impl Fn(&In) -> Out + Send + Sync + 'static) -> Self;
    /// input(state) 等值于缓存 ⇒ 返回缓存（零重算）；否则重算并换缓存。
    pub fn get(&self, state: &S) -> Out;
}
// v4 确认：Derived::Out 本来就是用户类型，载体改形零影响——且更顺：派生输出可
// 直接作为 View 变体载荷，不再被迫中转 Value（省一次翻译一次分配）。

/// 绑定级输入哨兵（轮 2）：Derived 的退化形（同型 Mutex<Option<In>> 胞，
/// 只记输入不记输出）——ViewMap 内持字段，`input_changed` 逐项转发。
pub struct Sentinel<S, In> { /* Mutex<Option<In>> + extract 纯闭包 */ }
impl<S, In: PartialEq> Sentinel<S, In> {
    pub fn new(extract: impl Fn(&S) -> In + Send + Sync + 'static) -> Self;
    /// 抽取+等值；变更 ⇒ 换缓存返 true（桥通知路径专用调用方——胞锁恒为
    /// 桥 core 锁的外层短叶锁，先取先放，与 core 锁零嵌套，无锁序面）。
    /// In 抽取纪律：零分配或近零（标量/Copy/短栈结构）——抽取即残余成本（§12-3）；
    /// 抽不廉价的绑定不设哨兵（缺省 true 形态），禁为设哨兵而哈希指纹
    ///（碰撞 = 漏报 = 静默陈旧，§5 否决依赖追踪同理）。
    pub fn changed(&self, state: &S) -> bool;
}

/// 编码器组件（v5 矩阵；infra trait——区域格式是实现层规范，与 journal 行格式
/// 同地位：骨架载端口语义、实现层载具体格式，§3.1 论证）。两 trait 的 View
/// 同型由 mount bound 钉死；穷尽 match 变体 + 全字段红线两形共用（§12-7/§12-9）。
/// lazy 通道：take_patches/snapshot_doc 交付时翻译。
pub trait JsonEncoder {
    type View;
    fn covers(&self) -> Coverage;
    fn to_json(&self, view: &Self::View) -> serde_json::Value;   // Wire=Value 桥 bound 钉死
}
/// eager 通道：通知路径变更即编（共享区始终最新；单写者 = 排空环保证）。
pub trait RegionEncoder {
    type View;
    fn covers(&self) -> Coverage;
    fn encode(&self, view: &Self::View, w: &mut RegionWriter);
}
/// 覆盖语义：All = 全部已绑定项；Only = 路径清单（装配期校验，未知路径 Err）。
/// v6：公开词表标 `#[non_exhaustive]`（下游须带通配臂 ⇒ 未来加变体非破坏演进，
/// 同 `skeleton/src/parse.rs` 三处既有纪律；见 §10 决策⑫）。
#[non_exhaustive]
pub enum Coverage { All, Only(&'static [&'static str]) }
/// 区域写入器（机制件）：tag 原语 + arena 追加游标 + 8 字节对齐填充。零 unsafe 纯 std。
pub struct RegionWriter { /* arena 追加游标（经背衬回调写） */ }
impl RegionWriter {
    pub fn u64(&mut self, v: u64);      // tag 0
    pub fn i64(&mut self, v: i64);      // tag 1
    pub fn f64(&mut self, v: f64);      // tag 2（IEEE754 LE）
    pub fn utf8(&mut self, s: &str);    // tag 3（len:u32 + UTF-8）
    pub fn bytes(&mut self, b: &[u8]);  // tag 4（len:u32 + raw）
    pub fn json_blob(&mut self, v: &serde_json::Value); // tag 5（len:u32 + 紧凑 JSON UTF-8）
}
/// 区域背衬（v5）：固定容量、地址稳定的共享内存抽象——框架经回调安全写入，
/// **零 unsafe 零裸指针留在框架侧**（宿主实现自持 SAB/shm/Vec，unsafe 归宿主
/// 实现内部，与 v4 入站方向同纪律）。固定容量是共享映射地址稳定的硬前提。
pub trait RegionBacking: Send + Sync + 'static {
    fn len(&self) -> usize;
    /// 框架唯一写入口：把 &mut [u8] 借给回调（宿主实现内自理锁/Atomic）。
    fn with_bytes_mut(&self, write: &mut dyn FnMut(&mut [u8]));
}
/// 默认背衬：框架自备固定容量 Vec（拷贝出站姿态；宿主经读侧 API 拷走）。
pub struct VecBacking { /* 固定容量 Vec<u8> + Mutex */ }
```

#### 3.2-v6 增补（踩平轮，v6.1 随复审修订）：版本来源两档分写 · 出站水位 · 未知标签跳过 · ADR 0008 案 C 形态（预记）

> **编号消歧（v6）**：本节及全文的 **M1–M6** 指本轮「踩平轮」的六项机制（版本水位 M1 / 未知标签兜底 M2 / 命名工件 M3 / 锚口径诚实化 M4 / 通知线程成本 M5 / 契约冻结与 non_exhaustive M6）。**外部扫描报告**（`reports/2026-10-27-mvi-mvvm-coupling-pitfalls.md`）里的 **M1–M8** 是另一套编号（业界「踩平机制清单」），**异步设计稿 §7** 引用的 M1/M5 指后者——跨文档引用一律写全称（「扫描报告 M5 = 事件规约成状态」/「本轮 M5 = 通知线程成本」），避免同号异义。

> **两档并存声明（v6，v6.1 随 Major 1 定案更新状态）**：F1 并发原文（v5）描述的是**同步档**——桥自挂 `store.subscribe`，通知体在 dispatch 线程同步执行（该段原文一字不动，仍然成立）。ADR 0008（**已定：案 C，已采纳（未实现）**）落地后，框架侧多出**异步档**（`NotifySink`/`Notify`，载荷 = 单调状态版本号），桥在**异步档**下的形态见下段 (e)——**该档的消费入口 API + mount 变体不在 0006 MVP 范围，另立后续**（§10 记接口依赖）。两档的投影管线、diff 闸、latest-wins 折叠、resync 闭环**完全共用**——档位只改「谁在哪个线程触发投影」与「版本号从哪来（§3.2-v6 (a) 两档分写）」两件事，**不改机制**。

**(a) 版本来源两档分写（v6.1 定案，PM 裁决；硬要求：每档一个能保证无丢失的单源）**

**统一语义（§2-2 同文逐字一致）**：**版本号生成者 = 该档下能保证无丢失单源的那一位：同步档是桥（逐拍无丢失），异步档是框架（合并可丢，须框架单源）**。

- **同步档（ADR 0006 MVP 唯一范围）——出站版本 = 桥内单调消费代数**。语义：**「本桥见过的第 N 次状态变更」**。依据：同步通知逐拍无丢失（`store.subscribe` 每 action 一拍携带新 `Arc<S>`，§2-2）⇒ 桥内代数与状态变更**一一对应** ⇒ 版本比较即状态变更比较，**撕裂结构性不可能**（同一桥的两个通道头取同一代数；§8 水位断言在同步档**全部可执行**）。桥 = 该代数唯一写者：投影体每消费一拍 +1（`u64` 单调不回退）。
  - **同 store 多通道仍同源**：区域通道 `Header.version` 与 JSON 通道 `PatchBatch.to_version` 取**同一个**桥内代数（同一桥实例同一代数）⇒ **两通道版本可直接互相比较**（跨通道撕裂检测的唯一依据；v5 两通道版本语义不同（区域=发布计数、JSON=无版本）⇒ 无法比较，v6 补齐）。
  - **跨桥/跨进程不同源（诚实边界）**：两个桥实例各自计代数，互不可比——跨桥/跨进程撕裂判定**不在本方案承诺内**（写入 §12-12）。
- **异步档（ADR 0008 案 C，已采纳（未实现））——切框架 `state_version` 同源**。异步档消费可合并丢拍（ADR 0008 §5.2 门控语义：零异步订阅者时不自增、订阅者可整拍未消费）⇒ 桥内代数不再与状态变更一一对应 ⇒ 版本必须由**框架**生成（快照落位时 +1 的 `state_version`），桥只读取并复用到两个通道头——**异步档桥不自造版本号**（自造 = 与框架版本分裂 ⇒ 撕裂判定失真，异步设计稿 R2 原文：单一来源）。**该档的消费入口 API（`subscribe_notify` 消费形态）+ mount 变体不在 0006 MVP 范围**：§10 记对 ADR 0008 的接口依赖，**异步档桥形态另立后续方案**——(e) 的三处措辞重述为该后续的预记约束，非 MVP 交付面。
- 桥内另有 `delivered_epoch`（上一次 JSON 交付的 `to_version`）——它是**交付游标**，不是版本源：**不进区域头、不参与撕裂比较**，只用于出站批的 `from_version`。**快照即交付（补审 N2）**：`snapshot_doc()` 与 `take_patches()` 同为交付点——快照交付时 `delivered_epoch` **推进至当前消费代数**（否则重同步后下一批 `from_version` 停在旧位 ⇒ UI 侧必判假缺段）；对应可执行断言 = 快照重同步后 `from_version == snapshot.version`（§8 断言链①）。
- 单调性（两档同形）：所见版本 < 已消费版本 ⇒ 丢弃该次消费并计数（同步档结构性不发生，防御性检查；异步档防御消费乱序）；版本跳跃 >1 ⇒ 同步档不发生（逐拍 +1）、异步档允许（消费合并），JSON 批的 `from/to` 如实标出跳跃，UI 侧据此判缺段（见 (b)）。
- **配对要求随两档分写收窄**：`(state, version)` 同拍配对问题**只存在于异步档**（框架须保证版本化读面或保守配对，ADR 0008 定稿载荷面 = `publish(version)` + 既有 `snapshot()`，无版本化快照读面 ⇒ 保守配对为唯一路径，§12-12 标「现行」）；同步档版本在桥内自产自销（代数随消费推进，与所消费的 state 同拍落槽），**无跨面配对问题**。异步档的配对与接口依赖随「异步档另立后续」移入 §10 记账。

**(b) UI 侧应用算法（规范伪码；两通道同源 ⇒ 可跨通道比对）**
```
# UI 侧持：applied_version（已应用到的出站版本；初值 = 首次 snapshot 的 version）
# 桥侧持：view_version（出站版本：同步档 = 桥内消费代数，异步档 = 框架 state_version）、
#         delivered_epoch（上批 to_version）

loop:
    batch = bridge.take_patches()
    if batch.resync:                       # 流完整性可疑（投影 panic 毒化）
        snap = bridge.snapshot_doc()       # 现态全量重投影（含 version）
        ui.replace_all(snap.doc); applied_version = snap.version; continue
    if batch.to_version < applied_version:      # 倒退：不该发生 ⇒ 判失配
        snap = bridge.snapshot_doc(); ui.replace_all(snap.doc)
        applied_version = snap.version; continue
    if batch.from_version != applied_version:   # 缺段：漏批/并发消费/中途失败
        snap = bridge.snapshot_doc(); ui.replace_all(snap.doc)
        applied_version = snap.version; continue
    for p in batch.patches: ui.apply(p)         # replace 语义，逐路径幂等
    applied_version = batch.to_version

# 跨通道撕裂检查（区域 + JSON 同开时）：
region_v = region.header.version
if region_v != applied_version:            # 两通道不同拍 ⇒ 视图撕裂
    re-read region (共享区读侧按 §3.6 协议重读)  # 以较旧者为基线；不得混用两拍的值
```
依据：React `useSyncExternalStore`（T1）的快照一致性契约——「渲染期间 store 变了就重跑」；本方案把它落成**版本比对 + 重同步**的可执行判据（对照本仓 F2 的 `resync` 位：完整性事实必达 UI，由返回值携带，不靠文档自觉）。

**(c) 出站水位字段语义**：`PatchBatch { from_version, to_version, protocol_version }`——`from_version` = 上一次交付的 `to_version`（= UI 应处的位置）；`to_version` = 本批内容对应的状态版本；`protocol_version` = 信封格式代次。`Snapshot { version, doc }`——resync 原语的返回值多带一个同源版本号，使 UI 能重建 `applied_version` 基线（机制不变，只多一个数）。

**(d) 未知标签跳过（协议兼容期不整批失败）**——「只加不改」从**文档纪律**升为**机制**：
- **信封版本位**：JSON 批级 `protocol_version`、区域头级 `protocol_version`（各通道一份）。
- **逐项标签**：JSON 批每项带 `kind`（绑定级载荷 schema 身份号）；区域索引条目带 `kind`；区域**值类型**段改 **TLV 长度前缀**（`tag:u8 | len:u32 | payload`）——长度前缀是「旧读侧能跳过未知 tag」的**前提**（无长度则未知 tag 长度不可知 ⇒ 无法安全跳过；protobuf TLV 同款机制，T1）。
- **读侧契约（两通道同款，规范强制）**：遇未知 `protocol_version` ⇒ 逐项按 `kind` 判定；遇未知 `kind` ⇒ **跳过该项**（不整条失败）、计数 `unknown_tags_skipped`、置 `needs_resync` 提示（宿主据此做全量重取）。**禁 panic、禁整批 Err、禁「解不出就崩」**。
- **框架层可踩平的部分**：框架**提供读侧参考实现**（骨架外的 infra 侧：区域 Rust 参考读器 + JSON 解码助手，见 §3.2-v6 签名块）+ 计数读侧；TS/Python/C 镜像读器按同一语义实现（规范原文即契约）。**未知标签的判定权在读侧**（谁更懂自己的 schema 谁判）——框架不替 UI 猜形状，只保证「遇到不认识的不炸」。

**(e) ADR 0008（已采纳（未实现），案 C）下的桥形态（F-06 三处措辞，按已定写入；v6.1 注：本段为异步档桥的**预记约束**——该档消费入口 API + mount 变体不在 0006 MVP 范围，随 ADR 0008 实现**另立后续方案**兑现，(a) 两档分写为准）**
1. **eager 最新性语义重述**：v5 措辞「共享区始终最新，否则 UI 读到旧值」**改为**「**至最近一次消费点为止**的最新」——异步档下投影/编码在宿主消费线程发生，共享区的新鲜度以区域头 `version` 为准，UI 凭它判新鲜度（版本进头 = 判据进协议，不靠约定）。
2. **两处版本号同源**：桥的 `PatchBatch` 版本（(a) 异步档分支的 `view_version`，取自框架 `state_version`）与异步档 `state_version` **必须同源**（谁生成谁、谁读谁：框架生成 `state_version`，桥读取并复用到两个通道头）——否则撕裂判定失真。
3. **通知线程归属改写**：异步档下桥的**投影从「dispatch 线程内」变为「宿主消费线程内」**；core 锁的竞争方从「dispatch 线程 vs pull 线程」变为「**消费线程 vs pull 线程**」。锁形态与持锁窗口声明随之改写：投影（`project` 纯函数）仍在锁外；锁内仍只做「类型 eq + 移槽置脏 + 版本推进」这一组短批操作（O(Σ哨兵变更项) 比较 + O(值变更) 搬移），**不与 reduce/排空环互斥**（锁只归桥）；绑定表封板后只读。**毒化归属（ADR 0008 §6.3 子点，v6.1 补记）**：桥的投影 panic 毒化位仍在桥内，但**毒化发生在消费线程**——`projection_panics` 计数与 `resync` 重同步收口均在消费线程视角下成立（同步档毒化则在 dispatch 线程，两档语义同形、线程归属随档）。**状态：F-06 已拍板 = 案 C（ADR 0008 已采纳（未实现））** ⇒ 按已定落笔；v5 同步档形态保留为「同步档」分支（两档并存，机制共用，见本节开头声明）。
4. **本方案不解决的核心问题（诚实声明）**：F-06 的本体是「通知在 dispatch 线程同步执行 ⇒ 慢订阅者阻塞全局状态机」——桥只是**该本体的一个受害者**。桥侧的三处改动（最新性重述 / 版本同源 / 线程归属）**不修**核心问题；核心修复由 ADR 0008 承担（异步档把重活移出排空环），**桥不预付异步通知面之外的任何新面**（如「订阅回调耗时」读侧计数属二期，见异步设计稿 R5）。

**(f) feature 门形态（依 ADR 0007 §4.2-5 跨 crate 开门，已实测）**：`view = ["dep:serde_json", "udf-std-ext/json"]`——开则 `serde_json` 进 infra 树、`std-ext` 的 json 域可用（入站解码面 + wire 的 `serde_json::Value`）；关则两者都不进（**默认树仍零三方**，ADR 0007 口径）。承重假设：view 门开时 `serde_json` 按需进 infra 树，默认树仍零三方。

```rust
// ── v6 读侧参考实现（infra 侧，feature view；零新依赖、零 unsafe）────────
// 目的：把「未知标签跳过 + 计数 + resync 提示」的语义**实现一次**，
// TS/Python/C 镜像读器按同一语义复刻（规范原文即契约）。
pub mod read {
    /// 区域读侧参考读器：按 §3.6 规范解一段共享区。
    pub struct RegionReader { /* 解析游标 + 计数 */ }
    impl RegionReader {
        /// 读一区；遇未知 tag/kind ⇒ 跳过该项（TLV 长度前缀使跳过恒可能）
        /// + 计数 + 置 needs_resync；**禁整区失败**。
        pub fn read(bytes: &[u8]) -> RegionRead;
    }
    pub struct RegionRead {
        pub values: Vec<(BindingId, serde_json::Value)>, // 已解出的绑定值（缺失者缺席）
        pub unknown_tags_skipped: u64,   // 读侧计数（累计只增，降级显形）
        pub needs_resync: bool,           // 任一未知标签 ⇒ 宿主应做全量重取
    }
    /// JSON 通道解码助手：未知 kind 的项跳过 + 计数 + resync 提示。
    pub fn decode_batch(batch_json: &serde_json::Value) -> JsonBatchRead;
    pub struct JsonBatchRead {
        pub patches: Vec<Patch>,
        pub unknown_tags_skipped: u64,
        pub needs_resync: bool,
    }
}
```

### 3.3 std-ext 复用面（只复用，不改动）

| 复用件 | 落点 | 白得的东西 |
|---|---|---|
| `std_ext::json::decode_str(s, Strict)` | `send_intent_str(&str)` 入站重载（宿主拿到的是 wire 文本时） | RFC 8259 严格 + 64 MiB 上限 + 行列错误，纪律免建 |
| serde_json `Value` 原生 `Serialize` | 出站序列化（`to_string`/宿主自选格式） | 零 derive、零新依赖（§2.1） |
| serde_json `Value::pointer/pointer_mut` | 装配期路径校验的参照语义（桥内编译为段索引，热路径不用） | RFC 6901 寻址语义官方实现对表（T1） |

### 3.4 数据走向（一段话可复述）

```
UI(任意语言) ──intent JSON──▶ send_intent ──▶ IntentMap(纯) ──▶ store.dispatch
store 排空环: reduce → 快照落位 → 效果转发 → notify ──▶ 桥订阅体(普通订阅者):
    逐【已绑定】项: 哨兵(L1; 未变⇒跳过) → 单点投影(类型化 View, 锁外) → 类型eq
    → 变更者移入 last 槽 + 置脏位(latest-wins 折叠; O(绑定数) 恒定, 无箱可溢)
UI ◀──take_patches() 脏路径 to_json→Value 补丁批(冷路径交付, lazy)──┘
    └─ v6: 批携带 (from_version, to_version, protocol_version) + 每项 kind
编码矩阵(v5): JsonEncoder(lazy, 上行) × RegionEncoder(eager, 变更即编入固定容量背衬区,
  单写者; version/region_id 在头, UI 判新旧与重映射) → 宿主投递(FFI/SAB/shm/网络)
    └─ v6.1: 头 version = 出站版本（同步档 = 桥内消费代数，与 JSON 批 to_version
         同源同值；异步档 = 框架 state_version，另立后续）; 段改 TLV;
         索引条目带 kind; 头带 protocol_version
初挂/失同步 ◀── snapshot_doc() 现态全量重投影 + to_json 组装（v6: 附带 version）
命名工件(v6): 清单(真源) ──生成──▶ TS/Python/C 常量文件 ──▶ UI 侧引用常量(非字面量)
```

### 3.5 可替换组件留位（契约不换，机器可换）

- **值载体**：`ViewMap::View`/`IntentMap::Wire` 泛型钉在桥的 impl bound 上——换 wire 格式（MessagePack 等）= 新增一个桥实例化，采用面词汇不动。
- **出站推送形态**：MVP 纯拉取（take_patches）；宿主要推（如 Tauri emit）在宿主适配层轮询或包回调，桥内 push-sink 留位不预付（YAGNI）。
- **wire 直写**：区域发布（v5）已兑现字节级出站主形态；`write_wire`（JSON 直写字节流，跳 Value 中转）降为 JSON 通道的小留位不预付。
- **投影粒度**：轮 2 起投影即按项调用（哨兵跳过的前提），且只有**已绑定**项进哨兵扫描；进一步的「运行期加绑/解封板」仍属二期通配断点（§12-1）留位，契约签名不动。

### 3.6 区域线格式规范 v1（跨语言唯一权威；实现期落 `docs/view-region-spec.md` + 镜像读取器，ADR 0006 引用）

目标读者验收线：**TS/Python/C 各 ~20 行可写只读解析器**（§8 镜像读取器即按此验收）。

**v6 变更（相对 v0）**：① 段改 **TLV 长度前缀**（未知 tag 可跳过——v0 的定长 tag-value 序列在遇未知 tag 时长度不可知 ⇒ 旧读侧只能失败，与 M2「禁整批失败」冲突）；② 索引条目增 `kind`（绑定级载荷 schema 身份号）+ 头增 `protocol_version`；③ 头 `version` 语义由「发布计数」改为「**出站版本**」（v6.1：同步档 = 桥内消费代数，与 JSON 通道同源，见 §3.2-v6 (a)；异步档 = 框架 state_version，另立后续）；④ 新增「未知 tag/kind 跳过路径」读侧契约。

```
Region（固定容量、地址稳定、单写者=桥消费点、多读者=UI；背衬宿主注册或框架默认 Vec）
  Header  : "UVBR"(4B) | protocol_version:u16(LE) | region_id:u32(LE) | version:u64(LE)
            | binding_count:u32 | index_offset:u64
  Index   : binding_count × { binding_id:u32 | kind:u16(LE) | offset:u64 | len:u32 | gen:u32 }
            // 条目地址恒定；kind = 绑定级载荷 schema 身份号（0=未声明）
  Arena   : 段数据追加区；段 = **TLV**（长度前缀使未知 tag 可跳过）：
            tag:u8 | len:u32(LE) | payload(len 字节)
            tag 空间（v6 版本化，只增不改、退役号不复用；与 §3.1 PayloadTag 是
            **两张不同的空间**——本表判「字节怎么读」，PayloadTag 判「形状变没变」）：
            0=u64 | 1=i64 | 2=f64(IEEE754 LE) | 3=utf8 | 4=bytes | 5=json_blob
            | 6=null；0x07-0x7F 保留（框架扩展）；0x80-0xFF 保留（采用方扩展）
写侧协议（eager，桥消费点）：变更绑定 → encode 追加进 Arena（TLV 写段）→ 更新该绑定
  Index 条目（offset/len/gen+1/kind）→ Header.version = 出站版本（v6.1：同步档 = 桥内
  消费代数，与 JSON 批 to_version 同源同值，见 §3.2-v6 (a)；发布后写）。
  binding_id = BindingId（装配期定，封板保证稳定）。
读侧协议（~20 行）：读 Header.version → 读 Index 条目 → 按 offset/len 读段 → 复读条目
  gen 与 Header.version，任一变化则重读（无锁重试，读者不被写者阻塞；wasm 需 atomics
  供 u64 版本单字原子读——与 SAB 前提同列）。
**未知标签跳过路径（M2 机制，规范强制）**：
  ① 遇未知 `protocol_version` ⇒ **不整区失败**：按条目 `kind` 与段 `tag` 逐项判定，
     能解的解、不能解的**跳过**；② 遇未知段 `tag` ⇒ 按 TLV `len` **跳过该段**
     （长度前缀是跳过的前提）；③ 遇未知条目 `kind` ⇒ 跳过该条目的整段；
     ④ 每跳过一次 ⇒ 计数 `unknown_tags_skipped` + 置 `needs_resync` 提示（宿主据此
     做全量重取）；⑤ **禁 panic、禁整区 Err**（「解不出就崩」不是可接受行为）。
  判据与语义由 §3.2-v6 (d) 的框架读侧参考实现给出（Rust 参考读器 + 计数读侧），
  TS/Python/C 镜像读器按同一语义复刻。
容量策略：背衬容量 = 预留上界（宿主定，默认 64 KiB）；Arena 耗尽/水位超限 ⇒ **换新区域**：
  新 region_id + 新背衬 + 版本延续（出站版本单调不回退）+ UI 重映射握手（读侧见
  region_id 变化即重映射）。**禁原地增长**（SAB memory.grow 会 detach JS 侧
  ArrayBuffer 视图——地址稳定是共享映射硬前提，§12-8）；旧区域内容随旧背衬持有者存活。
全域字节序 LE；offset 8 字节对齐（填充 0，读侧免对齐陷阱）；字符串 UTF-8；复杂嵌套一律
json_blob（与 JsonEncoder 同源取值——两编码语义一致性的机制保证，§12-9）。
**信封骨架布局冻结（v6.1 补审项）**：Header/Index 的字段布局（字段序、宽度、LE）跨版本
冻结——读侧契约「未知 protocol_version 仍能逐项按 kind/tag 判定」隐含此前提（否则连
index_offset 都定位不到）。演进只允许三形：动段/项内部（TLV 自描述可跳）、增条目、
或整区换代（protocol_version 升代 + 新 region 形）。
```

**命名工件（v6，M3：消除跨语言命名漂移）**：路径与字段名的**单一真源**是一份**声明式清单**（采用方仓内一份 JSON/TOML 清单，列出绑定路径 + 载荷字段名 + kind 编号），框架**在实现期生成**各语言常量文件（TS/Python/C），UI 侧引用常量而非手写字符串——Tauri 的 camelCase/snake_case 漂移（顶层 camelCase、嵌套 snake_case ⇒ 前端 `undefined`）由此在**生成期**暴露而非运行期。
- **工件格式（规范的一部分）**：每个语言一份常量文件，内容 = 路径常量（`"/todos"`）+ 字段名常量（`"title"`）+ kind 编号常量；命名规则统一 `UPPER_SNAKE`（TS 用 `as const`），键名与清单逐字一致（**不做大小写变换**——变换正是漂移来源）。
- **生成口径（v6.1 定案 = 最小执行件，PM 裁决）**：**真源 = `docs/view-bridge-constants/manifest.toml`**（手维护单一清单）；**生成器 = `skeleton-infra/examples/view_constants_gen.rs`**（~80 行，零三方纯 std：读清单 → 写 TS/Py/C 三件常量文件；`--check` 模式只比对不写）；**CI 一致性断言命令 = `cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`**（生成物与库内已入库生成物 diff 非空 ⇒ 红灯——清单改了忘了重生成即红）；**生成物入库**（不每次构建生成——构建期生成会引入工具链依赖，禁 proc-macro 纪律同理）。**Rust 侧 demo 不引常量工件**：demo 以 `include_str!` 引清单逐字对表（写下的路径/字段名与 manifest 内容逐字一致，不一致即红）——常量工件是 UI 侧（TS/Py/C）的消费物，Rust 侧以清单本身为对表基准。
- **采用方义务**：字段名在 Rust 侧的投影（`to_json`/`encode` 产出的键名）必须与清单一致——一致性由**双向往返断言**把住（§8②：Rust → wire → 按约定读回 → 逐字段核对）。
- 边界：清单只覆盖**绑定路径 + 载荷字段名**；视图内部结构（嵌套层级）由采用方自定，框架不代管（YAGNI）。

**部署可达性表**（「拷贝与否是宿主投递的事」——框架面止于固定容量背衬 + 头内版本/身份）：

| 目标环境 | 框架提供 | 宿主投递 | 拷贝发生点 |
|---|---|---|---|
| 浏览器 TS（wasm SAB） | 固定容量背衬抽象 + eager 写 + 头内版本 | COOP/COEP 头 + wasm atomics 前提下，背衬即 SAB 视图（**零拷贝**）；不可用则 postMessage ArrayBuffer（transfer） | 零（SAB）或一次转移（postMessage） |
| Node TS | 同上 | NAPI 拷入 Buffer，或 worker 共享背衬 | 一次拷贝（NAPI）或零（共享） |
| 同机跨进程 | 同上 | shm 段注册为背衬 | 零（shm 直写） |
| 远程 | 同上 | 读侧按规范整读字节上传输 | 网络拷贝（固有） |

---

## 4. 绑定寻址与更新形态：三案对比（派单项②）

| 维度 | **A. 路径订阅 + replace 补丁**（推荐） | B. 类型化路径句柄 | C. 整视图快照推送 |
|---|---|---|---|
| 核心思路 | 声明路径词表（RFC 6901 形），装配期编译为内部索引；每通知逐绑定值比较，变更者以 `{path, value}` replace 补丁出站 | 宏/类型 DSL 生成绑定表，Rust 内句柄寻址，wire 出 id 或派生名 | 每通知序列化整视图文档+版本号，UI 全量重渲 |
| 优 | 增量出站（带宽∝变更）；路径即「绑定好就能拿到」的用户语义；serde_json pointer 官方语义对表（T1）；零新依赖；UI 侧任意 JSON 库可用 | 运行期零字符串；词表拼错编译期暴露 | 桥最小（~50 行）；无 diff 无注册表；UI 实现最简；语义上不可能漏更新 |
| 劣 | 绑定粒度自担（大子树绑定 = 大比较成本，§6）；两套地址（wire 路径 + 内部索引）需装配期对账 | 需要 proc-macro/DSL——T2 实证构建期成本（serde 级 derive 链 +6.6ms/类型、IDE 最难啃）且**违约零新依赖**；UI 侧仍需一份 id↔语义对照表，解耦收益漏出边界 | O(视图) 序列化+传输+解析**每通知**——恰是被治愈的「无 diff 闸全量重渲」；每次通知整文档分配，内存抖动 |
| 性能预算 | 每通知 O(哨兵扫描+变更投影)，未变更 0 分配；变更项内部 0（类型折叠置脏），交付时每路径 1 Value（§6 对齐） | 每通知 O(绑定数) 整数比较（常数最优） | 每通知 O(视图) 分配 + 序列化 |
| 承重假设 | 投影纯且词表静态；绑定粒度纪律由采用方声明自觉 | 采用方接受构建期/工具链成本 | 视图小到全量重渲无感 |

**拍板：A，吸收 B 的内部形态**（路径串只活在语言边界，桥内编译为 `usize` 索引，运行期零字符串比较——B 的性能内核不需要宏就能拿到），**C 保留为重同步原语**（`snapshot_doc`，出站箱溢出后的恢复路径，§6）。三案不是平权比较：C 是 A 的退化特例（绑定 = 根路径），B 的用户可见价值被 A 的装配期校验覆盖（拼错路径 bind 即 Err，与编译期只差一步）。
**证伪条件**：若真实采用方普遍需要「UI 运行期动态构造路径」（非声明式，如行级路径 `/todos/3/done` 的任意索引订阅），A 的静态词表成为摩擦点，届时重估词表通配扩展（见 §12-1）而非换 B。

---

## 5. 派生机制：记忆化策略对比（派单项③）

| 维度 | **L0 版本键** | **L1 输入键等值**（推荐） | 依赖追踪 |
|---|---|---|---|
| 核心思路 | 桥每通知自增代数，代数变 ⇒ 全部派生重算 | 派生声明 `input: &S → In`（`In: PartialEq`）+ `compute: &In → Out`；In 等值 ⇒ 返缓存 | 记录派生读取的状态路径集，与本拍写集相交才失效（Vue/Signals 模型） |
| 优 | 零声明零类型约束，实现 ~10 行 | 失效粒度由声明掌控；正确性论证是**类型等值**（不是哈希碰运气）；`In =` 实际输入切片类型时陈旧不可能发生 | 精确失效，最贵派生零多余重算 |
| 劣 | 过度失效：无关切片变更也重算全部派生——贵派生（格式化大列表）每通知挨刀 | input 声明窄于 compute 实读 = 静默陈旧（一条文档纪律管住，§9，与 reducer 纯度同纪律级）；In 需 Clone/PartialEq | 零宏约束下 Rust 拿不到可靠读集：包装读计数（每字段访问一层间接+计数）或手写路径清单（**清单漏一条 = 静默错值，fail-visible 破坏**）或 O(S) 全量 diff 相交（把 C 案的成本请回来） |
| 预算 | 每通知 Σcompute | 每通知 In clone + eq；命中零重算 | 读包装每访问一计数；或每通知 O(S) |
| 承重假设 | 派生都便宜 | 输入可廉价抽取且等值可比 | 可靠的读集提取设施（宏/运行时） |

**拍板：L1**；L0 是 L1 的零配置退化（抽不出廉价 `In` 时的兜底，`Derived` 同型换胞）。依赖追踪**否决**（机制层理由，非「太复杂」）：其全部实现路线在零 proc-macro 约束下要么把正确性押在人记性上（手写清单），要么付出把 diff 成本推广到全状态的代价——与「性能收益来自 diff 闸+子集投影+记忆化」的收益模型（澄清记录拍板口径）自相矛盾。
**证伪条件**：若实测中出现「input 抽取成本 ≈ compute 本体」的派生形态，L1 预算优势消失，重估 L0 默认。

---

## 6. 性能与内存预算量化（派单项④）

**dispatch 热路径不受影响的论证**：桥是纯订阅者——挂在 `notify_snapshot_consumers` 之后，enqueue/reduce/forward 三段零新增原子、零新增分支；稳态桥不增删订阅（票据持有至桥 Drop），`NotifyCache` 版本对账路径不变。桥常挂后的 dispatch 全含成本进入「订阅态基线（~142ns，其中分发本体 ~63-72ns）+ 桥订阅体」档，与挂任何普通订阅者同构。**口径声明**：`end_to_end_dispatch ≤100ns` 锚以无订阅口径定义，桥不回退该锚；桥订阅体成本另行立账。

**diff 成本（v4 类型化双层闸）**：哨兵粗闸在前（跳投影+跳比较），类型 PartialEq 细闸在后（跳补丁）。每通知 = O(Σ已绑定 哨兵抽取+eq) + O(Σ**哨兵变更绑定** 类型投影) + O(Σ哨兵变更 类型eq) + O(Σ值变更 移槽置脏)；未绑定项零扫描零投影零比较。类型化细闸 vs v3 的 Value 深比较：字段直比较，无树遍历、无逐节点盒解引用，常数更低。绑定粒度即比较粒度：绑叶子 O(叶子)，绑 `/todos` 整数组 O(数组)。

**每绑定分配数**（稳态）：
| 事件 | 分配 |
|---|---|
| 通知 × 哨兵命中（未变更绑定） | **0**（无投影无比较；In 抽取遵守零分配纪律） |
| 通知 × 哨兵变更·类型投影 | 用户类型自有分配：标量/Copy 变体 **0**；Vec/String 按需 1-2——对比 Value 树同数据（Vec<Value> + 每行 Map + 每键 String 盒装）结构性 **~3-5×↓** |
| 通知 × 哨兵变更但值未变 | 投影产物被类型 eq 细闸拦下随比较丢弃（用户类型自有分配，同上——粗闸误报的代价） |
| 通知 × 值变更·折叠 | **0**（View 移入 last 槽 + dirty 置位；槽容量恒 = 绑定数，无箱可溢） |
| 初值/重同步 snapshot_doc | O(绑定数)，一次性（全量重投影 + to_wire 组装） |
| 交付 take_patches | 每脏路径 1 Value（to_wire）+ Patch 壳——**冷路径**，通知路径零 Value 构造 |
| 派生命中 | 0 重算（Out clone 一次，用户型大小自担） |

**投影税前后对比（1000 绑定视图、无关 action 连打 100 拍、标量哨兵）[INFERENCE，采集器实证前不作锚]**：全量投影形（轮 2 前）每拍 ~1000×(投影建 Value+比较 ≈ 70ns) ≈ **70μs/拍**，100 拍 ≈ 7ms 纯投影税且与 action 相关性无关地照付；哨兵形每拍 ~1000×(抽取+eq ≈ 10ns) ≈ **10μs/拍**（≈7×），变更绑定为零时补丁皆零——**残余即哨兵扫描本体 O(绑定数)**（§12-3）。典型几十绑定视图哨兵形亚微秒级。首挂/重同步全量重投影 O(view) 一次性保留。**v4 增量**：哨兵扫描底不变（无关 action 场景与 v3 同价）；增益在变更项与内存——类型投影+类型 eq 取代 Value 树构建+深比较（分配 ~3-5×↓），且出站翻译整段移出通知路径。

**内存（v4）**：每绑定一个类型化 last View 槽（enum 布局，最大变体主导；悬殊时 Box 由采用方自定）+ 每哨兵一份 `In` 缓存 + dirty 位图（每绑定 1 bit）+ 路径表（`'static` 静态串零拷贝）——**O(绑定数) 恒定上界**。v3 的出站箱 1024 上界与溢出毒化**结构性退役**：latest-wins 折叠无箱可溢（PM 轮 3 内核点 3 的形式化：折叠箱退化为 dirty 位图 + 交付即从 last 翻译，零重复副本）。毒化-重同步闭环（F2 语义演化后）状态机一句：**正常（置脏）→ 毒化（唯一残因：投影 panic——哨兵已提交而 last/dirty 未更新；`projection_panics` 记因）→ UI 于 take_patches 批内见 `resync == true` → snapshot_doc 重同步（现态全量重投影 + wire 组装 + 换 last + 清脏 + 复位）→ 正常**。「禁静默丢/陈旧」不变量演化：**丢弃结构性消灭**（折叠只合并不丢弃——「UI 只需每路径现值」的状态绑定语义使中间补丁可合并不可丢），完整性事实必达 UI 由 `resync` 位独担（返回值携带，不靠读侧文档自觉），fail-visible 降级到 C 案原语（全量重同步）而不是错视图。

**与 77-142ns 锚的关系声明**：桥订阅体热路径预算 **~200ns @ 10 绑定小视图**（最坏形：哨兵全变更 = 10 抽取+eq + 10 单点类型投影 + 10 类型 eq；典型形哨兵全命中仅 10 抽取+eq）——此为**设计预算 [INFERENCE]**，MVP 采集器实证前不写成锚（§8 验收二期）。

**编码矩阵成本模型（v5，两通道独立可测、都开 = 两条都付——采用方选择非隐藏税）**：

| 通道 | 时机 | 成本 | 计数读侧 |
|---|---|---|---|
| JsonEncoder（lazy） | take_patches/snapshot_doc 交付 | 每脏路径一次 to_json（Value 树） | 既有补丁路径计数 |
| RegionEncoder（eager） | **通知路径**，变更即编 | 每变更绑定一次展平写（用户 encode，短批纪律）+ 每通知批一次版本原子写 | `region_encodes()` |

- **eager 边界的性能含义**：区域通道把编码成本从交付时刻移进通知路径——变更率 × 单绑定编码成本成为新增量，通知预算相应加项（~200ns 锚声明不含区域编码，开启区域通道后以 `region_encodes` 与采集器另行立账）；只开 JSON 通道时通知路径零编码（v4 形态不变）。
- **区域容量/换区成本**：追加写 O(段)；Arena 耗尽/水位超限 ⇒ 换新区域（新背衬 + 新 region_id + 版本延续 + UI 重映射握手）——换区成本 = 一次性，频率由容量上界与变更数据量决定（估小了 = 换区频繁 = 性能税可见于 `region_encodes` 间隔，非正确性问题，§12-8）。
- **投递成本框架侧 = 0**：SAB/shm 直写零拷贝、FFI/NAPI 一次拷贝、网络固有（§3.6 部署表）——拷贝全部归宿主投递层。

---

**〔v6 增补·M1〕水位与撕裂的预算与机制**：版本号本身**不花钱**（一个 `u64` 自增 + 头内一次原子写），真正的成本在两处：
- **跨通道撕裂检测的代价 = UI 侧一次比较**（`region.version != applied_version` ⇒ 重读区域）。机制收益是「撕裂从静默错值变成可检出的显式不一致」——对照 React `useSyncExternalStore` 的三级支持阶梯（T1：允许撕裂 / 不撕裂但偶尔重渲染 / 既一致又不降级），本方案落的是**第二级**：检测到不一致 ⇒ 重取，而不是假装一致。**不落第三级**（不撕裂 + 不降级）——那要求出站面自带事务性快照，超出桥的职责（YAGNI + 逃生口 = 绑组合视图/单路径绑定）。
- **缺段/倒退 ⇒ 重同步的代价 = O(视图) 一次性全量重投影**（v5 的 snapshot_doc 机制，量级不变）。水位让「何时必须重同步」变成**可执行判据**（v5 的 resync 位只覆盖毒化一种残因；v6 补上「漏批/乱序/跨通道不同拍」三类）。**重同步频率是采用方行为**（UI 拉取节奏）；桥侧不缓存历史批次（latest-wins 折叠的既定语义）⇒ 频繁重同步是采用方拉取策略的代价，非桥的缺陷。

**〔v6 增补·M4〕dispatch 锚的诚实化（审计 F-05 实测）**：§2.4 的 `~77ns / ~142ns` 锚**只在小 State 下成立**。审计实测（`reports/2026-10-27-udf-architecture-audit.md` F-05）：**外部持有快照者存活时，每次 dispatch 额外付一次 `S::clone` 全量深克隆**，1 MiB State 实测 **~210µs/dispatch**——与 ~80ns 锚差 **~2600 倍**。三点必须写进采用面文档：
- **触发条件**：强计数 ≥2（有人持有 `snapshot()`）。桥的投影**不持有 State 快照**（投影吃 `&S` 借用，值随即落进类型化槽）⇒ **桥不制造该成本**；但 UI 侧若持有 `Arc<S>` 或桥以 `Arc<S>` 形态缓存状态，则成本由采用方承担。
- **成本位置**：`Arc::make_mut` 在 **state 锁内**执行 ⇒ 克隆耗时进临界区 ⇒ 并发 dispatch 被拉长（锁持有时间随 State 体量线性增长）。另：`S::clone` panic ⇒ 毒锁 ⇒ 实例报废（既有契约，v5 已述）。
- **采用面建议（不改契约）**：State 大时用 **`State = Arc<Inner>` 模式**——但**必须注意审计的反证**：单纯把 State 包成 `Arc` **无效**（实测比率 1.0x，因为 reducer 内部仍克隆整个 `Vec`）。有效的形态是 **reducer 返回 `Arc::make_mut` 就地改**（CoW 在 reducer 内完成，store 层的 `make_mut` 只搬指针）⇒ 快照持有者的深克隆成本降为「一次指针换代 + 局部 CoW」。**本方案不改 `Reducer` 契约**——这是采用面写法建议，写进 `docs/reference.md` 的 State 体量指引。
- **与桥的成本关系**：桥的投影成本是 **O(变更绑定)**（与 State 体量无关），State 克隆成本是 **O(State 体量)**（与绑定数无关）——**两者叠加**：一次 dispatch 的真实成本 ≈ State 落位（可能 O(State) 深克隆）+ 桥投影（O(变更绑定)）+ 编码（O(变更绑定)）。**桥的投影闸无法稀释 State 克隆成本**（二者正交）——文档不为「用了桥就快」背书。
- **落位策略已实测定案（ADR 0008 §8.1 回填，2026-10-27）**：`Arc::make_mut` 落位（现状 (a)）vs「每拍新 Arc」落位（(b)，免 CoW 克隆但每拍一次分配）——**实测完成，(b) 不落地，(a) 维持**。**M4 两证并引**：审计 F-05 实测 1 MiB State ≈ **210µs/dispatch** + ArcBench（`snapshot_placement.rs` 采集器，release 实测）1 MiB 大状态 **~230µs**（236.8k 无持有者 / 227.6k 有持有者）——同量级独立复核成立，「大 State 是反模式」红线获第二次实测支撑；小状态 79.2/80.9 ns 对齐 77ns 锚（±3ns 噪声内），同步档热路径零回退。**诚实边界**：ArcBench 的持有者差异不可分辨（<5%，被 ~230µs 地板淹没，疑似 harness 持有 Arc 未跨拍存活）——**待专项探针复核**，不影响「(b) 每拍强制新分配只会 ≥ (a)、无任何臂显示 (b) 有收益」的结论。本节按 (a)（维持 `make_mut`）描述；复评触发条件不变（ADR 0008 §8.1）。

**〔v6 增补·M5〕通知线程成本入预算 + 本方案不解决的核心问题（诚实声明）**：eager 区域编码在**通知路径**上 ⇒ 编码耗时进 dispatch 延迟；叠加审计 F-06 实测：**通知在 dispatch 线程同步执行 ⇒ 慢订阅者阻塞整个状态机**（桥是订阅者之一 ⇒ 桥的投影与编码成本直接进排空环）。三条记账：
- **成本归属**：桥的投影 + eager 编码 = dispatch 延迟的一部分（`~200ns` 锚声明**不含**区域编码；开启区域通道后以 `region_encodes` 计数另行立账，v5 已述）。F-06 实测把「订阅者做重活 ⇒ dispatch 延迟毛刺」的关联坐实。
- **已知取舍 + 二期候选**：框架侧异步通知面（ADR 0008 案 C）把重活移出排空环；桥侧对应三处措辞改动见 §3.2-v6 (e)。**二期候选 = 桥侧「消费点」粒度可配**（按批消费 vs 按绑定消费）——**不预付**（无实测需求）。
- **本方案不解决的核心问题（明确记账，不假装解决）**：F-06 的本体（通知同步执行 ⇒ 重活阻塞全局状态机）**由 ADR 0008 解决，不在本方案范围**；本方案只做「桥适配异步档 + 成本可见 + 判定可执行」三件事。**若 ADR 0008 未落**，桥维持 v5 同步档形态（两档并存，机制共用），此时桥的投影成本仍进 dispatch 延迟——**该数字对采用方可见**（`region_encodes` / `projection_runs`），不隐藏在文档里。

---

## 7. 字符串路由否决 vs 数据寻址：划界论证（派单非目标项）

compose.rs 封死的是**控制面字符串路由**：action 在 Feature 间「按名字找处理者」——它毁灭编译期穷尽性、制造环境耦合（谁能给谁发什么不可见），故以 `wrap`/`unwrap` 类型闭包封死。绑定路径是**数据面地址**，结构上三处不同，不是同物再辩：

1. **方向**：路径由生产方（Rust 侧 ViewMap）声明、UI 只读消费；不存在「UI 按路径寻址某个 Feature 的行为」——行为寻址仍全走类型化 action 树，路径永不触达 reduce。
2. **失效形态**：未知路径在 `bind()` 装配期显式 Err（fail-visible），不存在「字符串没match上就静默走默认」的路由语义；未知意图在 `send_intent` 显式 Err，翻译是采用方手写的穷尽 match（类型驱动），框架层不存在字符串分派表。
3. **表示层界**：字符串只活在语言边界（wire 协议常量，同 `EffectId` 是数值句柄、路径是文本句柄——同纪律不同载体，载体由「语言无关」需求倒逼）；桥内装配期一次解析编译为 `usize` 索引，**运行期零字符串比较**。

一句话：**路由 = 行为怎么找（类型封死）；寻址 = 数据在哪份声明里的第几项（索引封死）**。字符串在两边都只剩协议常量身份，任何一端都不拿字符串做运行期决策。

---

## 8. MVP 切片（~840 行 = ~740 截至 v6 + ~100 裁决③ M3 执行件增项；重定基 2026-10-27，PM 预批 ~840〔补审 N1 更新批准数〕）与验收口径提案（派单项⑤）

> **重定基记账（v6.1，Major 2；补审 N1 更新批准数）**：初版「≤400 行」承诺由 **~840（重定基 2026-10-27，PM 预批 ~840，用户终批与 ADR 0006 拍板同包）** 取代——**公式：~740（截至 v6）+ ~100（裁决③ M3 执行件增项）= ~840**（账本：链加 835〔400+60+40+145+90+100〕/ 表行加 838，取 ~840 容差）。理由一句：踩平轮六机制（M1–M6）为用户拍板方向之载体，预算增长是机制落点，非失控。表内「核心 / v6 增补 / v6.1 增补」标记切口；**若砍回 ≤400：砍 M3 工件链 + demo 扩展断言列二期**；**测试另计口径不变**。各轮明细：核心 ~400 → 轮 2 +60（哨兵）→ 轮 3 +40（类型化折叠）→ v5 +145（编码矩阵 + eager 区域 + 镜像读取器）→ v6 +90（读侧参考实现 + 水位/标签字段）→ v6.1 +100（M3 执行件 gen ~80 + manifest ~20，入总账不入 400 核心口径）。

**文件清单与行预算**（含 doc 注释）：

| 文件 | 动作 | 行预算 | 内容 |
|---|---|---|---|
| `skeleton/src/view.rs` | 新建 | ~85 | 〔核心，含 v6 词汇增补〕 `BindingDecl`（v6 增 `kind: PayloadTag`）/`ViewMap`（**纯投影**：View bound + 缺省 `input_changed`；v5 起 to_wire/Wire 迁出）/`IntentMap` + `PayloadTag` + `PROTOCOL_VERSION` + 语义契约 doc（§3.1 全部承诺） |
| `skeleton/src/lib.rs` | +2 行 | 2 | 〔核心〕 `pub mod view;` + 数据链路表「谁呈现」行加桥 |
| `skeleton-infra/src/view.rs` | 新建 | ~400 | 〔核心 + 轮2/3/v5 机制增量〕 `ViewBridge`/`Patch`（v6 增 `kind`）/`PatchBatch`（v6 增 `from_version`/`to_version`/`protocol_version`）/`Snapshot`/`Derived`/`Sentinel`/`JsonEncoder`/`RegionEncoder`/`Coverage`/`RegionWriter`/`RegionBacking`/`VecBacking`/`BindError`/`IntentError`/`RegionError`（手写 Display/Error，journal.rs 先例；dirty 位图折叠 + eager 区域通道 + 固定容量 arena + 换区握手；投影 panic catch+计数；`projection_runs`/`region_encodes` 读侧） |
| `skeleton-infra/src/view/read.rs` | 新建（v6） | ~90 | 〔v6 增补〕 **读侧参考实现**（§3.2-v6 (d)）：`RegionReader`（TLV 解段 + 未知 tag/kind 跳过 + `unknown_tags_skipped` 计数 + `needs_resync` 提示）+ `decode_batch`（JSON 通道同款跳过语义）——未知标签语义**实现一次**，各语言镜像读器复刻 |
| `skeleton-infra/src/lib.rs` | +2 行 | 2 | 〔核心〕 `pub mod view;` + `view` 模块内 `pub mod read;`（**`view::read`，v6.1 定名**——不设顶层 `view_read`，与 §3.2-v6 (d)/§9 行文一致） |
| `skeleton-infra/Cargo.toml` | +~9 行 | 9 | 〔核心，v6 门形态改写不增行〕 `view = ["dep:serde_json", "udf-std-ext/json"]` feature（**依 ADR 0007 §4.2-5 跨 crate 开门，已实测**：开则 serde_json 进 infra 树 + std-ext json 域可用；关则都不进，默认树仍零三方）+ `[[example]] view_bridge_demo, required-features=["view"]` + `[[example]] view_constants_gen`（均 hollow-gate 纪律） |
| `skeleton-infra/examples/view_bridge_demo.rs` | 新建 | ~150 | 〔核心 + v6 断言增补〕 正典验收环（下：矩阵行为 + 双编码一致性 + 镜像读取器 + 换区握手 + **水位自洽 + 未知标签跳过 + 清单 `include_str!` 对表**） |
| `docs/view-region-spec.md` + `docs/view-region-reader.ts` | 实现期工件（本轮只规划落位） | 另计 | §3.6 规范公开件（**v1**：TLV + protocol_version + kind + 未知标签跳过路径 + 信封骨架布局冻结）+ TS 镜像读取器片段；ADR 0006 引用 |
| `docs/view-bridge-constants/manifest.toml` | 新建（v6.1） | ~20 | 〔v6.1 增补，入总账不入 400 核心口径〕 **M3 真源**：手维护单一清单（绑定路径 + 载荷字段名 + kind 编号）；Rust demo `include_str!` 逐字对表基准（§3.6 末段） |
| `skeleton-infra/examples/view_constants_gen.rs` | 新建（v6.1） | ~80 | 〔v6.1 增补，入总账不入 400 核心口径〕 **M3 生成器**（零三方纯 std）：读清单 → 写 TS/Py/C 三件常量文件（生成物入库 `docs/view-bridge-constants/`）；`--check` 模式只比对不写；CI 一致性断言命令 = `cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`（diff 非空 = 红） |
| `skeleton-infra/tests/view_bridge_test.rs` | 新建 | ~70 | **另计，不入产品切片口径**（F3）：验收闸②的载体——panic 毒化闭环/封板/跨线程/一致性/换区断言，并发与毒化场景 demo 装不下；产品切片口径 = skeleton + infra + example + manifest |
| 合计（产品切片口径） | | **~840（重定基 2026-10-27，PM 预批 ~840〔补审 N1〕，用户终批与 ADR 0006 拍板同包；= ~740 截至 v6 + ~100 裁决③ M3 执行件增项；链加 835 / 表行加 838，取 ~840 容差）** | 核心 ~400 → 轮 2 +60（哨兵）→ 轮 3 +40（类型化折叠）→ v5 +145（编码矩阵 + eager 区域通道 + 镜像读取器）→ v6 +90（读侧参考实现 + 水位/标签字段）→ **v6.1 +100（M3 执行件：gen ~80 + manifest ~20，入总账不入 400 核心口径）**；切口标记见表内〔核心 / v6 增补 / v6.1 增补〕，实现期按 ~840 额度收口（超额度回落 = 砍 M3 工件链 + demo 扩展断言列二期）；验收测试另计 ~70 |

**demo 行为脚本（v5 + v6 增补）**：`enum DemoView { Count(u32), Remaining(u32) }` 类型化词表；`DemoJson`/`DemoRegion` 两编码器（`Coverage::All`，穷尽 match，**每变体 round-trip 断言**：构造 View → 编码 → 解码 → 逐字段核对，§12-7 红线的测试化）；counter + todos 两切片 + 派生 `remaining`（L1，input 抽 todo 切片）；两绑定各挂标量哨兵；`bind("/count")`、`bind("/derived/remaining")`、`bind("/nope")`（断言 Err）；`snapshot_doc()` 首挂 → 全量且 `projection_runs() == 绑定数`；dispatch `Add(2)` → take_patches 恰两条 + **区域头版本 +1**（同步档 = 桥内消费代数：一拍一状态变更一 +1；eager：变更即编）；**latest-wins 折叠**：不拉取连发 `Add(2)`×2 → JSON 恰一条 `/count=4` 现值、区域段即现值（一批一版本）；**矩阵行为（修订令点 4）**：只开 Json → 区域通道不存在（`region_version()==0`）；只开 Region → take_patches 空批且区域照常更新；都开 → **逐绑定双编码一致性断言**（区域段经镜像读取器回读重建 Value == to_json 的 Value）；**dispatch `Touch`（无关切片）→ 零补丁、`region_encodes()` 不变、`projection_runs()` 不变**（哨兵全命中）；**固定容量与换区**：注入小容量背衬 → 超限触发换区（`region_id` 变化 + 版本延续不回退 + 旧背衬内容不变）；`projection_panics()==0 && resync==false` 收尾断言。

**〔v6 增补·demo 断言链〕**（三条新机制各有可执行断言，不靠文档自觉）：
1. **版本水位自洽断言（M1，v6.1：同步档 = 桥内消费代数，§8 断言全部可执行）**：连续 dispatch N 次 ⇒ `take_patches()` 批的 `to_version` **逐拍 +1（严格单调）**且 `from_version == 上一批的 to_version`（首次 = 0——同步档逐拍无丢失 ⇒ 代数与状态变更一一对应，无跳跃面）；`snapshot_doc().version == 桥内当前消费代数`；**跨通道同源断言**：区域头 `version == 同一拍 JSON 批的 to_version`（同一桥实例同一代数 ⇒ 相等）；**倒退/缺段判定断言（防御路径，同步档结构性不触发）**：人为让 UI 侧 `applied_version` 落后一批 ⇒ 按 §3.2-v6 (b) 伪码 ⇒ 走 `snapshot_doc()` 重同步（断言路径被走到、文档重建）；**快照重同步断言（补审 N2）**：`snapshot_doc()` 重同步后下一批 `from_version == snapshot.version`（快照即交付：快照交付推进 `delivered_epoch`，§3.2-v6 (a)）。
2. **未知标签跳过断言（M2）**：构造一段**含未知 tag**（如 `tag=0x7F`）的区域字节 ⇒ `RegionReader::read` **不 panic、不整区失败**：已知段正常解出、未知段被跳过（TLV `len` 边界正确）、`unknown_tags_skipped == 1`、`needs_resync == true`；JSON 通道同款：批内放一项未知 `kind` ⇒ `decode_batch` 跳过该项 + 计数 + 提示，其余项照常。
3. **命名对表断言（M3，v6.1 最小形态）**：Rust 侧 demo **不引常量工件**——demo 以 `include_str!` 引 `docs/view-bridge-constants/manifest.toml` **逐字对表**（demo 写下的路径/字段名与清单内容逐字一致，不一致即红）；生成一致性 CI 命令 = `cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`（重新生成与库内生成物 diff 非空 ⇒ 红灯——清单改了未重生成即红）；TS/Py/C 常量工件为 UI 侧消费物（gen 产出、入库）。

**验收口径提案**（可执行行，拍板后生效）：
1. `cargo run -p udf-skeleton-infra --example view_bridge_demo --features view` —— 输出绑定路径收到的补丁序列（JSON 行）且退出码 0（断言内嵌，红即验收失败）；
2. `cargo test --workspace --all-features` 全绿（`tests/view_bridge_test.rs`）。**毒化-重同步闭环为可执行断言（panic 驱动）**：`project` 注入 panic → `projection_panics()` 增且次批 `resync == true` → `snapshot_doc()` 返回全量且毒化复位 → 后续变更补丁恢复到达。**编码矩阵（v5）**：只开一通道/双开的行为分叉、**双编码逐绑定一致性**（区域段经镜像读器回读 == to_json 语义，Value 相等）、固定容量换区握手（region_id 变化 + 版本延续 + 旧背衬内容不变）、eager 计数读侧（`region_encodes`）。**哨兵跳过**：哨兵命中 ⇒ `projection_runs()` 不变。**编码穷尽性**：每变体 round-trip 逐字段核对（§12-7/§12-9）。另含：未知路径 `Err` / 封板后 bind `Err(BindError::Sealed)`（F1）/ 未知意图 `Err` / 桥 Drop 注销票据 / 跨线程 pull（F1）/ 闭门 store `send_intent` 显式 `Ok(false)`。
   **〔v6 增补·三条新机制的可执行断言（v6.1 随定案更新）〕**：① **水位自洽（同步档 = 桥内消费代数）**：`to_version` 逐拍 +1、`from_version == 上批 to_version`、`snapshot.version == 桥内当前消费代数`、**区域头 version == 同拍 JSON 批 to_version**（同源）、人为制造倒退/缺段 ⇒ 走 `snapshot_doc()` 重同步路径（断言被走到）、**快照重同步后 `from_version == snapshot.version`**（快照即交付，补审 N2）。② **未知标签跳过**：未知段 tag ⇒ `RegionReader::read` 不 panic 不整区失败 + 跳过该段 + `unknown_tags_skipped == 1` + `needs_resync == true`；JSON 通道未知 `kind` ⇒ 跳过该项 + 计数 + 提示（其余项照常）。③ **命名对表（M3 最小形态）**：demo `include_str!` 清单逐字对表而非字面量失检；CI 断言生成物 == 重新生成（清单改了未重生成 ⇒ 红灯）。
3. `cargo tree -p udf-skeleton-infra -e normal`（默认 feature）零三方口径不变——serde_json 仅随 `view`/`journal`/`config-json` 进树，节点已被 journal 证明；**v6 追加**：`cargo tree -p udf-skeleton-infra --features view` ⇒ `serde_json` **与** `udf-std-ext/json` 同时进树（依 ADR 0007 §4.2-5 跨 crate 开门，已实测）；默认树（无 `view`）两者都不进。
4. **语言无关性证明口径**：demo 断言全程只经 `serde_json::Value` 与 `&str` 进出，无任何 UI 栈类型参与；补丁行可被任意语言 JSON 解析器原样消费——「UI 任意技术栈」由出站面形状证明，不靠口头。
5. **〔v6.1〕`cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`** —— 退出码 0 = 库内生成物与清单一致（diff 非空 = 红；`--check` 只比对不写；零三方，CI 可直接跑）。

**二期（非 MVP 闸）**：`view_bridge_multidim` 采集器（沿 multidim 先例，纯 std + memgov 计数分配器）实证 §6 预算并立桥体锚。

---

## 9. 规范落点（角色硬约束逐条）

| 规范 | 落点 |
|---|---|
| 错误形态 | 全库一种：手写 enum + `Display`/`Error`（thiserror 不入 manifest，journal.rs 先例）。BindError（未知路径）/IntentError（未知意图/载荷畸形）装配与入站显式 Err；投影 panic 桥内 catch + 计数 + 毒化闩收口到 `snapshot_doc` 重同步（对齐订阅回调 panic 摘除留痕，不裸传不吞；latest-wins 折叠后出站箱溢出结构性退役，毒化唯一残因 = 投影 panic，§6），**禁静默丢** |
| 日志/观测 | 不新增埋点（log-embed 纪律不动）；降级经读侧计数显形（`projection_panics`/`projection_runs`/`region_encodes`/`unknown_tags_skipped` 对齐 `subscriber_panics` 先例——`patches_dropped` 已随 latest-wins 折叠退役，L229-231 口径）；audit 面已发 `Notified` 不重复造节点 |
| 命名/目录 | 域名小写单词：`skeleton::view` / `skeleton_infra::view`（compose/journal 同款）；feature `view`；example `view_bridge_demo`；词汇名 ViewMap/IntentMap/ViewBridge/Derived/Patch（仓库无既有投影词汇，澄清账本已查无冲突） |
| 测试策略 | 行为面 `tests/view_bridge_test.rs` + 正典 example 验收 + hollow-gate（required-features 显式）+ CI `--all-features` 闸不变；derive 记忆化以计数断言咬住 |
| 文档树 | skeleton lib.rs 域典与数据链路表、reference.md feature 矩阵 + 域索引各一行、README 链接、CHANGELOG、ADR 0006（拍板后落 docs/adr/） |
| 依赖政策 | 零新三方；serde_json 走既有 optional（journal/config-json 同款形态）；skeleton 保持零依赖；禁 proc-macro（T2）。**v6**：`view` 门形态 = `["dep:serde_json", "udf-std-ext/json"]`（依 ADR 0007 §4.2-5 跨 crate 开门，已实测）；**view 门开时 serde_json 按需进 infra 树，默认树仍零三方** |
| **v6·协议兼容面** | 信封版本位（`PROTOCOL_VERSION`，两通道各一份）+ 逐项载荷标签（`PayloadTag`）+ 读侧「未知标签跳过 + 计数 + resync 提示」契约（§3.2-v6 (d)、§3.6）；「只加不改」由**文档纪律**升为**机制**；编号空间分段保留、退役号不复用（protobuf reserved 纪律，T1） |
| **v6·读侧实现归属** | 未知标签语义**实现一次**（infra 侧 `view::read`——`view` 模块子模块，v6.1 定名，不设顶层 `view_read`：区域参考读器 + JSON 解码助手 + `unknown_tags_skipped`/`needs_resync` 读侧）；TS/Python/C 镜像读器按同一语义复刻；读侧不属骨架（骨架零依赖纪律） |
| **v6·命名工件** | **真源 = `docs/view-bridge-constants/manifest.toml`**（手维护单一清单）→ **生成器 = `skeleton-infra/examples/view_constants_gen.rs`**（~80 行零三方：读清单 → 写 TS/Python/C 常量文件，路径 + 字段名 + kind；`--check` 只比对不写）；生成物入库 + **CI 一致性断言命令 = `cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`**（diff 非空 ⇒ 红灯）；**Rust demo 不引常量工件**（`include_str!` 清单逐字对表，§8 断言③）；**禁构建期生成**（禁 proc-macro 纪律同理，不引工具链依赖） |

---

## 10. ADR 0006 草稿（拍板后由主代理落 docs/adr/0006-view-bridge.md）

> **ADR 0006: 语言无关视图数据面（ViewMap/IntentMap 契约 + ViewBridge 机器）**
> - 状态：提案（Proposed，2026-10-27，待终审）· **v6.1 = 封版候选（v6 复审修订轮）**
> - 关联：0004（三 crate 分层）；0005（关联类型化先例）；**0007（std-ext 打包粒度——`view` 门依赖形态依其 §4.2-5 跨 crate 开门，已实测）**；**0008（通知面异步化，案 C，**已采纳（未实现）**——本 ADR 对其有一条接口依赖（异步档消费入口 + mount 变体，不在 MVP 范围，异步档另立后续），见「承重假设」）**；`docs/research/mvi-view-bridge-clarify.md`；`reports/2026-10-27-derive-decoupling-precedents.md`；`reports/2026-10-27-mvi-mvvm-coupling-pitfalls.md`（外部踩坑扫描）；`reports/2026-10-27-udf-architecture-audit.md`（架构审计）
>
> **背景**：Driver::render 下行全量领域 State、UI 手拼嵌套 Action 树、无 diff 闸全量通知，三处把 UI 焊死在领域类型形状上；用户拍板：映射与派生在 Rust 内完成，UI 任意语言按绑定取数，零新依赖。v6 追加背景：外部踩坑扫描给出两类业界成熟机制（协议兼容期的未知变体兜底、跨线程视图的撕裂检测），架构审计给出两条本仓事实（快照持有者致每次 dispatch 深克隆 F-05；通知在 dispatch 线程同步执行 F-06，**用户已拍板 = 案 C**，ADR 0008 已采纳（未实现））。
>
> **决策**：①契约词汇三件（`BindingDecl`/`ViewMap<S>`/`IntentMap<A>`）进 skeleton（零依赖、载体泛型）；②机器 `ViewBridge` 进 skeleton-infra，feature `view = ["dep:serde_json", "udf-std-ext/json"]`（**依 ADR 0007 §4.2-5 跨 crate 开门，已实测**），以纯订阅者挂 SyncStore 通知面，核心零改动；③更新形态取「路径订阅 + replace 补丁」（§4 案 A），吸收类型化句柄的内部索引形态，整视图快照保留为重同步原语；④**L1 输入键等值记忆化两处应用**（§5/§3.1，轮 2 增补）：派生值记忆化（`Derived`）+ **绑定级输入哨兵**（`ViewMap::input_changed` 契约义务 + `Sentinel` 机制胞，投影税 O(视图)/拍 → O(哨兵扫描+变更投影)/拍）；依赖追踪否决；⑤**载体分层（轮 3 增补）**：内部类型化（`View: Clone + PartialEq + Send + 'static`，std 内建派生可满足——diff = 类型 eq、缓存 = 类型槽）+ 边界 wire 化 + latest-wins 折叠（dirty 位图 + 交付即从 last 翻译，O(绑定数) 恒定上界，丢弃结构性消灭）；⑥**可组合编码矩阵（v5，用户澄清 supersedes「规范+可选」双轨初形与 ⑤ 的 ViewMap::to_wire 落位）**：编码义务整体迁出 ViewMap 为可注册编码器组件——`JsonEncoder`（lazy，交付时翻译）/ `RegionEncoder`（eager，通知路径变更即编），mount 注册 0..2、Coverage 按绑定，发布面 = 绑定×编码器矩阵（只开/都开/混搭，采用方自选）；区域 = 固定容量地址稳定背衬 + 头内 version/region_id 判新旧与重映射握手（**禁原地增长**——SAB memory.grow detach JS 视图）；框架侧零 unsafe 零裸指针（背衬经回调借用写入，unsafe 归宿主实现）。
> **〔v6 踩平轮增补·六项〕**：⑦**版本来源两档分写 + 出站水位**（M1，v6.1 定案，§3.2-v6 (a)(b)）：**版本号生成者 = 该档下能保证无丢失单源的那一位**——同步档（0006 MVP 唯一范围）= **桥内单调消费代数**（语义：「本桥见过的第 N 次状态变更」；同步通知逐拍无丢失 ⇒ 代数与状态变更一一对应 ⇒ 撕裂结构性不可能，§8 水位断言同步档全部可执行；同 store 多通道同源 = 同一桥实例同一代数；跨桥/跨进程不同源记 §12）；异步档（ADR 0008 案 C）= **框架 `state_version` 单源**（§5.2 门控语义：合并可丢 ⇒ 须框架生成，桥不自造版本号），**其消费入口 API + mount 变体不在 0006 MVP 范围——接口依赖记账，异步档另立后续**；`PatchBatch` 带 `(from_version, to_version)`、`Snapshot` 带 `version`；UI 侧伪码判「缺段/倒退 ⇒ snapshot_doc 重同步」，跨通道撕裂判据 = 区域头 version 与 applied version 比对。⑧**协议兼容期不整批失败**（M2，§3.2-v6 (d)、§3.6 v1）：信封版本位 + 逐项载荷标签（`PayloadTag`，编号空间分段保留、退役号不复用）+ 区域段改 **TLV 长度前缀**（未知 tag 可跳过的前提）+ 读侧契约「未知标签跳过该项 + 计数 + resync 提示，**禁整批失败**」；语义由 infra 侧 `view::read` 参考实现**实现一次**，各语言镜像读器复刻。⑨**命名工件**（M3，§3.6 末段；v6.1 = 最小执行件）：清单为真源（`docs/view-bridge-constants/manifest.toml`，手维护）→ 生成器 `skeleton-infra/examples/view_constants_gen.rs`（~80 行零三方，`--check` 只比对不写）生成 TS/Python/C 常量文件（路径 + 字段名 + kind）→ 生成物入库 + CI 断言命令 `cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`（diff 非空 = 红；Rust demo `include_str!` 清单对表，不引常量工件）；消除 camelCase/snake_case 类静默 `undefined`。⑩**dispatch 锚诚实化**（M4，§6）：锚仅在小 State 下成立；快照持有者致每次 dispatch 额外全量深克隆（实测 1 MiB State ≈ 210µs，vs 锚 ~80ns，差 ~2600 倍）；采用面建议 `State = Arc<Inner>` **且 reducer 内 CoW**（单纯包 Arc 无效，实测 1.0x）——**不改 `Reducer` 契约**；落位策略已实测定案（ADR 0008 §8.1 回填）：**(b) 不落地、(a) 维持**——小状态 79.2/80.9ns 对齐 77ns 锚零回退、1 MiB 大状态 ~230µs 与审计 ~210µs 同量级独立复核；持有者差异不可分辨、待专项探针复核，不影响结论。⑪**通知线程成本入账 + 诚实声明**（M5，§6）：eager 编码进通知路径 = dispatch 延迟的一部分；**本 ADR 不修 F-06 本体**（通知同步执行 ⇒ 重活阻塞全局）——由 ADR 0008 承担；桥侧只做三处适配（最新性重述为「至消费点为止最新」/ 版本同源 / 线程归属改为宿主消费线程）。⑫**契约面冻结 + 公开词表演进出口**（M6）：`ViewMap` **方法集冻结**（新增义务须走独立可选 trait 或带默认实现的新方法）；**bound 永不落在采用方领域 `S` 上**（Druid `Data` bound 反例：作者本人否定、迫使 clone+等值比较+禁内部可变性，T2）；桥的公开词表（`Coverage`/错误类型等）标 `#[non_exhaustive]`（下游须带通配臂；核心 crate `Effect`/`Restore` 的同款缺口属架构审计 C 桶，**不在本轮**，登记 §12-11）。
>
> **被否决备选**：类型化句柄全案（proc-macro 成本 T2 实证 + 违约零新依赖 + 用户价值被装配期校验覆盖）；依赖追踪（零宏下读集不可靠，手写清单静默错值破坏 fail-visible）；派生宏方案（澄清记录拍板：静态代码生成性能中性，不解决三耦合点中任何一处）；**内部全 Value 载体**（v3 形，轮 3 否决——盒装税：BTreeMap 键分配+树插入、结构化深比较、Value 树缓存驻内存翻倍、出站二次构值）；**逐绑定异构 View 类型**（擦除论证同哨兵，单关联类型 + 采用方 enum 词表替代）；**mount 注入翻译闭包**（对称关联类型案胜出：双侧 Wire 同构钉死、翻译与词表同 impl 穷尽 match 咬住；换 wire 风格走薄包装不预付）；**编码器类型擦除**（`Box<dyn Fn>` + Any 下转把编码/跳过路径变装箱——v4 教训沿用于新组件，泛型 `Option<J/R>` 单调化分发替代）；**原地增长区域**（共享映射地址稳定性破坏 + SAB memory.grow detach JS ArrayBuffer 视图——固定容量 + 换区握手替代）；**编码义务留在 ViewMap**（v4 初形；用户澄清可组合矩阵后迁出——投影与编码职责分家，编码器可独立开关、独立覆盖、独立计数）。
> **〔v6 增补的被否决备选〕**：**异步档桥自造版本号**（M1——异步档消费可合并丢拍 ⇒ 桥内代数不再对应状态变更，须框架 `state_version` 单源，异步设计稿 R2 原文要求单一来源；同步档桥内消费代数因逐拍无丢失而自洽，两档分写见 §3.2-v6 (a)）；**两通道各带独立版本号**（跨通道不可比 ⇒ 撕裂不可检测——单源是唯一解）；**遇未知标签整条失败**（M2——serde 未知变体默认 Err、Elm 解码显式失败，二者都把「不认识的」升级成「全挂」；protobuf TLV 旧解析器跳未知字段、serde `#[serde(other)]` 兜底变体、Rust `#[non_exhaustive]` 给通配臂是同向机制——本方案取「跳过 + 计数 + 提示」）；**区域段保留 v0 定长 tag-value 序列**（M2——未知 tag 长度不可知 ⇒ 旧读侧只能失败，与「禁整批失败」直接冲突 ⇒ 改 TLV）；**UI 侧手写路径/字段名字面量**（M3——Tauri camelCase/snake_case 漂移 ⇒ 前端静默 `undefined`；清单 + 生成工件把漂移挪到生成期）；**构建期生成工件（proc-macro）**（M3——禁 proc-macro 纪律 + 引入工具链依赖 ⇒ 生成物入库 + CI 断言）；**桥侧提供异步通知面**（F-06 越界——通知面属核心 crate 契约，桥不得自造第二套通知语义；由 ADR 0008 承担）；**只加不改纯靠文档纪律**（v6 之前的状态——审计 F-07 同型风险证明纪律不构成机制保障 ⇒ 升为信封版本 + 标签跳过）。
>
> **承重假设**：投影纯函数且可按项调用（同 reducer 纪律）；**哨兵（轮 2 增补）——In 廉价且忠实：漏报 = 静默陈旧，与 Derived input 共用同一条「input 覆盖实读」红线；误报仅性能损失；抽不廉价的绑定退缺省 true 形**；**线程模型（F1）——通知体在 dispatch 线程同步执行（lib.rs:373 契约③、subscribe :429），pull 侧必然跨线程，桥内态单锁成组 + 原子累计计数 + 绑定表装配期封板，ViewBridge: Send+Sync**（哨兵胞为其外层短叶锁，先取先放零嵌套，锁形态不变）；绑定词表静态且只加不改（协议常量演进纪律）；派生 input 覆盖 compute 实读（一条文档纪律）；UI 侧具备 JSON 解析能力；**区域（v5）——读侧按 §3.6 规范实现（各语言 ~20 行）且 wasm 需 atomics（u64 版本单字原子读）；宿主投递与背衬实现自理拷贝与锁（框架零 unsafe 零裸指针）；背衬容量由宿主预留（估小 = 换区频繁 = 性能税可见，非正确性问题）**。
> **〔v6 增补的承重假设（v6.1 随定案改判）〕**：**版本来源两档分写（硬）**——同步档（0006 MVP 唯一范围）= **桥内单调消费代数**（逐拍无丢失 ⇒ 代数与状态变更一一对应，撕裂结构性不可能；跨桥/跨进程不同源为诚实边界，§12）；异步档 = **框架 `state_version` 单源**（ADR 0008 已采纳（未实现）；§5.2 门控下合并可丢 ⇒ 不得自造）；**异步档消费入口 API + mount 变体不在 0006 MVP 范围——另立后续，本 ADR 对 ADR 0008 有一条接口依赖（随该后续兑现）**；**版本化快照读面/保守配对问题随异步档移入该后续**（ADR 0008 定稿载荷面 = `publish(version)` + 既有 `snapshot()`，无版本化快照读面 ⇒ 保守配对为唯一路径，§12-12 标「现行」）；**view 门开时 serde_json 按需进 infra 树（默认树仍零三方）**（ADR 0007 §4.2-5）；**异步档下最新性语义为「至消费点为止」**（区域头 version 为新鲜度判据）；**命名工件需采用方维护清单并跑 CI 一致性断言**（框架只提供格式与生成口径）；**State 体量与快照持有者习惯由采用方承担**（F-05 成本不由桥制造，桥不承诺 dispatch 锚在大 State 下成立）。
>
> **后果**：正——UI 与 `S`/`A` 类型彻底隔离（连 serde bound 都不需要）；diff 成本按绑定收敛；语言无关由出站面形状证明；零新依赖、零核心改动、零 dispatch 锚回退。**v6 追加**——协议兼容期不再整批失败（降级可见 + 可计数）；撕裂从静默错值变为可检出的显式不一致（落 React 第二级阶梯：检测 + 重取，不假装一致）；跨语言命名漂移在生成期暴露。负——绑定粒度误用 = 比较成本自担；路径词表成为协议面，改名即破坏兼容（只加不改纪律对齐 journal 行格式纪律）；input 纪律违规 = 静默陈旧（计数器 demo 咬不住采纳方代码，只能靠文档红线）；View 需 Clone+PartialEq（一行内建 derive，bound 只落在 ViewMap 新类型上、领域 State 零新增——Druid Data bound 焊死采用方核心类型的教训不重演，T2）；编码器漏字段（to_json/encode 两形共用红线）= 静默缺字段（红线 + 每变体 round-trip 断言，§12-7/§12-9）；双开成本 = 两通道都付（eager 编码进通知路径 + lazy 拉取翻译，计数分读侧显式可见非隐藏税，§6）；**v6 追加**——水位只保证「可检测 + 可自愈」，不保证「永不撕裂」（第三级需事务性出站，超出职责）；未知标签跳过意味着该拍该绑定**缺失**（降级而非静默，需 UI 显式处理 `needs_resync`）；协议面多两处演进点（信封版本 + 标签编号）⇒ 演进成本从「靠纪律」变为「靠机制 + 规范版本」；view feature 门多一个跨 crate 依赖（`udf-std-ext/json`）。
>
> **回滚成本**：低。feature 门独立（`view` 单开即编译）、skeleton 侧仅三类型、核心零改动——回滚 = 删两文件 + feature 行 + manifest 行；MVP 阶段无外部采用方，无迁移面。**v6 追加**：回滚面 +1（`view::read` 参考实现）与 +1 规范版本（区域格式 v1 的 TLV 变更）——两者均在实现期一次成型，无存量兼容面（MVP 未发布）。

---

## 11. 调研出处（逐条 tier）

| 出处 | tier | 支撑 |
|---|---|---|
| RFC 6901（JSON Pointer，IETF） | T1 | 绑定路径语法与语义（§3.1/§4） |
| RFC 6902（JSON Patch，IETF） | T1 | 补丁语义参照——只采 `replace` 子集，不采完整 op 集（§4） |
| serde_json 官方文档（docs.rs Value::pointer/pointer_mut，2026-10-27 核对） | T1 | pointer 无 feature 内建 = RFC 6901 语义对表零成本（§3.3）；`Value` 自带 `Serialize`，出站编码零 derive（§2.1） |
| `reports/2026-10-27-derive-decoupling-precedents.md` | T2 | proc-macro 成本实证（案 B 否决）、Druid Data bound 反例（框架 bound 不焊采用方核心类型——ViewMap 只约束 ViewMap 自身）、Bevy「派生只生 impl，注册归运行时」（派生不进本方案，思路同源） |
| 树内性能锚：`README.md:144` / `docs/reference.md:201` / `tracingx.rs:40-44`（2026-10-16 批 C 实测树内记录） | T2（树内一手测量记录） | §6 dispatch 锚关系声明 |
| ADR 0001-0005 + compose.rs/journal.rs/runtime.rs 源码（2026-10-27 逐处核读） | T2（树内一手） | 分层纪律、feature 形态、错误先例、通知面语义（§2/§3/§9） |
| **〔v6 增补〕** React 官方 `useSyncExternalStore` 文档 + reactwg/react-18 discussions/70（撕裂三级支持阶梯） | T1 / T2 | M1 撕裂检测的机制依据（快照一致性契约；落第二级：检测 + 重取）；§6 v6-M1 预算段 |
| **〔v6 增补〕** Protobuf 官方 proto3 指南 + encoding 规范（字段号不复用 + `reserved`；TLV 旧解析器跳未知字段） | T1 | M2 协议兼容机制依据（编号分段保留 + 长度前缀使未知 tag 可跳过）；§3.2-v6 (d)、§3.6 v1 |
| **〔v6 增补〕** serde 官方 enum-representations / variant-attrs（未知变体默认 Err；`#[serde(other)]` 兜底变体） | T1 | M2 反面证据（整条失败的行为）+ 兜底方向；§10 v6 被否决备选 |
| **〔v6 增补〕** Rust `#[non_exhaustive]` 官方文档 | T1 | M6 公开词表演进出口（下游须带通配臂）；§10 决策⑫ |
| **〔v6 增补〕** Tauri v2 官方调用前端文档（`rename_all = "camelCase"`；嵌套字段可能 snake_case ⇒ 前端 `undefined`） | T1 | M3 命名工件的问题陈述（跨语言命名漂移的静默形态）；§3.6 末段 |
| **〔v6 增补〕** Android 官方架构文档（UI state 不可变 + business logic 不在 UI 层）+ Redux 官方 Style Guide（单一 store + 不可变） | T1 | 投影纯度与唯一事实源的外部背书（v6 沿用，§12-7 同族） |
| **〔v6 增补〕** `reports/2026-10-27-mvi-mvvm-coupling-pitfalls.md`（外部踩坑扫描，10 类坑 + M1-M8 踩平机制清单） | T2（仓内一手调研，逐条附 T1/T2 出处） | M1/M2/M3 的问题陈述与机制来源；§1 新增驱动行 |
| **〔v6 增补〕** `reports/2026-10-27-udf-architecture-audit.md`（架构审计 F-01~F-15） | T2（仓内一手审计，带 file:line 与实测） | M4（1 MiB State ≈ 210µs/dispatch 实测；Arc 包裹无效的 1.0x 反证）、M5（通知同步执行）、§6 v6 预算段 |
| **〔v6 增补〕** `docs/research/async-notification-design.md`（F-06 设计稿，案 C）+ `docs/adr/0007-std-ext-packaging-granularity.md`（已落盘）+ `docs/adr/0008-async-notify-signal.md`（**已采纳（未实现）**） | T2（仓内一手） | §3.2-v6 (e) 三处措辞（已定案 C）；§3.2-v6 (a) 异步档分支（§5.2 门控语义）；`view` 门跨 crate 开门形态（ADR 0007 §4.2-5，已实测） |

---

## 12. 风险与证伪（我猜最可能错在哪）

1. **静态词表可能撑不住动态行级绑定**：`/todos/3/done` 含运行期索引，静态 `'static str` 声明覆盖不了「按行订阅」。MVP 答案 = 绑数组整体（变更 = 整组 replace）；行级通配（`/todos/+/done` 声明 + 通知带实例路径）列二期扩展。**二期断点点名（F4）**：实例路径是运行期串，与 `Patch.path: &'static str` **硬冲突**——届时 Patch 形必改（`enum Path { Declared(&'static str), Instance(String) }` 或全换 String），属出站协议破坏性变更（「只加不改」纪律的既定例外，需协议版本位）；§3.2 的绑定封板闸同为其预留重开语义。**若第一批真实采用方多为列表行内交互，本 MVP 形态会被推翻一半**——这是本方案最可能错的地方。
2. **input 纪律靠人**：L1 的正确性押在「input 覆盖 compute 实读」一条文档红线上，违规形态是静默陈旧（最坏的失败形态）。缓解只有 demo 计数器 + 文档；若采用方反复踩，证伪 L1、退 L0（正确性零声明）。
3. **通知频率 = 每 action（轮 2 已由哨兵缓解，残余改写）**：投影税已从 O(视图)/拍 降为 O(哨兵扫描 + 哨兵变更项投影)/拍——**残余 = 哨兵抽取+eq 本身的每绑定每拍成本**（O(绑定数) 底，不随投影重/视图深变化；`projection_runs` 读侧使残余可测）。**残余证伪条件写明**：当某绑定的哨兵 In 抽不廉价（抽取+eq ≈ 投影本体成本）时，该绑定退无哨兵形（缺省 true，行为正确仅退回性能）；若真实采用方普遍如此，哨兵机制整体退回全量投影形态（§6 前后对比反演）——机制可整体撤销，契约 `input_changed` 缺省 true 即退路。「排空尾合并」（动核心）仍不预付，哨兵形实测超预算才重开。
4. **深比较的边角**：非有限浮点（NaN≠NaN ⇒ 永远补丁抖动）；视图值禁非有限浮点——v4 类型化载荷同理（`PartialEq` 逐字段）——进文档红线（serde_json 序列化非有限值本就退化 null，纪律同源）。
5. **Driver 与桥的关系**：桥不替代 Driver（进程内 Rust UI 帧循环宿主照旧），两者是同一通知面上的两个消费者——若终审认为应「一个呈现面收编另一个」，契约需重排，本方案按共存立项。
6. **F2 机制的 v4 语义演化（本轮形式化的最大改动，提请复审重点）**：latest-wins 折叠使「溢出丢弃」结构性不可能 ⇒ `dropped_since_last`/`patches_dropped()` 退役（PatchBatch 收窄为 `{patches, resync}`），毒化闩唯一残因 = 投影 panic，F2 不变量「完整性事实必达 UI」由 `resync` 位独担。若复审认定丢弃计数须保留（审计面价值），恢复成本 = 折叠覆盖被替值时 +1（一原子操作），语义不冲突。
7. **编码器漏字段 = 静默缺字段（v4 引入，v5 扩为两形）**：编码翻译是手写 match（to_json/encode），漏字段 = 补丁缺字段 = UI 静默陈旧——哨兵/eq 闸都咬不住它（闸判「变没变」，翻译判「翻得全不全」）。红线：穷尽 match 变体 + 全字段覆盖（两形共用）；缓解 = 每变体 round-trip 断言（demo/test 各覆盖一次，§8②）。与 §12-2（input 纪律）同类：正确性押在采用方纪律上，是本方案「框架不做宏」代价的第三处显形。
8. **区域固定容量，禁原地增长（v5）**：共享映射要求地址稳定——SAB `memory.grow` 会 detach JS 侧 ArrayBuffer 视图，原地 realloc = 持有者悬垂读。区域容量 = 宿主预留上界；Arena 耗尽/水位超限 ⇒ **换新区域**（新 region_id + 版本延续 + UI 重映射握手，§3.6）；框架内部 scratch 增长自由（不出手）。估小容量 = 换区频繁 = 性能税可见于计数读侧，非正确性问题。
9. **双编码漂移 = 静默不一致（一等风险）**：JsonEncoder 与 RegionEncoder 是两份手写翻译，漂移形态 = 同绑定两形语义分叉、UI 读到两个真相——闸咬不住，只有一致性断言能咬。红线：复杂子树必须 json_blob 与 to_json **同源取值**（机制保证）；断言：逐绑定两形 round-trip 对表（demo + 测试各覆盖一次，§8②）。
10. **组合的意外成本 = 显式不隐藏（v5）**：双开总成本 = eager 区域编码（进通知路径，每变更绑定）+ lazy JSON 翻译（每拉取），两笔真实且计数分读侧；采用方若高频拉取 JSON 又双开，成本自负可见。文档不为「都开很便宜」背书；误以为 JSON 也 eager 而按帧拉取 = 拉取税自负（take_patches 路径计数可见）。
11. **〔v6·外部待办·不在本轮〕核心 crate 的 `Effect`/`Restore` 缺 `#[non_exhaustive]`（架构审计 F-07）**：加变体即破坏全部解释器穷尽匹配——**属核心 crate 契约演进（审计 C 桶），不在本方案范围**（本方案只对**桥自己的**公开词表 `Coverage`/错误类型标 `#[non_exhaustive]`，§10 决策⑫）。**登记为跨 ADR 待办**：核心 crate 需独立 ADR 裁决「加变体 = 非破坏演进」的落实方式（候选：加 `#[non_exhaustive]` 使下游带通配臂 / 引入显式 `Other` 兜底变体 / 文档化 semver 例外）。**本方案不假装它已被解决**——桥层做对了不代表 core 层做对了。
12. **〔v6·跨 ADR 依赖·残余风险 · v6.1 标注：现行（异步档），非 MVP 风险〕版本化快照读面缺位 ⇒ `(state, version)` 可能错配（仅异步档）**：ADR 0008 **已采纳（未实现）**，定稿载荷面 = `publish(version)` + 既有 `snapshot()`——**无版本化快照读面** ⇒ 异步档下宿主先读版本后取快照（或反之）都可能拿到**错配的一对**（标签旧于内容 / 新于内容）⇒ 撕裂判定失真或误报。**现状判定（v6.1 改判）**：保守配对为**唯一路径**（标签取所读版本，**框架不保证消除**）——该风险随异步档（消费入口 API + mount 变体，不在 0006 MVP 范围）**另立后续**处置；同步档（MVP）版本在桥内自产自销，**无此风险**。**同步档的诚实边界（v6.1 记账）**：桥内消费代数**跨桥/跨进程不同源**——两个桥实例各自计代数、互不可比，跨桥/跨进程撕裂判定**不在本方案承诺内**（未来若需要，须引入跨桥共享版本源，属新 ADR）。**证伪/升级条件**：异步档后续若提供版本化快照读面（如 `snapshot_versioned() -> (u64, Arc<S>)`）⇒ 风险关闭；若实测发现错配可观测 ⇒ 该后续须裁决「框架补读面」或「显式放弃跨组件同源」（代价记在该后续的 ADR 里）。
13. **〔v6·命名漂移的残余面〕工件只覆盖「路径 + 载荷字段名」，覆盖不到嵌套结构与语义改名**：清单机制消除「字面量拼错 / 大小写漂移」这一类（G3 主诉），但**嵌套对象的层级与语义改名**仍由采用方自定（框架不代管，YAGNI）⇒ 采用方改了内层字段名而忘了改 UI 侧引用 ⇒ 仍是静默 `undefined`。缓解：双向往返断言（Rust → wire → 按约定读回 → 逐字段核对）只能覆盖**已声明的字段**——未声明的字段名漂移仍逃逸。**这是本方案「框架不做宏」代价的第四处显形**（前三处：input 纪律 §12-2、编码器漏字段 §12-7、ViewMap 侵入深度 §12-14）。
14. **〔v6·契约面冻结的代价〕`ViewMap` 方法集冻结 = 采用方升级成本的承诺，也是框架演进成本的承诺**：冻结后任何新能力（如 v6 的载荷标签）只能走「独立可选 trait」或「带默认实现的新方法」⇒ **采用方零改动**（Druid `Data` bound 的教训不重演：框架 bound 不焊采用方核心类型）；代价 = **框架侧每个新能力都要设计扩展点**，且可选 trait 的数量本身会膨胀（R4：契约层词汇膨胀）。**边界**：本方案只冻结 `ViewMap`；`IntentMap`/`JsonEncoder`/`RegionEncoder` 是否一并冻结留待首批评审（**当前不冻结**——它们尚无第二个实现，冻结为时过早）。
15. **〔v6·我猜最可能错在哪·增补〕我猜 v6 最可能错在「把版本同源当成一个 API 问题」**：M1 的真正难点不是加三个 `u64` 字段，而是**跨进程/跨语言边界的版本语义**——宿主拿到的 `(state, version)` 是否同拍，取决于**框架的快照读面形状**，而那不在本方案手里（ADR 0008）。**若 ADR 0008 最终形态不提供版本化快照读面，M1 的「单源」承诺在实现层会退化为「保守近似」**——那时最诚实的做法不是硬撑「同源」，而是把「同源」降级为「同序列 + 保守配对」并**写进协议**（UI 侧据此把版本当作下界而非精确值）。**这是本轮最可能被 momus 打回的一条**——已提前记账，不是隐藏风险。
16. **〔v6·协议面演进的长期成本〕信封版本 + 标签编号 = 两个新的长期演进点**：每次改格式都要动版本号或发新标签号 ⇒ 规范维护成本从「一条纪律」变为「一条纪律 + 两套编号分配规则 + 跳过语义」。缓解：编号空间分段保留 + 只增不改 + 参考实现语义唯一 ⇒ 演进动作机械且低风险；**但若采用方 UI 侧不实现跳过语义**（仍遇未知标签就崩），则 M2 的全部机制价值归零——**跳过语义的执行者是 UI 侧代码，框架只能提供参考实现与规范**（这是 M2 的真实边界，必须写明）。
