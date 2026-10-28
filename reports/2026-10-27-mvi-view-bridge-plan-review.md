# 计划审查报告：udf 语言无关视图数据面（MVI View Bridge）架构方案

- 日期：2026-10-27 · 审查岗：BridgeReview（终审位，只审计划不实现）
- 待审工件：`docs/research/mvi-view-bridge-research.md` · `docs/research/mvi-view-bridge-clarify.md` · `reports/2026-10-27-derive-decoupling-precedents.md`
- 审查依据：原派单契约九项 + 用户需求锚（「udf 是通用型架构……映射和派生在 rust 内部做好；UI 用什么技术语言都没关系，绑定好要获取的数据就能拿到数据」）

---

## VERDICT: FAIL（打回）

红线七条机械扫描全部通过；三轴中完整性 ✗（两个 Major 缺陷）。方案骨架优秀（九项派单要求逐项有着落、对标真实引用、复用面干净），打回仅因两处计划级修补——均为小改，修完可只补审改动节，无需重走全流程。

## 红线七条扫描

| 红线 | 结果 | 依据 |
|---|---|---|
| 占位符 | ✓ 无 TBD/TODO/空验证 | §8 文件清单+demo 脚本逐字对表 |
| 接口一致 | ✓ | `ViewMap::View`/`IntentMap::Wire` 在 §3.1 定义、§3.2 以 `serde_json::Value` 钉死，前后一致；`mount` 引用的 `skeleton::Store` 真实存在（skeleton/src/lib.rs:395-429） |
| 依赖无环+被依赖在前 | ✓ | 分层方向单向（skeleton←infra←宿主），无任务依赖图 |
| 验收可测性 | ✓ | 三条可执行 verify 行 + 命名行为测试（非空泛「测试绿」） |
| 规范空洞 | ✓ | §9 六项（错误/日志/命名/测试/文档/依赖）全有落点 |
| 外部广度缺失 | ✓ | 对标报告真实覆盖 Druid/Xilem/Bevy/Yew/Sauron + proc-macro 成本 T1/T2 实证，且被实际用于否决案 B 与依赖追踪 |
| 重复实现 | ✓ | 复用 `std_ext::json::decode_str`、serde_json `Value`、`Subscription` 票据；派生机制查无既有词汇（澄清账本已核） |

## 三轴评估

- **清晰度 ✓**：签名级+文件级+行预算+demo 行为脚本（含「逐字对表」断言序列）。仅 F5 一处数字漂移。
- **可验证性 ✓**：验收①②③为可执行命令，行为场景具名（未知路径 Err/未知意图 Err/闭门 Ok(false)/Drop 注销）；但 F2 的「溢出毒化+重同步」测试因复位语义未定义而**写不出断言**——计入 F2。
- **完整性 ✗**：F1（并发/生命周期契约缺项）、F2（fail-visible 闭环缺复位语义）两个 Major；F3 预算表漏项。

## 缺陷清单

| # | 严重度 | 位置 | 缺陷 | 修法 |
|---|---|---|---|---|
| 1 | **Major** | §3.2 / §10 承重假设 | **并发与生命周期契约缺失。** 树内事实：`Store::subscribe` 是跨线程档（`FnMut(&S) + Send + 'static`，skeleton/src/lib.rs:429；槽位 Mutex，runtime.rs:667），通知体在 **dispatch 调用线程**同步执行（runtime.rs:1081→1168）。而「任意语言 UI」宿主里 pull 侧（take_patches）几乎必然在另一线程。方案全程未声明：ViewBridge 的 Send/Sync 形态、出站箱/绑定表的内部同步方式、通知体写侧与 pull 读侧的线程关系。连带两处签名矛盾：①`take_patches(&mut self)`——通知闭包需持共享态并发写，`&mut` 独占借用与之冲突（应为 `&self` + 内部同步）；②`bind(&self, path)` 运行期注册与通知体读写绑定表的并发未定义（demo 还演示了 mount 后 bind）。ADR 草稿承重假设四条中无一条涉线程。这是「任意语言 UI」承诺下唯一的隐性 Rust 侧同线程嵌入假设 | 在 §3.2/§10 显式声明并发契约：桥内态（箱+上次值+毒化位+计数）走 `Arc<Mutex<..>>`/原子，`ViewBridge: Send + Sync`；`take_patches(&self)`；`bind` 限定装配期（首通知前）或加锁；「通知体跑在 dispatch 线程、pull 侧任意线程」写进承重假设 |
| 2 | **Major** | §6 / §3.2 / §8 测试② | **毒化-重同步闭环缺复位语义，fail-visible 差最后一口。** 溢出 ⇒ 置毒化位+计数+「停止入箱」，但未定义：毒化位由谁/何时清除——`snapshot_doc(&self)` 是纯读签名，不含复位；若闩锁不清，重同步后补丁**永断**（fail-permanent 而非 fail-visible）；UI 检测依赖另调 `patches_dropped()` 轮询，`take_patches` 返回值不携带丢弃事实，义务只靠文档承载；`patches_dropped` 是累计值还是自上次读数未定义。§8 验收②的「出站箱溢出毒化+重同步」测试因无语义可依而不可写断言 | 定死闭环：`take_patches` 返回携带 `dropped_since_last`（或 Poisoned 标记）让重同步义务随数据走；`snapshot_doc()` 显式复位毒化位+重开入箱（或独立 `resync()` 原语）；写死 `patches_dropped` 语义（建议：累计只增，重同步判定走 take_patches 返回值） |
| 3 | Minor | §8 文件清单 vs 验收② | `tests/view_bridge_test.rs` 是验收闸②的载体，却不在 ≤400 行预算表内（表合计 ~401 行不含它） | 表内补一行行预算，或显式声明「测试文件不计入 400 行口径」 |
| 4 | Minor | §12-1 / §3.2 `Patch` | 二期行级通配与 `Patch.path: &'static str` 硬冲突：实例路径（`/todos/3/done`）是运行期构造串，Patch 形状必改（&'static → String），连带 BindingDecl/bind/上次值表。§12-1 自评「推翻一半」诚实，但未点名这个具体接口断裂点 | §12-1 补一句点明 `Patch.path` 形状是二期返工的第一断点，防低估波及 |
| 5 | Trivial | §4 vs §6 | 变更叶子分配数 §4 写「1-2 分配」、§6 表写「1」 | 对齐一个数 |

## 派单契约九项逐条核对

①分层设计 ✓（§3，skeleton 词汇零依赖可编译——代码核对无外部依赖）②三案对比 ✓（§4 五维含承重假设+拍板依据）③派生对比 ✓（§5 三案+否决理由是机制层论证）④预算量化 ✓（§6 热路径零扰动论证+diff 按绑定收敛+分配表）⑤MVP≤400 行+验收 ✓（§8，除 F3）⑥ADR 草稿 ✓（§10 五要素齐）⑦tier 标注 ✓（T1×3：RFC 6901/6902/serde_json 官方）⑧零新依赖+字符串划界 ✓（§7 三点划界成立：方向/失效形态/表示层界，运行期索引化封死）⑨既有面核对 ✓——**抽验全部属实**：serde_json optional（skeleton-infra/Cargo.toml:38,63,75）、std-ext json 常开（std-ext/Cargo.toml:40）、subscribe 票据（lib.rs:429）、FEEDBACK_DEPTH_BUDGET=64（reference.md:149）、subscriber_panics（runtime.rs:744）、EffectBuf 复用先例（全库）。

## 审查重点四问

1. **「任意语言 UI」真伪**：数据面成立——出站全程 `serde_json::Value`/`&str`、无 UI 栈类型、领域类型连 serde bound 都不出 Rust（比 Tauri emit 整 State 更彻底的声明属实）；headless 验收口径（§8-4 wire 形状证明）是诚实的代理。**但**任意语言宿主默认跨线程，F1 的并发契约缺失是方案唯一的隐性假设——补上即真。
2. **静态词表 vs 动态行级**：被认真对待——§12-1 自认「本方案最可能错的地方」，给了 MVP 退化答案（绑整组 replace 仍可用）+二期通配形态+证伪条件；返工半径自评属实，具体断点见 F4。
3. **[INFERENCE] 实证**：已排——~200ns 明确标注「设计预算，实证前不写成锚」，二期 `view_bridge_multidim` 采集器（沿 multidim 先例）闭环，且正确地不设为 MVP 闸。
4. **出站箱故障面**：机制在（毒化+计数+禁静默丢，纪律对齐树内 FEEDBACK_DEPTH_BUDGET/subscriber_panics 先例，核实无误），但闭环缺复位语义与检测通道（F2）——现状下最坏形态是 fail-permanent。

## 通过条件（修完即可过，补审范围=§3.2/§6/§8/§10/§12）

1. F1：并发/生命周期契约写入 §3.2 签名与 §10 承重假设（`take_patches` 改 `&self`、`ViewBridge: Send+Sync`、bind 时序、通知体线程归属）。
2. F2：毒化复位语义定死（谁清位、计数语义、检测随 take_patches 返回值走），§8 测试②的断言点随之成立。
3. F3/F4/F5 顺手修（各一句/一行）。

## 审查产物

- 本报告：`reports/2026-10-27-mvi-view-bridge-plan-review.md`
- 抽验引用（只读）：skeleton/src/lib.rs:395-429 · skeleton-infra/src/runtime.rs:667,744,1081,1168 · skeleton-infra/Cargo.toml:38,63,75 · std-ext/Cargo.toml:40 · docs/reference.md:149

---

# 复审轮 1（2026-10-27，方案 v2）

## VERDICT: CONDITIONAL（有条件通过）

复审范围：§3.2/§6/§8/§10/§12（Main 派单限定）。只验两件事：①F1-F5 真实解决 ②无新破坏。

### F1-F5 逐项核验（均对照树内事实）

| 原缺陷 | 结果 | 证据 |
|---|---|---|
| F1 并发契约 | **已解决** | §3.2 线程模型声明与树内事实一致（lib.rs:429 `subscribe` 签名 / runtime.rs:1081→1168 通知体 dispatch 线程同步执行）；`core: Mutex<OutboxCore>` 单锁成组+锁窗如实声明（不与排空环互斥）；绑定表装配期封板（`sealed` 闸+`Err(BindError::Sealed)`）；`take_patches(&self) -> PatchBatch`（dropped_since_last 随批走）；`ViewBridge: Send+Sync`、M/I bounds 补 `Send+Sync+'static`（通知体跨线程需要，自洽）；ADR §10 承重假设已纳入线程模型 |
| F2 毒化-重同步闭环 | **已解决** | §6 状态机一句闭环：正常→溢出毒化（止步入箱+双计数同增）→take_patches 批内见 dropped_since_last>0→snapshot_doc 锁内成组复位（毒化+箱残留+自上次计数）→正常；计数语义写死（累计只增=审计面；自上次=读清、只在 PatchBatch）；§8 测试②现含四步可执行断言（撑爆→批内>0→重同步→次批==0 且补丁恢复），状态机与断言咬合 |
| F3 测试行预算 | **已解决** | §8 表新增 `tests/view_bridge_test.rs` ~60 行，显式「另计不入 ≤400 口径」且口径定义（产品切片） |
| F4 二期断点 | **已解决** | §12-1 点名 `Patch.path: &'static str` 硬冲突，给出两种改形选项（enum Path / 全 String）+协议版本位+封板闸重开语义预留 |
| F5 分配数对齐 | **已解决** | §4 性能预算行改「变更叶子 1 分配（§6 对齐）」，与 §6 表一致 |

### 新破坏扫描：无实质矛盾

- 与未改动节交叉：§2.2「普通订阅者」、§4 案 A 拍板、§5 Derived（通知体内 dispatch 线程使用，Mutex 胞与 Send+Sync 自洽）、§7 划界、§9 错误形态均无新矛盾；`PatchBatch`/`BindError::Sealed` 新名在 §3.2/§6/§8 三处用法一致。
- 状态机边角自检：毒化期间二次 take_patches 仍报 >0（可恢复）、重同步后归零不死循环、整批带毒标记（不部分信任）——语义闭合。

### 残留缺陷（Minor，实现时顺手修，不阻实现）

| # | 严重度 | 位置 | 缺陷 | 修法 |
|---|---|---|---|---|
| R1 | Minor | §3.2 封板闸 | 封板前窗口的并发边界靠纪律不靠机制：bind 契约为「装配期专用+与 mount 同线程」，封板后 Err(Sealed) 无隙；但若采用方违约在装配期从他线程 send_intent 触发首通知，pre-seal 的 bind 写与通知体读存在窗口（方案「竞争方只有两个」一句默认 bind 非运行期竞争方） | 一句话钉死：bind 的 sealed 检查与绑定表登记也走 core 锁（pre-seal 一并受锁保护），或声明「首通知前 send_intent 未定义」入装配纪律 |

### 结论

五项修复真实、与树内事实一致、无新矛盾。方案进入实现；实现时顺手落 R1（一句锁序声明）。
