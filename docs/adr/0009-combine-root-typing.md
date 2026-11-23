# ADR 0009：combine 分片作根的类型层不可表达化（Fragment/Reducer 契约拆分）

- 状态：**已采纳（2026-10-28，用户线 PM 拍板）**（2026-10-28 设计稿；实现契约与迁移表同批落地）
- 关联：审计 `reports/2026-10-27-udf-architecture-audit.md` F-03（本案来源）；`skeleton/src/lib.rs:86-88`「非法状态进类型」自述（被本案兑现的仓库原则）；0005（0.1.0 未发布破坏性直迁先例）；0004（三 crate 分层——`Fragment` 契约归 skeleton，机器归 infra）；0001（契约沉 skeleton 的先例）；0006（其背景引用过 combine 六参 wrap/unwrap，分片签名出现面，见迁移表）

**背景**：`combine`/`combine!` 产物 `Combine<…>` 是状态分片——签名无父状态工厂，只有 reduce 委托语义。现行实现以 `impl Reducer for Combine` + `initial()` 内 `panic!`（compose.rs:292-297）兜「分片误用作根」：`SyncStore::new(fragment, fx)` 编译通过、装配期 `root.initial()`（runtime.rs:742-743）即炸。三处破面：①与仓库自述「非法状态进类型」直接矛盾——同型 `impl Reducer` 让分片与真根在装配点零形状差别，编译期零信号；②`#[track_caller]` 只解决「panic 指哪一行」，不解决「为什么这个类型不能当根」；③文档示例 `fn counter_fragment() -> impl Reducer<…>` 的返回注解进一步暗示分片就是普通 Reducer，`SyncStore::new(counter_fragment(), NoFx)` 是零思考的自然误用动作。审计判定：运行期 panic 兜不住契约，须类型层不可表达。0.1.0 未发布，无 shim 负担（0005 先例）。

**决策**：把「能 reduce」与「能作根」拆成两个 trait，分片只满足前者。

1. **skeleton 新增 `Fragment` trait（reduce-only 分片契约）**，与 `Reducer` 同文件相邻、关联类型逐字同形：

   ```rust
   /// 状态分片：只承载 reduce 委托，无根状态工厂——不能作根挂载。
   /// 唯一树内非 blanket 实现：`Combine`（compose）。
   pub trait Fragment {
       type State: Clone + Send;
       type Action: Send;
       type Op;
       fn reduce(
           &self,
           state: &Self::State,
           action: Self::Action,
           out: &mut EffectBuf<Self::Action, Self::Op>,
       ) -> Self::State;
   }

   /// 每个根都是合法子件（完整 Feature 可作 combine 的 child）。
   impl<T: Reducer> Fragment for T {
       type State = T::State;
       type Action = T::Action;
       type Op = T::Op;
       fn reduce(&self, s: &Self::State, a: Self::Action, out: &mut EffectBuf<Self::Action, Self::Op>) -> Self::State {
           Reducer::reduce(self, s, a, out)
       }
   }
   ```

   **`Reducer` 本体零改动**（initial+reduce 原样，语义收窄为「根契约」）：树内约 60 处 Feature 的 `impl Reducer` **一处不动**。blanket 方向是本案承重赌注（见「候选对比」的论证与「承重假设」）。
2. **`Combine` 改 `impl Fragment`**（compose.rs:281-324）：删 `initial` + `#[track_caller]` + panic（292-297 整段），reduce 体原样；`combine()` 的子件 bound `C: Reducer<…>` 放宽为 `C: Fragment<…>`（compose.rs:115）——完整 Feature 经 blanket 仍可作子件，分片可嵌分片。
3. **挂载面不动**：`SyncStore::new`/`TestStore::new` 的 bound 仍 `impl Reducer<…>`——分片作根从此 **E0277**。`Reducer` trait 挂 `#[diagnostic::on_unimplemented(message = "…不是可挂载的根：根契约 Reducer 未实现", note = "combine! 产物是状态分片（Fragment）：没有 initial，不能作根；根状态由父 Feature 的 initial 显式构造（含子切片初值）")]`——把「为什么不能」写进编译错误本身（Rust 1.78+ 稳定）。
4. **随拆分清理孤儿机制**：`Reducer::initial` 的 `#[track_caller]`（lib.rs:101，doc 自述用途=转发分片误用 panic）与 `SyncStore::new`/`TestStore::new` 的 `#[track_caller]`（runtime.rs:683、testing.rs:236，同用途）一并删除——用途消失，两处 `new` 内无 assert，属死重。`Restore::FromInitial` 播种、`TallyFeature.initial()` 组合惯用形不动（initial 仍是「根状态唯一合法来源」）。

**分片作根时用户看到什么**（现状 vs 决策后）：

```text
现状（运行期）:
thread 'main' panicked at …compose.rs:294:
combine 产物是状态分片不是根：根状态由父 Feature 的 initial 显式构造…

决策后（编译期）:
error[E0277]: `Combine<…>` 不是可挂载的根：根契约 Reducer 未实现
  --> src/main.rs:42:30
   |
42 |     let store = SyncStore::new(done_fragment(), NoFx);
   |                 -------------- ^^^^^^^^^^^^^^^ ---------
   |                 |              |
   |                 |              combine! 产物是状态分片（Fragment）：没有 initial，
   |                 |              不能作根；根状态由父 Feature 的 initial 显式构造
   |                 required by a bound introduced by this call
```

**候选方案对比**（最难逆的决策 = 契约拆分的方向：哪个 trait 是根、blanket 往哪边打。这决定谁付迁移税、付一次还是付永久）：

| | 案 A（推荐）：`Fragment` + blanket 上打 | 案 B：supertrait（`Reducer: Fragment`，initial 移入子 trait） | 案 C：`Root<R>` newtype 挂载令牌 | 案 D：挂载面收状态值（`new(root, state, fx)`） |
|---|---|---|---|---|
| 类型形态 | `Reducer` 不动；新 `Fragment`（reduce-only）；`impl<T: Reducer> Fragment for T`；`Combine: Fragment` | `Fragment`（assoc+reduce）；`Reducer: Fragment` 只加 `initial`；`Combine: Fragment` | `SyncStore::new(Root(f), fx)`；`Root` 构造器 gate 根性 | `Reducer` 删 `initial`；根状态由调用方作为值传入 |
| 编译错误 | E0277 + on_unimplemented 指引 | 同左 | ①无标记 trait：Root(分片) 仍编译通过→panic 原样搬家；②配标记 trait：退化为案 A+一层包装 | **编译通过且能跑**：分片挂载后子 action 正常路由、父 action 落 route_miss——静默错语义 |
| 迁移步数（树内） | **~27 处编辑/12 文件，其中 Feature impl 零处**（见表） | ~90+ 处：约 60 个 `impl Reducer` 全部拆成 `impl Fragment`+`impl Reducer` 两块（supertrait 不允许一块覆盖），reference/thirty-min/doctest 全部示例随拆 | 实现①=0 处但问题未解；实现②=案 A 全部 + 全仓 ~30 处 mount 点加 `Root(...)` | Restore 播种语义、「initial=唯一合法来源」契约、`TallyFeature.initial()` 组合惯用形（thirty-min:784、end_to_end:134、combine_teststore:128）三处契约重造 |
| 长期税 | 罕见二义：同文件 import 两 trait 且裸调完整 Feature 的 `.reduce()` → E0034（一行逃生口 `Reducer::reduce(&f, …)`；树内核实零此形态——Feature 全部经 store 驱动，直接裸调的都是分片） | 无二义（`.reduce` 唯一候选=Fragment）；但**每个未来 Feature 永久多一个 impl 块** | 无（若接受①的假安全） | 无 |
| 回滚成本 | 低：Combine impl 改回 + 删 trait/blanket/attribute + 文档还原，一次成型 | 低但回滚面同量级（拆块全还原） | —（被 A 严格支配） | 高（三处契约重造 + journal/restore API 重设计，越界） |

方向论证：blanket 只能单向打——「每个根都可当子件」为真且必要（完整 Feature 作 child 是既有用法），反向「每个分片都是根」恰是本案要封死的错误。案 B 的收益（无二义）对应一个树内不存在、树外罕见、有一行逃生口的形态；代价是永久样板税 + 全树拆块。**为修一个可文档化的 wart 付永久税，不划算。**（变体 B'：把 reduce-only 命名为 `Reducer`、新造 `Feature` 作根名——词汇上更贴文档口头语，但翻转一个未发布即全树通行的词的语义，`impl Reducer`→`impl Feature` 改名 ~60 块 + 全文档示例 churn，换得的只是名字观感；若拍板者视「Reducer 名下无 initial」为词汇硬伤，此为唯一值得重开的变体。）

**被否决备选**（具体破面）：
- **案 B supertrait 拆分**：Rust 禁止一个 impl 块覆盖 supertrait 成员 → 每个特征拆两块（`impl Fragment` assoc+reduce / `impl Reducer` initial-only），树内 ~60 处 + 文档示例 ~30 块全拆；且「Reducer」名字下只剩 initial，名实不符，逼出改名连锁（B'）。否决：迁移面 ~3.5 倍于案 A，收益仅消除一个可一行显式分发绕开的 E0034。
- **案 C `Root<R>` 包装**：不配标记 trait 则 `Root::new(impl Reducer)` 照收 Combine，panic 原样搬家——**加了仪式没加安全**；配标记 trait 则标记 trait 就是案 A 的 Fragment，包装层成为纯附加词汇（每处 mount 点 +1 词）。否决：单一机制做一件事，案 A 严格支配。
- **案 D 挂载面收状态值**：把误用从「运行期炸」恶化为「静默跑错」——分片作根后子 action 可路由、父 action 进 route_miss 克隆环，不炸不响，正踩「禁静默」红线（compose.rs 自述否决过同型「静默空状态」）；且重造 restore 播种与 initial 组合惯用形三处契约。否决：比现状更差。
- **案 E `Combine::initial` 退化 `PS: Default`**：树内已有明文否决（compose.rs「亦不以 PS: Default 界逼非法状态进类型的 State 写退化初值」）——静默空状态是「非法状态进类型」的负例。不重立。
- **只改文档纪律**（现状的来路）：F-07 同型审计已证明纪律不构成机制保障；panic+track_caller 即此路线的终点形态。否决。

**迁移面清点**（每处 file:line → 改法；行号以 9521453 + 现树未提交态为准，文档行号 ±3 容差，内容锚为准）：

| # | 位置 | 改法 |
|---|---|---|
| 1 | `skeleton/src/lib.rs:84-102` | `Reducer` trait doc：initial doc（~94-97）「状态分片…没有合法实现」句改为指向 `Fragment`；删 `#[track_caller]`（:101）及其 doc 句（:98-100）；trait 上挂 `#[diagnostic::on_unimplemented]`；紧随其后新增 `Fragment` trait + blanket impl |
| 2 | `skeleton-infra/src/compose.rs:115` | `C: Reducer<…>` → `C: Fragment<…>` |
| 3 | `skeleton-infra/src/compose.rs:281-324` | `impl Reducer for Combine` → `impl Fragment for Combine`；删 initial+`#[track_caller]`+panic（:292-297）；reduce 原样 |
| 4 | `skeleton-infra/src/compose.rs:93-98` | 「产物是**状态分片不是根**：…`initial` 恒 panic…fail-closed 显式崩溃」段 → 改写为「产物实现 `Fragment` 不实现 `Reducer`：分片作根 = E0277 编译期拒绝」+ 新增 compile_fail doctest（见「实现契约·验收」） |
| 5 | `skeleton-infra/src/compose.rs:~63、~171` | 两个 doctest 的 `fn counter_fragment() -> impl Reducer<…>` → `impl Fragment<…>`；两 doctest use 行（~27、~148）加 `Fragment` |
| 6 | `skeleton-infra/src/compose.rs:239-243` | Combine struct doc「匿名 `impl Reducer` 形态做不到」→「`impl Fragment`」措辞 |
| 7 | `skeleton-infra/src/runtime.rs:617-622` | new() doc「不能直接作 root 传入…装配首次 `initial()` 即 fail-closed panic」→「E0277 编译期拒绝（Fragment≠Reducer，分片无 initial）」 |
| 8 | `skeleton-infra/src/runtime.rs:683` | 删 `#[track_caller]`（用途=转发分片误用 panic，消失） |
| 9 | `skeleton-infra/src/testing.rs:236` | 删 `TestStore::new` 的 `#[track_caller]`（同上） |
| 10 | `tests/combine_teststore.rs:192-200` | 删 `combine_product_is_fragment_initial_panics`（should_panic）——由 #4 compile_fail doctest 接替 |
| 11 | `tests/combine_teststore.rs:441-473` | 删 `combine_misuse_panic_reports_adopter_line`（panic hook Location 断言）——同上接替（`receive_panic_location.rs` 锚的是 `TestStore::receive` 的 track_caller，不受影响，零改动） |
| 12 | `tests/combine_teststore.rs:88、205、275` | `-> impl Reducer<…>` ×3 → `impl Fragment<…>`；:8 use 加 `Fragment` |
| 13 | `tests/api_polish.rs:117-120` | `impl<R> Reducer for Parent<R> where R: Reducer<…>` → `R: Fragment<…>`；:20 use 加 `Fragment`；:122-123 注释改写 |
| 14 | `tests/store_behavior_gaps.rs:135-137` | `impl<F> Reducer for Root<F> where F: Reducer<…>` → `F: Fragment<…>`；:106 `-> impl Fragment`；:10 use 加 `Fragment`；:130-131 注释改写 |
| 15 | `examples/end_to_end.rs:97-98` | 注释「initial 恒 panic（fail-closed）」改写 + `-> impl Fragment`；use 行加 `Fragment` |
| 16 | `docs/reference.md:130` | 装配点段「分片作根在装配首次 `initial()` 即 fail-closed panic」→「E0277 编译期拒绝」 |
| 17 | `docs/reference.md:1873-1888` | 契约段：`// impl Reducer for Combine<…>（…initial 恒 panic）`（:1882）→ `impl Fragment`；:1886 bullet「用作根=fail-closed 显式崩溃」→「用作根=E0277」；:1909 `-> impl Fragment` + 示例块 use 行加 `Fragment` |
| 18 | `docs/tutorials/thirty-min.md:701、770-771、845-846` | use 行加 `Fragment`；doc comment「initial 恒 panic…」改写 + `-> impl Fragment`；「这步教你什么契约」①改写为编译期拒绝 |
| 19 | `docs/tutorials/cheatsheet.md:57、73` | 「共同红线」行改写（分片只实现 Fragment，作根 E0277）；盲测坑表加一行：分片作根 → E0277 `Reducer` not satisfied → 父 Feature 构造根状态并委托 |
| 20 | `CHANGELOG.md` | Unreleased 下新增 Changed 破坏性条目（文案见文末附录） |

零改动核对过：`benches/infra.rs`（无 combine）、`README.md`/`skeleton/README.md`/`skeleton-infra/README.md`（无「分片可作根」误导句）、`tests/receive_panic_location.rs`、`runtime.rs:741-746`（restore 内 `root.initial()`，bound 未动）。收尾断言：`grep -rn "恒 panic|fail-closed|状态分片不是根" src/ tests/ examples/ docs/` 仅剩 Fragment 语境的新表述，无 panic 语义残留。

**实现契约·验收断言**（执行者可直接开工）：
1. compile_fail doctest 进 compose.rs `combine` doc（对表 `skeleton/src/lib.rs:117-121` EffectId 先例，doctest 即外部 crate 视角编译验证）：

   ````rust
   /// 分片作根在类型层拒绝（E0277；doctest 即外部 crate 视角编译验证）：
   ///
   /// ```compile_fail
   /// use skeleton::{EffectBuf, Infallible, NoFx, Reducer};
   /// use skeleton_infra::runtime::SyncStore;
   ///
   /// #[derive(Clone, Debug, PartialEq)]
   /// struct Parent { total: u32, child: u32 }
   ///
   /// #[derive(Clone, Debug, PartialEq)]
   /// enum PAction { Child(u32) }
   ///
   /// struct ChildCounter;
   ///
   /// impl Reducer for ChildCounter {
   ///     type State = u32;
   ///     type Action = u32;
   ///     type Op = Infallible;
   ///     fn initial(&self) -> u32 { 0 }
   ///     fn reduce(&self, s: &u32, a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + a }
   /// }
   ///
   /// let fragment = skeleton_infra::combine!(ChildCounter, Parent.child, PAction::Child);
   /// let _store = SyncStore::new(fragment, NoFx); // E0277：Combine 只实现 Fragment（分片），不实现 Reducer（根契约）
   /// ```
   ````
2. `cargo test -p udf-skeleton-infra --features compose,testing,runtime` 全绿（route_miss 计数、Emit 回灌、lift_fx、宏两形态行为面不受影响——reduce 体未动）；`cargo test --doc` 含新 compile_fail 通过。
3. 全仓 `cargo clippy` 三道闸维持（memory：CI 三道 clippy 闸）；workspace 测试全绿。
4. 收尾 sweep：上表零改动核对段的 grep 零残留。

**规范落点**：错误形态——装配误用从「panic」改为「E0277+on_unimplemented」，全库运行期错误形态零新增（compile-time 即错误形态，对齐 EffectId 受控构造先例）；日志——零（编译期事件无运行期观测面）；命名——`Fragment` 对齐树内既有 identifier 惯用（`parent_child_fragment`/`make_fragment`/`done_fragment`/`shard` 字段）；测试策略——compile_fail doctest 为唯一新验收物（复用库内既有形态，不造第二套），删 2 个由其接替的 panic 测试，净删测试面；文档——reference/cheatsheet/thirty-min/CHANGELOG 四处同步 + 下游迁移说明（附录），无新文档树节点。

**承重假设**：①blanket 方向不变式「每个 `Reducer` 都是 `Fragment`」——若未来出现「是根但不得作子件」的类型，此模型破（现无此需求，YAGNI）；②**关联类型逐字同形**由 blanket impl 结构性保证（不同形=编译错，不漂移、不静默）；③树内对完整 Feature 的 `.reduce` 裸调为零（已核实）——二义 wart 仅存在于树外潜在形态，逃生口 `Reducer::reduce(&f, …)` 写入 Fragment doc；④`skeleton-infra` 内部泛型代码（runtime/testing/tracingx）只 import 其分发经的 `Reducer`——现状成立，若未来同文件引入 `Fragment` 且对 `R: Reducer` 泛型值调 `.reduce` 会出现二义（实现纪律：分发经哪个 trait 就 import 哪个）；⑤0.1.0 未发布、无外部采用方（0005 先例，破坏性直迁无 shim）；⑥`#[diagnostic::on_unimplemented]` 的 message 覆盖 Reducer 上一切 E0277（含真缺 impl 的用户类型），措辞须双向兼容（已按此写）。

**后果**：正——「分片≠根」进类型：误用从装配期 panic 变编译期拒绝，且错误信息自带修复指引；分片与根在签名层可区分（`-> impl Fragment` 自述「我是子件」）；`combine` 子件 bound 放宽为真语义（child 只需 reduce）；净删一个 panic 路径、两个 panic 测试、三处 `#[track_caller]` 孤儿机制；Feature 作者零迁移零新增样板。负——骨架多一个 trait（词汇面 +1，`use skeleton::` 导入清单变长）；同名 `.reduce` 双 trait 二义的罕见形态需一行显式分发（文档义务）；on_unimplemented 的 message 对「真忘 impl Reducer」的用户也出现分片字样（措辞已兼容，仍是轻微噪音）；文档四处 + CHANGELOG 一次性同步成本（已列入迁移表）。

**回滚成本**：低。无 feature 门变化（`Fragment` 随 skeleton 常驻，`compose` 门不动）；回滚 = `impl Fragment for Combine` 改回 `impl Reducer` + initial 恢复 + 删 `Fragment`/blanket/attribute + 文档还原——一次成型，无存量兼容面（MVP 未发布，树内 12 文件全部自持有）。若实施后发现 on_unimplemented 措辞误导，独立回退该 attribute 一行，不牵动主决策。

**风险与证伪（我猜最可能错在哪）**：①二义 wart 的真实频率是我拍脑袋的「树内零、树外罕见」——若首个外部采用方高频裸调 Feature reduce 并撞 E0033/E0034，升格案 B'（rename 翻转），判据记在此；②「分片无初值」若被未来需求打破（如需要构造参数的分片），`Fragment` 需加初始化面——届时新 ADR，本案不预埋；③`#[track_caller]` 三处删除的前提「new 内无 panic」——若后续给 `SyncStore::new` 加 assert，须同时恢复 attribute（删除理由随用途消失，不随代码惯性）。

---

## 附录 A：CHANGELOG 条目文案（Unreleased → Changed）

> ### Changed（combine 分片作根编译期拒绝：Fragment/Reducer 契约拆分，破坏性）
> - `skeleton` 新增 `Fragment` trait（reduce-only 状态分片契约）+ blanket `impl<T: Reducer> Fragment for T`（每个根都是合法子件）；`Reducer` 语义收窄为**根契约**（initial+reduce，trait 本体与既有 `impl Reducer` 零变化）。
> - `combine`/`combine!` 产物 `Combine<…>` 由 `impl Reducer` 改为 `impl Fragment`：分片作根（`SyncStore::new(分片, …)`）从「装配期 panic」升级为 **E0277 编译期拒绝**（`#[diagnostic::on_unimplemented]` 错误信息自带「分片≠根」修复指引）；`combine` 的子件 bound 由 `Reducer` 放宽为 `Fragment`（完整 Feature 经 blanket 仍可作子件）。随拆分删除误用 panic 与三处孤儿 `#[track_caller]`；两处分片误用 panic 测试由 compile_fail doctest 接替。决策依据 ADR 0009。（**破坏性直迁**：0.1.0 未发布无外部采用方，按 ADR 0005 先例无 shim。）

## 附录 B：下游迁移说明草稿（随 CHANGELOG 发布）

> **combine 分片：作根从 panic 变编译错（迁移说明）**
> - 只写 Feature（`impl Reducer` 含 initial+reduce，`SyncStore::new(feature, fx)` 挂载）：**零改动**。
> - 持有分片并注解返回类型（`-> impl Reducer<…>` 接 `combine`/`combine!` 产物）：改为 `-> impl Fragment<…>`，use 行加 `skeleton::Fragment`；委托调用 `.reduce(...)` 写法不变。
> - 曾把分片传给 `SyncStore::new`/`TestStore::new`：此前装配期 panic，现在编译期 E0277 即修复指引——根状态由父 Feature 的 `initial` 显式构造（含子切片初值），父 Feature 本体才是根（`tests/combine_teststore.rs` 固定装置为标准形）。
> - 少见形态：同文件 import `Fragment` 且直接裸调完整 Feature 的 `.reduce()` 会报 E0034 二义——用 `Reducer::reduce(&feature, …)` 显式分发（框架树内无此形态）。

---

## 实施注记（2026-10-28 落地对账）

- 迁移表 20 处全部落地；**清单外增补 1 处**：`skeleton-infra/examples/perf_batchc.rs:22` 的 `use skeleton::{EffectBuf, NoFx, Reducer, Store as _}` 需加 `Fragment`（该示例直接对 `combine!` 产物调 `.reduce()`，未列入迁移表）——教训：迁移表以**全仓符号调用点 grep 对账**为准，行号清单会漏示例/bench 等第三处。
- `README.md` / `skeleton/README.md` 的契约 trait 清单各加一词（`Fragment`）；ADR「零改动核对」原判「README 无误导句」仍成立（增补是被删 trait 清单的完整性补齐，非修错误表述）。
- `runtime.rs` 的 `restore_from` 上 `#[track_caller]` **保留**（不在本案三处删除范围）：它兜的是用户 `initial` 自身 panic 的装配行归因；随 `SyncStore::new` 的 attribute 删除，`new` 不再向该处转发——即「用户 `initial` 在 `new` 中 panic 时报库内 `new` 行」为本案的既有代价（无测试钉此位置；若需恢复，仅给 `new` 补回一行 attribute，不牵动主决策）。
- 验收原始输出与基线对比见 `verify/2026-10-28-adr0009/`（基线 654+109=763 → 现 652+110=762，差值 = 删 2 个 panic 测试 + 增 1 个 compile_fail doctest，零失败）。

---

## 拍板记录（2026-10-28 用户线 PM）

- **决定**：采纳**案 A**——契约拆分（`skeleton` 新增 reduce-only `Fragment` + 单向 blanket `impl<T: Reducer> Fragment for T`；`Reducer` 语义收窄为根契约；`Combine` 改 `impl Fragment`，`initial` panic 段净删；挂载面 bound 不动，分片作根 = E0277 + `#[diagnostic::on_unimplemented]` 指引）。备选 B/B'/C/D/E 与「只改文档纪律」一并否决（理由见「被否决备选」）。
- **理由**：迁移税最低（树内约 60 处 `impl Reducer` 一处不动，仅 12 文件 20 处签名/文档落点）；把误用从装配期 panic 升级为编译期拒绝，错误信息自带修复指引（「非法状态进类型」的兑现）；净删一个 panic 路径 + 两个 panic 测试 + 三处孤儿 `#[track_caller]`；0.1.0 未发布无外部采用方，破坏性直迁无 shim（0005 先例）。案 B 的收益（消 E0034 二义）对应树内不存在、树外罕见、有一行逃生口的形态，代价是永久样板税 + 全树拆块。
- **错了损失什么**：①若「二义 wart 树内零、树外罕见」判断错（首个外部采用方高频裸调完整 Feature 的 `.reduce()` 撞 E0034），升格案 B'（rename 翻转），判据与此记录同行；②若「分片无初值」被未来需求打破（需构造参数的分片），`Fragment` 需加初始化面——届时新 ADR，本案不预埋；③三处 `#[track_caller]` 删除的前提「`new` 内无 panic」若被后续 assert 打破，须同时恢复 attribute。回滚成本低（`Combine` impl 改回 + 删 trait/blanket/attribute + 文档还原，一次成型，无存量兼容面）。
