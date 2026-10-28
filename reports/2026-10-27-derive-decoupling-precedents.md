# 外部调研：Rust 框架用 derive 宏 + 类型映射解耦采用方代码的成熟先例与性能特征

日期：2026-10-27 · 调研员：DeriveScan（只读外部资料，未改任何仓库文件）

---

## 1. Druid/Xilem 的 `#[derive(Data)]`（值语义状态派生）

**机制**：Druid 要求应用状态实现 `trait Data: Clone + 'static { fn same(&self, other: &Self) -> bool; }`，框架在每次 update 时用 `same()` 做值比较决定是否重绘。派生宏（druid-derive 0.5.1）生成的内容极简：struct 展开为逐字段 `same_fn(&self.f, &other.f) &&*` 短路递归；enum 展开为按变体逐字段 match；C 风格 enum 直接 `self == other`（要求 PartialEq）；支持 helper 属性 `#[data(ignore)]`、`#[data(same_fn="path")]`、`#[data(eq)]`，泛型参数自动追加 `T: Data` bound，生成路径全部用 `::druid::Data` 绝对路径保证卫生。std 集合类型不实现 Data（比较贵），官方路径是包 `Arc` 或开 `im` feature 用持久化集合。

- trait 与派生行为文档：https://docs.rs/druid/latest/druid/trait.Data.html
- 派生宏源码（生成什么一目了然）：https://github.com/linebender/druid/blob/master/druid-derive/src/data.rs
- 宏 re-export 页：https://docs.rs/druid-derive/latest/druid_derive/derive.Data.html

**性能**：生成的代码是零分配的布尔字段比较（O(字段数)），`same` 官方定位"always fast"；但值语义本身要求 clone + 全量比较，且内部可变性被事实禁止——Druid 作者本人把这条列为架构缺陷（"The app data must have a Data bound, which implies cloning and equality testing. Interior mutability is effectively forbidden"）：https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html

**演进信号**：Druid 已停止维护（README 标注 UNMAINTAINED，https://github.com/linebender/druid ）；继任者 Xilem 从 0.1.0 起就完全抛弃了 Data/Lens 派生——官方示例的应用状态是无任何派生的 plain struct（`struct TaskList`、`struct Calculator`），视图函数签名 `fn app_logic(data: &mut Calculator) -> impl WidgetView<Calculator>`：
- Xilem 0.1.0 示例（无派生）：https://docs.rs/crate/xilem/0.1.0/source/examples/to_do_mvc.rs
- 当前 main 分支示例（同样无派生）：https://github.com/linebender/xilem/blob/main/xilem/examples/calc.rs

## 2. Bevy 的 `IntoSystem` + `#[derive(Component, Event, Resource)]`

**机制**：普通函数 `fn my_system(query: Query<&Transform>) {}` 不实现任何 trait；bevy_ecs 用**宏批量生成的 blanket impl**（0..=16 个参数各一个）为一切 `FnMut(In, F0..Fn) -> Out`（每个 `Fi: SystemParam`）实现 `SystemParamFunction`，再经 `IntoSystem::into_system(fn)` 转成具体类型的 `FunctionSystem`——用 `Marker` 泛型（形如 `(HasSystemInput, fn(In, F0..) -> Out)` 的元组类型）区分"同签名不同语义"的 impl。类型注册侧同理：`#[derive(Component)]` 只生成一个纯 trait impl，属性承载元数据（`#[component(storage = "SparseSet")]` 选存储、`#[component(immutable)]`、`#[require(B, C(1))]` 声明必需组件及构造器）；**派生宏不生成任何注册代码**，组件在首次使用时经运行时注册表（`ComponentsRegistrator`）按类型去重拿到 `ComponentId`。`derive(SystemParam)` 生成辅助状态结构 `FetchState` 时特意包进 `const _: () = { ... }` 匿名块避免污染用户命名空间。

- IntoSystem（"to get a system from a function"）：https://docs.rs/bevy_ecs/latest/bevy_ecs/system/trait.IntoSystem.html
- SystemParamFunction 的 16 参 blanket impl（源码 function_system.rs:995）：https://docs.rs/bevy_ecs/latest/bevy_ecs/system/trait.SystemParamFunction.html
- Component trait/派生属性文档：https://docs.rs/bevy_ecs/latest/bevy_ecs/component/trait.Component.html
- 派生宏实现（Bundle/Resource/Event/SystemParam，含 FetchState 卫生技巧）：https://github.com/bevyengine/bevy/blob/main/crates/bevy_ecs/macros/src/lib.rs

**性能**：函数→系统是单态化静态分发（无 dyn/boxing）；组件存储二选一有明确性能语义——默认 Table（为查询迭代优化）或 SparseSet（为插删优化），均为连续/稀疏数据布局，派生本身零运行时开销，注册成本一次性。详见 Component 文档"strategies with their own performance implications"段。

## 3. Yew 的 reducer/use_reducer 与组件宏

**机制**：Yew（0.23）把采用方面收窄到"plain struct + 一个手写 trait impl"：状态是普通 struct，Elm 式更新就是 `impl Reducible { type Action; fn reduce(self: Rc<Self>, action: Action) -> Rc<Self>; }`——**没有派生宏**，就是一个普通 trait impl；`use_reducer(init)` 只在 reduce 返回新 `Rc` 时触发重渲染（`use_reducer_eq` 版本先比较新旧再决定）。组件是 `#[component]` 标注的普通函数；Props 用 `#[derive(Properties, PartialEq)]`（默认全部字段必填，`#[prop_or_default]`/`#[prop_or(expr)]`/`#[prop_or_else(fn)]` 给默认值），由 yew-macro 生成。

- Reducible trait：https://docs.rs/yew/latest/yew/functional/trait.Reducible.html
- use_reducer（含完整 plain-struct 示例）：https://docs.rs/yew/latest/yew/functional/fn.use_reducer.html
- Properties 派生与字段属性：https://yew.rs/docs/concepts/function-components/properties
- yew-macro（提供 html! 与 Properties 派生）：https://docs.rs/yew-macro/latest/yew_macro/

**性能**：Props 内部是引用计数（Rc 共享指针下发，避免整份 clone），重渲染按组件粒度隔离；Reduce 走 Rc 替换而非原地 mutation，代价是每次更新一次 Rc 分配。

## 4. Sauron（Elm 式宏用法代表）

**机制**：Sauron（0.70.0，2025-01-27 发布）自称"inspired by elm-lang and is following The Elm Architecture"。采用方只写三样：普通 `struct App`（model）、普通 `enum Msg`、`impl Application for App`（view/update 是 trait 方法）——**组件模型零派生宏**，宏只出现在视图与样式层：`node!{ <main>... }`（类 HTML 的函数式宏）与 `jss!{}`（CSS 宏），事件直接是 `on_click=|_| Msg::Increment` 闭包。对比之下 Seed（Elm 式同类）最后版本 0.10.0 停在 2023-03-07（crates.io API 数据，https://crates.io/api/v1/crates/seed ），已实质停滞，故不作为对标。

- README（含完整 counter 示例）：https://github.com/ivanceras/sauron
- crate 文档（vdom diff/patch 架构）：https://docs.rs/sauron/latest/sauron/

**性能**：视图经 mt-dom 虚拟 DOM diff 生成 patch（O(树) diff），组件即普通类型静态分发，无派生代码路径。

## 5. proc-macro 依赖成本事实（对「框架零三方」卖点）

**事实链**（T1 官方）：
- proc-macro crate 是独立 crate 类型（`[lib] proc-macro = true`），"Procedural macros **run during compilation**, and thus have the same resources that the compiler has"——它以 dylib 形式被宿主编译器加载执行。https://doc.rust-lang.org/reference/procedural-macros.html
- Cargo 中 build 侧依赖与运行时依赖分离："build dependencies are **not available to the package itself** unless also explicitly added in `[dependencies]`"，且交叉编译时按 host 构建。https://doc.rust-lang.org/cargo/reference/build-scripts.html#build-dependencies
- 由此（[INFERENCE，基于上两条 T1]）：syn/quote/proc-macro2 只进 **host 侧构建图**、被链进 proc-macro dylib，**不进采用方 target 产物/运行时依赖树**；但它们仍会出现在 `cargo tree` 的依赖边上，并拉长构建关键路径。

**构建时间证据**：
- serde_derive 的依赖链 proc-macro2→quote→syn→serde_derive→serde 是最长依赖深度链，wasmtime 把 serde 与其 derive 解耦（运行时 crate 不依赖 derive crate）后 release 构建实测 24.75s→22.45s：https://github.com/bytecodealliance/wasmtime/pull/6917 （引 https://github.com/serde-rs/serde/issues/2584 ）
- 实测数据（2023-07，M2 Max，rustc 1.68.2，100 个嵌套 struct 的 crate）：内置派生 Debug 200ms / +Eq,PartialEq,Ord,PartialOrd 280ms（≈0.2–0.8ms/类型）；加 serde 派生 860ms（≈6.6ms/类型，其中 Deserialize 展开体积约为 Serialize 的 2 倍；仅 Serialize ≈1.3ms/类型）：https://speedyleion.github.io/rust/2023/07/21/the-cost-of-derives.html

**对卖点的实际影响**：「运行时零三方」完全守得住（syn/quote 不进最终二进制）；代价转移到**构建期**——采用方冷构建要多编译一遍 syn 系（每个项目各自编译，默认不跨项目共享缓存）。规避范式就是 wasmtime 那条：**运行时 crate 绝不依赖 derive crate**，derive 挂独立 crate/可选 feature。

## 6. 反面证据：derive 宏方案的已知坑

- **编译时间**：见第 5 节数据；决定性因素是依赖**深度**而非宽度，derive 链正好是最长链（wasmtime PR，T2，2023-08）。
- **IDE 支持**：rust-analyzer 官方博客（matklad, 2021）把 proc-macro 列为"the hardest nut to crack"：宏必须编译并跑在独立进程（可能死循环/崩溃/不确定输出，IDE 不能死）；"mapping back"（把展开代码上的分析结果映射回用户源码）只能靠启发式；支持成本是批处理编译器的 X²。https://rust-analyzer.github.io/blog/2021/11/21/ides-and-macros.html
- **错误消息可读性与可测试性**：保证宏报错"落在正确的 token 上"需要专门的 compile-fail/ui 测试（trybuild 快照 .stderr，随 rustc 版本漂移而脆）：https://docs.rs/trybuild/latest/trybuild/
- **卫生**：proc-macro 是 unhygienic 的，作者必须全用 `::std::option::Option` 式绝对路径、生成名加 `__` 前缀防碰撞：https://doc.rust-lang.org/reference/procedural-macros.html （druid-derive 实践 `::druid::Data`，bevy 用 `const _: ()` 块藏 FetchState）
- **架构级反噬**：Druid 的 Data bound 被其作者亲自否定（clone/相等比较负担、禁内部可变性），Xilem 整体弃用该派生改纯 `&mut State` 函数（见第 1 节）——**把框架 bound 焊死在采用方核心类型上，框架换代时采用方被迫大改**。

---

## 对 udf-frame 的可借鉴模式清单（≤5 条）

1. **Blanket impl 映射 plain 函数，不要求实现框架 trait**——学 Bevy：用宏生成的 0..=N 参 blanket impl（`FnMut(In, P0..Pn) -> Out` 且 `Pi: 框架参数 trait`）+ Marker 类型区分同签名，让采用方的 update/reduce 就是普通函数。来源：https://docs.rs/bevy_ecs/latest/bevy_ecs/system/trait.SystemParamFunction.html + https://github.com/bevyengine/bevy/blob/main/crates/bevy_ecs/macros/src/lib.rs
2. **派生宏只生成 trait impl（纯类型级映射），注册延迟到运行时注册表**——学 Bevy Component / Druid Data：生成物无注册代码、无状态，注册按类型在框架注册表去重完成，采用方类型保持平凡、注册成本一次性。来源：https://docs.rs/bevy_ecs/latest/bevy_ecs/component/trait.Component.html + https://github.com/linebender/druid/blob/master/druid-derive/src/data.rs
3. **derive crate 独立、运行时 crate 零依赖它**——学 wasmtime 反向教训：skeleton（契约层）不依赖任何 proc-macro 与 syn 系；derive 放独立 companion crate（可 feature 门控），保证「零三方」在运行时产物和构建关键路径两头都成立。来源：https://github.com/bytecodealliance/wasmtime/pull/6917
4. **生成代码卫生三件套**：`::框架::` 绝对路径 + `quote_spanned` 定位错误到用户字段 + 生成辅助类型藏进 `const _: ()` 或 `__` 前缀。来源：https://doc.rust-lang.org/reference/procedural-macros.html#procedural-macro-hygiene + druid-derive/bevy 源码实践
5. **派生必须可选，plain 路径是一等公民**——吸取 Druid/Xilem 教训：不把框架 trait bound 焊死在采用方核心状态类型上（Data bound 曾迫使 clone+全量比较、禁内部可变性，最终被 Xilem 弃用）；为每个派生提供手写 impl 逃逸口，并用 trybuild ui 测试看护错误消息质量。来源：https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html + https://github.com/linebender/xilem/blob/main/xilem/examples/calc.rs + https://docs.rs/trybuild/latest/trybuild/
