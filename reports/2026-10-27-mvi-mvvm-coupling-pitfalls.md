# 外部扫描：MVI / MVVM / 跨语言绑定项目的耦合问题与常见坑 —— 各框架的「踩平机制」提炼

- 日期：2026-10-27 · 调研员：PitfallScan（只读外部资料，未改任何仓库文件；本报告是唯一产物）
- 目标：为 udf-frame「View Bridge」（skeleton 契约层 ViewMap/IntentMap + infra 层 ViewBridge）找出**别人踩过的坑**与**别人把坑踩平所用的机制**，让采用方到手即用、不踩坑。
- tier 约定：**T1** = 官方文档 / 规范 / 语言参考（Rust Reference、RFC、Android Developers、React.dev、Vue 官方指南、Elm 官方指南、Serde 官方、Kotlin 官方、Protobuf 官方、Dart 官方）；**T2** = star≥100 且近一年活跃的 MIT/Apache 项目的一手源码/官方仓库文档（React 251k★/MIT、Vue core 54.9k★/MIT、Redux Toolkit 11.2k★/MIT、Reselect 19k★/MIT、Serde 10.9k★/Apache-2.0、cxx 6.85k★/Apache-2.0、Tauri 112k★/Apache-2.0、Solid 36.2k★/MIT、Bloc 12.5k★/MIT、Druid 9.7k★/Apache-2.0、Xilem 5.6k★/Apache-2.0、protobuf 72.1k★、nomicon 2.3k★/Apache-2.0；star/活跃度于 2026-10-27 经 GitHub API 核对）；**T3** = 社区博客 / 论坛 / issue 讨论。

---

## 1. MVVM 巨型 ViewModel / 业务逻辑漏进 ViewModel（God object 腐化）

**机制**。MVVM 里 ViewModel 同时承担「持有 UI 状态」「承接 UI 事件」「执行业务逻辑」三件事。三件事的**变化速率与生命周期都不同**：UI 状态随帧变，业务逻辑随产品需求变（周/月级），而 ViewModel 的生命周期被 Android 绑定在 Activity/导航目的地上。三者混在一处后，任何一处变化都牵动整类。

**真实失效形态**。
- Google 官方文档的判定原话（比任何博客都硬）：把交互逻辑放在 UI 里「can quickly get unwieldy as the UI takes on too much responsibility. Furthermore, this can affect testability because the resulting code is tightly coupled」；并给出业务逻辑归属的硬规则：「Business logic … is usually placed in the domain or data layers, **but never in the UI layer**」。
- God ViewModel 的具体症状（社区归纳，T3）：**责任泄漏**（responsibility leakage）—— ViewModel 越写越大，最终同时做参数校验、网络编排、缓存策略、格式化、导航决策、埋点。后果是：单元测试要 mock 半个世界；换一个屏幕尺寸要改同一个类；改一个字段名要翻三个文件。
- Android 官方对「ViewModel 太大」的**官方处方**是加一层 domain layer（use case），并明确写了这条层的目的之一就是「**It avoids large classes by allowing you to split responsibilities**」。同时给了 use case 自身的纪律：每个 use case 只负责一个动作、**不含可变数据**、必须 main-safe（重活自己换线程）。
- 反面证据：ViewModel 官方文档区分「business logic state holder」（ViewModel）与「UI logic state holder」（普通类）——**UI 逻辑不该进 ViewModel**，因为它随配置变更失效而 ViewModel 不失效，二者生命周期错配。这是官方自己承认的 ViewModel 腐化入口。

**证据**。
- https://developer.android.com/topic/architecture/ui-layer — **T1**（“Unless the UI state is very simple, make sure the UI's sole responsibility is to consume and display UI state.”）
- https://developer.android.com/topic/architecture/domain-layer — **T1**（“It avoids large classes by allowing you to split responsibilities.”；use case 不含可变数据；命名 = 动词+名词+UseCase）
- https://developer.android.com/topic/architecture/ui-layer/stateholders — **T1**（业务逻辑 state holder vs UI 逻辑 state holder 的生命周期二分表）
- https://developer.android.com/topic/architecture/recommendations — **T1**（“Recommended in big apps: Use a domain layer with use cases … to simplify the business logic complexity of a particular ViewModel”）
- https://medium.com/@guillermo.arispe/the-god-viewmodel-problem-in-android-a-craftsmanship-approach-1456d562641d — **T3**（“The God ViewModel problem … responsibility leakage”；直接读被 403，此处仅取检索摘要，未逐字引用全文）

**踩平机制（别人怎么把这类坑踩平）**。
1. **Android 官方：ViewModel 只产出 UI state，不持有业务规则**；业务规则下沉到「一个动作一个 use case」的 domain layer，且 use case **无状态**（无状态 = 可测试 = 可复用 = 可跨屏）。UI 逻辑（导航、Snackbar、动画）留在 UI 侧，因为它的生命周期跟着 UI 走。
2. **Redux 官方 Style Guide**：「Put as Much Logic as Possible in Reducers」——业务逻辑不是散在事件处理器里，而是集中在 reducer（纯函数）里，理由写得很直白：reducer 永远好测（纯函数，assert 输入输出）、逻辑集中一处你知道去哪儿找、避免「在 action 创建处算一半又在 reducer 里算一半」的形态漂移。**Reducer = ViewModel 的「无状态化、纯化」版本**。
3. **Redux 官方另一条**：「Organize State Structure Based on Data Types, Not Components」——状态按**数据类型**切，不按组件切，理由是 store 与组件不存在 1:1 关系，多个组件常读同一片状态。

**对 udf View Bridge 的启示**。ViewMap 就是「产出 UI state」的那一层，因此它**必须**是纯投影（State → 视图值），业务规则一律不进 `project`——这与方案的「投影纯函数纪律」一致，外部证据把它从「风格偏好」升级为「业界公认的腐化入口」。落地一句：**ViewMap 只许读 State 的一部分并映射，不许发起 IO/编排业务**；谁要业务规则，就在 reducer/领域层里做，桥不碰。

---

## 2. 双向绑定与陈旧状态（stale closure / 失去单向性）

**机制**。双向绑定的危险不在「两个方向」，而在**读取时机**：如果读取方拿到的是「渲染那一刻的快照」而不是「当前值」，写入就会基于旧值计算。框架若允许 UI 直接改 store 状态，就同时失去了「唯一写入口」和「同一拍的读一致性」。

**真实失效形态**。
- React 官方的「state as a snapshot」是最经典的陈旧闭包标本（T1）：一次点击里连写三次 `setNumber(number + 1)`，`number` 恒为该次渲染的值 0，结果只 +1 不是 +3；`setTimeout(() => alert(number), 3000)` 弹出的仍是 0——官方原话：“A state variable's value never changes within a render, even if its event handler's code is asynchronous.”
- React 官方另一处更狠：**「若 Effect 立即回写 state，React 会从头再跑一遍整个流程」**——`useEffect(() => setFullName(firstName + ' ' + lastName), [firstName, lastName])` 会先以陈旧的 `fullName` 渲染一次再重渲染一次；官方称之为「cascading updates」，并给出正解：**在渲染中直接算**（“Don't put it in state. Instead, calculate it during rendering.”）。
- Vue 官方的对应坑更隐蔽：**`reactive()` 返回的是 Proxy，与原对象 `!==`**。官方警告：“Only the proxy is reactive - mutating the original object will not trigger updates. Therefore, the best practice … is to exclusively use the proxied versions of your state.”，且 `reactive()` **不能整体替换**（换引用 = 丢失响应式连接），**不能解构**（`let { count } = state` 之后 `count++` 不再触发更新）。这就是「双脑」的技术根：**同一个值存在代理态和原始态两份，改了错误的那份，UI 不动**。

**证据**。
- https://react.dev/learn/state-as-a-snapshot — **T1**
- https://react.dev/learn/you-might-not-need-an-effect — **T1**（“it does an entire render pass with a stale value … then immediately re-renders with the updated value”）
- https://vuejs.org/guide/essentials/reactivity-fundamentals.html — **T1**（Reactive Proxy vs. Original；Limitations of `reactive()`：Limited value types / Cannot replace entire object / Not destructure-friendly）
- https://vuejs.org/guide/extras/reactivity-in-depth.html — **T1**（track/trigger 伪码：依赖靠**读取时**收集，写入时触发）

**踩平机制**。
1. **React：单向数据流 + 快照语义 + 派生量在渲染中算**。状态只通过 setState 单向写；读取永远拿当前 render 的快照；派生量（`fullName`、`visibleTodos`）**不入 state**，渲染时直接算（必要时 `useMemo` 缓存）——从根上消灭「派生量与源状态不同步」这一类。
2. **Vue：单一响应式载体纪律**。`ref()` 是首选声明 API（官方因为 `reactive()` 的三条限制而推荐 `ref`），**只经代理访问状态**，不解构、不整体替换——把「两份状态」在 API 层就堵死。
3. **Vue 的 watchEffect 调试钩子**（`onRenderTracked` / `onRenderTriggered`）——把「谁在跟踪、谁触发了重渲染」变成可观测事件，官方建议直接下 `debugger`：**依赖收集是隐式的，必须提供内省手段**，否则陈旧状态无法定位。

**对 udf View Bridge 的启示**。桥出站面是「替换式快照 + 路径」而非「双向可写引用」，天然免疫 Vue 式双脑；但**派生量陈旧**是同类风险的移植版——桥的 `Derived`/`Sentinel` 必须保证「读到的永远是当前值」，而不是「上一次拍的值」。同理，投影结果不得被 UI 端就地修改（桥出的 JSON 若是可变对象，UI 改了它，桥内 last 槽与 UI 分叉 = 事实上的双脑），**出站值应当是不可变快照语义**。

---

## 3. 状态爆炸（布尔旗子 soup）vs 密封状态 / 非法状态进类型

**机制**。n 个布尔旗子表示状态，理论上有 2ⁿ 种组合，其中绝大多数是非法的（「loading 且 error 且 有数据 且 isEmpty」）。布尔旗子的成本不在数量，而在**组合合法性无法被类型系统表达**，非法组合要靠运行时断言或纪律拦。

**真实失效形态**。
- React 官方给了精确的病灶：**`isSending` 与 `isSent` 两个布尔**，“if you forget to call `setIsSent` and `setIsSending` together, you may end up in a situation where both `isSending` and `isSent` are `true` at the same time.” 官方称之为「opens the door for "impossible" states」，并给出正解：换成**一个三态变量** `status: 'typing' | 'sending' | 'sent'`。**关键洞察：不是「少写几个布尔」，而是「把互斥关系编码进类型」**。
- Kotlin 官方的 sealed class 就是这件事的工业级答案：**「All direct subclasses of a sealed class are known at compile time」**，且「No other subclasses may appear outside the module and package」；配 `when` 表达式可**穷尽检查**，不需要 `else` 分支。Kotlin 官方还专门给了「状态管理」用途场景：`sealed class UIState { Loading; Success(data); Error(exception) }`。
- Flutter/Bloc 官方文档对「具体类 + 状态枚举」方案的判词（这是行业里**最诚实的反面评估**）：**「Not Type Safe: It's possible to `emit` a malformed state which can lead to bugs」**、**「Bloated: Results in a single state that can become bloated with many properties over time」**。而 sealed class 方案：**「Type Safe / Explicit / Exhaustive」**，代价是「Verbose / Complex，新增属性要改每个子类」。
- React 官方另一条同源纪律：**「Avoid redundancy in state」**（能从已有状态算出来的，不要存）——冗余存储 = 第二份真相 = 布尔 soup 的姐妹形态。

**证据**。
- https://react.dev/learn/choosing-the-state-structure — **T1**（Principles：Group related state / **Avoid contradictions in state** / Avoid redundant state / Avoid duplication / Avoid deeply nested state；正文用 `isSending`+`isSent` 演示“impossible states”并改为单 `status` 三态）
- https://kotlinlang.org/docs/sealed-classes.html — **T1**（编译期已知子类、`when` 穷尽、state management 场景）
- https://bloclibrary.dev/modeling-state — **T1**（Bloc 官方对两种建模法的 Pros/Cons 逐条对比，含「Not Type Safe」「Bloated」「Exhaustive」）
- https://developer.android.com/topic/architecture/ui-layer — **T1**（“Use a single UI state object to handle states that are related to each other. This leads to fewer inconsistencies”）
- https://developer.android.com/topic/architecture/ui-layer/events — **T1**（`LoginUiState(isLoginInProgress, errorMessage, isUserLoggedIn)` 三字段共处一个不可变 state；官方要求「business logic must always be applied before UI logic」）

**踩平机制**。
1. **Kotlin：sealed + 穷尽 when**——非法组合在类型层不可表示，新增状态编译器强制你处理（漏处理 = 编译错误）。
2. **Bloc：显式承认两种代价并让用户选**——「不可变但胖」vs「密封且穷尽」；无论选哪种，**状态对象是不可变的**（`final class` + `Equatable`）。
3. **Android：把相关状态合并成单一不可变 UiState**，并明确「不要在 UI 里直接改 UI state，否则形成 multiple sources of truth」。官方点名布尔组合的具体危害：“If you expose the list of news items and the number of bookmarks in two different streams, you might end up in a situation where one was updated and the other was not.”
4. **Rust 侧的语言级答案**：`enum` + 穷尽 `match`（编译器保证新增变体必须处理），以及 `#[non_exhaustive]`（给外部 crate 留通配臂的兼容出口）。

**对 udf View Bridge 的启示**。方案的「一绑定一变体的采用方 enum」正是 sealed-state 形态的 Rust 版——**采用方的视图词表应建模为 enum（互斥状态 + 每变体自带载荷），而不是「每个绑定一个 bool 旗子」**。桥这一侧可加一条纪律：**绑定词表 = 一组互斥/可组合的类型化视图值，禁用布尔旗子表达状态**；同时 `View: Clone + PartialEq` 的 bound 已经把「值语义 diff」锁死，天然排斥可变共享。

---

## 4. 派生状态陈旧与记忆化陷阱（输入等值 vs 引用等值、缓存失效）

**机制**。派生值缓存要判断「输入变了没有」。判断依据选错，就出现两种相反的病：**引用等值**（每次重建对象都算变了 → 缓存永不命中 → 过度重算）或者**值等值判据太粗**（只看顶层字段 → 深层变了没被发现 → 静默陈旧）。

**真实失效形态**。
- Redux 官方文档给了教科书级反例（T1）：`useSelector(state => state.todos.filter(todo => todo.completed))` —— 「❌ WARNING: This _always_ returns a new array reference, so it will _always_ re-render!」。原因：`useSelector` 靠 **`===` 引用等值**判断要不要重渲染，而 `filter`/`map` 每次返回新数组引用。**引用等值 = 缓存永不命中 + 每次 dispatch 全量重渲染**。
- Reselect 官方（Redux 生态）把这一族坑列成清单（T1）：**input selector 必须只做「取值」不做「计算」**，否则每次都返回新引用 → 一层缓存直接被击穿；**result function 不能原样返回 input**（“BROKEN: this will not memoize correctly”）；**不能用 `state => state` 作为 input**（“the input changes on every dispatch, so the result function re-runs every time”）。Reselect 5 用 `weakMapMemoize`（按参数引用的 WeakMap 缓存）取代 Reselect 4 的 `lruMemoize`（容量 1 的 LRU），官方给的动机正是「多组输入轮流调用时旧结果被逐出」。
- React 官方 `memo` 文档给出同源的另一半：**默认用 `Object.is` 逐 prop 浅比较**，`Object.is({}, {})` 是 `false`；因此「a single value that's "always new" is enough to break memoization for an entire component」。官方给出的五条性能原则里第一条是「**Prefer local state and don't lift state up any further than necessary**」，第二条是「**Avoid unnecessary Effects that update state**」。
- Vue 官方给出**缓存失效的边界条件**（T1）：computed 是**按响应式依赖缓存**的，依赖不变就复用旧结果；但官方当场给了反例——`computed(() => Date.now())` **永远不会更新**，因为 `Date.now()` **不是响应式依赖**。这条极重要：**依赖追踪是自动的，但自动依赖 = 你实际读了什么；如果你的计算读了非响应式来源，它就静默冻结**。
- 更硬的教训来自 React 官方：**Effect 派生是「多跑一趟渲染 + 陈旧一帧」的来源**——“it does an entire render pass with a stale value … then immediately re-renders”。

**证据**。
- https://redux.js.org/usage/deriving-data-selectors — **T1**（`===` 引用等值与 filter/map 新引用；`createSelector` 两层记忆化；Redux 5 弱映射缓存）
- https://redux.js.org/reselect/usage/best-practices — **T1**（Best Practices 四条 + Common Mistakes 三条）
- https://react.dev/reference/react/memo — **T1**（`Object.is` 浅比较；「always new」一个值即可击穿整棵子树）
- https://react.dev/learn/you-might-not-need-an-effect — **T1**（渲染中计算 vs Effect 派生；陈旧一帧）
- https://vuejs.org/guide/essentials/computed.html — **T1**（按响应式依赖缓存；`Date.now()` 例外；方法 vs computed 的差异）
- https://docs.solidjs.com/advanced-concepts/fine-grained-reactivity — **T1**（memo 的早退分支导致依赖集变化；同步跟踪的语义）

**踩平机制**。
1. **Redux/Reselect：显式两级记忆化 + 输入选择器纪律**。第一层比参数引用，第二层比**输入选择器的返回值**；结果函数只在此后跑。**判据 = 输入选择器返回的那个值本身**（可等值比较的原始值，如 `state.todos`），而不是「上游对象引用」。
2. **React：派生在渲染中算，昂贵时 `useMemo`，不用 Effect 派生**。
3. **Vue：computed 缓存按依赖自动失效，但要求「非响应式来源不进计算」**（Date.now 反例）。
4. **Solid：memo 缓存 + 依赖集随分支变化**——派生值的依赖是「运行时实际读到的信号集合」，early-return 会改变依赖集；官方同步跟踪语义保证「派生永远与依赖一致」。
5. 共同点：**判据要么是「值等值」（Rust `PartialEq`、JS `Object.is`），要么是「显式声明的依赖集」，绝不是「上游对象引用」**——因为上游引用在每次重建时都会变。

**对 udf View Bridge 的启示**。这与方案 §5 拍板的 **L1 输入键等值**完全同构，且外部证据把「输入等值 vs 引用等值」这条风险从 INFERENCE 升级为有 T1 出处的事实。两条可直接抄的纪律：①**派生判据必须是值等值**（`In: PartialEq`），不得用引用/指针身份；②**依赖声明必须覆盖计算实读范围**（方案的「input 覆盖实读」红线）——Vue 的 `Date.now()` 反例证明「声明窄于实读」在框架层看起来完全正常，只会静默冻结。

---

## 5. 事件 vs 状态（一次性事件在重建/配置变更后丢失）

**机制**。**状态是「现在是什么」，事件是「发生了什么」**。事件是瞬时的、一次性的；状态是可重复读取的。一次性事件若只经「事件流」投递，消费者暂停/重建时事件就永久消失——因为「发生过」这件事没有被任何持久的地方记下。

**真实失效形态**（这是本次扫描中外部证据最硬的一类）。
- Android 官方（Views 版）一句话定性：**“UI actions that originate from the ViewModel—ViewModel events—should always result in a UI state update.”** 理由写得很清楚：“This complies with the principles of Unidirectional Data Flow. It **makes events reproducible after configuration changes and guarantees that UI actions won't be lost.** Optionally, you can also make events reproducible after process death if you use the saved state module.”
- Android 官方架构建议表里的 **Strongly recommended** 条目：**「Do not send events from the ViewModel to the UI. Process the event immediately in the ViewModel and cause a state update with the result of handling the event.」**
- 失效形态的具体描述（T3，Google 官方工程师个人博客，Android 团队的 Manuel Vivo）：用 `Channel`/`SharedFlow` 暴露一次性事件时，**“a Channel doesn't guarantee the delivery and processing of the events”**；具体事故场景是「UI 消费者进后台停止 collect，ViewModel 生产者此刻 send → 事件丢失 → 支付已成功但 UI 停在旧页」。他把这称为**违反 ACID**：*payment may have succeeded as far as the repository is concerned, but we never moved to the proper next screen*。三条反模式：**①状态可丢失 ②让 UI 去执行动作（多屏尺寸下动作不同，ViewModel 不该决定「怎么做」）③没立即处理事件（fire-and-forget）**。
- Android 官方对「消费事件可能触发状态更新」的闭环处方：`userMessage` 这类瞬态消息建模为**状态字段**（`String?`），UI 显示后**回调 ViewModel 把它清空**（`userMessageShown()` → `copy(userMessage = null)`）——事件被**规约成状态**，消费事实也回写成状态。
- 更老的全局事件总线方案被官方判死：EventBus 官方 wiki 明确说 “Events posted while a Fragment is not running may be dropped”，并已归档 Otto/EventBus 的推荐用法。
- SavedStateHandle（T1）：如果要在**进程被杀**后仍可复现，必须把状态放进 `SavedStateHandle`（`Bundle` 支持的类型受限，非 Parcelable/Serializable 需自定义 `SavedStateProvider`）。这给「事件丢失」的最终防线划了界：**配置变更可由 ViewModel 内存保住；进程死亡只有 SavedState 能救**。

**证据**。
- https://developer.android.com/topic/architecture/ui-layer/events — **T1**（决策树；ViewModel events → 必须落成 UI state；消费触发状态更新）
- https://developer.android.com/topic/architecture/views/ui-layer/events-views — **T1**（同构的 Views 版；`LoginUiState` 模型；`userMessageShown()` 闭环）
- https://developer.android.com/topic/architecture/recommendations — **T1**（“Do not send events from the ViewModel to the UI. **Strongly recommended**”）
- https://manuelvivo.dev/viewmodel-events-antipatterns — **T3**（Channel/SharedFlow 不保证投递；三条 antipattern；ACID 论证；Channel 与 SharedFlow 的对比）
- https://developer.android.com/topic/libraries/architecture/viewmodel/viewmodel-savedstate — **T1**（SavedStateHandle 语义、Bundle 类型表、自定义 SavedStateProvider）
- https://github.com/codepath/android_guides/wiki/Communicating-with-an-Event-Bus — **T3**（社区 wiki，但已被 banner 标注为历史参考；含“Events posted while a Fragment is not running may be dropped”与官方现行建议的转述）

**踩平机制**。
1. **Android 官方：事件 → 状态规约（reduce）**。ViewModel 只暴露可重复读取的状态；一次性副作用以「状态字段 + 消费回调」表达；导航等「怎么做」留给 UI，业务规则「发生了什么」留在 ViewModel。
2. **消费确认闭环**：瞬态消息用可空字段建模，UI 显示后写回确认（清空）——**「消费」本身也变成状态**，于是「已消费」与「未消费」都可被重建后重建。
3. **进程死亡的第二道防线**：SavedStateHandle（受限类型集 + 可自定义 provider）——**只保状态，不保事件**。
4. **反向纪律**：全局事件总线被官方弃用，因为「事件流不保证投递」是流模型的结构性缺陷，不是实现 bug。

**对 udf View Bridge 的启示**。桥当前只有**状态下行**（补丁批/区域），**没有事件通道**——这正好是 Android 官方推荐的方向，等于默认踩平了「一次性事件丢失」这一整类。但要补两条：①若未来加事件通道，必须走「事件规约成状态」或「带确认的状态字段」，**不得引入不保证投递的一次性流**；②**跨进程/跨重启的复现**在桥层面无解（桥在 Rust 进程内），这与 Android「ViewModel 保配置变更、SavedState 保进程死亡」的分层一致——采用方若要跨进程复现，得自己在 store 侧做持久化，而不是指望桥。

---

## 6. 变体新增导致序列化 / 穷尽匹配漂移（我们的 `to_wire` 每变体穷尽 match 有同型风险）

**机制**。变体（enum variant / 消息类型）新增会同时冲击两侧：**编译期穷尽匹配**（Rust `match`、Kotlin `when`、Dart `switch`）与**运行期反序列化**（收到自己不认识的新变体）。前者编译期报错（好），后者静默或响亮地失败（取决于有没有 unknown 兜底）。

**真实失效形态**。
- **Rust 侧同型风险的精确对照**：`to_wire` 里手写 `match view { View::Count(c) => ..., View::Todos(t) => ... }`，新增一个 `View::Error(e)` 时，**编译器会报错**（match 非穷尽）——这是好事，它保证「不漏变体」。但真正的漂移不在编译期，而在**协议兼容期**：UI 侧（可能是旧版本的前端）收到一个它不认识的变体时会怎样。
- Serde 的默认行为：**未知变体 = 反序列化 Err**（`serde::de::Error::unknown_variant`，官方文档原话 “Raised when a `Deserialize` enum type received a variant with an unrecognized name”）。也就是说：**新版本 Rust 侧发的旧版 UI 解析不了 → 整条消息失败**，不是「忽略这个变体」。
- Serde 给的官方兜底是 `#[serde(other)]`：**“Deserialize this variant if the enum tag is anything other than the tag of one of the other variants”**——但**限制很硬**：只允许用在**内部标记（internally tagged）或相邻标记（adjacently tagged）**的枚举的 unit 变体上；外部标记（externally tagged，Serde 默认）的枚举用不了。相关 issue #2010 记录了「外部标记也能编译通过但行为不符」的坑。
- 另一个 Serde 陷阱：**`untagged` 表示没有显式标签，Serde 按定义顺序逐个试，第一个成功的即返回**——变体顺序即优先级，**新增变体可能改变旧数据的匹配结果**（歧义静默改判）。
- Protobuf 官方给出了这一整族问题的**工业级答案**：①字段号**永不复用**，删除字段必须 `reserved`（官方列出复用字段号的四类后果：开发时间浪费、解析/合并错误、**PII 泄漏**、数据损坏）；②新字段对旧解析器**天然可跳过**（TLV：tag = field_number<<3 | wire_type，old parser skip unknown）；③字段名也要 reserve（JSON/TextProto 里字段名是上线的）。
- Elm 官方（本文档证据来自 `elm/json` 的 `Json.Decode` 源码，T1 级官方包源码）：解码是**显式失败**的——`Decode.field` 缺字段即 `Err`，`oneOf` 的多个候选全失败才 `Err`；未知 tag 的处理由采用方的 `andThen` 手工决定（社区通用写法是「未知 tag → 明确 fail 或映射到专门的 Unknown 变体」）。

**证据**。
- https://serde.rs/enum-representations.html — **T1**（externally / internally / adjacently / untagged 四种表示；untagged「first one that deserializes successfully is the one returned」）
- https://serde.rs/variant-attrs.html — **T1**（`#[serde(other)]` 语义与「只允许 internally/adjacently tagged 的 unit 变体」限制；`rename`/`alias`/`skip` 家族）
- https://docs.rs/serde/latest/serde/de/trait.Error.html — **T1**（`unknown_variant` / `unknown_field` 语义）
- https://github.com/serde-rs/serde/issues/2010 — **T2**（`#[serde(other)]` 与 externally tagged 的限制）
- https://protobuf.dev/programming-guides/proto3/ — **T1**（字段号唯一/不可改/必须 reserved；「Consequences of Reusing Field Numbers」四类后果含 PII 泄漏与数据损坏）
- https://protobuf.dev/programming-guides/encoding/ — **T1**（TLV 结构：tag = `field_number << 3 | wire_type`，旧解析器可跳过不认识的字段）
- https://raw.githubusercontent.com/elm/json/master/src/Json/Decode.elm — **T1**（官方解码器源码：缺字段即 Err；`oneOf`/`andThen` 的失败语义）
- https://kotlinlang.org/docs/sealed-classes.html — **T1**（sealed + `when` 穷尽；外部无法新增实现类 = 「closed API」）

**踩平机制**。
1. **Rust `#[non_exhaustive]`**：官方文档原话——「Placing `#[non_exhaustive]` on a struct or enum tells code in other crates not to assume the definition is complete. **This lets a library add new fields or variants without breaking existing code.**」enum 在定义 crate 内仍可穷尽匹配，外部 crate 必须写通配臂。**这是「新增变体不破下游」的语言级答案**。
2. **Serde `#[serde(other)]`**：给内部/相邻标记枚举一个 Unknown 兜底变体，让不认识的新变体**降级而不是炸掉**（代价：Unknown 变体不能携带数据）。
3. **Protobuf**：字段号不复用 + `reserved` + TLV 让旧端跳过未知字段——**「向前兼容靠结构，不靠约定」**。
4. **Elm**：解码显式失败 + 采用方显式写 Unknown 分支——失败是可见的、可枚举的。

**对 udf View Bridge 的启示**。这是本次扫描里**与方案风险最直接对位**的一类：方案的 `to_wire` 是「手写穷尽 match + 全字段覆盖」，§12-7 自评「漏字段 = 静默缺字段」；而 §12-1 又承认「绑定路径词表只加不改」是协议纪律（无机制保障）。外部证据给出三条可直接抄的**机制级**答案：①**给 wire 契约加显式版本位 + Unknown 兜底变体**（借鉴 `#[serde(other)]`），让「旧 UI 收到新变体」降级为「读到一个 Unknown 而不是整条失败」；②**新增绑定/变体必须是「只加不改」且带版本号**（借鉴 Protobuf reserved + 版本位），并让 UI 侧能按版本拒绝或降级；③**Rust 侧用 `#[non_exhaustive]` 保护公开的 View/Actions 词表**——这样外部 crate 新增变体不会破坏下游穷尽匹配（若采用方 crate 与骨架 crate 不同 crate，这条是刚需）。

---

## 7. 视图局部状态与 store 状态双脑（split-brain）

**机制**。同一份事实同时存在于「视图局部状态」与「全局 store」两处，各有各的写入路径与生命周期。两处一旦不同步，UI 显示的与 store 里的不是一回事。

**真实失效形态**。
- **Redux 官方（Priority A 规则，最强档）**：“**Only One Redux Store Per App**”，且 “no app logic will import the store directly. It should be passed to a React component tree via `<Provider>`”——直接 import store 就是双脑的第一步。
- **Redux 官方 Priority A 另一条**：“**Do Not Mutate State**”，理由直指双脑：“Mutating state is the most common cause of bugs in Redux applications, **including components failing to re-render properly**”。配套机制：RTK 的 immutability check 中间件 + Immer（开发期冻结状态，让「谁偷偷改了」当场暴露）。
- **React 官方「Choosing the State Structure」**：**「Avoid duplication in state. When the same data is duplicated between multiple state variables, or within nested objects, it is difficult to keep them in sync. Reduce duplication when you can.」** ——把双脑命名为「duplication in state」，并给了解法方向：能算出来的就别存两份。
- **React 官方 `memo` 文档**的性能原则第 2 条：**「Prefer local state and don't lift state up any further than necessary. For example, don't keep transient state like forms and whether an item is hovered at the top of your tree or in a global state library.」**——**局部状态该局部就局部，全局化是双脑的制造机**。
- **Android 官方**：UI state 必须不可变，“**Never modify the UI state in the UI directly unless the UI itself is the sole source of its data. Violating this principle results in multiple sources of truth for the same piece of information, leading to data inconsistencies and subtle bugs.**” 举的具体例子：`bookmarked` 旗标在 Activity 里被改，就与 data layer 成了同一事实的两个来源。
- **Vue 官方**给出了技术性双脑：**原始对象 vs Proxy**（改原始对象不触发更新）＋ `reactive()` 不能整体替换（换引用丢连接）＋ 不能解构。三条限制合起来就是：**「你以为你改的那份和框架盯着的那份不是同一份」**。
- **Solid 官方**：如果 signal 是在 tracking scope 之外被访问的，更新**不会触发任何订阅者**（官方示例输出 `Count: 0`）——**「读了，但没被跟踪」= 又一种双脑**。

**证据**。
- https://redux.js.org/style-guide — **T1**（Only One Redux Store Per App / Do Not Mutate State / Do Not Put Non-Serializable Values in State or Actions）
- https://react.dev/learn/choosing-the-state-structure — **T1**（Avoid duplication in state）
- https://react.dev/reference/react/memo — **T1**（Prefer local state, don't lift state up）
- https://developer.android.com/topic/architecture/ui-layer — **T1**（不可变 + 「Never modify the UI state in the UI directly」＋ multiple sources of truth 原文）
- https://vuejs.org/guide/essentials/reactivity-fundamentals.html — **T1**（Proxy vs 原始对象；`reactive()` 三条限制）
- https://docs.solidjs.com/concepts/intro-to-reactivity — **T1**（tracking scope 之外访问 → 不触发；组件函数只运行一次）

**踩平机制**。
1. **单 store + 唯一写入口**（Redux Priority A）：store 由 Provider 注入，不被业务代码直接 import；所有写经 dispatch/reducer。**读侧可以很多，写侧只能一个**。
2. **不可变 + 开发期冻结**（Redux/Immer）：状态不可变 = 「谁改的」只有一个答案；开发期 immutability check 把偷改当场变成报错。
3. **局部状态不被上提**（React memo 原则）：临时 UI 态（表单、hover）留在组件内，不进全局 store——**双脑的最佳预防是不制造第二份**。
4. **单一 source of truth + 派生而非复制**（Android/Redux）：重复的数据一律改为「算出来的」，而不是「存两遍的」。
5. **载体唯一性**（Vue/Solid）：要么代理、要么 signal，**只有一份被框架盯着**。

**对 udf View Bridge 的启示**。桥的核心承诺就是「唯一事实源 = store」，出站只是投影快照——这在结构上消灭了 store 侧双脑。但有两个具体的双脑入口要堵：①**UI 端自己缓存了视图值又不停更新**——桥必须提供「版本号/水位」让 UI 判断自己手上的是不是当前版本（方案的 `RegionPub::version` 已有；JSON 补丁通道目前**没有版本号**，是缺口）；②**桥内 last 槽与 UI 手上快照分叉**——方案的 `snapshot_doc()` 重同步原语正是为此设计的（对应 React 的 `key` 重置 / Android 的 `SavedStateHandle` 恢复），但**重同步必须是显式可检测的**（方案的 `resync` 位做到了）。

---

## 8. 跨语言 / FFI 边界坑：内存生命周期（悬垂）、对齐 / 字节序、序列化往返、ABI 稳定性

**机制**。跨语言边界的四种失效各有各的机理：**生命周期**（谁负责释放、对象活多久）、**布局**（对齐、填充、字节序）、**往返**（序列化→反序列化→再序列化是否等值）、**ABI**（调用约定与类型布局是否稳定）。

**真实失效形态（逐项）**。
- **生命周期 / 悬垂**：Rust Nomicon 直言 —— 外部库「isn't valid for all possible inputs since the pointer could be **dangling**, and raw pointers fall outside of Rust's safe memory model」；异步回调场景更狠：「**it is also absolutely necessary that no more callbacks are performed by the C library after the respective Rust object gets destroyed.** This can be achieved by unregistering the callback in the object's destructor」。社区里 rust-unofficial/patterns 的 issue #313 就是官方文档示例里**真的写了悬垂指针**（`SAFETY: whoops, this contains a dangling pointer!`）。真实事故形态：C 侧返回 NULL 时不检查就解引用 → SIGSEGV（rust-lang/rust#65546 就是这类：第二次调用返回 NULL 导致段错误）。Rust `CStr` 官方文档的写法很有教育意义：非法指针的一切行为 “💀 undefined”。
- **对齐 / 字节序**：Rust Reference 明确：默认 `Rust` 表示的布局**只有健全性所需的最小保证**（“The only data layout guarantees made by this representation are those required for soundness”），字段偏移、size、alignment 都可能随编译变化；**只有 `#[repr(C)]` 才与 C 布局兼容**，且 `repr(C)` 的对齐算法（按字段对齐加填充、末尾向上取整到结构体对齐）是**规范化的**。字节序：Rust **没有**跨平台字节序保证的裸内存布局语义——要跨语言传二进制，必须显式规定字节序（Protobuf 的做法：varint 用小端 7-bit 分组、`double` 用 IEEE754、`fixed32/64` 用固定 wire type；Dart FFI 文档则要求用 `AbiSpecificInteger` 显式声明平台相关整数）。
- **ABI 稳定性**：Rust Reference 的 ABI 段写得毫不含糊：「`unsafe extern "Rust"` — The native calling convention for Rust functions and closures … **The Rust ABI offers no stability guarantees.**」；`extern "C"` 才是匹配平台 C ABI 的那个。也就是说：**Rust 默认 ABI 跨版本不保证**——这是「框架升级导致 ABI 断裂」的第一手规范依据。
- **序列化往返**：往返不等值的经典来源是**字段名大小写约定不统一**——Tauri 官方文档示例中 Rust 结构体用 `#[serde(rename_all = "camelCase")]` 才能对上 TS 侧的 `downloadId`；StackOverflow 上「Why does Tauri modify the parameter names of invoked functions?」记录了实情：**「So the outer name will be camelCase, but any nested fields will likely be snake_case (matching the Rust side) unless you configure serde」**——即**嵌套字段与顶层字段的命名约定可能不一致**，前端拿 `undefined`。这类「往返看着对、字段名悄悄不同」的坑，是纯手写 JSON 映射的高发区。
- **跨语言边界的正解样本（Flutter）**：官方保证 “This codec is guaranteed to be compatible with the corresponding `StandardMethodCodec` on the Dart side. **These parts of the Flutter SDK are evolved synchronously.**”——**两侧同步演进 + 明确保证**，这是「跨语言边界不腐化」的制度化做法。

**证据**。
- https://doc.rust-lang.org/nomicon/ffi.html — **T1**（dangling pointer；raw pointers 逃出安全模型；异步回调与析构顺序；`repr(C)` 与 `repr(C, packed)`；C 字符串不自带 `\0`；nullable pointer optimization；FFI 与 unwinding）
- https://doc.rust-lang.org/reference/items/external-blocks.html — **T1**（ABI 列表；**「The Rust ABI offers no stability guarantees」**；`extern "C"`/`"system"`/`"C-unwind"` 语义）
- https://doc.rust-lang.org/reference/type-layout.html — **T1**（默认 `Rust` 表示的最小保证；`#[repr(C)]` 的 size/offset/alignment 规范化算法；`#[repr(C, packed)]`/`align`）
- https://doc.rust-lang.org/stable/std/attribute.non_exhaustive.html — **T1**
- https://github.com/rust-unofficial/patterns/issues/313 — **T2**（官方文档示例中的悬垂指针）
- https://github.com/rust-lang/rust/issues/65546 — **T2**（C 侧返回 NULL 未检查 → SIGSEGV）
- https://cxx.rs/ — **T2**（cxx 6.85k★/Apache-2.0/活跃；「CXX uses static analysis of the types and function signatures to protect both Rust's and C++'s invariants … The resulting FFI bridge operates at zero or negligible overhead, i.e. no copying, no serialization, no memory allocation, no runtime checks needed」）
- https://docs.rs/bytemuck/latest/bytemuck/ — **T1**（对齐失败的具体形态：「`cast_ref::<[u8; 4], u32>` might fail if the specific array reference given at runtime doesn't have alignment 4」；`try_`/`must_cast` 两档处置）
- https://dart.dev/interop/c-interop — **T1**（Dart FFI 的 `AbiSpecificInteger` 家族：平台相关整数必须显式声明）
- https://protobuf.dev/programming-guides/encoding/ — **T1**（varint 小端 7-bit 分组、IEEE754 double、TLV）
- https://v2.tauri.app/develop/calling-frontend/ — **T2**（Tauri 112k★/Apache-2.0/活跃；`#[serde(rename_all = "camelCase")]` 事件负载；Common Pitfalls 段列出 `unlisten()` 时序、setup 时机、事件乱序）
- https://api.flutter.dev/javadoc/io/flutter/plugin/common/StandardMethodCodec.html — **T1**（两侧同步演进的兼容保证）

**踩平机制**。
1. **Rust：默认 ABI 无稳定性保证 → 显式 `extern "C"` + `#[repr(C)]`**；布局敏感处用 `repr(C)` 拿到规范化布局；裸内存传输必须自定字节序。
2. **bytemuck 的两档处置**：`try_cast`（运行期 Err，检查 alignment）vs `must_cast`（编译期能验的静态校验，否则编译错）——**把「对齐不匹配」从 UB 变成一个可处理的错误**。
3. **cxx 的路线**：**用静态分析在编译期证明两侧不变量**（签名/类型），并把边界缩到「两种语言都语义相似的公共子集」；生成代码两侧同时产出 + 编译期断言（static assertions）；结果是「零拷贝、零序列化、零运行期检查」的桥。**核心不是「小心写 unsafe」，是「让 unsafe 不必存在」**。
4. **Flutter：两侧同步演进的 codec + 明文兼容保证**（制度而非技巧）。
5. **Tauri：把跨语言序列化集中在 serde 约定上**（`rename_all`），并把边界陷阱（时序/时机/顺序）写进官方 Common Pitfalls。

**对 udf View Bridge 的启示**。桥的跨语言面是 **JSON/字节 + 版本**，不走裸内存 FFI，所以悬垂/对齐/字节序三类坑天然不适用——**这是选型上的正确，值得在文档里写明「为什么不用 FFI 直传」**。真正适用的是两条：①**往返一致性**：Rust 侧字段名与 UI 侧字段名的命名约定必须钉死（Tauri 的 camelCase 教训表明「顶层与嵌套可能不一致」是真实坑），桥应**固定 wire 字段命名约定并写进协议**（方案目前 wire 全由采用方手写 `to_wire`，约定无人管）；②**ABI/协议稳定性**：既然选了 JSON 这种「文本协议」，就用 Protobuf 式纪律（只加不改 + 版本位 + 保留字段）保护它——这直接对应第 6 类的结论。

---

## 9. 响应式重渲染性能陷阱（过度渲染、tearing、effect 循环）与细粒度响应式取舍

**机制**。粗粒度响应式（组件级重渲染）会把「一个值变了」放大成「整棵子树重渲染」；并发渲染下，外部可变状态可在渲染中途被改 → **tearing**（同一屏显示两个版本）；effect 若在追踪域内改自己的依赖 → **递归循环**。

**真实失效形态**。
- **过度渲染**：React 官方 `memo` 文档：**「memoization is completely useless if the props passed to the component are *always different*」**，并给出量级判断（「If your app is like this site … memoization is usually unnecessary」）；根因是 `Object.is` 浅比较 + 每次渲染新建对象/函数。官方还给出诊断路径：**React Developer Tools profiler**。
- **Effect 链**：React 官方 `memo` 文档的性能原则第 4 条：**「Avoid unnecessary Effects that update state. Most performance problems in React apps are caused by chains of updates originating from Effects that cause your components to render over and over.」**——**effect 更新 state 是重渲染循环的头号来源**。
- **Tearing（并发渲染下的一致性破坏）**：React 官方工作组的说法很硬——外部可变 store “can be updated **in the middle of rendering**, without React knowing about it. This problem is known as **"tearing"** and in certain situations can result in **inconsistent UIs or even errors**.” 官方给出**三级支持阶梯**：Level 1「允许 UI 暂时 tear」（最坏可见不一致闪烁）→ Level 2「不 tear，但偶尔渲染更久」（`useSyncExternalStore`：检测到渲染期间 store 变了就**重头再渲染**，用户永远看到一致 UI）→ Level 3「既一致又不降级」。工作组的 `useSyncExternalStore` 文档要求更具体：**`getSnapshot` 在 store 未变时必须返回同一个值**，“If the store changes and the returned value is different (as compared by `Object.is`), React will re-renders the component”；快照必须不可变（“If the underlying store has mutable data, return a new immutable snapshot … Otherwise, **return a cached last snapshot**”）。
- **Effect 循环**：Vue 官方 issue #9574 是典型形态：**「Maximum recursive updates exceeded in component … This means you have a reactive effect that is mutating its own dependencies and thus recursively triggering itself.」** 根因是 errorHandler 里读的响应式变量被 render effect 一并追踪，写它又触发 render 效果。Vue 官方 `watchEffect` 的坑同族（vuejs/core#5359：vuex mutation 里的 ref 被 watchEffect 顺带追踪 → store 一变就触发）。
- **细粒度响应式的取舍**：Solid 官方文档把取舍讲得最清楚——**「In Solid, updates are made to the targeted attribute that needs to be changed … In contrast, React would re-execute an entire component for a change in the single attribute」**；代价是 **「Components … will only run once」**（组件函数不再每次都跑，追踪作用域变成显式的），以及同步跟踪的语义约束（async 里读到 signal 不会建立订阅）。
- 另一个折中（Svelte 官方）：编译期做依赖分析，代价是需要构建步骤且「altering JavaScript semantics is essentially creating a language that looks like JavaScript but compiles into something else」（Vue 官方文档对 Svelte 路线的一句评价，Vue 自己探索过同路线并**放弃**了——见第 10 类）。

**证据**。
- https://react.dev/reference/react/memo — **T1**（过度渲染诊断；`Object.is`；effect 链；profiler）
- https://react.dev/learn/you-might-not-need-an-effect — **T1**（Effect 造成 cascading updates）
- https://github.com/reactwg/react-18/discussions/70 — **T2**（tearing 定义与三级支持阶梯；库维护者视角）
- https://react.dev/reference/react/useSyncExternalStore — **T1**（快照不可变 + 缓存上次快照；渲染期间 store 变化的处理；Transition 下的重渲染）
- https://react.dev/blog/2022/03/08/react-18-upgrade-guide — **T1**（库维护者需迁移到 `useSyncExternalStore`）
- https://github.com/vuejs/core/issues/9574 — **T2**（Maximum recursive updates exceeded；effect 改自己依赖）
- https://github.com/vuejs/core/issues/5359 — **T2**（watchEffect 追踪到 store 内部 ref → 额外依赖 → 重渲染循环）
- https://vuejs.org/guide/extras/reactivity-in-depth.html — **T1**（track/trigger；`onRenderTracked`/`onRenderTriggered` 调试钩子；运行时响应式 vs 编译期响应式的取舍）
- https://docs.solidjs.com/advanced-concepts/fine-grained-reactivity — **T1**（细粒度 vs 整组件重执行；组件只跑一次；memo 依赖集随分支变化；同步跟踪）
- https://docs.solidjs.com/concepts/intro-to-reactivity — **T1**（同步 vs 异步响应式；tracking scope 外访问不触发）

**踩平机制**。
1. **React：`useSyncExternalStore` 把「外部可变 store」接进并发渲染的一致性协议**——快照不可变 + 未变时返回同一引用 + 渲染中检测到变化就重跑。**注意**：官方明确说这是给「外部 store」的方案，且**推荐优先用 React 自己的 state**。
2. **Solid：细粒度到属性级**——「updates are made to the targeted attribute that needs to be changed」；代价是组件函数只跑一次、依赖必须被追踪到。
3. **Vue：批处理 + 可观测调试钩子**——effect 触发被排队并去重；`onRenderTracked`/`onRenderTriggered` 让「谁在追踪/谁触发」可见。
4. **共同的反模式共识（三家一致）**：**不要用「更新状态的 effect」来维护派生状态**（React 明确点名；Vue 的递归更新报错同族）。派生必须在同一次计算里算出来。

**对 udf View Bridge 的启示**。桥的出站是「按绑定收敛的补丁」，天然是细粒度的（React 那种整树重渲在桥这一侧不存在）——**但「tearing」的原语在跨线程 pull 侧真实存在**：UI 可能拿到一批来自不同拍的补丁拼出的不一致快照。三条可直接抄的机制：①**出站必须带版本/水位**（方案 `RegionPub::version` 已有；JSON 补丁通道缺——应补），让 UI 能判断「我手上这批是否自洽」，不自洽就整批重取 `snapshot_doc()`；②**一批补丁必须是单一拍的**（方案的 latest-wins 折叠天然保证：每路径取现值，同批同拍语义）；③**投影必须纯且确定**（同 state 恒同输出），否则「自洽」无从谈起——方案已有此纪律，外部证据（React 的 `getSnapshot` 必须返回缓存快照、Solid 的同步跟踪）同向。

---

## 10. 投影 / 视图层重构成本（框架升级迫使采用方大改 —— Druid Data bound 被作者否定的同类）

**机制**。框架把「实现细节」绑进「采用方必须实现的 trait / 必须满足的 bound / 必须遵循的编码约定」时，采用方代码就与框架版本耦合；框架一换代，采用方全量返工。破坏力 = 「绑定点的数量 × 每个绑定点的侵入深度」。

**真实失效形态**。
- **Druid 的 Data bound（作者亲自否定）**：Druid 要求应用状态实现 `trait Data: Clone + 'static { fn same(&self, other: &Self) -> bool; }`，并由 `#[derive(Data)]` 生成实现。**Druid 作者 Raph Levien 在 Xilem 架构博客里亲自列它为缺陷**：“The app data must have a `Data` bound, which implies **cloning and equality testing. Interior mutability is effectively forbidden.**”——注意这两句的破坏力：①采用方核心类型被迫 Clone + 全量相等比较；②**内部可变性被事实禁止**（很多真实应用状态天然需要）。
- 同一篇博客里，作者对 **lens 机制**的判词是：“The "lens" mechanism is confusing and it is not easy to implement complex binding patterns.”；对 **environment 机制**：“it is not efficient and doesn't support fine-grained change propagation.”；并直言 **“my previous attempts (including the current Druid architecture) have all been flawed”**。
- **框架真的死了**：Druid README 现在写着 **“UNMAINTAINED / The Druid project has been discontinued.”**，建议新项目别用（“we don't recommend using Druid for brand new applications”），并推荐继任者 Xilem。**采用方若押了 Data bound + Lens，就是押了一个已停止维护的框架。**
- **继任者的做法（对照组）**：Xilem 当前示例里，应用状态是**没有任何派生的 plain struct**（`struct Calculator { current_num_index: usize, clear_current_entry_on_input: bool, numbers: [String; 2], result: Option<String>, operation: Option<MathOperator> }`），视图函数签名是 `fn app_logic(data: &mut Calculator) -> impl WidgetView<Calculator>`——**采用方写普通函数，框架不要求任何 trait impl**。注意 `Calculator` 里的 `Option<MathOperator>` + `result: Option<String>`：**状态用密封 enum/Option 表达互斥，不是一堆布尔旗子**。
- **同类：语言级强制迁移逼采用方全量返工**——Dart null safety 迁移：官方迁移指南要求「按依赖图从叶子往上迁移」，每包要「Set the package version to indicate a breaking change. Update the SDK constraints and package dependencies」，并提供 `dart migrate` 工具批量改。这是「语言/框架级约束变更 → 全生态强制重写」的标准形态。
- **同类：框架新增能力时被迫改库**——React 18 升级指南对**库维护者**的要求：“some libraries may need to switch to one of the following APIs”（`useSyncExternalStore` / `useInsertionEffect`）；采用方升级框架时，依赖库若未跟上就出问题。
- **同类：框架在「开发者体验改进」上的自我否决留下了采用方负担**——Vue 的 Reactivity Transform（`$ref`/`$()` 宏）曾以实验特性发布，**最终被官方放弃**（RFC 标题即 “[⚠️ Dropped]”）；官方给出的放弃理由第一条是：“Losing `.value` makes it harder to tell what is being tracked and which line is triggering a reactive effect. This problem is not so noticeable inside small SFCs, but the mental overhead becomes much more noticeable in large codebases.” ——**这是「语法糖的隐式性在大代码库里变成追踪负担」的一手证据**。社区还指出放弃是「consensus from the team」，并有专门的工具帮用户把已用上的语法改回去。

**证据**。
- https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html — **T2**（Druid 作者本人：Data bound 迫使 clone + equality testing、内部可变性被禁；lens 令人困惑；environment 无细粒度传播；“my previous attempts … have all been flawed”）
- https://github.com/linebender/druid — **T2**（README：**UNMAINTAINED / discontinued**；Data trait 的 `same()` 要求；Lens 派生；Druid 9.7k★/Apache-2.0，最后 push 2026-04）
- https://github.com/linebender/xilem/blob/main/xilem/examples/calc.rs — **T2**（Xilem 5.6k★/Apache-2.0，push 2026-09；应用状态是 plain struct 无派生；`Option<MathOperator>` 表达互斥；视图函数 = 普通函数）
- https://docs.rs/druid/latest/druid/trait.Data.html — **T1**（Data trait 的 `same()` 契约）
- https://dart.dev/null-safety/migration-guide — **T1**（迁移顺序、破坏性版本号、工具化迁移）
- https://react.dev/blog/2022/03/08/react-18-upgrade-guide — **T1**（库维护者需改 API）
- https://github.com/vuejs/rfcs/discussions/369 — **T2**（Reactivity Transform 被 Dropped；官方放弃理由原文；社区的“consensus from the team”与配套迁移工具）

**踩平机制**。
1. **Xilem：把「实现 trait」降级为「写普通函数」**——`fn app_logic(data: &mut State) -> impl WidgetView<State>`，采用方核心类型零 bound、零派生。**框架升级时，采用方的类型定义与业务函数不用动。**
2. **Redux：bound 落在「框架自己的词表」上**（`Action`/`State` 由框架定义），采用方的 reducer 只是一个纯函数签名——**框架 trait 的数量被压到最少**。
3. **Elm：编译器做穷尽性守卫**（`update` 的 `case` 必须覆盖所有 `Msg`，新增 Msg 编译不过）——**升级造成的改动是「编译期定位的局部改动」，不是运行时腐化**。
4. **Rust：`#[non_exhaustive]`** 给公开词表留出「新变体不破下游」的兼容出口。
5. **反向教训（Vue）**：**任何让「追踪/依赖变得不可见」的语法糖，在大代码库里会变成升级负债**——所以 Vue 宁可保留 `.value` 的显式性。

**对 udf View Bridge 的启示**。这条最关键，因为方案里已经有一个**同型风险**：`ViewMap<S>` / `IntentMap<A>` 是**采用方必须实现的 trait**，且 `ViewMap` 的关联类型 `View` 与 `bindings()` / `project()` / `to_wire()` 构成了一整套必须实现的义务面。Druid 的教训给出三条硬约束：①**bound 绝不落在采用方的领域类型 `S` 上**（方案已做到：`S` 零新增 bound，只有 `View` 承担 `Clone + PartialEq + Send + 'static`——这点做对了，应在 ADR 里显式记为「Druid 教训的正面回应」）；②**trait 的方法数量越少越好**（当前 `ViewMap` 有 5 个方法 + 1 个关联类型 + 2 个关联类型义务，是「中等侵入」——未来任何新增义务都会抬高采用方的升级成本，**每次加方法都要问：能不能用默认实现或独立的可选 trait 承载**）；③**框架换代时，采用方要改的东西应该只有「词表」而非「状态类型」**（方案已满足：采用方状态是 plain struct）。此外，Vue 的教训提醒：**桥的隐式性必须可内省**——方案的 `projection_runs()` / `projection_panics()` 计数正是这个方向的机制（对齐 `subscriber_panics` 先例），应继续加强而非削弱。

---

## ① 踩平机制清单（≤8 条，每条注明来源项目 + 它踩平了哪类坑）

| # | 踩平机制 | 来源项目 | 踩平了哪类坑 | 一手出处 |
|---|---|---|---|---|
| M1 | **事件规约成状态**：一次性副作用必须落成「可重复读取的状态 + 消费确认回写」，不用不保证投递的事件流 | Android Architecture（ViewModel + SavedStateHandle） | 第 5 类（事件丢失/配置变更丢失） | https://developer.android.com/topic/architecture/ui-layer/events （T1） |
| M2 | **密封状态 + 编译期穷尽**：状态用 enum/sealed class 表达互斥，非法组合在类型层不可表示 | Kotlin sealed class、Bloc sealed state、Elm custom type、Rust enum | 第 3 类（布尔旗子 soup）、第 6 类（穷尽漂移） | https://kotlinlang.org/docs/sealed-classes.html （T1）、https://bloclibrary.dev/modeling-state （T1） |
| M3 | **派生在同一次计算里算，不入状态**：缓存判据用「输入值等值」，派生量永不同步 | Redux/Reselect、React 渲染期计算、Vue computed、Solid memo | 第 4 类（派生陈旧/记忆化陷阱）、第 2 类（陈旧闭包） | https://redux.js.org/reselect/usage/best-practices （T1）、https://react.dev/learn/you-might-not-need-an-effect （T1） |
| M4 | **唯一写入口 + 单一事实源 + 不可变**：一个 store、一个写路径、状态不可变、局部态不上提 | Redux（Priority A 规则）、Android 不可变 UiState、React（Avoid duplication in state） | 第 7 类（双脑 split-brain） | https://redux.js.org/style-guide （T1）、https://developer.android.com/topic/architecture/ui-layer （T1） |
| M5 | **帧/拍级一致的水位与版本**：出站带版本号，外部状态在渲染中途变化被检测并重跑，绝不撕裂 | React `useSyncExternalStore`（三级支持阶梯） | 第 9 类（tearing、过度渲染、effect 循环） | https://github.com/reactwg/react-18/discussions/70 （T2）、https://react.dev/reference/react/useSyncExternalStore （T1） |
| M6 | **协议只加不改 + 版本位 + Unknown 兜底 + 保留字段**：让「新增变体」不炸旧端 | Protobuf（reserved + TLV）、Serde `#[serde(other)]`、Rust `#[non_exhaustive]` | 第 6 类（变体新增/序列化漂移）、第 8 类（ABI 稳定性） | https://protobuf.dev/programming-guides/proto3/ （T1）、https://serde.rs/variant-attrs.html （T1） |
| M7 | **bound 只落在框架自己的词表上，采用方写普通函数**：框架 trait 数量与侵入深度最小化，框架换代不波及采用方状态类型 | Xilem（plain struct + 普通函数）、Redux（纯函数 reducer）、Elm（编译器穷尽守卫） | 第 10 类（重构成本 / 框架升级迫使大改）、第 1 类（God ViewModel 边界） | https://github.com/linebender/xilem/blob/main/xilem/examples/calc.rs （T2）、https://raphlinus.github.io/rust/gui/2022/05/07/ui-architecture.html （T2） |
| M8 | **把跨语言边界的危险面收窄到「两种语言都语义相似的公共子集」，用静态分析在编译期证明不变量** | cxx（Rust↔C++）、Flutter StandardMethodCodec（两侧同步演进）、bytemuck（对齐两档处置） | 第 8 类（悬垂/对齐/字节序/往返/ABI） | https://cxx.rs/ （T2）、https://api.flutter.dev/javadoc/io/flutter/plugin/common/StandardMethodCodec.html （T1） |

---

## ② udf 可直接抄的 3–5 条

1. **【抄 M1】意图通道只许「状态化」，不许「一次性事件流」。** 外部证据把「事件流不保证投递」定性为结构性缺陷（Channel/SharedFlow 在消费者暂停时丢事件，官方判为违反 ACID）。udf 侧动作：View Bridge 的上行面**只保留「意图 → Action」**（`send_intent`），**不提供一次性事件下行通道**；若将来要加「提示/导航」这类副作用，**必须建模成状态字段 + UI 消费后回写确认**（对齐 Android `userMessage`/`userMessageShown()` 闭环），且不承诺进程死亡后的复现（那属于 store 侧持久化，不属于桥）。

2. **【抄 M6】给 wire 协议补「版本位 + Unknown 兜底 + 只加不改」的机制化保障。** 这是本次扫描中与方案自评风险（§12-7 `to_wire` 漏字段、§12-1 路径词表只加不改无机制）**最直接对位**的一条。具体三条：(a) `Patch`/`RegionPub` 出站带协议版本号，旧 UI 见到高版本可选择降级或整批重取；(b) 采用方的 View enum **提供 `Unknown` 兜底变体**（借鉴 `#[serde(other)]`，注意其「只能带 unit」的代价），使「新变体到达旧 UI」降级为「读到一个 Unknown」而非整条失败；(c) 公开的 `View`/`Actions` 词表若跨 crate 暴露，加 `#[non_exhaustive]`。

3. **【抄 M5】JSON 补丁通道补版本/水位，消灭 tearing 原语。** 桥的 `RegionPub::version` 已有版本号，但**JSON 补丁通道没有**——UI 无法判断手上的一批补丁是否自洽。动作：给 `PatchBatch` 加 `version`（或单调代数），UI 见到版本跳跃即整批 `snapshot_doc()` 重同步；并保持「一批补丁同拍」的语义（现方案的 latest-wins 折叠已天然保证，别在二期改成跨拍合并）。同时把「投影必须纯且确定（同 state 恒同输出）」升格为**可测断言**（同 state 投影两次必等值），这是 React `getSnapshot` 必须返回缓存快照的同构要求。

4. **【抄 M7 + M2】ViewMap 的义务面只许增不许增，并给「状态表达」立一条类型纪律。** 外部证据（Druid 被作者否定、Xilem 全 plain struct、Vue 放弃隐式语法糖）指向同一条：框架 trait 的侵入深度就是升级成本。动作：(a) `ViewMap` 的方法集**冻结**（新增义务必须走独立可选 trait 或带默认实现的新方法）；(b) `bound` 继续只落在 `View` 上、**绝不落在采用方的 `S` 上**（写进 ADR 的「不重演 Druid Data bound」条款）；(c) 立一条纪律：**采用方的视图词表用 enum（密封状态）表达互斥状态，禁止布尔旗子 soup**——这既是 M2 的直接应用，也把「非法状态」挡在类型层。

5. **【抄 M4 + M8】wire 字段命名约定钉死在协议里，并把「跨语言往返一致性」做成可自动验证的检查。** Tauri 的教训表明「顶层 camelCase、嵌套 snake_case」这类不一致会静默产出 `undefined`。动作：协议文档写死字段命名约定（建议全 camelCase 或全 snake_case，二选一，写进 ADR）；并在验收里加一条**往返断言**（Rust 侧构造 → `to_wire` → JSON → UI 侧按约定读回 → 逐字段核对），与方案已有的「每变体 round-trip 断言」合并成一条更强的**双向**断言（现方案只验 Rust 侧 View→wire 的完整性，未验 wire→UI 消费的命名一致性）。

---

## ③ 坑清单中我们方案已有的覆盖点 vs 尚未覆盖点

> 覆盖点判定基于 `docs/research/mvi-view-bridge-research.md`（v5 封版候选）与 `reports/2026-10-27-mvi-view-bridge-plan-review.md`（复审轮 1 CONDITIONAL）。

### 已覆盖（外部证据支持，可直接引用为「业界共识」而非「自定纪律」）

| 坑类 | 方案覆盖点 | 外部背书 |
|---|---|---|
| 1 God ViewModel | `ViewMap` 是**纯投影**（`project` 禁 IO/时钟/随机/写共享态），业务规则不进视图层 | Android「business logic never in the UI layer」（T1）、Redux「logic in reducers」（T1） |
| 2 陈旧状态 | 桥是**单向出站**（只替换不双向写）；投影确定性（同 state 恒同输出） | React 单向数据流 + 快照语义（T1）、Vue 单一代理载体（T1） |
| 3 布尔 soup | `View` = 采用方 **enum 词表**（一绑定一变体）；`View: Clone + PartialEq` 锁值语义 | Kotlin sealed + 穷尽 when（T1）、Bloc 官方「Not Type Safe / Bloated」反面评估（T1） |
| 4 派生陈旧 | **L1 输入键等值**（`In: PartialEq`）拍板，否决依赖追踪；「input 覆盖实读」红线；`Derived`/`Sentinel` 同型机制胞 | Reselect 官方「input selector 只取值不算」「result function 不返回 input」（T1）、Vue `Date.now()` 反例（T1）、React「渲染中算，不入 state」（T1） |
| 5 事件 vs 状态 | 桥**只有状态下行**，无一次性事件通道；`resync` 位把完整性事实随数据交付 | Android「ViewModel events must result in a UI state update」+「Do not send events from the ViewModel to the UI」（T1） |
| 7 双脑 | 唯一事实源 = store；出站为投影快照；`snapshot_doc()` 为重同步原语 | Redux「Only One Store」+「Do Not Mutate State」（T1）、Android「Never modify UI state in the UI」（T1） |
| 9 tearing/过度渲染 | **按绑定收敛的 diff**（非整视图重渲）；latest-wins 折叠使一批补丁同拍；投影在锁外纯函数 | React「external store 必须返回缓存快照、渲染中变化需检测」（T1）、Solid 细粒度（T1） |
| 8 ABI 稳定性（部分） | 出站面是 JSON/字节 + **版本号**（`RegionPub::version`），不裸传内存，悬垂/对齐/字节序三类坑结构性不适用 | Rust Reference「Rust ABI offers no stability guarantees」（T1）、`repr(C)` 布局规范（T1）、Flutter codec 同步演进保证（T1） |

### 尚未覆盖 / 覆盖不足（按「外部证据支持 + 缺口明确」排序）

| # | 缺口 | 证据支撑（为什么这是真缺口而非吹毛求疵） | 建议动作 |
|---|---|---|---|
| **G1** | **JSON 补丁通道无版本/水位** —— tearing 原语仍在（UI 无法判断一批补丁是否自洽） | React 官方：外部 store 在渲染中途变化会 tearing，`useSyncExternalStore` 的解法核心就是「快照不可变 + 缓存上次快照 + 检测变化重跑」（T1）；`RegionPub` 有 `version` 而 `PatchBatch` 没有，是**通道间的不对称** | `PatchBatch` 加 `version`（或代数），UI 见跳跃即 `snapshot_doc()` 重同步；两通道版本语义对齐 |
| **G2** | **wire 协议无「新增变体」的兼容机制** —— 只有文档纪律「只加不改」，无机制保障 | Protobuf 官方把「不复用字段号 + reserved」列为硬规则并列出复用后果（含 PII 泄漏/数据损坏）（T1）；Serde `#[serde(other)]` 提供 Unknown 兜底（T1）；Rust `#[non_exhaustive]` 提供「新变体不破下游」的官方出口（T1） | 协议版本位 + 采用方 View enum 的 `Unknown` 兜底变体 + 公开词表 `#[non_exhaustive]`；把「只加不改」从文档纪律升为机制 |
| **G3** | **`to_wire` 的 round-trip 断言只覆盖 Rust 侧 View→wire，未覆盖「wire 字段名 → UI 消费」的命名一致性** | Tauri 官方示例要求显式 `#[serde(rename_all = "camelCase")]`；社区实证「顶层 camelCase、嵌套可能 snake_case」→ 前端 `undefined`（T2/T3）。当前方案 wire 全由采用方手写，**命名约定无人管** | 协议文档钉死字段命名约定（二选一，写进 ADR）；验收加**双向**往返断言（Rust→wire→JSON→按约定读回→逐字段核对） |
| **G4** | **缺少「状态用 enum 而非布尔旗子」的显式纪律** | Bloc 官方把「具体类 + 状态枚举」方案的病写为「Not Type Safe: It's possible to `emit` a malformed state」「Bloated」（T1）；React 官方用 `isSending`/`isSent` 演示「impossible states」（T1）。方案的 `View` 已是 enum 形态，但**未把「禁布尔旗子」写成采用方纪律** | 在 `ViewMap` 的 doc 注释立纪律：视图词表用 enum 表达互斥状态；`bindings()` 语义文档给正反例 |
| **G5** | **「派生 input 覆盖 compute 实读」红线仍是纯文档纪律，无机制保障** | Vue `computed(() => Date.now())` 静默冻结（T1）证明「依赖窄于实读」在框架层看起来完全正常；方案的 `Sentinel`/`Derived` 均以 `In: PartialEq` 判定，若采用方声明的 `In` 窄于 `project` 实读，**漏报 = 静默陈旧**——这是方案自评的第一静默面 | 保持红线，但补一条可自动验证的最小形态：**验收 demo 里放一个「input 覆盖实读」的反例用例**（故意让 input 少声明一项，断言 sentinel 误报/漏报的行为被文档化）；或让 `Sentinel` 支持「粗闸 + 细闸」双层（细闸 = 类型 eq 兜底陈旧） |
| **G6** | **`ViewMap` trait 义务面（5 方法 + 2 关联类型义务）的侵入深度未设上限** | Druid 作者亲自否定 Data bound（迫使 clone + equality testing、禁内部可变性，T2）；Druid 项目已 UNMAINTAINED（T2）；Xilem 继任方案采用 plain struct + 普通函数（T2）；Vue 因语法糖隐式性在大代码库变负担而放弃特性（T2） | 在 ADR 的「后果」段写明「`ViewMap` 方法集冻结」条款；新增义务必须走独立可选 trait 或带默认实现的方法；明确「bound 只落在 `View`，绝不落在 `S`」 |
| **G7** | **桥的隐式性缺乏内省面**（采用了方 debug「为什么这拍没补丁 / 为什么这拍重投影了」的抓手） | Vue 提供 `onRenderTracked`/`onRenderTriggered` 调试钩子（T1）；React 官方推荐 React DevTools profiler 定位过度渲染（T1）。方案已有 `projection_runs()`/`projection_panics()`（计数），但**只到全局总量**，无法定位「哪个绑定」 | 与方案既有纪律一致（读侧计数，累计只增，不进热路径）；二期 `view_bridge_multidim` 采集器定位到绑定级（方案已列为二期）——本次不新增面，仅标注为已知缺口 |
| **G8** | **跨进程/跨重启的视图复现在桥层无解**（已知取舍，未在文档化为「边界」） | Android 的分层：ViewModel 保配置变更、SavedStateHandle 保进程死亡（T1）。桥在 Rust 进程内，进程被杀后 UI 侧无从复现 | 在协议文档写明边界：**桥保证「同一进程内」的自洽与可重同步；跨进程复现由 store/持久化层负责**，避免采用方误以为桥能兜 |

---

## 附：本次调研的证据自评

- **T1 覆盖**：Rust Reference/Nomicon/serde 官方、Android Developers 全套架构文档、React.dev（state-as-a-snapshot / you-might-not-need-an-effect / choosing-the-state-structure / memo / useSyncExternalStore / React 18 升级指南 / reactwg #70）、Vue 官方指南与 RFC #369、Kotlin 官方 sealed classes、Bloc 官方 modeling-state、Elm 官方指南与 `elm/json` 源码、Protobuf 官方 proto3+encoding、Reselect/Redux 官方 style-guide 与 best-practices、Dart 官方 null-safety 迁移指南与 dart:ffi、Flutter StandardMethodCodec javadoc。
- **T2 覆盖**（star/活跃度 2026-10-27 经 GitHub API 核对）：React 251.9k★/MIT、Vue core 54.9k★/MIT、Tauri 112.2k★/Apache-2.0、protobuf 72.1k★、Solid 36.2k★/MIT、Reselect 19.0k★/MIT、Bloc 12.5k★/MIT、Redux Toolkit 11.2k★/MIT、Serde 10.9k★/Apache-2.0、Druid 9.7k★/Apache-2.0、cxx 6.85k★/Apache-2.0、Xilem 5.58k★/Apache-2.0、nomicon 2.35k★/Apache-2.0、vuejs/rfcs 4.93k★；源码级证据含 `elm/json` 的 `Json/Decode.elm`、Xilem `examples/calc.rs`、Druid `README.md`、serde issues #2010、rust-unofficial/patterns #313、rust-lang/rust #65546、vuejs/core #9574/#5359。
- **查不到 / 未取证项（如实记录，未用训练数据补）**：①Android 官方对「ViewModel 太大」没有专门页面，只能由 domain-layer 页与 stateholders 页的原文间接支撑——God ViewModel 的**具体事故报告**未找到官方级来源，仅有 T3 博客（且直接读被 403，仅取检索摘要）。②「Elm 对未知 tag 的默认行为」在 `elm/json` 源码里由 `oneOf`/`andThen` 组合表达，**未找到官方文档明确写「未知 tag → Err」**，故本文只陈述源码可见事实（缺字段即 Err、`oneOf` 全败才 Err），未断言 Elm 有内置 Unknown 兜底。③`bytemuck` 的 GitHub 仓库路径核对失败（404/重定向），仅引用 docs.rs 官方文档页，tier 按「官方 crate 文档」记 T1。④Svelte 的细粒度机制未单独取证（其官方 overview 页无相应章节），故第 9 类的细粒度证据只取 Solid 与 Vue，未把 Svelte 列入。⑤Tauri v1→v2 迁移指南页面 404，未取证「Tauri 换代迫使采用方大改」的官方迁移证据——第 10 类的换代证据改由 Druid→Xilem（同一 lineage 的框架换代）承担，结论更硬。
