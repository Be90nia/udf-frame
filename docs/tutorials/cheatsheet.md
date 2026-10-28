# 双形态速查（cheat sheet）

> 本框架多处「一对双形态」——每处单独都有理由（保签名 / 停机显式化 / 词表分工），
> 但第一天需要的是一张对照表：**场景 → 用哪个 → 为什么**。
> 背景与完整代码路径见 [三十分钟教程](thirty-min.md)；全键 API 见 [../reference.md](../reference.md)。

## 五对双形态

### 1. `JournalPort`（契约层）vs `Journal`（实现层）

| | `skeleton::JournalPort<A>`（`State` 关联类型） | `skeleton_infra::journal::Journal<A>`（`State` 关联类型） |
|---|---|---|
| 词表 | `append` / `restore` / `persist_snapshot` | `append_action` / `restore_stream` / `append_snapshot` + `replay` |
| 恢复形态 | 物化三态 `Restore<A, Self::State>`（`Empty` / `FromInitial` / `FromSnapshot`） | 流式：`Some((快照态, Replay<'_, A>))` 或 `None` |

- **场景：换后端（SQLite/网络账本）或业务逻辑依赖注入** → 面向 `JournalPort` 写 → 为什么：端口独占契约词表，后端可整体替换；两 trait 同屏 import 直呼无歧义（方法名刻意分叉）。
- **场景：宿主自己编排恢复/要做增量流式/直接翻账本** → 用 `Journal` → 为什么：`replay` 与 `restore_stream` 是实现层独有能力，词表与端口不重名。
- **调用惯用形**：状态面是关联类型 `State`（被 `Self` 钉死）——构造一次定型（`JsonlJournal::<Action, Board>::create(..)`）后全直呼零标注：`j.append_action(&a)`、`MutexGuard` 后 `g.append_action(&a)`、`let r = j.restore_stream()`（全推断）。trait-object 面用别名 `DynJournal<A, S>`（= `dyn Journal<A, State = S>`）。

### 2. `subscribe`（跨线程档）vs `subscribe_local`（单线程档）

| | `subscribe` | `subscribe_local` |
|---|---|---|
| 回调约束 | `FnMut(&S) + Send + 'static`，槽位 Mutex 串行 | `FnMut(&S) + 'static`（可用 `Rc`/`RefCell`） |
| 触发位置 | 任意线程的 dispatch | 只在**注册线程**的 dispatch |
| 票据 | `Subscription<S>`（`Send + Sync`） | `LocalSubscription`（`!Send + !Sync`，线程亲和的类型化表达） |
| 计入 `subscriber_count` | ✅ | ❌ |

- **场景：UI 线程外的观测/日志/指标** → `subscribe` → 为什么：跨线程可达，锁串行保证通知不重入。
- **场景：单线程渲染循环 / 回调里要持 `Rc` 状态** → `subscribe_local` → 为什么：免锁免原子量、免 `Send` 样板。
- 注意：tokio 多线程运行时下任务可能迁移 worker——**跨 `.await` 依赖本地订阅**的程序请用 current_thread 运行时或改跨线程档；同一 dispatch 内通知序确定：本地先、跨线程后。

### 3. `store.dispatch(a)`（静默 no-op）vs `store.sender().dispatch(a)`（返 `false`）

| | `Store::dispatch` | `ActionSender::dispatch` |
|---|---|---|
| 闭门后 | 恒 no-op（签名不引入 `Err`） | 返回 `false`（`#[must_use]` 兜底） |
| fail-visible 面 | 事后读 `rejected_after_close()`（同一计数） | 当场 `false`，同一计数 |

- **场景：动作源是同步代码/输入事件**（调用面不想被 Result 污染） → `dispatch` → 为什么：保签名；拒绝留痕在计数里，可观测但不感染类型。
- **场景：异步回灌/跨任务发送**（停机窗必须当场知道） → `sender().dispatch` → 为什么：回灌丢了必须显式，静默丢 action = 真相源丢史。
- 两入口闭门后走**同一个** `rejected_after_close` 计数，可交叉对账。

### 4. `combine!`（宏）vs `combine`（函数）

| | `combine!(Child, Parent.field, Enum::Variant)` | `combine(get, set, wrap, unwrap, child, lift_fx)` |
|---|---|---|
| 一行装配 | ✅ 镜头/路由/克隆上提一体生成 | ❌ 六参逐项手写 |
| `wrap` 收什么 | 变体构造器（tuple variant 天然是 `Fn(CA) -> PA`） | 任意闭包/`Fn(CA) -> PA` |
| 子动作 `Clone` 要求 | unwrap 自动克隆上浮 → 要求 `Clone` | 不要求（unwrap 自己写） |
| 效果上提 | `lift:` 省略 = 零效果上提 | `lift_fx` 必给（显式 `|_, _| {}` 同义） |
| 状态切片 | 限 `状态.字段` 裸段 | 任意（泛型/路径状态走这里） |

- **场景：子域有真动作类型、父路由是 tuple variant（`Act::Done(DoneAct)`）** → 宏短形态 → 为什么：一行装配，零样板，组合税个位数 ns。
- **场景：子动作非 Clone / 路由不是简单变体 / 状态切片是泛型或路径** → 函数形态（或宏显式形态 `wrap:`/`unwrap:`/`lift:`） → 为什么：逃生口，语义与六参逐项对应。
- **unit variant 路由（子动作 `()`）**：两形态都**不收裸构造器**——函数形态 E0277（`expected a `Fn(_)` closure, found `Act``），宏短形态 E0532（`expected tuple struct or tuple variant`）。修复 = 显式闭包 `|_: ()| Act::Ping`；能设计成 tuple variant 就设计成 tuple variant。
- 共同红线：产物是**状态分片不是根**——只实现 `Fragment`（reduce-only，无 `initial`），作根 = E0277 编译期拒绝；根状态由父 Feature 显式构造（含子初值）。

### 5. `subscriber_count()` 口径

- **只数跨线程档**（`subscribe`）；`subscribe_local` 的存活期由 `LocalSubscription` 票据表达，不进计数——断言订阅数时先分档。
- **闭门后**：`subscribe` 照常登记、计数照涨，但永无通知（已知取舍）——用 `subscriptions_after_close()` 交叉查证「这是闭门后登记的」。
- 票据 Drop 即注销（`#[must_use]` 拦裸调用），计数读侧同步回落。

## 第一天十坑速查（盲测实测）

| # | 坑 | 症状（真实报错摘录） | 修法 |
|---|---|---|---|
| 1 | journal payload 必须 JSON object | `action/state must serialize to a JSON object for tag flattening` | enum 加 `#[serde(tag = "kind")]`（名字任取，**不能叫 `"t"`**，那是行 tag 保留键：`payload must not contain a top-level "t" key`） |
| 2 | flush 前读侧空账 | 重放 0 条 | **flush=fsync 掉电真持久**（干净 drop 亦有 BufWriter 兜底写出；真丢数据分界=进程崩溃）：读侧重开/断言/停机前显式 `j.flush()` |
| 3 | `FxScope::spawn_tracked` 无 reactor | `there is no reactor running, must be called from the context of a Tokio 1.x runtime` | 从 `#[tokio::main]` 起步 |
| 4 | 往定型账本塞别的动作类型 | E0308 mismatched types（`expected &Act, found &Other`） | `JsonlJournal::<A, S>` 构造期定型：一个账本实例一个 payload 类型；异型动作另开账本或并入同一 enum |
| 5 | combine 传 unit variant 构造器 | E0277 `expected a `Fn(_)` closure, found `Act`` / E0532 | 显式闭包 `|_: ()| Act::Ping`，或改 tuple variant 路由 |
| 6 | `subscriber_count` 断言挂 | 本地订阅不在数内 | 口径见上表第 5 对 |
| 7 | 仓库树内 `cargo new` | 根 workspace 被吸收，`error: current package believes it's in a workspace when it's not` / `multiple workspace roots` | **demo 项目建在仓库外**（教程第 0 步已改 `~/kanban-demo`） |
| 8 | `Scheduler::every` 注册后永不点火 | 注册 `Ok` 无任何提示，任务零执行 | **注册仅入堆**：生产面 `sched.spawn_worker()`（或确定性测试 `tick()`）才有点火驱动——见 `after`/`every` doc「点火前提」 |
| 9 | `State` 缺 `Clone` | Store/combine 快照广播处编译错（CoW 快照与 `Snapshot` 广播要求 `S: Clone`） | `#[derive(Clone)]` 补在状态类型上（教程各步状态均已带；自建状态勿漏） |
| 10 | combine 分片当根挂载 | E0277 `` `Combine<…>` 不是可挂载的根：根契约 `Reducer` 未实现 ``（note：分片是 `Fragment`，没有 `initial`） | 根状态由父 Feature 的 `initial` 显式构造（含子初值）、父 Feature 本体挂载、`reduce` 内委托分片；`-> impl Fragment` 自述「我是子件」 |

## 一句话心智模型

> **契约层（skeleton）管形状，实现层（skeleton-infra）管机器；描述（Effect/Action/State）是数据，解释（EffectRuntime）在边缘；账本是唯一真相源，内存只是它的投影。**
