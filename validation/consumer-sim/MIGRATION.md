# 下游迁移说明（模拟采用方视角）

本文件由 `validation/consumer-sim/` 编译夹具实测得出——夹具是**独立 workspace**（自带空
`[workspace]` 根），只以 path 依赖指向仓库三 crate，因此编译视角 = 外部 crate 视角：
凡「同 crate 豁免」的规则在此一律生效。

验证命令（在本目录）：

```
cargo check --all-targets   # 期望：绿（迁移面完整）
cargo run                   # 期望：consumer-sim: OK（端到端真跑）
```

输出留痕：本地 `out.log`（不入库；见 `.gitignore` 的 `validation/**/out.log`）。

---

## 第二消费方（#56 Stage A）：CLI/workflow 型 · sync-only 面

`workflow/` 是同目录下的**第二个独立 workspace**（自带空 `[workspace]` 根），定位是
**结构相异的消费方**：CLI/workflow 型宿主、单线程同步档，只经公开 API 走一条最小
生命线。与 `src/main.rs`（全 API 面扫街）互补，不是替代：

- **自定义 `EffectRuntime` 实现**（`RecordingFx`）：把执行过的 `EffectId`/效果形态
  记进 `Vec`，不接真实 I/O、不 spawn、不 `await`；未知效果变体显式留痕。
- **有界准入**：`SyncStore::set_capacity` + `try_dispatch -> AdmissionOutcome`
  （后者非 `non_exhaustive` ⇒ 跨 crate 穷尽 match 即编译通过）。
- **graceful shutdown**：`Store::close` / `is_closed` 停机闸；闭门后 `try_dispatch`
  走 `AdmissionOutcome::Closed` typed 拒绝。

**Stage A 边界**：本切片只交付**编译级证据 + 依赖树证据**（无私有模块访问、不 require
内部 cfg、不 require 内部 feature 矩阵）。并发下的 `Full` 路径、停机排空时序等
**runtime 行为属 #56 Stage B**，此处不测。

```bash
cd workflow
cargo check          # sync-only 面：绿
cargo run            # 输出 workflow_consumer: OK
cargo tree -e normal # 无 tokio（infra 只开默认 feature：runtime + pool）
```

**依赖树两面证据**（全文见同目录 `dep-trees.txt`，入库）：

| 面 | 目录 | `cargo tree -e normal` | tokio |
|---|---|---|---|
| sync-only | `workflow/` | 4 节点全 path | **无** |
| async | 本目录（消费方 A） | 11 节点 | 有（`udf-skeleton-infra/tokio` + 直接 `tokio`） |

两侧是同一组三个 path 依赖，差异只在 feature 选择：**sync-only 是默认面，async 面
显式点亮 `udf-skeleton-infra/tokio`**（连带 `dep:tokio`）并由宿主直接依赖 tokio
（FxScope 的 `spawn_tracked` 需运行时上下文）——无需新增开关。

---

## 破面一：`EffectId` 构造入口收窄（受控构造）

**为什么变**：`EffectId` 内部值私有，裸元组语法 `EffectId(n)` 编译期不可用。构造入口
唯一化为 `EffectId::new`，读值出口唯一化为 `EffectId::get`——id 的产生点从此在代码里
可 grep 可审计，不再有「任何整数随手当句柄」的裸通道。

| | 旧写法 | 新写法 |
|---|---|---|
| 构造 | `EffectId(1)` | `EffectId::new(1)` |
| 读值 | `id.0`（字段私有，外部不可） | `id.get()` |

```rust
// 旧写法（外部 crate 编译期即拒：E0423 cannot initialize a tuple struct which
// contains private fields —— "constructor is not visible here due to private fields"）
// let id = EffectId(1);

// 新写法
let id = EffectId::new(7);
assert_eq!(id.get(), 7);
```

**迁移成本**：机械替换，无设计决策。`EffectId` 是 `Copy + Eq + Hash` 小句柄，
`new`/`get` 均为 `const fn`——常量上下文可用性与旧写法等价。

**鉴别力证明**（夹具内反例 A，取消注释即红）：见 `src/main.rs` 末尾注释块。

---

## 破面二：`#[non_exhaustive]` 词表 —— 跨 crate match 必须带通配臂

**为什么变**：`Effect` / `Restore` 词表按真实采用面渐进扩，`#[non_exhaustive]` 使加变体
属非破坏演进。跨 crate 的 match 必须带通配臂——采用方在编译期被强制面对「未知效果
形态 / 未知恢复形态」的分支（fail-closed），而非静默漏配。

| | 旧写法（穷举三/三态，无通配臂） | 新写法（补通配臂） |
|---|---|---|
| `Effect` | `match e { Emit(..) => .., Run{..} => .., Cancel{..} => .. }` | `match e { Emit(..) => .., Run{..} => .., Cancel{..} => .., _ => .. }` |
| `Restore` | `match r { Empty => .., FromInitial(v) => .., FromSnapshot(s,v) => .. }` | `match r { Empty => .., FromInitial(v) => .., FromSnapshot(s,v) => .., _ => .. }` |

```rust
// 旧写法（外部 crate 编译期即拒：E0004 non-exhaustive patterns: `&_` not covered
// —— "marked as non-exhaustive, so a wildcard `_` is necessary"）
// match e {
//     Effect::Emit(_) => {}
//     Effect::Run { .. } => {}
//     Effect::Cancel { .. } => {}
// }

// 新写法
fn describe_effect(effect: &Effect<u64, Infallible>) -> &'static str {
    match effect {
        Effect::Emit(_) => "emit",
        Effect::Run { id, .. } => { /* … */ "run" }
        Effect::Cancel { id } => { /* … */ "cancel" }
        _ => "unknown-effect-variant", // 未知形态走 fail-closed 分支
    }
}
```

**迁移成本**：加一个通配臂（惯用形 `_ => {}` 或 `_ => unreachable!()`——但仓库纪律
要求 fail-closed 显式分支，禁 `unreachable!()` 静默炸：建议落到可观测的兜底分支）。

**鉴别力证明**（夹具内反例 B）：见 `src/main.rs` 末尾注释块。

---

## 破面三：combine 分片返回类型 `impl Reducer<…>` → `impl Fragment<…>`

**为什么变**（ADR 0009）：`combine`/`combine!` 产物是**状态分片**——签名无父状态工厂，
只有 reduce 委托语义。旧形态它与真根同实现 `Reducer`，装配期零形状差别（误用只在
`initial()` 处 panic）；拆分后分片只实现 `Fragment`（reduce-only），根契约留在
`Reducer`（initial + reduce）。分片作根从此 **E0277 编译期拒绝**，错误信息自带修复指引。

| | 旧写法 | 新写法 |
|---|---|---|
| 分片工厂返回类型 | `fn f() -> impl Reducer<State = S, Action = A, Op = O>` | `fn f() -> impl Fragment<State = S, Action = A, Op = O>` |
| use 行 | `use skeleton::Reducer;` | `use skeleton::{Fragment, Reducer};`（两者都要：`Reducer` 仍用于完整 Feature） |
| 分片作根 | `SyncStore::new(fragment, fx)` → 装配期 panic | `SyncStore::new(fragment, fx)` → **E0277 编译期拒绝** |

```rust
// 旧写法（ADR 0009 后编译期即拒：E0277 "`impl Fragment<…>` 不是可挂载的根：根契约
// `Reducer` 未实现" —— note: "combine!/combine 产物是状态分片（`Fragment`）：没有
// `initial`，不能作根；根状态由父 Feature 的 `initial` 显式构造（含子切片初值）"）
// let store = SyncStore::new(child_fragment(), NoFx);

// 新写法：分片只作子件持有，根由父 Feature 的 initial 显式构造
fn child_fragment() -> impl Fragment<State = ParentState, Action = ParentAction, Op = Infallible> {
    combine(/* get, set, wrap, unwrap, child, lift_fx */)
}

struct ParentFeature;
impl Reducer for ParentFeature {
    type State = ParentState;
    type Action = ParentAction;
    type Op = Infallible;
    fn initial(&self) -> ParentState { /* 根状态唯一合法来源，含子切片初值 */ }
    fn reduce(&self, s: &ParentState, a: ParentAction, out: &mut EffectBuf<_, _>) -> ParentState {
        match a {
            ParentAction::Child(_) => child_fragment().reduce(s, a, out), // 委托分片
            /* … */
        }
    }
}

let store = SyncStore::new(ParentFeature, NoFx); // 根 = 完整 Feature
```

**迁移成本**：
- 只写 Feature（`impl Reducer` 含 initial + reduce，`SyncStore::new(feature, fx)`
  挂载）：**零改动**——`Reducer` trait 本体未变，`impl Reducer` 一处不动。
- 持有分片并注解返回类型：返回注解改 `impl Fragment<…>`，use 行加 `Fragment`。
  委托调用 `.reduce(...)` 写法不变。
- 曾把分片传给 `SyncStore::new`/`TestStore::new`：此前装配期 panic，现在编译期
  E0277 即给出修复指引——根状态由父 Feature 的 `initial` 显式构造（含子切片初值），
  父 Feature 本体才是根。
- 少见形态：同文件同时 import `Fragment` 与 `Reducer` 且直接裸调**完整 Feature** 的
  `.reduce()` 会报 E0034（二义，两个候选）——用 `Reducer::reduce(&feature, …)` 显式
  分发。（框架树内零此形态：直接裸调的都是分片。）

**鉴别力证明**（夹具内反例 C）：见 `src/main.rs` 末尾注释块。

---

## 附：今日新增面的采用方用法（无需迁移，但需知晓）

| 面 | 采用方入口 | 要点 |
|---|---|---|
| `Store` 六方法 | `skeleton::Store` trait | `dispatch` / `snapshot` / `subscribe` / `sender` / `close` / `is_closed`；`subscribe` 返回票据 `#[must_use]`（丢弃即死订阅），须绑定持有 |
| `notify` 异步信号档 | `skeleton::notify::{Notify, NotifySink}` | 宿主实现 `NotifySink::publish(version)`（**O(1) 返回、非阻塞**——重活移出调用）；登记经 `Notify::subscribe_notify`，票据 Drop = 注销；版本号单调水位，见版本变化再 `Store::snapshot` 拉现态 |
| `FxScope` 效果作用域 | `skeleton_infra::fx::FxScope` | 唯一合法 spawn 口（`spawn_tracked` 纳入生命周期集 / `spawn_detached` 显式脱管）；需 tokio 运行时上下文；`scope.expect_body_idle()` 作任务体结束断言锚（**原 `expect_idle(&scope)` 已删——见破面四**） |
| fx 三资源维度 + 有界准入 | `skeleton_infra::fx::{FxScope, AdmissionError}` | **纯新增面**（issue#39）：`with_concurrency` 只限**任务体并发**，**不是**内存/保留量上界——保留上界 = `with_max_tracked`（含已完成未收割条目），等待许可数 = `with_max_pending`，两者**默认无界**（opt-in 显式设）。有界准入走 `try_spawn_tracked -> Result<Spawned<T>, AdmissionError>`，过载 **typed 拒绝**（`Closed`/`PendingFull`/`TrackedFull`，不阻塞不 panic）；issue#92 另增 `NoRuntime`（调用线程不在 tokio runtime 上下文——**环境前置条件，不是过载也不是闸关**：typed 拒绝、不 panic、不计入 `rejected`；夹具 `src/main.rs` 在 `block_on` 之外实测）；`AdmissionError` 非 `non_exhaustive`，升级后穷尽 match 的采用方须补这一臂（否则 E0004）；`spawn_tracked` 仍是**无界旧口**。`counts()` 给三轴读数（`active`/`waiting`/`completed_unreaped`/`rejected`）。**许可分配不保证 FIFO**（tokio `try_acquire` 不看等待队列，新准入者可越过等待者） |
| SyncStore 有界准入 | `skeleton_infra::runtime::{AdmissionOutcome}` | **纯新增面**（issue#38）：`set_capacity(n)` 定容量、`try_dispatch(action) -> AdmissionOutcome`（`Accepted`/`Full`/`Closed`）typed 判定、`admission_full_count()` 读被拒次数。容量只约束**等待被 reduce 的 action 数**（执行中不计入），满时 typed 拒绝**不阻塞**（阻塞会与回调重入死锁）；legacy `dispatch` 显式 panic。`AdmissionOutcome` 与 `AdmissionError` 均**非** `non_exhaustive` ⇒ 跨 crate 可穷尽 match |
| 通用对象池 `ObjectPool` | `skeleton_infra::{ObjectPool, ObjectPoolTuning, Pooled, ResetPhase}` | **纯新增面，零迁移**（issue#15）：池化**有状态**对象。`acquire()` 返回 `Pooled<'_, T>`——`DerefMut` 拿对象本体改参数即复用、`Drop` RAII 归还、`detach()` 永久移交（对象脱离池生命周期）。可选 reset 钩子（`ResetPhase::OnReturn` 归还时 / `OnAcquire` 取出时）。`acquire()` **永不阻塞永不失败**（池满降级直构 + `degraded` 计数）。`Pooled<'p, T>` 借用池、非 `'static`，带不进 `'static` 任务闭包——跨边界用 `detach()`。实现 `skeleton::pool::PoolManaged`（`metrics()` 经 `Borrow<PoolMetrics>` 可达核心形）。详见 `docs/reference.md` 的 `ObjectPool` 章 |

---

## 新增面（非破面）：通用对象池 `ObjectPool`（issue#15）

**为什么是新增面**：本件是**纯增量**——`skeleton` 契约 crate 零改动、`PoolBase` 公开面与
行为零变化、`MemoryPool` 语义不动（两者并存，非替代）。采用方**无需任何迁移**，
现有代码编译行为逐字不变。

**采用方怎么用**（消费方三套自建池的收敛路径）：

| 场景 | 用法 |
|---|---|
| 加解密上下文池（构造昂贵、每次都被重造） | `ObjectPool::with_reset(max, idle_cap, factory, reset_fn, ResetPhase::OnReturn)`——`reset_fn` 把对象重置回干净初始态（只重置必要部分，保留可复用部分） |
| 连接对象池（捞出来改参数即用） | `acquire()` → `DerefMut` 改连接参数 → 用 → Drop 归还；池满时降级直构（不阻塞不失败），`degraded` 计数留痕 |
| 长期不用的对象要释放 | `idle_cap` 硬顶 + `prune_idle_with_clock(max_idle_age, &clock)` 显式老化口（Clock 注入：MockClock 测试 / SystemClock 生产；宿主自调，池不起后台线程；`Instant` 版 `prune_idle` 已弃用）+ 池 drop 全释放 |
| 坏对象禁还池 | 判定权在调用方（池不知道 `T` 是什么）——`detach()` 带走不还池 |

**鉴别力证明**（夹具内反例 E，取消注释即红）：`Pooled` 字段私有，外部 crate 写不出裸
守卫（裸守卫 = 绕过 RAII 归还 = 池语义自毁）——见 `src/main.rs` 末尾注释块。

---

## 破面四：`expect_idle` 自由函数删除 → 拆为两个口径的方法（issue#39）

**为什么变**：原 `expect_idle(scope)` 只断言「任务体数归零」，但名字易被读成更强的
「生命周期集合已完全收割」——而这是**两个不同的判据**（`Spawned::join()` 只消费结果、
**不**移除集合条目；收割口是 `try_reap`）。拆成两个各自精确的方法后，名字不可能被误读。

| | 旧写法 | 新写法 |
|---|---|---|
| import | `use skeleton_infra::fx::expect_idle;` | `use skeleton_infra::fx::FxScope;`（**方法**，无需 import） |
| 任务体结束断言 | `expect_idle(&scope);` | `scope.expect_body_idle();` |
| 集合已完全收割 | （旧口无法表达） | `scope.expect_fully_reaped_idle();`（需先 `try_reap()`） |

```rust
// 旧写法（外部 crate 编译期即拒：E0432 unresolved import `skeleton_infra::fx::expect_idle`
// + E0599 no method named `expect_idle` found for struct `FxScope`）：
// use skeleton_infra::fx::expect_idle;
// scope.expect_idle();

// 新写法
scope.expect_body_idle();        // 任务体全部终局（完成 / panic / 取消 / abort）
let _reaped = scope.try_reap();   // 收割已完结条目
scope.expect_fully_reaped_idle(); // 集合条目已全部收割（更强判据）
```

**迁移成本**：机械替换，但**必须选对口径**——原意若是「等任务跑完」→ `expect_body_idle`；
若原意是「集合清空」→ 需补 `try_reap()` 后用 `expect_fully_reaped_idle`。
**无 shim、无旧别名**（`skeleton_infra::expect_idle` 的 crate 根重导出同步移除）。

**鉴别力证明**（夹具内反例 F，取消注释即红）：见 `src/main.rs` 末尾注释块。

---

## 汇总：四处破面的一句话对照

| 破面 | 旧写法 | 新写法 | 失败形态（迁移时看到什么） |
|---|---|---|---|
| EffectId 构造 | `EffectId(1)` | `EffectId::new(1)` / `id.get()` | E0423（字段私有） |
| non_exhaustive 通配臂 | 穷举 `Effect`/`Restore` 无 `_` | 补 `_ =>` 兜底臂 | E0004（non-exhaustive patterns） |
| Fragment 返回类型 | `-> impl Reducer<…>` + 分片作根 | `-> impl Fragment<…>`，根由父 Feature 承 | E0277（根契约 `Reducer` 未实现） |
| fx 空闲断言 | `expect_idle(&scope)`（自由函数） | `scope.expect_body_idle()` / `scope.expect_fully_reaped_idle()` | E0432（unresolved import）+ E0599（no method） |
