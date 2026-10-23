# ADR 0005: Journal 状态面关联类型化（`Journal<A, S>` → `Journal<A>` + `type State`）

- 状态：已采纳（批 6A，2026-10-18）
- 关联：`0004-std-ext-split.md`（前序分层）；终评报告 `reports/2026-10-18-adopters-blind-final.md` 三槽点之三

## 背景

`Journal<A, S>`（实现层）与 `JournalPort<A, S>`（契约层）的 `S` 不被任何方法参数约束：`append_action(&mut self, action: &A)` 的签名里没有 `S`。方法选择时 trait 引用 `JsonlJournal: Journal<A, ?S>` 存在无法钉死的自由变量 → E0283，**每个调用点**被迫 `Journal::<A, S>::append_action(...)` UFCS 全限定；账本在 `Mutex` 后时 UFCS 不能自动 deref，再加 `&mut *guard` 显式 deref。框架用文档消化本应存在于 API 形状里的税（教程驻足节 + cheatsheet 惯用形行 + README 第一天提示三处专伺候此坑）。

## 方案对比（实证）

### 方案一：`S` 关联类型化（采纳）

`Journal<A>` + `type State: Serialize + DeserializeOwned`。

**关键约束（探针 1）**：`type State = S` 不约束 impl 泛型参数——非泛型具体类型上写 `impl<A, S> Journal<A> for JsonlJournal { type State = S; }` 被编译器 E0207 拒绝（最小复现 `rustc` 直证）。因此方案一**必然**要求 `JsonlJournal` 泛型化：`JsonlJournal<A, S>` + `PhantomData<fn() -> (A, S)>` 标记（`fn()` 形对 Send/Sync/变体中性，零布局成本）。

**收益（探针 3 全绿实证）**：`Self` 类型提及 (A, S) → 方法选择时 trait 引用被 Self 钉死——构造期一次定型（`JsonlJournal::<A, S>::create(..)`）后：

- `j.append_action(&a)` 直呼，`A` 从参数自然推断；
- `MutexGuard` 上 `g.append_action(&a)` 直呼（guard 自动 deref，零显式 deref）；
- `j.replay()` / `j.restore_stream()` 返回值**全推断**（比旧世界连读侧绑定标注都省）；
- `append_snapshot(&Self::State)` 直呼（S 由 Self 归一）。

税从「每调用点」搬到「每账本实例构造一次」——落最冷点。object safety 不破：`dyn Journal<A, State = S>` 合法，`DynJournal<A, S>` 别名同步为 `dyn Journal<A, State = S>` 短形。

### 方案二：写/读面拆 trait（否决）

`Journal<A>`（只留写面 append_action）+ `JournalRestore<A, S>`（restore_stream/append_snapshot/replay）。

**证伪（探针 2）**：拆面后读面方法 `append_snapshot(&mut self, state: &S)` 的 `A` 完全不进该方法签名且无任何钉住来源 → 方法直呼同样 E0282（最小复现 `rustc` 直证：`j.snap(&s)` 拒编译）；`replay` 需下游绑定钉 A（裸 `.count()` 链即挂）。即方案二**只修好写面**，读面同病还在，还多了「一个账本两个 trait」的物面分裂——结构性收益为负。

### 拍板

方案一。方案二的「保留非泛型 JsonlJournal」优点换不来 DX（写面之外的坑原样保留）；方案一的代价（构造点标注 + 泛型参数噪音）集中在最冷位置，且换来读侧推断升级。终评背书方向成立。

## 终形

```rust
pub trait Journal<A>
where
    A: Serialize + DeserializeOwned,
{
    type State: Serialize + DeserializeOwned;
    fn append_action(&mut self, action: &A) -> Result<(), JournalError>;
    fn restore_stream(&self) -> Result<Option<(Self::State, Replay<'_, A>)>, JournalError>;
    fn append_snapshot(&mut self, state: &Self::State) -> Result<(), JournalError>;
    fn replay(&self) -> Result<Replay<'_, A>, JournalError>;
}
pub type DynJournal<A, S> = dyn Journal<A, State = S>;
pub struct JsonlJournal<A, S> { /* PhantomData<fn() -> (A, S)> 标记 */ }
```

契约层同款：`JournalPort<A>` + `type State`（`restore` 返回 `Restore<A, Self::State>`；`type Err` 关联类型不变）。验收锚：`tests/journal_port.rs::mutex_guard_direct_append_zero_turbofish`（MutexGuard 直呼零 turbofish 零显式 deref 零绑定标注 + dyn 焊接面直呼，旧代码下该测试 E0283 红）。

## 破坏性清单（无 shim 直迁）

| 项 | 旧 | 新 |
|---|---|---|
| 实现层 trait | `Journal<A, S>` | `Journal<A>` + `type State` |
| 契约端口 | `JournalPort<A, S>` | `JournalPort<A>` + `type State` |
| 具体账本类型 | `JsonlJournal`（零参数） | `JsonlJournal<A, S>`（构造点需标注） |
| dyn 别名 | `dyn Journal<A, S>` | `dyn Journal<A, State = S>`（`DynJournal<A, S>` 短形不变） |
| 方法签名 | `restore_stream -> Option<(S, ..)>` / `append_snapshot(&S)` / `restore -> Restore<A, S>` | `Self::State` 形（方法调用面零变化） |
| 外部实现方 | `impl JournalPort<A, S> for ...` | `impl JournalPort<A> for ...` + `type State = S;` 一行 |

## 文档撤税面

| 处 | 旧（E0283 专伺候） | 新 |
|---|---|---|
| 教程三十分钟 · 驻足节 | 「UFCS 全限定」教行 | 「构造期定型，调用期直呼」 |
| 教程三十分钟 · 坑 E | MutexGuard E0283 坑（整节） | 删除（坑不存在了） |
| cheatsheet §1 惯用形行 | E0283 + UFCS 组合 | 新惯用形一行 |
| cheatsheet 坑 4 | MutexGuard E0283 | 换真坑：定型账本塞异型 payload（E0308） |
| README 第一天提示 | UFCS + 显式 deref | 新惯用形一行 |
| reference.md journal 节 | 旧签名 + turbofish 注记 | 新签名 + 构造期定型注记 |

附带：教程坑 A 教学指令修正——「删掉 `#[serde(tag)]` 再跑」实测不复现（struct variant 外部 tag 序列化恰好是 object），改为「给 `Clear`（unit variant）入账再跑」的真实命中路径（报错逐字命中 `action/state must serialize to a JSON object for tag flattening`）。README 补「只要散件」最小 Cargo.toml 组合示例（终评次级槽点）。

## 埋点宏（`embed_event!`）双副本评估

现状：`std-ext/src/lib.rs` 与 `skeleton-infra/src/lib.rs` 各持一份文本副本（14 行 × 2 cfg 臂；std-ext 七域 35 调用点，infra shutdown 一域 6 调用点）。

**单源化可行性**：技术可行——`#[macro_export]` 于 std-ext，infra `#[cfg(feature = "log-embed")] pub use std_ext::embed_event;` + 本地 no-op 臂（`use` 导入与 `macro_rules!` 同名经 cfg 互斥不冲突；宏体仅引用 `::tracing` 绝对路径，无 `$crate`，跨 crate 展开成立）。

**否决理由（可行但劣化面更大）**：

1. **门控语义错位**：宏的开/关绑在**定义 crate** 的 feature 上。infra 开自己的 `log-embed` 而 std-ext 关时，导入的是 no-op 臂 → infra 6 个调用点**静默失明**（埋点静默消失=可观测性静默失效）。补救需 infra 的 `log-embed` feature 加 `std-ext/log-embed` 依赖 → 启用 infra 埋点强制点亮 std-ext 七域 35 调用点，feature 语义被耦合改变。
2. **公开 API 面**：`embed_event!` 是内部埋点宏，`#[macro_export]` 使其成为 std-ext 公开 API（semver 面）；`#[doc(hidden)]` 只治文档不治承诺。
3. **cfg 复杂度**：双臂 cfg 导入 + 本地 fallback 臂的结构比「dumb copy」更难读，省下的是一份 14 行冻结宏的同步成本。

维持双副本 + 两侧**同步责任注释**（`skeleton-infra/src/lib.rs` 与 `std-ext/src/lib.rs` 宏头注互指、声明逐字镜像义务与 ADR 指针）。

## 验证

- 直呼验收测试：`tests/journal_port.rs::mutex_guard_direct_append_zero_turbofish`（红→绿全流程亲历）
- 正典口径全绿：`cargo test --workspace --all-features --no-fail-fast -- --test-threads=1`（数字见批 6A 报告）
- 教程代码块真实重验（第 0-3 步重跑）+ 坑 A 报错逐字命中
