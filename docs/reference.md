# skeleton-infra / std-ext API 参考

> 权威口径：本文从 `src/*.rs` doc 注释、`tests/` 行为面与 `Cargo.toml` feature 表提取，与源码逐字对表（抽查记录见 API 参考抽查（本地留存，未入库））。crate 级文档（`lib.rs`）仍是域典；本文是其展开的检索面。
>
> 姊妹 crate [`skeleton`](../skeleton)（零依赖）承载契约 trait 与数据词汇（`Store`/`Reducer`/`Effect`/`EffectRuntime`/`JournalPort`/`PoolManaged`/parse 端口等）；[`std-ext`](../std-ext)（批 5-1/5-2 物理拆分）承载十四个散件域（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`/`json`——十三域零门常驻、本体纯 std，`json` 域门控 `json` feature）；本 crate（skeleton-infra）兑现契约参考实现与核+胶水件。契约本身（trait 语义承诺）见 skeleton 文档，本文只写实现侧公共 API 与行为契约。

## 零依赖口径

`cargo tree -p udf-std-ext -e normal`：仅 `skeleton`（workspace 路径依赖）——零三方（开 `json` 门时 `serde_json` 子树按需进树；`log-embed` 埋点的 optional tracing 默认关不进树）。`cargo tree -p udf-skeleton-infra -e normal`——默认 feature 下仅 `skeleton` + `std-ext`（均 workspace 路径依赖）——零三方。各生态 feature 的三方依赖经 feature 门控（serde/tokio/tracing/toml 等），不随默认面进树。

口径说明：`json` 域随 P1-9 自 `xray-conf` 上浮，其 `serde_json` 依赖形态的决策见 ADR 0007。本节只陈述当前实测树。

下文各域标题里的「依赖面 … 零三方」说的是**该域自身代码**不调三方 API（如 `runtime` 域只用 `skeleton` 契约、`pool` 域只用纯 std）；**crate 级默认树**的第三方面以本节实测树为准（默认树零三方，可一行复核）。

## feature 矩阵

| feature | 依赖面 | 默认 | 内容 |
|---|---|---|---|
| `full` | 全部子开关 | — | **一键全开**（tokio 先例）：`features = ["full"]` = 下表 14 个消费面叶子并集（不含 `unsafe-evidence` 证据开关；`--all-features` = full + unsafe-evidence）；新手心智入口，子开关=进阶裁剪面 |
| `runtime` | — | ✅ | `SyncStore`（契约参考实现，std-only）+ `LocalSubscription` + `audit` 审计值面 |
| `pool` | `memgov` | ✅ | `PoolBase` 管理组合件 + `ThreadPool` + `PooledThreadPool` + `ObjectPool` 通用对象池（reset 钩子/有界降级/分片槽位）；连带启用 `memgov` |
| （常驻，std-ext） | 十三域零依赖；`json` 域经 `json` feature 门引 `serde_json` | — | `text` `backoff` `retry` `paths` `clock` `scheduler` `ratelimit` `cache` `fsx` `encode` `id` `iter` `hash` `json` **十四域**（`std-ext` crate；行为件面；时间源/配置/日志路径契约在 `skeleton::clock`/`skeleton::config`/`skeleton::logpath`，批 1 契约真下沉；十三域零门常驻，`json` 域门控 `json` feature） |
| （常驻，本 crate） | 零依赖 | — | `shutdown` 停机编排器（消费 `std_ext::scheduler`） |
| `memgov` | 零依赖 | — | `CountingAllocator`/`BoundedRegistry`/`LeakGuard`/`MemoryPool` |
| `journal` | serde, serde_json | — | `JsonlJournal`：`JournalPort` 契约端口参考实现 |
| `tokio` | tokio | — | `fx`：`FxScope` 异步并发托管 |
| `testing` | `tokio` + `runtime` | — | `TestStore`/`ScriptedRuntime` + `parse_mock` 测试替身 |
| `log-fs` | 零依赖 | — | `logx` 文件落盘面：密钥掩码/路径解析（契约 `LogPathResolver` 住 `skeleton::logpath`）/`FileSink` 按日滚动——单开即可用文件日志 |
| `log` | `log-fs` + log, tracing, tracing-subscriber, tracing-log | — | `logx` 门面绑定层：init 系（EnvFilter/双 sink）+ `FileSinkWriter` |
| `tracing` | `runtime` + tracing, tracing-subscriber | — | `tracingx`：审计事件流 → tracing 桥 |
| `log-embed` | tracing | — | 埋点：skeleton-infra=shutdown 一域 6 + fx 一域 1 = 7 点；std-ext 自有同款 feature=七域 48 点（合计九域 55；计数口径见 [log-embed 段](#log-embed--埋点规范)；默认关=宏消除零依赖零开销） |
| `compose` | `runtime` | — | `combine`/`combine!` 父子分片组合器（`compose = ["runtime"]`：单开连带 runtime，零三方成本） |
| `config-json` | serde, serde_json | — | `ConfigFile` 分层装配 + 原子保存（serde_json::Value 统一值面） |
| `config-toml` | `config-json` + toml | — | `load_toml`/`save_toml` + `file_layer_toml`（TOML↔值面翻译） |
| `view` | serde_json + `udf-std-ext/json` | — | `view::ViewBridge` 语言无关视图桥（ADR 0006）：契约词汇住 `skeleton::view`（零依赖）；默认树零三方不变 |

> 注（批 1 契约真下沉，2026-10-18；ADR 0001）：`skeleton` 由 optional 转常驻非 optional 依赖（常驻域 cache/id/retry/scheduler 消费 `skeleton::clock` 契约）——workspace path 非三方，`skeleton` 依赖面零三方口径不变。原 `runtime`/`pool`/`journal`/`compose` 的 `dep:skeleton` 叠门随之移除。

必选组合（`Cargo.toml` required-features 口径）：bench `infra` = `["tokio","journal","memgov","testing","pool"]`；示例 `end_to_end` = `["tokio","journal","testing","compose"]`。 hollow gate 纪律：feature 不全的目标整目标跳过——CI 闸与首跑一律 `--all-features`。

构建与测试：`cargo build --workspace --all-features` / `cargo test --workspace --all-features`。

## 域索引

> 批 5-1/5-2 物理拆分：下表十四常驻域（text/backoff/retry/paths/clock/scheduler/ratelimit/cache/fsx/encode/id/iter/hash/json）已迁 `std-ext` crate，模块路径 `std_ext::<domain>`；`shutdown` 留守 skeleton-infra（`skeleton_infra::shutdown`）。各域行内「常驻」= 零 feature 门（`json` 域例外：门控 `json` feature，见 ADR 0007）。

| 域 | feature 门 | 一行职责 | 模块锚 |
|---|---|---|---|
| runtime | `runtime`（默认） | `SyncStore`：单写者队列+同步排空环+快照广播的契约参考实现 | [`runtime`](#runtime--syncstore-契约参考实现) |
| audit | `runtime` | 审计事件值面（事件/票据），发射点在 runtime | [`runtime::..audit`](#runtime--syncstore-契约参考实现) |
| pool | `pool`（默认） | `PoolBase` 管理组合件 + `ThreadPool` 动态线程池 + `ObjectPool` 通用对象池 + 资源上限四问（cap 覆盖/满容量/超配/读数口径） | [`pool`](#pool--poolbase--threadpool--objectpool) |
| pooled_thread_pool | `pool` | `ObjectPool` × `ThreadPool` 互喂组合件（借空降级永不丢任务 + 可选 CallerRuns 档） | [`pool`](#pool--poolbase--threadpool--objectpool) |
| objpool | `pool` | `ObjectPool` 通用对象池：池化**有状态**对象（reset 钩子/有界降级/分片槽位） | [`pool`](#pool--poolbase--threadpool--objectpool) |
| memgov | `memgov`（pool 连带） | 计数分配器/有界注册表/泄漏断言 + `MemoryPool` 特化 + `ByteBudget`（仅 idle 常驻字节）+ 资源上限四问 | [`memgov`](#memgov--内存治理) |
| text | 常驻 | UTF-8 解码/字符截断/case 转换/编辑距离 | [`text`](#text--文本处理) |
| backoff | 常驻 | 确定性线性退避计算 | [`backoff`](#backoff--确定性退避) |
| retry | 常驻 | 同步重试编排（政策显式/预算闸/耗尽回灌） | [`retry`](#retry--同步重试编排) |
| paths | 常驻 | 路径归一/相对化/containment + 家目录/XDG 基目录 | [`paths`](#paths--路径归一与基目录) |
| clock | 常驻 | 时长格式化 + RFC 3339 双向（时间源契约在 `skeleton::clock`） | [`clock`](#clock--时间源) |
| scheduler | 常驻 | 同步任务调度器（after/every，tick 确定性内核） | [`scheduler`](#scheduler--同步调度器) |
| shutdown | 常驻 | 跨组件优雅停机编排器 | [`shutdown`](#shutdown--停机编排) |
| ratelimit | 常驻 | 令牌桶限流（突发+恒速回填） | [`ratelimit`](#ratelimit--令牌桶) |
| cache | 常驻 | LRU+TTL 缓存（O(1) arena 双向链） | [`cache`](#cache--lruttl-缓存) |
| fsx | 常驻 | 文件系统受控子集（walk/copy/rm/tempdir/atomic_write） | [`fsx`](#fsx--文件系统受控子集) |
| encode | 常驻 | hex/base64/BOM/UTF-16 编解码 | [`encode`](#encode--编解码) |
| id | 常驻 | UUIDv7 确定性变体时间有序 id | [`id`](#id--时间有序-id) |
| iter | 常驻 | 迭代器扩展精选（itertools 语义子集 5 件） | [`iter`](#iter--迭代器扩展) |
| config | `config-json`/`config-toml` | 配置文件分层装配 + 原子保存 + TOML 翻译（契约在 `skeleton::config`） | [`config`](#config--配置装配) |
| hash | 常驻 | 冻结稳定摘要（FNV-1a 64 / XXH64） | [`hash`](#hash--稳定摘要) |
| json | `json`（std-ext 侧门控） | JSON 双模解码（RFC 8259 严格 / 注释容忍）+ 64MiB 输入上限 | [`json`](#json--json-双模解码) |
| fx | `tokio` | `FxScope` 异步并发托管（唯一合法 spawn 口） | [`fx`](#fx--异步并发托管) |
| journal | `journal` | `JsonlJournal`：JSONL 追加式真相源 | [`journal`](#journal--追加式账本) |
| testing | `testing` | `TestStore`/`ScriptedRuntime` + 解析面 mock | [`testing`](#testing--测试替身) |
| log | `log`（含 `log-fs`） | `logx`：门面绑定（init 系/双 sink）；`log-fs` 单开=密钥掩码/路径解析/文件落盘 | [`logx`](#logx--日志门面) |
| tracing | `tracing` | `tracingx`：审计事件流 → tracing 桥 | [`tracingx`](#tracingx--tracing-桥) |
| compose | `compose` | `combine`/`combine!` 父子分片组合器 | [`compose`](#compose--父子分片组合器) |
| view | `view` | `ViewBridge` 语言无关视图桥（投影/意图/双编码矩阵/水位/未知标签跳过） | [`view`](#view--语言无关视图桥) |
| log-embed | `log-embed` | 九域 55 埋点规范（target/级别/字段纪律） | [`log-embed`](#log-embed--埋点规范) |

---

## runtime — SyncStore 契约参考实现

**feature**：`runtime`（默认）。**依赖面**：仅 `skeleton` 契约 crate（std-only，域面零三方——crate 树见 [§零依赖口径](#零依赖口径)）。

`SyncStore<S, A, Op = Infallible>` 兑现 `skeleton::Store` 契约：单写者队列 + 同步排空环（**dispatch 返回 = 本 action 的完成边界**——状态提交 → 效果转发 → 通知返回，且其全部**因果后代**终局；并发生产者下队列可能仍非空，同序 action 序列恒同终态）+ CoW 快照广播 + 停机闸 + 回灌深度预算 + **有界准入**（issue#38）+ **dispatch 回执**（issue#40）。`Op` 缺省 `Infallible` → `Effect::Run` 不可构造，零效果程序编译期证明无副作用（配 `skeleton::NoFx`）。

> **`Deferred` 例外（issue#57）**：void `dispatch` 的「完成边界」**不**证明**完整因果完成**——若某个因果后代被回灌深度预算**停放（deferred）**，`dispatch` 仍正常返回，但该子树并未完成。需要知道「完成 / 停放 / 容量拒收」用 `dispatch_with_receipt(action) -> Result<DispatchReceipt, AdmissionOutcome>`：`wait()` 返回 `DispatchOutcome::{Completed, Deferred{retained}, CapacityRejected{retained, rejected}, Failed}`。停放记录用 `resume_deferred(limit)` 显式恢复（#49）；恢复出的工作拿**独立续接收据**，**不改写**原根的 `Deferred` 终局。⚠️ 该可等待面**禁止在排空回调内等待**（自死锁，代码侧有 fail-visible 断言）。

> **宿主意务（跨线程阻塞）**：外部 `dispatch` **阻塞**至自己的完成边界，而 drain 回调（`EffectRuntime::run`、订阅回调）**运行在排空环内**。宿主若在 drain 回调里 `join` 一个**用阻塞式 `dispatch` 回调同一 store** 的线程 ⇒ **确定性互等死锁**（排空环被 join 卡住，回执只由排空环兑现）。**安全写法**：回调派生的 worker 用**框架提供的** sender（`Store::sender()` 或回调收到的 `back`）的 `dispatch`——**在 drain 回调内该 sender 必处竞争态**（排空权在排空线程）⇒ **纯准入、不等回执**（返回 `true` 只表示**被接受**，不表示已完成）⇒ 回调可以 join 该 worker（worker 不等回执，不与排空环互等），排队的 action 在**回调返回后**按 FIFO 处理。若 worker 确实需要该 action 的**完成结果**才能返回，则它**不得**被跑在同一 store 排空环上的回调同步 join。**自建** sender（`ActionSender::new`）的语义由实现方决定，**不可假定**框架提供的 sender 的任一条路径语义。框架**无法**检测宿主侧的跨线程 join ⇒ **不提供**框架侧防护或超时（义务与规避路径在宿主侧）。可编译示例见 `skeleton_infra::runtime::SyncStore::new` 的文档示例；正向回归场景见 `skeleton-infra/tests/dispatch_receipts.rs`；负向复现（六场景矩阵）见 `verify/2026-11-20-residual-join-risk/`。
>
> **框架提供的 sender 的语义是条件性的（两条路径，`bool` 无二义）**：**竞争态**（调用瞬间已有在任排空者）= **纯准入**——框架**只保证**该 action 已入队、**不等**它的完成边界；**返回瞬间该 action 是否已被 reduce，框架不作任何保证**（取决于在任排空者的推进速度：可能尚未被 reduce，也可能已被 reduce 甚至已完成）⇒ 下游**不得**据「竞争态」推断「此刻读状态仍是旧值」。**静默态**（调用瞬间无在任排空者）= 该线程**夺得排空权并同步排空至 idle** ⇒ 返回时该 action **已被 reduce、其完成边界必然已达成**（此路径**强于** `Store::dispatch` 的 per-action 完成边界契约——后者**不**承诺队列为空）。**返回的 `bool` 只承载准入结果**（被 store 接受/已入队 = `true`，被拒 = `false`），**不承载完成状态**：竞争态下是否已被 reduce、静默态下是否已达成完成边界，均**不可**由 `bool` 推断。机械闸（两态各一断言，确定性握手 + 有界超时）：`skeleton-infra/tests/drain_regressions.rs::framework_sender_admission_is_conditional_on_drain_state`；**反向闸**（钉住「不保证」而非「恒未 reduce」——竞争态下该 action 最终被 reduce 且**恰好一次**，**刻意不断言**「返回瞬间是否已被 reduce」）：`skeleton-infra/tests/drain_regressions.rs::competitive_state_does_not_guarantee_reduction_timing`。

### 类型与签名

```rust
pub struct SyncStore<S, A, Op = Infallible> { /* Arc 内脏 */ }

impl SyncStore<S, A, Op>
where S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static
{
    pub fn new(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
    ) -> Self;

    pub fn restore_from(
        root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static,
        fx: impl EffectRuntime<A, Op> + Send + 'static,
        restore: Restore<A, S>,
    ) -> Self;

    pub fn subscribe_local(&self, f: impl FnMut(&S) + 'static) -> LocalSubscription;
    pub fn subscriber_count(&self) -> usize;
    pub fn rejected_after_close(&self) -> u64;
    pub fn pending_actions(&self) -> usize;
    pub fn subscriber_panics(&self) -> u64;
    pub fn subscriber_panic_last_cause(&self) -> Option<String>;
    pub fn subscriptions_after_close(&self) -> u64;
    pub fn local_missed_notifications(&self) -> u64;
    pub fn audit_preempted(&self) -> u64;
    /// issue#62：**仅 opt-in 且视界完整、无驱逐**时才计数的精确 miss；默认关闭恒 0，
    /// 驱逐/饱和后停止增长（语义显式化，不静默改义）。
    pub fn cancel_unknown_id(&self) -> u64;
    pub fn runtime_panic_count(&self) -> u64;
    pub fn reducer_panic_count(&self) -> u64;
    pub fn feedback_budget_exceeded(&self) -> u64;

    // ── issue#62：历史 Run-ID 诊断（默认关闭，窄口径 opt-in）─────────────
    pub const DEFAULT_RUN_ID_TRACKING_CAPACITY: usize;   // 数值已拍板 keep current（2026-11-23），正典见本文件资源表决定记录
    /// 启用有界最近 ID 集 + 三态分类（`&mut self`：构造期/装配期一次性设置）。
    /// 关闭路径零 per-Run 锁与分配（门闸前仅一次 `OnceLock` load）。
    pub fn enable_run_id_diagnostics(&mut self, capacity: usize);
    /// 三态：`Known` / `DefinitelyUnknown`（仅视界内）/ `NotTracked`（未启用/饱和/已驱逐）。
    pub fn classify_run_id(&self, id: EffectId) -> RunIdClass;
    pub fn run_id_tracked_count(&self) -> usize;
    pub fn run_id_tracking_capacity(&self) -> usize;
    pub fn run_id_evicted_count(&self) -> u64;
    pub fn subscribe_events(&self, capacity: usize,
        on_event: impl FnMut(&AuditEvent) + Send + 'static) -> AuditTicket;
    pub fn take_events(&self) -> Vec<AuditEvent>;

    // ── issue#38 有界准入 ─────────────────────────────────────────────
    /// 准入容量上限：**等待被 reduce 的 action 数**（执行中不计入；同栈回灌
    /// 豁免）。满时 `try_dispatch` typed 拒绝、**不阻塞**（阻塞会与回调重入死锁）。
    pub fn set_capacity(&self, cap: usize);
    /// 有界准入判定：`Accepted` / `Full` / `Closed`。
    pub fn try_dispatch(&self, action: A) -> AdmissionOutcome;
    /// 因容量上界被拒的累计次数（闸关拒发不计入）。
    pub fn admission_full_count(&self) -> u64;
}

/// 有界准入结果（issue#38）：`Accepted` = 已入队；`Full` = 容量满（**未入队**，
/// 不部分插入）；`Closed` = 已闭门（**未入队**）。**非** `#[non_exhaustive]`
/// ⇒ 跨 crate 可穷尽 match（漏变体即编译错）。
pub enum AdmissionOutcome {
    Accepted,
    Full,
    Closed,
}

// skeleton::Store trait 兑现面：
//   fn dispatch(&self, action: A);
//   fn snapshot(&self) -> Arc<S>;
//   fn subscribe(&self, f: impl FnMut(&S) + Send + 'static) -> Subscription<S>;
//   fn sender(&self) -> ActionSender<A>;
//   fn close(&self);
//   fn is_closed(&self) -> bool;
```

装配点：`root` 是**根** Reducer（`initial()` 产出完整根状态）；`fx` 是效果解释器（核心只转发，永不解释）。`combine` 产物是状态分片不是根——分片作根即 E0277 编译期拒绝（`Fragment` ≠ `Reducer`，错误信息经 `on_unimplemented` 自带修复指引）。界不对称（有意）：根 Reducer 要 `Send + Sync`，解释器只要求 `Send`（锁后独占访问，多要 Sync 是白缴的税）。

`restore_from` 播种语义：`Restore::Empty` = `initial()` 零 apply（即 `new`）；`FromInitial(actions)` = 自 initial 起步全量 dispatch 重放；`FromSnapshot(state, actions)` = **快照态直入（不经 initial）**+增量重放。重放走 `dispatch` 而非裸 reduce 是契约：`Effect::Emit` 回灌等派生动作由排空环再生，终态与 live 收敛。

### 闭门后的入口对照（停机闸）

| 入口 | 闭门后行为 | fail-visible 面 |
|---|---|---|
| `dispatch` | 恒 no-op（签名不引入 Err） | 计入 `rejected_after_close` |
| `ActionSender::dispatch` | 返回 `false`（`#[must_use]` 兜底） | 同一计数 |
| `subscribe` / `subscribe_local` | 照常登记、票据照常注销，但永无触发——通知静默（已知取舍；需要显式拒绝面时用 `try_subscribe`，闭门后返回 `Err(StoreClosed)`） | 计入 `subscriptions_after_close`；`subscriber_count` 可交叉查证 |

**停机等待（#64）**：`close()` 是 **best-effort**——静默态（本调用夺得排空租约）同步排至 idle；**竞争态**（他线程已持租约）只完成交接即返，返回时排空可能仍在进行。要「等静止」用 `close_wait()` / `close_wait_timeout(d)`，结果 `Drained` / `TimedOutStillRunning` / `Reentrant`（同栈重入不自等）。谓词 = 租约释放 + 活跃队列空，**不计 deferred 停放记录**（#49 口径）；timeout 只界等待、不取消同步 reducer。宿主要真屏障（flush/drop 依赖前）用此等待面，勿依赖 `close()` 的返回时机。

**完成账本的复用边界（#84）**：账本进复用池**只在终局发布（`root_done && outstanding == 0`）且指定 owner 让渡之后**——非等待型根（`try_dispatch` / `ActionSender::dispatch`）的 owner 是它的**因果链**：`finish_root` / 最后一个 `descendant_done` / `detach_deferred` 三个转移点各尝试一次 claim（恰好一次入池）；等待型根的 owner 是**外部等待者**，它在 `wait()` 克隆并返回 `Outcome` 之后才归还。`Arc<Ledger>` 克隆可随在途后代存活到全部结算；`reset` 只在「终局 + `outstanding == 0` + 已 claim 归还」三不变式下执行（debug 构建断言兜底）。⇒ **不同根的收据记账永不交叉**（此前可提前入池并被 `reset` 擦掉在途后代计数）。

**终态报告与保留后缀（#61）**：`close()` **不是效果 flush**，**不承诺**终态 pending 效果快照。要「闭门 + 等静止 + 一次拿到终态报告」用 `SyncStore::close_wait_with_report(d)`——返回和类型，**只有 `Drained` 携带快照**：`Drained(CloseReport)` / `TimedOutStillRunning` / `Reentrant`（超时与重入**不带快照**，防调用方误当终态；`Reentrant` 时准入**已关**，闭门先于重入判定）。报告四个 population **分列、禁合并成一个 pending 计数**：`queued_actions`（`Drained` 恒 0）、`deferred_actions`（#49 停放记录，**不影响 `Drained` 判定**）、`pending_effects`（已提交未转发的效果后缀：总条数 + 段级身份 `origin_action_seq` / `next_effect_index` / 条数）、`interrupted_effect`（最近一次中断的归属：`dispatch_action_seq` / `origin_action_seq` / `effect_index` / 种类 / `EffectId`，与 #48 词汇同源）。报告**只读**——后缀在报告后**仍被 store 持有**、可重复报告检视；唯一解决面是 `discard_pending_effects()`（显式、返回精确条数与段身份；`None` = 未关闭或未静止，**什么都没丢**）。**close 与 discard 都不跑宿主 `EffectRuntime` 代码**（含 `Effect::Emit`：丢弃不回灌；载荷析构在锁外）。**不做 post-close 转发**：转发需要独立的 drain-to-idle continuation epoch 与回执归属，保留为独立规格；也**不提供** one-effect pop。与 #64 共用同一谓词与同一 Condvar（不复制等待机器），差别只有「闭门先于重入判定」与「`Drained` 后才快照」。

### 订阅双档

- `subscribe`（跨线程档）：回调需 `Send`，槽位 Mutex 串行化；返回 `Subscription<S>`（`#[must_use]`，Drop=注销即时摘表，无「一拍滞后」）。
- `subscribe_local`（单线程档）：回调免 `Send`（可用 Rc/RefCell），只在注册线程触发，免锁免原子量；返回 `LocalSubscription`（`#[must_use]`，`!Send + !Sync`——线程亲和的类型化表达）。同一 dispatch 内通知序：本地订阅先行，跨线程随后（确定性）。
- **登记与闭门同一把门**（#63）：`try_subscribe` 与 `close`（及 `close_wait`）共用一个**订阅门**（每 store 一把小锁，只在登记/闭门**冷路径**取，热 dispatch 路径零开销）⇒ `try_subscribe` 要么**线性化在 close 之前**并登记成功，要么**线性化在 close 之后**并返回 `Err(StoreClosed)`——**不会出现「已登记但 store 已关」**。legacy `subscribe` 语义不变：闭门后**照常登记**并计入 `subscriptions_after_close`（它不是可失败入口）。close 翻转 `closed` 后**放门再排空/回调**（不在持门期间跑用户回调、丢用户闭包或排空队列）。

- **审计面同理（#60）**：`AuditTicket` 的 Drop 在**持有审计 mutex 时**完成「摘槽 + `gate=false`」**一次**状态变更；`subscribe_events` 的装槽与开闸也在**同一把 mutex** 内 ⇒ 装槽与闸翻转串行，**陈旧票据绝不会禁用更新的 sink**，被禁的闸也**不会**压制环历史（发射在闸 false 时于环入账前返回）。热发射路径仍是一次 Acquire gate load 即返。#65/#75 的「有效代次内的实时投递竞态」是**另一条**边界，其 FIFO 溢出走独立投递丢弃计数（与 ring 溢出分账）。
- **去活后不再开始新投递/新回调**（#73/#74）：订阅槽持 **atomic alive 标志**（发布前初始化）；票据 Drop 与 panic 摘除**先原子失活**（幂等）再解链 + `subs_version` 递增。异步投递在**收集窗过滤已死槽**并在**每次 publish 前复查 alive**；同步订阅在**每次调用缓存回调前复查 active** ⇒ 缓存快照虽持强 `Arc`，也不能在失活后启动新回调。**窄契约**：通过了最后一次检查的投递/回调视为 **in-flight**，允许在票据 Drop 返回后完成（严格 quiescence 需另立阻塞式协议，本框架不提供）。

### 可观测计数面（fail-visible，对齐 `subscriber_panics` 先例）

| 读数 | 何时的静默面 | 形态 |
|---|---|---|
| `try_subscribe` | 闭门后登记成功但永不触发 | 显式 `Err(StoreClosed)`（旧 `subscribe` 保留，闭门登记仍计入 `subscriptions_after_close`） |
| `local_missed_notifications` | 跨线程 dispatch 时本地订阅零触达 | 累计计数（读侧；命中路径零分支零开销） |
| `audit_preempted` | 审计事件流二次订阅静默覆盖前订 | 累计计数（`subscribe_events` 覆盖旧槽前留痕） |
| `cancel_unknown_id` | `Effect::Cancel` 指向从未发出的 `Run` id | 累计计数，但**语义已显式化（issue#62）**：仅当 opt-in 启用诊断且视界完整、无驱逐时才是精确 miss；**默认关闭恒 0**，饱和/驱逐后**停止增长**（此后未知分类走三态 `NotTracked`，绝不判 `DefinitelyUnknown`）。转发路径不依赖该诊断。 |
| `Combine::route_misses` | `combine` 路由 miss 静默吞 action | 累计计数（分片装配一次持有，读侧无锁无原子量之外成本） |

### Panic 契约

- `S::Clone` 必须 panic-free：外部持有快照时状态落位走 `Arc::make_mut` 克隆分支，clone 在 state 锁内 panic 即毒锁——**实例整体报废且状态不可取出**（文档化显式契约）。
- reducer panic 不报废实例：计数（`reducer_panic_count`）+审计现场（`AuditEventKind::ReducerInterrupted`）后原样重抛；action 已出队随栈展开丢失，状态不前进。
- 解释器 panic 不报废实例：panic 原样从 `dispatch` 上浮（fail-visible），毒锁在下一次效果转发时恢复 + 计数（`runtime_panic_count`）——读数口径与首轮 panic 的细节见下文「effect 执行两档」节。
- 订阅回调 panic：fail-closed 摘除 + `subscriber_panics` 计数 + `subscriber_panic_last_cause` 成因摘要。
- **`A::Drop`（action 载荷）在展开清理期必被调用（#66）**：排空环 panic 时 `DrainGuard` 与 `SyncStore` 自身的析构都按值丢弃残留载荷 ⇒ 该 `Drop` **必须 panic-free**（二次 panic = abort）且**不得重入同一 store**（排空旗已放，重入会夺得新租约并在展开栈上跑排空环）；只读面（`snapshot` / `pending_actions` / `subscriber_count`）安全。正常运行期 `A` 析构同样在所有内部锁之外，**无**额外 panic-free 要求。正文见 `skeleton::Store` 语义契约 7 与 `SyncStore`「Action 析构义务」，回归见 `tests/drop_contract_unwind.rs`。

### 回灌深度预算

`FEEDBACK_DEPTH_BUDGET = 64`：单任排空内连续回灌链深度超限即停排 + `feedback_budget_exceeded` 计数；**触发点回灌 action 被脱钩停放（deferred）——主队列继续处理其他工作，停放记录保留其来源身份**，等宿主显式恢复（`resume_deferred(limit)`，#49）或以 `DispatchOutcome::Deferred` 形式如实上报——非 panic 非静默，**不是**「退回队尾自动重放」。预算按 dispatch 调用栈深度统一计数（TLS 在途深度，back 通道不绕预算）。时间线现场 = `AuditEventKind::FeedbackBudgetExceeded`。

### effect 执行两档（同步档 / 托管档）

蓝图 §11 项 6：ADR 0008 在**通知面**落地了「两档并存 + 同一语义契约」，同一模式在**效果执行面**沿用——不强迫一档替掉另一档，也不新增第二个入口。

**现状（源码坐标：落盘于 2026-11-17 的快照值，本轮 #34/#35/#39/#40/#41/#42/#44 改动后**行号可能已漂**——符号名恒准，定位请按符号名）**：转发环在排空环内、`dispatch` 调用栈上**同步**调用解释器——`runtime.rs:1486`（`forward_effects` 调用点，位于 `drain_owned` 循环体内）→ `runtime.rs:1529`（`forward_effects`）→ `runtime.rs:1587`（`runtime.run(effect, &inner.back)`）；`Effect::Emit` 不经解释器直接回灌入队（`runtime.rs:1536`）。异步**不在核心内**：托管由 `fx::FxScope::spawn_tracked`（`fx.rs:417`）提供，解释器实现方在 `run` 里 spawn 并把 `back` 句柄 move 进任务。

| 档 | 谁执行 | 适用条件 | 失败/取消可观察面 |
|---|---|---|---|
| **同步档**（内联） | 排空环转发段，在 `dispatch` 调用方线程内联执行 | 有界、非阻塞、不等待外部完成的工作；**禁 `await`、禁阻塞 IO**（内联耗时直接计入 dispatch 延迟，并阻塞整个状态机） | 解释器 `run` panic：`catch_unwind` 只观察不吞（审计 `EffectInterrupted`，`runtime.rs:1587-1589`）后 `resume_unwind`——**panic 原样从 `dispatch` 上浮**（`runtime.rs:1590`）。实例不报废：毒锁在下一次效果转发时 `into_inner` 恢复 + `clear_poison` + `runtime_panic_count` +1（`runtime.rs:1574-1580`）；读数口径 = **毒位被观察到时才计**（`runtime.rs:1141-1144`），首轮 panic 后不再产效果则读 0。panic 的那条效果随转发丢弃、不再重投（重投会恒 panic 死循环）；同批未转发的效果原地留缓冲，由下一任排空续转（`runtime.rs:1529-1536` 逐条 `pop_front` 摘除形）。 |
| **托管档**（异步） | `FxScope` 生命周期集内的 tokio 任务 | IO / 长任务 / 需与核心解耦的等待 | 完成/失败/取消经 `Spawned::join` → `TaskError::{Cancelled, Panicked, TimedOut}`（`fx.rs:577-591`）与停机排空报告（`fx.rs:251-303`）分桶可见：`completed` / `cancelled` / `panicked` / `failed`（+`errors` 逐条）/ `still_running` 终局桶互斥穷尽，`timed_out` 是与终局桶正交的截止事件计数。句柄被丢也不丢载荷：panic 消息进生命周期集，`join` 与停机排空双重可见（诊断面非 exactly-once）。 |

**边界规则（四条，两档共用）**：

1. **核心永不阻塞等待异步完成**（蓝图 §3 原句）：`run` 是同步 trait 方法（`skeleton/src/lib.rs:354-358`），排空环调用它时既不 `await` 也没有「等句柄」路径——托管档的 `run` 只 `spawn_tracked` 入集（`fx.rs:417`；闸关返回 `Err(ScopeClosed)`，`fx.rs:429-432`，fail-closed 禁静默丢弃 future）后立即返回。**`dispatch` 返回 ≠ 效果完成**：这是托管档的文档化时序，不是缺陷。
2. **异步结果回灌必须经 `ActionSender`**（显式消息边界）：任务完成后调 `back.dispatch(action)` 把结果变成普通 action 重入队列，状态仍只由 reducer 推进。跨线程回灌不是 dispatch 调用栈的一部分（按外部流量计，`reentrant_origin` 判定见 `runtime.rs:63`、深度归零见 `runtime.rs:1395`）——**跨线程回灌环的环防护责任在解释器/宿主自身**（契约注见 `skeleton::EffectRuntime`「回灌环责任边界」）。闭门后回灌显式返回 `false`（不静默）。
3. **同步档内禁 `await`/禁阻塞 IO**：`run` 在排空环内、持排空权执行，阻塞它 = 阻塞整个状态机（后续 action 与回灌链全部排队）。需要等待就换托管档，而不是在同步档里「小睡一下」。
4. **禁 join 会回调同一 store 的线程**（跨线程形态，2026-11-20 确定性复现）：`run`（或订阅回调）内 join 一个用**阻塞式** `dispatch` 回调**同一 store** 的线程 ⇒ **确定性互等死锁**——排空环被 join 卡住，而回执只由排空环兑现。规避：派生工作走**框架提供的** sender（`Store::sender()` 或回调收到的 `back`）的 `dispatch`——**回调内该 sender 必处竞争态**（排空权在排空线程）⇒ 纯准入、不等回执（其 `bool` = **被接受**而非完成）——或不 join；同线程重入仅入队不等待（不互等）。**自建** sender（`ActionSender::new`）的语义由实现方决定，**不可假定**框架提供的 sender 的任一条路径语义。**框架侧无防护**（无法检测宿主侧的跨线程 join）⇒ 义务与规避路径都在宿主侧。复现工程与场景矩阵：`verify/2026-11-20-residual-join-risk/`（6 场景：3 死锁 / 3 不死锁）。

**与 ADR 0008 的承继关系**（同一「两档并存 + 同一语义」在两个面上的对照）：

| 语义位 | 通知面（ADR 0008，已实现） | 效果执行面（本节） |
|---|---|---|
| 同步档 | `subscribe` / `subscribe_local`：回调在 dispatch 线程内联执行 | 内联 `run`：在排空环转发段执行（`runtime.rs:1486` → `:1587`） |
| 异步档 | `NotifySink::publish`：O(1) 版本投递，重活移出环（`skeleton/src/notify.rs:62`） | `FxScope::spawn_tracked`：工作移出环，结果经 `ActionSender` 回灌（`fx.rs:417`） |
| 载荷/契约 | 单调版本号（状态化），消费侧按需拉快照 | 仍是 `Action`（纯 reducer 推进状态） |
| 不救另一档 | 慢同步订阅者的耗时照常计入 dispatch | 同步档 `run` 的耗时照常计入 dispatch |
| 失败可见 | 投递口 panic = 捕获 + 摘除 + 计数 | 解释器 panic 上浮 + 计数；托管任务失败进 `TaskError` / `ShutdownReport` |
| 关闭语义 | 闭门后登记照常、永不触发（计数显形） | 闭门后 spawn = `Err(ScopeClosed)`；回灌返回 `false`（计数显形） |

行为面：`tests/effect_two_tier.rs`（两档各自时序 + 失败面 + 终态一致，5 条）。**无统一入口**（本单评估结论）：两档不是两个 API，而是同一个 `EffectRuntime` 契约的两种实现形态；造统一入口会逼核心替解释器选执行器，违反蓝图 §6「Do not add a new executor abstraction that silently moves arbitrary work between sync and async contexts」。

### audit — 审计事件值面

```rust
pub struct AuditEvent { pub seq: u64, pub kind: AuditEventKind }

pub enum AuditEventKind {
    Enqueued { feedback_depth: u64 },
    Reduced,
    ReducerInterrupted,
    EffectForwarded,
    EffectInterrupted {
        dispatch_action_seq: u64,
        origin_action_seq: u64,
        effect_index: u64,
        effect_kind: skeleton::EffectKind,
        effect_id: Option<skeleton::EffectId>,
    },
    Notified,
    Rejected,
    FeedbackBudgetExceeded,
}

#[must_use = "审计票据被丢弃即立即关闭事件流……"] pub struct AuditTicket { /* Drop = 关流 */ }
```

⚠️ **迁移**：`EffectInterrupted` 由单元变体改为结构变体 ⇒ 下游 `match` 该变体的分支须改为**解构形**（`AuditEventKind::EffectInterrupted { .. }`）；原先按**等值比较**该变体的写法（如 `e.kind == AuditEventKind::EffectInterrupted`）须改为 `matches!(e.kind, AuditEventKind::EffectInterrupted { .. })`；**派生 `PartialEq` 的语义随之变化**（该变体现在逐字段比较，不再与自身恒等）。

**`EffectInterrupted` 的五个字段**（GitHub issue#48）：解释器 panic 时被中断的那条效果的**结构化身份**——消费方据此定位「**哪一个**效果处于 indeterminate 态（外部结果不确定）」。

| 字段 | 含义 |
|---|---|
| `dispatch_action_seq` | **本次触发转发、并承接失败回执**的 action 的**入队序号**（store 内单调；**外部入队**路径的序号在队列锁内分配，**`Effect::Emit` 回灌**路径的序号在取锁**之前**分配 ⇒ 序号序与全局 FIFO 序**不保证**恒等——见下方「已知张力」） |
| `origin_action_seq` | **实际产出该效果**的 action 的入队序号——可能**早于** `dispatch_action_seq`（pending 效果跨 action 存活：A 产出的后缀在 A 的转发 panic 后留存，后来由 B 的 dispatch 触发续转并再次 panic ⇒ 来源是 A、触发者是 B） |
| `effect_index` | 该效果在**产出它的 action** 的 reducer 输出序列中的 **0-based** 位置（计数**包含** `Emit`——Emit 也占位）——它定位的是「该 action 产出的**第几条**效果」，与效果种类无关（`EffectId` 在一个 store 内**全局唯一**，见 `skeleton::EffectId` 的唯一性纪律） |
| `effect_kind` | 效果的解释器侧种类（`skeleton::EffectKind`：`Run` / `Cancel`） |
| `effect_id` | 效果的**逻辑句柄**（`Option<skeleton::EffectId>`）——当前 `Run`/`Cancel` 恒 `Some`；未来无句柄的种类为 `None`。⚠️ **语义随种类而变**：`Run` 的 `effect_id` 是**该效果自己的**句柄；`Cancel` 的 `effect_id` 是**被取消目标**的句柄（Cancel 效果自身没有句柄）⇒ 因此 `effect_id` **不可**单独当作「效果身份」用——同一 store 内 `Run(id=N)` 与 `Cancel(id=N)` 会发出**相同**的 `effect_id`；两条指向同一目标的 `Cancel` 效果也会发出**相同**的 `effect_id`（合法时序，非误用）。**唯一标识一个效果的作用域**：在**同一个产出 action 内**，`(effect_kind, effect_id, effect_index)` 三元组唯一标识一个效果；**跨 action** 时唯一标识一个效果的是 **`(origin_action_seq, effect_index)`**（= 哪个 action 产出的第几条）——`effect_index` 是**产出 action 内**的序号，不跨 action 唯一 |

**边界**：⚠️ 两个序号都是**入队序号**，**不是**「dispatch 调用发生的时刻序」——外部调用方看到的是「我这次调用排在队列第几位」，并发下与调用时刻序不同。同一次 action 内两者相等；跨 action 续转时**通常** `origin < dispatch`（见下方「已知张力」）。事件**不携带** `Op` / Action / payload（可能很大、不可 Clone、可能含敏感业务数据）。`Effect::Emit` 不经解释器 ⇒ 不产生本事件。

**已知张力（2026-11-20 第三轮对抗性审计登记）**：两个序号的**分配纪律不一致**——外部入队路径的序号在 `queue` 锁内分配（与入队同临界区），而 **`Effect::Emit` 回灌**路径的序号在**取锁之前**分配 ⇒ 理论上可能与并发入队的序号**交错**（外部 action 可能拿到更大序号却先入队）⇒ **不保证**全局 FIFO 序 = 序号序，跨 action 续转时的 `origin < dispatch` 亦**不保证**恒成立。**实测状态**：结构可证、经验未复现——探针（Emit 回灌 × 外部入队并发编排，20 秒 / 约 118 万外部轮次）实测 `origin_action_seq > dispatch_action_seq` 的事件数为 **0** ⇒ 窗口极窄，**未**实证该不变量被破坏。⇒ 登记为**已知张力**（登记缺口，非已证实缺陷），**本轮不改实现**（统一纪律需把 Emit 回灌路径的序号分配移入 `queue` 锁内 ⇒ 须先评估锁序与 `parent.add_descendant()` 的锁序约束，另行立项）。

**配套公共面（`skeleton` crate）**：

```rust
pub enum EffectKind { Run, Cancel }              // #[non_exhaustive]

impl EffectKind {
    /// 效果的解释器侧身份：`(种类, 逻辑句柄)`；`Emit` ⇒ `None`。
    pub fn of<A, Op>(effect: &Effect<A, Op>) -> Option<(EffectKind, Option<EffectId>)>;
}
```

`EffectKind::of` 是**效果 → (种类, 句柄) 的单一映射点**：匹配在本 crate 内对 `Effect` 词表**穷尽** ⇒ 词表演进（新增效果种类）时编译期强制同步，跨 crate 的下游免于各自补通配语义。

⚠️ **演进约束（已知限制，2026-11-20 第三轮对抗性审计登记）**：`of` 的返回类型是 `Option<(EffectKind, Option<EffectId>)>`——**双层 `Option`**：外层 = 「非解释器可见」（今仅 `Emit`），内层 = 「无逻辑句柄」。`of` 的穷尽匹配保证的是「**覆盖 `Effect` 词表的所有变体**」，**不**保证「所有非 `Emit` 变体都必经解释器」⇒ 若将来新增一个**不经解释器**的效果种类并让 `of` 对它返回 `None`，实现侧那条「非 `Emit` 效果必经解释器」的 `expect` 不变式将**在运行时 panic**（编译器**不会**报错——`of` 的编译期强制同步只强制**改 `of`**，不强制**改成 `Some`**）。⇒ **新增不经解释器的效果种类时，实现侧必须同步处理 `of` 返回 `None` 的分支，不得依赖 `expect`**（类型级解法：拆成 `kind_of` / `handle_of` 两个函数，或给 `Effect` 加「解释器可见」的类型级标记——**本轮只登记，不改实现**）。

**`EffectId` ≠ 投递身份**：`EffectId` 是 reducer/消费者定义的**逻辑效果句柄**（**一个 store 内全局唯一**——见 `skeleton::EffectId` 的唯一性纪律），**不是**框架生成的投递尝试身份。本批**不引入重试**、**不承诺** exactly-once；#45 拍板的 Option D 语义（至多一次调用尝试 · 外部结果不确定 · 不自动重放）**保持不变**，幂等/去重属消费方责任。

生命周期重建协议：记账单位是 **action 而非 dispatch**——一笔 dispatch 主 action 走 `Enqueued(depth) → Reduced → [EffectForwarded* | EffectInterrupted] → Notified?`；Emit 回灌派生的每个动作各走一条同形链交错嵌进主链时间线。`Notified` 仅当拍确有状态快照订阅者收到快照时记录。`seq` 在入环临界区内分配（⇒ 环序 = `seq` 序，并发发射下亦然；issue#65），订阅窗内单调**连续**——空洞 = 有界环溢出丢最旧的显式证据。**实时回调是 best-effort 观测面**（不承诺每事件必达，权威历史是环 + `seq`）——词汇与丢弃证据分类见下方「投递语义词汇（订阅面通用）」。默认关闭零开销：未订阅路径 = 每 emit 点一次 gate load 即返。多订阅者间逐字交错顺序不可预推；时间线断言用语义不变量（深度链/节点计数），勿钉逐字全局顺序。

### 投递语义词汇（订阅面通用）

四个订阅面（`SyncStore` 审计实时回调、状态订阅者、异步 `NotifySink`、池域审计流）共用同一套投递语义；词表在此定义一次，各面 rustdoc 引用本节，禁各自改写（口径漂移 = 采用方拿到两套保证）。

| 词 | 定义 |
|---|---|
| **权威历史** | 有界环 + 单调 `seq`（`SyncStore::take_events` / `PoolBase::take_audit_events` 取证面）。**完整性承诺只在历史侧**：环内事件按 `seq` 序可重放，`seq` 空洞 = 环溢出丢最旧的显式证据。 |
| **best-effort 观测面** | 实时回调（live callback）：在 emit 调用方线程内**同步**调用——慢回调拖慢 emit 调用方的时序（已知观测面张力）；并发发射窗内未取得回调权的事件、以及同线程重入的事件只入环不进回调。**不承诺**每事件必达、**不引入** backpressure；要完整性 ⇒ 消费方 drain 权威历史。**适用面**：状态订阅者与异步 `NotifySink` 等**未升级为串行派发**的面；`SyncStore` 审计实时回调与池域审计流**已升级为「串行派发」**（见上条）。 |
| **串行派发（不丢并发事件）** | 实时回调的**增强形态**（池域审计流 #75 / `SyncStore` 审计实时回调 #65 主路线①）：回调有**显式在途状态 + 待投递 FIFO** ⇒ 回调在途期间的并发发射事件**入队而非跳过**，在途者按 `seq` 序在所有内部锁之外补投；FIFO **有界**，溢出丢最旧并计入**独立的投递丢弃计数**（与 ring 溢出分账）；回调 panic 时 **fail-close 本代次**（排队事件不投递给替换订阅）。三条不变：`FnMut` 回调串行 / 禁锁内调用 / 禁同线程递归。**两类抑制分别文档化**：并发投递=入队不丢；**同 store 重入**=只记环、**不进待投递队列**（不递归调用同一回调）。 |
| **deactivation 线性化点** | 去活裁决点 = 摘除路径的 `alive` swap（`false`；票据 Drop 与 panic 摘除共用这一裁决与闸回落）。**该点之后不再准入新投递**（#73/#74 引用本定义）。 |
| **in-flight** | 已通过**最终 alive 检查**的已准入投递。线性化点之前准入的投递**可以完成**（可能在去活后、甚至票据 Drop 返回后仍在执行）⇒ 契约是「去活后无新投递开始，在途可完成」，**不是**静默保证；严格 quiescence（Drop 返回即无回调在跑）需另立阻塞式协议，本框架不提供。 |
| **丢弃证据（互不替代）** | ① **投递丢弃计数**：只数回调投递未发生的次数（观测面自己的账，不留 `seq` 痕迹）；② **ring 溢出**：历史侧丢最旧（容量因）；③ **seq 断裂**：ring 溢出的可观测形态。⇒ 回调漏投**不产生** `seq` 空洞；`seq` 空洞**只证**历史丢失，不证回调丢失；`take_events` 取走（drain）也不留空洞（空环 ≠ 历史丢失）。 |

**当前状态（2026-11-23）**：**`SyncStore` 审计实时回调与池域审计流均已升级为「串行派发」**——并发发射事件入队补投（`SyncStore` 侧 #65 主路线① / 池域 #75）、FIFO 有界、**独立的投递丢弃计数**（`SyncStore::audit_delivery_drops()` / `PoolBase::audit_delivery_drops()` / `ObjectPool::audit_delivery_drops()`，与 ring 溢出**分账**）、回调 panic **fail-close 本代次**（排队事件不投递给替换订阅）。权威历史 = 环 + `seq`（`seq` 在入环临界区内分配 ⇒ 环序 = `seq` 序）。状态订阅 / 异步 `NotifySink` 两面**仍为 best-effort 观测面**，其契约与计数以各自 rustdoc 为准，词表统一按本节。

**零分配与 panic 轴的机械锚（#60/#61 证据补强）**：未订阅路径的「一次 gate load、**零分配**」由 `tests/audit_no_subscriber_alloc.rs` 机械锚定（分配器仪器自检 + 冷启动/订阅态三条控制臂 + 弃票后回落 0；「**无锁**」仍为结构保证——分配器观测不到锁）。效果转发中断覆盖**首/中/末**三档（末档 = 中断恰在段末 ⇒ 空壳段被 `summarize_pending` 跳过），并由 `tests/close_report_contract.rs` 直接钉死「**被中断效果绝不重试**」（`close` → `close_wait_with_report` → `discard_pending_effects` 全程宿主 `run` 调用计数恒 == 1）。

**回调内 dispatch 的因果归属（issue#59）**：夺得排空租约的那次调用，在投递**首条 pop 记录**的 `Enqueued` 回调**之前**即建立临时同栈帧（装帧与闸判是**同一次** load ⇒ 不存在「判闸未订阅、随后并发订阅装上、回调却无帧」的窗口）。回调内的同 store dispatch 因此归为该首条记录的**因果后代**——共享其账本、回灌深度 +1、**只入队即返**（不等待外层排空；外层排空环在回调返回后覆盖它）。缺这一帧 = 嵌套 dispatch 被判外部根而 `draining` 已置 ⇒ 调用方在无界账本等待上**等自己 = 确定性死锁**。**因果父 = 正在投递其 `Enqueued` 回调的首条记录**，不一定是夺得排空权的外部调用点（回调先于该记录 reduce 投递，首条记录甚至可能不是调用者的 action）。首条记录自身的深度不因临时帧被重复计（嵌套 dispatch 深度 = 首条记录深度 + 1，环内 `Enqueued` 深度序列 = `[0, 1]`）。本规则**先于** #65 的待投递 FIFO 生效（事件序不变，FIFO 只改并发 emitter 间的补投顺序）。

### 最小示例

```rust
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;

struct Counter;
impl Reducer for Counter {
    type State = u32;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> u32 { 0 }
    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
}

let store = SyncStore::new(Counter, NoFx);
store.dispatch(2);
store.dispatch(3);
assert_eq!(*store.snapshot(), 5);
store.close(); // 闭门后 dispatch no-op、计入 rejected_after_close
```

### 实测锚（源码 doc 口径）

- dispatch 热路径锚：`examples/dispatch_profile.rs`（end_to_end_dispatch ≤100ns 契约）；订阅态 dispatch 全含 ~142ns / 无订阅 ~77ns（2026-10-16 批 C）。
- `subscribe_events` 未订阅 emit 面：一次 Acquire load，零分配零锁（机制断言，audit 域 doc）。

**锚口径（诚实化）**：上述 ns 级 dispatch 锚**仅在小 State 下成立**——锚的测量对象是状态迁移本身的固定开销，不含状态体量的复制成本。`Arc<State>` 是 CoW 快照：外部持有快照时（强计数 ≥2），状态落位走 `Arc::make_mut` 克隆分支，每次 dispatch 额外付一次 `S::clone`（在 state 锁内执行）。审计实测（架构审计（本地留存，未入库）F-05）：

| 口径 | 实测 |
|---|---|
| 克隆计数 | 每 100 次「读快照 + dispatch」= 200 次 State 克隆；每 100 次纯 dispatch = 100 次 |
| 深克隆代价 | 1 MiB State（`Vec<[u64;32]>` × 4096）约 210µs/dispatch（release）——与 ~80ns 锚差约 2600 倍 |
| `Arc` 包裹 State 是否解题 | **不解题**：Arc 包裹与深克隆 State 均为 ~205-210µs/dispatch（比率 1.0x）——reducer 内部照样克隆整个载荷 |

采用方指引：

1. **State 体量**：建议控制在数 KiB 级；大状态用分片 + 自选持久化结构（`compose` 域的父子分片即此路径）。
2. **持有快照的代价**：长期持有 `snapshot()` 会让后续每次 dispatch 都付一次 State 深克隆（成本随 State 体量线性增长）——快照是「一致性视图」，不是「廉价引用」。
3. **`Arc<Inner>` 模式**：把大载荷放进 `Arc<Inner>`、State 只持 Arc 句柄——`S::clone` 退化为 Arc 计数克隆。但**仅此不够**：审计实测「Arc 包裹 State」与深克隆 State 比率 1.0x，因为 reducer 内部仍整体克隆载荷；此模式要真正省下深拷贝，reducer 侧须经 `Arc::make_mut` 之类 CoW 路径只克隆被改动的部分（**该用法本仓未实测**，属采用方侧选择，框架不代为保证）。
4. **`S::clone` 必须 panic-free**：clone 在 state 锁内执行，panic 即毒锁——实例整体报废且状态不可取出（见上文「Panic 契约」）。

#### 状态布局建议（issue#53 证据，bench 可复现）

分片（每片独立 `Arc` + reducer 内 `Arc::make_mut` 只改受影响片）**不是**默认答案，是**按条件换收益**的取舍。bench 矩阵：`cargo bench -p udf-skeleton-infra --all-features --bench infra -- state_layout`（三组：`state_layout_update` / `state_layout_update_scope` / `state_layout_retention`）。

| 形状 | 结论（本机读数；同机跨 run 噪声 ±20-40% 属常态） |
|---|---|
| 大状态 + 只改一小块 + 有读者持快照 | 分片大幅占优：1 MiB 选择性更新 `212 µs`（整体）vs `2.30 µs`（16 片），16 MiB `3.28 ms` vs `214 µs` |
| **小状态（≈1 KiB）** | **反例**：分片反而慢 2.0×（16 片）／8.0×（256 片改一片）——逐片 `Arc` 计数压过拷贝本身 |
| **更新要触碰全部分片** | **反例**：1 MiB 全片更新的优势从 90× 塌到 3×（字节工作量相同，分片只多出 16 倍分配事件） |
| 单片分装（同样字节、只多一层 `Arc` + 根 `Vec`） | 与整体态基本持平（213 vs 225 µs）——分片粒度粗到「一片就是全部」只付壳的代价 |
| 留存成本 | 持有一份快照 ⇒ 下一次发布对被改动的那片深拷贝一次：整体态 ≈ 全量（1 MiB ≈ 1,048,624 B/份），16 片选择性 ≈ 1/16（65,752 B/份）；留存**深度**不改每拍工作量，只按份数线性增内存 |

计量口径（读 bench 数据前必读）：分配计数只见**事件**不见 memcpy 字节；单次分配跨过分配器 mmap 阈值（≥1 MiB）后每字节成本跳变（本机 1 MiB ≈ 210 µs vs 64 KiB ≈ 2.3 µs），故跨布局倍数里含**分配器类别效应**，不是纯带宽差。**不要**把上述倍数外推成「分片总是更快」。

可运行示例：`cargo run -p udf-skeleton-infra --example state_layout`（同一逻辑数据的整体 vs 分片布局，用真实深拷贝字节数演示「碰过的片必深拷贝、未碰片与旧快照同址」——输出不含墙钟/分配事件，那两项以 bench 为准）。

---

## 资源上限与默认值（决策载体）

跨面资源策略的**唯一正典表**（维护者裁定：表是本仓的决策载体，`#56` 是证据载体、不拥有决策；数值调整只走本表修订）。各面 rustdoc 只写「本面的界语义 + 超界行为」，数值引用本节，**禁各写各的**。

### 统一约定

1. **`0` = 不限（unbounded）**，沿用 `ByteBudget` / `set_capacity` 既有先例；显式 opt-in 才设界。
2. **默认 = 保守可用**：默认值必须让结构相异的第二消费方（#56 Stage B：`validation/consumer-sim/cli-host`）开箱可用，不为了炫技设小界。
3. **超界行为必须属三类之一并写进该面 rustdoc**：拒绝（typed error）· 降级（inline/degrade）· 摘除（fail-closed）——**禁止静默丢**；静默丢必须另有独立丢弃计数（见「丢弃证据（互不替代）」词条）。
4. **界是准入闸（hard cap），不是指标**：指标（borrowed/idle/dropped…）与准入闸分离，禁一个计数兼任两职（#28 教训）。
5. **pre-publication 时钟**：默认值一旦发布即表现层契约——**改大 = 兼容放宽，改小 = 破坏性变更（走 ADR）**。

### 当前策略（基线 = 实现现状；默认值已由维护者拍板 keep current，见下方决定记录）

| # | 资源项 | 所在面 | `0` 含义 | **当前基线** | 超界行为 | 逃生口 |
|---|---|---|---|---|---|---|
| 1 | 活跃注册数上限 | `std_ext::scheduler`（#58） | 无界（默认） | `Scheduler::set_capacity(cap)`；默认**无界**（提案 `1024` **未采纳**，见决定记录） | 拒绝：`register` 返回 typed `SchedulerFull`（任务未入堆，不静默丢） | 显式 `set_capacity(n)`；调低到当前活跃数以下不驱逐既有任务 |
| 2 | watchdog 执行数上限 | `shutdown::ShutdownCoordinator`（#54） | 无界（`None`，默认） | `with_max_watchdogs(n)`；默认**不限**（提案 `4096` **未采纳**，见决定记录） | 拒绝启动：该组件记 `Failed{reason: watchdog capacity exhausted}`（**不**内联跑闭包、不静默） | 显式 `with_max_watchdogs(n)`；`live_watchdogs()` / `peak_watchdogs()` 读数 |
| 3 | 历史 Run-ID 诊断注册表 | `runtime`（#62） | 不适用（关 = 不登记） | `enable_run_id_diagnostics(capacity)`，**默认关闭**（不登记、零 per-Run 锁与分配）；启用后容量 = `DEFAULT_RUN_ID_TRACKING_CAPACITY = 1024`（**已定**，见决定记录） | 饱和/驱逐 ⇒ 三态判 `not-tracked`（**绝不**判 `definitely-unknown`）；`cancel_unknown_id` 驱逐后停止增长 | 窄口径 opt-in；`run_id_tracked_count` / `run_id_tracking_capacity` / `run_id_evicted_count` 可观测 |
| 4 | 审计环容量（权威历史） | `SyncStore::subscribe_events` / `PoolBase` | 视作 `1`（调用方给 `0` 时取 `max(1)`） | `SyncStore`：**调用方显式给**；池域：`AUDIT_RING_DEFAULT_CAP = 1024` | 溢出丢最旧（`seq` 空洞 = 显式证据；`take_events` 取走不留空洞） | 调大 `subscribe_events(cap, ..)` / 池域容量；完整性靠「权威历史」不靠实时回调 |
| 5 | 审计待投递 FIFO | `SyncStore` / 池域（#65/#75） | —（固定） | `AUDIT_PENDING_CAP = 1024`（**无配置面**，两处同值） | 溢出丢最旧 + **独立投递丢弃计数**（`audit_delivery_drops()`，与环溢出分账） | 消费慢 ⇒ 改用 `take_events` 取权威历史 |
| 6 | 回灌深度预算 | `runtime` 排空环 | —（固定） | `FEEDBACK_DEPTH_BUDGET = 64`（单次 dispatch 连续回灌链深度） | 停排 + `feedback_budget_exceeded` 计数；触发点 action **脱钩停放（deferred）**（保留来源身份，主队列继续；非 panic 非静默，**不是**退回队尾自动重放） | 显式 `resume_deferred(limit)`（#49）；`DispatchOutcome::Deferred{retained}` 上报 |
| 7 | deferred 停放队列 | `runtime`（#49） | 不适用（容量独立配置） | `set_deferred_capacity(n)` + `deferred_capacity()` / `deferred_actions()`；默认 `DEFAULT_DEFERRED_CAPACITY = 64`（**已定**，见决定记录）；**独立于 ingress capacity**；**#95：与停放路径共锁线性化**——线性化点 = `inner.deferred_queue` 锁内发布（停放先得锁则按旧值留存、setter 返回后一律按新值；**缩容不丢既有 parked 记录**） | 满 ⇒ 新续接拿**显式非成功终局**（`FinishRoot(Failed)`）+ 专用计数 `deferred_capacity_rejected()`，不入队、不消失、不误标成功 deferred | `resume_deferred(limit)`（FIFO、fresh epoch、独立续接收据）；`DispatchOutcome::CapacityRejected` 可观测 |
| 8 | FxScope 等待/跟踪条目 | `fx`（#13/#39） | 无界（`usize::MAX`，默认） | `with_max_pending(n)` / `with_max_tracked(n)`；默认**均无界** | 拒绝：`try_spawn_tracked` 返回 typed `AdmissionError::{PendingFull,TrackedFull}`（不阻塞不 panic） | 显式设界；`try_reap()` 收割后回落；`counts()` 读数 |
| 9 | 对象池/线程池 idle 容量 | `pool` | 由 tuning 显式给 | `ObjectPoolTuning` / `ThreadPoolTuning` **显式必填，无默认**（`SizeClassPool` 逐档继承 tuning，无自有 cap） | 超 `idle_cap` ⇒ `dropped_overflow` 计数（有界降级） | 调 tuning；`shrink_to`/`prune_idle` 主动回收 |
| 10 | 外部准入队列容量（ingress） | `runtime`（#38） | 无界（默认） | `SyncStore::set_capacity(n)`；默认 **0 = 不限**（等待被 reduce 的 action 数；正在执行的已出队，不计入） | 拒绝：`try_dispatch` ⇒ typed `AdmissionOutcome::Full`；`ActionSender::dispatch` ⇒ `false`；`SyncStore::dispatch`（无失败面）过载 panic fail-visible——均不阻塞、不部分插入；计数 `admission_full_count()` | 显式 `set_capacity(n)`（**#95：与准入共锁线性化**——线性化点 = `inner.queue` 锁内发布；准入先得锁则按旧值完成、setter 返回后一律按新值；**缩容不回溯**既有记录，**扩容与 0 即时**生效）；`admission_full_count()` 读数 |

**数值决定记录（2026-11-23 维护者拍板）**：本表全部默认值**保持现值（keep current）**——具体为：Run-ID `1024`、deferred `64`、审计环/待投递 `1024`、回灌深度 `64`、ingress 无界 **照旧**；scheduler 与 watchdog 的提案值（`1024` / `4096`）**不采纳，保持无界**（草案见 `reports/2026-11-23-resource-defaults-draft.md`，留痕备查）。依据：#56 Stage C 消费方验收结论「全默认开箱可用，未出现要求把默认值改小的证据」（`reports/2026-11-23-56-stage-c-consumer-validation.md`）；共同约定 2「默认=保守可用，禁为炫技设小界」。`AUDIT_PENDING_CAP` 无配置面、`FEEDBACK_DEPTH_BUDGET` 固定——均按现状记录，不新增配置面。

**修订规则**：某默认值经 #56 Stage B 实测不合格 ⇒ 修订本表一次（改大 = 兼容放宽；改小 = 破坏性，需 ADR 记录理由与迁移路径），各面 rustdoc 随表同步，禁单点改数。

---

## notify — 异步信号式通知（案 C 水位）

**feature**：`runtime`（默认，随 SyncStore）。**依赖面**：契约 `skeleton::notify` 零依赖；实现面零新三方。决策与论证唯一权威 = ADR 0008（异步通知信号）。

```rust
// skeleton::notify（契约层，零依赖）
pub trait NotifySink: Send + Sync + 'static {
    fn publish(&self, version: u64);   // O(1)；允许合并；panic 由框架捕获并摘除（fail-closed）
}
pub trait Notify<S> {
    fn subscribe_notify(&self, sink: impl NotifySink) -> Subscription<S>;  // 票据复用 Subscription，Drop=注销
}
// skeleton_infra::runtime：impl<S> Notify<S> for SyncStore<S>
// 读侧：store.notify_published() -> u64（当前状态版本）
```

语义（ADR 0008 §2 公理 → §5 落地）：

- 排空环内对异步档只做 **O(1) 版本投递**，绝不执行用户重活；投递序 = 异步投递先于同步回调。
- 载荷 = 单调状态版本号（**无事件流**——M1 状态化判据）；消费侧按需 `store.snapshot()` 拉取现态。
- state_version 门控：零异步订阅者不自增（纯同步部署成本 = 一次 Acquire load——无订阅 dispatch 锚 77→78.4ns，±10ns 容忍内）；50ms 慢消费者在途 dispatch 实测 ~172ns（重活若进环应 ~5e7ns，隔离 4 个数量级）。
- 投递口 panic：框架捕获 + 摘除 + 留痕（fail-closed，对齐订阅回调 panic 先例）。
- 两档并存：既有 `subscribe`/`subscribe_local` 同步语义一字不改（既有通知/审计/回灌测试零改动实证）。
- 闭门/注销语义显式（A6），计数面可对账。

行为面：`tests/async_notify.rs`（§5.3 六项断言 + 投递序断言）。

## pool — PoolBase + ThreadPool + ObjectPool

**feature**：`pool`（默认，连带 `memgov`）。**依赖面**：纯 std + 契约词汇（`skeleton::pool`），域面零三方（crate 树见 [§零依赖口径](#零依赖口径)）。

### 资源上限四问 — 配置值到底约束什么

本域各池只有一个共同点：**每个配置值各自约束一个特定群体**。「把 `max` 当总对象数 / 总内存上限」是最常见的误读（蓝图 §5 的三轴口径即为此立）。下表逐参数回答四问：①cap 覆盖哪些对象/字节；②满容量时的动作与可观察读数；③是否存在合法的超配中间态、靠什么收敛；④各读数字段的口径，以及哪些读数**不是**准入面。

行号口径：全篇行号锚为 **2026-11-17 落盘时的快照值**（本轮 #34/#35/#39/#40/#41/#42/#44/#45/#46/#47/#48 改动后**未逐个更新行号**）——**符号名恒准，行号随重构漂移**；代码变动后行号可能已过时，定位请按符号名（各处坐标均附符号名或所在函数）。路径均相对 `skeleton-infra/src/`。核心形字段定义见 `skeleton/src/pool.rs:42`。`ObjectPool` 的三轴见 [§ObjectPool](#objectpool--通用对象池有状态对象reset-钩子--有界降级--分片槽位)，`MemoryPool` / `BoundedRegistry` / `ByteBudget` 见 [§memgov 四问](#资源上限四问--memgov-三件)。

#### PoolBase — 账本与裁决，不做准入

| 配置/入口 | ①覆盖群体 | ②满容量行为 | ③超配中间态与收敛 | ④读数口径 | 代码锚 |
|---|---|---|---|---|---|
| `max` | 实现方经 `note_borrow` / `note_return` 上报的「idle + 借出」合计（组合件不持有对象，群体由特化池定义） | **无满容量动作**：`PoolBase` 不裁决单笔借出是否放行，只在归还时按边界判留存 | 是：`resize` 缩容只改边界，已在借句柄不受影响 ⇒ `borrowed` 可短时高于新 `max`，随归还收敛 | `metrics().max` = 当前配置（非构造期原值）；`watermark = borrowed / max` | `pool.rs:280` |
| `idle_cap` | **仅闲槽留存**（不约束借出） | 归还判据 `idle < idle_cap && idle + borrowed < max`；不满足 ⇒ 丢弃 + `dropped_overflow += 1` + 审计 `OverflowDropped`（归还永不阻塞、永不报错） | 是：缩容窗内 `idle` 可短时高于新 `idle_cap`，由归还裁决 / 实现方裁剪路径收敛 | `dropped_overflow` 只数**容量因**；`PoolAuditKind::{Returned, OverflowDropped}` 与它一一对应 | `pool.rs:329` |
| `resize(max, idle_cap)` | 参数合法性（`max == 0` / `idle_cap == 0` / `idle_cap > max`） | 非法 ⇒ `Err(PoolResizeError)`，状态零变更 + 审计 `ResizeRejected`；合法 ⇒ `Resized` | — | 错误是 `Copy` 值（拒绝路径零分配，`Display` 自带两参原值） | `pool.rs:45`、`pool.rs:261` |

- **`note_borrow` / `metrics()` 是观测面，不是第三本准入账**（蓝图 §5）：`PoolBase` 不做 check-then-act；准入权在特化池——`MemoryPool` 在槽位锁内判 `created < max`（`memgov.rs:568`），`ObjectPool` 在片锁内判 `alive < share(max)`（`objpool.rs:1255`）。把 `metrics()` 当放行判据 = 无锁 check-then-act。
- **归还两入口（#98）**：`note_return` = 裁决与真实槽位提交**分属两段临界区**（事件先于提交发射；适用于「槽位存储自有锁、事件重入看不到中间态」的实现方）；`note_return_with(commit)` = `pub(crate)` 协作入口，core 裁决 + 实现方槽位提交/配额回吐在**同一 `core` 临界区**内完成，事件恒描述**已提交**状态（`MemoryPool` 用它）。锁序 `core → 实现方槽位锁`；任何实现方**禁**持槽位锁再取 core；`resize` 被裁对象的析构在 `resizing` 闸**外**。
- **池 metrics 的快照语义（#99）**：`MemoryPoolMetrics` / `ThreadPoolMetrics` 是**逐面非原子快照**（不取 `resizing`）——并发 `resize` 时各面可能来自相邻时刻；`ThreadPool` 的 actual-vs-target worker 差异属合法状态，不是混合窗口。要「同一已提交配置」请读 `resize`/审计回调内的状态（回调在全部内部锁释放后运行）。
- **resize 审计事件的时序（#99）**：`Resized` / `ResizeRejected` 在实现方状态**全部提交且内部锁释放后**发射 ⇒ 回调可重入 `resize`/`acquire`/`metrics`；事件顺序 = 实际发射序（并发 resize 由各实现方串行闸定序），载荷不带目标值——读配置用 `metrics()`。`ThreadPool::resize` 的 epoch 在**任何用户回调之前**领取（「最后发起的目标生效」）。

#### ThreadPool — 三参各管一件事

| 参数 | ①覆盖群体 | ②满容量行为 | ③超配中间态与收敛 | ④读数口径 | 代码锚 |
|---|---|---|---|---|---|
| `core` | **保活 worker 数**（驻留 park，零 CPU）；是下限，不是任务面上限 | 不触发任何拒绝；`resize` 提高 core ⇒ 补建至新值（返回即到数） | 是：扩容补建遇 worker 创建失败 ⇒ `PoolResizeError::SpawnFailed { target_core, workers }`——调参已落地而 worker 未建够（**部分生效**），已建 worker 继续服务，可重试 `resize` 补齐 | `metrics().core` = 目标保活数（非存活数） | `pool.rs:938`、`pool.rs:977` |
| `max` | **worker 数**上限（含 core）——不是任务数、不是队列长度 | 队列满且 `workers < max` ⇒ 新建 worker 直接接管本任务（first-task 交接，队列不越限）；队列满且 `workers >= max` 或已 shutdown ⇒ 走拒绝策略 + `rejected += 1` | 是：缩容只发退出令牌（差额 worker 处理完当前任务即退）⇒ `workers` 可短时 > 新 `max`，`shape.watermark = busy / max` 因此可 > 1.0 | `workers` = 真实存活数（创建失败不留幻影）；`busy` = 执行中；`idle` = parked（**不是**遗留对象数） | `pool.rs:885`、`pool.rs:954` |
| `queue_cap` | 中央队列槽数 | 两档互斥：`queue_len < cap` 入队 FIFO；`>= cap` 走上行的扩容或拒绝 | 否：入队与拒绝判决同锁完成，`queue_len <= queue_cap` 恒成立；`queue_cap == 0` 在 `resize` 入口即拒（直通管道 = 池语义自毁） | `queue_len` / `queue_cap` | `pool.rs:885`、`pool.rs:953` |
| （失败分账） | — | worker 创建失败 ⇒ `RejectedTask { cause: SpawnFailed }`，任务**原样退还**调用方 | — | `spawn_failures`（环境故障）与 `rejected`（策略触发，含 CallerRuns 内联）是两本账，禁用其一反推另一 | `pool.rs:1085` |

- **三参不可互相推导**：`max` 管 worker、`queue_cap` 管队列、任务总量 = 二者之和再加在跑件；调高 `queue_cap` 不提高并行度，调高 `max` 也不保证队列不溢出。

#### PooledThreadPool — 无自有上限，两池的账分开记

- **①覆盖群体**：自身不设任何 cap——缓冲侧吃 `ObjectPool` 三轴（`max` / 字节 / 降级），提交侧吃 `ThreadPool` 三参（`core` / `max` / `queue_cap`）[`pooled_thread_pool.rs:116`]（`submit` 直接透传 `ThreadPool::execute`）。
- **②满容量行为**：缓冲侧**永不拒绝**——宇宙满即降级直构（`degraded` 可见）；提交侧队列满且 worker 到 `max` ⇒ 默认 `Abort` 退还 `Err(RejectedTask)`，`with_caller_runs` 档改由提交线程代跑。
- **③超配中间态**：两侧各自成立（缓冲侧 `borrowed` 可含降级借出、提交侧 `workers` 可短时高于新 `max`）；组合件不新增第三条状态。
- **④读数口径**：`threads().metrics().rejected`（提交侧）与 `buffer_pool().metrics().degraded` / `constructed`（缓冲侧）是两条独立路径的留痕，**禁混账**——`rejected > 0` 不代表降级过，`degraded > 0` 也不代表丢过任务。

### PoolBase — 池化管理组合件

组合件不是池：不碰对象、不管槽位存储——承载任何池都要一遍的管理面账本与纪律（边界校验/水位线读数/池域审计事件流/溢出计数）。内存池/线程池各持一个 `PoolBase` 字段。

```rust
pub const AUDIT_RING_DEFAULT_CAP: usize = 1024;

impl PoolBase {
    pub fn new(max: usize, idle_cap: usize) -> Self;             // 非法参数 panic
    pub fn resize(&self, max: usize, idle_cap: usize) -> Result<(), PoolResizeError>;
    pub fn metrics(&self) -> PoolMetrics;                        // O(1) 单锁 Copy
    pub fn note_borrow(&self);                                   // 借出记账 +1
    pub fn note_idle_delta(&self, delta: isize);                 // idle 对账（负=panic fail-fast）
    pub fn note_return(&self) -> bool;                           // true=留存 / false=丢弃+计数
    pub fn subscribe_audit(&self, cap: usize,
        f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket;
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent>;      // 环随取随清
    pub fn audit_callback_panics(&self) -> u64;
    pub fn audit_delivery_drops(&self) -> u64;                   // #75：投递丢弃（实时流背压因），与 ring 溢出分账
}
```

- 校验规则一套两档出口：`max == 0` / `idle_cap == 0` / `idle_cap > max` → 构造期 panic、运行时 `Err(PoolResizeError)`（拒绝路径零状态变更）。
- 归还裁决 RAII 唯一自洽：归还永不阻塞永不报错；槽满 = 丢弃 + `PoolMetrics::dropped_overflow` 计数（fail-visible）。
- 审计发射唯一收口：未订阅 = 一次 gate load 即返；订阅态 = seq 单调入账（容量环溢出丢最旧）+ **锁外**回调（回调内重入 emit 只录环——TLS 在途标记封自指递归）；回调 panic 摘除留痕。
- **审计实时回调 = 串行派发，不丢并发事件**（#75）：回调有**显式在途状态 + 待投递 FIFO** ⇒ 回调在途期间的并发 emit **入队而非跳过**（此前是 `notify.take()` 窗口内静默丢件），在途者按 `seq` 序在**所有内部锁之外**补投；FIFO **有界**，溢出丢最旧并计入**独立的投递丢弃计数** `audit_delivery_drops()`（与 ring 溢出 `dropped_overflow` **分账**：历史侧丢 vs 实时流丢）；回调 panic 时 **fail-close 本代次**（摘回调 + 清本代次 pending 并计数），**排队事件不投递给替换订阅**。三条不变：`FnMut` 串行 / 禁锁内调用 / 禁同线程递归。
- **resize 预留协议：目标 epoch 串行化**（#69）：`workers` 视作**已承诺 + 已预留**总量（增长缺口**不再减去在途预留**）⇒ 并发 resize 不再双预留同一缺口（此前实测 `workers=7 max=4`）；`target_epoch`/`next_epoch` 串行化目标更新 ⇒ **最后发起**的目标生效，旧目标预留的席位在兑现后按最新 core 重算退役令牌（`reconcile_shrink`），**不会被新目标静默满足两次**；缩容令牌只从**可退休的真实 worker** 计且不得退到最新 core 以下；**OS 线程创建仍在状态锁外**，任务创建失败仍退还提交者。
- `watermark = borrowed / max`，可 >1.0（缩容后借出数高于新池深的合法中间态，读数如实不钳位）。

### ThreadPool — 动态线程池

[`PoolManaged`] 的 worker 调度特化：中央队列（Mutex+VecDeque+Condvar，不搞工作窃取）、零自旋（idle worker condvar park）、优雅缩容（退出令牌，手上无任务即退，执行中必跑完）、需求性扩容（队列满且 workers<max 时新 worker first-task 交接）。三参封顶 core/max/queue_cap。

```rust
pub type Task = Box<dyn FnOnce() + Send + 'static>;

pub struct ThreadPoolTuning { pub core: usize, pub max: usize, /* queue_cap 私有 */ }
impl ThreadPoolTuning { pub fn new(core: usize, max: usize, queue_cap: usize) -> Self; }

pub struct RejectedTask(Task);
impl RejectedTask { pub fn into_task(self) -> Task; }

pub trait RejectPolicy: Send + Sync + 'static {
    fn on_reject(&self, task: Task, pool: &PoolHandle) -> Result<(), Task>;
}
// 默认四件套（Java TPE 同名）：AbortPolicy（退还）/ CallerRunsPolicy（提交线程内联）
// / DiscardPolicy（丢弃+计数）/ DiscardOldestPolicy（弃队头换血）

pub struct PoolHandle { /* 策略受限操作柄 */ }
impl PoolHandle {
    pub fn queue_len(&self) -> usize;
    pub fn is_shutdown(&self) -> bool;
    pub fn pop_oldest(&self) -> Option<Task>;
    pub fn offer(&self, task: Task) -> bool;
}

pub struct ThreadPool { /* base + policy + shared */ }
impl ThreadPool {
    pub fn new(tuning: ThreadPoolTuning) -> Self;                // 默认 Abort
    pub fn with_policy(t: ThreadPoolTuning, policy: Arc<dyn RejectPolicy>) -> Self;
    pub fn execute(&self, task: impl FnOnce() + Send + 'static) -> Result<(), RejectedTask>;
    pub fn resize(&self, t: ThreadPoolTuning) -> Result<(), PoolResizeError>;
    pub fn metrics(&self) -> ThreadPoolMetrics;
    pub fn shutdown(&self);                                      // 幂等；排空后退役
    pub fn join(&self);                                          // 等 workers==0
    pub fn subscribe_audit(&self, f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket;
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent>;
}
impl Drop for ThreadPool { /* 发优雅关闭信号，不 join */ }

pub struct ThreadPoolMetrics {
    pub core: usize, pub workers: usize, pub busy: usize, pub idle: usize,
    pub queue_len: usize, pub queue_cap: usize,
    pub completed: u64, pub rejected: u64, pub task_panics: u64,
    pub shape: PoolMetrics,   // 经 Borrow<PoolMetrics> 可达核心形
}
```

- 构造 fail-fast：`core == 0` / `max == 0` / `core > max` / `queue_cap == 0` panic。
- `execute`：队列有位即入队（FIFO）；队列满且 workers<max → 新 worker first-task 交接（不越限排队）；队列满且到 max 或已 shutdown → 拒绝策略。每次提交恰一次 `Box` 分配（类型擦除不可避免）。
- 拒绝策略在**锁外**调用；`rejected` 计数在策略触发时已 +1。任务 panic 摘除（worker 存活接续）+ `task_panics` 留痕。

### PooledThreadPool — 互喂组合件

```rust
pub struct PooledThreadPool<T> { /* buffers: Arc<ObjectPool<T>>, threads, … */ }
impl<T> PooledThreadPool<T>
where T: Send + 'static
{
    pub fn new(buffers: Arc<ObjectPool<T>>, threads: ThreadPool) -> Self;
    pub fn with_caller_runs(buffers: Arc<ObjectPool<T>>, tuning: ThreadPoolTuning) -> Self;
    pub fn submit(&self, f: impl FnOnce(&mut T) + Send + 'static) -> Result<(), RejectedTask>;
    pub fn buffer_pool(&self) -> &ObjectPool<T>;
    pub fn threads(&self) -> &ThreadPool;
}
```

借还在 worker 线程任务闭包内（`Pooled` 句柄借用池、非 `'static`，提交线程不可持）：worker 认领后借缓冲 → 跑 → 句柄 Drop 即还（panic unwind 同覆盖：宇宙记账必回卷；`ObjectPool` 的 unwind 防御把 panic 件销毁不回池并计 `panic_dropped`）。

**借空 = 降级直构（永不丢任务）**：`ObjectPool::acquire` 永不阻塞永不失败——宇宙（idle + 正常借出，硬上界 `max`）满时直构降级件（不入宇宙、归还即释放），任务总能跑完。旧语义「借空丢任务 + `borrow_rejected` 计数」已退役（0.1.0 未发布直迁，`borrow_timeout` 参数一并移除），降级面读 `buffer_pool().metrics()` 的 `degraded` / `constructed`。

`with_caller_runs` 是**提交侧饱和档**（复用 `CallerRunsPolicy`，不另造策略机制）：队列满且 worker 到 `max` 时提交线程代跑，不丢工作。提交侧 `rejected` 与缓冲侧 `degraded` 是两个独立计数，不得混账。

### ObjectPool — 通用对象池（有状态对象，reset 钩子 + 有界降级 + 分片槽位）

池化**有状态**对象：现有 `MemoryPool` 只能池无状态物（构造器只负责造，归还不重置），本件补「捞出改参数即复用」的能力（加密上下文 / 连接 wrapper / 协议状态机——构造昂贵、每次都被重造）。零领域语义（公开 API 无 buffer/connection/crypto 词汇）。

**三轴各自独立，禁互相推导**（蓝图 §5 口径）：`max` 是**记账宇宙**的硬上界，`ByteBudget::total` 是**idle 常驻字节**的硬上界，降级件两条轴都不计。四问对照：

| 轴 / 参数 | ①覆盖群体 | ②满容量行为 | ③超配中间态与收敛 | ④读数口径 | 代码锚 |
|---|---|---|---|---|---|
| alive（`max`） | 池宇宙 = idle + **正常**借出；不含降级件 | `claim_growth` 全片配额满 ⇒ 走降级轴（不等待、不失败） | 是：`resize` 缩容**不回溯裁剪** `alive`（不夺借出）⇒ 已认领存活数可短时高于新 `max`，随归还按新配额收敛；`core.borrowed` **含降级借出** ⇒ `idle + borrowed <= max` **不成立** | `core.idle` + `core.borrowed` 是宇宙读数（后者含降级）；`core.watermark = borrowed / max` 可 > 1.0，不钳位 | `objpool.rs:1255`、`objpool.rs:1164` |
| 留存（`idle_cap`） | 仅 idle 槽位，按片 `share(idle_cap, shards, idx)` 均分（各片之和 = 配置值） | 归还时 home 片满 ⇒ 迁入有余量的片（`alive` 随迁）；全池余量零 ⇒ 丢弃 + `core.dropped_overflow += 1` + 审计 `OverflowDropped` | 是：缩容裁剪逐片串行，窗口内 `core.idle` 可短时高于新 `idle_cap`，裁剪完成即收敛到新上界 | `core.dropped_overflow` 只数**容量因**（idle 满 / 全池余量零 / resize 裁剪），与 `reset_panicked`（钩子因）分账 | `objpool.rs:1290`、`objpool.rs:1164` |
| 字节（`ByteBudget::total`） | **仅 idle 常驻字节**（`weight(&T)` 现值，`Vec` 类按 `capacity()`）；借出对象不计 | 入 idle 槽（归还 / `insert`）时 `try_take` 原子 check+act；余额不足 ⇒ 返回 `false`，调用方按容量因丢弃（无「认领一半」） | 否：`held <= total` 恒成立；**借出即回卷**（`give_back`）——所以 `total` 不是进程内存上限，也不是借出对象内存上限 | `metrics().idle_bytes` = 预算已认领字节（**共享预算**下 = 全局 held，不构成按池归因）；`byte_budget` = 构造期总额；未启用两者恒 0 | `objpool.rs:411`、`objpool.rs:1362` |
| 降级（`degraded`） | 宇宙满后直构的对象：不入宇宙、不占字节预算 | 直接调工厂（`degraded` +1、`constructed` +1），归还即释放、永不进 idle | 允许「交给调用方的对象数 > `max`」——ADR 0010 的有意设计，不是记账漏洞 | `degraded` = 降级直构次数；`constructed` = 工厂调用总数（含降级）——两独立计数，禁互推 | `objpool.rs:900` |

```rust
pub enum ResetPhase { OnReturn, OnAcquire }        // reset 钩子相位
pub struct ObjectPoolTuning { pub max: usize, pub idle_cap: usize }
pub struct ObjectPoolMetrics {                   // Borrow<PoolMetrics>
    pub core: PoolMetrics,
    pub degraded: u64,      // 降级直构次数（池宇宙已满时）
    pub reused: u64,        // 命中复用次数 = idle 槽移出次数（含随后被 reset panic 销毁者）
    pub constructed: u64,   // 工厂调用总次数
    pub pruned: u64,        // prune_idle 释放数
    pub reset_panicked: u64,// reset 钩子因销毁数（OnReturn 相位钩子 panic / 重入检出）——容量因走 core.dropped_overflow
    pub panic_dropped: u64, // unwind 中归还而销毁的对象数
    pub idle_bytes: usize,  // 预算已认领字节（仅 idle 常驻；未启用恒 0；共享预算下 = 全局 held）
    pub byte_budget: usize, // 池字节预算总额（构造期定死；未启用恒 0）
}

pub struct ObjectPool<T> { /* 分片槽位 + 片内计数（每片独占 cache line）*/ }
impl<T> ObjectPool<T> {
    pub fn new(max: usize, idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static) -> Self;   // 非法参数 panic
    pub fn with_reset(max: usize, idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        reset: impl Fn(&mut T) + Send + Sync + 'static,
        phase: ResetPhase) -> Self;
    pub fn with_byte_quota(max: usize, idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        byte_budget: usize,                          // 池私有预算（按池归因用这个）
        weight: impl Fn(&T) -> usize + Send + Sync + 'static) -> Self;
    pub fn with_shared_byte_quota(max: usize, idle_cap: usize,
        factory: impl Fn() -> T + Send + Sync + 'static,
        budget: Arc<ByteBudget>,                     // 多池共用一份上限
        weight: impl Fn(&T) -> usize + Send + Sync + 'static) -> Self;
    pub fn acquire(&self) -> Pooled<'_, T>;          // 永不阻塞、永不失败
    pub fn metrics(&self) -> ObjectPoolMetrics;      // O(shards) 逐片取锁求和
    pub fn prune_idle_with_clock(&self, max_idle_age: Duration, clock: &dyn Clock) -> usize;
        // 正典老化口（Clock 注入：MockClock 测试 / SystemClock 生产）；
        // `prune_idle(max_idle_age, now: Instant)` 已 #[deprecated]（issue#22）
    pub fn resize(&self, tuning: ObjectPoolTuning) -> Result<(), PoolResizeError>;
    pub fn audit_callback_panics(&self) -> u64;      // 回调 panic 摘除留痕（同 PoolBase 同名同义）
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent>;  // 环内事件随取随清
}
// 另实现 skeleton::pool::PoolManaged（Tuning=ObjectPoolTuning, Metrics=ObjectPoolMetrics）

pub struct Pooled<'p, T>(/* … */);
impl<'p, T> Pooled<'p, T> {
    pub fn detach(self) -> T;      // 交出所有权：对象脱离池生命周期
}
impl<T> Deref for Pooled<'_, T> { type Target = T; }   // Deref + DerefMut
impl<T> Drop for Pooled<'_, T> { /* Drop 即还（禁手动还） */ }
```

- **借出期池不持强引用**：借出 = 值移动出槽（池物理不持引用）⇒ 在借对象对 `prune_idle`/`resize` 清理路径**物理不可见**（不会被「还有人用着就释放」）；`DerefMut` 拿对象本体改参数即复用；`detach()` 让对象彻底脱离池生命周期（最后一个持有者放手即自然析构，无 GC 兜底亦无额外回收器）。
- **reset 钩子（可选，单钩子 + 相位）**：`OnReturn` 归还时重置（.NET `IResettable` / HikariCP 同位）/ `OnAcquire` 取出时重置（deadpool `recycle` 同位）；钩子在**锁外**、独占借用下调用；**归还路径的 reset 走 `catch_unwind` 兜底**——panic ⇒ 该对象状态不可信 ⇒ 丢弃（不入 idle）+ `reset_panicked` +1（禁 abort：Drop 期 panic 在 unwind 中会 abort 进程）；`OnReturn` 相位**不传播 payload**（Drop 期无日志设施，就地消化，仅计数留痕；`PoolAuditKind` 无钩子故障词汇 ⇒ 不发审计事件）、`OnAcquire` 相位**原样传播**；新构造对象不调 reset。
- **三因分账（`dropped_overflow` vs `reset_panicked` vs `panic_dropped`）**：容量因（idle 满 / 全池余量零 / `resize` 裁剪）只进 `core.dropped_overflow`；钩子因（**`OnReturn` 相位**的钩子 panic / 重入检出；`OnAcquire` 相位原样传播不计数）只进 `reset_panicked`；unwind 中归还只进 `panic_dropped`。三个口径互不混账，禁用其一线索反推另一因。
- **`core.borrowed` 含降级借出**：`idle + borrowed <= max` **不成立**——`max` 只约束宇宙部分（idle + 正常借出），降级件在宇宙之外（归还即释放、永不进 idle）；水位线 `borrowed / max` 因此可 > 1.0（形态合法，读数如实不钳位）。
- **unwind 感知归还**：`Pooled::drop` 首查 `thread::panicking()`——panic 路径析构 ⇒ **不调 reset、不回池**，直接销毁 + `panic_dropped` +1（r2d2 #31 教训：归还不变量已破的对象会污染下一个借出方）。
- **有界降级**：`acquire()` 无超时参数、结构上不存在等待——池宇宙（idle + 正常借出，硬上界 `max`）已满时**直接构造**（`degraded` +1），该对象**不入宇宙**、归还即释放、永不进 idle；**限额是留存限额，不是对象总数限额**（.NET 官方口径）。
- **释放三路径**：`idle_cap` 满 ⇒ 归还即丢弃（`dropped_overflow`）；`prune_idle_with_clock(max_idle_age, clock)` 显式老化口（宿主按自己节奏调，**池不起后台线程**；`Instant` 版 `prune_idle` 已弃用）；池 `drop` ⇒ 全部 idle 析构 **+ 其认领的字节额度回卷**（`impl Drop`；共享 `Arc<ByteBudget>` 下不回卷会让额度在池死后永久不可用）。**两条析构期承重假设**：①（**issue#35 起退役**）析构**不调** `weight` 钩子——字节按**入池时的存储权重**（`IdleSlot.weight`）随件回卷；故「钩子必须是留存对象的纯函数」这一假设**已撤销**（钩子只在入池计量时调用一次；钩子 panic 不再让字节永久漏记、计数悬挂）；②片锁中毒时按 `PoisonError::into_inner` 尽力回卷、不 panic（析构期数据已不可信，「拒绝归还」严格更差；若在 unwind 中 panic 还会 abort 进程）——这是「记账 bug 一律 fail-fast」的唯一刻意例外。**另**：由于①，钩子已不在片锁内运行 ⇒ 片锁中毒的**毒源消失**，②保留为**防御性容忍**而非常规路径。
- **分片与计数**：分片数构造期自适应 `clamp(idle_cap, 1, 8)`（不设旋钮）；每片一把 `Mutex`，`borrowed`/`alive`/`reused` 等**热路径计数下沉进片内**（普通 `usize`、锁内读写，无跨线程原子 RMW），且每片 `#[repr(align(64))]` 独占一条 cache line（相邻片伪共享同样把 8 线程扩展比压在 1 以下）。`max`/`idle_cap` **按片均分**（余数补给前几片，各片之和恰等于配置值 ⇒「总和 ≤ 配置值」硬上界不变；`resize` 后按新配额逐片裁剪）；**配额一律在目标片锁内读**——`resize` 先 `store` 新值再逐片加锁裁剪，于是并发归还的留存裁决要么读到新值、要么其入片被紧随其后的裁剪覆盖（锁外的陈旧快照没有这条收敛路径，缩容并发归还能把 idle 永久顶在新上界之外）；`metrics()` 逐片取锁求和（O(shards)，非热路径）。借用走**线程亲和片**（亲和表按池地址分槽、每 (线程, 池) 领一个片号后固定，首个领号者拿 0 号 ⇒ 单线程行为确定；同线程多池互不覆盖）、归还**优先回 home 片**（借出时的片，LIFO 热件常驻）；home 片 idle 满时**迁入有余量的片**（`alive` 随迁、先迁入再退 home ⇒ 账面守恒不低估；迁片只发生在归还且 home 满这一情形），全池余量为零才丢弃。`degraded` 与 `constructed` 是两个独立计数，不得混账；`constructed` 等**非每借还热路径**计数仍是全局原子（miss 路径打一次，dim4 计时窗内为 0 次）。**并发消费者调参**：`idle_cap` 按片均分 ⇒ `idle_cap = 16` 在 8 片下每片仅 2 槽，而每个线程通常「1 件在飞 + 1 件常驻」即占满本片，导致归还走迁片、借出走扫片（实测 `pop_iters/op` 2.00→14.06、`ret_steps/op` 1.00→4.26，每多一步 = 一次 mutex）。多线程消费者建议 `idle_cap >= 2 × 线程数` 以维持 home 片稳态；这是**调参建议不是正确性要求**（任何合法 `idle_cap` 都保证「总和 ≤ 配置值」硬上界）。
- **调用方红线**：坏对象禁还池（判定权在调用方——池不知道 `T` 是什么，处置 = `detach` 带走，r2d2 `has_broken` / Commons Pool2 `invalidateObject` 动机）；池化对象应「等重」（reset 负责容量归一，大小悬殊对象混池 = 池毒化 DoS，golang/go#23199、#27735）；`prune_idle_with_clock` 尽力而为，不构成释放失败信号；**weight 钩子体内禁 acquire 同一池**（片锁内计量 = 死锁面；debug 构建 fail-fast 检出，release 零开销仍禁）。**字节配额**：`idle_bytes` 在共享预算池（`with_shared_byte_quota`）下是全局 held 读数、不构成按池归因——按池归因用池私有预算（`with_byte_quota`），本件刻意不引入池侧第二账本。
- **与 `memgov::PoolGuard` 的关系**：两个**独立守卫类型**（非模式变体）——`Pooled` = 对象所有权移交式借用（`DerefMut` 改参数即复用 / RAII 归还 / `detach` 永久移交），`PoolGuard` = 缓冲借用（`acquire` 超时失败语义）。`Pooled<'p, T>` 借用池、非 `'static`，带不进 `'static` 任务闭包——用 `detach()` 移交后由任务/连接自持。尺寸分档不在本件（通用池单类型）；分档 = 组合层按档多实例，见下文 **`### SizeClassPool`**。
- 决策依据 ADR 0010：issue 字面要求的「idle 槽存 `Weak`」与「复用保证」结构性矛盾（权威依据：HikariCP 弱引用只能当缓存层、Go `sync.Pool` 官方明说放弃复用保证、Rust `Weak::upgrade` 可能返回 `None`），改以**行为等价**形态交付上述三条释放路径。

### SizeClassPool — 尺寸分档池组合件（按档借还 + 档位不匹配降级）

**feature**：`pool`（默认）。一张**严格递增**档位表 + 每档一个 `ObjectPool<T>`，对外按尺寸借还。消费方形态（sing-box `common/buf/alloc.go` 十二级 `sync.Pool`，64B→64KB）：请求 64KB 却拿到 128KB 块的根因不在单池，而在缺一层选档——本件只补这层，档内借还复用 `ObjectPool` 的四路径与记账（不改单池实现）。

```rust
pub struct SizeClassStat { pub class_bytes: usize, pub pool: ObjectPoolMetrics }
pub struct SizeClassMetrics {
    pub classes: Vec<SizeClassStat>,   // 按档明细，与档位表同序同长
    pub over_max_degraded: u64,        // 超最大档的档外直构次数（独立口径）
}
impl SizeClassMetrics {
    pub fn idle(&self) -> usize;         // 全档合计
    pub fn borrowed(&self) -> usize;     // 全档合计（含池内降级借出）
    pub fn class_degraded(&self) -> u64; // 全档**池内**降级合计
}

pub struct SizeClassPool<T> { /* 档位表 + 每档一个 ObjectPool */ }
impl<T: 'static> SizeClassPool<T> {
    pub fn new(class_bytes: impl IntoIterator<Item = usize>,
        max: usize, idle_cap: usize,
        factory: impl Fn(usize) -> T + Send + Sync + 'static) -> Self;
        // 非法档表 panic（空 / 含 0 / 非严格递增）；max/idle_cap 校验同 ObjectPool
    pub fn acquire(&self, need: usize) -> SizeClassHandle<'_, T>;  // 永不失败
    pub fn class_bytes(&self) -> &[usize];                         // 档位表
    pub fn metrics(&self) -> SizeClassMetrics;                     // 按档明细 + 档外降级
}

pub struct SizeClassHandle<'p, T>(/* … */);
impl<T> SizeClassHandle<'_, T> {
    pub fn class_index(&self) -> Option<usize>;  // None = 请求超出最大档（档外直构）
    pub fn class_bytes(&self) -> usize;          // 档字节；档外直构时 = 请求字节
    pub fn is_over_max_degraded(&self) -> bool;  // 是否走档外降级直构
}
impl<T> Deref for SizeClassHandle<'_, T> { type Target = T; }  // + DerefMut；Drop 即还
```

- **向上取整选档**：`acquire(need)` 取档位表里第一个 `>= need` 的档。请求 64KB 拿 128KB 块**允许**（浪费上界 = 档位表相邻档比值）；反向（请求 128KB 拿 64KB）**不可能**——严格递增表 + 单调选档，只有「多给」。
- **归还按档归位**：句柄记**借出时的档**，Drop 回那一档的池（`ObjectPool` 的 home 片亲和照旧）；**不按对象当前容量反推档位**——对象借出期涨大后按容量反推会把大件塞回小档池（池毒化，golang/go#23199、#27735）或漂到相邻档，两种都是错账。
- **超最大档 = 档外降级直构**：按**请求字节**直接构造（`over_max_degraded` +1），不入任何档的池宇宙、归还即释放；**不静默回退到最大档**——否则「请求 1MB 拿到 64KB」成为无信号的隐式截断，调用方拿到远小于请求的对象却查不到任何线头。
- **池内降级与档外降级分账**：档内池满走的仍是 `ObjectPool` 的降级直构（读数在 `classes[i].pool.degraded`）；与 `over_max_degraded` 是两个独立计数——混账则「档位错配」与「容量不足」无法区分。`classes[i].pool.core.borrowed` 含池内降级借出（同单池口径）。
- **组合红线**：①**池化对象等重**（同档件都应是该档字节量级；`factory` 收到档字节或请求字节，按其开容量）；②**若配 reset 钩子，钩子必须把容量归一到档字节**（`Vec::clear` 不缩容量，需 `shrink_to`）——容量只涨不归则档位语义随时间漂移、按档读数与字节预算全失真。本件构造的档池走 `ObjectPool::new`（无 reset 钩子，对象状态跨借还保留）；需要归还时重置的部署自建 `ObjectPool::with_reset` 档池并自证归一——本件刻意不暴露 reset 配置面（单池钩子相位/重入/panic 纪律属单池契约，组合层再开一个调参入口会让调用方无从判断哪个生效）。
- **每档池深相同**（`max`/`idle_cap` 逐档复用）：**聚合宇宙 = 档数 × `max`**——`max` 是**每档**的正常对象上界（idle + 正常借出），全池可同时持有的正常对象上限是 `档数 × max`；把它当「全池对象总数上限」是确切误读。档深差异化需求先改单池调参面，本件不复制第二个调参入口。`metrics()` 为 O(档数 × 分片数) 取锁求和、每次分配一个 `Vec`（档数固定、诊断面才读）。

**最小示例**

```rust
use skeleton_infra::SizeClassPool;

let pool = SizeClassPool::new([64, 1024, 65_536], 4, 2, |size| Vec::<u8>::with_capacity(size));
let buf = pool.acquire(600);              // 向上取整 → 1KiB 档
assert_eq!(buf.class_bytes(), 1024);
let huge = pool.acquire(1 << 20);         // 超最大档 → 档外直构，不回退到 64KiB
assert!(huge.is_over_max_degraded());
assert_eq!(pool.metrics().over_max_degraded, 1);
```

### 最小示例

```rust
use skeleton_infra::pool::{ThreadPool, ThreadPoolTuning};
use std::sync::Arc;

let pool = ThreadPool::new(ThreadPoolTuning::new(1, 2, 8));
let n = Arc::new(std::sync::atomic::AtomicUsize::new(0));
let n2 = Arc::clone(&n);
pool.execute(move || {
    n2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
})
.unwrap();
pool.shutdown();
pool.join();
assert_eq!(n.load(std::sync::atomic::Ordering::Relaxed), 1);
```

---

## memgov — 内存治理

**feature**：`memgov`（零外部依赖纯 std）；`pool` feature 连带启用。`MemoryPool` 特化面要求 `pool`。

### 资源上限四问 — memgov 三件

`ByteBudget` 类型住 `objpool`（根重导出 `skeleton_infra::ByteBudget`，`lib.rs:179`），与 `ObjectPool` 的字节轴同源——本节从「预算」视角钉四问，池内三轴见 [§ObjectPool](#objectpool--通用对象池有状态对象reset-钩子--有界降级--分片槽位)。

| 入口 | ①覆盖群体 | ②满容量行为 | ③超配中间态与收敛 | ④读数口径 | 代码锚 |
|---|---|---|---|---|---|
| `BoundedRegistry::capacity` | 条目数（`len`），**不约束值体量** | `Eviction::Reject` ⇒ `Err(BoundError { capacity })` 且原内容不动；`Eviction::EvictOldest` ⇒ 先 `pop_first()`（首键）再插；已存在 key = 覆盖（`len` 不变，不触发淘汰） | 否：判定与插入同函数内完成，`len <= capacity` 恒成立；`Clone` 是深拷贝，副本容量语义逐句柄独立 | `len()` / `capacity()`；`BoundError.capacity` 回报撞上的上界 | `memgov.rs:64`、`memgov.rs:79` |
| `MemoryPool::max` | `created` = idle 槽 + 借出在外（**槽位锁内** check-and-act：`inner.created < inner.max`） | 闲槽空且 `created == max` ⇒ condvar park 至调用方期限，到期 `Err(PoolAcquireTimeout)`（`Duration::ZERO` = 非阻塞尝试）；有配额空位则工厂**锁外**新建（panic 经配额回滚后原样传播） | 是：`resize` 缩容只弃超限 idle（`created` 同步递减），借出句柄不受影响 ⇒ `created` 可短时 > 新 `max`，随归还跌破（丢弃路径 `created -= 1`） | `metrics().core`：`max` 当前配置 / `idle` 闲槽 / `borrowed` 在外 / `dropped_overflow` 容量因丢弃累计 / `watermark = borrowed / max`；`MemoryPoolMetrics.idle_cap` 单列 | `memgov.rs:558`、`memgov.rs:568` |
| `MemoryPool::idle_cap` | 闲槽上限（收回留裁决） | 归还时判 `idle < idle_cap && idle + borrowed < max`；不满足 ⇒ 对象析构 + `created` 配额回吐 + `dropped_overflow` +1 | 仅账本瞬窗：缩容在同一临界区 `truncate` idle，`PoolBase` 账本更新跟在槽位锁之后（一次 `note_idle_delta` 延迟）——是读数时序，不是容量击穿 | `MemoryPoolMetrics.idle_cap` = 当前配置（resize 可调） | `pool.rs:329`（裁决）、`memgov.rs:718`（调用点） |
| `ByteBudget::total` | **仅 idle 常驻字节**（多池共享同一份 `Arc<ByteBudget>` 时 = 各池 idle 之和） | `try_take` 单字原子 CAS check+act（无锁）：余额不足返回 `false`，调用方按容量因丢弃；`n == 0` 恒 `true`（未启用计量面的调用点短路） | 否：`held <= total` 恒成立（总额构造期定死、不可 resize）；借出 / prune / resize 裁剪经 `give_back` 回卷，下溢 = 记账 bug ⇒ panic | `total()` 总额、`held()` 已认领、`available()` 余额；共享预算下 `ObjectPool::metrics().idle_bytes` = 全局 held，**不构成按池归因** | `objpool.rs:411`、`objpool.rs:394` |

- **观测面 ≠ 准入面**：`CountingAllocator::live_bytes` 一类读数是进程级观测（含测试框架自身分配），不能当池的准入判据；准入/裁决只有三处 check-and-act——`created < max`（槽位锁内，`memgov.rs:568`）、`idle < idle_cap && idle + borrowed < max`（槽位锁内，`pool.rs:329`）、`held + n <= total`（单字原子 CAS，`objpool.rs:411`）。`metrics()` 全是纯读数。

### 计数分配器与泄漏断言

```rust
pub struct CountingAllocator;   // #[global_allocator] 仅测试/基准侧声明，发布构建不声明=零开销
impl CountingAllocator {
    pub fn live_bytes() -> usize;    // O(1)
    pub fn live_allocs() -> usize;   // O(1)——净存活计数（gauge）
    pub fn peak_bytes() -> usize;    // 仅串行上下文有效（并行测试的他测分配计入读数）
    pub fn reset_peak();
    pub fn total_allocs() -> usize;    // 历史累计成功分配，只增不减（counter）
    pub fn total_deallocs() -> usize;  // 历史累计释放，只增不减（counter）
}

pub struct LeakGuard { /* … */ }
impl LeakGuard {
    pub fn arm() -> Self;            // 记基线；无计数分配器时 enabled=false 全 no-op（诚实降级）
    pub fn check(&self) -> Result<(), LeakReport>;  // allocs/bytes 回基线=Ok
}
pub struct LeakReport { pub allocs_delta: isize, pub bytes_delta: isize }

pub fn expect_empty<K: Ord, V>(reg: &BoundedRegistry<K, V>);  // 非空即 panic（判据二）
```

Drop 兜底尽力而为——**规范用法=测试尾显式 `check()`**/`expect_empty`/fx 侧 `expect_body_idle`。判据三（任务面）在 `FxScope::expect_body_idle`（任务体结束口径）；要「跟踪集合已完全收割」的更强判据用 `expect_fully_reaped_idle`。

**churn 口径（live × total 联用判级）**：窗口 churn = `total_allocs(t2) − total_allocs(t1)`。live 净额对「即用即还」恒 0——净 0 无法区分「真零分配」与「同量 churn」；联用：live 平 + total 增 = 高 churn 零泄漏，两者皆平 = 窗口真零分配。realloc 成功按「换块」计（total 两面各 +1；live 面不动）。total_allocs 与 total_deallocs 差值长期不等 = 累计泄漏/外部遗忘面。

### BoundedRegistry — 有界注册表

```rust
pub enum Eviction { Reject, EvictOldest }
pub struct BoundError { pub capacity: usize }

#[derive(Clone)]   // deep-copy 值容器（非共享句柄）；运行期只读共享用 Arc<BoundedRegistry> 外包
pub struct BoundedRegistry<K: Ord, V> { /* BTreeMap + 边界 + 策略 */ }
impl<K: Ord, V> BoundedRegistry<K, V> {
    pub fn new(max: usize, evict: Eviction) -> Self;   // max==0 panic
    pub fn insert(&mut self, k: K, v: V) -> Result<(), BoundError>;
    pub fn get(&self, k: &K) -> Option<&V>;
    pub fn get<Q: ?Sized + Ord>(&self, k: &Q) -> Option<&V> where K: Borrow<Q>;  // 零分配查找
    pub fn get_mut(&mut self, k: &K) -> Option<&mut V>;   // 只回值面：键面类型级不可达（改键破坏确定序）
    pub fn remove<Q: ?Sized + Ord>(&mut self, k: &Q) -> Option<V> where K: Borrow<Q>;  // 与 get/get_mut 同形借用纪律：String 键以 &str 摘除零分配（issue#8）
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize;
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_;   // 升序确定序
}
```

「上限+策略必填」由构造签名类型强制。已存在 key=覆盖（len 不变，不触发淘汰）；满：Reject→`Err(BoundError)`（原内容不动）/ EvictOldest→先 `pop_first`（首键）再插。`Clone`=deep-copy（map/max/evict 同拷，两句柄独立，容量语义逐句柄保持）。K 契约：K 须单调指派（序号/时间戳键）语义=逐最旧；乱序如实退化为 EvictMinKey（不静默补偿）。LRU/TTL 精确语义在 [`cache`](#cache--lruttl-缓存)。

### MemoryPool — 内存池特化（pool feature）

```rust
pub struct MemoryTuning { pub max: usize, pub idle_cap: usize }
pub struct MemoryPoolMetrics { pub core: PoolMetrics, pub idle_cap: usize }  // Borrow<PoolMetrics>
pub struct PoolAcquireTimeout;   // Copy；Display 面错误

#[repr(C)]
pub struct MemoryPool<T, F = fn() -> T> { /* PoolBase + 槽位锁 + condvar + 垫片 */ }
impl<T, F: Fn() -> T> MemoryPool<T, F> {
    pub fn new(max: usize, idle_cap: usize, factory: F) -> Self;   // 非法边界 panic
    pub fn acquire(&self, timeout: Duration) -> Result<PoolGuard<'_, T>, PoolAcquireTimeout>;
    pub fn resize(&self, tuning: MemoryTuning) -> Result<(), PoolResizeError>;
    pub fn metrics(&self) -> MemoryPoolMetrics;
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent>;
}
// 另实现 skeleton::pool::PoolManaged（Tuning=MemoryTuning, Metrics=MemoryPoolMetrics）

pub struct PoolGuard<'p, T>(/* … */);
impl<T> Deref for PoolGuard<'_, T> { type Target = T; }        // Deref + DerefMut
impl<T> Drop for PoolGuard<'_, T> { /* Drop 即还；禁手动还（armor #26） */ }
```

三参封顶：max/idle_cap 在 `MemoryTuning`（resize 可调）；获取超时逐调用必填（D21——拿不到=显式 `PoolAcquireTimeout`，`Duration::ZERO` = 非阻塞尝试），结构上不存在无限等模式。闲槽直取 LIFO；空槽且配额未满则工厂**锁外**新建（panic 经配额回滚后如实传播）；否则 condvar park 至期限（零自旋）。归还：闲槽未满留存，槽满对象析构+配额回吐。并发形态：锁不嵌套（账本锁与槽位锁顺序各取）；`#[repr(C)]` + `align(64)` 垫片防伪共享。

### 最小示例

```rust
use skeleton::pool::PoolManaged;
use skeleton_infra::memgov::{MemoryPool, MemoryTuning};
use std::time::Duration;

let pool: MemoryPool<Vec<u8>> = MemoryPool::new(4, 2, Vec::new);
let mut buf = pool.acquire(Duration::from_millis(10)).unwrap();
buf.push(7);
assert_eq!(pool.metrics().core.borrowed, 1);
drop(buf); // RAII：Drop 即还（禁手动还）
assert_eq!(pool.metrics().core.idle, 1);

// 管理面：动态调参 + 池域审计流（票据 Drop 即关流）
pool.resize(MemoryTuning { max: 8, idle_cap: 4 }).unwrap();
let mut kinds = Vec::new();
let ticket = PoolManaged::subscribe_audit(&pool, move |e| kinds.push(e.kind));
let again = pool.acquire(Duration::from_millis(10)).unwrap(); // 复用同一件
assert_eq!(&*again, &[7]);
drop(again);
drop(ticket);
```

### alloc_site — 分配站点目录（issue#6，2026-10-30）

**feature**：`memgov`。**依赖面**：纯 std。按站点归集资源计数的机制层：**编译期静态表注册**（`alloc_sites!` 宏，常量初始化——无 ctor/无惰性）+ 每站点原子计数/独立开关 + 全局闸一把切 + `&'static str` 查表零分配。

```rust
pub struct AllocSite { pub name: &'static str }              // count/enabled 私有原子
impl AllocSite {
    pub const fn new(name: &'static str) -> Self;            // 手工组表口
    pub fn bump(&self);                  // 全局开 && 站点开 ⇒ fetch_add(1, Relaxed)；关 ⇒ 零写入
    pub fn count(&self) -> u64;          // Relaxed load（单调计数器，无跨变量因果需求）
    pub fn set_enabled(&self, on: bool); // 站点闸：关 ⇒ 旁路，历史计数保留（清零走 reset）
    pub fn is_enabled(&self) -> bool;
    pub fn reset(&self);                 // 测试/基准复用
}
pub fn set_global_enabled(on: bool);     // 全局闸：默认开，关 ⇒ 所有站点一刀切零写入
pub fn global_enabled() -> bool;
pub fn find(table: &'static [&'static AllocSite], name: &str) -> Option<&'static AllocSite>;

#[macro_export] macro_rules! alloc_sites { /* 见下 */ }
```

宏（站点登记唯一口；每次调用生成**独立**静态表，表间互不干扰——与 `skeleton::lint` 的进程级全局注册表性质相反，测试隔离天然成立）：

```rust
alloc_sites! {
    pub static ADOPTER_SITES = {
        CONN_ACCEPT,      // 生成 pub static CONN_ACCEPT: AllocSite（名字 = stringify!）
        CONN_RECYCLE,
    };
}
// ADOPTER_SITES: &[&AllocSite]——声明序 = 枚举序（const 数组，编译期定死，跨平台稳定）
```

**坑面 armor（8 条，`src/alloc_site.rs` doc + `tests/alloc_site.rs` 双处钉死）**：①单调计数器 `Relaxed` 足够（无发布语义对象）②开关检查序先全局后站点（全局关一刀切最廉）③禁用路径**零写入**不脏缓存行（语义是「开关」非「计数为 0」）④枚举序=宏声明序 ⑤同表重名不编译期校验（`find` 返回先声明者，唯一性归采用方）⑥伪共享不预付（独立 static 布局不保证隔离；升级路径 `#[repr(align(64))]` 有证据再上）⑦bump/count/find/枚举全路径零堆分配（事件计数锚 + 仪器自检）⑧与既有面分工：`CountingAllocator`=进程级分配计数、`log-embed`=tracing 埋点不计数——本件是第三口径（按站点的应用层资源计数），不做日志不出指标格式。

**收窄**：不做阈值判定/上报/告警/序列化/站点归属映射/自动发现。

**实测**（单批，Windows 10 x64 / 16 核 / rustc 1.96.0；`cargo run --release -p udf-skeleton-infra --features memgov --example alloc_site_multidim`）：bump 全开 **5.3ns** / 关闸 **1.6–1.7ns**（计数不变=零写入证据）；find 线性 1/10/100 站点 = 5.6/22.0/160.0ns（边际 ~1.5–1.8ns/站点）；同站点 4 线程 bump **0.20x**（同一 `AtomicU64` 争用上限，设计口径登记）；宏表隔离 PASS。

---

## text — 文本处理

**feature**：常驻，零 feature 门。**依赖面**：纯 std。

```rust
pub enum DecodeStrategy { Strict, Lossy }

pub struct DecodeError {
    pub strategy: DecodeStrategy,  // 失败时所选策略
    pub bad_start: usize,          // = std Utf8Error::valid_up_to
    pub bad_end: usize,            // error_len 为 None（截断尾部）时取 bytes.len()
}

pub fn decode_bytes(bytes: &[u8], strategy: DecodeStrategy) -> Result<String, DecodeError>;
pub fn truncate_chars(s: &str, max_chars: usize) -> &str;

pub fn to_snake_case(s: &str) -> String;
pub fn to_camel_case(s: &str) -> String;
pub fn to_pascal_case(s: &str) -> String;
pub fn to_kebab_case(s: &str) -> String;
pub fn to_screaming_snake_case(s: &str) -> String;

pub fn edit_distance(a: &str, b: &str) -> usize;              // 字节面 Levenshtein
pub fn edit_distance_normalized(a: &str, b: &str) -> f64;     // 1 - dist/max_len（字节长）
```

- `decode_bytes`：显式策略禁静默 lossy 默认（信任边界默认 Strict）；Lossy 非法字节替换 U+FFFD 恒 Ok。错误=首个非法序列字节偏移区间。
- `truncate_chars`：字符边界安全截断，永不 panic；空串/短串原样返回。
- case 转换：ASCII 词界（heck 0.5 ASCII 子集语义）——分隔符=任何 ASCII 非字母数字字符；`fooBar`/`XMLHttpRequest`/`foo2Bar` 词界规则齐备；非 ASCII 字符一律按无大小写词内字符原样保留不折叠（与 heck 的 Unicode 折叠差异如实声明）。五向同内核：两趟扫描（词数+字节数）→ 精确容量一次分配，无中间 Vec。
- `edit_distance`：**字节面**契约——ASCII 输入与字符级语义一致；非 ASCII 按字节计（`"中文"` vs `"中闲"` = 3）。内存 `O(min(m,n))` 单行滚动数组恰一次分配，时间 `O(m·n)`。归一化同距 + 一次除法；两者皆空=1.0，一空一非空=0.0。

### 最小示例

```rust
use std_ext::text::{decode_bytes, truncate_chars, DecodeStrategy};

assert_eq!(decode_bytes("中文".as_bytes(), DecodeStrategy::Strict), Ok("中文".to_owned()));
assert_eq!(decode_bytes(b"\xff", DecodeStrategy::Lossy).unwrap(), "\u{FFFD}");
assert_eq!(truncate_chars("中文", 1), "中");
assert_eq!(truncate_chars("a\u{1F600}b", 2), "a\u{1F600}");
assert_eq!(std_ext::text::to_snake_case("XMLHttpRequest"), "xml_http_request");
assert_eq!(std_ext::text::edit_distance("kitten", "sitting"), 3);
```

### 实测锚（源码 doc 口径）

- `decode_bytes`：有效路径 ~48-71ns @30-130B；Strict 非法路径零分配 ~7ns（2026-10-16 批 D）。
- `truncate_chars`：~10-12ns @30 字符截 16，约 1ns/扫描字符（2026-10-17 复验）。
- case 五向 @XMLHttpRequest ≈165-178ns；snake @64B 混合 ≈600-700ns（2026-10-17 复验）。
- `edit_distance`：≈1.1ns/格——100B 对 ≈11.4µs、1KiB 对 ≈1.1ms（2026-10-17 复验）。

---

## backoff — 确定性退避

**feature**：常驻，零 feature 门。**依赖面**：纯 std。

```rust
pub fn backoff_delay(attempt: u32, base: Duration, max: Duration) -> Duration;
```

第 `attempt` 次重试前的确定性退避延迟：`attempt` 从 1 起（0 视为未重试返回 `Duration::ZERO`）；线性 `base.saturating_mul(attempt)` 饱和乘法防溢出；结果截到 `max`。**禁 jitter**（确定性回放要求）。零时钟零随机——sleep 归调用方（fx 侧 `tokio::time` / sync 侧 `std::thread::sleep`）；重试循环在 [`retry`](#retry--同步重试编排)。log-embed 埋点 target `skeleton::backoff`（debug 级，纯函数域唯一埋点=派单契约显式点，勿在其上加事件）。

```rust
use std_ext::backoff::backoff_delay;
use std::time::Duration;

let base = Duration::from_millis(100);
assert_eq!(backoff_delay(1, base, Duration::from_secs(10)), base);
assert_eq!(backoff_delay(3, base, Duration::from_millis(250)), Duration::from_millis(250));
```

实测锚：纯算术常数级 ~3ns/次（`saturating_mul`+`min`，零分配零时钟；2026-10-16 批 D）。

---

## retry — 同步 + 异步重试编排

**feature**：常驻，零 feature 门。**依赖面**：纯 std。函数 `retry`/`retry_if`/`retry_async`/`retry_if_async` **不进 crate 根 re-export**（避免与模块名撞名），调用形 `retry::retry(...)`；类型已 re-export。

```rust
pub trait Sleeper: Clock {
    fn sleep(&self, d: Duration);   // 足额等待；欠睡=退避语义失真，实现禁静默缩短
}
// 实现：SystemClock（thread::sleep）/ MockClock（advance——虚拟推进零真实 sleep）

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy { /* max_attempts/base/max_delay/budget */ }
impl RetryPolicy {
    pub fn new(max_attempts: u32, base: Duration, max_delay: Duration) -> Self; // 详见 Panics
    pub fn with_budget(mut self, budget: Duration) -> Self;   // 只配次数不配预算=风暴面（红线）
    pub fn with_jitter(mut self, j: JitterFn) -> Self;        // 默认 full jitter（禁关）；测试注入恒等源保确定性
}

pub enum RetryError<E> {
    Exhausted { attempts: u32, last: E },
    OutOfBudget { attempts: u32, last: E, elapsed: Duration },
}
impl<E> RetryError<E> { pub fn into_last(self) -> E; }

pub fn retry<T, E, F, S>(op: F, policy: &RetryPolicy, sleeper: &S) -> Result<T, RetryError<E>>
where F: FnMut() -> Result<T, E>, S: Sleeper;

pub fn retry_if<T, E, F, P, S>(
    op: F, policy: &RetryPolicy, sleeper: &S, retryable: P,
) -> Result<T, RetryError<E>>
where F: FnMut() -> Result<T, E>, P: FnMut(&E) -> bool, S: Sleeper;

pub trait AsyncSleeper { /* sleep(d) -> impl Future<Output = ()> + Send；调用方注入（如 tokio），零运行时依赖 */ }

pub async fn retry_async<T, E, F, Fut, S>(
    policy: &RetryPolicy, op: F, sleep: S,
) -> Result<T, RetryError<E>>
where F: FnMut(u32) -> Fut, Fut: Future<Output = Result<T, E>>, S: AsyncSleeper;

pub async fn retry_if_async<T, E, F, Fut, P, S>( /* 同形 + retryable 判定 */ );
```

**红线**（armor 定案）：① jitter 默认 full（禁关——无 jitter 同拍风暴，AWS 一手实测）；② 双闸（次数+预算）缺一即风暴面；③ retry 默认假定操作幂等，非幂等禁入（文档红线，不落类型门）；④ 异步 sleep 内联 await，future drop 即全停（取消安全）。同步/异步同 policy 同语义（对照测试钉死）。

语义契约：

- **政策显式**：次数/退避/预算构造期一次定死（`Copy`，运行期只读）。`new` Panics：`max_attempts == 0`（首试不是重试，零次执行请勿调用）、`base` 为零（零退避=忙等风暴）——构造期 fail-closed。
- `max_attempts` 含首试。第 N 次失败后（且裁决放行）睡 `backoff_delay(N, base, max_delay)`；`base > max_delay` 合法（saturating 语义静默截平）。
- `with_budget`：累计耗时（注入钟口径）≥ budget 即停（`OutOfBudget` 带 elapsed）。预算闸在每次退避睡眠**之前**检查；`budget = 0` 合法=失败即停。
- 判定器先于睡眠：`retryable` 为 false 的错误立即停（快路径零睡眠，`Exhausted.attempts` = 拒绝时的执行次数）。
- 裁决序（每次失败后、睡眠前）：次数 → 可重试 → 预算。
- **panic 非 error**：op panic 原样传播（不转 Err、不计次、不睡眠）。
- 预算测量与退避睡眠同钟（`Sleeper` 超 trait 绑 `Clock`——错配在类型层不可表达）。

```rust
use std::time::Duration;
use skeleton::clock::MockClock;
use std_ext::retry::{retry, RetryPolicy};

let clock = MockClock::new();
let policy = RetryPolicy::new(3, Duration::from_millis(100), Duration::from_secs(10));
let mut remaining = 2u32;
let op = || {
    if remaining > 0 {
        remaining -= 1;
        Err("transient")
    } else {
        Ok(7u32)
    }
};
assert_eq!(retry(op, &policy, &clock).unwrap(), 7);
assert_eq!(clock.unix_offset(), Duration::from_millis(300)); // delay(1)+delay(2)
```

实测锚（2026-10-16 批 2-b）：不可重试快路径零分配零睡眠；首试成功编排税 ~11ns/次、单次重试（含 MockClock 虚拟睡眠）~25ns/次。

---

## paths — 路径归一与基目录

**feature**：常驻，零 feature 门。**依赖面**：纯 std。事故锚：F19（std `canonicalize` Windows 返回 `\\?\` verbatim）+ F21（`..` 词法归一越根绕过 containment）。命名纪律：无 IO 归一名带 `lexical`、带 IO 归一名带 `canonicalize`，禁隐式择一。

### 词法面

```rust
pub fn normalize_lexical(path: &Path) -> PathBuf;
pub fn simplify(path: &Path) -> &Path;                            // Windows 剥 \\?\；非 Windows 恒等
pub fn canonicalize_compat(path: &Path) -> std::io::Result<PathBuf>; // fs::canonicalize + simplify
pub fn relative_to(path: &Path, base: &Path) -> Result<PathBuf, RelError>; // 目标在前基准在后（pathdiff 惯例）
pub fn ensure_inside(base: &Path, path: &Path, strategy: CaseStrategy) -> Result<(), NotInside>;

pub enum CaseStrategy { CaseSensitive, CaseInsensitive }

pub enum RelError {
    MismatchedKind { path: PathBuf, base: PathBuf },
    PrefixKindMismatch { path: PathBuf, base: PathBuf },
    NotPrefix { path: PathBuf, base: PathBuf },
}
pub struct NotInside { pub base: PathBuf, pub path: PathBuf }
```

- `normalize_lexical`：解 `.`、成对消解 `..`、统一平台分隔符。**保守语义（F21 装甲）**：绝对根处的 `..` 被吸收（POSIX `/..`≡`/`）；相对路径顶部的 `..` 保留（不臆造）；裸盘符后的 `..` 保留；**verbatim/设备路径原样返回**（其中 `.`/`..` 是字面文件名）。
- `simplify`：verbatim→legacy 剥离，dunce 1.0.5 同语义四道闸（仅 VerbatimDisk / 合法 legacy 文件名 / 非 DOS 保留名含上标变体 / UTF-16 ≤260）；不可剥离原样返回。热路径零分配。
- `relative_to`：参数序对齐 pathdiff（`relative_to(path, base)` 读作「path 相对 base」）；两侧先 `normalize_lexical` 再 `simplify` 后组件级前缀匹配；`path == base` → `Ok(".")`；失败模式全显式（禁 pathdiff 式 `Option` 静默）。
- `ensure_inside`：containment 判定（含相等），判定前两侧归一+简化；大小写策略显式参数（`CaseInsensitive` 为 ASCII 折叠，非 ASCII Unicode 折叠不在此面，如实声明上限）。

### 家目录与 XDG（env 解析面，零 IO——不创建目录）

```rust
pub fn home_dir() -> Option<PathBuf>;    // std::env::home_dir 直包
pub fn config_dir() -> Option<PathBuf>;  // $XDG_CONFIG_HOME（绝对）→ $HOME/.config
pub fn cache_dir() -> Option<PathBuf>;   // $XDG_CACHE_HOME（绝对）→ $HOME/.cache
pub fn data_dir() -> Option<PathBuf>;    // $XDG_DATA_HOME（绝对）→ $HOME/.local/share
```

- XDG 语义（spec v0.8 锚定）：home 缺失 ⇒ `None`（链断传播，比 spec 更保守的 fail-closed）；env 值**空=未设**；**相对值=invalid 忽略**回退默认；结果过 `normalize_lexical`。**Windows 面恒 `None`**（XDG 是 Unix 约定，调用方按 None 分支处置，勿静默回退）。macOS 走本 XDG 面。
- None 处置指引：**禁静默回退 cwd**——`logx::DefaultPathResolver` 在构造期显式 `Err` 即先例。
- armor 路径：relative→invalid→ignore（相对 env 值忽略回退默认）。

```rust
use std_ext::paths::{ensure_inside, normalize_lexical, CaseStrategy};
use std::path::Path;

let n = normalize_lexical(Path::new("a/./b/../c"));
assert_eq!(n.components().count(), 2); // a, c
assert!(ensure_inside(Path::new("a/b"), Path::new("a/b/c"), CaseStrategy::CaseSensitive).is_ok());
assert!(ensure_inside(Path::new("a/b"), Path::new("a/c"), CaseStrategy::CaseSensitive).is_err());
```

### 实测锚（源码 doc 口径）

- `simplify`/`strip_bom` 类纯函数：零分配；`normalize_lexical` 常规路径 2 次分配（事件口径 `tests/paths_alloc.rs` 逐字钉死）。
- 梯度锚（16/128/4096 = **输入字符长度**三档，非 ns 值；各档 ns 数见采集器输出）见采集器 `examples/paths_multidim.rs`（2026-10-16 批 b1-paths）。

---

## clock — 时间源

**feature**：常驻，零 feature 门。**依赖面**：纯 std。**批 1 契约真下沉**（2026-10-18；ADR 0001）：时间源契约 `Clock`/`SystemClock`/`MockClock` 现住 `skeleton::clock`（被 scheduler/ratelimit/retry/cache/id 多域消费的核心契约住契约层）；本 crate 的 `clock` 模块只余行为件（时长格式化 + RFC 3339）。

契约面（`skeleton::clock`）：

```rust
pub trait Clock: Send + Sync {
    fn now_instant(&self) -> Instant;     // 单调测量面（单调非递减但不均匀，可前跳）
    fn now_system(&self) -> SystemTime;   // 挂钟展示面（非单调、不计闰秒、可回拨）
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;                   // std 直通

#[derive(Debug)]
pub struct MockClock { /* Mutex 内部态 */ }
impl MockClock {
    pub fn new() -> Self;                          // 起点 UNIX_EPOCH（unix_offset()==ZERO）
    pub fn advance(&self, delta: Duration);        // 只进不退；累计溢出 fail-closed panic
    pub fn unix_offset(&self) -> Duration;         // 断言用读数
}
impl Default for MockClock { /* new() */ }
impl Clock for MockClock { /* … */ }
```

- 装甲 #19（Instant vs SystemTime 混用）防线：`Clock` 双方法名强制分流，调用点名字即语义。
- MockClock 确定性语义：`now_system = UNIX_EPOCH + 虚拟偏移`（挂钟面跨运行逐位确定）；`now_instant` 锚定构造瞬间的真实时刻，只做同实例内相对比较；后退不可表达（`Duration` 无符号）；时间只在显式 `advance` 时前进（零自动前进——tokio auto-advance 陷阱的反向对表）。

行为件（`std_ext::clock`，常驻）：

```rust
pub fn format_duration(d: Duration) -> String;
```

- `format_duration`：humantime 对标**去 calendar 单位**（year=365.25d 是虚构历法，只留 day=86400s 及以下）。规则：单位序 `day/h/m/s/ms/us/ns`，零分量省略，全零 `"0s"`；**截断不四舍五入**；纯整数除法无浮点；day>1 复数 `s`；`us` 用 ASCII（GBK 安全）。

### rfc3339 — RFC 3339 双向（子模块 `clock::rfc3339`）

```rust
pub fn format_rfc3339(time: SystemTime) -> String;               // 恒 Z 后缀、T 大写
pub fn format_rfc3339_seconds(time: SystemTime) -> String;       // 整秒定宽形，恒 20 字节（issue#3）
pub fn parse_rfc3339(s: &str) -> Result<SystemTime, TimeParseError>;       // 严格档
pub fn parse_rfc3339_weak(s: &str) -> Result<SystemTime, TimeParseError>;  // 宽容档

pub enum TimeParseError {
    InvalidDigit { at: usize, byte: u8 },
    InvalidFormat { at: usize },
    OutOfRange { at: usize },
    LeapSecond { at: usize },
}
```

- `format_rfc3339`：小数秒如实最小位（纳秒为零输出整秒，否则 9 位去尾零）；表示域 = current era `[0000-01-01T00:00:00Z, 9999-12-31T23:59:59.999999999Z]`，越界 fail-closed panic。
- `format_rfc3339_seconds`（issue#3 整秒定宽形）：恒 `YYYY-MM-DDTHH:MM:SSZ` 20 字节，亚秒截断（向过去取整，与 parse >9 位截断同向）；字节稳定——同秒任意亚秒恒同一字节序列（追加型日志/账本换芯面）；定宽 ⇒ 全 era 字典序==时间序（无「同 frac 位数」前提）。表示域/越界 panic 语义同 `format_rfc3339`。
- `parse_rfc3339`（严格档）：§5.6 ABNF 全集——`T`/`t`、`Z`/`z`、`±HH:MM`（强制冒号）；闰秒 `:60` RFC 合法但 std SystemTime 不计闰秒——fail-closed 拒（humantime 同输入静默截到 `:59`，本域不做）；>9 位小数秒截断；偏移换算越 era 报 `OutOfRange`。错误优先序：结构 → 数字 → 值域 → 闰秒（`at`=出错字节偏移）。
- `parse_rfc3339_weak`（宽容档/人输面）：另收空格代 `T`、无冒号偏移 `±HHMM`、缺偏移=UTC。值域语义恒同 strict。

```rust
use std::time::{Duration, SystemTime};
use std_ext::clock::rfc3339::{format_rfc3339, parse_rfc3339};

assert_eq!(format_rfc3339(SystemTime::UNIX_EPOCH), "1970-01-01T00:00:00Z");
let t = parse_rfc3339("1996-12-19T16:39:57-08:00")?;
assert_eq!(t, SystemTime::UNIX_EPOCH + Duration::new(851_042_397, 0));
# Ok::<(), std_ext::clock::rfc3339::TimeParseError>(())
```

### 实测锚（源码 doc 口径）

- MockClock 读取 ~11-13ns/次（2026-10-17 复验 11.3ns）；`format_duration` ~87-88ns 与量级无关，`Duration::MAX` 327ns，分配恒 1 次/调用。
- RFC 3339 parse 0 分配（契约口径，模块 tests）。

---

## scheduler — 同步调度器

**feature**：常驻，零 feature 门。**依赖面**：纯 std。双层形态：**确定性内核**（最小堆按注入钟单调面排程，`tick` 调用线程同步点火）+ **驱动线程**（生产外挂，condvar park 到最近 deadline 零自旋）。

```rust
#[derive(Clone)]
pub struct Scheduler { /* Arc 内脏 */ }
impl Scheduler {
    pub fn new(clock: Arc<dyn Clock>) -> Self;
    pub fn after(&self, delay: Duration, task: impl FnOnce() + Send + Sync + 'static)
        -> Result<TaskHandle, Closed>;                     // delay=0 合法（下个 tick 即火）
    pub fn every(&self, period: Duration, task: impl Fn() + Send + Sync + 'static)
        -> Result<TaskHandle, Closed>;                     // period=0 panic
    pub fn try_after(&self, delay: Duration, task: impl FnOnce() + Send + Sync + 'static)
        -> Result<TaskHandle, AdmissionError>;             // #58：触顶返回 Full（无部分插入）
    pub fn try_every(&self, period: Duration, task: impl Fn() + Send + Sync + 'static)
        -> Result<TaskHandle, AdmissionError>;             // #58：触顶返回 Full
    pub fn set_capacity(&self, cap: usize);                 // #58：0=无界（默认）；置上限后 legacy after/every 触顶 panic；#95：与 register 共锁线性化（state 锁内发布，缩容不驱逐、扩容与 0 即时）
    pub fn capacity(&self) -> usize;                        // #58
    pub fn live_registrations(&self) -> usize;              // #58：堆内待执行 + 执行中周期任务（≠ heap.len()）
    pub fn rejected_registrations(&self) -> u64;            // #58：闸关拒绝不计入（同 FxCounts::rejected）
    pub fn tick(&self) -> usize;                           // 点火全部到期任务，返回实际点火数
    pub fn next_deadline(&self) -> Option<Instant>;        // 含惰性墓碑（park 依据）
    pub fn close(&self);                                   // 幂等；回调内调用合法
    pub fn is_closed(&self) -> bool;
    pub fn panic_count(&self) -> u64;                      // 点火 catch_unwind 命中计数
    pub fn worker_passes(&self) -> u64;                    // 驱动循环圈数（零自旋证据）
    pub fn spawn_worker(self: &Arc<Self>) -> WorkerHandle;   // fail-visible 旧口：OS 创建失败 ⇒ 带因 panic（#87）
    pub fn try_spawn_worker(self: &Arc<Self>) -> std::io::Result<WorkerHandle>;  // #87：失败原样返回 io::Error、零残留；失败后注册照常、tick() 内核照常点火
}

pub struct TaskHandle { /* … */ }   // RAII（Drop 即 cancel）；非 Clone；Send + Sync
impl TaskHandle {
    pub fn cancel(&self) -> bool;   // 线性化点=状态 CAS；幂等（已完成取消返 false）
    pub fn detach(self);            // fire-and-forget 逃生门（消费句柄不触发 Drop-cancel；#55：只解除取消，TaskCell 随之释放，不泄漏）
}

pub struct WorkerHandle { /* … */ }
impl WorkerHandle {
    pub fn join(self);   // = close + 等线程退出；worker 侧 panic 原样重抛
}                        // 禁在驱动回调内 join 同一 worker（自等死锁）
                        // #72（维护者 03:59 定稿）：丢弃句柄（Drop）= **只停该 worker**（per-worker 停信标）
                        // ——调度器不关闸、其他 worker 与既有登记不受影响；不 join（句柄可在 worker 回调内被丢）
                        // ⇒ 不确认线程终止、不传播 worker panic。全部句柄丢弃而调度器仍开 ⇒ 注册入堆但不点火。

pub struct Closed;                  // 关闸后注册的显式拒绝
pub enum AdmissionError { Closed, Full }   // #58：拒绝原因（Full = 容量触顶）
```

语义拍板：

- **every = 锚定周期，超载退化固定延迟**：重臂锚 `prev_deadline + period`（零漂移）；回调超载（完成时刻已越过下锚）改锚 `完成时刻 + period`——无补发无追帧（tokio `MissedTickBehavior` 默认 Burst 连发追赶是坑，本件显式不采纳）。
- **cancel 线性化点 = 状态原子位**：CAS 置位成功即承诺此后不再点火；已出堆条目检查-执行窗口内落地的取消不可撤（本次照跑，文档化语义；单线程 tick 确定性面无此窗口）；执行中取消 ⇒ 周期任务不再重臂。
- **close = 关闸 + 清 pending + 在途跑完**：注册显式 `Err(Closed)`；pending 全部标 Cancelled（句柄侧可见）；驱动线程随后退出。
- **回调 panic 不杀驱动**：点火 catch_unwind 摘除 + `panic_count` 留痕；周期任务 panic 后摘除不再臂。
- **detach = 解除取消，不接管所有权**（#55）：此前用 `mem::forget` 抑制 Drop-cancel，连带永久泄漏 `Arc<TaskCell>`（周期任务的回调与捕获图随之留存）。现取消语义与所有权释放解耦——detach 后句柄照常释放，任务仍按调度照跑。
- **丢弃 WorkerHandle = 只停该 worker**（#72，维护者 03:59 定稿；旧「关闸不 join」形态已废止）：句柄持 `Arc<AtomicBool>` 停信标，Drop 在状态锁内置位 + `notify_all`（置位持锁、判定持锁 ⇒ 无丢唤醒）；**调度器不关闸**，其他 worker 与既有登记照常。`join` 仍是显式 scheduler-wide 停机。drop **不 join**（句柄可能在 worker 自身回调内被 drop ⇒ 自等死锁）⇒ 不确认线程终止、不传播 worker panic。**无 worker 态**：全部句柄丢弃而调度器仍开 ⇒ 注册继续入堆但不点火，直到再起 worker 或 `close()`。
- **惰性墓碑**：cancel 不摘堆（O(1)），死亡条目到点弹出时摘除——远期取消的墓碑多醒一拍（语义正确，代价一次空醒）。
- **有界准入**（#58）：`capacity` 默认 **0 = 无界**⇒ 既有行为零变化；**不预设**有界默认值（#56 第二消费方验证前不盲设）。触顶时 legacy `after`/`every` **panic**（fail-visible；返回 `Err(Closed)` 会撒谎），typed 面用 `try_after`/`try_every` 返回 `AdmissionError::Full` 且**无部分插入**。活跃注册数口径 = 堆内待执行条目 **+ 执行中周期任务**（后者仍可能重臂）——**不等同 `heap.len()`**（墓碑不计入）。额度**恰好一次**释放：一次性点火终态、周期任务取消·关闸·panic 四条退役路径共用单一 CAS 闸门（赢家释放）。取消仍 **O(1)**（零锁零扫堆）；容量检查与计数同在状态锁内 ⇒ 并发注册不越上界；被拒回调**不入堆**，其捕获在堆锁外析构。
- **远期取消条目提前回收**（#52）：取消路径仍 O(1)（弱引用记账 + 墓碑计数）；**清扫独立触发**（不在 cancel 路径），按绝对下限 16 / 比例阈值 4 在状态锁内重建堆只留 PENDING 条目，被摘条目**移出锁外**析构。取消 earliest-deadline 时通知驱动并**重算 park 锚**——驱动不再为堆顶墓碑空醒一拍。**执行语义零变化**：已出堆检查-执行窗口内落地的取消仍不可撤、仍照跑。墓碑计数为近似量（清扫期间落地的取消会漂移，良性——额度上限由 CAS 闸门精确管理）。
- 溢出 fail-closed：注册/重臂恒 `checked_add`；重入安全：点火在堆锁外——回调内 after/every/cancel/close 合法。

```rust
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::scheduler::Scheduler;

let clock = Arc::new(MockClock::new());
let sched = Scheduler::new(clock.clone());
let hits = Arc::new(AtomicU32::new(0));
let hits2 = Arc::clone(&hits);
sched
    .after(Duration::from_millis(100), move || {
        hits2.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap()
    .detach();
clock.advance(Duration::from_millis(100));
assert_eq!(sched.tick(), 1);
assert_eq!(hits.load(Ordering::SeqCst), 1);
assert_eq!(sched.tick(), 0); // 一次性任务不复火
```

实测锚（2026-10-16 批 b3-scheduler，采集器 `examples/scheduler_multidim.rs`）：注册竞争下全局状态锁负扩展（~6.2M→2.9M ops/s @1→8 线程，源码 `ponytail:` 注——deadline 全序要求堆顶唯一，分片留待真实负载）；tick 确定性内核零 sleep 时间线逐毫秒断言（MockClock 语义）。

---

## shutdown — 停机编排

**feature**：常驻，零 feature 门。**依赖面**：纯 std。

把三现状停机面（`SyncStore::close`/`Scheduler::close`/`FxScope::shutdown`）按**注册序串行**编排：单组件 panic/超时 fail-visible 上报、不拖垮整体，最终 `ShutdownReport` 全量如实（零组件=空报告合法，`is_clean()`=true）。

```rust
pub struct ShutdownCoordinator { /* … */ }   // Arc 共享线程安全；注册窗与编排窗互斥
impl ShutdownCoordinator {
    pub fn new() -> Self;                          // 注册窗开，无默认超时
    pub fn with_default_timeout(mut self, d: Duration) -> Self;  // builder 沿 FxScope 形
    pub fn with_max_watchdogs(mut self, n: usize) -> Self;       // #54：watchdog 执行数上限（默认不限）
    pub fn live_watchdogs(&self) -> usize;                       // #54：此刻仍在跑的 watchdog 执行数
    pub fn peak_watchdogs(&self) -> usize;                       // #54：live 历史峰值
    pub fn register<F>(&self, name: impl Into<String>, shutdown: F) -> Result<(), RegisterError>
    where F: FnOnce() + Send + 'static;
    pub fn register_with_timeout<F>(&self, name: impl Into<String>, shutdown: F,
        timeout: Duration) -> Result<(), RegisterError>
    where F: FnOnce() + Send + 'static;            // 覆盖协调器默认
    pub fn shutdown(&self) -> ShutdownReport;      // 幂等；并发叫等首叫同报告
    pub fn is_shutdown(&self) -> bool;
}
impl Default for ShutdownCoordinator { /* new() */ }

pub enum RegisterError { Closed, Duplicate(String) }

pub enum OutcomeKind {
    Stopped,
    Failed { reason: String },
    TimedOut,   // 放弃等待非中断——组件线程可能仍在运行（watchdog 线程照跑如实声明）
}

pub struct ComponentOutcome { pub name: String, pub kind: OutcomeKind }

#[derive(Default)]
pub struct ShutdownReport { pub per_component: Vec<ComponentOutcome> }
impl ShutdownReport { pub fn is_clean(&self) -> bool; }
```

语义契约：

- 组件接入形状 = `register(名, 停机闭包)`，闭包内自行调用组件自己的停机 API。注册失败两态：停机已启动/完成=`Err(Closed)`；重名=`Err(Duplicate)`。
- **sync 局限如实声明**：组件闭包无中断面——超时=**放弃等待**（watchdog 线程照跑、detach、报告 `TimedOut`），不是中断；`Duration::MAX` 超时=等到底。
- panic 装甲：组件闭包 panic（内联/watchdog 双路 catch_unwind）→ `Failed{reason}`，后续组件照跑。
- 重入装甲（双锚 ThreadId）：组件闭包内重入 `shutdown()` 就地 panic → 被编排面捕获记 `Failed`（非 HANG 无留痕）；无关线程并发叫=合法等待者等首叫同报告。
- watchdog 线程创建失败/上报通道断连：显式 `Failed`（fail-visible）。
- **watchdog 执行数有上界**（#54）：带超时组件的闭包跑在 watchdog 线程上，超时=**放弃等待、线程照跑** ⇒ 组件大量挂死时仍在跑的 watchdog 数随组件数线性增长。`with_max_watchdogs(n)` 给硬上界：**许可在 spawn 之前占用、于持有者消亡时归还**（线程退出/panic，或起线程失败时 std 丢弃闭包连带归还）⇒ 不靠手写每条分支。许可耗尽时该组件**未启动**（不 spawn 线程、不内联跑闭包），如实记 `Failed{reason: "watchdog capacity exhausted; component not started"}`——**非静默丢弃停机回调**。`live_watchdogs()` / `peak_watchdogs()` 给运营读数（区分「此刻仍在跑」与「曾堆积」）。默认**不限**（既有用户行为零变化）。
- 停机中/后注册静默丢失被 `Err(Closed)` 封死；重名静默覆盖被 `Err(Duplicate)` 封死。

```rust,no_run
use skeleton_infra::shutdown::ShutdownCoordinator;

let coord = std::sync::Arc::new(ShutdownCoordinator::new());
coord.register("store", || { /* store.close() */ }).unwrap();
coord.register("scheduler", || { /* scheduler.close() */ }).unwrap();
let report = coord.shutdown();
assert!(report.is_clean());
```

---

## ratelimit — 令牌桶

**feature**：常驻，零 feature 门。**依赖面**：纯 std。对标 Guava `RateLimiter`（SmoothBursty）的「突发许可 + 恒速补充」语义面——warmup 平滑不做。

```rust
pub struct TokenBucket { /* Mutex 状态胞 + Arc<dyn Sleeper> */ }
impl TokenBucket {
    pub fn new(clock: Arc<dyn Sleeper>, capacity: f64, refill_rate: f64) -> Self;
    pub fn capacity(&self) -> f64;
    pub fn refill_rate(&self) -> f64;
    pub fn try_acquire(&self, permits: u32) -> Result<(), TryAcquireError>;
    pub fn acquire(&self, permits: u32) -> Result<(), ExceedsCapacity>;
}

pub enum TryAcquireError {
    ExceedsCapacity(ExceedsCapacity),
    Insufficient { available: f64, wait: Duration },  // wait=可满足时刻估算（≥1ns）
}
impl TryAcquireError { pub fn wait(&self) -> Duration; }  // ExceedsCapacity 无此语义返 ZERO

pub struct ExceedsCapacity { pub requested: u32, pub capacity: f64 }
```

语义拍板：

- **满桶启动**（构造即满 `capacity` 个令牌）——与 Guava 构造后 `storedPermits = 0` 的差异文档化。
- `try_acquire`/`acquire` 共享同一结算内核：锁内惰性回填 + 判定 + 扣减一次完成（线性化点=临界区内回填后判定通过那一刻）。回填=从 `last` 起点乘法重算（单步误差 ≤1ulp 不跨步积累；禁 `tokens +=` 增量漂移写法）。
- `acquire` 阻塞形态=**锁外睡到再查**（零自旋：每轮真实 park，睡粒度下限 1ns 防忙转）；睡眠绝不持锁。
- `acquire(0)` = 恒成功 no-op；`acquire(n>capacity)` 立即 `Err(ExceedsCapacity)`（等待也无法满足，禁静默无限等）。
- 时间面只消费单调面 `now_instant`（回拨结构性排除）；测量与睡眠同钟（`Sleeper` 超 trait 绑 `Clock`）。
- `new` Panics：`capacity`/`refill_rate` ≤0、NaN、inf（死桶/无限流不该经限流器表达）。
- 公平性：多 acquirer 非 FIFO（无排队序）；活性保证——缺口单调收敛，有限 acquirer 集合内无永久饥饿。

```rust
use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::ratelimit::TokenBucket;

let clock = Arc::new(MockClock::new());
let bucket = TokenBucket::new(clock.clone(), 2.0, 1.0);
bucket.try_acquire(2).expect("首桶满：突发预算立即可取");

bucket.acquire(1).expect("acquire 阻塞到令牌足额");
assert_eq!(clock.unix_offset(), Duration::from_secs(1));

clock.advance(Duration::from_millis(500));
let err = bucket.try_acquire(1).unwrap_err();
assert_eq!(err.wait(), Duration::from_millis(500)); // 缺 0.5 ÷ 1/s
```

---

## cache — LRU+TTL 缓存

**feature**：常驻，零 feature 门。**依赖面**：纯 std。O(1) 结构拍板：`HashMap<K, u32>` + `Vec<Slot>` arena + 槽内 `u32 prev/next` 双向链 + free list，**零 unsafe**。`K: Clone` 来源：map 与 arena 各需一份键，clone 仅发生在 insert 写路径一次——**get 热路径零 clone 零分配**。

```rust
pub struct LruCache<K, V> { /* … */ }   // K: Eq + Hash + Clone
impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    pub fn new(cap: usize) -> Self;                       // cap==0 panic
    pub fn insert(&mut self, k: K, v: V);                 // 已存在=更新+touch；满=逐 LRU 尾
    pub fn get(&mut self, k: &K) -> Option<&V>;           // touch（刷新 recency，故 &mut）
    pub fn peek(&self, k: &K) -> Option<&V>;              // 不 touch 只读
    pub fn peek_mut(&mut self, k: &K) -> Option<&mut V>;  // 更新值不刷新 recency
    pub fn contains(&self, k: &K) -> bool;                // 物理存在，不 touch
    pub fn remove(&mut self, k: &K) -> Option<V>;
    pub fn pop_lru(&mut self) -> Option<(K, V)>;
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize;
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_;  // recency 序（最新→最旧）
}

pub struct TtlCache<K, V> { /* … */ }    // K: Eq + Hash + Clone
impl<K: Eq + Hash + Clone, V> TtlCache<K, V> {
    pub fn new(clock: Arc<dyn Clock>, cap: usize) -> Self;  // cap==0 panic
    pub fn insert(&mut self, k: K, v: V, ttl: Duration);    // expire-after-write；ttl==0 panic
    pub fn get(&mut self, k: &K) -> Option<&V>;             // touch + 惰性过期
    pub fn peek(&mut self, k: &K) -> Option<&V>;            // 不 touch + 惰性过期（需 &mut）
    pub fn peek_mut(&mut self, k: &K) -> Option<&mut V>;
    pub fn contains(&self, k: &K) -> bool;                  // 含僵尸（不判过期）
    pub fn remove(&mut self, k: &K) -> Option<V>;
    pub fn pop_lru(&mut self) -> Option<(K, V)>;
    pub fn purge_expired(&mut self) -> usize;               // 显式清扫，返回回收数
    pub fn expired(&self) -> u64;                           // 累计惰性过期回收数
    pub fn len(&self) -> usize;                             // 物理条数（含僵尸）
    pub fn is_empty(&self) -> bool;
    pub fn capacity(&self) -> usize;
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_;  // 含僵尸，无副作用
}
```

语义拍板（Guava/moka/lru 对表）：

- get=**touch**（刷新 recency）；不 touch 读面=`peek`/`peek_mut`。
- TTL=逐条 expire-after-write（覆盖重置计时）；`now + ttl` 溢出 panic（fail-closed）。
- 双界交互：满时**先清过期再逐 LRU 尾**（过期条目永不把活条目挤出去，当清得动时）。
- 惰性过期：触碰即回收 + `expired` 计数（fail-visible，过期不是静默 None）；`len()`=物理条数（回收前僵尸占容，如实声明）。
- 时钟只消费单调面；**并发面：单线程 `&mut self` 面 + `SyncLruCache`/`SyncTtlCache` 并发薄包装（`&self`+内部 Mutex，可观察语义与单线程版一致；共享面另有**锁内用户代码边界**，见下四条；分片留待命中率证据）**。
- **用户析构在锁外**（#71）：淘汰/过期/覆盖的 `V::drop`，以及 `remove`/`pop_lru`/`retain` 移除 map 条目或取走 arena 键时的 `K::drop`，**不在共享 mutex 内执行**——就**退休面**而言临界区内只做簿记与所有权转移（`HashMap::remove_entry` 取回 map 键，连同 arena 键/值装进退休批），解锁后按显式顺序（map 键 → arena 键 → 值）析构。退休批**零堆分配**（不破「覆盖插入不得分配」锚）。**本条只覆盖析构**——临界区内仍执行键的 `Hash`/`Eq` 与读侧 `V::clone`（见下两条）。
- **poison 契约拆分**（#71）：**锁外**析构 panic **不投毒**缓存（析构发生在临界区之外，缓存状态未受损）；**临界区内** panic 仍投毒并 fail-fast 带因——**包括临界区内用户 trait 的 panic**（键 `Hash`/`Eq`、读侧 `V::clone`）⇒ 键/值实现须 panic-free。
- **插入路径的 `K::clone` 在锁外**（#80）：`SyncLruCache::insert` / `SyncTtlCache::insert` 采**两阶段重验证插入**——先持锁查存在（存在则更新值与 recency，**不 clone**），不存在则**放锁**后 clone map 侧键副本（此刻无锁持有、未发生任何变更），再**重取锁重查**（第二次检查 = 线性化点），仍不存在才以已 clone 的键插入 ⇒ **锁内路径永不调 `K::clone`**。TTL 的第二次存在检查**早于**过期清扫/淘汰（panic 的 clone 不会造成部分变更）；「已有键重置过期且不 clone」保持；单线程 `LruCache` 路径不变。⚠️ 本条**只覆盖 `K::clone`**——锁内仍执行键的 `Hash`/`Eq` 与读路径的 `V::clone`。
- **键 trait 在锁内**（#82，共享面的显式限制）：`K::Hash`、`K::Eq` 与借用键 `Q::Hash`/`Q::Eq` **可能在内部 mutex 下执行**——查表（`idx_of`→`HashMap::get`：`get`/`peek`/`contains`/`remove`/TTL 过期检查/插入存在性检查）、建表项（`map.get` + `map.insert`）、删除（`map.remove`）、以及 `HashMap` 扩容/rehash ⇒ 键实现须**快、不 panic、不得重入同一缓存**。这是「`HashMap` 作权威索引 + 单 mutex」的固有属性：真正的 lock-free 边界需要另一种索引协议（快照候选键 + 锁外哈希 + 槽位代次重验证），属架构重设计；**用指纹替代键身份被否决**（指纹不是等价关系，当身份会静默改 `Eq` 语义）。
- **读路径 `V::clone` 在锁内**（#83，共享面的显式限制）：`get`/`peek` 返 owned ⇒ `V::clone` 在内部 mutex 下执行 ⇒ 值实现须**快、不 panic、不得重入同一缓存**。与 #71 的精确对照：**#71 移出锁的是退休值的析构；读侧 `Clone` 仍在锁内**。「锁外 clone + 重验证」不可安全实现（放锁后槽位可被替换/移除/淘汰/复用 ⇒ 锁外引用不安全），而 `Arc<V>` 存储会改所有权模型（`remove`/`pop_lru` 需新增 `V: Clone` 或改返回类型）⇒ 属公共兼容性决策，留待未来大版本。
- **注入 `Clock` 在锁外**（#101）：`SyncTtlCache` 的全部 `now_instant` 取样都在 cache mutex **之外**——含满插清扫用的时间快照（第二阶段**取锁前**读取，锁内只消费该不可变值）⇒ 用户 `Clock` 实现可在 `now_instant` 内重入本缓存的读面（`len`/`contains`/`get`）而不自锁、panic 也不投毒。代价：取样与取锁之间刚过期的条目留到下次惰性访问/清扫（惰性 TTL 语义，不为更晚的纳秒快照把调用方注入依赖带进锁内）。⚠️ 本条与上三条**分开记**：Clock 在锁外；键 `Hash`/`Eq` 与读路径 `V::clone` **仍在锁内**。
- **既有 API 属性未变**：`remove`/`pop_lru` 仍移动出 owned `V`、**不需要 `V: Clone`**。
- **单线程面区分**：`LruCache`/`TtlCache` 无共享 mutex，但 `Hash`/`Eq` 仍在调用方栈上同步执行。
- 构造签名类型强制「上限必填」（无默认无界构造，BoundedRegistry 同款）；满插入恒逐 LRU 尾（无 Reject 形）。

```rust
use std::sync::Arc;
use std::time::Duration;
use std_ext::cache::{LruCache, TtlCache};
use skeleton::clock::{Clock, MockClock};

let mut c = LruCache::new(2);
c.insert("a", 1);
c.insert("b", 2);
assert_eq!(c.get(&"a"), Some(&1)); // get=touch：a 变最新
c.insert("c", 3); // 满逐 LRU 尾 → 逐 b
assert_eq!(c.get(&"b"), None);

let clock = Arc::new(MockClock::new());
let mut t = TtlCache::new(clock.clone(), 4);
t.insert("k", 1, Duration::from_millis(100));
clock.advance(Duration::from_millis(99));
assert_eq!(t.get(&"k"), Some(&1));
clock.advance(Duration::from_millis(1));
assert_eq!(t.get(&"k"), None);      // 惰性过期：触碰即回收
assert_eq!(t.expired(), 1);
```

实测锚（2026-10-16 批 b4-cache，采集器 `examples/cache_multidim.rs`）：get/peek/insert ns 锚与容量梯度 100/1k/10k 见报告；TtlCache 路径净税 ~22ns = MockClock 读 ~11ns + 过期判定面之和（勿以钟锚核缓存路径成本）。

---

## fsx — 文件系统受控子集

**feature**：常驻，零 feature 门。**依赖面**：纯 std。对标 walkdir（惰性遍历形）/fs_extra（树复制）/tempfile（RAII 临时目录）各取其形，坑面装甲自有。

### walk — 惰性目录遍历

```rust
pub fn walk(root: impl AsRef<Path>) -> Walk;   // builder 即迭代器，首次 next() 才触 IO

pub struct Walk { /* … */ }
impl Walk {
    pub fn max_depth(mut self, depth: usize) -> Walk;   // 根=0；max_depth(0) 只产出根本身
    pub fn min_depth(mut self, depth: usize) -> Walk;   // 默认 0 含根
    pub fn follow_links(mut self, follow: bool) -> Walk; // 默认 false 不穿透（S1 装甲）
    pub fn sort(mut self, sort: Sort) -> Walk;
}
impl Iterator for Walk { type Item = Result<DirEntry, WalkError>; }

pub enum Sort { None, ByName }   // ByName = OsStr 字节序逐目录排序

pub struct DirEntry { /* … */ }
impl DirEntry {
    pub fn path(&self) -> &Path;
    pub fn depth(&self) -> usize;          // 根=0
    pub fn file_type(&self) -> FileType;   // 默认模式不穿透（symlink 即 symlink）
    pub fn metadata(&self) -> io::Result<Metadata>;  // 按跟随策略解读（无二次口径）
}

pub enum WalkError {
    Io { path: PathBuf, err: io::Error },
    Loop { path: PathBuf, ancestor: PathBuf },   // 仅跟随模式可达；命中即报且不下钻
}
```

symlink 环装甲：条目类型恒取 `read_dir` 不穿透类型；跟随模式全目录身份比对（Unix dev+ino / Windows canonicalize 路径）建祖先栈。**有意分歧一处**：walkdir 根路径恒穿透（文档特例），本域根路径穿透性同样遵循 `follow_links`——特例即坑，一致性优先。根不存在在首次 `next()` 以显式错误上浮（构造零 IO）；根失败只上浮一次，后续 `next()` 恒 None。

### copy_tree / rm_all / temp_dir_in / atomic_write

```rust
pub fn copy_tree(src: impl AsRef<Path>, dst: impl AsRef<Path>, links: SymlinkPolicy)
    -> Result<CopyReport, CopyError>;

pub enum SymlinkPolicy { Follow, Preserve }
// Preserve：Unix 悬空链接原样保留；Windows 保链需可解析出目标类型，悬空显式报错（平台差异申报）

pub struct CopyReport { pub dirs: usize, pub files: usize, pub symlinks: usize, pub bytes: u64 }

pub enum CopyError {
    Prepare { path: PathBuf, err: io::Error },                       // 零中间态
    Partial { report: CopyReport, path: PathBuf, err: io::Error },   // report=已处理量快照
    Cycle { report: CopyReport, path: PathBuf, ancestor: PathBuf },
}

pub fn rm_all(path: impl AsRef<Path>) -> Result<(), RmError>;
pub struct RmError { pub path: PathBuf, pub err: io::Error }

pub fn temp_dir_in(root: impl AsRef<Path>, prefix: &str) -> io::Result<TempDir>;
pub struct TempDir { /* … */ }
impl TempDir {
    pub fn path(&self) -> &Path;
    pub fn close(self) -> io::Result<()>;   // 显式清理（Drop 为兜底，错误只记录）
}
impl Drop for TempDir { /* rm_all；失败 fail-visible 记 stderr */ }

pub fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()>;  // fsx::atomic
```

- `copy_tree`：`dst` 必须不存在（create_dir 独占语义）；首错即停，已处理量以计数显式申报（禁静默半途）；遍历按文件名排序——报错点确定可复现；文件复制=`fs::copy` 本体语义（权限保留，时间戳不承诺）。
- `rm_all`：引擎=std 硬化原语（CVE-2022-21658 防护+Windows DELETE_PENDING 等待）；加 Windows 只读属性装甲（首次 `PermissionDenied` 清只读后重试恰一轮，仍败原错误上浮）。Unix 只读目录 EACCES 属 OS 语义如实上浮（不自动 chmod）。
- `temp_dir_in`：命名=前缀+pid+进程内原子序号+挂钟纳秒（同进程不撞/跨进程 pid 隔离/pid 回收由纳秒兜底）；创建走 `create_dir` 独占语义。落点由调用方给定（禁系统 TEMP，工作区内自选根）。`prefix` 按 `Path::join` 语义拼接，含 `..` 可越出 root（与 tempfile 同行为，调用方自负）。
- `atomic_write`：同目录临时件（write+`sync_all`）→ rename 原子替换——读者恒见旧内容或新内容，恒无半写中间态。armor：**不先删旧**（删旧引入目标缺失窗口毁掉原子性）；`to` 为已存在目录恒败；同目录临时件从根上排除跨分区 rename；错误路径自清临时件后以原错误上浮；父目录必须已存在（不隐式 mkdir）；新文件=umask 新建语义**不继承**原 mode/ACE；并发写同目标=最后赢（rename 原子线性化）。崩溃在 rename 前留 `atomic-{pid}-{seq}{nanos}` 临时件（可识别，手扫/启动扫）。

```rust
use std_ext::fsx::{walk, Sort};

let mut it = walk("definitely/missing/root").max_depth(2).sort(Sort::ByName);
assert!(it.next().unwrap().is_err()); // 构造零 IO；根不存在显式上浮
```

```rust,no_run
use std_ext::fsx::temp_dir_in;

let tmp = temp_dir_in("target/tmp", "myapp-").unwrap();
std::fs::write(tmp.path().join("scratch.txt"), b"x").unwrap();
drop(tmp); // RAII：Drop 即删树
```

```rust,no_run
use std_ext::fsx::atomic_write;

atomic_write("app.json", br#"{"port":8080}"#).unwrap();
```

### 实测锚（源码 doc 口径）

- walk 分配声明：每 entry 恰 1 个自有分配（yield 的 owned DirEntry 路径本体）+每目录帧 +1 帧路径 +排序模式 +1/entry 收集缓冲（采集器 `examples/fsx_multidim.rs`，报告 2026-10-16-b3-fsx）。
- `atomic_write` 分配：每调用自有分配=临时件名 String 1 + 路径拼接 PathBuf 2；`bytes` 借用零拷贝（`examples/config_atomic_multidim.rs` 事件口径）。

---

## encode — 编解码

**feature**：常驻，零 feature 门。**依赖面**：纯 std。同信任边界哲学：显式策略、非法输入显式 Err 禁静默。

### hex（Base16，RFC 4648 §8）

```rust
pub enum HexCase { Lower, Upper }

pub enum HexError {
    OddLength { len: usize },
    InvalidDigit { at: usize },
}

pub fn hex_encode(bytes: &[u8], case: HexCase) -> String;   // 输出长度恒为输入两倍
pub fn hex_decode(s: &str) -> Result<Vec<u8>, HexError>;    // 大小写不敏感（RFC §8）
```

### base64（RFC 4648 §4/§5）

```rust
pub enum Base64Alphabet { Std, UrlSafe }   // 两表不混用：解码非本表字符显式 Err

pub enum Base64Error {
    InvalidByte { at: usize, byte: u8 },
    InvalidLength { len: usize },          // len % 4 == 1
    InvalidPadding { at: usize },
    NonCanonicalTail,                      // 尾字符未用位非零（拍板拒绝；base64 crate 0.23 默认同向）
}

pub fn base64_encode(bytes: &[u8], alphabet: Base64Alphabet, padding: bool) -> String;
pub fn base64_decode(s: &str, alphabet: Base64Alphabet) -> Result<Vec<u8>, Base64Error>;
```

- 非字母表字符（含空白/NUL）恒 Err（RFC §3.3——宽松忽略即 covert channel）；pad 形态显式验证（`=` 只许尾部 0/1/2 个且与尾组形态匹配）；no-pad 输入按 §3.2 合法变体接受。编码端尾组未用位恒零填充（canonical 承诺）。错误优先序：InvalidLength → InvalidPadding → InvalidByte → NonCanonicalTail。

### BOM 与 UTF-16

```rust
pub fn strip_bom(bytes: &[u8]) -> &[u8];   // 只剥一层 UTF-8 BOM；零分配切片

pub enum UnicodeScheme { Utf8, Utf16Le, Utf16Be, None }
pub fn detect_unicode_scheme(bytes: &[u8]) -> UnicodeScheme;  // 仅看前 2-3 字节，不做内容推断

pub enum Utf16Endianness { Le, Be }   // 显式选择，无默认（无 BOM 的 UTF-16 端序不可判定）

pub enum Utf16Error {
    OddLength { len: usize },
    BomMismatch { found: Utf16Endianness },   // BOM 与参数矛盾=Err，不静默按 BOM 翻转
    UnpairedSurrogate { at: usize },          // Strict 哲学：禁 U+FFFD 静默替换
}

pub fn encode_utf16_bytes(s: &str, endianness: Utf16Endianness) -> Vec<u8>;  // 不写 BOM
pub fn decode_utf16_bytes(bytes: &[u8], endianness: Utf16Endianness) -> Result<String, Utf16Error>;
```

```rust
use std_ext::encode::{base64_encode, hex_encode, Base64Alphabet, HexCase};

assert_eq!(hex_encode(b"foobar", HexCase::Upper), "666F6F626172");
assert_eq!(base64_encode(b"foobar", Base64Alphabet::Std, true), "Zm9vYmFy");
assert_eq!(base64_encode(b"foob", Base64Alphabet::Std, false), "Zm9vYg");
```

```rust
use std_ext::encode::{encode_utf16_bytes, Utf16Endianness};

assert_eq!(encode_utf16_bytes("a中", Utf16Endianness::Le), vec![0x61, 0x00, 0x2D, 0x4E]);
assert_eq!(encode_utf16_bytes("a中", Utf16Endianness::Be), vec![0x00, 0x61, 0x4E, 0x2D]);
```

### 实测锚（2026-10-16 批 b1-encode，源码 doc 口径）

`hex_encode` ~1.37µs @1KiB / ~610-720 MiB/s @≥64KiB；`base64_encode/decode` ~710-800 MiB/s @≥64KiB；`strip_bom`/`detect_unicode_scheme` 零分配 ~1.5ns 级规模钝感。

---

## id — 时间有序 id

**feature**：常驻，零 feature 门。**依赖面**：纯 std（时间面复用 [`clock`](#clock--时间源)）。UUIDv7 确定性变体（RFC 9562 §5.7 布局逐位对表）。

```rust
#[derive(Clone)]   // Clone 廉价（Arc 句柄）；Send + Sync
pub struct IdGen { /* … */ }
impl IdGen {
    pub fn new(fingerprint: u64, clock: Arc<dyn Clock>) -> Self;  // 同指纹复用进程内单权威
    #[must_use]
    pub fn gen(&self) -> Id;          // Id 值（数值面 as_u128），恒零分配
    #[must_use]
    pub fn fingerprint(&self) -> u64; // 诊断/分片对账用
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id(u128);   // 数值面承载排序/持久化往返（as_u128/from_u128）
impl Id {
    pub const fn as_u128(self) -> u128;
    pub const fn from_u128(value: u128) -> Self;
}
impl fmt::Display for Id { /* RFC 9562 §5.1 8-4-4-4-12 小写 hex 连字形 */ }

pub enum IdParseError { Shape, Hex }  // 非 8-4-4-4-12 形 / hex 位非法
impl std::str::FromStr for Id {       // 连字形解析（hex 位大小写不敏感）
    type Err = IdParseError;
}
```

布局（128bit 大端）：`48bit unix_ts_ms | 4bit ver=0111 | 12bit rand_a | 2bit var=10 | 62bit rand_b`。确定性变体映射：`rand_a` = 同毫秒单调 counter（基址=指纹固定派生 `指纹 >> 13 & 0xFFF`），`rand_b` = 机器指纹低 62 位。

排序保证：同指纹（全部句柄）`gen()` 数值**严格递增**（线性化点=锁内装配）；跨指纹=毫秒级时间序，同毫秒内无全局序；时钟回拨=停走不倒退（时间戳复用 `last_ms`，counter 前进保序）。

姿态（fail-closed/拍板点）：

- **同毫秒配额（4096−基址）耗尽 = fail-closed panic**（拒绝静默借位与等待活锁；等待与 MockClock 冻结语义/零自旋纪律冲突；持续率超 4M id/s 应分片指纹）。
- **counter 基址=指纹固定派生**（回放可复现优先于不可预测性）。
- **同指纹单权威注册表**（进程级 `OnceLock<Mutex<HashMap>>`）：同指纹 `IdGen::new` 复用同一权威——跨句柄严格递增、零撞 id（生态多生成器同域撞 id 的失败类结构性消灭）。句柄不可手搓，只能经注册表获得。
- 48bit 时间戳越界 / epoch 前挂钟：fail-closed panic。

```rust
use std::sync::Arc;
use std::time::Duration;

use skeleton::clock::MockClock;
use std_ext::id::IdGen;

let clock = Arc::new(MockClock::new());
clock.advance(Duration::from_millis(1));
let gen = IdGen::new(0xC0FFEE, clock.clone());

let a = gen.gen();
let b = gen.gen();
assert!(b > a, "同毫秒：counter 前进，严格递增");

clock.advance(Duration::from_millis(1));
let c = gen.gen();
assert!(c > b, "跨毫秒：时间序");

assert_eq!((a.as_u128() >> 76) & 0xF, 7, "RFC 9562 version 7");
let s = a.to_string();
assert_eq!(s.len(), 36, "8-4-4-4-12 标准连字形");
assert_eq!(s.parse::<std_ext::id::Id>().unwrap(), a, "FromStr roundtrip");
```

### 实测锚（2026-10-17，采集器 `examples/id_multidim.rs`，源码 doc 口径）

`gen()` 恒零分配（`Id` 值面 Copy 无堆）；非竞争 gen：SystemClock 端到端 ~37ns（净算术+权威锁 ~21ns 差值口径）；批量梯度 ~34ns/id 持平；`to_string()`（Display 字符串面）~590ns（展示面非热路径）；p50/p99 ~100ns。

---

## iter — 迭代器扩展

**feature**：常驻，零 feature 门。**依赖面**：纯 std。itertools 语义子集，只收编 **5 件通用最高频件**（收窄拍板，禁全家桶）；全件零分配惰性组合子。

```rust
pub fn zip_longest<A, B>(a: A, b: B) -> ZipLongest<A::IntoIter, B::IntoIter>
where A: IntoIterator, B: IntoIterator;

pub enum EitherOrBoth<A, B> { Both(A, B), Left(A), Right(B) }

pub fn interleave<A, B>(a: A, b: B) -> Interleave<A::IntoIter, B::IntoIter>
where A: IntoIterator, B: IntoIterator<Item = A::Item>;

pub fn minmax<I>(iter: I) -> MinMaxResult<I::Item>
where I: IntoIterator, I::Item: Ord;

pub enum MinMaxResult<T> { NoElements, OneElement(T), MinMax(T, T) }
impl<T: Clone> MinMaxResult<T> { pub fn into_option(self) -> Option<(T, T)>; }

pub fn dedup<I>(iter: I) -> DedupBy<I::IntoIter, fn(&I::Item, &I::Item) -> bool>
where I: IntoIterator, I::Item: PartialEq;

pub fn dedup_by<I, F>(iter: I, eq: F) -> DedupBy<I::IntoIter, F>
where I: IntoIterator, F: FnMut(&I::Item, &I::Item) -> bool;

pub fn process_results<I, F, T, E, R>(iterable: I, processor: F) -> Result<R, E>
where I: IntoIterator<Item = Result<T, E>>,
      F: FnOnce(ProcessResults<I::IntoIter, E>) -> R;

pub struct ProcessResults<'a, I, E> { /* Iterator<Item = T> */ }
```

语义锚（itertools 0.15 源码逐件对表）：

- `zip_longest`：不齐时**显式三态**而非静默截断（std `zip` 的反面）；内部 `Fuse` 双包装（非 fused 底源复活不产生错流）。
- `interleave`：`a₀ b₀ a₁ b₁ …`；一侧耗尽后**另一侧余量如实追加**（非"短尽即止"，`interleave_shortest` 未收编）。
- `minmax`：单遍成对法（2 元素 3 比较）；min 取首现、max 取末现；全等值多元素 = `MinMax(v, v)`（按**元素计数**分流，不误报 `OneElement`）。
- `dedup_by`：**连续**去重保首现（`[1,2,1]` 全保留，非全量 unique）；谓词参数序 `(previous, candidate)`，true=candidate 视为重复丢弃。
- `process_results`：首 `Err` 短路（短路后底层不再被拉）；对照 `flatten()` 对 Result **静默吞 Err**。size_hint lower 恒 0（Err 面不可预知）。
- size_hint 公式化（zip_longest=max、interleave=add）+ `ExactSizeIterator` 只在公式精确时声称（DedupBy 永不声称）；全部适配器构造零推进（惰性纪律）。

std 邻名对照（防语义漂移装甲）：`iter::dedup_by` vs `Vec::dedup_by`（惰性适配器 vs 原地收缩）；`iter::minmax` vs `std::cmp::minmax`（整迭代器 vs 两值）；`iter::zip_longest` vs `Iterator::zip`（显式三态 vs 静默截断）；`iter::process_results` vs `Iterator::flatten`（首 Err 短路上抛 vs 静默吞）。

```rust
use std_ext::iter::{minmax, process_results, zip_longest, EitherOrBoth, MinMaxResult};

let got: Vec<_> = zip_longest([1, 2, 3], ["a"]).collect();
assert_eq!(got, [EitherOrBoth::Both(1, "a"), EitherOrBoth::Left(2), EitherOrBoth::Left(3)]);

assert_eq!(minmax([3, 1, 2]), MinMaxResult::MinMax(1, 3));
assert_eq!(minmax([7, 7, 7]), MinMaxResult::MinMax(7, 7));

let oks: [Result<i32, &str>; 3] = [Ok(1), Ok(2), Ok(3)];
let doubled: Result<Vec<i32>, &str> = process_results(oks, |it| it.map(|x| x * 2).collect());
assert_eq!(doubled, Ok(vec![2, 4, 6]));
```

---

## hash — 稳定摘要

**feature**：常驻，零 feature 门。**依赖面**：纯 std。核心卖点不是算法强度而是**稳定承诺**：`HASH_VERSION` 冻结，输出值跨本 crate 版本、跨平台、跨 Rust 工具链**永不变**；升级=新函数名 + 版本号递增，禁原地换算法。

**红线**：非加密摘要不承诺抗碰撞（RFC 9923 §1.2 / xxhash spec 明文）；对抗性场景用加密摘要（sha2 等按 ADR 引依赖），本域类型不与加密摘要混用。

```rust
pub const HASH_VERSION: u32 = 1;
// 1 = { stable64 = FNV-1a 64（RFC 9923），xxh64 = XXH64（spec v0.2.0、seed 0）}

pub fn stable64(data: &[u8]) -> u64;
pub fn stable64_io(mut read: impl Read) -> io::Result<(u64, u64)>;  // 流式到 EOF，返回 (摘要, 字节数)
pub fn xxh64(data: &[u8]) -> u64;                               // 高吞吐优先（4-lane 累加器）
```

- `stable64`：FNV-1a 64（先 XOR 后乘，`basis 0xcbf29ce484222325`/`prime 0x100000001b3`）；空输入返回 offset basis；零分配。
- `stable64_io`：分块不改变结果；按 std 约定重试 `Interrupted`、其余错误原样传播；字节计数 checked 累加超 u64 显式 Err；零堆分配（16 KiB 栈缓冲）。
- `xxh64`：spec v0.2.0、seed 恒 0；lane 一律小端读（`from_le_bytes`，禁 `from_ne_bytes`）；空输入=`0xEF46DB3751D8E999`。
- 不提供任何截断/归约 helper（禁 `hash % 2^k` 误用面；桶归约采用方自做且应全宽或 XOR folding）。

```rust
use std_ext::hash::{stable64, xxh64};

// RFC 9923 §8.3 官方测试向量
assert_eq!(stable64(b"foobar"), 0x8594_4171_f739_67e8);
assert_eq!(stable64(b""), 0xcbf2_9ce4_8422_2325);
// python-xxhash（参考实现绑定）README 发布值
assert_eq!(xxh64(b"xxhash"), 0x32DD_3895_2C4B_C720);
```

### 实测锚（2026-10-16 批 b2-hash，源码 doc 口径）

`stable64` 零分配、~1160-1190 MiB/s @≥1KiB、16B 热缓存 ~5.5ns；`xxh64` 零分配、~17.5 GiB/s @64KiB（≥64MiB 回落 ~11-12 GiB/s 属 DRAM 带宽界非实现缺陷）、16B ~4.1ns。

---

## json — JSON 双模解码

**feature**：`json`（std-ext 侧门控——显式开 `json` feature，默认关）。**依赖面**：`serde_json`（经 `json` feature 门，开则进树）；其余各域本体纯 std。本域随 P1-9 自 `xray-conf` 上浮，行为面对齐 Go `json_reader` 的 JSONC 语义。

```rust
pub enum JsonMode { Strict, Tolerant }

pub struct JsonParseError {
    pub line: u32,      // 1 起；无位置语义为 0
    pub column: u32,    // 1 起；无位置语义为 0
    pub message: String,
}

pub const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;   // 64 MiB

pub fn decode<R: Read>(reader: R, mode: JsonMode) -> Result<serde_json::Value, JsonParseError>;
pub fn decode_str(s: &str, mode: JsonMode) -> Result<serde_json::Value, JsonParseError>;
pub fn decode_slice(s: &[u8], mode: JsonMode) -> Result<serde_json::Value, JsonParseError>;
```

- `JsonMode::Strict`：serde_json 原生严格解析（RFC 8259）——注释即语法错。
- `JsonMode::Tolerant`：先剥离 `//` 行注释与 `/* */` 块注释再解析；**字符串字面量内的注释标记保留**（含转义引号后的内容）；未闭合块注释容忍至输入末尾（剥离到末尾后解析）。
- `MAX_INPUT_BYTES`（64 MiB）：读满即停——reader 路径最多读 `MAX_INPUT_BYTES + 1` 字节即判超限，**超限报错而非截断成功**；`decode_str`/`decode_slice` 超限同样报错。
- 错误位置：`line`/`column` 1 起（提取自 serde_json）；**无位置语义的失败（读取失败 / 输入超限 / 非 UTF-8）为 0**。

```rust
use std_ext::json::{decode_str, JsonMode};

let v = decode_str(r#"{"a": 1}"#, JsonMode::Strict).unwrap();
assert_eq!(v["a"], 1);

// 容忍模式收注释；字符串内注释标记不动
let v = decode_str("// c\n{ \"a\": 1 }", JsonMode::Tolerant).unwrap();
assert_eq!(v["a"], 1);
```

行为断言见 `std-ext/tests/json.rs`（严格拒注释 / 容忍收注释 / 字符串内标记保留 / 转义引号 / 未闭合块注释 / 超限报错非截断 / 行列与 serde_json 一致 / slice 与 reader 一致 / 非法 UTF-8 报无位置错误）。

---

## config — 配置装配

**feature**：文件面挂 `config-json`/`config-toml`。**依赖面**：`config-json` 增 serde/serde_json；`config-toml` 叠加 toml。**批 1 契约真下沉**（2026-10-18；ADR 0001）：配置契约 `EnvSource`/`StdEnv`/`MockEnv`/`ConfigPort`/`ConfigError` 现住 `skeleton::config`（纯 std 零门）；本 crate 的 `config` 模块只余文件行为件（分层装配/原子保存/TOML 翻译）。

### 覆盖链三层（谁负责什么）

```text
1. 底座（采用方）：默认值 / 文件加载结果，构造 Self 时自带——格式解析、字段默认全在契约之外
2. env 覆盖（契约层 skeleton::config）：ConfigPort::load 经 EnvSource 读环境，显式出现的键覆盖底座值
3. 校验（契约层 skeleton::config）：产出 Config 前完成——缺必填键报 Missing，非法值报 Invalid，解析失败报 Parse
```

### 契约面（`skeleton::config`，零 feature、纯 std）

```rust
pub enum EnvError { Missing, NotUnicode(std::ffi::OsString) }  // 非原始字节随错误保真（≠ Missing）

pub trait EnvSource: Send + Sync {
    fn get(&self, key: &str) -> Result<String, EnvError>;
    // Ok ⟺ 键存在且值为合法 Unicode；NotUnicode ≠ Missing（键在值非 Unicode 显式报）；
    // 大小写语义实现自理，契约层不归一
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StdEnv;   // std::env::var_os 两步判定：缺失=Missing，值非 Unicode=NotUnicode（禁 lossy）

#[derive(Debug, Clone, Default)]
pub struct MockEnv;  // 测试替身：字节精确查找（恒区分大小写）；唯 Missing 一态可达
impl MockEnv {
    pub fn new() -> Self;
    pub fn with<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self;
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V);
    pub fn vars(&self) -> &HashMap<String, String>;
}

pub trait ConfigPort {
    type Config;
    fn load(&self, env: &dyn EnvSource) -> Result<Self::Config, ConfigError>;
    // 缺必填键必报 Missing（禁静默默认值），语义非法报 Invalid，解析失败报 Parse
}

pub enum ConfigError {
    Missing { key: String },
    NotUnicode { key: String, value: std::ffi::OsString },  // env 值非 Unicode 映射面（原始值保真）
    Invalid { key: String, reason: String },
    Parse { key: String, source: Box<dyn std::error::Error + Send + Sync + 'static> },
    File { path: std::path::PathBuf, source: std::io::Error },
    Serialize { source: Box<dyn std::error::Error + Send + Sync + 'static> },
}
```

`ConfigPort` 是规范接口（零实现纪律）：契约面零真实解析、零格式词汇；`dyn EnvSource` 对象安全编译期钉死。与 config-rs/figment 的分工：它们做第 1 层「文件 → 结构体」，本契约规范第 2、3 层。

### ConfigFile — 分层装配 + 原子保存（`config-json`）

```rust
pub struct ConfigFile<C> { /* 层叠 serde_json::Value，低→高 */ }
impl<C> ConfigFile<C> {
    pub fn new() -> Self;
    pub fn layer(mut self, value: serde_json::Value) -> Self;      // 显式原始值层
    pub fn default_layer(self, value: &C) -> Result<Self, ConfigError>
    where C: serde::Serialize;                                      // 链图第 1 层
    pub fn file_layer(self, path: impl AsRef<Path>) -> Result<Self, ConfigError>;  // 第 2 层
    pub fn load(self) -> Result<C, ConfigError> where C: serde::de::DeserializeOwned;
    pub fn save(value: &C, path: impl AsRef<Path>) -> Result<(), ConfigError>
    where C: serde::Serialize;                                      // Pretty + 原子写
}
impl<C> Default for ConfigFile<C> { /* new() */ }
```

分层链四层：`default（代码构造）→ file（文件底座）→ env（ConfigPort 已有段）→ explicit（代码后覆盖）`。default/file/explicit 原始值层归本面；**env 层不进本面**（字符串 env 值 → 字段类型的解析是类型化职责——原始值面代劳会把 `"8080"` 串进数字字段）。装配姿势：`file` 层产物作底座喂采用方 ConfigPort 实现。

合并语义（折叠序=层序）：对象∩对象递归合并（高层键覆盖/新增，同层内后键赢）；高层非对象（标量/数组/null）整体替换——数组不按下标合并、null 是显式值（config-rs 式魔法判否，fail-visible）。

`save` = Pretty 序列化 + [`fsx::atomic_write`](#fsx--文件系统受控子集)（崩溃安全）。**保存=全量重序列化**：注释与键序不保留（前置声明）；并发保存最后赢。serde_json 序列化语义如实声明：整数键映射被静默字符串化、非有限浮点被静默写 null——需挡这两类值的采用方自理。

### TOML 面（`config-toml`，叠在 config-json 上）

```rust
impl<C> ConfigFile<C> {   // #[cfg(feature = "config-toml")]
    pub fn file_layer_toml(self, path: impl AsRef<Path>) -> Result<Self, ConfigError>;
}

pub fn load_toml<C: serde::de::DeserializeOwned>(path: impl AsRef<Path>) -> Result<C, ConfigError>;
pub fn save_toml<C: serde::Serialize>(value: &C, path: impl AsRef<Path>) -> Result<(), ConfigError>;
```

`load_toml`/`save_toml`：类型化直读直写单文件面（Datetime 全保真；TOML 文档须为表根，标量根序列化失败报 Serialize）。`file_layer_toml` 把 TOML 折进分层链——**TOML Datetime 在统一值面无表示，显式报错**（禁静默降级成私有标记映射）；需要 Datetime 全保真走 `load_toml`。

```rust,no_run
use serde::{Deserialize, Serialize};
use skeleton_infra::config::ConfigFile;

#[derive(Serialize, Deserialize)]
struct App { port: u16 }

let cfg: App = ConfigFile::new()
    .default_layer(&App { port: 8080 }).unwrap() // 第 1 层：代码构造
    .file_layer("app.json").unwrap()             // 第 2 层：{"port": 9090}
    .load().unwrap();                            // 高层覆盖 → port = 9090

ConfigFile::save(&cfg, "app.json").unwrap();     // Pretty + 原子写
```

---

## lint — 配置后处理阶段编排

**feature**：常驻，零 feature 门。**依赖面**：纯 std。住契约层（`skeleton::lint`）：消费形态是「拿到配置结构体后跑校验」，无 IO / 无线程池 / 无计时器——采用方单开 `udf-skeleton` 即可用，不必为配置校验把运行时拖进依赖树（判据同 `skeleton::config`）。

### 契约面

```rust
pub trait LintStage<C>: Send + Sync {
    fn name(&self) -> &'static str;                        // 诊断名单 + 错误归属
    fn run(&self, cfg: &mut C) -> Result<(), LintError>;   // 锁外执行；可改写 cfg
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LintError { /* stage: Option<&'static str>, message: String */ }
impl LintError {
    pub fn new(message: impl Into<String>) -> Self;        // stage 由编排器补齐
    pub fn stage(&self) -> Option<&'static str>;
    pub fn message(&self) -> &str;
}
impl std::fmt::Display for LintError {}                    // "{stage}: {message}" / "{message}"
impl std::error::Error for LintError {}

pub fn register_stage<C: 'static>(stage: Arc<dyn LintStage<C>>);
pub fn clear_stages<C: 'static>();                             // 槽位立即出表；阶段对象随最后一份在飞快照析构（锁外）
#[must_use]
pub fn registered_stages<C: 'static>() -> Vec<&'static str>;   // 注册序，重复名原样保留（诊断面）
pub fn post_process<C: 'static>(cfg: &mut C) -> Result<(), LintError>;
```

一次调用的四步：查表（`REGISTRY` 未初始化 → 直接 `Ok`，连锁都不碰）→ 快照（读锁内 `get` + `Arc::clone`，仅原子自增，随即放锁）→ 锁外按注册序执行 → 首个 `Err` 短路并补归属。

### 语义契约（`tests/lint.rs` 逐条钉死）

| 语义 | 测试 |
|---|---|
| 空表 → `Ok(())`；注册表未初始化连锁都不碰 | `empty_registry_is_ok` |
| 按注册序执行；名单同序；重复名原样（无去重、不校验唯一性） | `stages_run_in_registration_order` / `duplicate_names_are_reported_verbatim` |
| 首个 `Err` 短路（后续不跑，已发生的改动保留） | `first_error_short_circuits_and_keeps_prior_mutations` |
| 阶段可改写配置（`&mut C`，不只校验） | `stages_can_rewrite_config` |
| 类型隔离（不同 `C` 的阶段表互不可见） | `registries_are_isolated_per_config_type` |
| 错误归属：`stage` 为空则补失败阶段名；`Display` = `"stage: message"` | `orchestrator_fills_missing_stage_attribution` |
| 已带归属不覆盖（嵌套 `post_process` 的错误不被外层顶替） | `nested_error_keeps_its_own_stage_attribution` |
| 阶段体内注册 = 下一拍才生效（快照语义），且不死锁 | `reentrant_registration_is_snapshot_scoped` |
| 阶段体内可递归 `post_process` 其他类型 | `stage_can_recursively_post_process_other_types` |
| 阶段 panic 原样传播；注册表与全局锁不受影响 | `stage_panic_propagates_and_registry_survives` |
| 阶段**不被独占锁串行化**（barrier：两线程同时在场；覆盖 `RwLock` 读侧并行，**不**鉴别「锁外执行」——读锁共享，锁持在快照期也凑得齐） | `stages_are_not_serialized_by_an_exclusive_lock` |
| 阶段 `Drop` 可重入注册表（= 锁外析构的鉴别证据：体内要拿写锁） | `stage_drop_can_reenter_registry` |
| 阶段 `Drop` 内 panic 原样传播，注册表仍可读写 | `stage_drop_panic_leaves_registry_usable` |
| 锁中毒恢复（持写 guard 时 panic 真毒化 → `into_inner` 后仍可读写） | `lint.rs` 内 `#[cfg(test)] mod tests::registry_recovers_from_a_poisoned_lock` |
| 同类型递归合法但无防护（终止条件由阶段自设） | `same_type_recursion_is_allowed_and_self_limited` |
| `clear_stages` 不取消在飞那一拍；下一拍不再执行该阶段 | `clear_stages_does_not_cancel_the_in_flight_tick` |
| 并发 `post_process`（4 线程 × 200 拍，各自实例）全成功 | `concurrent_post_process_over_distinct_configs` |
| 8 线程并发注册不丢阶段 | `concurrent_registration_keeps_every_stage` |
| `clear_stages` 后名单空、不再跑阶段、幂等 | `clear_stages_empties_registry_and_stops_running` |

### 坑面 armor（生态事实 → 处置，11 条，doc 与测试双处声明）

| 生态事实 | 本件处置 |
|---|---|
| 锁内跑用户回调 = 真实死锁（tauri-apps/plugins-workspace#3394；std `Mutex`/`RwLock` 皆非重入） | 锁内只做 `get` + `Arc::clone`，快照后放锁；阶段可在体内注册 / 递归调用。鉴别证据 = 体内要拿锁的那两条（`reentrant_registration_is_snapshot_scoped` / `stage_drop_can_reenter_registry`） |
| **`Drop` 也是用户代码**（析构可再入锁 → 语句临时量逆序析构会把最后一个阶段的 `Drop` 拖进写锁内） | `clear_stages` 两步式：`remove` 的返回值出锁后再 `drop`（`stage_drop_can_reenter_registry`）；`Drop` 内 panic 原样传播不污染注册表（`stage_drop_panic_leaves_registry_usable`） |
| 每次调用 clone 整表（生态 `get_cloned`）→ 每拍一次堆分配（本仓 `notify` 曾因每拍 `collect` 被修，CHANGELOG 5753834） | 槽位存 `Arc<Vec<..>>`，改写走 `Arc::make_mut`（COW 只在**注册期**、且只在有在飞快照时克隆）；`post_process` 只原子自增 → 稳态零堆分配 |
| `TypeId` 要求 `'static`，且跨编译单元不稳定（std 文档） | 全部入口 `C: 'static`；**键仅进程内有效**（禁持久化 / 跨进程）；借用型 `C` 由 `compile_fail` doctest 钉住编译期拒绝 |
| 全局可变状态 + 并行测试线程 = flaky（Rust Internals「Global Registration」；`serial_test` 存在即证据） | 测试隔离靠**不同 `C` 类型**（每测试私有 newtype）+ 同类型内 `clear_stages`，测试仍并行跑 |
| 锁中毒让注册表永久不可用（生态 `global_registry` 直接返回 `LockError` = 静默退化） | `lock().unwrap_or_else(PoisonError::into_inner)` 恢复（锁内无可撕裂状态）；真毒化覆盖在 crate 内 `lint::tests::registry_recovers_from_a_poisoned_lock`（公开 API 造不出毒化），doc 写明理由 |
| 错误只带 message → 排障不知哪个阶段 | `LintError.stage` 由编排器按失败阶段 `name()` 补；已带归属不覆盖 |
| 运行期注册的可见性未写明 → 使用方以为立即生效 | 文档 + `reentrant_registration_is_snapshot_scoped` 双处声明「快照语义」 |
| 编译期/链接期注册（inventory / linkme）需 `#[ctor]` 式构造，链接序不可控、无运行期动态注册 | 选运行期注册表（`OnceLock` + `RwLock`）；代价 = 每拍一次全局读锁 + 一次原子自增 |
| 锁类型（`Mutex` vs `RwLock`）不是口味 | 实测选出 `RwLock`（快照走共享读锁、注册/清场走独占写锁）；对照与前值留档 `verify/2026-10-29-lint-lock-showdown/`（前值**非当前树可复跑**），见下「锁策略对决」 |
| 阶段 panic | 不 `catch_unwind`、不吞——原样传播；锁不在执行期持有故不毒化全局态 |

`OnceLock` 而非 `LazyLock`：需要「从未注册」这个**可区分状态**——`post_process` 走 `REGISTRY.get() == None` 直接返回，连锁都不碰（快路径零成本）。

### 收窄声明

- 无卸载语义：只能整表 `clear_stages`（单点撤销请自带 `Arc` 句柄 + 开关位）。
- 无优先级 / 分组 / 依赖排序：唯一顺序 = 注册序。
- **同类型递归无防护**：阶段体内 `post_process::<C>` 合法（不死锁）但会重跑本阶段，终止条件由阶段自设（无界递归 = 栈溢出，不是 `Err`）。
- `name()` 不做唯一性校验：重复名合法，排障按注册序列数位。
- 注册表自身线程安全；**`C` 不必 `Send`**——跨线程并发时由调用方保证各自 `&mut C` 独占。
- **无阶段时的多线程吞吐是显式登记的上限**：读路径 = 一次全局读锁 + 一次原子自增（~17 ns/拍，本批 17.0），0 阶段 4 线程只有 ~0.30x（全局锁争用退化）。高吞吐采用方两条路：①各自 config 类型分槽（表按 `TypeId` 天然隔离，不同 `C` 不争同一把锁）；②每拍不必重复 lint（配置装载一次跑一次）。**不提供** per-线程表缓存 / generation 计数器：TLS 缓存会让旧快照滞留每线程，内存维度受损，而调用点是配置装载期冷路径。

### 实测（**单批运行**：2026-10-29，本机 16 逻辑核，Windows 10 x64 / rustc 1.96.0；`cargo run --release -p udf-skeleton --example lint_multidim`；std `Instant` 粗测）

口径：**一次干净运行**，原文留档 `verify/2026-10-29-lint-fix-round/example.txt`——本表所有数字以该文件为准。粗测数字供判级、不供微基线；跨批次比较前先看漂移量级（同机同 example 正常波动 10%~30%）。

| 维度 | 读数（`RwLock` 版） |
|---|---|
| 稳态分配事件（0 / 1 / 3 / 5 阶段，100k 拍窗内计数） | 全档 **0.00 事件/次**（阶段命中 0 / 100k / 300k / 500k，证明阶段真跑到而非空表假零） |
| `post_process` ns/次 | 空表 **17.0**、1 阶段 **28.0**、5 阶段 **38.3** |
| `register_stage` ns/次（`Arc` 构造在计时外） | 39.1（写锁路径，不受读侧改造影响） |
| `registered_stages`（10k 名，含名单 `Vec` 分配，诊断面） | **~24–52 µs**（本批 49.6；含 10k 名 `Vec` 分配，**方差大，不供微基线**） |
| 规模梯度 1 / 10 / 100 / 1000 阶段 | 28.0 / 49.7 / 254.9 / 2656.2 ns/次；**边际 2.41 → 2.28 → 2.67 ns/阶段（10→100 段 vs 100→1000 段漂移 17%，近线性）** |
| 并发吞吐（~20 µs 计算阶段，锁成本 <1%） | 1 / 2 / 4 线程 = 1.00x / 2.06x / **4.12x**（锁不持在阶段执行期 → 用户代码真并行） |
| 并发吞吐（空阶段，30 ns 级任务） | 1 → 4 线程退化到 **0.30x**（32.8 → 9.98 Mops/s；2 线程 0.32x）：**每拍一次全局读锁的已知争用上限**——用「稳态零分配 + 锁外执行」换来的取舍；高吞吐两条路见「收窄声明」末条 |
| panic 后可用性 | panic 传播到调用方；清场后一次正常调用 29.2 ns，注册表未被毒化 |

分配锚回归（事件计数，窗内本线程）：`tests/lint_alloc.rs` —— 1 阶段 / 3 阶段 / 零注册三类各 1000 拍断言 **0 事件**，另带仪器自检（窗内 `Vec::with_capacity` 必须计 1，防「分配器没挂上」的假绿）；门上计数与门**同为本线程 TLS**（跨线程互不污染），故连跑 10 次无需串行闸。

### 锁策略对决（Mutex vs RwLock，2026-10-29）

判据：**未争用 ns 锚**（0/1/5 阶段）变化 ≤10% **且** 空阶段 4 线程吞吐明显更好 → 取 `RwLock`；否则保留 `Mutex`。

**前值出处（可复现性如实声明）**：`Mutex` 侧数字取自对决记录 `verify/2026-10-29-lint-lock-showdown/`（含 `lint.rs.mutex.bak` = Mutex 版完整源码、`example-rwlock-final.txt` = 当次 RwLock 版输出、`README.md` = 取值范围与结论），**非当前树可复跑**——当前树只有 `RwLock` 版，直接复跑只能得到后值；要重现前值须先 `cp verify/2026-10-29-lint-lock-showdown/lint.rs.mutex.bak skeleton/src/lint.rs`。

| 维度 | `Mutex`（前，取自对决记录） | `RwLock`（后；括号内为本批单批读数） | 结论 |
|---|---|---|---|
| 空阶段 4 线程 | 0.21x | **0.27~0.31x**（本批 0.30x） | **+25%~27%，明显更好** |
| `post_process` 0 阶段 | 17.3~17.4 ns | 17.1 ns（本批 17.0） | 略好 |
| `post_process` 1 阶段 | 28.2~30.6 ns | 29.3 ns（本批 28.0） | +3.2%（<10%） |
| `post_process` 5 阶段 | 37.6~37.8 ns | 37.9 ns（本批 38.3） | 持平 |
| 计算阶段 4 线程 | 3.88~3.96x | 3.89~4.11x（本批 4.12x） | 持平 |

结论：**采用 `RwLock`**（快照走共享读锁、注册/清场走独占写锁，毒化恢复两侧同走 `PoisonError::into_inner`）。2 线程仍退化、4 线程改善有限——因为瓶颈不是「读互斥」而是**同一缓存行上的原子 RMW 争用**，这是全局注册表的结构性上限，不是锁类型的锅；再往上要么分槽要么不每拍 lint。

---

## fields — 可观测字段值面

**feature**：常驻，零 feature 门。**依赖面**：纯 std。住契约层（`skeleton::fields`，issue#7，2026-10-30）：只承载「一组可观测字段（名 + 值）」的**纯值形状**——组装观测值交给导出方，无 IO/无运行时/无序列化（判据同 `skeleton::config`/`skeleton::lint`：采用方单开 `udf-skeleton` 即可用）。

### 契约面

```rust
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]                       // 跨 crate match 必带通配臂（Effect/Restore 同款纪律，compile_fail doctest 钉死）
pub enum FieldValue {
    Int(i64), Float(f64), Bool(bool), Str(String),
    Duration(std::time::Duration), Count(u64),   // Int=有符号测量值 / Count=无符号单调计数，语义不混用（相等性也按变体区分）
}
// 只派生 PartialEq 不派生 Eq/Hash：Float(f64) 的 NaN≠NaN ⇒ 全值相等性不成立，
// FieldValue 不可作 map 键/集元素（需要键时用字段名 &'static str）。

#[derive(Debug, Clone, PartialEq)]
pub struct FieldBag { /* Arc<Vec<(&'static str, FieldValue)>>：Arc + COW */ }
// Default 手写（同 new 走共享 EMPTY）——derive 会每次构造新 Arc（一次堆分配），违背空袋零分配契约
impl FieldBag {
    pub fn new() -> Self;   // 零堆分配（进程级共享 EMPTY Arc；Default 手写同走此路——derive 会每次分配）
    pub fn insert(&mut self, name: &'static str, value: FieldValue);  // 同名覆盖：值换、位置不动（last-wins，构造即去重）
    pub fn get(&self, name: &str) -> Option<&FieldValue>;
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &FieldValue)>;  // 插入序 = 稳定可断言
    pub fn len(&self) -> usize;
    pub fn is_empty(&self) -> bool;
    pub fn merge(&mut self, other: &FieldBag);  // 已有键覆盖（位置不动），新键按 other 序追加
}
```

**坑面 armor（7 条，`src/fields.rs` doc + `tests/fields.rs`/`tests/fields_alloc.rs` 双处钉死）**：①`Vec`+set 语义保**插入序**（HashMap 形会丢序）②同名覆盖策略显式：last-wins、原地覆盖、bag 恒无重名 ③浮点只 `PartialEq`（含 NaN 的两 bag 比较恒 false）④`Arc` COW：clone=一次原子自增 O(1)，改写独占时原地、共享时改写者付一次性 O(n) 整表克隆、共享副本不受影响 ⑤字段名 `&'static str`（可观测字段名是编译期常量，tracing 字段先例；String 名逼出每字段一次分配）⑥O(n) 线性 get/insert——观测袋是个位数量级小集合，线性换零哈希表分配（升级路径=排序索引，有大规模袋证据再上）⑦`#[non_exhaustive]` 词表。

**收窄**：无导出/上报/落盘/采样/限流（消费方自映射到 tracing/OTLP/日志等格式）；**无 serde**（契约层零依赖纪律）；无字段名注册表；无日志级别/过滤语义；无并发写（`&mut` 独占；跨线程共享靠 clone 只读）。

**实测**（单批，Windows 10 x64 / 16 核 / rustc 1.96.0；`cargo run --release -p udf-skeleton --example fields_multidim`；粗测供判级，跨批波动 10–30% 属常态）：new/clone/iter/覆盖稳态 **0.00 分配事件**（首插 2 事件如实）；clone 4 vs 256 字段**同量级（7.7–9.2ns，≤1.2x，O(1) 判级）**；iter 梯度 4/32/256 = 2.6/20.5/167.8ns（线性）；COW 首改一次性 O(n)（256 项单批 3.2–6.4µs）→稳态 ~11ns；跨线程 clone 交接 O(1)。

---

## fx — 异步并发托管

**feature**：`tokio`。**依赖面**：tokio（JoinSet/oneshot/watch/time::timeout/Semaphore）；CancellationToken 树/tokio-util 不引入。`tokio::spawn` 直调经 clippy `disallowed_methods` 闸封死——**效果只经 `FxScope` 命名口**（`spawn_tracked`/`spawn_detached`）；issue#92 起脱管口改用 `Handle::spawn`、入集用 `JoinSet::spawn_on`，**本 crate 零 allow 豁免**（`Handle::spawn` 尚未入 lint 清单，缺口见 bd 待办）。

```rust
pub struct FxScope { /* Mutex<JoinSet> + accepting 闸 + Semaphore + 取消源/令牌 */ }
impl FxScope {
    pub fn new() -> Self;                                  // 新建即 accepting
    pub fn with_concurrency(mut self, max: usize) -> Self; // 并发闸（permit-in-future，abort 连带回收）
    pub fn with_source(mut self, src: CancelSource) -> Self;
    pub fn token(&self) -> CancelToken;                    // Clone=同一取消态
    pub fn close(&self);                                   // accepting 闸关（拒新，放行在途）；幂等
    pub fn is_closed(&self) -> bool;
    pub fn in_flight(&self) -> usize;                      // 生命周期集未净数（LeakGuard 断言锚）
    pub fn spawn_tracked<T>(&self, fut: impl Future<Output = T> + Send + 'static)
        -> Result<Spawned<T>, ScopeClosed>
    where T: Send + 'static;
    pub fn try_reap(&self) -> Vec<TaskError>;              // 增量收割口：锁内同步扫已完成条目（禁 await）
    pub async fn shutdown(&self) -> ShutdownReport;        // 三段：close→abort_all→等净+收割
    pub async fn shutdown_timeout(&self, d: Duration) -> ShutdownReport; // 超时不再是 Err，timed_out>0
}
impl Default for FxScope { /* new() */ }

pub struct Spawned<T> { /* … */ }   // Drop 不 abort——任务归生命周期集托管
impl<T: Send + 'static> Spawned<T> {
    pub async fn join(self) -> Result<T, TaskError>;
    pub async fn join_timeout(self, d: Duration) -> Result<T, TaskError>; // 超时=截止已过+已请求 abort（≠ 已确认终止）
    pub fn abort(&self);               // 幂等
    pub fn is_finished(&self) -> bool;
}

pub enum TaskError { Cancelled, Panicked(String), TimedOut }
pub struct ScopeClosed;   // accepting 闸已关：spawn 拒新（fail-closed 显式）

#[must_use]
#[non_exhaustive]                    // 字段渐进扩：跨 crate 禁 struct literal（含 ..Default::default()），走 default() 起手
pub struct ShutdownReport {          // 停机排空报告（fx 域自有；与 shutdown 域 coordinator 报告同名不同型）
    // 五终局桶（completed/cancelled/panicked/failed/still_running）互斥且穷尽：
    // 本次排空观测到的每条任务终局恰落一桶。timed_out 与终局桶**正交**（截止事件
    // 计数，可覆盖截止瞬间已终局但尚未收割的条目）；drain_interrupted 是**报告来源
    // 标记**而非桶（中断结果不是整 scope 的 canonical 终局）。
    pub completed: usize,            // 正常终局条数（含滞留至本次排空才收割的条目）
    pub cancelled: usize,            // 已观测到被中止的条数（abort 已发而终止未观测者进 still_running）
    pub panicked: Vec<String>,       // 排空收割的 panic 消息（句柄被丢也不丢）
    pub failed: usize,               // 非 panic 失败终局条数（errors.len() == failed；常态 0）
    pub errors: Vec<String>,         // failed 的逐条诊断文案（与计数同点写入，禁静默吞）
    pub timed_out: usize,            // 正交截止计数：截止时未完成数（仅 shutdown_timeout 超时路径 >0）
    pub still_running: usize,        // 报告返回时「终止未观测到」的条数（不宣称仍在执行）
    pub drain_interrupted: bool,     // 报告来源：排空 owner 被 drop/panic ⇒ 中断结果（只请求 abort，不确认终止）
}

#[derive(Clone)]
pub struct CancelSource { /* … */ }
// Clone = 同一取消态的第二句柄（不是独立源）：任一句柄 cancel 全体可见；
// 宿主把源按值交给 `with_source` 后，仍可用预先 clone 的句柄触发取消。
impl CancelSource {
    pub fn cancel(&self);            // 幂等；单向 false→true（粘性：无接收者期间也存下，之后订阅者立即见到 true）
    pub fn is_cancelled(&self) -> bool;
}

#[derive(Clone)]
pub struct CancelToken { /* … */ }
impl CancelToken {
    pub fn is_cancelled(&self) -> bool;
    pub async fn cancelled(&self);   // cancel-safe（false→true 才返回）
    pub async fn run_until_cancelled<T>(&self, fut: impl Future<Output = T>) -> Option<T>
    where T: Send;                   // 取消=None（biased select，取消臂优先）
}

pub fn cancel_pair() -> (CancelSource, CancelToken);
pub enum AdmissionError { Closed, PendingFull, TrackedFull, NoRuntime }   // typed 过载/闸关/runtime 上下文缺失（issue#39/#92；**非** non_exhaustive ⇒ 跨 crate 穷尽 match 须列全）
pub struct FxCounts { pub active: usize, pub waiting: usize, pub completed_unreaped: usize, pub rejected: usize }
impl FxScope {
    pub fn with_max_pending(self, n: usize) -> Self;   // 等待许可的准入上限（默认无界，opt-in）
    pub fn with_max_tracked(self, n: usize) -> Self;    // 保留条目上限（含已完成未收割，默认无界）
    pub fn try_spawn_tracked<T>(&self, fut: impl Future<Output = T> + Send + 'static)
        -> Result<Spawned<T>, AdmissionError>;         // 有界准入：过载 typed 拒绝（不阻塞、不 panic）
    pub fn counts(&self) -> FxCounts;                   // active/waiting/completed_unreaped/rejected
    pub fn expect_body_idle(&self);                    // 任务体全部结束（LeakGuard fx 侧锚点）
    pub fn expect_fully_reaped_idle(&self);            // 更强：跟踪集合已完全收割
```

- **三资源维度（issue#39）**：`with_concurrency` 只限**任务体并发**，**不是**内存/保留量上界；保留上界 = `with_max_tracked`（含已完成未收割条目），等待许可数 = `with_max_pending`，两者默认**无界**（opt-in 显式设）。有界准入走 `try_spawn_tracked` → `Result<_, AdmissionError>`（过载 typed 拒绝，不阻塞不 panic）；`spawn_tracked` 是**无界旧口**（保持兼容，不受上界约束）。`Spawned::join()` 只消费结果、**不**移除集合条目（收割口 = `try_reap`）。`counts()` 暴露 active/waiting/completed_unreaped/rejected。
- **runtime 上下文前置条件（issue#92）**：`try_spawn_tracked` / `spawn_detached` 在**取任何内部锁之前**做 `Handle::try_current()` 预检——无当前 Tokio runtime ⇒ `AdmissionError::NoRuntime`：**不入容量账**（`admitted`/`tracked`/`active`/`waiting`/`rejected` 全不变）、**不消费宿主 Future**、不触闸。`spawn_tracked` 是 fail-visible 旧口（无 runtime 带因 panic，rustdoc `# Panics` 指向 typed 口）。`spawn_detached` 返回类型已统一为 `Result<JoinHandle<T>, AdmissionError>`（原 `ScopeClosed`），脱管语义（不进 JoinSet、不受 shutdown 等待、不计 tracked/in_flight）不变；入集用 `JoinSet::spawn_on(.., &handle)`、脱管用 `handle.spawn(..)`（不再隐式读当前位置）。timeout 面另需 runtime **启用 time driver**：`shutdown_timeout` / `join_timeout` 的 `# Panics` 已写明（`Builder::new_current_thread().build()` 未 `enable_time()` 即 panic），签名与返回面不变。
- 任务结局归一：panic 经 catch_unwind 归一 `TaskError::Panicked(消息)`（payload downcast &str/String，否则哨兵文案）；发送端消亡=Cancelled；**join 超时=`TimedOut` = 截止已过 + 已请求 abort（`AbortHandle::abort()` 只是取消请求）——不表示任务体已终止、其守卫已运行或集合条目已被收割**（issue#67 契约冻结；超时后的证据是 scope 级 `in_flight` / `expect_body_idle` / `expect_fully_reaped_idle` / `counts()`；abort 仍是请求，任务守卫与 `in_flight` 跟踪在任务真正退出前具权威）。
- `spawn_tracked` 线性化=锁内原子 RMW 过闸 + 插入与 shutdown 的 abort_all 串行——过闸任务必被 shutdown 覆及。
- 条目滞留与收割（issue#13）：生命周期集条目不再静默丢弃——panic 载荷进集合（句柄被丢也不丢），**join 与 `try_reap`/停机排空双重可见**（同一 panic 两条路都可能报出，诊断面语义非 exactly-once）；完成条目滞留集合至 `try_reap`（增量收割口：锁内同步 `try_join_next` 扫已完成条目，空集零分配、幂等，不动在途任务、不改 in_flight）或停机排空显式消费。
- 停机报告：`shutdown` 排空收割 panic 载荷入 `ShutdownReport.panicked`（abort 条目=预期消亡不计入）；`shutdown_timeout` 超时不再返回 `Err`——超时即返前先增量收割已完结条目（panic 载荷不因超时丢失），剩余未完成数计 `timed_out`（剩余集 Drop=abort 兜底，零残留）。
- **并发停机契约（issue#70）**：三个角色。**owner** = 首个进入排空的调用（`Open→Draining` 在取 JoinSet **之前**原子发布）——由它决定排空策略并发布 **canonical 报告**（正常完成 / 自身截止），实现上是一条 RAII 守卫，**每条退出路径（成功 / 超时 / panic / 取消）都先结算共享谓词再通知等待者**，共享状态锁内只碰报告自有数据（不跑用户代码、不跑用户析构）；守卫被 drop（owner future 被弃置）⇒ 发布**中断结果**（`drain_interrupted=true` + 中断时已知计数）再让 JoinSet drop 请求 abort，**绝不**停在 `Draining`。**等待者** = 之后的并发调用——醒来拿到同一份 canonical 报告；等待者**自己的**截止到期时只按自身读数返回（终局桶空，只有自身的 `timed_out`/`still_running`），**不得改写 owner 的策略**，其报告**不是** canonical。**已结算** = 排空完成后的再次调用——返回该次排空的 **canonical 报告副本**（顺序与并发晚到者可见面一致；审计 F3 修正：旧实现「先判 Draining、再 borrow」两次读，恰在结算瞬间到达的调用会落进已结算分支却只拿到默认报告 ⇒ 并发停机时 panic 载荷被静默丢）。判「整 scope 停机完成」请用「已结算」或 `in_flight()==0`，勿只看报告字段：中断/超时都**只请求 abort，不确认终止**。
- **执行模型与阻塞边界（issue#94）**：本域只托管**协作式** async——被托管 future 必须在有限时间内返回 poll、长工作段定期让渡。三类「阻塞」分开：①**协作式 async 等待**（`.await` 未就绪的 IO/timer/通道 ⇒ `Poll::Pending`，executor 照常推进 timer 与其他任务）；②**同步阻塞 / 长时不 yield**（poll 内忙等、同步锁等待、同步析构 ⇒ 占住 executor 线程，current-thread runtime 上 timer/取消观察/停机等待全部停摆）；③**真正的 blocking work**（文件 IO / CPU 密集 / 阻塞系统调用 ⇒ 应移出 async executor）。框架**不做** blocking adapter（不承诺能强制终止任意 Rust 代码，也不在 `FxScope` 内部再造 watchdog 线程伪装可抢占）：②③ 交给 `tokio::task::spawn_blocking` 或消费方自备 `ThreadPool`，任务内只 await / 收集结果。
- **截止不是硬墙钟上限（issue#94）**：`shutdown_timeout(d)` / `join_timeout(d)` 依赖 tokio timer——`d` 只在 **executor 可继续调度**时才保证「到期触发并请求 abort」；有任务卡在②的同步段里时，截止已过也要等该段返回、executor 恢复后才被观察到。`timed_out` = 截止已过计数、`still_running` = 报告返回时终止未观测、`TaskError::TimedOut` = 截止已过 + 已请求 abort：三者**都不是**「已终止」。abort 排在下一个让渡点，无 await 的同步段内排不上；外层 CI job timeout 只保 CI 进程，不改变本域语义。
- **runtime profile（issue#94）**：纯 async、无阻塞任务可用 current-thread runtime；含 blocking work 的应用必须 multi-thread runtime（或独立 worker）；UDF 不替消费方创建/切换 runtime，current-thread 上**不要**用 `block_in_place`（该 API 不适用于 current-thread）。两侧证据（`skeleton-infra/tests/fx_lifecycle.rs`，commit `f9b413d`）：`current_thread_blocked_poll_cannot_observe_shutdown_deadline`（阻塞期间截止无法被观察，放行后只能看到「已过期」）与 `multi_worker_observes_shutdown_deadline_while_one_worker_is_blocked`（另一 worker 照常观察截止，被阻塞任务不被标成已终止）。

**实测**（单批，Windows 10 x64 / 16 核 / rustc 1.96.0；`cargo run --release -p udf-skeleton-infra --features tokio,memgov --example fxscope_multidim` ⑤停机段；粗测供判级，跨批波动 10–30% 属常态）：shutdown 排空已完结条目 **252ns/条目**（10k 条目 join_next 收割面）；shutdown 中止在途 111µs（100 park 任务 close+abort_all+收割）；`shutdown_timeout` 守卫设定 5ms 中位 7ms、超时命中 3/3（报告形 `timed_out>0` 判定；超出部分=Windows 计时器粒度+传播开销）。

```rust
use skeleton_infra::fx::FxScope;

let scope = FxScope::new();
let handle = scope.spawn_tracked(async { 41 + 1 }).unwrap();
// assert_eq!(handle.join().await, Ok(42));  // 需 tokio 运行时：见 tests 与 fx.rs doctest
scope.expect_body_idle();   // 任务体结束；集合是否已收割用 expect_fully_reaped_idle()
```

> 注：`join()` 需在 tokio 运行时内 await；独立可编译片段见 `examples/quickstart.rs` 与 `src/fx.rs` 单元测试（`#[tokio::test]` 十九件：spawn/abort/panic/timeout/close/shutdown 排空/超时收割/`try_reap` 增量收割面语义全钉）。

---

## journal — 追加式账本

**feature**：`journal`（绑 serde + serde_json + 契约 crate）。**依赖面**：serde/serde_json 仅此 feature 可达。

契约要点：**先落盘后 apply**（`append` Err ⇒ 调用方禁 apply 该 Action）；**单写者**（双进程挂载同一账本=交错损坏，多进程场景上层编排）；**flush 点=持久点**（BufWriter 缓冲非逐行 flush，读侧可见性=已 flush 前缀）；行格式=版本头 `{"v":1}`（`deny_unknown_fields`——未知字段 fail-closed 拒挂载）+ action/snapshot 行 `{"t":"a"|"s", ...payload}` 平铺（**`"t"` 为顶层保留键**，payload 携带顶层 `"t"`=append 显式拒绝；payload 须序列化为 JSON 对象）。

```rust
pub trait Journal<A>
where A: Serialize + DeserializeOwned
{
    type State: Serialize + DeserializeOwned;                                    // 状态面关联类型（被 Self 钉死）
    fn append_action(&mut self, action: &A) -> Result<(), JournalError>;        // 追加 action 行（A 从参数推断）
    fn restore_stream(&self) -> Result<Option<(Self::State, Replay<'_, A>)>, JournalError>; // 流式：快照态+懒回放（全推断）
    fn append_snapshot(&mut self, state: &Self::State) -> Result<(), JournalError>; // flush 先行+写后即刷
    fn replay(&self) -> Result<Replay<'_, A>, JournalError>;                    // 实现层独有
}

pub type DynJournal<A, S> = dyn Journal<A, State = S>;   // (A, S) 焊接别名（trait-object 面）

pub struct JsonlJournal<A, S> { /* … */ }
impl<A, S> JsonlJournal<A, S> {
    pub fn create(path: impl AsRef<Path>) -> Result<Self, JournalError>;  // 已存在=Err(AlreadyExists)
    pub fn open(path: impl AsRef<Path>, expected_version: u32) -> Result<Self, JournalError>;
    pub fn flush(&mut self) -> Result<(), JournalError>;   // 宿主停机序的 flush 位
    pub fn with_snapshot_every(mut self, n: u64) -> Self;  // 每 n 次 append 自动 flush；0/None=关
}

pub enum JournalError {
    Io { path: PathBuf, source: std::io::Error },
    Serialize(serde_json::Error),
    Corrupt { line: u64, source: serde_json::Error },
    VersionMismatch { found: u32, expected: u32 },
}

pub struct Replay<'j, A> { /* Iterator<Item = Result<A, ReplayError>> */ }
impl<'j, A> Replay<'j, A> {
    pub fn skipped_unknown(&self) -> u64;   // 未知变体/快照行跳过计数（fail-visible）
}

pub enum ReplayError {
    Io(std::io::Error),
    Corrupt { line: u64, source: serde_json::Error },
}
```

- 实现层 `Journal` 与契约 `skeleton::JournalPort` 分工：实现层方法名带实现层词形（`append_action`/`restore_stream`/`append_snapshot`，`replay` 独有），端口独占契约词表（`append`/`restore`/`persist_snapshot`，返回三态 `Restore<A, S>` 物化）——两 trait 同屏 import 直呼无歧义（旧三同名双候选拒编译已结构性消灭，批 2A）。状态面为关联类型 `State`（批 6A）：被 `Self` 钉死，构造一次定型（`JsonlJournal::<A, S>::create(..)`）后方法调用零 turbofish 零绑定标注；dyn 焊接面走 [`DynJournal`] 别名（`State =` 短形）。宿主要增量流式恢复用实现层 `Journal`，要端口可换性面向 `skeleton::JournalPort` 编程。
- `open`：末行无 `\n` 结尾=崩溃残行 → 截断到上一完整行边界并计数留痕（残行必未越过 flush 点=非持久，截断不丢任何已持久 action）。
- `restore_stream`：自最新 "s" 行起播（快照探测=字节前缀短路，非 "s" 形行零 JSON 解析）；快照行之前的行跳过不解析；无快照行=`None`（调用方回退 initial 全量回放）。端口侧 `restore` 同扫描，物化为 `Restore` 三态。
- `replay`/`Replay`：未知 "t" tag 行与快照行跳过+计数（schema 演化 add-only——payload 缺字段走 schema default）；畸形 JSON 行 fail-closed Err 禁静默跳过。
- 自动快照注记：`snapshot_every` 命中时自动 **flush**（重开后可见）；快照节奏（写快照行）归调用方（端口 `persist_snapshot` / 实现层 `append_snapshot`）。

```rust,no_run
use serde::{Deserialize, Serialize};
use skeleton_infra::journal::{JsonlJournal, Journal};

#[derive(Serialize, Deserialize)]
struct Action { id: u64 }
#[derive(Serialize, Deserialize)]
struct State { count: u64 }

let mut j = JsonlJournal::<Action, State>::create("journal.jsonl")?; // 构造期一次定型
j.append_action(&Action { id: 1 })?; // 先落盘：Err ⇒ 禁 apply（A 从参数推断）
let _restored = j.restore_stream()?; // Some((快照态, 增量回放)) 或 None（全推断）
j.append_snapshot(&State { count: 1 })?; // 周期位：flush+写快照行
j.flush()?; // 宿主停机序：解释器 close → 排空 idle → flush → drop
```

> 注：`create`/`open` 涉 IO，正式落盘路径的端到端语义（含恢复装配 `SyncStore::restore_from` + 审计深度链收敛）由 `tests/journal_port.rs` 钉死。

---

## testing — 测试替身

**feature**：`testing`（连带 `tokio` + `runtime`；重量=已知取舍，拆轻量 feature 待真实采用方出现）。

### ScriptedRuntime — 脚本化解释器替身

```rust
pub enum Script<A, Op> {
    Emit(A),              // 回灌一个 Action（经 ActionSender）
    Panic(&'static str),  // 任务内 panic（scope 捕获→TaskError::Panicked）
    Never,                // 永不完成（挂起面/取消竞窗用）
    Run(Op),              // 占位透传：Op 语义由采用方在任务内断言
}

pub struct ScriptedRuntime<A, Op> { /* … */ }
impl<A, Op> ScriptedRuntime<A, Op> {
    pub fn new(scope: Arc<FxScope>) -> Self;
    pub fn feed(&mut self, scripts: impl IntoIterator<Item = Script<A, Op>>);  // FIFO 消费
    pub fn scope(&self) -> &Arc<FxScope>;
}
// 另实现 EffectRuntime<A, Op>：每次效果转发出队一脚本经 scope.spawn_tracked 执行
```

脚本耗尽=no-op（TestStore.receive 超时即自然失败信号）；scope 关闸时 spawn 失败 fail-fast（禁静默丢弃 future）。标准形模板：reducer 出描述式效果 → 按脚本解释 → Emit 经回灌通道闭环保环；取消竞窗测试用 `tokio::time::start_paused`（本件不实现 clock）。

### TestStore — 验收面

```rust
pub struct TestStore<S, A, Op = Infallible> { /* … */ }
impl<S, A, Op> TestStore<S, A, Op>
where S: Clone + Send + Sync + 'static, A: Send + 'static, Op: Send + 'static
{
    pub fn new(root: impl Reducer<State = S, Action = A, Op = Op> + Send + Sync + 'static) -> Self;
    pub fn dispatch(&mut self, action: A);        // dispatch 并刷新 idle 快照
    pub fn receive(&mut self, expected: ExpectedEffect<Op>)
    where A: std::fmt::Debug, Op: std::fmt::Debug + PartialEq;   // 弹出最早效果比对
    pub fn state(&self) -> &S;                    // 最近一次 dispatch 后的 idle 状态
}

#[derive(Debug, PartialEq)]
pub enum ExpectedEffect<Op = Infallible> {
    Run { id: Option<EffectId>, op: Op },    // id: None=不校验；Some=必须同值
    Cancel { id: Option<EffectId> },
}
```

`TestStore` 自带 `CollectRuntime`（效果原样回收、永不解释——逻辑层无 IO 全测）。`receive` 队列空时区分两因：真没产效果 vs 产了但走了回灌通道（Emit 永不进 receive 队列——报错并指正解：回环语义用 `dispatch` + `state()` 断言）。Emit 回环断言形：`store.dispatch(...)` 后 `store.state()` 比对。

### parse_mock — 解析面替身（同 feature）

`skeleton` 三解析端口（`JsonPort`/`SqlDialectPort`/`MarkdownPort`）的脚本化实现方与测试注入面。共享形状：脚本化 FIFO（`push_*` 入队，调用按序出队）；调用记录面（收到的输入原样入记录，先记录后出队）；**队列耗尽 panic**（替身误用=测试 bug，fail-fast）；本件零真实解析。

```rust
pub struct MockParseError { /* … */ }
impl MockParseError { pub fn new(msg: impl Into<String>) -> Self; }

pub struct MockJson { /* … */ }          // JsonPort 替身
impl MockJson {
    pub fn new() -> Self;
    pub fn push_decode(&mut self, step: Result<Json, MockParseError>);
    pub fn push_encode(&mut self, step: Result<String, MockParseError>);
    pub fn decoded_inputs(&self) -> Vec<String>;   // 收到的输入（调用序）
    pub fn encoded_values(&self) -> Vec<Json>;
}

pub struct MockSqlDialect { /* … */ }    // SqlDialectPort 替身
impl MockSqlDialect {
    pub fn new() -> Self;
    pub fn push_quote_ident(&mut self, step: Result<String, MockParseError>);
    pub fn push_placeholder(&mut self, step: Result<String, MockParseError>);
    pub fn quoted_idents(&self) -> Vec<String>;
    pub fn placeholder_nths(&self) -> Vec<usize>;
}

pub struct MockMarkdown { /* … */ }      // MarkdownPort 替身
impl MockMarkdown {
    pub fn new() -> Self;
    pub fn push_parse(&mut self, step: Result<Vec<MdEvent>, MockParseError>);
    pub fn sources(&self) -> Vec<String>;
}
```

```rust
use skeleton::parse::{Json, JsonPort};
use skeleton_infra::parse_mock::{MockJson, MockParseError};

fn read_port<J: JsonPort>(j: &J) -> Result<i64, String>
where
    J::Err: std::fmt::Display,
{
    match j.decode("{}") {
        Ok(Json::Int(n)) => Ok(n),
        Ok(_) => Err("非整数".to_string()),
        Err(e) => Err(e.to_string()),
    }
}

let mut mock = MockJson::new();
mock.push_decode(Ok(Json::Int(42)));
assert_eq!(read_port(&mock).unwrap(), 42);
assert_eq!(mock.decoded_inputs(), ["{}"], "收到的输入可断言");

let mut boom = MockJson::new();
boom.push_decode(Err(MockParseError::new("bad json")));
assert_eq!(read_port(&boom).unwrap_err(), "mock parse error: bad json");
```

---

## logx — 日志门面

**feature**：两层拆分（批 1，2026-10-18；ADR 0002）——`log-fs`（纯 std 零三方——域面口径，crate 树见 [§零依赖口径](#零依赖口径)：密钥掩码 + 路径解析 + `FileSink`，单开即可用文件日志）与 `log`（=`log-fs` 叠门 + log + tracing + tracing-subscriber + tracing-log：init 系门面绑定层）。日志根目录契约 `LogPathResolver` 住 `skeleton::logpath`。门面 trait=生态 `log::Log`（不自造）；核心自身永不打日志（核心零依赖，结构性排除）。

```rust
// ── log-fs 面（纯 std，域面零三方依赖）──
pub fn mask_secrets(input: &str, secrets: &[&str]) -> String;

pub struct DefaultPathResolver { /* … */ }
impl DefaultPathResolver {
    pub fn new(project_name: impl Into<String>) -> Result<Self, HomeDirUnresolvable>;
}
impl skeleton::logpath::LogPathResolver for DefaultPathResolver { /* … */ }
pub struct HomeDirUnresolvable;

pub struct FileSink { /* … */ }
impl FileSink {
    pub fn new(root: impl Into<std::path::PathBuf>) -> std::io::Result<Self>;
    pub fn dropped_lines(&self) -> u64;
}
impl std::io::Write for FileSink { /* 软失败面：恒 Ok，丢弃走计数 */ }

// ── 契约面（skeleton::logpath，零 feature）──
pub trait LogPathResolver: Send + Sync {
    fn log_root(&self) -> std::path::PathBuf;
}

// ── log 面（=log-fs + tracing 四件，门面绑定层）──
pub fn init_tracing_default(level: log::LevelFilter) -> Result<(), AlreadyInit>;
pub fn init_tracing_from_env(default_level: log::LevelFilter) -> Result<(), AlreadyInit>;

pub struct FileSinkWriter(std::sync::Arc<FileSink>);   // MakeWriter 适配壳
impl FileSinkWriter { pub fn new(sink: std::sync::Arc<FileSink>) -> Self; }

pub enum LogInitError {
    AlreadyInit,
    RootCreate { root: std::path::PathBuf, source: std::io::Error },
}

pub struct AlreadyInit;
```

- **EnvFilter 语义**：`init_tracing_from_env` 在 RUST_LOG 未设/非法时兜底恒为 `<default_level>,skeleton=off`（框架自身埋点默认静默，宿主日志零打扰）；RUST_LOG 已设且合法时逐字生效。解析失败 fail-visible：stderr 报非法语法与回退结果（禁静默降级成另一档级别）。三种开启姿势：`RUST_LOG=debug`（全开）/ `RUST_LOG=warn,skeleton::ratelimit=debug`（单域）/ `RUST_LOG=skeleton::scheduler=trace`（单流程）。
- **桥接语义知情声明**：log 门面经 tracing-log 桥后事件 target="log"、原 target 降为 `log.target` 字段——`RUST_LOG=skeleton=debug` 不路由 log 门面行。初始化组合注记：只调 `try_init()`（其内部已含桥初始化），不得再显式 `LogTracer::init`（撞 log 单例恒 Err）。
- `mask_secrets`：命中子串→`"***"`；按长度降序替换（最长优先防半掩）；空串项跳过；恒分配一次新 String。
- **`DefaultPathResolver` 双构建默认根**：debug=编译期 `CARGO_MANIFEST_DIR`/log（框架自身开发态，零运行时猜测；采方开发态请注入自有 resolver）；release=`home_dir()/.<project_name>/log/`——release 恒不引用 CARGO_MANIFEST_DIR（开发者机器路径烧进发布物=隐私面）。home 构造期一次性解析，失败显式 Err（禁静默落 cwd）。
- **`FileSink`**：按日滚动 `log-YYYY-MM-DD.log` 纯追加（跨进程按行交错安全，永不改名/截断），每行 flush；写失败=计数+stderr 一次告警+继续（禁 panic 拖垮业务、禁纯吞——`dropped_lines` 可观测）。时间戳与文件名恒 UTC（std 无本地时区 API，chrono 判否）。`new` 立即递归创建根目录（配置错误显式失败，禁等到首行才爆）；文件按日懒开（无事件不落空文件）。默认行格式：`[UTC 时间 LEVEL target] message key=value ...`（整行先合成单 String 再一次写出=行完整性前提）。
- **`init_with_file(resolver, default_level)`**：console+file 双 sink（同一默认行格式）；过滤语义同 `init_tracing_from_env`（恒含 `skeleton=off` 兜底）；本地资源（日志根目录）先建，任何配置错误在任何全局被触碰之前显式失败（`LogInitError::RootCreate`）。签名：

```rust
pub fn init_with_file(
    resolver: &dyn LogPathResolver,
    default_level: log::LevelFilter,
) -> Result<(), LogInitError>;
```

```rust
let r = skeleton_infra::logx::DefaultPathResolver::new("agentwiki")
    .expect("debug 构建不解析 home，恒 Ok；release 构建需 home 可解析");
// debug 构建下 r.log_root() == <本 crate 工程目录>/log（编译期锚）
```

### 实测锚（2026-10-16 批 C / 2026-10-17 log-file，源码 doc 口径）

`mask_secrets` 无命中纯扫描近线性：4KiB×8 规则 ~3µs（跨机实测 2.9-4）、64 条 ~55-85µs，双轴同向增长无平方爆点（高频日志路径宜上游过滤先行再掩码）。FileSink 多维矩阵（分配逐路径/ns 锚/1-8 线程竞争/p50-p99.9）见采集器 `examples/log_multidim.rs`（2026-10-17-log-file 报告，logx 稳态锚 ~1685ns/行口径同源）。稳态锚换算警示：FileSink 每行 flush=sync_all 主导（syscall 面，µs→ms 量级）——~1685ns/行 仅纯写入面上限量级口径（≈0.6M 行/s），具体数字以重采为准。

---

## tracingx — tracing 桥

**feature**：`tracing`（连带 `runtime` + tracing + tracing-subscriber）。核心零原生输出、宿主无库内 hook 可挂——收口形态：核心出审计事件流，本桥一行接线。

```rust
pub fn bridge_events_to_tracing<
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
>(
    store: &SyncStore<S, A, Op>,
    capacity: usize,
) -> AuditTicket;
```

每个投递到的事件一条 `tracing::event!`（target `skeleton::audit`，INFO 级），字段 `seq`/`kind`（生命周期节点名=过滤聚合稳定键）/`depth`（仅 Enqueued 携带回灌深度）。**上游已升级为「串行派发」**（`SyncStore` 审计实时回调，#65 主路线①）：并发发射窗内的事件**入队补投**而非跳过，故桥**不再有「并发窗内未进回调」的缺口**；仍存在的丢弃面只有**有界待投递 FIFO 的溢出**（计入 `SyncStore::audit_delivery_drops()`，与环溢出分账）与**环溢出**（历史侧，证据=`seq` 空洞）——完整性取证走 `take_events`，见「投递语义词汇（订阅面通用）」。`capacity` 语义同 `SyncStore::subscribe_events`；返回票据 Drop=停桥（零开销恢复）。

```rust
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::tracingx;

struct Step;
impl Reducer for Step {
    type State = u32;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> u32 { 0 }
    fn reduce(&self, s: &u32, _a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + 1 }
}

let store = SyncStore::new(Step, NoFx);
let _ticket = tracingx::bridge_events_to_tracing(&store, 64);
store.dispatch(1); // → tracing::event!（target "skeleton::audit"）
store.close();
```

实测锚（2026-10-16 批 C）：桥净转译 ~10ns/事件（八分支 match + event! 兴趣缓存）；有订阅者实收再 +~11ns——桥常挂的成本集中在宿主订阅者本体。

---

## compose — 父子分片组合器

**feature**：`compose`（只需契约词汇）。协作=显式结构，字符串路由组合的封死形态。

```rust
pub fn combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>(
    get: G, set: Se, wrap: W, unwrap: U, child: C, lift_fx: L,
) -> Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>
where /* 12 泛型参具名形态；G/Se=镜头，W/U=路由，C=子 Fragment，L=效果上提 */;

#[macro_export]
macro_rules! combine { /* 变体短形态 / 显式形态 */ }

pub struct Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L> { /* … */ }
// impl Fragment for Combine<…>（只实现分片契约：无 initial，作根 = E0277）
```

- 六参语义：`get`/`set`=父状态内子切片的读写镜头；`wrap`/`unwrap`=子 action 上浮 / 父 action 下沉路由（`unwrap` 返回 None=本层不路由，状态克隆原样返回零效果）；`lift_fx`=子效果词表到父词表的显式上提。子效果里的 `Emit(CA)` 由核心经 `wrap` 自动上浮——`lift_fx` 只收 `Run`/`Cancel` 两形。
- **产物实现 `Fragment` 不实现 `Reducer`**：分片没有 `initial`——根状态由父 Feature 的 `initial` 显式构造（含子初值），分片只承载 reduce 委托；用作根 = **E0277 编译期拒绝**（`#[diagnostic::on_unimplemented]` 错误信息自带修复指引；禁静默空状态）。子件 bound 相应放宽为 `Fragment`：完整 Feature 经 blanket impl 仍可作子件，分片可嵌分片。
- `combine!` 两形态：**变体短形态** `combine!(Child, Parent.field, Enum::Variant)` 一体生成 wrap/unwrap（unwrap 自 `&PA` 克隆上浮，要求子 action `Clone`）；`lift` 省略=零效果上提。**显式形态** `wrap:`/`unwrap:`/`lift:` 逐项给（非 Clone 词表与复杂路由的逃生口）。切片表达式限 `状态类型.字段名` 裸段（泛型/路径状态退回 `combine` 显式形态）。
- 零 dyn 零装箱；采用方可用 `Combine<_, _, …>` 注解持有产物（匿名 `impl Fragment` 形态做不到）。

```rust
use skeleton::{EffectBuf, Fragment, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;

#[derive(Clone, Debug, PartialEq)]
struct Parent { total: u32, child: u32 }

#[derive(Clone, Debug, PartialEq)]
enum PAction { Child(u32), Touch }

struct ChildCounter;
impl Reducer for ChildCounter {
    type State = u32;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> u32 { 0 }
    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 { s + a }
}

fn counter_fragment() -> impl Fragment<State = Parent, Action = PAction, Op = Infallible> {
    skeleton_infra::combine!(ChildCounter, Parent.child, PAction::Child)
}

struct ParentFeature;
impl Reducer for ParentFeature {
    type State = Parent;
    type Action = PAction;
    type Op = Infallible;
    fn initial(&self) -> Parent { Parent { total: 100, child: 0 } }
    fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
        match a {
            PAction::Child(_) => counter_fragment().reduce(s, a, out),
            PAction::Touch => Parent { total: s.total + 1, child: s.child },
        }
    }
}

let store = SyncStore::new(ParentFeature, NoFx);
store.dispatch(PAction::Child(2)); // unwrap 路由 → 子迁移 → set 写回切片
store.dispatch(PAction::Touch);    // 未路由：本层自理，子切片不动
assert_eq!(*store.snapshot(), Parent { total: 101, child: 2 });
store.close();
```

实测锚（2026-10-16 批 C）：组合税个位数 ns——combine! 装配 vs 等义手装单 Feature 的 dispatch 全含差值五轮交错中位 ~0-2ns（闭包全 ZST 逐字内联、子 EffectBuf 空态零分配）。

---

## view — 语言无关视图桥

**feature**：`view`（绑 serde_json + `udf-std-ext/json`；ADR 0007 §4.2-5 跨 crate 开门）。**依赖面**：契约词汇住 `skeleton::view`（零依赖）；机器与读侧参考实现住 `skeleton_infra::view`。规范唯一权威 = `docs/view-region-spec.md` v1；命名工件 = `docs/view-bridge-constants/`（manifest 真源 + TS/Py/C 生成物 + CI `--check` 断言）。

```rust
pub struct ViewBridge<S, A, M, I, J, R> { /* 订阅票据 + 单锁折叠态 + eager 区域通道 */ }
impl ViewBridge<S, A, M, I, J, R> {
    pub fn mount<T: Store<State = S, Action = A>>(store: T, map: M, intents: I, encoders: Encoders<J, R>) -> Self;
    pub fn bind(&self, path: &str) -> Result<BindingId, BindError>;      // 装配期；首通知封板
    pub fn send_intent(&self, intent: Value) -> Result<bool, IntentError>; // Ok(false) = 闭门
    pub fn take_patches(&self) -> PatchBatch;   // 水位 (from/to/protocol_version) + resync 位
    pub fn snapshot_doc(&self) -> Snapshot;     // 现态全量重投影 + 同源版本（快照即交付）
    pub fn region_version(&self) -> u64;        // 区域头版本（与 to_version 同源）
    pub fn projection_panics(&self) -> u64;     // 降级显形计数（对齐 subscriber_panics 先例）
}
```

要点：桥 = 纯订阅者（票据归桥，Drop = 注销；store 所有权随桥）；版本 = 桥内单调消费代数（同步档，与区域头同源，撕裂结构性不可能）；投影 panic = 毒化唯一残因（catch + 计数 + `resync` 位 ⇒ `snapshot_doc` 收口）；未知标签跳过（读侧 `view::read` 实现一次，TS/Py/C 镜像复刻，禁整区失败）；正典验收环 `examples/view_bridge_demo.rs`，行为面 `tests/view_bridge_test.rs`。

**边界：同一进程内 vs 跨进程/跨重启**。桥保证的是**同一进程内**的自洽与重同步——票据、补丁水位、快照与区域版本都活在同一个桥实例里，`snapshot_doc()` 是进程内重同步原语、**不是持久化**。进程死亡后 UI 侧无从复现：**视图复现由 store/持久化层负责**（journal 回放或采用方自有存储），桥不缓存状态、不作此承诺（Android 分层对照：ViewModel 保进程内，SavedStateHandle 保进程死亡——后者不属渲染层职责）。

**纪律指向**：①视图词表用 enum 表达互斥状态、禁布尔旗子（正反例见 `skeleton::view::ViewMap` 文档与 `bindings()` 语义）；②「派生 input 覆盖 compute 实读」红线的最小可执行反例 = `tests/view_bridge_test.rs::narrow_input_leaks_stale_and_is_detectable`——漏报的三处签名：`projection_runs` 停滞、水位推进而补丁为空、快照真值与最后交付的补丁值不等。

---

## log-embed — 埋点规范

**feature**：`log-embed`（绑 tracing；**默认不开启**——埋点宏展开为空、tracing 依赖不进树，cargo tree 零 tracing 证明；`tracingx`/`logx` 桥不受本 feature 影响）。

九域 55 调用点（`std-ext` 七域 48：scheduler 14 / retry 12 / cache 9 / fsx 6 / ratelimit 4 / iter 2 / backoff 1；本 crate 七点：shutdown 6 + fx 1），覆盖 scheduler/ratelimit/retry/backoff/fsx/cache/shutdown/iter/fx。**计数口径**：「调用点」= `embed_event!` 宏调用——排除宏定义行、`//` 文档/注释行与 `#[cfg(test)]` 之后的内容，多行调用按首行计一次；改埋点后按此口径重数，同步两 crate 的 `lib.rs`/`Cargo.toml` 注与本段。纪律（lib.rs `embed_event!` 注）：

- **只埋行为决策点**（register/cancel/fire/裁决/拒绝/等待分段），禁埋纯计算路径（`backoff_delay` 的 debug 入参事件是派单契约显式点）。
- **字段化禁格式化**：字段只收数值/布尔/静态串，禁 format!/Display。
- **关联序号进字段**：seq（scheduler 堆条目）/attempt（retry 轮次）/round（ratelimit 阻塞重查轮）承载跨事件关联，不起 span。
- 级别规范：正常流转=debug / 异常路径=warn / 终态=info。事件全部在状态锁外发射（订阅方 IO 不持锁）。

### 域级埋点表

| 域 | target | 事件点 | 级别 |
|---|---|---|---|
| scheduler | `skeleton::scheduler::{register,cancel,close,fire,worker}` | 注册/取消/点火/墓碑跳过=debug；关闸拒绝、**容量触顶拒绝注册（#58）**、回调 panic、超载退化重臂、驱动内部 panic=warn；close 与 worker 退出=info | 混合 |
| ratelimit | `skeleton::ratelimit::{try_acquire,acquire}` | 快路径拒绝与超容量拒绝=warn（available/wait_ns/capacity）；阻塞等待分段与醒后重查=debug（round/deficit）；成功扣减零事件 | 混合 |
| retry | `skeleton::retry::{attempt,verdict,backoff}` | 尝试开始=debug；重试放行=warn（backoff_ms）；停机终态=info；退避截平=warn（linear_ms→capped_ms） | 混合 |
| backoff | `skeleton::backoff` | 计算入参/结果（attempt/base_ms/max_ms/delay_ms）=debug（纯函数域唯一埋点） | debug |
| fsx | `skeleton::fsx::{walk,copy,tempdir}` | walk 帧推进=debug（depth/sort）；copy 逐项=debug + 部分失败=warn（copied/path/error）；tempdir Drop 清理失败=warn | 混合 |
| cache | `skeleton::cache::{evict,purge}` | evict（kind=ttl/ttl_sweep/capacity，key 指纹/僵尸数/LRU 序）=debug（真回收才发）；purge=info（回收数/残留） | 混合 |
| shutdown | `skeleton::shutdown::{registry,run,report}` | 注册成功/逐组件开始/终态 Stopped/整体报告=info；TimedOut/Failed=warn | 混合 |
| iter | `skeleton::iter::process_results` | 首 Err 短路=warn（pos=已产出 Ok 数，0 起）；next/fold 双通路各一点 | warn |
| fx | `skeleton::fx` | 托管任务 panic 归一为 `Panicked` 后若句柄已被丢（结果无人消费）=warn（msg=panic 文案）；`Ok`/`Cancelled` 无人接属设计内，不埋 | warn |

target 规范：域=`skeleton::<domain>`，子流程=`skeleton::<domain>::<phase>`——EnvFilter `skeleton::retry=debug` 按流程开，`skeleton=info` 按域开。键身份以 `DefaultHasher`（定长种子）指纹承载（跨 Rust 版本不承诺稳定，仅进程内日志关联）。开启方式：`--features log-embed` + 宿主 tracing 订阅（见 [`logx`](#logx--日志门面) 的 RUST_LOG 姿势）。

---

## feature 组合与必选关系速查

| 组合 | 形态 | 说明 |
|---|---|---|
| `full` | 一键全开 | 本 crate 14 消费面叶子并集（不含 `unsafe-evidence` 证据开关；`--all-features` = full + unsafe-evidence）；采用方默认心智入口——「不懂就 full」，子开关=进阶裁剪。std-ext 七域 48 埋点由其自有 `log-embed` 承载（ADR 0005 否决跨 crate 强制耦合）；workspace 全量=两 crate 各自 `--all-features` |
| `--no-default-features` | 纯生态件面 | 常驻 14 域（行为件面；十三域零门常驻、本体纯 std；`json` 域与凭据级 `RandomId`（`random` 门）按需显式开）+ memgov 可独立（`--features memgov`） |
| 默认 | runtime + pool | pool 连带 memgov；SyncStore/PoolBase/ThreadPool 可用 |
| `log-fs` 单开 | log-fs 面零三方 | 文件日志可用（mask_secrets/DefaultPathResolver/FileSink），不拉 tracing 四件套；std-ext 常驻依赖仍在树内（**不含** `serde_json`：`json` 域经 `json` feature 才进树，`log-fs` 不点亮它） |
| testing | tokio + runtime | TestStore/ScriptedRuntime/parse_mock |
| tracing | runtime + deps | 桥在审计值面上消费 |
| config-toml | config-json + toml | 值面叠层；toml 仅此 feature 可达 |
| bench `infra` | tokio+journal+memgov+testing+pool | required-features 并集（hollow gate 纪律） |
| example `end_to_end` | tokio+journal+testing+compose | 五件套+combine+TestStore+fx/journal 解释器标准形 |

零依赖证明口径：`cargo tree -p udf-skeleton-infra -e normal`（默认 feature 下 `skeleton` + `std-ext`，均 workspace path 非三方——**默认零三方**）；`--no-default-features --features memgov` 仍独立；`--no-default-features --features log-fs` 树内同样只有 workspace 路径依赖（`log-fs` 不点亮 `json`）。`serde_json` 是**门控依赖**（Cargo optional，ADR-0007 依赖隔离门），仅经这些 feature 进树：std-ext 侧 `json`；infra 侧 `view`（连带 `udf-std-ext/json`）、`journal`、`config-json`（后者连带 `config-toml`）。

