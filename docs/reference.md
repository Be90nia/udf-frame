# skeleton-infra / std-ext API 参考

> 权威口径：本文从 `src/*.rs` doc 注释、`tests/` 行为面与 `Cargo.toml` feature 表提取，与源码逐字对表（抽查记录见 `reports/2026-10-17-udf-api-reference.md`）。crate 级文档（`lib.rs`）仍是域典；本文是其展开的检索面。
>
> 姊妹 crate [`skeleton`](../skeleton)（零依赖）承载契约 trait 与数据词汇（`Store`/`Reducer`/`Effect`/`EffectRuntime`/`JournalPort`/`PoolManaged`/parse 端口等）；[`std-ext`](../std-ext)（批 5-1/5-2 物理拆分）承载十四个散件域（`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`/`paths`/`clock`/`retry`/`scheduler`/`ratelimit`/`cache`/`id`/`json`——十三域零门常驻、本体纯 std，`json` 域门控 `json` feature）；本 crate（skeleton-infra）兑现契约参考实现与核+胶水件。契约本身（trait 语义承诺）见 skeleton 文档，本文只写实现侧公共 API 与行为契约。

## 零依赖口径

`cargo tree -p udf-std-ext -e normal`：仅 `skeleton`（workspace 路径依赖）——零三方（开 `json` 门时 `serde_json` 子树按需进树；`log-embed` 埋点的 optional tracing 默认关不进树）。`cargo tree -p udf-skeleton-infra -e normal`——默认 feature 下仅 `skeleton` + `std-ext`（均 workspace 路径依赖）——零三方。各生态 feature 的三方依赖经 feature 门控（serde/tokio/tracing/toml 等），不随默认面进树。

口径说明：`json` 域随 P1-9 自 `xray-conf` 上浮，其 `serde_json` 依赖形态的决策见 ADR 0007。本节只陈述当前实测树。

下文各域标题里的「依赖面 … 零三方」说的是**该域自身代码**不调三方 API（如 `runtime` 域只用 `skeleton` 契约、`pool` 域只用纯 std）；**crate 级默认树**的第三方面以本节实测树为准（默认树零三方，可一行复核）。

## feature 矩阵

| feature | 依赖面 | 默认 | 内容 |
|---|---|---|---|
| `full` | 全部子开关 | — | **一键全开**（tokio 先例）：`features = ["full"]` = 下表全部子开关并集；新手心智入口，子开关=进阶裁剪面 |
| `runtime` | — | ✅ | `SyncStore`（契约参考实现，std-only）+ `LocalSubscription` + `audit` 审计值面 |
| `pool` | `memgov` | ✅ | `PoolBase` 管理组合件 + `ThreadPool` + `PooledThreadPool`；连带启用 `memgov` |
| （常驻，std-ext） | 十三域零依赖；`json` 域经 `json` feature 门引 `serde_json` | — | `text` `backoff` `retry` `paths` `clock` `scheduler` `ratelimit` `cache` `fsx` `encode` `id` `iter` `hash` `json` **十四域**（`std-ext` crate；行为件面；时间源/配置/日志路径契约在 `skeleton::clock`/`skeleton::config`/`skeleton::logpath`，批 1 契约真下沉；十三域零门常驻，`json` 域门控 `json` feature） |
| （常驻，本 crate） | 零依赖 | — | `shutdown` 停机编排器（消费 `std_ext::scheduler`） |
| `memgov` | 零依赖 | — | `CountingAllocator`/`BoundedRegistry`/`LeakGuard`/`MemoryPool` |
| `journal` | serde, serde_json | — | `JsonlJournal`：`JournalPort` 契约端口参考实现 |
| `tokio` | tokio | — | `fx`：`FxScope` 异步并发托管 |
| `testing` | `tokio` + `runtime` | — | `TestStore`/`ScriptedRuntime` + `parse_mock` 测试替身 |
| `log-fs` | 零依赖 | — | `logx` 文件落盘面：密钥掩码/路径解析（契约 `LogPathResolver` 住 `skeleton::logpath`）/`FileSink` 按日滚动——单开即可用文件日志 |
| `log` | `log-fs` + log, tracing, tracing-subscriber, tracing-log | — | `logx` 门面绑定层：init 系（EnvFilter/双 sink）+ `FileSinkWriter` |
| `tracing` | `runtime` + tracing, tracing-subscriber | — | `tracingx`：审计事件流 → tracing 桥 |
| `log-embed` | tracing | — | 埋点：skeleton-infra=shutdown 一域 6 点；std-ext 自有同款 feature=七域 41 点（合计八域 47；默认关=宏消除零依赖零开销） |
| `compose` | — | — | `combine`/`combine!` 父子分片组合器 |
| `config-json` | serde, serde_json | — | `ConfigFile` 分层装配 + 原子保存（serde_json::Value 统一值面） |
| `config-toml` | `config-json` + toml | — | `load_toml`/`save_toml` + `file_layer_toml`（TOML↔值面翻译） |
| `view` | serde_json + `udf-std-ext/json` | — | `view::ViewBridge` 语言无关视图桥（ADR 0006）：契约词汇住 `skeleton::view`（零依赖）；默认树零三方不变 |

> 注（批 1 契约真下沉，2026-10-18；ADR `docs/adr/0001-contract-sink.md`）：`skeleton` 由 optional 转常驻非 optional 依赖（常驻域 cache/id/retry/scheduler 消费 `skeleton::clock` 契约）——workspace path 非三方，`skeleton` 依赖面零三方口径不变。原 `runtime`/`pool`/`journal`/`compose` 的 `dep:skeleton` 叠门随之移除。

必选组合（`Cargo.toml` required-features 口径）：bench `infra` = `["tokio","journal","memgov","testing"]`；示例 `end_to_end` = `["tokio","journal","testing","compose"]`。 hollow gate 纪律：feature 不全的目标整目标跳过——CI 闸与首跑一律 `--all-features`。

构建与测试：`cargo build --workspace --all-features` / `cargo test --workspace --all-features`。

## 域索引

> 批 5-1/5-2 物理拆分：下表十四常驻域（text/backoff/retry/paths/clock/scheduler/ratelimit/cache/fsx/encode/id/iter/hash/json）已迁 `std-ext` crate，模块路径 `std_ext::<domain>`；`shutdown` 留守 skeleton-infra（`skeleton_infra::shutdown`）。各域行内「常驻」= 零 feature 门（`json` 域例外：门控 `json` feature，见 ADR 0007）。

| 域 | feature 门 | 一行职责 | 模块锚 |
|---|---|---|---|
| runtime | `runtime`（默认） | `SyncStore`：单写者队列+同步排空环+快照广播的契约参考实现 | [`runtime`](#runtime--syncstore-契约参考实现) |
| audit | `runtime` | 审计事件值面（事件/票据），发射点在 runtime | [`runtime::..audit`](#runtime--syncstore-契约参考实现) |
| pool | `pool`（默认） | `PoolBase` 管理组合件 + `ThreadPool` 动态线程池 | [`pool`](#pool--poolbase--threadpool) |
| pooled_thread_pool | `pool` | `MemoryPool` × `ThreadPool` 互喂组合件 | [`pool`](#pool--poolbase--threadpool) |
| memgov | `memgov`（pool 连带） | 计数分配器/有界注册表/泄漏断言 + `MemoryPool` 特化 | [`memgov`](#memgov--内存治理) |
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
| log-embed | `log-embed` | 八域 41 埋点规范（target/级别/字段纪律） | [`log-embed`](#log-embed--埋点规范) |

---

## runtime — SyncStore 契约参考实现

**feature**：`runtime`（默认）。**依赖面**：仅 `skeleton` 契约 crate（std-only，域面零三方——crate 树见 [§零依赖口径](#零依赖口径)）。

`SyncStore<S, A, Op = Infallible>` 兑现 `skeleton::Store` 契约：单写者队列 + 同步排空环（dispatch 返回即 idle，同序 action 序列恒同终态）+ CoW 快照广播 + 停机闸 + 回灌深度预算。`Op` 缺省 `Infallible` → `Effect::Run` 不可构造，零效果程序编译期证明无副作用（配 `skeleton::NoFx`）。

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
    pub fn cancel_unknown_id(&self) -> u64;
    pub fn runtime_panic_count(&self) -> u64;
    pub fn reducer_panic_count(&self) -> u64;
    pub fn feedback_budget_exceeded(&self) -> u64;
    pub fn subscribe_events(&self, capacity: usize,
        on_event: impl FnMut(&AuditEvent) + Send + 'static) -> AuditTicket;
    pub fn take_events(&self) -> Vec<AuditEvent>;
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

### 订阅双档

- `subscribe`（跨线程档）：回调需 `Send`，槽位 Mutex 串行化；返回 `Subscription<S>`（`#[must_use]`，Drop=注销即时摘表，无「一拍滞后」）。
- `subscribe_local`（单线程档）：回调免 `Send`（可用 Rc/RefCell），只在注册线程触发，免锁免原子量；返回 `LocalSubscription`（`#[must_use]`，`!Send + !Sync`——线程亲和的类型化表达）。同一 dispatch 内通知序：本地订阅先行，跨线程随后（确定性）。

### 可观测计数面（fail-visible，对齐 `subscriber_panics` 先例）

| 读数 | 何时的静默面 | 形态 |
|---|---|---|
| `try_subscribe` | 闭门后登记成功但永不触发 | 显式 `Err(StoreClosed)`（旧 `subscribe` 保留，闭门登记仍计入 `subscriptions_after_close`） |
| `local_missed_notifications` | 跨线程 dispatch 时本地订阅零触达 | 累计计数（读侧；命中路径零分支零开销） |
| `audit_preempted` | 审计事件流二次订阅静默覆盖前订 | 累计计数（`subscribe_events` 覆盖旧槽前留痕） |
| `cancel_unknown_id` | `Effect::Cancel` 指向从未发出的 `Run` id | 累计计数（不拦截——迟到 Cancel 属合法时序，核心无生命周期汇报面无法区分漂移；id 唯一性由采用方纪律保证） |
| `Combine::route_misses` | `combine` 路由 miss 静默吞 action | 累计计数（分片装配一次持有，读侧无锁无原子量之外成本） |

### Panic 契约

- `S::Clone` 必须 panic-free：外部持有快照时状态落位走 `Arc::make_mut` 克隆分支，clone 在 state 锁内 panic 即毒锁——**实例整体报废且状态不可取出**（文档化显式契约）。
- reducer panic 不报废实例：计数（`reducer_panic_count`）+审计现场（`AuditEventKind::ReducerInterrupted`）后原样重抛；action 已出队随栈展开丢失，状态不前进。
- 解释器 panic 不报废实例：毒锁恢复 + 计数（`runtime_panic_count`）。
- 订阅回调 panic：fail-closed 摘除 + `subscriber_panics` 计数 + `subscriber_panic_last_cause` 成因摘要。

### 回灌深度预算

`FEEDBACK_DEPTH_BUDGET = 64`：单任排空内连续回灌链深度超限即停排 + `feedback_budget_exceeded` 计数；触发点回灌 action 原值退回队尾（零丢失），本任 dispatch 随即返回——非 panic 非静默。预算按 dispatch 调用栈深度统一计数（TLS 在途深度，back 通道不绕预算）。时间线现场 = `AuditEventKind::FeedbackBudgetExceeded`。

### audit — 审计事件值面

```rust
pub struct AuditEvent { pub seq: u64, pub kind: AuditEventKind }

pub enum AuditEventKind {
    Enqueued { feedback_depth: u64 },
    Reduced,
    ReducerInterrupted,
    EffectForwarded,
    EffectInterrupted,
    Notified,
    Rejected,
    FeedbackBudgetExceeded,
}

#[must_use = "审计票据被丢弃即立即关闭事件流……"] pub struct AuditTicket { /* Drop = 关流 */ }
```

生命周期重建协议：记账单位是 **action 而非 dispatch**——一笔 dispatch 主 action 走 `Enqueued(depth) → Reduced → [EffectForwarded* | EffectInterrupted] → Notified?`；Emit 回灌派生的每个动作各走一条同形链交错嵌进主链时间线。`Notified` 仅当拍确有状态快照订阅者收到快照时记录。`seq` 单调，订阅窗内**连续**——空洞 = 有界环溢出丢最旧的显式证据。默认关闭零开销：未订阅路径 = 每 emit 点一次 gate load 即返。多订阅者间逐字交错顺序不可预推；时间线断言用语义不变量（深度链/节点计数），勿钉逐字全局顺序。

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

**锚口径（诚实化）**：上述 ns 级 dispatch 锚**仅在小 State 下成立**——锚的测量对象是状态迁移本身的固定开销，不含状态体量的复制成本。`Arc<State>` 是 CoW 快照：外部持有快照时（强计数 ≥2），状态落位走 `Arc::make_mut` 克隆分支，每次 dispatch 额外付一次 `S::clone`（在 state 锁内执行）。审计实测（`reports/2026-10-27-udf-architecture-audit.md` F-05）：

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

---

## notify — 异步信号式通知（案 C 水位）

**feature**：`runtime`（默认，随 SyncStore）。**依赖面**：契约 `skeleton::notify` 零依赖；实现面零新三方。决策与论证唯一权威 = `docs/adr/0008-async-notify-signal.md`。

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

## pool — PoolBase + ThreadPool

**feature**：`pool`（默认，连带 `memgov`）。**依赖面**：纯 std + 契约词汇（`skeleton::pool`），域面零三方（crate 树见 [§零依赖口径](#零依赖口径)）。

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
}
```

- 校验规则一套两档出口：`max == 0` / `idle_cap == 0` / `idle_cap > max` → 构造期 panic、运行时 `Err(PoolResizeError)`（拒绝路径零状态变更）。
- 归还裁决 RAII 唯一自洽：归还永不阻塞永不报错；槽满 = 丢弃 + `PoolMetrics::dropped_overflow` 计数（fail-visible）。
- 审计发射唯一收口：未订阅 = 一次 gate load 即返；订阅态 = seq 单调入账（容量环溢出丢最旧）+ **锁外**回调（回调内重入 emit 只录环——TLS 在途标记封自指递归）；回调 panic 摘除留痕。
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
pub struct PooledThreadPool<T, F = fn() -> T> { /* mem: Arc<MemoryPool<T,F>>, threads, … */ }
impl<T, F> PooledThreadPool<T, F>
where T: Send + 'static, F: Fn() -> T + Send + Sync + 'static
{
    pub fn new(mem: Arc<MemoryPool<T, F>>, threads: ThreadPool, borrow_timeout: Duration) -> Self;
    pub fn submit(&self, f: impl FnOnce(&mut T) + Send + 'static) -> Result<(), RejectedTask>;
    pub fn mem_pool(&self) -> &MemoryPool<T, F>;
    pub fn threads(&self) -> &ThreadPool;
    pub fn borrow_rejected(&self) -> u64;
}
```

借还在 worker 线程任务闭包内（`PoolGuard` 非 `'static`，提交线程不可持）：worker 认领后借缓冲 → 跑 → guard Drop 即还（panic unwind 同覆盖）。借空（超 `borrow_timeout` 无缓冲）= Discard 等价：任务体丢弃 + `borrow_rejected` 计数——与提交侧 `rejected` 是两个独立计数。`borrow_timeout` 构造期必填（无默认无限等）。

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

Drop 兜底尽力而为——**规范用法=测试尾显式 `check()`**/`expect_empty`/fx 侧 `expect_idle`。判据三（任务面）在 `fx::expect_idle`。

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
    pub fn remove(&mut self, k: &K) -> Option<V>;
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

**feature**：常驻，零 feature 门。**依赖面**：纯 std。**批 1 契约真下沉**（2026-10-18；ADR `docs/adr/0001-contract-sink.md`）：时间源契约 `Clock`/`SystemClock`/`MockClock` 现住 `skeleton::clock`（被 scheduler/ratelimit/retry/cache/id 多域消费的核心契约住契约层）；本 crate 的 `clock` 模块只余行为件（时长格式化 + RFC 3339）。

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
    pub fn tick(&self) -> usize;                           // 点火全部到期任务，返回实际点火数
    pub fn next_deadline(&self) -> Option<Instant>;        // 含惰性墓碑（park 依据）
    pub fn close(&self);                                   // 幂等；回调内调用合法
    pub fn is_closed(&self) -> bool;
    pub fn panic_count(&self) -> u64;                      // 点火 catch_unwind 命中计数
    pub fn worker_passes(&self) -> u64;                    // 驱动循环圈数（零自旋证据）
    pub fn spawn_worker(self: &Arc<Self>) -> WorkerHandle;
}

pub struct TaskHandle { /* … */ }   // RAII（Drop 即 cancel）；非 Clone；Send + Sync
impl TaskHandle {
    pub fn cancel(&self) -> bool;   // 线性化点=状态 CAS；幂等（已完成取消返 false）
    pub fn detach(self);            // fire-and-forget 逃生门（消费句柄不触发 Drop-cancel）
}

pub struct WorkerHandle { /* … */ }
impl WorkerHandle {
    pub fn join(self);   // = close + 等线程退出；worker 侧 panic 原样重抛
}                        // 禁在驱动回调内 join 同一 worker（自等死锁）

pub struct Closed;                  // 关闸后注册的显式拒绝
```

语义拍板：

- **every = 锚定周期，超载退化固定延迟**：重臂锚 `prev_deadline + period`（零漂移）；回调超载（完成时刻已越过下锚）改锚 `完成时刻 + period`——无补发无追帧（tokio `MissedTickBehavior` 默认 Burst 连发追赶是坑，本件显式不采纳）。
- **cancel 线性化点 = 状态原子位**：CAS 置位成功即承诺此后不再点火；已出堆条目检查-执行窗口内落地的取消不可撤（本次照跑，文档化语义；单线程 tick 确定性面无此窗口）；执行中取消 ⇒ 周期任务不再重臂。
- **close = 关闸 + 清 pending + 在途跑完**：注册显式 `Err(Closed)`；pending 全部标 Cancelled（句柄侧可见）；驱动线程随后退出。
- **回调 panic 不杀驱动**：点火 catch_unwind 摘除 + `panic_count` 留痕；周期任务 panic 后摘除不再臂。
- **惰性墓碑**：cancel 不摘堆（O(1)），死亡条目到点弹出时摘除——远期取消的墓碑多醒一拍（语义正确，代价一次空醒）。
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
- 时钟只消费单调面；**并发面：单线程 `&mut self` 面 + `SyncLruCache`/`SyncTtlCache` 并发薄包装（`&self`+内部 Mutex，语义与单线程版一致；poison=fail-fast panic 带因；分片留待命中率证据）**。
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

**feature**：文件面挂 `config-json`/`config-toml`。**依赖面**：`config-json` 增 serde/serde_json；`config-toml` 叠加 toml。**批 1 契约真下沉**（2026-10-18；ADR `docs/adr/0001-contract-sink.md`）：配置契约 `EnvSource`/`StdEnv`/`MockEnv`/`ConfigPort`/`ConfigError` 现住 `skeleton::config`（纯 std 零门）；本 crate 的 `config` 模块只余文件行为件（分层装配/原子保存/TOML 翻译）。

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

## fx — 异步并发托管

**feature**：`tokio`。**依赖面**：tokio（JoinSet/oneshot/watch/time::timeout/Semaphore）；CancellationToken 树/tokio-util 不引入。`tokio::spawn` 直调经 clippy `disallowed_methods` 闸封死——**效果只经 `FxScope` 命名口**（`spawn_tracked`/`spawn_detached`，后者为闸内唯一 allow 豁免）。

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
    pub fn spawn<T>(&self, fut: impl Future<Output = T> + Send + 'static)
        -> Result<Spawned<T>, ScopeClosed>
    where T: Send + 'static;
    pub async fn shutdown(&self);                          // 三段：close→abort_all→等净
    pub async fn shutdown_timeout(&self, d: Duration) -> Result<(), TaskError>;
}
impl Default for FxScope { /* new() */ }

pub struct Spawned<T> { /* … */ }   // Drop 不 abort——任务归生命周期集托管
impl<T: Send + 'static> Spawned<T> {
    pub async fn join(self) -> Result<T, TaskError>;
    pub async fn join_timeout(self, d: Duration) -> Result<T, TaskError>; // 超时先 abort 再回 TimedOut
    pub fn abort(&self);               // 幂等
    pub fn is_finished(&self) -> bool;
}

pub enum TaskError { Cancelled, Panicked(String), TimedOut }
pub struct ScopeClosed;   // accepting 闸已关：spawn 拒新（fail-closed 显式）

pub struct CancelSource { /* … */ }
impl CancelSource {
    pub fn cancel(&self);            // 幂等；观察面恒见 false→true 恰一次
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
pub fn expect_idle(scope: &FxScope);   // in_flight==0 断言（LeakGuard fx 侧锚点）
```

- 任务结局归一：panic 经 catch_unwind 归一 `TaskError::Panicked(消息)`（payload downcast &str/String，否则哨兵文案）；发送端消亡=Cancelled；join 超时=TimedOut 且任务已 abort（禁留活口）。
- `spawn_tracked` 线性化=锁内原子 RMW 过闸 + 插入与 shutdown 的 abort_all 串行——过闸任务必被 shutdown 覆及。
- `shutdown_timeout` 超时即返回：剩余集 Drop=abort 兜底（零残留）。

```rust
use skeleton_infra::fx::{expect_idle, FxScope};

let scope = FxScope::new();
let handle = scope.spawn_tracked(async { 41 + 1 }).unwrap();
// assert_eq!(handle.join().await, Ok(42));  // 需 tokio 运行时：见 tests 与 fx.rs doctest
expect_idle(&scope);
```

> 注：`join()` 需在 tokio 运行时内 await；独立可编译片段见 `examples/quickstart.rs` 与 `src/fx.rs` 单元测试（`#[tokio::test]` 五件套：spawn/abort/panic/timeout/close 语义全钉）。

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

**feature**：两层拆分（批 1，2026-10-18；ADR `docs/adr/0002-log-feature-split.md`）——`log-fs`（纯 std 零三方——域面口径，crate 树见 [§零依赖口径](#零依赖口径)：密钥掩码 + 路径解析 + `FileSink`，单开即可用文件日志）与 `log`（=`log-fs` 叠门 + log + tracing + tracing-subscriber + tracing-log：init 系门面绑定层）。日志根目录契约 `LogPathResolver` 住 `skeleton::logpath`。门面 trait=生态 `log::Log`（不自造）；核心自身永不打日志（核心零依赖，结构性排除）。

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

每事件一条 `tracing::event!`（target `skeleton::audit`，INFO 级），字段 `seq`/`kind`（生命周期节点名=过滤聚合稳定键）/`depth`（仅 Enqueued 携带回灌深度）。`capacity` 语义同 `SyncStore::subscribe_events`；返回票据 Drop=停桥（零开销恢复）。

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

八域 41 调用点，覆盖 scheduler/ratelimit/retry/backoff/fsx/cache/shutdown/iter。纪律（lib.rs `embed_event!` 注）：

- **只埋行为决策点**（register/cancel/fire/裁决/拒绝/等待分段），禁埋纯计算路径（`backoff_delay` 的 debug 入参事件是派单契约显式点）。
- **字段化禁格式化**：字段只收数值/布尔/静态串，禁 format!/Display。
- **关联序号进字段**：seq（scheduler 堆条目）/attempt（retry 轮次）/round（ratelimit 阻塞重查轮）承载跨事件关联，不起 span。
- 级别规范：正常流转=debug / 异常路径=warn / 终态=info。事件全部在状态锁外发射（订阅方 IO 不持锁）。

### 域级埋点表

| 域 | target | 事件点 | 级别 |
|---|---|---|---|
| scheduler | `skeleton::scheduler::{register,cancel,close,fire,worker}` | 注册/取消/点火/墓碑跳过=debug；关闸拒绝、回调 panic、超载退化重臂、驱动内部 panic=warn；close 与 worker 退出=info | 混合 |
| ratelimit | `skeleton::ratelimit::{try_acquire,acquire}` | 快路径拒绝与超容量拒绝=warn（available/wait_ns/capacity）；阻塞等待分段与醒后重查=debug（round/deficit）；成功扣减零事件 | 混合 |
| retry | `skeleton::retry::{attempt,verdict,backoff}` | 尝试开始=debug；重试放行=warn（backoff_ms）；停机终态=info；退避截平=warn（linear_ms→capped_ms） | 混合 |
| backoff | `skeleton::backoff` | 计算入参/结果（attempt/base_ms/max_ms/delay_ms）=debug（纯函数域唯一埋点） | debug |
| fsx | `skeleton::fsx::{walk,copy,tempdir}` | walk 帧推进=debug（depth/sort）；copy 逐项=debug + 部分失败=warn（copied/path/error）；tempdir Drop 清理失败=warn | 混合 |
| cache | `skeleton::cache::{evict,purge}` | evict（kind=ttl/ttl_sweep/capacity，key 指纹/僵尸数/LRU 序）=debug（真回收才发）；purge=info（回收数/残留） | 混合 |
| shutdown | `skeleton::shutdown::{registry,run,report}` | 注册成功/逐组件开始/终态 Stopped/整体报告=info；TimedOut/Failed=warn | 混合 |
| iter | `skeleton::iter::process_results` | 首 Err 短路=warn（pos=已产出 Ok 数，0 起）；next/fold 双通路各一点 | warn |

target 规范：域=`skeleton::<domain>`，子流程=`skeleton::<domain>::<phase>`——EnvFilter `skeleton::retry=debug` 按流程开，`skeleton=info` 按域开。键身份以 `DefaultHasher`（定长种子）指纹承载（跨 Rust 版本不承诺稳定，仅进程内日志关联）。开启方式：`--features log-embed` + 宿主 tracing 订阅（见 [`logx`](#logx--日志门面) 的 RUST_LOG 姿势）。

---

## feature 组合与必选关系速查

| 组合 | 形态 | 说明 |
|---|---|---|
| `full` | 一键全开 | 本 crate 13 子开关并集（等价本 crate `--all-features`）；采用方默认心智入口——「不懂就 full」，子开关=进阶裁剪。std-ext 七域 41 埋点由其自有 `log-embed` 承载（ADR 0005 否决跨 crate 强制耦合）；workspace 全量=两 crate 各自 `--all-features` |
| `--no-default-features` | 纯生态件面 | 常驻 14 域（行为件面；十三域零门常驻、本体纯 std；`json` 域与凭据级 `RandomId`（`random` 门）按需显式开）+ memgov 可独立（`--features memgov`） |
| 默认 | runtime + pool | pool 连带 memgov；SyncStore/PoolBase/ThreadPool 可用 |
| `log-fs` 单开 | log-fs 面零三方 | 文件日志可用（mask_secrets/DefaultPathResolver/FileSink），不拉 tracing 四件套；std-ext 常驻依赖（及其 `json` 域 `serde_json`）仍在树内 |
| testing | tokio + runtime | TestStore/ScriptedRuntime/parse_mock |
| tracing | runtime + deps | 桥在审计值面上消费 |
| config-toml | config-json + toml | 值面叠层；toml 仅此 feature 可达 |
| bench `infra` | tokio+journal+memgov+testing | required-features 并集（hollow gate 纪律） |
| example `end_to_end` | tokio+journal+testing+compose | 五件套+combine+TestStore+fx/journal 解释器标准形 |

零依赖证明口径：`cargo tree -p udf-skeleton-infra -e normal`（默认 feature 下 `skeleton` + `std-ext`，均 workspace path 非三方；std-ext 的 `json` 域带 `serde_json` 普通依赖进树）；`--no-default-features --features memgov` 仍独立；`--no-default-features --features log-fs` 树内同样含 std-ext 及其 `serde_json` 子树。

