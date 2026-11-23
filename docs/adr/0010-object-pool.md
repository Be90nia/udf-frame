# ADR 0010：通用对象池件（ObjectPool）——reset 钩子 + 有界降级 + 分片槽位

- 状态：**已采纳（2026-11-02，用户线 PM 拍板；实现落地中）**
- 关联：GitHub issue#15（消费方 `xray-core-rust` 提单）；消费方 bd 票 `Xray-core-rust-cr3ey`；消费方报告 `reports/audit-2026-11-02/design-buffer-pool.md`
- 前序件：`skeleton_infra::memgov::MemoryPool`（无状态对象池，阻塞等待 + 超时失败）、`skeleton_infra::pool::PoolBase`（池域记账/审计基座）、`skeleton::pool::PoolManaged`（池化管理面契约）

---

## 背景：消费方的两条真实场景

1. **加解密上下文池**（issue 原文 + 用户线复述）：加解密频繁 → 每次重新 `new` 加密上下文（密钥调度是最贵的一步）纯属浪费 → 池里放一个加解密对象，取用即用、用完丢回；**长期没人用了它就该被释放掉**（不许把内存换性能换成内存泄漏）。
2. **WebSocket 连接对象池**（用户线复述）：新连接来了、参数是新的 → 从池里捞一个已存在的连接对象 → **改一下连接参数** → 直接复用。若池只能「存放管理」而不能让对象被捞出来改参数重用，那它就不是池。

两条场景收敛到同一件能力：**「池化有状态对象」**——现有 `MemoryPool` 只能池无状态物（构造器只负责造，归还不重置），消费方因此在主仓自建了三套并行池（且档位错配：请求 64KB 拿到 128KB 块）。本仓要补的是这个缺口，不是再补一个缓冲池。

消费方铁律：改 udf 走独立回路（本仓三平台 CI 绿 + 消费方换依赖复测 + **消费方侧 API 面不得变化**）→ **本件 API 形状须一次定稳**。

---

## 决策

新增独立件 `skeleton_infra::objpool::ObjectPool<T>`（feature `pool`），语义契约如下。

### 1. 捞出即改参数（DerefMut 是主能力，不是 reset 钩子）

`acquire()` 返回 `Pooled<'_, T>`，实现 `Deref`/`DerefMut<Target = T>`：调用方拿到 `&mut T` 直接改连接参数 / 加密状态即可复用。**池不猜「该重置什么」**——重置策略归调用方（这正是 issue「只重置必要部分，保留可复用部分」的本意：池提供能力，不替业务定策略）。

### 2. reset 钩子（可选，单钩子 + 相位枚举）

`ResetPhase::OnReturn`（默认：归还时重置——与 .NET `IResettable`、HikariCP dirty-bit 同相）或 `ResetPhase::OnAcquire`（取出时重置——与 deadpool `Manager::recycle` 同相）。钩子签名 `Fn(&mut T)`，在**锁外**、**独占借用**下调用。

外部实践无统一答案（Commons Pool2 = 双端 passivate/activate 分离；.NET/HikariCP = 归还时；deadpool = 取出时；bb8/r2d2 的 `on_acquire` 实为**创建时**，命名误导）→ 本件以相位枚举同时覆盖两端形态，调用方按场景选。

**归还路径的 reset 走 `catch_unwind` 兜底**：reset panic ⇒ 该对象状态不可信 ⇒ 丢弃（不入 idle）+ 计数留痕，**禁 abort**（Drop 期 panic 在 unwind 中会直接 abort 进程）。这是本件与 .NET/HikariCP 的差异点，理由是 Rust 的 Drop-unwind 语义。

### 3. 有界降级（永不阻塞、永不失败）

`acquire()` 无超时参数、结构上不存在等待：
- idle 有货 → 复用（`reused` 计数 +1）；
- idle 空且池宇宙未满 → 工厂新建；
- **池宇宙已满 → 降级直构**（`degraded` 计数 +1），该对象**不入池宇宙**、归还即释放。

「池宇宙」= idle 槽 + 正常借出数，硬上界 `max`；**保留量**硬上界 `idle_cap`。降级对象的构造量由用户并发决定（`.NET ObjectPool` 官方口径：「不限制分配数，只限制保留数」），但池的**常驻保留量**永不超过 `idle_cap` → 内存换性能不会变成 OOM。

### 4. 长期不用的对象会释放（三条路径）

- `idle_cap` 满 ⇒ 归还即丢弃（`dropped_overflow` 计数）；
- `prune_idle(max_idle_age, now)` 显式老化口（宿主按自己节奏调；**池不起后台线程**——不引生命周期暗面）；
- 池 `drop` ⇒ 全部 idle 对象析构。

空闲槽记录 `last_used` 时间戳（归还时刷新），`prune_idle` 按龄释放。

### 5. 借出期池不钉住对象（issue ② 的行为落点）

idle 槽存的是**值**（`T`），借出时**移动**出槽（强引用转移给 `Pooled`）⇒ 借出期池不持强引用。`Pooled::detach(self) -> T` 让对象彻底脱离池生命周期 ⇒ 调用方最后一个持有者放手即自然析构（Rust 天然，无 GC 兜底、无额外回收器）。

**形状上不采用「idle 槽存 `Weak`」**——见下节「被否决备选」。

### 6. 分片槽位

> **⚠️ 2026-11-02 r2 改造取代本节**（见下方「r2 改造」小节）：分片容器已从 `Mutex<Vec<IdleSlot<T>>>` 升级为**片内自带计数**的 `Mutex<Shard<T>>`（`Shard{idle, borrowed, alive, reused}` + `#[repr(align(64))]`）；借用游标已从「每 op 轮转」改为**线程亲和片**（TLS 小数组按池地址查）；`max`/`idle_cap` 改为**按片均分**；归还新增**迁片留存**。本节保留为 r1 设计记录。

`shards` 片（构造期自适应：`clamp(idle_cap, 1, 8)`，**不设旋钮**，同 `MemoryPool`「内部自适应不设旋钮」先例），每片一把 `Mutex<Vec<IdleSlot<T>>>`（LIFO，热件常驻热缓存）。借用走本线程片（thread-local 游标，零共享热点），本片空则轮转扫其他片（提命中率）；**归还回借出时的片**（亲和）。

`resize`（`PoolManaged` 管理面）只改 `max`/`idle_cap`，**不改分片数**（分片数变更 = 槽位重分布，复杂度不值；文档钉死）。

### 6b. r2 改造（2026-11-02，实测驱动；**当前实现以此为准**）

**动因（实测）**：dim4 分片并发实测 8 线程扩展比 **0.51x**（比单线程还慢一倍）——槽位锁虽分片，但 `borrowed`/`idle`/`alive`/`reused` 仍是全局原子，每次借还 5-6 次同 cache line 的 lock 前缀操作 ⇒ 缓存行乒乓。**真凶用对照夹具锁定**：同代码 × 8 个独立池 = 4.69x、共享 1 池 = 1.16x ⇒ **跨核行迁移**（每 op 轮转换片，每片的热行在核间往返），非原子争用。

| # | 改造 | 效果（dim4 扩展比路径） |
|---|---|---|
| 1 | **片内计数**：`Shard<T>{idle, borrowed, alive, reused}`，锁内普通 `usize`（零原子；`constructed`/`degraded`/`pruned`/`panic_dropped`/`dropped_overflow` 仍全局——全在 miss/降级/老化/拒绝等非热路径） | 0.51x → 0.75x |
| 2 | **`#[repr(align(64))]`** 防伪共享（`Vec<Mutex<Shard<T>>>` 元素不再挤同一 cache line） | 0.75x → 1.09x |
| 3 | **线程亲和片**：TLS 小数组（按池地址线性查，k=4）⇒ 每 (线程, 池) 稳定一个片号，首线程取 0 号（单线程行为确定）；轮转只用于 miss 时跨片扫 | 1.09x → **4.5x** |
| 4 | **`max`/`idle_cap` 按片均分**（`share()`：余数补给前 `total % shards` 片，**总和恒等于配置值** ⇒ 硬上界性质不变） | —（正确性配套） |
| 5 | **迁片留存**：归还时 home 片配额满 ⇒ 轮转找有余量的片**迁入**（`alive` 随迁、**先迁入再退 home** ⇒ 账面永不低估；不做则单线程只能用 `idle_cap/shards` 额度，`resize_shrink_prunes_excess_idle` 实测红） | —（正确性配套） |

PM 亲测 dim4（Windows 10 x64，有后台负载）：单线程 71.2 ns/借还 → 8 线程聚合 33.3 ns/借还，**2.14x**；实现侧无负载时 4.66x。**两者都 >1，具体倍数受环境负载影响**（同仓「读数环境相关」口径）。

**r2 新增登记上限**：①每片仍一把 `Mutex`（无锁栈登记为升级路径）；②`constructed` 等为全局原子（非热路径）；③`metrics()` 为 **O(shards)**（逐片取锁求和，仅调用时）；④归还路径含一次 `Instant::now()`（老化时间戳，约 20-30ns，计入单线程基线）；⑤迁片路径多一次片锁（仅 home 满时）；⑥`AFFINITY` 以池地址为键，地址复用最坏只偏移起始片（正确性不依赖）。

### 7. 统一管理面（实现 `PoolManaged`）

> **⚠️ 2026-11-02 r2 改造取代本节「计数用原子」表述**：热路径计数已下沉进分片（锁内普通 `usize`），仅 `constructed`/`degraded`/`pruned`/`panic_dropped`/`dropped_overflow` 保留全局原子（非热路径）；`metrics` 因此为 O(shards) 而非 O(1)（逐片求和，仅调用时）。

`skeleton::pool::PoolManaged` 契约（resize 事前校验 / metrics O(1) / 审计流）→ 本件实现之，计数用**原子**（分片件的记账不借 `PoolBase` 的单锁账本——那把锁会成为全局争用点）。审计环机制从 `PoolBase` 内部**抽出为 `PoolAuditSink` 供两池共用**（公共逻辑写一次；`PoolBase` 公开面与行为零变化，既有 `tests/pool_base.rs` 必须仍绿）。

---

## 既有面核对（造轮子前三道门）

| 既有件 | 结论 |
|---|---|
| `skeleton::pool::{PoolManaged, PoolMetrics, PoolResizeError, PoolAuditEvent/Kind/Ticket}` | **复用**（契约词汇，零改动） |
| `skeleton_infra::pool::PoolBase` 的审计环（`AuditSlot`/`emit`/`subscribe_audit`/`seq`/`gen`/回调 panic 摘除） | **复用**：抽为 `PoolAuditSink`（`pub(crate)`）供两池共用；`PoolBase` 公开面零变化 |
| `skeleton_infra::pool` 的 `validate`（max/idle_cap 非法参数规则） | **复用**：`pub(crate)` 化供两池同源 |
| `skeleton_infra::memgov::MemoryPool` | **不复用**：①语义冲突（本件「不阻塞不失败、降级直构」vs 它「阻塞等待 + 超时失败」）②槽位是单锁 `Mutex<PoolInner>`（本件要分片）③无 reset 钩子。**并存**（两者语义不同，非替代关系） |
| `skeleton_infra::pool::PoolBase` 的账本（单 `Mutex<PoolCore>`） | **不复用**：全局记账锁与分片槽位件冲突；本件计数走原子 |
| 本仓 `Weak` 范式（`runtime.rs` 订阅表存 `Weak` + 票据持强引用 + `Drop` 即时摘表；`runtime/notify.rs` `NotifySinkSlot` 同构） | **不套用**：那套范式的前提是「对象生命周期归票据」，与「池要复用对象」的前提相反（见下） |
| `examples/memory_pool_profile.rs` 的 Treiber stack | **不落产品**（该文件自述「只在采集器内实现不落产品」）；本件分片用每片一锁（`ponytail:` 注明升级路径） |

---

## 被否决备选

1. **idle 槽存 `Weak<T>`（issue 字面形态）——否决**。**结构性矛盾**：要对象活着必须有人持强引用，而强引用只有两个来源——池自己（那就回到「池持强引用」，形态上不是 Weak 槽）或外部使用方一直攥着（那不是「归还入池」而是外部缓存）。两来源都堵死「Weak 槽 + 复用保证」。外部一手依据：
   - **HikariCP `ConcurrentBag`**（源码注释）：借出的对象「不会从任何集合移除，GC 不会发生」；弱引用**只放在 ThreadLocal 缓存层**，`sharedList` 权威存储一律强引用；弱引用被清掉时处理是「跳过，继续扫强引用表」——**权威存储一旦改成弱引用，复用保证即消失**。
   - **Go `sync.Pool`** 官方文档：`Get may choose to ignore the pool and treat as empty`——GC 驱动整池回收换来的是官方明说放弃复用保证。
   - **Rust `std::sync::Weak`** 文档：`Weak` 不阻止值被 drop，`upgrade()` 可能返回 `None`。
   - **Commons Pool2 `destroyObject`** javadoc 承认被 GC 收走的实例可能永远不会被 destroy。
   - Rust 生态「不持强引用但对象仍安全存活」的正解是 **epoch 回收**（crossbeam-epoch），不是 `Weak`。
   → 本件以**行为等价**的形态交付该诉求：借出期池不持强引用 + `detach()` 脱离池生命周期后自然析构 + `idle_cap`/`prune_idle`/池 drop 三条释放路径（详见 ADR「issue ② 映射」）。
2. **扩 `MemoryPool`（加 reset + 降级 + 分片）——否决**：它的 acquire 契约是「超时失败」，本件是「永不失败」；改它就是破坏性重定义既有公开语义（`MemoryPool::acquire(timeout) -> Result<_, PoolAcquireTimeout>` 是既有消费方的类型级依赖）。两者并存，语义互补。
3. **借 `PoolBase` 记账（单锁账本）——否决**：每次借还都碰同一把账本锁 = 全局争用点，与分片目标相抵；改为原子计数。
4. **自建第二套审计环——否决**（违反「公共逻辑写一次」）：抽出 `PoolAuditSink` 共用。
5. **池内起后台老化线程——否决**：引生命周期暗面（谁停它、停机序、跨运行时）；改由宿主显式调 `prune_idle`。
6. **构造期暴露 shards 旋钮——否决**（YAGNI + 同 `MemoryPool`「内部自适应不设旋钮」先例）；自适应即可，升级路径在文档注明。

---

## issue 四项能力 → 本件落点映射（逐条交代）

| issue 要求 | 本件落点 |
|---|---|
| ① reset 钩子 | `ResetPhase::{OnReturn, OnAcquire}` + `Fn(&mut T)` 钩子（锁外调用）+ `Pooled: DerefMut`（捞出改参数） |
| ② 弱引用自动释放 | **行为等价**：借出期池不持强引用（值移动出槽）+ `detach()` 后最后一个持有者放手即析构 + `idle_cap`/`prune_idle`/池 drop 三条释放路径。**形状改为不落 `Weak` idle 槽**（矛盾见「被否决备选 1」） |
| ③ 有界降级 | 池宇宙满 ⇒ 直接构造 + `degraded` 计数（不阻塞不失败）；保留量硬顶 `idle_cap` |
| ④ 分片 / 无锁 | 分片槽位（每片一锁）+ 原子计数 + thread-local 借出游标；**未做无锁 Treiber**（记为升级路径，`ponytail:` 注明） |

---

## 后果

正：消费方两套自建池可收敛到本件（加解密 ctx / WS 连接对象）；「捞出改参数即复用」由 `DerefMut` 直接支撑；池的常驻内存硬顶可证；统一管理面（`PoolManaged`）让宿主零特化。

负：idle 槽持强引用 ⇒ 长期不用的对象靠 `prune_idle`/容量淘汰释放（**不是**「无人持有即自动析构」——那与复用保证互斥，理由已钉）；归还路径的 reset 走 `catch_unwind`（panic 不 abort，但对象被丢弃）；分片数构造期定死，resize 不改。

**登记上限**（说清不藏）：①每片仍是一把 `Mutex`（非无锁 CAS）——升级路径 = 无锁栈；②`prune_idle` 需宿主显式调（池无后台线程）；③`reset` 钩子在归还路径执行时若 panic，对象被丢弃而非半态留存（fail-closed）；④~~分片命中靠 thread-local 游标 + 跨片扫，无亲和保证~~ → **r2 已改为线程亲和片**（TLS 小数组按池地址查，k=4）；多池线程退化为「最近使用的 4 个池各自亲和」；⑤`idle_cap` 与 `max` 的语义边界见各字段 doc（**r2 起为按片均分，总和恒等于配置值**）；⑥`metrics()` 为 O(shards)（r2 起）；⑦`constructed`/`degraded`/`pruned`/`panic_dropped`/`dropped_overflow` 为全局原子（非热路径）。

**回滚成本**：低。新增件 + `PoolBase` 内部审计环抽取（公开面零变化）→ 回滚 = 删 `objpool.rs` + 还原 `pool.rs` 抽取 + 删接线/测试/示例。

**风险与证伪**：①若消费方实际需要「idle 对象在无人触碰时自动消失」且不接受显式 `prune_idle` ⇒ 需引入 epoch 回收（crossbeam-epoch）或后台线程——届时新 ADR；②若分片一锁在消费方高并发下仍成瓶颈 ⇒ 升级无锁栈（登记为已知升级路径）；③若 `reset` 钩子在归还路径的成本高于构造成本 ⇒ 调用方应改用 `OnAcquire` 或干脆删对象（消费方自带的判据「reset 成本 ≪ 构造成本，否则优先删中间层」在本件同样成立，文档写明）。
