# ADR 0011：因果工作量预算——暂存期即受条数界 + reducer 转换全有或全无（#231）

- 状态：**已采纳（设计裁决 2026-11-27）**——#231 评审「Design review — proposed disposition」+ 评论「对待确认项的正式裁决（设计决定；不授权实现）」。**实现待授权**。
- 决策人：用户（#231 两条评论；本文只把裁决整理成契约，不新增决定）
- 关联：
  - #231（决策票与两轮评审）、#105 / #144（原始预算契约）、提交 `23ec9c9`（现行实现）
  - `docs/reference.md` §资源上限与默认值 第 11–13 行与「维护者评审（2026-11-27）」记录
  - ADR 0004（三 crate 分层：`EffectBuf` 属契约层 `skeleton`）
  - `docs/research/wave7-external-benchmarks.md` §4（Vue / Svelte 递归上限对标；本地留存，不入库）

---

## 1. 背景：现行实现与不追认的理由

现行（`23ec9c9`）的时序：

1. reducer 把全部输出写进暂存 `EffectBuf`；
2. reducer 返回后，框架才读取 `set_segment_effect_budget`；
3. 上限快照进 `PendingSegment`；
4. `forward_effects` 稍后截掉超量后缀。

这只约束了**转发**，没有约束 reducer 运行期间累积的**内存**——reducer 仍可在上限被读取之前暂存任意多的效果。超量的 `Run` / `Cancel` 还会形成「状态已提交、效果部分未转发」的分裂结局（提交后部分失败）。维护者评审（#231）因此**不追认**现行 per-reducer 语义为完整资源界，但保留默认启用的方向。

---

## 2. 契约（依 #231 正式裁决）

### 2.1 暂存期即受界（bounded staging）

- 在 reducer 入口快照配置的 per-reducer 上限（`0` 保持「不限」的既有含义）。
- 在效果缓冲的插入边界执行上限：暂存效果条数不得增长超过上限。
- 保留源码兼容：既有不可失败的 `push` 记录溢出并拒绝后续插入；可失败的 `try_push` 可直接返回结果（API 形态见 §3 与 §5.1）。

### 2.2 reducer 转换全有或全无（segment 级原子性）

- reducer 返回后、**发布其新状态或提交 / 转发其暂存效果之前**，先检查溢出状态。
- 发生溢出 ⇒ 该次 reducer transition 整体被拒：
  - 不发布该 transition 的 next state；
  - 不转发该 segment 中的任何 effect；
  - **不把超量 `Emit` 放入 deferred queue**，也不通过 `resume_deferred` 续接该 segment；
  - 返回**可区分的 typed budget-overflow outcome**，并增加**专属计数 / 审计记录**。
- 此前已提交的 segments 原样存活；本规则只作用于当前未提交的 reducer transition。
- **超限 action 没有部分进展**：调用方应调整预算或显式设为 `0`（unlimited）。
- **与 per-root 因果预算分立**：per-root 触发的后代 action `Deferred` 语义保持不变，两者不得混为一谈。

该规则消除了「状态可被提交而超量 `Run` / `Cancel` 静默不转发」的分裂结局。

### 2.3 不把 deferred 协议扩到 `Run` / `Cancel`

不以此为近期修法：那会引入尚未规定的顺序、身份、取消、重试与恢复语义。采用 §2.2 的全有或全无规则——它保留了既有的「状态 / 效果」提交边界，并让预算失败在发布之前显形。

### 2.4 per-drain：等待入口账本完成 ≠ 持有排空租约

现行 per-drain 上限不是无条件的硬上限：经 `try_dispatch` / `ActionSender::dispatch` 夺得租约者可越过上限继续执行，直到其入口 action 与因果子树落定。修订方向（保留「同步的逐 action 完成」契约）：

- 到界时，租约可以**让出所有权**，调用方继续等待其入口账本完成；
- 其他生产者可以接手；无人接手时原调用方可再次竞争；
- 交接全程保持 FIFO、单写者所有权、恰好一次记账、因果账本完成；
- 证明「无丢失唤醒 / 无队列搁浅」（没有其他生产者到来的情形）。

**已实现（2026-11-27，票② #234）**：到界必让出所有权；被接手时调用方先**认领**入口账本再等终局（认领期间账本不进复用池 ⇒ 复用的 `reset` 不可能清掉等待中的终局，无丢唤醒；进入分支时账本已终局则走不睡的快路径），醒来即返回（「静默态返回 = 本 action 的完成边界」不变）；无人接手则原线程重新竞争并续排（禁搁浅）。`ActionSender::dispatch` 的 `bool` 仍只表示「被接受」。**仍不是严格最大值**，但原因变为协作语义本身：无人接手时同一线程会连续排空整条队列（上限只决定它隔多久放一次手），有竞争时接手的租约按各自预算推进。

### 2.5 legacy `dispatch` 遇预算拒绝的显形（不 panic）

预算溢出是**资源保护拒绝**，不升级为进程级 panic。对 legacy `dispatch`：

- 不发布状态、不转发该 segment 的效果；
- 通过**专属计数**与**有界、非阻塞**的诊断 / 审计事件显形；
- **不在内部锁持有期间**调用用户回调，或执行可能阻塞的日志 / 审计 sink；
- 需要同步获知 typed 结局的调用方使用 `try_dispatch` 或 `dispatch_with_receipt(...).wait()`。

**API 文档必须写明**：legacy `dispatch` 的返回**不代表** reducer transition 一定成功提交；预算拒绝不会 panic，但可从诊断面观察——**不得把它伪装成普通成功**。

### 2.6 默认值与口径

- 默认值暂按临时兼容基线：per-root `16384`、per-reducer `4096`、per-drain `4096`。**不得**仅凭合成的 1,024 效果用例称其为「经实测验证的上限」。
- 定稿需：consumer-sim 代表性负载、峰值暂存内存测量、边界用例（cap-1 / cap / cap+1）、持续并发生产者。
- 三个上限都是**条数界**，不是字节界；不得写成对任意载荷大小的完整内存界。字节级保护需要独立的载荷计量契约。

---

## 3. `EffectBuf` 兼容性评审

现状：`skeleton::EffectBuf<A, Op>` 只有私有的 `VecDeque<Effect<A, Op>>`；公开面为 `new` / `push(effect) -> ()`（不可失败）/ `clear`（保容量）/ `is_empty` / `pop_front`。

| 方案 | 做法 | 兼容性 | 评价 |
|---|---|---|---|
| **A（推荐）** | `push` 签名不变；内部记录 `cap`（`0` = 不限）与溢出标志，到界后拒绝继续插入并置位；新增读侧 `overflowed()` 与实现面 `set_effect_cap(n)` | 源码兼容：所有既有 `push` 调用点零改动。布局变更：多 2 个机器字（类型无 FFI / `repr` 承诺） | 对 reducer 而言 `push` 是「不可见地被拒」，但框架在 reduce 返回后立即检查并以 typed 结局显形，不构成静默丢失 |
| B | 在 A 之上增加可失败的 `try_push(effect) -> bool` | 纯增量 | 让关心的 reducer 当场得知；可按需后加，不是落地 §2 的前提 |
| C | `push` 超限即 panic | 行为破坏 | **否决**：把资源上限变成崩溃面，与预算「typed、fail-visible、不 panic」的纪律相悖 |

**状态**：#231 的正式裁决逐条回答了「是否取代 Emit 停放」「legacy `dispatch` 是否 panic」「拆票与 bool 语义」「授权边界」，**未点名 A / B 的 API 形态**——§3 的 A 仍是本 ADR 的推荐，须在实现票 ① 开工前由维护者确认（见 §5.1）。

---

## 4. 验收证据（两张实现票必须逐条给出）

| # | 证据 | 载体 |
|---|---|---|
| 1 | reducer 无法把暂存效果条数增长超过配置上限 | cap-1 / cap / cap+1 边界测试 |
| 2 | 溢出不会发布 next state，不转发该 segment 任何效果，超量 `Emit` 不入 deferred queue | 溢出后状态不变 + 效果零转发 + deferred queue 无新增 |
| 3 | 后续 reducer 溢出时，此前已提交的 segments 原样存活 | 先提交一段、再触发溢出，断言前段完整 |
| 4 | 结局与计数能区分「预算溢出」「队列容量拒收」「解释器失败」 | 三类各自的 typed 结局 + 专属计数互不串账 |
| 5 | legacy `dispatch` 在预算拒绝时不 panic、不发布状态，可从诊断面观察；API 文档写明「返回值不代表 transition 已提交」 | legacy 路径用例 + 诊断面断言 + 文档核对 |
| 6 | per-drain 交接保持 FIFO、因果完成、恰好一次处理，并在有 / 无竞争生产者时都能推进 | 握手式并发测试 + 无接手者时的推进证明 |
| 7 | 文档清楚区分条数界与字节界、临时默认值与实测默认值 | `docs/reference.md` 正典表与本 ADR 状态同步 |

---

## 5. 未决问题

1. ~~**`EffectBuf` 的 API 形态**（§3）：A 还是 A+B~~ → **已定（2026-11-27 票① 实现轮）：采用 A**（`push` 签名不变、到界拒绝插入并置溢出标志）。理由：A 已满足「暂存条数不可能超过上限」；B（`try_push`）只让 reducer 感知拒绝，属后续可选增量，未实现。
2. ~~`try_dispatch` 上新预算结局的变体命名~~ → **已定：准入侧不加变体**。预算拒绝发生在**排空期的转换**而非准入期（action 已被接受），typed 面走 `dispatch_with_receipt(...).wait()` 的 `DispatchOutcome::BudgetRejected { rejected }`；`AdmissionOutcome` 语义不变。
3. ~~`segment_effect_budget_exceeded()` 保留 / 改名 / 拆分~~ → **已定：删除**（同名不同义 = 陷阱：旧=超量条数，新=被拒转换次数）。替代读侧 `segment_effect_overflow_count()`；0.1.0 未发布，按 ADR-0005 破坏性直迁先例处理。
4. per-drain 交接的具体唤醒机制，以及与「宿主不得 join 会回调同一 store 的线程」义务（`docs/reference.md` §runtime 边界规则 4）的关系。
5. 默认值定稿所需的实测负载与口径。

---

## 6. 后果

**正向**：预算成为真正的暂存期资源界；失败在发布前显形，不再有「状态已提交、效果部分丢失」的中间态；契约层只多两个机器字与一个读侧。

**负向 / 代价**：一次超量即整次转换被拒（没有部分进展）；`EffectBuf` 布局变大、每次 `push` 多一次长度比较；per-drain 交接是一次并发协议改造，风险与验证成本最高。

**实测幅度（2026-11-27 收口轮，同夹具三版本配对：5 轮、按臂过滤、顺序轮转）**：票① 的 per-push 上限检查使 **push 密集路径 +4.4% ~ +12%**（`effect_batch_forward` k16 +4.6% / k64 +8.6% / k1024 +11.4%，随批量线性增长）；票② ≈ 0（±1%）；`dispatch_per_beat/hit` +1.8%、`effect_steady_state` +2.1%（≈噪声）。⇒ 「暂存期硬界」这一裁决的实现成本已量化，是否接受 / 是否把检查压到更便宜的形态属维护者取舍（见终评报告 §四）。

**批量入口（opt-in，2026-11-27 票④ #237）**：旧 `push` 的**即时硬界不变**——对任意旧 reducer 的单条 `push`，写入前仍必须判断额度（把检查挪到 reduce 返回后、每 N 次检查一次、或让 `VecDeque` 自行扩容后再裁决，都会破坏 §2.1 的暂存期硬界）。可优化的是**显式批量写入**：新增 `EffectBuf::push_batch(iter)` 与实现面读侧 `remaining_limit() -> Option<usize>`（`None` = 不限、`Some(0)` = 精确零额度，与公共约定「`set_effect_cap(0)` = 不限」区分）。语义：写入前一次性快照剩余额度 R → 最多追加 R 条（循环内不再逐条判断）→ R 条后仍能产出一条则置溢出并整段拒绝，超出项不写入；`size_hint` / `ExactSizeIterator::len` **只作 reserve 提示，不是安全证明**；不先构造无界临时 `Vec`（直接填充根 `VecDeque`，reserve 只影响容量）；迭代器 panic 如实传播，由 runtime 既有 panic 路径清理。旧 reducer 继续调 `push` 时不自动获得批量收益。根 staged 仍是唯一提交缓冲、`mem::swap` O(1) 移交、不新增锁 / 原子计数 / `dyn` sink / MPSC。**收益与「消除中间缓冲」的结构性收益（§组合链融合）一起实测后再决定是否更换底层容器**。

**组合链全链预算（2026-11-27，票④ #235 一段）**：`combine!` 委托链不再让子缓冲无界——父缓冲把**全部剩余额度借给**子缓冲（`EffectBuf::reserve_all_remaining` 返回 RAII [`StagingLease`]），子 `reduce` 返回后归还未用部分，此后每转交一条再归还一个 credit。租约把父缓冲的有效上限压成「原上限 − 未归还 credits」，于是**任一时刻「父条数 + 子存活条数 ≤ 本段预算」**；嵌套组合时父层上限本身已扣除祖先存活缓冲 ⇒ 递归 ⇒ §1 的内存动机覆盖**整条链**。子缓冲溢出的事实经 `transfer_from` 沿父链传回（`mark_overflowed`），框架仍按全有或全无**整段拒绝**——不静默截断；旧 `lift_fx` 仍在 child reduce 返回后逐条调用（回调时序不变）。为此 `EffectBuf` 的内部上限表示改为 `Option<usize>`（`None` = 不限、`Some(0)` = **精确零额度**）：公共约定 `set_effect_cap(0)` = 不限 不变，但「零额度」与「不限」在内部必须可区分（借出额度时父缓冲正好需要「精确零可写」这一中间态）。租约 Drop 归还全部 credits 并恢复原上限（panic 展开也安全）。

**已闭合（#235 一段，2026-11-27）**：票① 实现轮登记的「`combine!` 委托链子件缓冲无界、内存动机只到父层」已由上述全链预算闭合——全链 live staging 受同一份条数预算约束，溢出沿父链传播后整段拒绝。

**批量入口的实测（2026-11-27，同二进制内对照 `benches/infra.rs::effect_push_forms`，Windows 本机）**：两臂写进同一个远大于批量的有限上限缓冲（只测边界判断、不测溢出），`Throughput::Elements(k)` 折算逐条。
- **每次插入的额度判断成本 = 0.06–0.11 ns/条**（有上限 vs 无上限的单条 `push`，其余形状完全相同；k=1024 时 +85 ns / 1637 ns ≈ 5%，k=16 ≈ 1%）。**远小于**先前推定的「~1.3 ns」。
- **`push_batch` 在当前实现下逐条更慢 ~9%**（k≥16：1.65–1.69 ns/条 vs 1.51–1.56 ns/条），另加 ~2.8 ns/次固定开销（k=1 时 5.33 vs 2.55 ns）——它去掉的判断比它引入的迭代器机制**更便宜**。⇒ 批量入口**没有独立收益**；保留它只为融合路径的 `IntoIterator` 形状（`EffectSink::push_many`），**不得**当作性能修复对外宣称。
- **归因修正**：上文「票① 的 per-push 检查使 push 密集路径 +4.4%~+12%」中，per-item 检查只能解释 **1–5 个百分点**（k16 ≈1%、k1024 ≈5%），其余未归因（候选：per-beat 机制与 `EffectBuf` 布局）。「票① 使 dispatch 路径变慢」是实测事实不变，改的是**归因**：继续优化应走「消除中间缓冲」的结构性路线，而不是在此处省分支。

**融合写入（opt-in，2026-11-27 #235 二段）**：给可融合的 reducer/fragment 一条**无中间缓冲**的写入路径。契约层新增 `EffectSink<E>`（`push` / `remaining` / `mark_overflowed` / `reserve_staging` / `release_staging` / 默认 `push_batch`；**方法级泛型 ⇒ 不承诺 `dyn` 兼容**）与 `FragmentInto`（`reduce_into<S: EffectSink<..>>`），`impl EffectSink for EffectBuf` 承担根适配器（**不新增 `EffectBufSink` newtype**——语义等价、少一个类型）；`SinkLease` 是 RAII 额度租约（Drop 归还剩余 credits）；`Buffered<F>` 把旧 `Fragment` 适配进新树并**保留旧效果回调时点**（走 §二 的有界兼容缓冲）。infra 侧新增 `MapSink`（持父 sink + 静态映射闭包：不装箱、不分配、不加锁；预算协作面**逐层转发**）、`CombineInto`（同时实现旧 `Fragment` 与新 `FragmentInto`——旧入口下 `EffectBuf` 本身就是 sink，因此同样零中间缓冲）与 `combine_into!` 宏。旧 `Fragment` / `combine!` **完全不动**；融合路径**不自动迁移**旧 `lift_fx`（纯映射约束：不做 IO、不写共享态、不依赖「回调在子 reduce 之后」；映射 panic 仍让根 transition 整体失败，框架不回滚其外部副作用）。`combine_into!` 必须显式给 `lift:` 或 `discard_effects`——融合路径不做零效果闸的隐式丢弃。验收：`tests/compose_into.rs` 三条（全链共用一份预算且停靠点观测到 `Some(预算−祖先已写)`、预算用尽 = 精确零额度且整段原子拒绝、旧 `Fragment` 经 `Buffered` 可进融合树）；变异两组（`MapSink` 不传回 overflow / 不转发 `remaining`）各自红。

**融合写入的实测（2026-11-27，同二进制内对照 `benches/infra.rs::effect_compose_forms`）**：同一 fixture（子件每拍产 k 条 `Run`、父层 1:1 上提），只换组合机制：`compat` = `combine!`（子件缓冲 + pop/drain/push）、`fused` = `combine_into!`（`MapSink` 直写根 sink）。逐拍墙钟：k1 373.6→296.0 ns（**−20.8%**）、k4 439.1→333.1（**−24.1%**）、k16 837.9→526.2（**−37.2%**）、k64 2111→1534（**−27.3%**）、k256 6008→4555（**−24.2%**）、k1024 22120→20488（**−7.4%**）。⇒ **结构性收益成立**（消除中间缓冲/分配/搬运），与批量入口的「无收益」形成对照；k1024 相对收益变小是因为该规模下转发环本身主导。

**已知限制（票② 实现轮发现，2026-11-27）**：交接等待用 condvar 等入口账本终局、**无超时**。若排空者在 reduce 内 panic 且此后无人继续排空，等待者会一直等——与既有的「排空者 panic ⇒ 队列搁浅」同类（等待型根的 `wait()` 一直如此），本票只是让 `try_dispatch` / `ActionSender::dispatch` 的调用方也进入该路径。

---

## 7. 授权边界（依 #231 裁决）

**2026-11-27 更新**：维护者在 #231 追加「设计完了就开工了」⇒ **票① 已获实现授权**（可改 `skeleton` / `skeleton-infra` 运行时代码与测试、同步文档）。票② 尚未单独确认——本次按依赖顺序只推进票①。

**票① 以外的边界仍适用**：不改 CI / `.github/**`；票② 与 per-drain 交接在维护者另行确认前不动。

---

## 8. 实现票（设计完成，待授权）

| 票 | 范围 | 风险 |
|---|---|---|
| ① | `EffectBuf` 有界暂存、overflow 标记、全有或全无 transition、typed outcome / 计数、legacy `dispatch` 显形与兼容性 / 边界验收 | 中：契约层公开面新增读侧；每次 `push` 多一次比较；默认启用 ⇒ 行为变更 |
| ②（**已实现 2026-11-27**） | per-drain lease handoff：并发交接、FIFO、无丢失唤醒、无队列搁浅、exactly-once accounting、因果账本完成 | 高：并发表约改造；**不得与 ① 捆绑实现**，也不得为赶进度弱化同步完成语义 |

② 依赖 ① 的 typed outcome / 计数口径先定稿（避免两票各造一套）；② 的语义前提已由 #231 确认：`ActionSender::dispatch` 的 `bool` 仍只表示「被接受」，需要完成结果的调用方继续用 `dispatch_with_receipt(...).wait()`，不因此重启 Option E。
