# Changelog

本文件记录 udf-frame 的对外可见变更（API/feature/文档/修复）。格式参照 Keep a Changelog；版本号语义遵循 SemVer。

## [Unreleased]

### Fixed（契约与文档面澄清：dispatch 完成边界 + 新面收录 + 两条回归闸补齐，2026-11-19；12 维独立复审）
- **完成边界口径更正**：`skeleton::Store` trait 与 `docs/reference.md` 此前三处写「dispatch 返回 = 队列 idle / 同步排空至 idle」，与 #40 拍板的 **per-action 完成边界**冲突——并发生产者下队列**可能仍非空，这是正常态、不是未排空证据** ⇒ 契约 crate 与 reference.md 同步更正为「dispatch 返回 = 本 action 的完成边界（状态提交 → 效果转发 → 通知返回，且其全部**因果后代**终局）」。
- **新面补收录**：`docs/reference.md` 此前**零收录** #38/#40 的四个公共面（`try_dispatch` / `set_capacity` / `AdmissionOutcome` / `admission_full_count`）⇒ 补入 runtime 章节，并写明容量口径：**等待被 reduce 的 action 数**（执行中不计入、同栈回灌豁免）、满时 typed 拒绝**不阻塞**（阻塞会与回调重入死锁）。
- **panic 语义入契约**：`skeleton::EffectRuntime::run` 的 panic 语义此前只在实现侧、契约 crate 不可见 ⇒ 写进契约 rustdoc：该效果已被取出且**不重放**；实现方若依赖重试须自行保证幂等。**行为未改**（#43/#44 评审要求保留该语义）。
- **退役假设改写**：`docs/reference.md` 的「weight 钩子必须是留存对象的纯函数、析构按钩子重算重量」假设已随 #35 退役 ⇒ 改写（照旧假设实现会重犯 #35 的 bug）。
- **两条缺失的回归闸补齐**（对外行为未变）：`external_dispatch_waits_for_causal_descendant_failure`（#40 因果后代屏障——跨线程 + 确定性握手，禁 sleep）与 `pool_drop_with_nonempty_idle_never_calls_the_weight_hook`（#35 析构路径——旧断言因先 prune 清空 idle 而**遍历空集、空转恒真**）。变异验证：**旧闸在变异下全绿（exit 0）**，新闸判红且归因正确 ⇒ 新闸具备真实鉴别力。

### Changed（`EffectBuf` 内部改前向可读环形缓冲--摘队首零搬移，2026-11-19；GitHub issue#43）
- `EffectBuf` 内部表示 `Vec<Effect>` → `VecDeque<Effect>`：追加在尾（`push_back`）、摘取在头（`pop_front`）⇒ 摘队首**只推进头指针、其余效果原地不动**（旧 `Vec::remove(0)` 每次把尾部整体前移一格 ⇒ 排空 k 条效果共 k(k-1)/2 次元素搬移，O(k²)）。
- **公共 API 逐字不变**（`push`/`clear`/`is_empty`/`pop_front`）：`effects` 字段私有且仅在 `skeleton` crate 内部被访问（无 `size_of` 布局断言、无跨 crate 直访）⇒ 调用方零改动；容量行为不变（`clear` 仍保容量）、panic 语义不变（未转发效果仍留缓冲）、契约 crate 零依赖纪律未破（`VecDeque` 是 std）。
- 实测（每拍 k 条效果，同机同口径 1.96.0，同码 dup 臂作噪声锚）：k=1 257→252 ns（无回归）· k=16 789→743 ns · k=64 2.66→2.39 µs · k=256 13.2→8.59 µs（−35%）· k=1024 116.9→33.7 µs（−71%）· k=4096 2.044 ms→134.8 µs（**−93.4%，15.2×**）。**规模律换了**：旧实现 k 涨 4096 倍时耗时涨 7960 倍（超线性），新实现只涨 536 倍。
- 元素搬移解析式计数（旧 k(k-1)/2 → 新 0）：k=16 120→0 · k=64 2016→0 · k=256 32640→0 · k=1024 523776→0 · k=4096 8386560→0。
- 分配面无回归：`Vec` 与 `VecDeque` 同为倍增扩容（分配次数 O(log k) 同量级）；探针实测多轮 push-k/pop-k 周期容量稳定、`clear` 保容量两者一致。
- 测试：红阶段 `effect_buf_pop_front_does_not_relocate_remaining_effects` FAILED（地址差 0x10 = 一个 `Effect` 的大小，证明旧实现每次前移一格）→ 改后转绿；判据取**地址不变**而非计时（计时被布局噪声主导，不可作证据）。新增 `tests/batch_effects_order.rs`（k=1/4/64/1024 的条数与顺序——此前「k 条效果」这一面无覆盖）与 `tests/effect_forward_panic.rs`（中断在第 1/中间/最后一条：成功前缀不重放、未转发后缀按序续转不丢不重）。
- **panic 语义已定（GitHub issue#45 拍板：Option D——非 A/B/C 原文）**：`EffectRuntime::run` panic 时**该次调用退出自动重放**（框架**不再调用**它 ⇒ **至多一次调用尝试**），但**不得**据此认为该效果「未执行」——`run` 可能在 panic 前**已产生外部副作用**（框架按值取走效果，**无法区分**「panic 前未执行 / 执行中 panic / 执行完后在清理或汇报时 panic」）⇒ 该效果的**外部结果不确定（indeterminate）**。**不承诺** exactly-once 投递或 exactly-once 副作用。**中断可见**：同一路径发出 `EffectInterrupted` 审计事件，转发失败回执仍报 `Failed(EffectsForwarding)` + `state_committed=true`。**既有保证不变**：成功前缀不重放、已提交未调用的后缀按 FIFO 续转恰好一次。**幂等性属消费方责任**；将来若需可重试投递，作为**显式 opt-in 能力**引入（稳定 delivery identity + 适配器侧持久幂等/去重 + 三态结果），**不**作为默认契约。

### Added（SyncStore dispatch 回执 + drain 恢复交接 + 有界准入，2026-11-18；GitHub issue#40 / #41 / #38）
- **#40 并发 dispatch 回执**：新增公共面 `try_dispatch` / `set_capacity` / `admission_full_count` / `AdmissionOutcome`。**完成边界 = per-action**（状态提交 → 效果转发 → 通知返回）+ 全部因果后代终局——外部并发 dispatch 在**自己的** action 到达该边界前不返回；同线程/同 store 重入保持「仅入队不返回」以免自死锁。
- **#41 drain owner 异常恢复交接**：`DrainGuard` 的 `Drop` **只记账**（当前记录按阶段失败 + 残留整批失败 + 摘队放旗同临界区），**零用户代码**——guard 的 drop 绝不跑用户代码，杜绝「unwind 中二次 panic ⇒ abort」。批次中断为 fail-visible：不重放、原 panic 照常上浮、等待者**恰好结算一次**。排空权判定由单槽改为 **TLS 帧栈**（修 A→B→A 同栈误判）。
- **#38 有界准入**：容量 = **等待被 reduce 的 action 数**（执行中不计入），只约束外部准入，同栈回灌豁免；满时 **typed reject 不阻塞**（阻塞会与回调重入死锁），legacy `dispatch` 显式 panic。超反馈预算的动作**停放 deferred 队列**（零丢失留痕，外部流量不被饿死）。
- **口径变更（相对 issue#44）**：反馈预算止步由「后续 dispatch 重放」改为「停放」——残留不再自动再执行，但**可查、可计数**。
- **行为变更**：legacy `dispatch` 对**并发外部调用者会阻塞**（issue#40 拍板）。已知残余风险：若宿主在 drain 回调里 spawn 工作线程并 join、且该线程回调 store，会形成跨线程互等（本批未加防护）。
- 机制：新增 `runtime/receipt.rs`（完成账本 `Ledger` + 阶段/失败分类），账本走复用池且**归还者唯一**（首发者胜）。
- 测试：新增 `tests/dispatch_receipts.rs`（7 用例）与 `tests/bounded_admission.rs`（4 用例）；`memgov` 三个零分配硬锚照跑通过。
- 变异三则：跳过 `ledger.wait()` ⇒ 红；中断批次误报成功 ⇒ 红；容量判定 off-by-one ⇒ 红（`left: Accepted, right: Full`）。
- 未做（明示）：`close_and_wait()`、显式反馈纪元恢复（`flush_deferred` / `resume_deferred`）、`ActionSender::try_dispatch_with_receipt`；因果屏障只覆盖有等待者的根（无等待者的根不分配账本）。
- PM 亲验：仓库级四门（test `--all-features` 串行 / clippy `-D warnings` / fmt `--check` / rustdoc `-D warnings`）全 exit 0。

### Changed（fx 三资源维度分离 + 准入上界 typed 化，2026-11-18；GitHub issue#39）
- **破坏性**：`skeleton_infra::fx::expect_idle`（自由函数）**已删除**，拆为 `FxScope::expect_body_idle`（任务体结束口径）与 `FxScope::expect_fully_reaped_idle`（跟踪集合已完全收割，更强）；crate 根重导出同步移除，调用点全量迁移（`fx.rs` 13 处 / `testing.rs` 6 处 / `examples/end_to_end.rs` 2 处），**无 shim、无旧别名**。`docs/reference.md` 与 `docs/tutorials/thirty-min.md` 的 6 处引用同步更正。
- 新增 `AdmissionError{Closed, PendingFull, TrackedFull}`（typed 过载/闸关）与 `FxCounts{active, waiting, completed_unreaped, rejected}`；新增 `FxScope::{with_max_pending, with_max_tracked, try_spawn_tracked, counts, expect_body_idle, expect_fully_reaped_idle}`。
- **三个互相独立的资源维度**：`max_concurrency`（任务体并发，信号量）/ `max_pending`（已准入待许可）/ `max_tracked`（保留的全部集合条目，**含已完成未收割**）。`with_concurrency` **不是**内存/保留量上界（rustdoc 明写）；保留上界 = `with_max_tracked`。两者**默认无界**（保持兼容，opt-in 显式设）。
- 有界准入走 `try_spawn_tracked -> Result<Spawned<T>, AdmissionError>`：过载 **typed 拒绝**（不阻塞、不 panic）；`spawn_tracked` 是**无界旧口**（保持兼容，不受上界约束）。
- `Spawned::join()` 只消费结果、**不**移除集合条目（收割口 = `try_reap`）——写进 rustdoc。
- **行为变更**：`spawn_tracked` 现在在**准入时**取并发许可（原在任务体内取）⇒ 许可随准入占用，abort 仍连带回收；`active` 计数含「已准入未开跑」者。
- **容量在六条终态路径恰好释放一次**：正常完成 / panic（`catch_unwind` 归一）/ 取消 / abort / shutdown(`abort_all`) / receiver-drop。证据三重：RAII 单一 `Drop` 落点（不多）+ `active/waiting` 归零断言（不少）+ 并发度探针 `peak ≤ 1`（多放会让实测并发度变 2）+ debug 计数下溢直接 panic。
- 测试：新增 `tests/fx_resource_axes.rs`（6 用例：pending 满 typed 拒绝 / 从不 reap 时计数可观测且有界 / 六终态路径恰好释放一次 / 三 limit 独立 / body-idle 与 fully-reaped-idle 可区分）；`tests/fx_lifecycle.rs` 补 F3 交叉面（`shutdown_timeout` × 非空已完成集，+103 行）。
- 变异两则：两轴上界检查改成 `>= usize::MAX` ⇒ 3 个测试 FAILED；`active -= 1` 写两次 ⇒ 4 个 FAILED + 进程 abort `0xc0000409`（证明释放路径有鉴别力）。
- PM 亲验：`fx_resource_axes` 6/6、`fx_lifecycle` 5/5；仓库级四门（test `--all-features` 串行 / clippy `-D warnings` / fmt `--check` / rustdoc `-D warnings`）全 exit 0。

### Fixed（reducer 事务边界：失败迁移的暂存效果不再执行，2026-11-18；GitHub issue#44）
- **根因**：`drain_owned` 里 `reducer.reduce` 写入共享 `effect_buf`（在 `catch_unwind` 内）；reducer panic 时不清理该次调用的 effects，panic 在 `place_snapshot` 之前 resume（`next_state` 未提交），下次成功 dispatch 转发时旧后缀先发 ⇒ **一次失败的状态转换，其副作用在别的 action 里执行了**。
- **修复**：新增私有结构 `EffectBufs<A, Op>{staged, pending}`——`staged` 是本次 reducer 调用的暂存区（每次调用级清零），`pending` 是已提交待转发队列；reducer 成功 ⇒ `commit()` 整段并入 `pending`（保持 action 序）；reducer panic ⇒ 只清 `staged`，不动 `pending`。
- **严格保留 `EffectRuntime` panic 路径语义**：state 已提交后 runtime panic 仍留未转发后缀（不重放已成功前缀）——由变异验证守住。
- **计数口径**：被丢弃的 `staged` effects 不计数（未提交的效果从未成为已提交效果；另立计数会把失败迁移的副作用混进已提交账，且新增公开计数 = 公共 API 扩张）；口径写死进 `reducer_panic_count` 文档。`reducer_panics` 计数与 `ReducerInterrupted` 事件语义未变（无 `Reduced`、action 丢失、+1、panic 照常上浮）。
- **契约 crate `skeleton` 零改动**：`EffectBuf` 只有 push/pop_front/clear/is_empty（无 len/truncate），无法在单缓冲上做「只丢尾部」⇒ 必须在实现侧切两段。
- **审计事件文档同步**：`ReducerInterrupted` 原写「panic 前已产效果原地留缓冲，由下一任排空先续转」——该语义已被本次推翻，改为「本次调用已产效果随未提交迁移一并丢弃，更早已提交待转发的效果原样留存」。
- 测试：`tests/robust_panic_recovery.rs` +204（固定装置 + 4 条验收）；红阶段 `reducer_panic_discards_staged_effects` FAILED（left `[Staged,Own]` vs right `[Own]`）、`reducer_panic_preserves_committed_pending_effects` FAILED；绿后全套 461 passed / 0 failed。变异两则：reducer panic 时整队清空 `pending` ⇒ 红；reducer panic 时仍提交暂存 ⇒ 红。

### Fixed（local 订阅通知改两阶段：不持表借用跑用户回调，2026-11-18；GitHub issue#42）
- **根因**：`local_notify` 持 TLS `RefCell` 的 `borrow_mut` 遍历并调用户回调 ⇒ 回调内任何再入（跨 store 嵌套通知、callback-time subscribe/unregister）撞 `already borrowed`，被外层 `catch` 误判为「用户回调 panic」⇒ **健康订阅被静默摘除** + 嵌套 dispatch 通知阶段 abort。
- **修法（两阶段）**：回调句柄改 `Rc<RefCell<Box<dyn LocalNotify>>>`；阶段一在表借用下只**快照句柄**（登记序即通知序）并立即释放借用；阶段二不持表借用逐个调回调；延迟增删在末段统一应用。
- **契约点（本次拍定）**：回调期间新注册的本轮不可见、下一轮可见；本轮开始时在册、被调到之前已注销的跳过（Drop = 退订即时生效，不复活死订阅）；真用户 panic 照旧 fail-closed 摘除。
- `catch_unwind` 只包用户回调调用，registry 访问不在其内 ⇒ 框架 `RefCell` 冲突不再被计为用户 panic。
- 测试 5 条全绿（跨 store 嵌套 dispatch 不被误判 / callback-time subscribe 下一轮可见 / callback-time unsubscribe 跳过本轮 / drop 自身票据 / 真实用户 panic 仍 fail-closed）。变异验证：快照后加回持表借用（模拟旧行为）⇒ 首个测试即 panic `RefCell already mutably borrowed`，证明测试有鉴别力。

### Fixed（`ObjectPool` 的 weight 计量改为入池一次性存槽，出池路径零钩子调用，2026-11-18；GitHub issue#35）
- **根因**：pop/prune/resize/池 Drop 均「先移出槽再调 `weight_of`」——钩子 panic 发生在移出后、记账前 ⇒ 对象丢 + 字节不回卷 + alive/borrowed/idle 悬挂；既有测试曾把 100B 永久漏记断言为「预期行为」。
- **修复**：`IdleSlot` 增 `weight: usize`（入池一次性计量，片锁外调用一次）；prune/resize/pop_idle/池 Drop 的释放回卷全部改用**存储权重**，出池路径不再调用户代码。
- **承重假设退役**：「weight 钩子必须是留存对象的纯函数」不再成立——新契约 = 每次入池对当前状态计量一次，借出期改参数、归还重计量即新值，逐轮配平；片锁毒源随之消失（`PoisonError::into_inner` 保留为防御性 fail-fast）。
- 修正 `return_to_shard` 语义：钩子 panic 时字节未认领（panic 在 `try_take` 之前）但 borrowed/alive 各悬挂 +1 ⇒ catch 后先 `release(home, true)` 再 `resume_unwind` 原样传播（与既有 reset-panic 路径同款；unwind 归还已在上游分流，不升级为 Drop 期 abort）。`drop_handle`：锁外 `catch_unwind` 包计量，panic ⇒ 回卷 borrowed/alive + 销毁 + 原样传播。
- 测试：`byte_budget` 18/18（旧 `drop_survives_a_poisoned_shard` 重写为 `release_paths_use_stored_weights_and_never_call_the_hook`，`held == 0` 终点不变）；新增 prune/pop/resize 释放路径 armed-tripwire 测试 + return 路径计数悬挂测试（单片池 `max=1` 钉死：悬挂 ⇒ insert 被拒）；`object_pool` 51/51 回归。变异验证三形态：释放 +1@pop（红）、跳过 release@prune（红 `held 200 ≠ 0`）、释放多调钩子@resize（armed tripwire 红）。

### Fixed（`ThreadPool` spawn 回滚不再留下 stale shrinking token，2026-11-18；GitHub issue#34）
- **根因**：`resize` 把 in-flight 席位算进 workers 计算缩容差额；spawn 失败回滚只减 workers 不回收 shrinking；worker 退出只看 `shrinking > 0` 不看是否已低于 core ⇒ 确定性交错可把池打到 `workers = 0 < core = 1` 且队列任务无人接。
- **修复（分册计数）**：`Inner` 新增 `reservations` 另册；`reserve_worker` +1、`create_worker` Ok 兑现 -1、两条 rollback 路径同步 -1；`resize` 的缩容差额/补建缺口按**真实存活** `workers - reservations` 计算。
- **退出守卫**：worker 退役需 `shrinking > 0` **且** 真实存活 > core（shutdown / resize-to-0 语义不变）。
- **读数口径统一**：`metrics().workers` 与 `SpawnFailed.workers` 读数改为真实存活（稳态等价，仅创建窗口内读数变小；公开 API 零变化）。
- 测试：`BlockingFailGate` + 两条确定性交错复现（submitter 线程 / resizer 线程各一）；修复前红（`workers = 0 < core = 1`，`queue_len = 1`），修复后 32/32 绿。变异验证：完整回退两层修复 ⇒ 红；只回退口径层 ⇒ 绿（证明双层各司其职）。

### Added（CI feature 隔离矩阵 + 集成测试/示例 `required-features` 门控，2026-11-18；GitHub issue#37）
- `ci.yml` 新增 `feature-isolation` job：full 的 14 个子开关各跑 check + test（单 OS——feature 门是 cfg 维度、与平台正交，14×3 违背「small, intentional」）；runtime 面另加 **Tokio-free fail-closed 断言**（`cargo tree -e normal`，tree 失败与检出 tokio 均 exit 1），与蓝图 §13「sync-only consumer 无 Tokio」对齐。
- **矩阵挖出两个真缺陷**（`cargo check` 探不到——它不编 example/test）：① 31 个集成测试全部无 `[[test]]` 段 ⇒ 隐式吃 default features（import `alloc_site`/`memgov`/`MAIN_*` 等 default-only 符号）⇒ 单开面 `cargo test` 14/14 全红（E0432/E0425）；现逐个补 `[[test]]` + `required-features`。② `compose = []` 声明零依赖，但 `src/compose.rs` 直接 import `skeleton_infra::runtime` ⇒ `--no-default-features --features compose` 编译失败；现 `compose = ["runtime"]`（runtime 零三方依赖，拉入成本为零）。
- 示例门控：`snapshot_placement` 补 `[[example]]` + `required-features = ["memgov", "runtime"]`（此前自动发现，单开面编译失败）。
- **覆盖零损失**：`required-features` 取 default 集（runtime+pool，pool 传递含 memgov）的子集 ⇒ default 与 `--all-features` 下 31 个集成测试二进制全部照跑（default run 实测 31/31），单开面则跳过不适用测试而非编译失败。

### Changed（目标架构蓝图状态与实现同步，2026-11-18；GitHub issue#36）
- §4：`ShutdownReport` 的过时缺口陈述划掉，改为实现实况（六桶 + `timed_out`/`still_running` 采样时点 + 钉住位置）。
- §5：字节轴承重假设改写为 **stored-weight 模型**（#35 已落地）——weight 入池一次计量存槽、释放路径零钩子调用；「hook 必须是纯函数」对**释放**语义退役（逐轮配平），保留「对入池那一刻的对象须给确定值」；中毒容忍改为**防御性保留**（钩子已不在片锁内跑，毒源消失，但拒绝归还严格更差，故保留）。
- §11 P0-1 标为 done-then-extended（#26 → #34：`reservations` 另册 + 不低于 core 的退出守卫）；§13 ThreadPool 行补 #34 的另册计数、退出守卫、两条确定性交错测试（修复前 `workers=0<core=1` 且队列有活）与变异验证。
- 表格结构修复：承重假设段落曾插在表格中间打断 Markdown 渲染，已移至表后。

### Fixed（`ObjectPool` 析构不回卷 idle 字节额度，2026-11-17；bd udf-frame-m34）
- 新增 `impl Drop for ObjectPool`：逐片摘下 idle 槽（片锁立即释放）→ 锁外按权重钩子汇总字节 → 循环外一次 `ByteBudget::give_back`。共享 `Arc<ByteBudget>` 场景下，此前丢弃一个池会让它占住的额度**永久不可用**直到进程结束；现在随池析构归还（`available()` 立即恢复）。
- 未启用字节预算的池在 `Drop` 首行早退（零开销），字段随后照常析构；锁序沿用「池片锁 → 预算叶子锁」单向、锁不嵌套；析构路径**不复用**内部 `lock()` 助手（其 debug 重入防呆会在「钩子体内 drop 另一个池」这种无死锁路径误报 panic）。
- 新增 4 测试（`tests/byte_budget.rs`）：drop 回卷、回收额度仍受全局硬上限、私有/无预算池 drop 回归、并发「drop × try_take × 采样」不击穿 `held <= total` 且终态归零。红-绿证据：实现前 3 个行为测试失败（400≠200 / 0≠300 / 800≠0）。

### Changed（`ByteBudget` 账本换型：`Mutex<usize>` → 原子 CAS，2026-11-17；bd udf-frame-d5i）
- `held` 改 `AtomicUsize` + `fetch_update`：`try_take` 仍一次性 check+act（**无**先加后回滚窗口）、`give_back` 下溢在写盘前 fail-fast（计数不被 wrap）、`held()` 改无锁 `Relaxed` 快照。语义逐条不变，零公开 API / 零依赖变化。
- 实测（同机同口径）：单线程 103–105 ns → **89–91 ns**（改善 13%）；4 线程 1.95–2.16 µs → **0.96–1.02 µs**（**2.1×**）。
- **契约目标「4 线程 ≤2× 单线程」未达成**——但「倍数」在此是**伪指标**：分母（无预算臂）跨 run 在 84–418 ns 之间跳动（同 run 内 dup 锚一致，故非噪声），比值随之为 3.2×–10.8×。绝对值才可比。
- 真值以 A/B 差分为准（同池同线程，唯一差异账本开/关）：**1003 ns vs 102 ns ⇒ 约 900 ns 由「存在一份共享账本」本身贡献**；而三重隔离测量证明换**形态**无益（争用下 take+give 一对 32–107 ns、真分片仍 696 ns、回滚后单线程退化 15 ns）⇒ 该成本是共享账本的结构代价，非实现选择问题。要消除只能分片账本 = 放弃「全局精确下溢 fail-fast」（ADR 级，未做）。
- 防复发：`benches/infra.rs` 的 4 线程臂新增同码 dup 噪声锚——同 run 一致才可信。

### Fixed（测试：`fx_lifecycle` 的 deadline 断言确定化，2026-11-17；bd udf-frame-k4p）
- 根因：tokio 的「任务 poll 返回 `Ready` → 标记 COMPLETE」与「另一线程 `abort()` → 置 CANCELLED」之间存在内部竞窗；deadline 路径上「正常完成/panic 落哪个桶」因此**不可断言**（CPU 竞争时漂移），旧测试在并行负载下 flaky（孤跑绿、并发红）。
- 拆分：deadline 路径只断言与 abort 无竞态的终局类（`completed == 0` 精确、`timed_out >= 1`、`still_running >= 1`、`still_running <= timed_out`、桶和 == 3）；逐类精确落账迁到无 deadline 路径（新测试，`completed == 2` / `cancelled == 2` / `panicked` 精确 / `timed_out == still_running == 0`）。断言强度是搬家不是弱化。
- 虚拟时钟方案实测否决：`start_paused` + `multi_thread` 编译期被拒（该选项要求 `current_thread`）；改 `current_thread` 后自旋占住唯一 worker ⇒ 虚拟钟不前进 ⇒ 30s 挂死。
- 稳定性：12 轮 × 8 并发实例 × 12 个真实忙进程 = 96 次全绿，且报告取值逐字节一致。

### Added（ThreadPool 创建失败 typed 化 + 幻影 worker 回滚，2026-11-17；GitHub issue#26 / bd udf-frame-ck0）
- **修复**：worker 记账改为「席位预留 → 创建成功才计入存活」，创建失败即回滚席位——幻影 worker 消除，`join()` 不再永久等待一个不存在的 worker；`resize` 补建部分失败后池保持有效、计数准确；构造期创建失败路径不再 `join()`（停机信号下发即返回，禁永久挂起）。
- `skeleton_infra::pool::ThreadSpawnError`：worker 创建失败的 typed operational error（§7 Runtime/infrastructure 类），保留底层 `io::Error` 现场（`io_error()`）；`RejectedTask` 增成因面 `RejectCause::{Policy, SpawnFailed}` + `cause()`/`spawn_error()`（`into_task()` 语义不变，创建失败时任务原样交还调用方）；`ThreadPoolMetrics` 增 `spawn_failures`（与策略拒绝 `rejected` 分账）。
- `ThreadPool` 增 typed 构造面 `try_new` / `try_with_policy` / `try_with_gate`；`new` / `with_policy` 签名与行为不变（创建失败仍 fail-fast panic，doc 已标注）。
- `ThreadPool` 增可插创建闸 `SpawnGate`（默认 `AllowAllSpawns`：生产单态化零开销——无 vtable/无堆分配，`size_of::<ThreadPool>()` 不变 112 字节）+ `ThreadPool<G>` 泛型参数（既有 `ThreadPool::new(..)` 调用点零改动）。

### Added（fx 停机报告六桶：终局分类 + 截止/未观测分离，2026-11-17；GitHub issue#27 / bd udf-frame-7ug）
- `fx::ShutdownReport` 扩为六字段：`completed` / `cancelled` / `panicked` / `failed`（+ 逐条 `errors`）/ `timed_out` / `still_running`。**`timed_out` 取样于截止瞬间**（调用方停止等待），**`still_running` 取样于收割后**（终止未被观测）——报告不再把仍在运行的任务记成已完成或已终止。
- `ShutdownReport` 标 `#[non_exhaustive]`：字段按真实采用面渐进扩，**跨 crate 构造须 `ShutdownReport::default()` 再赋值**（`..Default::default()` 形态同样被 E0639 拒绝），此后加字段属非破坏演进。
- `JoinError` 归类加显式 `is_cancelled` / `is_panic` 守卫 + 非静默兜底（未知 join 变体落 `failed` + `errors`，不再对非 panic 错误调 `into_panic`——该调用自身会 panic，而这是停机收尾路径）。
- 新增 `tests/fx_lifecycle.rs`（4 测试）：桶穷尽、`close` ≠ `cancel`、`close`/`cancel`/`shutdown` 幂等、混合终局分类。

### Added（`SizeClassPool` 尺寸分档池组合件，2026-11-17；GitHub issue#15 增量3 / bd udf-frame-dks）
- `skeleton_infra::SizeClassPool<T>`（feature `pool`，crate 根 re-export）：严格递增档位表 + 每档一个 `ObjectPool<T>`，`acquire(need)` **向上取整**选档（只有「多给」不会「少给」）；句柄记借出时的档、Drop 回那一档（**不按对象当前容量反推**——涨大后反推会把大件塞回小档池造成池毒化）；超最大档走**档外降级直构**（`over_max_degraded` 独立计数，不静默回退最大档，不入池宇宙、归还即释放）；`SizeClassMetrics` 给出按档 idle/borrowed/degraded 明细，池内降级与档外降级分账。
- 新增 `tests/size_class_pool.rs`（8 测试）：选档正确性（含 `0..140_000` 全扫断言「借出容量恒 ≥ 请求」与「前一档 < 请求」）、归还归位（涨大对象仍回原档）、档内容量紧张可观测、档外降级可辨识。

### Added（`DrainExclusive` unsafe 证据面 + `unsafe-evidence` feature，2026-11-17；GitHub issue#29 / bd udf-frame-83c）
- `DrainExclusive` 安全纪要改**三段式**：建立者（排空旗 `draining.swap` 互斥）/ 维持者（所有独占访问必经 `ExclusiveCell::with_mut` 且限 `DrainGuard` armed，**调用点全集即检查清单**）/ 违反后果（别名写 UB + 指向可执行检测面）。`get_mut` 收为 `with_mut` 内部实现，`&mut` 不再逃逸闭包；5 处独占区间（含 `effect_buf` 与 `notify_cache` 的拆段）改经 `with_mut`。
- 新 feature **`unsafe-evidence`**（默认关，**不在 `full` 并集内**——`full` 是消费能力合集，证据面不该让采用方白编对照实现；需要时显式 `--features unsafe-evidence`）：编入 `Mutex<T>` 版对照实现与证据专用入口 `SyncStore::new_mutex_reference`（与生产共用唯一 `assemble()`，**不重写 SyncStore**）。默认构建不编译对照实现——`strings` 符号级证明 `MutexExclusive` 在默认 rlib 中零命中；`#[cfg(test)]` 布局断言钉住 `size_of::<ExclusiveCell<u64>>() == size_of::<UnsafeCell<u64>>()`。
- 新增 `tests/unsafe_evidence.rs`（6 测试）：并发 dispatch × 快照 clone/drop × 订阅 register/unregister 的精确性、reducer/订阅回调 panic 后旗与缓冲一致、重入 dispatch 只入队、生产 vs Mutex 对照 8 项可观察面逐项一致。**变异验证**：旁路排空旗互斥 ⇒ 1 秒内 3 测试变红（打出 `EffectBuf` 被两个排空者并发摘取的实现级崩点）。
- Miri：**纯 std 单线程子集 3 测试**（`reducer_panic_keeps_drain_flag_consistent_and_effects_retained` / `subscriber_panic_is_quarantined_without_poisoning_drain` / `reference_store_keeps_same_public_semantics_under_panics`），命令
  `cargo +nightly miri test -p udf-skeleton-infra --no-default-features --features runtime,unsafe-evidence --test unsafe_evidence -- panic` → exit 0。
  **不声称覆盖并发**——并发证据由不变量测试 + 变异验证承担。**已知边界**：`miri --lib` 广跑会红于 `clock` 的 `GetSystemTimePreciseAsFileTime not available when isolation is enabled`（Miri 隔离下系统时钟不可用），故只跑上述过滤子集。

### Fixed（`LeakGuard` 分配器在岗探测假阴性，2026-11-17；bd udf-frame-1yd）
- `LeakGuard::arm()` 的探测改读**单调累计面** `total_allocs` 而非净额面 `live_allocs`：后者会被并发线程的释放对消回基线 ⇒ 探测假阴性 ⇒ 整个泄漏断言静默降级成 no-op（`check()` 恒 `Ok` = 假绿）。
- 新增 `LeakGuard::is_enabled()`：降级态对采用方可见（原先 `enabled` 私有，外部无从区分「真无泄漏」与「根本没在观测」）；`check()` doc 明示未启用时恒 `Ok`、严格断言先查 `is_enabled()`。行为无破坏（`check()` 返回值语义不变）。
- 新增并发锤回归闸：2 线程各预置 3 万分配、屏障后**只释放不分配**制造单向净额下降、2000 次探测断言零 miss（变异回净额面 ⇒ 1998/2000 次被欺骗，连跑 3 次必红）。

### Changed（破坏性：`PooledThreadPool` 缓冲域换 `ObjectPool`，2026-11-17；GitHub issue#15 增量1 / bd udf-frame-a01）
- 缓冲域 `MemoryPool` → `ObjectPool`：借空**不再丢任务**——`ObjectPool` 永不阻塞永不失败，满容量走降级直构，任务总能拿到缓冲执行完；`borrow_timeout` 参数**移除**、`borrow_rejected` 计数**退役**（旧语义「借空即丢」→ 新语义「借空降级」，0.1.0 未发布直迁无 shim）；降级读数走 `buffer_pool().metrics().degraded`/`constructed`。
- 新增 `with_caller_runs(buffers, tuning)`：复用既有 `CallerRunsPolicy`，池饱和时可由提交线程代跑（不丢、不排队）。
- `mem_pool()` 改名 `buffer_pool()`；泛型参数 `F` 移除。`examples/` 与 `docs/reference.md` 同批迁移，仓内调用点清零。

### Changed（破坏性：`PoolResizeError` 加 `SpawnFailed` 变体并标 `#[non_exhaustive]`，2026-11-17；GitHub issue#26）
- `skeleton::pool::PoolResizeError` 新增 `SpawnFailed { kind: std::io::ErrorKind, target_core: usize, workers: usize }`（**运行期环境故障**，与既有三变体「恒非法参数入口拒绝」分属两类；错误对象自带「部分生效」状态：目标 core vs 实际存活 worker 数）——`resize` 补建部分失败不再以指标代替提交方可见的结果（§7）。
- 该枚举标 `#[non_exhaustive]`（仓内词表同款纪律：`Effect`/`FieldValue`/`parse` 词表/`view::BindError`）：**下游对该枚举的 `match` 须带 `_` 通配臂**（变体构造不受限），此后加变体属非破坏演进。

### Changed（CI：sync-only 依赖图零 Tokio 断言，2026-11-17；bd udf-frame-679）
- `ci.yml` 新增 **Sync-only dependency tree (no Tokio)** 步（`Rustfmt` 后、`Test skeleton` 前，三 OS 矩阵，`shell: bash`）：对 `udf-skeleton-infra` 默认面、`udf-skeleton-infra --no-default-features --features runtime`、`udf-std-ext` 默认面各跑一次 `cargo tree -e normal`，出现 tokio 即 `exit 1` 并打印带入路径的实际节点。**fail-closed**：`cargo tree` 自身失败同样置红（无 `continue-on-error`/`|| true`）。反向对照（`--features tokio` / `--all-features`）实测 exit 1，证明非恒绿空门。

### Added（池维度基准 + 生命周期/资源 conformance，2026-11-17；bd udf-frame-9x4）
- `benches/infra.rs` 新增池维度基准组：`ObjectPool` acquire/drop 往返、`MemoryPool` 闲槽命中 vs 空池新建、`ThreadPool` 饱和拒绝（Abort vs CallerRuns）、共享 `ByteBudget` 2×2 争用矩阵。**只产数据、无阈值、无 committed baseline**（蓝图 §9）。首轮观测：`ObjectPool` 81 ns、`MemoryPool` 109/262 ns、拒绝 23–28 ns；共享 `ByteBudget` 4 线程 ~2.15 µs（相对单线程 ~25× 劣化，成因=单一全局 `Mutex`）——记为未来 ADR 候选，本批不修。
- 新增 `tests/conformance_lifecycle.rs`（8 测试）：六桶生命周期语义 + 资源三轴可观察断言（`max` 宇宙含降级借出 / 字节轴仅 idle、借出回卷 / 降级件两轴皆不计）+ 变异守卫。测试等待全部有界化（5–10 s 上限 + 诊断快照），不变量被破坏时**快速变红**而非挂死。
- `[[bench]] infra` 的 `required-features` 补 `pool`（原先按声明组合编译 bench 会失败，CI 的 `--all-features` 掩盖了该缺口）。

### Docs（资源上限四问 + effect 执行两档，2026-11-17；bd udf-frame-vtv / udf-frame-0zz）
- `docs/reference.md` 新增「**资源上限四问**」：对 `PoolBase` / `ThreadPool` / `PooledThreadPool` / `ObjectPool`（三轴）/ `BoundedRegistry` / `MemoryPool` / `ByteBudget` 逐项写明①cap 覆盖的群体②满容量行为与可观察读数③是否允许合法超配及其收敛条件④`metrics()` 读数口径；并明确 **metrics 不是准入面**（准入只在三处锁内 check-and-act：`memgov.rs:568` / `pool.rs:329` / `objpool.rs:411`）。全部声明由外部探针工程逐条 assert 验证（`ALL DOC CLAIMS HOLD`）。
- `docs/reference.md` 新增「**effect 执行两档（同步档 / 托管档）**」：排空环内联 `EffectRuntime::run` 与 `FxScope::spawn_tracked` 托管的现状坐标、两档适用条件与失败面、三条边界规则（核心永不阻塞等待异步完成 / 异步结果回灌必经 `ActionSender` / 同步档禁 await 与阻塞 IO）、与 ADR-0008 通知面两档的对应表。**不新增统一入口**——两档是同一 trait 的两种实现形态，造统一入口会逼核心替解释器选执行器（违反蓝图 §6）。
- 新增 `tests/effect_two_tier.rs`（5 测试）：同步档内联时序、同步档 panic 上浮 + 实例恢复、托管档 `dispatch` 不等完成、托管任务失败经 `join`/`ShutdownReport` 可见、两档同序列终态一致。

### Added（SyncTtlCache 借用键查面，2026-11-08；主仓 bd m6pkl N4，PR#24）
- `SyncTtlCache::get` 泛化 `K: Borrow<Q>`（连带 `LruCore` 内部查找族 + `key_fingerprint` `?Sized`）：`String` 键可 `&str` 直查，查询热路径免每次 String 堆分配。旧调用 `get(&owned)` 源兼容（std `HashMap::get` / lru crate 同款 Borrow 惯例）；`insert`/`peek`/`contains`/`remove` 公共签名逐字未变。

### Changed（docs 闸清零 + CI 防回潮，2026-11-05）
- 全 workspace `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` **exit 0**：清零存量 rustdoc 告警 24 处（skeleton 2 / std-ext 9 / infra 13——intra-doc 断链修指向、私有项改代码字面、冗余显式链接目标删除、宏链接改字面、宏调用前 doc 注释改普通注释）。
- `ci.yml` 新增 **Docs** 步（`env: RUSTDOCFLAGS=-D warnings` + `cargo doc --workspace --all-features --no-deps`）：消费方 CI 常带 docs 步，本仓先行自证不红，防回潮。

### Changed（objpool 消费方反馈 API 卫生四件，2026-11-05；GitHub issue#19/#20/#21/#22）
- **issue#19**：`ByteBudget` 提入 crate 根 re-export（pool 域七件同列，消费方不再写 `skeleton_infra::objpool::` 全路径）；`ByteBudget` doc 示例同步短路径。
- **issue#20**：`ObjectPoolMetrics.idle_bytes` 字段 doc 显式警告「共享预算池读数 = 全局 held，不构成按池归因」+ 归因指引（按池归因用池私有预算 `with_byte_quota`；刻意不引入池侧第二账本——双记会漂移）。
- **issue#21**：weight 钩子锁序红线从 doc 升级 **debug 防呆**——钩子执行窗（thread-local 标志，RAII 恢复）内任取片锁（`acquire`/`acquire_owned`/`insert`/`metrics`/`prune_idle_with_clock`/`resize`）⇒ `debug_assert` fail-fast，release 零开销仍禁（doc 红线不变）；`ByteBudget` 内部锁分离出 `budget_lock`（钩子体内只读预算合法，不误报）；新增 debug 重入检出测试（钩子延迟句柄 acquire → 翻牌 + 池仍干净可用）。
- **issue#22**：`prune_idle(max_idle_age, now: Instant)` **`#[deprecated]`**（note 指向 clock 版 + 迁移理由）；正典口径 = `prune_idle_with_clock(max_idle_age, &dyn Clock)`（MockClock 确定性测试 / SystemClock 生产）；内部实现抽 `prune_idle_impl` 共用，仓内测试（7 处）与示例（1 处）全量迁移 clock 版，零弃用告警。**下游迁移**：`pool.prune_idle(d, Instant::now())` → `pool.prune_idle_with_clock(d, &SystemClock)`（`use skeleton::clock::SystemClock`）。
- **issue#23（`spawn_idle_pruner` 托管老化任务）不收**：纯组合无新语义（`FxScope::spawn_tracked` + `tokio::time::interval` + `prune_idle_with_clock`）、`period`/`max_idle_age` 是宿主策略、与「池不起后台线程」既定纪律冲突——宿主侧 ~15 行派生，迁移 snippet 已贴 issue#23（已按 not planned 关闭）。

### Added（通用对象池件 `ObjectPool`：池化有状态对象，2026-11-02；GitHub issue#15）
- `skeleton_infra::objpool::ObjectPool<T>`（feature `pool`，crate 根 re-export 五件）：池化**有状态**对象——`acquire()` 返回 `Pooled<'_, T>`（`Deref`/`DerefMut` 拿对象本体改参数即复用、`Drop` RAII 归还、`detach(self) -> T` 永久移交使对象脱离池生命周期）；**reset 钩子**（`ResetPhase::OnReturn` 归还时 / `OnAcquire` 取出时，锁外调用，归还路径 `catch_unwind` 兜底 ⇒ reset panic 时丢弃该对象而非 abort）；**有界降级**（`acquire()` 无超时参数、结构上不存在等待；池宇宙已满时直接构造 + `degraded` 计数，降级对象不入宇宙、归还即释放）；**分片槽位**（构造期自适应 `clamp(idle_cap,1,8)`，每片一锁 + 原子计数，借用走 thread-local 游标、归还回借出时的片）；`prune_idle(max_idle_age, now)` 显式老化口（宿主自调，池不起后台线程）；实现 `skeleton::pool::PoolManaged`（`Tuning=ObjectPoolTuning`/`Metrics=ObjectPoolMetrics`，`Borrow<PoolMetrics>` 核心形）。
- **unwind 感知归还**：`Pooled::drop` 首查 `thread::panicking()`——panic 路径析构 ⇒ 不调 reset、不回池、直接销毁 + `panic_dropped` 计数（r2d2 #31 教训：归还不变量已破的对象会污染下一个借出方）。
- **既有件零破坏**：`PoolBase` 的审计环抽为 crate 内 `pub(crate) PoolAuditSink` 供两池共用（`PoolBase` 公开面与行为零变化，`validate` 提 `pub(crate)` 同源复用）；`skeleton` 契约 crate 零改动；`MemoryPool` 语义不动（本件与之并存，非替代关系）。
- 决策依据 ADR 0010：issue 字面要求的「idle 槽存 `Weak`」与「复用保证」结构性矛盾（权威依据：HikariCP 弱引用只能当缓存层、Go `sync.Pool` 官方明说放弃复用保证、Rust `Weak::upgrade` 可能返回 `None`），改以**行为等价**形态交付——借出期池不持强引用（值移出槽 ⇒ 在借对象对清理路径物理不可见）+ `detach` 脱离池生命周期后自然析构 + `idle_cap`/`prune_idle`/池 drop 三条释放路径。

### Added（fx 停机排空不再丢结果：`try_reap` 增量收割口 + `ShutdownReport`，2026-10-30；GitHub issue#13）
- `FxScope::try_reap(&self) -> Vec<TaskError>`（增量收割口，缺口一）：锁内同步 `try_join_next` 扫已完成条目——正常完成丢弃、panic 载荷归集返回、abort 条目映射 `Cancelled`；空集零分配（`Vec::new`）、幂等；不动在途任务、不改 `in_flight` 计数（FlightGuard 独立计数）。锁内禁 await（`try_join_next` 同步，临界区与 spawn 同口径）。
- `ShutdownReport { panicked: Vec<String>, timed_out: usize }`（`#[must_use]`，Debug/Clone/Default/PartialEq/Eq）：停机排空报告（fx 域自有；与 `shutdown` 域 coordinator 的同名报告不同型，不经 crate 根 re-export）。

### Changed（fx 停机面：包装条目载 panic 归一面 + 停机返回报告，破坏性；GitHub issue#13）
- 包装任务 `JoinSet<()>` → `JoinSet<Result<(), TaskError>>`：任务 panic 时先照旧 oneshot 发送（句柄在→join 收），**无论 send 成败都从包装返回 `Err(Panicked)` 入集合**——句柄被丢也不丢 panic。**双重可见**（doc 钉死）：同一 panic 可能同时经 `Spawned::join` 与 `try_reap`/停机排空报出，诊断面语义非 exactly-once。其余结局集合面统一 `Ok(())`（真实值只走 oneshot 旁路）。
- `shutdown(&self) -> ShutdownReport`（原 `()`）：排空循环收割 panic 载荷入 `panicked`（abort 条目=预期消亡不计入）；`#[must_use]`，纯收尾语句式调用点改 `let _ =` 显式让渡。
- `shutdown_timeout(&self, d) -> ShutdownReport`（原 `Result<(), TaskError>`）**有意破坏性**：超时不再是 `Err(TimedOut)`，改 `timed_out > 0`（截止时未完成、由剩余集 Drop=abort 兜底中止的任务数）；超时即返前先增量收割已完结条目（panic 载荷不因超时丢失）。`TaskError` 枚举与 `join_timeout` 的 `TimedOut` 面不动。
- `spawn_tracked` 不加自动收割（issue#13 明确否决：静默丢错误）；`in_flight`/`close`/`is_closed`/`abort`/`expect_idle` 语义零变化；Cargo.toml 零改动（无新依赖）。
- 调用点全仓机械迁移：fx 内测试（超时断言改报告形，时长断言与 abort 收割语义原样钉死）、`examples/end_to_end.rs`/`examples/fxscope_multidim.rs`（`r == Err(TimedOut)` → `r.timed_out > 0`；语句式收尾 `let _ =`）、`src/testing.rs` doctest、`docs/tutorials/thirty-min.md` 示例同形。（破坏性直迁：0.1.0 未发布，按 ADR 0005 先例无 shim。**下游注记**：vendored 采用方 agentwiki 同步上游时需把语句式 `FxScope::shutdown().await;` 补 `let _ =`——其 bgshell.rs:376、examples/orchestration_recipes.rs:351-352 两处，且其 CI deny warnings；迁移清单已贴 issue#13。）另：reference.md 顺手修正陈旧笔误 `spawn`→`spawn_tracked`（随批 drive-by）。

### Added（消费方反馈三件：remove 泛形化 + 可观测字段值面 + 分配站点目录，2026-10-30；GitHub issue#8/#7/#6）
- **BoundedRegistry::remove 泛形化**（memgov，issue#8）：`remove<Q: ?Sized + Ord>(&mut self, k: &Q) where K: Borrow<Q>`——与 `get`/`get_mut` 同款借用纪律，`String` 键以 `&str` 摘除**零堆分配**（事件计数锚：借用形 total 22==22 精确平 + live 只降不增；差分对照临时键路径 ≥1 事件证锚有鉴别力）。零行为变化（纯参数形放宽）；doc 钉三条：与 get 同形单一借用纪律 / `Borrow` 一致性契约（Q 与 K 的 Ord 同源禁第二套比较）/ `insert` 收所有权 vs `get`/`remove` 收借用=有意不对称。
- **`skeleton::fields` 可观测字段值面**（issue#7，纯值面零依赖零 feature 门）：`FieldValue`（`#[non_exhaustive]` 六变体：Int/Float/Bool/Str/Duration/Count；只 `PartialEq`——Float 的 NaN≠NaN 使其不可作 map 键）+ `FieldBag`（`Arc<Vec<(&'static str, FieldValue)>>`）：插入序稳定迭代、同名 last-wins 原地覆盖（构造即去重）、clone=一次原子自增 O(1)（实测 4 vs 256 字段同量级，≤1.2x）、COW 改写、`new`/`Default` 零堆分配（共享 EMPTY）、merge 覆盖+按 other 序追加。armor 7 条（顺序/覆盖策略显式/浮点 Eq 破坏/COW 语义/名零分配/O(n) 线性扫描/词表 non_exhaustive）；**无 serde/无导出**（值面到此为止，导出格式归消费方自映射）。实测：new/clone/iter 0.00 分配事件；iter 梯度 4/32/256 字段线性。
- **`skeleton_infra::alloc_site` 分配站点目录**（memgov feature，issue#6）：`alloc_sites!` 宏**编译期静态表注册**（每站点 `static` 常量初始化，无 ctor/无惰性；每次宏调用独立成表——测试隔离天然成立）+ `AllocSite::bump/count/set_enabled/reset`（两级开关：全局闸一把切 + 每站点；禁用路径**零写入**不脏缓存行）+ `find(table, name)` 线性查表零分配。armor 8 条：Relaxed 计数序/先全局后站点/**禁用路径零写入**（不脏缓存行）/枚举序=声明序（const 数组）/重名不编译校验（find 取先声明者）/伪共享不预付（升级路径 `#[repr(align(64))]` 登记）/零分配纪律/与 CountingAllocator（进程级）和 log-embed（tracing 埋点）的三口径分工。收窄：不做阈值/上报/序列化/归属映射/自动发现。实测：bump 全开 5.3ns / 关闸 1.6–1.7ns（计数不变=零写入）；find 线性 ~1.5-1.8ns/站点；同站点 4 线程 0.20x（同一 AtomicU64 争用上限，设计口径登记）。
- 验证：`cargo test --workspace --all-features` **869 passed / 0 failed**；clippy `--workspace --all-features --all-targets -D warnings` 干净；fmt 干净；外部 crate 夹具 `verify/consumer-sim` 新增 fields + alloc_site 消费面（编译+运行绿，反例词表通配臂纪律维持）。

### Added（`skeleton::lint` 配置后处理阶段编排：TypeId 键注册表 + 锁外执行，2026-10-29；GitHub issue#4）
- `skeleton::lint`（零依赖、零 feature 门、**零领域类型**）：`LintStage<C>`（`name()` + `run(&mut C)`，`Send + Sync` 硬约束）/ `LintError`（私有字段 + `new` 受控构造，`stage` 归属由编排器补，已带归属的不覆盖）/ `register_stage` / `clear_stages` / `registered_stages`（`#[must_use]`，诊断读数）/ `post_process`；全 `C: 'static`（`TypeId` 键，借用型配置**编译期**拒绝——`compile_fail,E0310` doctest 钉死）。
- 语义：注册序执行（名单同序）；首个 `Err` 短路（已发生的改写保留）；空表 `Ok(())`；阶段拿 `&mut C` **可改写**；不同 `C` 表隔离；阶段在注册表**锁外**执行——体内可 `register_stage`、可递归 `post_process`（快照语义：本次注册下次生效；同类型递归合法但无防护，阶段自设终止条件）；阶段 panic 原样传播（不 `catch_unwind`、不吞）。
- 析构纪律：清场时被移除槽位的**阶段 `Drop` 在锁外执行**（`lock(..).remove(..)` 的链式写法会把返回值析构拖进锁内——两步式 `let removed = …; drop(removed);`），负向对照测试（阶段 `Drop` 内重入注册 → 若锁内析构即 5s 超时判败）钉死。
- 内存/性能：槽位 `Arc<Vec<Arc<dyn LintStage<C>>>>`，改写走注册期 `Arc::make_mut` ⇒ `post_process` 稳态**零堆分配**（事件计数锚 `tests/lint_alloc.rs` 0/1/3 阶段各 1000 拍断言 0 事件 + example dim1 补 5 阶段；均带仪器自检）；release 实测 0 阶段 **17.0ns** / 1 阶段 **28.0ns** / 5 阶段 **38.3ns**，每阶段边际 **2.28–2.67ns**（1→1000 阶段近线性，漂移 17%），~20µs 级阶段 4 线程 **4.12x**（锁不持在执行期）。
- 锁策略实测裁决：读路径 `RwLock` 读锁（vs `Mutex` 同口径：空表 4 线程 0.21x→0.30x，未争用 ns 持平）——**采用 RwLock**；无阶段多线程空转吞吐（4 线程 ~0.30x）登记为结构性上限（全局注册表一次共享读锁 + 一次原子自增，绝对成本 ~百 ns 级，调用点 = 配置装载一次），规避路写在收窄声明。`Mutex` 前值留档 `verify/2026-10-29-lint-lock-showdown/`（含可回滚 `lint.rs.mutex.bak`），非当前树可复跑。
- 锁中毒：锁内只做 map 查找 / `Arc` 克隆 / `Vec` 追加 / 槽位移除（**含析构在内的用户代码一律锁外**）⇒ `PoisonError::into_inner` 恢复有真毒化测试覆盖（`lint::tests::registry_recovers_from_a_poisoned_lock`）。
- 外部踩坑对表（模块 doc armor 11 条）：锁内跑用户回调=真死锁（tauri-apps/plugins-workspace#3394）/ 每拍 clone 整表=每拍一次分配（`global_registry` 形态 + 本仓 `notify` 前例）/ `TypeId` 仅进程内有效禁持久化 / 全局态与 libtest 并行线程隔离（每测试私有 newtype `C`）/ 锁中毒不报废注册表 / 错误阶段归属 / 快照可见性 / inventory-linkme 链接期注册顺序不可控 / 锁类型实测选出。
- 收窄：无单点卸载（整表 `clear_stages`，单点撤销自带 `Arc` 句柄+开关位）、无优先级/依赖排序（只有注册序）、`name()` 不校验唯一。
- 验证：`tests/lint.rs` 19 测试（五条语义 + 重入注册/递归/panic/类型隔离/并发执行/并发注册/清场/清场不取消在飞那拍/析构重入/析构 panic，每测试私有 newtype 可并行）+ `tests/lint_alloc.rs` 4 测试（含仪器自检，计数与门同为本线程 TLS）+ `examples/lint_multidim.rs` 七维采集器（单批读数原文 `verify/2026-10-29-lint-fix-round/example.txt`）；外部 crate 视角 `verify/consumer-sim` 编译+运行绿（反例 `LintError` 裸构造 → E0451 具鉴别力）。

### Added（整秒定宽形 `format_rfc3339_seconds`：账本字节稳定时间戳，2026-10-28；GitHub issue#3）
- `std-ext/clock::rfc3339` 新增 `format_rfc3339_seconds(SystemTime) -> String`：恒 `YYYY-MM-DDTHH:MM:SSZ` 20 字节定宽，亚秒截断（向过去取整，与 parse 对 >9 位小数的截断同向）；字节稳定契约——同秒任意亚秒恒同一字节序列，消费方追加型日志/账本 4 处自持格式化可换芯；排序性质升级为全 era 字典序==时间序（定宽，无 `format_rfc3339` 的「同 frac 位数」前提）。实现与 `format_rfc3339` 共享单源日期时间写入核（审计 F6.1 禁拷贝链）；既有 `format_rfc3339`/parse 语义与断言零改动，表示域/越界 fail-closed panic 同款。

### Added（消费方反馈六件套 + 通知面异步化，2026-10-28；GitHub issue#1 / ADR 0008）
- **BoundedRegistry**（memgov）：`get<Q: Borrow<Q>>` 零分配泛型查找、`get_mut`（只回值面——键面类型级不可达，compile_fail doctest 钉死）、`Clone`（deep-copy 值容器；运行期只读共享用 `Arc<BoundedRegistry>` 外包）。
- **retry**：`retry_async`/`retry_if_async` + `AsyncSleeper` 注入式异步面（零运行时依赖，下游 tokio 自接线）；同步/异步同 policy 同语义（对照测试钉死）；取消安全（sleep 内联 await，future drop 即全停）。
- **cache**：`SyncLruCache`/`SyncTtlCache` 并发薄包装（`&self` + 内部 Mutex 短临界区，语义与单线程版一致；poison=fail-fast panic 带因；分片留待命中率证据）。
- **id**：`RandomId`（UUIDv4 凭据级，OS CSPRNG 直通 fail-closed 禁弱熵回退）——`random = ["dep:getrandom"]` 第三道依赖隔离门，默认树保持零三方；文档双用途口径：v7=排序友好事件 id / v4=凭据。
- **logx**：每通道独立 level（写路径 AtomicU8 原子过滤，`set_level` 运行期即时生效——结构性免疫 tracing callsite interest 缓存坑，tracing#2874/#2400）+ 逐通道丢弃计数读数 `dropped(channel)`。
- **FxScope**：`spawn` 更名 `spawn_tracked` + 新增 `spawn_detached`（显式脱管口，不计 in_flight、不被 shutdown 等待；全仓唯一 tokio::spawn allow 豁免）+ 文档范式（Arc 化标准写法/cancel-safe 清单/LocalSet 边界/两套 drop 语义对照）。
- **通知面异步化**（ADR 0008 案 C 两档并存）：`skeleton::notify`（`NotifySink` 宿主投递口 + `Notify<S>` 登记面，不扩 Store 六方法）+ SyncStore 实现（排空环内 O(1) 版本投递、先于同步回调；state_version 零异步订阅者不自增）+ `notify_published()` 读侧。锚：无订阅 dispatch 77→78.4ns（±10ns 容忍内）；50ms 慢消费者隔离实证 ~172ns。既有通知/审计/回灌测试零改动。
- **fx 域埋点**（`skeleton-infra`）：托管任务 panic 归一为 `Panicked` 后，若句柄已被丢（结果无人消费）留一行 warn 痕——target `skeleton::fx`、msg=panic 文案，消除「panic 结果静默丢失」；`Ok`/`Cancelled` 无人接属设计内不埋；`log-embed` 默认关=原静默语义零增量。埋点面口径：infra 七点（shutdown 6 + fx 1）+ std-ext 七域 35 点 = 九域 42 点。
- 六域 armor 逐坑一测（调研：外部踩坑调研（本地留存，未入库）——外部踩坑面/性能模式/生态先例，T1/T2 出处全覆盖）。

### Changed（retry 默认 jitter + FxScope spawn 双命名，破坏性）
- retry：退避默认 **full jitter**（禁关——无 jitter 同拍风暴，AWS 一手实测；确定性回放用 `with_jitter` 恒等源）；`with_budget` 红线升级「只配次数不配预算=风暴面」；retry 默认假定操作幂等（文档红线）。
- fx：`FxScope::spawn` → `spawn_tracked`（语义显名）；调用点全仓机械迁移（42 处）。
- （破坏性直迁：0.1.0 未发布无外部采用方，按 ADR 0005 先例无 shim。）

### Changed（fail-visible 封口：静默行为显形四件 + 契约词表封口，破坏性）
- `skeleton`：`EffectId` 构造封口——`EffectId(pub u64)` → 私字段 + `pub const fn new(u64)` / `get()`（std `NonZeroU64` 同形），裸元组构造由 compile_fail doctest 冻结；id 唯一性纪律（store 内全局唯一、多效果域不重叠区间）入文档。**破坏性**：`EffectId(id)` → `EffectId::new(id)`。
- `skeleton`：`Effect` / `Restore` 加 `#[non_exhaustive]`（与 `parse` 词表同口径，ADR 0007 协同）——跨 crate match 编译期强制带通配臂面对未知变体；`skeleton-infra` 内 7 处 match 站点按 fail-closed 语义补臂（panic 带因 / `unreachable!` 带因）。
- `skeleton-infra`：静默行为显形四件（对齐 `subscriber_panics` 计数先例）——`Combine::route_misses()`（combine 路由 miss 不再零计数）；`SyncStore::local_missed_notifications()`（跨线程 dispatch 下本地订阅零触达可观测）；`SyncStore::try_subscribe`（闭门后显式 `Err(StoreClosed)`，旧 `subscribe` 照常登记并计入闭门计数）；`SyncStore::audit_preempted()`（审计事件流二次订阅的静默覆盖留痕）。另 `SyncStore::cancel_unknown_id()`：Cancel 指向从未发出的 Run id 计数留痕（不拦截——迟到 Cancel 属合法时序，核心无生命周期面无法区分漂移）。
- 依据：架构审计（本地留存，未入库）F-04/07/08/09/10/13（B/C 桶残余清零）。

### Changed（combine 分片作根编译期拒绝：Fragment/Reducer 契约拆分，破坏性）
- `skeleton` 新增 `Fragment` trait（reduce-only 状态分片契约）+ blanket `impl<T: Reducer> Fragment for T`（每个根都是合法子件）；`Reducer` 语义收窄为**根契约**（initial+reduce，trait 本体与既有 `impl Reducer` 零变化）。
- `combine`/`combine!` 产物 `Combine<…>` 由 `impl Reducer` 改为 `impl Fragment`：分片作根（`SyncStore::new(分片, …)`）从「装配期 panic」升级为 **E0277 编译期拒绝**（`#[diagnostic::on_unimplemented]` 错误信息自带「分片≠根」修复指引）；`combine` 的子件 bound 由 `Reducer` 放宽为 `Fragment`（完整 Feature 经 blanket 仍可作子件）。随拆分删除误用 panic 与三处孤儿 `#[track_caller]`；两处分片误用 panic 测试由 compile_fail doctest 接替。决策依据 ADR 0009。（**破坏性直迁**：0.1.0 未发布无外部采用方，按 ADR 0005 先例无 shim。）
- **下游迁移说明（combine 分片：作根从 panic 变编译错）**：只写 Feature（`impl Reducer` 含 initial+reduce，`SyncStore::new(feature, fx)` 挂载）**零改动**；持有分片并注解返回类型（`-> impl Reducer<…>` 接 `combine`/`combine!` 产物）改为 `-> impl Fragment<…>`、use 行加 `skeleton::Fragment`（委托调用 `.reduce(...)` 写法不变）；曾把分片传给 `SyncStore::new`/`TestStore::new` 的：此前装配期 panic，现在编译期 E0277 即修复指引——根状态由父 Feature 的 `initial` 显式构造（含子切片初值），父 Feature 本体才是根（`skeleton-infra/tests/combine_teststore.rs` 固定装置为标准形）；少见形态：同文件 import `Fragment` 且直接裸调完整 Feature 的 `.reduce()` 会报 E0034 二义——用 `Reducer::reduce(&feature, …)` 显式分发（框架树内无此形态）。

### Added（ViewBridge MVP：语言无关视图数据面，ADR 0006 波 3）
- 契约词汇 `skeleton::view`（零依赖）：`BindingDecl`/`ViewMap<S>`（纯投影 + 缺省哨兵）/`IntentMap<A>` + `PayloadTag` + `PROTOCOL_VERSION`；`ViewMap` 方法集冻结与 bound 永不落领域 `S`（M6 冻结条款入 doc）。
- 机器 `skeleton_infra::view`（feature `view = ["dep:serde_json", "udf-std-ext/json"]`，ADR 0007 §4.2-5 跨 crate 开门；默认树零三方不变）：`ViewBridge` 纯订阅者挂 SyncStore 通知面（核心零改动）——类型化投影 + 哨兵粗闸/类型 eq 细闸 + latest-wins 折叠 + 版本水位（`PatchBatch.from/to_version`、`Snapshot.version`，桥内单调消费代数与区域头同源）+ 毒化-重同步闭环（投影 panic catch + 计数 + resync 位 ⇒ `snapshot_doc` 收口）+ 可组合双编码矩阵（`JsonEncoder` lazy / `RegionEncoder` eager + `Coverage` + `RegionWriter`/`RegionBacking`/`VecBacking` 固定容量区域通道 v1）+ `Derived`/`Sentinel` L1 记忆化胞。
- 读侧参考实现 `view::read`（M2 实现一次）：`RegionReader`（TLV 解段 + 未知 tag 跳过 + `unknown_tags_skipped` 计数 + `needs_resync` 提示）+ `decode_batch`（JSON 通道同款语义）；规范 `docs/view-region-spec.md` v1 + TS 镜像读器 `docs/view-region-reader.ts`。
- 命名工件链（M3）：真源 `docs/view-bridge-constants/manifest.toml` + 生成器 `examples/view_constants_gen.rs`（零三方纯 std，`--check` CI 断言）+ TS/Python/C 生成物入库；demo `include_str!` 清单逐字对表。
- 正典验收环 `examples/view_bridge_demo.rs`（矩阵行为/双编码一致性/换区握手/水位自洽/未知标签跳过，断言内嵌）+ 行为面 `tests/view_bridge_test.rs`（毒化闭环/封板/跨线程/闭门）。
- 采用方纪律与边界收口（外部踩坑扫描 G4-G8）：视图词表 = enum 表达互斥状态、禁布尔旗子（`ViewMap` doc 立纪律 + `bindings()` 语义给正反例）；「input 覆盖实读」红线补最小可执行反例 `tests/view_bridge_test.rs::narrow_input_leaks_stale_and_is_detectable`（漏报三签名：`projection_runs` 停滞 / 水位推进而补丁为空 / 快照真值与最后交付补丁不等）；ADR 0006 后果段补 M6 冻结记账（方法集冻结 + bound 不落 `S`）+ 后续工作段登记桥层内省面（按绑定维度的投影抓手，本次只登记不实现）；`docs/reference.md` view 章节写明边界「桥保证同一进程内自洽与重同步，跨进程/跨重启复现归 store/持久化层」。

### Changed（json 域门化：默认树恢复零三方，破坏性）
- `std-ext` 的 `json` 域门化为 `json` feature（ADR 0007）：`serde_json` 转 optional（`json = ["dep:serde_json"]`），`json` 域默认不进树——`std-ext` 与 `skeleton-infra` 的默认树**双双恢复零三方**（`cargo tree -p udf-std-ext -e normal` 仅 `skeleton`；`cargo tree -p udf-skeleton-infra -e normal` 仅 skeleton + std-ext，一行可复核）；开 `json` 门时 `serde_json` 及其五子节点（itoa/memchr/serde_core/zmij…）按需进树。域数口径由「十四域零门常驻」改为「**十三域零门常驻 + `json` 域门控**」。（**破坏性**：`std_ext::json` 默认不可用，使用者加 `features = ["json"]`；`pub use serde_json` 再导出经 PM 评审否决，不提供；0.1.0 未发布故无外部采用方迁移面，按 ADR 0005 直迁先例无 shim。）四文档同步终态口径：`README.md` / `docs/reference.md` / `std-ext/README.md` / `skeleton-infra/README.md` 的「除 `json` 域外零三方」过渡态限定（本区块下方 Fixed 首条落下的）全部翻转为终态表述。顺带清零三条 clippy warning：`config.rs` 的 `use std::fmt` 门由 `config-json` 修正到真实消费者门 `config-toml`（单开 config-json 时 unused import）；`logx.rs` 两处 doc list 误判（段落续行行首 `+` 被 markdown 当列表标记）调整断行消除。

### Fixed（对外口径诚实化 + 域数对齐 + CI 门整改）
- 「零三方」口径修正：`std-ext` 的 `json` 域引 `serde_json` 普通依赖（零 feature 常开，默认树即含 `serde_json` 子树）——此前 `README.md` / `docs/reference.md` / `std-ext/README.md` / `skeleton-infra/README.md` 仍写「零三方」，与实测依赖树矛盾；现四处统一为限定表述「除 `json` 域外零三方」，`skeleton-infra` 默认树随之同口径（`json` 域依赖形态的决策见 ADR 0007）。
- 域数口径对齐：`std-ext` 为**十四域**（除 `json` 域外各域零门常驻、本体纯 std + `json` 域带 `serde_json`）——`README.md`（crate 表 + 常驻域行 + 「只要散件」段）、`docs/reference.md`（头部 + feature 矩阵 + 域索引 + 速查表）、`skeleton-infra/README.md` 此前写「十三域」且漏 `json`；`docs/reference.md` 补 `## json` 域章节（`JsonMode`/`JsonParseError`/64MiB `MAX_INPUT_BYTES`/`decode`/`decode_str`/`decode_slice`，API 逐字对 `std-ext/src/json.rs`）与域索引 json 行。
- 性能锚口径诚实化：dispatch 锚（无订阅 ~77ns / 订阅态 ~142ns）此前无适用边界，现补「锚仅在小 State 成立」——外部持有快照时状态落位走 `Arc::make_mut` 克隆分支，每次 dispatch 额外付一次 `S::clone`（锁内），实测 1MiB State ≈210µs/dispatch；`README.md` 埋点规范段 + `docs/reference.md` runtime 锚段同口径（审计报告 F-05）。
- CI 门整改：`.github/workflows/ci.yml` 的 std-ext 步去掉全局 `--skip anchors --skip verbatim`——该字面量匹配连带屏蔽 13 个真实测试（含 `hash` 的 XXH64 官方向量锚、`paths` 的 Windows verbatim 路径语义测试、`scheduler` 的 every_anchors_* 等），属 hollow gate；并行污染根因已在 fe5728a 根治（计数窗加 IN_WINDOW 线程归属门），主步改全量跑，`paths_alloc serial` 单列步保留作双保险。
- 孤儿源文件处置：删 `skeleton-infra/src/text.rs`、`skeleton-infra/src/backoff.rs`（ADR 0004 迁移后残留的无 `mod` 声明副本，不参与编译、不产文档，且 backoff 双份实现已漂移——infra 副本无 `log-embed` 埋点）；内容已在 `std-ext`，无 shim 需求。
- **notify 每拍堆分配修复**（性能修复）：`notify_async` 对有订阅者的每次 dispatch 原走 `collect::<Vec<_>>()` 新建收集缓冲（PerfProbe 实测 total = 1.00 alloc/拍）；改为排空权独占的复用缓冲（`StoreInner::notify_sink_buf`，每 store 独立、跨拍 `clear` 保容量）。语义零变化：每订阅者每拍恰一次投递、投递仍在表锁外、`state_version` 门控与冻结（无人订阅不自增/增减订不回退）原样；钉住锚 `tests/memgov.rs::perf_anchor_alloc::notify_single_subscriber_zero_alloc_dual_caliber`（回退成每拍新建即红）。
- 分配锚注释口径修正：`skeleton-infra/tests/memgov.rs` 的 Borrow 查找锚称含分配路径「10×+ 慢」，PerfProbe 实测仅 **1.5–1.9×**（差 ~50ns ≈ 一次 alloc + copy）——按实测值改写并注明口径与 `verify/perf-probe/`（`registry` 组，10k/44c/100 键三口径）。

### Tests（分配锚与 bench 目标补齐，2026-10-28）
- 新增双口径（live 净增 + total 累计）分配锚：`skeleton-infra` 的 dispatch 命中 / route-miss 稳态 / notify 单订阅者（`tests/memgov.rs::perf_anchor_alloc`），`std-ext` 的 `SyncLruCache`/`SyncTtlCache` get+insert、`retry_async` 首试、`RandomId` 单枚与 fill 批量（`tests/alloc_anchors.rs`，事件计数 + 仪器自检）；补 `std-ext` 的 bench 目标 `std_ext`（cache/retry/id）与 `infra` bench 两组（`dispatch_per_beat` 命中 vs Cancel、`notify_dispatch` 0/1 订阅者）。

## [0.1.0] — 2026-10-18

> 0.1.0 首发准备态：三 crate（`skeleton` / `std-ext` / `skeleton-infra`）同版 0.1.0 首发；
> crate 名冲突处置见发布检查单（本地留存，未入库）。以下为 0.1.0 累计对外可见变更。

### Changed（批 6A Journal 状态面关联类型化，破坏性）
- `Journal<A, S>` → `Journal<A>` + `type State`（ADR 0005）：`JsonlJournal` 同步泛型化（`JsonlJournal<A, S>`，`PhantomData<fn() -> (A, S)>` 标记，零布局成本）——构造期一次定型（`JsonlJournal::<A, S>::create(..)`）后方法调用零 turbofish 零 UFCS 零绑定标注。旧世界 `S` 不被方法参数约束，每调用点 E0283 强迫 `Journal::<A, S>::` 全限定 + `MutexGuard` 场景再加 `&mut *guard` 显式 deref（终评三槽点之三），税清零。`skeleton::JournalPort<A, S>` → `JournalPort<A>` + `type State` 同款（`restore` 返回 `Restore<A, Self::State>`）。dyn 焊接面保留：`DynJournal<A, S>` = `dyn Journal<A, State = S>`。（**破坏性**：`JsonlJournal` 具体类型携带 (A, S) 参数，构造点需类型标注；外部实现方在 impl 块补 `type State = S;` 一行。方案对比与三探针证据：ADR 0005。）
- 文档伺候面全撤（E0283 专伺候教行清零）：教程驻足节 UFCS 全限定改为「构造期定型、调用期直呼」；cheatsheet §1 惯用形行与坑 4 重写（新坑=定型账本塞异型 payload 的 E0308）；README 第一天提示同步。教程坑 A 教学指令修正：删 `#[serde(tag)]` 不复现（struct variant 外部 tag 恰好是 object）→ 改为「给 Clear（unit variant）入账」真实命中路径，重跑报错逐字命中。README「只要散件」最小 Cargo.toml 组合示例补齐。埋点宏 `embed_event!` 单源化评估：`#[macro_export]` 单源可行但劣化面更大（公开 API 面+feature 耦合+cfg 复杂度 > 14 行冻结宏同步成本），维持双副本+两侧同步责任注释（ADR 0005 §埋点宏）。

### Added（批5-2 std-ext 七域迁移收口）
- std-ext 扩至十三常驻域全量：`paths`/`clock`（含 `clock::rfc3339` 行为件）/`retry`/`scheduler`/`ratelimit`/`cache`/`id` 自 `skeleton-infra` 物理迁入（各域集成测试随迁；自持采集器 `cache_multidim` 迁 std-ext/examples；memgov 依赖的五采集器留守 infra examples 改测 `std_ext::` 面；`humantime` dev-dep 随 RFC3339 对照测试迁 std-ext，infra 侧留守 bench 仍用）（**破坏性**：`skeleton_infra::{paths,clock,retry,scheduler,ratelimit,cache,id}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim）。`log-embed` 对账新基线 41=std-ext 七域 35 + infra shutdown 6。`shutdown` 留守 infra（胶水件反向依赖 `std_ext::scheduler`）。ADR 0004 补终版切面表。

### Added（批5-1 std-ext 拆分）
- 新子 crate `std-ext`：六个纯 std 叶子域自 `skeleton-infra` 物理迁入——`text`/`encode`/`hash`/`iter`/`backoff`/`fsx`（含各自内联测试/集成测试/示例/doctest 随域迁移；`log-embed` 埋点三域 9 调用点由 std-ext 自有同名 feature 承载，宏为同款文本作用域副本，target `skeleton::<domain>` 观测契约不变）。分层 `skeleton` ← `std-ext` ← `skeleton-infra`（反向依赖）；`cargo tree -p udf-std-ext` 默认树零三方（**破坏性**：`skeleton_infra::{text,encode,hash,iter,backoff,fsx}::*` → `std_ext::*`，infra 根再导出同批删除，无 shim；见 ADR 0004）。

### Added（批1A 契约下沉 + log 拆分）
- 契约层真下沉：`Clock`/`SystemClock`/`MockClock` → `skeleton::clock`；`EnvSource`/`StdEnv`/`MockEnv`/`ConfigPort`/`ConfigError` → `skeleton::config`；`LogPathResolver`（trait）→ `skeleton::logpath`。实现件留 `skeleton-infra`，无 re-export shim（**破坏性**：import 路径迁移，见 ADR 0001）。
- 新 feature `log-fs`：`logx` 文件落盘面（密钥掩码/路径解析/FileSink，纯 std 零三方依赖）；`log` 变为 `log-fs` + tracing 四件套门面绑定层（见 ADR 0002）。
- ADR 三篇落盘：ADR 0001、ADR 0002、ADR 0003（paths 域 dunce/XDG 拍板补档）。

### Changed（批2A API 形状四件，破坏性）
- `EnvSource::get` → `Result<String, EnvError>`；新增 `EnvError { Missing, NotUnicode(OsString) }`——键存在但值非 Unicode 不再谎报 Missing（`ConfigError` 相应新增 `NotUnicode` 变体）。
- id 类型化：新增 `Id(u128)` 值类型（`Display`/`FromStr`，RFC 9562 连字形）；删除 `to_string` 自由函数。
- `relative_to(path, base)`：参数序对齐 pathdiff 生态惯例（原 `(base, path)`）。
- JournalPort 双 trait 同名消歧：实现层方法改名 `append_action`/`restore_stream`/`append_snapshot`（端口词表不变），双 trait 同屏直呼零 turbofish。

### Fixed
- journal flush 补 `sync_all`（掉电真持久；`with_snapshot_every` 高频档有 µs→ms 成本警示）。
- 根 CI 修复：udf-frame 迁独立 workflow（`.github/workflows/udf-frame.yml`），根 ci.yml 收缩 harness-only；删除 inert 子目录 workflow。
- 性能口径诚实化：id_multidim ③位收窄声明 + ⑥竞争面 Barrier 栅栏对齐（spawn 移出计时窗；旧锚「无悬崖」结论作废）；reference.md logx 锚补 sync_all 换算警示。
- 文档精度：README 常驻域清单/埋点口径/default 漂移/debug 臂采用方警示/第一天提示（盲测 6 坑就近化）。

### Tests
- 全量 682 passed / 0 failed（`--test-threads=1 --no-fail-fast` 正典口径）；skeleton 契约层 `--no-default-features` 自持绿；clippy `--workspace --all-features --all-targets` 零警告；`cargo fmt --all` 清账。

### 独立盲测
- 采用方视角全流程盲测 4.35/5（有限采用）：UDF 核与池化三件值得采用；性能维 5.0（dispatch 80.3ns，契约 ≤100ns 达标）；稳态零分配亲手复现 PASS。报告：采用方盲测（本地留存，未入库）。
