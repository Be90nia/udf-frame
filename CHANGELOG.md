# Changelog

本文件记录 udf-frame 的对外可见变更（API/feature/文档/修复）。格式参照 Keep a Changelog；版本号语义遵循 SemVer。

## [Unreleased]

### Fix(drop): LeakGuard::drop 与 TaskHandle::drop 不再触发宿主回调（issue#183/#143 残留）
- `LeakGuard::drop` 不再 `eprintln!`（stderr 不可写会 panic）：改 best-effort 可失败写入并忽略错误；类型文档删掉「panic-in-drop 会被吞」的错误声明。
- `TaskHandle::drop` 改为静默取消：取消语义（CAS/墓碑/notify）不变，但析构路径不再同步调用宿主 tracing subscriber（#221 §2.5）；显式 `cancel()` 仍发 `skeleton::scheduler::cancel` 事件。
- 测试：`leak_drop_report_write_failure_does_not_panic`、`scheduler_drop_cancel_is_silent`。

### Fix(journal): 记录上限/权限/初始化原子发布/回放形状 fail-closed（issue#217/#212/#181/#178/#177/#106）
- **记录上限**：新增 `DEFAULT_MAX_RECORD_BYTES`（16 MiB，含终止换行）与 `create_with_limit`/`open_with_limit`；写侧经有界 sink 序列化（超限零落盘、writer 保持健康），读侧（头行/回放/快照扫描）读满上限未见换行即报 `JournalError::RecordTooLarge`/`ReplayError::RecordTooLarge`（带物理行号），超大行处理完回收行缓冲容量。
- **初始化不留半成品**：`create` 改「同目录独占临时文件 → 写头 + flush + `sync_all` → `hard_link` 原子发布」，失败只清本次临时文件并保留原始 IO 错误；`create_new` 语义不变（目标存在即失败，绝不覆盖）。
- **权限**：Unix 新建 0600（独立于 umask）；`open` 校验已有文件，含 group/other 位报 `JournalError::InsecurePermissions`（不隐式 chmod）。**迁移**：旧版本创建的 0644 账本需先 `chmod 600`；Windows 隐私靠父目录 ACL（文档声明）。
- **回放形状 fail-closed**：记录必须是 JSON object 且顶层 `t` 为字符串；非对象/缺 tag/非字符串 tag 一律 `Corrupt`（不再计为未知变体）；完整回放先消费头行（`skipped_unknown` 不再计入头行，首条 action 仍报物理行 2）。
- **写侧 fail-stop**：write/flush/sync 失败后置 poisoned，后续 append 直接 `Err(Poisoned)` 直到重开/恢复；`open` 要求头行带终止换行（否则 `HeaderUnterminated`）。
- **兼容性**：`JournalError` 新增 4 个变体（additive；枚举未标 `#[non_exhaustive]`，下游穷尽 `match` 需补臂）。

### Fix(logx): 软失败诊断改可注入 reporter + 写失败退役并降级恢复文件（issue#112/#184/#198/#143）
- `FileSink` 软失败告警不再走 `eprintln!`（stderr 不可写会 panic）：改走可注入的 `SoftFailureReporter`（默认 `StderrReporter` = 忽略错误的 `writeln!(stderr)`），新增 `FileSink::with_reporter`；`env_filter_or` 的两处 `eprintln!` 同策略。
- 写/刷失败即退役当前 writer（锁内摘除、锁外析构），当日落点降级 `主文件 → log-YYYY-MM-DD.recovery.log → 当日停写`；打开失败仍只重试新行。`dropped_lines` 保持权威计数，一次告警语义不变。
- 文档写明「恢复 ≠ 记录原子性」：不截断、不自动修复半行（跨进程 append 回截会删掉别的进程已写的行）。

### Fix(config/json): 配置读入上限 + tolerant 注释按空白处理（issue#197/#127/#196/#125/#195）
- config：`file_layer`/`file_layer_toml`/`load_toml` 读入上限 `DEFAULT_MAX_FILE_BYTES`（8 MiB，`ConfigFile::max_file_bytes` 覆盖）；按「最多读 limit+1 字节」强制，超限报 `ConfigError::File`（`source` 为 `ConfigFileTooLarge{limit, observed}`），不截断不部分接受。
- tolerant JSON：注释按词法空白处理（块注释内换行保留、其余补分隔符），`1/*c*/.5`、`1/*c*/e3`、`1/*c*/2` 一类 token 拼接现在报错；**未闭合块注释由「容忍到末尾」改为报错**（带原始开注行列）；剥离改单次字节扫描，去掉 `Vec<char>` 的 4x 放大。**行为变更**：未闭合块注释的输入以前可能解析成功，现在报错。

### Perf(bench): `alloc_atomics/relaxed_rmw` micro 臂 + `state_layout` 基准方差定性（issue#103）
- bench 新增 micro 臂 `alloc_atomics/relaxed_rmw`（一次 `fetch_add(1, Relaxed)` 的 ns/op），作为 `CountingAllocator` instrumentation 干扰的**上界估计量**；`perf.yml` 期望清单同步登记该 ID。既有 `state_layout_*` 臂零改动，公共 API 零变化。
- `state_layout_*` 相对 11-23 参考的 +8%~+14% 漂移定性：**跨会话/机器状态水平差，非代码回归**。同会话 OLD（`e5cbf26`）↔NEW（HEAD）二进制交替配对 8 轮：同一份旧代码隔两日差 −16.8%…+21.6%（20 个可比臂中 11 个区间不重叠），而 OLD→NEW 的配对比值有 28 臂落在 ±1–4% 配对噪声内；代码效应只有 4 臂达到或触及判据且幅度 ≤2.4%（`state_layout_update_scope/sharded_256parts_1k_all_{noretain,retain}` +1.7%/+1.6% 变慢、`state_layout_update/mono_16m_retain` −2.4% 变快、`state_layout_retention/sharded_window16` +1.3% 为边缘件），方向不一、未 bisect。
- instrumentation 变化排除：`state_layout_*` 与 bench 二进制的 `#[global_allocator] CountingAllocator` 同提交 `60a81de` 引入，11-23 基线 HEAD `e5cbf26` 已含它。
- 纪律：本机跨会话比较在 ±20% 内无判别力；性能归因必须用**同会话二进制交替配对**——单会话重复测量分不开「代码」与「会话」两个变量，「bench 臂没改」也不能证明被测路径没改。

### Fix(perf): 修正 12 条 scheduler 期望 ID —— 滚动基线恢复提升（#96 发现）
- `perf.yml` 的 `EXPECTED_BENCH_IDS` 曾把 scheduler 组写成下划线形态（`scheduler/register_1000`），而 bench 实际注册的是 group+参数形态（`scheduler/register/1000`）⇒ 完整性闸每轮判缺 12 条、`complete=false` ⇒ **滚动基线从未提升**（性能回归对比等于失效）。
- 按 `cargo bench ... -- --list` 实测改正 12 条；复核对 manifest ∪ 两 crate `--list`：expected 51 / actual 125、`missing_keys=[]`、`missing_groups=[]`。闸的 `complete` 逻辑与「不完整不提升基线」策略未动。
- 纪律：期望清单今后以 `--list` 实测为准，不凭记忆；删/改 bench 组时同步清单并在 PR 里给出 `--list` 对照。

### Fix(pool/audit): 审计订阅 gate 线性化 + 回调闭包整槽退役（issue#88/#89）
- #89：池域 `subscribe_audit` 与 `PoolAuditTicket` Drop 把「槽替换/摘除 + `gate` 翻转」放进同一 `audit` 临界区（对齐 Store 域 #60）——旧票据被替换后 Drop 为 inert，绝不关掉新代次的闸；新增机械锚 `store_gate_under_slot_lock`（debug 构建 `try_lock` 断言调用点持锁，release 零成本），`emit` 快路径零改动。
- #88：Store 域 `AuditTicket::drop` 与 `SyncStore::subscribe_events` 改为**整槽退役**——锁内只 `take`/`replace` 并完成代次/gate 线性化，锁外 `drop(retired)`；池域核对 #89 已满足。`AuditSink` 文档写明「只 take `notify` 而把其余字段留锁内析构」= 同缺陷变体。
- 验证：两域各 4 条测试（票据 Drop / 订阅替换 / 在途回调只销毁一次 / **控制臂**：跨线程持锁时同一探针必报 Held）+ 5 条池域 gate 交错测试；变异红（`retired` 放回锁内、`gate` 翻转移出锁）各自归因；crate 全量 645 passed。

### Fix(runtime): 三个容量 setter 与各自 population 共锁线性化（issue#95）
- `SyncStore::set_capacity`（`inner.queue` 锁）/ `set_deferred_capacity`（`inner.deferred_queue` 锁）/ `Scheduler::set_capacity`（`shared.state` 锁）在锁内发布 ⇒ 与 `enqueue`/停放/`register`（同锁内读 cap）线性化；删掉「运行期调整与准入竞争 = 尽力而为」。
- 契约：缩容**不回溯**既有记录/注册；扩容与 `0`（无界）即时生效；三个 population/三个 cap 互不合并；热路径零新增锁（enqueue/停放/register 一行未动）。
- 测试闸为 `#[cfg(test)]` **私有**件（`std-ext` 无 `testing` feature，公共面零变化、生产零检查）；7 条确定性用例（锁内停靠 + 有界负向断言，放行先于断言 ⇒ 变异时是失败而非挂死）+ 3 变异红。

### Fix/Docs(pool): resize 事件时序 + 工厂预留语义（issue#99/#100）
- #99：`PoolBase` 新增无回调 resize 原语；`MemoryPool`/`ThreadPool` 先提交自身状态、**释放全部内部锁后**再发 `Resized`/`ResizeRejected`（回调可重入 `resize`/`acquire`/`metrics`）；`ThreadPool::resize` 的 epoch 在**任何用户回调之前**领取（「最后发起的目标生效」补 #69 遗留窗口）；两个 `metrics()` 明确为**逐面非原子快照**。
- #100：`created` 定义为「池宇宙已占席位 = idle + 已借出 + 工厂创建中预留」；缩容**不撤销**已取得的创建预留（`created` 可短时高于新 max，新 acquire 一律按新 max）；`borrowed` 不含在途预留、`PoolMetrics` 非完整预留账本，创建中数量用 `slot_snapshot().created − idle − borrowed` 反推（`feature = "testing"`）；不新增公共 `creating` 指标。
- 验证：#99 七条确定性用例（重入/半状态/并发撕裂/暂停后 epoch 顺序）+#100 三条 barrier 工厂测试 + 两处变异红；crate 全量 645 passed。

### Fix(fx): spawn 的 runtime 上下文前置条件显式化 + timeout 前置条件文档化（issue#92）
- 新增 `AdmissionError::NoRuntime`：`try_spawn_tracked` / `spawn_detached` 在**取任何内部锁之前**做 `Handle::try_current()` 预检；无 runtime ⇒ typed 拒绝，不入容量账（`admitted/tracked/active/waiting/rejected` 全不变）、不消费宿主 Future。
- **破坏性**（0.1.0）：`spawn_detached` 返回类型 `ScopeClosed` → `AdmissionError`；`AdmissionError` 仍非 `#[non_exhaustive]` ⇒ 跨 crate 穷尽 `match` 需补 `NoRuntime` 臂（仓内 `validation/consumer-sim` 已同步）。`spawn_tracked` 作为 fail-visible 旧口：无 runtime 带因 panic（新增 `# Panics` 并指向 typed 口）。
- 入集改 `JoinSet::spawn_on(fut, &handle)`、脱管改 `handle.spawn`（不再有裸 `tokio::spawn` 与 clippy allow 豁免）；`shutdown_timeout` / `Spawned::join_timeout` 仅新增 `# Panics`（需 `enable_time()` 的 runtime），签名与返回面零变化。
- 验证：6 条确定性测试（typed 拒绝且锁外 / 不等待 `set` 锁 / legacy 带因 panic / runtime 停机时任务被丢弃且无残留 / 准入路径不变 / timeout 两类环境故障分开）；三处变异（预检挪到 `admit_gate` 后、挪到 `reserve_slot` 后、搬进持锁区）各自红；crate 全量 629 passed。

### Fix(runtime): 本地订阅 entry 整条移出后析构（issue#90）
- `local_sub_unregister` 与 `local_notify` 的毒回调摘除段：`retain` 改「定位 + `remove` 移出」，表借用释放后再析构——用户闭包/捕获对象的 `Drop` 重入本地订阅不再撞 `RefCell` 借用（`BorrowMutError`）。
- 通知序、命中数、摘除数、成因摘要与「注销即时可见 / 死订阅不复活」语义不变；`LocalSubscription::drop` 体不动、`!Send + !Sync` 与公共 API 不变。
- 验证：三条 `DropProbe` 路径测试（普通退订 / 通知中途退订 / panic 摘除）；两处变异回 `retain` 各自红（第三条为快照强引用路径，结构性不敏感，已在回执说明）；crate 全量 611 passed。

### Test(journal): 用例目录进程唯一 + RAII 清理（bd udf-frame-9ez）
- `case_dir()` 路径加 `-<pid>-<seq>`，两进程并发跑同 crate 测试不再互删目录；新增 `CaseDir` 守卫（`Drop` 清理，含 panic 路径），删掉 16 处收尾 `remove_dir_all(..).unwrap()`（并发下的 `NotFound` 触发点）。
- 鉴别力：修复前固定目录并发 5/5 轮至少一方红；修复后双进程并发 5 轮 10/10 绿，`target-iso/journal-tests/` 零残留。断言语义零改动。

### Fix(pool): MemoryPool 归还裁决与槽位提交合成一个 core 临界区（issue#98）
- `PoolBase` 新增内部协作入口 `note_return_with(commit)`：持 `core` 裁决后、释放 core 之前执行实现方的真实入槽/配额回吐；`note_return` 保留「裁决与提交分离」供 ObjectPool 等实现方，文档写明两者适用面。
- `MemoryPool::PoolGuard::drop` 改走协作入口 ⇒ 「账本已改、槽位未提交」窗口消失；审计事件恒描述已提交状态（回调重入 `acquire/metrics` 不再看到半提交），事件仍在锁外发射。
- `resize` 整段事务留在 `resizing` 串行闸内；**被裁掉的用户对象析构移到闸外**（闸内析构会让用户 Drop 重入 resize 自死锁）。
- 验证：`memory_pool` 20 passed ×3；负向验证（旧实现）稳定复现 `idle > idle_cap` / `created > max` 与审计半状态；变异（析构放回闸内）⇒ 新用例有界失败；crate 全量 608 passed。

### Fix(fx): try_reap 两阶段收割——JoinError/panic 载荷归一移到 JoinSet 锁外（issue#91）
- `FxScope::try_reap`：`set` 锁内只搬出已完成结果，`is_cancelled`/`into_panic`/`panic_message` 与 payload 最终 Drop 全在锁外；记账与条目搬出同区（不低估，宁可短暂高估保守拒），锁序 `set → caps` 不变。
- 承重事实：tokio 1.53.3 里 future 的 Drop panic 在 harness `complete()` 被 `catch_unwind` 吞掉，只有 `cancel_task()` 转 `JoinError` ⇒ 确定性复现只能走 abort（测试据此构造）。
- 验证：payload Drop 内 `set.try_lock()` 探针（旧代码红/新代码绿）+ 并发准入压测；reap 组 6 轮无 flaky；crate 全量 607 passed。

### Fix(std-ext): 注入 Clock 调用移到 cache/scheduler/ratelimit 内部锁外（issue#101）
- `SyncTtlCache::insert`：满插清扫时间快照改为取锁前取样；`Scheduler::worker_loop`：删除持 state 锁的取钟，改用本轮锁外样本 + 持锁复检（停信标/关闸/堆顶更早 deadline）；`TokenBucket::settle_and_take`：锁外采样，旧样本晚提交记 0 且不倒退 `last`、不 panic。
- 类型/方法 rustdoc 写明「Clock 锁外 / 键 Hash·Eq 锁内 / 读路径 V::clone 锁内」三类边界分开记。
- 验证：14 条新测（探针 + 故意持锁对照 + 可重入 + panic + 丢唤醒 + 逆序提交）；五处变异各自红；std-ext 492 passed；clippy/fmt/doc 0。

### Fix(scheduler): OS 线程创建失败可观察（issue#87）
- `Scheduler::spawn_worker` 原先直接 `thread::spawn`，OS 创建失败只能以 panic 呈现、调用方无从恢复。新增 `Scheduler::try_spawn_worker -> io::Result<WorkerHandle>`（`Builder::spawn`，`io::Error` 原样返回、kind 不变，失败零残留：不产生 WorkerHandle / 停信标 / 线程句柄）。
- 保留 `spawn_worker` 原签名作为 fail-visible 入口：内部委托新入口，失败带因 panic（消息含 `io::Error` 与 kind），rustdoc 标 `# Panics` 并指向新入口。
- 失败后调度器保持开放：注册照常受理，显式 `tick()` 仍按 deadline 点火；`WorkerHandle::drop` 仍只停本 worker（#72 语义不变）；不引入 worker 计数/启动状态原子。
- 测试注入缝为 `fn` 指针（仅起 worker 时一次间接调用，注册/点火/取消热路径零触碰），非 test 构建整体不存在。验证：std-ext 478 passed / 0 failed；变异（撤回注入缝）三条新测试全红。

### Docs(fx): FxScope 阻塞边界与截止保证（issue#94）
- `fx.rs` 模块文档新增「执行模型与阻塞边界」：三类阻塞（协作式 async / 同步阻塞·长时不 yield / 真正 blocking work）、截止不是硬墙钟上限、`TaskError::TimedOut` 与 `ShutdownReport::timed_out/still_running` 只表示「截止已过 / 已请求 abort / 终止未观测」。
- `spawn_tracked` / `try_spawn_tracked` / `Spawned::join` / `shutdown_timeout` / `join_timeout` 补对应保证边界；框架不做 blocking adapter（不承诺强制终止任意 Rust 代码、不在 `FxScope` 内造 watchdog 线程）的理由落点写清。
- `docs/reference.md` fx 章节三条 + `README.md` 消费方节（`spawn_blocking` / 独立 `ThreadPool` 写法；current-thread 禁用 `block_in_place`），证据引用 `f9b413d` 的两条边界测试。零代码/零测试改动。

### Fix(tests): 挂死类收口——护栏单源化 + 进程级兜底 + #94 两项确定性证明（issue#102/#94）,2026-11-24)
- **现象**：CI 两次 job 撞 `timeout-minutes: 20`（run 45311454174 / 45312409945），日志 `test fx::tests::shutdown_timeout_aborts_inflight_and_join_yields_cancelled has been running for over 60 seconds`。
- **机理（tokio 1.53.3 逐函数核实）**：`yield_now()` 已改为把 waker 交给当前 worker 延迟冲洗（`context::defer`）；带延迟 waker 的 worker 反复做 0 超时 `park_yield`，期间短暂独占 IO driver；另一 worker 此时 park 抢不到 driver 即回退为**不持 driver 的普通休眠**；时间轮只在 worker 进 `park_internal` 时被处理、注册 timer 只去打断正在 IO park 的线程 ⇒ 自旋任务占住原 driver 持有者后再不返回，全 runtime 无人在 driver 上，50ms 截止永不到期。
- **修复**：①护栏单源化到 `skeleton-infra/src/test_support/guard.rs`（crate 内单测与 5 个集成测试共用；`tests/common/guards.rs` 删除，无 shim、无第二份实现）；②`wait_until` / `bounded` 均加**进程级兜底**——OS 截止 → `fire` → 2s 宽限轮询**被守护线程栈上**的完成位（正常返回与 panic 展开都置位 ⇒ 零误杀）→ 具名诊断（测试名、上限、耗时、线程 id、指向性提示）+ `std::process::abort()`，把 20 分钟 job 超时变成 ≈12s 具名失败；库内不新增 watchdog 线程；③自旋握手后补一记 `kick_scheduler`（走既有 `spawn_detached` 通道，零新增 clippy 豁免；闸已关的站点用一次性 scope 发起），唤醒无 driver 的休眠 worker 让它接手时间轮；④crate 内 2 处、集成测试 6 处计时器 await 接入 `bounded`（业务断言零改动）。
- **#94 设计验证·两项确定性证明**（`tests/fx_lifecycle.rs`）：multi-thread——一个 worker 被同步阻塞时另一 worker 仍按期观察 shutdown 截止（`timed_out=1 / still_running=1 / completed=0 / cancelled=0`，被阻塞任务不被标成已终止）；current-thread——受控 Future 在 poll 中同步阻塞唯一 executor 线程，50ms 截止在 300ms 内不前进，放行后才观察到已过期 timeout。**顺带钉死一条口径**：current-thread 上 `shutdown_timeout` 的 `timed_out` 结构上恒 0——读截止必须拿到执行权，而拿到执行权那一刻 abort 早已处理（`in_flight` 已归零）。
- **验证**：`58 targets / 600 passed / 0 failed / 1 ignored`（基线 597 → +2 证明 +1 lib 侧护栏单测）；`--lib fx::` 与 5 个集成目标各 ×10 连跑全绿；满核压测一轮 0 失败、`over 60 seconds` 0 次、护栏诊断 0 次；变异一（外部线程永不放行）= 12.02s 具名 abort（rc `0xC0000409`）；变异二（阻塞改可让渡）= 负向断言转红；clippy `-D warnings` / fmt / doc 全 0；改动文件 LF + 严格 UTF-8。本机 16 核在 18/64 忙循环下均未复现 CI 的 2 核抢占窗口（新旧构造各 200/200，K=0），机理证据 = 源码推演 + CI 侧观察，持续观察留给 main CI。
- 注：上文 2026-11-23 条目所述 `tests/common/guards.rs` 为当时路径；自本条目起护栏已迁至 `src/test_support/guard.rs`。

### Fix(perf): 修正 Scheduler 基准清单误判 + PR 路径验证
- Criterion 实际 Scheduler ID 使用 `scheduler/register_1000` 等下划线形式；旧期望清单误写斜杠，导致已跑出的 12 个基准被误判缺失，阻止完整性能基线提升。
- 修正 12 个期望 ID；perf workflow 增加仅针对自身配置与 bench 定义变更的 PR 路径触发，并设 45 分钟 job 上限，仍只产出数据、不作为合并阻断闸。

### Fix(tests): OS 截止线程可取消并确保回收
- `bounded` 的独立 OS 截止线程由不可取消的 10s sleep 改为 OS 截止时间 + `park_timeout`；目标 future 提前完成时 RAII guard 设置取消信标、unpark 并 join，避免遗留睡眠线程。
- 截止线程按绝对截止时间循环，容忍 `park_timeout` 虚假唤醒；新增回归测试验证提前 drop 能在远早于 10s 的时间内完成回收。
- 将限流器「睡眠不持锁」测试改为可控虚拟 Sleeper + 显式闸门，去掉 50ms 真实时间窗口与 100 次探测跨过回填时刻的竞态。
- 明确边界：若同步自旋占死整个 current-thread runtime 线程，内部 select 仍无法运行，CI job timeout 继续作为外层进程护栏。


### Fix(tests): 有界等待护栏改用 OS 时钟——自旋任务会饿死 tokio 时间驱动,原护栏自身无界,2026-11-23;CI 两次挂死取证)
- **现象**：CI 两次挂死（ubuntu 与 macOS 各一次，被新加的 job `timeout-minutes: 20` 掐断，否则默认 6h）；日志 `has been running for over 60 seconds` 指向 `timeout_reports_survivors_without_claiming_termination`。
- **根因**：测试用「tokio 任务里无 await 的自旋循环」构造不可让渡段，而**护栏自己用 `tokio::time::timeout`** ——自旋占住 worker 后饿死 tokio 时间驱动 ⇒ 护栏永不触发 ⇒ 挂死。**用 tokio 计时器保护可能自旋的场景是自相矛盾**。
- **修复**：新增共享件 `tests/common/guards.rs`（`#[path]` 引入，同 `log_capture.rs` 惯例）：`wait_until` 用 `std::time::Instant` 截止（5s）；`bounded` 用独立 std 线程睡 10s + `oneshot` + `tokio::select! { biased, ... }`（future 已就绪优先于火警）。4 处内联 `tokio::time::timeout(LIMIT, …)` 全部改 `bounded`；5 个测试文件净 −83 行（删掉两份重复 helper）。
- **验证**：5 目标 ×10 连跑（串行/默认各 5）50/50 绿；**变异**：`pending()` → 10s panic、`|| false` → 5s panic（原始 panic 文本留 `verify/2026-11-23-hang-guard/mutation.log`）；full 592 passed/0；clippy/fmt 0。

### Docs: 层边界清单落地 + 9 条文档矛盾清零（C1–C9）,2026-11-23;reports/2026-11-23-layer-boundaries.md)
- **层边界清单**：5 层 / 32 行「职责｜承载者｜依据｜边界点」+ 蓝图 59 条可机判断言逐条核对（48 Conform / 2 Diverged / 9 Not-verifiable）+ 结构级复核（`skeleton ← std-ext ← skeleton-infra` 零反向引用；全仓唯一 `tokio::spawn` 在 `fx.rs`）。
- **C1**：std-ext `log-embed` 调用点统一为实测 **七域 48 / 合计九域 55**（backoff1/cache9/fsx6/iter2/ratelimit4/retry12/scheduler14；infra 本 crate 7），并写入口径；此前 README/reference/lib.rs/Cargo.toml 分别写 35/41/42。
- **C2**：`ShutdownReport` 文档代码块 3 字段 → 8 字段；蓝图「six buckets」改为「五终局桶互斥穷尽 + 正交 `timed_out` + 证据标志」。
- **C3**：`serde_json` 由「std-ext 普通依赖」更正为「门控依赖（ADR-0007），默认零三方」。**C4**：`compose` 依赖面 `—` → `runtime`。
- **C5**：资源表补第 10 行 ingress（`set_capacity`，0=无界，满 ⇒ typed `Full`/`false`/panic fail-visible，`admission_full_count()` 可观测）。
- **C6**：代码注释「待 #56 拍板」→「已拍板 keep current（2026-11-23），正典见 reference 资源表」。**C7**：蓝图 `CancelToken` 行号锚改符号锚。**C8**：thread_pool 测试数 30→实测 32。**C9**：README feature 表补 `config-json`/`config-toml`/`full`/`unsafe-evidence`/`random`。
- 纯文档（11 文件，+76/−47）；ADR 历史计数按「不可变」保留。验证：rustdoc all-features 0；clippy -D warnings 0；fmt 0。

### Fix(设计对照整改): #72 per-worker 停信标 / #64 闭门先于重入 / #53 示例 / bench 注释纠偏,2026-11-23;reports/2026-11-23-design-conformance.md)
- **#72（blocker，设计偏离）**：`WorkerHandle::drop` 由「scheduler-wide `close()`」改为 **per-worker 停信标**（`Arc<AtomicBool>`，Drop 在状态锁内置位 + `notify_all`；置位持锁、判定持锁 ⇒ 无丢唤醒）。丢一个句柄**不关闸**、不影响其他 worker 与既有登记；`join` 仍是显式 scheduler-wide 停机；全部句柄丢弃而调度器仍开 ⇒ 注册入堆不点火。测试：新 two-worker 用例（通道握手证明既有/新登记都点火）+ 三条旧用例改写为 worker-local 语义 + 10 次防 flaky；变异（改回 `close()`）3 用例红。
- **#64（blocker，设计偏离）**：`wait_quiescent` 的 `{lock(sub_gate); closed.store(true)}` 上移到重入判定**之前** ⇒ `Reentrant` 返回时准入已关（外部 `try_dispatch` 得 `Closed`）；重入路径仍不回调 `drain()`；`close_wait`/`close_wait_timeout`/`Reentrant` 三处 rustdoc 对齐。回归：既有重入用例尾部加外层 `try_dispatch == Closed` 断言（**修复前实跑红**：`left: Accepted / right: Closed`）。
- **#53 验收第 1 条（F5）**：新 `skeleton-infra/examples/state_layout.rs`（132 行，整体 vs 分片布局的真实深拷贝字节数；4 臂 + 3 条主张闸 + `Arc::ptr_eq` 结构断言；变异实测红）+ `Cargo.toml` 的 `required-features = ["runtime"]`（单开面不编译）。`docs/reference.md` #53 节加示例指针。
- **bench 注释纠偏**：`PartitionedReducer::reduce` 的「无快照钉住则原地写」为误——`s.clone()` 已使每片 `Arc` 计数 ≥2 ⇒ **被碰分片必深拷贝，与持快照无关**（示例实测持/不持同为 65,536 B）；快照只决定旧世代留存。`RetainShape::None` 说明同步收窄为「根发布原地写」。
- **#25 证据回贴**：Stage A/B/C + 资源表决定已贴在 issue#25。

### Ci+Docs: 挂死护栏、零/default 面闸、停机依赖序、资源表决定回写,2026-11-23;外部对标 F-11-1/F-11-4 + 设计对照 F3/F4)
- **CI 挂死护栏**：3-OS 主 job `timeout-minutes: 20`、矩阵/MSRV/夹具 job 各 15 —— 2026-11-23 实测一次 lib 测试在 ubuntu 挂死 >16min（Windows 全绿），默认 6h 上限会把 flaky 变成半天排队。
- **fx 串行复跑步**：`--lib fx:: -- --test-threads=1` 与主步（并行）互为对照——主步红本步绿 ⇒ 并发挤占复现；两步都红 ⇒ 真逻辑（自旋/计时类测试在 4 核 runner 上与其它多线程测试互相挤占）。
- **零 feature / default 面闸（F-11-4）**：新 job `zero-and-default-face`——`--no-default-features` 的 check+test+doc 与 default 面的 test+doc；此前矩阵 14 叶 + full 从不编译零面、default 面只有 MSRV check。
- **停机依赖序（F-11-1）**：`shutdown` 域模块头补「停机序=注册序 ⇒ **依赖方必须先注册**（Erlang/OTP 逆启动序口径）」；本域不持依赖图，排序责任归调用方。
- **资源表（F3 + 维护者拍板）**：行 1/2 标注提案值 `1024`/`4096` **未采纳**（保持无界）；决定记录改为全表 keep current（Run-ID 1024 / deferred 64 / 审计环与待投递 1024 / 回灌深度 64）。
- **#25 证据回贴（F4）**：Stage A/B/C 证据与 CI 绿 + 资源表决定已贴在 issue#25。

### Fix(tests)+Docs: 分配锚线程局部化 4 文件 + 资源表数值决定记录（keep current）,2026-11-23;bd udf-frame-2h7)
- **4 个兄弟文件的同型潜伏一并根治**（`alloc_site` / `run_id_diagnostics` / std-ext `alloc_anchors` / `paths_alloc`）：旧实现「进程级 `static AtomicUsize` + TLS 窗开关」在同一二进制并发测试窗口重叠时会多算别人的事件（`audit_no_subscriber_alloc` 已实测 1 次假红）。统一改为窗口开关与计数同住 `thread_local! Cell<(bool,usize)>`；**顺带纠正** `alloc_anchors`/`paths_alloc` 注释里「已按线程归属」的失实自称（此前实现与注释不符）。4 个目标各 10 连跑全绿，全量两 crate 绿。
- **资源表数值决定（维护者 2026-11-23 拍板：keep current）**：3/4/5/6/7 行与 watchdog/scheduler/Run-ID 默认值全部保持现值，依据 Stage C「全默认开箱可用、无改小证据」；已写入 `docs/reference.md` 决定记录段，行内「待拍板」标注改为「已定」。

### Fix(audit 整改): 12 维审计 4 条 P2 + 2 条 P3 清偿,2026-11-23;reports/2026-11-23-12dim-audit.md)
- **F1（#60 测试鉴别力）**：落闸改走 `AuditTicket::store_gate_under_slot_lock`，debug 下用 `audit.try_lock().is_err()` 断言「闸落必持槽锁」——把落闸挪出临界区（#60 缺陷形状）时测试**直接红**，不再依赖暂停钩子位置。release 零成本。
- **F2（Stage A 在 CI 空跑）**：夹具断言住在 `fn main()` ⇒ `cargo test` 对纯 bin 包 0 tests；CI 新增真跑两夹具 bin（async 根 + sync workflow，`--locked`），exit 0 才算端到端。
- **F3（并发停机报告窄窗丢载荷）**：`DrainSlot::Settled` 携带该次排空的 canonical 报告，`begin_drain` 改**单次 borrow** 同时取状态与载荷 ⇒ 结算瞬间到达的调用也拿到 canonical 报告（此前会拿默认报告、panic 载荷静默丢）；`shutdown`/`shutdown_timeout` 两分支同步；`reference.md` 口径更正。
- **F4（`try_reap` release 下溢）**：加 `debug_assert` 锚住「tokio 任务析构先于 JoinError 观测」外部不变量并注释；release zero-cost。
- **F6**：CI bench 注释改「五件并集（tokio/journal/memgov/testing/pool，`fx` 是模块不是 feature）」+ 注明 `-p udf-skeleton` 无 bench 目标。
- **F7（fmt 闸对夹具空心）**：三个夹具是独立 workspace，根 `cargo fmt --all` 看不到 ⇒ CI Rustfmt 步逐仓 `--check`（3 行）；并已 `cargo fmt` 排好 5 个文件（纯格式）。
- **F3 文案补完**：`drain_interrupted` 公共 rustdoc 与 `shutdown_timeout` rustdoc 改「该次排空的 canonical 报告副本」；`fx_lifecycle` 测试注同步；CHANGELOG #70 条目加「已被 F3 修订」标注。
- **F8（复审新观察）**：`tests/audit_no_subscriber_alloc.rs` 的分配锚改**真·线程局部**（窗口开关与计数同住一格 `Cell<(bool,usize)>`）——旧实现进程级 `EVENTS` + TLS 窗开关，同二进制并发测试窗口重叠时会多算别人的事件（复审实测 1 次假红）；改后 10 连跑全绿，无需串行步。
- **未改（P3 保留）**：F5 `Display` 中英混排（与 `shutdown.rs` 既有惯例同形，排文案批次）。

### Ci(docs): 单开 feature 面 doc 闸 + 面内 intra-doc 断链清偿,2026-11-23;bd udf-frame-ilh)
- **门洞**：docs 门原来只跑 `--all-features`，指向**门控符号**的 intra-doc 链接在该面能解析、单开面解析不到 ⇒ 单面断链静默通过。逐面实测 15 面中 **12 面红**（`crate::fx::*` 挂 `tokio` 门、`load_toml` 等挂 `config-toml`、`init_*`/`ChannelRegistry` 等挂 `log`）。
- **清偿**：把跨门控符号的链接改成 code span（与 config.rs 既有修法同款；模块头注在任一面都编译，链接不得依赖别的门）：`shutdown.rs` 1 处、`runtime.rs` 6 处、`config.rs` 3 处、`logx.rs` 模块头 14 处。
- **闸**：`feature-isolation` 矩阵每面加 `RUSTDOCFLAGS=-D warnings cargo doc --no-deps`（infra 15 面 + std-ext 3 面）；实测 18 面全绿。
- 纯文档 + CI，无公共面改动。

### Docs(runtime): 澄清弃队留痕的口径混同与不可读边界,2026-11-23;bd udf-frame-8ad)
- **纠偏**：`Drop for SyncStore` 旧文案称「drop 时队列非空只可能是预算止步残留」——**与实现相反**：store 句柄可在排空进行中被丢弃（另一线程持 sender / inner Arc 仍在跑），此快照到的队列可能是普通未处理 action。
- **声明**：弃队数与 `feedback_budget_exceeded` 同账合并，两个 population 不可区分；**最后一份句柄析构后该留痕不可读**（累加点在析构、读口在实例上）⇒ 该路径只服务白盒/仍有 inner 持有者，**不是对外可观测面**；对外可观测面是 `ShutdownReport` / `rejected_after_close`。
- 不改机制（按 bd 8ad 方案②：显式声明边界），无公共面改动。

### Docs(runtime): 纠正「unwind 清理期零用户代码」的不实表述并补 Action 析构义务,2026-11-23;GitHub issue#66)
- **纠偏**：`DrainGuard` 的 `Drop` **不调任何回调**（reducer / 解释器 / 订阅 / 审计），但框架**拥有并按值丢弃**被摘下的 action 载荷 ⇒ 展开清理期**必有用户代码**（锁已在结算与析构前释放；**锁外 ≠ 无用户代码**）。旧表述「零用户代码」为误，已在 CHANGELOG 历史条目、`runtime.rs` 内部注释与 `docs/reference.md`「Panic 契约」三处改正。
- **契约（窄口径，按维护者定稿）**：正常运行期载荷仍在内部锁外析构，**不新增**全局 panic-free Drop 要求；**DrainGuard/`SyncStore` 展开清理期**被丢弃 payload 的 `Drop` **必须 panic-free**（二次 panic = abort）且**不得重入同一 store**（排空旗已放，重入会夺新租约并跑排空环）；只读面安全。
- **不加** `catch_unwind` 隔离（捕得住 abort、挡不住重入，且会在 `Drop` 里引入新的诊断策略）；保留 #41 fail-visible 终局结算与「绝不重放」。
- **契约落点**：`skeleton::Store` 语义契约 7 + `SyncStore`「Action 析构义务」+ `docs/reference.md` Panic 契约第五条。
- **测试**：新 `tests/drop_contract_unwind.rs`（1 例 + 观测夹具）——detached 载荷在析构期调用只读面与准入（有界 10s）成功 ⇒ 证明**锁未持有**；**变异证据**：在 `DrainGuard::drop` 结算循环前插 `lock(queue)` ⇒ 该测 10s 超时判红，回滚后字节一致。
- 纯文档 + 测试增量，无机制/公共面改动。

### Tests: 补 #60 未订阅零分配锚 + #61 首/中/末 panic 三档 + 不重试断言,2026-11-23;bd udf-frame-u11)
- **#60 零分配机械锚**（新 `tests/audit_no_subscriber_alloc.rs`）：未订阅时 32 连发（含 `Effect::Run`）**分配事件 == 0**——伴随断言「32 条效果全到解释器 + `take_events()` 为空」防「没干活」假零；**不恒真**由三条控制臂证明（冷启动框架分配 / 订阅激活 / 订阅态回调确定性 `with_capacity`），另断言弃票后回落 0（gate 真关断）。「一次 gate load、无锁」仍为结构保证（分配器观测不到锁）。
- **#61 三档 panic 轴**（`tests/close_report_contract.rs`）：`PanicOnFirst` → 参数化 `PanicOnNth`，新增 **middle**（`effects==2` + 段身份精确 + `calls==3` 不变）与 **final**（中断在段末 ⇒ `effects==0`、无段、空壳段被跳过；再次报告同值）两档。
- **不重试直接断言**：`close` → `close_wait_with_report` → `discard_pending_effects` 全程宿主 `run` 调用**恒 == 1**（任一步重放即红）。
- 纯测试增量，无行为/公共面改动。

### Added(runtime): result-bearing 完成面 + deferred 有界恢复,2026-11-23;GitHub issue#57 / #49)
- **#57 可等待完成面（加法，不动 `Store` trait）**：`SyncStore::dispatch_with_receipt(action) -> Result<DispatchReceipt, AdmissionOutcome>`——`wait()`（可重复）返回 `DispatchOutcome::{Completed, Deferred{retained}, CapacityRejected{retained, rejected}, Failed}`。语义：正常根子因果树完成 ⇒ `Completed`；后代被回灌深度预算停放 ⇒ **`Deferred`（不是成功）**；deferred 队列满导致续接无法保留 ⇒ **独立非成功容量结局 + 专用计数**（不报 `Completed`、不把被拒记录算成 retained）。`try_dispatch` 与 `ActionSender` 仍 admission-only；`DispatchFailure`/`DispatchPhase` 公开化。**硬约束**：禁止在排空回调内等待该收据（自死锁）——rustdoc 写明 + 代码侧 fail-visible 断言。
- **#49 有界续接**：`resume_deferred(limit) -> DeferredResume{resumed, remaining, deferred_again, capacity_rejected}`——FIFO 取最旧、持队列锁搬运（与 close/准入串行、不插队）、每条开**新回灌深度 epoch** 并保留原 action 序号与 deferral 来源；恢复出的工作拿**独立续接收据**，**原根收据保持终局不可变（`Deferred`）**；闭门后恢复返回 `resumed=0` + `remaining`（可观测而非静默）。
- **停放容量独立可配**：`set_deferred_capacity` / `deferred_capacity()` / `deferred_actions()` / `deferred_capacity_rejected()`；**独立于 ingress `capacity`**；默认 `DEFAULT_DEFERRED_CAPACITY = 64`（**占位，数值归资源表 #56**）。
- **清理**：删除 #44 时代「退回队尾 / 后续 dispatch 有界重放」等过时注释（改成停放 + 显式恢复口径）；`deferred_queue` 的「flush_deferred 尚未实现」一并改写。
- **测试**：新 `tests/dispatch_outcome.rs`（5 例：完成 / 停放不冒充完成 / 容量拒收独立 / 关闭后无收据 / 无关并发根不窃取结局）与 `tests/deferred_recovery.rs`（6 例：FIFO+limit / fresh epoch / 原根收据不可改写 / 恢复与 close 串行 / 容量拒收独立计数 / 恢复排已准入外部工作之后），5 轮复跑全绿；**变异验证**：`Resumed` 不重置深度 ⇒ 5 红；容量拒收冒充 retained ⇒ 2 红。
- **未做（明示）**：`ActionSender::try_dispatch_with_receipt`（本批加法面在 `SyncStore` 侧）。

### Fixed(runtime): 完成账本在仍有在途后代时被归还复用池（收据记账错配）,2026-11-23;GitHub issue#84)
- **缺陷**:`release_ledger` 只按「根 && 非等待者」归还，不检查账本是否终局 ⇒ 非等待型根（`try_dispatch` / `ActionSender::dispatch`）在 `Effect::Emit` 后代仍排队（`outstanding > 0`）时把账本推进复用池；并发外部准入 pop 后 `reset` 清掉这些后代的计数 ⇒ **另一个根可能提前发布终局，或携带别的 action 的失败**（收据正确性缺陷，非仅池卫生）。
- **修复（维护者定稿：终局门控）**:账本侧 `claim_release` 四查（未 released / 非等待型根 / **终局已发布** / `outstanding == 0`）；**每个可能使账本终局的转移**（`finish_root` / 最后一个 `descendant_done` / `detach_deferred`）与**三条调用面**（正常结算 / 当前记录 unwind / 摘队 sweep）都走同一 claim——旧的「无条件按根归还」判据与 `ActionRecord/CurrentMeta.waiter` 字段**整体删除**；入池在**账本锁外**（claim 锁内、push 锁外，queue→ledger 序不变）。
- **owner 资格显式化**：等待型根的 owner 是外部等待者（`wait()` 消费 `Outcome` 后 `waiter_release`）；非等待型根由因果链最后一次转移归还——**不由任何后代记录的 `waiter` 推断**。
- **不变式硬化**：`reset` 在 debug 构建断言「终局 + `outstanding == 0` + 已 claim 归还」；后代计数递减前断言为正（**不再用 `saturating_sub` 掩盖**重复/迟到完成）。
- **测试**：新 `skeleton-infra/tests/ledger_reuse_ownership.rs`（4 例，确定性闸门禁 sleep）：后代被闸住时账本不在池中且第二根提前发布终局=红 / 后代失败归属不串根 / 当前记录与摘队两条 unwind 路径 / 等待型根在 waiter 消费前不可复用 / deferred detach 后旧账本按终局结算。**修复前真红**（`left: 0, right: 1` 提前返回 + 错 `phase`），修复后 4/4 绿。

### Added(runtime): 历史 Run-ID 诊断改为窄口径 opt-in + 三态分类（默认关闭）,2026-11-23;GitHub issue#62)
- **策略（维护者定稿）**：历史 Run-ID 诊断**默认关闭**；`Run`/`Cancel` 转发路径**不变、不依赖**诊断是否启用。
- **新公共面**（窄口径，非通用策略框架）：`SyncStore::enable_run_id_diagnostics(&mut self, capacity)`、`classify_run_id(&self, EffectId) -> RunIdClass`、`run_id_tracked_count()` / `run_id_tracking_capacity()` / `run_id_evicted_count()`、常量 `DEFAULT_RUN_ID_TRACKING_CAPACITY`（容量**数值归资源表 #56 拍板**，此处为保守默认）。
- **三态**：`Known` / `DefinitelyUnknown`（**仅视界完整且无驱逐**）/ `NotTracked`（未启用 / 饱和 / 已驱逐）——**被驱逐的 id 绝不判 `DefinitelyUnknown`**。
- **旧计数语义显式化（不静默改义）**：`cancel_unknown_id` 现在只数「启用 + 视界完整 + 无驱逐」时的精确 miss；**默认关闭恒 0**，驱逐/饱和后停止增长（此后未知分类走三态读数）。
- **禁用路径零成本**：门闸前只有一次 `OnceLock` load，无 per-Run 锁、无分配——由 `run_id_diagnostics.rs::disabled_path_registers_nothing_and_allocates_nothing`（32 次新 id 的 `Run` ⇒ 分配事件 0、`tracked_count == 0`）钉住。
- **测试**：新 `skeleton-infra/tests/run_id_diagnostics.rs`（6 例：默认关闭不影响转发 / 视界内精确 / 饱和不误判 / 迟到启用视界不完整 / 分配计数锚）。既有 `api_polish` 与 `counter_concurrency` 各加一行 opt-in（**原断言不动**，语义变为「启用后仍精确」）。默认启用变异 ⇒ 2 例红（有鉴别力）。
- **行为变更（需知）**：`dispatch_per_beat/cancel_unknown_id` bench 臂现在测的是**门闸-only 路径**（不再有查表 + miss 留痕），跨该日期的性能基线不可直接对比。

### Fixed(fx): `CancelSource` 实现 `Clone`——接线后宿主仍可触发取消,2026-11-23;GitHub issue#56（Stage B G1）
- **缺口（第二消费方实测）**：`CancelSource` 非 `Clone` 且 `FxScope::with_source(src)` **按值取走** ⇒ 宿主外接取消源后**再没有任何句柄能触发取消**（`source.clone()` → E0599），只剩「接线前已取消」的粘性用例可测。
- **修复**：`#[derive(Clone)]`（底层 `watch::Sender` 本身可 Clone）。**Clone = 同一取消态的第二句柄，不是独立源**——所有句柄共享同一 watch 值，任一句柄 `cancel()` 全体可见；取消幂等、单向（`false → true`，不回落；与 #68 的无接收者粘性叠加）。
- **测试**：新 `skeleton-infra/tests/fx_cancel_source_clone.rs`（2 例，确定性握手禁 sleep）——① 源被 `with_source` 取走后，用预先 clone 的句柄取消 ⇒ scope 的 token 见取消、任务在下一让渡点结束、`in_flight` 有界归 0；② 原句柄与初始 token 全丢弃后，唯一 clone 句柄仍可取消（连打两次验幂等），之后接入的两个 scope 都观察到 `true`。**红**：修复前两例均 `E0599 no method named clone`（编译级红）。
- 公共 API 仅新增 `impl Clone`（无签名变更、无新依赖）；`docs/reference.md` API 目录与 Stage B 夹具注释同步。

### Added(validation): 第二消费方 Stage B（结构相异的任务调度 CLI 宿主）+ 资源默认值正典表,2026-11-23;GitHub issue#56)
- **Stage B 夹具入库**：`validation/consumer-sim/cli-host/`（受版本控制，自带空 `[workspace]` + `Cargo.lock` + path 依赖）——真实宿主形态：submit / cancel / shutdown 三段 CLI、自定义 `EffectRuntime`（Run + Cancel 两种效果的执行序）、**并发下有界准入的真实 `Full`**（condvar 握手、无 sleep、断言被拒 action 未入队）、`close_wait_with_report → Drained` + 闭门后 typed `Closed`、**combine/Fragment 组合根** + `Arc::strong_count` 观测 `Arc::make_mut` 克隆分支与快照冻结。
- **两面依赖树证据**：`cargo tree` 确定性脚本追加进 `validation/consumer-sim/dep-trees.txt`——sync 面 tokio 计数 **0**，`--features tokio` 面 **3**（反向控制：证明 feature 真接线、断言非恒真）。
- **CI 闸**：`consumer-sim` job 扩为 Stage A（workflow 夹具）+ Stage B 两面（sync/tokio 各自 build+test，`--locked`）+ 两面依赖树双向断言（sync 无 tokio / async 必须有 tokio）。
- **发现并单独立票的契约缺口**：`CancelSource` 非 `Clone` 且 `with_source` 按值取走 ⇒ 外接取消源后宿主再无可触发句柄（#56 Stage B G1，另单修）。
- **资源上限与默认值正典表**（`docs/reference.md` 新章「资源上限与默认值（决策载体）」）：按维护者裁定——**表是决策载体、#56 是证据载体**；统一约定五条（0=不限 / 默认保守可用 / 超界三分类且禁静默丢 / 界≠指标 / 改大兼容改小破坏走 ADR）+ 9 行现状基线（scheduler 注册上限、watchdog live-cap、Run-ID 诊断、审计环、待投递 FIFO、回灌深度预算、deferred 停放、FxScope 准入、池 idle）。**提案数值不写入**——待 Stage B 证据后一次性拍板。

### 文档(reference): 新增「资源上限与默认值」跨面正典表
- 各面 rustdoc 从此只写「界语义 + 超界行为」，数值引用该表；修订只走表（改大 = 兼容放宽，改小 = 破坏性 + ADR）。

### Fixed(runtime): 审计回调在夺租约 dispatch 首条 Enqueued 上的单线程确定性死锁,2026-11-23;GitHub issue#59)
- **缺陷**:夺租约路径在 `drain_owned` **之前**投递首条 pop 记录的 `Enqueued` 回调，而排空帧只在 `drain_owned` 内 push ⇒ 回调期间 `admission_context` 看不到重入帧，把嵌套 `Store::dispatch` 判为**外部根**（`wait=true`）；此刻 `draining` 已置 ⇒ `enqueue` 返回 `Queued` ⇒ 调用方在无界账本等待上**等自己 = 单线程确定性死锁**（不需要并发）。三条夺租约路径同形：`Store::dispatch` / `SyncStore::try_dispatch` / 框架 `ActionSender::dispatch`。
- **修复（维护者定稿：临时帧，不延迟投递事件）**：在首条记录的 `Enqueued` 回调**之前**安装临时 RAII 排空帧（携带 store 身份 + 回灌深度 + 账本）⇒ 回调内的同 store dispatch 归为**因果后代**（共享账本、深度 +1、`wait=false` 只入队即返）。帧随投递函数返回（含 panic 展开）弹出，**先于** `drain_owned` 装真帧 ⇒ 首条记录自身深度不被重复计。**装帧与闸判下沉到同一次无锁 load**（`audit_emit_prepared`）——否则两次 load 之间会重开「判未订阅 → 并发订阅 → 回调无帧」的新竞态；零订阅稳态仍零 TLS/零 `Arc`。
- **归属规则（已入文档）**：嵌套 action 的因果父 = **正在投递其 `Enqueued` 回调的首条记录**，不一定是夺得排空权的外部调用点。
- **测试**:新 `skeleton-infra/tests/audit_callback_reentrancy.rs`(5 例,**有界** harness 10s：死锁判红而非挂住 CI):三条夺租约路径各一例（回调内同 store dispatch 恰好一次）+ panic 展开仍 5s 内排空且帧已弹 + 控制例钉 `Enqueued`-before-`Reduced`。**红→绿**:修复前 `1 passed; 3 failed`（三例均 10s 超时判红，`EXIT=101`）→ 修复后 5/5 绿 `EXIT=0`（0.00s）。深度断言：环内 `Enqueued` 深度序列 `== [0, 1]`（漏帧 0、重复计 2 两种错法均红）。
- 公共面零改动（仅内部帧 + rustdoc）；无依赖/feature 变更。

### Added(bench): State 布局证据三组（#53）+ 补 `#[global_allocator]` 使既有两组预算闸真正生效,2026-11-23;GitHub issue#53)
- **新增三组**（32 臂，扩充既有 `infra` bench 目标，无新目标/无新依赖）：`state_layout_update/`（整体 vs 16 片 × 1 KiB/64 KiB/1 MiB/16 MiB × 有/无留存）、`state_layout_update_scope/`（选择性 vs 整体更新 + 分片粒度，含**四类反例臂**）、`state_layout_retention/`（陈旧长持 vs 滚动窗口的留存成本模型）。每臂附 `allocs/beat`·`frees/beat` 与留存字节读数。
- **反例必须存在**（定稿要求）：1 KiB 分片慢 2.0×／256 片慢 8.0×；1 MiB 全片更新优势从 90× 塌到 3×；单片分装与整体态持平。
- **发现并修复一处静默假绿**：`benches/infra.rs` 此前**没有** `#[global_allocator]` 声明 ⇒ 既有 `effect_steady_state` / `notify_dispatch` 的分配预算闸一直是 `0 - 0 == 0` 恒真（永不失败）。现补声明 + `assert_allocator_instrumented()` 前提闸（配表但未装表即红）。
- **副作用（需知）**：既有性能读数会因计数分配器（每块 +16B 头、每分配 2 次原子加）整体轻微偏移 ⇒ perf 基线须重取。
- **文档**：`docs/reference.md` runtime 锚段新增「状态布局建议（#53 证据）」——正向建议 + 三类反例 + 留存成本模型 + 计量口径局限（分配计数只见事件不见字节；≥1 MiB 跨 mmap 阈值每字节成本跳变）。

### Added(fx): 并发停机契约（RAII drain-owner 守卫 + 中断结果）,2026-11-23;GitHub issue#70)
- **问题面**:`shutdown`/`shutdown_timeout` 并发调用时,排空状态此前不是共享状态机——并发叫者会**先于排空完成**拿到一份全零 `ShutdownReport`;owner future 被弃置(select 竞败)时会**永久停在排空中**,等待者等于等一个永不产生的报告。
- **契约(三角色)**:① **owner** = 首个进入排空者,`Open→Draining` 在**取 JoinSet 之前**原子发布(两段无 await),由它决定策略并发布 **canonical 报告**;实现为 RAII 守卫,**每条退出路径(成功/超时/panic/取消)先结算共享谓词再通知等待者**,锁内只碰报告自有数据(不跑用户代码/用户析构)。② **等待者** 醒来拿同一份 canonical 报告;自身截止到期只按自身读数返回,**不得改写 owner 策略**,其报告**非 canonical**。③ **已结算** 再调用返回默认报告(顺序二次调用语义不变)。〔已被同日 F3 条目修订：现在返回该次排空的 canonical 报告副本，不再返回默认报告〕守卫被 drop ⇒ 发布**中断结果**(新公共字段 `ShutdownReport::drain_interrupted` + 中断时已知计数)再让 JoinSet drop 请求 abort,**绝不**停在 `Draining`。
- **不声称取消确认终止**:中断/超时都只请求 abort;任务守卫与 `in_flight` 在任务真正退出前具权威(与 #67 同一口径)。
- **测试**:新 `skeleton-infra/tests/fx_cancel_and_shutdown_owner.rs`(7 例,确定性握手禁 sleep,含用户析构重入探针):并发等待者共享 canonical 报告(修复前实得全零报告)/ 更短截止的等待者不改写 owner 策略 / owner future 被 drop 时所有等待者以文档化中断结果醒来 / 超时 owner 向等待者发布截止报告 / 既有唤醒与顺序二次调用回归。**红→绿**:修复前 6 FAILED / 1 passed。
- **公共面**:仅新增 `ShutdownReport::drain_interrupted` 字段(`#[non_exhaustive]` 内,非破坏)与既有方法的行为修复;无依赖/feature 变更。

### Fixed(fx): `CancelSource` 取消态在无接收者期间不再丢失（单向存储转换）,2026-11-23;GitHub issue#68)
- **缺陷**:`watch::Sender::send(true)` 在**没有接收者**时不写入值 ⇒ 无接收者窗口里的取消**静默丢失**,之后 `with_source` 接上的订阅者观察到 `false`,取消永不生效。
- **修复**:改用 `send_if_modified`(单向 `false→true`,即 CAS)——无接收者也把值存下、只在真实转换时唤醒当前接收者(重复 cancel 无冗余版本通知)、**watch 值是唯一真源**(不另设原子标志)。同时修发送侧 `is_cancelled()`(读存下的值,不再受接收者影响)与「send 失败无害」的误导性注释。
- **测试**:`fx_cancel_and_shutdown_owner.rs` 内 2 例(无接收者窗口后接订阅者：source 与 token 都必须见到 true;全部 clone 丢弃后取消仍留存且幂等)+ 1 例唤醒回归。修复前红。
- **兼容**:`cancel`/`cancelled`/`is_cancelled` 签名与语义零破坏(旧行为是无接收者时丢失——属 bug 不是承诺)。

### Added(runtime): `close_wait_with_report` 终态报告 + `discard_pending_effects` 显式丢弃,2026-11-23;GitHub issue#61)
- **契约口径（维护者拍板 B+C）**:legacy `close()` 保持「准入关闭 + best-effort 排空/交接」,**不是效果 flush**,**不承诺**终态 pending 效果快照。新增**一个**受 quiescence 闸控的操作 `SyncStore::close_wait_with_report(d)`:闭门 → 复用 #64 谓词等待（排空租约释放 + 活跃队列空）→ **仅在 `Drained` 之后**快照。
- **返回和类型 `CloseReportOutcome`**:`Drained(CloseReport)` / `TimedOutStillRunning` / `Reentrant`;**只有 `Drained` 携带快照**（超时/重入带快照会被调用方误当终态）。`Reentrant` 时准入**已关**（闭门先于重入判定）。静止窗复核失败宁报 `TimedOutStillRunning`,不给假快照。
- **四个 population 分列**（禁合并成一个 pending 计数）:`queued_actions`（`Drained` 恒 0）/ `deferred_actions`（#49 停放记录,**不影响 `Drained` 判定**）/ `pending_effects`（已提交未转发的效果后缀:`effects` 总条数 + `segments` 段级身份 `origin_action_seq`/`next_effect_index`/条数）/ `interrupted_effect`（最近一次中断:`dispatch_action_seq`/`origin_action_seq`/`effect_index`/种类/`EffectId`,与 #48 词汇同源）。
- **报告只读**：后缀在报告后仍归 store 所有、可重复报告检视（读数逐字相等）；**唯一解决面** = `discard_pending_effects()`（显式、返回精确条数与段身份;`None` = 未关闭或未静止,**什么都没丢**）。**close 与 discard 都不跑宿主 `EffectRuntime` 代码**（含 `Effect::Emit`:丢弃不回灌;载荷析构在锁外）。
- **明确不做**:post-close 转发（需独立 drain-to-idle continuation epoch 与回执归属,保留为独立规格）、one-effect pop、为 close 拥有的解决动作伪造普通 dispatch action seq。与 #64 共用同一谓词与 Condvar。
- **测试**:新 `skeleton-infra/tests/close_report_contract.rs`(6 例,确定性握手,禁 sleep):报告等待活跃排空并含其提交的后缀 / 超时不暴露假终态快照 / `Reentrant` 不声称终态且准入已关 / deferred 与后缀分列 / legacy close 不动后缀 / discard 报精确身份且**不跑效果代码**（`calls` 恒 1 + `MustNotRun` 哨兵）。新面为变异红验证（跳过等待→假快照、报告不放回→条数 0）。
- **公共面**:仅新增（5 类型 + 2 方法）,无签名/依赖/feature 变更;`skeleton` 契约 crate 仅 `Store::close` rustdoc 补「不是效果 flush」一句（行为零变化）。

### Fixed(runtime): 审计票据落闸与 sink 摘除同一临界区（陈旧票据不再关掉新订阅）,2026-11-23;GitHub issue#60)
- **缺陷**:`AuditTicket::drop` 原在**放锁之后**才 `gate.store(false)` ⇒ 并发 `subscribe_events` 可在窗口内装入新 sink 并开闸,随后旧票据的延迟 `false` 把**新订阅静默关掉**——闸 false 时发射在环入账**之前**就返回 ⇒ 实时回调与取证环**一起失声**。
- **修复（线性化点）**:摘槽 + `gate=false` 在**仍持审计 mutex** 时一次完成;`subscribe_events` 的装槽与开闸也移入**同一把 mutex** ⇒ 两侧串行,无第三种交错:落闸先则新订阅随后重新开闸;装槽先则本票据代次失配、根本不碰闸。**热发射路径不变**（未订阅 = 一次 Acquire gate load 即返）;代价只有关闭路径多持一拍锁。
- **边界分账**:本条是**订阅代次/闸的正确性竞态**;#65/#75 是**有效代次内的实时投递竞态**（其 FIFO 溢出有独立投递丢弃计数）——两者互不替代。
- **测试**:`audit.rs` 内 3 例白盒红→绿（修复前 3 FAILED / 2 passed）+ 公共面 `tests/audit_gate_generation.rs` 4 例（两种顺序、多代次只当前代可关流、重订阅重开闸;每条都验证**随后一个事件**同时证明环历史与当前代次回调活跃）。测试钩子只在 `cfg(test)`,**不扩公共面**。

### Fixed(fx): `TaskError::TimedOut` 语义更正为「截止已过 + 已请求 abort」（≠ 已确认终止）,2026-11-23;GitHub issue#67)
- **契约冻结（issue#67）**:`TaskError::TimedOut` 表示**调用方截止已过且 `AbortHandle::abort()` 已被请求**——**不表示**任务体已终止、其守卫已运行或 `JoinSet` 条目已被收割（tokio abort 是**取消请求**：正在执行非 yield 的 poll 的任务要等它交还运行时才被销毁）。维护者拍板**不加** per-task 完成回执（YAGNI：无真实消费方需要）；超时后的证据是 **scope 级观察面**（`in_flight` / `expect_body_idle` / `expect_fully_reaped_idle` / `counts()`）。
- **行为零变化**:`join_timeout` 函数体未动（仍 `abort()` + 立即 `Err(TimedOut)`），**只更正承诺**;enum 形状与签名未变。**唯一可观测变化** = `TaskError::TimedOut` 的 `Display` 文本:`"join timed out; task aborted"` → `"join timed out; abort requested (task termination not confirmed)"`（按旧字符串匹配的下游需同步;兄弟仓 agentwiki 是独立 vendored 快照且不匹配该字符串,需跑同步窗才带上新措辞）。
- **同符号文档扫**:`TaskError` 变体文档与 `Display`、`join_timeout` rustdoc、`Spawned::abort`、`ShutdownReport::failed`、`FxScope::shutdown_timeout`、fx.rs 单测注释、reference 文档 —— 统一采用既有的 `timed_out` / `still_running` 词汇;`ShutdownReport` 侧的 `TimedOut` 引用同步对齐。
- **测试**:新 `skeleton-infra/tests/fx_join_timeout_semantics.rs`(5 例,确定性握手 + 有界等待,禁 sleep)——① `TimedOut` 必须申报「已请求 abort」(修复前红:`join timed out; task aborted`)② 任务体停在非 yield 段时 `join_timeout` 返回 `TimedOut` 而 `scope.in_flight() > 0`（任务体仍在跑）③ 消费句柄后 scope **仍跟踪**该条目（`completed_unreaped == 1`,只有收割才归零)④ 截止前完成=返回值(既有语义)⑤ 首次 poll 前 abort=Cancelled(既有语义)。**红→绿**:修复前同一文件 `4 passed; 1 failed` ⇒ 真红;修复后 5 通过,连跑 10 次无抖动。

### Fixed(runtime): 审计实时回调不再丢并发发射的事件（回到 #65 主路线①）,2026-11-23;GitHub issue#65)
- **背景**:`SyncStore` 审计实时回调此前是 **best-effort 观测面**——并发发射窗内未取得回调权的事件只入环不进回调(不承诺每事件必达)。issue 给了两条路线:① 显式串行派发器(**主路线**) 与 ⑥ 把契约降级为 best-effort(**备选**)。**本仓实现的是主路线①**(备选路线未经拍板即被采用属违约,已纠正:issue 重开、路线重定)。
- **实现**:`AuditDispatch { gen, inflight, pending }` 挂在 `StoreInner` 上——**显式在途派发状态 + 有界 pending FIFO**(`AUDIT_PENDING_CAP = 1024`,沿用池域同款常量,未新增配置面)。emitter 发现回调在飞 ⇒ **入队而非跳过**;回调 owner 按 **`seq` 序**在**两把锁之外**排空 pending;**禁在审计 mutex 内调回调**、全程未用阻塞锁替代回调(同 store 重入会死锁)。锁序固定「环锁 → 派发锁」。
- **与环溢出分账**:环溢出丢**历史侧**(容量因,证据=`seq` 空洞);**投递丢弃计数**丢**实时侧**(待投递 FIFO 背压因)⇒ 新增只读面 `SyncStore::audit_delivery_drops()`(**落在实现层 `SyncStore`,契约 crate `skeleton/src/**` 零改动**)。两套证据互不为对方兜底。
- **两类抑制分别文档化**:并发投递=入队不丢;**同 store 重入**(`AUDIT_CB_IN_FLIGHT`)=只记环、**不进待投递队列**(不递归调用同一回调)。
- **panic fail-close 当前代次**:回调 panic ⇒ 计数 + 该代次 pending 作废并计入丢弃;订阅换代次时**旧代次队列作废**(排队事件**不投递给替换订阅**);被替换的旧槽**不覆盖新槽回调**。
- **测试**:新 `skeleton-infra/tests/audit_dispatch_delivery.rs`(5 例,确定性握手,禁 sleep):① 并发 emit 期间的事件**恰好补投一次**(旧行为下 `[1]` vs 期望 `[1,2]`)② pending **严格按 seq 升序**补投(变异 `pop_back` ⇒ `[1,4,3,2]` 乱序红)③ 回调 panic ⇒ 本代次排队事件作废并计入丢弃计数(旧行为 0 vs 期望 76)④ 同 store 重入**只录环不进回调**(回归护栏,前后皆绿)⑤ **FIFO 溢出计数与环溢出分账**(旧行为 0 vs 期望 76)。**变异验证**:emitter 改回「跳过」⇒ 4 条红;改 `pop_front`→`pop_back` ⇒ 顺序用例红 ⇒ 断言有鉴别力。
- **公共面**:仅新增 `SyncStore::audit_delivery_drops()` 一个只读方法;`skeleton` 契约 crate **零改动**;无字段/签名/依赖/feature 变更。

### Added(bench): std-ext bench 增 scheduler 基准组（注册/取消/取消+回收/pending 对照 @1k/10k/100k）+ 回收字节计量,2026-11-23;GitHub issue#81)
- **扩展现有 `std_ext` bench 目标**（不新增目标、不新增依赖、保留 criterion 0.5 与既有 cache/retry/random 组）；scheduler 是纯 std 恒可用域 ⇒ 该组**无 `required-features`**，`--no-default-features` 下亦可编译可跑。
- **12 个新用例**（4 类 × 1k/10k/100k）：`register/{size}`（注册吞吐）· `cancel/{size}`（取消吞吐，**远期 deadline** = 1h 且 MockClock 冻结 ⇒ 测量期内**零点火**）· `cancel_reclaim/{size}`（取消 + **走 #52 的阈值触发压缩路径**）· `pending_control/{size}`（远期但**未取消**的对照组 ⇒ 区分「堆构造成本」与「回收成本」）。每次迭代用**全新 scheduler**（`iter_batched` + `BatchSize::PerIteration`，注册在 setup **不计时**、取消/回收在计时段；重对象析构落在计时区段外）。
- **报告不止耗时**：ns/op + ops/s（中位）、被回收墓碑数、剩余 live 条目、峰值与回收后保留堆字节、**Drop 探针计数**（证明被取消的闭包确已释放）。
- **字节计量 = 进程级计数分配器**（每块 16B 头 + 区段开关 + 计量标记位 ⇒ 区段差值 = **净保留字节，非 RSS**）。**与 issue 字面的一处偏离**：issue 写「isolated allocator-accounting run（或专用 bench 可执行文件/进程）」，而「不得新增第二个 bench 目标 / 不得改 Cargo.toml」使专用可执行文件不可行 ⇒ 改为进程级 + 区段隔离，**局限已逐条声明**（跨区段累计漂移；非独立进程）。若维护者要求字面的独立进程计量，需放行一个专用 bench 目标。
- **实测**（本机，`--all-features`，19 个 ID 全出）：register 158~182 ns/op；**cancel 45~52 ns/op（与规模无关 ⇒ 取消 O(1) 快路径成立）**；cancel+reclaim 73~95 ns/op；pending 对照 tick 100/100/500 ns（无墓碑 ⇒ 阈值早退）。字节：基线 192 B，峰值（pending 保留）94.50 KiB / 1.11 MiB / 10.10 MiB ⇒ **取消后保留 == 峰值**（cancel 不摘堆，与既有语义一致）⇒ 清扫后 31.25 KiB / 312.50 KiB / 3.05 MiB（= 堆条目数组容量仍在，Entry 32 B/条），清扫释放 63.25 KiB / 824.50 KiB / 7.05 MiB；**drop 探针 = 1000 / 10000 / 100000**（取消后的捕获在 deadline 前确已释放），pending 对照 drop=0、live=N。既有 7 个组的计量区段净字节差 **全 0 B** ⇒ 其运行时热路径零堆分配（头部开销不在其路径上）。
- **⚠️ 读数影响**：计数分配器给每块加 16B 头 ⇒ **既有组的读数会有一次性漂移**，新基线须重取；绝对 ns 读数请在安静 runner 重取（本机并行编译时 100k 注册 15.3 ms → 36.6 ms，2.4× 摆动）。
- **未做**：独立进程级字节计量（见上）；字节读数未拆分「堆条目数组容量」与「捕获/任务胞」两类归属（只给总量 + 推算口径）；「堆条目数组容量不还给分配器」这一发现未改行为（是否 shrink 属维护者决策）。

### Fixed(ci): 部分 bench 运行不得替换滚动基线（基线提升完整性闸）,2026-11-23;GitHub issue#51)
- **缺陷**:perf workflow 的 bench 步用 `continue-on-error`（性能失败不拦合并,阈值只告警——此策略**不动**),但摘要脚本此前**只要采到任何一份新鲜 estimates 就把本轮写回基线** ⇒ 某个 bench 二进制中途失败(已产出部分 estimates)会用**部分基线替换完整基线**,后续运行随即丢失对比点而表面成功。
- **提升条件收紧为三条全过**:① **每个期望 bench target 都有新鲜估计**——期望集合从 `cargo metadata` 推导(manifest 声明的 bench target),**不是**「本轮采到了什么」(那正是让部分运行自称完整的原因);② **上一份 good 基线里的每个 benchmark key 都在本轮出现**——key 级判据:缺一个 key 而其余全新鲜 ⇒ 不提升,且 summary **点名缺哪个 key**;③ **任一 target 的条目数不得少于上次**(中途死掉的二进制会留下更少条目)。基线文件另存每 target 的条目数供下轮比对。
- **不提升时的行为**:`perf-baseline/summary.json` **逐字节保持不变**(上一份 good 基线经缓存原样存回),摘要标注「本轮不完整 → 未提升基线」,本轮测量仍作为**诊断产物**上传(`if: always()`)⇒ 部分数据绝不变成下一个对比点。
- **取舍(明示)**:维护者**删掉**某个 bench 组同样会降低条目数 ⇒ 被保守判为「疑似部分运行」而不提升(需人工清理基线里的该 key)。刻意取的一边:误判成本 = 基线暂不更新(可人工修),漏判成本 = 部分运行永久替换完整基线且丢失对比点(不可见)。
- **未按字面实现的一处**:issue 建议给 bench 步加稳定 step ID 并捕获其 outcome;本实现改用**期望集合推导**(issue 设计第 2 条的「显式期望 bench manifest」+ 上一份基线的 key 集合)作为等价信号——失败的 target 产不出估计 ⇒ 条件 ① 必然失败,提升闸的实质相同。
- **验证**:闸逻辑用合成运行测过 7 例(完整→提升 / 缺 target→不提升 / 条目缩水→不提升 / 缺单个 key→不提升且点名 / 首轮无基线→提升 / metadata 缺失→不提升 / 不完整时基线逐字节未变),全绿;workflow 文件解析通过。性能失败仍不拦合并、回归仍只告警、CPU 型号不匹配仍只记录——三项既有行为未变。

### Fixed(runtime): 票据注销后不再投递/回调 + try_subscribe 与 close 线性化,2026-11-23;GitHub issue#73/#74/#63)
- **#73 修复(NotifySink 注销后仍投递)**:异步投递的收集窗只做 `w.upgrade()` **不看存活标志** ⇒ 票据已注销的 sink 仍收到 publish。现**收集窗过滤已死槽** + **每次 publish 前复查 alive**;唯一真源沿用既有 `alive.swap(false, AcqRel)`(不新增第二套登记表,闸回落恰好一次语义未动)。**竞态边界按 issue 的窄契约**:通过了最后一次 alive 检查的 publish 视为 **in-flight**,允许在票据 Drop 返回后完成——此后**不再开始新投递**(严格 quiescence 需另立阻塞式协议,本框架不提供)。
- **#74 修复(同步订阅缓存注销后仍回调)**:跨线程同步订阅缓存持**强 `Arc<SubscriptionSlot>`**,票据 drop 后本次 dispatch 的缓存快照仍会回调该订阅。现给 `SubscriptionSlot` 增 **atomic alive 标志**(发布前初始化);票据 Drop 与 panic 摘除**先原子失活**(幂等)再解链 + `subs_version` 递增;**每次调用缓存回调前复查 active** ⇒ 缓存快照持强引用也不能在失活后开始新回调。panic fail-closed / subscriber 计数 / 缓存失效 / 稳态零锁快路径均未动。
- **#63 修复(try_subscribe 与 close 的 TOCTOU)**:原实现先 `is_closed()` 再 `subscribe()`(后者在闭门后**照常登记**并计数)⇒ 并发 close 可线性化在两者之间,产生「已登记但 store 已关」。现把订阅插入抽成**假定已持订阅门**的私有 helper;`try_subscribe` **持门**检查 closed → 返回 `Err(StoreClosed)` 或插入;`close`(与 `close_wait`)**持同一门**做 `closed` 翻转后**放门再排空/回调**(不在持门期间跑用户回调、丢用户闭包或排空队列);**legacy `subscribe` 语义不变**(闭门后仍登记 + 计入 `subscriptions_after_close`)。闸为每 store 一把小锁,**热 dispatch 路径零开销**。
- **测试**:新 `skeleton-infra/tests/ticket_removal_delivery.rs`(5 例:采集后未投递被跳过 / 在途投递可在 Drop 后完成 / panic 摘除与票 Drop 竞争闸恰好一次 / 缓存回调失活后不启动)与 `tests/subscribe_close_linearization.rs`(4 例:竞态下**绝不**出现「close 之后仍登记」/ close 后必 `Err` 且不增计数 / legacy 路径仍登记并计数 / close 排空期间不持订阅门)。**红→绿证据**:同一测试文件在 HEAD 上 FAILED(`left: 1, right: 0` 与 `left: 4, right: 0`)⇒ 真红。公共面零破坏(`skeleton` 契约 crate 未动)。

### Fixed(pool): 池审计实时回调不再丢并发发射的事件 + resize 不再双预留,2026-11-23;GitHub issue#75/#69)
- **#75 修复(实时流丢事件)**:`PoolAuditSink::emit` 原用 `slot.notify.take()` 把回调临时取走(锁外调用),此窗口内并发 emit 的事件**只入环、不投递**,首个 emitter 归还回调后**被跳过的事件不重放** ⇒ 实时回调流静默丢件。现引入**显式在途派发状态 + 待投递 FIFO**(`AuditSlot{ inflight, pending }`):回调在途期间的并发 emit **入队而非跳过**;在途者按 `seq` 序在**所有内部锁之外**补投;回调 panic 时 **fail-close 本代次**(摘除回调 + 清本代次 pending 并计数),**排队事件不投递给替换订阅**。三条约束保持:`FnMut` 回调串行 / 禁锁内调用 / 禁同线程递归(重入走 `record()` 只录环)。
- **#75 有界与分账**:待投递 FIFO **有界**(cap 1024),溢出丢最旧并计入**独立的投递丢弃计数** `PoolBase::audit_delivery_drops()` / `ObjectPool::audit_delivery_drops()`——与 ring 溢出(`dropped_overflow`)**分账**(历史侧丢 vs 实时流丢,两套证据互不替代)。
- **#69 修复(双预留致 `workers > max`)**:resize 的增长缺口原按 `core - (workers - reservations)` 计算 ⇒ **在途预留被忽略**,并发 resize 会重复预留同一缺口(实测 `workers=7 max=4`,与 issue 复现一致)。现缺口按**已承诺+已预留**总量计算;并引入 `target_epoch`/`next_epoch` **串行化目标更新**(**最后发起**的目标生效),旧目标预留的席位在兑现后按最新 core 重算退役令牌(`reconcile_shrink`)⇒ 不会被新目标静默满足两次。缩容令牌只从**可退休的真实 worker** 计、不得退到最新 core 以下;**OS 线程创建仍在状态锁外**,任务创建失败仍退还提交者。
- **测试**:新 `skeleton-infra/tests/pool_audit_concurrent_delivery.rs`(3 例,PoolBase/ObjectPool 并发不丢事件 + panic 代次隔离)与 `tests/threadpool_resize_reservation.rs`(4 例,覆盖 issue 验收四条:同目标不超限且精确收敛 / 不同目标收敛到最新 / 缩容与增长预留竞争不过冲 / 注入失败回滚无幽灵预留且可补齐)。**红→绿证据**:原码跑同一测试文件 ⇒ 实时流只剩 1 件(`left: 1, right: 2`)、`workers=7 max=4`。公共面零破坏(仅新增两个只读计数方法)。

### Fixed(cache): 共享缓存的 `K::clone` 移出 mutex(两阶段重验证插入),2026-11-23;GitHub issue#80)
- **修复**:`SyncLruCache::insert` / `SyncTtlCache::insert` 在持共享 core mutex 时执行 `k.clone()`(map 侧键副本)⇒ 用户 `K::clone` 重入同缓存即自死锁、panic 即投毒锁。现按**两阶段重验证插入**:① 持锁查存在 → 存在则按今日路径更新值与 recency 且**不 clone**;② 不存在则**放锁**、clone map 侧键副本(此刻无锁持有、未发生任何变更,clone panic 在此展开)→ ③ **重取锁再查**(第二次锁定检查 = 操作的**线性化点**);④ 仍不存在则以已 clone 的 map 键插入,锁内路径**永不调 `K::clone`**(core 拆出 `overwrite_if_present` / `insert_new` / `insert_with_map_key`)。⚠️ 本次仅闭合**插入路径 `K::clone`** 边界;共享面的键 `Hash`/`Eq`(#82)与读侧 `V::clone`(#83)**仍在内部 mutex 下执行**,见 #82/#83 条目。
- **保留现有可观察语义**:覆盖已有键**不调 clone**(不新增用户代码执行与 panic 面);单线程 `LruCache` 路径不变;TTL 校验与过期计算仍在锁前;「已有键重置过期且不 clone」保持;**TTL 路径的第二次存在检查早于过期清扫/淘汰** ⇒ panic 的 clone 不会造成部分变更。
- **#71 退休批规则保持**:被挤出的键/值与**未被采用的候选克隆键**(竞态落败 / 覆盖路径未采用)全部在放锁后析构,无析构回锁内。
- **测试**:新 `std-ext/tests/cache_clone_outside_lock.rs`(9 例)——修复前实测 **2 过 7 红**(3 例重入 `Timeout` = 锁内 clone 自死锁;4 例「内部锁已 poison」);含 TTL 未满/已满两种 panic 前置断言与并发首插同键只留一条记录。**变异验证**:注入「无条件预 clone」形状 ⇒ 覆盖不 clone 与并发首插两例双双 FAILED ⇒ 断言有鉴别力。公共面零破坏(无 pub 签名变化)。

### Added(shutdown): watchdog 执行数上限 + live/peak 可见性,2026-11-23;GitHub issue#54)
- **新增公共面**:`ShutdownCoordinator::with_max_watchdogs(usize)`(容量闸)、`live_watchdogs()` / `peak_watchdogs()`(诊断读数)。`OutcomeKind` / `ShutdownReport` / `ComponentOutcome` 公开形状**零改动**(容量闸不加破坏性枚举变体,按 issue owner 口径用 `Failed{reason}`)。
- **问题面**:带超时组件的闭包跑在 watchdog 线程上,超时=**放弃等待、线程照跑(detach)**⇒ 组件大量挂死时仍在跑的 watchdog 线程数随组件数**线性增长**,且无上限、无 live 可见性。
- **机制**:**许可 RAII**——占用发生在 **spawn 之前**,归还于**持有者消亡**(watchdog 线程退出/panic,或起线程失败时 std 丢弃闭包连带归还)⇒ 不靠手写每条分支。许可耗尽时该组件**未启动**(不 spawn 线程、不内联跑闭包),如实记 `Failed{reason: "watchdog capacity exhausted; component not started"}`——**非静默丢弃停机回调**。既有语义保持:超时≠终止(仍 `TimedOut`+detach)、执行用户闭包时不持协调器锁、被拒组件的捕获值也在锁外析构。
- **默认不限**(`None`)⇒ 既有用户行为零变化;**不盲设**有界默认值(同 #58 口径)。
- **测试**:新 `skeleton-infra/tests/shutdown_watchdog_bound.rs`(5 例)+ lib 内白盒记账单测。**变异验证**:容量闸差一(`>=`→`>`)⇒ 3 例 FAILED;删许可归还 ⇒ 4 例 FAILED ⇒ 断言有鉴别力。

### Docs(cache): 共享缓存锁边界契约显式化（键 trait 与读侧 `V::clone` 仍在锁内）,2026-11-23;GitHub issue#82/#83)
- **维护者拍板走「显式限制文档化」路线**,**不做部分重构**:`HashMap` 作权威索引 + 单 mutex ⇒ 键的 `Hash`/`Eq` 在锁内是**固有属性**(查表 `idx_of`→`HashMap::get`、建表项 `map.get`+`map.insert`、删除 `map.remove`、`HashMap` 扩容/rehash,以及借用键 `Q::Hash`/`Q::Eq`);真正的 lock-free 边界需另一种索引协议(快照候选键 + 锁外哈希 + 槽位代次重验证)⇒ **架构重设计,非局部补丁**。**用指纹替代键身份被否决**(指纹不是等价关系,当身份会静默改 `Eq` 语义;也不给通用 API 加应用级键限制)。
- **读路径 `V::clone` 亦在锁内**:`get`/`peek` 返 owned ⇒ `V::clone` 在内部 mutex 下执行。「锁外 clone + 重验证」**不可安全实现**(放锁后槽位可被替换/移除/淘汰/复用 ⇒ 锁外引用不安全;重取锁再 clone 又回锁内)⇒ 改为显式限制;`Arc<V>` 存储会改所有权模型(`remove`/`pop_lru` 需新增 `V: Clone` 或改返回类型)⇒ 属公共兼容性决策,留待未来大版本(记录为未来选项)。
- **契约文本**:两个共享缓存类型 + crate 级 cache 契约写明键/值实现须**快、不 panic、不得重入同一缓存**;与 #71 的精确对照并列(**#71 移出锁的是退休值析构;读侧 Clone 仍在锁内**);`remove`/`pop_lru` 仍移动出 owned `V`、**不需要 `V: Clone`**(既有 API 属性未变);单线程 `LruCache`/`TtlCache` 面区分(无共享 mutex,但 `Hash`/`Eq` 仍在调用方栈上同步执行)。
- **措辞收窄**:仓内「用户代码都在锁外」类宽泛断言逐处收窄——#71 条改为「就退休面而言临界区内只做簿记与所有权转移」,#80 条追加「⚠️ 本条只覆盖插入路径 `K::clone`」,poison 条追加「临界区内用户 trait panic 同样投毒 ⇒ 键/值实现须 panic-free」。
- **探针**:私有 `try_lock` 单元测试(用户 `Hash`/`Eq`/`Clone` 内部尝试取同一 mutex ⇒ 断言恰为 `WouldBlock`;确定性、非阻塞、无子进程、不会挂死测试进程)——它是「限制确实存在」的可执行证据,且若将来把用户代码移出锁,探针会翻红成为变更探测器。**变异验证**:把探针目标改为无争用的独立 mutex ⇒ 恰好 6 个 LRU 侧探针 FAILED ⇒ 断言有鉴别力、非恒真。
- **无行为/API 变更**:`pub` 签名逐字未变,`Cargo.toml`/`Cargo.lock` 未动。

### Fixed(cache): 淘汰/过期/摘除的值与键在共享缓存锁**外**析构,2026-11-23;GitHub issue#71)
- **修复**:`SyncLruCache` / `SyncTtlCache` 在持共享 mutex 时执行用户析构——被淘汰/过期/覆盖的 `V::drop`,以及 `LruCore::remove` / `pop_lru` / `retain` 移除 map 条目或取走 arena 键时的 `K::drop`,全部跑在临界区内(用户 `Drop` 可能很慢、可能重入、可能 panic)。现改为**退休批**:就**退休面**而言临界区内只做簿记与所有权转移(用 `HashMap::remove_entry` 取回 map 键,连同 arena 键/值一起装进 `Retired<K,V>`),**解锁后**按显式顺序(map 键 → arena 键 → 值)析构。覆盖旧值用 `Option::replace` 取走;淘汰全量退休;过期回收值入批。四路(覆盖/淘汰/过期/摘除)一致。⚠️ 本条**只覆盖析构边界**——临界区内仍执行键的 `Hash`/`Eq` 与读侧 `V::clone`(见 #82/#83 条目)。
- **poison 契约拆分**:锁外析构 panic **不再投毒**缓存(§71 验收口径);**临界区内** panic 仍投毒并 fail-fast 带因——既有 `poisoned_lock_fails_visible` 按新契约改写为两段(析构 panic 不投毒 + 锁内 panic 仍 fail-visible)。
- **零分配锚不破**:退休批保键+值所有权至解锁后,但**不新增堆分配**(既有 `alloc_anchors` 的「覆盖插入不得分配」锚仍绿)。容量/TTL/LRU/计数语义与公共 API **零破坏**(HEAD 与新版 52 个 `pub fn` 签名逐字相同)。
- **测试**(新 `std-ext/tests/cache_drop_outside_lock.rs`,11 条):修复前实测 **1 过 10 红**(其中 5 条以超时判红 = 锁内析构自死锁)⇒ 真红且鉴别力强;覆盖值侧四路 + 键侧 remove/pop_lru/retain + 探针在用户 `Drop` 内重入同一缓存并与旁线程握手取锁(通道等待,禁 sleep)。
- **未覆盖(已另立票)**:`LruCore::insert` 的 map 键副本 `k.clone()` 仍在锁内(用户 `K::clone` 重入同样自锁死)——见 GitHub issue#80 / bd `udf-frame-j1c`。

### Added(scheduler): 有界准入——容量上限 + typed `Full` 拒绝,2026-11-23;GitHub issue#58)
- **新增公共面**:`Scheduler::set_capacity(usize)` / `capacity()`(0 = 无界,同 `SyncStore::set_capacity` 口径)、`try_after` / `try_every -> Result<TaskHandle, AdmissionError>`、`AdmissionError{Closed, Full}`、`live_registrations()` / `rejected_registrations()`。既有 `after` / `every` 签名**不变**——issue 第 5 条授权的两个选项中取「**加法 typed 面 + 既有方法触顶 fail-visible**」:触顶时 `after`/`every` **panic**(fail-visible,非静默丢回调;返回 `Err(Closed)` 会撒谎)。
- **默认零影响**:`capacity` 默认 0(无界)⇒ 既有行为与既有测试完全不变;**不预设**有界默认值(#56 第二消费方验证前不盲设)。
- **计数口径**:活跃注册数 = 堆内待执行条目 **+ 执行中周期任务**(仍可能重臂)——**不等同 `heap.len()`**(墓碑不计入)。额度**恰好一次**释放:一次性点火终态 / 周期任务取消·关闸·panic 四条退役路径共用单一 CAS 闸门(赢家释放)。
- **取消仍 O(1)**:不扫堆;容量检查与计数同在状态锁内 ⇒ 并发注册不越上界;被拒回调**不入堆**(不部分插入),其捕获在堆锁外析构。
- **测试**(新 `std-ext/tests/scheduler_bounded_admission.rs`,10 条):容量 N 恰收 N 条且下一条 typed `Full` 无部分插入 / 一次性点火释放额度 / 周期任务重臂保留额度 / 周期任务执行中被取消恰好释放一次且不再重臂 / 关闸释放堆内全部注册 / 并发注册·取消·tick·关闸竞争下计数精确且不越上界 / 被拒捕获在堆锁外析构 / legacy 触顶 panic。**变异验证**:去掉容量闸 ⇒ 6 个 #58 用例红;停用清扫 ⇒ 恰好墓碑回收用例红 ⇒ 断言有鉴别力。
- **未做**:benchmark 条款(1k/10k/100k 注册与取消吞吐)——std-ext 无 `[[bench]]` 目标且新增需改 `Cargo.toml`,已单开 bd 票跟踪。

### Fixed(scheduler): 已取消的远期条目在原 deadline 之前被回收,2026-11-23;GitHub issue#52)
- **修复**:调度器此前对取消只用惰性墓碑(堆条目到原定 deadline 弹顶时才摘除)⇒ 取消密集 + 远期 `after` 的负载会长期占住已取消的 task cell 与其捕获闭包。现新增**独立触发**的清扫(`reclaim_cancelled`):取消路径**仍 O(1)、零锁、零扫堆**(弱引用记账 + 墓碑计数),清扫按**绝对下限 16 / 比例阈值 4** 在状态锁内重建堆只留 PENDING 条目(O(n) heapify),被摘条目**移出锁外**析构。
- **驱动唤醒精度**:取消 earliest-deadline 时通知驱动并**重算 park 锚**——驱动不再为堆顶墓碑多醒一拍;`next_deadline` 同样在锁外析构被剪条目。
- **执行语义零变化**:已出堆检查-执行窗口内落地的取消**仍不可撤、仍照跑**;`tick`/重臂/关闸/取消的线性化语义未改。墓碑计数是近似量(清扫期间落地的那次取消会漂移,良性——额度上限由 CAS 闸门精确管理)。
- **测试**(同上文件):已取消的远期条目必在原 deadline 之前被回收(实现前实测 **0/48** 被回收 ⇒ 真红;实现后全绿,连跑 3 次无抖动)。

### Fixed(docs): 架构蓝图 ADR 治理口径 + README 快起完成边界 + bench feature 表对齐,2026-11-23;GitHub issue#50)
- **ADR 治理口径过期**(蓝图 §2):此前写「有意把 `docs/adr/` 排除在 Git 之外、决策副本仅存本地」——实际 `docs/adr/ADR-0001..0010` 与发布清单**已入库**,是规范且版本化的决策记录 ⇒ 蓝图改为「`docs/adr/` 已在库内,ADR 为权威记录,本蓝图表格是索引而非替代」。
- **README 快起把 per-action 完成写成全局 idle**(根 `README.md`、`skeleton/README.md`、`skeleton-infra/examples/quickstart.rs` 三处逐字同步):「dispatch 返回即 idle」改为「**dispatch 返回 = 本 action 的完成边界**(状态提交 → 效果转发 → 通知返回)且其因果后代终局;**不承诺全局队列为空**」——并发生产者下队列可能仍非空是正常态,不是未排空证据。
- **bench feature 表漏列**:`docs/reference.md` 两处(必选组合段 + 速查表)写 bench `infra` = `tokio+journal+memgov+testing`,而 `skeleton-infra/Cargo.toml` 的 `[[bench]]` 声明含 `pool` ⇒ 两处补齐 `pool`(照声明组合编译 bench 才不失败;CI 的 `--all-features` 曾掩盖该缺口)。
- **全仓复查 stale 措辞**:ADR 排除类仅蓝图一处(已改);「返回即 idle」剩余命中分两类——**框架提供 sender 的静默态**与**排空环内部路径**(字面准确,保留)与测试断言消息(已统一为完成边界口径)。代码与测试行为零变化。

### Fixed(scheduler): `TaskHandle::detach` 不再泄漏 TaskCell + 丢弃 `WorkerHandle` 即关闸,2026-11-23;GitHub issue#55/#72)
- **#55 修复(泄漏)**:`TaskHandle::detach` 原用 `mem::forget(self)` 抑制 Drop-cancel,**连带永久泄漏 `Arc<TaskCell>`**——调度器移除堆条目后泄漏引用仍保活 TaskCell 分配(周期任务的回调与捕获图随之永久留存)。现**取消语义与所有权释放解耦**:新增私有 `armed` 标志,detach 只置 `armed=false`(Drop 不再 cancel),`Arc<TaskCell>` 照常释放。公共面零破坏(`detach`/`cancel` 签名不变,调用方语义不变)。
- **#72 修复(线程+调度器双泄漏)**:丢弃 `WorkerHandle` 而不 `join` 时,`worker_loop` 持 `Arc<Scheduler>` ⇒ 驱动线程永远 park 在 condvar 上,且**再无公开句柄能关闭它**。现 `WorkerHandle` 增 `Drop`:**drop = 关闸不 join**——关闸唤醒驱动线程退出;**不 join**(句柄可能在 worker 自身回调内被 drop ⇒ 自等死锁),因此该路径**不确认线程终止、不传播 worker panic**(仅 `join` 确认终止并原样重抛 panic)。**行为变更**:丢弃句柄后调度器已关闸,后续注册返回 `Err(Closed)`。〔2026-11-23 设计对照修正：维护者 03:59 定稿为 **per-worker 停信标**，本「全局关闸」形态已废止——见同日「设计对照整改」条目〕
- **实现**:`WorkerHandle.thread` 改 `Option<JoinHandle<()>>`(类型有 `Drop` impl 时禁止移出字段),`join(mut self)` + `take()` 移出;`close()` 幂等(join 与 drop 两条路径各触发一次,无副作用叠加)。
- **测试**(新 `std-ext/tests/scheduler_handle_lifecycle.rs`,8 条):detach 后 TaskCell 被回收(`Weak::upgrade` 由 `Some` 转 `None`,断言非恒真)/ 周期任务捕获不滞留 / 丢句柄即关闸 / 回调内丢句柄不自等死锁 / 在途回调中丢句柄仍停线程 / 显式 join 仍关闸并等退出。既有 23 条 scheduler 回归全绿;公共面零破坏(无 pub 签名变化、零新依赖、`Cargo.toml`/`Cargo.lock` 零 diff)。

### Added(显式停机等待面 `close_wait` / `close_wait_timeout`,2026-11-23;GitHub issue#64)
- **新增公共面**：`SyncStore::close_wait() -> CloseWaitOutcome` 与 `close_wait_timeout(Duration) -> CloseWaitOutcome`（`Drained` / `TimedOutStillRunning` / `Reentrant`）。`close()` 语义**不变**（准入关闭 + best-effort 排空，静默态同步排至 idle、竞争态仅交接即返）——只是其 rustdoc 改为**不承诺 quiescence** 并指向等待面（此前文档高估了竞争路径的保证）。
- **语义**：等待谓词 = 排空租约已释放 且 活跃队列已空（**不计** #49 deferred 停放记录——非活跃排空工作）。timeout **只界调用方等待**，不取消同步 reducer、不声称已停。同栈重入（本 store 排空帧内调用）立即返回 `Reentrant`，不自等。〔2026-11-23 设计对照修正：**闭门先于重入判定**——`Reentrant` 返回时准入已关，外部 `try_dispatch` 得 `Closed`（原实现先返后闭，与维护者 06:59 定稿相反）〕
- **实现**：`Condvar` 与 `queue` 锁配对；排空租约释放的两个点（正常出口 + panic unwind 臂）各 `notify_all` 一次，无轮询旗。测试 4 条（阻塞至 gated reducer 退出 / timeout typed 且不取消 / 重入 / panic unwind 后可达 Drained）；legacy 兼容由既有 drain 回归面覆盖。

### Performance（`commit` 路径零搬移段化——逐条包装的载荷线性成本消除（bd j9q），2026-11-20）
- **变更**：已提交待转发队列由 `VecDeque<PendingEffect>`（逐条携带来源元数据）改为 `VecDeque<PendingSegment>`（**按 action 分段**：段 = `origin_action_seq` + `next_index` + 段内 `EffectBuf`）。提交点恢复 **`mem::swap` O(1) 三指针整体移交**（#48 时期待的方案：#48 的来源元数据收进**段级**，段内偏移即 `effect_index`——一个 action 恒提交一段、输出序从 0 连续 ⇒ 逐条 `(origin, index)` 语义零变化，审计事件/失败回执/探针全部同值）；转发环段内逐条借出、**段壳只在段空时出队** ⇒ panic 时未转发后缀原地留段、段原地留队首，续转从断点继续编号（禁吞语义与逐条版逐字节等价）；段空摘壳时满缓冲 `swap` 回 `staged`（缓冲在 staged ↔ 段之间循环复用 ⇒ **稳态零新增分配不破锚**）。
- **实测**（外部 crate 视角载荷探针、独占机器、同口径三方对比、median ns/effect、点估计）：**P=512** 125.86 ⇒ **76.02**（−39.6%）· **P=4096** 337.89 ⇒ **220.89**（−34.6%）· P=0 / P=64 回落基线水平（53.84 / 55.38）——#48 Changed 段登记的「已知代价（逐条包装 +50%）」**已消除**，回到 swap 时代基线。O(n·sizeof(Op)) ⇒ O(1)。
- **公共面零变更**（`PendingSegment` 为 infra 私有；`EffectBuf`/`Effect`/审计事件 shape 全部不动）；门禁：五门全 exit 0（串行全量 0 FAILED，`memgov` 零分配锚全绿）。证据：`verify/2026-11-20-j9q/`（三方探针日志 + 五门日志）。

### Changed（**破坏性**：`EffectInterrupted` 事件携带结构化效果身份 + 新增 `EffectKind`，2026-11-20；GitHub issue#48）
- **破坏性变更**：`skeleton_infra::audit::AuditEventKind::EffectInterrupted` 由**单元变体**改为**结构变体**，携带五个字段：
  - `dispatch_action_seq: u64`——**本次触发转发、并承接失败回执**的 action 的**入队序号**；
  - `origin_action_seq: u64`——**实际产出该效果**的 action 的入队序号（可能**早于**前者：pending 效果跨 action 存活——A 产出的后缀在 A 的转发 panic 后留存，后来由 B 的 dispatch 触发续转并再次 panic ⇒ 来源是 A、触发者是 B）；
  - `effect_index: u64`——该效果在**产出它的 action** 的 reducer 输出序列中的 **0-based** 位置（计数**包含** `Emit`——Emit 也占位）；
  - `effect_kind: skeleton::EffectKind`——效果的解释器侧种类；
  - `effect_id: Option<skeleton::EffectId>`——效果的逻辑句柄（当前 `Run`/`Cancel` 恒 `Some`）。

  **迁移**：下游对 `AuditEventKind` 做 match 的代码，`EffectInterrupted` 分支须改为解构形（`AuditEventKind::EffectInterrupted { .. }` 或逐字段绑定）；原先按「等值比较该变体」的写法（如 `e.kind == AuditEventKind::EffectInterrupted`）须改为 `matches!(e.kind, AuditEventKind::EffectInterrupted { .. })`。**无旧形状 shim**（0.1.0 首发准备态 ⇒ 未发布前破坏性直迁）。
  - **`PartialEq` 语义变化（独立于上面的 match 迁移）**：`AuditEventKind` 派生的 `PartialEq` 因该变体由**单元变体**改为**结构变体**而**语义改变**——原先两个 `EffectInterrupted` 事件彼此恒等（单元变体无字段可比），现在**逐字段比较**（五字段全同才等值）⇒ 下游若用 `==` 比较事件类型，其行为会变（原先恒真的相等断言现在可能为假）⇒ 须改用 `matches!` 判别种类。
- **新增公共面**：`skeleton::EffectKind`（`#[non_exhaustive]` 枚举，`Run` / `Cancel`）与 `skeleton::EffectKind::of(&Effect) -> Option<(EffectKind, Option<EffectId>)>`——后者是**效果 → (种类, 句柄) 的单一映射点**：匹配在 `skeleton` crate 内对 `Effect` 词表**穷尽** ⇒ 词表演进时编译期强制同步，下游免于各自补通配语义。**`Effect`/`EffectId`/`Reducer`/`EffectRuntime`/`Store`/`ActionSender` 的 trait 与签名零变更**；`Cargo.toml`/`Cargo.lock` 零 diff（无新依赖）。
- **实现要点**：来源元数据在**提交点固化**（`commit()` 时写入产出该效果的 action 序号与 ordinal）——**不可在 panic 时反推**（panic 时的「当前 drain 的 action」未必是效果的产出者）。已提交待转发队列的元素由裸 `Effect` 改为携带来源记录的内部结构（容器仍为 `VecDeque`——摘队首零搬移的 #43 优化不回退）。
- **已知代价（如实登记 · 两档口径）**：新 `commit()` **无法再走 `mem::swap` 的 O(1) 整体移交**——`staged` 的类型被公共 `Reducer::reduce` 签名钉死为 `EffectBuf`，而 `pending` 的元素是携带来源元数据的私有 `PendingEffect` ⇒ 两者类型不同、不可交换 ⇒ 改为**逐条包装**（n = 本 action 产出的效果数，典型 0~2）。**① 零载荷档**（`Op` 载荷 ≈ 0）：`effect_steady_state` 中位数 364.92 ns ⇒ 374.91 ns（**+10.0 ns / +2.7%**，区间大幅重叠 ⇒ 统计不显著）；**② 带载荷档**（逐条包装的搬移成本随 `Op` 大小**线性放大**，零载荷 bench 完全看不到——2026-11-20 第三轮对抗性审计 P2-1 补全）：自跑外部 crate 视角探针（同机同口径、基线 = #48 改动前的 `mem::swap` 树、5 轮交替取中位、n=32 条效果/次）——⚠️ 以下为**点估计**（5 轮交替取中位、**未做显著性检验**；4096 B 档单轮抖动大）：**0 B** 37.5 ⇒ 40.6 ns/效果（**+8.3%**）· **64 B** 40.6 ⇒ 43.8 ns/效果（**+7.9%**）· **512 B** 56.2 ⇒ 84.4 ns/效果（**+50.2%**）· **4096 B** 187.5 ⇒ 284.4 ns/效果（**+51.7%**）。⚠️ **另一套口径**（2026-11-20 第三轮对抗性审计报告）报**同一趋势**但**绝对值不同**（4096 B：181.2 ⇒ 415.6 ns/效果，**+129%**）⇒ 两套口径的**绝对值差异未查清**（基线树选择 / 探针参数 / 机器负载均可能）⇒ **本段不声称单一权威百分比**，**以「代价随载荷线性放大、零载荷档的「统计不显著」不可外推到带载荷场景」为结论**。**影响面**：单 action 产出**多条效果**且**单条 `Op` 载荷较大**（≥ 数百 B）的场景（典型：大载荷 blob 效果的批量转发）。**分配面不回归**（逐条包装**不新增**分配——两侧容器各自复用容量；探针实测每 action 分配恒 0.035，基线与新版同值）——代价是**搬移**（copy）而非分配。**升级触发条件**：若实测生产场景出现「单 action 效果数 > 10 条 **且** 单条 `Op` 载荷 > 4 KB」的组合 ⇒ 评估改为**按 commit 段整体移交**（让 `pending` 与 `staged` 共用同型容器，恢复 O(1) 移交）；**本批不做性能改造**（那是新能力，须另开票）。
- **`EffectId` ≠ 投递身份**：`EffectId` 是 reducer/消费者定义的**逻辑效果句柄**（**一个 store 内全局唯一**——见 `skeleton::EffectId` 的唯一性纪律），**不是**框架生成的投递尝试身份。**本批不引入重试**、**不承诺** exactly-once；**#45 拍板的 Option D 语义（至多一次调用尝试 · 外部结果不确定 · 不自动重放）保持不变**，幂等/去重属消费方责任。
- **不选 C/D**：效果明细**不进回执**（回执仍只答「当前 dispatch 是否完成 / 在哪个阶段失败」，`Failure.seq` 仍是**动作**序号）；**不新建查询面**（无历史索引/保留期/快照语义）。
- 测试：新增 `skeleton-infra/tests/effect_interrupted_identity.rs`（7 用例：kind/id/ordinal 逐条可辨 · **跨 action 续转归因**（来源 = 产出者、触发者更晚、且 `origin < dispatch`）· **失败回执的 seq 与事件 `dispatch_action_seq` 逐字相等** · `Emit` 置前 ordinal 不错位 · `Cancel` 中断携带其目标 `EffectId` · tracing 桥五字段与事件同值 · 环形缓冲溢出/订阅关闭既有语义回归）；既有 `tests/effect_forward_panic.rs` 的等值比较迁移为 `matches!`。
- 文档：`docs/reference.md` 的 audit 章节同步登记事件 shape、五个字段语义、两个序号的口径（**入队序号**而非调用时刻序）与新增公共面（`EffectKind` + `of()`）。

### Fixed（第四轮两份独立复审 2 条 P2 + 已登记限制的全量反查，2026-11-20；第四轮主审与对抗审计均 CONDITIONAL——11/11 前轮 findings 闭合确认，仅余本轮 2 条新 P2）
- **F-1/F1「FIFO 序 = 序号序」残留（两份审计独立命中同处）**：`skeleton-infra/src/audit.rs` 的公共面 rustdoc 仍写无条件「入队线性化点分配——FIFO 序 = 序号序」「跨 action 续转时恒 `origin < dispatch`」，与本批自己在 `docs/reference.md` 登记的「已知张力」（**不保证**）直接矛盾——前轮 A4 的闭合只把张力登记进了 reference.md，漏了下游 `cargo doc` 实际读到的面 ⇒ 改为条件化（与 reference 逐字语义一致）。**同型残留一并清理**：`runtime.rs` 的 `next_seq` 字段文档与 `ActionRecord` 文档同样写「queue 锁内分配——FIFO 序 = 序号序」，而 `Effect::Emit` 回灌路径的 seq 在取锁**之前**分配 ⇒ 同步更正为「外部入队路径 queue 锁内分配、Emit 回灌路径取锁前分配 ⇒ 序号序与全局 FIFO 序不保证恒等（已知张力）」。
- **F2「三元组唯一标识一个效果」被确定性证伪**：外部 crate 视角探针（3 跑一致）——两条**不同**的 `Cancel` 效果指向**同一**目标句柄 ⇒ `(kind=Cancel, id=Some(20), index=0)` × 2 **三元组完全相同**，且 `cancel_unknown_id=0` ⇒ 合法时序非误用 ⇒ 根因是三元组**不含 action 序号** ⇒ 只在同一产出 action 内唯一；**跨 action** 唯一标识是 `(origin_action_seq, effect_index)` ⇒ 三处同源文本（`skeleton/src/lib.rs` 的 `EffectKind` 文档 · `skeleton-infra/src/audit.rs` 的 `effect_id` 字段文档 · `docs/reference.md` 字段表）同步改为**带作用域**口径。
- **已登记限制的全量反查（本波新增流程动作，堵「登记了却在别处写成保证」的洞）**：把本批登记的每一条「已知张力 / 已知限制 / 已知重复 / 演进约束 / 口径差异」逐条反查仓内残留——R1 序号序（**2 处真残留已改**，见上）/ R2 `EffectKind::of` 的 `None` 语义（无残留）/ R3 Cancel 寻址校验统一性（无残留）/ R4 代价口径（点估计 + 两套口径差异未查清已登记，无残留）/ R5 `EffectId` 复用（零命中）/ R6 `effect_id` 与三元组身份（F2 已改）/ R7 reduce 时机（15 处命中全为带编排限定 / 双向可能 / 否定式语境，无残留）。
- **本批口径**：**运行行为零变更**（纯 rustdoc / 注释 / CHANGELOG / reference.md）；**未新增任何公共 API**、**未新增依赖**（`Cargo.toml`/`Cargo.lock` 零 diff）。

### Fixed（契约表述精度：竞争态「reduce 时机不作保证」+ 两条回归闸鉴别力，2026-11-20；第三轮对抗性审计 P1-1 / P2-2 / P2-1 / P2-3 / P3-1 / P3-2 / P3-3）
- **P1-1 契约表述更正（把竞态窗口内的读数写成无条件事实）**：`skeleton::ActionSender` 类型文档 / `ActionSender::dispatch` / `ActionSender::new` 的返回值段、`skeleton_infra::runtime::SyncStore::try_dispatch` 的 rustdoc、`docs/reference.md` runtime 章节、以及实现侧 `back_sender` 两个分支的注释，此前把「竞争态下该 action 一定**未被 reduce**」「该 action 一定由在任排空者**稍后处理**」「静默态与 `Store::dispatch` **完全同形**」写成**无条件事实**——外部 crate 视角编排式探针**确定性证伪**（真竞争态 8/8 轮次中该 action 在 `dispatch` **返回之前**就已被他人 reduce；竞速版 50 000 轮次中 9 770 次真竞争态样本在返回瞬间已被 reduce、39 880 次尚未 ⇒ **两个方向都发生**）⇒ 契约改为**条件性表述**：**竞争态** = 纯准入（**只保证已入队、不等完成边界**；**返回瞬间是否已被 reduce，框架不作任何保证**——取决于在任排空者的推进速度 ⇒ 下游**不得**据「竞争态」推断「此刻读状态仍是旧值」）；**静默态** = 夺得排空权并同步排空至 idle ⇒ 返回时该 action **已被 reduce、完成边界必然已达成**（此路径**强于** `Store::dispatch` 的 per-action 完成边界契约——后者**不**承诺队列为空）。**`bool` 只承载准入结果**（被接受/已入队 = `true`，被拒 = `false`），**不承载完成状态**。**实现零变更**（纯文档/注释）。
- **P1-1 反向机械闸（新增）**：`skeleton-infra/tests/drain_regressions.rs::competitive_state_does_not_guarantee_reduction_timing`——编排**真竞争态**（排空线程在 reducer 内持闸 ⇒ 排空权在它手里 ⇒ 发送方必走 `Enqueue::Queued` 分支），断言**只**针对框架**无条件保证**的事实：① 该 action **最终被 reduce 且恰好一次**（`Fence` 收尾屏障 ⇒ 观测无竞态）；② 竞争态下 sender 是**纯准入**（有界超时内返回，退化为「同步等待完成边界」即与在任排空者互等 ⇒ 死锁形态）。**刻意不断言**「返回瞬间是否已被 reduce」——那是竞态窗口内的读数，断言它等于把竞态写成契约。确定性编排（**禁 sleep**）+ 有界超时（`recv_timeout` 10s ⇒ 失败是红不是挂死）。**变异验证**：把 `back_sender` 的 `Enqueue::Queued` 分支改为「同步等待完成边界」⇒ 闸在 10s 后**超时判红**（基线绿 / 变异红 / 还原绿）。
- **P2-2 假绿闸强化**：`skeleton-infra/tests/effect_interrupted_identity.rs::audit_ring_overflow_and_subscription_close_semantics_unchanged` 此前断言为 `<= 2` + 空流 + `is_empty()` ⇒ **恒真**（在「审计事件全面失能」下仍绿）⇒ 改为**确证式**断言：① 订阅回调必须见到**多于环容量**的事件数（否则溢出未被制造，判据退化）；② 环内容必须等于回调所见序列的**最后 N 条**（钉住「溢出丢最旧、留最新」）；③ 环内首条 `seq` 必须**靠后**（丢最旧的显式证据 ⇒ `seq` 留空洞）；④ 票据 Drop 后**回调不再被调用**（差分计数 ⇒ 「从未记录」与「关闭后不再记录」可分辨）。**变异验证**：「审计事件全面失能」⇒ 闸在「回调见到 0 条」处判红；「溢出丢最新（保留最早）」⇒ 闸在「环内留存的必须是最新两条」处判红（`left: [1, 12]` vs `right: [11, 12]`）。
- **P2-3 已知张力登记（不改实现）**：`skeleton-infra/src/audit.rs` 声称审计事件的 FIFO 序 = 序号序，而 **`Effect::Emit` 回灌路径的序号在取锁之前分配** ⇒ 理论上可与并发入队的序号交错 ⇒ **登记为已知张力**（探针 118 万事件**未复现**逆序 ⇒ 不是已证实缺陷）。登记位置：`docs/reference.md` audit 章节「两个序号的分配纪律」处。
- **P3-1 已知限制登记（不改实现）**：`skeleton::EffectKind::of` 的穷尽匹配保证的是「**覆盖 `Effect` 词表的所有变体**」，**不**保证「所有非 `Emit` 变体都必经解释器」⇒ 若将来新增一个**不经解释器**的效果种类，实现侧那条「非 `Emit` 效果必经解释器」的 `expect` 不变式将在**运行时 panic**（编译器**不会**报错）⇒ **新增不经解释器的效果种类时，实现侧必须同步处理 `of` 返回 `None` 的分支，不得依赖 `expect`**。登记位置：`skeleton::EffectKind::of` 的 rustdoc + `docs/reference.md`。
- **P3-2 已知重复（本批不改）**：`forward_effects` 对 `Effect::Run` / `Effect::Cancel` 的**寻址校验**是手工 `match`，与 `EffectKind::of` 的映射重复 ⇒ **本批不改**：重复面小，且改动会碰**热路径**（效果转发）⇒ 收益不足以抵消回退风险。
- **P3-3 `PartialEq` 语义变化登记**：见上方 #48 迁移段的单列条目。
- **本批口径**：**运行行为零变更**（纯 rustdoc / 注释 / 测试 / CHANGELOG / reference.md）；**未新增任何公共 API**、**未新增依赖**（`Cargo.toml`/`Cargo.lock` 零 diff）。

### Fixed（第三轮主审 4 条 findings 整改：文档表述精度 + 一条假绿闸，2026-11-20）
- **P1 假绿闸修正**：`skeleton-infra/tests/effect_interrupted_identity.rs::tracing_bridge_reports_same_identity_fields_as_audit_event` 此前唯一场景是**单 action** ⇒ `origin_action_seq` 与 `dispatch_action_seq` **同值** ⇒ 桥里对调两字段**不可见** ⇒ 该闸**无鉴别力**（第三轮主审实证：变异「桥里对调两字段」下旧闸仍 exit 0）⇒ 场景改为**跨 action 续转**（复用同文件「跨 action 续转归因」用例的 store/reducer 编排：A 产 2 条 `Run` ⇒ 第一条转发即 panic（第二条留存）⇒ B 的 dispatch 触发续转 ⇒ 续转那条的两个 action 序号**值不同**）⇒ 断言形态不变（桥输出两串与事件值逐字比对）⇒ 现自动具备鉴别力。**桥实现未改**（`skeleton-infra/src/tracingx.rs` 零 diff）。
- **P2 `EffectId` 唯一性纪律**：#48 本批引入的四处表述（允许在同一 action 内复用同一 `EffectId` / 声称同一 action 内复用同一句柄也能定位到具体 occurrence / 在「`EffectId` ≠ 投递身份」段标注该句柄「可复用」）与 `skeleton::EffectId` 的唯一性纪律（**一个 store 内全局唯一**）**冲突** ⇒ 改写为符合纪律的版本（`skeleton/src/lib.rs` 的 `EffectKind` 文档 · `skeleton-infra/src/audit.rs` 的 `effect_index` 字段文档 · `skeleton-infra/src/runtime.rs` 的 `PendingEffect` 与 `EffectBufs::commit` 文档 · 本文件与 `docs/reference.md` 的「`EffectId` ≠ 投递身份」段）。**保留**正确的那半：「`EffectId` 是 reducer/消费者定义的**逻辑效果句柄**，**不是**框架生成的投递尝试身份」（#45 Option D 的基石）。**未**承诺「重复 id 可安全复用」（迟到 `Cancel` 是合法时序，核心无生命周期汇报面 ⇒ 无法区分「迟到」与「漂移」）。
- **P2 `effect_id` 对 `Cancel` 的歧义**：`AuditEventKind::EffectInterrupted.effect_id` 此前只写「效果的逻辑句柄」未区分种类 ⇒ 澄清（`skeleton-infra/src/audit.rs` 字段文档 · `skeleton/src/lib.rs` 的 `EffectKind` 文档 · `docs/reference.md` 字段表）：`Run` 的 `effect_id` 是**该效果自己的**句柄；`Cancel` 的 `effect_id` 是**被取消目标**的句柄（Cancel 效果自身没有句柄）⇒ `effect_id` **不可**单独当作「效果身份」用（同一 store 内 `Run(id=N)` 与 `Cancel(id=N)` 发出**相同**的 `effect_id`）⇒ 三元组 `(effect_kind, effect_id, effect_index)` **只在同一产出 action 内**唯一标识一个效果；**跨 action** 唯一标识用 `(origin_action_seq, effect_index)`（⚠️ 第四轮对抗性审计**再更正**——无作用域的「三元组唯一标识」被探针确定性证伪，见下方第四轮 Fixed 段）。
- **P3 迁移指引入 reference**：`docs/reference.md` 的 `AuditEventKind` 枚举块补**迁移**句（`match` 改解构形 · 等值比较改 `matches!` · 派生 `PartialEq` 语义随结构变体逐字段比较）⇒ 读者看 reference 即可改，不必翻 CHANGELOG（与本文件上方 #48 迁移段口径一致，无冲突）。
- **P2-1 数字口径加固（**不改动数字本身**，只加固表述的权威性）**：带载荷档数值标注为**点估计**（5 轮交替取中位、**未做显著性检验**、4096 B 档单轮抖动大），并如实登记**另一套口径**（对抗性审计：4096 B 181.2 ⇒ 415.6 ns/效果，**+129%**）与自跑值（**+51.7%**）**绝对值差异未查清** ⇒ 本段**不声称**单一权威百分比，结论仍为「代价随 `Op` 载荷线性放大、零载荷档的「统计不显著」不可外推」。
- **本批口径**：**运行行为零变更**（纯 rustdoc / 注释 / 测试场景 / CHANGELOG / reference.md）；**未新增任何公共 API**、**未新增依赖**（`Cargo.toml`/`Cargo.lock` 零 diff）。

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
- **panic 语义已定（GitHub issue#45 拍板：Option D——非 A/B/C 原文）**：`EffectRuntime::run` panic 时**该次调用退出自动重放**（框架**不再调用**它 ⇒ **至多一次调用尝试**），但**不得**据此认为该效果「未执行」——`run` 可能在 panic 前**已产生外部副作用**（框架按值取走效果，**无法区分**「panic 前未执行 / 执行中 panic / 执行完后在清理或汇报时 panic」）⇒ 该效果的**外部结果不确定（indeterminate）**。**不承诺** exactly-once 投递或 exactly-once 副作用。**中断可见**：同一路径发出 `EffectInterrupted` 审计事件，转发失败回执仍报 `Failed`（失败阶段 = `ForwardingEffects`——注意与**成功**转发完毕的 `EffectsForwarded` 词序相反）+ `state_committed=true`。**既有保证不变**：成功前缀不重放、已提交未调用的后缀按 FIFO 续转恰好一次。**幂等性属消费方责任**；将来若需可重试投递，作为**显式 opt-in 能力**引入（稳定 delivery identity + 适配器侧持久幂等/去重 + 三态结果），**不**作为默认契约。

### Added（SyncStore dispatch 回执 + drain 恢复交接 + 有界准入，2026-11-18；GitHub issue#40 / #41 / #38）
- **#40 并发 dispatch 回执**：新增公共面 `try_dispatch` / `set_capacity` / `admission_full_count` / `AdmissionOutcome`。**完成边界 = per-action**（状态提交 → 效果转发 → 通知返回）+ 全部因果后代终局——外部并发 dispatch 在**自己的** action 到达该边界前不返回；同线程/同 store 重入保持「仅入队不返回」以免自死锁。
- **#41 drain owner 异常恢复交接**：`DrainGuard` 的 `Drop` **只记账**（当前记录按阶段失败 + 残留整批失败 + 摘队放旗同临界区），**不调任何回调**（reducer / 解释器 / 订阅 / 审计）——但展开清理期**必有用户代码**：被摘下的队列载荷按值析构，其 `Drop` **必须 panic-free 且不得重入同一 store**（#66 补契约；原「零用户代码」表述为误）。批次中断为 fail-visible：不重放、原 panic 照常上浮、等待者**恰好结算一次**。排空权判定由单槽改为 **TLS 帧栈**（修 A→B→A 同栈误判）。
- **#38 有界准入**：容量 = **等待被 reduce 的 action 数**（执行中不计入），只约束外部准入，同栈回灌豁免；满时 **typed reject 不阻塞**（阻塞会与回调重入死锁），legacy `dispatch` 显式 panic。超反馈预算的动作**停放 deferred 队列**（零丢失留痕，外部流量不被饿死）。
- **口径变更（相对 issue#44）**：反馈预算止步由「后续 dispatch 重放」改为「停放」——残留不再自动再执行，但**可查、可计数**。
- **行为变更**：legacy `dispatch` 对**并发外部调用者会阻塞**（issue#40 拍板）。**已知残余风险（2026-11-20 已复现并文档化）**：若宿主在 drain 回调（`EffectRuntime::run` / 订阅回调）里 spawn 工作线程并 join、且该线程用**阻塞式** `dispatch` 回调同一 store ⇒ **确定性互等死锁**（排空环被 join 卡住，而回执只由排空环兑现）。**规避**：派生工作走**框架提供的** sender（`Store::sender()` 或回调收到的 `back`；**回调内 = 竞争态** ⇒ 纯准入、不等回执）或不得 join；同线程重入仅入队不等待（不互等）。该义务已写进契约（`skeleton::Store::dispatch` 与 `EffectRuntime::run` 的 rustdoc）；**框架侧未加防护**（属宿主误用），复现证据见 `verify/2026-11-20-residual-join-risk/`。
- 机制：新增 `runtime/receipt.rs`（完成账本 `Ledger` + 阶段/失败分类），账本走复用池且**归还者唯一**（首发者胜）。
- 测试：新增 `tests/dispatch_receipts.rs`（7 用例）与 `tests/bounded_admission.rs`（4 用例）；`memgov` 三个零分配硬锚照跑通过。
- 变异三则：跳过 `ledger.wait()` ⇒ 红；中断批次误报成功 ⇒ 红；容量判定 off-by-one ⇒ 红（`left: Accepted, right: Full`）。
- 未做（明示）：`close_and_wait()`、`ActionSender::try_dispatch_with_receipt`（本批的 result-bearing 面在 `SyncStore` 侧，见 issue#57）；因果屏障只覆盖有等待者的根（无等待者的根不分配账本）。
- PM 亲验：仓库级四门（test `--all-features` 串行 / clippy `-D warnings` / fmt `--check` / rustdoc `-D warnings`）全 exit 0。

### Fixed（契约表述更正 + 宿主意务入契约，2026-11-20；GitHub issue#46 / #40）
- **`ActionSender::dispatch` 的契约表述更正（两版口径都不精确 ⇒ 改为条件性）**：旧文档写「入队并同步至**本 action 的完成边界**，与 `Store::dispatch` 同语义」——**在竞争态为假**（只入队即返，不排空）；中间一版写「框架提供的 sender 是**准入-only**、不等完成边界、非阻塞」——**在静默态为假**（本线程夺得排空权并同步排空至 idle，返回时该 action **已被 reduce、完成边界已达成**）。⇒ 契约改为**条件性两条路径**：**竞争态**（调用瞬间已有在任排空者）= 纯准入（只保证已入队、**不等**完成边界）；**静默态**（调用瞬间无在任排空者）= 同步排空至 idle（返回即完成边界）。**`bool` 的语义消除二义**：只承载「被 store 接受（已入队）或被拒」，**不承载**「是否已 reduce / 是否已达完成边界」。**实现从未变，变的是文档** ⇒ 下游若照任一版口径写过「sender dispatch 返回即已提交/已生效」或「sender 一定不阻塞」的代码，是**潜在隐患**，建议核对。（⚠️ 2026-11-20 第三轮对抗性审计**再更正**：竞争态下「返回瞬间该 action 是否已被 reduce」**不是**框架保证——见下条「Fixed」项；本条只保留两条路径的划分。）
- **「准入-only」的适用范围收紧**：准入-only 只描述**框架提供的** sender 在**竞争态**下的行为（`Store::sender()` 与解释器回调收到的 `back`——后者在 drain 回调内**必处竞争态**，故回调内可 join）；经 `ActionSender::new` **自建**的 sender 语义**由实现方决定**（闭包行为即语义：是否阻塞、`true` 意味什么），调用方须知其语义、**不可假定**框架提供的 sender 的任一条路径语义。契约面 `ActionSender` 类型文档「两种来源，语义不同」与 `ActionSender::new` 的「闭包的语义即本句柄的语义」同步改为条件性表述。
- **#46 宿主机义务入契约**（GitHub issue#46 维护者拍板 **A + 现有 D**）：drain 回调（`EffectRuntime::run` / 订阅回调）**运行在排空环内**——宿主在其中 join 一个用**阻塞式** `dispatch` 回调同一 store 的线程 ⇒ **确定性互等死锁**（排空环被 join 卡住，而回执只由排空环兑现）。契约面新增/收紧四处：① `skeleton::Store::dispatch` 的「等待语义与宿主意务」段；② `skeleton::ActionSender::dispatch` 的「条件性两路径 + `bool` 唯一含义」与 joined worker 指引；③ `skeleton::EffectRuntime` 类型文档的边界铁律；④ `docs/reference.md` runtime 章节宿主意务块 + 边界规则第 4 条。**规避路径（唯一）**：回调派生的 worker 用**框架提供的** sender 提交（**回调内 = 竞争态** ⇒ 纯准入、不等回执 ⇒ 可被 join）；若 worker 确实需要该 action 的完成结果才能返回，则**不得**被同一 store 排空环上的回调同步 join。
- **可编译示例 + 正向回归闸**：`skeleton-infra::runtime::SyncStore::new` 的第二个 doctest（回调克隆 sender → worker `sender.dispatch(action)` → 回调 join 该 worker；排队 action 在**回调返回后**按 FIFO 处理）；`skeleton-infra/tests/dispatch_receipts.rs::joined_worker_using_sender_does_not_deadlock_and_is_processed_once`（准入 / 回调返回 / 恰好处理一次；**超时保护** ⇒ 死锁形态是**失败**不是挂死）。变异验证：worker 改用阻塞式 `Store::dispatch` ⇒ 闸在 10s 后**超时失败**（基线绿 / 变异红 / 还原绿），证据与复现工程见 `verify/2026-11-20-pm-impl/`。
- **条件性语义的机械闸（12 维独立复审整改，2026-11-20）**：契约表述本身**无回归闸**（rustdoc 只查链接与编译，不查语义真伪）⇒ 新增 `skeleton-infra/tests/drain_regressions.rs::framework_sender_admission_is_conditional_on_drain_state`，**同时钉住两态**：静默态 sender 返回时该 action **已被 reduce**（完成边界已达成）／竞争态 sender 返回时该 action **尚未被 reduce**——⚠️ 后者是**该闸特定编排下**的事实（drain 回调同步等待 worker 完成 ⇒ 排空环在该窗口内不可能推进），**不是**框架对「竞争态 ⇒ 该 action 未被 reduce」的无条件保证（⚠️ 2026-11-20 第三轮对抗性审计已证伪该无条件表述 ⇒ 见上方「Fixed（契约表述精度…）」项）。确定性编排（drain 回调内派生 worker + 同步等待完成信号，**禁 sleep**）+ 有界超时（`recv_timeout` 10s，失败不挂死）。
- **doctest 超时保护（同批整改）**：`SyncStore::new` 第二个 doctest 的**裸 `worker.join()`** 改为**带超时完成握手**（`mpsc` + `recv_timeout`）⇒ 同一退化形态（sender 退化为阻塞式）在 doctest 侧是**报红**（exit 101）而非**挂死**（原为 exit 124，CI 会被卡死）。
- **本批口径**：**运行行为零变更**（纯契约文本 / 注释 / 测试 / CHANGELOG）；**未新增任何公共 API**、**未新增依赖**。
- **框架侧不提供防护**：框架**无法**检测宿主侧的跨线程 join ⇒ **不加**框架侧防护或超时、**不加** drain-owner 可观测面、**不加** spawn/join helper（issue#46 决策：不实现 Option B/C/E；Option E 仅登记为 #40 的后续项 `ActionSender::try_dispatch_with_receipt` / `wait_timeout`，见上条「未做（明示）」）。
- **口径**：**未新增任何公共 API**；#40 的 per-action 回执语义与 #45 的 Option D 效果 panic 语义**均未变**（本次只改文档与测试）。

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
