# CI flake 治理：fx 域 shutdown 超时测试（macos runner）

**结论：修好了。根因是测试脆弱（归属①），不是产品缺陷（②排除）——同一测试有两个独立的脆弱面：a)「200ms spin 段 vs 50ms 超时」固定墙钟余量被慢机握手延迟击穿；b) `join` 返回 ≠ `FlightGuard` 已归零的跨线程 drop 序列观测窗。改为 hold 闸确定性握手 + 墙钟截止轮询，产品 `src/fx.rs` 实现零改动。**

## 根因分析

### CI 失败实况（ground truth）

- run 39895327075（2026-10-23）macos job：`fx::tests::shutdown_drains_and_shutdown_timeout_reports_timed_out` FAILED
  `panicked at skeleton-infra/src/fx.rs:370:5: fx scope not idle: 1 task(s) still in flight`
- fx.rs 自 4a7f8a4 未改 → 行 370 = `expect_idle` 的 `assert!`（本机源码逐行核实）。

### 本机复现（Windows，测试进程钉 CPU0-1 + 同核 2 个 CPU hog 争用，模拟 2 核饱和 runner）

复现出**两种**失败形态（后者 CI 尚未报过，但同机制必随负载出现）：

1. **370-mode**（CI 所报）：`expect_idle` 报 `1 task(s) still in flight`。回溯钉死调用点 = 测试尾部 join 断言**之后**的 `expect_idle`（`spinner.join()` 已返回 `Err(Cancelled)`）。探针实测：join 返回时 `in_flight=1`，150ms 真实 sleep 后才归零。
2. **501-mode**：`shutdown_timeout` 返回 `Ok(())` 而非 `Err(TimedOut)`（`assert_eq!` 于 fx.rs:501 失败，实测 elapsed 215-270ms）。

### 机理（对照 src/fx.rs 实现）

spawn 包装体（fx.rs:331-334）：

```rust
set.spawn(async move {
    let _flight = flight;              // FlightGuard——in_flight 计数守卫
    task_body(sem, fut, tx).await;     // tx = oneshot 发送端
});
```

- **面 b（370-mode）**：tokio 取消丢弃该包装 future 时，drop 序列为「内层 task_body future（含 tx）先、外层 `_flight` 守卫后」两条相邻语句。tx drop 即唤醒 join 侧（`Spawned::join` 的 rx → `Err(Cancelled)`）；被唤醒的测试线程若在另一 worker 上先被调度，会在守卫的 `fetch_sub` 执行前读到 `in_flight=1`。产品语义无罪：计数终将归零（守卫 drop 与 tx drop 同一 drop 序列，仅差一条指令）；`in_flight` 的文档定位是泄漏断言锚（最终归零），不是同步原语。旧测试把「join 返回」当「计数已归零」的同步点 = 越权假设。
- **面 a（501-mode）**：旧 spinner 以 200ms 墙钟 spin 段 + 每段一次 `yield_now` 构造「poll 不返回」；测试从握手（`spinning=true`，段首）到 `abort_all` 的延迟在饱和 2 核机上可达 150-270ms（本机实测），超过「段长 200ms − 超时 50ms」的 150ms 余量时，spinner 在超时触发前让渡被收割 → drain 提前完成 → `Ok(())`。
- **连带发现**：`wait_idle`（fx.rs:380 旧版）的 10⁴ 次 `yield_now` 预算防不了 OS 量子级抢占——让渡不释放 OS 线程，预算毫秒级耗尽而被抢占的 drop 执行线程几十毫秒后才被调度回来。修复面 a/b 后在极压装备下它成为新的失败点（fx.rs:388），已一并改造。
- **同款排查（契约要求的文件内自查）**：`shutdown_timeout_aborts_inflight_and_join_yields_cancelled`（⑭）与 ⑥ 完全同构（多线程 rt + 固定 spin 段 + join 后裸 `expect_idle`），两处脆弱面兼备，**一并加固**；①③⑦⑩⑧⑩的 join→`expect_idle` 走 current_thread 单线程顺序（drop 完成先于同线程上的后续 poll，无观测窗）；②④本就 `wait_idle` 收尾且 ④ 为 `start_paused` 定时器确定性形态；其余测试无墙钟依赖。

## 改动文件与要点

仅 `skeleton-infra/src/fx.rs`（测试模块内，产品 API/语义零改动；+56/−34 行）：

1. **⑥ `shutdown_drains_and_shutdown_timeout_reports_timed_out`**：
   - spinner 改 hold 闸结构：`hold`（测试→任务，留在 poll 内不让渡）/`holding`（任务→测试，已进不可让渡段）/`stop`（终局放行）。测试先 `hold=true`，等 `holding=true` 握手（确认任务滞留 poll、无可让渡 await 点）后才起 50ms 计时；超时返回后 `hold=false`+`stop=true` 放行让渡，abort 在下一 await 生效。
   - join 断言后：裸 `expect_idle` → `wait_idle` + `expect_idle`（沿用 ②④ 既有收尾形态）。
   - 所有断言保留且不放宽：`Err(TimedOut)`、`elapsed >= 50ms`、`Err(Cancelled)`、in_flight 归零，一个不少。
2. **⑭ `shutdown_timeout_aborts_inflight_and_join_yields_cancelled`**：同款 hold 闸改造（原 `entered` 握手 + 无限 `loop` spin 段 → hold 闸 + 让渡点），join 后同样 `wait_idle` + `expect_idle`。
3. **`wait_idle` helper**：10⁴ 次让渡预算 → 「首查归零即返；否则 1ms 真实 `tokio::time::sleep` 轮询，5s 墙钟截止后以同一文案 panic」。真实 sleep 让核给被抢占的 drop 执行线程；对 ②④⑧ 首查即归零零开销（④ 的 `start_paused` 下 sleep 自动推进，语义不变）。

## tokio test-util（pause/advance）必要性论证：不采用

超时路径的被测语义 = 「任务 poll 不返回则 drain 真超时」，要求任务滞留 poll 时**另一个 worker 能驱动实时定时器**——必须 `multi_thread` + 真实时钟（`start_paused` 仅支持 current_thread：单线程会被滞留 poll 堵死，定时器永不被驱动，直接死锁）。且 dev-dependencies 已含 test-util（④⑦ 在用），无 Cargo.toml 改动。

## 验证

复现装备 = 测试进程钉 CPU0-1 + 同核 2 个 CPU hog（修复前该装备 ~40 轮内 370-mode 与 501-mode 均复现）：

```
cargo test -p udf-skeleton-infra --all-features --lib fx::tests::shutdown_drains_and_shutdown_timeout_reports_timed_out
  unpinned:              5/5 PASS          （验收 ≥5 轮）
  pinned+contended:     30/30 PASS         （修复前 ~12 轮内即挂）
cargo test -p udf-skeleton-infra --all-features --lib fx::tests::shutdown_timeout_aborts_inflight_and_join_yields_cancelled
  pinned+contended:     30/30 PASS
fx::tests:: 全模块（11 测）pinned+contended: 10/10 轮全绿
cargo test -p udf-skeleton-infra --all-features
  20 suites / 260 passed / 0 failed     （工作树含同会话兄弟代理 memory_pool.rs/paths_alloc.rs 在途改动，数字为该树态实测）
cargo clippy -p udf-skeleton-infra --all-targets --all-features（touch fx.rs 强制重查）: fx.rs 相关 warning = 0
```

### CI 慢机（2 核）新等待余量依据（不再是运气）

- 面 a：**余量无穷大（按构造消除）**。holding 握手确认任务已在无可让渡段后才起表；该段持续到 `hold=false`，而 `hold=false` 只发生在 `shutdown_timeout` 返回之后——无论握手到 `abort_all` 被调度延迟多久，drain 在 50ms 窗口内都不可能完成，`Err(TimedOut)` 由构造保证。
- 面 b：**5s 墙钟截止 vs 实际 ~ms-150ms**。守卫归零只差被抢占线程一个调度量子（本机极压下实测 ≤150ms；GitHub runner 为独占 VM，争用仅来自同机 2-3 个 libtest 线程，量子数十 ms 级），5s = 30 倍以上余量；真泄漏时 5s 后以同一文案 panic，断言证明力不变（变慢不变松）。

## 残余风险

- `while !holding { yield_now }` 握手在任务被 OS 级饿死到永不首 poll 时会挂到 CI 超时（与旧 spinning 握手同风险类、无新增；多线程 rt 下两 worker 互喂让渡，实际不会发生）。
- 极压下 `wait_idle` 真泄漏误报延长到 5s 才红（可接受：只变慢不变松）。
- 本地 nightly-2026-05-28 rustfmt 对全仓 685 处报既有 diff（含 fx.rs 12 处非本次区域），即契约预告的已知假 diff 现象；本次改动维持仓内既有风格（四行 assert_eq 与旧 ⑥⑭ 同形），未对假 diff 做任何格式改写。

## 5 维自评

- 准确性 5/5 — 每条机理声明有 CI 日志/本机复现/探针时间线/源码行号四类证据交叉（run 39895327075 日志、370/501-mode 复现、探针 join 时 in_flight=1 → 150ms 后归零、fx.rs:331-334 包装体原文）。
- 完整性 4/5 — 契约逐条闭合（含 ⑭ 同款加固与 test-util 论证）；扣 1 分：全量套件数字含兄弟代理在途树态，非纯净 HEAD 基线（已在验证节注明）。
- 清晰度 5/5 — 双失败面分开归因，每面给机理/证据/修法三段。
- 可执行性 5/5 — 全部验证命令与复现装备（钉核+hog 参数）可直接重放。
- 简洁性 4/5 — hold 闸为最小构造（3 个 AtomicBool）；扣 1 分：探针脚本过程已删除，报告仅留结论与关键读数。
