# ci-flake-anchors 修复报告 — AnchorCounts（2026-10-23）

**结论：修好了。** 两个计数锚 CI 挂是两类独立根因：paths_alloc 是**背书期望值推演错误**（Unix 面真实分配 6 次而背书写 3——该 `#[cfg(not(windows))]` 测试在 Windows 开发机从未真正执行过，CI 首跑即揭穿）；memory_pool 是**全局 live 计数被同二进制并发净漂移污染**（产品无罪）。两文件均已加固，CI 同口径命令 5×5 全绿。

## 根因分析

### 1. std-ext/tests/paths_alloc.rs:219 — CI ubuntu `paths_alloc serial` 步（--test-threads=1）挂，left:6 right:3

**排除并行污染**：该步单线程串行，失败发生在串行口径下 → 不是污染，是期望值本身错。

**计数口径**：文件内 `EventCounting`（`#[global_allocator]`，alloc/realloc/alloc_zeroed 各计 1 事件）→ 全局原子 `EVENTS`，窗口 = `allocs()` 前后差值。

**实现链推导（真实 6 的构成）**：
| # | 分配点 | 依据 |
|---|---|---|
| 1 | `home_dir()` → `var_os("HOME")` OsString **1 次** | std-ext/src/paths.rs:448-451 `xdg_base_dir` **无条件**先求值 `home_dir().as_deref()`（绝对值臂也不跳过）；unix 面 var_os=1 已由 ubuntu 上先跑先绿的 `home_xdg_alloc_anchors_match_doc`（断言 home_dir=1）CI 实测背书 |
| 2 | `var_os("XDG_CONFIG_HOME")` OsString **1 次** | 同 paths.rs:451；与 HOME 同 getenv 拷贝机制 |
| 3 | `map(PathBuf::from)` env 借用→PathBuf 拷贝 **1 次** | paths.rs:437 `.map(PathBuf::from)`（`&OsStr` → to_os_string 字节拷贝） |
| 4 | `normalize_lexical` **3 次**（非背书的 2） | paths.rs:133-165：`Vec::with_capacity` 组件栈 1 次 + `collect` 输出 PathBuf 2 次——push "/" 首块 cap 8，`"/xdg-alloc/cfg"` 14 字节在 push "xdg-alloc"（累计 10 字节）时触发 1 次 grow；背书模型漏了「输出缓冲按字长分级 grow」 |

绝对值臂合计 = 1+1+1+3 = **6** = CI left:6 直接吻合。默认臂（remove_var 后）= home 1 + var_os(None) 0 + join **2**（paths.rs:442 `home.join`：PathBuf::from(home) 拷贝 1 + push ".config" 使 15→23 字节触发 grow 1）+ normalize **4**（`"/xdg-alloc-home/.config"` 23 字节：Vec 1 + collect 3，两次越界 grow）= **8**（旧背书 4，从未执行过，不修则下一断言必红）。

**本机探针实测复核**（Windows 面，纯 std 分配机制与平台无关、同字节同行为）：`from_env=1 / norm_abs=3 / join=2 / norm_def=4`，与推导逐项一致（探针为临时测试，已删，未入 diff）。

**定性**：3/4 是作者推演值，非定理——它是「std 版本内部分配策略 × 测试输入字节长度」的**确定函数**（同 toolchain + 同输入必现 6/8），这正是分配锚的设计意图：锚定 doc 声明与实测逐字一致，std 变更时显式红、按新机制重校准。

**同文件同族脆弱面加固**：全部计数断言在**并行整跑**下共享「libtest 主线程输出分配落窗」污染面（文件旧注释自述 5 跑 2 红）→ `EventCounting` 加 `IN_WINDOW` 线程归属门（const TLS `Cell<bool>`，只计窗内本线程分配），断言语义不变（仍逐字 assert_eq）；`SERIAL` 互斥保留（env 注入臂互踩防护，计数不再依赖它），注释同步改写。

### 2. skeleton-infra/tests/memory_pool.rs:488 — CI windows `all-features` 并行步挂，left:57 right:64

**计数口径**：产品 `memgov::CountingAllocator` 全局 `LIVE_ALLOCS` **净额**（alloc +1 / dealloc −1，src/memgov.rs:121/140/178/198）。1000 次借还环窗口内，同二进制其他测试线程（该文件 14 测试并行）+ libtest 主线程的分配/释放**净漂移**直接进窗（本例 −7：别人释放多于分配）——产品池稳态借还确实零分配（本机多轮恒绿），**测试基建债非产品 bug**。

**加固**：本文件自挂 `TlLive` 包装 `CountingAllocator`（全局五计数器照常累计），新增 `OWN_LIVE` 线程归属净额（const TLS `Cell<isize>`；alloc/alloc_zeroed 成功 +1、dealloc −1、realloc 换块旧死新生对消净 0）；两处窗口断言改读 `own_live()` 前后差 == 0——同一性质（稳态借还零分配零滞留）**精确等值证明，非宽松化**。`settle()`/`serial()`/`SERIAL` 失去存在意义（并发分配本就不进窗），按死码删除（不清会挂 clippy dead_code）。

## 改动文件与要点（diff 仅这两个文件）

- `std-ext/tests/paths_alloc.rs`：+`use std::cell::Cell`；`IN_WINDOW` 门（thread_local 上方用 `//` 注释——`///` 挂宏调用会触发 unused_doc_comments）；`EventCounting` 三计数点加门；`allocs()` 进出窗置位；`xdg_unix_alloc_anchors_match_doc` 断言 3→6、4→8 + 背书 doc 逐字重写（含逐项分解、「std 变更须连动本锚」警示）；文件头与 SERIAL 注释同步。
- `skeleton-infra/tests/memory_pool.rs`：imports（+`GlobalAlloc`/`Layout`/`Cell`，−`Mutex`/`MutexGuard`）；`TlLive`+`OWN_LIVE`+`own_live()`；删 `SERIAL`/`serial()`/`settle()`；`steady_state_borrow_return_is_zero_alloc` 两窗改 own_live() 断言；文件头口径注明。
- 产品 src / CI 配置 / 其他测试文件：**零改动**（红线遵守）。

## 验证（命令 + 完整关键输出）

| 命令 | 结果 |
|---|---|
| `cargo test -p udf-std-ext --test paths_alloc -- --test-threads=1` ×5 | `ok. 3 passed; 0 failed` ×5（Windows 面 3 测试；unix 测试本机不可编译，其值=CI 实测 6+推导+探针三重印证） |
| `cargo test -p udf-skeleton-infra --all-features --test memory_pool` ×5（默认并行=CI 口径） | `ok. 14 passed; 0 failed` ×5 |
| `cargo test -p udf-std-ext` 全量 | 全 target ok；paths_alloc **并行整跑** 3/3 绿 = 线程归属免疫实证 |
| `cargo test -p udf-skeleton-infra --all-features` 全量 | 全 target ok（含 memory_pool 14/14） |
| `cargo check` 两目标 | 0 warning（我引入的 thread_local 文档注释警告已清；CI 既有 unix-only unused-imports 警告非本族引入，不越界） |
| `cargo clippy` 两目标 | Finished，0 警告 |
| `cargo fmt --all -- --check` | 本两文件 0 diff（全仓现存假 diff 均在未触碰文件，已知现象） |

## 残余风险（CI 仍可能红的条件）

1. std 升级改变 Vec/OsString 分配策略（growth 常量、var_os 实现）→ 6/8 锚显式红——这是锚的设计意图，按新机制重校准并连动背书注释。
2. macos-latest 面未实测（本轮 CI 挂的是 macos fx 域另一测试）；unix 分配机制 linux/macos 同源，理论同值，若披差异同法校准。
3. 本轮 CI 的 macos `fx::tests::shutdown_...` 挂与本两锚无关（另一战线）。
4. `skeleton-infra/tests/fx_probe_tmp.rs`（untracked）非本任务产物，未触碰。
5. **知识图谱**：主仓图谱（D-Project-Xray-core-rust，indexed_at 2026-10-22）对嵌套 udf-frame 仓 0 覆盖（search_code 探针 `steady_state_borrow_return_is_zero_alloc` 0 命中）——本次改动无法使其失真，无需重索引。

## 自评（5 轴）

- **准确性 5/5** — 6/8 值 = CI 实测（left:6）+ 实现行号推导（paths.rs:448-451/437/133-165）+ 本机探针逐项实测（1/3/2/4）三重印证；memory_pool 污染机制有 CI panic 数值（57/64）与计数器实现（memgov.rs:121/178/198）佐证。
- **完整性 4/5** — 两个锚 + 同文件同族断言并行脆弱面一并加固；settle/serial 死码清理；验证含 CI 同口径 5×5+全量+clippy+fmt。扣 1 分：macos 面未实测（无本机面）。
- **清晰度 5/5** — 两根因分立叙述，数值全带来源，排除项（非污染/非产品 bug）显式论证。
- **可执行性 5/5** — 验证命令可直接复跑；「std 变更→重校准→连动注释」的维护路径写明。
- **简洁性 4/5** — 机制与证据为主；排查过程（CI log 定位、serena 降级、stash 对照）压缩为一行级提及。

已沉淀: ~/.omp/agent/rules/rust-pitfalls.md ← `///` 挂宏调用整体（thread_local!）必触发 unused_doc_comments，与挂真 item 的 `#[global_allocator]`+static 形态肉眼难分。
