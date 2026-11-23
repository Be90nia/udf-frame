# validation/ —— 第二消费方验证夹具（#56 Stage A）

本目录是 **受版本控制的验证路径**：`fresh clone` 必须能构建这里的消费方并复现其依赖树证据。
（早期版本把它放在被 `.gitignore` 忽略的 `verify/` 下 ⇒ 无法满足「干净检出可移植」这一验收，
已于 2026-11-23 迁入本目录；`verify/consumer-sim/` 仅作本地一次性实验区。）

## 目录结构

```
validation/consumer-sim/            # 消费方 A：全 API 面扫街（async 面：显式点亮 tokio）
  Cargo.toml / Cargo.lock          # 独立 workspace（自带空 [workspace] 根）⇒ 外部 crate 视角
  src/main.rs                      # 消费方本体 + 六个「反例」鉴别力注释块
  MIGRATION.md                     # 下游迁移说明（采用方视角，破面逐条）
  dep-trees.txt                    # 两面依赖树证据（入库）
  gen-dep-trees.py                 # 依赖树证据的生成脚本（改依赖后重跑）
  workflow/                        # 消费方 B：CLI/workflow 型（sync-only 面）
    Cargo.toml / Cargo.lock
    src/main.rs
```

两个消费方是**独立 workspace**，只以 path 依赖指向仓库三 crate ⇒ 编译视角 = 外部 crate 视角，
凡「同 crate 豁免」的规则在此一律生效。

## 跑法（两面）

```sh
# 面 1：sync-only（消费方 B，workflow/）—— 期望：无 tokio
cd validation/consumer-sim/workflow
cargo check                # 绿
cargo run                  # 输出 workflow_consumer: OK
cargo tree -e normal       # 4 节点全 path，无 tokio

# 面 2：async（消费方 A，consumer-sim/）—— 期望：有 tokio
cd validation/consumer-sim
cargo check --all-targets  # 绿（迁移面完整）
cargo run                  # 输出 consumer-sim: OK
cargo tree -e normal       # 11 节点，含 udf-skeleton-infra/tokio + 宿主直接依赖 tokio

# 重新生成两面依赖树证据（入库文件）
cd validation/consumer-sim && python gen-dep-trees.py
```

## Stage A 交付边界

本切片只交付**编译级证据 + 依赖树证据**：

- 无私有模块访问、不 require 内部 cfg、不 require 内部 feature 矩阵；
- 消费方 B（sync-only）额外覆盖：自定义 `EffectRuntime` 实现（`RecordingFx`：记录执行过的
  `EffectId`/效果形态，不接真实 I/O）、有界准入（`SyncStore::set_capacity` +
  `try_dispatch -> AdmissionOutcome`，跨 crate 穷尽 match 可编译通过）、graceful shutdown
  （`Store::close` / `is_closed`，闭门后 `try_dispatch` 走 `AdmissionOutcome::Closed` typed 拒绝）。

**并发下的 `Full` 路径、停机排空时序等 runtime 行为属 #56 Stage B**，此处不测。

## 鉴别力证明（负编译探针，保留）

`consumer-sim/src/main.rs` 末尾有六个**反例**注释块，取消任一注释后 `cargo check` 必红 ——
证明本夹具确实能检出破坏面泄漏，而不是恒绿：

| 反例 | 破坏面 | 期望编译错误 |
|---|---|---|
| A | `EffectId` 裸元组构造 | E0423 / E0603（字段私有） |
| B | 跨 crate match 漏通配臂 | E0004（non-exhaustive patterns） |
| C | `Combine` 分片作根 | E0277（根契约 `Reducer` 未实现） |
| D | `LintError` 裸构造 | E0451（受控构造） |
| E | `Pooled` 裸守卫 | E0451（只能经 `acquire` 取得） |
| F | `expect_idle` 歧义名 | E0432 + E0599（已拆为两个口径的方法） |

逐条迁移说明见 `MIGRATION.md`。
