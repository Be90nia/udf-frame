# udf-frame

最小单向数据流（UDF / Elm 式架构）Rust 框架：状态迁移是纯函数，副作用是描述式数据，解释器在边缘执行。

```
action ──▶ Store ──▶ Reducer（纯函数）──▶ 新状态 + 效果描述 ──▶ EffectRuntime（解释器）──▶ 回灌
                │                                                    │
                └──────────────── 订阅通知（快照） ◀─────────────────┘
```

## Crate 结构

| crate | 内容 | 依赖 |
|---|---|---|
| [`skeleton`](crates/skeleton) | 五件套签名（`Reducer`/`Effect`/`EffectRuntime`/`Store`/`Driver`/`TestStore`）+ 同步排空环 + `combine` 组合器 | **零依赖** |
| [`skeleton-infra`](crates/skeleton-infra) | 生态标准件绑定：tokio 效果作用域、serde_json 日志（journal）、有界注册表（memgov）、字节解码（text）、退避（backoff）、日志掩码（logx）、测试替身（testing） | 按 feature 取用 |

核心无 IO 依赖 → reducer「禁 IO」是编译期事实，不是评审约定。副作用只能以 `Effect::Run { op }` 描述值离场，由宿主提供的解释器执行。

## 快起

```sh
cargo run -p skeleton --example quickstart
```

最小环（完整代码见 [crates/skeleton](crates/skeleton)）：

```rust
let store = Store::new(Counter, NoFx);
store.dispatch(Msg::Inc);
assert_eq!(store.snapshot().0, 1); // 同步排空：dispatch 返回即 idle
```

## skeleton-infra 的 feature 门

| feature | 域 |
|---|---|
| `tokio` | `FxScope` 效果作用域：解释器在 tokio 任务里执行，回灌经 `ActionSender` 重入 |
| `journal` | `JsonlJournal`：JSONL 追加式日志 + 快照行 + 回放（serde） |
| `memgov` | 计数分配器 + 有界注册表（淘汰策略必选：满则报错或逐最旧） |
| `testing` | `ScriptedRuntime` 测试替身（引入核心 `skeleton` 依赖） |
| `log` | `logx`：日志门面绑定 + 密钥掩码 |

零 feature = 零依赖（`cargo tree` 单行可证）。

## 构建与测试

```sh
cargo build --workspace --all-features
cargo test --workspace --all-features
```

## License

MIT
