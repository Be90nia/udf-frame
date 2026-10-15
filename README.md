# udf-frame

最小单向数据流（UDF / Elm 式架构）Rust 框架：状态迁移是纯函数，副作用是描述式数据，解释器在边缘执行。

```
action ──▶ Store ──▶ Reducer（纯函数）──▶ 新状态 + 效果描述 ──▶ EffectRuntime（解释器）──▶ 回灌
                │                                                    │
                └──────────────── 订阅通知（快照） ◀─────────────────┘
```

## Crate 结构（两步分层）

| crate | 层 | 内容 | 依赖 |
|---|---|---|---|
| [`skeleton`](skeleton) | 契约层 | trait（`Reducer`/`Store`/`EffectRuntime`/`Driver`）+ 数据词汇（`Effect`/`EffectBuf`/`EffectId`/`ActionSender`/`Subscription`/`NoFx`）+ 语义契约文本 | **零依赖** |
| [`skeleton-infra`](skeleton-infra) | 实现层 | `runtime::SyncStore`（契约参考实现：同步排空环 + 快照广播）+ `audit` 审计事件流 + `compose` 组合器 + `testing` 测试替身 + 生态标准件（tokio fx / journal / memgov / text / backoff / logx / tracingx） | runtime（默认）std-only + skeleton 路径依赖，零三方 |

契约（约束后面代码长法的形状）与实现（机器）分家：`skeleton` 十分钟读完，`skeleton-infra` 按域查典。核心无 IO 依赖 → reducer「禁 IO」是编译期事实，不是评审约定；副作用只能以 `Effect::Run { op }` 描述值离场，由宿主提供的解释器执行。

## 快起

```sh
cargo run -p skeleton-infra --example quickstart
```

最小环（完整代码见 [skeleton-infra/examples/quickstart.rs](skeleton-infra/examples/quickstart.rs)）：

```rust
let store = SyncStore::new(Counter, NoFx);
store.dispatch(Msg::Inc);
assert_eq!(store.snapshot().0, 1); // 同步排空：dispatch 返回即 idle
```

契约即可实现：十几行即可实现 `skeleton::Store`（见 [skeleton](skeleton) 的 crate 文档 toy 环）。

## skeleton-infra 的 feature 门

| feature | 域 |
|---|---|
| `runtime`（**默认**） | `SyncStore`：`skeleton::Store` 参考实现（std-only）+ `audit` 审计事件值面 |
| `tokio` | `FxScope` 效果作用域：解释器在 tokio 任务里执行，回灌经 `ActionSender::dispatch` 重入 |
| `journal` | `JsonlJournal`：JSONL 追加式日志 + 快照行 + 回放（serde） |
| `memgov` | 计数分配器 + 有界注册表（淘汰策略必选：满则报错或逐最旧） |
| `testing` | `TestStore` 逐动作断言 + `ScriptedRuntime` 脚本化解释器替身 |
| `compose` | `combine` 父子分片组合器（只依赖契约词汇） |
| `log` | `logx`：日志门面绑定 + 密钥掩码 |
| `tracing` | `tracingx`：审计事件流 → tracing 桥 |

`cargo tree -p skeleton-infra -e normal`：默认 feature 下仅 skeleton（workspace 路径依赖），零三方。

## 构建与测试

```sh
cargo build --workspace --all-features
cargo test --workspace --all-features
```

## License

MIT
