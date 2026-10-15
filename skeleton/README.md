# skeleton — 最小单向数据流（UDF）骨架契约层

`Reducer` / `Store` / `EffectRuntime` / `Driver` 契约 trait + 数据词汇
（`Effect`/`EffectBuf`/`EffectId`/`ActionSender`/`Subscription`/`NoFx`）+ 语义契约文本。
**零依赖**（tokio/serde 都不进树），纯函数纪律由「核心无 IO 依赖」结构性保证——reducer 想 IO 也编译不过。

本 crate 只承载「约束后面代码长法的形状」：trait 是数据契约，名字即数据流向，
各 trait 文档承载实现必须兑现的语义承诺（同步排空/快照一致性/订阅/停机闸/
fail-visible/回灌有界）。**可运行的实现在姊妹 crate**
[`skeleton-infra`](../skeleton-infra)：`skeleton_infra::runtime::SyncStore`（同步排空环 +
快照广播）、审计事件流、`combine` 组合器、`TestStore` 测试替身——按 feature 取用。

## 数据链路（只看名字即可复述）

| 问 | 依据名 |
|---|---|
| 数据从哪进 | `Store::dispatch`；远处经 `sender()` 的 `ActionSender::dispatch` 回灌 |
| 谁变形 | `Reducer::reduce`（纯函数：状态进出，效果写 `EffectBuf`） |
| 谁存储 | `Store`（`snapshot()` 取值语义快照） |
| 谁消费 | `Driver::render` 下行呈现；`Subscription` 订阅票据收变更通知 |
| 效果从哪出 | `Effect::Run` 描述值 → `EffectRuntime::run` 解释，经 `ActionSender` 回灌闭环 |

契约即可实现：crate 文档内含一个十几行的 toy 实现——实现 `Store` 六方法不需要任何机器。

## 三分钟跑起来（真实现）

```sh
cargo run -p skeleton-infra --example quickstart
```

`Op = Infallible` 的计数器：`Run` 在类型层不可构造——零效果程序编译期证明无副作用
（完整代码见 [../skeleton-infra/examples/quickstart.rs](../skeleton-infra/examples/quickstart.rs)）：

```rust
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;

#[derive(Clone)]
struct Count(u32);

enum Msg {
    Inc,
}

struct Counter;

impl Reducer for Counter {
    type State = Count;
    type Action = Msg;
    type Op = Infallible; // 零效果：Run 在类型层不可构造
    fn initial(&self) -> Count {
        Count(0)
    }
    fn reduce(&self, s: &Count, a: Msg, _out: &mut EffectBuf<Msg, Infallible>) -> Count {
        match a {
            Msg::Inc => Count(s.0 + 1),
        }
    }
}

fn main() {
    let store = Store::new(Counter, NoFx);
    store.dispatch(Msg::Inc);
    store.dispatch(Msg::Inc);
    assert_eq!(store.snapshot().0, 2); // 同步排空：dispatch 返回即 idle
    store.close();
}
```

## 其余可跑示例（都在 skeleton-infra）

- `examples/counter_local_runtime.rs` — 订阅断言 + Emit 回灌链（撤销计数器）
- `examples/dispatch_profile.rs` — dispatch 热路径分段归因基准
- `examples/driver_tour.rs` — Driver 端口两职责（bind 上行 / render 呈现）
- `examples/end_to_end.rs` — 五件套 + `combine` + `TestStore` + fx/journal 解释器标准形

```sh
cargo run -p skeleton-infra --features tokio,journal,testing,compose --example end_to_end
```
