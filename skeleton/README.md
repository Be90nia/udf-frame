# skeleton — 最小单向数据流（UDF）骨架核心

`Reducer` / `Effect` / `EffectRuntime` / `Store` / `Driver` 五件套签名（+`TestStore` 测试面）+ 最小运转环。**零依赖**（tokio/serde 都不进树），纯函数纪律由「核心无 IO 依赖」结构性保证——reducer 想 IO 也编译不过。

本 crate 是纯签名壳与同步排空环；tokio 效果解释器、serde 日志、有界注册表、测试替身等标准件绑定在姊妹 crate [`skeleton-infra`](../skeleton-infra)（按 feature 取用）。

## 三分钟快起（零效果最小环）

`Op = Infallible` 的计数器：`Run` 在类型层不可构造——零效果程序编译期证明无副作用。
以下代码与 `examples/quickstart.rs` 逐字同步，或直接跑
`cargo run -p skeleton --example quickstart`：

```rust
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store};

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

## 本 crate 其余可跑示例

- `examples/counter_local_runtime.rs` — 订阅断言 + Emit 回灌链（撤销计数器）
- `examples/dispatch_profile.rs` — dispatch 热路径分段归因基准
- `examples/driver_tour.rs` — Driver 端口两职责（bind 上行 / render 呈现）

端到端小例（五件套 + `combine` + `TestStore` + fx/journal 解释器标准形）在
`crates/skeleton-infra/examples/end_to_end.rs`，跑法：

```sh
cargo run -p skeleton-infra --features tokio,journal,testing --example end_to_end
```
