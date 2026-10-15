//! README「三分钟快起」代码块的可运行原件（逐字同步——改一处必改另一处）：
//! 零效果最小环，`Op = Infallible` 使 `Effect::Run` 编译期不可构造。
//! 效果解释器直接配框架缺省件 `NoFx`——零手写空运行时样板。

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
