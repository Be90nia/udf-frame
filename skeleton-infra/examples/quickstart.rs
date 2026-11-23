//! README「三分钟快起」代码块的可运行原件（逐字同步——改一处必改另一处）：
//! 零效果最小环，`Op = Infallible` 使 `Effect::Run` 编译期不可构造。
//! 效果解释器直接配框架缺省件 `NoFx`——零手写空运行时样板。

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
    assert_eq!(store.snapshot().0, 2); // dispatch 返回 = 本 action 的完成边界（状态提交 → 效果转发 → 通知返回）且其因果后代终局；不承诺全局队列为空
    store.close();
}
