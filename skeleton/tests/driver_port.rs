//! Driver 端口行为验收。
//!
//! 端口契约（MVI 单输出，两职责两成员）：
//! - `bind`：唯一上行通道——宿主把回灌句柄交给事件源，事件经它闭环回 store；
//! - `render`：唯一呈现入口——收每次状态变更后的快照（render(state) 纪律）。
//!
//! 核心没有驱动循环：宿主循环由本测扮演（事件到达→send；状态变更→render）。
//! 单线程测试，帧收集用普通 Vec——零锁零毒化面。

use std::convert::Infallible;

use skeleton::{ActionSender, Driver, Effect, EffectBuf, EffectRuntime, Reducer, Store};

enum Msg {
    Bump,
}

struct Counter;

impl Reducer for Counter {
    type State = u32;
    type Action = Msg;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: Msg, _out: &mut EffectBuf<Msg, Infallible>) -> u32 {
        s + 1
    }
}

struct NoFx;

impl EffectRuntime<Msg, Infallible> for NoFx {
    fn run(&mut self, _: Effect<Msg, Infallible>, _: &ActionSender<Msg>) {}
}

/// 收集 render 帧序 + 持上行句柄的最小 Driver（examples/driver_tour 形态）。
struct FrameDriver {
    sender: Option<ActionSender<Msg>>,
    frames: Vec<u32>,
}

impl Driver<u32, Msg> for FrameDriver {
    fn bind(&mut self, sender: ActionSender<Msg>) {
        self.sender = Some(sender);
    }
    fn render(&mut self, state: &u32) {
        self.frames.push(*state);
    }
}

/// 契约：bind 交付的上行句柄可用（事件源→store 闭环推进）；render 收到的
/// &S 是每次 dispatch 后的 reduce 结果（帧序 = 状态迁移序）。
/// 此前 Driver 仅 examples 有签名壳占位、零行为验证——端口不可用/接线断裂
/// 在此现形。
#[test]
fn driver_bind_closes_event_loop_and_render_frames_track_state() {
    let store = Store::new(Counter, NoFx);
    let mut driver = FrameDriver { sender: None, frames: Vec::new() };

    // 宿主接线：bind 拿上行通道
    driver.bind(store.sender());

    // 宿主循环：状态变更后 render——帧序必须逐拍等于 reduce 后状态
    store.dispatch(Msg::Bump);
    driver.render(&store.snapshot());
    store.dispatch(Msg::Bump);
    driver.render(&store.snapshot());
    assert_eq!(driver.frames, vec![1, 2], "render 帧序 = reduce 后状态序");

    // 事件源经 bind 交付的句柄回灌——上行通道闭环推进 store
    let sender = driver.sender.take().expect("bind 必须交付上行句柄");
    assert!(sender.send(Msg::Bump), "bind 交付的句柄必须可回灌");
    assert_eq!(*store.snapshot(), 3, "事件源回灌推进 store（闭环成立）");
}
