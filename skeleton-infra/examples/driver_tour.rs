//! Driver 端口演示（五件套收口件，此前零可运行示例）：MVI 单输出，两职责两成员——
//! ①`bind`：唯一上行通道（拿 ActionSender 回灌句柄，事件源把外部事件送回 store）；
//! ②`render`：唯一呈现入口（render(state) 纪律：状态快照进视图层）。
//! 核心没有驱动循环——Driver 由宿主调用：状态变更后 render、事件到达后 send。

use skeleton::{ActionSender, Driver, EffectBuf, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;
use std::convert::Infallible;

#[derive(Clone, Debug, PartialEq)]
struct Count(u32);

enum Msg {
    Inc,
}

struct Counter;

impl Reducer for Counter {
    type State = Count;
    type Action = Msg;
    type Op = Infallible;
    fn initial(&self) -> Count {
        Count(0)
    }
    fn reduce(&self, s: &Count, a: Msg, _out: &mut EffectBuf<Msg>) -> Count {
        match a {
            Msg::Inc => Count(s.0 + 1),
        }
    }
}

/// 最小 Driver：收集 render 帧序（呈现面）+ 持回灌句柄（上行通道）。
struct FrameDriver {
    sender: Option<ActionSender<Msg>>,
    frames: Vec<u32>,
}

impl Driver<Count, Msg> for FrameDriver {
    fn bind(&mut self, sender: ActionSender<Msg>) {
        self.sender = Some(sender);
    }
    fn render(&mut self, state: &Count) {
        self.frames.push(state.0);
    }
}

fn main() {
    let store = Store::new(Counter, NoFx);
    let mut driver = FrameDriver {
        sender: None,
        frames: Vec::new(),
    };

    // ① 接线：宿主把回灌句柄交给驱动（事件源上行通道建立）
    driver.bind(store.sender());

    // ② 呈现：初始帧 + 每次状态变更后的帧（render(state) 纪律）
    driver.render(&store.snapshot());
    store.dispatch(Msg::Inc);
    driver.render(&store.snapshot());

    // ③ 上行：驱动经 bind 到的句柄把外部事件送回 store（模拟 IO 事件到达）
    let delivered = driver.sender.as_ref().unwrap().dispatch(Msg::Inc);
    assert!(delivered, "开门状态 dispatch 必须成功");
    driver.render(&store.snapshot());

    // 帧序自断言：初始 0 → dispatch 后 1 → 回灌 dispatch 后 2
    assert_eq!(driver.frames, vec![0, 1, 2]);
    store.close();
    println!("driver tour ok: frames={:?}", driver.frames);
}
