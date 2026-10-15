//! `TestStore::receive`
//! 的效果断言 panic 必须指采用方调用点（`#[track_caller]`），而非库内 assert 站
//! （src/lib.rs）——报错定位与 IDE 跳转价值。
//!
//! 通道：panic hook 捕 `Location`（std 无其他公开 panic 位置通道）；文件名+行号
//! 双锚断言。hook 活跃窗口内其他并发测试的 panic 位置也会入集，断言按「存在
//! 匹配本文件期望行号的记录」收敛，不受并发记录干扰。两个 panic 点各一：
//! 队列空（no effect produced）与变体不符（effect mismatch）。

use skeleton::{Effect, EffectBuf, EffectId, ExpectedEffect, Reducer, TestStore};
use std::convert::Infallible;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::channel;

#[derive(Debug)]
enum Msg {
    Inc,
}

/// 零效果 feature：receive 断言走「队列空」panic 点。
struct QuietFeature;

impl Reducer for QuietFeature {
    type State = u32;
    type Action = Msg;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: Msg, _out: &mut EffectBuf<Msg>) -> u32 {
        s + 1
    }
}

/// 产 Run 效果的 feature（Op=u8 使 Run 可构造）：receive 断言走「变体不符」点。
struct NotifyFeature;

impl Reducer for NotifyFeature {
    type State = u32;
    type Action = Msg;
    type Op = u8;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, _: Msg, out: &mut EffectBuf<Msg, u8>) -> u32 {
        out.push(Effect::Run { id: EffectId(1), op: 7 });
        s + 1
    }
}

#[test]
fn receive_panic_reports_caller_location() {
    // hook 与测试主体同线程（catch_unwind 同步 panic），send 恒达（rx 存活到最后）
    let (tx, rx) = channel();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Some(loc) = info.location() {
            let _ = tx.send(loc.to_string());
        }
    }));

    // 点 1：队列空断言（零效果 reducer → receive 弹空队列 → panic）
    let empty_line = {
        let mut ts = TestStore::new(QuietFeature);
        ts.send(Msg::Inc);
        let line = line!() + 1;
        let _ = catch_unwind(AssertUnwindSafe(|| ts.receive(ExpectedEffect::Cancel { id: None })));
        line
    };

    // 点 2：变体不符（期望 Cancel 实际 Run { op: 7 } → mismatch panic）
    let mismatch_line = {
        let mut ts = TestStore::new(NotifyFeature);
        ts.send(Msg::Inc);
        let line = line!() + 1;
        let _ = catch_unwind(AssertUnwindSafe(|| ts.receive(ExpectedEffect::Cancel { id: None })));
        line
    };

    std::panic::set_hook(prev_hook);

    // 收集仅在 hook 活跃窗口内发生；rx 存活覆盖全程，try_iter 排空即全集
    let locations: Vec<String> = rx.try_iter().collect();
    let hit = |line: u32| {
        locations
            .iter()
            .any(|l| l.contains(&format!("receive_panic_location.rs:{line}:")))
    };
    assert!(
        hit(empty_line),
        "队列空 panic 必须指调用行（receive_panic_location.rs:{empty_line}），实际 {locations:?}"
    );
    assert!(
        hit(mismatch_line),
        "mismatch panic 必须指调用行（receive_panic_location.rs:{mismatch_line}），实际 {locations:?}"
    );
}
