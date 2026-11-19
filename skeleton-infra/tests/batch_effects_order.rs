//! 批量效果转发的**条数与顺序**（issue#43 的端到端锚）。
//!
//! 一个 reducer 每拍产 k 条 `Effect::Run`（op 编码「拍序号×10000 + 条序号」），
//! 同步解释器按到达序记录收到的 op。断言：收到的序列**逐条等于**产生序
//! （0..k 的偏移后缀）⇒ 条数不多、不少、不乱序。
//!
//! 这条测试钉的是「效果缓冲的存储表示换了（`Vec` → 前向可读环形缓冲）之后，
//! 转发的语义与顺序不变」——#43 改的是**表示**，不是语义。
//!
//! 既有测试只覆盖 1–2 条效果的路径；「k 条效果」这一面此前无覆盖。

use std::sync::{Arc, Mutex};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store};
use skeleton_infra::runtime::SyncStore;

/// 每拍产 `per_action` 条 Run 效果的根特征。
struct BatchProducer {
    per_action: u64,
}

impl Reducer for BatchProducer {
    type State = u64;
    type Action = u64;
    type Op = u64;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(
        &self,
        state: &Self::State,
        _action: Self::Action,
        out: &mut EffectBuf<Self::Action, Self::Op>,
    ) -> Self::State {
        let seq = *state + 1;
        for i in 0..self.per_action {
            // op 编码拍序号与条序号 ⇒ 收到序列可逐条比对（否则同拍的 op 全同，验不出顺序）。
            out.push(Effect::Run {
                id: EffectId::new(i + 1),
                op: seq * 10_000 + i,
            });
        }
        seq
    }
}

/// 同步解释器：记录每条 Run 效果的到达序。
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
}

impl EffectRuntime<u64, u64> for Recorder {
    fn run(&mut self, effect: Effect<u64, u64>, _back: &ActionSender<u64>) {
        if let Effect::Run { op, .. } = effect {
            self.seen.lock().expect("recorder mutex").push(op);
        }
    }
}

fn batch_roundtrip(per_action: u64) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        BatchProducer { per_action },
        Recorder {
            seen: Arc::clone(&seen),
        },
    );

    store.dispatch(1u64);

    let seen = seen.lock().expect("recorder mutex");
    let expected: Vec<u64> = (0..per_action).map(|i| 10_000 + i).collect();
    assert_eq!(
        *seen, expected,
        "一条 action 产 {per_action} 条效果 ⇒ 转发序列必须逐条等于产生序（不多、不少、不乱序）"
    );
}

#[test]
fn batch_effects_forward_keeps_count_and_order() {
    // 批量规模覆盖：单条、少量、中量、大量（对应 #43 的 O(k²) → O(k) 改善面）。
    for per_action in [1u64, 4, 64, 1024] {
        batch_roundtrip(per_action);
    }
}
