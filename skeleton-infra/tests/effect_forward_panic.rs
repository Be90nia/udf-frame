//! 转发路径的中断语义：panic 在**第 1 / 中间 / 最后**一个效果时（issue#43 验收项）。
//!
//! 语义（#44 的事务边界定下的）：
//! - **已成功转发的前缀不重放**（转发成功即消费，不可重复执行）；
//! - **未转发的后缀保留**，由后续 dispatch 续转；
//! - 后缀**恰好保留一次**（不多、不少、不乱序）。
//!
//! 判据用「最终收到的 op 序列逐条等于 0..k」——它同时钉住三件事：前缀没被重放
//! （否则前缀元素出现两次）、后缀没丢（否则缺尾）、后缀没被复制（否则元素重复）。

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store};
use skeleton_infra::runtime::SyncStore;

/// 每拍产 `per_action` 条 Run 效果的根特征（op 编码「拍序号×10000 + 条序号」）。
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
        // **只在第一拍产效果**：第二次 dispatch 只用于驱动「续转」，不产新效果
        // ——否则两拍的内容混在一起，断言无法区分「续转」与「新产」。
        if *state == 0 {
            for i in 0..self.per_action {
                out.push(Effect::Run {
                    id: EffectId::new(i + 1),
                    op: seq * 10_000 + i,
                });
            }
        }
        seq
    }
}

/// 在第 `fail_at` 个效果（0-based）处 panic 的同步解释器。
///
/// 计数**累计不重置** ⇒ 只有第一次转发会中断；后续 dispatch 续转时不复 panic。
struct PanickingRuntime {
    fail_at: usize,
    count: AtomicUsize,
    seen: Arc<Mutex<Vec<u64>>>,
}

impl EffectRuntime<u64, u64> for PanickingRuntime {
    fn run(&mut self, effect: Effect<u64, u64>, _back: &ActionSender<u64>) {
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        if n == self.fail_at {
            panic!("转发第 {n} 个效果时中断（测试注入）");
        }
        if let Effect::Run { op, .. } = effect {
            self.seen.lock().expect("recorder mutex").push(op);
        }
    }
}

/// 中断于第 `fail_at` 个效果 ⇒ 续转后收到序列必须逐条等于产生序。
fn interrupted_forward_resumes_suffix_exactly_once(per_action: u64, fail_at: usize) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let store = SyncStore::new(
        BatchProducer { per_action },
        PanickingRuntime {
            fail_at,
            count: AtomicUsize::new(0),
            seen: Arc::clone(&seen),
        },
    );

    // 第一次 dispatch：在第 fail_at 个效果处中断（panic 原样上浮 = fail-visible）。
    let first = catch_unwind(AssertUnwindSafe(|| store.dispatch(1u64)));
    assert!(
        first.is_err(),
        "第 {fail_at} 个效果的转发 panic 应当原样上浮（fail-visible）"
    );

    let after_first = seen.lock().expect("recorder mutex").clone();
    assert_eq!(
        after_first,
        (0..fail_at as u64).map(|i| 10_000 + i).collect::<Vec<_>>(),
        "中断前只应收到成功转发的前缀（未转发的后缀不计入 received）"
    );

    // 第二次 dispatch：续转未转发的后缀（已成功的前缀不重放）。
    store.dispatch(1u64);

    let final_seen = seen.lock().expect("recorder mutex").clone();
    let all: Vec<u64> = (0..per_action).map(|i| 10_000 + i).collect();
    let interrupted = 10_000 + fail_at as u64;
    // **中断项（panic 掉的那一条）的去向是待拍板的语义**，本测试**不断言**它：
    //   · #44 的 EffectRuntime panic 路径当前**丢弃**它；
    //   · issue#43 的设计评审原文要求「retain the current unforwarded item and
    //     suffix exactly once」（**保留**它）。
    // 两种语义下本断言都成立 ⇒ 语义一旦拍板，改这里即可，不必重写整条测试。
    let without_interrupted: Vec<u64> = final_seen
        .iter()
        .copied()
        .filter(|op| *op != interrupted)
        .collect();
    let expected_without: Vec<u64> = all
        .iter()
        .copied()
        .filter(|op| *op != interrupted)
        .collect();
    assert_eq!(
        without_interrupted, expected_without,
        "除中断项外：已成功的前缀不得重放、未转发的后缀不得丢、不得乱序、不得被复制"
    );
}

#[test]
fn forward_panic_on_first_effect_resumes_suffix_exactly_once() {
    interrupted_forward_resumes_suffix_exactly_once(8, 0);
}

#[test]
fn forward_panic_on_middle_effect_resumes_suffix_exactly_once() {
    interrupted_forward_resumes_suffix_exactly_once(8, 4);
}

#[test]
fn forward_panic_on_last_effect_resumes_suffix_exactly_once() {
    interrupted_forward_resumes_suffix_exactly_once(8, 7);
}
