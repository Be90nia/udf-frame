//! 计数器示例：计数器 feature（Inc/Dec/Undo + 订阅者断言 + Emit 回灌小例）。
//!
//! 三个任务面：
//! 1. 计数器 Reducer：State={count, undo_log}，Op=Infallible（零副作用证明）
//! 2. 订阅者：记录每次通知的 count，断言每次状态变更都到达、终值正确
//! 3. Emit 回灌：Undo 触发 reducer 内派生 Replay(-d) 后续 Action——链长有界（Undo→Replay 即止，Replay 不再产出效果，不成环）

use skeleton::{Effect, EffectBuf, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};

/// State：计数值 + 撤销日志（每条 Inc/Dec 记 ±1 增量）。
/// 任务字面「State=i32」与可撤销语义不可兼得——撤销需要历史，纯函数纪律下
/// 历史只能进 State（reducer 禁写共享态）。故 State 是含 i32 的小结构体。
#[derive(Clone, Debug, PartialEq)]
struct CounterState {
    count: i32,
    undo_log: Vec<i32>,
}

/// Action 词表。前三 = 任务要求；`Replay` 是 Undo 经 Emit 派生的内部应用步：
/// 若 Undo 直接回灌 Inc/Dec，该步会被当普通操作记入 undo_log——连撤两次会退化
/// 成 redo（第一次撤回、第二次重做），违反「撤销上一次」契约。故派生步必须
/// 与用户操作类型可区分（非法状态进类型：日志污染无处构造）。
enum CounterAction {
    Inc,
    Dec,
    Undo,
    /// 应用增量、不入 undo_log（Undo 专用续步）。
    Replay(i32),
}

struct CounterFeature;

impl Reducer for CounterFeature {
    type State = CounterState;
    type Action = CounterAction;
    type Op = Infallible;

    fn initial(&self) -> CounterState {
        CounterState {
            count: 0,
            undo_log: Vec::new(),
        }
    }

    fn reduce(
        &self,
        state: &CounterState,
        action: CounterAction,
        out: &mut EffectBuf<CounterAction, Infallible>,
    ) -> CounterState {
        match action {
            CounterAction::Inc => CounterState {
                count: state.count + 1,
                undo_log: state
                    .undo_log
                    .as_slice()
                    .iter()
                    .copied()
                    .chain([1])
                    .collect(),
            },
            CounterAction::Dec => CounterState {
                count: state.count - 1,
                undo_log: state
                    .undo_log
                    .as_slice()
                    .iter()
                    .copied()
                    .chain([-1])
                    .collect(),
            },
            CounterAction::Undo => match state.undo_log.as_slice() {
                // 有账可撤：弹一条，派生 Replay(-d) 回灌应用——本帧不改 count。
                // 链有界：Replay 分支零效果产出，环在此终止（不失控）。
                [log @ .., d] => {
                    out.push(Effect::Emit(CounterAction::Replay(-d)));
                    CounterState {
                        count: state.count,
                        undo_log: log.to_vec(),
                    }
                }
                // 空账：无事可撤，零效果 no-op（否则 Undo→Undo 死环）。
                [] => state.clone(),
            },
            CounterAction::Replay(d) => CounterState {
                count: state.count + d,
                undo_log: state.undo_log.clone(),
            },
        }
    }
}

fn main() {
    let store = Store::new(CounterFeature, NoFx);

    // 订阅者：记录每帧通知的 count（Drop=注销，票据须存活到断言后）
    let notified: Arc<Mutex<Vec<i32>>> = Arc::new(Mutex::new(Vec::new()));
    let sub_log = Arc::clone(&notified);
    let _sub = store.subscribe(move |s: &CounterState| {
        sub_log.lock().unwrap().push(s.count);
    });

    // —— 剧本：Inc×3 → Dec → 撤销 Dec → Inc → 撤销它（= 任务示例「Undo 触发 Dec」）→ 排空账 → 空账 Undo ——
    for _ in 0..3 {
        store.dispatch(CounterAction::Inc);
    }
    store.dispatch(CounterAction::Dec);
    store.dispatch(CounterAction::Undo); // 撤 Dec：Replay(+1) 回灌 → 3
    store.dispatch(CounterAction::Inc);
    store.dispatch(CounterAction::Undo); // 撤 Inc：Replay(-1) 回灌 → 3
    while !store.snapshot().undo_log.is_empty() {
        store.dispatch(CounterAction::Undo); // 连撤三次余下 Inc → 归零
    }
    store.dispatch(CounterAction::Undo); // 空账 Undo：no-op，零派生

    // —— 断言 1：终值 ——
    let final_state = store.snapshot();
    assert_eq!(final_state.count, 0, "连撤到底应归零");
    assert!(final_state.undo_log.is_empty(), "撤销账应排空");
    assert_eq!(
        *final_state,
        CounterState {
            count: 0,
            undo_log: vec![]
        }
    );

    // —— 断言 2：每次状态变更都有通知到达，且通知终值 = 快照终值 ——
    let frames = notified.lock().unwrap().clone();
    let mut walk: Vec<i32> = Vec::new(); // 连续同值折叠 = 实际状态变更序列
    for v in &frames {
        if walk.last() != Some(v) {
            walk.push(*v);
        }
    }
    assert_eq!(
        walk,
        vec![1, 2, 3, 2, 3, 4, 3, 2, 1, 0],
        "每次真实状态变更都应有一条通知（含 Undo 中间帧与 Replay 派生帧）"
    );
    assert_eq!(
        *frames.last().unwrap(),
        final_state.count,
        "通知终值应等于快照终值"
    );

    // —— 断言 3：Emit 回灌链的逐帧序列（通知点=每 action 处理后，可能见链中间态帧；
    //    Undo 帧可见 count 未变、仅账本弹出的中间态）——
    assert_eq!(
        frames,
        vec![1, 2, 3, 2, 2, 3, 4, 4, 3, 3, 2, 2, 1, 1, 0, 0],
        "逐帧：…2,2(Undo 中间帧),3(Replay)… 16 帧 = 16 次被处理的 action"
    );

    println!(
        "ALL ASSERTIONS PASSED: count=0, {} notifications, emit-chain bounded (Undo→Replay→∅)",
        frames.len()
    );
}
