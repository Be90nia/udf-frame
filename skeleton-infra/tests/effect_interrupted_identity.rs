#![cfg(feature = "runtime")]
//! issue#48：`EffectInterrupted` 事件的结构化效果身份（消费方可定位「**哪一
//! 个**效果处于 indeterminate 态」）。
//!
//! 钉住五件事：
//!
//! 1. **kind / id / ordinal**：同一 action 产出的多条效果，中断事件逐条报出
//!    种类（`Run`/`Cancel`）、逻辑句柄与在产出 action 输出序列中的 0-based 位置。
//! 2. **跨 action 续转归因**：A 产出的效果后缀在 A 的转发 panic 后留存，后来
//!    由 B 的 dispatch 触发续转并再次 panic ⇒ 事件的 `origin_action_seq` 仍是
//!    **A**（产出者），`dispatch_action_seq` 是 **B**（触发者），且
//!    `origin < dispatch`。
//! 3. **receipt 仍对应触发 action**：转发失败回执（`Failure`）的 `seq` 与
//!    事件的 `dispatch_action_seq` 逐字相等（回执不落到效果产出者头上）。
//! 4. **Emit 置前 ordinal 不错位**：`Emit` 不经解释器，但**占** ordinal ⇒
//!    其后的 `Run` 的 ordinal 是 1 而不是 0。
//! 5. **tracing 与审计事件同值**：桥输出的五个字段与 `AuditEvent` 一致。
//!
//! 判据用「事件面 + 回执文本」两处独立证据交叉核对，禁只看其一。

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::audit::{AuditEvent, AuditEventKind};
use skeleton_infra::runtime::SyncStore;

// ── 动作与效果形态 ──────────────────────────────────────────────────────

/// 动作词表：每种动作对应一种「产出效果序列」的形状。
#[derive(Clone, Debug)]
enum Act {
    /// 产 n 条 `Run` 效果（id = 10 + i，op = i）。
    Runs(u64),
    /// 产一条 `Run`（id = 10）后一条 `Cancel`（id = 20）。
    RunThenCancel,
    /// 先 `Emit`（不经解释器）后一条 `Run`（id = 30）。
    EmitThenRun,
    /// 不产效果。
    Noop,
    /// 占住排空权直到闸开（用于让另一线程的 dispatch 走「入队即等」路径）。
    Hold(Arc<Gate>),
}

/// 一次性闸（放行排空权持有者）。
#[derive(Debug)]
struct Gate {
    open: Mutex<bool>,
    cv: Condvar,
}

impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            open: Mutex::new(false),
            cv: Condvar::new(),
        })
    }

    /// 等待放行（持锁等待 ⇒ 无竞速窗口）。
    fn wait_open(&self) {
        let mut open = self.open.lock().expect("gate lock");
        while !*open {
            open = self.cv.wait(open).expect("gate wait");
        }
    }

    fn open(&self) {
        let mut open = self.open.lock().expect("gate lock");
        *open = true;
        self.cv.notify_all();
    }
}

/// 产出器：按动作形状把效果写进 reducer 出参位。
#[derive(Clone)]
struct Producer {
    /// `Hold` 动作已进入 reducer 的标记（供测试判定「排空权已被占住」）。
    entered: Arc<AtomicBool>,
}

impl Reducer for Producer {
    type State = u64;
    type Action = Act;
    type Op = u64;

    fn initial(&self) -> u64 {
        0
    }

    fn reduce(&self, state: &u64, action: Act, out: &mut EffectBuf<Act, u64>) -> u64 {
        match action {
            Act::Runs(n) => {
                for i in 0..n {
                    out.push(Effect::Run {
                        id: EffectId::new(10 + i),
                        op: i,
                    });
                }
            }
            Act::RunThenCancel => {
                out.push(Effect::Run {
                    id: EffectId::new(10),
                    op: 0,
                });
                out.push(Effect::Cancel {
                    id: EffectId::new(20),
                });
            }
            Act::EmitThenRun => {
                out.push(Effect::Emit(Act::Noop));
                out.push(Effect::Run {
                    id: EffectId::new(30),
                    op: 30,
                });
            }
            Act::Noop => {}
            Act::Hold(gate) => {
                self.entered.store(true, Ordering::Release);
                gate.wait_open();
            }
        }
        *state + 1
    }
}

/// 解释器：**每次调用都 panic**（注入故障）⇒ 每次转发只推进一条效果，
/// 其余留在已提交待转发队列（跨 action 存活）。
struct AlwaysPanic;

impl EffectRuntime<Act, u64> for AlwaysPanic {
    fn run(&mut self, _effect: Effect<Act, u64>, _back: &ActionSender<Act>) {
        panic!("interpreter detonation (issue#48 identity test)");
    }
}

// ── 辅助 ────────────────────────────────────────────────────────────────

/// 建店（订阅闸开 ⇒ 事件可取证）。
///
/// 返回的票据**必须由调用方持有到断言之后**——票据 Drop 即关闭事件流
/// （既有语义：`AuditTicket` 的 Drop 关闸清槽）。
fn new_store() -> (
    SyncStore<u64, Act, u64>,
    skeleton_infra::audit::AuditTicket,
    Arc<Gate>,
    Arc<AtomicBool>,
) {
    let gate = Gate::new();
    let entered = Arc::new(AtomicBool::new(false));
    let store = SyncStore::new(
        Producer {
            entered: Arc::clone(&entered),
        },
        AlwaysPanic,
    );
    let ticket = store.subscribe_events(64, |_| {});
    (store, ticket, gate, entered)
}

/// 取出全部中断事件（按时间序）。
fn interrupted(store: &SyncStore<u64, Act, u64>) -> Vec<AuditEvent> {
    store
        .take_events()
        .into_iter()
        .filter(|e| matches!(e.kind, AuditEventKind::EffectInterrupted { .. }))
        .collect()
}

/// 取出全部中断事件的身份五元组（顺序 = 时间序）。
type Identity = (u64, u64, u64, skeleton::EffectKind, Option<EffectId>);

/// 把事件流里的中断事件折成身份五元组。
fn identities(store: &SyncStore<u64, Act, u64>) -> Vec<Identity> {
    interrupted(store)
        .into_iter()
        .map(|e| match e.kind {
            AuditEventKind::EffectInterrupted {
                dispatch_action_seq,
                origin_action_seq,
                effect_index,
                effect_kind,
                effect_id,
            } => (
                dispatch_action_seq,
                origin_action_seq,
                effect_index,
                effect_kind,
                effect_id,
            ),
            other => panic!("非中断事件混入：{other:?}"),
        })
        .collect()
}

/// 从回执文本里取 `seq=` 后的数字（`udf-frame: SyncStore dispatch 失败——seq=N …`）。
fn receipt_seq(text: &str) -> u64 {
    let tail = text
        .split("seq=")
        .nth(1)
        .unwrap_or_else(|| panic!("回执文本缺 seq=：{text}"));
    tail.split_whitespace()
        .next()
        .unwrap_or_else(|| panic!("回执文本 seq= 后无数字：{text}"))
        .parse()
        .unwrap_or_else(|e| panic!("回执 seq 解析失败：{text}（{e:?}）"))
}

// ── ① 单 action 多条 Run/Cancel：kind / id / ordinal 逐条可辨 ───────────

/// 一个 action 产出 `Run` + `Cancel`：两次转发各报一条中断事件——事件的
/// `origin_action_seq` 同为产出者，`effect_index` 逐条递增（0/1），
/// `effect_kind` / `effect_id` 分别是 `Run`/id10 与 `Cancel`/id20。
#[test]
fn interrupted_event_reports_kind_id_and_ordinal_per_effect() {
    let (store, _ticket, _gate, _entered) = new_store();
    // 第一次 dispatch：产出 [Run(id=10), Cancel(id=20)]，转发第一条即 panic。
    let first = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::RunThenCancel)));
    assert!(first.is_err(), "解释器 panic 必须原样上浮（fail-visible）");
    // 第二次 dispatch：续转残留的 Cancel（产出者仍是第一个 action）。
    let second = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Noop)));
    assert!(second.is_err(), "续转时解释器 panic 必须原样上浮");

    let ids = identities(&store);
    assert_eq!(ids.len(), 2, "两次转发 ⇒ 恰好两条中断事件");
    let (dispatch1, origin1, index1, kind1, id1) = ids[0];
    assert_eq!(kind1, skeleton::EffectKind::Run, "第一条是 Run");
    assert_eq!(id1, Some(EffectId::new(10)), "Run 的逻辑句柄");
    assert_eq!(index1, 0, "Run 是产出 action 输出序列的第 0 条");
    // 单 action 场景：产出者与触发者同一个 action ⇒ 两个序号相等。
    assert_eq!(
        origin1, dispatch1,
        "单 action 场景：产出者与触发者是同一个 action"
    );

    let (dispatch2, origin2, index2, kind2, id2) = ids[1];
    assert_eq!(kind2, skeleton::EffectKind::Cancel, "第二条是 Cancel");
    assert_eq!(id2, Some(EffectId::new(20)), "Cancel 携带其目标 EffectId");
    assert_eq!(index2, 1, "Cancel 是产出 action 输出序列的第 1 条");
    assert_eq!(origin2, origin1, "两条效果同出一个 action ⇒ 来源序号相同");
    assert!(
        dispatch2 > origin2,
        "第二条由后续 dispatch 触发 ⇒ 触发序号 > 来源序号（续转跨 action）"
    );
}

// ── ② 跨 action 续转归因（核心闸）────────────────────────────────────

/// **跨 action 续转**：A（`Runs(2)`）的转发在第一条即 panic ⇒ 第二条留在已
/// 提交待转发队列；后来 B 的 dispatch 触发续转 ⇒ 第二条再次 panic。
///
/// 断言：第二条中断事件的 `origin_action_seq` == A 的序号（**产出者**）、
/// `dispatch_action_seq` == B 的序号（**触发者**）、`origin < dispatch`，
/// 且 ordinal / id 指向 A 产出的**第 1 条**效果。
#[test]
fn interrupted_event_attributes_origin_to_producing_action_across_actions() {
    let (store, _ticket, _gate, _entered) = new_store();
    // A：产 2 条 Run，转发第一条即 panic（第二条留存于已提交待转发队列）。
    let a = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Runs(2))));
    assert!(a.is_err(), "A 的转发 panic 必须原样上浮");
    // B：不产效果，只触发续转（转发 A 留存的那条）。
    let b = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Noop)));
    assert!(b.is_err(), "续转时解释器 panic 必须原样上浮");

    let ids = identities(&store);
    assert_eq!(ids.len(), 2, "A 一次 + 续转一次 ⇒ 两条中断事件");
    let (dispatch_a, origin_a, index_a, _, id_a) = ids[0];
    assert_eq!(index_a, 0, "A 的第 0 条效果被中断");
    assert_eq!(id_a, Some(EffectId::new(10)), "A 的第 0 条 Run");
    assert_eq!(
        origin_a, dispatch_a,
        "A 内转发 ⇒ 产出者与触发者同一个 action"
    );

    let (dispatch_b, origin_b, index_b, kind_b, id_b) = ids[1];
    assert_eq!(
        origin_b, dispatch_a,
        "续转事件的来源仍是**产出者 A**（不是触发续转的 B）"
    );
    assert!(
        dispatch_b > origin_b,
        "跨 action 续转：触发者序号 > 产出者序号（入队序号单调）"
    );
    assert_eq!(kind_b, skeleton::EffectKind::Run, "被中断效果是 Run");
    assert_eq!(
        id_b,
        Some(EffectId::new(11)),
        "被中断效果是 A 的第 1 条 Run"
    );
    assert_eq!(
        index_b, 1,
        "ordinal 是**产出 action**输出序列中的位置（A 的第 1 条）"
    );
}

// ── ③ receipt 仍对应触发 action ──────────────────────────────────────

/// 转发失败回执的 `seq` 必须是**触发转发**的那个 action（承接失败者），与事件
/// 的 `dispatch_action_seq` 逐字相等——回执不落到别的 action 头上。
///
/// 编排：线程 H 的 `Hold` 占住排空权（reducer 内等闸）⇒ 线程 W 的 dispatch
/// 只入队并等待自己的完成边界 ⇒ H 放闸后转发 W 的效果并 panic ⇒ W 收到
/// 「dispatch 失败」回执（`seq=` + `phase=ForwardingEffects`）。
#[test]
fn forwarding_failure_receipt_seq_matches_interrupted_event_dispatch_seq() {
    let (store, _ticket, gate, entered) = new_store();
    let store = Arc::new(store);

    // 线程 H：dispatch 一个 `Hold` 动作占住排空权。
    let store_h = Arc::clone(&store);
    let gate_h = Arc::clone(&gate);
    let handle_h = thread::spawn(move || {
        let _ = catch_unwind(AssertUnwindSafe(|| store_h.dispatch(Act::Hold(gate_h))));
    });
    // 等 `Hold` 确实进入 reducer（标志位 + 有界自旋 ⇒ 无 sleep 竞速）。
    let mut spins = 0u32;
    while !entered.load(Ordering::Acquire) {
        assert!(spins < 10_000_000, "Hold 动作未进入 reducer（闸编排失败）");
        spins += 1;
        thread::yield_now();
    }

    // 线程 W：dispatch 产一条 Run 的动作 ⇒ 排空权被占 ⇒ 只入队并等回执。
    let store_w = Arc::clone(&store);
    let handle_w = thread::spawn(move || {
        catch_unwind(AssertUnwindSafe(|| store_w.dispatch(Act::Runs(1))))
            .err()
            .map(|payload| {
                if let Some(text) = payload.downcast_ref::<String>() {
                    text.clone()
                } else if let Some(text) = payload.downcast_ref::<&str>() {
                    (*text).to_string()
                } else {
                    "<非字符串 panic 载荷>".to_string()
                }
            })
    });

    // 等 W 的 action **确实入队**（队列深度 ≥ 1）⇒ 放闸后由 H 的排空环接手
    // （否则闸早开 ⇒ W 自己排空 ⇒ 拿到的是解释器原始 panic 而非失败回执）。
    let mut spins = 0u32;
    while store.pending_actions() < 1 {
        assert!(spins < 10_000_000, "等待者的 action 未入队（闸编排失败）");
        spins += 1;
        thread::yield_now();
    }

    // 放闸 ⇒ H 的排空环接手：转发 W 的效果 ⇒ 解释器 panic ⇒ W 收到回执。
    gate.open();
    let _ = handle_h.join();
    let receipt = handle_w
        .join()
        .expect("W 线程 join 失败")
        .expect("转发 panic 的 dispatch 必须以失败回执收场");
    assert!(
        receipt.contains("ForwardingEffects"),
        "失败回执的阶段必须是效果转发：{receipt}"
    );
    assert!(
        receipt.contains("state_committed=true"),
        "转发失败发生在状态提交之后 ⇒ 回执标注 state_committed=true：{receipt}"
    );

    let ids = identities(&store);
    assert_eq!(ids.len(), 1, "恰好一条中断事件");
    let (dispatch_seq, _origin, _index, _kind, _id) = ids[0];
    assert_eq!(
        receipt_seq(&receipt),
        dispatch_seq,
        "回执的 seq 必须与事件的 dispatch_action_seq 逐字相等（receipt 仍对应触发 action）"
    );
}

// ── ④ Emit 置前：ordinal 不错位 ───────────────────────────────────────

/// `Emit` 不经解释器（不产生中断事件），但**占** ordinal ⇒ 其后的 `Run` 的
/// `effect_index` 必须是 1（不是 0）。
#[test]
fn emit_before_run_does_not_shift_ordinal() {
    let (store, _ticket, _gate, _entered) = new_store();
    let result = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::EmitThenRun)));
    assert!(result.is_err(), "解释器 panic 必须原样上浮");

    let ids = identities(&store);
    assert_eq!(ids.len(), 1, "Emit 不经解释器 ⇒ 只有 Run 报中断");
    let (_dispatch, _origin, index, kind, id) = ids[0];
    assert_eq!(index, 1, "Emit 占 ordinal ⇒ 其后的 Run 是第 1 条（不错位）");
    assert_eq!(kind, skeleton::EffectKind::Run, "被中断效果是 Run");
    assert_eq!(id, Some(EffectId::new(30)), "被中断效果的逻辑句柄");
}

// ── ⑤ Cancel 中断也携带其目标 EffectId ───────────────────────────────

/// `Cancel` 中断时事件携带**其目标** `EffectId`（`Cancel` 自己的 id），
/// 且 kind 必须是 `Cancel`（与同批的 `Run` 可区分）。
#[test]
fn cancel_interruption_carries_its_target_effect_id() {
    let (store, _ticket, _gate, _entered) = new_store();
    // 一个 action 产 [Run(id=10), Cancel(id=20)]：两次转发各报一条。
    let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::RunThenCancel)));
    let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Noop)));

    let ids = identities(&store);
    assert_eq!(ids.len(), 2, "Run 与 Cancel 各报一条中断事件");
    assert_eq!(ids[0].3, skeleton::EffectKind::Run, "第一条是 Run");
    assert_eq!(ids[0].4, Some(EffectId::new(10)), "Run 的逻辑句柄");
    assert_eq!(ids[1].3, skeleton::EffectKind::Cancel, "第二条是 Cancel");
    assert_eq!(
        ids[1].4,
        Some(EffectId::new(20)),
        "Cancel 中断携带其目标 EffectId"
    );
}

// ── ⑥ tracing 桥与审计事件同值 ───────────────────────────────────────

/// tracing 桥输出的五个字段必须与 `AuditEvent` 同源同值（消费方按 tracing
/// 过滤时不必再回查审计缓冲）。
#[test]
#[cfg(feature = "tracing")]
fn tracing_bridge_reports_same_identity_fields_as_audit_event() {
    /// fmt 订阅者写捕集器（测试专件：共享内存缓冲收原始输出）。
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("capture buffer lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for Capture {
        type Writer = Capture;
        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    let (store, _ticket, _gate, _entered) = new_store();
    let buf = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(Capture(Arc::clone(&buf)))
        .finish();
    let _ticket = skeleton_infra::tracingx::bridge_events_to_tracing(&store, 64);
    let result = tracing::subscriber::with_default(subscriber, || {
        catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Runs(1))))
    });
    assert!(result.is_err(), "解释器 panic 必须原样上浮");

    let out = String::from_utf8(buf.lock().expect("capture buffer lock").clone())
        .expect("fmt 输出恒 UTF-8");
    let ids = identities(&store);
    assert_eq!(ids.len(), 1, "恰好一条中断事件");
    let (dispatch_seq, origin_seq, index, kind, id) = ids[0];
    assert!(
        out.contains("kind=\"effect_interrupted\""),
        "桥必须转译中断节点：{out}"
    );
    assert!(
        out.contains(&format!("dispatch_action_seq={dispatch_seq}")),
        "桥的 dispatch_action_seq 与事件一致：{out}"
    );
    assert!(
        out.contains(&format!("origin_action_seq={origin_seq}")),
        "桥的 origin_action_seq 与事件一致：{out}"
    );
    assert!(
        out.contains(&format!("effect_index={index}")),
        "桥的 effect_index 与事件一致：{out}"
    );
    assert!(
        out.contains(&format!("effect_kind={kind:?}")),
        "桥的 effect_kind 与事件一致：{out}"
    );
    // 桥输出 `effect_id` 的**数值形态**（`Some(u64)`；None 时为 `None`）——
    // 与事件的 `Option<EffectId>` 同值不同形（EffectId 的 Debug 是 `EffectId(10)`）。
    let id_num = id.map(|i| i.get());
    assert!(
        out.contains(&format!("effect_id={id_num:?}")),
        "桥的 effect_id 与事件一致：{out}"
    );
}

// ── ⑦ 既有语义不变（回归）：环形缓冲溢出 + 订阅关闭 ────────────────────

/// 有界环溢出丢最旧（`seq` 留空洞）、订阅票据 Drop 即关流——既有语义不变。
#[test]
fn audit_ring_overflow_and_subscription_close_semantics_unchanged() {
    let store = SyncStore::new(
        Producer {
            entered: Arc::new(AtomicBool::new(false)),
        },
        AlwaysPanic,
    );
    {
        let _ticket = store.subscribe_events(2, |_| {});
        for _ in 0..4 {
            let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Runs(1))));
        }
        let events = store.take_events();
        assert!(
            events.len() <= 2,
            "环形缓冲容量 2 ⇒ 取出不超过 2 条（溢出丢最旧）：{}",
            events.len()
        );
        let seqs: Vec<u64> = events.iter().map(|e| e.seq).collect();
        assert!(
            seqs.windows(2).all(|w| w[0] < w[1]),
            "留存事件按事件序号递增：{seqs:?}"
        );
    }
    // 票据已 Drop ⇒ 事件流关闭 ⇒ 取不到任何事件。
    assert!(
        store.take_events().is_empty(),
        "票据 Drop 后事件流关闭（既有语义）"
    );
    let _ = catch_unwind(AssertUnwindSafe(|| store.dispatch(Act::Runs(1))));
    assert!(
        store.take_events().is_empty(),
        "关闭后不再记录事件（既有语义）"
    );
}
