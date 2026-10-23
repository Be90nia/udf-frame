//! 骨架审计事件流 → tracing 桥。
//!
//! 核心零原生输出、宿主无库内 hook/span 可挂——收口形态：核心出审计事件流
//! （[`crate::runtime::SyncStore::subscribe_events`]），本桥一行接线把每事件转
//! `tracing::event!`（target `skeleton::audit`，INFO 级）。核心 crate 保持零
//! 依赖：桥在本层持 tracing 依赖（feature 门控，tokio/log 先例形态）；审计值面
//! 在本 crate `audit` 域，实现走 `runtime::SyncStore`。
//!
//! # Examples
//!
//! ```
//! use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
//! use skeleton_infra::runtime::SyncStore;
//! use skeleton_infra::tracingx;
//!
//! struct Step;
//! impl Reducer for Step {
//!     type State = u32;
//!     type Action = u32;
//!     type Op = Infallible;
//!     fn initial(&self) -> u32 { 0 }
//!     fn reduce(&self, s: &u32, _a: u32, _o: &mut EffectBuf<u32>) -> u32 { s + 1 }
//! }
//!
//! let store = SyncStore::new(Step, NoFx);
//! let _ticket = tracingx::bridge_events_to_tracing(&store, 64);
//! store.dispatch(1); // → tracing::event!（target "skeleton::audit"）
//! # store.close();
//! ```

use crate::audit::{AuditEvent, AuditEventKind, AuditTicket};
use crate::runtime::SyncStore;

/// 把 `store` 的审计事件流桥接到 tracing：每事件一条 `tracing::event!`
/// （target `skeleton::audit`，INFO 级），字段 `seq`（单调序号）/ `kind`
/// （生命周期节点名）/ `depth`（仅 Enqueued 事件携带回灌深度）。
/// `capacity` 语义同 [`SyncStore::subscribe_events`]；返回票据 Drop =
/// 停桥（零开销恢复）。
///
/// 性能锚（2026-10-16 批 C 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-perf-batchc.md）：订阅态 dispatch 全含 ~142ns（无订阅
/// ~77）中，订阅分发本体 ~63-72ns、本桥净转译 ~10ns/事件（八分支 match +
/// `event!` 兴趣缓存，无订阅者零输出面）、有订阅者实收再 +~11ns（计数订阅
/// 者字段记录下限，全含 ~21ns）——桥常挂的成本集中在宿主订阅者本体。
pub fn bridge_events_to_tracing<
    S: Clone + Send + Sync + 'static,
    A: Send + 'static,
    Op: Send + 'static,
>(
    store: &SyncStore<S, A, Op>,
    capacity: usize,
) -> AuditTicket {
    store.subscribe_events(capacity, emit)
}

/// 单事件转译：八节点八分支，`kind` 字段值即节点名（过滤/聚合的稳定键）。
fn emit(event: &AuditEvent) {
    let seq = event.seq;
    match event.kind {
        AuditEventKind::Enqueued { feedback_depth } => {
            let kind = "enqueued";
            let depth = feedback_depth;
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind, depth);
        }
        AuditEventKind::Reduced => {
            let kind = "reduced";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::ReducerInterrupted => {
            let kind = "reducer_interrupted";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::EffectForwarded => {
            let kind = "effect_forwarded";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::EffectInterrupted => {
            let kind = "effect_interrupted";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::Notified => {
            let kind = "notified";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::Rejected => {
            let kind = "rejected";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
        AuditEventKind::FeedbackBudgetExceeded => {
            let kind = "feedback_budget_exceeded";
            tracing::event!(target: "skeleton::audit", tracing::Level::INFO, seq, kind);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::SyncStore as Store;
    use skeleton::{
        ActionSender, Effect, EffectBuf, EffectRuntime, Infallible, NoFx, Reducer, Store as _,
    };
    use std::sync::{Arc, Mutex};

    enum Tick {
        Tick,
    }

    struct Step;

    impl Reducer for Step {
        type State = u64;
        type Action = Tick;
        type Op = Infallible;
        fn initial(&self) -> u64 {
            0
        }
        fn reduce(&self, s: &u64, _: Tick, _o: &mut EffectBuf<Tick>) -> u64 {
            s + 1
        }
    }

    /// fmt 订阅者写捕集器：共享内存缓冲收原始输出（本测试单线程写）。
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Capture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            // 偏差申报：tracing-subscriber 生态件锁形态，本 crate 零新依赖纪律下
            // 不可换无锁写；测试专件，锁竞争不存在。
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

    /// 桥的端到端实证：dispatch 一笔 → fmt 订阅者捕获的输出含 target、节点名、
    /// 深度与单调序号——「核心零原生输出」收口的证据。
    #[test]
    fn bridge_emits_lifecycle_events_to_tracing() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(Capture(Arc::clone(&buf)))
            .finish();
        let store = Store::new(Step, NoFx);
        let ticket = bridge_events_to_tracing(&store, 64);
        tracing::subscriber::with_default(subscriber, || {
            store.dispatch(Tick::Tick);
        });
        drop(ticket); // 停桥，防测试间残留
        let out = String::from_utf8(buf.lock().expect("capture buffer lock").clone())
            .expect("fmt 输出恒 UTF-8");
        assert!(out.contains("skeleton::audit"), "target 必须可过滤：{out}");
        assert!(out.contains("kind=\"enqueued\""), "入队事件：{out}");
        assert!(out.contains("depth=0"), "外部流量深 0：{out}");
        assert!(out.contains("kind=\"reduced\""), "reduce 完成：{out}");
        assert!(out.contains("seq=1"), "单调序号入字段：{out}");
    }

    enum DrillOp {
        Run,
    }

    struct DrillFeature;

    impl Reducer for DrillFeature {
        type State = u32;
        type Action = Tick;
        type Op = DrillOp;
        fn initial(&self) -> u32 {
            0
        }
        fn reduce(&self, s: &u32, _: Tick, out: &mut EffectBuf<Tick, DrillOp>) -> u32 {
            out.push(Effect::Run {
                id: skeleton::EffectId(1),
                op: DrillOp::Run,
            });
            s + 1
        }
    }

    /// 解释器 panic 配方：Run 一到即 panic（传播面不由桥改变）。
    struct BoomFx;

    impl EffectRuntime<Tick, DrillOp> for BoomFx {
        fn run(&mut self, _: Effect<Tick, DrillOp>, _: &ActionSender<Tick>) {
            panic!("interpreter drill");
        }
    }

    /// 解释器 panic 路径的桥转译：EffectInterrupted 节点同样入 tracing，
    /// panic 本身照常传播（观测面禁改变被观测者）。
    #[test]
    fn bridge_emits_effect_interrupted_on_interpreter_panic() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_writer(Capture(Arc::clone(&buf)))
            .finish();
        let store = Store::new(DrillFeature, BoomFx);
        let _ticket = bridge_events_to_tracing(&store, 64);
        let panicked = tracing::subscriber::with_default(subscriber, || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.dispatch(Tick::Tick)))
        })
        .is_err();
        assert!(panicked, "解释器 panic 必须照常传播（桥零吞）");
        let out = String::from_utf8(buf.lock().expect("capture buffer lock").clone())
            .expect("fmt 输出恒 UTF-8");
        assert!(
            out.contains("kind=\"effect_interrupted\""),
            "中断节点必须入桥：{out}"
        );
    }
}
