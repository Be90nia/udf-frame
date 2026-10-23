//! `PoolManaged` 契约测试：契约即可实现（toy 池十几行兑现全契约）+ 事前校验
//! 错误语义 + 共享核心形经 `Borrow` 可达。

use skeleton::pool::{
    PoolAuditEvent, PoolAuditKind, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
};
use std::borrow::Borrow;
use std::sync::Mutex;

/// 玩具池：互斥计数核心，无真实对象管理（特化面归实现方——约定不是锁定）。
struct ToyPool {
    core: Mutex<ToyCore>,
}

#[derive(Default)]
struct ToyCore {
    max: usize,
    idle: usize,
    borrowed: usize,
}

/// 三参封顶的玩具特化形：toy 只留一参（参数形状各池特化，基座不统一）。
struct ToyTuning {
    max: usize,
}

/// 指标组合扩展：核心形 `Borrow` 可达 + toy 无特有读数（直接用核心形亦可）。
struct ToyMetrics {
    core: PoolMetrics,
}

impl Borrow<PoolMetrics> for ToyMetrics {
    fn borrow(&self) -> &PoolMetrics {
        &self.core
    }
}

impl PoolManaged for ToyPool {
    type Tuning = ToyTuning;
    type Metrics = ToyMetrics;

    fn resize(&self, tuning: ToyTuning) -> Result<(), PoolResizeError> {
        if tuning.max == 0 {
            return Err(PoolResizeError::ZeroMax);
        }
        self.core.lock().expect("toy poisoned").max = tuning.max;
        Ok(())
    }

    fn metrics(&self) -> ToyMetrics {
        let core = self.core.lock().expect("toy poisoned");
        ToyMetrics {
            core: PoolMetrics {
                max: core.max,
                idle: core.idle,
                borrowed: core.borrowed,
                dropped_overflow: 0,
                watermark: core.borrowed as f64 / core.max.max(1) as f64,
            },
        }
    }

    fn subscribe_audit(&self, _f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket {
        PoolAuditTicket::new(|| {})
    }
}

#[test]
fn toy_implements_the_full_contract() {
    let pool = ToyPool {
        core: Mutex::default(),
    };
    // 事前校验：非法参数入口拒绝（fail-visible）。
    assert_eq!(
        pool.resize(ToyTuning { max: 0 }),
        Err(PoolResizeError::ZeroMax)
    );
    // 合法调参生效。
    pool.resize(ToyTuning { max: 4 }).expect("valid tuning");
    // 共享核心形经 Borrow 可达（组合扩展不挡统一读数）。
    let metrics = pool.metrics();
    let core: &PoolMetrics = metrics.borrow();
    assert_eq!(core.max, 4);
    assert_eq!((core.idle, core.borrowed, core.dropped_overflow), (0, 0, 0));
    assert_eq!(core.watermark, 0.0);
}

#[test]
fn resize_error_display_is_self_explanatory() {
    let e = PoolResizeError::IdleCapExceedsMax {
        idle_cap: 8,
        max: 4,
    };
    let msg = e.to_string();
    assert!(
        msg.contains("8") && msg.contains("4"),
        "display carries both figures: {msg}"
    );
    assert!(PoolResizeError::ZeroMax.to_string().contains("max"));
    assert!(PoolResizeError::IdleCapZero.to_string().contains("idle"));
}

#[test]
fn audit_vocabulary_is_value_semantics() {
    let ev = PoolAuditEvent {
        seq: 1,
        kind: PoolAuditKind::OverflowDropped,
    };
    let copy = ev;
    assert_eq!(copy, ev);
    assert_eq!(copy.seq, 1);
    assert_eq!(copy.kind, PoolAuditKind::OverflowDropped);
}
