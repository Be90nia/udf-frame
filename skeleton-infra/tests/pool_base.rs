#![cfg(feature = "pool")]
//! PoolBase 公共逻辑端到端：四件逐件断言（resize 校验拒绝 / 水位线读数 /
//! 审计事件发射 seq 单调 / dropped_overflow 计数）+ 骨架审计纪律（回调 panic
//! 摘除留痕 / 回调重入封死 / 票据关流与代次对账 / 容量环有界）。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use skeleton::pool::{PoolAuditKind, PoolResizeError};
use skeleton_infra::pool::{PoolBase, AUDIT_RING_DEFAULT_CAP};

// ── 四件之一：resize 事前校验，非法参数入口拒绝，拒绝不改状态 ──────────

#[test]
fn resize_rejects_illegal_params_and_keeps_state() {
    let pool = PoolBase::new(4, 2);
    // max == 0：恒不可借出=池语义自毁。
    assert_eq!(pool.resize(0, 2), Err(PoolResizeError::ZeroMax));
    // idle_cap > max：min>max 形非法参数。
    assert_eq!(
        pool.resize(4, 8),
        Err(PoolResizeError::IdleCapExceedsMax {
            idle_cap: 8,
            max: 4
        })
    );
    // idle_cap == 0：永不留存=池语义自毁。
    assert_eq!(pool.resize(4, 0), Err(PoolResizeError::IdleCapZero));
    // 三连拒绝不改任何状态：原读数原样（idle 计数从 0 起，2 是上限不是计数）。
    let m = pool.metrics();
    assert_eq!((m.max, m.idle, m.borrowed), (4, 0, 0));
    assert_eq!(m.dropped_overflow, 0);
    // 合法 resize（含缩容）即刻生效。
    pool.resize(2, 1).expect("legal resize");
    assert_eq!(pool.metrics().max, 2);
}

// ── 四件之二：水位线读数随借还如实移动 ────────────────────────────────

#[test]
fn watermark_readings_track_borrow_and_return() {
    let pool = PoolBase::new(4, 2);
    pool.note_borrow();
    pool.note_borrow();
    pool.note_borrow();
    let m = pool.metrics();
    assert_eq!((m.max, m.borrowed, m.idle), (4, 3, 0));
    assert_eq!(m.dropped_overflow, 0);
    assert!(
        (m.watermark - 0.75).abs() < 1e-12,
        "watermark = borrowed/max = 3/4, got {}",
        m.watermark
    );
    // 归还（留存路径：闲槽有余）→ 水位线回落。
    assert!(pool.note_return(), "kept: room under idle cap and depth");
    let m = pool.metrics();
    assert_eq!((m.borrowed, m.idle), (2, 1));
    assert!(
        (m.watermark - 0.5).abs() < 1e-12,
        "watermark 2/4, got {}",
        m.watermark
    );
    // 缩容后借出数高于新池深：水位线 >1.0 如实呈现（合法中间态，不钳位）。
    pool.resize(1, 1).expect("legal shrink");
    let m = pool.metrics();
    assert!(
        (m.watermark - 2.0).abs() < 1e-12,
        "watermark 2/1, got {}",
        m.watermark
    );
}

// ── 四件之三：审计事件发射，seq 单调，种类齐全 ────────────────────────

#[test]
fn audit_events_emit_with_monotonic_seq() {
    let pool = PoolBase::new(4, 2);
    let (tx, rx) = std::sync::mpsc::channel();
    {
        let _ticket = pool.subscribe_audit(AUDIT_RING_DEFAULT_CAP, move |e| {
            tx.send(*e).expect("receiver alive");
        });
        pool.resize(2, 1).expect("legal resize");
        pool.resize(0, 1).expect_err("rejected resize");
        pool.note_borrow();
        assert!(pool.note_return());
        pool.note_borrow();
        pool.resize(1, 1).expect("legal shrink");
        assert!(!pool.note_return(), "dropped: depth exceeded");
    }
    let seen: Vec<_> = rx.try_iter().collect();
    let kinds: Vec<_> = seen.iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        [
            PoolAuditKind::Resized,
            PoolAuditKind::ResizeRejected,
            PoolAuditKind::Borrowed,
            PoolAuditKind::Returned,
            PoolAuditKind::Borrowed,
            PoolAuditKind::Resized,
            PoolAuditKind::OverflowDropped,
        ]
    );
    // seq 严格单调；订阅窗内连续（无溢出无空洞），首件 seq=1。
    assert!(
        seen.windows(2).all(|w| w[0].seq < w[1].seq),
        "seq monotonic: {seen:?}"
    );
    assert_eq!(seen.first().unwrap().seq, 1);
    assert_eq!(seen.last().unwrap().seq, 7);
}

// ── 四件之四：归还槽满=丢弃+dropped_overflow 计数（RAII 语义唯一自洽） ─

#[test]
fn dropped_overflow_counts_when_depth_exceeded() {
    let pool = PoolBase::new(2, 1);
    pool.note_borrow();
    pool.note_borrow();
    // 缩容到池深 1：借出句柄不受影响。
    pool.resize(1, 1).expect("legal shrink");
    // 归还 #1：total(=2) 已超 max(=1)，槽满 → 丢弃。
    assert!(!pool.note_return(), "dropped: depth exceeded");
    // 归还 #2：total 回到池深内 → 留存入闲槽。
    assert!(pool.note_return(), "kept: total back within depth");
    let m = pool.metrics();
    assert_eq!(m.dropped_overflow, 1);
    assert_eq!((m.borrowed, m.idle, m.max), (0, 1, 1));
}

#[test]
fn return_drops_when_idle_cap_is_full() {
    let pool = PoolBase::new(2, 2);
    pool.note_borrow();
    pool.note_borrow();
    pool.resize(2, 1).expect("shrink idle cap");
    assert!(pool.note_return(), "kept: idle cap has room");
    assert!(!pool.note_return(), "dropped: idle cap full");
    let m = pool.metrics();
    assert_eq!(m.dropped_overflow, 1);
    assert_eq!((m.borrowed, m.idle), (0, 1));
}

// ── 骨架审计纪律：容量环有界/回调 panic 摘除留痕/重入封死/票据关流 ────

#[test]
fn ring_is_bounded_and_overflow_drops_oldest() {
    let pool = PoolBase::new(4, 2);
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let _t = pool.subscribe_audit(2, move |_| {
        h.fetch_add(1, Ordering::SeqCst);
    });
    pool.note_borrow();
    pool.note_borrow();
    pool.note_borrow();
    // 实时回调收到全部三件。
    assert_eq!(hits.load(Ordering::SeqCst), 3);
    // 环里只留最新两件（丢最旧 = 空洞显式证据，seq 可对账）。
    let drained = pool.take_audit_events();
    assert_eq!(drained.len(), 2);
    assert_eq!((drained[0].seq, drained[1].seq), (2, 3));
}

#[test]
fn audit_cap_zero_clamps_to_one() {
    let pool = PoolBase::new(4, 2);
    let _t = pool.subscribe_audit(0, |_| {});
    pool.note_borrow();
    pool.note_borrow();
    let drained = pool.take_audit_events();
    assert_eq!(drained.len(), 1, "cap 0 clamps to 1, newest kept");
    assert_eq!(drained[0].seq, 2);
}

#[test]
fn take_without_subscription_is_empty() {
    let pool = PoolBase::new(4, 2);
    pool.note_borrow();
    assert!(pool.take_audit_events().is_empty(), "no stream, no ring");
}

#[test]
fn callback_panic_is_removed_with_trace_pool_unaffected() {
    let pool = PoolBase::new(4, 2);
    let _t = pool.subscribe_audit(8, |_| panic!("boom"));
    pool.note_borrow(); // 回调 panic：池行为不变，回调被摘除。
    assert_eq!(pool.metrics().borrowed, 1);
    assert_eq!(pool.audit_callback_panics(), 1);
    // 环照录（观测留痕不丢）。
    assert_eq!(pool.take_audit_events().len(), 1);
    // 后续事件只录环，不再进已摘除的回调（计数不涨）。
    pool.note_borrow();
    assert_eq!(pool.audit_callback_panics(), 1);
    assert_eq!(pool.take_audit_events().len(), 1);
}

#[test]
fn callback_reentry_records_but_does_not_recurse() {
    let pool = Arc::new(PoolBase::new(4, 2));
    let p2 = Arc::clone(&pool);
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let _t = pool.subscribe_audit(8, move |e| {
        c.fetch_add(1, Ordering::SeqCst);
        if e.kind == PoolAuditKind::Borrowed {
            // 回调内重入调参 → 触发再入 emit：只录环不再进回调（封死自指递归）。
            p2.resize(2, 1).expect("reentrant resize");
        }
    });
    pool.note_borrow();
    // 回调恰跑一次：再入 emit 递归被闸。
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // 两件事件（Borrowed + 再入的 Resized）都进了环。
    let drained = pool.take_audit_events();
    assert_eq!(drained.len(), 2);
    assert_eq!(drained[1].kind, PoolAuditKind::Resized);
}

#[test]
fn ticket_drop_closes_stream() {
    let pool = PoolBase::new(4, 2);
    let hits = Arc::new(AtomicUsize::new(0));
    let h = hits.clone();
    let ticket = pool.subscribe_audit(8, move |_| {
        h.fetch_add(1, Ordering::SeqCst);
    });
    drop(ticket);
    pool.note_borrow();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        0,
        "closed stream delivers nothing"
    );
    assert!(
        pool.take_audit_events().is_empty(),
        "closed: slot cleared, no recording"
    );
}

#[test]
fn stale_ticket_drop_does_not_kill_newer_stream() {
    let pool = PoolBase::new(4, 2);
    let hits = Arc::new(AtomicUsize::new(0));
    let h2 = hits.clone();
    let h3 = hits.clone();
    let t2 = pool.subscribe_audit(8, move |_| {
        h2.fetch_add(1, Ordering::SeqCst);
    });
    // 新订阅替换现役槽：t2 的票据即刻 inert。
    let t3 = pool.subscribe_audit(8, move |_| {
        h3.fetch_add(1, Ordering::SeqCst);
    });
    drop(t2); // 旧代次票据：代次对账不过 → 禁落闸禁误杀。
    pool.note_borrow();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "newer stream alive after stale drop"
    );
    drop(t3); // 现役票据：正常关流。
    pool.note_borrow();
    assert_eq!(
        hits.load(Ordering::SeqCst),
        1,
        "stream closed by its own ticket"
    );
}

// ── 记账纪律：闲槽同步 + 记账 bug fail-fast ──────────────────────────

#[test]
fn idle_delta_maintains_honest_readouts() {
    let pool = PoolBase::new(4, 2);
    pool.note_idle_delta(2); // 预热两件入闲槽。
    assert_eq!(pool.metrics().idle, 2);
    pool.note_idle_delta(-1); // 自闲槽借出。
    assert_eq!(pool.metrics().idle, 1);
    pool.note_idle_delta(-1); // 清空。
    assert_eq!(pool.metrics().idle, 0);
}

#[test]
#[should_panic(expected = "bookkeeping bug")]
fn return_without_borrow_is_a_bookkeeping_bug() {
    let pool = PoolBase::new(4, 2);
    pool.note_return();
}

#[test]
#[should_panic(expected = "PoolBase::new")]
fn new_rejects_illegal_config_fail_fast() {
    let _ = PoolBase::new(0, 1);
}
