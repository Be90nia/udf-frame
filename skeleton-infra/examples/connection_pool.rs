//! 第三池省事度验收（调研档 §4.4.3 拍板锚「新池第三者接入只需写特化面」）：
//! `ConnectionPool<T>` 最小示例池。
//!
//! 只接两件——契约 [`PoolManaged`]（skeleton）+ 组合件 [`PoolBase`]
//! （skeleton_infra::pool）——美团理念全套自动到手：动态调参（事前校验）/
//! 水位线读数/池域审计/溢出丢弃计数。本文件只写**特化面**：槽位存储 +
//! 借还路径（guard RAII 归还回调 [`PoolBase::note_return`] 裁决）。
//!
//! 特化取舍（与双池的差异面）：借出**非阻塞**（无闲置且配额满 = 显式
//! [`Exhausted`]，无条件变量无超时——连接池最简语义）；三参封顶特化为
//! 单参 `max`（闲槽上限随池深，`idle_cap == max`）；无特有读数，Metrics
//! 直接用核心形 [`PoolMetrics`]（零包装，`Borrow` 自反幂等）。
//!
//! ponytail: create 配额判定先读 `metrics().max` 再取槽位锁（锁不嵌套；
//! TOCTOU 窗口 ≤ 一次并发 resize）——扩容无害，缩容窗多造的连接在归还时
//! 由 note_return 槽满裁决丢弃+计数自愈；MemoryPool 的边界副本+串行闸
//! 在真实采用方需要时再上（见 memgov.rs `resizing` 纪律）。

use std::borrow::Borrow;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use skeleton::pool::{
    PoolAuditEvent, PoolAuditKind, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
};
use skeleton_infra::pool::{PoolBase, AUDIT_RING_DEFAULT_CAP};

/// 连接池（泛型连接 + 工厂）：管理面全在 [`PoolBase`]，本类型只有特化面。
pub struct ConnectionPool<T, F = fn() -> T> {
    base: PoolBase,
    inner: Mutex<Slots<T>>,
    factory: F,
}

/// 槽位账（槽位锁单权威内）：闲置连接栈 + 存活数（闲置+借出）。
struct Slots<T> {
    idle: Vec<T>,
    created: usize,
}

/// 借出失败（非阻塞特化）：无闲置且配额满。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhausted;

impl std::fmt::Display for Exhausted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection pool exhausted")
    }
}

impl std::error::Error for Exhausted {}

impl<T, F: Fn() -> T> ConnectionPool<T, F> {
    /// 构造。非法边界 panic fail-fast（委托 [`PoolBase::new`] 同一套规则：
    /// `max == 0` 恒不合法）。
    pub fn new(max: usize, factory: F) -> Self {
        Self {
            base: PoolBase::new(max, max), // idle_cap 随池深（单参特化）
            inner: Mutex::new(Slots {
                idle: Vec::new(),
                created: 0,
            }),
            factory,
        }
    }

    /// 借出（非阻塞）：闲槽直取，否则配额内工厂新建，满则 [`Exhausted`]。
    pub fn try_acquire(&self) -> Result<ConnectionGuard<'_, T>, Exhausted> {
        let max = self.base.metrics().max; // 先读边界再取槽位锁（锁不嵌套）
        let mut slots = self
            .inner
            .lock()
            .expect("connection pool lock poisoned (prior holder panicked)");
        let conn = match slots.idle.pop() {
            Some(conn) => {
                self.base.note_idle_delta(-1);
                conn
            }
            None if slots.created < max => {
                slots.created += 1;
                (self.factory)()
            }
            None => return Err(Exhausted),
        };
        self.base.note_borrow();
        Ok(ConnectionGuard {
            base: &self.base,
            inner: &self.inner,
            slot: Some(conn),
        })
    }
}

impl<T, F: Fn() -> T> PoolManaged for ConnectionPool<T, F> {
    type Tuning = usize; // 单参特化：max（idle_cap 随 max）
    type Metrics = PoolMetrics; // 无特有读数：核心形直接用（零包装）

    fn resize(&self, max: usize) -> Result<(), PoolResizeError> {
        self.base.resize(max, max)
    }

    fn metrics(&self) -> PoolMetrics {
        self.base.metrics()
    }

    fn subscribe_audit(&self, f: impl FnMut(&PoolAuditEvent) + Send + 'static) -> PoolAuditTicket {
        self.base.subscribe_audit(AUDIT_RING_DEFAULT_CAP, f)
    }
}

impl<T, F: Fn() -> T> ConnectionPool<T, F> {
    /// 取走环内审计事件（双池同款读侧形态：委托基座，随取随清）。
    pub fn take_audit_events(&self) -> Vec<PoolAuditEvent> {
        self.base.take_audit_events()
    }
}

/// 借出句柄（RAII）：`Deref`/`DerefMut` 到连接；**Drop 即还**（禁手动还）——
/// 槽满（缩容后超池深）连接直接析构+溢出计数，归还永不阻塞永不报错。
pub struct ConnectionGuard<'p, T> {
    base: &'p PoolBase,
    inner: &'p Mutex<Slots<T>>,
    slot: Option<T>,
}

impl<T> Deref for ConnectionGuard<'_, T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.slot
            .as_ref()
            .expect("guard slot empty on use (bookkeeping bug)")
    }
}

impl<T> DerefMut for ConnectionGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.slot
            .as_mut()
            .expect("guard slot empty on use (bookkeeping bug)")
    }
}

impl<T> Drop for ConnectionGuard<'_, T> {
    fn drop(&mut self) {
        let conn = self
            .slot
            .take()
            .expect("guard slot empty on drop (bookkeeping bug)");
        if self.base.note_return() {
            self.inner
                .lock()
                .expect("connection pool lock poisoned (prior holder panicked)")
                .idle
                .push(conn);
        } else {
            // 溢出丢弃：配额回吐（created 同步递减，工厂配额不缩水）。
            self.inner
                .lock()
                .expect("connection pool lock poisoned (prior holder panicked)")
                .created -= 1;
        }
    }
}

fn main() {
    let n = AtomicUsize::new(0);
    let pool = ConnectionPool::new(4, move || {
        format!("conn-{}", n.fetch_add(1, Ordering::SeqCst))
    });

    // 先订审计再动（票据必须绑定持有——must_use 纪律，裸调用即关流）。
    let ticket = pool.subscribe_audit(|_| {});

    // ① 事前校验：非法调参入口拒绝，零状态变更（ResizeRejected 进审计环）。
    assert_eq!(pool.resize(0), Err(PoolResizeError::ZeroMax));
    assert_eq!(pool.metrics().max, 4);

    // ② 借出三连：借出数与水位线如实移动。
    let g1 = pool.try_acquire().unwrap();
    let g2 = pool.try_acquire().unwrap();
    let g3 = pool.try_acquire().unwrap();
    let m = pool.metrics();
    assert_eq!((m.borrowed, m.idle), (3, 0));
    assert!(
        (m.watermark - 0.75).abs() < 1e-9,
        "watermark 3/4, got {}",
        m.watermark
    );
    assert_eq!(m.dropped_overflow, 0);

    // ③ 动态缩容不夺借出：4→2，已借 3 件归还时逐件裁决（槽满=丢弃+计数）。
    pool.resize(2).unwrap();
    drop(g3); // keep = idle(0)<2 && 0+2<2 不成立 → 溢出丢弃，配额回吐
    drop(g2); // keep = 0<2 && 0+1<2 → 留存入闲槽
    drop(g1); // keep = 1<2 && 1+0<2 → 留存
    let m = pool.metrics();
    assert_eq!(m.dropped_overflow, 1, "缩容窗超限归还=溢出丢弃计数");
    assert_eq!((m.borrowed, m.idle), (0, 2), "终态无借出，闲槽不越新边界");

    // ④ 再借走闲槽（工厂不新建——配额回吐证据 created==2），满则显式 Exhausted。
    let a = pool.try_acquire().unwrap();
    let b = pool.try_acquire().unwrap();
    assert!(
        matches!(pool.try_acquire(), Err(Exhausted)),
        "非阻塞特化：满即显式拒绝"
    );
    drop((a, b));

    // ⑤ 审计取证：环内 seq 单调，拒绝与生效与丢弃都留痕。
    let events = pool.take_audit_events();
    let kinds: Vec<_> = events.iter().map(|e| e.kind).collect();
    assert!(kinds.contains(&PoolAuditKind::ResizeRejected), "拒绝留痕");
    assert!(kinds.contains(&PoolAuditKind::Resized), "生效留痕");
    assert!(kinds.contains(&PoolAuditKind::OverflowDropped), "溢出留痕");
    assert!(
        events.windows(2).all(|w| w[0].seq < w[1].seq),
        "seq 进程内单调"
    );
    drop(ticket); // 票据 Drop = 关流

    // ⑥ 核心形读侧：Metrics=PoolMetrics 零包装，宿主对任何池同一组读数。
    let m = pool.metrics();
    let core: &PoolMetrics = m.borrow();
    assert_eq!(core.max, 2);

    println!("connection_pool: 第三池全断言通过（管理面零重写，特化面见文件头对表）");
}
