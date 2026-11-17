//! 尺寸分档池组合件：一张档位表 + 每档一个 [`ObjectPool`]，对外按尺寸借还。
//!
//! **feature**：`pool`（默认）。零三方依赖，只组合本 crate 既有池件。
//!
//! 消费方形态（sing-box `common/buf/alloc.go` 十二级 `sync.Pool`，64B→64KB）：
//! 「三套池并存、请求 64KB 却拿到 128KB 块」的根因不在单池，而在缺一层选档。
//! 本件只补这层：选档在本件，档内借还复用 [`ObjectPool`] 的四路径与记账
//! （不复制第二套池机制，也不改单池实现）。
//!
//! # 语义契约
//!
//! - **向上取整选档**：`acquire(need)` 取档位表里第一个 `>= need` 的档。请求
//!   64KB 拿 128KB 块**允许**（浪费上界 = 档位表相邻档的比值）；反向（请求
//!   128KB 拿 64KB 块）结构上不可能——档位表严格递增且选档单调，只有「多给」。
//! - **归还按档归位**：句柄记住**借出时的档**，Drop 回那一档的池；**不按对象
//!   当前容量反推档位**——对象在借出期涨大后，按容量反推会把大件塞回小档池
//!   （池毒化，golang/go#23199 / #27735）或漂到相邻档，两种都是错账。
//! - **超最大档 = 档外降级直构**：请求大于最大档时**按请求字节直接构造**
//!   （`over_max_degraded` +1），不入任何档的池宇宙、归还即释放。**不静默回退
//!   到最大档**——否则「请求 1MB 拿到 64KB」会成为隐式截断：调用方拿到名不
//!   副实的对象，且没有任何信号可查。
//! - **池内降级与档外降级分账**：档内池宇宙满时 [`ObjectPool`] 照旧直构
//!   （`SizeClassStat::pool.degraded`），与本件的 `over_max_degraded` 是两个
//!   独立计数，混账会让「档位错配」与「容量不足」无法区分。
//!
//! # 组合红线（票面已定，bd `udf-frame-dks` / issue#15 增量 3）
//!
//! - **池化对象等重**：同一档池里的件都应是该档字节量级。`factory` 收到档字节
//!   （池化路）或请求字节（档外直构路），按其开容量；借出期把对象涨到远大于
//!   档字节，这个池就退化成「大件小档」的杂项堆（池毒化）。本件**不做重量
//!   校验**：泛型 `T` 没有容量概念，普遍判定不存在（判据由消费方的 reset 钩子
//!   或不变式承担）。
//! - **reset 钩子（若配）负责容量归一**：本件构造的档池走 [`ObjectPool::new`]
//!   ——无 reset 钩子，对象状态跨借还保留（「捞出改参数即复用」语义，与单池
//!   一致）。需要归还时重置的部署必须自己用 [`ObjectPool::with_reset`] 建档池，
//!   并保证钩子把容量**归一到档字节**（`Vec` 类用 `clear` 后容量仍然保留，
//!   归一需 `shrink_to` 或改走小档）：容量只涨不归 ⇒ 档位语义随时间漂移，
//!   按档读数与字节预算全部失真。**本件刻意不暴露 reset 配置**——单档池的
//!   钩子相位/重入/panic 纪律属单池契约，组合层再开一个配置面等于把两套调参
//!   拼在一起，调用方无从判断哪个生效。
//!
//! # 读数口径
//!
//! [`SizeClassPool::metrics`] 给「按档明细（与档位表同序）+ 档外降级计数」，
//! 不只有总数——档位错配的诊断前提是能看出**哪一档**在涨。O(档数 × 分片数)
//! 取锁求和、非热路径；借还热路径不新增原子与分配（档外直构才多一次原子加）。

use std::{
    ops::{Deref, DerefMut},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::objpool::{ObjectPool, ObjectPoolMetrics, Pooled};

/// 单档读数：档字节 + 该档池的完整 [`ObjectPoolMetrics`]。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SizeClassStat {
    /// 档字节（档位表原值）。
    pub class_bytes: usize,
    /// 该档池读数。`pool.core.borrowed` 含池内降级借出（同单池口径）；
    /// `pool.degraded` = 池内降级直构次数（与档外降级分账）。
    pub pool: ObjectPoolMetrics,
}

/// 组合读数：按档明细（与档位表同序同长）+ 档外降级计数。
#[derive(Debug, Clone, PartialEq)]
pub struct SizeClassMetrics {
    /// 按档明细，与 [`SizeClassPool::class_bytes`] 同序同长。
    pub classes: Vec<SizeClassStat>,
    /// 超最大档的档外直构次数（不入任何档池、归还即释放）。
    pub over_max_degraded: u64,
}

impl SizeClassMetrics {
    /// 全档 idle 合计。
    pub fn idle(&self) -> usize {
        self.classes.iter().map(|class| class.pool.core.idle).sum()
    }

    /// 全档借出合计（含池内降级借出，同 [`ObjectPoolMetrics::core`] 口径）。
    pub fn borrowed(&self) -> usize {
        self.classes
            .iter()
            .map(|class| class.pool.core.borrowed)
            .sum()
    }

    /// 全档**池内**降级合计（不含档外降级，后者见 [`Self::over_max_degraded`]）。
    pub fn class_degraded(&self) -> u64 {
        self.classes.iter().map(|class| class.pool.degraded).sum()
    }
}

/// 尺寸分档池：按档位表持有多个 [`ObjectPool`] 实例，按尺寸借还。
///
/// # Panics
///
/// 构造期 fail-fast（同 [`ObjectPool::new`] 纪律）：
/// - 档位表为空 / 含 0 / 非严格递增（重复档也是非法——同一档两次出现等于
///   沉默的双池，选档结果不确定）；
/// - `max` / `idle_cap` 非法（由 [`ObjectPool::new`] 校验：0 或 `idle_cap > max`）。
///
/// 每档池深相同（`max` / `idle_cap` 逐档复用）：档深差异化需求先改单池调参面，
/// 本件不复制第二个调参入口。
///
/// # Examples
///
/// ```
/// use skeleton_infra::SizeClassPool;
///
/// let pool = SizeClassPool::new([64, 1024, 65_536], 4, 2, |size| {
///     Vec::<u8>::with_capacity(size)
/// });
///
/// let small = pool.acquire(10); // → 64B 档
/// assert_eq!(small.class_bytes(), 64);
///
/// let mid = pool.acquire(600); // 向上取整：600B → 1KiB 档
/// assert_eq!(mid.class_bytes(), 1024);
/// assert!(mid.capacity() >= 600);
///
/// let huge = pool.acquire(1 << 20); // 超最大档：按请求直构，不回退到 64KiB
/// assert!(huge.is_over_max_degraded());
/// assert!(huge.capacity() >= 1 << 20);
///
/// drop((small, mid));
/// assert_eq!(pool.metrics().classes[1].pool.core.idle, 1);
/// assert_eq!(pool.metrics().over_max_degraded, 1);
/// ```
pub struct SizeClassPool<T> {
    /// 档位表（严格递增，构造期校验）。
    class_bytes: Vec<usize>,
    /// 每档一个池，与 `class_bytes` 同序同长。
    pools: Vec<ObjectPool<T>>,
    /// 档外直构用（只有超最大档请求调用它）。档池各自持一份克隆。
    factory: Arc<dyn Fn(usize) -> T + Send + Sync>,
    over_max_degraded: AtomicU64,
}

impl<T: 'static> SizeClassPool<T> {
    /// 装配：`class_bytes` 为严格递增档位表（如 `[64, 256, 1024, …, 1 << 16]`），
    /// `factory` 收**字节数**（池化路 = 档字节；档外直构路 = 请求字节）造对象。
    /// 工厂要求 `'static`（同 [`ObjectPool::new`]）⇒ `T` 随之受 `'static` 约束。
    pub fn new(
        class_bytes: impl IntoIterator<Item = usize>,
        max: usize,
        idle_cap: usize,
        factory: impl Fn(usize) -> T + Send + Sync + 'static,
    ) -> Self {
        let class_bytes: Vec<usize> = class_bytes.into_iter().collect();
        validate_classes(&class_bytes);
        let factory: Arc<dyn Fn(usize) -> T + Send + Sync> = Arc::new(factory);
        let pools = class_bytes
            .iter()
            .map(|&size| {
                let factory = Arc::clone(&factory);
                ObjectPool::new(max, idle_cap, move || factory(size))
            })
            .collect();
        Self {
            class_bytes,
            pools,
            factory,
            over_max_degraded: AtomicU64::new(0),
        }
    }

    /// 按需借出：向上取整到最小满足档；超最大档走档外降级直构。
    ///
    /// **「永不失败」指不存在容量/超时拒绝路径**（不像阻塞池那样在期限窗内
    /// 无件可借）——分配本身仍可能 OOM abort，那不属于本 API 的失败语义。
    /// 档外直构路径的开销 = 一次 `Relaxed` 原子加 + 一次 `dyn Fn` 调用
    /// （`factory` 经 `Arc<dyn Fn>` 擦除，非单态化）。
    pub fn acquire(&self, need: usize) -> SizeClassHandle<'_, T> {
        match self.class_index(need) {
            Some(index) => SizeClassHandle {
                class: Some(index),
                class_bytes: self.class_bytes[index],
                inner: HandleInner::Pooled(self.pools[index].acquire()),
            },
            None => {
                self.over_max_degraded.fetch_add(1, Ordering::Relaxed);
                SizeClassHandle {
                    class: None,
                    class_bytes: need,
                    inner: HandleInner::Direct((self.factory)(need)),
                }
            }
        }
    }

    /// 档位表（严格递增）。
    pub fn class_bytes(&self) -> &[usize] {
        &self.class_bytes
    }

    /// 选档：第一个 `>= need` 的档下标（`None` = 超最大档）。
    fn class_index(&self, need: usize) -> Option<usize> {
        let index = self.class_bytes.partition_point(|&size| size < need);
        (index < self.class_bytes.len()).then_some(index)
    }

    /// 组合读数：按档明细（与档位表同序同长）+ 档外降级计数。
    ///
    /// O(档数 × 分片数) 取锁求和，非热路径（每次调用分配一个 `Vec`：档数固定、
    /// 诊断面才读）。
    pub fn metrics(&self) -> SizeClassMetrics {
        SizeClassMetrics {
            classes: self
                .class_bytes
                .iter()
                .zip(&self.pools)
                .map(|(&class_bytes, pool)| SizeClassStat {
                    class_bytes,
                    pool: pool.metrics(),
                })
                .collect(),
            over_max_degraded: self.over_max_degraded.load(Ordering::Relaxed),
        }
    }
}

/// 档位表校验（构造期 fail-fast，同 [`ObjectPool::new`] 纪律）：非空、无 0、
/// 严格递增。
fn validate_classes(class_bytes: &[usize]) {
    assert!(
        !class_bytes.is_empty(),
        "SizeClassPool: class table must not be empty"
    );
    assert!(
        class_bytes[0] > 0,
        "SizeClassPool: class size 0 is not a size class"
    );
    for pair in class_bytes.windows(2) {
        assert!(
            pair[0] < pair[1],
            "SizeClassPool: class table must be strictly ascending, got {} then {}",
            pair[0],
            pair[1]
        );
    }
}

/// 借出句柄（RAII）：[`Deref`]/[`DerefMut`] 到 `T`；**Drop 即归还**——回
/// 借出时的档（档外降级直构件归还即释放，不进任何池）。
///
/// 与 [`Pooled`] 的差别只有一层：本件额外记住档位（用于归位与读数），归还路径
/// 仍是 [`Pooled`] 那条（reset/prune/unwind 纪律全归单池，组合层不复制）。
pub struct SizeClassHandle<'p, T> {
    /// 档下标（`None` = 超最大档的档外直构）。
    class: Option<usize>,
    /// 借出档字节；档外直构时 = 请求字节。
    class_bytes: usize,
    inner: HandleInner<'p, T>,
}

enum HandleInner<'p, T> {
    /// 来自某档池：Drop 由 [`Pooled`] 归还，回它借出时的池。
    Pooled(Pooled<'p, T>),
    /// 档外直构：不属任何池，Drop 即释放。
    Direct(T),
}

impl<T> SizeClassHandle<'_, T> {
    /// 借出时的档下标；`None` = 请求超出最大档、走了档外降级直构。
    pub fn class_index(&self) -> Option<usize> {
        self.class
    }

    /// 借出档字节；档外降级直构时 = 请求字节。
    pub fn class_bytes(&self) -> usize {
        self.class_bytes
    }

    /// 本件是否走的是**档外**降级直构（请求超出最大档）。
    ///
    /// 档内池满触发的池内降级**不在此列**：那种件照常回本档池决定去留，读数在
    /// [`SizeClassStat::pool`] 的 `degraded`。
    pub fn is_over_max_degraded(&self) -> bool {
        self.class.is_none()
    }
}

impl<T> Deref for SizeClassHandle<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match &self.inner {
            HandleInner::Pooled(pooled) => pooled,
            HandleInner::Direct(obj) => obj,
        }
    }
}

impl<T> DerefMut for SizeClassHandle<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        match &mut self.inner {
            HandleInner::Pooled(pooled) => pooled,
            HandleInner::Direct(obj) => obj,
        }
    }
}
