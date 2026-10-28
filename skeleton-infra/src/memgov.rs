//! 内存治理：有界容器族 + counting allocator + 泄漏断言（结构性保证不靠自觉）。
//!
//! feature `memgov`（零外部依赖纯 std）；由 lib.rs 以
//! `#[cfg(feature = "memgov")] pub mod memgov;` 接线。

use std::alloc::{GlobalAlloc, Layout, System};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// 淘汰策略（必选参数——「上限+策略必填」由构造签名类型强制，非约定自觉）：
/// Reject=满则显式错（fail-closed，默认推荐）；
/// EvictOldest=按 BTreeMap 首键淘汰。
///
/// K 契约：K 须单调指派（序号/时间戳键），此时语义=逐最旧；
/// K 乱序时语义如实退化为 EvictMinKey——调用方纪律，本件不静默补偿。
/// LRU/TTL 精确语义在 [`crate::cache`]（本件首键退化语义的邻接补全件）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Eviction {
    /// 满=Err(BoundError)（fail-closed）。
    Reject,
    /// 满=先 pop_first（首键）再插。
    EvictOldest,
}

/// [`BoundedRegistry::insert`] 在满且策略为 [`Eviction::Reject`] 时的显式错误。
///
/// 零依赖纪律（同 text.rs/journal.rs 先例）：thiserror 不入 manifest，
/// 手写同形 `Display`/`Error` impl（文案与 thiserror 派生形态逐字一致），
/// 引入 thiserror 依赖时可换回派生。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoundError {
    /// 撞上的注册表容量上界。
    pub capacity: usize,
}

impl std::fmt::Display for BoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "bounded registry full (capacity={}, eviction=Reject)",
            self.capacity
        )
    }
}

impl std::error::Error for BoundError {}

/// 有界注册表：上限+策略必填（无默认无界构造——构造签名层类型强制，不经约定自觉）。
/// 迭代恒升序确定序；缓存形态别名 BoundedCache=本类型（文档级别名，不另造类型）。
///
/// Clone=深拷贝（值容器语义，非共享句柄）：map/上限/策略逐字段完整拷贝，克隆体与
/// 原件的容量语义、确定序互不影响。运行期只读共享由调用方外包 `Arc<BoundedRegistry>`
/// ——本件不内嵌 Arc（与 `get_mut`/`insert` 的 `&mut self` 结构性冲突）。
#[derive(Clone)]
pub struct BoundedRegistry<K: Ord, V> {
    map: BTreeMap<K, V>,
    max: usize,
    evict: Eviction,
}

impl<K: Ord, V> BoundedRegistry<K, V> {
    /// 构造有界注册表。`max == 0` 构造期 panic（assert fail-fast：恒满注册表必是 bug）。
    pub fn new(max: usize, evict: Eviction) -> Self {
        assert!(
            max > 0,
            "BoundedRegistry::new: max == 0 is a bug (always-full registry)"
        );
        Self {
            map: BTreeMap::new(),
            max,
            evict,
        }
    }

    /// 插入。已存在 key=覆盖（len 不变，不触发淘汰）。
    /// 满：Reject→Err(BoundError)（原内容不动）；EvictOldest→先 pop_first 再插。
    /// Err ⇒ 调用方显式处理（fail-closed 回灌）。
    pub fn insert(&mut self, k: K, v: V) -> Result<(), BoundError> {
        if let Some(slot) = self.map.get_mut(&k) {
            *slot = v;
            return Ok(());
        }
        if self.map.len() >= self.max {
            match self.evict {
                Eviction::Reject => return Err(BoundError { capacity: self.max }),
                Eviction::EvictOldest => {
                    self.map.pop_first();
                }
            }
        }
        self.map.insert(k, v);
        Ok(())
    }

    /// 读侧（键不存在=`None`，不 panic）。`Q` 为键的借用形（`K: Borrow<Q>`，
    /// 如 `String` 键用 `&str` 查找）——热路径零分配；借用形与 owned 形走同一个
    /// `Ord` 实现，查找结果恒一致（单一序同源，禁第二套比较）。
    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.map.get(k)
    }

    /// 可变读侧（只回值面）。键存于 BTreeMap 节点内部，对调用方类型级不可达——
    /// 值面怎么改都伤不了表序（键序被改写=std 定义的结构性逻辑错误，此处杜绝）。
    ///
    /// ```compile_fail
    /// # use skeleton_infra::memgov::{BoundedRegistry, Eviction};
    /// fn tamper_key<K: Ord, V>(reg: &mut BoundedRegistry<K, V>, k: &K) {
    ///     if let Some(v) = reg.get_mut(k) {
    ///         let _key_face: &mut K = v; // get_mut 只回 &mut V：键面不可达
    ///     }
    /// }
    /// ```
    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.map.get_mut(k)
    }

    /// 摘除并返回值（登记面收缩的唯一口）。
    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.map.remove(k)
    }

    /// 当前登记数。
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// 空表判定（泄漏断言的便捷面）。
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// 容量上界（构造时定，不变）。
    pub fn capacity(&self) -> usize {
        self.max
    }

    /// 确定序升序迭代（泄漏断言面）。
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.map.iter()
    }
}

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static LIVE_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static TOTAL_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static TOTAL_DEALLOCS: AtomicUsize = AtomicUsize::new(0);

/// counting allocator：System 包装+五计数器——净额面 `live_bytes`/`live_allocs`/
/// `peak_bytes`，累计面 `total_allocs`/`total_deallocs`（Relaxed 序）。
/// **仅测试/基准侧声明**：`#[global_allocator] static A: CountingAllocator = CountingAllocator;`
/// 发布构建不声明=零开销（内存红线「泄漏=断言兜底+计数器可观测」的配置面）。
pub struct CountingAllocator;

impl CountingAllocator {
    /// 当前存活字节数（O(1)；G 类超阈告警钩子的数据源）。
    pub fn live_bytes() -> usize {
        LIVE_BYTES.load(Ordering::Relaxed)
    }

    /// 当前存活分配次数（O(1)）。
    pub fn live_allocs() -> usize {
        LIVE_ALLOCS.load(Ordering::Relaxed)
    }

    /// 历史峰值存活字节数（fetch_max 维护）。
    /// 计数器全局共享：**仅串行上下文有效**（并行测试的他测分配计入读数，无归因）。
    pub fn peak_bytes() -> usize {
        PEAK_BYTES.load(Ordering::Relaxed)
    }

    /// 清零峰值（同样仅串行上下文有意义）。
    pub fn reset_peak() {
        PEAK_BYTES.store(0, Ordering::Relaxed);
    }

    /// 历史累计成功分配次数（只增不减；与 [`Self::live_allocs`] 净额面互补）。
    ///
    /// **口径（churn 对账，与 live 联用）**：窗口 churn = `total_allocs(t2) −
    /// total_allocs(t1)`。live 净额对「即用即还」恒 0——净 0 无法区分「真零分配」
    /// 与「同量 churn」；联用判级：live 不变 + total 大增 = 高 churn 零泄漏，
    /// 两者皆平 = 窗口真零分配。realloc 成功按「换块」计（total_allocs/deallocs
    /// 各 +1：块替换即 churn；live 面照旧不动）。Relaxed 序尽力而为观测面。
    pub fn total_allocs() -> usize {
        TOTAL_ALLOCS.load(Ordering::Relaxed)
    }

    /// 历史累计释放次数（只增不减）。口径同 [`Self::total_allocs`]；与
    /// total_allocs 差值长期不等 = 累计泄漏/外部遗忘面（live 持平下的慢泄漏可辨）。
    pub fn total_deallocs() -> usize {
        TOTAL_DEALLOCS.load(Ordering::Relaxed)
    }
}

/// 成功分配的记账四连：bytes 增、峰值刷新、allocs 增、total 增（alloc/alloc_zeroed
/// 共用，防漂移）。
fn record_alloc(layout: Layout) {
    let live = LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
    LIVE_ALLOCS.fetch_add(1, Ordering::Relaxed);
    TOTAL_ALLOCS.fetch_add(1, Ordering::Relaxed);
}

// SAFETY: 计数器增减仅在 System 分配成功/释放后发生，不改变分配器本身的内存安全性；
// 计数为尽力而为观测面（Relaxed），不参与内存安全论证。
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: System 分配器对任意合法 Layout 安全；null 按契约原样返回，不产出悬垂指针。
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: ptr 须来自本分配器成功分配且未释放——GlobalAlloc 契约由调用方保证。
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        LIVE_ALLOCS.fetch_sub(1, Ordering::Relaxed);
        TOTAL_DEALLOCS.fetch_add(1, Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: 同 alloc；null 不计数。
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout);
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, old_layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: ptr/old_layout 配对由 GlobalAlloc 契约保证；null 原样返回不计数。
        let new_ptr = unsafe { System.realloc(ptr, old_layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= old_layout.size() {
                let grown = LIVE_BYTES.fetch_add(new_size - old_layout.size(), Ordering::Relaxed)
                    + (new_size - old_layout.size());
                PEAK_BYTES.fetch_max(grown, Ordering::Relaxed);
            } else {
                LIVE_BYTES.fetch_sub(old_layout.size() - new_size, Ordering::Relaxed);
            }
            // 换块即 churn（total 口径）：+1 alloc 且 +1 dealloc；live 面照旧不动。
            TOTAL_ALLOCS.fetch_add(1, Ordering::Relaxed);
            TOTAL_DEALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        new_ptr
    }
}

/// 泄漏断言 guard（三类判据之一：allocs/bytes 回基线）。
/// Drop 兜底尽力而为——panic-in-drop 会被吞，**规范用法=测试尾显式 [`LeakGuard::check`]**
/// /`expect_empty`/fx 侧 `expect_idle`。
pub struct LeakGuard {
    baseline_allocs: usize,
    baseline_bytes: usize,
    enabled: bool,
}

impl LeakGuard {
    /// 记当前基线。global_allocator 未声明 [`CountingAllocator`] 时 enabled=false
    /// 全 no-op（诚实降级不虚报）。
    pub fn arm() -> Self {
        // 探测：分配一枚探针看计数器动不动——动=计数分配器在岗；不动=裸 System（或其他
        // 分配器），降级。black_box 防 LLVM 消除 malloc/free 对造成假阴性。
        let before = LIVE_ALLOCS.load(Ordering::Relaxed);
        let probe = Vec::<u8>::with_capacity(64);
        std::hint::black_box(&probe);
        let moved = LIVE_ALLOCS.load(Ordering::Relaxed) > before;
        drop(probe);
        Self {
            baseline_allocs: LIVE_ALLOCS.load(Ordering::Relaxed),
            baseline_bytes: LIVE_BYTES.load(Ordering::Relaxed),
            enabled: moved,
        }
    }

    /// allocs/bytes 回基线=Ok；否则 Err(LeakReport)。
    pub fn check(&self) -> Result<(), LeakReport> {
        if !self.enabled {
            return Ok(());
        }
        let allocs_delta =
            LIVE_ALLOCS.load(Ordering::Relaxed) as isize - self.baseline_allocs as isize;
        let bytes_delta =
            LIVE_BYTES.load(Ordering::Relaxed) as isize - self.baseline_bytes as isize;
        if allocs_delta == 0 && bytes_delta == 0 {
            Ok(())
        } else {
            Err(LeakReport {
                allocs_delta,
                bytes_delta,
            })
        }
    }
}

impl Drop for LeakGuard {
    fn drop(&mut self) {
        if let Err(report) = self.check() {
            eprintln!("LeakGuard drop 兜底（尽力而为，请改用显式 check()）: {report:?}");
        }
    }
}

/// 泄漏报告：相对基线的净增量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeakReport {
    /// 分配次数净增量（相对基线）。
    pub allocs_delta: isize,
    /// 分配字节净增量（相对基线）。
    pub bytes_delta: isize,
}

/// 判据二：注册表排空断言——非空即 panic，带 len/capacity 现场。
/// 判据三（任务面 expect_idle）在 fx.rs——依赖 FxScope 类型故放彼处。
pub fn expect_empty<K: Ord, V>(reg: &BoundedRegistry<K, V>) {
    assert!(
        reg.is_empty(),
        "registry not drained: len={} capacity={}",
        reg.len(),
        reg.capacity()
    );
}

// ═══ MemoryPool：内存池特化（pool feature——PoolManaged 契约 + PoolBase 组合件）═══
//
// 形态：memgov 池三件（有界槽+RAII 归还+获取超时必填 D21）升一等（armor #26 /
// 调研档 §4.2）；管理面（校验/水位线/审计/溢出计数）委托 `crate::pool::PoolBase`
// 组合件，本块只写槽位借还特化面。
//
// 并发形态（性能矩阵③④⑦的机制基础）：
// - **锁不嵌套**：acquire 只取槽位锁；归还/读数=PoolBase 账本锁与槽位锁**顺序**
//   各取（绝不同时持有）——零死锁面；
// - **边界副本**：create 配额判定（`created < max`）必须与 `created` 变更原子，
//   副本写只发生在 `resize`（`resizing` 串行闸保证与 PoolBase 账本同序成对写，
//   并发 resize 不交错）；热路径零跨锁判定；
// - **伪共享防（⑦）**：`#[repr(C)]` 定序 + align(64) 垫片，把账本锁/槽位锁/
//   condvar 三条热词线分进独立 cache line。
#[cfg(feature = "pool")]
mod memory_pool {
    use std::borrow::Borrow;
    use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
    use std::sync::{Condvar, Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    use skeleton::pool::{
        PoolAuditEvent, PoolAuditTicket, PoolManaged, PoolMetrics, PoolResizeError,
    };

    use crate::pool::{PoolBase, AUDIT_RING_DEFAULT_CAP};

    /// std Mutex 取锁：中毒 = 持锁方已 panic——fail-fast 传播（同 `pool::lock`
    /// 纪律）。收口一处，调用点只见加锁。
    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex
            .lock()
            .expect("memgov pool lock poisoned (prior holder panicked)")
    }

    /// cache line 隔离垫（性能矩阵⑦）：ZST + align(64)，在 `#[repr(C)]` 定序下
    /// 把后继字段推到下一条 cache line——账本锁词与槽位锁词相邻时，跨锁竞争
    /// 会互相打脏对方线（伪共享），垫片隔离后两条锁线独立弹跳。
    #[repr(align(64))]
    struct CachePad(());

    /// 内存池调参形状（三参封顶之二：池深+闲槽上限；第三参=获取超时，逐调用
    /// 必填——`acquire` 签名层类型强制 D21，无默认无限等）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MemoryTuning {
        /// 池深：idle + borrowed 总量上限。
        pub max: usize,
        /// 空闲槽上限（归还留存判据之一）。
        pub idle_cap: usize,
    }

    /// 内存池指标：共享核心形 + 内存池特有读数（闲槽上限）。经 [`Borrow`] 可达
    /// [`PoolMetrics`]（宿主对任何池取同一组核心读数——契约面）。
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub struct MemoryPoolMetrics {
        /// 共享核心读数（max/idle/borrowed/dropped_overflow/watermark）。
        pub core: PoolMetrics,
        /// 闲槽上限（resize 可调）。
        pub idle_cap: usize,
    }

    impl Borrow<PoolMetrics> for MemoryPoolMetrics {
        fn borrow(&self) -> &PoolMetrics {
            &self.core
        }
    }

    /// [`MemoryPool::acquire`] 超时（D21 的显式错面）：期限窗内既无闲槽、配额
    /// 也无空位。值语义 `Copy` 零分配；`Display` 即用即格式化（只走错误路径，
    /// 热路径禁格式化）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PoolAcquireTimeout;

    impl std::fmt::Display for PoolAcquireTimeout {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "pool acquire timed out: no slot available within the caller-supplied deadline"
            )
        }
    }

    impl std::error::Error for PoolAcquireTimeout {}

    /// 槽位账（槽位锁单权威内）：闲置对象栈 + 存活件数 + 边界副本。
    struct PoolInner<T> {
        /// 闲置对象栈（LIFO——热件常驻热缓存）；容量=历史闲槽高水位，缩容
        /// truncate 保容量（归还再留存零再分配）。
        idle: Vec<T>,
        /// 池宇宙存活件数（= idle.len() + 借出在外）。缩容弃 idle/溢出丢弃/
        /// 工厂 panic 回滚均递减——配额随件回吐。
        created: usize,
        /// 池深副本（只由 resize 经串行闸写；create 配额判定用）。
        max: usize,
        /// 闲槽上限副本（只由 resize 经串行闸写；缩容 truncate 与读数用）。
        idle_cap: usize,
    }

    /// 内存池：有界对象池特化（memgov 池三件升一等）。借还面（槽位借还/等待/
    /// RAII 归还）在本类型，管理面（动态调参/水位线/池域审计/溢出计数）经
    /// [`PoolManaged`] + [`PoolBase`] 全套自动到手。
    ///
    /// # 三参封顶（Less is More）
    ///
    /// max / idle_cap 在 [`MemoryTuning`]（resize 可调）；获取超时逐调用必填
    /// （D21——拿不到=显式 [`PoolAcquireTimeout`]，结构上不存在无限等模式，
    /// `Duration::ZERO` = 非阻塞尝试）。其余（初始预热、栈序留存、扩缩时机）
    /// 内部自适应，不设旋钮。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::pool::PoolManaged;
    /// use skeleton_infra::memgov::{MemoryPool, MemoryTuning};
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool: MemoryPool<Vec<u8>> = MemoryPool::new(4, 2, Vec::new);
    /// let mut buf = pool.acquire(Duration::from_millis(10))?;
    /// buf.push(7);
    /// assert_eq!(pool.metrics().core.borrowed, 1);
    /// drop(buf); // RAII：Drop 即还（禁手动还）
    /// assert_eq!(pool.metrics().core.idle, 1);
    ///
    /// // 管理面：动态调参 + 池域审计流（票据 Drop 即关流）
    /// pool.resize(MemoryTuning { max: 8, idle_cap: 4 })?;
    /// let mut kinds = Vec::new();
    /// let ticket = PoolManaged::subscribe_audit(&pool, move |e| kinds.push(e.kind));
    /// let again = pool.acquire(Duration::from_millis(10))?; // 复用同一件
    /// assert_eq!(&*again, &[7]);
    /// drop(again);
    /// drop(ticket);
    /// # Ok(())
    /// # }
    /// ```
    #[repr(C)]
    pub struct MemoryPool<T, F = fn() -> T> {
        base: PoolBase,
        pad0: CachePad,
        inner: Mutex<PoolInner<T>>,
        pad1: CachePad,
        cv: Condvar,
        /// 调参串行闸：并发 resize 对账本锁与槽位锁**顺序**各写（锁不嵌套），
        /// 两把写会交错出 base/inner 边界不一致——闸保证「base 写→inner 写」
        /// 成对同序。热路径零触碰。
        resizing: Mutex<()>,
        factory: F,
    }

    impl<T, F: Fn() -> T> MemoryPool<T, F> {
        /// 构造。非法边界 panic fail-fast（委托 [`PoolBase::new`] 同一套规则：
        /// `max == 0` / `idle_cap == 0` / `idle_cap > max`）；获取超时不设默认
        /// （逐调用必填，D21）。
        ///
        /// # Panics
        ///
        /// 同 [`PoolBase::new`]：恒不合法的配置必是 bug。
        pub fn new(max: usize, idle_cap: usize, factory: F) -> Self {
            Self {
                base: PoolBase::new(max, idle_cap),
                pad0: CachePad(()),
                inner: Mutex::new(PoolInner {
                    idle: Vec::new(),
                    created: 0,
                    max,
                    idle_cap,
                }),
                pad1: CachePad(()),
                cv: Condvar::new(),
                resizing: Mutex::new(()),
                factory,
            }
        }

        /// 借出一件（超时必填，D21）：闲槽直取（LIFO）；空槽且配额未满则工厂
        /// 新建（**锁外**构造——工厂可能昂贵，panic 经配额回滚后如实传播）；
        /// 否则 condvar park 至期限（零自旋），期限到=显式 Err。resize 即时
        /// 生效：等待谓词每 wake 复判（调参 notify_all，扩容零阻塞等待者）。
        ///
        /// # Errors
        ///
        /// [`PoolAcquireTimeout`]：期限窗内无件可借。
        pub fn acquire(&self, timeout: Duration) -> Result<PoolGuard<'_, T>, PoolAcquireTimeout> {
            let deadline = Instant::now() + timeout;
            let mut inner = lock(&self.inner);
            loop {
                if let Some(obj) = inner.idle.pop() {
                    drop(inner);
                    self.base.note_idle_delta(-1);
                    self.base.note_borrow();
                    return Ok(self.guard(obj));
                }
                if inner.created < inner.max {
                    inner.created += 1;
                    drop(inner);
                    let obj = match catch_unwind(AssertUnwindSafe(|| (self.factory)())) {
                        Ok(obj) => obj,
                        Err(payload) => {
                            // 配额回滚 + 唤醒（等待者可重试），panic 原样传播
                            lock(&self.inner).created -= 1;
                            self.cv.notify_one();
                            resume_unwind(payload);
                        }
                    };
                    self.base.note_borrow();
                    return Ok(self.guard(obj));
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(PoolAcquireTimeout);
                }
                let (guard, _) = self
                    .cv
                    .wait_timeout(inner, deadline - now)
                    .expect("memgov pool lock poisoned (prior holder panicked)");
                inner = guard; // spurious/notify/超时统一回环复判谓词
            }
        }

        /// 运行时调参：事前校验非法参数入口拒绝（不改任何状态）；生效后缩容
        /// 只弃 idle 槽（超限闲置件**锁外**析构，`created` 同步递减=配额回吐），
        /// 借出句柄不受影响——归还时由 [`PoolBase::note_return`] 按新边界裁决
        /// （槽满=丢弃+`dropped_overflow` 计数）。末尾 notify_all：扩容即时
        /// 唤醒等待者（调参暂停窗零等待）。
        ///
        /// # Errors
        ///
        /// [`PoolResizeError`]：非法参数（拒绝路径零状态变更）。
        pub fn resize(&self, tuning: MemoryTuning) -> Result<(), PoolResizeError> {
            let _serial = lock(&self.resizing);
            self.base.resize(tuning.max, tuning.idle_cap)?;
            let excess = {
                let mut inner = lock(&self.inner);
                inner.max = tuning.max;
                inner.idle_cap = tuning.idle_cap;
                if inner.idle.len() > tuning.idle_cap {
                    let excess = inner.idle.split_off(tuning.idle_cap);
                    inner.created = inner
                        .created
                        .checked_sub(excess.len())
                        .expect("created below idle on shrink (bookkeeping bug)");
                    Some(excess)
                } else {
                    None
                }
            };
            drop(_serial);
            if let Some(excess) = excess {
                self.base.note_idle_delta(-(excess.len() as isize));
                drop(excess);
            }
            self.cv.notify_all();
            Ok(())
        }

        /// 指标快照：核心形（PoolBase 账本单锁 Copy）+ 特有读数 `idle_cap`。
        /// O(1) 顺序两锁零分配（实测锚见 `examples/memory_pool_profile.rs`）。
        pub fn metrics(&self) -> MemoryPoolMetrics {
            let core = self.base.metrics();
            let idle_cap = lock(&self.inner).idle_cap;
            MemoryPoolMetrics { core, idle_cap }
        }

        /// 取走环内审计事件（事后取证对账面；环随取随清——委托
        /// [`PoolBase::take_audit_events`]，批量读侧非热路径）。
        pub fn take_audit_events(&self) -> Vec<PoolAuditEvent> {
            self.base.take_audit_events()
        }

        /// 句柄装配（借出两路共用：闲槽直取 / 工厂新建）。
        fn guard(&self, obj: T) -> PoolGuard<'_, T> {
            PoolGuard {
                base: &self.base,
                inner: &self.inner,
                cv: &self.cv,
                slot: Some(obj),
            }
        }
    }

    impl<T, F: Fn() -> T> PoolManaged for MemoryPool<T, F> {
        type Tuning = MemoryTuning;
        type Metrics = MemoryPoolMetrics;

        fn resize(&self, tuning: MemoryTuning) -> Result<(), PoolResizeError> {
            MemoryPool::resize(self, tuning)
        }

        fn metrics(&self) -> MemoryPoolMetrics {
            MemoryPool::metrics(self)
        }

        fn subscribe_audit(
            &self,
            f: impl FnMut(&PoolAuditEvent) + Send + 'static,
        ) -> PoolAuditTicket {
            self.base.subscribe_audit(AUDIT_RING_DEFAULT_CAP, f)
        }
    }

    /// 借出句柄（RAII）：[`Deref`](std::ops::Deref)/[`DerefMut`](std::ops::DerefMut)
    /// 到池件；**Drop 即还**（禁手动还——armor #26）：闲槽未满留存（LIFO 入栈
    /// +唤醒等待者），槽满（缩容后超限/闲槽上限满）对象直接析构+计数，归还
    /// 永不阻塞永不报错。
    pub struct PoolGuard<'p, T> {
        base: &'p PoolBase,
        inner: &'p Mutex<PoolInner<T>>,
        cv: &'p Condvar,
        slot: Option<T>,
    }

    impl<T> PoolGuard<'_, T> {
        /// 池件本体引用（`slot` 在 Drop 前恒 Some——Drop 路径 take）。
        fn slot_ref(&self) -> &T {
            self.slot
                .as_ref()
                .expect("guard slot empty before drop (bookkeeping bug)")
        }
    }

    impl<T> std::ops::Deref for PoolGuard<'_, T> {
        type Target = T;

        fn deref(&self) -> &T {
            self.slot_ref()
        }
    }

    impl<T> std::ops::DerefMut for PoolGuard<'_, T> {
        fn deref_mut(&mut self) -> &mut T {
            self.slot
                .as_mut()
                .expect("guard slot empty before drop (bookkeeping bug)")
        }
    }

    impl<T> Drop for PoolGuard<'_, T> {
        fn drop(&mut self) {
            let obj = self
                .slot
                .take()
                .expect("guard slot empty on drop (bookkeeping bug)");
            if self.base.note_return() {
                lock(self.inner).idle.push(obj);
                self.cv.notify_one();
            } else {
                // 槽满：对象离开池宇宙——配额回吐（等待者可新建）+ 锁外析构
                let mut inner = lock(self.inner);
                inner.created = inner
                    .created
                    .checked_sub(1)
                    .expect("created underflow on overflow drop (bookkeeping bug)");
                drop(inner);
                self.cv.notify_one();
                drop(obj);
            }
        }
    }
}

#[cfg(feature = "pool")]
pub use memory_pool::{MemoryPool, MemoryPoolMetrics, MemoryTuning, PoolAcquireTimeout, PoolGuard};

#[cfg(test)]
mod tests {
    use super::*;

    /// ① insert/get/remove/len 恒等。
    #[test]
    fn insert_get_remove_len_roundtrip() {
        let mut reg = BoundedRegistry::new(4, Eviction::Reject);
        assert!(reg.is_empty());
        assert_eq!(reg.capacity(), 4);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(2, "b").expect("within capacity");
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&1), Some(&"a"));
        assert_eq!(reg.get(&9), None);
        assert_eq!(reg.remove(&1), Some("a"));
        assert_eq!(reg.remove(&1), None);
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.remove(&2), Some("b"));
        assert!(reg.is_empty());
    }

    /// ② 满+Reject=Err(BoundError)，原内容不动。
    #[test]
    fn reject_at_capacity_leaves_registry_untouched() {
        let mut reg = BoundedRegistry::new(2, Eviction::Reject);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(2, "b").expect("within capacity");
        let err = reg.insert(3, "c").expect_err("full + Reject");
        assert_eq!(err.capacity, 2);
        assert_eq!(
            err.to_string(),
            "bounded registry full (capacity=2, eviction=Reject)"
        );
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&1), Some(&"a"));
        assert_eq!(reg.get(&2), Some(&"b"));
        assert_eq!(reg.get(&3), None);
    }

    /// ③ 满+EvictOldest=首键淘汰+新键入。
    #[test]
    fn evict_oldest_pops_min_key_then_inserts() {
        // K 契约实证：乱序插入（10 后插 5）时淘汰的是首键 5——如实退化 EvictMinKey。
        let mut reg = BoundedRegistry::new(2, Eviction::EvictOldest);
        reg.insert(10, "ten").expect("within capacity");
        reg.insert(5, "five").expect("within capacity");
        reg.insert(7, "seven").expect("eviction makes room");
        assert_eq!(reg.len(), 2);
        assert_eq!(reg.get(&5), None);
        assert_eq!(reg.get(&10), Some(&"ten"));
        assert_eq!(reg.get(&7), Some(&"seven"));
    }

    /// ④ 同 key 覆盖不触发淘汰。
    #[test]
    fn overwrite_existing_key_neither_evicts_nor_grows() {
        let mut reg = BoundedRegistry::new(1, Eviction::EvictOldest);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(1, "b").expect("overwrite keeps len");
        assert_eq!(reg.len(), 1);
        assert_eq!(reg.get(&1), Some(&"b"));
        reg.insert(2, "c").expect("evict oldest to fit new key");
        assert_eq!(reg.get(&1), None);
        assert_eq!(reg.get(&2), Some(&"c"));

        let mut rej = BoundedRegistry::new(2, Eviction::Reject);
        rej.insert(1, "a").expect("within capacity");
        rej.insert(1, "b").expect("overwrite keeps len");
        assert_eq!(rej.len(), 1);
        rej.insert(2, "c")
            .expect("still one slot free after overwrite");
        rej.insert(3, "d")
            .expect_err("full + Reject, overwrite did not grow");
    }

    /// ⑤ iter 升序确定序（乱序插入断言序）。
    #[test]
    fn iter_is_ascending_deterministic_order() {
        let mut reg = BoundedRegistry::new(8, Eviction::Reject);
        for k in [5, 1, 3] {
            reg.insert(k, k * 10).expect("within capacity");
        }
        let keys: Vec<_> = reg.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 3, 5]);
        let pairs: Vec<_> = reg.iter().map(|(k, v)| (*k, *v)).collect();
        assert_eq!(pairs, vec![(1, 10), (3, 30), (5, 50)]);
    }

    /// ⑩ 未启用 CountingAllocator 时 arm→check=Ok（降级 no-op 诚实）。
    /// 本断言同时是绊线：若将来有人给 lib 测试二进制挂了计数分配器，此测试会立即红。
    #[test]
    fn leak_guard_degrades_to_noop_without_counting_allocator() {
        let guard = LeakGuard::arm();
        assert!(
            !guard.enabled,
            "lib test binary must NOT declare a CountingAllocator global allocator"
        );
        let _live = Vec::<u8>::with_capacity(64);
        assert!(guard.check().is_ok());
    }

    /// 零覆盖 #8：bounded(0) 构造期 panic——恒满注册表必是 bug，fail-fast
    /// 不给「永不可能插入成功」的僵尸实例留活口（构造面 fail-closed）。
    #[test]
    #[should_panic(expected = "max == 0 is a bug")]
    fn bounded_zero_capacity_panics_fail_fast() {
        let _ = BoundedRegistry::<u32, u32>::new(0, Eviction::Reject);
    }

    /// cqb① Borrow 泛型查找：借用形（`&str`）与 owned 形（`String`）同结果，
    /// 命中/未命中两态 parity（armor：借用/owned 同源单一 `Ord`——Windows
    /// EnvKey 式 Eq/Ord 分裂在此结构性不可表达）。
    #[test]
    fn borrow_lookup_matches_owned_lookup() {
        let mut reg: BoundedRegistry<String, u8> = BoundedRegistry::new(4, Eviction::Reject);
        reg.insert("alpha".to_owned(), 1).expect("within capacity");
        reg.insert("beta".to_owned(), 2).expect("within capacity");
        assert_eq!(reg.get("alpha"), Some(&1));
        assert_eq!(reg.get("zeta"), None);
        assert_eq!(reg.get(&"alpha".to_owned()), Some(&1));
        assert_eq!(reg.get(&"zeta".to_owned()), None);
    }

    /// cqb② get_mut 值面改动不伤表序（armor：键面类型级不可达，编译面钉在
    /// get_mut 文档 compile_fail doctest；本测为运行时面——V 内键镜像字段
    /// 随便改写，迭代序与查找只由 K 决定）。
    #[test]
    fn mutation_cannot_affect_ordering() {
        let mut reg: BoundedRegistry<u32, (u32, &'static str)> =
            BoundedRegistry::new(4, Eviction::Reject);
        reg.insert(2, (2, "two")).expect("within capacity");
        reg.insert(1, (1, "one")).expect("within capacity");
        reg.insert(3, (3, "three")).expect("within capacity");
        if let Some(v) = reg.get_mut(&2) {
            v.0 = 999; // V.0 是键的镜像副本，改它动不了表序
        }
        let keys: Vec<_> = reg.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 2, 3]);
        assert_eq!(reg.get(&2), Some(&(999, "two")));
    }

    /// cqb③ get_mut 借用形可变读侧：值就地更新、owned 读可见；未命中=`None`。
    #[test]
    fn borrow_mut_lookup_updates_value_in_place() {
        let mut reg: BoundedRegistry<String, u32> = BoundedRegistry::new(4, Eviction::Reject);
        reg.insert("counter".to_owned(), 1)
            .expect("within capacity");
        if let Some(v) = reg.get_mut("counter") {
            *v += 1;
        }
        assert_eq!(reg.get("counter"), Some(&2));
        assert_eq!(reg.get_mut("missing"), None);
    }

    /// cqb④ Clone=深拷贝（PM 改判：Registry 是值容器非生成器句柄）：克隆体与
    /// 原件后续变更互不可见。
    #[test]
    fn clone_is_deep_copy_independent() {
        let mut reg = BoundedRegistry::new(4, Eviction::Reject);
        reg.insert(1, "a").expect("within capacity");
        reg.insert(2, "b").expect("within capacity");
        let mut copy = reg.clone();
        reg.insert(3, "c").expect("within capacity");
        assert_eq!(copy.get(&3), None);
        assert_eq!(copy.len(), 2);
        copy.remove(&1);
        assert_eq!(reg.get(&1), Some(&"a"));
        assert_eq!(reg.len(), 3, "克隆体的 remove 不回流原件（原件已含 3 键）");
    }

    /// cqb⑤ Clone 后容量语义逐句柄保持（armor：map/上限/策略同住一个值同拷，
    /// 无「上限存句柄字段」的漂移面）：克隆体满容仍 fail-closed、淘汰独立演进、
    /// 迭代确定序同原件。
    #[test]
    fn clone_preserves_capacity_limit() {
        let mut rej = BoundedRegistry::new(2, Eviction::Reject);
        rej.insert(1, "a").expect("within capacity");
        rej.insert(2, "b").expect("within capacity");
        let mut rej_copy = rej.clone();
        assert_eq!(rej_copy.capacity(), 2);
        assert!(
            rej_copy.insert(3, "c").is_err(),
            "clone keeps Reject fail-closed at capacity"
        );
        assert_eq!(rej_copy.len(), 2);

        let mut evict = BoundedRegistry::new(2, Eviction::EvictOldest);
        evict.insert(1, "a").expect("within capacity");
        evict.insert(2, "b").expect("within capacity");
        let mut evict_copy = evict.clone();
        evict_copy
            .insert(3, "c")
            .expect("eviction still works on clone");
        assert_eq!(evict_copy.get(&1), None);
        assert_eq!(
            evict.get(&1),
            Some(&"a"),
            "clone eviction does not touch source"
        );

        let keys: Vec<_> = rej_copy.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![1, 2], "clone keeps ascending iteration order");
    }
}
