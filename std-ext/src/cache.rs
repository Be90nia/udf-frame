//! LRU+TTL 通用缓存（memgov 邻接扩展域）。
//!
//! log-embed 埋点（级别规范：正常流转=debug / 显式清扫=info；target
//! `skeleton::cache[::phase]`，字段化纪律见 lib.rs `embed_event!` 注）：
//! evict=TTL 惰性单条 debug（kind/key 指纹/过期累计）、满插清扫 debug
//!（kind/key 指纹/僵尸数，真回收才发）、容量淘汰 debug（kind/被逐者指纹/
//! LRU 序/残留）；purge=显式清扫 info（回收数/残留）。**键身份以
//! `DefaultHasher`（定长种子）指纹承载**——`K` 无 Debug/Display bound，加
//! bound=公共 API 破坏性变更（未经拍板不做）；指纹供进程内/同构建日志关联，
//! 跨 Rust 版本不承诺稳定。
//!
//! 常驻零依赖件：纯 std，无 feature 门。与 [`crate::memgov`] 的分工：memgov=
//! 有界容器族（BTreeMap 序淘汰，`K: Ord`，首键退化语义）；本模块=精确 LRU+
//! TTL 缓存域（`K: Eq + Hash + Clone`）。memgov `Eviction` 头注「LRU/TTL→moka
//! 对表，不在此实现」的缺口由本模块兑现（moka 重依赖不采——零依赖纪律；
//! Guava Cache / moka 语义面对表见 reports/2026-10-16-b4-cache.md）。
//!
//! # O(1) 结构（拍板点：真双向链，非键序退化）
//!
//! `HashMap<K, u32>` 键→槽号 + `Vec<Slot>` arena + 槽内 `u32 prev/next` 双向
//! 链 + free list。**零 unsafe**（索引代指针）。两条被否路线的坑面：
//!
//! - `std::collections::LinkedList`：cursor 无跨突变稳定性、按元素定位 O(n)
//!   ——move-to-front 退化 O(n)，结构性不可用；
//! - BTreeMap 键序退化：get=touch 需重组键（recency 进键），同样逃不掉
//!   `K: Clone` 且 O(log n)——被 arena 严格支配，不取。
//!
//! `K: Clone` 的来源：std `HashMap` 键单一所有权（map 与 arena 槽各需一份），
//! clone 仅发生在 `insert` 写路径一次（moka 同约定）；**get 热路径零 clone
//! 零分配**（`map.get(&k)` 借用查询 + u32 链操作）。
//!
//! # 语义拍板
//!
//! | 面 | 拍板 | 依据 |
//! |---|---|---|
//! | get 是否 touch | **touch**（刷新 recency） | Guava/moka/lru 同口径 |
//! | 不 touch 读面 | `peek` / `peek_mut`（peek_mut=「更新值不刷新 recency」显式面） | lru crate 高频 issue（get 强制 &mut 惊吓调用方） |
//! | TTL 形态 | 逐条 expire-after-write（insert 带过期，覆盖重置计时） | Guava expireAfterWrite 逐键投影 |
//! | 双界交互 | 满时**先清过期再逐 LRU 尾**（过期条目永不把活条目挤出去，当清得动时） | Guava maintenance 语义的确定性化 |
//! | 惰性过期 | get/peek 触碰即回收 + `expired` 计数（fail-visible）；`len()`=物理条数（回收前僵尸占容，如实声明）；`purge_expired` 显式清扫 | moka「过期条目暂占容量至 maintenance」的显式化 |
//! | 时钟 | 只消费单调面 `now_instant`（clock armor #19；回拨结构性排除） | MockClock 后退类型层不可表达 |
//! | 并发 | **单线程 `&mut self`**（并发另批，本单如实声明，无内部锁） | 契约收窄 |
//!
//! # Examples
//!
//! ```
//! use std_ext::cache::LruCache;
//!
//! let mut c = LruCache::new(2);
//! c.insert("a", 1);
//! c.insert("b", 2);
//! assert_eq!(c.get(&"a"), Some(&1)); // get=touch：a 变最新
//! c.insert("c", 3); // 满逐 LRU 尾 → 逐 b（a 刚被 touch）
//! assert_eq!(c.get(&"b"), None);
//! assert_eq!(c.get(&"a"), Some(&1));
//! assert_eq!(c.get(&"c"), Some(&3));
//! ```
//!
//! TTL 面走 [`TtlCache`]（MockClock 确定性时间线，零真睡测试）：
//!
//! ```
//! use std::sync::Arc;
//! use std::time::Duration;
//! use std_ext::cache::TtlCache;
//! use skeleton::clock::{Clock, MockClock};
//!
//! let clock = Arc::new(MockClock::new());
//! let mut c = TtlCache::new(clock.clone(), 4);
//! c.insert("k", 1, Duration::from_millis(100));
//! clock.advance(Duration::from_millis(99));
//! assert_eq!(c.get(&"k"), Some(&1)); // 99ms < 100ms：活
//! clock.advance(Duration::from_millis(1));
//! assert_eq!(c.get(&"k"), None); // 惰性过期：触碰即回收
//! assert_eq!(c.expired(), 1); // fail-visible 过期计数
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use std::time::{Duration, Instant};

use skeleton::clock::Clock;

/// 键指纹（log-embed 事件字段）：`DefaultHasher::new` 定长种子=进程内确定，
/// 日志关联用（跨 Rust 版本不承诺稳定，仅身份对账非持久标识）。
#[cfg(feature = "log-embed")]
fn key_fingerprint<K: Hash>(k: &K) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

/// 链尾哨兵：`u32::MAX` 不可能成为槽号（构造期 `cap <= u32::MAX - 1` 断言）。
const NONE: u32 = u32::MAX;

/// map→live 槽不变量（所有 mutator 共同维护：free 槽不入 map、live 槽键值恒存在）。
const INV_LIVE: &str = "cache 不变量：map 索引恒指向 live 槽（键值恒存在）";

/// arena 槽：键值取 `Option` 承载「腾位」（零 unsafe 下键值出槽的占位代价，
/// `Option` 无 niche 时各多 1 字节标签——用安全换掉裸指针自指链）。
#[derive(Debug)]
struct Slot<K, V> {
    key: Option<K>,
    value: Option<V>,
    /// recency 链：prev/next 槽号（`NONE`=边界）。
    prev: u32,
    next: u32,
}

/// O(1) LRU 内核（私有）：`HashMap<K, u32>` + arena + 双向链 + free list。
/// head=最新，tail=最旧；`len` 以 map 为权威（free 槽不入 map）。
#[derive(Debug)]
struct LruCore<K, V> {
    map: HashMap<K, u32>,
    slots: Vec<Slot<K, V>>,
    free: Vec<u32>,
    head: u32,
    tail: u32,
    cap: usize,
}

impl<K: Eq + Hash + Clone, V> LruCore<K, V> {
    fn new(cap: usize) -> Self {
        assert!(
            cap > 0 && cap <= (u32::MAX - 1) as usize,
            "cache::new: cap 必须 >0 且 <= u32::MAX-1（槽号 u32 哨兵 NONE=u32::MAX）"
        );
        Self {
            map: HashMap::with_capacity(cap),
            slots: Vec::with_capacity(cap),
            free: Vec::new(),
            head: NONE,
            tail: NONE,
            cap,
        }
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn capacity(&self) -> usize {
        self.cap
    }

    /// 键→槽号（只读查找，不 touch、不分配）。
    fn idx_of(&self, k: &K) -> Option<u32> {
        self.map.get(k).copied()
    }

    /// 链摘除（prev/next 复位 NONE；head/tail 边界同步）。
    fn unlink(&mut self, idx: u32) {
        let (prev, next) = {
            let s = &self.slots[idx as usize];
            (s.prev, s.next)
        };
        match prev {
            NONE => self.head = next,
            p => self.slots[p as usize].next = next,
        }
        match next {
            NONE => self.tail = prev,
            n => self.slots[n as usize].prev = prev,
        }
        let s = &mut self.slots[idx as usize];
        s.prev = NONE;
        s.next = NONE;
    }

    /// 链头插入（新最新）。
    fn push_front(&mut self, idx: u32) {
        let old_head = self.head;
        {
            let s = &mut self.slots[idx as usize];
            s.prev = NONE;
            s.next = old_head;
        }
        if old_head != NONE {
            self.slots[old_head as usize].prev = idx;
        } else {
            self.tail = idx;
        }
        self.head = idx;
    }

    /// touch：已在头则免操作（同键连击 O(1) 恒定）。
    fn move_to_front(&mut self, idx: u32) {
        if self.head != idx {
            self.unlink(idx);
            self.push_front(idx);
        }
    }

    /// 逐 LRU 尾（最旧）：返回 (键, 值)；空链 `None`。map 条目同步清除。
    fn pop_lru(&mut self) -> Option<(K, V)> {
        let idx = self.tail;
        if idx == NONE {
            return None;
        }
        self.unlink(idx);
        let slot = &mut self.slots[idx as usize];
        let key = slot.key.take().expect(INV_LIVE);
        let value = slot.value.take().expect(INV_LIVE);
        self.map.remove(&key);
        self.free.push(idx);
        Some((key, value))
    }

    /// 不 touch 读（只读借用面）。
    fn peek(&self, k: &K) -> Option<&V> {
        let idx = self.idx_of(k)?;
        self.slots[idx as usize].value.as_ref()
    }

    /// 不 touch 写值面（「更新值不刷新 recency」语义的唯一入口）。
    fn peek_mut(&mut self, k: &K) -> Option<&mut V> {
        let idx = self.idx_of(k)?;
        self.slots[idx as usize].value.as_mut()
    }

    /// touch 读 + 可变面（get=touch 语义的本体）。
    fn get_mut(&mut self, k: &K) -> Option<&mut V> {
        let idx = self.idx_of(k)?;
        self.move_to_front(idx);
        self.slots[idx as usize].value.as_mut()
    }

    /// 插入/覆盖：已存在=更新值+touch（len 不变，不触发淘汰——BoundedRegistry
    /// 同款语义）；新键满=先逐 LRU 尾再插。
    fn insert(&mut self, k: K, v: V) {
        if let Some(&idx) = self.map.get(&k) {
            self.slots[idx as usize].value = Some(v);
            self.move_to_front(idx);
            return;
        }
        if self.map.len() >= self.cap {
            // 埋点「容量淘汰」：被逐者恒 LRU 尾（order 字段=选择策略留痕）。
            let _evicted = self.pop_lru();
            embed_event!(
                debug,
                target: "skeleton::cache::evict",
                kind = "capacity",
                key_hash = _evicted.as_ref().map(|(k, _)| key_fingerprint(k)),
                order = "lru_tail",
                len_at_evict = self.map.len() as u64
            );
        }
        let idx = match self.free.pop() {
            Some(i) => i,
            None => {
                let i = self.slots.len();
                assert!(
                    i <= (u32::MAX - 1) as usize,
                    "cache 槽号溢出（cap 断言漏网）"
                );
                self.slots.push(Slot {
                    key: None,
                    value: None,
                    prev: NONE,
                    next: NONE,
                });
                i as u32
            }
        };
        self.map.insert(k.clone(), idx);
        let slot = &mut self.slots[idx as usize];
        slot.key = Some(k);
        slot.value = Some(v);
        self.push_front(idx);
    }

    /// 摘除（返回值；不判过期不 touch——物理面）。
    fn remove(&mut self, k: &K) -> Option<V> {
        let idx = self.idx_of(k)?;
        self.unlink(idx);
        let slot = &mut self.slots[idx as usize];
        let key = slot.key.take().expect(INV_LIVE);
        let value = slot.value.take().expect(INV_LIVE);
        self.map.remove(&key);
        self.free.push(idx);
        Some(value)
    }

    /// 条件清扫：`keep` 返回 false 的条目回收（arena 序遍历，确定序）。
    /// 返回回收数。
    fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) -> usize {
        let mut purged = 0;
        for idx in 0..self.slots.len() {
            if self.slots[idx].key.is_none() {
                continue;
            }
            let keep_it = {
                let s = &mut self.slots[idx];
                keep(
                    s.key.as_ref().expect(INV_LIVE),
                    s.value.as_mut().expect(INV_LIVE),
                )
            };
            if !keep_it {
                let idx32 = idx as u32;
                self.unlink(idx32);
                let slot = &mut self.slots[idx];
                let key = slot.key.take().expect(INV_LIVE);
                slot.value = None;
                self.map.remove(&key);
                self.free.push(idx32);
                purged += 1;
            }
        }
        purged
    }

    /// recency 序迭代（最新→最旧；arena 链确定序）。
    fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        let mut idx = self.head;
        std::iter::from_fn(move || {
            if idx == NONE {
                return None;
            }
            let s = &self.slots[idx as usize];
            let item = (
                s.key.as_ref().expect(INV_LIVE),
                s.value.as_ref().expect(INV_LIVE),
            );
            idx = s.next;
            Some(item)
        })
    }
}

/// 精确 LRU 缓存：容量上界 + O(1) `get`/`insert`（索引 arena 双向链，零 unsafe）。
///
/// 构造签名类型强制「上限必填」（无默认无界构造，[`crate::memgov::BoundedRegistry`]
/// 同款纪律）；满插入恒逐 LRU 尾（无 Reject 形——缓存的淘汰本来就是承诺）。
///
/// 并发面：单线程 `&mut self`，无内部锁（并发另批，如实声明）。
#[derive(Debug)]
pub struct LruCache<K, V> {
    core: LruCore<K, V>,
}

impl<K: Eq + Hash + Clone, V> LruCache<K, V> {
    /// 构造。`cap == 0` 构造期 panic（恒满缓存必是 bug，fail-fast 同 memgov）。
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            core: LruCore::new(cap),
        }
    }

    /// 插入/覆盖。已存在键=更新值+touch（len 不变，不触发淘汰）；
    /// 新键满=先逐 LRU 尾再插（被逐者静默丢弃，需要看见用 [`Self::pop_lru`]）。
    pub fn insert(&mut self, k: K, v: V) {
        self.core.insert(k, v);
    }

    /// 读（**touch**：命中刷新 recency，故需 `&mut self`）。只读共享场景用
    /// [`Self::peek`]。
    #[must_use]
    pub fn get(&mut self, k: &K) -> Option<&V> {
        self.core.get_mut(k).map(|v| &*v)
    }

    /// 读（不 touch，只读借用）。判定「在不在、值多少」而不扰动淘汰序。
    #[must_use]
    pub fn peek(&self, k: &K) -> Option<&V> {
        self.core.peek(k)
    }

    /// 可变读（**不 touch**：更新值不刷新 recency——该语义的显式面）。
    #[must_use]
    pub fn peek_mut(&mut self, k: &K) -> Option<&mut V> {
        self.core.peek_mut(k)
    }

    /// 物理存在判定（不 touch）。
    #[must_use]
    pub fn contains(&self, k: &K) -> bool {
        self.core.idx_of(k).is_some()
    }

    /// 摘除并返回值。
    #[must_use]
    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.core.remove(k)
    }

    /// 逐出 LRU 尾（最旧）并返回。
    #[must_use]
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        self.core.pop_lru()
    }

    /// 当前条目数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// 空判定。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.len() == 0
    }

    /// 容量上界（构造期定，不变）。
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// recency 序迭代（最新→最旧，确定序）。
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.core.iter()
    }
}

/// TTL 条目（TtlCache 内核值形）：过期时刻 + 值。
struct TtlEntry<V> {
    /// 过期时刻（单调面；`> now` 判活）。
    expires: Instant,
    value: V,
}

/// 带逐条 TTL 的 LRU 缓存：容量界 + 时钟注入（[`Clock`] 单调面）+ 惰性过期。
///
/// - `insert(k, v, ttl)`：expire-after-write（覆盖重置计时）；`ttl == 0` 与
///   `now + ttl` 溢出均构造期/调用期 panic（fail-closed：恒过期条目与不可
///   表达时刻必是调用方 bug，不静默）；
/// - 读面（get/peek/peek_mut）惰性过期：触碰即回收 + [`Self::expired`] 计数
///   （fail-visible）；`len()` 为物理条数（回收前僵尸占容，如实声明——
///   `purge_expired` 显式清扫 / 满插入自动清）；
/// - 满插入：先清过期（先过期先淘汰），仍满再逐 LRU 尾；
/// - 时钟只消费 `now_instant` 单调面（挂钟回拨结构性排除）。
///
/// 并发面：单线程 `&mut self`（并发另批，如实声明）。
pub struct TtlCache<K, V> {
    clock: Arc<dyn Clock>,
    core: LruCore<K, TtlEntry<V>>,
    expired: u64,
}

/// 手写 Debug：跳过时钟字段（`dyn Clock` 不可 Debug——注入件非状态），内容面
/// 按逻辑条目（recency 序，含僵尸）呈现。
impl<K: Eq + Hash + Clone + std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug
    for TtlCache<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TtlCache")
            .field("len", &self.core.len())
            .field("capacity", &self.core.capacity())
            .field("expired", &self.expired)
            .field(
                "entries",
                &self
                    .core
                    .iter()
                    .map(|(k, e)| (k, &e.value))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<K: Eq + Hash + Clone, V> TtlCache<K, V> {
    /// 构造。时钟注入（生产 [`skeleton::clock::SystemClock`]，测试
    /// [`skeleton::clock::MockClock`] 逐纳秒时间线）。`cap == 0` panic。
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, cap: usize) -> Self {
        Self {
            clock,
            core: LruCore::new(cap),
            expired: 0,
        }
    }

    /// 插入（逐条 TTL，expire-after-write）。覆盖已存在键=更新值+重置计时+
    /// touch，不触发淘汰。`ttl == 0` panic；`now + ttl` 溢出 panic（fail-closed）。
    pub fn insert(&mut self, k: K, v: V, ttl: Duration) {
        assert!(
            !ttl.is_zero(),
            "TtlCache::insert: ttl == 0 是恒过期条目（调用方 bug，fail-fast）"
        );
        let now = self.clock.now_instant();
        let expires = now.checked_add(ttl).expect(
            "TtlCache::insert: now + ttl 溢出 Instant 表示范围（fail-closed，clock armor 同款）",
        );
        if self.core.idx_of(&k).is_none() && self.core.len() >= self.core.capacity() {
            // 双界拍板：先过期先淘汰（单次 now 快照判定，确定性），仍满再逐 LRU 尾。
            let now = self.clock.now_instant();
            let purged = self.core.retain(|_, e| e.expires > now);
            self.expired += purged as u64;
            // 埋点「TTL 过期驱逐（满插清扫）」：真回收才发（僵尸数+触发键指纹；
            // 零僵尸路径由容量淘汰事件承载）。
            #[cfg(feature = "log-embed")]
            if purged > 0 {
                embed_event!(
                    debug,
                    target: "skeleton::cache::evict",
                    kind = "ttl_sweep",
                    key_hash = key_fingerprint(&k),
                    zombies = purged as u64
                );
            }
            if self.core.len() >= self.core.capacity() {
                // 埋点「容量淘汰」（TtlCache 站点）：被逐者恒 LRU 尾。
                let _evicted = self.core.pop_lru();
                embed_event!(
                    debug,
                    target: "skeleton::cache::evict",
                    kind = "capacity",
                    key_hash = _evicted.as_ref().map(|(evicted, _)| key_fingerprint(evicted)),
                    order = "lru_tail",
                    len_at_evict = self.core.len() as u64
                );
            }
        }
        self.core.insert(k, TtlEntry { expires, value: v });
    }

    /// 读（touch + 惰性过期）。过期触碰即回收并计入 [`Self::expired`]。
    #[must_use]
    pub fn get(&mut self, k: &K) -> Option<&V> {
        let now = self.clock.now_instant();
        if self.expire_if_due(k, now) {
            return None;
        }
        self.core.get_mut(k).map(|e| &e.value)
    }

    /// 读（不 touch + 惰性过期；需 `&mut self`：过期回收是突变）。
    #[must_use]
    pub fn peek(&mut self, k: &K) -> Option<&V> {
        let now = self.clock.now_instant();
        if self.expire_if_due(k, now) {
            return None;
        }
        self.core.peek(k).map(|e| &e.value)
    }

    /// 可变读（不 touch + 惰性过期）。
    #[must_use]
    pub fn peek_mut(&mut self, k: &K) -> Option<&mut V> {
        let now = self.clock.now_instant();
        if self.expire_if_due(k, now) {
            return None;
        }
        self.core.peek_mut(k).map(|e| &mut e.value)
    }

    /// 物理存在判定（**含僵尸**：过期未回收条目也算在——不 touch 不判定过期，
    /// 纯 map 查询）。活性判定走 [`Self::peek`]。
    #[must_use]
    pub fn contains(&self, k: &K) -> bool {
        self.core.idx_of(k).is_some()
    }

    /// 摘除并返回值（物理面：不判过期）。
    #[must_use]
    pub fn remove(&mut self, k: &K) -> Option<V> {
        self.core.remove(k).map(|e| e.value)
    }

    /// 逐出 LRU 尾（最旧；**物理面**：不判过期——过期与否不影响逐出序）。
    #[must_use]
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        self.core.pop_lru().map(|(k, e)| (k, e.value))
    }

    /// 显式清扫全部过期条目（arena 序确定遍历；单次 now 快照判定）。
    /// 返回回收数，计入 [`Self::expired`]。
    pub fn purge_expired(&mut self) -> usize {
        let now = self.clock.now_instant();
        let n = self.core.retain(|_, e| e.expires > now);
        self.expired += n as u64;
        // 埋点「purge」：显式清扫动作 info（回收数/残留）。
        embed_event!(
            info,
            target: "skeleton::cache::purge",
            purged = n as u64,
            len_after = self.core.len() as u64
        );
        n
    }

    /// 累计惰性过期回收数（fail-visible：过期不是静默 None）。
    #[must_use]
    pub fn expired(&self) -> u64 {
        self.expired
    }

    /// 物理条数（**含僵尸**——惰性过期语义下如实声明；活性面用读族 API）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// 空判定（物理面）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.len() == 0
    }

    /// 容量上界（构造期定，不变）。
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.core.capacity()
    }

    /// recency 序迭代（最新→最旧；**含僵尸**，无副作用面）。
    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> + '_ {
        self.core.iter().map(|(k, e)| (k, &e.value))
    }

    /// 惰性过期判定：到期则回收+计数，返回是否回收（满插入自动清共用）。
    fn expire_if_due(&mut self, k: &K, now: Instant) -> bool {
        let due = matches!(self.core.peek_mut(k), Some(e) if e.expires <= now);
        if due {
            self.core.remove(k);
            self.expired += 1;
            // 埋点「TTL 过期驱逐（惰性单条）」：触碰即回收留痕（键指纹+累计）。
            embed_event!(
                debug,
                target: "skeleton::cache::evict",
                kind = "ttl",
                key_hash = key_fingerprint(k),
                expired_total = self.expired
            );
        }
        due
    }
}
