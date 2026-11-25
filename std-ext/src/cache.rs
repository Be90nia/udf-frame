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
//! 常驻零依赖件：纯 std，无 feature 门。与 `memgov` 的分工：memgov=
//! 有界容器族（BTreeMap 序淘汰，`K: Ord`，首键退化语义）；本模块=精确 LRU+
//! TTL 缓存域（`K: Eq + Hash + Clone`）。memgov `Eviction` 头注「LRU/TTL→moka
//! 对表，不在此实现」的缺口由本模块兑现（moka 重依赖不采——零依赖纪律；
//! Guava Cache / moka 语义面对表见 cache 域性能锚（本地留存，未入库））。
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
//! clone 仅发生在 `insert` 写路径一次（moka 同约定）；**单线程面 get 热路径
//! 零 clone 零分配**（`map.get(&k)` 借用查询 + u32 链操作，读面返 `&V`）。
//! 共享面读面返 owned ⇒ 读路径有 `V::clone`，且该 clone 在 cache 锁内执行
//! （见下方「锁边界契约」）。
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
//! | 并发 | 单线程面 [`LruCache`]/[`TtlCache`]（`&mut self`）；共享面
//!   [`SyncLruCache`]/[`SyncTtlCache`]（内部 `Mutex` 短临界区 + 原子计数，
//!   poison fail-fast，无后台线程） | 契约收窄 + 并发批（issue#1-3）兑现 |
//! | 锁边界 | 共享面临界区内**仍跑用户代码**：键的 `Hash`/`Eq`（含借用键
//!   `Q::Hash`/`Q::Eq`）与读路径的 `V::clone` ⇒ 须**快、不 panic、不得重入
//!   同一个缓存**；#71 只把**退休值析构**移出锁、#80 只把**插入路径的
//!   `K::clone`** 移出锁 ⇒ 二者**不**使上述两类变成无锁 | 显式限制文档化
//!   （issue#82/#83；`HashMap<K,_>` 作权威索引 + 单 mutex 的固有属性，
//!   真锁外化属架构重设计） |
//!
//! # 锁边界契约（共享面 = 用户代码**仍在锁内**）
//!
//! 共享面用**一把 `Mutex` 串行化全部状态变更** ⇒ 临界区内执行的**用户代码**
//! 由两类 trait（键/值）与一个注入点（`Clock`）构成。**不是「所有用户代码都在
//! 锁外」**——#71 只把**退休值析构**移出锁，#80 只把**插入路径的 `K::clone`**
//! 移出锁，#101 只把**注入 `Clock` 的取样**移出锁；三者都不使 `Hash`/`Eq` 或
//! 读侧 `Clone` 变成无锁。逐条边界如下（issue#82/#83/#101）。
//!
//! ## 键 trait 在锁内（issue#82）
//!
//! `HashMap<K, u32>` 作权威索引 ⇒ 键哈希/比较在锁内发生。调用点清单：
//!
//! - **查表**：`idx_of` → `HashMap::get` —— 覆盖 `get`/`peek`/`contains`/
//!   `remove`/TTL 过期检查/插入存在性检查；
//! - **插入**：`LruCore::insert` → `map.get(&k)`（存在性查表）+ `map.insert`
//!   （建表项的哈希）；
//! - **删除**：`pop_lru`/`remove`/`retain` → `map.remove(&key)`；
//! - **扩容/rehash**：`HashMap` 增长时可能在锁内重新哈希既有键。
//!
//! ⇒ **键契约**：`K::Hash`、`K::Eq` 与借用键 `Q::Hash`/`Q::Eq`（`Borrow<Q>`
//! 路径，由调用方控制）**可能在内部 mutex 下执行** ⇒ 必须**快、不 panic、
//! 不得重入同一个缓存**（重入 = 非重入 mutex 自死锁；panic = 投毒锁，
//! 后续访问全部 fail-fast）。
//!
//! ## 读路径 `V::clone` 在锁内（issue#83）
//!
//! 读族返回 owned 值 ⇒ 克隆在锁内发生。四个调用点：
//!
//! - [`SyncLruCache::get`]：`g.get_mut(k).map(|v| v.clone())`；
//! - [`SyncLruCache::peek`]：`self.lock_core().peek(k).cloned()`；
//! - [`SyncTtlCache::get`]：过期检查 + LRU touch 之后 `e.value.clone()`；
//! - [`SyncTtlCache::peek`]：过期检查之后 `e.value.clone()`。
//!
//! ⇒ **值契约**：`V::clone` **在内部 mutex 下执行** ⇒ 必须**快、不 panic、
//! 不得重入同一个缓存**。
//!
//! ## 与 #71 / #80 的精确对照（两句并列，勿互相误读）
//!
//! - #71 移出锁的是**退休值的析构**（`K::drop`/`V::drop`）；
//! - 读侧 **`Clone` 仍在锁内**（本节）。
//!
//! #71 与 #80 **都没有**让 `Hash`/`Eq` 或读侧 `Clone` 变成无锁执行。
//!
//! ## 注入 `Clock` 在锁外（issue#101）
//!
//! [`SyncTtlCache`] 的全部 `Clock::now_instant` 取样都在 cache mutex **之外**——
//! 含满插清扫用的时间快照（第二阶段**取锁前**读取，锁内只消费该不可变值）。
//! ⇒ 自定义 `Clock` 可在 `now_instant` 内重入本缓存读面（`len`/`contains`/`get`）
//! 而不自锁、panic 也不投毒 cache。代价：取样与取锁之间刚过期的条目留到下次
//! 惰性访问/清扫（惰性 TTL 语义，不为更晚的纳秒快照把注入依赖带进锁内）。
//!
//! ⚠️ 与上面两条**分开记**：**`Clock` 在锁外**；键 `Hash`/`Eq` 与读路径
//! `V::clone` **仍在锁内**。
//!
//! ## 既有 API 属性（未被本契约改动）
//!
//! [`SyncLruCache::remove`]/[`SyncLruCache::pop_lru`] 与
//! [`SyncTtlCache::remove`]/[`SyncTtlCache::pop_lru`] 仍**移动出 owned `V`**，
//! **不需要 `V: Clone`**；只有返回 owned 值的读族 API（`get`/`peek`）要求
//! `V: Clone`。
//!
//! ## 单线程面区分
//!
//! [`LruCache`]/[`TtlCache`] **无共享 mutex** ⇒ 不存在上述锁内用户代码问题；
//! 但 `Hash`/`Eq` 仍在**调用方栈上同步执行**（调用方自担慢/重入/panic 后果，
//! 与锁无关）。
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

use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use skeleton::clock::Clock;

/// 键指纹（log-embed 事件字段）：`DefaultHasher::new` 定长种子=进程内确定，
/// 日志关联用（跨 Rust 版本不承诺稳定，仅身份对账非持久标识）。
#[cfg(feature = "log-embed")]
fn key_fingerprint<K: Hash + ?Sized>(k: &K) -> u64 {
    use std::hash::Hasher;
    let mut h = std::collections::hash_map::DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

/// 链尾哨兵：`u32::MAX` 不可能成为槽号（构造期 `cap <= u32::MAX - 1` 断言）。
const NONE: u32 = u32::MAX;

/// map→live 槽不变量（所有 mutator 共同维护：free 槽不入 map、live 槽键值恒存在）。
const INV_LIVE: &str = "cache 不变量：map 索引恒指向 live 槽（键值恒存在）";

/// 退休批（issue#71）：已从 cache 摘除、但**尚未析构**的用户键值所有权。
///
/// 共享面（[`SyncLruCache`]/[`SyncTtlCache`]）的临界区内就**本批退休面**而言只做
/// 簿记与所有权转移——本批在**解锁之后**才析构：用户 `K::drop`/`V::drop` 不再跑在
/// cache 锁内（重入析构不再自锁死；析构 panic 也不投毒 cache）。**本条只覆盖
/// 析构**：临界区内仍跑用户键 trait 与读侧 `V::clone`（见模块文档「锁边界契约」）。
///
/// 三件套各自至多一件（单次 mutator 的退休面）：
/// - `map_key`：map 侧键副本（`HashMap::remove_entry` 取回，未析构）；共享面
///   插入竞态落败时也含「备好但未被采用」的 map 侧键副本（issue#80）；
/// - `slot_key`：arena 侧槽键；覆盖路径也含「未被采用」的传入键副本（issue#80）；
/// - `value`：值（覆盖旧值 / 淘汰值 / 清扫回收值）。
struct Retired<K, V> {
    map_key: Option<K>,
    slot_key: Option<K>,
    value: Option<V>,
}

impl<K, V> Retired<K, V> {
    /// 空批（本次 mutator 无退休件）。
    const fn empty() -> Self {
        Self {
            map_key: None,
            slot_key: None,
            value: None,
        }
    }

    /// 退休件析构（共享面在**解锁之后**调用）：顺序固定 = map 键 → arena 键 → 值。
    ///
    /// 显式拆走三件套再析构（不靠字段自动 drop 的隐式顺序）：析构顺序写在
    /// 代码里，且「所有权已转移、析构时机由调用方掌控」是本类型的全部语义。
    fn retire(self) {
        let Self {
            map_key,
            slot_key,
            value,
        } = self;
        drop(map_key);
        drop(slot_key);
        drop(value);
    }
}

/// 共享面插入的**阶段 1** 结果（issue#80 两阶段插入）。
///
/// - `Replaced`：键已存在 ⇒ 临界区内已完成覆盖（值 + touch，**不 clone**）；
///   退休批交还调用方在解锁后析构。
/// - `Absent`：键不存在 ⇒ 调用方**出锁**备 map 侧键副本（锁外 `K::clone`），
///   再进阶段 2（重取锁 + 重查 = 线性化点）。
enum InsertPhase1<K, V> {
    Replaced(Retired<K, V>),
    Absent(K, V),
}

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
        // `cap` 是**淘汰上界**，不是预分配额度：`HashMap::with_capacity(cap)` +
        // `Vec::with_capacity(cap)` 会在构造瞬间吃掉 `cap × (Slot + 桶)` 字节，
        // 与实际条目数无关（`LruCache::new(65535)` 实测 7.63 MB 构造即驻留）。
        // 惰性增长：只在真正插入时按几何扩容，满载后仍收敛到同一上界。
        Self {
            map: HashMap::new(),
            slots: Vec::new(),
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

    /// 键→槽号（只读查找，不 touch、不分配；`K: Borrow<Q>` 让 `String` 键可用
    /// `&str` 直查，零分配）。
    fn idx_of<Q>(&self, k: &Q) -> Option<u32>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
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
    ///
    /// map 侧键副本用 `remove_entry` **取回**（不析构）——map 键与 arena 键是
    /// 两份所有权，map 那份进退休批（共享面解锁后才析构，issue#71）。
    fn pop_lru(&mut self) -> (Option<(K, V)>, Retired<K, V>) {
        let idx = self.tail;
        if idx == NONE {
            return (None, Retired::empty());
        }
        self.unlink(idx);
        let (key, value) = {
            let slot = &mut self.slots[idx as usize];
            (
                slot.key.take().expect(INV_LIVE),
                slot.value.take().expect(INV_LIVE),
            )
        };
        let map_key = self.map.remove_entry::<K>(&key).map(|(mk, _)| mk);
        self.free.push(idx);
        (
            Some((key, value)),
            Retired {
                map_key,
                slot_key: None,
                value: None,
            },
        )
    }

    /// 逐 LRU 尾并**全额退休**（arena 键 + 值 + map 键副本全进退休批，不交还
    /// 调用方）。
    ///
    /// 容量淘汰面用本方法：被逐者的两份键所有权与值全部延后到解锁后析构
    /// （issue#71——用户析构不在临界区内跑）。
    fn evict_tail(&mut self) -> Retired<K, V> {
        let (pair, mut retired) = self.pop_lru();
        if let Some((slot_key, value)) = pair {
            retired.slot_key = Some(slot_key);
            retired.value = Some(value);
        }
        retired
    }

    /// 存在即覆盖（更新值 + touch；len 不变、不触发淘汰）；**不存在则原样退回**
    /// `(键, 值)` 交还调用方。
    ///
    /// 退回而非就地处理：共享面需在**出锁**状态下备 map 侧键副本（issue#80：
    /// 用户 `K::clone` 不得在 cache 锁内跑）。键与值在临界区内不得析构 ⇒
    /// 不存在时所有权原样退回（调用方仍握有它们）。
    fn overwrite_if_present(&mut self, k: K, v: V) -> Result<Retired<K, V>, (K, V)> {
        let idx = self.map.get(&k).copied();
        match idx {
            Some(idx) => {
                let old = self.slots[idx as usize].value.replace(v);
                self.move_to_front(idx);
                Ok(Retired {
                    map_key: None,
                    // 传入键未被采用（缓存里存的是旧键）⇒ 退休（解锁后析构）。
                    slot_key: Some(k),
                    value: old,
                })
            }
            None => Err((k, v)),
        }
    }

    /// 新键插入（map 侧键副本**已备好** ⇒ 锁内不调 `K::clone`）。
    ///
    /// 容量淘汰 → 取槽 → map 侧用备好的键副本 → 挂链。退休批（被逐键值）
    /// 交还调用方在解锁后析构（issue#71）。
    fn insert_new(&mut self, k: K, map_key: K, v: V) -> (bool, Retired<K, V>) {
        let mut evicted = false;
        let mut retired = Retired::empty();
        if self.map.len() >= self.cap {
            retired = self.evict_tail();
            evicted = true;
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
        self.map.insert(map_key, idx);
        let slot = &mut self.slots[idx as usize];
        slot.key = Some(k);
        slot.value = Some(v);
        self.push_front(idx);
        (evicted, retired)
    }

    /// 插入/覆盖（map 侧键副本**已备好** ⇒ **锁内永不调 `K::clone`**，issue#80）。
    ///
    /// 键已存在（竞态：另一线程在阶段 1 与阶段 2 之间插入了它）⇒ 走普通覆盖
    /// 路径；**未被采用**的传入键与备好的键副本都进退休批（解锁后析构，
    /// issue#71）。键不存在 ⇒ 新键插入（[`Self::insert_new`]）。
    fn insert_with_map_key(&mut self, k: K, map_key: K, v: V) -> (bool, Retired<K, V>) {
        match self.overwrite_if_present(k, v) {
            Ok(mut retired) => {
                // 备好的键副本未被采用（键已被他人插入）⇒ 一并退休（解锁后析构）。
                retired.map_key = Some(map_key);
                (false, retired)
            }
            Err((k, v)) => self.insert_new(k, map_key, v),
        }
    }

    /// 不 touch 读（只读借用面）。
    fn peek(&self, k: &K) -> Option<&V> {
        let idx = self.idx_of(k)?;
        self.slots[idx as usize].value.as_ref()
    }

    /// 不 touch 写值面（「更新值不刷新 recency」语义的唯一入口）。
    fn peek_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let idx = self.idx_of(k)?;
        self.slots[idx as usize].value.as_mut()
    }

    /// touch 读 + 可变面（get=touch 语义的本体）。
    fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let idx = self.idx_of(k)?;
        self.move_to_front(idx);
        self.slots[idx as usize].value.as_mut()
    }

    /// 插入/覆盖：已存在=更新值+touch（len 不变，不触发淘汰——BoundedRegistry
    /// 同款语义）；新键满=先逐 LRU 尾再插。返回 (是否容量淘汰, 退休批)。
    ///
    /// 退休批（覆盖旧值 / 淘汰键值）**不在本方法内析构**——共享面解锁后才析构
    /// （issue#71）。
    ///
    /// 覆盖路径**不调 `K::clone`**（现有可观察语义）；新键才 clone 一份 map 侧
    /// 键副本（单线程面无共享锁 ⇒ clone 不在临界区内；共享面走
    /// [`Self::insert_with_map_key`]——锁内永不 clone，issue#80）。
    fn insert(&mut self, k: K, v: V) -> (bool, Retired<K, V>) {
        match self.overwrite_if_present(k, v) {
            Ok(retired) => (false, retired),
            Err((k, v)) => {
                let map_key = k.clone();
                self.insert_new(k, map_key, v)
            }
        }
    }

    /// 摘除（返回值；不判过期不 touch——物理面）。
    ///
    /// 返回 (值, 退休批)：键的两份所有权（arena 键 + map 键副本）进退休批，
    /// 值交还调用方。
    fn remove<Q>(&mut self, k: &Q) -> (Option<V>, Retired<K, V>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(idx) = self.idx_of(k) else {
            return (None, Retired::empty());
        };
        self.unlink(idx);
        let (key, value) = {
            let slot = &mut self.slots[idx as usize];
            (
                slot.key.take().expect(INV_LIVE),
                slot.value.take().expect(INV_LIVE),
            )
        };
        let map_key = self.map.remove_entry::<K>(&key).map(|(mk, _)| mk);
        self.free.push(idx);
        (
            Some(value),
            Retired {
                map_key,
                slot_key: Some(key),
                value: None,
            },
        )
    }

    /// 条件清扫：`keep` 返回 false 的条目回收（arena 序遍历，确定序）。
    /// 返回 (回收数, 退休批)：被回收条目的键值所有权全部进退休批。
    fn retain(&mut self, mut keep: impl FnMut(&K, &mut V) -> bool) -> (usize, Vec<Retired<K, V>>) {
        let mut purged = 0;
        let mut retired = Vec::new();
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
                let (key, value) = {
                    let slot = &mut self.slots[idx];
                    (
                        slot.key.take().expect(INV_LIVE),
                        slot.value.take().expect(INV_LIVE),
                    )
                };
                let map_key = self.map.remove_entry::<K>(&key).map(|(mk, _)| mk);
                self.free.push(idx32);
                purged += 1;
                retired.push(Retired {
                    map_key,
                    slot_key: Some(key),
                    value: Some(value),
                });
            }
        }
        (purged, retired)
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
/// 构造签名类型强制「上限必填」（无默认无界构造，`memgov::BoundedRegistry`
/// 同款纪律）；满插入恒逐 LRU 尾（无 Reject 形——缓存的淘汰本来就是承诺）。
///
/// 并发面：单线程 `&mut self`，无内部锁；`&self` 共享面见 [`SyncLruCache`]。
///
/// 键 trait 边界（issue#82）：本面**无共享 mutex** ⇒ 不存在「用户代码在 cache
/// 锁内」问题；但 `K::Hash`/`K::Eq` 仍在**调用方栈上同步执行**（慢/panic/重入
/// 的后果由调用方自担，与锁无关）。读面返 `&V` ⇒ 无 `V::clone`（issue#83 的
/// 读侧限制只存在于共享面）。
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
        let (evicted, retired) = self.core.insert(k, v);
        retired.retire();
        if evicted {
            embed_event!(
                debug,
                target: "skeleton::cache::evict",
                kind = "capacity",
                order = "lru_tail",
                len_at_evict = self.core.len().saturating_sub(1) as u64
            );
        }
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
        let (value, retired) = self.core.remove(k);
        retired.retire();
        value
    }

    /// 逐出 LRU 尾（最旧）并返回。
    #[must_use]
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        let (pair, retired) = self.core.pop_lru();
        retired.retire();
        pair
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
/// 并发面：单线程 `&mut self`；`&self` 共享面见 [`SyncTtlCache`]。
///
/// 键 trait 边界（issue#82）：本面**无共享 mutex** ⇒ 不存在「用户代码在 cache
/// 锁内」问题；但 `K::Hash`/`K::Eq` 仍在**调用方栈上同步执行**（慢/panic/重入
/// 的后果由调用方自担，与锁无关）。读面返 `&V` ⇒ 无 `V::clone`（issue#83 的
/// 读侧限制只存在于共享面）。
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
            let (purged, purge_retired) = self.core.retain(|_, e| e.expires > now);
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
                let retired = self.core.evict_tail();
                embed_event!(
                    debug,
                    target: "skeleton::cache::evict",
                    kind = "capacity",
                    key_hash = retired.slot_key.as_ref().map(|k| key_fingerprint(k)),
                    order = "lru_tail",
                    len_at_evict = self.core.len() as u64
                );
                retired.retire();
            }
            drop(purge_retired);
        }
        let (_, retired) = self.core.insert(k, TtlEntry { expires, value: v });
        retired.retire();
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
        let (value, retired) = self.core.remove(k);
        retired.retire();
        value.map(|e| e.value)
    }

    /// 逐出 LRU 尾（最旧；**物理面**：不判过期——过期与否不影响逐出序）。
    #[must_use]
    pub fn pop_lru(&mut self) -> Option<(K, V)> {
        let (pair, retired) = self.core.pop_lru();
        retired.retire();
        pair.map(|(k, e)| (k, e.value))
    }

    /// 显式清扫全部过期条目（arena 序确定遍历；单次 now 快照判定）。
    /// 返回回收数，计入 [`Self::expired`]。
    pub fn purge_expired(&mut self) -> usize {
        let now = self.clock.now_instant();
        let (n, retired) = self.core.retain(|_, e| e.expires > now);
        drop(retired);
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
            let (value, mut retired) = self.core.remove(k);
            // 值（含用户 V）一并进退休批 ⇒ 析构时机归调用方（issue#71）。
            retired.value = value;
            retired.retire();
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

/// 并发 LRU 缓存（[`LruCache`] 的 `&self` 共享面）：内部 `Mutex<LruCore>` 短
/// 临界区 + 单一原子计数出处，语义与单线程版逐字一致，仅两处并发面必然差异：
///
/// - **取值返 owned**（`V: Clone`）：`&V` 引用不可跨越锁边界——守卫外泄等于把
///   全局锁交到调用方手里（非重入锁自锁死 + 临界区失控），moka 同款约定；
///   同理 `iter`/`peek_mut` 引用面不设（诊断面走 [`Debug`]）。**该 `V::clone`
///   在锁内执行**（见下节「锁内用户代码契约」，issue#83）；
/// - **析构在锁外**（issue#71）：就**退休面**而言临界区内只做簿记与所有权转移
///   ——被覆盖/被淘汰/被摘除的键值所有权在**解锁之后**才析构（用户 `K::drop`/
///   `V::drop` 可能很慢、可能重入、可能 panic：跑在锁内 = 重入析构自锁死 +
///   析构 panic 投毒 cache）。**本条只覆盖析构**——锁内仍跑用户键 trait 与读侧
///   `V::clone`（见下节「锁内用户代码契约」）；
/// - **poison = fail-fast panic 带因**（拍板）：**临界区内的内部簿记** panic
///   （不变量破坏）后 LRU 链可能已破，`into_inner` 静默续跑 = 破链读写放大，禁
///   （与 logx 的 `into_inner` 先例区分——cache 不变量耦合度高）；用户析构 panic
///   发生在解锁之后 ⇒ **不投毒**（用户代码的锅不牵连 cache 不变量）。
///
/// # 锁内用户代码契约（issue#82/#83）
///
/// 上述「析构在锁外」「`K::clone` 在锁外」**只覆盖那两条边界**——共享面仍有
/// 用户代码跑在内部 mutex 下：
///
/// - **键 trait 在锁内**（issue#82）：`HashMap<K, u32>` 作权威索引 ⇒
///   `K::Hash`、`K::Eq` 与借用键 `Q::Hash`/`Q::Eq`（`Borrow<Q>` 路径）**可能
///   在内部 mutex 下执行**（查表 `idx_of`→`HashMap::get`、插入 `map.get`/
///   `map.insert`、删除 `map.remove`、`HashMap` 扩容/rehash）⇒ 键实现必须
///   **快、不 panic、不得重入同一个缓存**（重入 = 非重入 mutex 自死锁；panic
///   = 投毒锁 ⇒ 后续访问全部 fail-fast）。
/// - **读路径 `V::clone` 在锁内**（issue#83）：[`Self::get`]（`g.get_mut(k)
///   .map(|v| v.clone())`）与 [`Self::peek`]（`self.lock_core().peek(k)
///   .cloned()`）返 owned 值 ⇒ `V::clone` **在内部 mutex 下执行** ⇒ 值实现
///   必须**快、不 panic、不得重入同一个缓存**。
/// - **与 #71 的精确对照**（两句并列，勿互相误读）：#71 移出锁的是**退休值的
///   析构**；读侧 **`Clone` 仍在锁内**。#71/#80 **都没有**让 `Hash`/`Eq` 或
///   读侧 `Clone` 变成无锁执行。
/// - **既有 API 属性未被改动**：[`Self::remove`]/[`Self::pop_lru`] 仍**移动出
///   owned `V`**、**不需要 `V: Clone`**；只有返回 owned 值的 `get`/`peek`
///   要求 `V: Clone`。
///
/// 计数：命中（get 触碰命中）/淘汰（满插逐尾）各一个 `AtomicU64` 单点，锁内
/// 无第二套计数；`Relaxed` 序（诊断面，等价性断言由线程 join 同步保证，见
/// c4v 测试）。淘汰口径：容量满插自动逐出才计；`remove`/`pop_lru` 是显式
/// 物理面，不计。
///
/// 无后台维护线程（moka v0.12 头号变更即删后台线程——不可控调度面与确定性
/// 测试冲突）；TTL 惰性过期语义见 [`SyncTtlCache`]。
///
/// ponytail: 全局单 Mutex 起步（读共享写短临界区）；命中率证据出现再分片
///（dashmap/moka 分面路线），不预造。
#[derive(Debug)]
pub struct SyncLruCache<K, V> {
    core: Mutex<LruCore<K, V>>,
    hits: AtomicU64,
    evictions: AtomicU64,
}

impl<K: Eq + Hash + Clone, V> SyncLruCache<K, V> {
    /// 构造。`cap == 0` 构造期 panic（同 [`LruCache::new`]）。
    #[must_use]
    pub fn new(cap: usize) -> Self {
        Self {
            core: Mutex::new(LruCore::new(cap)),
            hits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// 锁入口：poison 即 panic 带因（fail-fast，见类型文档）。
    fn lock_core(&self) -> MutexGuard<'_, LruCore<K, V>> {
        self.core.lock().unwrap_or_else(|e| {
            panic!(
                "SyncLruCache: 内部锁已 poison（持锁 panic 后 LRU 链不变量可能已破，禁静默续跑）: {e}"
            )
        })
    }

    /// 插入/覆盖。已存在键=更新值+touch（不触发淘汰）；新键满=先逐 LRU 尾
    /// 再插（计入 [`Self::evictions`]）。
    ///
    /// 临界区内只做簿记与所有权转移：被覆盖/被淘汰的键值所有权在**解锁之后**
    /// 才析构（用户 `K::drop`/`V::drop` 不在 cache 锁内跑，issue#71）。
    ///
    /// **用户 `K::clone` 在锁外**（issue#80）：两阶段插入——① 持锁查存在，存在
    /// 即更新值 + touch（**不 clone**，覆盖语义不变）；② 不存在 ⇒ **放锁** →
    /// clone map 侧键副本（此刻无锁、无任何变更 ⇒ clone panic 不投毒 cache）→
    /// **重取锁再查**（第二次锁定检查 = 本操作的线性化点）：仍不存在则用备好的
    /// 键副本插入（锁内不调 `K::clone`）；已存在（竞态）则走普通覆盖路径，
    /// **未被采用**的候选键在**解锁之后**才析构。
    ///
    /// ⚠️ 本条**只覆盖 `K::clone`**：锁内仍会执行键的 `Hash`/`Eq`（查表/建表
    /// 项/删除/rehash）与读路径的 `V::clone` ⇒ 见类型文档「锁内用户代码契约」
    /// （issue#82/#83）。
    pub fn insert(&self, k: K, v: V) {
        // 阶段 1：持锁查存在 —— 存在即更新值 + touch（**不 clone**）。
        let phase1 = {
            let mut g = self.lock_core();
            match g.overwrite_if_present(k, v) {
                Ok(retired) => InsertPhase1::Replaced(retired),
                Err((k, v)) => InsertPhase1::Absent(k, v),
            }
        };
        let (k, v) = match phase1 {
            InsertPhase1::Replaced(retired) => {
                retired.retire();
                return;
            }
            InsertPhase1::Absent(k, v) => (k, v),
        };
        // 锁外 clone map 侧键副本：此刻**无 cache 锁持有、未发生任何变更**
        // ⇒ clone panic 在此展开（不投毒 cache，issue#80）。
        let map_key = k.clone();
        // 阶段 2：重取锁 + 重查（**第二次锁定检查 = 线性化点**）⇒ 锁内不调
        // `K::clone`；竞态落败（键已被他人插入）⇒ 走普通覆盖路径，未被采用的
        // 候选键**解锁后**才析构（issue#71/#80）。
        let (evicted, retired) = {
            let mut g = self.lock_core();
            g.insert_with_map_key(k, map_key, v)
        };
        retired.retire();
        if evicted {
            self.evictions.fetch_add(1, Ordering::Relaxed);
            // No key fingerprint here: invoking user Hash for diagnostics would
            // add another arbitrary callback after the state transition.
            embed_event!(
                debug,
                target: "skeleton::cache::evict",
                kind = "capacity",
                order = "lru_tail",
                len_at_evict = self.capacity().saturating_sub(1) as u64
            );
        }
    }

    /// 读（**touch**：命中刷新 recency 并计入 [`Self::hits`]）。返 owned
    ///（引用不可跨锁，moka 同款）；不扰动淘汰序的读面用 [`Self::peek`]。
    ///
    /// ⚠️ 锁内用户代码（issue#83）：`V::clone`（`g.get_mut(k).map(|v| v.clone())`）
    /// 与键的 `Hash`/`Eq`（查表）**都在内部 mutex 下执行** ⇒ `V` 与 `K` 的
    /// 实现必须快、不 panic、不得重入同一个缓存。
    pub fn get(&self, k: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut g = self.lock_core();
        let hit = g.get_mut(k).map(|v| v.clone());
        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// 读（不 touch、不计数，纯读面）。
    ///
    /// ⚠️ 锁内用户代码（issue#83）：`V::clone`（`self.lock_core().peek(k)
    /// .cloned()`）与键的 `Hash`/`Eq`（查表）**都在内部 mutex 下执行** ⇒ `V`
    /// 与 `K` 的实现必须快、不 panic、不得重入同一个缓存。
    #[must_use]
    pub fn peek(&self, k: &K) -> Option<V>
    where
        V: Clone,
    {
        self.lock_core().peek(k).cloned()
    }

    /// 物理存在判定（不 touch）。
    #[must_use]
    pub fn contains(&self, k: &K) -> bool {
        self.lock_core().idx_of(k).is_some()
    }

    /// 摘除并返回值。
    ///
    /// 摘除的两份键所有权在**解锁之后**才析构（issue#71）；值交还调用方。
    #[must_use]
    pub fn remove(&self, k: &K) -> Option<V> {
        let (value, retired) = {
            let mut g = self.lock_core();
            g.remove(k)
        };
        retired.retire();
        value
    }

    /// 逐出 LRU 尾（最旧）并返回。
    ///
    /// map 侧键副本在**解锁之后**才析构（issue#71）；键值对交还调用方。
    #[must_use]
    pub fn pop_lru(&self) -> Option<(K, V)> {
        let (pair, retired) = {
            let mut g = self.lock_core();
            g.pop_lru()
        };
        retired.retire();
        pair
    }

    /// 当前条目数。
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock_core().len()
    }

    /// 空判定。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock_core().len() == 0
    }

    /// 容量上界（构造期定，不变）。
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.lock_core().capacity()
    }

    /// 累计命中数（get 触碰命中；peek/contains 不计）。
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// 累计容量淘汰数（满插逐尾才计；显式 remove/pop_lru 不计）。
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}

/// 并发 TTL 缓存（[`TtlCache`] 的 `&self` 共享面）：内部 `Mutex<LruCore>` 短
/// 临界区 + 单一原子计数，语义与单线程版逐字一致（get=touch、peek 不 touch、
/// 惰性过期 fail-visible、满插先清过期再逐 LRU 尾、无后台维护线程）。差异面
/// 同 [`SyncLruCache`]：取值返 owned（`V: Clone`）、引用迭代面不设、poison
/// fail-fast panic 带因；[`Self::expired`] 惰性/清扫计数、[`Self::evictions`]
/// 满插逐尾计数均为原子单点（锁内无第二套）。
///
/// 调用期校验（`ttl == 0`、`now + ttl` 溢出）在**取锁前**完成：调用方 bug 的
/// fail-closed 不触碰内部不变量，panic 后缓存照常可用（与临界区内内部簿记
/// panic 的 poison 面相区分）。
///
/// 析构在锁外（issue#71，同 [`SyncLruCache`]）：就**退休面**而言临界区内只做簿记
/// 与所有权转移，被清扫/被淘汰/被摘除的键值所有权在解锁之后才析构（用户析构
/// panic 不投毒 cache）。**本条只覆盖析构**——锁内仍跑用户键 trait 与读侧
/// `V::clone`（见下节「锁内用户代码契约」）。
///
/// # 注入 Clock 在锁外（issue#101）
///
/// 全部 `Clock::now_instant` 调用都在 cache mutex **之外**取样——含满插清扫
/// 所用的时间快照（第二阶段取锁**前**读取，锁内只消费该不可变值）。用户
/// `Clock` 实现因此不会在这把锁下执行 ⇒ 其重入本缓存的读面（`len`/
/// `contains`/`get`）不自锁、panic 不投毒。三类边界**分开记**，勿互相误读：
///
/// - **`Clock` 在锁外**（本条）：注入时钟实现无需了解本类型的私有锁序；
/// - **键 `Hash`/`Eq` 仍在锁内**（issue#82）；
/// - **读路径 `V::clone` 仍在锁内**（issue#83）。
///
/// # 锁内用户代码契约（issue#82/#83）
///
/// 「析构在锁外」「`K::clone` 在锁外」**只覆盖那两条边界**——共享面仍有用户
/// 代码跑在内部 mutex 下：
///
/// - **键 trait 在锁内**（issue#82）：`HashMap<K, u32>` 作权威索引 ⇒
///   `K::Hash`、`K::Eq` 与借用键 `Q::Hash`/`Q::Eq`（`Borrow<Q>` 路径，由调用方
///   控制）**可能在内部 mutex 下执行**（查表 `idx_of`→`HashMap::get`、插入
///   `map.get`/`map.insert`、删除 `map.remove`、`HashMap` 扩容/rehash）⇒ 键实现
///   必须**快、不 panic、不得重入同一个缓存**（重入 = 非重入 mutex 自死锁；
///   panic = 投毒锁 ⇒ 后续访问全部 fail-fast）。
/// - **读路径 `V::clone` 在锁内**（issue#83）：[`Self::get`]（过期检查 + LRU
///   touch 之后 `e.value.clone()`）与 [`Self::peek`]（过期检查之后
///   `e.value.clone()`）返 owned 值 ⇒ `V::clone` **在内部 mutex 下执行** ⇒ 值
///   实现必须**快、不 panic、不得重入同一个缓存**。
/// - **与 #71 的精确对照**（两句并列，勿互相误读）：#71 移出锁的是**退休值的
///   析构**；读侧 **`Clone` 仍在锁内**。#71/#80 **都没有**让 `Hash`/`Eq` 或
///   读侧 `Clone` 变成无锁执行。
/// - **既有 API 属性未被改动**：[`Self::remove`]/[`Self::pop_lru`] 仍**移动出
///   owned `V`**、**不需要 `V: Clone`**；只有返回 owned 值的 `get`/`peek`
///   要求 `V: Clone`。
pub struct SyncTtlCache<K, V> {
    clock: Arc<dyn Clock>,
    core: Mutex<LruCore<K, TtlEntry<V>>>,
    expired: AtomicU64,
    hits: AtomicU64,
    evictions: AtomicU64,
}

/// 手写 Debug：跳过时钟字段（`dyn Clock` 不可 Debug——注入件非状态），内容面
/// 按逻辑条目（recency 序，含僵尸）呈现，形态对齐单线程版 [`TtlCache`]。
impl<K: Eq + Hash + Clone + std::fmt::Debug, V: std::fmt::Debug> std::fmt::Debug
    for SyncTtlCache<K, V>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let g = self.lock_core();
        f.debug_struct("SyncTtlCache")
            .field("len", &g.len())
            .field("capacity", &g.capacity())
            .field("expired", &self.expired.load(Ordering::Relaxed))
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("evictions", &self.evictions.load(Ordering::Relaxed))
            .field(
                "entries",
                &g.iter().map(|(k, e)| (k, &e.value)).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl<K: Eq + Hash + Clone, V> SyncTtlCache<K, V> {
    /// 构造。时钟注入同 [`TtlCache::new`]（生产 [`skeleton::clock::SystemClock`]，
    /// 测试 [`skeleton::clock::MockClock`]）。`cap == 0` panic。
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, cap: usize) -> Self {
        Self {
            clock,
            core: Mutex::new(LruCore::new(cap)),
            expired: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// 锁入口：poison 即 panic 带因（fail-fast，见类型文档）。
    fn lock_core(&self) -> MutexGuard<'_, LruCore<K, TtlEntry<V>>> {
        self.core.lock().unwrap_or_else(|e| {
            panic!(
                "SyncTtlCache: 内部锁已 poison（持锁 panic 后 LRU 链不变量可能已破，禁静默续跑）: {e}"
            )
        })
    }

    /// 插入（逐条 TTL，expire-after-write）。覆盖已存在键=更新值+重置计时+
    /// touch，不触发淘汰。`ttl == 0` panic、`now + ttl` 溢出 panic（fail-closed，
    /// 取锁前校验——不投毒）。新键满插：先清过期（计入 [`Self::expired`]），
    /// 仍满逐 LRU 尾（计入 [`Self::evictions`]）。
    ///
    /// 临界区内只做簿记与所有权转移：被清扫/被淘汰/被覆盖的键值所有权在
    /// **解锁之后**才析构（issue#71）。
    ///
    /// **用户 `K::clone` 在锁外**（issue#80）：与 [`SyncLruCache::insert`] 同款
    /// 两阶段插入——① 持锁查存在，存在即更新值 + 重置过期 + touch（**不
    /// clone**）；② 不存在 ⇒ **放锁** → clone map 侧键副本（此刻无锁、无任何
    /// 变更 ⇒ clone panic 不投毒 cache）→ **重取锁再查**（第二次锁定检查 =
    /// 线性化点，且**早于过期清扫/淘汰**：panic 的 clone 不得造成部分清扫/变更）
    /// → 仍不存在才走「先过期先淘汰」政策并用备好的键副本插入（锁内不调
    /// `K::clone`）；已存在（竞态）则走普通覆盖路径，未被采用的候选键**解锁
    /// 之后**才析构。
    ///
    /// ⚠️ 本条**只覆盖 `K::clone`**：锁内仍会执行键的 `Hash`/`Eq`（查表/建表
    /// 项/删除/rehash）与读路径的 `V::clone` ⇒ 见类型文档「锁内用户代码契约」
    /// （issue#82/#83）。
    ///
    /// **注入 `Clock` 在锁外**（issue#101）：两次取样都在 cache mutex 之外——
    /// 第一次算 `expires`（阶段 1 取锁前），第二次是满插清扫用的不可变时间
    /// 快照（阶段 2 取锁**前**读取，锁内不再调 `Clock`）。自定义 `Clock` 因此可
    /// 在 `now_instant` 内重入本缓存而不自锁、panic 也不投毒；快照 = 阶段 2
    /// 尝试开始的时刻，取样与拿锁之间刚过期的条目留到下次惰性访问/清扫。
    pub fn insert(&self, k: K, v: V, ttl: Duration) {
        assert!(
            !ttl.is_zero(),
            "TtlCache::insert: ttl == 0 是恒过期条目（调用方 bug，fail-fast）"
        );
        let now = self.clock.now_instant();
        let expires = now.checked_add(ttl).expect(
            "TtlCache::insert: now + ttl 溢出 Instant 表示范围（fail-closed，clock armor 同款）",
        );
        // 阶段 1：持锁查存在 —— 存在即更新值 + 重置过期 + touch（**不 clone**）。
        let phase1 = {
            let mut g = self.lock_core();
            match g.overwrite_if_present(k, TtlEntry { expires, value: v }) {
                Ok(retired) => InsertPhase1::Replaced(retired),
                Err((k, entry)) => InsertPhase1::Absent(k, entry),
            }
        };
        let (k, entry) = match phase1 {
            InsertPhase1::Replaced(retired) => {
                retired.retire();
                return;
            }
            InsertPhase1::Absent(k, entry) => (k, entry),
        };
        // 锁外 clone map 侧键副本：此刻**无 cache 锁持有、未发生任何变更**
        // ⇒ clone panic 在此展开（不投毒 cache，issue#80）。
        let map_key = k.clone();
        // 阶段 2 的时间快照：注入 Clock 在**取锁前**取样（issue#101）——临界区
        // 内只消费这个不可变值，用户 Clock 实现不得在 cache mutex 下执行（锁内
        // 重入本缓存会自锁，panic 会投毒）。快照 = 第二阶段尝试开始的时刻：取样
        // 与拿锁之间刚过期的条目留到下次惰性访问/清扫（惰性 TTL 语义，不为更晚
        // 的纳秒快照把调用方注入依赖带进锁内）。
        let purge_now = self.clock.now_instant();
        // 阶段 2：重取锁 + 重查（**第二次锁定检查 = 线性化点**；重查**早于**
        // 过期清扫/淘汰 ⇒ panic 的 clone 不会造成部分清扫/变更）。
        let (purge_retired, evict_retired, insert_retired) = {
            let mut g = self.lock_core();
            let mut purge_retired = Vec::new();
            let mut evict_retired = Retired::empty();
            if g.idx_of(&k).is_none() && g.len() >= g.capacity() {
                // 双界拍板：先过期先淘汰（单次锁外 now 快照判定，确定性），仍满再逐 LRU 尾。
                let (purged, retired) = g.retain(|_, e| e.expires > purge_now);
                purge_retired = retired;
                self.expired.fetch_add(purged as u64, Ordering::Relaxed);
                // 埋点「TTL 过期驱逐（满插清扫）」：真回收才发（僵尸数+触发键指纹）。
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
                if g.len() >= g.capacity() {
                    // 埋点「容量淘汰」（TtlCache 站点）：被逐者恒 LRU 尾。
                    let retired = g.evict_tail();
                    evict_retired = retired;
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                    embed_event!(
                        debug,
                        target: "skeleton::cache::evict",
                        kind = "capacity",
                        key_hash = evict_retired.slot_key.as_ref().map(|k| key_fingerprint(k)),
                        order = "lru_tail",
                        len_at_evict = g.len() as u64
                    );
                }
            }
            // 锁内不调 `K::clone`（备好的键副本直接用）；竞态落败（键已被他人
            // 插入）⇒ 走普通覆盖路径，未被采用的候选键**解锁后**才析构。
            let (_, retired) = g.insert_with_map_key(k, map_key, entry);
            (purge_retired, evict_retired, retired)
        };
        drop(purge_retired);
        evict_retired.retire();
        insert_retired.retire();
    }

    /// 读（touch + 惰性过期 + 命中计数）。过期条目按 miss（不计命中）。
    ///
    /// 借用键面（`K: Borrow<Q>`）：`K = String` 时可直接用 `&str` 查，热路径
    /// 零分配（`HashMap::get` 的借用查询不透支键类型）。
    ///
    /// 过期回收的键值所有权在**解锁之后**才析构（issue#71）。
    ///
    /// ⚠️ 锁内用户代码（issue#83）：过期检查 + LRU touch 之后的 `V::clone`
    /// （`e.value.clone()`）与键的 `Hash`/`Eq`（查表；借用键面由调用方控制
    /// `Q::Hash`/`Q::Eq`）**都在内部 mutex 下执行** ⇒ `V` 与 `K`/`Q` 的实现
    /// 必须快、不 panic、不得重入同一个缓存。
    pub fn get<Q>(&self, k: &Q) -> Option<V>
    where
        V: Clone,
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = self.clock.now_instant();
        let (hit, retired) = {
            let mut g = self.lock_core();
            let (due, retired) = Self::expire_if_due(&mut g, &self.expired, k, now);
            if due {
                (None, retired)
            } else {
                let hit = g.get_mut(k).map(|e| e.value.clone());
                if hit.is_some() {
                    self.hits.fetch_add(1, Ordering::Relaxed);
                }
                (hit, retired)
            }
        };
        retired.retire();
        hit
    }

    /// 读（不 touch + 惰性过期；无命中计数——纯读面）。
    ///
    /// 过期回收的键值所有权在**解锁之后**才析构（issue#71）。
    ///
    /// ⚠️ 锁内用户代码（issue#83）：过期检查之后的 `V::clone`（`e.value.clone()`）
    /// 与键的 `Hash`/`Eq`（查表）**都在内部 mutex 下执行** ⇒ `V` 与 `K` 的实现
    /// 必须快、不 panic、不得重入同一个缓存。
    #[must_use]
    pub fn peek(&self, k: &K) -> Option<V>
    where
        V: Clone,
    {
        let now = self.clock.now_instant();
        let (hit, retired) = {
            let mut g = self.lock_core();
            let (due, retired) = Self::expire_if_due(&mut g, &self.expired, k, now);
            if due {
                (None, retired)
            } else {
                (g.peek(k).map(|e| e.value.clone()), retired)
            }
        };
        retired.retire();
        hit
    }

    /// 物理存在判定（**含僵尸**：过期未回收条目也算在——不 touch 不判定过期）。
    /// 活性判定走 [`Self::peek`]。
    #[must_use]
    pub fn contains(&self, k: &K) -> bool {
        self.lock_core().idx_of(k).is_some()
    }

    /// 摘除并返回值（物理面：不判过期）。
    ///
    /// 摘除的两份键所有权在**解锁之后**才析构（issue#71）。
    #[must_use]
    pub fn remove(&self, k: &K) -> Option<V> {
        let (value, retired) = {
            let mut g = self.lock_core();
            g.remove(k)
        };
        retired.retire();
        value.map(|e| e.value)
    }

    /// 逐出 LRU 尾（最旧；**物理面**：不判过期）。
    ///
    /// map 侧键副本在**解锁之后**才析构（issue#71）。
    #[must_use]
    pub fn pop_lru(&self) -> Option<(K, V)> {
        let (pair, retired) = {
            let mut g = self.lock_core();
            g.pop_lru()
        };
        retired.retire();
        pair.map(|(k, e)| (k, e.value))
    }

    /// 显式清扫全部过期条目（arena 序确定遍历；单次 now 快照判定）。
    /// 返回回收数，计入 [`Self::expired`]。
    ///
    /// 被回收条目的键值所有权在**解锁之后**才析构（issue#71）。
    pub fn purge_expired(&self) -> usize {
        let now = self.clock.now_instant();
        let (n, retired) = {
            let mut g = self.lock_core();
            let (n, retired) = g.retain(|_, e| e.expires > now);
            self.expired.fetch_add(n as u64, Ordering::Relaxed);
            // 埋点「purge」：显式清扫动作 info（回收数/残留）。
            embed_event!(
                info,
                target: "skeleton::cache::purge",
                purged = n as u64,
                len_after = g.len() as u64
            );
            (n, retired)
        };
        drop(retired);
        n
    }

    /// 累计惰性/清扫过期回收数（fail-visible：过期不是静默 None）。
    #[must_use]
    pub fn expired(&self) -> u64 {
        self.expired.load(Ordering::Relaxed)
    }

    /// 物理条数（**含僵尸**——惰性过期语义下如实声明；活性面用读族 API）。
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock_core().len()
    }

    /// 空判定（物理面）。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock_core().len() == 0
    }

    /// 容量上界（构造期定，不变）。
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.lock_core().capacity()
    }

    /// 累计命中数（get 触碰命中；过期触碰按 miss 不计；peek/contains 不计）。
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// 累计容量淘汰数（满插逐尾才计；显式 remove/pop_lru 不计）。
    #[must_use]
    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }

    /// 惰性过期判定（语义同单线程版 [`TtlCache`]）：到期则回收+计数，返回
    /// 是否回收。计数走 [`AtomicU64`] 单点（free fn 形：guard 与计数分属
    /// 结构体字段，借参避免跨字段借用冲突）。
    ///
    /// 返回 (是否回收, 退休批)：被回收条目的键值所有权交还调用方，由共享面
    /// 在**解锁之后**才析构（issue#71）。
    fn expire_if_due<Q>(
        g: &mut LruCore<K, TtlEntry<V>>,
        expired: &AtomicU64,
        k: &Q,
        now: Instant,
    ) -> (bool, Retired<K, TtlEntry<V>>)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let due = matches!(g.peek_mut(k), Some(e) if e.expires <= now);
        if due {
            let (value, mut retired) = g.remove(k);
            // 值（含用户 V）一并进退休批 ⇒ 解锁后才析构（issue#71）。
            retired.value = value;
            expired.fetch_add(1, Ordering::Relaxed);
            // 埋点「TTL 过期驱逐（惰性单条）」：触碰即回收留痕（键指纹+累计）。
            #[cfg(feature = "log-embed")]
            embed_event!(
                debug,
                target: "skeleton::cache::evict",
                kind = "ttl",
                key_hash = key_fingerprint(k),
                expired_total = expired.load(Ordering::Relaxed)
            );
            return (true, retired);
        }
        (false, Retired::empty())
    }
}

/// 锁边界探针（issue#82/#83）：证明「用户 `Hash`/`Eq`/`V::clone` **确实在共享
/// cache 的内部 mutex 下执行**」——这是**实现边界的回归测试**，不是「mutex 必须
/// 永远是实现方式」的公共承诺：若将来索引协议改为锁外执行 Hash/Eq/Clone，本组
/// 测试会失败（届时应同步复核契约与文档）。
///
/// 手法（维护者拍板形态，确定性、无 sleep、无子进程）：探针态用
/// `Weak<SyncLruCache<..>>`/`Weak<SyncTtlCache<..>>` 持目标 cache，用户 trait
/// （`Hash`/`Eq`/`Clone`）内升级 Weak 并对**同一个内部 mutex** 调 `try_lock` ⇒
/// 期望 `Err(TryLockError::WouldBlock)`（同线程重入，非重入锁立即失败，不阻塞
/// ⇒ 不会挂死测试进程）。`Poisoned`/`Ok` 都不算「锁被持有」的证据。
#[cfg(test)]
mod lock_boundary_probe {
    use super::{Hash, LruCache, SyncLruCache, SyncTtlCache, TtlCache};
    use std::cell::RefCell;
    use std::hash::Hasher;
    use std::sync::{Arc, TryLockError, Weak};

    /// 探针结论：`try_lock` 的三种结果里**只有 `WouldBlock` 是「临界区确被持有」
    /// 的证据**（`Acquired` = 用户代码在锁外，`Poisoned` = 锁已投毒、均非证据）。
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ProbeOutcome {
        Acquired,
        WouldBlock,
        Poisoned,
    }

    thread_local! {
        /// 共享探针态：装入的闭包对目标 cache 的内部 mutex 做 `try_lock`。
        static PROBE: RefCell<Option<Box<dyn Fn() -> ProbeOutcome>>> =
            const { RefCell::new(None) };
        /// 本次操作期间所有探针命中记录（顺序无关 ⇒ 严判：必须**全部**是
        /// `WouldBlock` 且**至少一次**）。
        static PROBE_HITS: RefCell<Vec<ProbeOutcome>> = const { RefCell::new(Vec::new()) };
    }

    /// 装入探针（操作前调用）；`None` = 探针关闭 ⇒ 用户 trait 不记录任何东西。
    fn install_probe(f: Box<dyn Fn() -> ProbeOutcome>) {
        PROBE.with(|p| *p.borrow_mut() = Some(f));
        PROBE_HITS.with(|h| h.borrow_mut().clear());
    }

    /// 卸下探针（操作后调用，防止后续调用污染本次结论）。
    fn uninstall_probe() {
        PROBE.with(|p| *p.borrow_mut() = None);
    }

    /// 用户 trait（`Hash`/`Eq`/`Clone`）内部调用：探针开启时记录 `try_lock` 结果。
    fn probe_hit() {
        let outcome = PROBE.with(|p| {
            let borrowed = p.borrow();
            borrowed.as_ref().map(|f| f())
        });
        if let Some(outcome) = outcome {
            PROBE_HITS.with(|h| h.borrow_mut().push(outcome));
        }
    }

    /// 断言：本次操作期间用户 trait **至少被调用一次**，且每次都观测到内部
    /// mutex 被持有（`try_lock` = `WouldBlock`）。
    fn assert_ran_under_core_mutex(what: &str) {
        let hits = PROBE_HITS.with(|h| h.borrow_mut().drain(..).collect::<Vec<_>>());
        assert!(
            !hits.is_empty(),
            "{what}: 探针未被触发（该用户 trait 未在本次操作中被调用）"
        );
        assert!(
            hits.iter().all(|h| *h == ProbeOutcome::WouldBlock),
            "{what}: 期望用户 trait 在内部 mutex 下执行（try_lock = WouldBlock），实际 = {hits:?}"
        );
    }

    /// `SyncLruCache` 探针：对内部 `core` mutex 调 `try_lock`。
    fn lru_probe<K: 'static, V: 'static>(
        cache: &Arc<SyncLruCache<K, V>>,
    ) -> Box<dyn Fn() -> ProbeOutcome> {
        let weak: Weak<SyncLruCache<K, V>> = Arc::downgrade(cache);
        Box::new(move || match weak.upgrade() {
            Some(c) => match c.core.try_lock() {
                Ok(_) => ProbeOutcome::Acquired,
                Err(TryLockError::WouldBlock) => ProbeOutcome::WouldBlock,
                Err(TryLockError::Poisoned(_)) => ProbeOutcome::Poisoned,
            },
            None => ProbeOutcome::Poisoned,
        })
    }

    /// `SyncTtlCache` 探针：对内部 `core` mutex 调 `try_lock`。
    fn ttl_probe<K: 'static, V: 'static>(
        cache: &Arc<SyncTtlCache<K, V>>,
    ) -> Box<dyn Fn() -> ProbeOutcome> {
        let weak: Weak<SyncTtlCache<K, V>> = Arc::downgrade(cache);
        Box::new(move || match weak.upgrade() {
            Some(c) => match c.core.try_lock() {
                Ok(_) => ProbeOutcome::Acquired,
                Err(TryLockError::WouldBlock) => ProbeOutcome::WouldBlock,
                Err(TryLockError::Poisoned(_)) => ProbeOutcome::Poisoned,
            },
            None => ProbeOutcome::Poisoned,
        })
    }

    /// 查表面探针键：`Hash` 记录探针命中（`Eq` 保持普通语义）。
    #[derive(Clone, Debug)]
    struct HashProbeKey(u32);

    impl Hash for HashProbeKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            probe_hit();
            self.0.hash(state);
        }
    }

    impl PartialEq for HashProbeKey {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0
        }
    }

    impl Eq for HashProbeKey {}

    /// 相等面探针键：`Eq` 记录探针命中（`Hash` 保持普通语义）。
    #[derive(Clone, Debug)]
    struct EqProbeKey(u32);

    impl Hash for EqProbeKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.0.hash(state);
        }
    }

    impl PartialEq for EqProbeKey {
        fn eq(&self, other: &Self) -> bool {
            probe_hit();
            self.0 == other.0
        }
    }

    impl Eq for EqProbeKey {}

    /// 借用查询面探针键（`Q` 侧）：`Hash`/`Eq` 均记录命中。
    #[derive(Clone, Debug)]
    struct BorrowProbeKey(u32);

    impl Hash for BorrowProbeKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            probe_hit();
            self.0.hash(state);
        }
    }

    impl PartialEq for BorrowProbeKey {
        fn eq(&self, other: &Self) -> bool {
            probe_hit();
            self.0 == other.0
        }
    }

    impl Eq for BorrowProbeKey {}

    /// 借用查询面的拥有侧键（`K` 侧）：`Borrow<BorrowProbeKey>`；其 `Hash` 必须与
    /// `Q` 侧一致（`Borrow` 契约：`hash(borrowed) == hash(owner)`），故**不**带探针
    /// ——命中只可能来自调用方的 `Q` 实现。
    #[derive(Clone, Debug)]
    struct OwnedBorrowKey {
        inner: BorrowProbeKey,
    }

    impl std::borrow::Borrow<BorrowProbeKey> for OwnedBorrowKey {
        fn borrow(&self) -> &BorrowProbeKey {
            &self.inner
        }
    }

    impl Hash for OwnedBorrowKey {
        fn hash<H: Hasher>(&self, state: &mut H) {
            // 与 `BorrowProbeKey::hash` 逐字同源（`Borrow` 契约：两者 hash 相等）
            // ——但**不带探针**：命中只可能来自调用方的 `Q` 实现。
            self.inner.0.hash(state);
        }
    }

    impl PartialEq for OwnedBorrowKey {
        fn eq(&self, other: &Self) -> bool {
            self.inner.0 == other.inner.0
        }
    }

    impl Eq for OwnedBorrowKey {}

    /// 读路径探针值（`V` 侧）：`Clone` 记录命中。
    #[derive(Debug)]
    struct CloneProbeValue(u32);

    impl Clone for CloneProbeValue {
        fn clone(&self) -> Self {
            probe_hit();
            Self(self.0)
        }
    }

    /// #82：共享面查表路径（`get`）——键的 `Hash` 在内部 mutex 下执行。
    #[test]
    fn lru_get_hashes_key_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<HashProbeKey, u32>::new(8));
        cache.insert(HashProbeKey(1), 1); // 播种阶段探针未装 ⇒ 无命中记录
        install_probe(lru_probe(&cache));
        let got = cache.get(&HashProbeKey(1));
        uninstall_probe();

        assert_eq!(got, Some(1));
        assert_ran_under_core_mutex("SyncLruCache::get 的键 Hash");
    }

    /// #82：共享面查表路径（`peek`）——键的 `Hash` 在内部 mutex 下执行。
    #[test]
    fn lru_peek_hashes_key_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<HashProbeKey, u32>::new(8));
        cache.insert(HashProbeKey(1), 1);
        install_probe(lru_probe(&cache));
        let got = cache.peek(&HashProbeKey(1));
        uninstall_probe();

        assert_eq!(got, Some(1));
        assert_ran_under_core_mutex("SyncLruCache::peek 的键 Hash");
    }

    /// #82：共享面存在性判定（`contains`）——键的 `Hash` 在内部 mutex 下执行。
    #[test]
    fn lru_contains_hashes_key_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<HashProbeKey, u32>::new(8));
        cache.insert(HashProbeKey(1), 1);
        install_probe(lru_probe(&cache));
        let has = cache.contains(&HashProbeKey(1));
        uninstall_probe();

        assert!(has);
        assert_ran_under_core_mutex("SyncLruCache::contains 的键 Hash");
    }

    /// #82：共享面相等判定——键的 `Eq` 在内部 mutex 下执行（查表命中即比较）。
    #[test]
    fn lru_get_compares_key_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<EqProbeKey, u32>::new(8));
        cache.insert(EqProbeKey(1), 1);
        install_probe(lru_probe(&cache));
        let got = cache.get(&EqProbeKey(1));
        uninstall_probe();

        assert_eq!(got, Some(1));
        assert_ran_under_core_mutex("SyncLruCache::get 的键 Eq");
    }

    /// #82：共享面变更路径（`insert` 新键 + `remove`）——键的 `Hash` 在内部
    /// mutex 下执行（覆盖路径同样先查表 ⇒ 命中）。
    #[test]
    fn lru_mutation_hashes_key_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<HashProbeKey, u32>::new(8));
        install_probe(lru_probe(&cache));
        cache.insert(HashProbeKey(1), 1);
        cache.insert(HashProbeKey(2), 2);
        let removed = cache.remove(&HashProbeKey(1));
        uninstall_probe();

        assert_eq!(removed, Some(1));
        assert_ran_under_core_mutex("SyncLruCache::insert/remove 的键 Hash");
    }

    /// #82：TTL 共享面查表路径（`get`）——键的 `Hash` 在内部 mutex 下执行
    /// （过期检查同样走查表）。
    #[test]
    fn ttl_get_hashes_key_under_core_mutex() {
        let clock = Arc::new(skeleton::clock::MockClock::new());
        let cache = Arc::new(SyncTtlCache::<HashProbeKey, u32>::new(clock, 8));
        cache.insert(HashProbeKey(1), 1, std::time::Duration::from_secs(60));
        install_probe(ttl_probe(&cache));
        let got = cache.get(&HashProbeKey(1));
        uninstall_probe();

        assert_eq!(got, Some(1));
        assert_ran_under_core_mutex("SyncTtlCache::get 的键 Hash");
    }

    /// #82：借用查询面（`K: Borrow<Q>`）——**调用方**的 `Q::Hash`/`Q::Eq` 在内部
    /// mutex 下执行（`Q` 侧实现由调用方完全控制）。
    #[test]
    fn ttl_borrowed_lookup_runs_caller_key_traits_under_core_mutex() {
        let clock = Arc::new(skeleton::clock::MockClock::new());
        let cache = Arc::new(SyncTtlCache::<OwnedBorrowKey, u32>::new(clock, 8));
        cache.insert(
            OwnedBorrowKey {
                inner: BorrowProbeKey(1),
            },
            1,
            std::time::Duration::from_secs(60),
        );
        install_probe(ttl_probe(&cache));
        let got = cache.get(&BorrowProbeKey(1));
        uninstall_probe();

        assert_eq!(got, Some(1));
        assert_ran_under_core_mutex("SyncTtlCache::get 借用查询面的 Q::Hash/Q::Eq");
    }

    /// #83：`SyncLruCache` 读路径——`V::clone` 在内部 mutex 下执行（`get`/`peek`）。
    #[test]
    fn lru_read_clones_value_under_core_mutex() {
        let cache = Arc::new(SyncLruCache::<u32, CloneProbeValue>::new(8));
        cache.insert(1, CloneProbeValue(7));
        install_probe(lru_probe(&cache));
        let got = cache.get(&1);
        let peeked = cache.peek(&1);
        uninstall_probe();

        assert_eq!(got.map(|v| v.0), Some(7));
        assert_eq!(peeked.map(|v| v.0), Some(7));
        assert_ran_under_core_mutex("SyncLruCache::get/peek 的 V::clone");
    }

    /// #83：`SyncTtlCache` 读路径——过期检查与 LRU touch 之后的 `V::clone` 在内部
    /// mutex 下执行（`get`/`peek`）。
    #[test]
    fn ttl_read_clones_value_under_core_mutex() {
        let clock = Arc::new(skeleton::clock::MockClock::new());
        let cache = Arc::new(SyncTtlCache::<u32, CloneProbeValue>::new(clock, 8));
        cache.insert(1, CloneProbeValue(7), std::time::Duration::from_secs(60));
        install_probe(ttl_probe(&cache));
        let got = cache.get(&1);
        let peeked = cache.peek(&1);
        uninstall_probe();

        assert_eq!(got.map(|v| v.0), Some(7));
        assert_eq!(peeked.map(|v| v.0), Some(7));
        assert_ran_under_core_mutex("SyncTtlCache::get/peek 的 V::clone");
    }

    /// 单线程面**没有**共享 mutex ⇒ 用户 trait 在调用方栈上同步执行（不在任何
    /// 临界区内）。本测试只锁定「单线程面无内部锁」这一事实：共享面的
    /// `try_lock` 结论不适用于单线程面（那里不存在可争用的内部 mutex）。
    #[test]
    fn single_threaded_faces_have_no_shared_mutex() {
        let mut lru: LruCache<HashProbeKey, u32> = LruCache::new(8);
        lru.insert(HashProbeKey(1), 1);
        assert_eq!(lru.get(&HashProbeKey(1)), Some(&1));

        let clock = Arc::new(skeleton::clock::MockClock::new());
        let mut ttl: TtlCache<HashProbeKey, u32> = TtlCache::new(clock, 8);
        ttl.insert(HashProbeKey(1), 1, std::time::Duration::from_secs(60));
        assert_eq!(ttl.get(&HashProbeKey(1)), Some(&1));
    }
}

/// 锁外 Clock 边界（issue#101）：注入时钟的 `now_instant` **不得**在 cache mutex
/// 下执行。与 `lock_boundary_probe`（#82/#83：证明键 trait 与读侧 `V::clone`
/// **确实**在锁内）互为反面——本组只钉死「Clock 不在锁内」。
#[cfg(test)]
mod clock_outside_lock {
    use super::SyncTtlCache;
    use skeleton::clock::Clock;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, TryLockError, Weak};
    use std::time::{Duration, Instant, SystemTime};

    /// 探针读数：`calls` = `now_instant` 调用次数；`under_lock` = 观察到 cache
    /// mutex **已被持有**的次数（必须恒 0——那就是「Clock 在锁内」的回归信号）。
    struct Probe {
        calls: AtomicU64,
        under_lock: AtomicU64,
    }

    impl Probe {
        fn new() -> Self {
            Self {
                calls: AtomicU64::new(0),
                under_lock: AtomicU64::new(0),
            }
        }

        /// 对目标缓存的内部 mutex 做 `try_lock`：`WouldBlock` = 本次 Clock 调用
        /// 就发生在临界区里（同线程重入，非重入锁立即失败 ⇒ 不阻塞、不挂死）。
        fn hit(&self, target: &Mutex<Weak<SyncTtlCache<u32, u32>>>) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let weak = target.lock().expect("probe 目标缓存锁").clone();
            if let Some(cache) = weak.upgrade() {
                match cache.core.try_lock() {
                    Ok(_) | Err(TryLockError::Poisoned(_)) => {}
                    Err(TryLockError::WouldBlock) => {
                        self.under_lock.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }

    /// 探针钟：每次 `now_instant` 观测 cache mutex 是否被持有（时间恒为 `base`）。
    struct ProbeClock {
        base: Instant,
        target: Mutex<Weak<SyncTtlCache<u32, u32>>>,
        probe: Probe,
    }

    impl Clock for ProbeClock {
        fn now_instant(&self) -> Instant {
            self.probe.hit(&self.target);
            self.base
        }

        fn now_system(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }
    }

    fn probe_cache(cap: usize) -> (Arc<ProbeClock>, Arc<SyncTtlCache<u32, u32>>) {
        let clock = Arc::new(ProbeClock {
            base: Instant::now(),
            target: Mutex::new(Weak::new()),
            probe: Probe::new(),
        });
        let cache = Arc::new(SyncTtlCache::new(Arc::clone(&clock) as Arc<dyn Clock>, cap));
        *clock.target.lock().expect("probe 目标缓存锁") = Arc::downgrade(&cache);
        (clock, cache)
    }

    /// #101：满插路径（过期清扫 + 容量淘汰都走到）的每次 Clock 调用都在 cache
    /// mutex 之外——两阶段共多次取样，但**一次都不在锁内**。
    #[test]
    fn insert_reads_clock_outside_cache_mutex() {
        let (clock, cache) = probe_cache(2);
        cache.insert(1, 1, Duration::from_secs(60));
        cache.insert(2, 2, Duration::from_secs(60));
        cache.insert(3, 3, Duration::from_secs(60)); // 满插：清扫 + 逐 LRU 尾
        assert_eq!(cache.len(), 2);

        let calls = clock.probe.calls.load(Ordering::SeqCst);
        assert!(calls >= 4, "insert 两阶段都读到时钟（实际 {calls} 次）");
        assert_eq!(
            clock.probe.under_lock.load(Ordering::SeqCst),
            0,
            "insert 的 Clock 调用必须在 cache mutex 之外"
        );
    }

    /// 探针鉴别力对照：**故意**在临界区内取样 ⇒ 探针必须记到 `WouldBlock`。
    /// （证明上面那个「0 次」不是探针失效的空断言。）
    #[test]
    fn probe_detects_clock_read_inside_cache_mutex() {
        let (clock, cache) = probe_cache(2);
        let g = cache.lock_core();
        let _ = clock.now_instant();
        drop(g);

        assert_eq!(clock.probe.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            clock.probe.under_lock.load(Ordering::SeqCst),
            1,
            "对照：锁内取样必须被探针抓到（否则上面的 0 次无意义）"
        );
    }

    /// 可重入钟：`now_instant` 内调同一缓存的 `len`/`contains`——满插路径不得
    /// 自锁。先 `try_lock` 判定锁可用：若实现把 Clock 读回锁内，这里在阻塞前
    /// 就确定性失败（而非挂死测试进程）。
    struct ReentrantClock {
        base: Instant,
        target: Mutex<Weak<SyncTtlCache<u32, u32>>>,
        nested: AtomicBool,
    }

    impl Clock for ReentrantClock {
        fn now_instant(&self) -> Instant {
            if self.nested.swap(true, Ordering::SeqCst) {
                // 内层（由 `contains`/`get` 自己的时钟读触发）：不再递归探测。
                return self.base;
            }
            let weak = self.target.lock().expect("probe 目标缓存锁").clone();
            if let Some(cache) = weak.upgrade() {
                if let Err(TryLockError::WouldBlock) = cache.core.try_lock() {
                    panic!("Clock 在 cache mutex 内执行（issue#101 回归：重入自锁面）");
                }
                let _ = cache.len();
                let _ = cache.contains(&1);
            }
            self.nested.store(false, Ordering::SeqCst);
            self.base
        }

        fn now_system(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }
    }

    #[test]
    fn reentrant_clock_can_read_same_cache() {
        let clock = Arc::new(ReentrantClock {
            base: Instant::now(),
            target: Mutex::new(Weak::new()),
            nested: AtomicBool::new(false),
        });
        let cache = Arc::new(SyncTtlCache::new(Arc::clone(&clock) as Arc<dyn Clock>, 2));
        *clock.target.lock().expect("probe 目标缓存锁") = Arc::downgrade(&cache);

        cache.insert(1, 1, Duration::from_secs(60));
        cache.insert(2, 2, Duration::from_secs(60));
        cache.insert(3, 3, Duration::from_secs(60)); // 满插：清扫 + 驱逐
        assert_eq!(cache.get(&3), Some(3));
    }

    /// panic 钟：在第 **2** 次读取（阶段 2 的清扫时间快照）展开——锁外 ⇒ 不投毒
    /// （本类型 poison 策略是 fail-fast，炮在锁内会让后续访问全炸）。
    struct PanicClock {
        base: Instant,
        /// 剩余放行次数：减到 0 的那次读取 panic（0 = 已解除）。
        remaining: AtomicU64,
    }

    impl Clock for PanicClock {
        fn now_instant(&self) -> Instant {
            if self.remaining.load(Ordering::SeqCst) > 0
                && self.remaining.fetch_sub(1, Ordering::SeqCst) == 1
            {
                panic!("clock boom（锁外展开）");
            }
            self.base
        }

        fn now_system(&self) -> SystemTime {
            SystemTime::UNIX_EPOCH
        }
    }

    #[test]
    fn clock_panic_expands_outside_cache_mutex_and_cache_stays_usable() {
        let clock = Arc::new(PanicClock {
            base: Instant::now(),
            remaining: AtomicU64::new(0),
        });
        let cache = SyncTtlCache::<u32, u32>::new(Arc::clone(&clock) as Arc<dyn Clock>, 2);
        cache.insert(1, 1, Duration::from_secs(60));

        // 阶段 1 的读取放行，阶段 2 的清扫快照 panic（旧实现此刻持锁 ⇒ 投毒）。
        clock.remaining.store(2, Ordering::SeqCst);
        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cache.insert(2, 2, Duration::from_secs(60));
        }));
        assert!(caught.is_err(), "clock panic 原样传播给调用方");

        // 锁未被毒：后续读写照常（poison 策略是 fail-fast ⇒ 毒了这里必炸）。
        assert_eq!(cache.get(&1), Some(1));
        cache.insert(3, 3, Duration::from_secs(60));
        assert_eq!(cache.len(), 2);
    }
}
