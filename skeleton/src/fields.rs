//! 可观测字段值面：一组「名 + 值」字段的纯值形状（[`FieldValue`] + [`FieldBag`]）。
//!
//! # 为什么住契约层（`skeleton`）而不是 `skeleton_infra`
//!
//! 本件只依赖 std（`Arc`/`LazyLock`/`Duration`），零 feature 门、零第三方；消费
//! 形态是「组装一组观测值交给导出方」——**不需要任何机器**（无 IO、无运行时、
//! 无序列化），与 [`config`](crate::config)/[`lint`](crate::lint) 同性质：值面
//! 住零依赖 crate，采用方单开 `udf-skeleton` 即可用，不必为字段袋把运行时拖进
//! 依赖树。
//!
//! # 坑面 armor（生态事实逐条对表，7 条，`tests/fields.rs` + `tests/fields_alloc.rs` 钉死）
//!
//! - **迭代顺序**：HashMap 形会丢序——本件 `Vec` + set 语义保**插入序**，[`FieldBag::iter`]
//!   顺序稳定可断言（`iter_order_is_insertion_order_and_stable`）。
//! - **同名覆盖策略必须显式**：last-wins、**原地覆盖**——值换、位置不动（可断言，
//!   `same_name_overwrites_value_in_place` / `merge_overwrites_existing_and_appends_new_in_other_order`）；
//!   bag 构造即去重，**恒无重名**。
//! - **浮点**：`Float(f64)` 破坏 `Eq`/`Hash`（NaN ≠ NaN）——只派生 `PartialEq`；
//!   **Float 字段不可作 map 键/集元素**，含 NaN 的两 bag 比较恒 false
//!   （`value_variants_construct_and_compare` 钉死文档行为）。
//! - **`Arc` COW 克隆语义**：clone = 一次原子自增（O(1)，与字段数无关）；改写走
//!   `Arc::make_mut`——bag 独占时原地改，有在飞共享时改写者付**一次性** O(n) 整表
//!   克隆，共享副本不受影响；并发共享只读安全（`clone_is_isolated_from_later_writes`；
//!   分配锚 `tests/fields_alloc.rs`，example 维④/维⑤）。
//! - **字段名 `&'static str`**：可观测字段名是编译期常量（tracing 字段先例），
//!   零分配——`String` 名会逼出每字段一次分配（`new_and_default_are_zero_alloc` /
//!   `first_insert_allocates_at_least_one_event`）。
//! - **O(n) 线性 get/insert**：观测字段袋是小集合（个位数量级），线性扫描换零哈希
//!   表分配；若未来出现大规模袋证据，升级路径 = 排序索引（见收窄声明）。
//! - **非 exhaustive 词表**：[`FieldValue`] `#[non_exhaustive]`（同 [`Effect`](crate::Effect)/
//!   [`Restore`](crate::Restore) 纪律，F-07）——跨 crate 的 match 必须带通配臂，
//!   采用方在编译期被强制面对「未知值形态」的分支而非静默漏配（doctest 即下游
//!   视角演示）。
//!
//! # 收窄声明
//!
//! - **只做值面**：无导出/上报/落盘/采样/限流——消费方拿到 bag 后自行映射到
//!   tracing/OTLP/日志等导出格式。**无 serde**：契约层零依赖纪律，导出格式归
//!   消费方自映射。
//! - **无字段名注册表**：名字即 `&'static str` 编译期常量，无运行期登记/驻留池/
//!   去重表。
//! - **无日志级别/过滤语义**：值面与呈现语义解耦，级别判定归消费方。
//! - **无并发写**：改写走 `&mut`（独占借用）；跨线程共享靠 clone（只读，见 armor
//!   COW 条）。
//!
//! # Examples
//!
//! 典型消费形态：请求收尾时组装观测袋；采集面共享给别的子系统再补字段（COW，
//! 原袋不受影响）。导出不在本件面内（收窄声明）。
//!
//! ```
//! use skeleton::fields::{FieldBag, FieldValue};
//! use std::time::Duration;
//!
//! let mut bag = FieldBag::new();
//! bag.insert("http.method", FieldValue::Str(String::from("GET")));
//! bag.insert("http.status", FieldValue::Int(200));
//! bag.insert("http.duration", FieldValue::Duration(Duration::from_millis(12)));
//! bag.insert("http.retries", FieldValue::Count(1));
//!
//! let mut enriched = bag.clone(); // 克隆 = 一次原子自增
//! enriched.insert("peer", FieldValue::Str(String::from("edge-7")));
//! assert_eq!(enriched.len(), 5);
//! assert_eq!(bag.len(), 4); // 原袋不受影响（COW 隔离）
//! ```

use std::sync::{Arc, LazyLock};

/// 可观测字段的值词表：观测面最常见的六种形态。
///
/// `#[non_exhaustive]`（仓内词表 enum 纪律，同 [`Effect`](crate::Effect)/
/// [`Restore`](crate::Restore)，F-07）：加变体属非破坏演进，跨 crate 的 match
/// 必须带通配臂。下游穷尽 match 被编译期拒绝（doctest 即下游 crate 视角编译
/// 验证，纪律同 [`EffectId`](crate::EffectId) 的元组构造封口）：
///
/// ```compile_fail
/// use skeleton::fields::FieldValue;
/// match FieldValue::Int(1) {
///     FieldValue::Int(_) => {}
///     FieldValue::Float(_) => {}
///     FieldValue::Bool(_) => {}
///     FieldValue::Str(_) => {}
///     FieldValue::Duration(_) => {}
///     FieldValue::Count(_) => {}
/// } // ERROR E0004: non-exhaustive patterns，`_` 臂缺失
/// ```
///
/// 带通配臂即放行（本 doctest 以下游 crate 身份编译，通配臂是硬要求）：
///
/// ```
/// use skeleton::fields::FieldValue;
///
/// fn describe(value: &FieldValue) -> String {
///     match value {
///         FieldValue::Int(v) => format!("int {v}"),
///         FieldValue::Float(v) => format!("float {v}"),
///         FieldValue::Bool(v) => format!("bool {v}"),
///         FieldValue::Str(v) => format!("str {v}"),
///         FieldValue::Duration(v) => format!("duration {v:?}"),
///         FieldValue::Count(v) => format!("count {v}"),
///         _ => String::from("future variant"),
///     }
/// }
///
/// assert_eq!(describe(&FieldValue::Count(3)), "count 3");
/// ```
///
/// 只派生 `PartialEq` 不派生 `Eq`/`Hash`（armor 浮点条）：`Float(f64)` 的
/// NaN ≠ NaN 使全值相等性不成立——`FieldValue` **不可作 map 键/集元素**，需要
/// 键时用字段名（`&'static str`）。`Int`（有符号测量值）与 `Count`（无符号单调
/// 计数）语义不同不混用：相等性也按变体区分（`Int(1) != Count(1)`）。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum FieldValue {
    /// 有符号测量值（温度、带符号的差值等 gauge 形态）。
    Int(i64),
    /// 浮点测量值。NaN ≠ NaN（armor 浮点条）：含 NaN 的两值/两 bag 比较恒 false。
    Float(f64),
    /// 开关/标志位。
    Bool(bool),
    /// 文本值（标签、peer 名等）。构造计一次堆分配，随 bag 值语义克隆。
    Str(String),
    /// 时长（耗时观测；`Debug` 走 std 人类可读形态，如 `12ms`）。
    Duration(std::time::Duration),
    /// 无符号单调计数（请求数、重试次数——只增不减的 counter 形态）。
    Count(u64),
}

/// 进程级空表：所有空 bag 共享同一份 `Arc`（克隆 = 原子自增），`LazyLock` 懒建
/// 一次——`new`/`Default` 之外的任何构造路径都从这里起步，稳态零堆分配。
static EMPTY: LazyLock<Arc<Vec<(&'static str, FieldValue)>>> =
    LazyLock::new(|| Arc::new(Vec::new()));

/// 一组可观测字段（名 + 值）：`Arc<Vec<..>>` 值语义小袋——插入序稳定、同名覆盖、
/// 克隆 O(1)、构造即去重（恒无重名）。
///
/// 内部形状（armor 逐条的理由载体）：槽位存 `(&'static str, FieldValue)`——名字
/// 零分配；`Arc` 包 `Vec`——clone 廉价、改写 COW、`Vec` 保插入序（见[模块 armor](self)）。
///
/// # Examples
///
/// ```
/// use skeleton::fields::{FieldBag, FieldValue};
///
/// let mut bag = FieldBag::new();
/// assert!(bag.is_empty());
/// bag.insert("http.status", FieldValue::Int(503));
/// bag.insert("http.retries", FieldValue::Count(2));
/// bag.insert("http.status", FieldValue::Int(200)); // 同名覆盖：值换、位置不动
/// assert_eq!(bag.len(), 2); // bag 恒无重名（构造即去重）
/// assert_eq!(bag.get("http.status"), Some(&FieldValue::Int(200)));
/// assert_eq!(
///     bag.iter().map(|(name, _)| name).collect::<Vec<_>>(),
///     ["http.status", "http.retries"] // 插入序 = 迭代序
/// );
///
/// let mut base = FieldBag::new();
/// base.insert("peer", FieldValue::Str(String::from("edge-1")));
/// base.merge(&bag); // 已有键覆盖（位置不动），新键按 other 序追加
/// assert_eq!(base.get("http.retries"), Some(&FieldValue::Count(2)));
///
/// let snapshot = base.clone(); // 克隆 = 一次原子自增，与字段数无关
/// base.insert("peer", FieldValue::Str(String::from("edge-2")));
/// // COW：改写走 Arc::make_mut，在飞 clone 不受原体后续改写影响
/// assert_eq!(
///     snapshot.get("peer"),
///     Some(&FieldValue::Str(String::from("edge-1")))
/// );
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct FieldBag {
    inner: Arc<Vec<(&'static str, FieldValue)>>,
}

// Default 不走 derive：derive 会每次构造一个新 `Arc`（一次堆分配），违背
// 「空 bag 零分配」契约——与 `new` 同走共享 EMPTY（`tests/fields_alloc.rs`
// 的 `new_and_default_are_zero_alloc` 钉死）。
impl Default for FieldBag {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldBag {
    /// 空 bag：与 [`Default`] 同一形状——共享进程级空表（[`LazyLock`] 懒建一次），
    /// **零堆分配**（armor COW/名字条：克隆空 bag 也只是原子自增）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let bag = FieldBag::new();
    /// assert_eq!(bag.len(), 0);
    /// assert!(bag.is_empty());
    /// assert_eq!(bag, FieldBag::default()); // new 与 Default 同一值
    /// assert_eq!(bag.get("anything"), None);
    /// assert_eq!(bag, FieldBag::new());
    /// ```
    #[must_use = "空 bag 被丢弃通常意味着漏了绑定/返回"]
    pub fn new() -> Self {
        Self {
            inner: Arc::clone(&EMPTY),
        }
    }

    /// 插入或覆盖一个字段（last-wins）：同名已存在 → **值原位替换、位置不动**；
    /// 否则按插入序追加。调用后 bag 仍恒无重名。
    ///
    /// COW（armor COW 条）：`Arc::make_mut` 改写——bag 独占时原地改（零分配）；
    /// 有在飞 clone 共享时先付一次性 O(n) 整表克隆再改（共享副本不受影响）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut bag = FieldBag::new();
    /// bag.insert("a", FieldValue::Int(1));
    /// bag.insert("b", FieldValue::Int(2));
    /// bag.insert("a", FieldValue::Int(3)); // 覆盖：值换、位置不动
    /// assert_eq!(bag.len(), 2);
    /// assert_eq!(
    ///     bag.iter().collect::<Vec<_>>(),
    ///     [("a", &FieldValue::Int(3)), ("b", &FieldValue::Int(2))]
    /// );
    /// ```
    pub fn insert(&mut self, name: &'static str, value: FieldValue) {
        let entries = Arc::make_mut(&mut self.inner);
        if let Some(slot) = entries.iter_mut().find(|(existing, _)| *existing == name) {
            slot.1 = value;
        } else {
            entries.push((name, value));
        }
    }

    /// 按名取值（线性扫描，armor O(n) 条）：命中返回 `Some(&FieldValue)`，未命中
    /// `None`。构造时名字是 `&'static str`，读时任意 `&str` 皆可（按内容精确匹配，
    /// 无前缀语义）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut bag = FieldBag::new();
    /// bag.insert("http.status", FieldValue::Int(200));
    /// assert_eq!(bag.get("http.status"), Some(&FieldValue::Int(200)));
    /// assert_eq!(bag.get("missing"), None);
    /// assert_eq!(bag.get("http"), None); // 整名精确匹配，无前缀语义
    /// ```
    pub fn get(&self, name: &str) -> Option<&FieldValue> {
        self.inner
            .iter()
            .find(|(existing, _)| *existing == name)
            .map(|(_, value)| value)
    }

    /// 按插入序迭代（名，值）（armor 顺序条）：顺序稳定可断言，与字段数线性。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut bag = FieldBag::new();
    /// bag.insert("a", FieldValue::Int(1));
    /// bag.insert("b", FieldValue::Count(2));
    /// let names: Vec<&str> = bag.iter().map(|(name, _)| name).collect();
    /// assert_eq!(names, ["a", "b"]); // 插入序 = 迭代序
    /// ```
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &FieldValue)> {
        self.inner.iter().map(|(name, value)| (*name, value))
    }

    /// 字段数（bag 恒无重名，即不同名字个数）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut bag = FieldBag::new();
    /// assert_eq!(bag.len(), 0);
    /// bag.insert("a", FieldValue::Int(1));
    /// bag.insert("a", FieldValue::Int(2)); // 覆盖不改长度
    /// assert_eq!(bag.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// 是否为空（零字段）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut bag = FieldBag::new();
    /// assert!(bag.is_empty());
    /// bag.insert("a", FieldValue::Int(1));
    /// assert!(!bag.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// 并入 `other` 的全部字段：已存在的键**覆盖**（值换、位置不动），新键按
    /// `other` 的插入序**追加**——merge 后 `self` 仍恒无重名。`other` 不被改动。
    ///
    /// 语义 = 对 `other` 逐项 [`insert`](FieldBag::insert)：COW 与分配口径同
    /// `insert`（独占时追加段按 `Vec` 倍增摊销；`Str` 值随值克隆计分配）。
    ///
    /// # Examples
    ///
    /// ```
    /// use skeleton::fields::{FieldBag, FieldValue};
    ///
    /// let mut base = FieldBag::new();
    /// base.insert("a", FieldValue::Int(1));
    /// base.insert("b", FieldValue::Int(2));
    ///
    /// let mut patch = FieldBag::new();
    /// patch.insert("c", FieldValue::Bool(true));
    /// patch.insert("b", FieldValue::Count(9));
    /// base.merge(&patch);
    ///
    /// // 已有键 b 覆盖（位置不动）；新键 c 按 patch 序追加
    /// assert_eq!(
    ///     base.iter().collect::<Vec<_>>(),
    ///     [
    ///         ("a", &FieldValue::Int(1)),
    ///         ("b", &FieldValue::Count(9)),
    ///         ("c", &FieldValue::Bool(true)),
    ///     ]
    /// );
    /// assert_eq!(patch.len(), 2); // other 不被改动
    /// ```
    pub fn merge(&mut self, other: &FieldBag) {
        for (name, value) in other.inner.iter() {
            self.insert(name, value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// NaN 行为（armor 浮点条）：只派生 PartialEq——NaN 与自身比较恒 false，
    /// 编译器也接受（PartialEq 允许自反破缺），这正是不派生 Eq 的原因。
    #[test]
    fn float_nan_is_never_equal_to_itself() {
        let nan = FieldValue::Float(f64::NAN);
        assert_ne!(nan, nan);
    }
}
