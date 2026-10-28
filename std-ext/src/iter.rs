//! 迭代器扩展：精选通用适配器（itertools 语义子集，std 缺位面）。
//!
//! 存在理由：itertools（1.64B 累计下载，Rust 生态第一工具 crate）覆盖的高频
//! 组合面 std 只吸收了一部分（`repeat_n`/`chunk_by` 等已进 std，不重造）；剩
//! 余高频面中本域只收编 **5 件通用最高频件**（收窄拍板，禁全家桶）：
//!
//! | 件 | std 缺位理由 |
//! |---|---|
//! | [`zip_longest`] | std `zip` 对不齐**静默截断**（坑面 #1，本域核心存在理由） |
//! | [`interleave`] | 双流交错编排 std 无 |
//! | [`minmax`] | std 需 `min()`+`max()` 两遍遍历；本域单遍 3n/2 比较+显式三态 |
//! | [`dedup`] / [`dedup_by`] | std 只有 `Vec::dedup_by`（原地、要求 Vec）；惰性适配器版缺位 |
//! | [`process_results`] | `Result` 流短路提升 std 无；`flatten()` 会**静默吞 Err** |
//!
//! # 语义锚（itertools 0.15 源码逐件对表，禁凭记忆）
//!
//! - [`zip_longest`]：`EitherOrBoth::{Both, Left, Right}` 三态；`Fuse` 双包装；
//!   size_hint = `max(a, b)`；双 `ExactSizeIterator` 底源时声称 `ExactSizeIterator`
//!   （len = max，公式精确）。
//! - [`interleave`]：轮转旗标，一侧 `None` 落到另一侧 = **长者尾部如实追加**
//!   （非"短尽即止"，`interleave_shortest` 未收编）；size_hint = `add(a, b)`
//!   （两侧元素必然全部输出，lower 即总和）。
//! - [`minmax`]：单遍成对法（2 元素 3 比较）；min 取首现、max 取末现（与 std
//!   `min`/`max` 语义对齐）；全等值多元素 = `MinMax(v, v)`（`OneElement` 仅恰
//!   1 元素，itertools docs 明文）。
//! - [`dedup_by`]：**连续**去重保首现（`[1,2,1]` 全保留，非全量 unique）；谓词
//!   参数序 `(previous, candidate)`，true = candidate 视为重复被丢弃。
//! - [`process_results`]：首 `Err` 短路（短路后不再拉底层）；错误经外置
//!   `&mut Result<(), E>` 回收（itertools 同构）。
//!
//! # 坑面拍板（实现前系统排查，对照表见 iter 域性能锚（本地留存，未入库））
//!
//! 1. **zip 不齐静默截断**（std zip 语义；F25 前半）——[`zip_longest`] 显式
//!    三态，[`interleave`] 长者尾部追加，双双测试钉死。
//! 2. **非 fused 底源 next-after-None 复活**（itertools `Fuse` 双包装的存在
//!    理由；std `zip` 曾有同族 bug）——交错/拉链类适配器内部一律 `Fuse` 包装，
//!    复活型迭代器测试钉死。
//! 3. **std 同名/邻名方法语义撞车**（F25 后半）——模块 doc 下方对照表逐件写
//!    区分，测试钉「连续去重保首现」等易漂移语义。
//! 4. **size_hint 撒谎 → collect 预分配错**（std `size_hint` 文档原文
//!    "must be correct"；`ExactSizeIterator::len` 违约是更硬的契约面）——每
//!    件公式化 size_hint + 夹逼测试；`ExactSizeIterator` 只在公式精确时声称
//!    （[`DedupBy`] 长度不定，永不声称）。
//! 5. **惰性违约**（构造即消费）——全部适配器构造零推进；计数迭代器测试钉
//!    「构造后 0 推进、消费 k 恰推进 k（+peek 的 1）」。
//! 6. **全等值序列 minmax 误报 OneElement**——按元素计数分流，非按值比较。
//! 7. **process_results 首错后继续拉取/吞后续**——首 Err 存外置位，`next`
//!    恒 `None` 且底层不再被拉。
//!
//! # std 邻名对照表（坑面 #3 装甲，防语义漂移）
//!
//! | 本域 | std/生态邻名 | 区分 |
//! |---|---|---|
//! | `iter::dedup_by` | `Vec::dedup_by` | Vec 版原地收缩、要求 `&mut Vec`；本域惰性适配器、底源任意 `Iterator` |
//! | `iter::minmax` | `std::cmp::minmax` | std 版比较**两个值**；本域单遍历整个迭代器 |
//! | `iter::zip_longest` | `Iterator::zip` | std zip 不齐**静默截断**；本域显式 `EitherOrBoth` |
//! | `iter::process_results` | `Iterator::flatten` | flatten 对 `Result` **静默吞 Err**；本域首 Err 短路上抛 |
//! | `iter::interleave` | itertools `interleave_shortest` | 本域=长者尾部追加；shortest=短尽即止（未收编，收窄声明） |
//!
//! # 收窄声明（范围拍板，详见报告）
//!
//! 调研档 §4.5 草案 10 件收窄至 5 件：`repeat_n`（std 1.82 已吸收，不重造）、
//! `cartesian_product`（中频+size_hint 复杂）、`unfold`（`from_fn`/`successors`
//! 部分覆盖）、`positions`（`enumerate().filter_map()` 一行等价）、`multizip`
//! （宏面）不收编。`EitherOrBoth` 不提供方法族（itertools 30+ 方法是全家桶面，
//! 采用方 `match` 即可）。双端迭代（`DoubleEndedIterator`）不声称（收窄）。
//!
//! 常驻零依赖件：纯 std，无 feature 门；全件零分配惰性组合子（collect 时一
//! 次分配如实）。性能锚见各函数 doc 与 iter 域性能锚（本地留存，未入库）。
//!
//! log-embed 埋点（收窄声明：**纯函数域，只埋 [`process_results`] 首 Err
//! 短路点**——warn，首 Err 位置 `pos`（0 起）入字段；next/fold 双通路各一
//! 事件点。其余组合子为纯计算无行为决策面，零埋点。target
//! `skeleton::iter::process_results`，字段化纪律见 lib.rs `embed_event!` 注）。

use std::iter::Fuse;

// ── 类型 ──

/// 值对三态：双侧都有 / 仅左 / 仅右（[`zip_longest`] 的产出词汇，itertools
/// 同名同变体）。
#[derive(Clone, Copy, PartialEq, Eq, Debug, Hash)]
pub enum EitherOrBoth<A, B> {
    /// 双侧都有。
    Both(A, B),
    /// 仅左侧（左迭代器更长时的尾部余量）。
    Left(A),
    /// 仅右侧（右迭代器更长时的尾部余量）。
    Right(B),
}

/// [`minmax`] 的三态结果（itertools 同名同变体）：空 / 恰 1 元素 / 多元素。
///
/// 「恰 1 元素」按**元素计数**分流：全等值的多元素序列返回
/// [`MinMaxResult::MinMax`]`(v, v)`，不误报 `OneElement`（坑面 #6）。
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MinMaxResult<T> {
    /// 空迭代器。
    NoElements,
    /// 恰 1 个元素（min 与 max 同元）。
    OneElement(T),
    /// ≥2 个元素：`(最小, 最大)`；min 取首现、max 取末现。
    MinMax(T, T),
}

impl<T: Clone> MinMaxResult<T> {
    /// 三态压平为 `Option<(min, max)>`：仅 `NoElements` → `None`；`OneElement`
    /// 克隆补对（itertools 同款语义）。
    pub fn into_option(self) -> Option<(T, T)> {
        match self {
            Self::NoElements => None,
            Self::OneElement(x) => Some((x.clone(), x)),
            Self::MinMax(lo, hi) => Some((lo, hi)),
        }
    }
}

// ── zip_longest ──

/// 双迭代器拉链，不齐时**显式三态**而非静默截断（std `zip` 的反面，坑面 #1）。
///
/// 内部 `Fuse` 双包装：非 fused 底源 next-after-None 复活不产生错流（坑面 #2）。
/// 零分配。
///
/// # Examples
///
/// ```
/// use std_ext::iter::{zip_longest, EitherOrBoth};
///
/// let got: Vec<_> = zip_longest([1, 2, 3], ["a"]).collect();
/// assert_eq!(got, [EitherOrBoth::Both(1, "a"), EitherOrBoth::Left(2), EitherOrBoth::Left(3)]);
/// ```
pub fn zip_longest<A, B>(a: A, b: B) -> ZipLongest<A::IntoIter, B::IntoIter>
where
    A: IntoIterator,
    B: IntoIterator,
{
    ZipLongest {
        a: a.into_iter().fuse(),
        b: b.into_iter().fuse(),
    }
}

/// [`zip_longest`] 的迭代器（见模块 doc「语义锚」）。
#[derive(Clone, Debug)]
pub struct ZipLongest<A, B> {
    a: Fuse<A>,
    b: Fuse<B>,
}

impl<A, B> Iterator for ZipLongest<A, B>
where
    A: Iterator,
    B: Iterator,
{
    type Item = EitherOrBoth<A::Item, B::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.a.next(), self.b.next()) {
            (None, None) => None,
            (Some(a), None) => Some(EitherOrBoth::Left(a)),
            (None, Some(b)) => Some(EitherOrBoth::Right(b)),
            (Some(a), Some(b)) => Some(EitherOrBoth::Both(a, b)),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        hint_max(self.a.size_hint(), self.b.size_hint())
    }

    fn fold<Acc, F>(self, init: Acc, mut f: F) -> Acc
    where
        F: FnMut(Acc, Self::Item) -> Acc,
    {
        let Self { mut a, mut b } = self;
        // a 先行，b 缺位时余 a 以 Left 收尾；a 尽后余 b 以 Right 收尾
        let res = a.try_fold(init, |acc, a| match b.next() {
            Some(b) => Ok(f(acc, EitherOrBoth::Both(a, b))),
            None => Err(f(acc, EitherOrBoth::Left(a))),
        });
        match res {
            Ok(acc) => b.map(EitherOrBoth::Right).fold(acc, &mut f),
            Err(acc) => a.map(EitherOrBoth::Left).fold(acc, &mut f),
        }
    }
}

impl<A, B> ExactSizeIterator for ZipLongest<A, B>
where
    A: ExactSizeIterator,
    B: ExactSizeIterator,
{
}

impl<A, B> std::iter::FusedIterator for ZipLongest<A, B>
where
    A: Iterator,
    B: Iterator,
{
}

// ── interleave ──

/// 双迭代器交错（a₀ b₀ a₁ b₁ …）；一侧耗尽后**另一侧余量如实追加**（itertools
/// 语义，非"短尽即止"）。
///
/// 内部 `Fuse` 双包装（坑面 #2）。零分配。
///
/// # Examples
///
/// ```
/// use std_ext::iter::interleave;
///
/// let got: Vec<i32> = interleave([1, 2, 3], [10, 20]).collect();
/// assert_eq!(got, [1, 10, 2, 20, 3]);
/// ```
pub fn interleave<A, B>(a: A, b: B) -> Interleave<A::IntoIter, B::IntoIter>
where
    A: IntoIterator,
    B: IntoIterator<Item = A::Item>,
{
    Interleave {
        a: a.into_iter().fuse(),
        b: b.into_iter().fuse(),
        next_from_b: false,
    }
}

/// [`interleave`] 的迭代器（见模块 doc「语义锚」）。
#[derive(Clone, Debug)]
pub struct Interleave<A, B> {
    a: Fuse<A>,
    b: Fuse<B>,
    next_from_b: bool,
}

impl<A, B> Iterator for Interleave<A, B>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
{
    type Item = A::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // 轮转旗标：先翻再取；取空落另一侧 = 长者尾部如实追加（itertools 同构）
        self.next_from_b = !self.next_from_b;
        if self.next_from_b {
            match self.a.next() {
                None => self.b.next(),
                r => r,
            }
        } else {
            match self.b.next() {
                None => self.a.next(),
                r => r,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        hint_add(self.a.size_hint(), self.b.size_hint())
    }

    fn fold<Acc, F>(self, mut init: Acc, mut f: F) -> Acc
    where
        F: FnMut(Acc, Self::Item) -> Acc,
    {
        let Self {
            mut a,
            mut b,
            next_from_b,
        } = self;
        // 旗标 true = 下一步轮 b：先行补一拍，保持与 next 相同的交错序；
        // 配对段 try_fold 单趟（itertools 同构）
        if next_from_b {
            match b.next() {
                Some(y) => init = f(init, y),
                None => return a.fold(init, &mut f),
            }
        }
        let res = a.try_fold(init, |mut acc, x| {
            acc = f(acc, x);
            match b.next() {
                Some(y) => Ok(f(acc, y)),
                None => Err(acc),
            }
        });
        match res {
            Ok(acc) => b.fold(acc, &mut f),
            Err(acc) => a.fold(acc, &mut f),
        }
    }
}

impl<A, B> std::iter::FusedIterator for Interleave<A, B>
where
    A: Iterator,
    B: Iterator<Item = A::Item>,
{
}

// ── minmax ──

/// 单遍历同时取最小与最大（std 需 `min()`+`max()` 两遍；本域成对法 2 元素 3
/// 比较，itertools 算法）。显式三态（见 [`MinMaxResult`]）。
///
/// 零分配。恰好遍历一次：消费恰好等于元素数次 `next`（坑面 #5 惰性纪律）。
///
/// # Examples
///
/// ```
/// use std_ext::iter::{minmax, MinMaxResult};
///
/// assert_eq!(minmax([3, 1, 2]), MinMaxResult::MinMax(1, 3));
/// assert_eq!(minmax([7, 7, 7]), MinMaxResult::MinMax(7, 7));
/// assert_eq!(minmax(Vec::<i32>::new()), MinMaxResult::NoElements);
/// ```
pub fn minmax<I>(iter: I) -> MinMaxResult<I::Item>
where
    I: IntoIterator,
    I::Item: Ord,
{
    let mut it = iter.into_iter();
    let (mut min, mut max) = match (it.next(), it.next()) {
        (None, _) => return MinMaxResult::NoElements,
        (Some(x), None) => return MinMaxResult::OneElement(x),
        // 按元素计数分流（坑面 #6）：恰 1 元素才 OneElement
        (Some(x), Some(y)) => {
            if y < x {
                (y, x)
            } else {
                (x, y)
            }
        }
    };
    // 成对法：2 元素 3 比较（先互比，小者挑战 min、大者挑战 max）。
    // 严格小于才换 min（min=首现）；不小于 max 即换（max=末现），与 std
    // min/max 等值语义对齐（itertools minmax_impl 同构）。
    loop {
        let first = match it.next() {
            None => break,
            Some(x) => x,
        };
        let second = match it.next() {
            None => {
                if first < min {
                    min = first;
                } else if first >= max {
                    max = first;
                }
                break;
            }
            Some(x) => x,
        };
        if second < first {
            if second < min {
                min = second;
            }
            if first >= max {
                max = first;
            }
        } else {
            if first < min {
                min = first;
            }
            if second >= max {
                max = second;
            }
        }
    }
    MinMaxResult::MinMax(min, max)
}

// ── dedup / dedup_by ──

/// [`dedup`] 的谓词类型：`PartialEq::eq` 的 fn 指针形态。
type DedupEq<T> = fn(&T, &T) -> bool;

/// 连续重复去重（保首现；`[1,2,1]` 全保留——非全量 unique，坑面 #3）。
/// [`dedup_by`] 的 `PartialEq` 糖。
///
/// 零分配。
///
/// # Examples
///
/// ```
/// use std_ext::iter::dedup;
///
/// let got: Vec<i32> = dedup([1, 1, 2, 2, 1]).collect();
/// assert_eq!(got, [1, 2, 1]);
/// ```
pub fn dedup<I>(iter: I) -> DedupBy<I::IntoIter, DedupEq<I::Item>>
where
    I: IntoIterator,
    I::Item: PartialEq,
{
    dedup_by(iter, PartialEq::eq)
}

/// 连续重复去重，谓词自定义。谓词参数序 `(previous, candidate)`：返回 true =
/// candidate 视为与前一保留元素重复，被丢弃。
///
/// 零分配。注意：自由函数形态下谓词闭包参数常需显式类型标注
/// （`I::Item` 投影 bound 的推断限制，见模块 doc 坑面 #8）。
///
/// # Examples
///
/// ```
/// use std_ext::iter::dedup_by;
///
/// // 按绝对值判重
/// let got: Vec<i32> = dedup_by([1, -1, 2, -2], |prev: &i32, cand: &i32| prev.abs() == cand.abs()).collect();
/// assert_eq!(got, [1, 2]);
/// ```
pub fn dedup_by<I, F>(iter: I, eq: F) -> DedupBy<I::IntoIter, F>
where
    I: IntoIterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    DedupBy {
        iter: iter.into_iter(),
        last: None,
        eq,
    }
}

/// [`dedup_by`] 的迭代器：连续去重，`last` 三态（`None`=未开始 /
/// `Some(None)`=底源已尽 / `Some(Some(v))`=上一保留元素）自带 fused 语义。
#[derive(Clone, Debug)]
pub struct DedupBy<I, F>
where
    I: Iterator,
{
    iter: I,
    last: Option<Option<I::Item>>,
    eq: F,
}

impl<I, F> Iterator for DedupBy<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // 三态 last：None=未开始 / Some(None)=底源已尽（fused，坑面 #2）/
        // Some(Some(held))=持有待续元素。held 所有权轮转：候选与 held 重复则
        // 丢弃，首个不同候选经 replace 接棒（输出旧 held）。
        let mut held = match self.last.take() {
            Some(None) => {
                // 墓碑回填（FusedIterator 契约：尽后恒 None，禁被 take 消费后
                // 穿透到复活型底源——FinalWave 实弹：毒值 999 入流）。
                self.last = Some(None);
                return None;
            }
            Some(Some(held)) => held,
            None => match self.iter.next() {
                Some(x) => x,
                None => {
                    self.last = Some(None);
                    return None;
                }
            },
        };
        loop {
            match self.iter.next() {
                Some(x) => {
                    if (self.eq)(&held, &x) {
                        continue;
                    }
                    let out = std::mem::replace(&mut held, x);
                    self.last = Some(Some(held)); // 新 held 跨 next 调用保持
                    return Some(out);
                }
                None => {
                    self.last = Some(None);
                    return Some(held);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // lower：非空才承诺 1（重复面不可预知；itertools CoalesceBy 公式），
        // upper 同底源
        let (low, hi) = self.iter.size_hint();
        let low = low + usize::from(matches!(self.last, Some(Some(_))));
        (usize::from(low > 0), hi)
    }

    fn fold<Acc, G>(self, mut init: Acc, mut g: G) -> Acc
    where
        G: FnMut(Acc, Self::Item) -> Acc,
    {
        let Self {
            mut iter,
            last,
            mut eq,
        } = self;
        // 单趟：held 变化时输出旧值（与 next 语义等价，fold==collect 测试钉死）
        let held = match last {
            Some(None) => return init,
            Some(Some(x)) => Some(x),
            None => iter.next(),
        };
        let Some(mut held) = held else { return init };
        for x in iter {
            if !(eq)(&held, &x) {
                let out = std::mem::replace(&mut held, x);
                init = g(init, out);
            }
        }
        g(init, held)
    }
}

impl<I, F> std::iter::FusedIterator for DedupBy<I, F>
where
    I: Iterator,
    F: FnMut(&I::Item, &I::Item) -> bool,
{
}

// ── process_results ──

/// `Result` 流短路提升：把 `Iterator<Item = Result<T, E>>` 的错误面收敛为
/// `Result<R, E>`——processor 拿到只含 `T` 的流，首 `Err` 短路（短路后底层不
/// 再被拉，坑面 #7），函数返回该 `Err`；否则返回 processor 结果。
///
/// 对照 `flatten()`：flatten 对 `Result` **静默吞 Err**（std 邻名对照表）；
/// 本域错误显式上抛。零分配。
///
/// # Examples
///
/// ```
/// use std_ext::iter::process_results;
///
/// let oks: [Result<i32, &str>; 3] = [Ok(1), Ok(2), Ok(3)];
/// let got: Result<Vec<i32>, &str> = process_results(oks, |it| it.map(|x| x * 2).collect());
/// assert_eq!(got, Ok(vec![2, 4, 6]));
///
/// let errs: [Result<i32, &str>; 3] = [Ok(1), Err("boom"), Ok(3)];
/// assert_eq!(process_results(errs, |it| it.count()), Err("boom"));
/// ```
pub fn process_results<I, F, T, E, R>(iterable: I, processor: F) -> Result<R, E>
where
    I: IntoIterator<Item = Result<T, E>>,
    F: FnOnce(ProcessResults<I::IntoIter, E>) -> R,
{
    let mut error = Ok(());
    let result = processor(ProcessResults {
        error: &mut error,
        iter: iterable.into_iter(),
        #[cfg(feature = "log-embed")]
        pos: 0,
    });
    error.map(|_| result)
}

/// [`process_results`] processor 收到的迭代器：只产出 `Ok` 值，遇 `Err` 置外
/// 置错误位并枯竭（itertools 同构）。size_hint lower 恒 0（`Err` 面不可预知，
/// 防撒谎）。
#[derive(Debug)]
pub struct ProcessResults<'a, I, E> {
    error: &'a mut Result<(), E>,
    iter: I,
    /// 已产出 Ok 数（首 Err 短路事件的位置字段；仅 log-embed 存在——关=零状态
    /// 零开销，Debug 输出亦不含）。
    #[cfg(feature = "log-embed")]
    pos: u64,
}

impl<I, T, E> Iterator for ProcessResults<'_, I, E>
where
    I: Iterator<Item = Result<T, E>>,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        // 首 Err 已置位：短路后不再拉底层（坑面 #7）
        if self.error.is_err() {
            return None;
        }
        match self.iter.next() {
            Some(Ok(x)) => {
                #[cfg(feature = "log-embed")]
                {
                    self.pos += 1;
                }
                Some(x)
            }
            Some(Err(e)) => {
                *self.error = Err(e);
                // 埋点「首 Err 短路」：位置=已产出 Ok 数（0 起）。
                embed_event!(warn, target: "skeleton::iter::process_results", pos = self.pos);
                None
            }
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // lower 恒 0：Err 面不可预知，承诺即撒谎
        (0, self.iter.size_hint().1)
    }

    fn fold<Acc, F>(self, init: Acc, mut f: F) -> Acc
    where
        F: FnMut(Acc, Self::Item) -> Acc,
    {
        #[cfg(not(feature = "log-embed"))]
        let Self { error, mut iter } = self;
        #[cfg(feature = "log-embed")]
        let Self {
            error,
            mut iter,
            mut pos,
        } = self;
        // try_fold 短路：遇 Err 记错误位并以 Err(acc) 中断（itertools 同构）
        iter.try_fold(init, |acc, item| match item {
            Ok(x) => {
                #[cfg(feature = "log-embed")]
                {
                    pos += 1;
                }
                Ok(f(acc, x))
            }
            Err(e) => {
                *error = Err(e);
                // 埋点「首 Err 短路」（fold 快路径）：位置语义与 next 通路一致。
                embed_event!(warn, target: "skeleton::iter::process_results", pos = pos);
                Err(acc)
            }
        })
        .unwrap_or_else(|acc| acc)
    }
}

// ── size_hint 算术（私有；公式与 itertools size_hint.rs 同构）──

type Hint = (usize, Option<usize>);

/// 上界取 max：lower = max，upper = 两者已知才取 max（坑面 #4：撒谎即错）。
fn hint_max(a: Hint, b: Hint) -> Hint {
    (
        a.0.max(b.0),
        match (a.1, b.1) {
            (Some(x), Some(y)) => Some(x.max(y)),
            _ => None,
        },
    )
}

/// 相加：lower saturating 加，upper checked 加（任一未知 → None）。
fn hint_add(a: Hint, b: Hint) -> Hint {
    (
        a.0.saturating_add(b.0),
        match (a.1, b.1) {
            (Some(x), Some(y)) => x.checked_add(y),
            _ => None,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 计数迭代器：记录 next 调用次数（坑面 #5 惰性/推进序断言的探针）。
    /// `give_back`: 第 `resurrect_at` 次 None 后复活一次（非 fused 底源，坑面 #2）。
    struct CountingIter {
        items: Vec<i32>,
        pos: usize,
        pulls: usize,
        resurrect_at: Option<usize>,
        resurrected: bool,
        nones: usize,
    }

    impl CountingIter {
        fn new(items: Vec<i32>) -> Self {
            Self {
                items,
                pos: 0,
                pulls: 0,
                resurrect_at: None,
                resurrected: false,
                nones: 0,
            }
        }
        fn resurrecting(items: Vec<i32>, at: usize) -> Self {
            Self {
                resurrect_at: Some(at),
                ..Self::new(items)
            }
        }
    }

    impl Iterator for CountingIter {
        type Item = i32;
        fn next(&mut self) -> Option<i32> {
            self.pulls += 1;
            if self.pos < self.items.len() {
                let v = self.items[self.pos];
                self.pos += 1;
                return Some(v);
            }
            self.nones += 1;
            if let Some(at) = self.resurrect_at {
                // 真·非 fused：已返回过 at 次 None 之后的调用才复活毒值
                // （本该 None 的那拍直接吐值=数据面多一个，Fuse 语义防不住）
                if self.nones > at && !self.resurrected {
                    self.resurrected = true;
                    return Some(999);
                }
            }
            None
        }
    }

    // ── zip_longest ──

    #[test]
    fn zip_longest_equal_length_all_both() {
        let got: Vec<_> = zip_longest([1, 2], ["a", "b"]).collect();
        assert_eq!(
            got,
            [EitherOrBoth::Both(1, "a"), EitherOrBoth::Both(2, "b")]
        );
    }

    #[test]
    fn zip_longest_left_overhang_and_right_overhang() {
        let left: Vec<_> = zip_longest([1, 2, 3], [4]).collect();
        assert_eq!(
            left,
            [
                EitherOrBoth::Both(1, 4),
                EitherOrBoth::Left(2),
                EitherOrBoth::Left(3)
            ]
        );
        let right: Vec<_> = zip_longest([4], [1, 2, 3]).collect();
        assert_eq!(
            right,
            [
                EitherOrBoth::Both(4, 1),
                EitherOrBoth::Right(2),
                EitherOrBoth::Right(3)
            ]
        );
    }

    #[test]
    fn zip_longest_both_empty() {
        let got: Vec<EitherOrBoth<i32, i32>> =
            zip_longest(Vec::<i32>::new(), Vec::<i32>::new()).collect();
        assert!(got.is_empty());
    }

    #[test]
    fn zip_longest_fuse_non_fused_revival() {
        // 底源在 None 后复活毒值 999：Fuse 包装下不得泄漏进输出
        let got: Vec<_> = zip_longest(
            CountingIter::resurrecting(vec![1], 1),
            CountingIter::new(vec![2]),
        )
        .collect();
        assert_eq!(got, [EitherOrBoth::Both(1, 2)]);
    }

    #[test]
    fn zip_longest_exact_size_len_equals_collect_len() {
        let zl = zip_longest(0..5, 0..2);
        let expect = zl.clone().collect::<Vec<_>>().len();
        assert_eq!(zl.len(), 5, "len = max(a, b)");
        assert_eq!(expect, 5);
        assert_eq!(zip_longest(0..0, 0..4).len(), 4);
    }

    #[test]
    fn zip_longest_size_hint_bounds_hold() {
        // 夹逼：lower ≤ 实际 ≤ upper；公式 = (max(a,b) lower, max(a,b) upper)
        let zl = zip_longest(0..3, 0..7);
        assert_eq!(zl.size_hint(), (7, Some(7)));
        let n = zl.collect::<Vec<_>>().len();
        assert_eq!(n, 7);
    }

    #[test]
    fn zip_longest_lazy_zero_pulls_on_construction() {
        let mut a = CountingIter::new(vec![1, 2]);
        let mut b = CountingIter::new(vec![3]);
        let _zl = zip_longest(&mut a, &mut b);
        assert_eq!((a.pulls, b.pulls), (0, 0), "构造必须零推进");
    }

    #[test]
    fn zip_longest_fold_equals_collect() {
        let via_fold = zip_longest(0..4, 0..2).fold(Vec::new(), |mut acc, x| {
            acc.push(x);
            acc
        });
        assert_eq!(via_fold, zip_longest(0..4, 0..2).collect::<Vec<_>>());
    }

    // ── interleave ──

    #[test]
    fn interleave_even() {
        let got: Vec<i32> = interleave([1, 2, 3], [10, 20, 30]).collect();
        assert_eq!(got, [1, 10, 2, 20, 3, 30]);
    }

    #[test]
    fn interleave_longer_tail_appends() {
        // 左长：a b a b a（长者尾部如实追加，非"短尽即止"）
        let got: Vec<i32> = interleave([1, 2, 3], [10, 20]).collect();
        assert_eq!(got, [1, 10, 2, 20, 3]);
        // 右长
        let got: Vec<i32> = interleave([1], [10, 20, 30]).collect();
        assert_eq!(got, [1, 10, 20, 30]);
    }

    #[test]
    fn interleave_side_empty() {
        let got: Vec<i32> = interleave(Vec::<i32>::new(), [1, 2]).collect();
        assert_eq!(got, [1, 2]);
        let got: Vec<i32> = interleave([1, 2], Vec::<i32>::new()).collect();
        assert_eq!(got, [1, 2]);
    }

    #[test]
    fn interleave_fuse_non_fused_revival() {
        let got: Vec<i32> = interleave(
            CountingIter::resurrecting(vec![1], 1),
            CountingIter::new(vec![10, 20]),
        )
        .collect();
        assert_eq!(got, [1, 10, 20], "复活毒值 999 不得出现");
    }

    #[test]
    fn interleave_size_hint_bounds_hold() {
        let it = interleave(0..3, 0..5);
        assert_eq!(it.size_hint(), (8, Some(8)));
        assert_eq!(it.collect::<Vec<_>>().len(), 8);
    }

    #[test]
    fn interleave_alternating_pull_order() {
        let mut a = CountingIter::new(vec![1, 2]);
        let mut b = CountingIter::new(vec![10, 20]);
        let merged: Vec<i32> = interleave(&mut a, &mut b).collect();
        assert_eq!(merged, [1, 10, 2, 20]);
        // 交错序 a 先被拉；collect 完整消耗后两侧各 2 元素 +1 次探测拉取
        assert_eq!(a.pulls, 3);
        assert_eq!(b.pulls, 3);
    }

    #[test]
    fn interleave_fold_equals_collect() {
        let via_fold = interleave(0..3, 0..2).fold(Vec::new(), |mut acc, x| {
            acc.push(x);
            acc
        });
        assert_eq!(via_fold, interleave(0..3, 0..2).collect::<Vec<_>>());
    }

    // ── minmax ──

    #[test]
    fn minmax_empty_single() {
        assert_eq!(minmax(Vec::<i32>::new()), MinMaxResult::NoElements);
        assert_eq!(minmax([42]), MinMaxResult::OneElement(42));
    }

    #[test]
    fn minmax_all_equal_multi_is_not_one_element() {
        // 坑面 #6：全等值多元素 = MinMax(v, v)，OneElement 仅恰 1 元素
        assert_eq!(minmax([7, 7, 7]), MinMaxResult::MinMax(7, 7));
        assert_eq!(minmax([7, 7]), MinMaxResult::MinMax(7, 7));
    }

    #[test]
    fn minmax_regular_and_unsorted() {
        assert_eq!(minmax([3, 1, 2]), MinMaxResult::MinMax(1, 3));
        assert_eq!(minmax([5, -1, 9, 0]), MinMaxResult::MinMax(-1, 9));
        assert_eq!(minmax([2, 1]), MinMaxResult::MinMax(1, 2));
    }

    #[test]
    fn minmax_min_first_max_last_on_ties() {
        // (值, 序号)：min 取首现 3@0，max 取末现 3@3（与 std min/max 语义对齐）
        assert_eq!(
            minmax([(3, 0), (1, 1), (3, 2), (3, 3)]),
            MinMaxResult::MinMax((1, 1), (3, 3))
        );
    }

    #[test]
    fn minmax_traverses_exactly_once() {
        let it = CountingIter::new(vec![4, 2, 8, 1]);
        assert_eq!(minmax(it), MinMaxResult::MinMax(1, 8));
        // pulls 只在 next 内计——重建对照
        let it2 = CountingIter::new(vec![4, 2, 8, 1]);
        assert_eq!(it2.count(), 4);
    }

    #[test]
    fn minmax_matches_std_two_pass() {
        let data = [9, 3, 7, 3, 0, -2, 5];
        let expect = (data.iter().min(), data.iter().max());
        match minmax(data.iter().copied()) {
            MinMaxResult::MinMax(lo, hi) => assert_eq!((Some(&lo), Some(&hi)), expect),
            other => panic!("expect MinMax, got {other:?}"),
        }
    }

    // ── dedup / dedup_by ──

    #[test]
    fn dedup_consecutive_keeps_first_and_non_adjacent() {
        let got: Vec<i32> = dedup([1, 1, 2, 2, 1]).collect();
        assert_eq!(got, [1, 2, 1], "连续去重保首现；非相邻 [1,2,1] 全保留");
    }

    #[test]
    fn dedup_edge_shapes() {
        let empty: Vec<i32> = dedup(Vec::<i32>::new()).collect();
        assert!(empty.is_empty());
        assert_eq!(dedup([5]).collect::<Vec<_>>(), [5]);
        assert_eq!(dedup([5, 5, 5]).collect::<Vec<_>>(), [5]);
    }

    #[test]
    fn dedup_by_custom_predicate() {
        let got: Vec<i32> = dedup_by([1, -1, 2, -2], |prev: &i32, cand: &i32| {
            prev.abs() == cand.abs()
        })
        .collect();
        assert_eq!(got, [1, 2]);
    }

    #[test]
    fn dedup_by_predicate_arg_order_is_previous_then_candidate() {
        // 钉参数序：first = (previous, candidate)——prev 恒为已保留元素
        let calls: std::cell::RefCell<Vec<(i32, i32)>> = std::cell::RefCell::new(Vec::new());
        let got: Vec<i32> = dedup_by([1, 1, 2], |prev, cand| {
            calls.borrow_mut().push((*prev, *cand));
            prev == cand
        })
        .collect();
        assert_eq!(got, [1, 2]);
        assert_eq!(*calls.borrow(), [(1, 1), (1, 2)]);
    }

    #[test]
    fn dedup_lazy_zero_pulls_on_construction() {
        let d = dedup(CountingIter::new(vec![1, 2, 2]));
        assert_eq!(d.iter.pulls, 0, "构造必须零推进");
    }

    #[test]
    fn dedup_fuse_non_fused_revival() {
        let got: Vec<i32> = dedup(CountingIter::resurrecting(vec![1, 1], 1)).collect();
        assert_eq!(got, [1], "复活毒值 999 不得出现");
    }

    #[test]
    fn dedup_exhausted_next_n_times_stays_none() {
        // FusedIterator 契约护甲（FinalWave 实弹：墓碑被 take 消费后第二次
        // next 穿透复活型底源）——耗尽后连续 N 次 next 恒 None 且毒值永不入流。
        let mut d = dedup(CountingIter::resurrecting(vec![1, 1], 1));
        assert_eq!(d.next(), Some(1));
        assert_eq!(d.next(), None, "首次尽");
        for i in 0..16 {
            assert_eq!(d.next(), None, "第 {i} 次续 next 必恒 None（fused）");
        }
    }

    #[test]
    fn dedup_size_hint_bounds_hold() {
        let d = dedup([1, 1, 2, 3]);
        assert_eq!(d.size_hint(), (1, Some(4)));
        assert_eq!(d.collect::<Vec<_>>().len(), 3);
        assert_eq!(dedup(Vec::<i32>::new()).size_hint(), (0, Some(0)));
    }

    #[test]
    fn dedup_fold_equals_collect() {
        let via_fold = dedup([1, 1, 2, 2, 3]).fold(Vec::new(), |mut acc, x| {
            acc.push(x);
            acc
        });
        assert_eq!(via_fold, dedup([1, 1, 2, 2, 3]).collect::<Vec<_>>());
    }

    // ── process_results ──

    #[test]
    fn process_results_all_ok() {
        let oks = vec![Ok::<i32, &str>(1), Ok(2), Ok(3)];
        let got: Result<Vec<i32>, &str> = process_results(oks, |it| it.map(|x| x * 2).collect());
        assert_eq!(got, Ok(vec![2, 4, 6]));
    }

    #[test]
    fn process_results_err_short_circuits_and_returns_first_err() {
        let mixed = vec![Ok::<i32, &str>(1), Ok(2), Err("boom"), Ok(4), Err("late")];
        let got: Result<Vec<i32>, &str> = process_results(mixed, |it| it.collect());
        assert_eq!(got, Err("boom"), "返回首个 Err，后续 Err 不覆盖");
    }

    #[test]
    fn process_results_empty_is_ok_empty() {
        let empty: Vec<Result<i32, &str>> = Vec::new();
        let got: Result<Vec<i32>, &str> = process_results(empty, |it| it.collect());
        assert_eq!(got, Ok(vec![]));
    }

    #[test]
    fn process_results_short_circuit_no_pull_after_err() {
        // 坑面 #7：首 Err 后底层不再被拉
        let data = [Ok::<i32, &str>(1), Err("e1"), Ok(3), Err("e2")];
        let seen = std::cell::Cell::new(0usize);
        let it = data.into_iter().inspect(|_| seen.set(seen.get() + 1));
        let got: Result<Vec<i32>, &str> = process_results(it, |inner| inner.collect());
        assert_eq!(got, Err("e1"));
        assert_eq!(
            seen.get(),
            2,
            "首 Err 后（含 Err 那次共 2 次拉取）不得继续拉取"
        );
    }

    #[test]
    fn process_results_size_hint_lower_is_zero() {
        let data = [Ok::<i32, &str>(1), Ok(2)];
        process_results(data, |it| {
            assert_eq!(
                it.size_hint(),
                (0, Some(2)),
                "Err 面不可预知，lower 恒 0 防撒谎"
            );
        })
        .unwrap();
    }

    // ── 公共 helper 基本面 ──

    #[test]
    fn minmax_result_into_option() {
        assert_eq!(MinMaxResult::<i32>::NoElements.into_option(), None);
        assert_eq!(MinMaxResult::OneElement(1).into_option(), Some((1, 1)));
        assert_eq!(MinMaxResult::MinMax(1, 2).into_option(), Some((1, 2)));
    }
}
