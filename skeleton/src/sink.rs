//! 效果写入端口与融合写入（ADR-0011 §2.2 / issue#235 二段）。
//!
//! 给「可融合」的 reducer/fragment 一条**无中间缓冲**的写入路径：根 sink 直接落到
//! runtime 的 staged 缓冲，嵌套层只叠加静态映射适配器（不装箱、不分配、不加锁）。
//! 旧 [`Fragment`] / `combine!` **完全不动**——融合路径是 opt-in 的静态分派 API，
//! 旧实现经 [`Buffered`] 适配器进入新树（保留旧效果回调的时点）。
//!
//! **纯映射约束**（opt-in 契约，类型系统不能证明）：融合路径的映射闭包不做 IO、
//! 不写共享态、不依赖「回调在子 reduce 之后」这一时序；映射 panic 仍让根 transition
//! 整体失败，框架**不**回滚映射闭包自行产生的外部副作用。
//!
//! **对象安全**：[`EffectSink`] 带方法级泛型 ⇒ 不承诺 `dyn` 兼容；旧 `Fragment`
//! 仍可 dyn。

use crate::{Effect, EffectBuf, Fragment};

/// 效果写入端口（静态分派）。
///
/// 预算协作（隐藏面，普通 reducer 不应直接调用）：
/// - [`Self::remaining`] 是「考虑祖先活动缓冲之后」的精确可用额度（`None` = 不限，
///   `Some(0)` = 精确零额度）；
/// - [`Self::reserve_staging`] / [`Self::release_staging`] 是框架内部的额度租约协议
///   （用 [`SinkLease`] 而非手写配对）；
/// - [`Self::mark_overflowed`] 必须沿适配链**传回根**，不能只在适配层局部置位。
pub trait EffectSink<E> {
    /// 写一条效果（写入序 = 转发序）。
    fn push(&mut self, effect: E);

    /// 当前可用额度：`None` = 不限、`Some(0)` = 精确零额度。
    fn remaining(&self) -> Option<usize>;

    /// 标记「本段已溢出」——整次转换将被框架按全有或全无原子拒绝。
    fn mark_overflowed(&mut self);

    /// 预留 `want` 条额度，返回**实际借到**的条数（有限额度下 ≤ `want`）。
    fn reserve_staging(&mut self, want: usize) -> usize;

    /// 归还 `n` 条额度（与 [`Self::reserve_staging`] 配对）。
    fn release_staging(&mut self, n: usize);

    /// 批量写入：默认逐条 [`Self::push`]；根 `EffectBuf` 覆写为真正的批量路径。
    ///
    /// 实测（#237）：批量入口**没有**独立收益（逐条反而慢 ~9%），此处只提供形状，
    /// 不要把它当加速器。
    fn push_batch<I>(&mut self, effects: I)
    where
        I: IntoIterator<Item = E>,
    {
        for effect in effects {
            self.push(effect);
        }
    }
}

/// 根适配器：`EffectBuf` 本身就是根 sink（等价于设计里的 `EffectBufSink`，少一个
/// newtype）。
impl<A, Op> EffectSink<Effect<A, Op>> for EffectBuf<A, Op> {
    fn push(&mut self, effect: Effect<A, Op>) {
        EffectBuf::push(self, effect);
    }

    fn remaining(&self) -> Option<usize> {
        self.remaining_limit()
    }

    fn mark_overflowed(&mut self) {
        EffectBuf::mark_overflowed(self);
    }

    fn reserve_staging(&mut self, want: usize) -> usize {
        EffectBuf::reserve_staging(self, want)
    }

    fn release_staging(&mut self, n: usize) {
        EffectBuf::release_staging(self, n);
    }

    fn push_batch<I>(&mut self, effects: I)
    where
        I: IntoIterator<Item = Effect<A, Op>>,
    {
        EffectBuf::push_batch(self, effects);
    }
}

/// 额度租约（RAII）：从 sink 借出**全部剩余额度**，逐条归还；`Drop` 归还剩余。
///
/// 隐藏协作面的唯一正确用法——手写 `reserve_staging`/`release_staging` 配对会在
/// panic 展开时漏还，留下被永久缩小的缓冲。
pub struct SinkLease<'a, S: EffectSink<E>, E> {
    sink: &'a mut S,
    /// 尚未归还的 credits（`None` = 不限额度：租约是空操作）。
    reserved: Option<usize>,
    _e: core::marker::PhantomData<fn(E)>,
}

impl<'a, S, E> SinkLease<'a, S, E>
where
    S: EffectSink<E>,
{
    /// 借出 sink 当前的**全部**剩余额度。
    pub fn all(sink: &'a mut S) -> Self {
        let reserved = sink.remaining().map(|r| sink.reserve_staging(r));
        Self {
            sink,
            reserved,
            _e: core::marker::PhantomData,
        }
    }

    /// 借出的额度：子缓冲应以此为自己的上限（`None` = 不限、`Some(0)` = 精确零）。
    pub fn capacity(&self) -> Option<usize> {
        self.reserved
    }

    /// 子层只用了 `used` 条 ⇒ 归还多余额度。
    pub fn release_unused(&mut self, used: usize) {
        if let Some(reserved) = self.reserved {
            let extra = reserved.saturating_sub(used);
            self.sink.release_staging(extra);
            self.reserved = Some(used.min(reserved));
        }
    }

    /// 转交一条之前调用：归还一个 credit（父 sink 可写额度 +1）。
    pub fn release_one(&mut self) {
        if let Some(reserved) = self.reserved {
            self.sink.release_staging(1);
            self.reserved = Some(reserved.saturating_sub(1));
        }
    }

    /// 逐条把子缓冲转交给 sink：每 pop 一条先 [`Self::release_one`]，再调 `mapper`
    /// （mapper 拿到的是 sink 的可写借用）——fan-out 不能借用子缓冲尚未释放的槽位。
    ///
    /// 子缓冲曾溢出 ⇒ 把溢出事实传回 sink（整段由框架原子拒绝，不静默截断）。
    pub fn drain_map<CE, CO, F>(&mut self, child: &mut EffectBuf<CE, CO>, mut mapper: F)
    where
        F: FnMut(Effect<CE, CO>, &mut S),
    {
        while let Some(effect) = child.pop_front() {
            self.release_one();
            mapper(effect, self.sink);
        }
        if child.overflowed() {
            self.sink.mark_overflowed();
        }
    }

    /// 借用的 sink（调用方**已自行**归还相应 credits 时用）。
    pub fn sink(&mut self) -> &mut S {
        self.sink
    }
}

impl<S: EffectSink<E>, E> Drop for SinkLease<'_, S, E> {
    fn drop(&mut self) {
        // RAII：panic 展开也必须归还剩余 credits。
        if let Some(reserved) = self.reserved.take() {
            self.sink.release_staging(reserved);
        }
    }
}

/// 融合写入的 fragment：`reduce_into` 把效果直接写进**泛型 sink**（静态分派）。
///
/// 与旧 [`Fragment`] 的差别只有效果出参的类型：旧形态钉死 `&mut EffectBuf`，
/// 新形态接受任意 [`EffectSink`]——于是嵌套层可以只叠加静态映射、不建中间缓冲。
pub trait FragmentInto {
    /// 同 [`Fragment::State`]
    type State: Clone + Send;
    /// 同 [`Fragment::Action`]
    type Action: Send;
    /// 同 [`Fragment::Op`]
    type Op;

    /// 纯函数纪律同 [`Fragment::reduce`]（禁 IO/时钟/随机/写共享态）。
    fn reduce_into<S>(&self, state: &Self::State, action: Self::Action, out: &mut S) -> Self::State
    where
        S: EffectSink<Effect<Self::Action, Self::Op>>;
}

/// 旧 [`Fragment`] → [`FragmentInto`] 的显式适配器。
///
/// 保留旧 `reduce` 与效果回调的**时点**：先跑完子 `reduce`，再逐条转交给父 sink
/// （走「有界兼容缓冲」——子缓冲上限 = 父 sink 的剩余额度，溢出沿父链传回）。
pub struct Buffered<F> {
    inner: F,
}

impl<F> Buffered<F> {
    /// 包装一个旧 fragment / reducer。
    pub fn new(inner: F) -> Self {
        Self { inner }
    }

    /// 取回被包装的 fragment。
    pub fn into_inner(self) -> F {
        self.inner
    }
}

impl<F: Fragment> FragmentInto for Buffered<F> {
    type State = F::State;
    type Action = F::Action;
    type Op = F::Op;

    fn reduce_into<S>(&self, state: &Self::State, action: Self::Action, out: &mut S) -> Self::State
    where
        S: EffectSink<Effect<Self::Action, Self::Op>>,
    {
        let mut lease = SinkLease::all(out);
        let mut child: EffectBuf<Self::Action, Self::Op> = EffectBuf::new();
        child.set_staging_cap(lease.capacity());
        let next = self.inner.reduce(state, action, &mut child);
        lease.release_unused(child.len());
        lease.drain_map(&mut child, |effect, sink| sink.push(effect));
        next
    }
}
