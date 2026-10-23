//! 组合域：父子分片组合器（协作 = 显式结构，字符串路由组合的封死形态）。
//!
//! 契约层的组合概念只有一句——「Feature 以显式委托组合：父 reduce 内调子
//! reduce + 镜头闭包」（[`Reducer`] 的自然用法）；本域是把六参样板收口的
//! 便利件：[`combine`] 函数（12 泛型参具名形态）+ [`combine()`] 宏（从切片
//! 表达式生成六闭包，见下）。

use std::marker::PhantomData;

use skeleton::{Effect, EffectBuf, Reducer};

/// 组合器：父子分片，协作=显式结构（字符串路由组合的封死形态）。
///
/// - `get`/`set`：父状态内子切片的读写镜头
/// - `wrap`/`unwrap`：子 action 上浮 / 父 action 下沉路由；`unwrap` 返回
///   `None` = 本层不路由，状态克隆原样返回、零效果（父层分支自理）。
///   子效果里的 `Emit(CA)` 由核心经 `wrap` 自动上浮为 `Emit(PA)`——`lift_fx`
///   只会收到 `Run`/`Cancel` 两形（词表上提与 action 上浮分工，参数零死位）
/// - `lift_fx`：子效果词表到父词表的显式上提（同形出参：写父 `out`）
///
/// # Examples
///
/// 父子分片最小例（零效果词表，`lift_fx` 无可上提即空转）：子切片承载计数，
/// 子 action 经 `wrap`/`unwrap` 上浮/下沉路由；根状态由父 Feature 的
/// `initial` 显式构造（含子初值），路由分支整条委托分片（完整含效果词表形态见
/// `tests/combine_teststore.rs`）：
///
/// ```
/// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
/// use skeleton_infra::compose::combine;
/// use skeleton_infra::runtime::SyncStore;
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct Parent { total: u32, child: u32 }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum PAction { Child(CAction), Touch }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum CAction { Add(u32) }
///
/// struct ChildCounter;
///
/// impl Reducer for ChildCounter {
///     type State = u32;
///     type Action = CAction;
///     type Op = Infallible;
///     fn initial(&self) -> u32 { 0 }
///     fn reduce(&self, s: &u32, a: CAction, _out: &mut EffectBuf<CAction, Infallible>) -> u32 {
///         match a { CAction::Add(n) => s + n }
///     }
/// }
///
/// /// 子切片分片：get/set 镜头 + wrap/unwrap 路由 + lift_fx 效果上提（12 参具名）
/// fn counter_fragment() -> impl Reducer<State = Parent, Action = PAction, Op = Infallible> {
///     combine(
///         |p: &Parent| &p.child,                 // get：子切片读镜头
///         |p: &mut Parent, c: u32| p.child = c,  // set：子切片写回
///         PAction::Child,                        // wrap：子 action 上浮
///         |p: &PAction| match p {                // unwrap：父 action 下沉路由
///             PAction::Child(c) => Some(c.clone()),
///             PAction::Touch => None,
///         },
///         ChildCounter,                          // child：子 reducer
///         |_, _| {},                             // lift_fx：子零效果，无可上提
///     )
/// }
///
/// struct ParentFeature;
///
/// impl Reducer for ParentFeature {
///     type State = Parent;
///     type Action = PAction;
///     type Op = Infallible;
///     fn initial(&self) -> Parent { Parent { total: 100, child: 0 } }
///     fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
///         match a {
///             // 路由命中的子 action：整条委托分片（子迁移+子切片写回）
///             PAction::Child(_) => counter_fragment().reduce(s, a, out),
///             PAction::Touch => Parent { total: s.total + 1, child: s.child },
///         }
///     }
/// }
///
/// let store = SyncStore::new(ParentFeature, NoFx);
/// store.dispatch(PAction::Child(CAction::Add(2))); // wrap 下沉 → 子切片迁移
/// store.dispatch(PAction::Child(CAction::Add(3))); // → 5
/// store.dispatch(PAction::Touch);                  // 父自有分支，子切片不动
/// assert_eq!(*store.snapshot(), Parent { total: 101, child: 5 });
/// store.close();
/// ```
///
/// 产物是**状态分片不是根**：签名无父状态工厂，`initial` 恒 panic——根状态
/// 由父 Feature 的 `initial` 显式构造（含子切片初值），分片只承载 reduce
/// 委托；用作根 = 组合根误用，fail-closed 显式崩溃（禁静默空状态，
/// 亦不以 `PS: Default` 界逼「非法状态进类型」的 State 写退化初值）。
pub fn combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>(
    get: G,
    set: Se,
    wrap: W,
    unwrap: U,
    child: C,
    lift_fx: L,
) -> Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>
where
    PS: Clone + Send,
    PA: Send,
    G: Fn(&PS) -> &CS + 'static,
    Se: Fn(&mut PS, CS) + 'static,
    W: Fn(CA) -> PA + 'static,
    U: Fn(&PA) -> Option<CA> + 'static,
    C: Reducer<State = CS, Action = CA, Op = CO>,
    L: Fn(Effect<CA, CO>, &mut EffectBuf<PA, PO>) + 'static,
{
    Combine {
        get,
        set,
        wrap,
        unwrap,
        child,
        lift_fx,
        _slices: PhantomData,
    }
}

/// 组合宏：从切片表达式生成六闭包，收口 [`combine`] 的 12 泛型参手写。
///
/// 两形态：
///
/// - **变体短形态**：`枚举::变体` 一体生成 wrap/unwrap（unwrap 自 `&PA` 克隆
///   上浮，要求子 action `Clone`）；`lift` 省略 = 零效果上提——零效果词表
///   （`Infallible`）用短形态正合适；非零效果词表省略 `lift` = 子效果静默
///   丢弃，组合根必须自知（显式 `|_, _| {}` 同义，本宏不另设闸）。
/// - **显式形态**：`wrap:`/`unwrap:`/`lift:` 逐项显式给——非 Clone 词表与
///   复杂路由的逃生口，语义与 [`combine`] 函数逐参对应。
///
/// 切片表达式限 `状态类型.字段名` 裸段（宏字面限制：泛型/路径状态退回
/// [`combine`] 显式形态）。产物 `Op` 由采用方返回类型注解钉住（与
/// [`combine`] 同）；子切片类型与子 reducer 的 `State` 不匹配 = 装配点编译错。
///
/// # Examples
///
/// 变体短形态，组合面一行（完整分片语义与显式形态见 [`combine`] 的
/// 父子分片例与 `tests/combine_teststore.rs`）：
///
/// ```
/// use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
/// use skeleton_infra::runtime::SyncStore;
///
/// #[derive(Clone, Debug, PartialEq)]
/// struct Parent { total: u32, child: u32 }
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum PAction { Child(u32), Touch }
///
/// struct ChildCounter;
///
/// impl Reducer for ChildCounter {
///     type State = u32;
///     type Action = u32;
///     type Op = Infallible;
///     fn initial(&self) -> u32 { 0 }
///     fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
///         s + a
///     }
/// }
///
/// fn counter_fragment() -> impl Reducer<State = Parent, Action = PAction, Op = Infallible> {
///     skeleton_infra::combine!(ChildCounter, Parent.child, PAction::Child)
/// }
///
/// struct ParentFeature;
///
/// impl Reducer for ParentFeature {
///     type State = Parent;
///     type Action = PAction;
///     type Op = Infallible;
///     fn initial(&self) -> Parent { Parent { total: 100, child: 0 } }
///     fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
///         match a {
///             PAction::Child(_) => counter_fragment().reduce(s, a, out),
///             PAction::Touch => Parent { total: s.total + 1, child: s.child },
///         }
///     }
/// }
///
/// let store = SyncStore::new(ParentFeature, NoFx);
/// store.dispatch(PAction::Child(2)); // unwrap 路由 → 子迁移 → set 写回切片
/// store.dispatch(PAction::Touch);    // 未路由：本层自理，子切片不动
/// assert_eq!(*store.snapshot(), Parent { total: 101, child: 2 });
/// store.close();
/// ```
#[macro_export]
macro_rules! combine {
    // 变体短形态：wrap/unwrap 由变体构造子一体生成（unwrap 克隆上浮）；
    // lift 省略 = 零效果上提（委托显式无 lift 臂）
    ($child:expr, $state:ident . $field:ident, $pa:ident :: $variant:ident
        $(, lift: $lift:expr $(,)?)?) => {
        $crate::combine!(
            $child,
            $state . $field,
            wrap: $pa :: $variant,
            unwrap: |p: &$pa| match p {
                $pa :: $variant(c) => Some(c.clone()),
                _ => None,
            },
            $(lift: $lift,)?
        )
    };
    // 显式形态（无 lift）：六闭包全生成/收编，lift 固定零效果上提
    ($child:expr, $state:ident . $field:ident,
        wrap: $wrap:expr, unwrap: $unwrap:expr $(,)?) => {
        $crate::compose::combine(
            |p: &$state| &p.$field,           // get：子切片读镜头
            |p: &mut $state, c| p.$field = c, // set：子切片写回（c 类型由赋值位钉死）
            $wrap,
            $unwrap,
            $child,
            |_, _| {},
        )
    };
    // 显式形态（有 lift）：与 combine 函数逐参对应
    ($child:expr, $state:ident . $field:ident,
        wrap: $wrap:expr, unwrap: $unwrap:expr, lift: $lift:expr $(,)?) => {
        $crate::compose::combine(
            |p: &$state| &p.$field,
            |p: &mut $state, c| p.$field = c,
            $wrap,
            $unwrap,
            $child,
            $lift,
        )
    };
}

/// [`combine`] 产物（具名类型，字段私有、只经 [`combine`] 构造）：镜头/路由
/// 闭包 + 子 reducer + 效果上提闭包，零 dyn 零装箱；六个状态/词表参数与六个
/// 闭包参数经泛型记名——采用方可用 `Combine<_, _, …>` 注解或别名持有产物，
/// 匿名 `impl Reducer` 形态做不到。
///
/// 性能锚（2026-10-16 批 C 定性，release/std Instant 粗测，详见
/// reports/2026-10-16-perf-batchc.md）：组合税个位数 ns——combine! 装配 vs
/// 等义手装单 Feature 的 dispatch 全含差值五轮交错中位 ~0-2ns（带 ±5ns）；
/// 裸 reduce 两臂同折叠至 ~1ns 不可分辨（闭包全 ZST 逐字内联、子
/// `EffectBuf` 空态零分配）——「零 dyn 零装箱」零额外分配实证。
pub struct Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L> {
    get: G,
    set: Se,
    wrap: W,
    unwrap: U,
    child: C,
    lift_fx: L,
    _slices: PhantomData<(PS, PA, PO, CS, CA, CO)>,
}

impl<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L> Reducer
    for Combine<PS, PA, PO, CS, CA, CO, G, Se, W, U, C, L>
where
    G: Fn(&PS) -> &CS,
    Se: Fn(&mut PS, CS),
    W: Fn(CA) -> PA,
    U: Fn(&PA) -> Option<CA>,
    C: Reducer<State = CS, Action = CA, Op = CO>,
    L: Fn(Effect<CA, CO>, &mut EffectBuf<PA, PO>),
    PS: Clone + Send,
    PA: Send,
{
    type State = PS;
    type Action = PA;
    type Op = PO;

    // `#[track_caller]`：与 trait 声明同标——误用 panic 位置指采用方装配行。
    #[track_caller]
    fn initial(&self) -> PS {
        panic!(
            "combine 产物是状态分片不是根：根状态由父 Feature 的 initial 显式构造（含子切片初值），分片只承载 reduce 委托"
        );
    }

    fn reduce(&self, state: &PS, action: PA, out: &mut EffectBuf<PA, PO>) -> PS {
        let Some(child_action) = (self.unwrap)(&action) else {
            return state.clone();
        };
        // 子效果缓冲：Vec::new 空态零分配，子 action 实际产效果时才分配一次
        // （稳态零新增分配纪律约束的是 store 效果转发路径，见 EffectBuf 头注；
        // reducer 禁写共享态——本地缓冲进出即弃，不违纯函数契约）
        let mut child_out = EffectBuf::new();
        let child_next = self
            .child
            .reduce((self.get)(state), child_action, &mut child_out);
        while let Some(effect) = child_out.pop_front() {
            match effect {
                // Emit 回灌是子 action 上浮（wrap 的职责），核心代 lifted；
                // Run/Cancel 交 lift_fx 显式上提词表（lift_fx 只会收到这两形）
                Effect::Emit(a) => out.push(Effect::Emit((self.wrap)(a))),
                effect => (self.lift_fx)(effect, out),
            }
        }
        let mut next = state.clone();
        (self.set)(&mut next, child_next);
        next
    }
}
