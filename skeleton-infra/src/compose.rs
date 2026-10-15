//! 组合域：父子分片组合器（协作 = 显式结构，字符串路由组合的封死形态）。
//!
//! 契约层的组合概念只有一句——「Feature 以显式委托组合：父 reduce 内调子
//! reduce + 镜头闭包」（[`Reducer`](skeleton::Reducer) 的自然用法）；本域是把六参样板
//! 收口的便利件。`combine!` 宏（从切片表达式生成六闭包）后续批次落地。

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
    Combine { get, set, wrap, unwrap, child, lift_fx, _slices: PhantomData }
}

/// [`combine`] 产物（具名类型，字段私有、只经 [`combine`] 构造）：镜头/路由
/// 闭包 + 子 reducer + 效果上提闭包，零 dyn 零装箱；六个状态/词表参数与六个
/// 闭包参数经泛型记名——采用方可用 `Combine<_, _, …>` 注解或别名持有产物，
/// 匿名 `impl Reducer` 形态做不到。
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
        let child_next = self.child.reduce((self.get)(state), child_action, &mut child_out);
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
