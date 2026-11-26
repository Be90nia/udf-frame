#![cfg(feature = "compose")]
//! #163 方案 3 验收：效果 ID 命名空间「显式声明 + 装配期校验」。
//!
//! 判别力（每条断言答得出「什么变异下会红」）：
//! - 不相交声明通过 —— 校验改成恒 `Err` 则红
//! - 相交 / 同起点 / 一端包含（嵌套组合形态）被拒 —— 删掉重叠判定则红
//! - `lo > hi` 被拒 —— 删掉倒置判定则红（倒置区间会让重叠检查对全部 id 静默失效）
//! - 空声明通过 —— 声明面被改成必填则红（不写 `namespaces:` 的装配用例编译失败）
//! - `Display` 含两名 + 区间 + 重叠 id —— 文案丢字段则红
//! - 不写 `namespaces:` 的 `combine!` 照常装配并运行 —— 宏臂掰成必填则红

use skeleton::{
    EffectBuf, EffectId, EffectNamespace, EffectNamespaceConflict, Fragment, Infallible, Reducer,
    validate_effect_namespaces,
};

#[derive(Clone, Debug, PartialEq)]
struct Parent {
    timer: u32,
    http: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum PAction {
    Timer(u32),
    Http(u32),
}

/// 零效果子件（`Op = Infallible`）：组合面语义验证不需要解释器。
struct Counter;

impl Reducer for Counter {
    type State = u32;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
        s + a
    }
}

/// 声明式装配（#163 新增面）。
fn declared() -> impl Fragment<State = Parent, Action = PAction, Op = Infallible> {
    skeleton_infra::combine!(
        Counter,
        Parent.timer,
        wrap: PAction::Timer,
        unwrap: |p: &PAction| match p {
            PAction::Timer(c) => Some(*c),
            _ => None,
        },
        lift: |_, _| {},
        namespaces: [("timer", 0, 999), ("http", 1000, 1999)],
    )
}

/// 未声明装配（#163 之前的写法，一字不动）。
fn undeclared() -> impl Fragment<State = Parent, Action = PAction, Op = Infallible> {
    skeleton_infra::combine!(
        Counter,
        Parent.timer,
        wrap: PAction::Timer,
        unwrap: |p: &PAction| match p {
            PAction::Timer(c) => Some(*c),
            _ => None,
        },
        lift: |_, _| {},
    )
}

fn drive(
    frag: &impl Fragment<State = Parent, Action = PAction, Op = Infallible>,
    action: PAction,
    state: Parent,
) -> Parent {
    let mut buf = EffectBuf::new();
    let next = frag.reduce(&state, action, &mut buf);
    assert!(buf.is_empty(), "零效果子件不得产出效果");
    next
}

#[test]
fn declared_disjoint_ranges_pass() {
    let decls = [
        EffectNamespace::new("timer", 0, 999),
        EffectNamespace::new("http", 1000, 1999),
    ];
    assert_eq!(validate_effect_namespaces(&decls), Ok(()));
}

#[test]
fn adjacent_bounds_are_not_an_overlap() {
    // 闭区间：999 与 1000 不共享 id ⇒ 相邻合法（区间恰好相接不是冲突）
    let decls = [
        EffectNamespace::new("a", 0, 999),
        EffectNamespace::new("b", 1000, 1000),
    ];
    assert_eq!(validate_effect_namespaces(&decls), Ok(()));
}

#[test]
fn overlapping_ranges_rejected() {
    let cases = [
        // 两个子件都从 0 计数（issue 正文的原始故障形态）
        (
            EffectNamespace::new("timer", 0, 999),
            EffectNamespace::new("http", 0, 999),
            0u64,
        ),
        // 部分相交
        (
            EffectNamespace::new("timer", 0, 999),
            EffectNamespace::new("http", 500, 1499),
            500u64,
        ),
        // 一端完全包含另一端（嵌套组合 A ⊃ B 的形态）
        (
            EffectNamespace::new("outer", 0, 999),
            EffectNamespace::new("inner", 100, 199),
            100u64,
        ),
    ];
    for (left, right, at) in cases {
        let err = validate_effect_namespaces(&[left, right]).expect_err("区间相交必须被拒绝");
        assert_eq!(
            err,
            EffectNamespaceConflict::Overlap { left, right, at },
            "冲突必须按声明序报出（left 在前）并给出首个重叠 id"
        );
    }
}

#[test]
fn inverted_range_rejected() {
    let err = validate_effect_namespaces(&[EffectNamespace::new("timer", 10, 3)])
        .expect_err("lo > hi 必须被拒绝（空区间会让重叠检查静默失效）");
    assert_eq!(
        err,
        EffectNamespaceConflict::Inverted {
            name: "timer",
            lo: 10,
            hi: 3
        }
    );
}

#[test]
fn empty_declaration_is_no_validation() {
    assert_eq!(validate_effect_namespaces(&[]), Ok(()));
}

#[test]
fn run_and_cancel_span_one_namespace() {
    // Run 与取消它的 Cancel 共用同一 id 空间 ⇒ 每子件只声明一次区间：单条声明覆盖
    // 两种效果，不按种类拆分。
    let timer = EffectNamespace::new("timer", 0, 999);
    assert_eq!(validate_effect_namespaces(&[timer]), Ok(()));
    // 两个子件都声明同一区间 = 对方的 Cancel(id) 会寻址到本子件的 Run(id)
    let http = EffectNamespace::new("http", 0, 999);
    assert_eq!(
        validate_effect_namespaces(&[timer, http]),
        Err(EffectNamespaceConflict::Overlap {
            left: timer,
            right: http,
            at: 0
        })
    );
    assert_eq!(EffectId::new(5), EffectId::new(5));
}

#[test]
fn display_text_is_actionable() {
    let overlap = validate_effect_namespaces(&[
        EffectNamespace::new("timer", 0, 999),
        EffectNamespace::new("http", 500, 1499),
    ])
    .expect_err("相交");
    let text = overlap.to_string();
    for needle in ["timer", "http", "0..=999", "500..=1499", "share id 500"] {
        assert!(text.contains(needle), "Display 缺 `{needle}`：{text}");
    }

    let inverted =
        validate_effect_namespaces(&[EffectNamespace::new("timer", 10, 3)]).expect_err("倒置");
    let text = inverted.to_string();
    for needle in ["timer", "10..=3", "lo > hi"] {
        assert!(text.contains(needle), "Display 缺 `{needle}`：{text}");
    }

    assert!(std::error::Error::source(&overlap).is_none());
}

#[test]
fn undeclared_combine_still_assembles_and_runs() {
    // 不声明 = 不校验 = 现状：旧写法装配与运行行为不变
    assert_eq!(
        drive(
            &undeclared(),
            PAction::Timer(2),
            Parent { timer: 0, http: 0 }
        ),
        Parent { timer: 2, http: 0 }
    );
}

#[test]
fn declared_combine_assembles_and_runs() {
    // 声明只加装配期校验，不碰路由/状态语义
    let frag = declared();
    assert_eq!(
        drive(&frag, PAction::Timer(2), Parent { timer: 0, http: 0 }),
        Parent { timer: 2, http: 0 }
    );
    // 不路由到本分片的 action 仍走 miss：状态原样、零效果（miss 语义未被声明触碰）
    assert_eq!(
        drive(&frag, PAction::Http(1), Parent { timer: 2, http: 0 }),
        Parent { timer: 2, http: 0 }
    );
}
