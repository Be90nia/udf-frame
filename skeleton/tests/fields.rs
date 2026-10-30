//! `skeleton::fields` 值语义钉子：模块 armor 逐条对表（插入序 / 原地覆盖 / merge
//! 语义 / COW 隔离 / NaN 行为 / 线性查找）。

use skeleton::fields::{FieldBag, FieldValue};
use std::time::Duration;

/// 两字段 bag（"a"=Int / "b"=Count），多数用例的地基。
fn sample() -> FieldBag {
    let mut bag = FieldBag::new();
    bag.insert("a", FieldValue::Int(1));
    bag.insert("b", FieldValue::Count(2));
    bag
}

/// 空 bag：`new` 与 `Default` 同一值、零字段、迭代为空。
#[test]
fn new_and_default_are_equivalent_and_empty() {
    let bag = FieldBag::new();
    assert_eq!(bag.len(), 0);
    assert!(bag.is_empty());
    assert_eq!(bag.iter().next(), None);
    assert_eq!(bag.get("anything"), None);
    assert_eq!(bag, FieldBag::default());
    assert_eq!(FieldBag::default(), FieldBag::new());
}

/// 同名覆盖（armor 覆盖条）：last-wins——值换、位置不动；构造即去重，长度不变。
#[test]
fn same_name_overwrites_value_in_place() {
    let mut bag = FieldBag::new();
    bag.insert("a", FieldValue::Int(1));
    bag.insert("b", FieldValue::Int(2));
    bag.insert("a", FieldValue::Int(3));
    assert_eq!(
        bag.iter().collect::<Vec<_>>(),
        [("a", &FieldValue::Int(3)), ("b", &FieldValue::Int(2))]
    );
    assert_eq!(bag.len(), 2);
    assert_eq!(bag.get("a"), Some(&FieldValue::Int(3)));
}

/// merge（armor 覆盖条）：已有键覆盖（位置不动），新键按 other 序追加；other
/// 不被改动；重复 merge 幂等（覆盖不追加）。
#[test]
fn merge_overwrites_existing_and_appends_new_in_other_order() {
    let mut base = sample(); // [a=1, b=2]
    let mut patch = FieldBag::new();
    patch.insert("c", FieldValue::Bool(true));
    patch.insert("b", FieldValue::Count(9));
    base.merge(&patch);
    assert_eq!(
        base.iter().collect::<Vec<_>>(),
        [
            ("a", &FieldValue::Int(1)),
            ("b", &FieldValue::Count(9)),
            ("c", &FieldValue::Bool(true)),
        ]
    );
    assert_eq!(patch.len(), 2);
    assert_eq!(patch.get("b"), Some(&FieldValue::Count(9)));
    base.merge(&patch);
    assert_eq!(base.len(), 3); // 幂等：第二次 merge 只覆盖不追加
}

/// clone 隔离（armor COW 条）：clone 后改原体（覆盖 + 追加），clone 见旧值旧长度；
/// 原体自身改写照常生效。
#[test]
fn clone_is_isolated_from_later_writes() {
    let original = sample();
    let snapshot = original.clone();
    let mut original = original;
    original.insert("a", FieldValue::Int(99));
    original.insert("c", FieldValue::Bool(true));
    assert_eq!(snapshot.get("a"), Some(&FieldValue::Int(1)));
    assert_eq!(snapshot.get("c"), None);
    assert_eq!(snapshot.len(), 2);
    assert_eq!(
        snapshot.iter().collect::<Vec<_>>(),
        [("a", &FieldValue::Int(1)), ("b", &FieldValue::Count(2))]
    );
    assert_eq!(original.get("a"), Some(&FieldValue::Int(99)));
    assert_eq!(original.len(), 3);
}

/// 插入序 = 迭代序（armor 顺序条）：重复迭代同序；覆盖不改序。
#[test]
fn iter_order_is_insertion_order_and_stable() {
    let mut bag = FieldBag::new();
    for (name, value) in [
        ("first", FieldValue::Int(1)),
        ("second", FieldValue::Int(2)),
        ("third", FieldValue::Int(3)),
        ("fourth", FieldValue::Int(4)),
    ] {
        bag.insert(name, value);
    }
    let once: Vec<&str> = bag.iter().map(|(name, _)| name).collect();
    let twice: Vec<&str> = bag.iter().map(|(name, _)| name).collect();
    assert_eq!(once, ["first", "second", "third", "fourth"]);
    assert_eq!(once, twice);
    bag.insert("second", FieldValue::Int(20));
    assert_eq!(
        bag.iter().map(|(name, _)| name).collect::<Vec<_>>(),
        ["first", "second", "third", "fourth"]
    );
}

/// get（armor O(n) 条）：命中、未命中、整名精确匹配（无前缀语义）。
#[test]
fn get_hits_and_misses() {
    let bag = sample();
    assert_eq!(bag.get("a"), Some(&FieldValue::Int(1)));
    assert_eq!(bag.get("b"), Some(&FieldValue::Count(2)));
    assert_eq!(bag.get("missing"), None);
    assert_eq!(bag.get(""), None);
    assert_eq!(bag.get("prefix"), None);
}

/// len / is_empty 随尺寸走：覆盖不改长度，追加才改。
#[test]
fn len_and_is_empty_track_size() {
    let mut bag = FieldBag::new();
    assert!(bag.is_empty());
    assert_eq!(bag.len(), 0);
    bag.insert("a", FieldValue::Int(1));
    assert!(!bag.is_empty());
    assert_eq!(bag.len(), 1);
    bag.insert("a", FieldValue::Int(2));
    assert_eq!(bag.len(), 1);
    bag.insert("b", FieldValue::Int(3));
    assert_eq!(bag.len(), 2);
}

/// 全变体：构造、Clone+PartialEq 往返、跨变体不等（Int≠Count）、NaN 文档行为
/// （armor 浮点条：NaN≠NaN → 含 NaN 的两 bag 恒不相等）。
#[test]
fn value_variants_construct_and_compare() {
    let variants = [
        FieldValue::Int(-7),
        FieldValue::Float(1.5),
        FieldValue::Bool(true),
        FieldValue::Str(String::from("v")),
        FieldValue::Duration(Duration::from_millis(12)),
        FieldValue::Count(7),
    ];
    for value in &variants {
        assert_eq!(&value.clone(), value);
    }
    assert_eq!(FieldValue::Int(-7), FieldValue::Int(-7));
    assert_ne!(FieldValue::Int(1), FieldValue::Count(1));

    let nan = FieldValue::Float(f64::NAN);
    assert_ne!(nan, nan);
    let mut left = FieldBag::new();
    let mut right = FieldBag::new();
    left.insert("f", FieldValue::Float(f64::NAN));
    right.insert("f", FieldValue::Float(f64::NAN));
    assert_ne!(left, right);
}
