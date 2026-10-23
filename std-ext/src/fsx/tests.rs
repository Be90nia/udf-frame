//! fsx 域纯逻辑单测（零 IO：IO 面在 tests/fsx.rs——CARGO_TARGET_TMPDIR 仅
//! 集成测试目标可用，且禁系统 TEMP 红线禁用 env::temp_dir）。

use super::*;

#[test]
fn unique_temp_name_shape_and_uniqueness() {
    let a = unique_temp_name("t-");
    let b = unique_temp_name("t-");
    // 前缀保序；pid 段在名内；两段分隔符（pid/序号/纳秒三段 hex）
    assert!(a.starts_with("t-"));
    assert!(a.contains(std::process::id().to_string().as_str()));
    assert_eq!(a.matches('-').count(), 2);
    // 同 pid 并发竞态根除面：进程内序号恒新
    assert_ne!(a, b);
}

#[test]
fn walk_builder_defaults_and_setters() {
    let w = walk("x");
    assert_eq!(w.max_depth, usize::MAX);
    assert_eq!(w.min_depth, 0);
    assert!(!w.follow);
    assert_eq!(w.sort, Sort::None);
    let w = w
        .max_depth(3)
        .min_depth(1)
        .follow_links(true)
        .sort(Sort::ByName);
    assert_eq!(w.max_depth, 3);
    assert_eq!(w.min_depth, 1);
    assert!(w.follow);
    assert_eq!(w.sort, Sort::ByName);
}
