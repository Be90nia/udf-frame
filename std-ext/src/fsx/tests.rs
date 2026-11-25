//! fsx 域纯逻辑单测（零 IO：IO 面在 tests/fsx.rs——CARGO_TARGET_TMPDIR 仅
//! 集成测试目标可用，且禁系统 TEMP 红线禁用 env::temp_dir）。
//!
//! 例外：Windows 只读装甲 `clear_readonly_tree` 是私有件，且 std 1.96 的
//! `remove_dir_all` 自带只读重试 ⇒ 公开 API 面已不可达，只能在 crate 内直测
//! （落点仍守产物红线：crate 的 gitignored `target/`）。

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

/// 深链只读树：迭代式遍历必须清掉**每一层**的只读位（无自调用=每层不占调用
/// 帧），首个错误原样上浮。
#[cfg(windows)]
#[test]
fn clear_readonly_tree_clears_deep_chain_iteratively() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("fsx-ro-unit-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let tree = root.join("tree");
    let mut deep = tree.clone();
    fs::create_dir_all(&deep).unwrap();
    for i in 0..64 {
        deep.push(format!("d{i:02}"));
        fs::create_dir(&deep).unwrap();
    }
    fs::write(deep.join("leaf.txt"), b"x").unwrap();

    let set_ro = |p: &Path| {
        let mut perm = fs::metadata(p).unwrap().permissions();
        perm.set_readonly(true);
        fs::set_permissions(p, perm).unwrap();
        assert!(fs::metadata(p).unwrap().permissions().readonly());
    };
    set_ro(&deep.join("leaf.txt"));
    let mut cur = deep.clone();
    loop {
        set_ro(&cur);
        if cur == tree {
            break;
        }
        cur = cur.parent().unwrap().to_path_buf();
    }

    clear_readonly_tree(&tree).unwrap();
    assert!(!fs::metadata(&deep).unwrap().permissions().readonly());
    assert!(
        !fs::metadata(tree.join("d00"))
            .unwrap()
            .permissions()
            .readonly()
    );
    assert!(
        !fs::metadata(deep.join("leaf.txt"))
            .unwrap()
            .permissions()
            .readonly()
    );

    // 首个元数据错误不吞（fail-visible）
    let err = clear_readonly_tree(&tree.join("missing")).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::NotFound);

    fs::remove_dir_all(&root).unwrap();
}
