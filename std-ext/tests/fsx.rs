//! fsx 域集成测试（IO 面）：walk 惰性遍历（深度/排序/symlink 策略+环检测）/
//! copy_tree（中间态显式）/rm_all（Windows 只读装甲）/temp_dir_in（RAII+并发
//! 唯一）。
//!
//! 临时目录守卫=本域 [`temp_dir_in`] 自产自销（dogfood：域件即测试基建），
//! 落点 `CARGO_TARGET_TMPDIR`（工作区 target/，禁系统 TEMP 红线），Drop 兜底
//! +关键用例显式 close 断言清理成功。symlink 面无特权环境自动跳过（打印
//! skip 证据，Windows 开发者模式/Unix 原生可用）。

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;

use std_ext::fsx::{
    atomic_write, copy_tree, rm_all, temp_dir_in, walk, CopyError, CopyReport, Sort, SymlinkPolicy,
    TempDir, WalkError,
};

// ── 守卫与助手 ──

/// 工作区内临时根（dogfood temp_dir_in；清理=TempDir 自带 Drop 兜底，守卫
/// 禁再手动删树——双删只会产出 rm_all NotFound 噪音）。
struct Guard(TempDir);

fn guard(tag: &str) -> Guard {
    Guard(
        temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("fsx-{tag}-"))
            .expect("temp root under CARGO_TARGET_TMPDIR"),
    )
}

impl Guard {
    fn path(&self) -> &Path {
        self.0.path()
    }
}

fn write_file(path: impl AsRef<Path>, contents: &[u8]) {
    fs::write(path, contents).unwrap();
}

/// 断言目录已空（自清理纪律的可观测面）。
fn assert_dir_empty(path: &Path) {
    let entries: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(entries.is_empty(), "{path:?} not empty: {entries:?}");
}

/// 建目录符号链接，返回是否成功（无特权环境调用方跳过并打印证据）。
fn try_dir_link(link: impl AsRef<Path>, target: impl AsRef<Path>) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target.as_ref(), link.as_ref()).is_ok()
    }
    #[cfg(windows)]
    {
        let ok = std::os::windows::fs::symlink_dir(target.as_ref(), link.as_ref()).is_ok();
        if !ok {
            eprintln!("SKIP（无 symlink 特权）: dir link {:?}", link.as_ref());
        }
        ok
    }
}

/// 建文件符号链接（同上）。
fn try_file_link(link: impl AsRef<Path>, target: impl AsRef<Path>) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(target.as_ref(), link.as_ref()).is_ok()
    }
    #[cfg(windows)]
    {
        let ok = std::os::windows::fs::symlink_file(target.as_ref(), link.as_ref()).is_ok();
        if !ok {
            eprintln!("SKIP（无 symlink 特权）: file link {:?}", link.as_ref());
        }
        ok
    }
}

/// 收集遍历结果为 (深度, 文件名) 序列。
fn walk_names(w: std_ext::fsx::Walk) -> Vec<(usize, String)> {
    w.map(|e| {
        let e = e.unwrap();
        (
            e.depth(),
            e.path().file_name().unwrap().to_string_lossy().into_owned(),
        )
    })
    .collect()
}

// ── walk：结构/深度/排序 ──

#[test]
fn walk_yields_root_then_children_with_depth() {
    let g = guard("struct");
    fs::create_dir(g.path().join("sub")).unwrap();
    write_file(g.path().join("sub/b.txt"), b"b");
    write_file(g.path().join("a.txt"), b"a");

    let entries = walk_names(walk(g.path()));
    assert_eq!(
        entries,
        vec![
            (
                0,
                g.path().file_name().unwrap().to_string_lossy().into_owned()
            ),
            (1, "a.txt".into()),
            (1, "sub".into()),
            (2, "b.txt".into()),
        ]
    );
}

#[test]
fn walk_max_depth_zero_yields_root_only() {
    let g = guard("maxd");
    fs::create_dir(g.path().join("sub")).unwrap();
    write_file(g.path().join("sub/x"), b"x");
    let entries = walk_names(walk(g.path()).max_depth(0));
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, 0);
}

#[test]
fn walk_min_depth_skips_root() {
    let g = guard("mind");
    write_file(g.path().join("x"), b"x");
    let entries = walk_names(walk(g.path()).min_depth(1));
    assert_eq!(entries, vec![(1, "x".to_string())]);
}

#[test]
fn walk_file_root_yields_single_entry() {
    let g = guard("fileroot");
    write_file(g.path().join("f.txt"), b"f");
    let entries = walk_names(walk(g.path().join("f.txt")));
    assert_eq!(entries, vec![(0, "f.txt".to_string())]);
}

#[test]
fn walk_sort_by_name_orders_each_dir() {
    let g = guard("sort");
    for n in ["c", "a", "b"] {
        write_file(g.path().join(n), b"x");
    }
    let entries = walk_names(walk(g.path()).sort(Sort::ByName).min_depth(1));
    assert_eq!(
        entries,
        vec![(1, "a".into()), (1, "b".into()), (1, "c".into())]
    );
}

#[test]
fn walk_missing_root_yields_io_error_once() {
    let missing = Path::new(env!("CARGO_TARGET_TMPDIR")).join("fsx-no-such-root-ever");
    let mut it = walk(&missing);
    match it.next().unwrap() {
        Err(WalkError::Io { path, err }) => {
            assert_eq!(path, missing);
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        }
        other => panic!("expected Io error, got {other:?}"),
    }
    // 根失败即终止（walkdir 同语义）：第二次 next() 为 None
    assert!(it.next().is_none());
}

// ── walk：symlink 策略与环检测（S1 装甲）──

#[test]
fn walk_default_no_follow_skips_symlink_dir() {
    let g = guard("nofollow");
    fs::create_dir(g.path().join("real")).unwrap();
    write_file(g.path().join("real/inside.txt"), b"i");
    if !try_dir_link(g.path().join("link"), g.path().join("real")) {
        return;
    }
    let names: Vec<String> = walk_names(walk(g.path()).min_depth(1))
        .into_iter()
        .map(|(_, n)| n)
        .collect();
    // link 产出为条目本身，目标内容不被穿透（S1：只认 is_dir 被穿透的反面）
    assert_eq!(names, vec!["link", "real", "inside.txt"]);
}

#[test]
fn walk_follow_descends_symlink_dir() {
    let g = guard("follow");
    fs::create_dir(g.path().join("real")).unwrap();
    write_file(g.path().join("real/inside.txt"), b"i");
    if !try_dir_link(g.path().join("link"), g.path().join("real")) {
        return;
    }
    let names: Vec<String> = walk_names(walk(g.path()).follow_links(true).min_depth(1))
        .into_iter()
        .map(|(_, n)| n)
        .collect();
    assert!(
        names.contains(&"inside.txt".to_string()),
        "follow 后可达 {names:?}"
    );
}

#[test]
fn walk_follow_detects_self_loop() {
    let g = guard("selfloop");
    if !try_dir_link(g.path().join("self"), g.path()) {
        return;
    }
    let mut it = walk(g.path()).follow_links(true);
    let root = it.next().unwrap().unwrap();
    assert_eq!(root.depth(), 0);
    match it.next().unwrap() {
        Err(WalkError::Loop { path, ancestor }) => {
            assert_eq!(path, g.path().join("self"));
            assert_eq!(ancestor, g.path());
        }
        other => panic!("expected Loop, got {other:?}"),
    }
    // 环不下钻，遍历继续至正常收尾
    assert!(it.next().is_none());
}

#[test]
fn walk_follow_detects_ancestor_loop_with_sorted_determinism() {
    let g = guard("ancloop");
    fs::create_dir(g.path().join("sub")).unwrap();
    if !try_dir_link(g.path().join("up"), g.path()) {
        return;
    }
    let mut it = walk(g.path()).follow_links(true).sort(Sort::ByName);
    it.next().unwrap().unwrap(); // root
    let second = it.next().unwrap().unwrap(); // sub（字母序在 up 前）
    assert_eq!(second.path().file_name().unwrap(), "sub");
    match it.next().unwrap() {
        Err(WalkError::Loop { ancestor, .. }) => assert_eq!(ancestor, g.path()),
        other => panic!("expected Loop, got {other:?}"),
    }
}

#[cfg(windows)]
#[test]
fn walk_traverses_beyond_260_via_verbatim_root() {
    let g = guard("long260");
    // canonicalize 返回 \\?\ verbatim 形（F19），本域零归一原样透传
    let root = fs::canonicalize(g.path()).unwrap();
    let comp = "0123456789".repeat(4); // 40 字符组件 ×8 层 = 320 增量，legacy 形必 >260
    let mut deep = root.clone();
    for i in 0..8 {
        deep.push(format!("{comp}{i}"));
        fs::create_dir(&deep).unwrap();
    }
    write_file(deep.join("leaf.txt"), b"l");
    // 对照 pin 仅在禁长路径环境可构造：LongPathsEnabled=1 时 legacy 形亦可达，
    // 环境观测打印 skip（正向断言——verbatim 根遍历可达——不受影响）
    let legacy = g.path().join(format!("{comp}0"));
    if fs::create_dir_all(legacy.join(&comp).join(&comp)).is_ok() {
        eprintln!("SKIP（长路径环境已启用，260 对照不可构造）: legacy 形直达 >260 深链");
    }
    let max_depth = walk(&root).map(|e| e.unwrap().depth()).max().unwrap();
    assert_eq!(max_depth, 9); // 根 + 8 层目录（leaf 在 9 层被遍历到：8 层目录下）
}

// ── copy_tree ──

#[test]
fn copy_flat_and_nested_counts_bytes_and_content() {
    let g = guard("copysrc");
    let src = g.path().join("src");
    fs::create_dir_all(src.join("sub/empty")).unwrap();
    write_file(src.join("a.txt"), b"hello"); // 5 B
    write_file(src.join("sub/b.bin"), b"0123456789A"); // 11 B

    let dst = g.path().join("dst");
    let report = copy_tree(&src, &dst, SymlinkPolicy::Preserve).unwrap();
    assert_eq!(
        report,
        CopyReport {
            dirs: 3,
            files: 2,
            symlinks: 0,
            bytes: 16
        }
    );
    assert_eq!(fs::read(dst.join("a.txt")).unwrap(), b"hello");
    assert_eq!(fs::read(dst.join("sub/b.bin")).unwrap(), b"0123456789A");
    // 空目录照建
    assert!(dst.join("sub/empty").is_dir());
}

#[test]
fn copy_existing_dst_is_prepare_error() {
    let g = guard("copydst");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    let dst = g.path().join("dst");
    fs::create_dir(&dst).unwrap();
    match copy_tree(&src, &dst, SymlinkPolicy::Preserve).unwrap_err() {
        CopyError::Prepare { path, err } => {
            assert_eq!(path, dst);
            assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        }
        other => panic!("expected Prepare, got {other:?}"),
    }
}

#[test]
fn copy_src_not_dir_is_prepare_error() {
    let g = guard("copysrcfile");
    write_file(g.path().join("f"), b"f");
    match copy_tree(
        g.path().join("f"),
        g.path().join("d"),
        SymlinkPolicy::Preserve,
    )
    .unwrap_err()
    {
        CopyError::Prepare { path, .. } => assert!(path.ends_with("f")),
        other => panic!("expected Prepare, got {other:?}"),
    }
}

#[cfg(unix)]
#[test]
fn copy_partial_failure_reports_processed_counts() {
    use std::os::unix::fs::PermissionsExt;
    let g = guard("partialu");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    write_file(src.join("a-ok.txt"), b"good");
    write_file(src.join("b-noperm.txt"), b"secret");
    fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o000)).unwrap();

    match copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap_err() {
        CopyError::Partial { report, path, err } => {
            assert_eq!(report.files, 1, "首错前已处理量显式");
            assert!(path.ends_with("b-noperm.txt"));
            assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        }
        other => panic!("expected Partial, got {other:?}"),
    }
    fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o644)).unwrap();
}

#[cfg(windows)]
#[test]
fn copy_partial_failure_reports_processed_counts() {
    use std::os::windows::fs::OpenOptionsExt;
    let g = guard("partialw");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    write_file(src.join("a-ok.txt"), b"good");
    write_file(src.join("b-locked.txt"), b"locked");
    // 排他句柄（share_mode=0）持有期间复制必败（sharing violation）
    let _held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(src.join("b-locked.txt"))
        .unwrap();

    match copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap_err() {
        CopyError::Partial { report, path, .. } => {
            assert_eq!(report.files, 1, "首错前已处理量显式（排序保证确定性顺序）");
            assert!(path.ends_with("b-locked.txt"));
        }
        other => panic!("expected Partial, got {other:?}"),
    }
}

#[test]
fn copy_preserve_relinks_symlink() {
    let g = guard("preservelink");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    write_file(src.join("a.txt"), b"data");
    if !try_file_link(src.join("link"), src.join("a.txt")) {
        return;
    }
    let dst = g.path().join("dst");
    let report = copy_tree(&src, &dst, SymlinkPolicy::Preserve).unwrap();
    assert_eq!(report.symlinks, 1);
    assert_eq!(fs::read(dst.join("link")).unwrap(), b"data"); // 经链接可读
}

#[cfg(unix)]
#[test]
fn copy_preserve_keeps_dangling_link_on_unix() {
    let g = guard("dangling");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    std::os::unix::fs::symlink("no-such-target", src.join("dang")).unwrap();
    let dst = g.path().join("dst");
    let report = copy_tree(&src, &dst, SymlinkPolicy::Preserve).unwrap();
    assert_eq!(report.symlinks, 1);
    assert!(fs::symlink_metadata(dst.join("dang"))
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn copy_follow_copies_through_file_link_as_content() {
    let g = guard("followfile");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    write_file(src.join("a.txt"), b"data");
    if !try_file_link(src.join("link"), src.join("a.txt")) {
        return;
    }
    let dst = g.path().join("dst");
    let report = copy_tree(&src, &dst, SymlinkPolicy::Follow).unwrap();
    assert_eq!(report.symlinks, 0, "Follow 面无链接产物");
    assert_eq!(report.files, 2, "链接落为常规文件");
    assert_eq!(fs::read(dst.join("link")).unwrap(), b"data");
}

#[test]
fn copy_follow_hits_cycle_reports_processed_and_ancestor() {
    let g = guard("followcycle");
    let src = g.path().join("src");
    fs::create_dir(&src).unwrap();
    write_file(src.join("a.txt"), b"one");
    if !try_dir_link(src.join("z-loop"), &src) {
        return;
    }
    match copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Follow).unwrap_err() {
        CopyError::Cycle {
            report,
            path,
            ancestor,
        } => {
            assert_eq!(report.files, 1, "环命中前已处理量显式");
            assert!(path.ends_with("z-loop"));
            assert_eq!(ancestor, src);
        }
        other => panic!("expected Cycle, got {other:?}"),
    }
}

// ── rm_all ──

#[test]
fn rm_all_removes_nested_tree() {
    let g = guard("rmtree");
    let tree = g.path().join("tree");
    fs::create_dir_all(tree.join("x/y")).unwrap();
    write_file(tree.join("x/y/f.txt"), b"f");
    write_file(tree.join("top.txt"), b"t");
    rm_all(&tree).unwrap();
    assert!(!tree.exists());
}

#[test]
fn rm_all_single_file_works() {
    let g = guard("rmfile");
    write_file(g.path().join("f"), b"f");
    rm_all(g.path().join("f")).unwrap();
    assert!(!g.path().join("f").exists());
}

#[test]
fn rm_all_missing_is_explicit_error() {
    let ghost = Path::new(env!("CARGO_TARGET_TMPDIR")).join("fsx-no-such-rm-target");
    let err = rm_all(&ghost).unwrap_err();
    assert_eq!(err.path, ghost);
    assert_eq!(err.err.kind(), std::io::ErrorKind::NotFound);
}

#[cfg(windows)]
#[test]
fn rm_all_readonly_file_single() {
    let g = guard("rofile");
    let f = g.path().join("ro.txt");
    write_file(&f, b"ro");
    let mut perm = fs::metadata(&f).unwrap().permissions();
    perm.set_readonly(true);
    fs::set_permissions(&f, perm).unwrap();
    // 前提观测：只读属性已置上（近版 std 可直删只读文件，裸原语前提断言随
    // 环境失真已移除；装甲面=首次 PermissionDenied 才触发，语义不变）
    assert!(
        fs::metadata(&f).unwrap().permissions().readonly(),
        "readonly attr not set"
    );
    rm_all(&f).unwrap();
    assert!(!f.exists());
}

#[cfg(windows)]
#[test]
fn rm_all_readonly_file_inside_tree() {
    let g = guard("rotree");
    let tree = g.path().join("tree");
    fs::create_dir(&tree).unwrap();
    let ro = tree.join("ro.txt");
    write_file(&ro, b"ro");
    let mut perm = fs::metadata(&ro).unwrap().permissions();
    perm.set_readonly(true);
    fs::set_permissions(&ro, perm).unwrap();
    assert!(
        fs::metadata(&ro).unwrap().permissions().readonly(),
        "readonly attr not set"
    );
    rm_all(&tree).unwrap();
    assert!(!tree.exists());
}

#[cfg(unix)]
#[test]
fn rm_all_readonly_dir_surfaces_error_no_silent_chmod() {
    use std::os::unix::fs::PermissionsExt;
    let g = guard("rodir");
    let dir = g.path().join("ro-dir");
    fs::create_dir(&dir).unwrap();
    write_file(dir.join("inner"), b"x");
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o555)).unwrap();
    // Unix 只读目录删除 EACCES 属 OS 语义：如实上浮，不自动 chmod（声明上限）
    let err = rm_all(&dir).unwrap_err();
    assert_eq!(err.err.kind(), std::io::ErrorKind::PermissionDenied);
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o755)).unwrap();
    rm_all(&dir).unwrap(); // 恢复后自清理
}

// ── temp_dir_in：RAII+并发唯一 ──

#[test]
fn temp_dir_prefix_and_exists() {
    let g = guard("tproot");
    let t = temp_dir_in(g.path(), "scratch-").unwrap();
    assert!(t.path().is_dir());
    let name = t.path().file_name().unwrap().to_string_lossy().into_owned();
    assert!(name.starts_with("scratch-"));
    let p = t.path().to_owned();
    t.close().unwrap();
    assert!(!p.exists(), "close 后已删");
    assert_dir_empty(g.path());
}

#[test]
fn temp_dir_drop_removes_tree() {
    let g = guard("tdrop");
    let path: PathBuf;
    {
        let t = temp_dir_in(g.path(), "drop-").unwrap();
        write_file(t.path().join("junk"), b"j");
        path = t.path().to_owned();
        assert!(path.exists());
    } // Drop 兜底：树（含子文件）一并删
    assert!(!path.exists());
}

#[cfg(unix)]
#[test]
fn temp_dir_drop_failure_records_and_leaves() {
    use std::os::unix::fs::PermissionsExt;
    let g = guard("tfailu");
    let t = temp_dir_in(g.path(), "fail-").unwrap();
    let inner = t.path().join("inner");
    fs::create_dir(&inner).unwrap();
    write_file(inner.join("f"), b"f");
    fs::set_permissions(&inner, fs::Permissions::from_mode(0o500)).unwrap();
    let p = t.path().to_owned();
    drop(t); // 删除失败：记录 stderr 不 panic，路径保留（fail-visible 契约）
    assert!(p.exists(), "清理失败的中间态必须可见");
    // 自清理收口
    fs::set_permissions(&inner, fs::Permissions::from_mode(0o700)).unwrap();
    rm_all(&p).unwrap();
    assert_dir_empty(g.path());
}

#[cfg(windows)]
#[test]
fn temp_dir_drop_failure_records_and_leaves() {
    use std::os::windows::fs::OpenOptionsExt;
    let g = guard("tfailw");
    let t = temp_dir_in(g.path(), "fail-").unwrap();
    write_file(t.path().join("held"), b"h");
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(0)
        .open(t.path().join("held"))
        .unwrap();
    let p = t.path().to_owned();
    drop(t); // 排他句柄致删除失败：记录 stderr 不 panic，路径保留
    assert!(p.exists(), "清理失败的中间态必须可见");
    drop(held);
    rm_all(&p).unwrap();
    assert_dir_empty(g.path());
}

#[test]
fn temp_dir_concurrent_uniqueness_under_same_pid() {
    let g = guard("conc");
    const THREADS: usize = 8;
    const PER: usize = 16;
    let (tx, rx) = mpsc::channel();
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let tx = tx.clone();
            let root = g.path().to_owned();
            thread::spawn(move || {
                let mut kept = Vec::with_capacity(PER);
                for _ in 0..PER {
                    kept.push(temp_dir_in(&root, "conc-").unwrap());
                }
                tx.send(kept.iter().map(|t| t.path().to_owned()).collect::<Vec<_>>())
                    .unwrap();
                kept // 存活到断言后统一 Drop（并发存活面）
            })
        })
        .collect();
    drop(tx);
    let mut all: Vec<PathBuf> = rx.into_iter().flatten().collect();
    assert_eq!(all.len(), THREADS * PER);
    all.sort();
    let before = all.len();
    all.dedup();
    assert_eq!(all.len(), before, "同 pid 并发命名必须互异");
    for p in &all {
        assert!(p.exists(), "并发存活面：创建即存在 {p:?}");
    }
    handles.into_iter().for_each(|h| {
        h.join().unwrap();
    }); // TempDir 全部落下 → 根清空（自清理纪律）
    drop(all);
    assert_dir_empty(g.path());
}

// ── atomic_write：崩溃安全原子写（语义 armor 见 src/fsx/atomic.rs 头注）──

/// 断言目录内无 `atomic-*` 临时件残留（错误/成功路径自清纪律的可观测面）。
fn assert_no_atomic_residue(dir: &Path) {
    let residue: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("atomic-"))
        })
        .collect();
    assert!(residue.is_empty(), "atomic-* 残留: {residue:?}");
}

#[test]
fn atomic_write_creates_then_reads_back() {
    let g = guard("atomic-create");
    let file = g.path().join("app.json");
    atomic_write(&file, b"{\"port\":8080}").unwrap();
    assert_eq!(fs::read(&file).unwrap(), b"{\"port\":8080}");
    assert_no_atomic_residue(g.path());
}

#[test]
fn atomic_write_overwrites_existing_and_empty() {
    let g = guard("atomic-overwrite");
    let file = g.path().join("app.json");
    atomic_write(&file, b"v1-longer-content").unwrap();
    atomic_write(&file, b"v2").unwrap();
    assert_eq!(fs::read(&file).unwrap(), b"v2", "覆盖=新值");
    // 空字节载荷：原子截断（旧内容清空，无半写窗口）
    atomic_write(&file, b"").unwrap();
    assert_eq!(fs::read(&file).unwrap(), b"");
    assert_no_atomic_residue(g.path());
}

#[test]
fn atomic_write_failure_keeps_target_and_cleans_temp() {
    let g = guard("atomic-fail");
    // 确定性失败注入：目标=已存在目录（rename `to` 不得是目录，两平台同）。
    // 中间态契约：目标完好（原子语义：失败路径零触碰目标）+ 临时件自清
    // （错误路径）。真掉电残留（rename 前进程死）无人清扫是物理事实， armor
    // 声明项——可识别命名 atomic-* 供手扫/启动扫。
    let target = g.path().join("dir-target");
    fs::create_dir(&target).unwrap();

    let err = atomic_write(&target, b"payload").unwrap_err();
    assert!(
        fs::read_dir(&target).unwrap().next().is_none(),
        "目标目录完好：失败路径零触碰"
    );
    assert_no_atomic_residue(g.path());
    // 错误显式上浮（fail-visible）：来源为 OS rename 拒绝，平台 kind 各异，
    // 只断言非吞（有错误且非空文案）。
    assert!(!err.to_string().is_empty(), "{err}");
}

#[test]
fn atomic_write_concurrent_same_target_last_wins_never_torn() {
    let g = guard("atomic-conc");
    let file = g.path().join("app.json");
    atomic_write(&file, b"seed").unwrap();

    // 8 写者并发同目标：终值必为某写者完整载荷（rename 原子线性化=最后赢，
    // 无半写撕裂中间态）。
    let handles: Vec<_> = (0..8)
        .map(|i| {
            let file = file.clone();
            let bytes = format!("payload-{i:04}").into_bytes();
            thread::spawn(move || atomic_write(&file, &bytes).unwrap())
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let final_bytes = fs::read(&file).unwrap();
    let winners: Vec<Vec<u8>> = (0..8)
        .map(|i| format!("payload-{i:04}").into_bytes())
        .collect();
    assert!(
        winners.contains(&final_bytes),
        "终值必为完整载荷（无撕裂）: {final_bytes:?}"
    );
    assert_no_atomic_residue(g.path());
}

// ── log-embed 埋点（fsx 事件点红绿；断言基建见 common/log_capture.rs）──

#[cfg(feature = "log-embed")]
#[path = "common/log_capture.rs"]
mod log_capture;

#[cfg(feature = "log-embed")]
mod log_embed {
    use super::*;
    use log_capture::Capture;
    use tracing::Level;

    // 埋点「遍历帧推进」：下钻目录每帧一条 debug（depth/排序模式入字段）。
    #[test]
    fn walk_frame_push_emits_debug_with_depth_and_sort() {
        let g = guard("logwalk");
        let src = g.path().join("src");
        fs::create_dir(&src).unwrap();
        fs::create_dir(src.join("sub")).unwrap();
        write_file(src.join("sub").join("f"), b"x");

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            // walk 产出含根条目本身（depth 0，walkdir 同款）：根+sub+f=3。
            let got = walk(&src)
                .sort(Sort::ByName)
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(got.len(), 3);
        });

        let e = log_capture::only(&capture, "skeleton::fsx::walk");
        assert_eq!(e.level, Level::DEBUG);
        assert_eq!(e.field("depth"), Some("1"), "子目录帧深度=1");
        assert_eq!(e.field("sort"), Some("ByName"), "排序模式入字段");
    }

    // 埋点「树复制逐项」：每成功项一条 debug（kind/items 累计/bytes 累计）。
    #[test]
    fn copy_tree_emits_per_item_debug_events() {
        let g = guard("logcopy");
        let src = g.path().join("src");
        fs::create_dir(&src).unwrap();
        fs::create_dir(src.join("d")).unwrap();
        write_file(src.join("a"), b"aaaa");
        write_file(src.join("d").join("b"), b"bb");

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap();
        });

        let evs: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|e| e.target == "skeleton::fsx::copy")
            .collect();
        assert_eq!(evs.len(), 3, "3 项=3 事件: {evs:?}");
        assert!(evs.iter().all(|e| e.level == Level::DEBUG));
        let kinds: Vec<_> = evs.iter().filter_map(|e| e.field("kind")).collect();
        assert_eq!(
            kinds.iter().filter(|k| **k == "file").count(),
            2,
            "file×2: {kinds:?}"
        );
        assert!(kinds.contains(&"dir"), "dir 入 kind: {kinds:?}");
        // items 累计含 dst 根目录（report 初始 dirs=1）：1 根+3 项=4。
        assert_eq!(
            evs.last().unwrap().field("items"),
            Some("4"),
            "累计计数收尾"
        );
        assert_eq!(
            evs.last().unwrap().field("bytes"),
            Some("6"),
            "bytes 累计=4+2"
        );
    }

    // 埋点「部分失败」：Partial → warn（copied 已复制计数/path 定位/error 原文
    // 入字段；per-item debug 与本 warn 同 target，按级别过滤断言）。
    #[cfg(unix)]
    #[test]
    fn copy_partial_failure_emits_warn_with_copied_count() {
        use std::os::unix::fs::PermissionsExt;
        let g = guard("logpartialu");
        let src = g.path().join("src");
        fs::create_dir(&src).unwrap();
        write_file(src.join("a-ok.txt"), b"good");
        write_file(src.join("b-noperm.txt"), b"secret");
        fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o000)).unwrap();

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let err = copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap_err();
            assert!(matches!(err, CopyError::Partial { .. }));
        });

        let warns: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|e| e.target == "skeleton::fsx::copy" && e.level == Level::WARN)
            .collect();
        assert_eq!(warns.len(), 1, "恰一条部分失败 warn: {warns:?}");
        // copied=dirs+files+symlinks 全量：dst 根目录（report 初始 dirs=1）+ 1 文件。
        assert_eq!(warns[0].field("copied"), Some("2"), "已复制计数入字段");
        assert!(
            warns[0].field("path").unwrap().contains("b-noperm"),
            "失败定位入字段"
        );
        assert!(
            !warns[0].field("error").unwrap().is_empty(),
            "错误原文入字段"
        );
        fs::set_permissions(src.join("b-noperm.txt"), fs::Permissions::from_mode(0o644)).unwrap();
    }

    // Windows 同款：排他句柄（share_mode=0）致 sharing violation → Partial。
    #[cfg(windows)]
    #[test]
    fn copy_partial_failure_emits_warn_with_copied_count() {
        use std::os::windows::fs::OpenOptionsExt;
        let g = guard("logpartialw");
        let src = g.path().join("src");
        fs::create_dir(&src).unwrap();
        write_file(src.join("a-ok.txt"), b"good");
        write_file(src.join("b-locked.txt"), b"locked");
        let _held = fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(src.join("b-locked.txt"))
            .unwrap();

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || {
            let err = copy_tree(&src, g.path().join("dst"), SymlinkPolicy::Preserve).unwrap_err();
            assert!(matches!(err, CopyError::Partial { .. }));
        });

        let warns: Vec<_> = capture
            .events()
            .into_iter()
            .filter(|e| e.target == "skeleton::fsx::copy" && e.level == Level::WARN)
            .collect();
        assert_eq!(warns.len(), 1, "恰一条部分失败 warn: {warns:?}");
        // copied=dirs+files+symlinks 全量：dst 根目录（report 初始 dirs=1）+ 1 文件。
        assert_eq!(warns[0].field("copied"), Some("2"), "已复制计数入字段");
        assert!(
            warns[0].field("path").unwrap().contains("b-locked"),
            "失败定位入字段"
        );
        assert!(
            !warns[0].field("error").unwrap().is_empty(),
            "错误原文入字段"
        );
    }

    // 埋点「TempDir RAII 清理失败」：树被外部先删 → Drop 内 rm_all 失败 → warn
    //（path/error 入字段；eprintln fail-visible 通道保持不变，事件是增量面）。
    #[test]
    fn tempdir_drop_cleanup_failure_emits_warn() {
        let g = guard("logtmpdir");
        let tmp = temp_dir_in(g.path(), "victim-").unwrap();
        fs::remove_dir_all(tmp.path()).unwrap();

        let capture = Capture::new();
        tracing::subscriber::with_default(capture.subscriber(), || drop(tmp));

        let e = log_capture::only(&capture, "skeleton::fsx::tempdir");
        assert_eq!(e.level, Level::WARN);
        assert!(e.field("path").unwrap().contains("victim-"));
        assert!(!e.field("error").unwrap().is_empty());
    }
}
