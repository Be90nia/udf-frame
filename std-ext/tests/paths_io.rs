//! paths 域 IO 面集成测试：canonicalize_compat 真路径验证（F19 直击）与
//! ensure_inside 端到端 containment。临时目录守卫落工作区 target/（禁系统
//! TEMP 红线），Drop 清树。

use std::path::{Path, PathBuf};

use std_ext::paths::canonicalize_compat;
// containment 断言面仅 Windows 端到端用例用（verbatim 路径基准判定）——
// Unix 面未使用，按平台导入，否则 Unix 面 clippy 报 unused import。
#[cfg(windows)]
use std_ext::paths::{ensure_inside, CaseStrategy};

/// 工作区内临时目录 RAII 守卫（进程 id 隔离并发；Drop 清树）。
struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(tag: &str) -> std::io::Result<Self> {
        let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("paths-io-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir)?;
        Ok(TempDirGuard(dir))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn canonicalize_absolute_and_nonverbatim() {
    let guard = TempDirGuard::new("cc").unwrap();
    let sub = guard.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let c = canonicalize_compat(&sub).unwrap();
    assert!(c.is_absolute());
    // F19 直击：结果不得残留可剥离的 verbatim 前缀（此路径受控短于 260）
    assert!(!c.as_os_str().to_string_lossy().starts_with(r"\\?\"));
}

#[test]
fn canonicalize_missing_path_is_explicit_io_error() {
    let guard = TempDirGuard::new("miss").unwrap();
    let ghost = guard.path().join("no-such-child");
    let err = canonicalize_compat(&ghost).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
}

#[cfg(windows)]
#[test]
fn verbatim_base_containment_f19_end_to_end() {
    // F19 端到端：canonicalize 出的路径与基准判定 containment，verbatim 形态
    // 不得造成误判
    let guard = TempDirGuard::new("inside").unwrap();
    let file = guard.path().join("f.txt");
    std::fs::write(&file, b"x").unwrap();
    let base = canonicalize_compat(guard.path()).unwrap();
    let path = canonicalize_compat(&file).unwrap();
    ensure_inside(&base, &path, CaseStrategy::CaseSensitive)
        .expect("canonicalized child must be inside canonicalized base");
    // 逃逸面：同级外路径必须拒
    let outside = TempDirGuard::new("outside").unwrap();
    let outside_file = outside.path().join("f.txt");
    std::fs::write(&outside_file, b"x").unwrap();
    let outside_c = canonicalize_compat(&outside_file).unwrap();
    assert!(
        ensure_inside(&base, &outside_c, CaseStrategy::CaseSensitive).is_err(),
        "sibling temp dir must not be inside base"
    );
}
