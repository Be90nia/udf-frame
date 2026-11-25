//! #130：`init_with_file` 失败时只回收**本次新建**的空日志根——既有根即使为空
//! 也是调用方状态（`remove_dir` 成功不证明归属）。

#![cfg(feature = "log")]

use std::path::{Path, PathBuf};

use skeleton::logpath::LogPathResolver;
use skeleton_infra::logx::{LogInitError, init_tracing_default, init_with_file};
use std_ext::fsx::{TempDir, temp_dir_in};

/// 契约测试替身：任意根目录注入。
struct FixedRoot(PathBuf);

impl LogPathResolver for FixedRoot {
    fn log_root(&self) -> PathBuf {
        self.0.clone()
    }
}

fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logown-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 夹具根：Unix 需 0700 才能过既有根隐私校验（测试自己造的就是要模拟
/// 调用方自建的安全根）。
fn private_dir(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(path)
            .expect("fixture root");
    }
    #[cfg(not(unix))]
    std::fs::create_dir_all(path).expect("fixture root");
}

/// 全局 logger 单例：本文件只有一个测试（首次 init 占位后，后续 init_with_file
/// 恒 Err(AlreadyInit)），否则并发 init 测试会互相抢占。
#[test]
fn init_failure_preserves_preexisting_roots_and_reclaims_created_one() {
    let scratch = guard("ownership");
    init_tracing_default(log::LevelFilter::Info).expect("first init in this process");

    // ① 既有空根：init 失败不得回收（这是修复前会被删掉的那一个）。
    let existing = scratch.path().join("existing");
    private_dir(&existing);
    assert!(matches!(
        init_with_file(&FixedRoot(existing.clone()), log::LevelFilter::Info),
        Err(LogInitError::AlreadyInit)
    ));
    assert!(existing.is_dir(), "既有空日志根不得被 init 失败回收");

    // ② 既有非空根：目录与内容都保留。
    let populated = scratch.path().join("populated");
    private_dir(&populated);
    std::fs::write(populated.join("keep.txt"), b"caller data").expect("fixture file");
    assert!(matches!(
        init_with_file(&FixedRoot(populated.clone()), log::LevelFilter::Info),
        Err(LogInitError::AlreadyInit)
    ));
    assert_eq!(
        std::fs::read_to_string(populated.join("keep.txt")).expect("kept"),
        "caller data"
    );

    // ③ 本次新建的空根：best-effort 回收语义不变。
    let fresh = scratch.path().join("fresh");
    assert!(matches!(
        init_with_file(&FixedRoot(fresh.clone()), log::LevelFilter::Info),
        Err(LogInitError::AlreadyInit)
    ));
    assert!(!fresh.exists(), "本次新建的空根应被回收");
}
