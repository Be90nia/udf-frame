//! #180：私有日志目录/文件权限（Unix 面）。Windows 无 POSIX 权限位——隐私靠
//! 父目录 ACL 继承，故权限断言 Unix-only（Windows 策略见 `src/logx.rs` 文档）。

#![cfg(all(feature = "log-fs", unix))]

use std::io::Write as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use skeleton_infra::logx::FileSink;
use std_ext::fsx::{TempDir, temp_dir_in};

/// umask 是进程级全局：本文件测试串行跑，避免互相把对方的新建语义放宽。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("logperm-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

/// 唯一当日文件（文件名规则私有，不在此复制）。
fn only_file(root: &Path) -> PathBuf {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("log root readable")
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    assert_eq!(files.len(), 1, "按日滚动：单日恰一个文件");
    files.remove(0)
}

fn mode_of(path: &Path) -> u32 {
    std::fs::metadata(path).expect("stat").permissions().mode() & 0o777
}

#[cfg(target_os = "macos")]
type ModeT = u16;
#[cfg(not(target_os = "macos"))]
type ModeT = u32;

unsafe extern "C" {
    fn umask(mask: ModeT) -> ModeT;
}

/// 设进程 umask 并在 Drop 恢复（panic 展开路径同样恢复）。
struct UmaskGuard(ModeT);

impl UmaskGuard {
    fn set(mask: u16) -> Self {
        // SAFETY: libc umask 无指针参数、非重入依赖；本文件测试经 SERIAL 串行。
        Self(unsafe { umask(mask as ModeT) })
    }
}

impl Drop for UmaskGuard {
    fn drop(&mut self) {
        // SAFETY: 同 UmaskGuard::set——libc umask 无指针参数、无别名风险。
        unsafe { umask(self.0) };
    }
}

/// 最宽松 umask（000）下——mode 未显式给就会产出 0777/0666，故断言 0700/0600
/// 即证明 `mode` 独立于 umask。
#[test]
fn new_root_and_day_file_are_private_under_permissive_umask() {
    let _s = serial();
    let scratch = guard("umask");
    let root = scratch.path().join("logs");

    let umask = UmaskGuard::set(0o000);
    let mut sink = FileSink::new(&root).expect("root creatable");
    sink.write_all(b"line\n").expect("soft path never errs");
    drop(umask);

    assert_eq!(mode_of(&root), 0o700, "新建日志根恒 0700（独立于 umask）");
    let day = only_file(&root);
    assert_eq!(mode_of(&day), 0o600, "新建当日文件恒 0600（独立于 umask）");
}

/// 既有根 group/other 可访问 ⇒ typed 拒绝，且**不改**调用方目录权限。
#[test]
fn existing_group_readable_root_rejected_without_chmod() {
    let _s = serial();
    let scratch = guard("root-mode");
    let root = scratch.path().join("logs");
    std::fs::create_dir(&root).expect("pre-create root");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).expect("fixture");

    let err = FileSink::new(&root).expect_err("group/other accessible root must be rejected");
    assert_eq!(
        err.kind(),
        std::io::ErrorKind::PermissionDenied,
        "拒绝面须 typed（PermissionDenied），实际: {err}"
    );
    assert_eq!(mode_of(&root), 0o755, "拒绝不得静默 chmod 调用方目录");
}

/// 既有当日文件 group/other 可访问 ⇒ 打开即拒（权限校验早于写入首字节）。
#[test]
fn existing_group_readable_day_file_rejected_before_any_byte() {
    let _s = serial();
    let scratch = guard("file-mode");
    let root = scratch.path().join("logs");

    let mut probe = FileSink::new(&root).expect("root creatable");
    probe.write_all(b"probe\n").expect("soft path never errs");
    let day = only_file(&root);
    drop(probe);
    std::fs::set_permissions(&day, std::fs::Permissions::from_mode(0o644)).expect("fixture");

    let mut sink = FileSink::new(&root).expect("既有 0700 根仍私有");
    assert_eq!(sink.write(b"lost\n").expect("soft path never errs"), 5);
    assert_eq!(sink.dropped_lines(), 1, "非私有既有文件必须拒写");
    assert_eq!(
        std::fs::read_to_string(&day).expect("readable"),
        "probe\n",
        "拒写不得落任何字节"
    );
    assert_eq!(mode_of(&day), 0o644, "拒绝不得静默 chmod 调用方文件");
}
