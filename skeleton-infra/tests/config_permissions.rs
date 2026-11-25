//! #192 验收 5：config 层权限面（Unix）。#215 起 `ConfigFile::save` /
//! `save_toml` 默认走 `AtomicWritePolicy::Private`——临时件建时即 0600，
//! rename 发布同一 inode，故首存与替换都恒私有，且独立于 umask。
//! Windows 无 POSIX 权限位（私有性靠父目录 ACL 继承），断言 Unix-only。

#![cfg(all(feature = "config-json", unix))]

use std::fs;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use skeleton_infra::config::ConfigFile;
use std_ext::fsx::{TempDir, temp_dir_in};

/// umask 是进程级全局：本文件测试串行跑，避免互相把对方的新建语义放宽。
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn guard(tag: &str) -> TempDir {
    temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("configperm-{tag}-"))
        .expect("temp root under CARGO_TARGET_TMPDIR")
}

fn mode_of(path: &Path) -> u32 {
    fs::metadata(path).expect("stat").permissions().mode() & 0o777
}

/// 断言目录内无 `atomic-*` 临时件残留（临时件+rename 路径自清的可观测面）。
fn assert_no_atomic_residue(dir: &Path) {
    let residue: Vec<PathBuf> = fs::read_dir(dir)
        .expect("sandbox readable")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("atomic-"))
        })
        .collect();
    assert!(residue.is_empty(), "atomic-* 残留: {residue:?}");
}

#[derive(Debug, Serialize, Deserialize)]
struct App {
    host: String,
    port: u16,
}

fn app(host: &str) -> App {
    App {
        host: host.to_string(),
        port: 8080,
    }
}

/// 落盘入口（Private 策略的两条公开面）。
type Writer = fn(&App, &Path);

fn save_json(cfg: &App, path: &Path) {
    ConfigFile::save(cfg, path).expect("json save");
}

#[cfg(feature = "config-toml")]
fn save_toml_cfg(cfg: &App, path: &Path) {
    skeleton_infra::config::save_toml(cfg, path).expect("toml save");
}

fn writers() -> Vec<(&'static str, Writer)> {
    let mut all: Vec<(&'static str, Writer)> = vec![("json", save_json)];
    #[cfg(feature = "config-toml")]
    all.push(("toml", save_toml_cfg));
    all
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

/// 首存：新建配置文件即 0600（Private 在建临时件时显式给位）。
#[test]
fn new_config_files_are_private() {
    let _s = serial();
    for (ext, save) in writers() {
        let sandbox = guard(&format!("new-{ext}"));
        let file = sandbox.path().join(format!("app.{ext}"));

        save(&app("fresh"), &file);

        assert_eq!(mode_of(&file), 0o600, "首存 {ext} 须 0600");
        assert_no_atomic_residue(sandbox.path());
    }
}

/// 最宽松 umask（000）下仍是 0600——位在临时件创建时定死，umask 独立。
#[test]
fn new_config_files_are_private_under_permissive_umask() {
    let _s = serial();
    // 沙箱先建（umask=000 下建目录会得到 0777，与被测件无关的噪音）。
    let cases: Vec<(&'static str, Writer, TempDir)> = writers()
        .into_iter()
        .map(|(ext, save)| (ext, save, guard(&format!("umask-{ext}"))))
        .collect();

    let umask = UmaskGuard::set(0o000);
    for (ext, save, sandbox) in cases {
        let file = sandbox.path().join(format!("app.{ext}"));

        save(&app("fresh"), &file);

        assert_eq!(mode_of(&file), 0o600, "umask 000 下首存 {ext} 仍须 0600");
    }
    drop(umask);
}

/// 替换既有 0600 文件：仍 0600（Private 不放宽）。
#[test]
fn replacing_private_file_keeps_it_private() {
    let _s = serial();
    for (ext, save) in writers() {
        let sandbox = guard(&format!("keep-{ext}"));
        let file = sandbox.path().join(format!("app.{ext}"));
        fs::write(&file, b"").expect("pre-create");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).expect("fixture");

        save(&app("kept"), &file);

        assert_eq!(mode_of(&file), 0o600, "替换既有 0600 {ext} 不得放宽");
        assert_no_atomic_residue(sandbox.path());
    }
}

/// 替换既有 0644 文件：收窄为 0600。收窄是 **Private 的显式策略**，不是
/// 继承旧 mode（那个语义在 `AtomicWritePolicy::PreserveExisting`）。
#[test]
fn replacing_group_readable_file_narrows_to_private() {
    let _s = serial();
    for (ext, save) in writers() {
        let sandbox = guard(&format!("narrow-{ext}"));
        let file = sandbox.path().join(format!("app.{ext}"));
        fs::write(&file, b"").expect("pre-create");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).expect("fixture");

        save(&app("narrowed"), &file);

        assert_eq!(
            mode_of(&file),
            0o600,
            "0644 {ext} 覆盖后按 Private 显式收窄为 0600（非继承旧 mode）"
        );
        assert_no_atomic_residue(sandbox.path());
    }
}

/// 原子替换（临时件+rename）：内容为新值、旧内容零残留；短→长与长→短各一。
#[test]
fn replace_publishes_new_value_and_leaves_no_old_bytes() {
    let _s = serial();
    for (ext, save) in writers() {
        let sandbox = guard(&format!("replace-{ext}"));
        let file = sandbox.path().join(format!("app.{ext}"));
        let short = "s".repeat(4);
        let long = "l".repeat(200);

        save(&app(&short), &file);
        let inode = fs::metadata(&file).expect("stat").ino();
        save(&app(&long), &file); // 短 → 长
        let text = fs::read_to_string(&file).expect("readable");
        assert!(text.contains(&long), "替换后须含新值: {text}");
        assert!(!text.contains(&short), "旧短值不得残留: {text}");
        assert_ne!(
            fs::metadata(&file).expect("stat").ino(),
            inode,
            "替换走临时件+rename（新 inode 发布），非原地改写"
        );
        assert_eq!(mode_of(&file), 0o600, "替换后 {ext} 仍 0600");

        save(&app(&short), &file); // 长 → 短
        let text = fs::read_to_string(&file).expect("readable");
        assert!(text.contains(&short), "替换后须含新值: {text}");
        assert!(!text.contains(&long), "旧长值不得残留: {text}");
        assert_eq!(mode_of(&file), 0o600, "替换后 {ext} 仍 0600");

        assert_no_atomic_residue(sandbox.path());
    }
}
