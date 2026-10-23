//! home/XDG 域 env 接线验收（pub API 真读进程 env；纯解析矩阵在 src/paths.rs
//! 单测——`resolve_xdg_dir` 无 env 访问，全矩阵零 env 竞争）。
//!
//! env 是进程单例：本文件 SERIAL 串行闸 + EnvGuard 构造留存/Drop 恢复
//! （panic 展开路径同样恢复；tests/log_file.rs 同形 armor）。

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};

use std_ext::paths::{cache_dir, config_dir, data_dir, home_dir};

#[cfg(unix)]
const HOME_VAR: &str = "HOME";
#[cfg(windows)]
const HOME_VAR: &str = "USERPROFILE";

static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// env 守卫：构造留存原值、Drop 恢复（var_os 面无损往返——含非 UTF-8 环境值）。
struct EnvGuard {
    var: &'static str,
    saved: Option<OsString>,
}

impl EnvGuard {
    fn save(var: &'static str) -> Self {
        Self {
            var,
            saved: std::env::var_os(var),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match self.saved.take() {
            Some(v) => std::env::set_var(self.var, v),
            None => std::env::remove_var(self.var),
        }
    }
}

#[test]
fn home_dir_follows_env_injection() {
    let _s = serial();
    let _g = EnvGuard::save(HOME_VAR);
    std::env::set_var(HOME_VAR, "/xdg-test-home");
    assert_eq!(home_dir(), Some(PathBuf::from("/xdg-test-home")));
}

#[cfg(not(windows))]
#[test]
fn xdg_trio_reads_real_vars() {
    let _s = serial();
    let home_g = EnvGuard::save(HOME_VAR);
    let _cg = EnvGuard::save("XDG_CONFIG_HOME");
    let _kg = EnvGuard::save("XDG_CACHE_HOME");
    let _dg = EnvGuard::save("XDG_DATA_HOME");
    std::env::set_var(HOME_VAR, "/xdg-home");
    std::env::set_var("XDG_CONFIG_HOME", "/xdg/cfg/");
    std::env::set_var("XDG_CACHE_HOME", "");
    std::env::set_var("XDG_DATA_HOME", "relative/junk");
    // 绝对值赢（尾分隔符归一）／空串=未设→默认／相对=invalid 忽略→默认
    assert_eq!(config_dir(), Some(PathBuf::from("/xdg/cfg")));
    assert_eq!(cache_dir(), Some(PathBuf::from("/xdg-home/.cache")));
    assert_eq!(data_dir(), Some(PathBuf::from("/xdg-home/.local/share")));
    drop(home_g);
}

#[cfg(not(windows))]
#[test]
fn xdg_trio_unset_defaults_from_home() {
    let _s = serial();
    let _cg = EnvGuard::save("XDG_CONFIG_HOME");
    let _kg = EnvGuard::save("XDG_CACHE_HOME");
    let _dg = EnvGuard::save("XDG_DATA_HOME");
    let _hg = EnvGuard::save(HOME_VAR);
    std::env::remove_var("XDG_CONFIG_HOME");
    std::env::remove_var("XDG_CACHE_HOME");
    std::env::remove_var("XDG_DATA_HOME");
    std::env::set_var(HOME_VAR, "/xdg-home2/");
    // 三变量全未设 → spec 默认链；home 尾分隔符在 join 后归一
    assert_eq!(config_dir(), Some(PathBuf::from("/xdg-home2/.config")));
    assert_eq!(cache_dir(), Some(PathBuf::from("/xdg-home2/.cache")));
    assert_eq!(data_dir(), Some(PathBuf::from("/xdg-home2/.local/share")));
}

#[cfg(windows)]
#[test]
fn xdg_trio_windows_face_is_none() {
    let _s = serial();
    let _cg = EnvGuard::save("XDG_CONFIG_HOME");
    let _kg = EnvGuard::save("XDG_CACHE_HOME");
    let _dg = EnvGuard::save("XDG_DATA_HOME");
    // 拍板：Windows 面恒 None（XDG 是 Unix 约定；原生 Known Folders 面另立项）。
    // env 设置了也不读——按 None 分支处置的契约钉死。
    std::env::set_var("XDG_CONFIG_HOME", r"C:\xdg\cfg");
    std::env::set_var("XDG_CACHE_HOME", r"C:\xdg\cache");
    std::env::set_var("XDG_DATA_HOME", r"C:\xdg\data");
    assert_eq!(config_dir(), None);
    assert_eq!(cache_dir(), None);
    assert_eq!(data_dir(), None);
}
