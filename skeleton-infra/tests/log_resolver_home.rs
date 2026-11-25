//! #179 条 4：release 下 [`DefaultPathResolver`] 只接受**绝对**家锚——相对
//! `$HOME`/`USERPROFILE` 必须变成 typed 错误，而不是拼出 cwd 相对日志根。
//!
//! 本文件在 debug 构建下为空（debug 分支不解析 home，见 src/logx.rs）。

#![cfg(all(feature = "log-fs", not(debug_assertions)))]

use std::ffi::OsString;

use skeleton_infra::logx::{DefaultPathResolver, LogPathError};

#[cfg(unix)]
const HOME_VAR: &str = "HOME";
#[cfg(windows)]
const HOME_VAR: &str = "USERPROFILE";

/// env 守卫：构造留存原值、Drop 恢复（`var_os` 面，含非 UTF-8 环境值）。
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
fn relative_home_anchor_is_typed_error() {
    let _guard = EnvGuard::save(HOME_VAR);
    std::env::set_var(HOME_VAR, "relative-home");

    // 修复前：home_dir 原样返回相对路径 ⇒ 根变成 <cwd>/.agentwiki/log。
    assert!(
        matches!(
            DefaultPathResolver::new("agentwiki"),
            Err(LogPathError::HomeUnresolvable(_))
        ),
        "相对家锚必须 typed Err（禁静默落 cwd）"
    );

    // 名字校验先于家锚解析：配置错误优先报，且 debug/release 同判。
    assert!(
        matches!(
            DefaultPathResolver::new("../escape"),
            Err(LogPathError::InvalidProjectName { .. })
        ),
        "非法名字在 home 不可用时仍须先报 InvalidProjectName"
    );
}
