//! 日志根目录解析契约：文件日志落点的可替换端口。
//!
//! 自 `skeleton-infra::logx` 下沉（批 1 契约真下沉，2026-10-18；ADR 0001）：宿主对默认落点不满（如开发态想锚定
//! 采方自己工程根）时实现本 trait 注入 `skeleton_infra::logx::init_with_file`。
//! 默认实现 `DefaultPathResolver`（debug=编译期锚 / release=`.<project>/log`）
//! 是行为件，留在 `skeleton_infra::logx`（`log-fs` feature）。

/// 日志根目录解析契约（`skeleton::clock::Clock` / `skeleton::config::ConfigPort`
/// 同构：契约住契约层，实现件可替换）。宿主对默认落点不满时，实现本 trait
/// 注入 `skeleton_infra::logx::init_with_file`：
///
/// ```no_run
/// struct AppRoot;
///
/// impl skeleton::logpath::LogPathResolver for AppRoot {
///     fn log_root(&self) -> std::path::PathBuf {
///         // env! 在采方 crate 内展开 → 编译期锚=采方工程根
///         std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("log")
///     }
/// }
/// ```
pub trait LogPathResolver: Send + Sync {
    /// 日志根目录；按日滚动文件 `log-YYYY-MM-DD.log` 落在其下。不可解析时
    /// 的语义由实现自定——`skeleton_infra::logx::DefaultPathResolver` 在构造期
    /// 显式 Err，禁静默落 cwd。
    fn log_root(&self) -> std::path::PathBuf;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    struct FixedRoot(PathBuf);

    impl LogPathResolver for FixedRoot {
        fn log_root(&self) -> PathBuf {
            self.0.clone()
        }
    }

    // 契约面：实现自定义根逐字透传（dyn 注入形与 init_with_file 形参一致）。
    #[test]
    fn log_root_returns_impl_defined_root_verbatim() {
        let r = FixedRoot(PathBuf::from("Z:/logs"));
        let dyn_r: &dyn LogPathResolver = &r;
        assert_eq!(dyn_r.log_root(), Path::new("Z:/logs"));
    }

    // 对象安全与线程语义（Send + Sync，装配面可跨线程共享——Clock/EnvSource 先例）。
    #[test]
    fn resolver_objects_are_send_sync_dyn_safe() {
        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn LogPathResolver>();
        let boxed: Box<dyn LogPathResolver> = Box::new(FixedRoot(PathBuf::from("/tmp/log")));
        assert_eq!(boxed.log_root(), Path::new("/tmp/log"));
    }
}
