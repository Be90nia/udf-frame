#![cfg(feature = "config-json")]
//! 配置文件读入上限（#197/#127）：默认上限 + 构造器覆盖，恰等于 / 超一字节 /
//! 路径错误 / 不部分接受；`file_layer`、`file_layer_toml`、`load_toml` 三入口同策略。
//!
//! 测试收拢在 `mod config_limit` 下——`cargo test -p udf-skeleton-infra --all-features
//! config` 按函数名路径过滤，恰好全量命中（`tests/config.rs` 同款约定）。
//!
//! 临时目录守卫=fsx [`temp_dir_in`]（`tests/config.rs` 同款：Drop 兜底，禁手删）。

mod config_limit {
    use std::fs;
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Serialize};
    use skeleton::config::ConfigError;
    #[cfg(feature = "config-toml")]
    use skeleton_infra::config::load_toml;
    use skeleton_infra::config::{ConfigFile, ConfigFileTooLarge, DEFAULT_MAX_FILE_BYTES};
    use std_ext::fsx::{TempDir, temp_dir_in};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct App {
        host: String,
        port: u16,
    }

    // ── 守卫与助手 ──

    struct Guard(TempDir);

    fn guard(tag: &str) -> Guard {
        Guard(
            temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("config-limit-{tag}-"))
                .expect("temp root under CARGO_TARGET_TMPDIR"),
        )
    }

    impl Guard {
        fn path(&self) -> &Path {
            self.0.path()
        }
    }

    /// 超限错误的型别载荷（`ConfigError::File` + `source=ConfigFileTooLarge`）。
    fn too_large(err: &ConfigError) -> (PathBuf, ConfigFileTooLarge) {
        match err {
            ConfigError::File { path, source } => {
                assert_eq!(source.kind(), std::io::ErrorKind::InvalidData, "{err}");
                let payload = *source
                    .get_ref()
                    .and_then(|e| e.downcast_ref::<ConfigFileTooLarge>())
                    .expect("source 必须是 ConfigFileTooLarge");
                (path.clone(), payload)
            }
            other => panic!("期望 File（超限），得 {other:?}"),
        }
    }

    fn json_file(g: &Guard, name: &str) -> PathBuf {
        let file = g.path().join(name);
        fs::write(&file, br#"{"host":"db","port":1}"#).unwrap();
        file
    }

    // ── JSON 文件层 ──

    #[test]
    fn file_layer_accepts_exactly_limit_via_builder_override() {
        let g = guard("json-eq");
        let file = json_file(&g, "app.json");
        let limit = fs::metadata(&file).unwrap().len() as usize;

        let cfg: App = ConfigFile::new()
            .max_file_bytes(limit)
            .file_layer(&file)
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(
            cfg,
            App {
                host: "db".to_string(),
                port: 1
            },
            "恰等于上限必须放行"
        );
    }

    #[test]
    fn file_layer_rejects_one_byte_over_without_partial_acceptance() {
        let g = guard("json-over");
        let file = json_file(&g, "app.json");
        // 前缀恰为合法 JSON ⇒ 若被截断/部分接受就会「成功」，这里必须 Err
        let limit = fs::metadata(&file).unwrap().len() as usize - 1;

        let err = ConfigFile::<App>::new()
            .max_file_bytes(limit)
            .file_layer(&file)
            .unwrap_err();
        let (path, payload) = too_large(&err);
        assert_eq!(path, file);
        assert_eq!(
            payload,
            ConfigFileTooLarge {
                limit,
                observed: limit + 1
            },
            "observed=至少读到 limit+1（读满即停）"
        );
    }

    #[test]
    fn file_layer_path_error_is_file_error_not_size_error() {
        let g = guard("json-missing");
        let missing = g.path().join("nope.json");

        let err = ConfigFile::<App>::new().file_layer(&missing).unwrap_err();
        match &err {
            ConfigError::File { path, source } => {
                assert_eq!(path, &missing);
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("期望 File(NotFound)，得 {other:?}"),
        }
    }

    #[test]
    fn file_layer_default_limit_rejects_oversized_file() {
        let g = guard("json-default");
        let file = g.path().join("huge.json");
        fs::write(&file, vec![b' '; DEFAULT_MAX_FILE_BYTES + 1]).unwrap();

        let err = ConfigFile::<App>::new().file_layer(&file).unwrap_err();
        let (path, payload) = too_large(&err);
        assert_eq!(path, file);
        assert_eq!(payload.limit, DEFAULT_MAX_FILE_BYTES, "未覆盖时走默认上限");
        assert!(
            payload.observed > DEFAULT_MAX_FILE_BYTES,
            "observed={} 必须超过上限",
            payload.observed
        );
    }

    #[test]
    fn default_limit_still_accepts_small_file() {
        let g = guard("json-small");
        let file = json_file(&g, "small.json");
        assert!((fs::metadata(&file).unwrap().len() as usize) < DEFAULT_MAX_FILE_BYTES);

        let cfg: App = ConfigFile::new().file_layer(&file).unwrap().load().unwrap();
        assert_eq!(cfg.port, 1);
    }

    // ── TOML 面（config-toml）──

    #[cfg(feature = "config-toml")]
    mod toml_face {
        use super::*;

        #[test]
        fn file_layer_toml_accepts_exactly_limit_and_rejects_over() {
            let g = guard("toml");
            let file = g.path().join("app.toml");
            fs::write(&file, b"host = \"db\"\nport = 1\n").unwrap();
            let limit = fs::metadata(&file).unwrap().len() as usize;

            let cfg: App = ConfigFile::new()
                .max_file_bytes(limit)
                .file_layer_toml(&file)
                .unwrap()
                .load()
                .unwrap();
            assert_eq!(cfg.host, "db", "恰等于上限放行（Datetime 面同策略）");

            let err = ConfigFile::<App>::new()
                .max_file_bytes(limit - 1)
                .file_layer_toml(&file)
                .unwrap_err();
            let (path, payload) = too_large(&err);
            assert_eq!(path, file);
            assert_eq!((payload.limit, payload.observed), (limit - 1, limit));
        }

        #[test]
        fn load_toml_default_limit_rejects_oversized_file() {
            let g = guard("toml-load");
            let file = g.path().join("huge.toml");
            fs::write(&file, vec![b' '; DEFAULT_MAX_FILE_BYTES + 1]).unwrap();

            let err = load_toml::<App>(&file).unwrap_err();
            let (path, payload) = too_large(&err);
            assert_eq!(path, file);
            assert_eq!(
                payload.limit, DEFAULT_MAX_FILE_BYTES,
                "单文件面恒走默认上限"
            );
        }
    }
}
