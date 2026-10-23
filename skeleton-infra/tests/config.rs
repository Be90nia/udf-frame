//! config 域集成测试（IO 面）：ConfigFile 分层读写回环（TempDir 沙箱）/
//! 原子覆盖/残留断言/TOML 进出面。纯值面合并在 `src/config.rs` 内联测试
//! （CARGO_TARGET_TMPDIR 仅集成测试目标可用，且禁系统 TEMP 红线）。
//!
//! 临时目录守卫=fsx [`temp_dir_in`] 自产自销（dogfood：域件即测试基建，
//! tests/fsx.rs 同款），Drop 兜底+关键用例残留断言。

#![cfg(feature = "config-json")]

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use skeleton::config::ConfigError;
use skeleton_infra::config::{load_toml, save_toml, ConfigFile};
use std_ext::fsx::{temp_dir_in, TempDir};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct App {
    host: String,
    port: u16,
}

// ── 守卫与助手 ──

/// 工作区内临时根（dogfood temp_dir_in；清理=TempDir 自带 Drop 兜底，守卫
/// 禁再手动删树——双删只会产出 rm_all NotFound 噪音）。
struct Guard(TempDir);

fn guard(tag: &str) -> Guard {
    Guard(
        temp_dir_in(env!("CARGO_TARGET_TMPDIR"), &format!("config-{tag}-"))
            .expect("temp root under CARGO_TARGET_TMPDIR"),
    )
}

impl Guard {
    fn path(&self) -> &Path {
        self.0.path()
    }
}

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

// ── JSON 面：save/file_layer 回环 ──

#[test]
fn save_then_file_layer_round_trip() {
    let g = guard("roundtrip");
    let file = g.path().join("app.json");
    let cfg = App {
        host: "db.internal".to_string(),
        port: 8080,
    };

    ConfigFile::save(&cfg, &file).unwrap();

    let loaded: App = ConfigFile::new().file_layer(&file).unwrap().load().unwrap();
    assert_eq!(loaded, cfg);
    assert_no_atomic_residue(g.path());
}

#[test]
fn save_overwrites_existing_content_atomically() {
    let g = guard("overwrite");
    let file = g.path().join("app.json");

    ConfigFile::save(
        &App {
            host: "a".to_string(),
            port: 1,
        },
        &file,
    )
    .unwrap();
    ConfigFile::save(
        &App {
            host: "b".to_string(),
            port: 2,
        },
        &file,
    )
    .unwrap();

    let loaded: App = ConfigFile::new().file_layer(&file).unwrap().load().unwrap();
    assert_eq!(
        loaded,
        App {
            host: "b".to_string(),
            port: 2,
        },
        "覆盖后=新值（原子替换非追加）"
    );
    assert_no_atomic_residue(g.path());
}

#[test]
fn default_then_file_layer_full_chain_override() {
    let g = guard("chain");
    let file = g.path().join("partial.json");
    // 文件只给 port：host 留给 default 层（部分覆盖文件的真实形态）
    fs::write(&file, br#"{"port": 9090}"#).unwrap();

    let mut cfg: App = ConfigFile::new()
        .default_layer(&App {
            host: "base.internal".to_string(),
            port: 1,
        })
        .unwrap()
        .file_layer(&file)
        .unwrap()
        .load()
        .unwrap();
    assert_eq!(
        cfg,
        App {
            host: "base.internal".to_string(),
            port: 9090,
        },
        "file 层覆盖 default，缺键回落 default"
    );

    // explicit 层=代码后覆盖（链图第 4 层，无 API 面——就是代码）
    cfg.host = "explicit".to_string();
    assert_eq!(cfg.host, "explicit");
}

#[test]
fn file_layer_bad_json_is_parse_with_path_key() {
    let g = guard("badjson");
    let file = g.path().join("bad.json");
    fs::write(&file, b"{ not json").unwrap();

    let err = ConfigFile::<App>::new().file_layer(&file).unwrap_err();
    match &err {
        ConfigError::Parse { key, .. } => assert_eq!(key, &file.display().to_string()),
        other => panic!("期望 Parse，得 {other:?}"),
    }
}

// ── TOML 面（config-toml）──

#[cfg(feature = "config-toml")]
mod toml_face {
    use super::*;

    #[test]
    fn toml_save_load_round_trip() {
        let g = guard("toml-roundtrip");
        let file = g.path().join("app.toml");
        let cfg = App {
            host: "db.internal".to_string(),
            port: 8080,
        };

        save_toml(&cfg, &file).unwrap();
        let loaded: App = load_toml(&file).unwrap();
        assert_eq!(loaded, cfg);
        assert_no_atomic_residue(g.path());
    }

    #[test]
    fn toml_file_layer_rejects_datetime_fail_visible() {
        let g = guard("toml-dt");
        let file = g.path().join("with-dt.toml");
        fs::write(&file, b"when = 1979-05-27T07:32:00Z\n").unwrap();

        let err = ConfigFile::<App>::new().file_layer_toml(&file).unwrap_err();
        match &err {
            ConfigError::Parse { source, .. } => {
                assert!(source.to_string().contains("datetime"), "{source}");
            }
            other => panic!("期望 Parse，得 {other:?}"),
        }
    }

    #[test]
    fn toml_file_layer_merges_over_default() {
        let g = guard("toml-chain");
        let file = g.path().join("partial.toml");
        fs::write(&file, b"port = 9090\n").unwrap();

        let cfg: App = ConfigFile::new()
            .default_layer(&App {
                host: "base".to_string(),
                port: 1,
            })
            .unwrap()
            .file_layer_toml(&file)
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(
            cfg,
            App {
                host: "base".to_string(),
                port: 9090,
            },
            "TOML 文件层覆盖 default，缺键回落"
        );
    }
}
