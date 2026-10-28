//! 配置装配契约域：类型化配置 + env 覆盖 + fail-visible 校验。
//!
//! 自 `skeleton-infra::config` 下沉（批 1 契约真下沉，2026-10-18；ADR 0001）：[`ConfigPort`]/[`EnvSource`] 是配置域
//! 的规范接口（journal 先例：框架只做规范接口，格式归实现）。实现件
//! [`StdEnv`]/[`MockEnv`] 随迁（纯 std 零 feature 门）；文件装配面
//! （`ConfigFile` 分层 + TOML 翻译捷径）是行为件，留在 `skeleton_infra`
//! （`config-json`/`config-toml` feature）。
//!
//! 常驻核心只认「已反序列化结构体 + env 覆盖 + 校验」这一层，格式解析不进
//! 契约面。与 config-rs / figment 对接姿势：它们做第 1 层「文件 → 结构体」，
//! 本契约规范第 2、3 层——采用方把加载结果装进自身结构，`load` 只看 env。
//!
//! # 覆盖链三层（谁负责什么）
//!
//! ```text
//! 1. 底座（采用方）：默认值 / 文件加载结果，构造 Self 时自带——
//!    格式解析、字段默认全在契约之外（格式不感知）
//! 2. env 覆盖（本契约）：ConfigPort::load 经 EnvSource 读环境，
//!    显式出现的键覆盖底座值
//! 3. 校验（本契约）：产出 Config 前完成——缺必填键报 Missing（禁静默
//!    默认值），非法值报 Invalid，解析失败报 Parse
//! ```
//!
//! 坑面 armor（std 文档/生态事实逐条对表，tests 钉死）：
//!
//! - env 值非 Unicode（`std::env::var_os` + `into_string` 判定）→
//!   [`EnvError::NotUnicode`]（原始 [`OsString`](std::ffi::OsString) 保真），
//!   **≠ Missing**：lossy 替换 U+FFFD 会无声损坏路径/密钥/URL，谎报 Missing
//!   会把排障者引去查「谁删了键」，都违 fail-visible；
//! - 大小写语义随平台/实现（Windows env 键不分大小写；Unix/HashMap 字节
//!   精确）→ 契约不 normalize（归一会毁 Unix 大小写敏感键），采用方按目标
//!   平台自理，[`MockEnv`] 恒字节精确可断言；
//! - `dyn EnvSource` 对象安全（方法零泛型、零 `Self` 返回）→ 编译期钉死
//!   （[`ConfigPort::load`] 形参即 `&dyn EnvSource`）；
//! - 键名注入面：键只作透明查找键，不进命令/路径/URL，无注入面（声明项）；
//! - 静默默认值：缺必填键必报 [`ConfigError::Missing`]（fail-visible，
//!   禁 `unwrap_or(default)` 兜底）。

use std::collections::HashMap;
use std::fmt;

/// env 读取失败的两态（fail-visible：非 Unicode 不再谎报 Missing）。
///
/// 手写同形 `Display`/`Error`（同 config/journal 先例：thiserror 不入
/// manifest，零依赖）。原始值在 [`EnvError::NotUnicode`] 内保真供诊断。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EnvError {
    /// 键不存在。
    Missing,
    /// 键存在但值非 Unicode——原始 [`OsString`](std::ffi::OsString) 随行
    /// 供诊断（不丢信息；lossy 替换禁于此层）。
    NotUnicode(std::ffi::OsString),
}

impl fmt::Display for EnvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => write!(f, "env key missing"),
            Self::NotUnicode(_) => write!(f, "env value is not valid Unicode"),
        }
    }
}

impl std::error::Error for EnvError {}

/// env 读取抽象：配置域唯一环境入口（依赖注入 → 可测）。
///
/// 语义契约：
///
/// - `get` 返回 `Ok` ⟺ 键存在**且值为合法 Unicode**；键缺失 =
///   [`EnvError::Missing`]；键在但值非 Unicode =
///   [`EnvError::NotUnicode`]（原始字节保真，禁 lossy 静默替换）；
/// - 大小写语义实现自理（见模块 armor），契约层不归一。
///
/// 对象安全：[`ConfigPort::load`] 以 `&dyn EnvSource` 收环境（方法零泛型、
/// 零 `Self` 返回，编译期钉死）。`Send + Sync` 随 [`Clock`](crate::clock::Clock)
/// 先例——装配面可跨线程共享。
///
/// # Examples
///
/// ```
/// use skeleton::config::{EnvError, EnvSource, MockEnv};
///
/// fn probe(env: &dyn EnvSource) -> Result<String, EnvError> {
///     env.get("APP_PORT")
/// }
///
/// assert_eq!(probe(&MockEnv::new().with("APP_PORT", "8080")).as_deref(), Ok("8080"));
/// assert!(matches!(probe(&MockEnv::new()), Err(EnvError::Missing)));
/// ```
pub trait EnvSource: Send + Sync {
    /// 读一个环境值：缺失报 [`EnvError::Missing`]；键在但值非 Unicode 报
    /// [`EnvError::NotUnicode`]（≠ Missing，原始字节保真）。
    fn get(&self, key: &str) -> Result<String, EnvError>;
}

/// std::env 参考实现：[`EnvSource`] 零依赖直包。
///
/// 语义（std 文档对表，`var_os` 存在性 + `into_string` 转换两步判定）：
///
/// - 键存在且值合法 Unicode → `Ok`；
/// - 键不存在 → `Err(EnvError::Missing)`；
/// - 键存在但值非 Unicode（Windows 混编 WTF-16 / Unix 非 UTF-8 字节）→
///   `Err(EnvError::NotUnicode(原始 OsString))`（**拒绝，非 lossy**——静默
///   U+FFFD 替换会无声损坏路径/密钥；原始字节随错误保真，需要字节的采用方
///   直接取用，不再要求自持 `std::env::var_os` 面）。
///
/// # Examples
///
/// ```
/// use skeleton::config::{EnvError, EnvSource, StdEnv};
///
/// // 冒烟：进程环境必有 PATH（Windows）/ HOME（Unix）；野键 = Missing。
/// let key = if cfg!(windows) { "PATH" } else { "HOME" };
/// assert!(StdEnv.get(key).is_ok());
/// assert!(matches!(
///     StdEnv.get("UDF_CONFIG_NO_SUCH_KEY_7F3A"),
///     Err(EnvError::Missing)
/// ));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StdEnv;

impl EnvSource for StdEnv {
    fn get(&self, key: &str) -> Result<String, EnvError> {
        // 存在性与 Unicode 转换两步：键在但非 Unicode = NotUnicode ≠ Missing。
        match std::env::var_os(key) {
            None => Err(EnvError::Missing),
            Some(value) => value.into_string().map_err(EnvError::NotUnicode),
        }
    }
}

/// 测试注入面：[`EnvSource`] 测试替身（注入 + 断言）。
///
/// 字节精确查找（`HashMap` 语义，恒区分大小写，镜像 Unix 行为；Windows
/// 平台差异由采用方按目标平台自理——见 [`EnvSource`] 契约）。
///
/// # Examples
///
/// ```
/// use skeleton::config::{EnvError, EnvSource, MockEnv};
///
/// let env = MockEnv::new().with("APP_PORT", "8080");
/// assert_eq!(env.get("APP_PORT").as_deref(), Ok("8080"));
/// assert!(
///     matches!(env.get("app_port"), Err(EnvError::Missing)),
///     "字节精确：大小写不归一"
/// );
/// assert!(matches!(env.get("MISSING"), Err(EnvError::Missing)));
///
/// // 断言面：注入全集可直接对表
/// assert_eq!(env.vars().len(), 1);
/// ```
#[derive(Debug, Clone, Default)]
pub struct MockEnv {
    vars: HashMap<String, String>,
}

impl MockEnv {
    /// 空环境（链式注入起点）。
    pub fn new() -> Self {
        Self::default()
    }

    /// 注入一键（builder 风格，消费自身链式注入）。
    pub fn with<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.vars.insert(key.into(), value.into());
        self
    }

    /// 注入一键（可变式，循环注入用）。
    pub fn set<K: Into<String>, V: Into<String>>(&mut self, key: K, value: V) {
        self.vars.insert(key.into(), value.into());
    }

    /// 注入全集（断言面：测试对表注入状态）。
    pub fn vars(&self) -> &HashMap<String, String> {
        &self.vars
    }
}

impl EnvSource for MockEnv {
    fn get(&self, key: &str) -> Result<String, EnvError> {
        // 值面恒 String（Unicode 已保证），唯 Missing 一态可达。
        self.vars.get(key).cloned().ok_or(EnvError::Missing)
    }
}

/// 配置装配端口：env 覆盖 → 校验 → 类型化产出（覆盖链第 2、3 层的规范面；
/// 底座归采用方，见模块文档）。
///
/// `Self` 持底座（默认值/文件加载结果），[`ConfigPort::load`] 只看 env——
/// 同 `skeleton::parse` 端口先例：契约面零真实解析、零格式词汇。
///
/// # Examples
///
/// 契约即可实现：十几行演示覆盖链全语义（真实底座装载是采用方的事）。
///
/// ```
/// use skeleton::config::{ConfigError, ConfigPort, EnvError, EnvSource};
///
/// struct DbConfig {
///     host: String,
///     port: u16,
/// }
///
/// /// 底座（默认 host）随构造自带；load 只做 env 覆盖 + 校验。
/// struct DbLoader {
///     default_host: String,
/// }
///
/// impl ConfigPort for DbLoader {
///     type Config = DbConfig;
///
///     fn load(&self, env: &dyn EnvSource) -> Result<DbConfig, ConfigError> {
///         // 覆盖：env 显式给出的键压过底座值；非 Unicode 显式上报（禁 lossy 兜底）
///         let host = match env.get("APP_DB_HOST") {
///             Ok(h) => h,
///             Err(EnvError::Missing) => self.default_host.clone(),
///             Err(EnvError::NotUnicode(value)) => {
///                 return Err(ConfigError::NotUnicode {
///                     key: "APP_DB_HOST".to_string(),
///                     value,
///                 });
///             }
///         };
///         // env 覆盖段必填键缺失：fail-visible 报 Missing，禁静默默认
///         let raw = match env.get("APP_DB_PORT") {
///             Ok(raw) => raw,
///             Err(EnvError::Missing) => {
///                 return Err(ConfigError::Missing {
///                     key: "APP_DB_PORT".to_string(),
///                 });
///             }
///             Err(EnvError::NotUnicode(value)) => {
///                 return Err(ConfigError::NotUnicode {
///                     key: "APP_DB_PORT".to_string(),
///                     value,
///                 });
///             }
///         };
///         let port: u16 = raw.parse().map_err(|source| ConfigError::Parse {
///             key: "APP_DB_PORT".to_string(),
///             source: Box::new(source),
///         })?;
///         // 校验：语义非法报 Invalid
///         if port == 0 {
///             return Err(ConfigError::Invalid {
///                 key: "APP_DB_PORT".to_string(),
///                 reason: "port 0 is not bindable".to_string(),
///             });
///         }
///         Ok(DbConfig { host, port })
///     }
/// }
/// ```
pub trait ConfigPort {
    /// 装配产物：已校验的类型化配置。
    type Config;

    /// env 覆盖 → 校验 → 产出。缺必填键必报 [`ConfigError::Missing`]（禁
    /// 静默默认值），语义非法报 [`ConfigError::Invalid`]，解析失败报
    /// [`ConfigError::Parse`]（`source` 链保源头）。
    fn load(&self, env: &dyn EnvSource) -> Result<Self::Config, ConfigError>;
}

/// 配置装配错误（fail-visible 面：Display 含键名，`source` 链保源头）。
///
/// 手写同形 `Display`/`Error`（同 text/journal 先例：thiserror 不入
/// manifest，文案与派生形态逐字一致，引入依赖时可换回派生）。`File`/
/// `Serialize` 两变体服务 `skeleton_infra` 的文件装配面（`config-json`/
/// `config-toml` feature），随契约枚举同住避免错误类型分叉。
#[derive(Debug)]
pub enum ConfigError {
    /// 必填键缺失（fail-visible：禁静默默认值兜底）。
    Missing {
        /// 缺失的环境键名。
        key: String,
    },
    /// env 值非 Unicode（键在但 [`EnvError::NotUnicode`] 上抛的映射面；
    /// 原始值保真供诊断，禁 lossy 兜底）。
    NotUnicode {
        /// 非 Unicode 值所属键名。
        key: String,
        /// 原始非 Unicode 值（`OsString` 保真，不丢信息）。
        value: std::ffi::OsString,
    },
    /// 键存在但值语义非法（范围/形态校验失败）。
    Invalid {
        /// 非法值所属键名。
        key: String,
        /// 非法原因（人读）。
        reason: String,
    },
    /// 值解析失败（词法/反序列化层）。
    Parse {
        /// 解析失败所属键名（文件层=文件路径；整体反序列化=`(config)`）。
        key: String,
        /// 解析失败源头。
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
    /// 配置文件 IO 失败（读文件/原子写落盘；`path` 定位事故文件）。
    File {
        /// 失败所属文件路径。
        path: std::path::PathBuf,
        /// IO 失败源头。
        source: std::io::Error,
    },
    /// 序列化失败（保存面 Pretty 序列化；原子写前失败，零落盘）。
    Serialize {
        /// 序列化失败源头。
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { key } => write!(f, "missing config key: {key}"),
            Self::NotUnicode { key, .. } => {
                write!(f, "env value for {key} is not valid Unicode")
            }
            Self::Invalid { key, reason } => {
                write!(f, "invalid config value for {key}: {reason}")
            }
            Self::Parse { key, source } => write!(f, "parse config value for {key}: {source}"),
            Self::File { path, source } => {
                write!(f, "config file {}: {source}", path.display())
            }
            Self::Serialize { source } => write!(f, "serialize config: {source}"),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Parse { source, .. } => Some(source.as_ref()),
            Self::Serialize { source } => Some(source.as_ref()),
            Self::File { source, .. } => Some(source),
            Self::Missing { .. } | Self::Invalid { .. } | Self::NotUnicode { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdenv_smoke_existing_and_missing() {
        let key = if cfg!(windows) { "PATH" } else { "HOME" };
        assert!(StdEnv.get(key).is_ok(), "进程环境必有 PATH/HOME");
        assert!(
            matches!(
                StdEnv.get("UDF_CONFIG_NO_SUCH_KEY_7F3A"),
                Err(EnvError::Missing)
            ),
            "野键 = Missing（≠ NotUnicode）"
        );
    }

    // 非 Unicode ≠ Missing（本单拍板形状）：键在但值非 Unicode 必报
    // EnvError::NotUnicode（原始 OsString 保真），禁谎报 Missing 把排障者
    // 引去查「谁删了键」。进程 env 是全局面——唯一键名 + 读后恢复，不污染
    // 并行兄弟测试。
    #[test]
    #[cfg(windows)]
    fn stdenv_non_unicode_is_not_missing() {
        use std::ffi::OsString;
        use std::os::windows::ffi::OsStringExt;

        let key = "UDF_CONFIG_NONUNICODE_7F3A";
        let invalid = OsString::from_wide(&[0xD800]); // 未配对代理项：非 Unicode 标量值
        let prev = std::env::var_os(key);
        std::env::set_var(key, &invalid);
        let got = StdEnv.get(key);
        match prev {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        match got {
            Err(EnvError::NotUnicode(raw)) => assert_eq!(raw, invalid, "原始字节保真供诊断"),
            Err(EnvError::Missing) => panic!("键存在但值非 Unicode 不得谎报 Missing"),
            Ok(_) => panic!("非 Unicode 值必须拒绝（禁 lossy U+FFFD 替换）"),
        }
    }

    #[test]
    #[cfg(unix)]
    fn stdenv_non_unicode_is_not_missing() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let key = "UDF_CONFIG_NONUNICODE_7F3A";
        let invalid = OsString::from_vec(vec![0xFF]); // 非 UTF-8 字节
        let prev = std::env::var_os(key);
        std::env::set_var(key, &invalid);
        let got = StdEnv.get(key);
        match prev {
            Some(v) => std::env::set_var(key, v),
            None => std::env::remove_var(key),
        }
        match got {
            Err(EnvError::NotUnicode(raw)) => assert_eq!(raw, invalid, "原始字节保真供诊断"),
            Err(EnvError::Missing) => panic!("键存在但值非 Unicode 不得谎报 Missing"),
            Ok(_) => panic!("非 Unicode 值必须拒绝（禁 lossy U+FFFD 替换）"),
        }
    }

    #[test]
    fn dyn_env_source_object_safe_and_send_sync() {
        fn probe(env: &dyn EnvSource, key: &str) -> Result<String, EnvError> {
            env.get(key)
        }
        assert_eq!(
            probe(&MockEnv::new().with("K", "v"), "K").as_deref(),
            Ok("v")
        );
        assert!(matches!(
            probe(&StdEnv, "UDF_CONFIG_NO_SUCH_KEY_7F3A"),
            Err(EnvError::Missing)
        ));

        fn assert_send_sync<T: Send + Sync + ?Sized>() {}
        assert_send_sync::<dyn EnvSource>();
        assert_send_sync::<ConfigError>();
    }

    // MockEnv 字节精确（镜像 Unix 语义；大小写不归一）。
    #[test]
    fn mock_env_is_byte_exact_case_sensitive() {
        let env = MockEnv::new().with("APP_PORT", "8080");
        assert_eq!(env.get("APP_PORT").as_deref(), Ok("8080"));
        assert!(
            matches!(env.get("app_port"), Err(EnvError::Missing)),
            "字节精确：大小写不归一"
        );
        assert!(matches!(env.get("MISSING"), Err(EnvError::Missing)));
        assert_eq!(env.vars().len(), 1);
    }

    // 非 Unicode → ConfigError::NotUnicode（新增变体，拍板：复用 Invalid 会
    // 丢 OsString 原始字节且把「解码失败」谎报为「语义校验失败」）。替身
    // 注入，平台无关不碰进程 env。
    struct NonUnicodeEnv;

    impl EnvSource for NonUnicodeEnv {
        fn get(&self, _key: &str) -> Result<String, EnvError> {
            Err(EnvError::NotUnicode("\u{FFFD}raw-bytes".into()))
        }
    }

    #[test]
    fn config_port_not_unicode_maps_fail_visible() {
        let loader = TestLoader {
            default_host: String::new(),
        };
        let err = loader
            .load(&NonUnicodeEnv)
            .expect_err("非 Unicode 必显式报");
        match &err {
            ConfigError::NotUnicode { key, value } => {
                assert_eq!(key, "T_HOST");
                assert_eq!(value.to_string_lossy(), "\u{FFFD}raw-bytes", "原始值保真");
            }
            other => panic!("期望 NotUnicode，得 {other:?}"),
        }
        assert_eq!(err.to_string(), "env value for T_HOST is not valid Unicode");
        // 解码失败无 Error source（源头即事实本身，OsString 非 Error）
        assert!(std::error::Error::source(&err).is_none());
    }

    // 被测采用方：底座（默认 host）+ env 覆盖 port（必填）+ 校验——契约
    // 即可实现演示的生产化版本（Display/source 断言钉错误面形状）。
    #[derive(Debug, PartialEq)]
    struct TestConfig {
        host: String,
        port: u16,
    }

    struct TestLoader {
        default_host: String,
    }

    impl ConfigPort for TestLoader {
        type Config = TestConfig;

        fn load(&self, env: &dyn EnvSource) -> Result<TestConfig, ConfigError> {
            let host = match env.get("T_HOST") {
                Ok(h) => h,
                Err(EnvError::Missing) => self.default_host.clone(),
                Err(EnvError::NotUnicode(value)) => {
                    return Err(ConfigError::NotUnicode {
                        key: "T_HOST".to_string(),
                        value,
                    });
                }
            };
            let raw = match env.get("T_PORT") {
                Ok(raw) => raw,
                Err(EnvError::Missing) => {
                    return Err(ConfigError::Missing {
                        key: "T_PORT".to_string(),
                    });
                }
                Err(EnvError::NotUnicode(value)) => {
                    return Err(ConfigError::NotUnicode {
                        key: "T_PORT".to_string(),
                        value,
                    });
                }
            };
            let port: u16 = raw.parse().map_err(|source| ConfigError::Parse {
                key: "T_PORT".to_string(),
                source: Box::new(source),
            })?;
            if port == 0 {
                return Err(ConfigError::Invalid {
                    key: "T_PORT".to_string(),
                    reason: "port 0 is not bindable".to_string(),
                });
            }
            Ok(TestConfig { host, port })
        }
    }

    #[test]
    fn config_port_env_override_wins_over_base() {
        let loader = TestLoader {
            default_host: "base.internal".to_string(),
        };
        let cfg = loader
            .load(&MockEnv::new().with("T_PORT", "8080"))
            .expect("T_PORT 已给出");
        assert_eq!(
            cfg,
            TestConfig {
                host: "base.internal".to_string(),
                port: 8080,
            }
        );
        let cfg = loader
            .load(
                &MockEnv::new()
                    .with("T_HOST", "override")
                    .with("T_PORT", "9090"),
            )
            .expect("两键均已给出");
        assert_eq!(
            cfg,
            TestConfig {
                host: "override".to_string(),
                port: 9090,
            }
        );
    }

    #[test]
    fn config_port_missing_key_fails_visible() {
        let loader = TestLoader {
            default_host: "base.internal".to_string(),
        };
        let err = loader.load(&MockEnv::new()).expect_err("缺必填键必报");
        match &err {
            ConfigError::Missing { key } => assert_eq!(key, "T_PORT"),
            other => panic!("期望 Missing，得 {other:?}"),
        }
        assert_eq!(err.to_string(), "missing config key: T_PORT");
    }

    #[test]
    fn config_port_parse_error_keeps_source_chain() {
        let loader = TestLoader {
            default_host: String::new(),
        };
        let err = loader
            .load(&MockEnv::new().with("T_PORT", "not-a-number"))
            .expect_err("非数字必报 Parse");
        match &err {
            ConfigError::Parse { key, .. } => assert_eq!(key, "T_PORT"),
            other => panic!("期望 Parse，得 {other:?}"),
        }
        // source 链保真：ParseIntError 可达，真相不静默丢
        let src = std::error::Error::source(&err).expect("Parse 必有 source");
        assert_eq!(src.to_string(), "invalid digit found in string");
        assert_eq!(
            err.to_string(),
            "parse config value for T_PORT: invalid digit found in string"
        );
    }

    #[test]
    fn config_port_invalid_value_fails_visible() {
        let loader = TestLoader {
            default_host: String::new(),
        };
        let err = loader
            .load(&MockEnv::new().with("T_PORT", "0"))
            .expect_err("port 0 语义非法必报 Invalid");
        match &err {
            ConfigError::Invalid { key, reason } => {
                assert_eq!(key, "T_PORT");
                assert_eq!(reason, "port 0 is not bindable");
            }
            other => panic!("期望 Invalid，得 {other:?}"),
        }
        assert_eq!(
            err.to_string(),
            "invalid config value for T_PORT: port 0 is not bindable"
        );
        // Missing/Invalid 无 source（源头即本层事实）
        assert!(std::error::Error::source(&err).is_none());
    }
}
