//! 配置文件装配面：分层装配（default→file→层叠合并→load）+ 原子保存 + TOML
//! 翻译捷径（行为件）。
//!
//! 契约已下沉 `skeleton::config`（批 1 契约真下沉，2026-10-18；ADR
//! `docs/adr/0001-contract-sink.md`）：[`ConfigPort`](skeleton::config::ConfigPort)/
//! [`EnvSource`](skeleton::config::EnvSource)/[`ConfigError`](skeleton::config::ConfigError)
//! 住契约层（纯 std 零门），本域只剩 feature 门控行为件——
//! `config-json` = [`ConfigFile`] 分层装配+原子保存（serde_json::Value 统一
//! 值面），`config-toml` = TOML 翻译捷径（`load_toml`/`save_toml`/
//! `file_layer_toml`）。拍板修订（2026-10-17，配置文件读写升一等公民）：
//! 文件面框架供给，常驻核心格式盲的零依赖纪律由契约层承接。
//!
//! # 覆盖链三层（谁负责什么）
//!
//! ```text
//! 1. 底座（采用方）：默认值 / 文件加载结果，构造 Self 时自带——
//!    格式解析、字段默认全在契约之外（格式不感知）；文件层框架面见
//!    [`ConfigFile`]（config-json feature，default→file 分层装配）
//! 2. env 覆盖（契约层）：ConfigPort::load 经 EnvSource 读环境，
//!    显式出现的键覆盖底座值
//! 3. 校验（契约层）：产出 Config 前完成——缺必填键报 Missing（禁静默
//!    默认值），非法值报 Invalid，解析失败报 Parse
//! ```
//!
//! 与 config-rs / figment 对接姿势：它们做第 1 层「文件 → 结构体」，契约层
//! 规范第 2、3 层——采用方把加载结果装进自身结构，`load` 只看 env。
//!
//! 零依赖纪律：`config-json`/`config-toml` 为 optional feature（serde/
//! serde_json journal 先例形态，toml 仅 config-toml 可达）。形状拍板点：
//! reports/2026-10-17-config-port.md、reports/2026-10-17-config-file.md。

#[cfg(feature = "config-toml")]
use std::fmt;
#[cfg(feature = "config-json")]
use std::marker::PhantomData;
#[cfg(feature = "config-json")]
use std::path::Path;

#[cfg(feature = "config-json")]
use skeleton::config::ConfigError;

// ── ConfigFile：分层装配 + 原子保存（config-json/config-toml feature 面）──
//
// 拍板 2026-10-17：配置文件读写=框架一等公民；常驻核心保持零依赖格式盲，
// 值面/格式捷径挂 feature（报告 reports/2026-10-17-config-file.md）。

/// 配置文件分层装配面。
///
/// # 分层链四层（低层被高层覆盖，同层内后键赢）
///
/// ```text
/// default（代码构造）→ file（文件底座）→ env（ConfigPort 已有段）→ explicit（代码后覆盖）
/// ```
///
/// - **default/file/explicit 原始值层**归本面：层以 `serde_json::Value`
///   叠放（统一值模型），[`ConfigFile::load`] 低→高依次折叠后一次性反
///   序列化为 `C`；
/// - **env 层不进本面**：字符串 env 值 → 字段类型的解析是类型化职责
///   （[`ConfigPort`] 已有段），原始值面若代劳会把 `"8080"` 串进数字
///   字段=反序列化错误。装配姿势：`file` 层产物作为底座喂采用方的
///   ConfigPort 实现，`load(env)` 出值后采用方代码再改=explicit 层。
///
/// # 合并语义（折叠序=层序）
///
/// 递归深度两面（杠精盲审 2026-10-17 措辞统一）：**解析层**（文件/env 经
/// `from_str`）受 serde_json 128 层上限约束，超深=可恢复 `Err`；**程序化
/// `.layer()` 层**构造的 `Value` 不经解析器、无此上限，超深嵌套在
/// `merge_over`/`from_value` 递归处为**栈溢出进程 abort**（实弹探针
/// 2026-10-17，release exit 253，与 `serde_json::from_value` 同性质）——
/// 非可恢复错误，程序化层深度的上限防控是采用方职责。
///
/// - 对象∩对象：递归合并，高层键覆盖/新增（同层内后键赢）；
/// - 高层非对象（标量/数组/null）：整体替换——数组不按下标合并、null 是
///   显式值（config-rs 式 null 吞并/下标合并魔法判否，fail-visible）。
///
/// # 保存面
///
/// [`ConfigFile::save`] = Pretty 序列化 + [`fsx::atomic_write`](std_ext::fsx)
/// 原子落盘（同目录临时件+fsync+rename，崩溃安全语义见 fsx 域）。**保存=
/// 全量重序列化**：注释与键序不保留（fail-visible 前置声明；注释保真是
/// toml_edit 类采用方面，框架不代劳）；并发保存经 rename 原子性线性化=
/// 最后赢。serde_json 序列化语义如实声明（实测 2026-10-17）：整数键映射
/// 被静默字符串化、非有限浮点（NaN/±Inf）被静默写 null——类型层需挡这两
/// 类值的采用方自理。
///
/// TOML 面（`config-toml`）：[`load_toml`]/[`save_toml`] 类型化直读直写
/// （Datetime 全保真）；[`ConfigFile::file_layer_toml`] 把 TOML 文件折进
/// 分层链——TOML Datetime 在统一值面无表示，**显式报错**（禁静默降级成
/// 私有标记映射）。
///
/// # Examples
///
/// ```no_run
/// use serde::{Deserialize, Serialize};
/// use skeleton_infra::config::ConfigFile;
///
/// #[derive(Serialize, Deserialize)]
/// struct App { port: u16 }
///
/// let cfg: App = ConfigFile::new()
///     .default_layer(&App { port: 8080 }).unwrap() // 第 1 层：代码构造
///     .file_layer("app.json").unwrap()             // 第 2 层：{"port": 9090}
///     .load().unwrap();                            // 高层覆盖 → port = 9090
///
/// ConfigFile::save(&cfg, "app.json").unwrap();     // Pretty + 原子写
/// ```
#[cfg(feature = "config-json")]
pub struct ConfigFile<C> {
    /// 层叠原始值（低→高；load 时依次折叠）。
    layers: Vec<serde_json::Value>,
    _marker: PhantomData<fn() -> C>,
}

#[cfg(feature = "config-json")]
impl<C> ConfigFile<C> {
    /// 空分层链。
    pub fn new() -> Self {
        Self {
            layers: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// 显式原始值层（链图 explicit 层/外部覆盖/中间任意层皆可叠）。
    pub fn layer(mut self, value: serde_json::Value) -> Self {
        self.layers.push(value);
        self
    }

    /// 折叠载入：低→高合并后一次性反序列化为 `C`。
    ///
    /// 反序列化失败报 [`ConfigError::Parse`]（key=`"(config)"`，serde 错误
    /// 文案含缺失字段名，真相不静默丢）。
    pub fn load(self) -> Result<C, ConfigError>
    where
        C: serde::de::DeserializeOwned,
    {
        let mut layers = self.layers.into_iter();
        // 空链=null 底起折叠（无 default/file 的显式起点，非静默兜底：
        // 折不出字段照样在反序列化处报 Parse）
        let mut folded = layers.next().unwrap_or(serde_json::Value::Null);
        for layer in layers {
            folded = merge_over(folded, layer);
        }
        serde_json::from_value(folded).map_err(|source| ConfigError::Parse {
            key: "(config)".to_string(),
            source: Box::new(source),
        })
    }
}

#[cfg(feature = "config-json")]
impl<C> std::fmt::Debug for ConfigFile<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 手写防 derive 的 `C: Debug` 假界：PhantomData<fn() -> C> 恒 Debug
        f.debug_struct("ConfigFile")
            .field("layers", &self.layers)
            .finish()
    }
}

#[cfg(feature = "config-json")]
impl<C> Default for ConfigFile<C> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "config-json")]
impl<C> ConfigFile<C> {
    /// 底座层：代码构造默认值 → 值面入链（链图第 1 层）。
    pub fn default_layer(self, value: &C) -> Result<Self, ConfigError>
    where
        C: serde::Serialize,
    {
        let v = serde_json::to_value(value).map_err(|source| ConfigError::Serialize {
            source: Box::new(source),
        })?;
        Ok(self.layer(v))
    }

    pub fn file_layer(self, path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|source| ConfigError::File {
            path: path.to_path_buf(),
            source,
        })?;
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).map_err(|source| ConfigError::Parse {
                key: path.display().to_string(),
                source: Box::new(source),
            })?;
        Ok(self.layer(v))
    }

    pub fn save(value: &C, path: impl AsRef<Path>) -> Result<(), ConfigError>
    where
        C: serde::Serialize,
    {
        let path = path.as_ref();
        let text =
            serde_json::to_string_pretty(value).map_err(|source| ConfigError::Serialize {
                source: Box::new(source),
            })?;
        std_ext::fsx::atomic_write(path, text.as_bytes()).map_err(|source| ConfigError::File {
            path: path.to_path_buf(),
            source,
        })
    }
}

/// TOML 文件层：读文件 → 解析 → 统一值面入层（链图第 2 层的 TOML 变体）。
///
/// TOML Datetime 在统一值面无表示，显式报 [`ConfigError::Parse`]（禁静默
/// 降级）；需要 Datetime 全保真走 [`load_toml`] 类型化直读。
#[cfg(feature = "config-toml")]
impl<C> ConfigFile<C> {
    pub fn file_layer_toml(self, path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|source| ConfigError::File {
            path: path.to_path_buf(),
            source,
        })?;
        let v: toml::Value = toml::from_slice(&bytes).map_err(|source| ConfigError::Parse {
            key: path.display().to_string(),
            source: Box::new(source),
        })?;
        let j = toml_value_to_json(v)?;
        Ok(self.layer(j))
    }
}

/// 类型化直读 TOML 配置文件（单文件面，无分层；Datetime 全保真）。
///
/// IO 失败报 [`ConfigError::File`]；TOML 词法/反序列化失败报
/// [`ConfigError::Parse`]（key=文件路径）。
#[cfg(feature = "config-toml")]
pub fn load_toml<C: serde::de::DeserializeOwned>(path: impl AsRef<Path>) -> Result<C, ConfigError> {
    let path = path.as_ref();
    let bytes = std::fs::read(path).map_err(|source| ConfigError::File {
        path: path.to_path_buf(),
        source,
    })?;
    toml::from_slice(&bytes).map_err(|source| ConfigError::Parse {
        key: path.display().to_string(),
        source: Box::new(source),
    })
}

/// 保存：Pretty TOML + 原子写（语义见 [`ConfigFile`] 文档保存面；TOML 文档
/// 须为表根，标量根序列化失败报 [`ConfigError::Serialize`]，fail-visible）。
#[cfg(feature = "config-toml")]
pub fn save_toml<C: serde::Serialize>(
    value: &C,
    path: impl AsRef<Path>,
) -> Result<(), ConfigError> {
    let path = path.as_ref();
    let text = toml::to_string_pretty(value).map_err(|source| ConfigError::Serialize {
        source: Box::new(source),
    })?;
    std_ext::fsx::atomic_write(path, text.as_bytes()).map_err(|source| ConfigError::File {
        path: path.to_path_buf(),
        source,
    })
}

/// 高层 `over` 折上低层 `base`（对象递归合并，非对象整体替换；语义见
/// [`ConfigFile`] 文档合并节）。
#[cfg(feature = "config-json")]
fn merge_over(base: serde_json::Value, over: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match (base, over) {
        (Value::Object(mut b), Value::Object(o)) => {
            for (k, v) in o {
                let m = match b.remove(&k) {
                    Some(base_v) => merge_over(base_v, v),
                    None => v,
                };
                b.insert(k, m);
            }
            Value::Object(b)
        }
        // 非对象高层面整体替换（标量/数组/null 一致；config-rs 式 null 吞并
        // 与数组下标合并判否——语义见 ConfigFile 文档合并节）
        (_, over) => over,
    }
}

/// `toml::Value` → 统一值面转换：Datetime/非有限浮点在值面无表示**显式报错**
/// （禁静默降级成 null/私有标记映射；语义见 [`ConfigFile`] 文档 TOML 面节）。
#[cfg(feature = "config-toml")]
fn toml_value_to_json(value: toml::Value) -> Result<serde_json::Value, ConfigError> {
    let unsupported = |what: &'static str| ConfigError::Parse {
        key: "(toml)".to_string(),
        source: Box::new(TomlValueUnsupported(what)),
    };
    Ok(match value {
        toml::Value::String(s) => serde_json::Value::String(s),
        toml::Value::Integer(i) => serde_json::Value::Number(i.into()),
        toml::Value::Float(f) => match serde_json::Number::from_f64(f) {
            Some(n) => serde_json::Value::Number(n),
            None => return Err(unsupported("non-finite float")),
        },
        toml::Value::Boolean(b) => serde_json::Value::Bool(b),
        toml::Value::Datetime(_) => return Err(unsupported("datetime")),
        toml::Value::Array(a) => serde_json::Value::Array(
            a.into_iter()
                .map(toml_value_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        toml::Value::Table(t) => serde_json::Value::Object(
            t.into_iter()
                .map(|(k, v)| toml_value_to_json(v).map(|j| (k, j)))
                .collect::<Result<serde_json::Map<String, _>, _>>()?,
        ),
    })
}

/// TOML 值在统一值面无表示的拒绝错误源（source 链人读文案；`0`=值类别名）。
#[cfg(feature = "config-toml")]
#[derive(Debug)]
struct TomlValueUnsupported(&'static str);

#[cfg(feature = "config-toml")]
impl fmt::Display for TomlValueUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "toml {} has no unified layer-face representation; \
             use the typed load_toml face for such fields",
            self.0
        )
    }
}

#[cfg(feature = "config-toml")]
impl std::error::Error for TomlValueUnsupported {}

#[cfg(test)]
mod tests {
    // 分层/合并/TOML 翻译测试均在下方 feature 门控子模块；glob 随 config-json
    // 门开——config-toml 隐含 config-json，两子模块经此取 ConfigFile/ConfigError。
    #[cfg(feature = "config-json")]
    use super::*;

    // ── ConfigFile 分层装配（config-json：纯值面，零 IO；IO 面在 tests/config.rs）──

    #[cfg(feature = "config-json")]
    mod cfgfile {
        use super::*;
        use serde::{Deserialize, Serialize};
        use serde_json::json;

        #[derive(Debug, PartialEq, Serialize, Deserialize)]
        struct App {
            host: String,
            port: u16,
        }

        #[test]
        fn layering_higher_layer_wins_same_layer_later_wins() {
            let cfg: App = ConfigFile::new()
                .default_layer(&App {
                    host: "base".to_string(),
                    port: 1,
                })
                .unwrap()
                .layer(json!({ "port": 2 }))
                .layer(json!({ "port": 3, "host": "over" }))
                .load()
                .unwrap();
            assert_eq!(
                cfg,
                App {
                    host: "over".to_string(),
                    port: 3,
                },
                "高层面覆盖低层；同层内后叠赢"
            );
        }

        #[test]
        fn merge_object_recursive_scalar_array_null_replace() {
            let v: serde_json::Value = ConfigFile::new()
                .layer(json!({
                    "db": { "host": "a", "port": 1 },
                    "tags": [1, 2],
                    "opt": "kept",
                }))
                .layer(json!({
                    "db": { "port": 5432 },
                    "tags": [9],
                    "opt": null,
                }))
                .load()
                .unwrap();
            assert_eq!(
                v,
                json!({
                    "db": { "host": "a", "port": 5432 }, // 对象∩对象：递归合并
                    "tags": [9],                        // 数组：整体替换（不按下标）
                    "opt": null,                        // null：显式值（不吞并）
                })
            );
        }

        #[test]
        fn missing_file_is_file_error_with_path_and_source() {
            let err = ConfigFile::<App>::new()
                .file_layer("definitely/missing/config.json")
                .unwrap_err();
            match &err {
                ConfigError::File { path, source } => {
                    assert!(path.ends_with("config.json"), "{path:?}");
                    assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
                }
                other => panic!("期望 File，得 {other:?}"),
            }
            assert!(err.to_string().starts_with("config file "));
            assert!(std::error::Error::source(&err).is_some());
        }

        #[test]
        fn load_deserialize_error_is_parse_with_root_key() {
            let got: Result<App, ConfigError> =
                ConfigFile::new().layer(json!({ "port": 1 })).load();
            let err = got.unwrap_err();
            match &err {
                ConfigError::Parse { key, source } => {
                    assert_eq!(key, "(config)");
                    assert!(
                        source.to_string().contains("host"),
                        "serde 文案含缺失字段名：{source}"
                    );
                }
                other => panic!("期望 Parse，得 {other:?}"),
            }
        }

        #[test]
        fn save_serialize_failure_is_serialize_error_with_source() {
            // 元组键映射：serde_json 序列化必败「key must be a string」（实测
            // 钉死；整数键会被静默字符串化、非有限浮点会静默写 null，均不可
            // 作注入面）——失败在原子写之前，零落盘零 IO。
            #[derive(Serialize)]
            struct WithTupleKeyMap {
                m: std::collections::BTreeMap<(u32, u32), u32>,
            }
            let err = ConfigFile::<WithTupleKeyMap>::save(
                &WithTupleKeyMap {
                    m: [((1, 2), 3)].into_iter().collect(),
                },
                "unused-path.json",
            )
            .unwrap_err();
            match &err {
                ConfigError::Serialize { .. } => {}
                other => panic!("期望 Serialize，得 {other:?}"),
            }
            assert!(err.to_string().starts_with("serialize config: "));
            assert!(std::error::Error::source(&err).is_some());
        }
    }

    // ── ConfigFile TOML 翻译面（config-toml：纯转换，零 IO；IO 面在 tests/config.rs）──

    #[cfg(feature = "config-toml")]
    mod cfgtoml {
        use super::*;

        #[test]
        fn toml_value_to_json_rejects_datetime_fail_visible() {
            let dt: toml::value::Datetime = "1979-05-27T07:32:00Z".parse().unwrap();
            let err = toml_value_to_json(toml::Value::Datetime(dt)).unwrap_err();
            match &err {
                ConfigError::Parse { key, source } => {
                    assert_eq!(key, "(toml)");
                    assert!(source.to_string().contains("datetime"), "{source}");
                }
                other => panic!("期望 Parse，得 {other:?}"),
            }
        }

        #[test]
        fn toml_value_to_json_converts_scalars_arrays_tables() {
            let v: toml::Value =
                toml::from_str("a = 1\nb = \"x\"\nc = [1, 2]\n[d]\ne = true").expect("合法 TOML");
            let j = toml_value_to_json(v).unwrap();
            assert_eq!(j["a"], 1);
            assert_eq!(j["b"], "x");
            assert_eq!(j["c"], serde_json::json!([1, 2]));
            assert_eq!(j["d"]["e"], true);
        }
    }
}
