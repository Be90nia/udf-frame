//! 路径词法归一、相对化、containment 与家目录/XDG 基目录解析（零 IO 面）。
//!
//! 常驻零依赖件：纯 std，无 feature 门。事故锚 F19（std `canonicalize` Windows
//! 返回 `\\?\` verbatim，跨程序边界不兼容）+ F21（`..` 词法归一越根绕过白名单
//! containment）。设计纪律：无 IO 归一名带 `lexical`、带 IO 归一名带
//! `canonicalize`，禁隐式择一；containment 判定必须过 [`simplify`]（verbatim
//! 形式直比较=复现 F19）；相对化错误全显式（pathdiff 的 `Option` 静默坑收编为
//! 枚举错误）。
//!
//! 语义依据：MSDN《Naming Files, Paths, and Namespaces》+ std::path 文档
//! （T1）；verbatim 剥离算法对齐 dunce 1.0.5（VerbatimDisk-only + 文件名
//! 合法性 + DOS 保留名 + 260 UTF-16 上限四道闸；保留名覆盖上标 ¹²³ 变体，
//! 为 dunce 的有据超集——MSDN 明示 `COM¹` 属设备保留名）。
//!
//! 性能七维收窄声明（CPU 纯函数域，批 D 先例）：适用维度=①分配声明（
//! [`simplify`] 零分配恒返借用；归一/相对化语义必然分配，如实报数）②单次
//! ns 锚③输入规模梯度 16/128/4096（单线程域，以梯度代线程曲线）；④并发
//! 曲线⑤锁⑥内存水位⑦伪共享=n/a（无锁无线程面）。采集器
//! `examples/paths_multidim.rs`，报告：paths 域性能锚（本地留存，未入库）。

use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};

/// containment 判定的大小写策略（显式参数，禁静默默认）。
///
/// Windows 文件系统默认大小写不敏感（NTFS 可按目录开 POSIX 严格语义，MSDN
/// 「Do not assume case sensitivity」）——策略必须由调用方按真实文件系统
/// 语义拍板。`CaseInsensitive` 为 ASCII 折叠（非 ASCII Unicode 折叠不在此
/// 面，如实声明上限）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseStrategy {
    /// 逐组件严格比较（std 组件级 Eq：盘符大小写本就不敏感，其余敏感）。
    CaseSensitive,
    /// 逐组件 ASCII 折叠比较（近似 NTFS 默认语义）。
    CaseInsensitive,
}

/// 相对化失败（错误全显式，禁 pathdiff 式 `Option` 静默）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelError {
    /// 一方绝对一方相对（pathdiff 同形场景返回 `None`/绝对路径原样吞掉的静默坑 → 显式）。
    MismatchedKind {
        /// 被相对化的目标路径。
        path: PathBuf,
        /// 基准目录路径。
        base: PathBuf,
    },
    /// 双方均带 Windows 前缀但形制不同（盘符不同 / verbatim 剥离后仍不同 /
    /// 盘符 vs UNC）。直比会产出 pathdiff 式 `..\..\` 垃圾 → 显式拒绝。
    PrefixKindMismatch {
        /// 被相对化的目标路径。
        path: PathBuf,
        /// 基准目录路径。
        base: PathBuf,
    },
    /// 归一+简化后 `base` 仍不是 `path` 的组件前缀。
    NotPrefix {
        /// 被相对化的目标路径。
        path: PathBuf,
        /// 基准目录路径。
        base: PathBuf,
    },
}

impl std::fmt::Display for RelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RelError::MismatchedKind { path, base } => {
                write!(f, "cannot relativize: path `{path:?}` and base `{base:?}` differ in absolute/relative kind")
            }
            RelError::PrefixKindMismatch { path, base } => {
                write!(f, "cannot relativize: path `{path:?}` and base `{base:?}` have different Windows prefixes")
            }
            RelError::NotPrefix { path, base } => {
                write!(
                    f,
                    "base `{base:?}` is not a component prefix of path `{path:?}`"
                )
            }
        }
    }
}

impl std::error::Error for RelError {}

/// containment 判定失败（回传两侧归一后的路径便于诊断，禁只给 bool）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotInside {
    /// 归一+简化后的基准目录。
    pub base: PathBuf,
    /// 归一+简化后被判定的路径。
    pub path: PathBuf,
}

impl std::fmt::Display for NotInside {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "path `{path:?}` is not inside base `{base:?}`",
            base = self.base,
            path = self.path
        )
    }
}

impl std::error::Error for NotInside {}

/// 纯文本词法归一：解 `.`、成对消解 `..`、统一平台分隔符、去重复/尾部分隔符。
///
/// 保守语义（F21 装甲）：
/// - 绝对根处的 `..` 被吸收（POSIX `/..`≡`/`；Windows `GetFullPathNameW` 同语义）——
///   归一结果与 OS 实际解析一致，白名单 containment 不被文本花招绕过；
/// - 相对路径顶部的 `..` 保留（无基准无法解析，不臆造）；裸盘符（`C:..`）
///   后的 `..` 保留（依赖 drive-cwd，不可词法解析）；
/// - **verbatim/设备路径原样返回**：`\\?\` 前缀关闭 OS 的字符串解析（MSDN），
///   其中 `.`/`..` 是字面文件名，文本归一会改语义。
///
/// 分配如实申报（事件口径，`tests/paths_alloc.rs` 逐字钉死）：常规路径 2 次
/// （组件栈 `Vec` + 输出 `PathBuf` 各 1，`.`→`.` 标记同）；verbatim 透传 /
/// 空输入各 1 次（`to_path_buf` / 组件栈容量预留）。输入已归一时无更廉价
/// 快路径。
///
/// # Examples
///
/// ```
/// use std_ext::paths::normalize_lexical;
/// use std::path::Path;
///
/// let n = normalize_lexical(Path::new("a/./b/../c"));
/// assert_eq!(n.components().count(), 2); // a, c（分隔符形态平台各自合法）
/// assert_eq!(normalize_lexical(Path::new(".")), Path::new("."));
/// ```
pub fn normalize_lexical(path: &Path) -> PathBuf {
    if is_verbatim_like(path) {
        return path.to_path_buf();
    }
    let mut kept: Vec<Component> = Vec::with_capacity(path.as_os_str().len() / 2 + 1);
    for comp in path.components() {
        match comp {
            // `.` 全解（含首段 CurDir：可比较的规范形不留 `.`）
            Component::CurDir => {}
            Component::ParentDir => match kept.last() {
                // 成对消解
                Some(Component::Normal(_)) => {
                    kept.pop();
                }
                // 绝对根处吸收：POSIX `/..`≡`/`；Windows GetFullPathNameW 同语义——
                // 归一结果与 OS 实际解析一致，文本花招不产生越根形态（F21）
                Some(Component::RootDir) => {}
                // 相对顶部/裸盘符（drive-cwd 依赖）无法词法解析 → 保留，不臆造
                _ => kept.push(Component::ParentDir),
            },
            other => kept.push(other),
        }
    }
    if kept.is_empty() {
        // 非空输入归一后无剩余组件 → 当前目录标记（"." 比 "" 可诊断）；
        // 空输入保持空（与 std 语义一致）
        if path.as_os_str().is_empty() {
            return PathBuf::new();
        }
        return PathBuf::from(".");
    }
    kept.iter().map(|c| c.as_os_str()).collect()
}

/// verbatim/设备前缀检测：这些命名空间关闭 OS 的字符串解析（MSDN），其中
/// `.`/`..` 是字面文件名，文本归一会改语义。
fn is_verbatim_like(path: &Path) -> bool {
    use std::path::Prefix;
    matches!(
        path.components().next(),
        Some(Component::Prefix(p))
            if matches!(p.kind(), Prefix::Verbatim(_) | Prefix::VerbatimDisk(_) | Prefix::VerbatimUNC(..) | Prefix::DeviceNS(_))
    )
}

/// verbatim/UNC → legacy 形态：仅在可无歧义剥离时剥 `\\?\` 盘符前缀，否则
/// 原样返回（非 Windows 恒等）。
///
/// dunce 1.0.5 同语义四道闸（逐道对应测试见坑面对照表）：
/// 1. 仅 `Prefix::VerbatimDisk` 参与剥离（VerbatimUNC/Device/GLOBALROOT 保留）；
/// 2. 剥离后逐组件须为合法 legacy 文件名（无保留字符/控制符、不以 `.` 或
///    空格结尾、UTF-16 ≤255）；
/// 3. 非 DOS 保留设备名（CON/PRN/AUX/NUL/COM1-9/LPT1-9 及其上标 ¹²³ 变体、
///    含 `con.txt` 式扩展名等价形）；
/// 4. 整路径 UTF-16 ≤260（legacy MAX_PATH；超长保留 verbatim 才仍可访问）。
///
/// verbatim 内的 `.`/`..` 是字面名 → 不可剥离（剥了改语义），原样返回。
///
/// 热路径零分配：不可剥离时返回输入借用切片；可剥离时返回输入内的子切片。
/// 保留名判定同为零分配（ASCII 折叠逐字节比；`tests/paths_alloc.rs` 事件
/// 口径背书）。
///
/// # Examples
///
/// ```
/// use std_ext::paths::simplify;
/// use std::path::Path;
///
/// assert_eq!(simplify(Path::new("a/b")), Path::new("a/b")); // 非 Windows 与常规路径恒等
/// ```
#[cfg(windows)]
pub fn simplify(path: &Path) -> &Path {
    // dunce 1.0.5 同形：`str::get(4..)` 字节切片剥 `\\?\`（UTF-8 边界安全），
    // 保留 `C:` 盘符段；非 UTF-8 可逆表示的路径不剥（OsStr 无法无损操作）
    if is_safe_to_strip(path) {
        path.to_str()
            .and_then(|s| s.get(4..))
            .map_or(path, Path::new)
    } else {
        path
    }
}

#[cfg(not(windows))]
pub fn simplify(path: &Path) -> &Path {
    path
}

/// `std::fs::canonicalize` + [`simplify`] 合体（F19 直击）：解析符号链接与
/// 真实路径后，Windows 侧剥去可剥离的 `\\?\` verbatim 前缀。
///
/// IO 错误（路径不存在、非最终组件非目录——std T1 Errors 节）原样透传，
/// 禁吞禁包装。非 Windows 即 `std::fs::canonicalize` 本体。
///
/// # Examples
///
/// ```
/// use std_ext::paths::canonicalize_compat;
/// use std::path::Path;
///
/// let c = canonicalize_compat(Path::new(".")).unwrap();
/// assert!(c.is_absolute());
/// ```
pub fn canonicalize_compat(path: &Path) -> std::io::Result<PathBuf> {
    #[cfg(windows)]
    {
        std::fs::canonicalize(path).map(|p| simplify(&p).to_owned())
    }
    #[cfg(not(windows))]
    {
        std::fs::canonicalize(path)
    }
}

/// 相对化：`path` 相对 `base` 的组件级相对路径（pathdiff 的 `..` 回填形）。
///
/// 参数序对齐 pathdiff::diff_paths 生态惯例：**目标在前、基准在后**
///（`relative_to(path, base)` 读作「path 相对 base」）。
///
/// 流水线：两侧先 [`normalize_lexical`]（`..`/`.` 消解，混合分隔符归一），
/// 再 [`simplify`]（verbatim 剥离使 F19 形态可与 legacy 基准比较），然后组
/// 件级前缀匹配。失败模式全显式（见 [`RelError`]）：类型混杂、Windows 前
/// 缀形制不同、非前缀——pathdiff 的 `Option` 静默坑逐项收编。
///
/// `path == base` → `Ok(".")`（pathdiff 返回空串的坑位显式为当前目录标记）。
///
/// # Examples
///
/// ```
/// use std_ext::paths::relative_to;
/// use std::path::Path;
///
/// assert_eq!(relative_to(Path::new("a/b/c/d"), Path::new("a/b")).unwrap(), Path::new("c/d"));
/// assert_eq!(relative_to(Path::new("a/b"), Path::new("a/b")).unwrap(), Path::new("."));
/// ```
pub fn relative_to(path: &Path, base: &Path) -> Result<PathBuf, RelError> {
    let base = normalize_lexical(base);
    let path = normalize_lexical(path);
    // simplify 先行：F19 形态（verbatim）须能与 legacy 基准比较
    let base = simplify(&base);
    let path = simplify(&path);
    let path_abs = path.is_absolute();
    let base_abs = base.is_absolute();
    if path_abs != base_abs {
        return Err(RelError::MismatchedKind {
            path: path.to_path_buf(),
            base: base.to_path_buf(),
        });
    }
    // 双前缀形制比对：不同盘符/剥不掉的 verbatim vs legacy 直比会产出
    // pathdiff 式 `..\..\` 垃圾 → 显式拒绝（std 前缀组件 Eq 对盘符大小写
    // 不敏感——std::path 文档明示的唯一例外；其余 kind 严格区分）
    match (first_prefix(path), first_prefix(base)) {
        (Some(a), Some(b)) if a != b => {
            return Err(RelError::PrefixKindMismatch {
                path: path.to_path_buf(),
                base: base.to_path_buf(),
            });
        }
        _ => {}
    }
    match path.strip_prefix(base) {
        Ok(rel) if rel.as_os_str().is_empty() => Ok(PathBuf::from(".")),
        Ok(rel) => Ok(rel.to_path_buf()),
        Err(_) => Err(RelError::NotPrefix {
            path: path.to_path_buf(),
            base: base.to_path_buf(),
        }),
    }
}

fn first_prefix(path: &Path) -> Option<Component<'_>> {
    match path.components().next() {
        Some(p @ Component::Prefix(_)) => Some(p),
        _ => None,
    }
}

/// 白名单 containment 判定：`path` 是否位于 `base` 之内（含相等）。
///
/// 判定前两侧过 [`normalize_lexical`]+[`simplify`]：verbatim 形式直比较=
/// 复现 F19；`..` 逃逸（`a/../b`）在归一后现形=F21 装甲。大小写策略显式
/// 参数（MSDN：「Do not assume case sensitivity」）。
///
/// 相对/绝对类型混杂按 NotInside 自然失败（相对路径不可能在绝对基准内）。
///
/// # Examples
///
/// ```
/// use std_ext::paths::{ensure_inside, CaseStrategy};
/// use std::path::Path;
///
/// assert!(ensure_inside(Path::new("a/b"), Path::new("a/b/c"), CaseStrategy::CaseSensitive).is_ok());
/// assert!(ensure_inside(Path::new("a/b"), Path::new("a/c"), CaseStrategy::CaseSensitive).is_err());
/// ```
pub fn ensure_inside(base: &Path, path: &Path, strategy: CaseStrategy) -> Result<(), NotInside> {
    let base_nb = normalize_lexical(base);
    let path_nb = normalize_lexical(path);
    let base = simplify(&base_nb);
    let path = simplify(&path_nb);
    let inside = match strategy {
        CaseStrategy::CaseSensitive => path.strip_prefix(base).is_ok(),
        CaseStrategy::CaseInsensitive => fold_starts_with(path, base),
    };
    if inside {
        Ok(())
    } else {
        Err(NotInside {
            base: base.to_path_buf(),
            path: path.to_path_buf(),
        })
    }
}

/// 组件级 ASCII 折叠前缀匹配（近似 NTFS 默认语义；前缀组件折叠覆盖盘符）。
fn fold_starts_with(path: &Path, base: &Path) -> bool {
    let mut comps = path.components();
    for bc in base.components() {
        match comps.next() {
            Some(pc) => {
                let matched = match (pc, bc) {
                    (Component::Normal(_), Component::Normal(_)) => a_fold_eq(pc, bc),
                    (Component::Prefix(_), Component::Prefix(_)) => a_fold_eq(pc, bc),
                    _ => pc == bc,
                };
                if !matched {
                    return false;
                }
            }
            None => return false,
        }
    }
    true
}

fn a_fold_eq(a: Component<'_>, b: Component<'_>) -> bool {
    a.as_os_str()
        .to_string_lossy()
        .eq_ignore_ascii_case(&b.as_os_str().to_string_lossy())
}

// ===== 家目录与 XDG 用户基目录（env 解析面，零 IO——不创建目录） =====

/// 当前用户家目录：[`std::env::home_dir`] 直包（薄封装，域语义与 None 处置
/// 指引文档化）。纯解析面——返回路径不保证存在，存在性/创建归调用方 fs 面。
///
/// # None 面（std T1 文档，1.85.0 恢复非废弃）
///
/// - **Unix**：`$HOME`（设且非空）→ `getpwuid_r`（home 字段为空串也算有效
///   值）；None 仅当 passwd 无当前用户条目。
/// - **Windows**：`$USERPROFILE`（设且非空）→ `GetUserProfileDirectory`
///   （std 声明该回退将来可能变化）；UWP 目标恒 None。1.85 前此函数在
///   Windows 读 `$HOME`（Cygwin/MinGW 环境会给 `/home/you` 类非标准形态）。
///
/// # 调用方处置指引（None 时）
///
/// None = 家不可解析。**禁静默回退 cwd**（跨启动漂移、权限面不明、多进程
/// 各落各的）——log 域先例 [`crate::logx::DefaultPathResolver`] 在构造期
/// 显式 `Err`。需要 XDG 基目录的使用方走 [`config_dir`]/[`cache_dir`]/
/// [`data_dir`]（None 语义同此，链式传播）。
///
/// 分配声明（事件口径，`tests/paths_alloc.rs` 逐字钉死）：env 优先臂
/// Unix 1 次（var_os 字节直拷 OsString；`PathBuf::from` 移动零增）／
/// Windows 2 次（env 块 UTF-16→WTF-8 转换多一次）；`getpwuid_r`/
/// `GetUserProfileDirectory` 回退臂 std 内部另有瞬时分配，不在本锚面。
///
/// # Examples
///
/// ```
/// use std_ext::paths::home_dir;
///
/// // 家可能不可解析（passwd 无条目/UWP）——按 None 分支处置，禁静默回退 cwd
/// if let Some(home) = home_dir() {
///     let dot_cfg = home.join(".config"); // 子目录布局归调用方
///     println!("{dot_cfg:?}");
/// }
/// ```
pub fn home_dir() -> Option<PathBuf> {
    std::env::home_dir()
}

/// XDG 用户基目录解析核（纯函数，无 env 访问——单测全矩阵零 env 竞争）。
///
/// 语义（XDG Base Directory Spec v0.8 原文锚定）：
/// - `home` 缺失 ⇒ `None`（链断传播——契约拍板比 spec 更保守的 fail-closed：
///   家锚缺失不出路径；spec 仅在回退默认值时才需要 `$HOME`）；
/// - env 值**空 = 未设**（"either not set or empty"）；
/// - **相对值 = invalid，忽略**回退默认（"All paths ... must be absolute ...
///   relative path ... invalid and ignore it"）；
/// - 结果过 [`normalize_lexical`]（尾/重复分隔符、`.`、`..` 词法归一，与 OS
///   解析一致，F21 同语义；不含 IO，符号链接不解析——需真实路径走
///   [`canonicalize_compat`]）。
///
/// 分配声明（事件口径，`tests/paths_alloc.rs` 逐字钉死）：XDG 绝对值臂
/// 3 次（env OsString 1 + normalize_lexical 2）；默认臂 4 次（home 1 +
/// join 1 + normalize 2）；Windows 面恒零（恒返 None，入参不求值）。
#[cfg(not(windows))]
fn resolve_xdg_dir(
    home: Option<&Path>,
    env_value: Option<&OsStr>,
    default_suffix: &str,
) -> Option<PathBuf> {
    let home = home?;
    let from_env = env_value
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .filter(|p| p.is_absolute());
    let base = from_env.unwrap_or_else(|| home.join(default_suffix));
    Some(normalize_lexical(&base))
}

/// [`resolve_xdg_dir`] 的真 env 面。Windows 面恒 `None`（拍板：XDG 是 Unix
/// 约定，Windows 原生 Known Folders 面需 FFI + ADR 另立项——调用方按 None
/// 分支处置，勿静默回退）。
#[cfg(not(windows))]
fn xdg_base_dir(var: &str, default_suffix: &str) -> Option<PathBuf> {
    resolve_xdg_dir(
        home_dir().as_deref(),
        std::env::var_os(var).as_deref(),
        default_suffix,
    )
}

#[cfg(windows)]
fn xdg_base_dir(_var: &str, _default_suffix: &str) -> Option<PathBuf> {
    None
}

/// 用户配置基目录：`$XDG_CONFIG_HOME`（绝对）→ `$HOME/.config`（spec v0.8）。
///
/// 纯解析面，**不创建目录**——spec 把创建责任放在写入时（目的地不存在由
/// 写入方尝试以 0700 创建），本域只回答路径。语义与 None 面同
/// [`resolve_xdg_dir`]（家不可解析 ⇒ None；env 空串=未设、相对值=invalid
/// 忽略；结果词法归一）。macOS 走本 XDG 面（std-only 约束，不引
/// CoreFoundation FFI；XDG 变量在 macOS 亦被 git/cargo 等广泛尊重）。
///
/// Windows 面恒 [`None`]（见 [`xdg_base_dir`] 拍板注）。
///
/// # Examples
///
/// ```
/// use std_ext::paths::config_dir;
///
/// if let Some(dir) = config_dir() {
///     let app_cfg = dir.join("myapp"); // 子目录创建归调用方 fs 面
///     println!("{app_cfg:?}");
/// }
/// ```
pub fn config_dir() -> Option<PathBuf> {
    xdg_base_dir("XDG_CONFIG_HOME", ".config")
}

/// 用户缓存基目录：`$XDG_CACHE_HOME`（绝对）→ `$HOME/.cache`（spec v0.8）。
/// 语义同 [`config_dir`]；承载非必要数据——调用方可更宽松地整目录丢弃。
///
/// # Examples
///
/// ```
/// use std_ext::paths::cache_dir;
///
/// if let Some(dir) = cache_dir() {
///     println!("{:?}", dir.join("myapp"));
/// }
/// ```
pub fn cache_dir() -> Option<PathBuf> {
    xdg_base_dir("XDG_CACHE_HOME", ".cache")
}

/// 用户数据基目录：`$XDG_DATA_HOME`（绝对）→ `$HOME/.local/share`
/// （spec v0.8）。语义同 [`config_dir`]；承载应跨重启持久的数据。
/// `$XDG_DATA_DIRS` 系统检索链不在本域——那是读侧多目录搜索面。
///
/// # Examples
///
/// ```
/// use std_ext::paths::data_dir;
///
/// if let Some(dir) = data_dir() {
///     println!("{:?}", dir.join("myapp"));
/// }
/// ```
pub fn data_dir() -> Option<PathBuf> {
    xdg_base_dir("XDG_DATA_HOME", ".local/share")
}

// ===== 内部实现（Windows 侧 verbatim 解析，dunce 1.0.5 同形） =====

/// 剥离安全性判定（dunce `is_safe_to_strip_unc` 同形四道闸，逐闸有测试钉死）：
/// 仅 VerbatimDisk 参与、组件须为合法 legacy 文件名、非 DOS 保留设备名
/// （含上标 ¹²³ 变体的有据超集）、整路径 UTF-16 ≤260。
#[cfg(windows)]
fn is_safe_to_strip(path: &Path) -> bool {
    use std::path::Prefix;

    let mut components = path.components();
    match components.next() {
        // 闸 1：只有 verbatim 盘符路径可剥；VerbatimUNC/DeviceNS/GLOBALROOT/
        // 其他前缀保留原样
        Some(Component::Prefix(p)) if matches!(p.kind(), Prefix::VerbatimDisk(_)) => {}
        _ => return false,
    }
    for comp in components {
        match comp {
            Component::RootDir => {}
            Component::Normal(name) => {
                // 闸 2+3：legacy 合法名且非保留设备名——保留名/非法名下剥前缀
                // 会被 legacy API 重新解释（设备语义/尾点尾空格截断）
                if !is_valid_legacy_filename(name) || is_reserved_device_name(name) {
                    return false;
                }
            }
            // 闸 1 补充：verbatim 内 `.`/`..` 是字面名，剥前缀改语义
            _ => return false,
        }
    }
    // 闸 4：全路径 >260 UTF-16 超 legacy MAX_PATH，保留 verbatim 才仍可访问
    //（dunce 1.0.5 按剥前全路径计，保守侧）
    if utf16_len(path.as_os_str()) > 260 {
        return false;
    }
    true
}

/// legacy 单组件合法性（MSDN 命名规则 ASCII 子集）：非空、UTF-16 ≤255、
/// 无保留字符/控制符（含 NUL）、不以 `.` 或空格结尾。非 UTF-8/WTF-8 可逆
/// 表示的名称不参与剥离（Rust OsStr 无法无损操作，dunce 同判）。
#[cfg(windows)]
fn is_valid_legacy_filename(name: &OsStr) -> bool {
    let Some(s) = name.to_str() else {
        return false;
    };
    if s.is_empty() || utf16_len(name) > 255 {
        return false;
    }
    // 保留字符 < > : " / \ | ? * 与 1-31 控制符、NUL（MSDN 保留字符表）
    if s.bytes().any(|b| {
        matches!(
            b,
            0..=31 | b'<' | b'>' | b':' | b'"' | b'/' | b'\\' | b'|' | b'?' | b'*'
        )
    }) {
        return false;
    }
    // 尾点/尾空格：shell 与 UI 层会截断（MSDN「Do not end ... with a space or period」）
    !matches!(s.as_bytes().last(), Some(b' ' | b'.'))
}

/// DOS 保留设备名判定（MSDN 保留名表 + 「紧跟扩展名同保留」规则）：
/// CON/PRN/AUX/NUL + COM/LPT±单数字位（1-9 及上标 ¹²³——MSDN 明示 8 位
/// ISO 8859-1 上标数字被识别为设备名，dunce 1.0.5 的 ASCII 子集不覆盖，
/// 此处有据超集）。
#[cfg(windows)]
fn is_reserved_device_name(name: &OsStr) -> bool {
    let Some(stem) = Path::new(name).file_stem().and_then(OsStr::to_str) else {
        return false;
    };
    // 首段命中（dunce master split 语义，补 1.0.5 漏的 NUL.tar.gz 形）：
    // "NUL.tar.gz" → stem "NUL.tar" → 首段 "NUL"
    let head = stem.split('.').next().unwrap_or("");
    // 尾点/尾空格同裁（dunce right_trim 语义："con . " → "con"）
    let trimmed = head.trim_end_matches([' ', '.']);
    // 零分配 ASCII 折叠（热路径，simplify 逐组件过此处；原 to_ascii_uppercase
    // 每组件 2 次瞬时 String，证伪 doc「热路径零分配」——B1 盲评卡点）：
    if ["CON", "PRN", "AUX", "NUL"]
        .iter()
        .any(|k| trimmed.eq_ignore_ascii_case(k))
    {
        return true;
    }
    // COM/LPT±单数字位：前缀恒 ASCII（命中即字符边界，`&s[3..]` 切片安全）；
    // 后缀含非 ASCII 上标 ¹²³，ASCII 折叠本不触及 → 精确匹配与原实现逐字同语义
    let b = trimmed.as_bytes();
    if b.len() >= 3 && (b[..3].eq_ignore_ascii_case(b"COM") || b[..3].eq_ignore_ascii_case(b"LPT"))
    {
        return matches!(
            &trimmed[3..],
            "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
        );
    }
    false
}

#[cfg(windows)]
fn utf16_len(s: &OsStr) -> usize {
    use std::os::windows::ffi::OsStrExt;
    s.encode_wide().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // ---- normalize_lexical ----

    #[test]
    fn resolve_dots_double_seps_and_trailing() {
        assert_eq!(
            normalize_lexical(Path::new("a/./b//c/")),
            Path::new("a/b/c")
        );
    }

    #[test]
    fn drops_leading_curdir() {
        assert_eq!(normalize_lexical(Path::new("./a")), Path::new("a"));
    }

    #[test]
    fn dot_identity_preserved() {
        assert_eq!(normalize_lexical(Path::new(".")), Path::new("."));
    }

    #[test]
    fn empty_unchanged() {
        assert_eq!(normalize_lexical(Path::new("")), Path::new(""));
    }

    #[test]
    fn parent_pops_within_relative() {
        assert_eq!(normalize_lexical(Path::new("a/b/../c")), Path::new("a/c"));
    }

    #[test]
    fn parent_beyond_relative_top_kept() {
        // F21：不臆造，无法词法解析的 `..` 保留
        assert_eq!(normalize_lexical(Path::new("a/../../b")), Path::new("../b"));
        assert_eq!(normalize_lexical(Path::new("../..")), Path::new("../.."));
    }

    #[test]
    fn absolute_root_absorbs_parent() {
        // POSIX `/..`≡`/`；Windows GetFullPathNameW 同语义——归一与 OS 一致
        assert_eq!(normalize_lexical(Path::new("/..")), Path::new("/"));
        assert_eq!(normalize_lexical(Path::new("/a/../..")), Path::new("/"));
    }

    #[test]
    fn normal_case_preserved() {
        assert_eq!(normalize_lexical(Path::new("A/B")), Path::new("A/B"));
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_passthrough_literal_dots() {
        // verbatim 内 `.`/`..` 是字面名，归一改语义 → 原样返回
        assert_eq!(
            normalize_lexical(Path::new(r"\\?\C:\a\.\b")),
            Path::new(r"\\?\C:\a\.\b")
        );
        assert_eq!(
            normalize_lexical(Path::new(r"\\?\C:\a\..\b")),
            Path::new(r"\\?\C:\a\..\b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn device_namespace_passthrough() {
        assert_eq!(
            normalize_lexical(Path::new(r"\\.\C:\a\..\b")),
            Path::new(r"\\.\C:\a\..\b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn unc_nonverbatim_normalizes() {
        assert_eq!(
            normalize_lexical(Path::new(r"\\srv\share\a\..\b")),
            Path::new(r"\\srv\share\b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn drive_relative_parent_kept() {
        // `C:..` 依赖 drive-cwd，不可词法解析 → 保留且往返稳定
        assert_eq!(
            normalize_lexical(Path::new(r"C:..\x")),
            Path::new(r"C:..\x")
        );
    }

    #[cfg(windows)]
    #[test]
    fn forward_slash_unified() {
        assert_eq!(
            normalize_lexical(Path::new(r"C:/a//b/.")),
            Path::new(r"C:\a\b")
        );
        assert_eq!(normalize_lexical(Path::new(r"C:\")), Path::new(r"C:\"));
    }

    #[test]
    fn idempotence_over_weird_corpus() {
        let corpus = [
            "",
            ".",
            "./",
            "..",
            "../..",
            "a/..",
            "a/../..",
            "/..",
            "/a/../..",
            "a/./b//c/",
            "a/b/../../../../../../c",
            "///",
            "/a/",
            "x/../y/./z/..",
        ];
        for input in corpus {
            let once = normalize_lexical(Path::new(input));
            let twice = normalize_lexical(&once);
            assert_eq!(once, twice, "not idempotent for {input:?}");
        }
    }

    // ---- simplify ----

    #[test]
    fn relative_paths_unchanged() {
        assert_eq!(simplify(Path::new("a/b")), Path::new("a/b"));
        assert_eq!(simplify(Path::new("/a/b")), Path::new("/a/b"));
    }

    #[cfg(windows)]
    #[test]
    fn legacy_windows_unchanged_same_slice() {
        let p = Path::new(r"C:\foo\bar");
        // 热路径零分配证明：不可剥离时返回同一借用切片
        assert!(std::ptr::eq(simplify(p).as_os_str(), p.as_os_str()));
    }

    #[cfg(windows)]
    #[test]
    fn strips_verbatim_disk() {
        assert_eq!(
            simplify(Path::new(r"\\?\C:\foo\bar")),
            Path::new(r"C:\foo\bar")
        );
        assert_eq!(simplify(Path::new(r"\\?\c:\foo")), Path::new(r"c:\foo"));
        assert_eq!(simplify(Path::new(r"\\?\C:\")), Path::new(r"C:\"));
    }

    #[cfg(windows)]
    #[test]
    fn keeps_verbatim_unc_and_device() {
        assert_eq!(
            simplify(Path::new(r"\\?\UNC\srv\share\f")),
            Path::new(r"\\?\UNC\srv\share\f")
        );
        assert_eq!(
            simplify(Path::new(r"\\.\C:\notdisk")),
            Path::new(r"\\.\C:\notdisk")
        );
        assert_eq!(
            simplify(Path::new(r"\\?\GLOBALROOT\Device\ImDisk0\x")),
            Path::new(r"\\?\GLOBALROOT\Device\ImDisk0\x")
        );
    }

    #[cfg(windows)]
    #[test]
    fn keeps_verbatim_with_literal_dots() {
        // 剥前缀会把字面 `.`/`..` 变成导航语义 → 不可剥离，原样保留
        assert_eq!(
            simplify(Path::new(r"\\?\C:\a\.\b")),
            Path::new(r"\\?\C:\a\.\b")
        );
        assert_eq!(
            simplify(Path::new(r"\\?\C:\a\..\b")),
            Path::new(r"\\?\C:\a\..\b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn keeps_reserved_device_names() {
        for name in [
            r"\\?\C:\COM1",
            r"\\?\C:\con.txt",
            r"\\?\C:\NUL.tar.gz",
            r"\\?\C:\lpt4",
        ] {
            assert_eq!(
                simplify(Path::new(name)),
                Path::new(name),
                "must keep {name}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn keeps_superscript_reserved_names() {
        // MSDN：COM¹/LPT² 等上标变体亦属设备保留名（dunce 只查 ASCII 子集，此处超集）
        for name in [r"\\?\C:\COM¹", r"\\?\C:\lpt³.txt"] {
            assert_eq!(
                simplify(Path::new(name)),
                Path::new(name),
                "must keep {name}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn keeps_invalid_legacy_filenames() {
        for name in [
            r"\\?\C:\a<b",    // 保留字符
            r"\\?\C:\a|b",    // 保留字符
            r"\\?\C:\a.",     // 点结尾
            r"\\?\C:\a ",     // 空格结尾
            r"\\?\C:\a?.txt", // 保留字符
        ] {
            assert_eq!(
                simplify(Path::new(name)),
                Path::new(name),
                "must keep {name}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn keeps_overlong_paths() {
        let ok = "n".repeat(200);
        let stripped = format!(r"\\?\C:\{ok}");
        assert_eq!(
            simplify(Path::new(&stripped)),
            Path::new(&format!(r"C:\{ok}"))
        );
        // >260 UTF-16：legacy MAX_PATH 炸 → 保留 verbatim（dunce 长路径上限）
        let long = "n".repeat(300);
        let kept = format!(r"\\?\C:\{long}");
        assert_eq!(simplify(Path::new(&kept)), Path::new(&kept));
    }

    #[cfg(windows)]
    #[test]
    fn non_unicode_verbatim_kept() {
        use std::os::windows::ffi::OsStringExt;
        // unpaired surrogate：OsStr 无法无损转 str → 保守不剥（dunce doc
        // 「unpaired surrogates aren't converted」同判）
        let wide: Vec<u16> = vec![0x5C, 0x5C, 0x3F, 0x5C, 0x43, 0x3A, 0x5C, 0xDC00];
        let p = std::ffi::OsString::from_wide(&wide);
        assert_eq!(simplify(Path::new(&p)), Path::new(&p));
    }

    // ---- canonicalize_compat / ensure_inside IO 面测试在 tests/paths_io.rs
    //（集成测试；临时目录守卫禁系统 TEMP 红线，随进程清理）----

    // ---- relative_to ----

    #[test]
    fn equal_paths_yield_dot() {
        assert_eq!(
            relative_to(Path::new("a/b"), Path::new("a/b")).unwrap(),
            Path::new(".")
        );
    }

    #[test]
    fn under_base_yields_tail() {
        assert_eq!(
            relative_to(Path::new("a/b/c/d"), Path::new("a/b")).unwrap(),
            Path::new("c/d")
        );
    }

    #[test]
    fn base_deeper_is_not_prefix() {
        assert!(matches!(
            relative_to(Path::new("a"), Path::new("a/b/c")),
            Err(RelError::NotPrefix { .. })
        ));
    }

    #[test]
    fn divergent_sibling_is_not_prefix() {
        // pathdiff 会给 "../c"；本域显式 Err（fail-visible，禁猜公共祖先）
        assert!(matches!(
            relative_to(Path::new("a/c"), Path::new("a/b")),
            Err(RelError::NotPrefix { .. })
        ));
    }

    #[test]
    fn mixed_kind_is_explicit_error() {
        #[cfg(windows)]
        let mixed = relative_to(Path::new("rel/x"), Path::new(r"C:\abs"));
        #[cfg(not(windows))]
        let mixed = relative_to(Path::new("rel/x"), Path::new("/abs"));
        assert!(matches!(mixed, Err(RelError::MismatchedKind { .. })));
    }

    #[test]
    fn inputs_lexically_normalized_first() {
        assert_eq!(
            relative_to(Path::new("a/b/./c"), Path::new("a/./b")).unwrap(),
            Path::new("c")
        );
    }

    #[test]
    fn relative_parent_base_matches() {
        assert_eq!(
            relative_to(Path::new("../x/y"), Path::new("../x")).unwrap(),
            Path::new("y")
        );
    }

    #[cfg(windows)]
    #[test]
    fn cross_drive_is_explicit_error() {
        assert!(matches!(
            relative_to(Path::new(r"D:\a\b"), Path::new(r"C:\a")),
            Err(RelError::PrefixKindMismatch { .. })
        ));
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_path_relativizes_against_legacy_base() {
        assert_eq!(
            relative_to(Path::new(r"\\?\C:\a\b"), Path::new(r"C:\a")).unwrap(),
            Path::new(r"b")
        );
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_with_dots_cannot_relativize_silently() {
        // 剥不掉的 verbatim vs legacy：显式拒绝（pathdiff 此处产出 `..\..\` 垃圾）
        assert!(matches!(
            relative_to(Path::new(r"\\?\C:\a\.\b"), Path::new(r"C:\a")),
            Err(RelError::PrefixKindMismatch { .. })
        ));
    }

    // ---- ensure_inside ----

    #[test]
    fn direct_containment_ok() {
        assert!(ensure_inside(
            Path::new("a/b"),
            Path::new("a/b/c/d"),
            CaseStrategy::CaseSensitive
        )
        .is_ok());
    }

    #[test]
    fn equal_is_inside() {
        assert!(ensure_inside(
            Path::new("a/b"),
            Path::new("a/b"),
            CaseStrategy::CaseSensitive
        )
        .is_ok());
    }

    #[test]
    fn lexical_escape_rejected() {
        // F21 端到端：`..` 逃逸在归一后现形
        let err = ensure_inside(
            Path::new("a"),
            Path::new("a/../b"),
            CaseStrategy::CaseSensitive,
        )
        .unwrap_err();
        assert_eq!(err.base, Path::new("a"));
        assert_eq!(err.path, Path::new("b"));
    }

    #[test]
    fn component_boundary_respected() {
        // starts_with 语义是组件级不是字符串级：`bc` 不在 `b` 内
        assert!(ensure_inside(
            Path::new("a/b"),
            Path::new("a/bc"),
            CaseStrategy::CaseSensitive
        )
        .is_err());
    }

    #[test]
    fn mixed_absolute_relative_not_inside() {
        assert!(ensure_inside(
            Path::new("/abs"),
            Path::new("rel/x"),
            CaseStrategy::CaseSensitive
        )
        .is_err());
    }

    #[test]
    fn case_insensitive_matches() {
        assert!(ensure_inside(
            Path::new("Work/Docs"),
            Path::new("WORK/docs/sub/f.txt"),
            CaseStrategy::CaseInsensitive
        )
        .is_ok());
    }

    #[test]
    fn case_sensitive_rejects_fold() {
        assert!(ensure_inside(
            Path::new("Work/Docs"),
            Path::new("WORK/docs/f.txt"),
            CaseStrategy::CaseSensitive
        )
        .is_err());
    }

    #[test]
    fn error_display_mentions_both_paths() {
        let err = NotInside {
            base: PathBuf::from("B"),
            path: PathBuf::from("P"),
        };
        let s = err.to_string();
        assert!(s.contains("B") && s.contains("P"));
        let err = RelError::NotPrefix {
            base: PathBuf::from("B"),
            path: PathBuf::from("P"),
        };
        assert!(err.to_string().contains("B") && err.to_string().contains("P"));
    }

    // ===== home/XDG 域：resolve_xdg_dir 纯核全矩阵（无 env 访问，零 env 竞争） =====
    // 纯核仅在非 Windows 存在（Windows 面 XDG 恒 None，无矩阵可测——接线面
    // 由 tests/paths_xdg.rs 的 windows_face_is_none 钉死）。
    #[cfg(not(windows))]
    mod xdg_matrix {
        use super::*;

        #[test]
        fn xdg_home_none_breaks_chain() {
            // 契约拍板：家锚不可解析 ⇒ 回退链断裂，None 链式传播（fail-closed，
            // 即使 XDG env 值本身有效）
            let home: Option<&Path> = None;
            assert_eq!(
                resolve_xdg_dir(home, Some(OsStr::new("/abs/cfg")), ".config"),
                None
            );
            assert_eq!(resolve_xdg_dir(home, None, ".config"), None);
            assert_eq!(resolve_xdg_dir(home, Some(OsStr::new("")), ".config"), None);
            assert_eq!(
                resolve_xdg_dir(home, Some(OsStr::new("rel")), ".config"),
                None
            );
        }

        #[test]
        fn xdg_unset_and_empty_use_home_default() {
            // spec v0.8："either not set or empty" ⇒ 默认 $HOME/.config
            let home = Path::new("/h");
            assert_eq!(
                resolve_xdg_dir(Some(home), None, ".config"),
                Some(PathBuf::from("/h/.config"))
            );
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::new("")), ".config"),
                Some(PathBuf::from("/h/.config"))
            );
        }

        #[test]
        fn xdg_relative_value_invalid_ignored() {
            // spec v0.8："All paths ... must be absolute ... relative path ... invalid
            // and ignore it" ⇒ 忽略并回退默认（ armor：不信半截值）
            let home = Path::new("/h");
            for rel in ["rel/cfg", ".", "..", " ", "C:cfg"] {
                assert_eq!(
                    resolve_xdg_dir(Some(home), Some(OsStr::new(rel)), ".config"),
                    Some(PathBuf::from("/h/.config")),
                    "relative/invalid value must be ignored: {rel:?}"
                );
            }
        }

        #[test]
        fn xdg_absolute_value_wins() {
            let home = Path::new("/h");
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::new("/abs/cfg")), ".config"),
                Some(PathBuf::from("/abs/cfg"))
            );
        }

        #[test]
        fn xdg_lexical_normalization_applied() {
            // 尾分隔符/重复分隔符/`.`/`..` 词法归一（normalize_lexical 复用，F21 同语义）
            let home = Path::new("/h");
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::new("/abs//cfg///")), ".config"),
                Some(PathBuf::from("/abs/cfg"))
            );
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::new("/abs/cfg/../cfg2")), ".config"),
                Some(PathBuf::from("/abs/cfg2"))
            );
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::new("/abs/./cfg")), ".config"),
                Some(PathBuf::from("/abs/cfg"))
            );
        }

        #[test]
        fn xdg_home_join_normalized() {
            // home 尾分隔符在 join 后归一；多段默认尾组件完整
            assert_eq!(
                resolve_xdg_dir(Some(Path::new("/h/")), None, ".cache"),
                Some(PathBuf::from("/h/.cache"))
            );
            assert_eq!(
                resolve_xdg_dir(Some(Path::new("/h")), None, ".local/share"),
                Some(PathBuf::from("/h/.local/share"))
            );
        }

        #[test]
        #[cfg(unix)]
        fn xdg_non_utf8_value_lossless() {
            use std::os::unix::ffi::OsStrExt;
            let home = Path::new("/h");
            // 非 UTF-8 绝对值无损保留（var_os 面，禁 to_string_lossy 静默改写）
            let raw = OsStr::from_bytes(b"/abs/\xFFcfg");
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(raw), ".config"),
                Some(PathBuf::from(OsStr::from_bytes(b"/abs/\xFFcfg")))
            );
            // 非 UTF-8 相对值同按 invalid 忽略回退默认
            assert_eq!(
                resolve_xdg_dir(Some(home), Some(OsStr::from_bytes(b"rel\xFF")), ".config"),
                Some(PathBuf::from("/h/.config"))
            );
        }
    }
}
