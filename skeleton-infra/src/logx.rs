//! 日志域两层拆分（批 1 log feature 拆分，2026-10-18；ADR 0002）：
//!
//! - **`log-fs`（纯 std，零三方依赖）**：密钥掩码 [`mask_secrets`] + 文件落盘
//!   面——路径契约 [`LogPathResolver`]
//!   （trait 住 `skeleton::logpath`）+ 默认实现 [`DefaultPathResolver`] +
//!   按日滚动 [`FileSink`]。单开 `log-fs` 即可用文件日志，不拉 tracing 四件套。
//! - **`log`（= `log-fs` + log + tracing + tracing-subscriber + tracing-log，
//!   门面绑定层）**：init 系（`init_tracing_from_env`/`init_with_file`）、
//!   `FileSinkWriter`（MakeWriter 适配壳）、默认行格式（LineFormat）。
//!   门面 trait=生态 `log::Log`（不自造）。
//!
//! 级别过滤=订阅者层全局上限；per-module 交给 `init_tracing_from_env` 的
//! EnvFilter。核心自身永不打日志（核心零依赖，结构性排除）。
//!
//! 域级过滤预设：`init_tracing_from_env` 在 RUST_LOG 未设/非法时兜底的
//! EnvFilter 恒为 `<default_level>,skeleton=off`——框架自身埋点默认静默，宿主
//! 日志零打扰；RUST_LOG 已设且合法时逐字生效（显式值恒赢预设）。三种开启
//! 姿势（RUST_LOG 语法示例，埋点 target 形如 `skeleton::<域>`）：
//!
//! ```text
//! RUST_LOG=debug                           # 全开：所有域
//! RUST_LOG=warn,skeleton::ratelimit=debug  # 单域：基线 warn + 该域 debug
//! RUST_LOG=skeleton::scheduler=trace       # 单流程：只这一个子系统
//! ```
//!
//! EnvFilter 解析失败 fail-visible：stderr 报非法语法与回退结果（配置错误
//! 必须可见，禁静默降级成另一档级别；经同一 best-effort reporter，stderr
//! 不可写时告警丢失但不 panic、不回退成另一档级别）。
//!
//! 零依赖纪律（同 text.rs/journal.rs/memgov.rs 先例）：thiserror 不入 manifest，
//! 本件手写同形 `Display`/`Error` impl（文案与 thiserror 派生形态逐字一致），
//! 引入 thiserror 依赖时可换回派生。
//!
//! 初始化组合注记：显式 `tracing_log::LogTracer::init()` +
//! `fmt().try_init()` 组合运行时恒 Err——tracing-subscriber 0.3 默认 feature
//! `tracing-log` 使 `try_init` 内部再调一次 `LogTracer::init()`，而
//! `log::set_boxed_logger` 非幂等。故本件只调 `try_init()`（其内部已含桥
//! 初始化），首调 Ok/重复 Err/log→tracing 桥生效的语义不变。
//!
//! 文件落盘面（拍板 2026-10-17）：`init_with_file` = console+file 双 sink
//! （同一默认行格式）；落点经 `skeleton::logpath::LogPathResolver` 契约——
//! 默认实现 [`DefaultPathResolver`]，debug 构建=编译期 `CARGO_MANIFEST_DIR`/log
//!（框架自身开发态，零运行时猜测；采方开发态请注入自有 resolver，见
//! `skeleton::logpath` trait 文档示例），release 构建=
//! `home_dir()/.<project_name>/log/`，home 不可解析=构造期显式 Err（禁静默
//! 落 cwd）。[`FileSink`] 按日滚动 `log-YYYY-MM-DD.log` 纯追加（跨进程按行
//! 交错安全，永不改名/截断），每行 flush；写失败=计数+一次告警+继续（禁
//! panic 拖垮业务、禁纯吞）。告警走 [`SoftFailureReporter`]（默认
//! [`StderrReporter`]，best-effort、禁 `eprintln!`——stderr 不可写时它会
//! panic 或阻塞）；写/刷失败后当日落点按 [`FileSink`] 失败面阶梯降级
//! （主文件 → 恢复文件 → 当日停写）。时间戳与文件名恒 UTC（std 无本地时区
//! API，chrono 依赖判否）。
//!
//! 多通道扇出面（P1-10-a 前置件，拍板 2026-10-23；`log` feature）：固定三
//! 通道 `Channel`（general/access/dns，封闭枚举=非法通道名在类型层不可
//! 能）→ 每通道独立 sink（一通道一 sink 禁串扰）+ 独立 level（槽位
//! AtomicU8 写路径比较：低于通道级在 registry 即滤，不计丢弃；
//! `ChannelRegistry::set_level` 运行期原子改写）+ 重复注册 fail-visible
//!（原 sink 不动）+ `ChannelRegistry` 零分配扇出（未注册通道静默：不输出/
//! 不 panic/不分配/不计数）+ 掩码唯一布点（`set_secrets`，写前统一过
//! [`mask_secrets`]）+ 每通道独立落点（`PerChannelPathResolver` 通道子目录
//! + `ChannelFileSink` 组合件）。
//!
//! 通道动态级 armor 红线（域 ⑥ 调研：外部踩坑调研（本地留存，未入库）；tracing#2874/#2400 实锤）：禁 with_default 长驻、禁运行期
//! 全局换装 subscriber——callsite interest 缓存的失效竞态是结构性风险。
//! 本件通道级过滤在 registry 写路径（槽位 AtomicU8 比较），不在
//! subscriber/callsite 层：动态改级天然免疫缓存失效，下一 emit 即按新级别
//! 判定（无需重评 callsite）。两态显式：每条 emit 判定=投递或静默，静默
//! 不计丢弃；投递写失败由 sink 自计数（`ChannelRegistry::dropped`/
//! `ChannelRegistry::dropped_total` 可对账）。

use skeleton::logpath::LogPathResolver;

/// 全局 logger 已被占用。重复初始化=显式 Err（禁静默覆盖）。
#[cfg(feature = "log")]
#[derive(Debug)]
pub struct AlreadyInit;

#[cfg(feature = "log")]
impl std::fmt::Display for AlreadyInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "global logger already initialized")
    }
}

#[cfg(feature = "log")]
impl std::error::Error for AlreadyInit {}

/// 初始化推荐组合：tracing-log 桥（log→tracing）+ tracing fmt 订阅者。
/// 全局副作用一次性；重复调用=Err（禁静默覆盖）。`level` 为全局上限过滤。
///
/// 桥接语义知情声明：log 门面经 tracing-log 桥后事件 target="log"、原
/// target 降为 `log.target` 字段（生态标准桥语义）——`RUST_LOG=skeleton=debug`
/// 这类按域过滤不路由 log 门面行，需按 `log.target` 字段自滤。
#[cfg(feature = "log")]
pub fn init_tracing_default(level: log::LevelFilter) -> Result<(), AlreadyInit> {
    tracing_subscriber::fmt()
        .with_max_level(to_tracing_filter(level))
        .try_init()
        .map_err(|_| AlreadyInit)
}

/// 同 [`init_tracing_default`]，但级别经 RUST_LOG 语义 EnvFilter
/// （`default_level` 为环境未设/非法时的兜底；兜底恒含 `skeleton=off` 预设，
/// 三种开启姿势见模块文档）。
#[cfg(feature = "log")]
pub fn init_tracing_from_env(default_level: log::LevelFilter) -> Result<(), AlreadyInit> {
    let filter = env_filter_or(default_level);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|_| AlreadyInit)
}

/// RUST_LOG 已设且合法→按其语义（显式值恒赢预设）；未设/非法→兜底
/// `<default_level>,skeleton=off` 预设（非法时 stderr fail-visible，禁静默降级；
/// `init_tracing_from_env` 的级别构造器，独立成私有函数以便分支隔离测试）。
///
/// 分支隔离手写而非 `try_from_default_env`+`is_parse`：tracing-subscriber 0.3
/// 的 `FromEnvError` 未公开解析/环境错误判别方法——自读 env var 才能只对
/// 「已设但解析失败」告警，未设值保持静默兜底。
#[cfg(feature = "log")]
fn env_filter_or(default_level: log::LevelFilter) -> tracing_subscriber::EnvFilter {
    let fallback = || tracing_subscriber::EnvFilter::new(format!("{},skeleton=off", default_level));
    match std::env::var("RUST_LOG") {
        Ok(spec) => match tracing_subscriber::EnvFilter::try_new(&spec) {
            Ok(filter) => filter,
            Err(err) => {
                // fail-visible：RUST_LOG 写错不允许静默降级成另一档级别
                //（#143：告警走 best-effort reporter——stderr 坏掉时 eprintln!
                // 会 panic，把「告警后回退」变成崩溃）。
                StderrReporter.report(&format!(
                    "skeleton-infra::logx: RUST_LOG={spec:?} 语法非法（{err}）；\
                     回退默认过滤 `{}`+skeleton=off（框架域静默）",
                    default_level,
                ));
                fallback()
            }
        },
        Err(std::env::VarError::NotPresent) => fallback(), // 未设：正常兜底，非配置错误
        Err(err) => {
            // fail-visible 对齐（审计吹毛）：非 Unicode 值同属「已设但不可
            // 解析」面，禁静默降级
            StderrReporter.report(&format!(
                "skeleton-infra::logx: RUST_LOG 不可解析（{err}）；\
                 回退默认过滤 `{}`+skeleton=off（框架域静默）",
                default_level,
            ));
            fallback()
        }
    }
}

/// log→tracing 级别显式映射（tracing-core 的 `From<log::LevelFilter>` 挂在未启用
/// 的 `log` feature 下，本构建不可达，故手写六臂）。
#[cfg(feature = "log")]
fn to_tracing_filter(level: log::LevelFilter) -> tracing::level_filters::LevelFilter {
    match level {
        log::LevelFilter::Off => tracing::level_filters::LevelFilter::OFF,
        log::LevelFilter::Error => tracing::level_filters::LevelFilter::ERROR,
        log::LevelFilter::Warn => tracing::level_filters::LevelFilter::WARN,
        log::LevelFilter::Info => tracing::level_filters::LevelFilter::INFO,
        log::LevelFilter::Debug => tracing::level_filters::LevelFilter::DEBUG,
        log::LevelFilter::Trace => tracing::level_filters::LevelFilter::TRACE,
    }
}

/// E 类：密钥掩码。命中子串→`"***"`；
/// 按长度降序替换（最长优先防半掩）；空串项跳过；空表/空串原样返回；
/// 恒分配一次新 String。
///
/// 性能锚（2026-10-16 批 C 定性，release/std Instant 粗测，详见
/// 批 C 性能锚（本地留存，未入库））：无命中纯扫描近线性 O(n×m)——4KiB×
/// 1/8/64 规则 ~0.6/4/85µs，64KiB ~14/67/1175µs（四轮中位）；规则轴 8 条
/// ~6×、64 条 ~50-156×（分配/缓存压力放大），文本轴 16× 输入比值 ~10-37×
///（终轮 13.8-15.6 ≈ 线性）——双轴同向增长、无平方爆点；命中另计每规则
/// 一次 String 重建（8 命中@4KiB ~1.5µs vs 无命中 ~0.6µs；~0.6µs=1 规则
/// 口径，8 规则无命中见前 ~4µs，reference 跨机区间 2.9-4µs 同源）。数字供判级：
/// 高频日志路径宜上游过滤先行再掩码。
pub fn mask_secrets(input: &str, secrets: &[&str]) -> String {
    let mut sorted: Vec<&str> = secrets.to_vec();
    sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));
    let mut out = input.to_string();
    for secret in sorted {
        if secret.is_empty() {
            continue; // 空串 pattern 会在每个字符位注入 "***"
        }
        out = out.replace(secret, "***");
    }
    out
}

// ── 文件落盘面：路径解析实现 + 按日滚动 FileSink + 双 sink 初始化 ──
//
// 路径契约 `LogPathResolver` 已下沉 `skeleton::logpath`（批 1 契约真下沉，
// 2026-10-18）；本件 `use skeleton::logpath::LogPathResolver` 导入后只承载
// 默认实现与行为件。

/// release 构建下 [`std_ext::paths::home_dir`] 返回 None（std 文档：Unix=HOME 未设
/// 且 passwd 无条目；Windows=USERPROFILE 未设且 GetUserProfileDirectory 失败；
/// UWP 恒 None——工程内常规目标前两者有平台回退，此分支属防御面），或返回了
/// **非绝对**路径（#179：相对 `$HOME`／passwd home 空串，`paths::home_dir` 已按
/// None 过滤）。落地禁静默回退 cwd，故构造期显式 Err。
#[derive(Debug)]
pub struct HomeDirUnresolvable;

impl std::fmt::Display for HomeDirUnresolvable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "home directory unresolvable (std::env::home_dir returned None or a \
             relative path); refusing to fall back to the current directory — set \
             USERPROFILE/HOME to an absolute path or supply a custom LogPathResolver"
        )
    }
}

impl std::error::Error for HomeDirUnresolvable {}

/// [`DefaultPathResolver::new`] 的构造失败面（#175/#113/#179）：区分「家锚不可
/// 用」与「project_name 非法」——前者是环境问题（换机器/换用户），后者是配置
/// 问题（改名字），处置不同，禁混成一个错误。
#[derive(Debug)]
pub enum LogPathError {
    /// home 不可解析或不是绝对路径（#179）。
    HomeUnresolvable(HomeDirUnresolvable),
    /// project_name 不是单一安全路径组件（#175/#113）。
    InvalidProjectName {
        /// 被拒的原始名字。
        name: String,
        /// 违反的规则（诊断面，静态文案）。
        reason: &'static str,
    },
}

impl std::fmt::Display for LogPathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HomeUnresolvable(err) => err.fmt(f),
            Self::InvalidProjectName { name, reason } => write!(
                f,
                "invalid project name {name:?}: {reason}; project_name must be a single \
                 directory name — no path separators, `.`/`..`, drive/UNC prefix or \
                 control characters"
            ),
        }
    }
}

impl std::error::Error for LogPathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::HomeUnresolvable(err) => Some(err),
            Self::InvalidProjectName { .. } => None,
        }
    }
}

/// project_name 语法（#175/#113）：**单个 `Normal` 路径组件**，即一个目录名，
/// 不是相对路径表达式。拒绝空串、`/` 与 `\`（两平台分隔符都拒，禁把名字当
/// 多组件路径解释）、`.`/`..`、NUL/控制字符、以及 `components()` 给出的
/// 非 `Normal` 形（Windows 的盘符/UNC `Prefix`、根）。
///
/// 不静默改写（清洗会把不同输入折叠到同一目录，属歧义输入应拒绝）；不依赖
/// 字符串前缀判断。
fn validate_project_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("must not be empty");
    }
    if name.chars().any(|c| c == '/' || c == '\\') {
        return Err("must not contain a path separator (`/` or `\\`)");
    }
    if name.chars().any(char::is_control) {
        return Err("must not contain control or NUL characters");
    }
    let mut components = std::path::Path::new(name).components();
    match (components.next(), components.next()) {
        (Some(std::path::Component::Normal(component)), None)
            if component == std::ffi::OsStr::new(name) =>
        {
            Ok(())
        }
        _ => Err("must be a single normal path component (no `.`/`..`, root or drive/UNC prefix)"),
    }
}

/// 默认路径解析器（拍板 2026-10-17）：debug 构建=编译期
/// `CARGO_MANIFEST_DIR`/log（框架自身开发态，零运行时猜测）；release 构建=
/// `<home_dir>/.<project_name>/log/`——**release 恒不引用 CARGO_MANIFEST_DIR**
/// （那会把开发者机器路径烧进发布物，隐私面；测试钉死）。`project_name`
/// 为构造参数。
///
/// **`project_name` 语法**（#175/#113，debug/release 同判）：一个目录名 = 单个
/// `Normal` 路径组件，非空，不含 `/`、`\`、NUL/控制字符，不是 `.`/`..`，也
/// 不是盘符/UNC 前缀形——它**不是**相对路径表达式，含分隔符或穿越组件的值
/// 一律 `Err(LogPathError::InvalidProjectName)`（不静默清洗）。
///
/// home 在构造期一次性解析：解析失败或**非绝对**（#179）显式 Err（禁静默落
/// cwd），此后 [`LogPathResolver::log_root`] 恒不可失败。
///
/// # Examples
///
/// ```
/// let r = skeleton_infra::logx::DefaultPathResolver::new("agentwiki")
///     .expect("名字合法；debug 构建不解析 home，release 构建需 home 可解析");
/// // debug 构建下 r.log_root() == <本 crate 工程目录>/log（编译期锚）
/// ```
///
/// # 采用方警示（杠精盲审 2026-10-17）
///
/// debug 臂的编译期锚=**本框架包目录**（`env!` 于框架编译期展开）——框架
/// 经 crates.io/git 引用时，采用方 debug 态的默认日志会写进 registry 缓存
/// 或 checkout 树内的包目录（`框架包目录/log/`）。该默认只为框架自身
/// dogfood 便利；**采用方 debug 态必须注入自有 resolver**（[`LogPathResolver`]
/// 契约，trait 文档有示例），或显式接受日志落包目录。release 臂不受影响。
#[derive(Debug)]
pub struct DefaultPathResolver {
    #[cfg(not(debug_assertions))]
    project_name: String,
    #[cfg(not(debug_assertions))]
    home: std::path::PathBuf,
}

impl DefaultPathResolver {
    /// 名字恒校验（debug 也校验——名字未被使用不是接受非法值的理由）；
    /// debug 构建此后恒 Ok，release 构建紧接着解析 home，None/非绝对 →
    /// `Err(LogPathError::HomeUnresolvable)`。
    pub fn new(project_name: impl Into<String>) -> Result<Self, LogPathError> {
        let project_name = project_name.into();
        validate_project_name(&project_name).map_err(|reason| {
            LogPathError::InvalidProjectName {
                name: project_name.clone(),
                reason,
            }
        })?;
        #[cfg(not(debug_assertions))]
        {
            let home = std_ext::paths::home_dir()
                .ok_or(LogPathError::HomeUnresolvable(HomeDirUnresolvable))?;
            Ok(Self { project_name, home })
        }
        #[cfg(debug_assertions)]
        {
            let _ = project_name; // debug 分支零字段，名字只过校验不入布局
            Ok(Self {})
        }
    }
}

/// release 根布局：`<home>/.<project_name>/log`（独立纯函数便于无 env 断言；
/// 仅 release 分支可达，测试同 gate）。
#[cfg(not(debug_assertions))]
fn release_root(home: &std::path::Path, project_name: &str) -> std::path::PathBuf {
    home.join(format!(".{project_name}")).join("log")
}

impl LogPathResolver for DefaultPathResolver {
    fn log_root(&self) -> std::path::PathBuf {
        #[cfg(not(debug_assertions))]
        {
            release_root(&self.home, &self.project_name)
        }
        #[cfg(debug_assertions)]
        {
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("log")
        }
    }
}

/// [`init_with_file`] 的失败面：全局 logger 已被占用，或日志根目录创建失败
/// （fail-visible：目录建不出来就不装订阅者，禁半初始化静默）。
#[cfg(feature = "log")]
#[derive(Debug)]
pub enum LogInitError {
    /// 全局订阅者（或 log→tracing 桥）已被占用。
    AlreadyInit,
    /// 日志根目录 `create_dir_all` 失败。
    RootCreate {
        root: std::path::PathBuf,
        source: std::io::Error,
    },
}

#[cfg(feature = "log")]
impl std::fmt::Display for LogInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyInit => write!(f, "global logger already initialized"),
            Self::RootCreate { root, source } => {
                write!(f, "cannot create log root {}: {source}", root.display())
            }
        }
    }
}

#[cfg(feature = "log")]
impl std::error::Error for LogInitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AlreadyInit => None,
            Self::RootCreate { source, .. } => Some(source),
        }
    }
}

/// 软失败诊断出口（#184/#143）：软路径上的告警是 **best-effort**——实现既不得
/// panic，也不得无限期阻塞调用线程。`eprintln!`/`print!` 系列在 stderr 不可写
/// 时 panic、在管道满时阻塞，软失败路径用它们等于把日志故障升级成业务故障。
/// 实现方契约：
///
/// - **不 panic**：任何 IO 错误吞掉即可；
/// - **有界**：单次尝试，不重试、不循环、不排队等待（实现方还应尽量不引入可
///   无限期阻塞的操作；默认 [`StderrReporter`] 的残留见其文档）；
/// - 报告可以丢失——[`FileSink::dropped_lines`] 才是权威计数面。
///
/// 注入入口 [`FileSink::with_reporter`]：测试用它模拟坏 stderr（断言软失败
/// 路径不依赖 stderr 可写），宿主用它把诊断改道到自己的汇。
pub trait SoftFailureReporter: std::fmt::Debug + Send + Sync {
    /// 报告一条软失败诊断（不可失败、不可重试、不可上抛）。
    fn report(&self, message: &str);
}

/// 默认出口：best-effort 写 stderr，一切错误忽略（#184 Round 2：最终错误路径
/// 不得用 `eprintln!`——它自己会 panic）。
///
/// 有界性（诚实声明）：每次报告 = 单次持锁 + 单次写入的**常量工作量**——不重试、
/// 不循环、不追加（`eprintln!` 的失败会 panic，且没有放弃点）。两点残留：
///
/// - stderr 锁不可非阻塞获取：本仓固定的 1.96.0 工具链上 `Stderr::try_lock`
///   尚不可用（实测 E0599），故锁竞争时会等锁（std 的 stderr 锁只被别的
///   stderr 写者短暂持有）；
/// - stderr 管道满/下游停止读取时同步 `write` 仍可能阻塞——彻底非阻塞需后台写
///   线程或改 fd 标志，两者都在本件非目标内。
///
/// 需要硬保证的宿主注入自己的 [`SoftFailureReporter`]（如只计数、把诊断交给
/// 独立的可丢弃队列）。
#[derive(Debug, Default, Clone, Copy)]
pub struct StderrReporter;

impl SoftFailureReporter for StderrReporter {
    fn report(&self, message: &str) {
        use std::io::Write as _;
        let _ = writeln!(std::io::stderr().lock(), "{message}");
    }
}

/// 按日滚动的追加式文件日志汇（诊断面）。
///
/// 跨进程并发安全（armor，std 文档逐条对表）：纯追加**永不改名/截断**——
/// 换日只是打开新文件，无 rotation 改名交接面；`OpenOptions::append` 在
/// Unix=O_APPEND、Windows=FILE_APPEND_DATA，均按次 write() 原子追加到 EOF；
/// std 默认以 FILE_SHARE_READ|WRITE|DELETE 打开，不挡并发打开者。每次
/// `write` 调用=一个落盘单元（fmt 层经 `format_line` 保证整行单调用）。
///
/// 失败面（拍板 2026-10-17；#112/#184/#198 收紧）：写失败**不 panic、不上抛**
/// ——计数 + 首行一次告警 + 继续（日志失败拖垮业务是反模式；纯吞禁——
/// [`FileSink::dropped_lines`] 可观测）。两类失败分开处理：
///
/// - **打开失败**（目录缺失/权限/落点被占）：落点不变、`writer` 保持未打开，
///   下一行重走打开路径——只重试新行，不追认上一行；
/// - **写/刷失败**：该文件的记录边界已不可信（可能已落半行前缀）——句柄在
///   锁内摘除、锁外析构（`BufWriter` 析构自带的 flush 属 std 行为，错误忽略）；
///   退役即**放弃**该句柄：不补偿 flush、不重试上一行。当日主文件
///   **poisoned 不再续写**，后续行改落 `log-YYYY-MM-DD.recovery.log`；恢复
///   文件再写/刷失败则当日 fail-stop（后续行只计数，不再开新文件）。
///
/// 告警一律走 [`SoftFailureReporter`]（默认 [`StderrReporter`]）：best-effort、
/// 单次尝试、不 panic（`eprintln!` 在 stderr 不可写时会 panic）。失败告警
/// 仍只报首次；落点升级各有一次独立诊断（每日至多 2 条，非逐行），二者都不
/// 计入 [`FileSink::dropped_lines`]。
///
/// **恢复 ≠ 记录原子性**：poison 后的文件尾部可能留半行前缀，本件**不截断**
/// （日志文件跨进程共享，回截会删掉别的进程已追加的行）；恢复只保证「新行不
/// 再接在半行后面」，不自动修复已有半行，也不保证跨进程记录原子性——需要逐
/// 记录修复请按文件自建 framing/校验。
///
/// 边缘语义（固有 fd 行为，非缺陷）：日志根被外部删除后，长命句柄的续行
/// 写入已解除链接的文件（Windows share-delete/POSIX unlink 同构）——行入
/// void 且 `dropped_lines` 不涨（写未失败）、目录不重建、直到换日才重走
/// 打开路径。按天滚动天然限损 ≤1 天；需即时感知者自管根目录生命周期。
///
/// 时间戳/文件名恒 UTC（std 无本地时区 API，chrono 依赖判否）。
///
/// **隐私策略**（#180，显式而非隐含）：
///
/// - **Unix**：新建根 `0700`、每日文件 `0600`，用 `mode` 直接给（独立于进程
///   umask，umask 只能清位）；**既有**路径若带 group/other 位则拒绝——根在
///   [`FileSink::new`] 返回 `PermissionDenied`，当日文件在打开时返回错（软失败
///   计数 + 一次告警，不落任何字节）。本件**不隐式 chmod** 调用方目录/文件。
/// - **Windows**：无 POSIX 权限位，目录/文件权限沿用父目录 ACL 继承——父 ACL
///   未收紧时本路径**不是隐私边界**，宿主需自管 ACL（本件不宣称私有）。
///
/// # Examples
///
/// ```no_run
/// // no_run：doctest cwd=包根且隔离构建下无既有 scratch 根（禁系统 TEMP）；
/// // 运行时行为由 file_sink_tests 与 tests/log_file.rs 真跑覆盖。
/// use std::io::Write as _;
/// use std_ext::fsx::temp_dir_in;
/// use skeleton_infra::logx::FileSink;
///
/// let tmp = temp_dir_in("target", "logx-doc-")?;
/// let mut sink = FileSink::new(tmp.path().join("logs"))?;
/// sink.write_all(b"hello\n")?;
/// assert_eq!(sink.dropped_lines(), 0);
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct FileSink {
    root: std::path::PathBuf,
    state: std::sync::Mutex<SinkState>,
    dropped: std::sync::atomic::AtomicU64,
    warned: std::sync::atomic::AtomicBool,
    reporter: std::sync::Arc<dyn SoftFailureReporter>,
}

/// 当日落点的降级阶梯（#198：写/刷失败后禁用当前文件，禁原地续写）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Target {
    /// `log-YYYY-MM-DD.log`：正常态。
    Main,
    /// `log-YYYY-MM-DD.recovery.log`：主文件写/刷失败后的接管文件（append）。
    Recovery,
    /// 恢复文件也写/刷失败：当日 fail-stop，后续行只计数不落盘——禁续写边界
    /// 不明的文件，禁每失败一次再造一个新文件。
    Stopped,
}

impl Target {
    /// 失败一次后的下一落点。
    fn after_failure(self) -> Self {
        match self {
            Self::Main => Self::Recovery,
            Self::Recovery | Self::Stopped => Self::Stopped,
        }
    }

    /// 落点文件名（[`Target::Stopped`] 无落点）。
    fn file_name(self, day: i64) -> Option<String> {
        match self {
            Self::Main => Some(day_file_name(day)),
            Self::Recovery => Some(recovery_file_name(day)),
            Self::Stopped => None,
        }
    }
}

struct SinkState {
    /// 当前落点对应的 UTC 纪元日（换日即重开主文件）。
    day: i64,
    /// 当日落点阶梯。
    target: Target,
    /// 落点文件句柄；`None` = 待打开（首行/换日/上次打开失败）。
    ///
    /// 动态 `Write` 是 #198 要求的注入点：生产恒 `BufWriter<File>`，测试塞入
    /// 半写/刷失败替身以钉死恢复行为。
    writer: Option<Box<dyn std::io::Write + Send>>,
}

impl std::fmt::Debug for SinkState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 句柄不透明（dyn Write 无 Debug）：只报是否已打开。
        f.debug_struct("SinkState")
            .field("day", &self.day)
            .field("target", &self.target)
            .field("writer_open", &self.writer.is_some())
            .finish()
    }
}

/// 建日志根并回报**本调用是否新建了它**（#180 权限 + #130 归属）。
///
/// 归属判据 = 非递归 `create` 的 `AlreadyExists`：原子，没有 `exists()` 的
/// check-then-create 窗口。回收决策只认这个回执（`remove_dir` 成功**不**证明
/// 目录是本调用建的）。
///
/// Unix：新建 `0700`（`mode` 独立于 umask——umask 只能清位）；既有根校验无
/// group/other 位（**不隐式 chmod** 调用方目录）。Windows 无 POSIX 权限位：
/// 权限沿用父目录 ACL 继承，本件不宣称私有。
#[cfg(unix)]
fn create_root(root: &std::path::Path) -> std::io::Result<bool> {
    use std::os::unix::fs::DirBuilderExt as _;

    let mut builder = std::fs::DirBuilder::new();
    builder.mode(0o700);
    match builder.create(root) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            check_private_dir(root)?;
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            // 父级缺失 ⇒ 根不可能已存在，整棵由本次新建（各级同 0700）。
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(root)?;
            Ok(true)
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(unix))]
fn create_root(root: &std::path::Path) -> std::io::Result<bool> {
    match std::fs::DirBuilder::new().create(root) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            check_private_dir(root)?;
            Ok(false)
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(root)?;
            Ok(true)
        }
        Err(err) => Err(err),
    }
}

/// 既有根目录判定 + 隐私校验（#180）。Unix：group/other 有位即拒绝（静默
/// chmod 调用方目录越界）；其他平台只判「是目录」，隐私由父 ACL 继承承担。
#[cfg(unix)]
fn check_private_dir(root: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let metadata = std::fs::metadata(root)?;
    if !metadata.is_dir() {
        return Err(not_a_dir(root));
    }
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "log root {} is group/other accessible (mode {mode:03o}); \
                 refusing to write logs there — tighten it or pick another root",
                root.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_dir(root: &std::path::Path) -> std::io::Result<()> {
    if std::fs::metadata(root)?.is_dir() {
        Ok(())
    } else {
        Err(not_a_dir(root))
    }
}

fn not_a_dir(root: &std::path::Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!("log root {} exists and is not a directory", root.display()),
    )
}

/// 打开前的根目录兜底（根可能被外部删除后重建）：新建时同样 `0700`；既有根
/// **不**复检权限（构造期已校验，续写路径不因外部 umask 变化改判——软失败面
/// 只保证自己新建的路径私有）。
#[cfg(unix)]
fn ensure_root_exists(root: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt as _;

    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(root)
}

#[cfg(not(unix))]
fn ensure_root_exists(root: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(root)
}

/// 打开当日落点追加句柄（#180）：Unix 新建 `0600`（`mode` 独立于 umask），
/// 既有文件校验无 group/other 位（**不隐式 chmod**，拒绝早于写入首字节）。
fn open_day_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    check_private_file(&file, path)?;
    Ok(file)
}

#[cfg(unix)]
fn check_private_file(file: &std::fs::File, path: &std::path::Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;

    let mode = file.metadata()?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "log file {} is group/other accessible (mode {mode:03o}); \
                 refusing to append to a non-private file",
                path.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private_file(_file: &std::fs::File, _path: &std::path::Path) -> std::io::Result<()> {
    Ok(())
}

impl FileSink {
    /// 建汇：立即递归创建根目录（配置错误在此显式失败，禁等到首行才爆）；
    /// 文件按日懒开（无事件不落空文件）。诊断走默认 [`StderrReporter`]。
    ///
    /// 权限（#180）：Unix 新建根 `0700`；**既有**根带 group/other 位则
    /// `Err(PermissionDenied)`（不静默 chmod 调用方目录）。Windows 无 POSIX
    /// 权限位，权限沿用父目录 ACL 继承——父 ACL 未收紧时本路径**不构成隐私
    /// 边界**（每日文件 `0600` 的保护只在 Unix 成立）。
    pub fn new(root: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        Self::with_reporter(root, std::sync::Arc::new(StderrReporter))
    }

    /// 同 [`FileSink::new`]，诊断出口可注入：测试用它模拟坏 stderr（断言软失败
    /// 路径不依赖 stderr 可写），宿主用它把软失败诊断改道到自己的汇。`reporter`
    /// 须自守 [`SoftFailureReporter`] 的 no-panic/不阻塞契约（本件不替它兜底）。
    pub fn with_reporter(
        root: impl Into<std::path::PathBuf>,
        reporter: std::sync::Arc<dyn SoftFailureReporter>,
    ) -> std::io::Result<Self> {
        let root = root.into();
        create_root(&root)?;
        Ok(Self {
            root,
            state: std::sync::Mutex::new(SinkState {
                day: i64::MIN,
                target: Target::Main,
                writer: None,
            }),
            dropped: std::sync::atomic::AtomicU64::new(0),
            warned: std::sync::atomic::AtomicBool::new(false),
            reporter,
        })
    }

    /// 累计丢弃行数（写失败即丢一行；静默吞的解毒剂，测试与宿主可观测面）。
    pub fn dropped_lines(&self) -> u64 {
        self.dropped.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// 落盘一行（软失败面，见类型文档）。
    fn write_line_soft(&self, buf: &[u8]) -> usize {
        match self.try_write_line(std::time::SystemTime::now(), buf) {
            Ok(()) => buf.len(),
            Err(err) => {
                let dropped = self
                    .dropped
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    + 1;
                if !self.warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    self.reporter.report(&format!(
                        "skeleton-infra::logx: log file write failed (root={}, \
                         error={err}); dropped lines so far: {dropped} — \
                         application keeps running",
                        self.root.display(),
                    ));
                }
                buf.len()
            }
        }
    }

    /// 单次落盘：换日（或首行/上次打开失败）→ 递归建根 + 打开当前落点文件
    /// （追加）；整行一次 write_all + flush（每行 flush 拍板）。锁内串行=进程
    /// 内行完整；锁外只做两件慢事——析构退役句柄（`BufWriter::drop` 会 flush）
    /// 与报告诊断（可能写 stderr），故告警/析构阻塞都不占状态锁。
    fn try_write_line(&self, now: std::time::SystemTime, buf: &[u8]) -> std::io::Result<()> {
        let day = utc_epoch_days(now);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // 每次调用至多退役「换日旧句柄」与「失败句柄」；Vec 不 push 即不分配，
        // 故热路径（成功写入）零分配。
        let mut retired: Vec<Box<dyn std::io::Write + Send>> = Vec::new();
        let mut notice = None;
        let result = self.write_locked(&mut state, day, buf, &mut retired, &mut notice);
        drop(state);
        drop(retired);
        if let Some(message) = notice {
            self.reporter.report(&message);
        }
        result
    }

    /// [`Self::try_write_line`] 的锁内体：不碰 [`SoftFailureReporter`]、不析构
    /// 句柄——退役句柄与诊断消息交回调用方在锁外处理。
    fn write_locked(
        &self,
        state: &mut SinkState,
        day: i64,
        buf: &[u8],
        retired: &mut Vec<Box<dyn std::io::Write + Send>>,
        notice: &mut Option<String>,
    ) -> std::io::Result<()> {
        if state.day != day {
            // 换日：落点阶梯归零；旧句柄退役（锁外析构）。
            state.day = day;
            state.target = Target::Main;
            retired.extend(state.writer.take());
        }
        if state.target == Target::Stopped {
            return Err(std::io::Error::other(
                "log file fail-stopped for the current UTC day \
                 (main and recovery file both failed)",
            ));
        }
        if state.writer.is_none() {
            // 打开失败 ≠ 写失败：落点不动，下一行重试同一落点（只重试新行）。
            let path = self
                .root
                .join(state.target.file_name(day).expect("not Stopped"));
            ensure_root_exists(&self.root)?;
            let file = open_day_file(&path)?;
            state.writer = Some(Box::new(std::io::BufWriter::new(file)));
        }
        // ponytail: 极端跨进程竞争下行仍可能按 write() 边界交错（常规文件
        // 单次几 KiB 写实际恒整写）；进程内由 Mutex 保证行完整。若未来出现
        // 真实交错现场，升级=绕过 BufWriter 每行单 write 系统调用直写。
        let write_result = {
            let writer = state.writer.as_mut().expect("writer opened above");
            std::io::Write::write_all(writer, buf).and_then(|()| std::io::Write::flush(writer))
        };
        match write_result {
            Ok(()) => Ok(()),
            Err(err) => {
                // 记录边界已不可信：句柄退役、当日落点降级，绝不原地续写。
                retired.extend(state.writer.take());
                let poisoned = self
                    .root
                    .join(state.target.file_name(day).expect("not Stopped"))
                    .display()
                    .to_string();
                let next = state.target.after_failure();
                let destination = match next.file_name(day) {
                    Some(name) => self.root.join(name).display().to_string(),
                    None => "no file (UTC day fail-stopped)".to_string(),
                };
                *notice = Some(format!(
                    "skeleton-infra::logx: retiring log file {poisoned} after write/flush \
                     error ({err}); subsequent lines for UTC day {day} go to {destination}"
                ));
                state.target = next;
                Err(err)
            }
        }
    }
}

impl std::io::Write for FileSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 软失败面：恒 Ok（丢弃走计数 + reporter 告警；io 层 Err 会被 fmt 层
        // 吞掉，在这里截住才有可观测计数）。
        Ok(self.write_line_soft(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(()) // 每行已即时 flush
    }
}

/// [`tracing_subscriber::fmt::MakeWriter`] 适配壳：orphan 规则下
/// `Arc<LocalType>` 不得直接 impl 外部 trait（Arc 非本地/非 fundamental），
/// 以本地新类型承载 Arc。
#[cfg(feature = "log")]
#[derive(Clone, Debug)]
pub struct FileSinkWriter(std::sync::Arc<FileSink>);

#[cfg(feature = "log")]
impl FileSinkWriter {
    /// 包装共享汇（同一 sink 可多壳并行写，内部锁串行）。
    pub fn new(sink: std::sync::Arc<FileSink>) -> Self {
        Self(sink)
    }
}

#[cfg(feature = "log")]
impl std::io::Write for FileSinkWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 软失败面直连（与 FileSink 的 io::Write 同一语义：恒 Ok，丢弃走计数）。
        Ok(self.0.write_line_soft(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(()) // 每行已即时 flush
    }
}

#[cfg(feature = "log")]
impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for FileSinkWriter {
    type Writer = FileSinkWriter;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

// ── 多通道扇出面（P1-10-a 前置件，拍板 2026-10-23）──
//
// general/access/dns 三固定通道；每通道独立 sink + 独立 level + 失败丢弃
// 可观测；未注册通道 emit 静默（不输出/不 panic/不分配/不计数）。
//
// 扇出热路径零堆分配论证：通道查找=枚举判别式数组索引（无 HashMap、无开放
// 字符串键），命中后 `Arc::clone` 只是引用计数增量（非堆分配），message 以
// `&str` 直传 sink——掩码表未配置（默认）时 emit 全程零堆分配。配置掩码后
// 写前统一过 [`mask_secrets`]（唯一布点，坑约束 11：掩码是机制面不是调用方
// 自觉），掩码本身按其既有语义分配（性能锚见其文档：高频路径宜上游过滤先
// 行再掩码）——这是掩码的安全成本，不在零分配契约内。

/// 日志通道：固定三枚（框架级命名；Xray 侧映射 general/access/dns 由主仓
/// 负责）。封闭枚举=非法通道名在类型层不可能，「注册 fail-visible」只剩
/// 重复注册一臂（[`ChannelError`]），无运行时字符串校验面。
#[cfg(feature = "log")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Channel {
    /// 通用/框架日志。
    General,
    /// 访问日志。
    Access,
    /// DNS 日志。
    Dns,
}

#[cfg(feature = "log")]
impl Channel {
    /// 通道名（每通道落盘子目录名与诊断文案；固定映射，禁运行时改名）。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Access => "access",
            Self::Dns => "dns",
        }
    }
}

/// 通道注册失败面（fail-visible；静默覆盖=通道日志无声丢失，禁）。
#[cfg(feature = "log")]
#[derive(Debug)]
pub enum ChannelError {
    /// 同通道已有 sink 时二次注册被拒；原 sink 保持不变。
    AlreadyRegistered {
        /// 被拒的通道。
        channel: Channel,
    },
}

#[cfg(feature = "log")]
impl std::fmt::Display for ChannelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyRegistered { channel } => write!(
                f,
                "channel `{}` already registered (duplicate registration is rejected)",
                channel.as_str()
            ),
        }
    }
}

#[cfg(feature = "log")]
impl std::error::Error for ChannelError {}

/// 通道 sink：一通道一 sink（禁共享文件锁竞争 + 禁级别串扰）。
///
/// 失败记账契约：[`ChannelSink::write`] 失败=丢一行，**由 sink 自计数**
/// （[`ChannelSink::dropped_lines`]）——registry 无从知晓单次写成败（write
/// 返回 `()`），记账下沉到唯一知道成败的一方。低于槽位级别的过滤发生在
/// registry（事件根本不到 sink；级别注册时取 [`Self::level`] 初值、运行期
/// [`ChannelRegistry::set_level`] 可改），**不**计丢弃。实现禁 panic
///（日志失败拖垮业务是反模式，同 [`FileSink`] 软失败面纪律）。
#[cfg(feature = "log")]
pub trait ChannelSink: Send + Sync + 'static {
    /// 本 sink 承载的通道（注册时由此定槽位）。
    fn channel(&self) -> Channel;
    /// 本通道独立级别下限的**注册初值**：register 时写入槽位 AtomicU8，此后
    /// 运行期判定只看槽位值（[`ChannelRegistry::set_level`] 可改写），本方法
    /// 不参与 emit 过滤路径（低于槽位级别的消息在 registry 即被滤，不计丢弃）。
    fn level(&self) -> log::LevelFilter;
    /// 写一条；失败=丢一行（sink 自计数 [`Self::dropped_lines`]，禁 panic）。
    fn write(&self, level: log::Level, target: &str, message: &str);
    /// 本通道累计丢弃行数（写失败口径；级别过滤与未注册静默不在内）。
    fn dropped_lines(&self) -> u64;
}

/// `log::LevelFilter` → 槽位原子存储的严重度秩（Off=0…Trace=5）。穷尽
/// match 而非判别式强转：不依赖 `repr` 布局，上游加枚举臂=编译错。
#[cfg(feature = "log")]
fn level_filter_rank(level: log::LevelFilter) -> u8 {
    match level {
        log::LevelFilter::Off => 0,
        log::LevelFilter::Error => 1,
        log::LevelFilter::Warn => 2,
        log::LevelFilter::Info => 3,
        log::LevelFilter::Debug => 4,
        log::LevelFilter::Trace => 5,
    }
}

/// `log::Level` → 事件严重度秩（Error=1…Trace=5，与 [`level_filter_rank`]
/// 同序同刻度：emit 比较「事件秩 > 槽位秩」即滤，与原
/// `level > sink.level()` 交叉比较语义逐值一致）。
#[cfg(feature = "log")]
fn level_rank(level: log::Level) -> u8 {
    match level {
        log::Level::Error => 1,
        log::Level::Warn => 2,
        log::Level::Info => 3,
        log::Level::Debug => 4,
        log::Level::Trace => 5,
    }
}

/// 多通道注册表：通道 → sink 一对一（固定三槽位，登记一次后零分配扇出）。
///
/// 并发面：每槽位独立 `RwLock`——register 只锁一个槽（互不阻塞），emit 读锁 +
/// `Arc` 克隆后锁外写 sink（锁内无 IO）。级别面无锁：每槽位独立 `AtomicU8`，
/// `set_level` 原子写、emit 原子读比较，不参与槽位 `RwLock`。毒锁复用
/// [`FileSink`] 纪律
///（`into_inner` 继续：日志面禁毒锁放大业务故障）。无注销面——注册表面向
/// 进程生命期的装配期使用（同全局 logger 单次初始化纪律）。
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use skeleton_infra::logx::{Channel, ChannelRegistry, ChannelSink};
///
/// struct NullSink(Channel);
/// impl ChannelSink for NullSink {
///     fn channel(&self) -> Channel { self.0 }
///     fn level(&self) -> log::LevelFilter { log::LevelFilter::Info }
///     fn write(&self, _level: log::Level, _target: &str, _message: &str) {}
///     fn dropped_lines(&self) -> u64 { 0 }
/// }
///
/// let registry = ChannelRegistry::new();
/// registry.register(Arc::new(NullSink(Channel::General))).expect("首注恒 Ok");
/// assert!(
///     registry.register(Arc::new(NullSink(Channel::General))).is_err(),
///     "重复注册必须 Err（fail-visible）"
/// );
/// // 未注册通道静默：无输出、无 panic、零分配
/// registry.emit(Channel::Dns, log::Level::Error, "app", "vanish");
/// ```
#[cfg(feature = "log")]
pub struct ChannelRegistry {
    /// 槽位按 `Channel` 判别式索引（General=0/Access=1/Dns=2）。
    slots: [std::sync::RwLock<Option<std::sync::Arc<dyn ChannelSink>>>; 3],
    /// 槽位级别下限：u8 严重度秩（Off=0…Trace=5）。register 以
    /// `sink.level()` 初始化（初值 0 在未注册期不可观测——emit 先查槽位
    /// None 即静默）；`set_level` 运行期原子改写；emit 写路径直接 load
    /// 比较（不经 sink、不经 tracing 层）。
    levels: [std::sync::atomic::AtomicU8; 3],
    /// 密钥掩码表（空=透传零分配）；Arc 快照读，emit 锁外掩码。
    secrets: std::sync::RwLock<std::sync::Arc<Vec<String>>>,
}

#[cfg(feature = "log")]
impl ChannelRegistry {
    /// 空表：全部通道未注册（emit 恒静默）——默认静默纪律的构造起点。
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| std::sync::RwLock::new(None)),
            levels: std::array::from_fn(|_| std::sync::atomic::AtomicU8::new(0)),
            secrets: std::sync::RwLock::new(std::sync::Arc::new(Vec::new())),
        }
    }

    /// 注册通道 sink（槽位由 [`ChannelSink::channel`] 定）。同通道已有
    /// sink=`Err`（fail-visible，原 sink 不动）。槽位级别以 `sink.level()`
    /// 初始化（构造期 level 配置入口）——装配顺序=先 register 后
    /// [`ChannelRegistry::set_level`]（register 会以 sink 初值覆盖槽位级）。
    pub fn register(&self, sink: std::sync::Arc<dyn ChannelSink>) -> Result<(), ChannelError> {
        let channel = sink.channel();
        let slot = &self.slots[channel as usize];
        let mut guard = slot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_some() {
            return Err(ChannelError::AlreadyRegistered { channel });
        }
        self.levels[channel as usize].store(
            level_filter_rank(sink.level()),
            std::sync::atomic::Ordering::Relaxed,
        );
        *guard = Some(sink);
        Ok(())
    }

    /// 运行期改写通道级别下限：原子写（无锁、不经 tracing 订阅者层），下一条
    /// emit 即按新级别判定——写路径比较天然免疫 callsite interest 缓存失效
    /// 竞态（armor 红线见模块文档）。未注册通道可写，但 register 会以 sink
    /// 初值覆盖（改级置于 register 之后）。低频配置面：emit 热路径只做一次
    /// 原子 load 比较，禁把改级当热路径开关频繁翻转。
    pub fn set_level(&self, channel: Channel, level: log::LevelFilter) {
        self.levels[channel as usize].store(
            level_filter_rank(level),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// 配置密钥掩码表：此后所有通道写前统一过 [`mask_secrets`]（唯一布点，
    /// 可 grep）。空表（默认）=透传零分配；替换语义（非累加）。
    pub fn set_secrets(&self, secrets: Vec<String>) {
        *self
            .secrets
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = std::sync::Arc::new(secrets);
    }

    /// 扇出一条：未注册通道=静默返回（不输出/不 panic/不分配/不计数）；
    /// 低于本通道槽位级别=过滤（不计数；AtomicU8 写路径比较，注册初值=
    /// `sink.level()`、[`Self::set_level`] 运行期可改）；命中=掩码（如配置）
    /// 后交 sink 写。
    pub fn emit(&self, channel: Channel, level: log::Level, target: &str, message: &str) {
        let sink = {
            let slot = &self.slots[channel as usize];
            let guard = slot.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            match guard.as_ref() {
                None => return,
                Some(sink) => {
                    let gate =
                        self.levels[channel as usize].load(std::sync::atomic::Ordering::Relaxed);
                    if level_rank(level) > gate {
                        return;
                    }
                    std::sync::Arc::clone(sink)
                }
            }
        };
        let secrets = self
            .secrets
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if secrets.is_empty() {
            sink.write(level, target, message);
        } else {
            let refs: Vec<&str> = secrets.iter().map(String::as_str).collect();
            let masked = mask_secrets(message, &refs);
            sink.write(level, target, &masked);
        }
    }

    /// 单通道丢弃计数（写失败口径；级别过滤/未注册静默不在内）。未注册
    /// 通道=`None`（无 sink 即无记账方，不伪造 0）。
    pub fn dropped(&self, channel: Channel) -> Option<u64> {
        let guard = self.slots[channel as usize]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.as_ref().map(|sink| sink.dropped_lines())
    }

    /// 全部通道丢弃计数总和（写失败口径；级别过滤/未注册静默不在内；=
    /// [`Self::dropped`] 三通道之和）。
    pub fn dropped_total(&self) -> u64 {
        self.slots.iter().fold(0u64, |acc, slot| {
            let guard = slot.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            acc + guard.as_ref().map(|s| s.dropped_lines()).unwrap_or(0)
        })
    }
}

#[cfg(feature = "log")]
impl Default for ChannelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// 通道文件落盘 sink：[`FileSink`]（按日滚动/软失败自计数/跨进程追加语义，
/// 见其类型文档）+ 通道 + 独立级别的组合件——宿主「每通道一个文件汇」的
/// 现成形态，亦可自定义 [`ChannelSink`] 替代（终端/网络面等）。
///
/// 行格式与 LineFormat 默认行格式同形：`[UTC 时间 LEVEL target] message`
///（整行单写，行完整性同 [`FileSink`] 单落盘单元前提）。行合成每行分配一次
/// String——零分配契约在 [`ChannelRegistry::emit`] 扇出面，sink 内行合成是
/// 写入方固有成本，非扇出分配。
#[cfg(feature = "log")]
pub struct ChannelFileSink {
    channel: Channel,
    level: log::LevelFilter,
    sink: FileSink,
}

#[cfg(feature = "log")]
impl ChannelFileSink {
    /// 建通道文件汇：`root` 通常取 [`PerChannelPathResolver::channel_root`]
    /// （每通道独立子目录）。根目录立即递归创建（配置错误构造期显式失败，
    /// 同 [`FileSink::new`]）。
    pub fn new(
        channel: Channel,
        level: log::LevelFilter,
        root: impl Into<std::path::PathBuf>,
    ) -> std::io::Result<Self> {
        Ok(Self {
            channel,
            level,
            sink: FileSink::new(root)?,
        })
    }
}

#[cfg(feature = "log")]
impl ChannelSink for ChannelFileSink {
    fn channel(&self) -> Channel {
        self.channel
    }

    fn level(&self) -> log::LevelFilter {
        self.level
    }

    fn write(&self, level: log::Level, target: &str, message: &str) {
        let ts = format_utc_timestamp(std::time::SystemTime::now());
        let line = format!("[{ts} {level} {target}] {message}\n");
        self.sink.write_line_soft(line.as_bytes());
    }

    fn dropped_lines(&self) -> u64 {
        self.sink.dropped_lines()
    }
}

/// 每通道路径解析：复用 [`LogPathResolver`] 契约作基根，通道名作子目录——
/// `channel_root(ch) = <base.log_root()>/<ch.as_str()>`，各通道落点互不相交
///（独立文件锁竞争面 + 独立按日滚动序列）。
///
/// # Examples
///
/// ```
/// use skeleton_infra::logx::{Channel, DefaultPathResolver, PerChannelPathResolver};
///
/// let base = DefaultPathResolver::new("agentwiki").expect("home 可解析");
/// let resolver = PerChannelPathResolver::new(base);
/// assert_eq!(
///     resolver.channel_root(Channel::Access).file_name(),
///     Some(std::ffi::OsStr::new("access")),
///     "通道名=子目录名"
/// );
/// ```
///
/// # 采用方警示（与 [`DefaultPathResolver`] 同源）
///
/// base 为 [`DefaultPathResolver`] 时，debug 构建基根=框架包目录（编译期
/// 锚），多通道默认同样落包目录 `log/{general,access,dns}/`——采用方 debug
/// 态必须注入自有 resolver（构造参数传自定义 [`LogPathResolver`]），release
/// 臂不受影响。
#[cfg(feature = "log")]
#[derive(Debug)]
pub struct PerChannelPathResolver<R> {
    base: R,
}

#[cfg(feature = "log")]
impl<R: LogPathResolver> PerChannelPathResolver<R> {
    /// 以既有 resolver 为基根（[`DefaultPathResolver`] 或采用方自有实现）。
    pub fn new(base: R) -> Self {
        Self { base }
    }

    /// 通道根目录（不预创建；由 [`ChannelFileSink::new`] 首建时递归创建）。
    pub fn channel_root(&self, channel: Channel) -> std::path::PathBuf {
        self.base.log_root().join(channel.as_str())
    }
}

/// UTC 纪元日；纪元前的时钟回拨钳到纪元（格式化恒全，不 panic）。
fn utc_epoch_days(t: std::time::SystemTime) -> i64 {
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    (d.as_secs() / 86_400) as i64
}

/// 民用日期（Howard Hinnant `civil_from_days`，公有域算法）：
/// days-since-epoch → (年, 月, 日)。手算锚与闰年钉在单元测试。
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `YYYY-MM-DD HH:MM:SS.mmm`（UTC；毫秒，拍板默认行格式的时间段）。
/// log 门绑定层专用（LineFormat 行时间戳）；`log-fs` 单开时 FileSink 写入方
/// 自带时间戳，本函数不参与。
#[cfg(feature = "log")]
fn format_utc_timestamp(t: std::time::SystemTime) -> String {
    let d = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs();
    let (y, mo, da) = civil_from_days((secs / 86_400) as i64);
    let sod = secs % 86_400;
    format!(
        "{y:04}-{mo:02}-{da:02} {:02}:{:02}:{:02}.{:03}",
        sod / 3_600,
        sod / 60 % 60,
        sod % 60,
        d.subsec_millis(),
    )
}

fn day_file_name(day: i64) -> String {
    let (y, m, d) = civil_from_days(day);
    format!("log-{y:04}-{m:02}-{d:02}.log")
}

/// 恢复文件名：主文件写/刷失败后接管的同日文件（#198）。新文件而非续写主
/// 文件——主文件尾部可能留半行前缀，续写会把新行接在半行后面。
fn recovery_file_name(day: i64) -> String {
    let (y, m, d) = civil_from_days(day);
    format!("log-{y:04}-{m:02}-{d:02}.recovery.log")
}

/// 默认行格式（拍板）：`[UTC 时间 LEVEL target] message key=value ...`。
/// 整行先合成单 String 再一次写出——fmt 层的多段写会被 FileSink 视作多个
/// 落盘单元（行破碎），单写是行完整性的实现侧前提。以本地单元结构实现
/// [`FormatEvent`]（泛型 fn 经 `as` 转换不得通过 HRTB 检查，类型化实现是
/// tracing-subscriber 0.3 文档正道）。
#[cfg(feature = "log")]
struct LineFormat;

#[cfg(feature = "log")]
impl<S, N> tracing_subscriber::fmt::FormatEvent<S, N> for LineFormat
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
    N: for<'a> tracing_subscriber::fmt::format::FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        _ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        let mut fields = String::new();
        event.record(&mut LineFields { out: &mut fields });
        let line = format!(
            "[{} {} {}]{}\n",
            format_utc_timestamp(std::time::SystemTime::now()),
            event.metadata().level(),
            event.metadata().target(),
            fields,
        );
        std::fmt::Write::write_str(&mut writer, &line)
    }
}

/// 字段收集：message 裸值在前，其余 ` key=value`（与 fmt 默认字段序同形；
/// record_str/record_i64 等默认实现都收敛到 record_debug，只覆写一臂）。
#[cfg(feature = "log")]
struct LineFields<'a> {
    out: &'a mut String,
}

#[cfg(feature = "log")]
impl tracing::field::Visit for LineFields<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        if field.name() == "message" {
            let _ = write!(self.out, " {value:?}");
        } else {
            let _ = write!(self.out, " {}={value:?}", field.name());
        }
    }
}

/// 文件+控制台双 sink 初始化（拍板 2026-10-17）：落点经
/// [`LogPathResolver`]（默认实现 [`DefaultPathResolver`]），两路同一默认
/// 行格式；过滤语义与 [`init_tracing_from_env`] 一致（`default_level` 为
/// RUST_LOG 未设/非法时的兜底，恒含 `skeleton=off`）。全局副作用一次性：
/// 重复调用或与其它 init 系并存=Err（禁静默覆盖）。两路 fmt 层各自取
/// `now()` 打行时间戳，同一行的 console/文件副本可差毫秒（cosmetic 上限）。
///
/// log→tracing 桥：`SubscriberInitExt::try_init` 在 tracing-subscriber 的
/// `tracing-log` feature（默认开）下内建——先装订阅者再装桥（带全局 max
/// level hint），与 `fmt().try_init()` 同机制；本件**不得**再显式调
/// `LogTracer::init`（先装桥会让 try_init 的内建桥调用撞 log 单例，恒
/// Err——0.3.23 源码 util.rs 实证）。本地资源（日志根目录）先建，任何
/// 配置错误在任何全局被触碰之前显式失败。
///
/// # 失败收尾契约（#130）
///
/// `AlreadyInit` 时只回收**本次调用新建**的空日志根（归属由非递归
/// `create` 的 `AlreadyExists` 原子判定）——既有根即使为空也属于调用方状态，
/// 一律保留；非空根自然保留（只做 `remove_dir`，从不递归删除）。
/// 残留竞态：另一个初始化者可能刚建根就被本调用回收，它的下一行会重建根
/// （0700）并续写——不丢行、不越权；该窗口不可移植地消除。
///
/// # Examples
///
/// ```no_run
/// use skeleton_infra::logx::{init_with_file, DefaultPathResolver};
///
/// let resolver = DefaultPathResolver::new("my_app").expect("home 可解析");
/// init_with_file(&resolver, log::LevelFilter::Info).expect("首调 Ok");
/// ```
#[cfg(feature = "log")]
pub fn init_with_file(
    resolver: &dyn LogPathResolver,
    default_level: log::LevelFilter,
) -> Result<(), LogInitError> {
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;

    let root = resolver.log_root();
    // #130：归属回执——只有本调用**新建**的根才允许在 init 失败时回收。
    let root_created_here = create_root(&root).map_err(|source| LogInitError::RootCreate {
        root: root.clone(),
        source,
    })?;
    let sink = std::sync::Arc::new(FileSink::new(root.clone()).map_err(|source| {
        LogInitError::RootCreate {
            root: root.clone(),
            source,
        }
    })?);

    tracing_subscriber::registry()
        .with(env_filter_or(default_level))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stdout)
                .event_format(LineFormat),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(FileSinkWriter(sink))
                .event_format(LineFormat),
        )
        .try_init()
        .map_err(|_| {
            // AlreadyInit：只回收**本次新建**的空日志根（#130——既有根即使为
            // 空也是调用方状态，`remove_dir` 成功不代表归属）；非空根自然保留。
            // 残留竞态（不可移植消除）：另一初始化者可能刚建根就被本调用回收，
            // 它的下一行会重建根（0700）并续写——不丢行、不越权。
            if root_created_here {
                let _ = std::fs::remove_dir(&root);
            }
            LogInitError::AlreadyInit
        })
}

#[cfg(all(test, feature = "log"))]
mod tests {
    use super::*;
    use log::LevelFilter;

    // ① init 首调 Ok、二调 Err(AlreadyInit)（同进程顺序断言）。
    // 全局单例语义 → init 系断言必须集中在唯一测试函数内，
    // 并发跑多个 init 测试会互相抢占全局 logger。
    #[test]
    fn init_lifecycle_first_ok_then_already_init() {
        init_tracing_default(LevelFilter::Info).expect("first init must succeed");
        assert!(matches!(
            init_tracing_default(LevelFilter::Debug),
            Err(AlreadyInit)
        ));
        // 全局订阅者已被占用：from_env 同样 Err（禁静默覆盖的可达断言）
        assert!(matches!(
            init_tracing_from_env(LevelFilter::Warn),
            Err(AlreadyInit)
        ));
    }

    // ⑤ from_env：RUST_LOG 未设 → 兜底 default_level+skeleton=off 预设
    //（框架埋点默认静默）；已设且合法 → 依其语义（显式值恒赢预设）。
    // 走构造器分支隔离（「测试内 Builder 隔离」出口），
    // 不经全局订阅者。
    #[test]
    fn env_filter_falls_back_to_default_level_when_unset() {
        let _env = env_lock();
        let _guard = EnvGuard::save("RUST_LOG");
        std::env::remove_var("RUST_LOG");
        let fallback = env_filter_or(LevelFilter::Warn);
        assert_eq!(
            fallback.to_string(),
            tracing_subscriber::EnvFilter::new("warn,skeleton=off").to_string()
        );

        std::env::set_var("RUST_LOG", "trace");
        let from_env = env_filter_or(LevelFilter::Warn);
        assert_eq!(
            from_env.to_string(),
            tracing_subscriber::EnvFilter::new("trace").to_string()
        );
        // 原值恢复交 EnvGuard::drop（panic 展开路径同样恢复）
    }

    /// flaky 治理：进程 env 是
    /// 全局单例——旧的「自改自清 remove」在 panic 路径漏清、且会覆盖外部注入的
    /// RUST_LOG 原值（单向清零）。守卫构造时留存原值、Drop 恢复，env 污染面
    /// 收敛为零；同二进制内后续 RUST_LOG 读者沿用本守卫 + env_lock 即可。
    struct EnvGuard {
        var: &'static str,
        saved: Option<String>,
    }

    impl EnvGuard {
        fn save(var: &'static str) -> Self {
            Self {
                var,
                saved: std::env::var(var).ok(),
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

    /// env 单例互斥（同二进制内 env 测试并行会互踩 RUST_LOG 读数窗）。
    /// std Mutex + 毒锁复用：与 tests/memgov.rs SERIAL 闸同形（零新依赖纪律）。
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 零覆盖 #13：RUST_LOG 非法语法 → EnvFilter 构造 Err → 兜底
    /// default_level+skeleton=off 预设，且 stderr fail-visible（配置错误禁静默
    /// 降级成另一档级别；try_from_default_env 的 Err 分支此前仅「未设值」面有
    /// 测；解析失败同样必须兜底，禁 panic 禁静默透传）。
    #[test]
    fn env_filter_falls_back_when_rust_log_is_invalid() {
        let _env = env_lock();
        let _guard = EnvGuard::save("RUST_LOG");
        std::env::set_var("RUST_LOG", "target=not_a_level");
        let fallback = env_filter_or(LevelFilter::Warn);
        assert_eq!(
            fallback.to_string(),
            tracing_subscriber::EnvFilter::new("warn,skeleton=off").to_string()
        );
    }
}

/// `log-fs` 面（纯 std）单元测试：密钥掩码。
#[cfg(test)]
mod mask_tests {
    use super::*;

    // ② mask 命中替换
    #[test]
    fn mask_replaces_hit_substrings() {
        let out = mask_secrets("GET /?key=abc123 done", &["abc123"]);
        assert_eq!(out, "GET /?key=*** done");
    }

    // ③ mask 最长优先（"sk-long"/"sk" 同现只留全掩，防半掩）
    #[test]
    fn mask_longest_secret_wins() {
        let out = mask_secrets("sk-long and sk", &["sk", "sk-long"]);
        assert_eq!(out, "*** and ***");
    }

    // ④ mask 空表/空串恒等；空串项跳过（禁在每字符位注入 "***"）
    #[test]
    fn mask_empty_table_or_empty_input_is_identity() {
        assert_eq!(mask_secrets("plain", &[]), "plain");
        assert_eq!(mask_secrets("", &["x"]), "");
        assert_eq!(mask_secrets("plain", &["", "x"]), "plain");
    }
}

/// 文件落盘面单元测试（纯函数 + 换日/软失败文件 IO）。CARGO_TARGET_TMPDIR
/// 仅集成测试目标可用（同 fsx 域纪律，禁系统 TEMP），此处 scratch 落
/// CARGO_TARGET_DIR 或 `target/` 回退，TempDir RAII 自清。
#[cfg(test)]
mod file_sink_tests {
    use super::*;
    use std::path::Path;
    use std::time::{Duration, SystemTime};
    use std_ext::fsx::temp_dir_in;

    fn scratch(tag: &str) -> std_ext::fsx::TempDir {
        let base = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target"));
        // lib 测试 cwd=package 根，workspace 共享 target 在仓根——相对回退时
        // base 可能不存在（temp_dir_in 用独占 create_dir 不建父链），先补建
        std::fs::create_dir_all(&base).expect("create scratch base");
        temp_dir_in(&base, &format!("logx-{tag}-")).expect("scratch under target/")
    }

    /// 记录型 reporter：断言「首行一次告警」与落点降级诊断的可达性（#184）。
    #[derive(Debug, Default)]
    struct RecordingReporter(std::sync::Mutex<Vec<String>>);

    impl RecordingReporter {
        fn messages(&self) -> Vec<String> {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl SoftFailureReporter for RecordingReporter {
        fn report(&self, message: &str) {
            self.0
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(message.to_string());
        }
    }

    /// 静默 reporter：模拟坏 stderr（诊断全丢）——软失败契约不得依赖它成功。
    #[derive(Debug, Default)]
    struct SilentReporter;

    impl SoftFailureReporter for SilentReporter {
        fn report(&self, _message: &str) {}
    }

    /// 注入替身故障面（#198 要求可模拟「半写后报错」与「flush 报错」）。
    #[derive(Clone, Copy)]
    enum Fault {
        /// 写入半行即报错：记录边界不确定。
        PartialWrite,
        /// 整行写入成功、flush 报错。
        Flush,
        /// 恢复文件也立即写失败：当日应落 fail-stop。
        AlwaysWrite,
    }

    struct FaultyWriter {
        file: std::fs::File,
        fault: Fault,
    }

    impl std::io::Write for FaultyWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            match self.fault {
                Fault::Flush => self.file.write(buf),
                Fault::PartialWrite => {
                    let half = buf.len() / 2;
                    self.file.write_all(&buf[..half])?;
                    Err(std::io::Error::other("injected partial write failure"))
                }
                Fault::AlwaysWrite => Err(std::io::Error::other("injected write failure")),
            }
        }

        fn flush(&mut self) -> std::io::Result<()> {
            match self.fault {
                Fault::Flush => Err(std::io::Error::other("injected flush failure")),
                Fault::PartialWrite | Fault::AlwaysWrite => self.file.flush(),
            }
        }
    }

    /// 装作「当日落点句柄已打开、下一次写失败」：把替身塞进锁内状态。
    fn install_writer(
        sink: &FileSink,
        day: i64,
        target: Target,
        path: &std::path::Path,
        fault: Fault,
    ) {
        let file = std::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .expect("faulty writer backing file");
        let mut state = sink
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.day = day;
        state.target = target;
        state.writer = Some(Box::new(FaultyWriter { file, fault }));
    }

    /// 当日 scratch 根下的文件数（断言 fail-stop 不造新文件）。
    fn file_count(root: &std::path::Path) -> usize {
        std::fs::read_dir(root)
            .expect("log root readable")
            .map(|e| e.expect("dir entry"))
            .filter(|e| e.path().is_file())
            .count()
    }

    // 民用日期转换：纪元锚+闰年（手工计数交叉验证——2000 世纪闰走算法的
    // /36_524 修正臂，必须钉死）。
    #[test]
    fn civil_from_days_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29)); // 世纪闰
        assert_eq!(civil_from_days(19_782), (2024, 2, 29)); // 19723(2024-01-01)+59
        assert_eq!(civil_from_days(20_743), (2026, 10, 17)); // 20454(2026-01-01)+289
    }

    // 时间段格式：当日秒偏移与毫秒钉死（文件行格式的时间段直接对表）。
    // format_utc_timestamp 属 log 门绑定层（LineFormat），log-fs 单开时不可达。
    #[cfg(feature = "log")]
    #[test]
    fn utc_timestamp_format_pinned() {
        assert_eq!(
            format_utc_timestamp(SystemTime::UNIX_EPOCH),
            "1970-01-01 00:00:00.000"
        );
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(59_999);
        assert_eq!(format_utc_timestamp(t), "1970-01-01 00:00:59.999");
        let t = SystemTime::UNIX_EPOCH + Duration::from_secs(20_743 * 86_400 + 45_296);
        assert_eq!(format_utc_timestamp(t), "2026-10-17 12:34:56.000");
    }

    #[test]
    fn day_file_name_matches_utc_date() {
        assert_eq!(day_file_name(0), "log-1970-01-01.log");
        assert_eq!(day_file_name(20_743), "log-2026-10-17.log");
        assert_eq!(recovery_file_name(0), "log-1970-01-01.recovery.log");
        assert_eq!(recovery_file_name(20_743), "log-2026-10-17.recovery.log");
    }

    // release 布局纯函数（env 无关面；env 粘合面在 tests/log_file.rs --release）。
    // 同 gate 到 release profile：该函数在 debug 分支不存在。
    #[cfg(not(debug_assertions))]
    #[test]
    fn release_root_layout_dotted_project() {
        assert_eq!(
            release_root(Path::new("/home/u"), "agentwiki"),
            Path::new("/home/u").join(".agentwiki").join("log")
        );
    }

    // #175 条 3：最终根必须仍在 home 之下（不只靠字符串前缀——用组件级
    // containment 独立复核归一后的布局）。
    #[cfg(not(debug_assertions))]
    #[test]
    fn release_root_stays_under_home() {
        let home = Path::new("/home/u");
        let root = release_root(home, "agentwiki");
        assert!(
            std_ext::paths::ensure_inside(home, &root, std_ext::paths::CaseStrategy::CaseSensitive)
                .is_ok(),
            "release 根必须位于 home 之内: {root:?}"
        );
    }

    // #175/#113：project_name 必须是单一 Normal 组件；debug 也校验（名字未参与
    // 布局不是接受非法值的理由），故本测试两构建同跑。
    #[test]
    fn resolver_rejects_non_component_project_names() {
        for bad in [
            "",
            ".",
            "..",
            "foo/../../outside",
            "a/b",
            "a\\b",
            "\\server\\share",
            "foo\0bar",
            "foo\nbar",
            "./agentwiki",
            "agentwiki/",
        ] {
            match DefaultPathResolver::new(bad) {
                Err(LogPathError::InvalidProjectName { name, .. }) => {
                    assert_eq!(name, bad, "错误须回传原始名字");
                }
                Ok(_) => panic!("traversal/separator name must be rejected: {bad:?}"),
                Err(other) => panic!("expected InvalidProjectName for {bad:?}, got {other:?}"),
            }
        }
    }

    // Windows 平台前缀形（盘符/verbatim）由 components() 判为非 Normal ⇒ 拒绝。
    #[cfg(windows)]
    #[test]
    fn resolver_rejects_drive_and_unc_prefixed_names() {
        for bad in ["C:", "C:foo", r"\\?\C:\x", r"\\server\share"] {
            assert!(
                matches!(
                    DefaultPathResolver::new(bad),
                    Err(LogPathError::InvalidProjectName { .. })
                ),
                "Windows 前缀形名字必须被拒: {bad:?}"
            );
        }
    }

    // Unix 上 `C:foo` 不含分隔符、是普通目录名（平台语义由 components() 决定，
    // 禁跨平台臆断）。debug 分支不解析 home，故只在 debug 断言接受。
    #[cfg(all(not(windows), debug_assertions))]
    #[test]
    fn resolver_accepts_colon_name_on_unix() {
        assert!(
            DefaultPathResolver::new("C:foo").is_ok(),
            "Unix 上冒号是合法目录名字符"
        );
    }

    #[cfg(debug_assertions)]
    #[test]
    fn resolver_accepts_single_component_names() {
        for good in ["agentwiki", ".hidden", "a.b-c_d", "名字", "a b"] {
            assert!(
                DefaultPathResolver::new(good).is_ok(),
                "合法单一组件名字必须被接受: {good:?}"
            );
        }
    }

    // debug 构建编译期锚：恒 = CARGO_MANIFEST_DIR/log，home 不参与。
    #[cfg(debug_assertions)]
    #[test]
    fn default_resolver_debug_root_is_manifest_log() {
        let r = DefaultPathResolver::new("anything").expect("debug 构建不解析 home");
        assert_eq!(
            r.log_root(),
            Path::new(env!("CARGO_MANIFEST_DIR")).join("log")
        );
    }

    // 换日滚动：同日追加、跨日新文件；两日文件内容互不污染。
    #[test]
    fn rollover_switches_file_by_utc_day_and_appends_within_day() {
        let scratch = scratch("roll");
        let root = scratch.path().join("logs");
        let sink = FileSink::new(&root).expect("root creatable");
        let d0 = SystemTime::UNIX_EPOCH + Duration::from_secs(20_743 * 86_400 + 3_600);
        let d1 = d0 + Duration::from_secs(86_400);

        sink.try_write_line(d0, b"one\n").expect("day0 line");
        sink.try_write_line(d0, b"two\n").expect("day0 append");
        sink.try_write_line(d1, b"three\n").expect("day1 line");

        let f0 = std::fs::read_to_string(root.join("log-2026-10-17.log")).expect("day0 file");
        assert_eq!(f0, "one\ntwo\n");
        let f1 = std::fs::read_to_string(root.join("log-2026-10-18.log")).expect("day1 file");
        assert_eq!(f1, "three\n");
        assert_eq!(sink.dropped_lines(), 0);
    }

    // 软失败面：落点被文件占住 → 丢弃计数增长且不 panic（告警走注入 reporter，
    // 以计数器与 reporter 记录为断言面）；落点恢复 → 后续行自然落盘，计数停涨。
    #[test]
    fn write_failure_counts_warns_once_and_recovers() {
        use std::io::Write as _;
        let scratch = scratch("soft");
        let root = scratch.path().join("blocked");
        std::fs::write(&root, b"x").expect("placeholder file at root path");

        // 绕过 with_reporter()（其对被占根显式失败）直接构造，模拟「建汇成功后
        // 落点被占」。
        let reporter = std::sync::Arc::new(RecordingReporter::default());
        let mut sink = FileSink {
            root: root.clone(),
            state: std::sync::Mutex::new(SinkState {
                day: i64::MIN,
                target: Target::Main,
                writer: None,
            }),
            dropped: std::sync::atomic::AtomicU64::new(0),
            warned: std::sync::atomic::AtomicBool::new(false),
            reporter: reporter.clone(),
        };

        sink.write_all(b"lost-1\n").expect("soft path never errs");
        sink.write_all(b"lost-2\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 2);
        assert_eq!(
            reporter.messages().len(),
            1,
            "首行一次告警（one-warning 策略不变）"
        );

        // 落点恢复（文件→目录）→ 第三行自然落盘，计数停涨。
        std::fs::remove_file(&root).expect("unblock");
        std::fs::create_dir_all(&root).expect("restore dir");
        sink.write_all(b"ok\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 2);
        assert_eq!(reporter.messages().len(), 1, "恢复后不再告警");
        let today = day_file_name(utc_epoch_days(SystemTime::now()));
        assert_eq!(
            std::fs::read_to_string(root.join(today)).expect("file"),
            "ok\n"
        );
    }

    // #184：软失败契约不依赖诊断出口成功——坏 stderr（诊断全丢）下仍返回整行
    // 长度、不 panic、丢弃计数权威。
    #[test]
    fn soft_failure_survives_broken_reporter() {
        use std::io::Write as _;
        let scratch = scratch("broken-reporter");
        let root = scratch.path().join("blocked");
        std::fs::write(&root, b"x").expect("placeholder file at root path");
        let mut sink = FileSink {
            root: root.clone(),
            state: std::sync::Mutex::new(SinkState {
                day: i64::MIN,
                target: Target::Main,
                writer: None,
            }),
            dropped: std::sync::atomic::AtomicU64::new(0),
            warned: std::sync::atomic::AtomicBool::new(false),
            reporter: std::sync::Arc::new(SilentReporter),
        };

        assert_eq!(
            sink.write(b"line\n").expect("soft path never errs"),
            5,
            "write 恒返回整行长度（软失败面）"
        );
        assert_eq!(sink.write(b"line\n").expect("soft path never errs"), 5);
        assert_eq!(sink.dropped_lines(), 2, "丢弃计数不依赖诊断出口");
    }

    // #112/#198：写失败后句柄必须退役（半行前缀留在原文件），下一行改落恢复
    // 文件——新行不接在半行后面；只失败那一行计丢弃。
    #[test]
    fn partial_write_failure_retires_writer_and_rolls_to_recovery_file() {
        use std::io::Write as _;
        let scratch = scratch("partial");
        let root = scratch.path().join("logs");
        let reporter = std::sync::Arc::new(RecordingReporter::default());
        let mut sink = FileSink::with_reporter(&root, reporter.clone()).expect("root creatable");

        let day = utc_epoch_days(SystemTime::now());
        let main_path = root.join(day_file_name(day));
        install_writer(&sink, day, Target::Main, &main_path, Fault::PartialWrite);

        let failed_line = b"record-1\n";
        sink.write_all(failed_line).expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 1, "失败行计丢弃");

        sink.write_all(b"record-2\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 1, "重开后新行不再丢");

        let main = std::fs::read_to_string(&main_path).expect("main file readable");
        assert_eq!(
            main, "reco",
            "主文件尾巴=注入的半行前缀（不截断、不修复）：{main:?}"
        );
        let recovery_path = root.join(recovery_file_name(day));
        assert_eq!(
            std::fs::read_to_string(&recovery_path).expect("recovery file"),
            "record-2\n",
            "新行只进恢复文件，不接半行后"
        );

        let messages = reporter.messages();
        assert_eq!(
            messages.len(),
            2,
            "首失败一次告警 + 落点降级一次诊断: {messages:?}"
        );
        assert!(
            messages.iter().any(|m| m.contains("recovery")),
            "降级诊断须点名恢复落点: {messages:?}"
        );
    }

    // #198：flush 失败同属「记录边界不确定」——整行虽已写入，句柄仍须退役并切
    // 恢复文件（禁把后续行追加到可疑句柄）。
    #[test]
    fn flush_failure_retires_writer_and_rolls_to_recovery_file() {
        use std::io::Write as _;
        let scratch = scratch("flush");
        let root = scratch.path().join("logs");
        let mut sink =
            FileSink::with_reporter(&root, std::sync::Arc::new(SilentReporter)).expect("root");

        let day = utc_epoch_days(SystemTime::now());
        let main_path = root.join(day_file_name(day));
        install_writer(&sink, day, Target::Main, &main_path, Fault::Flush);

        sink.write_all(b"record-1\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 1);
        sink.write_all(b"record-2\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 1);

        assert_eq!(
            std::fs::read_to_string(&main_path).expect("main file"),
            "record-1\n",
            "主文件保持原样（不重复写、不追加新行）"
        );
        assert_eq!(
            std::fs::read_to_string(root.join(recovery_file_name(day))).expect("recovery file"),
            "record-2\n"
        );
    }

    // #198：恢复文件也失败 → 当日 fail-stop：后续行只计数、不再开新文件（禁
    // 每次失败造文件把故障放大），dropped_lines 逐行准确。
    #[test]
    fn recovery_failure_fail_stops_without_creating_more_files() {
        use std::io::Write as _;
        let scratch = scratch("fail-stop");
        let root = scratch.path().join("logs");
        let mut sink =
            FileSink::with_reporter(&root, std::sync::Arc::new(SilentReporter)).expect("root");

        let day = utc_epoch_days(SystemTime::now());
        let main_path = root.join(day_file_name(day));
        let recovery_path = root.join(recovery_file_name(day));

        install_writer(&sink, day, Target::Main, &main_path, Fault::PartialWrite);
        sink.write_all(b"record-1\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 1);

        install_writer(
            &sink,
            day,
            Target::Recovery,
            &recovery_path,
            Fault::AlwaysWrite,
        );
        sink.write_all(b"record-2\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 2);
        assert_eq!(
            sink.state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .target,
            Target::Stopped,
            "恢复文件也失败 → 当日 fail-stop（不再有新落点可试）"
        );

        // 当日已 fail-stop：不再尝试 IO，逐行计丢弃。
        sink.write_all(b"record-3\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 3);

        assert_eq!(
            file_count(&root),
            2,
            "只存在主文件与恢复文件，fail-stop 不造新文件"
        );
    }

    // 换日必须把落点阶梯归零：上一日 poisoned 不得影响新一日的正常文件。
    #[test]
    fn day_rollover_resets_poisoned_target_to_main_file() {
        let scratch = scratch("rollover-poison");
        let root = scratch.path().join("logs");
        let sink = FileSink::with_reporter(&root, std::sync::Arc::new(SilentReporter))
            .expect("root creatable");
        let d0 = SystemTime::UNIX_EPOCH + Duration::from_secs(20_743 * 86_400 + 3_600);
        let d1 = d0 + Duration::from_secs(86_400);
        let day0 = utc_epoch_days(d0);
        let day1 = utc_epoch_days(d1);

        install_writer(
            &sink,
            day0,
            Target::Main,
            &root.join(day_file_name(day0)),
            Fault::AlwaysWrite,
        );
        assert!(sink.try_write_line(d0, b"lost\n").is_err(), "注入故障面");
        assert_eq!(
            sink.dropped_lines(),
            0,
            "try_write_line 不做计数（软面在 write_line_soft）"
        );

        sink.try_write_line(d1, b"fresh\n").expect("新一日主文件");
        assert_eq!(
            std::fs::read_to_string(root.join(day_file_name(day1))).expect("day1 file"),
            "fresh\n",
            "换日回落主文件而不是上一日的恢复文件"
        );
        assert!(
            !root.join(recovery_file_name(day1)).exists(),
            "新一日不产生恢复文件"
        );
    }

    // #184 Round 2：sink 失败时 MakeWriter 壳仍返回 Ok(len)——tracing-subscriber
    // 0.3 只在 writer 返回 Err 时走自己的 stderr fallback，那会绕过本件的软失败
    // 面（重抛日志 IO 错误到业务）。
    #[cfg(feature = "log")]
    #[test]
    fn file_sink_writer_keeps_full_length_on_sink_failure() {
        use std::io::Write as _;
        let scratch = scratch("writer-shell");
        let root = scratch.path().join("logs");
        let sink = std::sync::Arc::new(
            FileSink::with_reporter(&root, std::sync::Arc::new(SilentReporter)).expect("root"),
        );
        let day = utc_epoch_days(SystemTime::now());
        install_writer(
            &sink,
            day,
            Target::Main,
            &root.join(day_file_name(day)),
            Fault::AlwaysWrite,
        );

        let mut writer = FileSinkWriter::new(std::sync::Arc::clone(&sink));
        assert_eq!(
            writer.write(b"line\n").expect("软失败面恒 Ok"),
            5,
            "壳不得把 sink 失败变成 io::Error"
        );
        assert_eq!(sink.dropped_lines(), 1);
    }
}
