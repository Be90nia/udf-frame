//! 日志域两层拆分（批 1 log feature 拆分，2026-10-18；ADR
//! `docs/adr/0002-log-feature-split.md`）：
//!
//! - **`log-fs`（纯 std，零三方依赖）**：密钥掩码 [`mask_secrets`] + 文件落盘
//!   面——路径契约 [`LogPathResolver`](skeleton::logpath::LogPathResolver)
//!   （trait 住 `skeleton::logpath`）+ 默认实现 [`DefaultPathResolver`] +
//!   按日滚动 [`FileSink`]。单开 `log-fs` 即可用文件日志，不拉 tracing 四件套。
//! - **`log`（= `log-fs` + log + tracing + tracing-subscriber + tracing-log，
//!   门面绑定层）**：init 系（[`init_tracing_from_env`]/[`init_with_file`]）、
//!   [`FileSinkWriter`]（MakeWriter 适配壳）、默认行格式（LineFormat）。
//!   门面 trait=生态 `log::Log`（不自造）。
//!
//! 级别过滤=订阅者层全局上限；per-module 交给 [`init_tracing_from_env`] 的
//! EnvFilter。核心自身永不打日志（核心零依赖，结构性排除）。
//!
//! 域级过滤预设：[`init_tracing_from_env`] 在 RUST_LOG 未设/非法时兜底的
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
//! 必须可见，禁静默降级成另一档级别）。
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
//! 文件落盘面（拍板 2026-10-17）：[`init_with_file`] = console+file 双 sink
//! （同一默认行格式）；落点经 `skeleton::logpath::LogPathResolver` 契约——
//! 默认实现 [`DefaultPathResolver`]，debug 构建=编译期 `CARGO_MANIFEST_DIR`/log
//!（框架自身开发态，零运行时猜测；采方开发态请注入自有 resolver，见
//! `skeleton::logpath` trait 文档示例），release 构建=
//! `home_dir()/.<project_name>/log/`，home 不可解析=构造期显式 Err（禁静默
//! 落 cwd）。[`FileSink`] 按日滚动 `log-YYYY-MM-DD.log` 纯追加（跨进程按行
//! 交错安全，永不改名/截断），每行 flush；写失败=计数+stderr 一次告警+继续
//!（禁 panic 拖垮业务、禁纯吞）。时间戳与文件名恒 UTC（std 无本地时区 API，
//! chrono 依赖判否）。
//!
//! 多通道扇出面（P1-10-a 前置件，拍板 2026-10-23；`log` feature）：固定三
//! 通道 [`Channel`]（general/access/dns，封闭枚举=非法通道名在类型层不可
//! 能）→ 每通道独立 sink（一通道一 sink 禁串扰）+ 独立 level（低于通道级
//! 在 registry 即滤，不计丢弃）+ 重复注册 fail-visible（原 sink 不动）+
//! [`ChannelRegistry`] 零分配扇出（未注册通道静默：不输出/不 panic/不分配/
//! 不计数）+ 掩码唯一布点（`set_secrets`，写前统一过 [`mask_secrets`]）+
//! 每通道独立落点（[`PerChannelPathResolver`] 通道子目录 +
//! [`ChannelFileSink`] 组合件）。

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
                eprintln!(
                    "skeleton-infra::logx: RUST_LOG={spec:?} 语法非法（{err}）；\
                     回退默认过滤 `{}`+skeleton=off（框架域静默）",
                    default_level,
                );
                fallback()
            }
        },
        Err(std::env::VarError::NotPresent) => fallback(), // 未设：正常兜底，非配置错误
        Err(err) => {
            // fail-visible 对齐（审计吹毛）：非 Unicode 值同属「已设但不可
            // 解析」面，禁静默降级
            eprintln!(
                "skeleton-infra::logx: RUST_LOG 不可解析（{err}）；\
                 回退默认过滤 `{}`+skeleton=off（框架域静默）",
                default_level,
            );
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
/// reports/2026-10-16-perf-batchc.md）：无命中纯扫描近线性 O(n×m)——4KiB×
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
/// UWP 恒 None——工程内常规目标前两者有平台回退，此分支属防御面）。落地禁
/// 静默回退 cwd，故构造期显式 Err。
#[derive(Debug)]
pub struct HomeDirUnresolvable;

impl std::fmt::Display for HomeDirUnresolvable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "home directory unresolvable (std::env::home_dir returned None); \
             refusing to fall back to the current directory — set USERPROFILE/HOME \
             or supply a custom LogPathResolver"
        )
    }
}

impl std::error::Error for HomeDirUnresolvable {}

/// 默认路径解析器（拍板 2026-10-17）：debug 构建=编译期
/// `CARGO_MANIFEST_DIR`/log（框架自身开发态，零运行时猜测）；release 构建=
/// `<home_dir>/.<project_name>/log/`——**release 恒不引用 CARGO_MANIFEST_DIR**
/// （那会把开发者机器路径烧进发布物，隐私面；测试钉死）。`project_name`
/// 为构造参数。
///
/// home 在构造期一次性解析：解析失败显式 Err（禁静默落 cwd），此后
/// [`LogPathResolver::log_root`] 恒不可失败。
///
/// # Examples
///
/// ```
/// let r = skeleton_infra::logx::DefaultPathResolver::new("agentwiki")
///     .expect("debug 构建不解析 home，恒 Ok；release 构建需 home 可解析");
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
    /// debug 构建恒 Ok（home 不参与）；release 构建立即解析 home，
    /// None → [`Err(HomeDirUnresolvable)`]。
    pub fn new(project_name: impl Into<String>) -> Result<Self, HomeDirUnresolvable> {
        #[cfg(not(debug_assertions))]
        {
            let home = std_ext::paths::home_dir().ok_or(HomeDirUnresolvable)?;
            Ok(Self {
                project_name: project_name.into(),
                home,
            })
        }
        #[cfg(debug_assertions)]
        {
            let _ = project_name.into(); // debug 分支零字段，参数仅签名存在
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

/// 按日滚动的追加式文件日志汇（诊断面）。
///
/// 跨进程并发安全（armor，std 文档逐条对表）：纯追加**永不改名/截断**——
/// 换日只是打开新文件，无 rotation 改名交接面；`OpenOptions::append` 在
/// Unix=O_APPEND、Windows=FILE_APPEND_DATA，均按次 write() 原子追加到 EOF；
/// std 默认以 FILE_SHARE_READ|WRITE|DELETE 打开，不挡并发打开者。每次
/// `write` 调用=一个落盘单元（fmt 层经 [`format_line`] 保证整行单调用）。
///
/// 失败面（拍板）：写失败**不 panic、不上抛**——计数 + stderr 一次告警 +
/// 继续（日志失败拖垮业务是反模式；纯吞禁——[`FileSink::dropped_lines`]
/// 可观测）。恢复自然发生：下一行重走打开路径。
///
/// 边缘语义（固有 fd 行为，非缺陷）：日志根被外部删除后，长命句柄的续行
/// 写入已解除链接的文件（Windows share-delete/POSIX unlink 同构）——行入
/// void 且 `dropped_lines` 不涨（写未失败）、目录不重建、直到换日才重走
/// 打开路径。按天滚动天然限损 ≤1 天；需即时感知者自管根目录生命周期。
///
/// 时间戳/文件名恒 UTC（std 无本地时区 API，chrono 依赖判否）。
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
}

#[derive(Debug)]
struct SinkState {
    /// 当前文件对应的 UTC 纪元日（换日即换文件）。
    day: i64,
    writer: Option<std::io::BufWriter<std::fs::File>>,
}

impl FileSink {
    /// 建汇：立即递归创建根目录（配置错误在此显式失败，禁等到首行才爆）；
    /// 文件按日懒开（无事件不落空文件）。
    pub fn new(root: impl Into<std::path::PathBuf>) -> std::io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            state: std::sync::Mutex::new(SinkState {
                day: i64::MIN,
                writer: None,
            }),
            dropped: std::sync::atomic::AtomicU64::new(0),
            warned: std::sync::atomic::AtomicBool::new(false),
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
                    eprintln!(
                        "skeleton-infra::logx: log file write failed (root={}, \
                         error={err}); dropped lines so far: {dropped} — \
                         application keeps running",
                        self.root.display(),
                    );
                }
                buf.len()
            }
        }
    }

    /// 单次落盘：换日（或首行）→ 递归建根 + 打开 `log-YYYY-MM-DD.log`
    /// （追加）；整行一次 write_all + flush（每行 flush 拍板）。锁内串行
    /// =单进程行完整。
    fn try_write_line(&self, now: std::time::SystemTime, buf: &[u8]) -> std::io::Result<()> {
        let day = utc_epoch_days(now);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.writer.is_none() || state.day != day {
            std::fs::create_dir_all(&self.root)?;
            let path = self.root.join(day_file_name(day));
            let file = std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)?;
            state.writer = Some(std::io::BufWriter::new(file));
            state.day = day;
        }
        // ponytail: 极端跨进程竞争下行仍可能按 write() 边界交错（常规文件
        // 单次几 KiB 写实际恒整写）；进程内由 Mutex 保证行完整。若未来出现
        // 真实交错现场，升级=绕过 BufWriter 每行单 write 系统调用直写。
        let writer = state.writer.as_mut().expect("writer opened above");
        std::io::Write::write_all(writer, buf)?;
        std::io::Write::flush(writer)
    }
}

impl std::io::Write for FileSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // 软失败面：恒 Ok（丢弃走计数+stderr；io 层 Err 会被 fmt 层吞掉，
        // 在这里截住才有可观测计数）。
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
/// 返回 `()`），记账下沉到唯一知道成败的一方。低于 [`ChannelSink::level`]
/// 的过滤发生在 registry（事件根本不到 sink），**不**计丢弃。实现禁 panic
///（日志失败拖垮业务是反模式，同 [`FileSink`] 软失败面纪律）。
#[cfg(feature = "log")]
pub trait ChannelSink: Send + Sync + 'static {
    /// 本 sink 承载的通道（注册时由此定槽位）。
    fn channel(&self) -> Channel;
    /// 本通道独立级别下限（低于此级别的消息在 registry 即被过滤，不计丢弃）。
    fn level(&self) -> log::LevelFilter;
    /// 写一条；失败=丢一行（sink 自计数 [`Self::dropped_lines`]，禁 panic）。
    fn write(&self, level: log::Level, target: &str, message: &str);
    /// 本通道累计丢弃行数（写失败口径；级别过滤与未注册静默不在内）。
    fn dropped_lines(&self) -> u64;
}

/// 多通道注册表：通道 → sink 一对一（固定三槽位，登记一次后零分配扇出）。
///
/// 并发面：每槽位独立 `RwLock`——register 只锁一个槽（互不阻塞），emit 读锁加
/// `Arc` 克隆后锁外写 sink（锁内无 IO）。毒锁复用 [`FileSink`] 纪律
/// （`into_inner` 继续：日志面禁毒锁放大业务故障）。无注销面——注册表面向
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
    /// 密钥掩码表（空=透传零分配）；Arc 快照读，emit 锁外掩码。
    secrets: std::sync::RwLock<std::sync::Arc<Vec<String>>>,
}

#[cfg(feature = "log")]
impl ChannelRegistry {
    /// 空表：全部通道未注册（emit 恒静默）——默认静默纪律的构造起点。
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| std::sync::RwLock::new(None)),
            secrets: std::sync::RwLock::new(std::sync::Arc::new(Vec::new())),
        }
    }

    /// 注册通道 sink（槽位由 [`ChannelSink::channel`] 定）。同通道已有
    /// sink=`Err`（fail-visible，原 sink 不动）。
    pub fn register(&self, sink: std::sync::Arc<dyn ChannelSink>) -> Result<(), ChannelError> {
        let slot = &self.slots[sink.channel() as usize];
        let mut guard = slot
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.is_some() {
            return Err(ChannelError::AlreadyRegistered {
                channel: sink.channel(),
            });
        }
        *guard = Some(sink);
        Ok(())
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
    /// 低于本通道 level=过滤（不计数）；命中=掩码（如配置）后交 sink 写。
    pub fn emit(&self, channel: Channel, level: log::Level, target: &str, message: &str) {
        let sink = {
            let slot = &self.slots[channel as usize];
            let guard = slot.read().unwrap_or_else(|poisoned| poisoned.into_inner());
            match guard.as_ref() {
                None => return,
                Some(sink) => {
                    if level > sink.level() {
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

    /// 全部通道丢弃计数总和（写失败口径；级别过滤/未注册静默不在内）。
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
            // AlreadyInit：尽力回收本次新建的空日志根（审计吹毛：失败路径
            // 副作用不残留）。remove_dir 只删空目录——根非空=本来就有内容，
            // 自然失败保留，不掩盖原错误。
            let _ = std::fs::remove_dir(&root);
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

    // 软失败面：落点被文件占住 → 丢弃计数增长且不 panic（stderr 一次告警为
    // 进程副作用，以计数器为断言面）；落点恢复 → 后续行自然落盘，计数停涨。
    #[test]
    fn write_failure_counts_warns_once_and_recovers() {
        use std::io::Write as _;
        let scratch = scratch("soft");
        let root = scratch.path().join("blocked");
        std::fs::write(&root, b"x").expect("placeholder file at root path");

        // 绕过 new()（其对被占根显式失败）直接构造，模拟「建汇成功后落点被占」。
        let mut sink = FileSink {
            root: root.clone(),
            state: std::sync::Mutex::new(SinkState {
                day: i64::MIN,
                writer: None,
            }),
            dropped: std::sync::atomic::AtomicU64::new(0),
            warned: std::sync::atomic::AtomicBool::new(false),
        };

        sink.write_all(b"lost-1\n").expect("soft path never errs");
        sink.write_all(b"lost-2\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 2);

        // 落点恢复（文件→目录）→ 第三行自然落盘，计数停涨。
        std::fs::remove_file(&root).expect("unblock");
        std::fs::create_dir_all(&root).expect("restore dir");
        sink.write_all(b"ok\n").expect("soft path never errs");
        assert_eq!(sink.dropped_lines(), 2);
        let today = day_file_name(utc_epoch_days(SystemTime::now()));
        assert_eq!(
            std::fs::read_to_string(root.join(today)).expect("file"),
            "ok\n"
        );
    }
}
