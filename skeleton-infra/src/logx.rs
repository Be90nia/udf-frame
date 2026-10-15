//! log 门面绑定：初始化器与密钥掩码。门面 trait=生态 `log::Log`（不自造）。
//!
//! 级别过滤=订阅者层全局上限；per-module 交给 [`init_tracing_from_env`] 的
//! EnvFilter。核心自身永不打日志（核心零依赖，结构性排除）。
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

/// 全局 logger 已被占用。重复初始化=显式 Err（禁静默覆盖）。
#[derive(Debug)]
pub struct AlreadyInit;

impl std::fmt::Display for AlreadyInit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "global logger already initialized")
    }
}

impl std::error::Error for AlreadyInit {}

/// 初始化推荐组合：tracing-log 桥（log→tracing）+ tracing fmt 订阅者。
/// 全局副作用一次性；重复调用=Err（禁静默覆盖）。`level` 为全局上限过滤。
pub fn init_tracing_default(level: log::LevelFilter) -> Result<(), AlreadyInit> {
    tracing_subscriber::fmt()
        .with_max_level(to_tracing_filter(level))
        .try_init()
        .map_err(|_| AlreadyInit)
}

/// 同 [`init_tracing_default`]，但级别经 RUST_LOG 语义 EnvFilter
/// （`default_level` 为环境未设时的兜底）。
pub fn init_tracing_from_env(default_level: log::LevelFilter) -> Result<(), AlreadyInit> {
    let filter = env_filter_or(default_level);
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(|_| AlreadyInit)
}

/// RUST_LOG 已设→按其语义；未设→兜底 `default_level`（`init_tracing_from_env` 的
/// 级别构造器，独立成私有函数以便分支隔离测试）。
fn env_filter_or(default_level: log::LevelFilter) -> tracing_subscriber::EnvFilter {
    tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level.to_string()))
}

/// log→tracing 级别显式映射（tracing-core 的 `From<log::LevelFilter>` 挂在未启用
/// 的 `log` feature 下，本构建不可达，故手写六臂）。
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

#[cfg(test)]
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

    // ⑤ from_env：RUST_LOG 未设 → 兜底 default_level；已设 → 依其语义。
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
            tracing_subscriber::EnvFilter::new(LevelFilter::Warn.to_string()).to_string()
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
            Self { var, saved: std::env::var(var).ok() }
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
        ENV_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 零覆盖 #13：RUST_LOG 非法语法 → EnvFilter 构造 Err → 兜底 default_level
    /// （try_from_default_env 的 Err 分支此前仅「未设值」面有测；解析失败同样
    /// 必须兜底，禁 panic 禁静默透传）。
    #[test]
    fn env_filter_falls_back_when_rust_log_is_invalid() {
        let _env = env_lock();
        let _guard = EnvGuard::save("RUST_LOG");
        std::env::set_var("RUST_LOG", "target=not_a_level");
        let fallback = env_filter_or(LevelFilter::Warn);
        assert_eq!(
            fallback.to_string(),
            tracing_subscriber::EnvFilter::new(LevelFilter::Warn.to_string()).to_string()
        );
    }

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
