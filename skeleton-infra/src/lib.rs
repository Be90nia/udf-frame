//! # skeleton-infra
//!
//! UDF 骨架基础设施层：生态标准件绑定 + 防坑门面。
//!
//! feature 指南（`default = []`，显式取用）：
//!
//! | feature | 域 |
//! |---|---|
//! | （常驻，零 feature） | `text`：UTF-8 解码与字符边界截断 |
//! | （常驻，零 feature） | `backoff`：确定性退避计算 |
//! | `tokio` | `fx`：异步并发托管 |
//! | `log` | `logx`：日志门面与密钥掩码 |
//! | `journal` | `journal`：账本（serde/serde_json 仅此 feature 可达） |
//! | `testing` | `testing`：测试替身 |
//! | `memgov` | `memgov`：内存治理 |
//! | `tracing` | `tracingx`：骨架审计事件流 → tracing 桥 |

pub mod backoff;
pub mod text;

#[cfg(feature = "tokio")]
pub mod fx;

#[cfg(feature = "testing")]
pub mod testing;

#[cfg(feature = "journal")]
pub mod journal;

#[cfg(feature = "log")]
pub mod logx;

#[cfg(feature = "tracing")]
pub mod tracingx;

#[cfg(feature = "memgov")]
pub mod memgov;

pub use backoff::backoff_delay;
pub use text::{decode_bytes, truncate_chars, DecodeError, DecodeStrategy};

#[cfg(feature = "tokio")]
pub use fx::{
    cancel_pair, expect_idle, CancelSource, CancelToken, FxScope, ScopeClosed, Spawned, TaskError,
};

#[cfg(feature = "testing")]
pub use testing::{Script, ScriptedRuntime};

#[cfg(feature = "journal")]
pub use journal::{JsonlJournal, Journal, JournalError, Replay, ReplayError};

#[cfg(feature = "log")]
pub use logx::{init_tracing_default, init_tracing_from_env, mask_secrets, AlreadyInit};

#[cfg(feature = "tracing")]
pub use tracingx::bridge_events_to_tracing;

#[cfg(feature = "memgov")]
pub use memgov::{
    expect_empty, BoundedRegistry, BoundError, CountingAllocator, Eviction, LeakGuard, LeakReport,
};
