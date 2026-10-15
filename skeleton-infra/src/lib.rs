//! # skeleton-infra
//!
//! UDF 骨架基础设施层：契约实现（`skeleton::Store` 的参考实现）+ 生态标准件
//! 绑定 + 防坑门面。契约（trait 与数据词汇）在 [`skeleton`]，本 crate 兑现。
//!
//! feature 指南（`default = ["runtime"]`；`--no-default-features` 得纯生态件面）：
//!
//! | feature | 域 |
//! |---|---|
//! | `runtime`（默认） | `runtime::SyncStore`：契约参考实现（std-only）+ `audit` 审计事件值面 |
//! | （常驻，零 feature） | `text`：UTF-8 解码与字符边界截断 |
//! | （常驻，零 feature） | `backoff`：确定性退避计算 |
//! | `tokio` | `fx`：异步并发托管 |
//! | `log` | `logx`：日志门面与密钥掩码 |
//! | `journal` | `journal`：账本（serde/serde_json 仅此 feature 可达） |
//! | `testing` | `testing`：测试替身（TestStore + 脚本化解释器） |
//! | `memgov` | `memgov`：内存治理 |
//! | `compose` | `compose`：父子分片组合器 |
//! | `tracing` | `tracingx`：骨架审计事件流 → tracing 桥 |

pub mod backoff;
pub mod text;

#[cfg(feature = "runtime")]
pub mod audit;

#[cfg(feature = "runtime")]
pub mod runtime;

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

#[cfg(feature = "compose")]
pub mod compose;

pub use backoff::backoff_delay;
pub use text::{decode_bytes, truncate_chars, DecodeError, DecodeStrategy};

#[cfg(feature = "runtime")]
pub use audit::{AuditEvent, AuditEventKind, AuditTicket};

#[cfg(feature = "runtime")]
pub use runtime::{LocalSubscription, SyncStore};

#[cfg(feature = "tokio")]
pub use fx::{
    cancel_pair, expect_idle, CancelSource, CancelToken, FxScope, ScopeClosed, Spawned, TaskError,
};

#[cfg(feature = "testing")]
pub use testing::{ExpectedEffect, Script, ScriptedRuntime, TestStore};

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

#[cfg(feature = "compose")]
pub use compose::{combine, Combine};
