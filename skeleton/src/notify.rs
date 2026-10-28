//! 异步（信号式）通知端口（ADR 0008）：**信号，不是事件流**。
//!
//! 与同步订阅档（[`crate::Store::subscribe`]，回调在 dispatch 线程同步执行）
//! 并存的第二档：登记面 [`Notify`] 收一个宿主实现的投递口 [`NotifySink`]，
//! 实现方在排空环内每拍只投递一个单调版本号——消费者见版本变化再拉取现态
//! （[`crate::Store::snapshot`]）。载荷状态化：信号非事件流（Android UI 架构
//! 指南的 M1 判据——UI 事件必须规约成状态）；版本号即水位：单调、跳跃可检出
//! （React `useSyncExternalStore` 的 M5 判据）——中间拍允许被合并，消费侧见
//! 版本跳跃即重取现态，绝不撕裂。
//!
//! # 语义要点
//!
//! - **版本号 = 状态代数**：仅在异步订阅在册的落位拍自增（无人订阅期间冻结、
//!   增减订不回退）；在册期间逐拍对应。参考实现从 1 起步。
//! - **O(1) 是宿主义务**：投递发生在实现方的排空环内，慢/阻塞的
//!   [`Self::publish`] 实现会拖慢每一拍 dispatch——框架既不检测也无法阻止
//!   违约。要解耦重活（IO/渲染），投递口内自备通道/工作线程移出重活。
//! - **失败可见**：投递口 panic 由实现方捕获并摘除该口（fail-closed，同订阅
//!   回调 panic 纪律），计数与成因经实现方读侧可见（参考实现
//!   `SyncStore`：`subscriber_panics` / `subscriber_panic_last_cause` /
//!   `notify_published`）。闭门后登记照常、永不发信号（参考实现计入
//!   `subscriptions_after_close`）；store 提前 drop 后投递口随注册表消亡。
//! - **异步档不救同步档**：同步订阅者的耗时照常计入 dispatch；两档并存，
//!   实现方投递序为异步先行、同步随后（同步档内部既有顺序不动）。

use crate::Subscription;

/// 状态变更信号投递口：**宿主实现**。框架只调用，绝不在调用内执行用户重活。
///
/// [`Self::publish`] 的实现契约（宿主义务）：
///
/// - **O(1) 返回、非阻塞**：重活必须经自备通道/工作线程移出本调用；
/// - **允许合并**：同一版本多次投递可折叠为一次处理（版本号让合并可检出）；
/// - **panic 由框架捕获并摘除该投递口**（fail-closed）：摘除后不再收到任何
///   投递，计数与成因经实现方读侧可见。
///
/// # Examples
///
/// 合并式投递口：只记最新版本，消费线程按需拉取（真实宿主在此唤醒工作线程）：
///
/// ```
/// use skeleton::notify::NotifySink;
/// use std::sync::atomic::{AtomicU64, Ordering};
/// use std::sync::Arc;
///
/// #[derive(Clone)]
/// struct Latest(Arc<AtomicU64>);
///
/// impl NotifySink for Latest {
///     fn publish(&self, version: u64) {
///         self.0.store(version, Ordering::Relaxed); // 合并合法：只留最新水位
///     }
/// }
///
/// let sink = Latest(Arc::new(AtomicU64::new(0)));
/// sink.publish(3);
/// assert_eq!(sink.0.load(Ordering::Relaxed), 3);
/// ```
pub trait NotifySink: Send + Sync + 'static {
    /// 投递一次「状态已变」的信号。契约：O(1) 返回；允许合并；
    /// panic 由框架捕获并摘除该投递口（fail-closed）。
    fn publish(&self, version: u64);
}

/// 异步（信号式）通知端口：**实现方**实现。与 [`crate::Store`] 正交——
/// 「六方法即全契约」纪律不因此扩员（不进 `Store`，独立登记面）。
///
/// 版本号语义（实现方必须兑现）：单调水位、无人订阅期间冻结、增减订不回退、
/// 在册期间与落位拍一一对应。消费侧模式：见版本变化 → [`crate::Store::snapshot`]
/// 拉取现态；版本跳跃 = 中间拍被合并，重取即自愈（撕裂判定依据）。
///
/// # Examples
///
/// 裸调用返回值被 `#[must_use]` 拦截（票据 Drop = 注销，死订阅不可静默表达）：
///
/// ```compile_fail
/// # #![deny(unused_must_use)]
/// use skeleton::notify::{Notify, NotifySink};
/// use skeleton::Subscription;
///
/// struct Sink;
/// impl NotifySink for Sink {
///     fn publish(&self, _version: u64) {}
/// }
///
/// struct Toy;
/// impl Notify<u64> for Toy {
///     fn subscribe_notify(&self, _sink: impl NotifySink) -> Subscription<u64> {
///         Subscription::new(|| {})
///     }
/// }
///
/// let toy = Toy;
/// toy.subscribe_notify(Sink); // ERROR: unused `Subscription` that must be used——丢弃即死订阅
/// ```
pub trait Notify<S> {
    /// 登记投递口。票据 Drop = 注销（复用 `Subscription<S>` 语义：
    /// [`Subscription`] 恒 `#[must_use]`，丢弃票据即立即注销）。
    fn subscribe_notify(&self, sink: impl NotifySink) -> Subscription<S>;
}
