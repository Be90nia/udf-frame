//! 排空权独占载体的对照实现（`unsafe-evidence` 构建，蓝图 §9 证据面）。
//!
//! 生产载体 [`super::DrainExclusive`] 用 `UnsafeCell` + 排空旗外部同步换掉互斥
//! 锁；本模块是同一契约的 `Mutex<T>` 版参考实现——独占区间由锁自证，供差分
//! 测试在同一进程内对照两实现的可观察行为（[`super::SyncStore::new_mutex_reference`]）。
//! 默认构建不编译本模块：生产装配恒选 [`super::ExclusiveCell::unsynchronized`]。

use std::sync::Mutex;

/// `Mutex<T>` 版独占载体：与 [`super::DrainExclusive`] 同一 `with_mut` 契约，
/// 但区间互斥由锁建立而非外部排空旗。
pub(super) struct MutexExclusive<T> {
    value: Mutex<T>,
}

impl<T> MutexExclusive<T> {
    pub(super) fn new(value: T) -> Self {
        Self {
            value: Mutex::new(value),
        }
    }

    /// 锁覆盖整个独占区间：旁路排空旗的第二进入者会阻塞（旗失效即挂死显形），
    /// 而不是别名写。毒锁按内容继续——panic 路径与生产实现同样不丢缓冲容量、
    /// 不报废实例（见 `DrainExclusive` 的 panic 语义）。
    pub(super) fn with_mut<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        let mut guard = match self.value.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        f(&mut guard)
    }
}
