//! 测试面共享件（`cfg(all(test, feature = "tokio"))` 编译）。集成测试不经过本模块
//! ——它们用 `#[path = "../src/test_support/guard.rs"] mod guards;` 直接引入同一份
//! 实现（单源、无第二份副本）。

pub(crate) mod guard;
