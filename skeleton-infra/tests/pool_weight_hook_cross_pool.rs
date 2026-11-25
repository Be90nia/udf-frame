#![cfg(feature = "pool")]
//! issue#117：weight 钩子的重入护栏按**池身份**判定。
//!
//! #35 之后计量在片锁**外**执行，剩余红线只有「同池重入」（插入路径会再次触发
//! 计量 = 递归面）。旧实现用线程级布尔标志 ⇒ 钩子内访问**任何**池的公开入口都
//! 在 debug 构建 panic（debug/release 语义分叉），而跨池组合本身无害。本文件钉
//! 住两端：跨池合法、同池仍 fail-fast。

use std::sync::Arc;

use skeleton_infra::objpool::{ByteBudget, ObjectPool};

/// 跨池：A 的 weight 钩子在一次计量里读 B 的指标并实借一件（都要取 B 的片锁）。
///
/// 修复前 debug 构建在此 panic（全局标志把「任何池的片锁」都判成重入）。
#[test]
fn weight_hook_may_touch_another_pool() {
    let budget = Arc::new(ByteBudget::new(1 << 20));
    let pool_b: Arc<ObjectPool<Vec<u8>>> = Arc::new(ObjectPool::with_shared_byte_quota(
        2,
        2,
        || vec![0u8; 4],
        Arc::clone(&budget),
        |b| b.capacity(),
    ));
    let probe = Arc::clone(&pool_b);
    let pool_a: ObjectPool<Vec<u8>> = ObjectPool::with_shared_byte_quota(
        2,
        2,
        || vec![0u8; 8],
        Arc::clone(&budget),
        move |b| {
            // 跨池：只读快照 + 实借（构造/归还都在 B 的片锁内）。
            let _idle = probe.metrics().core.idle;
            let held = probe.acquire();
            assert_eq!(held.len(), 4, "拿到的是 B 的对象");
            // held 在此析构 ⇒ B 的归还路径在自己的钩子上下文里跑，逐层还原后
            // 仍回到 A 的身份（嵌套不得把外层的身份弄丢）。
            b.capacity()
        },
    );

    let obj = pool_a.acquire(); // 工厂新建（不计量）
    drop(obj); // 归还路径计量 ⇒ 钩子跑完
    assert_eq!(pool_a.metrics().core.idle, 1, "钩子跑通且对象正常回池");
    assert_eq!(pool_b.metrics().core.idle, 1, "被钩子借走的 B 对象也回到 B");
}

/// 同池：钩子内访问**本池**仍是递归面，必须 fail-fast（修复不得连护栏一起删）。
#[cfg(debug_assertions)]
#[test]
fn weight_hook_same_pool_reentry_still_panics() {
    // `new_cyclic`：钩子需要本池句柄。构造期 `upgrade()` 返回 None 无妨——钩子
    // 在归还时才跑，那时 Arc 早已建成。
    let pool: Arc<ObjectPool<Vec<u8>>> =
        Arc::new_cyclic(|weak: &std::sync::Weak<ObjectPool<Vec<u8>>>| {
            let weak = weak.clone();
            ObjectPool::with_byte_quota(
                2,
                2,
                || vec![0u8; 8],
                1 << 20,
                move |b| {
                    let live = weak.upgrade().expect("钩子运行期池必然存活");
                    let _ = live.metrics(); // 同池取片锁 = 契约违规
                    b.capacity()
                },
            )
        });

    let obj = pool.acquire(); // 工厂新建（不计量）
    let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(obj)))
        .expect_err("同池重入必须以 panic 暴露（禁静默跳过）");
    let message = payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_owned()))
        .unwrap_or_default();
    assert!(
        message.contains("weight hook must not re-enter the same pool"),
        "panic 成因应为同池重入，实得：{message}"
    );
}
