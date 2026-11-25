//! issue#200：诊断写出失败时护栏仍必须终止进程。
//!
//! 子进程实测（abort 会终止本进程，不能在测试进程内跑）：子进程用**恒失败的
//! 诊断 writer** 起 OS 截止线程并永久阻塞，父进程只断言「子进程异常终止」。
//!
//! 区分三种出口，只接受 abort：
//! - 正常退出（`success()`）⇒ 护栏没生效；
//! - 退出码 101 / Unix 上正常 code ⇒ 看门狗自己 **panic** 了（诊断失败绕过了
//!   abort——正是本票要防的回归）；
//! - 超时仍在跑 ⇒ abort 被绕过、进程继续挂死（退回 job 级超时）。
//!
//! 退出码口径说明：abort 在 Unix 上是 SIGABRT（`code() == None`），在 Windows 上是
//! `0xC0000409`——两者都不是 101，故用「非成功 且 非 101」判定，跨平台同一条断言。

#![cfg(feature = "tokio")]

use std::io;
use std::process::Command;
use std::time::{Duration, Instant};

#[path = "../src/test_support/guard.rs"]
mod guards;

const CHILD_ENV: &str = "UDF_TEST_GUARD_FAILING_DIAGNOSTIC_CHILD";
const CHILD_TEST: &str = "deadline_guard_aborts_when_diagnostic_writer_fails";
const CHILD_DEADLINE: Duration = Duration::from_secs(30);

/// 恒失败的诊断写出面：模拟 stderr 已关闭 / 破管道。
fn failing_writer(_: &str) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::BrokenPipe,
        "注入：stderr 已关闭",
    ))
}

#[test]
fn deadline_guard_aborts_when_diagnostic_writer_fails() {
    if std::env::var_os(CHILD_ENV).is_some() {
        // 子进程：极短截止 + 恒失败 writer ⇒ 宽限后必须 abort（本循环永不到达出口）。
        let _guard = guards::spawn_deadline_with(
            "子进程：诊断写出恒失败",
            Duration::from_millis(10),
            None,
            failing_writer,
        );
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    let exe = std::env::current_exe().expect("current_exe");
    let mut child = Command::new(exe)
        .args(["--exact", CHILD_TEST, "--nocapture"])
        .env(CHILD_ENV, "1")
        .spawn()
        .expect("spawn 子进程测试二进制");
    let deadline = Instant::now() + CHILD_DEADLINE;
    loop {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(
                    !status.success(),
                    "诊断写出失败时必须异常终止（abort），实际 {status:?}"
                );
                assert_ne!(
                    status.code(),
                    Some(101),
                    "不得以 panic 退出：诊断失败不能绕过 abort（issue#200）"
                );
                return;
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("诊断写出失败后护栏未在 {CHILD_DEADLINE:?} 内终止进程：abort 被绕过");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}
