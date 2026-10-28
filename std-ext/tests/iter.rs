//! iter 域 log-embed 埋点集成测试：`process_results` 首 Err 短路 → warn（首
//! Err 位置 pos 入字段，0 起）。next/fold 双通路各一事件点；其余纯组合子面
//! 零埋点（收窄声明：无行为决策面，见 log-embed 埋点 b（本地留存，未入库））。
#![cfg(feature = "log-embed")]

#[path = "common/log_capture.rs"]
mod log_capture;

use log_capture::Capture;
use std_ext::iter::process_results;
use tracing::Level;

// next 通路（processor 逐个拉取/collect）：首 Err 位置入字段。
#[test]
fn short_circuit_via_next_emits_warn_with_position() {
    let data = vec![Ok::<i32, &str>(1), Ok(2), Err("boom"), Ok(4)];
    let capture = Capture::new();
    let got: Result<Vec<i32>, &str> =
        tracing::subscriber::with_default(capture.subscriber(), || {
            process_results(data, |it| it.collect())
        });
    assert_eq!(got, Err("boom"));

    let e = log_capture::only(&capture, "skeleton::iter::process_results");
    assert_eq!(e.level, Level::WARN);
    assert_eq!(e.field("pos"), Some("2"), "首 Err 下标（0 起）");
}

// fold 通路（try_fold 快路径）：同一 warn，pos 语义一致。
#[test]
fn short_circuit_via_fold_emits_warn_with_position() {
    let data = vec![Ok::<i32, &str>(1), Err("boom"), Ok(4)];
    let capture = Capture::new();
    let got: Result<usize, &str> = tracing::subscriber::with_default(capture.subscriber(), || {
        // 计数 fold（非求和）：既走 try_fold 快路径又不踩 unnecessary_fold。
        process_results(data, |it| it.fold(0usize, |acc, _| acc + 1))
    });
    assert_eq!(got, Err("boom"));

    let e = log_capture::only(&capture, "skeleton::iter::process_results");
    assert_eq!(e.level, Level::WARN);
    assert_eq!(e.field("pos"), Some("1"), "首 Err 下标（0 起）");
}

// 全 Ok 流零事件（正常流转零埋点面）。
#[test]
fn all_ok_emits_no_event() {
    let data = vec![Ok::<i32, &str>(1), Ok(2)];
    let capture = Capture::new();
    let got: Result<Vec<i32>, &str> =
        tracing::subscriber::with_default(capture.subscriber(), || {
            process_results(data, |it| it.collect())
        });
    assert_eq!(got, Ok(vec![1, 2]));
    assert!(
        capture.events().is_empty(),
        "全 Ok 零事件: {:?}",
        capture.events()
    );
}
