//! #176 条 5：每事件分配数与 p50/p99 延迟对账（0/1/8/64 条规则 × 256B/4KiB）。
//!
//! 判据（确定性断言，非性能阈值）：
//! - 0 条规则 ⇒ 每事件**零分配**（掩码表空时整条扇出路径不分配）；
//! - 1 条 vs 64 条规则 ⇒ 每事件分配**次数相同**且每事件至多一次输出分配。
//!   旧实现（每事件 clone 整表 + 建 `Vec<&str>` + 逐条 `String::replace`）在
//!   64 条规则时是「数十次分配/事件」，本断言直接把它判红。
//!
//! 延迟只**打印**不设阈值（性能数值受机器/负载影响，写成红绿判据必致随机红）。
//! 延迟采集含 `Instant::now()` 开销（两次调用/迭代，~20-40ns），故只对
//! µs 量级的配置有意义；这是有意为之的粗测口径，与 `mask_secrets` 文档的
//! 性能锚同一档。
//!
//! 本文件只有一个测试：计数分配器是进程级全局量，同二进制并行测试会污染读数。

#![cfg(all(feature = "log", feature = "memgov"))]

use std::sync::Arc;
use std::time::Instant;

use skeleton_infra::logx::{Channel, ChannelRegistry, ChannelSink, mask_secrets};
use skeleton_infra::memgov::CountingAllocator;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

/// 丢弃写入的 sink：测量只覆盖「扇出 + 掩码」，不含落盘 IO。
struct NullSink(Channel);

impl ChannelSink for NullSink {
    fn channel(&self) -> Channel {
        self.0
    }

    fn level(&self) -> log::LevelFilter {
        log::LevelFilter::Trace
    }

    fn write(&self, _level: log::Level, _target: &str, _message: &str) {}

    fn dropped_lines(&self) -> u64 {
        0
    }
}

/// 每次测量的迭代数（足够摊掉首迭代的缓存冷启动，又不至于跑几十秒）。
const ITERATIONS: usize = 1_000;

fn registry_with(secrets: Vec<String>) -> ChannelRegistry {
    let registry = ChannelRegistry::new();
    registry
        .register(Arc::new(NullSink(Channel::General)))
        .expect("首注恒 Ok");
    registry.set_secrets(secrets);
    registry
}

/// `n` 条同形规则（长度一致，排序结果与输入序无关）。
fn secrets(n: usize) -> Vec<String> {
    (0..n).map(|i| format!("sk-{i:04}-secret")).collect()
}

/// 指定字节长度的消息，首字段内含第 0 条规则（保证真命中、输出变短不触发扩容）。
fn message(len: usize, with_hit: bool) -> String {
    let mut text = String::with_capacity(len);
    if with_hit {
        text.push_str("token=sk-0000-secret ");
    }
    text.extend(std::iter::repeat_n('x', len - text.len()));
    text
}

fn allocs_for(registry: &ChannelRegistry, message: &str) -> usize {
    let before = CountingAllocator::total_allocs();
    for _ in 0..ITERATIONS {
        registry.emit(Channel::General, log::Level::Info, "app::t", message);
    }
    CountingAllocator::total_allocs() - before
}

fn percentile(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

fn latency_ns(f: impl Fn()) -> (u64, u64) {
    let mut samples = Vec::with_capacity(ITERATIONS);
    for _ in 0..ITERATIONS {
        let start = Instant::now();
        f();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    samples.sort_unstable();
    (percentile(&samples, 50), percentile(&samples, 99))
}

#[test]
fn precompiled_mask_cost_does_not_grow_with_secret_count() {
    let small = message(256, true);
    let large = message(4 * 1024, true);

    // ── 分配面（确定性判据）────────────────────────────────────────────
    let none = registry_with(Vec::new());
    assert_eq!(
        allocs_for(&none, &small),
        0,
        "空掩码表：扇出路径每事件零分配"
    );

    let one = registry_with(secrets(1));
    let eight = registry_with(secrets(8));
    let sixty_four = registry_with(secrets(64));
    let counts: Vec<usize> = [&one, &eight, &sixty_four]
        .iter()
        .map(|registry| allocs_for(registry, &small))
        .collect();
    assert_eq!(
        counts[0], counts[2],
        "#176：每事件分配次数不得随规则条数增长（1 条={} 64 条={} / {} 事件）",
        counts[0], counts[2], ITERATIONS
    );
    assert!(
        counts.iter().all(|count| *count <= ITERATIONS),
        "掩码每事件至多一次输出分配（实测 {counts:?} / {ITERATIONS} 事件）"
    );
    let large_counts = allocs_for(&sixty_four, &large);
    assert!(
        large_counts <= ITERATIONS,
        "4KiB 输入同样至多一次输出分配（实测 {large_counts} / {ITERATIONS} 事件）"
    );

    // ── 延迟面（只打印，不做阈值判定）──────────────────────────────────
    println!("== #176 mask cost (iterations={ITERATIONS}) ==");
    println!(
        "allocs/event: 0 secrets={:.3} 1={:.3} 8={:.3} 64={:.3} 64@4KiB={:.3}",
        allocs_for(&none, &small) as f64 / ITERATIONS as f64,
        counts[0] as f64 / ITERATIONS as f64,
        counts[1] as f64 / ITERATIONS as f64,
        counts[2] as f64 / ITERATIONS as f64,
        large_counts as f64 / ITERATIONS as f64,
    );
    for (label, registry, text) in [
        ("0 secrets@256B", &none, &small),
        ("1 secret @256B", &one, &small),
        ("8 secrets@256B", &eight, &small),
        ("64 secrets@256B", &sixty_four, &small),
        ("64 secrets@4KiB", &sixty_four, &large),
    ] {
        let (p50, p99) = latency_ns(|| {
            registry.emit(Channel::General, log::Level::Info, "app::t", text);
        });
        println!("emit  {label}: p50={p50}ns p99={p99}ns");
    }
    // 对照：公共助手（每调用剔空+排序+逐字节扫描）= 旧实现的成本形态。
    let owned_64 = secrets(64);
    let refs_64: Vec<&str> = owned_64.iter().map(String::as_str).collect();
    let (p50, p99) = latency_ns(|| {
        std::hint::black_box(mask_secrets(std::hint::black_box(&small), &refs_64));
    });
    println!("mask_secrets 64 secrets@256B (含每调用排序): p50={p50}ns p99={p99}ns");
}
