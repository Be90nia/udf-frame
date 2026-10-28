//! paths 域性能七维采集器（单线程 CPU 纯函数域收窄：①分配声明②单次 ns 锚
//! ③输入规模梯度 16/128/4096；④并发曲线⑤锁⑥内存水位⑦伪共享=n/a——无锁
//! 无线程面，报告：paths 域性能锚（本地留存，未入库））。
//!
//! 沿 multidim 模式纯 std，零新依赖。运行：
//! `cargo run -p udf-skeleton-infra --example paths_multidim --release`

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use std_ext::paths::{ensure_inside, normalize_lexical, relative_to, simplify, CaseStrategy};

/// 中位数采样（奇数次取中位，噪声抑制对齐既有 multidim 先例）。
fn median_ns(iters: u32, mut f: impl FnMut()) -> u128 {
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t0 = Instant::now();
        f();
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn report(name: &str, len_chars: usize, iters: u32, mut f: impl FnMut()) {
    let ns = median_ns(iters, &mut f);
    println!("| {name} | {len_chars} | {ns} |");
}

/// 梯度输入：逼近目标长度的多段合成路径（`C:\work\d0_seg_\d1_seg_...`）。
fn gradient_path(total_chars: usize) -> PathBuf {
    let mut s = String::from(r"C:\work");
    let mut i = 0usize;
    while s.chars().count() < total_chars {
        s.push_str(&format!(r"\d{i}_segment"));
        i += 1;
    }
    PathBuf::from(s)
}

fn main() {
    println!("## paths 域锚（release/std Instant 中位数；10 万次采样）");
    println!("| 操作 | 输入长度(字符) | ns |");
    println!("|---|---|---|");

    // ③ 规模梯度：归一（16/128/4096）
    for total in [16usize, 128, 4096] {
        let p = gradient_path(total);
        let n = p.to_string_lossy().chars().count();
        report("normalize_lexical", n, 100_000, || {
            black_box(normalize_lexical(black_box(p.as_path())));
        });
    }

    // 梯度对照：已归一输入（快路径形态——仍分配，锚定下界）
    for total in [16usize, 128, 4096] {
        let p = normalize_lexical(&gradient_path(total));
        let n = p.to_string_lossy().chars().count();
        report("normalize_lexical(已归一)", n, 100_000, || {
            black_box(normalize_lexical(black_box(p.as_path())));
        });
    }

    // simplify：可剥离 vs 保留面（热路径零分配声明=恒返借用切片）
    let verbatim = PathBuf::from(r"\\?\C:\work\alpha\beta\gamma\delta\epsilon");
    report(
        "simplify(verbatim可剥离)",
        verbatim.to_string_lossy().chars().count(),
        100_000,
        || {
            black_box(simplify(black_box(verbatim.as_path())));
        },
    );
    let reserved = PathBuf::from(r"\\?\C:\COM1\x");
    report(
        "simplify(verbatim保留面)",
        reserved.to_string_lossy().chars().count(),
        100_000,
        || {
            black_box(simplify(black_box(reserved.as_path())));
        },
    );
    let legacy = PathBuf::from(r"C:\work\alpha\beta\gamma\delta\epsilon");
    report(
        "simplify(legacy直通)",
        legacy.to_string_lossy().chars().count(),
        100_000,
        || {
            black_box(simplify(black_box(legacy.as_path())));
        },
    );

    // 相对化梯度（path 侧 16/128/4096，base 固定）
    let base = PathBuf::from(r"C:\work\alpha");
    for total in [16usize, 128, 4096] {
        let p = gradient_path(total);
        let n = p.to_string_lossy().chars().count();
        report("relative_to", n, 100_000, || {
            black_box(relative_to(black_box(p.as_path()), black_box(base.as_path())).ok());
        });
    }

    // containment 两策略（Insensitive 含 lossy 分配，如实计入）
    let inside = PathBuf::from(r"C:\work\alpha\beta\gamma\file.txt");
    report(
        "ensure_inside(Sensitive)",
        inside.to_string_lossy().chars().count(),
        100_000,
        || {
            black_box(
                ensure_inside(
                    black_box(base.as_path()),
                    black_box(inside.as_path()),
                    CaseStrategy::CaseSensitive,
                )
                .is_ok(),
            );
        },
    );
    report(
        "ensure_inside(Insensitive)",
        inside.to_string_lossy().chars().count(),
        100_000,
        || {
            black_box(
                ensure_inside(
                    black_box(base.as_path()),
                    black_box(inside.as_path()),
                    CaseStrategy::CaseInsensitive,
                )
                .is_ok(),
            );
        },
    );

    // home_dir 锚（跨平台；env 读 + PathBuf 移动，分配锚见 tests/paths_alloc.rs）
    use std_ext::paths::home_dir;
    report("home_dir(env臂)", 0, 100_000, || {
        black_box(home_dir());
    });

    // home/XDG 域锚（env 读 + PathBuf 拼接，IO 零面；分配锚见 tests/paths_alloc.rs）。
    // XDG 三函数 Windows 面恒 None 无锚可采（cfg 出）；main 单线程自设自清 env。
    #[cfg(not(windows))]
    {
        use std_ext::paths::{cache_dir, config_dir, data_dir};
        std::env::set_var("XDG_CONFIG_HOME", "/xdg-anchor/cfg/");
        report("config_dir(XDG臂)", 0, 100_000, || {
            black_box(config_dir());
        });
        std::env::remove_var("XDG_CONFIG_HOME");
        report("config_dir(默认臂)", 0, 100_000, || {
            black_box(config_dir());
        });
        report("cache_dir(默认臂)", 0, 100_000, || {
            black_box(cache_dir());
        });
        report("data_dir(默认臂)", 0, 100_000, || {
            black_box(data_dir());
        });
    }

    // ① 分配声明（7 维第 1 维，无仪器化开销的静态面）：
    // simplify 恒返借用切片=零分配；normalize_lexical/relative_to/ensure_inside
    // 语义必然分配（PathBuf 构造；Insensitive 另有 lossy 折叠串）。
    let _ = Path::new("");
}
