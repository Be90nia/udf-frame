//! journal 域多维实测数据采集器（批 A-1：热路径补锚——现仅 1 个 criterion 锚 ~0.23µs）。
//!
//! 跑法（仓库根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example journal_multidim --features journal,memgov
//! ```
//!
//! 采集面（派单契约五项）：
//! ①append 吞吐+延迟分布（p50/p99/p99.9——含 BufWriter 满 64KiB write 尖刺形态）
//! ②replay 吞吐（全量回放 + 快照起播增量，行/s 与 MB/s 双口径）
//! ③稳态分配留痕（append 热路径 CountingAllocator delta；flush 路径差值法如实单列）
//! ④挂载/损坏行检测代价（open 尾部校验 / VersionMismatch 拒载 / restore 快照扫描 /
//!   首坏行 fail-closed 检测）
//! ⑤并发 append 1/2/4 线程。**单写者边界**：`JsonlJournal` 无跨线程锁，同句柄并发=
//!   编译期不可能（`append(&mut self)` 独占借用）；本域并发面=多账本（每线程独立
//!   文件）聚合吞吐，测 OS 文件层并发写放大，无账本内并发断言面。
//!
//! 口径注（沿 multidim 惯例）：
//! ① CountingAllocator 常驻本二进制（内存维前提），计时含每分配 2 次 Relaxed 原子加
//!    噪声（量级 ns，相对 0.2µs 级 append 不可忽略，判级用）；
//! ② Instant 时钟对开销（~2×20ns）同臂同担；粗测 std 分位数非 criterion——数字供判级，
//!    不供微优化基线；
//! ③ 内存段全程串行（计数器仅串行上下文有效）；并发段只断言行数不变量不计内存；
//! ④ 临时账本落 `target-iso/journal-multidim/`（对齐 tests/journal_port.rs case_dir
//!    惯例），结束即清（异常退出残留由下次运行开头清理收口）；black_box 防 LLVM
//!    消除被测 op。
//!
//! **FAST 档**（`JOURNAL_MULTIDIM_FAST=1`，其余值=全量档）：两重臂采样/行数各取
//! 1/10——持久点臂 fsync 采样 5000→500、并发臂 400k→40k 行。口径：输出格式与判级
//! 规则不变（分位分布在 FAST 档样本量上仍稳定），仅采样密度下降；**正式锚数字以
//! 全量档（默认）为准**，FAST 档供日常冒烟/CI 快扫（全量档 ~10 min → FAST 档
//! ~1 min 量级，fsync 臂占比最高）。

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use skeleton_infra::journal::{Journal, JsonlJournal};
use skeleton_infra::memgov::CountingAllocator;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

#[derive(Serialize, Deserialize)]
struct Act {
    id: u64,
}

#[derive(Serialize, Deserialize)]
struct St {
    n: u64,
}

/// dyn 焊接面（批 6A 关联类型化后）：`State =` 把状态面焊进对象类型，
/// dyn 上方法调用零标注（具体类型路径的直呼惯用形见 journal.rs doctest）。
type J = dyn Journal<Act, State = St>;

const N: u64 = 100_000;
const WARM: u64 = 1_000;

/// FAST 档门控：`JOURNAL_MULTIDIM_FAST=1` 精确匹配开启（其余值=全量档，口径见头注）。
fn fast_mode() -> bool {
    std::env::var_os("JOURNAL_MULTIDIM_FAST")
        .map(|v| v == "1")
        .unwrap_or(false)
}

fn case_root() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("target-iso/journal-multidim");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    sorted[(sorted.len() * p / 100).min(sorted.len() - 1)]
}

/// p99.9（permille 取位）——BufWriter 满缓冲 write 尖刺落在这一带。
fn p999(sorted: &[u64]) -> u64 {
    sorted[(sorted.len() * 999 / 1000).min(sorted.len() - 1)]
}

/// 单发采样分布：样本排序后存回 v。
fn collect_samples(mut op: impl FnMut() -> u64, n: usize, v: &mut Vec<u64>) {
    v.clear();
    for _ in 0..n {
        v.push(op());
    }
    v.sort_unstable();
}

fn median3(runs: &mut [u64]) -> u64 {
    runs.sort_unstable();
    runs[runs.len() / 2]
}

fn format_rate(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.1}M", v / 1e6)
    } else {
        format!("{:.0}K", v / 1e3)
    }
}

fn main() {
    println!("== journal 多维矩阵（release, 纯 std）==\n");
    let root = case_root();
    dim1_append(&root);
    dim2_replay(&root);
    dim3_allocs(&root);
    dim4_mount_and_corruption(&root);
    dim5_concurrent_stores(&root);
    let _ = std::fs::remove_dir_all(&root);
    println!("\n== 采集完毕 ==");
}

/// ①append 吞吐+延迟分布。缓冲态批量主口径（median3，行数与字节数双口径）；
/// 单发分布辅证（n=100k ≈ 跨 ~30 个 64KiB 缓冲边界，p99.9 即满缓冲 write 系统调用
/// 尖刺带）；持久点臂（逐 append flush）=最坏持久化路径对照。flush 自 2026-10-17
/// 起含 `sync_all`（掉电真持久），本臂数字为 write+fsync 双系统调用口径，
/// 较缓冲态呈 µs→ms 级跳变属预期。
fn dim1_append(root: &Path) {
    // 批量吞吐 median3（每轮独立账本，flush 后取 metadata=诚实字节数）。
    let mut runs: Vec<(u64, u64)> = Vec::with_capacity(3);
    for r in 0..3 {
        let p = root.join(format!("d1-run{r}.jsonl"));
        let mut j = JsonlJournal::<Act, St>::create(&p).unwrap();
        for i in 0..WARM {
            J::append_action(&mut j, &Act { id: i }).unwrap();
        }
        let t0 = Instant::now();
        for i in 0..N {
            black_box(J::append_action(&mut j, &Act { id: i }).is_ok());
        }
        let ns = t0.elapsed().as_nanos() as u64;
        j.flush().unwrap();
        let bytes = std::fs::metadata(&p).unwrap().len();
        runs.push((ns, bytes));
    }
    runs.sort_unstable();
    let (ns, bytes) = runs[1];
    let secs = ns as f64 / 1e9;
    println!(
        "①append吞吐(缓冲态): {} ns/行 | {} 行/s | {:.1} MB/s（median3, n={N}, 行均~{}B）",
        ns / N,
        format_rate(N as f64 / secs),
        bytes as f64 / secs / 1e6,
        bytes / N
    );

    // 单发分布（独立账本，连续写跨多个满缓冲边界）。
    let p = root.join("d1-dist.jsonl");
    let mut j = JsonlJournal::<Act, St>::create(&p).unwrap();
    for i in 0..WARM {
        J::append_action(&mut j, &Act { id: i }).unwrap();
    }
    let n = 100_000usize;
    let mut samples: Vec<u64> = Vec::with_capacity(n);
    collect_samples(
        || {
            let a = Act { id: 1 };
            let t0 = Instant::now();
            black_box(J::append_action(&mut j, &a).is_ok());
            t0.elapsed().as_nanos() as u64
        },
        n,
        &mut samples,
    );
    // 闭包借用结束后收尾 flush（读侧可见性纪律）。
    j.flush().unwrap();
    println!(
        "①append延迟(缓冲态): p50={}ns p99={}ns p99.9={}ns max={}ns（n={n}；p99.9/max 即满 64KiB write 尖刺带）",
        pct(&samples, 50),
        pct(&samples, 99),
        p999(&samples),
        samples[samples.len() - 1],
    );

    // 持久点臂：append+flush 全程单发采样（flush 点=持久点，最坏路径）。
    let p = root.join("d1-durability.jsonl");
    let mut j = JsonlJournal::<Act, St>::create(&p).unwrap();
    let n = if fast_mode() { 500 } else { 5_000 };
    let mut samples: Vec<u64> = Vec::with_capacity(n);
    collect_samples(
        || {
            let a = Act { id: 1 };
            let t0 = Instant::now();
            J::append_action(&mut j, &a).unwrap();
            j.flush().unwrap();
            t0.elapsed().as_nanos() as u64
        },
        n,
        &mut samples,
    );
    println!(
        "①append延迟(逐append flush=持久点): p50={}ns p99={}ns max={}ns（n={n}；每行 write+sync_all 双系统调用，fsync 后 µs→ms 跳变属预期）",
        pct(&samples, 50),
        pct(&samples, 99),
        samples[samples.len() - 1],
    );
}

/// ②replay 吞吐：全量回放（无快照）与快照起播增量回放，3 连跑中位。
fn dim2_replay(root: &Path) {
    // 全量账本：N 行。
    let full = root.join("d2-full.jsonl");
    {
        let mut j = JsonlJournal::<Act, St>::create(&full).unwrap();
        for i in 0..N {
            J::append_action(&mut j, &Act { id: i }).unwrap();
        }
        j.flush().unwrap();
    }
    let bytes = std::fs::metadata(&full).unwrap().len();
    let mut runs: Vec<u64> = Vec::with_capacity(3);
    {
        let j = JsonlJournal::<Act, St>::open(&full, 1).unwrap();
        for _ in 0..3 {
            let t0 = Instant::now();
            let mut cnt = 0u64;
            for a in J::replay(&j).unwrap() {
                black_box(a.unwrap().id);
                cnt += 1;
            }
            assert_eq!(cnt, N, "全量回放行数不变量");
            runs.push(t0.elapsed().as_nanos() as u64);
        }
    }
    let med = median3(&mut runs);
    let secs = med as f64 / 1e9;
    println!(
        "②replay吞吐(全量): {} 行/s | {:.1} MB/s | {:.3} µs/行（median3, n={N}）",
        format_rate(N as f64 / secs),
        bytes as f64 / secs / 1e6,
        med as f64 / N as f64 / 1e3,
    );

    // 快照起播增量账本：中点 persist_snapshot，前半 skip 后半增量。
    // 分段口径（同④restore 臂）：单轮打印扫描/增量两段——吞吐主口径在全量臂 median3。
    let snap = root.join("d2-snap.jsonl");
    {
        let mut j = JsonlJournal::<Act, St>::create(&snap).unwrap();
        for i in 0..N / 2 {
            J::append_action(&mut j, &Act { id: i }).unwrap();
        }
        J::append_snapshot(&mut j, &St { n: N / 2 }).unwrap();
        for i in N / 2..N {
            J::append_action(&mut j, &Act { id: i }).unwrap();
        }
        j.flush().unwrap();
    }
    {
        let j = JsonlJournal::<Act, St>::open(&snap, 1).unwrap();
        let t0 = Instant::now();
        let (st, replay) = J::restore_stream(&j).unwrap().unwrap();
        let scan = t0.elapsed();
        let t1 = Instant::now();
        let mut cnt = 0u64;
        for a in replay {
            black_box(a.unwrap().id);
            cnt += 1;
        }
        assert_eq!(cnt, N / 2, "增量回放行数不变量");
        assert_eq!(st.n, N / 2, "快照态");
        println!(
            "②restore快照恢复: 扫描={}µs + 增量回放 {} 行={}µs（中点快照, 单轮分段）",
            scan.as_micros(),
            cnt,
            t1.elapsed().as_micros(),
        );
    }
}

/// ③稳态分配留痕（CountingAllocator delta，串行上下文）：append 热路径如实报数——
/// 预期**非零**（`tagged_line` 每行 serde_json `to_string` + `format!` 拼接 + 保留键
/// `Value` 探测均为堆分配，是 fail-visible 保留键检查的设计代价）；flush 路径差值法
/// 单列（append+flush 批 minus append-only 批）。
fn dim3_allocs(root: &Path) {
    let p = root.join("d3.jsonl");
    let mut j = JsonlJournal::<Act, St>::create(&p).unwrap();
    for i in 0..WARM {
        J::append_action(&mut j, &Act { id: i }).unwrap();
    }
    let n = 10_000u64;
    // 臂 1：append-only 稳态。
    let before_a = CountingAllocator::live_allocs();
    let before_ab = CountingAllocator::live_bytes();
    let churn_before = CountingAllocator::total_allocs();
    for i in 0..n {
        black_box(J::append_action(&mut j, &Act { id: i }).is_ok());
    }
    let append_allocs = CountingAllocator::live_allocs() - before_a;
    let append_bytes = CountingAllocator::live_bytes() as i64 - before_ab as i64;
    let append_churn = CountingAllocator::total_allocs() - churn_before;
    // 臂 2：append+flush（差值=flush 净分配）。
    let before_f = CountingAllocator::live_allocs();
    for i in 0..n {
        J::append_action(&mut j, &Act { id: i }).unwrap();
        j.flush().unwrap();
    }
    let flush_allocs = (CountingAllocator::live_allocs() - before_f) as f64 - append_allocs as f64;
    j.flush().unwrap();
    let per = append_allocs as f64 / n as f64;
    println!(
        "③append稳态净分配(live-delta): {:.2} allocs/append（{} allocs / {n}, Δlive_bytes={}）",
        per, append_allocs, append_bytes,
    );
    println!(
        "③append稳态churn(total-delta): {:.2} allocs/append（{append_churn} total / {n}；net={per:.2}——live×total 联用判级，net 差即临时分配辨真）",
        append_churn as f64 / n as f64,
    );
    // 口径（benches/infra.rs effect_steady_state 同款）：live_allocs 为净存活计数、
    // total_allocs 为累计计数——上两行联用判级：net 与 churn 同值=全为存活分配；
    // churn>net=临时分配（即用即还）被 live 净额吞掉的部分现形。tagged_line 静态面
    // 含 to_string + format! 拼接 + 保留键 Value 探测三类分配点，churn 行即其如实
    // 报数面。
    println!(
        "③flush路径净分配(差值法): {:.2} allocs/flush（append+flush 批 minus append-only 批；write 系统调用预期零净增）",
        flush_allocs / n as f64,
    );
}

/// ④挂载/损坏行检测代价。挂载=open（版本头校验+尾部完整性反向分块扫描，只查字节级
/// 残行不查 JSON 合法性）；损坏行分层在解析面（replay 逐行懒解析 fail-closed）。
/// restore 臂复用 ②的中点快照账本（d2-snap.jsonl）。
fn dim4_mount_and_corruption(root: &Path) {
    // open 臂：100k 行账本（~1.9MB）3 连跑中位。
    let full = root.join("d2-full.jsonl");
    let mut runs: Vec<u64> = Vec::with_capacity(3);
    for _ in 0..3 {
        let t0 = Instant::now();
        black_box(JsonlJournal::<Act, St>::open(&full, 1).is_ok());
        runs.push(t0.elapsed().as_nanos() as u64);
    }
    println!(
        "④挂载open(100k行): {} µs（median3；版本头校验+尾部完整性反向分块）",
        median3(&mut runs) as f64 / 1e3,
    );

    // VersionMismatch 拒载：只读头行即 Err，1000 次 p50。
    let bad_v = root.join("d4-v2.jsonl");
    std::fs::write(&bad_v, b"{\"v\":2}\n").unwrap();
    let n = 1_000usize;
    let mut samples: Vec<u64> = Vec::with_capacity(n);
    collect_samples(
        || {
            let t0 = Instant::now();
            black_box(JsonlJournal::<Act, St>::open(&bad_v, 1).is_err());
            t0.elapsed().as_nanos() as u64
        },
        n,
        &mut samples,
    );
    println!(
        "④VersionMismatch拒载: p50={}ns（n={n}；只读头行 fail-closed）",
        pct(&samples, 50),
    );

    // restore 快照扫描：单遍扫全文件找最后 "s" 行（不含增量迭代——那在②已分段）。
    let snap = root.join("d2-snap.jsonl");
    let j = JsonlJournal::<Act, St>::open(&snap, 1).unwrap();
    let t0 = Instant::now();
    black_box(J::restore_stream(&j).unwrap().unwrap());
    println!(
        "④restore快照扫描(100k行含1快照行): {} µs（单遍扫描+最后 \"s\" 行定位）",
        t0.elapsed().as_micros(),
    );

    // 首坏行 fail-closed 检测：99,999 合法行 + 1 畸形行（尾部完整行边界，open 应成功
    // ——挂载面只查字节级完整性；畸形行由 replay 解析面 fail-closed）。
    let corrupt = root.join("d4-corrupt.jsonl");
    let mut body = String::with_capacity(2_000_000);
    body.push_str("{\"v\":1}\n");
    for _ in 0..(N - 1) {
        body.push_str("{\"t\":\"a\",\"id\":7}\n");
    }
    body.push_str("{\"t\":\"a\",\"id\":}\n");
    std::fs::write(&corrupt, &body).unwrap();
    let j = JsonlJournal::<Act, St>::open(&corrupt, 1).unwrap();
    let t0 = Instant::now();
    let mut good = 0u64;
    for a in J::replay(&j).unwrap() {
        if a.is_err() {
            break;
        }
        good += 1;
    }
    let detect = t0.elapsed();
    assert_eq!(good, N - 1, "畸形行前全部合法行可解析");
    println!(
        "④首坏行检测(fail-closed): {} ms 扫过 {} 合法行后命中畸形行（≈全量 replay 成本，逐行懒解析面）",
        detect.as_millis(),
        good,
    );
}

/// ⑤并发 append 曲线（单写者边界内的并发面）：T 线程 × 各自独立账本（独立文件），
/// 聚合吞吐 + 行数不变量断言。窗口 400k 行（~0.2s/臂）压调度抖动——100k 窗口实测
/// 2 线程臂被 50ms 级 OS 抖动打出一9% 假倒退。verdict 沿 dim3 惯例（≥95%）。
fn dim5_concurrent_stores(root: &Path) {
    let n5: u64 = if fast_mode() { 40_000 } else { 400_000 };
    println!("⑤并发append(多账本聚合, 总行数恒={n5}):");
    let mut prev = 0.0;
    for t in [1usize, 2, 4] {
        let per = n5 / t as u64;
        let t0 = Instant::now();
        std::thread::scope(|s| {
            for k in 0..t {
                let p = root.join(format!("d5-t{t}-{k}.jsonl"));
                s.spawn(move || {
                    let mut j = JsonlJournal::<Act, St>::create(p).unwrap();
                    for i in 0..per {
                        black_box(J::append_action(&mut j, &Act { id: i }).is_ok());
                    }
                    // 停机序纪律：显式 flush 再 drop（drop 内 flush 吞错误）。
                    j.flush().unwrap();
                });
            }
        });
        let secs = t0.elapsed().as_secs_f64();
        let rate = n5 as f64 / secs;
        let verdict = if t == 1 {
            "基线"
        } else if rate >= prev * 0.95 {
            "饱和不倒退"
        } else {
            "⚠倒退>5%"
        };
        // 行数不变量：各账本 replay 计数 == per。
        for k in 0..t {
            let p = root.join(format!("d5-t{t}-{k}.jsonl"));
            let j = JsonlJournal::<Act, St>::open(&p, 1).unwrap();
            assert_eq!(
                J::replay(&j).unwrap().count() as u64,
                per,
                "t{t} 账本{k} 行数"
            );
        }
        println!(
            "   threads={t}: {} 行/s（{:.3}s, 每线程 {} 行）[{verdict}]",
            format_rate(rate),
            secs,
            per,
        );
        prev = rate;
    }
}
