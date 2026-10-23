//! config/fsx 件一+件二多维锚采集器（atomic_write + ConfigFile；沿
//! fsx_multidim 模式纯 std，零新依赖，事件计数器自持）。运行：
//! `cargo run -p udf-skeleton-infra --features config-json,config-toml --example config_atomic_multidim --release`
//!
//! 七维收窄声明（单线程 IO + 纯函数域）：
//! - ①分配事件面：atomic_write 每调用自有分配=临时件名 String 1+路径拼接
//!   PathBuf 2（bytes 借用零拷贝）；ConfigFile 逐路径（default 层 to_value/
//!   file 层读+解析/层叠 push/合并折叠+反序列化/save 序列化+落盘）——事件
//!   计数为全进程口径（含 serde_json/std 内部件），如实申报不拆自有限；
//! - ②ns 锚（热缓存中位数）③尺寸/键数梯度 ⑤延迟分布 p50/p99；
//! - ④并发曲线⑥零自旋⑦伪共享=n/a——单线程 IO 域+纯函数合并面，无锁无
//!   自旋无跨线程热点（并发正确性面在 tests：最后赢线性化+无撕裂断言）。

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use skeleton_infra::config::{load_toml, save_toml, ConfigFile};
use std_ext::fsx::{atomic_write, temp_dir_in, TempDir};

static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// SAFETY: 纯转发 System，仅在转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        EVENTS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static COUNTING: EventCounting = EventCounting;

/// op 执行期的分配事件数（瞬时生灭计入；对齐 fsx_multidim/paths_alloc 口径）。
fn allocs<T>(op: impl FnOnce() -> T) -> (usize, T) {
    let before = EVENTS.load(Ordering::Relaxed);
    let out = op();
    black_box(&out);
    (EVENTS.load(Ordering::Relaxed) - before, out)
}

/// 中位数采样（奇数次取中位，对齐既有 multidim 先例）。
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

/// 延迟分布（p50/p99，ns）。
fn dist_ns(iters: usize, mut f: impl FnMut()) -> (u128, u128) {
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        f();
        samples.push(t0.elapsed().as_nanos());
    }
    samples.sort_unstable();
    (samples[iters / 2], samples[iters * 99 / 100])
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct Cfg {
    host: String,
    port: u16,
    retries: u32,
    tags: Vec<String>,
}

impl Cfg {
    fn sample() -> Self {
        Self {
            host: "db.internal".to_string(),
            port: 8080,
            retries: 3,
            tags: ["primary", "pool"].iter().map(|s| s.to_string()).collect(),
        }
    }
}

/// N 键平坦对象（合并梯度载荷）。
fn flat_object(n: usize, offset: u32) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for i in 0..n {
        map.insert(
            format!("k{i:06}"),
            serde_json::Value::Number((i as u64 + offset as u64).into()),
        );
    }
    serde_json::Value::Object(map)
}

fn main() {
    // 落点=target/（CARGO_TARGET_DIR 或缺省 target），域件 RAII 自清（禁系统 TEMP）
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target"));
    let scratch: TempDir =
        temp_dir_in(&base, "config-atomic-multidim-").expect("scratch under target/");

    println!("## config/fsx 件一+件二锚（release/热缓存/Instant 中位数/7 次采样）");
    println!();

    // ── ① atomic_write 分配事件逐路径 ──
    let aw_new = scratch.path().join("aw-new.bin");
    let (events_new, ()) = allocs(|| atomic_write(&aw_new, b"payload").unwrap());
    let (events_over, ()) = allocs(|| atomic_write(&aw_new, b"payload").unwrap());
    println!("### ① atomic_write 分配事件/路径（瞬时生灭计入，全进程口径）");
    println!();
    println!("| 路径 | 事件/op |");
    println!("|---|---|");
    println!("| 建新（create_new+sync_all+rename） | {events_new} |");
    println!("| 覆盖（同上+替换已存在目标） | {events_over} |");
    println!();

    // ── ②③ atomic_write ns 锚（尺寸梯度）──
    println!("### ②③ atomic_write ns/op 尺寸梯度（热缓存，7 次中位）");
    println!();
    println!("| 载荷 | ns/op |");
    println!("|---|---|");
    for size in [64usize, 4 * 1024, 64 * 1024] {
        let file = scratch.path().join(format!("aw-{size}.bin"));
        let payload = vec![0xABu8; size];
        atomic_write(&file, &payload).unwrap(); // 预热+建文件
        let ns = median_ns(7, || atomic_write(&file, &payload).unwrap());
        println!("| {size} B | {ns} |");
        let _ = fs::remove_file(&file);
    }
    println!();

    // ── ⑤ atomic_write 延迟分布 ──
    let file = scratch.path().join("aw-dist.bin");
    let payload = vec![0xCDu8; 4 * 1024];
    atomic_write(&file, &payload).unwrap();
    let (p50, p99) = dist_ns(200, || atomic_write(&file, &payload).unwrap());
    println!("### ⑤ atomic_write 延迟分布（4 KiB × 200 op，ns）");
    println!();
    println!("| p50 | p99 |");
    println!("|---|---|");
    println!("| {p50} | {p99} |");
    println!();

    // ── ① ConfigFile 分层逐路径分配面 ──
    let cfg = Cfg::sample();
    let cfg_file = scratch.path().join("cfg.json");
    let (ev_default, b1) = allocs(|| ConfigFile::<Cfg>::new().default_layer(&cfg).unwrap());
    let (ev_layer_push, b1) = allocs(|| b1.layer(flat_object(0, 0)));
    let (ev_save, ()) = allocs(|| ConfigFile::<Cfg>::save(&cfg, &cfg_file).unwrap());
    let (ev_file_layer, b2) = allocs(|| b1.file_layer(&cfg_file).unwrap());
    let (ev_load, loaded): (usize, Cfg) = allocs(|| b2.load().unwrap());
    assert_eq!(loaded, cfg, "回环保真");
    let (ev_merge_1k, merged): (usize, serde_json::Value) = allocs(|| {
        ConfigFile::new()
            .layer(flat_object(1_000, 0))
            .layer(flat_object(1_000, 1))
            .load()
            .unwrap()
    });
    black_box(&merged);
    println!("### ① ConfigFile 分层逐路径分配事件（全进程口径，含 serde_json/std 内部件）");
    println!();
    println!("| 路径 | 事件 |");
    println!("|---|---|");
    println!("| default_layer（to_value 4 字段 Cfg） | {ev_default} |");
    println!("| layer（空对象 push，Vec 增长均摊） | {ev_layer_push} |");
    println!("| save（Pretty 序列化+atomic_write） | {ev_save} |");
    println!("| file_layer（read+解析 JSON） | {ev_file_layer} |");
    println!("| load（合并折叠+反序列化 4 字段） | {ev_load} |");
    println!("| 1000 键×2 层合并+产出 | {ev_merge_1k} |");
    println!();

    // ── ②③ 合并 ns 梯度 ──
    println!("### ②③ 分层合并 ns（双层 100% 键覆盖梯度，热缓存 7 次中位）");
    println!();
    println!("| 键数/层 | ns/次（合并+load） |");
    println!("|---|---|");
    for n in [10usize, 100, 1_000] {
        let a = flat_object(n, 0);
        let b = flat_object(n, 1);
        let ns = median_ns(7, || {
            let v: serde_json::Value = ConfigFile::new()
                .layer(a.clone())
                .layer(b.clone())
                .load()
                .unwrap();
            black_box(&v);
        });
        println!("| {n} | {ns} |");
    }
    println!();

    // ── TOML 进出锚 ──
    let toml_file = scratch.path().join("cfg.toml");
    let (ev_toml_save, ()) = allocs(|| save_toml(&cfg, &toml_file).unwrap());
    let (ev_toml_load, t) = allocs(|| load_toml::<Cfg>(&toml_file).unwrap());
    assert_eq!(t, cfg, "TOML 回环保真");
    let ns_toml_save = median_ns(7, || save_toml(&cfg, &toml_file).unwrap());
    let ns_toml_load = median_ns(7, || {
        let c: Cfg = load_toml(&toml_file).unwrap();
        black_box(&c);
    });
    println!("### TOML 面（config-toml：类型化直读直写）");
    println!();
    println!("| 面 | ns/op（中位） | 事件/op |");
    println!("|---|---|---|");
    println!("| save_toml（4 字段 Cfg） | {ns_toml_save} | {ev_toml_save} |");
    println!("| load_toml（同上） | {ns_toml_load} | {ev_toml_load} |");

    println!();
    println!("（scratch=TempDir RAII 自清；口径声明见文件头注七维收窄节）");
}
