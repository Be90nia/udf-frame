//! fields 域七维性能矩阵采集器：①分配事件/次（new/clone/iter/覆盖=0；首插/追加/
//! merge 如实）②ns 锚（new/clone/insert/get/iter(4 字段)/merge(4 字段)）③规模梯度
//! 4/32/256 字段 insert+iter（判线性）④clone 成本与字段数无关（O(1) 证据：4 vs
//! 256）⑤COW 改写成本（共享后在飞 clone 首改付一次性 O(n)，单批如实报）⑥跨线程
//! 共享（clone 交后台线程读，O(1) 交接 + 读回校验）⑦收窄声明（无 serde/无导出/
//! O(n) get）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton --example fields_multidim
//! ```
//!
//! 口径注（沿 lint_multidim 惯例）：
//! ① 分配读数=本线程窗内事件计数（`IN_WINDOW` 门），本采集器测量段单线程（维⑥
//!    的后台线程在窗外，其分配不进本线程计数）；
//! ② 粗测（std `Instant`）非 criterion——**数字供判级，不供微优化基线**；
//! ③ `black_box` 防 LLVM 消除；每个锚三连跑取中位；
//! ④ 字段名取自编译期字符串池 [`NAME_POOL`]（按空格切开的 `&'static str`，运行期
//!    零分配零泄漏，与模块 armor「字段名是编译期常量」同纪律）；名字Vec/袋的构造
//!    一律在测量窗外完成，`format!` 之类不进任何计数窗。

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Instant;

use skeleton::fields::{FieldBag, FieldValue};

// ---- 分配事件计数（同 tests/lint_alloc.rs 口径）----

/// 分配事件累计（窗内 + 本线程，见 [`IN_WINDOW`]）。
static EVENTS: AtomicUsize = AtomicUsize::new(0);

struct EventCounting;

// 计数窗门：仅窗内本线程的分配计入事件。const 初始化 TLS 无惰性分配、无析构注册。
thread_local! {
    static IN_WINDOW: Cell<bool> = const { Cell::new(false) };
}

// SAFETY: 纯转发 System，仅在窗内转发前累加事件计数；null 原样透传（alloc 契约）。
unsafe impl GlobalAlloc for EventCounting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // try_with：线程 TLS 析构期访问会 panic（分配器内 panic=abort），取不到当不在窗内。
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if IN_WINDOW.try_with(Cell::get).unwrap_or(false) {
            EVENTS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOC: EventCounting = EventCounting;

/// 窗内事件 / op 次数（预热一拍不进窗：EMPTY 懒建等一次性初始化不计）。
fn alloc_per<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    black_box(op());
    IN_WINDOW.with(|w| w.set(true));
    let before = EVENTS.load(Ordering::Relaxed);
    for _ in 0..n {
        black_box(op());
    }
    let events = EVENTS.load(Ordering::Relaxed) - before;
    IN_WINDOW.with(|w| w.set(false));
    events as f64 / n as f64
}

// ---- 测量助手（lint_multidim 同款）----

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut samples: Vec<f64> = (0..3).map(|_| f()).collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("ns 读数无 NaN"));
    samples[1]
}

fn batch_ns<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

// ---- 采集夹具 ----

/// 字段名池：编译期字面量（f0..f383 共 384 个），按空格切开即得 `&'static str`。
/// 量入为出：梯度最大袋 256 个 + 追加锚每批 128 个（256+128=384）。
const NAME_POOL: &str = "f0 f1 f2 f3 f4 f5 f6 f7 f8 f9 f10 f11 f12 f13 f14 f15 f16 f17 f18 f19 f20 f21 f22 f23 \
f24 f25 f26 f27 f28 f29 f30 f31 f32 f33 f34 f35 f36 f37 f38 f39 f40 f41 f42 f43 f44 f45 f46 f47 \
f48 f49 f50 f51 f52 f53 f54 f55 f56 f57 f58 f59 f60 f61 f62 f63 f64 f65 f66 f67 f68 f69 f70 f71 \
f72 f73 f74 f75 f76 f77 f78 f79 f80 f81 f82 f83 f84 f85 f86 f87 f88 f89 f90 f91 f92 f93 f94 f95 \
f96 f97 f98 f99 f100 f101 f102 f103 f104 f105 f106 f107 f108 f109 f110 f111 f112 f113 f114 f115 f116 f117 f118 f119 \
f120 f121 f122 f123 f124 f125 f126 f127 f128 f129 f130 f131 f132 f133 f134 f135 f136 f137 f138 f139 f140 f141 f142 f143 \
f144 f145 f146 f147 f148 f149 f150 f151 f152 f153 f154 f155 f156 f157 f158 f159 f160 f161 f162 f163 f164 f165 f166 f167 \
f168 f169 f170 f171 f172 f173 f174 f175 f176 f177 f178 f179 f180 f181 f182 f183 f184 f185 f186 f187 f188 f189 f190 f191 \
f192 f193 f194 f195 f196 f197 f198 f199 f200 f201 f202 f203 f204 f205 f206 f207 f208 f209 f210 f211 f212 f213 f214 f215 \
f216 f217 f218 f219 f220 f221 f222 f223 f224 f225 f226 f227 f228 f229 f230 f231 f232 f233 f234 f235 f236 f237 f238 f239 \
f240 f241 f242 f243 f244 f245 f246 f247 f248 f249 f250 f251 f252 f253 f254 f255 f256 f257 f258 f259 f260 f261 f262 f263 \
f264 f265 f266 f267 f268 f269 f270 f271 f272 f273 f274 f275 f276 f277 f278 f279 f280 f281 f282 f283 f284 f285 f286 f287 \
f288 f289 f290 f291 f292 f293 f294 f295 f296 f297 f298 f299 f300 f301 f302 f303 f304 f305 f306 f307 f308 f309 f310 f311 \
f312 f313 f314 f315 f316 f317 f318 f319 f320 f321 f322 f323 f324 f325 f326 f327 f328 f329 f330 f331 f332 f333 f334 f335 \
f336 f337 f338 f339 f340 f341 f342 f343 f344 f345 f346 f347 f348 f349 f350 f351 f352 f353 f354 f355 f356 f357 f358 f359 \
f360 f361 f362 f363 f364 f365 f366 f367 f368 f369 f370 f371 f372 f373 f374 f375 f376 f377 f378 f379 f380 f381 f382 f383";

/// 池里应有的名字个数（main 里自检：数量 + 唯一性，防手写笔误静默去重失真）。
const POOL_LEN: usize = 384;

/// 切出名字表（各维一次性调用，在测量窗外；元素仍是 `'static`）。
fn pool_names() -> Vec<&'static str> {
    NAME_POOL.split(' ').collect()
}

/// 预建 n 字段 bag（窗外构造，作测量基准）。
fn filled(n: usize, names: &[&'static str]) -> FieldBag {
    let mut bag = FieldBag::new();
    for (i, fname) in names.iter().take(n).enumerate() {
        bag.insert(fname, FieldValue::Count(i as u64));
    }
    bag
}

/// 追加锚每批新键个数（小基数：O(n) 扫描随基数平方涨，大批会跑到分钟级）。
const APPEND_BATCH: usize = 128;

fn main() {
    // 仪器自检：名字池数量与唯一性（重复名会让 bag 静默去重、规模失真）。
    let names = pool_names();
    assert_eq!(names.len(), POOL_LEN, "字段名池数量不符（手写笔误）");
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), POOL_LEN, "字段名池有重复名（手写笔误）");

    println!("== fields 域七维采集（release / std Instant 粗测；本地数字，环境相关）==");
    dim1_alloc(&names);
    dim2_ns(&names);
    dim3_scale(&names);
    dim4_clone_flat(&names);
    dim5_cow_rewrite(&names);
    dim6_cross_thread(&names);
    dim7_narrowing(&names);
}

// ---- ① 分配事件/次 ----

const N: u64 = 100_000;

fn dim1_alloc(names: &[&'static str]) {
    println!("\n[dim1] 分配事件/次（预热后 100k 拍，本线程窗内）");
    println!("  {:<34} | {:>12}", "op", "events/call");
    let bag4 = filled(4, names);
    let mut overwrite = filled(4, names);

    let new_ev = alloc_per(FieldBag::new, N);
    let clone_ev = alloc_per(|| bag4.clone(), N);
    let iter_ev = alloc_per(
        || {
            for (name, value) in bag4.iter() {
                black_box((name, value));
            }
        },
        N,
    );
    let overwrite_ev = alloc_per(|| overwrite.insert("f1", FieldValue::Count(7)), N);
    let first_ev = alloc_per(
        || {
            let mut bag = FieldBag::new();
            bag.insert("k", FieldValue::Count(1));
            bag
        },
        N,
    );
    // 追加：同一 bag 连续新键（Vec 倍增摊销；名字在池里，窗外备好）。
    let mut append_bag = filled(4, names);
    let mut idx = 4usize;
    let append_ev = alloc_per(
        || {
            append_bag.insert(names[idx], FieldValue::Count(idx as u64));
            idx += 1;
        },
        APPEND_BATCH as u64,
    );
    // merge：新空袋接 4 字段（Count 值，无 String 分配，读数纯结构成本）。
    let merge_src = filled(4, names);
    let merge_ev = alloc_per(
        || {
            let mut target = FieldBag::new();
            target.merge(&merge_src);
            target
        },
        N,
    );

    println!("  {:<34} | {:>12.2}", "new", new_ev);
    println!("  {:<34} | {:>12.2}", "clone (4 fields)", clone_ev);
    println!("  {:<34} | {:>12.2}", "iter (4 fields)", iter_ev);
    println!(
        "  {:<34} | {:>12.2}",
        "insert overwrite (steady)", overwrite_ev
    );
    println!(
        "  {:<34} | {:>12.2}",
        "insert first (per fresh bag)", first_ev
    );
    println!(
        "  {:<34} | {:>12.4}  (n={APPEND_BATCH}, 摊销)",
        "insert append (same bag)", append_ev
    );
    println!("  {:<34} | {:>12.2}", "merge 4 -> fresh bag", merge_ev);
    let verdict = new_ev == 0.0
        && clone_ev == 0.0
        && iter_ev == 0.0
        && overwrite_ev == 0.0
        && first_ev >= 1.0;
    println!(
        "  判定: {}（new/clone/iter/覆盖 = Arc 自增/原地改零事件；首插 ≥1 如实；追加/merge 如实摊销）",
        if verdict {
            "PASS 零档全 0，首插有事件"
        } else {
            "FAIL 零档出现事件或首插无事件"
        }
    );
}

// ---- ② ns 锚 ----

fn dim2_ns(names: &[&'static str]) {
    println!("\n[dim2] ns 锚（中位三连跑）");
    println!("  {:<36} | {:>10}", "op", "ns/call");
    let n = 200_000u64;
    let bag4 = filled(4, names);

    let new_ns = median3(|| batch_ns(FieldBag::new, n));
    let clone_ns = median3(|| batch_ns(|| bag4.clone(), n));
    let mut overwrite = filled(4, names);
    let insert_ns = median3(|| batch_ns(|| overwrite.insert("f1", FieldValue::Count(7)), n));
    let get_ns = median3(|| batch_ns(|| bag4.get("f1"), n));
    let iter_ns = median3(|| {
        batch_ns(
            || {
                for (name, value) in bag4.iter() {
                    black_box((name, value));
                }
            },
            n,
        )
    });
    let merge_src = filled(4, names);
    let merge_ns = median3(|| {
        batch_ns(
            || {
                let mut target = FieldBag::new();
                target.merge(&merge_src);
                target
            },
            n,
        )
    });

    println!("  {:<36} | {:>10.1}", "new", new_ns);
    println!("  {:<36} | {:>10.1}", "clone (4 fields)", clone_ns);
    println!(
        "  {:<36} | {:>10.1}",
        "insert (overwrite steady)", insert_ns
    );
    println!("  {:<36} | {:>10.1}", "get (hit)", get_ns);
    println!("  {:<36} | {:>10.1}", "iter (4 fields)", iter_ns);
    println!(
        "  {:<36} | {:>10.1}",
        "merge (4 fields, 含新空袋)", merge_ns
    );
    println!("  判定: new/clone 是一次原子自增 + Arc 装配；insert/get/iter 线性小集合扫描");
}

// ---- ③ 规模梯度（判线性）----

fn dim3_scale(names: &[&'static str]) {
    println!("\n[dim3] 规模梯度（iter ns/call 判线性；append ns/次含 O(n) 扫描，批内新键）");
    println!(
        "  {:>8} | {:>14} | {:>16}",
        "fields", "iter ns/call", "append ns/insert"
    );
    for n_fields in [4usize, 32, 256] {
        let bag = filled(n_fields, names);
        let iter_n = match n_fields {
            4 => 200_000u64,
            32 => 50_000,
            _ => 10_000,
        };
        let iter_ns = median3(|| {
            batch_ns(
                || {
                    for (name, value) in bag.iter() {
                        black_box((name, value));
                    }
                },
                iter_n,
            )
        });
        // append：往同尺寸新袋追加一批新键取均值（名字取池内 n_fields 之后段，计时外备好）。
        let fresh = &names[n_fields..n_fields + APPEND_BATCH];
        let append_ns = median3(|| {
            let mut grow = bag.clone();
            let t = Instant::now();
            for (i, fname) in fresh.iter().enumerate() {
                grow.insert(fname, FieldValue::Count(i as u64));
            }
            black_box(grow);
            t.elapsed().as_nanos() as f64 / APPEND_BATCH as f64
        });
        println!("  {n_fields:>8} | {iter_ns:>14.1} | {append_ns:>16.1}");
    }
    println!(
        "  判定: iter 随字段数近似线性（每字段 ~一次元组读）；append 的 O(n) 扫描随基数上升（armor O(n) 条）"
    );
}

// ---- ④ clone 成本与字段数无关（O(1) 证据）----

fn dim4_clone_flat(names: &[&'static str]) {
    println!("\n[dim4] clone 成本与字段数无关（O(1) 证据：4 vs 256）");
    let bag4 = filled(4, names);
    let bag256 = filled(256, names);
    let n = 500_000u64;
    let ns4 = median3(|| batch_ns(|| bag4.clone(), n));
    let ns256 = median3(|| batch_ns(|| bag256.clone(), n));
    println!("  clone (4 字段)   : {ns4:>8.1} ns");
    println!("  clone (256 字段) : {ns256:>8.1} ns");
    println!(
        "  判定: 倍率 {:.2}x——同量级即 O(1)（一次原子自增，与字段数无关）",
        ns256 / ns4
    );
}

// ---- ⑤ COW 改写成本 ----

fn dim5_cow_rewrite(names: &[&'static str]) {
    println!("\n[dim5] COW 改写成本（共享后首改付一次性 O(n) 整表克隆；单批如实报）");
    let shared = filled(256, names);
    let mut exclusive = filled(4, names);
    let base_ns = median3(|| batch_ns(|| exclusive.insert("f0", FieldValue::Count(1)), 100_000));
    let mut cow_bag = shared.clone(); // 与 shared 在飞共享（引用计数 2）
    let first_ns = {
        let t = Instant::now();
        cow_bag.insert("cow", FieldValue::Count(1)); // 触发 COW：256 项整表克隆一次
        t.elapsed().as_nanos() as f64
    };
    let after_ns = median3(|| batch_ns(|| cow_bag.insert("f1", FieldValue::Count(2)), 100_000));
    println!("  独占袋覆盖稳态           : {base_ns:>8.1} ns/次");
    println!("  共享后首改（256 项克隆） : {first_ns:>8.1} ns/次（单批，粗测噪声大）");
    println!("  首改之后稳态（已独占）   : {after_ns:>8.1} ns/次");
    println!("  判定: 改写者一次性付 O(n)，之后回到稳态；共享副本不受影响（tests 钉隔离）");
}

// ---- ⑥ 跨线程共享 ----

fn dim6_cross_thread(names: &[&'static str]) {
    println!("\n[dim6] 跨线程共享（clone 交后台线程只读，O(1) 交接 + 读回校验）");
    let bag = filled(256, names);
    let expect: u64 = bag
        .iter()
        .filter_map(|(_, value)| match value {
            FieldValue::Count(c) => Some(*c),
            _ => None,
        })
        .sum();
    let handoff_ns = median3(|| batch_ns(|| bag.clone(), 200_000));
    let reader = thread::spawn(move || {
        bag.iter()
            .filter_map(|(_, value)| match value {
                FieldValue::Count(c) => Some(*c),
                _ => None,
            })
            .sum::<u64>()
    });
    let got = reader.join().expect("后台线程读回");
    println!("  交接成本（clone ns）: {handoff_ns:.1} ns（同 dim2 clone 量级 = O(1)）");
    println!("  读回校验: sum = {got}（主线程侧 {expect}）");
    let verdict = if got == expect { "PASS" } else { "FAIL" };
    println!(
        "  判定: {verdict}——Arc 共享跨线程只读安全；match 带 `_` 臂即下游 crate 视角的词表纪律"
    );
}

// ---- ⑦ 收窄声明 ----

fn dim7_narrowing(names: &[&'static str]) {
    println!("\n[dim7] 收窄声明（如实对照）");
    let bag = filled(256, names);
    let n = 50_000u64;
    let hit_ns = median3(|| batch_ns(|| bag.get("f128"), n));
    let miss_ns = median3(|| batch_ns(|| bag.get("nope"), n));
    println!("  get 命中（256 字段袋）: {hit_ns:>8.1} ns/次");
    println!("  get 未命中（全表扫描）: {miss_ns:>8.1} ns/次");
    println!("  声明 1: 无 serde/无导出/无上报——值面到此为止，导出格式归消费方自映射（契约层零依赖纪律）");
    println!("  声明 2: O(n) 线性 get/insert——观测袋是个位数量级小集合，线性换零哈希表分配；");
    println!("          升级路径 = 排序索引，出现大规模袋证据再升级");
    println!("  声明 3: 字段名 &'static str 零分配（dim1 new/clone/iter=0 佐证），无字段名注册表、无日志级别语义");
}
