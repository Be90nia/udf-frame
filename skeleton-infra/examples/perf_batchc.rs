//! 温域三小件定性锚采集器（批 C：compose 组合税 / tracingx 净转译 / logx mask 曲线）。
//!
//! 跑法（udf-frame 根）：
//! ```text
//! cargo run --release -p udf-skeleton-infra --example perf_batchc --features compose,tracing,log-fs
//! ```
//!
//! 口径注（沿 multidim_eval 惯例）：
//! ① 纯计时口径：无 CountingAllocator（批 C 只采时延面；内存零分配语义见各域
//!    doc——compose 子 EffectBuf 空态零分配、logx mask 恒一次 String 分配）；
//! ② Instant 时钟对开销（~2×20ns）双臂同担——大批量均值摊薄，税值取差值口径；
//! ③ 粗测（std Instant）非 criterion——数字供判级，不供微优化基线；
//! ④ black_box 防 LLVM 消除；每个 batch_mean 三连跑取中位。
//!
//! 报告：reports/2026-10-16-perf-batchc.md（agentwiki 根）。

use std::convert::Infallible;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use skeleton::{EffectBuf, Fragment, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore as Store;
use skeleton_infra::{bridge_events_to_tracing, combine, mask_secrets, AuditEvent};

// ---- 测量助手（multidim_eval 同款）----

/// 大批量均值：ns/op。返回值黑箱化后弃置，防 LLVM 消除被测 op。
fn batch_mean<T>(mut op: impl FnMut() -> T, n: u64) -> f64 {
    let t = Instant::now();
    for _ in 0..n {
        black_box(op());
    }
    t.elapsed().as_nanos() as f64 / n as f64
}

fn median3(mut f: impl FnMut() -> f64) -> f64 {
    let mut v: Vec<f64> = (0..3).map(|_| f()).collect();
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[1]
}

fn main() {
    perf_compose();
    perf_tracingx();
    perf_logx();
}

// ---- 臂一：compose 组合税（combine! 装配 vs 等义手装单 Feature）----

#[derive(Clone, Debug, PartialEq)]
struct Parent {
    total: u32,
    child: u32,
}

#[derive(Clone, Debug, PartialEq)]
enum PAction {
    Child(u32),
    Touch,
}

struct ChildCounter;

impl Reducer for ChildCounter {
    type State = u32;
    type Action = u32;
    type Op = Infallible;
    fn initial(&self) -> u32 {
        0
    }
    fn reduce(&self, s: &u32, a: u32, _out: &mut EffectBuf<u32, Infallible>) -> u32 {
        s + a
    }
}

/// 等义手装：子逻辑内联进父 Feature（child lens 加法；Touch 恒等 = combine
/// 的 unwrap 未命中分支同形——两臂在被测 Child 路径与 Touch 路径逐语义等价）。
fn hand_reduce(s: &Parent, a: PAction, _out: &mut EffectBuf<PAction, Infallible>) -> Parent {
    match a {
        PAction::Child(n) => {
            let mut next = s.clone();
            next.child += n;
            next
        }
        PAction::Touch => Parent {
            total: s.total,
            child: s.child,
        },
    }
}

struct ParentHand;

impl Reducer for ParentHand {
    type State = Parent;
    type Action = PAction;
    type Op = Infallible;
    fn initial(&self) -> Parent {
        Parent {
            total: 100,
            child: 0,
        }
    }
    fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
        hand_reduce(s, a, out)
    }
}

/// combine! 装配形态（compose.rs 文档例同款：分片逐 dispatch 现场构造——
/// 闭包全 ZST，构造即消除；这正是「零 dyn 零装箱」声明的采用方标准形）。
struct ParentCombo;

impl Reducer for ParentCombo {
    type State = Parent;
    type Action = PAction;
    type Op = Infallible;
    fn initial(&self) -> Parent {
        Parent {
            total: 100,
            child: 0,
        }
    }
    fn reduce(&self, s: &Parent, a: PAction, out: &mut EffectBuf<PAction, Infallible>) -> Parent {
        combine!(ChildCounter, Parent.child, PAction::Child).reduce(s, a, out)
    }
}

fn perf_compose() {
    println!("== perf: compose 组合税（combine! 装配 vs 等义手装）==");
    const BATCH: u64 = 1_000_000;
    let parent = Parent {
        total: 100,
        child: 0,
    };
    let frag = combine!(ChildCounter, Parent.child, PAction::Child);

    // 裸 reduce（无 store 底噪）：净组合税 = 两臂差值。输入 black_box 防 LLVM
    // 折叠（首跑两臂全折叠至 0.5ns=循环底噪，读数无意义）；时钟对摊薄于批均值。
    let hand_ns = median3(|| {
        batch_mean(
            || {
                let mut out: EffectBuf<PAction, Infallible> = EffectBuf::new();
                hand_reduce(black_box(&parent), black_box(PAction::Child(1)), &mut out)
            },
            BATCH,
        )
    });
    let combo_ns = median3(|| {
        batch_mean(
            || {
                let mut out: EffectBuf<PAction, Infallible> = EffectBuf::new();
                frag.reduce(black_box(&parent), black_box(PAction::Child(1)), &mut out)
            },
            BATCH,
        )
    });
    println!(
        "compose_bare_hand_ns={hand_ns:.1} compose_bare_combo_ns={combo_ns:.1} compose_bare_tax_ns={:.1}",
        combo_ns - hand_ns
    );

    // store e2e（dispatch 全含 ~75ns 底噪上解析个位数税值）：交错轮次取每轮差值
    // 的中位——首跑双臂先后各 median3 出 -9.5ns 负税值（后跑臂偏热的序偏/布局
    // 漂移带内），交错是序偏的机械消解。
    const ROUNDS: u64 = 5;
    let hstore = Store::new(ParentHand, NoFx);
    let cstore = Store::new(ParentCombo, NoFx);
    // Touch 等价行使（dead_code 消解 + 两臂 Touch 路径逐语义等价断言）
    hstore.dispatch(PAction::Touch);
    cstore.dispatch(PAction::Touch);
    assert_eq!(*hstore.snapshot(), *cstore.snapshot(), "Touch 路径两臂等价");
    for _ in 0..10_000 {
        hstore.dispatch(PAction::Child(1));
        cstore.dispatch(PAction::Child(1));
    }
    let mut hand_tot = 0.0;
    let mut combo_tot = 0.0;
    let mut deltas: Vec<f64> = Vec::new();
    for _ in 0..ROUNDS {
        let h = batch_mean(|| hstore.dispatch(PAction::Child(1)), BATCH);
        let c = batch_mean(|| cstore.dispatch(PAction::Child(1)), BATCH);
        deltas.push(c - h);
        hand_tot += h;
        combo_tot += c;
    }
    deltas.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let tax = deltas[ROUNDS as usize / 2];
    let want = Parent {
        total: 100,
        child: 10_000 + (ROUNDS * BATCH) as u32,
    };
    assert_eq!(*hstore.snapshot(), want, "手装臂守恒");
    assert_eq!(
        *cstore.snapshot(),
        want,
        "combine 臂守恒；两臂终态逐字段等价"
    );
    println!(
        "compose_e2e_hand_ns={:.1} compose_e2e_combo_ns={:.1} compose_e2e_tax_ns_median5={tax:.1} rounds={deltas:.1?}",
        hand_tot / ROUNDS as f64,
        combo_tot / ROUNDS as f64,
    );
    hstore.close();
    cstore.close();
}

// ---- 臂二：tracingx 净转译分离（桥 vs 空回调订阅，差值 = 净转译）----

struct Count;

impl Reducer for Count {
    type State = u64;
    type Action = ();
    type Op = Infallible;
    fn initial(&self) -> u64 {
        0
    }
    fn reduce(&self, s: &u64, (): (), _out: &mut EffectBuf<(), Infallible>) -> u64 {
        s + 1
    }
}

struct Nop;

impl skeleton::EffectRuntime<(), Infallible> for Nop {
    fn run(&mut self, _: skeleton::Effect<(), Infallible>, _: &skeleton::ActionSender<()>) {}
}

/// 计数订阅者：只数 event 次数（字段记录全含口径的宿主下限替身）。
struct CountingSub(AtomicU64);

impl tracing::Subscriber for CountingSub {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::Id {
        tracing::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::Id, _: &tracing::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn enter(&self, _: &tracing::Id) {}
    fn exit(&self, _: &tracing::Id) {}
}

fn perf_tracingx() {
    println!("== perf: tracingx 净转译分离（桥 − 空回调 = 净转译，交错轮次）==");
    const BATCH: u64 = 500_000;
    const ROUNDS: u64 = 5;
    let store = Store::new(Count, Nop);
    for _ in 0..10_000 {
        store.dispatch(());
    }
    let mut total: u64 = 10_000;

    // T0：无订阅基线（对照上下文，非差值臂）
    let t0 = median3(|| batch_mean(|| store.dispatch(()), BATCH));
    total += 3 * BATCH;
    let sub = std::sync::Arc::new(CountingSub(AtomicU64::new(0)));

    // 交错轮次（序偏/机器漂移的机械消解，compose e2e 同款）：单订阅槽逐轮
    // 换票。每轮四拍：T1 空回调（订阅分发本体）→ T2 桥无订阅者（净转译，
    // event! 走 callsite 兴趣缓存即返）→ 计数订阅者安装 → T1' 空回调对照 →
    // T3 桥+计数订阅者（转译+字段记录全含）。
    let mut d2: Vec<f64> = Vec::new();
    let mut d3: Vec<f64> = Vec::new();
    let mut t1s: Vec<f64> = Vec::new();
    // 计数订阅者预热一轮（订阅者注册缓存走热），不进测量
    {
        let _guard = tracing::subscriber::set_default(sub.clone());
        let t = bridge_events_to_tracing(&store, 64);
        batch_mean(|| store.dispatch(()), BATCH);
        drop(t);
        drop(_guard);
        total += BATCH;
    }
    for _ in 0..ROUNDS {
        let t1_ticket = store.subscribe_events(64, |_: &AuditEvent| {});
        let t1 = batch_mean(|| store.dispatch(()), BATCH);
        drop(t1_ticket);
        let t2_ticket = bridge_events_to_tracing(&store, 64);
        let t2 = batch_mean(|| store.dispatch(()), BATCH);
        drop(t2_ticket);
        let _guard = tracing::subscriber::set_default(sub.clone());
        let t1b_ticket = store.subscribe_events(64, |_: &AuditEvent| {});
        let t1b = batch_mean(|| store.dispatch(()), BATCH);
        drop(t1b_ticket);
        let t3_ticket = bridge_events_to_tracing(&store, 64);
        let t3 = batch_mean(|| store.dispatch(()), BATCH);
        drop(t3_ticket);
        drop(_guard);
        t1s.push(t1);
        d2.push(t2 - t1);
        d3.push(t3 - t1b);
        total += 4 * BATCH;
    }
    let med = |v: &mut Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let net_translate = med(&mut d2);
    let translate_plus_fields = med(&mut d3);
    let t1_med = med(&mut t1s);
    // 守恒断言：每 dispatch 恰 2 事件（Enqueued+Reduced），证明转译实测发生
    assert_eq!(
        sub.0.load(Ordering::Relaxed),
        (ROUNDS + 1) * BATCH * 2,
        "转译事件计数守恒"
    );
    println!("tracingx_t0_nosub_ns={t0:.1} tracingx_t1_nopsub_ns={t1_med:.1}");
    println!(
        "tracingx_dispatch_body_ns={:.1} tracingx_net_translate_ns_median5={net_translate:.1} tracingx_translate_plus_fields_ns_median5={translate_plus_fields:.1}",
        t1_med - t0,
    );
    assert_eq!(*store.snapshot(), total, "dispatch 计数守恒");
    store.close();
}

// ---- 臂三：logx mask_secrets O(n×m) 曲线（无命中纯扫描两轴 + 命中重写一点）----

fn perf_logx() {
    println!("== perf: logx mask_secrets 曲线（无命中纯扫描，O(n×m) 形态验证）==");
    const LENS: [usize; 2] = [4096, 65_536];
    const RULES: [usize; 3] = [1, 8, 64];
    let mut cells: Vec<(&str, usize, usize, f64)> = Vec::new();
    for &len in &LENS {
        let input = "x".repeat(len);
        for &m in &RULES {
            let secret_strings: Vec<String> = (0..m).map(|i| format!("zz-secret-{i:02}")).collect();
            let secrets: Vec<&str> = secret_strings.iter().map(String::as_str).collect();
            // 单元成本自适应批量：目标 ~2M 字符规则交叠量，下限 20 防时钟噪声
            let n = (2_000_000 / (m as u64 * len as u64 / 1024)).clamp(20, 20_000);
            let ns = median3(|| batch_mean(|| mask_secrets(&input, &secrets), n));
            println!("logx_mask_nomatch len={len} rules={m} n={n} ns={ns:.1}");
            cells.push(("nomatch", len, m, ns));
        }
    }
    // 形态比值：固定文本看规则轴、固定规则看文本轴（O(n×m) 则比值 ≈ 倍率）
    let at = |len: usize, m: usize| cells.iter().find(|c| c.1 == len && c.2 == m).unwrap().3;
    for &m in &RULES {
        println!(
            "logx_ratio_text_64k_over_4k rules={m} ratio={:.2}",
            at(65_536, m) / at(4096, m)
        );
    }
    for &len in &LENS {
        println!(
            "logx_ratio_rules_64_over_1 len={len} ratio={:.2} rules_8_over_1={:.2}",
            at(len, 64) / at(len, 1),
            at(len, 8) / at(len, 1)
        );
    }
    // 命中重写一点（风味值）：8 命中 @4KiB，每命中规则一次 String 重建
    let mut input = "x".repeat(4096);
    for i in 0..8 {
        let mark = format!("zz-secret-00@{}", i * 500);
        let pos = i * 500;
        input.replace_range(pos..pos, &mark);
    }
    let secrets = ["zz-secret-00"];
    let ns = median3(|| batch_mean(|| mask_secrets(&input, &secrets), 2_000));
    let masked = mask_secrets(&input, &secrets);
    assert_eq!(masked.matches("***").count(), 8, "命中重写语义守恒");
    println!("logx_mask_match_8hits_4k ns={ns:.1}");
}
