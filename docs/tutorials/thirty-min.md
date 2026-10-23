# 三十分钟串联教程：任务看板（非零效果路径端到端）

> 读者：刚跑完 [README 快起](../../README.md)（`cargo run -p udf-skeleton-infra --example quickstart`）的你。
> 快起只走通了**零效果**最小环；本教程用一个小看板项目，把第一天真正会用到的非零效果路径
> 一次串联：**serde 词表 → journal 持久化 → FxScope 效果作用域 → combine 父子分片 → TestStore 验收**。
> 全部代码块取自真实运行过的程序（预期输出均为实际捕获文本）；每步末尾有「故意踩坑」，报错文本皆为真实输出。
>
> 时间盒：每步标注预计分钟（含读代码），首次编译含依赖约 15–30s（本地缓存后秒级）。

## 第 0 步（2 分钟）：建项目、接依赖

框架 0.1.0 未发布 crates.io，教程期用路径依赖。**在 udf-frame 仓库外**任选目录建项目
（发布后可整行换成版本号）。**禁在 `udf-frame/` 树内 `cargo new`**——新包会被根
workspace 吸收，照做即仓库损坏（`multiple workspace roots`，盲测实测复现）：

```sh
mkdir -p ~/kanban-demo && cd ~/kanban-demo
cargo new kanban && cd kanban
```

`Cargo.toml` 整体替换为（`<UDF_ROOT>` 换成你的 udf-frame 克隆绝对路径）：

```toml
[package]
name = "kanban"
version = "0.1.0"
edition = "2021"

# 独立 workspace：不并入 udf-frame workspace
[workspace]

[dependencies]
# path 指向 udf-frame 克隆位置；首次发布后可换版本号。
# 发布名带 udf- 前缀，但 lib 名未变——`use skeleton::` / `use skeleton_infra::` 照旧，零 import 改动。
udf-skeleton = { path = "<UDF_ROOT>/skeleton" }
udf-skeleton-infra = { path = "<UDF_ROOT>/skeleton-infra", features = [
    "journal",
    "tokio",
    "testing",
    "compose",
] }
# 不想逐个挑？一行全取：features = ["full"]（等价全部开关；子开关=进阶裁剪）。
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time", "sync"] }
```

四个 feature 对应后四步：`journal`（第 2 步）、`tokio`（第 3 步）、`compose`（第 4 步）、`testing`（第 5 步）。

## 第 1 步（4 分钟）：带 `#[serde(tag)]` 的域词表 + SyncStore 最小环

快起的 `u32` 计数器证明了环转得起来；现在把同一套词汇换成**看板域**。Action 是 enum，
并且**第一天就带 `#[serde(tag = "kind")]`**——第 2 步的账本要求每个动作都能序列化成
JSON object，内部 tag 让 unit variant 也变成 object（一步到位，避免返工）。

`src/main.rs` 整体替换为：

```rust
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Column {
    Todo,
    Doing,
    Done,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Task {
    id: u64,
    title: String,
    col: Column,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Board {
    next_id: u64,
    tasks: Vec<Task>,
}

/// 内部 tag 使每个变体都序列化为 JSON object（journal 的行格式要求）。
/// tag 键不能叫 "t"——那是 journal 行 tag 的顶层保留键。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum Action {
    Add { title: String },
    Move { id: u64, to: Column },
    Clear,
}

struct BoardFeature;

impl Reducer for BoardFeature {
    type State = Board;
    type Action = Action;
    type Op = Infallible; // 零效果：Effect::Run 不可构造，NoFx 即合法解释器

    fn initial(&self) -> Board {
        Board::default()
    }

    fn reduce(&self, s: &Board, a: Action, _out: &mut EffectBuf<Action, Infallible>) -> Board {
        match a {
            Action::Add { title } => {
                let mut next = Board {
                    next_id: s.next_id + 1,
                    tasks: s.tasks.clone(),
                };
                next.tasks.push(Task {
                    id: s.next_id,
                    title,
                    col: Column::Todo,
                });
                next
            }
            Action::Move { id, to } => {
                let mut next = s.clone();
                for t in &mut next.tasks {
                    if t.id == id {
                        t.col = to;
                    }
                }
                next
            }
            Action::Clear => {
                let mut next = s.clone();
                next.tasks.retain(|t| t.col != Column::Done);
                next
            }
        }
    }
}

fn main() {
    let store = SyncStore::new(BoardFeature, NoFx);
    store.dispatch(Action::Add {
        title: "写周报".into(),
    });
    store.dispatch(Action::Move {
        id: 0,
        to: Column::Doing,
    });
    println!("{:?}", *store.snapshot());
    store.close();
}
```

```sh
cargo run
```

预期输出：

```text
Board { next_id: 1, tasks: [Task { id: 0, title: "写周报", col: Doing }] }
```

**这步教你什么契约**：`Op = Infallible` 让 `Effect::Run` 在类型层不可构造——零效果程序配
`NoFx` 缺省解释器，副作用的缺席是编译期事实，不是纪律约定。reducer 是纯函数：状态进
出、效果写 `out`，禁 IO 由「本 crate 零 IO 依赖」结构性保证。

## 第 2 步（7 分钟）：journal 持久化——挂载、flush、重放恢复

给看板接上真相源。两条铁律贯穿本步：**先落盘后 apply**（append Err ⇒ 禁 apply，内存态
永不领先已持久前缀）、**flush 点 = 持久点**（写侧有 64KiB 缓冲，读侧可见性 = 已 flush 前缀）。

`src/main.rs` 整体替换为：

```rust
use serde::{Deserialize, Serialize};
use skeleton::{EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::{Journal as _, JsonlJournal};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Column {
    Todo,
    Doing,
    Done,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Task {
    id: u64,
    title: String,
    col: Column,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Board {
    next_id: u64,
    tasks: Vec<Task>,
}

/// 内部 tag 使每个变体都序列化为 JSON object（journal 的行格式要求）。
/// tag 键不能叫 "t"——那是 journal 行 tag 的顶层保留键。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum Action {
    Add { title: String },
    Move { id: u64, to: Column },
    Clear,
}

struct BoardFeature;

impl Reducer for BoardFeature {
    type State = Board;
    type Action = Action;
    type Op = Infallible;

    fn initial(&self) -> Board {
        Board::default()
    }

    fn reduce(&self, s: &Board, a: Action, _out: &mut EffectBuf<Action, Infallible>) -> Board {
        match a {
            Action::Add { title } => {
                let mut next = Board {
                    next_id: s.next_id + 1,
                    tasks: s.tasks.clone(),
                };
                next.tasks.push(Task {
                    id: s.next_id,
                    title,
                    col: Column::Todo,
                });
                next
            }
            Action::Move { id, to } => {
                let mut next = s.clone();
                for t in &mut next.tasks {
                    if t.id == id {
                        t.col = to;
                    }
                }
                next
            }
            Action::Clear => {
                let mut next = s.clone();
                next.tasks.retain(|t| t.col != Column::Done);
                next
            }
        }
    }
}

/// 先落盘后 apply：append Err ⇒ 禁 apply（内存态永不领先已持久前缀）。
/// 账本构造期定型（`JsonlJournal::<Action, Board>::create(..)`）后方法全直呼：
/// `A` 从 action 参数推断，状态面是关联类型、被 `Self` 钉死——零 turbofish。
fn journaled_dispatch(
    store: &SyncStore<Board, Action>,
    j: &mut JsonlJournal<Action, Board>,
    a: Action,
) {
    j.append_action(&a).expect("journal append");
    store.dispatch(a);
}

fn main() {
    // create 撞已存在文件 = Err(AlreadyExists)；演示前先清残留
    let _ = std::fs::remove_file("board.jsonl");

    // —— 会话 1：挂账本，走三个动作，flush（flush 点 = 持久点）——
    {
        let mut j = JsonlJournal::<Action, Board>::create("board.jsonl").expect("create journal");
        let store = SyncStore::new(BoardFeature, NoFx);
        journaled_dispatch(&store, &mut j, Action::Add { title: "写周报".into() });
        journaled_dispatch(&store, &mut j, Action::Add { title: "交房租".into() });
        journaled_dispatch(
            &store,
            &mut j,
            Action::Move {
                id: 1,
                to: Column::Doing,
            },
        );
        j.flush().expect("flush");
        store.close();
    } // 账本在此 drop —— 相当于进程带着完整持久历史重启

    // —— 会话 2：重开 + replay + 经 store 逐条 dispatch 重建终态 ——
    // open 第二参数=账本格式版本锚（创建时写入头行，重开时校验：不符显式拒载）
    let j = JsonlJournal::<Action, Board>::open("board.jsonl", 1).expect("open journal");
    let store = SyncStore::new(BoardFeature, NoFx);
    let actions: Vec<Action> = j
        .replay()
        .expect("replay")
        .map(|r| r.expect("corrupt journal line"))
        .collect();
    let replayed = actions.len();
    for a in actions {
        store.dispatch(a); // 重放走 dispatch（append 只属于新动作，重放禁重复入账）
    }

    println!("重放 {replayed} 条，终态：{:?}", *store.snapshot());
    assert_eq!(replayed, 3);
    assert_eq!(
        *store.snapshot(),
        Board {
            next_id: 2,
            tasks: vec![
                Task {
                    id: 0,
                    title: "写周报".into(),
                    col: Column::Todo
                },
                Task {
                    id: 1,
                    title: "交房租".into(),
                    col: Column::Doing
                },
            ]
        }
    );
    store.close();
    let _ = std::fs::remove_file("board.jsonl");
}
```

预期输出：

```text
重放 3 条，终态：Board { next_id: 2, tasks: [Task { id: 0, title: "写周报", col: Todo }, Task { id: 1, title: "交房租", col: Doing }] }
```

两个惯用形值得驻足：

- **构造期定型，调用期直呼**：`JsonlJournal::<Action, Board>::create(..)` 一次 turbofish 把
  (A, 状态) 焊进账本实例——此后 `j.append_action(&a)` 直呼（`A` 从参数推断，状态面是关联
  类型、被 `Self` 钉死），`MutexGuard` 后 `guard.append_action(&a)` 同形，`j.replay()` 返回
  值全推断。trait-object 面用别名 `DynJournal<A, S>`（`State =` 短形）。
- **重放协议**：重放的动作经 `store.dispatch` 逐条走，**不是**裸 reduce、也**不是**再 append
  ——派生动作（Emit 回灌）由排空环再生，终态与 live 收敛，且禁重复入账。

### 故意踩坑（各跑一次，报错为真实输出）

**坑 A：enum 不加 `serde(tag)`，unit variant 是裸字符串**。struct variant（`Add`/`Move`）
默认外部 tag 序列化恰好是 object、能过——删掉 `#[serde(tag = "kind")]` 只跑主路径
**不报错**；必须让 unit variant 真正入账才命中：把会话 1 的第一条动作换成
`journaled_dispatch(&store, &mut j, Action::Clear)` 再跑，`Clear` 序列化为 `"Clear"`
裸字符串——不是 JSON object，首条 append 即拒（fail-closed，字节零落盘）：

```text
journal append: Serialize(Error("action/state must serialize to a JSON object for tag flattening", line: 0, column: 0))
```

（这坑专咬 unit variant：只有 unit variant 序列化成裸字符串。前缀 `journal append:` 来自 `journaled_dispatch` 里 `.expect("journal append")` 的 panic 标注。）

**坑 B：tag 键恰好叫 `"t"`**——与 journal 行格式 `{"t":"a", ...payload}` 的顶层保留键相撞，
append 当场拒绝（fail-closed，字节零落盘）：

```text
called `Result::unwrap()` on an `Err` value: Serialize(Error("payload must not contain a top-level \"t\" key (reserved for line tag)", line: 0, column: 0))
```

**坑 C：忘 flush，读侧看到空账**。写侧不 flush 就另开读侧（或进程崩在 flush 前），
读侧可见性 = 已 flush 前缀 = 0 行：

```text
[flush 前] 重开读侧：重放 0 条
[flush 后] 重开读侧：重放 2 条
```

**这步教你什么契约**：真相源是账本不是内存；「先落盘后 apply」让崩溃窗口零副作用
（append 失败换对象形状重试即可）；重放走 dispatch 是恢复完备契约的一半（另一半见
第 3 步的效果环）。

## 第 3 步（8 分钟）：FxScope 效果作用域——订阅、解释器、回灌闭环

看板加点行为：任务进 Done 时「对外庆祝一下」（模拟通知/审计等外部交互）。reducer 只
**描述**（`Effect::Run`），解释器才**执行**；解释器在 tokio 任务里跑完经 `ActionSender`
回灌 `DoneNotified`，环闭合——而回灌动作同样**先落盘**。

`src/main.rs` 整体替换为：

```rust
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use skeleton::{ActionSender, Effect, EffectBuf, EffectId, EffectRuntime, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;
use skeleton_infra::{expect_idle, FxScope, Journal as _, JsonlJournal, Spawned};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Column {
    Todo,
    Doing,
    Done,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Task {
    id: u64,
    title: String,
    col: Column,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct Board {
    next_id: u64,
    tasks: Vec<Task>,
    /// 已对外庆祝过的任务 id（DoneNotified 回灌写入——回环在状态上留痕）
    cheers: Vec<u64>,
}

/// 内部 tag 使每个变体都序列化为 JSON object（journal 的行格式要求）。
/// tag 键不能叫 "t"——那是 journal 行 tag 的顶层保留键。
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
enum Action {
    Add { title: String },
    Move { id: u64, to: Column },
    /// 解释器回灌动作（环闭合）：外部已通知，状态记一笔
    DoneNotified { id: u64 },
    Clear,
}

/// 副作用词表：宿主定义的描述式 enum。reducer 只「描述」，解释器才「执行」。
#[derive(Debug, PartialEq)]
enum Fx {
    /// 任务进 Done：请外部世界庆祝（模拟通知/审计等外部交互）
    DoneReached { id: u64, title: String },
}

struct BoardFeature;

impl Reducer for BoardFeature {
    type State = Board;
    type Action = Action;
    type Op = Fx; // 非零效果词表：解释器必须真实提供

    fn initial(&self) -> Board {
        Board::default()
    }

    fn reduce(&self, s: &Board, a: Action, out: &mut EffectBuf<Action, Fx>) -> Board {
        match a {
            Action::Add { title } => {
                let mut next = Board {
                    next_id: s.next_id + 1,
                    tasks: s.tasks.clone(),
                    cheers: s.cheers.clone(),
                };
                next.tasks.push(Task {
                    id: s.next_id,
                    title,
                    col: Column::Todo,
                });
                next
            }
            Action::Move { id, to } => {
                let mut next = s.clone();
                for t in &mut next.tasks {
                    if t.id == id {
                        t.col = to;
                        if to == Column::Done {
                            // 描述式副作用离场：解释器执行后回灌 DoneNotified
                            out.push(Effect::Run {
                                id: EffectId(id),
                                op: Fx::DoneReached {
                                    id,
                                    title: t.title.clone(),
                                },
                            });
                        }
                    }
                }
                next
            }
            Action::DoneNotified { id } => {
                let mut next = s.clone();
                next.cheers.push(id);
                next
            }
            Action::Clear => {
                let mut next = s.clone();
                next.tasks.retain(|t| t.col != Column::Done);
                next
            }
        }
    }
}

/// 活跃会话的解释器：效果 spawn 后即返（dispatch 同步排空，解释器禁阻塞）；
/// 回灌句柄由核心经 run 的 back 参数传入，克隆进任务即闭环，宿主无需自存。
struct HostRuntime {
    scope: Arc<FxScope>,
    journal: Arc<Mutex<JsonlJournal<Action, Board>>>,
    pending: Arc<Mutex<Vec<Spawned<()>>>>,
}

impl EffectRuntime<Action, Fx> for HostRuntime {
    fn run(&mut self, effect: Effect<Action, Fx>, back: &ActionSender<Action>) {
        let next = match effect {
            Effect::Run {
                id: _,
                op: Fx::DoneReached { id, title: _ },
            } => Action::DoneNotified { id },
            Effect::Cancel { .. } => return,
            // Emit 由核心折叠入队，永不进入解释器转发路径
            Effect::Emit(_) => unreachable!("Emit never reaches the interpreter"),
        };
        let back = back.clone();
        let journal = self.journal.clone();
        let handle = self
            .scope
            .spawn(async move {
                tokio::time::sleep(Duration::from_millis(5)).await; // 模拟外部 IO
                // 回灌挂点·先落盘后 apply：append Err ⇒ 禁 send（fail-closed，禁静默丢）
                let appended = journal
                    .lock()
                    .expect("journal poisoned")
                    .append_action(&next)
                    .is_ok();
                assert!(appended, "journal append failed: refusing to re-enter");
                assert!(back.dispatch(next), "store closed: re-entry rejected");
            })
            .expect("scope open");
        self.pending.lock().expect("pending poisoned").push(handle);
    }
}

/// 恢复会话的解释器：通知早已发过（账本里有 DoneNotified 行），重放时
/// 静默丢弃效果即可——解释器是宿主件，换一件即换行为。
struct SilentFx;

impl EffectRuntime<Action, Fx> for SilentFx {
    fn run(&mut self, _effect: Effect<Action, Fx>, _back: &ActionSender<Action>) {}
}

/// 先落盘后 apply：append Err ⇒ 禁 apply。
fn journaled_dispatch(
    store: &SyncStore<Board, Action, Fx>,
    journal: &Arc<Mutex<JsonlJournal<Action, Board>>>,
    a: Action,
) {
    journal
        .lock()
        .expect("journal poisoned")
        .append_action(&a)
        .expect("journal append");
    store.dispatch(a);
}

#[tokio::main]
async fn main() {
    let _ = std::fs::remove_file("board.jsonl");
    let journal = Arc::new(Mutex::new(
        JsonlJournal::<Action, Board>::create("board.jsonl").expect("create journal"),
    ));
    let scope = Arc::new(FxScope::new());
    let pending = Arc::new(Mutex::new(Vec::new()));

    let store = SyncStore::new(
        BoardFeature,
        HostRuntime {
            scope: scope.clone(),
            journal: journal.clone(),
            pending: pending.clone(),
        },
    );

    // —— 订阅双档：跨线程档（Send + 锁串行）/ 单线程档（免 Send，Rc 可用）——
    let ticket = store.subscribe(|s: &Board| {
        println!(
            "  [跨线程档] 任务 {} 个，庆祝过 {:?}",
            s.tasks.len(),
            s.cheers
        )
    });
    let local_log = Rc::new(RefCell::new(Vec::<u64>::new()));
    let log2 = Rc::clone(&local_log);
    let local = store.subscribe_local(move |s: &Board| {
        log2.borrow_mut().push(s.next_id);
    });
    assert_eq!(
        store.subscriber_count(),
        1,
        "subscriber_count 只数跨线程档，不含 subscribe_local"
    );

    // 无 await 的同步段：dispatch 恒在注册线程 → 本地订阅确定收到这三拍
    journaled_dispatch(&store, &journal, Action::Add { title: "写周报".into() });
    journaled_dispatch(&store, &journal, Action::Add { title: "交房租".into() });
    journaled_dispatch(
        &store,
        &journal,
        Action::Move {
            id: 0,
            to: Column::Done,
        },
    );
    assert_eq!(local_log.borrow().len(), 3, "本地订阅收到注册线程的三拍");

    // 效果任务在途——join 全部在途回灌（DoneNotified 那拍从 tokio worker 线程进来）
    let handles: Vec<_> = pending.lock().expect("pending poisoned").drain(..).collect();
    for h in handles {
        h.join().await.expect("effect task ok");
    }

    let snap = store.snapshot();
    assert_eq!(snap.cheers, vec![0], "回灌闭环：DoneNotified 已入状态");
    assert_eq!(
        snap.tasks[0].col,
        Column::Done,
        "Move 的迁移照常生效"
    );

    // flush 点 = 持久点：读侧断言/停机前显式冲刷
    journal.lock().expect("journal poisoned").flush().expect("flush");
    let lines = journal
        .lock()
        .expect("journal poisoned")
        .replay()
        .expect("replay")
        .count();
    assert_eq!(lines, 4, "账本=真相源：3 外部动作 + 1 回灌动作");

    // 票据 Drop 即注销：订阅计数交叉查证
    drop(local);
    assert_eq!(store.subscriber_count(), 1);
    drop(ticket);
    assert_eq!(store.subscriber_count(), 0);

    // —— 停机拼装序：fx 拒新 → store 闭门 → 等净 → 账本 flush ——
    scope.close();
    store.close();

    // 闭门后入口对照：dispatch 静默 no-op（事后从计数读出），sender 当场返 false
    store.dispatch(Action::Clear);
    assert_eq!(store.rejected_after_close(), 1, "no-op 有计数留痕");
    let sent = store.sender().dispatch(Action::Clear);
    assert!(!sent, "sender 显式 false（#[must_use] 兜底）");
    assert_eq!(store.rejected_after_close(), 2, "两入口同一拒绝计数");

    scope.shutdown().await;
    expect_idle(&scope);
    journal.lock().expect("journal poisoned").flush().expect("flush");
    drop(store); // append 源归零后重开账本

    // —— 恢复会话：重开 + 重放（换 SilentFx：通知不再外发）——
    let j = JsonlJournal::<Action, Board>::open("board.jsonl", 1).expect("open journal"); // `1`=格式版本锚（与创建时一致）
    let store2 = SyncStore::new(BoardFeature, SilentFx);
    let actions: Vec<Action> = j
        .replay()
        .expect("replay")
        .map(|r| r.expect("corrupt journal line"))
        .collect();
    let replayed = actions.len();
    for a in actions {
        store2.dispatch(a); // 重放走 dispatch，禁重复入账
    }
    let snap2 = store2.snapshot();
    assert_eq!(*snap2, *snap, "重放终态与 live 收敛");
    println!("恢复会话：重放 {replayed} 条，终态与 live 一致 → {:?}", *snap2);

    store2.close();
    let _ = std::fs::remove_file("board.jsonl");
}
```

预期输出（4 条订阅通知 = 3 外部动作 + 1 回灌动作；每处理一个 action 恰一帧）：

```text
  [跨线程档] 任务 1 个，庆祝过 []
  [跨线程档] 任务 2 个，庆祝过 []
  [跨线程档] 任务 2 个，庆祝过 []
  [跨线程档] 任务 2 个，庆祝过 [0]
恢复会话：重放 4 条，终态与 live 一致 → Board { next_id: 2, tasks: [Task { id: 0, title: "写周报", col: Done }, Task { id: 1, title: "交房租", col: Todo }], cheers: [0] }
```

### 故意踩坑（各跑一次，报错为真实输出）

**坑 D：同步 `main` 里 `FxScope::spawn`**——没有 tokio reactor，当场 panic（报错信息
指路清晰，但第一天要先知道入口在 `#[tokio::main]`）：

```text
thread 'main' panicked at …/skeleton-infra/src/fx.rs:
there is no reactor running, must be called from the context of a Tokio 1.x runtime
```

**这步教你什么契约**：①核心只转发、永不解释——`Fx` 词表长什么样、任务怎么跑、
回灌前先不先落盘，全是宿主（解释器）的事；②「先落盘后 apply」在回灌挂点同样成立，
append 失败禁回灌（fail-closed 禁静默丢）；③订阅双档的计数口径与线程亲和；
④停机拼装序与闭门后的双入口语义。

## 第 4 步（5 分钟）：combine 父子分片——宏一行装配

程序长大就该分片：把「归档」从父域里拆出去，子 feature 只看得见 `DoneState` 与
`DoneAct`。`combine!` 宏一行生成镜头/路由/克隆上提；父层用 `Effect::Emit` 派生动作
（核心折叠回灌，不经解释器），完成任务自动归档。

`src/main.rs` 整体替换为：

```rust
use skeleton::{Effect, EffectBuf, Infallible, NoFx, Reducer, Store as _};
use skeleton_infra::runtime::SyncStore;

#[derive(Clone, Debug, PartialEq)]
struct Task {
    id: u64,
    title: String,
}

/// 父状态：工作区 = 待办流 + 归档子域（子切片 `done` 交给子 feature 独裁）
#[derive(Clone, Debug, Default, PartialEq)]
struct Workspace {
    next_id: u64,
    todo: Vec<Task>,
    done: DoneState,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct DoneState {
    archived: Vec<String>,
}

/// 父动作词表：`Done(..)` 路由给子分片；`Ping` 父层自理
#[derive(Clone, Debug, PartialEq)]
enum Act {
    Add { title: String },
    Complete { id: u64 },
    Done(DoneAct),
    Ping,
}

/// 子动作词表：归档域自治
#[derive(Clone, Debug, PartialEq)]
enum DoneAct {
    Archive { title: String },
    Clear,
}

/// 子 feature：只看得见 DoneState 与 DoneAct（自治边界）
struct DoneFeature;

impl Reducer for DoneFeature {
    type State = DoneState;
    type Action = DoneAct;
    type Op = Infallible;

    fn initial(&self) -> DoneState {
        DoneState::default()
    }

    fn reduce(
        &self,
        s: &DoneState,
        a: DoneAct,
        _out: &mut EffectBuf<DoneAct, Infallible>,
    ) -> DoneState {
        match a {
            DoneAct::Archive { title } => {
                let mut next = s.clone();
                next.archived.push(title);
                next
            }
            DoneAct::Clear => DoneState { archived: Vec::new() },
        }
    }
}

/// 一行装配：镜头 Workspace.done + 路由 Act::Done。unwrap 自动自 &Act 克隆上浮
///（要求 DoneAct: Clone）；lift 省略 = 零效果上提（子 Op 是 Infallible，正好）。
/// 产物是状态分片不是根：initial 恒 panic，根状态由 WorkspaceFeature 显式构造。
fn done_fragment() -> impl Reducer<State = Workspace, Action = Act, Op = Infallible> {
    skeleton_infra::combine!(DoneFeature, Workspace.done, Act::Done)
}

struct WorkspaceFeature;

impl Reducer for WorkspaceFeature {
    type State = Workspace;
    type Action = Act;
    type Op = Infallible;

    fn initial(&self) -> Workspace {
        Workspace {
            done: DoneFeature.initial(), // 子初值由根显式构造
            ..Workspace::default()
        }
    }

    fn reduce(&self, s: &Workspace, a: Act, out: &mut EffectBuf<Act, Infallible>) -> Workspace {
        match a {
            // 子路由动作整支委托分片（分片内部做镜头读写与路由）
            Act::Done(_) => done_fragment().reduce(s, a, out),
            Act::Ping => s.clone(), // 未路由：本层自理
            Act::Add { title } => {
                let mut next = s.clone();
                next.todo.push(Task {
                    id: s.next_id,
                    title,
                });
                next.next_id += 1;
                next
            }
            Act::Complete { id } => {
                // 任务完成：移出待办 + Emit 派生动作（核心折叠回灌 → 子分片归档）
                let mut next = s.clone();
                if let Some(pos) = next.todo.iter().position(|t| t.id == id) {
                    let task = next.todo.remove(pos);
                    out.push(Effect::Emit(Act::Done(DoneAct::Archive {
                        title: task.title,
                    })));
                }
                next
            }
        }
    }
}

fn main() {
    let store = SyncStore::new(WorkspaceFeature, NoFx);
    store.dispatch(Act::Add { title: "写周报".into() });
    store.dispatch(Act::Complete { id: 0 });

    let snap = store.snapshot();
    assert!(snap.todo.is_empty(), "任务已移出待办");
    assert_eq!(
        snap.done.archived,
        vec!["写周报".to_string()],
        "Emit 派生动作经回灌进了子分片"
    );

    store.dispatch(Act::Done(DoneAct::Clear)); // 子路由直呼：归档清空
    store.dispatch(Act::Ping); // 父层自理：整状态原样
    assert!(store.snapshot().done.archived.is_empty());
    println!("终态：{:?}", *store.snapshot());
    store.close();
}
```

预期输出：

```text
终态：Workspace { next_id: 1, todo: [], done: DoneState { archived: [] } }
```

**这步教你什么契约**：①`combine!` 产物是**状态分片不是根**——`initial` 恒 panic，
根状态（含子初值）由父 Feature 显式构造；②Emit 派生动作由核心折叠回灌（不进解释器、
不入账本——它是已落盘动作的确定性派生）；③零 dyn 零装箱，组合税个位数 ns。

### 故意踩坑（真实编译器输出）

**坑 E：unit variant 构造器不收**。想把 `Act::Ping`（unit variant）路由给一个动作词表为
`()` 的子 feature，函数形态传裸构造器会得到：

```text
error[E0277]: expected a `Fn(_)` closure, found `Act`
  --> wrap 传参处
   = help: the trait `Fn(_)` is not implemented for `Act`
```

宏短形态换了个姿势拒（它要生成 `Act::Ping(c)` 解构，unit variant 没有内层）：

```text
error[E0532]: expected tuple struct or tuple variant, found unit variant `Act::Ping`
```

**修复 = 显式闭包**（把「零参 fn」写成「收一个 `()`」）：

```rust
combine(
    |w: &Workspace| &w.done,
    |w: &mut Workspace, d: DoneState| w.done = d,
    |_: ()| Act::Ping, // ← 显式闭包：收一个 ()，产出父动作
    |a: &Act| match a {
        Act::Ping => Some(()),
        _ => None,
    },
    TickChild,
    |_, _| {},
)
```

（真实场景里更常见的是 tuple variant 路由——`Act::Done` 构造器是 `fn(DoneAct) -> Act`，
天然满足 `Fn(DoneAct) -> Act`，宏短形态直接收。）

## 第 5 步（4 分钟）：TestStore 逐动作断言

逻辑层验收不需要真解释器：`TestStore` 内置 `CollectRuntime`（效果原样回收、永不
解释），逐动作断言状态与效果。在第 3 步的域代码末尾追加测试模块（或存成独立文件），
`cargo test --bin kanban` 运行：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use skeleton_infra::testing::{ExpectedEffect, TestStore};

    #[test]
    fn add_moves_state_without_effects() {
        let mut ts = TestStore::new(BoardFeature);
        ts.dispatch(Action::Add { title: "写周报".into() });
        assert_eq!(ts.state().tasks.len(), 1);
        assert_eq!(ts.state().tasks[0].col, Column::Todo);
    }

    #[test]
    fn move_to_done_emits_exactly_one_effect() {
        let mut ts = TestStore::new(BoardFeature);
        ts.dispatch(Action::Add { title: "写周报".into() });
        ts.dispatch(Action::Move {
            id: 0,
            to: Column::Done,
        });
        // 逐动作断言：Move 必须恰好产出这一条描述式效果（id: None = 不校验寻址）
        ts.receive(ExpectedEffect::Run {
            id: None,
            op: Fx::DoneReached {
                id: 0,
                title: "写周报".into(),
            },
        });
        assert_eq!(ts.state().tasks[0].col, Column::Done);
    }

    #[test]
    fn reentry_loop_closes_in_state() {
        let mut ts = TestStore::new(BoardFeature);
        ts.dispatch(Action::Add { title: "写周报".into() });
        ts.dispatch(Action::Move {
            id: 0,
            to: Column::Done,
        });
        ts.receive(ExpectedEffect::Run {
            id: None,
            op: Fx::DoneReached {
                id: 0,
                title: "写周报".into(),
            },
        });
        // 回环语义：解释器回灌的动作直接 dispatch，用 state() 断言（不进 receive 队列）
        ts.dispatch(Action::DoneNotified { id: 0 });
        assert_eq!(ts.state().cheers, vec![0]);
    }
}
```

预期输出：

```text
running 3 tests
...
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

### 故意踩坑（真实 panic 文本）

**坑 F：`receive` 误用于零效果动作**。`Add` 不产效果，对它 `receive` 会拿到指路报错：

```text
receive: no effect produced (queue empty); expected Run { id: None, op: DoneReached { id: 0, title: "写周报" } }
```

**这步教你什么契约**：reducer 是纯函数 → 全部业务行为可以在零 IO 的替身上逐动作
钉死；效果是值 → `ExpectedEffect` 直接比对该值；回环断言的形状是 dispatch + state()
（Emit/回灌不进 receive 队列）。

## 收尾

- 对称性设计速查（双词表/双订阅/双入口/双形态/计数口径）：[cheatsheet.md](cheatsheet.md)
- 全键 API：[../reference.md](../reference.md)；feature 矩阵见根 [README](../../README.md)。
- 本教程的看板域是教学最小件：真实项目把 `Fx` 换成你的副作用词表、把 `JsonlJournal`
  换成任何 `JournalPort` 实现即可——端口形状不变。
