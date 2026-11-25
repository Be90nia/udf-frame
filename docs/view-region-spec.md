# View Region 线格式规范 v1（跨语言唯一权威）

- 状态：**v1 生效（pre-release 布局修订 2026-11-25，#222 §5.2 / §B）**
- 参考实现：Rust 写侧 `skeleton_infra::view::RegionChannel` + `RegionWriter`；
  读侧 `skeleton_infra::view::read::{RegionReader, decode_batch}`
- 常量真源：`docs/view-bridge-constants/manifest.toml`（手维护）→ `view_constants_gen`
  生成 TS/Python/C 三件（`--check` 只比对不写，diff 非空即红）。Rust 侧常量住
  `skeleton::view::region_layout`，与清单逐值一致（`tests/view_region_wire.rs` 咬）。
- 镜像读器：TS 片段 `docs/view-region-reader.ts`（**待按本文 §8 交接清单更新**，
  该文件归 #170 批次）；Python/C 按同一语义复刻。
- 演进纪律：Header/Index 的字段序/宽度/字节序**跨版本冻结**。任何 bootstrap 布局
  改动 = 升 `protocol_version` major + 读侧协商（§6），不靠「未知标签跳过」掩盖。

## 1. 布局

```
Region（固定容量、地址稳定、单写者=桥消费点、多读者=UI）
  Header  : 32B（见 §2 逐字节表）
  Index   : binding_count × 24B，起始 offset 恒 32（index_offset = 32）
  Arena   : 段数据追加区，段起点 8 字节对齐（填充 0）
```

Index 条目（24B，字段地址恒定）：

| 字节 | 字段 | 说明 |
|---|---|---|
| 0..4 | `binding_id` | LE `u32`，= 装配期 `bind` 注册序（封板后稳定） |
| 4..6 | `kind` | LE `u16`，绑定级载荷 schema 身份（`0` = 未声明），见 `skeleton::view::PayloadTag` |
| 6..8 | 填充 | v1 恒 0 |
| 8..16 | `offset` | LE `u64`，段在区域内的绝对字节偏移（8 字节对齐、落在 Arena） |
| 16..20 | `len` | LE `u32`，段字节数（**不含**对齐填充；`0` = 该绑定尚未发布） |
| 20..24 | `gen` | LE `u32`，该条目的**逻辑修订号**（每次发布 +1）。**不是**一致性标记：不参与无锁读判定（见 §4） |

段（Arena 内）：一段 = 若干 TLV，但**v1 写侧约定「一绑定恰一段、且恰一条 TLV」**
（§5.3）。TLV = `tag:u8 | len:u32(LE) | payload(len 字节)`。

tag 空间（只增不改、退役号不复用；与 `PayloadTag` 是两张不同的空间——本表判
「字节怎么读」，`PayloadTag` 判「形状变没变」）：

| tag | 含义 | 长度约束 |
|---|---|---|
| 0 | `u64`（LE） | 恰 8 |
| 1 | `i64`（LE） | 恰 8 |
| 2 | `f64`（IEEE754 LE） | 恰 8，**必须有限**（NaN/±Inf 拒收，§5.4） |
| 3 | UTF-8 串 | 任意，须合法 UTF-8 |
| 4 | 原始字节 | 任意 |
| 5 | 紧凑 JSON UTF-8（`json_blob`） | 任意，须合法 JSON |
| 6 | `null` | 恰 0 |
| 0x07–0x7F | 框架保留 | 读侧未知即整段失效（§7） |
| 0x80–0xFF | 采用方扩展 | 同上 |

全域字节序 **LE**；复杂嵌套一律 `json_blob`（与 `JsonEncoder::to_json` 同源取值）。

## 2. Header（32B 冻结）

| 字节 | 字段 | 规则 |
|---|---|---|
| 0..4 | magic | ASCII `UVBR` |
| 4..6 | `protocol_version`（major） | LE `u16`；bootstrap 布局代次（§6） |
| 6..8 | `flags` | v1 恒 0。非 0 ⇒ 布局不再是 v1，读侧拒收（不猜） |
| 8..16 | **publication sequence** | LE `u64`，**8 字节对齐**，SAB 原子访问（`Atomics.load/store`） |
| 16..20 | `region_id` | LE `u32`，换区握手（变化 = UI 重映射） |
| 20..24 | `binding_count` | LE `u32`，封板后恒定 |
| 24..32 | `index_offset` | LE `u64`，v1 恒 **32** |

**序列语义（v1 核心）**：

- 只有 **even** 值是已提交水位：`PatchBatch.from/to_version`、`Snapshot.version`、
  区域头序列**三面同源同值**；桥内没有第二个版本源。
- **odd** 只表示「一次写事务正在进行」，永不作为已提交版本对外暴露。
- 初始封板水位 = **2**（`INITIAL_SEQ`）；每条被消费的通知 **+2**（`SEQ_STEP`），
  即 2, 4, 6, …。pre-seal 状态为 0（未提交任何快照）。
- 序列与 `region_id` 都**禁 wrapping**：代次耗尽 ⇒ typed 失败
  （`RegionFailure::GenerationExhausted`），不得复用旧代次。

## 3. 写侧协议（eager，单写者）

一次发布 = 一个事务，事务边界是**序列**，不是单次 backing 回调：

1. **发布 odd**（`seq-1`，release store，对齐原子）：读者自此必拒收/重试。
2. 写 **段字节**（各段整段写入；一次 backing 调用）。
3. 写 **索引条目 + Header**（一次 backing 调用）：索引全部 24B 条目 + Header 的
   `0..8` 与 `16..32`。**`8..16` 序列字节由原子操作独占，禁字节拷贝覆盖**。
4. **发布 even**（新 `seq`，release store）：提交点，最后写。

约束：

- 任一步 panic/失败 ⇒ **不得**发布 even：区域留在「进行中」（odd），通道标
  unavailable，禁声称旧 region 仍有效；唯一恢复口 = 显式全量重编（`reseed_region`）。
- 事务可拆成多次 backing 调用（读者只认序列边界），因此单次 backing 回调**不必**
  原子。
- 无变更的拍也推进水位（与 JSON `to_version` 同源，跨通道撕裂检测的前提）。
- `gen` 每发布一次 +1，仅作逻辑修订号（不参与一致性判定）。
- 空批**不**写段（封板事务只有索引清零 + 头）。

## 4. 读侧协议（两个独立入口）

- **不可变字节快照**（整读拷贝，宿主已持有稳定副本）：
  `RegionReader::read_snapshot(bytes, decls)`。普通 load、单次扫描；序列必须为
  even，odd ⇒ `WriterInProgress`。
- **SAB 直读**（无锁）：`RegionReader::read_lock_free(backing, decls, retries)`。
  ① 对齐原子 **acquire load** 序列；② odd ⇒ 重试；③ 扫描 Header/Index/段；
  ④ 再 acquire load；⑤ **前后相等且为 even** 才接受（`region_id` 的变化天然被同一次
  稳定读观察到）；不等 ⇒ 重试；⑥ 重试**有界**，耗尽 ⇒ 可见的
  `RetryExhausted { attempts }`（写者忙不得把读者拖成死循环）。

读侧只消费「稳定 even」下的字节：**多字段索引撕裂不可能被接受**，因为任何撕裂都发生
在 odd 窗口内。`gen` 与 Header 序列的复读**不**构成第二套协议——判定权只在序列。

## 5. 写侧校验（`fail-visible`，禁 debug_assert 独占）

### 5.1 宽度与算术
段载荷长度、段长、`binding_count`、索引几何、对齐填充、cursor 推进、end 偏移一律
`try_from` + `checked_add/checked_mul`。超出 v1 `u32` 宽度域 ⇒
`WriterFault::PayloadTooLong`（不截断、不部分追加）。

### 5.2 段长度与对齐
`offset` 8 字节对齐、填充 0；`len` 为未填充长度。读侧据此免对齐陷阱。

### 5.3 一绑定恰一段完整 TLV（#168 / #185）
每次 `RegionEncoder::encode` 必须留下**恰好一条、结构完整**的 TLV：

- 零输出 ⇒ `WriterFault::TruncatedTlv`；
- 多条/段内残留 ⇒ `WriterFault::NotSingleTlv`；
- 定宽 tag 长度不符 ⇒ `WriterFault::BadTagLength`；
- 非法 UTF-8 / JSON ⇒ `WriterFault::BadPayload`。

空输出**不是**删除标记；v1 `len == 0` 恒表示「尚未发布」。删除语义留给未来显式表示。

### 5.4 非有限 `f64`（#153）
`f64` 只接受有限值（含 -0.0 与次正规数）。NaN/±Inf ⇒
`WriterFault::NonFiniteF64`：JSON 值模型无法表示，静默映射成 `null` 是语言边界数据
损坏。

### 5.5 容量（#141 / #142 / #172）
- **seal 时**校验 `HEADER_LEN + ENTRY_LEN × binding_count ≤ capacity`。放不进 =
  seal 失败（`HeaderIndexTooLarge`），**不是** arena-full；`region_version` 保持 0。
- Arena 耗尽：先准备**完整现值快照**并算出总编码大小，再决定是否换区；能放下 ⇒
  **一次事务**发布并推进 `region_id + 1`（版本延续）；放不下 ⇒
  `CapacityExceeded { required, capacity, region_id }`，上一份已知良好元数据与字节
  **保持有效**（未发 odd），失败在 release/debug 同口径可见。
- 禁原地增长（SAB `memory.grow` 会 detach JS 视图）；换更大背衬是宿主的显式操作。

## 6. 版本与 bootstrap（#187）

`protocol_version` 是 **bootstrap 布局 major**，不是可跳过的标签：

1. 读侧只有 `major == 支持值` 时才解释 Header/Index 偏移、`binding_count`、条目与
   载荷。
2. 不等值 ⇒ typed `UnsupportedVersion { major }`，**一个 v1 偏移都不解释**（旧读器
   绝不把未知布局当 v1 硬解）。
3. 条目级未知 `kind` / 未知 TLV `tag` 的跳过**只在 major 已支持后**适用。
4. v1 内 Header/Index 几何冻结；任何不兼容的 bootstrap 改动必须升 major 并做读器
   协商，不得宣称旧读器能跳过未知布局。
5. `region_id` 只是**代次/重映射**握手（同 major 内换区），与 `protocol_version`
   无关。

JSON 通道同规则：`decode_batch` 对缺失或不支持的 `protocol_version` 返回
`unsupported_version = true` 且**不解释任何条目**。

## 7. 未知标签与畸形数据（逐条目降级）

在 major 已支持的前提下：

1. **未知条目 `kind`** ⇒ 跳过该条目整段 + `unknown_tags_skipped += 1` +
   `needs_resync = true`；其余条目照常。`kind == 0` = 未声明（JSON 自描述语义），
   按已知处理。
2. **段内任一 TLV 不可解**（未知 tag 0x07–0xFF / TLV 头截断 / 载荷截断 /
   定宽 tag 长度不符 / 非法 UTF-8 / 非法 JSON / 非有限 f64 / `null` 载荷非空 /
   段内残留 1–4 字节）⇒ **整段失效**（不把半解数组当完整值），计数一次 + resync。
3. 每跳过一次 ⇒ 计数 + 置提示（宿主据此全量重取）。**禁 panic、禁整区 Err**。
4. 仍属**结构性损坏**（索引不可信 ⇒ typed `Err(RegionError)`，整区不采信）：
   magic 不符、`protocol_version` 不支持、`flags` 非 0、`index_offset ≠ 32`、
   索引越界/几何溢出、`binding_id` 重复、段偏移未对齐、段落进 Header/Index、
   段 end 溢出或越界。

## 8. `RegionBacking` 契约（宿主扩展点）

```rust
trait RegionBacking: Send + Sync + 'static {
    fn len(&self) -> usize;                      // 固定容量（装配期读一次，此后恒定）
    fn with_bytes_mut(&self, w: &mut dyn FnMut(&mut [u8]));  // 字节写（可多次/事务）
    fn with_bytes(&self, r: &mut dyn FnMut(&[u8]));          // 整读拷贝
    fn store_publish_seq(&self, seq: u64) -> Result<(), SeqError>;  // release，默认 Unsupported
    fn load_publish_seq(&self) -> Result<u64, SeqError>;            // acquire，默认 Unsupported
}
```

- **原子序列是必需能力**：字节拷贝（哪怕地址对齐）**不等价**于原子写。默认实现返回
  `SeqError::Unsupported`，**不降级为拷贝**。
- 序列字必须与 Header 字节 `8..16` 是**同一共享内存位置**（SAB 形 = 同一
  `BigUint64Array` 视图；进程内拷贝形 `VecBacking` 在 `store_publish_seq` 的同一临界区
  内镜像进字节，使整读拷贝读者看到同一序列）。
- 不支持该能力的 backing ⇒ **lock-free 区域模式在装配期被拒绝**：通道 unavailable，
  `region_version()` 恒 0，仅 JSON 通道可用（`region_failure()` 显形）。
- 背衬借出的切片长度必须等于 `len()` 报告的容量；不符 ⇒
  `BackingCapacityMismatch`，通道 unavailable（禁在半可信背衬上继续发布）。
- 背衬/编码器的 panic 在 ViewBridge 边界 contain 成 typed 失败 + 计数
  （`RegionFailure` / `region_failures()`），**Store 订阅保持存活**；
  任何采用方回调都**不在**桥内互斥量下执行。

## 9. 跨语言交接清单（#170：TS 读器改造）

Rust/规范已定稿的 v1；TS 读器需按以下清单一次性改写（不得新旧混用）：

1. **常量**：引 `docs/view-bridge-constants/view_bridge_constants.ts`（本次已生成
   `HEADER_LEN / ENTRY_LEN / SEG_ALIGN / TLV_HEADER_LEN / INITIAL_SEQ / SEQ_STEP /
   OFFSET_* / INDEX_OFFSET_V1 / ENTRY_OFFSET_*`），禁手写偏移字面量。
2. **偏移**：`bindingCount = getUint32(20, true)`、`indexOffset = getBigUint64(24,
   true)`（旧版是 18/22 —— 全部作废）、`regionId = getUint32(16, true)`、
   序列 = 8 字节对齐的 `getBigUint64(8, true)`。
3. **读协议**：SAB 直读用 `BigUint64Array(header, 8, 1)` 做
   `Atomics.load` → 扫描 → `Atomics.load`，两次相等且为 even 才接受；odd 或变化
   ⇒ 有界重试，耗尽返回 typed `writerBusy`；不可变拷贝用普通读并单选一次（odd ⇒
   `writerInProgress`）。
4. **版本闸**：`major != 1` ⇒ 立即 `unsupportedVersion`，不得按 v1 偏移硬解。
5. **逐条目降级**：未知 `kind` 跳过该条目；段内任一 TLV 不可解（含非有限 f64、
   `null` 长度非 0、段内残留字节）⇒ 整段失效 + 计数 + `needsResync`；
   结构损坏（magic/版本/flags/indexOffset≠32/索引越界/重复 id/未对齐/段重叠或越界）
   ⇒ 抛 typed 结构错误。禁 `JSON.parse` 未捕获。
6. **测试向量**：`skeleton-infra/tests/view_region_wire.rs` 的夹具（`region_auto` /
   `region_raw`）即跨语言黄金样本来源；TS 侧复制同一字节序列断言同一结果
   （含 odd 拒收、未知 kind 跳过、段内残留失效、非有限 f64 失效）。

## 10. 已知限制（v1）

- 段的删除/缺席**没有**线格式表示：`len == 0` 恒为「尚未发布」。
- 只有 `binding_id`（注册序），线上没有路径：读侧无法从 `binding_id` 反解声明路径，
  `kind` 校验只能按「声明集的 kind 集合」判定（与 `decode_batch` 同规则）。
- Region-only 模式的初始区域只发布空索引（水位 2）；初始**全量**载荷发布归
  #155/#190（本轮未做），首条变更后才出现段。
- `Snapshot` 与 `PatchBatch` 的生成仍可能在同一 critical section 外做采用方
  `View::clone/eq`（#140 非区域侧的后续批次）。
