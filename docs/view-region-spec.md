# View Region 线格式规范 v1（跨语言唯一权威）

- 状态：**v1 生效**（ADR 0006 / 规格 `docs/research/mvi-view-bridge-research.md` §3.6 落地件）
- 参考实现：Rust 读侧 `skeleton_infra::view::read::RegionReader`（`view` feature）；写侧 `skeleton_infra::view::RegionWriter` + `RegionChannel`
- 镜像读器：TS 片段 `docs/view-region-reader.ts`（Python/C 按同一语义复刻，各 ~20 行验收线）
- 信封骨架布局冻结：Header/Index 的字段序、宽度、LE 跨版本冻结——演进只允许动段/项内部（TLV 自描述可跳）、增条目、或整区换代（`protocol_version` 升代 + 新 region 形）

## 布局

```
Region（固定容量、地址稳定、单写者=桥消费点、多读者=UI）
  Header  : "UVBR"(4B) | protocol_version:u16(LE) | region_id:u32(LE) | version:u64(LE)
            | binding_count:u32(LE) | index_offset:u64(LE)   [32B；index_offset = 索引区起始，
             本版本恒为 32（索引紧随头），字段保留供未来布局演进]
  Index   : binding_count × { binding_id:u32 | kind:u16(LE) | <2B 填充> | offset:u64(LE)
            | len:u32(LE) | gen:u32(LE) }   [24B/条目；条目地址恒定；kind = 绑定级载荷
             schema 身份号（0=未声明），见 skeleton::view::PayloadTag]
  Arena   : 段数据追加区；段 = TLV：tag:u8 | len:u32(LE) | payload(len 字节)
            tag 空间（只增不改、退役号不复用；与 PayloadTag 是两张不同的空间——
            本表判「字节怎么读」，PayloadTag 判「形状变没变」）：
            0=u64(8B LE) | 1=i64(8B LE) | 2=f64(8B LE, IEEE754) | 3=utf8 | 4=bytes
            | 5=json_blob（len:u32 + 紧凑 JSON UTF-8）| 6=null
            | 0x07-0x7F 保留（框架扩展）| 0x80-0xFF 保留（采用方扩展）
```

全域字节序 **LE**；段 offset **8 字节对齐**（填充 0，读侧免对齐陷阱）；字符串 UTF-8；
复杂嵌套一律 json_blob（与 JsonEncoder 同源取值——双编码语义一致性的机制保证）。

## 写侧协议（eager，桥消费点）

变更绑定 → encode 追加进 Arena（TLV 写段）→ 更新该绑定 Index 条目
（offset/len/gen+1/kind）→ Header.version = 出站版本（**发布点，最后写**）。
`binding_id` = 装配期绑定注册序（封板保证稳定）。无变更的拍也推进 Header.version
（与 JSON 通道 `to_version` 同源同值，跨通道撕裂检测的前提）。

## 读侧协议（各语言 ~20 行）

读 Header.version → 读 Index 条目 → 按 offset/len 读段 → 复读条目 gen 与
Header.version，任一变化则重读（无锁重试，读者不被写者阻塞；wasm 需 atomics
供 u64 版本单字原子读——与 SAB 前提同列）。

## 未知标签跳过路径（M2 机制，规范强制）

① 遇未知 `protocol_version` ⇒ **不整区失败**：按条目 `kind` 与段 `tag` 逐项判定，
   能解的解、不能解的跳过；
② 遇未知段 `tag` ⇒ 按 TLV `len` **跳过该段**（长度前缀是跳过的前提）；
③ 遇未知条目 `kind` ⇒ 跳过该条目的整段（kind 判定集 = 采用方 schema 词表，
   参考实现见 `view::read::decode_batch` 的 `decls` 参数形态）；
④ 每跳过一次 ⇒ 计数 `unknown_tags_skipped` + 置 `needs_resync` 提示（宿主据此
   做全量重取）；
⑤ **禁 panic、禁整区 Err**（「解不出就崩」不是可接受行为）。
   例外：头 magic 非 "UVBR" / 字节截断 = 协议外字节，参考实现以 `Err(RegionError)`
   显形（那是结构性损坏，不是兼容期未知标签）。

## 容量策略

背衬容量 = 预留上界（宿主定，默认 64 KiB）；Arena 耗尽/水位超限 ⇒ **换新区域**：
新 region_id + 版本延续（出站版本单调不回退）+ UI 重映射握手（读侧见 region_id
变化即重映射）。**禁原地增长**（SAB memory.grow 会 detach JS 侧 ArrayBuffer 视
图——地址稳定是共享映射硬前提）。框架默认 VecBacking 形在互斥下原地重置无撕裂
窗口；宿主 SAB 形的「新背衬」换法随宿主背衬管理兑现。
