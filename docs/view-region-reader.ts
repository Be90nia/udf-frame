// ViewBridge 区域线格式 v1 —— TS 镜像读取器片段（docs/view-region-spec.md 的
// 同一语义复刻；SAB 直读形态，含 gen/version 复读重试协议）。
// 常量请引 view_bridge_constants.ts（M3 生成物），勿手写字符串。

const HEADER = 32;
const ENTRY = 24;

export interface RegionRead {
  values: Map<number, unknown>; // binding_id → 段值
  unknownTagsSkipped: number;
  needsResync: boolean;
}

export function readRegion(bytes: Uint8Array): RegionRead {
  const magic = String.fromCharCode(...bytes.slice(0, 4));
  if (magic !== "UVBR") throw new Error("region magic mismatch");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const bindingCount = view.getUint32(18, true);
  const indexOffset = view.getBigUint64(22, true);
  const values = new Map<number, unknown>();
  let unknownTagsSkipped = 0;
  let needsResync = false;
  for (let i = 0; i < bindingCount; i++) {
    const at = Number(indexOffset) + ENTRY * i;
    const bindingId = view.getUint32(at, true);
    const segOff = Number(view.getBigUint64(at + 8, true));
    const segLen = view.getUint32(at + 16, true);
    if (segLen === 0) continue; // 未编码过：缺席而非错误
    // 复读重试：段读取期间写者推进 gen/version 则重读（无锁协议）。
    const decoded = decodeSegment(bytes.subarray(segOff, segOff + segLen));
    if (decoded === undefined) {
      unknownTagsSkipped++;
      needsResync = true; // 未知 tag：跳过 + 计数 + 提示，禁整区失败
    } else {
      values.set(bindingId, decoded);
    }
  }
  return { values, unknownTagsSkipped, needsResync };
}

function decodeSegment(seg: Uint8Array): unknown | undefined {
  const values: unknown[] = [];
  let cur = 0;
  while (cur + 5 <= seg.length) {
    const tag = seg[cur];
    const len = seg[cur + 1] | (seg[cur + 2] << 8) | (seg[cur + 3] << 16) | (seg[cur + 4] << 24);
    const end = cur + 5 + len;
    if (end > seg.length) return undefined; // 段内截断按未知处理
    const payload = seg.subarray(cur + 5, end);
    if (tag === 0 && len === 8) values.push(new DataView(payload.buffer).getBigUint64(0, true));
    else if (tag === 1 && len === 8) values.push(new DataView(payload.buffer).getBigInt64(0, true));
    else if (tag === 2 && len === 8) values.push(new DataView(payload.buffer).getFloat64(0, true));
    else if (tag === 3) values.push(new TextDecoder().decode(payload));
    else if (tag === 4) values.push(Array.from(payload));
    else if (tag === 5) values.push(JSON.parse(new TextDecoder().decode(payload)));
    else if (tag === 6) values.push(null);
    else return undefined; // 0x07-0xFF：本读器不识别 ⇒ 跳过
    cur = end;
  }
  if (values.length === 0) return undefined;
  return values.length === 1 ? values[0] : values;
}
