// 由 view_constants_gen 自 manifest.toml 生成（ADR 0006 M3）——禁手改；清单为唯一真源。
// CI 断言：cargo run -p udf-skeleton-infra --example view_constants_gen -- --check
export const PROTOCOL_VERSION = 1 as const;
export const HEADER_LEN = 32 as const;
export const ENTRY_LEN = 24 as const;
export const SEG_ALIGN = 8 as const;
export const TLV_HEADER_LEN = 5 as const;
export const INITIAL_SEQ = 2 as const;
export const SEQ_STEP = 2 as const;
export const OFFSET_MAGIC = 0 as const;
export const OFFSET_PROTOCOL = 4 as const;
export const OFFSET_FLAGS = 6 as const;
export const OFFSET_PUBLISH_SEQ = 8 as const;
export const OFFSET_REGION_ID = 16 as const;
export const OFFSET_BINDING_COUNT = 20 as const;
export const OFFSET_INDEX_OFFSET = 24 as const;
export const INDEX_OFFSET_V1 = 32 as const;
export const ENTRY_OFFSET_BINDING_ID = 0 as const;
export const ENTRY_OFFSET_KIND = 4 as const;
export const ENTRY_OFFSET_OFFSET = 8 as const;
export const ENTRY_OFFSET_LEN = 16 as const;
export const ENTRY_OFFSET_GEN = 20 as const;
export const PATH_COUNT = "/count" as const;
export const KIND_COUNT = 1 as const;
export const PATH_DERIVED_REMAINING = "/derived/remaining" as const;
export const KIND_DERIVED_REMAINING = 2 as const;
export const FIELD_REMAINING = "remaining" as const;
