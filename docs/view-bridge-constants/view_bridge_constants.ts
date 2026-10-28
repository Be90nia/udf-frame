// 由 view_constants_gen 自 manifest.toml 生成（ADR 0006 M3）——禁手改；清单为唯一真源。
// CI 断言：cargo run -p udf-skeleton-infra --example view_constants_gen -- --check
export const PROTOCOL_VERSION = 1 as const;
export const PATH_COUNT = "/count" as const;
export const KIND_COUNT = 1 as const;
export const PATH_DERIVED_REMAINING = "/derived/remaining" as const;
export const KIND_DERIVED_REMAINING = 2 as const;
export const FIELD_REMAINING = "remaining" as const;
