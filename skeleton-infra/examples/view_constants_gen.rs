//! M3 命名工件生成器（ADR 0006 / 规格 §3.6 末段；零三方纯 std）：
//! 读 `docs/view-bridge-constants/manifest.toml`（手维护真源）→ 写 TS/Python/C
//! 三件常量文件（生成物入库同目录，UI 侧引用常量而非手写字符串——Tauri 式
//! 命名漂移在生成期暴露而非运行期）。
//!
//! - 默认：写生成物；
//! - `--check`：只比对不写（diff 非空 = 退出码 1——清单改了未重生成即红）。
//!
//! CI 一致性断言：`cargo run -p udf-skeleton-infra --example view_constants_gen -- --check`
use std::fs;
use std::path::Path;

const MANIFEST: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/view-bridge-constants/manifest.toml"
);

/// 最小行式清单解析（真源格式自持，不引 TOML 依赖）：`key = value` 顶层 +
/// `[[binding]]` 段；值支持数字、字符串、字符串数组。
#[derive(Default)]
struct Manifest {
    protocol_version: u8,
    bindings: Vec<Binding>,
}

#[derive(Default)]
struct Binding {
    path: String,
    kind: u16,
    fields: Vec<String>,
}

fn parse_unquote(v: &str) -> String {
    let t = v.trim();
    t.trim_matches('"').to_string()
}

fn parse_manifest(text: &str) -> Result<Manifest, String> {
    let mut m = Manifest::default();
    let mut in_binding = false;
    for (n, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        if line == "[[binding]]" {
            m.bindings.push(Binding::default());
            in_binding = true;
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            return Err(format!(
                "manifest line {}: expect `key = value`, got {raw:?}",
                n + 1
            ));
        };
        let (k, v) = (k.trim(), v.trim());
        match (in_binding, k) {
            (false, "protocol_version") => {
                m.protocol_version = v
                    .parse()
                    .map_err(|_| format!("manifest line {}: bad protocol_version", n + 1))?
            }
            (true, "path") => {
                m.bindings.last_mut().expect("binding opened").path = parse_unquote(v)
            }
            (true, "kind") => {
                m.bindings.last_mut().expect("binding opened").kind = v
                    .parse()
                    .map_err(|_| format!("manifest line {}: bad kind", n + 1))?
            }
            (true, "fields") => {
                let inner = v.trim_start_matches('[').trim_end_matches(']');
                let fields = inner
                    .split(',')
                    .map(parse_unquote)
                    .filter(|f| !f.is_empty())
                    .collect();
                m.bindings.last_mut().expect("binding opened").fields = fields;
            }
            _ => return Err(format!("manifest line {}: unexpected key `{k}`", n + 1)),
        }
    }
    Ok(m)
}

/// 路径 → UPPER_SNAKE 常量尾段："/derived/remaining" → DERIVED_REMAINING。
fn const_tail(path: &str) -> String {
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_uppercase())
        .collect::<Vec<_>>()
        .join("_")
}

fn field_consts(m: &Manifest) -> Vec<(String, String)> {
    let mut seen = Vec::new();
    for b in &m.bindings {
        for f in &b.fields {
            if !seen.iter().any(|(n, _): &(String, String)| n == f) {
                seen.push((f.clone(), f.to_uppercase()));
            }
        }
    }
    seen
}

fn render(m: &Manifest) -> (String, String, String) {
    let head = "// 由 view_constants_gen 自 manifest.toml 生成（ADR 0006 M3）——禁手改；清单为唯一真源。\n// CI 断言：cargo run -p udf-skeleton-infra --example view_constants_gen -- --check\n".to_string();
    let mut ts = format!(
        "{head}export const PROTOCOL_VERSION = {} as const;\n",
        m.protocol_version
    );
    let mut py = format!("# 由 view_constants_gen 自 manifest.toml 生成（ADR 0006 M3）——禁手改；清单为唯一真源。\nPROTOCOL_VERSION = {}\n", m.protocol_version);
    let mut c = format!("/* 由 view_constants_gen 自 manifest.toml 生成（ADR 0006 M3）——禁手改；清单为唯一真源。 */\n#define UVBR_PROTOCOL_VERSION {}\n", m.protocol_version);
    for b in &m.bindings {
        let tail = const_tail(&b.path);
        ts.push_str(&format!(
            "export const PATH_{tail} = {:?} as const;\nexport const KIND_{tail} = {} as const;\n",
            b.path, b.kind
        ));
        py.push_str(&format!(
            "PATH_{tail} = {:?}\nKIND_{tail} = {}\n",
            b.path, b.kind
        ));
        c.push_str(&format!(
            "#define UVBR_PATH_{tail} \"{}\"\n#define UVBR_KIND_{tail} {}\n",
            b.path, b.kind
        ));
    }
    for (name, upper) in field_consts(m) {
        ts.push_str(&format!(
            "export const FIELD_{upper} = {name:?} as const;\n"
        ));
        py.push_str(&format!("FIELD_{upper} = {name:?}\n"));
        c.push_str(&format!("#define UVBR_FIELD_{upper} \"{}\"\n", name));
    }
    (ts, py, c)
}

fn main() {
    let check = std::env::args().any(|a| a == "--check");
    let text = fs::read_to_string(MANIFEST).unwrap_or_else(|e| panic!("read manifest: {e}"));
    let m = parse_manifest(&text).unwrap_or_else(|e| panic!("{e}"));
    let (ts, py, c) = render(&m);
    let dir = Path::new(MANIFEST).parent().expect("manifest has parent");
    let mut dirty = false;
    for (name, body) in [
        ("view_bridge_constants.ts", ts),
        ("view_bridge_constants.py", py),
        ("view_bridge_constants.h", c),
    ] {
        let path = dir.join(name);
        match fs::read_to_string(&path) {
            Ok(existing) if existing == body => {}
            Ok(_) if check => {
                println!("--check: {name} differs from manifest (rerun generator to sync)");
                dirty = true;
            }
            _ => {
                if check {
                    println!("--check: {name} missing (rerun generator to sync)");
                    dirty = true;
                } else {
                    fs::write(&path, body).unwrap_or_else(|e| panic!("write {name}: {e}"));
                    println!("wrote {}", path.display());
                }
            }
        }
    }
    if dirty {
        std::process::exit(1);
    }
    if check {
        println!("--check ok: generated files match manifest");
    }
}
