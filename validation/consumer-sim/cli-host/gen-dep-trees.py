"""生成 #56 Stage B 的两面依赖树，并**追加**到 `../dep-trees.txt`（幂等）。

- 面 3 = sync-only（默认 feature）：期望 tokio 出现次数 **0**
- 面 4 = async-enabled（`--features tokio`）：期望 tokio 出现次数 **> 0**

只替换本脚本自己的标记块（`MARKER` 起至文件末），Stage A 的既有段一字不动。
依赖树全部由 `cargo tree` 产出，禁手写。
"""

import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent
EVIDENCE = ROOT.parent / "dep-trees.txt"
MARKER = "## 面 3: Stage B sync-only"

MANIFEST = ROOT / "Cargo.toml"


def tree(*extra: str) -> str:
    result = subprocess.run(
        [
            "cargo",
            "+1.96.0",
            "tree",
            "--manifest-path",
            str(MANIFEST),
            *extra,
            "-e",
            "normal",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        raise SystemExit(result.stderr)
    return (result.stdout + result.stderr).strip()


def count_tokio(text: str) -> str:
    return str(text.count("tokio"))


sync_tree = tree()
async_tree = tree("--features", "tokio")

block = "\n".join(
    [
        MARKER + "（`cli-host/` 默认 feature —— 任务调度 CLI 宿主）",
        "",
        "$ cargo +1.96.0 tree --manifest-path validation/consumer-sim/cli-host/Cargo.toml -e normal",
        "",
        sync_tree,
        "",
        "### 面 3b: sync-only 里 tokio 出现次数（期望：**0**）",
        "",
        "```",
        count_tokio(sync_tree),
        "```",
        "",
        "## 面 4: Stage B async-enabled（`cli-host/` + `--features tokio` —— FxScope 取消面）",
        "",
        "$ cargo +1.96.0 tree --manifest-path validation/consumer-sim/cli-host/Cargo.toml --features tokio -e normal",
        "",
        async_tree,
        "",
        "### 面 4b: async-enabled 里 tokio 出现次数（期望：**> 0**）",
        "",
        "```",
        count_tokio(async_tree),
        "```",
    ]
)

existing = EVIDENCE.read_text(encoding="utf-8")
if MARKER in existing:
    existing = existing[: existing.index(MARKER)].rstrip() + "\n"
EVIDENCE.write_text(existing + "\n" + block + "\n", encoding="utf-8")
print(EVIDENCE)
print(block)
