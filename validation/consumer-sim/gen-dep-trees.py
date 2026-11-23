"""生成 #56 Stage A 的依赖树两面证据（入库文件 dep-trees.txt）。

严格按 cwd 取树，避免标题与命令错位：
- 面 1 = sync-only  → validation/consumer-sim/workflow（CLI/workflow 消费方，默认 feature）
- 面 2 = async      → validation/consumer-sim（消费方 A，显式点亮 tokio）
"""
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parent


def tree(cwd: pathlib.Path) -> str:
    r = subprocess.run(["cargo", "+1.96.0", "tree", "-e", "normal"], cwd=cwd,
                       capture_output=True, text=True, encoding="utf-8")
    return (r.stdout + r.stderr).strip()


lines = [
    "# 依赖树两面证据（#56 Stage A）——由 `validation/consumer-sim/gen-dep-trees.py` 生成",
    "# 夹具本体入库（受版本控制路径）：fresh clone 必须能重建下列两面并复现同样的 tokio 判定。",
    "",
    "## 面 1: sync-only（`workflow/` —— CLI/workflow 型消费方，只用默认 feature）",
    "",
    "$ cd validation/consumer-sim/workflow && cargo tree -e normal",
    "",
    tree(ROOT / "workflow"),
    "",
    "## 面 1b: sync-only 依赖树里 tokio 出现次数（期望：**0** —— sync-only 面无 Tokio）",
    "",
    "```",
    str((tree(ROOT / "workflow").count("tokio"))),
    "```",
    "",
    "## 面 2: async（`validation/consumer-sim/` —— 消费方 A，显式点亮 `udf-skeleton-infra/tokio`）",
    "",
    "$ cd validation/consumer-sim && cargo tree -e normal",
    "",
    tree(ROOT),
    "",
    "## 面 2b: async 面里 tokio 出现次数（期望：>0）",
    "",
    "```",
    str((tree(ROOT)).count("tokio")),
    "```",
    "",
]

out = ROOT / "dep-trees.txt"
out.write_text("\n".join(lines), encoding="utf-8")
print(out)
print("\n".join(lines[:12]))
print("...")
print("\n".join(lines[-8:]))
