#!/usr/bin/env python3
"""本地等价门：复跑 `.github/workflows/ci.yml` 的 feature 隔离矩阵（check / test / doc 三步）。

ci.yml 是唯一事实源：feature 列表、crate 名、三条命令与 doc 步的 RUSTDOCFLAGS 全部
现读，脚本内不硬编码——矩阵改了这里自动跟随。只认「单行 `run:` 且引用
`${{ matrix.feature }}`」的 cargo check/test/doc 步骤；多行 `run: |` 步骤（如 runtime
面的 cargo tree 断言）不在本脚本范围。

用法（仓库根）：
    python scripts/feature_matrix.py                 # 全矩阵
    python scripts/feature_matrix.py --only view     # 只跑指定 feature 的矩阵行
    python scripts/feature_matrix.py --list          # 只打印将执行的矩阵，不跑 cargo

工具链与产物目录不干预：直接调 cargo，`RUSTUP_TOOLCHAIN` / `CARGO_TARGET_DIR` 等环境
变量自然生效（本机 stable 通道有 ICE 史，用 `RUSTUP_TOOLCHAIN=1.96.0`）。
退出码：0 全绿 / 1 任一步失败 / 2 ci.yml 解析失败或 `--only` 无匹配。
"""

import argparse
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from collections import namedtuple
from pathlib import Path

CI_PATH = Path(__file__).resolve().parent.parent / ".github" / "workflows" / "ci.yml"
FEATURE_REF = "${{ matrix.feature }}"
KINDS = ("check", "test", "doc")

JOB_ID = re.compile(r"^  ([A-Za-z0-9_-]+):\s*$")
STEP_ITEM = re.compile(r"^(\s+)-\s")
FEATURE_LIST = re.compile(r"^\s+feature:\s*\[(.*)\]\s*$")
RUN_LINE = re.compile(r"^\s+run:\s*(\S.*)$")
RUSTDOCFLAGS_LINE = re.compile(r"^\s+RUSTDOCFLAGS:\s*(.+?)\s*$")

# kind = cargo 子命令；argv = 已代入 feature 的完整命令；env = 该步骤声明的环境变量。
Step = namedtuple("Step", "kind argv env")
Row = namedtuple("Row", "crate feature steps")


class CiParseError(Exception):
    """ci.yml 的形状与脚本预期不符——显式失败，而不是静默漏跑某个矩阵行。"""


def indent_of(line):
    return len(line) - len(line.lstrip(" "))


def split_jobs(text):
    """切出 `jobs:` 下每个二级缩进键（job id）及其行块。"""
    jobs = []
    job = None
    in_jobs = False
    for line in text.splitlines():
        if line == "jobs:":
            in_jobs = True
        elif in_jobs and line.strip() and not line.startswith(" "):
            in_jobs = False  # 回到顶层键：jobs 段结束
        match = JOB_ID.match(line) if in_jobs else None
        if match:
            job = (match.group(1), [])
            jobs.append(job)
        elif job is not None:
            job[1].append(line)
    if not jobs:
        raise CiParseError("未找到 jobs: 下的任何 job")
    return jobs


def split_run(run, feature, job_id):
    """先代入 feature 再分词：`${{ matrix.feature }}` 自带空格，先分词会碎成三段。"""
    command = run.replace(FEATURE_REF, feature)
    if "${{" in command:
        raise CiParseError(f"job {job_id}: 命令含 feature 之外的矩阵表达式，脚本不展开: {command!r}")
    try:
        return shlex.split(command)
    except ValueError as exc:
        raise CiParseError(f"job {job_id}: run 行引号不成对: {command!r} ({exc})")


def parse_step(block, job_id):
    """单个步骤块 → (kind, 命令模板, env, crate)；非矩阵 cargo 步骤返回 None。"""
    run = None
    env = {}
    for line in block:
        match = RUN_LINE.match(line)
        if match:
            run = match.group(1)
        match = RUSTDOCFLAGS_LINE.match(line)
        if match:
            env["RUSTDOCFLAGS"] = match.group(1)
    if run is None or run[0] in "|>" or FEATURE_REF not in run:
        return None  # 无 run / 多行 run / 不随矩阵变化的 run
    argv = split_run(run, "<feature>", job_id)  # 占位值只为校验命令形状
    if len(argv) < 2 or argv[0] != "cargo" or argv[1] not in KINDS:
        raise CiParseError(f"job {job_id}: 只认单行 cargo check/test/doc，实得: {run!r}")
    if "-p" not in argv or argv.index("-p") + 1 == len(argv):
        raise CiParseError(f"job {job_id}: 缺 -p <crate>: {run!r}")
    return argv[1], run, env, argv[argv.index("-p") + 1]


def parse_steps(lines, job_id):
    """job 内每个列表项（步骤）逐块解析。"""
    steps = []
    i = 0
    while i < len(lines):
        match = STEP_ITEM.match(lines[i])
        if not match:
            i += 1
            continue
        item_indent = len(match.group(1))
        block = []
        i += 1
        while i < len(lines) and (not lines[i].strip() or indent_of(lines[i]) > item_indent):
            block.append(lines[i])
            i += 1
        step = parse_step(block, job_id)
        if step:
            steps.append(step)
    return steps


def parse_ci(text):
    """带 `feature: [...]` 矩阵的 job → 每个 feature 一行 Row（步骤顺序照 ci.yml）。"""
    rows = []
    for job_id, lines in split_jobs(text):
        feature_lists = [match.group(1) for match in map(FEATURE_LIST.match, lines) if match]
        if not feature_lists:
            continue  # 无 feature 矩阵的 job 不在本脚本范围
        if len(feature_lists) > 1:
            raise CiParseError(f"job {job_id}: 出现多个 feature: 列表，脚本不合并")
        steps = parse_steps(lines, job_id)
        if not steps:
            raise CiParseError(f"job {job_id}: 有 feature 矩阵，但没有随矩阵变化的单行 cargo 步骤")
        crates = {crate for _, _, _, crate in steps}
        if len(crates) != 1:
            raise CiParseError(f"job {job_id}: 同一 job 的步骤指向多个 crate: {sorted(crates)}")
        crate = crates.pop()
        for feature in (f.strip(" '\"") for f in feature_lists[0].split(",")):
            commands = [Step(kind, split_run(run, feature, job_id), env) for kind, run, env, _ in steps]
            rows.append(Row(crate, feature, commands))
    if not rows:
        raise CiParseError("ci.yml 里没有带 feature 矩阵的 job")
    return rows


def describe(step):
    """--list 用的可读命令：环境变量赋值前缀 + argv。"""
    assignments = [f"{key}={value}" for key, value in step.env.items()]
    return " ".join(assignments + step.argv)


def list_rows(rows):
    for row in rows:
        print(f"{row.crate}  {row.feature}")
        for step in row.steps:
            print(f"    {step.kind:<5} {describe(step)}")
    print(f"\n{len(rows)} matrix rows, {sum(len(row.steps) for row in rows)} commands")


def run_rows(rows):
    """逐条执行（顺序照 ci.yml），返回 {(行号, 步骤): 是否 exit 0}；失败不中断，跑完再汇总。"""
    if shutil.which("cargo") is None:
        print("feature_matrix: cargo 不在 PATH 上", file=sys.stderr)
        sys.exit(2)
    outcomes = {}
    total = sum(len(row.steps) for row in rows)
    done = 0
    for index, row in enumerate(rows):
        for step in row.steps:
            done += 1
            print(f"[{done}/{total}] {row.crate} {row.feature} {step.kind}", flush=True)
            started = time.monotonic()
            code = subprocess.run(step.argv, env=dict(os.environ, **step.env)).returncode
            outcomes[(index, step.kind)] = code == 0
            status = "ok" if code == 0 else "FAIL"
            print(f"[{done}/{total}] {status} exit={code} {time.monotonic() - started:.1f}s", flush=True)
    return outcomes


def cell(ok):
    if ok is None:
        return "-"  # ci.yml 该 job 没有这一步
    return "ok" if ok else "FAIL"


def print_table(rows, outcomes):
    crate_width = max(len(row.crate) for row in rows)
    feature_width = max(len(row.feature) for row in rows)
    print(f"\n{'crate':<{crate_width}}  {'feature':<{feature_width}}  check  test  doc")
    for index, row in enumerate(rows):
        cells = [f"{cell(outcomes.get((index, kind))):<5}" for kind in KINDS]
        print(f"{row.crate:<{crate_width}}  {row.feature:<{feature_width}}  {cells[0]}  {cells[1]}  {cells[2]}")


def main():
    parser = argparse.ArgumentParser(
        description="本地等价门：复跑 ci.yml 的 feature 隔离矩阵（check / test / doc）。"
    )
    parser.add_argument("--only", metavar="FEATURE", help="只跑该 feature 的矩阵行")
    parser.add_argument("--list", action="store_true", help="只打印将执行的矩阵，不跑 cargo")
    args = parser.parse_args()
    try:
        rows = parse_ci(CI_PATH.read_text(encoding="utf-8"))
        if args.only:
            rows = [row for row in rows if row.feature == args.only]
            if not rows:
                raise CiParseError(f"--only {args.only}: ci.yml 的 feature 矩阵里没有这个 feature")
    except (CiParseError, OSError) as exc:
        print(f"feature_matrix: {exc}", file=sys.stderr)
        return 2
    if args.list:
        list_rows(rows)
        return 0
    outcomes = run_rows(rows)
    print_table(rows, outcomes)
    return 0 if all(outcomes.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
