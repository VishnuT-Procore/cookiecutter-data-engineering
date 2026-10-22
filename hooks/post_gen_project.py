"""Assemble the generated project from plugins.

Everything target- or orchestrator-specific lives in a plugin folder that mirrors the project layout:

    targets/<target>/              the transform target (dbt profile, adapter macros, docs, tests)
    orchestrators/<orchestrator>/  the orchestrator (optional)

The selected plugins are overlaid onto the project root; the others are deleted. A plugin may add
files but never overwrite one, except that `<file>.append` is appended to `<file>` (used for
requirements.txt, .env.example, ...). Finally the generator renders the files derived from
config/pipeline.yml.
"""

import shutil
import subprocess
import sys
from pathlib import Path

TARGET = "{{ cookiecutter.target }}"
ORCHESTRATOR = "{{ cookiecutter.orchestrator }}"

root = Path.cwd()


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    sys.exit(1)


def overlay(plugin_dir: Path) -> None:
    if not plugin_dir.is_dir():
        fail(f"plugin folder {plugin_dir.relative_to(root)} not found")
    for src in sorted(plugin_dir.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(plugin_dir)
        if rel.suffix == ".append":
            dst = root / rel.with_suffix("")
            if not dst.exists():
                fail(f"{plugin_dir.relative_to(root)} appends to missing file {rel.with_suffix('')}")
            with open(dst, "a") as f:
                f.write(src.read_text())
            continue
        dst = root / rel
        if dst.exists():
            fail(f"{plugin_dir.relative_to(root)} would overwrite {rel}; plugins may only add files.")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), dst)


overlay(root / "targets" / TARGET)
if ORCHESTRATOR != "none":
    overlay(root / "orchestrators" / ORCHESTRATOR)

for folder in ("targets", "orchestrators"):
    shutil.rmtree(root / folder, ignore_errors=True)

subprocess.run([sys.executable, "scripts/generate.py"], check=True)
