"""Assemble the generated project from plugins.

The template keeps every option in its own folder, each mirroring the project layout:

    targets/<target>/                      the chosen target platform
    engines/<engine>/<target>/             optional streaming engine for that target
    orchestrators/<orchestrator>/common/   optional orchestrator, shared part
    orchestrators/<orchestrator>/<target>/ optional orchestrator, target-specific part

The selected folders are overlaid onto the project root in that order (a plugin may add files
but never overwrite one), unused plugins are deleted, and the target's generator renders the
files derived from config/pipeline.yml. An overlay may list paths to delete in `_remove.txt`.
"""

import shutil
import subprocess
import sys
from pathlib import Path

TARGET = "{{ cookiecutter.target }}"
STREAMING_ENGINE = "{{ cookiecutter.streaming_engine }}"
ORCHESTRATOR = "{{ cookiecutter.orchestrator }}"

root = Path.cwd()


def fail(message: str) -> None:
    print(f"ERROR: {message}")
    sys.exit(1)


def overlay(plugin_dir: Path, required: bool = True) -> None:
    if not plugin_dir.is_dir():
        if required:
            fail(f"plugin folder {plugin_dir.relative_to(root)} not found")
        return
    removals = []
    for src in sorted(plugin_dir.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(plugin_dir)
        if rel == Path("_remove.txt"):
            removals = [line.strip() for line in src.read_text().splitlines() if line.strip() and not line.startswith("#")]
            continue
        dst = root / rel
        if dst.exists():
            fail(f"{plugin_dir.relative_to(root)} would overwrite {rel}; plugins may only add files.")
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), dst)
    for rel in removals:
        path = root / rel
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


overlay(root / "targets" / TARGET)
if STREAMING_ENGINE != "none":
    overlay(root / "engines" / STREAMING_ENGINE / TARGET)
if ORCHESTRATOR != "none":
    overlay(root / "orchestrators" / ORCHESTRATOR / "common")
    overlay(root / "orchestrators" / ORCHESTRATOR / TARGET)

for folder in ("targets", "engines", "orchestrators"):
    shutil.rmtree(root / folder, ignore_errors=True)

subprocess.run([sys.executable, "scripts/generate.py"], check=True)
