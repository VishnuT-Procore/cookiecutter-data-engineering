"""Install the chosen target plugin into the generated project.

Every target lives in ``targets/<name>/`` and mirrors the project layout. The selected
plugin is overlaid onto the project root, the other plugins are discarded, and the
plugin's generator renders the entity-specific files from ``config/pipeline.yml``.
"""

import shutil
import subprocess
import sys
from pathlib import Path

TARGET = "{{ cookiecutter.target }}"

root = Path.cwd()
targets_dir = root / "targets"
plugin_dir = targets_dir / TARGET

if not plugin_dir.is_dir():
    print(f"ERROR: no plugin found for target '{TARGET}' in targets/")
    sys.exit(1)

for src in sorted(plugin_dir.rglob("*")):
    if src.is_dir():
        continue
    rel = src.relative_to(plugin_dir)
    dst = root / rel
    if dst.exists():
        print(f"ERROR: plugin '{TARGET}' would overwrite common file {rel}; plugins may only add files.")
        sys.exit(1)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), dst)

shutil.rmtree(targets_dir)

subprocess.run([sys.executable, "scripts/generate.py"], check=True)
