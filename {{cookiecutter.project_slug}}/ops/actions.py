"""Executes allowlisted actions with cooldowns and hourly caps. Only called in `act` mode."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import time

from ops.config import ROOT, STATE_DIR

STATE = STATE_DIR / "state.json"


def load_state() -> dict:
    try:
        return json.loads(STATE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(state: dict) -> None:
    STATE_DIR.mkdir(exist_ok=True)
    STATE.write_text(json.dumps(state, indent=2))


def _pipeline(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(ROOT / "scripts" / "pipeline.py"), *args],
                          cwd=ROOT, capture_output=True, text=True, timeout=1800)


def _rerun_failed_models() -> subprocess.CompletedProcess:
    # dbt's result: selector compares with the previous run's artifacts; copy them before they are overwritten.
    state_dir = STATE_DIR / "dbt_state"
    state_dir.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "run_results.json"):
        shutil.copy(ROOT / "dbt" / "target" / name, state_dir / name)
    return _pipeline("transform", "--select", "result:error+", "result:fail+", "--state", str(state_dir))


RUNNERS = {
    "redeploy_landing": lambda: _pipeline("ingest", "deploy", "--replace"),
    "rerun_transform": lambda: _pipeline("transform"),
    "rerun_failed_models": _rerun_failed_models,
}


def guard(name: str, policy: dict, state: dict, now: float) -> str | None:
    """Reason the action must not run, or None if it may."""
    if name not in RUNNERS:
        return "not in the action catalog"
    if not policy.get("allowed", False):
        return "not allowed in config/ops.yml"
    history = [t for t in state.get("actions", {}).get(name, []) if now - t < 3600]
    if history and now - max(history) < policy.get("cooldown_minutes", 15) * 60:
        return f"cooldown ({policy.get('cooldown_minutes', 15)} min) active"
    if len(history) >= policy.get("max_per_hour", 1):
        return f"hourly cap ({policy.get('max_per_hour', 1)}) reached"
    return None


def execute(actions: list[dict], policies: dict, mode: str) -> list[dict]:
    state = load_state()
    results = []
    for action in actions:
        name, now = action["name"], time.time()
        if mode != "act":
            results.append({**action, "outcome": "recommended"})
            continue
        blocked = guard(name, policies.get(name, {}), state, now)
        if blocked:
            results.append({**action, "outcome": f"skipped: {blocked}"})
            continue
        state.setdefault("actions", {}).setdefault(name, []).append(now)
        save_state(state)
        try:
            proc = RUNNERS[name]()
            ok = proc.returncode == 0
            detail = (proc.stdout + proc.stderr).strip()[-500:]
        except Exception as exc:
            ok, detail = False, str(exc)
        results.append({**action, "outcome": "succeeded" if ok else "failed", "detail": detail})
    return results
