"""AI ops command line (also reachable as `python scripts/pipeline.py ops ...`).

    python -m ops check [--mode advise|act] [--trigger EVENT]   # exit 0 healthy, 1 incidents, 2 error
    python -m ops explain                                       # show snapshot + decision; no alert, no action
    python -m ops test-alert                                    # send a test message to ALERT_WEBHOOK_URL
"""

from __future__ import annotations

import argparse
import json
import sys

import yaml

from ops import actions, alerts, audit, config, llm, rules, signals
from ops.redact import redact


def decide(cfg: dict, snap: dict, found: list[dict], trigger: str | None) -> tuple[dict, list[str], str | None]:
    """Rules first; the LLM refines when configured. Returns decision, rejected actions, LLM error."""
    baseline = rules.baseline_decision(found)
    if not found:
        return baseline, [], None
    context = {"trigger": trigger, "snapshot": redact(snap), "incidents": redact(found), "baseline_decision": baseline}
    try:
        decision, rejected = llm.decide(context, rules.CATALOG, cfg["llm"])
    except llm.LLMError as exc:
        return baseline, [], str(exc)
    # Guardrail: a critical incident is never silenced or downgraded below warning by the LLM.
    if any(i["severity"] == "critical" for i in found):
        decision["send_alert"] = True
        if decision["severity"] == "info":
            decision["severity"] = "warning"
    return decision, rejected, None


def check(mode: str | None, trigger: str | None, dry_run: bool = False) -> int:
    cfg = config.load()
    mode = mode or cfg["mode"]
    if mode == "off":
        print("ai ops is off (config/ops.yml or AI_OPS_MODE)")
        return 0
    project = yaml.safe_load((config.ROOT / "config" / "pipeline.yml").read_text())["project"]
    snap = signals.snapshot()
    found = rules.incidents(snap, cfg["thresholds"])
    decision, rejected, llm_error = decide(cfg, snap, found, trigger)
    if dry_run:
        print(json.dumps(redact({"mode": mode, "snapshot": snap, "incidents": found, "decision": decision,
                                 "rejected_actions": rejected, "llm_error": llm_error}), indent=2, default=str))
        return 1 if found else 0
    results = actions.execute(decision["actions"], cfg["actions"], mode) if found else []
    try:
        alert = alerts.notify(cfg, project, decision, found, results)
    except Exception as exc:  # alerting must never break the check
        alert = f"failed: {exc}"
    audit.write(redact({"mode": mode, "trigger": trigger, "snapshot": snap, "incidents": found,
                        "decision": decision, "rejected_actions": rejected, "llm_error": llm_error,
                        "actions": results, "alert": alert}))
    status = "healthy" if not found else f"{decision['severity']}: {decision['summary']}"
    print(f"[{mode}] {status} | decided by {decision['decided_by']} | alert: {alert}")
    for r in results:
        print(f"  action {r['name']}: {r['outcome']}")
    if llm_error:
        print(f"  llm: {llm_error} (rules decided)")
    return 1 if found else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ops", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p_check = sub.add_parser("check")
    p_check.add_argument("--mode", choices=["advise", "act"])
    p_check.add_argument("--trigger", help="what caused this check, e.g. schedule, run_failed")
    sub.add_parser("explain")
    sub.add_parser("test-alert")
    args = parser.parse_args(argv)
    try:
        if args.command == "check":
            return check(args.mode, args.trigger)
        if args.command == "explain":
            return check(None, "explain", dry_run=True)
        cfg = config.load()
        if not cfg["alerts"]["webhook_url"]:
            print("ALERT_WEBHOOK_URL is not set", file=sys.stderr)
            return 2
        alerts.post(cfg["alerts"]["webhook_url"], {"text": ":wave: ai ops test alert"})
        print("sent")
        return 0
    except Exception as exc:
        print(f"ops error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
