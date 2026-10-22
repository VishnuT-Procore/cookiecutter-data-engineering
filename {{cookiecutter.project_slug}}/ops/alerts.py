"""Slack-compatible webhook alerts with de-duplication and resolve notifications."""

from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.request

from ops.actions import load_state, save_state
from ops.config import severity_rank
from ops.redact import redact

ICON = {"info": ":information_source:", "warning": ":warning:", "critical": ":rotating_light:"}


def fingerprint(incidents: list[dict]) -> str:
    key = "|".join(sorted(f"{i['code']}:{i['severity']}" for i in incidents))
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _links() -> str:
    links = []
    if os.environ.get("FLINK_REST_URL"):
        links.append(f"<{os.environ['FLINK_REST_URL']}|Flink UI>")
    if os.environ.get("ORCHESTRATOR_URL"):
        links.append(f"<{os.environ['ORCHESTRATOR_URL']}|Orchestrator>")
    return " · ".join(links)


def payload(project: str, decision: dict, incidents: list[dict], actions: list[dict]) -> dict:
    sev = decision["severity"]
    lines = [f"{ICON.get(sev, '')} *[{sev.upper()}] {project}*: {decision['summary']}"]
    if decision.get("root_cause"):
        lines.append(f"*Likely cause:* {decision['root_cause']}")
    if incidents:
        lines.append("*Incidents:* " + ", ".join(f"`{i['code']}`" for i in incidents))
    for a in actions:
        lines.append(f"*Action* `{a['name']}`: {a['outcome']}" + (f" ({a['reason']})" if a.get("reason") else ""))
    footer = f"decided by {decision.get('decided_by', 'rules')}"
    if decision.get("confidence") is not None:
        footer += f", confidence {decision['confidence']:.2f}"
    if _links():
        footer += " · " + _links()
    text = "\n".join(redact(lines))
    return {"text": text, "blocks": [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": footer}]},
    ]}


def post(webhook_url: str, body: dict) -> None:
    request = urllib.request.Request(webhook_url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=15):
        pass


def notify(cfg: dict, project: str, decision: dict, incidents: list[dict], actions: list[dict]) -> str:
    """Send, suppress or resolve. Returns what happened (for the audit log)."""
    alerts = cfg["alerts"]
    state = load_state()
    last = state.get("last_alert")
    now = time.time()
    if not incidents:
        if last and alerts.get("notify_resolved", True):
            state.pop("last_alert", None)
            save_state(state)
            if alerts.get("webhook_url"):
                post(alerts["webhook_url"], {"text": f":white_check_mark: *[RESOLVED] {project}*: all checks pass again."})
            return "resolved"
        return "none"
    fp = fingerprint(incidents)
    if not decision.get("send_alert", True):
        return "not sent (decision)"
    if severity_rank(decision["severity"]) < severity_rank(alerts.get("min_severity", "warning")):
        return "not sent (below min_severity)"
    acted = any(a["outcome"] in ("succeeded", "failed") for a in actions)
    if last and last["fingerprint"] == fp and now - last["at"] < alerts.get("suppress_minutes", 60) * 60 and not acted:
        return "suppressed (duplicate)"
    if not alerts.get("webhook_url"):
        return "not sent (ALERT_WEBHOOK_URL not set)"
    post(alerts["webhook_url"], payload(project, decision, incidents, actions))
    state["last_alert"] = {"fingerprint": fp, "at": now}
    save_state(state)
    return "sent"
