"""Calls the user's LLM gateway (any OpenAI-compatible /chat/completions endpoint) and validates the reply.
Any failure raises LLMError; the caller then keeps the rule-based decision."""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.request
from pathlib import Path

from ops.config import SEVERITIES

PROMPT = Path(__file__).resolve().parent / "prompts" / "decide.md"


class LLMError(RuntimeError):
    pass


def _post(url: str, body: dict, api_key: str, timeout: float) -> dict:
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    request = urllib.request.Request(url, data=json.dumps(body).encode(), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise LLMError(f"gateway returned {exc.code}: {exc.read()[:300].decode(errors='replace')}") from None
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        raise LLMError(f"gateway request failed: {exc}") from None


def _parse(content: str) -> dict:
    content = content.strip()
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", content, re.S)
    if fenced:
        content = fenced.group(1)
    start, end = content.find("{"), content.rfind("}")
    if start < 0 or end < start:
        raise LLMError("reply contains no JSON object")
    try:
        return json.loads(content[start : end + 1])
    except json.JSONDecodeError as exc:
        raise LLMError(f"reply is not valid JSON: {exc}") from None


def validate(decision: dict, catalog: dict) -> tuple[dict, list[str]]:
    """Return a clean decision plus the names of rejected actions."""
    if decision.get("severity") not in SEVERITIES:
        raise LLMError(f"invalid severity {decision.get('severity')!r}")
    if not isinstance(decision.get("summary"), str) or not decision["summary"].strip():
        raise LLMError("missing summary")
    actions, rejected = [], []
    for action in decision.get("actions") or []:
        name = action.get("name") if isinstance(action, dict) else None
        if name in catalog and name != "notify_only":
            actions.append({"name": name, "reason": str(action.get("reason", ""))[:300]})
        elif name != "notify_only":
            rejected.append(str(name))
    confidence = decision.get("confidence")
    return {
        "severity": decision["severity"],
        "summary": decision["summary"][:500],
        "root_cause": str(decision.get("root_cause", ""))[:800],
        "actions": actions,
        "send_alert": bool(decision.get("send_alert", True)),
        "confidence": float(confidence) if isinstance(confidence, (int, float)) else None,
    }, rejected


def decide(context: dict, catalog: dict, llm: dict) -> tuple[dict, list[str]]:
    if not llm.get("gateway_url") or not llm.get("model"):
        raise LLMError("no LLM gateway configured (LLM_GATEWAY_URL / LLM_MODEL)")
    system = PROMPT.read_text().format(actions="\n".join(f"- {k}: {v}" for k, v in catalog.items()))
    body = {
        "model": llm["model"],
        "temperature": llm.get("temperature", 0),
        "max_tokens": llm.get("max_output_tokens", 800),
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(context, default=str)},
        ],
        "response_format": {"type": "json_object"},
    }
    url = f"{llm['gateway_url']}/chat/completions"
    timeout = llm.get("timeout_seconds", 30)
    try:
        reply = _post(url, body, llm.get("api_key", ""), timeout)
    except LLMError as exc:
        if "returned 400" not in str(exc):
            raise
        body.pop("response_format")  # some gateways/models do not support JSON mode
        reply = _post(url, body, llm.get("api_key", ""), timeout)
    try:
        content = reply["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        raise LLMError("unexpected gateway response shape") from None
    decision, rejected = validate(_parse(content or ""), catalog)
    decision["decided_by"] = f"llm:{reply.get('model') or llm['model']}"
    return decision, rejected
