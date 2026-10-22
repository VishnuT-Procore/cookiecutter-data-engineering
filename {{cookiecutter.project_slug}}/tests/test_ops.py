"""AI ops: rules, LLM gateway contract, guardrails, alerting and redaction (no infrastructure needed).

A fake OpenAI-compatible gateway and a fake Slack webhook run on localhost; health signals and
actions are stubbed, so these tests exercise exactly the decision and safety logic.
"""

from __future__ import annotations

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from ops import actions, alerts, audit, cli, signals  # noqa: E402

HEALTHY = {
    "landing_job": {"name": "p-landing", "reachable": True, "state": "RUNNING",
                    "checkpoints": {"last_completed_age_seconds": 5}},
    "kafka": {"total": 0},
    "transform": {"found": True, "age_seconds": 60, "problems": []},
}
LANDING_DOWN = {**HEALTHY, "landing_job": {"name": "p-landing", "reachable": True, "state": "CANCELED",
                                           "last_exception": "IOException: connect failed password=hunter2 "
                                                             "Authorization: Bearer abc.def.ghi"}}


class FakeServer:
    """Minimal HTTP server that records JSON requests and answers from a queue."""

    def __init__(self):
        self.requests: list[dict] = []
        self.replies: list[tuple[int, object]] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])) or b"{}")
                server.requests.append({"path": self.path, "headers": dict(self.headers), "body": body})
                status, reply = server.replies.pop(0) if server.replies else (200, {})
                data = reply.encode() if isinstance(reply, str) else json.dumps(reply).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *args):
                pass

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()


def llm_reply(decision: dict | str) -> tuple[int, dict]:
    content = decision if isinstance(decision, str) else json.dumps(decision)
    return 200, {"model": "fake-model", "choices": [{"message": {"role": "assistant", "content": content}}]}


@pytest.fixture
def env(tmp_path, monkeypatch):
    gateway, webhook = FakeServer(), FakeServer()
    monkeypatch.setattr(actions, "STATE_DIR", tmp_path)
    monkeypatch.setattr(actions, "STATE", tmp_path / "state.json")
    monkeypatch.setattr(audit, "STATE_DIR", tmp_path)
    monkeypatch.setenv("LLM_GATEWAY_URL", gateway.url + "/v1")
    monkeypatch.setenv("LLM_API_KEY", "sk-test")
    monkeypatch.setenv("LLM_MODEL", "fake-model")
    monkeypatch.setenv("ALERT_WEBHOOK_URL", webhook.url + "/hook")
    monkeypatch.setenv("AI_OPS_MODE", "advise")
    calls: list[str] = []

    def fake_runner(name):
        def run():
            calls.append(name)
            return subprocess.CompletedProcess([name], 0, "ok", "")
        return run

    monkeypatch.setattr(actions, "RUNNERS", {n: fake_runner(n) for n in actions.RUNNERS})
    state = {"snapshot": HEALTHY}
    monkeypatch.setattr(signals, "snapshot", lambda: state["snapshot"])
    yield {"gateway": gateway, "webhook": webhook, "calls": calls, "state": state, "dir": tmp_path}
    gateway.close()
    webhook.close()


REDEPLOY = {"severity": "critical", "summary": "Landing job stopped; no new data lands.",
            "root_cause": "Job was cancelled after a connection failure.",
            "actions": [{"name": "redeploy_landing", "reason": "restart ingestion"}],
            "send_alert": True, "confidence": 0.9}


def test_healthy_check_needs_no_llm_and_sends_nothing(env):
    assert cli.check(None, "test") == 0
    assert env["gateway"].requests == [] and env["webhook"].requests == []


def test_advise_mode_alerts_with_recommendation_but_never_acts(env):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(llm_reply(REDEPLOY))
    assert cli.check(None, "test") == 1
    assert env["calls"] == []
    request = env["gateway"].requests[0]
    assert request["path"] == "/v1/chat/completions"
    assert request["headers"]["Authorization"] == "Bearer sk-test"
    assert request["body"]["model"] == "fake-model"
    text = env["webhook"].requests[0]["body"]["text"]
    assert "[CRITICAL]" in text and "`redeploy_landing`: recommended" in text
    assert "llm:fake-model" in json.dumps(env["webhook"].requests[0]["body"])


def test_act_mode_runs_action_then_respects_cooldown(env, monkeypatch):
    monkeypatch.setenv("AI_OPS_MODE", "act")
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies += [llm_reply(REDEPLOY), llm_reply(REDEPLOY)]
    cli.check(None, "test")
    assert env["calls"] == ["redeploy_landing"]
    assert "`redeploy_landing`: succeeded" in env["webhook"].requests[0]["body"]["text"]
    cli.check(None, "test")  # still down: cooldown blocks a second redeploy, duplicate alert suppressed
    assert env["calls"] == ["redeploy_landing"]
    assert len(env["webhook"].requests) == 1


def test_action_outside_allowlist_is_never_executed(env, monkeypatch):
    monkeypatch.setenv("AI_OPS_MODE", "act")
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(llm_reply({**REDEPLOY, "actions": [{"name": "drop_table", "reason": "?"}]}))
    cli.check(None, "test")
    assert env["calls"] == []
    record = json.loads(next((env["dir"] / "audit").glob("*.jsonl")).read_text().splitlines()[-1])
    assert record["rejected_actions"] == ["drop_table"]


@pytest.mark.parametrize("reply", [llm_reply("I think you should restart it."), (500, {"error": "boom"})])
def test_bad_or_failing_gateway_falls_back_to_rules(env, reply):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(reply)
    assert cli.check(None, "test") == 1
    body = json.dumps(env["webhook"].requests[0]["body"])
    assert "decided by rules" in body and "landing_down" in body


def test_gateway_without_json_mode_is_retried_without_it(env):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies += [(400, {"error": "response_format not supported"}), llm_reply(REDEPLOY)]
    cli.check(None, "test")
    assert "response_format" in env["gateway"].requests[0]["body"]
    assert "response_format" not in env["gateway"].requests[1]["body"]
    assert "llm:fake-model" in json.dumps(env["webhook"].requests[0]["body"])


def test_llm_cannot_silence_a_critical_incident(env):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(llm_reply({**REDEPLOY, "severity": "info", "send_alert": False, "actions": []}))
    cli.check(None, "test")
    assert "[WARNING]" in env["webhook"].requests[0]["body"]["text"]


def test_secrets_never_reach_the_gateway_or_the_alert(env):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(llm_reply(REDEPLOY))
    cli.check(None, "test")
    sent = json.dumps(env["gateway"].requests[0]["body"]) + json.dumps(env["webhook"].requests)
    assert "hunter2" not in sent and "abc.def.ghi" not in sent


def test_recovery_sends_resolved_once(env):
    env["state"]["snapshot"] = LANDING_DOWN
    env["gateway"].replies.append(llm_reply(REDEPLOY))
    cli.check(None, "test")
    env["state"]["snapshot"] = HEALTHY
    cli.check(None, "test")
    cli.check(None, "test")
    texts = [r["body"]["text"] for r in env["webhook"].requests]
    assert len(texts) == 2 and "[RESOLVED]" in texts[1]


def test_without_gateway_rules_decide(env, monkeypatch):
    monkeypatch.setenv("LLM_GATEWAY_URL", "")
    env["state"]["snapshot"] = {**HEALTHY, "transform": {"found": True, "age_seconds": 60, "problems": [
        {"node": "model.p.silver_orders", "status": "error", "message": "division by zero"}]}}
    monkeypatch.setenv("AI_OPS_MODE", "act")
    cli.check(None, "test")
    assert env["calls"] == ["rerun_failed_models"]
    assert env["gateway"].requests == []


def test_alert_payload_is_slack_compatible():
    body = alerts.payload("p", {"severity": "warning", "summary": "s", "decided_by": "rules"},
                          [{"code": "x", "severity": "warning"}], [])
    assert body["text"] and body["blocks"][0]["type"] == "section"
