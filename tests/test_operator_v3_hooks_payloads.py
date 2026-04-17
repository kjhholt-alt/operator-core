"""Tests for Claude Code hook payload handling.

These fixtures mirror the shape Claude Code actually pipes to `PreToolUse` /
`PostToolUse` command hooks: a JSON blob with session_id, transcript_path,
cwd, hook_event_name, tool_name, tool_input (and tool_response for post).
We exercise the HTTP handler directly via a request-simulator so the tests
never open a real socket.
"""

from __future__ import annotations

import json
import threading
import urllib.request
from pathlib import Path

from operator_core.http_server import (
    ClaudeHookPayload,
    log_hook_activity,
    serve_http,
)
from operator_core.store import JobStore


def _pre_tool_use_payload(command: str) -> dict:
    return {
        "session_id": "sess_abc123",
        "transcript_path": "/tmp/transcript.jsonl",
        "cwd": "C:/Users/Kruz/Desktop/Projects/operator-scripts",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {
            "command": command,
            "description": "test command",
        },
    }


def _post_tool_use_payload() -> dict:
    return {
        "session_id": "sess_abc123",
        "transcript_path": "/tmp/transcript.jsonl",
        "cwd": "C:/Users/Kruz/Desktop/Projects/operator-scripts",
        "hook_event_name": "PostToolUse",
        "tool_name": "Bash",
        "tool_input": {"command": "git status"},
        "tool_response": {"stdout": "clean", "exit_code": 0},
    }


class _HttpHarness:
    """Spin up the real Operator HTTP server on an ephemeral localhost port."""

    def __init__(self, store: JobStore):
        self.server = serve_http(store, host="127.0.0.1", port=0)
        self.host, self.port = self.server.server_address
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def post(self, path: str, body: dict) -> tuple[int, dict]:
        url = f"http://{self.host}:{self.port}{path}"
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}, method="POST"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310 - localhost test
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else {}

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def _invoke(path: str, body: dict, store: JobStore) -> tuple[int, dict]:
    harness = _HttpHarness(store)
    try:
        return harness.post(path, body)
    finally:
        harness.close()


def test_pre_tool_use_denies_destructive_command(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = _pre_tool_use_payload("git reset --hard HEAD")
    status, body = _invoke("/hooks/claude/pre-tool-use", payload, store)
    assert status == 200
    assert body["permissionDecision"] == "deny"
    assert body["continue"] is False
    assert "reset" in body["permissionDecisionReason"].lower()


def test_pre_tool_use_allows_safe_command(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = _pre_tool_use_payload("npm run build")
    status, body = _invoke("/hooks/claude/pre-tool-use", payload, store)
    assert status == 200
    assert body.get("continue") is True
    assert "permissionDecision" not in body


def test_pre_tool_use_denies_force_push(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = _pre_tool_use_payload("git push origin main --force")
    status, body = _invoke("/hooks/claude/pre-tool-use", payload, store)
    assert status == 200
    assert body["permissionDecision"] == "deny"


def test_pre_tool_use_denies_drop_table(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = _pre_tool_use_payload("psql -c 'DROP TABLE users'")
    status, body = _invoke("/hooks/claude/pre-tool-use", payload, store)
    assert body["permissionDecision"] == "deny"


def test_post_tool_use_logs_and_returns_ok(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    payload = _post_tool_use_payload()
    status, body = _invoke("/hooks/claude/post-tool-use", payload, store)
    assert status == 200
    assert body["ok"] is True


def test_log_hook_activity_scrubs_secrets(tmp_path: Path):
    log_path = tmp_path / "hooks.jsonl"
    raw_body = {
        "session_id": "s1",
        "hook_event_name": "PreToolUse",
        "tool_name": "Bash",
        "tool_input": {
            "command": "curl -H 'Authorization: Bearer sk-ant-0123456789abcdefghij' x",
            "env": {"GITHUB_TOKEN": "ghp_aaaaaaaaaaaaaaaaaaaa"},
        },
        "api_key": "xoxb-secret-slack-token-123",
        "password": "hunter2",
    }
    payload = ClaudeHookPayload.from_body(raw_body)
    entry = log_hook_activity(
        "PreToolUse",
        payload,
        raw_body,
        blocked=False,
        reason=None,
        log_path=log_path,
    )

    serialized = json.dumps(entry)
    assert "hunter2" not in serialized
    assert "ghp_aaaaaaaaaaaaaaaaaaaa" not in serialized
    assert "xoxb-secret-slack-token-123" not in serialized
    assert "sk-ant-0123456789abcdefghij" not in serialized
    assert "[REDACTED" in serialized

    # file was written
    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    disk_entry = json.loads(lines[0])
    assert disk_entry["event"] == "PreToolUse"
    assert disk_entry["session_id"] == "s1"
    assert disk_entry["tool_name"] == "Bash"
    # Sensitive keys scrubbed
    assert disk_entry["payload"]["api_key"] == "[REDACTED]"
    assert disk_entry["payload"]["password"] == "[REDACTED]"
    assert disk_entry["payload"]["tool_input"]["env"] == "[REDACTED]"


def test_claude_hook_payload_from_body_handles_missing_fields():
    payload = ClaudeHookPayload.from_body({"tool_name": "Read"})
    assert payload.tool_name == "Read"
    assert payload.tool_input == {}
    assert payload.extract_command() == ""


def test_claude_hook_payload_extract_command_from_string_input():
    # Some hook versions pipe raw string tool_input; from_body wraps it
    payload = ClaudeHookPayload.from_body({"tool_name": "Bash", "tool_input": "ls -la"})
    assert payload.tool_input == {"_raw": "ls -la"}
    assert payload.extract_command() == ""  # command key missing, safe default
