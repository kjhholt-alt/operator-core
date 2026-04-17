"""Local HTTP surface for health checks, Claude hooks, and job creation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from .commands import CommandParseError, parse_operator_command
from .paths import DATA_DIR, STATUS_PATH
from .security import command_is_blocked, redact_mapping, redact_secrets
from .store import JobStore

# --- Extension point for other lanes (remote, slash commands, etc.) ---------
#
# Other lanes plug additional GET/POST routes in without touching this file.
# Each entry: path -> callable(handler, body_or_none) -> (status_code, dict|str).
#
# Usage from another module:
#     from operator_v3.http_server import register_extra_route
#     def handle_remote(handler, body):
#         return 200, {"ok": True}
#     register_extra_route("POST", "/remote/command", handle_remote)
#
# Handler receives the live `OperatorRequestHandler` so callers can read
# headers / write custom responses if they must; for simple JSON responses
# returning (status, dict) is enough and the server will serialize.
EXTRA_ROUTES: dict[tuple[str, str], Any] = {}


def register_extra_route(method: str, path: str, func: Any) -> None:
    """Register an extra HTTP route. Method is 'GET' or 'POST'."""
    EXTRA_ROUTES[(method.upper(), path)] = func


@dataclass(frozen=True)
class ClaudeHookPayload:
    """Shape of a Claude Code `PreToolUse` / `PostToolUse` hook invocation.

    Claude Code pipes JSON to stdin of the configured `command` hook. The
    documented fields include `session_id`, `transcript_path`, `cwd`,
    `hook_event_name`, `tool_name`, `tool_input`, and (for PostToolUse)
    `tool_response`. We only treat `tool_name` + `tool_input.command` as
    load-bearing; everything else is logged-but-scrubbed.
    """

    session_id: str
    hook_event_name: str
    tool_name: str
    tool_input: dict[str, Any]
    cwd: str = ""
    transcript_path: str = ""
    tool_response: Any = None

    @classmethod
    def from_body(cls, body: dict[str, Any]) -> "ClaudeHookPayload":
        tool_input = body.get("tool_input") or body.get("input") or {}
        if not isinstance(tool_input, dict):
            tool_input = {"_raw": tool_input}
        return cls(
            session_id=str(body.get("session_id") or ""),
            hook_event_name=str(body.get("hook_event_name") or body.get("event") or ""),
            tool_name=str(body.get("tool_name") or body.get("tool") or ""),
            tool_input=tool_input,
            cwd=str(body.get("cwd") or ""),
            transcript_path=str(body.get("transcript_path") or ""),
            tool_response=body.get("tool_response"),
        )

    def extract_command(self) -> str:
        value = self.tool_input.get("command") or self.tool_input.get("cmd") or ""
        return str(value)


HOOK_LOG_PATH = DATA_DIR / "logs" / "hooks.jsonl"


def append_hook_log(entry: dict[str, Any], log_path: Path = HOOK_LOG_PATH) -> None:
    """Append a hook activity record to the JSONL log with secrets scrubbed.

    Creates the parent directory on demand. Caller must pass a dict that has
    already been safety-scrubbed via `redact_mapping`.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, default=str) + "\n")


def log_hook_activity(
    event: str,
    payload: "ClaudeHookPayload",
    raw_body: dict[str, Any],
    *,
    blocked: bool,
    reason: str | None,
    log_path: Path = HOOK_LOG_PATH,
) -> dict[str, Any]:
    """Build a scrubbed JSONL log entry and append it. Returns the entry.

    Every field from the raw Claude hook body is passed through
    `redact_mapping`, which drops sensitive keys (env/token/key/password/
    secret/authorization/credential) and runs the string redactor on leaves.
    """
    scrubbed_body = redact_mapping(raw_body)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "session_id": payload.session_id,
        "tool_name": payload.tool_name,
        "blocked": blocked,
        "reason": reason,
        "payload": scrubbed_body,
    }
    append_hook_log(entry, log_path=log_path)
    return entry


class OperatorHttpServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        store: JobStore,
        status_path: Path | None = None,
    ):
        self.store = store
        self.status_path = status_path or STATUS_PATH
        super().__init__(server_address, OperatorRequestHandler)


class OperatorRequestHandler(BaseHTTPRequestHandler):
    server: OperatorHttpServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        if self.path == "/health":
            self._json(200, {"ok": True, "service": "operator-v3"})
            return
        if self.path == "/ops":
            self._ops_page()
            return
        if self._dispatch_extra("GET", None):
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self) -> None:
        body = self._body()
        if self.path == "/jobs":
            self._create_job(body)
            return
        if self.path == "/hooks/claude/pre-tool-use":
            self._pre_tool_use(body)
            return
        if self.path == "/hooks/claude/post-tool-use":
            self._post_tool_use(body)
            return
        if self._dispatch_extra("POST", body):
            return
        self._json(404, {"error": "not_found"})

    def _dispatch_extra(self, method: str, body: dict[str, Any] | None) -> bool:
        func = EXTRA_ROUTES.get((method, self.path))
        if func is None:
            return False
        try:
            result = func(self, body)
        except Exception as exc:  # pragma: no cover - defensive
            self._json(500, {"error": "extra_route_failed", "detail": str(exc)})
            return True
        if result is None:
            # Handler wrote its own response
            return True
        status, payload = result
        if isinstance(payload, str):
            self._html(status, payload)
        else:
            self._json(status, payload)
        return True

    def _ops_page(self) -> None:
        from utils import status as status_mod  # lazy to avoid cycles
        from .templates import render_ops_page

        status_data = status_mod.load_or_default(self.server.status_path)
        jobs = [
            {
                "id": j.id,
                "action": j.action,
                "status": j.status,
                "project": j.project,
                "cost_usd": j.cost_usd,
                "updated_at": j.updated_at,
            }
            for j in self.server.store.list_jobs(limit=20)
        ]
        html = render_ops_page(status_data, jobs)
        self._html(200, html)

    def _create_job(self, body: dict[str, Any]) -> None:
        try:
            if "command" in body:
                parsed = parse_operator_command(str(body["command"]))
                metadata = dict(parsed.args) if parsed.args else {}
                job = self.server.store.create_job(parsed.action, parsed.prompt, parsed.project, metadata=metadata)
            else:
                job = self.server.store.create_job(
                    action=str(body["action"]),
                    prompt=str(body.get("prompt", "")),
                    project=body.get("project"),
                    metadata=body.get("metadata") or {},
                )
        except (KeyError, CommandParseError, ValueError) as exc:
            self._json(400, {"error": str(exc)})
            return
        self._json(202, {"job_id": job.id, "status": job.status})

    def _pre_tool_use(self, body: dict[str, Any]) -> None:
        payload = ClaudeHookPayload.from_body(body)
        command = payload.extract_command()
        reason = command_is_blocked(command)
        log_hook_activity("PreToolUse", payload, body, blocked=bool(reason), reason=reason)

        metadata = {
            "tool_name": payload.tool_name,
            "command": redact_secrets(command),
            "blocked": bool(reason),
            "reason": reason,
        }
        self.server.store.create_job("hook_pre_tool_use", metadata=metadata)

        if reason:
            self._json(
                200,
                {
                    "permissionDecision": "deny",
                    "permissionDecisionReason": reason,
                    "continue": False,
                },
            )
            return
        self._json(200, {"continue": True})

    def _post_tool_use(self, body: dict[str, Any]) -> None:
        payload = ClaudeHookPayload.from_body(body)
        log_hook_activity("PostToolUse", payload, body, blocked=False, reason=None)
        metadata = {
            "tool_name": payload.tool_name,
            "summary": redact_secrets(json.dumps(body, default=str))[:4000],
        }
        self.server.store.create_job("hook_post_tool_use", metadata=metadata)
        self._json(200, {"ok": True})

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8") if length else "{}"
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self, status: int, html: str) -> None:
        data = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _text(self, status: int, body: str, content_type: str = "text/plain; charset=utf-8") -> None:
        data = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def serve_http(
    store: JobStore,
    host: str = "127.0.0.1",
    port: int = 8765,
    status_path: Path | None = None,
) -> OperatorHttpServer:
    """Create the local-only HTTP server. Caller owns serve_forever()."""
    if host not in {"127.0.0.1", "localhost"}:
        raise ValueError("Operator V3 HTTP server must bind to localhost only")
    return OperatorHttpServer((host, port), store, status_path=status_path)
