"""HMAC-signed remote HTTP bridge for Operator V3.

Exposes a single `POST /remote/command` endpoint handler that the existing
daemon HTTP server can delegate to. The handler validates an HMAC-SHA256
signature against `OPERATOR_REMOTE_SECRET`, rejects stale/replayed requests
using a SQLite nonce table, and enqueues a job identical to the Discord
code path so the same owner lock and risk gating apply.

Default bind remains 127.0.0.1. Going public requires BOTH
`OPERATOR_REMOTE_BIND=0.0.0.0` and an explicit risk-warning banner
(wired into `utils/status.py` by a future commit — see `is_public_bind`).

This module is deliberately standalone: it does not import the HTTP server
so tests can exercise handling against any dict-shaped request. The
integration point with `operator_v3/http_server.py` is a single line added
inside `OperatorRequestHandler.do_POST`:

    if self.path == "/remote/command":
        status, payload = remote.handle_remote_command(body, headers=dict(self.headers), store=self.server.store)
        self._json(status, payload)
        return
"""

from __future__ import annotations

import hmac
import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from .commands import CommandParseError, parse_operator_command
from .paths import DB_PATH, ensure_data_dirs
from .store import JobStore


REMOTE_SIGNATURE_HEADER = "X-Operator-Signature"
REMOTE_MAX_SKEW_SECONDS = 300
REMOTE_NONCE_TTL_SECONDS = 600
REMOTE_SECRET_ENV = "OPERATOR_REMOTE_SECRET"
REMOTE_BIND_ENV = "OPERATOR_REMOTE_BIND"


@dataclass(frozen=True)
class RemoteResult:
    status: int
    body: dict[str, Any]


def is_public_bind(env: Mapping[str, str] | None = None) -> bool:
    """Return True iff the remote bridge is intentionally public (0.0.0.0).

    `utils/status.py` should read this helper and render a loud risk-banner
    whenever it returns True. Default is False — the daemon always binds
    loopback unless the operator explicitly opts in.
    """
    env = env if env is not None else os.environ
    return env.get(REMOTE_BIND_ENV, "").strip() == "0.0.0.0"


class NonceStore:
    """Tiny SQLite-backed replay-protection table.

    Reuses the daemon SQLite database by default so there is no new file to
    manage. Nonces older than `REMOTE_NONCE_TTL_SECONDS` are GC'd on every
    check.
    """

    def __init__(self, db_path: Path | None = None):
        ensure_data_dirs()
        # Resolve DB_PATH at call time so monkeypatch can point tests at a
        # tmp SQLite file without hitting the default-arg capture trap.
        if db_path is None:
            from . import paths as _paths
            db_path = _paths.DB_PATH
        self.db_path = db_path
        self._init()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS remote_nonces (
                    nonce TEXT PRIMARY KEY,
                    ts INTEGER NOT NULL
                )
                """
            )

    def gc(self, now: int | None = None) -> int:
        now = now if now is not None else int(time.time())
        cutoff = now - REMOTE_NONCE_TTL_SECONDS
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM remote_nonces WHERE ts < ?", (cutoff,))
            return cur.rowcount or 0

    def seen(self, nonce: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM remote_nonces WHERE nonce = ?", (nonce,)
            ).fetchone()
            return row is not None

    def remember(self, nonce: str, ts: int) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO remote_nonces(nonce, ts) VALUES(?, ?)",
                (nonce, ts),
            )


def _canonical_message(command: str, nonce: str, ts: int | str) -> bytes:
    return f"{command}|{nonce}|{ts}".encode("utf-8")


def sign_payload(command: str, nonce: str, ts: int | str, secret: str) -> str:
    """Produce the hex HMAC-SHA256 signature expected by `handle_remote_command`.

    Clients (the phone) use this same function. Exposed so tests and the
    shortcut on-device script can import it.
    """
    msg = _canonical_message(command, nonce, ts)
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _lookup_header(headers: Mapping[str, Any], name: str) -> str | None:
    """Case-insensitive header lookup — HTTP headers are not case-sensitive."""
    name_lower = name.lower()
    for key, value in headers.items():
        if str(key).lower() == name_lower:
            return str(value)
    return None


def handle_remote_command(
    body: Mapping[str, Any],
    *,
    headers: Mapping[str, Any],
    store: JobStore,
    nonce_store: NonceStore | None = None,
    env: Mapping[str, str] | None = None,
    now: int | None = None,
) -> RemoteResult:
    """Validate and dispatch a remote `{command, nonce, ts}` request.

    Returns (status_code, json_body) that the HTTP layer can serialize.
    Never raises for normal client errors — all denials are shaped as 4xx
    responses. The only exceptions that propagate are JobStore failures,
    which indicate a bug in the daemon itself.
    """
    env = env if env is not None else os.environ
    now = now if now is not None else int(time.time())
    nonce_store = nonce_store or NonceStore()

    secret = env.get(REMOTE_SECRET_ENV, "").strip()
    if not secret:
        return RemoteResult(503, {"error": "remote_disabled", "detail": "OPERATOR_REMOTE_SECRET not set"})

    signature = _lookup_header(headers, REMOTE_SIGNATURE_HEADER)
    if not signature:
        return RemoteResult(401, {"error": "missing_signature"})

    command = body.get("command") if isinstance(body, Mapping) else None
    nonce = body.get("nonce") if isinstance(body, Mapping) else None
    ts_raw = body.get("ts") if isinstance(body, Mapping) else None

    if not isinstance(command, str) or not command.strip():
        return RemoteResult(400, {"error": "missing_command"})
    if not isinstance(nonce, str) or not nonce.strip():
        return RemoteResult(400, {"error": "missing_nonce"})
    try:
        ts = int(ts_raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return RemoteResult(400, {"error": "bad_ts"})

    if abs(now - ts) > REMOTE_MAX_SKEW_SECONDS:
        return RemoteResult(403, {"error": "stale_ts", "detail": f"|now-ts|>{REMOTE_MAX_SKEW_SECONDS}s"})

    expected = sign_payload(command, nonce, ts, secret)
    if not hmac.compare_digest(expected, signature):
        return RemoteResult(403, {"error": "bad_signature"})

    # Replay protection. GC first so the table does not grow unbounded, then
    # check-and-remember atomically-ish (best effort — SQLite PK prevents
    # duplicate inserts even under a race).
    nonce_store.gc(now)
    if nonce_store.seen(nonce):
        return RemoteResult(409, {"error": "replayed_nonce"})

    # Parse the command the same way the Discord bot does so owner lock /
    # risk gating / action routing all stay identical.
    try:
        parsed = parse_operator_command(command)
    except CommandParseError as exc:
        return RemoteResult(400, {"error": "parse_error", "detail": str(exc)})

    # Owner-lock parity: `!op` is already owner-only on Discord. The remote
    # bridge is additionally locked behind knowledge of the shared secret,
    # so anyone hitting this endpoint is by definition the owner. We still
    # reject non-owner-safe actions (cancel requests against foreign jobs
    # etc.) — but the command parser already restricts the action set, and
    # the runner enforces per-action risk gating. We only need to make sure
    # nothing arrives via the remote path that the Discord path would block.
    #
    # The single Discord-only check that does NOT apply here is the bot's
    # author.id == OPERATOR_OWNER_DISCORD_ID gate. That gate exists to stop
    # random Discord users; it is irrelevant to an HMAC-authenticated HTTP
    # request from the owner's phone. Documented here so a future reader
    # does not "fix" the missing owner check.

    nonce_store.remember(nonce, ts)

    job = store.create_job(
        parsed.action,
        parsed.prompt,
        parsed.project,
        metadata={"source": "remote", "nonce": nonce, "ts": ts},
    )
    return RemoteResult(
        202,
        {"job_id": job.id, "status": job.status, "action": parsed.action},
    )


def register_remote_route(store: JobStore) -> None:
    """Wire POST /remote/command into the shared HTTP server extension table.

    Observability lane shipped the `EXTRA_ROUTES` extension point in the
    V4 B2 commit, so the integration is a single call:

        from operator_v3.remote import register_remote_route
        register_remote_route(store)

    The daemon is expected to call this once at startup, after
    `serve_http(...)` has been built.
    """
    from .http_server import register_extra_route

    def _handler(handler, body):
        # Normalize headers into a plain dict so `handle_remote_command`
        # can do case-insensitive lookup without leaking the Message class.
        headers = {key: value for key, value in handler.headers.items()}
        result = handle_remote_command(body or {}, headers=headers, store=store)
        return result.status, result.body

    register_extra_route("POST", "/remote/command", _handler)


def try_handle_http(
    path: str,
    body: Mapping[str, Any],
    headers: Mapping[str, Any],
    store: JobStore,
) -> RemoteResult | None:
    """Extension-point hook for `OperatorRequestHandler.do_POST`.

    Returns None if this module should not handle the path, or a
    `RemoteResult` that the HTTP layer should serialize. Keeping the
    dispatch indirection here means `http_server.py` only needs a single
    line added inside `do_POST`:

        result = remote.try_handle_http(self.path, body, dict(self.headers), self.server.store)
        if result is not None:
            self._json(result.status, result.body); return
    """
    if path != "/remote/command":
        return None
    return handle_remote_command(body, headers=headers, store=store)


__all__ = [
    "REMOTE_SIGNATURE_HEADER",
    "REMOTE_MAX_SKEW_SECONDS",
    "REMOTE_NONCE_TTL_SECONDS",
    "REMOTE_SECRET_ENV",
    "REMOTE_BIND_ENV",
    "RemoteResult",
    "NonceStore",
    "sign_payload",
    "handle_remote_command",
    "try_handle_http",
    "is_public_bind",
]
