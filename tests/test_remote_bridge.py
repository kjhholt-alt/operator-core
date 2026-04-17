"""Tests for the HMAC-signed remote HTTP bridge (Queue D1)."""

from __future__ import annotations

import time

from operator_core import remote
from operator_core.remote import (
    NonceStore,
    REMOTE_SIGNATURE_HEADER,
    handle_remote_command,
    is_public_bind,
    sign_payload,
    try_handle_http,
)
from operator_core.store import JobStore


SECRET = "unit-test-secret"


def _make_store(tmp_path) -> tuple[JobStore, NonceStore]:
    store = JobStore(tmp_path / "jobs.sqlite3")
    nonce_store = NonceStore(tmp_path / "nonces.sqlite3")
    return store, nonce_store


def _valid_request(command: str = "!op status", nonce: str = "nonce-1", ts: int | None = None):
    ts = ts if ts is not None else int(time.time())
    body = {"command": command, "nonce": nonce, "ts": ts}
    sig = sign_payload(command, nonce, ts, SECRET)
    headers = {REMOTE_SIGNATURE_HEADER: sig}
    return body, headers


def test_valid_signature_enqueues_job(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body, headers = _valid_request()

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 202
    assert "job_id" in result.body
    assert result.body["action"] == "status"
    # Job was actually persisted.
    jobs = store.list_jobs(5)
    assert len(jobs) == 1
    assert jobs[0].action == "status"
    assert jobs[0].metadata.get("source") == "remote"


def test_missing_signature_returns_401(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body, _ = _valid_request()

    result = handle_remote_command(
        body,
        headers={},
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 401
    assert result.body["error"] == "missing_signature"


def test_wrong_secret_returns_403(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    ts = int(time.time())
    body = {"command": "!op status", "nonce": "nonce-x", "ts": ts}
    bad_sig = sign_payload("!op status", "nonce-x", ts, "wrong-secret")
    headers = {REMOTE_SIGNATURE_HEADER: bad_sig}

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 403
    assert result.body["error"] == "bad_signature"


def test_stale_timestamp_returns_403(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    stale_ts = int(time.time()) - 3600  # 1h old
    body, headers = _valid_request(ts=stale_ts)

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 403
    assert result.body["error"] == "stale_ts"


def test_replayed_nonce_returns_409(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body, headers = _valid_request(nonce="replay-me")

    first = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )
    second = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert first.status == 202
    assert second.status == 409
    assert second.body["error"] == "replayed_nonce"
    # Only one job created, not two.
    assert len(store.list_jobs(5)) == 1


def test_missing_secret_returns_503(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body, headers = _valid_request()

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={},
    )

    assert result.status == 503
    assert result.body["error"] == "remote_disabled"


def test_parse_error_returns_400(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    # Command without the `!op` prefix the parser expects.
    body, headers = _valid_request(command="not a real command")

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 400
    assert result.body["error"] == "parse_error"
    # No job should have been enqueued for an unparseable command.
    assert store.list_jobs(5) == []


def test_bad_ts_type_returns_400(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body = {"command": "!op status", "nonce": "n", "ts": "not-a-number"}
    sig = sign_payload("!op status", "n", "not-a-number", SECRET)
    headers = {REMOTE_SIGNATURE_HEADER: sig}

    result = handle_remote_command(
        body,
        headers=headers,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 400
    assert result.body["error"] == "bad_ts"


def test_nonce_gc_removes_old_entries(tmp_path):
    _, nonce_store = _make_store(tmp_path)
    now = 10_000_000
    # Insert an ancient nonce.
    nonce_store.remember("old", now - 10_000)
    # Insert a fresh one.
    nonce_store.remember("new", now)
    nonce_store.gc(now=now)

    assert not nonce_store.seen("old")
    assert nonce_store.seen("new")


def test_case_insensitive_signature_header(tmp_path):
    store, nonce_store = _make_store(tmp_path)
    body, headers = _valid_request(nonce="case-test")
    # Simulate how BaseHTTPRequestHandler normalizes headers.
    lowered = {"x-operator-signature": headers[REMOTE_SIGNATURE_HEADER]}

    result = handle_remote_command(
        body,
        headers=lowered,
        store=store,
        nonce_store=nonce_store,
        env={"OPERATOR_REMOTE_SECRET": SECRET},
    )

    assert result.status == 202


def test_try_handle_http_path_gate(tmp_path):
    store, _ = _make_store(tmp_path)
    assert try_handle_http("/health", {}, {}, store) is None
    assert try_handle_http("/jobs", {}, {}, store) is None


def test_is_public_bind_default_false():
    assert is_public_bind({}) is False
    assert is_public_bind({"OPERATOR_REMOTE_BIND": "127.0.0.1"}) is False
    assert is_public_bind({"OPERATOR_REMOTE_BIND": "0.0.0.0"}) is True


def test_register_remote_route_end_to_end(tmp_path, monkeypatch):
    """Wire register_remote_route through the real HTTP server extension point."""
    import json
    import threading
    import time as _time
    import urllib.request
    import uuid

    from operator_core import paths as paths_mod
    from operator_core.http_server import EXTRA_ROUTES, serve_http

    # Snapshot + restore EXTRA_ROUTES so this test doesn't leak globals.
    snapshot = dict(EXTRA_ROUTES)
    try:
        # Point the shared SQLite DB_PATH at a tmp file so the NonceStore
        # created inside the registered handler gets an isolated table.
        monkeypatch.setattr(paths_mod, "DB_PATH", tmp_path / "nonces.sqlite3")
        monkeypatch.setattr(remote, "DB_PATH", tmp_path / "nonces.sqlite3")

        store = JobStore(tmp_path / "jobs.sqlite3")
        monkeypatch.setenv("OPERATOR_REMOTE_SECRET", SECRET)

        remote.register_remote_route(store)

        server = serve_http(store, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            host, port = server.server_address[0], server.server_address[1]
            ts = int(_time.time())
            command = "!op status"
            nonce = "e2e-nonce"
            sig = sign_payload(command, nonce, ts, SECRET)
            payload = json.dumps({"command": command, "nonce": nonce, "ts": ts}).encode()

            req = urllib.request.Request(
                f"http://{host}:{port}/remote/command",
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    REMOTE_SIGNATURE_HEADER: sig,
                },
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2) as resp:
                assert resp.status == 202
                body = json.loads(resp.read().decode())
            assert body["action"] == "status"
        finally:
            server.shutdown()
            server.server_close()

        assert len(store.list_jobs(5)) == 1
    finally:
        EXTRA_ROUTES.clear()
        EXTRA_ROUTES.update(snapshot)
