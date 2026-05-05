"""Tests for the /cut-over web route."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from operator_core import cutover_streak as cs
from operator_core import outreach_audit
from operator_core.cutover_routes import register_cutover_routes
from operator_core.http_server import OperatorHttpServer
from operator_core.store import JobStore


@pytest.fixture
def state(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_DB", str(tmp_path / "streak.sqlite"))
    return tmp_path


@pytest.fixture
def http_server(tmp_path, monkeypatch):
    register_cutover_routes()
    sock = socket.socket(); sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    store = JobStore(db_path=tmp_path / "jobs.sqlite")
    server = OperatorHttpServer(("127.0.0.1", port), store, status_path=tmp_path / "status.json")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    time.sleep(0.05)
    yield port
    server.shutdown()
    server.server_close()


def _get(port: int, path: str) -> tuple[int, bytes]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read()
    conn.close()
    return resp.status, body


def _ts(seconds_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat().replace("+00:00", "Z")


def test_empty_state_renders_friendly_message(state, http_server, monkeypatch):
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [])
    status, body = _get(http_server, "/cut-over")
    assert status == 200
    assert b"No products tracked yet" in body


def test_healthy_promoted_product_renders_ready_card(state, http_server, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://github.com/x/y/pull/42", now_ts=_ts(60 * 60 * 6))
    monkeypatch.setattr(
        outreach_audit, "collect",
        lambda paths: [outreach_audit.ProductSummary(product="oe", total=100, match=100)],
    )
    status, body = _get(http_server, "/cut-over")
    assert status == 200
    text = body.decode("utf-8")
    assert "oe" in text
    assert "READY" in text
    assert "https://github.com/x/y/pull/42" in text
    # Healthy + in window => "Within rollback window, no regression."
    assert "no regression" in text


def test_regressed_promoted_product_renders_alert(state, http_server, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://github.com/x/y/pull/42", now_ts=_ts(60 * 60 * 6))
    monkeypatch.setattr(
        outreach_audit, "collect",
        lambda paths: [outreach_audit.ProductSummary(
            product="oe", total=100, match=80, would_block_new=10, would_allow_new=10,
        )],
    )
    status, body = _get(http_server, "/cut-over")
    text = body.decode("utf-8")
    assert "REGRESSION" in text


def test_streak_only_no_promotion_renders_progress_bar(state, http_server, monkeypatch):
    cs.record_check("oe", True, now_ts=_ts(7200))
    monkeypatch.setattr(
        outreach_audit, "collect",
        lambda paths: [outreach_audit.ProductSummary(product="oe", total=10, match=10)],
    )
    status, body = _get(http_server, "/cut-over")
    text = body.decode("utf-8")
    assert "oe" in text
    assert "Streak" in text
    assert "Not yet promoted" in text


def test_json_endpoint_returns_streak_rows(state, http_server, monkeypatch):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://x/pr/1")
    cs.record_check("pp", False)
    monkeypatch.setattr(outreach_audit, "collect", lambda paths: [])
    status, body = _get(http_server, "/cut-over.json")
    assert status == 200
    data = json.loads(body)
    assert data["count"] == 2
    products = {p["product"] for p in data["products"]}
    assert products == {"oe", "pp"}
