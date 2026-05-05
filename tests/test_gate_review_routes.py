"""Tests for /gate-review HTML + JSON + resolve routes."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from pathlib import Path

import pytest

from operator_core import gate_review
from operator_core.http_server import EXTRA_ROUTES, OperatorHttpServer
from operator_core.gate_review_routes import (
    register_gate_review_routes,
    _maybe_trigger_auto_suppression_pr,
    _AUTO_PR_LAST_RUN,
)
from operator_core.store import JobStore


# --- shared fixtures --------------------------------------------------------

@pytest.fixture
def gate_db(tmp_path, monkeypatch):
    """Point gate_review at a per-test sqlite file via env var."""
    db = tmp_path / "gate_review.sqlite"
    monkeypatch.setenv("OUTREACH_GATE_REVIEW_DB", str(db))
    return db


@pytest.fixture
def seeded_db(gate_db):
    """Seed the gate review DB with three pending disagreements."""
    events = [
        {
            "ts": "2026-05-05T10:00:00Z",
            "payload": {
                "product": "oe", "lead_hash": "abc1234",
                "agreement": "would_block_new",
                "lead_business_name": "Test Pizza Co",
                "gate_block_label": "network_scrub:business_name:test pizza co",
                "legacy_block_reason": None,
            },
        },
        {
            "ts": "2026-05-05T10:01:00Z",
            "payload": {
                "product": "oe", "lead_hash": "def5678",
                "agreement": "would_allow_new",
                "lead_business_name": "Honest Lawyer LLC",
                "gate_block_label": None,
                "legacy_block_reason": "manual_blocklist",
            },
        },
        {
            "ts": "2026-05-05T10:02:00Z",
            "payload": {
                "product": "pp", "lead_hash": "ghi9999",
                "agreement": "both_block_diff_reason",
                "lead_business_name": "Edge Case Inc",
                "gate_block_label": "tld:business_email:.invalid",
                "legacy_block_reason": "no_email",
            },
        },
    ]
    new, _ = gate_review.ingest_events(events)
    assert new == 3
    return gate_db


@pytest.fixture
def http_server(tmp_path):
    """Spin up an OperatorHttpServer on a random localhost port."""
    register_gate_review_routes()
    sock = socket.socket(); sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    store = JobStore(db_path=tmp_path / "jobs.sqlite")
    server = OperatorHttpServer(("127.0.0.1", port), store, status_path=tmp_path / "status.json")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    # Tiny wait for the bind.
    time.sleep(0.05)
    yield port
    server.shutdown()
    server.server_close()


def _http_get(port: int, path: str) -> tuple[int, bytes, dict]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    conn.request("GET", path)
    resp = conn.getresponse()
    body = resp.read()
    headers = dict(resp.getheaders())
    conn.close()
    return resp.status, body, headers


def _http_post_json(port: int, path: str, payload: dict) -> tuple[int, dict]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    body = json.dumps(payload)
    conn.request("POST", path, body=body, headers={"Content-Type": "application/json"})
    resp = conn.getresponse()
    raw = resp.read()
    conn.close()
    return resp.status, json.loads(raw or b"{}")


# --- registration -----------------------------------------------------------

class TestRegistration:
    def test_routes_register_into_extra_routes(self):
        register_gate_review_routes()
        assert ("GET", "/gate-review") in EXTRA_ROUTES
        assert ("GET", "/gate-review.json") in EXTRA_ROUTES
        assert ("POST", "/gate-review/resolve") in EXTRA_ROUTES


# --- GET /gate-review (HTML) ------------------------------------------------

class TestGetListing:
    def test_renders_summary_and_three_pending_items(self, seeded_db, http_server):
        status, body, headers = _http_get(http_server, "/gate-review")
        assert status == 200
        assert headers["Content-Type"].startswith("text/html")
        text = body.decode("utf-8")
        assert "Sender Gate review queue" in text
        assert "Test Pizza Co" in text
        assert "Honest Lawyer LLC" in text
        assert "Edge Case Inc" in text
        assert "Pending items (3)" in text

    def test_filter_by_product(self, seeded_db, http_server):
        status, body, _ = _http_get(http_server, "/gate-review?product=oe")
        assert status == 200
        text = body.decode("utf-8")
        assert "Test Pizza Co" in text
        assert "Honest Lawyer LLC" in text
        assert "Edge Case Inc" not in text  # pp filtered out
        assert "Pending items (2)" in text

    def test_empty_listing_shows_friendly_message(self, gate_db, http_server):
        status, body, _ = _http_get(http_server, "/gate-review")
        text = body.decode("utf-8")
        assert status == 200
        # Empty DB still returns 200 with the empty-state message.
        assert "No review items have ever been ingested" in text or "No pending review items" in text

    def test_limit_query_param_clamps(self, seeded_db, http_server):
        # limit=99999 should be clamped to 200, not error.
        status, body, _ = _http_get(http_server, "/gate-review?limit=99999")
        assert status == 200

    def test_query_string_is_stripped_for_dispatch(self, seeded_db, http_server):
        # Dispatcher previously matched only on bare path -- regression guard.
        status, _, _ = _http_get(http_server, "/gate-review?product=oe&limit=5")
        assert status == 200


# --- GET /gate-review.json --------------------------------------------------

class TestGetListingJson:
    def test_returns_pending_items_as_json(self, seeded_db, http_server):
        status, body, headers = _http_get(http_server, "/gate-review.json")
        assert status == 200
        assert headers["Content-Type"].startswith("application/json")
        data = json.loads(body)
        assert data["count"] == 3
        names = {item["business_name"] for item in data["items"]}
        assert "Test Pizza Co" in names

    def test_product_filter_in_json(self, seeded_db, http_server):
        status, body, _ = _http_get(http_server, "/gate-review.json?product=pp")
        data = json.loads(body)
        assert data["product_filter"] == "pp"
        assert data["count"] == 1
        assert data["items"][0]["business_name"] == "Edge Case Inc"


# --- POST /gate-review/resolve ----------------------------------------------

class TestPostResolve:
    def test_resolve_marks_item(self, seeded_db, http_server):
        # Find one pending item.
        before = gate_review.list_pending()
        assert len(before) == 3
        target_id = before[0].id

        status, data = _http_post_json(http_server, "/gate-review/resolve", {
            "id": target_id, "status": "approved_gate", "note": "test note",
        })
        assert status == 200
        assert data["ok"] is True
        assert data["item"]["status"] == "approved_gate"
        assert data["item"]["resolution_note"] == "test note"
        assert data["item"]["resolved_by"] == "web-ui"

        # That item is no longer pending.
        after = gate_review.list_pending()
        assert len(after) == 2
        assert target_id not in {i.id for i in after}

    def test_resolve_missing_id_returns_400(self, seeded_db, http_server):
        status, data = _http_post_json(http_server, "/gate-review/resolve", {"status": "approved_gate"})
        assert status == 400
        assert data["error"] == "id_required"

    def test_resolve_bad_status_returns_400(self, seeded_db, http_server):
        target = gate_review.list_pending()[0].id
        status, data = _http_post_json(http_server, "/gate-review/resolve", {
            "id": target, "status": "definitely_not_a_status",
        })
        assert status == 400
        assert data["error"] == "resolve_failed"
        assert "definitely_not_a_status" in data["detail"]

    def test_resolve_missing_status_returns_400(self, seeded_db, http_server):
        target = gate_review.list_pending()[0].id
        status, data = _http_post_json(http_server, "/gate-review/resolve", {"id": target})
        assert status == 400
        assert data["error"] == "status_required"


# --- Auto-suppression-PR trigger --------------------------------------------

class TestAutoSuppressionTrigger:
    def setup_method(self):
        # Reset the rate-limit timer between tests.
        _AUTO_PR_LAST_RUN["ts"] = 0.0

    def test_disabled_by_default_is_noop(self, monkeypatch):
        # Without env flag, must not spawn a thread or error.
        monkeypatch.delenv("OPERATOR_GATE_REVIEW_AUTO_SUPPRESS_PR", raising=False)
        before = threading.active_count()
        _maybe_trigger_auto_suppression_pr()
        # Allow scheduler latency.
        time.sleep(0.05)
        assert threading.active_count() == before

    def test_enabled_spawns_background_thread(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_GATE_REVIEW_AUTO_SUPPRESS_PR", "1")
        # Patch _run_auto_pr to a no-op recorder so we don't actually try to
        # call GitHub.
        from operator_core import gate_review_routes as mod
        called = {"count": 0}

        def fake_run(threshold):
            called["count"] += 1

        monkeypatch.setattr(mod, "_run_auto_pr", fake_run)
        _maybe_trigger_auto_suppression_pr()
        # The thread is daemon and runs immediately.
        time.sleep(0.1)
        assert called["count"] == 1

    def test_rate_limited_within_60_seconds(self, monkeypatch):
        monkeypatch.setenv("OPERATOR_GATE_REVIEW_AUTO_SUPPRESS_PR", "1")
        from operator_core import gate_review_routes as mod
        called = {"count": 0}

        def fake_run(threshold):
            called["count"] += 1

        monkeypatch.setattr(mod, "_run_auto_pr", fake_run)
        _maybe_trigger_auto_suppression_pr()
        _maybe_trigger_auto_suppression_pr()
        _maybe_trigger_auto_suppression_pr()
        time.sleep(0.1)
        assert called["count"] == 1

    def test_run_auto_pr_below_threshold_does_not_open_pr(self, gate_db, monkeypatch):
        """Single approved_gate item shouldn't trigger PR when threshold is 5."""
        # Seed one approved_gate item.
        events = [{"ts": "2026-05-05T10:00:00Z", "payload": {
            "product": "oe", "lead_hash": "x1",
            "agreement": "would_block_new",
            "lead_business_name": "One Co",
            "gate_block_label": "network_scrub:business_name:one co",
            "legacy_block_reason": None,
        }}]
        gate_review.ingest_events(events)
        item = gate_review.list_pending()[0]
        gate_review.resolve(item.id, "approved_gate", note="t", resolved_by="t")

        from operator_core import gate_review_routes as mod
        from operator_core import suppression_pr

        opened = {"count": 0}
        monkeypatch.setattr(suppression_pr, "open_pr", lambda plan: opened.update(count=opened["count"] + 1) or {})

        # Use a tmp scrub yml path so build_plan doesn't seed in $HOME.
        monkeypatch.setenv("OPERATOR_SCRUB_YML_PATH", str(gate_db.parent / "network_scrub.yml"))
        mod._run_auto_pr(threshold=5)
        assert opened["count"] == 0

    def test_run_auto_pr_above_threshold_opens_pr(self, gate_db, monkeypatch, tmp_path):
        # Seed 3 approved_gate items.
        events = [
            {"ts": f"2026-05-05T10:0{i}:00Z", "payload": {
                "product": "oe", "lead_hash": f"hash{i}",
                "agreement": "would_block_new",
                "lead_business_name": f"Biz {i}",
                "gate_block_label": f"network_scrub:business_name:biz {i}",
                "legacy_block_reason": None,
            }} for i in range(3)
        ]
        gate_review.ingest_events(events)
        for it in gate_review.list_pending():
            gate_review.resolve(it.id, "approved_gate", note="t", resolved_by="t")

        from operator_core import gate_review_routes as mod
        from operator_core import suppression_pr

        opens = []
        marks = []

        def fake_open_pr(plan):
            opens.append(plan)
            return {"url": "https://github.com/fake/pr/1", "number": 1}

        def fake_mark(items):
            marks.extend(items)
            return len(items)

        monkeypatch.setattr(suppression_pr, "open_pr", fake_open_pr)
        monkeypatch.setattr(suppression_pr, "mark_items_suppressed", fake_mark)
        monkeypatch.setenv("OPERATOR_SCRUB_YML_PATH", str(tmp_path / "scrub.yml"))

        mod._run_auto_pr(threshold=2)

        assert len(opens) == 1
        plan = opens[0]
        assert len(plan.new_business_names) == 3
        assert len(marks) == 3
