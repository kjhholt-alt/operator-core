"""Tests for `GET /ops` and the render_ops_page helper."""
from __future__ import annotations

import threading
import urllib.request

from operator_core.http_server import (
    EXTRA_ROUTES,
    register_extra_route,
    serve_http,
)
from operator_core.store import JobStore
from operator_core.templates import render_ops_page
from operator_core.utils import status as status_mod


class _Harness:
    def __init__(self, store: JobStore, status_path):
        self.server = serve_http(store, host="127.0.0.1", port=0, status_path=status_path)
        self.host, self.port = self.server.server_address
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def get(self, path: str) -> tuple[int, str]:
        url = f"http://{self.host}:{self.port}{path}"
        with urllib.request.urlopen(url, timeout=5) as resp:  # noqa: S310
            return resp.status, resp.read().decode("utf-8")

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def _populate(store: JobStore) -> None:
    store.create_job("morning", prompt="dry run", project="operator-ai")
    store.create_job("deploy_check", prompt="", project="dealbrain")
    store.create_job("review_prs", prompt="", project="prospector-pro")


def _seed_status(status_path):
    status_mod.update_daemon(9911, "2026-04-11T05:00:00", 42.0, status_path)
    status_mod.set_cost_today(1.75, status_path)
    status_mod.set_deploy_health("operator-ai", "ok", status_path)
    status_mod.set_deploy_health("dealbrain", "warn", status_path)
    status_mod.set_risk_tripped(False, status_path)
    status_mod.set_discord_unread(3, status_path)
    status_mod.record_hook_block("destructive", "rm -rf /", status_path)
    status_mod.record_recent_job(
        {"id": "jA", "action": "morning", "status": "done", "project": "operator-ai", "cost_usd": 0.42},
        status_path,
    )


def test_render_ops_page_sections_present(tmp_path):
    status_path = tmp_path / "status.json"
    _seed_status(status_path)
    data = status_mod.load_or_default(status_path)
    jobs = [
        {"id": "x1", "action": "morning", "status": "queued", "project": "operator-ai", "cost_usd": 0.0, "updated_at": "t"},
    ]
    html = render_ops_page(data, jobs)
    # operator-core uses a new Palantir-style template — check for rough section markers
    for section in ["DAEMON", "COST", "DEPLOY HEALTH", "HOOK BLOCKS", "DISCORD"]:
        assert section in html
    assert "9911" in html
    assert "1.75" in html
    assert "operator-ai" in html
    assert "dealbrain" in html
    assert "rm -rf /" in html


def test_render_ops_page_renders_risk_tripped_banner(tmp_path):
    status_path = tmp_path / "status.json"
    status_mod.set_risk_tripped(True, status_path)
    data = status_mod.load_or_default(status_path)
    html = render_ops_page(data, [])
    assert "RISK TRIPPED" in html


def test_render_ops_page_handles_empty_state():
    html = render_ops_page({"schema_version": 2}, [])
    assert "no jobs yet" in html
    assert "no deploy health data" in html
    # operator-core phrasing: "no hook blocks in recent window"
    assert "no hook blocks" in html


def test_ops_route_returns_html_with_jobs(tmp_path):
    status_path = tmp_path / "status.json"
    _seed_status(status_path)
    store = JobStore(tmp_path / "jobs.sqlite3")
    _populate(store)
    harness = _Harness(store, status_path)
    try:
        status, body = harness.get("/ops")
    finally:
        harness.close()
    assert status == 200
    assert "<!doctype html>" in body
    assert "OPERATOR" in body
    # Jobs from the ledger should show
    assert "morning" in body
    assert "deploy_check" in body
    # Deploy health from status should show
    assert "operator-ai" in body


def test_ops_route_renders_offline_no_cdn(tmp_path):
    status_path = tmp_path / "status.json"
    _seed_status(status_path)
    store = JobStore(tmp_path / "jobs.sqlite3")
    harness = _Harness(store, status_path)
    try:
        _, body = harness.get("/ops")
    finally:
        harness.close()
    # No external resource references
    assert "http://" not in body.lower().replace(f"http://{harness.host}", "")
    assert "https://" not in body
    assert "cdn." not in body.lower()
    assert "<style>" in body  # inline CSS only


def test_register_extra_route_plugs_into_get(tmp_path):
    # Ensure a clean routing table for this test
    saved = dict(EXTRA_ROUTES)
    EXTRA_ROUTES.clear()
    EXTRA_ROUTES.update(saved)

    def custom(handler, body):
        return 200, {"hello": "from-extra"}

    register_extra_route("GET", "/_test_extra", custom)
    try:
        store = JobStore(tmp_path / "jobs.sqlite3")
        harness = _Harness(store, tmp_path / "status.json")
        try:
            status, body = harness.get("/_test_extra")
        finally:
            harness.close()
        assert status == 200
        assert "from-extra" in body
    finally:
        EXTRA_ROUTES.pop(("GET", "/_test_extra"), None)
