from __future__ import annotations

import json
import threading
import urllib.request

from operator_core.demand_os import ExperimentStore
from operator_core.demand_routes import register_demand_routes, render_demand_page
from operator_core.http_server import EXTRA_ROUTES, serve_http
from operator_core.lead_ledger import LeadStore, SourceSpec, normalize_row
from operator_core.store import JobStore


def _add_event(store: LeadStore):
    spec = SourceSpec(
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        select="id,email,company_name,status,created_at,source",
        company_field="company_name",
        base_intent=85,
        next_action="Follow up.",
    )
    event = normalize_row(
        spec,
        {
            "id": "r1",
            "email": "buyer@firm.com",
            "company_name": "Firm Co",
            "status": "pending",
            "created_at": "2026-04-24T01:00:00+00:00",
            "source": "intake_form",
        },
    )
    return store.upsert_event(event)[0]


def test_render_demand_page_has_core_sections():
    html = render_demand_page(
        {
            "generated_at": "2026-04-24T01:00:00+00:00",
            "focus_product": "DealBrain",
            "scoreboard": [
                {
                    "product": "DealBrain",
                    "demand_score": 88,
                    "open_leads": 1,
                    "high_intent": 1,
                    "next_experiment": "Broker page",
                }
            ],
            "active_experiments": [],
            "backlog": [
                {
                    "id": "dealbrain-1",
                    "status": "BACKLOG",
                    "product": "DealBrain",
                    "title": "Broker page",
                    "priority": 88,
                }
            ],
            "top_leads": [
                {
                    "intent_score": 99,
                    "status": "NEW",
                    "product": "DealBrain",
                    "company": "Firm Co",
                    "email": "buyer@firm.com",
                    "event_type": "report_intake",
                }
            ],
            "watch_sources": [],
        }
    )

    assert "<!doctype html>" in html
    assert "OPERATOR // DEMAND" in html
    assert "Demand Scoreboard" in html
    assert "Follow-up Queue" in html
    assert "Experiment Bench" in html
    assert "https://" not in html
    assert "cdn." not in html.lower()


def test_demand_routes_return_html_and_json(tmp_path, monkeypatch):
    store = LeadStore(tmp_path / "leads.sqlite3")
    _add_event(store)
    experiments = ExperimentStore(store.db_path)

    monkeypatch.setattr("operator_core.demand_routes.LeadStore", lambda: store)
    monkeypatch.setattr("operator_core.demand_routes.ExperimentStore", lambda db_path=None: experiments)

    saved = dict(EXTRA_ROUTES)
    EXTRA_ROUTES.clear()
    try:
        register_demand_routes()
        server = serve_http(JobStore(tmp_path / "jobs.sqlite3"), host="127.0.0.1", port=0)
        host, port = server.server_address
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/demand", timeout=5) as resp:  # noqa: S310
                html = resp.read().decode("utf-8")
                assert resp.status == 200
            with urllib.request.urlopen(f"http://{host}:{port}/demand.json", timeout=5) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8"))
                assert resp.status == 200
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        EXTRA_ROUTES.clear()
        EXTRA_ROUTES.update(saved)

    assert "DealBrain" in html
    assert payload["focus_product"] == "DealBrain"
    assert payload["backlog"]
