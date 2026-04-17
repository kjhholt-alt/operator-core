"""Tests for operator_core.metrics — /metrics endpoint + render_metrics."""
from __future__ import annotations

import threading
import urllib.request

from operator_core.http_server import EXTRA_ROUTES, serve_http
from operator_core.metrics import (
    DURATION_BUCKETS,
    register_metrics_route,
    render_metrics,
)
from operator_core.store import JobStore
from operator_core.utils import status as status_mod


def _seed_jobs(store: JobStore) -> None:
    j1 = store.create_job("morning", project="operator-ai")
    store.update_job(j1.id, status="done", updated_at="2026-04-11T06:00:10+00:00")
    j2 = store.create_job("deploy_check", project="dealbrain")
    store.update_job(j2.id, status="failed")
    j3 = store.create_job("review_prs", project="prospector-pro")
    store.update_job(j3.id, status="running")


def _seed_status(path):
    status_mod.set_deploy_health("operator-ai", "ok", path)
    status_mod.set_deploy_health("dealbrain", "warn", path)
    status_mod.record_hook_block("destructive", "rm -rf /", path)
    status_mod.record_hook_block("force-push", "git push --force", path)


def _seed_costs(path):
    from datetime import date

    today = date.today().isoformat()
    path.write_text(
        f"{today}T06:00:00,morning-briefing,0.42,10,40,\n"
        f"{today}T07:00:00,deploy-checker,0.18,5,20,\n"
        "2026-01-01T00:00:00,old,9.99,1,1,\n",
        encoding="utf-8",
    )


def test_render_metrics_has_all_metric_families(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    _seed_jobs(store)
    status_path = tmp_path / "status.json"
    _seed_status(status_path)
    costs = tmp_path / "costs.csv"
    _seed_costs(costs)

    text = render_metrics(store=store, status_path=status_path, costs_csv=costs)

    assert "operator_jobs_total" in text
    assert 'operator_jobs_total{status="done"} 1' in text
    assert 'operator_jobs_total{status="failed"} 1' in text
    assert 'operator_jobs_total{status="running"} 1' in text
    assert "operator_jobs_duration_seconds_bucket" in text
    assert "operator_jobs_duration_seconds_count" in text
    assert 'operator_deploy_health{project="operator-ai",state="ok"} 1' in text
    assert 'operator_deploy_health{project="dealbrain",state="warn"} 1' in text
    assert "operator_hook_blocks_total 2" in text
    assert "operator_cost_usd_today 0.6000" in text


def test_render_metrics_stable_across_job_transitions(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    # Start with one queued job
    job = store.create_job("morning", project="operator-ai")
    text1 = render_metrics(
        store=store, status_path=tmp_path / "s.json", costs_csv=tmp_path / "missing.csv"
    )
    assert 'operator_jobs_total{status="queued"} 1' in text1
    assert 'operator_jobs_total{status="done"} 0' in text1

    # Transition to running
    store.update_job(job.id, status="running")
    text2 = render_metrics(
        store=store, status_path=tmp_path / "s.json", costs_csv=tmp_path / "missing.csv"
    )
    assert 'operator_jobs_total{status="running"} 1' in text2
    assert 'operator_jobs_total{status="queued"} 0' in text2

    # Transition to done
    store.update_job(job.id, status="done")
    text3 = render_metrics(
        store=store, status_path=tmp_path / "s.json", costs_csv=tmp_path / "missing.csv"
    )
    assert 'operator_jobs_total{status="done"} 1' in text3
    assert 'operator_jobs_total{status="running"} 0' in text3


def test_render_metrics_histogram_has_all_buckets(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    job = store.create_job("morning")
    store.update_job(job.id, status="done", updated_at="2026-04-11T06:00:05+00:00")
    text = render_metrics(
        store=store, status_path=tmp_path / "s.json", costs_csv=tmp_path / "m.csv"
    )
    for bucket in DURATION_BUCKETS:
        assert f'le="{bucket}"' in text
    assert 'le="+Inf"' in text


def test_render_metrics_handles_missing_costs(tmp_path):
    store = JobStore(tmp_path / "jobs.sqlite3")
    text = render_metrics(
        store=store, status_path=tmp_path / "s.json", costs_csv=tmp_path / "nope.csv"
    )
    assert "operator_cost_usd_today 0.0000" in text


def test_metrics_route_serves_plaintext(tmp_path):
    status_path = tmp_path / "status.json"
    _seed_status(status_path)
    store = JobStore(tmp_path / "jobs.sqlite3")
    _seed_jobs(store)
    server = serve_http(store, host="127.0.0.1", port=0, status_path=status_path)
    try:
        register_metrics_route(server, store)
        host, port = server.server_address
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(f"http://{host}:{port}/metrics", timeout=5) as resp:  # noqa: S310
                assert resp.status == 200
                ct = resp.headers.get("Content-Type", "")
                assert "text/plain" in ct
                body = resp.read().decode("utf-8")
        finally:
            server.shutdown()
            thread.join(timeout=2)
    finally:
        server.server_close()
        EXTRA_ROUTES.pop(("GET", "/metrics"), None)
    assert "operator_jobs_total" in body
    assert "operator_hook_blocks_total" in body
