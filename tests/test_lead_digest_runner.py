from __future__ import annotations

from operator_core import lead_ledger
from operator_core import demand_os
from operator_core.runner import JobRunner
from operator_core.store import JobStore


def test_runner_handles_lead_digest_job(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def fake_daily_digest(post_discord=False):
        return {
            "sync": {"inserted": 1, "updated": 0, "open_count": 3},
            "digest": {
                "open_count": 3,
                "new_24h": [{"id": "lead_1"}],
                "high_intent_uncontacted": [{"id": "lead_1"}, {"id": "lead_2"}],
                "stale_high_intent": [{"id": "lead_3"}],
                "counts_by_product": {"DealBrain": 1, "AI Ops Consulting": 2},
                "source_errors": [],
            },
            "status": {},
            "text": "Signup Follow-up Digest",
            "posted": False,
        }

    monkeypatch.setattr(lead_ledger, "run_daily_digest", fake_daily_digest)
    job = store.create_job("lead_digest")

    result = JobRunner(store).run(job.id)

    assert result.status == "complete"
    assert result.metadata["lead_digest"]["open_count"] == 3
    assert "high_intent=2" in result.metadata["summary"]


def test_runner_handles_demand_review_job(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def fake_weekly_review(write_file=True):
        return {
            "review": {
                "scoreboard": [{"product": "AI Ops Consulting", "demand_score": 93}],
                "experiments": [{"id": "ai-ops-1"}],
            },
            "status": {"watch_sources": [{"source_table": "ao_waitlist"}]},
            "path": str(tmp_path / "review.md"),
            "text": "Portfolio Demand Review",
        }

    monkeypatch.setattr(demand_os, "run_weekly_review", fake_weekly_review)
    job = store.create_job("demand_review")

    result = JobRunner(store).run(job.id)

    assert result.status == "complete"
    assert result.metadata["demand_review"]["top_product"] == "AI Ops Consulting"
    assert "score=93" in result.metadata["summary"]


def test_runner_handles_nightly_demand_plan_job(tmp_path, monkeypatch):
    store = JobStore(tmp_path / "jobs.sqlite3")

    def fake_nightly_plan(write_file=True):
        return {
            "plan": {
                "focus_product": "DealBrain",
                "active_experiments": [{"id": "dealbrain-1"}],
                "backlog": [{"id": "dealbrain-2"}, {"id": "ai-ops-1"}],
                "watch_sources": [{"source_table": "db_waitlist"}],
                "top_leads": [{"id": "lead_1"}],
            },
            "status": {"focus_product": "DealBrain"},
            "path": str(tmp_path / "nightly.md"),
            "text": "Signup-First Night Plan",
        }

    monkeypatch.setattr(demand_os, "run_nightly_plan", fake_nightly_plan)
    job = store.create_job("nightly_demand_plan")

    result = JobRunner(store).run(job.id)

    assert result.status == "complete"
    assert result.metadata["nightly_demand_plan"]["focus_product"] == "DealBrain"
    assert "running=1" in result.metadata["summary"]
