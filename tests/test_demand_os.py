from __future__ import annotations

from datetime import datetime, timezone

from operator_core.demand_os import (
    ExperimentStore,
    build_broker_close_state,
    build_experiments,
    build_nightly_plan,
    build_review,
    build_scoreboard,
    build_source_health,
    render_journey,
    render_nightly_plan,
    render_review,
    render_scoreboard,
    run_weekly_review,
    run_nightly_plan,
    seed_experiment_backlog,
)
from operator_core.lead_ledger import LeadStore, SourceSpec, normalize_row


def _add_event(store: LeadStore, *, product: str, event_type: str, table: str, row_id: str, email: str, company: str, created_at: str, base: int = 80):
    spec = SourceSpec(
        product=product,
        event_type=event_type,
        table=table,
        select="id,email,company_name,status,created_at,source",
        company_field="company_name",
        base_intent=base,
        next_action="Follow up.",
    )
    event = normalize_row(
        spec,
        {
            "id": row_id,
            "email": email,
            "company_name": company,
            "status": "pending",
            "created_at": created_at,
            "source": "intake_form",
        },
        now=datetime(2026, 4, 24, tzinfo=timezone.utc),
    )
    return store.upsert_event(event)[0]


def test_scoreboard_ranks_product_with_high_intent(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    _add_event(
        store,
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        row_id="r1",
        email="buyer@firm.com",
        company="Firm Co",
        created_at="2026-04-24T01:00:00+00:00",
        base=85,
    )
    _add_event(
        store,
        product="PC Bottleneck Analyzer",
        event_type="email_subscriber",
        table="email_subscribers",
        row_id="p1",
        email="builder@gmail.com",
        company="",
        created_at="2026-04-24T01:00:00+00:00",
        base=25,
    )

    rows = build_scoreboard(store, now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc))
    text = render_scoreboard(rows)

    assert rows[0].product == "DealBrain"
    assert rows[0].high_intent == 1
    assert "Portfolio Demand Scoreboard" in text


def test_source_health_reports_seen_and_watch_paths(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    _add_event(
        store,
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        row_id="r1",
        email="buyer@firm.com",
        company="Firm Co",
        created_at="2026-04-24T01:00:00+00:00",
        base=85,
    )

    rows = build_source_health(store)

    db_report = next(row for row in rows if row.source_table == "db_reports")
    db_waitlist = next(row for row in rows if row.source_table == "db_waitlist")
    assert db_report.health == "ok"
    assert db_waitlist.health == "watch"


def test_experiments_include_next_step(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    _add_event(
        store,
        product="AI Ops Consulting",
        event_type="intake",
        table="ao_leads",
        row_id="a1",
        email="ops@clinic.com",
        company="Clinic Ops",
        created_at="2026-04-24T01:00:00+00:00",
        base=75,
    )

    experiments = build_experiments(store, limit=12)

    assert experiments
    assert any(exp.product == "AI Ops Consulting" for exp in experiments)
    assert all(exp.next_step for exp in experiments)


def test_broker_close_state_merges_audit_context_with_lead_status(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    audit_spec = SourceSpec(
        product="AI Ops Consulting",
        event_type="broker_workflow_audit",
        table="ao_broker_workflow_audits",
        select="id,email,business_name,score,estimated_hours_saved_per_week,recommended_workflow,urgency,created_at",
        company_field="business_name",
        base_intent=90,
        next_action="Open broker audit.",
    )
    lead_spec = SourceSpec(
        product="AI Ops Consulting",
        event_type="intake",
        table="ao_leads",
        select="id,email,business_name,source,status,created_at",
        company_field="business_name",
        base_intent=75,
        next_action="Review lead.",
    )
    audit = normalize_row(
        audit_spec,
        {
            "id": "audit_1",
            "email": "owner@brokerage.com",
            "business_name": "Main Street Deals",
            "score": 91,
            "estimated_hours_saved_per_week": 19,
            "recommended_workflow": "Buyer follow-up queue",
            "urgency": "this-week",
            "created_at": "2026-04-24T01:00:00+00:00",
        },
        now=datetime(2026, 4, 24, tzinfo=timezone.utc),
    )
    synced_lead = normalize_row(
        lead_spec,
        {
            "id": "lead_1",
            "email": "owner@brokerage.com",
            "business_name": "Main Street Deals",
            "source": "broker_workflow_audit",
            "status": "booked",
            "created_at": "2026-04-24T01:05:00+00:00",
        },
        now=datetime(2026, 4, 24, tzinfo=timezone.utc),
    )
    store.upsert_event(audit)
    store.upsert_event(synced_lead)

    state = build_broker_close_state(store, now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc))
    review = build_review(store, now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc))
    text = render_review(review)

    assert state.total == 1
    assert state.booked == 1
    assert state.hot_unworked == 0
    assert state.top_actions == []
    assert review.to_dict()["broker_close_state"]["booked"] == 1
    assert "AI Ops Broker Close State" in text
    assert "Buyer follow-up queue" not in text


def test_journey_and_weekly_review_render(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    lead = _add_event(
        store,
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        row_id="r1",
        email="buyer@firm.com",
        company="Firm Co",
        created_at="2026-04-24T01:00:00+00:00",
        base=85,
    )
    store.add_note(lead.id, "Sent a first question.")

    journey = render_journey(store.get(lead.id))
    review = build_review(store, now=datetime(2026, 4, 24, tzinfo=timezone.utc))
    review_text = render_review(review)

    assert "Lead Journey" in journey
    assert "Suggested draft" in journey
    assert "Portfolio Demand Review" in review_text
    assert "Operating Rule" in review_text


def test_run_weekly_review_writes_artifacts_and_status(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    _add_event(
        store,
        product="AI Ops Consulting",
        event_type="intake",
        table="ao_leads",
        row_id="a1",
        email="ops@clinic.com",
        company="Clinic Ops",
        created_at="2026-04-24T01:00:00+00:00",
        base=75,
    )
    review_path = tmp_path / "review.md"
    status_path = tmp_path / "status.json"

    payload = run_weekly_review(
        store=store,
        write_file=True,
        review_path=review_path,
        status_path=status_path,
    )

    assert payload["path"] == str(review_path)
    assert review_path.exists()
    assert payload["status"]["top_product"] == "AI Ops Consulting"
    assert status_path.exists()


def test_experiment_store_seeds_and_preserves_status(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    experiments = ExperimentStore(store.db_path)
    _add_event(
        store,
        product="AI Ops Consulting",
        event_type="intake",
        table="ao_leads",
        row_id="a1",
        email="ops@clinic.com",
        company="Clinic Ops",
        created_at="2026-04-24T01:00:00+00:00",
        base=75,
    )

    inserted, refreshed = seed_experiment_backlog(store, experiments, limit=5)
    rows = experiments.list(limit=10)
    first = rows[0]
    experiments.mark(first.id, "RUNNING", note="Starting tonight.")
    inserted_again, refreshed_again = seed_experiment_backlog(store, experiments, limit=5)
    running = experiments.get(first.id)

    assert inserted >= 1
    assert refreshed == 0
    assert inserted_again == 0
    assert refreshed_again >= 0
    assert running.status == "RUNNING"
    assert "Starting tonight" in running.notes_md


def test_nightly_plan_renders_and_writes(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    experiments = ExperimentStore(store.db_path)
    _add_event(
        store,
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        row_id="r1",
        email="buyer@firm.com",
        company="Firm Co",
        created_at="2026-04-24T01:00:00+00:00",
        base=85,
    )
    plan_path = tmp_path / "nightly.md"
    status_path = tmp_path / "status.json"

    plan = build_nightly_plan(store, experiments, now=datetime(2026, 4, 24, tzinfo=timezone.utc))
    text = render_nightly_plan(plan)
    payload = run_nightly_plan(
        store=store,
        experiment_store=experiments,
        write_file=True,
        plan_path=plan_path,
        status_path=status_path,
    )

    assert plan.focus_product == "DealBrain"
    assert "Signup-First Night Plan" in text
    assert payload["path"] == str(plan_path)
    assert plan_path.exists()
    assert payload["status"]["focus_product"] == "DealBrain"
