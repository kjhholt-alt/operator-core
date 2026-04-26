from __future__ import annotations

from datetime import datetime, timezone

from operator_core.lead_ledger import (
    LeadStore,
    SourceSpec,
    SOURCE_SPECS,
    build_digest_report,
    collect_events,
    draft_for_lead,
    normalize_row,
    render_digest_report,
    render_digest,
    render_text,
    run_daily_digest,
    score_event,
    sync_leads,
)


class FakeClient:
    def __init__(self, rows_by_table):
        self.rows_by_table = rows_by_table

    def select_recent(self, spec, since_iso, limit=25):
        return self.rows_by_table.get(spec.table, [])[:limit]


def test_score_event_rewards_high_intent_and_business_email():
    now = datetime(2026, 4, 24, tzinfo=timezone.utc)
    spec = SourceSpec(
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        select="id,email,company_name,status,created_at",
        company_field="company_name",
        base_intent=85,
    )
    event = normalize_row(
        spec,
        {
            "id": "r1",
            "email": "buyer@wrightadvisors.com",
            "company_name": "Wright Advisors",
            "status": "pending",
            "created_at": "2026-04-24T00:00:00+00:00",
        },
        now=now,
    )

    assert score_event(event, now=now) == 100


def test_collect_events_sorts_by_intent_score():
    client = FakeClient(
        {
            "db_waitlist": [
                {
                    "id": "w1",
                    "email": "person@gmail.com",
                    "name": None,
                    "created_at": "2026-04-24T00:00:00+00:00",
                }
            ],
            "db_reports": [
                {
                    "id": "r1",
                    "email": "buyer@firm.com",
                    "company_name": "Firm Co",
                    "status": "pending",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )

    report = collect_events(
        client,
        now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc),
        per_source_limit=10,
    )

    assert report.events[0].event_type == "report_intake"
    assert report.events[0].intent_score > report.events[1].intent_score
    assert report.counts_by_product["DealBrain"] == 2


def test_ai_ops_waitlist_preserves_source_context():
    spec = next(item for item in SOURCE_SPECS if item.table == "ao_waitlist")
    event = normalize_row(
        spec,
        {
            "id": "ao_w1",
            "email": "owner@example.com",
            "business_name": "Ops Co",
            "industry": "services",
            "source": "landing_waitlist",
            "page_path": "/?utm_campaign=ops",
            "utm_campaign": "ops",
            "created_at": "2026-04-24T01:00:00+00:00",
        },
        now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc),
    )

    assert "source" in spec.select
    assert event.source == "ao_waitlist"
    assert event.metadata["source"] == "landing_waitlist"
    assert event.metadata["page_path"] == "/?utm_campaign=ops"
    assert event.metadata["utm_campaign"] == "ops"


def test_prospector_pro_sources_match_product_capture_paths():
    waitlist_spec = next(item for item in SOURCE_SPECS if item.table == "pp_waitlist")
    intake_spec = next(item for item in SOURCE_SPECS if item.table == "pp_intake")
    saved_lead_spec = next(item for item in SOURCE_SPECS if item.table == "pp_leads")

    event = normalize_row(
        intake_spec,
        {
            "id": "pp_i1",
            "email": "growth@agency.com",
            "company_name": "Growth Agency",
            "contact_name": "Jordan",
            "vertical_interest": "Dental Offices",
            "cities_interest": "Austin TX",
            "team_size": "2-5",
            "current_process": "Google Maps plus spreadsheets",
            "monthly_lead_volume": "50-200 / month",
            "contacted": False,
            "created_at": "2026-04-24T01:00:00+00:00",
        },
        now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc),
    )

    assert waitlist_spec.event_type == "waitlist"
    assert intake_spec.event_type == "intake"
    assert intake_spec.table == "pp_intake"
    assert saved_lead_spec.event_type == "saved_lead"
    assert event.company == "Growth Agency"
    assert event.metadata["vertical_interest"] == "Dental Offices"
    assert event.metadata["current_process"] == "Google Maps plus spreadsheets"


def test_render_text_includes_next_action():
    client = FakeClient(
        {
            "vr_waitlist": [
                {
                    "id": "v1",
                    "email": "owner@example.com",
                    "business_name": "Example Dental",
                    "created_at": "2026-04-24T00:00:00+00:00",
                }
            ]
        }
    )
    report = collect_events(client, now=datetime(2026, 4, 24, tzinfo=timezone.utc))

    text = render_text(report)

    assert "Example Dental" in text
    assert "next:" in text


def test_sync_persists_open_queue_and_preserves_status(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "db_reports": [
                {
                    "id": "r1",
                    "email": "buyer@firm.com",
                    "company_name": "Firm Co",
                    "status": "pending",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )

    result, warning = sync_leads(
        store=store,
        client=client,
        window_hours=168,
        per_source_limit=10,
    )

    assert warning is None
    assert result.inserted == 1
    assert result.updated == 0
    [lead] = store.list()
    assert lead.product == "DealBrain"
    assert lead.status == "NEW"

    marked = store.mark(lead.id, "contacted", note="Sent sample report.")
    assert marked.status == "CONTACTED"
    assert "Sent sample report" in marked.notes_md

    result, _ = sync_leads(store=store, client=client, window_hours=168)
    assert result.inserted == 0
    assert result.updated == 1
    assert store.get(lead.id).status == "CONTACTED"


def test_digest_renders_persistent_queue(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "ao_leads": [
                {
                    "id": "a1",
                    "email": "ops@clinic.com",
                    "business_name": "Clinic Ops",
                    "source": "intake_form",
                    "status": "new",
                    "industry": "healthcare",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )
    sync_leads(store=store, client=client)

    digest = render_digest(store.list(), open_count=store.count_open())

    assert "Open signup follow-up queue: 1" in digest
    assert "Clinic Ops" in digest


def test_broker_workflow_audit_syncs_as_hot_ai_ops_lead(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "ao_broker_workflow_audits": [
                {
                    "id": "bb1",
                    "email": "owner@brokerage.com",
                    "business_name": "Main Street Deals",
                    "score": 88,
                    "estimated_hours_saved_per_week": 18,
                    "recommended_workflow": "Buyer follow-up queue",
                    "urgency": "this-week",
                    "messy_workflows": ["buyer-followup", "seller-intake"],
                    "seller_intake": "painful",
                    "buyer_followup": "breaking",
                    "reporting_drag": "noticeable",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )

    sync_leads(store=store, client=client)
    [lead] = store.list(product="AI Ops Consulting", event_type="broker_workflow_audit")
    draft = draft_for_lead(lead)

    assert lead.intent_score == 100
    assert lead.company == "Main Street Deals"
    assert lead.metadata["recommended_workflow"] == "Buyer follow-up queue"
    assert "Broker workflow audit" in draft
    assert "recommended_workflow" in draft


def test_list_filters_and_draft_generation(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "db_reports": [
                {
                    "id": "r1",
                    "email": "buyer@firm.com",
                    "company_name": "Firm Co",
                    "status": "pending",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
            "email_subscribers": [
                {
                    "id": "p1",
                    "email": "builder@gmail.com",
                    "source": "blog",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )
    sync_leads(store=store, client=client)

    high_intent = store.list(min_score=80)

    assert len(high_intent) == 1
    assert high_intent[0].product == "DealBrain"
    draft = draft_for_lead(high_intent[0])
    assert "Subject:" in draft
    assert "DealBrain" in draft
    assert "What kind of deal" in draft


def test_dry_run_counts_without_writing(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "db_reports": [
                {
                    "id": "r1",
                    "email": "buyer@firm.com",
                    "company_name": "Firm Co",
                    "status": "pending",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )

    result, _ = sync_leads(store=store, client=client, dry_run=True)

    assert result.dry_run is True
    assert result.inserted == 1
    assert store.count_open() == 0


def test_digest_report_sections_and_status_metrics(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    client = FakeClient(
        {
            "db_reports": [
                {
                    "id": "fresh",
                    "email": "buyer@firm.com",
                    "company_name": "Firm Co",
                    "status": "pending",
                    "created_at": "2026-04-24T01:00:00+00:00",
                },
                {
                    "id": "stale",
                    "email": "old@firm.com",
                    "company_name": "Old Firm",
                    "status": "pending",
                    "created_at": "2026-04-20T01:00:00+00:00",
                },
            ],
        }
    )
    sync_leads(store=store, client=client)

    report = build_digest_report(
        store,
        now=datetime(2026, 4, 24, 2, tzinfo=timezone.utc),
        source_errors=["vr_waitlist missing"],
    )
    text = render_digest_report(report)

    assert report.open_count == 2
    assert len(report.new_24h) == 1
    assert len(report.stale_high_intent) == 1
    assert "Source warnings" in text
    assert "Stale hot leads" in text


def test_run_daily_digest_writes_status_metrics(tmp_path):
    store = LeadStore(tmp_path / "leads.sqlite3")
    status_path = tmp_path / "status.json"
    client = FakeClient(
        {
            "ao_leads": [
                {
                    "id": "a1",
                    "email": "ops@clinic.com",
                    "business_name": "Clinic Ops",
                    "source": "intake_form",
                    "status": "new",
                    "industry": "healthcare",
                    "created_at": "2026-04-24T01:00:00+00:00",
                }
            ],
        }
    )

    payload = run_daily_digest(
        store=store,
        client=client,
        status_path=status_path,
        post_discord=False,
    )

    assert payload["status"]["open_count"] == 1
    assert "Signup Follow-up Digest" in payload["text"]
    assert status_path.exists()
