"""Portfolio lead ledger.

Signup-first operating layer across the product portfolio. This module reads
the existing product-specific Supabase tables, normalizes them into one queue,
and stores follow-up state locally so Kruz can work the queue over time.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

from .settings import ConfigError, load_settings


load_dotenv()

OPEN_STATUSES = {"NEW", "CONTACTED", "FOLLOW_UP", "NURTURE"}
FINAL_STATUSES = {"WON", "LOST", "CLOSED"}
VALID_STATUSES = OPEN_STATUSES | FINAL_STATUSES


@dataclass(frozen=True)
class SourceSpec:
    product: str
    event_type: str
    table: str
    select: str
    email_field: str = "email"
    company_field: str | None = None
    created_field: str = "created_at"
    source_label: str = ""
    base_intent: int = 10
    next_action: str = "Review and decide follow-up."


@dataclass
class LeadEvent:
    product: str
    event_type: str
    table: str
    row_id: str
    email: str | None
    company: str | None
    created_at: str | None
    source: str
    status: str | None = None
    next_action: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    intent_score: int = 0


@dataclass
class LedgerReport:
    generated_at: str
    window_hours: int
    events: list[LeadEvent]
    counts_by_product: dict[str, int]
    counts_by_type: dict[str, int]
    errors: list[str]


@dataclass(frozen=True)
class LeadRecord:
    id: str
    event_key: str
    product: str
    event_type: str
    source_table: str
    source_row_id: str
    email: str | None
    company: str | None
    event_created_at: str | None
    first_seen_at: str
    last_seen_at: str
    status: str
    intent_score: int
    next_action: str
    notes_md: str
    metadata: dict[str, Any]
    last_contacted_at: str | None
    follow_up_at: str | None
    updated_at: str


@dataclass(frozen=True)
class SyncResult:
    report: LedgerReport
    inserted: int
    updated: int
    open_count: int
    dry_run: bool = False


@dataclass(frozen=True)
class DigestReport:
    generated_at: str
    open_count: int
    new_24h: list[LeadRecord]
    high_intent_uncontacted: list[LeadRecord]
    stale_high_intent: list[LeadRecord]
    top_queue: list[LeadRecord]
    counts_by_product: dict[str, int]
    source_errors: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "open_count": self.open_count,
            "new_24h": [_record_to_dict(r) for r in self.new_24h],
            "high_intent_uncontacted": [_record_to_dict(r) for r in self.high_intent_uncontacted],
            "stale_high_intent": [_record_to_dict(r) for r in self.stale_high_intent],
            "top_queue": [_record_to_dict(r) for r in self.top_queue],
            "counts_by_product": self.counts_by_product,
            "source_errors": self.source_errors,
        }


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        product="DealBrain",
        event_type="waitlist",
        table="db_waitlist",
        select="id,email,name,created_at,nurture_step,last_email_sent_at",
        company_field="name",
        base_intent=45,
        next_action="If business fit is clear, send sample report and ask what deal type they evaluate.",
    ),
    SourceSpec(
        product="DealBrain",
        event_type="report_intake",
        table="db_reports",
        select="id,email,company_name,status,created_at",
        company_field="company_name",
        base_intent=85,
        next_action="Follow up personally; they started a report/intake flow.",
    ),
    SourceSpec(
        product="AI Voice Receptionist",
        event_type="waitlist",
        table="vr_waitlist",
        select="id,email,business_name,created_at,nurture_sent_at,unsubscribed_at",
        company_field="business_name",
        base_intent=45,
        next_action="Offer a short demo and ask what calls they miss today.",
    ),
    SourceSpec(
        product="AI Voice Receptionist",
        event_type="demo_call",
        table="vr_calls",
        select="id,caller_number,business_id,outcome,created_at,demo_trigger_sent_at",
        email_field="caller_number",
        company_field="business_id",
        base_intent=80,
        next_action="Follow up same day; demo usage is high intent.",
    ),
    SourceSpec(
        product="Prospector Pro",
        event_type="waitlist",
        table="pp_waitlist",
        select="id,email,company_name,use_case,created_at,nurture_step,nurture_next_at",
        company_field="company_name",
        base_intent=50,
        next_action="Ask what vertical/city they want to prospect first.",
    ),
    SourceSpec(
        product="Prospector Pro",
        event_type="intake",
        table="pp_intake",
        select=(
            "id,email,company_name,contact_name,vertical_interest,cities_interest,"
            "team_size,current_process,monthly_lead_volume,contacted,created_at"
        ),
        company_field="company_name",
        base_intent=70,
        next_action="Follow up personally; they submitted the qualified early-access intake form.",
    ),
    SourceSpec(
        product="Prospector Pro",
        event_type="saved_lead",
        table="pp_leads",
        select="id,email,business_name,status,created_at,source",
        company_field="business_name",
        base_intent=45,
        next_action="Review saved lead/use case if Prospector Pro usage becomes the active lane.",
    ),
    SourceSpec(
        product="AI Ops Consulting",
        event_type="waitlist",
        table="ao_waitlist",
        select="id,email,business_name,industry,source,page_path,utm_campaign,created_at",
        company_field="business_name",
        base_intent=45,
        next_action="Send audit offer or ask one workflow-discovery question.",
    ),
    SourceSpec(
        product="AI Ops Consulting",
        event_type="intake",
        table="ao_leads",
        select="id,email,business_name,source,status,industry,created_at",
        company_field="business_name",
        base_intent=75,
        next_action="If source is intake/audit/reply, prepare direct follow-up.",
    ),
    SourceSpec(
        product="AI Ops Consulting",
        event_type="broker_workflow_audit",
        table="ao_broker_workflow_audits",
        select=(
            "id,email,business_name,score,estimated_hours_saved_per_week,"
            "recommended_workflow,urgency,messy_workflows,seller_intake,"
            "buyer_followup,reporting_drag,created_at"
        ),
        company_field="business_name",
        base_intent=90,
        next_action="Open the broker audit detail, personalize the workflow draft, and ask for a 15-minute pressure test.",
    ),
    SourceSpec(
        product="AI Ops Consulting",
        event_type="audit_unlock",
        table="op_audits",
        select="id,email,business_name,status,created_at,share_slug",
        company_field="business_name",
        base_intent=80,
        next_action="Send audit-specific follow-up and offer a short call.",
    ),
    SourceSpec(
        product="PC Bottleneck Analyzer",
        event_type="email_subscriber",
        table="email_subscribers",
        select="id,email,source,created_at",
        base_intent=25,
        next_action="Keep in nurture; no payment CTA yet.",
    ),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _age_hours(created_at: str | None, now: datetime) -> float | None:
    ts = _parse_time(created_at)
    if ts is None:
        return None
    return max(0.0, (now - ts).total_seconds() / 3600)


def _event_key(event: LeadEvent) -> str:
    return f"{event.product}:{event.table}:{event.row_id}"


def _lead_id(event_key: str) -> str:
    digest = hashlib.sha1(event_key.encode("utf-8")).hexdigest()
    return f"lead_{digest[:12]}"


def _default_db_path() -> Path:
    try:
        settings = load_settings()
        return settings.data_dir / "lead-ledger.sqlite3"
    except ConfigError:
        return Path.home() / ".operator" / "data" / "lead-ledger.sqlite3"


def score_event(event: LeadEvent, now: datetime | None = None) -> int:
    """Return a 0..100 intent score for a normalized event."""
    now = now or datetime.now(timezone.utc)
    base = int(event.intent_score)

    if event.email and "@" in event.email:
        base += 5
        domain = event.email.split("@", 1)[1].lower()
        consumer_domains = {
            "gmail.com",
            "yahoo.com",
            "outlook.com",
            "hotmail.com",
            "icloud.com",
        }
        if domain not in consumer_domains:
            base += 8
    if event.company:
        base += 7
    if event.event_type in {"report_intake", "demo_call", "audit_unlock", "broker_workflow_audit"}:
        base += 10
    if event.status in {"replied", "paid", "completed"}:
        base += 10
    if event.metadata.get("source") in {"intake_form", "audit_lead", "pp_warm", "broker_workflow_audit"}:
        base += 8
    if event.event_type == "broker_workflow_audit":
        try:
            base += min(10, max(0, int(event.metadata.get("score") or 0) // 10))
        except (TypeError, ValueError):
            pass
        if event.metadata.get("urgency") in {"this-week", "this-month"}:
            base += 8

    age = _age_hours(event.created_at, now)
    if age is not None:
        if age <= 24:
            base += 8
        elif age > 7 * 24:
            base -= 10

    return max(0, min(100, base))


def normalize_row(spec: SourceSpec, row: dict[str, Any], now: datetime | None = None) -> LeadEvent:
    """Convert one Supabase row into a LeadEvent."""
    email = row.get(spec.email_field)
    company = row.get(spec.company_field) if spec.company_field else None
    excluded_keys = {"id", spec.email_field}
    if spec.company_field:
        excluded_keys.add(spec.company_field)
    event = LeadEvent(
        product=spec.product,
        event_type=spec.event_type,
        table=spec.table,
        row_id=str(row.get("id") or ""),
        email=str(email).strip().lower() if email else None,
        company=str(company).strip() if company else None,
        created_at=row.get(spec.created_field),
        source=spec.source_label or spec.table,
        status=row.get("status"),
        next_action=spec.next_action,
        metadata={k: v for k, v in row.items() if k not in excluded_keys},
        intent_score=spec.base_intent,
    )
    event.intent_score = score_event(event, now=now)
    return event


class SupabaseRestClient:
    """Tiny Supabase REST reader using requests."""

    def __init__(self, url: str, key: str) -> None:
        self.url = url.rstrip("/")
        self.key = key

    @classmethod
    def from_env(cls) -> "SupabaseRestClient | None":
        url = os.environ.get("SUPABASE_URL") or os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
        key = (
            os.environ.get("SUPABASE_KEY")
            or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
            or os.environ.get("SUPABASE_SERVICE_KEY")
        )
        if not url or not key:
            return None
        return cls(url=url, key=key)

    def select_recent(self, spec: SourceSpec, since_iso: str, limit: int = 25) -> list[dict[str, Any]]:
        endpoint = f"{self.url}/rest/v1/{spec.table}"
        params = {
            "select": spec.select,
            spec.created_field: f"gte.{since_iso}",
            "order": f"{spec.created_field}.desc",
            "limit": str(limit),
        }
        headers = self._headers()
        resp = requests.get(endpoint, params=params, headers=headers, timeout=12)
        if resp.status_code >= 400:
            raise RuntimeError(f"{spec.table}: HTTP {resp.status_code} {resp.text[:180]}")
        data = resp.json()
        return data if isinstance(data, list) else []

    def upsert_operator_leads(self, records: list[LeadRecord]) -> tuple[int, str | None]:
        """Best-effort mirror into a Supabase `operator_leads` table."""
        if not records:
            return (0, None)
        endpoint = f"{self.url}/rest/v1/operator_leads"
        payload = [_remote_payload(record) for record in records]
        headers = self._headers()
        headers["Prefer"] = "resolution=merge-duplicates"
        resp = requests.post(
            endpoint,
            params={"on_conflict": "id"},
            headers=headers,
            json=payload,
            timeout=20,
        )
        if resp.status_code >= 400:
            return (0, f"operator_leads mirror failed: HTTP {resp.status_code} {resp.text[:180]}")
        return (len(records), None)

    def _headers(self) -> dict[str, str]:
        return {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }


class LeadStore:
    """Persistent lead queue state."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or _default_db_path()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leads (
                    id TEXT PRIMARY KEY,
                    event_key TEXT NOT NULL UNIQUE,
                    product TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    source_table TEXT NOT NULL,
                    source_row_id TEXT NOT NULL,
                    email TEXT,
                    company TEXT,
                    event_created_at TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'NEW',
                    intent_score INTEGER NOT NULL DEFAULT 0,
                    next_action TEXT NOT NULL DEFAULT '',
                    notes_md TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    last_contacted_at TEXT,
                    follow_up_at TEXT,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leads_status_score ON leads(status, intent_score DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_leads_email ON leads(email)"
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS lead_notes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    lead_id TEXT NOT NULL,
                    body_md TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )

    def upsert_event(self, event: LeadEvent, *, now: str | None = None) -> tuple[LeadRecord, bool]:
        now = now or _now()
        event_key = _event_key(event)
        lead_id = _lead_id(event_key)
        metadata_json = json.dumps(event.metadata, sort_keys=True)
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT id FROM leads WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            inserted = existing is None
            conn.execute(
                """
                INSERT INTO leads (
                    id, event_key, product, event_type, source_table, source_row_id,
                    email, company, event_created_at, first_seen_at, last_seen_at,
                    status, intent_score, next_action, metadata_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'NEW', ?, ?, ?, ?)
                ON CONFLICT(event_key) DO UPDATE SET
                    product = excluded.product,
                    event_type = excluded.event_type,
                    source_table = excluded.source_table,
                    source_row_id = excluded.source_row_id,
                    email = excluded.email,
                    company = excluded.company,
                    event_created_at = excluded.event_created_at,
                    last_seen_at = excluded.last_seen_at,
                    intent_score = excluded.intent_score,
                    next_action = excluded.next_action,
                    metadata_json = excluded.metadata_json,
                    updated_at = excluded.updated_at
                """,
                (
                    lead_id,
                    event_key,
                    event.product,
                    event.event_type,
                    event.table,
                    event.row_id,
                    event.email,
                    event.company,
                    event.created_at,
                    now,
                    now,
                    event.intent_score,
                    event.next_action,
                    metadata_json,
                    now,
                ),
            )
        return (self.get(lead_id), inserted)

    def sync_events(self, events: list[LeadEvent], *, now: str | None = None) -> tuple[int, int]:
        inserted = 0
        updated = 0
        for event in events:
            _, is_new = self.upsert_event(event, now=now)
            if is_new:
                inserted += 1
            else:
                updated += 1
        return inserted, updated

    def sync_plan(self, events: list[LeadEvent]) -> tuple[int, int]:
        """Return how many events would insert/update without writing."""
        inserted = 0
        updated = 0
        with self._connect() as conn:
            for event in events:
                row = conn.execute(
                    "SELECT id FROM leads WHERE event_key = ?",
                    (_event_key(event),),
                ).fetchone()
                if row is None:
                    inserted += 1
                else:
                    updated += 1
        return inserted, updated

    def list(
        self,
        *,
        status: str | None = None,
        open_only: bool = True,
        limit: int = 20,
        product: str | None = None,
        event_type: str | None = None,
        min_score: int | None = None,
    ) -> list[LeadRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(_normalize_status(status))
        elif open_only:
            clauses.append("status IN (?, ?, ?, ?)")
            params.extend(sorted(OPEN_STATUSES))
        if product:
            clauses.append("lower(product) = lower(?)")
            params.append(product)
        if event_type:
            clauses.append("event_type = ?")
            params.append(event_type)
        if min_score is not None:
            clauses.append("intent_score >= ?")
            params.append(min_score)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        sql = (
            "SELECT * FROM leads"
            f"{where}"
            " ORDER BY intent_score DESC, COALESCE(event_created_at, first_seen_at) DESC"
            " LIMIT ?"
        )
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_record_from_row(row) for row in rows]

    def count_open(self) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM leads WHERE status IN (?, ?, ?, ?)",
                sorted(OPEN_STATUSES),
            ).fetchone()
        return int(row["n"])

    def counts_by_product(self, *, open_only: bool = True) -> dict[str, int]:
        where = ""
        params: list[Any] = []
        if open_only:
            where = "WHERE status IN (?, ?, ?, ?)"
            params.extend(sorted(OPEN_STATUSES))
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT product, COUNT(*) AS n FROM leads {where} GROUP BY product ORDER BY product",
                params,
            ).fetchall()
        return {str(row["product"]): int(row["n"]) for row in rows}

    def get(self, lead_ref: str) -> LeadRecord:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM leads
                WHERE id = ? OR event_key = ? OR email = ?
                ORDER BY intent_score DESC
                LIMIT 1
                """,
                (lead_ref, lead_ref, lead_ref),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown lead: {lead_ref}")
        return _record_from_row(row)

    def mark(
        self,
        lead_ref: str,
        status: str,
        *,
        note: str | None = None,
        follow_up_at: str | None = None,
    ) -> LeadRecord:
        record = self.get(lead_ref)
        normalized = _normalize_status(status)
        now = _now()
        last_contacted_at = now if normalized in {"CONTACTED", "FOLLOW_UP", "WON"} else record.last_contacted_at
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE leads
                SET status = ?, last_contacted_at = ?, follow_up_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized, last_contacted_at, follow_up_at or record.follow_up_at, now, record.id),
            )
        if note:
            self.add_note(record.id, note)
        return self.get(record.id)

    def add_note(self, lead_ref: str, body_md: str) -> LeadRecord:
        record = self.get(lead_ref)
        now = _now()
        note = body_md.strip()
        if not note:
            return record
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO lead_notes (lead_id, body_md, created_at) VALUES (?, ?, ?)",
                (record.id, note, now),
            )
            notes_md = (record.notes_md.rstrip() + f"\n\n[{now[:16]}] {note}").strip()
            conn.execute(
                "UPDATE leads SET notes_md = ?, updated_at = ? WHERE id = ?",
                (notes_md, now, record.id),
            )
        return self.get(record.id)


def collect_events(
    client: SupabaseRestClient | None = None,
    *,
    window_hours: int = 168,
    per_source_limit: int = 25,
    now: datetime | None = None,
) -> LedgerReport:
    now = now or datetime.now(timezone.utc)
    since = (now - timedelta(hours=window_hours)).isoformat()
    client = client if client is not None else SupabaseRestClient.from_env()

    events: list[LeadEvent] = []
    errors: list[str] = []
    if client is None:
        errors.append("Supabase env missing: set SUPABASE_URL and SUPABASE_KEY or SUPABASE_SERVICE_ROLE_KEY.")
    else:
        for spec in SOURCE_SPECS:
            try:
                rows = client.select_recent(spec, since, limit=per_source_limit)
            except Exception as exc:  # noqa: BLE001 - report should be best-effort.
                errors.append(str(exc))
                continue
            events.extend(normalize_row(spec, row, now=now) for row in rows)

    events.sort(key=lambda event: (event.intent_score, event.created_at or ""), reverse=True)
    counts_by_product: dict[str, int] = {}
    counts_by_type: dict[str, int] = {}
    for event in events:
        counts_by_product[event.product] = counts_by_product.get(event.product, 0) + 1
        counts_by_type[event.event_type] = counts_by_type.get(event.event_type, 0) + 1

    return LedgerReport(
        generated_at=now.isoformat(),
        window_hours=window_hours,
        events=events,
        counts_by_product=counts_by_product,
        counts_by_type=counts_by_type,
        errors=errors,
    )


def sync_leads(
    *,
    store: LeadStore | None = None,
    client: SupabaseRestClient | None = None,
    window_hours: int = 168,
    per_source_limit: int = 25,
    mirror_supabase: bool = False,
    dry_run: bool = False,
) -> tuple[SyncResult, str | None]:
    store = store or LeadStore()
    report = collect_events(client, window_hours=window_hours, per_source_limit=per_source_limit)
    if dry_run:
        inserted, updated = store.sync_plan(report.events)
    else:
        inserted, updated = store.sync_events(report.events)
    mirror_warning = None
    if mirror_supabase and client is not None and not dry_run:
        records = store.list(open_only=False, limit=max(len(report.events), 1_000))
        _, mirror_warning = client.upsert_operator_leads(records)
    return (
        SyncResult(
            report=report,
            inserted=inserted,
            updated=updated,
            open_count=store.count_open(),
            dry_run=dry_run,
        ),
        mirror_warning,
    )


def render_text(report: LedgerReport, *, limit: int = 20) -> str:
    lines: list[str] = []
    lines.append(f"Portfolio Lead Ledger -- {report.generated_at[:16].replace('T', ' ')} UTC")
    lines.append("=" * 78)
    lines.append(f"Window: last {report.window_hours}h | events: {len(report.events)}")

    if report.counts_by_product:
        parts = [f"{product}={count}" for product, count in sorted(report.counts_by_product.items())]
        lines.append("Products: " + ", ".join(parts))
    if report.counts_by_type:
        parts = [f"{kind}={count}" for kind, count in sorted(report.counts_by_type.items())]
        lines.append("Types: " + ", ".join(parts))

    if report.errors:
        lines.append("")
        lines.append("Warnings:")
        for err in report.errors[:8]:
            lines.append(f"  - {err}")

    lines.append("")
    lines.append("Highest intent events:")
    if not report.events:
        lines.append("  (no recent lead events found)")
    for i, event in enumerate(report.events[:limit], 1):
        age = _age_hours(event.created_at, datetime.now(timezone.utc))
        age_text = "unknown age" if age is None else f"{age:.0f}h ago"
        email = event.email or "no email"
        company = event.company or "-"
        lines.append(
            f"  {i:2}. [{event.intent_score:3}] {event.product} / {event.event_type} / "
            f"{company} / {email} / {age_text}"
        )
        lines.append(f"      next: {event.next_action}")
    return "\n".join(lines)


def render_queue(records: list[LeadRecord], *, title: str = "Open lead queue") -> str:
    lines = [title, "=" * 78]
    if not records:
        lines.append("(empty)")
        return "\n".join(lines)
    for i, record in enumerate(records, 1):
        who = record.company or record.email or "-"
        email = record.email or "no email"
        created = (record.event_created_at or record.first_seen_at)[:10]
        lines.append(
            f"{i:2}. [{record.intent_score:3}] {record.status:<9} {record.product} / "
            f"{record.event_type} / {who} / {email} / {created}"
        )
        lines.append(f"    id: {record.id}")
        lines.append(f"    next: {record.next_action}")
        if record.follow_up_at:
            lines.append(f"    follow-up: {record.follow_up_at}")
    return "\n".join(lines)


def render_record(record: LeadRecord) -> str:
    lines = [
        f"Lead {record.id}",
        "=" * 78,
        f"Product:       {record.product}",
        f"Event:         {record.event_type} ({record.source_table}:{record.source_row_id})",
        f"Status:        {record.status}",
        f"Intent score:  {record.intent_score}",
        f"Company:       {record.company or '-'}",
        f"Email:         {record.email or '-'}",
        f"Created:       {record.event_created_at or '-'}",
        f"First seen:    {record.first_seen_at}",
        f"Last seen:     {record.last_seen_at}",
        f"Last contact:  {record.last_contacted_at or '-'}",
        f"Follow-up:     {record.follow_up_at or '-'}",
        "",
        "Next action:",
        record.next_action or "-",
    ]
    if record.notes_md:
        lines.extend(["", "Notes:", record.notes_md])
    if record.metadata:
        lines.extend(["", "Metadata:", json.dumps(record.metadata, indent=2, sort_keys=True)])
    return "\n".join(lines)


def render_digest(records: list[LeadRecord], *, open_count: int, limit: int = 10) -> str:
    lines = [
        f"Open signup follow-up queue: {open_count}",
        "",
        "Tonight's top follow-ups:",
    ]
    if not records:
        lines.append("_No open leads in the local queue._")
    for i, record in enumerate(records[:limit], 1):
        who = record.company or record.email or "-"
        email = record.email or "no email"
        lines.append(
            f"`{i:02}` **[{record.intent_score}] {record.product}** {record.event_type} - "
            f"{who} | `{email}` | `{record.status}`"
        )
        lines.append(f"     {record.next_action}")
    return "\n".join(lines)[:3900]


def build_digest_report(
    store: LeadStore,
    *,
    source_errors: list[str] | None = None,
    limit: int = 10,
    now: datetime | None = None,
) -> DigestReport:
    now = now or datetime.now(timezone.utc)
    open_records = store.list(open_only=True, limit=10_000)
    new_cutoff = now - timedelta(hours=24)
    stale_cutoff = now - timedelta(hours=48)

    def seen_at(record: LeadRecord) -> datetime | None:
        return _parse_time(record.event_created_at) or _parse_time(record.first_seen_at)

    new_24h = [
        record for record in open_records
        if (seen_at(record) is not None and seen_at(record) >= new_cutoff)
    ]
    high_intent_uncontacted = [
        record for record in open_records
        if record.intent_score >= 80 and not record.last_contacted_at
    ]
    stale_high_intent = [
        record for record in open_records
        if (
            record.intent_score >= 75
            and not record.last_contacted_at
            and seen_at(record) is not None
            and seen_at(record) <= stale_cutoff
        )
    ]
    key = lambda r: (r.intent_score, r.event_created_at or r.first_seen_at)
    new_24h.sort(key=key, reverse=True)
    high_intent_uncontacted.sort(key=key, reverse=True)
    stale_high_intent.sort(key=key, reverse=True)

    return DigestReport(
        generated_at=now.isoformat(),
        open_count=store.count_open(),
        new_24h=new_24h[:limit],
        high_intent_uncontacted=high_intent_uncontacted[:limit],
        stale_high_intent=stale_high_intent[:limit],
        top_queue=open_records[:limit],
        counts_by_product=store.counts_by_product(),
        source_errors=list(source_errors or []),
    )


def render_digest_report(report: DigestReport, *, limit: int = 10) -> str:
    lines: list[str] = [
        f"Signup Follow-up Digest -- {report.generated_at[:16].replace('T', ' ')} UTC",
        "=" * 78,
        f"Open queue: {report.open_count}",
    ]
    if report.counts_by_product:
        product_counts = ", ".join(
            f"{product}={count}" for product, count in sorted(report.counts_by_product.items())
        )
        lines.append(f"Products: {product_counts}")

    if report.source_errors:
        lines.extend(["", "Source warnings:"])
        for err in report.source_errors[:5]:
            lines.append(f"  - {err}")

    _append_digest_section(lines, "New in last 24h", report.new_24h, limit)
    _append_digest_section(lines, "High-intent uncontacted", report.high_intent_uncontacted, limit)
    _append_digest_section(lines, "Stale hot leads (>48h)", report.stale_high_intent, limit)
    _append_digest_section(lines, "Top queue", report.top_queue, limit)
    return "\n".join(lines)


def render_digest_discord(report: DigestReport, *, limit: int = 8) -> str:
    lines: list[str] = [f"Open queue: **{report.open_count}**"]
    if report.counts_by_product:
        lines.append(
            "Products: "
            + ", ".join(f"**{product}** {count}" for product, count in sorted(report.counts_by_product.items()))
        )
    _append_digest_section(lines, "New in last 24h", report.new_24h, limit, discord=True)
    _append_digest_section(lines, "High-intent uncontacted", report.high_intent_uncontacted, limit, discord=True)
    _append_digest_section(lines, "Stale hot leads (>48h)", report.stale_high_intent, limit, discord=True)
    if report.source_errors:
        lines.append("")
        lines.append("**Source warnings**")
        for err in report.source_errors[:4]:
            lines.append(f"- `{err[:140]}`")
    return "\n".join(lines)[:3900]


def _append_digest_section(
    lines: list[str],
    title: str,
    records: list[LeadRecord],
    limit: int,
    *,
    discord: bool = False,
) -> None:
    lines.append("")
    lines.append(f"**{title}**" if discord else title)
    if not records:
        lines.append("_None._" if discord else "  (none)")
        return
    for i, record in enumerate(records[:limit], 1):
        who = record.company or record.email or "-"
        email = record.email or "no email"
        if discord:
            lines.append(
                f"`{i:02}` **[{record.intent_score}] {record.product}** {record.event_type} - "
                f"{who} | `{email}` | `{record.status}`"
            )
            lines.append(f"     {record.next_action}")
        else:
            lines.append(
                f"  {i:2}. [{record.intent_score:3}] {record.status:<9} {record.product} / "
                f"{record.event_type} / {who} / {email}"
            )
            lines.append(f"      id: {record.id}")
            lines.append(f"      next: {record.next_action}")


def draft_for_lead(record: LeadRecord) -> str:
    """Return a plain-text follow-up draft. Never sends anything."""
    company = record.company or "your business"
    email_hint = f" ({record.email})" if record.email else ""
    product_key = (record.product, record.event_type)
    metadata_bits = _metadata_context(record)

    templates: dict[tuple[str, str], str] = {
        (
            "DealBrain",
            "report_intake",
        ): (
            f"Subject: Quick DealBrain follow-up for {company}\n\n"
            f"Hey,\n\n"
            f"I saw you started a DealBrain report for {company}{email_hint}. "
            "I can take a look and send back a short readout on the biggest deal risks, "
            "what I would diligence first, and whether the opportunity looks worth deeper work.\n\n"
            "What kind of deal are you evaluating right now: acquisition, investment, or internal growth?"
        ),
        (
            "DealBrain",
            "waitlist",
        ): (
            "Subject: DealBrain early access\n\n"
            f"Hey,\n\n"
            f"Thanks for joining the DealBrain list for {company}. "
            "I am opening this slowly around real analysis use cases first.\n\n"
            "What kind of company or deal would you want DealBrain to evaluate first?"
        ),
        (
            "AI Voice Receptionist",
            "waitlist",
        ): (
            "Subject: AI receptionist demo\n\n"
            f"Hey,\n\n"
            f"Thanks for raising your hand for AI Voice Receptionist for {company}. "
            "I am trying to learn where missed calls hurt the most before pushing anyone into a plan.\n\n"
            "What calls are you missing today: new customers, scheduling, after-hours, or follow-ups?"
        ),
        (
            "AI Voice Receptionist",
            "demo_call",
        ): (
            "Subject: Following up on the AI receptionist demo\n\n"
            f"Hey,\n\n"
            "Saw a demo call come through and wanted to follow up while it is fresh. "
            "If this is useful, I can help shape a simple call flow around your real business calls.\n\n"
            "What should the receptionist be able to answer or route first?"
        ),
        (
            "Prospector Pro",
            "waitlist",
        ): (
            "Subject: Prospector Pro early access\n\n"
            f"Hey,\n\n"
            f"Thanks for joining the Prospector Pro list for {company}. "
            "I am shaping early access around very specific local prospecting runs.\n\n"
            "What niche and city would you want the first prospect list for?"
        ),
        (
            "Prospector Pro",
            "intake",
        ): (
            "Subject: Prospector Pro intake\n\n"
            f"Hey,\n\n"
            f"I saw the Prospector Pro intake for {company}. "
            "I can review the target market and help turn it into a clean first lead run.\n\n"
            "What makes a prospect worth contacting for you right now?"
        ),
        (
            "AI Ops Consulting",
            "intake",
        ): (
            "Subject: AI ops intake follow-up\n\n"
            f"Hey,\n\n"
            f"I saw your AI Ops intake for {company}. "
            "I am prioritizing teams with one painful workflow we can make measurably faster.\n\n"
            "What is the workflow that wastes the most time each week?"
        ),
        (
            "AI Ops Consulting",
            "broker_workflow_audit",
        ): (
            "Subject: Broker workflow audit follow-up\n\n"
            f"Hey,\n\n"
            f"I saw the broker workflow audit for {company}. "
            "The useful move is to start with the first workflow the audit flagged, not automate the whole brokerage at once.\n\n"
            "Want me to map the smallest one-week version and pressure-test whether it is worth building?"
        ),
        (
            "AI Ops Consulting",
            "audit_unlock",
        ): (
            "Subject: AI ops audit follow-up\n\n"
            f"Hey,\n\n"
            f"I saw the audit unlock for {company}. "
            "I can turn the findings into a short implementation plan if the timing is useful.\n\n"
            "Which part of the audit felt most worth fixing first?"
        ),
        (
            "PC Bottleneck Analyzer",
            "email_subscriber",
        ): (
            "Subject: PC Bottleneck Analyzer updates\n\n"
            f"Hey,\n\n"
            "Thanks for joining the PC Bottleneck Analyzer update list. "
            "I am keeping it free while I learn what reports are most useful.\n\n"
            "What are you trying to improve first: gaming FPS, streaming, editing, or general upgrade planning?"
        ),
    }

    body = templates.get(
        product_key,
        (
            f"Subject: Quick follow-up\n\n"
            f"Hey,\n\n"
            f"I saw your signup for {record.product} and wanted to follow up personally. "
            "I am learning what people actually need before pushing pricing or payment.\n\n"
            "What made you interested enough to sign up?"
        ),
    )
    if metadata_bits:
        body += f"\n\nContext I saw:\n{metadata_bits}"
    return body


def render_discord_body(report: LedgerReport, *, limit: int = 10) -> str:
    lines: list[str] = []
    lines.append(f"Window: last **{report.window_hours}h** | **{len(report.events)}** lead events")
    if report.counts_by_product:
        lines.append("")
        lines.append("**By product**")
        for product, count in sorted(report.counts_by_product.items()):
            lines.append(f"- {product}: **{count}**")
    lines.append("")
    lines.append("**Highest intent events**")
    if not report.events:
        lines.append("_No recent lead events found._")
    for i, event in enumerate(report.events[:limit], 1):
        email = event.email or "no email"
        company = event.company or "-"
        lines.append(
            f"`{i:02}` **[{event.intent_score}] {event.product}** {event.event_type} - "
            f"{company} | `{email}`"
        )
        lines.append(f"     {event.next_action}")
    if report.errors:
        lines.append("")
        lines.append("**Warnings**")
        for err in report.errors[:4]:
            lines.append(f"- `{err[:140]}`")
    return "\n".join(lines)[:3900]


def _record_from_row(row: sqlite3.Row) -> LeadRecord:
    return LeadRecord(
        id=row["id"],
        event_key=row["event_key"],
        product=row["product"],
        event_type=row["event_type"],
        source_table=row["source_table"],
        source_row_id=row["source_row_id"],
        email=row["email"],
        company=row["company"],
        event_created_at=row["event_created_at"],
        first_seen_at=row["first_seen_at"],
        last_seen_at=row["last_seen_at"],
        status=row["status"],
        intent_score=int(row["intent_score"] or 0),
        next_action=row["next_action"] or "",
        notes_md=row["notes_md"] or "",
        metadata=json.loads(row["metadata_json"] or "{}"),
        last_contacted_at=row["last_contacted_at"],
        follow_up_at=row["follow_up_at"],
        updated_at=row["updated_at"],
    )


def _record_to_dict(record: LeadRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "event_key": record.event_key,
        "product": record.product,
        "event_type": record.event_type,
        "source_table": record.source_table,
        "source_row_id": record.source_row_id,
        "email": record.email,
        "company": record.company,
        "event_created_at": record.event_created_at,
        "first_seen_at": record.first_seen_at,
        "last_seen_at": record.last_seen_at,
        "status": record.status,
        "intent_score": record.intent_score,
        "next_action": record.next_action,
        "notes_md": record.notes_md,
        "metadata": record.metadata,
        "last_contacted_at": record.last_contacted_at,
        "follow_up_at": record.follow_up_at,
        "updated_at": record.updated_at,
    }


def _sync_result_to_dict(result: SyncResult, mirror_warning: str | None = None) -> dict[str, Any]:
    return {
        "inserted": result.inserted,
        "updated": result.updated,
        "open_count": result.open_count,
        "dry_run": result.dry_run,
        "events": len(result.report.events),
        "window_hours": result.report.window_hours,
        "counts_by_product": result.report.counts_by_product,
        "counts_by_type": result.report.counts_by_type,
        "errors": result.report.errors,
        "mirror_warning": mirror_warning,
    }


def _normalize_status(status: str) -> str:
    normalized = status.strip().upper().replace("-", "_")
    if normalized not in VALID_STATUSES:
        allowed = ", ".join(sorted(VALID_STATUSES))
        raise ValueError(f"Invalid lead status {status!r}. Use one of: {allowed}")
    return normalized


def _metadata_context(record: LeadRecord) -> str:
    interesting = []
    for key in (
        "industry",
        "source",
        "status",
        "use_case",
        "outcome",
        "share_slug",
        "score",
        "estimated_hours_saved_per_week",
        "recommended_workflow",
        "urgency",
        "messy_workflows",
    ):
        value = record.metadata.get(key)
        if value:
            interesting.append(f"- {key}: {value}")
    return "\n".join(interesting)


def _remote_payload(record: LeadRecord) -> dict[str, Any]:
    return {
        "id": record.id,
        "event_key": record.event_key,
        "product": record.product,
        "event_type": record.event_type,
        "source_table": record.source_table,
        "source_row_id": record.source_row_id,
        "email": record.email,
        "company": record.company,
        "event_created_at": record.event_created_at,
        "first_seen_at": record.first_seen_at,
        "last_seen_at": record.last_seen_at,
        "status": record.status,
        "intent_score": record.intent_score,
        "next_action": record.next_action,
        "notes_md": record.notes_md,
        "metadata": record.metadata,
        "last_contacted_at": record.last_contacted_at,
        "follow_up_at": record.follow_up_at,
        "updated_at": record.updated_at,
    }


def write_status_metrics(
    digest: DigestReport,
    *,
    path: Path | None = None,
) -> dict[str, Any]:
    """Write compact lead queue metrics into the operator status file."""
    from .utils import status as status_mod

    payload = {
        "generated_at": digest.generated_at,
        "open_count": digest.open_count,
        "new_24h": len(digest.new_24h),
        "high_intent_uncontacted": len(digest.high_intent_uncontacted),
        "stale_high_intent": len(digest.stale_high_intent),
        "counts_by_product": digest.counts_by_product,
        "source_errors": digest.source_errors[:8],
        "top_leads": [
            {
                "id": lead.id,
                "product": lead.product,
                "event_type": lead.event_type,
                "company": lead.company,
                "email": lead.email,
                "intent_score": lead.intent_score,
                "status": lead.status,
            }
            for lead in digest.top_queue[:5]
        ],
    }
    status_mod.write_status("lead_ledger", payload, path)
    return payload


def run_daily_digest(
    *,
    store: LeadStore | None = None,
    client: SupabaseRestClient | None = None,
    window_hours: int = 168,
    per_source_limit: int = 25,
    limit: int = 10,
    post_discord: bool = False,
    status_path: Path | None = None,
) -> dict[str, Any]:
    """Sync, render, and record the local daily signup follow-up digest."""
    store = store or LeadStore()
    client = client if client is not None else SupabaseRestClient.from_env()
    result, mirror_warning = sync_leads(
        store=store,
        client=client,
        window_hours=window_hours,
        per_source_limit=per_source_limit,
    )
    source_errors = list(result.report.errors)
    if mirror_warning:
        source_errors.append(mirror_warning)
    digest = build_digest_report(store, source_errors=source_errors, limit=limit)
    status_payload = write_status_metrics(digest, path=status_path)
    posted = False
    if post_discord:
        posted = _post_digest_report(digest, limit=limit) == 0
    return {
        "sync": _sync_result_to_dict(result, mirror_warning),
        "digest": digest.to_dict(),
        "status": status_payload,
        "text": render_digest_report(digest, limit=limit),
        "posted": posted,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="operator leads")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=None,
        help="Override local lead ledger sqlite path.",
    )
    sub = parser.add_subparsers(dest="command")

    report = sub.add_parser("report", help="Show recent raw lead events from product tables")
    report.add_argument("--window-hours", type=int, default=168)
    report.add_argument("--limit", type=int, default=20)
    report.add_argument("--post-discord", action="store_true")
    report.add_argument("--json", action="store_true")

    sync = sub.add_parser("sync", help="Pull recent product events into the persistent queue")
    sync.add_argument("--window-hours", type=int, default=168)
    sync.add_argument("--per-source-limit", type=int, default=25)
    sync.add_argument("--mirror-supabase", action="store_true")
    sync.add_argument("--post-discord", action="store_true")
    sync.add_argument("--dry-run", action="store_true")
    sync.add_argument("--json", action="store_true")

    list_cmd = sub.add_parser("list", help="List persistent leads")
    list_cmd.add_argument("--status", default=None)
    list_cmd.add_argument("--all", action="store_true", help="Include closed/won/lost leads")
    list_cmd.add_argument("--limit", type=int, default=20)
    list_cmd.add_argument("--product", default=None)
    list_cmd.add_argument("--event-type", default=None)
    list_cmd.add_argument("--min-score", type=int, default=None)
    list_cmd.add_argument("--json", action="store_true")

    show = sub.add_parser("show", help="Show one lead")
    show.add_argument("lead_ref")

    draft = sub.add_parser("draft", help="Draft a human follow-up for one lead")
    draft.add_argument("lead_ref")

    mark = sub.add_parser("mark", help="Change a lead status")
    mark.add_argument("lead_ref")
    mark.add_argument("status")
    mark.add_argument("--note", default=None)
    mark.add_argument("--follow-up-at", default=None)

    note = sub.add_parser("note", help="Add a note to one lead")
    note.add_argument("lead_ref")
    note.add_argument("body", nargs="*")

    digest = sub.add_parser("digest", help="Show the daily follow-up digest")
    digest.add_argument("--limit", type=int, default=10)
    digest.add_argument("--post-discord", action="store_true")
    digest.add_argument("--sync-first", action="store_true")
    digest.add_argument("--window-hours", type=int, default=168)
    digest.add_argument("--write-status", action="store_true")
    digest.add_argument("--json", action="store_true")

    daily = sub.add_parser("daily", help="Sync, write status metrics, and render the daily digest")
    daily.add_argument("--limit", type=int, default=10)
    daily.add_argument("--window-hours", type=int, default=168)
    daily.add_argument("--per-source-limit", type=int, default=25)
    daily.add_argument("--post-discord", action="store_true")
    daily.add_argument("--json", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass

    argv = list(sys.argv[1:] if argv is None else argv)
    if not argv or argv[0].startswith("--"):
        argv = ["report", *argv]

    parser = _build_parser()
    args = parser.parse_args(argv)
    store = LeadStore(args.db_path)

    try:
        if args.command == "report":
            report = collect_events(window_hours=args.window_hours)
            if args.json:
                print(json.dumps({
                    "generated_at": report.generated_at,
                    "window_hours": report.window_hours,
                    "events": [
                        {
                            "product": event.product,
                            "event_type": event.event_type,
                            "table": event.table,
                            "row_id": event.row_id,
                            "email": event.email,
                            "company": event.company,
                            "created_at": event.created_at,
                            "intent_score": event.intent_score,
                            "next_action": event.next_action,
                        }
                        for event in report.events
                    ],
                    "counts_by_product": report.counts_by_product,
                    "counts_by_type": report.counts_by_type,
                    "errors": report.errors,
                }, indent=2))
                return 0
            print(render_text(report, limit=args.limit))
            if args.post_discord:
                return _post_report(report, args.limit)
            return 0

        if args.command == "sync":
            client = SupabaseRestClient.from_env()
            result, mirror_warning = sync_leads(
                store=store,
                client=client,
                window_hours=args.window_hours,
                per_source_limit=args.per_source_limit,
                mirror_supabase=args.mirror_supabase,
                dry_run=args.dry_run,
            )
            if args.json:
                print(json.dumps(_sync_result_to_dict(result, mirror_warning), indent=2))
                return 0
            prefix = "[leads sync dry-run]" if args.dry_run else "[leads sync]"
            print(
                f"{prefix} inserted={result.inserted} updated={result.updated} "
                f"open={result.open_count} events={len(result.report.events)}"
            )
            for err in result.report.errors[:8]:
                print(f"  warning: {err}")
            if mirror_warning:
                print(f"  warning: {mirror_warning}")
            if args.post_discord:
                records = store.list(limit=10)
                return _post_digest(records, open_count=result.open_count, limit=10)
            return 0

        if args.command == "list":
            records = store.list(
                status=args.status,
                open_only=not args.all,
                limit=args.limit,
                product=args.product,
                event_type=args.event_type,
                min_score=args.min_score,
            )
            if args.json:
                print(json.dumps([_record_to_dict(record) for record in records], indent=2))
                return 0
            print(render_queue(records))
            return 0

        if args.command == "show":
            print(render_record(store.get(args.lead_ref)))
            return 0

        if args.command == "draft":
            print(draft_for_lead(store.get(args.lead_ref)))
            return 0

        if args.command == "mark":
            record = store.mark(
                args.lead_ref,
                args.status,
                note=args.note,
                follow_up_at=args.follow_up_at,
            )
            print(f"[leads] {record.id} status={record.status}")
            return 0

        if args.command == "note":
            body = " ".join(args.body).strip() or sys.stdin.read().strip()
            record = store.add_note(args.lead_ref, body)
            print(f"[leads] note added to {record.id}")
            return 0

        if args.command == "digest":
            source_errors: list[str] = []
            if args.sync_first:
                result, mirror_warning = sync_leads(store=store, window_hours=args.window_hours)
                source_errors.extend(result.report.errors)
                if mirror_warning:
                    source_errors.append(mirror_warning)
            digest = build_digest_report(store, source_errors=source_errors, limit=args.limit)
            if args.write_status:
                write_status_metrics(digest)
            if args.json:
                print(json.dumps(digest.to_dict(), indent=2))
                return 0
            text = render_digest_report(digest, limit=args.limit)
            print(text)
            if args.post_discord:
                return _post_digest_report(digest, limit=args.limit)
            return 0

        if args.command == "daily":
            payload = run_daily_digest(
                store=store,
                window_hours=args.window_hours,
                per_source_limit=args.per_source_limit,
                limit=args.limit,
                post_discord=args.post_discord,
            )
            if args.json:
                print(json.dumps({k: v for k, v in payload.items() if k != "text"}, indent=2))
            else:
                print(payload["text"])
            return 0
    except (KeyError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 0


def _post_report(report: LedgerReport, limit: int) -> int:
    from .utils.discord import make_footer, notify

    ok = notify(
        channel="projects",
        title="Portfolio Lead Ledger",
        body=render_discord_body(report, limit=min(limit, 10)),
        color="blue" if report.events else "yellow",
        footer=make_footer("operator leads report"),
    )
    print(f"\n[discord] post {'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


def _post_digest(records: list[LeadRecord], *, open_count: int, limit: int) -> int:
    from .utils.discord import make_footer, notify

    ok = notify(
        channel="projects",
        title="Signup Follow-up Queue",
        body=render_digest(records, open_count=open_count, limit=min(limit, 10)),
        color="green" if records else "yellow",
        footer=make_footer("operator leads digest"),
    )
    print(f"\n[discord] post {'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


def _post_digest_report(digest: DigestReport, *, limit: int) -> int:
    from .utils.discord import make_footer, notify

    ok = notify(
        channel="projects",
        title="Signup Follow-up Digest",
        body=render_digest_discord(digest, limit=min(limit, 8)),
        color="green" if digest.open_count else "yellow",
        footer=make_footer("operator leads daily"),
    )
    print(f"\n[discord] post {'OK' if ok else 'FAILED'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
