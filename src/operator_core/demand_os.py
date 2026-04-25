"""Portfolio Demand OS.

Second layer above the signup lead ledger. The lead ledger answers
"who raised their hand?" Demand OS answers "which product is pulling demand,
what experiment should we run next, and where is the funnel weak?"
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .lead_ledger import LeadRecord, LeadStore, SOURCE_SPECS, draft_for_lead
from .settings import ConfigError, load_settings


ACTIVE_PRODUCTS = (
    "DealBrain",
    "AI Voice Receptionist",
    "Prospector Pro",
    "AI Ops Consulting",
    "PC Bottleneck Analyzer",
)


PRODUCT_PROFILES: dict[str, dict[str, Any]] = {
    "DealBrain": {
        "thesis": "AI diligence/readout for people evaluating SMB deals.",
        "primary_audience": "business buyers, brokers, and operators evaluating acquisitions",
        "success_signal": "report intake or uploaded deal material",
        "experiments": (
            "Broker-specific landing page with free sample report",
            "Acquisition-buyer intake question set",
            "Founder-style follow-up offering a one-page deal-risk readout",
        ),
    },
    "AI Voice Receptionist": {
        "thesis": "Missed-call capture and routing for appointment/service businesses.",
        "primary_audience": "local service businesses with missed inbound calls",
        "success_signal": "demo call or waitlist with business name",
        "experiments": (
            "Dental-office missed-call landing page",
            "After-hours call demo with email capture after transcript",
            "Short missed-call calculator before waitlist signup",
        ),
    },
    "Prospector Pro": {
        "thesis": "Local prospect list builder for specific niches and cities.",
        "primary_audience": "small teams that need local outbound lists",
        "success_signal": "intake with niche, geography, and use case",
        "experiments": (
            "Roofing contractor sample prospect run",
            "City+niche intake form with example output",
            "Warm lead handoff into AI Ops audit offer",
        ),
    },
    "AI Ops Consulting": {
        "thesis": "Workflow audits and lightweight automation for SMB operators.",
        "primary_audience": "operators with repeated admin, sales, or service workflows",
        "success_signal": "intake or audit unlock with workflow context",
        "experiments": (
            "Business-broker ops audit follow-up lane",
            "Workflow pain quiz that writes structured intake",
            "One painful workflow offer page for local service businesses",
        ),
    },
    "PC Bottleneck Analyzer": {
        "thesis": "Free PC report that captures upgrade intent before monetization.",
        "primary_audience": "PC builders planning upgrades",
        "success_signal": "email subscriber with source or report context",
        "experiments": (
            "Report-result email capture with upgrade goal",
            "Blog source tagging for every subscriber",
            "Free upgrade-plan PDF lead magnet",
        ),
    },
}


@dataclass(frozen=True)
class ProductDemand:
    product: str
    open_leads: int
    total_leads: int
    high_intent: int
    stale_hot: int
    new_7d: int
    contacted: int
    closed: int
    source_count: int
    demand_score: int
    verdict: str
    next_experiment: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class SourceHealth:
    product: str
    source_table: str
    event_type: str
    rows_seen: int
    health: str
    note: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class GrowthExperiment:
    id: str
    product: str
    title: str
    hypothesis: str
    success_metric: str
    priority: int
    effort: str
    next_step: str

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


VALID_EXPERIMENT_STATUSES = {"BACKLOG", "RUNNING", "PAUSED", "SHIPPED", "KILLED"}
OPEN_EXPERIMENT_STATUSES = {"BACKLOG", "RUNNING", "PAUSED"}


@dataclass(frozen=True)
class ExperimentRecord:
    id: str
    product: str
    title: str
    hypothesis: str
    success_metric: str
    priority: int
    effort: str
    status: str
    next_step: str
    notes_md: str
    created_at: str
    updated_at: str
    started_at: str | None
    completed_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class BrokerCloseLead:
    email: str
    company: str | None
    status: str
    source_status: str | None
    audit_lead_id: str | None
    synced_lead_id: str | None
    intent_score: int
    recommended_workflow: str | None
    urgency: str | None
    estimated_hours_saved_per_week: Any
    age_hours: float | None

    def to_dict(self) -> dict[str, Any]:
        return self.__dict__.copy()


@dataclass(frozen=True)
class BrokerCloseState:
    total: int
    hot_unworked: int
    stale_hot: int
    contacted: int
    booked: int
    won: int
    lost: int
    nurture: int
    top_actions: list[BrokerCloseLead]

    def to_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "hot_unworked": self.hot_unworked,
            "stale_hot": self.stale_hot,
            "contacted": self.contacted,
            "booked": self.booked,
            "won": self.won,
            "lost": self.lost,
            "nurture": self.nurture,
            "top_actions": [lead.to_dict() for lead in self.top_actions],
        }


@dataclass(frozen=True)
class DemandReview:
    generated_at: str
    scoreboard: list[ProductDemand]
    source_health: list[SourceHealth]
    experiments: list[GrowthExperiment]
    top_leads: list[LeadRecord]
    broker_close_state: BrokerCloseState

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "scoreboard": [row.to_dict() for row in self.scoreboard],
            "source_health": [row.to_dict() for row in self.source_health],
            "experiments": [row.to_dict() for row in self.experiments],
            "top_leads": [_lead_summary(lead) for lead in self.top_leads],
            "broker_close_state": self.broker_close_state.to_dict(),
        }


@dataclass(frozen=True)
class NightlyPlan:
    generated_at: str
    focus_product: str | None
    scoreboard: list[ProductDemand]
    watch_sources: list[SourceHealth]
    active_experiments: list[ExperimentRecord]
    backlog: list[ExperimentRecord]
    top_leads: list[LeadRecord]
    broker_close_state: BrokerCloseState

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "focus_product": self.focus_product,
            "scoreboard": [row.to_dict() for row in self.scoreboard],
            "watch_sources": [row.to_dict() for row in self.watch_sources],
            "active_experiments": [row.to_dict() for row in self.active_experiments],
            "backlog": [row.to_dict() for row in self.backlog],
            "top_leads": [_lead_summary(lead) for lead in self.top_leads],
            "broker_close_state": self.broker_close_state.to_dict(),
        }


class ExperimentStore:
    """Persistent growth experiment registry stored next to the lead ledger."""

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or LeadStore().db_path
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
                CREATE TABLE IF NOT EXISTS growth_experiments (
                    id TEXT PRIMARY KEY,
                    product TEXT NOT NULL,
                    title TEXT NOT NULL,
                    hypothesis TEXT NOT NULL,
                    success_metric TEXT NOT NULL,
                    priority INTEGER NOT NULL DEFAULT 0,
                    effort TEXT NOT NULL DEFAULT 'S',
                    status TEXT NOT NULL DEFAULT 'BACKLOG',
                    next_step TEXT NOT NULL DEFAULT '',
                    notes_md TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_growth_experiments_status_priority "
                "ON growth_experiments(status, priority DESC)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_growth_experiments_product "
                "ON growth_experiments(product)"
            )

    def seed_generated(self, experiments: list[GrowthExperiment]) -> tuple[int, int]:
        """Insert generated experiments and refresh untouched backlog metadata."""
        inserted = 0
        refreshed = 0
        now = _now_iso()
        with self._connect() as conn:
            for experiment in experiments:
                row = conn.execute(
                    "SELECT status FROM growth_experiments WHERE id = ?",
                    (experiment.id,),
                ).fetchone()
                if row is None:
                    conn.execute(
                        """
                        INSERT INTO growth_experiments (
                            id, product, title, hypothesis, success_metric, priority,
                            effort, status, next_step, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'BACKLOG', ?, ?, ?)
                        """,
                        (
                            experiment.id,
                            experiment.product,
                            experiment.title,
                            experiment.hypothesis,
                            experiment.success_metric,
                            experiment.priority,
                            experiment.effort,
                            experiment.next_step,
                            now,
                            now,
                        ),
                    )
                    inserted += 1
                elif row["status"] == "BACKLOG":
                    conn.execute(
                        """
                        UPDATE growth_experiments
                        SET product = ?, title = ?, hypothesis = ?, success_metric = ?,
                            priority = ?, effort = ?, next_step = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            experiment.product,
                            experiment.title,
                            experiment.hypothesis,
                            experiment.success_metric,
                            experiment.priority,
                            experiment.effort,
                            experiment.next_step,
                            now,
                            experiment.id,
                        ),
                    )
                    refreshed += 1
        return inserted, refreshed

    def list(
        self,
        *,
        status: str | None = None,
        product: str | None = None,
        include_done: bool = False,
        limit: int = 20,
    ) -> list[ExperimentRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(_normalize_experiment_status(status))
        elif not include_done:
            clauses.append("status IN (?, ?, ?)")
            params.extend(sorted(OPEN_EXPERIMENT_STATUSES))
        if product:
            clauses.append("lower(product) = lower(?)")
            params.append(product)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        sql = (
            "SELECT * FROM growth_experiments"
            f"{where}"
            " ORDER BY CASE status WHEN 'RUNNING' THEN 0 WHEN 'BACKLOG' THEN 1 "
            "WHEN 'PAUSED' THEN 2 ELSE 3 END, priority DESC, updated_at DESC"
            " LIMIT ?"
        )
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [_experiment_from_row(row) for row in rows]

    def get(self, experiment_id: str) -> ExperimentRecord:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM growth_experiments WHERE id = ?",
                (experiment_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown experiment: {experiment_id}")
        return _experiment_from_row(row)

    def mark(self, experiment_id: str, status: str, *, note: str | None = None) -> ExperimentRecord:
        record = self.get(experiment_id)
        normalized = _normalize_experiment_status(status)
        now = _now_iso()
        started_at = record.started_at
        completed_at = record.completed_at
        if normalized == "RUNNING" and not started_at:
            started_at = now
        if normalized in {"SHIPPED", "KILLED"} and not completed_at:
            completed_at = now
        if normalized in {"BACKLOG", "RUNNING", "PAUSED"}:
            completed_at = None
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE growth_experiments
                SET status = ?, started_at = ?, completed_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (normalized, started_at, completed_at, now, experiment_id),
            )
        if note:
            self.add_note(experiment_id, note)
        return self.get(experiment_id)

    def add_note(self, experiment_id: str, note: str) -> ExperimentRecord:
        record = self.get(experiment_id)
        body = note.strip()
        if not body:
            return record
        now = _now_iso()
        notes_md = (record.notes_md.rstrip() + f"\n\n[{now[:16]}] {body}").strip()
        with self._connect() as conn:
            conn.execute(
                "UPDATE growth_experiments SET notes_md = ?, updated_at = ? WHERE id = ?",
                (notes_md, now, experiment_id),
            )
        return self.get(experiment_id)


def build_scoreboard(
    store: LeadStore | None = None,
    *,
    now: datetime | None = None,
) -> list[ProductDemand]:
    store = store or LeadStore()
    now = now or datetime.now(timezone.utc)
    records = store.list(open_only=False, limit=10_000)
    sources_by_product = _sources_by_product(records)
    rows: list[ProductDemand] = []

    for product in ACTIVE_PRODUCTS:
        product_records = [r for r in records if r.product == product]
        open_records = [r for r in product_records if r.status in {"NEW", "CONTACTED", "FOLLOW_UP", "NURTURE"}]
        high_intent = [r for r in open_records if r.intent_score >= 80]
        stale_hot = [
            r for r in high_intent
            if not r.last_contacted_at and _record_age_hours(r, now) is not None and _record_age_hours(r, now) >= 48
        ]
        new_7d = [
            r for r in product_records
            if _record_age_hours(r, now) is not None and _record_age_hours(r, now) <= 7 * 24
        ]
        contacted = [r for r in product_records if r.status in {"CONTACTED", "FOLLOW_UP", "WON"} or r.last_contacted_at]
        closed = [r for r in product_records if r.status in {"WON", "LOST", "CLOSED"}]
        source_count = len(sources_by_product.get(product, set()))

        score = _demand_score(
            open_leads=len(open_records),
            high_intent=len(high_intent),
            stale_hot=len(stale_hot),
            new_7d=len(new_7d),
            source_count=source_count,
            contacted=len(contacted),
        )
        rows.append(
            ProductDemand(
                product=product,
                open_leads=len(open_records),
                total_leads=len(product_records),
                high_intent=len(high_intent),
                stale_hot=len(stale_hot),
                new_7d=len(new_7d),
                contacted=len(contacted),
                closed=len(closed),
                source_count=source_count,
                demand_score=score,
                verdict=_verdict(score, len(stale_hot), len(open_records)),
                next_experiment=_pick_experiment(product, score, len(stale_hot), source_count),
            )
        )

    rows.sort(key=lambda r: (r.demand_score, r.high_intent, r.open_leads), reverse=True)
    return rows


def build_source_health(store: LeadStore | None = None) -> list[SourceHealth]:
    store = store or LeadStore()
    records = store.list(open_only=False, limit=10_000)
    by_table: dict[str, int] = {}
    for record in records:
        by_table[record.source_table] = by_table.get(record.source_table, 0) + 1

    rows: list[SourceHealth] = []
    for spec in SOURCE_SPECS:
        count = by_table.get(spec.table, 0)
        if count > 0:
            health = "ok"
            note = f"{count} normalized row(s) seen."
        elif spec.event_type in {"waitlist", "report_intake", "intake", "email_subscriber"}:
            health = "watch"
            note = "No rows in local ledger yet; verify product route, source tags, and storage path."
        else:
            health = "optional"
            note = "No rows yet; this may be fine if the path is not active."
        rows.append(
            SourceHealth(
                product=spec.product,
                source_table=spec.table,
                event_type=spec.event_type,
                rows_seen=count,
                health=health,
                note=note,
            )
        )
    return rows


def build_experiments(
    store: LeadStore | None = None,
    *,
    now: datetime | None = None,
    limit: int = 12,
) -> list[GrowthExperiment]:
    scoreboard = build_scoreboard(store, now=now)
    health_rows = build_source_health(store)
    health_by_product: dict[str, list[SourceHealth]] = {}
    for row in health_rows:
        health_by_product.setdefault(row.product, []).append(row)

    experiments: list[GrowthExperiment] = []
    for row in scoreboard:
        profile = PRODUCT_PROFILES[row.product]
        missing_sources = [h for h in health_by_product.get(row.product, []) if h.health == "watch"]
        base_priority = min(100, row.demand_score + row.stale_hot * 6)
        if row.open_leads == 0:
            base_priority = max(base_priority, 45)
        for idx, title in enumerate(profile["experiments"], 1):
            priority = max(1, base_priority - (idx - 1) * 8)
            if missing_sources and idx == 1:
                priority += 10
            experiments.append(
                GrowthExperiment(
                    id=f"{_slug(row.product)}-{idx}",
                    product=row.product,
                    title=title,
                    hypothesis=_experiment_hypothesis(row, title, bool(missing_sources)),
                    success_metric=profile["success_signal"],
                    priority=min(100, priority),
                    effort="M" if idx == 1 else "S",
                    next_step=_experiment_next_step(row.product, title, missing_sources),
                )
            )

    experiments.sort(key=lambda e: (e.priority, e.product), reverse=True)
    return experiments[:limit]


def build_broker_close_state(
    store: LeadStore | None = None,
    *,
    now: datetime | None = None,
) -> BrokerCloseState:
    """Summarize the AI Ops broker lane from normalized ledger records.

    Broker audit rows carry score/workflow context. Synced `ao_leads` rows carry
    operator close status from the AI Ops admin room. This rollup merges both by
    email so Demand OS can see close momentum without coupling to AI Ops UI code.
    """
    store = store or LeadStore()
    now = now or datetime.now(timezone.utc)
    records = [
        record
        for record in store.list(open_only=False, limit=10_000)
        if _is_broker_close_record(record)
    ]
    by_key: dict[str, list[LeadRecord]] = {}
    for record in records:
        key = (record.email or record.source_row_id).lower()
        by_key.setdefault(key, []).append(record)

    leads = [_merge_broker_close_records(group, now=now) for group in by_key.values()]
    leads.sort(key=_broker_close_priority, reverse=True)

    return BrokerCloseState(
        total=len(leads),
        hot_unworked=len([lead for lead in leads if lead.status == "NEW" and lead.intent_score >= 80]),
        stale_hot=len(
            [
                lead
                for lead in leads
                if lead.status == "NEW"
                and lead.intent_score >= 80
                and lead.age_hours is not None
                and lead.age_hours >= 48
            ]
        ),
        contacted=len([lead for lead in leads if lead.status == "CONTACTED"]),
        booked=len([lead for lead in leads if lead.status == "BOOKED"]),
        won=len([lead for lead in leads if lead.status == "WON"]),
        lost=len([lead for lead in leads if lead.status == "LOST"]),
        nurture=len([lead for lead in leads if lead.status == "NURTURE"]),
        top_actions=[
            lead
            for lead in leads
            if lead.status in {"NEW", "CONTACTED", "NURTURE"} and lead.intent_score >= 75
        ][:5],
    )


def seed_experiment_backlog(
    store: LeadStore | None = None,
    experiment_store: ExperimentStore | None = None,
    *,
    now: datetime | None = None,
    limit: int = 15,
) -> tuple[int, int]:
    store = store or LeadStore()
    experiment_store = experiment_store or ExperimentStore(store.db_path)
    return experiment_store.seed_generated(build_experiments(store, now=now, limit=limit))


def build_review(store: LeadStore | None = None, *, now: datetime | None = None) -> DemandReview:
    store = store or LeadStore()
    now = now or datetime.now(timezone.utc)
    return DemandReview(
        generated_at=now.isoformat(),
        scoreboard=build_scoreboard(store, now=now),
        source_health=build_source_health(store),
        experiments=build_experiments(store, now=now),
        top_leads=store.list(open_only=True, limit=10),
        broker_close_state=build_broker_close_state(store, now=now),
    )


def build_nightly_plan(
    store: LeadStore | None = None,
    experiment_store: ExperimentStore | None = None,
    *,
    now: datetime | None = None,
    seed: bool = True,
) -> NightlyPlan:
    store = store or LeadStore()
    experiment_store = experiment_store or ExperimentStore(store.db_path)
    now = now or datetime.now(timezone.utc)
    if seed:
        seed_experiment_backlog(store, experiment_store, now=now)
    scoreboard = build_scoreboard(store, now=now)
    source_health = build_source_health(store)
    return NightlyPlan(
        generated_at=now.isoformat(),
        focus_product=scoreboard[0].product if scoreboard else None,
        scoreboard=scoreboard,
        watch_sources=[row for row in source_health if row.health == "watch"],
        active_experiments=experiment_store.list(status="RUNNING", limit=8),
        backlog=experiment_store.list(include_done=False, limit=12),
        top_leads=store.list(open_only=True, limit=8),
        broker_close_state=build_broker_close_state(store, now=now),
    )


def render_scoreboard(rows: list[ProductDemand]) -> str:
    lines = ["Portfolio Demand Scoreboard", "=" * 78]
    if not rows:
        lines.append("(no products)")
        return "\n".join(lines)
    for idx, row in enumerate(rows, 1):
        lines.append(
            f"{idx:2}. [{row.demand_score:3}] {row.product} | "
            f"open={row.open_leads} high={row.high_intent} stale={row.stale_hot} "
            f"new7d={row.new_7d} sources={row.source_count}"
        )
        lines.append(f"    verdict: {row.verdict}")
        lines.append(f"    next: {row.next_experiment}")
    return "\n".join(lines)


def render_source_health(rows: list[SourceHealth]) -> str:
    lines = ["Signup Source Health", "=" * 78]
    for row in rows:
        lines.append(
            f"- [{row.health:<8}] {row.product} / {row.event_type} / "
            f"{row.source_table}: {row.note}"
        )
    return "\n".join(lines)


def render_experiments(rows: list[GrowthExperiment]) -> str:
    lines = ["Growth Experiment Backlog", "=" * 78]
    for idx, row in enumerate(rows, 1):
        lines.append(f"{idx:2}. [{row.priority:3}] {row.product}: {row.title} ({row.effort})")
        lines.append(f"    hypothesis: {row.hypothesis}")
        lines.append(f"    metric: {row.success_metric}")
        lines.append(f"    next: {row.next_step}")
    return "\n".join(lines)


def render_experiment_backlog(rows: list[ExperimentRecord]) -> str:
    lines = ["Persistent Growth Experiment Backlog", "=" * 78]
    if not rows:
        lines.append("(empty)")
        return "\n".join(lines)
    for idx, row in enumerate(rows, 1):
        lines.append(
            f"{idx:2}. [{row.priority:3}] {row.status:<8} {row.product}: "
            f"{row.title} ({row.effort})"
        )
        lines.append(f"    id: {row.id}")
        lines.append(f"    next: {row.next_step}")
        if row.notes_md:
            lines.append("    notes: yes")
    return "\n".join(lines)


def render_experiment_record(row: ExperimentRecord) -> str:
    lines = [
        f"Experiment {row.id}",
        "=" * 78,
        f"Product:  {row.product}",
        f"Status:   {row.status}",
        f"Priority: {row.priority}",
        f"Effort:   {row.effort}",
        "",
        "Title:",
        row.title,
        "",
        "Hypothesis:",
        row.hypothesis,
        "",
        "Success metric:",
        row.success_metric,
        "",
        "Next step:",
        row.next_step,
    ]
    if row.notes_md:
        lines.extend(["", "Notes:", row.notes_md])
    return "\n".join(lines)


def render_journey(record: LeadRecord) -> str:
    lines = [
        f"Lead Journey: {record.id}",
        "=" * 78,
        f"Product: {record.product}",
        f"Who: {record.company or '-'} | {record.email or 'no email'}",
        f"Status: {record.status} | Intent: {record.intent_score}",
        "",
        "Timeline:",
    ]
    events = [
        ("source event", record.event_created_at, f"{record.event_type} from {record.source_table}"),
        ("first seen", record.first_seen_at, "entered operator queue"),
        ("last seen", record.last_seen_at, "refreshed by sync"),
        ("last contacted", record.last_contacted_at, "human follow-up recorded"),
        ("follow-up due", record.follow_up_at, "next follow-up target"),
    ]
    for label, ts, detail in events:
        if ts:
            lines.append(f"- {ts[:16].replace('T', ' ')} | {label}: {detail}")
    if record.notes_md:
        lines.extend(["", "Notes:", record.notes_md])
    lines.extend(["", "Suggested draft:", draft_for_lead(record)])
    return "\n".join(lines)


def render_review(review: DemandReview) -> str:
    lines = [
        f"# Portfolio Demand Review - {review.generated_at[:10]}",
        "",
        "## Demand Scoreboard",
        "",
    ]
    for row in review.scoreboard:
        lines.append(
            f"- **{row.product}**: score {row.demand_score}; open {row.open_leads}; "
            f"high-intent {row.high_intent}; stale {row.stale_hot}; verdict: {row.verdict}"
        )
        lines.append(f"  Next experiment: {row.next_experiment}")
    lines.extend(["", "## Source Health", ""])
    for row in review.source_health:
        lines.append(f"- `{row.health}` {row.product} / {row.event_type} / `{row.source_table}`: {row.note}")
    broker = review.broker_close_state
    lines.extend(["", "## AI Ops Broker Close State", ""])
    lines.append(
        f"- Total broker leads: {broker.total}; hot unworked: {broker.hot_unworked}; "
        f"stale hot: {broker.stale_hot}; contacted: {broker.contacted}; "
        f"booked: {broker.booked}; won: {broker.won}; lost: {broker.lost}; nurture: {broker.nurture}"
    )
    if broker.top_actions:
        lines.append("- Top broker actions:")
        for lead in broker.top_actions:
            lines.append(
                f"  - [{lead.intent_score}] {lead.company or lead.email} "
                f"({lead.status}) - {lead.recommended_workflow or 'review workflow'}"
            )
    else:
        lines.append("- No broker close actions in the local queue.")
    lines.extend(["", "## Experiment Backlog", ""])
    for row in review.experiments:
        lines.append(f"- [{row.priority}] **{row.product}** - {row.title}")
        lines.append(f"  {row.next_step}")
    lines.extend(["", "## Top Open Leads", ""])
    for lead in review.top_leads:
        lines.append(
            f"- [{lead.intent_score}] **{lead.product}** {lead.event_type}: "
            f"{lead.company or lead.email or '-'} (`{lead.id}`)"
        )
    lines.extend([
        "",
        "## Operating Rule",
        "",
        "No Stripe and no payment-first work. Use this review to decide which signup path, follow-up, or distribution experiment deserves the next push.",
    ])
    return "\n".join(lines) + "\n"


def render_nightly_plan(plan: NightlyPlan) -> str:
    lines = [
        f"# Signup-First Night Plan - {plan.generated_at[:10]}",
        "",
        f"Focus lane: **{plan.focus_product or 'None yet'}**",
        "",
        "## Tonight's Moves",
        "",
    ]
    focus = plan.scoreboard[0] if plan.scoreboard else None
    if focus:
        lines.append(f"1. Work the highest-intent {focus.product} follow-ups before building new surface area.")
        lines.append(f"2. Advance one experiment: {focus.next_experiment}")
        lines.append("3. Fix only the source-health gaps that block signup learning.")
    else:
        lines.append("1. Seed the first signup experiment and confirm capture works end to end.")
    lines.extend(["", "## Active Experiments", ""])
    if not plan.active_experiments:
        lines.append("- No running experiments. Start exactly one before adding more.")
    for row in plan.active_experiments:
        lines.append(f"- [{row.priority}] **{row.product}** - {row.title} (`{row.id}`)")
        lines.append(f"  Next: {row.next_step}")
    lines.extend(["", "## Backlog Bench", ""])
    for row in plan.backlog[:8]:
        lines.append(f"- [{row.priority}] `{row.status}` **{row.product}** - {row.title} (`{row.id}`)")
    lines.extend(["", "## Follow-up Queue", ""])
    if not plan.top_leads:
        lines.append("- No open leads in the local queue.")
    for lead in plan.top_leads:
        lines.append(
            f"- [{lead.intent_score}] **{lead.product}** {lead.event_type}: "
            f"{lead.company or lead.email or '-'} (`{lead.id}`)"
        )
    broker = plan.broker_close_state
    lines.extend(["", "## Broker Close State", ""])
    lines.append(
        f"- Total: {broker.total}; hot unworked: {broker.hot_unworked}; "
        f"stale hot: {broker.stale_hot}; booked: {broker.booked}; won: {broker.won}"
    )
    for lead in broker.top_actions[:5]:
        lines.append(
            f"- [{lead.intent_score}] **{lead.status}** {lead.company or lead.email}: "
            f"{lead.recommended_workflow or 'review workflow'}"
        )
    lines.extend(["", "## Source Watch", ""])
    if not plan.watch_sources:
        lines.append("- No watch sources right now.")
    for row in plan.watch_sources[:8]:
        lines.append(f"- **{row.product}** / `{row.source_table}`: {row.note}")
    lines.extend([
        "",
        "## Rule",
        "",
        "Signup-first only. No Stripe/payment work until the queue proves enough demand to justify conversion work.",
    ])
    return "\n".join(lines) + "\n"


def write_weekly_review(review: DemandReview, *, path: Path | None = None) -> Path:
    target = path or _default_review_path(review.generated_at)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_review(review), encoding="utf-8")
    return target


def write_nightly_plan(plan: NightlyPlan, *, path: Path | None = None) -> Path:
    target = path or _default_nightly_path(plan.generated_at)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_nightly_plan(plan), encoding="utf-8")
    return target


def write_status_metrics(review: DemandReview, *, path: Path | None = None) -> dict[str, Any]:
    """Write compact demand metrics into the operator status file."""
    from .utils import status as status_mod

    top_product = review.scoreboard[0] if review.scoreboard else None
    payload = {
        "generated_at": review.generated_at,
        "top_product": top_product.product if top_product else None,
        "top_score": top_product.demand_score if top_product else 0,
        "products": [row.to_dict() for row in review.scoreboard],
        "watch_sources": [
            row.to_dict() for row in review.source_health if row.health == "watch"
        ],
        "top_experiments": [row.to_dict() for row in review.experiments[:5]],
        "broker_close_state": review.broker_close_state.to_dict(),
    }
    status_mod.write_status("demand_os", payload, path)
    return payload


def write_nightly_status(plan: NightlyPlan, *, path: Path | None = None) -> dict[str, Any]:
    from .utils import status as status_mod

    payload = {
        "generated_at": plan.generated_at,
        "focus_product": plan.focus_product,
        "running_experiments": len(plan.active_experiments),
        "open_experiments": len(plan.backlog),
        "watch_sources": [row.to_dict() for row in plan.watch_sources[:8]],
        "top_leads": [_lead_summary(lead) for lead in plan.top_leads[:5]],
        "broker_close_state": plan.broker_close_state.to_dict(),
    }
    status_mod.write_status("nightly_demand_plan", payload, path)
    return payload


def run_weekly_review(
    *,
    store: LeadStore | None = None,
    write_file: bool = True,
    review_path: Path | None = None,
    status_path: Path | None = None,
) -> dict[str, Any]:
    """Build the weekly demand review, optionally writing local artifacts."""
    store = store or LeadStore()
    review = build_review(store)
    written_path = write_weekly_review(review, path=review_path) if write_file else None
    status_payload = write_status_metrics(review, path=status_path)
    return {
        "review": review.to_dict(),
        "status": status_payload,
        "path": str(written_path) if written_path else None,
        "text": render_review(review),
    }


def run_nightly_plan(
    *,
    store: LeadStore | None = None,
    experiment_store: ExperimentStore | None = None,
    write_file: bool = True,
    plan_path: Path | None = None,
    status_path: Path | None = None,
) -> dict[str, Any]:
    store = store or LeadStore()
    experiment_store = experiment_store or ExperimentStore(store.db_path)
    plan = build_nightly_plan(store, experiment_store)
    written_path = write_nightly_plan(plan, path=plan_path) if write_file else None
    status_payload = write_nightly_status(plan, path=status_path)
    return {
        "plan": plan.to_dict(),
        "status": status_payload,
        "path": str(written_path) if written_path else None,
        "text": render_nightly_plan(plan),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _record_age_hours(record: LeadRecord, now: datetime) -> float | None:
    ts = _parse_time(record.event_created_at) or _parse_time(record.first_seen_at)
    if ts is None:
        return None
    return max(0.0, (now - ts).total_seconds() / 3600)


def _is_broker_close_record(record: LeadRecord) -> bool:
    if record.product != "AI Ops Consulting":
        return False
    return (
        record.event_type == "broker_workflow_audit"
        or record.metadata.get("source") == "broker_workflow_audit"
    )


def _merge_broker_close_records(records: list[LeadRecord], *, now: datetime) -> BrokerCloseLead:
    audit_record = next((r for r in records if r.event_type == "broker_workflow_audit"), None)
    lead_record = next((r for r in records if r.metadata.get("source") == "broker_workflow_audit"), None)
    primary = audit_record or lead_record or records[0]
    status_source = lead_record or primary
    metadata = audit_record.metadata if audit_record else primary.metadata
    return BrokerCloseLead(
        email=primary.email or "",
        company=primary.company or (lead_record.company if lead_record else None),
        status=_broker_close_status(status_source),
        source_status=_raw_broker_status(status_source),
        audit_lead_id=audit_record.id if audit_record else None,
        synced_lead_id=lead_record.id if lead_record else None,
        intent_score=max(record.intent_score for record in records),
        recommended_workflow=_metadata_str(metadata.get("recommended_workflow")),
        urgency=_metadata_str(metadata.get("urgency")),
        estimated_hours_saved_per_week=metadata.get("estimated_hours_saved_per_week"),
        age_hours=_record_age_hours(primary, now),
    )


def _broker_close_status(record: LeadRecord) -> str:
    raw = _raw_broker_status(record) or record.status
    normalized = raw.strip().upper().replace("-", "_")
    if normalized in {"NEW", "CONTACTED", "BOOKED", "WON", "LOST", "NURTURE"}:
        return normalized
    if normalized in {"REPLIED", "FOLLOW_UP"}:
        return "CONTACTED"
    if normalized in {"CLOSED"}:
        return "WON"
    return "NEW"


def _raw_broker_status(record: LeadRecord) -> str | None:
    value = record.metadata.get("status")
    if value is None:
        return None
    return str(value)


def _broker_close_priority(lead: BrokerCloseLead) -> tuple[int, int, float]:
    status_weight = {
        "NEW": 5,
        "CONTACTED": 4,
        "NURTURE": 3,
        "BOOKED": 2,
        "WON": 1,
        "LOST": 0,
    }.get(lead.status, 0)
    urgency_weight = 2 if lead.urgency == "this-week" else 1 if lead.urgency == "this-month" else 0
    return (status_weight + urgency_weight, lead.intent_score, lead.age_hours or 0)


def _metadata_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


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


def _sources_by_product(records: list[LeadRecord]) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for record in records:
        out.setdefault(record.product, set()).add(record.source_table)
    return out


def _demand_score(
    *,
    open_leads: int,
    high_intent: int,
    stale_hot: int,
    new_7d: int,
    source_count: int,
    contacted: int,
) -> int:
    score = 0
    score += min(open_leads * 3, 30)
    score += min(high_intent * 12, 42)
    score += min(new_7d * 4, 16)
    score += min(source_count * 5, 10)
    score += min(contacted * 2, 8)
    score -= min(stale_hot * 5, 20)
    return max(0, min(100, score))


def _verdict(score: int, stale_hot: int, open_leads: int) -> str:
    if open_leads == 0:
        return "No signal yet. Run a sharper signup or lead-magnet experiment."
    if stale_hot >= 3:
        return "Demand exists, but follow-up debt is becoming the bottleneck."
    if score >= 70:
        return "Strongest current demand lane. Prioritize follow-up and one focused experiment."
    if score >= 40:
        return "Some signal. Improve capture context and run a targeted distribution test."
    return "Weak signal. Keep lightweight unless a specific niche pulls interest."


def _pick_experiment(product: str, score: int, stale_hot: int, source_count: int) -> str:
    experiments = PRODUCT_PROFILES[product]["experiments"]
    if stale_hot:
        return "Clear stale hot leads first, then run: " + experiments[0]
    if source_count == 0:
        return "Verify signup source capture before distribution."
    if score >= 70:
        return experiments[0]
    if score >= 40:
        return experiments[1]
    return experiments[-1]


def _experiment_hypothesis(row: ProductDemand, title: str, has_missing_sources: bool) -> str:
    if has_missing_sources:
        return f"If {row.product} captures cleaner source context, we can separate real demand from noise."
    if row.high_intent:
        return f"If we focus {row.product} around its highest-intent behavior, more signups should become follow-upable."
    return f"If we give {row.product} a sharper niche offer, it should create a first measurable demand signal."


def _experiment_next_step(product: str, title: str, missing_sources: list[SourceHealth]) -> str:
    if missing_sources:
        source = missing_sources[0]
        return f"Instrument `{source.source_table}` path, then launch: {title}."
    return f"Create the smallest signup-first version of: {title}."


def _lead_summary(lead: LeadRecord) -> dict[str, Any]:
    return {
        "id": lead.id,
        "product": lead.product,
        "event_type": lead.event_type,
        "company": lead.company,
        "email": lead.email,
        "intent_score": lead.intent_score,
        "status": lead.status,
    }


def _experiment_from_row(row: sqlite3.Row) -> ExperimentRecord:
    return ExperimentRecord(
        id=row["id"],
        product=row["product"],
        title=row["title"],
        hypothesis=row["hypothesis"],
        success_metric=row["success_metric"],
        priority=int(row["priority"] or 0),
        effort=row["effort"],
        status=row["status"],
        next_step=row["next_step"] or "",
        notes_md=row["notes_md"] or "",
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        started_at=row["started_at"],
        completed_at=row["completed_at"],
    )


def _normalize_experiment_status(status: str) -> str:
    normalized = status.strip().upper().replace("-", "_")
    if normalized not in VALID_EXPERIMENT_STATUSES:
        allowed = ", ".join(sorted(VALID_EXPERIMENT_STATUSES))
        raise ValueError(f"Invalid experiment status {status!r}. Use one of: {allowed}")
    return normalized


def _slug(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-").replace("--", "-")


def _default_review_path(generated_at: str) -> Path:
    date = generated_at[:10]
    try:
        settings = load_settings()
        root = settings.projects_root
    except ConfigError:
        root = Path.cwd()
    return Path(root) / f"GROWTH_REVIEW_{date}.md"


def _default_nightly_path(generated_at: str) -> Path:
    date = generated_at[:10]
    try:
        settings = load_settings()
        root = settings.projects_root
    except ConfigError:
        root = Path.cwd()
    return Path(root) / f"NIGHTLY_DEMAND_PLAN_{date}.md"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="operator demand")
    parser.add_argument("--db-path", type=Path, default=None)
    sub = parser.add_subparsers(dest="command")

    scoreboard = sub.add_parser("scoreboard", help="Rank products by current demand")
    scoreboard.add_argument("--json", action="store_true")

    health = sub.add_parser("health", help="Show signup source health")
    health.add_argument("--json", action="store_true")

    experiments = sub.add_parser("experiments", help="Generate growth experiment backlog")
    experiments.add_argument("--limit", type=int, default=12)
    experiments.add_argument("--json", action="store_true")

    backlog = sub.add_parser("backlog", help="Show persistent growth experiment registry")
    backlog.add_argument("--seed", action="store_true", help="Refresh generated backlog first")
    backlog.add_argument("--status", default=None)
    backlog.add_argument("--product", default=None)
    backlog.add_argument("--all", action="store_true", help="Include shipped/killed experiments")
    backlog.add_argument("--limit", type=int, default=20)
    backlog.add_argument("--json", action="store_true")

    experiment = sub.add_parser("experiment", help="Show or update one persistent experiment")
    experiment.add_argument("experiment_id")
    experiment.add_argument(
        "action",
        nargs="?",
        default="show",
        choices=("show", "start", "pause", "backlog", "ship", "kill", "note"),
    )
    experiment.add_argument("note", nargs="*")
    experiment.add_argument("--json", action="store_true")

    journey = sub.add_parser("journey", help="Show one lead's journey timeline")
    journey.add_argument("lead_ref")

    nightly = sub.add_parser("nightly", help="Build the signup-first nightly plan")
    nightly.add_argument("--write", action="store_true")
    nightly.add_argument("--path", type=Path, default=None)
    nightly.add_argument("--write-status", action="store_true")
    nightly.add_argument("--json", action="store_true")

    weekly = sub.add_parser("weekly", help="Render or write weekly growth review")
    weekly.add_argument("--write", action="store_true")
    weekly.add_argument("--path", type=Path, default=None)
    weekly.add_argument("--write-status", action="store_true")
    weekly.add_argument("--json", action="store_true")

    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    except Exception:
        pass
    parser = _build_parser()
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    store = LeadStore(args.db_path)
    experiment_store = ExperimentStore(store.db_path)

    try:
        if args.command == "scoreboard":
            rows = build_scoreboard(store)
            if args.json:
                print(json.dumps([row.to_dict() for row in rows], indent=2))
            else:
                print(render_scoreboard(rows))
            return 0

        if args.command == "health":
            rows = build_source_health(store)
            if args.json:
                print(json.dumps([row.to_dict() for row in rows], indent=2))
            else:
                print(render_source_health(rows))
            return 0

        if args.command == "experiments":
            rows = build_experiments(store, limit=args.limit)
            if args.json:
                print(json.dumps([row.to_dict() for row in rows], indent=2))
            else:
                print(render_experiments(rows))
            return 0

        if args.command == "backlog":
            if args.seed:
                inserted, refreshed = seed_experiment_backlog(store, experiment_store, limit=max(args.limit, 15))
                if not args.json:
                    print(f"[demand] seeded backlog inserted={inserted} refreshed={refreshed}")
            rows = experiment_store.list(
                status=args.status,
                product=args.product,
                include_done=args.all,
                limit=args.limit,
            )
            if args.json:
                print(json.dumps([row.to_dict() for row in rows], indent=2))
            else:
                print(render_experiment_backlog(rows))
            return 0

        if args.command == "experiment":
            if args.action == "show":
                row = experiment_store.get(args.experiment_id)
            elif args.action == "note":
                row = experiment_store.add_note(args.experiment_id, " ".join(args.note))
            else:
                status_map = {
                    "start": "RUNNING",
                    "pause": "PAUSED",
                    "backlog": "BACKLOG",
                    "ship": "SHIPPED",
                    "kill": "KILLED",
                }
                row = experiment_store.mark(
                    args.experiment_id,
                    status_map[args.action],
                    note=" ".join(args.note) if args.note else None,
                )
            if args.json:
                print(json.dumps(row.to_dict(), indent=2))
            else:
                print(render_experiment_record(row))
            return 0

        if args.command == "nightly":
            plan = build_nightly_plan(store, experiment_store)
            if args.json:
                print(json.dumps(plan.to_dict(), indent=2))
                return 0
            text = render_nightly_plan(plan)
            if args.write_status:
                write_nightly_status(plan)
            if args.write:
                path = write_nightly_plan(plan, path=args.path)
                print(f"[demand] wrote {path}")
            print(text)
            return 0

        if args.command == "journey":
            print(render_journey(store.get(args.lead_ref)))
            return 0

        if args.command == "weekly":
            review = build_review(store)
            if args.json:
                print(json.dumps(review.to_dict(), indent=2))
                return 0
            text = render_review(review)
            if args.write_status:
                write_status_metrics(review)
            if args.write:
                path = write_weekly_review(review, path=args.path)
                print(f"[demand] wrote {path}")
            print(text)
            return 0
    except (KeyError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
