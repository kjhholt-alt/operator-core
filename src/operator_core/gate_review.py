"""Gate disagreement review queue.

Backs `operator outreach gate-review` and the audit-report's "triaged%"
metric. Each gate disagreement (would_block_new / would_allow_new /
both_block_diff_reason) found in the gate_audit event stream becomes a
row in this queue with a status the user resolves through.

Status workflow
---------------
    pending          -- newly ingested; nobody has looked yet
    approved_gate    -- the gate's decision is right; legacy needs fixing
                        (or, in the would_block case, the business should
                        join the suppression list permanently)
    approved_legacy  -- legacy is right; the gate has a bug to fix
    fix_gate         -- triaged but blocked on a code change to outreach-common
    fix_legacy       -- triaged but blocked on a code change in the product
    suppressed       -- already auto-suppressed via PR; awaits merge

Schema
------
    review_items
      id              INTEGER PK
      product         TEXT NOT NULL          -- outreach-engine, ...
      lead_hash       TEXT NOT NULL          -- correlate to ndjson event
      business_name   TEXT
      agreement       TEXT NOT NULL          -- would_block_new / ...
      gate_block_label TEXT
      legacy_block_reason TEXT
      first_seen_ts   TEXT NOT NULL          -- ISO 8601 UTC
      last_seen_ts    TEXT NOT NULL
      hit_count       INTEGER NOT NULL DEFAULT 1
      status          TEXT NOT NULL DEFAULT 'pending'
      resolution_note TEXT
      resolved_by     TEXT
      resolved_ts     TEXT
      UNIQUE(product, lead_hash, agreement)

The (product, lead_hash, agreement) unique constraint means re-ingesting
the same audit log just bumps hit_count + last_seen_ts; it doesn't
duplicate rows.
"""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, List, Optional

# Statuses that count as "triaged" for the cut-over rule.
TRIAGED_STATUSES = frozenset(
    {"approved_gate", "approved_legacy", "fix_gate", "fix_legacy", "suppressed"}
)
ALL_STATUSES = frozenset(TRIAGED_STATUSES | {"pending"})

_SCHEMA = """
CREATE TABLE IF NOT EXISTS review_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product TEXT NOT NULL,
    lead_hash TEXT NOT NULL,
    business_name TEXT,
    agreement TEXT NOT NULL,
    gate_block_label TEXT,
    legacy_block_reason TEXT,
    first_seen_ts TEXT NOT NULL,
    last_seen_ts TEXT NOT NULL,
    hit_count INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    resolution_note TEXT,
    resolved_by TEXT,
    resolved_ts TEXT,
    UNIQUE(product, lead_hash, agreement)
);
CREATE INDEX IF NOT EXISTS idx_review_status ON review_items(product, status);
CREATE INDEX IF NOT EXISTS idx_review_pending ON review_items(status, last_seen_ts);
"""


@dataclass
class ReviewItem:
    id: Optional[int]
    product: str
    lead_hash: str
    business_name: Optional[str]
    agreement: str
    gate_block_label: Optional[str]
    legacy_block_reason: Optional[str]
    first_seen_ts: str
    last_seen_ts: str
    hit_count: int
    status: str
    resolution_note: Optional[str]
    resolved_by: Optional[str]
    resolved_ts: Optional[str]


def default_db_path() -> Path:
    override = os.environ.get("OUTREACH_GATE_REVIEW_DB")
    if override:
        return Path(override)
    return Path.home() / ".operator" / "data" / "outreach" / "gate_review.sqlite"


@contextmanager
def open_db(path: Optional[Path] = None) -> Iterator[sqlite3.Connection]:
    target = path or default_db_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target))
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(_SCHEMA)
        yield conn
        conn.commit()
    finally:
        conn.close()


def _row_to_item(row: sqlite3.Row) -> ReviewItem:
    return ReviewItem(**{k: row[k] for k in row.keys()})


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_disagreement(agreement: str) -> bool:
    """Only disagreements get queued -- matches don't need triage."""
    return agreement in ("would_block_new", "would_allow_new", "both_block_diff_reason")


def ingest_events(events: Iterable[dict],
                  db_path: Optional[Path] = None) -> tuple[int, int]:
    """Walk gate_audit envelopes, upsert one row per (product, lead_hash, agreement).

    Returns (new_count, updated_count).
    """
    new = 0
    updated = 0
    with open_db(db_path) as conn:
        for env in events:
            payload = env.get("payload") or {}
            agreement = payload.get("agreement")
            if not agreement or not _is_disagreement(agreement):
                continue
            product = payload.get("product")
            lead_hash = payload.get("lead_hash")
            if not product or not lead_hash:
                continue
            ts = env.get("ts") or _now()
            cur = conn.execute(
                "SELECT id, hit_count FROM review_items "
                "WHERE product=? AND lead_hash=? AND agreement=?",
                (product, lead_hash, agreement),
            )
            row = cur.fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO review_items "
                    "(product, lead_hash, business_name, agreement, "
                    " gate_block_label, legacy_block_reason, "
                    " first_seen_ts, last_seen_ts) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (product, lead_hash,
                     payload.get("lead_business_name"),
                     agreement,
                     payload.get("gate_block_label"),
                     payload.get("legacy_block_reason"),
                     ts, ts),
                )
                new += 1
            else:
                conn.execute(
                    "UPDATE review_items SET hit_count = hit_count + 1, "
                    "last_seen_ts = ? WHERE id = ?",
                    (ts, row["id"]),
                )
                updated += 1
    return (new, updated)


def list_pending(product: Optional[str] = None,
                 limit: int = 50,
                 db_path: Optional[Path] = None) -> List[ReviewItem]:
    with open_db(db_path) as conn:
        if product:
            cur = conn.execute(
                "SELECT * FROM review_items WHERE status='pending' AND product=? "
                "ORDER BY last_seen_ts DESC LIMIT ?",
                (product, limit),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM review_items WHERE status='pending' "
                "ORDER BY last_seen_ts DESC LIMIT ?",
                (limit,),
            )
        return [_row_to_item(r) for r in cur.fetchall()]


def get_item(item_id: int, db_path: Optional[Path] = None) -> Optional[ReviewItem]:
    with open_db(db_path) as conn:
        cur = conn.execute("SELECT * FROM review_items WHERE id=?", (item_id,))
        row = cur.fetchone()
        return _row_to_item(row) if row else None


def resolve(item_id: int,
            status: str,
            *,
            note: Optional[str] = None,
            resolved_by: Optional[str] = None,
            db_path: Optional[Path] = None) -> ReviewItem:
    """Set the resolution status on a queue row. Raises ValueError on bad status."""
    if status not in ALL_STATUSES:
        raise ValueError(f"unknown status {status!r}; expected one of {sorted(ALL_STATUSES)}")
    if status == "pending":
        raise ValueError("can't resolve back to pending; use re-ingest if needed")
    with open_db(db_path) as conn:
        conn.execute(
            "UPDATE review_items SET status=?, resolution_note=?, "
            "resolved_by=?, resolved_ts=? WHERE id=?",
            (status, note, resolved_by, _now(), item_id),
        )
        cur = conn.execute("SELECT * FROM review_items WHERE id=?", (item_id,))
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"no review item with id={item_id}")
        return _row_to_item(row)


@dataclass
class ProductTriage:
    product: str
    total: int
    pending: int
    triaged: int

    @property
    def triaged_pct(self) -> float:
        return (self.triaged / self.total * 100.0) if self.total else 100.0


def triage_summary(db_path: Optional[Path] = None) -> List[ProductTriage]:
    with open_db(db_path) as conn:
        cur = conn.execute(
            "SELECT product, status, COUNT(*) AS n FROM review_items "
            "GROUP BY product, status"
        )
        by_product: dict[str, ProductTriage] = {}
        for row in cur.fetchall():
            product = row["product"]
            status = row["status"]
            n = row["n"]
            t = by_product.setdefault(product, ProductTriage(product, 0, 0, 0))
            t.total += n
            if status == "pending":
                t.pending += n
            elif status in TRIAGED_STATUSES:
                t.triaged += n
        return sorted(by_product.values(), key=lambda x: x.product)


def is_fully_triaged(product: str, db_path: Optional[Path] = None) -> bool:
    """True iff this product has zero pending review rows."""
    summary = {t.product: t for t in triage_summary(db_path)}
    t = summary.get(product)
    if t is None:
        return True  # no rows = nothing to triage
    return t.pending == 0
