"""Reply ledger — every inbound outreach reply gets a thread record.

Data model (SQLite tables in the same DB the rest of Operator uses):

    outreach_reply_threads
        thread_id PK, lead_id (nullable), sender_email, sender_name,
        subject, first_received_at, last_activity_at, status,
        dd_notes_md

    outreach_reply_messages
        id PK, thread_id FK, direction ("in"|"out"),
        sent_at (nullable, set on OUT+sent), received_at (nullable),
        subject, body_md, meta_json, created_at

Status lifecycle:
    NEW        — reply just landed; no draft yet.
    DRAFTING   — someone (Claude or Kruz) is composing a follow-up.
    READY      — draft saved, waiting on Kruz to approve + send.
    SENT       — follow-up was sent; thread is quiet until next inbound.
    CLOSED     — manually closed (negative reply, opt-out, bought).

The module is intentionally pure Python + stdlib sqlite — no ORM, no
async. Every operation is idempotent by thread_id so CLI calls and
webhook handlers can retry safely.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

STATUS_NEW = "NEW"
STATUS_DRAFTING = "DRAFTING"
STATUS_READY = "READY"
STATUS_SENT = "SENT"
STATUS_CLOSED = "CLOSED"
ALL_STATUSES = (
    STATUS_NEW,
    STATUS_DRAFTING,
    STATUS_READY,
    STATUS_SENT,
    STATUS_CLOSED,
)

# "Unread" from the operator's perspective: anything that needs action.
UNREAD_STATUSES = (STATUS_NEW, STATUS_DRAFTING, STATUS_READY)


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

THREADS_DDL = """
CREATE TABLE IF NOT EXISTS outreach_reply_threads (
    thread_id         TEXT PRIMARY KEY,
    lead_id           TEXT,
    sender_email      TEXT NOT NULL,
    sender_name       TEXT,
    subject           TEXT NOT NULL DEFAULT '',
    first_received_at TEXT NOT NULL,
    last_activity_at  TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'NEW',
    dd_notes_md       TEXT NOT NULL DEFAULT ''
);
"""

MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS outreach_reply_messages (
    id            TEXT PRIMARY KEY,
    thread_id     TEXT NOT NULL,
    direction     TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    sent_at       TEXT,
    received_at   TEXT,
    subject       TEXT NOT NULL DEFAULT '',
    body_md       TEXT NOT NULL DEFAULT '',
    meta_json     TEXT NOT NULL DEFAULT '{}',
    created_at    TEXT NOT NULL,
    FOREIGN KEY (thread_id) REFERENCES outreach_reply_threads(thread_id)
);
"""

INDEXES_DDL = (
    "CREATE INDEX IF NOT EXISTS reply_threads_status "
    "ON outreach_reply_threads (status);",
    "CREATE INDEX IF NOT EXISTS reply_threads_last_activity "
    "ON outreach_reply_threads (last_activity_at);",
    "CREATE INDEX IF NOT EXISTS reply_messages_thread "
    "ON outreach_reply_messages (thread_id);",
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ReplyThread:
    thread_id: str
    sender_email: str
    sender_name: str | None = None
    subject: str = ""
    status: str = STATUS_NEW
    first_received_at: str = ""
    last_activity_at: str = ""
    dd_notes_md: str = ""
    lead_id: str | None = None


@dataclass
class ReplyMessage:
    id: str
    thread_id: str
    direction: str  # "in" or "out"
    subject: str = ""
    body_md: str = ""
    received_at: str | None = None
    sent_at: str | None = None
    meta: dict[str, Any] = field(default_factory=dict)
    created_at: str = ""


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _thread_id(sender_email: str, subject: str) -> str:
    """Stable thread id from (sender_email, normalized subject).

    Strips "Re:" prefixes so reply-chains collapse into one thread.
    """
    normalized = subject.strip()
    while normalized.lower().startswith(("re:", "fw:", "fwd:")):
        normalized = normalized.split(":", 1)[1].strip()
    key = f"{sender_email.lower().strip()}::{normalized.lower()}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


class ReplyStore:
    """SQLite-backed reply ledger."""

    def __init__(self, db_path: Path):
        self.db_path = Path(str(db_path))
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(THREADS_DDL)
            conn.execute(MESSAGES_DDL)
            for stmt in INDEXES_DDL:
                conn.execute(stmt)

    # ---- threads -----------------------------------------------------------

    def upsert_thread_for_incoming(
        self,
        *,
        sender_email: str,
        sender_name: str | None,
        subject: str,
        body_md: str,
        received_at: str | None = None,
        lead_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> ReplyThread:
        """Add an inbound reply. Creates the thread if needed, appends the
        message, bumps last_activity_at. Status snaps to NEW on new
        inbound activity unless the thread is already DRAFTING / READY
        (in which case we keep that state so a draft-in-progress isn't
        lost)."""
        tid = _thread_id(sender_email, subject)
        now = received_at or _now()
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT * FROM outreach_reply_threads WHERE thread_id = ?",
                (tid,),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO outreach_reply_threads "
                    "(thread_id, lead_id, sender_email, sender_name, "
                    " subject, first_received_at, last_activity_at, status, "
                    " dd_notes_md) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        tid,
                        lead_id,
                        sender_email,
                        sender_name,
                        subject,
                        now,
                        now,
                        STATUS_NEW,
                        "",
                    ),
                )
            else:
                new_status = existing["status"]
                if new_status in (STATUS_SENT, STATUS_CLOSED):
                    # New reply after we sent → reset to NEW for another pass.
                    new_status = STATUS_NEW
                conn.execute(
                    "UPDATE outreach_reply_threads "
                    "SET last_activity_at = ?, status = ? "
                    "WHERE thread_id = ?",
                    (now, new_status, tid),
                )

            conn.execute(
                "INSERT INTO outreach_reply_messages "
                "(id, thread_id, direction, received_at, subject, body_md, "
                " meta_json, created_at) "
                "VALUES (?, ?, 'in', ?, ?, ?, ?, ?)",
                (
                    uuid.uuid4().hex[:12],
                    tid,
                    now,
                    subject,
                    body_md,
                    json.dumps(meta or {}, sort_keys=True),
                    _now(),
                ),
            )
        return self.get_thread(tid)

    def save_draft(
        self,
        thread_id: str,
        *,
        body_md: str,
        subject: str | None = None,
        dd_notes_md: str | None = None,
    ) -> ReplyThread:
        """Save a draft outbound message for a thread. Moves status to
        DRAFTING. Idempotent on repeated calls — the last draft wins."""
        now = _now()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM outreach_reply_threads WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"unknown thread_id: {thread_id}")

            next_subject = subject or row["subject"] or ""
            dd = dd_notes_md if dd_notes_md is not None else row["dd_notes_md"]
            conn.execute(
                "UPDATE outreach_reply_threads "
                "SET status = ?, last_activity_at = ?, dd_notes_md = ? "
                "WHERE thread_id = ?",
                (STATUS_DRAFTING, now, dd, thread_id),
            )
            # Replace any existing non-sent draft (direction=out, sent_at IS NULL).
            conn.execute(
                "DELETE FROM outreach_reply_messages "
                "WHERE thread_id = ? AND direction = 'out' AND sent_at IS NULL",
                (thread_id,),
            )
            conn.execute(
                "INSERT INTO outreach_reply_messages "
                "(id, thread_id, direction, subject, body_md, meta_json, "
                " created_at) "
                "VALUES (?, ?, 'out', ?, ?, ?, ?)",
                (
                    uuid.uuid4().hex[:12],
                    thread_id,
                    next_subject,
                    body_md,
                    "{}",
                    now,
                ),
            )
        return self.get_thread(thread_id)

    def mark_ready(self, thread_id: str) -> ReplyThread:
        """Draft is done; waiting on the human to hit send."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE outreach_reply_threads "
                "SET status = ?, last_activity_at = ? "
                "WHERE thread_id = ?",
                (STATUS_READY, _now(), thread_id),
            )
        return self.get_thread(thread_id)

    def mark_sent(self, thread_id: str, *, sent_at: str | None = None) -> ReplyThread:
        """Mark the latest pending outbound as sent; move status to SENT."""
        now = sent_at or _now()
        with self._connect() as conn:
            conn.execute(
                "UPDATE outreach_reply_messages "
                "SET sent_at = ? "
                "WHERE thread_id = ? AND direction = 'out' AND sent_at IS NULL",
                (now, thread_id),
            )
            conn.execute(
                "UPDATE outreach_reply_threads "
                "SET status = ?, last_activity_at = ? "
                "WHERE thread_id = ?",
                (STATUS_SENT, now, thread_id),
            )
        return self.get_thread(thread_id)

    def close_thread(self, thread_id: str) -> ReplyThread:
        with self._connect() as conn:
            conn.execute(
                "UPDATE outreach_reply_threads "
                "SET status = ?, last_activity_at = ? "
                "WHERE thread_id = ?",
                (STATUS_CLOSED, _now(), thread_id),
            )
        return self.get_thread(thread_id)

    def get_thread(self, thread_id: str) -> ReplyThread:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM outreach_reply_threads WHERE thread_id = ?",
                (thread_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown thread_id: {thread_id}")
        return _row_to_thread(row)

    def list_threads(
        self,
        *,
        status: str | None = None,
        limit: int = 50,
    ) -> list[ReplyThread]:
        query = "SELECT * FROM outreach_reply_threads"
        params: list[Any] = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY last_activity_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [_row_to_thread(r) for r in rows]

    def list_messages(self, thread_id: str) -> list[ReplyMessage]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM outreach_reply_messages "
                "WHERE thread_id = ? ORDER BY created_at ASC",
                (thread_id,),
            ).fetchall()
        return [_row_to_message(r) for r in rows]

    # ---- aggregates --------------------------------------------------------

    def summary(self) -> dict[str, int]:
        """Count threads by status, plus a 7-day SENT count for /kruz."""
        out = {s: 0 for s in ALL_STATUSES}
        with self._connect() as conn:
            for row in conn.execute(
                "SELECT status, COUNT(*) AS n FROM outreach_reply_threads "
                "GROUP BY status"
            ):
                out[row["status"]] = int(row["n"])
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=7)
            ).isoformat()
            sent_7d_row = conn.execute(
                "SELECT COUNT(*) AS n FROM outreach_reply_messages "
                "WHERE direction = 'out' AND sent_at IS NOT NULL AND sent_at >= ?",
                (cutoff,),
            ).fetchone()
            out["sent_7d"] = int(sent_7d_row["n"])
        out["unread"] = sum(out.get(s, 0) for s in UNREAD_STATUSES)
        return out


def _row_to_thread(row: sqlite3.Row) -> ReplyThread:
    return ReplyThread(
        thread_id=row["thread_id"],
        lead_id=row["lead_id"],
        sender_email=row["sender_email"],
        sender_name=row["sender_name"],
        subject=row["subject"],
        status=row["status"],
        first_received_at=row["first_received_at"],
        last_activity_at=row["last_activity_at"],
        dd_notes_md=row["dd_notes_md"] or "",
    )


def _row_to_message(row: sqlite3.Row) -> ReplyMessage:
    try:
        meta = json.loads(row["meta_json"] or "{}")
    except ValueError:
        meta = {}
    return ReplyMessage(
        id=row["id"],
        thread_id=row["thread_id"],
        direction=row["direction"],
        subject=row["subject"],
        body_md=row["body_md"],
        received_at=row["received_at"],
        sent_at=row["sent_at"],
        meta=meta,
        created_at=row["created_at"],
    )
