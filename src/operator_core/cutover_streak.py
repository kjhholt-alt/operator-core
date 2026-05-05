"""SQLite-backed READY-streak tracker for the cut-over promoter.

Records, per product:
- ``streak_start_ts``: when the current uninterrupted READY streak began.
                        NULL if not currently READY.
- ``last_check_ts``:   when we last saw any signal (READY or not).
- ``last_ready_state``: bool, last observed READY state.
- ``promoted_ts``:     when we successfully opened the flag-flip PR.
                        NULL if not yet promoted.

API:
- ``record_check(product, is_ready)`` -- update one product's state
- ``streak_seconds(product)``        -- length of current streak (0 if not READY)
- ``get(product)``                    -- read full row
- ``mark_promoted(product, pr_url)``  -- record a successful flag-flip PR
- ``list_all()``                      -- read everything

Storage lives at ``~/.operator/data/outreach/cutover_streak.sqlite`` by
default. Override with ``OPERATOR_CUTOVER_STREAK_DB``.
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS streaks (
  product           TEXT PRIMARY KEY,
  streak_start_ts   TEXT,
  last_check_ts     TEXT NOT NULL,
  last_ready_state  INTEGER NOT NULL,
  promoted_ts       TEXT,
  promoted_pr_url   TEXT
);
"""


@dataclass
class Streak:
    product: str
    streak_start_ts: Optional[str]
    last_check_ts: str
    last_ready_state: bool
    promoted_ts: Optional[str]
    promoted_pr_url: Optional[str]


def default_db_path() -> Path:
    override = os.environ.get("OPERATOR_CUTOVER_STREAK_DB")
    if override:
        return Path(override)
    return Path.home() / ".operator" / "data" / "outreach" / "cutover_streak.sqlite"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


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


def _row_to_streak(row: sqlite3.Row) -> Streak:
    return Streak(
        product=row["product"],
        streak_start_ts=row["streak_start_ts"],
        last_check_ts=row["last_check_ts"],
        last_ready_state=bool(row["last_ready_state"]),
        promoted_ts=row["promoted_ts"],
        promoted_pr_url=row["promoted_pr_url"],
    )


def get(product: str, db_path: Optional[Path] = None) -> Optional[Streak]:
    with open_db(db_path) as conn:
        cur = conn.execute("SELECT * FROM streaks WHERE product=?", (product,))
        row = cur.fetchone()
        return _row_to_streak(row) if row else None


def list_all(db_path: Optional[Path] = None) -> List[Streak]:
    with open_db(db_path) as conn:
        cur = conn.execute("SELECT * FROM streaks ORDER BY product")
        return [_row_to_streak(r) for r in cur.fetchall()]


def record_check(product: str, is_ready: bool, *, db_path: Optional[Path] = None,
                  now_ts: Optional[str] = None) -> Streak:
    """Update one product's streak state. Returns the new row."""
    now_ts = now_ts or _now()
    with open_db(db_path) as conn:
        cur = conn.execute("SELECT * FROM streaks WHERE product=?", (product,))
        row = cur.fetchone()

        if row is None:
            streak_start = now_ts if is_ready else None
            conn.execute(
                "INSERT INTO streaks "
                "(product, streak_start_ts, last_check_ts, last_ready_state, promoted_ts, promoted_pr_url) "
                "VALUES (?, ?, ?, ?, NULL, NULL)",
                (product, streak_start, now_ts, 1 if is_ready else 0),
            )
        else:
            existing = _row_to_streak(row)
            new_streak_start = existing.streak_start_ts
            if is_ready:
                # Start a fresh streak only if we weren't already READY.
                if not existing.last_ready_state or existing.streak_start_ts is None:
                    new_streak_start = now_ts
            else:
                new_streak_start = None  # any non-READY check resets
            conn.execute(
                "UPDATE streaks SET streak_start_ts=?, last_check_ts=?, last_ready_state=? "
                "WHERE product=?",
                (new_streak_start, now_ts, 1 if is_ready else 0, product),
            )

        cur = conn.execute("SELECT * FROM streaks WHERE product=?", (product,))
        return _row_to_streak(cur.fetchone())


def mark_promoted(product: str, pr_url: str, *, db_path: Optional[Path] = None,
                   now_ts: Optional[str] = None) -> None:
    """Record that a flag-flip PR was opened for this product."""
    now_ts = now_ts or _now()
    with open_db(db_path) as conn:
        conn.execute(
            "UPDATE streaks SET promoted_ts=?, promoted_pr_url=? WHERE product=?",
            (now_ts, pr_url, product),
        )


def streak_seconds(product: str, *, db_path: Optional[Path] = None,
                    now_ts: Optional[str] = None) -> float:
    """Return the current uninterrupted READY streak length in seconds.

    Returns 0.0 if the product isn't currently READY or has never been seen.
    """
    s = get(product, db_path=db_path)
    if s is None or not s.last_ready_state or not s.streak_start_ts:
        return 0.0
    try:
        start = datetime.fromisoformat(s.streak_start_ts.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    now_dt = datetime.fromisoformat((now_ts or _now()).replace("Z", "+00:00"))
    return (now_dt - start).total_seconds()
