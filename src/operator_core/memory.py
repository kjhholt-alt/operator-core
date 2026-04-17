"""Per-project memory store for Operator V3.

Small key/value bag keyed by (project, key). Backed by the same SQLite
database as the job ledger via `operator_v3.paths.DB_PATH`, but lazily
initialized via `ensure_schema(conn)` so this module works standalone
without forcing store.py to import it.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from .paths import DB_PATH, ensure_data_dirs
from .store_migrations import apply_migrations


@dataclass(frozen=True)
class MemoryEntry:
    project: str
    key: str
    value: str
    updated_at: str
    source_job_id: str | None


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Idempotent schema bootstrap. Safe to call on every connection."""
    apply_migrations(conn)


class ProjectMemory:
    """Thin SQLite-backed project memory store."""

    def __init__(self, db_path: Path | None = None):
        ensure_data_dirs()
        self.db_path = Path(db_path) if db_path is not None else DB_PATH
        with self._connect() as conn:
            ensure_schema(conn)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def remember(
        self,
        project: str,
        key: str,
        value: str,
        source_job_id: str | None = None,
    ) -> MemoryEntry:
        if not project or not key:
            raise ValueError("project and key are required")
        now = _now()
        with self._connect() as conn:
            ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO project_memory (project, key, value, updated_at, source_job_id)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(project, key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at,
                    source_job_id = excluded.source_job_id
                """,
                (project, key, str(value), now, source_job_id),
            )
        return MemoryEntry(project, key, str(value), now, source_job_id)

    def recall(self, project: str, key: str) -> str | None:
        with self._connect() as conn:
            ensure_schema(conn)
            row = conn.execute(
                "SELECT value FROM project_memory WHERE project = ? AND key = ?",
                (project, key),
            ).fetchone()
        if row is None:
            return None
        return str(row["value"])

    def list_project(self, project: str) -> dict[str, str]:
        with self._connect() as conn:
            ensure_schema(conn)
            rows = conn.execute(
                "SELECT key, value FROM project_memory WHERE project = ? ORDER BY key",
                (project,),
            ).fetchall()
        return {str(row["key"]): str(row["value"]) for row in rows}

    def forget(self, project: str, key: str) -> bool:
        with self._connect() as conn:
            ensure_schema(conn)
            cur = conn.execute(
                "DELETE FROM project_memory WHERE project = ? AND key = ?",
                (project, key),
            )
            return cur.rowcount > 0

    def increment(
        self,
        project: str,
        key: str,
        delta: int = 1,
        source_job_id: str | None = None,
    ) -> int:
        """Increment an integer-valued key. Non-integer current values reset to delta."""
        current = self.recall(project, key)
        try:
            new_val = int(current) + delta if current is not None else delta
        except (TypeError, ValueError):
            new_val = delta
        self.remember(project, key, str(new_val), source_job_id=source_job_id)
        return new_val


# Module-level singleton convenience API
_default: ProjectMemory | None = None


def _default_store() -> ProjectMemory:
    global _default
    if _default is None:
        _default = ProjectMemory()
    return _default


def remember(project: str, key: str, value: str, source_job_id: str | None = None) -> MemoryEntry:
    return _default_store().remember(project, key, value, source_job_id=source_job_id)


def recall(project: str, key: str) -> str | None:
    return _default_store().recall(project, key)


def list_project(project: str) -> dict[str, str]:
    return _default_store().list_project(project)


def increment(project: str, key: str, delta: int = 1, source_job_id: str | None = None) -> int:
    return _default_store().increment(project, key, delta=delta, source_job_id=source_job_id)


def reset_default_store_for_tests() -> None:
    """Test hook — clear the module singleton so DB_PATH monkeypatches take effect."""
    global _default
    _default = None
