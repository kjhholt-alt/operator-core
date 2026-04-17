"""Idempotent schema migration runner for the Operator V3 SQLite store.

Each migration is a (version, list-of-DDL-statements) tuple. Applied
versions are tracked in a `schema_migrations` table so every migration is
safe to call repeatedly, including from multiple modules (JobStore init,
memory module lazy init, etc).
"""

from __future__ import annotations

import sqlite3
from typing import Iterable


# Ordered migration list. NEVER reorder or delete existing entries — only
# append new ones. Each entry: (version_int, tuple_of_ddl_statements).
MIGRATIONS: tuple[tuple[int, tuple[str, ...]], ...] = (
    (
        1,
        (
            """
            CREATE TABLE IF NOT EXISTS project_memory (
                project TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source_job_id TEXT,
                PRIMARY KEY (project, key)
            )
            """,
        ),
    ),
    (
        2,
        (
            # attempts_json on jobs — add column if missing. SQLite has no
            # "IF NOT EXISTS" for ADD COLUMN so we detect via PRAGMA in
            # apply_migrations below; the DDL here is the statement to run
            # when the column is absent.
            "ALTER TABLE jobs ADD COLUMN attempts_json TEXT NOT NULL DEFAULT '[]'",
        ),
    ),
)


def _ensure_migrations_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def _applied_versions(conn: sqlite3.Connection) -> set[int]:
    _ensure_migrations_table(conn)
    rows = conn.execute("SELECT version FROM schema_migrations").fetchall()
    return {int(row[0]) for row in rows}


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(str(r[1]) == column for r in rows)


def _run_statement(conn: sqlite3.Connection, stmt: str) -> None:
    """Execute a DDL statement, tolerating idempotent no-ops.

    Specifically: ALTER TABLE ADD COLUMN is skipped if the target column
    already exists; same for CREATE TABLE if the table exists. Other
    statements are executed as-is.
    """
    stripped = stmt.strip()
    upper = stripped.upper()
    if upper.startswith("ALTER TABLE") and "ADD COLUMN" in upper:
        # Parse: ALTER TABLE <name> ADD COLUMN <col> ...
        try:
            after_alter = stripped.split(None, 2)[2]  # "<name> ADD COLUMN ..."
            table_name = after_alter.split(None, 1)[0]
            rest = after_alter.split("ADD COLUMN", 1)[1].strip()
            col_name = rest.split(None, 1)[0]
        except (IndexError, ValueError):
            conn.execute(stripped)
            return
        if _column_exists(conn, table_name, col_name):
            return
        # jobs table may not exist yet in a fresh memory-only DB. Skip.
        if not _table_exists(conn, table_name):
            return
        conn.execute(stripped)
        return
    conn.execute(stripped)


def apply_migrations(
    conn: sqlite3.Connection,
    migrations: Iterable[tuple[int, tuple[str, ...]]] | None = None,
) -> list[int]:
    """Apply any pending migrations. Returns the list of newly-applied versions.

    Idempotent: calling twice in a row is a no-op on the second call.
    """
    migs = tuple(migrations) if migrations is not None else MIGRATIONS
    applied = _applied_versions(conn)
    newly: list[int] = []
    for version, statements in sorted(migs, key=lambda m: m[0]):
        if version in applied:
            continue
        for stmt in statements:
            _run_statement(conn, stmt)
        conn.execute(
            "INSERT OR IGNORE INTO schema_migrations (version) VALUES (?)",
            (version,),
        )
        newly.append(version)
    conn.commit()
    return newly
