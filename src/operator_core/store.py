"""SQLite job store for Operator V3."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .paths import DB_PATH, ensure_data_dirs
from .security import redact_secrets


@dataclass(frozen=True)
class JobRecord:
    id: str
    action: str
    status: str
    prompt: str
    project: str | None
    branch: str | None
    worktree: str | None
    risk_tier: str | None
    pr_url: str | None
    deploy_result: str | None
    cost_usd: float
    metadata: dict[str, Any]
    created_at: str
    updated_at: str


class JobStore:
    def __init__(self, db_path: Path = DB_PATH):
        ensure_data_dirs()
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    action TEXT NOT NULL,
                    status TEXT NOT NULL,
                    prompt TEXT NOT NULL DEFAULT '',
                    project TEXT,
                    branch TEXT,
                    worktree TEXT,
                    risk_tier TEXT,
                    pr_url TEXT,
                    deploy_result TEXT,
                    cost_usd REAL NOT NULL DEFAULT 0,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS approvals (
                    job_id TEXT NOT NULL,
                    approver TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (job_id, approver)
                )
                """
            )

    def create_job(
        self,
        action: str,
        prompt: str = "",
        project: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> JobRecord:
        now = _now()
        job_id = uuid.uuid4().hex[:10]
        safe_prompt = redact_secrets(prompt)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    id, action, status, prompt, project, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    action,
                    "queued",
                    safe_prompt,
                    project,
                    json.dumps(metadata or {}, sort_keys=True),
                    now,
                    now,
                ),
            )
        return self.get_job(job_id)

    def update_job(self, job_id: str, **changes: Any) -> JobRecord:
        if not changes:
            return self.get_job(job_id)
        changes["updated_at"] = _now()
        if "prompt" in changes and changes["prompt"] is not None:
            changes["prompt"] = redact_secrets(str(changes["prompt"]))
        if "metadata" in changes:
            changes["metadata_json"] = json.dumps(changes.pop("metadata") or {}, sort_keys=True)

        allowed = {
            "status",
            "prompt",
            "project",
            "branch",
            "worktree",
            "risk_tier",
            "pr_url",
            "deploy_result",
            "cost_usd",
            "metadata_json",
            "updated_at",
        }
        unknown = set(changes) - allowed
        if unknown:
            raise ValueError(f"Unknown job fields: {', '.join(sorted(unknown))}")

        assignments = ", ".join(f"{key} = ?" for key in changes)
        values = list(changes.values()) + [job_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE jobs SET {assignments} WHERE id = ?", values)
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> JobRecord:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            raise KeyError(f"Unknown job: {job_id}")
        return _record_from_row(row)

    def list_jobs(self, limit: int = 20) -> list[JobRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_record_from_row(row) for row in rows]

    def approve(self, job_id: str, approver: str) -> int:
        now = _now()
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO approvals (job_id, approver, created_at) VALUES (?, ?, ?)",
                (job_id, approver, now),
            )
            rows = conn.execute(
                "SELECT COUNT(*) AS n FROM approvals WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return int(rows["n"])

    def approval_count(self, job_id: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM approvals WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        return int(row["n"])


def _record_from_row(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        id=row["id"],
        action=row["action"],
        status=row["status"],
        prompt=row["prompt"],
        project=row["project"],
        branch=row["branch"],
        worktree=row["worktree"],
        risk_tier=row["risk_tier"],
        pr_url=row["pr_url"],
        deploy_result=row["deploy_result"],
        cost_usd=float(row["cost_usd"] or 0),
        metadata=json.loads(row["metadata_json"] or "{}"),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
