"""Publish a sanitized snapshot of Operator state to Supabase.

Reads the local daemon's state files (status.json, scheduler state, sqlite
job ledger), builds a JSON payload, and POSTs it to the
`operator_snapshots` table. The operator-site /kruz page server-side
fetches the latest row for each node and renders it.

Run via:
    OPERATOR_NODE=kruz python -m operator_core.snapshot publish

Redact policy (intentionally conservative — this goes public):
  - Project slugs are published (they're already in the user's public
    GitHub profile in most cases). Set OPERATOR_SNAPSHOT_REDACT_PROJECTS=1
    to replace them with `project_<sha8>` hashes.
  - Job prompts, metadata_json, and PR URLs are never published.
  - Webhook URLs, env var values, and any secret-looking strings are
    never published.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from .settings import load_settings

SCHEMA_VERSION = 2


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[snapshot] could not read {path}: {e}", file=sys.stderr)
        return {}


def _age_label(iso_ts: str | None, now: datetime) -> str:
    if not iso_ts:
        return "never"
    try:
        ts = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except ValueError:
        return "never"
    delta = now - ts
    secs = int(delta.total_seconds())
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h {mins % 60}m"
    days = hours // 24
    return f"{days}d {hours % 24}h"


def _redact_slug(slug: str) -> str:
    if os.environ.get("OPERATOR_SNAPSHOT_REDACT_PROJECTS") != "1":
        return slug
    return "project_" + hashlib.sha256(slug.encode("utf-8")).hexdigest()[:8]


def _load_status(status_path: Path) -> dict[str, Any]:
    return _read_json(status_path)


WATCHDOG_SECTION_KEYS = {
    # section name -> description shown on the dashboard
    "briefing": "morning briefing",
    "services": "deploy checker",
    "prs": "PR review",
    "marketing": "marketing pulse",
    "outreach": "outreach pulse",
    "cost": "cost report",
    "advisor": "strategic advisor",
    "client_health": "client health",
    "dependencies": "dependency scan",
    "ci": "CI triage",
    "audit_intake": "audit intake",
}

DEFAULT_MAX_AGE_HOURS = {
    "briefing": 26,
    "services": 2,
    "prs": 2,
    "marketing": 26,
    "outreach": 26,
    "cost": 170,
    "advisor": 50,
    "client_health": 170,
    "dependencies": 170,
    "ci": 4,
    "audit_intake": 1,
}


def _watchdog_panel(
    status: dict[str, Any],
    watchdog_config: dict[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    max_ages = {}
    sections = (watchdog_config or {}).get("sections", {})
    for name, cfg in sections.items():
        if isinstance(cfg, dict) and "max_age_hours" in cfg:
            max_ages[name] = cfg["max_age_hours"]

    panel = []
    for name in WATCHDOG_SECTION_KEYS:
        sec = status.get(name) or {}
        ts = sec.get("timestamp")
        max_hours = max_ages.get(name, DEFAULT_MAX_AGE_HOURS.get(name, 24))
        if ts:
            try:
                ts_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=timezone.utc)
                age_hours = (now - ts_dt).total_seconds() / 3600
                ok = age_hours <= max_hours
            except ValueError:
                ok = False
        else:
            ok = False
        panel.append(
            {
                "name": name,
                "ok": ok,
                "age": _age_label(ts, now),
                "max_hours": max_hours,
            }
        )
    return panel


def _recent_jobs(db_path: Path, limit: int = 8) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            "SELECT id, action, project, status, cost_usd, created_at, updated_at "
            "FROM jobs ORDER BY created_at DESC LIMIT ?",
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
    except sqlite3.DatabaseError as e:
        print(f"[snapshot] sqlite read failed: {e}", file=sys.stderr)
        return []
    return rows


def _format_jobs(rows: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        proj = r.get("project")
        out.append(
            {
                "id": (r["id"] or "")[:10],
                "action": r["action"],
                "project": _redact_slug(proj) if proj else None,
                "status": r["status"],
                "cost": f"${float(r.get('cost_usd') or 0):.2f}",
                "when": _age_label(r.get("created_at"), now),
            }
        )
    return out


def _portfolio(settings) -> list[dict[str, Any]]:
    return [
        {
            "slug": _redact_slug(p.slug),
            "tier": p.autonomy_tier,
            "auto_merge": bool(p.auto_merge),
        }
        for p in settings.projects
    ]


def _deploy_health(status: dict[str, Any], settings) -> list[dict[str, Any]]:
    dh = status.get("deploy_health") or {}
    # Start from the configured projects so we always show a row, even
    # if the per-project health hasn't been checked yet.
    out = []
    for p in settings.projects:
        h = dh.get(p.slug, "unknown")
        out.append(
            {
                "project": _redact_slug(p.slug),
                "status": (
                    "ok"
                    if h == "ok"
                    else "warn"
                    if h in {"warn", "yellow"}
                    else "alert"
                    if h in {"tripped", "red"}
                    else "idle"
                ),
                "host": (p.deploy.provider or "custom").lower(),
            }
        )
    # Also surface any deploy_health entries that aren't in the project list
    # (lightweight, represents stuff the daemon is watching outside config).
    extras = set(dh.keys()) - {p.slug for p in settings.projects}
    for slug in sorted(extras):
        out.append(
            {
                "project": _redact_slug(slug),
                "status": "ok" if dh[slug] == "ok" else "warn",
                "host": "unknown",
            }
        )
    return out


def _cost_series_7d(db_path: Path, now: datetime) -> list[dict[str, Any]]:
    """Per-day spend over the last 7 days, oldest -> newest.

    Always returns 7 entries (zero-filled if no db or no matching rows) so
    the /kruz sparkline has a stable layout.
    """
    rows: list[dict[str, Any]] = []
    if db_path.exists():
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cutoff = (now - timedelta(days=7)).isoformat()
            cur.execute(
                "SELECT substr(created_at, 1, 10) AS day, "
                "SUM(COALESCE(cost_usd,0)) AS usd, COUNT(*) AS n "
                "FROM jobs WHERE created_at >= ? GROUP BY day ORDER BY day ASC",
                (cutoff,),
            )
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
        except sqlite3.DatabaseError:
            rows = []

    by_day = {r["day"]: r for r in rows}
    out: list[dict[str, Any]] = []
    for i in range(6, -1, -1):
        day = (now - timedelta(days=i)).date().isoformat()
        r = by_day.get(day)
        out.append({
            "day": day,
            "usd": round(float(r["usd"]) if r else 0.0, 4),
            "jobs": int(r["n"]) if r else 0,
        })
    return out


def _git_activity(settings, now: datetime) -> list[dict[str, Any]]:
    """Per-project git heartbeat: commits in last 7 days + last commit age.

    Uses a 2s subprocess timeout per project to avoid stalling the snapshot
    publisher on a slow/hung git process.
    """
    out: list[dict[str, Any]] = []
    since = (now - timedelta(days=7)).isoformat()

    for p in settings.projects:
        path = p.path
        entry = {
            "slug": _redact_slug(p.slug),
            "commits_7d": 0,
            "last_commit_iso": None,
            "last_commit_age": "never",
        }
        try:
            if not (path / ".git").exists():
                out.append(entry)
                continue
            cr = subprocess.run(
                ["git", "-C", str(path), "rev-list", "--count", f"--since={since}", "HEAD"],
                capture_output=True, text=True, timeout=2,
            )
            if cr.returncode == 0:
                entry["commits_7d"] = int((cr.stdout or "0").strip() or 0)

            lc = subprocess.run(
                ["git", "-C", str(path), "log", "-1", "--format=%cI"],
                capture_output=True, text=True, timeout=2,
            )
            if lc.returncode == 0:
                last = (lc.stdout or "").strip()
                if last:
                    entry["last_commit_iso"] = last
                    entry["last_commit_age"] = _age_label(last, now)
        except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
            pass
        out.append(entry)

    return out


def _tasks_panel() -> list[dict[str, Any]]:
    """Snapshot the scheduled-task enable/disable state + last-run."""
    try:
        from . import scheduler as sched_mod

        rows = sched_mod.list_all_tasks()
        # Drop fields the /kruz page doesn't need, keep redaction to slugs only.
        return [
            {
                "key": r["key"],
                "cadence": r["cadence"],
                "time": r["time"],
                "enabled": r["enabled"],
                "last_run": r["last_run"],
                "description": r["description"],
            }
            for r in rows
        ]
    except Exception:
        return []


def build_snapshot(
    *,
    status_path: Path,
    db_path: Path,
    watchdog_config_path: Path,
    settings,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    status = _load_status(status_path)
    watchdog_cfg = _read_json(watchdog_config_path)
    jobs = _recent_jobs(db_path)

    total_cost_24h = sum(float(r.get("cost_usd") or 0) for r in jobs)
    ok_count = 0
    for section in WATCHDOG_SECTION_KEYS:
        sec = status.get(section) or {}
        ts = sec.get("timestamp")
        if ts:
            ok_count += 1

    tasks = _tasks_panel()
    git_activity = _git_activity(settings, now)
    cost_7d = _cost_series_7d(db_path, now)

    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": now.isoformat(),
        "summary": {
            "projects": len(settings.projects),
            "tracked_sections": len(WATCHDOG_SECTION_KEYS),
            "jobs_24h": len(jobs),
            "cost_24h_usd": round(total_cost_24h, 2),
            "cost_7d_usd": round(sum(d["usd"] for d in cost_7d), 2),
            "tasks_enabled": sum(1 for t in tasks if t["enabled"]),
            "tasks_total": len(tasks),
        },
        "watchdog": _watchdog_panel(status, watchdog_cfg, now),
        "jobs": _format_jobs(jobs, now),
        "deploy_health": _deploy_health(status, settings),
        "portfolio": _portfolio(settings),
        "tasks": tasks,
        "git_activity": git_activity,
        "cost_series_7d": cost_7d,
        "daemon": {
            "pid": (status.get("daemon") or {}).get("pid"),
            "started_at": (status.get("daemon") or {}).get("started_at"),
            "uptime_sec": (status.get("daemon") or {}).get("uptime_sec", 0),
        },
    }


def publish(payload: dict[str, Any], *, node: str) -> None:
    url = os.environ.get("OPERATOR_SNAPSHOT_SUPABASE_URL") or os.environ.get(
        "SUPABASE_URL"
    )
    key = os.environ.get("OPERATOR_SNAPSHOT_SUPABASE_KEY") or os.environ.get(
        "SUPABASE_SERVICE_ROLE_KEY"
    )
    if not url or not key:
        raise RuntimeError(
            "OPERATOR_SNAPSHOT_SUPABASE_URL + OPERATOR_SNAPSHOT_SUPABASE_KEY "
            "(or SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY) must be set."
        )
    endpoint = f"{url.rstrip('/')}/rest/v1/operator_snapshots"
    r = requests.post(
        endpoint,
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal",
        },
        json={"node": node, "payload": payload},
        timeout=20,
    )
    r.raise_for_status()


def _status_path_for_kruz_monolith(settings) -> Path:
    """Kruz's live state files live in the operator-scripts monolith,
    not in the operator-core data_dir. Let env var override for when the
    daemon is running out of operator-core proper.
    """
    override = os.environ.get("OPERATOR_STATUS_PATH")
    if override:
        return Path(override)
    return settings.projects_root / ".operator-status.json"


def _db_path_for_kruz_monolith(settings) -> Path:
    override = os.environ.get("OPERATOR_DB_PATH")
    if override:
        return Path(override)
    return settings.projects_root / "operator-scripts" / ".operator-v3" / "operator-v3.sqlite3"


def _watchdog_config_path_for_kruz_monolith(settings) -> Path:
    override = os.environ.get("OPERATOR_WATCHDOG_CONFIG")
    if override:
        return Path(override)
    return (
        settings.projects_root
        / "operator-scripts"
        / "config"
        / "watchdog_expectations.json"
    )


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    cmd = args[0] if args else "publish"

    settings = load_settings()
    node = os.environ.get("OPERATOR_NODE", "kruz")

    status_path = _status_path_for_kruz_monolith(settings)
    db_path = _db_path_for_kruz_monolith(settings)
    watchdog_config_path = _watchdog_config_path_for_kruz_monolith(settings)

    payload = build_snapshot(
        status_path=status_path,
        db_path=db_path,
        watchdog_config_path=watchdog_config_path,
        settings=settings,
    )

    if cmd == "dump":
        print(json.dumps(payload, indent=2))
        return 0

    if cmd == "publish":
        publish(payload, node=node)
        summary = payload["summary"]
        watchdog_ok = sum(1 for s in payload["watchdog"] if s["ok"])
        print(
            f"[snapshot] node={node} "
            f"projects={summary['projects']} jobs={summary['jobs_24h']} "
            f"watchdog_ok={watchdog_ok}/{len(payload['watchdog'])} "
            f"cost_24h=${summary['cost_24h_usd']:.2f}"
        )
        return 0

    print(f"unknown command: {cmd}", file=sys.stderr)
    print("usage: python -m operator_core.snapshot [dump|publish]", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
