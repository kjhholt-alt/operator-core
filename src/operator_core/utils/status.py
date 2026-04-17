"""
Operator status file writer.

Writes/reads `.operator-status.json` for the WezTerm status bar, the ops page,
and cross-script state. V2 expands the schema with daemon info, recent jobs,
cost today, deploy health, hook block events, and discord unread count — while
preserving every legacy key so existing consumers (WezTerm status bar, other
scripts) keep working.

Schema v2 shape (additive):

    {
      "schema_version": 2,
      "last_updated": "...",
      "daemon": {"pid": 1234, "started_at": "...", "uptime_sec": 42},
      "jobs_recent": [{"id": ..., "action": ..., "status": ..., "project": ...}, ...],  # up to 10
      "cost_today_usd": 1.23,
      "deploy_health": {"operator-ai": "ok", "dealbrain": "warn"},
      "risk_tripped": false,
      "hook_blocks_recent": [{"ts": ..., "reason": ..., "command": ...}, ...],  # up to 5
      "discord_unread": 0,
      // -- legacy keys below are preserved verbatim --
      "<section>": {"timestamp": "...", ...},
      "_alerts": {...},
    }
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from ..paths import STATUS_PATH as _STATUS_PATH_LAZY

# Historic module-level constant kept for back-compat. Resolves via the
# configured status_path (~/.operator/data/status.json by default) unless
# OPERATOR_STATUS_PATH env var overrides.
_ENV_OVERRIDE = os.environ.get("OPERATOR_STATUS_PATH")
STATUS_PATH = Path(_ENV_OVERRIDE) if _ENV_OVERRIDE else _STATUS_PATH_LAZY

SCHEMA_VERSION = 2

MAX_JOBS_RECENT = 10
MAX_HOOK_BLOCKS_RECENT = 5


def _default_v2() -> dict[str, Any]:
    """Default shape for a fresh v2 status file."""
    return {
        "schema_version": SCHEMA_VERSION,
        "last_updated": None,
        "daemon": {"pid": None, "started_at": None, "uptime_sec": 0},
        "jobs_recent": [],
        "cost_today_usd": 0.0,
        "deploy_health": {},
        "risk_tripped": False,
        "hook_blocks_recent": [],
        "discord_unread": 0,
    }


def load_or_default(path: Path | None = None) -> dict[str, Any]:
    """Load the status file and overlay v2 defaults without clobbering legacy keys.

    Migration path:
      - Missing file → fresh v2 default.
      - Legacy v1 file (no `schema_version`) → keep every existing key, add v2
        keys with defaults, stamp `schema_version = 2`.
      - Existing v2 file → fill in any missing v2 keys with defaults.
    """
    target = Path(path) if path else STATUS_PATH
    if not target.exists():
        return _default_v2()
    try:
        with open(target, "r", encoding="utf-8") as fh:
            existing = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return _default_v2()
    if not isinstance(existing, dict):
        return _default_v2()

    merged: dict[str, Any] = dict(existing)  # preserve legacy keys
    defaults = _default_v2()
    for key, value in defaults.items():
        if key not in merged:
            merged[key] = value
    merged["schema_version"] = SCHEMA_VERSION
    return merged


def _write(status: dict[str, Any], path: Path | None = None) -> None:
    target = Path(path) if path else STATUS_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    status["last_updated"] = datetime.now().isoformat()
    with open(target, "w", encoding="utf-8") as fh:
        json.dump(status, fh, indent=2, default=str)


# --- Legacy API (kept verbatim for backwards compat) -------------------------


def read_status(path: Path | None = None) -> dict:
    """Read the current status file (raw). Does not migrate."""
    target = Path(path) if path else STATUS_PATH
    if target.exists():
        try:
            with open(target, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def write_status(section: str, data: dict, path: Path | None = None) -> None:
    """Update a named section of the status file. Merges with existing data.

    Preserves all v2 top-level keys by routing through `load_or_default`.
    """
    status = load_or_default(path)
    data = dict(data)
    data["timestamp"] = datetime.now().isoformat()
    status[section] = data
    _write(status, path)


def get_last_alert_state(service: str, path: Path | None = None) -> dict | None:
    """Get the last alert state for dedup logic."""
    status = read_status(path)
    alerts = status.get("_alerts", {})
    return alerts.get(service)


def set_alert_state(
    service: str,
    state: str,
    message: str = "",
    path: Path | None = None,
) -> None:
    """Track alert state for dedup (e.g., last time we alerted for a service being down)."""
    status = load_or_default(path)
    if "_alerts" not in status:
        status["_alerts"] = {}
    status["_alerts"][service] = {
        "state": state,
        "message": message,
        "timestamp": datetime.now().isoformat(),
    }
    _write(status, path)


# --- V2 API -----------------------------------------------------------------


def update_daemon(
    pid: int | None,
    started_at: str | None,
    uptime_sec: float,
    path: Path | None = None,
) -> dict[str, Any]:
    """Record the daemon's pid/start/uptime."""
    status = load_or_default(path)
    status["daemon"] = {
        "pid": pid,
        "started_at": started_at,
        "uptime_sec": float(uptime_sec),
    }
    _write(status, path)
    return status


def record_recent_job(job: dict[str, Any], path: Path | None = None) -> dict[str, Any]:
    """Push a compact job summary onto `jobs_recent`, keeping last MAX_JOBS_RECENT."""
    status = load_or_default(path)
    summary = {
        "id": job.get("id"),
        "action": job.get("action"),
        "status": job.get("status"),
        "project": job.get("project"),
        "cost_usd": float(job.get("cost_usd") or 0),
        "updated_at": job.get("updated_at"),
    }
    recent = list(status.get("jobs_recent") or [])
    # de-dup by id — most recent wins
    recent = [r for r in recent if r.get("id") != summary["id"]]
    recent.insert(0, summary)
    status["jobs_recent"] = recent[:MAX_JOBS_RECENT]
    _write(status, path)
    return status


def record_hook_block(
    reason: str,
    command: str,
    path: Path | None = None,
) -> dict[str, Any]:
    """Push a hook-block event onto `hook_blocks_recent`, keeping last MAX."""
    status = load_or_default(path)
    event = {
        "ts": datetime.now().isoformat(),
        "reason": reason,
        "command": command,
    }
    recent = list(status.get("hook_blocks_recent") or [])
    recent.insert(0, event)
    status["hook_blocks_recent"] = recent[:MAX_HOOK_BLOCKS_RECENT]
    _write(status, path)
    return status


def set_cost_today(cost_usd: float, path: Path | None = None) -> dict[str, Any]:
    """Overwrite today's running cost total."""
    status = load_or_default(path)
    status["cost_today_usd"] = float(cost_usd)
    _write(status, path)
    return status


def set_deploy_health(
    project: str,
    health: str,
    path: Path | None = None,
) -> dict[str, Any]:
    """Record deploy health for a single project. `health` is one of ok/warn/tripped."""
    status = load_or_default(path)
    dh = dict(status.get("deploy_health") or {})
    dh[project] = health
    status["deploy_health"] = dh
    _write(status, path)
    return status


def set_risk_tripped(tripped: bool, path: Path | None = None) -> dict[str, Any]:
    """Flip the `risk_tripped` flag."""
    status = load_or_default(path)
    status["risk_tripped"] = bool(tripped)
    _write(status, path)
    return status


def set_discord_unread(count: int, path: Path | None = None) -> dict[str, Any]:
    """Overwrite the Discord unread-command counter."""
    status = load_or_default(path)
    status["discord_unread"] = int(count)
    _write(status, path)
    return status
