"""Emit a status-spec/v1 file alongside the legacy status.json.

This module is a thin adapter. It reads the legacy v2 status dict
produced by `utils.status` and translates it into a status-spec/v1
document that the war-room dashboard (and any other reader) can
consume without bespoke per-project parsing.

We do not modify the legacy file. We write a sibling file at
`<status_path>.parent / 'status-spec.json'` (default
`~/.operator/data/status-spec.json`) atomically.

Schema source:
    https://github.com/kjhholt-alt/status-spec - status-spec/v1

We vendor a minimal validator here so this module has no extra
dependency on the as-yet-unpublished `status-spec` Python package.
Once that package ships, this module can `from status_spec import
StatusBuilder, write_atomic` and drop the local copy.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "status-spec/v1"
PROJECT_NAME = "operator-core"

_HEALTH_VALUES = ("green", "yellow", "red")
_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]00:00)$"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_legacy_ts(raw: Any) -> str:
    """Best-effort conversion of legacy ISO local-time strings into UTC."""
    if not isinstance(raw, str) or not raw:
        return _utc_now()
    if _TS_RE.match(raw):
        return raw
    # Legacy format: datetime.now().isoformat() -> '2026-05-04T22:31:00.123456'
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return _utc_now()


def _derive_health(legacy: dict[str, Any]) -> str:
    """Map the legacy v2 dict to a tri-state."""
    if legacy.get("risk_tripped"):
        return "red"
    deploys = legacy.get("deploy_health") or {}
    if any(v == "tripped" for v in deploys.values()):
        return "red"
    if any(v == "warn" for v in deploys.values()):
        return "yellow"
    blocks = legacy.get("hook_blocks_recent") or []
    if blocks:
        return "yellow"
    return "green"


def _map_subsystems(legacy: dict[str, Any]) -> list[dict[str, Any]]:
    subs: list[dict[str, Any]] = []
    deploys = legacy.get("deploy_health") or {}
    for proj, state in deploys.items():
        if not isinstance(proj, str) or not isinstance(state, str):
            continue
        h = "green"
        if state == "warn":
            h = "yellow"
        elif state == "tripped":
            h = "red"
        subs.append({"name": f"deploy.{proj}", "health": h, "detail": f"deploy state={state}"})

    daemon = legacy.get("daemon") or {}
    if daemon.get("pid"):
        uptime = daemon.get("uptime_sec") or 0
        subs.append({
            "name": "daemon",
            "health": "green",
            "detail": f"pid={daemon.get('pid')} uptime={int(uptime)}s",
        })
    return subs


def _map_counters(legacy: dict[str, Any]) -> dict[str, float]:
    counters: dict[str, float] = {}
    cost = legacy.get("cost_today_usd")
    if isinstance(cost, (int, float)):
        counters["cost_today_usd"] = float(cost)
    jobs = legacy.get("jobs_recent") or []
    counters["jobs_recent"] = float(len(jobs))
    blocks = legacy.get("hook_blocks_recent") or []
    counters["hook_blocks_recent"] = float(len(blocks))
    discord_unread = legacy.get("discord_unread")
    if isinstance(discord_unread, int) and not isinstance(discord_unread, bool):
        counters["discord_unread"] = float(discord_unread)
    return counters


def _map_last_event(legacy: dict[str, Any]) -> dict[str, Any] | None:
    jobs = legacy.get("jobs_recent") or []
    if not jobs:
        return None
    j0 = jobs[0]
    if not isinstance(j0, dict):
        return None
    summary = f"{j0.get('action') or 'job'} {j0.get('status') or ''}".strip()
    return {
        "ts": _coerce_legacy_ts(j0.get("updated_at") or legacy.get("last_updated")),
        "type": "job.recorded",
        "summary": summary[:280] or "job recorded",
    }


def translate(legacy: dict[str, Any], *, project: str = PROJECT_NAME) -> dict[str, Any]:
    """Convert a legacy v2 status dict into a status-spec/v1 document."""
    doc: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "project": project,
        "ts": _coerce_legacy_ts(legacy.get("last_updated")),
        "health": _derive_health(legacy),
    }
    subsystems = _map_subsystems(legacy)
    if subsystems:
        doc["subsystems"] = subsystems

    counters = _map_counters(legacy)
    if counters:
        doc["counters"] = counters

    last_event = _map_last_event(legacy)
    if last_event:
        doc["last_event"] = last_event

    # Surface the legacy daemon block under extensions for any reader
    # that wants the full detail.
    extensions = {"operator_core": {
        "schema_version_legacy": legacy.get("schema_version"),
        "daemon": legacy.get("daemon"),
    }}
    doc["extensions"] = extensions
    return doc


def _validate_minimal(doc: dict) -> list[str]:
    """Lightweight validation. Sufficient to catch obvious bugs."""
    errors: list[str] = []
    if doc.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"schema_version: must equal {SCHEMA_VERSION!r}")
    project = doc.get("project")
    if not isinstance(project, str) or not project:
        errors.append("project: required string")
    ts = doc.get("ts")
    if not isinstance(ts, str) or not _TS_RE.match(ts):
        errors.append("ts: must be ISO 8601 UTC")
    if doc.get("health") not in _HEALTH_VALUES:
        errors.append("health: must be green|yellow|red")
    return errors


def _atomic_write(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def emit(legacy: dict[str, Any], target: Path) -> Path:
    """Translate + atomically write a status-spec/v1 document."""
    doc = translate(legacy)
    errors = _validate_minimal(doc)
    if errors:
        # Don't crash the caller — emission is alongside the legacy
        # write. Surface the issue, then write a synthetic red doc so
        # consumers see something is wrong.
        doc = {
            "schema_version": SCHEMA_VERSION,
            "project": PROJECT_NAME,
            "ts": _utc_now(),
            "health": "red",
            "summary": ("status-spec emit failed: " + "; ".join(errors))[:280],
        }
    payload = json.dumps(doc, indent=2, ensure_ascii=False)
    _atomic_write(target, payload)
    return target


def emit_alongside(legacy: dict[str, Any], legacy_path: Path) -> Path:
    """Emit a sibling file at <legacy_path>.parent / status-spec.json."""
    sibling = Path(legacy_path).parent / "status-spec.json"
    return emit(legacy, sibling)
