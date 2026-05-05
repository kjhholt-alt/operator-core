"""Minimal ``status-spec`` writer stub.

Real spec (sibling agent ``status-spec``):
- Per-recipe status JSON written to ``~/.operator/data/status/<name>.json``
- Aggregated status at ``~/.operator/data/status.json`` with ``components``
  keyed by recipe name.
- Schema: ``{"name", "status", "last_run", "duration_sec", "cost_usd",
  "error", "version"}``.

This stub matches that surface so recipes can run today; once the real
package lands we replace ``from operator_core._vendor.status_spec import
write_component_status`` with the real import and delete this file.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

DEFAULT_STATUS_DIR = Path(
    os.environ.get(
        "OPERATOR_STATUS_DIR",
        str(Path.home() / ".operator" / "data" / "status"),
    )
)
AGGREGATE_PATH = DEFAULT_STATUS_DIR.parent / "status.json"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_component_status(
    name: str,
    status: str,
    *,
    duration_sec: float | None = None,
    cost_usd: float | None = None,
    error: str | None = None,
    version: str | None = None,
    extra: dict[str, Any] | None = None,
    status_dir: Path | None = None,
) -> dict[str, Any]:
    """Write a per-component status file and refresh the aggregate roll-up.

    ``status`` should be one of: ``ok``, ``warn``, ``error``, ``running``,
    ``skipped``.
    """
    target_dir = Path(status_dir) if status_dir else DEFAULT_STATUS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "name": name,
        "status": status,
        "last_run": _now(),
        "duration_sec": duration_sec,
        "cost_usd": cost_usd,
        "error": error,
        "version": version,
    }
    if extra:
        payload["extra"] = extra

    component_path = target_dir / f"{name}.json"
    component_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    _refresh_aggregate(target_dir)
    return payload


def _refresh_aggregate(status_dir: Path) -> None:
    components: dict[str, Any] = {}
    for path in sorted(status_dir.glob("*.json")):
        try:
            components[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue

    overall = "ok"
    for comp in components.values():
        s = comp.get("status")
        if s == "error":
            overall = "error"
            break
        if s == "warn" and overall != "error":
            overall = "warn"

    aggregate = {
        "generated_at": _now(),
        "overall": overall,
        "components": components,
    }
    AGGREGATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    AGGREGATE_PATH.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")


def read_aggregate(path: Path | None = None) -> dict[str, Any]:
    target = Path(path) if path else AGGREGATE_PATH
    if not target.exists():
        return {"generated_at": None, "overall": "unknown", "components": {}}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"generated_at": None, "overall": "unknown", "components": {}}
