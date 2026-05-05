"""status-spec shim.

Operator-core's per-component status format predates the canonical
``status-spec/v1`` schema. This module preserves the legacy
``write_component_status(name, status, ...)`` surface that recipes call
into, and -- when the real ``status_spec`` package is installed --
*additionally* emits a canonical status-spec/v1 aggregate at
``OPERATOR_STATUS_DIR/../status-spec.json`` so cross-portfolio status
dashboards can read the same shape they read everywhere else.

Surface (unchanged):
    write_component_status(name, status, *, duration_sec=None,
        cost_usd=None, error=None, version=None, extra=None,
        status_dir=None) -> dict
    read_aggregate(path=None) -> dict
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

def _default_status_dir() -> Path:
    return Path(
        os.environ.get(
            "OPERATOR_STATUS_DIR",
            str(Path.home() / ".operator" / "data" / "status"),
        )
    )


def _aggregate_path(status_dir: Path | None = None) -> Path:
    base = (status_dir or _default_status_dir()).parent
    return base / "status.json"


def _canonical_aggregate_path(status_dir: Path | None = None) -> Path:
    base = (status_dir or _default_status_dir()).parent
    return base / "status-spec.json"


# Backward-compat constants (computed once at import). For programmatic
# callers that override OPERATOR_STATUS_DIR after import, prefer
# `_aggregate_path()` / `_canonical_aggregate_path()`.
DEFAULT_STATUS_DIR = _default_status_dir()
AGGREGATE_PATH = _aggregate_path()
CANONICAL_AGGREGATE_PATH = _canonical_aggregate_path()

# Map operator-core's component statuses -> status-spec/v1 subsystem health.
# status-spec/v1 uses green/yellow/red per the canonical Health Literal.
_STATUS_TO_HEALTH = {
    "ok": "green",
    "warn": "yellow",
    "error": "red",
    "running": "green",
    "skipped": "green",
}


_USING_REAL_LIB = False
try:
    from status_spec import StatusBuilder, write_atomic  # type: ignore  # noqa: F401
    _USING_REAL_LIB = True
except ImportError:
    StatusBuilder = None  # type: ignore[assignment]
    write_atomic = None  # type: ignore[assignment]


def using_real_lib() -> bool:
    """Diagnostic: True iff the real status-spec package is in use."""
    return _USING_REAL_LIB


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
    target_dir = Path(status_dir) if status_dir else _default_status_dir()
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

    components = _refresh_aggregate(target_dir)

    # Also emit a canonical status-spec/v1 document when the real lib is
    # available. Failures here never break the legacy write -- canonical
    # emission is an additive observability path.
    if _USING_REAL_LIB:
        try:
            _emit_canonical_aggregate(target_dir, components)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("[status-spec] canonical emit failed: %s", exc)

    return payload


def _emit_canonical_aggregate_to(path: Path, status_dir: Path, components: Dict[str, Any]) -> None:
    """Internal helper to write a canonical doc to a specific path."""
    if StatusBuilder is None or write_atomic is None:
        return
    overall_health = "green"
    for comp in components.values():
        s = comp.get("status")
        if s == "error":
            overall_health = "red"
            break
        if s == "warn" and overall_health != "red":
            overall_health = "yellow"
    builder = StatusBuilder("operator-core").health(overall_health)
    total_cost = 0.0
    error_count = 0
    for comp in components.values():
        sub_health = _STATUS_TO_HEALTH.get(comp.get("status", ""), "green")
        detail = comp.get("error") or None
        builder.subsystem(comp["name"], sub_health, detail=detail)
        if isinstance(comp.get("cost_usd"), (int, float)):
            total_cost += float(comp["cost_usd"])
        if comp.get("status") == "error":
            error_count += 1
    builder.counter("components_total", len(components))
    builder.counter("components_errors", error_count)
    builder.counter("cost_usd_sum", round(total_cost, 6))
    write_atomic(path, builder.build())


def _refresh_aggregate(status_dir: Path) -> Dict[str, Any]:
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
    aggregate_path = _aggregate_path(status_dir)
    aggregate_path.parent.mkdir(parents=True, exist_ok=True)
    aggregate_path.write_text(json.dumps(aggregate, indent=2), encoding="utf-8")
    return components


def _emit_canonical_aggregate(status_dir: Path, components: Dict[str, Any]) -> None:
    """Build and write a canonical status-spec/v1 document. No-op without the real lib."""
    _emit_canonical_aggregate_to(_canonical_aggregate_path(status_dir), status_dir, components)


def read_aggregate(path: Path | None = None) -> dict[str, Any]:
    target = Path(path) if path else _aggregate_path()
    if not target.exists():
        return {"generated_at": None, "overall": "unknown", "components": {}}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"generated_at": None, "overall": "unknown", "components": {}}
