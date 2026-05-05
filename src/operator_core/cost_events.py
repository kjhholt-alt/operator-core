"""Cost-stream events-ndjson emitter for operator-core.

Adds an additive, opt-in NDJSON cost event emission alongside the existing
``costs.csv`` and Supabase ``agent_costs`` table. Disabled by default;
enable by setting ``OPERATOR_COST_NDJSON_PATH`` to a writable file path.

Once events-ndjson ships on PyPI this module switches its import from
``operator_core._vendor.events_ndjson`` to ``events_ndjson`` with no
behavior change.

Failures are logged and swallowed: cost telemetry must never block a
real workload.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from operator_core._vendor.events_ndjson import EventsNdjsonError, Writer

logger = logging.getLogger(__name__)

_writer: Optional[Writer] = None


def _resolve_path() -> Optional[Path]:
    raw = os.environ.get("OPERATOR_COST_NDJSON_PATH")
    if not raw:
        return None
    return Path(raw)


def _get_writer() -> Optional[Writer]:
    global _writer
    if _writer is not None:
        return _writer
    path = _resolve_path()
    if path is None:
        return None
    try:
        _writer = Writer(stream="cost", source="operator-core", path=path)
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("[cost-events] could not open writer: %s", e)
        _writer = None
    return _writer


def emit_cost(
    *,
    agent: str,
    cost_usd: float,
    duration_ms: Optional[int] = None,
    input_tokens: Optional[int] = None,
    output_tokens: Optional[int] = None,
    model: Optional[str] = None,
    session_id: Optional[str] = None,
    exit_code: Optional[int] = None,
    event_type: str = "agent_complete",
) -> bool:
    """Emit one cost-stream event. Returns True if an event was written."""
    writer = _get_writer()
    if writer is None:
        return False

    payload = {"agent": agent, "cost_usd": float(cost_usd)}
    if duration_ms is not None:
        payload["duration_ms"] = int(duration_ms)
    if input_tokens is not None:
        payload["input_tokens"] = int(input_tokens)
    if output_tokens is not None:
        payload["output_tokens"] = int(output_tokens)
    if model is not None:
        payload["model"] = str(model)
    if session_id is not None:
        payload["session_id"] = str(session_id)
    if exit_code is not None:
        payload["exit_code"] = int(exit_code)

    try:
        writer.append(event_type, payload)
        return True
    except EventsNdjsonError as e:
        logger.warning("[cost-events] validation failed: %s", e)
        return False
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("[cost-events] append failed: %s", e)
        return False


def reset_for_tests() -> None:
    """Test helper: drop the cached writer so OPERATOR_COST_NDJSON_PATH is
    re-read."""
    global _writer
    if _writer is not None:
        try:
            _writer.close()
        except Exception:
            pass
    _writer = None
