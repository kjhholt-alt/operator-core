"""events-ndjson shim.

Imports the real ``events_ndjson`` package when installed and re-exports
its public surface. Falls back to a minimal local implementation for
environments where the real lib is not yet on the system (fresh clones,
CI without the optional ``specs`` extra, downstream packagers).

Operator-core only needs three things:

1. ``Writer`` — strict, schema-validated, used by ``cost_events.py``
2. ``EventsNdjsonError`` — base exception for caller error handling
3. ``append_event`` / ``read_events`` — file-per-stream helpers used by
   the recipes runtime (``runs`` + ``cost`` streams). These are operator-
   core-specific conveniences; the real lib does not provide them.

The fallback path matches the real lib's surface for ``Writer`` /
``EventsNdjsonError`` (so ``cost_events`` works either way) and supplies
``append_event`` / ``read_events`` regardless of which path is active.
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

# ---------------------------------------------------------------------------
# Real-package preference
# ---------------------------------------------------------------------------
_USING_REAL_LIB = False
try:
    from events_ndjson import Writer  # type: ignore  # noqa: F401
    from events_ndjson.types import EventsNdjsonError  # type: ignore  # noqa: F401
    SCHEMA_VERSION = "events-ndjson/v1"
    _USING_REAL_LIB = True
except ImportError:
    # ----- fallback Writer + error type ------------------------------------
    SCHEMA_VERSION = "events-ndjson/v1"
    _TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
    _COST_REQUIRED = ("agent", "cost_usd")

    class EventsNdjsonError(Exception):
        """Base class for fallback shim errors."""

    def _utc_ts() -> str:
        now = datetime.now(timezone.utc)
        return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"

    def _validate_cost_payload(payload: Dict[str, Any]) -> None:
        if not isinstance(payload, dict):
            raise EventsNdjsonError("payload must be a dict")
        for k in _COST_REQUIRED:
            if k not in payload:
                raise EventsNdjsonError(f"cost payload missing required field: {k}")
        if not isinstance(payload["agent"], str) or not payload["agent"]:
            raise EventsNdjsonError("agent must be a non-empty string")
        cost = payload["cost_usd"]
        if not isinstance(cost, (int, float)) or cost < 0:
            raise EventsNdjsonError("cost_usd must be a non-negative number")

    class Writer:  # type: ignore[no-redef]
        """Minimal cost-stream writer with atomic line append (fallback)."""

        def __init__(
            self,
            stream: str,
            source: str,
            path: Union[str, Path],
            *,
            ensure_dir: bool = True,
        ) -> None:
            if stream != "cost":
                raise EventsNdjsonError(
                    f"fallback shim only supports stream='cost', got {stream!r}. "
                    "Install the real `events-ndjson` package for full stream support."
                )
            self.stream = stream
            self.source = source
            self.path = Path(path)
            self._lock = threading.Lock()
            self._fd: Optional[int] = None
            if ensure_dir:
                self.path.parent.mkdir(parents=True, exist_ok=True)
            self._open()

        def _open(self) -> None:
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            self._fd = os.open(str(self.path), flags, 0o644)

        def append(
            self,
            event_type: str,
            payload: Dict[str, Any],
            *,
            ts: Optional[str] = None,
            correlation_id: Optional[str] = None,
        ) -> Dict[str, Any]:
            _validate_cost_payload(payload)
            envelope = {
                "ts": ts or _utc_ts(),
                "source": self.source,
                "stream": self.stream,
                "event_type": event_type,
                "payload": payload,
                "correlation_id": correlation_id or str(uuid.uuid4()),
                "schema_version": SCHEMA_VERSION,
            }
            if not _TS_RE.match(envelope["ts"]):
                raise EventsNdjsonError("ts must be UTC ISO 8601 with millisecond precision")
            line = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n"
            data = line.encode("utf-8")
            with self._lock:
                if self._fd is None:
                    self._open()
                assert self._fd is not None
                written = os.write(self._fd, data)
                while written < len(data):  # pragma: no cover
                    written += os.write(self._fd, data[written:])
            return envelope

        def close(self) -> None:
            with self._lock:
                if self._fd is not None:
                    try:
                        os.close(self._fd)
                    finally:
                        self._fd = None

        def __enter__(self) -> "Writer":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            self.close()

        def __del__(self) -> None:  # pragma: no cover
            try:
                self.close()
            except Exception:
                pass


def using_real_lib() -> bool:
    """Diagnostic: True iff the real events-ndjson package is in use."""
    return _USING_REAL_LIB


# ---------------------------------------------------------------------------
# Operator-core file-per-stream helpers (always present, both paths).
#
# The recipes runtime emits both 'runs' and 'cost' stream events from a
# single recipe execution. These helpers write to OPERATOR_EVENTS_DIR
# without going through the strict cost-only Writer.
# ---------------------------------------------------------------------------

def _events_dir() -> Path:
    return Path(
        os.environ.get(
            "OPERATOR_EVENTS_DIR",
            str(Path.home() / ".operator" / "data"),
        )
    )


def _stream_path(stream: str) -> Path:
    base = _events_dir()
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{stream}.ndjson"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def append_event(
    stream: str,
    kind: str,
    *,
    recipe: Optional[str] = None,
    correlation_id: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    ts: Optional[str] = None,
) -> Dict[str, Any]:
    """Append one event to <events_dir>/<stream>.ndjson.

    Simple file-per-stream API. Accepts any stream name (unlike the
    strict cost-only ``Writer``).
    """
    if not isinstance(stream, str) or not stream:
        raise EventsNdjsonError("stream must be a non-empty string")
    if not isinstance(kind, str) or not kind:
        raise EventsNdjsonError("kind must be a non-empty string")

    envelope: Dict[str, Any] = {
        "ts": ts or _now_iso(),
        "stream": stream,
        "kind": kind,
        "recipe": recipe,
        "correlation_id": correlation_id,
    }
    if payload:
        for k, v in payload.items():
            envelope[k] = v

    line = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")) + "\n"
    target = _stream_path(stream)
    with open(target, "a", encoding="utf-8") as fh:
        fh.write(line)
    return envelope


def read_events(stream: str) -> list[Dict[str, Any]]:
    """Return all events written to ``<events_dir>/<stream>.ndjson``."""
    target = _stream_path(stream)
    if not target.exists():
        return []
    out: list[Dict[str, Any]] = []
    with open(target, "r", encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                out.append(json.loads(raw))
            except json.JSONDecodeError:
                continue
    return out
