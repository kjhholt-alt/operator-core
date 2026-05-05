"""Vendored stub of events-ndjson v1 Writer.

This is a minimal, dependency-free implementation of just enough of the
events-ndjson v1 spec to emit cost-stream events from operator-core. We
vendor it instead of taking a hard dependency because:

1. The library is brand-new and not yet on PyPI.
2. operator-core needs to ship without a network install step.

Once events-ndjson is published we replace this file with
`from events_ndjson import Writer`.

Spec: https://github.com/kjhholt-alt/events-ndjson
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

SCHEMA_VERSION = "events-ndjson/v1"
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")
_COST_REQUIRED = ("agent", "cost_usd")


class EventsNdjsonError(Exception):
    """Base class for vendored stub errors."""


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


class Writer:
    """Minimal cost-stream writer with atomic line append."""

    def __init__(
        self,
        stream: str,
        source: str,
        path: Union[str, Path],
        *,
        ensure_dir: bool = True,
    ) -> None:
        if stream != "cost":
            # The vendored stub deliberately only knows the cost stream.
            # Other streams should wait until we depend on the real lib.
            raise EventsNdjsonError(
                f"vendored stub only supports stream='cost', got {stream!r}"
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
            while written < len(data):  # pragma: no cover - kernel guarantee
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

    def __del__(self) -> None:  # pragma: no cover - best effort
        try:
            self.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Module-level helpers used by the recipes runtime.
#
# These provide a simpler, file-per-stream NDJSON store rooted at
# OPERATOR_EVENTS_DIR (default: ~/.operator/data). The runtime emits both
# 'runs' and 'cost' stream events from a single recipe execution; the strict
# cost-only Writer above is kept for the cost_events.py public API.
# ---------------------------------------------------------------------------

DEFAULT_EVENTS_DIR = Path(
    os.environ.get(
        "OPERATOR_EVENTS_DIR",
        str(Path.home() / ".operator" / "data"),
    )
)


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

    This is the simple file-per-stream API the recipes runtime uses. It
    deliberately accepts any stream name -- unlike the strict cost-only
    ``Writer`` class -- because the runtime emits both 'runs' and 'cost'
    events from one recipe execution.
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
    """Return all events written to ``<events_dir>/<stream>.ndjson``.

    Returns an empty list if the file does not yet exist.
    """
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
