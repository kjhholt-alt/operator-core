"""Deploy health polling for Vercel / Railway / Supabase / local services.

HTTP-only. Uses urllib so there is no new dependency. All calls are mocked in
tests — this module must never be exercised against real endpoints from the
test suite.

Also provides the `--canary <project>` CLI mode (one live poll, verbose hop
log written to `.operator-v3/logs/deploy-health.jsonl` with 10 MB rotation)
and a circuit-breaker surface (`is_tripped` / `reset_trip`) that the
auto-merge gate can consume after a future wiring pass.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from .config import ConfigError, ProjectConfig, find_project, load_projects
from .paths import DATA_DIR, ensure_data_dirs


@dataclass(frozen=True)
class DeployHealth:
    status: str
    elapsed_seconds: float
    attempts: int
    http_status: int | None = None
    last_error: str | None = None


HttpGetter = Callable[[str, float], tuple[int, bytes]]


def _default_http_get(url: str, timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(url, headers={"User-Agent": "operator-v3-healthcheck/1"})
    with urllib.request.urlopen(req, timeout=timeout) as response:  # noqa: S310 - health URL only
        return response.status, response.read()


def poll_health_url(
    url: str,
    *,
    expected_status: int = 200,
    per_request_timeout: float = 30.0,
    total_timeout: float = 300.0,
    interval: float = 15.0,
    http_get: HttpGetter | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> DeployHealth:
    """Poll an HTTP health URL until 2xx (matching expected_status) or timeout.

    Total wait bounded by total_timeout; interval between attempts is `interval`.
    Caller supplies http_get/sleep/clock in tests to avoid real network/time.
    """
    getter = http_get or _default_http_get
    started = clock()
    attempts = 0
    last_error: str | None = None
    last_status: int | None = None

    while True:
        attempts += 1
        try:
            status, _ = getter(url, per_request_timeout)
            last_status = status
            if 200 <= status < 300 and status == expected_status:
                return DeployHealth(
                    status="healthy",
                    elapsed_seconds=clock() - started,
                    attempts=attempts,
                    http_status=status,
                )
            last_error = f"unexpected status {status}"
        except urllib.error.HTTPError as exc:
            last_status = exc.code
            last_error = f"http error {exc.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        elapsed = clock() - started
        if elapsed + interval >= total_timeout:
            return DeployHealth(
                status="unhealthy",
                elapsed_seconds=elapsed,
                attempts=attempts,
                http_status=last_status,
                last_error=last_error or "timeout",
            )
        sleep(interval)


def check_project_deploy(
    project: ProjectConfig,
    *,
    http_get: HttpGetter | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> DeployHealth:
    """Provider-aware wrapper. Vercel/Railway/Supabase all use URL polling.

    Railway does NOT shell out to the interactive CLI — if a configured
    health URL isn't set, we return `skipped` rather than guessing.
    """
    provider = project.deploy.provider.lower()
    url = project.deploy_health_url

    if provider == "local":
        return DeployHealth(status="skipped", elapsed_seconds=0.0, attempts=0, last_error="local provider")

    if provider == "railway" and not project.deploy.url:
        return DeployHealth(
            status="skipped",
            elapsed_seconds=0.0,
            attempts=0,
            last_error="no Railway health URL configured",
        )

    return poll_health_url(
        url,
        expected_status=project.health.expected_status,
        http_get=http_get,
        sleep=sleep,
        clock=clock,
    )


# ---------------------------------------------------------------------------
# Canary mode, logging, rotation, circuit-breaker
# ---------------------------------------------------------------------------


LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB rotation threshold
CANARY_TRIP_THRESHOLD = 3
TRIP_FLAG_PATH = DATA_DIR / "deploy-health-tripped"
CANARY_LOG_PATH = DATA_DIR / "logs" / "deploy-health.jsonl"


@dataclass(frozen=True)
class HttpHop:
    """Single HTTP hop captured during a canary run."""

    attempt: int
    url: str
    status: int | None
    latency_seconds: float
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "attempt": self.attempt,
            "url": self.url,
            "status": self.status,
            "latency_seconds": round(self.latency_seconds, 4),
            "error": self.error,
        }


@dataclass(frozen=True)
class CanaryResult:
    project: str
    url: str
    ok: bool
    hops: list[HttpHop]
    final_status: int | None
    final_error: str | None
    duration_seconds: float

    def signature(self) -> str:
        """Stable identifier for 'the same failure repeated'.

        We collapse the last hop's outcome (status or error class) so three
        consecutive 502s count as the same failure, but a 502 followed by a
        connect-timeout does not.
        """
        if self.ok:
            return "ok"
        if self.final_status is not None:
            return f"status:{self.final_status}"
        if self.final_error:
            head = self.final_error.split(":", 1)[0].strip()
            return f"error:{head}"
        return "error:unknown"

    def to_dict(self) -> dict[str, Any]:
        return {
            "project": self.project,
            "url": self.url,
            "ok": self.ok,
            "hops": [hop.to_dict() for hop in self.hops],
            "final_status": self.final_status,
            "final_error": self.final_error,
            "duration_seconds": round(self.duration_seconds, 4),
            "signature": self.signature(),
        }


def _rotate_log(path: Path, max_bytes: int = LOG_MAX_BYTES) -> None:
    try:
        if path.exists() and path.stat().st_size >= max_bytes:
            rotated = path.with_suffix(path.suffix + ".1")
            if rotated.exists():
                rotated.unlink()
            path.rename(rotated)
    except OSError:
        # Rotation is best-effort; never block a canary on FS quirks.
        pass


def _append_jsonl(path: Path, record: dict[str, Any], *, max_bytes: int = LOG_MAX_BYTES) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _rotate_log(path, max_bytes=max_bytes)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, sort_keys=True) + "\n")


def is_tripped(flag_path: Path = TRIP_FLAG_PATH) -> bool:
    """Return True when a circuit-breaker flag file is present.

    The auto-merge gate should consult this before approving any deploy-
    sensitive merge. Exposed as a plain helper so `security.py` (owned by
    another lane) can wire it in with a one-line import later.
    """
    return flag_path.exists()


def reset_trip(flag_path: Path = TRIP_FLAG_PATH) -> bool:
    """Clear the circuit-breaker flag. Returns True if it was tripped."""
    if flag_path.exists():
        flag_path.unlink()
        return True
    return False


def _read_trip_history(flag_path: Path) -> dict[str, Any]:
    if not flag_path.exists():
        return {"signatures": [], "consecutive": 0, "last_signature": None}
    try:
        return json.loads(flag_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"signatures": [], "consecutive": 0, "last_signature": None}


def _canary_history_path() -> Path:
    return DATA_DIR / "deploy-health-canary-history.json"


def _load_canary_history() -> dict[str, Any]:
    path = _canary_history_path()
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _save_canary_history(history: dict[str, Any]) -> None:
    ensure_data_dirs()
    path = _canary_history_path()
    path.write_text(json.dumps(history, sort_keys=True), encoding="utf-8")


def _update_circuit_breaker(
    project: str,
    signature: str,
    *,
    threshold: int = CANARY_TRIP_THRESHOLD,
    flag_path: Path = TRIP_FLAG_PATH,
) -> tuple[int, bool]:
    """Record a canary signature; trip the breaker after `threshold` in a row."""
    history = _load_canary_history()
    entry = history.get(project) or {"last_signature": None, "consecutive": 0}

    if signature == "ok":
        entry = {"last_signature": "ok", "consecutive": 0}
        history[project] = entry
        _save_canary_history(history)
        return 0, False

    if entry.get("last_signature") == signature:
        entry["consecutive"] = int(entry.get("consecutive", 0)) + 1
    else:
        entry["last_signature"] = signature
        entry["consecutive"] = 1
    history[project] = entry
    _save_canary_history(history)

    consecutive = int(entry["consecutive"])
    tripped_now = False
    if consecutive >= threshold and not flag_path.exists():
        flag_path.parent.mkdir(parents=True, exist_ok=True)
        flag_path.write_text(
            json.dumps(
                {
                    "project": project,
                    "signature": signature,
                    "consecutive": consecutive,
                    "tripped_at": datetime.now().isoformat(timespec="seconds"),
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        tripped_now = True
    return consecutive, tripped_now


def run_canary(
    project: ProjectConfig,
    *,
    attempts: int = 1,
    http_get: HttpGetter | None = None,
    clock: Callable[[], float] = time.monotonic,
    now: Callable[[], datetime] = datetime.now,
    log_path: Path = CANARY_LOG_PATH,
    flag_path: Path = TRIP_FLAG_PATH,
    threshold: int = CANARY_TRIP_THRESHOLD,
) -> CanaryResult:
    """Execute a single live canary pass and log every HTTP hop."""
    getter = http_get or _default_http_get
    url = project.deploy_health_url
    hops: list[HttpHop] = []
    started = clock()
    final_status: int | None = None
    final_error: str | None = None
    ok = False

    for attempt in range(1, attempts + 1):
        hop_start = clock()
        try:
            status, _ = getter(url, 30.0)
            latency = clock() - hop_start
            hop = HttpHop(attempt=attempt, url=url, status=status, latency_seconds=latency)
            hops.append(hop)
            final_status = status
            if 200 <= status < 300 and status == project.health.expected_status:
                ok = True
                break
            final_error = f"unexpected status {status}"
        except urllib.error.HTTPError as exc:
            latency = clock() - hop_start
            hops.append(
                HttpHop(
                    attempt=attempt,
                    url=url,
                    status=exc.code,
                    latency_seconds=latency,
                    error=f"HTTPError: {exc.code}",
                )
            )
            final_status = exc.code
            final_error = f"HTTPError: {exc.code}"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            latency = clock() - hop_start
            hops.append(
                HttpHop(
                    attempt=attempt,
                    url=url,
                    status=None,
                    latency_seconds=latency,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            final_error = f"{type(exc).__name__}: {exc}"

    result = CanaryResult(
        project=project.slug,
        url=url,
        ok=ok,
        hops=hops,
        final_status=final_status,
        final_error=final_error,
        duration_seconds=clock() - started,
    )

    consecutive, tripped_now = _update_circuit_breaker(
        project.slug, result.signature(), threshold=threshold, flag_path=flag_path
    )

    record = {
        "type": "canary",
        "timestamp": now().isoformat(timespec="seconds"),
        "consecutive_failures": consecutive,
        "tripped_now": tripped_now,
        "breaker_tripped": flag_path.exists(),
        **result.to_dict(),
    }
    _append_jsonl(log_path, record)
    return result


def _print_canary(result: CanaryResult) -> None:
    print(f"canary project={result.project} url={result.url}")
    for hop in result.hops:
        marker = "ok" if hop.status and 200 <= hop.status < 300 else "fail"
        print(
            f"  hop#{hop.attempt} [{marker}] status={hop.status} "
            f"latency={hop.latency_seconds:.3f}s error={hop.error or '-'}"
        )
    print(
        f"  result ok={result.ok} final_status={result.final_status} "
        f"signature={result.signature()} duration={result.duration_seconds:.3f}s"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m operator_v3.deploy_health",
        description="Operator V3 deploy health canary runner.",
    )
    parser.add_argument(
        "--canary",
        metavar="PROJECT",
        help="Run one live canary against the given project slug.",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=1,
        help="How many hops per canary run (default 1).",
    )
    parser.add_argument(
        "--is-tripped",
        action="store_true",
        help="Exit 0 if the circuit-breaker is tripped, else exit 1.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Clear the circuit-breaker flag.",
    )
    args = parser.parse_args(argv)

    if args.is_tripped:
        tripped = is_tripped()
        print("tripped" if tripped else "ok")
        return 0 if tripped else 1

    if args.reset:
        was = reset_trip()
        print("cleared" if was else "already-clear")
        return 0

    if not args.canary:
        parser.print_help()
        return 2

    try:
        project = find_project(args.canary)
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    result = run_canary(project, attempts=args.attempts)
    _print_canary(result)
    return 0 if result.ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
