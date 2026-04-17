"""Prometheus-style text metrics for Operator V3.

No prometheus_client dependency — Prom text format is trivial to emit by
hand. We read directly from the SQLite job ledger, costs.csv, and the
`.operator-status.json` file; there is no separate collector process.

Exposed metrics:
  operator_jobs_total{status="..."}           counter
  operator_jobs_duration_seconds_bucket{le=...} histogram buckets
  operator_jobs_duration_seconds_sum          histogram sum
  operator_jobs_duration_seconds_count        histogram count
  operator_deploy_health{project="...",state="ok|warn|tripped"} gauge (1/0)
  operator_hook_blocks_total                  counter
  operator_cost_usd_today                     gauge
"""
from __future__ import annotations

import csv
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from .paths import DB_PATH, PACKAGE_ROOT, STATUS_PATH
from .store import JobStore

DEFAULT_COSTS_CSV = PACKAGE_ROOT / "costs.csv"

# Duration histogram buckets in seconds. Tuned for local ops jobs: small
# scripts under 10s, feature builds under 10min, worst-case an hour.
DURATION_BUCKETS = (1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 3600.0)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _fmt_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{_escape_label(v)}"' for k, v in sorted(labels.items()))
    return "{" + inner + "}"


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _collect_job_counters(jobs: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for job in jobs:
        counts[job.status] = counts.get(job.status, 0) + 1
    return counts


def _collect_durations(jobs: Iterable[Any]) -> list[float]:
    durations: list[float] = []
    for job in jobs:
        start = _parse_iso(job.created_at)
        end = _parse_iso(job.updated_at)
        if start is None or end is None:
            continue
        delta = (end - start).total_seconds()
        if delta < 0:
            continue
        durations.append(delta)
    return durations


def _histogram_lines(metric: str, durations: list[float]) -> list[str]:
    counts = [0] * len(DURATION_BUCKETS)
    total = 0.0
    for d in durations:
        total += d
        for i, threshold in enumerate(DURATION_BUCKETS):
            if d <= threshold:
                counts[i] += 1
    lines: list[str] = [f"# TYPE {metric} histogram"]
    cumulative = 0
    for i, threshold in enumerate(DURATION_BUCKETS):
        cumulative += counts[i]
        lines.append(f'{metric}_bucket{{le="{threshold}"}} {cumulative}')
    lines.append(f'{metric}_bucket{{le="+Inf"}} {len(durations)}')
    lines.append(f"{metric}_sum {total:.3f}")
    lines.append(f"{metric}_count {len(durations)}")
    return lines


def _cost_today(costs_csv: Path) -> float:
    if not costs_csv.exists():
        return 0.0
    today = date.today().isoformat()
    total = 0.0
    try:
        with costs_csv.open("r", encoding="utf-8", newline="") as fh:
            reader = csv.reader(fh)
            for row in reader:
                if not row:
                    continue
                ts = row[0]
                if not ts.startswith(today):
                    continue
                try:
                    total += float(row[2])
                except (IndexError, ValueError):
                    continue
    except OSError:
        return 0.0
    return total


def _read_status(status_path: Path) -> dict[str, Any]:
    from utils import status as status_mod

    return status_mod.load_or_default(status_path)


def render_metrics(
    store: JobStore | None = None,
    *,
    status_path: Path | None = None,
    costs_csv: Path | None = None,
    db_path: Path | None = None,
) -> str:
    """Render the full plaintext metrics blob.

    Arguments are all injectable for tests. In the daemon, the server owns
    a JobStore and the paths defaults from `operator_v3.paths` apply.
    """
    if store is None:
        store = JobStore(db_path or DB_PATH)
    jobs = store.list_jobs(limit=10000)
    status_data = _read_status(status_path or STATUS_PATH)
    cost_today = _cost_today(Path(costs_csv) if costs_csv else DEFAULT_COSTS_CSV)

    lines: list[str] = []

    # operator_jobs_total
    counts = _collect_job_counters(jobs)
    lines.append("# TYPE operator_jobs_total counter")
    # Emit every observed status; guarantee a zero-row for common statuses so
    # the counter is stable across transitions.
    for status_name in sorted(set(list(counts.keys()) + ["queued", "running", "done", "failed"])):
        lines.append(
            f"operator_jobs_total{_fmt_labels({'status': status_name})} {counts.get(status_name, 0)}"
        )

    # operator_jobs_duration_seconds histogram
    durations = _collect_durations(jobs)
    lines.extend(_histogram_lines("operator_jobs_duration_seconds", durations))

    # operator_deploy_health
    lines.append("# TYPE operator_deploy_health gauge")
    deploy_health = status_data.get("deploy_health") or {}
    if not deploy_health:
        lines.append("operator_deploy_health 0")
    for project, state in sorted(deploy_health.items()):
        labels = {"project": project, "state": str(state)}
        lines.append(f"operator_deploy_health{_fmt_labels(labels)} 1")

    # operator_hook_blocks_total
    hook_blocks = status_data.get("hook_blocks_recent") or []
    lines.append("# TYPE operator_hook_blocks_total counter")
    lines.append(f"operator_hook_blocks_total {len(hook_blocks)}")

    # operator_cost_usd_today
    lines.append("# TYPE operator_cost_usd_today gauge")
    lines.append(f"operator_cost_usd_today {cost_today:.4f}")

    return "\n".join(lines) + "\n"


def register_metrics_route(server: Any, store: JobStore) -> None:
    """Register GET /metrics on the shared HTTP server extension table."""
    from .http_server import register_extra_route

    def _handler(handler, body):
        text = render_metrics(store=store, status_path=getattr(server, "status_path", None))
        handler._text(200, text, content_type="text/plain; version=0.0.4; charset=utf-8")
        return None  # handler wrote its own response

    register_extra_route("GET", "/metrics", _handler)
