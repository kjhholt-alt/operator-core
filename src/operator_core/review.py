"""Weekly Operator V3 review generator.

Summarizes the last 7 days of job ledger activity, hook block events,
deploy health, and costs into a markdown blob. Saved to
`.operator-v3/reviews/YYYY-WW.md` and (via the CLI entry) posted to
`#automations`.

The Claude client is injectable so tests run offline. The CLI entry only
touches the network when invoked at runtime.
"""
from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .paths import DATA_DIR, DB_PATH, PACKAGE_ROOT, STATUS_PATH
from .store import JobStore

REVIEWS_DIR = DATA_DIR / "reviews"
DEFAULT_COSTS_CSV = PACKAGE_ROOT / "costs.csv"
DEFAULT_HOOKS_LOG = DATA_DIR / "logs" / "hooks.jsonl"

ClaudeClient = Callable[[str], str]
"""A callable that takes a prompt and returns a markdown summary string.

Tests inject a fake that returns canned text. The real CLI entry wires up
the `anthropic` client lazily so importing this module stays cheap.
"""

REQUIRED_SECTIONS = (
    "# Operator Weekly Review",
    "## What ran",
    "## What failed",
    "## Cost delta",
    "## Top 3 recommendations",
)


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _week_stamp(now: datetime) -> str:
    iso_year, iso_week, _ = now.isocalendar()
    return f"{iso_year}-W{iso_week:02d}"


def gather_job_stats(store: JobStore, cutoff: datetime) -> dict[str, Any]:
    jobs = store.list_jobs(limit=10000)
    window = []
    for job in jobs:
        created = _parse_iso(job.created_at)
        if created is None:
            continue
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        if created >= cutoff:
            window.append(job)
    by_status: dict[str, int] = {}
    by_project: dict[str, int] = {}
    failed: list[dict[str, Any]] = []
    for job in window:
        by_status[job.status] = by_status.get(job.status, 0) + 1
        project = job.project or "-"
        by_project[project] = by_project.get(project, 0) + 1
        if job.status in {"failed", "error"}:
            failed.append(
                {
                    "id": job.id,
                    "action": job.action,
                    "project": project,
                    "updated_at": job.updated_at,
                }
            )
    return {
        "total": len(window),
        "by_status": by_status,
        "by_project": by_project,
        "failed": failed,
    }


def gather_hook_blocks(hooks_log: Path, cutoff: datetime) -> list[dict[str, Any]]:
    if not hooks_log.exists():
        return []
    import json as _json

    out: list[dict[str, Any]] = []
    try:
        with hooks_log.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = _json.loads(line)
                except _json.JSONDecodeError:
                    continue
                if not entry.get("blocked"):
                    continue
                ts = _parse_iso(entry.get("ts"))
                if ts and ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts and ts >= cutoff:
                    out.append(
                        {
                            "ts": entry.get("ts"),
                            "reason": entry.get("reason"),
                            "tool_name": entry.get("tool_name"),
                        }
                    )
    except OSError:
        return []
    return out


def gather_cost_delta(costs_csv: Path, cutoff: datetime) -> dict[str, float]:
    if not costs_csv.exists():
        return {"window_total": 0.0, "prior_window_total": 0.0, "delta": 0.0}
    prior_cutoff = cutoff - timedelta(days=7)
    window_total = 0.0
    prior_total = 0.0
    try:
        with costs_csv.open("r", encoding="utf-8", newline="") as fh:
            for row in csv.reader(fh):
                if len(row) < 3:
                    continue
                ts = _parse_iso(row[0])
                if ts is None:
                    continue
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                try:
                    cost = float(row[2])
                except ValueError:
                    continue
                if ts >= cutoff:
                    window_total += cost
                elif ts >= prior_cutoff:
                    prior_total += cost
    except OSError:
        pass
    return {
        "window_total": window_total,
        "prior_window_total": prior_total,
        "delta": window_total - prior_total,
    }


def _build_prompt(
    job_stats: dict[str, Any],
    hook_blocks: Iterable[dict[str, Any]],
    deploy_health: dict[str, str],
    cost_delta: dict[str, float],
) -> str:
    lines = [
        "You are the Operator weekly review agent. Summarize the last 7 days of",
        "Operator V3 activity using ONLY the data below. Output markdown with",
        "these exact H2 sections: 'What ran', 'What failed', 'Cost delta',",
        "'Top 3 recommendations'. Start the document with '# Operator Weekly Review'.",
        "",
        f"Jobs total: {job_stats['total']}",
        f"By status: {job_stats['by_status']}",
        f"By project: {job_stats['by_project']}",
        f"Failed jobs: {job_stats['failed']}",
        f"Hook blocks: {list(hook_blocks)}",
        f"Deploy health: {deploy_health}",
        f"Cost window total: ${cost_delta['window_total']:.2f}",
        f"Cost prior window total: ${cost_delta['prior_window_total']:.2f}",
        f"Cost delta: ${cost_delta['delta']:.2f}",
    ]
    return "\n".join(lines)


def _offline_fallback(
    job_stats: dict[str, Any],
    hook_blocks: list[dict[str, Any]],
    deploy_health: dict[str, str],
    cost_delta: dict[str, float],
) -> str:
    """Deterministic fallback used when no Claude client is provided."""
    lines = [
        "# Operator Weekly Review",
        "",
        "## What ran",
        f"- Total jobs: {job_stats['total']}",
    ]
    for project, count in sorted(job_stats["by_project"].items()):
        lines.append(f"- {project}: {count} job(s)")
    lines += ["", "## What failed"]
    if job_stats["failed"]:
        for f in job_stats["failed"]:
            lines.append(f"- {f['id']} {f['action']} ({f['project']})")
    else:
        lines.append("- No failures recorded.")
    lines += ["", "## Cost delta"]
    lines.append(f"- Window: ${cost_delta['window_total']:.2f}")
    lines.append(f"- Prior:  ${cost_delta['prior_window_total']:.2f}")
    lines.append(f"- Delta:  ${cost_delta['delta']:+.2f}")
    lines += ["", "## Top 3 recommendations"]
    recs = []
    if job_stats["failed"]:
        recs.append("Investigate recent job failures in the ledger.")
    if hook_blocks:
        recs.append("Review hook block events; tighten command guardrails if repeated.")
    if cost_delta["delta"] > 0:
        recs.append("Cost is up week-over-week — audit top spending scripts.")
    while len(recs) < 3:
        recs.append("Keep watching the dashboard at http://127.0.0.1:8765/ops.")
    for rec in recs[:3]:
        lines.append(f"- {rec}")
    lines.append("")
    return "\n".join(lines)


def generate_weekly_review(
    now: datetime | None = None,
    claude_client: ClaudeClient | None = None,
    *,
    store: JobStore | None = None,
    status_path: Path | None = None,
    costs_csv: Path | None = None,
    hooks_log: Path | None = None,
    reviews_dir: Path | None = None,
    db_path: Path | None = None,
) -> dict[str, Any]:
    """Generate and persist the weekly review. Returns a dict with
    `path`, `markdown`, `week`. All path args are injectable for tests.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cutoff = now - timedelta(days=7)

    store = store or JobStore(db_path or DB_PATH)
    job_stats = gather_job_stats(store, cutoff)
    hook_blocks = gather_hook_blocks(hooks_log or DEFAULT_HOOKS_LOG, cutoff)
    cost_delta = gather_cost_delta(costs_csv or DEFAULT_COSTS_CSV, cutoff)

    from .utils import status as status_mod

    status_data = status_mod.load_or_default(status_path or STATUS_PATH)
    deploy_health = status_data.get("deploy_health") or {}

    prompt = _build_prompt(job_stats, hook_blocks, deploy_health, cost_delta)

    if claude_client is not None:
        markdown = claude_client(prompt)
        # Guarantee required sections exist even if the model skipped one.
        missing = [s for s in REQUIRED_SECTIONS if s not in markdown]
        if missing:
            markdown = (
                markdown.rstrip()
                + "\n\n"
                + "\n".join(f"{s}\n- (not produced by model)" for s in missing)
                + "\n"
            )
    else:
        markdown = _offline_fallback(job_stats, hook_blocks, deploy_health, cost_delta)

    target_dir = Path(reviews_dir) if reviews_dir else REVIEWS_DIR
    target_dir.mkdir(parents=True, exist_ok=True)
    week = _week_stamp(now)
    target = target_dir / f"{week}.md"
    target.write_text(markdown, encoding="utf-8")
    return {"path": target, "markdown": markdown, "week": week, "prompt": prompt}


def _post_to_discord(markdown: str, week: str) -> bool:
    try:
        from .utils.discord import notify  # lazy
    except Exception:
        return False
    title = f"Operator weekly review — {week}"
    return bool(notify("automations", title=title, body=markdown[:3900], color="blue"))


def _cli(argv: list[str]) -> int:
    if not argv or argv[0] != "week":
        print("usage: python -m operator_v3.review week")
        return 2
    result = generate_weekly_review()
    print(f"wrote {result['path']}")
    posted = _post_to_discord(result["markdown"], result["week"])
    print(f"discord posted: {posted}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    import sys

    raise SystemExit(_cli(sys.argv[1:]))
