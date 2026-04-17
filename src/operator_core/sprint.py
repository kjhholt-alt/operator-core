"""Sprint + handoff tooling.

The "90-minute sweet spot" workflow dog-fooded:
    operator sprint start "<goal>"    → record git heads, start clock
    operator sprint status             → elapsed + commits-since-start
    operator handoff                   → generate HANDOFF_<ts>.md + post paste-blob
    operator sprint resume             → cat newest HANDOFF_*.md

State lives at `<data_dir>/current-sprint.json`.

Git operations shell out to the `git` binary with a short timeout — they're
expected to work on real repos but must not crash if a project isn't a git
repo or git isn't on PATH. Everything returns gracefully on error so the
demo/handoff never fails because one project is in a weird state.
"""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


# Sweet-spot warning thresholds (minutes).
SWEET_SPOT_MIN = 70
SWEET_SPOT_MAX = 95


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------


@dataclass
class SprintState:
    """A single sprint recorded on disk."""

    goal: str
    started_at_iso: str
    git_heads: dict[str, str] = field(default_factory=dict)
    branches: dict[str, str] = field(default_factory=dict)
    title: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict) -> "SprintState":
        return cls(
            goal=payload.get("goal", ""),
            started_at_iso=payload.get("started_at_iso", ""),
            git_heads=dict(payload.get("git_heads") or {}),
            branches=dict(payload.get("branches") or {}),
            title=payload.get("title"),
        )


def _state_path(data_dir: Path) -> Path:
    return Path(data_dir) / "current-sprint.json"


def load_state(data_dir: Path) -> SprintState | None:
    path = _state_path(data_dir)
    if not path.exists():
        return None
    try:
        return SprintState.from_dict(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError):
        return None


def save_state(state: SprintState, data_dir: Path) -> Path:
    path = _state_path(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), indent=2), encoding="utf-8")
    return path


def clear_state(data_dir: Path) -> None:
    path = _state_path(data_dir)
    if path.exists():
        path.unlink()


# ---------------------------------------------------------------------------
# Git helpers — all safe, all timeout-bounded
# ---------------------------------------------------------------------------


def _git(args: list[str], cwd: Path, timeout: float = 3.0) -> str | None:
    """Run `git <args>` in `cwd`, return stdout stripped. None on failure."""
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout.strip()


def _git_head(cwd: Path) -> str | None:
    return _git(["rev-parse", "HEAD"], cwd)


def _git_branch(cwd: Path) -> str | None:
    return _git(["rev-parse", "--abbrev-ref", "HEAD"], cwd)


def _git_log_since(cwd: Path, since_sha: str, limit: int = 40) -> list[str]:
    out = _git(
        ["log", "--oneline", "--no-decorate", f"-n{limit}", f"{since_sha}..HEAD"],
        cwd,
    )
    if not out:
        return []
    return [line.rstrip() for line in out.splitlines() if line.strip()]


def _git_diff_stat_since(cwd: Path, since_sha: str) -> str | None:
    return _git(["diff", "--stat", f"{since_sha}..HEAD"], cwd)


def _git_commit_count_since(cwd: Path, since_sha: str) -> int:
    out = _git(["rev-list", "--count", f"{since_sha}..HEAD"], cwd)
    try:
        return int(out or "0")
    except ValueError:
        return 0


def _git_status_short(cwd: Path) -> str | None:
    return _git(["status", "--short"], cwd)


def _git_files_changed_since(cwd: Path, since_sha: str) -> list[str]:
    out = _git(["diff", "--name-only", f"{since_sha}..HEAD"], cwd)
    if not out:
        return []
    return [line for line in out.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Sprint operations
# ---------------------------------------------------------------------------


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _iter_project_paths(settings) -> list[tuple[str, Path]]:
    """Return (slug, path) for each tracked project. Settings may be None."""
    out: list[tuple[str, Path]] = []
    if settings is None:
        return out
    for p in getattr(settings, "projects", []) or []:
        path = Path(str(p.path))
        if path.exists():
            out.append((p.slug, path))
    return out


def start_sprint(
    goal: str,
    *,
    settings,
    data_dir: Path,
    title: str | None = None,
) -> tuple[SprintState, bool]:
    """Start (or return) the current sprint.

    Idempotent: if a sprint is already active, returns (existing, False).
    Otherwise records git heads for every tracked project and returns
    (new_state, True).
    """
    existing = load_state(data_dir)
    if existing is not None:
        return existing, False

    heads: dict[str, str] = {}
    branches: dict[str, str] = {}
    for slug, path in _iter_project_paths(settings):
        sha = _git_head(path)
        if sha:
            heads[slug] = sha
        branch = _git_branch(path)
        if branch:
            branches[slug] = branch

    state = SprintState(
        goal=goal,
        started_at_iso=_utcnow_iso(),
        git_heads=heads,
        branches=branches,
        title=title,
    )
    save_state(state, data_dir)
    return state, True


def elapsed_minutes(state: SprintState, now: datetime | None = None) -> float:
    try:
        started = datetime.fromisoformat(state.started_at_iso)
    except ValueError:
        return 0.0
    now = now or datetime.now(timezone.utc)
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    delta = now - started
    return max(0.0, delta.total_seconds() / 60.0)


def sweet_spot_banner(minutes: float) -> str | None:
    if minutes >= SWEET_SPOT_MAX:
        return (
            f"[!] past sweet spot ({minutes:.0f}m) — wrap up + `operator handoff`"
        )
    if minutes >= SWEET_SPOT_MIN:
        return (
            f"[~] approaching sweet spot ({minutes:.0f}m) — plan the landing"
        )
    return None


def status_rows(state: SprintState, *, settings) -> list[dict[str, Any]]:
    """Per-project commit-since-start + files-changed + dirty-flag."""
    rows: list[dict[str, Any]] = []
    for slug, path in _iter_project_paths(settings):
        sha = state.git_heads.get(slug)
        if not sha:
            rows.append(
                {
                    "slug": slug,
                    "commits": 0,
                    "files": 0,
                    "dirty": False,
                    "note": "no baseline",
                }
            )
            continue
        commits = _git_commit_count_since(path, sha)
        files = len(_git_files_changed_since(path, sha))
        dirty_raw = _git_status_short(path) or ""
        rows.append(
            {
                "slug": slug,
                "commits": commits,
                "files": files,
                "dirty": bool(dirty_raw.strip()),
                "note": "",
            }
        )
    return rows


# ---------------------------------------------------------------------------
# Handoff document generation
# ---------------------------------------------------------------------------


def _scan_todo_items(project_root: Path, limit: int = 5) -> list[str]:
    """Pull top incomplete `- [ ] ...` items from a TODO.md if one exists."""
    candidates = [
        project_root / "TODO.md",
        project_root / "todo.md",
    ]
    for p in candidates:
        try:
            if not p.is_file():
                continue
            text = p.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        items: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("- [ ]") or stripped.startswith("* [ ]"):
                items.append(stripped.lstrip("-* ").strip())
            if len(items) >= limit:
                break
        if items:
            return items
    return []


def _collect_deploy_urls(settings) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    if settings is None:
        return out
    for p in getattr(settings, "projects", []) or []:
        deploy = getattr(p, "deploy", None)
        url = getattr(deploy, "url", None) if deploy else None
        if url:
            out.append((p.slug, url))
    return out


def _safe_git_summary(
    slug: str, path: Path, since_sha: str | None
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "slug": slug,
        "log": [],
        "diff_stat": "",
        "status": "",
        "commits": 0,
    }
    if since_sha:
        summary["log"] = _git_log_since(path, since_sha)
        summary["diff_stat"] = _git_diff_stat_since(path, since_sha) or ""
        summary["commits"] = _git_commit_count_since(path, since_sha)
    summary["status"] = _git_status_short(path) or ""
    return summary


def _filename_timestamp(now: datetime) -> str:
    return now.strftime("%Y-%m-%d_%H%M")


def render_handoff(
    *,
    state: SprintState | None,
    settings,
    projects_root: Path,
    title: str,
    now: datetime | None = None,
) -> str:
    """Render the HANDOFF_*.md body. Pure: caller writes the file."""
    now = now or datetime.now(timezone.utc)

    deploy_urls = _collect_deploy_urls(settings)

    per_project: list[dict[str, Any]] = []
    total_commits = 0
    for slug, path in _iter_project_paths(settings):
        since = state.git_heads.get(slug) if state else None
        summary = _safe_git_summary(slug, path, since)
        total_commits += summary["commits"]
        per_project.append(summary)

    elapsed = elapsed_minutes(state, now) if state else 0.0
    banner = sweet_spot_banner(elapsed) if state else None

    todo_items: list[str] = _scan_todo_items(projects_root)

    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(
        f"**Generated:** {now.isoformat(timespec='seconds')}  "
        f"**Duration:** {elapsed:.0f}m  "
        f"**Commits across tracked projects:** {total_commits}"
    )
    lines.append("")
    if state:
        lines.append(f"**Goal:** {state.goal or '(none recorded)'}")
        lines.append(f"**Sprint started:** {state.started_at_iso}")
        if banner:
            lines.append("")
            lines.append(f"> {banner}")
        lines.append("")

    # --- one-liner
    lines.append("## One-liner")
    lines.append("")
    if state and state.goal:
        lines.append(f"{state.goal}. {total_commits} commits across "
                     f"{len([p for p in per_project if p['commits'] > 0])} "
                     "repos in this session.")
    else:
        lines.append(
            f"{total_commits} commits across "
            f"{len([p for p in per_project if p['commits'] > 0])} repos "
            "in this session."
        )
    lines.append("")

    # --- what shipped
    lines.append("## What shipped")
    lines.append("")
    any_log = False
    for summary in per_project:
        if not summary["log"]:
            continue
        any_log = True
        lines.append(f"### {summary['slug']} ({summary['commits']} commits)")
        lines.append("")
        lines.append("```")
        for item in summary["log"]:
            lines.append(item)
        lines.append("```")
        lines.append("")
    if not any_log:
        lines.append("_No commits recorded since sprint start._")
        lines.append("")

    # --- what's in flight
    lines.append("## What's in flight")
    lines.append("")
    any_dirty = False
    for summary in per_project:
        status = summary.get("status") or ""
        diff_stat = summary.get("diff_stat") or ""
        if not status.strip() and not diff_stat.strip():
            continue
        any_dirty = True
        lines.append(f"### {summary['slug']}")
        lines.append("")
        if status.strip():
            lines.append("Uncommitted:")
            lines.append("```")
            lines.append(status.rstrip())
            lines.append("```")
            lines.append("")
        if diff_stat.strip():
            lines.append("Diff vs sprint start:")
            lines.append("```")
            lines.append(diff_stat.rstrip())
            lines.append("```")
            lines.append("")
    if not any_dirty:
        lines.append("_Working trees clean — everything committed._")
        lines.append("")

    # --- next best move
    lines.append("## Next best move")
    lines.append("")
    if todo_items:
        lines.append("Top open items from TODO.md:")
        lines.append("")
        for item in todo_items:
            lines.append(f"- [ ] {item}")
    else:
        lines.append(
            "No open TODO items detected. Suggested next step: review the "
            "handoff, run `operator sprint start` on the next goal."
        )
    lines.append("")

    # --- live URLs
    if deploy_urls:
        lines.append("## Live URLs")
        lines.append("")
        for slug, url in deploy_urls:
            lines.append(f"- **{slug}** — {url}")
        lines.append("")

    # --- rehydrate commands
    lines.append("## Rehydrate commands")
    lines.append("")
    lines.append("```bash")
    lines.append("# Daemon sanity")
    lines.append("operator doctor")
    lines.append("operator tasks list")
    lines.append("operator status --once")
    lines.append("")
    lines.append("# Verify the live site (if deployed)")
    lines.append(
        'curl -s -o /dev/null -w "/      %{http_code}\\n" '
        "https://operator.buildkit.store/"
    )
    lines.append(
        'curl -s -o /dev/null -w "/kruz  %{http_code}\\n" '
        "https://operator.buildkit.store/kruz"
    )
    lines.append(
        'curl -s -o /dev/null -w "/docs  %{http_code}\\n" '
        "https://operator.buildkit.store/docs"
    )
    lines.append("")
    lines.append("# Kick a fresh snapshot")
    lines.append("operator snapshot")
    lines.append("```")
    lines.append("")

    # --- paste blob
    lines.append("## Paste-blob for the fresh session")
    lines.append("")
    lines.append("> " + _paste_blob(state=state, title=title))
    lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _paste_blob(*, state: SprintState | None, title: str) -> str:
    base = f"Resume from `{title}` — latest HANDOFF in projects root."
    if state and state.goal:
        return f"{base} Sprint goal was: {state.goal}. Do not re-plan — start where the handoff ends."
    return f"{base} Do not re-plan — start where the handoff ends."


def generate_handoff_file(
    *,
    state: SprintState | None,
    settings,
    projects_root: Path,
    title: str | None = None,
    now: datetime | None = None,
) -> tuple[Path, str]:
    """Write HANDOFF_<YYYY-MM-DD_HHMM>.md under projects_root.

    Returns (written_path, body).
    """
    now = now or datetime.now(timezone.utc)
    resolved_title = title or (
        state.title if (state and state.title) else
        f"Operator sprint handoff {now.strftime('%Y-%m-%d %H:%M UTC')}"
    )
    body = render_handoff(
        state=state,
        settings=settings,
        projects_root=projects_root,
        title=resolved_title,
        now=now,
    )
    filename = f"HANDOFF_{_filename_timestamp(now)}.md"
    target = Path(projects_root) / filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(body, encoding="utf-8")
    return target, body


def newest_handoff(projects_root: Path) -> Path | None:
    """Return the newest HANDOFF_*.md in projects_root, by mtime."""
    try:
        candidates = [
            p for p in Path(projects_root).iterdir()
            if p.is_file() and p.name.startswith("HANDOFF_") and p.suffix == ".md"
        ]
    except OSError:
        return None
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def resume_text(projects_root: Path) -> str | None:
    newest = newest_handoff(projects_root)
    if newest is None:
        return None
    try:
        return newest.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
