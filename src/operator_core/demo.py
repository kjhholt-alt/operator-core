"""Cinematic demo briefing — `operator demo briefing`.

Runs in <5 seconds and is demo-safe: every data source falls back to a
`[no data]` badge instead of crashing or prompting.

Output is plain stdout (ANSI color optional via $NO_COLOR / isatty). No
Rich dependency — Kruz demos straight from a fresh terminal, and the
goal is that even a freshly cloned repo with no creds shows something
recognizable on screen.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import Settings


# ---------------------------------------------------------------------------
# ANSI coloring (auto-disabled when stdout is not a tty or NO_COLOR is set)
# ---------------------------------------------------------------------------


def _supports_color() -> bool:
    if os.environ.get("NO_COLOR"):
        return False
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return False
    return True


_COLOR = _supports_color()


def _c(code: str, text: str) -> str:
    if not _COLOR:
        return text
    return f"\x1b[{code}m{text}\x1b[0m"


def _cyan(t): return _c("36", t)
def _green(t): return _c("32", t)
def _yellow(t): return _c("33", t)
def _red(t): return _c("31", t)
def _dim(t): return _c("2", t)
def _bold(t): return _c("1", t)


# ---------------------------------------------------------------------------
# Header art
# ---------------------------------------------------------------------------


HEADER_ART = r"""
   ____                      _
  / __ \____  ___  _________(_)___ _/ /_____  _____
 / / / / __ \/ _ \/ ___/ __  / __ `/ __/ __ \/ ___/
/ /_/ / /_/ /  __/ /  / /_/ / /_/ / /_/ /_/ / /
\____/ .___/\___/_/   \__,_/\__,_/\__/\____/_/
    /_/   one daemon. every project. every morning.
"""


def _print_header() -> None:
    for line in HEADER_ART.strip("\n").splitlines():
        print(_cyan(line))
    now = datetime.now(timezone.utc)
    pip = _green("[*]")
    print(
        f"  {pip} {_bold('OPERATOR LIVE BRIEFING')}   "
        f"{_dim('UTC ' + now.strftime('%Y-%m-%d %H:%M:%S'))}"
    )
    print()


# ---------------------------------------------------------------------------
# Git helpers — timeout-bounded, safe on missing repos
# ---------------------------------------------------------------------------


def _git(args: list[str], cwd: Path, timeout: float = 1.5) -> str | None:
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


def _commits_last_7d(path: Path) -> int:
    out = _git(
        ["rev-list", "--count", "--since=7.days.ago", "HEAD"], path
    )
    try:
        return int(out or "0")
    except ValueError:
        return 0


def _last_commit(path: Path) -> str | None:
    return _git(
        ["log", "-1", "--format=%cr %s", "--no-decorate"], path
    )


def _recent_commits_oneline(path: Path, limit: int = 5) -> list[str]:
    out = _git(
        [
            "log",
            f"-{limit}",
            "--no-decorate",
            "--format=%h %s",
            "--since=7.days.ago",
        ],
        path,
    )
    if not out:
        return []
    return [line for line in out.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Project heartbeat table
# ---------------------------------------------------------------------------


def _print_project_heartbeat(settings: Settings | None) -> None:
    print(_bold("  PROJECT HEARTBEAT") + _dim("   last 7 days"))
    print(_dim("  " + "-" * 72))
    if settings is None or not settings.projects:
        print(_dim("  [no data] - no projects configured"))
        print()
        return

    rows: list[tuple[str, int, str, str]] = []
    for p in settings.projects:
        path = Path(str(p.path))
        if not path.exists():
            rows.append((p.slug, 0, "-", _dim("missing on disk")))
            continue
        commits = _commits_last_7d(path)
        last = _last_commit(path) or "no commits"
        deploy = (getattr(p.deploy, "url", None) if p.deploy else None) or ""
        deploy_short = deploy.replace("https://", "").replace("http://", "")
        rows.append((p.slug, commits, deploy_short, last[:44]))

    slug_w = max(len(r[0]) for r in rows)
    for slug, commits, deploy, last in rows:
        if commits >= 10:
            pip = _green("[*]")
        elif commits >= 1:
            pip = _yellow("[*]")
        else:
            pip = _dim("[ ]")
        commits_txt = _bold(f"{commits:>3}") if commits > 0 else _dim("  0")
        deploy_col = _dim(deploy or "-")
        print(
            f"  {pip} {slug:<{slug_w}}  {commits_txt} commits  "
            f"{deploy_col:<36}  {_dim(last)}"
        )
    print()


# ---------------------------------------------------------------------------
# Recent portfolio commits — last 5 most interesting
# ---------------------------------------------------------------------------


def _print_recent_commits(settings: Settings | None, limit: int = 5) -> None:
    print(_bold("  RECENT WORK") + _dim("   across portfolio, last 7 days"))
    print(_dim("  " + "-" * 72))
    if settings is None or not settings.projects:
        print(_dim("  [no data]"))
        print()
        return

    collected: list[tuple[str, str]] = []
    for p in settings.projects:
        path = Path(str(p.path))
        if not path.exists():
            continue
        for line in _recent_commits_oneline(path, limit=limit):
            collected.append((p.slug, line))
        if len(collected) >= limit * 3:
            break

    if not collected:
        print(_dim("  [no data] - no commits in the last 7 days"))
        print()
        return

    # Just show the first `limit` entries for now.
    for slug, commit in collected[:limit]:
        parts = commit.split(" ", 1)
        sha = parts[0] if parts else ""
        msg = parts[1] if len(parts) > 1 else ""
        print(f"  {_cyan(sha):<8}  {_dim(slug):<24}  {msg}")
    print()


# ---------------------------------------------------------------------------
# Deploys ticker — from Supabase if available, else [no data]
# ---------------------------------------------------------------------------


def _fetch_recent_deploys(limit: int = 3) -> list[dict[str, Any]]:
    """Read the last N deploy events from Supabase. Silent on any failure."""
    url = os.environ.get("SUPABASE_URL") or os.environ.get(
        "NEXT_PUBLIC_SUPABASE_URL"
    )
    key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
        or os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
    )
    if not url or not key:
        return []
    try:
        from urllib.request import Request, urlopen
        from urllib.error import URLError

        endpoint = (
            f"{url.rstrip('/')}/rest/v1/deploy_events"
            f"?select=project,status,created_at,url"
            f"&order=created_at.desc&limit={limit}"
        )
        req = Request(
            endpoint,
            headers={
                "apikey": key,
                "Authorization": f"Bearer {key}",
                "Accept": "application/json",
            },
        )
        with urlopen(req, timeout=1.5) as resp:
            import json as _json
            data = _json.loads(resp.read().decode("utf-8"))
        if isinstance(data, list):
            return data
    except (URLError, OSError, ValueError, TimeoutError, Exception):
        return []
    return []


def _print_deploys_ticker(limit: int = 3) -> None:
    print(
        _bold("  DEPLOYS TICKER") + _dim(f"   last {limit} events from Supabase")
    )
    print(_dim("  " + "-" * 72))
    events = _fetch_recent_deploys(limit=limit)
    if not events:
        print(_dim("  [no data] - deploy_events table unreachable or empty"))
        print()
        return

    for e in events:
        project = e.get("project") or "?"
        status = (e.get("status") or "?").lower()
        when = e.get("created_at", "?")
        pip = (
            _green("[*]") if status in ("succeeded", "ready", "ok")
            else _red("[x]") if status in ("error", "failed", "canceled")
            else _yellow("[~]")
        )
        print(f"  {pip} {project:<24}  {status:<12}  {_dim(when)}")
    print()


# ---------------------------------------------------------------------------
# Footer — short "what to say next"
# ---------------------------------------------------------------------------


def _print_footer() -> None:
    print(_dim("  " + "-" * 72))
    print(
        f"  {_cyan('>')} open {_bold('https://operator.buildkit.store/kruz')} "
        "for the live broadcast"
    )
    print(
        f"  {_cyan('>')} {_bold('operator handoff')} writes the session "
        "doc and posts the paste-blob"
    )
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_briefing(settings: Settings | None) -> int:
    """Render the full demo briefing. Returns 0; the briefing never fails."""
    _print_header()
    _print_project_heartbeat(settings)
    _print_recent_commits(settings)
    _print_deploys_ticker()
    _print_footer()
    return 0
