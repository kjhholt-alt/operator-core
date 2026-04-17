"""Terminal dashboard for Operator.

Invoked by `operator status`. Shows daemon health, scheduled tasks, last
snapshot, recent jobs. Rich-based when `rich` is importable; falls back
to plain ASCII otherwise so the command always works.

  pip install operator-core[status]    # pulls rich
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .settings import ConfigError, Settings, load_settings


# --------------------------------------------------------------------------
# Data gathering — same inputs the snapshot publisher uses
# --------------------------------------------------------------------------


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _daemon_status(settings: Settings) -> dict[str, Any]:
    status = _read_json(Path(str(settings.status_path)))
    daemon = status.get("daemon") or {}
    return {
        "pid": daemon.get("pid"),
        "started_at": daemon.get("started_at"),
        "uptime_sec": daemon.get("uptime_sec"),
        "alive": _pid_alive(daemon.get("pid")),
    }


def _pid_alive(pid: Any) -> bool:
    """Cross-platform PID liveness check.

    POSIX: `os.kill(pid, 0)` is the canonical no-op probe.
    Windows: `os.kill(pid, 0)` raises WinError 87 ("parameter is
    incorrect") instead of signaling liveness, so we call the Win32
    `OpenProcess` + `GetExitCodeProcess` pair via ctypes.
    """
    if not isinstance(pid, int) or pid <= 0:
        return False

    if os.name == "nt":
        try:
            import ctypes

            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            STILL_ACTIVE = 259
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
            )
            if not handle:
                return False
            try:
                exit_code = ctypes.c_ulong()
                ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
                if not ok:
                    return False
                return exit_code.value == STILL_ACTIVE
            finally:
                kernel32.CloseHandle(handle)
        except Exception:
            return False

    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _recent_jobs(settings: Settings, limit: int = 10) -> list[dict[str, Any]]:
    try:
        from .store import JobStore

        store = JobStore(settings.db_path)
        # JobStore doesn't expose recent() consistently across versions —
        # fall back to SQL.
        import sqlite3

        conn = sqlite3.connect(str(settings.db_path))
        try:
            cur = conn.execute(
                "SELECT id, action, status, created_at FROM jobs "
                "ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
            rows = [
                {"id": r[0], "action": r[1], "status": r[2], "created_at": r[3]}
                for r in cur.fetchall()
            ]
        finally:
            conn.close()
        return rows
    except Exception:
        return []


def _snapshot_summary() -> dict[str, Any]:
    # Best effort — the snapshot table is read-only public.
    url = os.environ.get("SUPABASE_URL") or os.environ.get("OPERATOR_SNAPSHOT_SUPABASE_URL")
    key = (
        os.environ.get("NEXT_PUBLIC_SUPABASE_ANON_KEY")
        or os.environ.get("SUPABASE_ANON_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("SUPABASE_KEY")
    )
    if not (url and key):
        return {}
    try:
        import requests

        r = requests.get(
            f"{url.rstrip('/')}/rest/v1/operator_snapshots",
            params={
                "select": "published_at,payload",
                "node": "eq.kruz",
                "order": "published_at.desc",
                "limit": 1,
            },
            headers={"apikey": key, "Authorization": f"Bearer {key}"},
            timeout=5,
        )
        if r.status_code != 200:
            return {}
        rows = r.json()
        if not rows:
            return {}
        return rows[0]
    except Exception:
        return {}


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def render(*, once: bool = True, json_mode: bool = False) -> int:
    try:
        settings = load_settings(reload=True)
    except ConfigError as exc:
        print(f"config error: {exc}")
        return 1

    payload = _collect(settings)

    if json_mode:
        print(json.dumps(payload, indent=2, default=str))
        return 0

    try:
        _render_rich(payload)
    except ImportError:
        _render_plain(payload)
    return 0


def _collect(settings: Settings) -> dict[str, Any]:
    from . import scheduler as sched_mod

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "config_path": str(settings.config_path),
        "github_handle": settings.github_handle,
        "projects_root": str(settings.projects_root),
        "projects_configured": len(settings.projects),
        "daemon": _daemon_status(settings),
        "tasks": sched_mod.list_all_tasks(),
        "jobs_recent": _recent_jobs(settings, 10),
        "snapshot": _snapshot_summary(),
    }


def _render_plain(payload: dict[str, Any]) -> None:
    def line(c="-", n=80):
        print(c * n)

    line("=")
    print(f"OPERATOR STATUS    generated {payload['generated_at']}")
    line("=")

    d = payload["daemon"]
    alive = "UP" if d["alive"] else "DOWN"
    print(f"daemon:    {alive}   pid={d['pid'] or '-'}   started={d['started_at'] or '-'}")
    print(f"config:    {payload['config_path']}")
    print(f"github:    {payload['github_handle']}")
    print(f"projects:  {payload['projects_configured']} configured")
    line()

    print("TASKS")
    for t in payload["tasks"]:
        on = "on " if t["enabled"] else "off"
        last = t["last_run"] or "-"
        print(
            f"  [{on}] {t['key']:<20} {t['cadence']:<10} {t['time']:<10} "
            f"last-run={last}   {t['description']}"
        )
    line()

    print("RECENT JOBS")
    for j in payload["jobs_recent"]:
        print(f"  {j['created_at']:<24}  {j['status']:<10}  {j['action']:<20}  {j['id']}")
    if not payload["jobs_recent"]:
        print("  (none)")
    line()

    snap = payload["snapshot"]
    if snap:
        pub = snap.get("published_at", "-")
        summary = (snap.get("payload") or {}).get("summary", {}) or {}
        print(f"SNAPSHOT   published_at={pub}")
        print(f"           projects={summary.get('projects','-')}  "
              f"jobs_24h={summary.get('jobs_24h','-')}  "
              f"cost_24h=${summary.get('cost_24h_usd','-')}")
    else:
        print("SNAPSHOT   (no remote data — SUPABASE_URL / key unset)")
    line("=")


def _render_rich(payload: dict[str, Any]) -> None:
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table
    from rich.text import Text

    console = Console()

    d = payload["daemon"]
    alive = d["alive"]
    header = Text()
    header.append("OPERATOR", style="bold cyan")
    header.append("  /  ", style="dim")
    header.append("STATUS", style="bold white")
    header.append("    ", style="dim")
    header.append(f"generated {payload['generated_at']}", style="dim")
    console.print(header)

    daemon_tbl = Table.grid(padding=(0, 2))
    daemon_tbl.add_column(justify="right", style="dim")
    daemon_tbl.add_column()
    daemon_tbl.add_row(
        "daemon",
        Text(" UP " if alive else "DOWN", style="black on green" if alive else "white on red"),
    )
    daemon_tbl.add_row("pid", str(d["pid"] or "-"))
    daemon_tbl.add_row("started", str(d["started_at"] or "-"))
    daemon_tbl.add_row("config", payload["config_path"])
    daemon_tbl.add_row("github", payload["github_handle"])
    daemon_tbl.add_row("projects", str(payload["projects_configured"]))
    console.print(Panel(daemon_tbl, title="DAEMON", border_style="cyan"))

    tasks_tbl = Table(show_header=True, header_style="bold cyan", expand=True)
    tasks_tbl.add_column("on",       width=3)
    tasks_tbl.add_column("key")
    tasks_tbl.add_column("cadence",  width=10)
    tasks_tbl.add_column("time",     width=10)
    tasks_tbl.add_column("last-run", width=12)
    tasks_tbl.add_column("description")
    for t in payload["tasks"]:
        on_style = "bold green" if t["enabled"] else "dim"
        tasks_tbl.add_row(
            Text("on" if t["enabled"] else "off", style=on_style),
            t["key"],
            t["cadence"],
            str(t["time"]),
            str(t["last_run"] or "-"),
            t["description"],
        )
    console.print(Panel(tasks_tbl, title="TASKS", border_style="cyan"))

    jobs_tbl = Table(show_header=True, header_style="bold cyan", expand=True)
    jobs_tbl.add_column("when")
    jobs_tbl.add_column("status", width=10)
    jobs_tbl.add_column("action")
    jobs_tbl.add_column("id", overflow="crop")
    if payload["jobs_recent"]:
        for j in payload["jobs_recent"]:
            s = (j["status"] or "").lower()
            style = {"complete": "green", "error": "red", "running": "yellow"}.get(s, "")
            jobs_tbl.add_row(
                str(j["created_at"]),
                Text(str(j["status"]), style=style),
                str(j["action"]),
                str(j["id"]),
            )
    else:
        jobs_tbl.add_row("-", "-", "(no recent jobs)", "-")
    console.print(Panel(jobs_tbl, title="RECENT JOBS", border_style="cyan"))

    snap = payload["snapshot"]
    if snap:
        summary = (snap.get("payload") or {}).get("summary", {}) or {}
        snap_tbl = Table.grid(padding=(0, 2))
        snap_tbl.add_column(justify="right", style="dim")
        snap_tbl.add_column()
        snap_tbl.add_row("published_at", str(snap.get("published_at", "-")))
        snap_tbl.add_row("projects",     str(summary.get("projects", "-")))
        snap_tbl.add_row("jobs_24h",     str(summary.get("jobs_24h", "-")))
        snap_tbl.add_row("cost_24h",     f"${summary.get('cost_24h_usd', '-')}")
        console.print(Panel(snap_tbl, title="LATEST SNAPSHOT", border_style="cyan"))
    else:
        console.print(Panel(
            Text("no remote data — SUPABASE_URL / key unset", style="dim"),
            title="LATEST SNAPSHOT",
            border_style="cyan",
        ))
