"""Operator CLI - `operator` command.

Subcommands:
    operator init           Bootstrap ~/.operator/config.toml from the template.
    operator run            Start the daemon (http + scheduler + snapshot + discord).
    operator snapshot       Publish one snapshot to Supabase immediately.
    operator config path    Print the resolved config path.
    operator config show    Print the effective loaded config.
    operator doctor         Validate config + runtime env; exit 0 if healthy.
    operator version        Print version.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

from . import __version__
from .settings import (
    ConfigError,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DATA_DIR,
    DEFAULT_WORKTREES_DIR,
    clear_cache,
    config_path,
    load_settings,
)


CONFIG_TEMPLATE = """\
# Operator Core config.
# Edit this file then run `operator doctor` to validate.
# Full reference: https://operator.dev/docs/config

[user]
# Your GitHub handle - used for PR automation, worktree naming, commit attribution.
github = "{github}"

# Where your projects live on disk. Each [[projects]] `path` below is
# resolved relative to this unless it's already absolute.
projects_root = "{projects_root}"


[daemon]
# Local HTTP hook surface. Stay on 127.0.0.1 unless you know what you're doing.
bind = "127.0.0.1"
port = 8765


[data]
# Where Operator stores state (sqlite, scheduler state, etc).
# Defaults to ~/.operator/data - usually fine as-is.
# dir = "~/.operator/data"


[discord.channels]
# Discord channels -> env var that holds the webhook URL.
# Set the env vars in your shell / .env, never in this file.
# Remove any channel you don't use; add more as needed.
projects = "DISCORD_PROJECTS_WEBHOOK_URL"
code_review = "DISCORD_CODE_REVIEW_WEBHOOK_URL"
deploys = "DISCORD_DEPLOYS_WEBHOOK_URL"
automations = "DISCORD_AUTOMATIONS_WEBHOOK_URL"
claude_chat = "DISCORD_WEBHOOK_URL"


# --- Projects ---------------------------------------------------------------
#
# Uncomment and fill in one block per project you want Operator to manage.
# Copy+paste this block to add more.
#
# [[projects]]
# slug = "my-app"                        # short unique id
# path = "my-app"                        # relative to projects_root, or absolute
# repo = "{github}/my-app"               # "owner/repo"
# type = "nextjs"                        # nextjs | python | go | docs | ...
# autonomy_tier = "medium"               # low | medium | high
# protected_patterns = []                # glob patterns requiring approval
# auto_merge = false                     # allow low-risk auto-merges
# checks = ["npm test", "npm run build"] # commands run before merge
#
# [projects.deploy]
# provider = "vercel"                    # vercel | railway | cloudflare | custom
# url = "https://my-app.vercel.app"
#
# [projects.health]
# path = "/"                             # health-check endpoint path
# expected_status = 200
"""


def _cmd_init(args: argparse.Namespace) -> int:
    target = Path(args.path) if args.path else config_path()
    target = Path(os.path.expandvars(os.path.expanduser(str(target)))).resolve()

    if target.exists() and not args.force:
        print(f"config already exists at {target}", file=sys.stderr)
        print("use --force to overwrite (your file will be backed up to *.bak)", file=sys.stderr)
        return 1

    if target.exists():
        backup = target.with_suffix(target.suffix + ".bak")
        shutil.copy2(target, backup)
        print(f"[backup] {target}  ->  {backup}")

    target.parent.mkdir(parents=True, exist_ok=True)
    home = Path.home()
    # Reasonable default for projects_root: ~/Projects on any OS.
    # User can edit before running the daemon. Always use forward slashes in
    # the written TOML so Windows paths don't collide with TOML escape parsing.
    default_projects_root = (home / "Projects").as_posix()
    github = os.environ.get("GITHUB_USER") or os.environ.get("USER") or "your-handle"
    target.write_text(
        CONFIG_TEMPLATE.format(
            github=github,
            projects_root=default_projects_root,
        ),
        encoding="utf-8",
    )

    DEFAULT_DATA_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_WORKTREES_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[created] {target}")
    print(f"[created] {DEFAULT_DATA_DIR}")
    print(f"[created] {DEFAULT_WORKTREES_DIR}")
    print()
    print("Next:")
    print(f"  1. edit {target}")
    print(f"     - set [user].github to your GitHub handle")
    print(f"     - set [user].projects_root to where your code lives")
    print(f"     - add [[projects]] blocks for anything you want Operator to watch")
    print(f"  2. run `operator doctor` to validate")
    print(f"  3. run `operator run` to start the daemon (coming soon)")
    return 0


def _cmd_config_path(args: argparse.Namespace) -> int:
    print(config_path())
    return 0


def _cmd_config_show(args: argparse.Namespace) -> int:
    try:
        s = load_settings(reload=True)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1

    print(f"config_path:       {s.config_path}")
    print(f"github_handle:     {s.github_handle}")
    print(f"projects_root:     {s.projects_root}")
    print(f"data_dir:          {s.data_dir}")
    print(f"worktrees_dir:     {s.worktrees_dir}")
    print(f"daemon:            {s.daemon.bind}:{s.daemon.port}")
    print(f"discord channels:  {len(s.discord_channels)}")
    for channel, env_var in sorted(s.discord_channels.items()):
        present = " (env set)" if os.environ.get(env_var) else " (env MISSING)"
        print(f"  - {channel:<12} -> {env_var}{present}")
    print(f"projects:          {len(s.projects)}")
    for p in s.projects:
        exists = "ok" if p.path.exists() else "MISSING"
        print(f"  - {p.slug:<20} {p.path} [{exists}]")
    return 0


def _cmd_doctor(args: argparse.Namespace) -> int:
    errors: list[str] = []
    warnings: list[str] = []

    target = config_path()
    print(f"config path: {target}")
    if not target.exists():
        errors.append(f"config file missing. Run `operator init`.")
    else:
        try:
            s = load_settings(reload=True)
        except ConfigError as exc:
            errors.append(f"config parse: {exc}")
            s = None

        if s is not None:
            print(f"github:        {s.github_handle}")
            print(f"projects_root: {s.projects_root}")
            if not s.projects_root.exists():
                warnings.append(
                    f"projects_root does not exist yet: {s.projects_root}"
                )

            print(f"data_dir:      {s.data_dir}")
            if not s.data_dir.exists():
                warnings.append(f"data_dir does not exist: {s.data_dir}")

            print(f"projects:      {len(s.projects)}")
            if not s.projects:
                warnings.append(
                    "no [[projects]] configured - Operator has nothing to watch."
                )
            for p in s.projects:
                if not p.path.exists():
                    warnings.append(
                        f"project {p.slug!r}: path does not exist: {p.path}"
                    )

            print(f"discord:       {len(s.discord_channels)} channels")
            for channel, env_var in sorted(s.discord_channels.items()):
                if not os.environ.get(env_var):
                    warnings.append(
                        f"discord channel {channel!r}: env var {env_var} is not set"
                    )

    print()
    if warnings:
        print(f"[warnings] {len(warnings)}")
        for w in warnings:
            print(f"  - {w}")
    if errors:
        print(f"[errors] {len(errors)}")
        for e in errors:
            print(f"  - {e}")
        print("doctor: FAIL")
        return 1

    if warnings:
        print("doctor: OK (with warnings)")
    else:
        print("doctor: OK")
    return 0


def _cmd_version(args: argparse.Namespace) -> int:
    print(f"operator-core {__version__}")
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    # Lazy import: keeps `operator init` / `operator doctor` fast and
    # avoids pulling in sqlite / http deps just to bootstrap a config.
    from . import daemon

    return daemon.run(
        host=args.host,
        port=args.port,
        no_discord=args.no_discord,
        no_scheduler=args.no_scheduler,
        no_snapshot=args.no_snapshot,
        once=args.once,
        snapshot_interval=args.snapshot_interval,
        log_level=args.log_level,
        log_file=args.log_file,
    )


def _cmd_snapshot(args: argparse.Namespace) -> int:
    from . import snapshot

    argv = ["dump"] if args.dump else ["publish"]
    return snapshot.main(argv)


# ---------------------------------------------------------------------------
# tasks
# ---------------------------------------------------------------------------


def _cmd_tasks_list(args: argparse.Namespace) -> int:
    from . import scheduler as sched_mod

    rows = sched_mod.list_all_tasks()
    if args.json:
        import json as _json
        print(_json.dumps(rows, indent=2))
        return 0

    if not rows:
        print("no tasks registered.")
        return 0

    # Tabular format: key | enabled | cadence | time | last_run | description
    key_w  = max(len(r["key"]) for r in rows)
    cad_w  = max(len(r["cadence"]) for r in rows)
    time_w = max(len(str(r["time"])) for r in rows)

    hdr = f"  {'KEY':<{key_w}}  {'ON':<3}  {'CADENCE':<{cad_w}}  {'TIME':<{time_w}}  {'LAST-RUN':<10}  DESCRIPTION"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for r in rows:
        on = "on " if r["enabled"] else "off"
        last = r["last_run"] or "-"
        print(
            f"  {r['key']:<{key_w}}  {on:<3}  {r['cadence']:<{cad_w}}  "
            f"{str(r['time']):<{time_w}}  {last:<10}  {r['description']}"
        )
    return 0


def _resolve_task(key: str):
    from . import scheduler as sched_mod

    t = sched_mod.find_task(key)
    if t is not None:
        return ("builtin", t)
    # custom schedule?
    for entry in sched_mod.list_schedules():
        if isinstance(entry, dict) and entry.get("name") == key:
            return ("custom", entry)
    return (None, None)


def _cmd_tasks_run(args: argparse.Namespace) -> int:
    kind, t = _resolve_task(args.key)
    if kind is None:
        print(f"no task with key {args.key!r}. Try `operator tasks list`.", file=sys.stderr)
        return 2

    try:
        s = load_settings(reload=True)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1

    from .runner import JobRunner
    from .store import JobStore

    store = JobStore(s.db_path)
    runner = JobRunner(store, settings=s)

    if kind == "builtin":
        action, prompt, project = t.action, t.prompt, t.project
    else:
        action = t.get("command", "unknown")
        prompt = ""
        project = None

    job = store.create_job(
        action,
        prompt=prompt,
        project=project,
        metadata={"schedule": args.key, "source": "cli"},
    )
    print(f"[run] job_id={job.id}  action={action}")
    try:
        runner.run(job.id)
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1
    print(f"[done] job_id={job.id}")
    return 0


def _cmd_tasks_enable(args: argparse.Namespace) -> int:
    from . import scheduler as sched_mod

    kind, _ = _resolve_task(args.key)
    if kind is None:
        print(f"no task with key {args.key!r}", file=sys.stderr)
        return 2
    changed = sched_mod.enable_task(args.key)
    print(f"{'enabled' if changed else 'already enabled'}: {args.key}")
    return 0


def _cmd_tasks_disable(args: argparse.Namespace) -> int:
    from . import scheduler as sched_mod

    kind, _ = _resolve_task(args.key)
    if kind is None:
        print(f"no task with key {args.key!r}", file=sys.stderr)
        return 2
    changed = sched_mod.disable_task(args.key)
    print(f"{'disabled' if changed else 'already disabled'}: {args.key}")
    return 0


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def _cmd_status(args: argparse.Namespace) -> int:
    from . import status_tui

    return status_tui.render(once=args.once, json_mode=args.json)


# ---------------------------------------------------------------------------
# sprint + handoff
# ---------------------------------------------------------------------------


def _try_load_settings():
    """Load settings, or return None on any error (so sprint tooling works
    even before `operator init`)."""
    try:
        return load_settings(reload=True)
    except ConfigError:
        return None


def _resolve_projects_root(settings) -> Path:
    if settings and getattr(settings, "projects_root", None):
        return Path(settings.projects_root)
    # Fallback: current working directory.
    return Path.cwd()


def _resolve_data_dir(settings) -> Path:
    if settings and getattr(settings, "data_dir", None):
        return Path(settings.data_dir)
    return Path.home() / ".operator" / "data"


def _cmd_sprint_start(args: argparse.Namespace) -> int:
    from . import sprint as sprint_mod

    settings = _try_load_settings()
    data_dir = _resolve_data_dir(settings)
    data_dir.mkdir(parents=True, exist_ok=True)

    goal = args.goal.strip() if args.goal else ""
    if not goal:
        print("operator sprint start: provide a goal in quotes", file=sys.stderr)
        return 2

    state, created = sprint_mod.start_sprint(
        goal, settings=settings, data_dir=data_dir, title=args.title
    )
    if args.json:
        import json as _json
        print(_json.dumps({
            "created": created,
            "state": state.to_dict(),
        }, indent=2))
        return 0

    if created:
        print(f"[sprint] started at {state.started_at_iso}")
    else:
        print(f"[sprint] already running since {state.started_at_iso}")
    print(f"  goal: {state.goal}")
    print(f"  tracked repos: {len(state.git_heads)}")
    for slug, sha in sorted(state.git_heads.items()):
        short = sha[:10] if sha else "-"
        branch = state.branches.get(slug, "?")
        print(f"    - {slug:<22} {branch:<18} {short}")
    return 0


def _cmd_sprint_status(args: argparse.Namespace) -> int:
    from . import sprint as sprint_mod

    settings = _try_load_settings()
    data_dir = _resolve_data_dir(settings)
    state = sprint_mod.load_state(data_dir)
    if state is None:
        if args.json:
            import json as _json
            print(_json.dumps({"active": False}, indent=2))
            return 0
        print("no active sprint. Start one with `operator sprint start \"<goal>\"`")
        return 0

    elapsed = sprint_mod.elapsed_minutes(state)
    rows = sprint_mod.status_rows(state, settings=settings)
    banner = sprint_mod.sweet_spot_banner(elapsed)

    if args.json:
        import json as _json
        print(_json.dumps({
            "active": True,
            "state": state.to_dict(),
            "elapsed_minutes": round(elapsed, 1),
            "rows": rows,
            "banner": banner,
        }, indent=2))
        return 0

    print(f"[sprint] goal: {state.goal}")
    print(f"  started: {state.started_at_iso}")
    print(f"  elapsed: {elapsed:.0f} min")
    if banner:
        print(f"  {banner}")
    if rows:
        print()
        slug_w = max(len(r["slug"]) for r in rows)
        print(f"  {'SLUG':<{slug_w}}  COMMITS  FILES  DIRTY  NOTE")
        for r in rows:
            dirty = "yes" if r["dirty"] else "no"
            note = r.get("note") or ""
            print(
                f"  {r['slug']:<{slug_w}}  "
                f"{r['commits']:>7}  {r['files']:>5}  {dirty:<5}  {note}"
            )
    return 0


def _cmd_sprint_resume(args: argparse.Namespace) -> int:
    from . import sprint as sprint_mod

    settings = _try_load_settings()
    projects_root = _resolve_projects_root(settings)
    text = sprint_mod.resume_text(projects_root)
    if text is None:
        print(
            f"no HANDOFF_*.md found in {projects_root}. "
            "Run `operator handoff` first.",
            file=sys.stderr,
        )
        return 1
    sys.stdout.write(text)
    if not text.endswith("\n"):
        sys.stdout.write("\n")
    return 0


def _cmd_handoff(args: argparse.Namespace) -> int:
    from . import sprint as sprint_mod

    settings = _try_load_settings()
    projects_root = _resolve_projects_root(settings)
    data_dir = _resolve_data_dir(settings)
    state = sprint_mod.load_state(data_dir)

    path, body = sprint_mod.generate_handoff_file(
        state=state,
        settings=settings,
        projects_root=projects_root,
        title=args.title,
    )
    print(f"[handoff] wrote {path}")

    if not args.no_discord:
        try:
            from .utils.discord import notify
            paste = sprint_mod._paste_blob(
                state=state,
                title=args.title or path.name,
            )
            footer = f"operator handoff | {path.name}"
            notify(
                channel="projects",
                title=f"Sprint handoff: {path.name}",
                body=paste,
                color="green",
                footer=footer,
            )
            print("[handoff] posted paste-blob to #projects")
        except Exception as exc:  # pragma: no cover - best-effort notify
            print(f"[handoff] discord post skipped: {exc}", file=sys.stderr)

    if args.clear:
        sprint_mod.clear_state(data_dir)
        print("[handoff] cleared current-sprint state")
    return 0


# ---------------------------------------------------------------------------
# demo briefing
# ---------------------------------------------------------------------------


def _replies_store():
    from .replies import ReplyStore

    settings = load_settings()
    return ReplyStore(settings.data_dir / "replies.sqlite3")


def _cmd_replies_list(args: argparse.Namespace) -> int:
    store = _replies_store()
    threads = store.list_threads(status=getattr(args, "status", None), limit=50)
    if not threads:
        print("no reply threads yet")
        return 0
    for t in threads:
        name = t.sender_name or t.sender_email
        subj = (t.subject or "(no subject)")[:60]
        print(
            f"  [{t.status:<8}] {t.thread_id}  {name[:24]:<24}  "
            f"{subj}"
        )
    summary = store.summary()
    print(
        f"\n  unread={summary.get('unread', 0)}  "
        f"drafting={summary.get('DRAFTING', 0)}  "
        f"ready={summary.get('READY', 0)}  "
        f"sent_7d={summary.get('sent_7d', 0)}"
    )
    return 0


def _cmd_replies_show(args: argparse.Namespace) -> int:
    store = _replies_store()
    try:
        thread = store.get_thread(args.thread_id)
    except KeyError:
        print(f"unknown thread_id: {args.thread_id}", file=sys.stderr)
        return 1
    print(f"thread: {thread.thread_id}")
    print(f"status: {thread.status}")
    print(f"sender: {thread.sender_name or thread.sender_email} <{thread.sender_email}>")
    print(f"subject: {thread.subject}")
    print(f"first: {thread.first_received_at}")
    print(f"last:  {thread.last_activity_at}")
    if thread.dd_notes_md:
        print("\n--- dd notes ---")
        print(thread.dd_notes_md)
    for m in store.list_messages(thread.thread_id):
        ts = m.received_at or m.sent_at or m.created_at
        print(f"\n--- {m.direction.upper()} {ts} ---")
        print(m.body_md or "(empty)")
    return 0


def _cmd_replies_add_incoming(args: argparse.Namespace) -> int:
    store = _replies_store()
    body = args.body or (sys.stdin.read() if not sys.stdin.isatty() else "")
    thread = store.upsert_thread_for_incoming(
        sender_email=args.sender,
        sender_name=args.name,
        subject=args.subject,
        body_md=body,
    )
    print(
        f"[replies] thread {thread.thread_id} "
        f"status={thread.status} sender={thread.sender_email}"
    )
    return 0


def _cmd_replies_save_draft(args: argparse.Namespace) -> int:
    store = _replies_store()
    body = args.body or (sys.stdin.read() if not sys.stdin.isatty() else "")
    if not body.strip():
        print("error: no draft body (pass --body or pipe on stdin)", file=sys.stderr)
        return 1
    thread = store.save_draft(
        args.thread_id,
        body_md=body,
        subject=args.subject,
        dd_notes_md=args.dd,
    )
    print(f"[replies] draft saved; status now {thread.status}")
    return 0


def _cmd_replies_mark_sent(args: argparse.Namespace) -> int:
    store = _replies_store()
    thread = store.mark_sent(args.thread_id)
    print(f"[replies] thread {thread.thread_id} marked SENT")
    return 0


def _cmd_replies_mark_ready(args: argparse.Namespace) -> int:
    store = _replies_store()
    thread = store.mark_ready(args.thread_id)
    print(f"[replies] thread {thread.thread_id} marked READY")
    return 0


def _cmd_replies_close(args: argparse.Namespace) -> int:
    store = _replies_store()
    thread = store.close_thread(args.thread_id)
    print(f"[replies] thread {thread.thread_id} marked CLOSED")
    return 0


def _cmd_demo_briefing(args: argparse.Namespace) -> int:
    from . import demo as demo_mod

    settings = _try_load_settings()
    return demo_mod.run_briefing(settings)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="operator",
        description="Self-hosted AI operator daemon.",
    )
    sub = p.add_subparsers(dest="command", required=False)

    # init
    p_init = sub.add_parser("init", help="Bootstrap ~/.operator/config.toml")
    p_init.add_argument("--path", help="Override config path (default: ~/.operator/config.toml)")
    p_init.add_argument("--force", action="store_true", help="Overwrite existing config (backup .bak first)")
    p_init.set_defaults(func=_cmd_init)

    # config
    p_config = sub.add_parser("config", help="Inspect the active config")
    config_sub = p_config.add_subparsers(dest="config_command", required=True)
    p_config_path = config_sub.add_parser("path", help="Print the config file path")
    p_config_path.set_defaults(func=_cmd_config_path)
    p_config_show = config_sub.add_parser("show", help="Print the effective loaded config")
    p_config_show.set_defaults(func=_cmd_config_show)

    # doctor
    p_doctor = sub.add_parser("doctor", help="Validate config + runtime env")
    p_doctor.set_defaults(func=_cmd_doctor)

    # run
    p_run = sub.add_parser("run", help="Start the operator daemon")
    p_run.add_argument("--host", default=None, help="HTTP bind address (overrides config)")
    p_run.add_argument("--port", type=int, default=None, help="HTTP port (overrides config)")
    p_run.add_argument("--no-discord", action="store_true", help="Skip the Discord bot")
    p_run.add_argument("--no-scheduler", action="store_true", help="Skip the cron scheduler")
    p_run.add_argument("--no-snapshot", action="store_true", help="Skip the /kruz snapshot publisher")
    p_run.add_argument("--once", action="store_true", help="Start, publish one snapshot, exit")
    p_run.add_argument(
        "--snapshot-interval", type=int, default=1800,
        help="Snapshot cadence in seconds (default 1800 = 30 min)",
    )
    p_run.add_argument("--log-level", default="info", help="debug|info|warn|error")
    p_run.add_argument("--log-file", type=Path, default=None, help="Log file path (default: data_dir/operator.log)")
    p_run.set_defaults(func=_cmd_run)

    # snapshot
    p_snap = sub.add_parser("snapshot", help="Publish one snapshot immediately")
    p_snap.add_argument("--dump", action="store_true", help="Print JSON only, don't publish")
    p_snap.set_defaults(func=_cmd_snapshot)

    # tasks
    p_tasks = sub.add_parser("tasks", help="List / run / enable / disable scheduled tasks")
    tasks_sub = p_tasks.add_subparsers(dest="tasks_command", required=True)

    p_tasks_list = tasks_sub.add_parser("list", help="Show all scheduled tasks")
    p_tasks_list.add_argument("--json", action="store_true", help="Emit JSON instead of a table")
    p_tasks_list.set_defaults(func=_cmd_tasks_list)

    p_tasks_run = tasks_sub.add_parser("run", help="Run a task immediately, out of cadence")
    p_tasks_run.add_argument("key", help="Task key from `operator tasks list`")
    p_tasks_run.set_defaults(func=_cmd_tasks_run)

    p_tasks_enable = tasks_sub.add_parser("enable", help="Enable a task (resume cadence)")
    p_tasks_enable.add_argument("key", help="Task key")
    p_tasks_enable.set_defaults(func=_cmd_tasks_enable)

    p_tasks_disable = tasks_sub.add_parser("disable", help="Disable a task (stop cadence)")
    p_tasks_disable.add_argument("key", help="Task key")
    p_tasks_disable.set_defaults(func=_cmd_tasks_disable)

    # status
    p_status = sub.add_parser("status", help="Terminal dashboard (daemon + tasks + snapshot)")
    p_status.add_argument("--once", action="store_true", help="Print once and exit (no live refresh)")
    p_status.add_argument("--json", action="store_true", help="Emit JSON instead of a rendered table")
    p_status.set_defaults(func=_cmd_status)

    # sprint
    p_sprint = sub.add_parser(
        "sprint",
        help="Record and track a focused work session (start/status/resume)",
    )
    sprint_sub = p_sprint.add_subparsers(dest="sprint_command", required=True)

    p_sprint_start = sprint_sub.add_parser(
        "start", help="Record current git heads and start the sprint clock"
    )
    p_sprint_start.add_argument(
        "goal", help="One-line description of what this sprint is trying to do"
    )
    p_sprint_start.add_argument("--title", default=None, help="Optional sprint title")
    p_sprint_start.add_argument("--json", action="store_true")
    p_sprint_start.set_defaults(func=_cmd_sprint_start)

    p_sprint_status = sprint_sub.add_parser(
        "status", help="Elapsed time + commits since sprint start"
    )
    p_sprint_status.add_argument("--json", action="store_true")
    p_sprint_status.set_defaults(func=_cmd_sprint_status)

    p_sprint_resume = sprint_sub.add_parser(
        "resume", help="Print the newest HANDOFF_*.md from projects_root"
    )
    p_sprint_resume.set_defaults(func=_cmd_sprint_resume)

    # handoff
    p_handoff = sub.add_parser(
        "handoff",
        help="Write HANDOFF_<ts>.md + post paste-blob to Discord",
    )
    p_handoff.add_argument(
        "--title", default=None, help="Optional handoff title (default auto-generated)"
    )
    p_handoff.add_argument(
        "--no-discord",
        action="store_true",
        help="Skip posting the paste-blob to the #projects webhook",
    )
    p_handoff.add_argument(
        "--clear",
        action="store_true",
        help="Clear current-sprint.json after writing the handoff",
    )
    p_handoff.set_defaults(func=_cmd_handoff)

    # replies — outreach reply ledger
    p_replies = sub.add_parser(
        "replies", help="Inspect + manage inbound outreach reply threads"
    )
    replies_sub = p_replies.add_subparsers(dest="replies_command", required=True)

    p_rl = replies_sub.add_parser("list", help="List reply threads (most recent first)")
    p_rl.add_argument("--status", help="Filter: NEW / DRAFTING / READY / SENT / CLOSED")
    p_rl.set_defaults(func=_cmd_replies_list)

    p_rs = replies_sub.add_parser("show", help="Print one thread with every message")
    p_rs.add_argument("thread_id")
    p_rs.set_defaults(func=_cmd_replies_show)

    p_ra = replies_sub.add_parser(
        "add-incoming", help="Record an inbound reply (pass --body or pipe on stdin)"
    )
    p_ra.add_argument("--sender", required=True, help="Sender email")
    p_ra.add_argument("--name", help="Sender display name")
    p_ra.add_argument("--subject", required=True, help="Subject line")
    p_ra.add_argument("--body", help="Body markdown (omit to read stdin)")
    p_ra.set_defaults(func=_cmd_replies_add_incoming)

    p_rd = replies_sub.add_parser(
        "save-draft", help="Save an outbound draft for a thread (DRAFTING status)"
    )
    p_rd.add_argument("thread_id")
    p_rd.add_argument("--body", help="Draft body markdown (omit to read stdin)")
    p_rd.add_argument("--subject", help="Optional subject override")
    p_rd.add_argument("--dd", help="Due-diligence notes (markdown)")
    p_rd.set_defaults(func=_cmd_replies_save_draft)

    p_rm = replies_sub.add_parser(
        "mark-sent", help="Mark the latest pending outbound as sent"
    )
    p_rm.add_argument("thread_id")
    p_rm.set_defaults(func=_cmd_replies_mark_sent)

    p_rr = replies_sub.add_parser(
        "mark-ready", help="Mark a drafted thread as READY for human send"
    )
    p_rr.add_argument("thread_id")
    p_rr.set_defaults(func=_cmd_replies_mark_ready)

    p_rc = replies_sub.add_parser(
        "close", help="Close a thread (negative reply, resolved, or done)"
    )
    p_rc.add_argument("thread_id")
    p_rc.set_defaults(func=_cmd_replies_close)

    # demo
    p_demo = sub.add_parser(
        "demo", help="Cinematic terminal briefings (demo-safe, <5s runtime)"
    )
    demo_sub = p_demo.add_subparsers(dest="demo_command", required=True)
    p_demo_briefing = demo_sub.add_parser(
        "briefing", help="Live portfolio briefing — header + heartbeat + ticker"
    )
    p_demo_briefing.set_defaults(func=_cmd_demo_briefing)

    # version
    p_version = sub.add_parser("version", help="Print version")
    p_version.set_defaults(func=_cmd_version)

    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "func", None):
        parser.print_help()
        return 0
    try:
        return args.func(args)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
