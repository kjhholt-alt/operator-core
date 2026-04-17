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
