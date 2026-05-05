# Operator Core

> Self-hosted AI operator daemon for multi-project founders.

Operator is the background process that runs your portfolio while you build. Schedules Claude agents, watches deploy health, fires recipes from Discord, audits secrets, and keeps your status visible without you thinking about it.

**Status:** pre-alpha. Extracted from the monolith that's been running one founder's 15-project portfolio for months. Packaging as something you can install is the current sprint.

## Install

```bash
pip install operator-core
# or
pipx install operator-core
```

### Optional extras

| Extra      | Adds                                    | Why                                                                     |
|------------|-----------------------------------------|-------------------------------------------------------------------------|
| `discord`  | `discord.py>=2.3`                       | Slash bot + webhook posting                                             |
| `status`   | `rich>=13.0`                            | Terminal status renderer                                                |
| `specs`    | `status-spec>=1.0`, `events-ndjson>=0.1`| Canonical observability — see below                                     |
| `dev`      | `pytest`, `pytest-asyncio`, `rich`      | Test suite + CLI niceties                                               |

```bash
pip install 'operator-core[specs,discord]'
```

### What `[specs]` gives you

When `status-spec` and `events-ndjson` are installed, operator-core's
internal vendor shim (`operator_core._vendor.{status_spec,events_ndjson}`)
transparently delegates to the canonical packages. You get:

- **Schema-validated event streams** — every `runs` and `cost` event the
  recipes runtime emits is validated against the canonical
  `events-ndjson/v1` JSON Schemas before being appended.
- **Canonical status aggregate** — alongside the legacy
  `~/.operator/data/status.json`, a `status-spec/v1`-conformant document
  is emitted at `~/.operator/data/status-spec.json` with subsystems +
  counters that any cross-portfolio dashboard can read.
- **Backward compatibility** — without `[specs]`, an in-tree fallback
  shim provides the same surface so nothing breaks. Use
  `operator_core._vendor.events_ndjson.using_real_lib()` (and the
  status-spec twin) to check at runtime.

## Get started

```bash
operator init                    # creates ~/.operator/config.toml
$EDITOR ~/.operator/config.toml  # set projects_root + add your projects
operator run                     # starts the daemon
```

## What it does

- **Scheduled agents** — recipes fire on cron, results post to Discord
- **Watchdog** — flags stale status sections (jobs that should have run but didn't)
- **Portfolio brain** — one place that knows your whole project roster
- **Discord bot** — `!op status`, `!op morning`, `!op review prs`, etc.
- **Claude Code hooks** — pre/post tool-use guardrails, risk tiering, auto-merge gating
- **PR factory** — spin up worktrees, run agents, open PRs, triage CI
- **Observability** — `/ops` dashboard, Prometheus metrics, job ledger

## Config

Single file at `~/.operator/config.toml`. Hot-reloaded (no restart).

```toml
[user]
github = "your-handle"
projects_root = "~/Projects"

[daemon]
bind = "127.0.0.1"
port = 8765

[discord.channels]
projects = "DISCORD_PROJECTS_WEBHOOK_URL"
deploys = "DISCORD_DEPLOYS_WEBHOOK_URL"

[[projects]]
slug = "my-app"
path = "my-app"
repo = "you/my-app"
type = "nextjs"
autonomy_tier = "medium"

[projects.deploy]
provider = "vercel"
url = "https://my-app.vercel.app"

[projects.health]
path = "/"
expected_status = 200
```

See `docs/config.md` for the full reference.

## Status

Pre-alpha. Not yet published to PyPI. Waitlist: https://operator.dev

## License

MIT.
