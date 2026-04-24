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

## Get started

```bash
operator init                    # creates ~/.operator/config.toml
$EDITOR ~/.operator/config.toml  # set projects_root + add your projects
operator run                     # starts the daemon
operator leads report --window-hours 24
operator leads sync              # pull signups/intakes into the follow-up queue
operator leads list              # who needs attention next
operator leads digest            # daily signup-first follow-up briefing
operator tasks run lead-digest   # run the scheduled local digest job now
operator demand scoreboard       # rank products by current demand signal
```

## What it does

- **Scheduled agents** — recipes fire on cron, results post to Discord
- **Watchdog** — flags stale status sections (jobs that should have run but didn't)
- **Portfolio brain** — one place that knows your whole project roster
- **Discord bot** — `!op status`, `!op morning`, `!op review prs`, etc.
- **Claude Code hooks** — pre/post tool-use guardrails, risk tiering, auto-merge gating
- **PR factory** — spin up worktrees, run agents, open PRs, triage CI
- **Observability** — `/ops` dashboard, Prometheus metrics, job ledger
- **Lead ledger** - signup-first queue across products, with notes, statuses, and Discord digest

## Signup-first lead ledger

The Growth OS sprint starts with `operator leads`: one command surface for
seeing every signup, intake, audit unlock, and subscriber event before asking
anyone to pay.

```bash
operator leads report --window-hours 168
operator leads sync --window-hours 168 --dry-run
operator leads sync --window-hours 168
operator leads list --min-score 70 --limit 20
operator leads show lead_abc123
operator leads draft lead_abc123
operator leads mark lead_abc123 CONTACTED --note "Sent personal follow-up."
operator leads note lead_abc123 "Asked what workflow hurts most."
operator leads daily             # sync + write local status metrics
operator leads digest --post-discord
```

## Portfolio Demand OS

The Demand OS layer uses the lead ledger to decide what product and distribution
experiment deserves attention next.

```bash
operator demand scoreboard
operator demand health
operator demand experiments --limit 10
operator demand backlog --seed
operator demand experiment ai-ops-consulting-1 start "Active sprint lane."
operator demand journey lead_abc123
operator demand nightly --write --write-status
operator demand weekly --write --write-status
operator tasks run nightly-demand-plan
operator tasks run demand-review
```

The daemon also serves a local signup-first demand dashboard:

```bash
operator run --no-discord --no-scheduler --no-snapshot
# open http://127.0.0.1:8765/demand
```

For the shared Supabase table, apply `docs/operator-leads-schema.sql`, then run:

```bash
operator leads sync --mirror-supabase
```

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
