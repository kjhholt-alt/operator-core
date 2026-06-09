# Morning Briefing — Setup

The `morning_briefing` recipe (v2) renders a single-page Palantir-themed
HTML dashboard at `<projects>/war-room/morning.html` every morning at
7am local time. Output also lands as `morning.md` next to it.

Design doc: `docs/MORNING_BRIEFING_V2_DESIGN.md`. Recipe code:
`recipes/morning_briefing.py`.

## What it shows

1. **Today's 3 things** — rule-based ranker; surfaces the top callouts
   from hot replies, ready-to-merge PRs, red projects, stale emitters,
   completed audits, signup spikes, cost spikes.
2. **Overnight replies** (last 24h, by classifier category)
3. **Portfolio health** (KPIs from `portfolio_health` IR — needs that
   recipe to have run)
4. **Open PRs** across tracked repos (via `gh pr list`)
5. **Waitlist signups** (last 24h, hosted-agents)
6. **AI Ops audit** state
7. **Pool Prospector** uncle-demo + leads
8. **Cost (24h)** rollup from `events-ndjson` cost stream
9. **Recent commits** (last 24h, tracked repos)
10. **Stale alerts** — status-spec emitters > 48h, BLOCKER docs

## Required env vars

| Var | Purpose | Default if unset |
|---|---|---|
| `OPERATOR_COST_NDJSON_PATH` | Where the "Cost (24h)" section reads `events-ndjson` cost events from | `~/.operator/data/events/cost.ndjson` |
| `OPERATOR_PROJECTS_DIR` | Roots `<projects>/war-room/`, `<projects>/<repo>/` | Walks `~/Desktop/Projects`, `~/Projects`, then cwd |

The recipe reads everything else (Discord webhooks, Supabase creds) via
the standard `.env` chain that other recipes use. Adapter setup lives in
`src/operator_core/integrations/` — see the main README for that.

### Optional / nice-to-have

- `DISCORD_PROJECTS_WEBHOOK_URL` — Discord channel the briefing posts
  to. The recipe sets `discord_channel = "projects"`. If no webhook is
  configured, the recipe logs `morning_briefing.no_discord_skip` and
  the HTML is still written.
- The recipe's `requires_clients` is empty, so the Discord adapter is
  only built when the daemon explicitly wires it. Plain `recipe run`
  (e.g. from `schtasks`) skips the Discord post unless the context is
  built with the discord adapter.

### Where to set them

On Windows, persist to your user environment so `schtasks`-launched
processes pick them up regardless of cwd:

```powershell
setx OPERATOR_COST_NDJSON_PATH "C:\Users\Kruz\.operator\data\events\cost.ndjson"
```

Or add them to the project `.env` (loaded by the daemon and the
Discord/Supabase adapters via `python-dotenv`):

```
OPERATOR_COST_NDJSON_PATH=C:\Users\Kruz\.operator\data\events\cost.ndjson
```

Both work; `setx` survives across project directories.

## Install / uninstall the scheduled task

The cron entry lives in `schedules/schedule.yaml`:

```yaml
- name: morning_briefing
  cron: "0 7 * * *"
  enabled: true
  notes: daily portfolio status to projects
```

### Install all scheduled recipes (39 entries)

```powershell
operator schedule install
# or:
py -m operator_core.cli schedule install
```

This registers every enabled `schedule.yaml` entry as
`operator-recipe-<name>` with the host scheduler (Windows `schtasks`,
macOS `launchctl`, Linux `systemd-timer`).

### Install just morning_briefing (manual)

If you want only this one task without registering the other 38, run
the equivalent `schtasks` command directly. This matches what the
installer would emit (verified via `schedule install --dry-run`):

```powershell
schtasks /Create /F /TN "operator-recipe-morning_briefing" `
  /TR "py -m operator_core.cli recipe run morning_briefing" `
  /SC DAILY /ST 07:00
```

### Verify

```powershell
schtasks /Query /TN "operator-recipe-morning_briefing"
operator schedule status         # drift report: schedule.yaml vs registered tasks
```

`Next Run Time` should be tomorrow at 7:00 AM local.

### Uninstall

```powershell
operator schedule uninstall      # removes every operator-recipe-* task
# or just this one:
schtasks /Delete /F /TN "operator-recipe-morning_briefing"
```

## Test it manually

```powershell
# Full pipeline (writes morning.html + morning.md, no Discord post unless
# the daemon-wired context provides a discord client):
py -m operator_core.cli recipe run morning_briefing

# Verify-only (skips query/analyze/format/post):
py -m operator_core.cli recipe run morning_briefing --dry-run
```

A successful run prints something like:

```
[recipe] morning_briefing -> ok (cost $0.0000, 7.10s)
```

## Output

| Path | What |
|---|---|
| `<projects>/war-room/morning.html` | Single-page HTML dashboard, Palantir theme |
| `<projects>/war-room/morning.md` | Markdown rendering of the same IR |
| `~/.operator/data/runs.ndjson` | `runs.started` + `runs.finished` events |
| `~/.operator/data/events/cost.ndjson` | Cost events (only written when cost > $0) |
| `~/.operator/data/status/morning_briefing.json` | status-spec component status |

Open the HTML directly in a browser:

```
file:///C:/Users/Kruz/Desktop/Projects/war-room/morning.html
```

## Discord ping behaviour

When the recipe runs inside the daemon (which wires the Discord adapter
into the context), `post()` builds a short embed listing the top 3
callouts plus a `file:///` link to the full HTML, posted to the
`projects` channel webhook (`DISCORD_PROJECTS_WEBHOOK_URL`).

When run from `schtasks` (which does NOT wire a discord adapter,
because `requires_clients = ()` is empty), the recipe logs
`morning_briefing.no_discord_skip` and the briefing is left on disk
only. To get the Discord post from the scheduled run, either:

1. Run via the daemon (`operator run`) instead of `schtasks` directly, or
2. Add `"discord"` to `requires_clients` on the recipe class (a code
   change — out of scope for this setup doc).

## Troubleshooting

**`morning.html` is stale** — check the task ran:

```powershell
schtasks /Query /FO LIST /V /TN "operator-recipe-morning_briefing" | findstr "Last"
```

`Last Result` should be `0`. Anything else, run it manually with the
command above and read the stderr.

**Section X says "ERROR / no data"** — sections are independent. The
HTML still renders; just the failing section gets a red callout.
Common causes:

- Portfolio health: run `recipe run portfolio_health` first to seed
  `war-room/portfolio-health.ir.json`.
- Open PRs: `gh auth status` — needs `gh` CLI logged in.
- Cost (24h): expected to read `$0.0000` until something records cost.

**Task runs but writes nothing** — check `OPERATOR_PROJECTS_DIR`. The
recipe writes to `<projects_dir>/war-room/`. If the env var isn't set
and the auto-detect picks a wrong root, you'll find a `war-room/`
folder somewhere unexpected.
