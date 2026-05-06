# Morning Briefing v2 — design

Single HTML page rendered to `war-room/morning.html` daily at 7am via cron.
Aggregates every surface Kruz needs to start his day. Dogfoods every lib
shipped 2026-05-05: status-spec, events-ndjson, templated-dashboards
v0.2.0, recipes framework, outreach-common gate digest, portfolio_health.

Existing `recipes/morning_briefing.py` (~80 lines, posts a Discord bullet
list of project folders) is the surface to extend, not replace.

## 10 sections, in priority order

| # | Section | Source | Fallback when empty |
|---|---|---|---|
| 1 | "Today's 3 things" banner | rule-based ranker over sections 2-10 | "No surfaced items today — clear runway" |
| 2 | Overnight outreach replies + classifier outcomes | `replies.py` ledger + `reply_classifier.py` outcomes (last 24h) | "0 replies overnight" |
| 3 | Portfolio health rollup | read `war-room/portfolio-health.ir.json`, slice Overview section | "portfolio_health not run yet — schedule it" |
| 4 | Open PRs + CI state | `gh pr list` per tracked repo | "0 open PRs" |
| 5 | hosted-agents waitlist signups since last brief | Supabase `op_waitlist` table + `op_waitlists` (per `revenue.collect_waitlists`) | "0 new signups" |
| 6 | AI Ops audit pipeline state | `~/.operator/data/status/ai-ops-consulting.json` (status-spec) + recent `ai_ops_audit` events | "no audits in 24h" |
| 7 | Pool prospector lead pipeline + uncle-demo state | `pool-prospector` Supabase pool_leads table + check examples/uncle_demo/<recent>/ exists | "no recent activity" |
| 8 | Cost rollup last 24h | events-ndjson `cost` stream tail | "$0.00 / no LLM activity" |
| 9 | Recent commits across portfolio | `git log --since="24h ago"` per tracked repo | "0 commits in 24h" |
| 10 | Stale alerts (status >48h, blockers, dirty workspaces) | reuse portfolio_health stale logic + scan for BLOCKER docs in tracked repos | "nothing stale" |

## "Today's 3 things" rule-based ranker

Scoring (high-to-low priority):

1. Hot-leads escalation captured by reply classifier (interested / hostile categories) — score 100
2. PR open >24h with green CI awaiting merge — score 80
3. status-spec health=red on any project — score 70
4. status-spec stale (no update >48h) on any tracked project — score 50
5. Audit pipeline emitted `audit_completed` in last 24h — score 40 (review the deliverable)
6. Waitlist signup spike (>=3 new in 24h on any product) — score 35
7. Cost spike (>3× rolling 7d avg) — score 30
8. Recent commit on a project with no STATUS.md update — score 20

Take top 3 with reason text. If fewer than 3 items score, fill with "No surfaced items in category X" placeholders. Never fewer than 1, never more than 3.

## IR shape

```json
{
  "title": "Morning briefing — <date>",
  "subtitle": "<n> things to look at first",
  "theme": "palantir",
  "sections": [
    {
      "title": "Today's 3 things",
      "layout": "grid",
      "components": [{"type": "callout", "tone": "warn|good|bad", "kicker": "...", "title": "...", "body": "..."}, ...]
    },
    {
      "title": "Overnight replies",
      "components": [{"type": "table", "rows": [["sender", "category", "score", "preview"], ...]}]
    },
    {
      "title": "Portfolio health",
      "components": [{"type": "kpi_tile", ...}, ...]  // copied from portfolio-health.ir.json
    },
    // ... sections 4-10
  ]
}
```

## Recipe lifecycle

- **verify**: `war-room/portfolio-health.ir.json` exists OR portfolio_health recipe is enabled in schedule.yaml; status-spec dir exists; gh CLI authenticated.
- **query**: 10 `_gather_*` async functions, gather in parallel via `asyncio.gather`. Each returns a `dict` with section-shaped data. Failures are caught per-section; section gets a `"_error"` flag, render renders a small error band rather than crashing the whole briefing.
- **analyze**: `_rank_today(sections)` → list[3] of callout payloads.
- **format**: build IR from sections + ranker output → `Dashboard` builder → render HTML and Markdown via `templated_dashboards` lib → write `war-room/morning.html` and `war-room/morning.md`.
- **post**: Discord embed with title "Morning briefing — <date>", body = top 3 things + count summary, link = `file://...morning.html`. Posts to `#projects` channel.
- **log_cost**: minimal — no LLM call in this recipe; ranker is pure rules.

## Schedule

- Cron: `0 7 * * *` (daily 7am local). Existing `morning_briefing` runs at 6am — shift to 7am to land AFTER `portfolio_health` (which runs 8am). Actually: `portfolio_health` is 8am, briefing is 7am — move briefing to 9am? Or run briefing AFTER portfolio_health.
- **Resolution:** schedule briefing at 7am but verify() falls back gracefully if portfolio-health.ir.json is older than 24h or missing. Briefing will degrade (section 3 shows "portfolio_health not run yet") rather than block. Operator can adjust schedule order later.

## Out of scope

- Don't reinvent portfolio_health — read its IR json output
- Don't reinvent reply classifier — read its ledger output
- Don't run real Anthropic API — ranker is rule-based, no LLM
- Don't post to multiple channels — single Discord embed to #projects
- Don't paginate — one HTML page, scrollable
- Don't auto-resolve PRs — surface them, Kruz decides

## Tests

`tests/test_morning_briefing.py`:
- `test_aggregator_handles_missing_portfolio_health` — no IR json on disk, section 3 renders fallback
- `test_aggregator_handles_supabase_offline` — collect_waitlists raises, section 5 renders error band
- `test_ranker_picks_hot_lead_first` — synthetic interested reply scores top
- `test_ranker_fills_to_3_even_with_few_items` — empty sections produce 1+ placeholders
- `test_ranker_caps_at_3` — many high-priority items, returns exactly 3
- `test_format_produces_valid_ir` — IR validates against templated-dashboards schema
- `test_format_writes_html_and_md` — both files written, non-zero size
- `test_recipe_lifecycle_runs_clean` — verify→query→analyze→format→post all succeed in dry-run
- `test_post_includes_morning_html_link` — Discord embed body has the file:// link
