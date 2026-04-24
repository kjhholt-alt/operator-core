# Sprint: Revenue Cockpit + Campaign Factory

**Started:** 2026-04-23 evening (overnight build)
**Owner:** Claude (autonomous), reviewing with Kruz at milestones via Discord

## Why now

Kruz has 5 SaaS products + several internal tools, but no single morning view of:
- Who replied / needs follow-up
- Which campaigns are sending / blocked
- Which projects have dirty repos / failed deploys / stale sprint items
- What today's highest-revenue-proximity action is per project

He's been running a different command per product to check state. The morning briefing exists but it's flat — every project gets equal weight, no ranking, no per-product action recommendation.

**Result:** revenue work is reactive (he sees a Discord ping → he acts). What's missing is the proactive "here are 5 things in priority order, do them today."

## In scope (this sprint)

### Part A — Revenue Cockpit (`operator_core.revenue`)

CLI: `operator revenue [--post-discord]`

Reads:
1. **Per-project STATUS.md** — current sprint items, blockers (already on disk for: ax02-pl27-worklog, ax02-sl27-pilot, game-forge, hearth, municipal-crm, outdoor-crm, pc-bottleneck-analyzer, pl-engine, pool-prospector, portfolio, warcouncil, website-factory). SaaS products without STATUS.md fall back to git activity + DB stats.
2. **AO Leads (Supabase ao_leads)** — pending touches today, replies in last 24h, recent bounces/blocks, validated-but-unpitched count.
3. **Open PRs across all repos** — `gh pr list` per project. Stale PR (>3 days no commit) flagged.
4. **Deploy health** — already in operator-core snapshot (`deploy_health.py`).
5. **Git working-tree state** — dirty trees, unpushed branches, branches behind origin.
6. **Project waitlists** — pp_waitlist, vr_waitlist, db_waitlist, ao_waitlist (via Supabase).

Outputs:
1. **Per-project ranked actions (1-5):** Each project gets up to 5 actions ranked by `revenue_proximity` score. Examples: "Reply to Wayne Wright (DealBrain)" > "Top up GC personalization batch" > "Fix CI on Prospector".
2. **Cross-product summary at top:** today's revenue actions in priority order.
3. **Blockers section:** dirty repos, failed deploys, stale PRs, stuck campaigns.

Discord post: morning at 7am via existing `OperatorScript-BriefingMorning` task (or new `OperatorScript-RevenueCockpit`).

### Part B — Campaign Factory (`outreach_common.campaign`)

`Campaign` class wraps existing primitives:
- `outreach_common.suppression` — drop bounced/complained
- `outreach_common.budget_gate` — daily cap enforcement
- `outreach_common.email_filter` + `email_verify` + `smtp_probe` — validation
- `outreach_common.subject_variants` — A/B
- `outreach_common.pitch_dispatcher` — per-lead Opus pitch (optional)
- `outreach_common.smtp_sender` — Gmail SMTP send
- `outreach_common.reply_ingest` — bidirectional reply capture (existing)

Each product (AI Ops, DealBrain, Prospector Pro, etc.) defines a campaign config:
```python
Campaign(
    name="ao_trades_v1",
    source_filter=["ao_trades_hvac", "ao_trades_plumbing", ...],
    pitch_strategy="dispatcher",  # or "template" or "generic"
    daily_cap=12,                  # honored alongside budget_gate
    validation_required=True,      # only valid+catchall sendable
    discord_channel="projects",
    suppression_check=True,
)
```

Migrate AI Ops's `cmd_send` to use `Campaign` as proof point. DealBrain + Prospector Pro can adopt later when convenient — not blocking.

### Part C — Watchdog tail (small)

Add to operator-core snapshot:
- `dirty_trees` — list of repos with uncommitted changes
- `unpushed_branches` — branches ahead of origin
- `stale_prs` — PRs with no activity > 3 days

Already-existing watchdog config will pick these up if added to `watchdog_expectations.json`.

## Out of scope (deferred)

- **#2 Cross-Product Lead Ledger** — coupling risk too high. The pp_warm bridge already covers the only actual cross-product flow.
- **#4 Demo Asset Generator** — useful but not revenue-blocking.
- **#5 Weekly Kill/Double-Down Review** — write a markdown template, do it as a meeting.
- **#6 No-Key Demo Mode Standard** — codify the existing pattern, don't build central infra.

## Build order (overnight)

1. ✅ This spec doc
2. Scaffolding: `revenue.py` + `campaign.py` with type signatures
3. Implement revenue collectors (STATUS.md reader, gh PR reader, git state reader, AO leads reader)
4. Implement revenue scorer (rank actions per project)
5. Implement Discord embed renderer
6. CLI command + scheduler hook
7. Implement Campaign class
8. Migrate AI Ops `cmd_send` to use Campaign (dry-run only — sends still paused)
9. Watchdog tail additions
10. Smoke test — run `operator revenue --post-discord` and verify Discord output makes sense
11. Discord ping at each milestone

## Discord update cadence

Per `feedback_proactive_updates.md`: ping after every commit. Use channel `1485421359113703505` (#claude-chat).
