# Outreach Sender-Gate cut-over plan — 2026-05-05

## Flip order

1. **ai-ops-consulting** — first.
2. **prospector-pro** — second.
3. **outreach-engine** — third.

This matches the brief's default order. ai-ops has the lowest send
volume (12/day cap, low-touch broker outreach), prospector-pro is
middle (10/day cap, growing volume since the campaign-factory shipped
2026-04-24), and outreach-engine is highest (20/day cap, oldest pipeline
with the most complex template + scheduling state). Promoting in that
order means a regression in the canonical gate fires on the smallest
blast radius first.

## Phases (per product)

For each product:

- **Phase 0 -> Phase A (shadow):** wire `audit_send()` into the sender
  alongside the legacy send. Set `ROUTE_VIA_OUTREACH_COMMON=true`,
  `OUTREACH_COMMON_AUDIT_ONLY=true`. Zero behavior change for the
  recipient; one extra event row per send for the dashboard.
- **Phase A -> Phase B (compare):** observe at least one full daily
  cycle. Run `operator outreach audit-dashboard` and confirm
  `shadow_blocks` matches what the legacy filter+verify+suppression
  layer would also have blocked. Investigate every divergence.
- **Phase B -> Phase C (live):** flip `OUTREACH_COMMON_AUDIT_ONLY=false`.
  The canonical gate becomes the only send path. The kill switch
  (`OUTREACH_LIVE_KILL_SWITCH=1`) must remain available so a single env
  var change reverts to shadow.

## What this PR ships (ai-ops, branch feat/cutover-ai-ops)

- Phase 0 -> Phase A wiring for ai-ops only.
- The kill switch env var, plumbed through the adapter so flipping it
  forces shadow even if the live flag is on.
- The divergence guard test in `outreach-common`. Run on every CI.
- Documentation, including the live-flip recipe (NOT yet pulled).

## What this PR does NOT ship

- Live cut-over. ai-ops, PP, and outreach-engine all stay on legacy
  send paths after this PR is merged. Shadow runs alongside.
- PP and outreach-engine sender wiring. The same adapter shape applies
  to those repos but their wiring lands in their own `feat/cutover-*`
  branches when shadow data confirms the gate is identical.

## Decision rule for proceeding

If after one daily cycle of ai-ops shadow runs:

- `shadow_blocks` for ai-ops in the dashboard is 0, OR
- every shadow_block has a corresponding legacy block (network scrub
  matches a manual filter, suppression matches the existing
  `is_suppressed` call, etc.)

then PR to flip ai-ops live (`OUTREACH_COMMON_AUDIT_ONLY=false`).
After 24 hours of live ai-ops with no kill-switch trips, repeat the
exercise for prospector-pro, then outreach-engine.

If divergence shows up that the dashboard cannot explain, write a
blocker doc and do NOT flip the rest.
