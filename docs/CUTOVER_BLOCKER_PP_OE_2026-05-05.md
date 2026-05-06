# Cut-over blocker: prospector-pro and outreach-engine NOT flipped

Branch: `feat/cutover-orchestration`. Companion to
`CUTOVER_STATE_2026-05-05.md` and `CUTOVER_PLAN_2026-05-05.md`.

## Decision

**Do NOT flip prospector-pro or outreach-engine to live in this
sprint.** Both stay on the legacy send path with no shadow wiring.

## Why

Per `CUTOVER_PLAN_2026-05-05.md`, the cut-over decision rule is:

> If after one daily cycle of ai-ops shadow runs:
>   - shadow_blocks for ai-ops in the dashboard is 0, OR
>   - every shadow_block has a corresponding legacy block
>
> then PR to flip ai-ops live. After 24 hours of live ai-ops with no
> kill-switch trips, repeat the exercise for prospector-pro, then
> outreach-engine.

We have not yet completed the **first** prerequisite: ai-ops shadow
itself only became functional minutes ago (the
`outreach_common.shadow` module + the vendored shim that lets the
adapter find it landed in this branch + outreach-common's
`feat/shadow-send`). No shadow-data daily cycle has elapsed, and we
have not even confirmed that the new wiring lights up the dashboard
in the production environment after deploy.

PP and outreach-engine are also one step further behind: their
adapter shims have a `shadow_wrap` skeleton (mirroring ai-ops
pre-cut-over) but their `sender.py` files have **no call sites** for
that wrapper. Wiring them up is a separate, mechanical task that
should land only after the ai-ops Phase A run produces a clean
dashboard.

## What would unblock the next flip

1. Merge ai-ops `feat/cutover-ai-ops` PR. Set
   `OUTREACH_COMMON_SHADOW_MODE=true` in ai-ops production env (no
   other env-var changes needed).
2. Run a daily ai-ops cycle. Confirm shadow envelopes show up at
   `~/.outreach-common/events.ndjson` or in Supabase
   `outreach_events`.
3. Run `operator outreach audit-dashboard`. Confirm
   `shadow_sends > 0` and `divergences == 0`.
4. PR a follow-up branch `feat/cutover-pp-shadow` that adds the
   identical `shadow_wrap` call to `prospector-pro/outreach/main.py`'s
   send loop and its vendored `outreach_common/` shim. Repeat the
   verification.
5. Same for `feat/cutover-outreach-engine-shadow`.

Only after both products have observed clean shadow cycles do we
consider flipping any of them to live (`OUTREACH_COMMON_AUDIT_ONLY=false`).

## Why this is the safe call

- The `_InMemoryRateLimiter` in
  `outreach-common/src/outreach_common/sender.py` is process-local.
  Flipping all three products live simultaneously would not coordinate
  per-domain rate limits across processes. That's a known design gap
  the gate's author flagged for a future sprint.
- The 2026-04-23 All Around Town incident makes any aggressive
  outreach-pipeline change a board-level event in this codebase.
  "Flip three live in one PR" is the exact pattern that produced
  that incident. The kill switch added in
  `feat/cutover-ai-ops` is single-product scope; coordinated
  rollback across three products under load is untested.
- Sibling agents are simultaneously doing hosted-agents launch and
  war-room migration. If any of those changes touches Discord
  webhook routing, the divergence-alert path needs re-validation
  before more products go live.

## Forward path

This blocker doc + the `cut_over_promoter` recipe (now wired into
operator-core) make this safer next time. The recipe runs daily,
inspects the dashboard, and posts a `flip_live` recommendation to
`#projects` only when shadow data is clean. When a recommendation
lands, the operator opens the next `feat/cutover-*` PR by hand.

Effectively: instead of three branches in flight today, we have one
shipped (ai-ops shadow), one tooling shipped (operator-core dashboard
+ recipe), and a structured queue for the rest.
