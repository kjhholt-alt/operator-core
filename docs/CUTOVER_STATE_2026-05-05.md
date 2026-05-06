# Outreach Sender-Gate cut-over state — 2026-05-05

Run by the cut-over orchestration agent on the
`feat/cutover-orchestration` branch in `operator-core`.

## TL;DR

- The Sender Gate canonical implementation is shipped and tested
  (`outreach-common`, 106/106 passing).
- The three product adapters (`outreach_common_adapter.py` in
  `ai-ops-consulting`, `prospector-pro`, `outreach-engine`) are present
  and import cleanly.
- **No product currently calls those adapters.** `audit_send`,
  `send_via_gate`, `is_audit_active`, and `is_enabled` have **zero
  call-sites** outside the adapter file in any product sender.
- Result: there is **no shadow data**. The
  `~/.outreach-common/events.ndjson` fallback does not exist on disk;
  Supabase `outreach_events` has not seen a non-test write from any
  product.
- Therefore the brief's premise ("promote shadow -> live") is one phase
  early. The work below promotes pre-shadow -> shadow first, with the
  live-flip prepared but not pulled.

## What I read

| File | Purpose |
|---|---|
| `outreach-common/src/outreach_common/sender.py` | The canonical 9-step gate. Identical code path in shadow + live; only the final provider call differs (`provider_send_fn` is injected as a no-op stub in audit mode). |
| `outreach-common/src/outreach_common/events.py` | Append-only event log. Writes to Supabase `outreach_events` first, falls back to NDJSON. The audit-dashboard reads the NDJSON. |
| `outreach-common/src/outreach_common/reply_classifier.py` | 5-class classifier (opt_out, oof, interested, hostile, neutral). Already implemented. |
| `outreach-common/src/outreach_common/reply_ingest.py` | Older 2-class (auto/human) classifier. Posts every human reply to `#projects` via the registry. Auto-suppress on opt_out is **not** wired here -- the path goes straight to `mark_replied`. |
| `ai-ops-consulting/outreach/outreach_common_adapter.py` | Shim for ai-ops. `is_enabled() == ROUTE_VIA_OUTREACH_COMMON && !OUTREACH_COMMON_AUDIT_ONLY`. |
| `prospector-pro/outreach/outreach_common_adapter.py` | Mirror of the above. |
| `outreach-engine/outreach_common_adapter.py` | Mirror. |
| `ai-ops-consulting/outreach/sender.py` | Legacy sender. Calls `send_via_gmail` or `_send_via_resend` directly. **Does not import the adapter.** |
| `outreach-engine/sender.py` | Same -- adapter not imported. |

## Dashboard snapshot (baseline)

```
operator outreach audit-dashboard
```

```
Outreach Sender-Gate audit dashboard
  generated_at : 2026-05-06T00:01:37.714423Z
  events_path  : C:\Users\Kruz\.outreach-common\events.ndjson
  total_events : 0

  (no send events recorded yet)
```

The events log file does not exist on the box.

## Per-product status

| Product | Adapter shim | Adapter wired into sender? | Shadow events | Live via gate? |
|---|---|---|---|---|
| ai-ops-consulting | yes | no | 0 | no |
| prospector-pro | yes | no | 0 | no |
| outreach-engine | yes | no | 0 | no |

## Gate identity in shadow vs live

Per `sender.py`, the gate is exactly the same code path in both modes.
`audit_send` constructs a `Sender` with
`provider_send_fn=lambda **kw: (True, "audit_noop")`; `send_via_gate`
constructs one with the default dispatch. Steps 1-6 (schema, CAN-SPAM,
suppression, network scrub, rate limit, unsubscribe mint) run
identically. Step 7 differs only in whether a real provider HTTP
request goes out. Steps 8-9 (event append + status hook) run identically.

That means once shadow is wired, divergence between shadow and live can
only come from:
- different `provider_send_fn` outcomes (only meaningful on live)
- a code change to one branch that doesn't ship to the other (which the
  gate forbids by design)

## Correct flip ordering for THIS state

Because we have no shadow data, the brief's
"ai-ops first -> PP -> outreach-engine" order is sound but the meaning
of the first flip changes.

- **Step 1 (this branch):** wire `audit_send` into all three product
  senders so shadow data starts flowing. Zero behavior change for the
  user; only side-effect is one extra event-log row per send.
- **Step 2 (next iteration):** observe one full send cycle, run
  `operator outreach audit-dashboard` and confirm shadow_blocks track
  what the legacy senders would also have blocked. If they don't,
  STOP and reconcile.
- **Step 3 (`feat/cutover-ai-ops` -- this branch):** flip ai-ops to
  live by adding `OUTREACH_COMMON_AUDIT_ONLY=false` to the docs as a
  staged change, gated by a new kill-switch env var.

The ai-ops PR opened from this branch contains:
1. Adapter shim wired into `ai-ops-consulting/outreach/sender.py` so
   shadow runs alongside legacy.
2. New env var `OUTREACH_LIVE_KILL_SWITCH` -- when set to `1`, forces
   shadow regardless of any other flag. Belt-and-braces revert.
3. New divergence guard test in `outreach-common/tests/`.
4. Documentation update.

ai-ops is **not flipped to live** in this PR; the live flag is
documented and ready, behind the new kill-switch, awaiting a follow-up
PR after shadow data confirms the gate is identical.

## Why not flip live now

Two reasons:
1. **No data.** Nothing has ever run through the canonical gate against
   real lead rows. Cutting over without one observed parallel cycle is
   the exact pattern that produced the All Around Town incident.
2. **Single-process rate limiter.** The `_InMemoryRateLimiter` in
   `outreach-common/src/outreach_common/sender.py` is process-local. If
   ai-ops' cron and outreach-engine's heartbeat both use the live gate
   simultaneously, the per-domain cap fires per-process, not globally.
   This is fine in shadow (no real send) but is a real concern at live.
   That coordination is in scope for a separate sprint.

## Hand-off

- Branch `feat/cutover-orchestration` (operator-core) ships the
  audit-dashboard CLI + this state doc.
- Branch `feat/cutover-ai-ops` (ai-ops-consulting) wires the shadow
  shim, adds the kill switch, adds the divergence guard test, and
  documents the live flag without flipping it.
- Reply classifier verification is in
  `feat/reply-classifier-verify` (operator-core).
