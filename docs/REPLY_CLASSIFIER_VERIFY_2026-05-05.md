# Reply classifier verification — 2026-05-05

Cut-over checkpoint: confirm the canonical reply classifier behaves
correctly across all 5 categories and that the side-effects (auto
suppress, #hot-leads ping) are wired.

## What was verified

Source: `outreach-common/src/outreach_common/replies.py` +
`outreach-common/src/outreach_common/reply_classifier.py`.

Tests live at `outreach-common/tests/test_replies.py` and
`outreach-common/tests/test_reply_classifier.py`. Both pass at HEAD.

## Canonical 5-class coverage

| Category | Pure-classifier test | handle_reply test | Suppression auto-add | #hot-leads ping |
|---|---|---|---|---|
| opt_out | test_canonical_opt_out | test_opt_out_reply_auto_suppresses | yes | no (silent suppress) |
| oof | test_canonical_oof + 3 detection variants | test_oof_reply_logs_no_discord | no | no |
| interested | test_canonical_interested | test_interested_reply_posts_discord_and_logs_event | no | yes (green embed) |
| hostile | test_canonical_hostile | test_hostile_reply_posts_discord_and_logs_event (NEW) | no | yes (red embed) |
| neutral | test_canonical_neutral | test_neutral_reply_logs_only | no | no |

The hostile path was newly verified 2026-05-05; previously had
classifier-only coverage but no `handle_reply` integration test. Now
every category has both layers covered.

## #hot-leads webhook resolution

`outreach_common.replies._post_discord("hot-leads", payload)` resolves the
webhook URL by reading these env vars in order:

1. `DISCORD_HOT_LEADS_WEBHOOK_URL` (preferred, dedicated channel)
2. `DISCORD_PROJECTS_WEBHOOK_URL` (fallback)

Pinned in test `test_post_discord_uses_hot_leads_env_var_first`.

## Suppression auto-add on opt_out

`replies.handle_reply` calls
`outreach_common.suppression.add_suppression(email, reason="manual_unsub", notes="reply_classifier:opt_out:<reason>")`
on every opt_out classification. Verified by
`test_opt_out_reply_auto_suppresses`.

## Reply ingestion path coverage

The canonical `replies.handle_reply` is **not yet called** by the
production reply scanner. The legacy
`outreach-common/src/outreach_common/reply_ingest.py` still uses its
own 2-class auto/human classifier and writes to lead-table directly,
not through `handle_reply`. That gap is in scope for a separate PR --
mentioned in the cut-over state doc as a deferred item.

What IS shipped today:

- the canonical 5-class classifier
- the canonical handle_reply orchestration with Discord + suppression
  side-effects
- 100% test coverage on both
- the `replies` Operator-core CLI sub-tree (`operator replies list`,
  etc.) which uses `outreach_common.replies` for inbound messages
  routed through the operator daemon's webhook ingest

What is NOT shipped today (deferred):

- legacy `reply_ingest.py` migration to call `handle_reply`
  (currently calls `mark_replied` directly, missing the suppression +
  hot-leads side-effects)

## Synthetic-load verification

Each test runs the classifier with a hand-crafted reply payload that
matches the canonical shape for its category, then asserts on the
side-effects via monkeypatched HTTP/Supabase clients. No real Gmail
account, no real Discord webhook, and no real Supabase row was
touched in these test runs.

## Acceptance for the cut-over brief

- [x] Reply classifier verified against 5 canonical shapes
- [x] #hot-leads ping path confirmed (synthetic verification)
- [x] suppression auto-add on opt_out confirmed
- [ ] Production-traffic verification deferred -- requires a real
      reply or a synthetic Gmail test account, and is a separate
      sprint from cut-over orchestration
