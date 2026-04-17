Reply-ledger audit fixes shipped on 2026-04-17.

Scope:
- Added best-effort Supabase mirror sync for `outreach_reply_threads` and
  `outreach_reply_messages` from the local SQLite reply ledger.
- Added `operator replies mark-ready` so READY is reachable from the CLI.
- Added `operator replies close` so CLOSED is reachable from the CLI.

Why:
- `/ops/replies` had a full-inbox reader but no daemon-side mirror writer.
- The reply state model exposed READY/CLOSED in code and UI but not in the
  operator-facing CLI.

Verification:
- `pytest tests/test_replies.py -q`
