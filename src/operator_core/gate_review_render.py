"""Discord-friendly text renderers for gate-review actions.

Handlers in `discord_bot.py` reply directly with whatever these return.
Output is plain Markdown (Discord renders ``` fences and **bold**), no
embeds — keeps the wiring testable without touching discord.py.
"""
from __future__ import annotations

from typing import Optional

from . import gate_review


_VALID_STATUSES = (
    "approved_gate",
    "approved_legacy",
    "fix_gate",
    "fix_legacy",
    "suppressed",
)


def _ingest_first(verbose: bool = False) -> str:
    """Tell the user how to populate the queue when it's empty."""
    msg = (
        "No pending review items.\n\n"
        "If you've turned on `OUTREACH_COMMON_SHADOW_MODE=true` and have "
        "audit events, run:\n"
        "```\noperator outreach gate-review ingest\n```\n"
        "Otherwise see `outreach-common/CUTOVER.md` to start collecting."
    )
    return msg


def render_next(product: Optional[str] = None) -> str:
    """Format the next pending review item as a Discord card.

    If `product` is provided, filter to that product. Returns a help-text
    response when nothing is pending.
    """
    items = gate_review.list_pending(product, limit=1)
    if not items:
        if product:
            return f"No pending items for `{product}`. Try `!op gate-review` for any product."
        return _ingest_first()

    item = items[0]
    summary = gate_review.triage_summary()
    counts_line = ""
    for s in summary:
        if s.product == item.product:
            counts_line = (
                f"\n_Queue for `{s.product}`: {s.pending} pending, "
                f"{s.triaged} triaged of {s.total} total ({s.triaged_pct:.0f}%)._"
            )
            break

    bn = item.business_name or "(no name)"
    label = item.gate_block_label or "(no label)"
    legacy = item.legacy_block_reason or "(no legacy reason)"
    seen = (
        f"first seen {item.first_seen_ts}, last {item.last_seen_ts}, "
        f"hits {item.hit_count}"
    )

    body = (
        f"**Gate disagreement #{item.id}** -- `{item.product}`\n"
        f"> Business: **{bn}**\n"
        f"> Agreement: `{item.agreement}`\n"
        f"> Gate said: `{label}`\n"
        f"> Legacy said: `{legacy}`\n"
        f"> {seen}\n\n"
        f"Resolve with one of:\n"
        f"```\n"
        f"!op gate-resolve {item.id} approved_gate    [note]\n"
        f"!op gate-resolve {item.id} approved_legacy  [note]\n"
        f"!op gate-resolve {item.id} fix_gate         [note]\n"
        f"!op gate-resolve {item.id} fix_legacy       [note]\n"
        f"!op gate-resolve {item.id} suppressed       [note]\n"
        f"```"
        f"{counts_line}"
    )
    return body


def render_resolve(item_id: int,
                   status: str,
                   *,
                   note: Optional[str] = None,
                   resolved_by: str = "discord") -> str:
    """Apply a resolution and return the confirmation reply."""
    if not item_id or item_id < 1:
        return f":x: Bad item id `{item_id}`."
    if status not in _VALID_STATUSES:
        return (
            f":x: Unknown status `{status}`.\n"
            f"Valid: {', '.join('`'+s+'`' for s in _VALID_STATUSES)}"
        )
    try:
        item = gate_review.resolve(
            item_id, status,
            note=note or None,
            resolved_by=resolved_by,
        )
    except ValueError as exc:
        return f":x: {exc}"

    summary_line = ""
    for s in gate_review.triage_summary():
        if s.product == item.product:
            summary_line = (
                f"\n_Queue for `{s.product}` now: {s.pending} pending, "
                f"{s.triaged}/{s.total} triaged ({s.triaged_pct:.0f}%)._"
            )
            break

    note_line = f" (note: _{item.resolution_note}_)" if item.resolution_note else ""
    return (
        f":white_check_mark: Resolved #{item.id} `{item.product}` -> "
        f"**{item.status}**{note_line}{summary_line}"
    )
