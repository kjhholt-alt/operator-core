"""Auto-classifier for the Sender Gate review queue.

Pre-resolves obvious ``would_block_new`` items as ``approved_gate`` so
the operator only sees ambiguous items. Conservative by design: any
classifier output is just a state transition in the same SQLite that
the operator already trusts; nothing about a misclassified item is
unrecoverable (just re-resolve it).

The classifier rules (in priority order; first match wins):

1. **network_scrub recurrence** -- gate label starts with
   ``network_scrub:business_name:`` AND legacy reason is null AND the
   same business_name has been seen >= ``min_hits`` times. The
   business is on the canonical block list and legacy missed it. This
   is the high-confidence path: 90%+ of would_block_new disagreements
   we've seen so far are this pattern.

2. **tld guard** -- gate label starts with ``tld:`` AND legacy reason
   is null AND hit_count >= ``min_hits``. Bad TLDs (.invalid, .test)
   are deterministic; if the gate flags one and legacy doesn't, the
   gate is right.

Anything else stays pending for human review.

Auto-resolution stamps:
  resolved_by = "auto-classifier:<rule_name>"
  resolution_note = "auto: <reason summary>"
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import gate_review

logger = logging.getLogger("operator.gate_review_classifier")


# --- defaults / config ------------------------------------------------------

DEFAULT_MIN_HITS = 2  # require 2+ confirmations before auto-resolving


@dataclass
class ClassifierResult:
    inspected: int
    auto_resolved: int
    rules_fired: dict[str, int]
    item_ids_resolved: list[int]


# --- per-rule predicates ----------------------------------------------------

def _is_network_scrub_recurrence(item: gate_review.ReviewItem, *, min_hits: int) -> Optional[str]:
    """Return a note string if rule fires, else None."""
    if item.agreement != "would_block_new":
        return None
    if item.hit_count < min_hits:
        return None
    if item.legacy_block_reason:  # legacy had its own reason -- ambiguous
        return None
    label = item.gate_block_label or ""
    if not label.startswith("network_scrub:business_name:"):
        return None
    name = label.split(":", 2)[2] if label.count(":") >= 2 else "(unknown)"
    return f"auto: network_scrub recurrence for {name!r} (hit_count={item.hit_count})"


def _is_tld_guard(item: gate_review.ReviewItem, *, min_hits: int) -> Optional[str]:
    if item.agreement != "would_block_new":
        return None
    if item.hit_count < min_hits:
        return None
    if item.legacy_block_reason:
        return None
    label = item.gate_block_label or ""
    if not label.startswith("tld:"):
        return None
    return f"auto: tld guard fired (label={label!r}, hit_count={item.hit_count})"


# Order matters: more specific rules first.
_RULES = (
    ("network_scrub_recurrence", _is_network_scrub_recurrence),
    ("tld_guard", _is_tld_guard),
)


# --- main entry point -------------------------------------------------------

def classify_pending(
    *,
    min_hits: int = DEFAULT_MIN_HITS,
    db_path: Optional[Path] = None,
    dry_run: bool = False,
    limit: int = 500,
) -> ClassifierResult:
    """Walk pending items, auto-resolve the obvious ones.

    ``dry_run`` returns the would-be-resolved counts without writing.
    """
    pending = gate_review.list_pending(limit=limit, db_path=db_path)
    rules_fired: dict[str, int] = {name: 0 for name, _ in _RULES}
    resolved_ids: list[int] = []

    for item in pending:
        for rule_name, predicate in _RULES:
            note = predicate(item, min_hits=min_hits)
            if note is None:
                continue
            rules_fired[rule_name] += 1
            if dry_run:
                logger.debug(
                    "classifier.dry_run.would_resolve",
                    extra={"item_id": item.id, "rule": rule_name},
                )
            else:
                try:
                    gate_review.resolve(
                        item.id,
                        "approved_gate",
                        note=note,
                        resolved_by=f"auto-classifier:{rule_name}",
                        db_path=db_path,
                    )
                    resolved_ids.append(item.id)
                    logger.info(
                        "classifier.auto_resolved",
                        extra={"item_id": item.id, "rule": rule_name},
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "classifier.resolve_failed",
                        extra={"item_id": item.id, "rule": rule_name, "error": str(exc)},
                    )
            break  # one rule per item

    return ClassifierResult(
        inspected=len(pending),
        auto_resolved=len(resolved_ids) if not dry_run else sum(rules_fired.values()),
        rules_fired=rules_fired,
        item_ids_resolved=resolved_ids,
    )
