"""Auto-classifier for the outreach reply ledger.

Same shape as ``gate_review_classifier``: walk NEW threads, look for
high-confidence patterns in the latest inbound message body, auto-close
the obvious ones. The operator only sees ambiguous threads.

**Important scope decision:** this classifier ONLY auto-closes negative
or terminal reply types (OOO, opt-out, hard bounce, autoresponder). It
NEVER touches positive-interest replies, even if it's confident. The
cost of false-negative ("we lost a deal") is much higher than the cost
of a human reviewing a clearly positive reply that the classifier
could have skipped. So the upside is: no more clicking through
auto-replies; the downside is bounded to "human still sees the good
stuff."

Rules (in priority order; first match wins):

1. **opt_out** -- unmistakable opt-out / unsubscribe / remove-me
   language in the body. Closes the thread; future outreach to the
   sender stays out (suppression list management is the next step,
   not this recipe's job).

2. **hard_bounce** -- mailer-daemon / postmaster bounce signatures.
   Almost always already auto-handled by the SMTP layer, but operators
   sometimes get a forwarded copy in the reply ledger.

3. **out_of_office** -- vacation auto-reply. Will recheck in N days
   if a fresh inbound arrives, since the OOO date may have passed.

Anything else stays NEW. Recipe runs every 15 minutes.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

from . import replies

logger = logging.getLogger("operator.reply_classifier")


# --- pattern banks ----------------------------------------------------------
# Each rule is a tuple (rule_name, list of compiled regex patterns).
# Patterns are case-insensitive and matched against the FIRST inbound
# message body (the most recent reply we've ingested).

_OPT_OUT_PATTERNS = [
    re.compile(r"\bunsubscribe\b", re.IGNORECASE),
    re.compile(r"\bopt[- ]?out\b", re.IGNORECASE),
    re.compile(r"\b(stop|cease)\s+(emailing|contacting|messaging)\b", re.IGNORECASE),
    re.compile(r"\bremove\s+me\s+from\b", re.IGNORECASE),
    re.compile(r"\bdo\s+not\s+(email|contact)\s+me\b", re.IGNORECASE),
    re.compile(r"\bplease\s+(stop|don'?t)\s+(emailing|contacting)\b", re.IGNORECASE),
    re.compile(r"\btake\s+me\s+off\s+(your\s+)?list\b", re.IGNORECASE),
    re.compile(r"\bnot\s+interested\b", re.IGNORECASE),
]

_HARD_BOUNCE_PATTERNS = [
    re.compile(r"mail\s+delivery\s+(failed|failure)", re.IGNORECASE),
    re.compile(r"mailer[- ]daemon", re.IGNORECASE),
    re.compile(r"postmaster", re.IGNORECASE),
    re.compile(r"undeliverable", re.IGNORECASE),
    re.compile(r"recipient\s+address\s+rejected", re.IGNORECASE),
    re.compile(r"550 5\.\d\.\d", re.IGNORECASE),  # SMTP 5xx perm-fail
    re.compile(r"address\s+not\s+found", re.IGNORECASE),
    re.compile(r"no\s+such\s+(user|recipient)", re.IGNORECASE),
]

_OOO_PATTERNS = [
    re.compile(r"\bout\s+of\s+(the\s+)?office\b", re.IGNORECASE),
    re.compile(r"\bautomatic\s+reply\b", re.IGNORECASE),
    re.compile(r"\bauto[- ]reply\b", re.IGNORECASE),
    re.compile(r"\bI\s+am\s+(currently\s+)?(out|away|on\s+vacation|on\s+leave)\b", re.IGNORECASE),
    re.compile(r"\bI\s+will\s+be\s+out\b", re.IGNORECASE),
    re.compile(r"\bvacation\s+(reply|message|response)\b", re.IGNORECASE),
    re.compile(r"\bthank\s+you\s+for\s+your\s+(email|message)[\.,!]?\s*I\s+am\b", re.IGNORECASE),
]

_RULES = (
    ("opt_out", _OPT_OUT_PATTERNS),
    ("hard_bounce", _HARD_BOUNCE_PATTERNS),
    ("out_of_office", _OOO_PATTERNS),
)


@dataclass
class ClassifierResult:
    inspected: int
    auto_closed: int
    rules_fired: dict[str, int]
    thread_ids_closed: list[str]


# --- per-rule predicate -----------------------------------------------------

def classify_message_body(body: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Match a body against the rule bank.

    Returns ``(rule_name, matched_phrase)`` or ``(None, None)`` for no match.
    First rule + first pattern wins.
    """
    if not body:
        return None, None
    for rule_name, patterns in _RULES:
        for pat in patterns:
            m = pat.search(body)
            if m:
                return rule_name, m.group(0)
    return None, None


# --- main entry point -------------------------------------------------------

def classify_pending(
    *,
    store: Optional[replies.ReplyStore] = None,
    dry_run: bool = False,
    limit: int = 200,
) -> ClassifierResult:
    """Walk NEW threads; auto-close on the first matching rule.

    ``dry_run`` returns the would-close counts without writing.
    """
    store = store or replies.ReplyStore()
    threads = store.list_threads(status=replies.STATUS_NEW, limit=limit)

    rules_fired: dict[str, int] = {name: 0 for name, _ in _RULES}
    closed_ids: list[str] = []

    for thread in threads:
        # Use the most recent inbound message as the classification target.
        msgs = store.list_messages(thread.thread_id)
        last_in = next((m for m in reversed(msgs) if m.direction == "in"), None)
        if last_in is None:
            continue
        rule, phrase = classify_message_body(last_in.body_md)
        if rule is None:
            continue
        rules_fired[rule] += 1
        if dry_run:
            logger.debug(
                "reply_classifier.dry_run.would_close",
                extra={"thread_id": thread.thread_id, "rule": rule, "phrase": phrase},
            )
            continue
        try:
            store.close_thread(thread.thread_id)
            closed_ids.append(thread.thread_id)
            logger.info(
                "reply_classifier.auto_closed",
                extra={"thread_id": thread.thread_id, "rule": rule, "phrase": phrase[:80] if phrase else None},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "reply_classifier.close_failed",
                extra={"thread_id": thread.thread_id, "rule": rule, "error": str(exc)},
            )

    return ClassifierResult(
        inspected=len(threads),
        auto_closed=len(closed_ids) if not dry_run else sum(rules_fired.values()),
        rules_fired=rules_fired,
        thread_ids_closed=closed_ids,
    )
