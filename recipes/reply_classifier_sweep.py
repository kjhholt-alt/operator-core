"""reply_classifier_sweep -- run the reply auto-classifier every 15 minutes.

Mirror of ``gate_audit_ingest`` but pointed at the outreach reply ledger.
Auto-closes obvious negatives (opt-out, hard bounce, OOO autoresponder)
so the operator only sees genuinely ambiguous or positive replies.

Always-on. Opt-out via ``OPERATOR_REPLY_AUTO_CLASSIFY=0``.

Quiet on the happy path -- only posts to #automations when at least
one thread was auto-closed this tick.
"""

from __future__ import annotations

import os
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class ReplyClassifierSweep(Recipe):
    name = "reply_classifier_sweep"
    version = "1.0.0"
    description = (
        "Walk NEW outreach reply threads and auto-close obvious negatives "
        "(opt-out / hard bounce / OOO). Operator only sees ambiguous + "
        "positive replies. Opt-out via OPERATOR_REPLY_AUTO_CLASSIFY=0."
    )
    cost_budget_usd = 0.0
    schedule = "*/15 * * * *"
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "every-15m")

    async def verify(self, ctx: RecipeContext) -> bool:
        try:
            from operator_core import reply_classifier, replies  # noqa: F401
        except ImportError:
            return False
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        if os.environ.get("OPERATOR_REPLY_AUTO_CLASSIFY", "1").strip().lower() not in {"1", "true", "yes"}:
            return {"enabled": False, "inspected": 0, "auto_closed": 0, "rules_fired": {}}

        from operator_core import reply_classifier

        try:
            res = reply_classifier.classify_pending(dry_run=ctx.dry_run)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("reply_classifier_sweep.failed", extra={"error": str(exc)})
            return {"enabled": True, "inspected": 0, "auto_closed": 0,
                    "rules_fired": {}, "error": str(exc)}

        return {
            "enabled": True,
            "inspected": res.inspected,
            "auto_closed": res.auto_closed,
            "rules_fired": res.rules_fired,
            "dry_run": ctx.dry_run,
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        if not result.get("enabled"):
            return ""
        if result.get("error"):
            return f":warning: reply_classifier_sweep failed: `{result['error']}`"
        if result.get("dry_run"):
            return ""  # dry-run never posts
        auto_closed = int(result.get("auto_closed", 0))
        if auto_closed == 0:
            return ""
        rules = result.get("rules_fired") or {}
        rb = ", ".join(f"{r}: {n}" for r, n in sorted(rules.items()) if n)
        return (
            "**reply_classifier** -- auto-closed obvious negatives.\n"
            f"- inspected: {result.get('inspected', 0)}\n"
            f"- auto-closed: {auto_closed}" + (f" ({rb})" if rb else "") + "\n"
            "Positive interest threads still surface for human review."
        )
