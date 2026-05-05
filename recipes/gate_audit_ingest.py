"""gate_audit_ingest -- pull new gate_audit ndjson events into the review queue.

The Sender Gate cut-over loop has one remaining manual step: the operator
has to run ``operator outreach gate-review ingest`` to move new
``gate_audit`` events from the events-ndjson stream into the SQLite
``review_queue``. This recipe automates that.

It is the ONLY mutating recipe in the framework that is safe to run on a
short cadence (default every 10 minutes) -- it only reads the audit log
and writes idempotently to the queue. Multiple concurrent runs do not
double-count because ``ingest_events`` keys on
(product, lead_hash, agreement).
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class GateAuditIngest(Recipe):
    name = "gate_audit_ingest"
    version = "1.0.0"
    description = (
        "Sweep gate_audit ndjson events into the SQLite review queue so "
        "/op gate-review can show pending disagreements without manual ingest."
    )
    cost_budget_usd = 0.0
    schedule = "*/10 * * * *"     # every 10 minutes
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "cut-over", "every-10m")

    async def verify(self, ctx: RecipeContext) -> bool:
        # Pure-local recipe -- needs only the gate_review module to be importable
        # and the audit log directory to be readable. Both are stdlib + sqlite.
        try:
            from operator_core import gate_review, outreach_audit  # noqa: F401
        except ImportError:
            return False
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        from operator_core import gate_review, outreach_audit

        paths = outreach_audit.default_audit_paths()
        events = list(outreach_audit._iter_events(paths))
        if ctx.dry_run:
            return {"events": events, "new": 0, "updated": 0, "dry_run": True}
        new, updated = gate_review.ingest_events(events)
        return {"events": events, "new": new, "updated": updated, "dry_run": False}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        from operator_core import gate_review

        # Snapshot pending counts per product so the format step can highlight
        # any backlog growth without re-querying.
        pending = gate_review.list_pending(limit=500)
        per_product: dict[str, int] = {}
        for item in pending:
            per_product[item.product] = per_product.get(item.product, 0) + 1
        return {
            **data,
            "pending_total": len(pending),
            "pending_by_product": per_product,
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        # Stay quiet on the happy path: only post when there is a non-zero
        # ingest delta or growing backlog. This is a 6x/hour recipe -- it
        # should not spam #automations.
        if result.get("dry_run"):
            return ""
        new = int(result.get("new", 0))
        updated = int(result.get("updated", 0))
        pending_total = int(result.get("pending_total", 0))
        if new == 0 and updated == 0:
            return ""
        breakdown = result.get("pending_by_product") or {}
        per = ", ".join(f"{p}: {n}" for p, n in sorted(breakdown.items()))
        lines = [
            "**Sender Gate audit ingest** -- new disagreements landed in the review queue.",
            f"- ingested: {new} new / {updated} updated",
            f"- pending total: {pending_total}" + (f" ({per})" if per else ""),
            "Triage with `/op gate-review` (Discord) or `operator outreach gate-review list`.",
        ]
        return "\n".join(lines)
