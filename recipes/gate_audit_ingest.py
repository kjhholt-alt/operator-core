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
        import os
        from operator_core import gate_review, gate_review_classifier, outreach_audit

        # Resilience: if the audit log doesn't exist yet (no shadow product
        # has emitted any events) just return empty — fires every 10 min so
        # we don't want to crash the daemon scheduler on a missing path.
        try:
            paths = outreach_audit.default_audit_paths()
            events = list(outreach_audit._iter_events(paths))
        except (FileNotFoundError, OSError) as exc:
            ctx.logger.debug("gate_audit_ingest.no_log", extra={"error": str(exc)})
            events = []

        if ctx.dry_run:
            return {
                "events": events, "new": 0, "updated": 0, "dry_run": True,
                "auto_resolved": 0, "rules_fired": {},
            }

        try:
            new, updated = gate_review.ingest_events(events)
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("gate_audit_ingest.ingest_failed", extra={"error": str(exc)})
            return {
                "events": events, "new": 0, "updated": 0, "dry_run": False,
                "ingest_error": str(exc),
                "auto_resolved": 0, "rules_fired": {},
            }

        # Auto-classifier sweep (opt-out via env var). Runs after every
        # ingest so newly-arrived items that match high-confidence rules
        # never sit in the queue waiting on a human click.
        auto_resolved = 0
        rules_fired: dict[str, int] = {}
        if os.environ.get("OPERATOR_GATE_REVIEW_AUTO_CLASSIFY", "1").strip().lower() in {"1", "true", "yes"}:
            try:
                min_hits = int(os.environ.get("OPERATOR_GATE_REVIEW_CLASSIFY_MIN_HITS", "2"))
            except ValueError:
                min_hits = 2
            try:
                cres = gate_review_classifier.classify_pending(min_hits=min_hits)
                auto_resolved = cres.auto_resolved
                rules_fired = cres.rules_fired
            except Exception as exc:  # noqa: BLE001
                ctx.logger.warning("gate_audit_ingest.classifier_failed", extra={"error": str(exc)})

        return {
            "events": events,
            "new": new,
            "updated": updated,
            "dry_run": False,
            "auto_resolved": auto_resolved,
            "rules_fired": rules_fired,
        }

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
        if result.get("ingest_error"):
            return f":warning: gate_audit_ingest failed: `{result['ingest_error']}`"
        auto_resolved = int(result.get("auto_resolved", 0))
        if new == 0 and updated == 0 and auto_resolved == 0:
            return ""
        breakdown = result.get("pending_by_product") or {}
        per = ", ".join(f"{p}: {n}" for p, n in sorted(breakdown.items()))
        lines = [
            "**Sender Gate audit ingest** -- new disagreements landed in the review queue.",
            f"- ingested: {new} new / {updated} updated",
        ]
        if auto_resolved:
            rules = result.get("rules_fired") or {}
            rule_breakdown = ", ".join(f"{r}: {n}" for r, n in sorted(rules.items()) if n)
            lines.append(f"- auto-resolved: {auto_resolved} (" + (rule_breakdown or "no breakdown") + ")")
        lines.append(f"- pending total: {pending_total}" + (f" ({per})" if per else ""))
        lines.append("Triage with `/op gate-review` (Discord) or open `/gate-review` in browser.")
        return "\n".join(lines)
