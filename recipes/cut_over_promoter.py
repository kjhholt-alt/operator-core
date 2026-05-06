"""cut_over_promoter -- recommend the next Sender Gate cut-over flip.

Reads the local outreach-common events log via the audit dashboard,
checks divergence per product, and recommends one of:

  - ``stay``         : not enough shadow data yet OR live already
  - ``flip_live``    : ai-ops/PP/oe is ready to promote audit-only -> live
  - ``halt``         : divergence detected; halt the rest of the cut-over

The recipe is read-only: it never flips an env var or touches a
sender. It posts a recommendation to ``#projects`` so the operator
can pull the trigger via PR.

Decision rule (per product):

- ``shadow_sends < SHADOW_THRESHOLD``  -> ``stay``: not enough data
- ``divergences > DIVERGENCE_TOLERANCE`` -> ``halt``
- otherwise -> ``flip_live``
"""

from __future__ import annotations

from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe

# Tunables -- intentionally conservative for the first cut-over.
SHADOW_THRESHOLD = 10  # min shadow envelopes before we recommend live
DIVERGENCE_TOLERANCE = 0  # any divergence halts


@register_recipe
class CutOverPromoter(Recipe):
    name = "cut_over_promoter"
    version = "1.0.0"
    description = (
        "Inspect the outreach Sender-Gate audit dashboard, recommend"
        " the next product to flip from shadow to live, halt on divergence"
    )
    cost_budget_usd = 0.05  # all local work; tiny budget for any Discord
    schedule = "15 7 * * *"  # daily, 07:15 (after morning briefing)
    timeout_sec = 60
    discord_channel = "projects"
    requires_clients = ()  # uses local files only; discord is optional
    tags = ("daily", "outreach", "cutover")

    async def verify(self, ctx: RecipeContext) -> bool:
        # Only requires the outreach-common events module to be importable
        try:
            from operator_core.outreach_audit import compute_dashboard  # noqa: F401
        except Exception:
            return False
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        from operator_core.outreach_audit import compute_dashboard

        dash = compute_dashboard()
        return {"dashboard": dash.to_dict()}

    async def analyze(self, ctx: RecipeContext, data: dict[str, Any]) -> dict[str, Any]:
        dash = data.get("dashboard") or {}
        recs: list[dict[str, Any]] = []
        for prod in dash.get("products", []):
            shadow = int(prod.get("shadow_sends", 0))
            divergences = int(prod.get("divergences", 0))
            if divergences > DIVERGENCE_TOLERANCE:
                action = "halt"
                reason = (
                    "divergences="
                    + str(divergences)
                    + " (tolerance="
                    + str(DIVERGENCE_TOLERANCE)
                    + ")"
                )
            elif shadow < SHADOW_THRESHOLD:
                action = "stay"
                reason = (
                    "shadow_sends="
                    + str(shadow)
                    + " < threshold="
                    + str(SHADOW_THRESHOLD)
                )
            else:
                action = "flip_live"
                reason = (
                    "shadow_sends="
                    + str(shadow)
                    + " divergences=0"
                )
            recs.append(
                {
                    "product": prod.get("product"),
                    "action": action,
                    "reason": reason,
                    "shadow_sends": shadow,
                    "real_sends": int(prod.get("real_sends", 0)),
                    "divergences": divergences,
                }
            )
        return {"recommendations": recs, "dashboard_total_events": dash.get("total_events", 0)}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        recs = result.get("recommendations") or []
        total = result.get("dashboard_total_events", 0)
        if not recs:
            return ""  # nothing to post
        lines = ["**Cut-over promoter** -- " + str(total) + " send events scanned"]
        for r in recs:
            tag = {
                "halt": ":octagonal_sign:",
                "flip_live": ":rocket:",
                "stay": ":hourglass_flowing_sand:",
            }.get(r["action"], "?")
            lines.append(
                "  "
                + tag
                + " "
                + str(r["product"])
                + " -> `"
                + r["action"]
                + "` ("
                + r["reason"]
                + ")"
            )
        return "\n".join(lines)
