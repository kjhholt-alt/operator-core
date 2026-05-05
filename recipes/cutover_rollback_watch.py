"""cutover_rollback_watch -- watch recently-promoted products for regressions.

After a product flips live (cut_over_promoter PR merged), we need to
notice fast if the gate routing turns out to be wrong: a sudden drop
in match%, a `would_allow_new` appearing, or pending items piling up.

This recipe runs every 30 minutes. For each product whose
``promoted_ts`` is within the last ROLLBACK_WINDOW_HOURS (default 48h),
it re-runs audit-report. If any regression signal fires, it posts a
loud alert to #automations *and* writes a ``rollback_alert`` event to
the events stream so the alert is replayable.

The recipe is deliberately read-only -- it never touches code, never
opens a rollback PR. Auto-rollback would be the wrong default: the
fix might be "tweak the gate", not "revert the flip". This recipe
just makes sure you SEE the regression within 30 minutes of it
starting, instead of hours later when a customer notices.

Always-on (no opt-in env flag) because rollback alerts are pure
observability; they cost nothing if nothing's wrong.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from operator_core import cutover_streak, outreach_audit
from operator_core._vendor import events_ndjson
from operator_core.recipes import Recipe, RecipeContext, register_recipe


DEFAULT_ROLLBACK_WINDOW_HOURS = 48
DEFAULT_MATCH_THRESHOLD = 95.0


@register_recipe
class CutoverRollbackWatch(Recipe):
    name = "cutover_rollback_watch"
    version = "1.0.0"
    description = (
        "Alert if a recently-promoted product's audit-report regresses. "
        "Watches every product with promoted_ts within the rollback window "
        "(default 48h). Read-only -- never auto-reverts."
    )
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "cut-over", "every-30m", "safety-net")

    async def verify(self, ctx: RecipeContext) -> bool:
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        try:
            window_h = int(os.environ.get("OPERATOR_ROLLBACK_WINDOW_HOURS", DEFAULT_ROLLBACK_WINDOW_HOURS))
        except ValueError:
            window_h = DEFAULT_ROLLBACK_WINDOW_HOURS
        try:
            threshold = float(os.environ.get("OPERATOR_CUTOVER_MATCH_PCT", DEFAULT_MATCH_THRESHOLD))
        except ValueError:
            threshold = DEFAULT_MATCH_THRESHOLD

        cutoff = datetime.now(timezone.utc) - timedelta(hours=window_h)
        in_window = [
            s for s in cutover_streak.list_all()
            if s.promoted_ts and _parse_ts(s.promoted_ts) and _parse_ts(s.promoted_ts) >= cutoff
        ]
        if not in_window:
            return {"watched": [], "alerts": [], "errors": []}

        try:
            paths = outreach_audit.default_audit_paths()
            summaries = outreach_audit.collect(paths)
        except (FileNotFoundError, OSError) as exc:
            return {"watched": [], "alerts": [], "errors": [f"audit log unreadable: {exc}"]}

        by_product = {s.product: s for s in summaries}
        watched: list[dict[str, Any]] = []
        alerts: list[dict[str, Any]] = []

        for streak in in_window:
            summary = by_product.get(streak.product)
            row = {
                "product": streak.product,
                "promoted_ts": streak.promoted_ts,
                "promoted_pr_url": streak.promoted_pr_url,
                "match_pct": summary.match_pct if summary else None,
                "would_allow_new": summary.would_allow_new if summary else 0,
                "would_block_new": summary.would_block_new if summary else 0,
                "pending": summary.triage_pending if summary else 0,
                "ready_now": bool(summary and summary.cutover_ready(threshold)),
            }
            watched.append(row)

            if summary is None:
                # No data at all for a product we just promoted -- alert.
                reasons = ["no_audit_data_post_promotion"]
            else:
                reasons = _regression_reasons(summary, threshold)
            if reasons:
                alert = {**row, "reasons": reasons}
                alerts.append(alert)
                # Skip emit during dry_run so test runs don't pollute the
                # events log; production runs always emit.
                if not ctx.dry_run:
                    _emit_rollback_alert_event(streak.product, alert)

        return {"watched": watched, "alerts": alerts, "errors": []}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        alerts = result.get("alerts") or []
        errors = result.get("errors") or []
        if not alerts and not errors:
            return ""
        lines = [":rotating_light: **cutover_rollback_watch** -- regression(s) detected:"]
        for a in alerts:
            mp = a.get("match_pct")
            mp_str = f"{mp:.1f}%" if mp is not None else "n/a"
            lines.append(
                f"- `{a['product']}` (promoted {a['promoted_ts']}) -- "
                f"match={mp_str}, allow_new={a['would_allow_new']}, "
                f"pending={a['pending']}, reasons={a['reasons']}"
            )
            if a.get("promoted_pr_url"):
                lines.append(f"  promotion PR: {a['promoted_pr_url']}")
        for e in errors:
            lines.append(f"- :warning: {e}")
        lines.append("Investigate before customers do. Roll back the flip if needed.")
        return "\n".join(lines)


def _regression_reasons(summary: outreach_audit.ProductSummary, threshold: float) -> list[str]:
    reasons: list[str] = []
    if summary.total == 0:
        reasons.append("no_events_post_promotion")
        return reasons
    if summary.match_pct < threshold:
        reasons.append(f"match_pct_{summary.match_pct:.1f}_below_{threshold:.1f}")
    if summary.would_allow_new > 0:
        reasons.append(f"would_allow_new_{summary.would_allow_new}")
    if not summary.fully_triaged:
        reasons.append(f"pending_disagreements_{summary.triage_pending}")
    return reasons


def _emit_rollback_alert_event(product: str, alert: dict[str, Any]) -> None:
    try:
        events_ndjson.append_event(
            stream="rollback_alerts",
            kind="cutover_regression",
            recipe="cutover_rollback_watch",
            payload={
                "product": product,
                "match_pct": alert.get("match_pct"),
                "would_allow_new": alert.get("would_allow_new"),
                "pending": alert.get("pending"),
                "reasons": alert.get("reasons"),
                "promoted_ts": alert.get("promoted_ts"),
            },
        )
    except Exception:
        # Telemetry failure must never sink the alert -- the Discord post
        # is what matters here. Swallow.
        pass


def _parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
