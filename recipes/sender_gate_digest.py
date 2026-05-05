"""sender_gate_digest -- one daily Sender Gate roll-up.

Single message per day to #automations. Reads everything we built over the
cut-over loop (gate_review queue, cutover_streak, rollback_alerts events,
auto-suppression PR activity) and renders one bird's-eye-view post:

  - yesterday's auto-classified count + per-rule breakdown
  - yesterday's human-resolved count
  - current pending queue size (per product)
  - products promoted in the last 24h
  - any rollback_alerts events in the last 24h
  - any auto_merge_suppression / auto_merge_labeled merges in the last 24h
    (best-effort -- read from cost+jobs if available, otherwise skip)

Always-on. Pure read; never writes. Single Discord post per day at 06:45.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe


@register_recipe
class SenderGateDigest(Recipe):
    name = "sender_gate_digest"
    version = "1.0.0"
    description = (
        "One daily Sender Gate roll-up to #automations. Auto-classified vs "
        "human-resolved, pending queue, recent promotions, rollback alerts."
    )
    cost_budget_usd = 0.0
    schedule = "45 6 * * *"   # 06:45 daily
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ()
    tags = ("outreach", "cut-over", "daily", "digest")

    async def verify(self, ctx: RecipeContext) -> bool:
        return True

    async def query(self, ctx: RecipeContext) -> dict[str, Any]:
        from operator_core import gate_review, cutover_streak

        now = datetime.now(timezone.utc)
        yesterday_cutoff = now - timedelta(hours=24)

        # ---- gate_review aggregates --------------------------------------------
        # Read the full review_items table once and bucket in Python -- avoids
        # per-product round trips.
        rows: list[dict[str, Any]] = []
        try:
            with gate_review.open_db() as conn:
                cur = conn.execute(
                    "SELECT product, status, resolved_by, resolved_ts FROM review_items"
                )
                rows = [dict(r) for r in cur.fetchall()]
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("digest.gate_review_read_failed", extra={"error": str(exc)})

        pending_by_product: dict[str, int] = {}
        auto_resolved_24h_by_rule: dict[str, int] = {}
        human_resolved_24h_by_source: dict[str, int] = {}
        for r in rows:
            status_v = r.get("status")
            product = r.get("product") or "?"
            if status_v == "pending":
                pending_by_product[product] = pending_by_product.get(product, 0) + 1
                continue
            ts_str = r.get("resolved_ts")
            if not ts_str:
                continue
            ts = _parse_ts(ts_str)
            if ts is None or ts < yesterday_cutoff:
                continue
            resolver = r.get("resolved_by") or ""
            if resolver.startswith("auto-classifier:"):
                rule = resolver.split(":", 1)[1] or "unknown"
                auto_resolved_24h_by_rule[rule] = auto_resolved_24h_by_rule.get(rule, 0) + 1
            elif resolver.startswith("operator-core/"):
                # Auto-suppression mark counts as automation, not human.
                key = "operator-core-mark"
                auto_resolved_24h_by_rule[key] = auto_resolved_24h_by_rule.get(key, 0) + 1
            else:
                src = resolver or "unknown"
                human_resolved_24h_by_source[src] = human_resolved_24h_by_source.get(src, 0) + 1

        # ---- promotions in last 24h --------------------------------------------
        recent_promotions: list[dict[str, str]] = []
        try:
            for s in cutover_streak.list_all():
                if not s.promoted_ts:
                    continue
                pt = _parse_ts(s.promoted_ts)
                if pt and pt >= yesterday_cutoff:
                    recent_promotions.append({
                        "product": s.product,
                        "promoted_ts": s.promoted_ts,
                        "pr_url": s.promoted_pr_url or "",
                    })
        except Exception as exc:  # noqa: BLE001
            ctx.logger.warning("digest.streak_read_failed", extra={"error": str(exc)})

        # ---- rollback_alerts events in last 24h --------------------------------
        rollback_alerts_24h = _read_recent_events("rollback_alerts", yesterday_cutoff)

        # ---- pending total -----------------------------------------------------
        total_auto = sum(auto_resolved_24h_by_rule.values())
        total_human = sum(human_resolved_24h_by_source.values())
        ratio = (total_auto / (total_auto + total_human)) if (total_auto + total_human) else None

        return {
            "now": now.isoformat().replace("+00:00", "Z"),
            "auto_resolved_24h_by_rule": auto_resolved_24h_by_rule,
            "auto_resolved_24h_total": total_auto,
            "human_resolved_24h_by_source": human_resolved_24h_by_source,
            "human_resolved_24h_total": total_human,
            "auto_classify_ratio_24h": ratio,
            "pending_by_product": pending_by_product,
            "pending_total": sum(pending_by_product.values()),
            "recent_promotions": recent_promotions,
            "rollback_alerts_24h": rollback_alerts_24h,
        }

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        # Always emits -- this is a daily ritual; "nothing happened" is itself
        # a useful signal in the channel.
        auto_total = result["auto_resolved_24h_total"]
        human_total = result["human_resolved_24h_total"]
        ratio = result["auto_classify_ratio_24h"]
        pending_total = result["pending_total"]
        promotions = result["recent_promotions"]
        rollback_alerts = result["rollback_alerts_24h"]

        lines = ["**Sender Gate -- daily digest**"]

        # Triage line.
        if auto_total + human_total == 0:
            lines.append("- triage: no resolutions in last 24h")
        else:
            ratio_str = f"{ratio*100:.0f}%" if ratio is not None else "n/a"
            lines.append(
                f"- triage 24h: {auto_total + human_total} resolutions "
                f"({auto_total} auto / {human_total} human, auto-rate {ratio_str})"
            )
            rules = result["auto_resolved_24h_by_rule"]
            if rules:
                rb = ", ".join(f"{k}: {v}" for k, v in sorted(rules.items()))
                lines.append(f"  - auto by rule: {rb}")
            srcs = result["human_resolved_24h_by_source"]
            if srcs:
                sb = ", ".join(f"{k}: {v}" for k, v in sorted(srcs.items()))
                lines.append(f"  - human by source: {sb}")

        # Pending line.
        if pending_total == 0:
            lines.append("- pending queue: empty :white_check_mark:")
        else:
            pb = ", ".join(f"{p}: {n}" for p, n in sorted(result["pending_by_product"].items()))
            lines.append(f"- pending queue: {pending_total} total ({pb})")

        # Promotions.
        if promotions:
            lines.append(f"- promotions in last 24h: {len(promotions)}")
            for p in promotions:
                pr = f" <{p['pr_url']}>" if p.get("pr_url") else ""
                lines.append(f"  - `{p['product']}` at {p['promoted_ts']}{pr}")
        else:
            lines.append("- promotions in last 24h: 0")

        # Rollback alerts -- always show, even when zero, because that's the safety signal.
        if rollback_alerts:
            lines.append(f"- :rotating_light: rollback alerts in last 24h: {len(rollback_alerts)}")
            for a in rollback_alerts[:5]:
                payload = a.get("payload") or a
                product = payload.get("product", "?")
                reasons = payload.get("reasons") or []
                lines.append(f"  - `{product}`: {', '.join(reasons)[:200]}")
        else:
            lines.append("- rollback alerts in last 24h: 0 :white_check_mark:")

        lines.append("---")
        lines.append("Dashboards: /gate-review · /cut-over · /metrics")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_ts(s: str) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _read_recent_events(stream: str, since: datetime) -> list[dict[str, Any]]:
    """Read events written by the vendored shim and filter by ts."""
    events_dir = Path(os.environ.get("OPERATOR_EVENTS_DIR", str(Path.home() / ".operator" / "data")))
    target = events_dir / f"{stream}.ndjson"
    if not target.exists():
        return []
    out: list[dict[str, Any]] = []
    try:
        with open(target, "r", encoding="utf-8") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    env = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                ts = _parse_ts(env.get("ts") or "")
                if ts is None or ts >= since:
                    out.append(env)
    except OSError:
        return []
    return out
