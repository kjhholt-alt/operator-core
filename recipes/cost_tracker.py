"""cost_tracker -- portfolio-wide recipe/model spend rollup."""

from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

from operator_core._vendor.events_ndjson import read_events
from operator_core.recipes import Recipe, RecipeContext, register_recipe


def _utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _data_dir() -> Path:
    return Path(os.environ.get("OPERATOR_DATA_DIR", str(Path.home() / ".operator" / "data")))


def _portfolio_cost_path() -> Path:
    return Path(os.environ.get("OPERATOR_PORTFOLIO_COST_PATH", str(_data_dir() / "portfolio_cost.json")))


def _parse_ts(value: Any) -> dt.datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def _payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload")
    return payload if isinstance(payload, dict) else {}


def _cost_usd(event: dict[str, Any]) -> float:
    payload = _payload(event)
    raw = (
        event.get("cost_usd")
        if event.get("cost_usd") is not None
        else payload.get("cost_usd")
    )
    if raw is None:
        raw = event.get("amount_usd") if event.get("amount_usd") is not None else payload.get("amount_usd")
    try:
        amount = float(raw or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return amount if amount > 0 else 0.0


def _recipe(event: dict[str, Any]) -> str:
    payload = _payload(event)
    return str(event.get("recipe") or payload.get("recipe") or event.get("source") or payload.get("agent") or "unknown")


def _project(event: dict[str, Any]) -> str:
    payload = _payload(event)
    return str(event.get("project") or payload.get("project") or _recipe(event))


def _model(event: dict[str, Any]) -> str:
    payload = _payload(event)
    return str(event.get("model") or payload.get("model") or "unknown")


def _round_map(values: dict[str, float]) -> dict[str, float]:
    return {k: round(v, 6) for k, v in sorted(values.items())}


def _pct_change(current: float, previous: float) -> float | None:
    if previous <= 0:
        return None if current > 0 else 0.0
    return round(((current - previous) / previous) * 100.0, 2)


def _write_atomic_json(target: Path, data: dict[str, Any]) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target


def build_rollup(events: list[dict[str, Any]], *, now: dt.datetime | None = None) -> dict[str, Any]:
    now = (now or _utc_now()).astimezone(dt.timezone.utc)
    cutoff_30 = now - dt.timedelta(days=30)
    cutoff_7 = now - dt.timedelta(days=7)
    prior_7_cutoff = now - dt.timedelta(days=14)
    prior_30_cutoff = now - dt.timedelta(days=60)

    by_recipe: dict[str, float] = defaultdict(float)
    by_project: dict[str, float] = defaultdict(float)
    by_model: dict[str, float] = defaultdict(float)
    by_day: dict[str, float] = defaultdict(float)
    current_week = 0.0
    previous_week = 0.0
    current_month = 0.0
    previous_month = 0.0
    accepted = 0

    for event in events:
        ts = _parse_ts(event.get("ts"))
        if ts is None:
            continue
        cost = _cost_usd(event)
        if cost <= 0:
            continue
        accepted += 1

        if ts >= cutoff_30:
            by_recipe[_recipe(event)] += cost
            by_project[_project(event)] += cost
            by_model[_model(event)] += cost
            by_day[ts.date().isoformat()] += cost
            current_month += cost
        elif ts >= prior_30_cutoff:
            previous_month += cost

        if ts >= cutoff_7:
            current_week += cost
        elif ts >= prior_7_cutoff:
            previous_week += cost

    total_30d = sum(by_day.values())
    return {
        "schema_version": 1,
        "generated_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "window_days": 30,
        "event_count": accepted,
        "total_30d_usd": round(total_30d, 6),
        "by_recipe": _round_map(by_recipe),
        "by_project": _round_map(by_project),
        "by_day": _round_map(by_day),
        "by_model": _round_map(by_model),
        "trends": {
            "week_current_usd": round(current_week, 6),
            "week_previous_usd": round(previous_week, 6),
            "week_over_week_pct": _pct_change(current_week, previous_week),
            "month_current_usd": round(current_month, 6),
            "month_previous_usd": round(previous_month, 6),
            "month_over_month_pct": _pct_change(current_month, previous_month),
        },
    }


@register_recipe
class CostTracker(Recipe):
    name = "cost_tracker"
    version = "1.1.0"
    description = "Portfolio-wide cost roll-up across recipes, projects, days, and models"
    cost_budget_usd = 0.0
    schedule = "0 8 * * *"
    timeout_sec = 60
    discord_channel = "automations"
    requires_clients = ("discord",)
    tags = ("weekly", "cost")

    async def verify(self, ctx: RecipeContext) -> bool:
        return ctx.clients.get("discord") is not None

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        return read_events("cost")[-50_000:]

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        return build_rollup(data)

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        out = _write_atomic_json(_portfolio_cost_path(), result)
        ranked = sorted(result["by_recipe"].items(), key=lambda kv: -kv[1])
        top_models = sorted(result["by_model"].items(), key=lambda kv: -kv[1])
        lines = [
            f"**Cost report** -- ${result['trends']['week_current_usd']:.4f} this week",
            f"- 30d total: ${result['total_30d_usd']:.4f}",
            f"- WoW: {result['trends']['week_over_week_pct']}%",
            f"- MoM: {result['trends']['month_over_month_pct']}%",
            f"- rollup: {out}",
        ]
        if ranked:
            lines.append("- top recipes: " + ", ".join(f"{name} ${amt:.4f}" for name, amt in ranked[:3]))
        if top_models:
            known = [(name, amt) for name, amt in top_models if name != "unknown"]
            if known:
                lines.append("- model mix: " + ", ".join(f"{name} ${amt:.4f}" for name, amt in known[:3]))
            elif result["by_model"].get("unknown", 0) > 0:
                lines.append("- model mix anomaly: all recent cost events have unknown model")
        return "\n".join(lines)[:1990]
