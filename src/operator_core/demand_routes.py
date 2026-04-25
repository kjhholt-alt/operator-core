"""Local HTTP routes for the signup-first demand dashboard."""

from __future__ import annotations

from typing import Any

from .demand_os import (
    ExperimentStore,
    build_nightly_plan,
)
from .http_server import register_extra_route
from .lead_ledger import LeadStore


def register_demand_routes() -> None:
    """Register local-only demand routes on the shared HTTP server."""

    def _get_demand(handler: Any, body: Any) -> tuple[int, str]:
        store = LeadStore()
        plan = build_nightly_plan(store, ExperimentStore(store.db_path))
        return 200, render_demand_page(plan.to_dict())

    def _get_demand_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        store = LeadStore()
        plan = build_nightly_plan(store, ExperimentStore(store.db_path))
        return 200, plan.to_dict()

    def _get_leads_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        store = LeadStore()
        return 200, {
            "open_count": store.count_open(),
            "leads": [_lead_payload(lead) for lead in store.list(open_only=True, limit=50)],
        }

    def _get_experiments_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        store = LeadStore()
        experiments = ExperimentStore(store.db_path).list(limit=50)
        return 200, {"experiments": [row.to_dict() for row in experiments]}

    register_extra_route("GET", "/demand", _get_demand)
    register_extra_route("GET", "/demand.json", _get_demand_json)
    register_extra_route("GET", "/leads.json", _get_leads_json)
    register_extra_route("GET", "/experiments.json", _get_experiments_json)


def render_demand_page(plan: dict[str, Any]) -> str:
    scoreboard = list(plan.get("scoreboard") or [])
    active = list(plan.get("active_experiments") or [])
    backlog = list(plan.get("backlog") or [])
    leads = list(plan.get("top_leads") or [])
    watch = list(plan.get("watch_sources") or [])
    broker = dict(plan.get("broker_close_state") or {})
    focus = plan.get("focus_product") or "-"
    generated = plan.get("generated_at") or "-"
    top_score = scoreboard[0].get("demand_score") if scoreboard else 0

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OPERATOR // DEMAND</title>
<style>
{_CSS}
</style>
</head>
<body>
<div class="page">
  <header class="top">
    <div class="eyebrow"><span class="pip pip-ok"></span>OPERATOR // DEMAND</div>
    <nav><a href="/ops">Ops</a><a href="/demand.json">JSON</a><a href="/leads.json">Leads</a></nav>
  </header>
  <main>
    <section class="summary">
      <div>
        <div class="eyebrow">Focus Lane</div>
        <h1>{_esc(focus)}</h1>
        <p>Signup-first operating plan. No payment work until demand is visible.</p>
      </div>
      <div class="kpis">
        {_stat("top score", top_score)}
        {_stat("open leads", len(leads))}
        {_stat("running", len(active))}
        {_stat("watch", len(watch))}
      </div>
    </section>

    <section class="grid two">
      <div class="panel">
        <div class="panel-head"><span>Demand Scoreboard</span><small>{_esc(generated[:16]).replace("T", " ")}</small></div>
        <table>
          <thead><tr><th>Product</th><th>Score</th><th>Open</th><th>High</th><th>Next</th></tr></thead>
          <tbody>{_score_rows(scoreboard)}</tbody>
        </table>
      </div>
      <div class="panel">
        <div class="panel-head"><span>Active Experiments</span><small>work one lane</small></div>
        <table>
          <thead><tr><th>Status</th><th>Product</th><th>Experiment</th><th>Priority</th></tr></thead>
          <tbody>{_experiment_rows(active) or _empty_row(4, "no running experiments")}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <div class="panel-head"><span>Follow-up Queue</span><small>highest intent first</small></div>
      <table>
        <thead><tr><th>Score</th><th>Status</th><th>Product</th><th>Who</th><th>Event</th></tr></thead>
        <tbody>{_lead_rows(leads)}</tbody>
      </table>
    </section>

    <section class="panel">
      <div class="panel-head"><span>AI Ops Broker Close State</span><small>manual sales lane</small></div>
      <div class="kpis broker-kpis">
        {_stat("hot unworked", broker.get("hot_unworked", 0))}
        {_stat("stale hot", broker.get("stale_hot", 0))}
        {_stat("booked", broker.get("booked", 0))}
        {_stat("won", broker.get("won", 0))}
      </div>
      <table>
        <thead><tr><th>Status</th><th>Score</th><th>Broker</th><th>Workflow</th></tr></thead>
        <tbody>{_broker_rows(list(broker.get("top_actions") or []))}</tbody>
      </table>
    </section>

    <section class="grid two">
      <div class="panel">
        <div class="panel-head"><span>Experiment Bench</span><small>persistent registry</small></div>
        <table>
          <thead><tr><th>Status</th><th>Product</th><th>Experiment</th><th>Priority</th></tr></thead>
          <tbody>{_experiment_rows(backlog)}</tbody>
        </table>
      </div>
      <div class="panel">
        <div class="panel-head"><span>Source Watch</span><small>capture gaps</small></div>
        <table>
          <thead><tr><th>Product</th><th>Source</th><th>Note</th></tr></thead>
          <tbody>{_watch_rows(watch)}</tbody>
        </table>
      </div>
    </section>

    <section class="panel plan">
      <div class="panel-head"><span>Night Plan</span><small>local markdown</small></div>
      <pre>{_esc(render_nightly_plan_text(plan))}</pre>
    </section>
  </main>
</div>
</body>
</html>"""


def render_nightly_plan_text(plan: dict[str, Any]) -> str:
    # Keep the browser page independent from dataclass internals by rendering a
    # compact plain-text summary from the JSON-shaped payload.
    lines = [
        f"Focus lane: {plan.get('focus_product') or '-'}",
        "",
        "Tonight:",
        "- Work highest-intent follow-ups.",
        "- Advance one running or top backlog experiment.",
        "- Fix signup-source capture gaps before broad distribution.",
    ]
    active = list(plan.get("active_experiments") or [])
    if active:
        lines.extend(["", "Running:"])
        for row in active[:3]:
            lines.append(f"- {row.get('product')}: {row.get('title')} ({row.get('id')})")
    return "\n".join(lines)


def _lead_payload(lead: Any) -> dict[str, Any]:
    return {
        "id": lead.id,
        "product": lead.product,
        "event_type": lead.event_type,
        "email": lead.email,
        "company": lead.company,
        "status": lead.status,
        "intent_score": lead.intent_score,
        "next_action": lead.next_action,
        "event_created_at": lead.event_created_at,
    }


def _score_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return _empty_row(5, "no demand data")
    out = []
    for row in rows:
        out.append(
            "<tr>"
            f"<td>{_pip_for_score(row.get('demand_score'))}{_esc(row.get('product'))}</td>"
            f"<td class=\"mono strong\">{_esc(row.get('demand_score'))}</td>"
            f"<td class=\"mono\">{_esc(row.get('open_leads'))}</td>"
            f"<td class=\"mono\">{_esc(row.get('high_intent'))}</td>"
            f"<td>{_esc(row.get('next_experiment'))}</td>"
            "</tr>"
        )
    return "\n".join(out)


def _lead_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return _empty_row(5, "no open leads")
    out = []
    for row in rows:
        who = row.get("company") or row.get("email") or "-"
        out.append(
            "<tr>"
            f"<td class=\"mono strong\">{_esc(row.get('intent_score'))}</td>"
            f"<td class=\"mono\">{_esc(row.get('status'))}</td>"
            f"<td>{_esc(row.get('product'))}</td>"
            f"<td>{_esc(who)}</td>"
            f"<td>{_esc(row.get('event_type'))}</td>"
            "</tr>"
        )
    return "\n".join(out)


def _broker_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return _empty_row(4, "no broker close actions")
    out = []
    for row in rows:
        who = row.get("company") or row.get("email") or "-"
        out.append(
            "<tr>"
            f"<td class=\"mono\">{_esc(row.get('status'))}</td>"
            f"<td class=\"mono strong\">{_esc(row.get('intent_score'))}</td>"
            f"<td>{_esc(who)}</td>"
            f"<td>{_esc(row.get('recommended_workflow'))}</td>"
            "</tr>"
        )
    return "\n".join(out)


def _experiment_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return _empty_row(4, "no experiments")
    out = []
    for row in rows:
        out.append(
            "<tr>"
            f"<td class=\"mono\">{_esc(row.get('status'))}</td>"
            f"<td>{_esc(row.get('product'))}</td>"
            f"<td><span class=\"muted mono\">{_esc(row.get('id'))}</span><br>{_esc(row.get('title'))}</td>"
            f"<td class=\"mono strong\">{_esc(row.get('priority'))}</td>"
            "</tr>"
        )
    return "\n".join(out)


def _watch_rows(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return _empty_row(3, "no source gaps")
    out = []
    for row in rows:
        out.append(
            "<tr>"
            f"<td>{_esc(row.get('product'))}</td>"
            f"<td class=\"mono\">{_esc(row.get('source_table'))}</td>"
            f"<td>{_esc(row.get('note'))}</td>"
            "</tr>"
        )
    return "\n".join(out)


def _stat(label: str, value: Any) -> str:
    return (
        '<div class="stat">'
        f'<div class="stat-value">{_esc(value)}</div>'
        f'<div class="stat-label">{_esc(label)}</div>'
        "</div>"
    )


def _pip_for_score(score: Any) -> str:
    try:
        value = int(score or 0)
    except (TypeError, ValueError):
        value = 0
    cls = "pip-ok" if value >= 70 else "pip-warn" if value >= 30 else "pip-idle"
    return f'<span class="pip {cls}"></span>'


def _empty_row(cols: int, text: str) -> str:
    return f'<tr><td colspan="{cols}" class="empty">{_esc(text)}</td></tr>'


def _esc(value: Any) -> str:
    text = "" if value is None else str(value)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


_CSS = """
:root {
  --bg: #07090c;
  --surface: #101318;
  --surface-2: #151a21;
  --border: #27303a;
  --fg: #e9edf2;
  --muted: #8d98a7;
  --accent: #58a6ff;
  --ok: #72c090;
  --warn: #d6a84f;
  --idle: #66717f;
}
* { box-sizing: border-box; }
html, body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--fg); }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 14px; line-height: 1.45; }
.page { max-width: 1440px; margin: 0 auto; padding: 18px; }
.top { display: flex; justify-content: space-between; gap: 16px; align-items: center; border-bottom: 1px solid var(--border); padding-bottom: 12px; }
nav { display: flex; gap: 14px; }
a { color: var(--accent); text-decoration: none; }
.eyebrow, .mono, th { font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }
.eyebrow { color: var(--muted); font-size: 11px; letter-spacing: .12em; text-transform: uppercase; }
h1 { margin: 6px 0 4px; font-size: 34px; line-height: 1.05; letter-spacing: 0; }
p { margin: 0; color: var(--muted); }
.summary { display: grid; grid-template-columns: minmax(260px, 1fr) minmax(420px, 620px); gap: 18px; padding: 22px 0; align-items: end; }
.kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }
.broker-kpis { padding: 12px; }
.stat, .panel { background: var(--surface); border: 1px solid var(--border); }
.stat { padding: 12px; min-height: 72px; }
.stat-value { font-size: 24px; font-weight: 700; }
.stat-label { color: var(--muted); font-size: 10px; letter-spacing: .12em; text-transform: uppercase; }
.grid { display: grid; gap: 14px; }
.two { grid-template-columns: minmax(0, 1.2fr) minmax(0, .8fr); }
.panel { margin-bottom: 14px; overflow: hidden; }
.panel-head { display: flex; justify-content: space-between; gap: 10px; padding: 12px 14px; border-bottom: 1px solid var(--border); font-weight: 700; }
.panel-head small { color: var(--muted); font-weight: 400; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 10px 12px; border-bottom: 1px solid var(--border); text-align: left; vertical-align: top; }
th { color: var(--muted); font-size: 10px; letter-spacing: .1em; text-transform: uppercase; }
tr:last-child td { border-bottom: 0; }
.muted { color: var(--muted); }
.strong { color: var(--fg); font-weight: 700; }
.empty { color: var(--muted); text-align: center; padding: 28px; }
.pip { display: inline-block; width: 7px; height: 7px; border-radius: 999px; margin-right: 8px; vertical-align: 1px; }
.pip-ok { background: var(--ok); box-shadow: 0 0 10px var(--ok); }
.pip-warn { background: var(--warn); box-shadow: 0 0 10px var(--warn); }
.pip-idle { background: var(--idle); }
pre { margin: 0; padding: 14px; white-space: pre-wrap; color: #dce4ee; background: var(--surface-2); overflow: auto; }
@media (max-width: 900px) {
  .summary, .two { grid-template-columns: 1fr; }
  .kpis { grid-template-columns: repeat(2, 1fr); }
  table { font-size: 12px; }
}
"""
