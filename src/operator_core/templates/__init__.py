"""Inline HTML templates for the Operator V3 local ops page.

No Jinja, no framework — just a single module-level render function so the
server is dependency-free and can render offline.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

_TEMPLATE_DIR = Path(__file__).parent


def _read(name: str) -> str:
    return (_TEMPLATE_DIR / name).read_text(encoding="utf-8")


def _html_escape(value: Any) -> str:
    s = "" if value is None else str(value)
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _row(cells: list[Any]) -> str:
    return "<tr>" + "".join(f"<td>{_html_escape(c)}</td>" for c in cells) + "</tr>"


def render_ops_page(status: dict[str, Any], jobs: list[dict[str, Any]]) -> str:
    """Render the full ops dashboard from a status dict + jobs list.

    `status` is expected to be v2-shaped (see `utils.status.load_or_default`).
    `jobs` is a list of dicts with id/action/status/project/cost_usd/updated_at
    keys — typically JobStore.list_jobs() mapped to dicts.
    """
    daemon = status.get("daemon") or {}
    deploy_health = status.get("deploy_health") or {}
    hook_blocks = status.get("hook_blocks_recent") or []
    jobs_recent = status.get("jobs_recent") or []
    cost_today = status.get("cost_today_usd") or 0.0
    risk_tripped = status.get("risk_tripped") or False
    unread = status.get("discord_unread") or 0

    jobs_rows = "\n".join(
        _row(
            [
                j.get("id"),
                j.get("action"),
                j.get("status"),
                j.get("project") or "-",
                f"${float(j.get('cost_usd') or 0):.2f}",
                j.get("updated_at") or "-",
            ]
        )
        for j in jobs[:20]
    ) or '<tr><td colspan="6" class="empty">no jobs yet</td></tr>'

    deploy_rows = "\n".join(
        _row([project, health]) for project, health in sorted(deploy_health.items())
    ) or '<tr><td colspan="2" class="empty">no deploy health data</td></tr>'

    hook_rows = "\n".join(
        _row([h.get("ts"), h.get("reason"), h.get("command")]) for h in hook_blocks
    ) or '<tr><td colspan="3" class="empty">no hook blocks in recent window</td></tr>'

    recent_rows = "\n".join(
        _row(
            [
                j.get("id"),
                j.get("action"),
                j.get("status"),
                j.get("project") or "-",
                f"${float(j.get('cost_usd') or 0):.2f}",
            ]
        )
        for j in jobs_recent
    ) or '<tr><td colspan="5" class="empty">no recent discord/job traffic</td></tr>'

    risk_banner = (
        '<div class="banner tripped">RISK TRIPPED — auto-merge gate denied</div>'
        if risk_tripped
        else '<div class="banner ok">risk gate: ok</div>'
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Operator V3 — ops</title>
<style>
  body {{ background: #000; color: #33ff66; font-family: 'Consolas','Courier New',monospace; margin: 0; padding: 1.2rem 1.5rem; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 1rem 0; letter-spacing: 0.08em; }}
  h2 {{ font-size: 1rem; margin: 1.2rem 0 0.4rem; color: #66ff99; border-bottom: 1px solid #225533; padding-bottom: 0.2rem; letter-spacing: 0.05em; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th, td {{ text-align: left; padding: 0.25rem 0.6rem; border-bottom: 1px solid #113322; vertical-align: top; }}
  th {{ color: #99ffaa; font-weight: normal; text-transform: uppercase; letter-spacing: 0.05em; }}
  td.empty, .empty {{ color: #225533; font-style: italic; }}
  .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.8rem; margin-bottom: 0.5rem; }}
  .stat {{ border: 1px solid #225533; padding: 0.6rem 0.8rem; }}
  .stat .label {{ font-size: 0.65rem; text-transform: uppercase; color: #99ffaa; letter-spacing: 0.1em; }}
  .stat .value {{ font-size: 1.4rem; color: #66ff99; margin-top: 0.2rem; }}
  .banner {{ padding: 0.4rem 0.8rem; margin: 0.5rem 0 1rem; border: 1px solid #225533; }}
  .banner.ok {{ color: #66ff99; }}
  .banner.tripped {{ background: #330000; color: #ff6666; border-color: #661111; }}
  a {{ color: #66ff99; }}
</style>
</head>
<body>
<h1>OPERATOR V3 — OPS</h1>
{risk_banner}

<h2>Daemon</h2>
<div class="grid">
  <div class="stat"><div class="label">pid</div><div class="value">{_html_escape(daemon.get("pid") or "-")}</div></div>
  <div class="stat"><div class="label">started</div><div class="value">{_html_escape(daemon.get("started_at") or "-")}</div></div>
  <div class="stat"><div class="label">uptime_sec</div><div class="value">{_html_escape(int(daemon.get("uptime_sec") or 0))}</div></div>
  <div class="stat"><div class="label">schema</div><div class="value">v{_html_escape(status.get("schema_version") or 1)}</div></div>
</div>

<h2>Costs</h2>
<div class="grid">
  <div class="stat"><div class="label">today</div><div class="value">${float(cost_today):.2f}</div></div>
  <div class="stat"><div class="label">discord unread</div><div class="value">{_html_escape(unread)}</div></div>
  <div class="stat"><div class="label">recent jobs</div><div class="value">{len(jobs_recent)}</div></div>
  <div class="stat"><div class="label">tracked deploys</div><div class="value">{len(deploy_health)}</div></div>
</div>

<h2>Jobs (last 20)</h2>
<table>
<thead><tr><th>id</th><th>action</th><th>status</th><th>project</th><th>cost</th><th>updated</th></tr></thead>
<tbody>
{jobs_rows}
</tbody>
</table>

<h2>Deploys</h2>
<table>
<thead><tr><th>project</th><th>health</th></tr></thead>
<tbody>
{deploy_rows}
</tbody>
</table>

<h2>Hooks (recent blocks)</h2>
<table>
<thead><tr><th>ts</th><th>reason</th><th>command</th></tr></thead>
<tbody>
{hook_rows}
</tbody>
</table>

<h2>Discord (recent)</h2>
<table>
<thead><tr><th>id</th><th>action</th><th>status</th><th>project</th><th>cost</th></tr></thead>
<tbody>
{recent_rows}
</tbody>
</table>

</body>
</html>"""
