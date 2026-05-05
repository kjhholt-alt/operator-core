"""Inline HTML templates for the Operator Core local ops page.

No Jinja, no framework - just a module-level render function so the
server is dependency-free and can render offline.

Aesthetic matches operator-site: tactical / Palantir. Dark bg, steel-blue
accents, status pips, mono typography, corner brackets. 100% self-contained
(no external CSS or fonts).
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


def _pip(status: str) -> str:
    cls = {
        "ok": "pip-ok",
        "green": "pip-ok",
        "complete": "pip-ok",
        "warn": "pip-warn",
        "yellow": "pip-warn",
        "pending": "pip-warn",
        "running": "pip-warn",
        "failed": "pip-alert",
        "red": "pip-alert",
        "tripped": "pip-alert",
        "error": "pip-alert",
    }.get(str(status).lower(), "pip-idle")
    return f'<span class="pip {cls}"></span>'


def _stat(label: str, value: Any, *, mono: bool = True) -> str:
    val_cls = "stat-val mono" if mono else "stat-val"
    return f'<div class="stat"><div class="stat-label">{_html_escape(label)}</div><div class="{val_cls}">{_html_escape(value)}</div></div>'


def render_ops_page(status: dict[str, Any], jobs: list[dict[str, Any]]) -> str:
    """Render the full ops dashboard from a status dict + jobs list.

    `status` is expected to be v2-shaped (see `utils.status.load_or_default`).
    `jobs` is a list of dicts with id/action/status/project/cost_usd/updated_at
    keys - typically JobStore.list_jobs() mapped to dicts.
    """
    daemon = status.get("daemon") or {}
    deploy_health = status.get("deploy_health") or {}
    hook_blocks = status.get("hook_blocks_recent") or []
    jobs_recent = status.get("jobs_recent") or []
    cost_today = status.get("cost_today_usd") or 0.0
    risk_tripped = status.get("risk_tripped") or False
    unread = status.get("discord_unread") or 0

    # --- Jobs table (last 20) ---
    jobs_html_rows = []
    for j in jobs[:20]:
        status_str = j.get("status") or "-"
        jobs_html_rows.append(
            "<tr>"
            f'<td class="mono muted">{_html_escape(j.get("id"))}</td>'
            f'<td class="mono">{_pip(status_str)}{_html_escape(j.get("action"))}</td>'
            f'<td class="mono muted">{_html_escape(status_str)}</td>'
            f'<td class="mono">{_html_escape(j.get("project") or "-")}</td>'
            f'<td class="mono muted right">${float(j.get("cost_usd") or 0):.2f}</td>'
            f'<td class="mono muted right">{_html_escape(j.get("updated_at") or "-")}</td>'
            "</tr>"
        )
    jobs_rows = (
        "\n".join(jobs_html_rows)
        or '<tr><td colspan="6" class="empty">no jobs yet</td></tr>'
    )

    # --- Deploy health ---
    deploy_html_rows = []
    for project, health in sorted(deploy_health.items()):
        deploy_html_rows.append(
            "<tr>"
            f'<td class="mono">{_pip(health)}{_html_escape(project)}</td>'
            f'<td class="mono muted">{_html_escape(health)}</td>'
            "</tr>"
        )
    deploy_rows = (
        "\n".join(deploy_html_rows)
        or '<tr><td colspan="2" class="empty">no deploy health data</td></tr>'
    )

    # --- Hook blocks ---
    hook_html_rows = []
    for h in hook_blocks:
        hook_html_rows.append(
            "<tr>"
            f'<td class="mono muted">{_html_escape(h.get("ts"))}</td>'
            f'<td class="mono">{_html_escape(h.get("reason"))}</td>'
            f'<td class="mono muted">{_html_escape(h.get("command"))}</td>'
            "</tr>"
        )
    hook_rows = (
        "\n".join(hook_html_rows)
        or '<tr><td colspan="3" class="empty">no hook blocks in recent window</td></tr>'
    )

    # --- Discord recent ---
    recent_html_rows = []
    for j in jobs_recent:
        recent_html_rows.append(
            "<tr>"
            f'<td class="mono muted">{_html_escape(j.get("id"))}</td>'
            f'<td class="mono">{_pip(j.get("status"))}{_html_escape(j.get("action"))}</td>'
            f'<td class="mono muted">{_html_escape(j.get("status"))}</td>'
            f'<td class="mono">{_html_escape(j.get("project") or "-")}</td>'
            f'<td class="mono muted right">${float(j.get("cost_usd") or 0):.2f}</td>'
            "</tr>"
        )
    recent_rows = (
        "\n".join(recent_html_rows)
        or '<tr><td colspan="5" class="empty">no recent discord/job traffic</td></tr>'
    )

    # --- Risk banner ---
    risk_banner = (
        '<div class="banner banner-alert"><span class="pip pip-alert"></span>RISK TRIPPED &mdash; AUTO-MERGE GATE DENIED</div>'
        if risk_tripped
        else '<div class="banner banner-ok"><span class="pip pip-ok"></span>RISK GATE OK</div>'
    )

    uptime = int(daemon.get("uptime_sec") or 0)
    uptime_fmt = _format_uptime(uptime)

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OPERATOR // OPS</title>
<style>
{_CSS}
</style>
</head>
<body>
<div class="page">
  <header class="top">
    <div class="eyebrow"><span class="pip pip-ok scan-pulse"></span>SYS.ONLINE &middot; OPERATOR // v0.1.0 &middot; NODE // OPERATOR-PRIMARY</div>
    <div class="eyebrow right muted">uptime {uptime_fmt}</div>
  </header>

  <h1>OPERATOR <span class="accent">//</span> OPS</h1>
  <p class="muted">Local control surface. 127.0.0.1 only. Palantir-tier read-only.</p>
  <p class="muted">
    <a href="/ops">/ops</a> &middot;
    <a href="/gate-review">/gate-review</a> &middot;
    <a href="/metrics">/metrics</a> &middot;
    <a href="/health">/health</a>
  </p>

  {risk_banner}

  <section class="panel bracket-corners">
    <div class="panel-head">
      <div class="eyebrow">DAEMON</div>
    </div>
    <div class="grid grid-4">
      {_stat("PID", daemon.get("pid") or "-")}
      {_stat("STARTED", daemon.get("started_at") or "-")}
      {_stat("UPTIME", uptime_fmt)}
      {_stat("SCHEMA", "v" + str(status.get("schema_version") or 1))}
    </div>
  </section>

  <section class="panel bracket-corners">
    <div class="panel-head">
      <div class="eyebrow">COSTS &middot; TRAFFIC</div>
    </div>
    <div class="grid grid-4">
      {_stat("24H COST", f"${float(cost_today):.2f}")}
      {_stat("DISCORD UNREAD", unread)}
      {_stat("RECENT JOBS", len(jobs_recent))}
      {_stat("TRACKED DEPLOYS", len(deploy_health))}
    </div>
  </section>

  <section class="panel bracket-corners">
    <div class="panel-head">
      <div class="eyebrow">JOB LEDGER &middot; LAST 20</div>
    </div>
    <table>
      <thead><tr>
        <th>JOB_ID</th><th>ACTION</th><th>STATUS</th><th>PROJECT</th>
        <th class="right">COST</th><th class="right">UPDATED</th>
      </tr></thead>
      <tbody>
      {jobs_rows}
      </tbody>
    </table>
  </section>

  <section class="two-col">
    <div class="panel bracket-corners">
      <div class="panel-head"><div class="eyebrow">DEPLOY HEALTH</div></div>
      <table>
        <thead><tr><th>PROJECT</th><th>HEALTH</th></tr></thead>
        <tbody>
        {deploy_rows}
        </tbody>
      </table>
    </div>

    <div class="panel bracket-corners">
      <div class="panel-head"><div class="eyebrow">HOOK BLOCKS &middot; RECENT</div></div>
      <table>
        <thead><tr><th>TS</th><th>REASON</th><th>COMMAND</th></tr></thead>
        <tbody>
        {hook_rows}
        </tbody>
      </table>
    </div>
  </section>

  <section class="panel bracket-corners">
    <div class="panel-head">
      <div class="eyebrow">DISCORD &middot; RECENT TRAFFIC</div>
    </div>
    <table>
      <thead><tr>
        <th>JOB_ID</th><th>ACTION</th><th>STATUS</th><th>PROJECT</th>
        <th class="right">COST</th>
      </tr></thead>
      <tbody>
      {recent_rows}
      </tbody>
    </table>
  </section>

  <footer class="foot muted mono">
    OPERATOR // v0.1.0 &middot; 127.0.0.1 &middot; <a href="/metrics">/metrics</a> &middot; <a href="/health">/health</a>
  </footer>
</div>
</body>
</html>"""


def _format_uptime(sec: int) -> str:
    if sec < 60:
        return f"{sec}s"
    m = sec // 60
    if m < 60:
        return f"{m}m"
    h = m // 60
    if h < 24:
        return f"{h}h {m % 60}m"
    d = h // 24
    return f"{d}d {h % 24}h"


_CSS = """
  :root {
    --bg: #05060a;
    --surface: #0b0d13;
    --surface-2: #10131c;
    --border: #1c2030;
    --border-bright: #2a3148;
    --fg: #e6e9f0;
    --muted: #8b91a4;
    --accent: #5a9bff;
    --accent-bright: #84b4ff;
    --warn: #d4a14a;
    --alert: #e57373;
    --ok: #7cc49b;
  }
  * { border-color: var(--border); box-sizing: border-box; }
  html, body { margin: 0; padding: 0; background: var(--bg); color: var(--fg); min-height: 100vh; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Oxygen, Ubuntu, sans-serif;
    font-size: 14px; line-height: 1.5;
    background-image: radial-gradient(circle at 1px 1px, rgba(90,155,255,0.06) 1px, transparent 0);
    background-size: 24px 24px;
  }
  body::before {
    content: ""; position: fixed; inset: 0; pointer-events: none; z-index: 1;
    background: repeating-linear-gradient(0deg,
      rgba(255,255,255,0.01) 0, rgba(255,255,255,0.01) 1px,
      transparent 1px, transparent 3px);
    mix-blend-mode: overlay;
  }
  .page { max-width: 1280px; margin: 0 auto; padding: 1.5rem; position: relative; z-index: 2; }
  .mono { font-family: ui-monospace, "JetBrains Mono", "Fira Code", SFMono-Regular, Consolas, monospace; font-size: 12px; }
  .muted { color: var(--muted); }
  .accent { color: var(--accent); }
  .right { text-align: right; }
  .top {
    border-bottom: 1px solid var(--border); background: var(--surface);
    padding: 0.5rem 1rem; margin: -1.5rem -1.5rem 1.5rem;
    display: flex; justify-content: space-between; align-items: center;
    font-family: ui-monospace, "JetBrains Mono", monospace; font-size: 10px;
    letter-spacing: 0.18em; text-transform: uppercase; color: var(--muted);
  }
  .eyebrow {
    font-family: ui-monospace, "JetBrains Mono", monospace;
    font-size: 10px; font-weight: 500; letter-spacing: 0.18em;
    text-transform: uppercase; color: var(--muted);
    display: flex; align-items: center; gap: 0.5rem;
  }
  h1 { font-size: 2rem; font-weight: 700; letter-spacing: -0.01em; margin: 0 0 0.25rem; line-height: 1.1; }
  h1 .accent { color: var(--accent); }
  p { margin: 0 0 1rem; }

  .pip {
    display: inline-block; width: 6px; height: 6px; border-radius: 50%;
    box-shadow: 0 0 8px currentColor; margin-right: 0.5rem;
    vertical-align: middle;
  }
  .pip-ok { background: var(--ok); color: var(--ok); }
  .pip-warn { background: var(--warn); color: var(--warn); }
  .pip-alert { background: var(--alert); color: var(--alert); }
  .pip-idle { background: var(--muted); color: transparent; box-shadow: none; }
  @keyframes scan-pulse { 0%, 100% { opacity: 0.3; } 50% { opacity: 1; } }
  .scan-pulse { animation: scan-pulse 2s ease-in-out infinite; }

  .panel {
    background: var(--surface); border: 1px solid var(--border);
    padding: 1.25rem; margin: 1rem 0; position: relative;
  }
  .panel::before {
    content: ""; position: absolute; top: -1px; left: -1px; right: -1px; height: 1px;
    background: linear-gradient(90deg, transparent 0%, var(--accent) 20%, var(--accent) 80%, transparent 100%);
    opacity: 0.4;
  }
  .bracket-corners::before,
  .bracket-corners::after {
    content: ""; position: absolute; width: 12px; height: 12px;
    border-color: var(--accent); border-style: solid; opacity: 0.6;
  }
  .bracket-corners::before {
    top: -1px; left: -1px; border-width: 1px 0 0 1px; background: none;
    height: 12px; right: auto;
  }
  .bracket-corners::after {
    bottom: -1px; right: -1px; border-width: 0 1px 1px 0;
  }
  .panel-head { margin-bottom: 1rem; }

  .grid { display: grid; gap: 0.8rem; }
  .grid-4 { grid-template-columns: repeat(4, 1fr); }
  @media (max-width: 720px) { .grid-4 { grid-template-columns: repeat(2, 1fr); } }
  .stat { border: 1px solid var(--border-bright); padding: 0.75rem 1rem; background: var(--surface-2); }
  .stat-label { font-family: ui-monospace, "JetBrains Mono", monospace;
    font-size: 10px; letter-spacing: 0.15em; text-transform: uppercase; color: var(--muted);
    margin-bottom: 0.35rem;
  }
  .stat-val { font-size: 1.25rem; font-weight: 600; color: var(--fg); word-break: break-all; }

  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  .two-col .panel { margin: 0; }
  @media (max-width: 880px) { .two-col { grid-template-columns: 1fr; } }

  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-weight: 500; text-transform: uppercase; letter-spacing: 0.12em; font-size: 10px;
    font-family: ui-monospace, "JetBrains Mono", monospace;
  }
  td.empty, .empty { color: var(--muted); opacity: 0.6; font-style: italic; text-align: center; padding: 1.5rem; }
  tbody tr { transition: background 120ms ease; }
  tbody tr:hover { background: var(--surface-2); }

  .banner {
    display: flex; align-items: center; gap: 0.5rem;
    padding: 0.5rem 1rem; margin: 1rem 0; border: 1px solid var(--border-bright);
    font-family: ui-monospace, "JetBrains Mono", monospace;
    font-size: 11px; letter-spacing: 0.12em; text-transform: uppercase;
  }
  .banner-ok { background: rgba(124,196,155,0.05); border-color: var(--ok); color: var(--ok); }
  .banner-alert { background: rgba(229,115,115,0.08); border-color: var(--alert); color: var(--alert); }

  a { color: var(--accent); text-decoration: none; }
  a:hover { color: var(--accent-bright); text-decoration: underline; }

  .foot {
    margin-top: 2rem; padding-top: 1rem; border-top: 1px solid var(--border);
    font-size: 11px; letter-spacing: 0.1em;
  }
"""
