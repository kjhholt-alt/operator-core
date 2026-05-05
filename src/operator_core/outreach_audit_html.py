"""Render the gate_audit cut-over dashboard as a single static HTML file.

Powers `operator outreach audit-dashboard`. No JS framework, no build
step -- just CDN Tailwind + a tiny <script> block that reads inline JSON
the renderer writes alongside the dashboard.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from . import outreach_audit

_HEAD = """\
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Sender Gate Cut-Over Audit</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-zinc-950 text-zinc-100 antialiased font-sans">
"""

_FOOT = """\
<script>
  // Allow ?json=1 to dump the raw payload for any downstream tooling.
  const params = new URLSearchParams(location.search);
  if (params.get("json") === "1") {
    document.body.innerHTML = "<pre class='p-6 text-xs'>" +
      JSON.stringify(window.__AUDIT__, null, 2) + "</pre>";
  }
</script>
</body>
</html>
"""


def _bar(label: str, value: int, total: int, color: str) -> str:
    pct = (value / total * 100.0) if total else 0.0
    return (
        f'<div class="flex items-center gap-3 text-sm">'
        f'<div class="w-44 truncate text-zinc-300">{label}</div>'
        f'<div class="flex-1 bg-zinc-900 rounded-sm h-2 overflow-hidden">'
        f'<div class="h-full {color}" style="width:{pct:.1f}%"></div></div>'
        f'<div class="w-20 text-right tabular-nums text-zinc-400">{value}'
        f' <span class="text-zinc-600">({pct:.1f}%)</span></div>'
        f'</div>'
    )


def _product_card(s: outreach_audit.ProductSummary, threshold: float) -> str:
    ready = s.cutover_ready(threshold)
    badge_color = "bg-emerald-500/15 text-emerald-300 border-emerald-500/30" if ready \
        else "bg-rose-500/15 text-rose-300 border-rose-500/30"
    badge_text = "CUT-OVER READY" if ready else "NOT READY"
    samples_html = ""
    if s.sample_would_block:
        samples_html += '<div class="mt-4 text-xs">'
        samples_html += '<div class="text-zinc-500 uppercase tracking-wider mb-1">would block (gate stricter)</div>'
        samples_html += '<ul class="space-y-1">'
        for sample in s.sample_would_block[:5]:
            bn = (sample.get("business_name") or "(no name)")[:60]
            label = (sample.get("gate_block_label") or "(no label)")[:60]
            samples_html += (
                f'<li class="flex items-start gap-2 text-zinc-300">'
                f'<span class="text-rose-400 mt-px">block</span>'
                f'<span class="font-medium">{bn}</span>'
                f'<span class="text-zinc-500 text-[11px] ml-auto truncate">{label}</span>'
                f'</li>'
            )
        samples_html += '</ul></div>'
    if s.sample_would_allow:
        samples_html += '<div class="mt-4 text-xs">'
        samples_html += '<div class="text-amber-400 uppercase tracking-wider mb-1 font-medium">would allow (DANGER -- review each)</div>'
        samples_html += '<ul class="space-y-1">'
        for sample in s.sample_would_allow[:5]:
            bn = (sample.get("business_name") or "(no name)")[:60]
            reason = (sample.get("legacy_block_reason") or "(no reason)")[:60]
            samples_html += (
                f'<li class="flex items-start gap-2 text-zinc-300">'
                f'<span class="text-amber-400 mt-px">allow</span>'
                f'<span class="font-medium">{bn}</span>'
                f'<span class="text-zinc-500 text-[11px] ml-auto truncate">legacy: {reason}</span>'
                f'</li>'
            )
        samples_html += '</ul></div>'
    return (
        f'<section class="rounded-md border border-zinc-800 bg-zinc-900/50 p-5">'
        f'  <div class="flex items-baseline justify-between mb-4">'
        f'    <h2 class="text-lg font-semibold tracking-tight">{s.product}</h2>'
        f'    <span class="text-[10px] uppercase tracking-wider px-2 py-0.5 rounded-sm border {badge_color}">{badge_text}</span>'
        f'  </div>'
        f'  <div class="space-y-1.5">'
        f'    {_bar("match", s.match, s.total, "bg-emerald-500/70")}'
        f'    {_bar("would_block_new", s.would_block_new, s.total, "bg-sky-500/70")}'
        f'    {_bar("would_allow_new", s.would_allow_new, s.total, "bg-amber-500/70")}'
        f'    {_bar("both_block_diff_reason", s.both_block_diff_reason, s.total, "bg-zinc-500/70")}'
        f'  </div>'
        f'  {samples_html}'
        f'  <div class="mt-4 text-[11px] text-zinc-500">'
        f'    {s.total} events &middot; match% required &ge; {threshold:.1f}'
        f'  </div>'
        f'</section>'
    )


def render(summaries: List[outreach_audit.ProductSummary],
           threshold: float = 95.0,
           since_label: Optional[str] = None) -> str:
    overall_ready = outreach_audit.overall_ready(summaries, threshold)
    overall_text = "ALL PRODUCTS READY" if overall_ready else "NOT READY TO FLIP"
    overall_color = "text-emerald-400" if overall_ready else "text-rose-400"
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    cards = "\n".join(_product_card(s, threshold) for s in summaries) if summaries else \
        '<div class="text-zinc-400">No gate_audit events found. Have you turned on shadow mode? See <code>CUTOVER.md</code>.</div>'

    raw_payload = json.dumps({
        "generated_at": generated,
        "threshold_pct": threshold,
        "products": [
            {
                "product": s.product,
                "total": s.total,
                "match": s.match,
                "match_pct": round(s.match_pct, 2),
                "would_block_new": s.would_block_new,
                "would_allow_new": s.would_allow_new,
                "both_block_diff_reason": s.both_block_diff_reason,
                "cutover_ready": s.cutover_ready(threshold),
            }
            for s in summaries
        ],
    })

    body = f"""
<main class="max-w-5xl mx-auto px-6 py-10">
  <header class="mb-10">
    <div class="text-xs uppercase tracking-[0.2em] text-zinc-500 mb-2">Sender Gate Cut-Over</div>
    <div class="flex items-baseline justify-between gap-4">
      <h1 class="text-3xl font-semibold tracking-tight">Audit Dashboard</h1>
      <div class="text-sm {overall_color} font-medium">{overall_text}</div>
    </div>
    <div class="mt-2 text-xs text-zinc-500">
      Generated {generated}{' &middot; window: ' + since_label if since_label else ''}
    </div>
  </header>
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    {cards}
  </div>
  <footer class="mt-10 text-[11px] text-zinc-600 leading-relaxed">
    Source: <code>~/.operator/data/outreach/&lt;product&gt;/gate_audit.ndjson</code><br>
    Cut-over decision rule lives in <code>outreach-common/CUTOVER.md</code>.<br>
    Generate again with <code>operator outreach audit-dashboard --out path</code>.<br>
    Raw payload: <a href="?json=1" class="underline hover:text-zinc-400">?json=1</a>.
  </footer>
</main>
<script>window.__AUDIT__ = {raw_payload};</script>
"""
    return _HEAD + body + _FOOT


def render_to(path: Path, summaries: List[outreach_audit.ProductSummary],
              threshold: float = 95.0,
              since_label: Optional[str] = None) -> Path:
    """Atomic write: tmp + replace, mkdir parents."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(render(summaries, threshold, since_label), encoding="utf-8")
    tmp.replace(path)
    return path
