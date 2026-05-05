"""HTTP route for the cut-over dashboard at /cut-over.

One HTML view of every product the promoter is tracking: current
ready-state, streak length, promoted timestamp + PR url, and (if
within the rollback window) any active regression alert.

This is the read-side companion to ``cut_over_promoter`` and
``cutover_rollback_watch`` -- so you can glance once and see whether
the cut-over loop is healthy across all products.
"""

from __future__ import annotations

import html
import json
import os
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .http_server import register_extra_route


def register_cutover_routes() -> None:
    def _get_cutover(handler: Any, body: Any) -> None:
        from . import cutover_streak, outreach_audit

        # Pull live audit-report snapshot so the page reflects current
        # match% / pending without a 30-min recipe lag.
        try:
            paths = outreach_audit.default_audit_paths()
            summaries = outreach_audit.collect(paths)
        except (FileNotFoundError, OSError):
            summaries = []
        by_product = {s.product: s for s in summaries}

        try:
            window_h = int(os.environ.get("OPERATOR_ROLLBACK_WINDOW_HOURS", "48"))
        except ValueError:
            window_h = 48
        try:
            threshold = float(os.environ.get("OPERATOR_CUTOVER_MATCH_PCT", "95.0"))
        except ValueError:
            threshold = 95.0
        try:
            streak_threshold_s = int(os.environ.get("OPERATOR_CUTOVER_STREAK_SECONDS", str(24 * 3600)))
        except ValueError:
            streak_threshold_s = 24 * 3600

        rows = []
        for streak in cutover_streak.list_all():
            audit = by_product.get(streak.product)
            sec = cutover_streak.streak_seconds(streak.product)
            row = {
                "product": streak.product,
                "ready_now": bool(audit and audit.cutover_ready(threshold)),
                "match_pct": (audit.match_pct if audit else None),
                "would_block_new": (audit.would_block_new if audit else 0),
                "would_allow_new": (audit.would_allow_new if audit else 0),
                "pending": (audit.triage_pending if audit else 0),
                "streak_seconds": int(sec),
                "streak_threshold_seconds": streak_threshold_s,
                "promoted_ts": streak.promoted_ts,
                "promoted_pr_url": streak.promoted_pr_url,
                "in_rollback_window": _in_window(streak.promoted_ts, window_h),
            }
            row["regression"] = _regression(row, threshold)
            rows.append(row)

        rows.sort(key=lambda r: (
            0 if r["regression"] else (1 if r["promoted_ts"] else 2),
            -r["streak_seconds"],
            r["product"],
        ))

        handler._html(200, _render(rows, window_h, threshold, streak_threshold_s))

    def _get_cutover_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        from . import cutover_streak

        return 200, {
            "products": [asdict(s) for s in cutover_streak.list_all()],
            "count": len(cutover_streak.list_all()),
        }

    register_extra_route("GET", "/cut-over", _get_cutover)
    register_extra_route("GET", "/cut-over.json", _get_cutover_json)


def _in_window(promoted_ts: str | None, window_h: int) -> bool:
    if not promoted_ts:
        return False
    try:
        dt = datetime.fromisoformat(promoted_ts.replace("Z", "+00:00"))
    except ValueError:
        return False
    return datetime.now(timezone.utc) - dt <= timedelta(hours=window_h)


def _regression(row: dict[str, Any], threshold: float) -> list[str]:
    """Return regression reasons IFF this product is in the rollback window."""
    if not row["in_rollback_window"]:
        return []
    reasons = []
    mp = row.get("match_pct")
    if mp is None:
        reasons.append("no_audit_data")
    elif mp < threshold:
        reasons.append(f"match_pct_{mp:.1f}<{threshold:.0f}")
    if row.get("would_allow_new", 0) > 0:
        reasons.append(f"would_allow_new_{row['would_allow_new']}")
    if row.get("pending", 0) > 0:
        reasons.append(f"pending_{row['pending']}")
    return reasons


def _fmt_seconds(s: int) -> str:
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h{(s % 3600) // 60}m"
    d = s // 86400
    h = (s % 86400) // 3600
    return f"{d}d{h}h"


def _render(rows: list[dict[str, Any]], window_h: int, threshold: float, streak_threshold_s: int) -> str:
    e = html.escape

    if not rows:
        body = "<p class=empty>No products tracked yet. Configure <code>OPERATOR_CUTOVER_PROMOTER_CONFIG</code> + run the promoter once.</p>"
    else:
        cards = []
        for r in rows:
            mp = r.get("match_pct")
            mp_str = f"{mp:.1f}%" if mp is not None else "n/a"
            promoted = ""
            if r["promoted_ts"]:
                pr_link = f' <a href="{e(r["promoted_pr_url"] or "")}" target=_blank>PR</a>' if r["promoted_pr_url"] else ""
                promoted = f'<div class=meta>Promoted: <code>{e(r["promoted_ts"])}</code>{pr_link}</div>'
            else:
                promoted = '<div class=meta muted>Not yet promoted.</div>'

            streak_pct = min(100, int(100 * r["streak_seconds"] / r["streak_threshold_seconds"])) if r["streak_threshold_seconds"] else 0
            streak_block = (
                f'<div class=meta>Streak: <code>{_fmt_seconds(r["streak_seconds"])}</code> '
                f'/ {_fmt_seconds(r["streak_threshold_seconds"])} '
                f'<span class="bar"><span class="bar-fill" style="width:{streak_pct}%"></span></span></div>'
            ) if r["streak_seconds"] > 0 or not r["promoted_ts"] else ""

            regression_block = ""
            if r["regression"]:
                regression_block = (
                    '<div class=alert>:rotating_light: REGRESSION '
                    + ", ".join(e(x) for x in r["regression"]) + '</div>'
                )
            elif r["in_rollback_window"]:
                regression_block = '<div class=ok>Within rollback window, no regression.</div>'

            ready_class = "ready" if r["ready_now"] else "not-ready"
            ready_label = "READY" if r["ready_now"] else "NOT READY"
            cards.append(f"""
<div class="card {ready_class}">
  <h3>{e(r["product"])} <span class=badge>{ready_label}</span></h3>
  <div class=meta>
    match: <code>{mp_str}</code>
    · would_block_new: <code>{r["would_block_new"]}</code>
    · would_allow_new: <code>{r["would_allow_new"]}</code>
    · pending: <code>{r["pending"]}</code>
  </div>
  {streak_block}
  {promoted}
  {regression_block}
</div>
""")
        body = '<div class=cards>' + "\n".join(cards) + '</div>'

    return f"""<!doctype html>
<html><head><meta charset=utf-8>
<title>Cut-over dashboard</title>
<style>
  body {{ font-family: ui-sans-serif, system-ui, sans-serif; max-width: 1100px;
         margin: 24px auto; padding: 0 20px; color: #1d1d1f; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .lede {{ color: #555; margin-bottom: 24px; font-size: 14px; }}
  .lede code {{ background: #f4f4f6; padding: 1px 5px; border-radius: 3px; }}
  .cards {{ display: grid; gap: 12px; }}
  .card {{ border: 1px solid #e2e2e8; border-radius: 8px; padding: 14px 18px;
          background: #fff; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
  .card.not-ready {{ border-color: #f5c2c2; background: #fff8f8; }}
  .card.ready {{ border-color: #b9e7b9; background: #f6fff6; }}
  .card h3 {{ margin: 0 0 4px; font-size: 16px; display: flex; gap: 10px; align-items: center; }}
  .badge {{ font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 999px;
           background: #e2e2e8; color: #1d1d1f; }}
  .ready .badge {{ background: #b9e7b9; color: #1a4d1a; }}
  .not-ready .badge {{ background: #f5c2c2; color: #6e1717; }}
  .meta {{ color: #555; font-size: 13px; margin: 4px 0; }}
  .meta code {{ background: #f4f4f6; padding: 1px 5px; border-radius: 3px; }}
  .meta.muted {{ color: #888; font-style: italic; }}
  .bar {{ display: inline-block; width: 120px; height: 8px; background: #e8e8ee; border-radius: 4px; vertical-align: middle; margin-left: 6px; overflow: hidden; }}
  .bar-fill {{ display: block; height: 100%; background: #6aa3ff; }}
  .alert {{ margin-top: 8px; padding: 8px 12px; background: #ffe5e5; border: 1px solid #f5a3a3; border-radius: 4px; color: #6e1717; font-size: 13px; }}
  .ok {{ margin-top: 8px; padding: 6px 10px; background: #f0fff0; border-radius: 4px; color: #1a4d1a; font-size: 12px; }}
  .empty {{ color: #777; font-style: italic; }}
  p.nav {{ font-size: 13px; color: #555; }}
  p.nav a {{ margin-right: 12px; }}
</style>
</head>
<body>
<h1>Cut-over dashboard</h1>
<p class=lede>One row per tracked product. Threshold <code>{threshold:.1f}%</code> match, streak target <code>{_fmt_seconds(streak_threshold_s)}</code>, rollback window <code>{window_h}h</code>.</p>
<p class=nav><a href=/ops>/ops</a> <a href=/gate-review>/gate-review</a> <a href=/cut-over>/cut-over</a> <a href=/metrics>/metrics</a></p>

{body}
</body></html>
"""
