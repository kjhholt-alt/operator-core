"""HTTP routes for the Sender Gate review queue web triage UI.

Routes registered:
  GET  /gate-review              -- HTML listing of pending review items
                                    (?product=<slug> filter, ?limit=N)
  POST /gate-review/resolve      -- JSON {id, status, note?, resolved_by?}
                                    -> resolves the queue row, returns JSON
  GET  /gate-review.json         -- JSON dump of pending items (for tooling)

Auto-suppression-PR trigger:
  When ``OPERATOR_GATE_REVIEW_AUTO_SUPPRESS_PR=1`` is set, the resolve
  endpoint will, after each resolution, check whether the running tally
  of unsuppressed ``approved_gate`` items has crossed the threshold
  (default 5; override with ``OPERATOR_GATE_REVIEW_AUTO_PR_THRESHOLD``).
  If so, it queues a suppression-PR build in a background thread so the
  HTTP response stays snappy.

Why a web UI: Discord ``/op gate-review`` shows ONE item at a time and
is great for "I have a moment, look at the latest." The web view is for
"I just sat down to triage 20 items in a row" -- you see all of them
and click through.
"""

from __future__ import annotations

import html
import json
import logging
import os
import threading
from dataclasses import asdict
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

from .http_server import register_extra_route

logger = logging.getLogger("operator.gate_review_routes")


# Resolution status options shown in the UI dropdown.
RESOLVE_STATUSES = (
    ("approved_gate", "Approve gate (suppress this lead going forward)"),
    ("approved_legacy", "Approve legacy (gate was wrong)"),
    ("fix_gate", "Fix needed: gate logic"),
    ("fix_legacy", "Fix needed: legacy logic"),
    ("suppressed", "Already suppressed (housekeeping)"),
)


def register_gate_review_routes() -> None:
    """Register GET/POST /gate-review and /gate-review/resolve."""

    def _get_listing(handler: Any, body: Any) -> None:
        from . import gate_review

        # Parse query string off handler.path -- the dispatcher matched
        # the bare path but query-aware filters live here.
        parsed = urlparse(handler.path)
        params = parse_qs(parsed.query)
        product = (params.get("product") or [None])[0]
        try:
            limit = int((params.get("limit") or ["50"])[0])
        except ValueError:
            limit = 50
        limit = max(1, min(200, limit))

        items = gate_review.list_pending(product=product, limit=limit)
        summary = gate_review.triage_summary()
        html_body = _render_listing(items, summary, product=product, limit=limit)
        handler._html(200, html_body)

    def _get_listing_json(handler: Any, body: Any) -> tuple[int, dict[str, Any]]:
        from . import gate_review

        parsed = urlparse(handler.path)
        params = parse_qs(parsed.query)
        product = (params.get("product") or [None])[0]
        try:
            limit = int((params.get("limit") or ["50"])[0])
        except ValueError:
            limit = 50
        limit = max(1, min(200, limit))

        items = gate_review.list_pending(product=product, limit=limit)
        return 200, {
            "items": [asdict(i) for i in items],
            "count": len(items),
            "product_filter": product,
            "limit": limit,
        }

    def _post_resolve(handler: Any, body: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        from . import gate_review

        body = body or {}
        try:
            item_id = int(body.get("id"))
        except (TypeError, ValueError):
            return 400, {"error": "id_required", "detail": "POST body must include integer 'id'"}
        status = str(body.get("status") or "")
        if not status:
            return 400, {"error": "status_required"}
        note = body.get("note")
        resolved_by = body.get("resolved_by") or "web-ui"

        try:
            item = gate_review.resolve(item_id, status, note=note, resolved_by=resolved_by)
        except ValueError as exc:
            return 400, {"error": "resolve_failed", "detail": str(exc)}

        # Background-trigger the suppression-PR builder if configured.
        _maybe_trigger_auto_suppression_pr()

        return 200, {"ok": True, "item": asdict(item)}

    register_extra_route("GET", "/gate-review", _get_listing)
    register_extra_route("GET", "/gate-review.json", _get_listing_json)
    register_extra_route("POST", "/gate-review/resolve", _post_resolve)


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def _render_listing(items, summary, *, product: Optional[str], limit: int) -> str:
    e = html.escape

    # Triage summary header.
    summary_rows = []
    for t in summary:
        summary_rows.append(
            f'<tr><td>{e(t.product)}</td>'
            f'<td>{t.total}</td>'
            f'<td>{t.pending}</td>'
            f'<td>{t.triaged}</td>'
            f'<td>{t.triaged_pct:.1f}%</td></tr>'
        )
    summary_table = (
        "<table class=summary><thead><tr>"
        "<th>Product</th><th>Total</th><th>Pending</th><th>Triaged</th><th>Done %</th>"
        "</tr></thead><tbody>" + "".join(summary_rows) + "</tbody></table>"
    ) if summary else "<p class=empty>No review items have ever been ingested.</p>"

    # Item cards.
    if items:
        cards = "\n".join(_render_item_card(i) for i in items)
        items_block = f'<div class=cards>{cards}</div>'
    else:
        filter_msg = f" matching product=<code>{e(product)}</code>" if product else ""
        items_block = f'<p class=empty>No pending review items{filter_msg}.</p>'

    # Status options for the per-card resolve form.
    status_options_json = json.dumps([{"value": v, "label": l} for v, l in RESOLVE_STATUSES])

    title_filter = f" (product: {e(product)})" if product else ""
    return f"""<!doctype html>
<html><head><meta charset=utf-8>
<title>Sender Gate review queue{title_filter}</title>
<style>
  body {{ font-family: ui-sans-serif, system-ui, sans-serif; max-width: 1100px;
         margin: 24px auto; padding: 0 20px; color: #1d1d1f; }}
  h1 {{ font-size: 22px; margin-bottom: 4px; }}
  .lede {{ color: #555; margin-bottom: 24px; }}
  table.summary {{ border-collapse: collapse; margin: 12px 0 28px; font-size: 14px; }}
  table.summary th, table.summary td {{
       border: 1px solid #ddd; padding: 6px 12px; text-align: left;
  }}
  table.summary thead {{ background: #f7f7f9; }}
  .cards {{ display: grid; gap: 12px; }}
  .card {{ border: 1px solid #e2e2e8; border-radius: 8px; padding: 12px 16px;
          background: #fff; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
  .card h3 {{ margin: 0 0 4px; font-size: 16px; }}
  .meta {{ color: #555; font-size: 13px; margin-bottom: 8px; }}
  .meta code {{ background: #f4f4f6; padding: 1px 5px; border-radius: 3px; }}
  .agreement-would_block_new {{ color: #b00020; }}
  .agreement-would_allow_new {{ color: #b58105; }}
  .agreement-both_block_diff_reason {{ color: #4a4a8a; }}
  form.resolve {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
  form.resolve select, form.resolve input[type=text] {{
        padding: 6px 8px; border: 1px solid #ccc; border-radius: 4px;
  }}
  form.resolve input[type=text] {{ flex: 1 1 220px; }}
  form.resolve button {{ padding: 6px 14px; border: 0; border-radius: 4px;
          background: #1d6fdc; color: white; cursor: pointer; }}
  form.resolve button:hover {{ background: #1859b4; }}
  .status-msg {{ font-size: 12px; color: #1a7f1a; margin-left: 6px; }}
  .status-msg.error {{ color: #b00020; }}
  p.empty {{ color: #777; font-style: italic; }}
  .controls {{ margin-bottom: 16px; }}
  .controls label {{ font-size: 13px; color: #555; margin-right: 6px; }}
  .controls input, .controls button {{ padding: 4px 8px; }}
</style>
</head>
<body>
<h1>Sender Gate review queue{title_filter}</h1>
<p class=lede>Per-product triage summary, then pending disagreements. Resolve from here or via Discord <code>/op gate-review</code>.</p>

<h2>Per-product triage</h2>
{summary_table}

<form method=get action=/gate-review class=controls>
  <label for=product-filter>Filter by product:</label>
  <input id=product-filter name=product value="{e(product or '')}" placeholder="e.g. oe, pp, ai-ops">
  <label for=limit-filter>Limit:</label>
  <input id=limit-filter name=limit type=number min=1 max=200 value="{limit}">
  <button type=submit>Apply</button>
  <a href=/gate-review style="margin-left:10px;font-size:13px">clear</a>
</form>

<h2>Pending items ({len(items)})</h2>
{items_block}

<script>
const STATUS_OPTIONS = {status_options_json};

function buildSelect(itemId) {{
  const sel = document.createElement('select');
  sel.name = 'status';
  for (const o of STATUS_OPTIONS) {{
    const opt = document.createElement('option');
    opt.value = o.value;
    opt.textContent = o.label;
    sel.appendChild(opt);
  }}
  return sel;
}}

document.querySelectorAll('form.resolve').forEach(form => {{
  const itemId = parseInt(form.dataset.itemId, 10);
  // Inject the status dropdown (built via JS so it's always in sync with server enum).
  const slot = form.querySelector('.status-slot');
  if (slot) slot.replaceWith(buildSelect(itemId));

  form.addEventListener('submit', async (ev) => {{
    ev.preventDefault();
    const fd = new FormData(form);
    const body = {{
      id: itemId,
      status: fd.get('status'),
      note: fd.get('note') || null,
    }};
    const msg = form.querySelector('.status-msg');
    msg.textContent = 'resolving...';
    msg.classList.remove('error');
    try {{
      const resp = await fetch('/gate-review/resolve', {{
        method: 'POST',
        headers: {{'Content-Type': 'application/json'}},
        body: JSON.stringify(body),
      }});
      const data = await resp.json();
      if (resp.ok) {{
        msg.textContent = 'resolved as ' + data.item.status;
        // Fade card out then remove.
        const card = form.closest('.card');
        if (card) {{ card.style.opacity = '0.4'; }}
      }} else {{
        msg.textContent = 'error: ' + (data.detail || data.error || 'unknown');
        msg.classList.add('error');
      }}
    }} catch (e) {{
      msg.textContent = 'network error: ' + e.message;
      msg.classList.add('error');
    }}
  }});
}});
</script>
</body></html>
"""


def _render_item_card(item) -> str:
    e = html.escape
    business = e(item.business_name or "(no business name)")
    legacy = e(item.legacy_block_reason or "—")
    gate = e(item.gate_block_label or "—")
    return f"""
<div class=card>
  <h3>{business} <span class="agreement-{e(item.agreement)}">[{e(item.agreement)}]</span></h3>
  <div class=meta>
    id <code>{item.id}</code> · product <code>{e(item.product)}</code>
    · lead_hash <code>{e(item.lead_hash)}</code>
    · seen {item.hit_count}× · last <code>{e(item.last_seen_ts)}</code>
  </div>
  <div class=meta>
    legacy says: <code>{legacy}</code>
    &nbsp; gate says: <code>{gate}</code>
  </div>
  <form class=resolve data-item-id="{item.id}">
    <span class=status-slot></span>
    <input type=text name=note placeholder="resolution note (optional)">
    <button type=submit>resolve</button>
    <span class=status-msg></span>
  </form>
</div>
"""


# ---------------------------------------------------------------------------
# Auto-suppression-PR trigger
# ---------------------------------------------------------------------------

_AUTO_PR_LOCK = threading.Lock()
_AUTO_PR_LAST_RUN = {"ts": 0.0}


def _maybe_trigger_auto_suppression_pr() -> None:
    """If env-flag is set and threshold is crossed, kick off a suppression-PR build."""
    if os.environ.get("OPERATOR_GATE_REVIEW_AUTO_SUPPRESS_PR", "").strip() not in {"1", "true", "yes"}:
        return
    try:
        threshold = int(os.environ.get("OPERATOR_GATE_REVIEW_AUTO_PR_THRESHOLD", "5"))
    except ValueError:
        threshold = 5

    # Cheap rate-limit -- avoid retriggering more than once per 60s even
    # if the operator clicks resolve many times in a row.
    import time
    with _AUTO_PR_LOCK:
        now = time.monotonic()
        if now - _AUTO_PR_LAST_RUN["ts"] < 60.0:
            return
        _AUTO_PR_LAST_RUN["ts"] = now

    threading.Thread(
        target=_run_auto_pr,
        name="gate-review-auto-pr",
        args=(threshold,),
        daemon=True,
    ).start()


def _run_auto_pr(threshold: int) -> None:
    """Background worker -- count approved_gate items, build + open PR if over threshold."""
    from pathlib import Path

    try:
        from . import suppression_pr
    except ImportError:
        logger.warning("auto_suppression_pr.import_failed")
        return

    scrub_yml = os.environ.get("OPERATOR_SCRUB_YML_PATH")
    if not scrub_yml:
        # Default sits beside the outreach-common checkout if the env var
        # isn't set. We don't fail loudly -- if the path doesn't exist,
        # build_plan creates the seed contents itself.
        scrub_yml = str(Path.home() / ".operator" / "data" / "network_scrub.yml")

    try:
        plan = suppression_pr.build_plan(Path(scrub_yml))
    except Exception as exc:  # noqa: BLE001
        logger.warning("auto_suppression_pr.build_failed", extra={"error": str(exc)})
        return

    entry_count = len(plan.new_business_names) if plan else 0
    if not plan or entry_count < threshold:
        logger.debug(
            "auto_suppression_pr.below_threshold",
            extra={"entries": entry_count, "threshold": threshold},
        )
        return

    try:
        result = suppression_pr.open_pr(plan)
    except Exception as exc:  # noqa: BLE001
        logger.warning("auto_suppression_pr.open_failed", extra={"error": str(exc)})
        return

    if isinstance(result, dict) and result.get("error"):
        logger.warning("auto_suppression_pr.api_error", extra={"detail": str(result)[:200]})
        return

    try:
        suppression_pr.mark_items_suppressed(plan.items)
    except Exception as exc:  # noqa: BLE001
        logger.warning("auto_suppression_pr.mark_failed", extra={"error": str(exc)})

    logger.info(
        "auto_suppression_pr.opened",
        extra={"entries": entry_count, "result": str(result)[:200]},
    )
