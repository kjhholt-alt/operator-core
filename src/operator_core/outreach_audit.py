"""Outreach Sender-Gate audit dashboard.

Reads the local outreach_common events.ndjson fallback (and optionally the
Supabase outreach_events table) to compute shadow-vs-live divergence per
product. Used by:

    operator outreach audit-dashboard [--json]

and the ``cut_over_promoter`` recipe.

The audit log records two kinds of "send" events that we care about for
cut-over:

  - real sends (outreach goes out via legacy or via the gate)
  - shadow sends (the gate ran in audit mode alongside the legacy send)

Shadow sends are tagged ``payload.audit_only = True`` in the envelope. The
adapter ``audit_send`` writes these. A divergence is any case where the
shadow gate would have *blocked* a send that the legacy path actually let
through, or vice versa.

This module is read-only. It NEVER sends email and NEVER mutates the
events log.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


# ---------------------------------------------------------------------------
# Locating events
# ---------------------------------------------------------------------------


def _local_log_path() -> Path:
    """Mirror outreach_common.events._local_log_path so the dashboard
    reads the same file the gate writes.
    """
    p = os.environ.get("OUTREACH_EVENTS_LOG")
    if p:
        return Path(p)
    return Path(os.path.expanduser("~/.outreach-common/events.ndjson"))


def _read_local_events(path: Optional[Path] = None) -> List[Dict[str, Any]]:
    p = path or _local_log_path()
    if not p.is_file():
        return []
    out: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


# ---------------------------------------------------------------------------
# Audit summary dataclasses
# ---------------------------------------------------------------------------


@dataclass
class ProductAudit:
    product: str
    total_events: int = 0
    real_sends: int = 0
    shadow_sends: int = 0
    shadow_blocks: int = 0
    real_blocks: int = 0
    divergences: int = 0
    divergence_examples: List[Dict[str, Any]] = field(default_factory=list)
    last_real_at: str = ""
    last_shadow_at: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class AuditDashboard:
    generated_at: str
    events_path: str
    total_events: int
    products: List[ProductAudit] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "events_path": self.events_path,
            "total_events": self.total_events,
            "products": [p.to_dict() for p in self.products],
        }


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def _is_shadow(envelope: Dict[str, Any]) -> bool:
    payload = envelope.get("payload") or {}
    return bool(payload.get("audit_only"))


def _is_block(envelope: Dict[str, Any]) -> bool:
    """A send envelope is a 'block' when the gate refused to send.

    The gate writes a send_event only when ``outcome=='sent'``; a blocked
    gate does NOT append a send event today. The shadow adapter writes
    a synthetic envelope with ``payload.audit_outcome`` so we can spot
    blocks in shadow runs without hooking the live gate.
    """
    payload = envelope.get("payload") or {}
    outcome = (payload.get("audit_outcome") or "").lower()
    return outcome == "blocked"


def compute_dashboard(
    events: Optional[Iterable[Dict[str, Any]]] = None,
    *,
    events_path: Optional[Path] = None,
    products: Optional[List[str]] = None,
) -> AuditDashboard:
    """Compute the divergence dashboard.

    Parameters
    ----------
    events : iterable of envelope dicts. If None, reads the local NDJSON
        fallback.
    events_path : override the NDJSON location.
    products : restrict to these product slugs. Default: all products
        seen in the log.
    """
    from datetime import datetime, timezone

    if events is None:
        path = events_path or _local_log_path()
        evs = _read_local_events(path)
    else:
        evs = [e for e in events]
        path = events_path or _local_log_path()

    by_product: Dict[str, ProductAudit] = {}
    total = 0

    for env in evs:
        if env.get("type") != "send":
            continue
        total += 1
        prod = (env.get("product") or "").strip() or "unknown"
        if products and prod not in products:
            continue
        pa = by_product.setdefault(prod, ProductAudit(product=prod))
        pa.total_events += 1
        ts = env.get("occurred_at") or ""
        shadow = _is_shadow(env)
        block = _is_block(env)
        if shadow:
            pa.shadow_sends += 1
            if ts > pa.last_shadow_at:
                pa.last_shadow_at = ts
            if block:
                pa.shadow_blocks += 1
                if len(pa.divergence_examples) < 5:
                    pa.divergence_examples.append(
                        {
                            "kind": "shadow_block",
                            "lead_email": env.get("lead_email", ""),
                            "block_label": (env.get("payload") or {}).get(
                                "audit_block_label", ""
                            ),
                            "occurred_at": ts,
                        }
                    )
        else:
            pa.real_sends += 1
            if ts > pa.last_real_at:
                pa.last_real_at = ts
            if block:
                pa.real_blocks += 1

    # Divergence count per product = shadow_blocks (the gate would have
    # blocked something legacy actually delivered). We surface real_blocks
    # too; legacy doesn't produce blocks today, so non-zero real_blocks
    # implies the gate is now the live path.
    for pa in by_product.values():
        pa.divergences = pa.shadow_blocks

    return AuditDashboard(
        generated_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        events_path=str(path),
        total_events=total,
        products=sorted(by_product.values(), key=lambda p: p.product),
    )


# ---------------------------------------------------------------------------
# Pretty rendering for the CLI
# ---------------------------------------------------------------------------


def render_text(d: AuditDashboard) -> str:
    lines: List[str] = []
    lines.append(f"Outreach Sender-Gate audit dashboard")
    lines.append(f"  generated_at : {d.generated_at}")
    lines.append(f"  events_path  : {d.events_path}")
    lines.append(f"  total_events : {d.total_events}")
    lines.append("")
    if not d.products:
        lines.append("  (no send events recorded yet)")
        return "\n".join(lines)
    header = (
        f"  {'product':<24} {'real':>6} {'shadow':>7} {'sh.blocks':>9} "
        f"{'r.blocks':>8} {'diverge':>8}"
    )
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for p in d.products:
        lines.append(
            f"  {p.product:<24} {p.real_sends:>6} {p.shadow_sends:>7} "
            f"{p.shadow_blocks:>9} {p.real_blocks:>8} {p.divergences:>8}"
        )
    lines.append("")
    for p in d.products:
        if p.divergence_examples:
            lines.append(f"  divergences for {p.product}:")
            for ex in p.divergence_examples:
                lines.append(
                    f"    - [{ex['occurred_at']}] {ex['kind']} "
                    f"{ex['lead_email']} :: {ex['block_label']}"
                )
            lines.append("")
    return "\n".join(lines)
