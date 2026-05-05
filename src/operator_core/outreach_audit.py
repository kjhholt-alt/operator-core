"""Read gate_audit events and report cut-over decision data.

Powers `operator outreach audit-report`. Walks one or more
``gate_audit.ndjson`` files, summarizes per product, and renders a
table or JSON. Exit code 0 if every product is cut-over-ready
(match >= threshold and zero would_allow_new), 1 otherwise.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional


@dataclass
class ProductSummary:
    product: str
    total: int = 0
    match: int = 0
    would_block_new: int = 0
    would_allow_new: int = 0
    both_block_diff_reason: int = 0
    error: int = 0
    sample_would_block: list = field(default_factory=list)
    sample_would_allow: list = field(default_factory=list)
    # Triage state from gate_review queue (filled in by collect()).
    triage_total: int = 0
    triage_pending: int = 0
    triage_triaged: int = 0

    @property
    def match_pct(self) -> float:
        return (self.match / self.total * 100.0) if self.total else 0.0

    @property
    def triaged_pct(self) -> float:
        if self.triage_total == 0:
            # Nothing in the queue -> nothing to triage. Treat as 100% so
            # products with no disagreements at all stay READY.
            return 100.0
        return self.triage_triaged / self.triage_total * 100.0

    @property
    def fully_triaged(self) -> bool:
        return self.triage_pending == 0

    def cutover_ready(self, threshold_pct: float = 95.0) -> bool:
        return (
            self.total > 0
            and self.match_pct >= threshold_pct
            and self.would_allow_new == 0
            and self.fully_triaged
        )


def _parse_ts(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _parse_since(spec: Optional[str]) -> Optional[datetime]:
    """Accept '24h', '7d', '30m', or an ISO-8601 string."""
    if not spec:
        return None
    spec = spec.strip()
    if spec[-1:] in ("h", "d", "m") and spec[:-1].isdigit():
        n = int(spec[:-1])
        unit = spec[-1]
        delta = {
            "h": timedelta(hours=n),
            "d": timedelta(days=n),
            "m": timedelta(minutes=n),
        }[unit]
        return datetime.now(timezone.utc) - delta
    return _parse_ts(spec)


def _iter_events(paths: Iterable[Path]) -> Iterable[dict]:
    for p in paths:
        if not p.is_file():
            continue
        with open(p, "r", encoding="utf-8") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    env = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                yield env


def collect(paths: List[Path], since: Optional[datetime] = None,
            *, triage_db_path: Optional[Path] = None,
            include_triage: bool = True) -> List[ProductSummary]:
    """Walk events, group by product, count agreements. Optionally annotate
    each product with triage state from the gate_review queue so the
    cut-over rule can require triaged% == 100."""
    by_product: dict[str, ProductSummary] = {}
    for env in _iter_events(paths):
        ts = _parse_ts(env.get("ts"))
        if since and ts and ts < since:
            continue
        payload = env.get("payload") or {}
        product = payload.get("product")
        agreement = payload.get("agreement")
        if not product or not agreement:
            continue
        s = by_product.get(product)
        if s is None:
            s = ProductSummary(product=product)
            by_product[product] = s
        s.total += 1
        if agreement == "match":
            s.match += 1
        elif agreement == "would_block_new":
            s.would_block_new += 1
            if len(s.sample_would_block) < 5:
                s.sample_would_block.append({
                    "lead_hash": payload.get("lead_hash"),
                    "business_name": payload.get("lead_business_name"),
                    "gate_block_label": payload.get("gate_block_label"),
                })
        elif agreement == "would_allow_new":
            s.would_allow_new += 1
            if len(s.sample_would_allow) < 5:
                s.sample_would_allow.append({
                    "lead_hash": payload.get("lead_hash"),
                    "business_name": payload.get("lead_business_name"),
                    "legacy_block_reason": payload.get("legacy_block_reason"),
                })
        elif agreement == "both_block_diff_reason":
            s.both_block_diff_reason += 1
        else:
            s.error += 1

    if include_triage:
        try:
            from . import gate_review
            triage_by_product = {t.product: t for t in
                                 gate_review.triage_summary(db_path=triage_db_path)}
            for s in by_product.values():
                t = triage_by_product.get(s.product)
                if t is not None:
                    s.triage_total = t.total
                    s.triage_pending = t.pending
                    s.triage_triaged = t.triaged
        except Exception:  # pragma: no cover - defensive
            pass
    return sorted(by_product.values(), key=lambda x: x.product)


def default_audit_paths() -> List[Path]:
    """Look for gate_audit ndjson files in conventional places."""
    out: List[Path] = []
    explicit = os.environ.get("OUTREACH_GATE_AUDIT_PATH")
    if explicit:
        out.append(Path(explicit))
    home_audit = Path.home() / ".operator" / "data" / "outreach"
    if home_audit.is_dir():
        out.extend(sorted(home_audit.glob("**/gate_audit.ndjson")))
    return out


def render_table(summaries: List[ProductSummary], threshold: float) -> str:
    if not summaries:
        return "No gate_audit events found.\n"
    name_w = max(8, max(len(s.product) for s in summaries) + 2)
    header = (
        f"{'PRODUCT':<{name_w}} {'TOTAL':>6} {'MATCH%':>7} "
        f"{'WBLOCK':>7} {'WALLOW':>7} {'DIFFR':>6} "
        f"{'TRIAGE%':>8} {'READY':>6}"
    )
    lines = [header, "-" * len(header)]
    for s in summaries:
        ready = "YES" if s.cutover_ready(threshold) else "no"
        lines.append(
            f"{s.product:<{name_w}} "
            f"{s.total:>6} "
            f"{s.match_pct:>6.1f}% "
            f"{s.would_block_new:>7} "
            f"{s.would_allow_new:>7} "
            f"{s.both_block_diff_reason:>6} "
            f"{s.triaged_pct:>7.1f}% "
            f"{ready:>6}"
        )
    lines.append("")
    lines.append(
        f"Decision rule: cut-over READY when match% >= {threshold:.1f}, "
        f"would_allow_new == 0, and triaged% == 100 (no pending review items)."
    )
    # Sample lines (first 3 per product)
    for s in summaries:
        if s.sample_would_block:
            lines.append(f"\n{s.product} would_block_new samples:")
            for sample in s.sample_would_block[:3]:
                bn = sample.get("business_name") or "(no name)"
                label = sample.get("gate_block_label") or "(no label)"
                lines.append(f"  - {bn[:50]:<50}  {label[:60]}")
        if s.sample_would_allow:
            lines.append(f"\n{s.product} would_allow_new samples (DANGER -- gate may be wrong):")
            for sample in s.sample_would_allow[:3]:
                bn = sample.get("business_name") or "(no name)"
                reason = sample.get("legacy_block_reason") or "(no legacy reason)"
                lines.append(f"  - {bn[:50]:<50}  legacy: {reason[:50]}")
    return "\n".join(lines) + "\n"


def render_json(summaries: List[ProductSummary], threshold: float) -> str:
    payload = {
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
                "error": s.error,
                "triage_total": s.triage_total,
                "triage_pending": s.triage_pending,
                "triage_triaged": s.triage_triaged,
                "triaged_pct": round(s.triaged_pct, 2),
                "cutover_ready": s.cutover_ready(threshold),
                "sample_would_block": s.sample_would_block,
                "sample_would_allow": s.sample_would_allow,
            }
            for s in summaries
        ],
    }
    return json.dumps(payload, indent=2) + "\n"


def overall_ready(summaries: List[ProductSummary], threshold: float) -> bool:
    return bool(summaries) and all(s.cutover_ready(threshold) for s in summaries)
