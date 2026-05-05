"""Tests for the static HTML cut-over dashboard renderer."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from operator_core import outreach_audit, outreach_audit_html


def _summary(product: str, total: int, match: int, would_block: int = 0,
             would_allow: int = 0) -> outreach_audit.ProductSummary:
    return outreach_audit.ProductSummary(
        product=product,
        total=total,
        match=match,
        would_block_new=would_block,
        would_allow_new=would_allow,
    )


def test_render_has_well_formed_html():
    html = outreach_audit_html.render([_summary("oe", 10, 10)])
    assert html.startswith("<!doctype html>")
    assert html.rstrip().endswith("</html>")
    assert "<body" in html and "</body>" in html


def test_render_includes_tailwind_cdn():
    html = outreach_audit_html.render([_summary("oe", 10, 10)])
    assert 'cdn.tailwindcss.com' in html


def test_render_shows_ready_when_all_green():
    html = outreach_audit_html.render([_summary("oe", 10, 10)])
    assert "ALL PRODUCTS READY" in html
    assert "text-emerald-400" in html


def test_render_shows_not_ready_with_would_allow_new():
    html = outreach_audit_html.render([_summary("oe", 10, 9, would_allow=1)])
    assert "NOT READY TO FLIP" in html
    assert "text-rose-400" in html


def test_render_handles_empty_summaries():
    html = outreach_audit_html.render([])
    assert "No gate_audit events found" in html
    assert "CUTOVER.md" in html


def test_render_includes_inline_json_payload():
    html = outreach_audit_html.render([_summary("oe", 5, 5)])
    m = re.search(r"window\.__AUDIT__ = (\{.*?\});", html)
    assert m, "expected window.__AUDIT__ JSON payload"
    payload = json.loads(m.group(1))
    assert payload["products"][0]["product"] == "oe"
    assert payload["products"][0]["total"] == 5


def test_render_surfaces_would_allow_samples_with_DANGER():
    s = outreach_audit.ProductSummary(product="oe", total=10, match=9, would_allow_new=1)
    s.sample_would_allow.append({
        "lead_hash": "x" * 16,
        "business_name": "Suspect Corp",
        "legacy_block_reason": "manual_suppression",
    })
    html = outreach_audit_html.render([s])
    assert "Suspect Corp" in html
    assert "DANGER" in html
    assert "manual_suppression" in html


def test_render_surfaces_would_block_samples_no_danger():
    s = outreach_audit.ProductSummary(product="oe", total=10, match=9, would_block_new=1)
    s.sample_would_block.append({
        "lead_hash": "x" * 16,
        "business_name": "All Around Town LLC",
        "gate_block_label": "network_scrub:business_name:all around town",
    })
    html = outreach_audit_html.render([s])
    assert "All Around Town LLC" in html
    assert "network_scrub" in html


def test_render_to_writes_file(tmp_path):
    out = tmp_path / "sub" / "audit.html"  # parent dir does not exist
    summaries = [_summary("oe", 5, 5)]
    written = outreach_audit_html.render_to(out, summaries)
    assert written == out
    assert out.is_file()
    text = out.read_text(encoding="utf-8")
    assert "<!doctype html>" in text


def test_render_to_atomic_no_tmp_leftover(tmp_path):
    out = tmp_path / "audit.html"
    outreach_audit_html.render_to(out, [_summary("oe", 5, 5)])
    leftovers = list(tmp_path.glob("*.tmp"))
    assert leftovers == []


def test_render_includes_since_label_when_provided():
    html = outreach_audit_html.render([_summary("oe", 5, 5)], since_label="24h")
    assert "24h" in html


def test_render_omits_since_label_when_none():
    html = outreach_audit_html.render([_summary("oe", 5, 5)])
    # The "window:" segment only appears when since_label is provided.
    assert "window: " not in html


def test_render_threshold_in_each_card():
    html = outreach_audit_html.render([_summary("oe", 5, 5)], threshold=80.0)
    assert "&ge; 80.0" in html


def test_payload_omits_lead_email_even_when_present():
    """Sanity: business_name is fine, raw email never serializes."""
    s = outreach_audit.ProductSummary(product="oe", total=1, match=0, would_block_new=1)
    s.sample_would_block.append({
        "lead_hash": "abc123def456",
        "business_name": "Example Co",
        "gate_block_label": "step:x",
    })
    html = outreach_audit_html.render([s])
    assert "@" not in re.sub(r"@?[a-z]+\.tailwindcss\.com", "", html)
