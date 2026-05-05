"""Tests for the Discord-side gate-review renderers."""
from __future__ import annotations

from pathlib import Path

import pytest

from operator_core import gate_review, gate_review_render


@pytest.fixture
def isolated_db(tmp_path, monkeypatch):
    """Point the renderer at an empty per-test sqlite db."""
    db = tmp_path / "review.sqlite"
    monkeypatch.setenv("OUTREACH_GATE_REVIEW_DB", str(db))
    return db


def _ev(product: str, agreement: str, lead_hash: str, **extra) -> dict:
    payload = {"product": product, "agreement": agreement, "lead_hash": lead_hash}
    payload.update(extra)
    return {
        "ts": "2026-05-05T15:00:00Z",
        "stream": "gate_audit",
        "event_type": "decision",
        "payload": payload,
    }


# -- render_next ----------------------------------------------------------


def test_render_next_empty_queue_prompts_to_ingest(isolated_db):
    out = gate_review_render.render_next()
    assert "No pending review items" in out
    assert "gate-review ingest" in out


def test_render_next_empty_for_specific_product(isolated_db):
    """Product filter shows a different message."""
    gate_review.ingest_events([_ev("ai-ops-consulting", "would_block_new", "x" * 16)])
    out = gate_review_render.render_next("outreach-engine")
    assert "No pending items for `outreach-engine`" in out


def test_render_next_returns_pending_card(isolated_db):
    gate_review.ingest_events([
        _ev("outreach-engine", "would_block_new", "a" * 16,
            lead_business_name="Acme Co",
            gate_block_label="network_scrub:business_name:acme co"),
    ])
    out = gate_review_render.render_next()
    assert "Gate disagreement" in out
    assert "outreach-engine" in out
    assert "Acme Co" in out
    assert "network_scrub:business_name:acme co" in out
    assert "gate-resolve" in out  # shows resolve hints
    assert "approved_gate" in out
    assert "1 pending" in out  # summary line


def test_render_next_filters_by_product(isolated_db):
    gate_review.ingest_events([
        _ev("outreach-engine", "would_block_new", "a" * 16, lead_business_name="OE"),
        _ev("ai-ops-consulting", "would_block_new", "b" * 16, lead_business_name="AI"),
    ])
    out_oe = gate_review_render.render_next("outreach-engine")
    assert "OE" in out_oe
    assert "AI" not in out_oe


def test_render_next_uses_label_when_no_business_name(isolated_db):
    gate_review.ingest_events([
        _ev("oe", "would_block_new", "x" * 16,
            gate_block_label="network_scrub:business_name:nightowl pools"),
    ])
    out = gate_review_render.render_next()
    assert "(no name)" in out
    assert "nightowl pools" in out


# -- render_resolve --------------------------------------------------------


def test_render_resolve_happy_path(isolated_db):
    gate_review.ingest_events([_ev("oe", "would_block_new", "x" * 16,
                                    lead_business_name="X")])
    item = gate_review.list_pending(db_path=isolated_db)[0]
    out = gate_review_render.render_resolve(
        item.id, "approved_gate",
        note="legit block",
        resolved_by="kruz",
    )
    assert ":white_check_mark:" in out
    assert f"#{item.id}" in out
    assert "approved_gate" in out
    assert "legit block" in out
    assert "0 pending" in out


def test_render_resolve_bad_id(isolated_db):
    out = gate_review_render.render_resolve(0, "approved_gate")
    assert ":x:" in out
    assert "Bad item id" in out


def test_render_resolve_unknown_status(isolated_db):
    out = gate_review_render.render_resolve(1, "totally_made_up")
    assert ":x:" in out
    assert "Unknown status" in out
    assert "approved_gate" in out  # lists valid options


def test_render_resolve_nonexistent_item(isolated_db):
    out = gate_review_render.render_resolve(99999, "approved_gate")
    assert ":x:" in out
    assert "no review item" in out
