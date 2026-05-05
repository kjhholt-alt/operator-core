"""Tests for the gate disagreement review queue."""
from __future__ import annotations

from pathlib import Path

import pytest

from operator_core import gate_review


def _ev(product: str, agreement: str, lead_hash: str = "a" * 16, **extra) -> dict:
    payload = {"product": product, "agreement": agreement, "lead_hash": lead_hash}
    payload.update(extra)
    return {"ts": "2026-05-05T15:00:00Z", "stream": "gate_audit",
            "event_type": "decision", "payload": payload}


# -- ingest ---------------------------------------------------------------


def test_ingest_creates_one_row_per_disagreement(tmp_path):
    db = tmp_path / "g.sqlite"
    new, updated = gate_review.ingest_events([
        _ev("oe", "would_block_new", "a" * 16),
        _ev("oe", "would_allow_new", "b" * 16),
    ], db_path=db)
    assert (new, updated) == (2, 0)
    pending = gate_review.list_pending(db_path=db)
    assert len(pending) == 2


def test_ingest_dedupes_repeated_events(tmp_path):
    db = tmp_path / "g.sqlite"
    ev = _ev("oe", "would_block_new", "x" * 16, lead_business_name="Foo")
    new1, _ = gate_review.ingest_events([ev], db_path=db)
    new2, updated2 = gate_review.ingest_events([ev, ev], db_path=db)
    assert new1 == 1
    assert (new2, updated2) == (0, 2)
    items = gate_review.list_pending(db_path=db)
    assert len(items) == 1
    assert items[0].hit_count == 3


def test_ingest_skips_match_events(tmp_path):
    """Matches don't need triage -- never queue them."""
    db = tmp_path / "g.sqlite"
    new, _ = gate_review.ingest_events([
        _ev("oe", "match", "a" * 16),
        _ev("oe", "match", "b" * 16),
        _ev("oe", "would_block_new", "c" * 16),
    ], db_path=db)
    assert new == 1


def test_ingest_skips_malformed_events(tmp_path):
    db = tmp_path / "g.sqlite"
    new, _ = gate_review.ingest_events([
        {"payload": {"agreement": "would_block_new"}},  # no product
        {"payload": {"product": "oe"}},  # no agreement
        {},  # no payload
        _ev("oe", "would_block_new", "z" * 16),
    ], db_path=db)
    assert new == 1


# -- list / get ----------------------------------------------------------


def test_list_pending_filters_by_product(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([
        _ev("oe", "would_block_new", "a" * 16),
        _ev("ai", "would_block_new", "b" * 16),
    ], db_path=db)
    assert len(gate_review.list_pending("oe", db_path=db)) == 1
    assert len(gate_review.list_pending("ai", db_path=db)) == 1
    assert len(gate_review.list_pending(db_path=db)) == 2


def test_list_pending_excludes_resolved(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([
        _ev("oe", "would_block_new", "a" * 16),
        _ev("oe", "would_block_new", "b" * 16),
    ], db_path=db)
    items = gate_review.list_pending(db_path=db)
    gate_review.resolve(items[0].id, "approved_gate", db_path=db)
    pending = gate_review.list_pending(db_path=db)
    assert len(pending) == 1


def test_get_item_returns_full_row(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([
        _ev("oe", "would_block_new", "a" * 16,
            lead_business_name="Acme Co",
            gate_block_label="network_scrub:business_name:acme co"),
    ], db_path=db)
    item = gate_review.list_pending(db_path=db)[0]
    full = gate_review.get_item(item.id, db_path=db)
    assert full.business_name == "Acme Co"
    assert full.gate_block_label == "network_scrub:business_name:acme co"


# -- resolve --------------------------------------------------------------


def test_resolve_sets_status_and_audit_fields(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([_ev("oe", "would_block_new", "a" * 16)], db_path=db)
    item = gate_review.list_pending(db_path=db)[0]
    resolved = gate_review.resolve(
        item.id, "approved_gate",
        note="business already on canonical block list",
        resolved_by="kruz",
        db_path=db,
    )
    assert resolved.status == "approved_gate"
    assert resolved.resolution_note.startswith("business")
    assert resolved.resolved_by == "kruz"
    assert resolved.resolved_ts


def test_resolve_rejects_unknown_status(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([_ev("oe", "would_block_new")], db_path=db)
    item = gate_review.list_pending(db_path=db)[0]
    with pytest.raises(ValueError, match="unknown status"):
        gate_review.resolve(item.id, "totally_made_up", db_path=db)


def test_resolve_rejects_back_to_pending(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([_ev("oe", "would_block_new")], db_path=db)
    item = gate_review.list_pending(db_path=db)[0]
    gate_review.resolve(item.id, "approved_gate", db_path=db)
    with pytest.raises(ValueError, match="re-ingest"):
        gate_review.resolve(item.id, "pending", db_path=db)


def test_resolve_unknown_id_raises(tmp_path):
    db = tmp_path / "g.sqlite"
    with pytest.raises(ValueError, match="no review item"):
        gate_review.resolve(99999, "approved_gate", db_path=db)


# -- triage summary -----------------------------------------------------


def test_triage_summary_counts_pending_vs_triaged(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([
        _ev("oe", "would_block_new", "a" * 16),
        _ev("oe", "would_block_new", "b" * 16),
        _ev("oe", "would_allow_new", "c" * 16),
        _ev("ai", "would_block_new", "d" * 16),
    ], db_path=db)
    # Resolve a specific oe row, not list_pending()[0] (order undefined).
    oe_items = gate_review.list_pending("oe", db_path=db)
    gate_review.resolve(oe_items[0].id, "approved_gate", db_path=db)
    s = {t.product: t for t in gate_review.triage_summary(db_path=db)}
    assert s["oe"].total == 3
    assert s["oe"].triaged == 1
    assert s["oe"].pending == 2
    assert s["oe"].triaged_pct == pytest.approx(33.333, rel=1e-2)
    assert s["ai"].total == 1
    assert s["ai"].pending == 1
    assert s["ai"].triaged_pct == 0.0


def test_is_fully_triaged_no_rows_returns_true(tmp_path):
    db = tmp_path / "g.sqlite"
    assert gate_review.is_fully_triaged("oe", db_path=db) is True


def test_is_fully_triaged_pending_row_returns_false(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([_ev("oe", "would_block_new")], db_path=db)
    assert gate_review.is_fully_triaged("oe", db_path=db) is False


def test_is_fully_triaged_after_all_resolved(tmp_path):
    db = tmp_path / "g.sqlite"
    gate_review.ingest_events([_ev("oe", "would_block_new", "a" * 16)], db_path=db)
    item = gate_review.list_pending(db_path=db)[0]
    gate_review.resolve(item.id, "approved_gate", db_path=db)
    assert gate_review.is_fully_triaged("oe", db_path=db) is True
