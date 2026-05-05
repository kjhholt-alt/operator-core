"""Tests for the gate-review auto-classifier."""

from __future__ import annotations

from pathlib import Path

import pytest

from operator_core import gate_review
from operator_core import gate_review_classifier as cls


@pytest.fixture
def gate_db(tmp_path, monkeypatch):
    db = tmp_path / "gr.sqlite"
    monkeypatch.setenv("OUTREACH_GATE_REVIEW_DB", str(db))
    return db


def _ingest(events: list[dict]) -> None:
    # Returns (new, updated). Duplicate keys collapse into the same row,
    # incrementing hit_count -- which is what we WANT to drive the
    # min_hits threshold in classifier tests.
    gate_review.ingest_events(events)


def _ev(*, lead_hash: str, agreement: str = "would_block_new",
        gate_label: str | None = "network_scrub:business_name:test pizza co",
        legacy_reason: str | None = None,
        business_name: str = "Test Pizza Co",
        product: str = "oe", ts: str = "2026-05-05T10:00:00Z") -> dict:
    return {
        "ts": ts,
        "payload": {
            "product": product, "lead_hash": lead_hash,
            "agreement": agreement,
            "lead_business_name": business_name,
            "gate_block_label": gate_label,
            "legacy_block_reason": legacy_reason,
        },
    }


# --- network_scrub_recurrence rule ------------------------------------------

class TestNetworkScrubRecurrence:
    def test_resolves_when_hit_count_meets_threshold(self, gate_db):
        # Same (product, lead_hash, agreement) = same row, hit_count increments.
        _ingest([_ev(lead_hash="aa", ts="2026-05-05T10:00:00Z"),
                 _ev(lead_hash="aa", ts="2026-05-05T10:01:00Z")])
        # Now hit_count is 2.
        assert len(gate_review.list_pending()) == 1
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 1
        assert res.rules_fired["network_scrub_recurrence"] == 1
        assert gate_review.list_pending() == []
        # Resolved row must carry the auto-classifier stamp.
        item = gate_review.get_item(res.item_ids_resolved[0])
        assert item.status == "approved_gate"
        assert item.resolved_by == "auto-classifier:network_scrub_recurrence"
        # Note carries the lower-cased name from the gate label.
        assert "test pizza co" in (item.resolution_note or "").lower()

    def test_skips_when_below_threshold(self, gate_db):
        _ingest([_ev(lead_hash="bb")])
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 0
        assert res.rules_fired["network_scrub_recurrence"] == 0
        assert len(gate_review.list_pending()) == 1

    def test_skips_when_legacy_reason_present(self, gate_db):
        _ingest([_ev(lead_hash="cc", ts="2026-05-05T10:00:00Z", legacy_reason="manual"),
                 _ev(lead_hash="cc", ts="2026-05-05T10:01:00Z", legacy_reason="manual")])
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 0
        # Legacy had its own opinion -- not a clean win for the gate.

    def test_skips_when_label_not_network_scrub(self, gate_db):
        _ingest([_ev(lead_hash="dd", gate_label="something_else:abc"),
                 _ev(lead_hash="dd", gate_label="something_else:abc")])
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 0


# --- tld_guard rule ---------------------------------------------------------

class TestTldGuard:
    def test_resolves_when_tld_label_and_hits(self, gate_db):
        _ingest([_ev(lead_hash="t1", gate_label="tld:business_email:.invalid"),
                 _ev(lead_hash="t1", gate_label="tld:business_email:.invalid")])
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 1
        assert res.rules_fired["tld_guard"] == 1


# --- mixed batches + dry run ------------------------------------------------

class TestMixedBatches:
    def test_classifier_handles_mixed_pending(self, gate_db):
        _ingest([
            # network_scrub recurrence (auto)
            _ev(lead_hash="ns1"), _ev(lead_hash="ns1"),
            # tld guard (auto)
            _ev(lead_hash="t1", gate_label="tld:business_email:.invalid"),
            _ev(lead_hash="t1", gate_label="tld:business_email:.invalid"),
            # ambiguous: legacy had a reason (skip)
            _ev(lead_hash="amb", legacy_reason="manual_blocklist"),
            _ev(lead_hash="amb", legacy_reason="manual_blocklist"),
            # would_allow_new (skip -- different agreement)
            _ev(lead_hash="al1", agreement="would_allow_new"),
            _ev(lead_hash="al1", agreement="would_allow_new"),
        ])
        res = cls.classify_pending(min_hits=2)
        assert res.auto_resolved == 2
        assert res.rules_fired["network_scrub_recurrence"] == 1
        assert res.rules_fired["tld_guard"] == 1
        # Two ambiguous items survive.
        remaining = gate_review.list_pending()
        assert len(remaining) == 2

    def test_dry_run_does_not_resolve(self, gate_db):
        _ingest([_ev(lead_hash="d1"), _ev(lead_hash="d1")])
        res = cls.classify_pending(min_hits=2, dry_run=True)
        assert res.auto_resolved == 1  # would-resolve count
        # But nothing was written.
        assert len(gate_review.list_pending()) == 1


# --- min_hits override ------------------------------------------------------

def test_min_hits_can_be_raised(gate_db):
    _ingest([_ev(lead_hash="a"), _ev(lead_hash="a"), _ev(lead_hash="a")])
    # hit_count = 3; with min_hits=5 we should NOT resolve.
    res = cls.classify_pending(min_hits=5)
    assert res.auto_resolved == 0
    # But min_hits=3 will.
    res2 = cls.classify_pending(min_hits=3)
    assert res2.auto_resolved == 1
