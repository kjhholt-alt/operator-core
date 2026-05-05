"""Tests for the gate-review classifier metrics in /metrics output."""

from __future__ import annotations

import pytest

from operator_core import gate_review
from operator_core import metrics


@pytest.fixture
def gate_db(tmp_path, monkeypatch):
    p = tmp_path / "gr.sqlite"
    monkeypatch.setenv("OUTREACH_GATE_REVIEW_DB", str(p))
    return p


def _seed(events):
    gate_review.ingest_events(events)


def _ev(*, lead_hash, agreement="would_block_new", product="oe",
        gate_label="network_scrub:business_name:foo"):
    return {
        "ts": "2026-05-05T10:00:00Z",
        "payload": {
            "product": product, "lead_hash": lead_hash, "agreement": agreement,
            "lead_business_name": "Foo Co",
            "gate_block_label": gate_label, "legacy_block_reason": None,
        },
    }


class TestResolverClassification:
    def test_buckets(self):
        assert metrics._classify_resolver("auto-classifier:network_scrub_recurrence") == "auto"
        assert metrics._classify_resolver("auto-classifier:tld_guard") == "auto"
        assert metrics._classify_resolver("operator-core/suppression_pr") == "auto"
        assert metrics._classify_resolver("web-ui") == "web"
        assert metrics._classify_resolver("cli") == "cli"
        assert metrics._classify_resolver("discord-user-1234") == "discord"
        assert metrics._classify_resolver("operator-bot") == "discord"
        assert metrics._classify_resolver(None) == "unknown"
        assert metrics._classify_resolver("") == "unknown"
        assert metrics._classify_resolver("kruz@email") == "other"


class TestGateReviewMetricLines:
    def test_empty_db_returns_no_lines(self, gate_db):
        lines = metrics._gate_review_metric_lines()
        assert lines == []

    def test_pending_only_emits_pending_gauge(self, gate_db):
        _seed([_ev(lead_hash="a"), _ev(lead_hash="b")])
        lines = metrics._gate_review_metric_lines()
        text = "\n".join(lines)
        assert "operator_gate_review_pending" in text
        assert 'product="oe"' in text
        # No resolutions yet -> ratio gauge is 0.
        assert "operator_gate_review_auto_classify_ratio 0" in text

    def test_mixed_resolutions_emit_ratio(self, gate_db):
        _seed([_ev(lead_hash="a"), _ev(lead_hash="b"), _ev(lead_hash="c"), _ev(lead_hash="d")])
        items = gate_review.list_pending()
        assert len(items) == 4
        gate_review.resolve(items[0].id, "approved_gate", resolved_by="auto-classifier:network_scrub_recurrence")
        gate_review.resolve(items[1].id, "approved_gate", resolved_by="auto-classifier:tld_guard")
        gate_review.resolve(items[2].id, "approved_gate", resolved_by="web-ui")
        gate_review.resolve(items[3].id, "approved_legacy", resolved_by="cli")

        lines = metrics._gate_review_metric_lines()
        text = "\n".join(lines)
        assert "operator_gate_review_auto_classify_ratio" in text
        # 2 auto + 2 human => ratio 0.5
        assert 'operator_gate_review_auto_classify_ratio{product="oe"} 0.5000' in text
        # Per-source counter rows present.
        assert 'source="auto"' in text
        assert 'source="web"' in text
        assert 'source="cli"' in text

    def test_render_metrics_includes_classifier_block(self, gate_db, tmp_path, monkeypatch):
        # Seed minimal data so the line set is non-empty.
        _seed([_ev(lead_hash="x")])
        # render_metrics needs a JobStore + status path. Use the minimal happy path.
        from operator_core.store import JobStore
        store = JobStore(db_path=tmp_path / "jobs.sqlite")
        text = metrics.render_metrics(
            store=store,
            status_path=tmp_path / "status.json",  # missing -> defaults
        )
        assert "operator_gate_review_pending" in text
