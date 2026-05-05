"""Tests for the daily sender_gate_digest recipe."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from operator_core import cutover_streak as cs
from operator_core import gate_review
from operator_core.recipes import RecipeContext


def _load_module():
    repo_root = Path(__file__).resolve().parent.parent
    src = repo_root / "recipes" / "sender_gate_digest.py"
    spec = importlib.util.spec_from_file_location("operator_recipes_sender_gate_digest", src)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _ctx(**ov):
    base = dict(
        recipe_name="sender_gate_digest",
        correlation_id="t",
        env={}, clients={}, cost_so_far=0.0, cost_budget_usd=0.0, dry_run=False,
    )
    base.update(ov)
    return RecipeContext(**base)


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_CUTOVER_STREAK_DB", str(tmp_path / "streak.sqlite"))
    monkeypatch.setenv("OUTREACH_GATE_REVIEW_DB", str(tmp_path / "gr.sqlite"))
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path / "events"))
    return tmp_path


def _ts(seconds_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat().replace("+00:00", "Z")


def _ev(*, lead_hash, agreement="would_block_new", product="oe"):
    return {
        "ts": "2026-05-05T10:00:00Z",
        "payload": {
            "product": product, "lead_hash": lead_hash, "agreement": agreement,
            "lead_business_name": "Foo Co",
            "gate_block_label": "network_scrub:business_name:foo",
            "legacy_block_reason": None,
        },
    }


def test_metadata():
    mod = _load_module()
    r = mod.SenderGateDigest()
    assert r.name == "sender_gate_digest"
    assert r.schedule == "45 6 * * *"


def test_quiet_state_still_emits_friendly_message(isolated):
    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    msg = asyncio.run(rec.format(_ctx(), result))
    # Always emits -- empty-state is itself a useful daily signal.
    assert "Sender Gate" in msg
    assert "no resolutions in last 24h" in msg
    assert "pending queue: empty" in msg
    assert "promotions in last 24h: 0" in msg
    assert "rollback alerts in last 24h: 0" in msg


def test_counts_pending_per_product(isolated):
    gate_review.ingest_events([_ev(lead_hash="a"), _ev(lead_hash="b", product="pp")])
    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    assert result["pending_total"] == 2
    assert result["pending_by_product"] == {"oe": 1, "pp": 1}
    msg = asyncio.run(rec.format(_ctx(), result))
    assert "pending queue: 2 total" in msg


def test_counts_24h_resolutions_split_auto_vs_human(isolated):
    gate_review.ingest_events([
        _ev(lead_hash="a"), _ev(lead_hash="b"),
        _ev(lead_hash="c"), _ev(lead_hash="d"),
    ])
    items = gate_review.list_pending()
    # Two by classifier, one by web, one by cli.
    gate_review.resolve(items[0].id, "approved_gate", resolved_by="auto-classifier:network_scrub_recurrence")
    gate_review.resolve(items[1].id, "approved_gate", resolved_by="auto-classifier:network_scrub_recurrence")
    gate_review.resolve(items[2].id, "approved_gate", resolved_by="web-ui")
    gate_review.resolve(items[3].id, "approved_legacy", resolved_by="cli")

    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    assert result["auto_resolved_24h_total"] == 2
    assert result["human_resolved_24h_total"] == 2
    assert result["auto_classify_ratio_24h"] == 0.5

    msg = asyncio.run(rec.format(_ctx(), result))
    assert "4 resolutions" in msg
    assert "auto-rate 50%" in msg
    assert "network_scrub_recurrence: 2" in msg


def test_counts_recent_promotions_only(isolated):
    cs.record_check("oe", True)
    cs.mark_promoted("oe", "https://github.com/x/y/pull/1", now_ts=_ts(60 * 60 * 6))   # 6h ago, in
    cs.record_check("pp", True)
    cs.mark_promoted("pp", "https://github.com/x/y/pull/2", now_ts=_ts(60 * 60 * 30))  # 30h ago, out

    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    assert len(result["recent_promotions"]) == 1
    assert result["recent_promotions"][0]["product"] == "oe"
    msg = asyncio.run(rec.format(_ctx(), result))
    assert "promotions in last 24h: 1" in msg
    assert "https://github.com/x/y/pull/1" in msg


def test_reads_recent_rollback_alerts(isolated):
    # Write two rollback_alerts events: one fresh, one stale.
    events_dir = isolated / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    log = events_dir / "rollback_alerts.ndjson"
    fresh = {
        "ts": _ts(60 * 60 * 2),  # 2h ago
        "stream": "rollback_alerts", "kind": "cutover_regression",
        "payload": {"product": "oe", "reasons": ["match_pct_80<95"], "match_pct": 80.0},
    }
    stale = {
        "ts": _ts(60 * 60 * 50),  # 50h ago
        "stream": "rollback_alerts", "kind": "cutover_regression",
        "payload": {"product": "pp", "reasons": ["something_old"]},
    }
    log.write_text(json.dumps(fresh) + "\n" + json.dumps(stale) + "\n", encoding="utf-8")

    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    assert len(result["rollback_alerts_24h"]) == 1
    msg = asyncio.run(rec.format(_ctx(), result))
    assert ":rotating_light:" in msg
    assert "rollback alerts in last 24h: 1" in msg
    assert "match_pct_80<95" in msg


def test_old_resolutions_excluded(isolated, monkeypatch):
    """Resolutions outside the 24h window must not count toward today's tally."""
    gate_review.ingest_events([_ev(lead_hash="x")])
    item = gate_review.list_pending()[0]
    # Resolve normally (creates a recent resolved_ts).
    gate_review.resolve(item.id, "approved_gate", resolved_by="auto-classifier:network_scrub_recurrence")
    # Hand-edit the row to simulate an old resolution.
    with gate_review.open_db() as conn:
        conn.execute("UPDATE review_items SET resolved_ts=? WHERE id=?",
                     (_ts(60 * 60 * 50), item.id))

    mod = _load_module()
    rec = mod.SenderGateDigest()
    result = asyncio.run(rec.query(_ctx()))
    assert result["auto_resolved_24h_total"] == 0
    assert result["human_resolved_24h_total"] == 0
