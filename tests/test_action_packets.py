from __future__ import annotations

import json

import pytest

from operator_core.action_packets import (
    PACKET_STATUSES,
    action_packet_dir,
    action_packet_kinds,
    action_packet_summary,
    create_action_packet,
    list_action_packets,
    update_action_packet_status,
)


def test_create_action_packet_writes_markdown_and_json(tmp_path):
    packet_dir = tmp_path / "packets"
    packet = create_action_packet(
        kind="source_action_work_order",
        title="Connect source registry gap",
        context={"summary": "Source registry shows one missing cockpit connection."},
        packet_dir=packet_dir,
    )

    json_path = packet_dir / f"{packet['id']}.json"
    md_path = packet_dir / f"{packet['id']}.md"
    assert json_path.exists()
    assert md_path.exists()

    metadata = json.loads(json_path.read_text(encoding="utf-8"))
    body = md_path.read_text(encoding="utf-8")
    assert metadata["status"] == "draft"
    assert metadata["safety"]["external_apis"] is False
    assert "## Stop Rules" in body
    assert "No sends, deletes" not in body
    assert "Source registry shows one missing cockpit connection." in body
    assert not list(packet_dir.glob("*.tmp"))


def test_list_and_summary_action_packets(tmp_path):
    packet_dir = tmp_path / "packets"
    first = create_action_packet(kind="next_agent_handoff", packet_dir=packet_dir)
    second = create_action_packet(kind="weekly_review_follow_up", packet_dir=packet_dir, status="ready")

    packets = list_action_packets(packet_dir)
    ids = {packet["id"] for packet in packets}
    summary = action_packet_summary(packets)

    assert {first["id"], second["id"]} == ids
    assert summary["count"] == 2
    assert summary["by_status"]["draft"] == 1
    assert summary["by_status"]["ready"] == 1
    assert summary["open_count"] == 2


def test_update_action_packet_status_rewrites_metadata_and_markdown(tmp_path):
    packet_dir = tmp_path / "packets"
    packet = create_action_packet(kind="claude_audit_packet", packet_dir=packet_dir)

    updated = update_action_packet_status(packet["id"], "claimed", packet_dir)

    assert updated["status"] == "claimed"
    metadata = json.loads((packet_dir / f"{packet['id']}.json").read_text(encoding="utf-8"))
    markdown = (packet_dir / f"{packet['id']}.md").read_text(encoding="utf-8")
    assert metadata["status"] == "claimed"
    assert "- Status: `claimed`" in markdown


def test_action_packets_reject_unknown_kind_and_status(tmp_path):
    with pytest.raises(ValueError):
        create_action_packet(kind="external_send", packet_dir=tmp_path)

    with pytest.raises(ValueError):
        create_action_packet(kind="next_agent_handoff", packet_dir=tmp_path, status="sent")

    packet = create_action_packet(kind="next_agent_handoff", packet_dir=tmp_path)
    with pytest.raises(ValueError):
        update_action_packet_status(packet["id"], "deleted", tmp_path)


def test_action_packet_catalog_and_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_ACTION_PACKET_DIR", str(tmp_path / "custom"))

    kinds = {kind["id"] for kind in action_packet_kinds()}

    assert len(kinds) == 6
    assert "codex_implementation_packet" in kinds
    assert set(PACKET_STATUSES) == {"draft", "ready", "claimed", "done"}
    assert action_packet_dir(tmp_path) == tmp_path / "custom"
