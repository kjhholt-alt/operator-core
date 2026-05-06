from __future__ import annotations

import json

from operator_core.action_packets import create_action_packet
from operator_core.agent_launches import (
    agent_launch_dir,
    find_launch_by_packet,
    launch_summary,
    list_agent_launches,
    prepare_agent_launch,
)
from operator_core.store import JobStore


def test_prepare_agent_launch_writes_local_artifacts_and_job(tmp_path):
    packet_dir = tmp_path / "packets"
    launch_dir = tmp_path / "launches"
    store = JobStore(tmp_path / "jobs.sqlite")
    packet = create_action_packet(
        kind="codex_implementation_packet",
        title="Fix red status",
        context={"project": "operator-core", "summary": "Status red: tests failing"},
        packet_dir=packet_dir,
        status="ready",
    )

    launch = prepare_agent_launch(packet, launch_dir, job_store=store, actor="codex")

    assert launch["status"] == "prepared"
    assert launch["project"] == "operator-core"
    assert launch["job_id"]
    assert launch["safety"]["starts_agent"] is False
    assert (launch_dir / f"{launch['id']}.json").exists()
    assert (launch_dir / f"{launch['id']}.md").exists()
    saved = json.loads((launch_dir / f"{launch['id']}.json").read_text(encoding="utf-8"))
    assert saved["packet_id"] == packet["id"]
    jobs = store.list_jobs()
    assert jobs[0].action == "agent.launch_prepared"
    assert jobs[0].metadata["packet_id"] == packet["id"]


def test_prepare_agent_launch_dedupes_by_packet(tmp_path):
    packet = create_action_packet(kind="next_agent_handoff", packet_dir=tmp_path / "packets")

    first = prepare_agent_launch(packet, tmp_path / "launches")
    second = prepare_agent_launch(packet, tmp_path / "launches")

    assert second["id"] == first["id"]
    assert find_launch_by_packet(tmp_path / "launches", packet["id"])["id"] == first["id"]
    launches = list_agent_launches(tmp_path / "launches")
    assert launch_summary(launches)["prepared_count"] == 1


def test_agent_launch_dir_honors_env(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_AGENT_LAUNCH_DIR", str(tmp_path / "custom"))

    assert agent_launch_dir(tmp_path) == tmp_path / "custom"
