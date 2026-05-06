from __future__ import annotations

import asyncio
import json
import urllib.error
from pathlib import Path
from unittest.mock import MagicMock

from recipes import status_sync


def _valid_doc(project: str = "ax02-work") -> dict:
    return {
        "schema_version": "status-spec/v1",
        "project": project,
        "ts": "2026-05-06T12:00:00Z",
        "health": "green",
        "summary": "synced from work laptop",
    }


def test_status_sync_valid_pull_writes_status_doc(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    monkeypatch.setenv("OPERATOR_STATUS_SYNC_URLS", "https://gist.example/ax02-work.json")
    monkeypatch.setattr(status_sync, "_fetch_json", lambda url: _valid_doc())

    recipe = status_sync.StatusSync()
    ctx = MagicMock()
    ctx.logger = MagicMock()

    data = asyncio.run(recipe.query(ctx))
    result = asyncio.run(recipe.analyze(ctx, data))
    message = asyncio.run(recipe.format(ctx, result))

    out = tmp_path / "status" / "ax02-work.json"
    assert out.exists()
    assert json.loads(out.read_text(encoding="utf-8"))["project"] == "ax02-work"
    assert "accepted 1" in message


def test_status_sync_rejects_invalid_schema(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    monkeypatch.setenv("OPERATOR_STATUS_SYNC_URLS", "https://gist.example/bad.json")
    bad = _valid_doc()
    bad["health"] = "degraded"
    monkeypatch.setattr(status_sync, "_fetch_json", lambda url: bad)

    recipe = status_sync.StatusSync()
    ctx = MagicMock()
    ctx.logger = MagicMock()

    data = asyncio.run(recipe.query(ctx))
    result = asyncio.run(recipe.analyze(ctx, data))
    asyncio.run(recipe.format(ctx, result))

    assert result["accepted"] == []
    assert result["rejected"][0]["reason"]
    assert not (tmp_path / "status" / "ax02-work.json").exists()


def test_status_sync_tolerates_network_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    monkeypatch.setenv("OPERATOR_STATUS_SYNC_URLS", "https://gist.example/down.json")

    def fail(_url):
        raise urllib.error.URLError("offline")

    monkeypatch.setattr(status_sync, "_fetch_json", fail)
    recipe = status_sync.StatusSync()
    ctx = MagicMock()
    ctx.logger = MagicMock()

    data = asyncio.run(recipe.query(ctx))
    result = asyncio.run(recipe.analyze(ctx, data))

    assert result["accepted"] == []
    assert "offline" in result["rejected"][0]["reason"]


def test_portfolio_health_tracks_synced_ax02_slugs():
    from recipes import portfolio_health

    names = {p["name"] for p in portfolio_health.TRACKED_PROJECTS}
    assert {"ax02-work", "ax02-engine"} <= names

