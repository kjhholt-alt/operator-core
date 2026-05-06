"""status_sync -- pull remote status-spec/v1 documents into local status dir."""

from __future__ import annotations

import json
import os
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from operator_core.recipes import Recipe, RecipeContext, register_recipe
from recipes.portfolio_health import _is_valid_status_spec_doc


def _status_dir() -> Path:
    return Path(os.environ.get("OPERATOR_STATUS_DIR", str(Path.home() / ".operator" / "data" / "status")))


def _urls_from_env() -> list[str]:
    raw = os.environ.get("OPERATOR_STATUS_SYNC_URLS", "")
    return [part.strip() for part in raw.split(",") if part.strip()]


def _fetch_json(url: str, *, timeout: float = 20.0) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": "operator-core/status-sync"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        body = resp.read()
    data = json.loads(body.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("expected JSON object")
    return data


def _write_atomic_json(target: Path, doc: dict[str, Any]) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=target.name + ".", suffix=".tmp", dir=str(target.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")
            fh.flush()
            try:
                os.fsync(fh.fileno())
            except OSError:
                pass
        os.replace(tmp_name, target)
    except Exception:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise
    return target


def _validate_status_doc(doc: dict[str, Any]) -> tuple[bool, str]:
    try:
        from status_spec.validator import validate  # type: ignore

        validate(doc)
        return True, ""
    except ImportError:
        return _is_valid_status_spec_doc(doc)
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


@register_recipe
class StatusSync(Recipe):
    name = "status_sync"
    version = "1.0.0"
    description = "Pull remote status-spec/v1 documents into ~/.operator/data/status"
    cost_budget_usd = 0.0
    schedule = "*/30 * * * *"
    timeout_sec = 120
    discord_channel = None
    requires_clients = ()
    tags = ("status", "sync", "ops")

    async def verify(self, ctx: RecipeContext) -> bool:
        urls = _urls_from_env()
        if not urls:
            ctx.logger.info("status_sync.no_urls", extra={"env": "OPERATOR_STATUS_SYNC_URLS"})
            return False
        _status_dir().mkdir(parents=True, exist_ok=True)
        return _status_dir().exists()

    async def query(self, ctx: RecipeContext) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for url in _urls_from_env():
            try:
                doc = _fetch_json(url)
                rows.append({"url": url, "doc": doc})
            except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
                rows.append({"url": url, "error": str(exc)})
        return rows

    async def analyze(self, ctx: RecipeContext, data: list[dict[str, Any]]) -> dict[str, Any]:
        accepted: list[dict[str, Any]] = []
        rejected: list[dict[str, str]] = []
        for row in data:
            url = row["url"]
            if row.get("error"):
                rejected.append({"url": url, "reason": str(row["error"])})
                continue
            doc = row.get("doc")
            ok, reason = _validate_status_doc(doc)
            if not ok:
                rejected.append({"url": url, "reason": reason})
                continue
            accepted.append({"url": url, "doc": doc, "project": str(doc["project"])})
        return {"accepted": accepted, "rejected": rejected}

    async def format(self, ctx: RecipeContext, result: dict[str, Any]) -> str:
        written: list[dict[str, str]] = []
        for item in result.get("accepted", []):
            project = item["project"]
            target = _status_dir() / f"{project}.json"
            _write_atomic_json(target, item["doc"])
            written.append({"project": project, "path": str(target)})
            ctx.logger.info("status_sync.accepted", extra={"project": project, "path": str(target)})
        for item in result.get("rejected", []):
            ctx.logger.warning("status_sync.rejected", extra={"url": item["url"], "reason": item["reason"]})
        return (
            f"status_sync: accepted {len(written)}, rejected {len(result.get('rejected', []))}"
        )

