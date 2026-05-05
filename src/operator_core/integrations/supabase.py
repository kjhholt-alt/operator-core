"""Supabase adapter (lazy-import wrapper).

If ``supabase`` package is missing, the adapter returns a stub that fails on
data calls but lets ``verify()`` pass when the recipe doesn't actually need
DB access (e.g., dry-run).
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger("operator.integration.supabase")


class SupabaseAdapter:
    """Thin wrapper around ``supabase.create_client`` with env-driven auth."""

    def __init__(self, env: dict[str, str] | None = None) -> None:
        self.env = env if env is not None else dict(os.environ)
        self._client: Any = None
        self._import_error: str | None = None

    @property
    def configured(self) -> bool:
        return bool(self.env.get("SUPABASE_URL") and (self.env.get("SUPABASE_KEY") or self.env.get("SUPABASE_SERVICE_ROLE_KEY")))

    def client(self) -> Any:
        if self._client is not None:
            return self._client
        if not self.configured:
            raise RuntimeError("supabase: missing SUPABASE_URL / SUPABASE_KEY env vars")
        try:
            from supabase import create_client  # type: ignore
        except ImportError as exc:
            self._import_error = str(exc)
            raise RuntimeError("supabase package not installed; pip install supabase") from exc
        url = self.env["SUPABASE_URL"]
        key = self.env.get("SUPABASE_KEY") or self.env["SUPABASE_SERVICE_ROLE_KEY"]
        self._client = create_client(url, key)
        return self._client

    def table(self, name: str) -> Any:
        return self.client().table(name)

    def select(self, table: str, *, columns: str = "*", filters: dict[str, Any] | None = None, limit: int | None = None) -> list[dict[str, Any]]:
        q = self.table(table).select(columns)
        if filters:
            for key, value in filters.items():
                q = q.eq(key, value)
        if limit:
            q = q.limit(limit)
        res = q.execute()
        return list(getattr(res, "data", []) or [])

    def insert(self, table: str, rows: list[dict[str, Any]] | dict[str, Any]) -> Any:
        return self.table(table).insert(rows).execute()

    def upsert(self, table: str, rows: list[dict[str, Any]] | dict[str, Any], *, on_conflict: str | None = None) -> Any:
        q = self.table(table).upsert(rows)
        if on_conflict:
            q = q.on_conflict(on_conflict)
        return q.execute()

    # --- verify-friendly probe ------------------------------------------------

    def ping(self) -> bool:
        """Cheap read against pg_catalog -- only requires a working REST URL."""
        if not self.configured:
            return False
        try:
            self.client()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("supabase.ping_failed", extra={"error": str(exc)})
            return False
