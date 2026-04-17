"""
Shared Supabase client for operator scripts.
Reads credentials from .env file.
"""
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

_client = None


def get_client():
    """Get or create a Supabase client."""
    global _client
    if _client is not None:
        return _client

    from supabase import create_client

    url = os.environ.get("SUPABASE_URL", "")
    key = os.environ.get("SUPABASE_KEY", "")
    if not url or not key:
        raise RuntimeError("SUPABASE_URL and SUPABASE_KEY must be set in .env")

    _client = create_client(url, key)
    return _client


def query_table(table: str, select: str = "*", filters: dict | None = None, limit: int = 100) -> list:
    """Query a Supabase table with optional filters."""
    client = get_client()
    q = client.table(table).select(select)
    if filters:
        for col, val in filters.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                if op == "gte":
                    q = q.gte(col, v)
                elif op == "lte":
                    q = q.lte(col, v)
                elif op == "eq":
                    q = q.eq(col, v)
                elif op == "gt":
                    q = q.gt(col, v)
                elif op == "lt":
                    q = q.lt(col, v)
            else:
                q = q.eq(col, val)
    q = q.limit(limit)
    result = q.execute()
    return result.data if result.data else []


def count_since(table: str, hours: int = 24, time_column: str = "created_at") -> int:
    """Count rows created in the last N hours."""
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    client = get_client()
    result = client.table(table).select("id", count="exact").gte(time_column, since).execute()
    return result.count if result.count else 0


def insert_row(table: str, data: dict) -> dict | None:
    """Insert a row into a Supabase table."""
    client = get_client()
    result = client.table(table).insert(data).execute()
    return result.data[0] if result.data else None
