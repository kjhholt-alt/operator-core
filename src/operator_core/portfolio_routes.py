"""HTTP route registration for portfolio endpoints.

Plugs into the shared HTTP server via the EXTRA_ROUTES extension point.
Endpoints:
  GET /portfolio       — latest PortfolioSnapshot as JSON
  GET /portfolio/<slug> — single project state
  GET /priorities      — ranked next actions
  GET /blocked         — items blocked on human
  POST /analyze        — trigger analysis (body: {"project": "slug"} or empty for portfolio)
  POST /brief          — generate briefing (body: {"format": "markdown|compact"})
"""

from __future__ import annotations

import json
from typing import Any

from .http_server import register_extra_route


def register_portfolio_routes() -> None:
    """Register all portfolio routes on the shared HTTP server."""

    def _get_portfolio(handler: Any, body: Any) -> tuple[int, dict]:
        from .portfolio import collect_portfolio, load_snapshot, snapshot_to_dict
        from .adapters import load_adapters

        # Try cached first, fall back to live collection
        snapshot = load_snapshot()
        if snapshot is None:
            load_adapters()
            snapshot = collect_portfolio()
        return 200, snapshot_to_dict(snapshot)

    def _get_priorities(handler: Any, body: Any) -> tuple[int, dict]:
        from .portfolio import collect_portfolio, load_snapshot
        from .briefing import priorities_json
        from .adapters import load_adapters

        snapshot = load_snapshot()
        if snapshot is None:
            load_adapters()
            snapshot = collect_portfolio()
        return 200, priorities_json(snapshot)

    def _get_blocked(handler: Any, body: Any) -> tuple[int, dict]:
        from .portfolio import load_snapshot, collect_portfolio
        from .adapters import load_adapters

        snapshot = load_snapshot()
        if snapshot is None:
            load_adapters()
            snapshot = collect_portfolio()
        return 200, {
            "blocked_on_human": snapshot.blocked_on_human,
            "count": len(snapshot.blocked_on_human),
        }

    def _post_analyze(handler: Any, body: dict) -> tuple[int, dict]:
        from .portfolio import collect_project_state, collect_portfolio
        from .analysis import (
            analyze_portfolio_local,
            analyze_project_local,
            build_project_evidence,
            log_analysis,
        )
        from .adapters import get_adapter, load_adapters

        project = (body or {}).get("project")
        if project:
            adapter = get_adapter(project)
            if adapter is None:
                load_adapters()
                adapter = get_adapter(project)
            if adapter is None:
                return 404, {"error": f"Unknown project: {project}"}
            state = collect_project_state(adapter)
            evidence = build_project_evidence(state)
            response = analyze_project_local(state)
            log_analysis(evidence, response)
            return 200, response.to_dict()
        else:
            snapshot = collect_portfolio()
            response = analyze_portfolio_local(snapshot)
            return 200, response.to_dict()

    def _post_brief(handler: Any, body: dict) -> tuple[int, dict | str]:
        from .portfolio import collect_portfolio, load_snapshot
        from .briefing import briefing_markdown, briefing_compact
        from .adapters import load_adapters

        fmt = (body or {}).get("format", "markdown")
        snapshot = load_snapshot()
        if snapshot is None:
            load_adapters()
            snapshot = collect_portfolio()

        if fmt == "compact":
            text = briefing_compact(snapshot)
        else:
            text = briefing_markdown(snapshot)
        return 200, {"format": fmt, "briefing": text}

    register_extra_route("GET", "/portfolio", _get_portfolio)
    register_extra_route("GET", "/priorities", _get_priorities)
    register_extra_route("GET", "/blocked", _get_blocked)
    register_extra_route("POST", "/analyze", _post_analyze)
    register_extra_route("POST", "/brief", _post_brief)
