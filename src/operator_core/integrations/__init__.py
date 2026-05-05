"""Integration adapters shared across recipes.

Each adapter wraps a single external service (Discord, Supabase, Gmail,
Anthropic) and:
- reads creds from the environment (no hard-coded secrets)
- exposes a small, typed surface
- records cost via the events stream when applicable
"""
