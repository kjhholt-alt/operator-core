"""Tests for the declarative recipe framework.

Covers: ABC, runtime, lifecycle, registry, verify, schedule, integrations.
40+ assertions across the file.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from operator_core.recipes import (
    Recipe,
    RecipeContext,
    RecipeStatus,
    RecipeRunner,
    register_recipe,
    discover_recipes,
    list_registered_recipes,
    get_registered_recipe,
    run_recipe,
    BudgetExceeded,
)
from operator_core.recipes.registry import clear_registry
from operator_core.recipes.lifecycle import with_retries, with_timeout, RecipeTimeout
from operator_core.recipes.schedule import (
    ScheduledRecipe,
    Schedule,
    parse_schedule_yaml,
    cron_to_schtasks,
    load_schedule,
)
from operator_core.recipes.verify import verify_all
from operator_core._vendor import events_ndjson, status_spec


# --- fixtures -----------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_data_dirs(tmp_path, monkeypatch):
    """Point status + events writes at a tmp dir so tests don't pollute home."""
    monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "status"))
    monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path / "events"))
    # status_spec stub still bakes its dir at import; reload it so the
    # OPERATOR_STATUS_DIR override takes effect. events_ndjson reads its
    # dir on every call, so no reload needed there -- and reloading it
    # invalidates the EventsNdjsonError class identity used elsewhere.
    import importlib

    importlib.reload(status_spec)
    yield


@pytest.fixture(autouse=True)
def reset_registry():
    clear_registry()
    yield
    clear_registry()


# --- helper recipes -----------------------------------------------------------

class _NoopRecipe(Recipe):
    name = "test_noop"
    version = "0.0.1"
    description = "no-op for tests"
    cost_budget_usd = 0.10
    timeout_sec = 5

    async def verify(self, ctx):
        return True


class _BudgetBlowoutRecipe(Recipe):
    name = "test_budget"
    version = "0.0.1"
    cost_budget_usd = 0.05
    timeout_sec = 5

    async def verify(self, ctx):
        return True

    async def query(self, ctx):
        ctx.add_cost(0.10, source="test")
        return {}


class _FailingVerifyRecipe(Recipe):
    name = "test_fail_verify"
    version = "0.0.1"
    timeout_sec = 5

    async def verify(self, ctx):
        return False


class _ErroringRecipe(Recipe):
    name = "test_error"
    version = "0.0.1"
    timeout_sec = 5

    async def verify(self, ctx):
        return True

    async def query(self, ctx):
        raise RuntimeError("boom")


class _SlowRecipe(Recipe):
    name = "test_slow"
    version = "0.0.1"
    timeout_sec = 1

    async def verify(self, ctx):
        return True

    async def query(self, ctx):
        await asyncio.sleep(2)


# --- ABC + context tests ------------------------------------------------------

class TestRecipeABC:
    def test_abstract_recipe_cannot_skip_verify(self):
        with pytest.raises(TypeError):
            class _Bad(Recipe):
                name = "bad"
            _Bad()  # pragma: no cover -- verify is abstract

    def test_recipe_metadata_defaults(self):
        r = _NoopRecipe()
        assert r.name == "test_noop"
        assert r.version == "0.0.1"
        assert r.cost_budget_usd == 0.10
        assert r.timeout_sec == 5

    def test_context_add_cost_accumulates(self):
        ctx = RecipeContext(recipe_name="x", correlation_id="abc")
        ctx.add_cost(0.01, source="t")
        ctx.add_cost(0.02, source="t")
        assert ctx.cost_so_far == pytest.approx(0.03)

    def test_context_add_cost_raises_when_over_budget(self):
        ctx = RecipeContext(recipe_name="x", correlation_id="abc", cost_budget_usd=0.05)
        with pytest.raises(BudgetExceeded):
            ctx.add_cost(0.10)


# --- runtime tests ------------------------------------------------------------

class TestRecipeRunner:
    def test_runner_runs_clean_recipe(self):
        runner = RecipeRunner(_NoopRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.OK
        assert result.error is None
        assert result.duration_sec >= 0

    def test_runner_dry_run_skips_pipeline(self):
        runner = RecipeRunner(_NoopRecipe(), dry_run=True)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.OK
        assert result.payload.get("dry_run") is True

    def test_runner_marks_verify_failed(self):
        runner = RecipeRunner(_FailingVerifyRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.VERIFY_FAILED

    def test_runner_marks_error_on_exception(self):
        runner = RecipeRunner(_ErroringRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.ERROR
        assert "boom" in result.error

    def test_runner_marks_budget_exceeded(self):
        runner = RecipeRunner(_BudgetBlowoutRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.BUDGET_EXCEEDED

    def test_runner_marks_timeout(self):
        runner = RecipeRunner(_SlowRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.TIMEOUT

    def test_runner_emits_run_events(self, tmp_path, monkeypatch):
        # events writer points at OPERATOR_EVENTS_DIR (set by autouse fixture).
        runner = RecipeRunner(_NoopRecipe(), dry_run=False)
        result = asyncio.run(runner.run())
        assert result.status == RecipeStatus.OK

        events_dir = Path(os.environ["OPERATOR_EVENTS_DIR"])
        runs = events_dir / "runs.ndjson"
        assert runs.exists()
        lines = [json.loads(l) for l in runs.read_text().splitlines() if l.strip()]
        kinds = [e["kind"] for e in lines]
        assert "started" in kinds
        assert "finished" in kinds

    def test_runner_writes_status_component(self, tmp_path, monkeypatch):
        runner = RecipeRunner(_NoopRecipe(), dry_run=False)
        asyncio.run(runner.run())
        status_dir = Path(os.environ["OPERATOR_STATUS_DIR"])
        component = status_dir / "test_noop.json"
        assert component.exists()
        data = json.loads(component.read_text())
        assert data["name"] == "test_noop"
        assert data["status"] == "ok"
        assert data["version"] == "0.0.1"

    def test_runner_correlation_id_unique_per_run(self):
        r1 = RecipeRunner(_NoopRecipe()).correlation_id
        r2 = RecipeRunner(_NoopRecipe()).correlation_id
        assert r1 != r2


# --- registry tests -----------------------------------------------------------

class TestRegistry:
    def test_register_and_lookup(self):
        @register_recipe
        class _Local(Recipe):
            name = "registry_local"

            async def verify(self, ctx):
                return True

        cls = get_registered_recipe("registry_local")
        assert cls is _Local

    def test_register_requires_name(self):
        # A Recipe subclass without `name` should be rejected at definition time.
        with pytest.raises(TypeError):
            class _NoName(Recipe):
                async def verify(self, ctx):
                    return True

    def test_list_registered(self):
        @register_recipe
        class _A(Recipe):
            name = "list_a"

            async def verify(self, ctx):
                return True

        @register_recipe
        class _B(Recipe):
            name = "list_b"

            async def verify(self, ctx):
                return True

        names = [c.name for c in list_registered_recipes()]
        assert "list_a" in names and "list_b" in names

    def test_discover_recipes_loads_directory(self, tmp_path):
        recipe_file = tmp_path / "demo.py"
        recipe_file.write_text(
            "from operator_core.recipes import Recipe, register_recipe\n"
            "@register_recipe\n"
            "class Demo(Recipe):\n"
            "    name = 'demo_discovered'\n"
            "    async def verify(self, ctx):\n"
            "        return True\n",
            encoding="utf-8",
        )
        found = discover_recipes(tmp_path)
        assert any(c.name == "demo_discovered" for c in found)
        assert get_registered_recipe("demo_discovered") is not None


# --- run_recipe convenience --------------------------------------------------

class TestRunRecipe:
    def test_run_recipe_by_name(self):
        register_recipe(_NoopRecipe)
        result = asyncio.run(run_recipe("test_noop"))
        assert result.status == RecipeStatus.OK

    def test_run_recipe_unknown_name_raises(self):
        with pytest.raises(KeyError):
            asyncio.run(run_recipe("does_not_exist"))


# --- verify-all --------------------------------------------------------------

class TestVerifyAll:
    def test_verify_all_passes(self):
        register_recipe(_NoopRecipe)
        report = asyncio.run(verify_all())
        assert report.green
        assert report.passed >= 1

    def test_verify_all_reports_failure(self):
        register_recipe(_FailingVerifyRecipe)
        report = asyncio.run(verify_all())
        assert not report.green
        assert any(name == "test_fail_verify" for name, _ in report.failures)


# --- schedule tests ----------------------------------------------------------

class TestScheduleParser:
    def test_parse_basic_schedule(self):
        text = (
            "version: 1\n"
            "recipes:\n"
            "  - name: foo\n"
            "    cron: \"0 7 * * *\"\n"
            "    enabled: true\n"
            "    notes: bar\n"
            "  - name: baz\n"
            "    cron: \"*/15 * * * *\"\n"
            "    enabled: false\n"
        )
        schedule = parse_schedule_yaml(text)
        assert schedule.version == 1
        assert len(schedule.recipes) == 2
        assert schedule.recipes[0].name == "foo"
        assert schedule.recipes[0].enabled is True
        assert schedule.recipes[1].enabled is False

    def test_load_schedule_missing_file(self, tmp_path):
        schedule = load_schedule(tmp_path / "missing.yaml")
        assert schedule.recipes == []

    def test_cron_to_schtasks_daily(self):
        out = cron_to_schtasks("0 7 * * *")
        assert out == ["/SC", "DAILY", "/ST", "07:00"]

    def test_cron_to_schtasks_minute(self):
        out = cron_to_schtasks("*/15 * * * *")
        assert out == ["/SC", "MINUTE", "/MO", "15"]

    def test_cron_to_schtasks_weekly(self):
        out = cron_to_schtasks("0 9 * * 1-5")
        assert out[0:2] == ["/SC", "WEEKLY"]
        # /D gets a comma-joined day list. Verify both endpoints appear.
        joined = ",".join(out)
        assert "MON" in joined and "FRI" in joined

    def test_cron_to_schtasks_unsupported(self):
        out = cron_to_schtasks("not a cron")
        assert out[0] == "UNSUPPORTED"


# --- lifecycle helpers --------------------------------------------------------

class TestLifecycleHelpers:
    def test_with_timeout_completes(self):
        async def quick():
            return 42
        assert asyncio.run(with_timeout(quick(), 5)) == 42

    def test_with_timeout_raises(self):
        async def slow():
            await asyncio.sleep(2)
        with pytest.raises(RecipeTimeout):
            asyncio.run(with_timeout(slow(), 1))

    def test_with_retries_succeeds_eventually(self):
        attempts = {"n": 0}

        async def flaky():
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise RuntimeError("nope")
            return "done"

        assert asyncio.run(with_retries(flaky, retries=3)) == "done"
        assert attempts["n"] == 3

    def test_with_retries_does_not_retry_budget_exceeded(self):
        attempts = {"n": 0}

        async def boom():
            attempts["n"] += 1
            raise BudgetExceeded("over")

        with pytest.raises(BudgetExceeded):
            asyncio.run(with_retries(boom, retries=5))
        assert attempts["n"] == 1


# --- integrations smoke tests -------------------------------------------------

class TestIntegrationsSmoke:
    def test_discord_adapter_no_webhook(self):
        from operator_core.integrations.discord import DiscordAdapter

        adapter = DiscordAdapter(env={})
        assert adapter.webhook_url("projects") is None
        assert adapter.notify(channel="projects", title="x", body="y") is False

    def test_supabase_adapter_unconfigured(self):
        from operator_core.integrations.supabase import SupabaseAdapter

        adapter = SupabaseAdapter(env={})
        assert adapter.configured is False
        assert adapter.ping() is False

    def test_anthropic_adapter_unconfigured(self):
        from operator_core.integrations.anthropic import AnthropicAdapter

        adapter = AnthropicAdapter(env={})
        assert adapter.configured is False
        assert adapter.ping() is False

    def test_anthropic_estimate_cost(self):
        from operator_core.integrations.anthropic import AnthropicAdapter

        adapter = AnthropicAdapter(env={"ANTHROPIC_API_KEY": "fake"})
        cost = adapter.estimate_cost("claude-haiku-4-5", 1_000_000, 1_000_000)
        assert cost > 0


# --- vendored stubs round-trip ------------------------------------------------

class TestVendorStubs:
    def test_status_spec_writes_aggregate(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPERATOR_STATUS_DIR", str(tmp_path / "s"))
        import importlib
        from operator_core._vendor import status_spec as ss

        importlib.reload(ss)
        ss.write_component_status("alpha", "ok", duration_sec=1.0, cost_usd=0.0, version="1")
        ss.write_component_status("beta", "warn", duration_sec=1.0, cost_usd=0.0, version="1")
        agg = ss.read_aggregate()
        assert agg["overall"] == "warn"
        assert "alpha" in agg["components"]
        assert "beta" in agg["components"]

    def test_events_ndjson_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OPERATOR_EVENTS_DIR", str(tmp_path / "e"))
        from operator_core._vendor import events_ndjson as en

        en.append_event("cost", "test_kind", recipe="r", correlation_id="c", payload={"amount_usd": 0.5})
        rows = en.read_events("cost")
        assert len(rows) == 1
        assert rows[0]["kind"] == "test_kind"
        assert rows[0]["amount_usd"] == 0.5


# --- discovered recipes -------------------------------------------------------

class TestBundledRecipes:
    def test_repo_recipes_directory_loads_all(self):
        repo_root = Path(__file__).resolve().parent.parent
        recipes_dir = repo_root / "recipes"
        assert recipes_dir.exists(), f"missing: {recipes_dir}"
        found = discover_recipes(recipes_dir)
        names = {c.name for c in found}
        # At least the first 5 migrated recipes should be discoverable.
        for required in {"morning_briefing", "pr_reviewer", "outreach_pulse", "deploy_checker", "watchdog"}:
            assert required in names, f"recipe {required} not discovered"

    def test_each_bundled_recipe_has_verify_method(self):
        repo_root = Path(__file__).resolve().parent.parent
        recipes_dir = repo_root / "recipes"
        found = discover_recipes(recipes_dir)
        for cls in found:
            # `verify` must not be abstract.
            assert "verify" not in cls.__abstractmethods__, f"{cls.name}.verify still abstract"
