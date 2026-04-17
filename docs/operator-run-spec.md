# `operator run` — daemon entrypoint spec

**Status:** proposed
**Author:** Kruz + Claude
**Date:** 2026-04-17
**Scope:** v0.1 daemon — Mac + Linux + Windows. Minimum viable to replace the monolith's `py operator-v3.py daemon` on Kruz's machine AND be installable on Uncle's Mac.

---

## Goal

One command — `operator run` — brings up the whole daemon: HTTP hook surface, scheduler, Discord bot, job runner, snapshot publisher. Reads `~/.operator/config.toml`. No hardcoded paths. Graceful shutdown on Ctrl-C or SIGTERM. Works on Mac.

The old `operator-v3.py daemon` works today but imports `from utils import status` (broken in operator-core) and has no signal handling. We're not rewriting it — we're adapting it.

---

## What you type

```bash
operator run                          # start with all components
operator run --no-discord             # skip Discord bot (no token)
operator run --no-scheduler           # skip cron scheduler (http + runner only)
operator run --no-snapshot            # skip snapshot publisher
operator run --once                   # run one scheduler tick, publish one snapshot, exit
operator run --log-level debug        # stderr log verbosity
operator run --log-file FILE          # mirror logs to file (default: ~/.operator/data/operator.log)
operator run --foreground             # run in foreground (default). --background not supported v0.1.
```

All flags have config-file equivalents under `[daemon]`. CLI wins over config.

---

## Components (what actually runs)

Every component is optional and isolated — daemon keeps going if one fails.

### 1. Job store (always on)
- `JobStore` backed by sqlite at `~/.operator/data/operator.sqlite3`
- Migrations run on open
- No threads; shared instance passed to other components

### 2. HTTP hook surface (always on)
- `ThreadingHTTPServer` bound to `config.daemon.bind` (default 127.0.0.1) on `config.daemon.port` (default 8765)
- Routes:
  - `GET /health` — cheap liveness probe
  - `GET /ops` — local HTML dashboard (for Kruz on his machine)
  - `GET /metrics` — Prometheus text format
  - `POST /hooks/claude/pre-tool-use` + `post-tool-use` — Claude Code hooks
  - `POST /jobs` — enqueue a job
  - `POST /remote/command` — HMAC-signed remote trigger (optional)
- Serves in a dedicated thread. Shutdown via `server.shutdown()`.

### 3. Scheduler (default on, `--no-scheduler` skips)
- `MorningOpsScheduler` — in-process cron, ticks every 60s, due tasks get queued via JobStore
- Catches up once if the machine was asleep past a scheduled time
- Scheduler state persisted to `~/.operator/data/scheduler-state.json` so "already ran today" survives restarts

### 4. Job runner (always on when jobs exist)
- `JobRunner` pulls queued jobs off the store, dispatches to the recipes registry
- Runs in a worker thread per job, up to `config.daemon.max_concurrency` (default 2)
- Every job result persists: exit code, output tail, cost, attempts

### 5. Discord bot (default on if `DISCORD_BOT_TOKEN` is set, `--no-discord` skips)
- `OperatorDiscordBot` — `!op status`, `!op morning`, etc.
- Runs its own asyncio event loop on a dedicated thread
- Imports `discord.py` lazily — if not installed, logs and skips (no hard dep)

### 6. Snapshot publisher (default on, `--no-snapshot` skips)
- **New in v0.1:** runs `operator_core.snapshot.publish()` on a timer (default every 15 min)
- Reads local state, POSTs to Supabase if creds configured; otherwise skipped with a warning
- Lives in a dedicated thread, backoff on error

---

## Startup sequence

```
┌─ load_settings()                    # reads ~/.operator/config.toml; fail fast if invalid
│
├─ setup_logging()                    # stream handler + optional file handler
│
├─ JobStore(settings)                 # opens sqlite, runs migrations
│
├─ HttpServer.start(store)            # bind + serve in thread
│   └─ log: "http listening on 127.0.0.1:8765"
│
├─ JobRunner(store).start_worker()    # worker thread; pulls queued jobs
│
├─ if not --no-scheduler:
│   └─ MorningOpsScheduler.start_background()
│       └─ log: "scheduler started — N tasks registered"
│
├─ if not --no-snapshot:
│   └─ SnapshotPublisher.start_background(interval=900)
│       └─ log: "snapshot publisher started — every 15m"
│
├─ if not --no-discord and DISCORD_BOT_TOKEN:
│   └─ OperatorDiscordBot.start_background(token)
│       └─ log: "discord bot online"
│
├─ install_signal_handlers()          # SIGINT, SIGTERM → shutdown
│
└─ main loop: sleep + update status file once/minute until shutdown
```

Each component failure is caught, logged, and non-fatal — daemon continues with a reduced feature set rather than crashing. One clearly-scoped exception: if HTTP fails to bind (port conflict), we exit non-zero since the whole point of the daemon is its HTTP surface.

---

## Shutdown sequence (graceful)

On SIGINT or SIGTERM:

1. Log `"shutting down..."`
2. Stop the scheduler (sets `_stop` event, worker exits after current tick)
3. Tell runner to drain current jobs, reject new ones (wait up to 30s)
4. Shutdown HTTP server (`server.shutdown()`)
5. Close Discord bot (`bot.close()` on its loop)
6. Close JobStore (flush + sqlite close)
7. Final snapshot publish (optional, best-effort)
8. Exit 0

If second SIGINT arrives during shutdown → immediate exit 1.

Windows: signal handling uses `signal.SIGBREAK` fallback (Ctrl+Break).
Mac/Linux: standard `signal.SIGTERM` + `signal.SIGINT`.

---

## Config additions

New `[daemon]` keys in `~/.operator/config.toml`:

```toml
[daemon]
bind = "127.0.0.1"               # existing
port = 8765                       # existing
max_concurrency = 2               # NEW — worker threads for runner
log_level = "info"                # NEW — debug|info|warn|error
log_file = "~/.operator/data/operator.log"  # NEW — optional mirror to file
shutdown_grace_sec = 30           # NEW — how long to wait for drain

[daemon.snapshot]
enabled = true                    # NEW
interval_sec = 900                # NEW — 15 min default
node = "kruz"                     # NEW — which key to publish under
redact_projects = false           # NEW — set true to hash project slugs
supabase_url = ""                 # optional; falls back to env SUPABASE_URL
supabase_key = ""                 # optional; falls back to env SUPABASE_SERVICE_ROLE_KEY
```

All new keys have sensible defaults — existing configs keep working without changes.

---

## Platform compatibility

v0.1 must work on Mac, Linux, and Windows. Specific considerations:

- **Mac + Linux**: `signal.SIGTERM`, `signal.SIGINT`. File paths via `pathlib`. No issues.
- **Windows**: `signal.SIGINT` delivery in main thread only. `SIGBREAK` for graceful. We don't use Unix-only syscalls (no `os.fork`, no `resource`). The monolith has been Windows-only in practice but the code is already portable.
- **No hidden-run.vbs / schtasks in core.** `operator run` is a foreground process. Kruz's Windows-specific Task Scheduler wrapping happens in his personal `operator-scripts/` layer, which invokes `operator run` via `run-operator.bat`. Uncle on Mac uses `launchd` or `brew services` — that's documented in `docs/autostart.md` (future sprint), not part of core.

**Testing platforms:**
- Windows 11 (Kruz's box) — primary dogfood
- macOS (spin up VM or ask Uncle for a test once the polish bar is met)
- Linux (CI only: GitHub Actions runs the test suite on ubuntu-latest)

---

## Error handling + recovery

| Failure | Response |
|---|---|
| Config file missing | Exit 1 with message pointing to `operator init` |
| Config file invalid | Exit 1 with line number + field |
| HTTP port in use | Exit 1 with message: "port N in use — try `--port` or kill other process" |
| Scheduler tick raises | Log error, skip that tick, continue |
| Job runner raises | Mark job failed in store, log, continue to next job |
| Discord bot disconnect | Log, reconnect loop (built into discord.py client) |
| Snapshot publish fails | Log, skip, try again next interval (network hiccups are normal) |
| SQLite corrupt | Exit 1 (user must investigate — silent retry would mask real damage) |

No automatic process restart in v0.1. If the daemon exits, it stays exited until the user (or a launchd/schtasks wrapper) restarts it. Restart policy lives in the OS layer, not in operator-core.

---

## Observability

- **Stderr logs** — every component tagged: `[http] bound ...`, `[scheduler] tick: ...`, `[runner] job j_abc123 started`, etc.
- **File log** — if `log_file` set, same stream mirrored (line-buffered, UTF-8).
- **Metrics** — `/metrics` exposes Prometheus counters for jobs queued/running/complete/failed, HTTP request count, snapshot publishes, scheduler ticks.
- **`/ops` dashboard** — existing HTML dashboard served on the local port; shows recent jobs, daemon uptime, scheduled tasks, latest snapshot.
- **Status file** — `~/.operator/data/status.json` still written (Schema v2 compatible) so external consumers (WezTerm status bar, other scripts) keep working.

No remote telemetry. No phone-home. All observation local until the user sends a snapshot.

---

## What's IN v0.1 vs later

**IN v0.1 (this sprint):**
- ✅ `operator run` with all flags above
- ✅ Cross-platform signal handling
- ✅ Startup/shutdown sequence
- ✅ Snapshot publisher on a timer
- ✅ Status file writing
- ✅ `/ops` local dashboard (port from monolith)
- ✅ Existing MorningOpsScheduler default tasks
- ✅ Fix `from utils import status` imports (shim already exists via `operator_core.utils.status`)
- ✅ New tests: daemon startup, graceful shutdown, component isolation

**DEFERRED (later sprints):**
- ❌ `--background` daemonization (use OS tools: `launchd`, `systemd`, `schtasks`)
- ❌ Config hot-reload (restart-on-change is fine for v0.1)
- ❌ Recipe marketplace / plugin loader
- ❌ Remote upgrade channel
- ❌ Telemetry opt-in
- ❌ Multi-node federation
- ❌ Log rotation (let the OS handle it via newsyslog/logrotate; or user wraps in a rotator)

---

## Migration path (Kruz's machine)

Kruz currently runs `operator-v3.py daemon` via `OperatorV3Daemon` scheduled task. When `operator run` ships:

1. `pip install -e C:/Users/Kruz/Desktop/Projects/operator-core` (editable dev install)
2. Kruz's existing `~/.operator/config.toml` already exists (generated during Sprint 2)
3. One test run: `operator run --once` → one scheduler tick, one snapshot, exit
4. If clean: swap `OperatorV3Daemon` scheduled task to invoke `operator run` instead of the monolith's `operator-v3.py daemon`
5. Monolith stays intact (`operator-scripts/` directory) as fallback for two weeks
6. After two weeks of zero issues: the monolith's daemon.py + scheduler + http_server + discord_bot get removed (they're duplicated in operator-core anyway)

---

## Testing plan

**Unit:**
- `test_daemon_startup.py` — mocks every component, verifies start order + flag handling
- `test_daemon_shutdown.py` — sends fake SIGTERM, asserts graceful drain
- `test_snapshot_publisher_loop.py` — time-mocked, verifies interval + backoff
- `test_signal_handling_cross_platform.py` — skipped on Windows where applicable

**Integration (local, opt-in via env var):**
- `test_daemon_live.py` — actually starts `operator run --no-discord --no-scheduler --no-snapshot` on a random port, hits `/health`, shuts down. Runs ~3s. Useful smoke check.

**Manual:**
- Kruz runs `operator run --once` — confirms scheduler ticks and publishes a snapshot
- Kruz runs `operator run` for an hour — confirms HTTP + scheduler + snapshot cadence work end-to-end
- Same on Mac (Uncle's when ready, or Kruz's spare / an EC2 Mac VM for CI)

Pass bar: everything green on Kruz's Windows + at least one Mac smoke test before we declare v0.1.

---

## Open questions for Kruz

1. **Snapshot cadence default** — 15 min OK, or should it match the watchdog's 30 min tick? 15 feels responsive without being chatty.
2. **`--background` support** — worth it for v0.1, or are you OK using Task Scheduler / launchd to manage that externally? I'd lean external; keeps core simple.
3. **`/ops` dashboard** — is the existing HTML page good enough, or should we port it to the Palantir aesthetic from `operator-site/kruz` while we're in there? The second is a fun 2-3 hour polish, but not required.
4. **Log file location** — `~/.operator/data/operator.log` works cross-platform. Any reason to prefer `/var/log/operator/` on Mac/Linux? (I say no — keeps everything under one root.)
5. **Discord bot as optional dep** — in v0.1 `discord.py` is listed under `[project.optional-dependencies]`. To use the bot, you `pip install operator-core[discord]`. OK or should we hard-require it?

---

## Estimated effort

If I get the go, Sprint 3 is about 4-6 hours of focused work:
- 1h: fix imports + adapt existing daemon.py to the new shape
- 1h: signal handling + graceful shutdown
- 1h: snapshot publisher loop integration
- 1h: CLI wiring (`operator run` → `daemon.main()`)
- 1h: unit tests
- 1h: Kruz smoke test + iterate

End state: one command, one daemon, Mac-ready.

---

## Immediate next step

Say "go" → I start Sprint 3 with the imports + signal handling slice first (smallest verifiable win). Hold → I'll park and come back when you're ready, PR'll wait on the branch.
