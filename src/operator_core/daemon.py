"""Operator Core daemon entrypoint.

Brings up the full local daemon: HTTP hook surface, scheduler, job runner,
snapshot publisher, and (optionally) the Discord bot. Lifecycle is owned
here; every component is optional and isolated so one failure doesn't
take down the rest.

See docs/operator-run-spec.md for the full spec.
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from dotenv import load_dotenv

from .http_server import serve_http
from .metrics import register_metrics_route
from .remote import register_remote_route
from .runner import JobRunner
from .scheduler import MorningOpsScheduler
from .settings import ConfigError, Settings, load_settings
from .store import JobStore

logger = logging.getLogger("operator_core.daemon")


# --------------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------------


def setup_logging(level_name: str, log_file: Path | None) -> None:
    """Configure root logging for the daemon.

    stderr is always written. When `log_file` is provided, we mirror there
    too (line-buffered UTF-8). Cross-platform - no syslog, no journald.
    """
    level = getattr(logging, level_name.upper(), logging.INFO)
    root = logging.getLogger()
    # Clear any defaults (esp. Vercel / CI that might inject a handler)
    for h in list(root.handlers):
        root.removeHandler(h)
    root.setLevel(level)

    fmt = logging.Formatter(
        fmt="%(asctime)s %(name)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    stream = logging.StreamHandler(stream=sys.stderr)
    stream.setFormatter(fmt)
    root.addHandler(stream)

    if log_file is not None:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_file), mode="a", encoding="utf-8")
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)


# --------------------------------------------------------------------------
# Snapshot publisher loop
# --------------------------------------------------------------------------


class SnapshotPublisherThread(threading.Thread):
    """Background thread that calls the snapshot publisher on a timer.

    Failures are logged and swallowed - a bad Supabase minute should not
    propagate up the daemon.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        interval_sec: int,
        stop_event: threading.Event,
    ) -> None:
        super().__init__(name="operator-snapshot-publisher", daemon=True)
        self.settings = settings
        self.interval = interval_sec
        self.stop_event = stop_event

    def _publish_once(self) -> None:
        # Lazy import so daemon start doesn't require `requests`
        # if the user has disabled the publisher.
        from . import snapshot

        payload = snapshot.build_snapshot(
            status_path=snapshot._status_path_for_kruz_monolith(self.settings),
            db_path=snapshot._db_path_for_kruz_monolith(self.settings),
            watchdog_config_path=snapshot._watchdog_config_path_for_kruz_monolith(
                self.settings
            ),
            settings=self.settings,
        )
        node = os.environ.get("OPERATOR_NODE", "kruz")
        snapshot.publish(payload, node=node)
        summary = payload["summary"]
        logger.info(
            "snapshot published node=%s projects=%d jobs=%d cost=$%.2f",
            node,
            summary["projects"],
            summary["jobs_24h"],
            summary["cost_24h_usd"],
        )

    def run(self) -> None:
        logger.info("snapshot publisher started - every %ds", self.interval)
        # Publish once immediately so /kruz lights up without waiting
        # for the first interval.
        try:
            self._publish_once()
        except Exception as e:
            logger.warning("snapshot publish failed (first): %s", e)

        while not self.stop_event.is_set():
            # Sleep in small increments so shutdown is responsive.
            waited = 0
            while waited < self.interval and not self.stop_event.is_set():
                self.stop_event.wait(1.0)
                waited += 1
            if self.stop_event.is_set():
                break
            try:
                self._publish_once()
            except Exception as e:
                logger.warning("snapshot publish failed: %s", e)


# --------------------------------------------------------------------------
# Daemon orchestration
# --------------------------------------------------------------------------


class Daemon:
    """Single-process daemon owning all background components.

    Call `start()` to bring things up, then either block on `wait()` or let
    signal handlers drive shutdown via `stop()`.
    """

    def __init__(
        self,
        settings: Settings,
        *,
        no_discord: bool = False,
        no_scheduler: bool = False,
        no_snapshot: bool = False,
        snapshot_interval: int = 1800,  # 30 minutes - matches watchdog cadence
    ) -> None:
        self.settings = settings
        self.no_discord = no_discord
        self.no_scheduler = no_scheduler
        self.no_snapshot = no_snapshot
        self.snapshot_interval = snapshot_interval

        self._stop_event = threading.Event()
        self.store: JobStore | None = None
        self.http = None
        self._http_thread: threading.Thread | None = None
        self._scheduler: MorningOpsScheduler | None = None
        self._snapshot_thread: SnapshotPublisherThread | None = None
        self._discord_bot = None
        self._discord_thread: threading.Thread | None = None
        self._runner: JobRunner | None = None
        self._started_at: datetime | None = None

    # ---- lifecycle ----

    def start(self) -> None:
        self._started_at = datetime.now(timezone.utc)
        self.settings.ensure_dirs()

        self.store = JobStore(self.settings.db_path)
        self._runner = JobRunner(self.store)

        self._start_http()
        self._write_status_daemon()

        if not self.no_scheduler:
            self._scheduler = MorningOpsScheduler(self.store, self._runner)
            self._scheduler.start_background()
            logger.info("scheduler started")

        if not self.no_snapshot:
            self._snapshot_thread = SnapshotPublisherThread(
                settings=self.settings,
                interval_sec=self.snapshot_interval,
                stop_event=self._stop_event,
            )
            self._snapshot_thread.start()

        if not self.no_discord:
            self._start_discord()

    def _start_http(self) -> None:
        bind = self.settings.daemon.bind
        port = self.settings.daemon.port
        try:
            self.http = serve_http(self.store, host=bind, port=port)
        except OSError as e:
            # Port conflict / permission denied - fatal. Can't do our job
            # without the HTTP surface (hooks, /ops, /metrics all need it).
            logger.error(
                "could not bind http %s:%d: %s. Try --port or kill the other process.",
                bind,
                port,
                e,
            )
            raise

        register_remote_route(self.store)
        register_metrics_route(self.http, self.store)
        try:
            from .gate_review_routes import register_gate_review_routes
            register_gate_review_routes()
        except Exception as exc:  # noqa: BLE001
            logger.warning("gate_review_routes registration failed: %s", exc)
        self._http_thread = threading.Thread(
            target=self.http.serve_forever,
            name="operator-http",
            daemon=True,
        )
        self._http_thread.start()
        logger.info("http listening on http://%s:%d", bind, port)

    def _start_discord(self) -> None:
        token = os.environ.get("DISCORD_BOT_TOKEN", "")
        if not token:
            logger.info("discord skipped: DISCORD_BOT_TOKEN not set")
            return
        try:
            from .discord_bot import DiscordUnavailable, OperatorDiscordBot
        except ImportError:
            logger.warning(
                "discord skipped: discord.py not installed "
                "(install with `pip install operator-core[discord]`)"
            )
            return

        def _run_bot():
            try:
                OperatorDiscordBot(self.store, self._runner).run(token)
            except DiscordUnavailable as exc:
                logger.warning("discord bot unavailable: %s", exc)
            except Exception as exc:  # noqa: BLE001
                logger.exception("discord bot crashed: %s", exc)

        self._discord_thread = threading.Thread(
            target=_run_bot,
            name="operator-discord",
            daemon=True,
        )
        self._discord_thread.start()
        logger.info("discord bot starting...")

    def _write_status_daemon(self) -> None:
        try:
            from .utils import status as status_mod

            started_iso = (
                self._started_at.isoformat() if self._started_at else None
            )
            status_mod.update_daemon(
                pid=os.getpid(),
                started_at=started_iso,
                uptime_sec=0,
            )
        except Exception as e:
            logger.warning("could not write daemon status: %s", e)

    # ---- status ticker ----

    def status_tick(self) -> None:
        try:
            from .utils import status as status_mod

            if self._started_at is None:
                return
            uptime = (datetime.now(timezone.utc) - self._started_at).total_seconds()
            status_mod.update_daemon(
                pid=os.getpid(),
                started_at=self._started_at.isoformat(),
                uptime_sec=uptime,
            )
        except Exception:
            pass

    # ---- main loop ----

    def wait(self, *, once: bool = False) -> int:
        """Block until stop() is called, or return after one tick if once=True."""
        if once:
            logger.info("--once: performed startup + first snapshot; exiting")
            # Let the snapshot thread do its immediate publish, then exit.
            time.sleep(3.0)
            self.stop()
            return 0

        try:
            while not self._stop_event.is_set():
                self._stop_event.wait(60)
                self.status_tick()
        except KeyboardInterrupt:
            logger.info("keyboard interrupt")
            self.stop()
        return 0

    def stop(self) -> None:
        if self._stop_event.is_set():
            return
        logger.info("shutting down...")
        self._stop_event.set()

        if self._scheduler is not None:
            self._scheduler.stop()

        if self.http is not None:
            try:
                self.http.shutdown()
            except Exception as e:
                logger.warning("http shutdown error: %s", e)

        # Grace period for workers to drain.
        grace = 30
        deadline = time.time() + grace
        if self._http_thread and self._http_thread.is_alive():
            self._http_thread.join(timeout=max(0.0, deadline - time.time()))

        if self._snapshot_thread and self._snapshot_thread.is_alive():
            self._snapshot_thread.join(timeout=5.0)

        # Discord bot owns its own asyncio loop; daemon-thread exit will
        # terminate it when we return from wait().

        logger.info("shutdown complete")


# --------------------------------------------------------------------------
# Signal handling
# --------------------------------------------------------------------------


def _install_signal_handlers(daemon: Daemon) -> None:
    _forced = {"count": 0}

    def _handler(signum, _frame):
        _forced["count"] += 1
        if _forced["count"] >= 2:
            logger.warning("second signal - forcing exit")
            os._exit(1)
        logger.info("signal %s received", signum)
        daemon.stop()

    signal.signal(signal.SIGINT, _handler)
    # SIGTERM on Unix; Windows doesn't deliver SIGTERM to Python but we
    # still register for completeness in case we're wrapped in Cygwin etc.
    if hasattr(signal, "SIGTERM"):
        try:
            signal.signal(signal.SIGTERM, _handler)
        except (ValueError, OSError):
            pass
    # Windows: Ctrl+Break
    if hasattr(signal, "SIGBREAK"):
        try:
            signal.signal(signal.SIGBREAK, _handler)  # type: ignore[attr-defined]
        except (ValueError, OSError):
            pass


# --------------------------------------------------------------------------
# Public entrypoint called by the CLI
# --------------------------------------------------------------------------


def run(
    *,
    host: str | None = None,
    port: int | None = None,
    no_discord: bool = False,
    no_scheduler: bool = False,
    no_snapshot: bool = False,
    once: bool = False,
    snapshot_interval: int = 1800,
    log_level: str = "info",
    log_file: Path | None = None,
) -> int:
    load_dotenv()

    try:
        settings = load_settings(reload=True)
    except ConfigError as exc:
        print(f"operator run: {exc}", file=sys.stderr)
        print("Hint: run `operator init` to bootstrap the config file.", file=sys.stderr)
        return 1

    # CLI --host / --port override config at runtime without mutating the file.
    # DaemonConfig is frozen; use dataclasses.replace.
    if host is not None or port is not None:
        import dataclasses

        settings.daemon = dataclasses.replace(
            settings.daemon,
            bind=host if host is not None else settings.daemon.bind,
            port=port if port is not None else settings.daemon.port,
        )

    # Effective log file: CLI > config ~/.operator/data/operator.log default
    effective_log_file = (
        log_file if log_file is not None else settings.data_dir / "operator.log"
    )
    setup_logging(log_level, effective_log_file)

    logger.info(
        "operator run starting - bind=%s:%d pid=%d",
        settings.daemon.bind,
        settings.daemon.port,
        os.getpid(),
    )

    daemon = Daemon(
        settings,
        no_discord=no_discord,
        no_scheduler=no_scheduler,
        no_snapshot=no_snapshot,
        snapshot_interval=snapshot_interval,
    )
    try:
        daemon.start()
    except OSError:
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("daemon startup failed: %s", exc)
        return 1

    _install_signal_handlers(daemon)

    try:
        return daemon.wait(once=once)
    finally:
        daemon.stop()


# Legacy direct-module invocation kept for back-compat.
def main() -> int:
    parser = argparse.ArgumentParser(description="Operator Core daemon")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--no-discord", action="store_true")
    parser.add_argument("--no-scheduler", action="store_true")
    parser.add_argument("--no-snapshot", action="store_true")
    parser.add_argument("--once", action="store_true", help="Start, publish once, exit")
    parser.add_argument("--snapshot-interval", type=int, default=1800)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--log-file", type=Path, default=None)
    args = parser.parse_args()
    return run(
        host=args.host,
        port=args.port,
        no_discord=args.no_discord,
        no_scheduler=args.no_scheduler,
        no_snapshot=args.no_snapshot,
        once=args.once,
        snapshot_interval=args.snapshot_interval,
        log_level=args.log_level,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
