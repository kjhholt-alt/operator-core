"""Operator V3 daemon entrypoint."""

from __future__ import annotations

import argparse
import os
import threading
import time
from datetime import datetime

from dotenv import load_dotenv

from .discord_bot import DiscordUnavailable, OperatorDiscordBot
from .http_server import serve_http
from .metrics import register_metrics_route
from .remote import register_remote_route
from .runner import JobRunner
from .scheduler import MorningOpsScheduler
from .store import JobStore


def run_daemon(
    host: str = "127.0.0.1",
    port: int | None = None,
    no_discord: bool = False,
    no_scheduler: bool = False,
) -> int:
    load_dotenv()
    port = port or int(os.environ.get("OPERATOR_HTTP_PORT", "8765"))

    store = JobStore()
    runner = JobRunner(store)

    http = serve_http(store, host=host, port=port)
    register_remote_route(store)
    register_metrics_route(http, store)
    threading.Thread(target=http.serve_forever, name="operator-v3-http", daemon=True).start()
    print(f"Operator V3 HTTP listening on http://{host}:{port}")

    # Write daemon PID and start time to status file
    from utils import status as status_mod
    started_at = datetime.now().isoformat()
    status_mod.update_daemon(pid=os.getpid(), started_at=started_at, uptime_sec=0)

    if not no_scheduler:
        MorningOpsScheduler(store, runner).start_background()
        print("Operator V3 scheduler started")

    token = os.environ.get("DISCORD_BOT_TOKEN", "")
    if not no_discord and token:
        try:
            OperatorDiscordBot(store, runner).run(token)
        except DiscordUnavailable as exc:
            print(exc)
    else:
        print("Discord bot disabled or DISCORD_BOT_TOKEN missing")

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        http.shutdown()
        return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Operator V3 local daemon")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(os.environ.get("OPERATOR_HTTP_PORT", "8765")))
    parser.add_argument("--no-discord", action="store_true")
    parser.add_argument("--no-scheduler", action="store_true")
    args = parser.parse_args()
    return run_daemon(args.host, args.port, args.no_discord, args.no_scheduler)


if __name__ == "__main__":
    raise SystemExit(main())
