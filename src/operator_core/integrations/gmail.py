"""Gmail adapter -- thin wrapper around google-api-python-client.

Loads creds via ``GOOGLE_APPLICATION_CREDENTIALS`` or
``OPERATOR_GMAIL_TOKEN_JSON``. If the client libs aren't installed, the
adapter still imports cleanly and ``configured`` returns False.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("operator.integration.gmail")


class GmailAdapter:
    def __init__(self, env: dict[str, str] | None = None) -> None:
        self.env = env if env is not None else dict(os.environ)
        self._service: Any = None

    @property
    def configured(self) -> bool:
        token_path = self.env.get("OPERATOR_GMAIL_TOKEN_JSON") or self.env.get("GOOGLE_APPLICATION_CREDENTIALS")
        return bool(token_path and Path(token_path).exists())

    def service(self) -> Any:
        if self._service is not None:
            return self._service
        if not self.configured:
            raise RuntimeError("gmail: no OPERATOR_GMAIL_TOKEN_JSON or GOOGLE_APPLICATION_CREDENTIALS configured")
        try:
            from google.oauth2.credentials import Credentials  # type: ignore
            from googleapiclient.discovery import build  # type: ignore
        except ImportError as exc:
            raise RuntimeError("install google-api-python-client + google-auth") from exc

        token_path = self.env.get("OPERATOR_GMAIL_TOKEN_JSON") or self.env["GOOGLE_APPLICATION_CREDENTIALS"]
        token_data = json.loads(Path(token_path).read_text(encoding="utf-8"))
        creds = Credentials.from_authorized_user_info(token_data, scopes=["https://www.googleapis.com/auth/gmail.modify"])
        self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)
        return self._service

    def list_messages(self, query: str = "", max_results: int = 25) -> list[dict[str, Any]]:
        svc = self.service()
        res = svc.users().messages().list(userId="me", q=query, maxResults=max_results).execute()
        return list(res.get("messages", []) or [])

    def get_message(self, msg_id: str) -> dict[str, Any]:
        svc = self.service()
        return svc.users().messages().get(userId="me", id=msg_id, format="full").execute()

    def ping(self) -> bool:
        if not self.configured:
            return False
        try:
            self.service()
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("gmail.ping_failed", extra={"error": str(exc)})
            return False
