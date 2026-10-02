"""Telegram alerts (step 5): best-effort, log-only fallback.

Sends short metadata-only messages via the Bot API ``sendMessage``
(HTTPS + standard certifi chain - api.telegram.org is not a NUTS endpoint,
no special CA work needed).

Contract:

- Not configured (``notify.telegram_token``/``telegram_chat_id`` empty) ->
  log-only mode: every call logs the alert text at INFO. This is the
  documented fallback for dev/test environments.
- A Telegram failure must NEVER break SMS processing: timeouts/transport/
  HTTP errors are logged (metadata) and swallowed.
- PRIVACY: alerts carry statuses and counts only - never message text,
  phone numbers, ICCID or health details (152-FZ).
"""

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

TELEGRAM_API_URL = "https://api.telegram.org"
USER_AGENT = "sms-ai-gateway/0.1"


class TelegramNotifier:
    """Best-effort Telegram sender; safe to call from the worker thread."""

    def __init__(
        self,
        *,
        bot_token: str = "",
        chat_id: str = "",
        api_base_url: str = TELEGRAM_API_URL,
        timeout_s: float = 10.0,
        transport: Any | None = None,
    ) -> None:
        """``transport`` is a test injection point (httpx.MockTransport)."""
        self._configured = bool(bot_token and chat_id)
        self._url = "%s/bot%s/sendMessage" % (api_base_url.rstrip("/"), bot_token)
        self._chat_id = chat_id
        self._timeout_s = timeout_s
        self._transport = transport
        self._http: httpx.Client | None = None

    @property
    def configured(self) -> bool:
        return self._configured

    def send_alert(self, text: str) -> None:
        """Deliver an alert; failures degrade to a log line, never raise."""
        if not self._configured:
            logger.info("Notify (log-only): %s", text)
            return
        try:
            response = self._client().post(
                self._url,
                json={"chat_id": self._chat_id, "text": text},
                headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
                timeout=self._timeout_s,
            )
            if 200 <= response.status_code < 300:
                logger.info("Notify sent: telegram ok, text_len=%d", len(text))
                return
            # Non-2xx: logged with status only (body may echo the token).
            logger.warning(
                "Notify failed: telegram status=%d text_len=%d", response.status_code, len(text)
            )
        except httpx.TimeoutException:
            logger.warning("Notify failed: telegram timeout (text_len=%d)", len(text))
        except httpx.TransportError as exc:
            logger.warning(
                "Notify failed: telegram transport type=%s", type(exc).__name__
            )

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(transport=self._transport, follow_redirects=False)
        return self._http
