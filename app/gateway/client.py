"""SMS sending via the android-sms-gateway Private Server REST API (step 5).

Architecture reminder (docs.sms-gate.app): the Private Server is a separate
Go component running on our VPS (docker-compose, step 6); the phone is a
CLIENT of that server (it initiates the connection itself - NAT-friendly).
Sending an SMS therefore means enqueueing the message on the Private
Server; the phone pulls it down (Ping / FCM / SSE). VPS never talks to the
phone directly.

API (retrieved 2026-09-17):

- ``POST {base_url}{messages_path}`` (default
  ``/api/3rdparty/v1/messages``; the docs are inconsistent about the
  ``/api`` prefix, so the full path is configurable - verified against the
  real deployment in step 6), Basic auth built from the phone app's
  username/password pair (Settings -> Connectivity -> Web server
  credentials, provided via .env), body
  ``{"message": ..., "phoneNumbers": [...], "withDeliveryReport": true,
  "id": "<uuid>"}``. A 2xx means "accepted into the server's queue"
  (asynchronous delivery to the phone); the response carries the gateway's
  message id which is logged (metadata only).

Retry policy: 5xx and transport errors are retried with a small bounded
backoff (3 attempts); 4xx (auth/body errors) are never retried. A final
failure is counted and logged - the user has already received the LLM
reply content, so a dead send must not trigger any further user-facing
action (the failure surfaces in Telegram/metrics, not in the SMS thread).

Idempotency: we generate our own ``id`` (uuid4) per send attempt series;
the gateway deduplicates on it, so a retried 5xx does not double-send.
The id is logged (metadata only); message TEXT never is (152-FZ).

Mock mode (``MOCK_GATEWAY=true`` env or ``gateway.mock: true`` config, env
wins - same precedence as the LLM): no network at all, the send is logged
as ``Gateway mock send`` with metadata. Lets the step-6 e2e run without a
live phone/server.

PRIVACY: message text is NEVER logged; metadata only (ids, sizes, status).
"""

import base64
import logging
import os
import uuid
from typing import TYPE_CHECKING, Any, Callable

import httpx

if TYPE_CHECKING:
    from ..config import ConfigManager

logger = logging.getLogger(__name__)

USER_AGENT = "sms-ai-gateway/0.1"

#: Total attempts per send (first + 2 retries). The send is best-effort
#: (the user already has the reply content), so the budget is intentionally
#: small - the queue must not stall on a dead gateway.
MAX_ATTEMPTS = 3

DEFAULT_MESSAGES_PATH = "/api/3rdparty/v1/messages"

_TRUTHY_ENV = frozenset({"1", "true", "yes", "on"})


class GatewayError(RuntimeError):
    """Final send failure (retries exhausted or a non-retryable response)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def env_mock_enabled(environ_get: Callable[[str], str | None]) -> bool:
    """True when ``MOCK_GATEWAY`` turns gateway mock mode on (env wins over
    the YAML ``gateway.mock`` flag; same precedence rule as the LLM)."""
    value = environ_get("MOCK_GATEWAY")
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_ENV


class GatewayClient:
    """Synchronous client, used from the single worker thread."""

    def __init__(
        self,
        config_manager: "ConfigManager",
        *,
        transport: Any | None = None,
        sleep_func: Callable[[float], None] = None,
        clock: Callable[[], float] = None,
        send_id: str | None = None,
    ) -> None:
        """``transport``/``sleep_func``/``clock`` are test injection points;
        production defaults to real network, real sleeps, real clock.
        ``send_id`` is a pre-set idempotency key (v4+); passed to ``send()``
        unless overridden there."""
        import time as _time

        self._config_manager = config_manager
        self._transport = transport
        self._sleep_func = sleep_func or _time.sleep
        self._clock = clock or _time.time
        self._http: httpx.Client | None = None  # created lazily
        self._send_id = send_id

    def send(
        self,
        *,
        text: str,
        phone_number: str,
        send_id: str | None = None,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> str:
        """Send ``text`` to ``phone_number``; returns the gateway message id.

        ``send_id`` (optional): idempotency key constant across the retries of
        one send.  When provided (v4+ persistent key stamped on the queue row),
        a retried 5xx cannot double-send.  When ``None`` a fresh UUID is
        generated per call (backwards-compatible with pre-v4 queue rows).

        Raises :class:`GatewayError` when the bounded budget is exhausted or
        the request is unrecoverable. PRIVACY: never logs the text.
        """
        config = self._config_manager.get()
        # Instance-level key (v4+), call-level override, or fresh fallback.
        effective_send_id = send_id or self._send_id or str(uuid.uuid4())
        messages_url = config.gateway.base_url.rstrip("/") + config.gateway.messages_path
        # Basic auth from the phone app's credentials (secrets are never
        # logged; the header is rebuilt per attempt like the LLM client's).
        credentials = base64.b64encode(
            f"{config.gateway.username}:{config.gateway.password}".encode("utf-8")
        ).decode("ascii")
        for attempt in range(1, max_attempts + 1):
            try:
                response = self._client().post(
                    messages_url,
                    json={
                        "message": text,
                        "phoneNumbers": [phone_number],
                        "withDeliveryReport": True,
                        "id": effective_send_id,
                    },
                    headers={
                        "Authorization": "Basic " + credentials,
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                        "User-Agent": USER_AGENT,
                    },
                    timeout=config.gateway.timeout_s,
                )
            except httpx.TimeoutException:
                logger.warning("Gateway send timeout: attempt=%d", attempt)
                if attempt >= max_attempts:
                    raise GatewayError("Gateway send timed out after retries") from None
                self._sleep_backoff(attempt)
                continue
            except httpx.TransportError as exc:
                logger.warning(
                    "Gateway transport error: type=%s attempt=%d",
                    type(exc).__name__,
                    attempt,
                )
                if attempt >= max_attempts:
                    raise GatewayError("Gateway transport error after retries") from exc
                self._sleep_backoff(attempt)
                continue

            status = response.status_code
            if 200 <= status < 300:
                gateway_id = self._extract_id(response)
                logger.info(
                    "Gateway send ok: gateway_id=%s send_id=%s text_len=%d "
                    "phone_hash=%s attempt=%d",
                    gateway_id,
                    effective_send_id,
                    len(text),
                    _hash(phone_number),
                    attempt,
                )
                return gateway_id
            if status >= 500 and attempt < max_attempts:
                self._sleep_backoff(attempt)
                logger.info("Gateway send retried: status=%d attempt=%d", status, attempt + 1)
                continue
            # 4xx (auth/body errors) and exhausted 5xx: final.
            raise GatewayError(
                "Gateway send failed: status=%d" % status, status=status
            )
        raise GatewayError("Gateway send failed: retries exhausted")  # pragma: no cover

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _client(self) -> httpx.Client:
        if self._http is None:
            self._http = httpx.Client(transport=self._transport, follow_redirects=False)
        return self._http

    @staticmethod
    def _extract_id(response: httpx.Response) -> str:
        """Pull the gateway message id from the response (metadata only)."""
        try:
            payload = response.json()
            gateway_id = payload.get("id") if isinstance(payload, dict) else None
            if not isinstance(gateway_id, str) or not gateway_id:
                raise ValueError("no id in response")
        except ValueError:
            # The docs' response shape is a plain id string or an object;
            # both are fine, anything else still means "accepted".
            return "(unknown)"
        return gateway_id

    def _sleep_backoff(self, attempt: int) -> None:
        """Bounded backoff: 0.5 s, 1 s (capped at 8.0 s regardless of the
        configured request timeout).  The cap is intentionally separate from
        ``gateway.timeout_s`` — the timeout limits one HTTP request, while
        the backoff cap limits the total retry interval (both are small by
        design because the reply is best-effort: the user already holds the
        content and the queue must not stall on a dead gateway)."""
        delay = min(0.5 * 2.0 ** (attempt - 1), 8.0)
        self._sleep_func(delay)


class MockGatewayClient:
    """Log-only stub (``MOCK_GATEWAY=true``): no network, no credentials.

    Shares the ``send`` signature with :class:`GatewayClient`.
    """

    def __init__(self, send_id: str | None = None) -> None:
        self._send_id = send_id

    def send(
        self,
        *,
        text: str,
        phone_number: str,
        send_id: str | None = None,
        max_attempts: int = MAX_ATTEMPTS,
    ) -> str:
        effective = send_id or self._send_id or "(fresh)"
        logger.info(
            "Gateway mock send: would send text_len=%d phone_hash=%s send_id=%s",
            len(text),
            _hash(phone_number),
            effective,
        )
        return "mock-gateway-id"


def _hash(value: str) -> str:
    """Short stable hash for metadata logs (no raw numbers/addresses)."""
    import hashlib

    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def make_gateway_client(
    config_manager: "ConfigManager",
    *,
    send_id: str | None = None,
) -> GatewayClient | MockGatewayClient:
    """Resolve the client: ``MOCK_GATEWAY`` env wins over ``gateway.mock``.

    ``send_id`` is forwarded to the client instance so it can be reused
    across retries (the pipeline stamps it persistently before processing).
    """
    config = config_manager.get()
    if env_mock_enabled(os.environ.get) or config.gateway.mock:
        return MockGatewayClient(send_id=send_id)
    return GatewayClient(config_manager, send_id=send_id)
