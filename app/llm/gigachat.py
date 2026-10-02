"""GigaChat REST client (step 4): lazy OAuth token + chat completions.

Protocol (developers.sber.ru docs, retrieved 2026-09-17):

- OAuth: POST ``{oauth_url}`` (default
  ``https://ngw.devices.sberbank.ru:9443/api/v2/oauth``) with
  ``Authorization: Basic <auth_key>``, ``RqUID: <uuid4>`` (mandatory) and a
  form body ``scope=GIGACHAT_API_PERS``. The response carries
  ``{"access_token": ..., "expires_at": <int>}``; the token lives ~30 min.
- Chat: POST ``{base_url}/chat/completions`` (default
  ``https://api.giga.chat/v1/chat/completions`` - the post-17.07.2026
  endpoint) with ``Authorization: Bearer <token>``. RqUID is OAuth-only.
  The server keeps NO context: the caller passes the full ``messages``
  array with exactly one system message first.

Token caching is LAZY (agreed design, PLAN.md step 4): the token is fetched
right before the first chat request, cached until expiry, and refetched
before the next request when it has expired or the chat API answers 401.
There is intentionally NO background refresh timer - the single worker
makes one request at a time, so a lazy fetch is always timely enough.

Retry policy (bounded, decisions documented in the report):

- 429 / 5xx / transport errors: retried within ``MAX_ATTEMPTS`` total
  attempts, exponential backoff (1 s, 2 s); a numeric ``Retry-After``
  header is honored when present (the docs do not promise it).
- 401: force a token refresh, retry exactly once; a second 401 is a broken
  key - fail without more retries.
- 422: context overflow or bad messages layout - truncate the history in
  half (most recent messages kept: fresher context is more informative
  for a chat) and retry exactly once.
- 400 / 404 / other 4xx: never retried - a bad request will not fix itself.
- ANY malformed response body (HTML error page, truncated JSON, unexpected
  shape - including on a 200) is converted to :class:`GigaChatError` and is
  final. This is critical for the worker contract: the pipeline absorbs only
  :class:`GigaChatError`; any other exception would requeue the row and
  repeat the full retry budget before the rest of the queue - an infinite
  retry loop for a permanently broken response.

TLS: both endpoints present NUTS Mintsifry certificates; the deployment
must provide the Russian trusted root CA (README "TLS setup", Dockerfile).
TLS verification is never disabled here.

PRIVACY (152-FZ): message texts, prompts, model replies and the access
token are NEVER logged - statuses, latencies, sizes and attempt counts
only.
"""

import logging
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Callable

import httpx

if TYPE_CHECKING:
    from ..config import ConfigManager

logger = logging.getLogger(__name__)

USER_AGENT = "sms-ai-gateway/0.1"

#: Exceptions that mean "the response body is not the JSON shape we expect"
#: (html error page, truncated JSON, unexpected structure). All of them are
#: converted to GigaChatError - a malformed response must never escape the
#: client, or the worker would requeue the row forever (see module docstring).
_MALFORMED_RESPONSE_ERRORS = (ValueError, KeyError, TypeError, IndexError, AttributeError)

#: Total attempts per chat call (initial + 2 retries). Freemium allows a
#: single concurrent generation and a SMS user waits tens of seconds, not
#: minutes - a longer budget would stall the single-worker queue.
MAX_ATTEMPTS = 3

#: The token nominally lives 30 minutes; cache it for 29 to never race the
#: expiry (also covers the docs' ms-vs-s ambiguity in ``expires_at``).
TOKEN_SAFETY_TTL_S = 29 * 60
TOKEN_MAX_TTL_S = 30 * 60

_TRUTHY_ENV = frozenset({"1", "true", "yes", "on"})


class GigaChatError(RuntimeError):
    """Final LLM failure (retries exhausted or a non-retryable response)."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def env_mock_enabled(environ_get: Callable[[str], str | None]) -> bool:
    """True when the ``MOCK_LLM`` env var turns mock mode on (set to
    ``1/true/yes/on`` case-insensitively). The env switch wins over the
    YAML ``llm.mock`` flag (deployment-level override)."""
    value = environ_get("MOCK_LLM")
    if value is None:
        return False
    return value.strip().lower() in _TRUTHY_ENV


class GigaChatClient:
    """Synchronous client, safe for use from the single worker thread.

    The httpx client is created lazily (with TLS verification always on);
    ``transport``/``sleep_func``/``clock`` are injection points for tests -
    production code passes nothing and gets real network, real sleeps and
    the real clock.
    """

    def __init__(
        self,
        config_manager: "ConfigManager",
        *,
        transport: Any | None = None,
        sleep_func: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._config_manager = config_manager
        self._transport = transport
        self._sleep_func = sleep_func
        self._clock = clock
        self._http: httpx.Client | None = None  # created lazily
        self._token_lock = threading.Lock()
        self._access_token: str | None = None
        self._token_expires_at = 0.0  # wall-clock seconds

    # ------------------------------------------------------------------
    # OAuth token (lazy cache)
    # ------------------------------------------------------------------

    @staticmethod
    def _token_ttl_s(expires_at: Any, now: float) -> int:
        """Cache TTL for a fresh token, given the server's ``expires_at``.

        The docs disagree on the unit (OpenAPI: milliseconds, the text
        example reads like seconds), so both are handled: values above
        1e12 are treated as milliseconds. A missing or suspicious value
        falls back to the fixed 29-minute safety TTL.
        """
        if expires_at is None:
            return TOKEN_SAFETY_TTL_S
        if expires_at > 1e12:
            expires_at = expires_at / 1000
        ttl = expires_at - now
        if ttl <= 0 or ttl > TOKEN_MAX_TTL_S:
            return TOKEN_SAFETY_TTL_S
        return min(ttl, TOKEN_SAFETY_TTL_S)

    def _get_token(self, *, force: bool = False) -> str:
        config = self._config_manager.get()
        with self._token_lock:
            if (
                not force
                and self._access_token is not None
                and self._clock() < self._token_expires_at
            ):
                return self._access_token
            if not config.llm.auth_key:
                raise GigaChatError(
                    "GigaChat auth key is not configured "
                    "(llm.auth_key / GIGACHAT_AUTH_KEY)"
                )
            response = self._client().post(
                config.llm.oauth_url,
                content="scope=" + config.llm.scope,
                headers={
                    "Authorization": "Basic " + config.llm.auth_key,
                    "RqUID": str(uuid.uuid4()),
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                    "User-Agent": USER_AGENT,
                },
                timeout=config.llm.timeout_s,
            )
            if response.status_code in (401, 403):
                raise GigaChatError(
                    "OAuth request rejected (check GIGACHAT_AUTH_KEY)",
                    status=response.status_code,
                )
            if response.status_code != 200:
                raise GigaChatError(
                    "OAuth request failed: status=%d" % response.status_code,
                    status=response.status_code,
                )
            try:
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("OAuth response is not a JSON object")
                access_token = data["access_token"] if "access_token" in data else None
                if not isinstance(access_token, str) or not access_token:
                    raise ValueError("OAuth response has no access_token")
                expires_at = data.get("expires_at")
            except _MALFORMED_RESPONSE_ERRORS as exc:
                # Malformed OAuth body is final: the endpoint is broken, not
                # transient (and the caller would requeue forever otherwise).
                raise GigaChatError(
                    "OAuth response is malformed: %s" % type(exc).__name__,
                    status=response.status_code,
                ) from exc
            ttl_s = self._token_ttl_s(expires_at, self._clock())
            self._access_token = access_token
            self._token_expires_at = self._clock() + ttl_s
            # The token itself is a secret: never logged, only its lifetime.
            logger.info("GigaChat OAuth token acquired: expires_in_s=%d", int(ttl_s))
            return access_token

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float,
        max_tokens: int,
    ) -> str:
        """Run one chat completion and return the reply text.

        The messages list must already be laid out as ``[system, ...history]``
        (the server keeps no context); on a 422 the history is halved once
        and the request retried. Raises :class:`GigaChatError` when the
        bounded retry budget is exhausted or the request is unrecoverable.
        """
        config = self._config_manager.get()
        current = list(messages)
        token_refreshed = False
        truncated = False
        attempts = 0
        started = time.perf_counter()
        while True:
            attempts += 1
            try:
                response = self._post_chat(config, current, temperature, max_tokens)
            except httpx.TimeoutException:
                logger.warning("LLM transport timeout: attempt=%d", attempts)
                if attempts >= MAX_ATTEMPTS:
                    raise GigaChatError("LLM request timed out after retries") from None
                self._sleep_backoff(attempts)
                continue
            except httpx.TransportError as exc:
                # Metadata only - transport errors carry URLs, never bodies.
                logger.warning(
                    "LLM transport error: type=%s attempt=%d",
                    type(exc).__name__,
                    attempts,
                )
                if attempts >= MAX_ATTEMPTS:
                    raise GigaChatError("LLM transport error after retries") from exc
                self._sleep_backoff(attempts)
                continue

            status = response.status_code
            if status == 200:
                return self._finish_ok(
                    response, started=started, original_len=len(messages), attempts=attempts
                )
            if status == 401 and not token_refreshed:
                logger.info("LLM call rejected (401): refreshing token, retry=1")
                try:
                    self._get_token(force=True)
                except GigaChatError:
                    raise
                except Exception as exc:
                    # A transport error in the forced refresh must not escape
                    # the retry matrix: the pipeline absorbs only
                    # GigaChatError, anything else would requeue forever.
                    raise GigaChatError(
                        "LLM token refresh failed after 401 (transport)"
                    ) from exc
                token_refreshed = True
                continue
            if status == 422 and not truncated:
                truncated_messages = self._truncate_history(current)
                if truncated_messages is None:
                    raise GigaChatError(
                        "LLM rejected the request (422); history too short to truncate",
                        status=422,
                    )
                current = truncated_messages
                truncated = True
                logger.info(
                    "LLM 422: history truncated, messages=%d, retry=1", len(current)
                )
                continue
            if status == 429 or status >= 500:
                if attempts < MAX_ATTEMPTS:
                    self._sleep_backoff(attempts, response.headers.get("Retry-After"))
                    logger.info(
                        "LLM call retried: status=%d attempt=%d", status, attempts + 1
                    )
                    continue
                raise GigaChatError(
                    "LLM call failed after %d attempts: status=%d" % (attempts, status),
                    status=status,
                )
            # 400/404/other 4xx: not retryable.
            raise GigaChatError(
                "LLM request failed: status=%d" % status, status=status
            )

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _client(self) -> httpx.Client:
        if self._http is None:
            # TLS verification stays ON (httpx default); the deployment must
            # provide the Russian trusted root CA in the CA bundle.
            self._http = httpx.Client(transport=self._transport, follow_redirects=False)
        return self._http

    def _post_chat(
        self, config: Any, messages: list[dict[str, str]], temperature: float, max_tokens: int
    ) -> httpx.Response:
        token = self._get_token()
        return self._client().post(
            config.llm.base_url + "/chat/completions",
            json={
                "model": config.llm.model,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            headers={
                "Authorization": "Bearer " + token,
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
            timeout=config.llm.timeout_s,
        )

    def _finish_ok(
        self,
        response: httpx.Response,
        *,
        started: float,
        original_len: int,
        attempts: int,
    ) -> str:
        latency_ms = int((time.perf_counter() - started) * 1000)
        try:
            content = self._extract_content(response.json())
        except GigaChatError:
            raise
        except _MALFORMED_RESPONSE_ERRORS as exc:
            # A 200 with a malformed body (HTML error page from a proxy,
            # truncated JSON, unexpected shape) must be final, not a
            # requeue-loop trigger - see the module docstring.
            raise GigaChatError(
                "LLM 200 response is malformed: %s" % type(exc).__name__,
                status=200,
            ) from exc
        logger.info(
            "LLM call ok: latency_ms=%d messages=%d attempts=%d reply_len=%d",
            latency_ms,
            original_len,
            attempts,
            len(content),
        )
        return content

    @staticmethod
    def _extract_content(payload: Any) -> str:
        choices = payload["choices"]  # KeyError -> GigaChatError (caller)
        if not isinstance(choices, list) or not choices:
            raise ValueError("no non-empty choices list")
        message = choices[0].get("message", {})
        content = message.get("content")
        if not isinstance(content, str) or not content.strip():
            raise ValueError("empty or non-string completion content")
        return content

    def _sleep_backoff(
        self, attempt: int, retry_after: str | None = None, *, max_delay_s: float | None = None
    ) -> None:
        """Exponential backoff (1 s, 2 s, capped), honoring numeric Retry-After.

        A numeric ``Retry-After`` is capped at ``max_delay_s`` (the request
        timeout by default): a misbehaving server must not be able to put
        the single worker to sleep for minutes/hours with one header - the
        user waits on SMS, and the queue must keep moving.
        """
        if max_delay_s is None:
            max_delay_s = self._config_manager.get().llm.timeout_s
        if retry_after is not None:
            try:
                delay = max(0.0, float(retry_after))
            except ValueError:
                delay = None
            if delay is not None:
                self._sleep_func(min(delay, max_delay_s))
                return
        self._sleep_func(min(2.0 ** (attempt - 1), max_delay_s, 8.0))

    @staticmethod
    def _truncate_history(messages: list[dict[str, str]]) -> list[dict[str, str]] | None:
        """Half the history (most recent kept); system prompt stays first.

        Returns ``None`` when there is no history beyond the system message
        (retrying would repeat the same payload).
        """
        system, history = messages[0], messages[1:]
        if len(history) < 2:
            return None
        keep = max(1, len(history) // 2)
        return [system] + history[-keep:]
