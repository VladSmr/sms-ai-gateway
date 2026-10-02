"""Step 4: the GigaChat client (OAuth token cache, chat completions,
bounded retries, 422 truncation, mock mode) - all over httpx.MockTransport,
zero real network calls. Fake clocks/sleeps: no real waiting."""

import httpx
import pytest

from app.config import ConfigManager
from app.llm.gigachat import (
    GigaChatClient,
    GigaChatError,
    env_mock_enabled,
)
from tests.conftest import VALID_CONFIG

NOW = 1_780_000_000.0  # fake clock base: a realistic 2026 epoch (seconds)


class FakeClock:
    def __init__(self) -> None:
        self.now = NOW
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def make_client(
    handler,
    config_file,
    clock: FakeClock | None = None,
) -> GigaChatClient:
    """Client over MockTransport with a non-empty test auth key (conftest's
    VALID_CONFIG ships an empty key for the mock-mode fail-fast test)."""
    import copy

    import yaml

    data = copy.deepcopy(VALID_CONFIG)
    data["llm"]["auth_key"] = "test-auth-key"
    path = config_file.parent / "llm-config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    fake = clock or FakeClock()
    return GigaChatClient(
        manager,
        transport=httpx.MockTransport(handler),
        sleep_func=fake.sleep,
        clock=fake,
    )


def oauth_response(expires_at: int) -> httpx.Response:
    return httpx.Response(200, json={"access_token": "tok-1", "expires_at": expires_at})


def chat_response(content: str = "model reply") -> httpx.Response:
    return httpx.Response(
        200, json={"choices": [{"message": {"role": "assistant", "content": content}}]}
    )


# ---------------------------------------------------------------------------
# Unit: expires_at unit heuristic (ms vs seconds conflict in the docs)
# ---------------------------------------------------------------------------


def test_token_ttl_seconds_value_now_plus_5():
    now = 1_000_000.0
    ttl = GigaChatClient._token_ttl_s(now + 5, now)
    assert 4 <= ttl <= 5


def test_token_ttl_milliseconds_value_is_detected():
    # A realistic epoch (2026) makes the units discriminative: seconds would
    # be ~1.78e9, milliseconds ~1.78e12 (above the 1e12 threshold).
    now = 1_780_000_000.0
    ttl = GigaChatClient._token_ttl_s((now + 5) * 1000, now)
    assert 4 <= ttl <= 5


def test_token_ttl_capped_at_29_minutes():
    now = 1_000_000.0
    ttl = GigaChatClient._token_ttl_s(now + 10 * 3600, now)  # absurd 10 h
    assert ttl == 29 * 60


def test_token_ttl_none_or_expired_falls_back_to_safety():
    now = 1_000_000.0
    assert GigaChatClient._token_ttl_s(None, now) == 29 * 60
    assert GigaChatClient._token_ttl_s(now - 10, now) == 29 * 60
    assert GigaChatClient._token_ttl_s(0, now) == 29 * 60


# ---------------------------------------------------------------------------
# OAuth: request shape and errors
# ---------------------------------------------------------------------------


def test_oauth_request_shape(config_file):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        seen["rquid"] = request.headers.get("RqUID")
        seen["body"] = request.read().decode()
        return oauth_response((NOW + 100) * 1000)

    client = make_client(handler, config_file)
    token = client._get_token()
    assert token == "tok-1"
    assert seen["url"].startswith("https://ngw.devices.sberbank.ru:9443/api/v2/oauth")
    assert seen["auth"].startswith("Basic ")
    assert seen["rquid"], "RqUID header is mandatory for OAuth"
    assert seen["body"] == "scope=GIGACHAT_API_PERS"


@pytest.mark.parametrize("status", [401, 403])
def test_oauth_rejection_raises_clear_error(config_file, status):
    client = make_client(lambda request: httpx.Response(status), config_file)
    with pytest.raises(GigaChatError, match="OAuth request rejected"):
        client._get_token()


def test_oauth_unexpected_status_raises(config_file):
    client = make_client(lambda request: httpx.Response(500), config_file)
    with pytest.raises(GigaChatError, match="status=500"):
        client._get_token()


def test_missing_auth_key_raises_without_network(config_file, monkeypatch):
    # Strip auth_key from the config file itself.
    import copy

    import yaml

    data = copy.deepcopy(VALID_CONFIG)
    data["llm"]["auth_key"] = ""

    def handler(request):  # pragma: no cover - must never be reached
        raise AssertionError("no network call expected without an auth key")

    path = config_file.parent / "empty-key.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    client = GigaChatClient(
        manager, transport=httpx.MockTransport(handler), clock=FakeClock()
    )
    with pytest.raises(GigaChatError, match="auth key is not configured"):
        client._get_token()


# ---------------------------------------------------------------------------
# Token cache: lazy, reused while fresh, refetched on expiry/force
# ---------------------------------------------------------------------------


def test_token_cached_until_expiry(config_file):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return oauth_response((NOW + 100) * 1000)

    client = make_client(handler, config_file)
    client._get_token()
    client._get_token()
    client._get_token()
    assert calls["count"] == 1


def test_expired_token_is_refetched(config_file):
    calls = {"count": 0}
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return oauth_response((NOW + 5) * 1000)  # 5 s TTL in ms

    client = make_client(handler, config_file, clock)
    client._get_token()
    clock.now += 10  # past expiry
    client._get_token()
    assert calls["count"] == 2


def test_forced_refresh_ignores_cache(config_file):
    calls = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["count"] += 1
        return oauth_response((NOW + 100) * 1000)

    client = make_client(handler, config_file)
    client._get_token()
    client._get_token(force=True)
    assert calls["count"] == 2


# ---------------------------------------------------------------------------
# Chat completions: request shape, content extraction
# ---------------------------------------------------------------------------


def test_chat_request_shape_and_reply(config_file):
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        seen["body"] = request.read().decode()
        return chat_response("Привет!")

    client = make_client(handler, config_file)
    reply = client.complete(
        [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
        temperature=0.5,
        max_tokens=100,
    )
    assert reply == "Привет!"
    assert seen["url"].startswith("https://api.giga.chat/v1/chat/completions")
    assert seen["auth"] == "Bearer tok-1"
    assert '"temperature":0.5' in seen["body"]
    assert '"model":"GigaChat-2"' in seen["body"]


def test_empty_completion_raises(config_file):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(200, json={"choices": [{"message": {"content": "  "}}]})

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="malformed"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)


# ---------------------------------------------------------------------------
# 401: one token refresh + exactly one retry
# ---------------------------------------------------------------------------


def test_chat_401_refreshes_token_and_retries_once(config_file):
    state = {"oauth": 0, "chat": 0, "chat401": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            state["oauth"] += 1
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        if state["chat"] == 1:
            state["chat401"] += 1
            return httpx.Response(401)
        return chat_response("after refresh")

    client = make_client(handler, config_file)
    reply = client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert reply == "after refresh"
    assert state["chat"] == 2
    assert state["oauth"] == 2  # initial + forced refresh


def test_second_401_is_final(config_file):
    state = {"chat": 0, "oauth": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            state["oauth"] += 1
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        return httpx.Response(401)

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="status=401"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert state["chat"] == 2  # initial + the single 401 retry


def test_401_forced_refresh_transport_error_is_gigachat_error(config_file):
    """A transport error in the FORCED refresh (401 path) must surface as
    GigaChatError, not a raw httpx exception (the pipeline absorbs only
    GigaChatError - a raw one would requeue the row forever)."""
    state = {"oauth": 0, "chat": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            state["oauth"] += 1
            if state["oauth"] == 1:
                return oauth_response((NOW + 100) * 1000)
            # The FORCED refresh (after the chat 401) dies on transport.
            raise httpx.ConnectError("oauth endpoint unreachable")
        state["chat"] += 1
        return httpx.Response(401)

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="token refresh failed"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert state["chat"] == 1  # the 401 response happened exactly once


# ---------------------------------------------------------------------------
# Malformed response bodies: always GigaChatError, never a raw exception
# (a raw one would escape the pipeline's absorption and requeue forever)
# ---------------------------------------------------------------------------


def test_chat_200_html_body_raises_gigachat_error(config_file):
    """A 200 with an HTML body must be final GigaChatError (no retries)."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(200, text="<html>gateway error page</html>")

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="malformed"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)


def test_chat_200_broken_json_raises_gigachat_error(config_file):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(200, text='{"choices": [{"message": {"content":')

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="malformed"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)


@pytest.mark.parametrize(
    "payload",
    [
        {"choices": "not-a-list"},
        {"choices": []},
        {"choices": ["not-a-dict"]},
        {"choices": [{"message": "str"}]},
        {"choices": [{"message": {}}]},
        {"choices": [{"message": {"content": 123}}]},
        {},
        "just a string",
    ],
)
def test_chat_200_unexpected_shapes_raise_gigachat_error(config_file, payload):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(200, json=payload)

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)


def test_chat_200_malformed_body_is_final_no_retries(config_file):
    state = {"chat": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        return httpx.Response(200, text="<html>oops</html>")

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert state["chat"] == 1  # malformed 200 is not retried


def test_oauth_malformed_body_raises_gigachat_error(config_file):
    state = {"oauth": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        state["oauth"] += 1
        return httpx.Response(200, text="<html>proxy error</html>")

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="OAuth response is malformed"):
        client._get_token()
    assert state["oauth"] == 1


def test_oauth_wrong_json_shape_raises_gigachat_error(config_file):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="malformed"):
        client._get_token()


# ---------------------------------------------------------------------------
# 429 / 5xx: bounded retries with fake-clock backoff, Retry-After honored
# ---------------------------------------------------------------------------


def test_429_retries_then_succeeds_with_exponential_backoff(config_file):
    state = {"chat": 0}
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        if state["chat"] < 3:
            return httpx.Response(429)
        return chat_response("ok")

    client = make_client(handler, config_file, clock)
    reply = client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert reply == "ok"
    assert state["chat"] == 3
    assert clock.sleeps == [1.0, 2.0]  # exponential, no real sleeps


def test_retry_after_header_is_respected(config_file):
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(429, headers={"Retry-After": "7"})

    client = make_client(handler, config_file, clock)
    with pytest.raises(GigaChatError):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    # Every retry waits the Retry-After interval (3 attempts -> 2 sleeps).
    assert clock.sleeps == [7.0, 7.0]


def test_retry_after_is_capped_at_request_timeout(config_file):
    """A huge/garbage Retry-After must not put the worker to sleep for hours.

    The cap is the configured LLM timeout (90 s in this config): a single
    worker waits on SMS users, so one header must never stall the queue
    longer than a single attempt itself may take.
    """
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(429, headers={"Retry-After": "3600"})

    client = make_client(handler, config_file, clock)
    with pytest.raises(GigaChatError):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert clock.sleeps == [90.0, 90.0]  # min(3600, timeout_s)


def test_backoff_is_capped_at_request_timeout(config_file):
    """The exponential backoff branch respects the same cap."""
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(503)

    client = make_client(handler, config_file, clock)
    with pytest.raises(GigaChatError):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    # 1 s, 2 s normally - both are far below the 90 s cap; assert the cap
    # does not inflate small delays (regression guard for the min() order).
    assert clock.sleeps == [1.0, 2.0]


def test_5xx_retries_exhaustion_raises(config_file):
    clock = FakeClock()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return httpx.Response(503)

    client = make_client(handler, config_file, clock)
    with pytest.raises(GigaChatError, match="after 3 attempts: status=503"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert clock.sleeps == [1.0, 2.0]


def test_transport_timeout_retries_then_raises(config_file):
    clock = FakeClock()
    state = {"chat": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        raise httpx.ReadTimeout("boom", request=request)

    client = make_client(handler, config_file, clock)
    with pytest.raises(GigaChatError, match="timed out"):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert state["chat"] == 3
    assert clock.sleeps == [1.0, 2.0]


# ---------------------------------------------------------------------------
# 422: history truncated in half, exactly one retry
# ---------------------------------------------------------------------------


def test_422_truncates_history_once_then_succeeds(config_file):
    state = {"chat": 0, "payloads": []}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        import json

        state["payloads"].append(json.loads(request.read().decode("utf-8")))
        state["chat"] += 1
        if state["chat"] == 1:
            return httpx.Response(422)
        return chat_response("ok")

    client = make_client(handler, config_file)
    history = [{"role": "user", "content": "m%d" % i} for i in range(6)]
    messages = [{"role": "system", "content": "sys"}] + history
    reply = client.complete(messages, temperature=0.5, max_tokens=10)
    assert reply == "ok"
    assert state["chat"] == 2
    # 7 messages -> system + last 3 of 6 history entries.
    sent = state["payloads"][1]["messages"]
    assert sent[0] == {"role": "system", "content": "sys"}
    assert [m["content"] for m in sent[1:]] == ["m3", "m4", "m5"]


def test_422_without_truncatable_history_is_final(config_file):
    state = {"chat": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        return httpx.Response(422)

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="422"):
        client.complete(
            [{"role": "system", "content": "sys"}, {"role": "user", "content": "hi"}],
            temperature=0.5,
            max_tokens=10,
        )
    assert state["chat"] == 1  # no pointless retry


# ---------------------------------------------------------------------------
# 400 / 404: no retries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 404])
def test_bad_request_statuses_are_never_retried(config_file, status):
    state = {"chat": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        state["chat"] += 1
        return httpx.Response(status)

    client = make_client(handler, config_file)
    with pytest.raises(GigaChatError, match="status=%d" % status):
        client.complete([{"role": "user", "content": "hi"}], temperature=0.5, max_tokens=10)
    assert state["chat"] == 1


# ---------------------------------------------------------------------------
# Privacy: nothing sensitive in the client's own logs
# ---------------------------------------------------------------------------


def test_client_logs_never_contain_message_or_reply_text(config_file, caplog):
    import logging

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth"):
            return oauth_response((NOW + 100) * 1000)
        return chat_response("SECRET-REPLY-never-logged")

    client = make_client(handler, config_file)
    with caplog.at_level(logging.DEBUG, logger="app"):
        client.complete(
            [
                {"role": "system", "content": "SECRET-SYSTEM-PROMPT"},
                {"role": "user", "content": "SECRET-USER-TEXT"},
            ],
            temperature=0.5,
            max_tokens=10,
        )
    for record in caplog.records:
        rendered = record.getMessage()
        assert "SECRET-" not in rendered
        assert "tok-1" not in rendered
    # Discriminator: metadata IS present.
    all_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "reply_len=%d" % len("SECRET-REPLY-never-logged") in all_logs


# ---------------------------------------------------------------------------
# Mock mode env switch
# ---------------------------------------------------------------------------


def test_env_mock_enabled_variants(monkeypatch):
    import os

    monkeypatch.delenv("MOCK_LLM", raising=False)
    assert env_mock_enabled(os.environ.get) is False
    for value in ("1", "true", "TRUE", "Yes", "on"):
        monkeypatch.setenv("MOCK_LLM", value)
        assert env_mock_enabled(os.environ.get) is True
    for value in ("0", "false", "off", "", "maybe"):
        monkeypatch.setenv("MOCK_LLM", value)
        assert env_mock_enabled(os.environ.get) is False
