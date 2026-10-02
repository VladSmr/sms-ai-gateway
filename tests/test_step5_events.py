"""Step 5: non-SMS event handling (delivery reports, heartbeat, app:started),
the reply send path, the trimmer, the watchdog and Telegram alerts.

PRIVACY (152-FZ) is asserted in-process: delivery reasons, health values,
SIM numbers/ICCID never appear in the logs.
"""

import base64
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app import db
from app.config import ConfigManager
from app.gateway import trim_reply, utf16_units
from app.gateway.client import GatewayClient, GatewayError, MockGatewayClient, env_mock_enabled
from app.notify import watchdog as watchdog_mod
from app.notify.telegram import TelegramNotifier
from app.pipeline import MessagePipeline, ReplySendDeferred
from tests.conftest import VALID_CONFIG
from tests.test_pipeline import counters, enqueue_envelope, sms_envelope

WHITELISTED = "+79001234567"


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


class RecordingGateway:
    """Fake gateway client: records sends, optionally always failing."""

    def __init__(self, fail: bool = False, fail_status: int = 503) -> None:
        self.calls: list[dict] = []
        self.fail = fail
        self.fail_status = fail_status

    def send(
        self,
        *,
        text: str,
        phone_number: str,
        send_id: str | None = None,
        max_attempts: int = 3,
    ) -> str:
        self.calls.append({"text": text, "phone_number": phone_number, "send_id": send_id})
        if self.fail:
            raise GatewayError(
                "Gateway send failed: status=%d" % self.fail_status,
                status=self.fail_status,
            )
        return "gw-id-1"


class RecordingNotifier:
    """Fake Telegram notifier: records alert texts, never raises."""

    def __init__(self) -> None:
        self.alerts: list[str] = []

    def send_alert(self, text: str) -> None:
        self.alerts.append(text)


@pytest.fixture
def conn():
    connection = db.open_db(":memory:")
    db.run_migrations(connection)
    yield connection
    connection.close()


def make_pipeline(conn, config_file, *, gateway=None, notifier=None) -> MessagePipeline:
    manager = ConfigManager(config_file)
    manager.load_initial()
    return MessagePipeline(conn, manager, gateway_client=gateway, notifier=notifier)


def event_envelope(event: str, payload: dict, message_id: str) -> dict:
    return {
        "deviceId": "dev-1",
        "event": event,
        "id": "env-" + message_id,
        "payload": payload,
        "webhookId": "wh-1",
    }


def delivered_payload(message_id: str = "m1") -> dict:
    return {
        "messageId": message_id,
        "sender": WHITELISTED,
        "recipient": "+79995556677",
        "simNumber": 1,
        "deliveredAt": "2026-09-17T12:00:00+03:00",
    }


def failed_payload(message_id: str = "m1") -> dict:
    return {
        "messageId": message_id,
        "sender": WHITELISTED,
        "recipient": "+79995556677",
        "simNumber": 1,
        "reason": "radio off - DEVICE-SECRET-REASON",
        "failedAt": "2026-09-17T12:00:00+03:00",
    }


# ---------------------------------------------------------------------------
# sms:delivered / sms:failed
# ---------------------------------------------------------------------------


def test_delivered_event_counts_parts(conn, config_file):
    pipeline = make_pipeline(conn, config_file)
    row = enqueue_envelope(
        conn, "d1", event_envelope("sms:delivered", delivered_payload(), "d1")
    )
    assert pipeline.process(row) == ""
    assert counters(conn)["delivered_parts"] == 1
    # No session side effects: delivery reports never touch conversations.
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0


def test_delivered_event_multipart_parts_counted_separately(conn, config_file):
    pipeline = make_pipeline(conn, config_file)
    payload = delivered_payload()
    for i in (1, 2, 3):
        row = enqueue_envelope(
            conn, f"d{i}", event_envelope("sms:delivered", payload, f"d{i}")
        )
        pipeline.process(row)
    assert counters(conn)["delivered_parts"] == 3


def test_failed_event_is_log_only(conn, config_file, caplog):
    pipeline = make_pipeline(conn, config_file)
    row = enqueue_envelope(conn, "f1", event_envelope("sms:failed", failed_payload(), "f1"))
    with caplog.at_level(logging.DEBUG, logger="app"):
        assert pipeline.process(row) == ""
    assert counters(conn)["failed_reports"] == 1
    # PRIVACY: the device-provided reason text is never logged (length only).
    all_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "DEVICE-SECRET-REASON" not in all_logs
    assert "reason_len=%d" % len("radio off - DEVICE-SECRET-REASON") in all_logs


def test_delivered_with_bad_payload_is_dropped(conn, config_file):
    pipeline = make_pipeline(conn, config_file)
    envelope = event_envelope("sms:delivered", {"no_message_id": True}, "bad")
    row = enqueue_envelope(conn, "bad", envelope)
    assert pipeline.process(row) == ""
    assert counters(conn)["dropped_parse"] == 1
    assert "delivered_parts" not in counters(conn)


# ---------------------------------------------------------------------------
# system:ping (heartbeat) and app:started
# ---------------------------------------------------------------------------


def test_ping_records_heartbeat(conn, config_file):
    pipeline = make_pipeline(conn, config_file)
    payload = {"health": {"battery": 88, "signal": 3}}
    row = enqueue_envelope(conn, "p1", event_envelope("system:ping", payload, "p1"))
    assert pipeline.process(row) == ""
    assert counters(conn)["heartbeats"] == 1
    assert db.get_device_state(conn, "last_ping") is not None
    # PRIVACY: only the key count is stored - never the values.
    assert db.get_device_state(conn, "last_ping_health_keys") == "2"


def test_ping_clears_watchdog_flag_and_alerts_recovery(conn, config_file):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    db.set_device_state(conn, "watchdog_alerted", "2026-09-17T00:00:00.000+00:00")
    row = enqueue_envelope(conn, "p1", event_envelope("system:ping", {}, "p1"))
    pipeline.process(row)
    assert db.get_device_state(conn, "watchdog_alerted") is None
    assert notifier.alerts == ["SMS gateway: device heartbeat recovered"]


def test_app_started_updates_state_and_alerts(conn, config_file, caplog):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    payload = {
        "simCards": [
            {"slotIndex": 0, "simNumber": 1, "phoneNumber": "+79001234567",
             "carrierName": "MTS", "iccid": "8970199SECRETICCID"},
        ]
    }
    row = enqueue_envelope(conn, "s1", event_envelope("app:started", payload, "s1"))
    with caplog.at_level(logging.DEBUG, logger="app"):
        assert pipeline.process(row) == ""
    assert counters(conn)["app_restarts"] == 1
    assert db.get_device_state(conn, "last_app_started") is not None
    assert notifier.alerts == ["SMS gateway: phone app (re)started, sims=1"]
    # PRIVACY: the phone number and ICCID from simCards are never logged.
    all_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "+79001234567" not in all_logs
    assert "SECRETICCID" not in all_logs


def test_bad_ping_payload_is_dropped(conn, config_file):
    pipeline = make_pipeline(conn, config_file)
    envelope = event_envelope("system:ping", {"health": "not-a-dict"}, "bad")
    row = enqueue_envelope(conn, "bad", envelope)
    assert pipeline.process(row) == ""
    assert counters(conn)["dropped_parse"] == 1
    assert "heartbeats" not in counters(conn)


# ---------------------------------------------------------------------------
# Reply send path (trim -> gateway) and its failure handling
# ---------------------------------------------------------------------------


def test_reply_is_sent_via_gateway_with_trimmed_text(conn, config_file):
    gateway = RecordingGateway()
    pipeline = make_pipeline(conn, config_file, gateway=gateway)
    long_text = "x" * 500
    row = enqueue_envelope(conn, "m1", sms_envelope(text=long_text))
    sent_text = pipeline.process(row)
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["phone_number"] == WHITELISTED
    # Echo reply is "(mock) " + 500 x's -> hard-trimmed to the 201 budget.
    assert utf16_units(sent_text) <= 201
    assert counters(conn)["sms_sent"] == 1


def test_command_reply_is_sent_too(conn, config_file):
    gateway = RecordingGateway()
    pipeline = make_pipeline(conn, config_file, gateway=gateway)
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="HELP")))
    assert len(gateway.calls) == 1
    assert gateway.calls[0]["phone_number"] == WHITELISTED
    assert counters(conn)["sms_sent"] == 1


def test_gateway_retryable_failure_is_deferred(conn, config_file):
    """W5: a retryable send failure (5xx) raises ReplySendDeferred - the
    worker requeues the row and the reply is replayed from the history,
    so a transient gateway outage no longer drops the reply."""
    gateway = RecordingGateway(fail=True, fail_status=503)
    pipeline = make_pipeline(conn, config_file, gateway=gateway)
    row = enqueue_envelope(conn, "m1", sms_envelope(text="hi"))
    with pytest.raises(ReplySendDeferred):
        pipeline.process(row)
    assert counters(conn)["sms_send_requeued"] == 1
    assert "sms_sent" not in counters(conn)


def test_gateway_client_error_is_absorbed_no_requeue(conn, config_file):
    """Non-retryable send failures (4xx) keep the step-5 semantics: the
    error is absorbed into a fixed reply (counted ``sms_send_failed``),
    no requeue - auth/body errors do not heal by waiting."""
    gateway = RecordingGateway(fail=True, fail_status=400)
    pipeline = make_pipeline(conn, config_file, gateway=gateway)
    row = enqueue_envelope(conn, "m1", sms_envelope(text="hi"))
    result = pipeline.process(row)
    assert result  # the (trimmed) fixed error reply is returned
    assert counters(conn)["sms_send_failed"] == 1
    assert "sms_sent" not in counters(conn)


def test_send_is_skipped_for_dropped_rows(conn, config_file):
    gateway = RecordingGateway()
    pipeline = make_pipeline(conn, config_file, gateway=gateway)
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(sender="MTS", text="spam")))
    assert gateway.calls == []


# ---------------------------------------------------------------------------
# Trimmer (UTF-16 units, sentence boundaries, digit-dot protection)
# ---------------------------------------------------------------------------


def test_short_reply_is_unchanged():
    assert trim_reply("привет") == "привет"


def test_exact_budget_is_unchanged():
    text = "a" * 201
    assert trim_reply(text) == text


def test_emoji_counted_as_two_units():
    # U+1F600 is a surrogate pair: 2 UTF-16 units.
    assert utf16_units("\U0001F600") == 2
    assert trim_reply("\U0001F600" * 101, max_units=201) == "\U0001F600" * 100


def test_cuts_at_last_sentence_boundary():
    first = "Первое предложение. "  # 20 units incl. the space
    second = "Второе предложение. Хвост без точки"
    text = first + second
    trimmed = trim_reply(text, max_units=utf16_units(first) + 5)
    assert trimmed == "Первое предложение."


def test_decimal_dot_is_never_a_cut_point():
    text = "Pi is 3.14 exactly and here is a long tail without any sentence end"
    trimmed = trim_reply(text, max_units=20)
    # The "3.14" dot must not become a cut point: hard cut instead.
    assert trimmed != "Pi is 3.14"
    assert utf16_units(trimmed) <= 20


def test_abbreviation_dot_is_never_a_cut_point():
    text = "И т.д. и т.п. продолжение"  # abbreviation dots have no space after
    trimmed = trim_reply(text, max_units=10)
    assert utf16_units(trimmed) <= 10
    # The abbreviation is NEVER split mid-word: an internal dot ("т|д") is
    # not a cut point, so the cut lands after the completed "т.д." — the
    # documentable trade-off of cutting only at punct + whitespace.
    assert trimmed == "И т.д."


def test_hard_cut_never_splits_surrogate_pair():
    text = "ж" * 200 + "\U0001F600" + "хвост"
    trimmed = trim_reply(text, max_units=201)
    assert utf16_units(trimmed) <= 201
    # 200 'ж' (200 units) + emoji would be 202 -> emoji dropped, tail cut.
    assert trimmed == "ж" * 200


def test_cyrillic_fits_two_units_per_char():
    text = "б" * 150
    assert utf16_units(text) == 150
    assert trim_reply(text) == text


# ---------------------------------------------------------------------------
# Watchdog (device_state based)
# ---------------------------------------------------------------------------


def _ping_now(conn, config_file, notifier=None, seq=[0]):
    """Record a fresh heartbeat through the pipeline (unique event ids -
    the queue UNIQUE(message_id) forbids re-enqueueing the same id)."""
    notifier = notifier or RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    seq[0] += 1
    message_id = "ping-%d" % seq[0]
    pipeline.process(
        enqueue_envelope(conn, message_id, event_envelope("system:ping", {}, message_id))
    )
    return notifier


def test_watchdog_silent_when_no_heartbeat_yet(conn, config_file):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    pipeline.watchdog_tick()
    assert notifier.alerts == []
    assert "watchdog_alerts" not in counters(conn)


def test_watchdog_alerts_once_after_threshold(conn, config_file):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    _ping_now(conn, config_file, notifier)
    # Fresh heartbeat -> healthy.
    pipeline.watchdog_tick()
    assert notifier.alerts == []
    # Simulate silence beyond 3 x 15 min = 45 min.
    stale = (datetime.now(timezone.utc) - timedelta(minutes=50)).isoformat(
        timespec="milliseconds"
    )
    db.set_device_state(conn, "last_ping", stale)
    pipeline.watchdog_tick()
    assert len(notifier.alerts) == 1
    assert "45 min" in notifier.alerts[0]
    assert counters(conn)["watchdog_alerts"] == 1
    # Next tick: flagged -> NO repeat alert (the anti-spam flag is what
    # prevents the second alert).
    pipeline.watchdog_tick()
    assert len(notifier.alerts) == 1
    assert db.get_device_state(conn, "watchdog_alerted") is not None


def test_watchdog_recovers_on_next_ping(conn, config_file):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    _ping_now(conn, config_file, notifier)
    stale = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(
        timespec="milliseconds"
    )
    db.set_device_state(conn, "last_ping", stale)
    pipeline.watchdog_tick()
    assert len(notifier.alerts) == 1
    assert "45 min" in notifier.alerts[0]
    assert db.get_device_state(conn, "watchdog_alerted") is not None
    # A fresh ping clears the flag and sends exactly one recovery alert.
    _ping_now(conn, config_file, notifier)
    assert db.get_device_state(conn, "watchdog_alerted") is None
    assert notifier.alerts[-1] == "SMS gateway: device heartbeat recovered"
    # The next sweep sees a FRESH heartbeat: healthy, no new alert.
    assert pipeline.sweep() == 0
    assert len(notifier.alerts) == 2
    assert notifier.alerts.count("SMS gateway: device heartbeat recovered") == 1


def test_watchdog_recovers_on_ping_after_flag(conn, config_file):
    notifier = RecordingNotifier()
    pipeline = make_pipeline(conn, config_file, notifier=notifier)
    # Stale flagged state without a pipeline sweep (as after a restart).
    db.set_device_state(conn, "watchdog_alerted", "2026-09-17T00:00:00.000+00:00")
    _ping_now(conn, config_file, notifier)  # recovery alert on the ping itself
    assert db.get_device_state(conn, "watchdog_alerted") is None
    # The following sweep is healthy: no extra watchdog alert.
    assert pipeline.sweep() == 0
    assert notifier.alerts == ["SMS gateway: device heartbeat recovered"]


# ---------------------------------------------------------------------------
# Telegram notifier
# ---------------------------------------------------------------------------


def test_telegram_unconfigured_is_log_only(caplog):
    notifier = TelegramNotifier()
    assert notifier.configured is False
    with caplog.at_level(logging.INFO, logger="app.notify.telegram"):
        notifier.send_alert("SMS gateway: test alert")
    messages = [record.getMessage() for record in caplog.records]
    assert any("Notify (log-only)" in m for m in messages)


def test_telegram_sends_message_via_bot_api():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"ok": True})

    notifier = TelegramNotifier(
        bot_token="123:abc", chat_id="42", transport=httpx.MockTransport(handler)
    )
    notifier.send_alert("SMS gateway: test")
    assert seen["url"].endswith("/bot123:abc/sendMessage")
    assert seen["body"] == {"chat_id": "42", "text": "SMS gateway: test"}


def test_telegram_failure_never_raises():
    def handler(request):
        raise httpx.ConnectError("boom")

    notifier = TelegramNotifier(
        bot_token="123:abc", chat_id="42", transport=httpx.MockTransport(handler)
    )
    notifier.send_alert("SMS gateway: test")  # must not raise


def test_telegram_http_error_is_swallowed():
    notifier = TelegramNotifier(
        bot_token="123:abc",
        chat_id="42",
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
    )
    notifier.send_alert("SMS gateway: test")  # must not raise


# ---------------------------------------------------------------------------
# Gateway client unit behavior (MockTransport)
# ---------------------------------------------------------------------------


def test_gateway_client_sends_expected_body(tmp_path):
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("Authorization")
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": "gw-777"})

    config_data = json.loads(json.dumps(VALID_CONFIG))
    config_data["gateway"] = {
        "base_url": "https://gw.example.com",
        "username": "user",
        "password": "pass",
        "messages_path": "/api/3rdparty/v1/messages",
    }
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(config_data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    client = GatewayClient(manager, transport=httpx.MockTransport(handler))
    gateway_id = client.send(text="reply", phone_number=WHITELISTED)

    assert gateway_id == "gw-777"
    assert seen["url"].endswith("/api/3rdparty/v1/messages")
    assert seen["auth"] == "Basic " + base64.b64encode(b"user:pass").decode()
    assert seen["body"]["message"] == "reply"
    assert seen["body"]["phoneNumbers"] == [WHITELISTED]
    assert seen["body"]["withDeliveryReport"] is True
    assert len(seen["body"]["id"]) == 36  # our own uuid id


def test_gateway_client_retries_5xx_then_succeeds(tmp_path):
    attempts = {"n": 0}

    def handler(request):
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"id": "gw-1"})

    config_data = json.loads(json.dumps(VALID_CONFIG))
    config_data["gateway"] = {
        "base_url": "https://gw.example.com",
        "username": "u",
        "password": "p",
    }
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(config_data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    sleeps: list[float] = []
    client = GatewayClient(
        manager, transport=httpx.MockTransport(handler), sleep_func=sleeps.append
    )
    assert client.send(text="r", phone_number=WHITELISTED) == "gw-1"
    assert attempts["n"] == 2
    assert sleeps == [0.5]


def test_gateway_client_4xx_is_final(tmp_path):
    config_data = json.loads(json.dumps(VALID_CONFIG))
    config_data["gateway"] = {
        "base_url": "https://gw.example.com",
        "username": "u",
        "password": "p",
    }
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(config_data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    client = GatewayClient(
        manager,
        transport=httpx.MockTransport(lambda request: httpx.Response(401)),
        sleep_func=lambda _s: None,
    )
    with pytest.raises(GatewayError) as exc_info:
        client.send(text="r", phone_number=WHITELISTED)
    assert exc_info.value.status == 401


def test_env_mock_gateway_precedence(monkeypatch):
    monkeypatch.setenv("MOCK_GATEWAY", "TRUE")
    assert env_mock_enabled(os.environ.get) is True
    monkeypatch.setenv("MOCK_GATEWAY", "0")
    assert env_mock_enabled(os.environ.get) is False
    monkeypatch.delenv("MOCK_GATEWAY")
    assert env_mock_enabled(os.environ.get) is False


def test_mock_gateway_client_returns_id():
    assert MockGatewayClient().send(text="t", phone_number="+7") == "mock-gateway-id"


def test_send_id_persists_across_5xx_retry(tmp_path):
    """GatewayClient reuses the same send_id across all retry attempts."""
    import uuid

    fixed_id = str(uuid.uuid4())
    attempts: dict[str, list] = {"ids": []}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        attempts["ids"].append(body["id"])
        # First attempt: 503 -> retry, second attempt: success.
        if len(attempts["ids"]) == 1:
            return httpx.Response(503)
        return httpx.Response(200, json={"id": "gw-final-1"})

    config_data = json.loads(json.dumps(VALID_CONFIG))
    config_data["gateway"] = {
        "base_url": "https://gw.example.com",
        "username": "u",
        "password": "p",
    }
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(config_data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    sleeps: list[float] = []
    client = GatewayClient(
        manager,
        transport=httpx.MockTransport(handler),
        sleep_func=sleeps.append,
        send_id=fixed_id,  # pre-set persistent key
    )
    assert client.send(text="r", phone_number=WHITELISTED) == "gw-final-1"
    assert attempts["ids"] == [fixed_id, fixed_id]  # same id on both attempts
    assert sleeps == [0.5]  # one backoff


def test_send_id_none_generates_fresh_per_call(tmp_path):
    """Without a pre-set send_id a fresh UUID is used (backwards-compatible)."""
    attempts: dict[str, list] = {"ids": []}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        attempts["ids"].append(body["id"])
        return httpx.Response(200, json={"id": "gw-1"})

    config_data = json.loads(json.dumps(VALID_CONFIG))
    config_data["gateway"] = {
        "base_url": "https://gw.example.com",
        "username": "u",
        "password": "p",
    }
    path = tmp_path / "config.yaml"
    path.write_text(json.dumps(config_data), encoding="utf-8")
    manager = ConfigManager(path)
    manager.load_initial()
    client = GatewayClient(
        manager,
        transport=httpx.MockTransport(handler),
        send_id=None,  # default: fresh per call
    )
    client.send(text="a", phone_number=WHITELISTED)
    client.send(text="b", phone_number=WHITELISTED)
    assert len(attempts["ids"]) == 2
    # Two distinct ids.
    assert attempts["ids"][0] != attempts["ids"][1]
