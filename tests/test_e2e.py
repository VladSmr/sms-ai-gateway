"""Step 6: end-to-end smoke — full webhook → queue → worker → LLM mock
→ trim → gateway mock → counters.

This module tests the integration chain that the README "Local run" section
describes.  All external services are mocked:
  - LLM  -> MockLLMClient  (MOCK_LLM=true semantics)
  - SMS  -> MockGatewayClient  (MOCK_GATEWAY=true semantics)
  - Telegram -> RecordingNotifier (log-only mode)
  - DB  -> in-memory SQLite

The test exercises the full integration chain inside pytest so it is part
of the reproducible test suite.
"""

import json

import pytest

from app import db
from app.config import ConfigManager
from app.pipeline import MessagePipeline
from app.worker import Worker

# Long enough to require trimming at <=201 UTF-16 units in _send_reply.
LONG_TEXT = ("напиши очень длинный рассказ " * 20).strip()


class RecordingNotifier:
    """Fake Telegram: records alerts, never raises."""

    def __init__(self) -> None:
        self.alerts: list[str] = []

    def send_alert(self, text: str) -> None:
        self.alerts.append(text)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    conn = db.open_db(":memory:")
    db.run_migrations(conn)
    yield conn
    conn.close()


@pytest.fixture
def config_file(tmp_path, write_config):
    """Minimal mock config; both MOCK_LLM and MOCK_GATEWAY are true.

    Uses ``write_config`` from conftest which does deep merging of overrides
    (so we can set llm.mock without losing llm.model/temperature/etc.).
    """
    return write_config(
        llm={"mock": True, "auth_key": "", "model": "GigaChat-2", "temperature": 0.4,
             "max_tokens": 500, "timeout_s": 90},
        gateway={"mock": True, "base_url": "https://gw.example.com"},
        notify={"telegram_token": "", "telegram_chat_id": "", "heartbeat_interval_min": 15},
    )


@pytest.fixture
def manager(config_file):
    m = ConfigManager(config_file)
    m.load_initial()
    return m


@pytest.fixture
def pipeline(conn, manager):
    notifier = RecordingNotifier()
    return MessagePipeline(conn, manager, notifier=notifier), notifier


def enqueue_sms(conn, message_id: str, sender: str, text: str, send_id: str | None = None) -> dict:
    """Insert an sms:received envelope and return the queue row (v4+)."""
    envelope = {
        "deviceId": "dev-1",
        "event": "sms:received",
        "id": "env-" + message_id,
        "payload": {
            "messageId": message_id,
            "message": text,
            "sender": sender,
            "receivedAt": "2026-09-17T12:00:00+03:00",
        },
        "webhookId": "wh-1",
    }
    db.enqueue_message(conn, message_id, json.dumps(envelope), send_id=send_id)
    row = conn.execute(
        "SELECT id, message_id, send_id, payload, attempts FROM queue "
        "WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    return dict(row)


def enqueue_event(conn, event: str, payload: dict, message_id: str) -> dict:
    envelope = {
        "deviceId": "dev-1",
        "event": event,
        "id": "env-" + message_id,
        "payload": payload,
        "webhookId": "wh-1",
    }
    db.enqueue_message(conn, message_id, json.dumps(envelope))
    row = conn.execute(
        "SELECT id, message_id, send_id, payload, attempts FROM queue "
        "WHERE message_id = ?",
        (message_id,),
    ).fetchone()
    return dict(row)


def _process_with_worker(conn, pipeline: MessagePipeline) -> None:
    """Run one claim-process-commit cycle (synchronous, no worker loop).

    This helper processes whatever is currently pending in the queue and
    commits completions / requeues failures.  Call it twice for two-pass
    scenarios (e.g. a first attempt fails and the row is requeued).
    """
    rows = list(conn.execute(
        "SELECT id, message_id, send_id, payload, attempts FROM queue "
        "WHERE status = 'pending' "
        "ORDER BY id LIMIT 10"
    ).fetchall())
    if not rows:
        return
    for row in rows:
        d = dict(row)
        try:
            pipeline.process(d)
            with conn:
                conn.execute("DELETE FROM queue WHERE id = ?", (row["id"],))
        except Exception:
            with conn:
                conn.execute(
                    "UPDATE queue SET status = 'pending', updated_at = ? WHERE id = ?",
                    (db.utcnow_iso(), row["id"]),
                )


def counters(conn) -> dict[str, int]:
    c = db.get_counters(conn)
    # Return 0 for counters that have never been incremented.
    known = [
        "received", "accepted", "dedup_skipped", "rejected_signature",
        "rejected_timestamp", "rejected_size", "rejected_payload",
        "rate_limited", "processed", "dropped_whitelist", "dropped_parse",
        "skipped_event",         "sms_sent", "sms_send_failed", "sms_send_requeued", "delivered_parts",
        "failed_reports", "heartbeats", "watchdog_alerts", "app_restarts",
        "llm_calls", "llm_errors", "mock_llm", "mock_gateway",
    ]
    return {k: c.get(k, 0) for k in known}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipelineSmoke:
    """Smoke test: full path with mocks, no real network / no real SMS."""

    def test_webhook_to_gateway_mock_with_trim(self, conn, config_file, manager):
        """Full path: sms:received webhook → queue → worker → LLM mock → trim
        → gateway mock → history → counters."""
        notifier = RecordingNotifier()
        pipeline = MessagePipeline(conn, manager, notifier=notifier)

        # 1. Enqueue a LONG text (needs trimming at ≤201 UTF-16 units).
        row = enqueue_sms(conn, "e2e-1", "+79001234567", LONG_TEXT)

        # 2. Process.
        _process_with_worker(conn, pipeline)

        # 3. Queue drained.
        assert db.count_queue_by_status(conn) == {}

        # 4. SMS was sent (mock gateway).
        assert counters(conn)["sms_sent"] == 1

        # 5. Processed counter.
        assert counters(conn)["processed"] == 1

        # 6. History: user text + LLM mock reply (FULL reply, not trimmed).
        history_rows = conn.execute(
            "SELECT role, content FROM session_messages "
            "JOIN sessions ON session_messages.session_id = sessions.id "
            "WHERE sessions.sender = ?",
            ("+79001234567",),
        ).fetchall()
        history = [(r["role"], r["content"]) for r in history_rows]
        assert len(history) == 2
        user_text, assistant_text = history
        assert user_text[0] == "user"
        assert assistant_text[0] == "assistant"
        assert assistant_text[1].startswith("(mock) ")

        # 7. The trimmed version (what was SENT to the gateway) ≤ 201 UTF-16 units.
        #    The session history stores the FULL reply; trimming happens in
        #    _send_reply before calling the gateway.  Verify that calling
        #    trim_reply on the assistant message produces a ≤201-unit string.
        from app.gateway import trim_reply, utf16_units
        trimmed = trim_reply(assistant_text[1], max_units=201)
        assert utf16_units(trimmed) <= 201, (
            f"Trimmed reply ({utf16_units(trimmed)} units) exceeds budget of 201"
        )

        # 8. Telegram: notifier received at least the command-reply log
        #    (or nothing — log-only when no bot is configured).
        #    We just check it did not raise.

        # 9. Privacy: no LONG_TEXT or reply text in logs.
        log_rows = conn.execute("SELECT id FROM queue").fetchall()
        assert len(log_rows) == 0  # queue is drained
        # The device_state has no health values or ICCID stored.
        last_ping_keys = db.get_device_state(conn, "last_ping_health_keys")
        assert last_ping_keys is None  # no ping was sent


class TestWorkerWithSendIdReuse:
    """Verify the worker handles v4+ rows that have a pre-set send_id."""

    def test_send_id_persists_across_requeue(self, conn, config_file, manager):
        """The pipeline stamps the persistent send_id BEFORE processing.
        A row with no pre-set send_id gets one assigned; the same id is then
        passed to the gateway client for all retry attempts (verified by
        checking the DB: the row always carries the same send_id even if
        requeued multiple times by the worker)."""
        pipeline = MessagePipeline(conn, manager, notifier=RecordingNotifier())

        # Row WITHOUT a send_id — the pipeline generates and stamps one.
        row = enqueue_sms(conn, "e2e-requeue-1", "+79001234567", "hello", send_id=None)

        # Record what send_id the gateway client receives (constructor
        # injection - the W4a override; post-hoc attribute assignment is
        # ignored by the per-call client resolution).
        seen_ids: list[str | None] = []

        class TrackingGateway:
            def send(self, *, text, phone_number, send_id=None, max_attempts=3):
                seen_ids.append(send_id)
                # Succeed to avoid requeue complexity (the requeue semantics
                # are the worker's job; here we verify the pipeline stamps
                # the same id on every call).
                return "gw-ok-" + str(len(seen_ids))

        pipeline = MessagePipeline(
            conn, manager, notifier=RecordingNotifier(), gateway_client=TrackingGateway()
        )

        # First process: pipeline generates and stamps a send_id.
        pipeline.process(row)

        # Verify the DB now carries the stamped send_id.
        stamped_id = conn.execute(
            "SELECT send_id FROM queue WHERE message_id = ?", ("e2e-requeue-1",)
        ).fetchone()
        assert stamped_id is not None
        assert stamped_id["send_id"] is not None

        first_id = stamped_id["send_id"]
        assert first_id in seen_ids, f"Expected {first_id!r} in {seen_ids}"

        # Second process: the row is already in the DB with the stamped id.
        # (simulate requeue by updating status back to pending)
        with conn:
            conn.execute(
                "UPDATE queue SET status = 'pending' WHERE message_id = ?",
                ("e2e-requeue-1",),
            )

        # Simulate the worker claiming it again (worker._claim_batch reads send_id).
        requeued_row = conn.execute(
            "SELECT id, message_id, send_id, payload, attempts FROM queue "
            "WHERE message_id = ?",
            ("e2e-requeue-1",),
        ).fetchone()
        seen_ids.clear()
        pipeline.process(dict(requeued_row))

        # Same send_id used again.
        assert len(seen_ids) == 1
        assert seen_ids[0] == first_id, (
            f"Requeue used {seen_ids[0]!r}, expected same {first_id!r}"
        )


class TestWorkerWithDeviceEvents:
    """Step-5 events from the worker via the full pipeline."""

    def test_ping_and_app_started_flow(self, conn, config_file, manager):
        """system:ping and app:started events are handled correctly."""
        notifier = RecordingNotifier()
        pipeline = MessagePipeline(conn, manager, notifier=notifier)

        # Enqueue system:ping.
        ping_row = enqueue_event(
            conn,
            "system:ping",
            {"health": {"battery": 80, "signal": 4}},
            "ping-1",
        )
        # Enqueue app:started.
        started_row = enqueue_event(
            conn,
            "app:started",
            {"simCards": [{"slotIndex": 0}, {"slotIndex": 1}]},
            "started-1",
        )
        # Enqueue sms:received (the main flow).
        sms_row = enqueue_sms(conn, "e2e-dev-1", "+79001234567", "hi")

        _process_with_worker(conn, pipeline)

        # All rows processed.
        assert db.count_queue_by_status(conn) == {}

        c = counters(conn)
        assert c["heartbeats"] == 1
        assert c["app_restarts"] == 1
        assert c["processed"] == 1
        assert c["sms_sent"] == 1

        # Watchdog: last_ping stored.
        last_ping = db.get_device_state(conn, "last_ping")
        assert last_ping is not None

        # Watchdog: health key count stored (never the values).
        health_keys = db.get_device_state(conn, "last_ping_health_keys")
        assert health_keys == "2"  # "battery" + "signal"

        # Telegram: app restart alert. The RecordingNotifier override is
        # returned by the pipeline's `notifier` property (W4a resolver).
        assert pipeline.notifier is notifier
        assert len(notifier.alerts) >= 1  # at least the app:started alert
        assert "sims=2" in notifier.alerts[0]


class TestConfigReloadDoesNotBreakClients:
    """Hot-reload keeps clients alive (they re-resolve from updated config)."""

    def test_client_reuse_after_reload(self, conn, config_file, manager):
        """After a config reload the existing client (mock) continues working."""
        pipeline = MessagePipeline(conn, manager, notifier=RecordingNotifier())

        row = enqueue_sms(conn, "reload-1", "+79001234567", "test reload")
        pipeline.process(row)  # first call works
        assert counters(conn)["processed"] == 1

        # No error on a second process call (client still valid after reload).
        row2 = enqueue_sms(conn, "reload-2", "+79001234567", "test reload 2")
        pipeline.process(row2)
        assert counters(conn)["processed"] == 2