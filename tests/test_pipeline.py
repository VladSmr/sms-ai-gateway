"""Steps 3-4: the message pipeline end-to-end (whitelist, routing, commands,
LLM call via mock/fake clients, skip/broken events, worker integration)
plus the privacy guarantees."""

import asyncio
import copy
import json
import logging

import pytest

from app import db
from app.config import ConfigManager
from app.llm.gigachat import GigaChatError
from app.pipeline import LLM_FAILURE_REPLY, MessagePipeline
from app.sessions import store
from app.worker import Worker
from tests.conftest import VALID_CONFIG

WHITELISTED = "+79001234567"


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


class RecordingLLMClient:
    """Fake client with the GigaChatClient.complete signature: records calls."""

    def __init__(self, reply: str = "fake reply") -> None:
        self.calls: list[dict] = []
        self.reply = reply

    def complete(
        self, messages: list[dict[str, str]], *, temperature: float, max_tokens: int
    ) -> str:
        self.calls.append(
            {"messages": messages, "temperature": temperature, "max_tokens": max_tokens}
        )
        return self.reply


class FailingLLMClient:
    """Fake client that always fails (retries exhausted on the client side)."""

    def complete(
        self, messages: list[dict[str, str]], *, temperature: float, max_tokens: int
    ) -> str:
        raise GigaChatError("LLM call failed after 3 attempts: status=503", status=503)


@pytest.fixture
def conn():
    connection = db.open_db(":memory:")
    db.run_migrations(connection)
    yield connection
    connection.close()


@pytest.fixture
def pipeline(conn, config_file):
    """Pipeline in mock mode (conftest config: llm.mock=true) -> echo replies."""
    manager = ConfigManager(config_file)
    manager.load_initial()
    return MessagePipeline(conn, manager)


@pytest.fixture
def recording_client():
    return RecordingLLMClient()


@pytest.fixture
def pipeline_llm(conn, config_file, recording_client):
    """Pipeline with an injected fake LLM client (for precedence checks)."""
    manager = ConfigManager(config_file)
    manager.load_initial()
    return MessagePipeline(conn, manager, llm_client=recording_client)


def sms_envelope(
    *,
    sender: str = WHITELISTED,
    text: str = "hello",
    message_id: str = "m1",
    event: str = "sms:received",
) -> dict:
    return {
        "deviceId": "dev-1",
        "event": event,
        "id": "env-" + message_id,
        "payload": {"messageId": message_id, "message": text, "sender": sender},
        "webhookId": "wh-1",
    }


def enqueue_envelope(conn, message_id: str, envelope: dict, send_id: str | None = None) -> dict:
    """Insert into the queue and return the claimed-style row dict (v4+)."""
    db.enqueue_message(conn, message_id, json.dumps(envelope), send_id=send_id)
    row = conn.execute(
        "SELECT id, message_id, send_id, payload FROM queue WHERE message_id = ?", (message_id,)
    ).fetchone()
    return dict(row)


def counters(conn) -> dict[str, int]:
    return db.get_counters(conn)


def user_contents(conn, session_id: int) -> list[str]:
    """History contents of the user messages only (assistant replies grow
    the history dynamically and are asserted separately)."""
    return [
        item["content"]
        for item in store.get_history(conn, session_id)
        if item["role"] == "user"
    ]


# ---------------------------------------------------------------------------
# Whitelist: silent drop of foreign numeric AND alphanumeric senders
# ---------------------------------------------------------------------------


def test_whitelisted_sender_in_trunk_format_is_processed(pipeline, conn):
    # "8-900-123-45-67" normalizes to the whitelisted +79001234567.
    reply = pipeline.process(
        enqueue_envelope(conn, "m1", sms_envelope(sender="8-900-123-45-67", text="hi"))
    )
    assert reply == "(mock) hi"
    assert counters(conn)["processed"] == 1
    assert "dropped_whitelist" not in counters(conn)


@pytest.mark.parametrize("sender", ["+79001112233", "8-901-111-22-33", "MTS", "MCHS-moskva"])
def test_foreign_senders_are_silently_dropped(pipeline, conn, sender):
    row = enqueue_envelope(
        conn, "drop-" + sender.replace("+", ""), sms_envelope(sender=sender, text="spam")
    )
    reply = pipeline.process(row)
    assert reply == ""
    assert counters(conn)["dropped_whitelist"] == 1
    assert "processed" not in counters(conn)
    # No sessions, no history for dropped senders.
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0


def test_legacy_phone_number_field_is_respected(pipeline, conn):
    envelope = sms_envelope(sender="8-900-123-45-67", text="hi")
    envelope["payload"].pop("sender")
    envelope["payload"]["phoneNumber"] = "8-900-123-45-67"
    reply = pipeline.process(enqueue_envelope(conn, "legacy", envelope))
    assert reply == "(mock) hi"


# ---------------------------------------------------------------------------
# Routing: #N prefix, default session, invalid prefixes
# ---------------------------------------------------------------------------


def test_hash_prefix_routes_to_named_session(pipeline, conn):
    reply = pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="#2 hello")))
    assert reply == "(mock) hello"
    session = store.get_or_create(conn, WHITELISTED, 2, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["hello"]


def test_no_prefix_goes_to_default_session(pipeline, conn):
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["hello"]


def test_invalid_prefix_zero_is_plain_text_for_default(pipeline, conn):
    # Decision (documented in app.pipeline): there is no session 0 - the
    # whole text, prefix included, is treated as ordinary content.
    reply = pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="#0 hello")))
    assert reply == "(mock) #0 hello"
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    # Only the user turn is stored before the LLM call; the mock echo lands
    # as the assistant turn (covered by the dedicated history tests).
    assert user_contents(conn, session.id) == ["#0 hello"]


def test_bare_hash_number_is_plain_text(pipeline, conn):
    # "#2" without text is not a route (no content to process for it).
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="#2")))
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["#2"]


def test_multiline_message_after_prefix(pipeline, conn):
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="#2 line1\nline2")))
    session = store.get_or_create(conn, WHITELISTED, 2, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["line1\nline2"]


# ---------------------------------------------------------------------------
# Commands inside the pipeline + override precedence (via a fake LLM)
# ---------------------------------------------------------------------------


def test_temp_override_beats_yaml_default(pipeline_llm, recording_client, conn):
    # YAML default temperature is 0.4.
    pipeline_llm.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    assert pipeline_llm.process(
        enqueue_envelope(conn, "m2", sms_envelope(text="TEMP 1.5"))
    ).startswith("Session 1 temperature=1.5")
    pipeline_llm.process(enqueue_envelope(conn, "m3", sms_envelope(text="next")))
    assert recording_client.calls[-1]["temperature"] == 1.5


def test_prompt_override_reaches_llm_system_message(pipeline_llm, recording_client, conn):
    pipeline_llm.process(enqueue_envelope(conn, "m1", sms_envelope(text="PROMPT be brief")))
    pipeline_llm.process(enqueue_envelope(conn, "m2", sms_envelope(text="hello")))
    system = recording_client.calls[-1]["messages"][0]
    assert system["role"] == "system"
    assert system["content"] == "be brief"


def test_yaml_system_prompt_is_default_system_message(pipeline_llm, recording_client, conn):
    pipeline_llm.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    system = recording_client.calls[0]["messages"][0]
    assert system["role"] == "system"
    assert system["content"] == VALID_CONFIG["system_prompt"]


def test_reset_restores_yaml_defaults_and_clears_history(pipeline_llm, recording_client, conn):
    pipeline_llm.process(enqueue_envelope(conn, "m1", sms_envelope(text="TEMP 1.5")))
    pipeline_llm.process(enqueue_envelope(conn, "m2", sms_envelope(text="PROMPT be terse")))
    pipeline_llm.process(enqueue_envelope(conn, "m3", sms_envelope(text="first")))
    assert pipeline_llm.process(enqueue_envelope(conn, "m4", sms_envelope(text="RESET"))) == (
        "Session 1 reset"
    )
    pipeline_llm.process(enqueue_envelope(conn, "m5", sms_envelope(text="hello")))
    # Back to the YAML defaults (temperature 0.4, the YAML system prompt).
    assert recording_client.calls[-1]["temperature"] == 0.4
    assert recording_client.calls[-1]["messages"][0]["content"] == VALID_CONFIG["system_prompt"]
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["hello"]


def test_session_scoped_override_does_not_leak(pipeline_llm, recording_client, conn):
    pipeline_llm.process(enqueue_envelope(conn, "m1", sms_envelope(text="#2 TEMP 1.5")))
    # Session 1 (default) stays on the YAML default...
    pipeline_llm.process(enqueue_envelope(conn, "m2", sms_envelope(text="hello")))
    assert recording_client.calls[-1]["temperature"] == 0.4
    # ...while session 2 keeps its override.
    pipeline_llm.process(enqueue_envelope(conn, "m3", sms_envelope(text="#2 hello")))
    assert recording_client.calls[-1]["temperature"] == 1.5


def test_command_is_not_stored_in_history(pipeline, conn):
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="TEMP 1.5")))
    pipeline.process(enqueue_envelope(conn, "m2", sms_envelope(text="LIST")))
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    assert store.get_history(conn, session.id) == []


# ---------------------------------------------------------------------------
# LLM integration: mock echo, assistant history, failure reply, counters
# ---------------------------------------------------------------------------


def test_mock_mode_echoes_without_llm_calls(pipeline, conn):
    # conftest config: llm.mock=true -> zero network, reply = "(mock) <text>".
    reply = pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="#3 echo me")))
    assert reply == "(mock) echo me"
    assert "llm_calls" not in counters(conn)
    assert "llm_errors" not in counters(conn)


def test_env_mock_llm_wins_over_config(pipeline, conn, monkeypatch):
    # MOCK_LLM env is the deployment-level switch: it wins even when the
    # config disables mock mode.
    monkeypatch.setenv("MOCK_LLM", "true")
    reply = pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="env wins")))
    assert reply == "(mock) env wins"
    assert "llm_calls" not in counters(conn)


def test_assistant_reply_is_appended_to_history(pipeline, conn):
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    history = store.get_history(conn, session.id)
    assert [(item["role"], item["content"]) for item in history] == [
        ("user", "hello"),
        ("assistant", "(mock) hello"),
    ]
    assert counters(conn)["processed"] == 1


def test_llm_failure_returns_user_facing_error_reply(conn, config_file):
    manager = ConfigManager(config_file)
    manager.load_initial()
    pipeline = MessagePipeline(conn, manager, llm_client=FailingLLMClient())
    reply = pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    assert reply == LLM_FAILURE_REPLY
    assert reply == "Ошибка, попробуйте позже"
    # The row is still processed (the worker removes it, no infinite retry);
    # llm_calls counts call attempts (1), llm_errors the final failure.
    assert counters(conn)["llm_errors"] == 1
    assert counters(conn)["llm_calls"] == 1
    assert counters(conn)["processed"] == 1


def test_llm_failure_reply_is_not_stored_in_history(conn, config_file):
    manager = ConfigManager(config_file)
    manager.load_initial()
    pipeline = MessagePipeline(conn, manager, llm_client=FailingLLMClient())
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    session = store.get_or_create(conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24)
    assert user_contents(conn, session.id) == ["hello"]
    assert all(item["role"] == "user" for item in store.get_history(conn, session.id))


def test_real_client_increments_llm_calls_counter(conn, config_file, recording_client):
    manager = ConfigManager(config_file)
    manager.load_initial()
    pipeline = MessagePipeline(conn, manager, llm_client=recording_client)
    pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text="hello")))
    assert counters(conn)["llm_calls"] == 1
    assert "llm_errors" not in counters(conn)


def test_worker_e2e_malformed_llm_body_is_final_no_requeue(tmp_path):
    """Review Major, full worker path: the client converts a malformed LLM
    response (HTML on a 200) into GigaChatError -> the pipeline replies with
    the fixed error text and the row is REMOVED (no requeue loop)."""

    class Malformed200Client:
        """Mimics the real client receiving an HTML error page on 200."""

        def complete(self, messages, *, temperature, max_tokens):
            # The real client converts the malformed body internally; this
            # raises the resulting GigaChatError directly.
            raise GigaChatError("LLM 200 response is malformed: JSONDecodeError", status=200)

    async def scenario():
        conn = db.open_db(tmp_path / "malformed.db")
        try:
            db.run_migrations(conn)
            config_file = tmp_path / "config.yaml"
            config_file.write_text(json.dumps(VALID_CONFIG), encoding="utf-8")
            manager = ConfigManager(config_file)
            manager.load_initial()
            pipeline = MessagePipeline(conn, manager, llm_client=Malformed200Client())

            db.enqueue_message(conn, "m1", json.dumps(sms_envelope(text="hello")))
            worker = Worker(
                conn,
                process_func=pipeline.process,
                poll_interval_s=0.02,
                sweep_func=pipeline.sweep,
            )
            task = asyncio.ensure_future(worker.run())
            try:
                for _ in range(250):
                    if db.count_queue_by_status(conn).get("pending", 0) == 0:
                        break
                    await asyncio.sleep(0.02)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

            # The row was processed exactly once and removed: no requeue loop.
            assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
            assert counters(conn)["processed"] == 1
            assert counters(conn)["llm_errors"] == 1
        finally:
            conn.close()

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Non-SMS events and broken payloads: counted, removed, never retried
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "event", ["sms:sent", "sms:cancelled", "mms:received", "totally:unknown"]
)
def test_other_non_sms_events_are_skipped(pipeline, conn, event):
    # Events WITHOUT step-5 semantics keep the step-3 behavior: counted as
    # skipped_event and removed (the four step-5 events have their own
    # suite in test_step5_events.py).
    row = enqueue_envelope(conn, "evt-" + event.replace(":", "-"), sms_envelope(event=event))
    assert pipeline.process(row) == ""
    assert counters(conn)["skipped_event"] == 1
    assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0


def test_broken_json_is_dropped_not_retried(pipeline, conn):
    db.enqueue_message(conn, "broken", "{not json")
    row = dict(
        conn.execute(
            "SELECT id, message_id, payload FROM queue WHERE message_id = 'broken'"
        ).fetchone()
    )
    assert pipeline.process(row) == ""
    assert counters(conn)["dropped_parse"] == 1


def test_sms_received_without_message_id_is_dropped(pipeline, conn):
    envelope = sms_envelope()
    envelope["payload"] = {"message": "no id here"}
    row = enqueue_envelope(conn, "m1", envelope)
    assert pipeline.process(row) == ""
    assert counters(conn)["dropped_parse"] == 1


def test_sms_received_without_message_text_is_dropped(pipeline, conn):
    envelope = sms_envelope()
    envelope["payload"] = {"messageId": "m1"}
    row = enqueue_envelope(conn, "m1", envelope)
    assert pipeline.process(row) == ""
    assert counters(conn)["dropped_parse"] == 1


# ---------------------------------------------------------------------------
# Worker integration: rows are processed and removed; no infinite retry loop
# ---------------------------------------------------------------------------


def test_worker_e2e_processes_drops_and_skips(tmp_path):
    async def scenario():
        conn = db.open_db(tmp_path / "e2e.db")
        try:
            db.run_migrations(conn)
            config_file = tmp_path / "config.yaml"
            config_file.write_text(json.dumps(VALID_CONFIG), encoding="utf-8")
            manager = ConfigManager(config_file)
            manager.load_initial()
            pipeline = MessagePipeline(conn, manager)

            db.enqueue_message(conn, "good", json.dumps(sms_envelope(text="#2 hi there")))
            db.enqueue_message(
                conn, "foreign", json.dumps(sms_envelope(sender="MTS", text="spam"))
            )
            db.enqueue_message(conn, "broken", "{not json")
            db.enqueue_message(conn, "ping", json.dumps(sms_envelope(event="system:ping")))

            worker = Worker(
                conn,
                process_func=pipeline.process,
                poll_interval_s=0.02,
                sweep_func=pipeline.sweep,
            )
            task = asyncio.ensure_future(worker.run())
            try:
                for _ in range(250):
                    if db.count_queue_by_status(conn).get("pending", 0) == 0:
                        break
                    await asyncio.sleep(0.02)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

            # Every row was handled exactly once and removed from the queue
            # (drops and handled events included - nothing retries forever).
            # The mock gateway delivered the single reply (sms_sent=1) and
            # system:ping is a handled step-5 event (heartbeats=1).
            assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
            assert counters(conn) == {
                "processed": 1,
                "dropped_whitelist": 1,
                "dropped_parse": 1,
                "heartbeats": 1,
                "sms_sent": 1,
            }
            # The routed session exists with its message stored (mock echo
            # appended as the assistant turn).
            session = store.get_or_create(
                conn, WHITELISTED, 2, max_sessions=10, session_ttl_h=24
            )
            assert [
                (item["role"], item["content"])
                for item in store.get_history(conn, session.id)
            ] == [("user", "hi there"), ("assistant", "(mock) hi there")]
        finally:
            conn.close()

    asyncio.run(scenario())


def test_worker_daily_sweep_removes_expired_session(tmp_path):
    async def scenario():
        conn = db.open_db(tmp_path / "sweep.db")
        try:
            db.run_migrations(conn)
            session = store.get_or_create(
                conn, WHITELISTED, 1, max_sessions=10, session_ttl_h=24
            )
            store.append_message(conn, session.id, "user", "old", history_messages=10)
            with conn:
                conn.execute(
                    "UPDATE sessions SET expires_at = ? WHERE id = ?",
                    ("2000-01-01T00:00:00.000+00:00", session.id),
                )

            from app.sessions.store import sweep_expired

            worker = Worker(
                conn,
                process_func=lambda row: None,
                poll_interval_s=0.02,
                sweep_func=lambda: store.sweep_expired(conn),
                sweep_interval_s=0.05,
            )
            task = asyncio.ensure_future(worker.run())
            try:
                for _ in range(250):
                    if conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0:
                        break
                    await asyncio.sleep(0.02)
            finally:
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

            assert conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0] == 0
        finally:
            conn.close()

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Privacy (152-FZ): no message or PROMPT text in the logs
# ---------------------------------------------------------------------------


def test_logs_never_contain_message_or_prompt_text(pipeline, conn, caplog):
    with caplog.at_level(logging.DEBUG, logger="app"):
        secret_text = "TOPSECRET-TEXT-should-never-be-logged"
        pipeline.process(enqueue_envelope(conn, "m1", sms_envelope(text=secret_text)))
        secret_prompt = "TOPSECRET-PROMPT-custom-system-instruction"
        pipeline.process(
            enqueue_envelope(conn, "m2", sms_envelope(text="PROMPT " + secret_prompt))
        )
        # And a drop path too.
        pipeline.process(enqueue_envelope(conn, "m3", sms_envelope(sender="MTS", text=secret_text)))

    for record in caplog.records:
        rendered = record.getMessage()
        assert secret_text not in rendered
        assert secret_prompt not in rendered
        assert "TOPSECRET" not in rendered
        if record.exc_info:
            assert "TOPSECRET" not in str(record.exc_info)

    # Discriminator: metadata IS logged, so the absence above is meaningful.
    all_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "text_len=%d" % len(secret_text) in all_logs
    assert "prompt_len=%d" % len(secret_prompt) in all_logs
    assert "sender=+79001234567" in all_logs
    assert "sender=MTS" in all_logs or "non-numeric" in all_logs
