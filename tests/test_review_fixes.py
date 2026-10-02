"""Tests for the independent code-critic fix package (B1/C1/C2, W1-W8, M2/M5).

Grouped by finding:

- W1: watchdog on the FAST worker tick (dead-device detection within ~1h).
- W2: per-row attempt budget -> dead-letter ``failed`` status.
- W3: separate SQLite connections per component (no shared transaction state).
- W4a: mock flags and notifier credentials re-resolved per call.
- W5: deferred re-send of an undelivered reply (one budgeted retry; the
  trimmed reply text is stored in ``queue.deferred_reply`` (schema v6) and
  replayed verbatim - no second LLM call, no command re-execution,
  idempotent via send_id).
- W8: configurable webhook dedup TTL (``webhook.dedup_ttl_hours``).
- C1: Telegram credentials injected via ${ENV} in config.example.yaml.
- v5/v6 migrations: data-preserving queue rebuild, dead-letter helpers,
  the deferred-reply marker column.
"""

import asyncio
import json
import sqlite3
import time
from pathlib import Path

import pytest

from app import db
from app.config import ConfigError, ConfigManager, load_config
from app.pipeline import MockLLMClient, MessagePipeline, ReplySendDeferred
from app.worker import Worker

PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_manager(write_config, **overrides):
    path = write_config(**overrides)
    manager = ConfigManager(path)
    manager.load_initial()
    return manager


def enqueue_sms(conn, message_id: str, text: str, sender: str = "+79001234567") -> None:
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
    db.enqueue_message(conn, message_id, json.dumps(envelope))


class FlakyGateway:
    """Gateway stub: fails the first N sends with a retryable 503."""

    def __init__(self, failures: int = 1) -> None:
        self.failures = failures
        self.calls: list[dict] = []

    def send(self, *, text, phone_number, send_id=None, max_attempts=3):
        self.calls.append(
            {"text": text, "phone": phone_number, "send_id": send_id}
        )
        if len(self.calls) <= self.failures:
            from app.gateway import GatewayError

            raise GatewayError("server restarting", status=503)
        return "gw-%d" % len(self.calls)


class CountingMockLLM(MockLLMClient):
    """Mock LLM stub counting complete() invocations (replay assertions)."""

    def __init__(self) -> None:
        self.calls = 0

    def complete(self, messages, *, temperature, max_tokens):
        self.calls += 1
        return super().complete(
            messages, temperature=temperature, max_tokens=max_tokens
        )


class RecordingNotifier:
    def __init__(self) -> None:
        self.alerts: list[str] = []

    def send_alert(self, text: str) -> None:
        self.alerts.append(text)


# ---------------------------------------------------------------------------
# W1: watchdog cadence on the fast tick
# ---------------------------------------------------------------------------


class TestW1WatchdogCadence:
    def test_watchdog_runs_on_fast_tick_sweep_stays_daily(self, tmp_path):
        async def scenario():
            conn = db.open_db(tmp_path / "w1.db")
            try:
                db.run_migrations(conn)
                watchdog_calls = []
                sweep_calls = []

                worker = Worker(
                    conn,
                    watchdog_func=lambda: watchdog_calls.append(1),
                    watchdog_interval_s=0.1,
                    sweep_func=lambda: sweep_calls.append(1) or 0,
                    sweep_interval_s=3600.0,
                    poll_interval_s=0.05,
                )
                task = asyncio.ensure_future(worker.run())
                await asyncio.sleep(0.6)
                worker.request_stop()
                await asyncio.wait_for(task, timeout=5)

                # >= 3 watchdog ticks in 0.6 s at a 0.1 s cadence; the daily
                # sweep must not have fired at all.
                assert len(watchdog_calls) >= 3
                assert sweep_calls == []
            finally:
                conn.close()

        asyncio.run(scenario())

    def test_watchdog_params_validated(self, tmp_path):
        conn = db.open_db(tmp_path / "w1v.db")
        try:
            with pytest.raises(ValueError):
                Worker(conn, watchdog_interval_s=0)
            with pytest.raises(ValueError):
                Worker(conn, max_row_attempts=0)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# W2: dead-letter policy
# ---------------------------------------------------------------------------


class TestW2DeadLetter:
    def test_poison_row_dead_letters_others_progress(self, tmp_path):
        async def scenario():
            conn = db.open_db(tmp_path / "w2.db")
            try:
                db.run_migrations(conn)
                db.enqueue_message(conn, "poison", '{"n": 1}')
                db.enqueue_message(conn, "fine", '{"n": 2}')

                def failing_process(row):
                    if row["message_id"] == "poison":
                        raise RuntimeError("simulated crash")

                worker = Worker(
                    conn, process_func=failing_process, poll_interval_s=0.02,
                    max_row_attempts=3,
                )
                task = asyncio.ensure_future(worker.run())
                try:
                    # Wait until the poison row exhausts its budget.
                    for _ in range(500):
                        row = conn.execute(
                            "SELECT status, attempts FROM queue "
                            "WHERE message_id = 'poison'"
                        ).fetchone()
                        if row is not None and row["status"] == "failed":
                            break
                        await asyncio.sleep(0.02)
                    else:
                        pytest.fail("poison row was never dead-lettered")
                finally:
                    worker.request_stop()
                    await asyncio.wait_for(task, timeout=5)

                row = conn.execute(
                    "SELECT attempts FROM queue WHERE message_id = 'poison'"
                ).fetchone()
                assert row["attempts"] == 3
                assert db.count_queue_by_status(conn) == {"failed": 1}
                # The healthy row was processed and removed: the FIFO moved on.
                assert (
                    conn.execute(
                        "SELECT COUNT(*) FROM queue WHERE message_id = 'fine'"
                    ).fetchone()[0]
                    == 0
                )
            finally:
                conn.close()

        asyncio.run(scenario())

    def test_helpers_increment_and_mark_failed(self, tmp_path):
        conn = db.open_db(tmp_path / "w2h.db")
        try:
            db.run_migrations(conn)
            db.enqueue_message(conn, "m1", "{}")
            row_id = conn.execute(
                "SELECT id FROM queue WHERE message_id = 'm1'"
            ).fetchone()["id"]
            assert db.increment_queue_attempts(conn, row_id) == 1
            assert db.increment_queue_attempts(conn, row_id) == 2
            db.mark_queue_failed(conn, [row_id])
            assert db.count_queue_by_status(conn) == {"failed": 1}
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# W3: separate connections per component
# ---------------------------------------------------------------------------


class TestW3SeparateConnections:
    def test_transactions_do_not_leak_across_connections(self, tmp_path):
        """An uncommitted write on one connection is invisible to another.

        With the old shared-connection wiring this is trivially false (same
        transaction state) - exactly the interleaving bug the review flagged.
        Raw SQL is used on purpose: the db helpers auto-commit.
        """
        path = tmp_path / "w3.db"
        receiver = db.open_db(path)
        try:
            db.run_migrations(receiver)
            worker = db.open_db(path)
            health = db.open_db(path)
            try:
                # Uncommitted write on the worker connection.
                worker.execute("BEGIN")
                worker.execute(
                    "INSERT INTO counters (key, value) VALUES ('worker_probe', 1)"
                )

                # The health connection must NOT see the uncommitted row.
                assert db.get_counters(health) == {}

                worker.commit()
                # ...and after the commit it does.
                assert db.get_counters(health) == {"worker_probe": 1}
            finally:
                worker.close()
                health.close()
        finally:
            receiver.close()


# ---------------------------------------------------------------------------
# W4a: per-call resolution of mock flags / notifier
# ---------------------------------------------------------------------------


class TestW4MockReload:
    def test_mock_flags_re_resolved_per_call(self, write_config, monkeypatch):
        manager = make_manager(
            write_config,
            llm={"model": "GigaChat-2", "temperature": 0.4, "max_tokens": 500,
                 "timeout_s": 90, "mock": False, "auth_key": ""},
            gateway={"base_url": "https://gw.example.com", "mock": False},
        )
        conn = db.open_db(":memory:")
        try:
            db.run_migrations(conn)
            pipeline = MessagePipeline(conn, manager)

            # No mocks anywhere: real client classes.
            from app.gateway.client import GatewayClient
            from app.llm.gigachat import GigaChatClient

            assert isinstance(pipeline._resolve_llm_client(), GigaChatClient)

            # Env mock wins over the YAML flag, re-resolved on the NEXT call.
            monkeypatch.setenv("MOCK_LLM", "true")
            resolved = pipeline._resolve_llm_client()
            assert isinstance(resolved, MockLLMClient)
            # The shared instance is reused (W4a nit), not a fresh stub.
            from app import pipeline as pipeline_mod

            assert resolved is pipeline_mod._MOCK_LLM_CLIENT

            # Gateway: same semantics.
            assert isinstance(pipeline._resolve_gateway_client(), GatewayClient)
            monkeypatch.setenv("MOCK_GATEWAY", "true")
            assert isinstance(
                pipeline._resolve_gateway_client(), pipeline_mod.MockGatewayClient
            )
        finally:
            conn.close()

    def test_notifier_override_wins_and_property_resolves(self, write_config):
        manager = make_manager(write_config)
        conn = db.open_db(":memory:")
        try:
            db.run_migrations(conn)
            notifier = RecordingNotifier()
            pipeline = MessagePipeline(conn, manager, notifier=notifier)
            assert pipeline.notifier is notifier
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# W5: deferred re-send
# ---------------------------------------------------------------------------


class TestW5DeferredResend:
    def _worker(self, conn, pipeline):
        return Worker(conn, process_func=pipeline.process, poll_interval_s=0.05)

    def test_deferred_reply_replayed_without_second_llm_call(
        self, write_config, tmp_path
    ):
        manager = make_manager(write_config)  # mock LLM on, mock gateway on
        conn = db.open_db(tmp_path / "w5.db")
        try:
            db.run_migrations(conn)
            gateway = FlakyGateway(failures=1)
            llm = CountingMockLLM()  # override: counts every LLM call
            pipeline = MessagePipeline(
                conn, manager, llm_client=llm, gateway_client=gateway
            )
            worker = self._worker(conn, pipeline)

            enqueue_sms(conn, "w5-1", "hello")
            first = worker._claim_batch()
            assert len(first) == 1
            assert first[0]["attempts"] == 0

            # Pass 1: the LLM runs, the send fails with a retryable 503.
            with pytest.raises(ReplySendDeferred):
                pipeline.process(dict(first[0]))

            counters = db.get_counters(conn)
            assert counters["sms_send_requeued"] == 1
            assert llm.calls == 1
            assert "processed" not in counters  # terminal outcome only
            # The row is still claimed ('processing') with the bumped budget
            # AND the deferred marker carrying the exact reply text.
            row = conn.execute(
                "SELECT status, attempts, send_id, deferred_reply FROM queue "
                "WHERE message_id = 'w5-1'"
            ).fetchone()
            assert row["status"] == "processing"
            assert row["attempts"] == 1
            assert row["send_id"] is not None
            assert row["deferred_reply"] == "(mock) hello"
            # The full reply is in the history (from the LLM step); the
            # replay itself uses the marker, not the history.
            history = conn.execute(
                "SELECT role, content FROM session_messages ORDER BY id"
            ).fetchall()
            assert [h["role"] for h in history] == ["user", "assistant"]
            assert history[1]["content"] == "(mock) hello"

            # The worker's dedicated requeue path (no budget consumed).
            worker._requeue_deferred([first[0]["id"]])
            row = conn.execute(
                "SELECT status, attempts, deferred_reply FROM queue WHERE message_id = 'w5-1'"
            ).fetchone()
            assert row["status"] == "pending"
            assert row["attempts"] == 1
            assert row["deferred_reply"] == "(mock) hello"

            # Pass 2: the stored marker is replayed verbatim - the LLM is
            # NOT called again, the same send_id is reused.
            second = worker._claim_batch()
            assert second[0]["attempts"] == 1
            assert second[0]["deferred_reply"] == "(mock) hello"
            sent = pipeline.process(dict(second[0]))

            assert sent == "(mock) hello"
            assert len(gateway.calls) == 2
            assert gateway.calls[0]["send_id"] == gateway.calls[1]["send_id"]
            assert llm.calls == 1  # no second LLM call
            counters = db.get_counters(conn)
            assert counters["sms_sent"] == 1
            assert counters["processed"] == 1  # bumped once, on the terminal pass
        finally:
            conn.close()

    def test_second_final_failure_is_absorbed(self, write_config, tmp_path):
        manager = make_manager(write_config)
        conn = db.open_db(tmp_path / "w5b.db")
        try:
            db.run_migrations(conn)
            gateway = FlakyGateway(failures=99)  # never succeeds
            pipeline = MessagePipeline(conn, manager, gateway_client=gateway)
            worker = self._worker(conn, pipeline)

            enqueue_sms(conn, "w5-2", "hello")
            claimed = worker._claim_batch()
            with pytest.raises(ReplySendDeferred):
                pipeline.process(dict(claimed[0]))
            worker._requeue_deferred([claimed[0]["id"]])

            # Replay fails finally: absorbed (no exception), row deletable.
            second = worker._claim_batch()
            result = pipeline.process(dict(second[0]))
            assert result == "(mock) hello"  # trimmed reply returned, counted
            counters = db.get_counters(conn)
            assert counters["sms_send_failed"] == 1
            assert counters.get("sms_sent", 0) == 0
            # And the dead-letter budget was NOT consumed by the deferral
            # (attempts is 1 for one deferral + 0 unexpected failures).
            row = conn.execute(
                "SELECT attempts FROM queue WHERE message_id = 'w5-2'"
            ).fetchone()
            assert row["attempts"] == 1
        finally:
            conn.close()

    def test_deferred_command_reply_replays_confirmation_not_stale_llm(
        self, write_config, tmp_path
    ):
        """(а) Deferral on a COMMAND: the replay must send the command's own
        confirmation, never a stale LLM reply from a previous conversation.

        Commands write no ``assistant`` message to the history (project
        doctrine), so the old attempts-based replay picked the last LLM
        answer of an earlier chat - state applied, wrong SMS delivered. The
        v6 marker stores the exact text to re-send on the row itself.
        """
        manager = make_manager(write_config)
        conn = db.open_db(tmp_path / "w5cmd.db")
        try:
            db.run_migrations(conn)
            gateway = FlakyGateway(failures=1)
            llm = CountingMockLLM()
            pipeline = MessagePipeline(
                conn, manager, llm_client=llm, gateway_client=gateway
            )
            worker = self._worker(conn, pipeline)

            # A PREVIOUS conversation left an assistant reply in the history.
            from app.sessions import store as sessions_store

            session = sessions_store.get_or_create(
                conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
            )
            sessions_store.append_message(
                conn, session.id, "user", "старый вопрос", history_messages=10
            )
            sessions_store.append_message(
                conn, session.id, "assistant", "(mock) старый ответ",
                history_messages=10,
            )

            # New row: a command - its confirmation must survive the deferral.
            enqueue_sms(conn, "w5-cmd", "TEMP 1.5")
            claimed = worker._claim_batch()
            with pytest.raises(ReplySendDeferred):
                pipeline.process(dict(claimed[0]))

            # The marker carries the confirmation, not the old LLM reply.
            marker = conn.execute(
                "SELECT deferred_reply FROM queue WHERE message_id = 'w5-cmd'"
            ).fetchone()["deferred_reply"]
            assert marker
            assert marker != "(mock) старый ответ"

            worker._requeue_deferred([claimed[0]["id"]])
            second = worker._claim_batch()
            sent = pipeline.process(dict(second[0]))

            # EXACTLY the deferred confirmation was re-sent - not the stale
            # assistant message from the previous conversation.
            assert sent == marker
            assert len(gateway.calls) == 2
            assert gateway.calls[1]["text"] == marker
            assert "(mock) старый ответ" not in [c["text"] for c in gateway.calls]
            assert llm.calls == 0  # commands never call the LLM - replay included
            counters = db.get_counters(conn)
            assert counters["sms_sent"] == 1
            assert counters["processed"] == 1
        finally:
            conn.close()

    def test_generic_failure_requeues_without_marker_and_reprocesses(
        self, write_config, tmp_path, monkeypatch
    ):
        """(б) A generic crash before the LLM (e.g. OperationalError in the
        history append) requeues the row WITHOUT the deferred marker: the
        next claim must process the message NORMALLY (text -> context, a
        fresh LLM call), not replay anything."""
        manager = make_manager(write_config)
        conn = db.open_db(tmp_path / "w5gen.db")
        try:
            db.run_migrations(conn)
            gateway = FlakyGateway(failures=0)  # the gateway is not the failure
            llm = CountingMockLLM()
            pipeline = MessagePipeline(
                conn, manager, llm_client=llm, gateway_client=gateway
            )
            worker = self._worker(conn, pipeline)

            from app.sessions import store as sessions_store

            original = sessions_store.append_message
            calls = {"n": 0}

            def flaky_append(*args, **kwargs):
                if not calls["n"]:
                    calls["n"] += 1
                    raise sqlite3.OperationalError("database is locked")
                return original(*args, **kwargs)

            monkeypatch.setattr(sessions_store, "append_message", flaky_append)

            enqueue_sms(conn, "w5-gen", "hello")
            claimed = worker._claim_batch()
            with pytest.raises(sqlite3.OperationalError):
                pipeline.process(dict(claimed[0]))

            # The generic requeue path bumps attempts but must NOT plant
            # the deferred marker.
            worker._requeue([claimed[0]["id"]])
            row = conn.execute(
                "SELECT status, attempts, deferred_reply FROM queue "
                "WHERE message_id = 'w5-gen'"
            ).fetchone()
            assert row["status"] == "pending"
            assert row["attempts"] == 1
            assert row["deferred_reply"] is None

            # Pass 2: attempts == 1 but NO marker -> normal processing.
            second = worker._claim_batch()
            assert second[0]["deferred_reply"] is None
            sent = pipeline.process(dict(second[0]))

            assert sent == "(mock) hello"
            assert llm.calls == 1  # fresh LLM call, not a history replay
            history = conn.execute(
                "SELECT content FROM session_messages WHERE role = 'user'"
            ).fetchall()
            assert [h["content"] for h in history] == ["hello"]  # text in context
            counters = db.get_counters(conn)
            assert counters["sms_sent"] == 1
        finally:
            conn.close()

    def test_worker_loop_requeues_deferred_row(self, write_config, tmp_path):
        """The run() loop routes ReplySendDeferred to the dedicated path."""

        async def scenario():
            manager = make_manager(write_config)
            conn = db.open_db(tmp_path / "w5c.db")
            try:
                db.run_migrations(conn)
                gateway = FlakyGateway(failures=1)
                pipeline = MessagePipeline(conn, manager, gateway_client=gateway)
                worker = self._worker(conn, pipeline)

                enqueue_sms(conn, "w5-3", "hello")
                task = asyncio.ensure_future(worker.run())
                try:
                    # Wait for: pass 1 (defer), requeue, pass 2 (success).
                    for _ in range(500):
                        if len(gateway.calls) >= 2:
                            break
                        await asyncio.sleep(0.02)
                    # The drained queue (both passes done) may need one more
                    # cycle for the DELETE.
                    for _ in range(100):
                        n = await asyncio.to_thread(
                            lambda: conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
                        )
                        if n == 0:
                            break
                        await asyncio.sleep(0.02)
                    assert n == 0, "deferred row was not completed after replay"
                finally:
                    worker.request_stop()
                    await asyncio.wait_for(task, timeout=5)

                assert len(gateway.calls) == 2
                assert gateway.calls[0]["send_id"] == gateway.calls[1]["send_id"]
                assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
                counters = db.get_counters(conn)
                assert counters["sms_send_requeued"] == 1
                assert counters["sms_sent"] == 1
            finally:
                conn.close()

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# W7 margin fix: liveness stamp refreshed per ROW (heartbeat-per-item)
# ---------------------------------------------------------------------------


class TestHeartbeatPerItem:
    def test_last_cycle_ts_refreshed_after_every_row(self, tmp_path):
        """A long batch must not stall the /healthz probe: run() refreshes
        the liveness stamp after EVERY processed row, so the 300 s margin
        covers one row's worst case - not the whole batch duration."""

        async def scenario():
            conn = db.open_db(tmp_path / "hb.db")
            try:
                db.run_migrations(conn)
                stamps: list[float] = []

                def slow_process(row):
                    time.sleep(0.05)
                    stamps.append(worker.last_cycle_ts)

                worker = Worker(
                    conn, process_func=slow_process, poll_interval_s=0.05
                )
                for i in range(3):
                    enqueue_sms(conn, "hb-%d" % i, "hello %d" % i)

                task = asyncio.ensure_future(worker.run())
                try:
                    # All 3 rows are claimed in ONE batch; wait until the
                    # queue is drained.
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        n = await asyncio.to_thread(
                            lambda: conn.execute(
                                "SELECT COUNT(*) FROM queue"
                            ).fetchone()[0]
                        )
                        if n == 0 and len(stamps) >= 3:
                            break
                        await asyncio.sleep(0.02)
                    assert len(stamps) == 3, "not all rows were processed"
                finally:
                    worker.request_stop()
                    await asyncio.wait_for(task, timeout=5)

                assert all(b > a for a, b in zip(stamps, stamps[1:])), (
                    "liveness stamp was not refreshed between rows: %r" % stamps
                )
            finally:
                conn.close()

        asyncio.run(scenario())


# ---------------------------------------------------------------------------
# W8: configurable dedup TTL
# ---------------------------------------------------------------------------


class TestW8DedupTtl:
    def test_config_field_loaded(self, write_config):
        cfg = load_config(
            write_config(webhook={
                "secret": "s", "rate_limit_per_min": 120,
                "max_body_bytes": 65536, "timestamp_window_s": 300,
                "dedup_ttl_hours": 72,
            })
        )
        assert cfg.webhook.dedup_ttl_hours == 72

    def test_non_positive_ttl_rejected(self, write_config):
        with pytest.raises(ConfigError, match="positive"):
            load_config(
                write_config(webhook={
                    "secret": "s", "rate_limit_per_min": 120,
                    "max_body_bytes": 65536, "timestamp_window_s": 300,
                    "dedup_ttl_hours": 0,
                })
            )

    def test_custom_ttl_governs_tombstone_purge(self, tmp_path):
        """accept_message honors the configured TTL (wired from the receiver)."""
        from datetime import datetime, timedelta, timezone

        conn = db.open_db(tmp_path / "w8.db")
        try:
            db.run_migrations(conn)
            assert db.accept_message(conn, "old", "{}", ttl_hours=1) is not None
            aged = (
                datetime.now(timezone.utc) - timedelta(hours=2)
            ).isoformat(timespec="milliseconds")
            conn.execute(
                "UPDATE processed_events SET processed_at = ? WHERE message_id = 'old'",
                (aged,),
            )
            conn.commit()
            # 1 h TTL: the 2 h-old tombstone is purged on the next accept.
            assert db.accept_message(conn, "new", "{}", ttl_hours=1) is not None
            remaining = conn.execute(
                "SELECT message_id FROM processed_events"
            ).fetchall()
            assert [r["message_id"] for r in remaining] == ["new"]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# C1: ${ENV} substitution for Telegram credentials in the example config
# ---------------------------------------------------------------------------


class TestC1EnvSubstitution:
    ENV = {
        "GIGACHAT_AUTH_KEY": "test-auth-key",
        "GATEWAY_USERNAME": "gw-user",
        "GATEWAY_PASSWORD": "gw-pass",
        "GATEWAY_HMAC_SECRET": "hmac-secret",
        "TELEGRAM_BOT_TOKEN": "bot-token-xyz",
        "TELEGRAM_CHAT_ID": "chat-42",
    }

    def test_example_config_resolves_telegram_from_env(self, monkeypatch):
        for key, value in self.ENV.items():
            monkeypatch.setenv(key, value)
        cfg = load_config(PROJECT_ROOT / "config.example.yaml")
        assert cfg.notify.telegram_token == "bot-token-xyz"
        assert cfg.notify.telegram_chat_id == "chat-42"

    def test_example_config_fails_without_telegram_env(self, monkeypatch):
        for key, value in self.ENV.items():
            monkeypatch.setenv(key, value)
        monkeypatch.delenv("TELEGRAM_BOT_TOKEN")
        with pytest.raises(ConfigError, match="TELEGRAM_BOT_TOKEN"):
            load_config(PROJECT_ROOT / "config.example.yaml")


# ---------------------------------------------------------------------------
# v5/v6 migrations: data-preserving rebuild + the deferred-reply marker
# ---------------------------------------------------------------------------


class TestMigrationV5:
    def _make_v4_db(self, path: Path) -> sqlite3.Connection:
        """Hand-craft a v4-shape database (queue without attempts, narrow
        CHECK) and stamp user_version = 4, as a pre-upgrade install has."""
        conn = db.open_db(path)
        conn.execute("PRAGMA user_version = 4")
        conn.execute(
            """
            CREATE TABLE queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                send_id TEXT
            )
            """
        )
        now = db.utcnow_iso()
        conn.execute(
            "INSERT INTO queue (message_id, payload, status, created_at, "
            "updated_at, send_id) VALUES (?, ?, ?, ?, ?, ?)",
            ("legacy-1", "{}", "pending", now, now, "legacy-send-id"),
        )
        conn.commit()
        return conn

    def test_v4_upgrade_preserves_rows_and_adds_columns(self, tmp_path):
        conn = self._make_v4_db(tmp_path / "upgrade.db")
        try:
            version = db.run_migrations(conn)
            assert version == db.SCHEMA_VERSION

            # The v6 marker column arrived together with the v5 rebuild.
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(queue)")
            }
            assert {"attempts", "deferred_reply"} <= columns

            # Data preserved (ids, payload, send_id); attempts defaulted to 0.
            row = conn.execute(
                "SELECT id, message_id, send_id, attempts, status FROM queue"
            ).fetchone()
            assert row["message_id"] == "legacy-1"
            assert row["send_id"] == "legacy-send-id"
            assert row["attempts"] == 0

            # The rebuilt CHECK accepts the new dead-letter status...
            conn.execute(
                "INSERT INTO queue (message_id, payload, status, created_at, "
                "updated_at) VALUES ('dl', '{}', 'failed', ?, ?)",
                (db.utcnow_iso(), db.utcnow_iso()),
            )
            # ...and still rejects garbage statuses.
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO queue (message_id, payload, status, created_at, "
                    "updated_at) VALUES ('bad', '{}', 'bogus', ?, ?)",
                    (db.utcnow_iso(), db.utcnow_iso()),
                )
            conn.commit()

            # Re-running migrations is idempotent and preserves data.
            db.increment_queue_attempts(conn, 1)
            db.run_migrations(conn)
            row = conn.execute(
                "SELECT attempts, send_id FROM queue WHERE id = 1"
            ).fetchone()
            assert row["attempts"] == 1
            assert row["send_id"] == "legacy-send-id"
        finally:
            conn.close()

    def test_fresh_schema_rejects_bad_status_and_has_marker_column(self, tmp_path):
        conn = db.open_db(tmp_path / "fresh.db")
        try:
            assert db.run_migrations(conn) == db.SCHEMA_VERSION
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(queue)")
            }
            assert "deferred_reply" in columns
            now = db.utcnow_iso()
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO queue (message_id, payload, status, created_at, "
                    "updated_at) VALUES ('m1', '{}', 'bogus', ?, ?)",
                    (now, now),
                )
        finally:
            conn.close()

    def test_v6_marker_write_roundtrip(self, tmp_path):
        """defer_queue_row: one committed write = marker + budget bump."""
        conn = db.open_db(tmp_path / "v6rt.db")
        try:
            db.run_migrations(conn)
            db.enqueue_message(conn, "m1", "{}")
            db.defer_queue_row(conn, 1, "(mock) hello")
            row = conn.execute(
                "SELECT attempts, deferred_reply FROM queue WHERE id = 1"
            ).fetchone()
            assert row["attempts"] == 1
            assert row["deferred_reply"] == "(mock) hello"
        finally:
            conn.close()
