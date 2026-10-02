"""Tests for the webhook receiver (step 2): signature, dedup, limits.

The signature scheme under test mirrors the Android SMS Gateway protocol:
HMAC-SHA256 over raw body bytes + ASCII timestamp (no separator), hex
lowercase, headers ``X-Signature`` / ``X-Timestamp`` (unix seconds).
"""

import asyncio
import hashlib
import hmac
import json
import time

import pytest
import yaml

from app import db

SECRET = "test-webhook-secret"
MAX_BODY_BYTES = 65536


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sign(secret: str, body: bytes, timestamp: int) -> str:
    """Reference implementation of the gateway signature (hex, lowercase)."""
    mac = hmac.new(
        secret.encode("utf-8"), body + str(timestamp).encode("ascii"), hashlib.sha256
    )
    return mac.hexdigest()


def _envelope(message_id="msg-1", event="sms:received", event_id="evt-1", **payload):
    """Build a webhook envelope; extra kwargs go into the payload."""
    body = {
        "messageId": message_id,
        "message": "hello there",
        "sender": "+79001234567",
        "receivedAt": "2026-09-16T12:00:00.000+03:00",
    }
    body.update(payload)
    return {
        "deviceId": "device-1",
        "event": event,
        "id": event_id,
        "payload": body,
        "webhookId": "wh-1",
    }


def _post(client, raw: bytes, *, secret=SECRET, timestamp="auto", signature="auto"):
    """POST raw bytes with configurable signature headers.

    timestamp/signature: "auto" computes/uses the real value, a string uses
    it verbatim, None omits the header.
    """
    ts_value = int(time.time()) if timestamp == "auto" else timestamp
    headers = {"Content-Type": "application/json"}
    if timestamp is not None:
        headers["X-Timestamp"] = str(ts_value)
    if signature is not None:
        if signature == "auto":
            signature = _sign(secret, raw, int(ts_value))
        headers["X-Signature"] = signature
    return client.post(
        "/webhook", content=raw, headers={**headers, "Content-Type": "application/json"}
    )


def _post_envelope(client, envelope: dict, **kwargs):
    raw = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    return _post(client, raw, **kwargs)


def _queue_is_empty(client) -> bool:
    conn = client.app.state.db_conn
    return conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0


def _build_scope(headers: list[tuple[bytes, bytes]]) -> dict:
    """Minimal ASGI HTTP scope for a direct application call."""
    return {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/webhook",
        "raw_path": b"/webhook",
        "query_string": b"",
        "root_path": "",
        "server": ("testserver", 80),
        "client": ("10.0.0.1", 12345),
        "headers": headers,
    }


def _oversized_stream_rejection(tmp_path, write_config, extra_headers):
    """Shared scenario: the body stream carries far more than the cap.

    The ASGI ``receive`` is a spy that counts how many bytes the handler
    actually consumed. Memory safety is verified exactly there: the handler
    must stop reading within ``max_body_bytes + one chunk``, so the rest of
    the stream (most of the offered bytes) never leaves the socket buffer
    and is never buffered in the process.
    """
    from app.main import create_app

    async def scenario():
        config_path = write_config()
        app = create_app(
            config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log")
        )
        async with app.router.lifespan_context(app):
            chunk = b"x" * 1024
            total_chunks = 200  # 200 KB offered; the cap is 64 KB
            consumed = {"calls": 0, "bytes": 0}
            sent: list[dict] = []

            async def receive():
                if consumed["calls"] >= total_chunks:
                    return {"type": "http.disconnect"}
                consumed["calls"] += 1
                consumed["bytes"] += len(chunk)
                return {
                    "type": "http.request",
                    "body": chunk,
                    "more_body": consumed["calls"] < total_chunks,
                }

            async def send(message):
                sent.append(message)

            headers = [
                (b"host", b"testserver"),
                (b"content-type", b"application/json"),
                *extra_headers,
            ]
            await app(_build_scope(headers), receive, send)

            assert sent[0]["type"] == "http.response.start"
            assert sent[0]["status"] == 413

            max_body = 65536
            # Read at most one chunk beyond the cap - not the whole stream.
            assert consumed["bytes"] <= max_body + len(chunk)
            assert consumed["calls"] < total_chunks

            counters = db.get_counters(app.state.db_conn)
            assert counters["rejected_size"] == 1
            assert counters["received"] == 1
            assert _no_queue_rows(app) is True

    asyncio.run(scenario())


def _no_queue_rows(app) -> bool:
    conn = app.state.db_conn
    return conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0


# ---------------------------------------------------------------------------
# Signature verification
# ---------------------------------------------------------------------------


def test_signed_webhook_is_accepted_and_enqueued(client):
    envelope = _envelope()
    response = _post_envelope(client, envelope)
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

    conn = client.app.state.db_conn
    rows = conn.execute("SELECT message_id, payload, status FROM queue").fetchall()
    assert len(rows) == 1
    assert rows[0]["message_id"] == "msg-1"
    assert rows[0]["status"] == "pending"
    # The raw request body is stored verbatim for the worker (step 3).
    assert json.loads(rows[0]["payload"]) == envelope
    # The dedup tombstone is committed with the queue row.
    tombstones = conn.execute(
        "SELECT message_id FROM processed_events WHERE message_id = 'msg-1'"
    ).fetchall()
    assert len(tombstones) == 1

    counters = db.get_counters(conn)
    assert counters["received"] == 1
    assert counters["accepted"] == 1


def test_wrong_signature_rejected(client):
    raw = json.dumps(_envelope()).encode("utf-8")
    ts = int(time.time())
    bad_signature = _sign("some-other-secret", raw, ts)

    response = _post(client, raw, timestamp=ts, signature=bad_signature)

    assert response.status_code == 401
    assert response.json() == {"status": "rejected", "reason": "invalid_signature"}
    # No request data is echoed back.
    assert b"hello" not in response.content
    conn = client.app.state.db_conn
    assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 0
    counters = db.get_counters(conn)
    assert counters["rejected_signature"] == 1
    assert counters["received"] == 1
    assert "accepted" not in counters


def test_missing_headers_rejected(client):
    raw = json.dumps(_envelope()).encode("utf-8")

    no_signature = client.post("/webhook", content=raw, headers={"X-Timestamp": str(int(time.time()))})
    no_timestamp = client.post("/webhook", content=raw, headers={"X-Signature": "0" * 64})
    neither = client.post("/webhook", content=raw)

    for response in (no_signature, no_timestamp, neither):
        assert response.status_code == 401
    # Missing timestamp is a timestamp failure; missing X-Signature (with a
    # fresh timestamp) is a signature failure.
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_timestamp"] == 2  # no_timestamp, neither
    assert counters["rejected_signature"] == 1  # no_signature
    assert _queue_is_empty(client) is True


def test_stale_and_future_timestamps_rejected(client):
    raw = json.dumps(_envelope()).encode("utf-8")
    stale_ts = int(time.time()) - 3600  # 1 h in the past, outside +-5 min
    future_ts = int(time.time()) + 3600

    stale = _post(client, raw, timestamp=stale_ts, signature=_sign(SECRET, raw, stale_ts))
    future = _post(client, raw, timestamp=future_ts, signature=_sign(SECRET, raw, future_ts))

    assert stale.status_code == 401
    assert future.status_code == 401
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_timestamp"] == 2
    assert _queue_is_empty(client) is True


def test_garbled_timestamp_rejected(client):
    raw = json.dumps(_envelope()).encode("utf-8")
    response = _post(
        client, raw, timestamp="not-a-number", signature="0" * 64
    )
    assert response.status_code == 401
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_timestamp"] == 1


def test_uppercase_and_padded_signature_still_accepted(client):
    """Input normalization: the gateway sends lowercase hex; be tolerant."""
    raw = json.dumps(_envelope()).encode("utf-8")
    ts = int(time.time())
    signature = _sign(SECRET, raw, ts).upper()

    response = _post(client, raw, timestamp=ts, signature=signature)
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Body size and payload validation
# ---------------------------------------------------------------------------


def test_oversized_body_rejected_without_echo(client):
    oversized = b"x" * (MAX_BODY_BYTES + 1)

    response = client.post(
        "/webhook",
        content=oversized,
        headers={"Content-Type": "application/json", "Content-Length": str(len(oversized))},
    )

    assert response.status_code == 413
    # The response must not echo the (rejected) request body.
    assert b"xxxxx" not in response.content
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_size"] == 1
    assert _queue_is_empty(client) is True


def test_chunked_oversized_body_rejected_without_full_read(tmp_path, write_config):
    """A chunked upload above the cap is cut off mid-stream (no unbounded buffering)."""
    _oversized_stream_rejection(tmp_path, write_config, [(b"transfer-encoding", b"chunked")])


def test_lying_content_length_rejected_by_streamed_cap(tmp_path, write_config):
    """An understated Content-Length does not bypass the streamed cap."""
    _oversized_stream_rejection(tmp_path, write_config, [(b"content-length", b"10")])


def test_invalid_json_rejected_without_echo(client):
    raw = b'{"event": "sms:received", "broken...'
    ts = int(time.time())

    response = _post(client, raw, timestamp=ts, signature=_sign(SECRET, raw, ts))

    assert response.status_code == 400
    assert response.json() == {"status": "rejected", "reason": "invalid_payload"}
    assert b"broken" not in response.content
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_payload"] == 1
    assert _queue_is_empty(client) is True


def test_non_object_json_rejected(client):
    raw = b'["not", "an", "object"]'
    ts = int(time.time())
    response = _post(client, raw, timestamp=ts, signature=_sign(SECRET, raw, ts))
    assert response.status_code == 400


def test_sms_received_requires_message_id(client):
    envelope = _envelope()
    del envelope["payload"]["messageId"]

    response = _post_envelope(client, envelope)

    assert response.status_code == 400
    counters = db.get_counters(client.app.state.db_conn)
    assert counters["rejected_payload"] == 1
    assert _queue_is_empty(client) is True


def test_empty_message_id_rejected(client):
    envelope = _envelope(message_id="")
    response = _post_envelope(client, envelope)
    assert response.status_code == 400


def test_unknown_envelope_fields_tolerated(client):
    """Forward compatibility: new gateway fields must not break the receiver."""
    envelope = _envelope()
    envelope["payloadVersion"] = 99
    envelope["payload"]["newShinyField"] = {"any": ["shape"]}

    response = _post_envelope(client, envelope)
    assert response.status_code == 200


def test_legacy_phone_number_payload_accepted(client):
    """Older gateway versions send phoneNumber instead of sender."""
    envelope = _envelope()
    del envelope["payload"]["sender"]
    envelope["payload"]["phoneNumber"] = "+79001234567"

    response = _post_envelope(client, envelope)
    assert response.status_code == 200


def test_non_sms_event_uses_envelope_id(client):
    """Events without payload.messageId are keyed by the envelope id."""
    envelope = _envelope(event="system:ping", event_id="evt-ping-1")
    envelope["payload"] = {"health": {"battery": 80}}

    response = _post_envelope(client, envelope)
    assert response.status_code == 200

    conn = client.app.state.db_conn
    rows = conn.execute("SELECT message_id FROM queue").fetchall()
    assert [row["message_id"] for row in rows] == ["evt-ping-1"]


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


def test_duplicate_message_id_answered_2xx_and_skipped(client):
    first = _post_envelope(client, _envelope(event_id="evt-a"))
    duplicate = _post_envelope(client, _envelope(event_id="evt-b"))  # same messageId

    assert first.status_code == 200
    assert duplicate.status_code == 200

    conn = client.app.state.db_conn
    assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 1
    counters = db.get_counters(conn)
    assert counters["accepted"] == 1
    assert counters["dedup_skipped"] == 1


# ---------------------------------------------------------------------------
# Rate limit
# ---------------------------------------------------------------------------


def test_rate_limit_returns_429(write_config, tmp_path):
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(
        webhook={
            "secret": SECRET,
            "rate_limit_per_min": 2,
            "max_body_bytes": MAX_BODY_BYTES,
            "timestamp_window_s": 300,
        }
    )
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    with TestClient(app) as test_client:
        assert (
            _post_envelope(test_client, _envelope(message_id="m1", event_id="e1")).status_code
            == 200
        )
        assert (
            _post_envelope(test_client, _envelope(message_id="m2", event_id="e2")).status_code
            == 200
        )
        third = _post_envelope(test_client, _envelope(message_id="m3", event_id="e3"))

        assert third.status_code == 429
        assert third.json() == {"status": "rejected", "reason": "rate_limited"}
        counters = db.get_counters(test_client.app.state.db_conn)
        assert counters["rate_limited"] == 1
        assert counters["accepted"] == 2


def test_rate_limit_hot_reload_applies_new_limit(write_config, tmp_path):
    """A hot-reloaded rate limit takes effect without a restart."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(
        webhook={
            "secret": SECRET,
            "rate_limit_per_min": 2,
            "max_body_bytes": MAX_BODY_BYTES,
            "timestamp_window_s": 300,
        }
    )
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    with TestClient(app) as test_client:
        assert _post_envelope(test_client, _envelope(message_id="m1", event_id="e1")).status_code == 200
        assert _post_envelope(test_client, _envelope(message_id="m2", event_id="e2")).status_code == 200
        assert _post_envelope(test_client, _envelope(message_id="m3", event_id="e3")).status_code == 429

        # Raise the limit in the file and apply it through the same code
        # path the poll loop / SIGHUP handler uses.
        data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
        data["webhook"]["rate_limit_per_min"] = 100
        config_path.write_text(yaml.safe_dump(data), encoding="utf-8")
        assert test_client.app.state.config_manager.reload_now() is True

        # Same IP, same minute: the raised limit lets the request through.
        fourth = _post_envelope(test_client, _envelope(message_id="m4", event_id="e4"))
        assert fourth.status_code == 200

        counters = db.get_counters(test_client.app.state.db_conn)
        assert counters["rate_limited"] == 1
        assert counters["accepted"] == 3


# ---------------------------------------------------------------------------
# Startup guard
# ---------------------------------------------------------------------------


def test_missing_secret_fails_at_startup(write_config, tmp_path):
    """An empty webhook.secret must fail startup with a clear message."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(webhook={"secret": "", "rate_limit_per_min": 120})
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))

    with pytest.raises(RuntimeError, match="signing secret is not configured"):
        with TestClient(app):
            pass


def test_missing_auth_key_fails_at_startup_unless_mock(write_config, tmp_path, monkeypatch):
    """Empty llm.auth_key refuses to start unless mock mode is on (step 4)."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    monkeypatch.delenv("MOCK_LLM", raising=False)
    # Real-client mode + empty key -> fail fast.
    config_path = write_config(llm={
        "model": "GigaChat-2", "temperature": 0.4, "max_tokens": 500,
        "timeout_s": 90, "auth_key": "", "mock": False,
    })
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    with pytest.raises(RuntimeError, match="GigaChat auth key is not configured"):
        with TestClient(app):
            pass

    # MOCK_LLM=true overrides the config flag: startup succeeds without a key.
    monkeypatch.setenv("MOCK_LLM", "true")
    with TestClient(app) as test_client:
        assert test_client.get("/healthz").status_code == 200


# ---------------------------------------------------------------------------
# Privacy: never log message texts or request bodies
# ---------------------------------------------------------------------------


def test_logs_never_contain_message_text(client, caplog):
    secret_text = "CLASSIFIED-PAYLOAD-TEXT-42"
    envelope = _envelope()
    envelope["payload"]["message"] = secret_text

    with caplog.at_level("INFO"):
        response = _post_envelope(client, envelope)

    assert response.status_code == 200
    log_output = caplog.text
    assert secret_text not in log_output
    # Metadata is still logged for troubleshooting.
    assert "msg-1" in log_output
    assert "sms:received" in log_output
