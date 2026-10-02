"""Final-review fixes: rate-limit hardening, webhook concurrency, banner (V1/M3/V3).

A1: the per-IP limiter must not leak memory under a flood of UNIQUE spoofed
    X-Forwarded-For keys, and the shared per-IP budget must not be bypassed
    by key rotation (the Caddy header_up rewrite makes the key the real
    client; these tests pin the limiter-side contract it relies on).
M3: concurrent /webhook requests share one sqlite connection whose writes
    mix explicit (accept_message) and implicit (counters) transactions;
    serialized by the app's accept_lock they must never raise
    OperationalError and must never enqueue one message twice.
V3: an empty whitelist must log a loud startup warning (no fail-fast) and
    set the ``empty_whitelist`` health counter.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time

import pytest

from app import db
from app.rate_limit import RateLimiter

SECRET = "test-webhook-secret"


# ---------------------------------------------------------------------------
# A1: limiter memory bound under unique-key floods
# ---------------------------------------------------------------------------


class _FakeClock:
    """Deterministic monotonic clock (same shape as test_rate_limit's)."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_flood_of_unique_keys_does_not_grow_dict_unboundedly(monkeypatch):
    """1000 requests with UNIQUE keys (spoofed-XFF flood): the tracked-keys
    dict stays bounded - keys whose 60 s window has fully drained are swept
    away while the flood is still running."""
    import app.rate_limit as rl

    monkeypatch.setattr(rl, "MAX_KEYS", 10)
    monkeypatch.setattr(rl, "SWEEP_INTERVAL", 1)

    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=5, time_func=clock)

    # 1 s between requests: the flood spans 1000 s, so at every sweep the
    # keys whose newest hit is older than 60 s are dead and get dropped.
    for i in range(1000):
        clock.now += 1.0
        assert limiter.allow("10.0.%d.%d" % (i // 256, i % 256)) is True

    # Only keys hit within the last 60 s may remain (the newest ~60 keys).
    assert len(limiter._hits) <= 61
    assert "10.0.0.0" not in limiter._hits  # long expired -> swept


def test_sweep_drops_drained_keys_keeps_live():
    """A sweep removes fully-drained windows and never touches live keys."""
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=5, time_func=clock)

    limiter.allow("old")
    clock.now += 61.0  # the "old" window is fully drained now
    limiter.allow("fresh")  # live key

    limiter._sweep(cutoff=clock.now - 60.0)
    assert "old" not in limiter._hits
    assert "fresh" in limiter._hits


def test_spoofed_xff_rotation_does_not_bypass_shared_budget():
    """With ONE resolved client identity (what Caddy's header_up rewrite
    guarantees in production), rotating fake XFF values cannot spread
    requests across buckets: the single real-IP key is capped."""
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=3, time_func=clock)

    # Attacker rotates fake XFF values; Caddy replaces the header, so the
    # app sees one stable key (the real remote host) for all of them.
    results = [limiter.allow("203.0.113.7") for _ in range(10)]
    assert results == [True] * 3 + [False] * 7

    # A different real client is unaffected.
    assert limiter.allow("198.51.100.9") is True


def test_flood_unique_keys_limit_still_applies_per_key():
    """Rotation does not lift the cap: each unique spoofed key gets its own
    budget (bounded by the sweep), and a repeated key is still limited."""
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=2, time_func=clock)

    for i in range(6):
        assert limiter.allow("9.9.9.%d" % i) is True
    # Repeating the first spoofed key: 1 hit left in its window -> one more
    # allowed, then the per-key cap applies again.
    assert limiter.allow("9.9.9.0") is True
    assert limiter.allow("9.9.9.0") is False


# ---------------------------------------------------------------------------
# A1 (integration): webhook rate limit per resolved client identity
# ---------------------------------------------------------------------------


def _signed_headers(raw: bytes, secret: str = SECRET) -> dict:
    ts = int(time.time())
    sig = hmac.new(secret.encode(), raw + str(ts).encode(), hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Timestamp": str(ts),
        "X-Signature": sig,
    }


def _ping_envelope(event_id: str) -> bytes:
    return json.dumps(
        {
            "deviceId": "d",
            "event": "system:ping",
            "id": event_id,
            "payload": {"health": {}},
        }
    ).encode()


def test_webhook_same_xff_shares_one_budget_bucket(write_config, tmp_path):
    """Requests arriving with the SAME client identity (in production the
    real IP after Caddy's header_up rewrite) share one budget bucket: the
    third request is 429 even though the envelopes differ."""
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(
        webhook={
            "secret": SECRET,
            "rate_limit_per_min": 2,
            "max_body_bytes": 65536,
            "timestamp_window_s": 300,
        }
    )
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    with TestClient(app) as test_client:
        statuses = []
        for i in range(3):
            raw = _ping_envelope("evt-xff-same-%d" % i)
            response = test_client.post(
                "/webhook",
                content=raw,
                headers={
                    **_signed_headers(raw),
                    "X-Forwarded-For": "203.0.113.7",  # identical every time
                },
            )
            statuses.append(response.status_code)
        assert statuses == [200, 200, 429]
        # A further request from the same identity stays gated (429 - the
        # limiter fires before the signature check).
        assert (
            test_client.post(
                "/webhook", content=b"x", headers={"X-Forwarded-For": "203.0.113.7"}
            ).status_code
            == 429
        )


# ---------------------------------------------------------------------------
# M3: concurrent /webhook requests must not corrupt the shared connection
# ---------------------------------------------------------------------------


def _ping_request(ac, event_id: str, xff: str):
    raw = _ping_envelope(event_id)
    return ac.post(
        "/webhook",
        content=raw,
        headers={**_signed_headers(raw), "X-Forwarded-For": xff},
    )


def test_parallel_webhook_requests_no_operational_error(write_config, tmp_path):
    """Two simultaneous /webhook requests with the SAME message id: both get
    a 2xx, exactly one queue row exists, no OperationalError surfaces. The
    requests run CONCURRENTLY on one event loop (httpx ASGITransport), so
    the to_thread DB sections truly overlap under the receiver's lock."""
    import httpx

    from app.main import create_app

    config_path = write_config()
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))

    async def scenario():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as ac:
                responses = await asyncio.gather(
                    *[
                        ac.post(
                            "/webhook",
                            content=_ping_envelope("evt-race-1"),
                            headers={
                                **_signed_headers(_ping_envelope("evt-race-1")),
                                "X-Forwarded-For": "9.9.9.%d" % i,
                            },
                        )
                        for i in range(2)
                    ]
                )
            # Assert INSIDE the lifespan: the app closes its DB connections
            # on exit, so the DB must still be queried here.
            conn = app.state.db_conn
            rows = conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
            counters = db.get_counters(conn)
        return responses, rows, counters

    responses, rows, counters = asyncio.run(scenario())
    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    assert rows == 1, "expected exactly one queue row, got %d" % rows
    assert counters["accepted"] + counters.get("dedup_skipped", 0) == 2


def test_parallel_webhook_distinct_messages_all_enqueued(write_config, tmp_path):
    """Six concurrent /webhook requests with DISTINCT ids: all 200, all six
    rows enqueued, no OperationalError - the serialized critical sections
    keep every explicit transaction contiguous."""
    import httpx

    from app.main import create_app

    config_path = write_config()
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))

    async def scenario():
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://testserver"
            ) as ac:
                responses = await asyncio.gather(
                    *[
                        ac.post(
                            "/webhook",
                            content=_ping_envelope("evt-par-%d" % i),
                            headers=_signed_headers(_ping_envelope("evt-par-%d" % i)),
                        )
                        for i in range(6)
                    ]
                )
            conn = app.state.db_conn
            rows = conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
        return responses, rows

    responses, rows = asyncio.run(scenario())
    assert all(r.status_code == 200 for r in responses), [r.status_code for r in responses]
    assert rows == 6, "expected 6 enqueued rows, got %d" % rows


# ---------------------------------------------------------------------------
# V3: empty-whitelist startup banner
# ---------------------------------------------------------------------------


class _LoggerProbe(logging.Handler):
    """Capture records from one logger even though create_app() reconfigures
    the ROOT logger (setup_logging removes foreign root handlers, so
    pytest's caplog - installed on the root logger - cannot see records
    emitted after the app reconfigured logging)."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def test_empty_whitelist_logs_warning_banner(write_config, tmp_path):
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(whitelist=[])
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    probe = _LoggerProbe()
    app_logger = logging.getLogger("app.main")
    app_logger.addHandler(probe)
    try:
        with TestClient(app):
            pass
    finally:
        app_logger.removeHandler(probe)
    assert any(
        "Empty whitelist" in m and "ALL incoming SMS will be dropped" in m
        for m in probe.messages
    ), probe.messages


def test_empty_whitelist_sets_health_counter(write_config, tmp_path):
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(whitelist=[])
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    with TestClient(app):
        counters = db.get_counters(app.state.db_conn)
        assert counters.get("empty_whitelist") == 1


def test_non_empty_whitelist_no_warning(write_config, tmp_path):
    from fastapi.testclient import TestClient

    from app.main import create_app

    config_path = write_config(whitelist=["+79001234567"])
    app = create_app(config_path=str(config_path), log_file=str(tmp_path / "logs" / "app.log"))
    probe = _LoggerProbe()
    app_logger = logging.getLogger("app.main")
    app_logger.addHandler(probe)
    try:
        with TestClient(app):
            pass
    finally:
        app_logger.removeHandler(probe)
    assert not any("Empty whitelist" in m for m in probe.messages)
