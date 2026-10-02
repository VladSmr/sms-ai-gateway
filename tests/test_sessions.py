"""Step 3: sender normalization and the session store (TTL/LRU/history)."""

import sqlite3
import time
from datetime import datetime, timedelta, timezone

import pytest

from app import db
from app.sessions import normalizer, store


# ---------------------------------------------------------------------------
# normalize_sender
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Canonical form stays canonical.
        ("+79161234567", "+79161234567"),
        (" +79161234567 ", "+79161234567"),
        # Russian trunk prefix 8 with common separators.
        ("8-9161234567", "+79161234567"),
        ("8-916-123-45-67", "+79161234567"),
        ("8 916 123 45 67", "+79161234567"),
        ("8(916)123-45-67", "+79161234567"),
        ("8.916.123.45.67", "+79161234567"),
        # Country code without the plus.
        ("79161234567", "+79161234567"),
        ("7-916-123-45-67", "+79161234567"),
        # 10-digit national number without any country prefix.
        ("9161234567", "+79161234567"),
        ("916-123-45-67", "+79161234567"),
        # International non-RU numbers pass through.
        ("+447911123456", "+447911123456"),
    ],
)
def test_normalize_sender_canonical_forms(raw, expected):
    assert normalizer.normalize_sender(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",  # empty
        "   ",  # whitespace only
        "MTS",  # alphanumeric operator sender
        "MCHS-moskva",  # alphanumeric service sender
        "SBERBANK",  # alphanumeric bank sender
        "900",  # short service code
        "8-900",  # truncated / short
        "12345678901234567890",  # implausibly long
        "+",  # plus without digits
    ],
)
def test_normalize_sender_unparseable(raw):
    assert normalizer.normalize_sender(raw) is None


def test_normalize_sender_digits_from_mixed_alphanumeric():
    # Alphanumeric sender with embedded digits is still not a phone number.
    assert normalizer.normalize_sender("MTS-900") is None


# ---------------------------------------------------------------------------
# Session store
# ---------------------------------------------------------------------------


@pytest.fixture
def conn():
    connection = db.open_db(":memory:")
    db.run_migrations(connection)
    yield connection
    connection.close()


def _sender_row_count(conn: sqlite3.Connection, sender: str) -> int:
    return int(
        conn.execute("SELECT COUNT(*) FROM sessions WHERE sender = ?", (sender,)).fetchone()[0]
    )


def test_get_or_create_reuses_and_touches_session(conn):
    first = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    second = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    assert first.id == second.id
    assert _sender_row_count(conn, "+79001234567") == 1
    # Touch refreshes expires_at (allow a small clock skew between the
    # timestamp captured inside get_or_create and this recomputation).
    row = conn.execute("SELECT expires_at FROM sessions WHERE id = ?", (first.id,)).fetchone()
    expected = (
        datetime.fromisoformat(db.utcnow_iso()) + timedelta(hours=24)
    ).isoformat(timespec="milliseconds")
    actual = datetime.fromisoformat(row["expires_at"])
    assert abs(actual - datetime.fromisoformat(expected)) < timedelta(seconds=1)


def test_sessions_are_per_sender(conn):
    store.get_or_create(conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24)
    other = store.get_or_create(
        conn, "+79009999999", 1, max_sessions=10, session_ttl_h=24
    )
    assert _sender_row_count(conn, "+79001234567") == 1
    assert _sender_row_count(conn, "+79009999999") == 1


def test_history_trimmed_to_limit(conn):
    session = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    for i in range(12):
        store.append_message(
            conn, session.id, "user", "message-%d" % i, history_messages=10
        )
    history = store.get_history(conn, session.id)
    assert len(history) == 10
    assert [item["content"] for item in history] == ["message-%d" % i for i in range(2, 12)]


def test_expired_session_recreated_lazily(conn):
    session = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    store.append_message(conn, session.id, "user", "old text", history_messages=10)
    # Force-expire: push expires_at into the past.
    with conn:
        conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000+00:00", session.id),
        )
    fresh = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    assert fresh.id != session.id
    assert store.get_history(conn, fresh.id) == []
    assert _sender_row_count(conn, "+79001234567") == 1


def test_lru_evicts_least_recently_active(conn):
    s1 = store.get_or_create(conn, "+79001234567", 1, max_sessions=2, session_ttl_h=24)
    s2 = store.get_or_create(conn, "+79001234567", 2, max_sessions=2, session_ttl_h=24)
    # Timestamps have millisecond precision; make the touch strictly newer
    # than s2's creation (all three operations can fit into one otherwise).
    time.sleep(0.005)
    # Touch session 1 so that session 2 becomes the least recently active.
    store.get_or_create(conn, "+79001234567", 1, max_sessions=2, session_ttl_h=24)
    s3 = store.get_or_create(conn, "+79001234567", 3, max_sessions=2, session_ttl_h=24)
    assert s3.id != s1.id
    remaining = {
        int(row["session_no"])
        for row in conn.execute(
            "SELECT session_no FROM sessions WHERE sender = ?", ("+79001234567",)
        ).fetchall()
    }
    assert remaining == {1, 3}  # session 2 (not touched) was evicted
    assert s2.id != s1.id  # sanity: distinct rows were created


def test_sweep_expired_removes_only_stale_sessions(conn):
    live = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    stale = store.get_or_create(
        conn, "+79001234567", 2, max_sessions=10, session_ttl_h=24
    )
    with conn:
        conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000+00:00", stale.id),
        )
    removed = store.sweep_expired(conn)
    assert removed == 1
    assert conn.execute("SELECT COUNT(*) FROM sessions WHERE id = ?", (live.id,)).fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM sessions WHERE id = ?", (stale.id,)).fetchone()[0] == 0


def test_sweep_cascades_session_messages(conn):
    stale = store.get_or_create(
        conn, "+79001234567", 2, max_sessions=10, session_ttl_h=24
    )
    store.append_message(conn, stale.id, "user", "will vanish", history_messages=10)
    with conn:
        conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000+00:00", stale.id),
        )
    store.sweep_expired(conn)
    assert (
        conn.execute("SELECT COUNT(*) FROM session_messages").fetchone()[0] == 0
    )


def test_list_sessions_filters_expired_and_orders_by_number(conn):
    store.get_or_create(conn, "+79001234567", 2, max_sessions=10, session_ttl_h=24)
    store.get_or_create(conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24)
    expired = store.get_or_create(
        conn, "+79001234567", 5, max_sessions=10, session_ttl_h=24
    )
    with conn:
        conn.execute(
            "UPDATE sessions SET expires_at = ? WHERE id = ?",
            ("2000-01-01T00:00:00.000+00:00", expired.id),
        )
    infos = store.list_sessions(conn, "+79001234567")
    assert [info.session_no for info in infos] == [1, 2]


def test_override_set_and_clear(conn):
    session = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    store.set_temperature(conn, session.id, 1.5)
    store.set_prompt(conn, session.id, "custom prompt")
    reloaded = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    assert reloaded.override_temp == 1.5
    assert reloaded.override_prompt == "custom prompt"
    store.clear_override(conn, reloaded.id)
    cleared = store.get_or_create(
        conn, "+79001234567", 1, max_sessions=10, session_ttl_h=24
    )
    assert cleared.override_temp is None
    assert cleared.override_prompt is None


def test_compute_expiry_uses_utc_offset_string():
    now = db.utcnow_iso()
    expiry = store.compute_expiry(now, 24)
    assert datetime.fromisoformat(expiry) - datetime.fromisoformat(now) == timedelta(hours=24)
