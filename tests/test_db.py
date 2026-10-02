"""Tests for the SQLite layer: WAL, idempotent migrations, counters."""

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from app import db

EXPECTED_TABLES = {
    "queue",
    "processed_events",
    "sessions",
    "session_messages",
    "device_state",
    "counters",
}

EXPECTED_INDEXES = {
    "idx_queue_status_id",
    "idx_session_messages_session_id",
    "idx_processed_events_processed_at",
    "idx_sessions_sender_session_no",
}


def _table_names(conn: sqlite3.Connection) -> set:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row["name"] for row in rows}


def _index_names(conn: sqlite3.Connection) -> set:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {row["name"] for row in rows}


def test_wal_mode_enabled(tmp_path):
    conn = db.open_db(tmp_path / "wal.db")
    try:
        db.run_migrations(conn)
        assert conn.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    finally:
        conn.close()


def test_migrations_create_all_tables_and_indexes(tmp_path):
    conn = db.open_db(tmp_path / "schema.db")
    try:
        db.run_migrations(conn)
        assert _table_names(conn) == EXPECTED_TABLES
        assert EXPECTED_INDEXES.issubset(_index_names(conn))
        assert (
            conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
        )
    finally:
        conn.close()


def test_migrations_are_idempotent(tmp_path):
    conn = db.open_db(tmp_path / "idem.db")
    try:
        for _ in range(3):
            db.run_migrations(conn)
        assert _table_names(conn) == EXPECTED_TABLES
        assert (
            conn.execute("PRAGMA user_version").fetchone()[0] == db.SCHEMA_VERSION
        )
        # Existing data survives repeated migrations.
        db.increment_counter(conn, "messages_in", 2)
        db.run_migrations(conn)
        assert db.get_counters(conn) == {"messages_in": 2}
    finally:
        conn.close()


def test_unique_session_index_enforces_one_session_per_pair(tmp_path):
    """Schema v3 invariant: one session per (sender, session_no) at the DB level."""
    conn = db.open_db(tmp_path / "unique.db")
    try:
        db.run_migrations(conn)
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        with conn:
            conn.execute(
                "INSERT INTO sessions (sender, session_no, created_at, updated_at, expires_at)"
                " VALUES (?, ?, ?, ?, ?)",
                ("+79001234567", 1, now, now, "2099-01-01T00:00:00.000+00:00"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            with conn:
                conn.execute(
                    "INSERT INTO sessions (sender, session_no, created_at, updated_at, expires_at)"
                    " VALUES (?, ?, ?, ?, ?)",
                    ("+79001234567", 1, now, now, "2099-01-01T00:00:00.000+00:00"),
                )
    finally:
        conn.close()


def test_refuses_newer_schema_version(tmp_path):
    conn = db.open_db(tmp_path / "future.db")
    try:
        conn.execute("PRAGMA user_version = %d" % (db.SCHEMA_VERSION + 5))
        with pytest.raises(RuntimeError):
            db.run_migrations(conn)
    finally:
        conn.close()


def test_failed_migration_rolls_back(tmp_path, monkeypatch):
    """A migration failing mid-way must leave neither partial DDL nor a version bump."""
    conn = db.open_db(tmp_path / "rollback.db")
    try:
        db.run_migrations(conn)  # bring schema to the real SCHEMA_VERSION

        # The rollback mechanics are version-independent, so the failing
        # migration is injected as "the next one" and survives schema
        # evolution (the test does not hardcode a version number).
        real_version = db.SCHEMA_VERSION
        next_version = real_version + 1

        def failing_migration(connection):
            # First statement succeeds, then the migration blows up: the
            # explicit BEGIN in run_migrations must roll the DDL back.
            connection.execute("CREATE TABLE rollback_probe (x INTEGER)")
            raise RuntimeError("boom")

        monkeypatch.setitem(db.MIGRATIONS, next_version, failing_migration)
        monkeypatch.setattr(db, "SCHEMA_VERSION", next_version)

        with pytest.raises(RuntimeError):
            db.run_migrations(conn)

        assert conn.execute("PRAGMA user_version").fetchone()[0] == real_version
        names = _table_names(conn)
        assert "rollback_probe" not in names
        assert EXPECTED_TABLES.issubset(names)
        # The version stamp was not advanced, so re-running migrations would
        # retry the failed migration from scratch (its partial DDL was fully
        # rolled back).
    finally:
        conn.close()


def test_counter_upsert_accumulates(tmp_path):
    conn = db.open_db(tmp_path / "counters.db")
    try:
        db.run_migrations(conn)
        db.increment_counter(conn, "messages_in")
        db.increment_counter(conn, "messages_in")
        db.increment_counter(conn, "messages_in", 5)
        db.increment_counter(conn, "other_counter", 3)
        assert db.get_counters(conn) == {"messages_in": 7, "other_counter": 3}
    finally:
        conn.close()


def test_enqueue_message_defaults_and_unique_constraint(tmp_path):
    conn = db.open_db(tmp_path / "queue.db")
    try:
        db.run_migrations(conn)
        db.enqueue_message(conn, "m1", '{"sender": "+79001234567"}')
        db.enqueue_message(conn, "m2", '{"sender": "+79001234567"}')
        with pytest.raises(sqlite3.IntegrityError):
            db.enqueue_message(conn, "m1", "{}")  # message_id is UNIQUE

        rows = conn.execute(
            "SELECT message_id, status FROM queue ORDER BY id"
        ).fetchall()
        assert [row["message_id"] for row in rows] == ["m1", "m2"]
        assert all(row["status"] == "pending" for row in rows)
        assert db.count_queue_by_status(conn) == {"pending": 2}
    finally:
        conn.close()


def test_accept_message_enqueues_and_marks_processed(tmp_path):
    conn = db.open_db(tmp_path / "accept.db")
    try:
        db.run_migrations(conn)
        row_id = db.accept_message(conn, "m1", '{"messageId": "m1"}')

        assert row_id is not None
        queue_rows = conn.execute(
            "SELECT message_id, payload, status FROM queue WHERE id = ?", (row_id,)
        ).fetchall()
        assert len(queue_rows) == 1
        assert queue_rows[0]["message_id"] == "m1"
        assert queue_rows[0]["status"] == "pending"
        tombstones = conn.execute(
            "SELECT processed_at FROM processed_events WHERE message_id = 'm1'"
        ).fetchall()
        assert len(tombstones) == 1
    finally:
        conn.close()


def test_accept_message_returns_none_on_duplicate(tmp_path):
    conn = db.open_db(tmp_path / "accept-dup.db")
    try:
        db.run_migrations(conn)
        assert db.accept_message(conn, "m1", "{}") is not None
        # Duplicate delivery: nothing is enqueued twice, no exception raised.
        assert db.accept_message(conn, "m1", "{}") is None
        assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM processed_events").fetchone()[0] == 1
    finally:
        conn.close()


def test_accept_message_purges_expired_dedup_rows(tmp_path):
    """Lazy TTL cleanup: expired tombstones are removed on the next accept."""
    conn = db.open_db(tmp_path / "accept-ttl.db")
    try:
        db.run_migrations(conn)
        assert db.accept_message(conn, "old", "{}") is not None

        # Make the first tombstone 49 hours old: beyond the 48 h TTL
        # (gateway retry horizon is ~45.5 h).
        stale_cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=49)
        ).isoformat(timespec="milliseconds")
        conn.execute(
            "UPDATE processed_events SET processed_at = ? WHERE message_id = 'old'",
            (stale_cutoff,),
        )
        conn.commit()

        assert db.accept_message(conn, "new", "{}") is not None

        remaining = conn.execute(
            "SELECT message_id FROM processed_events ORDER BY message_id"
        ).fetchall()
        assert [row["message_id"] for row in remaining] == ["new"]
        assert conn.execute("SELECT COUNT(*) FROM queue").fetchone()[0] == 2
    finally:
        conn.close()
