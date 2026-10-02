"""SQLite storage: WAL mode, idempotent migrations via ``PRAGMA user_version``.

Tables (schema v6): queue (with per-row ``attempts``, ``deferred_reply``
(W5) and a ``failed`` dead-letter status), processed_events (TTL-dedup),
sessions
(unique per sender+number), session_messages, device_state, counters.
The receiver (step 2) is
append-only on ``queue``/``processed_events``; the domain logic (worker)
owns the rest.

Timestamps are stored as ISO-8601 UTC strings.
"""

import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 6

# processed_events TTL: must cover the gateway's webhook retry horizon
# (exponential backoff starting at 10 s, 14 retries by default ~= 45.5 h).
PROCESSED_EVENT_TTL_HOURS = 48


def utcnow_iso() -> str:
    """Current UTC time as an ISO-8601 string with millisecond precision."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def open_db(path: str | Path) -> sqlite3.Connection:
    """Open a SQLite connection tuned for this application.

    - WAL journal mode (concurrent reads during writes, Windows-friendly);
    - foreign keys enabled;
    - busy timeout to survive brief lock contention;
    - ``check_same_thread=False`` because sync calls are made from executor
      threads (single worker serialises writes by design).

    The bundled sqlite3 library version is logged at INFO level: sqlite-vec
    (Phase 2) has a minimum supported SQLite version, so the runtime value
    must be known on the target host / container image.
    """
    db_path = Path(path)
    if db_path.parent and not db_path.parent.exists():
        db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    journal_mode = conn.execute("PRAGMA journal_mode=WAL").fetchone()[0]
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    logger.info(
        "SQLite opened: path=%s sqlite_version=%s journal_mode=%s",
        db_path,
        sqlite3.sqlite_version,
        journal_mode,
    )
    return conn


def _migration_v1(conn: sqlite3.Connection) -> None:
    """Schema v1: initial tables and indexes (all idempotent).

    Each statement is executed individually (no ``executescript``): in
    Python 3.8 ``executescript`` commits any pending transaction first and
    runs its statements in autocommit mode, which would break the
    transactional guarantee of :func:`run_migrations`.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL UNIQUE,
            payload TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'processing', 'failed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_queue_status_id ON queue (status, id)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processed_events (
            message_id TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_no INTEGER NOT NULL,
            sender TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            override_temp REAL,
            override_prompt TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS session_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL
                REFERENCES sessions(id) ON DELETE CASCADE,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_session_messages_session_id
            ON session_messages (session_id)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS device_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS counters (
            key TEXT PRIMARY KEY,
            value INTEGER NOT NULL DEFAULT 0
        )
        """
    )


def _migration_v2(conn: sqlite3.Connection) -> None:
    """Schema v2: index for the lazy processed_events TTL cleanup.

    The webhook receiver purges expired dedup rows lazily on every accepted
    delivery (``DELETE ... WHERE processed_at < cutoff``), so ``processed_at``
    needs an index to keep that statement cheap.
    """
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at "
        "ON processed_events (processed_at)"
    )


def _migration_v3(conn: sqlite3.Connection) -> None:
    """Schema v3: unique index enforcing one session per (sender, number).

    ``sessions.store.get_or_create`` looks a session up and then inserts it;
    with a single worker the race is unlikely, but the database-level
    UNIQUE constraint makes the invariant absolute (a losing racer would
    get ``IntegrityError`` instead of a silent duplicate).
    """
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_sender_session_no "
        "ON sessions (sender, session_no)"
    )


def _migration_v4(conn: sqlite3.Connection) -> None:
    """Schema v4: persistent send_id in the queue.

    The ``send_id`` column (UUID, NULL on legacy rows) stores the idempotency
    key for the gateway send call.  When the worker requeues a row after a
    transient failure it reuses the same send_id — the gateway dedupes on it,
    so a retried 5xx cannot double-send an SMS.

    NULL on pre-existing rows (the gateway uses the message_id hash as its
    dedup key for those, which is still safe — a cancelled message_id would
    mean the SMS was never sent and a retry is harmless).  New rows always
    carry a send_id set by the pipeline before enqueue.
    """
    conn.execute(
        "ALTER TABLE queue ADD COLUMN send_id TEXT"
    )


def _migration_v5(conn: sqlite3.Connection) -> None:
    """Schema v5: per-row retry counter for the dead-letter policy (W2).

    The ``attempts`` column counts failed processing attempts per queue row.
    The worker increments it on every unexpected exception; once the count
    reaches the retry budget (``worker.max_row_attempts``) the row moves to
    status ``failed`` (dead-letter) instead of being requeued forever - a
    permanently broken row must not block the FIFO queue (a stuck pending
    row with a lower id delays every newer row behind it).

    SQLite cannot ALTER a CHECK constraint, so the ``queue`` table is
    rebuilt (12-step recreate) to (a) add ``attempts`` and (b) extend the
    ``status`` CHECK with ``'failed'`` for databases created by v1 - v4.
    Databases created fresh at v1+ already get the new CHECK from
    :func:`_migration_v1`; the rebuild keeps both paths identical.

    Data-preserving: all rows, ids and the AUTOINCREMENT sequence survive.
    """
    # Capture the legacy schema shape (v1-v4 DBs have the narrow CHECK).
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(queue)").fetchall()]
    has_send_id = "send_id" in columns
    has_attempts = "attempts" in columns
    if not has_send_id or not has_attempts:
        # Rebuild: rename old -> new via INSERT SELECT preserving ids.
        conn.execute("ALTER TABLE queue RENAME TO queue_old_v5")
        conn.execute(
            """
            CREATE TABLE queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT NOT NULL UNIQUE,
                payload TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'processing', 'failed')),
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                send_id TEXT,
                attempts INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        if has_send_id:
            conn.execute(
                "INSERT INTO queue (id, message_id, payload, status, created_at, "
                "updated_at, send_id, attempts) "
                "SELECT id, message_id, payload, status, created_at, updated_at, "
                "send_id, 0 FROM queue_old_v5"
            )
        else:
            conn.execute(
                "INSERT INTO queue (id, message_id, payload, status, created_at, "
                "updated_at, attempts) "
                "SELECT id, message_id, payload, status, created_at, updated_at, 0 "
                "FROM queue_old_v5"
            )
        # The old index travels with the renamed table (SQLite keeps indexes
        # attached across RENAME), so it must be dropped WITH the old table
        # before the fresh one is created - otherwise `IF NOT EXISTS` skips
        # the create and DROP TABLE silently removes the only index.
        conn.execute("DROP TABLE queue_old_v5")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_queue_status_id ON queue (status, id)"
        )


def _migration_v6(conn: sqlite3.Connection) -> None:
    """W5 (stale-replay fix): explicit deferred-re-send marker on the queue.

    The previous replay trigger (``attempts >= 1``) conflated the W2 failure
    budget with the W5 deferral state: a row deferred on a COMMAND (commands
    write no assistant history) or requeued after a generic crash would
    replay an unrelated old reply on the next claim. The reply text to
    re-send is now stored in ``deferred_reply``; NULL means "process
    normally" regardless of ``attempts``.
    """
    columns = [row["name"] for row in conn.execute("PRAGMA table_info(queue)").fetchall()]
    if "deferred_reply" not in columns:
        conn.execute("ALTER TABLE queue ADD COLUMN deferred_reply TEXT")


# version -> migration function; append v7+ below in future steps.
MIGRATIONS = {
    1: _migration_v1,
    2: _migration_v2,
    3: _migration_v3,
    4: _migration_v4,
    5: _migration_v5,
    6: _migration_v6,
}


def run_migrations(conn: sqlite3.Connection) -> int:
    """Apply pending migrations; safe to call repeatedly (idempotent).

    Each migration runs inside an explicit BEGIN/COMMIT transaction together
    with its ``PRAGMA user_version`` stamp: sqlite3's implicit transaction
    handling does not open one for DDL statements (and ``executescript``
    would auto-commit), so a manual BEGIN is required to guarantee that a
    failed migration leaves neither partial DDL nor a version bump.

    Returns the resulting schema version.
    """
    current = conn.execute("PRAGMA user_version").fetchone()[0]
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            "Database schema version %d is newer than supported version %d"
            % (current, SCHEMA_VERSION)
        )
    for version in range(current + 1, SCHEMA_VERSION + 1):
        migration = MIGRATIONS[version]
        logger.info("Applying database migration to version %d", version)
        conn.execute("BEGIN")
        try:
            migration(conn)
            conn.execute("PRAGMA user_version = %d" % version)
        except BaseException:
            conn.rollback()
            raise
        conn.commit()
    return conn.execute("PRAGMA user_version").fetchone()[0]


# ---------------------------------------------------------------------------
# Small typed helpers shared by receiver / worker / health endpoint
# ---------------------------------------------------------------------------


def enqueue_message(
    conn: sqlite3.Connection,
    message_id: str,
    payload: str,
    send_id: str | None = None,
) -> int:
    """Insert a message into the FIFO queue as ``pending``. Returns row id.

    ``send_id`` (optional, v4+) stores the gateway idempotency key so the
    pipeline can reuse it on requeue (no double-SMS if the send is retried).
    """
    now = utcnow_iso()
    with conn:
        cursor = conn.execute(
            "INSERT INTO queue (message_id, payload, send_id, status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?, ?)",
            (message_id, payload, send_id, now, now),
        )
    return int(cursor.lastrowid)


def update_queue_send_id(conn: sqlite3.Connection, row_id: int, send_id: str) -> None:
    """Write the persistent send_id onto an existing queue row (v4+).

    Used by the pipeline to stamp a new send_id before the row is
    processed; the worker requeues with the same id on transient failures.
    """
    now = utcnow_iso()
    with conn:
        conn.execute(
            "UPDATE queue SET send_id = ?, updated_at = ? WHERE id = ?",
            (send_id, now, row_id),
        )


def accept_message(
    conn: sqlite3.Connection,
    message_id: str,
    payload: str,
    ttl_hours: int = PROCESSED_EVENT_TTL_HOURS,
) -> int | None:
    """Atomically mark the event as processed and enqueue it.

    Used by the webhook receiver: both the ``processed_events`` tombstone and
    the ``queue`` row are committed in a single transaction, so a duplicate
    delivery can never be half-recorded. Returns the queue row id, or
    ``None`` when ``message_id`` was already processed (duplicate webhook
    delivery or duplicate SMS content - the gateway uses content-based
    message ids): the caller answers 2xx without re-enqueueing. The new row
    carries ``send_id = NULL``; the pipeline stamps a fresh idempotency key
    before the first send attempt (``update_queue_send_id``).

    Expired dedup rows (older than ``ttl_hours``) are purged lazily inside
    the same transaction; the TTL covers the gateway's retry horizon
    (~45.5 h: exponential backoff from 10 s, 14 retries by default).

    Manual BEGIN/COMMIT is required (same reasoning as in
    :func:`run_migrations`): the two INSERTs must be committed or discarded
    together, and the ``IntegrityError`` path must roll everything back,
    including the tombstone of the losing racer.
    """
    now = utcnow_iso()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).isoformat(
        timespec="milliseconds"
    )
    conn.execute("BEGIN")
    try:
        conn.execute("DELETE FROM processed_events WHERE processed_at < ?", (cutoff,))
        cursor = conn.execute(
            "INSERT INTO queue (message_id, payload, send_id, status, created_at, updated_at) "
            "VALUES (?, ?, NULL, 'pending', ?, ?)",
            (message_id, payload, now, now),
        )
        row_id = int(cursor.lastrowid)
        conn.execute(
            "INSERT INTO processed_events (message_id, processed_at) VALUES (?, ?)",
            (message_id, now),
        )
        conn.commit()
    except sqlite3.IntegrityError:
        # Duplicate message_id (queue UNIQUE or processed_events PK).
        conn.rollback()
        return None
    except BaseException:
        conn.rollback()
        raise
    return row_id


def increment_counter(conn: sqlite3.Connection, key: str, amount: int = 1) -> None:
    """Atomically accumulate a named counter (UPSERT)."""
    with conn:
        conn.execute(
            "INSERT INTO counters (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = value + ?",
            (key, amount, amount),
        )


def get_counters(conn: sqlite3.Connection) -> dict[str, int]:
    """Return all counters as a mapping (empty dict when none)."""
    rows = conn.execute("SELECT key, value FROM counters ORDER BY key").fetchall()
    return {row["key"]: int(row["value"]) for row in rows}


def count_queue_by_status(conn: sqlite3.Connection) -> dict[str, int]:
    """Queue depth per status (diagnostics helper)."""
    rows = conn.execute("SELECT status, COUNT(*) AS n FROM queue GROUP BY status").fetchall()
    return {row["status"]: int(row["n"]) for row in rows}


def increment_queue_attempts(conn: sqlite3.Connection, row_id: int) -> int:
    """Add one failed-attempt to a queue row; return the new attempt count.

    Part of the dead-letter policy (schema v5): the worker calls this after
    an unexpected processing exception, before deciding to requeue.
    """
    now = utcnow_iso()
    with conn:
        conn.execute(
            "UPDATE queue SET attempts = attempts + 1, updated_at = ? WHERE id = ?",
            (now, row_id),
        )
    row = conn.execute(
        "SELECT attempts FROM queue WHERE id = ?", (row_id,)
    ).fetchone()
    return int(row["attempts"]) if row is not None else 0


def defer_queue_row(conn: sqlite3.Connection, row_id: int, reply_text: str) -> None:
    """Store the deferred reply text on a queue row (schema v6+, W5).

    One committed write stores the trimmed reply text to re-send AND bumps
    the ``attempts`` budget by exactly one (the single deferred re-send) -
    a crash between two separate writes must not leave the marker without
    the budget bump or vice versa. The marker, NOT the attempt counter, is
    the replay trigger: the next claim sends exactly this text (no second
    LLM call, no command re-execution), keeping the persistent ``send_id``
    so the gateway cannot double-deliver.
    """
    now = utcnow_iso()
    with conn:
        conn.execute(
            "UPDATE queue SET attempts = attempts + 1, deferred_reply = ?, "
            "updated_at = ? WHERE id = ?",
            (reply_text, now, row_id),
        )


def mark_queue_failed(conn: sqlite3.Connection, row_ids: list[int]) -> None:
    """Move queue rows to the ``failed`` dead-letter status (schema v5+).

    Dead-lettered rows keep the payload for inspection (metadata only, never
    logged) and are excluded from ``pending`` claims, so a poisoned row can
    no longer block the FIFO queue. They are NOT auto-deleted: an operator
    inspects/removes them explicitly (``SELECT ... WHERE status = 'failed'``).
    """
    now = utcnow_iso()
    with conn:
        conn.executemany(
            "UPDATE queue SET status = 'failed', updated_at = ? WHERE id = ?",
            [(now, row_id) for row_id in row_ids],
        )


# ---------------------------------------------------------------------------
# device_state (step 5): heartbeat/watchdog and device facts, privacy-safe
# ---------------------------------------------------------------------------


def set_device_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    """Store a device_state key with the current UTC timestamp (UPSERT)."""
    now = utcnow_iso()
    with conn:
        conn.execute(
            "INSERT INTO device_state (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, value, now),
        )


def get_device_state(conn: sqlite3.Connection, key: str) -> str | None:
    """Return the stored value for ``key`` or ``None``."""
    row = conn.execute("SELECT value FROM device_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row is not None else None


def delete_device_state(conn: sqlite3.Connection, key: str) -> None:
    with conn:
        conn.execute("DELETE FROM device_state WHERE key = ?", (key,))
