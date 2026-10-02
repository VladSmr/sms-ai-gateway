"""Session store: numbered sessions per whitelisted sender (step 3).

Domain logic on top of the v1 schema (``sessions``/``session_messages``);
a schema migration was not required: TTL is served by ``expires_at``,
LRU ordering by ``updated_at``, and history cleanup by the existing
``ON DELETE CASCADE`` foreign key.

Invariants:

- TTL: a session expires after ``limits.session_ttl_h`` of inactivity
  (``expires_at`` is refreshed on every touch). Expired sessions are
  dropped lazily on access (a fresh empty session is created in the same
  transaction) and by the daily sweep (:func:`sweep_expired`, wired into
  the worker loop).
- LRU cap: at most ``limits.max_sessions`` live sessions per sender.
  Creating session ``N+1`` when the cap is reached evicts the least
  recently active session of that sender (oldest ``updated_at``).
- History: at most ``limits.history_messages`` latest messages are kept;
  older rows are trimmed on every append.

PRIVACY (152-FZ): this module never logs message or override prompt text -
session/sender metadata only.
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .. import db

logger = logging.getLogger(__name__)

#: The session number used when a message has no ``#N`` routing prefix.
DEFAULT_SESSION_NO = 1


@dataclass(frozen=True)
class Session:
    """A live session row (snapshot)."""

    id: int
    session_no: int
    sender: str
    override_temp: float | None
    override_prompt: str | None


@dataclass(frozen=True)
class SessionInfo:
    """Listing record for the ``LIST`` command (no message content)."""

    session_no: int
    updated_at: str
    expires_at: str


def compute_expiry(now_iso: str, ttl_hours: int) -> str:
    """``expires_at`` value for a session touched at ``now_iso``.

    The offset-based UTC ISO string (the ``utcnow_iso`` format) compares
    correctly as a plain string, which keeps the SQL side string-only.
    """
    now = datetime.fromisoformat(now_iso)
    return (now + timedelta(hours=ttl_hours)).isoformat(timespec="milliseconds")


def get_or_create(
    conn: sqlite3.Connection,
    sender: str,
    session_no: int,
    *,
    max_sessions: int,
    session_ttl_h: int,
) -> Session:
    """Return the live session ``(sender, session_no)``, creating it if needed.

    An expired session is removed and recreated (lazy TTL cleanup); the LRU
    cap is enforced afterwards, so a brand-new session always wins a slot.
    """
    now = db.utcnow_iso()
    with conn:
        row = conn.execute(
            "SELECT id, expires_at, override_temp, override_prompt FROM sessions "
            "WHERE sender = ? AND session_no = ?",
            (sender, session_no),
        ).fetchone()
        if row is not None and row["expires_at"] > now:
            conn.execute(
                "UPDATE sessions SET updated_at = ?, expires_at = ? WHERE id = ?",
                (now, compute_expiry(now, session_ttl_h), row["id"]),
            )
            return Session(
                id=int(row["id"]),
                session_no=session_no,
                sender=sender,
                override_temp=row["override_temp"],
                override_prompt=row["override_prompt"],
            )
        if row is not None:
            # Expired: drop it (messages cascade) and start fresh.
            conn.execute("DELETE FROM sessions WHERE id = ?", (row["id"],))
            logger.info(
                "Session expired, recreated: sender=%s session_no=%d", sender, session_no
            )
        count = conn.execute(
            "SELECT COUNT(*) FROM sessions WHERE sender = ?", (sender,)
        ).fetchone()[0]
        if count >= max_sessions:
            oldest = conn.execute(
                "SELECT id FROM sessions WHERE sender = ? "
                "ORDER BY updated_at ASC, id ASC LIMIT 1",
                (sender,),
            ).fetchone()
            conn.execute("DELETE FROM sessions WHERE id = ?", (oldest["id"],))
            logger.info(
                "Session evicted (LRU): sender=%s max_sessions=%d", sender, max_sessions
            )
        cursor = conn.execute(
            "INSERT INTO sessions (session_no, sender, created_at, updated_at, expires_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (session_no, sender, now, now, compute_expiry(now, session_ttl_h)),
        )
        session_id = int(cursor.lastrowid)
    return Session(
        id=session_id, session_no=session_no, sender=sender, override_temp=None,
        override_prompt=None,
    )


def append_message(
    conn: sqlite3.Connection,
    session_id: int,
    role: str,
    content: str,
    *,
    history_messages: int,
) -> None:
    """Append a message and trim the history to the last ``history_messages``."""
    now = db.utcnow_iso()
    with conn:
        conn.execute(
            "INSERT INTO session_messages (session_id, role, content, created_at) "
            "VALUES (?, ?, ?, ?)",
            (session_id, role, content, now),
        )
        conn.execute(
            "DELETE FROM session_messages WHERE session_id = ? AND id NOT IN ("
            "SELECT id FROM session_messages WHERE session_id = ? "
            "ORDER BY id DESC LIMIT ?)",
            (session_id, session_id, history_messages),
        )


def get_history(conn: sqlite3.Connection, session_id: int) -> list[dict[str, str]]:
    """Return the stored history (oldest first) as ``{"role", "content"}``."""
    rows = conn.execute(
        "SELECT role, content FROM session_messages WHERE session_id = ? ORDER BY id",
        (session_id,),
    ).fetchall()
    return [{"role": row["role"], "content": row["content"]} for row in rows]


def clear_history(conn: sqlite3.Connection, session_id: int) -> int:
    """Remove all messages of a session (``RESET``). Returns removed count."""
    with conn:
        cursor = conn.execute(
            "DELETE FROM session_messages WHERE session_id = ?", (session_id,)
        )
    return int(cursor.rowcount)


def set_temperature(conn: sqlite3.Connection, session_id: int, value: float) -> None:
    """Store a session-level temperature override (``TEMP``)."""
    with conn:
        conn.execute(
            "UPDATE sessions SET override_temp = ? WHERE id = ?", (value, session_id)
        )


def set_prompt(conn: sqlite3.Connection, session_id: int, text: str) -> None:
    """Store a session-level system prompt override (``PROMPT``)."""
    with conn:
        conn.execute(
            "UPDATE sessions SET override_prompt = ? WHERE id = ?", (text, session_id)
        )


def clear_override(conn: sqlite3.Connection, session_id: int) -> None:
    """Drop both overrides (``RESET``): YAML defaults apply again."""
    with conn:
        conn.execute(
            "UPDATE sessions SET override_temp = NULL, override_prompt = NULL "
            "WHERE id = ?",
            (session_id,),
        )


def list_sessions(conn: sqlite3.Connection, sender: str) -> list[SessionInfo]:
    """All live sessions of a sender, ordered by session number.

    Expired rows may still be present until they are touched or the sweep
    runs; the caller decides whether to filter them out (the ``LIST``
    command does, to answer with actionable sessions only).
    """
    now = db.utcnow_iso()
    rows = conn.execute(
        "SELECT session_no, updated_at, expires_at FROM sessions "
        "WHERE sender = ? AND expires_at > ? ORDER BY session_no",
        (sender, now),
    ).fetchall()
    return [
        SessionInfo(
            session_no=int(row["session_no"]),
            updated_at=row["updated_at"],
            expires_at=row["expires_at"],
        )
        for row in rows
    ]


def sweep_expired(conn: sqlite3.Connection) -> int:
    """Delete all expired sessions; returns the removed count.

    Called by the worker's daily sweep (the lazy path in
    :func:`get_or_create` covers sessions that are actually addressed).
    """
    now = db.utcnow_iso()
    with conn:
        cursor = conn.execute("DELETE FROM sessions WHERE expires_at < ?", (now,))
    removed = int(cursor.rowcount)
    if removed:
        logger.info("Session sweep: expired_removed=%d", removed)
    return removed
