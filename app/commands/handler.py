"""User commands (step 3): RESET / LIST / TEMP / PROMPT / HELP.

Commands are recognized before any LLM call and are case-insensitive
(``reset``, ``Reset``, ``RESET`` all work). They never enter the session
history and never reach the model.

Parsing (:func:`parse_command`) is deliberately permissive about arguments -
it returns a :class:`Command` with the raw argument string; argument
validation (and the short usage hints) lives in :func:`execute_command`,
which composes the SMS-sized replies (the reply budget is
``limits.max_reply_units``-driven and currently ~201 UTF-16 code units).
"""

import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum, auto

from .. import sessions
from ..config import AppConfig

logger = logging.getLogger(__name__)


class CommandType(Enum):
    RESET = auto()
    LIST = auto()
    TEMP = auto()
    PROMPT = auto()
    HELP = auto()


@dataclass(frozen=True)
class Command:
    """A parsed command; ``arg`` is the raw remainder after the keyword."""

    type: CommandType
    arg: str


# Short usage hints - doubled as the invalid-argument replies.
USAGE_RESET = "Usage: RESET [N]"
USAGE_TEMP = "Usage: TEMP 0.0-2.0"
USAGE_PROMPT = "Usage: PROMPT <text>"
HELP_TEXT = (
    "Commands: RESET [N] reset session; LIST sessions; "
    "TEMP x (0.0-2.0); PROMPT <text>; #N <text> -> session N"
)

_TEMP_MIN = 0.0
_TEMP_MAX = 2.0


def parse_command(text: str) -> Command | None:
    """Recognize a command (case-insensitive) or return ``None``.

    Only the exact keyword (optionally followed by an argument) counts:
    ``reset`` is a command, ``reset please`` is ``RESET`` with arg
    ``please`` (rejected later), ``resetting`` is not a command.
    """
    stripped = text.strip()
    if not stripped:
        return None
    head, _, rest = stripped.partition(" ")
    keyword = head.upper()
    if keyword == "RESET":
        return Command(CommandType.RESET, rest.strip())
    if keyword == "LIST":
        return Command(CommandType.LIST, rest.strip())
    if keyword == "TEMP":
        return Command(CommandType.TEMP, rest.strip())
    if keyword == "PROMPT":
        return Command(CommandType.PROMPT, rest.strip())
    if keyword == "HELP":
        return Command(CommandType.HELP, rest.strip())
    return None


def _resolve_session_no(command: Command, route_session_no: int) -> int:
    """``RESET [N]`` argument wins over the routing session number."""
    if command.type is CommandType.RESET and command.arg:
        return int(command.arg)
    return route_session_no


def execute_command(
    conn: sqlite3.Connection,
    command: Command,
    *,
    sender: str,
    route_session_no: int,
    config: AppConfig,
) -> str:
    """Run a command against the session store; returns the SMS reply text.

    ``route_session_no`` is the session selected by the ``#N`` prefix (or
    the default session); ``RESET`` may override it via its ``[N]``
    argument. The session is created on demand so that overrides can be
    stored even for a first message.

    PRIVACY: the PROMPT text is never logged and never echoed back in full -
    the reply only reports its length.
    """
    if command.type is CommandType.HELP:
        return HELP_TEXT

    if command.type is CommandType.LIST:
        infos = sessions.store.list_sessions(conn, sender)
        if not infos:
            return "No active sessions."
        named = config.sessions_named
        parts = []
        for info in infos:
            name = named.get(info.session_no)
            freshness = _format_freshness(info.updated_at)
            parts.append(
                "#%d %s %s" % (info.session_no, name, freshness)
                if name
                else "#%d %s" % (info.session_no, freshness)
            )
        return "Sessions: " + "; ".join(parts)

    if command.type is CommandType.RESET:
        if command.arg and not (command.arg.isdigit() and int(command.arg) >= 1):
            return USAGE_RESET
        session_no = _resolve_session_no(command, route_session_no)
    else:
        session_no = route_session_no

    limits = config.limits
    session = sessions.store.get_or_create(
        conn,
        sender,
        session_no,
        max_sessions=limits.max_sessions,
        session_ttl_h=limits.session_ttl_h,
    )

    if command.type is CommandType.RESET:
        sessions.store.clear_override(conn, session.id)
        sessions.store.clear_history(conn, session.id)
        logger.info("Command reset: sender=%s session_no=%d", sender, session_no)
        return "Session %d reset" % session_no

    if command.type is CommandType.TEMP:
        try:
            value = float(command.arg)
        except ValueError:
            return USAGE_TEMP
        if not _TEMP_MIN <= value <= _TEMP_MAX:
            return USAGE_TEMP
        sessions.store.set_temperature(conn, session.id, value)
        logger.info(
            "Command temp: sender=%s session_no=%d value=%.2f", sender, session_no, value
        )
        return "Session %d temperature=%s" % (session_no, _format_temp(value))

    # CommandType.PROMPT - the last remaining branch.
    if not command.arg:
        return USAGE_PROMPT
    sessions.store.set_prompt(conn, session.id, command.arg)
    logger.info(
        "Command prompt: sender=%s session_no=%d prompt_len=%d",
        sender,
        session_no,
        len(command.arg),
    )
    return "Session %d prompt updated (%d chars)" % (session_no, len(command.arg))


def _format_temp(value: float) -> str:
    """Compact temperature formatting: 1.0 -> "1", 1.5 -> "1.5"."""
    return ("%g" % value)


def _format_freshness(updated_at: str) -> str:
    """Human freshness of a session: ``5m`` / ``3h`` / ``2d``."""
    delta = datetime.now(timezone.utc) - datetime.fromisoformat(updated_at)
    minutes = max(0, int(delta.total_seconds() // 60))
    if minutes < 60:
        return "%dm" % minutes
    hours = minutes // 60
    if hours < 48:
        return "%dh" % hours
    return "%dd" % (hours // 24)
