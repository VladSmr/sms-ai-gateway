"""Step 3: command parsing and execution (RESET/LIST/TEMP/PROMPT/HELP)."""

import copy

import pytest

from app import db
from app.commands import handler
from app.config import AppConfig
from tests.conftest import VALID_CONFIG

SENDER = "+79001234567"


@pytest.fixture
def conn():
    connection = db.open_db(":memory:")
    db.run_migrations(connection)
    yield connection
    connection.close()


@pytest.fixture
def config():
    return AppConfig.model_validate(copy.deepcopy(VALID_CONFIG))


def _execute(conn, config, text, *, route_session_no=1):
    command = handler.parse_command(text)
    assert command is not None, "expected %r to parse as a command" % text
    return handler.execute_command(
        conn, command, sender=SENDER, route_session_no=route_session_no, config=config
    )


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected_type", "expected_arg"),
    [
        ("RESET", handler.CommandType.RESET, ""),
        ("reset", handler.CommandType.RESET, ""),
        ("Reset", handler.CommandType.RESET, ""),
        ("  RESET 2  ", handler.CommandType.RESET, "2"),
        ("RESET 3", handler.CommandType.RESET, "3"),
        ("LIST", handler.CommandType.LIST, ""),
        ("list", handler.CommandType.LIST, ""),
        ("TEMP 1.5", handler.CommandType.TEMP, "1.5"),
        ("temp 0", handler.CommandType.TEMP, "0"),
        ("Temp 2", handler.CommandType.TEMP, "2"),
        ("PROMPT be brief", handler.CommandType.PROMPT, "be brief"),
        ("prompt   spaced   text ", handler.CommandType.PROMPT, "spaced   text"),
        ("HELP", handler.CommandType.HELP, ""),
        ("help", handler.CommandType.HELP, ""),
    ],
)
def test_parse_commands_case_insensitive(text, expected_type, expected_arg):
    command = handler.parse_command(text)
    assert command is not None
    assert command.type is expected_type
    assert command.arg == expected_arg


@pytest.mark.parametrize(
    "text",
    [
        "hello there",  # ordinary text
        "",  # empty
        "   ",  # whitespace only
        "resetting the session",  # not the keyword
        "temporary value",  # prefix of the keyword
        " temperatures",  # leading space changes the head word
    ],
)
def test_parse_rejects_non_commands(text):
    assert handler.parse_command(text) is None


# ---------------------------------------------------------------------------
# TEMP boundaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["0", "0.0", "2", "2.0", "1.5", "0.01", "1.999"])
def test_temp_accepts_bounds(conn, config, value):
    reply = _execute(conn, config, "TEMP %s" % value)
    assert reply.startswith("Session 1 temperature=")
    session = handler.sessions.store.get_or_create(
        conn, SENDER, 1, max_sessions=10, session_ttl_h=24
    )
    assert session.override_temp == float(value)


@pytest.mark.parametrize("value", ["-1", "2.1", "3", "abc", "", "1.5.6", "NaN"])
def test_temp_rejects_invalid_values_with_usage_hint(conn, config, value):
    reply = _execute(conn, config, "TEMP %s" % value)
    assert reply == handler.USAGE_TEMP


# ---------------------------------------------------------------------------
# RESET
# ---------------------------------------------------------------------------


def test_reset_clears_history_and_overrides(conn, config):
    _execute(conn, config, "TEMP 1.5")
    _execute(conn, config, "PROMPT be terse")
    session = handler.sessions.store.get_or_create(
        conn, SENDER, 1, max_sessions=10, session_ttl_h=24
    )
    handler.sessions.store.append_message(
        conn, session.id, "user", "some text", history_messages=10
    )
    assert session.override_temp == 1.5

    reply = _execute(conn, config, "RESET")
    assert reply == "Session 1 reset"

    reloaded = handler.sessions.store.get_or_create(
        conn, SENDER, 1, max_sessions=10, session_ttl_h=24
    )
    assert reloaded.override_temp is None
    assert reloaded.override_prompt is None
    assert handler.sessions.store.get_history(conn, reloaded.id) == []


def test_reset_with_argument_targets_that_session(conn, config):
    # Set an override on session 2 (via routing) and reset it by argument.
    _execute(conn, config, "TEMP 1.5", route_session_no=2)
    reply = _execute(conn, config, "RESET 2", route_session_no=1)
    assert reply == "Session 2 reset"
    session2 = handler.sessions.store.get_or_create(
        conn, SENDER, 2, max_sessions=10, session_ttl_h=24
    )
    assert session2.override_temp is None


@pytest.mark.parametrize("arg", ["abc", "0", "-1", "1.5"])
def test_reset_with_invalid_argument_returns_usage(conn, config, arg):
    assert _execute(conn, config, "RESET %s" % arg) == handler.USAGE_RESET


# ---------------------------------------------------------------------------
# PROMPT
# ---------------------------------------------------------------------------


def test_prompt_sets_override_and_reports_length_only(conn, config):
    secret = "you are a helpful pirate assistant"
    reply = _execute(conn, config, "PROMPT %s" % secret)
    assert reply == "Session 1 prompt updated (%d chars)" % len(secret)
    assert secret not in reply  # the text itself never goes back into an SMS
    session = handler.sessions.store.get_or_create(
        conn, SENDER, 1, max_sessions=10, session_ttl_h=24
    )
    assert session.override_prompt == secret


def test_prompt_without_text_returns_usage(conn, config):
    assert _execute(conn, config, "PROMPT") == handler.USAGE_PROMPT
    assert _execute(conn, config, "PROMPT   ") == handler.USAGE_PROMPT


# ---------------------------------------------------------------------------
# LIST / HELP
# ---------------------------------------------------------------------------


def test_list_empty(conn, config):
    assert _execute(conn, config, "LIST") == "No active sessions."


def test_list_shows_names_from_config(conn, config):
    store = handler.sessions.store
    store.get_or_create(conn, SENDER, 2, max_sessions=10, session_ttl_h=24)
    store.get_or_create(conn, SENDER, 5, max_sessions=10, session_ttl_h=24)
    _execute(conn, config, "TEMP 1.0")  # creates the named default session 1
    reply = _execute(conn, config, "LIST")
    # Session 1 has a name from sessions_named ({1: default}); unnamed
    # sessions are listed by number and freshness only.
    assert "Sessions: " in reply
    assert "#1 default" in reply
    assert "#2 " in reply
    assert "#5" in reply
    assert "work" not in reply  # not in VALID_CONFIG.sessions_named


def test_list_does_not_count_commands_as_sessions(conn, config):
    _execute(conn, config, "TEMP 1.0")  # creates session 1
    _execute(conn, config, "LIST")


def test_help_is_short_and_complete(conn, config):
    reply = _execute(conn, config, "HELP")
    for keyword in ("RESET", "LIST", "TEMP", "PROMPT", "#N"):
        assert keyword in reply
    # The reply becomes an SMS: must fit the 201 UTF-16 code unit budget.
    assert len(reply.encode("utf-16-le")) // 2 <= 201
