"""Message pipeline (step 5): whitelist -> routing -> commands/LLM -> send.

Runs synchronously inside the worker thread (the worker wraps it with
``asyncio.to_thread``). Each queued row is a raw JSON webhook body saved by
the receiver (step 2).

Processing order per row:

1. Parse the envelope (``app.events.parse_envelope``). Broken JSON or a
   schema-violating envelope -> ``dropped_parse`` counter + metadata log;
   the row is NOT retried (the caller removes it as processed).
2. Event dispatch:

   - ``sms:received`` -> the full SMS flow (below);
   - ``sms:delivered`` -> per-part delivery confirmation: ``delivered_parts``
     counter + metadata log (dedup is the receiver's envelope-id tombstone -
     multipart parts arrive as distinct events);
   - ``sms:failed`` -> delivery-failure report: log-only (the device may
     retry on its own; we never re-notify the user), ``failed_reports``
     counter; the free-text ``reason`` is NEVER logged (152-FZ), only its
     length;
   - ``system:ping`` -> heartbeat: refreshes ``device_state['last_ping']``,
     clears the watchdog alert flag (one "recovered" alert), ``heartbeats``
     counter; only a non-identifying health summary (key count) is stored;
   - ``app:started`` -> device came up: ``device_state['last_app_started']``,
     Telegram alert, ``app_restarts`` counter (SIM numbers/ICCID never
     logged);
   - any other non-SMS event -> ``skipped_event`` (INFO for known names,
     WARNING for unknown ones).
3. Strict ``sms:received`` payload validation; violations -> ``dropped_parse``.
4. Sender normalization (``app.sessions.normalizer``) + whitelist check.
   Non-whitelisted senders - numeric AND alphanumeric - are silently
   dropped: no reply, no error log (this is normal operation, not a
   failure), only the ``dropped_whitelist`` counter and a metadata log.
5. Routing: ``#N <text>`` selects session N (created on demand); no prefix
   -> the default session (``sessions.store.DEFAULT_SESSION_NO``). An
   invalid prefix (``#0``, a bare ``#2`` without text) is not a route: the
   whole text goes to the default session as ordinary content.
6. A command (case-insensitive, before any LLM call) is executed against
   the session store; otherwise the text is appended to the session
   history and the GigaChat client produces the reply:

   - the messages array is ``[system, ...history]`` (the server keeps no
     context), the system prompt is the session override when set, the
     YAML default otherwise; the temperature likewise (override wins);
   - mock mode (``MOCK_LLM=true`` env or ``llm.mock: true`` config, env
     wins, re-resolved on EVERY call - hot-reload flips it live) replaces
     the client with an echo stub ``(mock) <text>`` - zero network, no
     keys needed (pipeline smoke tests);
   - a final client failure (retries exhausted, unrecoverable status)
     becomes a Russian user-facing error reply (ARCHITECTURE.md section 7)
     plus the ``llm_errors`` counter - the row is still processed, not
     retried;
   - a successful completion's reply is appended to the session history
     as an ``assistant`` message (server-side context = none, client-side
     = the full history next time).
7. The reply (command output or LLM completion) is trimmed to
   ``limits.max_reply_units`` UTF-16 units (sentence boundaries preferred,
   see ``app.gateway.trim``) and sent to the phone via the gateway client
   (Private Server queue, Basic auth, persistent ``send_id`` from queue row,
   ``withDeliveryReport``). Mock mode (``MOCK_GATEWAY=true`` env or
   ``gateway.mock: true``, env wins, re-resolved per call) logs the send.
   A final send failure is handled per W5: a retryable transport/5xx
   failure requeues the row ONCE (deferred re-send: the trimmed reply text
   is stored in ``queue.deferred_reply``, schema v6, and replayed verbatim
   on the next claim - no second LLM call, no command re-execution,
   idempotent via the persistent send_id); a second final failure - and any
   non-retryable 4xx - is counted (``sms_send_failed``) and logged with
   metadata only (best-effort semantics, README "Known limitations").

PRIVACY (152-FZ): message text, prompt text and model replies are NEVER
logged - metadata only (sender, lengths, session number, event name,
command type, ids). Phone numbers appear only as short hashes.
"""

import logging
import os
import re
import sqlite3

from pydantic import ValidationError

from . import commands, db, sessions
from .config import ConfigManager
from .events import (
    EVENT_APP_STARTED,
    EVENT_SMS_DELIVERED,
    EVENT_SMS_FAILED,
    EVENT_SMS_RECEIVED,
    EVENT_SYSTEM_PING,
    HANDLED_NON_SMS_EVENTS,
    KNOWN_NON_SMS_EVENTS,
    AppStartedPayload,
    SmsDeliveredPayload,
    SmsFailedPayload,
    SmsReceivedPayload,
    SystemPingPayload,
    parse_envelope,
)
from .gateway import GatewayError, make_gateway_client, trim_reply
from .gateway.client import MockGatewayClient, env_mock_enabled as gateway_mock_enabled
from .llm.gigachat import GigaChatClient, GigaChatError, env_mock_enabled
from .notify import TelegramNotifier
from .notify import watchdog as watchdog_mod

logger = logging.getLogger(__name__)

# "#2 text" -> session 2; requires text after the prefix (a bare "#2" is
# treated as ordinary text for the default session). DOTALL keeps multiline
# messages intact after the prefix. The session number is capped at 4 digits:
# a monster like "#999999999999 text" must not become an int of arbitrary
# size (DoS-by-parse guard); 4 digits (в‰¤ #9999) is far beyond any real
# per-sender session count (limits.max_sessions caps at 10 by default).
_ROUTE_PATTERN = re.compile(r"^#(\d{1,4})\s+(.+)$", re.DOTALL)

# User-facing final-failure reply (ARCHITECTURE.md section 7 wins over
# PLAN.md's English wording: the audience is a Russian SMS user).
LLM_FAILURE_REPLY = "Ошибка, попробуйте позже"


class ReplySendDeferred(Exception):
    """The reply was produced but the gateway send failed with a retryable
    (transport/5xx) error; the row must be requeued for exactly one deferred
    re-send attempt (W5). NOT an unexpected error - the worker catches it
    explicitly and requeues the row WITHOUT the dead-letter counter noise
    (the attempt budget bump is done by the pipeline itself)."""

    def __init__(self, reply: str) -> None:
        super().__init__("reply send deferred")
        self.reply = reply


class MockLLMClient:
    """Echo stub for smoke/pipeline runs: ``(mock) <last user message>``.

    Zero network, no keys needed; shares the ``complete`` signature with
    :class:`GigaChatClient`.
    """

    def complete(
        self,
        messages: list[dict[str, str]],
        *,
        temperature: float,
        max_tokens: int,
    ) -> str:
        last_user = "(no user message)"
        for message in reversed(messages):
            if message.get("role") == "user":
                last_user = message.get("content", "")
                break
        return "(mock) " + last_user


# Shared immutable mock instances (stateless; resolved fresh per call, W4a).
# Defined after both classes (the LLM stub lives in this module).
_MOCK_LLM_CLIENT = MockLLMClient()
_MOCK_GATEWAY_CLIENT = MockGatewayClient()


class MessagePipeline:
    """Worker-facing pipeline; one instance per application."""

    def __init__(
        self,
        conn: sqlite3.Connection,
        config_manager: ConfigManager,
        llm_client: GigaChatClient | None = None,
        gateway_client: object | None = None,
        notifier: TelegramNotifier | None = None,
    ) -> None:
        """``llm_client``/``gateway_client``/``notifier`` are injection
        points for tests; production resolves the REAL clients here and the
        MOCK flags on every use (W4a): the ``MOCK_LLM``/``MOCK_GATEWAY``
        env vars and the YAML ``llm.mock``/``gateway.mock`` flags are
        re-resolved per call, so a hot-reloaded config flips mock mode
        without a restart. The Telegram notifier is rebuilt from the fresh
        ``notify.*`` settings on every alert (hot-reloadable credentials);
        unconfigured it degrades to log-only alerts."""
        self._conn = conn
        self._config_manager = config_manager
        self._whitelist_cache_key: int | None = None
        self._whitelist_cache: frozenset[str] = frozenset()
        self._llm_override = llm_client
        self._gateway_override = gateway_client
        self._notifier_override = notifier
        # Nit: declared up front so every code path sees a defined value.
        self._current_send_id: str | None = None

    # ------------------------------------------------------------------
    # Client resolution (fresh per use, W4a)
    # ------------------------------------------------------------------

    def _resolve_llm_client(self):
        """LLM client for this call: injected override wins, otherwise the
        mock decision is re-made from the CURRENT env + config (hot-reload
        flips ``llm.mock``/``MOCK_LLM`` without a restart)."""
        if self._llm_override is not None:
            return self._llm_override
        if env_mock_enabled(os.environ.get) or self._config_manager.get().llm.mock:
            return _MOCK_LLM_CLIENT
        return GigaChatClient(self._config_manager)

    def _resolve_gateway_client(self):
        """Gateway client for this call (same semantics as the LLM one)."""
        if self._gateway_override is not None:
            return self._gateway_override
        if gateway_mock_enabled(os.environ.get) or self._config_manager.get().gateway.mock:
            return _MOCK_GATEWAY_CLIENT
        return make_gateway_client(self._config_manager)

    def _resolve_notifier(self) -> TelegramNotifier:
        """Telegram notifier rebuilt from the CURRENT config (hot-reloadable
        credentials, W4a); the injected test override always wins."""
        if self._notifier_override is not None:
            return self._notifier_override
        config = self._config_manager.get()
        return TelegramNotifier(
            bot_token=config.notify.telegram_token,
            chat_id=config.notify.telegram_chat_id,
        )

    @property
    def notifier(self) -> TelegramNotifier:
        """Current notifier (mostly for tests/e2e assertions)."""
        return self._resolve_notifier()

    # ------------------------------------------------------------------
    # Worker entry points (called via asyncio.to_thread)
    # ------------------------------------------------------------------

    def process(self, row: dict) -> str:
        """Process one claimed queue row; returns the reply text.

        Returns an empty string when the row produces no reply (drop/skip
        cases). Expected data errors are fully absorbed here (counted and
        logged with metadata) so the worker deletes the row instead of
        retrying it forever; unexpected internal errors still propagate and
        keep the existing worker retry semantics (with the v5 dead-letter
        cap so a poisoned row cannot block the FIFO).

        The row dict carries ``send_id`` (v4+ schema) from the worker's
        batch SELECT; the pipeline stamps it persistently on the queue row
        before processing so that a requeue keeps the same idempotency key.
        """
        config = self._config_manager.get()

        # Stamp persistent send_id on the queue row (v4+); the worker requeues
        # with the same row id, preserving the id across retries.
        send_id: str | None = row.get("send_id")
        if send_id is None:
            import uuid as _uuid
            send_id = str(_uuid.uuid4())
            db.update_queue_send_id(self._conn, row["id"], send_id)
        self._current_send_id = send_id
        self._current_queue_id = row["id"]
        # W5: a deferred row carries the exact reply text to re-send; the
        # marker also identifies this pass as the re-send attempt (so a
        # second final failure is absorbed instead of deferring again).
        self._send_retried = bool(row.get("deferred_reply"))

        try:
            envelope = parse_envelope(row["payload"])
        except (ValueError, ValidationError):
            # json.JSONDecodeError/UnicodeDecodeError are ValueError.
            db.increment_counter(self._conn, "dropped_parse")
            logger.info(
                "Dropped: reason=parse event_id=%s payload_len=%d",
                row["message_id"],
                len(row["payload"]),
            )
            return ""

        if envelope.event != EVENT_SMS_RECEIVED:
            return self._process_notification(envelope)

        try:
            payload = SmsReceivedPayload.model_validate(envelope.payload)
        except ValidationError:
            db.increment_counter(self._conn, "dropped_parse")
            logger.info(
                "Dropped: reason=payload_schema event_id=%s payload_len=%d",
                envelope.id,
                len(row["payload"]),
            )
            return ""

        sender = sessions.normalizer.normalize_sender(payload.sender_raw())
        whitelist = self._whitelist_set()
        if sender is None or sender not in whitelist:
            # Silent drop: no reply, INFO-level metadata only (normal
            # operation per ARCHITECTURE.md section 10, not an error).
            db.increment_counter(self._conn, "dropped_whitelist")
            logger.info(
                "Dropped: reason=whitelist sender=%s text_len=%d event_id=%s",
                sender if sender is not None else "non-numeric",
                len(payload.message),
                payload.messageId,
            )
            return ""

        session_no, text = self._route(text=payload.message)
        limits = config.limits

        # W5 deferred re-send: this row's reply was produced but the send
        # failed with a retryable error; the trimmed reply text travels in
        # ``deferred_reply`` (v6). Replay it verbatim - the attempt counter
        # (W2) must NEVER select this branch: a row requeued after a generic
        # crash, or deferred on a command (commands write no assistant
        # history), must be processed NORMALLY, not replayed from history.
        deferred_reply = (row.get("deferred_reply") or "").strip()
        if deferred_reply:
            logger.info(
                "Replaying deferred send: queue_id=%d message_id=%s "
                "reply_len=%d sender=%s",
                row["id"],
                row["message_id"],
                len(deferred_reply),
                sender,
            )
            sent = self._send_reply(
                deferred_reply, sender, send_id=self._current_send_id
            )
            db.increment_counter(self._conn, "processed")
            logger.info(
                "Replay processed: sender=%s session_no=%d sent_len=%d "
                "message_id=%s",
                sender,
                session_no,
                len(sent),
                payload.messageId,
            )
            return sent

        command = commands.handler.parse_command(text)
        if command is not None:
            reply = commands.handler.execute_command(
                self._conn,
                command,
                sender=sender,
                route_session_no=session_no,
                config=config,
            )
            try:
                sent = self._send_reply(reply, sender, send_id=self._current_send_id)
            except ReplySendDeferred as exc:
                # W5: store the trimmed reply as the deferred marker (v6) and
                # re-raise so the worker requeues the row instead of counting
                # it completed and deleting it. The `processed` counter is
                # only bumped on a terminal outcome below.
                self._defer_row(row, exc.reply)
                raise
            db.increment_counter(self._conn, "processed")
            logger.info(
                "Processed: sender=%s session_no=%d text_len=%d command=%s "
                "reply_len=%d sent_len=%d message_id=%s",
                sender,
                session_no,
                len(text),
                command.type.name,
                len(reply),
                len(sent),
                payload.messageId,
            )
            return sent

        session = sessions.store.get_or_create(
            self._conn,
            sender,
            session_no,
            max_sessions=limits.max_sessions,
            session_ttl_h=limits.session_ttl_h,
        )
        sessions.store.append_message(
            self._conn, session.id, "user", text, history_messages=limits.history_messages
        )
        reply = self._llm_reply(session, text, config)
        try:
            sent = self._send_reply(reply, sender, send_id=self._current_send_id)
        except ReplySendDeferred as exc:
            # W5: same as the command branch - store the deferred marker and
            # re-raise; the worker requeues the row (NOT completed).
            self._defer_row(row, exc.reply)
            raise
        db.increment_counter(self._conn, "processed")
        logger.info(
            "Processed: sender=%s session_no=%d text_len=%d command=None "
            "reply_len=%d sent_len=%d message_id=%s",
            sender,
            session_no,
            len(text),
            len(reply),
            len(sent),
            payload.messageId,
        )
        return sent

    def _defer_row(self, row: dict, reply_text: str) -> None:
        """Mark a row for the deferred re-send (W5, schema v6).

        The trimmed reply text is written to ``queue.deferred_reply`` and the
        attempt budget is bumped by exactly one in the same committed write
        (:func:`db.defer_queue_row`), keeping the persistent ``send_id``: the
        gateway deduplicates on it, so the re-send cannot double-deliver. On
        the next claim the pipeline replays EXACTLY this text - never the
        session history (a command produces no assistant message, and a row
        requeued after a generic crash must be processed normally, not
        replayed). A second final failure is absorbed (best-effort, README
        "Known limitations").
        """
        db.defer_queue_row(self._conn, row["id"], reply_text)
        logger.info(
            "Reply deferred: queue_id=%d message_id=%s reply_len=%d "
            "(one re-send budgeted)",
            row["id"],
            row["message_id"],
            len(reply_text),
        )

    def watchdog_tick(self) -> None:
        """Heartbeat watchdog tick on the FAST worker cadence (W1 fix).

        Runs every ``worker.watchdog_interval_s`` (default 5 min) instead of
        the old daily sweep, so a dead phone is alerted within ~one alert
        threshold (``3 x notify.heartbeat_interval_min``) of the actual
        silence instead of up to a day later.
        """
        watchdog_mod.check_heartbeat(
            self._conn, self._config_manager.get(), self._resolve_notifier()
        )

    def sweep(self) -> int:
        """Periodic maintenance: expire sessions (daily cadence, W1 split).

        The heartbeat watchdog was moved to its own fast tick
        (:meth:`watchdog_tick`); a sweep failure is absorbed by the worker's
        sweep guard.
        """
        removed = sessions.store.sweep_expired(self._conn)
        return removed

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _whitelist_set(self) -> frozenset[str]:
        """Whitelist as a set, cached and invalidated on config reloads."""
        config = self._config_manager.get()
        identity = id(config)
        if self._whitelist_cache_key != identity:
            self._whitelist_cache = frozenset(config.whitelist)
            self._whitelist_cache_key = identity
        return self._whitelist_cache

    # ------------------------------------------------------------------
    # Non-SMS events (step 5)
    # ------------------------------------------------------------------

    def _process_notification(self, envelope) -> str:
        """Dispatch a non-SMS event; returns "" (no reply, row removed).

        Malformed payloads of KNOWN events are counted as ``dropped_parse``
        (same as SMS): the queue must not retry them forever. UNKNOWN event
        names keep the step-3 semantics: ``skipped_event`` (WARNING).
        """
        event = envelope.event
        if event not in HANDLED_NON_SMS_EVENTS:
            db.increment_counter(self._conn, "skipped_event")
            level = logging.INFO if event in KNOWN_NON_SMS_EVENTS else logging.WARNING
            logger.log(
                level,
                "Skipped event: event=%s event_id=%s payload_len=%d",
                event,
                envelope.id,
                len(str(envelope.payload)),
            )
            return ""
        try:
            if event == EVENT_SMS_DELIVERED:
                payload = SmsDeliveredPayload.model_validate(envelope.payload)
            elif event == EVENT_SMS_FAILED:
                payload = SmsFailedPayload.model_validate(envelope.payload)
            elif event == EVENT_SYSTEM_PING:
                payload = SystemPingPayload.model_validate(envelope.payload)
            else:
                payload = AppStartedPayload.model_validate(envelope.payload)
        except ValidationError:
            db.increment_counter(self._conn, "dropped_parse")
            logger.info(
                "Dropped: reason=payload_schema event=%s event_id=%s",
                event,
                envelope.id,
            )
            return ""

        if event == EVENT_SMS_DELIVERED:
            # One event per multipart part; per-part dedup is the receiver's
            # envelope-id tombstone, so every row here is a distinct part.
            db.increment_counter(self._conn, "delivered_parts")
            logger.info(
                "Delivery confirmed: message_id=%s event_id=%s sim=%s",
                payload.messageId,
                envelope.id,
                payload.simNumber,
            )
        elif event == EVENT_SMS_FAILED:
            # Log-only by design: the device handles its own retries; the
            # failure surfaces in metrics, never as a new user notification.
            # PRIVACY: reason text is device-provided free text - length only.
            db.increment_counter(self._conn, "failed_reports")
            logger.info(
                "Delivery failed: message_id=%s event_id=%s reason_len=%s",
                payload.messageId,
                envelope.id,
                len(payload.reason) if payload.reason else 0,
            )
        elif event == EVENT_SYSTEM_PING:
            recovered = watchdog_mod.record_heartbeat(self._conn)
            db.increment_counter(self._conn, "heartbeats")
            if recovered == "recovered":
                self._resolve_notifier().send_alert(
                    "SMS gateway: device heartbeat recovered"
                )
            # PRIVACY: health values are device-internal; store/log the key
            # count only.
            db.set_device_state(
                self._conn, "last_ping_health_keys", str(len(payload.health))
            )
            logger.info(
                "Heartbeat recorded: event_id=%s health_keys=%d",
                envelope.id,
                len(payload.health),
            )
        else:  # EVENT_APP_STARTED
            db.set_device_state(self._conn, "last_app_started", db.utcnow_iso())
            db.increment_counter(self._conn, "app_restarts")
            # PRIVACY: simCards carry phone numbers/ICCID - count only.
            self._resolve_notifier().send_alert(
                "SMS gateway: phone app (re)started, sims=%d" % len(payload.simCards)
            )
            logger.info(
                "Device app started: event_id=%s sims=%d",
                envelope.id,
                len(payload.simCards),
            )
        return ""

    def _send_reply(self, reply: str, sender: str, send_id: str | None = None) -> str:
        """Trim the reply to the SMS budget and send it via the gateway.

        The trimmed text is what the user receives; the session history
        keeps the FULL reply (done by the caller/LLM step).

        W5 (silent reply loss): a final :class:`GatewayError` used to drop
        the reply forever - a 30 s Go-server restart outlived the 1.5 s
        client backoff budget. The row is now requeued for EXACTLY ONE
        deferred retry (the worker's own poll cadence serves as the
        cooldown), and only a second final failure abandons the reply
        (best-effort documented in README "Known limitations"). The
        persistent ``send_id`` (v4+) keeps the retry idempotent, so no
        double SMS is possible. A 4xx is NOT retried (auth/body errors do
        not heal by waiting): only transport/5xx-style failures requeue.

        ``send_id`` (v4+) is the persistent idempotency key stamped on the
        queue row before processing. The caller passes it from
        ``self._current_send_id`` so the same id is used across retries.
        """
        config = self._config_manager.get()
        trimmed = trim_reply(reply, max_units=config.limits.max_reply_units)
        try:
            gateway_id = self._resolve_gateway_client().send(
                text=trimmed, phone_number=sender, send_id=send_id
            )
        except GatewayError as exc:
            already_retried = getattr(self, "_send_retried", False)
            retryable = exc.status is None or exc.status >= 500
            if retryable and not already_retried:
                # One deferred retry: requeue the row so the reply is not
                # lost to a transient gateway outage (server restart etc.).
                db.increment_counter(self._conn, "sms_send_requeued")
                logger.info(
                    "Gateway send deferred for retry: status=%s sender=%s "
                    "sent_len=%d queue_id=%s",
                    exc.status if exc.status is not None else -1,
                    sender,
                    len(trimmed),
                    getattr(self, "_current_queue_id", None),
                )
                raise ReplySendDeferred(trimmed) from exc
            db.increment_counter(self._conn, "sms_send_failed")
            logger.info(
                "Gateway send failed: status=%s sender=%s sent_len=%d",
                exc.status if exc.status is not None else -1,
                sender,
                len(trimmed),
            )
            return trimmed
        db.increment_counter(self._conn, "sms_sent")
        logger.info(
            "Reply sent: gateway_id=%s sender=%s sent_len=%d",
            gateway_id,
            sender,
            len(trimmed),
        )
        return trimmed

    def _llm_reply(self, session: "sessions.store.Session", text: str, config) -> str:
        """Build messages, call the LLM (or the mock), record the reply.

        The user message is already in the history; the assistant reply is
        appended after a successful completion so the next request carries
        full context. A final client failure is absorbed into a fixed
        Russian error reply (counted as ``llm_errors``). The client (real
        or mock) is re-resolved from the current config on every call (W4a:
        hot-reload flips ``llm.mock`` without a restart).
        """
        llm_client = self._resolve_llm_client()
        effective_temp = (
            session.override_temp
            if session.override_temp is not None
            else config.llm.temperature
        )
        system_prompt = (
            session.override_prompt
            if session.override_prompt is not None
            else config.system_prompt
        )
        history = sessions.store.get_history(self._conn, session.id)
        messages = [{"role": "system", "content": system_prompt}] + history

        if isinstance(llm_client, MockLLMClient):
            reply = llm_client.complete(
                messages,
                temperature=effective_temp,
                max_tokens=config.llm.max_tokens,
            )
        else:
            db.increment_counter(self._conn, "llm_calls")
            try:
                reply = llm_client.complete(
                    messages,
                    temperature=effective_temp,
                    max_tokens=config.llm.max_tokens,
                )
            except GigaChatError:
                db.increment_counter(self._conn, "llm_errors")
                logger.info(
                    "LLM failed: session_id=%d history_len=%d reply=error",
                    session.id,
                    len(history),
                )
                return LLM_FAILURE_REPLY
        sessions.store.append_message(
            self._conn,
            session.id,
            "assistant",
            reply,
            history_messages=config.limits.history_messages,
        )
        return reply

    @staticmethod
    def _route(*, text: str) -> tuple[int, str]:
        """Extract the ``#N`` routing prefix; fall back to the default."""
        match = _ROUTE_PATTERN.match(text.strip())
        if match is not None and int(match.group(1)) >= 1:
            return int(match.group(1)), match.group(2).strip()
        return sessions.store.DEFAULT_SESSION_NO, text.strip()
