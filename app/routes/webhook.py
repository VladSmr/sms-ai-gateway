"""Webhook receiver: HMAC verification -> dedup -> enqueue -> immediate 2xx.

Pipeline (step 2 of the plan; no business logic here - the worker decides
what to do with the queued events):

1. Per-IP in-memory sliding-window rate limit -> 429 (before reading the
   body: streaming is the most expensive step, so the limit gates it).
2. Body size limit: Content-Length pre-check, then a streamed read with a
   hard byte cap (a lying Content-Length or a chunked flood is cut off at
   the cap instead of buffering unbounded memory) -> 413.
3. Signature verification (see :mod:`app.signing`; raw body + timestamp,
   HMAC-SHA256, constant-time compare, +-timestamp window) -> 401.
4. JSON + envelope validation (Pydantic, ``extra="allow"``) -> 400.
5. Dedup by message id (``processed_events``, TTL 48 h) -> 200 + skip.
6. Atomic enqueue (queue row + dedup tombstone, one transaction) -> 200.

The gateway requires a 2xx answer within 30 s and retries non-2xx (or timed
out) deliveries with exponential backoff (10 s, 20 s, ... default 14 retries
~= 45.5 h), so every request is answered immediately; 200 is returned both
for newly accepted events and for duplicates (the gateway must not retry a
deduplicated delivery).

Counters (UPSERT-maintained, visible via /healthz): ``received``,
``accepted``, ``dedup_skipped``, ``rejected_signature``,
``rejected_timestamp``, ``rejected_size``, ``rejected_payload``,
``rate_limited``. In a steady state ``received`` equals the sum of the rest.

Privacy (152-FZ): request bodies and message texts are NEVER logged - only
metadata (event name, ids, sender number, body length, verdict).
"""

import asyncio
import json
import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .. import db, signing
from ..events import EVENT_SMS_RECEIVED, WebhookEnvelope
from ..rate_limit import RateLimiter

logger = logging.getLogger(__name__)

router = APIRouter()


class ReceivedSmsPayload(BaseModel):
    """Minimal structural check for ``sms:received`` payloads.

    Only ``messageId`` (the gateway's content-based message id) is validated
    here - it keys the dedup tombstone and the queue. The rest of the payload
    (``message``, ``sender``, the deprecated ``phoneNumber``, ``receivedAt``,
    ...) is parsed by the worker in step 3; ``extra="allow"`` keeps forward
    compatibility with old (``phoneNumber``-only) and future gateway builds.
    """

    model_config = ConfigDict(extra="allow")

    messageId: str = Field(min_length=1)


def _client_ip(request: Request) -> str:
    """Best-effort client identity for the soft rate limit.

    In the production topology requests arrive through Caddy, which sets
    ``X-Forwarded-For`` to the real client address, so the first XFF entry is
    trusted. Direct (unproxied) clients fall back to the socket address.
    """
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client is not None:
        return request.client.host
    return "unknown"


def _rejected(status_code: int, reason: str) -> JSONResponse:
    """Build a rejection response (never echoes request data back)."""
    return JSONResponse(
        status_code=status_code,
        content={"status": "rejected", "reason": reason},
    )


@router.post("/webhook")
async def receive_webhook(request: Request) -> JSONResponse:
    """Accept an inbound SMS gateway webhook event."""
    state = request.app.state
    config = state.config_manager.get()
    conn = state.db_conn
    # M3: EVERY write on the shared receiver connection goes through this
    # lock. sqlite3 executes single statements atomically, but transactions
    # are connection-global: accept_message runs an explicit BEGIN/COMMIT,
    # while increment_counter uses an implicit one ("with conn") - if two
    # concurrent requests interleave those on the same connection, one of
    # them fails with OperationalError ("cannot start a transaction within
    # a transaction"). Serializing all receiver writes keeps each
    # transaction contiguous; the await inside each critical section keeps
    # the event loop responsive for other requests (they still stream their
    # bodies / verify signatures while this one holds SQLite).
    accept_lock: asyncio.Lock = state.accept_lock

    async def _counter(key: str) -> None:
        async with accept_lock:
            await asyncio.to_thread(db.increment_counter, conn, key)

    async with accept_lock:
        await asyncio.to_thread(db.increment_counter, conn, "received")

    # -- 1. Soft per-IP rate limit (before reading anything: body streaming
    #    is the most expensive step of the pipeline, so the limit must gate
    #    it; the counter arithmetic is unaffected - every rejected request
    #    still maps to exactly one counter besides "received") -------------
    limiter: RateLimiter = state.rate_limiter
    limit = config.webhook.rate_limit_per_min
    if limiter.max_per_minute != limit:
        # Pick up a hot-reloaded value without discarding tracked windows.
        limiter.update_limit(limit)
    if not limiter.allow(_client_ip(request)):
        await _counter("rate_limited")
        logger.info("Webhook rejected: reason=rate_limit")
        return _rejected(429, "rate_limited")

    # -- 2. Body size limit (before any parsing) ---------------------------
    max_body = config.webhook.max_body_bytes
    content_length = request.headers.get("content-length", "")
    try:
        declared = int(content_length) if content_length else None
    except ValueError:
        declared = None
    if declared is not None and declared > max_body:
        # Fast path: reject before reading the body at all.
        await _counter("rejected_size")
        logger.info("Webhook rejected: reason=size declared_length=%s", declared)
        return _rejected(413, "payload_too_large")

    # Stream the body under a hard byte cap: a lying Content-Length or a
    # chunked upload is cut off as soon as the limit is crossed, so an
    # attacker-controlled body can never buffer unbounded memory. The
    # connection is closed with the 413 response without draining the rest
    # of the body; the gateway treats the failure as retryable.
    chunks: list[bytes] = []
    total = 0
    oversized = False
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_body:
            oversized = True
            break
        chunks.append(chunk)
    if oversized:
        await _counter("rejected_size")
        logger.info("Webhook rejected: reason=size streamed_more_than=%d", max_body)
        return _rejected(413, "payload_too_large")
    body = b"".join(chunks)

    # -- 3. Signature verification -----------------------------------------
    timestamp = signing.parse_timestamp(request.headers.get(signing.HEADER_TIMESTAMP))
    if timestamp is None or not signing.is_timestamp_fresh(
        timestamp, config.webhook.timestamp_window_s
    ):
        await _counter("rejected_timestamp")
        logger.info("Webhook rejected: reason=timestamp")
        return _rejected(401, "invalid_timestamp")
    if not signing.verify_signature(
        config.webhook.secret, body, timestamp, request.headers.get(signing.HEADER_SIGNATURE)
    ):
        await _counter("rejected_signature")
        logger.info("Webhook rejected: reason=signature")
        return _rejected(401, "invalid_signature")

    # -- 4. Payload validation ---------------------------------------------
    try:
        raw_envelope = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        await _counter("rejected_payload")
        logger.info("Webhook rejected: reason=payload (not JSON)")
        return _rejected(400, "invalid_payload")
    if not isinstance(raw_envelope, dict):
        await _counter("rejected_payload")
        logger.info("Webhook rejected: reason=payload (not an object)")
        return _rejected(400, "invalid_payload")
    try:
        envelope = WebhookEnvelope.model_validate(raw_envelope)
    except ValidationError:
        await _counter("rejected_payload")
        logger.info("Webhook rejected: reason=payload (envelope schema)")
        return _rejected(400, "invalid_payload")

    if envelope.event == EVENT_SMS_RECEIVED:
        try:
            sms_payload = ReceivedSmsPayload.model_validate(envelope.payload)
        except ValidationError:
            await _counter("rejected_payload")
            logger.info(
                "Webhook rejected: reason=payload (sms:received schema) event_id=%s",
                envelope.id,
            )
            return _rejected(400, "invalid_payload")
        message_id = sms_payload.messageId
    else:
        # Non-SMS events (system:ping, app:started, delivery reports, batch
        # envelopes) carry no per-message id; the envelope id identifies the
        # delivery. The worker (step 3) interprets their semantics.
        message_id = envelope.id

    # -- 5+6. Dedup + atomic enqueue (queue row + tombstone, one commit) ----
    # M3: same lock as every other receiver write (see the header comment):
    # accept_message holds an explicit BEGIN..COMMIT, which must never
    # interleave with another request's implicit counter transaction.
    payload_json = body.decode("utf-8")
    async with state.accept_lock:
        row_id = await asyncio.to_thread(
            db.accept_message,
            conn,
            message_id,
            payload_json,
            ttl_hours=config.webhook.dedup_ttl_hours,
        )
    if row_id is None:
        # Duplicate delivery (or a concurrent duplicate lost the race):
        # answer 2xx so the gateway stops retrying.
        await _counter("dedup_skipped")
        logger.info(
            "Webhook duplicate skipped: event=%s message_id=%s", envelope.event, message_id
        )
        return JSONResponse(status_code=200, content={"status": "ok"})

    await _counter("accepted")
    sender = envelope.payload.get("sender") or envelope.payload.get("phoneNumber")
    logger.info(
        "Webhook accepted: event=%s message_id=%s sender=%s body_length=%d queue_id=%d",
        envelope.event,
        message_id,
        sender if isinstance(sender, str) else "unknown",
        len(body),
        row_id,
    )
    return JSONResponse(status_code=200, content={"status": "ok"})
