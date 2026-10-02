"""Gateway event parsing (steps 3 and 5): strict models for webhook payloads.

The webhook receiver (step 2) validates the envelope tolerantly
(``extra="allow"``) and requires only ``payload.messageId``; the worker
applies the strict schema. The split is intentional: the receiver must keep
accepting future gateway fields, while the worker must not silently process
malformed payloads (those are counted as ``dropped_parse`` and removed from
the queue instead of retrying forever).

Envelope shape per docs.sms-gate.app (Webhooks):
``{"deviceId": str, "event": str, "id": str, "payload": object,
"webhookId": str}`` - camelCase, ``extra="allow"``.
"""

import json

from pydantic import BaseModel, ConfigDict, Field

EVENT_SMS_RECEIVED = "sms:received"
EVENT_SMS_DELIVERED = "sms:delivered"
EVENT_SMS_FAILED = "sms:failed"
EVENT_SYSTEM_PING = "system:ping"
EVENT_APP_STARTED = "app:started"

# Events with step-5 handling (heartbeat, device state, reply tracking).
# Anything else non-SMS is still skipped with a metadata log (step 3).
HANDLED_NON_SMS_EVENTS = frozenset(
    {EVENT_SMS_DELIVERED, EVENT_SMS_FAILED, EVENT_SYSTEM_PING, EVENT_APP_STARTED}
)
# Known-but-not-handled non-SMS event names (step 3 convention: skipped with
# INFO, metadata only).  These events arrive from the gateway/phone app
# but do not require any action from our pipeline (the Go server and phone
# manage their own delivery/sending logic).
KNOWN_NON_SMS_EVENTS = HANDLED_NON_SMS_EVENTS | frozenset(
    {
        "sms:sent",
        "sms:cancelled",
        "sms:data-received",
        "mms:received",
        "mms:downloaded",
    }
)


class WebhookEnvelope(BaseModel):
    """Gateway webhook envelope (moved here from the receiver in step 3).

    ``extra="allow"``: the gateway evolves; unknown fields must not break
    parsing. Semantic validation of payload contents is the worker's job.
    """

    model_config = ConfigDict(extra="allow")

    event: str = Field(min_length=1)
    id: str = Field(min_length=1)
    payload: dict
    deviceId: str | None = None
    webhookId: str | None = None


class SmsReceivedPayload(BaseModel):
    """Strict ``sms:received`` payload (docs.sms-gate.app, Events).

    ``messageId`` - the gateway's content-based id (dedup key of step 2);
    ``message`` - the SMS text (never logged, 152-FZ);
    ``sender`` - the current field name for the originating number;
    ``phoneNumber`` - the deprecated duplicate still serialized by the app
    (kept for backward compatibility with older gateway versions);
    ``recipient`` - the device's own number, ``None`` without
    READ_PHONE_STATE; ``simNumber`` - SIM slot index; ``receivedAt`` -
    ISO-8601 with the device's local offset.
    """

    model_config = ConfigDict(extra="allow")

    messageId: str = Field(min_length=1)
    message: str
    sender: str | None = None
    phoneNumber: str | None = None
    recipient: str | None = None
    simNumber: int | None = None
    receivedAt: str | None = None

    def sender_raw(self) -> str:
        """Best-effort raw sender value (either field, empty when absent)."""
        return self.sender or self.phoneNumber or ""


class SmsDeliveredPayload(BaseModel):
    """``sms:delivered`` payload (docs.sms-gate.app, Events).

    Arrives ONCE PER MULTIPART PART (there is no "part" field; dedup per
    part is the envelope id, already enforced by the receiver's
    ``processed_events`` tombstone). ``sender``/``recipient`` are nullable;
    PRIVACY: never logged - part counting only (152-FZ).
    """

    model_config = ConfigDict(extra="allow")

    messageId: str = Field(min_length=1)
    sender: str | None = None
    recipient: str | None = None
    simNumber: int | None = None
    deliveredAt: str | None = None


class SmsFailedPayload(BaseModel):
    """``sms:failed`` payload (docs.sms-gate.app, Events).

    Arrives once when any part was not delivered. PRIVACY: ``reason`` is
    device-provided free text and may contain phone data - never logged;
    only its length is recorded (152-FZ).
    """

    model_config = ConfigDict(extra="allow")

    messageId: str = Field(min_length=1)
    sender: str | None = None
    recipient: str | None = None
    simNumber: int | None = None
    reason: str | None = None
    failedAt: str | None = None


class SimCardInfo(BaseModel):
    """One SIM slot from the ``app:started`` payload."""

    model_config = ConfigDict(extra="allow")

    slotIndex: int | None = None
    simNumber: int | None = None
    phoneNumber: str | None = None
    carrierName: str | None = None
    iccid: str | None = None


class AppStartedPayload(BaseModel):
    """``app:started`` payload: the phone app (re)started - device came up."""

    model_config = ConfigDict(extra="allow")

    simCards: list[SimCardInfo] = Field(default_factory=list)


class SystemPingPayload(BaseModel):
    """``system:ping`` payload: device heartbeat with a health summary.

    PRIVACY: health details are device-internal; only a coarse, non-identifying
    summary (keys present, no values) is ever stored/logged (152-FZ).
    """

    model_config = ConfigDict(extra="allow")

    health: dict = Field(default_factory=dict)


def parse_envelope(raw: str) -> WebhookEnvelope:
    """Parse a queued raw JSON body into the envelope model.

    Raises ``json.JSONDecodeError``/``UnicodeDecodeError`` on broken JSON and
    ``pydantic.ValidationError`` on a schema-violating envelope.
    """
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("envelope is not a JSON object")
    return WebhookEnvelope.model_validate(value)
