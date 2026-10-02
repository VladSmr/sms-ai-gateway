"""Step 3: strict gateway event models (app.events)."""

import pytest
from pydantic import ValidationError

from app.events import (
    EVENT_SMS_RECEIVED,
    SmsReceivedPayload,
    WebhookEnvelope,
    parse_envelope,
)


def test_sms_received_payload_full_shape():
    payload = SmsReceivedPayload.model_validate(
        {
            "messageId": "msg-1",
            "message": "hello",
            "sender": "+79001234567",
            "recipient": "+79005553535",
            "simNumber": 1,
            "receivedAt": "2026-09-16T12:00:00+03:00",
        }
    )
    assert payload.messageId == "msg-1"
    assert payload.message == "hello"
    assert payload.sender == "+79001234567"
    assert payload.recipient == "+79005553535"
    assert payload.simNumber == 1
    assert payload.receivedAt == "2026-09-16T12:00:00+03:00"


def test_sms_received_payload_requires_message_id_and_message():
    with pytest.raises(ValidationError):
        SmsReceivedPayload.model_validate({"message": "no id"})
    with pytest.raises(ValidationError):
        SmsReceivedPayload.model_validate({"messageId": "msg-1"})


def test_sms_received_payload_tolerates_unknown_fields():
    payload = SmsReceivedPayload.model_validate(
        {"messageId": "msg-1", "message": "hi", "futureField": {"a": 1}}
    )
    assert payload.messageId == "msg-1"


def test_sender_raw_prefers_sender_over_deprecated_phone_number():
    both = SmsReceivedPayload.model_validate(
        {"messageId": "m", "message": "x", "sender": "+79001234567", "phoneNumber": "89001234567"}
    )
    assert both.sender_raw() == "+79001234567"
    legacy = SmsReceivedPayload.model_validate(
        {"messageId": "m", "message": "x", "phoneNumber": "89001234567"}
    )
    assert legacy.sender_raw() == "89001234567"
    neither = SmsReceivedPayload.model_validate({"messageId": "m", "message": "x"})
    assert neither.sender_raw() == ""


def test_parse_envelope_roundtrip():
    raw = (
        '{"deviceId":"dev","event":"%s","id":"e1",'
        '"payload":{"messageId":"m1","message":"hi"},"webhookId":"w1"}'
        % EVENT_SMS_RECEIVED
    )
    envelope = parse_envelope(raw)
    assert isinstance(envelope, WebhookEnvelope)
    assert envelope.event == EVENT_SMS_RECEIVED
    assert envelope.id == "e1"
    assert envelope.payload["messageId"] == "m1"


def test_parse_envelope_rejects_broken_json_and_non_objects():
    with pytest.raises(ValueError):
        parse_envelope("{not json")
    with pytest.raises(ValueError):
        parse_envelope('"just a string"')
    with pytest.raises((ValueError, ValidationError)):
        parse_envelope('{"event":"","id":"e1","payload":{}}')
