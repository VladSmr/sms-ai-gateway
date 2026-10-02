"""Webhook signature verification (Android SMS Gateway protocol).

The exact scheme was verified against the gateway sources and official
documentation (docs.sms-gate.app, "Webhooks" / "Payload signing"; reference
implementation github.com/android-sms-gateway/example-webhooks-fastapi;
signing plugin app/.../webhooks/plugins/PayloadSingingPlugin.kt):

- Headers: ``X-Signature`` carries the HMAC as lowercase hex (the gateway
  formats every byte with ``%02x``); ``X-Timestamp`` carries Unix time in
  SECONDS as an ASCII decimal string.
- Canonical input: the RAW request body bytes (before any JSON decoding)
  concatenated with the ASCII timestamp bytes, with NO separator, no method
  or path involved:
      expected = HMAC_SHA256(key=secret, msg=body + b"<timestamp>").hexdigest()
- Comparison is constant-time (:func:`hmac.compare_digest`).
- Replay protection: the timestamp must be within +-``window_s`` seconds of
  the receiver's clock. The official example does not enforce a window; the
  documentation explicitly recommends +-5 minutes, which is our default.

The signing key is the gateway's "Signing Key" setting (separate from the
API credentials); it is provided to this service through the
``webhook.secret`` config value, typically ``${GATEWAY_HMAC_SECRET}``.
"""

import hashlib
import hmac
import time

HEADER_SIGNATURE = "X-Signature"
HEADER_TIMESTAMP = "X-Timestamp"

DEFAULT_TIMESTAMP_WINDOW_S = 300  # docs recommendation: accept +-5 minutes


def compute_signature(secret: str, body: bytes, timestamp: int) -> str:
    """Return the expected signature for a request (lowercase hex).

    The canonical input is the raw body followed by the ASCII timestamp,
    concatenated without any separator - exactly what the gateway's
    ``PayloadSingingPlugin`` signs.
    """
    mac = hmac.new(
        secret.encode("utf-8"),
        body + str(timestamp).encode("ascii"),
        hashlib.sha256,
    )
    return mac.hexdigest()


def parse_timestamp(value: str | None) -> int | None:
    """Parse the ``X-Timestamp`` header (unix seconds).

    Returns ``None`` when the header is missing, empty or not a plain
    non-negative ASCII decimal (e.g. millisecond values or garbled input).
    """
    if value is None:
        return None
    value = value.strip()
    if not value or not value.isascii() or not value.isdigit():
        return None
    try:
        return int(value)
    except ValueError:  # pragma: no cover - isdigit() guarantees success
        return None


def is_timestamp_fresh(timestamp: int, window_s: int, now: float | None = None) -> bool:
    """True when the timestamp is within +-``window_s`` seconds of ``now``."""
    if now is None:
        now = time.time()
    return abs(now - timestamp) <= window_s


def verify_signature(
    secret: str, body: bytes, timestamp: int, signature: str | None
) -> bool:
    """Constant-time verification of the ``X-Signature`` header value.

    The input header is normalized (trimmed, lowercased) before comparison,
    mirroring the reference implementations.
    """
    if not signature:
        return False
    expected = compute_signature(secret, body, timestamp)
    return hmac.compare_digest(expected, signature.strip().lower())
