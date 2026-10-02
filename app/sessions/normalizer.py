"""Sender normalization to E.164 (Russian-centric, step 3).

The Android gateway reports the originating address in whatever form the
device's SMS PDU and contacts database provide: ``8-916-123-45-67``,
``8 916 123 45 67``, ``+7 (916) 123-45-67``, a bare ``89161234567``, or an
alphanumeric sender id (``MTS``, ``MCHS-moskva``, bank names). The whitelist
stores strict E.164 entries (validated in :mod:`app.config`), so inbound
senders must be reduced to the same canonical ``+<country><number>`` form
before the membership check.

Normalization rules (documented in ARCHITECTURE.md section 10):

1. Whitespace is stripped; every character that is not a digit is discarded
   (separators ``-``, spaces, dots, parentheses). A leading ``+`` is kept.
2. No digits left -> ``None`` (alphanumeric senders, empty strings): these
   are silently dropped like any other non-whitelisted sender.
3. Leading ``+`` -> returned as-is (``+`` + digits): international format is
   trusted; whether it belongs to the whitelist is decided by the caller.
4. 11 digits starting with ``8`` -> ``+7`` + the last 10 digits (Russian
   trunk prefix replaced with the country code).
5. 11 digits starting with ``7`` -> ``+`` + digits (country code without the
   plus, as some devices report it).
6. 10 digits -> ``+7`` + digits (a Russian number reported without any
   country code; e.g. from the contacts DB). The whitelist still gates.
7. Anything else (short codes like ``900``, too long sequences) -> ``None``.

There is intentionally no full libphonenumber-style validation here: the
normalized value is only compared against the whitelist, so a weird-but-
parseable number simply fails the membership check.
"""

import re

_DIGITS_PATTERN = re.compile(r"[0-9]")

_RU_COUNTRY_CODE = "7"
_RU_TRUNK_PREFIX = "8"
_RU_NATIONAL_LENGTH = 10
_FULL_LENGTH = _RU_NATIONAL_LENGTH + 1  # country code + national number


def normalize_sender(raw: str) -> str | None:
    """Normalize a raw sender value to E.164 or ``None`` if unparseable.

    ``None`` means "cannot be a phone number" (alphanumeric sender, empty
    string, short code) - the caller treats it as a whitelist drop.
    """
    stripped = raw.strip()
    digits = "".join(_DIGITS_PATTERN.findall(stripped))
    if not digits:
        # Alphanumeric sender id (MTS, MCHS-moskva, a bank name) or empty.
        return None
    has_plus = stripped.startswith("+")
    if has_plus:
        return "+" + digits
    if len(digits) == _FULL_LENGTH and digits[0] == _RU_TRUNK_PREFIX:
        return "+" + _RU_COUNTRY_CODE + digits[1:]
    if len(digits) == _FULL_LENGTH and digits[0] == _RU_COUNTRY_CODE:
        return "+" + digits
    if len(digits) == _RU_NATIONAL_LENGTH:
        return "+" + _RU_COUNTRY_CODE + digits
    # Short codes (e.g. "900"), overlong sequences, other country formats
    # without a plus: not representable in the whitelist's canonical form.
    return None
