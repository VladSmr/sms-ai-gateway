"""Reply trimming for the SMS transport (step 5).

One SMS segment carries 70 UTF-16 code units (UCS-2) or 160 in the GSM-7
alphabet; the practical single-message budget for Cyrillic text is well
below the multipart cap, so the reply is trimmed to
``limits.max_reply_units`` (default 201 - three GSM-7 segments, the value
from PLAN.md step 5) BEFORE it is handed to the gateway client. Trimming is
a transport concern only: the FULL (untrimmed) reply is what gets stored in
the session history.

Cut-point rules (justified, deterministic):

- Already within budget -> returned unchanged.
- Preferred cut: the LAST sentence-ending punctuation (``.`` ``!`` ``?``
  ``…``) that is followed by whitespace and fits the budget.
- A dot is NOT a cut point when it is not followed by whitespace. This
  single rule covers both hazardous classes: abbreviation dots inside
  "т.д." / "т.п." (no space after the internal dot - the abbreviation can
  never be split) and decimal dots ("3.14").
- A dot immediately AFTER a digit is additionally excluded ("3. 14" exotic
  spacing); the cut never lands between the digits of a number.
- No fitting sentence boundary -> hard cut at the budget, never inside a
  UTF-16 surrogate pair (emoji are counted as whole code points).

Emoji/markdown styling of the reply is the LLM system prompt's job (step 3)
- this module is deliberately dumb about content.
"""

_UTF16_HIGH_SURROGATE_MIN = 0xD800
_UTF16_HIGH_SURROGATE_MAX = 0xDBFF
_SENTENCE_ENDS = ".!?\u2026"  # . ! ? …

DEFAULT_MAX_REPLY_UNITS = 201


def utf16_units(text: str) -> int:
    """Length of ``text`` in UTF-16 code units (SMS length accounting)."""
    return len(text.encode("utf-16-le")) // 2


def trim_reply(text: str, max_units: int = DEFAULT_MAX_REPLY_UNITS) -> str:
    """Trim ``text`` to at most ``max_units`` UTF-16 code units.

    Prefers a sentence boundary (see the module docstring); falls back to a
    hard cut that never splits a surrogate pair.
    """
    if max_units <= 0:
        raise ValueError("max_units must be positive")
    if utf16_units(text) <= max_units:
        return text

    best: str | None = None
    for index, char in enumerate(text):
        if char not in _SENTENCE_ENDS:
            continue
        nxt = text[index + 1] if index + 1 < len(text) else " "
        if not nxt.isspace():
            # No whitespace after the punctuation: abbreviation dot
            # ("т.д.") or decimal dot ("3.14") - never a cut point.
            continue
        if char == "." and index > 0 and text[index - 1].isdigit():
            # Dot right after a digit ("3. 14" spacing): protect numbers.
            continue
        candidate = text[: index + 1].rstrip()
        if utf16_units(candidate) <= max_units:
            best = candidate
        else:
            # Prefixes only grow from here - no better boundary exists.
            break
    if best is not None:
        return best

    # Hard cut: binary search the longest fitting char prefix (UTF-16 units
    # are monotonic in the prefix length), then repair a dangling surrogate.
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if utf16_units(text[:mid]) <= max_units:
            lo = mid
        else:
            hi = mid - 1
    cut = text[:lo]
    if cut and _UTF16_HIGH_SURROGATE_MIN <= ord(cut[-1]) <= _UTF16_HIGH_SURROGATE_MAX:
        cut = cut[:-1]
    return cut.rstrip()
