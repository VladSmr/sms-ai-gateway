"""Session management package (step 3).

- :mod:`.normalizer` - raw sender to E.164 normalization;
- :mod:`.store` - numbered sessions: history, TTL (lazy + sweep), LRU cap,
  per-session TEMP/PROMPT overrides.

The submodules are imported here on purpose: domain code addresses them as
``sessions.normalizer`` / ``sessions.store``, which must not depend on the
import order of test or application modules.
"""

from . import normalizer, store  # noqa: F401
