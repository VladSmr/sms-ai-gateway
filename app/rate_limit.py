"""In-memory, per-process, per-client rate limiter (soft limit).

The limiter keeps a sliding 60-second window of request timestamps per key
(the client IP). It is intentionally simple: state lives in the process
memory and resets on restart, which is acceptable for a "soft" limit whose
purpose is to blunt request floods, not to enforce a hard quota. A 429 makes
the gateway retry the delivery later with its exponential backoff, so no
data is lost.

Thread-safety: :meth:`allow` is called from async request handlers on the
event loop thread and performs no awaits, so each call is atomic with
respect to other handlers (single-threaded event loop). If the limiter is
ever used from worker threads, wrap calls in a lock.
"""

import time
from collections import deque

WINDOW_S = 60.0

# V1: soft cap on tracked keys + amortized cleanup. A flood with UNIQUE
# spoofed keys (rotated X-Forwarded-For values) inserts a new dict entry per
# request; the per-key window drains after WINDOW_S, but a one-shot key is
# never revisited, so without a sweep its entry would stay forever. The
# sweep runs at most once per SWEEP_INTERVAL brand-new keys and only while
# the dict is over MAX_KEYS: it drops every key whose window has fully
# drained, so memory stays bounded regardless of the number of unique
# attack keys, and the O(size) pass stays amortized O(1) per request.
MAX_KEYS = 8192
SWEEP_INTERVAL = 256


class RateLimiter:
    """Sliding-window rate limiter: at most ``max_per_minute`` hits per key."""

    def __init__(self, max_per_minute: int, time_func=time.monotonic) -> None:
        if max_per_minute <= 0:
            raise ValueError("max_per_minute must be positive")
        self._max = max_per_minute
        self._time_func = time_func
        self._hits: dict[str, deque[float]] = {}
        self._insertions = 0  # V1: brand-new keys since the last sweep

    @property
    def max_per_minute(self) -> int:
        """Currently applied limit (may be changed via :meth:`update_limit`)."""
        return self._max

    def update_limit(self, max_per_minute: int) -> None:
        """Apply a new limit without discarding tracked windows.

        Used for config hot reload: re-creating the limiter would wipe the
        per-key windows, handing a flood in progress a fresh budget.
        """
        if max_per_minute <= 0:
            raise ValueError("max_per_minute must be positive")
        self._max = max_per_minute

    def allow(self, key: str) -> bool:
        """Register a hit for ``key``; True when within the limit."""
        now = self._time_func()
        cutoff = now - WINDOW_S

        if len(self._hits) > MAX_KEYS and self._insertions >= SWEEP_INTERVAL:
            # V1: amortized dead-key sweep. O(size) pass, but throttled by
            # SWEEP_INTERVAL fresh insertions, so it cannot become a CPU
            # drain even if the dict stays over the cap with live keys.
            self._sweep(cutoff)

        hits = self._hits.get(key)
        if hits is None:
            hits = deque()
            self._hits[key] = hits
            self._insertions += 1
        while hits and hits[0] <= cutoff:
            hits.popleft()

        if len(hits) >= self._max:
            # Rejected requests do not consume slots; the key stays tracked
            # while its window holds live hits (a flood in progress must not
            # get a fresh budget). A fully drained window is reclaimed by
            # the sweep above (or by the next call for this key).
            return False
        hits.append(now)
        return True

    def _sweep(self, cutoff: float) -> None:
        """Drop keys whose sliding window has fully drained (V1).

        A key is dead when its NEWEST hit is older than the window: no
        request inside WINDOW_S can still be counted for it. Such keys are
        exactly the memory leak of a flood with UNIQUE spoofed keys (rotated
        X-Forwarded-For) - they are never revisited, so only this pass can
        remove them. Keys with live hits are kept: dropping them would hand
        an in-progress flood a fresh budget.
        """
        dead = [k for k, v in self._hits.items() if not v or v[-1] <= cutoff]
        for key in dead:
            del self._hits[key]
        self._insertions = 0
