"""Unit tests for the in-memory sliding-window rate limiter."""

import pytest

from app.rate_limit import RateLimiter


class _FakeClock:
    """Deterministic monotonic clock for limiter tests."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_allows_up_to_limit_and_blocks_excess():
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=3, time_func=clock)

    results = [limiter.allow("ip") for _ in range(4)]

    assert results == [True, True, True, False]


def test_window_slides_after_a_minute():
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=2, time_func=clock)

    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is False

    clock.advance(61)  # both hits fell out of the 60 s window
    assert limiter.allow("ip") is True


def test_keys_are_independent():
    limiter = RateLimiter(max_per_minute=1, time_func=_FakeClock())

    assert limiter.allow("ip-1") is True
    assert limiter.allow("ip-2") is True
    assert limiter.allow("ip-1") is False


def test_rejected_requests_do_not_consume_slots():
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=1, time_func=clock)

    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is False
    assert limiter.allow("ip") is False

    clock.advance(61)
    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is False


def test_update_limit_applies_without_discarding_windows():
    clock = _FakeClock()
    limiter = RateLimiter(max_per_minute=2, time_func=clock)

    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is True
    assert limiter.allow("ip") is False

    limiter.update_limit(100)  # hot reload: raise the limit
    assert limiter.max_per_minute == 100

    # The already-tracked window is kept (a flood in progress does not get a
    # fresh budget for its recorded hits), but the raised limit lets new
    # requests through immediately.
    assert limiter.allow("ip") is True

    clock.advance(61)
    assert limiter.allow("ip") is True


def test_update_limit_rejects_non_positive():
    limiter = RateLimiter(max_per_minute=1, time_func=_FakeClock())

    with pytest.raises(ValueError):
        limiter.update_limit(0)
    with pytest.raises(ValueError):
        limiter.update_limit(-5)
    assert limiter.max_per_minute == 1
