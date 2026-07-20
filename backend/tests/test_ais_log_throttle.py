"""Tests for services.log_throttle (2026-07-20 AIS storm hardening)."""

from services.log_throttle import RateLimitedRelay


class FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_burst_passes_through_then_suppresses():
    clock = FakeClock()
    relay = RateLimitedRelay(burst=3, window_s=60.0, clock=clock)

    emitted = [relay.offer(f"line {i}")[0] for i in range(5)]
    assert emitted == [True, True, True, False, False]
    assert relay.suppressed_total == 2


def test_summary_emitted_on_window_rollover():
    clock = FakeClock()
    relay = RateLimitedRelay(burst=2, window_s=60.0, clock=clock)

    for i in range(10):
        relay.offer(f"line {i}")

    clock.advance(61.0)
    emit, summary = relay.offer("after rollover")
    assert emit is True
    assert summary is not None
    assert "8 similar lines suppressed" in summary


def test_no_summary_when_nothing_suppressed():
    clock = FakeClock()
    relay = RateLimitedRelay(burst=10, window_s=60.0, clock=clock)

    relay.offer("a")
    clock.advance(61.0)
    emit, summary = relay.offer("b")
    assert emit is True
    assert summary is None


def test_rate_in_window_tracks_storm_signal():
    clock = FakeClock()
    relay = RateLimitedRelay(burst=2, window_s=60.0, clock=clock)

    for i in range(500):
        relay.offer(f"storm {i}")
    assert relay.rate_in_window() == 500

    # New window: the signal resets instead of accumulating forever.
    clock.advance(61.0)
    relay.offer("calm")
    assert relay.rate_in_window() == 1


def test_suppressed_total_accumulates_across_windows():
    clock = FakeClock()
    relay = RateLimitedRelay(burst=1, window_s=60.0, clock=clock)

    relay.offer("w1 a")
    relay.offer("w1 b")
    clock.advance(61.0)
    relay.offer("w2 a")
    relay.offer("w2 b")
    assert relay.suppressed_total == 2
