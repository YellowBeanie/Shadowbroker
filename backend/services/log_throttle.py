"""Rate-limited relay for noisy subprocess log streams.

Added after the 2026-07-20 AIS reconnect storm: ais_proxy.js stderr was
re-logged line-for-line by the backend, pushing 12.9M lines/24h into the
cluster log pipeline. The relay lets a small burst through per rolling
window and summarises the rest, and exposes the in-window rate so the
caller can trip a circuit breaker when a storm is underway.
"""

from __future__ import annotations

import time


class RateLimitedRelay:
    """Allow up to ``burst`` lines per ``window_s`` rolling window.

    ``offer(line)`` returns ``(emit, summary)``:
      * ``emit`` — True when the line should be logged as-is;
      * ``summary`` — a suppression report to log once per window
        rollover, or None.
    """

    def __init__(
        self,
        burst: int = 30,
        window_s: float = 60.0,
        clock=time.monotonic,
    ) -> None:
        self.burst = burst
        self.window_s = window_s
        self._clock = clock
        self._window_start = clock()
        self._seen_in_window = 0
        self._suppressed_in_window = 0
        self.suppressed_total = 0

    def offer(self, line: str) -> tuple[bool, str | None]:
        now = self._clock()
        summary = None
        if now - self._window_start >= self.window_s:
            if self._suppressed_in_window:
                summary = (
                    f"({self._suppressed_in_window} similar lines suppressed "
                    f"in the last {int(now - self._window_start)}s)"
                )
            self._window_start = now
            self._seen_in_window = 0
            self._suppressed_in_window = 0
        self._seen_in_window += 1
        if self._seen_in_window <= self.burst:
            return True, summary
        self._suppressed_in_window += 1
        self.suppressed_total += 1
        return False, summary

    def rate_in_window(self) -> int:
        """Lines offered in the current window — the storm-detection signal."""
        return self._seen_in_window
