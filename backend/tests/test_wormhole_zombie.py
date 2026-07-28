"""Regression tests: wormhole process zombie reaping (incident 2026-07-27).

Confirmed live: PID 309987 [python3] was left as a zombie because the code
returned from the poll loop without calling process.wait(), and the next
start_wormhole() call overwrote _PROCESS without reaping the old one.

These tests cover _reap_if_exited() directly — the helper that encapsulates
both reap sites — rather than mocking the full connect_wormhole() setup.
"""

import subprocess
from unittest.mock import MagicMock, call

import pytest

import services.wormhole_supervisor as ws


# ---------------------------------------------------------------------------
# _reap_if_exited() unit tests
# ---------------------------------------------------------------------------


def test_reap_if_exited_calls_wait_on_exited_process():
    """wait() must be called when the process has already exited (poll != None)."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.poll.return_value = 0  # already exited, exit code 0
    proc.pid = 12345

    ws._reap_if_exited(proc)

    proc.wait.assert_called_once()
    # Verify a timeout was supplied so we can never block indefinitely.
    args, kwargs = proc.wait.call_args
    assert "timeout" in kwargs, "wait() must be called with timeout= to avoid blocking"


def test_reap_if_exited_skips_running_process():
    """wait() must NOT be called when the process is still running (poll == None)."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.poll.return_value = None  # still running

    ws._reap_if_exited(proc)

    proc.wait.assert_not_called()


def test_reap_if_exited_accepts_none():
    """Passing None (no previous process) must be a no-op."""
    ws._reap_if_exited(None)  # must not raise


def test_reap_if_exited_handles_timeout_expired():
    """TimeoutExpired from wait() must be caught, not propagated to the caller."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.poll.return_value = 1  # already exited
    proc.pid = 99999
    proc.wait.side_effect = subprocess.TimeoutExpired(cmd="python3", timeout=5)

    # Must not raise.
    ws._reap_if_exited(proc)

    proc.wait.assert_called_once()


def test_reap_if_exited_nonzero_exit_still_reaped():
    """A process that crashed (exit code != 0) must also be reaped."""
    proc = MagicMock(spec=subprocess.Popen)
    proc.poll.return_value = -11  # SIGSEGV
    proc.pid = 42

    ws._reap_if_exited(proc)

    proc.wait.assert_called_once()
