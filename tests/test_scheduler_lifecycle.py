"""Tests for scheduler process lifecycle: graceful shutdown and liveness heartbeat.

These cover the container runtime contract:
  - `docker stop` sends SIGTERM; the loop must exit between jobs, not mid-order.
  - In --serve mode the scheduler is a background thread.  If it dies, uvicorn
    keeps answering, so /healthz needs a heartbeat to tell live from dead.
"""

import sys
import os
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scheduler


@pytest.fixture(autouse=True)
def reset_lifecycle_state():
    """Each test starts with a clear shutdown event and no heartbeat."""
    scheduler._shutdown.clear()
    scheduler._last_tick = None
    yield
    scheduler._shutdown.set()      # release any loop still running
    scheduler._shutdown.clear()
    scheduler._last_tick = None


class TestGracefulShutdown:
    """SIGTERM must drain the current job rather than kill the process."""

    def test_request_shutdown_sets_event(self):
        assert not scheduler._shutdown.is_set()
        scheduler.request_shutdown()
        assert scheduler._shutdown.is_set()

    def test_run_loop_exits_when_shutdown_requested(self):
        """run_loop must return promptly after request_shutdown, not after the 30s tick."""
        thread = threading.Thread(target=scheduler.run_loop, daemon=True)
        thread.start()
        time.sleep(0.2)
        assert thread.is_alive(), "loop should still be running before shutdown"

        scheduler.request_shutdown()
        thread.join(timeout=5)
        assert not thread.is_alive(), "loop did not exit within 5s of shutdown request"

    def test_run_loop_returns_immediately_if_already_shut_down(self):
        scheduler.request_shutdown()
        start = time.monotonic()
        scheduler.run_loop()
        assert time.monotonic() - start < 1.0

    def test_install_signal_handlers_is_noop_off_main_thread(self):
        """signal.signal() raises ValueError off the main thread; we must not."""
        error = {}

        def target():
            try:
                scheduler.install_signal_handlers()
            except Exception as e:      # noqa: BLE001 - test asserts nothing escapes
                error["e"] = e

        t = threading.Thread(target=target)
        t.start()
        t.join(timeout=5)
        assert "e" not in error, f"install_signal_handlers raised off-thread: {error.get('e')}"

    def test_job_exception_does_not_kill_the_loop(self, monkeypatch):
        """A job that escapes its own error handling must not stop the scheduler.

        The tick interval is shortened so the job becomes due inside the test
        window; the real 30s value would need a 30s test to prove the same thing.
        """
        monkeypatch.setattr(scheduler, "_TICK_SECONDS", 0.05)
        calls = []

        def exploding_job():
            calls.append(1)
            raise RuntimeError("job blew up")

        scheduler.schedule.every(1).seconds.do(exploding_job)
        thread = threading.Thread(target=scheduler.run_loop, daemon=True)
        try:
            thread.start()
            time.sleep(1.5)
            assert calls, "job should have run at least once"
            assert thread.is_alive(), "loop died on a job exception"
        finally:
            scheduler.request_shutdown()
            thread.join(timeout=5)
            scheduler.schedule.clear()


class TestLivenessHeartbeat:
    """/healthz distinguishes a ticking loop from a dead thread."""

    def test_scheduler_alive_false_before_any_tick(self):
        assert scheduler.scheduler_alive() is False

    def test_run_loop_stamps_heartbeat_while_running(self):
        """Heartbeat is live during the loop and cleared once it exits.

        Clearing on exit matters: otherwise /healthz keeps answering 200 for up
        to 90s after the scheduler is gone — precisely the "uvicorn still
        serving, scheduler dead" case the probe exists to catch.
        """
        thread = threading.Thread(target=scheduler.run_loop, daemon=True)
        thread.start()
        try:
            time.sleep(0.2)
            assert scheduler._last_tick is not None
            assert scheduler.scheduler_alive() is True
        finally:
            scheduler.request_shutdown()
            thread.join(timeout=5)

        assert scheduler._last_tick is None
        assert scheduler.scheduler_alive() is False

    def test_scheduler_alive_false_when_heartbeat_stale(self):
        scheduler._last_tick = time.monotonic() - 120.0
        assert scheduler.scheduler_alive(max_age_secs=90.0) is False

    def test_scheduler_alive_true_within_window(self):
        scheduler._last_tick = time.monotonic() - 10.0
        assert scheduler.scheduler_alive(max_age_secs=90.0) is True
