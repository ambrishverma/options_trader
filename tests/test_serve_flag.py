"""Tests for the --serve entrypoint wiring.

--serve is the container entrypoint: it registers the scheduler's jobs, hands
the loop to a daemon thread, and gives the main thread to uvicorn.  Ordering
matters — jobs must be registered before uvicorn blocks, and signal handlers
must install on the main thread because the signal module requires it.
"""

import sys
import os
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import main


class TestServeFlagParsing:

    def test_serve_flag_is_accepted(self):
        parser = main.build_parser()
        args = parser.parse_args(["--serve"])
        assert args.serve is True

    def test_serve_defaults_to_false(self):
        parser = main.build_parser()
        args = parser.parse_args(["--status"])
        assert args.serve is False

    def test_serve_is_mutually_exclusive_with_schedule(self):
        """Both are long-running primary commands; running them together is nonsense."""
        parser = main.build_parser()
        with mock.patch("sys.stderr"):
            try:
                parser.parse_args(["--serve", "--schedule"])
            except SystemExit as e:
                assert e.code == 2
            else:
                raise AssertionError("--serve and --schedule should be mutually exclusive")


class TestCmdServe:

    def test_starts_scheduler_non_blocking_then_serves(self):
        """Jobs must be registered before uvicorn takes the main thread."""
        calls = []

        with mock.patch("main.check_env"), \
             mock.patch("scheduler.start_scheduler",
                        side_effect=lambda block: calls.append(("start", block))), \
             mock.patch("scheduler.install_signal_handlers",
                        side_effect=lambda: calls.append(("signals",))), \
             mock.patch("threading.Thread") as mock_thread, \
             mock.patch("uvicorn.run", side_effect=lambda *a, **k: calls.append(("uvicorn",))):
            main.cmd_serve()

        assert ("start", False) in calls, "scheduler must be started with block=False"
        assert ("signals",) in calls, "signal handlers must install on the main thread"
        assert ("uvicorn",) in calls, "uvicorn must be started"
        assert calls.index(("start", False)) < calls.index(("uvicorn",)), \
            "jobs must be registered before uvicorn blocks the main thread"

    def test_scheduler_runs_on_a_daemon_thread(self):
        """A non-daemon thread would block process exit after SIGTERM."""
        with mock.patch("main.check_env"), \
             mock.patch("scheduler.start_scheduler"), \
             mock.patch("scheduler.install_signal_handlers"), \
             mock.patch("threading.Thread") as mock_thread, \
             mock.patch("uvicorn.run"):
            main.cmd_serve()

        mock_thread.assert_called_once()
        assert mock_thread.call_args.kwargs.get("daemon") is True, \
            "scheduler thread must be a daemon so it cannot block process exit"
        mock_thread.return_value.start.assert_called_once()

    def test_binds_all_interfaces_on_8080(self):
        """Container networking requires 0.0.0.0; Caddy proxies to app:8080."""
        with mock.patch("main.check_env"), \
             mock.patch("scheduler.start_scheduler"), \
             mock.patch("scheduler.install_signal_handlers"), \
             mock.patch("threading.Thread"), \
             mock.patch("uvicorn.run") as mock_run:
            main.cmd_serve()

        assert mock_run.call_args.kwargs["host"] == "0.0.0.0"
        assert mock_run.call_args.kwargs["port"] == 8080
