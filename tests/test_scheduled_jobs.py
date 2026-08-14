"""Smoke tests for the six scheduled job entry points.

These exist because a refactor deleted the `_Watchdog` class — used by every
job and defined nowhere else — and the full suite stayed green.  No test
referenced any `job_*` function, so nothing noticed that every scheduled job
raised NameError on its first line.

That failure is invisible at runtime too: run_loop catches job exceptions so
one bad job cannot stop the others, and it refreshes the heartbeat in its
finally, so /healthz keeps answering 200 while nothing ever runs.  A silent
total outage on a system that manages live option positions.

Each test asserts only that the job reaches its real work with its collaborators
patched — cheap, and enough to catch an entry point that cannot execute at all.
"""

import sys
import os
from unittest import mock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scheduler


@pytest.fixture(autouse=True)
def _trading_day_with_network():
    """Every job gates on these two; make them permissive."""
    with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
         mock.patch.object(scheduler, "_wait_for_network", return_value=True):
        yield


class TestWatchdogExists:
    """The regression that motivated this file."""

    def test_watchdog_is_defined(self):
        assert hasattr(scheduler, "_Watchdog"), \
            "_Watchdog is referenced by every scheduled job and defined nowhere else"

    def test_watchdog_is_usable_as_a_context_manager(self):
        with scheduler._Watchdog("test", timeout=3600):
            pass


class TestJobEntryPoints:
    """Each job must reach its real work rather than dying on a NameError."""

    def test_job_daily_portfolio_pull(self):
        with mock.patch("portfolio.pull_daily_robinhood_snapshot",
                        return_value="snap.json") as m:
            scheduler.job_daily_portfolio_pull()
        m.assert_called_once()

    def test_job_daily_pipeline(self):
        with mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_daily_pipeline()
        m.assert_called_once()

    def test_job_early_pipeline(self):
        with mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_early_pipeline()
        m.assert_called_once()

    def test_job_daily_options_report(self):
        with mock.patch("reporter.build_options_report",
                        return_value={"orders": [], "order_count": 0}) as m, \
             mock.patch("report_emailer.send_options_report_email"):
            scheduler.job_daily_options_report()
        m.assert_called_once()

    def test_job_weekly_options_report(self):
        with mock.patch("reporter.build_options_report",
                        return_value={"orders": [], "order_count": 0}) as m, \
             mock.patch("report_emailer.send_options_report_email"):
            scheduler.job_weekly_options_report()
        m.assert_called_once()

    def test_job_market_move_check_catch_up_runs_pipeline(self):
        """With no run recorded today, the catch-up branch places LIVE orders.

        Asserting dry_run is False is deliberate: this is the branch that made
        the timezone bug expensive, so its liveness should be explicit in a test
        rather than discovered from the logs.
        """
        with mock.patch.object(scheduler, "_has_pipeline_run_today", return_value=False), \
             mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_market_move_check()
        m.assert_called_once()
        assert m.call_args.kwargs.get("dry_run") is False

    def test_job_market_move_check_no_rerun_without_a_move(self):
        with mock.patch.object(scheduler, "_has_pipeline_run_today", return_value=True), \
             mock.patch.object(scheduler, "_check_market_move", return_value={}), \
             mock.patch.object(scheduler, "_capture_market_baseline"), \
             mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_market_move_check()
        m.assert_not_called()


class TestJobsSkipNonTradingDays:
    """The trading-day guard is what keeps weekend runs from touching the account."""

    def test_pipeline_skipped_when_not_a_trading_day(self):
        with mock.patch.object(scheduler, "_is_trading_day", return_value=False), \
             mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_daily_pipeline()
        m.assert_not_called()

    def test_portfolio_pull_skipped_when_not_a_trading_day(self):
        with mock.patch.object(scheduler, "_is_trading_day", return_value=False), \
             mock.patch("portfolio.pull_daily_robinhood_snapshot") as m:
            scheduler.job_daily_portfolio_pull()
        m.assert_not_called()
