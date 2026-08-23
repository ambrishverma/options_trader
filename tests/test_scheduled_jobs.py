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
    """Every job gates on these two; make them permissive.

    _capture_market_baseline is patched because it is NOT a no-op: it makes live
    yfinance calls for QQQ/SPY/XLK and writes DATA_DIR/snapshots/market_baseline.json.
    Unpatched, running this suite in the main checkout would overwrite the
    running scheduler's baseline with the current price — after which a real
    move from the morning reference is either masked, or fires a spurious
    catch-up run that places live orders.
    """
    with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
         mock.patch.object(scheduler, "_wait_for_network", return_value=True), \
         mock.patch.object(scheduler, "_capture_market_baseline"), \
         mock.patch.object(scheduler, "_save_baseline_to_disk"):
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

    def test_job_daily_pipeline_runs_live(self):
        """Full kwargs asserted: a stray dry_run=True would silently stop trading."""
        with mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_daily_pipeline()
        m.assert_called_once()
        assert m.call_args.kwargs.get("dry_run", False) is False

    def test_job_early_pipeline_is_scan_only(self):
        """The 06:35 PT run must NOT generate income or buy defense.

        Flipping either skip_* to False turns a scan into a live
        income + auto-defense run, which an assert_called_once() would miss.
        """
        with mock.patch.object(scheduler, "run_pipeline") as m:
            scheduler.job_early_pipeline()
        m.assert_called_once()
        kw = m.call_args.kwargs
        assert kw.get("skip_income") is True, "early pipeline must skip income generation"
        assert kw.get("skip_auto_defense") is True, "early pipeline must skip auto-defense"

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


class TestWatchdogBudgets:
    """Every job must run under a watchdog with the right budget.

    Nothing previously inspected the timeout= argument, so a job silently
    demoted to a 10-minute budget (or none at all) would ship green.
    """

    def _record(self):
        calls = []

        class _Recorder:
            def __init__(self, label, timeout=scheduler._WATCHDOG_CC_PIPELINE):
                calls.append((label, timeout))

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        return calls, _Recorder

    def test_portfolio_pull_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch("portfolio.pull_daily_robinhood_snapshot", return_value="s"):
            scheduler.job_daily_portfolio_pull()
        assert calls == [("portfolio pull", scheduler._WATCHDOG_PORTFOLIO)]

    def test_daily_pipeline_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch.object(scheduler, "run_pipeline"):
            scheduler.job_daily_pipeline()
        assert calls and calls[0][1] == scheduler._WATCHDOG_CC_PIPELINE

    def test_options_report_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch("reporter.build_options_report", return_value={"orders": [], "order_count": 0}), \
             mock.patch("report_emailer.send_options_report_email"):
            scheduler.job_daily_options_report()
        assert calls == [("options report", scheduler._WATCHDOG_REPORT)]


    def test_early_pipeline_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch.object(scheduler, "run_pipeline"):
            scheduler.job_early_pipeline()
        assert calls and calls[0][1] == scheduler._WATCHDOG_CC_PIPELINE

    def test_weekly_report_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch("reporter.build_options_report",
                        return_value={"orders": [], "order_count": 0, "net_gain": 0.0,
                                      "total_credit": 0.0, "total_debit": 0.0,
                                      "start_date": "2026-08-13", "end_date": "2026-08-13"}), \
             mock.patch("report_emailer.send_options_report_email"):
            scheduler.job_weekly_options_report()
        assert calls == [("weekly options report", scheduler._WATCHDOG_REPORT)]

    def test_market_move_catch_up_budget(self):
        """The catch-up branch fires a LIVE pipeline; it must be watchdogged."""
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch.object(scheduler, "_has_pipeline_run_today", return_value=False), \
             mock.patch.object(scheduler, "run_pipeline"):
            scheduler.job_market_move_check()
        assert calls == [("catch-up run", scheduler._WATCHDOG_CC_PIPELINE)]

    def test_market_move_triggered_rerun_budget(self):
        calls, rec = self._record()
        with mock.patch.object(scheduler, "_Watchdog", rec), \
             mock.patch.object(scheduler, "_has_pipeline_run_today", return_value=True), \
             mock.patch.object(scheduler, "_check_market_move",
                               return_value={"QQQ": {"pct_change": 1.2}}), \
             mock.patch.object(scheduler, "run_pipeline"):
            scheduler.job_market_move_check()
        assert calls and calls[0][1] == scheduler._WATCHDOG_CC_PIPELINE

class TestJobRegistration:
    """start_scheduler must actually wire all six jobs into `schedule`.

    Deleting a `schedule.every().day.at(...).do(job_*)` line is the same class
    of accident as deleting _Watchdog: the loop keeps ticking, /healthz stays
    200, and that job simply never runs again.
    """

    def _register(self, tz_name="Asia/Tokyo"):
        """Register jobs with the machine pinned to a non-US timezone.

        Deliberately NOT the developer's zone.  Comparing registrations against
        _resolve_job_times() is self-referential on the conversion itself, so on
        a PT machine a hardcoded `at("06:35")` — the exact shape of the bug #56
        fixed — still matches.  Pinning to Tokyo makes any unconverted literal
        diverge from the resolved value.
        """
        from zoneinfo import ZoneInfo
        scheduler.schedule.clear()
        with mock.patch.object(scheduler, "_local_tz", return_value=ZoneInfo(tz_name)), \
             mock.patch.object(scheduler, "_acquire_pid_lock"), \
             mock.patch.object(scheduler, "setup_logging"), \
             mock.patch.object(scheduler, "_capture_market_baseline"), \
             mock.patch.object(scheduler, "_load_baseline_from_disk", return_value={}):
            scheduler.start_scheduler(block=False)
            resolved = scheduler._resolve_job_times(scheduler.load_config())
        rows = [(j.job_func.func.__name__, str(j.at_time), j.unit, j.start_day)
                for j in scheduler.schedule.jobs]
        return rows, resolved

    def test_all_scheduled_jobs_are_registered(self):
        try:
            rows, _ = self._register()
            names = {r[0] for r in rows}
            for name in ("job_daily_portfolio_pull",
                         "job_daily_pipeline", "job_daily_options_report",
                         "job_weekly_options_report", "job_market_move_check"):
                assert name in names, f"{name} was never registered with schedule"
        finally:
            scheduler.schedule.clear()

    def test_early_pipeline_is_deliberately_not_registered(self):
        """The scan-only early run was replaced by the full daily pipeline.

        job_early_pipeline still exists and is still gate-tested, but nothing
        schedules it. Re-registering it would mean two pipelines in one
        session, so this pins the absence rather than leaving it to a count.
        """
        try:
            rows, _ = self._register()
            names = {r[0] for r in rows}
            assert "job_early_pipeline" not in names, (
                "job_early_pipeline is registered again — the daily pipeline now "
                "occupies that slot, so this would run two pipelines per session"
            )
        finally:
            scheduler.schedule.clear()

    def test_registered_times_match_the_resolved_schedule(self):
        """Names alone are not enough.

        A set of names is blind to a job registered at the wrong wall-clock
        time — precisely the hardcoded-PT bug fixed in #56 — and it collapses
        the market checks into one entry, so dropping all but one would pass
        silently.
        """
        try:
            rows, expected = self._register()

            by_name = {}
            for name, at, unit, start_day in rows:
                by_name.setdefault(name, []).append((at[:5], unit, start_day))

            assert by_name["job_daily_portfolio_pull"] == [(expected["portfolio_pull"], "days", None)]
            assert by_name["job_daily_pipeline"] == [(expected["daily_pipeline"], "days", None)]
            assert by_name["job_daily_options_report"] == [(expected["options_report"], "days", None)]

            # Weekly report must stay weekly and stay on Saturday.
            assert by_name["job_weekly_options_report"] == [
                (expected["weekly_report"], "weeks", "saturday")
            ]

            # Every market check must be registered — the set-based test would
            # have passed with four of the five missing.
            checks = sorted(t for t, _, _ in by_name["job_market_move_check"])
            assert checks == sorted(expected["market_checks"]), \
                f"market checks {checks} != resolved {sorted(expected['market_checks'])}"
        finally:
            scheduler.schedule.clear()

    def test_total_registration_count_is_stable(self):
        """Guards against a duplicate registration as well as a dropped one."""
        try:
            rows, resolved = self._register()
            # 4 daily/weekly jobs (portfolio pull, daily pipeline, options
            # report, weekly report) — the early pipeline is no longer one.
            expected = 4 + len(resolved["market_checks"])
            assert len(rows) == expected, f"expected {expected} jobs, got {len(rows)}"
        finally:
            scheduler.schedule.clear()




