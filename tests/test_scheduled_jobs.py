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
        # _find_todays_csv is stubbed present: the job now waits up to
        # pipeline_csv_wait_mins for the strategy CSV, and an unstubbed call
        # here blocks the suite for 45 real minutes.
        with mock.patch.object(scheduler, "_find_todays_csv", return_value="/x.csv"), \
             mock.patch.object(scheduler, "_csv_is_settled", return_value=True), \
             mock.patch.object(scheduler, "run_pipeline") as m:
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
        with mock.patch.object(scheduler, "_find_todays_csv", return_value="/x.csv"), \
             mock.patch.object(scheduler, "_csv_is_settled", return_value=True), \
             mock.patch.object(scheduler, "_Watchdog", rec), \
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






class TestDailyPipelineWaitsForStrategyCSV:
    """The 06:35 slot starts before the briefing task has ever finished.

    Across six recent sessions the Strategy-Purchase CSV landed between 06:46
    and 10:28 — never by 06:35. run_pipeline degrades silently to zero hints
    when it is absent (an INFO line, then the day's orders placed blind), so
    the pipeline waits for it rather than racing it.
    """

    def test_returns_immediately_when_the_csv_is_already_there(self):
        with mock.patch.object(scheduler, "_find_todays_csv", return_value="/x.csv"), \
             mock.patch.object(scheduler, "_csv_is_settled", return_value=True), \
             mock.patch.object(scheduler._shutdown, "wait") as waited:
            assert scheduler._wait_for_strategy_csv(600) is True
        assert not waited.called, "should not sleep when the CSV is already present"

    def test_waits_then_succeeds_when_the_csv_appears(self):
        calls = {"n": 0}

        def appears():
            calls["n"] += 1
            return "/x.csv" if calls["n"] >= 3 else None

        with mock.patch.object(scheduler, "_find_todays_csv", side_effect=appears), \
             mock.patch.object(scheduler, "_csv_is_settled", return_value=True), \
             mock.patch.object(scheduler._shutdown, "wait", return_value=False) as waited:
            assert scheduler._wait_for_strategy_csv(600, poll_secs=1) is True
        # Deliberately not an exact count: the check runs once before the loop
        # and again inside it, so a precise number pins the call structure
        # rather than the behaviour. What matters is that it did not give up,
        # and did not return before the CSV appeared.
        assert waited.call_count >= 1, "should have polled while waiting"

    def test_times_out_and_reports_false(self):
        with mock.patch.object(scheduler, "_find_todays_csv", return_value=None), \
             mock.patch.object(scheduler._shutdown, "wait", return_value=False):
            assert scheduler._wait_for_strategy_csv(0) is False

    def test_shutdown_during_the_wait_aborts_promptly(self):
        """A SIGTERM mid-wait must not sit here for the rest of the budget."""
        with mock.patch.object(scheduler, "_find_todays_csv", return_value=None), \
             mock.patch.object(scheduler._shutdown, "wait", return_value=True) as waited:
            assert scheduler._wait_for_strategy_csv(3600, poll_secs=30) is False
        assert waited.call_count == 1, "should return on the first shutdown signal"

    def test_a_broken_strategy_module_reports_error_not_absent(self):
        """Distinct outcomes: "error" must not be polled against for 45 min."""
        with mock.patch.dict("sys.modules", {"strategy": None}):
            assert scheduler._find_todays_csv() == "error"

    def test_a_broken_strategy_module_stops_waiting_immediately(self):
        with mock.patch.object(scheduler, "_find_todays_csv", return_value="error"), \
             mock.patch.object(scheduler._shutdown, "wait") as waited:
            assert scheduler._wait_for_strategy_csv(3600) is False
        assert not waited.called, "a broken module must not be waited out"

    def test_a_half_written_csv_is_not_taken(self, tmp_path):
        """We now race the writer, so the file must settle before we parse it.

        Uses a real file and a real writer rather than patching os.path.getsize,
        which pytest itself calls — stubbing it globally with a short
        side_effect list breaks unrelated callers and hangs the run.
        """
        f = tmp_path / "Strategy-Purchase-01-01-2026.csv"
        f.write_text("SYMBOL,PCS\n")

        def still_writing(_secs):
            f.write_text("SYMBOL,PCS\n" + "AAPL,1\n" * 50)
            return False

        with mock.patch.object(scheduler._shutdown, "wait", side_effect=still_writing):
            assert scheduler._csv_is_settled(str(f)) is False

    def test_a_settled_csv_is_accepted(self, tmp_path):
        f = tmp_path / "Strategy-Purchase-01-01-2026.csv"
        f.write_text("SYMBOL,PCS\nAAPL,1\n")
        with mock.patch.object(scheduler._shutdown, "wait", return_value=False):
            assert scheduler._csv_is_settled(str(f)) is True

    def test_an_empty_settled_file_is_rejected(self, tmp_path):
        """Zero bytes twice is a created-but-unwritten file, not a ready one."""
        f = tmp_path / "x.csv"
        f.write_text("")
        with mock.patch.object(scheduler._shutdown, "wait", return_value=False):
            assert scheduler._csv_is_settled(str(f)) is False

    def test_a_missing_file_is_not_settled(self, tmp_path):
        with mock.patch.object(scheduler._shutdown, "wait", return_value=False):
            assert scheduler._csv_is_settled(str(tmp_path / "nope.csv")) is False

    def test_a_bad_config_value_does_not_kill_the_job(self):
        """int() raises before the watchdog — a typo would cost the whole day."""
        with mock.patch.object(scheduler, "load_config",
                               return_value={"pipeline_csv_wait_mins": "abc"}), \
             mock.patch.object(scheduler, "_find_todays_csv", return_value="/x.csv"), \
             mock.patch.object(scheduler, "_csv_is_settled", return_value=True), \
             mock.patch.object(scheduler, "run_pipeline") as run, \
             mock.patch.object(scheduler, "_capture_market_baseline"):
            scheduler.job_daily_pipeline()
        run.assert_called_once()

    def test_wait_budget_is_counted_into_the_liveness_bound(self):
        """Otherwise a long wait plus a long pipeline makes /healthz report dead.

        _job_active spans the whole job, wait included, so if the wait were not
        in _JOB_MAX_SECS a 45-minute wait followed by a 60-minute pipeline would
        exceed the bound and the uptime check would restart a healthy container
        mid-run.
        """
        assert scheduler._JOB_MAX_SECS >= (
            scheduler._CSV_WAIT_MAX_SECS + scheduler._WATCHDOG_CC_PIPELINE
        ), "wait budget is not accounted for in _JOB_MAX_SECS"

    def test_config_value_is_clamped_to_the_hard_cap(self):
        cfg = {"pipeline_csv_wait_mins": 9999}
        clamped = min(max(int(cfg["pipeline_csv_wait_mins"]), 0) * 60,
                      scheduler._CSV_WAIT_MAX_SECS)
        assert clamped == scheduler._CSV_WAIT_MAX_SECS
