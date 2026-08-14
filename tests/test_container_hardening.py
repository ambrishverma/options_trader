"""Tests for container-runtime correctness.

These cover failure modes that only appear when the app runs as a container
against a live brokerage account:

  - a stale PID file must not permanently wedge a container whose PID is
    deterministic (under tini the app is always the same PID)
  - mutable state must live on the mounted volume, not in the image layer,
    or a rebuild silently re-arms a duplicate live pipeline run
  - /healthz must not report dead while a long job is legitimately running
  - optional credentials must not become hard requirements
"""

import os
import subprocess
import sys
import threading
import time
from unittest import mock
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import scheduler


@pytest.fixture(autouse=True)
def reset_lifecycle_state():
    scheduler._shutdown.clear()
    scheduler._last_tick = None
    scheduler._job_active = False
    yield
    scheduler._shutdown.set()
    scheduler._shutdown.clear()
    scheduler._last_tick = None
    scheduler._job_active = False


def _run_py(code: str, env_extra: dict) -> subprocess.CompletedProcess:
    """Run a snippet in a fresh interpreter so module-level paths re-evaluate."""
    return subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, **env_extra},
        capture_output=True, text=True, cwd=str(REPO),
    )


class TestDataDir:
    """State must be redirectable to the mounted volume.

    Without this the compose `./local-data:/data` mount is inert: every module
    computes BASE_DIR = Path(__file__).parent, so state lands in the image
    layer. After a rebuild logs/run_<date>.json is gone, _has_pipeline_run_today()
    returns False, and the next market check fires a second *live* pipeline on a
    day that already ran — with the action-ledger dedupe wiped by the same rebuild.
    """

    def test_data_dir_defaults_to_repo_when_unset(self):
        r = _run_py("import utils; print(utils.DATA_DIR)", {"TRADER_DATA_DIR": ""})
        assert r.returncode == 0, r.stderr
        assert str(REPO) in r.stdout

    def test_data_dir_honours_env_var(self, tmp_path):
        r = _run_py("import utils; print(utils.DATA_DIR)", {"TRADER_DATA_DIR": str(tmp_path)})
        assert r.returncode == 0, r.stderr
        assert str(tmp_path) in r.stdout

    def test_state_dirs_follow_data_dir(self, tmp_path):
        """Enumerated explicitly — an unconverted path IS the failure mode.

        The income-generator ledger, the market baseline and the strategy dir
        were each missed by an earlier pass, and were invisible while this list
        was shorter.
        """
        code = (
            "import utils, scheduler, portfolio, earnings, income_generator, strategy;"
            "print(utils.LOG_DIR); print(utils.RECS_DIR); print(utils.SNAPSHOTS_DIR);"
            "print(scheduler._SNAPSHOT_DIR); print(scheduler._PID_FILE);"
            "print(scheduler._BASELINE_FILE);"
            "print(portfolio.SNAPSHOT_DIR); print(earnings.CACHE_DIR);"
            "print(income_generator._SNAPSHOT_DIR);"
            "print(strategy._get_strategy_dir())"
        )
        r = _run_py(code, {"TRADER_DATA_DIR": str(tmp_path)})
        assert r.returncode == 0, r.stderr
        lines = [l for l in r.stdout.strip().splitlines() if l]
        assert len(lines) == 10, f"expected 10 state paths, got {len(lines)}: {lines}"
        for line in lines:
            assert line.startswith(str(tmp_path)), f"{line} is not under the data dir"

    def test_no_module_writes_state_next_to_the_code(self, tmp_path):
        """Catch-all so a *new* unconverted path fails without updating a list.

        Scans every already-imported project module rather than a hardcoded
        list — an earlier version enumerated five modules and therefore missed
        a planted leak in strategy.py, which is exactly the class of bug it
        claims to catch.  Also coerces str, since _BASELINE_FILE is a str.

        It still cannot see paths built lazily inside functions; the explicit
        enumeration above covers those.
        """
        code = (
            "import pathlib, sys, utils\n"
            # Import the whole surface so module-level paths are materialised.
            "import scheduler, portfolio, earnings, income_generator, strategy\n"
            "import emailer, report_emailer, reporter, trader, collar, setup_wizard\n"
            "base = str(utils.BASE_DIR)\n"
            "bad = []\n"
            "for name, mod in list(sys.modules.items()):\n"
            "    f = getattr(mod, '__file__', None)\n"
            "    if not f or not f.startswith(base):\n"
            "        continue\n"
            "    for k, v in list(vars(mod).items()):\n"
            "        if not isinstance(v, (pathlib.Path, str)):\n"
            "            continue\n"
            "        sv = str(v)\n"
            "        if not sv.startswith(base):\n"
            "            continue\n"
            "        if any(s in sv for s in ('/logs','/snapshots','/recommendations','/cache','.pid')):\n"
            "            bad.append(f'{name}.{k}={sv}')\n"
            "print('\\n'.join(sorted(set(bad))))\n"
        )
        r = _run_py(code, {"TRADER_DATA_DIR": str(tmp_path)})
        assert r.returncode == 0, r.stderr
        leaked = [l for l in r.stdout.strip().splitlines() if l]
        assert not leaked, f"state paths still resolving next to the code: {leaked}"

    def test_config_stays_with_the_code_not_the_data_volume(self, tmp_path):
        """config.yaml ships in the image; it must not be looked for on the volume."""
        r = _run_py("import utils; print(utils.CONFIG_FILE)", {"TRADER_DATA_DIR": str(tmp_path)})
        assert r.returncode == 0, r.stderr
        assert str(REPO) in r.stdout


class TestPidLock:
    """The lock must release on any death, including SIGKILL.

    Under tini the app process gets the same PID on every container start, and
    atexit does not run on SIGKILL / OOM-kill / the watchdog's os._exit(1). An
    existence + os.kill(pid, 0) check therefore matches the *new* process
    against its own stale PID file and exits — turning a hang into a permanent
    crash loop under `restart: unless-stopped`.
    """

    def test_stale_file_with_our_own_pid_does_not_block_startup(self, tmp_path, monkeypatch):
        pid_file = tmp_path / "scheduler.pid"
        pid_file.write_text(str(os.getpid()))   # exactly the tini/PID-reuse case
        monkeypatch.setattr(scheduler, "_PID_FILE", pid_file)
        try:
            scheduler._acquire_pid_lock()       # must not SystemExit
            assert pid_file.read_text().strip() == str(os.getpid())
        finally:
            # Release the lock this test acquired in the pytest process.
            # Leaving it held leaks an fd for the rest of the session.
            if scheduler._pid_lock_handle is not None:
                scheduler._pid_lock_handle.close()
                scheduler._pid_lock_handle = None

    def test_lock_is_released_when_holder_is_sigkilled(self, tmp_path):
        """A killed holder must not leave the lock held."""
        pid_file = tmp_path / "scheduler.pid"
        holder = subprocess.Popen(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0,{str(REPO)!r});"
             f"import scheduler, pathlib, time;"
             f"scheduler._PID_FILE = pathlib.Path({str(pid_file)!r});"
             f"scheduler._acquire_pid_lock(); print('locked', flush=True); time.sleep(60)"],
            stdout=subprocess.PIPE, text=True,
        )
        assert holder.stdout.readline().strip() == "locked"

        holder.kill()          # SIGKILL — no atexit, file survives
        holder.wait(timeout=10)
        assert pid_file.exists(), "precondition: stale file remains after SIGKILL"

        # A fresh acquirer must succeed against that stale file.
        r = subprocess.run(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0,{str(REPO)!r});"
             f"import scheduler, pathlib;"
             f"scheduler._PID_FILE = pathlib.Path({str(pid_file)!r});"
             f"scheduler._acquire_pid_lock(); print('acquired')"],
            capture_output=True, text=True, timeout=30,
        )
        assert r.returncode == 0, f"crash-loop regression: {r.stderr}"
        assert "acquired" in r.stdout

    def test_second_live_instance_is_refused(self, tmp_path):
        pid_file = tmp_path / "scheduler.pid"
        holder = subprocess.Popen(
            [sys.executable, "-c",
             f"import sys; sys.path.insert(0,{str(REPO)!r});"
             f"import scheduler, pathlib, time;"
             f"scheduler._PID_FILE = pathlib.Path({str(pid_file)!r});"
             f"scheduler._acquire_pid_lock(); print('locked', flush=True); time.sleep(60)"],
            stdout=subprocess.PIPE, text=True,
        )
        try:
            assert holder.stdout.readline().strip() == "locked"
            r = subprocess.run(
                [sys.executable, "-c",
                 f"import sys; sys.path.insert(0,{str(REPO)!r});"
                 f"import scheduler, pathlib;"
                 f"scheduler._PID_FILE = pathlib.Path({str(pid_file)!r});"
                 f"scheduler._acquire_pid_lock()"],
                capture_output=True, text=True, timeout=30,
            )
            assert r.returncode == 1, "a concurrent instance must be refused"
        finally:
            holder.kill(); holder.wait(timeout=10)


class TestLivenessDuringJobs:
    """A running job is not a dead scheduler.

    _last_tick is stamped before schedule.run_pending(), and jobs run
    synchronously in that thread for up to _WATCHDOG_CC_PIPELINE (3600s). With a
    90s liveness window a perfectly healthy scheduler reported 503 for the whole
    pipeline — and Phase 4 wires /healthz to an uptime check.
    """

    def test_alive_while_a_long_job_runs(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_TICK_SECONDS", 0.05)
        started = threading.Event()

        def slow_job():
            started.set()
            time.sleep(1.5)

        scheduler.schedule.every(1).seconds.do(slow_job)
        t = threading.Thread(target=scheduler.run_loop, daemon=True)
        try:
            t.start()
            assert started.wait(timeout=10), "job never started"
            # Backdate the heartbeat well beyond the window; the job is still running.
            scheduler._last_tick = time.monotonic() - 10_000
            assert scheduler.scheduler_alive(max_age_secs=90.0) is True, \
                "reported dead while a job was legitimately executing"
        finally:
            scheduler.request_shutdown()
            t.join(timeout=10)
            scheduler.schedule.clear()

    def test_dead_when_idle_and_heartbeat_stale(self):
        scheduler._job_active = False
        scheduler._last_tick = time.monotonic() - 10_000
        assert scheduler.scheduler_alive(max_age_secs=90.0) is False

    def test_heartbeat_cleared_when_loop_exits(self):
        """Otherwise /healthz reports 200 for up to 90s after the scheduler is gone."""
        t = threading.Thread(target=scheduler.run_loop, daemon=True)
        t.start()
        time.sleep(0.2)
        scheduler.request_shutdown()
        t.join(timeout=10)
        assert scheduler.scheduler_alive() is False


class TestCheckEnvOptionalCredentials:
    """FINNHUB_API_KEY has a documented fallback and must stay optional.

    earnings.py logs "not set — skipping Finnhub" and falls through to other
    providers. Requiring it locked every command — including read-only --status
    — behind a credential the code itself treats as optional.
    """

    REQUIRED = {
        "ROBINHOOD_USERNAME": "u@example.com",
        "ROBINHOOD_PASSWORD": "pw",
        "ROBINHOOD_TOTP_SEED": "JBSWY3DPEHPK3PXP",
        "RESEND_API_KEY": "re_x",
        "RESEND_FROM": "Trader <t@example.com>",
    }

    def _check_env_with(self, env: dict):
        code = (
            "import main, sys;"
            "main.Path = __import__('pathlib').Path;"
            "main.check_env(); print('OK')"
        )
        return _run_py(code, {**{k: "" for k in ("FINNHUB_API_KEY",)}, **env,
                              "TRADER_SKIP_ENV_FILE_PREFLIGHT": "1"})

    def test_passes_without_finnhub_key(self):
        r = self._check_env_with(self.REQUIRED)
        assert r.returncode == 0, f"optional credential was treated as required: {r.stdout}{r.stderr}"

    def test_still_fails_without_a_truly_required_key(self):
        partial = dict(self.REQUIRED)
        del partial["ROBINHOOD_PASSWORD"]
        r = self._check_env_with({**partial, "ROBINHOOD_PASSWORD": ""})
        assert r.returncode == 1
        assert "ROBINHOOD_PASSWORD" in r.stdout


class TestLiveTradingGate:
    """--serve must not place live orders unless explicitly opted in.

    The duplicate-instance flock is scoped to DATA_DIR, so a container
    (/data/scheduler.pid) cannot see a host scheduler (<repo>/scheduler.pid) —
    and neither can any per-day dedupe. Bringing the stack up while the laptop
    scheduler runs would otherwise mean two live pipelines on one account.
    """

    def teardown_method(self):
        scheduler.set_force_dry_run(False)

    @staticmethod
    def _probe_effective_dry_run(force: bool, arg: bool) -> bool:
        """Return the dry_run value run_pipeline actually operates under.

        Behavioural, not a source grep.  An earlier version asserted
        `"_force_dry_run" in inspect.getsource(run_pipeline)`, which still
        passed with the `dry_run = True` line deleted — the identifier survives
        in the `if` and the comment.  Guarding a replacement safety mechanism
        with an assertion that cannot detect its removal is the exact mistake
        this PR keeps repeating.

        Reads the flag through run_pipeline's own early return:
        `if not dry_run and not _is_trading_day(): return`.  With
        _is_trading_day False, getting past it proves dry_run was flipped.
        """
        class _Reached(BaseException):   # BaseException: run_pipeline catches Exception
            pass

        scheduler.set_force_dry_run(force)
        try:
            # write_run_log is patched on the SCHEDULER module, not utils:
            # scheduler does `from utils import write_run_log`, binding the name
            # at import, so patching utils has no effect. An earlier version of
            # this test (and tests/test_emailer_consolidation.py) missed that and
            # wrote real run_<date>.json files into the developer's checkout,
            # overwriting the day's actual run record.
            with mock.patch.object(scheduler, "_is_trading_day", return_value=False), \
                 mock.patch.object(scheduler, "load_config", return_value={}), \
                 mock.patch.object(scheduler, "write_run_log"), \
                 mock.patch.object(scheduler, "_close_yfinance_dbs",
                                   create=True, side_effect=_Reached):
                try:
                    scheduler.run_pipeline(dry_run=arg)
                    return "returned-early"
                except _Reached:
                    return "proceeded"
        finally:
            scheduler.set_force_dry_run(False)

    def test_live_call_proceeds_when_gate_is_off(self):
        """Ungated, a live call is not blocked by the safety check."""
        assert self._probe_effective_dry_run(force=False, arg=False) == "returned-early", \
            "with _is_trading_day False the pipeline should hit its own early return"

    def test_gate_refuses_a_live_call(self):
        """Refusal, not downgrade — a downgraded dry run still logs into Robinhood.

        Fails if the safety `return` is removed, unlike the source-grep
        assertion this replaced.
        """
        scheduler.set_force_dry_run(True)
        try:
            # _is_trading_day doubles as a tripwire: if the gate ever stops
            # refusing, this raises instead of executing the real pipeline body
            # (an earlier version wrote a real email preview when mutated).
            class _Reached(BaseException):
                pass

            with mock.patch.object(scheduler, "_is_trading_day",
                                   side_effect=_Reached) as trading_day, \
                 mock.patch.object(scheduler, "write_run_log") as run_log:
                try:
                    scheduler.run_pipeline(dry_run=False)
                except _Reached:
                    pass
            assert not trading_day.called, "must refuse before any pipeline work"
            assert not run_log.called, "a refused run must not record a run log"
        finally:
            scheduler.set_force_dry_run(False)

    def test_explicit_dry_run_is_still_allowed_when_gated(self):
        """An operator asking for a preview is a deliberate act, so it proceeds.

        (`run_pipeline`'s own trading-day early return is skipped when
        dry_run=True, which is why this reaches the pipeline body.)
        """
        assert self._probe_effective_dry_run(force=True, arg=True) == "proceeded"

    def test_account_contacting_jobs_are_skipped_when_not_authoritative(self):
        """Orders are not the only exposure.

        A second Robinhood login uses a different device token (compose sets
        HOME=/data/home) which triggers device verification and hangs, breaking
        the authoritative instance's session; the report jobs would also email
        duplicates every day.
        """
        scheduler.set_force_dry_run(True)
        try:
            with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
                 mock.patch.object(scheduler, "_wait_for_network", return_value=True), \
                 mock.patch("portfolio.pull_daily_robinhood_snapshot") as pull, \
                 mock.patch("reporter.build_options_report") as rpt:
                scheduler.job_daily_portfolio_pull()
                scheduler.job_daily_options_report()
                scheduler.job_weekly_options_report()
            pull.assert_not_called()
            rpt.assert_not_called()
        finally:
            scheduler.set_force_dry_run(False)

    def test_gate_defaults_on_in_a_container_without_opt_in(self):
        """Inherited by every process in the container, not just --serve —
        otherwise `docker exec … --run` places the orders --serve refuses to."""
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": "/tmp", "TRADER_ALLOW_LIVE": ""})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "True"

    def test_gate_defaults_off_outside_a_container(self):
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": "", "TRADER_ALLOW_LIVE": ""})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "False"

    def test_opt_in_re_enables_live(self):
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": "/tmp", "TRADER_ALLOW_LIVE": "1"})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "False"

    def test_serve_defaults_to_dry_run(self):
        with mock.patch("main.check_env"), \
             mock.patch.dict(os.environ, {"TRADER_ALLOW_LIVE": ""}, clear=False), \
             mock.patch("scheduler.set_force_dry_run") as m, \
             mock.patch("scheduler.start_scheduler"), \
             mock.patch("scheduler.install_signal_handlers"), \
             mock.patch("threading.Thread"), \
             mock.patch("uvicorn.run"):
            import main
            main.cmd_serve()
        m.assert_called_once_with(True)

    def test_serve_allows_live_when_opted_in(self):
        with mock.patch("main.check_env"), \
             mock.patch.dict(os.environ, {"TRADER_ALLOW_LIVE": "1"}, clear=False), \
             mock.patch("scheduler.set_force_dry_run") as m, \
             mock.patch("scheduler.start_scheduler"), \
             mock.patch("scheduler.install_signal_handlers"), \
             mock.patch("threading.Thread"), \
             mock.patch("uvicorn.run"):
            import main
            main.cmd_serve()
        m.assert_called_once_with(False)


class TestSuiteHasNoStateSideEffects:
    """The test suite must never write real trading state.

    Twice in this PR a test wrote a real logs/run_<date>.json into the
    developer's checkout — once via a probe that called run_pipeline unpatched,
    and once via a pre-existing @patch("utils.write_run_log") that never took
    effect because scheduler.py does `from utils import write_run_log`, binding
    the name at import.

    The dated run log is not inert: _has_pipeline_run_today() reads it, so a
    stray dry_run=false marker suppresses every live catch-up for the rest of
    that day, and a stray dry_run=true one (after the liveness change) makes the
    catch-up fire repeatedly.
    """

    def test_scheduler_binds_write_run_log_directly(self):
        """Guards the patch target itself.

        If this ever becomes False, `@patch("scheduler.write_run_log")` silently
        stops working and the side effect returns.
        """
        assert "write_run_log" in vars(scheduler), (
            "scheduler must bind write_run_log at import for the tests' patch "
            "target to be correct"
        )

    def test_no_test_module_patches_utils_write_run_log(self):
        """utils.write_run_log is the wrong target and fails silently."""
        import re
        # Match an actual patch target, not prose mentioning the name (this
        # file's own docstring explains the trap and must not self-report).
        pattern = re.compile(r"""patch\w*\(\s*['"]utils\.write_run_log['"]""")
        this_file = Path(__file__).name
        offenders = [
            path.name
            for path in (REPO / "tests").glob("test_*.py")
            # Skip self: the docstring above quotes the offending decorator
            # verbatim in order to explain it.
            if path.name != this_file and pattern.search(path.read_text())
        ]
        assert not offenders, (
            f"{offenders} patch utils.write_run_log, which scheduler does not "
            "use — patch scheduler.write_run_log instead"
        )


class TestGateCoversEveryEntryPoint:
    """The gate is checked by iterating the entry-point tables, not by listing
    call sites by hand.

    Rounds 3-7 all shared one shape: a safety check added per call site, with a
    test that enumerated call sites by hand. Both lists drifted from reality —
    the gate reached 3 of 6 jobs and 2 of 13 commands while the suite stayed
    green. These tests derive their coverage from the code's own tables, so a
    newly added job or command is covered without anyone remembering.
    """

    ALL_JOBS = (
        "job_daily_portfolio_pull",
        "job_early_pipeline",
        "job_daily_pipeline",
        "job_daily_options_report",
        "job_weekly_options_report",
        "job_market_move_check",
    )

    def teardown_method(self):
        scheduler.set_force_dry_run(False)

    def test_every_scheduled_job_is_gated(self):
        """Loops the job table; catches a gate deleted from ANY job."""
        scheduler.set_force_dry_run(True)
        for name in self.ALL_JOBS:
            job = getattr(scheduler, name)
            with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
                 mock.patch.object(scheduler, "_wait_for_network", return_value=True), \
                 mock.patch.object(scheduler, "run_pipeline") as rp, \
                 mock.patch.object(scheduler, "_check_market_move") as move, \
                 mock.patch.object(scheduler, "_capture_market_baseline") as base, \
                 mock.patch("portfolio.pull_daily_robinhood_snapshot") as pull, \
                 mock.patch("reporter.build_options_report") as rpt:
                job()
            for label, m in (("run_pipeline", rp), ("_check_market_move", move),
                             ("_capture_market_baseline", base),
                             ("RH snapshot", pull), ("report build", rpt)):
                assert not m.called, f"{name} reached {label} while gated"

    def test_job_table_matches_the_scheduler_module(self):
        """Keeps ALL_JOBS honest — a new job_* function must be added here."""
        actual = {n for n in dir(scheduler)
                  if n.startswith("job_") and callable(getattr(scheduler, n))}
        assert actual == set(self.ALL_JOBS), (
            f"job table drifted: module has {sorted(actual)}, "
            f"test covers {sorted(self.ALL_JOBS)}"
        )

    def test_every_account_touching_command_is_gated(self):
        """Reads main's own dest table, so it cannot enumerate a stale subset.

        An earlier version derived the list from the mutually-exclusive argparse
        group and therefore missed --roll and --show, which are declared outside
        it — the same hand-derived-list failure this class exists to end.
        """
        import main
        scheduler.set_force_dry_run(True)
        parser = main.build_parser()
        dests = main._PRIMARY_COMMAND_DESTS

        allowed = []
        gated = []
        for dest in dests:
            argv = _argv_for(parser, dest)
            if argv is None:
                continue
            try:
                args = parser.parse_args(argv)
            except SystemExit:
                continue
            blocked = main._gated_command(args, dests)
            (allowed if blocked is None else gated).append(dest)

        for dest in allowed:
            assert dest in main._NON_ACCOUNT_COMMANDS, (
                f"--{dest} runs on a non-authoritative instance but is not "
                "declared safe in _NON_ACCOUNT_COMMANDS"
            )
        # Sanity: the account-touching majority must actually be refused.
        assert len(gated) >= len(dests) - len(main._NON_ACCOUNT_COMMANDS), \
            f"only {len(gated)} of {len(dests)} commands gated"

    def test_declared_safe_commands_all_exist(self):
        """A typo in _NON_ACCOUNT_COMMANDS would silently gate nothing."""
        import main
        unknown = set(main._NON_ACCOUNT_COMMANDS) - set(main._PRIMARY_COMMAND_DESTS)
        assert not unknown, f"_NON_ACCOUNT_COMMANDS names non-existent commands: {unknown}"

    def test_gate_is_off_when_authoritative(self):
        import main
        scheduler.set_force_dry_run(False)
        parser = main.build_parser()
        args = parser.parse_args(["--run"])
        assert main._gated_command(args, ["run"]) is None


def _argv_for(parser, dest):
    """Minimal argv selecting `dest`, or None if it cannot be built."""
    action = next((a for a in parser._actions if a.dest == dest), None)
    if action is None or not action.option_strings:
        return None
    flag = action.option_strings[0]
    if action.nargs == 0 or isinstance(action.const, bool):
        return [flag]
    if action.nargs == "?":
        return [flag]
    return [flag, "TSLA"]


class TestDispatchActuallyEnforcesTheGate:
    """Testing _gated_command's logic is not enough — dispatch must CALL it.

    A first version of these tests exercised the helper directly and passed even
    with the dispatch call site deleted, and passed again with an order command
    mis-declared safe. Both mutations are now caught.
    """

    ORDER_COMMANDS = (
        ["--run"], ["--ccs", "TSLA", "--add"], ["--pcs", "TSLA", "--add"],
        ["--pds", "TSLA", "--add"], ["--cds", "TSLA", "--add"],
        ["--buy", "TSLA"], ["--roll", "TSLA"], ["--short", "TSLA"],
        ["--optimize"], ["--report"], ["--pull-portfolio"],
        ["--generate-income", "TSLA"], ["--auto-defense"],
        ["--insurance-optimize"], ["--collar", "TSLA", "--add"],
    )

    def teardown_method(self):
        scheduler.set_force_dry_run(False)

    @pytest.mark.parametrize("argv", ORDER_COMMANDS, ids=lambda a: a[0])
    def test_order_command_exits_before_doing_anything(self, argv, capsys):
        """Runs main.main() for real: catches a deleted dispatch call site."""
        import main
        scheduler.set_force_dry_run(True)
        with mock.patch.object(main, "check_env") as env, \
             mock.patch.object(sys, "argv", ["main.py", *argv]):
            with pytest.raises(SystemExit) as exc:
                main.main()
        assert exc.value.code == 1, f"{argv} should exit 1 when gated"
        assert "not authoritative" in capsys.readouterr().out
        assert not env.called, f"{argv} began work before the gate refused it"

    def test_safe_commands_are_not_refused(self, capsys):
        """--status must still work in a gated container, or operators are blind."""
        import main
        scheduler.set_force_dry_run(True)
        with mock.patch.object(main, "cmd_status") as run, \
             mock.patch.object(sys, "argv", ["main.py", "--status"]):
            main.main()
        run.assert_called_once()
        assert "not authoritative" not in capsys.readouterr().out

    def test_order_commands_run_normally_when_authoritative(self):
        import main
        scheduler.set_force_dry_run(False)
        with mock.patch.object(main, "cmd_run") as run, \
             mock.patch.object(sys, "argv", ["main.py", "--run"]):
            main.main()
        run.assert_called_once()
