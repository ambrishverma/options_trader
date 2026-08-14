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
    """Run a snippet in a fresh interpreter so module-level paths re-evaluate.

    A value of None UNSETS the variable. Merging a "" instead is not the same
    thing and never was: `ENV TRADER_DATA_DIR=` in a Dockerfile sets the
    variable to the empty string, and telling those two cases apart is the
    whole point of the gate's `is not None` test.
    """
    env = {**os.environ, **env_extra}
    for k, v in env_extra.items():
        if v is None:
            env.pop(k, None)
    return subprocess.run(
        [sys.executable, "-c", code],
        env=env,
        capture_output=True, text=True, cwd=str(REPO),
    )


class TestDataDir:
    """State must be redirectable to the mounted volume.

    Without this the compose `./local-data:/data` mount is inert: every module
    computes BASE_DIR = Path(__file__).parent, so state lands in the image
    layer. After a rebuild logs/run_log.jsonl is gone, _has_pipeline_run_today()
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

        Neither this nor the enumeration above sees paths built lazily INSIDE
        functions — the six on-demand HTML previews in scheduler.py, plus
        emailer.py and report_emailer.py. Those write preview HTML only, never
        trading state, so the gap is bounded; it is stated here rather than
        implied away.
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

        The tripwire is patched WITHOUT create=True on purpose: the attribute
        must already exist, so renaming it raises AttributeError rather than
        silently letting the real pipeline body run (which reaches _ins_login in
        Steps 6h/6i/7, where auth.login is not patched by this test).

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
                                   side_effect=_Reached):
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

    def test_gate_defaults_on_in_a_container_without_opt_in(self, tmp_path):
        """Inherited by every process in the container, not just --serve —
        otherwise `docker exec … --run` places the orders --serve refuses to."""
        # tmp_path, not "/tmp": the latter creates /tmp/logs, /tmp/snapshots and
        # /tmp/recommendations on the developer's machine via _ensure_dir.
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": str(tmp_path), "TRADER_ALLOW_LIVE": ""})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "True"

    def test_gate_defaults_off_outside_a_container(self):
        """Truly unset — the laptop. The gate must stay OFF or every local
        --run silently becomes a no-op."""
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": None, "TRADER_ALLOW_LIVE": ""})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "False"

    def test_an_empty_data_dir_still_arms_the_gate(self):
        """`ENV TRADER_DATA_DIR=` sets the variable to "". Under the old
        bool(getenv(...)) test that read as "not a container", so an image built
        with an emptied ENV line would trade live from the cloud VM while the
        laptop was also authoritative — two instances placing orders."""
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": "", "TRADER_ALLOW_LIVE": ""})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == "True", (
            "an empty TRADER_DATA_DIR must arm the gate, not disarm it"
        )

    def test_opt_in_re_enables_live(self, tmp_path):
        r = _run_py("import scheduler; print(scheduler._force_dry_run)",
                    {"TRADER_DATA_DIR": str(tmp_path), "TRADER_ALLOW_LIVE": "1"})
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


class _BrokerageReached(BaseException):
    """Sentinel raised by the booby-trap in _trap_the_brokerage().

    Deliberately a BaseException rather than an Exception: run_pipeline, the
    job wrappers and reporter all catch Exception broadly, and a swallowed
    sentinel would turn a genuine breach into a silently passing test.
    """


def _brokerage_half(value):
    """Which half of the boundary a callable belongs to, by ORIGIN not by name.

    Returns "robin_stocks", "auth", or None.  The two halves are counted
    separately because they can fail independently: deleting the robin_stocks
    branch dropped the armed bindings from ~1500 to 2 while every test,
    positive control included, still passed — silently degrading the probe
    back into the maintained-by-hand entry-point list it replaced.  A
    robin_stocks upgrade that changes how __module__ is set would do the same.
    """
    origin = getattr(value, "__module__", "") or ""
    if origin.startswith("robin_stocks"):
        return "robin_stocks"
    if origin == "auth" and getattr(value, "__name__", "") in {
        "login", "validate_credentials",
    }:
        return "auth"
    return None


class _Trap:
    """What _trap_the_brokerage installed: how much it armed, and what it caught."""

    def __init__(self, counts, breaches):
        self.counts = counts
        self.breaches = breaches


def _assert_trap_armed(trap):
    """Both halves must be live, or the trap silently covers only one."""
    for half, n in trap.counts.items():
        assert n > 0, (
            f"the brokerage trap armed no {half!r} bindings ({trap.counts}) — "
            "it would pass vacuously on any path that reaches only that half"
        )


def _assert_no_thread_breach(trap, label):
    """A breach on a worker thread is still a breach."""
    assert not trap.breaches, (
        f"{label} reached the brokerage on a background thread "
        f"({len(trap.breaches)} time(s)) — pytest would otherwise downgrade "
        "this to a warning and pass"
    )


def _trap_the_brokerage(stack):
    """Patch the brokerage boundary so any path reaching it raises.

    Patching the boundary rather than a maintained-by-hand list of app-level
    entry points is the entire point: a new code path trips the trap no matter
    which helper it goes through or how it imports it.

    Project modules are patched as well as robin_stocks itself, because
    `from robin_stocks.robinhood import orders` (or `from auth import login`)
    at module scope binds its own reference, which patching the source module
    alone would miss.  Returns the number of bindings patched so callers can
    assert the trap is actually armed.
    """
    from types import ModuleType

    import auth  # noqa: F401 — ensures the module is in sys.modules to patch
    import robin_stocks.robinhood  # noqa: F401

    project = {q.stem for q in REPO.glob("*.py")}

    def boom(*_a, **_k):
        raise _BrokerageReached("reached the brokerage")

    counts = {"robin_stocks": 0, "auth": 0}
    for mod_name, module in list(sys.modules.items()):
        if module is None:
            continue
        if not (mod_name.startswith("robin_stocks") or mod_name in project):
            continue
        for attr, value in list(vars(module).items()):
            if attr.startswith("_") or isinstance(value, ModuleType):
                continue
            if not callable(value):
                continue
            half = _brokerage_half(value)
            if half:
                stack.enter_context(mock.patch.object(module, attr, boom))
                counts[half] += 1

    # A BaseException raised on a NON-test thread never reaches the caller —
    # pytest downgrades it to PytestUnhandledThreadExceptionWarning, which does
    # not fail the run.  cmd_serve hands run_loop to a real threading.Thread,
    # which is the case this covers.
    #
    # SCOPE, precisely: threading.excepthook fires for threading.Thread only.
    # It does NOT fire for concurrent.futures workers, which is this repo's
    # usual offload idiom (trader.py:101/:128, portfolio.py:274,
    # options_chain.py:265).  Those are covered anyway wherever the caller
    # retrieves .result(), because that re-raises the BaseException on the
    # calling thread — trader._rh_call does exactly this.  The residual gap is
    # a submit() whose result is never retrieved: _rh_call's timeout path calls
    # pool.shutdown(wait=False), so a trapped call reached by the abandoned
    # worker afterwards is recorded nowhere.  Do not read this hook as covering
    # executors.
    breaches = []
    previous = threading.excepthook

    def _hook(args):
        if args.exc_type is not None and issubclass(args.exc_type, _BrokerageReached):
            breaches.append(args)
            return
        previous(args)

    stack.enter_context(mock.patch.object(threading, "excepthook", _hook))
    return _Trap(counts, breaches)


# There is no table of brokerage function names.  The rule is derived: any
# use of a name imported from robin_stocks is a brokerage touch, and anything
# that transitively reaches such a use is too.  A named table was carried here
# for several rounds (auth.login, auth.validate_credentials, trader.place_*)
# until it was measured to be redundant — each of those four is already found
# through robin_stocks on its own merits, so the table only added a list that
# could be silently shortened.  test_the_functions_that_used_to_be_named_sinks
# pins that redundancy, so if a refactor ever breaks it we hear about it.


def _project_modules(root=REPO):
    return {q.stem for q in root.glob("*.py")}


_TREE_CACHE = {}


def _tree_for(mod_name, root=REPO):
    key = (str(root), mod_name)
    if key not in _TREE_CACHE:
        import ast
        f = root / f"{mod_name}.py"
        _TREE_CACHE[key] = ast.parse(f.read_text()) if f.exists() else None
    return _TREE_CACHE[key]


def _import_map(tree):
    """local name -> (module, original name), from imports at EVERY level.

    The walk that this replaced looked only at ImportFrom nodes inside the
    target function, so a module-level `from auth import login` was invisible.
    """
    import ast
    out = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for a in node.names:
                out[a.asname or a.name] = (node.module, a.name)
        elif isinstance(node, ast.Import):
            for a in node.names:
                out[a.asname or a.name.split(".")[0]] = (a.name, None)
    return out


def _references(fn):
    """Every name a function could reach other code through, as (name, attr).

    (name, None)  a bare reference — a from-imported callee, or a callee
                  defined in the same module.
    (name, attr)  an attribute call on a bare name, e.g. `trader.fetch(...)`.
                  This carries the callee that the bare form loses: `import X`
                  binds only ("X", None), so without the pair the edge is
                  dropped and a declared-safe command written that way reaches
                  the brokerage unnoticed.

    One traversal.  These were two passes with subtly different resolution
    rules, which is how the attribute form came to be missing for a while.
    """
    import ast

    refs = set()
    for n in ast.walk(fn):
        if isinstance(n, ast.Name):
            refs.add((n.id, None))
        elif isinstance(n, ast.Attribute):
            # The attribute alone, in case it names a from-imported symbol.
            refs.add((n.attr, None))
            base = n
            while isinstance(base, ast.Attribute):
                base = base.value
            if isinstance(base, ast.Name):
                refs.add((base.id, None))
                if base is n.value:
                    refs.add((base.id, n.attr))
    return refs


def _func_in(tree, name):
    import ast
    return next((n for n in ast.walk(tree)
                 if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                 and n.name == name), None)


def _is_gated_job(mod_name, func_name):
    """Scheduler jobs are a recursion boundary, not a leak.

    --serve and --schedule are declared safe because they only START the loop.
    They do reach the brokerage in the sense that the loop's jobs eventually
    authenticate — but every job carries its own _skip_unless_authoritative
    guard, and test_every_scheduled_job_is_gated above verifies each one
    individually (mutation confirms it fails when any single guard is removed).
    Recursing past this point would report the gate's own design as a breach.
    """
    return mod_name == "scheduler" and func_name.startswith("job_")


def _static_reaches_brokerage(mod_name, func_name, seen=None, depth=0, root=REPO):
    """Transitively decide whether mod.func can reach the brokerage.

    Follows module-level imports and same-module callees, both of which the
    previous version missed.  Returns a human-readable path or None.
    """
    if depth > 6:
        return None
    seen = seen if seen is not None else set()
    if (mod_name, func_name) in seen:
        return None
    seen.add((mod_name, func_name))

    if _is_gated_job(mod_name, func_name):
        return None

    tree = _tree_for(mod_name, root)
    if tree is None:
        return None
    fn = _func_in(tree, func_name)
    if fn is None:
        return None

    project = _project_modules(root)
    imports = _import_map(tree)

    for name, attr in sorted(_references(fn), key=lambda r: (r[0], r[1] or "")):
        target = imports.get(name)
        if target:
            tmod, tname = target
            # Derived, not listed: any use of a name imported from robin_stocks
            # is a brokerage touch, whichever function it happens to be.
            if tmod.startswith("robin_stocks"):
                return f"{mod_name}.{func_name} -> {tmod}.{attr or tname or name}"
            # `from X import y` supplies the callee as tname; `import X` plus
            # `X.y()` supplies it as attr.
            callee = tname or attr
            if tmod in project and callee:
                deeper = _static_reaches_brokerage(tmod, callee, seen, depth + 1, root)
                if deeper:
                    return f"{mod_name}.{func_name} -> {deeper}"
        elif attr is None and name != func_name and _func_in(tree, name) is not None:
            # Callee defined in this same module.
            deeper = _static_reaches_brokerage(mod_name, name, seen, depth + 1, root)
            if deeper:
                return f"{mod_name}.{func_name} -> {deeper}"
    return None


def _dispatch_handlers():
    """dest -> [handler names], read from main()'s own dispatch chain."""
    import ast
    tree = _tree_for("main")
    handlers = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        dests = [n.attr for n in ast.walk(node.test)
                 if isinstance(n, ast.Attribute)
                 and isinstance(n.value, ast.Name) and n.value.id == "args"]
        # node.body only — walking node would descend into the whole elif chain
        # below and attribute every downstream handler to this branch.
        calls = [n.func.id
                 for stmt in node.body
                 for n in ast.walk(stmt)
                 if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                 and n.func.id.startswith("cmd_")]
        for d in dests:
            if calls:
                handlers.setdefault(d, []).extend(calls)
    return handlers


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
        """Loops the job table; catches a gate deleted from ANY job.

        Two halves, because the named half alone is not enough.  The five
        mocks below are fast and name the sink in the failure message, but
        they are a hand-maintained list: a job added later that authenticates
        through some other path satisfies all five `not called` assertions.
        The brokerage trap is the derived half and catches that case.

        This matters more here than in an ordinary test.  _is_gated_job()
        makes this test the entire justification for treating scheduler jobs
        as a recursion boundary in the static safe-command walk — which is in
        turn the only reason --serve and --schedule can be declared
        account-safe.  If this test cannot fail, that escape hatch is
        load-bearing on nothing.
        """
        import contextlib

        scheduler.set_force_dry_run(True)
        for name in self.ALL_JOBS:
            job = getattr(scheduler, name)
            with contextlib.ExitStack() as stack:
                trap = _trap_the_brokerage(stack)
                _assert_trap_armed(trap)
                stack.enter_context(
                    mock.patch.object(scheduler, "_is_trading_day", return_value=True))
                stack.enter_context(
                    mock.patch.object(scheduler, "_wait_for_network", return_value=True))
                rp = stack.enter_context(mock.patch.object(scheduler, "run_pipeline"))
                move = stack.enter_context(
                    mock.patch.object(scheduler, "_check_market_move"))
                base = stack.enter_context(
                    mock.patch.object(scheduler, "_capture_market_baseline"))
                pull = stack.enter_context(
                    mock.patch("portfolio.pull_daily_robinhood_snapshot"))
                rpt = stack.enter_context(mock.patch("reporter.build_options_report"))
                try:
                    job()
                except _BrokerageReached as exc:
                    raise AssertionError(
                        f"{name} reached the brokerage while gated: {exc}"
                    ) from None
                _assert_no_thread_breach(trap, name)
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

    # ---- the safe-set contract, verified at runtime ----------------------
    #
    # This replaced a static AST walk over each safe command's handler, which
    # searched for a hand-listed set of brokerage function NAMES and followed
    # only `from X import Y` written INSIDE the handler.  Two holes left it
    # unable to fail on a real breach:
    #
    #   - the name list omitted `login` — the very call the rationale for
    #     gating --dry-run rests on (run_pipeline Steps 6h/6i/7);
    #   - module-level imports and same-module callees were invisible, so
    #     declaring --report, --optimize or --generate-income safe left the
    #     suite green even though all three authenticate and the latter two
    #     submit live orders.
    #
    # A runtime probe was rejected when the AST walk was written, on the
    # grounds that --setup is interactive and dies at its first prompt.
    # --setup is no longer in the safe set, so that objection no longer holds.

    @staticmethod
    def _argv_for(parser, dest):
        """Bare invocation for a dest, derived from the parser rather than listed."""
        import argparse

        for act in parser._actions:
            if act.dest != dest or not act.option_strings:
                continue
            opt = act.option_strings[0]
            if act.nargs == 0 or isinstance(act, argparse._StoreTrueAction):
                return [opt]
            if act.nargs == "?":
                return [opt]  # const supplies the value
            return [opt, "TSLA"]
        raise AssertionError(f"no parser option resolves dest {dest!r}")

    def _reaches_brokerage(self, dest, parser):
        """Run one command with the brokerage trapped; True if it got there."""
        import contextlib

        import main

        with contextlib.ExitStack() as stack:
            # cmd_serve calls scheduler.set_force_dry_run() on the real module,
            # so probing --serve leaves the gate armed for every later test in
            # the session (13 failures in test_scheduled_jobs.py when this was
            # missing).  Restored here rather than in each caller's teardown so
            # a new caller cannot forget.
            was_gated = scheduler._force_dry_run
            stack.callback(scheduler.set_force_dry_run, was_gated)
            trap = _trap_the_brokerage(stack)
            _assert_trap_armed(trap)
            # check_env() exits 1 when no .env is present, which on a clean
            # machine would end every command before it reached any real work.
            stack.enter_context(mock.patch.object(main, "check_env"))
            # --serve and --schedule block forever otherwise.  Each scheduled
            # job carries its own _skip_unless_authoritative guard and is
            # covered above, so stubbing the blocking call costs no coverage.
            stack.enter_context(mock.patch("scheduler.start_scheduler"))
            stack.enter_context(mock.patch("uvicorn.run"))
            # cmd_serve calls this directly; unstubbed it replaces pytest's
            # SIGINT handler for the rest of the session, so Ctrl-C stops
            # raising KeyboardInterrupt and a second one hits os._exit(130).
            stack.enter_context(mock.patch("scheduler.install_signal_handlers"))
            stack.enter_context(mock.patch.object(
                sys, "argv", ["main.py", *self._argv_for(parser, dest)]))
            try:
                main.main()
            except _BrokerageReached:
                return True
            except BaseException:
                # Failed for an unrelated reason (missing data, SystemExit).
                # Not a breach — and the positive control below is what proves
                # this branch is not quietly swallowing every run.
                pass
            # cmd_serve hands run_loop to a real threading.Thread, so a breach
            # there never propagates here; it would surface only as a pytest
            # warning.  Any recorded thread breach is still a breach.
            return bool(trap.breaches)
        return False

    def test_the_brokerage_trap_fires_on_a_command_that_authenticates(self):
        """Positive control: without it the probe below could pass vacuously.

        --report is deliberately NOT in the safe set and reaches auth.login
        through reporter.build_options_report.  A trap that cannot catch that
        could not catch a mis-declared safe command either.
        """
        import main

        scheduler.set_force_dry_run(False)
        assert self._reaches_brokerage("report", main.build_parser()), (
            "the trap did not fire on --report, which authenticates — the "
            "safe-command probe below is therefore not testing anything"
        )

    def test_no_safe_command_reaches_the_brokerage(self):
        """Everything declared account-safe must never touch Robinhood."""
        import main

        scheduler.set_force_dry_run(False)
        parser = main.build_parser()
        for dest in sorted(main._NON_ACCOUNT_COMMANDS):
            assert not self._reaches_brokerage(dest, parser), (
                f"--{dest.replace('_', '-')} is declared account-safe in "
                "_NON_ACCOUNT_COMMANDS but reaches the brokerage"
            )

    def test_no_safe_command_statically_reaches_the_brokerage(self):
        """Static companion to the runtime probe above — neither suffices alone.

        The probe only observes what a bare invocation actually executes, and
        two real paths stay out of its reach: `--generate-income` without
        `--add` stops at the preview, and `--auto-defense` exits early when no
        portfolio snapshot is on disk.  Both reach the brokerage under other
        conditions, so declaring either safe must fail here even though the
        probe alone stays green.

        Conversely this walk cannot see through a dynamic dispatch, which the
        probe does catch.  They cover different halves.
        """
        import main

        handlers = _dispatch_handlers()
        for dest in sorted(main._NON_ACCOUNT_COMMANDS):
            resolved = handlers.get(dest, [])
            assert resolved, (
                "cannot resolve a handler for declared-safe "
                f"--{dest.replace('_', '-')} — the audit would silently skip it"
            )
            for handler in resolved:
                path = _static_reaches_brokerage("main", handler)
                assert path is None, (
                    f"--{dest.replace('_', '-')} is declared account-safe but "
                    f"{handler}() reaches the brokerage: {path}"
                )

    def test_declared_safe_commands_all_exist(self):
        """A typo in _NON_ACCOUNT_COMMANDS would silently gate nothing."""
        import main
        dests = main._primary_command_dests(main.build_parser())
        unknown = set(main._NON_ACCOUNT_COMMANDS) - set(dests)
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
        # auth.login is rigged to raise so that if the gate ever stops refusing,
        # this test fails loudly instead of reaching live code. Without it a
        # regression run on a machine with a populated .env would attempt a real
        # login — and for --ccs/--pull-portfolio, real order submission — from the
        # test suite.
        boom = AssertionError("gate regression: reached the brokerage")
        with mock.patch.object(main, "check_env") as env, \
             mock.patch("auth.login", side_effect=boom), \
             mock.patch("auth.validate_credentials", side_effect=boom), \
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


class TestSafeCommandContractHolds:
    """Everything declared safe must genuinely never reach the brokerage.

    The declaration is only as good as its accuracy. --dry-run was listed as
    safe on the reasoning that a preview is harmless, but run_pipeline's
    Steps 6h/6i/7 call auth.login() unconditionally — so it authenticates, and
    in a container that login breaks the authoritative instance's session.
    """

    EXPECTED_SAFE = {
        "status", "config", "income_config", "strategy",
        "serve", "schedule",
    }

    def test_safe_set_is_exactly_what_was_reviewed(self):
        """Pins the set so widening it is a deliberate, reviewed edit."""
        import main
        assert set(main._NON_ACCOUNT_COMMANDS) == self.EXPECTED_SAFE, (
            "the safe-command set changed; each addition must be shown not to "
            "reach auth.login or place an order"
        )

    def test_dry_run_is_not_declared_safe(self):
        """Explicit regression: a dry run authenticates, so it is gated."""
        import main
        for dest in ("dry_run", "collar_dry_run"):
            assert dest not in main._NON_ACCOUNT_COMMANDS, (
                f"--{dest.replace('_','-')} authenticates via run_pipeline "
                "Steps 6h/6i/7 and must not be declared account-safe"
            )

    def test_run_pipeline_authenticates_even_in_dry_run(self):
        """The premise of the above. If this ever stops being true, revisit it."""
        import inspect
        src = inspect.getsource(scheduler.run_pipeline)
        assert "_ins_login()" in src, "expected an unconditional login in run_pipeline"
        # The login must not be guarded by `not dry_run` on its own line.
        for line in src.splitlines():
            if "_ins_login()" in line:
                assert "dry_run" not in line, (
                    "login is now dry_run-aware — --dry-run may be safe to allow, "
                    "re-evaluate _NON_ACCOUNT_COMMANDS"
                )


class TestRunMarkerSemantics:
    """_has_pipeline_run_today decides whether a LIVE catch-up pipeline fires.

    It had no test that could detect its removal: all four tests touching it
    mocked it out, and none read run_log.jsonl. Reverting it to the dated-file
    existence check left the suite green — the highest-consequence change of its
    round, unguarded.
    """

    def _write_log(self, tmp_path, rows):
        import json
        logs = tmp_path / "logs"
        logs.mkdir(parents=True, exist_ok=True)
        (logs / "run_log.jsonl").write_text(
            "".join(json.dumps(r) + "\n" for r in rows)
        )

    def _probe(self, tmp_path, rows, extra_text=""):
        import json
        from datetime import date
        self._write_log(tmp_path, rows)
        if extra_text:
            (tmp_path / "logs" / "run_log.jsonl").open("a").write(extra_text)
        code = (
            "import scheduler; print(scheduler._has_pipeline_run_today())"
        )
        r = _run_py(code, {"TRADER_DATA_DIR": str(tmp_path)})
        assert r.returncode == 0, r.stderr
        return r.stdout.strip() == "True"

    def _today(self):
        from datetime import date
        return date.today().strftime("%Y-%m-%d")

    def test_no_log_means_no_run(self, tmp_path):
        assert self._probe(tmp_path, []) is False

    def test_live_run_counts(self, tmp_path):
        assert self._probe(tmp_path, [{"run_date": self._today(), "dry_run": False}]) is True

    def test_dry_run_does_not_count(self, tmp_path):
        """A preview must not suppress the live catch-up."""
        assert self._probe(tmp_path, [{"run_date": self._today(), "dry_run": True}]) is False

    def test_dry_run_after_live_still_counts_as_run(self, tmp_path):
        """The append-only log keeps the live row; a later dry run cannot erase it.

        This is why the dated run_<date>.json is not read: write_run_log
        truncates it, so a dry run wiped the live marker and armed a duplicate
        live pipeline.
        """
        t = self._today()
        assert self._probe(tmp_path, [{"run_date": t, "dry_run": False},
                                      {"run_date": t, "dry_run": True}]) is True

    def test_scan_only_run_does_not_count(self, tmp_path):
        """The 06:35 PT scan writes dry_run: false but performs no trading.

        Counting it meant a missed 10:15 ET pipeline never triggered a catch-up.
        """
        assert self._probe(tmp_path, [{"run_date": self._today(), "dry_run": False,
                                       "scan_only": True}]) is False

    def test_yesterdays_live_run_does_not_count(self, tmp_path):
        assert self._probe(tmp_path, [{"run_date": "2020-01-01", "dry_run": False}]) is False

    def test_torn_final_line_is_tolerated(self, tmp_path):
        """An interrupted write must not hide a completed live run."""
        assert self._probe(tmp_path,
                           [{"run_date": self._today(), "dry_run": False}],
                           extra_text='{"truncated": ') is True


    def test_run_pipeline_records_scan_only(self):
        """The reader test above supplies scan_only itself, so it cannot catch the
        WRITER dropping the field. This closes that gap.

        Uses the non-trading-day early return (dry_run=False), which builds
        `results` and calls write_run_log immediately — no pipeline body, no
        network, no portfolio needed.
        """
        captured = {}

        def probe(**kw):
            captured.clear()
            with mock.patch.object(scheduler, "_is_trading_day", return_value=False), \
                 mock.patch.object(scheduler, "load_config", return_value={}), \
                 mock.patch.object(scheduler, "write_run_log",
                                   side_effect=lambda r: captured.update(r)):
                scheduler.run_pipeline(dry_run=False, **kw)
            return captured

        r = probe(skip_income=True, skip_auto_defense=True)
        assert r.get("scan_only") is True, (
            "run_pipeline must record scan_only, or the 06:35 PT scan reads as a "
            "full run and suppresses the live catch-up"
        )
        assert r.get("dry_run") is False

        r = probe()
        assert r.get("scan_only") is False, "a full pipeline must not be scan_only"

class TestOperatorVisibilityOfTheGate:
    """A passive instance must say so. Deleting either banner left the suite green."""

    def test_start_scheduler_announces_when_not_authoritative(self, caplog):
        import logging
        scheduler.set_force_dry_run(True)
        try:
            with mock.patch.object(scheduler, "_acquire_pid_lock"), \
                 mock.patch.object(scheduler, "setup_logging"), \
                 mock.patch.object(scheduler, "_capture_market_baseline"), \
                 mock.patch.object(scheduler, "_load_baseline_from_disk", return_value={}), \
                 caplog.at_level(logging.WARNING, logger="scheduler"):
                scheduler.start_scheduler(block=False)
            assert any("NOT AUTHORITATIVE" in r.message for r in caplog.records), \
                "a passive scheduler logged a normal-looking schedule with no warning"
        finally:
            scheduler.set_force_dry_run(False)
            scheduler.schedule.clear()

    def test_print_status_announces_when_not_authoritative(self, capsys):
        import utils
        scheduler.set_force_dry_run(True)
        try:
            utils.print_status()
        except Exception:
            pass
        finally:
            scheduler.set_force_dry_run(False)
        assert "NOT AUTHORITATIVE" in capsys.readouterr().out


class TestEnsureDirTolerance:
    """_ensure_dir must warn, not raise: raising kills `import main` and --help."""

    def test_read_only_parent_does_not_raise(self, tmp_path):
        import utils
        ro = tmp_path / "ro"
        ro.mkdir()
        ro.chmod(0o555)
        try:
            utils._ensure_dir(ro / "child")   # must not raise
        finally:
            ro.chmod(0o755)

    def test_import_survives_a_read_only_data_dir(self, tmp_path):
        ro = tmp_path / "ro2"
        ro.mkdir()
        ro.chmod(0o555)
        try:
            r = _run_py("import main; print('imported')", {"TRADER_DATA_DIR": str(ro)})
            assert r.returncode == 0, f"import main died on a read-only volume: {r.stderr}"
            assert "imported" in r.stdout
        finally:
            ro.chmod(0o755)


class TestDestTableIsDerivedNotListed:
    """A hand-maintained dest table was the round-9 finding.

    Planting a new order command in the mutually-exclusive group left it
    completely ungated with all 908 tests green, because _gated_command iterated
    a hardcoded tuple. The table is now derived from the parser; these tests hold
    that property and keep the six named exceptions honest.
    """

    def test_every_grouped_command_is_in_the_derived_table(self):
        import main
        parser = main.build_parser()
        grouped = {a.dest for g in parser._mutually_exclusive_groups
                   for a in g._group_actions}
        derived = set(main._primary_command_dests(parser))
        missing = grouped - derived
        assert not missing, f"primary commands absent from the gate's table: {missing}"

    def test_named_extras_are_real_parser_dests(self):
        """A typo in _EXTRA_PRIMARY_DESTS would silently gate nothing."""
        import main
        parser = main.build_parser()
        all_dests = {a.dest for a in parser._actions}
        unknown = set(main._EXTRA_PRIMARY_DESTS) - all_dests
        assert not unknown, f"_EXTRA_PRIMARY_DESTS names non-existent dests: {unknown}"

    def test_named_extras_are_genuinely_outside_the_group(self):
        """If one is moved into the group, it should be dropped from the list."""
        import main
        parser = main.build_parser()
        grouped = {a.dest for g in parser._mutually_exclusive_groups
                   for a in g._group_actions}
        redundant = set(main._EXTRA_PRIMARY_DESTS) & grouped
        assert not redundant, (
            f"{redundant} are now inside the mutex group and are derived "
            "automatically — remove them from _EXTRA_PRIMARY_DESTS"
        )

    def test_a_new_grouped_command_is_gated_automatically(self):
        """The regression itself: add a command to the group, expect it refused."""
        import main
        scheduler.set_force_dry_run(True)
        try:
            parser = main.build_parser()
            group = parser._mutually_exclusive_groups[0]
            group.add_argument("--liquidate-everything", nargs="?", const="ALL")
            args = parser.parse_args(["--liquidate-everything", "TSLA"])
            dests = main._primary_command_dests(parser)
            assert main._gated_command(args, dests) == "liquidate_everything", (
                "a newly added primary command was NOT gated — the table is not "
                "deriving from the parser"
            )
        finally:
            scheduler.set_force_dry_run(False)


class TestPreviouslyUnguardedFixes:
    """Round 9 found seven fixes revertible with the suite green. These cover the
    ones with real teeth."""

    def test_scheduler_alive_bounds_a_busy_job(self):
        """An unconditional True keeps /healthz green on a loop wedged inside
        run_pending() — the exact 'healthy while doing nothing' mode the probe
        exists to catch."""
        import math
        import time
        assert math.isfinite(scheduler._JOB_MAX_SECS) and scheduler._JOB_MAX_SECS <= 8 * 3600, \
            "_JOB_MAX_SECS must be a real bound — inf would restore the unbounded exemption"
        scheduler._job_active = True
        scheduler._job_started_at = time.monotonic() - 86_400   # literal, not derived
        try:
            assert scheduler.scheduler_alive() is False, \
                "a job running past _JOB_MAX_SECS must not report healthy"
        finally:
            scheduler._job_active = False
            scheduler._job_started_at = None

    def test_yf_cache_dir_is_platform_aware(self, monkeypatch):
        """Hardcoding the macOS path made yfinance corruption recovery a silent
        no-op on Linux — i.e. in the container.

        Patches sys.platform rather than branching on the host: branching meant
        this could not detect the regression when run on a Mac, which is where it
        is run.
        """
        import utils

        monkeypatch.setattr(utils.sys, "platform", "linux")
        monkeypatch.setenv("XDG_CACHE_HOME", "/tmp/xdgprobe")
        linux_path = str(utils._yf_cache_dir())
        assert linux_path.startswith("/tmp/xdgprobe"), (
            f"on Linux the cache must follow XDG_CACHE_HOME, got {linux_path}"
        )
        assert "Library/Caches" not in linux_path

        monkeypatch.setattr(utils.sys, "platform", "darwin")
        mac_path = str(utils._yf_cache_dir())
        assert "Library/Caches" in mac_path, mac_path

    def test_check_env_does_not_send_container_operators_to_setup(self, tmp_path):
        """--setup performs a live login and is gated; pointing at it is wrong."""
        code = (
            "import main\n"
            "try:\n"
            "    main.check_env()\n"
            "except SystemExit:\n"
            "    pass\n"
        )
        r = _run_py(code, {
            "TRADER_DATA_DIR": str(tmp_path),
            **{v: "" for v in ("ROBINHOOD_USERNAME", "ROBINHOOD_PASSWORD",
                               "ROBINHOOD_TOTP_SEED", "RESEND_API_KEY", "RESEND_FROM")},
        })
        # The message may *mention* --setup (as "not --setup"); what it must not
        # do is instruct the operator to run it.
        assert "Run  python main.py --setup" not in r.stdout, (
            "check_env told a container operator to run --setup, which is gated "
            "and writes into the ephemeral image layer"
        )
        assert "Secret" in r.stdout or "secret" in r.stdout

    def test_run_log_corruption_is_reported(self, tmp_path, caplog):
        """A wholly corrupt log concluding 'no run today' fires a catch-up live
        pipeline; it must at least say why."""
        import logging
        logs = tmp_path / "logs"
        logs.mkdir(parents=True)
        (logs / "run_log.jsonl").write_text("{not json\n{also not json\n")
        with mock.patch.object(scheduler, "DATA_DIR", tmp_path), \
             caplog.at_level(logging.WARNING, logger="scheduler"):
            assert scheduler._has_pipeline_run_today() is False
        assert any("unparseable" in r.message for r in caplog.records), \
            "corruption was swallowed silently"


class TestGateTableMatchesDispatch:
    """The dispatch chain is the real authority on what commands exist.

    _primary_command_dests derives 25 dests from the argparse mutex group and
    unions six hand-listed extras. The derived half is covered; the LISTED half
    was not — and all four order-placing --insurance-* commands plus --show and
    --roll live there, so that is the path this codebase actually grows along.
    A command added outside the group with a dispatch branch was ungated when
    paired with a safe command, with the whole suite green.
    """

    @staticmethod
    def _dispatch_dests():
        """Every `args.X` main() branches on, across ALL top-level if statements.

        An earlier version took only the LAST chain (`ifs[-1]`), so a command
        dispatched from a separate `if ...: handle(); return` placed before the
        chain — the natural shape for an early-exit command — appeared in
        neither the gate table nor this scan, and both cross-checks passed while
        it ran ungated.
        """
        import ast
        tree = ast.parse((REPO / "main.py").read_text())
        fn = next(n for n in tree.body
                  if isinstance(n, ast.FunctionDef) and n.name == "main")
        dests = set()
        for top in [n for n in fn.body if isinstance(n, ast.If)]:
            node = top
            while isinstance(node, ast.If):
                dests |= {a.attr for a in ast.walk(node.test)
                          if isinstance(a, ast.Attribute)
                          and isinstance(a.value, ast.Name) and a.value.id == "args"}
                node = node.orelse[0] if len(node.orelse) == 1 else None
        # Floor: if a refactor makes this scan match nothing, both cross-check
        # tests would pass vacuously. Fail loudly instead.
        assert len(dests) >= 31, (
            f"dispatch-chain scan found only {len(dests)} dests — it has drifted "
            "and both cross-checks would be vacuous"
        )
        return dests

    def test_no_dispatch_branch_is_missing_from_the_gate_table(self):
        """Catches a command added OUTSIDE the mutex group without listing it."""
        import main
        table = set(main._primary_command_dests(main.build_parser()))
        missing = self._dispatch_dests() - table
        assert not missing, (
            f"dispatch handles {sorted(missing)} but the gate's table does not "
            "contain them — add to _EXTRA_PRIMARY_DESTS or the mutex group"
        )

    def test_gate_table_has_no_phantom_entries(self):
        """A dest in the table with no dispatch branch would silently do nothing."""
        import main
        table = set(main._primary_command_dests(main.build_parser()))
        phantom = table - self._dispatch_dests()
        assert not phantom, f"gate table lists commands dispatch never handles: {sorted(phantom)}"


class TestEmptyValueIsRejected:
    """An empty symbol DISABLES every downstream filter, so it means
    'the entire portfolio'.

    Downstream code is written `if symbol_filter and ...`, so
    `main.py --generate-income "$SYM" --add` with SYM unset placed live credit
    spreads against every scanned recommendation and exited 0. The first fix
    guarded only the two truthiness-dispatched branches; every nargs="?" command
    dispatches on `is not None` and sailed through.
    """

    ARGVS = (
        ["--generate-income", " "],          # whitespace: truthy, so it silently
        ["--auto-defense", "\t"],            # matched nothing and exited 0
        ["--generate-income", "", "--add"],
        ["--auto-defense", ""],
        ["--insurance-optimize", ""],
        ["--ccs", ""],
        ["--pcs", ""],
        ["--buy", ""],
        ["--cc", ""],
        ["--short", ""],
    )

    @pytest.mark.parametrize("argv", ARGVS, ids=lambda a: a[0])
    def test_empty_value_exits_two(self, argv):
        import main
        with mock.patch.object(main, "check_env") as env, \
             mock.patch("auth.login", side_effect=AssertionError("reached the brokerage")), \
             mock.patch.object(sys, "argv", ["main.py", *argv]), \
             mock.patch("sys.stderr"):
            with pytest.raises(SystemExit) as exc:
                main.main()
        assert exc.value.code == 2, f"{argv} should be an argparse error, got {exc.value.code}"
        assert not env.called, f"{argv} began work before rejecting the empty value"

    def test_a_real_symbol_still_dispatches(self):
        """The guard must not break normal use."""
        import main
        with mock.patch.object(main, "check_env"), \
             mock.patch.object(main, "cmd_generate_income") as run, \
             mock.patch.object(sys, "argv", ["main.py", "--generate-income", "TSLA"]):
            main.main()
        run.assert_called_once()


class TestInvariantsWithNoPriorTest:
    """Fixes whose removal left the whole suite green.

    Each of these is stated as a correctness requirement in a code comment; a
    comment is not a test.
    """

    def test_finally_records_the_run_when_the_pipeline_fails_early(self):
        """_auto_income is read in run_pipeline's finally but assigned in Step 7.

        Without the pre-init, an early failure raised UnboundLocalError out of
        the finally itself — skipping write_run_log and the RH session close, so
        every market check then fired a live catch-up that failed identically.
        """
        captured = {}
        with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
             mock.patch.object(scheduler, "load_config", return_value={}), \
             mock.patch("portfolio.get_portfolio",
                        side_effect=RuntimeError("no portfolio data")), \
             mock.patch.object(scheduler, "write_run_log",
                               side_effect=lambda r: captured.update(r)):
            try:
                scheduler.run_pipeline(dry_run=True)
            except UnboundLocalError:
                raise AssertionError(
                    "run_pipeline's finally raised UnboundLocalError — "
                    "write_run_log and the session close were skipped"
                ) from None
            except Exception:
                pass
        assert captured, "a failed pipeline must still record a run-log row"

    def test_setup_refuses_before_authenticating_in_a_container(self, monkeypatch, tmp_path):
        """The CLI gate does NOT cover --setup in production (TRADER_ALLOW_LIVE=1),
        so the wizard's own refusal is the only protection. It must fire before
        Step 3's login(force_fresh=True)."""
        import setup_wizard
        monkeypatch.setenv("TRADER_DATA_DIR", str(tmp_path))
        with mock.patch("auth.validate_credentials",
                        side_effect=AssertionError("reached the live login")) as v:
            with pytest.raises(SystemExit) as exc:
                setup_wizard.run_setup_wizard()
        assert exc.value.code == 1
        assert not v.called, "the wizard authenticated before refusing"

    @pytest.mark.parametrize("kwargs,expected", [
        ({}, False),
        ({"skip_income": True}, True),
        ({"skip_auto_defense": True}, True),
        ({"skip_income": True, "skip_auto_defense": True}, True),
    ])
    def test_scan_only_uses_or_not_and(self, kwargs, expected):
        """The single-skip cases are the whole reason the operator is `or`.

        With `and`, run_pipeline(skip_income=True) records a FULL run and
        suppresses the catch-up that should have generated income.
        """
        captured = {}
        with mock.patch.object(scheduler, "_is_trading_day", return_value=False), \
             mock.patch.object(scheduler, "load_config", return_value={}), \
             mock.patch.object(scheduler, "write_run_log",
                               side_effect=lambda r: captured.update(r)):
            scheduler.run_pipeline(dry_run=False, **kwargs)
        assert captured.get("scan_only") is expected, f"{kwargs} -> {captured.get('scan_only')}"

    def test_flock_errno_handling_is_an_allow_list(self, tmp_path, monkeypatch):
        """An unrecognised errno must REFUSE.

        Guessing wrong in the other direction means two schedulers submitting
        duplicate live orders, which is why the code uses an allow-list.
        """
        import errno as errno_mod
        monkeypatch.setattr(scheduler, "_PID_FILE", tmp_path / "scheduler.pid")

        def raise_errno(code):
            def _f(*a, **k):
                raise OSError(code, "simulated")
            return _f

        # Unrecognised errno -> refuse (exit 1), never "continue unlocked".
        with mock.patch.object(scheduler.fcntl, "flock", raise_errno(errno_mod.EIO)):
            with pytest.raises(SystemExit) as exc:
                scheduler._acquire_pid_lock()
        assert exc.value.code == 1

        # A filesystem that genuinely cannot lock -> continue without protection.
        with mock.patch.object(scheduler.fcntl, "flock", raise_errno(errno_mod.ENOLCK)):
            scheduler._acquire_pid_lock()      # must not raise
        if scheduler._pid_lock_handle is not None:
            scheduler._pid_lock_handle.close()
            scheduler._pid_lock_handle = None

    def test_unreadable_run_log_fails_safe(self, tmp_path):
        """An unreadable log must read as 'a run happened', not 'no run today' —
        the latter fires a duplicate live catch-up."""
        logs = tmp_path / "logs"
        logs.mkdir(parents=True)
        (logs / "run_log.jsonl").write_text("{}")
        with mock.patch.object(scheduler, "DATA_DIR", tmp_path), \
             mock.patch("builtins.open", side_effect=OSError("unreadable")):
            assert scheduler._has_pipeline_run_today() is True

    def test_pid_lock_registers_no_atexit_unlink(self):
        """Unlinking the path breaks inode-scoped mutual exclusion: the next
        instance creates a fresh inode and locks it while this one still holds
        the old one."""
        import ast
        import inspect
        import textwrap
        # AST, not string matching: the docstring legitimately explains WHY
        # flock is used ("atexit does not run on SIGKILL").
        tree = ast.parse(textwrap.dedent(inspect.getsource(scheduler._acquire_pid_lock)))
        calls = [
            n for n in ast.walk(tree)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
            and n.func.attr == "register"
            and isinstance(n.func.value, ast.Name) and n.func.value.id == "atexit"
        ]
        assert not calls, (
            "atexit.register would unlink the PID path, and the lock lives on the "
            "INODE — the next instance would create a fresh inode and lock it "
            "while this one still holds the old one"
        )

    def test_serve_defaults_to_dry_run_when_var_is_absent(self, monkeypatch):
        """Absent, not empty: os.getenv's DEFAULT is what matters here."""
        monkeypatch.delenv("TRADER_ALLOW_LIVE", raising=False)
        with mock.patch("main.check_env"), \
             mock.patch("scheduler.set_force_dry_run") as m, \
             mock.patch("scheduler.start_scheduler"), \
             mock.patch("scheduler.install_signal_handlers"), \
             mock.patch("threading.Thread"), \
             mock.patch("uvicorn.run"):
            import main
            main.cmd_serve()
        m.assert_called_once_with(True)


class TestPipelineFailuresAlwaysReachTheFinally:
    """Anything between the `results` dict and run_pipeline's `try` escapes past
    the `finally`, so write_run_log and the failure email never run.
    _has_pipeline_run_today() then stays False and all five market checks fire a
    catch-up that fails identically — five silent failures a day, no email.

    This is a structural property, not a property of two specific statements:
    the test drives the two known raisers, but what it asserts is "the run log
    was still written", which holds for anything moved inside the try.
    """

    def _run_and_capture(self, breakage):
        written = {}
        with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
             mock.patch.object(scheduler, "load_config",
                               return_value={"recipient_email": "x@example.com"}), \
             mock.patch.object(scheduler, "write_run_log",
                               side_effect=lambda r: written.update(r)), \
             mock.patch("emailer.send_recommendations", return_value=True), \
             mock.patch("reporter.build_options_report",
                        return_value={"orders": [], "order_count": 0,
                                      "net_gain": 0.0, "total_credit": 0.0,
                                      "total_debit": 0.0}), \
             breakage:
            escaped = None
            try:
                scheduler.run_pipeline(dry_run=True)
            except BaseException as exc:      # noqa: BLE001 - that IS the bug
                escaped = exc
        return escaped, written

    def test_an_unreadable_action_ledger_still_writes_the_run_log(self):
        """_load_action_ledger's read_text raises OSError on a truncated or
        permission-denied file; its per-line `except (JSONDecodeError, ValueError)`
        does not cover that."""
        escaped, written = self._run_and_capture(
            mock.patch.object(scheduler, "_load_action_ledger",
                              side_effect=OSError("I/O error reading ledger"))
        )
        assert escaped is None, f"escaped past the finally: {escaped!r}"
        assert written, "run log was not written — catch-up will fire all day"

    def test_a_corrupt_ledger_encoding_still_writes_the_run_log(self):
        escaped, written = self._run_and_capture(
            mock.patch.object(
                scheduler, "_load_action_ledger",
                side_effect=UnicodeDecodeError("utf-8", b"\xff", 0, 1, "invalid"))
        )
        assert escaped is None, f"escaped past the finally: {escaped!r}"
        assert written, "run log was not written — catch-up will fire all day"

    def test_an_unimportable_auth_module_still_writes_the_run_log(self):
        """`from auth import logout` raises ImportError if auth or one of its
        own imports is broken — a bad deploy, a missing wheel in the image."""
        escaped, written = self._run_and_capture(
            mock.patch.dict(sys.modules, {"auth": None})
        )
        assert escaped is None, f"escaped past the finally: {escaped!r}"
        assert written, "run log was not written — catch-up will fire all day"


class TestProductionEntrypointsAreCovered:
    """Paths that only production exercises, and that mutation showed unguarded."""

    def test_start_scheduler_blocking_actually_runs_the_loop(self):
        """`--schedule` is the laptop scheduler running the live account.

        Every other test calls start_scheduler(block=False), so mutating
        `if block:` to `if False:` left the whole suite green while turning
        --schedule into a process that registers jobs and exits.
        """
        with mock.patch.object(scheduler, "_acquire_pid_lock"), \
             mock.patch.object(scheduler, "setup_logging"), \
             mock.patch.object(scheduler, "_capture_market_baseline"), \
             mock.patch.object(scheduler, "_load_baseline_from_disk", return_value={}), \
             mock.patch.object(scheduler, "install_signal_handlers") as sig, \
             mock.patch.object(scheduler, "run_loop") as loop:
            scheduler.start_scheduler(block=True)
        assert loop.called, "block=True must enter the polling loop"
        assert sig.called, "block=True must install signal handlers"
        scheduler.schedule.clear()

    def test_dockerfile_arms_the_safety_gate(self):
        """_force_dry_run keys off TRADER_DATA_DIR.

        Setting it only in docker-compose.yaml meant any run of the image
        outside the shipped compose file — a docker run smoke test, a Phase 2/4
        unit that omits the environment block — was a fully ungated live
        instance beside the laptop scheduler.
        """
        import re
        import yaml

        # Regex the VALUE, not the substring: a commented-out line and
        # `ENV TRADER_DATA_DIR=` (empty) both satisfied the old check, and the
        # empty form is the dangerous one — Docker sets the variable to "",
        # which disarms the gate and returns state to the image layer.
        dockerfile = (REPO / "Dockerfile").read_text()
        matches = re.findall(r"^\s*ENV\s+TRADER_DATA_DIR=(\S*)\s*$",
                             dockerfile, re.M)
        assert matches, "Dockerfile must set TRADER_DATA_DIR (uncommented)"
        value = matches[-1]
        assert value, "TRADER_DATA_DIR is set to an empty value — this DISARMS the gate"

        # All three places must agree, or the volume and the code disagree.
        compose = yaml.safe_load((REPO / "docker-compose.yaml").read_text())
        app = compose["services"]["app"]
        assert app["environment"]["TRADER_DATA_DIR"] == value, (
            "Dockerfile and docker-compose.yaml disagree on TRADER_DATA_DIR"
        )
        assert any(m.split(":")[1] == value for m in app["volumes"]), (
            f"no volume is mounted at {value}"
        )

    def test_dockerfile_sets_the_timezone(self):
        """Date reads (_has_pipeline_run_today, the ledger key, _is_trading_day)
        use date.today(). On UTC the 22:00 ET report job fires at 02:00 UTC the
        NEXT day — reporting zero orders nightly, and skipping entirely on
        Fridays because it sees Saturday."""
        import re
        dockerfile = (REPO / "Dockerfile").read_text()
        m = re.findall(r"^\s*ENV\s+TZ=(\S+)\s*$", dockerfile, re.M)
        assert m and m[-1] == "America/New_York", (
            "the image must set TZ itself, not rely on docker-compose.yaml"
        )

    def test_pipeline_records_a_run_when_the_shared_login_fails(self):
        """auth.login RAISES on every failure path; it never returns False.

        With the login outside run_pipeline's try, that escaped before the try
        was entered — skipping write_run_log, the failure email and the session
        close — so every market check fired a live catch-up that repeated the
        failing login.
        """
        captured = {}
        with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
             mock.patch.object(scheduler, "load_config", return_value={}), \
             mock.patch("auth.login", side_effect=RuntimeError("no TTY for device approval")), \
             mock.patch.object(scheduler, "write_run_log",
                               side_effect=lambda r: captured.update(r)):
            scheduler.run_pipeline(dry_run=False)   # must not raise
        assert captured, "a login failure must still record a run-log row"
        assert captured.get("outcome") == "error"

    def test_trade_report_uses_a_date_format_the_reporter_accepts(self):
        """The daily email's trade report silently failed on every run.

        build_options_report -> _parse_date_range accepts mm/dd or None only;
        the pipeline passed YYYY-MM-DD, so the section never rendered and the
        failure was swallowed by a warning-only except.
        """
        called = {}

        def _capture(*args, **kwargs):
            called["args"] = args
            called["kwargs"] = kwargs
            return {"orders": [], "order_count": 0, "net_gain": 0.0,
                    "total_credit": 0.0, "total_debit": 0.0}

        with mock.patch.object(scheduler, "_is_trading_day", return_value=True), \
             mock.patch.object(scheduler, "load_config",
                               return_value={"recipient_email": "x@example.com"}), \
             mock.patch("reporter.build_options_report", side_effect=_capture), \
             mock.patch.object(scheduler, "write_run_log"), \
             mock.patch("portfolio.get_portfolio",
                        side_effect=RuntimeError("stop after the report")):
            try:
                scheduler.run_pipeline(dry_run=False)
            except Exception:
                pass

        # Unconditional: an `if called:` guard would let this test go green
        # again the moment the report call moves or stops running.
        assert called, "run_pipeline no longer builds the trade report"
        # Assert the ACTUAL argument, not the absence of one bad spelling:
        # a negative grep passes for every other wrong format too.
        import reporter
        passed = called["args"][0] if called["args"] else called["kwargs"].get("date_arg")
        assert passed is None, f"expected None (=today), got {passed!r}"
        reporter._parse_date_range(passed)   # must not raise


class TestContainerDetectionIsCentralised:
    """One predicate, one meaning.

    The container check was written out by hand at seven sites and two of them
    disagreed: `os.getenv(...)` truthiness reads `ENV TRADER_DATA_DIR=` — a
    perfectly legal Dockerfile line — as "not a container", while the
    live-trading gate used `is not None`.  The gate is the site where being
    wrong means an unauthoritative instance places real orders, so that is the
    behaviour utils.is_container() standardises on.
    """

    def test_empty_value_is_still_a_container(self):
        import utils
        with mock.patch.dict(os.environ, {"TRADER_DATA_DIR": ""}):
            assert utils.is_container() is True

    def test_set_value_is_a_container(self):
        import utils
        with mock.patch.dict(os.environ, {"TRADER_DATA_DIR": "/data"}):
            assert utils.is_container() is True

    def test_unset_is_not_a_container(self):
        import utils
        kept = {k: v for k, v in os.environ.items() if k != "TRADER_DATA_DIR"}
        with mock.patch.dict(os.environ, kept, clear=True):
            assert utils.is_container() is False

    def test_no_module_hand_writes_the_predicate(self):
        """Derived, not listed: a NEW hand-written check fails here by itself.

        utils.py is the one exemption — it defines the helper, and DATA_DIR
        deliberately keeps `or` because an empty value is a container marker
        but not a usable path.
        """
        import ast

        offenders = []
        for path in sorted(REPO.glob("*.py")):
            if path.name == "utils.py":
                continue
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.Call):
                    continue
                fn = node.func
                is_getenv = isinstance(fn, ast.Attribute) and fn.attr in {"getenv", "get"}
                if not is_getenv or not node.args:
                    continue
                first = node.args[0]
                if isinstance(first, ast.Constant) and first.value == "TRADER_DATA_DIR":
                    offenders.append(f"{path.name}:{node.lineno}")
        assert not offenders, (
            "these read TRADER_DATA_DIR directly instead of calling "
            f"utils.is_container(): {offenders} — the two forms disagree on an "
            "empty value, which is exactly how the live-trading gate got it wrong"
        )


class TestEphemeralConfigWriteWarning:
    """These warnings shipped with no test, so deleting either left the suite green.

    They guard a real hazard: config.yaml holds the live-order master switches
    (ig_enabled, auto_income, auto_defense), and in a container the write lands
    in the image layer and is silently reverted by the next deploy — re-enabling
    trading an operator turned off mid-incident.
    """

    def test_main_warns_inside_a_container(self, capsys):
        import main
        with mock.patch.object(main, "is_container", return_value=True):
            main._warn_if_config_write_is_ephemeral()
        assert "REVERTED" in capsys.readouterr().out

    def test_main_is_silent_outside_a_container(self, capsys):
        import main
        with mock.patch.object(main, "is_container", return_value=False):
            main._warn_if_config_write_is_ephemeral()
        assert capsys.readouterr().out == ""

    def test_cmd_config_calls_the_warning(self, tmp_path, capsys):
        """Catches deletion of the CALL SITE, which the two tests above cannot.

        CONFIG_FILE is redirected at tmp_path: cmd_config imports it inside the
        function, so an un-redirected run rewrites the REPO's own config.yaml —
        which is a live trading parameter file, not scratch space.
        """
        import main
        import utils

        cfg = tmp_path / "config.yaml"
        cfg.write_text("min_otm_pct: 10.0\n")
        with mock.patch.object(utils, "CONFIG_FILE", cfg), \
             mock.patch.object(main, "_warn_if_config_write_is_ephemeral") as warn:
            main.cmd_config("min_otm_pct=7.5")
        warn.assert_called_once()
        # And the write landed in the sandbox, not the repo.
        assert "7.5" in cfg.read_text()

    def test_income_generator_warns_inside_a_container(self, tmp_path, capsys):
        import income_generator
        cfg = tmp_path / "config.yaml"
        cfg.write_text("ig_risk_factor: 1.0\n")
        with mock.patch.object(income_generator, "is_container", return_value=True):
            ok = income_generator.set_config("ig_risk_factor=0.5", config_path=cfg)
        assert ok
        assert "REVERTED" in capsys.readouterr().out

    def test_income_generator_is_silent_outside_a_container(self, tmp_path, capsys):
        import income_generator
        cfg = tmp_path / "config.yaml"
        cfg.write_text("ig_risk_factor: 1.0\n")
        with mock.patch.object(income_generator, "is_container", return_value=False):
            ok = income_generator.set_config("ig_risk_factor=0.5", config_path=cfg)
        assert ok
        assert "REVERTED" not in capsys.readouterr().out


class TestTheAuditsThemselvesCanFail:
    """Positive controls for the safety machinery.

    The runtime probe has had one since it was written; the static walk had
    none, and that is not a theoretical gap.  Inserting `return None` as the
    first line of _static_reaches_brokerage left all 960 tests green, and the
    _attr_pairs resolution branch — added specifically to close a demonstrated
    bypass — could be deleted in full with the suite still green.  A walk that
    finds nothing is indistinguishable from a walk that is broken unless
    something asserts that it DOES find.

    Each case below is a resolution style the walk must support, written as a
    synthetic package so the control keeps working when the real call graph
    changes shape.
    """

    CASES = {
        "from_import": (
            "from trader import place_spread_order\n"
            "def h():\n"
            "    place_spread_order()\n"
        ),
        "module_import_attribute_call": (
            # The style _attr_pairs exists for: `import X` + `X.fn()`.
            "import trader\n"
            "def h():\n"
            "    trader.fetch_buying_power()\n"
        ),
        "same_module_callee": (
            "from trader import place_spread_order\n"
            "def inner():\n"
            "    place_spread_order()\n"
            "def h():\n"
            "    inner()\n"
        ),
        "import_inside_the_function": (
            "def h():\n"
            "    from trader import place_spread_order\n"
            "    place_spread_order()\n"
        ),
        "aliased_robin_stocks": (
            "import robin_stocks.robinhood as rh\n"
            "def h():\n"
            "    rh.orders.order_option_spread()\n"
        ),
        "transitive_two_hops": (
            "from helper_a import step\n"
            "def h():\n"
            "    step()\n"
        ),
    }

    @pytest.fixture
    def fake_repo(self, tmp_path):
        # auth.login reaches robin_stocks, exactly as the real one does; the
        # walk has no table of names, so the control must exercise the derived
        # rule.  robin_stocks need not exist on disk — resolution is by import
        # module name.
        (tmp_path / "auth.py").write_text(
            "from robin_stocks.robinhood import helper\n"
            "def login():\n    helper.request_get()\n"
        )
        (tmp_path / "trader.py").write_text(
            "from auth import login\n"
            "def place_spread_order():\n    login()\n"
            "def fetch_buying_power():\n    login()\n"
        )
        (tmp_path / "helper_a.py").write_text(
            "from helper_b import deeper\n"
            "def step():\n    deeper()\n"
        )
        (tmp_path / "helper_b.py").write_text(
            "from auth import login\n"
            "def deeper():\n    login()\n"
        )
        for name, src in self.CASES.items():
            (tmp_path / f"m_{name}.py").write_text(src)
        return tmp_path

    @pytest.mark.parametrize("style", sorted(CASES))
    def test_static_walk_resolves_every_import_style(self, style, fake_repo):
        path = _static_reaches_brokerage(f"m_{style}", "h", root=fake_repo)
        assert path is not None, (
            f"the static walk did not resolve the {style!r} style — a "
            "declared-safe command written this way would reach the brokerage "
            "with the suite green"
        )

    # The four functions the walk used to name explicitly.  Not a table the
    # walk consults — it has none — but a pin on the redundancy that let the
    # table go: each of these is found through robin_stocks on its own merits.
    # If a refactor breaks that for one of them, the derived rule alone may no
    # longer cover it and this fails rather than going quiet.
    FORMERLY_NAMED_SINKS = (
        ("auth", "login"),
        ("auth", "validate_credentials"),
        ("trader", "place_spread_order"),
        ("trader", "place_debit_spread_order"),
    )

    @pytest.mark.parametrize("mod,func", FORMERLY_NAMED_SINKS)
    def test_the_functions_that_used_to_be_named_sinks_are_still_found(self, mod, func):
        assert _func_in(_tree_for(mod), func) is not None, (
            f"{mod}.{func} no longer exists — this pin is stale"
        )
        path = _static_reaches_brokerage(mod, func)
        assert path is not None, (
            f"{mod}.{func} is no longer detected as reaching the brokerage. It "
            "used to be named explicitly; the name was dropped because the "
            "derived rule found it anyway. That is no longer true, so either "
            "restore an explicit entry or fix the walk"
        )

    def test_static_walk_follows_a_chain_as_deep_as_real_code(self, fake_repo):
        """Pins the depth cap against the longest real path.

        cmd_run -> run_pipeline -> _fetch_and_pair_debit_spreads ->
        _fetch_and_pair_generic -> _build_order_leg_pairs -> robin_stocks is
        five hops; the other synthetic cases reach only two, so lowering the
        cap to 2 used to pass everything.

        Dropping the named sink table cost one hop of effective reach for
        paths that end at auth.login: the walk must now recurse INTO login
        rather than matching its name in the caller's frame.  This chain ends
        at robin_stocks directly, which is the deepest shape the rule has to
        resolve.
        """
        for i in range(6):
            body = ("    from robin_stocks.robinhood import helper\n"
                    "    helper.request_get()\n") if i == 5 else \
                   f"    from chain_{i + 1} import step\n    step()\n"
            (fake_repo / f"chain_{i}.py").write_text(f"def step():\n{body}")
        (fake_repo / "m_deep.py").write_text(
            "from chain_0 import step\ndef h():\n    step()\n")
        assert _static_reaches_brokerage("m_deep", "h", root=fake_repo) is not None, (
            "the walk gave up before six hops — real call graphs are that deep"
        )

    def test_the_gated_job_boundary_is_scoped_to_jobs_only(self):
        """_is_gated_job must exempt job_* and nothing else.

        Broadening it to `mod_name == "scheduler"` swallows the whole module,
        and --serve/--schedule are exactly the two commands whose account-safe
        status rests on this walk.
        """
        assert _is_gated_job("scheduler", "job_daily_pipeline") is True
        for name in ("start_scheduler", "run_loop", "run_pipeline",
                     "_skip_unless_authoritative"):
            assert _is_gated_job("scheduler", name) is False, (
                f"scheduler.{name} is exempted from the static walk but is not "
                "a scheduled job — the boundary is too wide"
            )
        assert _is_gated_job("trader", "job_like_name") is False

    def test_static_walk_does_not_flag_a_genuinely_safe_handler(self, fake_repo):
        """The control must not pass by flagging everything."""
        (fake_repo / "m_safe.py").write_text(
            "import json\n"
            "def h():\n"
            "    return json.dumps({'ok': True})\n"
        )
        assert _static_reaches_brokerage("m_safe", "h", root=fake_repo) is None


class TestTheThreadHookItselfCanFail:
    """Positive control for the worker-thread half of the trap.

    Without this the whole threading.excepthook block deletes green: nothing
    in the suite produces a thread breach, so trap.breaches is permanently
    empty, _assert_no_thread_breach passes vacuously and the probe's
    `return bool(trap.breaches)` is a constant False. That is the same
    "load-bearing code with zero exercising tests" defect the hook was added
    to close, one level further out.
    """

    def test_a_breach_on_a_worker_thread_is_recorded(self):
        import contextlib

        import auth

        with contextlib.ExitStack() as stack:
            trap = _trap_the_brokerage(stack)
            _assert_trap_armed(trap)
            t = threading.Thread(target=auth.login)
            t.start()
            t.join(5)
            assert trap.breaches, (
                "a trapped brokerage call on a worker thread was not recorded "
                "— pytest downgrades it to a warning, so both the runtime "
                "probe and the job test would report the breach as safe"
            )

    def test_the_hook_leaves_unrelated_thread_errors_alone(self):
        """It must not swallow every thread exception, only brokerage ones."""
        import contextlib

        seen = []
        with contextlib.ExitStack() as stack:
            trap = _trap_the_brokerage(stack)
            stack.enter_context(
                mock.patch.object(threading, "excepthook", lambda a: seen.append(a)))
            # Re-arm on top of the trap's hook, then confirm the trap did not
            # claim an unrelated failure.
            t = threading.Thread(target=lambda: 1 / 0)
            t.start()
            t.join(5)
        assert not trap.breaches, "an unrelated ZeroDivisionError was recorded as a breach"

    def test_a_recorded_thread_breach_makes_the_probe_report_reached(self):
        """The record must actually change the probe's verdict, not just exist."""
        import main

        probe = TestGateCoversEveryEntryPoint()
        parser = main.build_parser()
        # --status is genuinely safe; with a thread breach injected it must
        # nonetheless be reported as reaching the brokerage.
        assert probe._reaches_brokerage("status", parser) is False
        real = _trap_the_brokerage

        def _trap_and_breach(stack):
            trap = real(stack)
            trap.breaches.append("injected")
            return trap

        with mock.patch(f"{__name__}._trap_the_brokerage", _trap_and_breach):
            assert probe._reaches_brokerage("status", parser) is True, (
                "a recorded worker-thread breach did not change the probe's "
                "verdict — the record is inert"
            )


class TestEveryJobGuardsBeforeItActs:
    """The gate must be the FIRST thing a job does, not merely present.

    _is_gated_job() makes the runtime job test the sole justification for
    treating scheduler jobs as a recursion boundary in the static walk, and
    that test only observes what one bare call executes on a developer host.
    A job whose guard is correct but preceded by container-only work — say a
    session warm-up behind `if glob(DATA_DIR/'snapshots'/...)`, empty in a
    checkout and populated on the mounted volume — logs in on a
    non-authoritative instance while the suite stays green.

    Position is checkable statically, so check it rather than exempt it.
    """

    @staticmethod
    def _job_nodes():
        """Every job_* definition, by AST.

        ast.walk, not tree.body: a `def job_x` nested in a module-level `if`
        or `try` is still a scheduled job and must not escape the check.
        Both function kinds, because AsyncFunctionDef is NOT a subclass of
        FunctionDef and skipping it silently exempts the job.
        """
        import ast

        tree = ast.parse((REPO / "scheduler.py").read_text())
        return {n.name: n for n in ast.walk(tree)
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
                and n.name.startswith("job_")}

    def test_the_ast_job_set_matches_the_runtime_module(self):
        """Anything this check cannot see is a job it cannot guard.

        Without this, a job the AST scan misses is exempt from the position
        check AND passes the runtime check vacuously, so it has zero coverage
        from either half.
        """
        ast_names = set(self._job_nodes())
        runtime = {n for n in dir(scheduler)
                   if n.startswith("job_") and callable(getattr(scheduler, n))}
        assert ast_names == runtime, (
            f"job discovery drifted: AST sees {sorted(ast_names)}, the module "
            f"exposes {sorted(runtime)} — the difference is unguarded"
        )

    def test_no_job_is_async(self):
        """The runtime half calls job(); on a coroutine that executes nothing.

        An `async def job_*` would pass test_every_scheduled_job_is_gated
        vacuously — calling it only builds a coroutine — leaving a
        never-awaited RuntimeWarning as the sole trace, and warnings are not
        errors here.
        """
        import ast

        offenders = [name for name, node in self._job_nodes().items()
                     if isinstance(node, ast.AsyncFunctionDef)]
        assert not offenders, (
            f"{offenders} are async; the runtime gate test cannot execute them "
            "and would pass without running a single statement"
        )

    def test_guard_is_the_first_statement_of_every_job(self):
        import ast

        jobs = self._job_nodes()
        assert jobs, "no job_* functions found — the check would be vacuous"

        for name, fn in sorted(jobs.items()):
            # A decorator runs BEFORE the body, so it can reach the brokerage
            # while the first statement below is still the guard.
            assert not fn.decorator_list, (
                f"{name} is decorated; a decorator runs before the guard and "
                "the position check below cannot see it"
            )
            body = fn.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)):
                body = body[1:]          # docstring
            assert body, f"{name} has an empty body"
            first = body[0]
            guarded = (
                isinstance(first, ast.If)
                and any(isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                        and n.func.id == "_skip_unless_authoritative"
                        for n in ast.walk(first.test))
                and any(isinstance(n, ast.Return) for n in first.body)
            )
            assert guarded, (
                f"{name} does not open with "
                "`if _skip_unless_authoritative(...): return` — anything above "
                "the guard runs on a non-authoritative instance, and the "
                "runtime job test cannot see work that is conditional on "
                "container-only state"
            )


class TestProbeDoesNotLeakSignalHandlers:
    """cmd_serve installs real SIGTERM/SIGINT handlers on the main thread.

    Probing --serve without stubbing install_signal_handlers replaced pytest's
    own SIGINT handler for the rest of the session: Ctrl-C stopped raising
    KeyboardInterrupt and a second one hit os._exit(130), killing the run with
    no report.  The stub had no test, so deleting it silently restored that.
    """

    def test_serve_probe_does_not_leak_the_gate_flag(self):
        """The probe must not arm the gate for the rest of the session."""
        import main

        scheduler.set_force_dry_run(False)
        TestGateCoversEveryEntryPoint()._reaches_brokerage(
            "serve", main.build_parser())
        assert scheduler._force_dry_run is False, (
            "probing --serve left _force_dry_run armed — every later test "
            "sees a gated scheduler and its jobs silently skip"
        )

    def test_sigint_handler_survives_the_serve_probe(self):
        import signal

        before = signal.getsignal(signal.SIGINT)
        TestGateCoversEveryEntryPoint()._reaches_brokerage(
            "serve", __import__("main").build_parser())
        after = signal.getsignal(signal.SIGINT)
        assert after is before, (
            f"probing --serve replaced the SIGINT handler ({before!r} -> "
            f"{after!r}) — Ctrl-C no longer interrupts the test run"
        )
