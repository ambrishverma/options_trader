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
