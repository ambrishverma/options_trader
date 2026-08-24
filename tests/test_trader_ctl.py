"""Behavioural tests for scripts/trader-ctl's persistent disable/enable.

`bootout` alone unloads a job only for the current login session; the plist
stays in ~/Library/LaunchAgents with RunAtLoad, so a stood-down laptop restarts
the scheduler at the next login and places a second set of live orders on the
same account.  `stop` therefore has to set launchd's persistent override, and
that behaviour is what these tests pin down.

launchctl and pgrep are stubbed onto PATH.  pgrep in particular MUST be stubbed:
the real one would find this machine's actual running scheduler and send the
script down the bootout path mid-test.
"""
import os
import stat
import subprocess
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "trader-ctl"
LABEL = "com.ambrish.options-trader"


def _write_exec(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


@pytest.fixture
def harness(tmp_path):
    """Run trader-ctl with stubbed launchctl/pgrep and a throwaway HOME."""
    bindir = tmp_path / "bin"
    bindir.mkdir()
    calls = tmp_path / "calls.log"

    def run(*, disabled_line: str, disable_rc: int = 0, cmd: str = "stop",
            running_pid: str = "", with_plist: bool = False,
            extra_env: dict | None = None):
        _write_exec(
            bindir / "pgrep",
            "#!/bin/bash\nexit 1\n" if not running_pid
            else f"#!/bin/bash\necho {running_pid}\n",
        )
        if with_plist:
            la = tmp_path / "Library" / "LaunchAgents"
            la.mkdir(parents=True, exist_ok=True)
            (la / f"{LABEL}.plist").write_text("<plist/>")
        _write_exec(
            bindir / "launchctl",
            f"""#!/bin/bash
echo "$@" >> {calls}
case "$1" in
  list)           exit 1 ;;                    # job not loaded
  print-disabled) printf '%s\\n' '{disabled_line}' ; exit 0 ;;
  disable)        exit {disable_rc} ;;
  enable)         exit 0 ;;
  *)              exit 0 ;;
esac
""",
        )
        env = dict(os.environ)
        env["PATH"] = f"{bindir}:{env['PATH']}"
        env["HOME"] = str(tmp_path)
        env.update(extra_env or {})
        proc = subprocess.run(
            [str(SCRIPT), cmd],
            capture_output=True, text=True, env=env, timeout=60,
        )
        logged = calls.read_text() if calls.exists() else ""
        return proc, logged

    return run


DISABLED_TRUE = f'\t"{LABEL}" => true'          # macOS 12
DISABLED_WORD = f'\t"{LABEL}" => disabled'      # newer macOS
ENABLED_FALSE = f'\t"{LABEL}" => false'
UNPARSEABLE = f'\t"{LABEL}" => 1'               # a format this script cannot read
ABSENT = '\t"com.example.other" => true'


class TestStopSetsPersistentDisable:
    def test_stop_calls_disable(self, harness):
        proc, calls = harness(disabled_line=ABSENT)
        assert f"disable gui/{os.getuid()}/{LABEL}" in calls, (
            "stop must set the persistent override, or the laptop re-arms at next login"
        )
        assert proc.returncode == 0

    @pytest.mark.parametrize("line", [DISABLED_TRUE, DISABLED_WORD])
    def test_confirms_both_macos_formats(self, harness, line):
        """print-disabled prints `=> true` on macOS 12 and `=> disabled` later."""
        proc, _ = harness(disabled_line=line)
        assert "confirmed" in proc.stdout
        assert proc.returncode == 0

    def test_unreadable_readback_is_not_a_failure(self, harness):
        """A disable that returned 0 is trusted even if the read-back is unparseable.

        Gating success on print-disabled instead would report a working
        stand-down as broken and block the operator from starting the other
        laptop.
        """
        proc, _ = harness(disabled_line=UNPARSEABLE)
        assert proc.returncode == 0
        assert "could not set the persistent disable" not in proc.stdout
        assert "could not read back" in proc.stdout

    def test_disable_failure_exits_nonzero(self, harness):
        proc, _ = harness(disabled_line=ABSENT, disable_rc=1)
        assert proc.returncode == 1
        assert "could not set the persistent disable" in proc.stdout

    def test_reminder_survives_disable_failure(self, harness):
        """The audit-task reminder must print on the failure path too.

        The laptop is stood down either way, so verify-daily-pipeline will still
        fire and send a false critical alarm.  Exiting before the reminder would
        suppress it exactly when it matters most.
        """
        proc, _ = harness(disabled_line=ABSENT, disable_rc=1)
        assert "verify-daily-pipeline" in proc.stdout
        assert proc.returncode == 1


class TestStartClearsPersistentDisable:
    def test_enable_runs_even_when_already_running(self, harness):
        """`start` must clear the override on every path that returns success.

        `launchctl disable` does not stop a running job, so a laptop can be
        running AND persistently disabled at once.  It looks healthy today and
        silently fails to come back after the next reboot.  The early "already
        running" exit must not skip the enable.
        """
        proc, calls = harness(
            disabled_line=DISABLED_TRUE, cmd="start",
            running_pid="4242", with_plist=True,
        )
        assert f"enable gui/{os.getuid()}/{LABEL}" in calls, (
            "start exited early without clearing the persistent disable"
        )
        assert "already running" in proc.stdout
        assert proc.returncode == 0


    def test_abort_at_interlock_leaves_disable_intact(self, harness):
        """Aborting the one-laptop prompt must NOT arm this machine.

        Everything before `head_ "starting"` can still abort — a failed
        preflight, or the operator answering anything but "yes". Clearing the
        override before those exits would leave a laptop the operator believes
        is stood down armed to start itself at the next login: the exact
        duplicate-scheduler failure this command exists to prevent.
        """
        proc, calls = harness(
            disabled_line=DISABLED_TRUE, cmd="start",
            running_pid="", with_plist=True,
        )
        # Preflight fails in the harness (no repo, no interpreter), so start
        # exits before reaching the starting stage — the same class of abort.
        assert proc.returncode != 0
        assert "enable" not in calls, (
            "start cleared the persistent disable on a path that then aborted"
        )


class TestFailedStartLeavesLaptopDisarmed:
    def test_failed_start_re_disables(self, harness, tmp_path):
        """`start` either arms AND runs this laptop, or leaves it disarmed.

        A start that enabled the service and then failed to bring it up leaves
        a machine the operator watched fail but which starts itself at the next
        login — beside whichever laptop they moved to instead.
        """
        # Reach the starting stage: preflight must pass, so give it a real
        # interpreter and a .env, and skip the git update with --no-pull.
        repo = tmp_path / "repo"
        (repo / "logs").mkdir(parents=True)
        (repo / ".env").write_text("x=1\n")
        la = tmp_path / "Library" / "LaunchAgents"
        la.mkdir(parents=True, exist_ok=True)
        (la / f"{LABEL}.plist").write_text(
            f"<plist><key>ProgramArguments</key><array>"
            f"<string>{os.sys.executable}</string></array></plist>"
        )
        proc, calls = harness(
            disabled_line=DISABLED_TRUE, cmd="start",
            running_pid="", extra_env={"TRADER_REPO": str(repo)},
        )
        # Preflight cannot pass in a sandbox (no importable scheduler module), so
        # this asserts only where the start path is genuinely reachable. Checking
        # for "starting" would be wrong: "preflight failed — not starting" also
        # contains it.
        if "preflight failed" in proc.stdout or "no plist" in proc.stdout:
            pytest.skip("preflight gated before the starting stage")
        assert f"enable gui/{os.getuid()}/{LABEL}" in calls
        assert f"disable gui/{os.getuid()}/{LABEL}" in calls, (
            "a start that failed to bring the job up left the laptop armed"
        )
        assert proc.returncode == 1


class TestRestorePersistentDisable:
    """Exercised directly, not through do_start.

    The end-to-end test below can only assert where preflight passes, which is
    never true in a sandbox — so it skips, leaving the guard against a laptop
    armed after a visible failure untested. Sourcing the script defines the
    functions without dispatching.
    """

    def _call(self, tmp_path, *, disable_rc: int, stderr: str = ""):
        bindir = tmp_path / "bin2"
        bindir.mkdir(exist_ok=True)
        _write_exec(
            bindir / "launchctl",
            f"#!/bin/bash\n[ -n \"{stderr}\" ] && echo \"{stderr}\" >&2\n"
            f"exit {disable_rc}\n",
        )
        env = dict(os.environ)
        env["PATH"] = f"{bindir}:{env['PATH']}"
        return subprocess.run(
            ["bash", "-c", f". {SCRIPT}; restore_persistent_disable"],
            capture_output=True, text=True, env=env, timeout=30,
        )

    def test_success_confirms_the_laptop_is_disarmed(self, tmp_path):
        proc = self._call(tmp_path, disable_rc=0)
        assert "will NOT start itself at the next login" in proc.stdout

    def test_failure_surfaces_launchd_error(self, tmp_path):
        """The one path where failing leaves the laptop armed after a visible
        failure — 'it did not work' without launchd's reason is the least useful
        place to be terse."""
        proc = self._call(tmp_path, disable_rc=1, stderr="Operation not permitted")
        assert "could not re-disable" in proc.stdout
        assert "Operation not permitted" in proc.stdout, (
            "launchd's reason was discarded on the most dangerous failure path"
        )
        assert "launchctl disable" in proc.stdout  # actionable retry


class TestSourcingHook:
    def test_sourcing_defines_without_dispatching(self):
        proc = subprocess.run(
            ["bash", "-c", f". {SCRIPT}; type -t restore_persistent_disable"],
            capture_output=True, text=True, timeout=30,
        )
        assert proc.stdout.strip() == "function"
        assert "usage:" not in proc.stdout

    def test_execution_still_dispatches(self):
        """Guards the hook against becoming a silent no-op.

        A `trader-ctl stop` that defined its functions and exited 0 would read
        as a laptop stood down while the scheduler kept running and trading.
        Detection is from BASH_SOURCE, not an env var, so no ambient variable
        can cause that.
        """
        proc = subprocess.run(
            [str(SCRIPT), "badcmd"], capture_output=True, text=True, timeout=30,
        )
        assert proc.returncode == 2

    def test_stale_env_var_cannot_suppress_dispatch(self):
        env = dict(os.environ, TRADER_CTL_LIB="1")
        proc = subprocess.run(
            [str(SCRIPT), "badcmd"], capture_output=True, text=True,
            env=env, timeout=30,
        )
        assert proc.returncode == 2


class TestStatusReportsNextLogin:
    @pytest.mark.parametrize(
        "line,expected",
        [(DISABLED_TRUE, "stays stopped"), (ENABLED_FALSE, "STARTS AUTOMATICALLY"),
         (ABSENT, "STARTS AUTOMATICALLY")],
    )
    def test_next_login_state(self, harness, line, expected):
        proc, _ = harness(disabled_line=line, cmd="status")
        assert expected in proc.stdout
