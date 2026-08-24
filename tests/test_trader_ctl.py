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

    # No scheduler process anywhere, whatever is really running on this Mac.
    _write_exec(bindir / "pgrep", "#!/bin/bash\nexit 1\n")

    def run(*, disabled_line: str, disable_rc: int = 0, cmd: str = "stop"):
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


class TestStatusReportsNextLogin:
    @pytest.mark.parametrize(
        "line,expected",
        [(DISABLED_TRUE, "stays stopped"), (ENABLED_FALSE, "STARTS AUTOMATICALLY"),
         (ABSENT, "STARTS AUTOMATICALLY")],
    )
    def test_next_login_state(self, harness, line, expected):
        proc, _ = harness(disabled_line=line, cmd="status")
        assert expected in proc.stdout
