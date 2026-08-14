"""Regression tests for ET -> system-local schedule conversion.

The `schedule` library has no timezone support: it fires on the machine's
local wall-clock time.  Job times are configured in ET, so they must be
converted to whatever local actually is.

This used to be hardcoded to America/Los_Angeles.  On any machine not on PT
that silently shifted every job by the PT/ET offset — on an ET machine the
03:30 ET portfolio pull was handed to schedule as "00:30" and fired three
hours early.  The container runs TZ=America/New_York, so this must be right.
"""

import sys
import os
import time
import importlib

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


@pytest.fixture
def in_timezone():
    """Run a block with the process in a given timezone, then restore."""
    original = os.environ.get("TZ")

    def _set(tz_name):
        os.environ["TZ"] = tz_name
        time.tzset()
        import scheduler
        importlib.reload(scheduler)
        return scheduler

    yield _set

    if original is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = original
    time.tzset()
    import scheduler
    importlib.reload(scheduler)


class TestEtToLocal:

    def test_identity_when_machine_is_on_et(self, in_timezone):
        """The container runs on ET — conversion must be a no-op there."""
        scheduler = in_timezone("America/New_York")
        assert scheduler._et_to_local("03:30") == "03:30"
        assert scheduler._et_to_local("10:15") == "10:15"
        assert scheduler._et_to_local("22:00") == "22:00"

    def test_shifts_back_three_hours_when_machine_is_on_pt(self, in_timezone):
        """The laptop runs on PT — behaviour there is unchanged."""
        scheduler = in_timezone("America/Los_Angeles")
        assert scheduler._et_to_local("03:30") == "00:30"
        assert scheduler._et_to_local("10:15") == "07:15"

    def test_shifts_forward_when_machine_is_on_utc(self, in_timezone):
        """A VM left on UTC must still schedule correctly (ET is UTC-4 in DST)."""
        scheduler = in_timezone("UTC")
        # 10:15 EDT == 14:15 UTC
        assert scheduler._et_to_local("10:15") == "14:15"
