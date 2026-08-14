"""Tests for machine-timezone-independent job scheduling.

The `schedule` library has no timezone support: it fires on the machine's
local wall-clock time.  Job times are configured in a declared zone — keys
ending `_et` are Eastern, keys ending `_pt` are Pacific — so every one of them
must be converted to whatever local actually is before registration.

Historically `LOCAL` was hardcoded to America/Los_Angeles, which is correct
only while the machine is on PT.  On any other machine every ET-derived job
silently shifted, and the PT-derived jobs were passed through unconverted.

These tests monkeypatch `_local_tz` rather than reloading the module: a reload
rebinds `_shutdown`/`_last_tick`, which would race the live scheduler threads
started by test_scheduler_lifecycle.py.
"""

import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scheduler


PACIFIC = ZoneInfo("America/Los_Angeles")
EASTERN = ZoneInfo("America/New_York")
UTC = ZoneInfo("UTC")


def _offset_hours(a: ZoneInfo, b: ZoneInfo) -> int:
    """Whole-hour offset from zone a to zone b, evaluated today.

    Computed rather than hardcoded so these tests do not start failing at the
    next DST transition — the previous version asserted a literal "14:15",
    which is only correct while ET is UTC-4.
    """
    now = datetime.now(a)
    return round((now.utcoffset() - now.astimezone(b).utcoffset()).total_seconds() / 3600)


def _shift(hhmm: str, hours: int) -> str:
    h, m = map(int, hhmm.split(":"))
    return f"{(h + hours) % 24:02d}:{m:02d}"


class TestLocalTz:
    """_local_tz must return a real DST-aware zone, not a fixed-offset snapshot."""

    def test_returns_dst_aware_zone(self, monkeypatch):
        monkeypatch.setenv("TZ", "America/New_York")
        tz = scheduler._local_tz()
        jan = datetime(2026, 1, 15, 12, 0, tzinfo=UTC).astimezone(tz)
        jul = datetime(2026, 7, 15, 12, 0, tzinfo=UTC).astimezone(tz)
        assert jan.utcoffset() != jul.utcoffset(), (
            "a fixed-offset tzinfo gives the same offset year-round; historical "
            "timestamps in reporter.py would be bucketed an hour off across DST"
        )

    def test_honours_tz_environment_variable(self, monkeypatch):
        monkeypatch.setenv("TZ", "Asia/Tokyo")
        assert scheduler._local_tz().key == "Asia/Tokyo"

    def test_falls_back_to_system_zone_when_tz_unset(self, monkeypatch):
        monkeypatch.delenv("TZ", raising=False)
        tz = scheduler._local_tz()
        assert tz is not None
        # Must still be usable for conversion
        assert datetime.now(UTC).astimezone(tz) is not None


class TestConversionToLocal:

    def test_et_to_local_is_identity_on_eastern(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_local_tz", lambda: EASTERN)
        assert scheduler._et_to_local("03:30") == "03:30"
        assert scheduler._et_to_local("10:15") == "10:15"

    def test_pt_to_local_is_identity_on_pacific(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_local_tz", lambda: PACIFIC)
        assert scheduler._pt_to_local("08:15") == "08:15"
        assert scheduler._pt_to_local("12:15") == "12:15"

    def test_et_to_local_shifts_on_pacific(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_local_tz", lambda: PACIFIC)
        delta = -_offset_hours(EASTERN, PACIFIC)
        assert scheduler._et_to_local("10:15") == _shift("10:15", delta)

    def test_pt_to_local_shifts_on_eastern(self, monkeypatch):
        """The bug that mattered: PT-configured times passed through unconverted."""
        monkeypatch.setattr(scheduler, "_local_tz", lambda: EASTERN)
        delta = -_offset_hours(PACIFIC, EASTERN)
        assert scheduler._pt_to_local("08:15") == _shift("08:15", delta)
        assert delta == 3, "PT->ET should be +3h"


class TestResolveJobTimes:
    """Every configured job time must be converted — this is the regression guard.

    The previous fix converted four of the six registrations and left the two
    PT-configured ones raw.  Asserting the whole mapping in one place makes a
    missed conversion impossible to overlook.
    """

    CONFIG = {
        "portfolio_pull_time_et": "03:30",
        "early_pipeline_time_pt": "06:35",
        "pipeline_time_et": "10:15",
        "report_time_et": "22:00",
        "weekly_report_time_et": "09:00",
        "market_check_times_pt": ["08:15", "09:15", "10:15", "11:15", "12:15"],
    }

    def test_identity_on_pacific_matches_legacy_behaviour(self, monkeypatch):
        """On the PT laptop nothing changes — PT times stay, ET times shift back."""
        monkeypatch.setattr(scheduler, "_local_tz", lambda: PACIFIC)
        t = scheduler._resolve_job_times(self.CONFIG)
        assert t["early_pipeline"] == "06:35"
        assert t["market_checks"] == ["08:15", "09:15", "10:15", "11:15", "12:15"]
        delta = -_offset_hours(EASTERN, PACIFIC)
        assert t["daily_pipeline"] == _shift("10:15", delta)

    def test_all_times_converted_on_eastern(self, monkeypatch):
        """On the ET container, ET times are identity and PT times shift +3h."""
        monkeypatch.setattr(scheduler, "_local_tz", lambda: EASTERN)
        t = scheduler._resolve_job_times(self.CONFIG)

        assert t["portfolio_pull"] == "03:30"
        assert t["daily_pipeline"] == "10:15"
        assert t["options_report"] == "22:00"
        assert t["weekly_report"] == "09:00"
        # PT-configured jobs must be shifted, not passed through
        assert t["early_pipeline"] == "09:35", "06:35 PT is 09:35 ET"
        assert t["market_checks"] == ["11:15", "12:15", "13:15", "14:15", "15:15"]

    def test_no_market_check_lands_before_market_open_on_eastern(self, monkeypatch):
        """The money bug: an unconverted 08:15 fires pre-open and the catch-up
        branch runs a full live order-placing pipeline."""
        monkeypatch.setattr(scheduler, "_local_tz", lambda: EASTERN)
        t = scheduler._resolve_job_times(self.CONFIG)
        for hhmm in t["market_checks"]:
            assert hhmm >= "09:30", f"market check at {hhmm} ET is before the open"

    def test_every_configured_time_key_is_resolved(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_local_tz", lambda: EASTERN)
        t = scheduler._resolve_job_times(self.CONFIG)
        assert set(t) == {
            "portfolio_pull", "early_pipeline", "daily_pipeline",
            "options_report", "weekly_report", "market_checks",
        }

    def test_defaults_apply_for_absent_keys(self, monkeypatch):
        monkeypatch.setattr(scheduler, "_local_tz", lambda: PACIFIC)
        t = scheduler._resolve_job_times({})
        assert t["early_pipeline"] == "06:35"
        assert len(t["market_checks"]) >= 1
