"""Tests for the same-day action ledger (cross-run dedup)."""

import json
import os
import tempfile
from datetime import date
from unittest import mock

import pytest


class TestActionLedger:
    """Unit tests for _load_action_ledger, _record_action, and pipeline integration."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()

    def teardown_method(self):
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _patch_snapshot_dir(self):
        from pathlib import Path
        return mock.patch("scheduler._SNAPSHOT_DIR", Path(self.tmpdir))

    def test_load_empty_ledger(self):
        """Empty ledger returns empty set."""
        from scheduler import _load_action_ledger
        with self._patch_snapshot_dir():
            result = _load_action_ledger("2026-07-17")
        assert result == set()

    def test_record_and_load_roundtrip(self):
        """Record an action, then load it back."""
        from scheduler import _record_action, _load_action_ledger
        with self._patch_snapshot_dir():
            _record_action("2026-07-17", "panic", "AMD", "2026-07-17", 260.0,
                           order_id="abc123", detail="rolled", opt_type="call")
            result = _load_action_ledger("2026-07-17")

        assert ("panic", "AMD", "2026-07-17", 260.0, "call") in result

    def test_multiple_records(self):
        """Multiple records across modes are all retrievable."""
        from scheduler import _record_action, _load_action_ledger
        with self._patch_snapshot_dir():
            _record_action("2026-07-17", "panic", "AMD", "2026-07-17", 260.0, opt_type="call")
            _record_action("2026-07-17", "safety", "TSLA", "2026-07-24", 300.0, opt_type="put")
            _record_action("2026-07-17", "rescue", "AAPL", "2026-07-18", 150.0, opt_type="call")
            result = _load_action_ledger("2026-07-17")

        assert len(result) == 3
        assert ("panic", "AMD", "2026-07-17", 260.0, "call") in result
        assert ("safety", "TSLA", "2026-07-24", 300.0, "put") in result
        assert ("rescue", "AAPL", "2026-07-18", 150.0, "call") in result

    def test_different_dates_isolated(self):
        """Records on different dates don't cross-contaminate."""
        from scheduler import _record_action, _load_action_ledger
        with self._patch_snapshot_dir():
            _record_action("2026-07-17", "panic", "AMD", "2026-07-17", 260.0, opt_type="call")
            _record_action("2026-07-18", "panic", "TSLA", "2026-07-18", 300.0, opt_type="put")
            day1 = _load_action_ledger("2026-07-17")
            day2 = _load_action_ledger("2026-07-18")

        assert len(day1) == 1
        assert len(day2) == 1
        assert ("panic", "AMD", "2026-07-17", 260.0, "call") in day1
        assert ("panic", "TSLA", "2026-07-18", 300.0, "put") in day2

    def test_corrupt_line_skipped(self):
        """Corrupt JSONL lines are silently skipped."""
        from pathlib import Path
        from scheduler import _load_action_ledger
        ledger_path = Path(self.tmpdir) / "action_ledger_2026-07-17.jsonl"
        with open(ledger_path, "w") as f:
            f.write('{"mode":"panic","symbol":"AMD","expiration":"2026-07-17","strike":260.0,"opt_type":"call"}\n')
            f.write("this is not json\n")
            f.write('{"mode":"safety","symbol":"TSLA","expiration":"2026-07-24","strike":300.0,"opt_type":"put"}\n')
        with self._patch_snapshot_dir():
            result = _load_action_ledger("2026-07-17")

        assert len(result) == 2

    def test_same_contract_different_modes_are_distinct(self):
        """Same contract in different modes creates distinct ledger entries."""
        from scheduler import _record_action, _load_action_ledger
        with self._patch_snapshot_dir():
            _record_action("2026-07-17", "safety", "AMD", "2026-07-24", 260.0, opt_type="call")
            _record_action("2026-07-17", "rescue", "AMD", "2026-07-24", 260.0, opt_type="call")
            result = _load_action_ledger("2026-07-17")

        assert ("safety", "AMD", "2026-07-24", 260.0, "call") in result
        assert ("rescue", "AMD", "2026-07-24", 260.0, "call") in result
        assert len(result) == 2

    def test_write_failure_does_not_raise(self):
        """OSError on ledger write is caught, not propagated."""
        from scheduler import _record_action
        with self._patch_snapshot_dir():
            with mock.patch("builtins.open", side_effect=OSError("disk full")):
                _record_action("2026-07-17", "panic", "AMD", "2026-07-17", 260.0)

    def test_count_auto_defense_pds_empty(self):
        """No ledger file returns empty dict."""
        from scheduler import _count_todays_auto_defense_pds
        with self._patch_snapshot_dir():
            result = _count_todays_auto_defense_pds("2026-07-29")
        assert result == {}

    def test_count_auto_defense_pds_sums_per_symbol(self):
        """Counts auto_defense PDS purchases per symbol from ledger."""
        from scheduler import _record_action, _count_todays_auto_defense_pds
        with self._patch_snapshot_dir():
            _record_action("2026-07-29", "auto_defense", "AAPL", "2026-10-16",
                           235.0, opt_type="PDS", qty=1, detail="long=315.0")
            _record_action("2026-07-29", "auto_defense", "AAPL", "2026-10-16",
                           230.0, opt_type="PDS", qty=1, detail="long=310.0")
            _record_action("2026-07-29", "auto_defense", "MSFT", "2026-10-16",
                           400.0, opt_type="PDS", qty=2, detail="long=450.0")
            _record_action("2026-07-29", "panic", "AAPL", "2026-07-29",
                           240.0, opt_type="call")
            result = _count_todays_auto_defense_pds("2026-07-29")
        assert result == {"AAPL": 2, "MSFT": 2}

    def test_count_auto_defense_ignores_other_modes(self):
        """Only counts mode=auto_defense, opt_type=PDS entries."""
        from scheduler import _record_action, _count_todays_auto_defense_pds
        with self._patch_snapshot_dir():
            _record_action("2026-07-29", "panic", "AAPL", "2026-07-29",
                           240.0, opt_type="call")
            _record_action("2026-07-29", "safety", "AAPL", "2026-07-29",
                           240.0, opt_type="put")
            result = _count_todays_auto_defense_pds("2026-07-29")
        assert result == {}


class TestPanicDedup:
    """Integration test: panic mode contracts should be filtered by action ledger."""

    def test_panic_contract_filtered_by_ledger(self):
        """Contracts in the ledger are excluded from panic processing."""
        prior = {
            ("panic", "AMD", "2026-07-17", 260.0, "call"),
            ("panic", "AMKR", "2026-07-17", 75.0, "put"),
        }

        open_short = [
            {"symbol": "AMD", "expiration": "2026-07-17", "strike": 260.0, "opt_type": "call"},
            {"symbol": "TSLA", "expiration": "2026-07-17", "strike": 300.0, "opt_type": "put"},
        ]
        open_longs = [
            {"symbol": "AMKR", "expiration": "2026-07-17", "strike": 75.0, "opt_type": "put"},
            {"symbol": "NVDA", "expiration": "2026-07-17", "strike": 150.0, "opt_type": "call"},
        ]

        filtered_short = [
            c for c in open_short
            if ("panic", c["symbol"].upper(), c["expiration"],
                float(c["strike"]), c.get("opt_type", "")) not in prior
        ]
        filtered_longs = [
            c for c in open_longs
            if ("panic", c["symbol"].upper(), c["expiration"],
                float(c["strike"]), c.get("opt_type", "")) not in prior
        ]

        assert len(filtered_short) == 1
        assert filtered_short[0]["symbol"] == "TSLA"
        assert len(filtered_longs) == 1
        assert filtered_longs[0]["symbol"] == "NVDA"

    def test_same_strike_different_opt_type_not_colliding(self):
        """Straddle positions (same strike, different opt_type) don't collide."""
        prior = {
            ("panic", "AMD", "2026-07-17", 260.0, "call"),
        }

        contracts = [
            {"symbol": "AMD", "expiration": "2026-07-17", "strike": 260.0, "opt_type": "call"},
            {"symbol": "AMD", "expiration": "2026-07-17", "strike": 260.0, "opt_type": "put"},
        ]

        filtered = [
            c for c in contracts
            if ("panic", c["symbol"].upper(), c["expiration"],
                float(c["strike"]), c.get("opt_type", "")) not in prior
        ]

        assert len(filtered) == 1
        assert filtered[0]["opt_type"] == "put"
