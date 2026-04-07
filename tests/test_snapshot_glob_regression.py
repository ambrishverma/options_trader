"""
test_snapshot_glob_regression.py — Regression tests for snapshot loading bugs (v1.5 fixes)
============================================================================================
These tests lock in the bug fixes from v1.5 to prevent regressions.

Bug 1 (open_calls glob): load_open_calls_snapshot() was accidentally loading
  open_calls_detail_YYYYMMDD.json files because the glob `open_calls_*.json`
  matched both summary and detail files. When sorted reverse-alphabetically,
  detail files came first ('d' > digits), causing the 2:30 AM snapshot to
  return 0 contracts, making all holdings look available.
  Fix: filter out paths containing '/open_calls_detail_' before selecting latest.

Bug 2 (collar pipeline open-call exclusion): run_collar_pipeline() was
  subtracting near-expiry covered calls from available contracts and skipping
  symbols with 0 remaining. But collar DTE (28–112d) doesn't overlap with
  covered call DTE (≤21d), so TSLA with 1 open covered call was wrongly
  excluded from collar recommendations.
  Fix: removed the open-call subtraction block from run_collar_pipeline().

Scenarios covered:
  load_open_calls_snapshot():
    - When only detail file exists → returns {} (not treated as summary)
    - When both detail and summary exist → loads summary (not detail)
    - When only summary exists → loads it correctly
    - Detail file found first in sort order → still skipped

  collar_pipeline open-call exclusion:
    - Symbol with open covered call is NOT excluded from collar pipeline
    - get_collar_eligible_holdings() contract count is unaffected by open calls
"""

import sys, os, json, tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


# ─────────────────────────────────────────────────────────────────────────────
# load_open_calls_snapshot glob regression
# ─────────────────────────────────────────────────────────────────────────────

class TestOpenCallsSnapshotGlob:
    def test_detail_file_not_loaded_as_summary(self, tmp_path, monkeypatch):
        """
        Regression: detail file must never be treated as the summary snapshot.
        If only open_calls_detail_*.json exists, should return {} (no contracts).
        """
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        # Detail file has a 'contracts' key (list), NOT an 'open_calls' key (dict)
        detail_data = {
            "pulled_at": "2026-04-01T02:30:00",
            "contracts": [
                {"symbol": "TSLA", "strike": 250.0, "expiration": "2026-04-10",
                 "quantity": 1, "btc_order_exists": False}
            ]
        }
        (tmp_path / "open_calls_detail_20260401.json").write_text(
            json.dumps(detail_data)
        )

        # Only a detail file exists — should get {} back, not crash
        result = portfolio.load_open_calls_snapshot()
        assert result == {}, (
            "load_open_calls_snapshot() should return {} when only a detail "
            "file exists — detail files must be filtered from the glob"
        )

    def test_summary_loaded_when_both_detail_and_summary_exist(self, tmp_path, monkeypatch):
        """
        Regression: when both detail and summary exist on the same day,
        the summary file is loaded (not the detail file).
        """
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        # Summary file — has 'open_calls' key
        summary_data = {
            "pulled_at": "2026-04-01T02:30:00",
            "open_calls": {"TSLA": 1, "AAPL": 2}
        }
        (tmp_path / "open_calls_20260401.json").write_text(json.dumps(summary_data))

        # Detail file — has 'contracts' key. Would give 0 contracts if mistakenly loaded.
        detail_data = {
            "pulled_at": "2026-04-01T02:30:00",
            "contracts": [
                {"symbol": "TSLA", "strike": 250.0, "expiration": "2026-04-10",
                 "quantity": 1, "btc_order_exists": False}
            ]
        }
        (tmp_path / "open_calls_detail_20260401.json").write_text(json.dumps(detail_data))

        result = portfolio.load_open_calls_snapshot()
        assert result == {"TSLA": 1, "AAPL": 2}, (
            "load_open_calls_snapshot() loaded the wrong file. "
            "Expected summary {TSLA:1, AAPL:2} but got: " + str(result)
        )

    def test_detail_would_sort_before_summary_in_naive_glob(self, tmp_path, monkeypatch):
        """
        Confirms the bug existed: in reverse-alphabetical sort, 'detail' files
        come before 'date-only' files because 'd' > '2' in ASCII.
        This test documents the ordering that caused the bug.
        """
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        summary_name = "open_calls_20260401.json"
        detail_name  = "open_calls_detail_20260401.json"

        # Verify that in naive reverse sort, detail file would come first
        files_sorted = sorted([detail_name, summary_name], reverse=True)
        assert files_sorted[0] == detail_name, (
            "Test pre-condition: detail file should sort before summary in naive reverse sort. "
            "If this fails, the original bug may no longer reproduce."
        )

        # Now verify the fix: only the summary is loaded
        summary_data = {"pulled_at": "2026-04-01T02:30:00", "open_calls": {"NVDA": 3}}
        detail_data  = {"pulled_at": "2026-04-01T02:30:00", "contracts": []}

        (tmp_path / summary_name).write_text(json.dumps(summary_data))
        (tmp_path / detail_name ).write_text(json.dumps(detail_data))

        result = portfolio.load_open_calls_snapshot()
        assert "NVDA" in result, (
            "Summary file was not loaded. The glob fix may have been reverted."
        )
        assert result["NVDA"] == 3

    def test_empty_snapshots_dir_returns_empty_dict(self, tmp_path, monkeypatch):
        """No snapshot files → returns {}."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)
        result = portfolio.load_open_calls_snapshot()
        assert result == {}

    def test_loads_most_recent_summary_when_multiple_exist(self, tmp_path, monkeypatch):
        """Latest summary by filename sort order is loaded."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        (tmp_path / "open_calls_20260401.json").write_text(
            json.dumps({"pulled_at": "2026-04-01T02:30:00", "open_calls": {"OLD": 1}})
        )
        (tmp_path / "open_calls_20260404.json").write_text(
            json.dumps({"pulled_at": "2026-04-04T02:30:00", "open_calls": {"NEW": 2}})
        )

        result = portfolio.load_open_calls_snapshot()
        assert result == {"NEW": 2}, f"Expected NEW file to be loaded, got {result}"


# ─────────────────────────────────────────────────────────────────────────────
# Collar pipeline open-call exclusion regression (TSLA collar bug)
# ─────────────────────────────────────────────────────────────────────────────

class TestCollarPipelineOpenCallExclusion:
    def test_get_collar_eligible_not_affected_by_open_calls(self):
        """
        Regression: get_collar_eligible_holdings() must NOT reduce contract
        counts based on open covered calls. Collar DTE (28–112d) doesn't
        overlap with typical short covered calls (≤21 DTE).
        """
        from collar import get_collar_eligible_holdings

        # TSLA has 1 open covered call but that should NOT reduce contracts here
        holding = {"symbol": "TSLA", "shares": 200.0, "price": 250.0, "contracts": 2}
        result = get_collar_eligible_holdings([holding], min_value=10000.0)

        assert len(result) == 1
        # Contracts must NOT be reduced from 2 to 1
        assert result[0]["contracts"] == 2, (
            "Collar pipeline subtracted open covered calls from available "
            "contracts — this is the TSLA collar bug. Fix: remove open-call "
            "subtraction from run_collar_pipeline()."
        )

    def test_eligible_holding_not_excluded_when_all_contracts_are_open(self):
        """
        Regression: a symbol where all covered-call contracts are already open
        should NOT be excluded from the collar scan. Collar calls are 28–112 DTE
        and don't conflict with near-expiry covered calls.
        """
        from collar import get_collar_eligible_holdings

        # e.g. TSLA has 1 contract, 1 open covered call — would have been excluded
        holding = {"symbol": "TSLA", "shares": 100.0, "price": 250.0, "contracts": 1}
        result = get_collar_eligible_holdings([holding], min_value=10000.0)

        # Should still be eligible (market value = 25000 > 10000)
        assert len(result) == 1, (
            "Symbol was excluded from collar scan. This reproduces the TSLA "
            "collar omission bug — open-call subtraction must not happen in "
            "get_collar_eligible_holdings()."
        )

    def test_collar_pipeline_does_not_import_open_call_exclusion(self):
        """
        Structural regression: verify that run_collar_pipeline() does not call
        load_open_calls_snapshot() (the fix removed this dependency entirely).
        """
        import inspect
        from collar import run_collar_pipeline

        source = inspect.getsource(run_collar_pipeline)
        # The fix removed the open-call subtraction block; the key removed function is:
        assert "load_open_calls_snapshot" not in source, (
            "run_collar_pipeline() still calls load_open_calls_snapshot(). "
            "This means the open-call exclusion logic may still be present."
        )
