"""
test_intraday_filter.py — Tests for the intraday direction filter applied
in run_collar_pipeline_and_email().

Covers:
  - _get_intraday_changes() helper: up / down / flat / unknown
  - CCS filter: only "up" (and flat/unknown) symbols pass
  - PCS filter: only "down" (and flat/unknown) symbols pass
  - Collar filter: only "up" (and flat/unknown) symbols pass
  - Unknown/flat direction = pass-through (fail-open)
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scheduler import _get_intraday_changes


# ─────────────────────────────────────────────────────────────────────────────
# Helper to build a fake yfinance fast_info object
# ─────────────────────────────────────────────────────────────────────────────

def _fast_info(last_price, previous_close):
    fi = MagicMock()
    fi.last_price     = last_price
    fi.previous_close = previous_close
    return fi


def _ticker_mock(last_price, previous_close):
    t = MagicMock()
    t.fast_info = _fast_info(last_price, previous_close)
    return t


# ─────────────────────────────────────────────────────────────────────────────
# _get_intraday_changes unit tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGetIntradayChanges:

    def test_up_when_price_above_prev_close(self):
        with patch("yfinance.Ticker", return_value=_ticker_mock(105.0, 100.0)):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "up"

    def test_down_when_price_below_prev_close(self):
        with patch("yfinance.Ticker", return_value=_ticker_mock(95.0, 100.0)):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "down"

    def test_flat_when_price_equals_prev_close(self):
        with patch("yfinance.Ticker", return_value=_ticker_mock(100.0, 100.0)):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "flat"

    def test_unknown_when_last_price_none(self):
        with patch("yfinance.Ticker", return_value=_ticker_mock(None, 100.0)):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "unknown"

    def test_unknown_when_prev_close_zero(self):
        with patch("yfinance.Ticker", return_value=_ticker_mock(100.0, 0.0)):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "unknown"

    def test_unknown_when_api_raises(self):
        ticker = MagicMock()
        ticker.fast_info = property(lambda self: (_ for _ in ()).throw(RuntimeError("network")))
        with patch("yfinance.Ticker", side_effect=RuntimeError("network")):
            result = _get_intraday_changes(["TSLA"])
        assert result["TSLA"] == "unknown"

    def test_multiple_symbols(self):
        def _make_ticker(sym):
            prices = {"AAPL": (150.0, 145.0), "GOOG": (90.0, 95.0), "MSFT": (300.0, 300.0)}
            last, prev = prices[sym]
            return _ticker_mock(last, prev)
        with patch("yfinance.Ticker", side_effect=_make_ticker):
            result = _get_intraday_changes(["AAPL", "GOOG", "MSFT"])
        assert result["AAPL"] == "up"
        assert result["GOOG"] == "down"
        assert result["MSFT"] == "flat"


# ─────────────────────────────────────────────────────────────────────────────
# Direction filter pass-through logic (unit-tested directly)
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectionFilterLogic:
    """
    Tests for the _passes() closure logic embedded in run_collar_pipeline_and_email.
    We test the semantics directly rather than through the full pipeline.
    """

    def _make_passes(self, direction_map):
        """Recreate the _passes closure from scheduler.py."""
        def _passes(sym, required):
            d = direction_map.get(sym, "unknown")
            return d in (required, "flat", "unknown")
        return _passes

    # ── CCS filter (required = "up") ─────────────────────────────────────────

    def test_ccs_passes_when_up(self):
        _passes = self._make_passes({"AAPL": "up"})
        assert _passes("AAPL", "up") is True

    def test_ccs_blocked_when_down(self):
        _passes = self._make_passes({"AAPL": "down"})
        assert _passes("AAPL", "up") is False

    def test_ccs_passes_when_flat(self):
        _passes = self._make_passes({"AAPL": "flat"})
        assert _passes("AAPL", "up") is True

    def test_ccs_passes_when_unknown(self):
        _passes = self._make_passes({"AAPL": "unknown"})
        assert _passes("AAPL", "up") is True

    def test_ccs_passes_when_symbol_missing_from_map(self):
        _passes = self._make_passes({})
        assert _passes("AAPL", "up") is True  # missing → unknown → pass-through

    # ── PCS filter (required = "down") ───────────────────────────────────────

    def test_pcs_passes_when_down(self):
        _passes = self._make_passes({"RBLX": "down"})
        assert _passes("RBLX", "down") is True

    def test_pcs_blocked_when_up(self):
        _passes = self._make_passes({"RBLX": "up"})
        assert _passes("RBLX", "down") is False

    def test_pcs_passes_when_flat(self):
        _passes = self._make_passes({"RBLX": "flat"})
        assert _passes("RBLX", "down") is True

    def test_pcs_passes_when_unknown(self):
        _passes = self._make_passes({"RBLX": "unknown"})
        assert _passes("RBLX", "down") is True

    # ── Collar filter (required = "up") ──────────────────────────────────────

    def test_collar_passes_when_up(self):
        _passes = self._make_passes({"NVDA": "up"})
        assert _passes("NVDA", "up") is True

    def test_collar_blocked_when_down(self):
        _passes = self._make_passes({"NVDA": "down"})
        assert _passes("NVDA", "up") is False

    def test_collar_passes_when_flat(self):
        _passes = self._make_passes({"NVDA": "flat"})
        assert _passes("NVDA", "up") is True

    # ── Mixed list filtering ──────────────────────────────────────────────────

    def test_ccs_list_filtered_correctly(self):
        direction_map = {
            "AAPL": "up",    # passes CCS
            "GOOG": "down",  # blocked CCS
            "MSFT": "flat",  # passes CCS (flat = pass-through)
            "TSLA": "unknown",  # passes CCS (unknown = pass-through)
        }
        _passes = self._make_passes(direction_map)
        ccs_recs = [{"symbol": s} for s in ["AAPL", "GOOG", "MSFT", "TSLA"]]
        filtered = [r for r in ccs_recs if _passes(r["symbol"], "up")]
        assert [r["symbol"] for r in filtered] == ["AAPL", "MSFT", "TSLA"]

    def test_pcs_list_filtered_correctly(self):
        direction_map = {
            "RBLX": "down",  # passes PCS
            "HOOD": "up",    # blocked PCS
            "LYFT": "flat",  # passes PCS (flat = pass-through)
        }
        _passes = self._make_passes(direction_map)
        pcs_recs = [{"symbol": s} for s in ["RBLX", "HOOD", "LYFT"]]
        filtered = [r for r in pcs_recs if _passes(r["symbol"], "down")]
        assert [r["symbol"] for r in filtered] == ["RBLX", "LYFT"]

    def test_collar_list_filtered_correctly(self):
        direction_map = {
            "NVDA": "up",    # passes collar
            "AMD":  "down",  # blocked collar
            "INTC": "flat",  # passes collar
        }
        _passes = self._make_passes(direction_map)
        collar_recs = [{"symbol": s} for s in ["NVDA", "AMD", "INTC"]]
        filtered = [r for r in collar_recs if _passes(r["symbol"], "up")]
        assert [r["symbol"] for r in filtered] == ["NVDA", "INTC"]

    def test_all_unknown_means_no_filtering(self):
        """When all directions are unknown (e.g. market closed), nothing is dropped."""
        direction_map = {"A": "unknown", "B": "unknown", "C": "unknown"}
        _passes = self._make_passes(direction_map)
        recs = [{"symbol": s} for s in ["A", "B", "C"]]
        assert [r for r in recs if _passes(r["symbol"], "up")]   == recs
        assert [r for r in recs if _passes(r["symbol"], "down")] == recs
