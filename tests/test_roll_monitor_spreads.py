"""
test_roll_monitor_spreads.py — Unit tests for spread danger zone detection in roll_monitor.py (v1.6)
=====================================================================================================
Tests the new CCS/PCS danger zone logic in build_roll_forward_candidates() and
build_btc_candidates() without any live yfinance calls.

Scenarios covered:
  build_roll_forward_candidates():
    - Covered call: ITM + DTE ≤5 → appears (pre-existing behaviour preserved)
    - Covered call: OTM → does NOT appear (pre-existing behaviour preserved)
    - CCS spread in danger zone (short < price < long) + DTE ≤5 → appears
    - PCS spread in danger zone (short < price < long) + DTE ≤5 → appears
    - Spread: price BELOW short_strike (both legs OTM) → does NOT appear
    - Spread: price ABOVE long_strike (both legs ITM) → does NOT appear
    - Spread: DTE >5 → does NOT appear in roll-forward
    - Spread: DTE =0 (expired today) → does NOT appear
    - is_spread=True on returned spread entries
    - spread_type field present on spread entries

  build_btc_candidates():
    - Covered call: DTE 5–14, no BTC → appears (pre-existing behaviour preserved)
    - Covered call: BTC order exists → does NOT appear (pre-existing behaviour)
    - CCS spread in danger zone + DTE 5–14, no BTC → appears
    - PCS spread in danger zone + DTE 5–14, no BTC → appears
    - Spread: BTC order exists → does NOT appear
    - Spread: DTE ≤5 → NOT in BTC (belongs in roll-forward instead)
    - Spread: DTE >14 → does NOT appear
    - Spread: price outside legs → does NOT appear
"""

import sys, os
from datetime import date, timedelta
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from roll_monitor import build_roll_forward_candidates, build_btc_candidates

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

TODAY = date.today()

def _exp(days_out: int) -> str:
    return (TODAY + timedelta(days=days_out)).strftime("%Y-%m-%d")


def _cc(symbol, strike, dte, btc=False, qty=1, purchase_price=2.50):
    """Synthetic covered-call detail record."""
    return {
        "symbol":          symbol,
        "strike":          float(strike),
        "expiration":      _exp(dte),
        "quantity":        qty,
        "btc_order_exists":btc,
        "option_id":       "OPT001",
        "purchase_price":  purchase_price,
    }


def _spread(symbol, spread_type, short_strike, long_strike, dte, btc=False, qty=1):
    """Synthetic spread detail record."""
    return {
        "symbol":          symbol,
        "type":            spread_type,
        "short_strike":    float(short_strike),
        "long_strike":     float(long_strike),
        "expiration":      _exp(dte),
        "quantity":        qty,
        "btc_order_exists":btc,
        "purchase_price":  5.00,
    }


# Suppress all live price fetches and option chain fetches
_NO_LIVE = patch("roll_monitor._fresh_price", return_value=0.0)
_NO_MID  = patch("roll_monitor._fetch_spread_mid", return_value=None)


# ─────────────────────────────────────────────────────────────────────────────
# build_roll_forward_candidates
# ─────────────────────────────────────────────────────────────────────────────

class TestRollForwardCandidates:
    def test_itm_covered_call_within_5d_appears(self):
        """Pre-existing: ITM covered call ≤5 DTE shows up in roll-forward."""
        cc = _cc("AAPL", strike=200.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [cc],
                live_prices={"AAPL": 210.0},   # price > strike → ITM
                name_map={"AAPL": "Apple"},
            )
        symbols = [r["symbol"] for r in result]
        assert "AAPL" in symbols
        # Verify is_spread is NOT set (or falsy) on covered-call entries
        rec = next(r for r in result if r["symbol"] == "AAPL")
        assert not rec.get("is_spread")

    def test_otm_covered_call_not_in_roll_forward(self):
        """Pre-existing: OTM covered call does NOT show up."""
        cc = _cc("AAPL", strike=220.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [cc],
                live_prices={"AAPL": 200.0},   # price < strike → OTM
                name_map={},
            )
        assert not any(r["symbol"] == "AAPL" for r in result)

    def test_ccs_spread_in_danger_zone_within_5d_appears(self):
        """CCS: 800 < 815 < 825 → in danger zone, DTE=4 → roll-forward."""
        sp = _spread("NVDA", "CCS", short_strike=800.0, long_strike=825.0, dte=4)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"NVDA": 815.0},
                name_map={"NVDA": "NVIDIA"},
                spread_contracts=[sp],
            )
        assert any(r["symbol"] == "NVDA" for r in result)
        rec = next(r for r in result if r["symbol"] == "NVDA")
        assert rec.get("is_spread") is True
        assert rec.get("spread_type") == "CCS"

    def test_pcs_spread_in_danger_zone_within_5d_appears(self):
        """PCS: 700 < 715 < 730 → in danger zone, DTE=3 → roll-forward."""
        sp = _spread("TSLA", "PCS", short_strike=700.0, long_strike=730.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"TSLA": 715.0},
                name_map={"TSLA": "Tesla"},
                spread_contracts=[sp],
            )
        assert any(r["symbol"] == "TSLA" for r in result)
        rec = next(r for r in result if r["symbol"] == "TSLA")
        assert rec.get("is_spread") is True
        assert rec.get("spread_type") == "PCS"

    def test_spread_price_below_both_legs_not_in_roll(self):
        """Price below min(short, long) — both legs OTM — no alert."""
        sp = _spread("MSFT", "CCS", short_strike=400.0, long_strike=420.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"MSFT": 380.0},   # price < 400 → safe
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "MSFT" for r in result)

    def test_spread_price_above_both_legs_not_in_roll(self):
        """Price above max(short, long) — beyond danger zone — no alert."""
        sp = _spread("MSFT", "CCS", short_strike=400.0, long_strike=420.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"MSFT": 430.0},   # price > 420 → beyond spread
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "MSFT" for r in result)

    def test_spread_dte_greater_than_5_not_in_roll(self):
        """DTE=6 → belongs in BTC window, not roll-forward."""
        sp = _spread("AMZN", "CCS", short_strike=200.0, long_strike=210.0, dte=6)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"AMZN": 205.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "AMZN" for r in result)

    def test_spread_dte_negative_not_in_roll(self):
        """DTE<0 (already expired) → no alert. DTE=0 (expiring today) is still shown."""
        # Use dte=-1 to simulate a past-expiry contract (should be excluded)
        sp = _spread("META", "PCS", short_strike=400.0, long_strike=420.0, dte=-1)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [],
                live_prices={"META": 410.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "META" for r in result)

    def test_empty_spread_contracts_no_crash(self):
        """Passing spread_contracts=[] (default) works without error."""
        cc = _cc("AAPL", strike=200.0, dte=3)
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [cc],
                live_prices={"AAPL": 210.0},
                name_map={},
                spread_contracts=[],
            )
        assert isinstance(result, list)

    def test_none_spread_contracts_no_crash(self):
        """Passing spread_contracts=None works without error."""
        with _NO_LIVE, _NO_MID:
            result = build_roll_forward_candidates(
                [], live_prices={}, name_map={}, spread_contracts=None
            )
        assert result == []


# ─────────────────────────────────────────────────────────────────────────────
# build_btc_candidates
# ─────────────────────────────────────────────────────────────────────────────

class TestBTCCandidates:
    def test_covered_call_5_to_14d_no_btc_appears(self):
        """Pre-existing: covered call DTE 6–14, no BTC → appears in BTC list."""
        cc = _cc("AAPL", strike=200.0, dte=10, btc=False)
        with _NO_LIVE, _NO_MID:
            with patch("roll_monitor._fetch_current_mid", return_value=0.50, create=True):
                result = build_btc_candidates(
                    [cc],
                    live_prices={"AAPL": 195.0},
                    name_map={"AAPL": "Apple"},
                )
        assert any(r["symbol"] == "AAPL" for r in result)
        rec = next(r for r in result if r["symbol"] == "AAPL")
        assert not rec.get("is_spread")

    def test_covered_call_with_btc_order_not_in_list(self):
        """Pre-existing: covered call with BTC order → excluded."""
        cc = _cc("AAPL", strike=200.0, dte=10, btc=True)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [cc],
                live_prices={"AAPL": 195.0},
                name_map={},
            )
        assert not any(r["symbol"] == "AAPL" for r in result)

    def test_ccs_spread_in_danger_zone_5_to_14d_no_btc_appears(self):
        """CCS in danger zone, DTE=10, no BTC → BTC candidate."""
        sp = _spread("NVDA", "CCS", short_strike=800.0, long_strike=825.0, dte=10, btc=False)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [],
                live_prices={"NVDA": 815.0},
                name_map={"NVDA": "NVIDIA"},
                spread_contracts=[sp],
            )
        assert any(r["symbol"] == "NVDA" for r in result)
        rec = next(r for r in result if r["symbol"] == "NVDA")
        assert rec.get("is_spread") is True
        assert rec.get("spread_type") == "CCS"

    def test_pcs_spread_in_danger_zone_5_to_14d_no_btc_appears(self):
        """PCS in danger zone, DTE=7, no BTC → BTC candidate."""
        sp = _spread("TSLA", "PCS", short_strike=700.0, long_strike=730.0, dte=7, btc=False)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [],
                live_prices={"TSLA": 715.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert any(r["symbol"] == "TSLA" for r in result)

    def test_spread_with_btc_order_not_in_btc_list(self):
        """Spread with existing BTC order → NOT shown in BTC candidates."""
        sp = _spread("NVDA", "CCS", short_strike=800.0, long_strike=825.0,
                     dte=10, btc=True)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [],
                live_prices={"NVDA": 815.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "NVDA" for r in result)

    def test_spread_dte_5_or_less_not_in_btc(self):
        """DTE ≤5 belongs in roll-forward, not BTC window (exclusive lower bound)."""
        sp = _spread("META", "CCS", short_strike=550.0, long_strike=570.0, dte=5)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [],
                live_prices={"META": 560.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "META" for r in result)

    def test_spread_dte_greater_than_14_not_in_btc(self):
        """DTE=15 → too far out, not in BTC window."""
        sp = _spread("AMZN", "PCS", short_strike=200.0, long_strike=220.0, dte=15)
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [],
                live_prices={"AMZN": 210.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "AMZN" for r in result)

    def test_spread_price_outside_legs_not_in_btc(self):
        """Price outside the spread legs → no danger → no BTC alert."""
        sp = _spread("GOOG", "CCS", short_strike=180.0, long_strike=200.0, dte=10)
        with _NO_LIVE, _NO_MID:
            # Price is 170 — below both legs (safe)
            result = build_btc_candidates(
                [],
                live_prices={"GOOG": 170.0},
                name_map={},
                spread_contracts=[sp],
            )
        assert not any(r["symbol"] == "GOOG" for r in result)

    def test_none_spread_contracts_no_crash(self):
        """Passing spread_contracts=None works without error."""
        with _NO_LIVE, _NO_MID:
            result = build_btc_candidates(
                [], live_prices={}, name_map={}, spread_contracts=None
            )
        assert result == []
