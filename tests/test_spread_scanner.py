"""
test_spread_scanner.py — Unit tests for spread_scanner.py (v1.6)
=================================================================
Tests scan_ccs(), scan_pcs(), and run_spread_weekly_pipeline() using
synthetic option chain data injected via monkeypatching — no live
network calls are made.

Scenarios covered:
  CCS / PCS:
    - Correct net credit calculation (short bid - long ask)
    - OTM filter: short leg must be >= 10% OTM
    - Target premium filter: net_credit >= threshold
    - DTE window filter: only expirations within [dte_min, dte_max]
    - Open interest filter: both legs must meet min OI
    - Long-leg matching: pick nearest strike to short + spread_size (single-width test)
    - Range evaluation: best score (YPD × C/L ratio) wins across all spread widths in [min, max]
    - Range no-match: returns None when no spread in range has a valid long leg
    - YPD formula: net_credit * 100 / dte
    - Score formula: YPD × credit_to_loss_ratio (ranking key)
    - Best score selection: highest score wins across multiple candidates
    - Returns None when no qualifying spread exists
    - Returns None when net_credit <= 0 (debit spread)
  run_spread_weekly_pipeline():
    - Returns {"ccs": [...], "pcs": [...]} structure
    - CCS list sorted by score (YPD × C/L ratio) descending
    - PCS list sorted by score (YPD × C/L ratio) descending
    - Handles holdings with no qualifying spreads (empty lists)
"""

import sys, os
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from spread_scanner import scan_ccs, scan_pcs, run_spread_weekly_pipeline

# ─────────────────────────────────────────────────────────────────────────────
# Helpers: build synthetic chain data
# ─────────────────────────────────────────────────────────────────────────────

TODAY = date.today()

def _exp(days_out: int) -> str:
    return (TODAY + timedelta(days=days_out)).strftime("%Y-%m-%d")


def _make_chain_data(
    current_price: float = 100.0,
    calls: list = None,
    puts: list = None,
    dte: int = 21,
) -> list:
    """Return a synthetic chain_data list (one expiration)."""
    return [{
        "expiration":    _exp(dte),
        "dte":           dte,
        "current_price": current_price,
        "calls": calls or [],
        "puts":  puts  or [],
    }]


def _call(strike, bid, ask, oi=50):
    mid = round((bid + ask) / 2, 2)
    return {"strike": strike, "bid": bid, "ask": ask, "mid": mid, "open_interest": oi}


def _put(strike, bid, ask, oi=50):
    mid = round((bid + ask) / 2, 2)
    return {"strike": strike, "bid": bid, "ask": ask, "mid": mid, "open_interest": oi}


# ─────────────────────────────────────────────────────────────────────────────
# Fixture: default config
# ─────────────────────────────────────────────────────────────────────────────

CFG = {
    "spread_dte_min":           14,
    "spread_dte_max":           42,
    "spread_short_otm_pct":     10.0,
    "spread_min_open_interest": 2,
    "spread_size_min_pct":      1.0,
    "spread_size_max_pct":      10.0,
    "spread_min_premium_pct":   1.0,
}


# ─────────────────────────────────────────────────────────────────────────────
# CCS tests
# ─────────────────────────────────────────────────────────────────────────────

def _ccs_with_chains(chain_data, **kwargs):
    """Call scan_ccs with injected chain data. Returns rec only (discards scenario count)."""
    with patch("spread_scanner._fetch_chains", return_value=chain_data):
        rec, _ = scan_ccs("TEST", name="Test Corp", **kwargs)
    return rec


class TestScanCCS:
    def test_returns_rec_for_qualifying_spread(self):
        """Basic qualifying CCS → returns a rec dict."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),   # short: 12% OTM, bid=2.00
                _call(115.0, bid=0.80, ask=1.00),   # long:  15% OTM, ask=1.00
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["type"] == "CCS"
        assert rec["symbol"] == "TEST"
        assert rec["short_leg"]["strike"] == 112.0
        assert rec["long_leg"]["strike"] == 115.0

    def test_net_credit_calculation(self):
        """net_credit = short bid - long ask."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["net_credit"] == round(2.00 - 1.00, 2)   # 1.00
        assert rec["net_credit_total"] == 100.0              # 1.00 * 100

    def test_ypd_formula(self):
        """YPD = net_credit * 100 / DTE."""
        chains = _make_chain_data(
            current_price=100.0, dte=20,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        expected_ypd = round(1.00 * 100 / 20, 4)   # 5.0
        assert rec["ypd"] == expected_ypd

    def test_max_loss_formula(self):
        """max_loss = (spread_size * 100) - net_credit_total."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),  # spread = 3.0
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        expected_max_loss = round(3.0 * 100 - 1.00 * 100, 2)   # 200.0
        assert rec["max_loss"] == expected_max_loss

    def test_short_otm_filter_rejected(self):
        """Short call strike < 10% OTM is rejected."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(108.0, bid=2.50, ask=2.70),   # only 8% OTM → rejected
                _call(112.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=4.0, spread_size_max_pct=4.0, min_premium_pct=0.0)
        # 108 is 8% OTM, below the 10% threshold; 112 becomes the short but
        # there's no higher-strike long candidate → no spread
        assert rec is None

    def test_short_otm_exactly_10pct_passes(self):
        """Short call at exactly 10% OTM passes."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=2.00, ask=2.20),   # exactly 10% OTM
                _call(113.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["short_leg"]["strike"] == 110.0

    def test_buffer_size_pct_honored(self):
        """
        Regression: short_otm_pct (buffer size) must filter out strikes below threshold.
        With buffer=15%, short call at 110 (only 10% OTM) must be rejected;
        short at 116 (16% OTM) must pass.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=3.00, ask=3.20),   # 10% OTM → below 15% buffer → REJECTED
                _call(116.0, bid=1.50, ask=1.70),   # 16% OTM → passes buffer
                _call(119.0, bid=0.80, ask=1.00),   # long leg
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=15.0, min_open_interest=2,
                               spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                               min_premium_pct=0.0)
        assert rec is not None
        assert rec["short_leg"]["strike"] == 116.0, (
            "Short leg at 110 (10% OTM) must be rejected when buffer=15%; "
            "116 (16% OTM) should be the short leg."
        )
        assert rec["short_leg"]["otm_pct"] >= 15.0

    def test_target_premium_filter(self):
        """net_credit must meet target_premium threshold."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=1.30, ask=1.50),   # short bid = 1.30
                _call(115.0, bid=0.60, ask=0.80),   # long  ask = 0.80 → net = 0.50
            ]
        )
        # target_premium = 0.60 — net=0.50 fails
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, target_premium=0.60)
        assert rec is None

    def test_target_premium_exact_passes(self):
        """net_credit == target_premium passes the filter."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=1.60, ask=1.80),   # short bid = 1.60
                _call(115.0, bid=0.60, ask=1.00),   # long  ask = 1.00 → net = 0.60
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, target_premium=0.60)
        assert rec is not None

    def test_dte_window_filter_too_short(self):
        """Expiration DTE < dte_min is skipped."""
        chains = _make_chain_data(
            current_price=100.0, dte=10,   # below dte_min=14
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is None

    def test_dte_window_filter_too_long(self):
        """Expiration DTE > dte_max is skipped."""
        chains = _make_chain_data(
            current_price=100.0, dte=50,   # above dte_max=42
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is None

    def test_dte_at_boundary_passes(self):
        """DTE exactly at dte_min and dte_max both pass."""
        for dte in (14, 42):
            chains = _make_chain_data(
                current_price=100.0, dte=dte,
                calls=[
                    _call(112.0, bid=2.00, ask=2.20),
                    _call(115.0, bid=0.80, ask=1.00),
                ]
            )
            rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                                   short_otm_pct=10.0, min_open_interest=2,
                                   spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
            assert rec is not None, f"DTE={dte} should produce a rec"

    def test_open_interest_filter_both_legs(self):
        """Both legs must meet min_open_interest."""
        # Low OI on short leg
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, oi=1),  # oi=1, below min=2
                _call(115.0, bid=0.80, ask=1.00, oi=50),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is None

        # Low OI on long leg
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, oi=50),
                _call(115.0, bid=0.80, ask=1.00, oi=1),  # oi=1, below min=2
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is None

    def test_zero_bid_short_leg_rejected(self):
        """Short leg with bid=0 is rejected (no premium to collect)."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=0.0, ask=0.5),    # bid=0 → rejected
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None

    def test_debit_spread_rejected(self):
        """net_credit <= 0 (debit spread) is rejected."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=1.00, ask=1.20),   # short bid = 1.00
                _call(115.0, bid=0.90, ask=1.50),   # long  ask = 1.50 → net = -0.50
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None

    def test_zero_ask_long_leg_rejected(self):
        """
        Regression (NOW bug): long call with ask=0 must be rejected.
        Without this check, net_credit = short.bid - 0 = short.bid, which
        inflates the credit and produces a fictitious recommendation.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),    # short: valid
                _call(115.0, bid=0.00, ask=0.00),    # long: no market → must be rejected
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None, (
            "Long leg with ask=0 should be rejected — otherwise net credit is "
            "fictitiously inflated by the full short premium (NOW bug)."
        )

    def test_short_bid_exceeds_stock_price_rejected(self):
        """
        Regression (NFLX bug): short call bid >= current_price indicates
        stale/corrupt yfinance data (OTM calls can never be worth more than
        the underlying). Must be rejected regardless of OTM% filter.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=105.0, ask=110.0),  # bid > stock price → corrupt data
                _call(115.0, bid=0.80,  ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None, (
            "Short call bid >= current_price signals corrupt yfinance data "
            "(NFLX bug). Must be filtered out."
        )

    def test_best_ypd_selected_across_expirations(self):
        """When multiple expirations qualify, the highest score (YPD × C/L ratio) is returned."""
        chain1 = {
            "expiration": _exp(14), "dte": 14, "current_price": 100.0,
            "calls": [
                _call(112.0, bid=2.00, ask=2.20),   # net=1.00, YPD=100/14≈7.14
                _call(115.0, bid=0.80, ask=1.00),
            ], "puts": [],
        }
        chain2 = {
            "expiration": _exp(42), "dte": 42, "current_price": 100.0,
            "calls": [
                _call(112.0, bid=1.50, ask=1.70),   # net=0.50, YPD=50/42≈1.19
                _call(115.0, bid=0.60, ask=1.00),
            ], "puts": [],
        }
        with patch("spread_scanner._fetch_chains", return_value=[chain1, chain2]):
            rec, _ = scan_ccs("TEST", dte_min=14, dte_max=42,
                              short_otm_pct=10.0, min_open_interest=2,
                              spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["dte"] == 14   # shorter DTE had higher YPD

    def test_long_leg_picks_nearest_strike(self):
        """Long leg is the nearest available strike to short + spread_size (single-width: 3%)."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),   # short: 12% OTM
                _call(115.5, bid=0.80, ask=1.00),   # nearest to 115 (target=112+3)
                _call(118.0, bid=0.50, ask=0.70),   # further from 115
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        # target = 112 + 3 = 115; both 115.5 and 118 are >= 114.99
        # nearest to 115 is 115.5 (0.5 away) vs 118 (3 away)
        assert rec["long_leg"]["strike"] == 115.5

    def test_best_spread_across_range_ccs(self):
        """
        Range evaluation: scanner tries all spread widths in 1%-step increments
        and returns the combination with the highest score (YPD × C/L ratio).

        Setup: short=110 (10% OTM), long candidates at 112 and 115.
          spread ~$2 → long=112, net=1.00, YPD≈4.76, C/L=1.00, score≈4.76
          spread ~$5 → long=115, net=2.20, YPD≈10.48, C/L≈0.79, score≈8.24  ← should win
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=3.00, ask=3.20),   # short: exactly 10% OTM
                _call(112.0, bid=1.50, ask=2.00),   # long at spread ~$2: net=1.00
                _call(115.0, bid=0.50, ask=0.80),   # long at spread ~$5: net=2.20 (best)
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                               min_premium_pct=1.0)
        assert rec is not None
        assert rec["long_leg"]["strike"] == 115.0, (
            "Scanner should pick long=115 (score≈8.24) over long=112 (score≈4.76) "
            "across the full spread range."
        )
        assert rec["net_credit"] == 2.20

    def test_spread_size_range_no_match_ccs(self):
        """
        Returns None when no long-leg candidate exists for any spread size in range.
        Short at 110, long only at 112 (spread=$2). Range $5–$7 → long_target=115–117;
        no strike available there → None.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=3.00, ask=3.20),   # short: 10% OTM
                _call(112.0, bid=1.50, ask=2.00),   # only available long: $2 from short
            ]
        )
        # Range $5–$7: long_target = 110+[5,6,7] = [115,116,117] → no candidate
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min=5.0, spread_size_max=7.0,
                               min_premium_pct=0.0)
        assert rec is None, "No long call in the spread range [$5–$7] → should return None."

    def test_scenarios_evaluated_counter(self):
        """
        scenarios_evaluated counts (short_strike × spread_size) pairs actually tried
        (after the short-leg guards pass). With 2 valid short calls (112 and 115, both
        ≥10% OTM on a $100 stock) and 10 spread sizes ([1..10] at $1 step), the counter
        must be exactly 20 (2 shorts × 10 spread sizes).
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),   # valid short
                _call(115.0, bid=0.80, ask=1.00),   # long candidate
            ]
        )
        with patch("spread_scanner._fetch_chains", return_value=chains):
            rec, cnt = scan_ccs("TEST", dte_min=14, dte_max=42,
                                short_otm_pct=10.0, min_open_interest=2,
                                spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                                min_premium_pct=0.0)
        assert cnt == 20, f"Expected 20 scenarios (2 shorts × 10 spread sizes), got {cnt}"

    def test_credit_to_loss_ratio_ccs(self):
        """credit_to_loss_ratio = net_credit_total / max_loss."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),   # net=1.00/share
                _call(115.0, bid=0.80, ask=1.00),   # spread=3 → max_loss=200
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0)
        assert rec is not None
        # net_credit_total=100, max_loss=200 → ratio=0.50
        assert rec["credit_to_loss_ratio"] == round(100.0 / 200.0, 2)

    def test_returns_none_when_empty_chains(self):
        """Returns None when _fetch_chains returns empty list."""
        with patch("spread_scanner._fetch_chains", return_value=[]):
            rec, cnt = scan_ccs("TEST")
        assert rec is None
        assert cnt == 0

    def test_rec_dict_has_all_required_fields(self):
        """Returned rec contains all required fields per the PRD spec."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        for field in ("symbol", "name", "current_price", "type", "expiration",
                      "dte", "short_leg", "long_leg", "net_credit",
                      "net_credit_total", "max_loss", "spread_size", "ypd",
                      "credit_to_loss_ratio", "score"):
            assert field in rec, f"Missing field: {field}"
        for field in ("strike", "bid", "ask", "mid", "open_interest", "otm_pct"):
            assert field in rec["short_leg"], f"Missing short_leg field: {field}"
        for field in ("strike", "bid", "ask", "mid", "open_interest"):
            assert field in rec["long_leg"], f"Missing long_leg field: {field}"


# ─────────────────────────────────────────────────────────────────────────────
# PCS tests
# ─────────────────────────────────────────────────────────────────────────────

def _pcs_with_chains(chain_data, **kwargs):
    """Call scan_pcs with injected chain data. Returns rec only (discards scenario count)."""
    with patch("spread_scanner._fetch_chains", return_value=chain_data):
        rec, _ = scan_pcs("TEST", name="Test Corp", **kwargs)
    return rec


class TestScanPCS:
    def test_returns_rec_for_qualifying_spread(self):
        """Basic qualifying PCS → returns a rec dict."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20),    # short: 12% OTM below
                _put(85.0, bid=0.80, ask=1.00),    # long:  15% OTM below
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["type"] == "PCS"
        assert rec["short_leg"]["strike"] == 88.0
        assert rec["long_leg"]["strike"] == 85.0

    def test_net_credit_calculation(self):
        """net_credit = short bid - long ask."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20),
                _put(85.0, bid=0.80, ask=1.00),   # long ask = 1.00
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["net_credit"] == 1.00   # 2.00 - 1.00
        assert rec["net_credit_total"] == 100.0

    def test_otm_filter_put(self):
        """Short put must be >= 10% OTM (below current price)."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(93.0, bid=3.00, ask=3.20),   # only 7% OTM → rejected
                _put(90.0, bid=1.50, ask=1.70),   # 10% OTM
                _put(87.0, bid=0.80, ask=1.00),   # long
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        # 93 is only 7% OTM → rejected. 90 is exactly 10% OTM → valid short.
        assert rec is not None
        assert rec["short_leg"]["strike"] == 90.0

    def test_short_otm_field_is_positive(self):
        """OTM pct in rec is stored as a positive value for puts."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20),   # 12% OTM
                _put(85.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert rec is not None
        assert rec["short_leg"]["otm_pct"] > 0.0   # stored positive for puts

    def test_zero_ask_long_put_leg_rejected(self):
        """
        Regression (NOW-equivalent for PCS): long put with ask=0 is rejected.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20),    # short: valid
                _put(85.0, bid=0.00, ask=0.00),    # long: no market
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None, "Long put with ask=0 must be rejected."

    def test_short_put_bid_exceeds_stock_price_rejected(self):
        """
        Regression (NFLX-equivalent for PCS): short put bid >= current_price
        indicates stale/corrupt data and must be rejected.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=105.0, ask=110.0),  # bid > stock price → corrupt
                _put(85.0, bid=0.80,  ask=1.00),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None, "Short put bid >= current_price signals corrupt data."

    def test_debit_spread_rejected(self):
        """Long ask > short bid → net_credit <= 0 → rejected."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=1.00, ask=1.20),
                _put(85.0, bid=0.90, ask=1.50),   # long ask=1.50 > short bid=1.00
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=0.0)
        assert rec is None

    def test_spread_structure_ccs_vs_pcs(self):
        """CCS: short < long strike (bear call). PCS: short > long strike (bull put)."""
        call_chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[_call(112.0, bid=2.00, ask=2.20), _call(115.0, bid=0.80, ask=1.00)]
        )
        put_chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[_put(88.0, bid=2.00, ask=2.20), _put(85.0, bid=0.80, ask=1.00)]
        )
        ccs_rec = _ccs_with_chains(call_chains, dte_min=14, dte_max=42,
                                   short_otm_pct=10.0, min_open_interest=2,
                                   spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        pcs_rec = _pcs_with_chains(put_chains, dte_min=14, dte_max=42,
                                   short_otm_pct=10.0, min_open_interest=2,
                                   spread_size_min_pct=3.0, spread_size_max_pct=3.0, min_premium_pct=1.0)
        assert ccs_rec is not None
        assert pcs_rec is not None
        # CCS (bear call spread): sell lower call, buy higher call → short < long
        assert ccs_rec["short_leg"]["strike"] < ccs_rec["long_leg"]["strike"]
        # PCS (bull put spread): sell higher put, buy lower put → short > long
        assert pcs_rec["short_leg"]["strike"] > pcs_rec["long_leg"]["strike"]

    def test_best_spread_across_range_pcs(self):
        """
        Range evaluation for PCS: scanner tries all spread widths and returns
        the highest score (YPD × C/L ratio) combination.

        Setup: short=88 (12% OTM below $100), long candidates at 85 and 82.
          spread ~$3 → long=85, net=1.00, YPD≈4.76, C/L=0.50, score≈2.38
          spread ~$6 → long=82, net=2.20, YPD≈10.48, C/L≈0.58, score≈6.07  ← should win
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=3.00, ask=3.20),   # short: 12% OTM
                _put(85.0, bid=1.50, ask=2.00),   # long at spread ~$3: net=1.00
                _put(82.0, bid=0.50, ask=0.80),   # long at spread ~$6: net=2.20 (best)
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                               min_premium_pct=1.0)
        assert rec is not None
        assert rec["long_leg"]["strike"] == 82.0, (
            "Scanner should pick long=82 (score≈6.07) over long=85 (score≈2.38) "
            "across the full spread range."
        )
        assert rec["net_credit"] == 2.20

    def test_spread_size_range_no_match_pcs(self):
        """
        Returns None when no long-leg put candidate exists for any spread size in range.
        Short at 88, long only at 85 (spread=$3). Range $5–$7 → long_target=83–81;
        no put available at those strikes → None.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.50, ask=2.70),   # short: 12% OTM
                _put(85.0, bid=1.00, ask=1.20),   # only available long: $3 from short
            ]
        )
        # Range $5–$7: long_target = 88-[5,6,7] = [83,82,81] → no put available
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min=5.0, spread_size_max=7.0,
                               min_premium_pct=0.0)
        assert rec is None, "No long put in the spread range [$5–$7] → should return None."

    def test_returns_none_when_empty_chains(self):
        with patch("spread_scanner._fetch_chains", return_value=[]):
            rec, cnt = scan_pcs("TEST")
        assert rec is None
        assert cnt == 0


# ─────────────────────────────────────────────────────────────────────────────
# run_spread_weekly_pipeline tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_holdings(symbols=("AAPL", "TSLA")):
    return [{"symbol": s, "name": f"{s} Corp", "shares": 200.0,
             "price": 100.0, "eligible": True, "contracts": 2}
            for s in symbols]


class TestRunSpreadWeeklyPipeline:
    def test_returns_correct_structure(self):
        """Returns {"ccs": [...], "pcs": [...], "ccs_scenarios": int, "pcs_scenarios": int}."""
        with patch("spread_scanner._fetch_chains", return_value=[]):
            result = run_spread_weekly_pipeline(_make_holdings(), CFG)
        assert "ccs" in result
        assert "pcs" in result
        assert "ccs_scenarios" in result
        assert "pcs_scenarios" in result
        assert isinstance(result["ccs"], list)
        assert isinstance(result["pcs"], list)
        assert isinstance(result["ccs_scenarios"], int)
        assert isinstance(result["pcs_scenarios"], int)

    def test_empty_holdings_returns_empty_lists(self):
        result = run_spread_weekly_pipeline([], CFG)
        assert result["ccs"] == []
        assert result["pcs"] == []
        assert result["ccs_scenarios"] == 0
        assert result["pcs_scenarios"] == 0

    def test_ccs_sorted_by_score_descending(self):
        """CCS recs are sorted by score (YPD × C/L ratio) from highest to lowest."""
        # AAPL: DTE=42, net=1.00, spread=3, YPD≈2.38, C/L=0.50, score≈1.19
        # TSLA: DTE=14, net=1.50, spread=3, YPD≈10.71, C/L=1.00, score≈10.71  ← wins
        aapl_chain = [{
            "expiration": _exp(42), "dte": 42, "current_price": 100.0,
            "calls": [_call(112.0, bid=2.00, ask=2.20), _call(115.0, bid=0.60, ask=1.00)],
            "puts":  [],
        }]
        tsla_chain = [{
            "expiration": _exp(14), "dte": 14, "current_price": 100.0,
            "calls": [_call(112.0, bid=2.50, ask=2.70), _call(115.0, bid=0.80, ask=1.00)],
            "puts":  [],
        }]

        call_count = {"n": 0}
        def _fetch_side_effect(sym, dte_min, dte_max):
            call_count["n"] += 1
            return aapl_chain if sym == "AAPL" else tsla_chain

        with patch("spread_scanner._fetch_chains", side_effect=_fetch_side_effect):
            result = run_spread_weekly_pipeline(_make_holdings(("AAPL", "TSLA")), CFG)

        # Both should have CCS recs; TSLA's should come first (higher score)
        ccs = result["ccs"]
        assert len(ccs) == 2
        assert ccs[0]["score"] >= ccs[1]["score"]
        assert ccs[0]["symbol"] == "TSLA"

    def test_pcs_sorted_by_score_descending(self):
        """PCS recs are sorted by score (YPD × C/L ratio) from highest to lowest."""
        # low-score: DTE=42, net=1.00, spread=3, YPD≈2.38, C/L=0.50, score≈1.19
        # high-score: DTE=14, net=1.50, spread=3, YPD≈10.71, C/L=1.00, score≈10.71  ← wins
        low_score_chain = [{
            "expiration": _exp(42), "dte": 42, "current_price": 100.0,
            "calls": [],
            "puts": [_put(88.0, bid=2.00, ask=2.20), _put(85.0, bid=0.60, ask=1.00)],
        }]
        high_score_chain = [{
            "expiration": _exp(14), "dte": 14, "current_price": 100.0,
            "calls": [],
            "puts": [_put(88.0, bid=2.50, ask=2.70), _put(85.0, bid=0.80, ask=1.00)],
        }]

        def _fetch_side_effect(sym, dte_min, dte_max):
            return low_score_chain if sym == "AAPL" else high_score_chain

        with patch("spread_scanner._fetch_chains", side_effect=_fetch_side_effect):
            result = run_spread_weekly_pipeline(_make_holdings(("AAPL", "TSLA")), CFG)

        pcs = result["pcs"]
        assert len(pcs) == 2
        assert pcs[0]["score"] >= pcs[1]["score"]

    def test_symbol_with_no_qualifying_spread_is_absent(self):
        """Holdings with no qualifying spreads produce no rec in output."""
        empty_chain = [{"expiration": _exp(21), "dte": 21, "current_price": 100.0,
                        "calls": [], "puts": []}]
        with patch("spread_scanner._fetch_chains", return_value=empty_chain):
            result = run_spread_weekly_pipeline(_make_holdings(("AAPL",)), CFG)
        assert result["ccs"] == []
        assert result["pcs"] == []
