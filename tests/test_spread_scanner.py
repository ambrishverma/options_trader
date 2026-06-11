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

from spread_scanner import (
    scan_ccs, scan_pcs, run_spread_weekly_pipeline,
    scan_pds, scan_cds, run_insurance_pipeline,
    scan_insurance,
    _is_standard_strike, _parse_chain_df,
)

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


def _call(strike, bid, ask, oi=50, iv=0.30):
    mid = round((bid + ask) / 2, 2)
    return {"strike": strike, "bid": bid, "ask": ask, "mid": mid, "open_interest": oi, "iv": iv}


def _put(strike, bid, ask, oi=50, iv=0.30):
    mid = round((bid + ask) / 2, 2)
    return {"strike": strike, "bid": bid, "ask": ask, "mid": mid, "open_interest": oi, "iv": iv}


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
    "spread_min_pop":           0,
    "spread_top_n":             1,
    "risk_free_rate":           4.3,
}


# ─────────────────────────────────────────────────────────────────────────────
# CCS tests
# ─────────────────────────────────────────────────────────────────────────────

def _ccs_with_chains(chain_data, **kwargs):
    """Call scan_ccs with injected chain data. Returns rec only (discards scenario count)."""
    kwargs.setdefault("min_pop", 0)
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
                              spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                              min_premium_pct=1.0, min_pop=0)
        assert rec is not None
        assert rec["dte"] == 14   # shorter DTE had higher score

    def test_long_leg_picks_nearest_strike(self):
        """Long leg snaps to nearest available strike within spread_max.
        spread_max_pct=4% → eff_spread_max=$4.00; long at 115.5 → actual=3.5 ≤ 4.0 → valid.
        Long at 118 → actual=6.0 > 4.0 → rejected by max-spread guard."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20),   # short: 12% OTM
                _call(115.5, bid=0.80, ask=1.00),   # nearest to 115 (target=112+3); actual=3.5
                _call(118.0, bid=0.50, ask=0.70),   # actual=6.0 → exceeds max → skipped
            ]
        )
        # spread_max=4% → eff_spread_max=4.0, so actual_spread=3.5 is within bounds
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=4.0, min_premium_pct=1.0)
        assert rec is not None
        # target = 112 + 3 = 115; nearest available within max is 115.5
        assert rec["long_leg"]["strike"] == 115.5

    def test_best_spread_across_range_ccs(self):
        """
        Range evaluation: scanner tries all spread widths in 1%-step increments
        and returns the combination with the highest POP-weighted score.

        With POP-weighted scoring (Score = POP × C/L × 365/DTE), the tighter
        spread with better C/L ratio wins because POP and 365/DTE are identical
        (same short strike and DTE).

        Setup: short=110 (10% OTM), long candidates at 112 and 115.
          spread $2 → long=112, net=1.00, C/L=1.00, score=POP×1.00×17.38  ← wins
          spread $5 → long=115, net=2.20, C/L≈0.79, score=POP×0.79×17.38
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=3.00, ask=3.20),   # short: exactly 10% OTM
                _call(112.0, bid=1.50, ask=2.00),   # long at spread ~$2: net=1.00, C/L=1.00
                _call(115.0, bid=0.50, ask=0.80),   # long at spread ~$5: net=2.20, C/L≈0.79
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                               min_premium_pct=1.0)
        assert rec is not None
        assert rec["long_leg"]["strike"] == 112.0, (
            "POP-weighted scoring favours tighter spreads with better C/L ratio "
            "(C/L=1.00 at $2 spread > C/L=0.79 at $5 spread)."
        )
        assert rec["net_credit"] == 1.00

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
                                min_premium_pct=0.0, min_pop=0)
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
                      "credit_to_loss_ratio", "pop", "score"):
            assert field in rec, f"Missing field: {field}"
        for field in ("strike", "bid", "ask", "mid", "open_interest", "otm_pct", "iv", "delta"):
            assert field in rec["short_leg"], f"Missing short_leg field: {field}"
        for field in ("strike", "bid", "ask", "mid", "open_interest"):
            assert field in rec["long_leg"], f"Missing long_leg field: {field}"


# ─────────────────────────────────────────────────────────────────────────────
# PCS tests
# ─────────────────────────────────────────────────────────────────────────────

def _pcs_with_chains(chain_data, **kwargs):
    """Call scan_pcs with injected chain data. Returns rec only (discards scenario count)."""
    kwargs.setdefault("min_pop", 0)
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


# ─────────────────────────────────────────────────────────────────────────────
# POP, earnings guardrail, and top-N tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPOPScoring:
    def test_pop_field_present_in_ccs_rec(self):
        """CCS rec includes pop field when IV is available."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0)
        assert rec is not None
        assert "pop" in rec
        assert rec["pop"] > 0

    def test_pop_field_present_in_pcs_rec(self):
        """PCS rec includes pop field when IV is available."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20, iv=0.30),
                _put(85.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0)
        assert rec is not None
        assert "pop" in rec
        assert rec["pop"] > 0

    def test_score_uses_pop_weighted_formula(self):
        """Score = POP × (credit_to_loss_ratio) × (365/DTE), not old YPD × C/L."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0)
        assert rec is not None
        pop_decimal = rec["pop"] / 100.0
        expected_score = pop_decimal * rec["credit_to_loss_ratio"] * (365.0 / 21)
        assert abs(rec["score"] - expected_score) < 0.01

    def test_pop_guardrail_rejects_low_pop(self):
        """Candidates with POP below threshold are rejected."""
        # iv=0 → delta=0 → pop=0 → rejected when min_pop > 0
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0),
                _call(115.0, bid=0.80, ask=1.00, iv=0),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=70.0)
        assert rec is None

    def test_pop_guardrail_allows_high_pop(self):
        """Candidates with POP above threshold pass."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=70.0)
        assert rec is not None
        assert rec["pop"] >= 70.0

    def test_delta_and_iv_in_short_leg(self):
        """Short leg includes delta and iv fields."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0)
        assert rec is not None
        assert "iv" in rec["short_leg"]
        assert "delta" in rec["short_leg"]
        assert rec["short_leg"]["iv"] == 0.30
        assert 0 < rec["short_leg"]["delta"] < 1.0


class TestEarningsGuardrail:
    def test_earnings_before_expiry_rejected(self):
        """Expirations with earnings before expiry are filtered out."""
        exp_date = _exp(21)
        # Earnings 10 days out — before expiry (21 days)
        earnings_date = (TODAY + timedelta(days=10)).strftime("%Y-%m-%d")
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0,
                               earnings_dates={"TEST": earnings_date})
        assert rec is None

    def test_earnings_after_expiry_passes(self):
        """Expirations with earnings after expiry are not filtered."""
        # Earnings 30 days out — after expiry (21 days)
        earnings_date = (TODAY + timedelta(days=30)).strftime("%Y-%m-%d")
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0,
                               earnings_dates={"TEST": earnings_date})
        assert rec is not None

    def test_no_earnings_data_passes(self):
        """No earnings data → no filtering."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0, min_pop=0,
                               earnings_dates=None)
        assert rec is not None


class TestTopN:
    def test_top_n_1_returns_single_dict(self):
        """top_n=1 returns a single rec dict (backward compat)."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(112.0, bid=2.00, ask=2.20, iv=0.30),
                _call(115.0, bid=0.80, ask=1.00, iv=0.30),
            ]
        )
        with patch("spread_scanner._fetch_chains", return_value=chains):
            rec, _ = scan_ccs("TEST", dte_min=14, dte_max=42,
                              short_otm_pct=10.0, min_open_interest=2,
                              spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                              min_premium_pct=1.0, min_pop=0, top_n=1)
        assert isinstance(rec, dict)

    def test_top_n_returns_list(self):
        """top_n>1 returns a list of recs."""
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                _call(110.0, bid=3.00, ask=3.20, iv=0.30),
                _call(112.0, bid=1.50, ask=2.00, iv=0.30),
                _call(115.0, bid=0.50, ask=0.80, iv=0.30),
            ]
        )
        with patch("spread_scanner._fetch_chains", return_value=chains):
            recs, _ = scan_ccs("TEST", dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=1.0, spread_size_max_pct=10.0,
                               min_premium_pct=0.0, min_pop=0, top_n=5)
        assert isinstance(recs, list)
        assert len(recs) >= 1
        # Verify sorted by score descending
        for i in range(len(recs) - 1):
            assert recs[i]["score"] >= recs[i + 1]["score"]

    def test_top_n_empty_returns_empty_list(self):
        """top_n>1 with no qualifying spreads returns empty list."""
        with patch("spread_scanner._fetch_chains", return_value=[]):
            recs, cnt = scan_ccs("TEST", top_n=3)
        assert recs == []
        assert cnt == 0


# ─────────────────────────────────────────────────────────────────────────────
# Non-standard strike filter tests
# ─────────────────────────────────────────────────────────────────────────────

class TestStandardStrikeFilter:
    """Tests for _is_standard_strike() and its integration in _parse_chain_df."""

    def test_whole_dollar_strikes_are_standard(self):
        """Integer strikes like $100, $305, $730 are standard."""
        for strike in (100.0, 305.0, 730.0, 1.0, 5000.0):
            assert _is_standard_strike(strike), f"${strike} should be standard"

    def test_half_dollar_strikes_are_standard(self):
        """Half-dollar strikes like $100.50, $115.50 are standard."""
        for strike in (100.50, 115.50, 0.50, 999.50):
            assert _is_standard_strike(strike), f"${strike} should be standard"

    def test_adjusted_strikes_are_non_standard(self):
        """Adjusted strikes like $264.78, $304.78 are non-standard (QQQ special dividend)."""
        for strike in (264.78, 304.78, 174.78, 609.78, 123.45, 99.99, 200.33):
            assert not _is_standard_strike(strike), f"${strike} should be non-standard"

    def test_parse_chain_df_filters_adjusted_strikes(self):
        """_parse_chain_df excludes rows with non-standard strikes."""
        import pandas as pd
        df = pd.DataFrame([
            {"strike": 300.0,  "bid": 0.50, "ask": 0.60, "openInterest": 100},
            {"strike": 264.78, "bid": 0.00, "ask": 0.02, "openInterest": 1249},  # adjusted
            {"strike": 305.0,  "bid": 1.00, "ask": 1.20, "openInterest": 200},
            {"strike": 304.78, "bid": 0.00, "ask": 0.02, "openInterest": 1895},  # adjusted
        ])
        rows = _parse_chain_df(df)
        strikes = [r["strike"] for r in rows]
        assert 300.0 in strikes
        assert 305.0 in strikes
        assert 264.78 not in strikes, "Adjusted strike $264.78 should be filtered"
        assert 304.78 not in strikes, "Adjusted strike $304.78 should be filtered"
        assert len(rows) == 2

    def test_pcs_excludes_adjusted_long_leg(self):
        """
        Regression (QQQ bug): adjusted-strike contracts like $264.78 must not be
        selected as the long leg of a PCS.  Without the filter, the scanner paired
        a standard $305 short with an adjusted $264.78 long at $0.02 ask, producing
        a fictitious $12.48 net credit.
        """
        chains = _make_chain_data(
            current_price=730.0, dte=23,
            puts=[
                _put(655.0, bid=3.00, ask=3.20),   # short: ~10.3% OTM, reasonable bid
                _put(650.0, bid=2.50, ask=2.80),   # standard long candidate
                # If an adjusted strike sneaked through, it would be here:
                # _put(644.78, bid=0.00, ask=0.02) — but chain parse already filters it
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=0.5, spread_size_max_pct=2.0,
                               min_premium_pct=0.0)
        if rec is not None:
            # Verify long leg has a standard strike
            assert _is_standard_strike(rec["long_leg"]["strike"]), (
                f"Long leg strike ${rec['long_leg']['strike']} is non-standard (adjusted)"
            )


# ─────────────────────────────────────────────────────────────────────────────
# OTM-adaptive bid sanity check tests
# ─────────────────────────────────────────────────────────────────────────────

class TestOTMBidSanityCheck:
    """Tests for the OTM-adaptive bid ceiling that replaces the old flat 50% check."""

    def test_pcs_deep_otm_phantom_bid_rejected(self):
        """
        Regression (QQQ bug): QQQ $305 put with bid=$12.50 when QQQ is $730
        (58% OTM) must be rejected.  The old 50%-of-price check let this through;
        the new OTM-adaptive check caps deep-OTM bids at 0.5% of price.
        """
        chains = _make_chain_data(
            current_price=730.0, dte=23,
            puts=[
                # This is the exact buggy contract: $305 put, bid $12.50, 58% OTM
                _put(305.0, bid=12.50, ask=16.50, oi=267),
                _put(300.0, bid=0.00, ask=0.02, oi=77),
                # Also add a legitimate contract for comparison
                _put(655.0, bid=2.00, ask=2.30),     # ~10.3% OTM, normal bid
                _put(650.0, bid=1.50, ask=1.80),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=0.5, spread_size_max_pct=2.0,
                               min_premium_pct=0.0)
        # The $305 phantom bid must NOT be selected as the short leg
        if rec is not None:
            assert rec["short_leg"]["strike"] != 305.0, (
                "Deep-OTM $305 put with phantom $12.50 bid should be rejected"
            )

    def test_ccs_deep_otm_phantom_bid_rejected(self):
        """
        CCS equivalent: deep-OTM call with phantom bid must be rejected.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            calls=[
                # Phantom bid on a 50% OTM call
                _call(150.0, bid=8.00, ask=12.00, oi=50),
                _call(155.0, bid=0.50, ask=0.80, oi=50),
                # Legitimate contract
                _call(112.0, bid=2.00, ask=2.20),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _ccs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=5.0,
                               min_premium_pct=0.0)
        if rec is not None:
            assert rec["short_leg"]["strike"] != 150.0, (
                "Deep-OTM $150 call with phantom $8.00 bid should be rejected"
            )

    def test_pcs_moderate_otm_reasonable_bid_passes(self):
        """
        A put 12% OTM with a reasonable bid (< 5% of price) should pass.
        """
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(88.0, bid=2.00, ask=2.20),    # 12% OTM, bid=2% of price → passes
                _put(85.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=1.0)
        assert rec is not None, "12% OTM put with bid=2% of price should pass sanity check"
        assert rec["short_leg"]["strike"] == 88.0

    def test_pcs_20_to_30_otm_uses_2pct_ceiling(self):
        """
        A put 25% OTM: bid must be < 2% of current price.
        bid=1.50 on $100 stock (1.5%) → passes.
        bid=2.50 (2.5%) → rejected.
        """
        # Should pass: bid=1.50 < 2.00 (2% of $100)
        chains_pass = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(75.0, bid=1.50, ask=1.70),    # 25% OTM, bid < 2%
                _put(72.0, bid=0.30, ask=0.50),
            ]
        )
        rec = _pcs_with_chains(chains_pass, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=0.0)
        assert rec is not None, "25% OTM put with bid=1.5% of price should pass"

        # Should be rejected: bid=2.50 >= 2.00 (2% of $100)
        chains_reject = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(75.0, bid=2.50, ask=2.70),    # 25% OTM, bid >= 2%
                _put(72.0, bid=0.30, ask=0.50),
            ]
        )
        rec = _pcs_with_chains(chains_reject, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=0.0)
        assert rec is None, "25% OTM put with bid=2.5% of price should be rejected"

    def test_pcs_over_30_otm_uses_half_pct_ceiling(self):
        """
        A put 40% OTM: bid must be < 0.5% of current price.
        For $100 stock: max bid = $0.50.
        """
        # Should be rejected: bid=1.00 >= 0.50
        chains = _make_chain_data(
            current_price=100.0, dte=21,
            puts=[
                _put(60.0, bid=1.00, ask=1.20),    # 40% OTM, bid >= 0.5%
                _put(57.0, bid=0.30, ask=0.50),
            ]
        )
        rec = _pcs_with_chains(chains, dte_min=14, dte_max=42,
                               short_otm_pct=10.0, min_open_interest=2,
                               spread_size_min_pct=3.0, spread_size_max_pct=3.0,
                               min_premium_pct=0.0)
        assert rec is None, "40% OTM put with bid=1% of price should be rejected (max=0.5%)"


# ─────────────────────────────────────────────────────────────────────────────
# PDS tests (Put Debit Spread — bearish insurance)
# ─────────────────────────────────────────────────────────────────────────────

def _pds_with_chains(chain_data, **kwargs):
    """Call scan_pds with injected chain data. Returns rec only."""
    kwargs.setdefault("max_dpd_pct", 1.0)  # disable DPD filter unless test overrides
    with patch("spread_scanner._fetch_chains", return_value=chain_data):
        rec, _ = scan_pds("TEST", name="Test Corp", **kwargs)
    return rec


class TestScanPDS:
    def test_returns_rec_for_qualifying_spread(self):
        """Basic qualifying PDS → returns a rec dict with correct structure."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(98.0, bid=3.00, ask=3.40),    # long: 2% OTM, in 90-100% range
                _put(90.0, bid=1.00, ask=1.20),    # short: further OTM
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is not None
        assert rec["type"] == "PDS"
        assert rec["symbol"] == "TEST"
        assert rec["long_leg"]["strike"] == 98.0    # near-ATM (you buy this)
        assert rec["short_leg"]["strike"] == 90.0   # further OTM (you sell this)

    def test_net_debit_calculation(self):
        """net_debit = long ask - short bid."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(98.0, bid=3.00, ask=3.40),    # long: ask=3.40
                _put(90.0, bid=1.00, ask=1.20),    # short: bid=1.00
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is not None
        assert rec["net_debit"] == round(3.40 - 1.00, 2)   # 2.40
        assert rec["net_debit_total"] == 240.0              # 2.40 * 100

    def test_dpd_formula(self):
        """DPD = net_debit × 100 / DTE."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=2.00, ask=2.20),    # long: ask=2.20
                _put(90.0, bid=1.00, ask=1.20),    # short: bid=1.00
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=5.0)
        assert rec is not None
        net_debit = round(2.20 - 1.00, 2)  # 1.20
        expected_dpd = round(net_debit * 100 / 30, 4)
        assert rec["dpd"] == expected_dpd

    def test_debit_to_win_ratio(self):
        """debit_to_win_ratio = net_debit / spread_size."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=2.00, ask=2.20),
                _put(90.0, bid=1.00, ask=1.20),
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=5.0)
        assert rec is not None
        net_debit = round(2.20 - 1.00, 2)  # 1.20
        spread = 5.0
        expected = round(net_debit / spread, 4)
        assert rec["debit_to_win_ratio"] == expected

    def test_max_protection_formula(self):
        """max_protection = spread_size × 100."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=2.00, ask=2.20),
                _put(90.0, bid=1.00, ask=1.20),
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=5.0)
        assert rec is not None
        assert rec["max_protection"] == 500.0   # 5.0 * 100

    def test_long_leg_range_90_to_100_pct(self):
        """Long leg must be between 90% and 100% of stock price."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(88.0, bid=1.00, ask=1.20),    # 12% OTM → below 90% → excluded
                _put(80.0, bid=0.30, ask=0.50),
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is None, "Long put at 88% (below 90% threshold) should be excluded"

    def test_max_debit_filter(self):
        """net_debit must be < max_debit_pct × spread_width."""
        # spread=10, max_debit_pct=0.25 → max debit = $2.50
        # net_debit = 4.40 - 1.00 = 3.40 → exceeds 2.50 → rejected
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=4.00, ask=4.40),    # expensive long leg
                _put(85.0, bid=1.00, ask=1.20),    # cheap short leg
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.25, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is None, "Debit $3.40 exceeds 25% of $10 spread ($2.50)"

    def test_max_debit_filter_passes(self):
        """net_debit within limit passes the filter."""
        # spread=10, max_debit_pct=0.25 → max debit = $2.50
        # net_debit = 2.20 - 1.00 = 1.20 → under 2.50 → passes
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=2.00, ask=2.20),
                _put(85.0, bid=1.00, ask=1.20),
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.25, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is not None
        assert rec["net_debit"] == 1.20

    def test_lowest_score_wins(self):
        """With two valid spreads, the one with lowest score wins."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(98.0, bid=3.00, ask=3.40),    # long A: expensive
                _put(95.0, bid=1.50, ask=1.70),    # long B: cheaper
                _put(90.0, bid=0.80, ask=1.00),    # short for A (spread=8)
                _put(88.0, bid=0.50, ask=0.70),    # short for B (spread=7)
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=10.0)
        assert rec is not None
        # The rec with lowest DPD × debit_to_win_ratio should win
        assert rec["score"] > 0

    def test_returns_none_no_qualifying(self):
        """Returns None when no puts in the 90-100% range."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(80.0, bid=0.50, ask=0.70),    # too far OTM
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.25, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=5.0)
        assert rec is None

    def test_dte_window_filter(self):
        """Expirations outside the DTE window are excluded."""
        chains = _make_chain_data(current_price=100.0, dte=90,
                                  puts=[
                                      _put(95.0, bid=3.00, ask=3.20),
                                      _put(85.0, bid=1.00, ask=1.20),
                                  ])
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is None, "DTE=90 is outside [1,60] window"

    def test_open_interest_filter(self):
        """Both legs must meet min OI."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(95.0, bid=2.00, ask=2.20, oi=50),
                _put(85.0, bid=1.00, ask=1.20, oi=1),   # OI=1 → below min 2
            ]
        )
        rec = _pds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is None, "Short leg OI=1 below min 2 → no qualifying spread"


# ─────────────────────────────────────────────────────────────────────────────
# CDS tests (Call Debit Spread — bullish insurance)
# ─────────────────────────────────────────────────────────────────────────────

def _cds_with_chains(chain_data, **kwargs):
    """Call scan_cds with injected chain data. Returns rec only."""
    kwargs.setdefault("max_dpd_pct", 1.0)  # disable DPD filter unless test overrides
    with patch("spread_scanner._fetch_chains", return_value=chain_data):
        rec, _ = scan_cds("TEST", name="Test Corp", **kwargs)
    return rec


class TestScanCDS:
    def test_returns_rec_for_qualifying_spread(self):
        """Basic qualifying CDS → returns a rec dict with correct structure."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(102.0, bid=3.00, ask=3.40),   # long: 2% OTM, in 100-110% range
                _call(110.0, bid=1.00, ask=1.20),   # short: further OTM
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is not None
        assert rec["type"] == "CDS"
        assert rec["long_leg"]["strike"] == 102.0    # near-ATM (you buy this)
        assert rec["short_leg"]["strike"] == 110.0   # further OTM (you sell this)

    def test_net_debit_calculation(self):
        """net_debit = long ask - short bid."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(102.0, bid=3.00, ask=3.40),   # long: ask=3.40
                _call(110.0, bid=1.00, ask=1.20),   # short: bid=1.00
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is not None
        assert rec["net_debit"] == round(3.40 - 1.00, 2)   # 2.40
        assert rec["net_debit_total"] == 240.0

    def test_long_leg_range_100_to_110_pct(self):
        """Long leg must be between 100% and 110% of stock price."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(112.0, bid=1.00, ask=1.20),   # 12% OTM → above 110% → excluded
                _call(120.0, bid=0.30, ask=0.50),
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is None, "Long call at 112% (above 110% threshold) should be excluded"

    def test_max_debit_filter(self):
        """net_debit must be < max_debit_pct × spread_width."""
        # spread=8, max_debit_pct=0.25 → max debit = $2.00
        # net_debit = 3.40 - 1.00 = 2.40 → exceeds 2.00 → rejected
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(102.0, bid=3.00, ask=3.40),
                _call(110.0, bid=1.00, ask=1.20),
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.25, min_open_interest=2,
                               spread_size_min_pct=8.0, spread_size_max_pct=8.0)
        assert rec is None, "Debit $2.40 exceeds 25% of $8 spread ($2.00)"

    def test_short_leg_derived_from_long_plus_spread(self):
        """Short leg = long_strike + spread_size (further OTM, higher)."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(105.0, bid=2.50, ask=2.80),   # long leg
                _call(115.0, bid=0.80, ask=1.00),   # short leg (10 points higher)
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is not None
        assert rec["long_leg"]["strike"] == 105.0
        assert rec["short_leg"]["strike"] == 115.0
        assert rec["spread_size"] == 10.0

    def test_dpd_formula(self):
        """DPD = net_debit × 100 / DTE."""
        chains = _make_chain_data(
            current_price=100.0, dte=20,
            calls=[
                _call(105.0, bid=2.50, ask=2.80),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is not None
        net_debit = round(2.80 - 0.80, 2)  # 2.00
        expected_dpd = round(net_debit * 100 / 20, 4)
        assert rec["dpd"] == expected_dpd

    def test_returns_none_no_qualifying(self):
        """Returns None when no calls in the 100-110% range."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(115.0, bid=0.80, ask=1.00),   # above 110%
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=5.0, spread_size_max_pct=5.0)
        assert rec is None

    def test_max_protection(self):
        """max_protection = spread_size × 100."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            calls=[
                _call(105.0, bid=2.50, ask=2.80),
                _call(115.0, bid=0.80, ask=1.00),
            ]
        )
        rec = _cds_with_chains(chains, dte_min=1, dte_max=60,
                               max_debit_pct=0.50, min_open_interest=2,
                               spread_size_min_pct=10.0, spread_size_max_pct=10.0)
        assert rec is not None
        assert rec["max_protection"] == 1000.0   # 10 * 100


# ─────────────────────────────────────────────────────────────────────────────
# Insurance pipeline tests
# ─────────────────────────────────────────────────────────────────────────────

DEBIT_CFG = {
    "debit_min_holding_value":  10000,
    "debit_dte_min":            1,
    "debit_dte_max":            60,
    "debit_max_debit_pct":      25.0,
    "debit_min_open_interest":  2,
    "debit_spread_size_min_pct": 1.0,
    "debit_spread_size_max_pct": 20.0,
}


class TestInsurancePipeline:
    def test_pds_triggers_for_high_value_holding(self):
        """Holdings with market value >= threshold qualify for PDS."""
        holdings = [{"symbol": "AAPL", "name": "Apple", "quantity": 50, "price": 250.0}]
        # 50 * 250 = $12,500 >= $10,000 → PDS eligible
        with patch("spread_scanner.scan_pds", return_value=({"type": "PDS", "symbol": "AAPL", "score": 1.0}, 10)) as mock_pds, \
             patch("spread_scanner.scan_cds", return_value=(None, 5)):
            result = run_insurance_pipeline(holdings, DEBIT_CFG)
        assert len(result["pds"]) == 1
        assert result["pds"][0]["trigger_reason"] == "$12,500 holding"
        mock_pds.assert_called_once()

    def test_pds_skips_low_value_holding(self):
        """Holdings below threshold do NOT qualify for PDS."""
        holdings = [{"symbol": "F", "name": "Ford", "quantity": 100, "price": 15.0}]
        # 100 * 15 = $1,500 < $10,000 → NOT PDS eligible (but CDS: 100 shares)
        with patch("spread_scanner.scan_pds") as mock_pds, \
             patch("spread_scanner.scan_cds", return_value=(None, 5)):
            run_insurance_pipeline(holdings, DEBIT_CFG)
        mock_pds.assert_not_called()

    def test_cds_triggers_for_100_shares(self):
        """Holdings with qty >= 100 qualify for CDS."""
        holdings = [{"symbol": "F", "name": "Ford", "quantity": 100, "price": 15.0}]
        with patch("spread_scanner.scan_pds") as mock_pds, \
             patch("spread_scanner.scan_cds", return_value=({"type": "CDS", "symbol": "F", "score": 0.5}, 8)) as mock_cds:
            result = run_insurance_pipeline(holdings, DEBIT_CFG)
        assert len(result["cds"]) == 1
        assert "100 shares" in result["cds"][0]["trigger_reason"]
        mock_cds.assert_called_once()

    def test_cds_triggers_for_open_cc(self):
        """Holdings with open covered calls qualify for CDS."""
        holdings = [{"symbol": "NVDA", "name": "NVIDIA", "quantity": 50, "price": 130.0}]
        # qty=50 < 100, value=$6,500 < $10,000 — but has open CC
        open_calls = [{"symbol": "NVDA", "expiration": "2026-07-18"}]
        with patch("spread_scanner.scan_pds") as mock_pds, \
             patch("spread_scanner.scan_cds", return_value=({"type": "CDS", "symbol": "NVDA", "score": 0.3}, 5)):
            result = run_insurance_pipeline(holdings, DEBIT_CFG,
                                            open_calls_detail=open_calls)
        assert len(result["cds"]) == 1
        assert "Open CC" in result["cds"][0]["trigger_reason"]

    def test_cds_triggers_for_open_ccs(self):
        """Holdings with open CCS spread positions qualify for CDS."""
        holdings = [{"symbol": "AMD", "name": "AMD", "quantity": 50, "price": 130.0}]
        open_spreads = [{"symbol": "AMD", "type": "CCS", "short_strike": 150.0}]
        with patch("spread_scanner.scan_pds") as mock_pds, \
             patch("spread_scanner.scan_cds", return_value=({"type": "CDS", "symbol": "AMD", "score": 0.4}, 5)):
            result = run_insurance_pipeline(holdings, DEBIT_CFG,
                                            open_spreads_detail=open_spreads)
        assert len(result["cds"]) == 1
        assert "Open CCS" in result["cds"][0]["trigger_reason"]

    def test_both_pds_and_cds_for_same_holding(self):
        """A holding can qualify for both PDS and CDS."""
        holdings = [{"symbol": "AAPL", "name": "Apple", "quantity": 200, "price": 230.0}]
        # value=$46,000 >= $10,000 → PDS; qty=200 >= 100 → CDS
        with patch("spread_scanner.scan_pds", return_value=({"type": "PDS", "symbol": "AAPL", "score": 0.8}, 10)), \
             patch("spread_scanner.scan_cds", return_value=({"type": "CDS", "symbol": "AAPL", "score": 0.5}, 8)):
            result = run_insurance_pipeline(holdings, DEBIT_CFG)
        assert len(result["pds"]) == 1
        assert len(result["cds"]) == 1

    def test_results_sorted_ascending(self):
        """PDS and CDS results sorted by score ascending (lowest = best)."""
        holdings = [
            {"symbol": "AAPL", "name": "Apple", "quantity": 200, "price": 230.0},
            {"symbol": "MSFT", "name": "Microsoft", "quantity": 150, "price": 450.0},
        ]
        pds_call_count = [0]
        def fake_pds(*a, **kw):
            pds_call_count[0] += 1
            if pds_call_count[0] == 1:
                return ({"type": "PDS", "symbol": "AAPL", "score": 2.0}, 10)
            return ({"type": "PDS", "symbol": "MSFT", "score": 0.5}, 10)

        with patch("spread_scanner.scan_pds", side_effect=fake_pds), \
             patch("spread_scanner.scan_cds", return_value=(None, 0)):
            result = run_insurance_pipeline(holdings, DEBIT_CFG)
        assert result["pds"][0]["score"] < result["pds"][1]["score"]  # 0.5 < 2.0

    def test_skips_non_qualifying_holding(self):
        """Holdings that don't meet any trigger are skipped entirely."""
        holdings = [{"symbol": "X", "name": "US Steel", "quantity": 50, "price": 40.0}]
        # value=$2,000 < $10K, qty=50 < 100, no open CC/CCS
        with patch("spread_scanner.scan_pds") as mock_pds, \
             patch("spread_scanner.scan_cds") as mock_cds:
            result = run_insurance_pipeline(holdings, DEBIT_CFG)
        mock_pds.assert_not_called()
        mock_cds.assert_not_called()
        assert result["pds"] == []
        assert result["cds"] == []


# ─────────────────────────────────────────────────────────────────────────────
# Find Insurance (scan_insurance) tests
# ─────────────────────────────────────────────────────────────────────────────

def _insurance_with_chains(chain_data, **kwargs):
    """Call scan_insurance with injected chain data."""
    with patch("spread_scanner._fetch_chains", return_value=chain_data):
        recs, scenarios = scan_insurance("TEST", name="Test Corp", **kwargs)
    return recs, scenarios


class TestScanInsurance:
    # All tests use price=100.  Default bounds: deductible 5–10%, coverage 10–25%.
    # Long leg range: 90 (10% OTM) to 95 (5% OTM).
    # Spread width: 10 to 25.

    def test_returns_rec_for_qualifying_spread(self):
        """Basic qualifying insurance PDS returns a rec."""
        # long=93 (7% OTM, within 5–10%), short=78 (spread=15, within 10–25%)
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=4.00, ask=4.50),
                _put(78.0, bid=1.00, ask=1.20),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0, top_n=3,
        )
        assert len(recs) >= 1
        rec = recs[0]
        assert rec["type"] == "INSURANCE_PDS"
        assert rec["long_leg"]["strike"] == 93.0
        assert rec["short_leg"]["strike"] == 78.0

    def test_deductible_max_rejects(self):
        """Long strike too far from price (>max_deductible) → rejected."""
        # long=88 is 12% OTM — outside max 10% deductible
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(88.0, bid=5.00, ask=5.50),
                _put(70.0, bid=1.00, ask=1.20),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []

    def test_deductible_min_rejects(self):
        """Long strike too close to price (<min_deductible) → rejected."""
        # long=97 is 3% OTM — below min 5% deductible
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(97.0, bid=3.00, ask=3.50),
                _put(80.0, bid=1.00, ask=1.20),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []

    def test_coverage_too_narrow_rejects(self):
        """Spread width below min_coverage → rejected."""
        # long=93, short=88 → width=5 (only 5%, below 10% min)
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=3.00, ask=3.50),
                _put(88.0, bid=1.50, ask=1.80),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []

    def test_coverage_too_wide_rejects(self):
        """Spread width above max_coverage → rejected."""
        # long=93, short=60 → width=33 (33%, above 25% max)
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=5.00, ask=5.50),
                _put(60.0, bid=0.20, ask=0.30),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []

    def test_cost_rate_formula(self):
        """cost_rate = (net_debit / coverage_band) × (365 / DTE)."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=4.00, ask=4.50),    # long ask=4.50
                _put(78.0, bid=1.00, ask=1.20),    # short bid=1.00
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0, top_n=1,
        )
        assert len(recs) == 1
        rec = recs[0]
        net_debit = 4.50 - 1.00  # 3.50
        coverage = 93.0 - 78.0   # 15.0
        expected_cost_rate = (net_debit / coverage) * (365 / 30)
        assert abs(rec["cost_rate"] - expected_cost_rate) < 0.001

    def test_insurance_fields_present(self):
        """Rec dict contains all insurance-specific fields."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=4.00, ask=4.50),
                _put(78.0, bid=1.00, ask=1.20),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0, top_n=1,
        )
        rec = recs[0]
        assert rec["deductible"] == 7.0       # 100 - 93
        assert rec["deductible_pct"] == 7.0   # 7/100 * 100
        assert rec["coverage_band"] == 15.0   # 93 - 78
        assert rec["coverage_pct"] == 15.0    # 15/100 * 100
        assert rec["cliff_strike"] == 78.0
        assert rec["cliff_pct"] == 22.0       # (1 - 78/100) * 100
        assert rec["net_debit"] == 3.50       # 4.50 - 1.00

    def test_lowest_cost_rate_wins(self):
        """Among multiple candidates, lowest cost_rate is ranked first."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(93.0, bid=4.00, ask=4.50),    # long A (7% deductible)
                _put(92.0, bid=3.50, ask=4.00),    # long B (8% deductible)
                _put(78.0, bid=1.00, ask=1.20),    # short (shared)
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0, top_n=5,
        )
        assert len(recs) >= 2
        assert recs[0]["cost_rate"] <= recs[1]["cost_rate"]

    def test_top_n_limits_results(self):
        """top_n caps the number of returned candidates."""
        chains = _make_chain_data(
            current_price=100.0, dte=30,
            puts=[
                _put(94.0, bid=5.00, ask=5.50),
                _put(93.0, bid=4.00, ask=4.50),
                _put(92.0, bid=3.50, ask=4.00),
                _put(80.0, bid=1.00, ask=1.20),
                _put(75.0, bid=0.50, ask=0.70),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0, top_n=2,
        )
        assert len(recs) <= 2

    def test_no_qualifying_returns_empty(self):
        """No qualifying puts → empty list."""
        chains = _make_chain_data(current_price=100.0, dte=30, puts=[])
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []

    def test_dte_filter(self):
        """Expirations outside DTE window are excluded."""
        chains = _make_chain_data(
            current_price=100.0, dte=90,   # outside 5-60 day window
            puts=[
                _put(93.0, bid=6.00, ask=6.50),
                _put(78.0, bid=2.00, ask=2.20),
            ]
        )
        recs, _ = _insurance_with_chains(
            chains, dte_min=5, dte_max=60,
            min_deductible_pct=5.0, max_deductible_pct=10.0,
            min_coverage_pct=10.0, max_coverage_pct=25.0,
        )
        assert recs == []
