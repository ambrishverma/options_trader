import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from diversifier import diversify_holding


def _make_opt(mid, dte, otm_pct=10.0, ann_yield=25.0, strike=100.0):
    return {
        "symbol": "TEST", "name": "Test Corp", "strike": strike,
        "expiration": "2026-04-01", "dte": dte, "mid": mid,
        "bid": mid - 0.05, "ask": mid + 0.05,
        "otm_pct": otm_pct, "annualized_yield": ann_yield,
        "open_interest": 100, "contracts": 2,
    }


def test_ypd_yield_leg_single_contract():
    """YPD = mid * 100 / DTE for yield leg."""
    opt = _make_opt(mid=1.00, dte=5)
    rec = diversify_holding("TEST", "Test Corp", 1, opt, None)
    assert rec["yield_leg"]["ypd"] == 20.0   # 1.00 * 100 / 5 = 20


def test_ypd_safety_leg():
    """Safety leg gets its own YPD."""
    yo = _make_opt(mid=1.00, dte=5,  otm_pct=5.0)
    so = _make_opt(mid=0.60, dte=10, otm_pct=12.0, strike=110.0)
    rec = diversify_holding("TEST", "Test Corp", 2, yo, so)
    assert rec["safety_leg"]["ypd"] == 6.0   # 0.60 * 100 / 10 = 6


def test_combined_ypd_two_contracts():
    """combined_ypd = sum of leg_ypd * leg_contracts."""
    yo = _make_opt(mid=1.00, dte=5)   # 1 contract → 20/day
    so = _make_opt(mid=0.60, dte=10, otm_pct=12.0, strike=110.0)  # 1 contract → 6/day
    rec = diversify_holding("TEST", "Test Corp", 2, yo, so)
    # 1*20 + 1*6 = 26
    assert rec["combined_ypd"] == 26.0


def test_ypd_rounds_to_two_decimals():
    opt = _make_opt(mid=0.33, dte=7)
    rec = diversify_holding("TEST", "Test Corp", 1, opt, None)
    # 0.33 * 100 / 7 = 4.714... → 4.71
    assert rec["yield_leg"]["ypd"] == round(0.33 * 100 / 7, 2)
