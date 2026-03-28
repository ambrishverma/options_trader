import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collar import _filter_collar_pairs, _deduplicate_by_month, _apply_fallback, get_collar_eligible_holdings

# ── Helpers ─────────────────────────────────────────────────────────────────

def _make_pair(
    cc_strike=115.0, cc_mid=1.50, cc_oi=10,
    put_strike=92.0, put_mid=1.20, put_oi=10,
    expiration="2026-05-16", current_price=100.0, dte=50,
):
    return {
        "symbol": "TEST",
        "name": "Test Corp",
        "current_price": current_price,
        "market_value": current_price * 200,
        "contracts": 2,
        "expiration": expiration,
        "dte": dte,
        "call_leg": {
            "strike": cc_strike,
            "mid": cc_mid,
            "bid": cc_mid - 0.05,
            "ask": cc_mid + 0.05,
            "open_interest": cc_oi,
            "otm_pct": round((cc_strike / current_price - 1) * 100, 2),
            "annualized_yield": round(cc_mid / current_price * 365 / dte * 100, 2),
        },
        "put_leg": {
            "strike": put_strike,
            "mid": put_mid,
            "bid": put_mid - 0.05,
            "ask": put_mid + 0.05,
            "open_interest": put_oi,
            "protection_pct": round((put_strike / current_price - 1) * 100, 2),
        },
        "net_gain_per_share": round(cc_mid - put_mid, 2),
        "net_gain_total": round((cc_mid - put_mid) * 100 * 2, 2),
        "upside_cap_pct": round((cc_strike / current_price - 1) * 100, 2),
        "downside_floor_pct": round((put_strike / current_price - 1) * 100, 2),
        "low_gain": False,
    }

CFG = {
    "collar_call_otm_min_pct": 10.0,
    "collar_put_otm_max_pct": 10.0,
    "collar_min_open_interest": 6,
    "collar_min_net_gain_per_share": 0.10,
    "collar_dte_min": 28,
    "collar_dte_max": 112,
}


# ── _filter_collar_pairs ─────────────────────────────────────────────────────

def test_qualifying_pair_passes():
    pair = _make_pair()
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 1

def test_call_not_otm_enough_rejected():
    pair = _make_pair(cc_strike=105.0)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_put_too_far_otm_rejected():
    pair = _make_pair(put_strike=85.0)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_low_oi_rejected():
    pair = _make_pair(cc_oi=3, put_oi=3)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_oi_exactly_6_passes():
    pair = _make_pair(cc_oi=6, put_oi=6)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 1

def test_net_gain_below_floor_rejected():
    pair = _make_pair(cc_mid=1.20, put_mid=1.15)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_net_gain_exactly_floor_passes():
    pair = _make_pair(cc_mid=1.20, put_mid=1.10)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 1

def test_not_self_financing_rejected():
    pair = _make_pair(cc_mid=1.00, put_mid=1.50)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_dte_too_short_rejected():
    pair = _make_pair(dte=20)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_dte_too_long_rejected():
    pair = _make_pair(dte=120)
    result = _filter_collar_pairs([pair], CFG)
    assert len(result) == 0

def test_dte_at_boundaries_passes():
    for dte in (28, 112):
        pair = _make_pair(dte=dte, expiration="2026-06-01")
        result = _filter_collar_pairs([pair], CFG)
        assert len(result) == 1, f"DTE={dte} should pass"


# ── _deduplicate_by_month ────────────────────────────────────────────────────

def test_dedup_keeps_highest_net_gain_per_month():
    low  = _make_pair(cc_mid=1.20, put_mid=1.10, expiration="2026-05-16")
    high = _make_pair(cc_mid=1.50, put_mid=1.20, expiration="2026-05-21")
    result = _deduplicate_by_month([low, high])
    assert len(result) == 1
    assert result[0]["net_gain_per_share"] == 0.30

def test_dedup_keeps_one_per_month_across_months():
    may  = _make_pair(expiration="2026-05-16")
    june = _make_pair(expiration="2026-06-20")
    result = _deduplicate_by_month([may, june])
    assert len(result) == 2

def test_dedup_orders_by_expiration_month():
    june = _make_pair(expiration="2026-06-20")
    may  = _make_pair(expiration="2026-05-16")
    result = _deduplicate_by_month([june, may])
    assert result[0]["expiration"][:7] == "2026-05"
    assert result[1]["expiration"][:7] == "2026-06"


# ── _apply_fallback ──────────────────────────────────────────────────────────

def test_fallback_returns_best_pair_below_floor():
    weak = _make_pair(cc_mid=1.10, put_mid=1.04)
    best = _make_pair(cc_mid=1.10, put_mid=1.02)
    result = _apply_fallback("TEST", [weak, best], CFG)
    assert result is not None
    assert result["net_gain_per_share"] == 0.08
    assert result["low_gain"] is True

def test_fallback_returns_none_when_not_self_financing():
    bad = _make_pair(cc_mid=0.80, put_mid=1.20)
    result = _apply_fallback("TEST", [bad], CFG)
    assert result is None

def test_fallback_not_triggered_when_qualifying_pairs_exist():
    pair = _make_pair(cc_mid=1.05, put_mid=1.00)
    result = _apply_fallback("TEST", [pair], CFG)
    assert result is not None


# ── get_collar_eligible_holdings ─────────────────────────────────────────────

def test_eligible_filters_by_market_value():
    holdings = [
        {"symbol": "BIG",   "shares": 600, "price": 100.0, "contracts": 6},
        {"symbol": "SMALL", "shares": 200, "price": 100.0, "contracts": 2},
        {"symbol": "EXACT", "shares": 500, "price": 100.0, "contracts": 5},
    ]
    result = get_collar_eligible_holdings(holdings, min_value=50000.0)
    assert [h["symbol"] for h in result] == ["BIG"]

def test_eligible_requires_at_least_one_contract():
    holdings = [{"symbol": "X", "shares": 80, "price": 100.0, "contracts": 0}]
    result = get_collar_eligible_holdings(holdings, min_value=0.0)
    assert len(result) == 0


def test_eligible_rejects_exact_boundary():
    """Holdings at exactly min_value are excluded (must be strictly greater than)."""
    holdings = [{"symbol": "EXACT", "shares": 500, "price": 100.0, "contracts": 5}]
    result = get_collar_eligible_holdings(holdings, min_value=50000.0)
    assert len(result) == 0
