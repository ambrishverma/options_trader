# tests/test_collar_emailer.py
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collar_emailer import _render_collar_html

def _make_collar_rec(
    symbol="AAPL", exp="2026-05-16", dte=50,
    cc_strike=115.0, cc_mid=1.50,
    put_strike=93.0, put_mid=1.20,
    current_price=100.0, contracts=3,
    low_gain=False, earnings_date=None, earnings_warning=None,
):
    net = round(cc_mid - put_mid, 2)
    return {
        "symbol": symbol,
        "name": "Test Corp",
        "current_price": current_price,
        "market_value": round(current_price * contracts * 100, 2),
        "contracts": contracts,
        "expiration": exp,
        "dte": dte,
        "cc_expiration": exp,
        "cc_dte": dte,
        "lp_expiration": exp,
        "lp_dte": dte,
        "call_leg": {
            "strike": cc_strike,
            "bid": cc_mid - 0.05,
            "ask": cc_mid + 0.05,
            "mid": cc_mid,
            "open_interest": 20,
            "otm_pct": round((cc_strike / current_price - 1) * 100, 2),
            "annualized_yield": round(cc_mid / current_price * 365 / dte * 100, 2),
        },
        "put_leg": {
            "strike": put_strike,
            "bid": put_mid - 0.05,
            "ask": put_mid + 0.05,
            "mid": put_mid,
            "open_interest": 15,
            "protection_pct": round((put_strike / current_price - 1) * 100, 2),
        },
        "net_gain_per_share": net,
        "net_gain_total": round(net * 100 * contracts, 2),
        "upside_cap_pct": round((cc_strike / current_price - 1) * 100, 2),
        "downside_floor_pct": round((put_strike / current_price - 1) * 100, 2),
        "low_gain": low_gain,
        "earnings_date": earnings_date,
        "earnings_warning": earnings_warning,
    }

META = {
    "run_date": "2026-03-29",
    "recipient_email": "test@test.com",
    "duration_sec": 8.3,
    "eligible_holdings": 4,
    "total_recommendations": 2,
    "symbols_with_collars": 2,
    "low_gain_count": 0,
    "earnings_flags": 0,
}


def test_template_renders_without_error():
    recs = [_make_collar_rec()]
    html = _render_collar_html(recs, META)
    assert "AAPL" in html
    assert "Collar" in html

def test_template_shows_summary_bar_fields():
    html = _render_collar_html([_make_collar_rec()], META)
    assert "Eligible Holdings" in html
    assert "Collar Opportunities" in html

def test_template_shows_both_legs():
    html = _render_collar_html([_make_collar_rec()], META)
    assert "Covered Call" in html or "CC" in html
    assert "Long Put" in html or "LP" in html

def test_template_shows_net_gain():
    html = _render_collar_html([_make_collar_rec(cc_mid=1.50, put_mid=1.20)], META)
    assert "0.30" in html   # net gain per share

def test_low_gain_rec_has_light_red_indicator():
    rec = _make_collar_rec(low_gain=True)
    html = _render_collar_html([rec], META)
    assert "below" in html.lower() or "threshold" in html.lower() or "fff1f2" in html

def test_earnings_warning_shown():
    rec = _make_collar_rec(
        earnings_date="2026-05-14",
        earnings_warning="⚠ Earnings: 2026-05-14 — falls within expiration month",
    )
    html = _render_collar_html([rec], META)
    assert "2026-05-14" in html

def test_no_crash_with_empty_recs():
    html = _render_collar_html([], META)
    assert "Collar" in html   # header still renders
