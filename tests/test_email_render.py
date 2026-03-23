import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from emailer import _render_html


def _make_rec(symbol="AAPL", mid_y=1.0, dte_y=5, mid_s=0.6, dte_s=10):
    yo = {"symbol": symbol, "name": "Corp", "strike": 200.0,
          "expiration": "2026-04-01", "dte": dte_y, "mid": mid_y,
          "bid": mid_y - 0.05, "ask": mid_y + 0.05,
          "otm_pct": 8.0, "annualized_yield": 40.0, "open_interest": 50, "contracts": 2}
    so = {"symbol": symbol, "name": "Corp", "strike": 220.0,
          "expiration": "2026-04-01", "dte": dte_s, "mid": mid_s,
          "bid": mid_s - 0.05, "ask": mid_s + 0.05,
          "otm_pct": 16.0, "annualized_yield": 20.0, "open_interest": 30, "contracts": 2}
    ypd_y = round(mid_y * 100 / dte_y, 2)
    ypd_s = round(mid_s * 100 / dte_s, 2)
    combined_ypd = round(ypd_y * 1 + ypd_s * 1, 2)
    return {
        "symbol": symbol, "name": "Corp", "rank": 1,
        "contracts_total": 2,
        "combined_premium_total": (mid_y + mid_s) * 100,
        "combined_ann_yield": 30.0,
        "combined_ypd": combined_ypd,
        "earnings_flag": None, "earnings_warning": None,
        "yield_leg":  {"option": yo, "contracts": 1, "rationale": "r", "ypd": ypd_y},
        "safety_leg": {"option": so, "contracts": 1, "rationale": "r", "ypd": ypd_s},
    }


META = {
    "run_date": "2026-03-22",
    "duration_sec": 12.3,
    "recipient_email": "test@test.com",
    "pur_pct": 50.0, "pur_open": 7, "pur_max": 14,
    "portfolio_ypd": 26.0,
}


def test_template_renders_pur_in_summary_bar():
    html = _render_html([_make_rec()], META)
    assert "50.0%" in html, "PUR % not found in rendered HTML"
    assert "7/14" in html, "PUR open/max not found"


def test_template_renders_ypd_in_summary_bar():
    html = _render_html([_make_rec()], META)
    assert "26.00" in html or "26.0" in html, "Portfolio YPD not found in HTML"


def test_template_renders_ypd_column_in_table():
    html = _render_html([_make_rec()], META)
    assert "YPD" in html, "YPD column header not in table"
    assert "/day" in html, "YPD /day unit not in table"


def test_template_no_crash_without_safety_leg():
    rec = _make_rec()
    rec["safety_leg"] = None
    html = _render_html([rec], META)
    assert rec["symbol"] in html
