import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))


def _compute_pur(open_calls: dict, holdings: list) -> dict:
    """Extracted PUR logic — mirrors what scheduler.py will do."""
    max_possible = sum(h["contracts"] for h in holdings)
    open_total   = sum(open_calls.values())
    pur_pct = round(open_total / max_possible * 100, 1) if max_possible > 0 else 0.0
    return {
        "pur_pct":  pur_pct,
        "pur_open": open_total,
        "pur_max":  max_possible,
    }


def test_pur_50_percent():
    holdings = [
        {"symbol": "INTU", "contracts": 10},
        {"symbol": "META", "contracts": 4},
    ]
    open_calls = {"INTU": 7}
    r = _compute_pur(open_calls, holdings)
    assert r["pur_pct"] == 50.0
    assert r["pur_open"] == 7
    assert r["pur_max"] == 14


def test_pur_fully_deployed():
    holdings = [{"symbol": "AAPL", "contracts": 3}]
    open_calls = {"AAPL": 3}
    r = _compute_pur(open_calls, holdings)
    assert r["pur_pct"] == 100.0


def test_pur_none_open():
    holdings = [{"symbol": "TSLA", "contracts": 5}]
    open_calls = {}
    r = _compute_pur(open_calls, holdings)
    assert r["pur_pct"] == 0.0


def test_pur_zero_holdings():
    """Guard against division by zero when no eligible holdings."""
    r = _compute_pur({}, [])
    assert r["pur_pct"] == 0.0
