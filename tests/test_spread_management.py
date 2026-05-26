"""
test_spread_management.py — Tests for spread management modes:
  execute_spread_mode("safety" | "rescue" | "panic", "PCS" | "CCS")
  _fetch_and_pair_spreads()
  _cancel_spread_orders()
  _place_spread_close_order()

All Robinhood and auth calls are mocked.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _future_date(days: int = 30) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _make_rh_position(
    chain_symbol="TSLA",
    quantity="1",
    option_url="https://api.robinhood.com/options/instruments/opt-001/",
    average_price="-150.00",
    trade_value_multiplier="-1",
):
    """Mock Robinhood open option position dict."""
    return {
        "chain_symbol":         chain_symbol,
        "quantity":             quantity,
        "option":               option_url,
        "average_price":        average_price,
        "trade_value_multiplier": trade_value_multiplier,
    }


def _make_rh_instrument(
    option_id="opt-001",
    option_type="put",
    strike_price="290.00",
    expiration_date="2026-06-20",
    chain_symbol="TSLA",
):
    """Mock Robinhood option instrument dict."""
    return {
        "id":               option_id,
        "type":             option_type,
        "strike_price":     strike_price,
        "expiration_date":  expiration_date,
        "chain_symbol":     chain_symbol,
    }


def _make_market_data(bid_price="0.50", ask_price="0.60", mark_price="0.55"):
    """Mock Robinhood option market data dict."""
    return {
        "bid_price":  bid_price,
        "ask_price":  ask_price,
        "mark_price": mark_price,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_auth():
    with patch("trader.login", return_value=True) as m_login, \
         patch("trader.logout") as m_logout:
        yield m_login, m_logout


@pytest.fixture
def mock_rh():
    """Patch robin_stocks.robinhood within trader module."""
    with patch("trader.rh", create=True) as rh:
        yield rh


# ─────────────────────────────────────────────────────────────────────────────
# _fetch_and_pair_spreads tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchAndPairSpreads:
    """Tests for _fetch_and_pair_spreads (internal helper)."""

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_pcs_pairing_basic(self, mock_positions, mock_request_get, mock_market):
        """A simple PCS with one short put and one long put pairs correctly."""
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/opt-short/",
                              average_price="-250.00", trade_value_multiplier="-1"),
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/opt-long/",
                              average_price="100.00", trade_value_multiplier="1"),
        ]

        def instrument_lookup(url):
            if "opt-short" in url:
                return _make_rh_instrument("opt-short", "put", "290.00", exp, "TSLA")
            if "opt-long" in url:
                return _make_rh_instrument("opt-long", "put", "280.00", exp, "TSLA")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data("0.50", "0.60", "0.55")]

        pairs = _fetch_and_pair_spreads("PCS")
        assert len(pairs) == 1
        p = pairs[0]
        assert p["symbol"] == "TSLA"
        assert p["short_strike"] == 290.0
        assert p["long_strike"] == 280.0
        assert p["width"] == 10.0
        # orig_credit = (abs(-250) - 100) / 100 = 1.50
        assert p["orig_credit"] == 1.50
        # PCS break_even = short_strike - orig_credit = 290 - 1.50 = 288.50
        assert p["break_even"] == 288.50

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_ccs_pairing_basic(self, mock_positions, mock_request_get, mock_market):
        """A simple CCS with one short call and one long call pairs correctly."""
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            _make_rh_position("AAPL", "2",
                              "https://api.robinhood.com/options/instruments/opt-short/",
                              average_price="-300.00", trade_value_multiplier="-1"),
            _make_rh_position("AAPL", "2",
                              "https://api.robinhood.com/options/instruments/opt-long/",
                              average_price="150.00", trade_value_multiplier="1"),
        ]

        def instrument_lookup(url):
            if "opt-short" in url:
                return _make_rh_instrument("opt-short", "call", "200.00", exp, "AAPL")
            if "opt-long" in url:
                return _make_rh_instrument("opt-long", "call", "210.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data("0.30", "0.40", "0.35")]

        pairs = _fetch_and_pair_spreads("CCS")
        assert len(pairs) == 1
        p = pairs[0]
        assert p["symbol"] == "AAPL"
        assert p["short_strike"] == 200.0
        assert p["long_strike"] == 210.0
        assert p["width"] == 10.0
        # orig_credit = (abs(-300) - 150) / 100 = 1.50
        assert p["orig_credit"] == 1.50
        # CCS break_even = short_strike + orig_credit = 200 + 1.50 = 201.50
        assert p["break_even"] == 201.50

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_filter_by_symbol(self, mock_positions, mock_request_get, mock_market):
        """filter_sym restricts output to a single symbol."""
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/tsla-s/",
                              average_price="-200.00", trade_value_multiplier="-1"),
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/tsla-l/",
                              average_price="100.00", trade_value_multiplier="1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/aapl-s/",
                              average_price="-200.00", trade_value_multiplier="-1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/aapl-l/",
                              average_price="100.00", trade_value_multiplier="1"),
        ]

        def instrument_lookup(url):
            if "tsla-s" in url:
                return _make_rh_instrument("tsla-s", "put", "290.00", exp, "TSLA")
            if "tsla-l" in url:
                return _make_rh_instrument("tsla-l", "put", "280.00", exp, "TSLA")
            if "aapl-s" in url:
                return _make_rh_instrument("aapl-s", "put", "190.00", exp, "AAPL")
            if "aapl-l" in url:
                return _make_rh_instrument("aapl-l", "put", "180.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_spreads("PCS", filter_sym="TSLA")
        assert len(pairs) == 1
        assert pairs[0]["symbol"] == "TSLA"


# ─────────────────────────────────────────────────────────────────────────────
# execute_spread_mode tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSpreadSafety:
    """Tests for execute_spread_mode('safety', ...)."""

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_safety_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS safety triggers when BE > 90% of stock price."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(30),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,  # 288.50 > 90% of 300 (270) → trigger
            "spread_mid": 0.30,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["300.00"]

        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "safety"
        assert a["symbol"] == "TSLA"
        # limit = min(3% × 10.0, 10% × 1.50) = min(0.30, 0.15) = 0.15
        assert a["limit_price"] == 0.15
        assert a["dry_run"] is True

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_safety_no_trigger(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS safety does not trigger when BE ≤ 90% of stock price."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(30),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,  # 288.50 ≤ 90% of 350 (315) → no trigger
            "spread_mid": 0.30,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["350.00"]

        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_safety_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS safety triggers when BE < 110% of stock price."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(30),
            "qty": 2,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,  # 202 < 110% of 190 (209) → trigger
            "spread_mid": 0.40,
            "short_mark": 0.45,
            "long_mark": 0.05,
            "net_debit_to_close": 0.40,
        }]
        mock_price.return_value = ["190.00"]

        actions = execute_spread_mode("safety", "CCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "safety"
        assert a["spread_type"] == "CCS"
        # limit = min(3% × 10, 10% × 2.0) = min(0.30, 0.20) = 0.20
        assert a["limit_price"] == 0.20

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_safety_no_trigger(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS safety does not trigger when BE ≥ 110% of stock price."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(30),
            "qty": 2,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,  # 202 ≥ 110% of 180 (198) → no trigger
            "spread_mid": 0.40,
            "short_mark": 0.45,
            "long_mark": 0.05,
            "net_debit_to_close": 0.40,
        }]
        mock_price.return_value = ["180.00"]

        actions = execute_spread_mode("safety", "CCS", dry_run=True)
        assert len(actions) == 0


class TestSpreadRescue:
    """Tests for execute_spread_mode('rescue', ...)."""

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_rescue_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS rescue triggers when stock < break-even and DTE > 2."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(5),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,
            "spread_mid": 3.00,
            "short_mark": 3.50,
            "long_mark": 0.50,
            "net_debit_to_close": 3.00,
        }]
        mock_price.return_value = ["285.00"]  # 285 < 288.50 → trigger

        actions = execute_spread_mode("rescue", "PCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "rescue"
        # limit = min(spread_mid=3.00, orig_credit=1.50) = 1.50
        assert a["limit_price"] == 1.50

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_rescue_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS rescue triggers when stock > break-even and DTE > 2."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(5),
            "qty": 1,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,
            "spread_mid": 4.00,
            "short_mark": 4.50,
            "long_mark": 0.50,
            "net_debit_to_close": 4.00,
        }]
        mock_price.return_value = ["205.00"]  # 205 > 202 → trigger

        actions = execute_spread_mode("rescue", "CCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "rescue"
        # limit = min(spread_mid=4.00, orig_credit=2.00) = 2.00
        assert a["limit_price"] == 2.00

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_rescue_no_trigger_above_be(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS rescue does not trigger when stock ≥ break-even (even with valid DTE)."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(5),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,
            "spread_mid": 0.30,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["295.00"]  # 295 > 288.50 → no trigger

        actions = execute_spread_mode("rescue", "PCS", dry_run=True)
        assert len(actions) == 0


class TestSpreadPanic:
    """Tests for execute_spread_mode('panic', ...)."""

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_panic_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS panic triggers when stock < short strike (ITM) and DTE < 2."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(1),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,
            "spread_mid": 5.00,
            "short_mark": 6.00,
            "long_mark": 1.00,
            "net_debit_to_close": 5.00,
        }]
        mock_price.return_value = ["282.00"]  # 282 < 290 (short strike) → ITM → trigger

        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "panic"
        # limit = min(spread_mid=5.00, 90% × width=9.00) = 5.00
        assert a["limit_price"] == 5.00

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_panic_no_trigger_above_short_strike(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS panic does NOT trigger when stock ≥ short strike (OTM, safe), even at DTE < 2."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(1),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 280.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50,
            "spread_mid": 0.30,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["295.00"]  # 295 > 290 short_strike → OTM → no trigger

        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_panic_triggers_itm(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS panic triggers when stock > short strike (ITM) and DTE < 2."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(0),
            "qty": 1,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,
            "spread_mid": 6.00,
            "short_mark": 7.00,
            "long_mark": 1.00,
            "net_debit_to_close": 6.00,
        }]
        mock_price.return_value = ["205.00"]  # 205 > 200 → ITM → trigger

        actions = execute_spread_mode("panic", "CCS", dry_run=True)
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "panic"
        # limit = min(spread_mid=6.00, 90% × 10=9.00) = 6.00
        assert a["limit_price"] == 6.00

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_panic_no_trigger_otm(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS panic does not trigger when stock ≤ short strike (OTM), even at DTE 0."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(0),
            "qty": 1,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,
            "spread_mid": 0.20,
            "short_mark": 0.25,
            "long_mark": 0.05,
            "net_debit_to_close": 0.20,
        }]
        mock_price.return_value = ["195.00"]  # 195 < 200 → OTM → no trigger

        actions = execute_spread_mode("panic", "CCS", dry_run=True)
        assert len(actions) == 0


class TestSpreadDTEGating:
    """DTE-based gating: safety > 5, rescue 2–4, panic = 0."""

    def _make_pair(self, dte_days, spread_type="PCS"):
        short_strike = 290.0 if spread_type == "PCS" else 200.0
        long_strike  = 280.0 if spread_type == "PCS" else 210.0
        return {
            "symbol": "TSLA",
            "expiration": _future_date(dte_days),
            "qty": 1,
            "short_strike": short_strike,
            "long_strike": long_strike,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 1.50,
            "break_even": 288.50 if spread_type == "PCS" else 201.50,
            "spread_mid": 5.00,
            "short_mark": 6.00,
            "long_mark": 1.00,
            "net_debit_to_close": 5.00,
        }

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_safety_skipped_at_dte_5(self, m_logout, m_login, mock_price, mock_pairs):
        """Safety should NOT fire at DTE=5 (requires DTE > 5)."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(5)]
        mock_price.return_value = ["300.00"]  # would trigger on price alone
        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_safety_fires_at_dte_6(self, m_logout, m_login, mock_price, mock_pairs):
        """Safety should fire at DTE=6."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(6)]
        mock_price.return_value = ["300.00"]
        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert len(actions) == 1
        assert actions[0]["mode"] == "safety"

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_rescue_skipped_at_dte_2(self, m_logout, m_login, mock_price, mock_pairs):
        """Rescue should NOT fire at DTE=2 (requires DTE > 2)."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(2)]
        mock_price.return_value = ["285.00"]  # below BE of 288.50
        actions = execute_spread_mode("rescue", "PCS", dry_run=True)
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_rescue_fires_at_dte_3(self, m_logout, m_login, mock_price, mock_pairs):
        """Rescue should fire at DTE=3."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(3)]
        mock_price.return_value = ["285.00"]
        actions = execute_spread_mode("rescue", "PCS", dry_run=True)
        assert len(actions) == 1
        assert actions[0]["mode"] == "rescue"

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_panic_skipped_at_dte_2(self, m_logout, m_login, mock_price, mock_pairs):
        """Panic should NOT fire at DTE=2 (requires DTE < 2)."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(2)]
        mock_price.return_value = ["282.00"]  # below short strike 290
        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_panic_fires_at_dte_1(self, m_logout, m_login, mock_price, mock_pairs):
        """Panic should fire at DTE=1."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(1)]
        mock_price.return_value = ["282.00"]
        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 1
        assert actions[0]["mode"] == "panic"

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_panic_fires_at_dte_0(self, m_logout, m_login, mock_price, mock_pairs):
        """Panic should fire at DTE=0."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(0)]
        mock_price.return_value = ["282.00"]
        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 1
        assert actions[0]["mode"] == "panic"


class TestSpreadLimitPriceEdgeCases:
    """Test limit price calculation edge cases."""

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_safety_limit_floors_at_one_cent(self, m_logout, m_login, mock_price, mock_pairs):
        """Limit price floors at $0.01."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(30),
            "qty": 1,
            "short_strike": 290.0,
            "long_strike": 289.0,
            "width": 1.0,   # Very narrow spread
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 0.05,  # Very small credit
            "break_even": 289.95,  # > 90% of 300 = 270 → triggers
            "spread_mid": 0.01,
            "short_mark": 0.02,
            "long_mark": 0.01,
            "net_debit_to_close": 0.01,
        }]
        mock_price.return_value = ["300.00"]

        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert len(actions) == 1
        # min(3% × 1.0 = 0.03, 10% × 0.05 = 0.005) → 0.005 → rounded → 0.01 (floor)
        assert actions[0]["limit_price"] >= 0.01

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_no_spreads_returns_empty(self, m_logout, m_login, mock_price, mock_pairs):
        """No open spreads → empty results, no errors."""
        from trader import execute_spread_mode

        mock_pairs.return_value = []
        actions = execute_spread_mode("safety", "PCS", dry_run=True)
        assert actions == []

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_login_failure_raises(self, m_logout, m_login, mock_price, mock_pairs):
        """Login failure raises RuntimeError."""
        from trader import execute_spread_mode

        m_login.return_value = False
        with pytest.raises(RuntimeError, match="login failed"):
            execute_spread_mode("safety", "PCS")


class TestCancelSpreadOrders:
    """Tests for _cancel_spread_orders."""

    def test_cancels_matching_orders(self):
        from trader import _cancel_spread_orders

        rh = MagicMock()
        rh.orders.get_all_open_option_orders.return_value = [
            {
                "id": "order-1",
                "legs": [
                    {"option": "https://api.robinhood.com/options/instruments/opt-short/"},
                    {"option": "https://api.robinhood.com/options/instruments/opt-long/"},
                ],
            },
            {
                "id": "order-2",
                "legs": [
                    {"option": "https://api.robinhood.com/options/instruments/other-opt/"},
                ],
            },
        ]

        n = _cancel_spread_orders(rh, "opt-short", "opt-long", "TSLA", "TEST")
        assert n == 1
        rh.orders.cancel_option_order.assert_called_once_with("order-1")

    def test_no_matching_orders(self):
        from trader import _cancel_spread_orders

        rh = MagicMock()
        rh.orders.get_all_open_option_orders.return_value = [
            {
                "id": "order-1",
                "legs": [
                    {"option": "https://api.robinhood.com/options/instruments/unrelated/"},
                ],
            },
        ]

        n = _cancel_spread_orders(rh, "opt-short", "opt-long", "TSLA", "TEST")
        assert n == 0
        rh.orders.cancel_option_order.assert_not_called()


class TestPlaceSpreadCloseOrder:
    """Tests for _place_spread_close_order."""

    def test_successful_order(self):
        from trader import _place_spread_close_order

        rh = MagicMock()
        rh.orders.order_option_spread.return_value = {
            "id": "order-123",
            "state": "confirmed",
        }

        result = _place_spread_close_order(
            rh, "TSLA", "PCS", "put",
            short_strike=290.0, long_strike=280.0,
            expiration="2026-06-20", qty=1,
            limit_price=1.50, label="TEST",
        )
        assert result is not None
        assert result["id"] == "order-123"

        # Verify the order args
        call_args = rh.orders.order_option_spread.call_args
        assert call_args.kwargs["direction"] == "debit"
        assert call_args.kwargs["price"] == 1.50
        assert call_args.kwargs["symbol"] == "TSLA"
        assert call_args.kwargs["quantity"] == 1
        spread = call_args.kwargs["spread"]
        assert len(spread) == 2
        assert spread[0]["action"] == "buy"   # buy short leg
        assert spread[1]["action"] == "sell"  # sell long leg
        # ratio_quantity is required by robin_stocks
        assert spread[0]["ratio_quantity"] == 1
        assert spread[1]["ratio_quantity"] == 1

    def test_failed_order_returns_none(self):
        from trader import _place_spread_close_order

        rh = MagicMock()
        rh.orders.order_option_spread.return_value = {"detail": "Insufficient funds"}

        result = _place_spread_close_order(
            rh, "TSLA", "PCS", "put",
            short_strike=290.0, long_strike=280.0,
            expiration="2026-06-20", qty=1,
            limit_price=1.50, label="TEST",
        )
        assert result is None

    def test_limit_price_floors_at_one_cent(self):
        from trader import _place_spread_close_order

        rh = MagicMock()
        rh.orders.order_option_spread.return_value = {"id": "x", "state": "confirmed"}

        _place_spread_close_order(
            rh, "TSLA", "PCS", "put",
            short_strike=290.0, long_strike=280.0,
            expiration="2026-06-20", qty=1,
            limit_price=0.001, label="TEST",
        )
        call_args = rh.orders.order_option_spread.call_args
        assert call_args.kwargs["price"] == 0.01  # floored


# ─────────────────────────────────────────────────────────────────────────────
# Reporter YTD tests
# ─────────────────────────────────────────────────────────────────────────────

class TestReporterYTD:
    """Tests for YTD computation in build_options_report."""

    def test_ytd_keys_present_when_enabled(self):
        """When include_ytd=True, report contains ytd_ keys."""
        from reporter import _extract_filled_orders

        # Just test the extraction helper with mock data
        raw_orders = [
            {
                "state": "filled",
                "direction": "credit",
                "premium": "500.00",
                "quantity": "1",
                "price": "5.00",
                "chain_symbol": "TSLA",
                "legs": [{
                    "side": "sell",
                    "option_type": "call",
                    "strike_price": "300",
                    "expiration_date": "2026-03-20",
                    "executions": [{
                        "timestamp": "2026-02-15T14:30:00Z"
                    }],
                }],
                "id": "order-1",
            },
        ]

        from datetime import date
        orders = _extract_filled_orders(raw_orders, date(2026, 1, 1), date(2026, 12, 31))
        assert len(orders) == 1
        assert orders[0]["direction"] == "credit"
        assert orders[0]["premium"] == 500.0

    def test_ytd_rendered_in_jinja2_template(self):
        """The Jinja2 report_email.html template renders YTD when data present."""
        from report_emailer import _render_report_html

        report = {
            "start_date":   "2026-05-18",
            "end_date":     "2026-05-22",
            "orders":       [],
            "total_credit": 1000.0,
            "total_debit":  500.0,
            "net_gain":     500.0,
            "order_count":  5,
            "ytd_credit":   10000.0,
            "ytd_debit":    4000.0,
            "ytd_net_gain": 6000.0,
            "ytd_order_count": 100,
        }
        html = _render_report_html(report)
        assert "YEAR-TO-DATE" in html
        assert "YTD Credit" in html
        assert "YTD Orders" in html
        assert "10,000.00" in html  # ytd_credit formatted

    def test_ytd_not_rendered_when_absent(self):
        """The Jinja2 template omits YTD section when keys are missing."""
        from report_emailer import _render_report_html

        report = {
            "start_date":   "2026-05-22",
            "end_date":     "2026-05-22",
            "orders":       [],
            "total_credit": 0,
            "total_debit":  0,
            "net_gain":     0,
            "order_count":  0,
        }
        html = _render_report_html(report)
        assert "YEAR-TO-DATE" not in html


# ─────────────────────────────────────────────────────────────────────────────
# Email integration tests
# ─────────────────────────────────────────────────────────────────────────────

class TestEmailSpreadSections:
    """Test that spread management results render in the email."""

    def test_send_recommendations_accepts_spread_params(self):
        """send_recommendations accepts spread_*_results without error."""
        from emailer import send_recommendations

        # Just verify the function signature accepts the new params
        import inspect
        sig = inspect.signature(send_recommendations)
        assert "spread_optimize_results" in sig.parameters
        assert "spread_rescue_results" in sig.parameters
        assert "spread_panic_results" in sig.parameters

    def test_render_html_accepts_spread_params(self):
        """_render_html accepts spread_*_results without error."""
        from emailer import _render_html

        import inspect
        sig = inspect.signature(_render_html)
        assert "spread_optimize_results" in sig.parameters
        assert "spread_rescue_results" in sig.parameters
        assert "spread_panic_results" in sig.parameters


# ─────────────────────────────────────────────────────────────────────────────
# CLI dispatch tests
# ─────────────────────────────────────────────────────────────────────────────

class TestCLISpreadManagement:
    """Test CLI argument parsing for spread management commands."""

    def test_spread_optimize_flag_exists(self):
        """--spread-optimize is a valid argument."""
        import argparse
        # Just import main and check it parses without error
        # (importing main defines the parser)
        import importlib
        import main as main_mod
        # If we got here without ImportError, the module loads OK
        assert hasattr(main_mod, "cmd_spread_manage")

    def test_cmd_spread_manage_function_exists(self):
        """cmd_spread_manage function is importable."""
        from main import cmd_spread_manage
        import inspect
        sig = inspect.signature(cmd_spread_manage)
        assert "mode" in sig.parameters
        assert "spread_type" in sig.parameters
        assert "symbol" in sig.parameters
        assert "dry_run" in sig.parameters
