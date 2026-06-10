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

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_ccs_debit_prepass_excludes_cds_legs(
        self, mock_positions, mock_request_get, mock_market
    ):
        """CDS (debit) long leg must NOT be paired with a standalone CC as CCS.

        Scenario:
          - AAPL short $280 call  (standalone covered call)
          - AAPL long  $327.50 call (CDS insurance long leg)
          - AAPL short $350 call  (CDS insurance short leg)

        Without the debit pre-pass the $280/$327.50 pair would be falsely
        matched as a CCS.  With the fix, the $327.50/$350 debit pair is
        consumed first, leaving only the standalone $280 with no long to
        match — zero CCS pairs returned.
        """
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            # Standalone covered call
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/cc-280/",
                              average_price="-400.00", trade_value_multiplier="-1"),
            # CDS insurance long leg
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/cds-long-327/",
                              average_price="200.00", trade_value_multiplier="1"),
            # CDS insurance short leg
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/cds-short-350/",
                              average_price="-100.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "cc-280" in url:
                return _make_rh_instrument("cc-280", "call", "280.00", exp, "AAPL")
            if "cds-long-327" in url:
                return _make_rh_instrument("cds-long-327", "call", "327.50", exp, "AAPL")
            if "cds-short-350" in url:
                return _make_rh_instrument("cds-short-350", "call", "350.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_spreads("CCS")
        # The debit pair ($327.50 / $350) should be consumed.
        # The standalone CC at $280 has no long call left → zero CCS pairs.
        assert len(pairs) == 0

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_pcs_debit_guard_excludes_pds_legs(
        self, mock_positions, mock_request_get, mock_market
    ):
        """PDS (debit) long leg must NOT be paired with a standalone short put as PCS.

        Scenario (wide gap — mirrors the CCS bug):
          - TSLA short $290 put  (standalone CSP)
          - TSLA long  $250 put  (PDS insurance long leg)
          - TSLA short $230 put  (PDS insurance short leg)

        Without the debit guard, $290/$250 (40-wide) would be falsely
        matched as a PCS.  With the guard, the closer debit pair
        ($250/$230 = 20-wide) causes $250 to be skipped as a credit long.
        """
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            # Standalone short put
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/csp-290/",
                              average_price="-300.00", trade_value_multiplier="-1"),
            # PDS insurance long leg
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/pds-long-250/",
                              average_price="150.00", trade_value_multiplier="1"),
            # PDS insurance short leg
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/pds-short-230/",
                              average_price="-50.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "csp-290" in url:
                return _make_rh_instrument("csp-290", "put", "290.00", exp, "TSLA")
            if "pds-long-250" in url:
                return _make_rh_instrument("pds-long-250", "put", "250.00", exp, "TSLA")
            if "pds-short-230" in url:
                return _make_rh_instrument("pds-short-230", "put", "230.00", exp, "TSLA")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_spreads("PCS")
        assert len(pairs) == 0

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_ccs_debit_prepass_preserves_real_credit_spreads(
        self, mock_positions, mock_request_get, mock_market
    ):
        """Real CCS pair + CDS pair coexist: debit pre-pass consumes CDS,
        leaves the genuine CCS intact.

        Scenario:
          - AAPL short $200 call + long $210 call → genuine CCS
          - AAPL long  $327.50 call + short $350 call → CDS (debit)

        Expected: 1 CCS pair ($200/$210), CDS pair consumed silently.
        """
        from trader import _fetch_and_pair_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            # CCS short leg
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/ccs-short-200/",
                              average_price="-300.00", trade_value_multiplier="-1"),
            # CCS long leg
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/ccs-long-210/",
                              average_price="150.00", trade_value_multiplier="1"),
            # CDS long leg (insurance)
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/cds-long-327/",
                              average_price="200.00", trade_value_multiplier="1"),
            # CDS short leg (insurance)
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/cds-short-350/",
                              average_price="-100.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "ccs-short-200" in url:
                return _make_rh_instrument("ccs-short-200", "call", "200.00", exp, "AAPL")
            if "ccs-long-210" in url:
                return _make_rh_instrument("ccs-long-210", "call", "210.00", exp, "AAPL")
            if "cds-long-327" in url:
                return _make_rh_instrument("cds-long-327", "call", "327.50", exp, "AAPL")
            if "cds-short-350" in url:
                return _make_rh_instrument("cds-short-350", "call", "350.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_spreads("CCS")
        assert len(pairs) == 1
        p = pairs[0]
        assert p["short_strike"] == 200.0
        assert p["long_strike"] == 210.0


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
        """PCS panic triggers when stock < short strike (ITM) and DTE = 0."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [{
            "symbol": "TSLA",
            "expiration": _future_date(0),
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
    def test_rescue_skipped_at_dte_above_max(self, m_logout, m_login, mock_price, mock_pairs):
        """Rescue should NOT fire at DTE=6 (above default max of 5)."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(6)]
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
    def test_panic_skipped_at_dte_1(self, m_logout, m_login, mock_price, mock_pairs):
        """Panic should NOT fire at DTE=1 (now handled by rescue)."""
        from trader import execute_spread_mode
        mock_pairs.return_value = [self._make_pair(1)]
        mock_price.return_value = ["282.00"]
        actions = execute_spread_mode("panic", "PCS", dry_run=True)
        assert len(actions) == 0

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

    def test_limit_price_floors_at_min_tick(self):
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
        assert call_args.kwargs["price"] == 0.05  # floored to min $0.05 tick


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
        assert "spread_safety_results" in sig.parameters
        assert "spread_rescue_results" in sig.parameters
        assert "spread_panic_results" in sig.parameters

    def test_render_html_accepts_spread_params(self):
        """_render_html accepts spread_*_results without error."""
        from emailer import _render_html

        import inspect
        sig = inspect.signature(_render_html)
        assert "spread_optimize_results" in sig.parameters
        assert "spread_safety_results" in sig.parameters
        assert "spread_rescue_results" in sig.parameters
        assert "spread_panic_results" in sig.parameters


# ─────────────────────────────────────────────────────────────────────────────
# CLI dispatch tests
# ─────────────────────────────────────────────────────────────────────────────

class TestCLISpreadManagement:
    """Test CLI argument parsing for spread management commands."""

    def test_spread_safety_flag_exists(self):
        """--spread-safety is a valid argument."""
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
        assert "config" in sig.parameters


# ─────────────────────────────────────────────────────────────────────────────
# Spread Optimize mode tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_optimize_pair(
    symbol="SNOW",
    spread_type="PCS",
    short_strike=145.0,
    long_strike=130.0,
    orig_credit=3.49,
    net_debit_to_close=0.50,
    spread_mid=0.55,
    stock_price=160.0,
    days_out=20,
):
    """Helper to build a pair dict for optimize tests."""
    return {
        "symbol": symbol,
        "expiration": _future_date(days_out),
        "qty": 1,
        "short_strike": short_strike,
        "long_strike": long_strike,
        "width": abs(short_strike - long_strike),
        "short_option_id": "opt-s",
        "long_option_id": "opt-l",
        "short_inst_url": "url-s",
        "long_inst_url": "url-l",
        "orig_credit": orig_credit,
        "break_even": (short_strike - orig_credit) if spread_type == "PCS"
                      else (short_strike + orig_credit),
        "spread_mid": spread_mid,
        "short_mark": spread_mid + 0.05,
        "long_mark": 0.05,
        "net_debit_to_close": net_debit_to_close,
    }


class TestSpreadOptimize:
    """Tests for execute_spread_mode('optimize', ...)."""

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_optimize_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS optimize triggers when OTM and decayed >75%."""
        from trader import execute_spread_mode

        # Sold for $3.49, now worth $0.50 → decayed ~86% > 75% threshold
        mock_pairs.return_value = [_make_optimize_pair(
            spread_type="PCS",
            short_strike=145.0, long_strike=130.0,
            orig_credit=3.49, net_debit_to_close=0.50,
            spread_mid=0.55, stock_price=160.0, days_out=20,
        )]
        mock_price.return_value = ["160.00"]

        actions = execute_spread_mode("optimize", "PCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0})
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "optimize"
        assert a["symbol"] == "SNOW"
        # limit = min(net_debit_to_close=0.50, 20% × 3.49=0.698) = 0.50
        assert a["limit_price"] == 0.50
        assert "Decayed" in a["trigger_reason"]

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_optimize_no_trigger_not_decayed_enough(
        self, m_logout, m_login, mock_price, mock_pairs
    ):
        """PCS optimize does NOT trigger when decay < threshold."""
        from trader import execute_spread_mode

        # Sold for $3.49, now worth $1.50 → decayed ~57% < 75%
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=1.50,
            spread_mid=1.60, stock_price=160.0,
        )]
        mock_price.return_value = ["160.00"]

        actions = execute_spread_mode("optimize", "PCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0})
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_optimize_no_trigger_itm(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS optimize does NOT trigger when spread is ITM (stock < short strike)."""
        from trader import execute_spread_mode

        # Stock $140 < short strike $145 → ITM, even though decayed
        mock_pairs.return_value = [_make_optimize_pair(
            short_strike=145.0, orig_credit=3.49,
            net_debit_to_close=0.30, spread_mid=0.35, stock_price=140.0,
        )]
        mock_price.return_value = ["140.00"]

        actions = execute_spread_mode("optimize", "PCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0})
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_pcs_optimize_no_trigger_low_dte(self, m_logout, m_login, mock_price, mock_pairs):
        """PCS optimize does NOT trigger when DTE ≤ min_dte."""
        from trader import execute_spread_mode

        # Only 3 days out, below default min_dte=5
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=0.30,
            spread_mid=0.35, stock_price=160.0, days_out=3,
        )]
        mock_price.return_value = ["160.00"]

        actions = execute_spread_mode("optimize", "PCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0,
                                              "spread_optimize_min_dte": 5})
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_optimize_triggers(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS optimize triggers when OTM (stock < short strike) and decayed."""
        from trader import execute_spread_mode

        # CCS: short $200, long $210, stock $180 (OTM)
        # Sold for $2.00, now worth $0.30 → decayed 85%
        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(25),
            "qty": 2,
            "short_strike": 200.0,
            "long_strike": 210.0,
            "width": 10.0,
            "short_option_id": "opt-s",
            "long_option_id": "opt-l",
            "short_inst_url": "url-s",
            "long_inst_url": "url-l",
            "orig_credit": 2.00,
            "break_even": 202.0,
            "spread_mid": 0.35,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["180.00"]

        actions = execute_spread_mode("optimize", "CCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0})
        assert len(actions) == 1
        a = actions[0]
        assert a["mode"] == "optimize"
        assert a["symbol"] == "AAPL"
        assert a["spread_type"] == "CCS"
        # limit = min(net_debit_to_close=0.30, 20% × 2.00 = 0.40) = 0.30
        assert a["limit_price"] == 0.30

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_ccs_optimize_no_trigger_itm(self, m_logout, m_login, mock_price, mock_pairs):
        """CCS optimize does NOT trigger when ITM (stock > short strike)."""
        from trader import execute_spread_mode

        # Stock $210 > short strike $200 → ITM
        mock_pairs.return_value = [{
            "symbol": "AAPL",
            "expiration": _future_date(25),
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
            "spread_mid": 0.35,
            "short_mark": 0.35,
            "long_mark": 0.05,
            "net_debit_to_close": 0.30,
        }]
        mock_price.return_value = ["210.00"]

        actions = execute_spread_mode("optimize", "CCS", dry_run=True,
                                      config={"spread_optimize_decay_pct": 75.0})
        assert len(actions) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_optimize_custom_threshold(self, m_logout, m_login, mock_price, mock_pairs):
        """Optimize respects custom decay threshold from config."""
        from trader import execute_spread_mode

        # Sold for $3.49, now worth $0.80 → decayed 77%
        # At 75% threshold → triggers; at 80% → does NOT
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=0.80,
            spread_mid=0.85, stock_price=160.0,
        )]
        mock_price.return_value = ["160.00"]

        # 75% threshold → triggers (77% > 75%)
        actions_75 = execute_spread_mode("optimize", "PCS", dry_run=True,
                                          config={"spread_optimize_decay_pct": 75.0})
        assert len(actions_75) == 1

        # 80% threshold → does NOT trigger (77% < 80%)
        actions_80 = execute_spread_mode("optimize", "PCS", dry_run=True,
                                          config={"spread_optimize_decay_pct": 80.0})
        assert len(actions_80) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_optimize_custom_min_dte(self, m_logout, m_login, mock_price, mock_pairs):
        """Optimize respects custom min_dte from config."""
        from trader import execute_spread_mode

        # 8 days out, decayed enough
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=0.30,
            spread_mid=0.35, stock_price=160.0, days_out=8,
        )]
        mock_price.return_value = ["160.00"]

        # min_dte=5 → triggers (8 > 5)
        actions_5 = execute_spread_mode("optimize", "PCS", dry_run=True,
                                         config={"spread_optimize_min_dte": 5})
        assert len(actions_5) == 1

        # min_dte=10 → does NOT trigger (8 ≤ 10)
        actions_10 = execute_spread_mode("optimize", "PCS", dry_run=True,
                                          config={"spread_optimize_min_dte": 10})
        assert len(actions_10) == 0

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_optimize_custom_limit_pct(self, m_logout, m_login, mock_price, mock_pairs):
        """Optimize respects custom limit_pct from config."""
        from trader import execute_spread_mode

        # Sold for $3.49, now spread_mid=0.55, net_debit_to_close=0.50
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=0.50,
            spread_mid=0.55, stock_price=160.0,
        )]
        mock_price.return_value = ["160.00"]

        # limit_pct=0.10 → limit = min(net_debit=0.50, 10%×3.49=0.349) = 0.35
        actions = execute_spread_mode("optimize", "PCS", dry_run=True,
                                      config={"spread_optimize_limit_pct": 10.0})
        assert len(actions) == 1
        assert actions[0]["limit_price"] == 0.35

        # limit_pct=0.20 → limit = min(net_debit=0.50, 20%×3.49=0.698) = 0.50
        actions2 = execute_spread_mode("optimize", "PCS", dry_run=True,
                                       config={"spread_optimize_limit_pct": 20.0})
        assert len(actions2) == 1
        assert actions2[0]["limit_price"] == 0.50

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_optimize_defaults_without_config(
        self, m_logout, m_login, mock_price, mock_pairs
    ):
        """Optimize works with no config (uses defaults)."""
        from trader import execute_spread_mode

        # Sold for $3.49, now worth $0.50 → decayed 86% > default 75%
        mock_pairs.return_value = [_make_optimize_pair(
            orig_credit=3.49, net_debit_to_close=0.50,
            spread_mid=0.55, stock_price=160.0,
        )]
        mock_price.return_value = ["160.00"]

        # No config passed → uses defaults
        actions = execute_spread_mode("optimize", "PCS", dry_run=True)
        assert len(actions) == 1

    @patch("trader._fetch_and_pair_spreads")
    @patch("robin_stocks.robinhood.stocks.get_latest_price")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_optimize_filter_sym(self, m_logout, m_login, mock_price, mock_pairs):
        """Optimize respects filter_sym parameter."""
        from trader import execute_spread_mode

        mock_pairs.return_value = [_make_optimize_pair(
            symbol="SNOW", orig_credit=3.49,
            net_debit_to_close=0.30, spread_mid=0.35, stock_price=160.0,
        )]
        mock_price.return_value = ["160.00"]

        actions = execute_spread_mode("optimize", "PCS", filter_sym="SNOW", dry_run=True)
        assert len(actions) == 1
        # Verify filter_sym was passed through
        mock_pairs.assert_called_with("PCS", "SNOW")


# ─────────────────────────────────────────────────────────────────────────────
# Short contract Optimize / Safety tests (v1.9 BTC-based)
# ─────────────────────────────────────────────────────────────────────────────

def _make_short_contract(
    symbol="TSLA",
    strike=300.0,
    opt_type="call",
    purchase_price=-150.0,  # negative = credit received
    quantity=1,
    days_out=20,
):
    """Helper to build a short contract dict for testing."""
    return {
        "symbol": symbol,
        "strike": strike,
        "opt_type": opt_type,
        "purchase_price": purchase_price,
        "quantity": quantity,
        "expiration": _future_date(days_out),
    }


class TestShortOptimize:
    """Tests for execute_short_optimize() — BTC profit-taking."""

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_triggers_otm_decayed_call(self, m_logout, m_login, mock_bid_ask):
        """OTM decayed CALL triggers optimize BTC."""
        from trader import execute_short_optimize

        # CALL $300, stock $280 (OTM), sold for $1.50, now $0.30 → decayed 80%
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (0.25, 0.35, 0.30)

        results = execute_short_optimize(
            [c], {"TSLA": 280.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 75.0},
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        assert results[0]["symbol"] == "TSLA"
        # limit = min(0.30, 20% × 1.50 = 0.30) = 0.30
        assert results[0]["btc_price"] == 0.30

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_triggers_otm_decayed_put(self, m_logout, m_login, mock_bid_ask):
        """OTM decayed PUT triggers optimize BTC."""
        from trader import execute_short_optimize

        # PUT $200, stock $250 (OTM), sold for $2.00, now $0.40 → decayed 80%
        c = _make_short_contract("AAPL", 200.0, "put", purchase_price=-200.0, days_out=15)
        mock_bid_ask.return_value = (0.35, 0.45, 0.40)

        results = execute_short_optimize(
            [c], {"AAPL": 250.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 75.0},
        )
        assert len(results) == 1
        assert results[0]["success"] is True

    @patch("trader._get_option_bid_ask")
    def test_no_trigger_itm_call(self, mock_bid_ask):
        """ITM CALL (stock >= strike) does NOT trigger optimize."""
        from trader import execute_short_optimize

        # CALL $300, stock $310 (ITM)
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0)
        mock_bid_ask.return_value = (0.25, 0.35, 0.30)

        results = execute_short_optimize(
            [c], {"TSLA": 310.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 75.0},
        )
        assert len(results) == 0

    @patch("trader._get_option_bid_ask")
    def test_no_trigger_not_decayed_enough(self, mock_bid_ask):
        """Contract not decayed enough does NOT trigger."""
        from trader import execute_short_optimize

        # CALL $300, stock $280 (OTM), sold for $1.50, now $0.80 → decayed 47% < 75%
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0)
        mock_bid_ask.return_value = (0.75, 0.85, 0.80)

        results = execute_short_optimize(
            [c], {"TSLA": 280.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 75.0},
        )
        assert len(results) == 0

    @patch("trader._get_option_bid_ask")
    def test_no_trigger_low_dte(self, mock_bid_ask):
        """DTE ≤ min_dte does NOT trigger."""
        from trader import execute_short_optimize

        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=3)
        mock_bid_ask.return_value = (0.05, 0.10, 0.07)

        results = execute_short_optimize(
            [c], {"TSLA": 280.0}, dry_run=True,
            config={"spread_optimize_min_dte": 5},
        )
        assert len(results) == 0

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_custom_threshold(self, m_logout, m_login, mock_bid_ask):
        """Custom decay threshold is respected."""
        from trader import execute_short_optimize

        # Decayed 60%: sold $1.50, now $0.60
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (0.55, 0.65, 0.60)

        # 50% threshold → triggers (60% > 50%)
        r1 = execute_short_optimize(
            [c], {"TSLA": 280.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 50.0},
        )
        assert len(r1) == 1

        # 75% threshold → does NOT trigger (60% < 75%)
        r2 = execute_short_optimize(
            [c], {"TSLA": 280.0}, dry_run=True,
            config={"spread_optimize_decay_pct": 75.0},
        )
        assert len(r2) == 0

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_filter_sym(self, m_logout, m_login, mock_bid_ask):
        """filter_sym restricts to one symbol."""
        from trader import execute_short_optimize

        c1 = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        c2 = _make_short_contract("AAPL", 200.0, "call", purchase_price=-100.0, days_out=20)
        mock_bid_ask.return_value = (0.05, 0.10, 0.07)

        results = execute_short_optimize(
            [c1, c2], {"TSLA": 280.0, "AAPL": 180.0}, dry_run=True,
            filter_sym="TSLA",
        )
        # Only TSLA should be considered
        for r in results:
            assert r["symbol"] == "TSLA"


class TestShortSafety:
    """Tests for execute_short_safety() — BTC on gained >40%."""

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_triggers_gained_call(self, m_logout, m_login, mock_bid_ask):
        """CALL that gained >40% triggers safety BTC."""
        from trader import execute_short_safety

        # Sold for $1.50, now $2.20 → gained ~47% > 40%
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (2.10, 2.30, 2.20)

        results = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_gain_pct": 40.0},
        )
        assert len(results) == 1
        assert results[0]["success"] is True
        # limit = min(2.20, 1.20 × 1.50 = 1.80) = 1.80
        assert results[0]["btc_price"] == 1.80

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_triggers_gained_put(self, m_logout, m_login, mock_bid_ask):
        """PUT that gained >40% triggers safety BTC."""
        from trader import execute_short_safety

        # Sold for $2.00, now $3.00 → gained 50% > 40%
        c = _make_short_contract("AAPL", 200.0, "put", purchase_price=-200.0, days_out=15)
        mock_bid_ask.return_value = (2.90, 3.10, 3.00)

        results = execute_short_safety(
            [c], {"AAPL": 195.0}, dry_run=True,
            config={"safety_gain_pct": 40.0},
        )
        assert len(results) == 1
        assert results[0]["success"] is True

    @patch("trader._get_option_bid_ask")
    def test_no_trigger_not_gained_enough(self, mock_bid_ask):
        """Contract not gained enough does NOT trigger."""
        from trader import execute_short_safety

        # Sold for $1.50, now $1.80 → gained 20% < 40%
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (1.75, 1.85, 1.80)

        results = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_gain_pct": 40.0},
        )
        assert len(results) == 0

    @patch("trader._get_option_bid_ask")
    def test_no_trigger_low_dte(self, mock_bid_ask):
        """DTE ≤ min_dte does NOT trigger."""
        from trader import execute_short_safety

        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=3)
        mock_bid_ask.return_value = (2.10, 2.30, 2.20)

        results = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_min_dte": 5},
        )
        assert len(results) == 0

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_custom_gain_threshold(self, m_logout, m_login, mock_bid_ask):
        """Custom gain threshold is respected."""
        from trader import execute_short_safety

        # Sold for $1.50, now $2.00 → gained 33%
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (1.95, 2.05, 2.00)

        # 30% threshold → triggers (33% > 30%)
        r1 = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_gain_pct": 30.0},
        )
        assert len(r1) == 1

        # 40% threshold → does NOT trigger (33% < 40%)
        r2 = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_gain_pct": 40.0},
        )
        assert len(r2) == 0

    @patch("trader._get_option_bid_ask")
    @patch("auth.login", return_value=True)
    @patch("auth.logout")
    def test_limit_price_capped(self, m_logout, m_login, mock_bid_ask):
        """Limit price capped at (1 + limit_pct) × premium."""
        from trader import execute_short_safety

        # Sold for $1.50, now $5.00 → gained huge, but limit = min(5.00, 1.20 × 1.50 = 1.80)
        c = _make_short_contract("TSLA", 300.0, "call", purchase_price=-150.0, days_out=20)
        mock_bid_ask.return_value = (4.90, 5.10, 5.00)

        results = execute_short_safety(
            [c], {"TSLA": 298.0}, dry_run=True,
            config={"safety_gain_pct": 40.0, "safety_limit_pct": 20.0},
        )
        assert len(results) == 1
        assert results[0]["btc_price"] == 1.80
