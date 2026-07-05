"""
test_insurance_optimization.py — Tests for insurance PDS optimization modes:
  _fetch_and_pair_debit_spreads()
  _place_pds_close_order()
  execute_insurance_mode("optimize" | "safety" | "rescue" | "cashout")

All Robinhood and auth calls are mocked.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def _future_date(days: int = 30) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _make_rh_position(
    chain_symbol="AAPL",
    quantity="1",
    option_url="https://api.robinhood.com/options/instruments/opt-001/",
    average_price="500.00",
    trade_value_multiplier="1",
):
    return {
        "chain_symbol": chain_symbol,
        "quantity": quantity,
        "option": option_url,
        "average_price": average_price,
        "trade_value_multiplier": trade_value_multiplier,
    }


def _make_rh_instrument(
    option_id="opt-001",
    option_type="put",
    strike_price="200.00",
    expiration_date="2026-08-15",
    chain_symbol="AAPL",
):
    return {
        "id": option_id,
        "type": option_type,
        "strike_price": strike_price,
        "expiration_date": expiration_date,
        "chain_symbol": chain_symbol,
    }


def _make_market_data(bid_price="3.00", ask_price="3.50", mark_price="3.25"):
    return {
        "bid_price": bid_price,
        "ask_price": ask_price,
        "mark_price": mark_price,
    }


# ─────────────────────────────────────────────────────────────────────────────
# _fetch_and_pair_debit_spreads tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchAndPairDebitSpreads:

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_basic_pds_pairing(self, mock_positions, mock_request_get, mock_market):
        """A PDS with long put at higher strike + short put at lower strike pairs correctly."""
        from trader import _fetch_and_pair_debit_spreads

        exp = _future_date(60)
        mock_positions.return_value = [
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-long/",
                              average_price="500.00", trade_value_multiplier="1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-short/",
                              average_price="-200.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "opt-long" in url:
                return _make_rh_instrument("opt-long", "put", "200.00", exp, "AAPL")
            if "opt-short" in url:
                return _make_rh_instrument("opt-short", "put", "180.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data("3.00", "3.50", "3.25")]

        pairs = _fetch_and_pair_debit_spreads()
        assert len(pairs) == 1
        p = pairs[0]
        assert p["symbol"] == "AAPL"
        assert p["long_strike"] == 200.0
        assert p["short_strike"] == 180.0
        assert p["width"] == 20.0
        # orig_debit = (500 - 200) / 100 = 3.00
        assert p["orig_debit"] == 3.0

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_filters_non_put_options(self, mock_positions, mock_request_get, mock_market):
        """Call options are excluded from PDS pairing."""
        from trader import _fetch_and_pair_debit_spreads

        exp = _future_date(30)
        mock_positions.return_value = [
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-long/",
                              average_price="500.00", trade_value_multiplier="1"),
        ]

        def instrument_lookup(url):
            return _make_rh_instrument("opt-long", "call", "200.00", exp, "AAPL")

        mock_request_get.side_effect = instrument_lookup
        pairs = _fetch_and_pair_debit_spreads()
        assert len(pairs) == 0

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_filter_by_symbol(self, mock_positions, mock_request_get, mock_market):
        """filter_sym restricts results to matching symbol."""
        from trader import _fetch_and_pair_debit_spreads

        exp = _future_date(60)
        mock_positions.return_value = [
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-long-aapl/",
                              average_price="500.00", trade_value_multiplier="1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-short-aapl/",
                              average_price="-200.00", trade_value_multiplier="-1"),
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/opt-long-tsla/",
                              average_price="400.00", trade_value_multiplier="1"),
            _make_rh_position("TSLA", "1",
                              "https://api.robinhood.com/options/instruments/opt-short-tsla/",
                              average_price="-100.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "opt-long-aapl" in url:
                return _make_rh_instrument("opt-long-aapl", "put", "200.00", exp, "AAPL")
            if "opt-short-aapl" in url:
                return _make_rh_instrument("opt-short-aapl", "put", "180.00", exp, "AAPL")
            if "opt-long-tsla" in url:
                return _make_rh_instrument("opt-long-tsla", "put", "250.00", exp, "TSLA")
            if "opt-short-tsla" in url:
                return _make_rh_instrument("opt-short-tsla", "put", "230.00", exp, "TSLA")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_debit_spreads(filter_sym="AAPL")
        assert len(pairs) == 1
        assert pairs[0]["symbol"] == "AAPL"

    @patch("robin_stocks.robinhood.options.get_option_market_data_by_id")
    @patch("robin_stocks.robinhood.helper.request_get")
    @patch("robin_stocks.robinhood.options.get_open_option_positions")
    def test_credit_direction_guard(self, mock_positions, mock_request_get, mock_market):
        """A long put with a closer credit-direction short is NOT paired as PDS."""
        from trader import _fetch_and_pair_debit_spreads

        exp = _future_date(30)
        # Positions: long put at $200, short put at $210 (PCS pair),
        # and short put at $180. The long at $200 has a closer credit match
        # at $210 (distance 10) vs debit match at $180 (distance 20).
        mock_positions.return_value = [
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-long-200/",
                              average_price="500.00", trade_value_multiplier="1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-short-210/",
                              average_price="-600.00", trade_value_multiplier="-1"),
            _make_rh_position("AAPL", "1",
                              "https://api.robinhood.com/options/instruments/opt-short-180/",
                              average_price="-200.00", trade_value_multiplier="-1"),
        ]

        def instrument_lookup(url):
            if "opt-long-200" in url:
                return _make_rh_instrument("opt-long-200", "put", "200.00", exp, "AAPL")
            if "opt-short-210" in url:
                return _make_rh_instrument("opt-short-210", "put", "210.00", exp, "AAPL")
            if "opt-short-180" in url:
                return _make_rh_instrument("opt-short-180", "put", "180.00", exp, "AAPL")
            return {}

        mock_request_get.side_effect = instrument_lookup
        mock_market.return_value = [_make_market_data()]

        pairs = _fetch_and_pair_debit_spreads()
        # The long at $200 should NOT pair with short at $180 because
        # the short at $210 is a closer credit-direction match
        assert len(pairs) == 0


# ─────────────────────────────────────────────────────────────────────────────
# execute_insurance_mode tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_pds_pair(
    symbol="AAPL",
    long_strike=200.0,
    short_strike=180.0,
    orig_debit=3.0,
    close_credit=1.5,
    dte_days=25,
    qty=1,
):
    exp = (date.today() + timedelta(days=dte_days)).isoformat()
    return {
        "symbol": symbol,
        "expiration": exp,
        "qty": qty,
        "long_strike": long_strike,
        "short_strike": short_strike,
        "width": long_strike - short_strike,
        "long_option_id": "opt-long",
        "short_option_id": "opt-short",
        "long_inst_url": "https://api.robinhood.com/options/instruments/opt-long/",
        "short_inst_url": "https://api.robinhood.com/options/instruments/opt-short/",
        "orig_debit": orig_debit,
        "long_mark": close_credit + 0.5,
        "short_mark": 0.5,
        "close_credit": close_credit,
    }


class TestExecuteInsuranceMode:

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_optimize_triggers_on_rally(self, mock_login, mock_logout, mock_fetch):
        """Optimize triggers when stock > 115% of long strike."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, orig_debit=3.0,
                              close_credit=1.0, dte_days=60)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["235.00"]):
            actions = execute_insurance_mode("optimize", dry_run=True, config={})

        assert len(actions) == 1
        assert actions[0]["mode"] == "optimize"
        assert actions[0]["action"] == "roll_up"
        assert "115%" in actions[0]["trigger_reason"]

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_optimize_skips_when_below_threshold(self, mock_login, mock_logout, mock_fetch):
        """Optimize does NOT trigger when stock < 115% of long strike."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, dte_days=60)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["225.00"]):
            actions = execute_insurance_mode("optimize", dry_run=True, config={})

        assert len(actions) == 0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_safety_triggers_on_low_dte_otm(self, mock_login, mock_logout, mock_fetch):
        """Safety triggers when DTE <= 30 and stock >= long strike (OTM)."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0,
                              close_credit=0.50, dte_days=20)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["210.00"]):
            actions = execute_insurance_mode("safety", dry_run=True, config={})

        assert len(actions) == 1
        assert actions[0]["mode"] == "safety"
        assert actions[0]["action"] == "roll_out"

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_safety_skips_high_dte(self, mock_login, mock_logout, mock_fetch):
        """Safety does NOT trigger when DTE > 30."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, dte_days=45)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["210.00"]):
            actions = execute_insurance_mode("safety", dry_run=True, config={})

        assert len(actions) == 0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_safety_skips_itm(self, mock_login, mock_logout, mock_fetch):
        """Safety does NOT trigger when stock < long strike (ITM for puts)."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, dte_days=20)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["195.00"]):
            actions = execute_insurance_mode("safety", dry_run=True, config={})

        assert len(actions) == 0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_rescue_triggers_moderate_itm(self, mock_login, mock_logout, mock_fetch):
        """Rescue triggers when stock is between long strike and midpoint."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0,
                              close_credit=5.0, dte_days=20)
        mock_fetch.return_value = [pair]

        # midpoint = 200 - (20 * 0.5) = 190. Stock 195 is between 190 and 200.
        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["195.00"]):
            actions = execute_insurance_mode("rescue", dry_run=True, config={})

        assert len(actions) == 1
        assert actions[0]["mode"] == "rescue"
        assert actions[0]["action"] == "harvest"

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_rescue_skips_deep_itm(self, mock_login, mock_logout, mock_fetch):
        """Rescue does NOT trigger when stock is below midpoint (that's cashout territory)."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, dte_days=20)
        mock_fetch.return_value = [pair]

        # midpoint = 200 - (20 * 0.5) = 190. Stock 185 is below midpoint.
        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["185.00"]):
            actions = execute_insurance_mode("rescue", dry_run=True, config={})

        assert len(actions) == 0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_rescue_harvest_floor_decay(self, mock_login, mock_logout, mock_fetch):
        """Rescue harvest floor decays with DTE: 75% at DTE=30, 0% at DTE=5."""
        from trader import execute_insurance_mode

        # At DTE=5, decay = max(0, (5-5))/(30-5) = 0, so harvest_floor = 0
        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0,
                              close_credit=0.10, dte_days=5)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["195.00"]):
            actions = execute_insurance_mode("rescue", dry_run=True, config={})

        assert len(actions) == 1

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_cashout_triggers_deep_itm(self, mock_login, mock_logout, mock_fetch):
        """Cashout triggers when stock is below midpoint of spread."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0,
                              close_credit=12.0, dte_days=15)
        mock_fetch.return_value = [pair]

        # midpoint = 200 - (20 * 0.5) = 190. Stock 185 < 190.
        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["185.00"]):
            actions = execute_insurance_mode("cashout", dry_run=True, config={})

        assert len(actions) == 1
        assert actions[0]["mode"] == "cashout"
        assert actions[0]["action"] == "cashout"
        # close_limit should be >= cashout_limit_pct * width = 0.70 * 20 = 14.0
        assert actions[0]["close_limit"] >= 14.0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_cashout_skips_above_midpoint(self, mock_login, mock_logout, mock_fetch):
        """Cashout does NOT trigger when stock is above midpoint."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0, dte_days=15)
        mock_fetch.return_value = [pair]

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["195.00"]):
            actions = execute_insurance_mode("cashout", dry_run=True, config={})

        assert len(actions) == 0

    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_no_positions_returns_empty(self, mock_login, mock_logout, mock_fetch):
        """Empty positions list returns empty actions."""
        from trader import execute_insurance_mode

        mock_fetch.return_value = []
        actions = execute_insurance_mode("optimize", dry_run=True, config={})
        assert actions == []


# ─────────────────────────────────────────────────────────────────────────────
# Max contract debit guard tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMaxContractDebitGuard:

    @patch("trader._place_pds_close_order")
    @patch("trader._cancel_spread_orders", return_value=0)
    @patch("trader._fetch_and_pair_debit_spreads")
    @patch("auth.logout")
    @patch("auth.login", return_value=True)
    def test_reopen_blocked_by_max_debit(self, mock_login, mock_logout, mock_fetch,
                                          mock_cancel, mock_close):
        """Reopen is blocked when new PDS would exceed max_contract_debit."""
        from trader import execute_insurance_mode

        pair = _make_pds_pair(long_strike=200.0, short_strike=180.0,
                              orig_debit=3.0, close_credit=1.0, dte_days=60)
        mock_fetch.return_value = [pair]
        mock_close.return_value = {"id": "order-123", "state": "confirmed"}

        cfg = {
            "insurance_max_contract_debit": 100,  # Very low limit
            "insurance_ratchet_gain_trigger_pct": 0.15,
            "insurance_ratchet_net_limit_pct": 0.20,
        }

        with patch("robin_stocks.robinhood.stocks.get_latest_price", return_value=["235.00"]):
            actions = execute_insurance_mode("optimize", dry_run=False, config=cfg)

        assert len(actions) == 1
        assert "exceeds" in actions[0]["reopen_blocked"]
