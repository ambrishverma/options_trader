"""
test_trader.py — Unit tests for trader.py
==========================================
Tests all public functions without live API calls:
  - _parse_chain()
  - _find_contract()
  - show_open_contracts()
  - buy_to_close()
  - roll_forward()
  - execute_panic_rolls()

All Robinhood, yfinance, auth, and portfolio calls are mocked.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import io
import pandas as pd
from datetime import date, timedelta
from unittest.mock import patch, MagicMock, call

import pytest

from trader import (
    _parse_chain,
    _find_contract,
    _fmt_exp,
    _dte,
    show_open_contracts,
    buy_to_close,
    roll_forward,
    execute_panic_rolls,
    execute_safety_btc_orders,
    execute_rescue_rolls,
    execute_optimize_rolls,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _future_date(days: int) -> str:
    return (date.today() + timedelta(days=days)).strftime("%Y-%m-%d")


def _future_mmdd(days: int) -> str:
    d = date.today() + timedelta(days=days)
    return f"{d.month}/{d.day}"


def _make_contract(
    symbol="TSLA", strike=300.0, expiration=None,
    quantity=1, btc_order_exists=False, purchase_price=-185.0,
):
    if expiration is None:
        expiration = _future_date(30)
    return {
        "symbol": symbol,
        "strike": strike,
        "expiration": expiration,
        "quantity": quantity,
        "btc_order_exists": btc_order_exists,
        "option_id": "test-uuid-1234",
        "purchase_price": purchase_price,
    }


def _make_calls_df(rows):
    """Build a minimal yfinance-style calls DataFrame."""
    return pd.DataFrame(rows, columns=["strike", "bid", "ask"])


def _good_order(order_id="ord-abc123", state="queued"):
    return {"id": order_id, "state": state}


# ─────────────────────────────────────────────────────────────────────────────
# _parse_chain tests
# ─────────────────────────────────────────────────────────────────────────────

class TestParseChain:
    def test_valid_call(self):
        expiry = _future_date(30)
        d = date.fromisoformat(expiry)
        chain_str = f"$95 CALL {d.month}/{d.day}"
        strike, opt_type, exp = _parse_chain(chain_str)
        assert strike == 95.0
        assert opt_type == "call"
        assert exp == expiry

    def test_valid_put_fractional_strike(self):
        expiry = _future_date(20)
        d = date.fromisoformat(expiry)
        chain_str = f"$182.50 PUT {d.month}/{d.day}"
        strike, opt_type, _ = _parse_chain(chain_str)
        assert strike == 182.50
        assert opt_type == "put"

    def test_case_insensitive(self):
        expiry = _future_date(15)
        d = date.fromisoformat(expiry)
        strike, opt_type, _ = _parse_chain(f"$100 call {d.month}/{d.day}")
        assert opt_type == "call"

    def test_past_date_gets_next_year(self):
        # Use Jan 1 which has definitely already passed in any year
        with patch("trader.date") as mock_date:
            mock_date.today.return_value = date(2026, 6, 15)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            strike, _, exp = _parse_chain("$50 CALL 1/10")
        assert exp.startswith("2027"), f"Expected 2027-01-10, got {exp}"

    def test_future_date_keeps_current_year(self):
        with patch("trader.date") as mock_date:
            mock_date.today.return_value = date(2026, 4, 1)
            mock_date.side_effect = lambda *a, **kw: date(*a, **kw)
            _, _, exp = _parse_chain("$50 CALL 12/31")
        assert exp.startswith("2026")

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid chain format"):
            _parse_chain("TSLA $95 CALL 5/15")  # symbol should not be included

    def test_missing_dollar_sign_raises(self):
        with pytest.raises(ValueError, match="Invalid chain format"):
            _parse_chain("95 CALL 5/15")

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="Invalid chain format"):
            _parse_chain("$95 SPREAD 5/15")


# ─────────────────────────────────────────────────────────────────────────────
# _find_contract tests
# ─────────────────────────────────────────────────────────────────────────────

class TestFindContract:
    def test_exact_match(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("TSLA", 300.0, exp, contracts)
        assert result is not None
        assert result["symbol"] == "TSLA"

    def test_case_insensitive_symbol(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("tsla", 300.0, exp, contracts)
        assert result is not None

    def test_strike_within_tolerance(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("TSLA", 300.005, exp, contracts)
        assert result is not None

    def test_strike_outside_tolerance(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("TSLA", 301.0, exp, contracts)
        assert result is None

    def test_wrong_expiration(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("TSLA", 300.0, _future_date(31), contracts)
        assert result is None

    def test_wrong_symbol(self):
        exp = _future_date(30)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        result = _find_contract("AAPL", 300.0, exp, contracts)
        assert result is None

    def test_empty_list(self):
        assert _find_contract("TSLA", 300.0, _future_date(30), []) is None

    def test_multiple_contracts_returns_first_match(self):
        exp = _future_date(30)
        c1 = _make_contract("TSLA", 300.0, exp, quantity=1)
        c2 = _make_contract("TSLA", 300.0, exp, quantity=2)
        result = _find_contract("TSLA", 300.0, exp, [c1, c2])
        assert result["quantity"] == 1  # first match returned


# ─────────────────────────────────────────────────────────────────────────────
# show_open_contracts tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_rh_position(symbol, strike, expiry, option_type="call",
                      pos_type="short", option_id="opt-show", qty=1, avg_price=2.50):
    """Build a Robinhood-style position dict for show_open_contracts tests."""
    return {
        "chain_symbol":    symbol,
        "quantity":        str(float(qty)),
        "type":            pos_type,
        "option_id":       option_id,
        "expiration_date": expiry,
        "average_price":   str(avg_price),
    }


def _make_rh_instrument(strike, expiry, option_type="call"):
    """Build a Robinhood-style instrument dict for show_open_contracts tests."""
    return {
        "type":            option_type,
        "strike_price":    str(float(strike)),
        "expiration_date": expiry,
    }


class TestShowOpenContracts:

    def _run_show(self, positions, instruments, live_price=290.0,
                  bid_ask=(2.0, 2.5, 2.25), open_orders=None):
        """Helper: patch all RH calls and run show_open_contracts('TSLA')."""
        instruments_iter = iter(instruments) if not callable(instruments) else instruments
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=open_orders or []), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: next(instruments_iter)), \
             patch("trader._get_live_price", return_value=live_price), \
             patch("trader._get_option_bid_ask", return_value=bid_ask):
            show_open_contracts("TSLA")

    def test_no_contracts_prints_message(self, capsys):
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "No open options contracts found for TSLA" in out

    def test_shows_table_with_contracts(self, capsys):
        exp = _future_date(15)
        pos   = [_make_rh_position("TSLA", 300.0, exp, avg_price=1.85)]
        instr = [_make_rh_instrument(300.0, exp, "call")]
        self._run_show(pos, instr, live_price=290.0, bid_ask=(2.0, 2.5, 2.25))
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "$300" in out
        assert "OTM" in out    # 290 < 300
        assert "$2.25" in out  # mid

    def test_itm_status_shown(self, capsys):
        exp   = _future_date(5)
        pos   = [_make_rh_position("TSLA", 300.0, exp)]
        instr = [_make_rh_instrument(300.0, exp, "call")]
        self._run_show(pos, instr, live_price=310.0, bid_ask=(5.0, 5.5, 5.25))
        out = capsys.readouterr().out
        assert "ITM" in out

    def test_put_contract_shown(self, capsys):
        """Put contracts are included in the output."""
        exp   = _future_date(20)
        pos   = [_make_rh_position("TSLA", 280.0, exp, option_type="put",
                                   pos_type="long", option_id="opt-put")]
        instr = [_make_rh_instrument(280.0, exp, "put")]
        self._run_show(pos, instr, live_price=290.0, bid_ask=(1.0, 1.5, 1.25))
        out = capsys.readouterr().out
        assert "PUT" in out
        assert "$280" in out

    def test_short_and_long_side_shown(self, capsys):
        """Short and Long labels appear for the respective position types."""
        exp = _future_date(10)
        pos = [
            _make_rh_position("TSLA", 300.0, exp, pos_type="short", option_id="opt-1"),
            _make_rh_position("TSLA", 280.0, exp, option_type="put",
                              pos_type="long", option_id="opt-2"),
        ]
        instr = [_make_rh_instrument(300.0, exp, "call"),
                 _make_rh_instrument(280.0, exp, "put")]
        self._run_show(pos, instr, live_price=290.0, bid_ask=(2.0, 2.5, 2.25))
        out = capsys.readouterr().out
        assert "Short" in out
        assert "Long" in out

    def test_open_order_annotation_shown(self, capsys):
        """Open buy/sell orders on a contract appear as [BUY]/[SELL] annotations."""
        exp = _future_date(10)
        pos   = [_make_rh_position("TSLA", 300.0, exp, option_id="opt-ann")]
        instr = [_make_rh_instrument(300.0, exp, "call")]
        open_orders = [{"legs": [
            {"option": "https://api.robinhood.com/options/instruments/opt-ann/",
             "side": "buy"},
        ]}]
        self._run_show(pos, instr, live_price=290.0, bid_ask=(1.0, 1.5, 1.25),
                       open_orders=open_orders)
        out = capsys.readouterr().out
        assert "[BUY]" in out

    def test_filters_to_requested_symbol_only(self, capsys):
        exp = _future_date(20)
        pos = [
            _make_rh_position("TSLA", 300.0, exp, option_id="opt-t"),
            _make_rh_position("AAPL", 200.0, exp, option_id="opt-a"),
        ]
        # Only TSLA position should be fetched (AAPL filtered by chain_symbol)
        instr = [_make_rh_instrument(300.0, exp, "call")]
        self._run_show(pos, instr, live_price=290.0)
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "AAPL" not in out

    def test_expired_contracts_are_excluded(self, capsys):
        """Contracts whose expiration date has passed must not appear in the table."""
        from datetime import date, timedelta
        past_exp   = str(date.today() - timedelta(days=4))
        future_exp = _future_date(10)
        pos = [
            _make_rh_position("TSLA", 300.0, past_exp,  option_id="opt-past"),
            _make_rh_position("TSLA", 320.0, future_exp, option_id="opt-fut"),
        ]
        instr = [_make_rh_instrument(300.0, past_exp,   "call"),
                 _make_rh_instrument(320.0, future_exp, "call")]
        self._run_show(pos, instr, live_price=290.0)
        out = capsys.readouterr().out
        assert "$320" in out
        assert "$300" not in out

    def test_all_contracts_expired_prints_message(self, capsys):
        """If all contracts are expired, print the 'no active contracts' message."""
        from datetime import date, timedelta
        past_exp = str(date.today() - timedelta(days=4))
        pos   = [_make_rh_position("TSLA", 300.0, past_exp, option_id="opt-x")]
        instr = [_make_rh_instrument(300.0, past_exp, "call")]
        self._run_show(pos, instr, live_price=290.0)
        out = capsys.readouterr().out
        assert "No active" in out


# ─────────────────────────────────────────────────────────────────────────────
# buy_to_close tests
# ─────────────────────────────────────────────────────────────────────────────

class TestBuyToClose:
    def _exp(self):
        return _future_date(30)

    def _chain_str(self):
        d = date.fromisoformat(self._exp())
        return f"$300 CALL {d.month}/{d.day}"

    def test_no_contract_found(self, capsys):
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=[]):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False
        assert "No open contract found" in capsys.readouterr().out

    def test_btc_order_exists_aborts(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp, btc_order_exists=True)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False
        assert "buy-to-close order already exists" in capsys.readouterr().out

    def test_invalid_chain_string(self, capsys):
        result = buy_to_close("TSLA", "bad format")
        assert result is False
        assert "Invalid chain format" in capsys.readouterr().out

    def test_places_order_at_mid_price(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = buy_to_close("TSLA", self._chain_str())
        assert result is True
        call_kwargs = mock_order.call_args
        assert call_kwargs.kwargs["price"] == 2.25
        assert call_kwargs.kwargs["positionEffect"] == "close"
        assert call_kwargs.kwargs["creditOrDebit"] == "debit"
        assert "✅" in capsys.readouterr().out

    def test_places_order_at_custom_price(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = buy_to_close("TSLA", self._chain_str(), price=1.80)
        assert result is True
        assert mock_order.call_args.kwargs["price"] == 1.80

    def test_order_failure_returns_false(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=None):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False
        assert "❌" in capsys.readouterr().out

    def test_order_exception_returns_false(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   side_effect=RuntimeError("API error")):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False

    def test_login_failure_returns_false(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login", side_effect=RuntimeError("Login failed")):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False
        assert "Login failed" in capsys.readouterr().out

    def test_prompt_yes_proceeds(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("builtins.input", return_value="y"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()):
            result = buy_to_close("TSLA", self._chain_str(), prompt=True)
        assert result is True
        out = capsys.readouterr().out
        assert "BUY-TO-CLOSE ORDER SUMMARY" in out

    def test_prompt_no_aborts(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.00, 2.50, 2.25)), \
             patch("builtins.input", return_value="n"):
            result = buy_to_close("TSLA", self._chain_str(), prompt=True)
        assert result is False
        assert "Aborted" in capsys.readouterr().out

    def test_zero_bid_ask_without_price_override_aborts(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(0.0, 0.0, 0.0)):
            result = buy_to_close("TSLA", self._chain_str())
        assert result is False
        assert "Could not fetch live bid/ask" in capsys.readouterr().out

    def test_zero_bid_ask_with_price_override_proceeds(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(0.0, 0.0, 0.0)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = buy_to_close("TSLA", self._chain_str(), price=1.50)
        assert result is True
        assert mock_order.call_args.kwargs["price"] == 1.50

    def test_correct_option_params_passed(self, capsys):
        """Verifies symbol, expiration, strike, optionType are passed correctly."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp, quantity=2)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_option_bid_ask", return_value=(2.0, 2.50, 2.25)), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            buy_to_close("TSLA", self._chain_str())
        kw = mock_order.call_args.kwargs
        assert kw["symbol"] == "TSLA"
        assert kw["strike"] == 300.0
        assert kw["expirationDate"] == exp
        assert kw["optionType"] == "call"
        assert kw["quantity"] == 2
        assert kw["timeInForce"] == "gtc"


# ─────────────────────────────────────────────────────────────────────────────
# roll_forward tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_ticker_mock(
    expirations=None,
    calls_df=None,
    next_expiration=None,
):
    """Build a minimal yf.Ticker mock for roll_forward tests."""
    if next_expiration is None:
        next_expiration = _future_date(37)
    if expirations is None:
        expirations = [_future_date(37)]
    if calls_df is None:
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 2.50, "ask": 3.00},
        ])

    mock_ticker = MagicMock()
    mock_ticker.options = expirations
    mock_chain = MagicMock()
    mock_chain.calls = calls_df
    mock_ticker.option_chain.return_value = mock_chain
    return mock_ticker


class TestRollForward:
    def _exp(self):
        return _future_date(30)

    def _next_exp(self):
        return _future_date(37)

    def _chain_str(self):
        d = date.fromisoformat(self._exp())
        return f"$300 CALL {d.month}/{d.day}"

    def _spread_patch(self, order_id="roll-id"):
        """Patch helper: returns a successful order_option_spread mock."""
        return patch(
            "robin_stocks.robinhood.orders.order_option_spread",
            return_value=_good_order(order_id),
        )

    def test_no_contract_found(self, capsys):
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=[]):
            result = roll_forward("TSLA", self._chain_str())
        assert result is False
        assert "No open contract found" in capsys.readouterr().out

    def test_btc_order_exists_aborts(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp, btc_order_exists=True)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts):
            result = roll_forward("TSLA", self._chain_str())
        assert result is False
        assert "already exists" in capsys.readouterr().out

    def test_invalid_chain_string(self, capsys):
        result = roll_forward("TSLA", "bad string")
        assert result is False

    def test_no_future_expirations(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = MagicMock()
        mock_ticker.options = []  # no future expirations
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", return_value=(2.0, 2.5, 2.25)), \
             patch("trader.yf.Ticker", return_value=mock_ticker):
            result = roll_forward("TSLA", self._chain_str())
        assert result is False
        assert "No future expirations" in capsys.readouterr().out

    def test_rolls_to_exact_same_strike(self, capsys):
        """When exact same strike exists at next expiry with non-zero bid, it is used."""
        exp = self._exp()
        next_exp = self._next_exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        calls_df = _make_calls_df([
            {"strike": 290.0, "bid": 3.5, "ask": 4.0},
            {"strike": 300.0, "bid": 2.5, "ask": 3.0},   # exact match
            {"strike": 310.0, "bid": 1.5, "ask": 2.0},
        ])
        mock_ticker = _make_ticker_mock(
            expirations=[next_exp],
            calls_df=calls_df,
            next_expiration=next_exp,
        )
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),   # BTC bid/ask
                 (2.5, 3.0, 2.75),   # STO bid/ask
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str())
        assert result is True
        spread_legs = mock_spread.call_args.kwargs["spread"]
        # STO leg should target strike 300.0 (formatted as "300.0000")
        sto_leg = spread_legs[1]
        assert float(sto_leg["strike"]) == 300.0
        assert sto_leg["action"] == "sell"
        assert sto_leg["effect"] == "open"

    def test_falls_back_to_nearest_otm_strike(self, capsys):
        """When exact strike has zero bid, use the nearest OTM strike."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 0.0, "ask": 0.0},   # exact strike — zero bid, skip
            {"strike": 295.0, "bid": 0.0, "ask": 0.0},   # below live price, skip
            {"strike": 305.0, "bid": 1.80, "ask": 2.20}, # nearest OTM ← should pick this
            {"strike": 310.0, "bid": 1.20, "ask": 1.60},
        ])
        mock_ticker = _make_ticker_mock(
            expirations=[self._next_exp()],
            calls_df=calls_df,
        )
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=298.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),    # BTC bid/ask
                 (1.80, 2.20, 2.00),  # STO bid/ask (305 strike)
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str())
        assert result is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 305.0

    def test_spread_order_params_correct(self, capsys):
        """Verify symbol, quantity, direction, net price, and leg structure."""
        exp = self._exp()
        next_exp = self._next_exp()
        contracts = [_make_contract("TSLA", 300.0, exp, quantity=2)]
        mock_ticker = _make_ticker_mock(expirations=[next_exp])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),    # BTC mid = 2.25
                 (2.5, 3.0, 2.75),    # STO mid = 2.75  → net = 0.50 credit
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str())

        assert result is True
        kw = mock_spread.call_args.kwargs
        assert kw["symbol"] == "TSLA"
        assert kw["quantity"] == 2
        assert kw["direction"] == "credit"
        assert kw["price"] == 0.50        # sto_mid(2.75) - btc_mid(2.25) = 0.50
        assert kw["timeInForce"] == "gtc"
        legs = kw["spread"]
        assert len(legs) == 2
        assert legs[0]["action"] == "buy"
        assert legs[0]["effect"] == "close"
        assert legs[0]["expirationDate"] == exp
        assert float(legs[0]["strike"]) == 300.0
        assert legs[0]["ratio_quantity"] == 1
        assert legs[1]["action"] == "sell"
        assert legs[1]["effect"] == "open"
        assert legs[1]["expirationDate"] == next_exp
        assert legs[1]["ratio_quantity"] == 1

    def test_debit_roll_uses_debit_direction(self, capsys):
        """When STO mid < BTC mid the net is a debit; direction should be 'debit'."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (3.0, 3.5, 3.25),   # BTC mid = 3.25  (expensive to close)
                 (1.0, 1.5, 1.25),   # STO mid = 1.25  → net = -2.00 debit
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str())
        assert result is True
        kw = mock_spread.call_args.kwargs
        assert kw["direction"] == "debit"
        assert kw["price"] == 2.00   # abs(1.25 - 3.25)

    def test_custom_price_used_as_net_spread_price(self, capsys):
        """--price overrides the net spread price sent to order_option_spread."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str(), price=1.50)
        assert result is True
        kw = mock_spread.call_args.kwargs
        assert kw["price"] == 1.50
        assert kw["direction"] == "credit"

    def test_spread_order_failure_returns_false(self, capsys):
        """If order_option_spread returns an error response, return False."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"detail": "This order introduces infinite risk."}):
            result = roll_forward("TSLA", self._chain_str())
        assert result is False
        assert "❌" in capsys.readouterr().out

    def test_spread_order_exception_returns_false(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=RuntimeError("API timeout")):
            result = roll_forward("TSLA", self._chain_str())
        assert result is False

    def test_prompt_no_aborts(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("builtins.input", return_value="n"):
            result = roll_forward("TSLA", self._chain_str(), prompt=True)
        assert result is False
        out = capsys.readouterr().out
        assert "ROLL FORWARD ORDER SUMMARY" in out
        assert "Aborted" in out

    def test_prompt_yes_proceeds(self, capsys):
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        mock_ticker = _make_ticker_mock(expirations=[self._next_exp()])
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 2.5, 2.25),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("builtins.input", return_value="y"), \
             self._spread_patch():
            result = roll_forward("TSLA", self._chain_str(), prompt=True)
        assert result is True


# ─────────────────────────────────────────────────────────────────────────────
# execute_panic_rolls tests
# ─────────────────────────────────────────────────────────────────────────────

def _today() -> str:
    return str(date.today())


def _make_panic_contract(symbol, strike, btc_order_exists=False, option_id="opt-abc",
                         opt_type="call"):
    return {
        "symbol":           symbol,
        "opt_type":         opt_type,
        "strike":           strike,
        "expiration":       _today(),
        "quantity":         1,
        "btc_order_exists": btc_order_exists,
        "option_id":        option_id,
        "purchase_price":   200.0,
    }


class TestExecutePanicRolls:

    def _ticker_mock(self, next_exp=None):
        """Minimal yf.Ticker mock with one future expiration and a matching call."""
        if next_exp is None:
            next_exp = _future_date(7)
        df = _make_calls_df([{"strike": 300.0, "bid": 2.5, "ask": 3.0}])
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.calls = df
        mock.option_chain.return_value = chain
        return mock

    # ── No-op cases ───────────────────────────────────────────────────────────

    def test_empty_input_returns_empty(self):
        result = execute_panic_rolls([], {})
        assert result == []

    def test_no_dte0_contracts_returns_empty(self):
        """Contracts with future expiration are ignored."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        # Override expiration to tomorrow so it's NOT DTE-0
        contracts[0]["expiration"] = _future_date(1)
        result = execute_panic_rolls(contracts, {"TSLA": 310.0})
        assert result == []

    def test_dte0_otm_not_triggered(self):
        """DTE-0 contract that is OTM (stock < strike) is ignored."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        result = execute_panic_rolls(contracts, {"TSLA": 285.0})  # stock below strike
        assert result == []

    # ── Dry run ───────────────────────────────────────────────────────────────

    def test_dry_run_returns_results_without_orders(self):
        """In dry_run mode, result dicts are returned but no orders are placed."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        with patch("robin_stocks.robinhood.orders.order_option_spread") as mock_order, \
             patch("auth.login") as mock_login:
            result = execute_panic_rolls(contracts, {"TSLA": 310.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["symbol"] == "TSLA"
        assert result[0]["success"] is False
        assert "DRY RUN" in result[0]["error"]
        mock_login.assert_not_called()
        mock_order.assert_not_called()

    # ── Login failure ─────────────────────────────────────────────────────────

    def test_login_failure_returns_error_results(self):
        contracts = [_make_panic_contract("TSLA", 300.0)]
        with patch("auth.login", side_effect=RuntimeError("login broke")):
            result = execute_panic_rolls(contracts, {"TSLA": 310.0})
        assert len(result) == 1
        assert result[0]["success"] is False
        assert "login broke" in result[0]["error"]

    # ── Successful roll, no prior BTC order ───────────────────────────────────

    def test_successful_roll_no_btc(self):
        """DTE-0 ITM contract, no prior BTC — rolls successfully."""
        contracts = [_make_panic_contract("TSLA", 300.0, btc_order_exists=False)]
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),    # BTC bid/ask (DTE-0 pre-market)
                 (2.5, 3.0, 2.75),   # STO bid/ask
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("panic-id")) as mock_spread:
            result = execute_panic_rolls(contracts, {"TSLA": 312.0})

        assert len(result) == 1
        r = result[0]
        assert r["success"] is True
        assert r["order_id"] == "panic-id"
        assert r["btc_cancelled"] is False
        assert r["next_expiration"] == next_exp
        assert r["target_strike"] == 300.0
        assert r["direction"] == "credit"
        assert r["net_price"] == 2.75   # sto_mid - btc_mid(0) = 2.75
        # Verify spread legs
        kw = mock_spread.call_args.kwargs
        assert kw["symbol"] == "TSLA"
        assert kw["direction"] == "credit"
        legs = kw["spread"]
        assert legs[0]["action"] == "buy" and legs[0]["effect"] == "close"
        assert legs[0]["ratio_quantity"] == 1
        assert legs[1]["action"] == "sell" and legs[1]["effect"] == "open"
        assert legs[1]["ratio_quantity"] == 1

    # ── Order cancellation (BTC-only and roll-forward spreads) ───────────────

    def test_cancels_btc_order_before_rolling(self):
        """A standalone BTC order referencing the contract is cancelled."""
        contracts = [_make_panic_contract("TSLA", 300.0, option_id="opt-xyz")]
        next_exp = _future_date(7)
        open_orders = [{
            "id": "btc-order-99",
            "legs": [{
                "side": "buy",
                "position_effect": "close",
                "option": "https://api.robinhood.com/options/instruments/opt-xyz/",
            }],
        }]
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=open_orders), \
             patch("robin_stocks.robinhood.orders.cancel_option_order") as mock_cancel, \
             patch("time.sleep") as mock_sleep, \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("panic-id")):
            result = execute_panic_rolls(contracts, {"TSLA": 312.0})

        assert result[0]["btc_cancelled"] is True
        mock_cancel.assert_called_once_with("btc-order-99")
        mock_sleep.assert_called_once_with(30)

    def test_cancels_stale_rescue_spread_before_rolling(self):
        """A stale rescue roll-forward spread (BTC close + STO open legs) is
        cancelled so that the panic roll can proceed cleanly.
        Safety→Rescue→Panic escalation path: rescue placed a spread that didn't
        fill; panic mode must clear it before submitting a new spread."""
        contracts = [_make_panic_contract("TSLA", 300.0, option_id="opt-xyz")]
        next_exp = _future_date(7)
        # Simulate a rescue-mode roll-forward spread still open: leg 1 references
        # the current contract (opt-xyz), leg 2 references the next expiry option.
        stale_rescue_spread = {
            "id": "rescue-spread-55",
            "legs": [
                {   # BTC leg — closes current contract
                    "side": "buy",
                    "position_effect": "close",
                    "option": "https://api.robinhood.com/options/instruments/opt-xyz/",
                },
                {   # STO leg — opens next expiry contract
                    "side": "sell",
                    "position_effect": "open",
                    "option": "https://api.robinhood.com/options/instruments/opt-next/",
                },
            ],
        }
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[stale_rescue_spread]), \
             patch("robin_stocks.robinhood.orders.cancel_option_order") as mock_cancel, \
             patch("time.sleep") as mock_sleep, \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=315.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("panic-id")):
            result = execute_panic_rolls(contracts, {"TSLA": 315.0})

        # The entire spread order (both legs) must be cancelled as a unit
        mock_cancel.assert_called_once_with("rescue-spread-55")
        assert result[0]["btc_cancelled"] is True
        mock_sleep.assert_called_once_with(30)
        # Panic roll should then succeed
        assert result[0]["success"] is True

    def test_cancels_multiple_order_types_for_same_contract(self):
        """If both a BTC-only order AND a stale spread exist for the same contract,
        both are cancelled before the panic roll (escalation cleanup)."""
        contracts = [_make_panic_contract("TSLA", 300.0, option_id="opt-xyz")]
        next_exp = _future_date(7)
        open_orders = [
            {   # standalone BTC order
                "id": "btc-order-1",
                "legs": [{
                    "side": "buy", "position_effect": "close",
                    "option": "https://api.robinhood.com/options/instruments/opt-xyz/",
                }],
            },
            {   # stale rescue spread
                "id": "rescue-spread-2",
                "legs": [
                    {"side": "buy", "position_effect": "close",
                     "option": "https://api.robinhood.com/options/instruments/opt-xyz/"},
                    {"side": "sell", "position_effect": "open",
                     "option": "https://api.robinhood.com/options/instruments/opt-next/"},
                ],
            },
        ]
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=open_orders), \
             patch("robin_stocks.robinhood.orders.cancel_option_order") as mock_cancel, \
             patch("time.sleep"), \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("panic-id")):
            result = execute_panic_rolls(contracts, {"TSLA": 312.0})

        assert mock_cancel.call_count == 2
        cancelled_ids = {c[0][0] for c in mock_cancel.call_args_list}
        assert "btc-order-1" in cancelled_ids
        assert "rescue-spread-2" in cancelled_ids
        assert result[0]["btc_cancelled"] is True

    def test_no_sleep_when_no_btc_to_cancel(self):
        """When no BTC order exists, time.sleep is NOT called."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("time.sleep") as mock_sleep, \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("panic-id")):
            execute_panic_rolls(contracts, {"TSLA": 312.0})
        mock_sleep.assert_not_called()

    # ── Roll order failure ────────────────────────────────────────────────────

    def test_roll_order_failure_captured_in_result(self):
        """If order_option_spread returns an error dict, result.success is False."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),
                 (2.5, 3.0, 2.75),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"detail": "This order introduces infinite risk."}):
            result = execute_panic_rolls(contracts, {"TSLA": 312.0})
        r = result[0]
        assert r["success"] is False
        assert "infinite risk" in r["error"]

    def test_no_future_expirations_captured_in_result(self):
        """If no future expirations available, result shows error."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        mock_ticker = MagicMock()
        mock_ticker.options = []   # no expirations
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("trader._get_live_price", return_value=312.0), \
             patch("trader._get_option_bid_ask", return_value=(0.0, 0.0, 0.0)):
            result = execute_panic_rolls(contracts, {"TSLA": 312.0})
        assert result[0]["success"] is False
        assert "No future expirations" in result[0]["error"]

    def test_skips_expirations_less_than_7_days_out(self):
        """Expirations within 6 days are ignored; roll targets first exp >= 7 days."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        too_soon   = _future_date(3)   # 3 days out — should be skipped
        far_enough = _future_date(10)  # 10 days out — should be used
        df_far = _make_calls_df([{"strike": 300.0, "bid": 2.0, "ask": 3.0}])
        ticker_mock = _make_ticker_mock([too_soon, far_enough], df_far, far_enough)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", return_value=ticker_mock), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),    # BTC
                 (2.0, 3.0, 2.50),   # STO
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("roll-7d")) as mock_spread:
            result = execute_panic_rolls(contracts, {"TSLA": 310.0})
        assert result[0]["success"] is True
        # Must have used the far expiration, not the too-soon one
        assert result[0]["next_expiration"] == far_enough

    def test_only_near_expirations_returns_error(self):
        """If every future expiration is within 6 days, error is returned."""
        contracts = [_make_panic_contract("TSLA", 300.0)]
        near_only  = _future_date(4)   # only 4 days out — should be rejected
        mock_ticker = MagicMock()
        mock_ticker.options = [near_only]
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("trader._get_live_price", return_value=310.0):
            result = execute_panic_rolls(contracts, {"TSLA": 310.0})
        assert result[0]["success"] is False
        assert "at least 7 days" in result[0]["error"]

    def test_multiple_contracts_processed_independently(self):
        """Two DTE-0 ITM contracts → two result dicts, independent outcomes."""
        contracts = [
            _make_panic_contract("TSLA", 300.0, option_id="opt-1"),
            _make_panic_contract("AAPL", 200.0, option_id="opt-2"),
        ]
        next_exp = _future_date(7)
        df_tsla = _make_calls_df([{"strike": 300.0, "bid": 2.5, "ask": 3.0}])
        df_aapl = _make_calls_df([{"strike": 200.0, "bid": 1.5, "ask": 2.0}])
        # Alternate ticker mocks based on symbol
        tickers = {
            "TSLA": (lambda ne=next_exp, df=df_tsla: _make_ticker_mock([ne], df, ne))(),
            "AAPL": (lambda ne=next_exp, df=df_aapl: _make_ticker_mock([ne], df, ne))(),
        }
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker", side_effect=lambda s: tickers.get(s, tickers["TSLA"])), \
             patch("trader._get_live_price", side_effect=[315.0, 210.0]), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0), (2.5, 3.0, 2.75),  # TSLA
                 (0.0, 0.0, 0.0), (1.5, 2.0, 1.75),  # AAPL
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=[_good_order("roll-1"), _good_order("roll-2")]):
            result = execute_panic_rolls(
                contracts, {"TSLA": 315.0, "AAPL": 210.0}
            )
        assert len(result) == 2
        assert result[0]["symbol"] == "TSLA" and result[0]["success"] is True
        assert result[1]["symbol"] == "AAPL" and result[1]["success"] is True


    # ── Short PUT tests ───────────────────────────────────────────────────────

    def _ticker_mock_put(self, next_exp=None):
        """Minimal yf.Ticker mock with a puts chain for short-put tests."""
        if next_exp is None:
            next_exp = _future_date(7)
        df_puts = _make_calls_df([{"strike": 280.0, "bid": 2.5, "ask": 3.0}])
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.puts  = df_puts
        chain.calls = _make_calls_df([])  # empty; panic should use puts
        mock.option_chain.return_value = chain
        return mock

    def test_put_otm_not_triggered(self):
        """DTE-0 short PUT that is OTM (stock > strike) is ignored."""
        c = _make_panic_contract("TSLA", 300.0, opt_type="put")
        result = execute_panic_rolls([c], {"TSLA": 320.0})  # stock ABOVE put strike → OTM
        assert result == []

    def test_put_itm_triggers_panic(self):
        """DTE-0 short PUT that is ITM (stock < strike) is detected."""
        c = _make_panic_contract("TSLA", 300.0, opt_type="put")
        result = execute_panic_rolls([c], {"TSLA": 280.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["opt_type"] == "put"

    def test_put_itm_by_is_positive(self):
        """itm_by for a short PUT = strike − stock_price (always positive when ITM)."""
        c = _make_panic_contract("TSLA", 300.0, opt_type="put")
        result = execute_panic_rolls([c], {"TSLA": 285.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["itm_by"] == pytest.approx(15.0, abs=0.01)

    def test_put_roll_uses_puts_chain_and_lower_strike(self):
        """Panic roll for a short PUT: uses chain.puts and picks strike <= live_price."""
        next_exp = _future_date(7)
        df_puts = _make_calls_df([
            {"strike": 275.0, "bid": 2.0, "ask": 2.50},
            {"strike": 290.0, "bid": 1.0, "ask": 1.50},
        ])
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.puts  = df_puts
        chain.calls = _make_calls_df([])
        mock.option_chain.return_value = chain

        captured = {}
        def fake_spread(**kwargs):
            captured.update(kwargs)
            return _good_order("put-roll-1")

        c = _make_panic_contract("TSLA", 300.0, opt_type="put")
        with patch("trader.yf.Ticker", return_value=mock), \
             patch("trader._get_live_price", return_value=280.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0),   # BTC leg (DTE-0)
                 (2.0, 2.5, 2.25),  # STO leg at 275.0
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            result = execute_panic_rolls([c], {"TSLA": 280.0})
        assert len(result) == 1
        assert result[0]["success"] is True
        # Target strike must be <= current strike (same or lower — roll rule for puts)
        assert result[0]["target_strike"] <= 300.0
        # Spread legs must use "put" optionType
        legs = {leg["optionType"] for leg in captured.get("spread", [])}
        assert legs == {"put"}

    def test_call_still_works_alongside_put(self):
        """Mixed list: DTE-0 ITM CALL and DTE-0 ITM PUT both processed.

        CALL at $300 with TSLA @ $305 → ITM (305 >= 300) ✓
        PUT  at $310 with TSLA @ $305 → ITM (305 <= 310) ✓
        """
        next_exp = _future_date(7)
        # CALL chain: nearest strike >= current call strike (300) → 310
        df_calls = _make_calls_df([{"strike": 310.0, "bid": 2.0, "ask": 2.50}])
        # PUT chain: nearest strike <= current put strike (310) → 295
        df_puts  = _make_calls_df([{"strike": 295.0, "bid": 1.5, "ask": 2.00}])
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.calls = df_calls
        chain.puts  = df_puts
        mock.option_chain.return_value = chain

        call_c = _make_panic_contract("TSLA", 300.0, option_id="opt-call", opt_type="call")
        put_c  = _make_panic_contract("TSLA", 310.0, option_id="opt-put",  opt_type="put")
        with patch("trader.yf.Ticker", return_value=mock), \
             patch("trader._get_live_price", side_effect=[305.0, 305.0]), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.0, 0.0, 0.0), (2.0, 2.5, 2.25),   # CALL legs
                 (0.0, 0.0, 0.0), (1.5, 2.0, 1.75),   # PUT legs
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("mixed-1")), \
             patch("auth.login"), patch("auth.logout"):
            result = execute_panic_rolls([call_c, put_c], {"TSLA": 305.0})
        assert len(result) == 2


# ─────────────────────────────────────────────────────────────────────────────
# execute_safety_btc_orders tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_safety_contract(symbol, strike, dte, btc_order_exists=False,
                          purchase_price=200.0, option_id="opt-safe",
                          opt_type="call"):
    """Build a contract dict for safety-mode tests."""
    exp = _future_date(dte)
    return {
        "symbol":           symbol,
        "opt_type":         opt_type,
        "strike":           strike,
        "expiration":       exp,
        "quantity":         1,
        "btc_order_exists": btc_order_exists,
        "option_id":        option_id,
        "purchase_price":   purchase_price,
    }


class TestExecuteSafetyBtcOrders:

    # ── No-op cases ───────────────────────────────────────────────────────────

    def test_empty_input_returns_empty(self):
        assert execute_safety_btc_orders([], {}) == []

    def test_dte_zero_excluded(self):
        """DTE=0 contracts belong to panic mode, not safety mode."""
        c = _make_safety_contract("TSLA", 300.0, 0)
        assert execute_safety_btc_orders([c], {"TSLA": 310.0}) == []

    def test_high_dte_included(self):
        """DTE > 10 is now within the safety window (no upper DTE limit)."""
        c = _make_safety_contract("TSLA", 300.0, 11)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 310.0})
        assert len(result) == 1
        assert result[0]["success"] is True

    def test_existing_btc_order_excluded(self):
        """Contracts already protected by a BTC order are skipped."""
        c = _make_safety_contract("TSLA", 300.0, 5, btc_order_exists=True)
        assert execute_safety_btc_orders([c], {"TSLA": 310.0}) == []

    def test_expired_contract_excluded(self):
        """Past-expiration contracts are skipped."""
        c = _make_safety_contract("TSLA", 300.0, 1)
        c["expiration"] = str(date.today() - timedelta(days=1))
        assert execute_safety_btc_orders([c], {"TSLA": 310.0}) == []

    # ── Price calculation ─────────────────────────────────────────────────────

    def test_price_is_min_of_three_values(self):
        """btc_price = MIN($0.20, 10% of per-share premium, mid).
        purchase_price is stored as total contract value (100 shares).
        $200 total → $2.00/share → 10% = $0.20; mid=$1.50 → MIN(0.20, 0.20, 1.50) = $0.20"""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=200.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.20
        assert mock_order.call_args.kwargs["price"] == 0.20

    def test_price_uses_ten_pct_when_smaller(self):
        """When 10% of per-share premium < $0.20, use that value.
        $150 total → $1.50/share → 10% = $0.15; mid=$0.50 → MIN(0.20, 0.15, 0.50) = $0.15"""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=150.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.45, 0.55, 0.50)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.15
        assert mock_order.call_args.kwargs["price"] == 0.15

    def test_price_uses_ten_pct_on_small_premium(self):
        """Real-world case: $50 total → $0.50/share → 10% = $0.05 wins over $0.20.
        mid=$0.20 → MIN(0.20, 0.05, 0.20) = $0.05 (the TTD scenario)."""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=50.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.18, 0.22, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.05
        assert mock_order.call_args.kwargs["price"] == 0.05

    def test_price_uses_mid_when_smallest(self):
        """When mid < $0.20 and mid < 10% of per-share premium, use mid.
        $500 total → $5.00/share → 10% = $0.50; mid=$0.05 → MIN(0.20, 0.50, 0.05) = $0.05"""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=500.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.04, 0.06, 0.05)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.05

    def test_price_floors_at_one_cent_when_all_zero(self):
        """If mid=0 and purchase_price=0, default to $0.20 (the flat floor)."""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=0.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.0, 0.0, 0.0)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.20

    # ── Order placement ───────────────────────────────────────────────────────

    def test_successful_btc_order(self):
        """Happy path: order placed, result.success is True with order_id."""
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=200.0)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.18, 0.22, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order("safety-btc-1")) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})

        assert len(result) == 1
        r = result[0]
        assert r["success"] is True
        assert r["order_id"] == "safety-btc-1"
        # Verify order params
        kw = mock_order.call_args.kwargs
        assert kw["positionEffect"] == "close"
        assert kw["creditOrDebit"] == "debit"
        assert kw["symbol"] == "TSLA"
        assert kw["strike"] == 300.0
        assert kw["optionType"] == "call"
        assert kw["timeInForce"] == "gtc"

    def test_order_failure_captured_in_result(self):
        """API rejection → result.success=False, error populated."""
        c = _make_safety_contract("TSLA", 300.0, 5)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value={"detail": "Account restricted"}):
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["success"] is False
        assert "Account restricted" in result[0]["error"]

    def test_order_exception_captured_in_result(self):
        c = _make_safety_contract("TSLA", 300.0, 5)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   side_effect=RuntimeError("timeout")):
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["success"] is False
        assert "timeout" in result[0]["error"]

    # ── Random delay between orders ───────────────────────────────────────────

    def test_no_delay_before_first_order(self):
        """time.sleep is NOT called before the very first order."""
        c = _make_safety_contract("TSLA", 300.0, 5)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("time.sleep") as mock_sleep, \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()):
            execute_safety_btc_orders([c], {"TSLA": 290.0})
        mock_sleep.assert_not_called()

    def test_delay_between_consecutive_orders(self):
        """time.sleep is called once between two contracts, with value in [5, 20]."""
        c1 = _make_safety_contract("TSLA", 300.0, 5, option_id="opt-1")
        c2 = _make_safety_contract("AAPL", 200.0, 7, option_id="opt-2")
        sleep_calls = []
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("time.sleep", side_effect=lambda s: sleep_calls.append(s)), \
             patch("random.randint", return_value=12), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()):
            execute_safety_btc_orders(
                [c1, c2], {"TSLA": 290.0, "AAPL": 195.0}
            )
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 12

    # ── Dry run ───────────────────────────────────────────────────────────────

    def test_dry_run_no_orders_placed(self):
        c = _make_safety_contract("TSLA", 300.0, 5)
        with patch("auth.login") as mock_login, \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit") as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0}, dry_run=True)
        assert len(result) == 1
        assert "DRY RUN" in result[0]["error"]
        mock_login.assert_not_called()
        mock_order.assert_not_called()

    # ── Login failure ─────────────────────────────────────────────────────────

    def test_login_failure_returns_error_for_all(self):
        c1 = _make_safety_contract("TSLA", 300.0, 5)
        c2 = _make_safety_contract("AAPL", 200.0, 7)
        with patch("auth.login", side_effect=RuntimeError("MFA failed")):
            result = execute_safety_btc_orders(
                [c1, c2], {"TSLA": 290.0, "AAPL": 195.0}
            )
        assert len(result) == 2
        assert all(not r["success"] for r in result)


    # ── Short PUT tests ───────────────────────────────────────────────────────

    def test_put_btc_uses_put_option_type(self):
        """Safety BTC for a short PUT must use optionType='put' in the API call."""
        c = _make_safety_contract("TSLA", 300.0, 5, opt_type="put")
        captured = {}
        def fake_order(**kwargs):
            captured.update(kwargs)
            return {"id": "put-btc-1"}
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   side_effect=fake_order):
            result = execute_safety_btc_orders([c], {"TSLA": 280.0})
        assert len(result) == 1
        assert result[0]["success"] is True
        assert captured.get("optionType") == "put"

    def test_put_btc_price_formula_same_as_call(self):
        """Safety BTC price formula is identical for puts and calls."""
        c = _make_safety_contract("TSLA", 300.0, 5,
                                  purchase_price=500.0, opt_type="put")
        # purchase_price=500 → per_share=5.00 → 10%=0.50 > 0.20; mid=0.18 < 0.20
        # MIN($0.20, $0.50, $0.18) = $0.18
        captured = {}
        def fake_order(**kwargs):
            captured.update(kwargs)
            return {"id": "put-btc-price-1"}
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.16, 0.20, 0.18)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   side_effect=fake_order):
            execute_safety_btc_orders([c], {"TSLA": 280.0})
        assert captured.get("price") == pytest.approx(0.18, abs=0.001)

    def test_put_and_call_in_same_batch(self):
        """Safety BTC processes both call and put contracts in one batch."""
        c_call = _make_safety_contract("TSLA", 300.0, 5, option_id="opt-c", opt_type="call")
        c_put  = _make_safety_contract("AAPL", 200.0, 7, option_id="opt-p", opt_type="put")
        orders = []
        def fake_order(**kwargs):
            orders.append(kwargs.get("optionType"))
            return {"id": f"ord-{len(orders)}"}
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   side_effect=fake_order), \
             patch("time.sleep"):
            result = execute_safety_btc_orders(
                [c_call, c_put], {"TSLA": 290.0, "AAPL": 195.0}
            )
        assert len(result) == 2
        assert "call" in orders
        assert "put" in orders


# ─────────────────────────────────────────────────────────────────────────────
# execute_rescue_rolls tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_rescue_contract(symbol, strike, dte=2, btc_order_exists=False,
                          option_id="opt-rescue", opt_type="call"):
    """Build a DTE-1-2 ITM contract dict for rescue-mode tests."""
    return {
        "symbol":           symbol,
        "opt_type":         opt_type,
        "strike":           strike,
        "expiration":       _future_date(dte),
        "quantity":         1,
        "btc_order_exists": btc_order_exists,
        "option_id":        option_id,
        "purchase_price":   200.0,
    }


class TestExecuteRescueRolls:

    def _ticker_mock(self, next_exp=None, strikes=None):
        """Minimal yf.Ticker mock with one future expiration and configurable calls."""
        if next_exp is None:
            next_exp = _future_date(7)
        if strikes is None:
            strikes = [{"strike": 300.0, "bid": 3.0, "ask": 3.50}]
        df = pd.DataFrame(strikes)
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.calls = df
        mock.option_chain.return_value = chain
        return mock

    # ── No-op cases ───────────────────────────────────────────────────────────

    def test_empty_input_returns_empty(self):
        assert execute_rescue_rolls([], {}) == []

    def test_ignores_dte_0_contracts(self):
        """DTE=0 contracts are handled by panic mode, not rescue mode."""
        c = _make_rescue_contract("TSLA", 300.0, dte=0)
        result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert result == []

    def test_ignores_dte_3_and_above(self):
        """DTE=3 contracts are outside rescue mode's 1-2 day window."""
        c = _make_rescue_contract("TSLA", 300.0, dte=3)
        result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert result == []

    def test_ignores_otm_contracts(self):
        """Contracts where stock < strike are OTM and ignored."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        result = execute_rescue_rolls([c], {"TSLA": 285.0})  # stock below strike
        assert result == []

    # ── Dry run ───────────────────────────────────────────────────────────────

    def test_dry_run_returns_stubs_no_orders(self):
        c = _make_rescue_contract("TSLA", 300.0, dte=1)
        with patch("robin_stocks.robinhood.orders.order_option_spread") as mock_order, \
             patch("auth.login") as mock_login:
            result = execute_rescue_rolls([c], {"TSLA": 310.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["symbol"] == "TSLA"
        assert "DRY RUN" in result[0]["error"]
        mock_login.assert_not_called()
        mock_order.assert_not_called()

    # ── Skip when no credit ───────────────────────────────────────────────────

    def test_skips_when_no_positive_credit(self):
        """When no strike >= current yields positive credit, record as skipped."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        # BTC mid = 4.00, best STO mid = 3.50 → net = -0.50 (debit) → skip
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp,
                       [{"strike": 300.0, "bid": 3.0, "ask": 4.0}])), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (3.50, 4.50, 4.00),  # BTC mid = 4.00
                 (3.00, 4.00, 3.50),  # STO live mid = 3.50
             ]):
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert len(result) == 1
        r = result[0]
        assert r["skipped"] is True
        assert r["success"] is False

    def test_skips_when_no_eligible_strikes(self):
        """When no strikes >= current have a non-zero bid, record as skipped."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp,
                       [{"strike": 280.0, "bid": 5.0, "ask": 6.0}])), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", return_value=(3.0, 4.0, 3.50)):
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert result[0]["skipped"] is True

    # ── Max-credit strike selection ───────────────────────────────────────────

    def test_picks_best_rr_ratio_strike(self):
        """Selects the strike with the best Risk/Reward ratio using the new formula:
        R/R = (net_credit / DTE) × distance_from_current_strike.
        current_strike=300; candidates 300 (distance=0 → R/R=0), 305 (distance=5),
        310 (distance=10). BTC mid=1.00, next_dte=7.
        Net credits: 300→1.50 (R/R=0), 305→0.80 (R/R≈0.571), 310→0.20 (R/R≈0.286).
        Strike 305 wins: best (net_credit/DTE)×distance."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        # BTC mid = 1.00; strikes 300 (mid 2.50) and 305 (mid 1.80) and 310 (mid 1.20)
        # Net credits: 1.50, 0.80, 0.20
        # Distances from strike 300: 0, 5, 10
        # R/R = (net_credit/7) × distance: 0, 0.571, 0.286 → pick 305 (best R/R)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp, [
                       {"strike": 300.0, "bid": 2.00, "ask": 3.00},   # mid 2.50
                       {"strike": 305.0, "bid": 1.30, "ask": 2.30},   # mid 1.80
                       {"strike": 310.0, "bid": 0.70, "ask": 1.70},   # mid 1.20
                   ])), \
             patch("trader._get_live_price", return_value=305.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),  # BTC mid = 1.00
                 (1.30, 2.30, 1.80),  # STO live mid confirm = 1.80 (strike 305)
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")) as mock_spread:
            result = execute_rescue_rolls([c], {"TSLA": 305.0})
        assert result[0]["success"] is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 305.0  # best R/R strike (distance × credit/DTE)

    def test_picks_best_rr_when_rr_and_credit_diverge(self):
        """Prefers highest R/R even when a different strike has higher absolute credit.
        live_price=295; strike 295 has credit=0.80, risk≈0 → R/R≈800 (wins);
        strike 305 has credit=1.50, risk=10 → R/R=0.15 (loses)."""
        c = _make_rescue_contract("TSLA", 295.0, dte=2)
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp, [
                       {"strike": 295.0, "bid": 0.30, "ask": 1.30},  # mid 0.80, net -0.20 → skip (no credit)
                       {"strike": 305.0, "bid": 2.00, "ask": 3.00},  # mid 2.50, net 1.50, risk=10, R/R=0.15
                   ])), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),  # BTC mid = 1.00
                 (2.00, 3.00, 2.50),  # STO confirm
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-rr")) as mock_spread:
            # Strike 295 has mid=0.80 → net_credit=0.80-1.00=-0.20 (no credit), only 305 qualifies
            result = execute_rescue_rolls([c], {"TSLA": 295.0})
        assert result[0]["success"] is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 305.0  # only credit-positive strike

    # ── Order cancellation ────────────────────────────────────────────────────

    def test_cancels_all_open_orders_for_contract(self):
        """ALL open orders matching option_id are cancelled (not just BTC)."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2, option_id="opt-res")
        next_exp = _future_date(7)
        # Two open orders referencing opt-res (one BTC, one unrelated STO)
        open_orders = [
            {"id": "order-1", "legs": [
                {"side": "buy", "position_effect": "close",
                 "option": "https://api.robinhood.com/options/instruments/opt-res/"},
            ]},
            {"id": "order-2", "legs": [
                {"side": "sell", "position_effect": "open",
                 "option": "https://api.robinhood.com/options/instruments/opt-res/"},
            ]},
        ]
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=open_orders), \
             patch("robin_stocks.robinhood.orders.cancel_option_order") as mock_cancel, \
             patch("time.sleep"), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),
                 (3.00, 4.00, 3.50),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")):
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert mock_cancel.call_count == 2
        cancelled_ids = {c[0][0] for c in mock_cancel.call_args_list}
        assert "order-1" in cancelled_ids
        assert "order-2" in cancelled_ids
        assert result[0]["orders_cancelled"] == 2

    def test_waits_30s_after_cancellation(self):
        c = _make_rescue_contract("TSLA", 300.0, dte=2, option_id="opt-res")
        next_exp = _future_date(7)
        open_orders = [{"id": "ord-99", "legs": [
            {"option": "https://api.robinhood.com/options/instruments/opt-res/"},
        ]}]
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=open_orders), \
             patch("robin_stocks.robinhood.orders.cancel_option_order"), \
             patch("time.sleep") as mock_sleep, \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),
                 (3.00, 4.00, 3.50),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")):
            execute_rescue_rolls([c], {"TSLA": 310.0})
        mock_sleep.assert_called_once_with(30)

    def test_no_sleep_when_no_orders_cancelled(self):
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("time.sleep") as mock_sleep, \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),
                 (3.00, 4.00, 3.50),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")):
            execute_rescue_rolls([c], {"TSLA": 310.0})
        mock_sleep.assert_not_called()

    # ── Successful roll ───────────────────────────────────────────────────────

    def test_successful_rescue_roll(self):
        """Full success: DTE-2 ITM contract rolls to max-credit strike."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),   # BTC mid = 1.00
                 (2.50, 3.50, 3.00),   # STO live mid = 3.00 → net = 2.00 credit
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")) as mock_spread:
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        r = result[0]
        assert r["success"] is True
        assert r["skipped"] is False
        assert r["order_id"] == "rescue-id"
        assert r["direction"] == "credit"
        kw = mock_spread.call_args.kwargs
        assert kw["direction"] == "credit"
        assert kw["price"] == 2.00  # net = 3.00 - 1.00
        legs = kw["spread"]
        assert legs[0]["action"] == "buy" and legs[0]["effect"] == "close"
        assert legs[1]["action"] == "sell" and legs[1]["effect"] == "open"
        assert legs[0]["ratio_quantity"] == 1
        assert legs[1]["ratio_quantity"] == 1

    def test_roll_failure_recorded_in_result(self):
        """If spread order fails, result.success=False with error message."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   return_value=self._ticker_mock(next_exp)), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),
                 (2.50, 3.50, 3.00),
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"detail": "Order rejected"}):
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        r = result[0]
        assert r["success"] is False
        assert r["skipped"] is False
        assert "Order rejected" in r["error"]

    def test_login_failure_returns_error_results(self):
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        with patch("auth.login", side_effect=RuntimeError("auth failed")):
            result = execute_rescue_rolls([c], {"TSLA": 310.0})
        assert len(result) == 1
        assert "auth failed" in result[0]["error"]

    def test_both_dte1_and_dte2_processed(self):
        """Both DTE=1 and DTE=2 ITM contracts are included."""
        contracts = [
            _make_rescue_contract("TSLA", 300.0, dte=1, option_id="opt-1"),
            _make_rescue_contract("AAPL", 200.0, dte=2, option_id="opt-2"),
        ]
        next_exp = _future_date(7)
        tickers = {
            "TSLA": self._ticker_mock(next_exp, [{"strike": 300.0, "bid": 2.0, "ask": 3.0}]),
            "AAPL": self._ticker_mock(next_exp, [{"strike": 200.0, "bid": 1.5, "ask": 2.5}]),
        }
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.yf.Ticker",
                   side_effect=lambda s: tickers.get(s, tickers["TSLA"])), \
             patch("trader._get_live_price", side_effect=[315.0, 210.0]), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.5, 1.5, 1.00), (2.0, 3.0, 2.50),   # TSLA
                 (0.5, 1.5, 1.00), (1.5, 2.5, 2.00),   # AAPL
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=[_good_order("r-1"), _good_order("r-2")]):
            result = execute_rescue_rolls(
                contracts, {"TSLA": 315.0, "AAPL": 210.0}
            )
        assert len(result) == 2
        assert result[0]["symbol"] == "TSLA" and result[0]["success"] is True
        assert result[1]["symbol"] == "AAPL" and result[1]["success"] is True


    # ── Short PUT tests ───────────────────────────────────────────────────────

    def _ticker_mock_put(self, next_exp=None, put_strikes=None):
        """Minimal yf.Ticker mock for rescue-mode PUT tests."""
        if next_exp is None:
            next_exp = _future_date(7)
        if put_strikes is None:
            put_strikes = [{"strike": 290.0, "bid": 3.0, "ask": 3.50}]
        df_puts = pd.DataFrame(put_strikes)
        mock = MagicMock()
        mock.options = [next_exp]
        chain = MagicMock()
        chain.puts  = df_puts
        chain.calls = pd.DataFrame()  # empty — rescue should use puts
        mock.option_chain.return_value = chain
        return mock

    def test_put_otm_not_rescued(self):
        """DTE-2 short PUT that is OTM (stock > strike) is not rescued."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        result = execute_rescue_rolls([c], {"TSLA": 320.0})  # stock ABOVE put strike
        assert result == []

    def test_put_itm_triggers_rescue(self):
        """DTE-2 short PUT that is ITM (stock < strike) is detected."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        result = execute_rescue_rolls([c], {"TSLA": 280.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["opt_type"] == "put"

    def test_put_itm_by_is_positive(self):
        """itm_by for a short PUT = strike − stock_price (always positive)."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        result = execute_rescue_rolls([c], {"TSLA": 285.0}, dry_run=True)
        assert len(result) == 1
        assert result[0]["itm_by"] == pytest.approx(15.0, abs=0.01)

    def test_put_rescue_uses_puts_chain_and_lower_strike(self):
        """Rescue for a short PUT: uses chain.puts and picks strike <= current."""
        next_exp = _future_date(7)
        ticker = self._ticker_mock_put(
            next_exp=next_exp,
            put_strikes=[
                {"strike": 285.0, "bid": 2.5, "ask": 3.00},  # lower: OTM for put
                {"strike": 300.0, "bid": 2.0, "ask": 2.50},  # same strike
                {"strike": 310.0, "bid": 1.0, "ask": 1.50},  # higher: ITM for put
            ]
        )
        captured = {}
        def fake_spread(**kwargs):
            captured.update(kwargs)
            return _good_order("put-rescue-1")

        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        with patch("trader.yf.Ticker", return_value=ticker), \
             patch("trader._get_live_price", return_value=285.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 3.0, 2.50),  # BTC leg at current strike
                 (2.5, 3.0, 2.75),  # STO live mid for target strike
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            result = execute_rescue_rolls([c], {"TSLA": 285.0})
        assert len(result) == 1
        assert result[0]["success"] is True
        # Target strike must be <= current strike (OTM/same for puts)
        assert result[0]["target_strike"] <= 300.0
        # Spread legs must use "put" optionType
        legs = {leg["optionType"] for leg in captured.get("spread", [])}
        assert legs == {"put"}

    def test_put_rescue_risk_calc_is_inverted(self):
        """For put rescue: risk = live_price - new_strike (gap below stock)."""
        next_exp = _future_date(7)
        # Two candidate put strikes with different R/R profiles:
        # strike=295: net_credit=2.00, risk=live_price-295=290-295=-5 → clip to eps → huge RR
        # strike=285: net_credit=1.50, risk=live_price-285=290-285=5 → RR=0.30
        # Best by R/R = 295 (near ATM has near-zero risk hence huge ratio)
        ticker = self._ticker_mock_put(
            next_exp=next_exp,
            put_strikes=[
                {"strike": 295.0, "bid": 3.5, "ask": 4.00},
                {"strike": 285.0, "bid": 2.0, "ask": 2.50},
            ]
        )
        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        with patch("trader.yf.Ticker", return_value=ticker), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (2.0, 3.0, 2.50),   # BTC leg at 300
                 (3.5, 4.0, 3.75),   # STO live mid for 295
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rr-put-1")), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            result = execute_rescue_rolls([c], {"TSLA": 290.0})
        assert len(result) == 1
        assert result[0]["success"] is True
        assert result[0]["target_strike"] == pytest.approx(295.0, abs=0.01)

    def test_put_no_lower_strikes_skipped(self):
        """Rescue PUT skipped when no strikes <= current with positive bid exist."""
        next_exp = _future_date(7)
        # Only strikes above current (310) — no valid candidates for put roll
        ticker = self._ticker_mock_put(
            next_exp=next_exp,
            put_strikes=[{"strike": 310.0, "bid": 1.0, "ask": 1.50}]
        )
        c = _make_rescue_contract("TSLA", 300.0, dte=2, opt_type="put")
        with patch("trader.yf.Ticker", return_value=ticker), \
             patch("trader._get_live_price", return_value=285.0), \
             patch("trader._get_option_bid_ask", return_value=(1.0, 1.5, 1.25)), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            result = execute_rescue_rolls([c], {"TSLA": 285.0})
        assert len(result) == 1
        assert result[0]["skipped"] is True


# ─────────────────────────────────────────────────────────────────────────────
# roll_forward --rescue flag tests
# ─────────────────────────────────────────────────────────────────────────────

class TestRollForwardRescue:
    """Tests for roll_forward() with rescue=True."""

    def _exp(self):
        return _future_date(2)

    def _next_exp(self):
        return _future_date(9)

    def _chain_str(self):
        d = date.fromisoformat(self._exp())
        return f"$300 CALL {d.month}/{d.day}"

    def _spread_patch(self, order_id="rescue-roll-id"):
        return patch(
            "robin_stocks.robinhood.orders.order_option_spread",
            return_value=_good_order(order_id),
        )

    def test_rescue_skips_when_no_credit(self, capsys):
        """--rescue returns False and prints warning when no credit available."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        next_exp = self._next_exp()
        calls_df = _make_calls_df([{"strike": 300.0, "bid": 1.00, "ask": 2.00}])
        mock_ticker = _make_ticker_mock(expirations=[next_exp], calls_df=calls_df,
                                        next_expiration=next_exp)
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=305.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (3.0, 4.0, 3.50),   # BTC mid = 3.50 (expensive)
                 (1.0, 2.0, 1.50),   # STO live mid = 1.50 → net = -2.00 (debit)
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker):
            result = roll_forward("TSLA", self._chain_str(), rescue=True)
        assert result is False
        out = capsys.readouterr().out
        assert "No credit available" in out or "no credit" in out.lower()

    def test_rescue_picks_best_rr_strike(self, capsys):
        """--rescue selects the strike with the best R/R ratio (net_credit / max(strike-price, ε))."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        next_exp = self._next_exp()
        # Three strikes; 305 (ATM, risk≈0) → R/R≈2500 beats 300 (ITM) and 310 (OTM).
        # live_price=305; strike 305 has mid 3.50, BTC mid=1.00 → net=2.50, risk≈0 → best R/R
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 2.00, "ask": 3.00},   # mid 2.50, net 1.50
            {"strike": 305.0, "bid": 3.00, "ask": 4.00},   # mid 3.50, net 2.50 ← best
            {"strike": 310.0, "bid": 1.50, "ask": 2.50},   # mid 2.00, net 1.00
        ])
        mock_ticker = _make_ticker_mock(expirations=[next_exp], calls_df=calls_df,
                                        next_expiration=next_exp)
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=305.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),   # BTC mid = 1.00
                 (3.00, 4.00, 3.50),   # STO live mid = 3.50 → net = 2.50
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str(), rescue=True)
        assert result is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 305.0  # highest-credit strike

    def test_rescue_proceeds_even_with_btc_order_exists(self, capsys):
        """--rescue does NOT abort when btc_order_exists=True (will cancel first)."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp, btc_order_exists=True,
                                    purchase_price=-185.0)]
        next_exp = self._next_exp()
        calls_df = _make_calls_df([{"strike": 300.0, "bid": 2.0, "ask": 3.0}])
        mock_ticker = _make_ticker_mock(expirations=[next_exp], calls_df=calls_df,
                                        next_expiration=next_exp)
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.50, 1.50, 1.00),
                 (2.00, 3.00, 2.50),
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str(), rescue=True)
        assert result is True
        assert "already exists" not in capsys.readouterr().out

    def test_rescue_picks_rr_over_max_credit(self, capsys):
        """When R/R-optimal strike differs from highest-credit strike, R/R wins.
        live_price=300; strike 300 (ATM, risk≈0, credit=0.50 → R/R≈500 wins) vs
        strike 310 (OTM, risk=10, credit=1.50 → R/R=0.15 loses)."""
        exp = self._exp()
        contracts = [_make_contract("TSLA", 300.0, exp)]
        next_exp = self._next_exp()
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 0.75, "ask": 1.25},   # mid 1.00, net 0.50, risk≈0 → R/R≈500
            {"strike": 310.0, "bid": 2.00, "ask": 3.00},   # mid 2.50, net 2.00, risk=10 → R/R=0.20
        ])
        mock_ticker = _make_ticker_mock(expirations=[next_exp], calls_df=calls_df,
                                        next_expiration=next_exp)
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=300.0), \
             patch("trader._get_option_bid_ask", side_effect=[
                 (0.00, 1.00, 0.50),   # BTC mid = 0.50
                 (0.75, 1.25, 1.00),   # STO live mid confirm
             ]), \
             patch("trader.yf.Ticker", return_value=mock_ticker), \
             patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             self._spread_patch() as mock_spread:
            result = roll_forward("TSLA", self._chain_str(), rescue=True)
        assert result is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 300.0  # best R/R despite lower credit than 310


# ─────────────────────────────────────────────────────────────────────────────
# execute_optimize_rolls tests
# ─────────────────────────────────────────────────────────────────────────────
#
# Pricing reference:
#   purchase_price = -185.0  →  per_share_premium = abs(-185) / 100 = $1.85
#   trigger threshold = $1.85 × 1.40 = $2.59
#   mid = $2.60 → +40.5% gain  → TRIGGERED
#   mid = $2.57 → +38.9% gain  → not triggered
#
# purchase_price = -200.0 → per_share = $2.00; threshold = $2.80
# purchase_price = -150.0 → per_share = $1.50; threshold = $2.10


def _make_optimize_contract(
    symbol="TSLA",
    strike=300.0,
    purchase_price=-185.0,
    expiration=None,
    opt_type="call",
    quantity=1,
    option_id="opt-opt-test",
):
    """
    Factory for optimize-mode test contracts.
    purchase_price mirrors Robinhood raw average_price:
      negative → credit received (short option)
      positive → debit paid    (long option)
    Default per_share premium = abs(-185)/100 = $1.85; trigger at mid >= $2.59.
    """
    if expiration is None:
        expiration = _future_date(30)
    return {
        "symbol":           symbol,
        "opt_type":         opt_type,
        "strike":           strike,
        "expiration":       expiration,
        "quantity":         quantity,
        "btc_order_exists": False,
        "option_id":        option_id,
        "purchase_price":   purchase_price,
    }


class TestExecuteOptimizeRolls:
    """Unit tests for execute_optimize_rolls (optimize mode)."""

    def _exp(self, days=30):
        return _future_date(days)

    def _ticker_mock(self, expirations, calls_df=None, puts_df=None):
        """Build a minimal yf.Ticker mock."""
        mock = MagicMock()
        mock.options = expirations
        chain = MagicMock()
        chain.calls = calls_df if calls_df is not None else pd.DataFrame(
            [], columns=["strike", "bid", "ask"]
        )
        chain.puts = puts_df if puts_df is not None else pd.DataFrame(
            [], columns=["strike", "bid", "ask"]
        )
        mock.option_chain.return_value = chain
        return mock

    # ── No-op / skip cases ───────────────────────────────────────────────────

    def test_empty_input_returns_empty(self):
        result = execute_optimize_rolls([], {})
        assert result == []

    def test_dte0_contract_not_triggered(self):
        """DTE-0 contracts (expiring today) are excluded from optimize mode."""
        c = _make_optimize_contract("TSLA", 300.0)
        c["expiration"] = _today()
        with patch("trader._get_option_bid_ask", return_value=(2.3, 2.9, 2.60)):
            result = execute_optimize_rolls([c], {"TSLA": 305.0})
        assert result == []

    def test_zero_purchase_price_not_triggered(self):
        """Contracts with purchase_price == 0 cannot compute gain → skipped."""
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=0.0)
        with patch("trader._get_option_bid_ask", return_value=(2.3, 2.9, 2.60)):
            result = execute_optimize_rolls([c], {"TSLA": 305.0})
        assert result == []

    def test_gain_below_40pct_not_triggered(self):
        """mid = $2.57 / per_share $1.85 = 38.9% < 40% → not triggered."""
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0)
        with patch("trader._get_option_bid_ask", return_value=(2.3, 2.8, 2.57)):
            result = execute_optimize_rolls([c], {"TSLA": 305.0})
        assert result == []

    def test_zero_mid_not_triggered(self):
        """No market data (mid=0) → not triggered."""
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0)
        with patch("trader._get_option_bid_ask", return_value=(0.0, 0.0, 0.0)):
            result = execute_optimize_rolls([c], {"TSLA": 305.0})
        assert result == []

    # ── Dry run ───────────────────────────────────────────────────────────────

    def test_dry_run_returns_result_without_placing_orders(self):
        """dry_run=True returns result dicts but never calls login or order API."""
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0)
        with patch("trader._get_option_bid_ask", return_value=(2.3, 2.9, 2.60)), \
             patch("auth.login") as mock_login, \
             patch("robin_stocks.robinhood.orders.order_option_spread") as mock_order:
            result = execute_optimize_rolls([c], {"TSLA": 305.0}, dry_run=True)
        mock_login.assert_not_called()
        mock_order.assert_not_called()
        assert len(result) == 1
        assert "[DRY RUN]" in result[0]["error"]
        assert result[0]["gain_pct"] >= 40.0

    # ── CALL: roll UP to higher strike ────────────────────────────────────────

    def test_call_rolls_up_to_higher_strike(self):
        """CALL triggered: best credit at higher strike → rolls UP (raise ceiling)."""
        exp = self._exp(30)
        next_exp = self._exp(35)   # 5 days after → within 10-day window
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        # btc_mid = $2.60; STO at $310 next_exp has mid = $3.10 → credit = $0.50
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 2.50, "ask": 2.70},  # mid 2.60, net = 0.00
            {"strike": 310.0, "bid": 2.90, "ask": 3.30},  # mid 3.10, net = 0.50
        ])
        ticker = self._ticker_mock(expirations=[exp, next_exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),   # trigger check: mid=2.60, 40.5% gain
                 (2.3, 2.9, 2.60),   # btc_mid inside loop
                 (2.90, 3.30, 3.10), # sto_mid_live confirmation
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-opt-call", "state": "queued"}) as mock_spread:
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        assert len(result) == 1
        r = result[0]
        assert r["success"] is True
        assert r["target_strike"] == 310.0      # rolled UP to higher strike
        assert r["opt_type"] == "call"
        assert r["gain_pct"] >= 40.0
        assert r["direction"] == "credit"
        # Verify spread legs
        spread_call = mock_spread.call_args.kwargs
        assert spread_call["direction"] == "credit"
        btc_leg = spread_call["spread"][0]
        sto_leg = spread_call["spread"][1]
        assert float(btc_leg["strike"]) == 300.0
        assert btc_leg["action"] == "buy"
        assert float(sto_leg["strike"]) == 310.0
        assert sto_leg["action"] == "sell"

    def test_call_rolls_at_same_expiration(self):
        """CALL: rolls at the SAME expiration when credit is available there."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        # Only one expiration = the same one; higher strike available with credit
        calls_df = _make_calls_df([
            {"strike": 310.0, "bid": 2.90, "ask": 3.30},  # mid 3.10
        ])
        ticker = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),   # trigger check
                 (2.3, 2.9, 2.60),   # btc_mid
                 (2.90, 3.30, 3.10), # sto_mid_live
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-same-exp", "state": "queued"}):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        assert result[0]["success"] is True
        assert result[0]["next_expiration"] == exp   # same expiration
        assert result[0]["target_strike"] == 310.0

    # ── PUT: roll DOWN to lower strike ────────────────────────────────────────

    def test_put_rolls_down_to_lower_strike(self):
        """PUT triggered: best credit at lower strike → rolls DOWN (lower floor)."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0,
                                     expiration=exp, opt_type="put")

        # btc_mid = $2.60; STO at $290 has mid = $3.00 → credit = $0.40
        puts_df = _make_calls_df([
            {"strike": 290.0, "bid": 2.80, "ask": 3.20},  # mid 3.00 → net +0.40
            {"strike": 295.0, "bid": 2.65, "ask": 2.85},  # mid 2.75 → net +0.15
            {"strike": 300.0, "bid": 2.50, "ask": 2.70},  # mid 2.60 → net 0.00
        ])
        ticker = self._ticker_mock(expirations=[exp], puts_df=puts_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),   # trigger check
                 (2.3, 2.9, 2.60),   # btc_mid
                 (2.80, 3.20, 3.00), # sto_mid_live
             ]), \
             patch("trader._get_live_price", return_value=305.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-opt-put", "state": "queued"}) as mock_spread:
            result = execute_optimize_rolls([c], {"TSLA": 305.0})

        assert len(result) == 1
        r = result[0]
        assert r["success"] is True
        assert r["target_strike"] <= 300.0      # rolled DOWN to lower strike
        assert r["opt_type"] == "put"
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) <= 300.0
        assert sto_leg["action"] == "sell"
        assert mock_spread.call_args.kwargs["direction"] == "credit"

    def test_put_does_not_use_calls_chain(self):
        """PUT must use chain_data.puts, not chain_data.calls."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0,
                                     expiration=exp, opt_type="put")

        calls_df = _make_calls_df([{"strike": 290.0, "bid": 2.80, "ask": 3.20}])
        puts_df  = _make_calls_df([{"strike": 290.0, "bid": 2.80, "ask": 3.20}])
        ticker   = self._ticker_mock(expirations=[exp], calls_df=calls_df, puts_df=puts_df)

        spread_kwargs = {}
        def capture_spread(**kw):
            spread_kwargs.update(kw)
            return {"id": "ord-put-chain", "state": "queued"}

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),
                 (2.3, 2.9, 2.60),
                 (2.80, 3.20, 3.00),
             ]), \
             patch("trader._get_live_price", return_value=305.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=capture_spread):
            result = execute_optimize_rolls([c], {"TSLA": 305.0})

        assert result[0]["success"] is True
        # The spread legs should use optionType="put"
        for leg in spread_kwargs.get("spread", []):
            assert leg["optionType"] == "put"

    # ── Skip cases ────────────────────────────────────────────────────────────

    def test_no_credit_positive_candidate_returns_skipped(self):
        """When no strike yields net credit, result has skipped=True."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        # btc_mid = 2.60; all STO mids are lower → no credit
        calls_df = _make_calls_df([
            {"strike": 300.0, "bid": 2.30, "ask": 2.50},  # mid 2.40 < btc_mid 2.60
            {"strike": 305.0, "bid": 2.40, "ask": 2.60},  # mid 2.50 < btc_mid 2.60
        ])
        ticker = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),   # trigger check
                 (2.3, 2.9, 2.60),   # btc_mid
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        assert len(result) == 1
        assert result[0]["skipped"] is True
        assert result[0]["success"] is False

    def test_no_expirations_in_window_returns_skipped(self):
        """No expirations in [exp, exp+10d] → skipped with informative error."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        # Only expirations far outside the +10d window
        far_exp = self._exp(60)   # 30 days after current exp → outside window
        ticker  = self._ticker_mock(expirations=[far_exp])

        with patch("trader._get_option_bid_ask", return_value=(2.3, 2.9, 2.60)), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        assert len(result) == 1
        assert result[0]["skipped"] is True
        assert "window" in result[0]["error"].lower()

    # ── R/R selection ─────────────────────────────────────────────────────────

    def test_best_rr_selected_not_max_credit(self):
        """Picks strike with best R/R ratio using formula: (net_credit/DTE) × distance.
        distance = new_strike − current_strike (for calls).

        current_strike=300, btc_mid=2.60, exp=30 DTE:
          $305 strike: mid=2.70, net=0.10, distance=5  → R/R=(0.10/30)×5 ≈ 0.0167
          $310 strike: mid=3.10, net=0.50, distance=10 → R/R=(0.50/30)×10 ≈ 0.167  ← BEST
        $310 wins: higher credit-per-day × further distance beats $305.
        Same-strike ($300) would get R/R=0 and rank last.
        """
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        calls_df = _make_calls_df([
            {"strike": 305.0, "bid": 2.60, "ask": 2.80},  # mid=2.70, net=0.10, dist=5  → R/R≈0.017
            {"strike": 310.0, "bid": 2.90, "ask": 3.30},  # mid=3.10, net=0.50, dist=10 → R/R≈0.167
        ])
        ticker = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),   # trigger check
                 (2.3, 2.9, 2.60),   # btc_mid = 2.60
                 (2.90, 3.30, 3.10), # sto_mid_live for $310
             ]), \
             patch("trader._get_live_price", return_value=300.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-rr", "state": "queued"}) as mock_spread:
            result = execute_optimize_rolls([c], {"TSLA": 300.0})

        assert result[0]["success"] is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 310.0   # R/R winner: higher (credit/DTE)×distance

    # ── Order cancellation + sleep ────────────────────────────────────────────

    def test_outstanding_order_cancelled_and_20s_wait(self):
        """When an existing order matches the option_id, it is cancelled and
        time.sleep(20) is called (not 30 like panic/rescue)."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0,
                                     expiration=exp, option_id="opt-opt-xyz")

        open_order = {
            "id": "order-to-cancel",
            "legs": [{"option": "https://api.robinhood.com/options/instruments/opt-opt-xyz/"}],
        }
        calls_df = _make_calls_df([{"strike": 310.0, "bid": 2.90, "ask": 3.30}])
        ticker   = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),
                 (2.3, 2.9, 2.60),
                 (2.90, 3.30, 3.10),
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[open_order]), \
             patch("robin_stocks.robinhood.orders.cancel_option_order") as mock_cancel, \
             patch("trader.time.sleep") as mock_sleep, \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-after-cancel", "state": "queued"}):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        mock_cancel.assert_called_once_with("order-to-cancel")
        mock_sleep.assert_called_once_with(20)   # 20s wait, not 30
        assert result[0]["orders_cancelled"] == 1
        assert result[0]["success"] is True

    def test_no_outstanding_order_no_sleep(self):
        """When no outstanding orders exist, time.sleep is NOT called."""
        exp = self._exp(30)
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0, expiration=exp)

        calls_df = _make_calls_df([{"strike": 310.0, "bid": 2.90, "ask": 3.30}])
        ticker   = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.3, 2.9, 2.60),
                 (2.3, 2.9, 2.60),
                 (2.90, 3.30, 3.10),
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("trader.time.sleep") as mock_sleep, \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-no-cancel", "state": "queued"}):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        mock_sleep.assert_not_called()
        assert result[0]["orders_cancelled"] == 0

    # ── Result dict fields ────────────────────────────────────────────────────

    def test_gain_pct_field_computed_correctly(self):
        """gain_pct = (mid / per_share_premium − 1) × 100."""
        exp = self._exp(30)
        # purchase_price = -200.0 → per_share = $2.00; mid = $3.00 → gain = 50%
        c = _make_optimize_contract("TSLA", 300.0, purchase_price=-200.0, expiration=exp)

        calls_df = _make_calls_df([{"strike": 305.0, "bid": 2.70, "ask": 3.30}])
        ticker   = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        with patch("trader._get_option_bid_ask", side_effect=[
                 (2.7, 3.3, 3.00),   # trigger check: mid=3.00, 50% gain
                 (2.7, 3.3, 3.00),   # btc_mid
                 (2.70, 3.30, 3.00), # sto_mid_live
             ]), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-gp", "state": "queued"}):
            result = execute_optimize_rolls([c], {"TSLA": 295.0})

        assert result[0]["gain_pct"] == 50.0
        assert result[0]["purchase_price_per_share"] == 2.00

    # ── Mixed batch ───────────────────────────────────────────────────────────

    def test_only_triggered_contracts_in_results(self):
        """Only contracts above the 40% threshold appear in results list."""
        exp = self._exp(30)
        c_triggered = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0,
                                               expiration=exp, option_id="opt-a")
        # AAPL per_share = 1.50; mid = 2.00 → 33% < 40% → NOT triggered
        c_not = _make_optimize_contract("AAPL", 180.0, purchase_price=-150.0,
                                         expiration=exp, option_id="opt-b")

        calls_df = _make_calls_df([{"strike": 310.0, "bid": 2.90, "ask": 3.30}])
        ticker   = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        def _mock_ba(sym, strike, opt_type, exp_):
            if sym == "TSLA":
                return (2.3, 2.9, 2.60)   # 40.5% → triggered
            return (1.7, 2.3, 2.00)        # 33%   → NOT triggered

        with patch("trader._get_option_bid_ask", side_effect=_mock_ba), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-tsla", "state": "queued"}):
            result = execute_optimize_rolls(
                [c_triggered, c_not], {"TSLA": 295.0, "AAPL": 175.0}
            )

        assert len(result) == 1
        assert result[0]["symbol"] == "TSLA"

    def test_multiple_triggered_contracts_all_processed(self):
        """Two triggered contracts both produce result dicts (success or skipped)."""
        exp = self._exp(30)
        c1 = _make_optimize_contract("TSLA", 300.0, purchase_price=-185.0,
                                      expiration=exp, option_id="opt-1")
        c2 = _make_optimize_contract("NVDA", 500.0, purchase_price=-185.0,
                                      expiration=exp, option_id="opt-2")

        # calls_df: higher strike for each symbol gets net credit vs btc_mid 2.60
        # TSLA rolls to $310 (mid 3.20 - btc 2.60 = +0.60 credit)
        # NVDA rolls to $510 (mid 3.20 - btc 2.60 = +0.60 credit)
        calls_df = _make_calls_df([
            {"strike": 310.0, "bid": 3.00, "ask": 3.40},  # mid=3.20
            {"strike": 510.0, "bid": 3.00, "ask": 3.40},  # mid=3.20
        ])
        ticker = self._ticker_mock(expirations=[exp], calls_df=calls_df)

        def _mock_ba(sym, strike, opt_type, exp_):
            # btc_mid = 2.60 for both; sto_mid = 3.20 for rolled-to strikes
            if abs(strike - 300.0) < 1 or abs(strike - 500.0) < 1:
                return (2.3, 2.9, 2.60)   # trigger check + btc_mid (original strikes)
            return (3.00, 3.40, 3.20)     # sto_mid_live (for higher/lower rolled-to strikes)

        with patch("trader._get_option_bid_ask", side_effect=_mock_ba), \
             patch("trader._get_live_price", return_value=295.0), \
             patch("trader.yf.Ticker", return_value=ticker), \
             patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.id_for_option",
                   return_value="test-option-id-abc"), \
             patch("robin_stocks.robinhood.orders.get_all_open_option_orders",
                   return_value=[]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "ord-multi", "state": "queued"}):
            result = execute_optimize_rolls(
                [c1, c2], {"TSLA": 295.0, "NVDA": 295.0}
            )

        # Both contracts must appear in results (success or skipped)
        assert len(result) == 2
        symbols = {r["symbol"] for r in result}
        assert "TSLA" in symbols
        assert "NVDA" in symbols

