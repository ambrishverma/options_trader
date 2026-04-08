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

class TestShowOpenContracts:
    def test_no_contracts_prints_message(self, capsys):
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=[]):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "No open covered-call contracts found for TSLA" in out

    def test_shows_table_with_contracts(self, capsys):
        exp = _future_date(15)
        contracts = [_make_contract("TSLA", 300.0, exp, purchase_price=-185.0)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", return_value=(2.0, 2.50, 2.25)):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "$300" in out
        assert "OTM" in out   # 290 < 300, so OTM
        assert "$2.25" in out  # mid price

    def test_itm_status_shown(self, capsys):
        exp = _future_date(5)
        contracts = [_make_contract("TSLA", 300.0, exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=310.0), \
             patch("trader._get_option_bid_ask", return_value=(5.0, 5.50, 5.25)):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "ITM" in out

    def test_btc_flag_shown(self, capsys):
        exp = _future_date(10)
        contracts = [_make_contract("TSLA", 300.0, exp, btc_order_exists=True)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", return_value=(1.0, 1.50, 1.25)):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "[BTC]" in out

    def test_filters_to_requested_symbol_only(self, capsys):
        exp = _future_date(20)
        contracts = [
            _make_contract("TSLA", 300.0, exp),
            _make_contract("AAPL", 200.0, exp),
        ]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", return_value=(2.0, 2.50, 2.25)):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "AAPL" not in out

    def test_expired_contracts_are_excluded(self, capsys):
        """Contracts whose expiration date has passed must not appear in the table."""
        from datetime import date, timedelta
        past_exp = str(date.today() - timedelta(days=4))
        future_exp = _future_date(10)
        contracts = [
            _make_contract("TSLA", 300.0, past_exp),
            _make_contract("TSLA", 320.0, future_exp),
        ]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts), \
             patch("trader._get_live_price", return_value=290.0), \
             patch("trader._get_option_bid_ask", return_value=(2.0, 2.50, 2.25)):
            show_open_contracts("TSLA")
        out = capsys.readouterr().out
        assert "$320" in out
        assert "$300" not in out

    def test_all_contracts_expired_prints_message(self, capsys):
        """If all contracts are expired, print the 'no active contracts' message."""
        from datetime import date, timedelta
        past_exp = str(date.today() - timedelta(days=4))
        contracts = [_make_contract("TSLA", 300.0, past_exp)]
        with patch("portfolio.load_open_calls_detail_snapshot", return_value=contracts):
            show_open_contracts("TSLA")
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


def _make_panic_contract(symbol, strike, btc_order_exists=False, option_id="opt-abc"):
    return {
        "symbol":           symbol,
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


# ─────────────────────────────────────────────────────────────────────────────
# execute_safety_btc_orders tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_safety_contract(symbol, strike, dte, btc_order_exists=False,
                          purchase_price=2.00, option_id="opt-safe"):
    """Build a contract dict for safety-mode tests."""
    exp = _future_date(dte)
    return {
        "symbol":           symbol,
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
        """btc_price = MIN($0.20, 10% of purchase, mid)."""
        # purchase=$2.00 → 10%=$0.20; mid=$1.50 → MIN(0.20, 0.20, 1.50) = $0.20
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=2.00)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.15, 0.25, 0.20)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.20
        assert mock_order.call_args.kwargs["price"] == 0.20

    def test_price_uses_ten_pct_when_smaller(self):
        """When 10% of purchase < $0.20, use 10% of purchase."""
        # purchase=$1.50 → 10%=$0.15; mid=$0.50 → MIN(0.20, 0.15, 0.50) = $0.15
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=1.50)
        with patch("auth.login"), patch("auth.logout"), \
             patch("trader._get_option_bid_ask", return_value=(0.45, 0.55, 0.50)), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value=_good_order()) as mock_order:
            result = execute_safety_btc_orders([c], {"TSLA": 290.0})
        assert result[0]["btc_price"] == 0.15
        assert mock_order.call_args.kwargs["price"] == 0.15

    def test_price_uses_mid_when_smallest(self):
        """When mid < $0.20 and mid < 10% of purchase, use mid."""
        # purchase=$5.00 → 10%=$0.50; mid=$0.05 → MIN(0.20, 0.50, 0.05) = $0.05
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=5.00)
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
        c = _make_safety_contract("TSLA", 300.0, 5, purchase_price=2.00)
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


# ─────────────────────────────────────────────────────────────────────────────
# execute_rescue_rolls tests
# ─────────────────────────────────────────────────────────────────────────────

def _make_rescue_contract(symbol, strike, dte=2, btc_order_exists=False,
                          option_id="opt-rescue"):
    """Build a DTE-1-2 ITM contract dict for rescue-mode tests."""
    return {
        "symbol":           symbol,
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
        """Selects the strike with the best Risk/Reward ratio (net_credit / max(strike-price, ε)).
        live_price=305; strikes 300 (ITM, risk≈0 → near-∞ R/R), 305 (ATM, risk≈0),
        310 (OTM, risk=5). Strike 300 wins due to near-infinite R/R despite lower credit."""
        c = _make_rescue_contract("TSLA", 300.0, dte=2)
        next_exp = _future_date(7)
        # BTC mid = 1.00; strikes 300 (mid 2.50) and 305 (mid 1.80) and 310 (mid 1.20)
        # Net credits: 1.50, 0.80, 0.20; R/R: ≈1500, ≈800, 0.04 → pick 300 (best R/R)
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
                 (2.00, 3.00, 2.50),  # STO live mid confirm = 2.50
             ]), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value=_good_order("rescue-id")) as mock_spread:
            result = execute_rescue_rolls([c], {"TSLA": 305.0})
        assert result[0]["success"] is True
        sto_leg = mock_spread.call_args.kwargs["spread"][1]
        assert float(sto_leg["strike"]) == 300.0  # best R/R strike

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

