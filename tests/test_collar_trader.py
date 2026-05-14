"""
test_collar_trader.py — Tests for collar trader functions (show_collar_holdings,
place_collar_order) and collar.find_collar_roll_options.

Tests for show_collar_holdings:
  - Displays collar pairs (short CALL + long PUT, same symbol/expiration)
  - Handles unpaired legs (short call without matching put, etc.)
  - No-positions case prints appropriate message
  - Symbol filter works

Tests for place_collar_order:
  - Successful placement submits two orders (STO call + BTO put)
  - Call order failure aborts without attempting put order
  - User declines prompt → returns False

Tests for find_collar_roll_options:
  - Filters recs to require higher call/put strikes than current
  - Falls back to all recs when no higher-strike options found
  - No current position (min_call/put = 0) → returns all recs (same as run_collar_on_demand)
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import date

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trader import show_collar_holdings, place_collar_order
from collar import find_collar_roll_options


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _future_date(days: int) -> str:
    from datetime import date, timedelta
    return (date.today() + timedelta(days=days)).isoformat()


def _make_rh_position(
    chain_symbol="TSLA",
    quantity="1",
    pos_type="short",        # "short" / "long"
    option_id="opt-001",
    expiration_date="",
    average_price="150.00",
):
    return {
        "chain_symbol": chain_symbol,
        "quantity":     quantity,
        "type":         pos_type,
        "option_id":    option_id,
        "expiration_date": expiration_date,
        "average_price": average_price,
    }


def _make_rh_instrument(
    option_type="call",
    strike_price="300.00",
    expiration_date="2026-06-20",
):
    return {
        "type":            option_type,
        "strike_price":    strike_price,
        "expiration_date": expiration_date,
    }


# ─────────────────────────────────────────────────────────────────────────────
# show_collar_holdings
# ─────────────────────────────────────────────────────────────────────────────

class TestShowCollarHoldings:

    def _run_show(self, positions, instruments, symbol=None, capsys=None):
        """
        Drive show_collar_holdings with mocked Robinhood auth and positions.
        instruments: list of instr dicts, matched positionally to positions.
        """
        import robin_stocks.robinhood as _rh_unused
        instr_iter = iter(instruments)

        def fake_get_instrument(option_id):
            return next(instr_iter)

        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=fake_get_instrument):
            show_collar_holdings(symbol)

        if capsys:
            return capsys.readouterr().out
        return ""

    def test_displays_collar_pair(self, capsys):
        exp = _future_date(45)
        positions = [
            _make_rh_position("TSLA", "2", "short", "opt-c", average_price="200.00"),
            _make_rh_position("TSLA", "2", "long",  "opt-p", average_price="150.00"),
        ]
        instruments = [
            _make_rh_instrument("call", "300.00", exp),
            _make_rh_instrument("put",  "280.00", exp),
        ]
        self._run_show(positions, instruments)
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "300.00" in out   # call strike
        assert "280.00" in out   # put strike
        assert str(exp) in out

    def test_no_positions_prints_message(self, capsys):
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[]):
            show_collar_holdings()
        out = capsys.readouterr().out
        assert "No open options positions" in out

    def test_symbol_filter_excludes_other_symbols(self, capsys):
        exp = _future_date(45)
        positions = [
            _make_rh_position("TSLA", "1", "short", "opt-c"),
            _make_rh_position("TSLA", "1", "long",  "opt-p"),
            _make_rh_position("AAPL", "1", "short", "opt-a"),
            _make_rh_position("AAPL", "1", "long",  "opt-b"),
        ]
        instruments = [
            _make_rh_instrument("call", "300.00", exp),
            _make_rh_instrument("put",  "280.00", exp),
            _make_rh_instrument("call", "200.00", exp),
            _make_rh_instrument("put",  "190.00", exp),
        ]
        self._run_show(positions, instruments, symbol="TSLA")
        out = capsys.readouterr().out
        assert "TSLA" in out
        # AAPL should not appear when filtering for TSLA
        assert "AAPL" not in out

    def test_unpaired_leg_shown_separately(self, capsys):
        """A short call without a matching long put is listed under unpaired legs."""
        exp = _future_date(45)
        positions = [
            _make_rh_position("TSLA", "1", "short", "opt-c"),  # no matching put
        ]
        instruments = [
            _make_rh_instrument("call", "300.00", exp),
        ]
        self._run_show(positions, instruments)
        out = capsys.readouterr().out
        assert "Unpaired" in out
        assert "SHORT CALL" in out

    def test_multiple_collars_same_expiration_all_shown(self, capsys):
        """Two short calls + two long puts on the same symbol/expiry → two collar rows."""
        exp = _future_date(45)
        positions = [
            _make_rh_position("INTU", "1", "short", "opt-c1", average_price="6.50"),
            _make_rh_position("INTU", "1", "short", "opt-c2", average_price="5.55"),
            _make_rh_position("INTU", "1", "long",  "opt-p1", average_price="51.90"),
            _make_rh_position("INTU", "1", "long",  "opt-p2", average_price="57.60"),
        ]
        instruments = [
            _make_rh_instrument("call", "460.00", exp),
            _make_rh_instrument("call", "470.00", exp),
            _make_rh_instrument("put",  "380.00", exp),
            _make_rh_instrument("put",  "390.00", exp),
        ]
        self._run_show(positions, instruments)
        out = capsys.readouterr().out
        assert out.count("INTU") >= 2      # at least two collar rows
        assert "460.00" in out
        assert "470.00" in out
        assert "380.00" in out
        assert "390.00" in out
        assert "Unpaired" not in out

    def test_zero_quantity_positions_excluded(self, capsys):
        with patch("auth.login"), \
             patch("auth.logout"), \
             patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[_make_rh_position(quantity="0")]):
            show_collar_holdings()
        out = capsys.readouterr().out
        assert "No open options positions" in out


# ─────────────────────────────────────────────────────────────────────────────
# place_collar_order
# ─────────────────────────────────────────────────────────────────────────────

def _make_rec(
    call_strike=300.0,
    call_exp="2026-06-20",
    call_mid=3.00,
    put_strike=280.0,
    put_exp="2026-06-20",
    put_mid=2.50,
    contracts=1,
):
    return {
        "contracts": contracts,
        "call_leg": {
            "strike":     call_strike,
            "expiration": call_exp,
            "mid":        call_mid,
        },
        "put_leg": {
            "strike":     put_strike,
            "expiration": put_exp,
            "mid":        put_mid,
        },
    }


class TestPlaceCollarOrder:

    def _patch_orders(self, put_return=None, call_return=None, pending=None):
        """Helper: patch both order functions and pending-order check."""
        return (
            patch("auth.login"),
            patch("auth.logout"),
            patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                  return_value=put_return or {"id": "put-1"}),
            patch("robin_stocks.robinhood.orders.order_sell_option_limit",
                  return_value=call_return or {"id": "call-1"}),
            patch("robin_stocks.robinhood.orders.get_all_option_orders",
                  return_value=pending or []),
        )

    def test_successful_placement_returns_true(self):
        rec = _make_rec()
        patches = self._patch_orders()
        with patches[0], patches[1], patches[2], patches[3], patches[4]:
            result = place_collar_order("TSLA", rec, prompt=False)
        assert result is True

    def test_two_step_put_placed_before_call(self):
        """BUY PUT is submitted first, then SELL CALL — in that order."""
        rec = _make_rec()
        call_order = []
        mock_put  = MagicMock(side_effect=lambda **kw: call_order.append("put")  or {"id": "put-1"})
        mock_call = MagicMock(side_effect=lambda **kw: call_order.append("call") or {"id": "call-1"})
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",  mock_put), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit", mock_call), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            place_collar_order("TSLA", rec, prompt=False)
        assert call_order == ["put", "call"]   # PUT always first

    def test_strike_format_is_4dp(self):
        """Strikes passed to Robinhood must be formatted as NNN.NNNN (4 decimal places)."""
        rec = _make_rec(call_strike=300.0, put_strike=280.0)
        mock_put  = MagicMock(return_value={"id": "put-1"})
        mock_call = MagicMock(return_value={"id": "call-1"})
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",  mock_put), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit", mock_call), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            place_collar_order("TSLA", rec, prompt=False)
        assert mock_put.call_args.kwargs["strike"]  == "280.0000"
        assert mock_call.call_args.kwargs["strike"] == "300.0000"

    def test_contracts_override_used_instead_of_rec_contracts(self):
        rec = _make_rec(contracts=5)    # rec says 5
        mock_put  = MagicMock(return_value={"id": "put-1"})
        mock_call = MagicMock(return_value={"id": "call-1"})
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",  mock_put), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit", mock_call), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            place_collar_order("TSLA", rec, prompt=False, contracts_override=1)
        assert mock_put.call_args.kwargs["quantity"]  == 1   # override wins
        assert mock_call.call_args.kwargs["quantity"] == 1

    def test_put_failure_aborts_without_call(self):
        """If BUY PUT fails the SELL CALL is never attempted."""
        rec = _make_rec()
        mock_call = MagicMock(return_value={"id": "call-1"})
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value={"detail": "Insufficient funds"}), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit", mock_call), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            result = place_collar_order("TSLA", rec, prompt=False)
        assert result is False
        mock_call.assert_not_called()

    def test_call_failure_after_put_returns_false_with_guidance(self, capsys):
        """If SELL CALL fails after PUT is placed, user is told to complete manually."""
        rec = _make_rec()
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value={"id": "put-1"}), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit",
                   return_value={"detail": "This order introduces infinite risk."}), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            result = place_collar_order("TSLA", rec, prompt=False)
        assert result is False
        out = capsys.readouterr().out
        assert "put-1" in out            # PUT order ID shown
        assert "COMPLETE THE COLLAR" in out or "complete" in out.lower()

    def test_pending_call_orders_warning_shown(self, capsys):
        """Pre-flight check warns when there are pending SELL CALL orders."""
        rec = _make_rec()
        pending_order = {
            "chain_symbol": "TSLA",
            "state": "confirmed",
            "id": "pending-1",
            "legs": [{"side": "sell", "position_effect": "open"}],
        }
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",
                   return_value={"id": "put-1"}), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit",
                   return_value={"id": "call-1"}), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders",
                   return_value=[pending_order]):
            place_collar_order("TSLA", rec, prompt=False)
        out = capsys.readouterr().out
        assert "pending" in out.lower()

    def test_prompt_decline_returns_false(self, monkeypatch):
        rec = _make_rec()
        monkeypatch.setattr("builtins.input", lambda _: "n")
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            result = place_collar_order("TSLA", rec, prompt=True)
        assert result is False

    def test_prompt_accept_places_order(self, monkeypatch):
        rec = _make_rec()
        monkeypatch.setattr("builtins.input", lambda _: "y")
        mock_put  = MagicMock(return_value={"id": "put-1"})
        mock_call = MagicMock(return_value={"id": "call-1"})
        with patch("auth.login"), patch("auth.logout"), \
             patch("robin_stocks.robinhood.orders.order_buy_option_limit",  mock_put), \
             patch("robin_stocks.robinhood.orders.order_sell_option_limit", mock_call), \
             patch("robin_stocks.robinhood.orders.get_all_option_orders", return_value=[]):
            result = place_collar_order("TSLA", rec, prompt=True)
        assert result is True
        mock_put.assert_called_once()
        mock_call.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# find_collar_roll_options
# ─────────────────────────────────────────────────────────────────────────────

def _make_collar_rec(call_strike, put_strike, net=0.20):
    return {
        "call_leg": {"strike": call_strike, "mid": 3.0, "bid": 2.5, "ask": 3.5,
                     "expiration": "2026-07-18"},
        "put_leg":  {"strike": put_strike,  "mid": 2.5, "bid": 2.0, "ask": 3.0,
                     "expiration": "2026-07-18"},
        "net_gain_per_share": net,
        "expiration": "2026-07-18",
        "dte": 98,
    }


class TestFindCollarRollOptions:

    def _run(self, recs, min_call=0.0, min_put=0.0):
        """Run find_collar_roll_options with a mocked run_collar_on_demand."""
        base_result = {"recommendations": recs, "eligible_count": 1,
                       "symbol": "TSLA", "holding": {}}
        with patch("collar.run_collar_on_demand", return_value=base_result):
            return find_collar_roll_options("TSLA", 28, 182,
                                            min_call_strike=min_call,
                                            min_put_strike=min_put)

    def test_no_constraints_returns_all_recs(self):
        recs = [_make_collar_rec(300, 280), _make_collar_rec(310, 290)]
        result = self._run(recs, min_call=0.0, min_put=0.0)
        assert len(result["recommendations"]) == 2

    def test_filters_recs_below_call_strike(self):
        recs = [
            _make_collar_rec(300, 280),   # call 300 < 305 → excluded
            _make_collar_rec(310, 290),   # call 310 >= 305 → included
        ]
        result = self._run(recs, min_call=305.0, min_put=0.0)
        assert len(result["recommendations"]) == 1
        assert result["recommendations"][0]["call_leg"]["strike"] == 310

    def test_filters_recs_below_put_strike(self):
        recs = [
            _make_collar_rec(310, 280),   # put 280 < 285 → excluded
            _make_collar_rec(320, 295),   # put 295 >= 285 → included
        ]
        result = self._run(recs, min_call=0.0, min_put=285.0)
        assert len(result["recommendations"]) == 1
        assert result["recommendations"][0]["put_leg"]["strike"] == 295

    def test_both_constraints_must_be_met(self):
        recs = [
            _make_collar_rec(310, 290),  # call ok, put 290 < 292 → excluded
            _make_collar_rec(320, 295),  # both ok → included
        ]
        result = self._run(recs, min_call=305.0, min_put=292.0)
        assert len(result["recommendations"]) == 1
        assert result["recommendations"][0]["call_leg"]["strike"] == 320

    def test_fallback_returns_all_when_no_higher_strikes(self):
        recs = [_make_collar_rec(290, 270)]  # both below constraints
        result = self._run(recs, min_call=300.0, min_put=280.0)
        # Falls back to all recs and sets roll_constrained flag
        assert len(result["recommendations"]) == 1
        assert result.get("roll_constrained") is True

    def test_empty_recs_returns_empty(self):
        result = self._run([], min_call=300.0, min_put=280.0)
        assert result["recommendations"] == []
        assert not result.get("roll_constrained")
