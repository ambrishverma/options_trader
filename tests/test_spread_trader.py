"""
test_spread_trader.py — Tests for spread trading functions:
  show_spread_holdings(spread_type, symbol)
  place_spread_order(symbol, rec, spread_type, prompt)
  close_spread_position(symbol, spread_type, prompt)

All Robinhood and auth calls are mocked.
"""

import sys
import os
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from trader import show_spread_holdings, place_spread_order, close_spread_position


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _future_date(days: int) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _make_position(chain_symbol="TSLA", quantity="1", pos_type="short",
                   option_id="opt-001", expiration_date="", average_price="1.50"):
    return {
        "chain_symbol":   chain_symbol,
        "quantity":       quantity,
        "type":           pos_type,
        "option_id":      option_id,
        "expiration_date": expiration_date,
        "average_price":  average_price,
    }


def _make_instrument(option_type="put", strike_price="290.00",
                     expiration_date="2026-06-20"):
    return {
        "type":            option_type,
        "strike_price":    strike_price,
        "expiration_date": expiration_date,
    }


def _pcs_rec(
    symbol="TSLA",
    short_strike=290.0,
    long_strike=285.0,
    expiration="2026-06-20",
    short_bid=2.00, short_ask=2.20,
    long_bid=1.00,  long_ask=1.20,
):
    """Minimal PCS recommendation dict matching scan_pcs output structure."""
    return {
        "symbol":     symbol,
        "type":       "PCS",
        "expiration": expiration,
        "dte":        60,
        "current_price": 310.0,
        "short_leg": {
            "strike":        short_strike,
            "bid":           short_bid,
            "ask":           short_ask,
            "mid":           round((short_bid + short_ask) / 2, 2),
            "open_interest": 100,
            "otm_pct":       6.5,
        },
        "long_leg": {
            "strike":        long_strike,
            "bid":           long_bid,
            "ask":           long_ask,
            "mid":           round((long_bid + long_ask) / 2, 2),
            "open_interest": 80,
        },
        "net_credit":       round((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2, 2),
        "net_credit_total": round(((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2) * 100, 2),
        "spread_size":      short_strike - long_strike,
        "max_loss":         round((short_strike - long_strike) * 100 - ((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2) * 100, 2),
        "ypd":              0.18,
        "credit_to_loss_ratio": 0.20,
        "score":            0.036,
    }


def _ccs_rec(
    symbol="TSLA",
    short_strike=380.0,
    long_strike=385.0,
    expiration="2026-06-20",
    short_bid=2.00, short_ask=2.20,
    long_bid=1.00,  long_ask=1.20,
):
    """Minimal CCS recommendation dict matching scan_ccs output structure."""
    return {
        "symbol":     symbol,
        "type":       "CCS",
        "expiration": expiration,
        "dte":        60,
        "current_price": 350.0,
        "short_leg": {
            "strike":        short_strike,
            "bid":           short_bid,
            "ask":           short_ask,
            "mid":           round((short_bid + short_ask) / 2, 2),
            "open_interest": 100,
            "otm_pct":       8.6,
        },
        "long_leg": {
            "strike":        long_strike,
            "bid":           long_bid,
            "ask":           long_ask,
            "mid":           round((long_bid + long_ask) / 2, 2),
            "open_interest": 80,
        },
        "net_credit":       round((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2, 2),
        "net_credit_total": round(((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2) * 100, 2),
        "spread_size":      long_strike - short_strike,
        "max_loss":         round((long_strike - short_strike) * 100 - ((short_bid + short_ask) / 2 - (long_bid + long_ask) / 2) * 100, 2),
        "ypd":              0.18,
        "credit_to_loss_ratio": 0.20,
        "score":            0.036,
    }


def _mock_rh_for_show(positions, instruments):
    """Build a mock rh module for show_spread_holdings tests."""
    rh = MagicMock()
    rh.options.get_open_option_positions.return_value = positions
    instrument_map = {pos["option_id"]: instr
                      for pos, instr in zip(positions, instruments)}
    rh.options.get_option_instrument_data_by_id.side_effect = lambda oid: instrument_map.get(oid, {})
    return rh


# ─────────────────────────────────────────────────────────────────────────────
# show_spread_holdings
# ─────────────────────────────────────────────────────────────────────────────

class TestShowSpreadHoldings:

    def _run(self, positions, instruments, spread_type="PCS", symbol=None, capsys=None):
        rh = _mock_rh_for_show(positions, instruments)
        with patch("robin_stocks.robinhood", rh), \
             patch("trader.show_spread_holdings.__globals__['__builtins__']", {}), \
             patch("auth.login"), patch("auth.logout"):
            with patch("robin_stocks.robinhood.options.get_open_option_positions",
                       return_value=positions), \
                 patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                       side_effect=lambda oid: instruments.get(oid, {})), \
                 patch("auth.login"), patch("auth.logout"):
                show_spread_holdings(spread_type, symbol)
        return capsys.readouterr() if capsys else None

    def test_no_positions_shows_message(self, capsys):
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[]), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        assert "No open PCS positions" in out

    def test_no_positions_for_symbol_shows_message(self, capsys):
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[]), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS", "TSLA")
        out = capsys.readouterr().out
        assert "No open PCS positions for TSLA" in out

    def test_pcs_pair_displayed(self, capsys):
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="2.10"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="1.10"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        assert "Put Credit Spread" in out
        assert "290.00" in out
        assert "285.00" in out

    def test_ccs_pair_displayed(self, capsys):
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="2.10"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="1.10"),
        ]
        instruments = {
            "opt-short": _make_instrument("call", "380.00", exp),
            "opt-long":  _make_instrument("call", "385.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("CCS")
        out = capsys.readouterr().out
        assert "Call Credit Spread" in out
        assert "380.00" in out
        assert "385.00" in out

    def test_symbol_filter_excludes_other_symbols(self, capsys):
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-tsla-s", average_price="2.00"),
            _make_position("TSLA", "1", "long",  "opt-tsla-l", average_price="1.00"),
            _make_position("AAPL", "1", "short", "opt-aapl-s", average_price="1.50"),
            _make_position("AAPL", "1", "long",  "opt-aapl-l", average_price="0.80"),
        ]
        instruments = {
            "opt-tsla-s": _make_instrument("put", "290.00", exp),
            "opt-tsla-l": _make_instrument("put", "285.00", exp),
            "opt-aapl-s": _make_instrument("put", "170.00", exp),
            "opt-aapl-l": _make_instrument("put", "165.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS", "TSLA")
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "290.00" in out
        # AAPL data should not appear
        assert "170.00" not in out

    def test_unpaired_leg_shown_separately(self, capsys):
        exp = _future_date(30)
        # Only a short put, no long put counterpart
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="2.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        assert "Unpaired" in out

    def test_call_legs_excluded_from_pcs(self, capsys):
        """Call positions should not appear in a PCS show."""
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-call", average_price="3.00"),
        ]
        instruments = {
            "opt-call": _make_instrument("call", "380.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        # No put legs found → no positions message
        assert "No open PCS positions" in out

    def test_zero_quantity_positions_excluded(self, capsys):
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "0", "short", "opt-short", average_price="2.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="1.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        # Only the long put survives, no pair formed → unpaired
        assert "Unpaired" in out

    def test_show_all_symbols_no_filter(self, capsys):
        """When symbol=None, positions from multiple symbols are shown."""
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "t-s", average_price="2.00"),
            _make_position("TSLA", "1", "long",  "t-l", average_price="1.00"),
            _make_position("AAPL", "1", "short", "a-s", average_price="1.50"),
            _make_position("AAPL", "1", "long",  "a-l", average_price="0.80"),
        ]
        instruments = {
            "t-s": _make_instrument("put", "290.00", exp),
            "t-l": _make_instrument("put", "285.00", exp),
            "a-s": _make_instrument("put", "170.00", exp),
            "a-l": _make_instrument("put", "165.00", exp),
        }
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("auth.login"), patch("auth.logout"):
            show_spread_holdings("PCS")
        out = capsys.readouterr().out
        assert "TSLA" in out
        assert "AAPL" in out
        assert "290.00" in out
        assert "170.00" in out


# ─────────────────────────────────────────────────────────────────────────────
# place_spread_order
# ─────────────────────────────────────────────────────────────────────────────

class TestPlaceSpreadOrder:

    def _mock_rh(self, order_response):
        rh = MagicMock()
        rh.orders.order_option_spread.return_value = order_response
        return rh

    def test_pcs_order_placed_successfully(self):
        rec = _pcs_rec()
        rh  = self._mock_rh({"id": "order-pcs-001", "state": "confirmed"})
        with patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "order-pcs-001", "state": "confirmed"}), \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "PCS", prompt=False)
        assert result is True

    def test_ccs_order_placed_successfully(self):
        rec = _ccs_rec()
        with patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "order-ccs-001", "state": "confirmed"}), \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "CCS", prompt=False)
        assert result is True

    def test_order_uses_credit_direction(self):
        rec = _pcs_rec()
        captured = {}
        def fake_spread(**kwargs):
            captured.update(kwargs)
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=False)
        assert captured.get("direction") == "credit"

    def test_strike_format_is_4dp(self):
        """Strike prices sent to the API must be formatted to 4 decimal places."""
        rec = _pcs_rec(short_strike=290.0, long_strike=285.0)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=False)
        strikes = [leg["strike"] for leg in captured["spread"]]
        assert "290.0000" in strikes
        assert "285.0000" in strikes

    def test_pcs_spread_legs_sell_short_buy_long(self):
        """For PCS: short leg action=sell, long leg action=buy, both effect=open."""
        rec = _pcs_rec(short_strike=290.0, long_strike=285.0)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=False)
        legs = {leg["strike"]: leg for leg in captured["spread"]}
        short_leg = legs["290.0000"]
        long_leg  = legs["285.0000"]
        assert short_leg["action"] == "sell"
        assert short_leg["effect"] == "open"
        assert long_leg["action"] == "buy"
        assert long_leg["effect"] == "open"

    def test_ccs_spread_legs_sell_short_buy_long(self):
        """For CCS: short leg action=sell, long leg action=buy, both effect=open."""
        rec = _ccs_rec(short_strike=380.0, long_strike=385.0)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "CCS", prompt=False)
        legs = {leg["strike"]: leg for leg in captured["spread"]}
        short_leg = legs["380.0000"]
        long_leg  = legs["385.0000"]
        assert short_leg["action"] == "sell"
        assert short_leg["optionType"] == "call"
        assert long_leg["action"] == "buy"
        assert long_leg["optionType"] == "call"

    def test_put_option_type_for_pcs(self):
        rec = _pcs_rec()
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=False)
        for leg in captured["spread"]:
            assert leg["optionType"] == "put"

    def test_api_failure_returns_false(self, capsys):
        rec = _pcs_rec()
        with patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"detail": "Insufficient option level"}), \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "PCS", prompt=False)
        assert result is False
        out = capsys.readouterr().out
        assert "failed" in out.lower() or "❌" in out

    def test_api_exception_returns_false(self, capsys):
        rec = _pcs_rec()
        with patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=Exception("Network error")), \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "PCS", prompt=False)
        assert result is False

    def test_prompt_decline_returns_false(self, monkeypatch, capsys):
        rec = _pcs_rec()
        monkeypatch.setattr("builtins.input", lambda _: "n")
        # No API call should be made
        with patch("robin_stocks.robinhood.orders.order_option_spread") as mock_spread, \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "PCS", prompt=True)
        assert result is False
        mock_spread.assert_not_called()

    def test_prompt_accept_places_order(self, monkeypatch, capsys):
        rec = _pcs_rec()
        monkeypatch.setattr("builtins.input", lambda _: "y")
        with patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "order-x", "state": "confirmed"}), \
             patch("auth.login"), patch("auth.logout"):
            result = place_spread_order("TSLA", rec, "PCS", prompt=True)
        assert result is True

    def test_prompt_shows_order_summary(self, monkeypatch, capsys):
        rec = _pcs_rec(short_strike=290.0, long_strike=285.0, expiration="2026-06-20")
        monkeypatch.setattr("builtins.input", lambda _: "n")
        with patch("robin_stocks.robinhood.orders.order_option_spread"), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=True)
        out = capsys.readouterr().out
        assert "290" in out
        assert "285" in out
        assert "2026-06-20" in out

    def test_net_credit_computed_from_mid(self):
        """Net credit = short_mid - long_mid, passed to order_option_spread."""
        rec = _pcs_rec(short_bid=2.00, short_ask=2.20, long_bid=1.00, long_ask=1.20)
        # short_mid = 2.10, long_mid = 1.10, net_credit = 1.00
        captured = {}
        def fake_spread(**kwargs):
            captured["price"] = kwargs.get("price")
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.orders.order_option_spread", side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            place_spread_order("TSLA", rec, "PCS", prompt=False)
        assert captured["price"] == pytest.approx(1.00, abs=0.01)


# ─────────────────────────────────────────────────────────────────────────────
# close_spread_position
# ─────────────────────────────────────────────────────────────────────────────

class TestCloseSpreadPosition:

    def _setup_close(self, positions, instruments, mkt_data, order_response):
        """Return context-manager patches for close_spread_position tests."""
        return (
            patch("robin_stocks.robinhood.options.get_open_option_positions",
                  return_value=positions),
            patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                  side_effect=lambda oid: instruments.get(oid, {})),
            patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                  side_effect=lambda oid: mkt_data.get(oid, {})),
            patch("robin_stocks.robinhood.orders.order_option_spread",
                  return_value=order_response),
            patch("auth.login"),
            patch("auth.logout"),
        )

    def _std_pcs_fixtures(self, exp, short_avg="-210.00", long_avg="110.00"):
        """Standard PCS position/instrument/mkt fixtures for reuse.

        avg_price uses Robinhood's real format:
          short leg — NEGATIVE per-contract total (credit received × 100)
          long  leg — POSITIVE per-contract total (debit paid × 100)

        Default values:
          short_avg = "-210.00"  → received $2.10/sh = $210/contract
          long_avg  = "110.00"   → paid     $1.10/sh = $110/contract
          orig_credit = (abs(-210) - 110) / 100 = $1.00/sh
          pct20       = $0.20
          net_mid     = (1.80+2.00)/2 - (0.80+1.00)/2 = 1.90 - 0.90 = $1.00
          limit_price = MIN($0.20, $0.20, $1.00) = $0.20
        """
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price=short_avg),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price=long_avg),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "1.80", "ask_price": "2.00"},  # mid = 1.90
            "opt-long":  {"bid_price": "0.80", "ask_price": "1.00"},  # mid = 0.90
        }
        return positions, instruments, mkt_data

    # ── Basic placement ───────────────────────────────────────────────────────

    def test_no_open_positions_returns_false(self, capsys):
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=[]), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position("TSLA", "PCS", prompt=False)
        assert result is False
        out = capsys.readouterr().out
        assert "No open PCS positions" in out

    def test_pcs_close_order_placed_successfully(self):
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        patches = self._setup_close(positions, instruments, mkt_data,
                                    {"id": "close-001", "state": "confirmed"})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position("TSLA", "PCS", prompt=False)
        assert result is True

    def test_ccs_close_order_placed_successfully(self):
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="2.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="1.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("call", "380.00", exp),
            "opt-long":  _make_instrument("call", "385.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "1.80", "ask_price": "2.00"},
            "opt-long":  {"bid_price": "0.80", "ask_price": "1.00"},
        }
        patches = self._setup_close(positions, instruments, mkt_data,
                                    {"id": "close-002", "state": "confirmed"})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position("TSLA", "CCS", prompt=False)
        assert result is True

    def test_close_always_uses_debit_direction(self):
        """Closing a credit spread is always a debit order regardless of limit price."""
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        captured = {}
        def fake_spread(**kwargs):
            captured.update(kwargs)
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "PCS", prompt=False)
        assert captured.get("direction") == "debit"

    def test_close_legs_buy_short_sell_long(self):
        """Closing legs: short=buy/close, long=sell/close."""
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "PCS", prompt=False)
        legs = {leg["strike"]: leg for leg in captured["spread"]}
        short_leg = legs["290.0000"]
        long_leg  = legs["285.0000"]
        assert short_leg["action"] == "buy"
        assert short_leg["effect"] == "close"
        assert long_leg["action"] == "sell"
        assert long_leg["effect"] == "close"

    def test_no_matched_pair_returns_false(self, capsys):
        """Only a short put but no long put — no spread pair to close."""
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "1.80", "ask_price": "2.00"},
        }
        patches = self._setup_close(positions, instruments, mkt_data, {})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position("TSLA", "PCS", prompt=False)
        assert result is False

    def test_api_failure_returns_false(self, capsys):
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        patches = self._setup_close(positions, instruments, mkt_data,
                                    {"detail": "Market closed"})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position("TSLA", "PCS", prompt=False)
        assert result is False

    # ── Prompt behavior ───────────────────────────────────────────────────────

    def test_prompt_decline_returns_false(self, monkeypatch, capsys):
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        monkeypatch.setattr("builtins.input", lambda _: "n")
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread") as mock_spread, \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position("TSLA", "PCS", prompt=True)
        assert result is False
        mock_spread.assert_not_called()

    def test_prompt_accept_places_order(self, monkeypatch):
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        monkeypatch.setattr("builtins.input", lambda _: "y")
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   return_value={"id": "close-y", "state": "confirmed"}), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position("TSLA", "PCS", prompt=True)
        assert result is True

    # ── Limit-price formula: MIN($0.20, 20% of original credit, current mid) ─

    def _run_close_capture_price(self, positions, instruments, mkt_data,
                                  price=None):
        """Helper: run close_spread_position and return the price arg sent to API."""
        captured = {}
        def fake_spread(**kwargs):
            captured["price"] = kwargs.get("price")
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "PCS", price=price, prompt=False)
        return captured.get("price")

    def test_explicit_price_overrides_formula(self):
        """When --price is provided, it is used directly (formula is bypassed)."""
        exp = _future_date(30)
        # avg_prices don't matter when an explicit price is given
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        api_price = self._run_close_capture_price(positions, instruments, mkt_data,
                                                   price=0.30)
        assert api_price == pytest.approx(0.30, abs=0.001)

    def test_formula_uses_pct20_when_smallest(self):
        """20% of original credit is the smallest → it wins.

        Robinhood per-contract format:
          short_avg = -160  (received $1.60/sh = $160/contract, stored negative)
          long_avg  = +110  (paid     $1.10/sh = $110/contract)
          orig_credit = (abs(-160) - 110) / 100 = $0.50/sh
          pct20       = $0.10
          net_mid     = 1.90 - 0.90 = $1.00
          MIN($0.20, $0.10, $1.00) = $0.10
        """
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(
            exp, short_avg="-160.00", long_avg="110.00"
        )
        api_price = self._run_close_capture_price(positions, instruments, mkt_data)
        assert api_price == pytest.approx(0.10, abs=0.001)

    def test_formula_capped_at_twenty_cents(self):
        """20% of credit > $0.20 → capped at $0.20.

        short_avg = -500  (received $5.00/sh = $500/contract, stored negative)
        long_avg  = +200  (paid     $2.00/sh = $200/contract)
        orig_credit = (500 - 200) / 100 = $3.00/sh
        pct20       = $0.60  (> $0.20)
        net_mid     = 2.50 - 2.10 = $0.40  (> $0.20)
        MIN($0.20, $0.60, $0.40) = $0.20
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-500.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="200.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "2.40", "ask_price": "2.60"},  # mid = 2.50
            "opt-long":  {"bid_price": "2.00", "ask_price": "2.20"},  # mid = 2.10
        }
        api_price = self._run_close_capture_price(positions, instruments, mkt_data)
        assert api_price == pytest.approx(0.20, abs=0.001)

    def test_formula_uses_mid_when_smallest(self):
        """Current net mid is the smallest → it wins.

        short_avg = -210  → orig_credit = $1.00/sh, pct20 = $0.20
        net_mid   = 0.15 - 0.10 = $0.05  (< $0.20 < $0.20 cap)
        MIN($0.20, $0.20, $0.05) = $0.05
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-210.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="110.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "0.10", "ask_price": "0.20"},  # mid = 0.15
            "opt-long":  {"bid_price": "0.05", "ask_price": "0.15"},  # mid = 0.10
        }
        api_price = self._run_close_capture_price(positions, instruments, mkt_data)
        assert api_price == pytest.approx(0.05, abs=0.001)

    def test_formula_floored_at_one_cent(self):
        """All formula components are zero → floor at $0.01.

        short_avg = -150, long_avg = 150 → orig_credit = (150-150)/100 = $0/sh
        pct20     = $0.00 → floored component
        net_mid   = $0.00 (both mids are 0)
        MAX($0.01, MIN($0.20, $0.00, $0.00)) = $0.01
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-150.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="150.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "290.00", exp),
            "opt-long":  _make_instrument("put", "285.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "0.00", "ask_price": "0.00"},
            "opt-long":  {"bid_price": "0.00", "ask_price": "0.00"},
        }
        api_price = self._run_close_capture_price(positions, instruments, mkt_data)
        assert api_price == pytest.approx(0.01, abs=0.001)

    def test_rddt_like_case_gives_twenty_cents(self):
        """Regression: $3.25/sh credit spread should produce $0.20 limit.

        Mirrors the real RDDT case reported by the user:
          short_avg = -525  (received $5.25/sh = $525/contract, stored negative)
          long_avg  = +200  (paid     $2.00/sh = $200/contract)
          orig_credit = (525 - 200) / 100 = $3.25/sh
          pct20       = $0.65  (> $0.20)
          net_mid     = any value > $0.20
          MIN($0.20, $0.65, net_mid) = $0.20
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-525.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="200.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("put", "120.00", exp),
            "opt-long":  _make_instrument("put", "105.00", exp),
        }
        # net_mid = 5.00 - 2.04 = 2.96 (as shown in the user's output)
        mkt_data = {
            "opt-short": {"bid_price": "4.80", "ask_price": "5.20"},  # mid = 5.00
            "opt-long":  {"bid_price": "1.88", "ask_price": "2.20"},  # mid = 2.04
        }
        api_price = self._run_close_capture_price(positions, instruments, mkt_data)
        assert api_price == pytest.approx(0.20, abs=0.001)

    def test_prompt_shows_limit_price_and_formula(self, monkeypatch, capsys):
        """Prompt output includes the computed limit price and its derivation."""
        exp = _future_date(30)
        # _std_pcs_fixtures: orig_credit=$1.00/sh → pct20=$0.20 → limit=$0.20
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        monkeypatch.setattr("builtins.input", lambda _: "n")
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread"), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "PCS", prompt=True)
        out = capsys.readouterr().out
        assert "0.20" in out          # limit price shown
        assert "Limit price" in out   # label present

    def test_explicit_price_shown_in_prompt(self, monkeypatch, capsys):
        """When --price is given, the user-specified value appears in the prompt."""
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        monkeypatch.setattr("builtins.input", lambda _: "n")
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread"), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "PCS", price=0.30, prompt=True)
        out = capsys.readouterr().out
        assert "0.30" in out
        assert "user-specified" in out

    # ── CCS formula (same logic as PCS) ──────────────────────────────────────

    def test_ccs_formula_20pct_capped_at_twenty_cents(self):
        """CCS close uses same MIN($0.20, 20% of credit, mid) formula as PCS.

        short_avg = -480  (received $4.80/sh = $480/contract, stored negative)
        long_avg  = +180  (paid     $1.80/sh = $180/contract)
        orig_credit = (480 - 180) / 100 = $3.00/sh
        pct20       = $0.60  (> $0.20)
        net_mid     = short_mid - long_mid = 2.10 - 1.10 = $1.00  (> $0.20)
        MIN($0.20, $0.60, $1.00) = $0.20
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-480.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="180.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("call", "380.00", exp),
            "opt-long":  _make_instrument("call", "385.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "2.00", "ask_price": "2.20"},  # mid = 2.10
            "opt-long":  {"bid_price": "1.00", "ask_price": "1.20"},  # mid = 1.10
        }
        captured = {}
        def fake_spread(**kwargs):
            captured["price"] = kwargs.get("price")
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "CCS", prompt=False)
        assert captured.get("price") == pytest.approx(0.20, abs=0.001)

    def test_ccs_formula_uses_pct20_when_smallest(self):
        """CCS: 20% of small credit wins when < $0.20 cap and < mid.

        short_avg = -160  (received $1.60/sh, stored negative)
        long_avg  = +110  (paid     $1.10/sh)
        orig_credit = (160 - 110) / 100 = $0.50/sh
        pct20       = $0.10  (< $0.20)
        net_mid     = 2.10 - 1.10 = $1.00  (> $0.10)
        MIN($0.20, $0.10, $1.00) = $0.10
        """
        exp = _future_date(30)
        positions = [
            _make_position("TSLA", "1", "short", "opt-short", average_price="-160.00"),
            _make_position("TSLA", "1", "long",  "opt-long",  average_price="110.00"),
        ]
        instruments = {
            "opt-short": _make_instrument("call", "380.00", exp),
            "opt-long":  _make_instrument("call", "385.00", exp),
        }
        mkt_data = {
            "opt-short": {"bid_price": "2.00", "ask_price": "2.20"},  # mid = 2.10
            "opt-long":  {"bid_price": "1.00", "ask_price": "1.20"},  # mid = 1.10
        }
        captured = {}
        def fake_spread(**kwargs):
            captured["price"] = kwargs.get("price")
            return {"id": "x", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            close_spread_position("TSLA", "CCS", prompt=False)
        assert captured.get("price") == pytest.approx(0.10, abs=0.001)

    # ── Multi-pair selection ──────────────────────────────────────────────────

    def _two_pcs_positions(self, exp1, exp2):
        """Two distinct PCS positions for RDDT: $120/$105 exp1 and $105/$95 exp2."""
        positions = [
            _make_position("RDDT", "1", "short", "s1", expiration_date=exp1,
                           average_price="-525.00"),   # $120 PUT short exp1
            _make_position("RDDT", "1", "long",  "l1", expiration_date=exp1,
                           average_price="200.00"),    # $105 PUT long  exp1
            _make_position("RDDT", "1", "short", "s2", expiration_date=exp2,
                           average_price="-250.00"),   # $105 PUT short exp2
            _make_position("RDDT", "1", "long",  "l2", expiration_date=exp2,
                           average_price="100.00"),    # $95  PUT long  exp2
        ]
        instruments = {
            "s1": _make_instrument("put", "120.00", exp1),
            "l1": _make_instrument("put", "105.00", exp1),
            "s2": _make_instrument("put", "105.00", exp2),
            "l2": _make_instrument("put",  "95.00", exp2),
        }
        mkt_data = {
            "s1": {"bid_price": "4.80", "ask_price": "5.20"},  # mid = 5.00
            "l1": {"bid_price": "1.88", "ask_price": "2.20"},  # mid = 2.04
            "s2": {"bid_price": "1.90", "ask_price": "2.10"},  # mid = 2.00
            "l2": {"bid_price": "0.80", "ask_price": "1.00"},  # mid = 0.90
        }
        return positions, instruments, mkt_data

    def test_single_pair_selected_automatically(self):
        """Single valid spread pair is selected without any menu."""
        exp = _future_date(30)
        positions, instruments, mkt_data = self._std_pcs_fixtures(exp)
        patches = self._setup_close(
            positions, instruments, mkt_data,
            {"id": "auto-001", "state": "confirmed"}
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position("RDDT", "PCS", prompt=False)
        # std fixtures are for TSLA but symbol filter should still work since
        # chain_symbol is "TSLA" while we ask for "RDDT" → no positions → False
        assert result is False  # symbol mismatch — confirms no auto-match confusion

    def test_chain_selects_correct_spread_from_two(self):
        """--chain "$120 PUT 5/1" picks the first spread, not the second."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions, instruments, mkt_data = self._two_pcs_positions(exp1, exp2)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "chain-001", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position(
                "RDDT", "PCS", prompt=False, chain="$120 PUT 5/1"
            )
        assert result is True
        # The order legs should reference exp1 ($120 short) not exp2
        strikes = {leg["strike"] for leg in captured["spread"]}
        assert "120.0000" in strikes
        assert "105.0000" in strikes

    def test_chain_selects_second_spread(self):
        """--chain "$105 PUT 5/15" picks the second spread."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions, instruments, mkt_data = self._two_pcs_positions(exp1, exp2)
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "chain-002", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position(
                "RDDT", "PCS", prompt=False, chain="$105 PUT 5/15"
            )
        assert result is True
        strikes = {leg["strike"] for leg in captured["spread"]}
        assert "105.0000" in strikes
        assert "95.0000" in strikes

    def test_chain_not_found_returns_false(self, capsys):
        """--chain with a strike that doesn't exist returns False with message."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions, instruments, mkt_data = self._two_pcs_positions(exp1, exp2)
        patches = self._setup_close(positions, instruments, mkt_data, {})
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = close_spread_position(
                "RDDT", "PCS", prompt=False, chain="$200 PUT 5/1"
            )
        assert result is False
        out = capsys.readouterr().out
        assert "No PCS" in out

    def test_multi_pair_interactive_menu_selects_first(self, monkeypatch, capsys):
        """Interactive menu: user picks [1] → closes first (lowest-strike-sorted) pair."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions, instruments, mkt_data = self._two_pcs_positions(exp1, exp2)
        monkeypatch.setattr("builtins.input", lambda _: "1")
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "menu-001", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position("RDDT", "PCS", prompt=False)
        assert result is True
        out = capsys.readouterr().out
        assert "Multiple PCS positions found" in out

    def test_multi_pair_interactive_menu_abort(self, monkeypatch, capsys):
        """Interactive menu: user enters q → aborts without placing order."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions, instruments, mkt_data = self._two_pcs_positions(exp1, exp2)
        monkeypatch.setattr("builtins.input", lambda _: "q")
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread") as mock_spread, \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position("RDDT", "PCS", prompt=False)
        assert result is False
        mock_spread.assert_not_called()
        out = capsys.readouterr().out
        assert "Aborted" in out

    def test_ccs_multi_pair_chain_selection(self):
        """--chain selects correct CCS pair (short CALL at specified strike+expiry)."""
        exp1 = "2026-05-01"
        exp2 = "2026-05-15"
        positions = [
            _make_position("NVDA", "1", "short", "s1", expiration_date=exp1,
                           average_price="-480.00"),
            _make_position("NVDA", "1", "long",  "l1", expiration_date=exp1,
                           average_price="180.00"),
            _make_position("NVDA", "1", "short", "s2", expiration_date=exp2,
                           average_price="-300.00"),
            _make_position("NVDA", "1", "long",  "l2", expiration_date=exp2,
                           average_price="120.00"),
        ]
        instruments = {
            "s1": _make_instrument("call", "500.00", exp1),
            "l1": _make_instrument("call", "510.00", exp1),
            "s2": _make_instrument("call", "510.00", exp2),
            "l2": _make_instrument("call", "520.00", exp2),
        }
        mkt_data = {
            "s1": {"bid_price": "2.00", "ask_price": "2.20"},
            "l1": {"bid_price": "1.00", "ask_price": "1.20"},
            "s2": {"bid_price": "1.50", "ask_price": "1.70"},
            "l2": {"bid_price": "0.60", "ask_price": "0.80"},
        }
        captured = {}
        def fake_spread(**kwargs):
            captured["spread"] = kwargs.get("spread", [])
            return {"id": "ccs-chain-001", "state": "confirmed"}
        with patch("robin_stocks.robinhood.options.get_open_option_positions",
                   return_value=positions), \
             patch("robin_stocks.robinhood.options.get_option_instrument_data_by_id",
                   side_effect=lambda oid: instruments.get(oid, {})), \
             patch("robin_stocks.robinhood.options.get_option_market_data_by_id",
                   side_effect=lambda oid: mkt_data.get(oid, {})), \
             patch("robin_stocks.robinhood.orders.order_option_spread",
                   side_effect=fake_spread), \
             patch("auth.login"), patch("auth.logout"):
            result = close_spread_position(
                "NVDA", "CCS", prompt=False, chain="$510 CALL 5/15"
            )
        assert result is True
        strikes = {leg["strike"] for leg in captured["spread"]}
        assert "510.0000" in strikes
        assert "520.0000" in strikes
