"""
test_portfolio_spreads.py — Unit tests for spread detection in portfolio.py (v1.6)
===================================================================================
Tests the _match_spread_pairs() helper and load_open_spreads_detail_snapshot()
without any live Robinhood API calls.

Scenarios covered:
  _match_spread_pairs():
    - CCS (bear call spread): short call + long call same expiry → detected
    - PCS (bull put spread):  short put  + long put  same expiry → detected
    - No pair when single short call only (covered call, not spread)
    - No pair when same option type but no matching opposite leg
    - CCS requires short_strike > long_strike (otherwise not a bear call spread)
    - PCS requires short_strike < long_strike (otherwise not a bull put spread)
    - Mixed option types (short call + long put) → NOT matched as spread
    - Multiple symbols → each pair detected independently
    - BTC flag propagated from short leg's option_id
    - Quantity uses min(short_qty, long_qty)
    - Multiple contracts per symbol aggregated correctly

  load_open_spreads_detail_snapshot():
    - Returns [] when no snapshot files exist
    - Loads latest file (not an older one) when multiple exist
"""

import sys, os, json, tempfile, glob as _glob
from pathlib import Path
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# ── import the internal function we want to test ─────────────────────────────
from portfolio import _match_spread_pairs, load_open_spreads_detail_snapshot


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _leg(symbol, opt_type, pos_type, strike, expiration="2026-05-16",
         quantity=1, option_id="OPT001", purchase_price=2.50):
    return {
        "symbol":        symbol,
        "option_type":   opt_type,
        "pos_type":      pos_type,
        "strike":        strike,
        "expiration":    expiration,
        "quantity":      quantity,
        "option_id":     option_id,
        "purchase_price":purchase_price,
    }


# ─────────────────────────────────────────────────────────────────────────────
# _match_spread_pairs tests
# ─────────────────────────────────────────────────────────────────────────────

class TestMatchSpreadPairs:
    def test_ccs_detected_from_short_plus_long_call(self):
        """CCS (bear call): short call at lower strike + long call at higher strike."""
        legs = [
            _leg("TSLA", "call", "short", 280.0, option_id="SHORT1"),  # lower strike (sold)
            _leg("TSLA", "call", "long",  290.0, option_id="LONG1"),   # higher strike (bought)
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        p = pairs[0]
        assert p["type"] == "CCS"
        assert p["symbol"] == "TSLA"
        assert p["short_strike"] == 280.0
        assert p["long_strike"] == 290.0

    def test_pcs_detected_from_short_plus_long_put(self):
        """PCS (bull put): short put at higher strike + long put at lower strike."""
        legs = [
            _leg("NVDA", "put", "short", 725.0, option_id="SHORT2"),  # higher strike (sold)
            _leg("NVDA", "put", "long",  700.0, option_id="LONG2"),   # lower strike (bought)
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        p = pairs[0]
        assert p["type"] == "PCS"
        assert p["symbol"] == "NVDA"
        assert p["short_strike"] == 725.0
        assert p["long_strike"] == 700.0

    def test_single_short_call_not_a_spread(self):
        """A single short call (covered call) produces no spread pair."""
        legs = [_leg("AAPL", "call", "short", 200.0)]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_single_long_call_not_a_spread(self):
        """A single long call with no matching short produces no pair."""
        legs = [_leg("AAPL", "call", "long", 200.0)]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_reversed_call_legs_not_a_ccs(self):
        """CCS requires short_strike < long_strike; reversed (short > long) → no CCS."""
        legs = [
            # short strike 290 > long strike 280 → reversed, not a bear call spread
            _leg("AAPL", "call", "short", 290.0, option_id="S"),
            _leg("AAPL", "call", "long",  280.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        # short strike > long strike for calls → NOT a CCS
        assert pairs == []

    def test_reversed_put_legs_not_a_pcs(self):
        """PCS requires short_strike > long_strike; reversed (short < long) → no PCS."""
        legs = [
            # short strike 180 < long strike 190 → reversed, not a bull put spread
            _leg("AAPL", "put", "short", 180.0, option_id="S"),
            _leg("AAPL", "put", "long",  190.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        # short strike < long strike for puts → NOT a PCS
        assert pairs == []

    def test_mixed_types_not_matched(self):
        """Short call + long put (different option types) → no spread."""
        legs = [
            _leg("META", "call", "short", 600.0, option_id="SC"),
            _leg("META", "put",  "long",  550.0, option_id="LP"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_different_expirations_not_matched(self):
        """Legs with different expirations are not matched into a spread."""
        legs = [
            _leg("AMZN", "call", "short", 210.0, expiration="2026-05-16", option_id="S"),
            _leg("AMZN", "call", "long",  200.0, expiration="2026-06-20", option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_different_symbols_not_matched(self):
        """Legs from different symbols are not mixed."""
        legs = [
            _leg("AAPL", "call", "short", 210.0, option_id="S"),
            _leg("MSFT", "call", "long",  200.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_multiple_symbols_each_detected_independently(self):
        """Each symbol's spread pair is detected independently."""
        legs = [
            _leg("TSLA", "call", "short", 280.0, option_id="TS"),  # CCS: short < long
            _leg("TSLA", "call", "long",  290.0, option_id="TL"),
            _leg("NVDA", "put",  "short", 725.0, option_id="NS"),  # PCS: short > long
            _leg("NVDA", "put",  "long",  700.0, option_id="NL"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 2
        types = {p["symbol"]: p["type"] for p in pairs}
        assert types["TSLA"] == "CCS"
        assert types["NVDA"] == "PCS"

    def test_btc_flag_propagated_from_short_leg(self):
        """BTC flag is True when the short leg's option_id is in btc_option_ids."""
        legs = [
            _leg("AAPL", "call", "short", 200.0, option_id="OPEN_BTC"),  # CCS: short < long
            _leg("AAPL", "call", "long",  210.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids={"OPEN_BTC"})
        assert len(pairs) == 1
        assert pairs[0]["btc_order_exists"] is True

    def test_btc_flag_false_when_not_in_set(self):
        legs = [
            _leg("AAPL", "call", "short", 200.0, option_id="NO_BTC"),  # CCS: short < long
            _leg("AAPL", "call", "long",  210.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs[0]["btc_order_exists"] is False

    def test_quantity_is_min_of_both_legs(self):
        """Spread quantity = min(short_qty, long_qty)."""
        legs = [
            _leg("TSLA", "call", "short", 280.0, quantity=3, option_id="S"),  # CCS: short < long
            _leg("TSLA", "call", "long",  290.0, quantity=2, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        assert pairs[0]["quantity"] == 2

    def test_zero_strike_legs_ignored(self):
        """Legs with strike=0 are not used in spread matching."""
        legs = [
            _leg("AAPL", "call", "short", 0.0, option_id="S"),   # bad data
            _leg("AAPL", "call", "long",  200.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []

    def test_empty_expiration_legs_ignored(self):
        """Legs with empty expiration string are not used."""
        legs = [
            _leg("AAPL", "call", "short", 210.0, expiration="", option_id="S"),
            _leg("AAPL", "call", "long",  200.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert pairs == []


# ─────────────────────────────────────────────────────────────────────────────
# load_open_spreads_detail_snapshot tests
# ─────────────────────────────────────────────────────────────────────────────

class TestLoadOpenSpreadsSnapshot:
    def test_returns_empty_list_when_no_file_exists(self, tmp_path, monkeypatch):
        """Returns [] when no open_spreads_detail_*.json exists."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)
        result = load_open_spreads_detail_snapshot()
        assert result == []

    def test_loads_spreads_from_latest_file(self, tmp_path, monkeypatch):
        """Reads spreads list from the most recent file."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        spreads = [
            {"symbol": "TSLA", "type": "CCS", "short_strike": 290.0, "long_strike": 280.0,
             "expiration": "2026-05-16", "quantity": 1, "btc_order_exists": False}
        ]
        snap = tmp_path / "open_spreads_detail_20260401.json"
        snap.write_text(json.dumps({"pulled_at": "2026-04-01T02:30:00",
                                    "spreads": spreads}))
        result = load_open_spreads_detail_snapshot()
        assert len(result) == 1
        assert result[0]["symbol"] == "TSLA"
        assert result[0]["type"] == "CCS"

    def test_loads_most_recent_file_when_multiple_exist(self, tmp_path, monkeypatch):
        """When multiple files exist, loads the most recent (by name desc)."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)

        old_spreads = [{"symbol": "OLD", "type": "CCS", "short_strike": 100.0,
                        "long_strike": 90.0, "expiration": "2026-04-16",
                        "quantity": 1, "btc_order_exists": False}]
        new_spreads = [{"symbol": "NEW", "type": "PCS", "short_strike": 200.0,
                        "long_strike": 210.0, "expiration": "2026-05-16",
                        "quantity": 2, "btc_order_exists": False}]

        (tmp_path / "open_spreads_detail_20260401.json").write_text(
            json.dumps({"pulled_at": "2026-04-01T02:30:00", "spreads": old_spreads}))
        (tmp_path / "open_spreads_detail_20260404.json").write_text(
            json.dumps({"pulled_at": "2026-04-04T02:30:00", "spreads": new_spreads}))

        result = load_open_spreads_detail_snapshot()
        assert len(result) == 1
        assert result[0]["symbol"] == "NEW"   # most recent file

    def test_returns_empty_on_malformed_file(self, tmp_path, monkeypatch):
        """Returns [] gracefully when file is malformed JSON."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)
        (tmp_path / "open_spreads_detail_20260401.json").write_text("NOT JSON")
        result = load_open_spreads_detail_snapshot()
        assert result == []

    def test_returns_empty_when_spreads_key_missing(self, tmp_path, monkeypatch):
        """Returns [] when file is valid JSON but missing 'spreads' key."""
        import portfolio
        monkeypatch.setattr(portfolio, "SNAPSHOT_DIR", tmp_path)
        (tmp_path / "open_spreads_detail_20260401.json").write_text(
            json.dumps({"pulled_at": "2026-04-01T02:30:00"})  # no "spreads" key
        )
        result = load_open_spreads_detail_snapshot()
        assert result == []
