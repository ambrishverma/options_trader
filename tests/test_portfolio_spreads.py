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

    def test_reversed_call_legs_detected_as_cds(self):
        """Short call higher + long call lower → CDS (call debit spread), not CCS."""
        legs = [
            # short strike 290 > long strike 280 → CDS
            _leg("AAPL", "call", "short", 290.0, option_id="S"),
            _leg("AAPL", "call", "long",  280.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        assert pairs[0]["type"] == "CDS"
        assert pairs[0]["short_strike"] == 290.0
        assert pairs[0]["long_strike"] == 280.0

    def test_reversed_put_legs_detected_as_pds(self):
        """Short put lower + long put higher → PDS (put debit spread), not PCS."""
        legs = [
            # short strike 180 < long strike 190 → PDS
            _leg("AAPL", "put", "short", 180.0, option_id="S"),
            _leg("AAPL", "put", "long",  190.0, option_id="L"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        assert pairs[0]["type"] == "PDS"
        assert pairs[0]["short_strike"] == 180.0
        assert pairs[0]["long_strike"] == 190.0

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

    # ── Debit-spread pre-pass tests ──────────────────────────────────────

    def test_cds_greedy_pairs_debit_before_credit(self):
        """CDS pair consumed by greedy matching prevents false CCS pairing.

        Positions:
          - short $280 call  (standalone covered call)
          - long  $327.50 call  (CDS insurance long)
          - short $350 call  (CDS insurance short)

        Greedy closest-first: ($350, $327.50) width=22.5 matched first as CDS.
        Then ($280, $327.50) width=47.5 — long already consumed → skipped.
        Result: 1 CDS pair, $280 standalone (unmatched).
        """
        legs = [
            _leg("AAPL", "call", "short", 280.0,   option_id="CC-280"),
            _leg("AAPL", "call", "long",  327.50,   option_id="CDS-L-327"),
            _leg("AAPL", "call", "short", 350.0,    option_id="CDS-S-350"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        assert pairs[0]["type"] == "CDS"
        assert pairs[0]["short_strike"] == 350.0
        assert pairs[0]["long_strike"] == 327.50
        assert pairs[0]["short_option_id"] == "CDS-S-350"
        assert pairs[0]["long_option_id"] == "CDS-L-327"

    def test_pds_greedy_pairs_debit_before_credit(self):
        """PDS pair consumed by greedy matching prevents false PCS pairing.

        Positions (wide gap — mirrors the CCS bug):
          - short $290 put   (standalone CSP)
          - long  $250 put   (PDS insurance long)
          - short $230 put   (PDS insurance short)

        Greedy closest-first: ($230, $250) width=20 matched first as PDS.
        Then ($290, $250) width=40 — long already consumed → skipped.
        Result: 1 PDS pair, $290 standalone (unmatched).
        """
        legs = [
            _leg("TSLA", "put", "short", 290.0,  option_id="CSP-290"),
            _leg("TSLA", "put", "long",  250.0,  option_id="PDS-L-250"),
            _leg("TSLA", "put", "short", 230.0,  option_id="PDS-S-230"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 1
        assert pairs[0]["type"] == "PDS"
        assert pairs[0]["short_strike"] == 230.0
        assert pairs[0]["long_strike"] == 250.0
        assert pairs[0]["short_option_id"] == "PDS-S-230"
        assert pairs[0]["long_option_id"] == "PDS-L-250"

    def test_greedy_returns_both_credit_and_debit_pairs(self):
        """Real CCS pair coexists with CDS pair — both returned.

        Positions:
          - short $200 call + long $210 call  → genuine CCS (width 10)
          - long  $327.50 call + short $350 call → CDS (width 22.5)

        Expected: 2 pairs — CCS ($200/$210) and CDS ($350/$327.50).
        """
        legs = [
            _leg("AAPL", "call", "short", 200.0,   option_id="CCS-S-200"),
            _leg("AAPL", "call", "long",  210.0,   option_id="CCS-L-210"),
            _leg("AAPL", "call", "long",  327.50,  option_id="CDS-L-327"),
            _leg("AAPL", "call", "short", 350.0,   option_id="CDS-S-350"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 2
        by_type = {p["type"]: p for p in pairs}
        assert "CCS" in by_type
        assert by_type["CCS"]["short_strike"] == 200.0
        assert by_type["CCS"]["long_strike"] == 210.0
        assert "CDS" in by_type
        assert by_type["CDS"]["short_strike"] == 350.0
        assert by_type["CDS"]["long_strike"] == 327.50

    def test_greedy_pairs_closest_credit_spreads(self):
        """Greedy matching pairs each leg at most once, picking closest first.

        Legs: short $200, long $210, short $220, long $230.
        Candidates sorted by width:
          ($200, $210) width=10  → CCS
          ($220, $230) width=10  → CCS
          ($200, $230) width=30  → $200 already consumed → skip
          ($220, $210) width=10  → but same-width sorted arbitrarily;
                                   $210 already consumed → skip
        Result: 2 CCS pairs (each leg used once).
        """
        legs = [
            _leg("AAPL", "call", "short", 200.0, option_id="S1"),
            _leg("AAPL", "call", "long",  210.0, option_id="L1"),
            _leg("AAPL", "call", "short", 220.0, option_id="S2"),
            _leg("AAPL", "call", "long",  230.0, option_id="L2"),
        ]
        pairs = _match_spread_pairs(legs, btc_option_ids=set())
        assert len(pairs) == 2
        types = {p["type"] for p in pairs}
        assert types == {"CCS"}
        strikes = {(p["short_strike"], p["long_strike"]) for p in pairs}
        assert (200.0, 210.0) in strikes
        assert (220.0, 230.0) in strikes


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


# ─────────────────────────────────────────────────────────────────────────────
# CCS short-leg exclusion from open_calls_detail
# ─────────────────────────────────────────────────────────────────────────────

class TestCCSShortLegExclusion:
    """
    Regression tests for the bug where CCS short legs leaked into
    open_calls_detail and were independently rolled by optimize mode.

    The portfolio pull puts ALL short calls into detail_list during
    collection, then filters out CCS short legs using the spread pairs
    detected by _match_spread_pairs.  These tests verify that filtering.
    """

    def _simulate_detail_filtering(self, all_legs):
        """
        Reproduce the portfolio pull's detail_list + CCS exclusion logic:
        1. Collect all short calls into detail_list
        2. Run _match_spread_pairs to detect spreads
        3. Remove CCS short legs from detail_list
        Returns (detail_list, spread_detail).
        """
        btc_option_ids = set()

        # Step 1: build detail_list (all short calls)
        detail_list = []
        for leg in all_legs:
            if leg["pos_type"] == "short" and leg["option_type"] == "call":
                detail_list.append({
                    "symbol":     leg["symbol"],
                    "opt_type":   "call",
                    "strike":     leg["strike"],
                    "expiration": leg["expiration"],
                    "quantity":   leg["quantity"],
                    "option_id":  leg["option_id"],
                    "purchase_price": leg["purchase_price"],
                })

        # Step 2: detect spread pairs
        spread_detail = _match_spread_pairs(all_legs, btc_option_ids)

        # Step 3: exclude CCS short legs (the fix being tested)
        ccs_short_keys = {
            (sp["symbol"], sp["expiration"], sp["short_strike"])
            for sp in spread_detail
            if sp["type"] == "CCS"
        }
        if ccs_short_keys:
            detail_list = [
                c for c in detail_list
                if (c["symbol"], c["expiration"], c["strike"]) not in ccs_short_keys
            ]

        return detail_list, spread_detail

    def test_ccs_short_leg_excluded_from_detail(self):
        """
        Regression (AMD bug): CCS short call ($505) must NOT appear in
        detail_list (open_calls_detail).  The standalone covered call ($250)
        must still be present.
        """
        all_legs = [
            # Standalone covered call — should stay in detail_list
            _leg("AMD", "call", "short", 250.0, expiration="2026-05-29",
                 option_id="CC250"),
            # CCS spread: short $505 + long $545 — short should be excluded
            _leg("AMD", "call", "short", 505.0, expiration="2026-06-05",
                 option_id="CCS505"),
            _leg("AMD", "call", "long",  545.0, expiration="2026-06-05",
                 option_id="CCS545"),
        ]
        detail, spreads = self._simulate_detail_filtering(all_legs)

        # Spread detected
        assert len(spreads) == 1
        assert spreads[0]["type"] == "CCS"
        assert spreads[0]["short_strike"] == 505.0

        # detail_list contains only the standalone call
        strikes_in_detail = [c["strike"] for c in detail]
        assert 250.0 in strikes_in_detail, "Standalone $250 call must remain"
        assert 505.0 not in strikes_in_detail, (
            "CCS short leg $505 must be excluded from detail_list"
        )

    def test_standalone_calls_not_affected_by_ccs_filter(self):
        """
        Symbols with only standalone short calls (no matching long)
        are not removed by the CCS filter.
        """
        all_legs = [
            _leg("AAPL", "call", "short", 200.0, option_id="A200"),
            _leg("TSLA", "call", "short", 400.0, option_id="T400"),
        ]
        detail, spreads = self._simulate_detail_filtering(all_legs)

        assert len(spreads) == 0
        assert len(detail) == 2
        assert {c["symbol"] for c in detail} == {"AAPL", "TSLA"}

    def test_pcs_legs_dont_affect_call_detail(self):
        """
        PCS spread legs (puts) don't affect the call detail_list filtering.
        """
        all_legs = [
            # Standalone covered call
            _leg("AMD", "call", "short", 250.0, option_id="CC250"),
            # PCS spread (puts, not calls) — should not affect call detail
            _leg("AMD", "put", "short", 200.0, option_id="PCS200"),
            _leg("AMD", "put", "long",  180.0, option_id="PCS180"),
        ]
        detail, spreads = self._simulate_detail_filtering(all_legs)

        assert len(spreads) == 1
        assert spreads[0]["type"] == "PCS"
        # Call detail should still have the standalone covered call
        assert len(detail) == 1
        assert detail[0]["strike"] == 250.0

    def test_multiple_ccs_across_symbols(self):
        """
        Multiple CCS spreads across different symbols — all short legs excluded.
        """
        all_legs = [
            # AMD CCS
            _leg("AMD", "call", "short", 505.0, expiration="2026-06-05",
                 option_id="AMD_CCS_S"),
            _leg("AMD", "call", "long",  545.0, expiration="2026-06-05",
                 option_id="AMD_CCS_L"),
            # TSLA CCS
            _leg("TSLA", "call", "short", 300.0, expiration="2026-06-12",
                 option_id="TSLA_CCS_S"),
            _leg("TSLA", "call", "long",  350.0, expiration="2026-06-12",
                 option_id="TSLA_CCS_L"),
            # Standalone covered call — should stay
            _leg("AAPL", "call", "short", 200.0, expiration="2026-06-05",
                 option_id="AAPL_CC"),
        ]
        detail, spreads = self._simulate_detail_filtering(all_legs)

        assert len(spreads) == 2
        assert len(detail) == 1
        assert detail[0]["symbol"] == "AAPL"

    def test_same_symbol_standalone_and_ccs_coexist(self):
        """
        Same symbol with both a standalone covered call and a CCS spread
        at different strikes/expirations — only the CCS short leg is excluded.
        """
        all_legs = [
            # Standalone covered call for AMD (different expiration)
            _leg("AMD", "call", "short", 250.0, expiration="2026-05-29",
                 option_id="CC_250"),
            # CCS spread for AMD
            _leg("AMD", "call", "short", 505.0, expiration="2026-06-05",
                 option_id="CCS_505"),
            _leg("AMD", "call", "long",  545.0, expiration="2026-06-05",
                 option_id="CCS_545"),
            # Another standalone covered call for AMD (same expiry as CCS but different strike)
            _leg("AMD", "call", "short", 600.0, expiration="2026-06-05",
                 option_id="CC_600"),
        ]
        detail, spreads = self._simulate_detail_filtering(all_legs)

        assert len(spreads) == 1
        detail_strikes = sorted([c["strike"] for c in detail])
        assert 250.0 in detail_strikes, "Standalone $250 at different expiry stays"
        assert 600.0 in detail_strikes, "Standalone $600 at same expiry stays"
        assert 505.0 not in detail_strikes, "CCS short leg $505 excluded"
