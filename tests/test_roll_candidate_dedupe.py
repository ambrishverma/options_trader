"""Roll candidates must not include spreads already being closed.

_suppress_closed_spreads returns the suppressed entries rather than a count:
each one is a position that will not be acted on, so the caller logs it by
name instead of reporting a bare number.

Rescue and panic place CLOSE orders on spreads. The roll scan reads the same
positions independently, so without suppression the report listed one spread
twice under the single "Rescue" header — "close at $4.00" in the Spread Rescue
table, and "price between legs, consider rolling" in the roll-candidate table
below it. Two contradictory remedies for a position already being closed.
"""
import scheduler


TSLA = {"symbol": "TSLA", "expiration": "2026-08-28",
        "short_strike": 340.0, "long_strike": 360.0, "is_spread": True}
HOOD = {"symbol": "HOOD", "expiration": "2026-08-28",
        "short_strike": 104.0, "long_strike": 111.0, "is_spread": True}

# Single-leg covered call on the SAME symbol and expiry as the TSLA spread.
# roll_monitor emits these without is_spread and with `strike`, not
# `short_strike` (roll_monitor.py:139, :233).
TSLA_CC = {"symbol": "TSLA", "expiration": "2026-08-28", "strike": 345.0}


def _closed(symbol, expiration="2026-08-28", short_strike=None):
    return {"symbol": symbol, "expiration": expiration, "spread_type": "CCS",
            "short_strike": short_strike if short_strike is not None
            else {"TSLA": 340.0, "HOOD": 104.0}[symbol]}


class TestSuppressClosedSpreads:
    def test_rescue_closed_spread_is_dropped(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [_closed("TSLA")], []
        )
        assert [c["symbol"] for c in kept] == ["HOOD"]
        assert len(dropped) == 1

    def test_panic_closed_spread_is_dropped(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [], [_closed("HOOD")]
        )
        assert [c["symbol"] for c in kept] == ["TSLA"]
        assert len(dropped) == 1

    def test_nothing_closed_leaves_list_untouched(self):
        kept, dropped = scheduler._suppress_closed_spreads([TSLA, HOOD], [], [])
        assert kept == [TSLA, HOOD]
        assert len(dropped) == 0

    def test_none_results_are_tolerated(self):
        """Sections can be skipped entirely, leaving None rather than []."""
        kept, dropped = scheduler._suppress_closed_spreads([TSLA], None, None)
        assert kept == [TSLA]
        assert len(dropped) == 0

    def test_same_symbol_different_expiration_is_kept(self):
        """Keyed on (symbol, expiration) — a later-dated spread is a separate
        position and must survive."""
        later = dict(TSLA, expiration="2026-09-18")
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, later], [_closed("TSLA", "2026-08-28")], []
        )
        assert [c["expiration"] for c in kept] == ["2026-09-18"]
        assert len(dropped) == 1

    def test_both_scans_closing_the_same_spread_drops_it_once(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [_closed("TSLA")], [_closed("TSLA")]
        )
        assert [c["symbol"] for c in kept] == ["HOOD"]
        assert len(dropped) == 1

    def test_single_leg_candidate_on_same_symbol_survives(self):
        """The inverse bug: keying on (symbol, expiration) alone hid real work.

        A covered call sharing a symbol and expiry with a spread being closed is
        a separate position and still worth rolling. Dropping it would remove a
        genuine candidate from the report with nothing naming what went.
        """
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA_CC, TSLA, HOOD], [_closed("TSLA")], []
        )
        assert TSLA_CC in kept
        assert [c["symbol"] for c in kept] == ["TSLA", "HOOD"]
        assert len(dropped) == 1

    def test_different_short_strike_on_same_symbol_and_expiry_survives(self):
        """Two spreads, same symbol and expiry, different strikes are distinct."""
        other = dict(TSLA, short_strike=330.0, long_strike=350.0)
        kept, _ = scheduler._suppress_closed_spreads(
            [TSLA, other], [_closed("TSLA", short_strike=340.0)], []
        )
        assert [c["short_strike"] for c in kept] == [330.0]

    def test_strike_type_mismatch_still_matches(self):
        """The two sides come from different sources, so 340, 340.0 and "340"
        must compare equal or the filter silently never fires."""
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA], [_closed("TSLA", short_strike="340")], []
        )
        assert kept == []
        assert len(dropped) == 1

    def test_unparseable_strike_does_not_crash_or_match(self):
        """A malformed strike must not raise, and must not match a real one.

        Normalising to None on failure means it can only collide with another
        unparseable strike, never silently suppress a valid position.
        """
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA], [_closed("TSLA", short_strike="n/a")], []
        )
        assert kept == [TSLA]
        assert len(dropped) == 0

    def test_dropped_entries_are_returned_not_just_counted(self):
        """The caller logs each suppressed position by symbol and strike."""
        _, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [_closed("TSLA")], []
        )
        assert [d["symbol"] for d in dropped] == ["TSLA"]
        assert dropped[0]["short_strike"] == 340.0

    def test_empty_candidate_list(self):
        kept, dropped = scheduler._suppress_closed_spreads([], [_closed("TSLA")], [])
        assert kept == []
        assert len(dropped) == 0
