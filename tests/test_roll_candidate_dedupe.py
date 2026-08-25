"""Roll candidates must not include spreads already being closed.

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


def _closed(symbol, expiration="2026-08-28"):
    return {"symbol": symbol, "expiration": expiration, "spread_type": "CCS"}


class TestSuppressClosedSpreads:
    def test_rescue_closed_spread_is_dropped(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [_closed("TSLA")], []
        )
        assert [c["symbol"] for c in kept] == ["HOOD"]
        assert dropped == 1

    def test_panic_closed_spread_is_dropped(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [], [_closed("HOOD")]
        )
        assert [c["symbol"] for c in kept] == ["TSLA"]
        assert dropped == 1

    def test_nothing_closed_leaves_list_untouched(self):
        kept, dropped = scheduler._suppress_closed_spreads([TSLA, HOOD], [], [])
        assert kept == [TSLA, HOOD]
        assert dropped == 0

    def test_none_results_are_tolerated(self):
        """Sections can be skipped entirely, leaving None rather than []."""
        kept, dropped = scheduler._suppress_closed_spreads([TSLA], None, None)
        assert kept == [TSLA]
        assert dropped == 0

    def test_same_symbol_different_expiration_is_kept(self):
        """Keyed on (symbol, expiration) — a later-dated spread is a separate
        position and must survive."""
        later = dict(TSLA, expiration="2026-09-18")
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, later], [_closed("TSLA", "2026-08-28")], []
        )
        assert [c["expiration"] for c in kept] == ["2026-09-18"]
        assert dropped == 1

    def test_both_scans_closing_the_same_spread_drops_it_once(self):
        kept, dropped = scheduler._suppress_closed_spreads(
            [TSLA, HOOD], [_closed("TSLA")], [_closed("TSLA")]
        )
        assert [c["symbol"] for c in kept] == ["HOOD"]
        assert dropped == 1

    def test_empty_candidate_list(self):
        kept, dropped = scheduler._suppress_closed_spreads([], [_closed("TSLA")], [])
        assert kept == []
        assert dropped == 0
