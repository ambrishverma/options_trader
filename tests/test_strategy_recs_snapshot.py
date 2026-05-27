"""
test_strategy_recs_snapshot.py — Tests for strategy recs persistence
====================================================================
Verifies write_strategy_recs_snapshot and load_strategy_recs_snapshot
round-trip correctly, handle missing files, and respect date filtering.
"""

import json
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import patch
from utils import write_strategy_recs_snapshot, load_strategy_recs_snapshot


def _sample_recs():
    return [
        {
            "symbol": "NVDA", "type": "CCS", "expiration": "2026-06-20",
            "dte": 25, "net_credit": 1.30, "credit_to_loss_ratio": 0.15,
            "strategy_hint": "CCS — sell calls above $180",
        },
        {
            "symbol": "TSLA", "type": "CCS", "no_contract": True,
            "strategy_hint": "CCS — sell calls above $260", "scenarios": 42,
        },
    ]


class TestWriteStrategyRecsSnapshot:
    """write_strategy_recs_snapshot persists to snapshots/strategy_recs_DATE.json."""

    def test_writes_json_file(self, tmp_path):
        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            dest = write_strategy_recs_snapshot(_sample_recs(), "2026-05-26")

        assert dest.exists()
        data = json.loads(dest.read_text())
        assert data["run_date"] == "2026-05-26"
        assert data["count"] == 2
        assert len(data["strategy_recs"]) == 2
        assert data["strategy_recs"][0]["symbol"] == "NVDA"

    def test_overwrites_existing(self, tmp_path):
        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            write_strategy_recs_snapshot([{"symbol": "OLD"}], "2026-05-26")
            dest = write_strategy_recs_snapshot(_sample_recs(), "2026-05-26")

        data = json.loads(dest.read_text())
        assert data["count"] == 2  # overwritten, not appended

    def test_dry_run_flag_persisted(self, tmp_path):
        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            dest = write_strategy_recs_snapshot(_sample_recs(), "2026-05-26", dry_run=True)

        data = json.loads(dest.read_text())
        assert data["dry_run"] is True


class TestLoadStrategyRecsSnapshot:
    """load_strategy_recs_snapshot reads back what write persisted."""

    def test_round_trip(self, tmp_path):
        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            write_strategy_recs_snapshot(_sample_recs(), "2026-05-26")
            loaded = load_strategy_recs_snapshot("2026-05-26")

        assert len(loaded) == 2
        assert loaded[0]["symbol"] == "NVDA"
        assert loaded[1]["no_contract"] is True

    def test_missing_file_returns_empty(self, tmp_path):
        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            loaded = load_strategy_recs_snapshot("2099-01-01")

        assert loaded == []

    def test_defaults_to_today(self, tmp_path):
        from datetime import date
        today = date.today().strftime("%Y-%m-%d")

        with patch("utils.SNAPSHOTS_DIR", tmp_path):
            write_strategy_recs_snapshot(_sample_recs(), today)
            loaded = load_strategy_recs_snapshot()  # no date arg

        assert len(loaded) == 2
