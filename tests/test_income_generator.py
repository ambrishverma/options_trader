"""
test_income_generator.py — Unit tests for income_generator.py (v1.9)
====================================================================
Tests contract quantity formula, duplicate detection, config display/update,
and the generate_income orchestrator.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from income_generator import calculate_quantity, is_duplicate


class TestCalculateQuantity:
    """Contract quantity formula: floor((cl_ratio / min_cl) * risk_factor), capped."""

    def test_below_threshold_returns_zero(self):
        assert calculate_quantity(cl_ratio=0.05, min_cl=0.10, risk_factor=1.0, max_qty=5) == 0

    def test_at_threshold_returns_one(self):
        assert calculate_quantity(cl_ratio=0.10, min_cl=0.10, risk_factor=1.0, max_qty=5) == 1

    def test_above_threshold_floors_down(self):
        # 0.15 / 0.10 = 1.5  →  floor = 1
        assert calculate_quantity(cl_ratio=0.15, min_cl=0.10, risk_factor=1.0, max_qty=5) == 1

    def test_double_threshold(self):
        # 0.25 / 0.10 = 2.5  →  floor = 2
        assert calculate_quantity(cl_ratio=0.25, min_cl=0.10, risk_factor=1.0, max_qty=5) == 2

    def test_capped_at_max(self):
        # 0.80 / 0.10 = 8.0  →  floor = 8, capped at 5
        assert calculate_quantity(cl_ratio=0.80, min_cl=0.10, risk_factor=1.0, max_qty=5) == 5

    def test_risk_factor_scales_up(self):
        # 0.10 / 0.10 * 2.0 = 2.0  →  floor = 2
        assert calculate_quantity(cl_ratio=0.10, min_cl=0.10, risk_factor=2.0, max_qty=5) == 2

    def test_risk_factor_scales_down(self):
        # 0.10 / 0.10 * 0.5 = 0.5  →  floor = 0
        assert calculate_quantity(cl_ratio=0.10, min_cl=0.10, risk_factor=0.5, max_qty=5) == 0

    def test_zero_cl_ratio(self):
        assert calculate_quantity(cl_ratio=0.0, min_cl=0.10, risk_factor=1.0, max_qty=5) == 0

    def test_negative_cl_ratio(self):
        assert calculate_quantity(cl_ratio=-0.05, min_cl=0.10, risk_factor=1.0, max_qty=5) == 0

    def test_exact_five_hits_cap(self):
        # 0.50 / 0.10 = 5.0  →  floor = 5, capped at 5
        assert calculate_quantity(cl_ratio=0.50, min_cl=0.10, risk_factor=1.0, max_qty=5) == 5


class TestIsDuplicate:
    """Duplicate detection: match on (symbol, type, expiration)."""

    OPEN_SPREADS = [
        {"symbol": "NVDA", "type": "CCS", "expiration": "2026-06-20",
         "short_strike": 180.0, "long_strike": 190.0, "quantity": 1},
        {"symbol": "AMD",  "type": "PCS", "expiration": "2026-06-20",
         "short_strike": 140.0, "long_strike": 130.0, "quantity": 1},
    ]

    def test_exact_match_is_duplicate(self):
        contract = {"symbol": "NVDA", "type": "CCS", "expiration": "2026-06-20"}
        assert is_duplicate(contract, self.OPEN_SPREADS) is True

    def test_same_symbol_different_type_is_not_duplicate(self):
        contract = {"symbol": "NVDA", "type": "PCS", "expiration": "2026-06-20"}
        assert is_duplicate(contract, self.OPEN_SPREADS) is False

    def test_same_symbol_same_type_different_exp_is_not_duplicate(self):
        contract = {"symbol": "NVDA", "type": "CCS", "expiration": "2026-07-18"}
        assert is_duplicate(contract, self.OPEN_SPREADS) is False

    def test_empty_open_spreads_is_not_duplicate(self):
        contract = {"symbol": "NVDA", "type": "CCS", "expiration": "2026-06-20"}
        assert is_duplicate(contract, []) is False

    def test_case_insensitive_symbol(self):
        contract = {"symbol": "nvda", "type": "CCS", "expiration": "2026-06-20"}
        assert is_duplicate(contract, self.OPEN_SPREADS) is True


from unittest.mock import patch, MagicMock
from income_generator import generate_income


def _make_scanner_result(symbol, spread_type, cl_ratio=0.15, expiration="2026-06-20"):
    """Build a minimal scanner result dict matching spread_scanner.py output."""
    return {
        "symbol": symbol,
        "type": spread_type,
        "expiration": expiration,
        "dte": 25,
        "current_price": 180.0,
        "short_leg": {"strike": 175.0, "bid": 2.00, "ask": 2.20, "mid": 2.10,
                       "open_interest": 50, "otm_pct": 12.0},
        "long_leg":  {"strike": 165.0, "bid": 0.50, "ask": 0.70, "mid": 0.60,
                       "open_interest": 30},
        "net_credit": 1.30,
        "net_credit_total": 130.0,
        "max_loss": 870.0,
        "spread_size": 10.0,
        "ypd": 5.20,
        "credit_to_loss_ratio": cl_ratio,
        "score": 5.20 * cl_ratio,
    }


class TestGenerateIncome:
    """Orchestrator: load snapshot → duplicate check → quantity → place."""

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_happy_path_places_order(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10,
            "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5,
            "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 1
        assert result["skipped_duplicate"] == 0
        assert result["skipped_threshold"] == 0
        mock_place.assert_called_once()
        call_kw = mock_place.call_args
        assert call_kw[1]["quantity"] == 1  # floor(0.15/0.10 * 1.0) = 1
        assert call_kw[1]["dry_run"] is False

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot")
    def test_duplicate_skipped(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS")]
        mock_snap.return_value = [
            {"symbol": "NVDA", "type": "CCS", "expiration": "2026-06-20",
             "short_strike": 175.0, "long_strike": 165.0, "quantity": 1},
        ]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["skipped_duplicate"] == 1
        mock_place.assert_not_called()

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_below_threshold_skipped(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("GOOG", "PCS", cl_ratio=0.05)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["skipped_threshold"] == 1
        mock_place.assert_not_called()

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_preview_mode_uses_dry_run(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=False, config=config)

        call_kw = mock_place.call_args
        assert call_kw[1]["dry_run"] is True

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_ig_enabled_false_forces_preview(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": False,
        }
        # live=True but ig_enabled=False → still dry_run
        result = generate_income(symbol_filter=None, live=True, config=config)

        call_kw = mock_place.call_args
        assert call_kw[1]["dry_run"] is True

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_symbol_filter(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [
            _make_scanner_result("NVDA", "CCS", cl_ratio=0.15),
            _make_scanner_result("GOOG", "PCS", cl_ratio=0.20),
        ]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter="NVDA", live=True, config=config)

        # Only NVDA should be processed (GOOG filtered out)
        assert result["placed"] == 1
        mock_place.assert_called_once()
        assert mock_place.call_args[1]["symbol"] == "NVDA"

    @patch("income_generator.place_spread_order")
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_no_contract_skipped(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [
            {"symbol": "TSLA", "type": "CCS", "strategy_hint": "CCS — sell calls above $260",
             "no_contract": True, "scenarios": 42},
        ]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["no_contract"] == 1
        assert result["placed"] == 0
        mock_place.assert_not_called()

    @patch("income_generator.place_spread_order", return_value=False)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_order_failure_counted(self, mock_snap, mock_load_recs, mock_place):
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["failed"] == 1

    @patch("income_generator.place_spread_order")
    @patch("utils.load_strategy_recs_snapshot", return_value=[])
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_no_recommendations_exits_cleanly(self, mock_snap, mock_load_recs, mock_place):
        """Empty snapshot returns zeroed summary without placing orders."""
        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["no_contract"] == 0
        mock_place.assert_not_called()


class TestSnapshotFreshness:
    """Snapshot freshness warning when >24h old."""

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_stale_snapshot_prints_warning(self, mock_snap, mock_load_recs,
                                            mock_place, capsys, tmp_path):
        """A snapshot older than 24h triggers a WARNING message."""
        import json as _json
        from datetime import datetime as _dt, timedelta as _td

        # Create a stale snapshot (48h old)
        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()
        stale_time = (_dt.now() - _td(hours=48)).isoformat()
        snap_file = snap_dir / "open_spreads_detail_20260524.json"
        snap_file.write_text(_json.dumps({"pulled_at": stale_time, "spreads": []}))

        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }

        import income_generator as ig_mod
        orig_dir = ig_mod._SNAPSHOT_DIR
        ig_mod._SNAPSHOT_DIR = snap_dir
        try:
            generate_income(symbol_filter=None, live=True, config=config)
        finally:
            ig_mod._SNAPSHOT_DIR = orig_dir

        out = capsys.readouterr().out
        assert "WARNING" in out
        assert ">24h old" in out

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_fresh_snapshot_no_warning(self, mock_snap, mock_load_recs,
                                       mock_place, capsys, tmp_path):
        """A snapshot less than 24h old does NOT trigger a warning."""
        import json as _json
        from datetime import datetime as _dt, timedelta as _td

        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()
        fresh_time = (_dt.now() - _td(hours=2)).isoformat()
        snap_file = snap_dir / "open_spreads_detail_20260526.json"
        snap_file.write_text(_json.dumps({"pulled_at": fresh_time, "spreads": []}))

        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }

        import income_generator as ig_mod
        orig_dir = ig_mod._SNAPSHOT_DIR
        ig_mod._SNAPSHOT_DIR = snap_dir
        try:
            generate_income(symbol_filter=None, live=True, config=config)
        finally:
            ig_mod._SNAPSHOT_DIR = orig_dir

        out = capsys.readouterr().out
        assert "WARNING" not in out


class TestMixedResults:
    """Orchestration with multiple symbols producing mixed outcomes."""

    @patch("income_generator.place_spread_order")
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot")
    def test_mixed_outcomes_in_single_run(self, mock_snap, mock_load_recs, mock_place):
        """4 symbols: 1 placed, 1 duplicate, 1 below threshold, 1 no contract."""
        mock_load_recs.return_value = [
            _make_scanner_result("NVDA", "CCS", cl_ratio=0.20),           # → placed (qty 2)
            _make_scanner_result("AMD", "PCS", cl_ratio=0.15),            # → duplicate
            _make_scanner_result("GOOG", "PCS", cl_ratio=0.05),           # → below threshold
            {"symbol": "TSLA", "type": "CCS", "no_contract": True,
             "strategy_hint": "x", "scenarios": 0},                        # → no contract
        ]
        mock_snap.return_value = [
            {"symbol": "AMD", "type": "PCS", "expiration": "2026-06-20",
             "short_strike": 140.0, "long_strike": 130.0, "quantity": 1},
        ]
        mock_place.return_value = True

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 1
        assert result["skipped_duplicate"] == 1
        assert result["skipped_threshold"] == 1
        assert result["no_contract"] == 1
        assert result["failed"] == 0
        mock_place.assert_called_once()


import tempfile, shutil
from pathlib import Path
from income_generator import show_config, set_config


class TestCollateralTracking:
    """Collateral (max_loss × qty) is tracked in summary and details."""

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_collateral_in_summary(self, mock_snap, mock_load_recs, mock_place):
        """total_collateral = max_loss × qty for placed orders."""
        rec = _make_scanner_result("NVDA", "CCS", cl_ratio=0.20)
        # rec has max_loss=870.0, cl=0.20 → qty = floor(0.20/0.10 * 1.0) = 2
        mock_load_recs.return_value = [rec]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 1
        assert result["total_collateral"] == 870.0 * 2  # max_loss × qty

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_collateral_in_detail(self, mock_snap, mock_load_recs, mock_place):
        """Each placed detail includes collateral field."""
        rec = _make_scanner_result("NVDA", "CCS", cl_ratio=0.15)
        # cl=0.15 → qty = floor(0.15/0.10 * 1.0) = 1
        mock_load_recs.return_value = [rec]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        placed = [d for d in result["details"] if d["action"] == "placed"]
        assert len(placed) == 1
        assert placed[0]["collateral"] == 870.0  # max_loss × 1

    @patch("income_generator.place_spread_order", return_value=False)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_failed_order_not_counted_in_total_collateral(self, mock_snap, mock_load_recs, mock_place):
        """Failed orders should NOT contribute to total_collateral."""
        mock_load_recs.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["failed"] == 1
        assert result["total_collateral"] == 0.0


class TestAutoIncomeConfig:
    """auto_income config key is recognized and settable."""

    def test_show_config_includes_auto_income(self, capsys):
        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
            "auto_income": False,
        }
        show_config(config)
        out = capsys.readouterr().out
        assert "auto_income" in out

    def test_set_auto_income_true(self, tmp_path):
        cfg_path = _make_test_config(tmp_path)
        ok = set_config("auto_income=true", config_path=cfg_path)
        assert ok is True
        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)
        assert data["auto_income"] is True

    def test_set_auto_income_false(self, tmp_path):
        cfg_path = _make_test_config(tmp_path)
        ok = set_config("auto_income=false", config_path=cfg_path)
        assert ok is True
        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)
        assert data["auto_income"] is False


def _make_test_config(tmp_path):
    """Copy project config.yaml to tmp_path for test use."""
    src = Path(__file__).parent.parent / "config.yaml"
    dst = tmp_path / "config.yaml"
    shutil.copy(src, dst)
    return dst


class TestShowConfig:
    """--income-config (no arg) displays all ig_* keys."""

    def test_displays_all_keys(self, capsys):
        config = {
            "ig_min_cl_ratio": 0.10,
            "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5,
            "ig_enabled": True,
        }
        show_config(config)
        out = capsys.readouterr().out
        assert "ig_min_cl_ratio" in out
        assert "0.1" in out
        assert "ig_risk_factor" in out
        assert "ig_max_contracts_per_equity" in out
        assert "ig_enabled" in out


class TestSetConfig:
    """--income-config KEY=VALUE updates config.yaml."""

    def _make_config_file(self, tmp_dir):
        src = Path(__file__).parent.parent / "config.yaml"
        dst = Path(tmp_dir) / "config.yaml"
        shutil.copy(src, dst)
        return dst

    def test_updates_valid_key(self, tmp_path):
        cfg_path = self._make_config_file(tmp_path)
        ok = set_config("ig_risk_factor=0.5", config_path=cfg_path)
        assert ok is True
        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)
        assert data["ig_risk_factor"] == 0.5

    def test_rejects_invalid_key(self, capsys, tmp_path):
        cfg_path = self._make_config_file(tmp_path)
        ok = set_config("ig_bogus_key=1.0", config_path=cfg_path)
        assert ok is False
        out = capsys.readouterr().out
        assert "Unknown" in out or "unknown" in out

    def test_rejects_bad_value_type(self, capsys, tmp_path):
        cfg_path = self._make_config_file(tmp_path)
        ok = set_config("ig_min_cl_ratio=not_a_number", config_path=cfg_path)
        assert ok is False
        out = capsys.readouterr().out
        assert "Invalid" in out or "invalid" in out

    def test_rejects_missing_equals(self, capsys, tmp_path):
        cfg_path = self._make_config_file(tmp_path)
        ok = set_config("ig_risk_factor", config_path=cfg_path)
        assert ok is False

    def test_updates_boolean_key(self, tmp_path):
        cfg_path = self._make_config_file(tmp_path)
        ok = set_config("ig_enabled=false", config_path=cfg_path)
        assert ok is True
        import yaml
        with open(cfg_path) as f:
            data = yaml.safe_load(f)
        assert data["ig_enabled"] is False

    def test_preserves_yaml_comments(self, tmp_path):
        """set_config must not strip YAML comments from the file."""
        cfg_path = self._make_config_file(tmp_path)
        original_text = cfg_path.read_text()
        assert "#" in original_text  # sanity: original has comments
        set_config("ig_risk_factor=0.5", config_path=cfg_path)
        updated_text = cfg_path.read_text()
        # Comments should still be present
        assert "# -- Income Generator" in updated_text or "# Income Generator" in updated_text
        # Value should be updated
        assert "ig_risk_factor: 0.5" in updated_text


# ─────────────────────────────────────────────────────────────────────────────
# Goal-oriented income generation tests
# ─────────────────────────────────────────────────────────────────────────────

class TestGoalOrientedIncome:
    """Tests for the two-pass goal-chasing logic in generate_income."""

    def _base_config(self, **overrides):
        cfg = {
            "ig_min_cl_ratio": 0.20,
            "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5,
            "ig_enabled": True,
            "ig_min_daily_income_goal": 0,
            "ig_cl_ratio_buffer": 0.0,
        }
        cfg.update(overrides)
        return cfg

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_sorted_by_cl_ratio_high_to_low(self, mock_snap, mock_recs, mock_place):
        """Recs are processed in CL ratio descending order."""
        mock_recs.return_value = [
            _make_scanner_result("LOW",  "PCS", cl_ratio=0.20),
            _make_scanner_result("HIGH", "CCS", cl_ratio=0.40),
            _make_scanner_result("MID",  "PCS", cl_ratio=0.30),
        ]
        result = generate_income(live=True, config=self._base_config())
        # All 3 should be placed; check order via details
        placed = [d["symbol"] for d in result["details"] if d["action"] == "placed"]
        assert placed == ["HIGH", "MID", "LOW"]

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_no_goal_no_chase(self, mock_snap, mock_recs, mock_place):
        """With goal=$0, recs below ig_min_cl_ratio are skipped (no goal chase)."""
        mock_recs.return_value = [
            _make_scanner_result("ABOVE", "CCS", cl_ratio=0.25),
            _make_scanner_result("BELOW", "PCS", cl_ratio=0.15),
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=0,
        ))
        assert result["placed"] == 1
        assert result["skipped_threshold"] == 1

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_goal_chase_lowers_threshold(self, mock_snap, mock_recs, mock_place):
        """Pass 2 lowers threshold to reach income goal."""
        # CL=0.25 rec gives $130 credit (net_credit_total=130, qty=1)
        # CL=0.18 rec also gives $130 (below min_cl=0.20 but within buffer)
        mock_recs.return_value = [
            _make_scanner_result("PASS1", "CCS", cl_ratio=0.25),
            _make_scanner_result("PASS2", "PCS", cl_ratio=0.18),
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=250,   # $250 goal
            ig_cl_ratio_buffer=0.05,        # floor at 0.15
        ))
        # Pass 1: PASS1 placed ($130) — goal not met
        # Pass 2: PASS2 placed at lowered threshold ($130) — total $260 ≥ $250
        assert result["placed"] == 2
        assert result["total_credit"] >= 250
        assert result["skipped_threshold"] == 0

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_goal_chase_stops_when_met(self, mock_snap, mock_recs, mock_place):
        """Pass 2 stops purchasing once goal is met."""
        mock_recs.return_value = [
            _make_scanner_result("A", "CCS", cl_ratio=0.25),    # $130
            _make_scanner_result("B", "PCS", cl_ratio=0.19),    # $130 (in buffer)
            _make_scanner_result("C", "CCS", cl_ratio=0.17),    # $130 (in buffer)
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=250,   # need $250
            ig_cl_ratio_buffer=0.05,        # floor at 0.15
        ))
        # Pass 1: A placed ($130)
        # Pass 2: B placed ($260 ≥ $250 → stop)
        # C should NOT be placed (goal already met)
        assert result["placed"] == 2
        assert result["total_credit"] >= 250

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_goal_chase_respects_floor(self, mock_snap, mock_recs, mock_place):
        """Pass 2 never goes below ig_min_cl_ratio - ig_cl_ratio_buffer."""
        mock_recs.return_value = [
            _make_scanner_result("ABOVE", "CCS", cl_ratio=0.25),  # $130 (pass 1)
            _make_scanner_result("FLOOR", "PCS", cl_ratio=0.16),  # in buffer
            _make_scanner_result("BELOW", "CCS", cl_ratio=0.10),  # below floor
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=1000,  # high goal, can't be met
            ig_cl_ratio_buffer=0.05,        # floor at 0.15
        ))
        # ABOVE placed (pass 1), FLOOR placed (pass 2), BELOW skipped (below floor 0.15)
        assert result["placed"] == 2
        assert result["skipped_threshold"] == 1

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_pass2_uses_lowered_threshold_as_divisor(self, mock_snap, mock_recs, mock_place):
        """In Pass 2, calculate_quantity uses the lowered threshold as divisor."""
        # CL=0.19, min_cl=0.20, so in pass 1 this is skipped.
        # In pass 2 at threshold 0.19: qty = floor(0.19/0.19 * 1.0) = 1
        mock_recs.return_value = [
            _make_scanner_result("GOAL", "CCS", cl_ratio=0.19),
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=100,
            ig_cl_ratio_buffer=0.05,
        ))
        assert result["placed"] == 1
        placed = [d for d in result["details"] if d["action"] == "placed"]
        assert placed[0]["quantity"] == 1

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_negative_floor_guarded(self, mock_snap, mock_recs, mock_place):
        """Buffer larger than min_cl doesn't produce negative floor."""
        mock_recs.return_value = [
            _make_scanner_result("A", "CCS", cl_ratio=0.05),
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_cl_ratio=0.10,
            ig_min_daily_income_goal=1000,
            ig_cl_ratio_buffer=0.50,  # would make floor -0.40, guarded to 0.01
        ))
        # CL=0.05 is above floor (0.01) but the function should not crash
        # and should process if within the guarded range
        assert result is not None
        assert "placed" in result

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_pass1_always_purchases_above_min_cl(self, mock_snap, mock_recs, mock_place):
        """Pass 1 purchases ALL recs above min_cl even if goal is already met."""
        mock_recs.return_value = [
            _make_scanner_result("A", "CCS", cl_ratio=0.40),  # $130 × 2 = $260
            _make_scanner_result("B", "PCS", cl_ratio=0.30),  # $130 × 1 = $130
            _make_scanner_result("C", "CCS", cl_ratio=0.25),  # $130 × 1 = $130
        ]
        result = generate_income(live=True, config=self._base_config(
            ig_min_daily_income_goal=100,  # very low goal, met after first contract
        ))
        # All 3 are above min_cl=0.20, so ALL should be placed regardless of goal
        assert result["placed"] == 3

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("utils.load_strategy_recs_snapshot")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_config_shows_new_keys(self, mock_snap, mock_recs, mock_place):
        """show_config displays the new goal-oriented keys."""
        from income_generator import show_config
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            show_config({
                "ig_min_cl_ratio": 0.20,
                "ig_min_daily_income_goal": 500,
                "ig_cl_ratio_buffer": 0.05,
            })
        output = buf.getvalue()
        assert "ig_min_daily_income_goal" in output
        assert "ig_cl_ratio_buffer" in output
