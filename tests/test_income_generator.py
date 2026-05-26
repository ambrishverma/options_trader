"""
test_income_generator.py — Unit tests for income_generator.py (v1.8)
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
    """Orchestrator: parse → scan → duplicate check → quantity → place."""

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_happy_path_places_order(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

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
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot")
    def test_duplicate_skipped(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS")]
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
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_below_threshold_skipped(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "GOOG", "spread_type": "PCS", "action": "sell puts below",
             "strike": 170.0, "raw_text": "PCS — sell puts below $170"},
        ]
        mock_scan.return_value = [_make_scanner_result("GOOG", "PCS", cl_ratio=0.05)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["skipped_threshold"] == 1
        mock_place.assert_not_called()

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_preview_mode_uses_dry_run(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=False, config=config)

        call_kw = mock_place.call_args
        assert call_kw[1]["dry_run"] is True

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_ig_enabled_false_forces_preview(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": False,
        }
        # live=True but ig_enabled=False → still dry_run
        result = generate_income(symbol_filter=None, live=True, config=config)

        call_kw = mock_place.call_args
        assert call_kw[1]["dry_run"] is True

    @patch("income_generator.place_spread_order", return_value=True)
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_symbol_filter(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter="NVDA", live=True, config=config)

        mock_parse.assert_called_once()
        # filter_sym is passed through to parse_strategy_table
        assert mock_parse.call_args[1]["filter_sym"] == "NVDA"

    @patch("income_generator.place_spread_order")
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_no_contract_skipped(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "TSLA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 260.0, "raw_text": "CCS — sell calls above $260"},
        ]
        mock_scan.return_value = [
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
    @patch("income_generator.scan_strategy_recommendations")
    @patch("income_generator.parse_strategy_table")
    @patch("income_generator.load_open_spreads_detail_snapshot", return_value=[])
    def test_order_failure_counted(self, mock_snap, mock_parse, mock_scan, mock_place):
        mock_parse.return_value = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above",
             "strike": 180.0, "raw_text": "CCS — sell calls above $180"},
        ]
        mock_scan.return_value = [_make_scanner_result("NVDA", "CCS", cl_ratio=0.15)]

        config = {
            "ig_min_cl_ratio": 0.10, "ig_risk_factor": 1.0,
            "ig_max_contracts_per_equity": 5, "ig_enabled": True,
        }
        result = generate_income(symbol_filter=None, live=True, config=config)

        assert result["placed"] == 0
        assert result["failed"] == 1


import tempfile, shutil
from pathlib import Path
from income_generator import show_config, set_config


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
