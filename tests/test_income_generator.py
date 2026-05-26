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
