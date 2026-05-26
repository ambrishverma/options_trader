"""
income_generator.py — Autonomous Spread Purchasing (v1.8)
=========================================================
Reads daily strategy briefing recommendations, calculates per-symbol
contract quantities via a config-driven formula, checks for duplicates
against the portfolio snapshot, and places PCS/CCS orders.

Public API:
  calculate_quantity(cl_ratio, min_cl, risk_factor, max_qty) -> int
  is_duplicate(contract, open_spreads) -> bool
  generate_income(symbol_filter, live, config) -> dict
  show_config(config) -> None
  set_config(key_value, config_path) -> bool
"""

import logging
import math
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Quantity formula
# ─────────────────────────────────────────────────────────────────────────────

def calculate_quantity(
    cl_ratio: float,
    min_cl: float,
    risk_factor: float,
    max_qty: int,
) -> int:
    """
    Compute contract quantity from credit-to-loss ratio.

    Formula: floor((cl_ratio / min_cl) * risk_factor), capped at max_qty.
    Returns 0 for negative or zero cl_ratio.
    """
    if cl_ratio <= 0 or min_cl <= 0:
        return 0
    raw = (cl_ratio / min_cl) * risk_factor
    qty = min(math.floor(raw), max_qty)
    return max(qty, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Duplicate detection
# ─────────────────────────────────────────────────────────────────────────────

def is_duplicate(contract: dict, open_spreads: list) -> bool:
    """
    Check if a matching spread already exists in the portfolio snapshot.

    Match criteria: symbol + spread type + expiration (all three must match).
    """
    sym  = contract.get("symbol", "").upper()
    stype = contract.get("type", "").upper()
    exp  = contract.get("expiration", "")

    for sp in open_spreads:
        if (sp.get("symbol", "").upper() == sym
                and sp.get("type", "").upper() == stype
                and sp.get("expiration", "") == exp):
            return True
    return False
