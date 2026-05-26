# Income Generator (v1.8) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an autonomous spread-purchasing system that reads the daily strategy briefing, calculates per-symbol contract quantities via a config-driven formula, checks for duplicates, and places PCS/CCS orders through Robinhood.

**Architecture:** New `income_generator.py` orchestrator composes existing building blocks: `strategy.py` (parse briefing + scan contracts), `portfolio.py` (load snapshots for duplicate detection), and `trader.py` (place orders). Minor modifications to `trader.py` (parameterize quantity/dry_run) and `main.py` (add CLI commands). Config extends the flat `config.yaml`.

**Tech Stack:** Python 3.9+, robin_stocks, PyYAML, pytest, unittest.mock

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `income_generator.py` | Create | `calculate_quantity()`, `is_duplicate()`, `generate_income()`, `show_config()`, `set_config()` |
| `trader.py:3264-3385` | Modify | Add `quantity` and `dry_run` params to `place_spread_order()` |
| `main.py:827-833` | Modify | Add `--generate-income` and `--income-config` to argparse mutex group |
| `main.py:1123-1131` | Modify | Add dispatch for new commands |
| `config.yaml:37` | Modify | Append 4 `ig_*` keys |
| `tests/test_income_generator.py` | Create | Unit + integration tests |

---

### Task 1: Add `ig_*` config keys

**Files:**
- Modify: `config.yaml:37` (append after last line)

- [ ] **Step 1: Append income generator config block**

Add these lines at the end of `config.yaml`:

```yaml

# -- Income Generator (v1.8) -------------------------------------------------
ig_min_cl_ratio: 0.10            # min credit/loss ratio to consider a spread
ig_risk_factor: 1.0              # quantity multiplier (0.5=conservative, 2.0=aggressive)
ig_max_contracts_per_equity: 5   # hard cap on contracts per symbol per run
ig_enabled: true                 # master switch (false = force preview mode)
```

- [ ] **Step 2: Verify config loads**

Run:
```bash
python3 -c "from utils import load_config; c = load_config(reload=True); print(c.get('ig_min_cl_ratio'), c.get('ig_risk_factor'), c.get('ig_max_contracts_per_equity'), c.get('ig_enabled'))"
```
Expected: `0.1 1.0 5 True`

- [ ] **Step 3: Commit**

```bash
git add config.yaml
git commit -m "config: add Income Generator (v1.8) ig_* keys"
```

---

### Task 2: Quantity formula + duplicate detection (TDD)

**Files:**
- Create: `income_generator.py`
- Create: `tests/test_income_generator.py`

- [ ] **Step 1: Write failing tests for `calculate_quantity`**

Create `tests/test_income_generator.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestCalculateQuantity -v
```
Expected: `ModuleNotFoundError: No module named 'income_generator'` (all fail)

- [ ] **Step 3: Implement `calculate_quantity` and `is_duplicate`**

Create `income_generator.py`:

```python
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
```

- [ ] **Step 4: Run quantity tests to verify they pass**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestCalculateQuantity -v
```
Expected: all 10 tests PASS

- [ ] **Step 5: Write failing tests for `is_duplicate`**

Append to `tests/test_income_generator.py`:

```python
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
```

- [ ] **Step 6: Run duplicate tests to verify they pass**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestIsDuplicate -v
```
Expected: all 5 tests PASS (implementation already handles these cases)

- [ ] **Step 7: Commit**

```bash
git add income_generator.py tests/test_income_generator.py
git commit -m "feat(ig): add quantity formula and duplicate detection with tests"
```

---

### Task 3: Parameterize `place_spread_order()` in trader.py

**Files:**
- Modify: `trader.py:3264-3385`

- [ ] **Step 1: Add `quantity` and `dry_run` parameters to function signature**

In `trader.py`, change the function signature at line 3264 from:

```python
def place_spread_order(symbol: str, rec: dict, spread_type: str,
                       prompt: bool = True) -> bool:
```

to:

```python
def place_spread_order(symbol: str, rec: dict, spread_type: str,
                       prompt: bool = True, quantity: int = 1,
                       dry_run: bool = False) -> bool:
```

- [ ] **Step 2: Update the docstring**

Replace the existing docstring (lines 3266-3279) with:

```python
    """
    Place a new PCS (Bull Put Spread) or CCS (Bear Call Spread) order.

    rec: recommendation dict from scan_pcs / scan_ccs:
        short_leg: {strike, bid, ask, mid, ...}
        long_leg:  {strike, bid, ask, mid, ...}
        expiration: YYYY-MM-DD
        net_credit: float (per share)
        spread_size: float

    spread_type: "PCS" or "CCS"
    prompt: if True, show order summary and require y/n before submitting.
    quantity: number of contracts to place (default 1).
    dry_run: if True, print order summary but skip Robinhood API call.

    Returns True on successful order placement (or dry-run), False otherwise.
    """
```

- [ ] **Step 3: Update the prompt block to show quantity**

Replace lines 3314-3329 (the `if prompt:` block) with:

```python
    if prompt or dry_run:
        print(f"\n{'─' * 72}")
        print(f"  {label} Order for {symbol}  ×{quantity}")
        print(f"{'─' * 72}")
        print(f"  SELL {opt_type.upper()}  ${short_strike:.2f}  exp {expiration}"
              f"  @ ${short_mid:.2f}/sh")
        print(f"  BUY  {opt_type.upper()}  ${long_strike:.2f}  exp {expiration}"
              f"  @ ${long_mid:.2f}/sh")
        print(f"{'─' * 72}")
        print(f"  Net Credit:   ${net_credit:.2f}/sh  →  "
              f"${net_ct_total:.2f}/ct  →  ${net_ct_total * quantity:.2f} total")
        print(f"  Spread Width: ${spread_width:.2f}")
        print(f"{'─' * 72}")
        if dry_run:
            print(f"  [DRY RUN] Order not placed.\n")
            return True
        if prompt:
            answer = input("  Place this order? [y/N]: ").strip().lower()
            if answer != "y":
                print("  Aborted.\n")
                return False
```

- [ ] **Step 4: Replace the hardcoded `quantity=1` in the API call**

At line 3364, change:

```python
            quantity=1,
```

to:

```python
            quantity=quantity,
```

- [ ] **Step 5: Update the logger line to include quantity**

At line 3355-3358, replace:

```python
        logger.info(
            f"[{spread_type} ADD] STO ${short_strike_s} / BTO ${long_strike_s} "
            f"{opt_type.upper()} {expiration} {symbol} @ ${net_credit:.2f}/sh net credit"
        )
```

with:

```python
        logger.info(
            f"[{spread_type} ADD] STO ${short_strike_s} / BTO ${long_strike_s} "
            f"{opt_type.upper()} {expiration} {symbol} ×{quantity} "
            f"@ ${net_credit:.2f}/sh net credit"
        )
```

- [ ] **Step 6: Run existing spread order tests to confirm no regressions**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_trader.py -v -k "spread" 2>&1 | tail -30
```
Expected: all existing spread tests PASS (defaults are backward-compatible)

- [ ] **Step 7: Commit**

```bash
git add trader.py
git commit -m "feat(trader): parameterize place_spread_order with quantity and dry_run"
```

---

### Task 4: `generate_income()` orchestrator (TDD)

**Files:**
- Modify: `income_generator.py`
- Modify: `tests/test_income_generator.py`

- [ ] **Step 1: Write failing test for happy-path orchestration**

Append to `tests/test_income_generator.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestGenerateIncome -v
```
Expected: `ImportError: cannot import name 'generate_income'` (all fail)

- [ ] **Step 3: Implement `generate_income()` orchestrator**

Append to `income_generator.py`:

```python
# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def generate_income(
    symbol_filter: Optional[str] = None,
    live: bool = False,
    config: Optional[dict] = None,
) -> dict:
    """
    Run the income generation workflow.

    Parameters
    ----------
    symbol_filter : optional symbol to restrict processing to
    live          : if True (--add), place real orders; False = preview/dry-run
    config        : loaded config dict (must contain ig_* keys)

    Returns
    -------
    Summary dict: {placed, failed, skipped_duplicate, skipped_threshold,
                   no_contract, total_credit, details}
    """
    from datetime import date as _date
    from strategy import parse_strategy_table, scan_strategy_recommendations
    from portfolio import load_open_spreads_detail_snapshot
    from trader import place_spread_order

    config = config or {}
    min_cl     = float(config.get("ig_min_cl_ratio", 0.10))
    risk_factor = float(config.get("ig_risk_factor", 1.0))
    max_qty    = int(config.get("ig_max_contracts_per_equity", 5))
    enabled    = config.get("ig_enabled", True)

    # ig_enabled=False forces preview even if live=True
    dry_run = not live or not enabled

    today = _date.today().isoformat()
    mode_label = "LIVE MODE" if not dry_run else "PREVIEW MODE"
    if not enabled and live:
        print(f"\n  ⚠️  ig_enabled is false — forcing preview mode\n")

    print(f"\n{'=' * 60}")
    print(f"  Income Generator — {today}  [{mode_label}]")
    print(f"{'=' * 60}")

    # 1. Parse strategy recommendations from daily briefing
    parsed = parse_strategy_table(filter_sym=symbol_filter)
    if not parsed:
        print(f"  No PCS/CCS strategy recommendations found in today's briefing.\n")
        return {"placed": 0, "failed": 0, "skipped_duplicate": 0,
                "skipped_threshold": 0, "no_contract": 0, "total_credit": 0.0,
                "details": []}

    print(f"  Strategy hints: {len(parsed)} parsed from daily briefing")

    # 2. Scan for actual contracts
    scanned = scan_strategy_recommendations(parsed, config)

    found = [r for r in scanned if not r.get("no_contract")]
    print(f"  Contracts found: {len(found)} of {len(parsed)} symbols had qualifying spreads\n")

    # 3. Load portfolio for duplicate detection
    open_spreads = load_open_spreads_detail_snapshot()

    # Check snapshot staleness
    # (load_open_spreads_detail_snapshot returns [] with log on missing)
    # Staleness is detected by the snapshot filename date — handled in logs.

    # 4. Process each scanned contract
    summary = {
        "placed": 0, "failed": 0, "skipped_duplicate": 0,
        "skipped_threshold": 0, "no_contract": 0, "total_credit": 0.0,
        "details": [],
    }

    for rec in scanned:
        symbol = rec["symbol"]
        stype  = rec.get("type", "")

        # No qualifying contract found by scanner
        if rec.get("no_contract"):
            print(f"  {symbol:>6s}  {stype}  — no qualifying contract found\n")
            summary["no_contract"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "no_contract"})
            continue

        expiration = rec.get("expiration", "")
        dte        = rec.get("dte", 0)
        cl_ratio   = rec.get("credit_to_loss_ratio", 0.0)
        net_credit = rec.get("net_credit", 0.0)
        net_total  = rec.get("net_credit_total", 0.0)
        short_leg  = rec.get("short_leg", {})
        long_leg   = rec.get("long_leg", {})

        # Print contract details
        print(f"  {symbol:>6s}  {stype}  {expiration} ({dte}d)")
        print(f"        Short ${short_leg.get('strike', 0):.2f} / "
              f"Long ${long_leg.get('strike', 0):.2f}  |  "
              f"Credit ${net_credit:.2f}/sh")

        # Duplicate check
        if is_duplicate(rec, open_spreads):
            print(f"        SKIP — duplicate spread already open "
                  f"(exp {expiration})\n")
            summary["skipped_duplicate"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "duplicate"})
            continue

        # Quantity calculation
        qty = calculate_quantity(cl_ratio, min_cl, risk_factor, max_qty)
        if qty == 0:
            print(f"        C/L: {cl_ratio:.2f}  →  qty: 0  |  "
                  f"SKIP (below min C/L {min_cl})\n")
            summary["skipped_threshold"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "threshold"})
            continue

        credit_total = net_total * qty
        print(f"        C/L: {cl_ratio:.2f}  →  qty: {qty}  |  "
              f"Total credit: ${credit_total:.2f}")

        # Place order
        success = place_spread_order(
            symbol=symbol,
            rec=rec,
            spread_type=stype,
            prompt=False,
            quantity=qty,
            dry_run=dry_run,
        )

        if success:
            if dry_run:
                print(f"        📋 Would place order (preview)\n")
            # else: place_spread_order already printed ✅
            else:
                print()
            summary["placed"] += 1
            summary["total_credit"] += credit_total
        else:
            summary["failed"] += 1
            print()

        summary["details"].append({
            "symbol": symbol, "type": stype, "quantity": qty,
            "credit": credit_total,
            "action": "placed" if success else "failed",
        })

    # 5. Print summary
    print(f"{'=' * 60}")
    parts = []
    if summary["placed"]:
        label = "placed" if not dry_run else "would place"
        parts.append(f"{summary['placed']} {label}")
    if summary["failed"]:
        parts.append(f"{summary['failed']} failed")
    if summary["skipped_threshold"]:
        parts.append(f"{summary['skipped_threshold']} below threshold")
    if summary["skipped_duplicate"]:
        parts.append(f"{summary['skipped_duplicate']} duplicate")
    if summary["no_contract"]:
        parts.append(f"{summary['no_contract']} no contract")
    print(f"  Summary: {', '.join(parts)}")
    if dry_run and summary["placed"]:
        print(f"  → Re-run with --add to execute orders")
    print(f"{'=' * 60}\n")

    return summary
```

- [ ] **Step 4: Run orchestrator tests to verify they pass**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestGenerateIncome -v
```
Expected: all 8 tests PASS

- [ ] **Step 5: Commit**

```bash
git add income_generator.py tests/test_income_generator.py
git commit -m "feat(ig): add generate_income orchestrator with tests"
```

---

### Task 5: Config display and update functions (TDD)

**Files:**
- Modify: `income_generator.py`
- Modify: `tests/test_income_generator.py`

- [ ] **Step 1: Write failing tests for `show_config` and `set_config`**

Append to `tests/test_income_generator.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestShowConfig tests/test_income_generator.py::TestSetConfig -v
```
Expected: `ImportError: cannot import name 'show_config'` (all fail)

- [ ] **Step 3: Implement `show_config` and `set_config`**

Append to `income_generator.py` (before the orchestrator section):

```python
# ─────────────────────────────────────────────────────────────────────────────
# Config display & update
# ─────────────────────────────────────────────────────────────────────────────

# Valid ig_* keys with (description, type)
_IG_CONFIG_KEYS = {
    "ig_min_cl_ratio":            ("Min credit/loss ratio threshold",                    float),
    "ig_risk_factor":             ("Quantity multiplier (0.5=conservative, 2.0=aggressive)", float),
    "ig_max_contracts_per_equity": ("Max contracts per symbol per run",                   int),
    "ig_enabled":                 ("Master switch (false = preview only)",                bool),
}


def show_config(config: dict) -> None:
    """Print current income generator configuration."""
    print(f"\n{'=' * 60}")
    print(f"  Income Generator Configuration")
    print(f"{'=' * 60}")
    for key, (desc, _) in _IG_CONFIG_KEYS.items():
        val = config.get(key, "NOT SET")
        padding = "." * (35 - len(key))
        print(f"  {key} {padding} {val!s:<8s} {desc}")
    print(f"{'=' * 60}\n")


def set_config(key_value: str, config_path=None) -> bool:
    """
    Update a single ig_* config key in config.yaml.

    key_value: string like "ig_risk_factor=0.5"
    config_path: override path for testing (default: project config.yaml)

    Returns True on success, False on validation error.
    """
    import yaml
    from pathlib import Path as _Path

    if config_path is None:
        config_path = _Path(__file__).parent / "config.yaml"
    else:
        config_path = _Path(config_path)

    if "=" not in key_value:
        print(f"  ❌  Expected KEY=VALUE format, got: {key_value}\n")
        return False

    key, raw_value = key_value.split("=", 1)
    key = key.strip()
    raw_value = raw_value.strip()

    if key not in _IG_CONFIG_KEYS:
        print(f"  ❌  Unknown config key: {key}")
        print(f"      Valid keys: {', '.join(_IG_CONFIG_KEYS.keys())}\n")
        return False

    _, expected_type = _IG_CONFIG_KEYS[key]
    try:
        if expected_type is bool:
            if raw_value.lower() in ("true", "1", "yes"):
                value = True
            elif raw_value.lower() in ("false", "0", "no"):
                value = False
            else:
                raise ValueError(f"expected true/false, got '{raw_value}'")
        elif expected_type is int:
            value = int(raw_value)
        else:
            value = float(raw_value)
    except (ValueError, TypeError) as e:
        print(f"  ❌  Invalid value for {key}: {e}\n")
        return False

    # Read, update, write
    with open(config_path) as f:
        data = yaml.safe_load(f) or {}
    old_value = data.get(key, "NOT SET")
    data[key] = value
    with open(config_path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    print(f"  ✅  {key}: {old_value} → {value}\n")
    return True
```

- [ ] **Step 4: Run config tests to verify they pass**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py::TestShowConfig tests/test_income_generator.py::TestSetConfig -v
```
Expected: all 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add income_generator.py tests/test_income_generator.py
git commit -m "feat(ig): add show_config and set_config with tests"
```

---

### Task 6: CLI integration in main.py

**Files:**
- Modify: `main.py:1-64` (usage docstring)
- Modify: `main.py:827-833` (argparse group — add new entries)
- Modify: `main.py:942-951` (primary_flags list)
- Modify: `main.py:1123-1131` (dispatch block)

- [ ] **Step 1: Add new commands to the argparse mutex group**

In `main.py`, after the `--strategy` argument (line 829-830) and before `--pull-portfolio` (line 831), insert:

```python
    group.add_argument(
        "--generate-income", nargs="?", const="ALL", metavar="SYMBOL",
        help="Income generator: preview plan (default) or place orders with --add",
    )
    group.add_argument(
        "--income-config", nargs="?", const="SHOW", metavar="KEY=VALUE",
        help="Show or update income generator config (e.g. --income-config ig_risk_factor=0.5)",
    )
```

- [ ] **Step 2: Add new flags to `primary_flags` list**

In `main.py`, in the `primary_flags` list (around line 942-950), add after `args.strategy is not None,`:

```python
        args.generate_income is not None,
        args.income_config is not None,
```

- [ ] **Step 3: Add dispatch block for new commands**

In `main.py`, after the `elif args.strategy is not None:` block (line 1123-1125) and before `elif args.pull_portfolio:` (line 1126), insert:

```python
    elif args.generate_income is not None:
        sym = None if args.generate_income == "ALL" else args.generate_income.upper()
        cmd_generate_income(sym, live=args.add)
    elif args.income_config is not None:
        if args.income_config == "SHOW":
            cmd_income_config_show()
        else:
            cmd_income_config_set(args.income_config)
```

- [ ] **Step 4: Add `cmd_generate_income`, `cmd_income_config_show`, and `cmd_income_config_set` functions**

In `main.py`, after the `cmd_strategy` function (around line 455), insert:

```python
def cmd_generate_income(symbol: Optional[str] = None, live: bool = False):
    """Run the income generator: preview by default, --add to execute."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from income_generator import generate_income
    config = load_config()
    generate_income(symbol_filter=symbol, live=live, config=config)


def cmd_income_config_show():
    """Display current income generator configuration."""
    from utils import load_config
    from income_generator import show_config
    config = load_config()
    show_config(config)


def cmd_income_config_set(key_value: str):
    """Update an income generator config key."""
    from income_generator import set_config
    set_config(key_value)
```

- [ ] **Step 5: Update the usage docstring**

In the docstring at the top of `main.py` (lines 1-64), add after the `--strategy` line (around line 63), before the closing `"""`:

```python
  python main.py --generate-income                                     # Preview income plan (dry-run, no orders placed)
  python main.py --generate-income NVDA                                # Preview for a single symbol
  python main.py --generate-income --add                               # Execute: place orders for all recommended symbols
  python main.py --generate-income NVDA --add                          # Place orders for one symbol only
  python main.py --income-config                                       # Show income generator config
  python main.py --income-config ig_risk_factor=0.5                    # Update a config value
```

- [ ] **Step 6: Update the argparse epilog help text**

In `main.py`, in the epilog string of `ArgumentParser` (around lines 726-791), add a new section before the closing `"""`:

```
Income generator:
  --generate-income                         Preview income plan from daily strategy briefing
  --generate-income SYMBOL                  Preview for one symbol only
  --generate-income --add                   Execute: place all recommended spread orders
  --generate-income SYMBOL --add            Execute for one symbol only
  --income-config                           Show income generator config
  --income-config ig_risk_factor=0.5        Update a config value
```

- [ ] **Step 7: Verify CLI parsing works**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python main.py --help 2>&1 | grep -A2 "generate-income\|income-config"
```
Expected: both `--generate-income` and `--income-config` appear in help output.

- [ ] **Step 8: Commit**

```bash
git add main.py
git commit -m "feat(ig): add --generate-income and --income-config CLI commands"
```

---

### Task 7: Full integration test + regression check

**Files:**
- All existing test files

- [ ] **Step 1: Run the complete income generator test suite**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/test_income_generator.py -v
```
Expected: all tests PASS (quantity formula 10, duplicate detection 5, orchestrator 8, config 5 = 28 total)

- [ ] **Step 2: Run the full project test suite for regressions**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python -m pytest tests/ -v --tb=short 2>&1 | tail -40
```
Expected: all existing tests PASS, no regressions from `trader.py` signature change.

- [ ] **Step 3: Smoke-test the CLI in preview mode**

Run:
```bash
cd /Users/ambrish/Code/options_trader && python main.py --income-config
```
Expected: prints the 4 `ig_*` config keys with current values.

Run:
```bash
cd /Users/ambrish/Code/options_trader && python main.py --generate-income 2>&1 | head -20
```
Expected: prints the Income Generator header in PREVIEW MODE. May show "No PCS/CCS strategy recommendations found" if no briefing exists for today, or shows strategy hints and scanned contracts.

- [ ] **Step 4: Final commit (if any fixes needed)**

```bash
git add -A
git commit -m "fix(ig): address integration test findings"
```

Only commit this if step 2 or 3 revealed issues. Otherwise skip.

- [ ] **Step 5: Create feature branch and PR**

```bash
git checkout -b feat/income-generator
git push -u origin feat/income-generator
gh pr create --title "feat: Income Generator (v1.8)" --body "$(cat <<'EOF'
## Summary
- New `income_generator.py` module: autonomous spread purchasing from daily strategy briefing
- Config-driven quantity formula: `floor((C/L ratio / min_cl) × risk_factor)`, capped
- Duplicate detection against open_spreads_detail portfolio snapshot
- CLI: `--generate-income [SYMBOL]` (preview default), `--generate-income --add` (execute)
- CLI: `--income-config [KEY=VALUE]` for viewing/updating ig_* config
- Safety: preview-by-default, `ig_enabled` master switch, per-symbol error isolation
- `place_spread_order()` parameterized with `quantity` and `dry_run` (backward-compatible)

## Test plan
- [ ] `pytest tests/test_income_generator.py -v` — 28 unit + integration tests
- [ ] `pytest tests/ -v` — full suite regression check
- [ ] `python main.py --income-config` — verify config display
- [ ] `python main.py --generate-income` — preview mode smoke test
- [ ] `python main.py --generate-income NVDA` — single-symbol filter
- [ ] `python main.py --income-config ig_risk_factor=0.5` — config update

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```
