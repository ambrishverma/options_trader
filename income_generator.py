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

import glob
import json
import logging
import math
from datetime import datetime, timedelta
from pathlib import Path
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


# ─────────────────────────────────────────────────────────────────────────────
# Module-level imports for orchestrator dependencies (enables mock patching)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from strategy import parse_strategy_table, scan_strategy_recommendations
    from portfolio import load_open_spreads_detail_snapshot
    from trader import place_spread_order
except ImportError:
    parse_strategy_table = None  # type: ignore[assignment]
    scan_strategy_recommendations = None  # type: ignore[assignment]
    load_open_spreads_detail_snapshot = None  # type: ignore[assignment]
    place_spread_order = None  # type: ignore[assignment]


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot freshness check
# ─────────────────────────────────────────────────────────────────────────────

_SNAPSHOT_DIR = Path(__file__).parent / "snapshots"
_STALE_HOURS = 24


def _check_snapshot_freshness() -> None:
    """
    Warn if the most recent open_spreads_detail snapshot is older than 24h.

    Reads the ``pulled_at`` ISO timestamp from the latest snapshot file.
    Prints a warning to stdout when stale; prints nothing otherwise.
    """
    files = sorted(
        glob.glob(str(_SNAPSHOT_DIR / "open_spreads_detail_*.json")),
        reverse=True,
    )
    if not files:
        return  # no snapshot at all — handled elsewhere as "no duplicate filter"

    try:
        with open(files[0]) as f:
            data = json.load(f)
        pulled_at_str = data.get("pulled_at", "")
        if not pulled_at_str:
            return
        pulled_at = datetime.fromisoformat(pulled_at_str)
        age = datetime.now() - pulled_at
        if age > timedelta(hours=_STALE_HOURS):
            hours = age.total_seconds() / 3600
            print(f"  [IG] WARNING: portfolio snapshot is >{_STALE_HOURS}h old "
                  f"(from {pulled_at_str[:19]}, {hours:.0f}h ago)")
    except Exception as e:
        logger.debug(f"Could not check snapshot freshness: {e}")


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

    config = config or {}
    min_cl      = float(config.get("ig_min_cl_ratio", 0.10))
    risk_factor = float(config.get("ig_risk_factor", 1.0))
    max_qty     = int(config.get("ig_max_contracts_per_equity", 5))
    enabled     = config.get("ig_enabled", True)

    # ig_enabled=False forces preview even if live=True
    dry_run = not live or not enabled

    today = _date.today().isoformat()
    mode_label = "LIVE MODE" if not dry_run else "PREVIEW MODE"
    if not enabled and live:
        print(f"\n  ig_enabled is false -- forcing preview mode\n")

    print(f"\n{'=' * 60}")
    print(f"  Income Generator -- {today}  [{mode_label}]")
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
    _check_snapshot_freshness()

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
            print(f"  {symbol:>6s}  {stype}  -- no qualifying contract found\n")
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
            print(f"        SKIP -- duplicate spread already open "
                  f"(exp {expiration})\n")
            summary["skipped_duplicate"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "duplicate"})
            continue

        # Quantity calculation
        qty = calculate_quantity(cl_ratio, min_cl, risk_factor, max_qty)
        if qty == 0:
            print(f"        C/L: {cl_ratio:.2f}  ->  qty: 0  |  "
                  f"SKIP (below min C/L {min_cl})\n")
            summary["skipped_threshold"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "threshold"})
            continue

        credit_total = net_total * qty
        print(f"        C/L: {cl_ratio:.2f}  ->  qty: {qty}  |  "
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
                print(f"        Would place order (preview)\n")
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
        print(f"  -> Re-run with --add to execute orders")
    print(f"{'=' * 60}\n")

    return summary
