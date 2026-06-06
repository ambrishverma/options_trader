"""
income_generator.py — Autonomous Spread Purchasing (v1.9)
=========================================================
Loads pre-scanned strategy recommendations from the daily pipeline run
(persisted to snapshots/strategy_recs_YYYY-MM-DD.json), calculates
per-symbol contract quantities via a config-driven formula, checks for
duplicates against the portfolio snapshot, and places PCS/CCS orders.

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
    "ig_min_daily_income_goal":   ("Daily income target ($); 0 = no goal chasing",       float),
    "ig_cl_ratio_buffer":         ("Max CL ratio buffer below min for goal chasing",     float),
    "auto_income":                ("Auto-purchase in daily pipeline run",                 bool),
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
    Uses line-level replacement to preserve YAML comments.
    """
    import re

    if config_path is None:
        config_path = Path(__file__).parent / "config.yaml"
    else:
        config_path = Path(config_path)

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

    # Line-level replacement to preserve YAML comments and formatting
    lines = config_path.read_text().splitlines(keepends=True)
    pattern = re.compile(rf"^{re.escape(key)}\s*:")
    old_value = "NOT SET"
    found = False
    for i, line in enumerate(lines):
        if pattern.match(line):
            # Extract old value (before any inline comment)
            after_colon = line.split(":", 1)[1]
            comment = ""
            if "#" in after_colon:
                val_part, comment = after_colon.split("#", 1)
                old_value = val_part.strip()
                comment = f"  # {comment.strip()}"
            else:
                old_value = after_colon.strip()
            # Format the YAML value appropriately
            yaml_val = str(value).lower() if isinstance(value, bool) else str(value)
            lines[i] = f"{key}: {yaml_val}{comment}\n"
            found = True
            break

    if not found:
        yaml_val = str(value).lower() if isinstance(value, bool) else str(value)
        lines.append(f"{key}: {yaml_val}\n")
        old_value = "NOT SET"

    config_path.write_text("".join(lines))

    print(f"  ✅  {key}: {old_value} → {value}\n")
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Module-level imports for orchestrator dependencies (enables mock patching)
# ─────────────────────────────────────────────────────────────────────────────

try:
    from portfolio import load_open_spreads_detail_snapshot
    from trader import place_spread_order
except ImportError:
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

def _process_rec(
    rec: dict,
    cl_threshold: float,
    risk_factor: float,
    max_qty: int,
    open_spreads: list,
    dry_run: bool,
    summary: dict,
    pass_label: str = "",
) -> bool:
    """
    Process a single strategy recommendation: duplicate check, quantity
    calculation, order placement.  Mutates *summary* in place.

    Returns True if the rec was successfully placed (credit earned).
    """
    symbol = rec["symbol"]
    stype  = rec.get("type", "")

    expiration = rec.get("expiration", "")
    dte        = rec.get("dte", 0)
    cl_ratio   = rec.get("credit_to_loss_ratio", 0.0)
    net_credit = rec.get("net_credit", 0.0)
    net_total  = rec.get("net_credit_total", 0.0)
    short_leg  = rec.get("short_leg", {})
    long_leg   = rec.get("long_leg", {})

    # Print contract details
    tag = f" [{pass_label}]" if pass_label else ""
    print(f"  {symbol:>6s}  {stype}  {expiration} ({dte}d){tag}")
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
        return False

    # Quantity calculation — use cl_threshold as divisor
    qty = calculate_quantity(cl_ratio, cl_threshold, risk_factor, max_qty)
    if qty == 0:
        print(f"        C/L: {cl_ratio:.2f}  ->  qty: 0  |  "
              f"SKIP (below threshold {cl_threshold:.2f})\n")
        summary["skipped_threshold"] += 1
        summary["details"].append({"symbol": symbol, "type": stype,
                                    "action": "threshold"})
        return False

    credit_total = net_total * qty
    max_loss     = rec.get("max_loss", 0.0)
    collateral   = round(max_loss * qty, 2)
    print(f"        C/L: {cl_ratio:.2f}  ->  qty: {qty}  |  "
          f"Total credit: ${credit_total:.2f}  |  Collateral: ${collateral:.2f}")

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
        summary["total_collateral"] += collateral
    else:
        summary["failed"] += 1
        print()

    summary["details"].append({
        "symbol": symbol, "type": stype, "quantity": qty,
        "credit": credit_total, "collateral": collateral,
        "action": "placed" if success else "failed",
    })
    return success


def generate_income(
    symbol_filter: Optional[str] = None,
    live: bool = False,
    config: Optional[dict] = None,
) -> dict:
    """
    Run the goal-oriented income generation workflow.

    Two-pass approach:
      Pass 1: Purchase all recs with CL ratio >= ig_min_cl_ratio (always,
              regardless of income goal).  Sorted by CL ratio high → low.
      Pass 2: If ig_min_daily_income_goal > 0 and not yet met, lower the
              CL threshold by 0.01 decrements and purchase additional recs
              until the goal is met or the floor (ig_min_cl_ratio -
              ig_cl_ratio_buffer) is reached.

    Parameters
    ----------
    symbol_filter : optional symbol to restrict processing to
    live          : if True (--add), place real orders; False = preview/dry-run
    config        : loaded config dict (must contain ig_* keys)

    Returns
    -------
    Summary dict: {placed, failed, skipped_duplicate, skipped_threshold,
                   no_contract, total_credit, total_collateral, details}
    """
    config = config or {}
    min_cl      = float(config.get("ig_min_cl_ratio", 0.10))
    risk_factor = float(config.get("ig_risk_factor", 1.0))
    max_qty     = int(config.get("ig_max_contracts_per_equity", 5))
    enabled     = config.get("ig_enabled", True)
    income_goal = float(config.get("ig_min_daily_income_goal", 0.0))
    cl_buffer   = float(config.get("ig_cl_ratio_buffer", 0.0))

    # Guard against negative floor
    cl_floor = max(min_cl - cl_buffer, 0.01) if cl_buffer > 0 else min_cl

    # ig_enabled=False forces preview even if live=True
    dry_run = not live or not enabled

    # Use ET date to match the pipeline's snapshot date
    from zoneinfo import ZoneInfo
    today = datetime.now(tz=ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    mode_label = "LIVE MODE" if not dry_run else "PREVIEW MODE"
    if not enabled and live:
        print(f"\n  ig_enabled is false -- forcing preview mode\n")

    print(f"\n{'=' * 60}")
    print(f"  Income Generator -- {today}  [{mode_label}]")
    if income_goal > 0:
        print(f"  Goal: ${income_goal:.2f}/day  |  "
              f"CL range: {min_cl:.2f} → {cl_floor:.2f}")
    print(f"{'=' * 60}")

    # 1. Load pre-scanned strategy recommendations from today's pipeline run
    from utils import load_strategy_recs_snapshot
    scanned = load_strategy_recs_snapshot(today)

    # Apply symbol filter if provided
    if symbol_filter and scanned:
        scanned = [r for r in scanned if r.get("symbol", "").upper() == symbol_filter.upper()]

    if not scanned:
        print(f"  No strategy recommendations found (run --run first to generate).\n")
        return {"placed": 0, "failed": 0, "skipped_duplicate": 0,
                "skipped_threshold": 0, "no_contract": 0, "total_credit": 0.0,
                "total_collateral": 0.0, "details": []}

    found = [r for r in scanned if not r.get("no_contract")]
    print(f"  Strategy recs loaded: {len(scanned)} total, {len(found)} with qualifying contracts\n")

    # 2. Load portfolio for duplicate detection
    open_spreads = load_open_spreads_detail_snapshot()
    _check_snapshot_freshness()

    # 3. Separate no-contract recs from actionable ones
    summary = {
        "placed": 0, "failed": 0, "skipped_duplicate": 0,
        "skipped_threshold": 0, "no_contract": 0, "total_credit": 0.0,
        "total_collateral": 0.0,
        "details": [],
    }

    actionable = []
    for rec in scanned:
        if rec.get("no_contract"):
            symbol = rec["symbol"]
            stype  = rec.get("type", "")
            print(f"  {symbol:>6s}  {stype}  -- no qualifying contract found\n")
            summary["no_contract"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "no_contract"})
        else:
            actionable.append(rec)

    # 4. Sort actionable recs by CL ratio high → low
    actionable.sort(
        key=lambda r: r.get("credit_to_loss_ratio", 0.0), reverse=True
    )

    # ── Pass 1: Purchase all recs at or above ig_min_cl_ratio ────────────────
    pass1_processed = set()  # track indices of recs processed in Pass 1
    if actionable:
        print(f"  ── Pass 1: CL ratio ≥ {min_cl:.2f} ──")

    for i, rec in enumerate(actionable):
        cl_ratio = rec.get("credit_to_loss_ratio", 0.0)
        if cl_ratio < min_cl:
            break  # sorted high→low, so all remaining are below threshold
        pass1_processed.add(i)
        _process_rec(rec, min_cl, risk_factor, max_qty,
                     open_spreads, dry_run, summary)

    # ── Pass 2: Goal chase — lower CL threshold in 0.01 decrements ──────────
    # Only if: income goal > 0, goal not yet met, and buffer > 0
    if (income_goal > 0
            and summary["total_credit"] < income_goal
            and cl_buffer > 0):

        # Collect recs not yet processed that are within the buffer zone
        remaining = [
            (i, rec) for i, rec in enumerate(actionable)
            if i not in pass1_processed
            and rec.get("credit_to_loss_ratio", 0.0) >= cl_floor
        ]
        # Already sorted high → low from earlier sort

        if remaining:
            print(f"\n  ── Pass 2: Goal chase (${summary['total_credit']:.2f}"
                  f" / ${income_goal:.2f} target) ──")

            # Walk down CL threshold in 0.01 steps
            # At each step, process recs with CL >= current_threshold that
            # haven't been processed yet.
            current_threshold = round(min_cl - 0.01, 4)

            while current_threshold >= cl_floor:
                if summary["total_credit"] >= income_goal:
                    print(f"  🎯 Goal met: ${summary['total_credit']:.2f}"
                          f" ≥ ${income_goal:.2f}\n")
                    break

                # Find recs at this threshold level
                # (CL ratio >= current_threshold but < previous threshold)
                batch = [
                    (i, rec) for i, rec in remaining
                    if i not in pass1_processed
                    and rec.get("credit_to_loss_ratio", 0.0) >= current_threshold
                ]

                for i, rec in batch:
                    if summary["total_credit"] >= income_goal:
                        break
                    pass1_processed.add(i)  # mark as processed
                    _process_rec(
                        rec, current_threshold, risk_factor, max_qty,
                        open_spreads, dry_run, summary,
                        pass_label=f"CL≥{current_threshold:.2f}",
                    )

                current_threshold = round(current_threshold - 0.01, 4)

        if summary["total_credit"] < income_goal:
            shortfall = income_goal - summary["total_credit"]
            print(f"\n  ⚠️  Goal NOT met: ${summary['total_credit']:.2f}"
                  f" / ${income_goal:.2f} (shortfall ${shortfall:.2f})\n")

    # Count recs that were never processed as skipped_threshold
    for i, rec in enumerate(actionable):
        if i not in pass1_processed and not rec.get("no_contract"):
            cl_ratio = rec.get("credit_to_loss_ratio", 0.0)
            symbol   = rec["symbol"]
            stype    = rec.get("type", "")
            summary["skipped_threshold"] += 1
            summary["details"].append({"symbol": symbol, "type": stype,
                                        "action": "threshold"})

    # ── Summary ──────────────────────────────────────────────────────────────
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
    if summary["total_credit"] > 0:
        print(f"  Total credit: ${summary['total_credit']:.2f}  |  "
              f"Collateral: ${summary['total_collateral']:.2f}")
    if income_goal > 0:
        status = "✅ MET" if summary["total_credit"] >= income_goal else "❌ NOT MET"
        print(f"  Income goal: ${income_goal:.2f} — {status}")
    if dry_run and summary["placed"]:
        print(f"  -> Re-run with --add to execute orders")
    print(f"{'=' * 60}\n")

    return summary
