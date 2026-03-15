"""
filters.py — Safe Mode Filters + Scoring
==========================================
Applies all Safe Mode filters to a flat list of option records,
then scores and ranks surviving candidates.

Safe Mode filter rules (all must pass):
  F1: OTM ≥ min_otm_pct (default 7%)           — strike must be ≥7% above current price
  F2: Bid ≥ min_bid (default $0.20)             — minimum premium liquidity
  F3: Open Interest ≥ min_open_interest (default 2) — minimum market activity
  F4: DTE in [1, lookahead_days] (default 21)   — within the 3-week window

Scoring formula:
  score = annualized_yield  (primary)
  tiebreaker = otm_pct      (more OTM = safer)

The output is a ranked list, one best-per-symbol per expiration.
"""

import logging
from typing import List

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Filter application
# ─────────────────────────────────────────────────────────────────────────────

def apply_safe_mode_filters(
    options: List[dict],
    min_otm_pct:      float = 7.0,
    min_bid:          float = 0.20,
    min_open_interest: int  = 2,
    lookahead_days:    int  = 21,
) -> List[dict]:
    """
    Apply Safe Mode filters to a list of option records.

    Returns:
        List of passing options, with a "fail_reasons" key on each rejected option
        removed (only passing options returned).
    """
    passed = []
    rejected_counts = {
        "F1_otm":  0,
        "F2_bid":  0,
        "F3_oi":   0,
        "F4_dte":  0,
    }

    for opt in options:
        reasons = []

        # F1: OTM percentage
        if opt["otm_pct"] < min_otm_pct:
            reasons.append(f"OTM {opt['otm_pct']:.1f}% < {min_otm_pct}%")
            rejected_counts["F1_otm"] += 1

        # F2: Minimum bid
        if opt["bid"] < min_bid:
            reasons.append(f"bid ${opt['bid']:.2f} < ${min_bid:.2f}")
            rejected_counts["F2_bid"] += 1

        # F3: Open interest
        if opt["open_interest"] < min_open_interest:
            reasons.append(f"OI {opt['open_interest']} < {min_open_interest}")
            rejected_counts["F3_oi"] += 1

        # F4: DTE in window
        if not (1 <= opt["dte"] <= lookahead_days):
            reasons.append(f"DTE {opt['dte']} outside [1, {lookahead_days}]")
            rejected_counts["F4_dte"] += 1

        if not reasons:
            passed.append(opt)

    total = len(options)
    logger.info(
        f"Filter results: {len(passed)}/{total} passed "
        f"[F1_otm: -{rejected_counts['F1_otm']}, "
        f"F2_bid: -{rejected_counts['F2_bid']}, "
        f"F3_oi: -{rejected_counts['F3_oi']}, "
        f"F4_dte: -{rejected_counts['F4_dte']}]"
    )
    return passed


# ─────────────────────────────────────────────────────────────────────────────
# Scoring and ranking
# ─────────────────────────────────────────────────────────────────────────────

def score_and_rank(options: List[dict]) -> List[dict]:
    """
    Score and rank options by annualized yield (desc), OTM% tiebreaker (desc).

    Also adds a "rank" field (1 = best).
    """
    if not options:
        return []

    # Sort: primary = annualized_yield (desc), tiebreaker = otm_pct (desc)
    ranked = sorted(
        options,
        key=lambda o: (o["annualized_yield"], o["otm_pct"]),
        reverse=True
    )

    for i, opt in enumerate(ranked, 1):
        opt["rank"] = i

    logger.info(f"Ranked {len(ranked)} options. "
                f"Top yield: {ranked[0]['annualized_yield']:.1f}% "
                f"({ranked[0]['symbol']} {ranked[0]['expiration']} "
                f"${ranked[0]['strike']}c)")
    return ranked


# ─────────────────────────────────────────────────────────────────────────────
# Best-per-symbol selector
# ─────────────────────────────────────────────────────────────────────────────

def best_per_symbol(options: List[dict]) -> List[dict]:
    """
    From a ranked list, select the single best (highest yield) option per symbol.
    Used for the "top recommendations" view in the email.
    """
    seen = {}
    for opt in options:  # already ranked, so first encounter = best
        sym = opt["symbol"]
        if sym not in seen:
            seen[sym] = opt
    return list(seen.values())


# ─────────────────────────────────────────────────────────────────────────────
# Safest-per-symbol selector (for 50/50 diversification)
# ─────────────────────────────────────────────────────────────────────────────

def safest_per_symbol(options: List[dict]) -> List[dict]:
    """
    For each symbol, select the option that is:
      - Furthest OTM (highest otm_pct), tiebreaker = latest expiration (highest dte)

    Used to find the "safety" option in the 50/50 diversification split.
    """
    by_symbol = {}
    for opt in options:
        sym = opt["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = opt
        else:
            existing = by_symbol[sym]
            # Prefer higher OTM; tiebreak on later expiry
            if opt["otm_pct"] > existing["otm_pct"] or (
                opt["otm_pct"] == existing["otm_pct"] and opt["dte"] > existing["dte"]
            ):
                by_symbol[sym] = opt

    return list(by_symbol.values())


# ─────────────────────────────────────────────────────────────────────────────
# Full filter pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_filters(
    raw_options: List[dict],
    config: dict,
) -> dict:
    """
    Full filtering pipeline. Returns a structured result dict.

    Args:
        raw_options: Flat list of all option records
        config: config.yaml dict with filter thresholds

    Returns:
        {
            "all_passing":  List of all passing options (ranked),
            "best_per_sym": List of best option per symbol,
            "safe_per_sym": List of safest option per symbol,
            "count_raw":    int,
            "count_passing": int,
        }
    """
    count_raw = len(raw_options)

    passing = apply_safe_mode_filters(
        raw_options,
        min_otm_pct=config.get("min_otm_pct", 7.0),
        min_bid=config.get("min_bid", 0.20),
        min_open_interest=config.get("min_open_interest", 2),
        lookahead_days=config.get("lookahead_days", 21),
    )

    ranked = score_and_rank(passing)

    return {
        "all_passing":   ranked,
        "best_per_sym":  best_per_symbol(ranked),
        "safe_per_sym":  safest_per_symbol(passing),
        "count_raw":     count_raw,
        "count_passing": len(ranked),
    }
