"""
collar.py — Collar Recommendation Pipeline
============================================
Finds self-financing collar pairs (covered call + long put, same expiration)
for large equity holdings (>$50K market value).

A collar qualifies when:
  - CC strike >= 10% OTM, LP strike <= 10% below current price
  - Both legs: OI > 5, same exact expiration date
  - CC mid - LP mid >= $0.10/share (self-financing with minimum net gain)
  - DTE: 28-112 calendar days (4-16 weeks)

Output per holding: list of collar pair dicts, one per calendar month,
highest net-gain pair selected. Falls back to best self-financing pair
(even if < $0.10) when no qualifying pairs exist, marked low_gain=True.
"""

import logging
import math
from datetime import datetime, date, timedelta
from typing import List, Optional

import yfinance as yf

logger = logging.getLogger(__name__)


def _safe_float(value, default: float = 0.0) -> float:
    """Convert value to float, treating NaN / None as default."""
    if value is None:
        return default
    try:
        f = float(value)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    if value is None:
        return default
    try:
        if math.isnan(float(value)):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def _filter_collar_pairs(candidates: List[dict], config: dict) -> List[dict]:
    """
    Apply all collar qualification filters to a list of candidate pairs.

    Filters (all must pass):
      - CC OTM >= collar_call_otm_min_pct (default 10%)
      - LP OTM <= collar_put_otm_max_pct below current price (default 10%)
      - CC OI > collar_min_open_interest - 1 (i.e. >= 6)
      - LP OI > collar_min_open_interest - 1
      - DTE in [collar_dte_min, collar_dte_max]
      - net_gain_per_share >= collar_min_net_gain_per_share

    Returns only passing pairs.
    """
    call_otm_min  = config.get("collar_call_otm_min_pct", 10.0)
    put_otm_max   = config.get("collar_put_otm_max_pct", 10.0)
    min_oi        = config.get("collar_min_open_interest", 6)
    min_net_gain  = config.get("collar_min_net_gain_per_share", 0.10)
    dte_min       = config.get("collar_dte_min", 28)
    dte_max       = config.get("collar_dte_max", 112)

    passing = []
    for pair in candidates:
        cc = pair["call_leg"]
        lp = pair["put_leg"]
        dte = pair["dte"]
        net = pair["net_gain_per_share"]

        # DTE window
        if not (dte_min <= dte <= dte_max):
            continue
        # Call must be >= 10% OTM
        if cc["otm_pct"] < call_otm_min:
            continue
        # Put must be within 10% below current price (protection_pct is negative)
        if abs(lp["protection_pct"]) > put_otm_max:
            continue
        # OI on both legs
        if cc["open_interest"] < min_oi or lp["open_interest"] < min_oi:
            continue
        # Self-financing + net gain floor
        if net < min_net_gain:
            continue

        passing.append(pair)

    return passing


def _deduplicate_by_month(pairs: List[dict]) -> List[dict]:
    """
    Keep the single highest net-gain pair per calendar month.
    Returns pairs ordered by expiration month ascending.
    """
    best_by_month: dict = {}
    for pair in pairs:
        month_key = pair["expiration"][:7]  # "YYYY-MM"
        existing = best_by_month.get(month_key)
        if existing is None or pair["net_gain_per_share"] > existing["net_gain_per_share"]:
            best_by_month[month_key] = pair

    return sorted(best_by_month.values(), key=lambda p: p["expiration"])


def _apply_fallback(
    symbol: str,
    all_pairs: List[dict],
    config: dict,
) -> Optional[dict]:
    """
    When no pairs pass the net-gain floor, return the single best self-financing
    pair (CC mid >= LP mid, net_gain >= $0.00), marked low_gain=True.
    Returns None if no self-financing pair exists at all.

    Note: OTM%, OI, and DTE criteria are still enforced here.
    Only the $0.10/share net gain floor is relaxed.
    """
    call_otm_min = config.get("collar_call_otm_min_pct", 10.0)
    put_otm_max  = config.get("collar_put_otm_max_pct", 10.0)
    min_oi       = config.get("collar_min_open_interest", 6)
    dte_min      = config.get("collar_dte_min", 28)
    dte_max      = config.get("collar_dte_max", 112)

    candidates = []
    for pair in all_pairs:
        cc  = pair["call_leg"]
        lp  = pair["put_leg"]
        dte = pair["dte"]
        net = pair["net_gain_per_share"]

        if not (dte_min <= dte <= dte_max):
            continue
        if cc["otm_pct"] < call_otm_min:
            continue
        if abs(lp["protection_pct"]) > put_otm_max:
            continue
        if cc["open_interest"] < min_oi or lp["open_interest"] < min_oi:
            continue
        if net < 0.0:  # only require self-financing (>= $0.00), not $0.10
            continue
        candidates.append(pair)

    if not candidates:
        logger.info(f"  {symbol}: no self-financing pairs found — omitting from report")
        return None

    best = max(candidates, key=lambda p: p["net_gain_per_share"])
    best = dict(best)
    best["low_gain"] = True
    logger.info(
        f"  {symbol}: fallback rec — best available net gain "
        f"${best['net_gain_per_share']:.2f}/share (below $0.10 threshold)"
    )
    return best


def get_collar_eligible_holdings(holdings: List[dict], min_value: float = 50000.0) -> List[dict]:
    """
    Filter holdings to those with current_market_value > min_value.

    Uses holding["price"] (from portfolio snapshot) as the price estimate.
    The chain fetch in fetch_collar_candidates() will get a live price anyway,
    so this is a fast pre-filter to avoid unnecessary API calls.

    Returns list of eligible holdings, each guaranteed to have contracts >= 1.
    """
    eligible = []
    for h in holdings:
        price = h.get("price", 0)
        shares = h.get("shares", 0)
        contracts = h.get("contracts", 0)
        market_value = price * shares

        if contracts < 1:
            continue
        if market_value <= min_value:
            logger.info(
                f"  {h['symbol']}: market value ${market_value:,.0f} "
                f"< ${min_value:,.0f} — skipping collar"
            )
            continue

        eligible.append(h)
        logger.info(
            f"  {h['symbol']}: eligible for collar — "
            f"${market_value:,.0f} market value, {contracts} contract(s)"
        )

    logger.info(f"Collar eligible holdings: {len(eligible)}/{len(holdings)}")
    return eligible


def fetch_collar_candidates(
    holding: dict,
    dte_min: int = 28,
    dte_max: int = 112,
) -> List[dict]:
    raise NotImplementedError


def build_collar_pairs(
    symbol: str,
    name: str,
    chain_data: List[dict],
    config: dict,
) -> List[dict]:
    raise NotImplementedError


def add_collar_earnings(collar_recs: List[dict]) -> List[dict]:
    raise NotImplementedError


def run_collar_pipeline(dry_run: bool = False) -> dict:
    raise NotImplementedError
