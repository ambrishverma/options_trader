"""
diversifier.py — 50/50 Contract Diversification
================================================
When a holding has ≥2 contracts available, splits allocation between:
  - Yield side:  highest annualized yield option (best income)
  - Safety side: furthest OTM / latest expiry option (lowest assignment risk)

Allocation rules:
  - 1 contract:  100% to yield side
  - 2 contracts: 1 yield, 1 safety
  - 3 contracts: 1 yield, 2 safety (odd extra → safety)
  - 4 contracts: 2 yield, 2 safety
  - 5 contracts: 2 yield, 3 safety
  - N contracts: floor(N * split) yield, remainder safety
                 where split defaults to 0.5

Output per holding:
  {
    "symbol":         str,
    "name":           str,
    "contracts_total": int,
    "yield_leg": {
      "option":    <option dict>,
      "contracts": int,
      "rationale": str,
    },
    "safety_leg": {
      "option":    <option dict>,
      "contracts": int,
      "rationale": str,
    } | None,         # None if only 1 contract
    "combined_premium_total": float,   # total premium collected × 100
    "combined_ann_yield":     float,   # weighted average annualized yield
  }
"""

import logging
import math
from typing import List, Optional

logger = logging.getLogger(__name__)


def _combined_metrics(yield_leg: dict, safety_leg: Optional[dict]) -> dict:
    """Compute combined premium and weighted average annualized yield."""
    y_opt  = yield_leg["option"]
    y_cts  = yield_leg["contracts"]

    y_premium = y_opt["mid"] * 100 * y_cts   # 1 contract = 100 shares

    if safety_leg:
        s_opt  = safety_leg["option"]
        s_cts  = safety_leg["contracts"]
        s_premium = s_opt["mid"] * 100 * s_cts
        total_premium = y_premium + s_premium
        total_cts = y_cts + s_cts
        # Weighted average annualized yield
        combined_yield = (
            (y_opt["annualized_yield"] * y_cts + s_opt["annualized_yield"] * s_cts)
            / total_cts
        )
    else:
        total_premium = y_premium
        combined_yield = y_opt["annualized_yield"]

    return {
        "combined_premium_total": round(total_premium, 2),
        "combined_ann_yield":     round(combined_yield, 2),
    }


def diversify_holding(
    symbol:       str,
    name:         str,
    contracts:    int,
    yield_option: dict,
    safe_option:  Optional[dict],
    split:        float = 0.5,
) -> dict:
    """
    Apply 50/50 diversification for a single holding.

    Args:
        symbol, name:    Holding identifiers
        contracts:       Total number of available contracts (floor(shares/100))
        yield_option:    Best annualized yield option (from best_per_symbol)
        safe_option:     Safest/furthest OTM option (from safest_per_symbol)
                         If None (no alternative found), all go to yield.
        split:           Target fraction for yield side (default 0.5)

    Returns:
        Diversification allocation dict.
    """
    if contracts <= 0:
        raise ValueError(f"{symbol}: contracts must be ≥1, got {contracts}")

    if contracts == 1 or safe_option is None or yield_option == safe_option:
        # Single contract or no alternative — all to yield
        result = {
            "symbol":          symbol,
            "name":            name,
            "contracts_total": contracts,
            "yield_leg": {
                "option":    yield_option,
                "contracts": contracts,
                "rationale": "Single contract — full allocation to highest yield.",
            },
            "safety_leg": None,
        }
        result.update(_combined_metrics(result["yield_leg"], None))
        return result

    # Calculate split
    yield_cts  = max(1, math.floor(contracts * split))
    safety_cts = contracts - yield_cts  # odd extras → safety

    result = {
        "symbol":          symbol,
        "name":            name,
        "contracts_total": contracts,
        "yield_leg": {
            "option":    yield_option,
            "contracts": yield_cts,
            "rationale": (
                f"{yield_cts} of {contracts} contracts to highest yield "
                f"({yield_option['annualized_yield']:.1f}% ann. yield, "
                f"{yield_option['otm_pct']:.1f}% OTM, "
                f"exp {yield_option['expiration']})"
            ),
        },
        "safety_leg": {
            "option":    safe_option,
            "contracts": safety_cts,
            "rationale": (
                f"{safety_cts} of {contracts} contracts to safest strike "
                f"({safe_option['otm_pct']:.1f}% OTM, "
                f"exp {safe_option['expiration']}, "
                f"{safe_option['annualized_yield']:.1f}% ann. yield)"
            ),
        },
    }
    result.update(_combined_metrics(result["yield_leg"], result["safety_leg"]))
    return result


def build_recommendations(
    filter_result: dict,
    config: dict,
) -> List[dict]:
    """
    Build full diversification recommendations for all eligible symbols.

    Args:
        filter_result: Output from filters.run_filters()
        config: config.yaml dict

    Returns:
        List of diversification dicts, sorted by combined_ann_yield desc.
    """
    split = config.get("diversify_split", 0.5)

    best_by_sym = {opt["symbol"]: opt for opt in filter_result["best_per_sym"]}
    safe_by_sym = {opt["symbol"]: opt for opt in filter_result["safe_per_sym"]}

    recommendations = []

    for symbol, yield_opt in best_by_sym.items():
        safe_opt = safe_by_sym.get(symbol)

        # If yield and safe are the same option, pass safe as None
        if safe_opt and safe_opt.get("strike") == yield_opt.get("strike") and \
           safe_opt.get("expiration") == yield_opt.get("expiration"):
            safe_opt = None

        rec = diversify_holding(
            symbol=symbol,
            name=yield_opt["name"],
            contracts=yield_opt["contracts"],
            yield_option=yield_opt,
            safe_option=safe_opt,
            split=split,
        )
        recommendations.append(rec)

    # Sort by combined annualized yield desc
    recommendations.sort(key=lambda r: r["combined_ann_yield"], reverse=True)

    for i, rec in enumerate(recommendations, 1):
        rec["rank"] = i

    logger.info(f"Built {len(recommendations)} recommendations")
    if recommendations:
        top = recommendations[0]
        logger.info(
            f"Top recommendation: {top['symbol']} — "
            f"{top['combined_ann_yield']:.1f}% ann. yield, "
            f"${top['combined_premium_total']:.0f} total premium"
        )

    return recommendations
