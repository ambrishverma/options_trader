"""
collar.py — Collar Recommendation Pipeline
============================================
Finds self-financing collar pairs (covered call + long put) for large equity
holdings (>$50K market value).

A collar qualifies when:
  - CC strike >= 10% OTM (and <= 40% OTM), LP strike <= 10% below current price
  - Both legs: OI > 5
  - CC expiration <= LP expiration (CC expires on or before the put)
  - Both expirations within DTE window: 28-112 calendar days (4-16 weeks)
  - CC mid - LP mid >= $0.10/share (self-financing with minimum net gain)

Output per holding: list of collar pair dicts, one per CC expiration calendar
month, highest net-gain pair selected. Falls back to best self-financing pair
(even if < $0.10) when no qualifying pairs exist, marked low_gain=True.
"""

import copy
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
    call_otm_max  = config.get("collar_call_otm_max_pct", 40.0)
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
        # Call must be within OTM range [min, max]
        if not (call_otm_min <= cc["otm_pct"] <= call_otm_max):
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
    call_otm_max = config.get("collar_call_otm_max_pct", 40.0)
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
        if not (call_otm_min <= cc["otm_pct"] <= call_otm_max):
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
    best = copy.deepcopy(best)
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
    """
    Fetch all (call, put) raw option records for a holding within the DTE window.

    Returns a list of dicts, each representing one expiration date:
    {
        "expiration": str,      # "YYYY-MM-DD"
        "dte": int,
        "calls": [              # raw call rows as dicts
            {"strike": float, "bid": float, "ask": float, "mid": float, "open_interest": int},
            ...
        ],
        "puts": [...],          # same structure
        "current_price": float,
        "contracts": int,
        "market_value": float,
    }
    Returns empty list on any fetch error.
    """
    symbol = holding["symbol"]
    contracts = holding.get("contracts", 0)

    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))

        # Get live price
        current_price = 0.0
        try:
            hist = ticker.history(period="2d")
            if not hist.empty:
                current_price = _safe_float(float(hist["Close"].iloc[-1]))
        except Exception:
            pass
        if current_price <= 0:
            logger.warning(f"{symbol}: could not fetch price, skipping collar scan")
            return []

        expirations = ticker.options
        if not expirations:
            logger.info(f"{symbol}: no options available")
            return []

        today = date.today()
        results = []

        def _parse_chain(df) -> list:
            rows = []
            for _, row in df.iterrows():
                strike = _safe_float(row.get("strike", 0))
                bid    = _safe_float(row.get("bid",    0))
                ask    = _safe_float(row.get("ask",    0))
                oi     = _safe_int(row.get("openInterest"))
                if strike <= 0:
                    continue
                rows.append({
                    "strike": strike,
                    "bid":    round(bid, 2),
                    "ask":    round(ask, 2),
                    "mid":    round((bid + ask) / 2, 2),
                    "open_interest": oi,
                })
            return rows

        for exp_str in expirations:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
            dte = (exp_date - today).days
            if not (dte_min <= dte <= dte_max):
                continue

            try:
                chain = ticker.option_chain(exp_str)

                results.append({
                    "expiration":    exp_str,
                    "dte":           dte,
                    "calls":         _parse_chain(chain.calls),
                    "puts":          _parse_chain(chain.puts),
                    "current_price": round(current_price, 2),
                    "contracts":     contracts,
                    "market_value":  round(holding.get("shares", 0) * holding.get("price", current_price), 2),
                })

            except Exception as e:
                logger.warning(f"{symbol}/{exp_str}: chain fetch failed: {e}")
                continue

        logger.info(
            f"{symbol}: {len(results)} expiration(s) in "
            f"{dte_min}–{dte_max}d window"
        )
        return results

    except Exception as e:
        logger.error(f"{symbol}: collar chain fetch failed: {e}", exc_info=False)
        return []


def build_collar_pairs(
    symbol: str,
    name: str,
    chain_data: List[dict],
    config: dict,
) -> List[dict]:
    """
    Build all candidate (CC, LP) pairs from raw chain data for one symbol.

    Pairs qualifying calls from one expiration with qualifying puts from the
    same or any later expiration (CC must expire on or before LP). This allows
    collecting call premium sooner while maintaining longer downside protection.

    Returns a flat list of pair dicts. Does NOT filter — call _filter_collar_pairs() next.
    Each pair has both cc_expiration/cc_dte and lp_expiration/lp_dte fields.
    The top-level expiration/dte fields mirror the CC leg for backward compatibility.
    """
    call_otm_min = config.get("collar_call_otm_min_pct", 10.0)
    call_otm_max = config.get("collar_call_otm_max_pct", 40.0)
    put_otm_max  = config.get("collar_put_otm_max_pct", 10.0)

    # Collect qualifying legs per expiration date.
    # Upper OTM bound rejects corrupt/legacy pre-split records (e.g. NVDA $2000 strike).
    # mid < current_price rejects non-standard contracts with absurd premiums.
    by_exp: dict = {}
    for exp_data in chain_data:
        exp_str       = exp_data["expiration"]
        current_price = exp_data["current_price"]

        if current_price <= 0:
            continue

        cc_candidates = [
            c for c in exp_data["calls"]
            if 0 < c["mid"] < current_price
            and call_otm_min <= ((c["strike"] - current_price) / current_price * 100) <= call_otm_max
        ]
        lp_candidates = [
            p for p in exp_data["puts"]
            if 0 < p["mid"] < p["strike"]
            and 0 <= ((current_price - p["strike"]) / current_price * 100) <= put_otm_max
        ]

        if cc_candidates or lp_candidates:
            by_exp[exp_str] = {
                "dte":           exp_data["dte"],
                "current_price": current_price,
                "contracts":     exp_data["contracts"],
                "market_value":  exp_data["market_value"],
                "cc_candidates": cc_candidates,
                "lp_candidates": lp_candidates,
            }

    if not by_exp:
        return []

    pairs = []
    exp_list = sorted(by_exp.keys())  # chronological order

    for cc_exp in exp_list:
        cc_data = by_exp[cc_exp]
        if not cc_data["cc_candidates"]:
            continue

        cc_dte        = cc_data["dte"]
        current_price = cc_data["current_price"]
        contracts     = cc_data["contracts"]
        market_value  = cc_data["market_value"]

        # LP can share the CC expiration or use any later expiration in the window
        for lp_exp in exp_list:
            if lp_exp < cc_exp:
                continue  # LP must expire on or after CC
            lp_data = by_exp[lp_exp]
            if not lp_data["lp_candidates"]:
                continue
            lp_dte = lp_data["dte"]

            for cc in cc_data["cc_candidates"]:
                cc_mid = cc["mid"]
                cc_otm = round((cc["strike"] / current_price - 1) * 100, 2)
                cc_ann = round(cc_mid / current_price * 365 / max(cc_dte, 1) * 100, 2)

                for lp in lp_data["lp_candidates"]:
                    lp_mid   = lp["mid"]
                    net      = round(cc_mid - lp_mid, 2)
                    prot_pct = round((lp["strike"] / current_price - 1) * 100, 2)  # negative

                    pairs.append({
                        "symbol":           symbol,
                        "name":             name,
                        "current_price":    current_price,
                        "market_value":     market_value,
                        "contracts":        contracts,
                        # Top-level expiration/dte mirror the CC leg for backward compat
                        "expiration":       cc_exp,
                        "dte":              cc_dte,
                        "cc_expiration":    cc_exp,
                        "cc_dte":           cc_dte,
                        "lp_expiration":    lp_exp,
                        "lp_dte":           lp_dte,
                        "call_leg": {
                            "strike":           cc["strike"],
                            "bid":              cc["bid"],
                            "ask":              cc["ask"],
                            "mid":              cc_mid,
                            "open_interest":    cc["open_interest"],
                            "otm_pct":          cc_otm,
                            "annualized_yield": cc_ann,
                        },
                        "put_leg": {
                            "strike":           lp["strike"],
                            "bid":              lp["bid"],
                            "ask":              lp["ask"],
                            "mid":              lp_mid,
                            "open_interest":    lp["open_interest"],
                            "protection_pct":   prot_pct,
                        },
                        "net_gain_per_share": net,
                        "net_gain_total":     round(net * 100 * contracts, 2),
                        "upside_cap_pct":     cc_otm,
                        "downside_floor_pct": prot_pct,
                        "low_gain":           False,
                    })

    logger.info(f"  {symbol}: {len(pairs)} candidate pairs built across {len(chain_data)} expiration(s)")
    return pairs


def add_collar_earnings(collar_recs: List[dict]) -> List[dict]:
    """
    Annotate each collar recommendation with earnings date if it falls
    within the expiration calendar month.

    Adds to each rec:
      "earnings_date":    str | None   ("YYYY-MM-DD")
      "earnings_warning": str | None
    """
    from earnings import get_earnings_dates

    if not collar_recs:
        return collar_recs

    symbols = list({rec["symbol"] for rec in collar_recs})
    try:
        earnings_map = get_earnings_dates(symbols)
    except Exception as e:
        logger.warning(f"Collar earnings check failed ({e}) — skipping earnings annotations")
        for rec in collar_recs:
            rec["next_earnings_date"] = None
            rec["earnings_date"]      = None
            rec["earnings_warning"]   = None
        return collar_recs

    for rec in collar_recs:
        sym          = rec["symbol"]
        exp_month    = rec["expiration"][:7]   # "YYYY-MM"
        earnings_str = earnings_map.get(sym)

        rec["next_earnings_date"] = earnings_str  # always set (for column display)
        rec["earnings_date"]      = None
        rec["earnings_warning"]   = None

        if not earnings_str:
            continue

        if earnings_str[:7] == exp_month:
            rec["earnings_date"]    = earnings_str
            rec["earnings_warning"] = (
                f"⚠ Earnings: {earnings_str} — "
                f"earnings fall within the {exp_month} expiration month. "
                f"Elevated IV may affect option prices."
            )

    flagged = sum(1 for r in collar_recs if r.get("earnings_date"))
    if flagged:
        logger.info(f"Collar earnings flags: {flagged} of {len(collar_recs)} recs")
    return collar_recs


def add_collar_dividends(collar_recs: List[dict]) -> List[dict]:
    """
    Annotate each collar recommendation with the next ex-dividend date.

    Adds to each rec:
      "ex_dividend_date": str | None   ("YYYY-MM-DD")
    """
    if not collar_recs:
        return collar_recs

    symbols = list({rec["symbol"] for rec in collar_recs})
    ex_div_map: dict = {}

    for sym in symbols:
        try:
            info = yf.Ticker(_yahoo_symbol(sym)).info
            ts   = info.get("exDividendDate")
            if ts:
                ex_div_map[sym] = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")
            else:
                ex_div_map[sym] = None
        except Exception as e:
            logger.warning(f"{sym}: ex-dividend date fetch failed ({e})")
            ex_div_map[sym] = None

    for rec in collar_recs:
        rec["ex_dividend_date"] = ex_div_map.get(rec["symbol"])

    fetched = sum(1 for v in ex_div_map.values() if v)
    logger.info(f"Ex-dividend dates: {fetched}/{len(symbols)} symbol(s) have upcoming ex-div")
    return collar_recs


def run_collar_on_demand(symbol: str, dte_min: int, dte_max: int) -> dict:
    """
    On-demand collar scan for a single symbol with a custom DTE window.

    If the symbol is in the current portfolio snapshot, uses actual shares/contracts.
    Otherwise fetches live price from yfinance and uses 1 contract so any symbol works.

    Open-call subtraction is intentionally skipped — this is an exploratory scan;
    the user is asking "what collars exist for this symbol" regardless of open positions.

    Returns:
        {
          "recommendations": list of collar rec dicts,
          "eligible_count":  1,
          "symbol":          str,
          "holding":         dict — the holding used for the scan,
        }
    """
    from utils import load_config

    config = load_config()
    symbol = symbol.upper()

    # Look up in portfolio; fall back to a synthetic 1-contract holding
    holding = None
    try:
        from portfolio import get_portfolio
        for h in get_portfolio():
            if h["symbol"] == symbol:
                holding = h
                break
    except Exception as e:
        logger.warning(f"Portfolio load failed ({e}) — will fetch live price")

    if holding is None:
        logger.info(f"{symbol}: not in portfolio — fetching live price")
        try:
            ticker = yf.Ticker(symbol.replace(".", "-"))
            hist = ticker.history(period="2d")
            if hist.empty:
                raise ValueError("no price data returned")
            price = _safe_float(float(hist["Close"].iloc[-1]))
        except Exception as e:
            logger.error(f"{symbol}: live price fetch failed: {e}")
            return {"recommendations": [], "eligible_count": 0, "symbol": symbol, "holding": {}}
        holding = {
            "symbol": symbol, "name": symbol,
            "shares": 100.0, "price": price,
            "eligible": True, "contracts": 1,
        }
        logger.info(f"{symbol}: using synthetic holding — ${price:.2f}, 1 contract")
    else:
        logger.info(
            f"{symbol}: found in portfolio — "
            f"${holding['shares'] * holding['price']:,.0f} market value, "
            f"{holding['contracts']} contract(s)"
        )

    start = datetime.now()
    logger.info("=" * 60)
    logger.info(
        f"On-demand collar scan: {symbol} | "
        f"DTE {dte_min}–{dte_max}d ({dte_min // 7}–{dte_max // 7} weeks)"
    )

    chain_data = fetch_collar_candidates(holding, dte_min=dte_min, dte_max=dte_max)
    if not chain_data:
        logger.info(f"{symbol}: no chain data in {dte_min}–{dte_max}d window")
        return {"recommendations": [], "eligible_count": 1, "symbol": symbol, "holding": holding}

    name      = holding.get("name", symbol)
    all_pairs = build_collar_pairs(symbol, name, chain_data, config)
    if not all_pairs:
        logger.info(f"{symbol}: no candidate pairs built")
        return {"recommendations": [], "eligible_count": 1, "symbol": symbol, "holding": holding}

    qualifying = _filter_collar_pairs(all_pairs, config)
    if qualifying:
        recs = _deduplicate_by_month(qualifying)
        logger.info(f"{symbol}: {len(recs)} qualifying collar(s) found")
    else:
        fallback = _apply_fallback(symbol, all_pairs, config)
        recs     = [fallback] if fallback else []

    recs = add_collar_earnings(recs)
    recs = add_collar_dividends(recs)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(f"On-demand scan complete in {elapsed:.1f}s — {len(recs)} rec(s)")
    return {"recommendations": recs, "eligible_count": 1, "symbol": symbol, "holding": holding}


def run_collar_pipeline(dry_run: bool = False) -> dict:
    """
    Full collar recommendation pipeline.

    Steps:
      1. Load portfolio snapshot
      2. Filter eligible holdings (market value > collar_min_holding_value)
      3. For each holding: fetch chain, build pairs, filter, dedup, fallback
      4. Annotate with earnings dates
      5. Return sorted list of collar recommendations

    Returns:
        {
          "recommendations": list of collar rec dicts (grouped by symbol, ordered by expiration month),
          "eligible_count":  int — number of holdings that passed the market value filter,
        }
        Both values are present even when no recommendations are found.
    """
    from utils import load_config
    from portfolio import get_portfolio

    start = datetime.now()
    config = load_config()

    logger.info("=" * 60)
    logger.info(f"Collar pipeline start {'[DRY RUN]' if dry_run else ''} — {start.strftime('%Y-%m-%d %H:%M:%S')}")

    # Step 1: load portfolio
    holdings = get_portfolio()
    logger.info(f"[1/4] Portfolio: {len(holdings)} holdings loaded")

    # Step 2: filter eligible
    min_value = config.get("collar_min_holding_value", 10000.0)
    eligible = get_collar_eligible_holdings(holdings, min_value=min_value)
    logger.info(f"[2/4] {len(eligible)} holdings eligible (market value > ${min_value:,.0f})")

    if not eligible:
        logger.info("No eligible holdings — collar pipeline complete")
        return {"recommendations": [], "eligible_count": 0}

    # Open-call subtraction is intentionally skipped for the weekly collar scan —
    # consistent with the on-demand scan.  Collar calls are typically 28–112 DTE,
    # while open covered calls are usually near-expiry (≤21 DTE).  They don't
    # overlap, so an existing short call on a symbol doesn't block a new collar.
    # The user decides whether to act based on their current position.
    logger.info(f"  Open-call exclusion skipped — collar scan is independent of existing covered calls")

    # Step 3: scan chains, build, filter, dedup per symbol
    all_recs = []
    logger.info(f"[3/4] Scanning options chains for {len(eligible)} holding(s)...")

    for holding in eligible:
        sym  = holding["symbol"]
        name = holding.get("name", sym)
        logger.info(f"  Processing {sym}...")

        chain_data = fetch_collar_candidates(
            holding,
            dte_min=config.get("collar_dte_min", 28),
            dte_max=config.get("collar_dte_max", 112),
        )

        if not chain_data:
            logger.info(f"  {sym}: no chain data — skipping")
            continue

        # Build all candidate pairs
        all_pairs = build_collar_pairs(sym, name, chain_data, config)

        if not all_pairs:
            logger.info(f"  {sym}: no candidate pairs — skipping")
            continue

        # Apply qualifying filters
        qualifying = _filter_collar_pairs(all_pairs, config)

        if qualifying:
            # Dedup to one per month
            monthly_recs = _deduplicate_by_month(qualifying)
            logger.info(f"  {sym}: {len(monthly_recs)} qualifying collar(s) found")
            all_recs.extend(monthly_recs)
        else:
            # Fallback: best self-financing pair even if < $0.10
            fallback = _apply_fallback(sym, all_pairs, config)
            if fallback:
                all_recs.append(fallback)

    # Step 4: earnings + dividends
    logger.info(f"[4/4] Checking earnings and dividends for {len(all_recs)} recommendation(s)...")
    all_recs = add_collar_earnings(all_recs)
    all_recs = add_collar_dividends(all_recs)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info(
        f"Collar pipeline complete in {elapsed:.1f}s — "
        f"{len(all_recs)} rec(s) across "
        f"{len({r['symbol'] for r in all_recs})} symbol(s)"
    )
    return {
        "recommendations":    all_recs,
        "eligible_count":     len(eligible),
    }
