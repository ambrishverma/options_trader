"""
spread_scanner.py — Call Credit Spread (CCS) and Put Credit Spread (PCS) Scanner
==================================================================================
Finds self-financing credit spread pairs for on-demand scans and the weekly
collar email.

  scan_ccs(symbol, spread_size, target_premium, dte_min, dte_max)
    Bear Call Spread: short call (≥10% OTM) + long call at short_strike + spread_size.
    Returns the single best CCS rec dict (highest YPD × credit_to_loss_ratio) or None.

  scan_pcs(symbol, spread_size, target_premium, dte_min, dte_max)
    Bull Put Spread: short put (≥10% OTM) + long put at short_strike - spread_size.
    Returns the single best PCS rec dict (highest YPD × credit_to_loss_ratio) or None.

  run_spread_weekly_pipeline(holdings, config)
    Runs scan_ccs() and scan_pcs() for every portfolio holding (all stocks qualify
    regardless of share count, since credit spreads don't require stock ownership).
    Returns {"ccs": [...], "pcs": [...]} sorted by YPD × credit_to_loss_ratio descending.

Rec dict structure (same for CCS and PCS):
  {
    "symbol":           str,
    "name":             str,
    "current_price":    float,
    "type":             str,            # "CCS" or "PCS"
    "expiration":       str,            # "YYYY-MM-DD"
    "dte":              int,
    "short_leg": {
        "strike":       float,
        "bid":          float,
        "ask":          float,
        "mid":          float,
        "open_interest":int,
        "otm_pct":      float,
    },
    "long_leg": {
        "strike":       float,
        "bid":          float,
        "ask":          float,
        "mid":          float,
        "open_interest":int,
    },
    "net_credit":       float,  # per share = short bid − long ask
    "net_credit_total": float,  # × 100 per contract
    "max_loss":              float,  # (spread_size × 100) − net_credit_total
    "spread_size":           float,  # strike distance between legs
    "ypd":                   float,  # net_credit × 100 / dte
    "credit_to_loss_ratio":  float,  # net_credit_total / max_loss
    "score":                 float,  # ypd × credit_to_loss_ratio (ranking key)
  }
"""

import logging
import math
from datetime import date, datetime, timedelta
from typing import List, Optional

import yfinance as yf

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(value, default: float = 0.0) -> float:
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


def _get_live_price(symbol: str) -> float:
    """Fetch live price via yfinance. Returns 0.0 on failure."""
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        hist = ticker.history(period="2d")
        if not hist.empty:
            price = _safe_float(float(hist["Close"].iloc[-1]))
            if price > 0:
                return price
        # Fallback to fast_info
        info  = ticker.fast_info
        price = _safe_float(getattr(info, "last_price", 0) or getattr(info, "previous_close", 0))
        return price
    except Exception as e:
        logger.warning(f"{symbol}: live price fetch failed ({e})")
        return 0.0


def _parse_chain_df(df) -> list:
    """Parse a yfinance option chain DataFrame into a list of dicts."""
    rows = []
    for _, row in df.iterrows():
        strike = _safe_float(row.get("strike", 0))
        bid    = _safe_float(row.get("bid",    0))
        ask    = _safe_float(row.get("ask",    0))
        oi     = _safe_int(row.get("openInterest"))
        if strike <= 0:
            continue
        mid = round((bid + ask) / 2, 2)
        rows.append({
            "strike":        strike,
            "bid":           round(bid, 2),
            "ask":           round(ask, 2),
            "mid":           mid,
            "open_interest": oi,
        })
    return rows


def _fetch_chains(symbol: str, dte_min: int, dte_max: int) -> list:
    """
    Fetch call + put chains for all expirations within the DTE window.

    Returns list of:
      {
        "expiration": str,
        "dte": int,
        "current_price": float,
        "calls": [...],    # list of row dicts
        "puts":  [...],
      }
    """
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))

        # Live price
        current_price = 0.0
        try:
            hist = ticker.history(period="2d")
            if not hist.empty:
                current_price = _safe_float(float(hist["Close"].iloc[-1]))
        except Exception:
            pass
        if current_price <= 0:
            logger.warning(f"{symbol}: could not fetch price for spread scan")
            return []

        expirations = ticker.options or []
        today = date.today()
        results = []

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
                    "current_price": round(current_price, 2),
                    "calls":         _parse_chain_df(chain.calls),
                    "puts":          _parse_chain_df(chain.puts),
                })
            except Exception as e:
                logger.warning(f"{symbol}/{exp_str}: chain fetch failed ({e})")

        logger.info(f"{symbol}: {len(results)} expiration(s) in {dte_min}–{dte_max}d window for spread scan")
        return results

    except Exception as e:
        logger.error(f"{symbol}: spread chain fetch failed ({e})")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Core scan functions
# ─────────────────────────────────────────────────────────────────────────────

def scan_ccs(
    symbol: str,
    name: str = None,
    spread_size_min: float = None,
    spread_size_max: float = None,
    target_premium: float = None,
    dte_min: int = 14,
    dte_max: int = 42,
    short_otm_pct: float = 10.0,
    min_open_interest: int = 2,
    spread_size_min_pct: float = 1.0,
    spread_size_max_pct: float = 10.0,
    min_premium_pct: float = 1.0,
) -> Optional[dict]:
    """
    Find the best Call Credit Spread (Bear Call Spread) for a symbol.

    Short leg: call with strike >= current_price × (1 + short_otm_pct/100)
    Long  leg: call with strike ≈ short_strike + spread_size (nearest available)

    The scanner evaluates ALL spread widths from spread_size_min to spread_size_max
    in increments of 1% of the current stock price, building a 3-dimensional search
    table: [expiry] × [short_strike] × [spread_size]. The combination with the
    highest score (YPD × credit_to_loss_ratio) is returned.

    net_credit = bid(short) − ask(long)
    YPD = net_credit × 100 / dte
    score = YPD × credit_to_loss_ratio

    Args:
        symbol:              Stock ticker
        name:                Company name for display
        spread_size_min:     Min dollar width to try (overrides spread_size_min_pct if set)
        spread_size_max:     Max dollar width to try (overrides spread_size_max_pct if set)
        target_premium:      Minimum net credit per share (overrides min_premium_pct if set)
        dte_min / dte_max:   DTE window
        short_otm_pct:       Minimum OTM% for short call leg (buffer size)
        min_open_interest:   Minimum OI required on both legs
        spread_size_min_pct: Min spread width as % of current price (default: 1%)
        spread_size_max_pct: Max spread width as % of current price (default: 10%)
        min_premium_pct:     Min net credit as % of current price (used if target_premium is None)

    Returns (best_rec_dict_or_None, scenarios_evaluated_count).
    """
    symbol = symbol.upper()
    name   = name or symbol

    chain_data = _fetch_chains(symbol, dte_min, dte_max)
    if not chain_data:
        return (None, 0)

    current_price = chain_data[0]["current_price"]
    if current_price <= 0:
        return (None, 0)

    # Build the list of spread sizes to evaluate (step = 1% of stock price)
    step = round(current_price * 0.01, 2)
    eff_spread_min = spread_size_min if spread_size_min is not None else round(current_price * spread_size_min_pct / 100, 2)
    eff_spread_max = spread_size_max if spread_size_max is not None else round(current_price * spread_size_max_pct / 100, 2)
    if eff_spread_min > eff_spread_max:
        eff_spread_min, eff_spread_max = eff_spread_max, eff_spread_min
    spread_sizes: list = []
    sz = eff_spread_min
    while sz <= eff_spread_max + 1e-9:
        spread_sizes.append(round(sz, 2))
        sz = round(sz + step, 2)
    if not spread_sizes:
        spread_sizes = [eff_spread_min]

    eff_target_premium = target_premium if target_premium is not None else round(current_price * min_premium_pct / 100, 2)

    # Round to avoid floating-point representation issues (e.g. 100*1.1=110.00000000000001)
    short_strike_min = round(current_price * (1 + short_otm_pct / 100), 4)

    best: Optional[dict] = None
    best_score: float = 0.0   # YPD × credit_to_loss_ratio
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte      = exp_data["dte"]
        exp_str  = exp_data["expiration"]
        calls    = sorted(exp_data["calls"], key=lambda c: c["strike"])

        # Re-validate DTE — _fetch_chains filters in production, but tests may inject
        # out-of-window data directly.
        if not (dte_min <= dte <= dte_max):
            continue

        for short_call in calls:
            short_strike = short_call["strike"]
            if short_strike < short_strike_min:
                continue   # must be >= OTM buffer
            if short_call["open_interest"] < min_open_interest:
                continue
            if short_call["bid"] <= 0:
                continue
            # Sanity: an OTM call's bid can never exceed the stock price.
            # If it does, the data is stale or corrupt (e.g. yfinance returning
            # intrinsic value from when the stock was at a much higher price).
            if short_call["bid"] >= current_price:
                continue

            # Evaluate every spread width in the range
            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                long_target = short_strike + spread_size

                # Find nearest available long call strike >= long_target
                long_candidates = [
                    c for c in calls
                    if c["strike"] >= long_target - 0.01
                    and c["open_interest"] >= min_open_interest
                ]
                if not long_candidates:
                    continue

                long_call = min(long_candidates, key=lambda c: abs(c["strike"] - long_target))

                # Long leg must have a real market (non-zero ask); if ask=0 the
                # spread cannot be executed and the net credit would be fictitiously
                # inflated by the full short premium.
                if long_call["ask"] <= 0:
                    continue

                # Net credit (per share)
                net_credit = round(short_call["bid"] - long_call["ask"], 2)
                if net_credit <= 0:
                    continue
                if net_credit < eff_target_premium:
                    continue

                actual_spread = round(long_call["strike"] - short_strike, 2)
                net_credit_total = round(net_credit * 100, 2)
                max_loss = round(actual_spread * 100 - net_credit * 100, 2)
                ypd = round(net_credit * 100 / dte, 4)
                credit_to_loss_ratio = round(net_credit_total / max_loss, 2) if max_loss > 0 else 0.0
                score = round(ypd * credit_to_loss_ratio, 6)

                if score <= best_score:
                    continue

                short_otm = round((short_strike / current_price - 1) * 100, 2)
                best_score = score
                best = {
                    "symbol":        symbol,
                    "name":          name,
                    "current_price": round(current_price, 2),
                    "type":          "CCS",
                    "expiration":    exp_str,
                    "dte":           dte,
                    "short_leg": {
                        "strike":        short_strike,
                        "bid":           short_call["bid"],
                        "ask":           short_call["ask"],
                        "mid":           short_call["mid"],
                        "open_interest": short_call["open_interest"],
                        "otm_pct":       short_otm,
                    },
                    "long_leg": {
                        "strike":        long_call["strike"],
                        "bid":           long_call["bid"],
                        "ask":           long_call["ask"],
                        "mid":           long_call["mid"],
                        "open_interest": long_call["open_interest"],
                    },
                    "net_credit":            net_credit,
                    "net_credit_total":      net_credit_total,
                    "spread_size":           actual_spread,
                    "max_loss":              max_loss,
                    "ypd":                   ypd,
                    "credit_to_loss_ratio":  credit_to_loss_ratio,
                    "score":                 score,
                }

    if best:
        logger.info(
            f"{symbol}: CCS best — {best['expiration']} ({best['dte']}d) "
            f"short ${best['short_leg']['strike']} / long ${best['long_leg']['strike']} "
            f"net ${best['net_credit']:.2f} YPD={best['ypd']:.2f} "
            f"C/L={best['credit_to_loss_ratio']:.2f} score={best['score']:.4f} "
            f"scenarios={scenarios_evaluated}"
        )
    else:
        logger.info(f"{symbol}: no qualifying CCS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    return (best, scenarios_evaluated)


def scan_pcs(
    symbol: str,
    name: str = None,
    spread_size_min: float = None,
    spread_size_max: float = None,
    target_premium: float = None,
    dte_min: int = 14,
    dte_max: int = 42,
    short_otm_pct: float = 10.0,
    min_open_interest: int = 2,
    spread_size_min_pct: float = 1.0,
    spread_size_max_pct: float = 10.0,
    min_premium_pct: float = 1.0,
) -> Optional[dict]:
    """
    Find the best Put Credit Spread (Bull Put Spread) for a symbol.

    Short leg: put with strike <= current_price × (1 - short_otm_pct/100)
    Long  leg: put with strike ≈ short_strike − spread_size (nearest available)

    Evaluates ALL spread widths in the range [spread_size_min, spread_size_max]
    in 1%-of-price increments. Returns the (expiry, short_strike, spread_size)
    triple with the highest score (YPD × credit_to_loss_ratio).

    net_credit = bid(short) − ask(long)
    YPD = net_credit × 100 / dte
    score = YPD × credit_to_loss_ratio

    Args: same as scan_ccs() (mirrored for puts).
    Returns (best_rec_dict_or_None, scenarios_evaluated_count).
    """
    symbol = symbol.upper()
    name   = name or symbol

    chain_data = _fetch_chains(symbol, dte_min, dte_max)
    if not chain_data:
        return (None, 0)

    current_price = chain_data[0]["current_price"]
    if current_price <= 0:
        return (None, 0)

    # Build the list of spread sizes to evaluate (step = 1% of stock price)
    step = round(current_price * 0.01, 2)
    eff_spread_min = spread_size_min if spread_size_min is not None else round(current_price * spread_size_min_pct / 100, 2)
    eff_spread_max = spread_size_max if spread_size_max is not None else round(current_price * spread_size_max_pct / 100, 2)
    if eff_spread_min > eff_spread_max:
        eff_spread_min, eff_spread_max = eff_spread_max, eff_spread_min
    spread_sizes: list = []
    sz = eff_spread_min
    while sz <= eff_spread_max + 1e-9:
        spread_sizes.append(round(sz, 2))
        sz = round(sz + step, 2)
    if not spread_sizes:
        spread_sizes = [eff_spread_min]

    eff_target_premium = target_premium if target_premium is not None else round(current_price * min_premium_pct / 100, 2)

    # Round to avoid floating-point representation issues (e.g. 100*0.9=89.99999999999999)
    short_strike_max = round(current_price * (1 - short_otm_pct / 100), 4)

    best: Optional[dict] = None
    best_score: float = 0.0   # YPD × credit_to_loss_ratio
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte      = exp_data["dte"]
        exp_str  = exp_data["expiration"]
        puts     = sorted(exp_data["puts"], key=lambda p: p["strike"], reverse=True)

        # Re-validate DTE — _fetch_chains filters in production, but tests may inject
        # out-of-window data directly.
        if not (dte_min <= dte <= dte_max):
            continue

        for short_put in puts:
            short_strike = short_put["strike"]
            if short_strike > short_strike_max:
                continue   # must be >= OTM buffer (below current price)
            if short_put["open_interest"] < min_open_interest:
                continue
            if short_put["bid"] <= 0:
                continue
            # Sanity: an OTM put's bid can never exceed the stock price.
            # Bids >= current_price indicate stale or corrupt yfinance data.
            if short_put["bid"] >= current_price:
                continue

            # Evaluate every spread width in the range
            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                long_target = short_strike - spread_size

                # Find nearest available long put strike <= long_target
                long_candidates = [
                    p for p in puts
                    if p["strike"] <= long_target + 0.01
                    and p["open_interest"] >= min_open_interest
                ]
                if not long_candidates:
                    continue

                long_put = min(long_candidates, key=lambda p: abs(p["strike"] - long_target))

                # Long leg must have a real market (non-zero ask).
                if long_put["ask"] <= 0:
                    continue

                net_credit = round(short_put["bid"] - long_put["ask"], 2)
                if net_credit <= 0:
                    continue
                if net_credit < eff_target_premium:
                    continue

                actual_spread = round(short_strike - long_put["strike"], 2)
                net_credit_total = round(net_credit * 100, 2)
                max_loss = round(actual_spread * 100 - net_credit * 100, 2)
                ypd = round(net_credit * 100 / dte, 4)
                credit_to_loss_ratio = round(net_credit_total / max_loss, 2) if max_loss > 0 else 0.0
                score = round(ypd * credit_to_loss_ratio, 6)

                if score <= best_score:
                    continue

                short_otm = round((1 - short_strike / current_price) * 100, 2)
                best_score = score
                best = {
                    "symbol":        symbol,
                    "name":          name,
                    "current_price": round(current_price, 2),
                    "type":          "PCS",
                    "expiration":    exp_str,
                    "dte":           dte,
                    "short_leg": {
                        "strike":        short_strike,
                        "bid":           short_put["bid"],
                        "ask":           short_put["ask"],
                        "mid":           short_put["mid"],
                        "open_interest": short_put["open_interest"],
                        "otm_pct":       short_otm,
                    },
                    "long_leg": {
                        "strike":        long_put["strike"],
                        "bid":           long_put["bid"],
                        "ask":           long_put["ask"],
                        "mid":           long_put["mid"],
                        "open_interest": long_put["open_interest"],
                    },
                    "net_credit":            net_credit,
                    "net_credit_total":      net_credit_total,
                    "spread_size":           actual_spread,
                    "max_loss":              max_loss,
                    "ypd":                   ypd,
                    "credit_to_loss_ratio":  credit_to_loss_ratio,
                    "score":                 score,
                }

    if best:
        logger.info(
            f"{symbol}: PCS best — {best['expiration']} ({best['dte']}d) "
            f"short ${best['short_leg']['strike']} / long ${best['long_leg']['strike']} "
            f"net ${best['net_credit']:.2f} YPD={best['ypd']:.2f} "
            f"C/L={best['credit_to_loss_ratio']:.2f} score={best['score']:.4f} "
            f"scenarios={scenarios_evaluated}"
        )
    else:
        logger.info(f"{symbol}: no qualifying PCS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    return (best, scenarios_evaluated)


# ─────────────────────────────────────────────────────────────────────────────
# Weekly pipeline (all portfolio holdings)
# ─────────────────────────────────────────────────────────────────────────────

def run_spread_weekly_pipeline(holdings: List[dict], config: dict) -> dict:
    """
    Run CCS and PCS scans for every portfolio holding.

    Credit spreads do NOT require owning 100 shares, so all holdings qualify
    regardless of share count. This differs from covered calls / collars.

    Args:
        holdings: Full portfolio holding list (from get_portfolio())
        config:   Loaded config dict (for spread_ keys)

    Returns:
        {
          "ccs": [rec, ...]  sorted by YPD descending,
          "pcs": [rec, ...]  sorted by YPD descending,
        }
    """
    from datetime import datetime as _dt

    dte_min       = int(config.get("spread_dte_min",             14))
    dte_max       = int(config.get("spread_dte_max",             42))
    short_otm     = float(config.get("spread_short_otm_pct",   10.0))
    min_oi        = int(config.get("spread_min_open_interest",    2))
    size_min_pct  = float(config.get("spread_size_min_pct",     1.0))
    size_max_pct  = float(config.get("spread_size_max_pct",    10.0))
    premium_pct   = float(config.get("spread_min_premium_pct",  1.0))

    ccs_recs: list = []
    pcs_recs: list = []
    ccs_scenarios_total: int = 0
    pcs_scenarios_total: int = 0

    start = _dt.now()
    logger.info("=" * 60)
    logger.info(
        f"Spread weekly pipeline — {len(holdings)} holding(s) | "
        f"DTE {dte_min}–{dte_max}d | OTM≥{short_otm}% | "
        f"spread {size_min_pct}%–{size_max_pct}% of price"
    )

    for i, h in enumerate(holdings, 1):
        sym  = h["symbol"]
        name = h.get("name", sym)
        logger.info(f"  [{i}/{len(holdings)}] Scanning {sym} for CCS + PCS...")

        ccs, ccs_cnt = scan_ccs(
            sym, name=name,
            dte_min=dte_min, dte_max=dte_max,
            short_otm_pct=short_otm, min_open_interest=min_oi,
            spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
            min_premium_pct=premium_pct,
        )
        pcs, pcs_cnt = scan_pcs(
            sym, name=name,
            dte_min=dte_min, dte_max=dte_max,
            short_otm_pct=short_otm, min_open_interest=min_oi,
            spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
            min_premium_pct=premium_pct,
        )

        ccs_scenarios_total += ccs_cnt
        pcs_scenarios_total += pcs_cnt

        if ccs:
            ccs_recs.append(ccs)
        if pcs:
            pcs_recs.append(pcs)

    # Sort by score (YPD × credit_to_loss_ratio) descending
    ccs_recs.sort(key=lambda r: r.get("score", 0.0), reverse=True)
    pcs_recs.sort(key=lambda r: r.get("score", 0.0), reverse=True)

    elapsed = (_dt.now() - start).total_seconds()
    logger.info(
        f"Spread weekly pipeline complete in {elapsed:.1f}s — "
        f"{len(ccs_recs)} CCS rec(s) [{ccs_scenarios_total} scenarios], "
        f"{len(pcs_recs)} PCS rec(s) [{pcs_scenarios_total} scenarios]"
    )
    return {
        "ccs":            ccs_recs,
        "pcs":            pcs_recs,
        "ccs_scenarios":  ccs_scenarios_total,
        "pcs_scenarios":  pcs_scenarios_total,
    }
