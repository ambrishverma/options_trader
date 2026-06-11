"""
spread_scanner.py — Credit & Debit Spread Scanner
===================================================
Credit spreads (income):
  scan_ccs  — Call Credit Spread (Bear Call): sell OTM call, buy further-OTM call
  scan_pcs  — Put Credit Spread (Bull Put):  sell OTM put,  buy further-OTM put

Debit spreads (insurance / protection):
  scan_pds  — Put Debit Spread: buy near-ATM put, sell further-OTM put
  scan_cds  — Call Debit Spread: buy near-ATM call, sell further-OTM call

Pipeline helpers:
  run_spread_weekly_pipeline  — CCS + PCS for all holdings (income)
  run_insurance_pipeline      — PDS + CDS for qualifying holdings (protection)

Credit spread rec dict (CCS / PCS):
  {
    "symbol", "name", "current_price",
    "type":             "CCS" | "PCS",
    "expiration", "dte",
    "short_leg":        {strike, bid, ask, mid, open_interest, otm_pct, iv, delta},
    "long_leg":         {strike, bid, ask, mid, open_interest},
    "net_credit",       # per share = short bid − long ask
    "net_credit_total", # × 100 per contract
    "max_loss",         # (spread_size × 100) − net_credit_total
    "spread_size",      # strike distance between legs
    "ypd",              # net_credit × 100 / dte
    "credit_to_loss_ratio",
    "pop",              # probability of profit (%) = (1 − |short delta|) × 100
    "score",            # POP × credit_to_loss_ratio × (365/DTE) (highest wins)
  }

Debit spread rec dict (PDS / CDS):
  {
    "symbol", "name", "current_price",
    "type":             "PDS" | "CDS",
    "expiration", "dte",
    "long_leg":         {strike, bid, ask, mid, open_interest, otm_pct},
    "short_leg":        {strike, bid, ask, mid, open_interest},
    "net_debit",        # per share = long ask − short bid
    "net_debit_total",  # × 100 per contract
    "max_protection",   # spread_size × 100 (max payout)
    "spread_size",      # strike distance between legs
    "dpd",              # net_debit × 100 / dte  (debit per day)
    "debit_to_win_ratio",  # net_debit / spread_size
    "score",            # dpd × debit_to_win_ratio (LOWEST wins = cheapest insurance)
    "trigger_reason",   # why this symbol qualifies (pipeline only)
  }
"""

import logging
import math
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import yfinance as yf
from scipy.stats import norm

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


def _is_standard_strike(strike: float) -> bool:
    """
    Return True if *strike* is a standard option strike (multiple of $0.50).

    Adjusted / non-standard contracts (from special dividends, mergers, etc.)
    carry fractional strikes like $264.78 or $304.78.  These have
    unreliable pricing and should be excluded from spread scanning.
    """
    # Check if strike × 2 is an integer (i.e. strike is a multiple of $0.50)
    return abs(round(strike * 2) - strike * 2) < 0.001


def _bs_delta(
    S: float, K: float, T: float, r: float, sigma: float, opt_type: str = "call",
) -> float:
    """Black-Scholes delta for a European option.

    Args:
        S: current stock price
        K: strike price
        T: time to expiration in years (DTE / 365)
        r: risk-free rate (annualized, e.g. 0.043 for 4.3%)
        sigma: implied volatility (annualized, e.g. 0.30 for 30%)
        opt_type: "call" or "put"

    Returns delta in [-1, 1].  Returns 0.0 on degenerate inputs.
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    try:
        d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * math.sqrt(T))
        if opt_type == "put":
            return float(norm.cdf(d1) - 1.0)
        return float(norm.cdf(d1))
    except (ValueError, ZeroDivisionError):
        return 0.0


def _parse_chain_df(df) -> list:
    """Parse a yfinance option chain DataFrame into a list of dicts.

    After-hours fallback: when bid and ask are both 0 but lastPrice is
    available, synthesise bid/ask/mid from lastPrice (±3% spread).
    When openInterest is 0 but volume > 0, use volume as OI proxy so
    the option isn't rejected by the OI filter during off-market scans.
    """
    rows = []
    for _, row in df.iterrows():
        strike     = _safe_float(row.get("strike", 0))
        bid        = _safe_float(row.get("bid",    0))
        ask        = _safe_float(row.get("ask",    0))
        last_price = _safe_float(row.get("lastPrice", 0))
        volume     = _safe_int(row.get("volume"))
        oi         = _safe_int(row.get("openInterest"))
        iv         = _safe_float(row.get("impliedVolatility", 0))
        if strike <= 0:
            continue
        # Filter adjusted / non-standard contracts (e.g. $264.78 from QQQ
        # special dividends).  Standard strikes are always multiples of $0.50.
        if not _is_standard_strike(strike):
            continue
        # After-hours fallback: synthesise bid/ask from lastPrice when
        # both are zeroed out (common in off-market yfinance data).
        if bid <= 0 and ask <= 0 and last_price > 0:
            bid = round(last_price * 0.97, 2)
            ask = round(last_price * 1.03, 2)
        mid = round((bid + ask) / 2, 2)
        # OI fallback: use volume when OI is zero (weekend/after-hours).
        effective_oi = oi if oi > 0 else max(volume, 0)
        rows.append({
            "strike":        strike,
            "bid":           round(bid, 2),
            "ask":           round(ask, 2),
            "mid":           mid,
            "open_interest": effective_oi,
            "iv":            iv,
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
    short_strike_min_hint: float = None,
    min_pop: float = 70.0,
    risk_free_rate: float = 0.043,
    top_n: int = 1,
    earnings_dates: dict = None,
) -> Tuple[Optional[dict], int]:
    """
    Find the best Call Credit Spread (Bear Call Spread) for a symbol.

    Scoring (POP-weighted):
      delta = Black-Scholes delta of the short call
      POP   = 1 − |delta|
      Score = POP × (Credit / MaxLoss) × (365 / DTE)

    Guardrails: POP >= min_pop, no earnings before expiry.
    Returns (rec_or_list, scenarios_count). top_n=1 → single dict; top_n>1 → list.
    """
    symbol = symbol.upper()
    name   = name or symbol

    chain_data = _fetch_chains(symbol, dte_min, dte_max)
    if not chain_data:
        return (None, 0) if top_n == 1 else ([], 0)

    current_price = chain_data[0]["current_price"]
    if current_price <= 0:
        return (None, 0) if top_n == 1 else ([], 0)

    # Earnings guard
    _earnings_date = None
    if earnings_dates and symbol in earnings_dates and earnings_dates[symbol]:
        try:
            _earnings_date = date.fromisoformat(earnings_dates[symbol])
        except (ValueError, TypeError):
            pass

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

    short_strike_min = round(current_price * (1 + short_otm_pct / 100), 4)
    if short_strike_min_hint is not None:
        short_strike_min = max(short_strike_min, short_strike_min_hint)

    pop_threshold = min_pop / 100.0
    candidates: list = []
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte      = exp_data["dte"]
        exp_str  = exp_data["expiration"]
        calls    = sorted(exp_data["calls"], key=lambda c: c["strike"])

        if dte <= 0 or not (dte_min <= dte <= dte_max):
            continue

        # Earnings guardrail: reject expirations with earnings before expiry
        if _earnings_date:
            try:
                exp_date = date.fromisoformat(exp_str)
                if _earnings_date <= exp_date:
                    continue
            except (ValueError, TypeError):
                pass

        T = dte / 365.0

        for short_call in calls:
            short_strike = short_call["strike"]
            if short_strike < short_strike_min:
                continue
            if short_call["open_interest"] < min_open_interest:
                continue
            if short_call["bid"] <= 0:
                continue
            _otm_pct = (short_strike - current_price) / current_price * 100
            if _otm_pct > 30:
                _max_bid = current_price * 0.005
            elif _otm_pct > 20:
                _max_bid = current_price * 0.02
            else:
                _max_bid = current_price * 0.05
            if short_call["bid"] >= _max_bid:
                continue

            # Compute delta and POP for the short call
            iv = short_call.get("iv", 0)
            if iv > 0:
                delta = _bs_delta(current_price, short_strike, T, risk_free_rate, iv, "call")
            else:
                delta = 0.0
            pop = 1.0 - abs(delta) if delta != 0 else 0.0

            if pop < pop_threshold:
                continue

            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                long_target = short_strike + spread_size

                long_candidates = [
                    c for c in calls
                    if c["strike"] >= long_target - 0.01
                    and c["open_interest"] >= min_open_interest
                ]
                if not long_candidates:
                    continue

                long_call = min(long_candidates, key=lambda c: abs(c["strike"] - long_target))

                if long_call["ask"] <= 0:
                    continue

                actual_spread = round(long_call["strike"] - short_strike, 2)
                if actual_spread > eff_spread_max + 0.01:
                    continue

                net_credit = round(short_call["bid"] - long_call["ask"], 2)
                if net_credit <= 0:
                    continue
                if net_credit < eff_target_premium:
                    continue
                net_credit_total = round(net_credit * 100, 2)
                max_loss = round(actual_spread * 100 - net_credit * 100, 2)
                ypd = round(net_credit * 100 / dte, 4)
                credit_to_loss_ratio = round(net_credit_total / max_loss, 2) if max_loss > 0 else 0.0
                score = round(pop * credit_to_loss_ratio * (365.0 / dte), 6)

                short_otm = round((short_strike / current_price - 1) * 100, 2)
                rec = {
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
                        "iv":            iv,
                        "delta":         round(delta, 4),
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
                    "pop":                   round(pop * 100, 1),
                    "score":                 score,
                }
                candidates.append(rec)

    candidates.sort(key=lambda r: r["score"], reverse=True)
    top = candidates[:top_n]

    if top:
        for r in top:
            logger.info(
                f"{symbol}: CCS — {r['expiration']} ({r['dte']}d) "
                f"short ${r['short_leg']['strike']} / long ${r['long_leg']['strike']} "
                f"net ${r['net_credit']:.2f} POP={r['pop']:.0f}% "
                f"C/L={r['credit_to_loss_ratio']:.2f} score={r['score']:.4f} "
                f"scenarios={scenarios_evaluated}"
            )
    else:
        logger.info(f"{symbol}: no qualifying CCS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    if top_n == 1:
        return (top[0] if top else None, scenarios_evaluated)
    return (top, scenarios_evaluated)


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
    short_strike_max_hint: float = None,
    min_pop: float = 70.0,
    risk_free_rate: float = 0.043,
    top_n: int = 1,
    earnings_dates: dict = None,
) -> Tuple[Optional[dict], int]:
    """
    Find the best Put Credit Spread (Bull Put Spread) for a symbol.

    Scoring (POP-weighted):
      delta = Black-Scholes delta of the short put
      POP   = 1 − |delta|
      Score = POP × (Credit / MaxLoss) × (365 / DTE)

    Guardrails: POP >= min_pop, no earnings before expiry.
    Returns (rec_or_list, scenarios_count). top_n=1 → single dict; top_n>1 → list.
    """
    symbol = symbol.upper()
    name   = name or symbol

    chain_data = _fetch_chains(symbol, dte_min, dte_max)
    if not chain_data:
        return (None, 0) if top_n == 1 else ([], 0)

    current_price = chain_data[0]["current_price"]
    if current_price <= 0:
        return (None, 0) if top_n == 1 else ([], 0)

    # Earnings guard
    _earnings_date = None
    if earnings_dates and symbol in earnings_dates and earnings_dates[symbol]:
        try:
            _earnings_date = date.fromisoformat(earnings_dates[symbol])
        except (ValueError, TypeError):
            pass

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

    short_strike_max = round(current_price * (1 - short_otm_pct / 100), 4)
    if short_strike_max_hint is not None:
        short_strike_max = min(short_strike_max, short_strike_max_hint)

    pop_threshold = min_pop / 100.0
    candidates: list = []
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte      = exp_data["dte"]
        exp_str  = exp_data["expiration"]
        puts     = sorted(exp_data["puts"], key=lambda p: p["strike"], reverse=True)

        if dte <= 0 or not (dte_min <= dte <= dte_max):
            continue

        # Earnings guardrail: reject expirations with earnings before expiry
        if _earnings_date:
            try:
                exp_date = date.fromisoformat(exp_str)
                if _earnings_date <= exp_date:
                    continue
            except (ValueError, TypeError):
                pass

        T = dte / 365.0

        for short_put in puts:
            short_strike = short_put["strike"]
            if short_strike > short_strike_max:
                continue
            if short_put["open_interest"] < min_open_interest:
                continue
            if short_put["bid"] <= 0:
                continue
            _otm_pct = (current_price - short_strike) / current_price * 100
            if _otm_pct > 30:
                _max_bid = current_price * 0.005
            elif _otm_pct > 20:
                _max_bid = current_price * 0.02
            else:
                _max_bid = current_price * 0.05
            if short_put["bid"] >= _max_bid:
                continue

            # Compute delta and POP for the short put
            iv = short_put.get("iv", 0)
            if iv > 0:
                delta = _bs_delta(current_price, short_strike, T, risk_free_rate, iv, "put")
            else:
                delta = 0.0
            pop = 1.0 - abs(delta) if delta != 0 else 0.0

            if pop < pop_threshold:
                continue

            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                long_target = short_strike - spread_size

                long_candidates = [
                    p for p in puts
                    if p["strike"] <= long_target + 0.01
                    and p["open_interest"] >= min_open_interest
                ]
                if not long_candidates:
                    continue

                long_put = min(long_candidates, key=lambda p: abs(p["strike"] - long_target))

                if long_put["ask"] <= 0:
                    continue

                actual_spread = round(short_strike - long_put["strike"], 2)
                if actual_spread > eff_spread_max + 0.01:
                    continue

                net_credit = round(short_put["bid"] - long_put["ask"], 2)
                if net_credit <= 0:
                    continue
                if net_credit < eff_target_premium:
                    continue
                net_credit_total = round(net_credit * 100, 2)
                max_loss = round(actual_spread * 100 - net_credit * 100, 2)
                ypd = round(net_credit * 100 / dte, 4)
                credit_to_loss_ratio = round(net_credit_total / max_loss, 2) if max_loss > 0 else 0.0
                score = round(pop * credit_to_loss_ratio * (365.0 / dte), 6)

                short_otm = round((1 - short_strike / current_price) * 100, 2)
                rec = {
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
                        "iv":            iv,
                        "delta":         round(delta, 4),
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
                    "pop":                   round(pop * 100, 1),
                    "score":                 score,
                }
                candidates.append(rec)

    candidates.sort(key=lambda r: r["score"], reverse=True)
    top = candidates[:top_n]

    if top:
        for r in top:
            logger.info(
                f"{symbol}: PCS — {r['expiration']} ({r['dte']}d) "
                f"short ${r['short_leg']['strike']} / long ${r['long_leg']['strike']} "
                f"net ${r['net_credit']:.2f} POP={r['pop']:.0f}% "
                f"C/L={r['credit_to_loss_ratio']:.2f} score={r['score']:.4f} "
                f"scenarios={scenarios_evaluated}"
            )
    else:
        logger.info(f"{symbol}: no qualifying PCS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    if top_n == 1:
        return (top[0] if top else None, scenarios_evaluated)
    return (top, scenarios_evaluated)


# ─────────────────────────────────────────────────────────────────────────────
# Debit spread scanners (insurance / protection)
# ─────────────────────────────────────────────────────────────────────────────

def scan_pds(
    symbol: str,
    name: str = None,
    spread_size_min: float = None,
    spread_size_max: float = None,
    dte_min: int = 1,
    dte_max: int = 60,
    max_debit_pct: float = 0.25,
    min_open_interest: int = 2,
    spread_size_min_pct: float = 1.0,
    spread_size_max_pct: float = 20.0,
    long_leg_offset: float = 0.05,
    max_dpd_pct: float = 0.01,
) -> Optional[dict]:
    """
    Find the best Put Debit Spread (bearish insurance) for a symbol.

    Long  leg: put with strike between price*(1-long_leg_offset) and price
    Short leg: put at long_strike − spread_size (further OTM, lower)

    You BUY the long put (protection) and SELL the short put (to reduce cost).
    Profitable when the stock falls below the short strike.

    net_debit = ask(long) − bid(short)
    DPD = net_debit × 100 / dte  (daily insurance cost)
    debit_to_win_ratio = net_debit / spread_size
    score = DPD × debit_to_win_ratio  (LOWEST wins = cheapest insurance)

    Args:
        symbol:              Stock ticker
        name:                Company name for display
        spread_size_min:     Min dollar width (overrides spread_size_min_pct)
        spread_size_max:     Max dollar width (overrides spread_size_max_pct)
        dte_min / dte_max:   DTE window (default 1–60 days)
        max_debit_pct:       Max net debit as fraction of spread width (default 25%)
        min_open_interest:   Min OI on both legs
        spread_size_min_pct: Min spread width as % of price (default 1%)
        spread_size_max_pct: Max spread width as % of price (default 20%)
        long_leg_offset:     How far from ATM the long leg can be (default 5%)
        max_dpd_pct:         Max DPD as fraction of stock value (default 1%)

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

    # Long leg range: price*(1-offset) to price (near-ATM puts)
    long_strike_min = round(current_price * (1 - long_leg_offset), 4)
    long_strike_max = round(current_price, 4)

    # Max DPD threshold: max_dpd_pct × stock price
    # DPD is already per-contract (net_debit×100/dte), so compare to price×pct
    max_dpd = current_price * max_dpd_pct

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

    best: Optional[dict] = None
    best_score: float = float("inf")   # lowest wins
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte     = exp_data["dte"]
        exp_str = exp_data["expiration"]
        puts    = sorted(exp_data["puts"], key=lambda p: p["strike"], reverse=True)

        if dte <= 0 or not (dte_min <= dte <= dte_max):
            continue

        for long_put in puts:
            long_strike = long_put["strike"]
            # Long leg must be within offset of current price
            if long_strike < long_strike_min or long_strike > long_strike_max:
                continue
            if long_put["open_interest"] < min_open_interest:
                continue
            if long_put["ask"] <= 0:
                continue

            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                short_target = long_strike - spread_size

                # Find nearest available short put strike <= short_target
                short_candidates = [
                    p for p in puts
                    if p["strike"] <= short_target + 0.01
                    and p["open_interest"] >= min_open_interest
                ]
                if not short_candidates:
                    continue

                short_put = min(short_candidates, key=lambda p: abs(p["strike"] - short_target))

                # Short leg must have a real bid
                if short_put["bid"] <= 0:
                    continue

                # Actual spread width
                actual_spread = round(long_strike - short_put["strike"], 2)
                if actual_spread <= 0:
                    continue
                if actual_spread > eff_spread_max + 0.01:
                    continue

                # Net debit (per share) — you pay this
                net_debit = round(long_put["ask"] - short_put["bid"], 2)
                if net_debit <= 0:
                    continue

                # Max debit filter: net_debit < max_debit_pct × spread_width
                if net_debit > max_debit_pct * actual_spread:
                    continue

                net_debit_total  = round(net_debit * 100, 2)
                max_protection   = round(actual_spread * 100, 2)
                dpd              = round(net_debit * 100 / dte, 4)

                # DPD filter: daily cost must be < max_dpd_pct of stock value
                if dpd >= max_dpd:
                    continue

                debit_to_win     = round(net_debit / actual_spread, 4) if actual_spread > 0 else 99.0
                score            = round(dpd * debit_to_win, 6)

                if score >= best_score:
                    continue

                long_otm = round((1 - long_strike / current_price) * 100, 2)
                best_score = score
                best = {
                    "symbol":        symbol,
                    "name":          name,
                    "current_price": round(current_price, 2),
                    "type":          "PDS",
                    "expiration":    exp_str,
                    "dte":           dte,
                    "long_leg": {
                        "strike":        long_strike,
                        "bid":           long_put["bid"],
                        "ask":           long_put["ask"],
                        "mid":           long_put["mid"],
                        "open_interest": long_put["open_interest"],
                        "otm_pct":       long_otm,
                    },
                    "short_leg": {
                        "strike":        short_put["strike"],
                        "bid":           short_put["bid"],
                        "ask":           short_put["ask"],
                        "mid":           short_put["mid"],
                        "open_interest": short_put["open_interest"],
                    },
                    "net_debit":            net_debit,
                    "net_debit_total":      net_debit_total,
                    "spread_size":          actual_spread,
                    "max_protection":       max_protection,
                    "dpd":                  dpd,
                    "debit_to_win_ratio":   debit_to_win,
                    "score":                score,
                }

    if best:
        logger.info(
            f"{symbol}: PDS best — {best['expiration']} ({best['dte']}d) "
            f"long ${best['long_leg']['strike']} / short ${best['short_leg']['strike']} "
            f"debit ${best['net_debit']:.2f} DPD={best['dpd']:.2f} "
            f"D/W={best['debit_to_win_ratio']:.2f} score={best['score']:.4f} "
            f"scenarios={scenarios_evaluated}"
        )
    else:
        logger.info(f"{symbol}: no qualifying PDS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    return (best, scenarios_evaluated)


def scan_cds(
    symbol: str,
    name: str = None,
    spread_size_min: float = None,
    spread_size_max: float = None,
    dte_min: int = 1,
    dte_max: int = 60,
    max_debit_pct: float = 0.25,
    min_open_interest: int = 2,
    spread_size_min_pct: float = 1.0,
    spread_size_max_pct: float = 20.0,
    long_leg_offset: float = 0.05,
    max_dpd_pct: float = 0.01,
) -> Optional[dict]:
    """
    Find the best Call Debit Spread (bullish insurance) for a symbol.

    Long  leg: call with strike between price and price*(1+long_leg_offset)
    Short leg: call at long_strike + spread_size (further OTM, higher)

    You BUY the long call (protection) and SELL the short call (to reduce cost).
    Profitable when the stock rises above the short strike.

    net_debit = ask(long) − bid(short)
    DPD = net_debit × 100 / dte  (daily insurance cost)
    debit_to_win_ratio = net_debit / spread_size
    score = DPD × debit_to_win_ratio  (LOWEST wins = cheapest insurance)

    Args: same as scan_pds() (mirrored for calls).
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

    # Long leg range: price to price*(1+offset) (near-ATM to slightly OTM calls)
    long_strike_min = round(current_price, 4)
    long_strike_max = round(current_price * (1 + long_leg_offset), 4)

    # Max DPD threshold: max_dpd_pct × stock price
    # DPD is already per-contract (net_debit×100/dte), so compare to price×pct
    max_dpd = current_price * max_dpd_pct

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

    best: Optional[dict] = None
    best_score: float = float("inf")   # lowest wins
    scenarios_evaluated: int = 0

    for exp_data in chain_data:
        dte     = exp_data["dte"]
        exp_str = exp_data["expiration"]
        calls   = sorted(exp_data["calls"], key=lambda c: c["strike"])

        if dte <= 0 or not (dte_min <= dte <= dte_max):
            continue

        for long_call in calls:
            long_strike = long_call["strike"]
            # Long leg must be within offset of current price
            if long_strike < long_strike_min or long_strike > long_strike_max:
                continue
            if long_call["open_interest"] < min_open_interest:
                continue
            if long_call["ask"] <= 0:
                continue

            for spread_size in spread_sizes:
                scenarios_evaluated += 1
                short_target = long_strike + spread_size

                # Find nearest available short call strike >= short_target
                short_candidates = [
                    c for c in calls
                    if c["strike"] >= short_target - 0.01
                    and c["open_interest"] >= min_open_interest
                ]
                if not short_candidates:
                    continue

                short_call = min(short_candidates, key=lambda c: abs(c["strike"] - short_target))

                # Short leg must have a real bid
                if short_call["bid"] <= 0:
                    continue

                # Actual spread width
                actual_spread = round(short_call["strike"] - long_strike, 2)
                if actual_spread <= 0:
                    continue
                if actual_spread > eff_spread_max + 0.01:
                    continue

                # Net debit (per share) — you pay this
                net_debit = round(long_call["ask"] - short_call["bid"], 2)
                if net_debit <= 0:
                    continue

                # Max debit filter: net_debit < max_debit_pct × spread_width
                if net_debit > max_debit_pct * actual_spread:
                    continue

                net_debit_total  = round(net_debit * 100, 2)
                max_protection   = round(actual_spread * 100, 2)
                dpd              = round(net_debit * 100 / dte, 4)

                # DPD filter: daily cost must be < max_dpd_pct of stock value
                if dpd >= max_dpd:
                    continue

                debit_to_win     = round(net_debit / actual_spread, 4) if actual_spread > 0 else 99.0
                score            = round(dpd * debit_to_win, 6)

                if score >= best_score:
                    continue

                long_otm = round((long_strike / current_price - 1) * 100, 2)
                best_score = score
                best = {
                    "symbol":        symbol,
                    "name":          name,
                    "current_price": round(current_price, 2),
                    "type":          "CDS",
                    "expiration":    exp_str,
                    "dte":           dte,
                    "long_leg": {
                        "strike":        long_strike,
                        "bid":           long_call["bid"],
                        "ask":           long_call["ask"],
                        "mid":           long_call["mid"],
                        "open_interest": long_call["open_interest"],
                        "otm_pct":       long_otm,
                    },
                    "short_leg": {
                        "strike":        short_call["strike"],
                        "bid":           short_call["bid"],
                        "ask":           short_call["ask"],
                        "mid":           short_call["mid"],
                        "open_interest": short_call["open_interest"],
                    },
                    "net_debit":            net_debit,
                    "net_debit_total":      net_debit_total,
                    "spread_size":          actual_spread,
                    "max_protection":       max_protection,
                    "dpd":                  dpd,
                    "debit_to_win_ratio":   debit_to_win,
                    "score":                score,
                }

    if best:
        logger.info(
            f"{symbol}: CDS best — {best['expiration']} ({best['dte']}d) "
            f"long ${best['long_leg']['strike']} / short ${best['short_leg']['strike']} "
            f"debit ${best['net_debit']:.2f} DPD={best['dpd']:.2f} "
            f"D/W={best['debit_to_win_ratio']:.2f} score={best['score']:.4f} "
            f"scenarios={scenarios_evaluated}"
        )
    else:
        logger.info(f"{symbol}: no qualifying CDS found (DTE {dte_min}–{dte_max}d, {scenarios_evaluated} scenarios)")

    return (best, scenarios_evaluated)


# ─────────────────────────────────────────────────────────────────────────────
# Insurance pipeline (qualifying holdings only)
# ─────────────────────────────────────────────────────────────────────────────

def run_insurance_pipeline(
    holdings: List[dict],
    config: dict,
    open_calls_detail: list = None,
    open_spreads_detail: list = None,
) -> dict:
    """
    Run PDS and CDS scans for qualifying portfolio holdings.

    PDS (downside protection): holdings with market value >= debit_min_holding_value.
    CDS (upside protection):   holdings with qty >= 100, or with open covered calls,
                               or with open CCS positions.

    Args:
        holdings:            Portfolio holdings list (from get_portfolio())
        config:              Loaded config dict (for debit_ keys)
        open_calls_detail:   Open covered call positions (for CDS trigger)
        open_spreads_detail: Open spread positions (for CDS trigger on CCS)

    Returns:
        {
          "pds": [rec, ...]  sorted by score ascending (lowest = best),
          "cds": [rec, ...]  sorted by score ascending (lowest = best),
          "pds_scenarios": int,
          "cds_scenarios": int,
        }
    """
    from datetime import datetime as _dt

    min_value       = float(config.get("debit_min_holding_value",
                            config.get("collar_min_holding_value", 10000)))
    dte_min         = int(config.get("debit_dte_min",             1))
    dte_max         = int(config.get("debit_dte_max",            60))
    max_debit_pct   = float(config.get("debit_max_debit_pct",  25.0)) / 100
    min_oi          = int(config.get("debit_min_open_interest",    2))
    size_min_pct    = float(config.get("debit_spread_size_min_pct", 1.0))
    size_max_pct    = float(config.get("debit_spread_size_max_pct", 20.0))
    long_leg_offset = float(config.get("debit_long_leg_offset_pct",  5.0)) / 100
    max_dpd_pct     = float(config.get("debit_max_dpd_pct",     1.0)) / 100

    open_calls_detail   = open_calls_detail or []
    open_spreads_detail = open_spreads_detail or []

    # Build lookup sets for CDS triggers
    # Symbols with open covered calls
    cc_symbols = {c.get("symbol", "").upper() for c in open_calls_detail}
    # Symbols with open CCS positions
    ccs_symbols = {
        s.get("symbol", "").upper()
        for s in open_spreads_detail
        if s.get("type", "").upper() == "CCS"
    }

    pds_recs: list = []
    cds_recs: list = []
    pds_scenarios_total: int = 0
    cds_scenarios_total: int = 0

    start = _dt.now()
    logger.info("=" * 60)
    logger.info(
        f"Insurance pipeline — {len(holdings)} holding(s) | "
        f"DTE {dte_min}–{dte_max}d | max debit {max_debit_pct*100:.0f}% | "
        f"spread {size_min_pct}%–{size_max_pct}% of price"
    )

    for i, h in enumerate(holdings, 1):
        sym   = h["symbol"]
        name  = h.get("name", sym)
        qty   = h.get("quantity", 0)
        price = h.get("price", 0)
        value = qty * price

        # ── PDS eligibility: market value >= threshold ──
        pds_eligible = value >= min_value
        pds_reason   = f"${value:,.0f} holding" if pds_eligible else None

        # ── CDS eligibility: qty >= 100 OR open CC OR open CCS ──
        cds_reasons = []
        if qty >= 100:
            cds_reasons.append(f"{qty} shares")
        if sym.upper() in cc_symbols:
            cds_reasons.append("Open CC")
        if sym.upper() in ccs_symbols:
            cds_reasons.append("Open CCS")
        cds_eligible = len(cds_reasons) > 0
        cds_reason   = " + ".join(cds_reasons) if cds_eligible else None

        if not pds_eligible and not cds_eligible:
            continue

        logger.info(
            f"  [{i}/{len(holdings)}] {sym}: "
            f"{'PDS' if pds_eligible else '-'}"
            f"{'/' if pds_eligible and cds_eligible else ''}"
            f"{'CDS' if cds_eligible else '-'}"
        )

        if pds_eligible:
            rec, cnt = scan_pds(
                sym, name=name,
                dte_min=dte_min, dte_max=dte_max,
                max_debit_pct=max_debit_pct, min_open_interest=min_oi,
                spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
                long_leg_offset=long_leg_offset, max_dpd_pct=max_dpd_pct,
            )
            pds_scenarios_total += cnt
            if rec:
                rec["trigger_reason"] = pds_reason
                pds_recs.append(rec)

        if cds_eligible:
            rec, cnt = scan_cds(
                sym, name=name,
                dte_min=dte_min, dte_max=dte_max,
                max_debit_pct=max_debit_pct, min_open_interest=min_oi,
                spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
                long_leg_offset=long_leg_offset, max_dpd_pct=max_dpd_pct,
            )
            cds_scenarios_total += cnt
            if rec:
                rec["trigger_reason"] = cds_reason
                cds_recs.append(rec)

    # Sort by score ascending (lowest = cheapest insurance)
    pds_recs.sort(key=lambda r: r.get("score", float("inf")))
    cds_recs.sort(key=lambda r: r.get("score", float("inf")))

    elapsed = (_dt.now() - start).total_seconds()
    logger.info(
        f"Insurance pipeline complete in {elapsed:.1f}s — "
        f"{len(pds_recs)} PDS rec(s) [{pds_scenarios_total} scenarios], "
        f"{len(cds_recs)} CDS rec(s) [{cds_scenarios_total} scenarios]"
    )
    return {
        "pds":            pds_recs,
        "cds":            cds_recs,
        "pds_scenarios":  pds_scenarios_total,
        "cds_scenarios":  cds_scenarios_total,
    }


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
          "ccs": [rec, ...]  sorted by score descending,
          "pcs": [rec, ...]  sorted by score descending,
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
    min_pop       = float(config.get("spread_min_pop",          70.0))
    _rfr_raw      = float(config.get("risk_free_rate",          4.3))
    risk_free     = _rfr_raw / 100.0 if _rfr_raw > 1 else _rfr_raw
    top_n         = int(config.get("spread_top_n",               1))

    # Earnings dates for the guardrail
    earnings_dates = config.get("_earnings_dates", None)

    ccs_recs: list = []
    pcs_recs: list = []
    ccs_scenarios_total: int = 0
    pcs_scenarios_total: int = 0

    start = _dt.now()
    logger.info("=" * 60)
    logger.info(
        f"Spread weekly pipeline — {len(holdings)} holding(s) | "
        f"DTE {dte_min}–{dte_max}d | OTM≥{short_otm}% | "
        f"spread {size_min_pct}%–{size_max_pct}% of price | "
        f"POP≥{min_pop}% | top_n={top_n}"
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
            min_pop=min_pop, risk_free_rate=risk_free, top_n=top_n,
            earnings_dates=earnings_dates,
        )
        pcs, pcs_cnt = scan_pcs(
            sym, name=name,
            dte_min=dte_min, dte_max=dte_max,
            short_otm_pct=short_otm, min_open_interest=min_oi,
            spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
            min_premium_pct=premium_pct,
            min_pop=min_pop, risk_free_rate=risk_free, top_n=top_n,
            earnings_dates=earnings_dates,
        )

        ccs_scenarios_total += ccs_cnt
        pcs_scenarios_total += pcs_cnt

        # Handle both single-rec (top_n=1) and multi-rec (top_n>1) returns
        if ccs:
            if isinstance(ccs, list):
                ccs_recs.extend(ccs)
            else:
                ccs_recs.append(ccs)
        if pcs:
            if isinstance(pcs, list):
                pcs_recs.extend(pcs)
            else:
                pcs_recs.append(pcs)

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
