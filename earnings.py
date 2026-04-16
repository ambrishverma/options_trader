"""
earnings.py — Earnings Calendar with Finnhub + Alpha Vantage + yfinance
=========================================================================
Fetches earnings dates for all recommendation symbols using three sources
in priority order:

  1. Daily cache  (./cache/earnings_YYYY_MM_DD.json)
  2. Finnhub      (primary, if FINNHUB_API_KEY is set — bulk 90-day calendar)
  3. Alpha Vantage (optional second bulk source, if ALPHA_VANTAGE_API_KEY set)
  4. yfinance     (per-symbol fallback for any still-missing symbols)

Cache strategy:
  - Cache file: ./cache/earnings_YYYY_MM_DD.json
  - Written once per day; subsequent calls within the same day read from cache
  - Avoids redundant API calls (Finnhub free tier: 60 req/min)

For each symbol, determines:
  - earnings_date: next upcoming earnings date (or None)
  - within_window: True if earnings falls within the option's DTE window
  - warning:       human-readable warning string (or None)

Warning tiers:
  - 🔴 EARNINGS IN WINDOW:  earnings date falls before option expiration
  - 🟡 EARNINGS NEAR WINDOW: earnings within 3 days of expiration
  - None: no earnings concern
"""

import os
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

FINNHUB_BASE = "https://finnhub.io/api/v1"
BUFFER_DAYS = 3   # warn if earnings within BUFFER_DAYS of expiration


# ─────────────────────────────────────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cache_path() -> Path:
    today = date.today().strftime("%Y_%m_%d")
    return CACHE_DIR / f"earnings_{today}.json"


def _load_cache() -> Optional[dict]:
    cp = _cache_path()
    if cp.exists():
        try:
            with open(cp) as f:
                data = json.load(f)
            logger.debug(f"Earnings cache hit: {cp}")
            return data
        except Exception as e:
            logger.warning(f"Cache read failed: {e}")
    return None


def _save_cache(data: dict):
    cp = _cache_path()
    try:
        with open(cp, "w") as f:
            json.dump(data, f, indent=2)
        logger.debug(f"Earnings cache written: {cp}")
    except Exception as e:
        logger.warning(f"Cache write failed: {e}")


def _prune_old_cache():
    """Remove cache files older than 7 days to avoid accumulation."""
    cutoff = date.today() - timedelta(days=7)
    for fp in CACHE_DIR.glob("earnings_*.json"):
        try:
            # Extract date from filename: earnings_YYYY_MM_DD.json
            stem = fp.stem  # "earnings_2026_03_10"
            parts = stem.split("_")[1:]  # ["2026", "03", "10"]
            file_date = date(int(parts[0]), int(parts[1]), int(parts[2]))
            if file_date < cutoff:
                fp.unlink()
                logger.debug(f"Pruned old earnings cache: {fp.name}")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Finnhub API
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_finnhub_earnings(symbols: list, api_key: str) -> dict:
    """
    Fetch earnings calendar from Finnhub for the next 90 days.
    Returns dict: {symbol: earnings_date_str or None}
    """
    today = date.today()
    to_date = today + timedelta(days=90)

    result = {sym: None for sym in symbols}

    try:
        resp = requests.get(
            f"{FINNHUB_BASE}/calendar/earnings",
            params={
                "from": today.strftime("%Y-%m-%d"),
                "to":   to_date.strftime("%Y-%m-%d"),
                "token": api_key,
            },
            timeout=10,
        )

        if resp.status_code == 429:
            logger.warning("Finnhub rate limit hit — using yfinance fallback")
            return {}

        if resp.status_code != 200:
            logger.warning(f"Finnhub error: HTTP {resp.status_code}")
            return {}

        data = resp.json()
        earnings_calendar = data.get("earningsCalendar", [])

        # Build a map: symbol → earliest upcoming date
        sym_set = set(symbols)
        for entry in earnings_calendar:
            sym   = entry.get("symbol", "").upper()
            edate = entry.get("date", "")
            if sym in sym_set and edate:
                # Keep the earliest date only
                if result[sym] is None or edate < result[sym]:
                    result[sym] = edate

        found = sum(1 for v in result.values() if v)
        logger.info(f"Finnhub earnings: found {found}/{len(symbols)} symbols")

    except Exception as e:
        logger.warning(f"Finnhub fetch failed: {e}")
        return {}

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Alpha Vantage bulk earnings calendar (optional second source)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_alpha_vantage_earnings(symbols: list, api_key: str) -> dict:
    """
    Fetch upcoming earnings from Alpha Vantage's EARNINGS_CALENDAR endpoint.
    One API call returns a CSV of all upcoming earnings for the next 3 months —
    no per-symbol requests, no rate-limit concern.

    Returns dict: {symbol: earnings_date_str or None}
    """
    result = {sym: None for sym in symbols}
    sym_set = {s.upper() for s in symbols}

    try:
        resp = requests.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "EARNINGS_CALENDAR",
                "horizon":  "3month",
                "apikey":   api_key,
            },
            timeout=15,
        )
        if resp.status_code != 200:
            logger.warning(f"Alpha Vantage earnings: HTTP {resp.status_code}")
            return {}

        # Response is CSV: symbol,name,reportDate,fiscalDateEnding,estimate,currency
        lines = resp.text.strip().splitlines()
        if len(lines) < 2:
            return {}

        today_str = str(date.today())
        for line in lines[1:]:  # skip header
            parts = line.split(",")
            if len(parts) < 3:
                continue
            sym = parts[0].strip().upper()
            report_date = parts[2].strip()
            if sym in sym_set and report_date >= today_str:
                # Keep earliest date for each symbol
                if result.get(sym) is None or report_date < result[sym]:
                    result[sym] = report_date

        found = sum(1 for v in result.values() if v)
        logger.info(f"Alpha Vantage earnings: found {found}/{len(symbols)} symbols")

    except Exception as e:
        logger.warning(f"Alpha Vantage earnings fetch failed: {e}")
        return {}

    return result


# ─────────────────────────────────────────────────────────────────────────────
# yfinance per-symbol fallback
# ─────────────────────────────────────────────────────────────────────────────

def _yfinance_next_earnings(yahoo_sym: str, today_str: str) -> Optional[str]:
    """
    Try multiple yfinance APIs to find the next earnings date for a single symbol.

    Strategy 1: ticker.earnings_dates DataFrame (yfinance >= 0.2 — most reliable)
      - DatetimeIndex sorted descending (newest first); future dates have NaN EPS
    Strategy 2: ticker.calendar dict (yfinance >= 0.2 dict format)
      - Returns {"Earnings Date": [Timestamp, Timestamp], ...} (range estimate)
    Strategy 3: ticker.calendar legacy DataFrame (older yfinance builds)

    Returns "YYYY-MM-DD" or None.
    """
    import yfinance as yf

    ticker = yf.Ticker(yahoo_sym)

    # ── Strategy 1: earnings_dates DataFrame ────────────────────────────────
    try:
        df = ticker.earnings_dates
        if df is not None and not df.empty:
            # Index is tz-aware DatetimeIndex; strip tz and find future dates
            idx = df.index
            if getattr(idx, "tz", None) is not None:
                idx = idx.tz_convert(None)
            future = [
                d.strftime("%Y-%m-%d") for d in idx
                if d.strftime("%Y-%m-%d") >= today_str
            ]
            if future:
                return min(future)   # nearest upcoming (min of future dates)
    except Exception:
        pass

    # ── Strategy 2: calendar dict (modern yfinance) ──────────────────────────
    try:
        cal = ticker.calendar
        if isinstance(cal, dict):
            dates = cal.get("Earnings Date") or []
            if not isinstance(dates, (list, tuple)):
                dates = [dates]
            candidates = []
            for d in dates:
                if d is None:
                    continue
                ds = d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]
                if ds >= today_str:
                    candidates.append(ds)
            if candidates:
                return min(candidates)
        elif cal is not None and not getattr(cal, "empty", True):
            # ── Strategy 3: legacy DataFrame format ──────────────────────────
            try:
                # Older yfinance: DataFrame with date columns, rows = metrics
                row = cal.loc["Earnings Date"] if "Earnings Date" in cal.index else None
                if row is None and hasattr(cal, "T"):
                    transposed = cal.T
                    row = transposed["Earnings Date"] if "Earnings Date" in transposed.columns else None
                if row is not None:
                    d = row.iloc[0] if hasattr(row, "iloc") else row
                    ds = str(d)[:10]
                    if ds >= today_str:
                        return ds
            except Exception:
                pass
    except Exception:
        pass

    return None


def _fetch_yfinance_earnings(symbols: list) -> dict:
    """
    Per-symbol earnings fallback via yfinance.
    Tries three internal strategies per symbol (earnings_dates DataFrame,
    calendar dict, legacy calendar DataFrame).
    """
    import yfinance as yf  # noqa: F401 — imported inside for lazy loading

    today_str = str(date.today())
    result = {}
    for sym in symbols:
        result[sym] = _yfinance_next_earnings(sym.replace(".", "-"), today_str)

    found = sum(1 for v in result.values() if v)
    logger.info(f"yfinance earnings fallback: found {found}/{len(symbols)} symbols")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main earnings checker
# ─────────────────────────────────────────────────────────────────────────────

def get_earnings_dates(symbols: list) -> dict:
    """
    Get next earnings date for each symbol.

    Source priority:
      1. Daily cache (file per day — avoids repeat API calls within a run)
      2. Finnhub bulk calendar        (if FINNHUB_API_KEY is set)
      3. Alpha Vantage bulk calendar  (if ALPHA_VANTAGE_API_KEY is set)
      4. yfinance per-symbol fallback (for any still-missing symbols)

    Returns:
        dict: {symbol: "YYYY-MM-DD" or None}
    """
    # ── 1. Cache ──────────────────────────────────────────────────────────────
    cached = _load_cache()
    if cached:
        missing = [s for s in symbols if s not in cached]
        if not missing:
            return {sym: cached[sym] for sym in symbols}
        # Partial cache hit — only fetch the missing ones
        symbols_to_fetch = missing
    else:
        symbols_to_fetch = symbols

    earnings: dict = {sym: None for sym in symbols_to_fetch}

    # ── 2. Finnhub (primary bulk source) ─────────────────────────────────────
    finnhub_key = os.getenv("FINNHUB_API_KEY", "").strip()
    if finnhub_key:
        fh = _fetch_finnhub_earnings(symbols_to_fetch, finnhub_key)
        for sym, val in fh.items():
            if val:
                earnings[sym] = val
    else:
        logger.warning("FINNHUB_API_KEY not set — skipping Finnhub")

    # ── 3. Alpha Vantage (optional second bulk source) ────────────────────────
    still_missing = [s for s, v in earnings.items() if v is None]
    if still_missing:
        av_key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
        if av_key:
            av = _fetch_alpha_vantage_earnings(still_missing, av_key)
            for sym, val in av.items():
                if val:
                    earnings[sym] = val
        # (silently skipped if key not set — yfinance handles the gap)

    # ── 4. yfinance per-symbol fallback ──────────────────────────────────────
    still_missing = [s for s, v in earnings.items() if v is None]
    if still_missing:
        yf_result = _fetch_yfinance_earnings(still_missing)
        for sym, val in yf_result.items():
            if val:
                earnings[sym] = val

    # ── Merge with cache and persist ─────────────────────────────────────────
    full_cache = cached or {}
    full_cache.update(earnings)
    _save_cache(full_cache)
    _prune_old_cache()

    # Return requested symbols (combining cache hits + freshly fetched)
    combined = {**earnings, **(cached or {})}
    return {sym: combined.get(sym) for sym in symbols}


# ─────────────────────────────────────────────────────────────────────────────
# Warning generator
# ─────────────────────────────────────────────────────────────────────────────

def build_earnings_warnings(recommendations: list) -> list:
    """
    Annotate each recommendation with earnings warnings.

    For each recommendation, checks if the earnings date for that symbol
    falls before (or near) the option expiration date.

    Adds to each recommendation:
      "earnings_date":    str or None
      "earnings_warning": str or None  (warning message)
      "earnings_flag":    "red" | "yellow" | None
    """
    symbols = list({rec["symbol"] for rec in recommendations})
    earnings_map = get_earnings_dates(symbols)

    today = date.today()

    for rec in recommendations:
        sym = rec["symbol"]
        earnings_str = earnings_map.get(sym)

        rec["earnings_date"] = earnings_str
        rec["earnings_warning"] = None
        rec["earnings_flag"] = None

        if not earnings_str:
            continue

        try:
            earnings_dt = datetime.strptime(earnings_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        # Check against each leg's expiration
        legs = [rec["yield_leg"]]
        if rec.get("safety_leg"):
            legs.append(rec["safety_leg"])

        for leg in legs:
            exp_str = leg["option"]["expiration"]
            exp_dt  = datetime.strptime(exp_str, "%Y-%m-%d").date()

            if earnings_dt <= exp_dt:
                # Earnings before or on expiration — red flag
                days_before = (exp_dt - earnings_dt).days
                rec["earnings_warning"] = (
                    f"⚠️  EARNINGS BEFORE EXPIRY: {sym} reports ~{earnings_str} "
                    f"({days_before}d before {exp_str}). "
                    f"Selling this call exposes you to earnings volatility."
                )
                rec["earnings_flag"] = "red"
                break

            elif (earnings_dt - exp_dt).days <= BUFFER_DAYS:
                # Earnings within buffer zone
                rec["earnings_warning"] = (
                    f"🔔  EARNINGS NEAR EXPIRY: {sym} reports ~{earnings_str} "
                    f"(within {BUFFER_DAYS}d of expiry {exp_str}). "
                    f"Proceed with caution."
                )
                rec["earnings_flag"] = "yellow"

    flagged = sum(1 for r in recommendations if r.get("earnings_flag"))
    if flagged:
        logger.info(f"Earnings warnings: {flagged} of {len(recommendations)} symbols flagged")

    return recommendations


def annotate_candidates_with_earnings(candidates: list) -> list:
    """
    Attach ``earnings_date`` to each roll/BTC candidate or action-result dict.

    Reuses the daily cache populated earlier in the pipeline, so no extra API
    calls are made when the main recommendations have already been processed.

    Adds to each item:
      earnings_date: str ("YYYY-MM-DD") or None
    """
    if not candidates:
        return candidates
    symbols = list({c.get("symbol", "").upper() for c in candidates if c.get("symbol")})
    if not symbols:
        return candidates
    earnings_map = get_earnings_dates(symbols)
    for c in candidates:
        sym = c.get("symbol", "").upper()
        if "earnings_date" not in c:   # don't overwrite if already set
            c["earnings_date"] = earnings_map.get(sym)
    return candidates


def add_ex_dividend_dates(recommendations: list) -> list:
    """
    Annotate each recommendation with the next ex-dividend date.

    Adds to each recommendation:
      "ex_dividend_date": str | None   ("YYYY-MM-DD")
    """
    if not recommendations:
        return recommendations

    import yfinance as yf
    from datetime import datetime as dt

    symbols = list({rec["symbol"] for rec in recommendations})
    ex_div_map: dict = {}

    for sym in symbols:
        try:
            info = yf.Ticker(sym.replace(".", "-")).info
            ts   = info.get("exDividendDate")
            if ts:
                ex_div_map[sym] = dt.fromtimestamp(int(ts)).strftime("%Y-%m-%d")
            else:
                ex_div_map[sym] = None
        except Exception as e:
            logger.warning(f"{sym}: ex-dividend date fetch failed ({e})")
            ex_div_map[sym] = None

    for rec in recommendations:
        rec["ex_dividend_date"] = ex_div_map.get(rec["symbol"])

    fetched = sum(1 for v in ex_div_map.values() if v)
    logger.info(f"Ex-dividend dates: {fetched}/{len(symbols)} symbol(s) have upcoming ex-div")
    return recommendations
