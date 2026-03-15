"""
earnings.py — Earnings Calendar with Finnhub + Daily Cache
============================================================
Fetches earnings dates for all recommendation symbols using Finnhub.io
as primary source, yfinance as fallback.

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
# yfinance fallback
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_yfinance_earnings(symbols: list) -> dict:
    """
    Fallback earnings fetch via yfinance.
    Less reliable but requires no API key.
    """
    import yfinance as yf

    result = {}
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym.replace(".", "-"))  # Convert to Yahoo format (e.g., BRK.B -> BRK-B)
            cal = ticker.calendar
            if cal is not None and not cal.empty:
                earnings_date = cal.columns[0] if hasattr(cal, "columns") else None
                # Handle both dict and DataFrame formats
                if hasattr(cal, "loc"):
                    ed = cal.T.get("Earnings Date", [None])[0]
                else:
                    ed = cal.get("Earnings Date")
                if ed is not None:
                    result[sym] = str(ed)[:10]
                    continue
        except Exception:
            pass
        result[sym] = None

    found = sum(1 for v in result.values() if v)
    logger.info(f"yfinance earnings fallback: found {found}/{len(symbols)} symbols")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Main earnings checker
# ─────────────────────────────────────────────────────────────────────────────

def get_earnings_dates(symbols: list) -> dict:
    """
    Get next earnings date for each symbol.
    Uses daily cache → Finnhub → yfinance fallback.

    Returns:
        dict: {symbol: "YYYY-MM-DD" or None}
    """
    # Load from cache if available
    cached = _load_cache()
    if cached:
        # Check if all requested symbols are in cache
        missing = [s for s in symbols if s not in cached]
        if not missing:
            return {sym: cached[sym] for sym in symbols}

    # Fetch fresh data
    api_key = os.getenv("FINNHUB_API_KEY", "").strip()

    if api_key:
        earnings = _fetch_finnhub_earnings(symbols, api_key)
        # Fill any gaps with yfinance
        missing = [sym for sym, val in earnings.items() if val is None]
        if missing:
            fallback = _fetch_yfinance_earnings(missing)
            for sym, val in fallback.items():
                if val and not earnings.get(sym):
                    earnings[sym] = val
    else:
        logger.warning("FINNHUB_API_KEY not set — using yfinance fallback for all symbols")
        earnings = _fetch_yfinance_earnings(symbols)

    # Merge with existing cache (if partial)
    full_cache = cached or {}
    full_cache.update(earnings)
    _save_cache(full_cache)
    _prune_old_cache()

    return {sym: earnings.get(sym) for sym in symbols}


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
