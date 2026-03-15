"""
options_chain.py — Options Chain Fetcher
==========================================
Fetches covered-call candidates for each eligible holding using yfinance.

For each symbol:
  1. Get all available expiration dates
  2. Filter to dates within the next `lookahead_days` calendar days (default 21)
  3. Fetch the calls chain for each qualifying expiration
  4. Return structured option records with all data needed for filtering/scoring

Each option record:
  {
    "symbol":          str,
    "name":            str,
    "shares":          float,
    "contracts":       int,
    "current_price":   float,
    "expiration":      str,          # "YYYY-MM-DD"
    "dte":             int,          # days to expiration
    "strike":          float,
    "bid":             float,
    "ask":             float,
    "mid":             float,        # (bid + ask) / 2
    "open_interest":   int,
    "volume":          int,
    "otm_pct":         float,        # (strike - price) / price * 100
    "annualized_yield":float,        # (mid / price) * (365 / dte) * 100
  }
"""

import logging
import math
from datetime import datetime, timedelta, date
from typing import List, Optional

import yfinance as yf


def _safe_int(value, default: int = 0) -> int:
    """Convert value to int, handling NaN and None gracefully."""
    if value is None:
        return default
    try:
        if math.isnan(value):
            return default
    except (TypeError, ValueError):
        pass
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _yahoo_symbol(symbol: str) -> str:
    """Convert broker-style symbol to Yahoo Finance format.

    Examples:
        BRK.B -> BRK-B
        BRK.A -> BRK-A
    """
    return symbol.replace(".", "-")


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Live price fetch
# ─────────────────────────────────────────────────────────────────────────────

def get_live_price(symbol: str) -> Optional[float]:
    """Fetch current price from yfinance. Returns None on failure."""
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        info = ticker.fast_info
        price = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
        if price and price > 0:
            return float(price)
        # Fallback: 1-day history
        hist = ticker.history(period="1d")
        if not hist.empty:
            return float(hist["Close"].iloc[-1])
    except Exception as e:
        logger.warning(f"Price fetch failed for {symbol}: {e}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Options chain for a single symbol
# ─────────────────────────────────────────────────────────────────────────────

def fetch_options_for_symbol(
    holding: dict,
    lookahead_days: int = 21,
) -> List[dict]:
    """
    Fetch all call options within lookahead_days for a single holding.

    Args:
        holding: dict with keys: symbol, name, shares, contracts, price
        lookahead_days: max calendar days to expiration

    Returns:
        List of option dicts (may be empty if no options found or error).
    """
    symbol = holding["symbol"]
    contracts = holding["contracts"]

    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))

        # Get live price (use portfolio price as fallback)
        current_price = get_live_price(symbol) or holding.get("price", 0)
        if current_price <= 0:
            logger.warning(f"{symbol}: Invalid price ({current_price}), skipping")
            return []

        # Get available expiration dates
        expirations = ticker.options
        if not expirations:
            logger.info(f"{symbol}: No options available")
            return []

        today = date.today()
        cutoff = today + timedelta(days=lookahead_days)

        # Filter expirations within window
        valid_expirations = [
            exp for exp in expirations
            if today < datetime.strptime(exp, "%Y-%m-%d").date() <= cutoff
        ]

        if not valid_expirations:
            logger.info(f"{symbol}: No expirations within {lookahead_days} days "
                        f"(next: {expirations[0] if expirations else 'none'})")
            return []

        results = []

        for exp_str in valid_expirations:
            try:
                chain = ticker.option_chain(exp_str)
                calls = chain.calls

                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
                dte = (exp_date - today).days

                for _, row in calls.iterrows():
                    strike = float(row.get("strike", 0))
                    bid    = float(row.get("bid",    0))
                    ask    = float(row.get("ask",    0))
                    oi     = _safe_int(row.get("openInterest"))
                    vol    = _safe_int(row.get("volume"))

                    if strike <= 0 or dte <= 0:
                        continue

                    mid    = (bid + ask) / 2
                    otm_pct = ((strike - current_price) / current_price) * 100

                    # Annualized yield: (mid / current_price) * (365 / dte) * 100
                    ann_yield = (mid / current_price) * (365 / dte) * 100 if mid > 0 else 0.0

                    results.append({
                        "symbol":           symbol,
                        "name":             holding.get("name", symbol),
                        "shares":           holding["shares"],
                        "contracts":        contracts,
                        "current_price":    round(current_price, 2),
                        "expiration":       exp_str,
                        "dte":              dte,
                        "strike":           strike,
                        "bid":              round(bid,  2),
                        "ask":              round(ask,  2),
                        "mid":              round(mid,  2),
                        "open_interest":    oi,
                        "volume":           vol,
                        "otm_pct":          round(otm_pct,    2),
                        "annualized_yield": round(ann_yield,  2),
                    })

            except Exception as e:
                logger.warning(f"{symbol}/{exp_str}: Chain fetch failed: {e}")
                continue

        logger.info(f"{symbol}: {len(results)} call options found "
                    f"across {len(valid_expirations)} expirations")
        return results

    except Exception as e:
        logger.error(f"{symbol}: Options fetch failed: {e}", exc_info=False)
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Batch fetch for all eligible holdings
# ─────────────────────────────────────────────────────────────────────────────

def fetch_all_options(
    holdings: List[dict],
    lookahead_days: int = 21,
) -> List[dict]:
    """
    Fetch options for all eligible holdings.

    Args:
        holdings: List of eligible holding dicts (shares >= 100)
        lookahead_days: Max DTE window

    Returns:
        Flat list of all option records from all holdings.
    """
    all_options = []

    for i, holding in enumerate(holdings, 1):
        symbol = holding["symbol"]
        logger.info(f"[{i}/{len(holdings)}] Fetching options for {symbol}")
        options = fetch_options_for_symbol(holding, lookahead_days=lookahead_days)
        all_options.extend(options)

    logger.info(f"Total options fetched: {len(all_options)} "
                f"across {len(holdings)} symbols")
    return all_options
