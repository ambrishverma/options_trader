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
    "prev_close":      float,        # previous trading day's closing price
    "stock_up_today":  bool,         # True if current_price >= prev_close
    "otm_pct":         float,        # (strike - price) / price * 100
    "annualized_yield":float,        # (mid / price) * (365 / dte) * 100
  }
"""

import concurrent.futures
import logging
import math
from datetime import datetime, timedelta, date
from typing import List, Optional

import yfinance as yf

# Hard timeout (seconds) for fetching one symbol's options chain.
# yfinance has no built-in timeout, so a single slow symbol can stall the
# entire chain-fetch step for minutes.  We enforce this limit using a
# daemon thread; the stuck yfinance request is abandoned (wait=False).
_SYMBOL_FETCH_TIMEOUT = 45


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


def _safe_float(value, default: float = 0.0) -> float:
    """Convert value to float, treating NaN / None as `default`."""
    if value is None:
        return default
    try:
        f = float(value)
        return default if math.isnan(f) else f
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
    """Fetch current price from yfinance. Returns None on failure or NaN."""
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        # Prefer 1-day history — more reliable than fast_info which can return stale prices
        hist = ticker.history(period="1d")
        if not hist.empty:
            price = float(hist["Close"].iloc[-1])
            # Explicit NaN guard: hist can return NaN for illiquid/halted tickers
            if not math.isnan(price) and price > 0:
                return price
        # Fallback: fast_info
        info = ticker.fast_info
        raw = getattr(info, "last_price", None) or getattr(info, "regular_market_price", None)
        if raw is not None:
            price = float(raw)
            if not math.isnan(price) and price > 0:
                return price
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

        # Get live price + previous close for day-direction filter.
        # Use 2d history: iloc[-1] = today, iloc[-2] = previous trading day close.
        # Falls back to portfolio price if live fetch fails.
        current_price = 0.0
        prev_close    = 0.0
        try:
            hist2 = ticker.history(period="2d")
            if not hist2.empty:
                current_price = _safe_float(float(hist2["Close"].iloc[-1]))
                if len(hist2) >= 2:
                    prev_close = _safe_float(float(hist2["Close"].iloc[-2]))
        except Exception:
            pass
        if current_price <= 0 or math.isnan(current_price):
            current_price = _safe_float(get_live_price(symbol) or holding.get("price", 0))
        if current_price <= 0:
            logger.warning(f"{symbol}: Invalid price ({current_price}), skipping")
            return []
        # stock_up_today: True when current >= prev close (neutral treated as not-declining)
        stock_up_today = (prev_close <= 0) or (current_price >= prev_close)

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
                    strike = _safe_float(row.get("strike", 0))
                    bid    = _safe_float(row.get("bid",    0))
                    ask    = _safe_float(row.get("ask",    0))
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
                        "prev_close":       round(prev_close, 2),
                        "stock_up_today":   stock_up_today,
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
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            future = pool.submit(fetch_options_for_symbol, holding, lookahead_days)
            try:
                options = future.result(timeout=_SYMBOL_FETCH_TIMEOUT)
                all_options.extend(options)
            except concurrent.futures.TimeoutError:
                logger.warning(
                    f"{symbol}: options chain fetch timed out "
                    f"(>{_SYMBOL_FETCH_TIMEOUT}s) — skipping symbol"
                )
        except Exception as e:
            logger.warning(f"{symbol}: options chain fetch failed: {e} — skipping")
        finally:
            pool.shutdown(wait=False)  # abandon stuck yfinance thread; never block

    logger.info(f"Total options fetched: {len(all_options)} "
                f"across {len(holdings)} symbols")
    return all_options
