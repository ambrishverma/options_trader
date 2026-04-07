"""
roll_monitor.py — Roll-Forward and Buy-to-Close Candidate Detection
====================================================================
Analyses the open covered-call detail snapshot (saved each morning by the
Robinhood pull) and returns two candidate lists for the daily email:

  build_roll_forward_candidates()
    Open calls expiring in 0–5 calendar days where the stock is trading
    at or above the strike price (in-the-money).  These should be rolled
    before assignment.

    Also includes open CCS/PCS spread positions expiring in 0–5 calendar days
    where the stock price has moved between the short and long strike legs
    (the "danger zone" — the short leg is now ITM).

  build_btc_candidates()
    Open calls expiring in 5–14 calendar days that have no open BTC order
    on Robinhood.  Fetches a fresh mid price via yfinance per contract.

    Also includes open CCS/PCS spread positions expiring in 5–14 calendar days
    where the stock price is between the two legs and no BTC order is open.

Both functions accept:
  detail_contracts — list from load_open_calls_detail_snapshot()
  name_map         — {symbol: company_name}  (from portfolio holdings)
  live_prices      — {symbol: current_price} (from portfolio holdings)
  spread_contracts — list from load_open_spreads_detail_snapshot()  (optional)
"""

import logging
from datetime import date, datetime
from typing import List, Optional

import yfinance as yf

logger = logging.getLogger(__name__)


def _fresh_price(symbol: str) -> float:
    """Fetch the latest trade price for a symbol via yfinance."""
    try:
        info = yf.Ticker(symbol.replace(".", "-")).fast_info
        price = float(info.last_price or info.previous_close or 0)
        return price
    except Exception as e:
        logger.warning(f"{symbol}: live price fetch failed ({e})")
        return 0.0


def build_roll_forward_candidates(
    detail_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    spread_contracts: List[dict] = None,
) -> List[dict]:
    """
    Find open covered calls expiring within 5 days that are in-the-money.

    Args:
        detail_contracts: from load_open_calls_detail_snapshot()
        live_prices:      {symbol: price} — from portfolio holdings (2:30 AM pull)
        name_map:         {symbol: company_name} — optional, for display

    Returns list of candidate dicts sorted by expiration (most urgent first).
    """
    today = date.today()
    name_map = name_map or {}
    candidates = []

    # Reject yfinance-inferred records — their strikes/expiries are guesses and
    # will produce misleading roll-forward alerts.  Real data arrives after the
    # next 2:30 AM Robinhood pull.
    real_contracts = [c for c in detail_contracts if not c.get("_inferred")]
    if len(real_contracts) < len(detail_contracts):
        logger.warning(
            "Roll-forward scan skipping inferred contracts "
            f"({len(detail_contracts) - len(real_contracts)} dropped). "
            "Run --pull-portfolio for accurate data."
        )

    for contract in real_contracts:
        sym        = contract.get("symbol", "")
        strike     = float(contract.get("strike", 0) or 0)
        exp_str    = contract.get("expiration", "")
        qty        = int(contract.get("quantity", 0))

        if not sym or not exp_str or strike <= 0:
            continue

        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        dte = (exp_date - today).days
        if dte < 0 or dte > 5:
            continue

        # Always fetch a fresh price at pipeline time — the morning snapshot
        # can be 8+ hours old (2:30 AM pull) and miss intraday moves.
        live_price = _fresh_price(sym)
        if live_price <= 0:
            live_price = live_prices.get(sym, 0.0)   # fall back to snapshot

        if live_price <= 0 or live_price < strike:
            continue   # OTM — no action needed

        # Fetch current option mid (bid+ask)/2 for this contract
        current_mid = None
        try:
            ticker = yf.Ticker(sym.replace(".", "-"))
            chain  = ticker.option_chain(exp_str)
            calls  = chain.calls
            row    = calls[abs(calls["strike"] - strike) < 0.01]
            if not row.empty:
                bid = float(row.iloc[0]["bid"] or 0)
                ask = float(row.iloc[0]["ask"] or 0)
                if bid > 0 or ask > 0:
                    current_mid = round((bid + ask) / 2, 2)
        except Exception as e:
            logger.warning(f"{sym}: option mid fetch failed for roll-forward candidate ({e})")

        pp = contract.get("purchase_price")
        # Robinhood stores short-call credits as negative per-contract totals;
        # normalise to positive for display (it's money received, not owed).
        purchase_price = abs(pp) if pp is not None else None

        candidates.append({
            "symbol":         sym,
            "name":           name_map.get(sym, sym),
            "strike":         strike,
            "expiration":     exp_str,
            "dte":            dte,
            "quantity":       qty,
            "live_price":     round(live_price, 2),
            "itm_by":         round(live_price - strike, 2),
            "current_mid":    current_mid,
            "purchase_price": purchase_price,
        })

    # ── Open CCS/PCS spread positions in danger zone ─────────────────────────
    spread_candidates = _build_spread_roll_candidates(
        spread_contracts or [], live_prices, name_map or {}, dte_max=5
    )
    candidates.extend(spread_candidates)

    candidates.sort(key=lambda c: c["expiration"])
    logger.info(f"Roll-forward candidates: {len(candidates)} ({len(spread_candidates)} from spreads)")
    return candidates


def build_btc_candidates(
    detail_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    spread_contracts: List[dict] = None,
) -> List[dict]:
    """
    Find open covered calls expiring in 5–14 days with no open BTC order.
    Fetches a live mid price via yfinance for each candidate.

    Args:
        detail_contracts: from load_open_calls_detail_snapshot()
        live_prices:      {symbol: price} — from portfolio holdings
        name_map:         {symbol: company_name} — optional, for display

    Returns list of candidate dicts sorted by expiration (most urgent first).
    """
    today = date.today()
    name_map = name_map or {}
    candidates = []

    # Reject yfinance-inferred records — strikes are guesses, not real positions.
    real_contracts = [c for c in detail_contracts if not c.get("_inferred")]
    if len(real_contracts) < len(detail_contracts):
        logger.warning(
            "BTC scan skipping inferred contracts "
            f"({len(detail_contracts) - len(real_contracts)} dropped). "
            "Run --pull-portfolio for accurate data."
        )

    for contract in real_contracts:
        sym        = contract.get("symbol", "")
        strike     = float(contract.get("strike", 0) or 0)
        exp_str    = contract.get("expiration", "")
        qty        = int(contract.get("quantity", 0))
        btc_exists = bool(contract.get("btc_order_exists", False))

        if btc_exists:
            continue   # BTC already open — skip

        if not sym or not exp_str or strike <= 0:
            continue

        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        dte = (exp_date - today).days
        if dte <= 5 or dte > 14:
            continue

        # Fetch live mid price for this specific option contract
        current_mid = None
        try:
            ticker = yf.Ticker(sym.replace(".", "-"))
            chain  = ticker.option_chain(exp_str)
            calls  = chain.calls
            row    = calls[abs(calls["strike"] - strike) < 0.01]
            if not row.empty:
                bid = float(row.iloc[0]["bid"] or 0)
                ask = float(row.iloc[0]["ask"] or 0)
                if bid > 0 or ask > 0:
                    current_mid = round((bid + ask) / 2, 2)
        except Exception as e:
            logger.warning(f"{sym}: option mid fetch failed for BTC candidate ({e})")

        pp = contract.get("purchase_price")
        purchase_price = abs(pp) if pp is not None else None

        candidates.append({
            "symbol":         sym,
            "name":           name_map.get(sym, sym),
            "strike":         strike,
            "expiration":     exp_str,
            "dte":            dte,
            "quantity":       qty,
            "current_mid":    current_mid,
            "live_price":     round(live_prices.get(sym, 0.0), 2),
            "purchase_price": purchase_price,
        })

    # ── Open CCS/PCS spread positions in danger zone ─────────────────────────
    spread_candidates = _build_spread_btc_candidates(
        spread_contracts or [], live_prices, name_map or {}, dte_min=5, dte_max=14
    )
    candidates.extend(spread_candidates)

    candidates.sort(key=lambda c: c["expiration"])
    logger.info(f"Buy-to-close candidates: {len(candidates)} ({len(spread_candidates)} from spreads)")
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# Spread (CCS/PCS) roll-forward and BTC helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _build_spread_roll_candidates(
    spread_contracts: List[dict],
    live_prices: dict,
    name_map: dict,
    dte_max: int = 5,
) -> List[dict]:
    """
    Find open CCS/PCS spread positions expiring within dte_max days where the
    stock price has moved into the danger zone: short_strike < live_price < long_strike.

    For CCS: short call is ITM, long call still provides a cap — needs attention.
    For PCS: short put is ITM, long put provides a floor — needs attention.

    Returns list of candidate dicts with an extra "spread_type" field.
    """
    today = date.today()
    candidates = []

    for spread in spread_contracts:
        sym          = spread.get("symbol", "")
        spread_type  = spread.get("type", "")          # "CCS" or "PCS"
        short_strike = float(spread.get("short_strike", 0) or 0)
        long_strike  = float(spread.get("long_strike",  0) or 0)
        exp_str      = spread.get("expiration", "")
        qty          = int(spread.get("quantity", 0))

        if not sym or not exp_str or short_strike <= 0 or long_strike <= 0:
            continue

        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        dte = (exp_date - today).days
        if dte < 0 or dte > dte_max:
            continue

        # Always fetch a fresh price
        live_price = _fresh_price(sym)
        if live_price <= 0:
            live_price = live_prices.get(sym, 0.0)
        if live_price <= 0:
            continue

        # Danger zone: price between the two legs
        lo = min(short_strike, long_strike)
        hi = max(short_strike, long_strike)
        if not (lo < live_price < hi):
            continue

        # Fetch current spread mid (net of both legs)
        current_mid = _fetch_spread_mid(sym, exp_str, short_strike, long_strike, spread_type)

        pp = spread.get("purchase_price")
        purchase_price = abs(pp) if pp is not None else None

        candidates.append({
            "symbol":         sym,
            "name":           name_map.get(sym, sym),
            "spread_type":    spread_type,
            "short_strike":   short_strike,
            "long_strike":    long_strike,
            "expiration":     exp_str,
            "dte":            dte,
            "quantity":       qty,
            "live_price":     round(live_price, 2),
            "current_mid":    current_mid,
            "purchase_price": purchase_price,
            "is_spread":      True,
        })

    return candidates


def _build_spread_btc_candidates(
    spread_contracts: List[dict],
    live_prices: dict,
    name_map: dict,
    dte_min: int = 5,
    dte_max: int = 14,
) -> List[dict]:
    """
    Find open CCS/PCS spread positions expiring in (dte_min, dte_max] days where the
    stock price is in the danger zone and no BTC order exists.
    """
    today = date.today()
    candidates = []

    for spread in spread_contracts:
        sym          = spread.get("symbol", "")
        spread_type  = spread.get("type", "")
        short_strike = float(spread.get("short_strike", 0) or 0)
        long_strike  = float(spread.get("long_strike",  0) or 0)
        exp_str      = spread.get("expiration", "")
        qty          = int(spread.get("quantity", 0))
        btc_exists   = bool(spread.get("btc_order_exists", False))

        if btc_exists:
            continue

        if not sym or not exp_str or short_strike <= 0 or long_strike <= 0:
            continue

        try:
            exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
        except ValueError:
            continue

        dte = (exp_date - today).days
        if dte <= dte_min or dte > dte_max:
            continue

        live_price = _fresh_price(sym)
        if live_price <= 0:
            live_price = live_prices.get(sym, 0.0)
        if live_price <= 0:
            continue

        lo = min(short_strike, long_strike)
        hi = max(short_strike, long_strike)
        if not (lo < live_price < hi):
            continue   # not in danger zone — no alert needed

        current_mid = _fetch_spread_mid(sym, exp_str, short_strike, long_strike, spread_type)

        pp = spread.get("purchase_price")
        purchase_price = abs(pp) if pp is not None else None

        candidates.append({
            "symbol":         sym,
            "name":           name_map.get(sym, sym),
            "spread_type":    spread_type,
            "short_strike":   short_strike,
            "long_strike":    long_strike,
            "expiration":     exp_str,
            "dte":            dte,
            "quantity":       qty,
            "live_price":     round(live_price, 2),
            "current_mid":    current_mid,
            "purchase_price": purchase_price,
            "is_spread":      True,
        })

    return candidates


def _fetch_spread_mid(
    sym: str,
    exp_str: str,
    short_strike: float,
    long_strike: float,
    spread_type: str,
) -> Optional[float]:
    """
    Fetch the current net mid for a credit spread:
      net_mid = mid(short_leg) - mid(long_leg)

    For CCS: short leg is the higher-strike call, long leg is the lower-strike call.
    For PCS: short leg is the lower-strike put, long leg is the higher-strike put.
    """
    try:
        import yfinance as yf
        ticker = yf.Ticker(sym.replace(".", "-"))
        chain  = ticker.option_chain(exp_str)
        df     = chain.calls if spread_type == "CCS" else chain.puts

        def _get_mid(strike: float) -> Optional[float]:
            row = df[abs(df["strike"] - strike) < 0.01]
            if row.empty:
                return None
            bid = float(row.iloc[0]["bid"] or 0)
            ask = float(row.iloc[0]["ask"] or 0)
            if bid > 0 or ask > 0:
                return round((bid + ask) / 2, 2)
            return None

        short_mid = _get_mid(short_strike)
        long_mid  = _get_mid(long_strike)

        if short_mid is not None and long_mid is not None:
            return round(short_mid - long_mid, 2)
    except Exception as e:
        logger.warning(f"{sym}: spread mid fetch failed for {spread_type} {short_strike}/{long_strike} ({e})")
    return None
