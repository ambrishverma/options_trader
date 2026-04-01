"""
roll_monitor.py — Roll-Forward and Buy-to-Close Candidate Detection
====================================================================
Analyses the open covered-call detail snapshot (saved each morning by the
Robinhood pull) and returns two candidate lists for the daily email:

  build_roll_forward_candidates()
    Open calls expiring in 0–5 calendar days where the stock is trading
    at or above the strike price (in-the-money).  These should be rolled
    before assignment.

  build_btc_candidates()
    Open calls expiring in 5–14 calendar days that have no open BTC order
    on Robinhood.  Fetches a fresh mid price via yfinance per contract.

Both functions accept:
  detail_contracts — list from load_open_calls_detail_snapshot()
  name_map         — {symbol: company_name}  (from portfolio holdings)
  live_prices      — {symbol: current_price} (from portfolio holdings)
"""

import logging
from datetime import date, datetime
from typing import List

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

    for contract in detail_contracts:
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

        candidates.append({
            "symbol":     sym,
            "name":       name_map.get(sym, sym),
            "strike":     strike,
            "expiration": exp_str,
            "dte":        dte,
            "quantity":   qty,
            "live_price": round(live_price, 2),
            "itm_by":     round(live_price - strike, 2),
        })

    candidates.sort(key=lambda c: c["expiration"])
    logger.info(f"Roll-forward candidates: {len(candidates)}")
    return candidates


def build_btc_candidates(
    detail_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
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

    for contract in detail_contracts:
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

        candidates.append({
            "symbol":      sym,
            "name":        name_map.get(sym, sym),
            "strike":      strike,
            "expiration":  exp_str,
            "dte":         dte,
            "quantity":    qty,
            "current_mid": current_mid,
            "live_price":  round(live_prices.get(sym, 0.0), 2),
        })

    candidates.sort(key=lambda c: c["expiration"])
    logger.info(f"Buy-to-close candidates: {len(candidates)}")
    return candidates
