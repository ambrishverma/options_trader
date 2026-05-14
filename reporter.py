"""
reporter.py — Options Trade Report Builder
===========================================
Fetches filled options orders from Robinhood for a date range and
produces a structured summary of Total Credit, Total Debit, and Net Gain.

Usage:
    from reporter import build_options_report
    report = build_options_report("04/09")             # today
    report = build_options_report("04/01-04/09")       # range

Report dict shape:
    {
        "start_date":   "2026-04-01",
        "end_date":     "2026-04-09",
        "orders": [
            {
                "date":          "2026-04-09",
                "symbol":        "TSLA",
                "type":          "CALL",   # CALL / PUT
                "side":          "sell",   # buy / sell
                "strike":        float,
                "expiration":    "2026-05-16",
                "quantity":      int,
                "price":         float,    # per share
                "premium":       float,    # total = price × quantity × 100
                "direction":     "credit", # credit / debit
                "order_id":      str,
            },
            ...
        ],
        "total_credit":  float,
        "total_debit":   float,
        "net_gain":      float,            # credit − debit (positive = profit)
        "order_count":   int,
    }
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

LOCAL = ZoneInfo("America/Los_Angeles")   # machine timezone (PT)
ET    = ZoneInfo("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# Date helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_range(arg: Optional[str]) -> Tuple[date, date]:
    """
    Parse the optional --report argument into (start_date, end_date).

    Accepted formats:
        None            → today (same as mm/dd for today)
        "mm/dd"         → single day in the current year
        "mm/dd-mm/dd"   → date range in the current year

    Returns a (start, end) tuple where start ≤ end.
    Raises ValueError for unrecognised formats.
    """
    today = date.today()
    if arg is None:
        return today, today

    # Normalise — allow spaces around "-"
    arg = arg.strip()

    # Single-date: mm/dd
    single_re = r"^(\d{1,2})/(\d{1,2})$"
    range_re  = r"^(\d{1,2})/(\d{1,2})\s*-\s*(\d{1,2})/(\d{1,2})$"

    m = re.match(single_re, arg)
    if m:
        month, day = int(m.group(1)), int(m.group(2))
        d = date(today.year, month, day)
        return d, d

    m = re.match(range_re, arg)
    if m:
        start = date(today.year, int(m.group(1)), int(m.group(2)))
        end   = date(today.year, int(m.group(3)), int(m.group(4)))
        if start > end:
            raise ValueError(f"Start date {start} is after end date {end}")
        return start, end

    raise ValueError(
        f"Unrecognised date format: '{arg}'. "
        "Expected 'mm/dd' or 'mm/dd-mm/dd'  (e.g. '04/09' or '04/01-04/09')"
    )


def _execution_date_local(order: dict) -> Optional[date]:
    """
    Return the local (PT) date of the first filled execution in an order,
    or None if no executions are available.

    Robinhood timestamps are UTC ISO strings, e.g. "2026-04-09T17:45:00Z".
    We convert to local time before extracting the date so that trades
    executed near midnight aren't mis-attributed.
    """
    legs = order.get("legs") or []
    for leg in legs:
        executions = leg.get("executions") or []
        for ex in executions:
            ts_str = ex.get("timestamp") or ex.get("settled_at")
            if ts_str:
                try:
                    # Parse UTC timestamp
                    ts_str = ts_str.replace("Z", "+00:00")
                    dt_utc = datetime.fromisoformat(ts_str)
                    dt_local = dt_utc.astimezone(LOCAL)
                    return dt_local.date()
                except (ValueError, TypeError):
                    continue

    # Fallback: use order-level created_at if no executions have timestamps
    created = order.get("created_at")
    if created:
        try:
            created = created.replace("Z", "+00:00")
            dt_utc = datetime.fromisoformat(created)
            return dt_utc.astimezone(LOCAL).date()
        except (ValueError, TypeError):
            pass

    return None


def _order_date_local(order: dict) -> Optional[date]:
    """
    Return the local (PT) date an order was last updated / filled,
    preferring updated_at over created_at.
    """
    for field in ("updated_at", "created_at"):
        ts_str = order.get(field)
        if ts_str:
            try:
                ts_str = ts_str.replace("Z", "+00:00")
                dt_utc = datetime.fromisoformat(ts_str)
                return dt_utc.astimezone(LOCAL).date()
            except (ValueError, TypeError):
                continue
    return None


def _get_order_date(order: dict) -> Optional[date]:
    """Best-effort: try execution timestamps first, fall back to updated_at."""
    d = _execution_date_local(order)
    if d:
        return d
    return _order_date_local(order)


# ─────────────────────────────────────────────────────────────────────────────
# Leg data extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_leg_info(order: dict) -> dict:
    """
    Pull strike, expiration, type and side from the first leg of an order.
    Returns a dict with keys: strike, expiration, option_type, side.
    """
    legs = order.get("legs") or []
    if not legs:
        return {}
    leg = legs[0]
    return {
        "side":       (leg.get("side") or "").lower(),            # "buy" / "sell"
        "option_type": (leg.get("option_type") or "").upper(),    # "CALL" / "PUT"
        "strike":     float(leg.get("strike_price") or 0),
        "expiration": leg.get("expiration_date") or "",           # YYYY-MM-DD
    }


# ─────────────────────────────────────────────────────────────────────────────
# Core report builder
# ─────────────────────────────────────────────────────────────────────────────

def build_options_report(date_arg: Optional[str] = None) -> dict:
    """
    Fetch all filled options orders from Robinhood for the given date range
    and return a structured summary report.

    Parameters
    ----------
    date_arg : Optional[str]
        Date range string as accepted by _parse_date_range().
        None → today only.

    Returns
    -------
    dict — the report (see module docstring for shape).
    """
    start_date, end_date = _parse_date_range(date_arg)
    logger.info(f"📋  Building options report: {start_date} → {end_date}")

    # Login to Robinhood
    from auth import login, logout
    if not login():
        raise RuntimeError("Robinhood login failed — cannot fetch orders")

    try:
        import robin_stocks.robinhood as rh
        raw_orders = rh.orders.get_all_option_orders() or []
    finally:
        logout()

    logger.info(f"  Fetched {len(raw_orders)} total option orders from Robinhood")

    # Filter: filled orders only, within the date window
    matched_orders = []
    for order in raw_orders:
        state = (order.get("state") or "").lower()
        if state != "filled":
            continue

        order_date = _get_order_date(order)
        if order_date is None:
            continue

        if not (start_date <= order_date <= end_date):
            continue

        leg_info = _extract_leg_info(order)

        # quantity = number of contracts
        quantity  = int(float(order.get("quantity") or 0))
        # price stored as per-share (Robinhood format)
        price     = float(order.get("price") or 0)
        # premium = total dollar amount  (price × contracts × 100 shares/contract)
        # robin_stocks may provide "premium" directly as well
        premium_raw = order.get("premium")
        if premium_raw is not None:
            premium = abs(float(premium_raw))
        else:
            premium = round(price * quantity * 100, 2)

        direction = (order.get("direction") or "").lower()

        matched_orders.append({
            "date":        str(order_date),
            "symbol":      (order.get("chain_symbol") or "").upper(),
            "type":        leg_info.get("option_type", ""),
            "side":        leg_info.get("side", ""),
            "strike":      leg_info.get("strike", 0.0),
            "expiration":  leg_info.get("expiration", ""),
            "quantity":    quantity,
            "price":       round(price, 2),
            "premium":     round(premium, 2),
            "direction":   direction,
            "order_id":    order.get("id", ""),
        })

    # Sort by date ascending, then by symbol
    matched_orders.sort(key=lambda o: (o["date"], o["symbol"]))

    total_credit = sum(o["premium"] for o in matched_orders if o["direction"] == "credit")
    total_debit  = sum(o["premium"] for o in matched_orders if o["direction"] == "debit")
    net_gain     = round(total_credit - total_debit, 2)

    logger.info(
        f"  Report: {len(matched_orders)} orders | "
        f"Credit ${total_credit:.2f} | Debit ${total_debit:.2f} | Net ${net_gain:+.2f}"
    )

    return {
        "start_date":   str(start_date),
        "end_date":     str(end_date),
        "orders":       matched_orders,
        "total_credit": round(total_credit, 2),
        "total_debit":  round(total_debit, 2),
        "net_gain":     net_gain,
        "order_count":  len(matched_orders),
    }
