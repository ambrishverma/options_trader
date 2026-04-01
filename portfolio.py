"""
portfolio.py — Portfolio Loading
==================================
Two data paths:
  1. Spreadsheet reader (primary daily path)
     Reads the most recent snapshot from ./snapshots/*.json
     Falls back to reading a .xlsx file if no snapshot exists.

  2. Robinhood API pull (automated daily refresh)
     Called every trading day at 2:30 AM ET.
     Saves a timestamped JSON snapshot to ./snapshots/.

Each holding is a dict:
  {
    "symbol":  "NVDA",
    "name":    "NVIDIA Corporation",
    "shares":  float,       # total shares held
    "price":   float,       # current price (from Robinhood or live fetch)
    "eligible": bool,       # True if shares >= 100
    "contracts": int,       # floor(shares / 100)
  }
"""

import os
import json
import glob
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
SNAPSHOT_DIR = BASE_DIR / "snapshots"
SNAPSHOT_DIR.mkdir(exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# Robinhood API Pull  (single-session: portfolio + open calls together)
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_open_calls_in_session(rh) -> tuple:
    """
    Fetch open short-call positions using an ALREADY AUTHENTICATED rh session.
    No login/logout — the caller owns the session lifecycle.

    Also fetches open option orders to detect existing BTC (buy-to-close) orders.

    Returns:
        (summary_dict, detail_list) where:
          summary_dict  — {symbol: contract_count}  (for pipeline deduplication)
          detail_list   — [{symbol, strike, expiration, quantity, btc_order_exists}]
    """
    logger.info("Fetching open options positions from Robinhood...")
    positions = rh.options.get_open_option_positions() or []
    logger.info(f"  {len(positions)} open option position(s) found")

    # ── Detect existing BTC orders (buy-to-close = buy-side call orders) ──────
    btc_option_ids: set = set()
    try:
        open_orders = rh.orders.get_all_open_option_orders() or []
        for order in open_orders:
            if (order.get("side") or "").lower() != "buy":
                continue
            for leg in order.get("legs", []):
                opt_url = leg.get("option", "")
                if opt_url:
                    oid = opt_url.rstrip("/").split("/")[-1]
                    btc_option_ids.add(oid)
        logger.info(f"  {len(btc_option_ids)} open BTC order(s) found")
    except Exception as e:
        logger.warning(f"  Could not fetch open option orders: {e}")

    open_calls: dict = {}
    detail_list: list = []

    for pos in positions:
        try:
            qty      = float(pos.get("quantity", 0))
            pos_type = (pos.get("type") or "").lower()
            symbol   = (pos.get("chain_symbol") or "").upper()

            if qty <= 0 or pos_type != "short" or not symbol:
                continue

            option_id   = pos.get("option_id", "")
            option_type = ""
            strike      = 0.0
            expiration  = pos.get("expiration_date", "")

            if option_id:
                try:
                    instrument  = rh.options.get_option_instrument_data_by_id(option_id)
                    option_type = (instrument.get("type") or "").lower()
                    strike      = float(instrument.get("strike_price", 0) or 0)
                    expiration  = instrument.get("expiration_date", expiration) or expiration
                except Exception as inst_exc:
                    logger.warning(
                        f"  {symbol}: could not fetch instrument {option_id}: {inst_exc}"
                    )

            if option_type == "call":
                qty_int = int(qty)
                open_calls[symbol] = open_calls.get(symbol, 0) + qty_int
                btc_exists = option_id in btc_option_ids
                detail_list.append({
                    "symbol":          symbol,
                    "strike":          strike,
                    "expiration":      expiration,
                    "quantity":        qty_int,
                    "btc_order_exists": btc_exists,
                    "option_id":       option_id,
                })
                logger.info(
                    f"  Open covered call: {symbol} — {qty_int} contract(s)"
                    f" strike ${strike} exp {expiration}"
                    + (" [BTC open]" if btc_exists else "")
                )
            else:
                logger.debug(
                    f"  {symbol}: short {option_type or 'unknown'} position ignored"
                )

        except (TypeError, ValueError) as exc:
            logger.warning(f"Skipping option position record: {exc}")

    logger.info(
        f"Open covered calls: {len(open_calls)} symbol(s) with existing positions — "
        + (str(dict(open_calls)) if open_calls else "none")
    )
    return open_calls, detail_list


def pull_daily_robinhood_snapshot() -> Optional[str]:
    """
    Single Robinhood session: fetch portfolio holdings AND open covered calls.

    Saves to disk:
      - ./snapshots/portfolio_YYYYMMDD_HHMMSS.json   (holdings)
      - ./snapshots/open_calls_YYYYMMDD.json          (short-call positions)

    Using one session avoids a second login at pipeline time, which would
    trigger Robinhood's device-verification challenge and hang indefinitely.

    Returns the portfolio snapshot path on success, None on failure.
    """
    try:
        from auth import login, logout
        import robin_stocks.robinhood as rh

        login()
        logger.info("Fetching Robinhood holdings...")

        raw = rh.build_holdings()   # dict: {symbol: {price, quantity, ...}}
        holdings = []
        for symbol, data in raw.items():
            try:
                shares = float(data.get("quantity", 0))
                price  = float(data.get("price", 0))
                name   = data.get("name", symbol)
                holdings.append({
                    "symbol":    symbol.upper(),
                    "name":      name,
                    "shares":    shares,
                    "price":     price,
                    "eligible":  shares >= 100,
                    "contracts": int(shares // 100),
                })
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping {symbol}: {e}")

        # Fetch open covered calls in the SAME authenticated session
        open_calls, open_calls_detail = _fetch_open_calls_in_session(rh)

        logout()

        # Save portfolio snapshot
        holdings.sort(key=lambda h: h["shares"] * h["price"], reverse=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        snap_path = SNAPSHOT_DIR / f"portfolio_{ts}.json"
        with open(snap_path, "w") as f:
            json.dump({
                "pulled_at": datetime.now().isoformat(),
                "source": "robinhood_api",
                "holdings": holdings,
            }, f, indent=2)
        logger.info(f"✅  Portfolio snapshot saved: {snap_path} "
                    f"({len(holdings)} holdings, "
                    f"{sum(1 for h in holdings if h['eligible'])} eligible)")

        # Save open calls summary snapshot (one file per day, overwrite if re-run)
        today_str = datetime.now().strftime("%Y%m%d")
        calls_path = SNAPSHOT_DIR / f"open_calls_{today_str}.json"
        with open(calls_path, "w") as f:
            json.dump({
                "pulled_at": datetime.now().isoformat(),
                "open_calls": open_calls,
            }, f, indent=2)
        logger.info(f"✅  Open calls snapshot saved: {calls_path} "
                    f"({sum(open_calls.values())} contract(s) across "
                    f"{len(open_calls)} symbol(s))")

        # Save per-contract detail snapshot (for roll-forward / BTC detection)
        detail_path = SNAPSHOT_DIR / f"open_calls_detail_{today_str}.json"
        with open(detail_path, "w") as f:
            json.dump({
                "pulled_at": datetime.now().isoformat(),
                "contracts": open_calls_detail,
            }, f, indent=2)
        logger.info(f"✅  Open calls detail snapshot saved: {detail_path} "
                    f"({len(open_calls_detail)} contract record(s))")

        return str(snap_path)

    except Exception as e:
        logger.error(f"❌  Robinhood daily snapshot failed: {e}", exc_info=True)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Open Covered-Call Snapshot Reader
# ─────────────────────────────────────────────────────────────────────────────

def load_open_calls_snapshot() -> dict:
    """
    Load the most recent open_calls snapshot from ./snapshots/open_calls_*.json.

    The snapshot is written by pull_daily_robinhood_snapshot() at 2:30 AM ET
    alongside the portfolio snapshot — no second Robinhood login needed at
    pipeline time.

    Returns {symbol: contract_count}, or {} if no snapshot exists yet.
    """
    snapshots = sorted(glob.glob(str(SNAPSHOT_DIR / "open_calls_*.json")), reverse=True)
    if not snapshots:
        logger.warning("No open_calls snapshot found — assuming no open covered calls")
        return {}

    latest = snapshots[0]
    logger.info(f"Loading open calls snapshot: {latest}")

    try:
        with open(latest) as f:
            data = json.load(f)
        open_calls = data.get("open_calls", {})
        pulled_at  = data.get("pulled_at", "unknown")
        total_cts  = sum(open_calls.values())
        logger.info(
            f"Open calls snapshot from {pulled_at} — "
            f"{total_cts} contract(s) across {len(open_calls)} symbol(s)"
            + (f": {dict(open_calls)}" if open_calls else "")
        )
        return open_calls
    except Exception as e:
        logger.error(f"Failed to load open calls snapshot: {e}")
        return {}


def load_open_calls_detail_snapshot() -> list:
    """
    Load the most recent open_calls_detail snapshot.

    Returns list of per-contract dicts:
      [{symbol, strike, expiration, quantity, btc_order_exists, option_id}]
    Returns [] if no snapshot exists.
    """
    snapshots = sorted(
        glob.glob(str(SNAPSHOT_DIR / "open_calls_detail_*.json")), reverse=True
    )
    if not snapshots:
        logger.warning("No open_calls_detail snapshot found — roll/BTC sections will be empty")
        return []

    latest = snapshots[0]
    logger.info(f"Loading open calls detail snapshot: {latest}")

    try:
        with open(latest) as f:
            data = json.load(f)
        contracts  = data.get("contracts", [])
        pulled_at  = data.get("pulled_at", "unknown")
        logger.info(f"  {len(contracts)} contract record(s) from {pulled_at}")
        return contracts
    except Exception as e:
        logger.error(f"Failed to load open calls detail snapshot: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Snapshot Reader
# ─────────────────────────────────────────────────────────────────────────────

def load_latest_snapshot() -> Optional[list]:
    """
    Load the most recent portfolio snapshot from ./snapshots/*.json.
    Returns list of holdings, or None if no snapshot exists.
    """
    snapshots = sorted(glob.glob(str(SNAPSHOT_DIR / "portfolio_*.json")), reverse=True)
    if not snapshots:
        logger.warning("No portfolio snapshot found in ./snapshots/")
        return None

    latest = snapshots[0]
    logger.info(f"Loading snapshot: {latest}")

    with open(latest) as f:
        data = json.load(f)

    holdings = data.get("holdings", [])
    pulled_at = data.get("pulled_at", "unknown")
    logger.info(f"Portfolio snapshot from {pulled_at} — {len(holdings)} holdings")

    return holdings


# ─────────────────────────────────────────────────────────────────────────────
# Spreadsheet Fallback Reader
# ─────────────────────────────────────────────────────────────────────────────

def load_from_spreadsheet(xlsx_path: str) -> Optional[list]:
    """
    Read portfolio from a .xlsx file (Robinhood export format).
    Expected columns (case-insensitive, partial match ok):
      Name, Symbol, Shares/Quantity, Price/Average cost

    Returns list of holdings dicts.
    """
    try:
        df = pd.read_excel(xlsx_path, header=0)

        # Normalize column names
        df.columns = [c.strip().lower() for c in df.columns]

        def find_col(candidates):
            for c in candidates:
                for col in df.columns:
                    if c in col:
                        return col
            return None

        sym_col    = find_col(["symbol", "ticker"])
        name_col   = find_col(["name", "stock"])
        shares_col = find_col(["shares", "quantity"])
        price_col  = find_col(["price", "last price", "current"])

        if not sym_col:
            raise ValueError("Cannot find symbol/ticker column in spreadsheet")
        if not shares_col:
            raise ValueError("Cannot find shares/quantity column in spreadsheet")

        holdings = []
        for _, row in df.iterrows():
            symbol = str(row.get(sym_col, "")).strip().upper()
            if not symbol or symbol == "NAN":
                continue

            try:
                shares = float(str(row.get(shares_col, 0)).replace(",", ""))
                price  = float(str(row.get(price_col, 0)).replace(",", "").replace("$", "")) if price_col else 0.0
                name   = str(row.get(name_col, symbol)) if name_col else symbol

                holdings.append({
                    "symbol":    symbol,
                    "name":      name,
                    "shares":    shares,
                    "price":     price,
                    "eligible":  shares >= 100,
                    "contracts": int(shares // 100),
                })
            except (TypeError, ValueError) as e:
                logger.warning(f"Skipping row ({symbol}): {e}")

        logger.info(f"Loaded {len(holdings)} holdings from spreadsheet — "
                    f"{sum(1 for h in holdings if h['eligible'])} eligible")

        # Save as a snapshot so pipeline can use snapshot path going forward
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        snap_path = SNAPSHOT_DIR / f"portfolio_{ts}.json"
        with open(snap_path, "w") as f:
            json.dump({
                "pulled_at": datetime.now().isoformat(),
                "source": "spreadsheet",
                "source_file": xlsx_path,
                "holdings": holdings,
            }, f, indent=2)
        logger.info(f"Spreadsheet snapshot saved: {snap_path}")

        return holdings

    except Exception as e:
        logger.error(f"Spreadsheet read failed: {e}", exc_info=True)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Primary Portfolio Loader
# ─────────────────────────────────────────────────────────────────────────────

def get_portfolio(xlsx_path: str = None) -> list:
    """
    Main entry point for portfolio loading.
    Priority:
      1. Latest snapshot (if exists)
      2. Spreadsheet file (if xlsx_path provided)
      3. Raises RuntimeError if neither available.

    Returns list of eligible holdings (shares >= 100) only.
    """
    holdings = load_latest_snapshot()

    if not holdings and xlsx_path:
        logger.info(f"No snapshot found — falling back to spreadsheet: {xlsx_path}")
        holdings = load_from_spreadsheet(xlsx_path)

    if not holdings:
        raise RuntimeError(
            "No portfolio data available. "
            "Run: python main.py --pull-portfolio  OR  provide a .xlsx spreadsheet path."
        )

    # Return only eligible holdings
    eligible = [h for h in holdings if h["eligible"]]
    logger.info(f"Eligible holdings (≥100 shares): {len(eligible)}")
    return eligible
