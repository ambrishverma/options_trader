"""
portfolio.py — Portfolio Loading
==================================
Two data paths:
  1. Spreadsheet reader (primary daily path)
     Reads the most recent snapshot from ./snapshots/*.json
     Falls back to reading a .xlsx file if no snapshot exists.

  2. Robinhood API pull (automated weekly refresh)
     Called every Monday morning at 6:00 AM ET.
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
# Robinhood API Pull
# ─────────────────────────────────────────────────────────────────────────────

def pull_robinhood_portfolio() -> Optional[str]:
    """
    Login to Robinhood, fetch holdings, save a JSON snapshot.
    Returns the snapshot file path on success, None on failure.
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

        logout()

        # Sort by equity descending
        holdings.sort(key=lambda h: h["shares"] * h["price"], reverse=True)

        # Write snapshot
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        snap_path = SNAPSHOT_DIR / f"portfolio_{ts}.json"
        with open(snap_path, "w") as f:
            json.dump({
                "pulled_at": datetime.now().isoformat(),
                "source": "robinhood_api",
                "holdings": holdings,
            }, f, indent=2)

        logger.info(f"✅  Snapshot saved: {snap_path} ({len(holdings)} holdings, "
                    f"{sum(1 for h in holdings if h['eligible'])} eligible)")
        return str(snap_path)

    except Exception as e:
        logger.error(f"❌  Robinhood pull failed: {e}", exc_info=True)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Open Covered-Call Position Checker
# ─────────────────────────────────────────────────────────────────────────────

def get_open_covered_calls() -> dict:
    """
    Login to Robinhood, fetch all open options positions, and return a dict
    mapping each symbol that already has at least one short-call contract to
    the number of contracts currently open:

        { "AAPL": 2, "NVDA": 1, ... }

    Only SHORT CALL positions are counted (those are the covered calls already
    written).  Long calls and any put positions are ignored.

    Returns an empty dict on any failure so the daily pipeline still runs.
    """
    try:
        from auth import login, logout
        import robin_stocks.robinhood as rh

        login()
        logger.info("Fetching open options positions from Robinhood...")
        positions = rh.options.get_open_option_positions() or []
        logout()

        open_calls: dict = {}
        for pos in positions:
            try:
                qty = float(pos.get("quantity", 0))
                if qty <= 0:
                    continue

                option_type = (pos.get("option_type") or "").lower()
                pos_type    = (pos.get("type")        or "").lower()
                symbol      = (pos.get("chain_symbol") or "").upper()

                # Covered call = short call
                if option_type == "call" and pos_type == "short" and symbol:
                    open_calls[symbol] = open_calls.get(symbol, 0) + int(qty)
                    logger.info(
                        f"  Open covered call: {symbol} — {int(qty)} contract(s) already written"
                    )

            except (TypeError, ValueError) as exc:
                logger.warning(f"Skipping option position record: {exc}")

        logger.info(
            f"Open covered calls: {len(open_calls)} symbol(s) with existing positions"
        )
        return open_calls

    except Exception as exc:
        logger.error(
            f"Could not fetch open options positions — skipping exclusion check: {exc}",
            exc_info=True,
        )
        return {}


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
