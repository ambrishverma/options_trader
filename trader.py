"""
trader.py — On-demand contract actions: show, buy-to-close, roll-forward, panic rolls
======================================================================================
Provides four user-facing functions:

  show_open_contracts(symbol)
      Display all open options contracts (calls AND puts, long AND short) for a
      symbol with live ITM/OTM state, current option mid price, and original
      purchase price. Requires Robinhood login.

  buy_to_close(symbol, chain_str, price, prompt)
      Place a buy-to-close limit order for an open covered call at
      mid-price (or a custom price). Requires Robinhood login.

  roll_forward(symbol, chain_str, price, prompt)
      Roll a covered call to the next available expiration as a single
      atomic multi-leg spread order. Requires Robinhood login.

  execute_panic_rolls(open_calls_detail, live_prices, name_map, dry_run)
      Called automatically by the daily pipeline. Finds DTE=0 ITM covered
      calls, cancels ALL outstanding orders (BTC-only or roll-forward spreads
      left open by a prior rescue-mode attempt), waits 30 seconds, then
      submits roll-forward spread orders. Returns a list of result dicts
      for email reporting.

Automatic pipeline sequence (daily):
  Optimize mode (any DTE ≥ 1) — rolls UP (CALL) or DOWN (PUT) when option gained >40%
                                 vs purchase price, picking the best R/R credit roll
                                 within current expiration + 10 calendar days.
  Safety mode  (DTE ≥ 1)   — places conservative BTC at low limit price (all future expiries)
  Rescue mode  (DTE 1-2)   — cancels all orders, rolls for max Risk/Reward ratio
  Panic mode   (DTE 0)     — cancels all orders (incl. stale rescue spreads),
                              rolls to next expiration regardless of credit

  execute_optimize_rolls(open_short_contracts, live_prices, name_map, dry_run)
      Called automatically by the daily pipeline FIRST. Finds any short option
      whose current mid price is ≥ 140% of the original purchase price, then
      scans strikes/expirations within a 10-day window for the best credit roll.
      Returns a list of result dicts for email reporting.

  execute_safety_btc_orders(open_calls_detail, live_prices, name_map, dry_run)
      Called automatically by the daily pipeline. Finds covered-call
      contracts expiring within the next 10 days with no open BTC order and
      places a buy-to-close limit order at MIN($0.20, 10% of purchase price,
      mid of bid/ask). A random 5–20 second delay is inserted between orders.
      Returns a list of result dicts for email reporting.

  execute_rescue_rolls(open_calls_detail, live_prices, name_map, dry_run)
      Called automatically by the daily pipeline. Finds covered-call
      contracts expiring in the next 1–2 days that are in-the-money, then
      scans all strikes >= current at the next expiration to find the one
      yielding maximum net credit (sto_mid - btc_mid). Contracts with no
      positive credit available are skipped. For contracts where a
      credit-generating roll is found: cancels ALL outstanding orders for
      that contract, waits 30 seconds if any were cancelled, then submits an
      atomic roll-forward spread at the net mid price.
      Returns a list of result dicts for email reporting.

Chain string format:
  "$STRIKE TYPE MM/DD"  e.g. "$95 CALL 5/15" or "$182.50 CALL 4/17"
  TYPE must be CALL or PUT (case-insensitive).
"""

import re
import math
import time
import logging
from datetime import date, timedelta
from typing import List, Optional, Tuple

import yfinance as yf

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        f = float(value)
        return default if math.isnan(f) else f
    except (TypeError, ValueError):
        return default


def _yahoo_symbol(symbol: str) -> str:
    return symbol.replace(".", "-")


def _get_live_price(symbol: str) -> float:
    """Fetch current stock price via yfinance. Returns 0.0 on failure."""
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        hist = ticker.history(period="2d")
        if not hist.empty:
            price = _safe_float(float(hist["Close"].iloc[-1]))
            if price > 0:
                return price
        info = ticker.fast_info
        price = _safe_float(
            getattr(info, "last_price", 0) or getattr(info, "previous_close", 0)
        )
        return price
    except Exception as e:
        logger.warning(f"{symbol}: live price fetch failed ({e})")
        return 0.0


def _parse_chain(chain_str: str) -> Tuple[float, str, str]:
    """
    Parse a chain string into (strike, option_type, expiration).

    Format: "$STRIKE TYPE MM/DD"  e.g. "$95 CALL 5/15" or "$182.50 PUT 4/17"
    Returns:
        strike:       float
        option_type:  "call" or "put"
        expiration:   "YYYY-MM-DD" (year inferred: current year unless date has passed)

    Raises ValueError on bad format.
    """
    m = re.fullmatch(
        r"\$(\d+(?:\.\d+)?)\s+(call|put)\s+(\d{1,2})/(\d{1,2})",
        chain_str.strip(),
        re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Invalid chain format: {chain_str!r}. "
            "Expected: \"$STRIKE CALL MM/DD\"  e.g. \"$95 CALL 5/15\""
        )

    strike = float(m.group(1))
    option_type = m.group(2).lower()
    month = int(m.group(3))
    day = int(m.group(4))

    today = date.today()
    year = today.year
    try:
        candidate = date(year, month, day)
    except ValueError:
        raise ValueError(f"Invalid date in chain string: {month}/{day}")

    if candidate < today:
        year += 1

    return strike, option_type, f"{year:04d}-{month:02d}-{day:02d}"


def _find_contract(
    symbol: str,
    strike: float,
    expiration: str,
    contracts: list,
) -> Optional[dict]:
    """
    Find a matching contract from the open_calls_detail snapshot list.

    Matches on: symbol (case-insensitive) + strike (within $0.01) + expiration date.
    Returns the first matching contract dict, or None.
    """
    for c in contracts:
        if c.get("symbol", "").upper() != symbol.upper():
            continue
        if abs(c.get("strike", 0) - strike) > 0.01:
            continue
        if c.get("expiration", "") != expiration:
            continue
        return c
    return None


def _fetch_contract_in_session(rh, symbol: str, strike: float,
                               option_type: str, expiration: str) -> Optional[dict]:
    """
    Live fallback: find a specific open short option contract using an already-
    authenticated robin_stocks session.  Used when the portfolio snapshot is
    stale or missing (e.g. puts snapshot not yet generated after a code update).

    Returns a minimal contract dict compatible with _find_contract output,
    or None if the contract is not found.
    """
    try:
        positions = rh.options.get_open_option_positions() or []
        # Detect existing BTC orders for the btc_order_exists flag
        try:
            open_orders = rh.orders.get_all_open_option_orders() or []
            btc_ids: set = set()
            for order in open_orders:
                for leg in order.get("legs", []):
                    if (leg.get("side", "").lower() == "buy"
                            and leg.get("position_effect", "").lower() == "close"):
                        opt_url = leg.get("option", "")
                        if opt_url:
                            btc_ids.add(opt_url.rstrip("/").split("/")[-1])
        except Exception:
            btc_ids = set()

        for pos in positions:
            sym = (pos.get("chain_symbol") or "").upper()
            if sym != symbol.upper():
                continue
            qty = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            if (pos.get("type") or "").lower() != "short":
                continue

            oid = pos.get("option_id", "")
            try:
                inst = rh.options.get_option_instrument_data_by_id(oid)
                inst_type   = (inst.get("type") or "").lower()
                inst_strike = float(inst.get("strike_price", 0) or 0)
                inst_exp    = inst.get("expiration_date", "")
            except Exception:
                continue

            if (abs(inst_strike - strike) < 0.01
                    and inst_exp == expiration
                    and inst_type == option_type.lower()):
                return {
                    "symbol":           sym,
                    "opt_type":         inst_type,
                    "strike":           inst_strike,
                    "expiration":       inst_exp,
                    "quantity":         int(qty),
                    "btc_order_exists": oid in btc_ids,
                    "option_id":        oid,
                    "purchase_price":   float(pos.get("average_price", 0) or 0),
                }
    except Exception as exc:
        logger.warning(f"Live contract lookup failed for {symbol}: {exc}")
    return None


def _get_option_bid_ask(
    symbol: str,
    strike: float,
    option_type: str,
    expiration: str,
) -> Tuple[float, float, float]:
    """
    Fetch bid, ask, mid for a specific option via yfinance.
    Returns (bid, ask, mid). Returns (0.0, 0.0, 0.0) on failure.
    """
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        chain = ticker.option_chain(expiration)
        df = chain.calls if option_type == "call" else chain.puts
        row = df[abs(df["strike"].astype(float) - strike) < 0.01]
        if row.empty:
            return 0.0, 0.0, 0.0
        r = row.iloc[0]
        bid = _safe_float(r.get("bid", 0))
        ask = _safe_float(r.get("ask", 0))
        mid = round((bid + ask) / 2, 2)
        return bid, ask, mid
    except Exception as e:
        logger.warning(
            f"{symbol} ${strike:g} {option_type} {expiration}: bid/ask fetch failed ({e})"
        )
        return 0.0, 0.0, 0.0


def _dte(expiration: str) -> int:
    """Calendar days from today to expiration date."""
    try:
        return (date.fromisoformat(expiration) - date.today()).days
    except (ValueError, TypeError):
        return -1


def _fmt_exp(expiration: str) -> str:
    """Format 'YYYY-MM-DD' as 'M/DD' for compact display."""
    try:
        d = date.fromisoformat(expiration)
        return f"{d.month}/{d.day:02d}"
    except (ValueError, TypeError):
        return expiration


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def show_open_contracts(symbol: str) -> None:
    """
    Display all open options contracts (calls AND puts, long AND short) for a symbol.

    Logs into Robinhood to fetch live positions, fetches current stock price and
    option mid prices via yfinance, and prints a formatted table.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    symbol = symbol.upper()

    login()
    try:
        positions = rh.options.get_open_option_positions() or []

        # Detect existing open orders per option_id (for [BTC]/[STO] annotation)
        open_order_ids: dict = {}  # option_id -> list of sides ("buy"/"sell")
        try:
            open_orders = rh.orders.get_all_open_option_orders() or []
            for order in open_orders:
                for leg in order.get("legs", []):
                    opt_url = leg.get("option", "")
                    if opt_url:
                        oid = opt_url.rstrip("/").split("/")[-1]
                        side = (leg.get("side") or "").lower()
                        open_order_ids.setdefault(oid, []).append(side)
        except Exception:
            pass

        contracts = []
        for pos in positions:
            try:
                chain_sym = (pos.get("chain_symbol") or "").upper()
                if chain_sym != symbol:
                    continue
                qty      = float(pos.get("quantity", 0))
                pos_type = (pos.get("type") or "").lower()  # "short" or "long"
                if qty <= 0:
                    continue

                option_id   = pos.get("option_id", "")
                option_type = ""
                strike      = 0.0
                expiration  = pos.get("expiration_date", "")

                if option_id:
                    try:
                        instr       = rh.options.get_option_instrument_data_by_id(option_id)
                        option_type = (instr.get("type") or "").lower()
                        strike      = float(instr.get("strike_price", 0) or 0)
                        expiration  = instr.get("expiration_date", expiration) or expiration
                    except Exception:
                        pass

                purchase_price = float(pos.get("average_price", 0) or 0)
                contracts.append({
                    "symbol":        symbol,
                    "option_type":   option_type,
                    "pos_type":      pos_type,
                    "strike":        strike,
                    "expiration":    expiration,
                    "quantity":      int(qty),
                    "option_id":     option_id,
                    "purchase_price": purchase_price,
                    "open_order_sides": open_order_ids.get(option_id, []),
                })
            except Exception:
                continue
    finally:
        logout()

    if not contracts:
        print(f"\nNo open options contracts found for {symbol}.\n")
        return

    # Drop expired
    contracts = [c for c in contracts if _dte(c.get("expiration", "")) >= 0]
    if not contracts:
        print(f"\nNo active (non-expired) options contracts found for {symbol}.\n")
        return

    live_price = _get_live_price(symbol)
    price_str  = f"${live_price:.2f}" if live_price > 0 else "N/A"

    print(f"\nOpen options contracts for {symbol}  (stock: {price_str})")
    print("─" * 98)
    print(f"  {'Chain':<26}  {'Side':<5}  {'Expiry':<12}  {'DTE':>4}  {'Status':<17}  "
          f"{'Strike':>8}  {'Opt Mid':>8}  {'Paid':>8}")
    print("─" * 98)

    for c in sorted(contracts, key=lambda x: (x.get("expiration", ""), x.get("option_type", ""), x.get("strike", 0))):
        strike      = float(c["strike"])
        expiry      = c["expiration"]
        qty         = c["quantity"]
        dte         = _dte(expiry)
        opt_type    = c["option_type"].upper() if c["option_type"] else "?"
        pos_side    = "Short" if c["pos_type"] == "short" else "Long"
        label       = f"${strike:g} {opt_type} {_fmt_exp(expiry)}"
        if qty > 1:
            label += f" ×{qty}"
        # Annotate open orders on this contract
        sides = c["open_order_sides"]
        if sides:
            tags = "+".join(sorted(set(s.upper() for s in sides)))
            label += f" [{tags}]"

        _, _, mid = _get_option_bid_ask(symbol, strike, c["option_type"] or "call", expiry)
        paid = abs(_safe_float(c.get("purchase_price", 0)))

        if live_price > 0 and strike > 0:
            diff = live_price - strike
            status = f"ITM (+${diff:.2f})" if diff >= 0 else f"OTM (-${abs(diff):.2f})"
        else:
            status = "N/A"

        mid_str  = f"${mid:.2f}" if mid > 0 else "N/A"
        paid_str = f"${paid:.2f}"
        print(f"  {label:<26}  {pos_side:<5}  {expiry:<12}  {dte:>4}  {status:<17}  "
              f"${strike:>7g}  {mid_str:>8}  {paid_str:>8}")

    print("─" * 98)
    print("  [BUY]/[SELL] = open order already on Robinhood for this contract\n")


def buy_to_close(
    symbol: str,
    chain_str: str,
    price: Optional[float] = None,
    prompt: bool = False,
) -> bool:
    """
    Place a limit buy-to-close order for an open covered-call contract.

    Args:
        symbol:     Stock ticker (e.g. "TSLA")
        chain_str:  Contract identifier: "$STRIKE TYPE MM/DD" (e.g. "$95 CALL 5/15")
        price:      Limit price per share. Default: (bid+ask)/2 rounded to $0.01.
        prompt:     If True, display order summary and require y/n confirmation.

    Returns True if the order was submitted, False if aborted or failed.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout
    from portfolio import load_open_calls_detail_snapshot, load_open_puts_detail_snapshot

    symbol = symbol.upper()

    # 1. Parse chain string
    try:
        strike, option_type, expiration = _parse_chain(chain_str)
    except ValueError as e:
        print(f"\n❌  {e}\n")
        return False

    # 2. Find matching open contract — try snapshots first, live fallback after login
    contracts = load_open_calls_detail_snapshot() + load_open_puts_detail_snapshot()
    contract = _find_contract(symbol, strike, expiration, contracts)

    def _btc_prompt(qty: int, lp: float, b: float, a: float) -> bool:
        """Print BTC order summary and return True if user confirms (or prompt=False)."""
        if not prompt:
            return True
        est = round(lp * 100 * qty, 2)
        print(f"\n{'─' * 62}")
        print("  BUY-TO-CLOSE ORDER SUMMARY")
        print(f"{'─' * 62}")
        print(f"  Symbol     : {symbol}")
        print(f"  Contract   : {chain_str}")
        print(f"  Quantity   : {qty} contract(s)")
        print(f"  Bid / Ask  : ${b:.2f} / ${a:.2f}")
        print(f"  Limit      : ${lp:.2f} per share  (${est:.2f} total est.)")
        print(f"{'─' * 62}")
        ans = input("  Proceed? [y/N]: ").strip().lower()
        if ans != "y":
            print("  Aborted.\n")
            return False
        return True

    # ── SNAPSHOT PATH ──────────────────────────────────────────────────────────
    if contract is not None:
        # 3. Guard: BTC order already exists
        if contract.get("btc_order_exists"):
            print(f"\n⚠️   A buy-to-close order already exists for {symbol} {chain_str}.")
            print("     Check Robinhood to avoid placing a duplicate order.\n")
            return False
        quantity = int(contract.get("quantity", 1))

        # 4. Fetch live bid/ask
        bid, ask, mid = _get_option_bid_ask(symbol, strike, option_type, expiration)
        if bid == 0 and ask == 0 and price is None:
            print(f"\n⚠️   Could not fetch live bid/ask for {symbol} {chain_str}.")
            print("     Specify --price to override and proceed.\n")
            return False

        # 5. Determine limit price
        limit_price = round(price, 2) if price is not None else mid
        if limit_price <= 0:
            print(f"\n❌  Limit price is $0.00 — cannot place order. Use --price to override.\n")
            return False

        # 6. Optional pre-login confirmation prompt
        if not _btc_prompt(quantity, limit_price, bid, ask):
            return False

        # 7. Login and place order
        print("\nLogging in to Robinhood...")
        try:
            login(force_fresh=False)
        except Exception as e:
            print(f"❌  Login failed: {e}\n")
            return False

    # ── LIVE-LOOKUP PATH (no snapshot file yet) ────────────────────────────────
    else:
        # 3. Login first — we need an authenticated session for the live lookup
        print("\nLogging in to Robinhood...")
        try:
            login(force_fresh=False)
        except Exception as e:
            print(f"❌  Login failed: {e}\n")
            return False

        # Inside the try/finally block below we'll finish the setup

    try:
        # 7a / 3b. Live fallback: snapshot was missing (e.g. first run after puts-tracking)
        if contract is None:
            print(f"  Snapshot not found — looking up {symbol} contract live on Robinhood...")
            contract = _fetch_contract_in_session(rh, symbol, strike, option_type, expiration)
            if contract is None:
                print(f"\n❌  No open contract found for {symbol} {chain_str!r}.")
                print(f"     Run  --show {symbol}  to see open contracts.\n")
                return False
            if contract.get("btc_order_exists"):
                print(f"\n⚠️   A buy-to-close order already exists for {symbol} {chain_str}.")
                print("     Check Robinhood to avoid placing a duplicate order.\n")
                return False
            quantity = int(contract.get("quantity", 1))

            # Fetch bid/ask and compute limit price (deferred to post-login)
            bid, ask, mid = _get_option_bid_ask(symbol, strike, option_type, expiration)
            if bid == 0 and ask == 0 and price is None:
                print(f"\n⚠️   Could not fetch live bid/ask for {symbol} {chain_str}.")
                print("     Specify --price to override and proceed.\n")
                return False
            limit_price = round(price, 2) if price is not None else mid
            if limit_price <= 0:
                print(f"\n❌  Limit price is $0.00 — cannot place order. Use --price to override.\n")
                return False

            # Deferred --prompt: show now that we have quantity and pricing
            if not _btc_prompt(quantity, limit_price, bid, ask):
                return False

        result = rh.orders.order_buy_option_limit(
            positionEffect="close",
            creditOrDebit="debit",
            price=limit_price,
            symbol=symbol,
            quantity=quantity,
            expirationDate=expiration,
            strike=strike,
            optionType=option_type,
            timeInForce="gtc",
        )
        order_id = (result or {}).get("id", "")
        state    = (result or {}).get("state", "unknown")
        if result and order_id:
            print(f"✅  BTC order placed — id: {order_id}  state: {state}\n")
            return True
        else:
            print(f"❌  Order failed — unexpected response: {result}\n")
            return False
    except Exception as e:
        print(f"❌  Order failed: {e}\n")
        return False
    finally:
        try:
            logout()
        except Exception:
            pass


def roll_forward(
    symbol: str,
    chain_str: str,
    price: Optional[float] = None,
    prompt: bool = False,
    rescue: bool = False,
) -> bool:
    """
    Roll a covered call to the next available expiration as a single
    multi-leg spread order via order_option_spread.

    Submitting both legs atomically lets Robinhood treat the existing
    equity as collateral for the new leg, avoiding the 'infinite risk'
    rejection that occurs when BTC and STO are placed as two separate orders.

    --price X   Net credit (positive) or debit (negative) for the entire
                spread. Default: sto_mid − btc_mid from live market.

    rescue=True  Scans all strikes >= current at the next expiration and
                picks the one generating the highest net credit
                (sto_mid - btc_mid). Cancels ALL open orders for this
                contract before rolling. Only proceeds if net credit > 0.

    Returns True if the order was submitted, False if aborted or failed.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout
    from portfolio import load_open_calls_detail_snapshot, load_open_puts_detail_snapshot

    symbol = symbol.upper()

    # 1. Parse chain string
    try:
        strike, option_type, expiration = _parse_chain(chain_str)
    except ValueError as e:
        print(f"\n❌  {e}\n")
        return False

    # 2. Find matching open contract — try snapshots first, live fallback after login
    contracts = load_open_calls_detail_snapshot() + load_open_puts_detail_snapshot()
    contract = _find_contract(symbol, strike, expiration, contracts)

    # ── Helper: resolve all option-chain data (steps 3-6) ─────────────────────
    # Used by both the snapshot path (pre-login) and live path (post-login).
    # Returns a dict with all computed values, or None on failure (error already printed).
    def _resolve_chain(qty: int):
        # 3. Live price + current contract bid/ask
        lp = _get_live_price(symbol)
        bb, ba, bm = _get_option_bid_ask(symbol, strike, option_type, expiration)
        if bb == 0 and ba == 0 and price is None:
            print(f"\n⚠️   Could not fetch live bid/ask for {symbol} {chain_str}.")
            print("     Specify --price to override and proceed.\n")
            return None
        # 4. Find next available expiration
        try:
            tkr = yf.Ticker(_yahoo_symbol(symbol))
            all_exps = list(tkr.options)
        except Exception as exc:
            print(f"\n❌  Could not fetch option expirations for {symbol}: {exc}\n")
            return None
        fut = [e for e in all_exps if e > expiration]
        if not fut:
            print(f"\n❌  No future expirations available for {symbol} beyond {expiration}.\n")
            return None
        nxt_exp = fut[0]
        # 5. Find target strike at next expiration
        ts: Optional[float] = None
        sb, sa, sm = 0.0, 0.0, 0.0
        try:
            ch = tkr.option_chain(nxt_exp)
            df = (ch.calls if option_type == "call" else ch.puts).copy()
            df["strike"] = df["strike"].astype(float)
            df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))
            df["ask"]    = df["ask"].apply(lambda x: _safe_float(x))
            if rescue:
                df["mid"] = (df["bid"] + df["ask"]) / 2.0
                cands = df[(df["strike"] >= strike - 0.01) & (df["bid"] > 0)].copy()
                if cands.empty:
                    print(f"\n❌  No strikes >= ${strike:g} with a non-zero bid at {nxt_exp}.\n")
                    return None
                cands["net_credit"] = cands["mid"] - bm
                cp = cands[cands["net_credit"] > 0]
                if cp.empty:
                    best_nc = cands.loc[cands["net_credit"].idxmax()]
                    print(
                        f"\n⚠️   No credit available for {symbol} rescue roll — "
                        f"best strike ${float(best_nc['strike']):g} yields "
                        f"${float(best_nc['net_credit']):.2f} net. No order placed.\n"
                    )
                    return None
                _EPS = 0.001
                cp = cp.copy()
                cp["risk"]     = (cp["strike"] - lp).clip(lower=_EPS)
                cp["rr_ratio"] = cp["net_credit"] / cp["risk"]
                ts = float(cp.loc[cp["rr_ratio"].idxmax()]["strike"])
            else:
                exact = df[abs(df["strike"] - strike) < 0.01]
                if not exact.empty and float(exact.iloc[0]["bid"]) > 0:
                    ts = float(exact.iloc[0]["strike"])
                else:
                    ref = lp if lp > 0 else strike
                    otm = df[df["strike"] >= ref].sort_values("strike")
                    otm_v = otm[otm["bid"] > 0]
                    if otm_v.empty:
                        print(f"\n❌  No OTM {option_type} options with a non-zero bid "
                              f"found at expiration {nxt_exp}.\n")
                        return None
                    ts = float(otm_v.iloc[0]["strike"])
            sb, sa, sm = _get_option_bid_ask(symbol, ts, option_type, nxt_exp)
        except Exception as exc:
            print(f"\n❌  Failed to find target strike at {nxt_exp}: {exc}\n")
            return None
        # 6. Compute net spread price
        np_ = round(price, 2) if price is not None else round(sm - bm, 2)
        dir_ = "credit" if np_ >= 0 else "debit"
        abs_ = max(round(abs(np_), 2), 0.01)
        lbl  = f"${abs_:.2f} {dir_}"
        sc   = abs(ts - strike) > 0.01
        return dict(
            live_price=lp, btc_bid=bb, btc_ask=ba, btc_mid=bm,
            next_expiration=nxt_exp, target_strike=ts,
            sto_bid=sb, sto_ask=sa, sto_mid=sm,
            net_price=np_, direction=dir_, abs_net=abs_,
            net_label=lbl, strike_changed=sc, quantity=qty,
        )

    def _show_roll_prompt(r: dict) -> bool:
        """Print roll summary and return True if user confirms (or prompt=False)."""
        if not prompt:
            sc_note = f" (strike → ${r['target_strike']:g})" if r['strike_changed'] else ""
            print(f"\nRolling {symbol} ${strike:g} {option_type.upper()} "
                  f"{_fmt_exp(expiration)} → "
                  f"${r['target_strike']:g} {option_type.upper()} "
                  f"{_fmt_exp(r['next_expiration'])}{sc_note}")
            print(f"  BTC: ~${r['btc_mid']:.2f}  |  STO: ~${r['sto_mid']:.2f}"
                  f"  |  Net: {r['net_label']}\n")
            return True
        print(f"\n{'─' * 65}")
        print("  ROLL FORWARD ORDER SUMMARY  (single multi-leg spread order)")
        print(f"{'─' * 65}")
        print(f"  Symbol       : {symbol}  (live: ${r['live_price']:.2f})")
        print(f"  Leg 1 — BTC  : ${strike:g} {option_type.upper()} "
              f"{_fmt_exp(expiration)}  "
              f"bid ${r['btc_bid']:.2f} / ask ${r['btc_ask']:.2f}  →  pay ~${r['btc_mid']:.2f}")
        print(f"  Leg 2 — STO  : ${r['target_strike']:g} {option_type.upper()} "
              f"{_fmt_exp(r['next_expiration'])}  "
              f"bid ${r['sto_bid']:.2f} / ask ${r['sto_ask']:.2f}  →  collect ~${r['sto_mid']:.2f}")
        print(f"  Net spread   : {r['net_label']}  (limit for the combined order)")
        print(f"  Quantity     : {r['quantity']} contract(s)")
        if r['strike_changed']:
            print(f"  ⚠️  Strike changed: ${strike:g} → ${r['target_strike']:g} "
                  "(exact strike unavailable at next expiry)")
        print(f"{'─' * 65}")
        ans = input("  Proceed? [y/N]: ").strip().lower()
        if ans != "y":
            print("  Aborted.\n")
            return False
        return True

    # ── SNAPSHOT PATH ──────────────────────────────────────────────────────────
    if contract is not None:
        if contract.get("btc_order_exists") and not rescue:
            print(f"\n⚠️   A buy-to-close order already exists for {symbol} {chain_str}.")
            print("     Rolling would result in a duplicate BTC order.\n")
            print("     Use --rescue to cancel the existing order and roll for maximum credit.\n")
            return False
        quantity = int(contract.get("quantity", 1))

        r = _resolve_chain(quantity)
        if r is None:
            return False

        if not _show_roll_prompt(r):
            return False

        # Login
        print("Logging in to Robinhood...")
        try:
            login(force_fresh=False)
        except Exception as e:
            print(f"❌  Login failed: {e}\n")
            return False

    # ── LIVE-LOOKUP PATH (no snapshot file yet) ────────────────────────────────
    else:
        print("Logging in to Robinhood...")
        try:
            login(force_fresh=False)
        except Exception as e:
            print(f"❌  Login failed: {e}\n")
            return False
        # r and contract will be resolved inside the try block below

    try:
        # Live fallback: snapshot was missing (e.g. first run after puts-tracking was added)
        if contract is None:
            print(f"  Snapshot not found — looking up {symbol} contract live on Robinhood...")
            contract = _fetch_contract_in_session(rh, symbol, strike, option_type, expiration)
            if contract is None:
                print(f"\n❌  No open contract found for {symbol} {chain_str!r}.")
                print(f"     Run  --show {symbol}  to see open contracts.\n")
                return False
            if contract.get("btc_order_exists") and not rescue:
                print(f"\n⚠️   A buy-to-close order already exists for {symbol} {chain_str}.")
                print("     Rolling would result in a duplicate BTC order.\n")
                print("     Use --rescue to cancel the existing order and roll for maximum credit.\n")
                return False
            quantity = int(contract.get("quantity", 1))
            r = _resolve_chain(quantity)
            if r is None:
                return False
            if not _show_roll_prompt(r):
                return False

        # Unpack resolved chain data
        target_strike  = r["target_strike"]
        next_expiration = r["next_expiration"]
        abs_net        = r["abs_net"]
        direction      = r["direction"]
        quantity       = r["quantity"]

        # Rescue mode: cancel ALL open orders for this contract before rolling
        if rescue:
            option_id = contract.get("option_id", "")
            if option_id:
                try:
                    open_orders = rh.orders.get_all_open_option_orders() or []
                except Exception as fetch_err:
                    logger.warning(f"[RESCUE] Could not fetch open orders: {fetch_err}")
                    open_orders = []
                cancelled_count = 0
                for order in open_orders:
                    order_matched = False
                    for leg in order.get("legs", []):
                        leg_oid = (leg.get("option", "").rstrip("/").split("/"))[-1]
                        if leg_oid == option_id:
                            order_matched = True
                            break
                    if order_matched:
                        try:
                            rh.orders.cancel_option_order(order["id"])
                            cancelled_count += 1
                            print(f"  Cancelled order {order['id']} for {symbol} ${strike:g}")
                        except Exception as cancel_err:
                            logger.warning(f"[RESCUE] Failed to cancel order {order['id']}: {cancel_err}")
                if cancelled_count > 0:
                    print(f"  Waiting 30s for {cancelled_count} cancellation(s) to settle...")
                    time.sleep(30)

        spread_legs = [
            {
                "expirationDate": expiration,
                "strike":         f"{strike:.4f}",
                "optionType":     option_type,
                "effect":         "close",
                "action":         "buy",
                "ratio_quantity": 1,
            },
            {
                "expirationDate": next_expiration,
                "strike":         f"{target_strike:.4f}",
                "optionType":     option_type,
                "effect":         "open",
                "action":         "sell",
                "ratio_quantity": 1,
            },
        ]
        result = rh.orders.order_option_spread(
            direction=direction,
            price=abs_net,
            symbol=symbol,
            quantity=quantity,
            spread=spread_legs,
            timeInForce="gtc",
        )
        order_id = (result or {}).get("id", "")
        state    = (result or {}).get("state", "unknown")
        if result and order_id:
            print(f"✅  Roll order placed — id: {order_id}  state: {state}\n")
            return True
        else:
            detail  = (result or {}).get("detail", "") or str((result or {}).get("non_field_errors", ""))
            err_msg = detail or f"response: {result}"
            logger.error(f"roll_forward spread order failed for {symbol}: {err_msg}")
            print(f"❌  Roll order failed — {err_msg}\n")
            return False
    except Exception as e:
        logger.error(f"roll_forward spread order exception for {symbol}: {e}", exc_info=True)
        print(f"❌  Roll order failed: {e}\n")
        return False
    finally:
        try:
            logout()
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Optimize-mode roll execution (called automatically by the daily pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def execute_optimize_rolls(
    open_short_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Optimize mode: raise the ceiling (CALL → roll UP to higher strike) or lower
    the floor (PUT → roll DOWN to lower strike), or roll OUT to a later expiry,
    when a contract's current option mid price has gained more than 40% relative
    to the original purchase price.

    Trigger (per contract):
      current_mid >= 1.40 × (abs(purchase_price) / 100)
      where purchase_price is the raw Robinhood average_price (negative for
      shorts = credit received; positive for longs = debit paid).
      Contracts with no purchase price (zero) are skipped.

    Roll target selection:
      - Scan expirations: current expiration through (current expiration + 10 days)
      - Candidate strikes:
          CALL: strike >= current strike (same or higher — raise ceiling)
          PUT:  strike <= current strike (same or lower  — lower floor)
      - Compute net credit = STO_mid − BTC_mid  for each (expiration, strike)
      - Keep only credit-positive candidates (net_credit > 0)
      - Pick the candidate with the highest Risk/Reward ratio:
            Reward = net credit
            Risk   = |new_strike − live_price|  (floored at ε to avoid ÷0)
            R/R    = Reward / Risk
      - If no credit-positive candidate exists across all scanned expirations:
        record skipped=True and move to the next contract.

    Execution (when a target is found):
      1. Cancel ALL outstanding orders for the contract
      2. Wait 20 seconds (only if any orders were cancelled)
      3. Submit an atomic roll-forward spread at the net mid price (GTC limit)

    Input contracts must carry an ``opt_type`` field ("call" or "put");
    defaults to "call" if omitted (backward-compatible with covered-call pipeline).

    Returns a list of result dicts — one per triggered contract (including
    skipped ones) — consumed by the emailer to populate the optimize sub-section.

    Each result dict contains:
      symbol, name, opt_type, strike, expiration, dte, next_expiration,
      target_strike, next_dte, quantity, live_price, purchase_price_per_share,
      current_mid, gain_pct, orders_cancelled, success, skipped,
      order_id, net_price, direction, net_label, error
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    if name_map is None:
        name_map = {}

    today = date.today()
    today_str = str(today)

    # ── Step 1: Identify triggered contracts (>40% gain) ─────────────────────
    triggered: list = []
    for c in open_short_contracts:
        exp = c.get("expiration", "")
        if not exp or exp <= today_str:
            continue   # skip DTE-0 (panic mode) and expired
        try:
            dte = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if dte < 1:
            continue

        sym           = c.get("symbol", "").upper()
        strike        = float(c.get("strike", 0))
        purchase_price = _safe_float(c.get("purchase_price", 0))
        abs_pp        = abs(purchase_price)
        if abs_pp == 0:
            continue   # no purchase price — cannot compute gain

        per_share_premium = abs_pp / 100.0   # Robinhood stores as per-contract total ×100

        # Fetch current mid for gain check
        opt_type = c.get("opt_type", "call")
        lp       = live_prices.get(sym, 0.0)
        bid, ask, mid = _get_option_bid_ask(sym, strike, opt_type, exp)

        if mid <= 0:
            continue   # no market data — skip

        if mid < per_share_premium * 1.40:
            continue   # not triggered (gain < 40%)

        gain_pct = round((mid / per_share_premium - 1) * 100, 1)
        triggered.append((c, dte, per_share_premium, mid, gain_pct))

    if not triggered:
        return []

    logger.info(
        f"[OPTIMIZE MODE] {len(triggered)} contract(s) triggered (>40% gain): "
        + ", ".join(
            f"{c['symbol']} ${c.get('strike'):g} {c.get('opt_type','call').upper()} "
            f"+{gain_pct:.0f}%"
            for c, _, _, _, gain_pct in triggered
        )
    )

    def _make_result(c: dict, dte: int, lp: float,
                     per_share_premium: float, current_mid: float,
                     gain_pct: float) -> dict:
        sym      = c["symbol"].upper()
        strike   = float(c["strike"])
        opt_type = c.get("opt_type", "call")
        return {
            "symbol":                sym,
            "name":                  name_map.get(sym, sym),
            "opt_type":              opt_type,
            "strike":                strike,
            "expiration":            c["expiration"],
            "dte":                   dte,
            "next_expiration":       "",
            "target_strike":         0.0,
            "next_dte":              0,
            "quantity":              int(c.get("quantity", 1)),
            "live_price":            lp,
            "purchase_price_per_share": per_share_premium,
            "current_mid":           current_mid,
            "gain_pct":              gain_pct,
            "orders_cancelled":      0,
            "success":               False,
            "skipped":               False,
            "order_id":              "",
            "net_price":             0.0,
            "direction":             "",
            "net_label":             "",
            "error":                 "",
        }

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        results = []
        for c, dte, per_share_premium, mid, gain_pct in triggered:
            sym = c["symbol"].upper()
            lp  = live_prices.get(sym, 0.0)
            r   = _make_result(c, dte, lp, per_share_premium, mid, gain_pct)
            r["error"] = "[DRY RUN] no orders placed"
            results.append(r)
            logger.info(
                f"[OPTIMIZE MODE][DRY RUN] would optimize-roll "
                f"{sym} ${c['strike']:g} +{gain_pct:.0f}%"
            )
        return results

    # ── Live run: login once, process all, logout ─────────────────────────────
    results = []
    try:
        login(force_fresh=False)
    except Exception as e:
        logger.error(f"[OPTIMIZE MODE] Login failed: {e}")
        for c, dte, per_share_premium, mid, gain_pct in triggered:
            sym = c["symbol"].upper()
            lp  = live_prices.get(sym, 0.0)
            r   = _make_result(c, dte, lp, per_share_premium, mid, gain_pct)
            r["error"] = f"Login failed: {e}"
            results.append(r)
        return results

    try:
        try:
            open_orders = rh.orders.get_all_open_option_orders() or []
        except Exception as fetch_err:
            logger.warning(f"[OPTIMIZE MODE] Could not fetch open orders: {fetch_err}")
            open_orders = []

        for c, dte, per_share_premium, current_mid, gain_pct in triggered:
            sym        = c["symbol"].upper()
            strike     = float(c["strike"])
            expiration = c["expiration"]
            option_id  = c.get("option_id", "")
            opt_type   = c.get("opt_type", "call")
            live_price = _get_live_price(sym) or live_prices.get(sym, 0.0)
            r = _make_result(c, dte, live_price, per_share_premium, current_mid, gain_pct)

            # ── Step 1: Scan expirations within window ────────────────────────
            # Range: current expiration ... (current expiration + 10 days)
            # This lets the scanner check the same expiration (roll UP/DOWN in
            # strike) and any expirations up to 10 days later (roll OUT).
            exp_date     = date.fromisoformat(expiration)
            max_exp_date = (exp_date + timedelta(days=10)).isoformat()

            try:
                ticker = yf.Ticker(_yahoo_symbol(sym))
                all_expirations = list(ticker.options)
            except Exception as e:
                r["error"] = f"Could not fetch expirations: {e}"
                logger.error(f"[OPTIMIZE MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            # Include current expiration and any future ones up to +10 days
            window_exps = [
                e for e in all_expirations
                if expiration <= e <= max_exp_date
            ]
            if not window_exps:
                r["skipped"] = True
                r["error"]   = (
                    f"No expirations in window [{expiration}, {max_exp_date}]"
                )
                logger.info(f"[OPTIMIZE MODE] {sym}: skipped — {r['error']}")
                results.append(r)
                continue

            # ── Step 2: Collect ALL credit-positive R/R candidates across window ─
            # Reward = net_credit = STO_mid − BTC_mid
            # Risk   = |new_strike − live_price| (floored at ε)
            # R/R    = Reward / Risk
            # We collect ALL candidates (not just the best) so we can fall back
            # to lower-ranked ones if a candidate's option URL is not resolvable
            # on Robinhood's instruments API (yfinance/RH chain data can diverge).
            btc_mid = _get_option_bid_ask(sym, strike, opt_type, expiration)[2]

            _EPS = 0.001
            # all_candidates: list of (rr_ratio, exp, strike, net_credit, sto_mid_yf)
            all_candidates: list = []

            for scan_exp in window_exps:
                try:
                    chain_data = ticker.option_chain(scan_exp)
                    df = (chain_data.puts if opt_type == "put" else chain_data.calls).copy()
                    df["strike"] = df["strike"].astype(float)
                    df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))
                    df["ask"]    = df["ask"].apply(lambda x: _safe_float(x))
                    df["mid"]    = (df["bid"] + df["ask"]) / 2.0

                    # Filter by strike direction
                    if opt_type == "put":
                        # Roll to same or lower strike (lower floor)
                        cands = df[
                            (df["strike"] <= strike + 0.01) & (df["bid"] > 0)
                        ].copy()
                    else:
                        # Roll to same or higher strike (raise ceiling)
                        cands = df[
                            (df["strike"] >= strike - 0.01) & (df["bid"] > 0)
                        ].copy()

                    if cands.empty:
                        continue

                    cands["net_credit"] = cands["mid"] - btc_mid
                    credit_pos = cands[cands["net_credit"] > 0].copy()
                    if credit_pos.empty:
                        continue

                    # R/R: Reward / Risk
                    if opt_type == "put":
                        credit_pos["risk"] = (
                            live_price - credit_pos["strike"]
                        ).clip(lower=_EPS)
                    else:
                        credit_pos["risk"] = (
                            credit_pos["strike"] - live_price
                        ).clip(lower=_EPS)
                    credit_pos["rr_ratio"] = credit_pos["net_credit"] / credit_pos["risk"]

                    for _, row in credit_pos.iterrows():
                        all_candidates.append((
                            float(row["rr_ratio"]),
                            scan_exp,
                            float(row["strike"]),
                            round(float(row["net_credit"]), 2),
                            round(float(row["mid"]), 2),
                        ))
                except Exception as scan_err:
                    logger.debug(
                        f"[OPTIMIZE MODE] {sym} scan_exp={scan_exp} failed: {scan_err}"
                    )
                    continue

            if not all_candidates:
                r["skipped"] = True
                r["error"]   = (
                    f"No credit-positive strike found in window "
                    f"[{expiration}, {max_exp_date}] for "
                    f"{'put strike <= ' if opt_type == 'put' else 'call strike >= '}"
                    f"${strike:g}"
                )
                logger.info(f"[OPTIMIZE MODE] {sym}: skipped — {r['error']}")
                results.append(r)
                continue

            # Sort globally by R/R descending (best first)
            all_candidates.sort(key=lambda x: x[0], reverse=True)

            # ── Step 2b: Pick first candidate resolvable on Robinhood ─────────
            # yfinance and Robinhood option chains can diverge: a strike may appear
            # in yfinance but not in Robinhood's instruments API (e.g. non-standard
            # strikes, recently de-listed options, precision mismatches).
            # order_option_spread internally calls id_for_option; if that returns
            # None the submitted URL is invalid → "Invalid hyperlink" API error.
            # We pre-validate here and skip any candidate Robinhood can't find.
            best_exp = best_strike = best_net = best_sto_mid = None
            for _rr, cand_exp, cand_strike, cand_net, cand_sto_mid in all_candidates:
                try:
                    opt_id = rh.options.id_for_option(
                        sym, cand_exp, str(cand_strike), opt_type
                    )
                except Exception:
                    opt_id = None
                if opt_id:
                    best_exp     = cand_exp
                    best_strike  = cand_strike
                    best_net     = cand_net
                    best_sto_mid = cand_sto_mid
                    logger.debug(
                        f"[OPTIMIZE MODE] {sym}: STO option confirmed on Robinhood — "
                        f"${cand_strike:g} exp={cand_exp} id={opt_id} R/R={_rr:.2f}"
                    )
                    break
                logger.debug(
                    f"[OPTIMIZE MODE] {sym}: ${cand_strike:g} exp={cand_exp} "
                    f"not found on Robinhood instruments API — trying next candidate"
                )

            if best_exp is None:
                r["skipped"] = True
                r["error"]   = (
                    f"No STO option URL resolvable on Robinhood in window "
                    f"[{expiration}, {max_exp_date}] — "
                    f"tried {len(all_candidates)} candidate(s). "
                    f"Try --pull-portfolio to refresh the snapshot."
                )
                logger.warning(f"[OPTIMIZE MODE] {sym}: skipped — {r['error']}")
                results.append(r)
                continue

            # Confirm with a live bid/ask fetch for accurate mid
            _, _, sto_mid_live = _get_option_bid_ask(sym, best_strike, opt_type, best_exp)
            sto_mid    = sto_mid_live if sto_mid_live > 0 else best_sto_mid
            net_credit = round(sto_mid - btc_mid, 2)

            if net_credit <= 0:
                r["skipped"] = True
                r["error"]   = (
                    f"Live check: no credit — ${best_strike:g} "
                    f"mid ${sto_mid:.2f} − BTC ${btc_mid:.2f} = ${net_credit:.2f}"
                )
                logger.info(f"[OPTIMIZE MODE] {sym}: skipped — {r['error']}")
                results.append(r)
                continue

            next_dte = (date.fromisoformat(best_exp) - today).days

            r["next_expiration"] = best_exp
            r["target_strike"]   = best_strike
            r["next_dte"]        = next_dte
            r["net_price"]       = net_credit
            r["direction"]       = "credit"
            r["net_label"]       = f"${net_credit:.2f} credit"

            # ── Step 3: Cancel ALL outstanding orders for this contract ────────
            cancelled_count = 0
            if option_id:
                for order in open_orders:
                    order_matched = False
                    for leg in order.get("legs", []):
                        leg_oid = (leg.get("option", "").rstrip("/").split("/"))[-1]
                        if leg_oid == option_id:
                            order_matched = True
                            break
                    if order_matched:
                        try:
                            rh.orders.cancel_option_order(order["id"])
                            cancelled_count += 1
                            logger.info(
                                f"[OPTIMIZE MODE] Cancelled order {order['id']} "
                                f"for {sym} ${strike:g}"
                            )
                        except Exception as cancel_err:
                            logger.warning(
                                f"[OPTIMIZE MODE] Failed to cancel order "
                                f"{order['id']} for {sym}: {cancel_err}"
                            )

            r["orders_cancelled"] = cancelled_count

            # ── Step 4: Wait 20s if any orders were cancelled ─────────────────
            if cancelled_count > 0:
                logger.info("[OPTIMIZE MODE] Waiting 20s for cancellations to settle...")
                time.sleep(20)

            # ── Step 5: Submit atomic roll spread ─────────────────────────────
            spread_legs = [
                {
                    "expirationDate": expiration,
                    "strike":         f"{strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "close",
                    "action":         "buy",
                    "ratio_quantity": 1,
                },
                {
                    "expirationDate": best_exp,
                    "strike":         f"{best_strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "open",
                    "action":         "sell",
                    "ratio_quantity": 1,
                },
            ]
            try:
                roll_result = rh.orders.order_option_spread(
                    direction="credit",
                    price=net_credit,
                    symbol=sym,
                    quantity=r["quantity"],
                    spread=spread_legs,
                    timeInForce="gtc",
                )
                order_id = (roll_result or {}).get("id", "")
                if roll_result and order_id:
                    r["success"]  = True
                    r["order_id"] = order_id
                    logger.info(
                        f"[OPTIMIZE MODE] ✅ {sym} ${strike:g} → ${best_strike:g} "
                        f"exp {best_exp}  ${net_credit:.2f} credit  id={order_id}"
                    )
                else:
                    detail = (roll_result or {}).get("detail", "") or str(
                        (roll_result or {}).get("non_field_errors", "")
                    )
                    r["error"] = detail or f"Unexpected API response: {roll_result}"
                    logger.error(
                        f"[OPTIMIZE MODE] ❌ {sym} roll failed: {r['error']}"
                    )
            except Exception as e:
                r["error"] = str(e)
                logger.error(
                    f"[OPTIMIZE MODE] ❌ {sym} roll exception: {e}", exc_info=True
                )

            results.append(r)

    finally:
        try:
            logout()
        except Exception:
            pass

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Panic-mode roll execution (called automatically by the daily pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def execute_panic_rolls(
    open_short_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Panic mode: called by the daily pipeline for short CALL or short PUT
    contracts expiring TODAY that are in-the-money.

    For each such contract:
      1. Cancel any outstanding BTC/STC order for that contract
      2. Wait 30 seconds (only if an order was found and cancelled)
      3. Submit an atomic roll-forward spread order to the next available
         expiration at the same strike (or nearest OTM if exact unavailable)

    ITM definition:
      - CALL: stock_price >= strike  (called away at expiry)
      - PUT:  stock_price <= strike  (put to you at expiry)

    Strike selection for the roll-forward leg:
      - CALL: nearest OTM strike >= live_price  (same or higher)
      - PUT:  nearest OTM strike <= live_price  (same or lower)

    Input contracts must carry an ``opt_type`` field ("call" or "put");
    defaults to "call" if omitted (backward-compatible with covered-call pipeline).

    Returns a list of result dicts — one per DTE-0 ITM contract found —
    consumed by the emailer to populate the panic sub-section in the report.

    Each result dict contains:
      symbol, name, opt_type, strike, expiration, next_expiration, target_strike,
      next_dte, quantity, live_price, itm_by, btc_cancelled, success,
      order_id, net_price, direction, net_label, error
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    if name_map is None:
        name_map = {}

    today_str = str(date.today())

    # Detect DTE-0 ITM contracts (call: stock >= strike; put: stock <= strike)
    panic_contracts = []
    for c in open_short_contracts:
        if c.get("expiration", "") != today_str:
            continue
        sym      = c.get("symbol", "").upper()
        lp       = live_prices.get(sym, 0)
        strike   = c.get("strike", float("inf"))
        opt_type = c.get("opt_type", "call")
        if opt_type == "put":
            if lp <= strike:
                panic_contracts.append(c)
        else:
            if lp >= strike:
                panic_contracts.append(c)

    if not panic_contracts:
        return []

    logger.warning(
        f"[PANIC MODE] {len(panic_contracts)} DTE-0 ITM contract(s) detected: "
        + ", ".join(
            f"{c['symbol']} ${c['strike']:g} {c.get('opt_type','call').upper()}"
            for c in panic_contracts
        )
    )

    def _make_result(c: dict, lp: float) -> dict:
        """Scaffold an empty result dict for a contract."""
        sym      = c["symbol"].upper()
        strike   = float(c["strike"])
        opt_type = c.get("opt_type", "call")
        # itm_by: how deep in-the-money — always positive
        itm_by   = round(strike - lp, 2) if opt_type == "put" else round(lp - strike, 2)
        return {
            "symbol":          sym,
            "name":            name_map.get(sym, sym),
            "opt_type":        opt_type,
            "strike":          strike,
            "expiration":      c["expiration"],
            "next_expiration": "",
            "target_strike":   0.0,
            "next_dte":        0,
            "quantity":        int(c.get("quantity", 1)),
            "live_price":      lp,
            "itm_by":          itm_by,
            "btc_cancelled":   False,
            "success":         False,
            "order_id":        "",
            "net_price":       0.0,
            "direction":       "",
            "net_label":       "",
            "error":           "",
        }

    # Dry-run: record every contract but place no orders
    if dry_run:
        results = []
        for c in panic_contracts:
            sym = c["symbol"].upper()
            r = _make_result(c, live_prices.get(sym, 0.0))
            r["error"] = "[DRY RUN] no orders placed"
            results.append(r)
            logger.info(f"[PANIC MODE][DRY RUN] would roll {sym} ${c['strike']:g}")
        return results

    # Live run: login once, process all contracts, logout
    results = []
    try:
        login(force_fresh=False)
    except Exception as e:
        logger.error(f"[PANIC MODE] Login failed: {e}")
        for c in panic_contracts:
            sym = c["symbol"].upper()
            r = _make_result(c, live_prices.get(sym, 0.0))
            r["error"] = f"Login failed: {e}"
            results.append(r)
        return results

    try:
        # Fetch all open option orders once for cancellation lookup.
        # We cancel ALL orders touching this contract — not just BTC-only orders —
        # because a prior rescue-mode roll-forward spread (BTC close + STO open legs)
        # may still be open and unfilled. Leaving it open would block the panic roll.
        try:
            open_orders = rh.orders.get_all_open_option_orders() or []
        except Exception as fetch_err:
            logger.warning(f"[PANIC MODE] Could not fetch open orders: {fetch_err}")
            open_orders = []

        for c in panic_contracts:
            sym        = c["symbol"].upper()
            strike     = float(c["strike"])
            expiration = c["expiration"]
            option_id  = c.get("option_id", "")
            opt_type   = c.get("opt_type", "call")   # "call" or "put"
            # Use a fresh live price for execution accuracy
            live_price = _get_live_price(sym) or live_prices.get(sym, 0.0)
            r = _make_result(c, live_price)

            # ── Step 1: Cancel ALL outstanding orders for this contract ───────
            # Matches any order (BTC-only, roll-forward spread, or other) whose
            # legs reference this option_id — regardless of side/position_effect.
            cancelled_count = 0
            if option_id:
                for order in open_orders:
                    order_matched = False
                    for leg in order.get("legs", []):
                        leg_oid = (leg.get("option", "").rstrip("/").split("/"))[-1]
                        if leg_oid == option_id:
                            order_matched = True
                            break
                    if order_matched:
                        try:
                            rh.orders.cancel_option_order(order["id"])
                            cancelled_count += 1
                            logger.info(
                                f"[PANIC MODE] Cancelled order {order['id']} "
                                f"for {sym} ${strike:g}"
                            )
                        except Exception as cancel_err:
                            logger.warning(
                                f"[PANIC MODE] Failed to cancel order "
                                f"{order['id']} for {sym}: {cancel_err}"
                            )

            r["btc_cancelled"] = cancelled_count > 0

            # ── Step 2: Wait if we cancelled anything ─────────────────────────
            if cancelled_count > 0:
                logger.info("[PANIC MODE] Waiting 30s for cancellation to settle...")
                time.sleep(30)

            # ── Step 3: Find next expiration and target strike ────────────────
            try:
                ticker = yf.Ticker(_yahoo_symbol(sym))
                all_expirations = list(ticker.options)
            except Exception as e:
                r["error"] = f"Could not fetch expirations: {e}"
                logger.error(f"[PANIC MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            min_exp_date = (date.today() + timedelta(days=7)).isoformat()
            future_exps = [
                e for e in all_expirations
                if e > expiration and e >= min_exp_date
            ]
            if not future_exps:
                r["error"] = (
                    f"No future expirations available beyond {expiration} "
                    f"that are at least 7 days out"
                )
                logger.error(f"[PANIC MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            next_expiration = future_exps[0]
            next_dte = (date.fromisoformat(next_expiration) - date.today()).days

            target_strike = None
            btc_mid = sto_mid = 0.0
            try:
                chain_data = ticker.option_chain(next_expiration)
                # Use puts chain for short PUTs, calls chain for short CALLs
                df = (chain_data.puts if opt_type == "put" else chain_data.calls).copy()
                df["strike"] = df["strike"].astype(float)
                df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))

                exact = df[abs(df["strike"] - strike) < 0.01]
                if not exact.empty and float(exact.iloc[0]["bid"]) > 0:
                    target_strike = float(exact.iloc[0]["strike"])
                else:
                    if opt_type == "put":
                        # Roll to same or lower strike (stays OTM/same for puts)
                        otm = df[df["strike"] <= strike].sort_values("strike", ascending=False)
                    else:
                        # Roll to same or higher strike (stays OTM/same for calls)
                        otm = df[df["strike"] >= strike].sort_values("strike")
                    otm_valid = otm[otm["bid"] > 0]
                    if otm_valid.empty:
                        r["error"] = (
                            f"No {opt_type}s with strike "
                            f"{'<=' if opt_type == 'put' else '>='} ${strike:g} "
                            f"and non-zero bid at {next_expiration}"
                        )
                        logger.error(f"[PANIC MODE] {sym}: {r['error']}")
                        results.append(r)
                        continue
                    target_strike = float(otm_valid.iloc[0]["strike"])

                # BTC leg may have 0 bid/ask pre-market at DTE-0 — that is fine;
                # net will be based on STO mid alone (intrinsic is effectively 0)
                _, _, btc_mid = _get_option_bid_ask(sym, strike, opt_type, expiration)
                _, _, sto_mid = _get_option_bid_ask(sym, target_strike, opt_type, next_expiration)
            except Exception as e:
                r["error"] = f"Failed to find target strike: {e}"
                logger.error(f"[PANIC MODE] {sym}: {r['error']}", exc_info=True)
                results.append(r)
                continue

            net_raw   = round(sto_mid - btc_mid, 2)
            direction = "credit" if net_raw >= 0 else "debit"
            abs_net   = max(round(abs(net_raw), 2), 0.01)
            net_label = f"${abs_net:.2f} {direction}"

            r["next_expiration"] = next_expiration
            r["target_strike"]   = target_strike
            r["next_dte"]        = next_dte
            r["net_price"]       = abs_net
            r["direction"]       = direction
            r["net_label"]       = net_label

            # ── Step 4: Submit atomic spread order ────────────────────────────
            spread_legs = [
                {
                    "expirationDate": expiration,
                    "strike":         f"{strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "close",
                    "action":         "buy",
                    "ratio_quantity": 1,
                },
                {
                    "expirationDate": next_expiration,
                    "strike":         f"{target_strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "open",
                    "action":         "sell",
                    "ratio_quantity": 1,
                },
            ]
            try:
                roll_result = rh.orders.order_option_spread(
                    direction=direction,
                    price=abs_net,
                    symbol=sym,
                    quantity=r["quantity"],
                    spread=spread_legs,
                    timeInForce="gtc",
                )
                order_id = (roll_result or {}).get("id", "")
                if roll_result and order_id:
                    r["success"]  = True
                    r["order_id"] = order_id
                    logger.info(
                        f"[PANIC MODE] ✅ {sym} ${strike:g} → ${target_strike:g} "
                        f"{next_expiration}  {net_label}  id={order_id}"
                    )
                else:
                    detail = (roll_result or {}).get("detail", "") or str(
                        (roll_result or {}).get("non_field_errors", "")
                    )
                    r["error"] = detail or f"Unexpected API response: {roll_result}"
                    logger.error(f"[PANIC MODE] ❌ {sym} roll failed: {r['error']}")
            except Exception as e:
                r["error"] = str(e)
                logger.error(
                    f"[PANIC MODE] ❌ {sym} roll exception: {e}", exc_info=True
                )

            results.append(r)

    finally:
        try:
            logout()
        except Exception:
            pass

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Safety-mode BTC execution (called automatically by the daily pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def execute_safety_btc_orders(
    open_short_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Safety mode: called by the daily pipeline for ALL short CALL and short PUT
    contracts with DTE >= 1 that have no open BTC order (no upper DTE limit).

    For each such contract, places a buy-to-close GTC limit order at:
        MIN($0.20,  10% of original purchase price,  live mid of bid/ask)
    A random delay of 5–20 seconds is inserted between consecutive orders
    to avoid Robinhood rate-limiting.

    Input contracts must carry an ``opt_type`` field ("call" or "put");
    defaults to "call" if omitted (backward-compatible with covered-call pipeline).

    Returns a list of result dicts — one per candidate contract — consumed
    by the emailer to populate the safety sub-section in Section 3.

    Each result dict contains:
      symbol, name, opt_type, strike, expiration, dte, quantity, live_price,
      purchase_price, bid, ask, mid, btc_price, order_id, success, error
    """
    import random
    import robin_stocks.robinhood as rh
    from auth import login, logout

    if name_map is None:
        name_map = {}

    today = date.today()
    today_str = str(today)

    # Collect all future contracts (DTE >= 1) that have no existing BTC order.
    # No upper DTE limit — every open short option without protection is a candidate.
    candidates = []
    for c in open_short_contracts:
        exp = c.get("expiration", "")
        if not exp or exp <= today_str:
            continue          # skip DTE-0 (panic mode) and expired
        try:
            dte = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if dte < 1:
            continue
        if c.get("btc_order_exists", False):
            continue          # already protected
        candidates.append((c, dte))

    if not candidates:
        return []

    logger.info(
        f"[SAFETY MODE] {len(candidates)} contract(s) need BTC orders: "
        + ", ".join(
            f"{c['symbol']} ${c['strike']:g} DTE={dte}"
            for c, dte in candidates
        )
    )

    def _build_result(c: dict, dte: int, lp: float) -> dict:
        sym = c["symbol"].upper()
        return {
            "symbol":         sym,
            "name":           name_map.get(sym, sym),
            "opt_type":       c.get("opt_type", "call"),
            "strike":         float(c["strike"]),
            "expiration":     c["expiration"],
            "dte":            dte,
            "quantity":       int(c.get("quantity", 1)),
            "live_price":     lp,
            "purchase_price": abs(_safe_float(c.get("purchase_price", 0))),
            "bid":            0.0,
            "ask":            0.0,
            "mid":            0.0,
            "btc_price":      0.0,
            "order_id":       "",
            "success":        False,
            "error":          "",
        }

    # Dry-run: return stubs with no orders
    if dry_run:
        results = []
        for c, dte in candidates:
            sym = c["symbol"].upper()
            r = _build_result(c, dte, live_prices.get(sym, 0.0))
            r["error"] = "[DRY RUN] no orders placed"
            results.append(r)
            logger.info(
                f"[SAFETY MODE][DRY RUN] would BTC {sym} ${c['strike']:g} DTE={dte}"
            )
        return results

    # Login once for all safety orders
    results = []
    try:
        login(force_fresh=False)
    except Exception as e:
        logger.error(f"[SAFETY MODE] Login failed: {e}")
        for c, dte in candidates:
            sym = c["symbol"].upper()
            r = _build_result(c, dte, live_prices.get(sym, 0.0))
            r["error"] = f"Login failed: {e}"
            results.append(r)
        return results

    try:
        for idx, (c, dte) in enumerate(candidates):
            sym            = c["symbol"].upper()
            strike         = float(c["strike"])
            expiration     = c["expiration"]
            quantity       = int(c.get("quantity", 1))
            purchase_price = abs(_safe_float(c.get("purchase_price", 0)))
            live_price     = live_prices.get(sym, 0.0)
            opt_type       = c.get("opt_type", "call")   # "call" or "put"

            # Random inter-order delay (skip before the very first order)
            if idx > 0:
                delay = random.randint(5, 20)
                logger.info(
                    f"[SAFETY MODE] Waiting {delay}s before next order..."
                )
                time.sleep(delay)

            # Fetch live bid / ask using the correct option type
            bid, ask, mid = _get_option_bid_ask(sym, strike, opt_type, expiration)

            # BTC limit price = MIN($0.20, 10% of per-share purchase premium, live mid)
            # purchase_price is stored as total contract value (100 shares), so divide by 100
            # for the per-share premium before applying the 10% threshold.
            price_candidates = [0.20]
            if purchase_price > 0:
                per_share_premium = purchase_price / 100.0
                price_candidates.append(round(per_share_premium * 0.10, 2))
            if mid > 0:
                price_candidates.append(mid)
            btc_price = max(round(min(price_candidates), 2), 0.01)

            r = _build_result(c, dte, live_price)
            r["bid"]       = bid
            r["ask"]       = ask
            r["mid"]       = mid
            r["btc_price"] = btc_price

            try:
                order = rh.orders.order_buy_option_limit(
                    positionEffect="close",
                    creditOrDebit="debit",
                    price=btc_price,
                    symbol=sym,
                    quantity=quantity,
                    expirationDate=expiration,
                    strike=strike,
                    optionType=opt_type,
                    timeInForce="gtc",
                )
                order_id = (order or {}).get("id", "")
                if order and order_id:
                    r["success"]  = True
                    r["order_id"] = order_id
                    logger.info(
                        f"[SAFETY MODE] ✅ BTC placed for {sym} ${strike:g} "
                        f"{opt_type.upper()} exp {expiration} DTE={dte} "
                        f"at ${btc_price:.2f}  id={order_id}"
                    )
                else:
                    detail = (order or {}).get("detail", "") or str(
                        (order or {}).get("non_field_errors", "")
                    )
                    r["error"] = detail or f"Unexpected response: {order}"
                    logger.error(
                        f"[SAFETY MODE] ❌ BTC failed for {sym} ${strike:g}: "
                        f"{r['error']}"
                    )
            except Exception as e:
                r["error"] = str(e)
                logger.error(
                    f"[SAFETY MODE] ❌ BTC exception for {sym} ${strike:g}: {e}",
                    exc_info=True,
                )

            results.append(r)

    finally:
        try:
            logout()
        except Exception:
            pass

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Rescue-mode roll execution (called automatically by the daily pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def execute_rescue_rolls(
    open_short_contracts: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Rescue mode: called by the daily pipeline for short CALL and short PUT
    contracts expiring in the next 1–2 days that are in-the-money.

    For each such contract:
      1. Find the next available expiration (≥ 7 days out); scan candidate
         strikes and pick the one that maximises Risk/Reward ratio with
         positive net credit.  Record skipped=True if none found.
         Strike selection:
           - CALL: strikes >= current (same or higher — stays OTM on calls)
           - PUT:  strikes <= current (same or lower  — stays OTM on puts)
         Risk calculation:
           - CALL: new_strike − live_price  (gap above stock price)
           - PUT:  live_price − new_strike  (gap below stock price)
      2. Cancel ALL outstanding orders for the contract (any side/effect)
      3. Wait 30 seconds (only if any orders were cancelled)
      4. Submit an atomic roll-forward spread at the net mid price

    Input contracts must carry an ``opt_type`` field ("call" or "put");
    defaults to "call" if omitted (backward-compatible with covered-call pipeline).

    Returns a list of result dicts — one per DTE-1-2 ITM contract found —
    consumed by the emailer to populate the rescue sub-section in Section 2.
    Skipped contracts (no credit available) are included with skipped=True
    and are NOT removed from roll_candidates in the scheduler (safety mode
    may still protect them with a BTC order).

    Each result dict contains:
      symbol, name, opt_type, strike, expiration, dte, next_expiration,
      target_strike, next_dte, quantity, live_price, itm_by, orders_cancelled,
      success, skipped, order_id, net_price, direction, net_label, error
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    if name_map is None:
        name_map = {}

    today = date.today()
    today_str = str(today)

    # ── Collect DTE 1-2 ITM contracts ────────────────────────────────────────
    rescue_contracts = []
    for c in open_short_contracts:
        exp = c.get("expiration", "")
        if not exp or exp <= today_str:
            continue   # skip DTE-0 (handled by panic mode) and expired
        try:
            dte = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if dte not in (1, 2):
            continue
        sym      = c.get("symbol", "").upper()
        lp       = live_prices.get(sym, 0)
        strike   = c.get("strike", float("inf"))
        opt_type = c.get("opt_type", "call")
        # ITM: call if stock >= strike; put if stock <= strike
        if opt_type == "put":
            if lp <= strike:
                rescue_contracts.append((c, dte))
        else:
            if lp >= strike:
                rescue_contracts.append((c, dte))

    if not rescue_contracts:
        return []

    logger.info(
        f"[RESCUE MODE] {len(rescue_contracts)} DTE-1-2 ITM contract(s) detected: "
        + ", ".join(
            f"{c['symbol']} ${c['strike']:g} {c.get('opt_type','call').upper()} DTE={dte}"
            for c, dte in rescue_contracts
        )
    )

    def _make_result(c: dict, lp: float, dte: int) -> dict:
        sym      = c["symbol"].upper()
        strike   = float(c["strike"])
        opt_type = c.get("opt_type", "call")
        # itm_by: always positive — how deep in-the-money
        itm_by   = round(strike - lp, 2) if opt_type == "put" else round(lp - strike, 2)
        return {
            "symbol":           sym,
            "name":             name_map.get(sym, sym),
            "opt_type":         opt_type,
            "strike":           strike,
            "expiration":       c["expiration"],
            "dte":              dte,
            "next_expiration":  "",
            "target_strike":    0.0,
            "next_dte":         0,
            "quantity":         int(c.get("quantity", 1)),
            "live_price":       lp,
            "itm_by":           itm_by,
            "orders_cancelled": 0,
            "success":          False,
            "skipped":          False,
            "order_id":         "",
            "net_price":        0.0,
            "direction":        "",
            "net_label":        "",
            "error":            "",
        }

    # ── Dry run ───────────────────────────────────────────────────────────────
    if dry_run:
        results = []
        for c, dte in rescue_contracts:
            sym = c["symbol"].upper()
            r = _make_result(c, live_prices.get(sym, 0.0), dte)
            r["error"] = "[DRY RUN] no orders placed"
            results.append(r)
            logger.info(f"[RESCUE MODE][DRY RUN] would rescue-roll {sym} ${c['strike']:g}")
        return results

    # ── Live run: login once, process all, logout ─────────────────────────────
    results = []
    try:
        login(force_fresh=False)
    except Exception as e:
        logger.error(f"[RESCUE MODE] Login failed: {e}")
        for c, dte in rescue_contracts:
            sym = c["symbol"].upper()
            r = _make_result(c, live_prices.get(sym, 0.0), dte)
            r["error"] = f"Login failed: {e}"
            results.append(r)
        return results

    try:
        try:
            open_orders = rh.orders.get_all_open_option_orders() or []
        except Exception as fetch_err:
            logger.warning(f"[RESCUE MODE] Could not fetch open orders: {fetch_err}")
            open_orders = []

        for c, dte in rescue_contracts:
            sym        = c["symbol"].upper()
            strike     = float(c["strike"])
            expiration = c["expiration"]
            option_id  = c.get("option_id", "")
            opt_type   = c.get("opt_type", "call")   # "call" or "put"
            live_price = _get_live_price(sym) or live_prices.get(sym, 0.0)
            r = _make_result(c, live_price, dte)

            # ── Step 1: Find next expiration ──────────────────────────────────
            try:
                ticker = yf.Ticker(_yahoo_symbol(sym))
                all_expirations = list(ticker.options)
            except Exception as e:
                r["error"] = f"Could not fetch expirations: {e}"
                logger.error(f"[RESCUE MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            future_exps = [e for e in all_expirations if e > expiration]
            if not future_exps:
                r["error"] = f"No future expirations available beyond {expiration}"
                logger.error(f"[RESCUE MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            next_expiration = future_exps[0]
            next_dte = (date.fromisoformat(next_expiration) - date.today()).days

            # ── Step 2: Find best Risk/Reward strike ─────────────────────────
            # Reward = net credit (sto_mid - btc_mid)
            # Risk   = |new_strike - live_price|  (gap to new strike)
            #          CALL: new_strike − live_price  (strike above stock)
            #          PUT:  live_price − new_strike  (strike below stock)
            # R/R    = Reward / max(Risk, ε)  — higher is better
            # Only credit-positive strikes are considered; if none exist, skip.
            target_strike = None
            sto_mid       = 0.0
            btc_mid       = 0.0

            try:
                chain_data = ticker.option_chain(next_expiration)
                # Use puts chain for short PUTs, calls chain for short CALLs
                df = (chain_data.puts if opt_type == "put" else chain_data.calls).copy()
                df["strike"] = df["strike"].astype(float)
                df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))
                df["ask"]    = df["ask"].apply(lambda x: _safe_float(x))
                df["mid"]    = (df["bid"] + df["ask"]) / 2.0

                # Strike filter: calls roll to same-or-higher; puts roll to same-or-lower
                if opt_type == "put":
                    candidates = df[(df["strike"] <= strike + 0.01) & (df["bid"] > 0)].copy()
                else:
                    candidates = df[(df["strike"] >= strike - 0.01) & (df["bid"] > 0)].copy()

                # BTC mid for the current (closing) leg
                _, _, btc_mid = _get_option_bid_ask(sym, strike, opt_type, expiration)

                if candidates.empty:
                    r["skipped"] = True
                    if opt_type == "put":
                        r["error"] = (
                            f"No strikes <= ${strike:g} with non-zero bid at {next_expiration}"
                        )
                    else:
                        r["error"] = (
                            f"No strikes >= ${strike:g} with non-zero bid at {next_expiration}"
                        )
                    logger.info(f"[RESCUE MODE] {sym}: skipped — {r['error']}")
                    results.append(r)
                    continue

                candidates = candidates.copy()
                candidates["net_credit"] = candidates["mid"] - btc_mid

                # Filter to credit-positive strikes only
                credit_pos = candidates[candidates["net_credit"] > 0]
                if credit_pos.empty:
                    best_nc   = candidates.loc[candidates["net_credit"].idxmax()]
                    r["skipped"] = True
                    r["error"]   = (
                        f"No credit available — best ${float(best_nc['strike']):g} "
                        f"yields ${float(best_nc['net_credit']):.2f} net"
                    )
                    logger.info(f"[RESCUE MODE] {sym}: skipped — {r['error']}")
                    results.append(r)
                    continue

                # Maximise Risk/Reward ratio among credit-positive candidates
                _EPS = 0.001   # floor to avoid division by zero for at-price strikes
                credit_pos = credit_pos.copy()
                if opt_type == "put":
                    # Risk for put roll = live_price − new_strike (gap below stock price)
                    credit_pos["risk"] = (live_price - credit_pos["strike"]).clip(lower=_EPS)
                else:
                    # Risk for call roll = new_strike − live_price (gap above stock price)
                    credit_pos["risk"] = (credit_pos["strike"] - live_price).clip(lower=_EPS)
                credit_pos["rr_ratio"] = credit_pos["net_credit"] / credit_pos["risk"]
                best_row = credit_pos.loc[credit_pos["rr_ratio"].idxmax()]

                target_strike = float(best_row["strike"])

                # Confirm with a live bid/ask fetch for accurate mid
                _, _, sto_mid_live = _get_option_bid_ask(
                    sym, target_strike, opt_type, next_expiration
                )
                sto_mid    = sto_mid_live if sto_mid_live > 0 else round(float(best_row["mid"]), 2)
                net_credit = round(sto_mid - btc_mid, 2)

                if net_credit <= 0:
                    r["skipped"] = True
                    r["error"]   = (
                        f"Live check: no credit — ${target_strike:g} "
                        f"mid ${sto_mid:.2f} − BTC ${btc_mid:.2f} = ${net_credit:.2f}"
                    )
                    logger.info(f"[RESCUE MODE] {sym}: skipped — {r['error']}")
                    results.append(r)
                    continue

            except Exception as e:
                r["error"] = f"Failed to find max-credit strike: {e}"
                logger.error(f"[RESCUE MODE] {sym}: {r['error']}", exc_info=True)
                results.append(r)
                continue

            direction = "credit"   # only proceed when net_credit > 0
            abs_net   = max(round(net_credit, 2), 0.01)
            net_label = f"${abs_net:.2f} credit"

            r["next_expiration"] = next_expiration
            r["target_strike"]   = target_strike
            r["next_dte"]        = next_dte
            r["net_price"]       = abs_net
            r["direction"]       = direction
            r["net_label"]       = net_label

            # ── Step 3: Cancel ALL outstanding orders for this contract ────────
            cancelled_count = 0
            if option_id:
                for order in open_orders:
                    order_matched = False
                    for leg in order.get("legs", []):
                        leg_oid = (leg.get("option", "").rstrip("/").split("/"))[-1]
                        if leg_oid == option_id:
                            order_matched = True
                            break
                    if order_matched:
                        try:
                            rh.orders.cancel_option_order(order["id"])
                            cancelled_count += 1
                            logger.info(
                                f"[RESCUE MODE] Cancelled order {order['id']} "
                                f"for {sym} ${strike:g}"
                            )
                        except Exception as cancel_err:
                            logger.warning(
                                f"[RESCUE MODE] Failed to cancel order "
                                f"{order['id']} for {sym}: {cancel_err}"
                            )

            r["orders_cancelled"] = cancelled_count

            # ── Step 4: Wait if any orders were cancelled ─────────────────────
            if cancelled_count > 0:
                logger.info("[RESCUE MODE] Waiting 30s for cancellations to settle...")
                time.sleep(30)

            # ── Step 5: Submit atomic roll-forward spread ─────────────────────
            spread_legs = [
                {
                    "expirationDate": expiration,
                    "strike":         f"{strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "close",
                    "action":         "buy",
                    "ratio_quantity": 1,
                },
                {
                    "expirationDate": next_expiration,
                    "strike":         f"{target_strike:.4f}",
                    "optionType":     opt_type,
                    "effect":         "open",
                    "action":         "sell",
                    "ratio_quantity": 1,
                },
            ]
            try:
                roll_result = rh.orders.order_option_spread(
                    direction=direction,
                    price=abs_net,
                    symbol=sym,
                    quantity=r["quantity"],
                    spread=spread_legs,
                    timeInForce="gtc",
                )
                order_id = (roll_result or {}).get("id", "")
                if roll_result and order_id:
                    r["success"]  = True
                    r["order_id"] = order_id
                    logger.info(
                        f"[RESCUE MODE] ✅ {sym} ${strike:g} → ${target_strike:g} "
                        f"{next_expiration}  {net_label}  id={order_id}"
                    )
                else:
                    detail = (roll_result or {}).get("detail", "") or str(
                        (roll_result or {}).get("non_field_errors", "")
                    )
                    r["error"] = detail or f"Unexpected API response: {roll_result}"
                    logger.error(f"[RESCUE MODE] ❌ {sym} roll failed: {r['error']}")
            except Exception as e:
                r["error"] = str(e)
                logger.error(f"[RESCUE MODE] ❌ {sym} roll exception: {e}", exc_info=True)

            results.append(r)

    finally:
        try:
            logout()
        except Exception:
            pass

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Collar holdings (show / add / roll) — v1.8
# ─────────────────────────────────────────────────────────────────────────────

def show_collar_holdings(symbol=None):
    """
    Display open collar positions (short calls + long puts) from Robinhood.

    If `symbol` is provided, only show that ticker; otherwise show all.
    A collar pair is identified as: a short CALL + a long PUT on the same symbol
    with the same expiration date.

    Positions without a matching pair are listed under "Unpaired legs".
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout
    from collections import defaultdict

    filter_sym = symbol.upper() if symbol else None

    login()
    try:
        positions = rh.options.get_open_option_positions() or []
        legs = []
        for pos in positions:
            try:
                chain_sym = (pos.get("chain_symbol") or "").upper()
                if filter_sym and chain_sym != filter_sym:
                    continue
                qty      = float(pos.get("quantity", 0))
                pos_type = (pos.get("type") or "").lower()
                if qty <= 0:
                    continue

                option_id   = pos.get("option_id", "")
                option_type = ""
                strike      = 0.0
                expiration  = pos.get("expiration_date", "")

                if option_id:
                    try:
                        instr       = rh.options.get_option_instrument_data_by_id(option_id)
                        option_type = (instr.get("type") or "").lower()
                        strike      = float(instr.get("strike_price", 0) or 0)
                        expiration  = instr.get("expiration_date", expiration) or expiration
                    except Exception:
                        pass

                legs.append({
                    "symbol":      chain_sym,
                    "option_type": option_type,
                    "pos_type":    pos_type,
                    "strike":      strike,
                    "expiration":  expiration,
                    "quantity":    int(qty),
                    "avg_price":   float(pos.get("average_price", 0) or 0),
                })
            except Exception:
                continue
    finally:
        logout()

    if not legs:
        msg = f"No open options positions for {filter_sym}." if filter_sym else "No open options positions."
        print(f"\n{msg}\n")
        return

    # Pair short CALLs + long PUTs sharing (symbol, expiration).
    # Multiple collars on the same symbol/expiry are supported: sort both lists
    # by strike ascending and pair them positionally.
    groups = defaultdict(lambda: {"short_calls": [], "long_puts": [], "other": []})
    for leg in legs:
        key = (leg["symbol"], leg["expiration"])
        if leg["option_type"] == "call" and leg["pos_type"] == "short":
            groups[key]["short_calls"].append(leg)
        elif leg["option_type"] == "put" and leg["pos_type"] == "long":
            groups[key]["long_puts"].append(leg)
        else:
            groups[key]["other"].append(leg)

    pairs   = []   # list of (sym, exp, sc_leg, lp_leg)
    orphans = []
    for (sym, exp), v in groups.items():
        calls = sorted(v["short_calls"], key=lambda x: x["strike"])
        puts  = sorted(v["long_puts"],   key=lambda x: x["strike"])
        n_pairs = min(len(calls), len(puts))
        for i in range(n_pairs):
            pairs.append((sym, exp, calls[i], puts[i]))
        # leftover unpaired legs
        for leg in calls[n_pairs:] + puts[n_pairs:] + v["other"]:
            orphans.append(leg)

    if not pairs and not orphans:
        msg = f"No collar positions found for {filter_sym}." if filter_sym else "No collar positions found."
        print(f"\n{msg}\n")
        return

    title = "Collar Holdings" + (f" — {filter_sym}" if filter_sym else "")
    print(f"\n{title}")
    print("─" * 92)

    if pairs:
        print(f"  {'Symbol':<8}  {'Expiry':<12}  {'DTE':>4}  {'Call Strike':>12}  "
              f"{'Put Strike':>10}  {'Qty':>4}  {'CC Avg':>8}  {'LP Avg':>8}  {'Net/sh':>10}")
        print("─" * 92)
        for sym, exp, sc, lp in sorted(pairs, key=lambda x: (x[0], x[1], x[2]["strike"])):
            dte_val = _dte(exp)
            dte_str = str(dte_val) if dte_val >= 0 else "EXP"
            qty     = min(sc["quantity"], lp["quantity"])
            net     = round(lp["avg_price"] - sc["avg_price"], 2)
            net_str = f"+${abs(net):.2f} cr" if net <= 0 else f"-${net:.2f} db"
            print(f"  {sym:<8}  {exp:<12}  {dte_str:>4}  ${sc['strike']:>11.2f}  "
                  f"${lp['strike']:>9.2f}  {qty:>4}  ${sc['avg_price']:>7.2f}  "
                  f"${lp['avg_price']:>7.2f}  {net_str:>10}")
        print()

    if orphans:
        print("  Unpaired legs:")
        for leg in sorted(orphans, key=lambda x: (x["symbol"], x["expiration"])):
            side_type = f"{leg['pos_type'].upper()} {leg['option_type'].upper()}"
            print(f"    {leg['symbol']:<8}  {side_type:<14}  strike=${leg['strike']:.2f}  "
                  f"exp={leg['expiration']}  qty={leg['quantity']}")
        print()


def _print_call_manual_instructions(symbol, contracts, call_strike, call_exp,
                                     call_mid, put_order_id, n_pending):
    """
    Print the manual SELL CALL instructions after the API path fails.
    Called whenever Robinhood's API rejects the covered-call order.
    """
    print()
    print(f"  ⚠️  Robinhood's API cannot verify that the short call is covered by your")
    print(f"  {symbol} shares — this is a known limitation of the options API endpoint.")
    print(f"  The covered-call verification only works through the Robinhood app UI.")
    if n_pending:
        print()
        print(f"  Note: {n_pending} pending SELL CALL order(s) found for {symbol}.")
        print(f"  If those have already filled, your shares may be fully committed.")
        print(f"  Check Orders → Pending in the app before selling the call below.")
    print()
    print(f"  ✅  Your BUY PUT is placed (id={put_order_id}).")
    print()
    print(f"  ┌─ COMPLETE THE COLLAR IN ROBINHOOD APP ─────────────────────────┐")
    print(f"  │  Symbol   : {symbol}")
    print(f"  │  Action   : Sell Call (Covered)")
    print(f"  │  Strike   : ${call_strike:.2f}")
    print(f"  │  Expiry   : {call_exp}")
    print(f"  │  Quantity : {contracts} contract(s)")
    print(f"  │  Limit    : ${call_mid:.2f}/sh  (mid-price — adjust to current market)")
    print(f"  └────────────────────────────────────────────────────────────────┘")


def _check_pending_call_orders(rh, symbol):
    """
    Return (count, order_ids) of OPEN/QUEUED sell-call orders for `symbol`.
    Used as a pre-flight check before placing a collar to diagnose
    "infinite risk" rejections caused by shares already reserved for pending orders.
    """
    try:
        all_orders = rh.orders.get_all_option_orders() or []
        pending = []
        for o in all_orders:
            if (o.get("chain_symbol") or "").upper() != symbol.upper():
                continue
            state = (o.get("state") or "").lower()
            if state not in ("confirmed", "queued", "unconfirmed", "partially_filled"):
                continue
            for leg in (o.get("legs") or []):
                if leg.get("side") == "sell" and leg.get("position_effect") == "open":
                    pending.append(o.get("id", "?"))
                    break
        return len(pending), pending
    except Exception:
        return 0, []


def place_collar_order(symbol, rec, prompt=True, contracts_override=None):
    """
    Place a collar order for SYMBOL.

    Strategy
    --------
    First attempt: single multi-leg `order_option_spread` (SELL CALL + BUY PUT).
    Robinhood's API does NOT evaluate equity positions when processing options orders,
    so a short call in a combined options order is still flagged as potentially naked
    unless the account has Level 3 options approval (which explicitly covers collars).

    If the combined order is rejected with "infinite risk":
      • Prints a clear diagnostic with actionable steps.
      • Offers a fallback: place only the BUY PUT via API so downside protection is
        in place, and prompts the user to complete the SELL CALL manually through the
        Robinhood app (where equity context IS checked).

    `contracts_override` — overrides rec["contracts"] when supplied (e.g. --contracts N).
    If prompt=True, prints a summary and requires y/n confirmation before placing.

    Returns True if both legs were successfully submitted, False otherwise.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    symbol = symbol.upper()
    call_leg  = rec.get("call_leg", {})
    put_leg   = rec.get("put_leg", {})
    contracts = contracts_override if contracts_override is not None else rec.get("contracts", 1)

    call_strike = call_leg.get("strike")
    call_exp    = call_leg.get("expiration") or rec.get("expiration") or rec.get("cc_expiration")
    call_mid    = round(float(call_leg.get("mid", 0.0)), 2)

    put_strike  = put_leg.get("strike")
    put_exp     = put_leg.get("expiration") or rec.get("expiration") or rec.get("lp_expiration")
    put_mid     = round(float(put_leg.get("mid", 0.0)), 2)

    net_per_share = round(call_mid - put_mid, 2)
    direction     = "credit" if net_per_share >= 0 else "debit"
    net_price     = round(abs(net_per_share), 2)

    if prompt:
        print(f"\n{'─' * 68}")
        print(f"  Collar order for {symbol}  ({contracts} contract(s))")
        print(f"{'─' * 68}")
        print(f"  SELL CALL  ${call_strike:.2f}  exp {call_exp}  @ ${call_mid:.2f}/sh"
              f"  →  +${call_mid * 100 * contracts:.2f} credit")
        print(f"  BUY  PUT   ${put_strike:.2f}  exp {put_exp}  @ ${put_mid:.2f}/sh"
              f"  →  -${put_mid * 100 * contracts:.2f} debit")
        print(f"{'─' * 68}")
        if net_per_share >= 0:
            net_str = f"+${net_per_share:.2f}/sh net credit"
        else:
            net_str = f"-${abs(net_per_share):.2f}/sh net debit"
        print(f"  Net: {net_str}  (${abs(net_per_share) * 100 * contracts:.2f} total)")
        print(f"{'─' * 68}")
        answer = input("  Place this order? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborted.\n")
            return False

    login()
    try:
        # ── Pre-flight: warn about pending call orders that could exhaust covered capacity ──
        n_pending, pending_ids = _check_pending_call_orders(rh, symbol)
        if n_pending:
            logger.warning(
                f"[COLLAR ADD] {n_pending} pending SELL CALL order(s) found for {symbol} "
                f"(ids: {pending_ids}); these may exhaust covered-call capacity."
            )
            print(f"\n  ⚠️  {n_pending} pending SELL CALL order(s) exist for {symbol}.")
            print(f"     These may already be using your covered-call capacity and could")
            print(f"     cause an 'infinite risk' rejection below.")
            print(f"     Consider cancelling them first via the Robinhood app.\n")

        # ── Two-step approach: BUY PUT (Level 1) then SELL CALL (Level 2 covered call) ──
        #
        # Why NOT multi-leg order_option_spread:
        #   A collar (SELL CALL + BUY PUT) is a cross-type spread whose call leg has no
        #   options-level hedge against the put. Robinhood's spread endpoint evaluates
        #   options risk in isolation — it does not look at equity holdings — so the
        #   naked-call risk triggers "infinite risk" regardless of options level.
        #
        # Why two single-leg orders work:
        #   order_buy_option_limit  → Level 1, always accepted.
        #   order_sell_option_limit → uses Robinhood's covered-call path (Level 2) which
        #   DOES check equity holdings. If the account owns enough shares the call is
        #   approved as covered; no equity visibility issue.
        #
        # Strikes must be formatted as "NNN.NNNN" (4dp) so that id_for_option's
        # strike_price query matches Robinhood's stored format exactly.

        call_strike_s = f"{float(call_strike):.4f}"
        put_strike_s  = f"{float(put_strike):.4f}"

        # ── Step 1: BUY PUT ─────────────────────────────────────────────────────────────
        logger.info(
            f"[COLLAR ADD] Step 1 — BTO {symbol} ${put_strike_s} PUT {put_exp} "
            f"@ ${put_mid:.2f}/sh x{contracts}"
        )
        put_result = rh.orders.order_buy_option_limit(
            positionEffect="open",
            creditOrDebit="debit",
            price=str(put_mid),
            symbol=symbol,
            quantity=contracts,
            expirationDate=put_exp,
            strike=put_strike_s,
            optionType="put",
            timeInForce="gfd",
        )
        put_id = (put_result or {}).get("id", "")
        if not put_id:
            put_err = (put_result or {}).get("detail") or str(put_result)
            logger.error(f"[COLLAR ADD] BTO put failed: {put_err}")
            print(f"\n  ❌  BUY PUT order failed: {put_err}\n")
            return False
        print(f"\n  ✅  BUY PUT  submitted  (id={put_id})")

        # ── Step 2: SELL CALL (covered call — equity check happens here) ────────────────
        logger.info(
            f"[COLLAR ADD] Step 2 — STO {symbol} ${call_strike_s} CALL {call_exp} "
            f"@ ${call_mid:.2f}/sh x{contracts}"
        )
        call_result = rh.orders.order_sell_option_limit(
            positionEffect="open",
            creditOrDebit="credit",
            price=str(call_mid),
            symbol=symbol,
            quantity=contracts,
            expirationDate=call_exp,
            strike=call_strike_s,
            optionType="call",
            timeInForce="gfd",
        )
        call_id = (call_result or {}).get("id", "")
        if not call_id:
            call_err = (call_result or {}).get("detail") or str(call_result)
            logger.error(f"[COLLAR ADD] STO call failed: {call_err}")
            print(f"  ❌  SELL CALL order rejected: {call_err}")
            _print_call_manual_instructions(symbol, contracts, call_strike, call_exp,
                                            call_mid, put_id, n_pending)
            print()
            return False

        print(f"  ✅  SELL CALL submitted  (id={call_id})\n")
        return True

    except Exception as e:  # noqa: BLE001
        logger.error(f"[COLLAR ADD] Exception: {e}", exc_info=True)
        print(f"\n  ❌  Order failed: {e}\n")
        return False
    finally:
        logout()


# ─────────────────────────────────────────────────────────────────────────────
# PCS / CCS spread holdings display and order placement
# ─────────────────────────────────────────────────────────────────────────────

def show_spread_holdings(spread_type: str, symbol: Optional[str] = None) -> None:
    """
    Display open PCS (Bull Put Spread) or CCS (Bear Call Spread) positions.

    spread_type: "PCS" or "CCS"
    symbol: ticker to filter; None = show all symbols.

    A spread pair is identified as:
      PCS: short PUT + long PUT on the same symbol/expiration, short_strike > long_strike
      CCS: short CALL + long CALL on the same symbol/expiration, short_strike < long_strike

    Positions without a matching counterpart are listed under "Unpaired legs".
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout
    from collections import defaultdict

    spread_type = spread_type.upper()
    opt_type   = "put"  if spread_type == "PCS" else "call"
    filter_sym = symbol.upper() if symbol else None
    label      = ("Put Credit Spread (Bull Put)"
                  if spread_type == "PCS" else "Call Credit Spread (Bear Call)")

    login()
    try:
        positions = rh.options.get_open_option_positions() or []
        legs = []
        for pos in positions:
            try:
                chain_sym = (pos.get("chain_symbol") or "").upper()
                if filter_sym and chain_sym != filter_sym:
                    continue
                qty      = float(pos.get("quantity", 0))
                pos_type = (pos.get("type") or "").lower()
                if qty <= 0:
                    continue

                option_id  = pos.get("option_id", "")
                o_type     = ""
                strike     = 0.0
                expiration = pos.get("expiration_date", "")

                if option_id:
                    try:
                        instr      = rh.options.get_option_instrument_data_by_id(option_id)
                        o_type     = (instr.get("type") or "").lower()
                        strike     = float(instr.get("strike_price", 0) or 0)
                        expiration = instr.get("expiration_date", expiration) or expiration
                    except Exception:
                        pass

                # Only include legs of the relevant option type
                if o_type != opt_type:
                    continue

                legs.append({
                    "symbol":     chain_sym,
                    "opt_type":   o_type,
                    "pos_type":   pos_type,
                    "strike":     strike,
                    "expiration": expiration,
                    "quantity":   int(qty),
                    "avg_price":  float(pos.get("average_price", 0) or 0),
                })
            except Exception:
                continue
    finally:
        logout()

    if not legs:
        msg = (f"No open {spread_type} positions for {filter_sym}."
               if filter_sym else f"No open {spread_type} positions.")
        print(f"\n{msg}\n")
        return

    # Group by (symbol, expiration) and bucket by position side
    groups = defaultdict(lambda: {"short": [], "long": []})
    for leg in legs:
        key = (leg["symbol"], leg["expiration"])
        side = leg["pos_type"]
        if side in ("short", "long"):
            groups[key][side].append(leg)

    pairs: List[tuple] = []  # (sym, exp, short_leg, long_leg)
    orphans: List[dict] = []

    for (sym, exp), v in groups.items():
        # Pair by positional order sorted so the spread direction is correct:
        #   PCS: short_strike > long_strike → sort short DESC, long ASC
        #   CCS: short_strike < long_strike → sort short ASC, long DESC
        if spread_type == "PCS":
            shorts = sorted(v["short"], key=lambda x: x["strike"], reverse=True)
            longs  = sorted(v["long"],  key=lambda x: x["strike"])
        else:
            shorts = sorted(v["short"], key=lambda x: x["strike"])
            longs  = sorted(v["long"],  key=lambda x: x["strike"], reverse=True)

        n_pairs = min(len(shorts), len(longs))
        for i in range(n_pairs):
            sh, lo = shorts[i], longs[i]
            valid = (spread_type == "PCS" and sh["strike"] > lo["strike"]) or \
                    (spread_type == "CCS" and sh["strike"] < lo["strike"])
            if valid:
                pairs.append((sym, exp, sh, lo))
            else:
                orphans += [sh, lo]
        for leg in shorts[n_pairs:] + longs[n_pairs:]:
            orphans.append(leg)

    if not pairs and not orphans:
        msg = (f"No {spread_type} pairs found for {filter_sym}."
               if filter_sym else f"No {spread_type} pairs found.")
        print(f"\n{msg}\n")
        return

    title = f"{label} Holdings" + (f" — {filter_sym}" if filter_sym else "")
    print(f"\n{title}")
    print("─" * 96)

    if pairs:
        print(f"  {'Symbol':<8}  {'Expiry':<12}  {'DTE':>4}  {'Short Strike':>12}  "
              f"{'Long  Strike':>12}  {'Width':>7}  {'Qty':>4}  {'Credit/sh':>10}  {'Net/ct':>8}")
        print("─" * 96)
        for sym, exp, sh, lo in sorted(pairs, key=lambda x: (x[0], x[1], x[2]["strike"])):
            dte_val  = _dte(exp)
            dte_str  = str(dte_val) if dte_val >= 0 else "EXP"
            qty      = min(sh["quantity"], lo["quantity"])
            width    = abs(sh["strike"] - lo["strike"])
            # Robinhood stores avg_price as per-contract total with short=negative.
            # orig credit/sh = (abs(short_avg) - long_avg) / 100
            credit   = round((abs(sh["avg_price"]) - lo["avg_price"]) / 100, 2)
            net_ct   = round(credit * 100, 2)
            print(f"  {sym:<8}  {exp:<12}  {dte_str:>4}  ${sh['strike']:>11.2f}  "
                  f"${lo['strike']:>11.2f}  ${width:>6.2f}  {qty:>4}  ${credit:>9.2f}  ${net_ct:>7.2f}")
        print()

    if orphans:
        print("  Unpaired legs:")
        for leg in sorted(orphans, key=lambda x: (x["symbol"], x["expiration"])):
            side_type = f"{leg['pos_type'].upper()} {leg['opt_type'].upper()}"
            print(f"    {leg['symbol']:<8}  {side_type:<14}  strike=${leg['strike']:.2f}  "
                  f"exp={leg['expiration']}  qty={leg['quantity']}")
        print()


def place_spread_order(symbol: str, rec: dict, spread_type: str,
                       prompt: bool = True) -> bool:
    """
    Place a new PCS (Bull Put Spread) or CCS (Bear Call Spread) order.

    rec: recommendation dict from scan_pcs / scan_ccs:
        short_leg: {strike, bid, ask, mid, ...}
        long_leg:  {strike, bid, ask, mid, ...}
        expiration: YYYY-MM-DD
        net_credit: float (per share)
        spread_size: float

    spread_type: "PCS" or "CCS"
    prompt: if True, show order summary and require y/n before submitting.

    Returns True on successful order placement, False otherwise.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    spread_type = spread_type.upper()
    symbol      = symbol.upper()
    opt_type    = "put"  if spread_type == "PCS" else "call"
    label       = ("Put Credit Spread (Bull Put)"
                   if spread_type == "PCS" else "Call Credit Spread (Bear Call)")

    short_leg  = rec.get("short_leg", {})
    long_leg   = rec.get("long_leg", {})
    expiration = rec.get("expiration", "")

    short_strike = float(short_leg.get("strike", 0))
    long_strike  = float(long_leg.get("strike", 0))

    # Prefer the pre-computed mid field; fall back to (bid+ask)/2
    def _mid(leg: dict) -> float:
        m = leg.get("mid")
        if m is not None:
            return round(float(m), 2)
        b = float(leg.get("bid", 0) or 0)
        a = float(leg.get("ask", 0) or 0)
        if b > 0 and a > 0:
            return round((b + a) / 2, 2)
        return round(max(b, a), 2)

    short_mid    = _mid(short_leg)
    long_mid     = _mid(long_leg)
    net_credit   = round(short_mid - long_mid, 2)
    net_ct_total = round(net_credit * 100, 2)
    spread_width = abs(short_strike - long_strike)

    if prompt:
        print(f"\n{'─' * 72}")
        print(f"  {label} Order for {symbol}")
        print(f"{'─' * 72}")
        print(f"  SELL {opt_type.upper()}  ${short_strike:.2f}  exp {expiration}"
              f"  @ ${short_mid:.2f}/sh")
        print(f"  BUY  {opt_type.upper()}  ${long_strike:.2f}  exp {expiration}"
              f"  @ ${long_mid:.2f}/sh")
        print(f"{'─' * 72}")
        print(f"  Net Credit:   ${net_credit:.2f}/sh  →  ${net_ct_total:.2f}/contract")
        print(f"  Spread Width: ${spread_width:.2f}")
        print(f"{'─' * 72}")
        answer = input("  Place this order? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborted.\n")
            return False

    short_strike_s = f"{short_strike:.4f}"
    long_strike_s  = f"{long_strike:.4f}"

    spread_legs = [
        {
            "expirationDate": expiration,
            "strike":         short_strike_s,
            "optionType":     opt_type,
            "effect":         "open",
            "action":         "sell",
            "ratio_quantity": 1,
        },
        {
            "expirationDate": expiration,
            "strike":         long_strike_s,
            "optionType":     opt_type,
            "effect":         "open",
            "action":         "buy",
            "ratio_quantity": 1,
        },
    ]

    login()
    try:
        logger.info(
            f"[{spread_type} ADD] STO ${short_strike_s} / BTO ${long_strike_s} "
            f"{opt_type.upper()} {expiration} {symbol} @ ${net_credit:.2f}/sh net credit"
        )
        result = rh.orders.order_option_spread(
            direction="credit",
            price=net_credit,
            symbol=symbol,
            quantity=1,
            spread=spread_legs,
            timeInForce="gfd",
        )
        order_id = (result or {}).get("id", "")
        state    = (result or {}).get("state", "unknown")
        if result and order_id:
            print(f"\n  ✅  {spread_type} order placed  (id={order_id}  state={state})\n")
            return True
        else:
            detail  = (result or {}).get("detail", "") or \
                      str((result or {}).get("non_field_errors", ""))
            err_msg = detail or f"Unexpected API response: {result}"
            logger.error(f"[{spread_type} ADD] Order failed: {err_msg}")
            print(f"\n  ❌  {spread_type} order failed: {err_msg}\n")
            return False
    except Exception as e:  # noqa: BLE001
        logger.error(f"[{spread_type} ADD] Exception: {e}", exc_info=True)
        print(f"\n  ❌  {spread_type} order error: {e}\n")
        return False
    finally:
        logout()


def close_spread_position(symbol: str, spread_type: str,
                          price: Optional[float] = None,
                          prompt: bool = True,
                          chain: Optional[str] = None) -> bool:
    """
    Close an open PCS or CCS spread position for SYMBOL.

    Fetches the open spread legs from Robinhood, fetches live bid/ask for each
    leg, then places a closing spread order (buy back the short, sell back the
    long).

    Limit price selection:
      - If ``price`` is provided, it is used as-is.
      - Otherwise: MIN($0.20, 20% of original net credit received, current net
        mid-point of bid/ask), floored at $0.01.
        - Original net credit = (abs(short_leg avg_price) − long_leg avg_price) / 100
          Robinhood stores avg_price as a per-contract dollar total (premium×100).
          Short legs carry a NEGATIVE value (credit received = negative cost basis);
          long legs carry a POSITIVE value (debit paid).
        - Current net mid = (short bid+ask)/2 − (long bid+ask)/2

    spread_type: "PCS" or "CCS"
    price: explicit limit price per share; None = use smart default above.
    prompt: if True, show closing order summary and require y/n.
    chain: optional chain identifier ``"$STRIKE TYPE MM/DD"`` to select a
           specific spread when multiple are open (e.g. ``"$120 PUT 5/1"``).
           If None and multiple spreads exist, an interactive numbered menu
           is shown. If None and exactly one spread exists, it is used
           automatically.

    Returns True on successful order placement, False otherwise.
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout
    from collections import defaultdict

    spread_type = spread_type.upper()
    symbol      = symbol.upper()
    opt_type    = "put"  if spread_type == "PCS" else "call"
    label       = ("Put Credit Spread (Bull Put)"
                   if spread_type == "PCS" else "Call Credit Spread (Bear Call)")

    login()
    try:
        positions = rh.options.get_open_option_positions() or []
        legs = []
        for pos in positions:
            try:
                chain_sym = (pos.get("chain_symbol") or "").upper()
                if chain_sym != symbol:
                    continue
                qty      = float(pos.get("quantity", 0))
                pos_type = (pos.get("type") or "").lower()
                if qty <= 0:
                    continue

                option_id  = pos.get("option_id", "")
                o_type     = ""
                strike     = 0.0
                expiration = pos.get("expiration_date", "")

                if option_id:
                    try:
                        instr      = rh.options.get_option_instrument_data_by_id(option_id)
                        o_type     = (instr.get("type") or "").lower()
                        strike     = float(instr.get("strike_price", 0) or 0)
                        expiration = instr.get("expiration_date", expiration) or expiration
                    except Exception:
                        pass

                if o_type != opt_type:
                    continue

                legs.append({
                    "symbol":     chain_sym,
                    "opt_type":   o_type,
                    "pos_type":   pos_type,
                    "strike":     strike,
                    "expiration": expiration,
                    "quantity":   int(qty),
                    "option_id":  option_id,
                    "avg_price":  float(pos.get("average_price", 0) or 0),
                })
            except Exception:
                continue

        if not legs:
            print(f"\nNo open {spread_type} positions found for {symbol}.\n")
            return False

        # Group by (symbol, expiration) and find first valid spread pair
        groups: dict = defaultdict(lambda: {"short": [], "long": []})
        for leg in legs:
            key  = (leg["symbol"], leg["expiration"])
            side = leg["pos_type"]
            if side in ("short", "long"):
                groups[key][side].append(leg)

        # Build ALL valid spread pairs (same sorting as show_spread_holdings)
        all_pairs: list = []
        for (sym, exp), v in groups.items():
            if not v["short"] or not v["long"]:
                continue
            if spread_type == "PCS":
                shorts = sorted(v["short"], key=lambda x: x["strike"], reverse=True)
                longs  = sorted(v["long"],  key=lambda x: x["strike"])
            else:
                shorts = sorted(v["short"], key=lambda x: x["strike"])
                longs  = sorted(v["long"],  key=lambda x: x["strike"], reverse=True)
            n = min(len(shorts), len(longs))
            for i in range(n):
                sh, lo = shorts[i], longs[i]
                if (spread_type == "PCS" and sh["strike"] > lo["strike"]) or \
                   (spread_type == "CCS" and sh["strike"] < lo["strike"]):
                    all_pairs.append((exp, sh, lo))

        if not all_pairs:
            print(f"\nNo matched {spread_type} pair found for {symbol}.\n")
            return False

        # Select which spread to close
        if chain is not None:
            try:
                chain_strike, _, chain_exp = _parse_chain(chain)
            except ValueError as e:
                print(f"\n  ❌  {e}\n")
                return False
            matched = [p for p in all_pairs
                       if abs(p[1]["strike"] - chain_strike) < 0.01 and p[0] == chain_exp]
            if not matched:
                short_lbl = "PUT" if spread_type == "PCS" else "CALL"
                print(f"\nNo {spread_type} with short {short_lbl} @ ${chain_strike:.2f} "
                      f"exp {chain_exp} found for {symbol}.\n")
                return False
            spread_pair_tuple = matched[0]
        elif len(all_pairs) == 1:
            spread_pair_tuple = all_pairs[0]
        else:
            # Multiple spreads — show a numbered menu
            sorted_pairs = sorted(all_pairs, key=lambda x: (x[0], x[1]["strike"]))
            short_lbl    = "PUT" if spread_type == "PCS" else "CALL"
            print(f"\n  Multiple {spread_type} positions found for {symbol}:")
            for i, (exp, sh, lo) in enumerate(sorted_pairs, 1):
                dte_val = _dte(exp)
                dte_str = str(dte_val) if dte_val >= 0 else "EXP"
                credit  = round((abs(float(sh.get("avg_price", 0)))
                                 - float(lo.get("avg_price", 0))) / 100, 2)
                print(f"  [{i}]  ${sh['strike']:.2f} / ${lo['strike']:.2f} {short_lbl}"
                      f"  exp {exp}  ({dte_str}d)  credit ${credit:.2f}/sh")
            print()
            while True:
                answer = input(
                    f"  Select position to close [1-{len(sorted_pairs)}] or q to abort: "
                ).strip()
                if answer.lower() == "q":
                    print("  Aborted.\n")
                    return False
                try:
                    idx = int(answer) - 1
                    if 0 <= idx < len(sorted_pairs):
                        break
                except ValueError:
                    pass
                print(f"  Please enter a number between 1 and {len(sorted_pairs)}.")
            spread_pair_tuple = sorted_pairs[idx]

        expiration, short_leg, long_leg = spread_pair_tuple
        short_strike = short_leg["strike"]
        long_strike  = long_leg["strike"]
        qty          = min(short_leg["quantity"], long_leg["quantity"])

        # Fetch live mid-price for each leg
        def _live_mid(option_id: str) -> float:
            try:
                mkt = rh.options.get_option_market_data_by_id(option_id)
                if isinstance(mkt, list):
                    mkt = mkt[0] if mkt else {}
                bid = float((mkt or {}).get("bid_price", 0) or 0)
                ask = float((mkt or {}).get("ask_price", 0) or 0)
                if bid > 0 and ask > 0:
                    return round((bid + ask) / 2, 2)
                return round(max(bid, ask), 2)
            except Exception:
                return 0.0

        short_mid = _live_mid(short_leg["option_id"])
        long_mid  = _live_mid(long_leg["option_id"])

        # Current net mid = cost to close at mid-market (positive = debit to close)
        net_mid = round(short_mid - long_mid, 2)

        # ── Limit price calculation ───────────────────────────────────────────
        if price is not None:
            limit_price  = round(price, 2)
            price_source = f"user-specified ${limit_price:.2f}/sh"
        else:
            # Original net credit received when the spread was opened.
            #
            # Robinhood stores average_price for option positions as the
            # per-contract dollar total (premium × 100 multiplier).  Short
            # legs carry a NEGATIVE value (credit received = negative cost
            # basis); long legs carry a POSITIVE value (debit paid).
            #
            # Examples for a $3.25/sh net-credit spread:
            #   short_avg = -525.00  (received $5.25/sh → $525/contract)
            #   long_avg  = +200.00  (paid    $2.00/sh → $200/contract)
            #   orig_credit = (abs(-525) - 200) / 100 = 3.25/sh ✓
            #
            # Using the raw difference (short - long) gives a large negative
            # number like -721 — that is NOT the credit per share.
            short_avg_raw = float(short_leg.get("avg_price", 0))
            long_avg_raw  = float(long_leg.get("avg_price", 0))
            orig_credit   = round(
                (abs(short_avg_raw) - long_avg_raw) / 100,
                2
            )
            pct20        = round(max(0.0, orig_credit) * 0.20, 2)
            limit_price  = round(max(0.01, min(0.20, pct20, net_mid)), 2)
            price_source = (
                f"MIN($0.20, 20%×${orig_credit:.2f}=${pct20:.2f}, "
                f"mid=${net_mid:.2f}) = ${limit_price:.2f}/sh"
            )

        direction = "debit"   # closing a credit spread always costs a debit

        if prompt:
            print(f"\n{'─' * 72}")
            print(f"  Close {label} for {symbol}  ({qty} contract(s))")
            print(f"{'─' * 72}")
            print(f"  BUY  {opt_type.upper()}  ${short_strike:.2f}  exp {expiration}"
                  f"  @ ${short_mid:.2f}/sh  (close short)  [market mid]")
            print(f"  SELL {opt_type.upper()}  ${long_strike:.2f}  exp {expiration}"
                  f"  @ ${long_mid:.2f}/sh  (close long)   [market mid]")
            print(f"{'─' * 72}")
            print(f"  Current net mid: ${net_mid:.2f}/sh")
            print(f"  Limit price:     {price_source}")
            print(f"  Order total:     ${limit_price * 100 * qty:.2f}  ({qty} contract(s))")
            print(f"{'─' * 72}")
            answer = input("  Place closing order? [y/N]: ").strip().lower()
            if answer != "y":
                print("  Aborted.\n")
                return False

        short_strike_s = f"{short_strike:.4f}"
        long_strike_s  = f"{long_strike:.4f}"

        spread_legs = [
            {
                "expirationDate": expiration,
                "strike":         short_strike_s,
                "optionType":     opt_type,
                "effect":         "close",
                "action":         "buy",
                "ratio_quantity": 1,
            },
            {
                "expirationDate": expiration,
                "strike":         long_strike_s,
                "optionType":     opt_type,
                "effect":         "close",
                "action":         "sell",
                "ratio_quantity": 1,
            },
        ]

        logger.info(
            f"[{spread_type} CLOSE] BTC ${short_strike_s} / STC ${long_strike_s} "
            f"{opt_type.upper()} {expiration} {symbol} @ ${limit_price:.2f}/sh {direction}"
        )
        result = rh.orders.order_option_spread(
            direction=direction,
            price=limit_price,
            symbol=symbol,
            quantity=qty,
            spread=spread_legs,
            timeInForce="gfd",
        )
        order_id = (result or {}).get("id", "")
        state    = (result or {}).get("state", "unknown")
        if result and order_id:
            print(f"\n  ✅  {spread_type} close order placed  (id={order_id}  state={state})\n")
            return True
        else:
            detail  = (result or {}).get("detail", "") or \
                      str((result or {}).get("non_field_errors", ""))
            err_msg = detail or f"Unexpected API response: {result}"
            logger.error(f"[{spread_type} CLOSE] Order failed: {err_msg}")
            print(f"\n  ❌  {spread_type} close failed: {err_msg}\n")
            return False

    except Exception as e:  # noqa: BLE001
        logger.error(f"[{spread_type} CLOSE] Exception: {e}", exc_info=True)
        print(f"\n  ❌  {spread_type} close error: {e}\n")
        return False
    finally:
        logout()
