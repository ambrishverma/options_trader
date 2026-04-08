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

Automatic escalation sequence (assignment avoidance):
  Safety mode  (DTE ≥ 1)   — places conservative BTC at low limit price (all future expiries)
  Rescue mode  (DTE 1-2)   — cancels all orders, rolls for max Risk/Reward ratio
  Panic mode   (DTE 0)     — cancels all orders (incl. stale rescue spreads),
                              rolls to next expiration regardless of credit

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
from datetime import date
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
    from portfolio import load_open_calls_detail_snapshot

    symbol = symbol.upper()

    # 1. Parse chain string
    try:
        strike, option_type, expiration = _parse_chain(chain_str)
    except ValueError as e:
        print(f"\n❌  {e}\n")
        return False

    # 2. Find matching open contract
    contracts = load_open_calls_detail_snapshot()
    contract = _find_contract(symbol, strike, expiration, contracts)
    if contract is None:
        print(f"\n❌  No open contract found for {symbol} {chain_str!r}.")
        print(f"     Run  --show {symbol}  to see open contracts.\n")
        return False

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

    estimated_cost = round(limit_price * 100 * quantity, 2)

    # 6. Optional confirmation prompt
    if prompt:
        print(f"\n{'─' * 62}")
        print("  BUY-TO-CLOSE ORDER SUMMARY")
        print(f"{'─' * 62}")
        print(f"  Symbol     : {symbol}")
        print(f"  Contract   : {chain_str}")
        print(f"  Quantity   : {quantity} contract(s)")
        print(f"  Bid / Ask  : ${bid:.2f} / ${ask:.2f}")
        print(f"  Limit      : ${limit_price:.2f} per share  (${estimated_cost:.2f} total est.)")
        print(f"{'─' * 62}")
        answer = input("  Proceed? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborted.\n")
            return False

    # 7. Login and place order
    print("\nLogging in to Robinhood...")
    try:
        login(force_fresh=False)
    except Exception as e:
        print(f"❌  Login failed: {e}\n")
        return False

    try:
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
    from portfolio import load_open_calls_detail_snapshot

    symbol = symbol.upper()

    # 1. Parse chain string
    try:
        strike, option_type, expiration = _parse_chain(chain_str)
    except ValueError as e:
        print(f"\n❌  {e}\n")
        return False

    # 2. Find matching open contract
    contracts = load_open_calls_detail_snapshot()
    contract = _find_contract(symbol, strike, expiration, contracts)
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

    # 3. Live price + current contract bid/ask (used for display and default net)
    live_price = _get_live_price(symbol)
    btc_bid, btc_ask, btc_mid = _get_option_bid_ask(symbol, strike, option_type, expiration)
    if btc_bid == 0 and btc_ask == 0 and price is None:
        print(f"\n⚠️   Could not fetch live bid/ask for {symbol} {chain_str}.")
        print("     Specify --price to override and proceed.\n")
        return False

    # 4. Find next available expiration
    try:
        ticker = yf.Ticker(_yahoo_symbol(symbol))
        all_expirations = list(ticker.options)  # sorted ascending
    except Exception as e:
        print(f"\n❌  Could not fetch option expirations for {symbol}: {e}\n")
        return False

    future_exps = [e for e in all_expirations if e > expiration]
    if not future_exps:
        print(f"\n❌  No future expirations available for {symbol} beyond {expiration}.\n")
        return False

    next_expiration = future_exps[0]

    # 5. Find target strike at next expiration
    target_strike: Optional[float] = None
    sto_bid, sto_ask, sto_mid = 0.0, 0.0, 0.0
    try:
        chain = ticker.option_chain(next_expiration)
        df = (chain.calls if option_type == "call" else chain.puts).copy()
        df["strike"] = df["strike"].astype(float)
        df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))
        df["ask"]    = df["ask"].apply(lambda x: _safe_float(x))

        if rescue:
            # Rescue: scan all strikes >= current for best Risk/Reward ratio
            # R/R = net_credit / max(new_strike - live_price, ε)
            # Higher R/R = better (strikes at/below live price get near-infinite R/R)
            df["mid"] = (df["bid"] + df["ask"]) / 2.0
            candidates = df[(df["strike"] >= strike - 0.01) & (df["bid"] > 0)].copy()
            if candidates.empty:
                print(f"\n❌  No strikes >= ${strike:g} with a non-zero bid at {next_expiration}.\n")
                return False
            candidates = candidates.copy()
            candidates["net_credit"] = candidates["mid"] - btc_mid
            credit_pos = candidates[candidates["net_credit"] > 0]
            if credit_pos.empty:
                best_nc = candidates.loc[candidates["net_credit"].idxmax()]
                print(
                    f"\n⚠️   No credit available for {symbol} rescue roll — "
                    f"best strike ${float(best_nc['strike']):g} yields "
                    f"${float(best_nc['net_credit']):.2f} net. No order placed.\n"
                )
                return False
            _EPS = 0.001
            credit_pos = credit_pos.copy()
            credit_pos["risk"]     = (credit_pos["strike"] - live_price).clip(lower=_EPS)
            credit_pos["rr_ratio"] = credit_pos["net_credit"] / credit_pos["risk"]
            best_row = credit_pos.loc[credit_pos["rr_ratio"].idxmax()]
            target_strike = float(best_row["strike"])
        else:
            # Normal: prefer exact same strike, fall back to nearest OTM
            exact = df[abs(df["strike"] - strike) < 0.01]
            if not exact.empty and float(exact.iloc[0]["bid"]) > 0:
                target_strike = float(exact.iloc[0]["strike"])
            else:
                ref_price = live_price if live_price > 0 else strike
                otm = df[df["strike"] >= ref_price].sort_values("strike")
                otm_valid = otm[otm["bid"] > 0]
                if otm_valid.empty:
                    print(f"\n❌  No OTM {option_type} options with a non-zero bid found "
                          f"at expiration {next_expiration}.\n")
                    return False
                target_strike = float(otm_valid.iloc[0]["strike"])

        sto_bid, sto_ask, sto_mid = _get_option_bid_ask(
            symbol, target_strike, option_type, next_expiration
        )
    except Exception as e:
        print(f"\n❌  Failed to find target strike at {next_expiration}: {e}\n")
        return False

    # 6. Compute net spread price
    #    --price overrides the net; otherwise derive from live mids.
    #    direction="credit" if we collect net premium, "debit" if we pay net.
    #    Robinhood requires abs_net > 0; floor at $0.01.
    if price is not None:
        net_price = round(price, 2)
    else:
        net_price = round(sto_mid - btc_mid, 2)

    direction = "credit" if net_price >= 0 else "debit"
    abs_net   = max(round(abs(net_price), 2), 0.01)
    net_label = f"${abs_net:.2f} {direction}"
    strike_changed = abs(target_strike - strike) > 0.01

    # 7. Optional confirmation prompt
    if prompt:
        print(f"\n{'─' * 65}")
        print("  ROLL FORWARD ORDER SUMMARY  (single multi-leg spread order)")
        print(f"{'─' * 65}")
        print(f"  Symbol       : {symbol}  (live: ${live_price:.2f})")
        print(f"  Leg 1 — BTC  : ${strike:g} {option_type.upper()} "
              f"{_fmt_exp(expiration)}  "
              f"bid ${btc_bid:.2f} / ask ${btc_ask:.2f}  →  pay ~${btc_mid:.2f}")
        print(f"  Leg 2 — STO  : ${target_strike:g} {option_type.upper()} "
              f"{_fmt_exp(next_expiration)}  "
              f"bid ${sto_bid:.2f} / ask ${sto_ask:.2f}  →  collect ~${sto_mid:.2f}")
        print(f"  Net spread   : {net_label}  (limit for the combined order)")
        print(f"  Quantity     : {quantity} contract(s)")
        if strike_changed:
            print(f"  ⚠️  Strike changed: ${strike:g} → ${target_strike:g} "
                  "(exact strike unavailable at next expiry)")
        print(f"{'─' * 65}")
        answer = input("  Proceed? [y/N]: ").strip().lower()
        if answer != "y":
            print("  Aborted.\n")
            return False
    else:
        strike_note = f" (strike → ${target_strike:g})" if strike_changed else ""
        print(f"\nRolling {symbol} ${strike:g} {option_type.upper()} "
              f"{_fmt_exp(expiration)} → "
              f"${target_strike:g} {option_type.upper()} "
              f"{_fmt_exp(next_expiration)}{strike_note}")
        print(f"  BTC: ~${btc_mid:.2f}  |  STO: ~${sto_mid:.2f}"
              f"  |  Net: {net_label}\n")

    # 8. Login and place single atomic spread order
    print("Logging in to Robinhood...")
    try:
        login(force_fresh=False)
    except Exception as e:
        print(f"❌  Login failed: {e}\n")
        return False

    try:
        # 8a. Rescue mode: cancel ALL open orders for this contract before rolling
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
# Panic-mode roll execution (called automatically by the daily pipeline)
# ─────────────────────────────────────────────────────────────────────────────

def execute_panic_rolls(
    open_calls_detail: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Panic mode: called by the daily pipeline for covered-call contracts
    expiring TODAY that are in-the-money.

    For each such contract:
      1. Cancel any outstanding BTC order for that contract
      2. Wait 30 seconds (only if a BTC order was found and cancelled)
      3. Submit an atomic roll-forward spread order to the next available
         expiration at the same strike (or nearest OTM if exact unavailable)

    Returns a list of result dicts — one per DTE-0 ITM contract found —
    consumed by the emailer to populate the panic sub-section in the report.

    Each result dict contains:
      symbol, name, strike, expiration, next_expiration, target_strike,
      next_dte, quantity, live_price, itm_by, btc_cancelled, success,
      order_id, net_price, direction, net_label, error
    """
    import robin_stocks.robinhood as rh
    from auth import login, logout

    if name_map is None:
        name_map = {}

    today_str = str(date.today())

    # Detect DTE-0 ITM covered calls
    panic_contracts = [
        c for c in open_calls_detail
        if c.get("expiration", "") == today_str
        and live_prices.get(c.get("symbol", "").upper(), 0) >= c.get("strike", float("inf"))
    ]

    if not panic_contracts:
        return []

    logger.warning(
        f"[PANIC MODE] {len(panic_contracts)} DTE-0 ITM contract(s) detected: "
        + ", ".join(f"{c['symbol']} ${c['strike']:g}" for c in panic_contracts)
    )

    def _make_result(c: dict, lp: float) -> dict:
        """Scaffold an empty result dict for a contract."""
        sym = c["symbol"].upper()
        strike = float(c["strike"])
        return {
            "symbol":          sym,
            "name":            name_map.get(sym, sym),
            "strike":          strike,
            "expiration":      c["expiration"],
            "next_expiration": "",
            "target_strike":   0.0,
            "next_dte":        0,
            "quantity":        int(c.get("quantity", 1)),
            "live_price":      lp,
            "itm_by":          round(lp - strike, 2),
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

            future_exps = [e for e in all_expirations if e > expiration]
            if not future_exps:
                r["error"] = f"No future expirations available beyond {expiration}"
                logger.error(f"[PANIC MODE] {sym}: {r['error']}")
                results.append(r)
                continue

            next_expiration = future_exps[0]
            next_dte = (date.fromisoformat(next_expiration) - date.today()).days

            target_strike = None
            btc_mid = sto_mid = 0.0
            try:
                chain_data = ticker.option_chain(next_expiration)
                df = chain_data.calls.copy()
                df["strike"] = df["strike"].astype(float)
                df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))

                exact = df[abs(df["strike"] - strike) < 0.01]
                if not exact.empty and float(exact.iloc[0]["bid"]) > 0:
                    target_strike = float(exact.iloc[0]["strike"])
                else:
                    ref_price = live_price if live_price > 0 else strike
                    otm = df[df["strike"] >= ref_price].sort_values("strike")
                    otm_valid = otm[otm["bid"] > 0]
                    if otm_valid.empty:
                        r["error"] = f"No OTM calls with non-zero bid at {next_expiration}"
                        logger.error(f"[PANIC MODE] {sym}: {r['error']}")
                        results.append(r)
                        continue
                    target_strike = float(otm_valid.iloc[0]["strike"])

                # BTC leg may have 0 bid/ask pre-market at DTE-0 — that is fine;
                # net will be based on STO mid alone (intrinsic is effectively 0)
                _, _, btc_mid = _get_option_bid_ask(sym, strike, "call", expiration)
                _, _, sto_mid = _get_option_bid_ask(sym, target_strike, "call", next_expiration)
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
                    "optionType":     "call",
                    "effect":         "close",
                    "action":         "buy",
                    "ratio_quantity": 1,
                },
                {
                    "expirationDate": next_expiration,
                    "strike":         f"{target_strike:.4f}",
                    "optionType":     "call",
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
    open_calls_detail: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Safety mode: called by the daily pipeline for ALL covered-call contracts
    with DTE >= 1 that have no open BTC order (no upper DTE limit).

    For each such contract, places a buy-to-close GTC limit order at:
        MIN($0.20,  10% of original purchase price,  live mid of bid/ask)
    A random delay of 5–20 seconds is inserted between consecutive orders
    to avoid Robinhood rate-limiting.

    Returns a list of result dicts — one per candidate contract — consumed
    by the emailer to populate the safety sub-section in Section 3.

    Each result dict contains:
      symbol, name, strike, expiration, dte, quantity, live_price,
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
    # No upper DTE limit — every open covered call without protection is a candidate.
    candidates = []
    for c in open_calls_detail:
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
            sym           = c["symbol"].upper()
            strike        = float(c["strike"])
            expiration    = c["expiration"]
            quantity      = int(c.get("quantity", 1))
            purchase_price = abs(_safe_float(c.get("purchase_price", 0)))
            live_price    = live_prices.get(sym, 0.0)

            # Random inter-order delay (skip before the very first order)
            if idx > 0:
                delay = random.randint(5, 20)
                logger.info(
                    f"[SAFETY MODE] Waiting {delay}s before next order..."
                )
                time.sleep(delay)

            # Fetch live bid / ask
            bid, ask, mid = _get_option_bid_ask(sym, strike, "call", expiration)

            # BTC limit price = MIN($0.20, 10% of purchase, live mid)
            price_candidates = [0.20]
            if purchase_price > 0:
                price_candidates.append(round(purchase_price * 0.10, 2))
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
                    optionType="call",
                    timeInForce="gtc",
                )
                order_id = (order or {}).get("id", "")
                if order and order_id:
                    r["success"]  = True
                    r["order_id"] = order_id
                    logger.info(
                        f"[SAFETY MODE] ✅ BTC placed for {sym} ${strike:g} "
                        f"exp {expiration} DTE={dte} at ${btc_price:.2f}  id={order_id}"
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
    open_calls_detail: List[dict],
    live_prices: dict,
    name_map: dict = None,
    dry_run: bool = False,
) -> List[dict]:
    """
    Rescue mode: called by the daily pipeline for covered-call contracts
    expiring in the next 1–2 days that are in-the-money.

    For each such contract:
      1. Find the next available expiration; scan all strikes >= current and
         pick the one that maximises net credit (sto_mid - btc_mid).
         Record as skipped=True (no order placed) if no positive credit found.
      2. Cancel ALL outstanding orders for the contract (any side/effect)
      3. Wait 30 seconds (only if any orders were cancelled)
      4. Submit an atomic roll-forward spread at the net mid price

    Returns a list of result dicts — one per DTE-1-2 ITM contract found —
    consumed by the emailer to populate the rescue sub-section in Section 2.
    Skipped contracts (no credit available) are included with skipped=True
    and are NOT removed from roll_candidates in the scheduler (safety mode
    may still protect them with a BTC order).

    Each result dict contains:
      symbol, name, strike, expiration, dte, next_expiration, target_strike,
      next_dte, quantity, live_price, itm_by, orders_cancelled,
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
    for c in open_calls_detail:
        exp = c.get("expiration", "")
        if not exp or exp <= today_str:
            continue   # skip DTE-0 (handled by panic mode) and expired
        try:
            dte = (date.fromisoformat(exp) - today).days
        except ValueError:
            continue
        if dte not in (1, 2):
            continue
        sym = c.get("symbol", "").upper()
        if live_prices.get(sym, 0) >= c.get("strike", float("inf")):
            rescue_contracts.append((c, dte))

    if not rescue_contracts:
        return []

    logger.info(
        f"[RESCUE MODE] {len(rescue_contracts)} DTE-1-2 ITM contract(s) detected: "
        + ", ".join(f"{c['symbol']} ${c['strike']:g} DTE={dte}" for c, dte in rescue_contracts)
    )

    def _make_result(c: dict, lp: float, dte: int) -> dict:
        sym    = c["symbol"].upper()
        strike = float(c["strike"])
        return {
            "symbol":           sym,
            "name":             name_map.get(sym, sym),
            "strike":           strike,
            "expiration":       c["expiration"],
            "dte":              dte,
            "next_expiration":  "",
            "target_strike":    0.0,
            "next_dte":         0,
            "quantity":         int(c.get("quantity", 1)),
            "live_price":       lp,
            "itm_by":           round(lp - strike, 2),
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

            # ── Step 2: Find best Risk/Reward strike >= current ───────────────
            # Reward = net credit (sto_mid - btc_mid)
            # Risk   = new_strike - live_price  (gap from stock price to new strike)
            # R/R    = Reward / max(Risk, ε)   — higher is better
            # Strikes at or below live_price get near-infinite R/R (ε in denominator).
            # Only credit-positive strikes are considered; if none exist, skip.
            target_strike = None
            sto_mid       = 0.0
            btc_mid       = 0.0

            try:
                chain_data = ticker.option_chain(next_expiration)
                df = chain_data.calls.copy()
                df["strike"] = df["strike"].astype(float)
                df["bid"]    = df["bid"].apply(lambda x: _safe_float(x))
                df["ask"]    = df["ask"].apply(lambda x: _safe_float(x))
                df["mid"]    = (df["bid"] + df["ask"]) / 2.0

                # Only consider strikes >= current strike with a non-zero bid
                candidates = df[(df["strike"] >= strike - 0.01) & (df["bid"] > 0)].copy()

                # BTC mid for the current (closing) leg
                _, _, btc_mid = _get_option_bid_ask(sym, strike, "call", expiration)

                if candidates.empty:
                    r["skipped"] = True
                    r["error"]   = (
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
                _EPS = 0.001   # floor to avoid division by zero for at/below-price strikes
                credit_pos = credit_pos.copy()
                credit_pos["risk"]     = (credit_pos["strike"] - live_price).clip(lower=_EPS)
                credit_pos["rr_ratio"] = credit_pos["net_credit"] / credit_pos["risk"]
                best_row = credit_pos.loc[credit_pos["rr_ratio"].idxmax()]

                target_strike = float(best_row["strike"])

                # Confirm with a live bid/ask fetch for accurate mid
                _, _, sto_mid_live = _get_option_bid_ask(
                    sym, target_strike, "call", next_expiration
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
                    "optionType":     "call",
                    "effect":         "close",
                    "action":         "buy",
                    "ratio_quantity": 1,
                },
                {
                    "expirationDate": next_expiration,
                    "strike":         f"{target_strike:.4f}",
                    "optionType":     "call",
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
