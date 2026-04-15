#!/usr/bin/env python3
"""
Options Trader — CLI Entry Point
Usage:
  python main.py --setup                                           # One-time setup wizard
  python main.py --run                                             # Run the full covered-call pipeline now
  python main.py --dry-run                                         # Full covered-call pipeline, no email sent
  python main.py --collar                                          # Run collar pipeline and send email now
  python main.py --collar-dry-run                                  # Collar pipeline, no email sent (saves HTML preview)
  python main.py --collar SYMBOL --find                            # Find best collar for SYMBOL (on-demand scan)
  python main.py --collar SYMBOL --find --weeks 4 16              # On-demand scan with custom week range
  python main.py --collar SYMBOL --add                             # Place 1 collar contract for SYMBOL (prompts confirm)
  python main.py --collar SYMBOL --add --contracts 3              # Place 3 collar contracts for SYMBOL
  python main.py --collar SYMBOL --show                            # Show existing collar holdings for SYMBOL
  python main.py --collar --show                                   # Show all collar holdings in portfolio
  python main.py --collar SYMBOL --roll                            # Find roll-up/roll-out options for SYMBOL collar
  python main.py --collar SYMBOL --roll --weeks 4 26              # Roll options with custom week range
  python main.py --cc SYMBOL                                       # On-demand covered-call scan for any symbol
  python main.py --cc SYMBOL --buffer-size 5 --target-premium 1.5 # On-demand CC scan with custom params
  python main.py --ccs SYMBOL                                      # On-demand Call Credit Spread scan (--find)
  python main.py --ccs SYMBOL --find                               # Find best CCS for SYMBOL (explicit)
  python main.py --ccs SYMBOL --add                                # Place order for recommended CCS
  python main.py --ccs --show                                      # Show all CCS holdings in portfolio
  python main.py --ccs SYMBOL --show                               # Show CCS holdings for SYMBOL
  python main.py --ccs SYMBOL --close                              # Close existing CCS (price = MIN($0.20, 10% of credit, mid))
  python main.py --ccs SYMBOL --close --price 0.10               # Close CCS at a specific limit price
  python main.py --ccs SYMBOL --spread-size 2 8 --target-premium 2  # CCS scan: spread range $2–$8
  python main.py --pcs SYMBOL                                      # On-demand Put Credit Spread scan (--find)
  python main.py --pcs SYMBOL --find                               # Find best PCS for SYMBOL (explicit)
  python main.py --pcs SYMBOL --add                                # Place order for recommended PCS
  python main.py --pcs --show                                      # Show all PCS holdings in portfolio
  python main.py --pcs SYMBOL --show                               # Show PCS holdings for SYMBOL
  python main.py --pcs SYMBOL --close                              # Close existing PCS (price = MIN($0.20, 20% of credit, mid))
  python main.py --pcs SYMBOL --close --price 0.10               # Close PCS at a specific limit price
  python main.py --pcs SYMBOL --close --chain "$120 PUT 5/1"     # Close specific PCS spread by chain
  python main.py --show SYMBOL                                     # Show open contracts for SYMBOL (ITM/OTM status)
  python main.py --buy SYMBOL --chain "$95 CALL 5/15"              # Buy-to-close at mid-price
  python main.py --buy SYMBOL --chain "$95 CALL 5/15" --price 2.50 # Buy-to-close at a specific price
  python main.py --roll SYMBOL --chain "$95 CALL 5/15"                    # Roll to next expiry at nearest OTM strike
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --prompt           # Roll with confirmation prompt
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --rescue           # Roll at max-credit strike ≥ current (credit-only)
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --rescue --prompt  # Rescue roll with confirmation
  python main.py --optimize                                        # On-demand optimize: roll contracts that gained >40%
  python main.py --optimize TSLA                                   # Optimize only TSLA contracts
  python main.py --optimize --min-gain 30                          # Lower trigger to 30% gain
  python main.py --optimize --date-range 30 --prompt               # Wider window + confirm each roll
  python main.py --report                                          # Options trade report for today (print + email)
  python main.py --report 04/09                                    # Report for a specific date
  python main.py --report 04/01-04/09                              # Report for a date range
  python main.py --report --no-email                               # Print today's report to console only, no email
  python main.py --pull-portfolio                                  # Manually pull portfolio from Robinhood
  python main.py --status                                          # Show health / last run info
  python main.py --schedule                                        # Start the background scheduler daemon
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Optional

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent))


def check_env():
    """Quick pre-flight: .env must exist before any command except --setup."""
    env_file = Path(__file__).parent / ".env"
    if not env_file.exists():
        print("\n❌  No .env file found.")
        print("   Run  python main.py --setup  to complete first-time setup.\n")
        sys.exit(1)


def cmd_setup():
    from setup_wizard import run_setup_wizard
    run_setup_wizard()


def cmd_run(dry_run: bool = False):
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_pipeline
    run_pipeline(dry_run=dry_run)


def cmd_collar(dry_run: bool = False):
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_collar_pipeline_and_email
    run_collar_pipeline_and_email(dry_run=dry_run)


def cmd_collar_find(symbol: str, weeks_min: int, weeks_max: int):
    """On-demand collar scan for a single symbol (--collar SYMBOL --find)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_collar_on_demand_and_preview
    run_collar_on_demand_and_preview(symbol, weeks_min, weeks_max)


def cmd_collar_show(symbol: Optional[str] = None):
    """Show existing collar holdings from Robinhood portfolio."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_collar_holdings
    show_collar_holdings(symbol)


def cmd_collar_add(symbol: str, weeks_min: int, weeks_max: int, contracts: int = 1):
    """Find best collar for SYMBOL and interactively place the order."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from collar import run_collar_on_demand
    from trader import place_collar_order

    config = load_config()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7

    result = run_collar_on_demand(symbol, dte_min, dte_max)
    recs   = result.get("recommendations", [])

    if not recs:
        print(f"\nNo collar recommendations found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return

    best = recs[0]
    place_collar_order(symbol, best, prompt=True, contracts_override=contracts)


def cmd_collar_roll(symbol: str, weeks_min: int, weeks_max: int):
    """Find roll-up / roll-out collar options for an existing collar position."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from collar import find_collar_roll_options
    from trader import show_collar_holdings, _get_live_price
    import robin_stocks.robinhood as rh
    from auth import login, logout

    symbol = symbol.upper()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7

    # Fetch current collar strikes to constrain the roll-up search
    min_call_strike = 0.0
    min_put_strike  = 0.0
    print(f"\nFetching current collar positions for {symbol}...")
    login()
    try:
        positions = rh.options.get_open_option_positions() or []
        for pos in positions:
            if (pos.get("chain_symbol") or "").upper() != symbol:
                continue
            if float(pos.get("quantity", 0)) <= 0:
                continue
            option_id = pos.get("option_id", "")
            if not option_id:
                continue
            try:
                instr       = rh.options.get_option_instrument_data_by_id(option_id)
                opt_type    = (instr.get("type") or "").lower()
                pos_type    = (pos.get("type") or "").lower()
                strike      = float(instr.get("strike_price", 0) or 0)
                if opt_type == "call" and pos_type == "short" and strike > min_call_strike:
                    min_call_strike = strike
                elif opt_type == "put" and pos_type == "long" and strike > min_put_strike:
                    min_put_strike  = strike
            except Exception:
                continue
    finally:
        logout()

    if min_call_strike > 0 or min_put_strike > 0:
        print(f"  Current collar: call=${min_call_strike:.2f} / put=${min_put_strike:.2f}")
        print(f"  Scanning for roll options with higher strikes...")
    else:
        print(f"  No existing collar found for {symbol} — showing all available collars.")

    result = find_collar_roll_options(
        symbol, dte_min, dte_max,
        min_call_strike=min_call_strike,
        min_put_strike=min_put_strike,
    )
    recs = result.get("recommendations", [])

    if not recs:
        print(f"\nNo roll options found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return

    if result.get("roll_constrained"):
        print(f"\n  Note: No strictly higher strikes available — showing best available collars.\n")

    print(f"\nRoll options for {symbol}  ({len(recs)} found, {weeks_min}–{weeks_max} week window):")
    print("─" * 80)
    for i, r in enumerate(recs, 1):
        cl = r.get("call_leg", {})
        pl = r.get("put_leg", {})
        net = r.get("net_gain_per_share", 0)
        cc_exp = r.get("cc_expiration", r.get("expiration", ""))
        lp_exp = r.get("lp_expiration", r.get("expiration", ""))
        cc_dte = r.get("cc_dte", r.get("dte", 0))
        lp_dte = r.get("lp_dte", r.get("dte", 0))
        print(f"  [{i}] CALL ${cl.get('strike', 0):.2f} ({cc_exp}/{cc_dte}d)  |  "
              f"PUT ${pl.get('strike', 0):.2f} ({lp_exp}/{lp_dte}d)  |  "
              f"Net ${net:+.2f}/sh")
    print()


def cmd_cc(symbol: str, buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand covered-call scan for a single symbol."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_cc_on_demand_and_preview
    run_cc_on_demand_and_preview(
        symbol,
        buffer_size_pct=buffer_size,
        target_premium=target_premium,
        weeks_min=weeks_min,
        weeks_max=weeks_max,
    )


def cmd_ccs_find(symbol: str, spread_size_min: float, spread_size_max: float,
                 buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Call Credit Spread scan for a single symbol (--find)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_ccs_on_demand_and_preview
    run_ccs_on_demand_and_preview(
        symbol,
        spread_size_min=spread_size_min,
        spread_size_max=spread_size_max,
        buffer_size_pct=buffer_size,
        target_premium=target_premium,
        weeks_min=weeks_min,
        weeks_max=weeks_max,
    )


def cmd_ccs_add(symbol: str, spread_size_min: float, spread_size_max: float,
                buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """Find best CCS for SYMBOL and interactively place the order (--add)."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from spread_scanner import scan_ccs
    from trader import place_spread_order

    config = load_config()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7
    short_otm    = buffer_size if buffer_size is not None else float(config.get("spread_short_otm_pct", 10.0))
    min_oi       = int(config.get("spread_min_open_interest", 2))
    size_min_pct = float(config.get("spread_size_min_pct", 1.0))
    size_max_pct = float(config.get("spread_size_max_pct", 10.0))
    prem_pct     = float(config.get("spread_min_premium_pct", 1.0))

    rec, _ = scan_ccs(
        symbol,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        target_premium=target_premium,
        dte_min=dte_min, dte_max=dte_max,
        short_otm_pct=short_otm, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
        min_premium_pct=prem_pct,
    )
    if not rec:
        print(f"\nNo qualifying CCS found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return
    place_spread_order(symbol, rec, spread_type="CCS", prompt=True)


def cmd_ccs_show(symbol: Optional[str] = None):
    """Show existing CCS (Call Credit Spread) holdings from portfolio (--show)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_spread_holdings
    show_spread_holdings("CCS", symbol)


def cmd_ccs_close(symbol: str, price: Optional[float] = None,
                  chain: Optional[str] = None):
    """Close an existing CCS position for SYMBOL (--close)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import close_spread_position
    close_spread_position(symbol, spread_type="CCS", price=price, prompt=True,
                          chain=chain)


# Keep original name as alias for backward compatibility within this file
def cmd_ccs(symbol: str, spread_size_min: float, spread_size_max: float,
            buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Call Credit Spread scan (alias for cmd_ccs_find)."""
    cmd_ccs_find(symbol, spread_size_min, spread_size_max, buffer_size,
                 target_premium, weeks_min, weeks_max)


def cmd_pcs_find(symbol: str, spread_size_min: float, spread_size_max: float,
                 buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Put Credit Spread scan for a single symbol (--find)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_pcs_on_demand_and_preview
    run_pcs_on_demand_and_preview(
        symbol,
        spread_size_min=spread_size_min,
        spread_size_max=spread_size_max,
        buffer_size_pct=buffer_size,
        target_premium=target_premium,
        weeks_min=weeks_min,
        weeks_max=weeks_max,
    )


def cmd_pcs_add(symbol: str, spread_size_min: float, spread_size_max: float,
                buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """Find best PCS for SYMBOL and interactively place the order (--add)."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from spread_scanner import scan_pcs
    from trader import place_spread_order

    config = load_config()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7
    short_otm    = buffer_size if buffer_size is not None else float(config.get("spread_short_otm_pct", 10.0))
    min_oi       = int(config.get("spread_min_open_interest", 2))
    size_min_pct = float(config.get("spread_size_min_pct", 1.0))
    size_max_pct = float(config.get("spread_size_max_pct", 10.0))
    prem_pct     = float(config.get("spread_min_premium_pct", 1.0))

    rec, _ = scan_pcs(
        symbol,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        target_premium=target_premium,
        dte_min=dte_min, dte_max=dte_max,
        short_otm_pct=short_otm, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
        min_premium_pct=prem_pct,
    )
    if not rec:
        print(f"\nNo qualifying PCS found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return
    place_spread_order(symbol, rec, spread_type="PCS", prompt=True)


def cmd_pcs_show(symbol: Optional[str] = None):
    """Show existing PCS (Put Credit Spread) holdings from portfolio (--show)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_spread_holdings
    show_spread_holdings("PCS", symbol)


def cmd_pcs_close(symbol: str, price: Optional[float] = None,
                  chain: Optional[str] = None):
    """Close an existing PCS position for SYMBOL (--close)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import close_spread_position
    close_spread_position(symbol, spread_type="PCS", price=price, prompt=True,
                          chain=chain)


# Keep original name as alias for backward compatibility within this file
def cmd_pcs(symbol: str, spread_size_min: float, spread_size_max: float,
            buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Put Credit Spread scan (alias for cmd_pcs_find)."""
    cmd_pcs_find(symbol, spread_size_min, spread_size_max, buffer_size,
                 target_premium, weeks_min, weeks_max)


def cmd_show(symbol: str):
    """Show open covered-call contracts for a symbol with live ITM/OTM state."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_open_contracts
    show_open_contracts(symbol)


def cmd_buy(symbol: str, chain: str, price: Optional[float], prompt: bool):
    """Buy-to-close an open covered-call contract."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import buy_to_close
    buy_to_close(symbol, chain, price=price, prompt=prompt)


def cmd_roll(symbol: str, chain_str: str, price: float = None,
             prompt: bool = False, rescue: bool = False):
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import roll_forward
    roll_forward(symbol, chain_str, price=price, prompt=prompt, rescue=rescue)


def cmd_optimize(
    symbol: Optional[str] = None,
    dry_run: bool = False,
    min_gain_pct: float = 40.0,
    date_range_days: int = 10,
    prompt: bool = False,
):
    """
    Run optimize mode on-demand: roll UP (CALL) or DOWN (PUT) for any open
    short contract whose current option price has gained >min_gain_pct% vs. purchase price.

    Loads today's (or most recent) portfolio snapshot, fetches live prices,
    then calls execute_optimize_rolls — exactly as the daily pipeline does.

    Args:
        symbol:          If provided, filter to only that ticker.  None = all contracts.
        dry_run:         If True, find and report candidates but do NOT place orders.
        min_gain_pct:    Minimum % gain vs. purchase price to trigger the roll (default 40.0).
        date_range_days: Max days beyond current expiration to scan for new contracts (default 10).
        prompt:          If True, ask for y/n confirmation before placing each roll.
    """
    check_env()
    from utils import setup_logging
    setup_logging()

    from portfolio import load_open_calls_detail_snapshot, load_open_puts_detail_snapshot
    open_calls_detail = load_open_calls_detail_snapshot()
    open_puts_detail  = load_open_puts_detail_snapshot()
    open_short_contracts = open_calls_detail + open_puts_detail

    if not open_short_contracts:
        print("\n❌  No open contracts found in snapshot.")
        print("     Run  --pull-portfolio  to refresh, or wait for the 2:30 AM pull.\n")
        return

    # Optionally filter to one symbol
    if symbol:
        sym_upper = symbol.upper()
        open_short_contracts = [
            c for c in open_short_contracts
            if c.get("symbol", "").upper() == sym_upper
        ]
        if not open_short_contracts:
            print(f"\n❌  No open contracts found for {sym_upper} in snapshot.\n")
            return

    # Fetch live prices for all relevant symbols
    import yfinance as yf
    symbols = list({c.get("symbol", "").upper() for c in open_short_contracts})
    live_prices: dict = {}
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="2d")
            if not hist.empty:
                live_prices[sym] = float(hist["Close"].iloc[-1])
            else:
                fi = ticker.fast_info
                live_prices[sym] = float(
                    getattr(fi, "last_price", 0) or getattr(fi, "previous_close", 0) or 0
                )
        except Exception:
            live_prices[sym] = 0.0

    from trader import execute_optimize_rolls
    results = execute_optimize_rolls(
        open_short_contracts, live_prices,
        dry_run=dry_run,
        min_gain_pct=min_gain_pct,
        date_range_days=date_range_days,
        prompt=prompt,
    )

    if not results:
        print(
            f"\n✅  No contracts triggered optimize mode today "
            f"(none gained >{min_gain_pct:.0f}% vs. original purchase price).\n"
        )
        return

    # Print summary
    mode_label = " [DRY RUN]" if dry_run else ""
    print(f"\n{'─' * 68}")
    print(f"  OPTIMIZE ROLL RESULTS{mode_label}")
    print(f"{'─' * 68}")

    acted   = [r for r in results if not r.get("skipped")]
    skipped = [r for r in results if r.get("skipped")]

    for r in acted:
        sym_label  = f"{r['symbol']} ${r['strike']:g} {r['opt_type'].upper()}"
        direction  = "↑" if r["opt_type"] == "call" else "↓"
        target_str = (
            f"→ ${r['target_strike']:g} {direction} exp "
            f"{r['next_expiration'][5:].replace('-','/')}"
            if r.get("target_strike") else ""
        )
        gain_str  = f"+{r['gain_pct']:.0f}%"
        if r.get("success"):
            print(
                f"  ✅  {sym_label} {target_str}  {gain_str} gain  "
                f"{r['net_label']}  id={r['order_id'][:8] if r.get('order_id') else 'n/a'}"
            )
        else:
            print(f"  ❌  {sym_label}  FAILED — {r['error']}")

    for r in skipped:
        sym_label = f"{r['symbol']} ${r['strike']:g} {r['opt_type'].upper()}"
        print(
            f"  ⏭   {sym_label}  +{r['gain_pct']:.0f}% gain — skipped: {r['error']}"
        )

    n_ok   = sum(1 for r in acted if r.get("success"))
    n_fail = len(acted) - n_ok
    print(f"\n  {n_ok} rolled ✅  {n_fail} failed ❌  {len(skipped)} skipped (no credit)\n")


def cmd_report(date_arg: Optional[str] = None, dry_run: bool = False,
               no_email: bool = False):
    """Fetch filled options orders for the date range, print summary, and email.

    Args:
        date_arg:  Date or range string (MM/DD or MM/DD-MM/DD), or None for today.
        dry_run:   If True, skip sending the email (legacy flag, same as no_email).
        no_email:  If True, print to console only — do not send email.
    """
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from reporter import build_options_report
    from report_emailer import send_options_report_email

    report = build_options_report(date_arg)

    # Print to console
    start = report["start_date"]
    end   = report["end_date"]
    date_label = start if start == end else f"{start} → {end}"
    print(f"\n📋  Options Trade Report — {date_label}")
    print(f"    Orders:       {report['order_count']}")
    print(f"    Total Credit: ${report['total_credit']:,.2f}")
    print(f"    Total Debit:  ${report['total_debit']:,.2f}")
    net = report["net_gain"]
    if net >= 0:
        net_label = f"Net Gain:     +${net:,.2f}"
    else:
        net_label = f"Net Loss:     -${abs(net):,.2f}"
    print(f"    {net_label}")

    if report["orders"]:
        print()
        header = f"  {'Date':<12} {'Symbol':<8} {'Type':<6} {'Side':<5} {'Strike':>8}  {'Expiry':<12} {'Qty':>4}  {'Price':>7}  {'Premium':>10}  Dir"
        print(header)
        print("  " + "-" * (len(header) - 2))
        for o in report["orders"]:
            dir_label = "CREDIT" if o["direction"] == "credit" else "DEBIT "
            print(
                f"  {o['date']:<12} {o['symbol']:<8} {o['type']:<6} {o['side'].upper():<5} "
                f"${o['strike']:>7.2f}  {o['expiration']:<12} {o['quantity']:>4}  "
                f"${o['price']:>6.2f}  ${o['premium']:>9.2f}  {dir_label}"
            )

    # Send email unless suppressed
    if no_email or dry_run:
        print("\n  (email suppressed — --no-email / --dry-run)\n")
        return

    config = load_config()
    recipient = config.get("recipient_email", "")
    send_options_report_email(report, recipient_email=recipient, dry_run=False)


def cmd_pull_portfolio():
    check_env()
    from portfolio import pull_daily_robinhood_snapshot
    snap = pull_daily_robinhood_snapshot()
    if snap:
        print(f"✅  Portfolio snapshot saved → {snap}")
    else:
        print("❌  Portfolio pull failed. Check logs.")


def cmd_status():
    check_env()
    from utils import print_status
    print_status()


def cmd_schedule():
    check_env()
    from scheduler import start_scheduler
    start_scheduler()


def main():
    parser = argparse.ArgumentParser(
        prog="options_trader",
        description="Covered-call recommendation engine — Safe Mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands:
  --setup                              One-time wizard: collect & validate all credentials
  --run                                Execute full covered-call pipeline immediately
  --dry-run                            Execute full covered-call pipeline without sending email
  --collar                             Run collar pipeline and send email now
  --collar-dry-run                     Run collar pipeline, save HTML preview, no email sent
  --collar SYMBOL --find               On-demand collar scan for SYMBOL (replaces --collar-run)
  --collar SYMBOL --find --weeks 4 16  On-demand collar scan with custom week range (default: 4–26)
  --collar SYMBOL --add                Find best collar for SYMBOL and place the order (prompts confirm)
  --collar --show                      Show all collar holdings in portfolio
  --collar SYMBOL --show               Show collar holdings for SYMBOL
  --collar SYMBOL --roll               Find roll-up / roll-out options for existing SYMBOL collar
  --collar SYMBOL --roll --weeks 4 26  Roll search with custom week range (default: 4–26)
  --cc SYMBOL                          On-demand covered-call scan (best YPD; defaults: 7% buffer, 1% premium, 0–6 weeks)
  --ccs SYMBOL                         On-demand CCS scan / find (defaults: 1%–10% spread, 1% premium, 2–6 weeks)
  --ccs SYMBOL --find                  Find best CCS for SYMBOL (explicit)
  --ccs SYMBOL --add                   Find and place a CCS order for SYMBOL (prompts confirm)
  --ccs --show                         Show all CCS holdings in portfolio
  --ccs SYMBOL --show                  Show CCS holdings for SYMBOL
  --ccs SYMBOL --close                 Close existing CCS (limit = MIN($0.20, 20% of original credit, current mid))
  --ccs SYMBOL --close --price 0.10    Close CCS at a specific limit price of $0.10
  --ccs SYMBOL --close --chain "$115 CALL 5/15"  Close the specific CCS with short CALL @ $115 exp 5/15
  --pcs SYMBOL                         On-demand PCS scan / find (same defaults as --ccs)
  --pcs SYMBOL --find                  Find best PCS for SYMBOL (explicit)
  --pcs SYMBOL --add                   Find and place a PCS order for SYMBOL (prompts confirm)
  --pcs --show                         Show all PCS holdings in portfolio
  --pcs SYMBOL --show                  Show PCS holdings for SYMBOL
  --pcs SYMBOL --close                 Close existing PCS (limit = MIN($0.20, 20% of original credit, current mid))
  --pcs SYMBOL --close --price 0.10    Close PCS at a specific limit price of $0.10
  --pcs SYMBOL --close --chain "$120 PUT 5/1"    Close the specific PCS with short PUT @ $120 exp 5/1
  --report [mm/dd or mm/dd-mm/dd]      Options trade report (default: today). Fetches filled orders, prints summary, and emails it.
  --report --no-email                  Print report to console only — suppress email.
  --pull-portfolio                     Pull latest portfolio snapshot from Robinhood
  --status                             Show last-run summary and system health
  --schedule                           Start background scheduler daemon (blocks)

Contract actions (open covered-call management):
  --show SYMBOL                        List open contracts for SYMBOL with live ITM/OTM status
  --buy TSLA --chain "$300 CALL 5/16"              Buy-to-close at mid-price
  --buy TSLA --chain "$300 CALL 5/16" --price 2.50 Buy-to-close at a specific limit price
  --roll TSLA --chain "$300 CALL 5/16"                  Roll to next expiry at same/nearest OTM strike
  --roll TSLA --chain "$300 CALL 5/16" --prompt         Roll with y/n confirmation before placing orders
  --roll TSLA --chain "$300 CALL 5/16" --rescue         Roll at max-credit strike >= current (credit-only, cancels open orders)
  --roll TSLA --chain "$300 CALL 5/16" --rescue --prompt  Rescue roll with confirmation

Optimize mode (on-demand):
  --optimize                                        Optimize ALL open short contracts that gained >40% vs. purchase price
  --optimize TSLA                                   Optimize only TSLA contracts
  --optimize --min-gain 30                          Lower trigger threshold to 30% gain
  --optimize --date-range 30                        Scan expirations up to 30 days beyond current expiration
  --optimize TSLA --min-gain 50 --date-range 20 --prompt  Custom thresholds with per-roll confirmation
        """
    )

    # Primary mutex group — required=False so --show/--roll can act as primary commands too
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--setup",          action="store_true",  help="Run first-time setup wizard")
    group.add_argument("--run",            action="store_true",  help="Run pipeline now")
    group.add_argument("--dry-run",        action="store_true",  help="Run pipeline, skip email")
    group.add_argument(
        "--collar", nargs="?", const="PIPELINE", metavar="SYMBOL",
        help="Collar pipeline (no symbol) or on-demand collar action (with SYMBOL + --find/--add/--show/--roll)",
    )
    group.add_argument("--collar-dry-run", action="store_true",  help="Run collar pipeline, save preview, no email sent")
    group.add_argument("--cc",  metavar="SYMBOL", help="On-demand covered-call scan for SYMBOL")
    group.add_argument(
        "--ccs", nargs="?", const="ALL", metavar="SYMBOL",
        help="CCS scan (no symbol) or CCS action for SYMBOL + --find/--add/--show/--close",
    )
    group.add_argument(
        "--pcs", nargs="?", const="ALL", metavar="SYMBOL",
        help="PCS scan (no symbol) or PCS action for SYMBOL + --find/--add/--show/--close",
    )
    group.add_argument("--buy",            metavar="SYMBOL",     help="Buy-to-close an open contract (requires --chain)")
    group.add_argument(
        "--optimize", nargs="?", const="ALL", metavar="SYMBOL",
        help="Run optimize mode on-demand: roll UP/DOWN contracts that gained >40%% vs. purchase price "
             "(omit SYMBOL for all open positions, or supply SYMBOL to filter)",
    )
    group.add_argument(
        "--report", nargs="?", const="TODAY", metavar="DATE_RANGE",
        help="Options trade report for today or mm/dd / mm/dd-mm/dd range",
    )
    group.add_argument("--pull-portfolio", action="store_true",  help="Pull portfolio from Robinhood")
    group.add_argument("--status",         action="store_true",  help="Show system status")
    group.add_argument("--schedule",       action="store_true",  help="Start scheduler daemon")

    # --show and --roll are dual-purpose: standalone (covered calls) or sub-option for --collar
    parser.add_argument(
        "--show", nargs="?", const=True, metavar="SYMBOL",
        help="Show open contracts for SYMBOL (covered calls) OR collar holdings when used with --collar",
    )
    parser.add_argument(
        "--roll", nargs="?", const=True, metavar="SYMBOL",
        help="Roll a covered call (--roll SYMBOL --chain ...) OR find collar roll options (--collar SYMBOL --roll)",
    )
    # Collar sub-action flags (for use with --collar SYMBOL)
    parser.add_argument("--find",  action="store_true", default=False,
                        help="Find best opportunity for SYMBOL (used with --collar / --pcs / --ccs)")
    parser.add_argument("--add",   action="store_true", default=False,
                        help="Place order for SYMBOL (used with --collar / --pcs / --ccs)")
    parser.add_argument("--close", action="store_true", default=False,
                        help="Close existing spread position for SYMBOL (used with --pcs / --ccs)")
    parser.add_argument(
        "--contracts", type=int, metavar="N", default=1,
        help="Number of collar contracts to place with --add (default: 1)",
    )

    # Shared optional args for on-demand scans
    parser.add_argument(
        "--weeks", nargs=2, type=int, metavar=("MIN", "MAX"),
        default=None,
        help="Week range for --collar / --cc / --ccs / --pcs (e.g. --weeks 4 26). Default varies by command.",
    )
    parser.add_argument(
        "--buffer-size", type=float, metavar="PCT", default=None,
        help="Minimum OTM%% buffer for --cc (default: 7%%).",
    )
    parser.add_argument(
        "--spread-size", nargs=2, type=float, metavar=("MIN", "MAX"), default=None,
        help="Dollar range for spread width for --ccs / --pcs (e.g. --spread-size 2 8). Default: 1%%–10%% of stock price.",
    )
    parser.add_argument(
        "--target-premium", type=float, metavar="DOLLARS", default=None,
        help="Minimum net credit (or mid) per share for --cc / --ccs / --pcs (default: 1%% of stock price).",
    )
    # Args for --optimize
    parser.add_argument(
        "--min-gain", type=float, metavar="PCT", default=40.0,
        help="Min %%gain vs. purchase price to trigger --optimize (default: 40.0).",
    )
    parser.add_argument(
        "--date-range", type=int, metavar="DAYS", default=10,
        help="Max days beyond current expiration to scan for new contracts in "
             "--optimize (default: 10).",
    )

    # Args for --buy and --roll
    parser.add_argument(
        "--chain", type=str, metavar="CHAIN", default=None,
        help=(
            "Contract identifier for --buy / --roll, or spread selector for "
            "--pcs --close / --ccs --close. "
            "Format: \"$STRIKE TYPE MM/DD\"  e.g. \"$95 CALL 5/15\" or \"$120 PUT 5/1\". "
            "For --close: selects which spread to close when multiple are open for SYMBOL."
        ),
    )
    parser.add_argument(
        "--price", type=float, metavar="DOLLARS", default=None,
        help=(
            "Limit price per share for --buy / --roll / --close. "
            "For --close: default is MIN($0.20, 20%% of original credit, current mid)."
        ),
    )
    parser.add_argument(
        "--prompt", action="store_true", default=False,
        help="For --buy / --roll / --optimize: display order summary and require y/n confirmation before submitting.",
    )
    parser.add_argument(
        "--rescue", action="store_true", default=False,
        help="For --roll: find max-credit strike >= current at next expiry, cancel all open orders "
             "for the contract, and only roll if net credit > 0.",
    )
    parser.add_argument(
        "--no-email", action="store_true", default=False,
        help="For --report: print to console only, suppress sending the email.",
    )

    args = parser.parse_args()

    # Manual "at least one primary action" check (mutex group is required=False
    # because --show and --roll can act as standalone primary commands)
    primary_flags = [
        args.setup, args.run, args.dry_run, args.collar is not None,
        args.collar_dry_run, args.cc, args.ccs is not None, args.pcs is not None, args.buy,
        args.optimize is not None,
        args.report is not None, args.pull_portfolio, args.status, args.schedule,
        args.show is not None, args.roll is not None,
    ]
    if not any(primary_flags):
        parser.error("one of the arguments --setup --run --dry-run --collar ... is required")

    if args.setup:
        cmd_setup()
    elif args.run:
        cmd_run(dry_run=False)
    elif args.dry_run:
        cmd_run(dry_run=True)
    elif args.collar is not None:
        # Dispatch based on sub-options (--find / --add / --show / --roll)
        sym = None if args.collar == "PIPELINE" else args.collar
        weeks = args.weeks or [4, 26]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")

        if sym is None and not args.show and not args.find and not args.add and args.roll is None:
            # Bare --collar (no symbol, no sub-option) → run full pipeline
            cmd_collar(dry_run=False)
        elif args.show is not None:
            # --collar [SYMBOL] --show
            cmd_collar_show(sym)
        elif args.add:
            if not sym:
                parser.error("--collar --add requires a SYMBOL  e.g. --collar TSLA --add")
            cmd_collar_add(sym, weeks_min=weeks[0], weeks_max=weeks[1],
                           contracts=args.contracts)
        elif args.roll is not None:
            if not sym:
                parser.error("--collar --roll requires a SYMBOL  e.g. --collar TSLA --roll")
            cmd_collar_roll(sym, weeks_min=weeks[0], weeks_max=weeks[1])
        else:
            # --find (explicit) or --collar SYMBOL with no other sub-option → find
            if not sym:
                parser.error("--collar --find requires a SYMBOL  e.g. --collar TSLA --find")
            cmd_collar_find(sym, weeks_min=weeks[0], weeks_max=weeks[1])
    elif args.collar_dry_run:
        cmd_collar(dry_run=True)
    elif args.cc:
        weeks = args.weeks or [0, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        cmd_cc(
            args.cc,
            buffer_size=args.buffer_size,          # None → use default 7%
            target_premium=args.target_premium,    # None → use default 1%
            weeks_min=weeks[0], weeks_max=weeks[1],
        )
    elif args.ccs is not None:
        # Dispatch based on sub-options (--find / --add / --show / --close)
        sym = None if args.ccs == "ALL" else args.ccs.upper()
        weeks = args.weeks or [2, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size
        if spread_range and spread_range[0] > spread_range[1]:
            parser.error("--spread-size MIN must be less than or equal to MAX")

        if args.show is not None:
            # --ccs [SYMBOL] --show
            cmd_ccs_show(sym)
        elif args.add:
            if not sym:
                parser.error("--ccs --add requires a SYMBOL  e.g. --ccs TSLA --add")
            cmd_ccs_add(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                buffer_size=args.buffer_size,
                target_premium=args.target_premium,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
        elif args.close:
            if not sym:
                parser.error("--ccs --close requires a SYMBOL  e.g. --ccs TSLA --close")
            cmd_ccs_close(sym, price=args.price, chain=args.chain)
        else:
            # --find (explicit) or bare --ccs SYMBOL → find
            if not sym:
                parser.error("--ccs requires a SYMBOL  e.g. --ccs TSLA  or  --ccs TSLA --find")
            cmd_ccs_find(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                buffer_size=args.buffer_size,
                target_premium=args.target_premium,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
    elif args.pcs is not None:
        # Dispatch based on sub-options (--find / --add / --show / --close)
        sym = None if args.pcs == "ALL" else args.pcs.upper()
        weeks = args.weeks or [2, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size
        if spread_range and spread_range[0] > spread_range[1]:
            parser.error("--spread-size MIN must be less than or equal to MAX")

        if args.show is not None:
            # --pcs [SYMBOL] --show
            cmd_pcs_show(sym)
        elif args.add:
            if not sym:
                parser.error("--pcs --add requires a SYMBOL  e.g. --pcs TSLA --add")
            cmd_pcs_add(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                buffer_size=args.buffer_size,
                target_premium=args.target_premium,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
        elif args.close:
            if not sym:
                parser.error("--pcs --close requires a SYMBOL  e.g. --pcs TSLA --close")
            cmd_pcs_close(sym, price=args.price, chain=args.chain)
        else:
            # --find (explicit) or bare --pcs SYMBOL → find
            if not sym:
                parser.error("--pcs requires a SYMBOL  e.g. --pcs TSLA  or  --pcs TSLA --find")
            cmd_pcs_find(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                buffer_size=args.buffer_size,
                target_premium=args.target_premium,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
    elif args.show is not None:
        # Standalone --show SYMBOL → show covered call contracts
        if args.show is True:
            parser.error("--show requires a SYMBOL  e.g. --show TSLA")
        cmd_show(args.show)
    elif args.buy:
        if not args.chain:
            parser.error("--buy requires --chain  e.g. --chain \"$95 CALL 5/15\"")
        cmd_buy(args.buy, chain=args.chain, price=args.price, prompt=args.prompt)
    elif args.roll is not None:
        # Standalone --roll SYMBOL --chain ... → roll covered call
        if args.roll is True:
            parser.error("--roll requires a SYMBOL  e.g. --roll TSLA --chain \"$95 CALL 5/15\"")
        if not args.chain:
            parser.error("--roll requires --chain  e.g. --chain \"$95 CALL 5/15\"")
        cmd_roll(args.roll, args.chain, price=args.price, prompt=args.prompt, rescue=args.rescue)
    elif args.optimize is not None:
        sym = None if args.optimize == "ALL" else args.optimize
        cmd_optimize(
            symbol=sym,
            min_gain_pct=args.min_gain,
            date_range_days=args.date_range,
            prompt=args.prompt,
        )
    elif args.report is not None:
        # nargs="?" with const="TODAY": args.report == "TODAY" means no arg given
        date_arg = None if args.report == "TODAY" else args.report
        cmd_report(date_arg, no_email=args.no_email)
    elif args.pull_portfolio:
        cmd_pull_portfolio()
    elif args.status:
        cmd_status()
    elif args.schedule:
        cmd_schedule()


if __name__ == "__main__":
    main()
