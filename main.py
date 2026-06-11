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
  python main.py --pcs --spread-optimize                            # Optimize all PCS: take profit on decayed OTM spreads
  python main.py --pcs TSLA --spread-optimize                      # Optimize only TSLA PCS spreads
  python main.py --pcs --spread-optimize --threshold 0.80          # Override decay threshold to 80%
  python main.py --pcs --spread-safety                             # Safety-close all qualifying PCS spreads
  python main.py --pcs TSLA --spread-safety                       # Safety-close only TSLA PCS spreads
  python main.py --pcs --spread-rescue                             # Rescue all PCS spreads below break-even
  python main.py --pcs --spread-panic                              # Panic-close all PCS spreads breaching break-even
  python main.py --ccs --spread-optimize                            # Optimize all CCS: take profit on decayed OTM spreads
  python main.py --ccs TSLA --spread-optimize --threshold 0.70     # Optimize CCS with custom 70% threshold
  python main.py --ccs --spread-safety                             # Safety-close all qualifying CCS spreads
  python main.py --ccs --spread-rescue                             # Rescue all CCS spreads above break-even
  python main.py --ccs --spread-panic                              # Panic-close all CCS spreads breaching short strike
  python main.py --short --short-optimize                            # Optimize all short contracts: BTC decayed OTM contracts
  python main.py --short TSLA --short-optimize                     # Optimize only TSLA short contracts
  python main.py --short --short-optimize --threshold 0.80         # Override decay threshold to 80%
  python main.py --short --short-safety                            # Safety-close short contracts gained >40% against you
  python main.py --short TSLA --short-safety                       # Safety-close only TSLA shorts
  python main.py --short --short-rescue                            # Rescue-roll all ITM short contracts (DTE ≤ 5)
  python main.py --short --short-panic                             # Panic-roll DTE-0 ITM short contracts
  python main.py --pds SYMBOL                                      # On-demand Put Debit Spread (insurance) scan
  python main.py --pds SYMBOL --find                               # Find best PDS for SYMBOL (explicit)
  python main.py --pds SYMBOL --add                                # Place order for recommended PDS
  python main.py --pds --show                                      # Show put debit spread holdings
  python main.py --pds SYMBOL --close                              # Close existing PDS position
  python main.py --cds SYMBOL                                      # On-demand Call Debit Spread (insurance) scan
  python main.py --cds SYMBOL --find                               # Find best CDS for SYMBOL (explicit)
  python main.py --cds SYMBOL --add                                # Place order for recommended CDS
  python main.py --cds --show                                      # Show call debit spread holdings
  python main.py --cds SYMBOL --close                              # Close existing CDS position
  python main.py --find-insurance                                   # Find protective PDS for all holdings >= $50k
  python main.py --find-insurance META                              # Find protective PDS for META only
  python main.py --spreads                                         # List all open spread holdings (PCS + CCS)
  python main.py --spreads SYMBOL                                  # List open spread holdings for SYMBOL
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
  python main.py --optimize --min-credit 0.50                      # Require at least $0.50 net credit per share
  python main.py --optimize --date-range 30 --prompt               # Wider window + confirm each roll
  python main.py --report                                          # Options trade report for today (print + email)
  python main.py --report 04/09                                    # Report for a specific date
  python main.py --report 04/01-04/09                              # Report for a date range
  python main.py --report --no-email                               # Print today's report to console only, no email
  python main.py --pull-portfolio                                  # Manually pull portfolio from Robinhood
  python main.py --status                                          # Show health / last run info
  python main.py --schedule                                        # Start the background scheduler daemon
  python main.py --generate-income                                     # Preview income plan (dry-run, no orders placed)
  python main.py --generate-income NVDA                                # Preview for a single symbol
  python main.py --generate-income --add                               # Execute: place orders for all recommended symbols
  python main.py --generate-income NVDA --add                          # Place orders for one symbol only
  python main.py --income-config                                       # Show income generator config
  python main.py --income-config ig_risk_factor=0.5                    # Update a config value
  python main.py --config                                              # Show all config values
  python main.py --config min_otm_pct                                  # Show a single config value
  python main.py --config min_otm_pct=8.0                              # Update a config value
  python main.py --config spread_optimize_decay_pct=0.80               # Update any config key
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
    from scheduler import run_collar_scan
    run_collar_scan(dry_run=dry_run)


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


def cmd_pds_find(symbol: str, spread_size_min: float = None, spread_size_max: float = None,
                 weeks_min: int = 0, weeks_max: int = 8):
    """On-demand Put Debit Spread scan for a single symbol (--find)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_pds_on_demand_and_preview
    run_pds_on_demand_and_preview(
        symbol,
        spread_size_min=spread_size_min,
        spread_size_max=spread_size_max,
        weeks_min=weeks_min,
        weeks_max=weeks_max,
    )


def cmd_pds_add(symbol: str, spread_size_min: float = None, spread_size_max: float = None,
                weeks_min: int = 0, weeks_max: int = 8, quantity: int = 1):
    """Find best PDS for SYMBOL and interactively place the order (--add)."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from spread_scanner import scan_pds
    from trader import place_debit_spread_order

    config = load_config()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7
    min_oi       = int(config.get("debit_min_open_interest",    2))
    size_min_pct = float(config.get("debit_spread_size_min_pct", 1.0))
    size_max_pct = float(config.get("debit_spread_size_max_pct", 20.0))
    max_debit    = float(config.get("debit_max_debit_pct",      0.25))

    rec, _ = scan_pds(
        symbol,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        dte_min=dte_min, dte_max=dte_max,
        max_debit_pct=max_debit, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
    )
    if not rec:
        print(f"\nNo qualifying PDS found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return
    place_debit_spread_order(symbol, rec, spread_type="PDS", prompt=True, quantity=quantity)


def cmd_pds_show(symbol: Optional[str] = None):
    """Show existing put debit spread holdings from portfolio (--show)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_spread_holdings
    # PDS positions are put spreads — show them via PCS view (same underlying legs)
    show_spread_holdings("PCS", symbol)


def cmd_pds_close(symbol: str, price: Optional[float] = None,
                  chain: Optional[str] = None):
    """Close an existing PDS position for SYMBOL (--close)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import close_spread_position
    close_spread_position(symbol, spread_type="PCS", price=price, prompt=True,
                          chain=chain)


def cmd_cds_find(symbol: str, spread_size_min: float = None, spread_size_max: float = None,
                 weeks_min: int = 0, weeks_max: int = 8):
    """On-demand Call Debit Spread scan for a single symbol (--find)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_cds_on_demand_and_preview
    run_cds_on_demand_and_preview(
        symbol,
        spread_size_min=spread_size_min,
        spread_size_max=spread_size_max,
        weeks_min=weeks_min,
        weeks_max=weeks_max,
    )


def cmd_cds_add(symbol: str, spread_size_min: float = None, spread_size_max: float = None,
                weeks_min: int = 0, weeks_max: int = 8, quantity: int = 1):
    """Find best CDS for SYMBOL and interactively place the order (--add)."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from spread_scanner import scan_cds
    from trader import place_debit_spread_order

    config = load_config()
    dte_min = weeks_min * 7
    dte_max = weeks_max * 7
    min_oi       = int(config.get("debit_min_open_interest",    2))
    size_min_pct = float(config.get("debit_spread_size_min_pct", 1.0))
    size_max_pct = float(config.get("debit_spread_size_max_pct", 20.0))
    max_debit    = float(config.get("debit_max_debit_pct",      0.25))

    rec, _ = scan_cds(
        symbol,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        dte_min=dte_min, dte_max=dte_max,
        max_debit_pct=max_debit, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
    )
    if not rec:
        print(f"\nNo qualifying CDS found for {symbol} in the {weeks_min}–{weeks_max} week window.\n")
        return
    place_debit_spread_order(symbol, rec, spread_type="CDS", prompt=True, quantity=quantity)


def cmd_cds_show(symbol: Optional[str] = None):
    """Show existing call debit spread holdings from portfolio (--show)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_spread_holdings
    # CDS positions are call spreads — show them via CCS view (same underlying legs)
    show_spread_holdings("CCS", symbol)


def cmd_cds_close(symbol: str, price: Optional[float] = None,
                  chain: Optional[str] = None):
    """Close an existing CDS position for SYMBOL (--close)."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import close_spread_position
    close_spread_position(symbol, spread_type="CCS", price=price, prompt=True,
                          chain=chain)


def cmd_find_insurance(symbol: Optional[str] = None):
    """Find protective PDS (insurance) for large holdings or a specific symbol."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from spread_scanner import scan_insurance

    config = load_config()
    dte_min = int(config.get("debit_dte_min", 10))
    dte_max = int(config.get("debit_dte_max", 100))
    min_oi = int(config.get("debit_min_open_interest", 2))
    min_deductible = float(config.get("debit_long_leg_offset_pct", 5.0))
    max_deductible = float(config.get("insurance_max_deductible_pct", 10.0))
    min_coverage = float(config.get("insurance_min_coverage_pct", 10.0))
    max_coverage = float(config.get("debit_spread_size_max_pct", 25.0))
    top_n = int(config.get("spread_top_n", 1))
    min_value = float(config.get("debit_min_holding_value", 10000))

    if symbol:
        symbols = [(symbol.upper(), symbol.upper(), 0, 0)]
    else:
        from portfolio import get_portfolio
        holdings = get_portfolio()
        symbols = []
        for h in holdings:
            qty = h.get("shares", h.get("quantity", 0))
            price = h.get("price", 0)
            value = qty * price
            if value >= min_value:
                symbols.append((h["symbol"], h.get("name", h["symbol"]), qty, value))
        symbols.sort(key=lambda s: s[3], reverse=True)

    if not symbols:
        print(f"\nNo holdings found above ${min_value:,.0f} threshold.\n")
        return

    print(f"\n{'='*70}")
    print(f"  FIND INSURANCE — Protective Put Debit Spreads")
    print(f"  Deductible {min_deductible}–{max_deductible}% | Coverage {min_coverage}–{max_coverage}% | DTE {dte_min}–{dte_max}d")
    print(f"{'='*70}")

    total_scenarios = 0
    for sym, name, qty, value in symbols:
        value_str = f" (${value:,.0f})" if value > 0 else ""
        print(f"\n  {sym} {name}{value_str}")
        print(f"  {'-'*60}")

        recs, scenarios = scan_insurance(
            sym, name=name,
            dte_min=dte_min, dte_max=dte_max,
            min_open_interest=min_oi,
            min_deductible_pct=min_deductible,
            max_deductible_pct=max_deductible,
            min_coverage_pct=min_coverage,
            max_coverage_pct=max_coverage,
            top_n=top_n,
        )
        total_scenarios += scenarios

        if not recs:
            print(f"    No qualifying insurance found ({scenarios} scenarios evaluated)")
            continue

        for i, rec in enumerate(recs, 1):
            rank = f"#{i}" if len(recs) > 1 else "  "
            print(f"  {rank} {rec['expiration']} ({rec['dte']}d)")
            print(f"     Long  ${rec['long_leg']['strike']:.0f} put (ask ${rec['long_leg']['ask']:.2f})")
            print(f"     Short ${rec['short_leg']['strike']:.0f} put (bid ${rec['short_leg']['bid']:.2f})")
            print(f"     Premium:    ${rec['net_debit']:.2f}/sh  (${rec['net_debit_total']:.0f}/contract)")
            print(f"     Deductible: ${rec['deductible']:.0f} ({rec['deductible_pct']:.1f}%)")
            print(f"     Coverage:   ${rec['coverage_band']:.0f} ({rec['coverage_pct']:.1f}%)")
            print(f"     Cliff:      ${rec['cliff_strike']:.0f} ({rec['cliff_pct']:.1f}% below)")
            print(f"     Cost rate:  {rec['cost_rate']:.4f} (annualized $/$ of protection)")

    print(f"\n  Total scenarios evaluated: {total_scenarios:,}")
    print()


def cmd_strategy(symbol: Optional[str] = None):
    """Parse daily briefing strategy hints, scan for best contracts, and display."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from strategy import parse_strategy_table, scan_strategy_recommendations

    parsed = parse_strategy_table(filter_sym=symbol)

    print(f"\n{'='*60}")
    print(f"Strategy Recommendations (PCS / CCS)")
    print(f"{'='*60}")

    if not parsed:
        if symbol:
            print(f"  No PCS/CCS strategy found for {symbol} in today's briefing.\n")
        else:
            print("  No PCS/CCS strategies found in today's briefing.\n")
        return

    # Show parsed hints
    print(f"  Briefing hints ({len(parsed)}):")
    for r in parsed:
        print(f"    {r['symbol']:>6s}  {r['spread_type']}  {r['action']} ${r['strike']:.0f}")

    # Scan for actual contracts
    print(f"\n  Scanning for best contracts...\n")
    config = load_config()
    recs = scan_strategy_recommendations(parsed, config)

    if not recs:
        print("  No qualifying contracts found for any strategy hint.\n")
        return

    for rec in recs:
        hint = rec.get("strategy_hint", "")
        if rec.get("no_contract"):
            # No qualifying contract — show the hint only
            print(f"  {rec['symbol']:>6s}  {rec['type']}  — no qualifying contracts found")
            if hint:
                print(f"         Hint: {hint}")
            print()
            continue

        print(f"  {rec['symbol']:>6s}  {rec['type']}  {rec['expiration']} ({rec['dte']}d)")
        print(f"         Short: ${rec['short_leg']['strike']:.2f}  bid ${rec['short_leg']['bid']:.2f}  "
              f"ask ${rec['short_leg']['ask']:.2f}  OI {rec['short_leg']['open_interest']}  "
              f"+{rec['short_leg']['otm_pct']:.1f}% OTM")
        print(f"         Long:  ${rec['long_leg']['strike']:.2f}  bid ${rec['long_leg']['bid']:.2f}  "
              f"ask ${rec['long_leg']['ask']:.2f}  OI {rec['long_leg']['open_interest']}")
        print(f"         Net credit: ${rec['net_credit']:.2f}/share (${rec['net_credit_total']:.2f} total)")
        print(f"         Max loss: ${rec['max_loss']:.0f}  |  C/L ratio: {rec['credit_to_loss_ratio']:.2f}  "
              f"|  YPD: ${rec['ypd']:.2f}")
        if hint:
            print(f"         Hint: {hint}")
        print()


def cmd_generate_income(symbol: Optional[str] = None, live: bool = False):
    """Run the income generator: preview by default, --add to execute."""
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from income_generator import generate_income
    config = load_config()
    generate_income(symbol_filter=symbol, live=live, config=config)


def cmd_income_config_show():
    """Display current income generator configuration."""
    from utils import load_config
    from income_generator import show_config
    config = load_config()
    show_config(config)


def cmd_income_config_set(key_value: str):
    """Update an income generator config key."""
    from income_generator import set_config
    set_config(key_value)


def cmd_config(arg: str):
    """
    Read or update any config.yaml value.

    Args:
        arg: "SHOW" to show all, "KEY" to show one, or "KEY=VALUE" to update.
    """
    import re
    from utils import load_config, CONFIG_FILE

    config = load_config(reload=True)

    if arg == "SHOW":
        # Show all config values
        print(f"\n{'=' * 65}")
        print(f"  Configuration  ({CONFIG_FILE})")
        print(f"{'=' * 65}")
        max_key_len = max(len(k) for k in config) if config else 10
        for key, val in config.items():
            padding = "." * (max_key_len + 3 - len(key))
            print(f"  {key} {padding} {val}")
        print(f"{'=' * 65}\n")
        return

    if "=" not in arg:
        # Show a single key
        key = arg.strip()
        if key in config:
            print(f"  {key} = {config[key]}")
        else:
            print(f"  ❌  Unknown config key: {key}")
            print(f"      Available keys: {', '.join(config.keys())}")
        return

    # Update a key
    key, raw_value = arg.split("=", 1)
    key = key.strip()
    raw_value = raw_value.strip()

    if key not in config:
        print(f"  ❌  Unknown config key: {key}")
        print(f"      Available keys: {', '.join(config.keys())}")
        return

    # Infer type from current value
    current = config[key]
    try:
        if isinstance(current, bool):
            if raw_value.lower() in ("true", "1", "yes"):
                value = True
            elif raw_value.lower() in ("false", "0", "no"):
                value = False
            else:
                raise ValueError(f"expected true/false, got '{raw_value}'")
        elif isinstance(current, int):
            value = int(raw_value)
        elif isinstance(current, float):
            value = float(raw_value)
        else:
            # String — keep as-is (strip surrounding quotes if present)
            value = raw_value.strip("'\"")
    except (ValueError, TypeError) as e:
        print(f"  ❌  Invalid value for {key} ({type(current).__name__}): {e}")
        return

    # Line-level replacement to preserve YAML comments and formatting
    config_path = CONFIG_FILE
    lines = config_path.read_text().splitlines(keepends=True)
    pattern = re.compile(rf"^{re.escape(key)}\s*:")
    old_value = current
    found = False
    for i, line in enumerate(lines):
        if pattern.match(line):
            after_colon = line.split(":", 1)[1]
            comment = ""
            if "#" in after_colon:
                val_part, comment = after_colon.split("#", 1)
                comment = f"  # {comment.strip()}"
            # Format the YAML value
            if isinstance(value, bool):
                yaml_val = str(value).lower()
            elif isinstance(value, str):
                # Quote strings that look like times or contain special chars
                if re.match(r'^\d{1,2}:\d{2}$', value) or any(c in value for c in ':#{}[]'):
                    yaml_val = f'"{value}"'
                else:
                    yaml_val = value
            else:
                yaml_val = str(value)
            lines[i] = f"{key}: {yaml_val}{comment}\n"
            found = True
            break

    if not found:
        print(f"  ❌  Key '{key}' not found in config file (but exists in loaded config)")
        return

    config_path.write_text("".join(lines))
    print(f"  ✅  {key}: {old_value} → {value}")


def cmd_spreads_show(symbol: Optional[str] = None):
    """Show all open spread holdings (PCS + CCS) in one Robinhood session."""
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import show_all_spread_holdings
    show_all_spread_holdings(symbol)


def cmd_short_manage(
    mode: str,
    symbol: Optional[str] = None,
    config: Optional[dict] = None,
):
    """
    Execute a short contract management mode on-demand.

    Args:
        mode:   "optimize" | "safety" | "rescue" | "panic"
        symbol: Restrict to a single ticker (None = all).
        config: Config dict for configurable thresholds.
    """
    check_env()
    from utils import setup_logging, load_config
    setup_logging()
    from portfolio import (
        load_open_calls_detail_snapshot,
        load_open_puts_detail_snapshot,
        load_open_longs_detail_snapshot,
    )

    cfg = config or load_config()

    # Load open short contracts from latest snapshots
    open_calls_detail = load_open_calls_detail_snapshot()
    open_puts_detail  = load_open_puts_detail_snapshot()
    open_shorts = open_calls_detail + open_puts_detail

    # Fetch fresh live prices for all unique symbols
    from trader import _get_live_price
    symbols = list({c.get("symbol", "").upper() for c in open_shorts if c.get("symbol")})
    if symbol:
        symbols = [s for s in symbols if s == symbol.upper()]
    print(f"  Fetching live prices for {len(symbols)} symbol(s)...")
    live_prices = {}
    for sym in symbols:
        lp = _get_live_price(sym)
        if lp > 0:
            live_prices[sym] = lp
    name_map = {c.get("symbol", "").upper(): c.get("name", c.get("symbol", ""))
                for c in open_shorts if c.get("symbol")}

    if mode == "optimize":
        from trader import execute_short_optimize
        results = execute_short_optimize(
            open_shorts, live_prices, name_map,
            dry_run=False, config=cfg, filter_sym=symbol,
        )
    elif mode == "safety":
        from trader import execute_short_safety
        results = execute_short_safety(
            open_shorts, live_prices, name_map,
            dry_run=False, config=cfg, filter_sym=symbol,
        )
    elif mode == "rescue":
        from trader import execute_rescue_rolls
        results = execute_rescue_rolls(
            open_shorts, live_prices, name_map,
            dry_run=False, config=cfg,
        )
    elif mode == "panic":
        from trader import execute_panic_rolls
        open_longs = load_open_longs_detail_snapshot()
        results = execute_panic_rolls(
            open_shorts, live_prices, name_map, dry_run=False,
            open_long_contracts=open_longs,
        )
    else:
        print(f"  ❌  Unknown mode: {mode}")
        return

    if results:
        acted = [r for r in results if r.get("success") or not r.get("skipped", True)]
        print(f"\n  Summary: {len(acted)} contract(s) processed for {mode} mode.")
    else:
        print(f"\n  No contracts triggered for {mode} mode.")


def cmd_spread_manage(
    mode: str,
    spread_type: str,
    symbol: Optional[str] = None,
    dry_run: bool = False,
    config: Optional[dict] = None,
):
    """
    Execute a spread management mode (optimize / safety / rescue / panic) for PCS or CCS.

    Args:
        mode:        "optimize" | "safety" | "rescue" | "panic"
        spread_type: "PCS" | "CCS"
        symbol:      Restrict to a single ticker (None = all open spreads).
        dry_run:     If True, log what would happen but don't place orders.
        config:      Optional config dict for configurable thresholds (optimize mode).
    """
    check_env()
    from utils import setup_logging
    setup_logging()
    from trader import execute_spread_mode
    actions = execute_spread_mode(
        mode=mode,
        spread_type=spread_type,
        filter_sym=symbol,
        dry_run=dry_run,
        config=config,
    )
    if actions:
        print(f"\n  Summary: {len(actions)} {spread_type} spread(s) processed for {mode} mode.")
    else:
        print(f"\n  No {spread_type} spreads triggered for {mode} mode.")


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
    min_credit: float = 0.20,
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
        min_credit:      Minimum net credit per share required for a candidate to qualify (default 0.20).
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
        min_credit=min_credit,
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
  --spreads                            List all open spread holdings (PCS + CCS) across the portfolio
  --spreads SYMBOL                     List open spread holdings (PCS + CCS) for SYMBOL only
  --find-insurance                      Find protective PDS (insurance) for all holdings >= $50k
  --find-insurance META                Find protective PDS for META only
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
  --optimize --min-credit 0.50                      Require at least $0.50 net credit per share for candidates
  --optimize --date-range 30                        Scan expirations up to 30 days beyond current expiration
  --optimize TSLA --min-gain 50 --date-range 20 --prompt  Custom thresholds with per-roll confirmation

Spread management (PCS/CCS):
  --pcs --spread-optimize                     Optimize all PCS: take profit on OTM spreads that decayed >75%
  --pcs TSLA --spread-optimize                Optimize only TSLA PCS spreads
  --pcs --spread-optimize --threshold 0.80    Override decay threshold to 80%
  --pcs --spread-safety                       Safety-close all PCS: close spreads where BE > 90% of stock price
  --pcs TSLA --spread-safety                  Safety-close only TSLA PCS spreads
  --pcs --spread-rescue                       Rescue all PCS: close spreads where stock < break-even
  --pcs TSLA --spread-rescue                  Rescue only TSLA PCS spreads
  --pcs --spread-panic                        Panic all PCS: close spreads where stock < break-even (wider limit)
  --ccs --spread-optimize                     Optimize all CCS: take profit on OTM spreads that decayed >75%
  --ccs --spread-safety                       Safety-close all CCS: close spreads where BE < 110% of stock price
  --ccs --spread-rescue                       Rescue all CCS: close spreads where stock > break-even
  --ccs --spread-panic                        Panic all CCS: close spreads where stock > short strike (ITM)

Short contract management:
  --short --short-optimize                    Optimize all shorts: BTC decayed OTM contracts (>75% decay)
  --short TSLA --short-optimize               Optimize only TSLA short contracts
  --short --short-optimize --threshold 0.80   Override decay threshold to 80%
  --short --short-safety                      Safety-close shorts that gained >40% against you
  --short TSLA --short-safety                 Safety-close only TSLA shorts
  --short --short-rescue                      Rescue-roll all ITM short contracts (DTE ≤ 5)
  --short --short-panic                       Panic-roll DTE-0 ITM short contracts

Income generator:
  --generate-income                         Preview income plan from daily strategy briefing
  --generate-income SYMBOL                  Preview for one symbol only
  --generate-income --add                   Execute: place all recommended spread orders
  --generate-income SYMBOL --add            Execute for one symbol only
  --income-config                           Show income generator config
  --income-config ig_risk_factor=0.5        Update a config value

Configuration (general):
  --config                                  Show all config values
  --config KEY                              Show a single config value
  --config KEY=VALUE                        Update any config value
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
    group.add_argument(
        "--pds", nargs="?", const="ALL", metavar="SYMBOL",
        help="PDS (Put Debit Spread) insurance scan for SYMBOL + --find/--add/--show/--close",
    )
    group.add_argument(
        "--cds", nargs="?", const="ALL", metavar="SYMBOL",
        help="CDS (Call Debit Spread) insurance scan for SYMBOL + --find/--add/--show/--close",
    )
    group.add_argument(
        "--find-insurance", nargs="?", const="ALL", metavar="SYMBOL",
        help="Find protective PDS (insurance) for holdings >= $50k, or for a specific SYMBOL.",
    )
    group.add_argument(
        "--spreads", nargs="?", const="ALL", metavar="SYMBOL",
        help="List all open spread holdings (PCS + CCS). Optional SYMBOL to filter.",
    )
    group.add_argument(
        "--short", nargs="?", const="ALL", metavar="SYMBOL",
        help="Short contract management. Combine with --optimize, --safety, --rescue, or --panic.",
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
    group.add_argument(
        "--strategy", nargs="?", const="ALL", metavar="SYMBOL",
        help="Show PCS/CCS strategy recommendations from daily briefing (optional SYMBOL filter)",
    )
    group.add_argument(
        "--generate-income", nargs="?", const="ALL", metavar="SYMBOL",
        help="Income generator: preview plan (default) or place orders with --add",
    )
    group.add_argument(
        "--income-config", nargs="?", const="SHOW", metavar="KEY=VALUE",
        help="Show or update income generator config (e.g. --income-config ig_risk_factor=0.5)",
    )
    group.add_argument(
        "--config", nargs="?", const="SHOW", metavar="KEY[=VALUE]",
        help="Show all config, read a single key, or update any config value "
             "(e.g. --config, --config min_otm_pct, --config min_otm_pct=8.0)",
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
    parser.add_argument(
        "--min-credit", type=float, metavar="DOLLARS", default=0.20,
        help="Minimum net credit per share (STO − BTC) required for a candidate "
             "to qualify in --optimize (default: 0.20).",
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

    # Spread management mode flags (for use with --pcs / --ccs)
    parser.add_argument(
        "--spread-optimize", action="store_true", default=False,
        help="For --pcs/--ccs: take profit on OTM spreads that have decayed "
             "beyond the threshold (default 75%%). Uses config or --threshold.",
    )
    parser.add_argument(
        "--threshold", type=float, default=None, metavar="PCT",
        help="For --spread-optimize / --short-optimize: override decay threshold "
             "(0.0–1.0). Defaults to spread_optimize_decay_pct in config.yaml.",
    )
    parser.add_argument(
        "--spread-safety", action="store_true", default=False,
        help="For --pcs/--ccs: close spreads meeting safety criteria "
             "(PCS: BE > 90%% stock price; CCS: BE < 110%% stock price).",
    )
    parser.add_argument(
        "--spread-rescue", action="store_true", default=False,
        help="For --pcs/--ccs: close spreads where stock has crossed break-even "
             "(PCS: stock < BE; CCS: stock > BE).",
    )
    parser.add_argument(
        "--spread-panic", action="store_true", default=False,
        help="For --pcs/--ccs: panic-close spreads at wider limit prices "
             "(PCS: stock < BE; CCS: stock > short strike / ITM).",
    )

    # Short contract management mode flags (for use with --short)
    parser.add_argument(
        "--short-optimize", action="store_true", default=False,
        help="For --short: BTC profit-taking on OTM contracts decayed >75%%. "
             "Uses config or --threshold to override.",
    )
    parser.add_argument(
        "--short-safety", action="store_true", default=False,
        help="For --short: BTC close contracts where option gained >40%% against you.",
    )
    parser.add_argument(
        "--short-rescue", action="store_true", default=False,
        help="For --short: rescue-roll ITM contracts within DTE ≤ min_dte (default 5).",
    )
    parser.add_argument(
        "--short-panic", action="store_true", default=False,
        help="For --short: panic-roll DTE-0 ITM contracts.",
    )

    args = parser.parse_args()

    # Manual "at least one primary action" check (mutex group is required=False
    # because --show and --roll can act as standalone primary commands)
    primary_flags = [
        args.setup, args.run, args.dry_run, args.collar is not None,
        args.collar_dry_run, args.cc, args.ccs is not None, args.pcs is not None,
        args.pds is not None, args.cds is not None,
        args.find_insurance is not None,
        args.spreads is not None, args.short is not None, args.buy,
        args.optimize is not None,
        args.report is not None, args.strategy is not None,
        args.generate_income is not None,
        args.income_config is not None,
        args.config is not None,
        args.pull_portfolio, args.status, args.schedule,
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

        if args.spread_optimize:
            from utils import load_config
            cfg = load_config()
            if args.threshold is not None:
                cfg["spread_optimize_decay_pct"] = args.threshold
            cmd_spread_manage("optimize", "CCS", symbol=sym, config=cfg)
        elif args.spread_safety:
            cmd_spread_manage("safety", "CCS", symbol=sym)
        elif args.spread_rescue:
            cmd_spread_manage("rescue", "CCS", symbol=sym)
        elif args.spread_panic:
            cmd_spread_manage("panic", "CCS", symbol=sym)
        elif args.show is not None:
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
    elif args.spreads is not None:
        sym = None if args.spreads == "ALL" else args.spreads.upper()
        cmd_spreads_show(sym)
    elif args.short is not None:
        sym = None if args.short == "ALL" else args.short.upper()
        from utils import load_config
        cfg = load_config()
        if args.short_optimize:
            if args.threshold is not None:
                cfg["spread_optimize_decay_pct"] = args.threshold
            cmd_short_manage("optimize", symbol=sym, config=cfg)
        elif args.short_safety:
            cmd_short_manage("safety", symbol=sym, config=cfg)
        elif args.short_rescue:
            cmd_short_manage("rescue", symbol=sym, config=cfg)
        elif args.short_panic:
            cmd_short_manage("panic", symbol=sym, config=cfg)
        else:
            parser.error("--short requires a mode: --short-optimize, --short-safety, "
                         "--short-rescue, or --short-panic")
    elif args.pcs is not None:
        # Dispatch based on sub-options (--find / --add / --show / --close)
        sym = None if args.pcs == "ALL" else args.pcs.upper()
        weeks = args.weeks or [2, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size
        if spread_range and spread_range[0] > spread_range[1]:
            parser.error("--spread-size MIN must be less than or equal to MAX")

        if args.spread_optimize:
            from utils import load_config
            cfg = load_config()
            if args.threshold is not None:
                cfg["spread_optimize_decay_pct"] = args.threshold
            cmd_spread_manage("optimize", "PCS", symbol=sym, config=cfg)
        elif args.spread_safety:
            cmd_spread_manage("safety", "PCS", symbol=sym)
        elif args.spread_rescue:
            cmd_spread_manage("rescue", "PCS", symbol=sym)
        elif args.spread_panic:
            cmd_spread_manage("panic", "PCS", symbol=sym)
        elif args.show is not None:
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
    elif args.pds is not None:
        # Dispatch based on sub-options (--find / --add / --show / --close)
        sym = None if args.pds == "ALL" else args.pds.upper()
        weeks = args.weeks or [0, 8]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size

        if args.show is not None:
            cmd_pds_show(sym)
        elif args.add:
            if not sym:
                parser.error("--pds --add requires a SYMBOL  e.g. --pds TSLA --add")
            cmd_pds_add(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                weeks_min=weeks[0], weeks_max=weeks[1],
                quantity=args.contracts,
            )
        elif args.close:
            if not sym:
                parser.error("--pds --close requires a SYMBOL  e.g. --pds TSLA --close")
            cmd_pds_close(sym, price=args.price, chain=args.chain)
        else:
            # --find (explicit) or bare --pds SYMBOL → find
            if not sym:
                parser.error("--pds requires a SYMBOL  e.g. --pds TSLA  or  --pds TSLA --find")
            cmd_pds_find(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
    elif args.cds is not None:
        # Dispatch based on sub-options (--find / --add / --show / --close)
        sym = None if args.cds == "ALL" else args.cds.upper()
        weeks = args.weeks or [0, 8]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size

        if args.show is not None:
            cmd_cds_show(sym)
        elif args.add:
            if not sym:
                parser.error("--cds --add requires a SYMBOL  e.g. --cds TSLA --add")
            cmd_cds_add(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                weeks_min=weeks[0], weeks_max=weeks[1],
                quantity=args.contracts,
            )
        elif args.close:
            if not sym:
                parser.error("--cds --close requires a SYMBOL  e.g. --cds TSLA --close")
            cmd_cds_close(sym, price=args.price, chain=args.chain)
        else:
            # --find (explicit) or bare --cds SYMBOL → find
            if not sym:
                parser.error("--cds requires a SYMBOL  e.g. --cds TSLA  or  --cds TSLA --find")
            cmd_cds_find(
                sym,
                spread_size_min=spread_range[0] if spread_range else None,
                spread_size_max=spread_range[1] if spread_range else None,
                weeks_min=weeks[0], weeks_max=weeks[1],
            )
    elif args.find_insurance is not None:
        sym = None if args.find_insurance == "ALL" else args.find_insurance.upper()
        cmd_find_insurance(sym)
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
            min_credit=args.min_credit,
        )
    elif args.report is not None:
        # nargs="?" with const="TODAY": args.report == "TODAY" means no arg given
        date_arg = None if args.report == "TODAY" else args.report
        cmd_report(date_arg, no_email=args.no_email)
    elif args.strategy is not None:
        sym = None if args.strategy == "ALL" else args.strategy.upper()
        cmd_strategy(sym)
    elif args.generate_income is not None:
        sym = None if args.generate_income == "ALL" else args.generate_income.upper()
        cmd_generate_income(sym, live=args.add)
    elif args.income_config is not None:
        if args.income_config == "SHOW":
            cmd_income_config_show()
        else:
            cmd_income_config_set(args.income_config)
    elif args.config is not None:
        cmd_config(args.config)
    elif args.pull_portfolio:
        cmd_pull_portfolio()
    elif args.status:
        cmd_status()
    elif args.schedule:
        cmd_schedule()


if __name__ == "__main__":
    main()
