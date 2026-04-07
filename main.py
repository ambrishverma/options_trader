#!/usr/bin/env python3
"""
Options Trader — CLI Entry Point
Usage:
  python main.py --setup                                           # One-time setup wizard
  python main.py --run                                             # Run the full covered-call pipeline now
  python main.py --dry-run                                         # Full covered-call pipeline, no email sent
  python main.py --collar                                          # Run collar pipeline and send email now
  python main.py --collar-dry-run                                  # Collar pipeline, no email sent (saves HTML preview)
  python main.py --collar-run SYMBOL                               # On-demand collar scan for any symbol
  python main.py --collar-run SYMBOL --weeks 4 16                  # On-demand scan with custom week range
  python main.py --cc SYMBOL                                       # On-demand covered-call scan for any symbol
  python main.py --cc SYMBOL --buffer-size 5 --target-premium 1.5 # On-demand CC scan with custom params
  python main.py --ccs SYMBOL                                      # On-demand Call Credit Spread scan
  python main.py --pcs SYMBOL                                      # On-demand Put Credit Spread scan
  python main.py --ccs SYMBOL --spread-size 2 8 --target-premium 2  # CCS scan: spread range $2–$8
  python main.py --show SYMBOL                                     # Show open contracts for SYMBOL (ITM/OTM status)
  python main.py --buy SYMBOL --chain "$95 CALL 5/15"              # Buy-to-close at mid-price
  python main.py --buy SYMBOL --chain "$95 CALL 5/15" --price 2.50 # Buy-to-close at a specific price
  python main.py --roll SYMBOL --chain "$95 CALL 5/15"                    # Roll to next expiry at nearest OTM strike
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --prompt           # Roll with confirmation prompt
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --rescue           # Roll at max-credit strike ≥ current (credit-only)
  python main.py --roll SYMBOL --chain "$95 CALL 5/15" --rescue --prompt  # Rescue roll with confirmation
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


def cmd_collar_run(symbol: str, weeks_min: int, weeks_max: int):
    check_env()
    from utils import setup_logging
    setup_logging()
    from scheduler import run_collar_on_demand_and_preview
    run_collar_on_demand_and_preview(symbol, weeks_min, weeks_max)


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


def cmd_ccs(symbol: str, spread_size_min: float, spread_size_max: float,
            buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Call Credit Spread scan for a single symbol."""
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


def cmd_pcs(symbol: str, spread_size_min: float, spread_size_max: float,
            buffer_size: float, target_premium: float, weeks_min: int, weeks_max: int):
    """On-demand Put Credit Spread scan for a single symbol."""
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
  --collar-run SYMBOL                  On-demand collar scan for any symbol (portfolio or not)
  --collar-run SYMBOL --weeks 4 16     On-demand collar scan with custom week range (default: 4–16)
  --cc SYMBOL                          On-demand covered-call scan (best YPD; defaults: 7% buffer, 1% premium, 0–6 weeks)
  --ccs SYMBOL                         On-demand Call Credit Spread scan (defaults: 1%–10% spread range, 1% premium, 2–6 weeks)
  --pcs SYMBOL                         On-demand Put Credit Spread scan (same defaults as --ccs)
  --pull-portfolio                     Pull latest portfolio snapshot from Robinhood
  --status                             Show last-run summary and system health
  --schedule                           Start background scheduler daemon (blocks)

Contract actions (open covered-call management):
  --show TSLA                          List open TSLA contracts with live ITM/OTM status
  --buy TSLA --chain "$300 CALL 5/16"              Buy-to-close at mid-price
  --buy TSLA --chain "$300 CALL 5/16" --price 2.50 Buy-to-close at a specific limit price
  --roll TSLA --chain "$300 CALL 5/16"                  Roll to next expiry at same/nearest OTM strike
  --roll TSLA --chain "$300 CALL 5/16" --prompt         Roll with y/n confirmation before placing orders
  --roll TSLA --chain "$300 CALL 5/16" --rescue         Roll at max-credit strike >= current (credit-only, cancels open orders)
  --roll TSLA --chain "$300 CALL 5/16" --rescue --prompt  Rescue roll with confirmation
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup",          action="store_true",  help="Run first-time setup wizard")
    group.add_argument("--run",            action="store_true",  help="Run pipeline now")
    group.add_argument("--dry-run",        action="store_true",  help="Run pipeline, skip email")
    group.add_argument("--collar",         action="store_true",  help="Run collar pipeline and send email now")
    group.add_argument("--collar-dry-run", action="store_true",  help="Run collar pipeline, save preview, no email sent")
    group.add_argument("--collar-run",     metavar="SYMBOL",     help="On-demand collar scan for SYMBOL")
    group.add_argument("--cc",             metavar="SYMBOL",     help="On-demand covered-call scan for SYMBOL")
    group.add_argument("--ccs",            metavar="SYMBOL",     help="On-demand Call Credit Spread scan for SYMBOL")
    group.add_argument("--pcs",            metavar="SYMBOL",     help="On-demand Put Credit Spread scan for SYMBOL")
    group.add_argument("--show",            metavar="SYMBOL",     help="Show open contracts for SYMBOL with live ITM/OTM status")
    group.add_argument("--buy",            metavar="SYMBOL",     help="Buy-to-close an open contract (requires --chain)")
    group.add_argument("--roll",           metavar="SYMBOL",     help="Roll an open contract to next expiry (requires --chain)")
    group.add_argument("--pull-portfolio", action="store_true",  help="Pull portfolio from Robinhood")
    group.add_argument("--status",         action="store_true",  help="Show system status")
    group.add_argument("--schedule",       action="store_true",  help="Start scheduler daemon")

    # Shared optional args for on-demand scans
    parser.add_argument(
        "--weeks", nargs=2, type=int, metavar=("MIN", "MAX"),
        default=None,
        help="Week range for --collar-run / --cc / --ccs / --pcs (e.g. --weeks 2 6). Default varies by command.",
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
    # Args for --buy and --roll
    parser.add_argument(
        "--chain", type=str, metavar="CHAIN", default=None,
        help="Contract identifier for --buy / --roll. Format: \"$STRIKE TYPE MM/DD\"  e.g. \"$95 CALL 5/15\".",
    )
    parser.add_argument(
        "--price", type=float, metavar="DOLLARS", default=None,
        help="Limit price per share for --buy / --roll. Default: (bid+ask)/2 (mid-point).",
    )
    parser.add_argument(
        "--prompt", action="store_true", default=False,
        help="For --buy / --roll: display order summary and require y/n confirmation before submitting.",
    )
    parser.add_argument(
        "--rescue", action="store_true", default=False,
        help="For --roll: find max-credit strike >= current at next expiry, cancel all open orders "
             "for the contract, and only roll if net credit > 0.",
    )

    args = parser.parse_args()

    if args.setup:
        cmd_setup()
    elif args.run:
        cmd_run(dry_run=False)
    elif args.dry_run:
        cmd_run(dry_run=True)
    elif args.collar:
        cmd_collar(dry_run=False)
    elif args.collar_dry_run:
        cmd_collar(dry_run=True)
    elif args.collar_run:
        weeks = args.weeks or [4, 16]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        cmd_collar_run(args.collar_run, weeks_min=weeks[0], weeks_max=weeks[1])
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
    elif args.ccs:
        weeks = args.weeks or [2, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size  # None or [min, max]
        if spread_range and spread_range[0] > spread_range[1]:
            parser.error("--spread-size MIN must be less than or equal to MAX")
        cmd_ccs(
            args.ccs,
            spread_size_min=spread_range[0] if spread_range else None,
            spread_size_max=spread_range[1] if spread_range else None,
            buffer_size=args.buffer_size,          # None → use config default (10%)
            target_premium=args.target_premium,    # None → use default 1%
            weeks_min=weeks[0], weeks_max=weeks[1],
        )
    elif args.pcs:
        weeks = args.weeks or [2, 6]
        if weeks[0] >= weeks[1]:
            parser.error("--weeks MIN must be less than MAX")
        spread_range = args.spread_size  # None or [min, max]
        if spread_range and spread_range[0] > spread_range[1]:
            parser.error("--spread-size MIN must be less than or equal to MAX")
        cmd_pcs(
            args.pcs,
            spread_size_min=spread_range[0] if spread_range else None,
            spread_size_max=spread_range[1] if spread_range else None,
            buffer_size=args.buffer_size,          # None → use config default (10%)
            target_premium=args.target_premium,
            weeks_min=weeks[0], weeks_max=weeks[1],
        )
    elif args.show:
        cmd_show(args.show)
    elif args.buy:
        if not args.chain:
            parser.error("--buy requires --chain  e.g. --chain \"$95 CALL 5/15\"")
        cmd_buy(args.buy, chain=args.chain, price=args.price, prompt=args.prompt)
    elif args.roll:
        if not args.chain:
            parser.error("--roll requires --chain  e.g. --chain \"$95 CALL 5/15\"")
        cmd_roll(args.roll, args.chain, price=args.price, prompt=args.prompt, rescue=args.rescue)
    elif args.pull_portfolio:
        cmd_pull_portfolio()
    elif args.status:
        cmd_status()
    elif args.schedule:
        cmd_schedule()


if __name__ == "__main__":
    main()
