#!/usr/bin/env python3
"""
Options Trader — CLI Entry Point
Usage:
  python main.py --setup          # One-time setup wizard
  python main.py --run            # Run the full pipeline now
  python main.py --dry-run        # Full pipeline, no email sent
  python main.py --pull-portfolio # Manually pull portfolio from Robinhood
  python main.py --status         # Show health / last run info
  python main.py --schedule       # Start the background scheduler daemon
"""

import argparse
import sys
import os
from pathlib import Path

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


def cmd_pull_portfolio():
    check_env()
    from portfolio import pull_robinhood_portfolio
    snap = pull_robinhood_portfolio()
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
  --setup           One-time wizard: collect & validate all credentials
  --run             Execute full pipeline immediately
  --dry-run         Execute full pipeline without sending email
  --pull-portfolio  Pull latest portfolio snapshot from Robinhood
  --status          Show last-run summary and system health
  --schedule        Start background scheduler daemon (blocks)
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--setup",          action="store_true", help="Run first-time setup wizard")
    group.add_argument("--run",            action="store_true", help="Run pipeline now")
    group.add_argument("--dry-run",        action="store_true", help="Run pipeline, skip email")
    group.add_argument("--pull-portfolio", action="store_true", help="Pull portfolio from Robinhood")
    group.add_argument("--status",         action="store_true", help="Show system status")
    group.add_argument("--schedule",       action="store_true", help="Start scheduler daemon")

    args = parser.parse_args()

    if args.setup:
        cmd_setup()
    elif args.run:
        cmd_run(dry_run=False)
    elif args.dry_run:
        cmd_run(dry_run=True)
    elif args.pull_portfolio:
        cmd_pull_portfolio()
    elif args.status:
        cmd_status()
    elif args.schedule:
        cmd_schedule()


if __name__ == "__main__":
    main()
