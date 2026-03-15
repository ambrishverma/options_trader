"""
scheduler.py — Daily Pipeline Orchestrator
============================================
Two responsibilities:

1. run_pipeline(dry_run)  — Execute the full pipeline once, immediately.
   Called by `--run` and `--dry-run` CLI commands.

2. start_scheduler()      — Block and run the scheduler daemon.
   Called by `--schedule` CLI command.

Pipeline sequence (weekdays only):
  ┌─ 6:00 AM ET (1st trading day/month) ──────────────────────┐
  │  TOTP login → Robinhood portfolio pull → save snapshot     │
  └────────────────────────────────────────────────────────────┘
  ┌─ 9:35 AM ET (every weekday) ───────────────────────────────┐
  │  1. Load portfolio snapshot                                 │
  │  2. Fetch live prices + options chains (21-day window)      │
  │  3. Apply Safe Mode filters                                 │
  │  4. Score, rank, diversify (50/50)                          │
  │  5. Check Finnhub earnings warnings                         │
  │  6. Send SendGrid email                                     │
  │  7. Write run log                                           │
  └────────────────────────────────────────────────────────────┘
"""

import logging
import time
import yaml
import json
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import schedule
import exchange_calendars as xcals

from utils import setup_logging, load_config, write_run_log

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
ET = ZoneInfo("America/New_York")


# ─────────────────────────────────────────────────────────────────────────────
# NYSE trading calendar helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_trading_day(dt: date = None) -> bool:
    """Return True if dt (default today) is a NYSE trading day."""
    dt = dt or date.today()
    try:
        cal = xcals.get_calendar("XNYS")
        return cal.is_session(dt.strftime("%Y-%m-%d"))
    except Exception as e:
        logger.warning(f"NYSE calendar check failed: {e}. Assuming trading day.")
        return True


def _is_first_trading_day_of_month(dt: date = None) -> bool:
    """Return True if dt is the first NYSE trading day of its month."""
    dt = dt or date.today()
    try:
        cal = xcals.get_calendar("XNYS")
        year, month = dt.year, dt.month
        sessions = cal.sessions_in_range(
            f"{year}-{month:02d}-01",
            f"{year}-{month:02d}-{_days_in_month(year, month):02d}",
        )
        return len(sessions) > 0 and str(sessions[0].date()) == str(dt)
    except Exception as e:
        logger.warning(f"First-trading-day check failed: {e}")
        return dt.day == 1  # fallback


def _days_in_month(year: int, month: int) -> int:
    import calendar
    return calendar.monthrange(year, month)[1]


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio pull job (monthly, 6:00 AM ET)
# ─────────────────────────────────────────────────────────────────────────────

def job_monthly_portfolio_pull():
    """Pull Robinhood portfolio on the 1st trading day of the month."""
    today = date.today()

    if not _is_trading_day(today):
        logger.info("Portfolio pull skipped — not a trading day")
        return

    if not _is_first_trading_day_of_month(today):
        logger.info("Portfolio pull skipped — not the first trading day of the month")
        return

    logger.info("🏦  Starting monthly Robinhood portfolio pull...")

    from portfolio import pull_robinhood_portfolio
    snap = pull_robinhood_portfolio()

    if snap:
        logger.info(f"✅  Monthly portfolio pull complete: {snap}")
    else:
        logger.error("❌  Monthly portfolio pull failed")


# ─────────────────────────────────────────────────────────────────────────────
# Full daily pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(dry_run: bool = False):
    """
    Execute the full covered-call recommendation pipeline.
    Can be called directly (--run / --dry-run) or by the scheduler.
    """
    start_ts = datetime.now(tz=ET)
    today_str = start_ts.strftime("%Y-%m-%d")

    mode = "[DRY RUN]" if dry_run else ""
    logger.info(f"{'='*60}")
    logger.info(f"Pipeline start {mode} — {start_ts.strftime('%Y-%m-%d %H:%M:%S ET')}")

    config = load_config()
    results = {
        "run_date":       today_str,
        "dry_run":        dry_run,
        "started_at":     start_ts.isoformat(),
        "recipient_email": config.get("recipient_email", ""),
    }

    if not dry_run and not _is_trading_day():
        logger.info("Pipeline skipped — today is not a NYSE trading day")
        results["skipped"] = True
        results["skip_reason"] = "non-trading day"
        write_run_log(results)
        return

    try:
        # ── Step 1: Load portfolio ─────────────────────────────────────────────
        logger.info("[1/6] Loading portfolio snapshot...")
        from portfolio import get_portfolio
        xlsx_path = config.get("xlsx_fallback_path")
        holdings = get_portfolio(xlsx_path=xlsx_path)
        results["holdings_eligible"] = len(holdings)
        logger.info(f"  {len(holdings)} eligible holdings loaded")

        if not holdings:
            logger.warning("No eligible holdings — pipeline complete (no email)")
            results["outcome"] = "no_eligible_holdings"
            write_run_log(results)
            return

        # ── Step 2: Fetch options chains ───────────────────────────────────────
        logger.info("[2/6] Fetching options chains...")
        from options_chain import fetch_all_options
        raw_options = fetch_all_options(
            holdings,
            lookahead_days=config.get("lookahead_days", 21),
        )
        results["options_raw"] = len(raw_options)
        logger.info(f"  {len(raw_options)} raw option records fetched")

        # ── Step 3: Filter + rank ──────────────────────────────────────────────
        logger.info("[3/6] Applying Safe Mode filters...")
        from filters import run_filters
        filter_result = run_filters(raw_options, config)
        results["options_passing"] = filter_result["count_passing"]
        logger.info(f"  {filter_result['count_passing']}/{filter_result['count_raw']} options passed filters")

        if not filter_result["all_passing"]:
            logger.warning("No options passed filters — pipeline complete (no email)")
            results["outcome"] = "no_options_passed"
            write_run_log(results)
            return

        # ── Step 4: Build diversified recommendations ──────────────────────────
        logger.info("[4/6] Building 50/50 diversified recommendations...")
        from diversifier import build_recommendations
        recommendations = build_recommendations(filter_result, config)
        results["recommendations"] = len(recommendations)
        logger.info(f"  {len(recommendations)} recommendations built")

        # ── Step 5: Earnings warnings ──────────────────────────────────────────
        logger.info("[5/6] Checking earnings calendar...")
        from earnings import build_earnings_warnings
        recommendations = build_earnings_warnings(recommendations)
        flagged = sum(1 for r in recommendations if r.get("earnings_flag"))
        results["earnings_flagged"] = flagged
        logger.info(f"  {flagged} earnings warnings")

        # ── Step 6: Send email ─────────────────────────────────────────────────
        logger.info(f"[6/6] {'Generating email preview' if dry_run else 'Sending email'}...")
        from emailer import send_recommendations

        run_meta = {
            "run_date":       today_str,
            "recipient_email": config.get("recipient_email", ""),
            "duration_sec":   round((datetime.now(tz=ET) - start_ts).total_seconds(), 1),
        }

        email_ok = send_recommendations(recommendations, run_meta, dry_run=dry_run)
        results["email_sent"] = email_ok

        end_ts = datetime.now(tz=ET)
        duration = (end_ts - start_ts).total_seconds()
        results["duration_sec"] = round(duration, 1)
        results["completed_at"] = end_ts.isoformat()
        results["outcome"] = "success"

        logger.info(
            f"{'='*60}\n"
            f"Pipeline {'dry run ' if dry_run else ''}complete in {duration:.0f}s\n"
            f"  Holdings: {results['holdings_eligible']} eligible\n"
            f"  Options:  {results['options_raw']} raw → {results['options_passing']} passing\n"
            f"  Recs:     {results['recommendations']}\n"
            f"  Earnings: {flagged} warning(s)\n"
            f"  Email:    {'sent ✅' if email_ok else 'FAILED ❌'} → {config.get('recipient_email', 'n/a')}"
        )

    except Exception as e:
        end_ts = datetime.now(tz=ET)
        results["outcome"] = "error"
        results["error"] = str(e)
        results["duration_sec"] = round((end_ts - start_ts).total_seconds(), 1)
        logger.error(f"Pipeline failed: {e}", exc_info=True)

    finally:
        write_run_log(results)


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler daemon
# ─────────────────────────────────────────────────────────────────────────────

def job_daily_pipeline():
    """Scheduled daily pipeline job — skips non-trading days."""
    if not _is_trading_day():
        logger.info(f"Daily pipeline skipped — {date.today()} is not a trading day")
        return
    run_pipeline(dry_run=False)


def start_scheduler():
    """
    Start the blocking scheduler daemon.
    Runs:
      - 6:00 AM ET:  Monthly portfolio pull check
      - 9:35 AM ET:  Daily covered-call pipeline
    """
    config = load_config()
    pipeline_time = config.get("pipeline_time_et", "09:35")
    pull_time     = "06:00"

    logger.info(f"Scheduler starting...")
    logger.info(f"  Portfolio pull: {pull_time} ET (runs on first trading day of month)")
    logger.info(f"  Daily pipeline: {pipeline_time} ET (weekdays only)")

    # Schedule jobs
    schedule.every().day.at(pull_time).do(job_monthly_portfolio_pull)
    schedule.every().day.at(pipeline_time).do(job_daily_pipeline)

    logger.info("Scheduler running. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)   # check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
