"""
scheduler.py — Daily Pipeline Orchestrator
============================================
Two responsibilities:

1. run_pipeline(dry_run)  — Execute the full pipeline once, immediately.
   Called by `--run` and `--dry-run` CLI commands.

2. start_scheduler()      — Block and run the scheduler daemon.
   Called by `--schedule` CLI command.

Pipeline sequence (weekdays only):
  ┌─ 6:00 AM ET (every Monday) ────────────────────────────────┐
  │  TOTP login → Robinhood portfolio pull → save snapshot     │
  └────────────────────────────────────────────────────────────┘
  ┌─ 9:35 AM ET (every weekday) ───────────────────────────────┐
  │  1. Load portfolio snapshot                                 │
  │  2. Check open covered-call positions (Robinhood)           │
  │     → subtract already-written contracts per symbol        │
  │     → drop symbols with no remaining contracts             │
  │  3. Fetch live prices + options chains (21-day window)      │
  │  4. Apply Safe Mode filters                                 │
  │  5. Score, rank, diversify (50/50)                          │
  │  6. Check Finnhub earnings warnings                         │
  │  7. Send SendGrid email                                     │
  │  8. Write run log                                           │
  └────────────────────────────────────────────────────────────┘
"""

import logging
import time
import yaml
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import schedule
import exchange_calendars as xcals

from utils import setup_logging, load_config, write_run_log

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
ET    = ZoneInfo("America/New_York")
LOCAL = ZoneInfo("America/Los_Angeles")   # machine timezone (PT)


def _et_to_local(time_et: str) -> str:
    """Convert an HH:MM ET time string to the equivalent local (PT) wall-clock
    time, fully DST-aware.  The schedule library has no timezone support, so
    we always pass it a *local* time."""
    h, m = map(int, time_et.split(":"))
    # Anchor to today's date so DST offsets are correct right now
    et_dt    = datetime.now(ET).replace(hour=h, minute=m, second=0, microsecond=0)
    local_dt = et_dt.astimezone(LOCAL)
    return local_dt.strftime("%H:%M")


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
# Portfolio pull job (weekly, Monday 6:00 AM ET)
# ─────────────────────────────────────────────────────────────────────────────

def job_weekly_portfolio_pull():
    """Pull Robinhood portfolio every Monday (skips if Monday is a market holiday)."""
    today = date.today()

    if not _is_trading_day(today):
        logger.info("Portfolio pull skipped — Monday is not a trading day (market holiday)")
        return

    logger.info("🏦  Starting weekly Robinhood portfolio pull...")

    from portfolio import pull_robinhood_portfolio
    snap = pull_robinhood_portfolio()

    if snap:
        logger.info(f"✅  Weekly portfolio pull complete: {snap}")
    else:
        logger.error("❌  Weekly portfolio pull failed")


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
        logger.info("[1/7] Loading portfolio snapshot...")
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

        # ── Step 2: Check open covered-call positions ──────────────────────────
        logger.info("[2/7] Checking open covered-call positions on Robinhood...")
        from portfolio import get_open_covered_calls
        open_calls = get_open_covered_calls()
        results["open_covered_calls"] = open_calls

        if open_calls:
            adjusted = []
            for h in holdings:
                sym = h["symbol"]
                already_open = open_calls.get(sym, 0)
                if already_open == 0:
                    adjusted.append(h)
                    continue
                remaining = h["contracts"] - already_open
                if remaining <= 0:
                    logger.info(
                        f"  {sym}: all {h['contracts']} contract(s) already written — excluded"
                    )
                else:
                    h = dict(h)   # don't mutate the original
                    h["contracts"] = remaining
                    adjusted.append(h)
                    logger.info(
                        f"  {sym}: {already_open} contract(s) already open → "
                        f"{remaining} remaining available"
                    )
            excluded = len(holdings) - len(adjusted)
            logger.info(
                f"  {excluded} symbol(s) fully excluded, "
                f"{len(adjusted)} holding(s) proceeding"
            )
            holdings = adjusted
        else:
            logger.info("  No open covered calls — all holdings eligible")

        if not holdings:
            logger.warning("All holdings have open covered calls — pipeline complete (no email)")
            results["outcome"] = "all_holdings_covered"
            write_run_log(results)
            return

        # ── Step 3: Fetch options chains ───────────────────────────────────────
        logger.info("[3/7] Fetching options chains...")
        from options_chain import fetch_all_options
        raw_options = fetch_all_options(
            holdings,
            lookahead_days=config.get("lookahead_days", 21),
        )
        results["options_raw"] = len(raw_options)
        logger.info(f"  {len(raw_options)} raw option records fetched")

        # ── Step 4: Filter + rank ──────────────────────────────────────────────
        logger.info("[4/7] Applying Safe Mode filters...")
        from filters import run_filters
        filter_result = run_filters(raw_options, config)
        results["options_passing"] = filter_result["count_passing"]
        results["filter_rejected"] = filter_result.get("rejected_counts", {})
        logger.info(f"  {filter_result['count_passing']}/{filter_result['count_raw']} options passed filters")

        if not filter_result["all_passing"]:
            logger.warning("No options passed filters — pipeline complete (no email)")
            results["outcome"] = "no_options_passed"
            write_run_log(results)
            return

        # ── Step 5: Build diversified recommendations ──────────────────────────
        logger.info("[5/7] Building 50/50 diversified recommendations...")
        from diversifier import build_recommendations
        recommendations = build_recommendations(filter_result, config)
        results["recommendations"] = len(recommendations)
        logger.info(f"  {len(recommendations)} recommendations built")

        # ── Step 6: Earnings warnings ──────────────────────────────────────────
        logger.info("[6/7] Checking earnings calendar...")
        from earnings import build_earnings_warnings
        recommendations = build_earnings_warnings(recommendations)
        flagged = sum(1 for r in recommendations if r.get("earnings_flag"))
        results["earnings_flagged"] = flagged
        logger.info(f"  {flagged} earnings warnings")

        # ── Persist recommendations history ────────────────────────────────────
        from utils import write_recommendations_log
        write_recommendations_log(recommendations, today_str, dry_run=dry_run)

        # ── Step 7: Send email ─────────────────────────────────────────────────
        logger.info(f"[7/7] {'Generating email preview' if dry_run else 'Sending email'}...")
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

        open_calls_count = len(results.get("open_covered_calls", {}))
        logger.info(
            f"{'='*60}\n"
            f"Pipeline {'dry run ' if dry_run else ''}complete in {duration:.0f}s\n"
            f"  Holdings: {results['holdings_eligible']} eligible, "
            f"{open_calls_count} symbol(s) skipped (open covered calls)\n"
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
      - Monday 6:00 AM ET:  Weekly Robinhood portfolio pull
      - Weekdays 10:15 AM ET: Daily covered-call pipeline
    """
    setup_logging()          # <-- MUST be called here; without this all logs are silently dropped
    config = load_config()
    pipeline_time_et = config.get("pipeline_time_et", "10:15")
    pull_time_et     = "06:00"

    # schedule library uses local (PT) wall-clock time — convert from ET
    pipeline_time_local = _et_to_local(pipeline_time_et)
    pull_time_local     = _et_to_local(pull_time_et)

    logger.info(f"Scheduler starting...")
    logger.info(f"  Portfolio pull: {pull_time_et} ET  →  {pull_time_local} PT  (every Monday)")
    logger.info(f"  Daily pipeline: {pipeline_time_et} ET  →  {pipeline_time_local} PT  (weekdays only)")

    # Portfolio pull: every Monday morning (trading-day guard inside the job)
    schedule.every().monday.at(pull_time_local).do(job_weekly_portfolio_pull)
    # Daily pipeline: every day — job itself skips non-trading days
    schedule.every().day.at(pipeline_time_local).do(job_daily_pipeline)

    logger.info("Scheduler running. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)   # check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
