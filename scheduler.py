"""
scheduler.py — Daily Pipeline Orchestrator
============================================
Two responsibilities:

1. run_pipeline(dry_run)  — Execute the full pipeline once, immediately.
   Called by `--run` and `--dry-run` CLI commands.

2. start_scheduler()      — Block and run the scheduler daemon.
   Called by `--schedule` CLI command.

Pipeline sequence (weekdays only):
  ┌─ 2:30 AM ET (every trading day) ──────────────────────────┐
  │  TOTP login → Robinhood portfolio pull → save snapshot    │
  └───────────────────────────────────────────────────────────┘
  ┌─ 10:15 AM ET (every weekday) ──────────────────────────────┐
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
  ┌─ Saturday 10:00 AM ET (= 7:00 AM PT) ─────────────────────┐
  │  Collar recommendations pipeline → SendGrid email          │
  └────────────────────────────────────────────────────────────┘
"""

import atexit
import logging
import os
import sys
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
# Portfolio pull job (daily, 2:30 AM ET, trading days only)
# ─────────────────────────────────────────────────────────────────────────────

def job_daily_portfolio_pull():
    """Pull Robinhood portfolio every day (skips market holidays and weekends).

    IMPORTANT: Uses ET date, not local (PT) date.  The pull fires at 02:30 AM ET,
    which is 23:30 PT the *previous* calendar day.  date.today() in PT would
    return yesterday (e.g. Sunday), causing a false "not a trading day" skip on
    Monday mornings.  Anchoring to ET ensures we check Monday's session correctly.
    """
    today = datetime.now(tz=ET).date()   # ET date — correct for 02:30 AM ET job

    if not _is_trading_day(today):
        logger.info("Portfolio pull skipped — today is not a trading day")
        return

    logger.info("🏦  Starting daily Robinhood snapshot (portfolio + open calls)...")

    from portfolio import pull_daily_robinhood_snapshot
    snap = pull_daily_robinhood_snapshot()

    if snap:
        logger.info(f"✅  Daily portfolio pull complete: {snap}")
    else:
        logger.error("❌  Daily portfolio pull failed — pipeline will use last snapshot")


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

        # ── Step 2: Load open covered-call positions from morning snapshot ───────
        # The 2:30 AM portfolio pull (pull_daily_robinhood_snapshot) fetches both
        # the portfolio and open calls in ONE session, saving open_calls_YYYYMMDD.json.
        # Loading from that snapshot here avoids a second Robinhood login at
        # 10:15 AM, which triggers device-verification challenges and hangs.
        logger.info("[2/7] Loading open covered-call positions from snapshot...")
        from portfolio import load_open_calls_snapshot
        open_calls = load_open_calls_snapshot()
        results["open_covered_calls"] = open_calls

        # Snapshot of UNADJUSTED holdings — used as PUR denominator
        holdings_all = list(holdings)

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

        # ── Compute Portfolio Utilization Ratio (PUR) ─────────────────────────
        # Denominator = full holdings list BEFORE adjustment (max possible contracts)
        _max_possible = sum(h["contracts"] for h in holdings_all)
        _open_total   = sum(open_calls.values())
        pur_pct  = round(_open_total / _max_possible * 100, 1) if _max_possible > 0 else 0.0
        results["pur_pct"]  = pur_pct
        results["pur_open"] = _open_total
        results["pur_max"]  = _max_possible
        logger.info(
            f"  PUR: {pur_pct:.1f}% — {_open_total} of {_max_possible} possible contracts deployed"
        )

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

        # Compute portfolio-level YPD (total $/day if all recs are opened today)
        portfolio_ypd = round(sum(r.get("combined_ypd", 0) for r in recommendations), 2)
        results["portfolio_ypd"] = portfolio_ypd

        # ── Step 6: Earnings warnings + ex-dividend dates ─────────────────────
        logger.info("[6/7] Checking earnings calendar and ex-dividend dates...")
        from earnings import build_earnings_warnings, add_ex_dividend_dates
        recommendations = build_earnings_warnings(recommendations)
        recommendations = add_ex_dividend_dates(recommendations)
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
            "run_date":        today_str,
            "recipient_email": config.get("recipient_email", ""),
            "duration_sec":    round((datetime.now(tz=ET) - start_ts).total_seconds(), 1),
            "pur_pct":         results.get("pur_pct", 0.0),
            "pur_open":        results.get("pur_open", 0),
            "pur_max":         results.get("pur_max", 0),
            "portfolio_ypd":   results.get("portfolio_ypd", 0.0),
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
# Collar pipeline (weekly Saturday)
# ─────────────────────────────────────────────────────────────────────────────

def run_collar_on_demand_and_preview(symbol: str, weeks_min: int, weeks_max: int):
    """
    On-demand collar scan for a single symbol.  Saves an HTML preview to
    logs/collar_on_demand_<SYMBOL>_<date>.html and prints a summary to stdout.
    """
    config    = load_config()
    dte_min   = weeks_min * 7
    dte_max   = weeks_max * 7
    today_str = datetime.now(tz=ET).strftime("%Y-%m-%d")

    from collar import run_collar_on_demand
    result  = run_collar_on_demand(symbol, dte_min=dte_min, dte_max=dte_max)
    recs    = result["recommendations"]
    holding = result.get("holding", {})

    collar_meta = {
        "run_date":              today_str,
        "recipient_email":       config.get("recipient_email", ""),
        "duration_sec":          0,
        "eligible_holdings":     1,
        "total_recommendations": len(recs),
        "symbols_with_collars":  len({r["symbol"] for r in recs}),
        "low_gain_count":        sum(1 for r in recs if r.get("low_gain")),
        "earnings_flags":        sum(1 for r in recs if r.get("earnings_date")),
    }

    from collar_emailer import _render_collar_html
    html_body    = _render_collar_html(recs, collar_meta)
    preview_path = BASE_DIR / "logs" / f"collar_on_demand_{symbol.upper()}_{today_str}.html"
    preview_path.parent.mkdir(exist_ok=True)
    preview_path.write_text(html_body)

    # ── Console summary ───────────────────────────────────────────────────────
    price     = holding.get("price", 0)
    contracts = holding.get("contracts", 0)
    name      = holding.get("name", symbol)

    print(f"\n{'='*60}")
    print(f"Collar On-Demand: {symbol.upper()} — {name}")
    if price:
        print(f"Price: ${price:.2f}  |  Contracts: {contracts}  |  Window: {weeks_min}–{weeks_max} weeks")
    print(f"{'='*60}")

    if not recs:
        print("No collar opportunities found in this window.\n")
    else:
        print(f"{len(recs)} recommendation(s):\n")
        for rec in recs:
            low_tag  = "  ⚠ below $0.10 threshold" if rec.get("low_gain") else ""
            earn_tag = f"  ⚠ Earnings: {rec['earnings_date']}" if rec.get("earnings_date") else ""
            print(
                f"  Covered Call  {rec['cc_expiration']} ({rec['cc_dte']}d)"
                f"  strike ${rec['call_leg']['strike']:.2f}"
                f"  mid ${rec['call_leg']['mid']:.2f}"
                f"  +{rec['upside_cap_pct']:.1f}% OTM"
                f"  OI {rec['call_leg']['open_interest']}"
            )
            print(
                f"  Long Put      {rec['lp_expiration']} ({rec['lp_dte']}d)"
                f"  strike ${rec['put_leg']['strike']:.2f}"
                f"  mid ${rec['put_leg']['mid']:.2f}"
                f"  {rec['downside_floor_pct']:.1f}% floor"
                f"  OI {rec['put_leg']['open_interest']}"
            )
            print(
                f"  Net: ${rec['net_gain_per_share']:.2f}/share"
                f"  Total: ${rec['net_gain_total']:.0f}"
                f"{low_tag}{earn_tag}"
            )
            print()

    print(f"HTML preview → {preview_path}\n")


def run_collar_pipeline_and_email(dry_run: bool = False):
    """
    Execute the full collar recommendation pipeline and send the email.
    Can be called directly (--collar / --collar-dry-run) or by the scheduler.
    """
    start_ts  = datetime.now(tz=ET)
    today_str = start_ts.strftime("%Y-%m-%d")

    logger.info(f"{'='*60}")
    logger.info(f"Collar pipeline start {'[DRY RUN]' if dry_run else ''} — "
                f"{start_ts.strftime('%Y-%m-%d %H:%M:%S ET')}")

    config = load_config()

    try:
        from collar import run_collar_pipeline
        result = run_collar_pipeline(dry_run=dry_run)
        recs   = result["recommendations"]

        symbols_with_collars = len({r["symbol"] for r in recs})
        low_gain_count       = sum(1 for r in recs if r.get("low_gain"))
        earnings_flags       = sum(1 for r in recs if r.get("earnings_date"))

        collar_meta = {
            "run_date":              today_str,
            "recipient_email":       config.get("recipient_email", ""),
            "duration_sec":          round((datetime.now(tz=ET) - start_ts).total_seconds(), 1),
            "eligible_holdings":     result["eligible_count"],
            "total_recommendations": len(recs),
            "symbols_with_collars":  symbols_with_collars,
            "low_gain_count":        low_gain_count,
            "earnings_flags":        earnings_flags,
        }

        from collar_emailer import send_collar_report
        email_ok = send_collar_report(recs, collar_meta, dry_run=dry_run)

        logger.info(
            f"{'='*60}\n"
            f"Collar pipeline complete — {len(recs)} rec(s) "
            f"across {symbols_with_collars} symbol(s)\n"
            f"  Low-gain fallbacks: {low_gain_count}\n"
            f"  Earnings flags:     {earnings_flags}\n"
            f"  Email: {'sent' if email_ok else 'FAILED'}"
        )

    except Exception as e:
        logger.error(f"Collar pipeline failed: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
# Scheduler daemon
# ─────────────────────────────────────────────────────────────────────────────

def job_daily_pipeline():
    """Scheduled daily pipeline job — skips non-trading days."""
    if not _is_trading_day():
        logger.info(f"Daily pipeline skipped — {date.today()} is not a trading day")
        return
    run_pipeline(dry_run=False)


def job_weekly_collar():
    """Scheduled Saturday collar pipeline job."""
    run_collar_pipeline_and_email(dry_run=False)


_PID_FILE = BASE_DIR / "scheduler.pid"


def _acquire_pid_lock() -> None:
    """Exit if another scheduler instance is already running.

    Writes a PID file on success; removes it via atexit when this process exits.
    Prevents launchd from accidentally running two overlapping instances.
    """
    if _PID_FILE.exists():
        try:
            old_pid = int(_PID_FILE.read_text().strip())
            os.kill(old_pid, 0)   # signal 0 = check if process exists (no signal sent)
            # Process is alive — refuse to start
            print(
                f"[scheduler] ERROR: another instance is already running (PID {old_pid}). "
                "Exiting to prevent duplicate runs.",
                file=sys.stderr,
            )
            sys.exit(1)
        except (ValueError, ProcessLookupError, PermissionError):
            # Stale PID file (process gone) — safe to overwrite
            pass

    _PID_FILE.write_text(str(os.getpid()))
    atexit.register(lambda: _PID_FILE.unlink(missing_ok=True))


def start_scheduler():
    """
    Start the blocking scheduler daemon.
    Runs:
      - Daily 2:30 AM ET (trading days only): Robinhood portfolio pull
      - Daily 10:15 AM ET (trading days only): Covered-call pipeline
      - Saturday 10:00 AM ET: Collar recommendations pipeline
    """
    _acquire_pid_lock()
    setup_logging()          # <-- MUST be called here; without this all logs are silently dropped
    config = load_config()
    pipeline_time_et = config.get("pipeline_time_et", "10:15")
    pull_time_et     = config.get("portfolio_pull_time_et", "02:30")

    # schedule library uses local (PT) wall-clock time — convert from ET
    pipeline_time_local = _et_to_local(pipeline_time_et)
    pull_time_local     = _et_to_local(pull_time_et)

    logger.info(f"Scheduler starting...")
    logger.info(f"  Portfolio pull: {pull_time_et} ET  →  {pull_time_local} PT  (daily, trading days only)")
    logger.info(f"  Daily pipeline: {pipeline_time_et} ET  →  {pipeline_time_local} PT  (weekdays only)")

    # Daily portfolio pull — job itself skips non-trading days
    schedule.every().day.at(pull_time_local).do(job_daily_portfolio_pull)
    # Daily pipeline — job itself skips non-trading days
    schedule.every().day.at(pipeline_time_local).do(job_daily_pipeline)

    collar_time_et    = config.get("collar_pipeline_time_et", "10:00")
    collar_time_local = _et_to_local(collar_time_et)

    logger.info(f"  Collar pipeline: {collar_time_et} ET  →  {collar_time_local} PT  (Saturdays)")

    # Weekly collar report — every Saturday
    schedule.every().saturday.at(collar_time_local).do(job_weekly_collar)

    logger.info("Scheduler running. Press Ctrl+C to stop.")

    try:
        while True:
            schedule.run_pending()
            time.sleep(30)   # check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
