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
            logger.warning("No eligible holdings — sending empty report")
            results["outcome"] = "no_eligible_holdings"

        # ── Step 2: Load open covered-call positions from morning snapshot ───────
        # The 2:30 AM portfolio pull (pull_daily_robinhood_snapshot) fetches both
        # the portfolio and open calls in ONE session, saving open_calls_YYYYMMDD.json.
        # Loading from that snapshot here avoids a second Robinhood login at
        # 10:15 AM, which triggers device-verification challenges and hangs.
        logger.info("[2/7] Loading open covered-call positions from snapshot...")
        from portfolio import load_open_calls_snapshot, load_open_calls_detail_snapshot
        open_calls        = load_open_calls_snapshot()
        open_calls_detail = load_open_calls_detail_snapshot()
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
            logger.warning("All holdings have open covered calls — sending empty report")
            results["outcome"] = "all_holdings_covered"

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

        # ── Steps 3-5: Options fetch, filter, recommendations ─────────────────
        # Initialise to empty/zero so the summary log and email always have values
        results["options_raw"]     = 0
        results["options_passing"] = 0
        results["recommendations"] = 0
        recommendations            = []
        portfolio_ypd              = 0.0

        if holdings:
            # ── Step 3: Fetch options chains ───────────────────────────────────
            logger.info("[3/7] Fetching options chains...")
            from options_chain import fetch_all_options
            raw_options = fetch_all_options(
                holdings,
                lookahead_days=config.get("lookahead_days", 28),
            )
            results["options_raw"] = len(raw_options)
            logger.info(f"  {len(raw_options)} raw option records fetched")

            # ── Step 4: Filter + rank ──────────────────────────────────────────
            logger.info("[4/7] Applying Safe Mode filters...")
            from filters import run_filters
            filter_result = run_filters(raw_options, config)
            results["options_passing"] = filter_result["count_passing"]
            results["filter_rejected"] = filter_result.get("rejected_counts", {})
            logger.info(
                f"  {filter_result['count_passing']}/{filter_result['count_raw']} options passed filters"
            )

            if not filter_result["all_passing"]:
                logger.warning("No options passed filters — sending empty report")
                results["outcome"] = "no_options_passed"
            else:
                # ── Step 5: Build diversified recommendations ──────────────────
                logger.info("[5/7] Building 50/50 diversified recommendations...")
                from diversifier import build_recommendations
                recommendations = build_recommendations(filter_result, config)
                results["recommendations"] = len(recommendations)
                logger.info(f"  {len(recommendations)} recommendations built")

                portfolio_ypd = round(
                    sum(r.get("combined_ypd", 0) for r in recommendations), 2
                )
                results["portfolio_ypd"] = portfolio_ypd
        else:
            logger.info("[3-5/7] Skipped — no eligible holdings to write new calls against")

        # ── Step 6: Earnings warnings + ex-dividend dates ─────────────────────
        logger.info("[6/7] Checking earnings calendar and ex-dividend dates...")
        from earnings import build_earnings_warnings, add_ex_dividend_dates
        recommendations = build_earnings_warnings(recommendations)
        recommendations = add_ex_dividend_dates(recommendations)
        flagged = sum(1 for r in recommendations if r.get("earnings_flag"))
        results["earnings_flagged"] = flagged
        logger.info(f"  {flagged} earnings warnings")

        # ── Step 6b: Roll-forward and BTC candidates ──────────────────────────
        live_prices = {h["symbol"]: h["price"] for h in holdings_all}
        name_map    = {h["symbol"]: h["name"]  for h in holdings_all}
        from portfolio import load_open_spreads_detail_snapshot
        open_spreads_detail = load_open_spreads_detail_snapshot()
        from roll_monitor import build_roll_forward_candidates, build_btc_candidates
        roll_candidates = build_roll_forward_candidates(
            open_calls_detail, live_prices, name_map,
            spread_contracts=open_spreads_detail,
        )
        btc_candidates = build_btc_candidates(
            open_calls_detail, live_prices, name_map,
            spread_contracts=open_spreads_detail,
        )
        results["roll_candidates"] = len(roll_candidates)
        results["btc_candidates"]  = len(btc_candidates)
        logger.info(
            f"  Roll-forward: {len(roll_candidates)} candidate(s)  |  "
            f"BTC: {len(btc_candidates)} candidate(s)"
        )

        # ── Step 6c: Panic mode — auto-roll DTE-0 ITM contracts ───────────────
        from trader import execute_panic_rolls
        panic_results = execute_panic_rolls(
            open_calls_detail, live_prices, name_map, dry_run=dry_run
        )
        if panic_results:
            n_ok  = sum(1 for p in panic_results if p["success"])
            n_err = len(panic_results) - n_ok
            logger.warning(
                f"[PANIC MODE] Processed {len(panic_results)} DTE-0 ITM contract(s): "
                f"{n_ok} rolled ✅  {n_err} failed ❌"
            )
            results["panic_rolls_ok"]  = n_ok
            results["panic_rolls_err"] = n_err
            # Remove panic-handled contracts from roll_candidates to avoid duplication
            panic_keys = {(p["symbol"], p["expiration"]) for p in panic_results}
            roll_candidates = [
                c for c in roll_candidates
                if (c.get("symbol"), c.get("expiration")) not in panic_keys
            ]

        # ── Step 6d: Rescue mode — max-credit roll for DTE-1-2 ITM contracts ────
        rescue_results = []
        from trader import execute_rescue_rolls
        rescue_results = execute_rescue_rolls(
            open_calls_detail, live_prices, name_map, dry_run=dry_run
        )
        if rescue_results:
            acted = [g for g in rescue_results if not g.get("skipped")]
            n_ok   = sum(1 for g in acted if g["success"])
            n_err  = len(acted) - n_ok
            n_skip = len(rescue_results) - len(acted)
            logger.info(
                f"[RESCUE MODE] Processed {len(rescue_results)} DTE-1-2 ITM contract(s): "
                f"{n_ok} rolled ✅  {n_err} failed ❌  {n_skip} skipped (no credit)"
            )
            results["rescue_rolls_ok"]   = n_ok
            results["rescue_rolls_err"]  = n_err
            results["rescue_rolls_skip"] = n_skip
            # Remove rescue-acted contracts from roll_candidates (skipped ones stay)
            rescue_keys = {
                (g["symbol"], g["expiration"])
                for g in rescue_results if not g.get("skipped")
            }
            roll_candidates = [
                c for c in roll_candidates
                if (c.get("symbol"), c.get("expiration")) not in rescue_keys
            ]

        # ── Step 6e: Safety mode — auto-BTC contracts expiring ≤ 10 days ────────
        # Exclude rescue-acted contracts (their orders were cancelled; if roll failed,
        # user needs to act manually — safety BTC would place a duplicate risk order)
        rescue_acted_keys = {
            (g["symbol"], g["expiration"])
            for g in rescue_results
            if not g.get("skipped")
        } if rescue_results else set()
        open_calls_for_safety = [
            c for c in open_calls_detail
            if (c.get("symbol", "").upper(), c.get("expiration", "")) not in rescue_acted_keys
        ]
        from trader import execute_safety_btc_orders
        safety_results = execute_safety_btc_orders(
            open_calls_for_safety, live_prices, name_map, dry_run=dry_run
        )
        if safety_results:
            n_ok  = sum(1 for s in safety_results if s["success"])
            n_err = len(safety_results) - n_ok
            logger.info(
                f"[SAFETY MODE] Processed {len(safety_results)} contract(s): "
                f"{n_ok} BTC placed ✅  {n_err} failed ❌"
            )
            results["safety_btc_ok"]  = n_ok
            results["safety_btc_err"] = n_err
            # Remove safety-actioned contracts from btc_candidates (DTE 5–10 overlap)
            safety_keys = {(s["symbol"], s["expiration"]) for s in safety_results}
            btc_candidates = [
                c for c in btc_candidates
                if (c.get("symbol"), c.get("expiration")) not in safety_keys
            ]

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

        email_ok = send_recommendations(
            recommendations, run_meta, dry_run=dry_run,
            roll_candidates=roll_candidates, btc_candidates=btc_candidates,
            panic_results=panic_results, rescue_results=rescue_results,
            safety_results=safety_results,
        )
        results["email_sent"] = email_ok

        end_ts = datetime.now(tz=ET)
        duration = (end_ts - start_ts).total_seconds()
        results["duration_sec"] = round(duration, 1)
        results["completed_at"] = end_ts.isoformat()
        # Preserve an early-exit outcome code (no_eligible_holdings, no_options_passed, …)
        # Only mark "success" when the pipeline produced recommendations normally.
        if not results.get("outcome"):
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

def run_cc_on_demand_and_preview(
    symbol: str,
    buffer_size_pct: float = None,
    target_premium: float = None,
    weeks_min: int = 0,
    weeks_max: int = 6,
):
    """
    On-demand covered-call scan for a single symbol.
    Finds the single best call option (highest YPD) respecting the given params.
    Saves an HTML preview and prints a console summary.
    """
    from utils import setup_logging
    from options_chain import fetch_options_for_symbol, get_live_price

    config    = load_config()
    today_str = datetime.now(tz=ET).strftime("%Y-%m-%d")
    symbol    = symbol.upper()

    # Resolve defaults from config
    eff_buffer  = buffer_size_pct if buffer_size_pct is not None else config.get("min_otm_pct", 7.0)
    lookahead   = weeks_max * 7

    # Build a synthetic holding for the scan
    live_price = get_live_price(symbol) or 0.0
    if live_price <= 0:
        print(f"\n❌  Could not fetch price for {symbol}. Check ticker and try again.")
        return

    holding = {
        "symbol": symbol, "name": symbol,
        "shares": 100.0, "price": live_price,
        "eligible": True, "contracts": 1,
    }

    # Try to look up name from portfolio
    try:
        from portfolio import get_portfolio
        for h in get_portfolio():
            if h["symbol"] == symbol:
                holding = h
                live_price = h.get("price", live_price)
                break
    except Exception:
        pass

    logger.info(f"On-demand CC scan: {symbol} | buffer={eff_buffer}% | "
                f"DTE {weeks_min*7}–{weeks_max*7}d")

    options = fetch_options_for_symbol(holding, lookahead_days=lookahead)

    # Apply buffer + DTE min filter
    dte_min = weeks_min * 7
    qualified = [
        o for o in options
        if o["otm_pct"] >= eff_buffer
        and o["dte"] >= dte_min
        and o["mid"] > 0
    ]

    # Apply target premium filter if given
    if target_premium is not None:
        qualified = [o for o in qualified if o["mid"] >= target_premium]
    else:
        # Default: 1% of stock price
        min_prem = round(live_price * 0.01, 2)
        qualified = [o for o in qualified if o["mid"] >= min_prem]

    # Compute YPD for each and pick best
    for o in qualified:
        o["ypd"] = round(o["mid"] * 100 / max(o["dte"], 1), 4)

    qualified.sort(key=lambda o: o["ypd"], reverse=True)
    best = qualified[0] if qualified else None

    # Console summary
    print(f"\n{'='*60}")
    print(f"Covered Call On-Demand: {symbol}")
    print(f"Current Price: ${live_price:.2f}  |  Buffer: {eff_buffer}%  |  "
          f"Window: {weeks_min}–{weeks_max} weeks")
    print(f"{'='*60}")

    if not best:
        print("No qualifying covered call found in this window.\n")
    else:
        ypd_total = round(best["ypd"] * holding.get("contracts", 1), 2)
        print(f"Best call option found:\n")
        print(f"  Strike:     ${best['strike']:.2f}  (+{best['otm_pct']:.1f}% OTM)")
        print(f"  Expiry:     {best['expiration']}  ({best['dte']}d)")
        print(f"  Bid/Ask:    ${best['bid']:.2f} / ${best['ask']:.2f}")
        print(f"  Mid:        ${best['mid']:.2f}/share  →  ${best['mid']*100:.0f}/contract")
        print(f"  OI:         {best['open_interest']}")
        print(f"  YPD:        ${best['ypd']:.2f}/day  (${ypd_total:.2f}/day × {holding.get('contracts',1)} contract(s))")
        print()

    # HTML preview (minimal)
    rec_rows = ""
    for o in qualified[:10]:
        rec_rows += (
            f"<tr>"
            f"<td>${o['strike']:.2f}</td>"
            f"<td>{o['expiration']}</td>"
            f"<td>{o['dte']}d</td>"
            f"<td>${o['bid']:.2f}</td>"
            f"<td>${o['ask']:.2f}</td>"
            f"<td style='font-weight:600'>${o['mid']:.2f}</td>"
            f"<td>+{o['otm_pct']:.1f}%</td>"
            f"<td>{o['open_interest']}</td>"
            f"<td style='font-weight:600'>${o['ypd']:.2f}</td>"
            f"</tr>\n"
        )
    html_body = f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:20px">
<h2>Covered Call On-Demand — {symbol} — {today_str}</h2>
<p>Price: <b>${live_price:.2f}</b> &nbsp;|&nbsp; Buffer: {eff_buffer}% &nbsp;|&nbsp; Window: {weeks_min}–{weeks_max} weeks</p>
{'<p style="color:#22c55e;font-weight:bold">No qualifying options found.</p>' if not qualified else f'<p>{len(qualified)} qualifying option(s). Top 10 by YPD:</p>'}
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-size:13px">
<thead><tr style="background:#334155;color:white">
<th>Strike</th><th>Expiry</th><th>DTE</th><th>Bid</th><th>Ask</th><th>Mid</th><th>OTM%</th><th>OI</th><th>YPD</th>
</tr></thead>
<tbody>{rec_rows}</tbody></table>
<p style="font-size:11px;color:#888">Automated analysis — not financial advice.</p>
</body></html>"""

    preview_path = BASE_DIR / "logs" / f"cc_on_demand_{symbol}_{today_str}.html"
    preview_path.parent.mkdir(exist_ok=True)
    preview_path.write_text(html_body)
    print(f"HTML preview → {preview_path}\n")


def run_ccs_on_demand_and_preview(
    symbol: str,
    spread_size_min: float = None,
    spread_size_max: float = None,
    buffer_size_pct: float = None,
    target_premium: float = None,
    weeks_min: int = 2,
    weeks_max: int = 6,
):
    """
    On-demand Call Credit Spread scan for a single symbol.
    Saves an HTML preview and prints a console summary.
    Evaluates all spread widths from spread_size_min to spread_size_max
    (in 1%-of-price steps) and returns the highest-YPD combination.
    buffer_size_pct overrides config spread_short_otm_pct when provided.
    """
    config    = load_config()
    today_str = datetime.now(tz=ET).strftime("%Y-%m-%d")
    symbol    = symbol.upper()

    dte_min = weeks_min * 7
    dte_max = weeks_max * 7

    # Resolve name from portfolio if available
    name = symbol
    try:
        from portfolio import get_portfolio
        for h in get_portfolio():
            if h["symbol"] == symbol:
                name = h.get("name", symbol)
                break
    except Exception:
        pass

    from spread_scanner import scan_ccs

    # buffer_size_pct (CLI --buffer-size) overrides config; fall back to config default
    short_otm     = buffer_size_pct if buffer_size_pct is not None else float(config.get("spread_short_otm_pct", 10.0))
    min_oi        = int(config.get("spread_min_open_interest",    2))
    size_min_pct  = float(config.get("spread_size_min_pct",     1.0))
    size_max_pct  = float(config.get("spread_size_max_pct",    10.0))
    prem_pct      = float(config.get("spread_min_premium_pct",  1.0))

    logger.info(f"On-demand CCS scan: {symbol} | DTE {dte_min}–{dte_max}d | OTM≥{short_otm}%")

    rec, _ = scan_ccs(
        symbol, name=name,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        target_premium=target_premium,
        dte_min=dte_min, dte_max=dte_max,
        short_otm_pct=short_otm, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
        min_premium_pct=prem_pct,
    )

    print(f"\n{'='*60}")
    print(f"Call Credit Spread (Bear Call Spread) On-Demand: {symbol}")
    print(f"{'='*60}")

    if not rec:
        print("No qualifying CCS found in this window.\n")
    else:
        sl = rec["short_leg"]
        ll = rec["long_leg"]
        print(f"Current Price:  ${rec['current_price']:.2f}")
        print(f"Expiry:         {rec['expiration']}  ({rec['dte']}d)")
        print(f"Short Call:     ${sl['strike']:.2f}  bid=${sl['bid']:.2f}  OI={sl['open_interest']}  OTM={sl['otm_pct']:.1f}%")
        print(f"Long  Call:     ${ll['strike']:.2f}  ask=${ll['ask']:.2f}  OI={ll['open_interest']}")
        print(f"Spread Width:   ${rec['spread_size']:.2f}")
        print(f"Net Credit:     ${rec['net_credit']:.2f}/share  →  ${rec['net_credit_total']:.0f}/contract")
        print(f"Max Loss:       ${rec['max_loss']:.0f}/contract")
        print(f"YPD:            ${rec['ypd']:.2f}/day/contract")
        print()

    # HTML preview
    html_body = _render_spread_preview_html(
        rec, "CCS", symbol, today_str, weeks_min, weeks_max
    )
    preview_path = BASE_DIR / "logs" / f"ccs_on_demand_{symbol}_{today_str}.html"
    preview_path.parent.mkdir(exist_ok=True)
    preview_path.write_text(html_body)
    print(f"HTML preview → {preview_path}\n")


def run_pcs_on_demand_and_preview(
    symbol: str,
    spread_size_min: float = None,
    spread_size_max: float = None,
    buffer_size_pct: float = None,
    target_premium: float = None,
    weeks_min: int = 2,
    weeks_max: int = 6,
):
    """
    On-demand Put Credit Spread scan for a single symbol.
    Saves an HTML preview and prints a console summary.
    Evaluates all spread widths from spread_size_min to spread_size_max
    (in 1%-of-price steps) and returns the highest-YPD combination.
    buffer_size_pct overrides config spread_short_otm_pct when provided.
    """
    config    = load_config()
    today_str = datetime.now(tz=ET).strftime("%Y-%m-%d")
    symbol    = symbol.upper()

    dte_min = weeks_min * 7
    dte_max = weeks_max * 7

    name = symbol
    try:
        from portfolio import get_portfolio
        for h in get_portfolio():
            if h["symbol"] == symbol:
                name = h.get("name", symbol)
                break
    except Exception:
        pass

    from spread_scanner import scan_pcs

    # buffer_size_pct (CLI --buffer-size) overrides config; fall back to config default
    short_otm     = buffer_size_pct if buffer_size_pct is not None else float(config.get("spread_short_otm_pct", 10.0))
    min_oi        = int(config.get("spread_min_open_interest",    2))
    size_min_pct  = float(config.get("spread_size_min_pct",     1.0))
    size_max_pct  = float(config.get("spread_size_max_pct",    10.0))
    prem_pct      = float(config.get("spread_min_premium_pct",  1.0))

    logger.info(f"On-demand PCS scan: {symbol} | DTE {dte_min}–{dte_max}d | OTM≥{short_otm}%")

    rec, _ = scan_pcs(
        symbol, name=name,
        spread_size_min=spread_size_min, spread_size_max=spread_size_max,
        target_premium=target_premium,
        dte_min=dte_min, dte_max=dte_max,
        short_otm_pct=short_otm, min_open_interest=min_oi,
        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
        min_premium_pct=prem_pct,
    )

    print(f"\n{'='*60}")
    print(f"Put Credit Spread (Bull Put Spread) On-Demand: {symbol}")
    print(f"{'='*60}")

    if not rec:
        print("No qualifying PCS found in this window.\n")
    else:
        sl = rec["short_leg"]
        ll = rec["long_leg"]
        print(f"Current Price:  ${rec['current_price']:.2f}")
        print(f"Expiry:         {rec['expiration']}  ({rec['dte']}d)")
        print(f"Short Put:      ${sl['strike']:.2f}  bid=${sl['bid']:.2f}  OI={sl['open_interest']}  OTM={sl['otm_pct']:.1f}%")
        print(f"Long  Put:      ${ll['strike']:.2f}  ask=${ll['ask']:.2f}  OI={ll['open_interest']}")
        print(f"Spread Width:   ${rec['spread_size']:.2f}")
        print(f"Net Credit:     ${rec['net_credit']:.2f}/share  →  ${rec['net_credit_total']:.0f}/contract")
        print(f"Max Loss:       ${rec['max_loss']:.0f}/contract")
        print(f"YPD:            ${rec['ypd']:.2f}/day/contract")
        print()

    html_body = _render_spread_preview_html(
        rec, "PCS", symbol, today_str, weeks_min, weeks_max
    )
    preview_path = BASE_DIR / "logs" / f"pcs_on_demand_{symbol}_{today_str}.html"
    preview_path.parent.mkdir(exist_ok=True)
    preview_path.write_text(html_body)
    print(f"HTML preview → {preview_path}\n")


def _render_spread_preview_html(rec: dict, spread_type: str, symbol: str, today_str: str, weeks_min: int, weeks_max: int) -> str:
    """Render a minimal HTML preview for a CCS or PCS on-demand scan result."""
    header_color = "#1e3a5f" if spread_type == "CCS" else "#14532d"
    label        = "Call Credit Spread (Bear Call Spread)" if spread_type == "CCS" else "Put Credit Spread (Bull Put Spread)"
    short_label  = "Short Call" if spread_type == "CCS" else "Short Put"
    long_label   = "Long Call"  if spread_type == "CCS" else "Long Put"

    if not rec:
        body = '<p style="color:#64748b;font-size:14px;">No qualifying spread found in this window.</p>'
    else:
        sl = rec["short_leg"]
        ll = rec["long_leg"]
        body = f"""
<table border="1" cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-size:13px;width:100%;max-width:640px">
<thead><tr style="background:{header_color};color:white">
<th>Leg</th><th>Strike</th><th>Bid</th><th>Ask</th><th>Mid</th><th>OI</th><th>OTM%</th>
</tr></thead>
<tbody>
<tr style="background:#eff6ff">
  <td><b>{short_label}</b></td>
  <td>${sl['strike']:.2f}</td>
  <td>${sl['bid']:.2f}</td>
  <td>${sl['ask']:.2f}</td>
  <td>${sl['mid']:.2f}</td>
  <td>{sl['open_interest']}</td>
  <td>{sl['otm_pct']:.1f}%</td>
</tr>
<tr style="background:#f0fdf4">
  <td><b>{long_label}</b></td>
  <td>${ll['strike']:.2f}</td>
  <td>${ll['bid']:.2f}</td>
  <td>${ll['ask']:.2f}</td>
  <td>${ll['mid']:.2f}</td>
  <td>{ll['open_interest']}</td>
  <td>—</td>
</tr>
</tbody></table>
<table style="margin-top:16px;font-size:13px;border-collapse:collapse">
<tr><td style="padding:4px 12px 4px 0;color:#64748b">Expiry</td><td><b>{rec['expiration']}</b> ({rec['dte']}d)</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#64748b">Current Price</td><td><b>${rec['current_price']:.2f}</b></td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#64748b">Spread Width</td><td>${rec['spread_size']:.2f}</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#64748b">Net Credit</td><td style="color:#15803d;font-weight:bold">${rec['net_credit']:.2f}/share &nbsp;→&nbsp; ${rec['net_credit_total']:.0f}/contract</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#64748b">Max Loss</td><td style="color:#dc2626">${rec['max_loss']:.0f}/contract</td></tr>
<tr><td style="padding:4px 12px 4px 0;color:#64748b">YPD</td><td style="font-weight:bold">${rec['ypd']:.2f}/day/contract</td></tr>
</table>"""

    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;padding:20px;background:#f1f5f9">
<div style="max-width:700px;background:white;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08)">
<div style="background:{header_color};color:white;padding:16px 24px">
<h2 style="margin:0;font-size:18px">{label}</h2>
<p style="margin:4px 0 0;opacity:0.7;font-size:12px">{symbol} &nbsp;|&nbsp; {today_str} &nbsp;|&nbsp; Window: {weeks_min}–{weeks_max} weeks</p>
</div>
<div style="padding:20px 24px">{body}</div>
<div style="padding:12px 24px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0">
Automated analysis — not financial advice.
</div>
</div>
</body></html>"""


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
    html_body    = _render_collar_html(recs, collar_meta, ccs_recs=[], pcs_recs=[])
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
    Execute the full collar recommendation pipeline (collars + CCS + PCS) and send the email.
    Can be called directly (--collar / --collar-dry-run) or by the scheduler.
    """
    start_ts  = datetime.now(tz=ET)
    today_str = start_ts.strftime("%Y-%m-%d")

    logger.info(f"{'='*60}")
    logger.info(f"Collar pipeline start {'[DRY RUN]' if dry_run else ''} — "
                f"{start_ts.strftime('%Y-%m-%d %H:%M:%S ET')}")

    config = load_config()

    try:
        # ── Collar recommendations ─────────────────────────────────────────────
        from collar import run_collar_pipeline
        result = run_collar_pipeline(dry_run=dry_run)
        recs   = result["recommendations"]

        symbols_with_collars = len({r["symbol"] for r in recs})
        low_gain_count       = sum(1 for r in recs if r.get("low_gain"))
        earnings_flags       = sum(1 for r in recs if r.get("earnings_date"))

        # ── CCS + PCS spread recommendations (all portfolio holdings) ─────────
        logger.info("Running spread weekly pipeline (CCS + PCS)...")
        from portfolio import get_portfolio
        from spread_scanner import run_spread_weekly_pipeline
        all_holdings  = get_portfolio()
        spread_result = run_spread_weekly_pipeline(all_holdings, config)
        ccs_recs      = spread_result.get("ccs", [])
        pcs_recs      = spread_result.get("pcs", [])
        ccs_scenarios = spread_result.get("ccs_scenarios", 0)
        pcs_scenarios = spread_result.get("pcs_scenarios", 0)
        logger.info(
            f"  CCS: {len(ccs_recs)} rec(s) [{ccs_scenarios} scenarios]  |  "
            f"PCS: {len(pcs_recs)} rec(s) [{pcs_scenarios} scenarios]"
        )

        collar_meta = {
            "run_date":              today_str,
            "recipient_email":       config.get("recipient_email", ""),
            "duration_sec":          round((datetime.now(tz=ET) - start_ts).total_seconds(), 1),
            "eligible_holdings":     result["eligible_count"],
            "total_recommendations": len(recs),
            "symbols_with_collars":  symbols_with_collars,
            "low_gain_count":        low_gain_count,
            "earnings_flags":        earnings_flags,
            "ccs_count":             len(ccs_recs),
            "pcs_count":             len(pcs_recs),
        }

        from collar_emailer import send_collar_report
        email_ok = send_collar_report(
            recs, collar_meta, dry_run=dry_run,
            ccs_recs=ccs_recs, pcs_recs=pcs_recs,
            ccs_scenarios=ccs_scenarios, pcs_scenarios=pcs_scenarios,
        )

        logger.info(
            f"{'='*60}\n"
            f"Collar pipeline complete — {len(recs)} collar rec(s) "
            f"across {symbols_with_collars} symbol(s)\n"
            f"  CCS rec(s):         {len(ccs_recs)}\n"
            f"  PCS rec(s):         {len(pcs_recs)}\n"
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
