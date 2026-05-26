# Reports Consolidation — Design Spec

**Date:** 2026-05-26
**Version:** 1.0
**Status:** Draft

## Overview

Merge the Collar Report email and the Daily-run email into a single **"Daily Options Report"** email. The two pipelines (`run_pipeline()` and `run_collar_pipeline_and_email()`) become one unified pipeline with a strict four-phase execution order. One HTML template, one emailer path, one scheduler job at 10:15 AM ET.

## Goals

1. One email instead of two — collar/CCS/PCS sections flow first, then covered calls and protection modes
2. Strict sequential execution: data collection → collar & spreads → covered calls → protection modes → earnings enrichment → send
3. Unified HTML template with consistent styling
4. Retire `collar_emailer.py` and `templates/collar_email.html`
5. Tighter, cleaner report layout

## Execution Order

### Phase 0 — Collect Data

Shared data collection for all subsequent phases.

| Step | Action | Source |
|------|--------|--------|
| 0a | Load portfolio | `portfolio.get_portfolio()` |
| 0b | Load open positions snapshots | `load_open_calls_snapshot()`, `load_open_calls_detail_snapshot()`, `load_open_puts_detail_snapshot()`, `load_open_longs_detail_snapshot()`, `load_open_spreads_detail_snapshot()` |
| 0c | Fetch options chains | `options_chain.fetch_all_options()` (28-day lookahead for all eligible holdings) |

**Why Phase 0 is separate:** Both the collar pipeline and the CC pipeline need portfolio data and open positions. Fetching once avoids duplicate Robinhood API calls.

### Phase 1 — Collars & Spreads

| Step | Action | Source |
|------|--------|--------|
| 1a | Collar scan | `collar.run_collar_pipeline()` |
| 1b | CCS + PCS scan | `spread_scanner.run_spread_weekly_pipeline()` |
| 1c | Intraday direction filter (collars only) | `_get_intraday_changes()` — CCS/PCS are NOT filtered |

**CCS/PCS quality filter** (preserved from `collar_emailer.py`): Suppress recs below `$50` net credit total or below `0.25` credit-to-loss ratio. This filtering happens when building email data, not during scanning.

### Phase 2 — Covered Calls

| Step | Action | Source |
|------|--------|--------|
| 2a | Adjust holdings (subtract open calls), compute PUR | Inline logic in `run_pipeline()` |
| 2b | Apply Safe Mode filters | `filters.run_filters()` |
| 2c | Build 50/50 diversified recommendations | `diversifier.build_recommendations()` |

### Phase 3 — Protection Modes (strict order)

**Pre-step:** Build roll-forward and BTC candidate lists BEFORE running protection
modes, because each mode filters contracts out of these lists as it acts on them.

| Step | Action | Source |
|------|--------|--------|
| 3.0 | Build roll-forward and BTC candidate lists | `build_roll_forward_candidates()`, `build_btc_candidates()` |
| 3a.1 | Spread Safety (for PCS + CCS) | `execute_spread_mode("safety", sp_type)` |
| 3a.2 | Spread Rescue (for PCS + CCS) | `execute_spread_mode("rescue", sp_type)` |
| 3a.3 | Spread Panic (for PCS + CCS) | `execute_spread_mode("panic", sp_type)` |
| 3b.1 | Optimize — roll contracts up >40% gain | `execute_optimize_rolls()` |
| 3b.2 | Safety — auto-BTC unprotected ≤10 DTE | `execute_safety_btc_orders()` |
| 3b.3 | Rescue — max-credit roll DTE 1-2 ITM | `execute_rescue_rolls()` |
| 3b.4 | Panic — auto-roll DTE-0 ITM | `execute_panic_rolls()` |
| 3c | Strategy recommendations | `parse_strategy_table()` + `scan_strategy_recommendations()` |

Each downstream mode excludes contracts already acted on by upstream modes (via
acted-keys set tracking). Safety/rescue/panic keys filter the roll/BTC candidate
lists as today.

**Spread management rename:** The existing `execute_spread_mode("optimize", ...)` is renamed to `execute_spread_mode("safety", ...)`. This requires:
- Renaming the `mode == "optimize"` branch in `execute_spread_mode()` to `mode == "safety"`
- Updating the mode string in the `action["mode"]` field
- Updating template labels from "Spread Optimize" to "Spread Safety"
- Updating CLI `--spread-optimize` to `--spread-safety` (and argparse flag)

The existing cascade logic is preserved: each downstream mode excludes contracts already acted on by upstream modes via `optimize_acted_keys` (renamed to track all acted-on contracts).

### Phase 4 — Finalize & Send

| Step | Action | Source |
|------|--------|--------|
| 4a | Earnings warnings + ex-dividend dates | `build_earnings_warnings()`, `add_ex_dividend_dates()`, `annotate_candidates_with_earnings()` applied to ALL sections: collar recs, CC recs, CCS/PCS recs, roll candidates, BTC candidates, all action results |
| 4b | CCS/PCS quality filter | Suppress recs below $50 net credit total or 0.25 C/L ratio (moved from `collar_emailer.py` into pipeline, applied before passing to emailer) |
| 4c | Send unified email | `emailer.send_recommendations()` with all data |

## Email Template

### Section Order

1. **Header**: "📊 Daily Options Report" — date, combined stats
2. **Combined summary bar**: collar count, CCS count, PCS count, CC rec count, PUR%, Total YPD, earnings flags
3. **🛡 Collar Recommendations** — same table structure (symbol groups, CC + LP rows, net gain cell)
4. **📉 CCS Recommendations** — summary bar + per-symbol tables (short call + long call rows)
5. **📈 PCS Recommendations** — summary bar + per-symbol tables (short put + long put rows)
6. **📊 Covered Call Recommendations** — yield + safety legs table (same as today)
7. **🔁 Roll-Forward Candidates** — optimize actions, panic actions, rescue actions, candidate table
8. **💰 Buy-to-Close Candidates** — safety BTC actions, spread safety/rescue/panic actions, candidate table
9. **🧭 Strategy Recommendations** — PCS/CCS from daily briefing
10. **Footer**: disclaimer, run metadata (date, duration, mode)

### Style Unification

The unified template uses one CSS block based on the daily template's style system:
- Consistent `font-family`, `background`, `color` scheme across all sections
- Same `table.recs` styling for collar tables (currently collar_email.html has its own)
- Same `.section-header` pattern for CCS/PCS sections
- Same `.summary-bar` component for all summary bars
- `max-width: 900px` container (collar template uses 900px, daily uses 860px — use 900px)

### Layout Tightening

- Reduce `.tbl-wrap` padding from `20px 28px` to `16px 24px`
- Reduce section-header padding from `14px 28px 10px` to `12px 24px 8px`
- Reduce empty-state padding
- Single footer (no duplicate disclaimers)

### Subject Line

Combined format:
```
📊 Daily Options — {date} — {n} CC recs | {collar_n} collars | {ccs_n} CCS, {pcs_n} PCS
```

Plus existing dynamic indicators appended:
- `| 🚀 {n} optimize roll(s)` / `| ⚠️ {n} OPTIMIZE ROLL FAILED`
- `| ⚡ {n} panic roll(s)` / `| 🚨 {n} PANIC ROLL FAILED`
- `| 🎯 {n} rescue roll(s)` / `| ⚠️ {n} RESCUE ROLL FAILED`
- `| 🛡 {n} safety BTC(s)` / `| ⚠️ {n} safety BTC failed`
- `| ⚠️ {n} earnings warning(s)`
- `| 📐 {n} spread safety/rescue/panic`

When collar count is 0 (e.g., due to direction filter), that segment is omitted from the subject.

## Scheduler Changes

### Remove separate collar job

`start_scheduler()` currently schedules:
- `job_daily_pipeline()` at 10:15 AM ET
- `job_daily_collar()` at 10:30 AM ET

After consolidation:
- `job_daily_pipeline()` at 10:15 AM ET (handles everything)
- `job_daily_collar()` — **removed**

### Config cleanup

Remove `collar_pipeline_time_et` from `config.yaml` — no longer used.

### Watchdog timeout

Current: CC pipeline 50 min, collar 20 min. Combined pipeline gets 60 min to account for both workloads.

## CLI Changes

### `--run` / `--dry-run`

Runs the full combined pipeline (collar + CCS/PCS + CC + protection modes + email). This is the primary entry point.

### `--collar` / `--collar-dry-run`

**Kept as standalone.** Runs collar scan + CCS/PCS scan only, prints results to terminal. Does NOT send email. This is useful for ad-hoc collar analysis.

`cmd_collar()` continues to call `run_collar_pipeline_and_email()` — but that function is refactored to only scan and print (no email sending). Rename to `run_collar_scan()` to reflect its new purpose.

### `--spread-safety` (replaces `--spread-optimize`)

Rename `--spread-optimize` to `--spread-safety` in argparse. The underlying function call changes from `execute_spread_mode("optimize", ...)` to `execute_spread_mode("safety", ...)`.

## File Changes Summary

| File | Type | Description |
|------|------|-------------|
| `scheduler.py` | Modify | Merge collar scan into `run_pipeline()`, refactor `run_collar_pipeline_and_email()` to scan-only, remove `job_daily_collar()` from schedule |
| `emailer.py` | Modify | Add collar/CCS/PCS params to `send_recommendations()` and `_render_html()`, update subject line |
| `templates/email.html` | Modify | Add collar/CCS/PCS sections, unify CSS, tighten layout |
| `trader.py` | Modify | Rename spread mode "optimize" to "safety" in `execute_spread_mode()` |
| `main.py` | Modify | Rename `--spread-optimize` to `--spread-safety`, update `cmd_collar()` to not send email |
| `config.yaml` | Modify | Remove `collar_pipeline_time_et` |
| `collar_emailer.py` | Delete | No longer needed |
| `templates/collar_email.html` | Delete | Sections absorbed into `email.html` |

## Error Handling

Individual phase failures are non-fatal — the pipeline logs the error and continues:

| Scenario | Behavior |
|----------|----------|
| Collar scan fails | Log error, skip collar section in email, continue to Phase 2 |
| CCS/PCS scan fails | Log error, skip CCS/PCS sections, continue |
| CC pipeline fails (no holdings / no options) | Log warning, empty CC section, continue to Phase 3 |
| Any protection mode fails | Log error for that mode, continue to next |
| Strategy recs fail | Log warning, skip strategy section |
| Earnings enrichment fails | Log warning, send email without earnings data |
| Email send fails | 3-attempt retry with 30s sleep (existing behavior) |
| Auth failure | Abort entire run (existing behavior) |

## Testing Strategy

### Existing tests

All existing tests for `emailer.py`, `scheduler.py`, `trader.py`, and template rendering must continue to pass. The `execute_spread_mode("optimize", ...)` calls in tests need updating to `"safety"`.

### New/modified test coverage

1. **Spread mode rename**: verify `execute_spread_mode("safety", ...)` triggers correctly
2. **Unified emailer**: verify `send_recommendations()` renders collar/CCS/PCS sections when data is provided
3. **Pipeline phases**: verify Phase 0 data is shared (not fetched twice)
4. **Subject line**: verify combined subject includes collar/CCS/PCS counts
5. **Empty sections**: verify collar=0 / CCS=0 / PCS=0 renders cleanly (empty state messages)
6. **CLI --collar standalone**: verify it scans without sending email

## Out of Scope

- Trade Report email — stays separate (daily 10 PM + weekly Saturday)
- Changes to collar.run_collar_pipeline() internal logic
- Changes to spread_scanner.run_spread_weekly_pipeline() internal logic
- Refactoring emailer fallback renderers (inline HTML / plain text) — only Jinja2 template updated
