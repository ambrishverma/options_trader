"""
strategy.py — Strategy Purchase Parser
=======================================
Primary data source: CSV file with explicit PCS/CCS/PDS/CDS columns.
  ~/Documents/Documents/Claude-Cowork/Daily briefings/Strategy-Purchase-DD-MM-YYYY.csv

Fallback data source (deprecated): Markdown daily briefing with PCS/CCS only.
  ~/Documents/Documents/Claude-Cowork/Daily briefings/daily-stocks-briefing-YYYY-MM-DD.md
"""

import csv
import re
import os
import logging
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

BRIEFINGS_DIR = Path.home() / "Documents" / "Documents" / "Claude-Cowork" / "Daily briefings"


# ─────────────────────────────────────────────────────────────────────────────
# File discovery
# ─────────────────────────────────────────────────────────────────────────────

def _find_briefing_file(target_date: Optional[date] = None) -> Optional[Path]:
    """
    Locate the daily briefing markdown file for *target_date*.
    Falls back to today if no date is given.
    Returns None if no matching file exists.
    """
    d = target_date or date.today()
    filename = f"daily-stocks-briefing-{d.isoformat()}.md"
    path = BRIEFINGS_DIR / filename
    if path.exists():
        return path
    return None


# ─────────────────────────────────────────────────────────────────────────────
# CSV-based purchase recommendations (primary data source)
# ─────────────────────────────────────────────────────────────────────────────

_SPREAD_COLUMNS = {
    "pcs ceiling": ("PCS", "sell puts below"),
    "ccs floor":   ("CCS", "sell calls above"),
    "pds ceiling": ("PDS", "buy puts below"),
    "cds floor":   ("CDS", "buy calls above"),
}


def _find_purchase_csv(target_date: Optional[date] = None) -> Optional[Path]:
    d = target_date or date.today()
    filename = f"Strategy-Purchase-{d.strftime('%d-%m-%Y')}.csv"
    path = BRIEFINGS_DIR / filename
    if path.exists():
        return path
    return None


def parse_purchase_csv(
    target_date: Optional[date] = None,
    filter_sym: Optional[str] = None,
) -> list[dict]:
    """
    Parse the Strategy-Purchase CSV file into hint dicts.

    CSV format: SYMBOL | PCS ceiling | CCS Floor | PDS ceiling | CDS Floor
    Cells contain a dollar price (e.g. "$210") or "-" for no recommendation.

    Returns list of dicts:
        {
            "symbol":       "NVDA",
            "spread_type":  "CCS",
            "action":       "sell calls above",
            "strike":       215.0,
            "raw_text":     "CCS floor $215",
        }
    """
    path = _find_purchase_csv(target_date)
    if path is None:
        d = target_date or date.today()
        logger.info(f"No strategy purchase CSV found for {d.strftime('%d-%m-%Y')}")
        return []

    logger.info(f"Reading strategy from CSV: {path.name}")
    recommendations: list[dict] = []

    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            logger.warning(f"CSV has no header row: {path.name}")
            return []

        col_map: dict[str, tuple[str, str]] = {}
        sym_col: Optional[str] = None
        for header in reader.fieldnames:
            key = header.strip().lower()
            if key in _SPREAD_COLUMNS:
                col_map[header] = _SPREAD_COLUMNS[key]
            elif key == "symbol":
                sym_col = header

        for row in reader:
            symbol = (row.get(sym_col, "") if sym_col else "").strip().upper()
            if not symbol:
                continue
            if filter_sym and symbol != filter_sym.upper():
                continue

            for col_name, (spread_type, action) in col_map.items():
                cell = row.get(col_name, "").strip()
                if not cell or cell == "-":
                    continue
                price_str = cell.replace("$", "").replace(",", "").strip()
                try:
                    strike = float(price_str)
                except ValueError:
                    logger.warning(f"  [{symbol}] Cannot parse '{cell}' as price in column '{col_name}'")
                    continue

                rec = {
                    "symbol":      symbol,
                    "spread_type": spread_type,
                    "action":      action,
                    "strike":      strike,
                    "raw_text":    f"{spread_type} {col_name.strip().split()[-1]} ${strike:g}",
                }
                recommendations.append(rec)
                logger.info(f"  [{symbol}] {spread_type} — {action} ${strike:.0f}")

    logger.info(f"Parsed {len(recommendations)} strategy recommendation(s) from CSV")
    return recommendations


# ─────────────────────────────────────────────────────────────────────────────
# Markdown table parsing (fallback — deprecated)
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# Table parsing
# ─────────────────────────────────────────────────────────────────────────────

# Regex for the markdown table row in "Summary Strategy Table"
# Captures the ticker (column 2) and the LAST column (alternate strategy).
# Column count varies across briefing formats (4–6 columns after #), so we
# grab everything after the ticker and then extract the final pipe-delimited cell.
_TABLE_ROW_RE = re.compile(
    r"^\|\s*\d+\s*\|"               # | # |
    r"\s*\*{0,2}([\w.]+)\*{0,2}\s*\|"  # | **TICKER** |  (captures TICKER, allows dots e.g. BRK.B)
    r"(.+)\|\s*$"                    # rest of row   (captures all remaining cells)
)

# Regex for "PCS — sell puts below $290" or "CCS — sell calls above $260"
# Also handles optional month name: "sell June puts below $290"
# Separator can be em-dash, en-dash, single hyphen, or double-hyphen (--)
_ALT_RE = re.compile(
    r"(PCS|CCS)\s*[—–-]+\s*sell\s+(?:\w+\s+)?(puts|calls)\s+(below|above)\s+\$?([\d,]+(?:\.\d+)?)",
    re.IGNORECASE,
)


def _parse_alt_recommendation(alt_text: str) -> Optional[dict]:
    """
    Parse an "Alt (PCS or CCS)" cell into a structured recommendation.

    Returns
    -------
    {
        "spread_type": "PCS" | "CCS",
        "action":      "sell puts below" | "sell calls above",
        "strike":      float,
    }
    or None if the text doesn't match PCS/CCS pattern.
    """
    m = _ALT_RE.search(alt_text)
    if not m:
        return None
    spread_type = m.group(1).upper()
    option_side = m.group(2).lower()   # "puts" or "calls"
    direction   = m.group(3).lower()   # "below" or "above"
    strike      = float(m.group(4).replace(",", ""))
    return {
        "spread_type": spread_type,
        "action":      f"sell {option_side} {direction}",
        "strike":      strike,
    }


def _parse_alt_with_llm(alt_text: str, symbol: str) -> Optional[dict]:
    """
    Fallback: use Claude API to interpret an ambiguous Alt recommendation.

    Returns the same dict shape as _parse_alt_recommendation, or None.
    """
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        logger.debug("ANTHROPIC_API_KEY not set — skipping LLM fallback")
        return None

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)

        prompt = (
            f"Parse this options strategy recommendation for {symbol} into structured data.\n"
            f"Recommendation: \"{alt_text}\"\n\n"
            f"If this is a PCS (Put Credit Spread) or CCS (Call Credit Spread) recommendation, "
            f"respond with EXACTLY one line in this format:\n"
            f"SPREAD_TYPE|ACTION|STRIKE\n"
            f"Example: PCS|sell puts below|290.0\n"
            f"Example: CCS|sell calls above|260.0\n\n"
            f"If this is NOT a PCS or CCS recommendation, respond with: SKIP"
        )

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=100,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()

        if text.upper() == "SKIP":
            return None

        parts = text.split("|")
        if len(parts) == 3:
            spread_type = parts[0].strip().upper()
            if spread_type in ("PCS", "CCS"):
                return {
                    "spread_type": spread_type,
                    "action":      parts[1].strip().lower(),
                    "strike":      float(parts[2].strip()),
                }
    except Exception as e:
        logger.warning(f"LLM fallback failed for {symbol}: {e}")

    return None


def parse_strategy_table(
    target_date: Optional[date] = None,
    filter_sym: Optional[str] = None,
    use_llm_fallback: bool = True,
) -> list[dict]:
    """
    Parse the Summary Strategy Table from the daily briefing file.

    Parameters
    ----------
    target_date     : date to look for (default: today)
    filter_sym      : if set, return only this symbol's recommendation
    use_llm_fallback: attempt Claude API for unrecognized patterns

    Returns
    -------
    List of dicts:
        {
            "symbol":       "NVDA",
            "spread_type":  "CCS",
            "action":       "sell calls above",
            "strike":       260.0,
            "raw_text":     "CCS — sell calls above $260",
        }
    """
    path = _find_briefing_file(target_date)
    if path is None:
        d = target_date or date.today()
        logger.warning(f"No strategy briefing file found for {d}")
        return []

    logger.info(f"Reading strategy from: {path.name}")
    content = path.read_text(encoding="utf-8")

    # Find the "Summary Strategy Table" section (case-insensitive, may have
    # extra text after the title, e.g. "## SUMMARY STRATEGY TABLE — Strategy Recommendations")
    table_start = -1
    for m_hdr in re.finditer(r"^## .*summary\s+strategy\s+table", content, re.IGNORECASE | re.MULTILINE):
        table_start = m_hdr.start()
        break
    if table_start == -1:
        logger.warning("No 'Summary Strategy Table' section found in briefing")
        return []

    # Extract lines from table start until next section or end
    table_section = content[table_start:]
    next_section = table_section.find("\n## ", 1)
    if next_section > 0:
        table_section = table_section[:next_section]

    recommendations: list[dict] = []

    for line in table_section.split("\n"):
        m = _TABLE_ROW_RE.match(line)
        if not m:
            continue

        symbol = m.group(1).upper()
        # Extract the last pipe-delimited cell as the alternate strategy
        remaining_cells = [c.strip() for c in m.group(2).split("|") if c.strip()]
        alt_text = remaining_cells[-1] if remaining_cells else ""

        if filter_sym and symbol != filter_sym.upper():
            continue

        # Try regex parse first
        rec = _parse_alt_recommendation(alt_text)

        # Fallback to LLM if regex didn't match
        if rec is None and use_llm_fallback:
            rec = _parse_alt_with_llm(alt_text, symbol)

        if rec is None:
            logger.info(f"  [{symbol}] Alt '{alt_text}' — not a PCS/CCS, skipping")
            continue

        rec["symbol"]   = symbol
        rec["raw_text"] = alt_text
        recommendations.append(rec)
        logger.info(
            f"  [{symbol}] {rec['spread_type']} — {rec['action']} ${rec['strike']:.0f}"
        )

    logger.info(f"Parsed {len(recommendations)} PCS/CCS strategy recommendation(s)")
    return recommendations


# ─────────────────────────────────────────────────────────────────────────────
# Scanner integration — convert parsed recs into full contract recommendations
# ─────────────────────────────────────────────────────────────────────────────

def scan_strategy_recommendations(
    parsed_recs: list[dict],
    config: dict = None,
) -> list[dict]:
    """
    Take parsed strategy recommendations and run the appropriate scanner for
    each symbol to find the best available contract.

    Supports all four spread types: CCS, PCS, PDS, CDS.
    """
    from spread_scanner import scan_ccs, scan_pcs, scan_pds, scan_cds
    import time as _time

    config = config or {}

    # Credit spread params (PCS/CCS)
    cs_dte_min      = int(config.get("spread_dte_min",            14))
    cs_dte_max      = int(config.get("spread_dte_max",            42))
    cs_short_otm    = float(config.get("spread_short_otm_pct",  10.0))
    cs_min_oi       = int(config.get("spread_min_open_interest",   2))
    cs_size_min_pct = float(config.get("spread_size_min_pct",    1.0))
    cs_size_max_pct = float(config.get("spread_size_max_pct",   10.0))
    cs_premium_pct  = float(config.get("spread_min_premium_pct", 1.0))

    # Debit spread params (PDS/CDS)
    ds_dte_min      = int(config.get("debit_dte_min",              30))
    ds_dte_max      = int(config.get("debit_dte_max",              60))
    ds_min_oi       = int(config.get("debit_min_open_interest",     2))
    ds_size_min_pct = float(config.get("debit_spread_size_min_pct", 5.0))
    ds_size_max_pct = float(config.get("debit_spread_size_max_pct", 25.0))
    ds_max_debit    = float(config.get("debit_max_debit_pct",      25.0)) / 100
    ds_leg_offset   = float(config.get("debit_long_leg_offset_pct", 5.0)) / 100
    ds_max_dpd_pct  = float(config.get("debit_max_dpd_pct",        10.0)) / 100

    results: list[dict] = []

    for rec in parsed_recs:
        symbol      = rec["symbol"]
        spread_type = rec["spread_type"]
        hint_strike = rec.get("strike")
        hint_action = rec.get("action", "")

        logger.info(
            f"  [STRATEGY] Scanning {symbol} for {spread_type} "
            f"(hint: {rec.get('raw_text', 'N/A')})..."
        )

        if spread_type not in ("CCS", "PCS", "PDS", "CDS"):
            logger.warning(f"  [STRATEGY] Unknown spread_type '{spread_type}' for {symbol}")
            continue

        contract = None
        scenarios = 0
        for _attempt in range(3):
            try:
                if spread_type == "CCS":
                    strike_min = hint_strike if ("above" in hint_action and hint_strike) else None
                    contract, scenarios = scan_ccs(
                        symbol,
                        dte_min=cs_dte_min, dte_max=cs_dte_max,
                        short_otm_pct=cs_short_otm, min_open_interest=cs_min_oi,
                        spread_size_min_pct=cs_size_min_pct, spread_size_max_pct=cs_size_max_pct,
                        min_premium_pct=cs_premium_pct,
                        short_strike_min_hint=strike_min,
                    )
                elif spread_type == "PCS":
                    strike_max = hint_strike if ("below" in hint_action and hint_strike) else None
                    contract, scenarios = scan_pcs(
                        symbol,
                        dte_min=cs_dte_min, dte_max=cs_dte_max,
                        short_otm_pct=cs_short_otm, min_open_interest=cs_min_oi,
                        spread_size_min_pct=cs_size_min_pct, spread_size_max_pct=cs_size_max_pct,
                        min_premium_pct=cs_premium_pct,
                        short_strike_max_hint=strike_max,
                    )
                elif spread_type == "PDS":
                    strike_max = hint_strike if hint_strike else None
                    contract, scenarios = scan_pds(
                        symbol,
                        dte_min=ds_dte_min, dte_max=ds_dte_max,
                        min_open_interest=ds_min_oi,
                        spread_size_min_pct=ds_size_min_pct, spread_size_max_pct=ds_size_max_pct,
                        max_debit_pct=ds_max_debit,
                        long_leg_offset=ds_leg_offset, max_dpd_pct=ds_max_dpd_pct,
                        long_strike_max_hint=strike_max,
                    )
                else:  # CDS
                    strike_min = hint_strike if hint_strike else None
                    contract, scenarios = scan_cds(
                        symbol,
                        dte_min=ds_dte_min, dte_max=ds_dte_max,
                        min_open_interest=ds_min_oi,
                        spread_size_min_pct=ds_size_min_pct, spread_size_max_pct=ds_size_max_pct,
                        max_debit_pct=ds_max_debit,
                        long_leg_offset=ds_leg_offset, max_dpd_pct=ds_max_dpd_pct,
                        long_strike_min_hint=strike_min,
                    )
                break  # success — exit retry loop
            except Exception as exc:
                from utils import _is_cache_corruption, nuke_yfinance_cache
                recoverable = "deadlock" in str(exc).lower() or _is_cache_corruption(exc)
                if _attempt < 2 and recoverable:
                    delay = [5, 15][_attempt]
                    logger.warning(f"  [STRATEGY] {symbol}: {exc} — clearing cache, retrying in {delay}s...")
                    nuke_yfinance_cache()
                    _time.sleep(delay)
                elif _attempt == 2 and recoverable:
                    logger.error(f"  [STRATEGY] {symbol}: failed after 3 attempts: {exc}")
                else:
                    logger.error(f"  [STRATEGY] {symbol}: scan error: {exc}")
                    break

        if contract:
            contract["strategy_hint"] = rec.get("raw_text", "")
            results.append(contract)
            if spread_type in ("PCS", "CCS"):
                logger.info(
                    f"  [STRATEGY] {symbol} {spread_type}: "
                    f"{contract['expiration']} ({contract['dte']}d) "
                    f"net ${contract['net_credit']:.2f} YPD=${contract['ypd']:.2f}"
                )
            else:
                logger.info(
                    f"  [STRATEGY] {symbol} {spread_type}: "
                    f"{contract['expiration']} ({contract['dte']}d) "
                    f"net debit ${contract['net_debit']:.2f} DPD=${contract['dpd']:.4f}"
                )
        else:
            results.append({
                "symbol":         symbol,
                "type":           spread_type,
                "strategy_hint":  rec.get("raw_text", ""),
                "no_contract":    True,
                "scenarios":      scenarios,
            })
            logger.info(
                f"  [STRATEGY] {symbol} {spread_type}: "
                f"no qualifying contract found ({scenarios} scenarios evaluated)"
            )

    found    = [r for r in results if not r.get("no_contract")]
    no_match = [r for r in results if r.get("no_contract")]
    logger.info(
        f"[STRATEGY] Scanned {len(parsed_recs)} hint(s) → "
        f"{len(found)} contract(s), {len(no_match)} no-match"
    )
    return results
