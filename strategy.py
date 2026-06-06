"""
strategy.py — Daily Briefing Strategy Parser
=============================================
Reads the latest daily briefing file from the Claude-Cowork directory,
extracts PCS/CCS recommendations from the "Summary Strategy Table",
and returns actionable strategy dicts.

The briefing files live at:
  ~/Documents/Documents/Claude-Cowork/Daily briefings/daily-stocks-briefing-YYYY-MM-DD.md

The "Alt (PCS or CCS)" column contains entries like:
  "PCS — sell puts below $290"
  "CCS — sell calls above $260"

We parse these with regex first.  If a line doesn't match the expected
pattern we optionally call the Claude API to interpret it.
"""

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
    Take parsed strategy recommendations and run the CCS/PCS scanner for
    each symbol to find the best available contract.

    For each parsed rec (e.g. ``{"symbol": "NVDA", "spread_type": "CCS", ...}``),
    this calls ``scan_ccs`` or ``scan_pcs`` from ``spread_scanner`` with the
    same parameters used in the daily collar pipeline.

    Parameters
    ----------
    parsed_recs : list of dicts from ``parse_strategy_table()``
    config      : loaded config dict (for spread_ keys); uses defaults if None

    Returns
    -------
    List of full scanner-result dicts (same shape as ``run_spread_weekly_pipeline``
    output).  Each dict is enriched with a ``"strategy_hint"`` key containing the
    original parsed recommendation text.  Recs that produced no qualifying
    contract are omitted.
    """
    from spread_scanner import scan_ccs, scan_pcs
    import time as _time

    config = config or {}
    dte_min      = int(config.get("spread_dte_min",            14))
    dte_max      = int(config.get("spread_dte_max",            42))
    short_otm    = float(config.get("spread_short_otm_pct",  10.0))
    min_oi       = int(config.get("spread_min_open_interest",   2))
    size_min_pct = float(config.get("spread_size_min_pct",    1.0))
    size_max_pct = float(config.get("spread_size_max_pct",   10.0))
    premium_pct  = float(config.get("spread_min_premium_pct", 1.0))

    results: list[dict] = []

    for rec in parsed_recs:
        symbol      = rec["symbol"]
        spread_type = rec["spread_type"]
        hint_strike = rec.get("strike")      # e.g. 400.0
        hint_action = rec.get("action", "")  # e.g. "sell puts below"

        logger.info(
            f"  [STRATEGY] Scanning {symbol} for {spread_type} "
            f"(hint: {rec.get('raw_text', 'N/A')})..."
        )

        # Per-symbol try/except with retry — prevents one symbol's yfinance
        # SQLite cache deadlock from killing the entire strategy scan.
        if spread_type not in ("CCS", "PCS"):
            logger.warning(f"  [STRATEGY] Unknown spread_type '{spread_type}' for {symbol}")
            continue

        contract = None
        scenarios = 0
        for _attempt in range(2):
            try:
                if spread_type == "CCS":
                    strike_min = hint_strike if ("above" in hint_action and hint_strike) else None
                    contract, scenarios = scan_ccs(
                        symbol,
                        dte_min=dte_min, dte_max=dte_max,
                        short_otm_pct=short_otm, min_open_interest=min_oi,
                        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
                        min_premium_pct=premium_pct,
                        short_strike_min_hint=strike_min,
                    )
                else:  # PCS
                    strike_max = hint_strike if ("below" in hint_action and hint_strike) else None
                    contract, scenarios = scan_pcs(
                        symbol,
                        dte_min=dte_min, dte_max=dte_max,
                        short_otm_pct=short_otm, min_open_interest=min_oi,
                        spread_size_min_pct=size_min_pct, spread_size_max_pct=size_max_pct,
                        min_premium_pct=premium_pct,
                        short_strike_max_hint=strike_max,
                    )
                break  # success — exit retry loop
            except OSError as os_exc:
                if _attempt == 0:
                    logger.warning(f"  [STRATEGY] {symbol}: {os_exc} — retrying in 3s...")
                    _time.sleep(3)
                else:
                    logger.error(f"  [STRATEGY] {symbol}: failed after retry: {os_exc}")
            except Exception as scan_exc:
                logger.error(f"  [STRATEGY] {symbol}: scan error: {scan_exc}")
                break  # non-retryable error

        if contract:
            contract["strategy_hint"] = rec.get("raw_text", "")
            results.append(contract)
            logger.info(
                f"  [STRATEGY] {symbol} {spread_type}: "
                f"{contract['expiration']} ({contract['dte']}d) "
                f"net ${contract['net_credit']:.2f} YPD=${contract['ypd']:.2f}"
            )
        else:
            # Keep a stub so callers can report "no qualifying contract"
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
