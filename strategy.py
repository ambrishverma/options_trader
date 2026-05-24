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
# Example: | 3 | **NVDA** | $198K | Beat, dip | Put Credit Spread (PCS) | CCS — sell calls above $260 |
_TABLE_ROW_RE = re.compile(
    r"^\|\s*\d+\s*\|"               # | # |
    r"\s*\*{0,2}(\w+)\*{0,2}\s*\|"  # | **TICKER** |  (captures TICKER)
    r"[^|]*\|"                       # | ~Value |
    r"[^|]*\|"                       # | Event Signal |
    r"[^|]*\|"                       # | Primary Strategy |
    r"\s*(.+?)\s*\|"                 # | Alt (PCS or CCS) |  (captures alt text)
)

# Regex for "PCS — sell puts below $290" or "CCS — sell calls above $260"
_ALT_RE = re.compile(
    r"(PCS|CCS)\s*[—–-]\s*sell\s+(puts|calls)\s+(below|above)\s+\$?([\d,]+(?:\.\d+)?)",
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

    # Find the "Summary Strategy Table" section
    table_start = content.find("## Summary Strategy Table")
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
        alt_text = m.group(2).strip()

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
