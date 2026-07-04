"""
test_strategy.py — Tests for strategy.py
=========================================
Tests cover:
  - parse_purchase_csv()        CSV-based strategy parsing (primary)
  - _find_briefing_file()       file discovery (fallback)
  - _parse_alt_recommendation() regex-based parsing (fallback)
  - _parse_alt_with_llm()       Claude API fallback
  - parse_strategy_table()      full table parse + symbol filter
  - scan_strategy_recommendations() for PCS/CCS/PDS/CDS
  - CLI --strategy flag
  - Email template rendering of strategy_recs
"""

import sys
import os
import textwrap
import pytest
from unittest.mock import patch, MagicMock
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from strategy import (
    _find_briefing_file,
    _find_purchase_csv,
    _parse_alt_recommendation,
    _parse_alt_with_llm,
    parse_purchase_csv,
    parse_strategy_table,
    scan_strategy_recommendations,
    _TABLE_ROW_RE,
    _ALT_RE,
    BRIEFINGS_DIR,
)


# ─────────────────────────────────────────────────────────────────────────────
# CSV-based purchase recommendations (primary data source)
# ─────────────────────────────────────────────────────────────────────────────

MOCK_CSV_CONTENT = """\
Symbol,PCS ceiling,CCS Floor,PDS ceiling,CDS Floor
INTU,$210,$290,-,-
TSLA,-,$440,$380,-
MSFT,$340,-,-,$420
GOOG,$310,$380,-,-
"""


class TestFindPurchaseCsv:
    def test_csv_found(self, tmp_path):
        d = date(2026, 7, 2)
        (tmp_path / "Strategy-Purchase-02-07-2026.csv").write_text(MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            result = _find_purchase_csv(d)
        assert result is not None
        assert result.name == "Strategy-Purchase-02-07-2026.csv"

    def test_csv_missing(self, tmp_path):
        d = date(2026, 7, 2)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            result = _find_purchase_csv(d)
        assert result is None


class TestParsePurchaseCsv:
    def _write_csv(self, tmp_path, d: date, content: str):
        fname = f"Strategy-Purchase-{d.strftime('%d-%m-%Y')}.csv"
        (tmp_path / fname).write_text(content)

    def test_parses_all_spread_types(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        assert len(recs) == 8
        types = {(r["symbol"], r["spread_type"]) for r in recs}
        assert ("INTU", "PCS") in types
        assert ("INTU", "CCS") in types
        assert ("TSLA", "CCS") in types
        assert ("TSLA", "PDS") in types
        assert ("MSFT", "PCS") in types
        assert ("MSFT", "CDS") in types
        assert ("GOOG", "PCS") in types
        assert ("GOOG", "CCS") in types

    def test_pcs_fields(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        intu_pcs = next(r for r in recs if r["symbol"] == "INTU" and r["spread_type"] == "PCS")
        assert intu_pcs["action"] == "sell puts below"
        assert intu_pcs["strike"] == 210.0
        assert "raw_text" in intu_pcs

    def test_pds_fields(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        tsla_pds = next(r for r in recs if r["symbol"] == "TSLA" and r["spread_type"] == "PDS")
        assert tsla_pds["action"] == "buy puts below"
        assert tsla_pds["strike"] == 380.0

    def test_cds_fields(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        msft_cds = next(r for r in recs if r["symbol"] == "MSFT" and r["spread_type"] == "CDS")
        assert msft_cds["action"] == "buy calls above"
        assert msft_cds["strike"] == 420.0

    def test_filter_by_symbol(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d, filter_sym="TSLA")
        assert all(r["symbol"] == "TSLA" for r in recs)
        assert len(recs) == 2

    def test_dashes_skipped(self, tmp_path):
        d = date(2026, 7, 2)
        self._write_csv(tmp_path, d, MOCK_CSV_CONTENT)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d, filter_sym="INTU")
        types = [r["spread_type"] for r in recs]
        assert "PDS" not in types
        assert "CDS" not in types

    def test_missing_csv_returns_empty(self, tmp_path):
        d = date(2026, 1, 1)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        assert recs == []

    def test_comma_in_price(self, tmp_path):
        d = date(2026, 7, 2)
        csv = "Symbol,PCS ceiling,CCS Floor,PDS ceiling,CDS Floor\nMU,\"$1,350\",-,-,-\n"
        self._write_csv(tmp_path, d, csv)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        assert len(recs) == 1
        assert recs[0]["strike"] == 1350.0

    def test_no_dollar_sign(self, tmp_path):
        d = date(2026, 7, 2)
        csv = "Symbol,PCS ceiling,CCS Floor,PDS ceiling,CDS Floor\nAAPL,250,-,-,-\n"
        self._write_csv(tmp_path, d, csv)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_purchase_csv(target_date=d)
        assert recs[0]["strike"] == 250.0


# ─────────────────────────────────────────────────────────────────────────────
# Shared test fixtures — full scanner-result dicts
# ─────────────────────────────────────────────────────────────────────────────

def _make_scanner_rec(
    symbol="NVDA",
    spread_type="CCS",
    current_price=260.0,
    short_strike=290.0,
    long_strike=300.0,
    net_credit=1.50,
    strategy_hint="CCS — sell calls above $260",
):
    """Build a mock scanner result dict (same shape as scan_ccs/scan_pcs output)."""
    is_ccs = spread_type == "CCS"
    return {
        "symbol":        symbol,
        "name":          symbol,
        "current_price": current_price,
        "type":          spread_type,
        "expiration":    "2026-06-20",
        "dte":           28,
        "short_leg": {
            "strike":        short_strike,
            "bid":           2.50,
            "ask":           2.80,
            "mid":           2.65,
            "open_interest": 150,
            "otm_pct":       round((short_strike / current_price - 1) * 100, 1) if is_ccs
                             else round((1 - short_strike / current_price) * 100, 1),
        },
        "long_leg": {
            "strike":        long_strike,
            "bid":           0.90,
            "ask":           1.10,
            "mid":           1.00,
            "open_interest": 80,
        },
        "net_credit":            net_credit,
        "net_credit_total":      net_credit * 100,
        "spread_size":           abs(long_strike - short_strike),
        "max_loss":              abs(long_strike - short_strike) * 100 - net_credit * 100,
        "ypd":                   round(net_credit * 100 / 28, 2),
        "credit_to_loss_ratio":  round(net_credit / (abs(long_strike - short_strike) - net_credit), 2),
        "score":                 1.5,
        "strategy_hint":         strategy_hint,
    }


def _make_debit_rec(
    symbol="TSLA",
    spread_type="PDS",
    current_price=400.0,
    long_strike=380.0,
    short_strike=350.0,
    net_debit=3.50,
    strategy_hint="PDS ceiling $380",
):
    """Build a mock debit spread result dict (same shape as scan_pds/scan_cds output)."""
    spread_size = abs(long_strike - short_strike)
    dte = 45
    return {
        "symbol":           symbol,
        "name":             symbol,
        "current_price":    current_price,
        "type":             spread_type,
        "expiration":       "2026-08-15",
        "dte":              dte,
        "long_leg": {
            "strike":        long_strike,
            "bid":           4.00,
            "ask":           4.50,
            "mid":           4.25,
            "open_interest": 100,
            "otm_pct":       round(abs(long_strike / current_price - 1) * 100, 1),
        },
        "short_leg": {
            "strike":        short_strike,
            "bid":           0.80,
            "ask":           1.00,
            "mid":           0.90,
            "open_interest": 60,
        },
        "net_debit":            net_debit,
        "net_debit_total":      net_debit * 100,
        "spread_size":          spread_size,
        "max_protection":       (spread_size - net_debit) * 100,
        "dpd":                  round(net_debit * 100 / dte, 4),
        "debit_to_win_ratio":   round(net_debit / spread_size, 4),
        "score":                0.005,
        "strategy_hint":        strategy_hint,
    }


# ─────────────────────────────────────────────────────────────────────────────
# _parse_alt_recommendation — regex parser
# ─────────────────────────────────────────────────────────────────────────────

class TestParseAltRecommendation:
    """Test regex-based parsing of Alt (PCS or CCS) column values."""

    def test_pcs_standard(self):
        result = _parse_alt_recommendation("PCS — sell puts below $290")
        assert result is not None
        assert result["spread_type"] == "PCS"
        assert result["action"] == "sell puts below"
        assert result["strike"] == 290.0

    def test_ccs_standard(self):
        result = _parse_alt_recommendation("CCS — sell calls above $260")
        assert result is not None
        assert result["spread_type"] == "CCS"
        assert result["action"] == "sell calls above"
        assert result["strike"] == 260.0

    def test_pcs_with_comma_in_price(self):
        result = _parse_alt_recommendation("PCS — sell puts below $1,250")
        assert result is not None
        assert result["spread_type"] == "PCS"
        assert result["strike"] == 1250.0

    def test_ccs_with_decimal_price(self):
        result = _parse_alt_recommendation("CCS — sell calls above $145.50")
        assert result is not None
        assert result["spread_type"] == "CCS"
        assert result["strike"] == 145.50

    def test_en_dash_separator(self):
        result = _parse_alt_recommendation("PCS – sell puts below $300")
        assert result is not None
        assert result["spread_type"] == "PCS"
        assert result["strike"] == 300.0

    def test_hyphen_separator(self):
        result = _parse_alt_recommendation("CCS - sell calls above $180")
        assert result is not None
        assert result["spread_type"] == "CCS"
        assert result["strike"] == 180.0

    def test_case_insensitive(self):
        result = _parse_alt_recommendation("pcs — Sell Puts Below $200")
        assert result is not None
        assert result["spread_type"] == "PCS"

    def test_no_dollar_sign(self):
        result = _parse_alt_recommendation("PCS — sell puts below 290")
        assert result is not None
        assert result["strike"] == 290.0

    def test_non_matching_text(self):
        assert _parse_alt_recommendation("Buy calls") is None

    def test_empty_string(self):
        assert _parse_alt_recommendation("") is None

    def test_partial_match_no_strike(self):
        assert _parse_alt_recommendation("PCS — sell puts") is None

    def test_irrelevant_strategy(self):
        assert _parse_alt_recommendation("Iron Condor $280-$320") is None

    def test_hold_no_alt(self):
        assert _parse_alt_recommendation("Hold / no alt") is None

    def test_pcs_with_month_name(self):
        result = _parse_alt_recommendation("PCS — sell June puts below $290")
        assert result is not None
        assert result["spread_type"] == "PCS"
        assert result["action"] == "sell puts below"
        assert result["strike"] == 290.0

    def test_ccs_with_month_name(self):
        result = _parse_alt_recommendation("CCS — sell July calls above $260")
        assert result is not None
        assert result["spread_type"] == "CCS"
        assert result["action"] == "sell calls above"
        assert result["strike"] == 260.0


# ─────────────────────────────────────────────────────────────────────────────
# _find_briefing_file — file discovery
# ─────────────────────────────────────────────────────────────────────────────

class TestFindBriefingFile:
    """Test file discovery logic."""

    def test_file_exists(self, tmp_path):
        """Should return the path when the file exists."""
        d = date(2026, 5, 20)
        fname = f"daily-stocks-briefing-{d.isoformat()}.md"
        (tmp_path / fname).write_text("# test")

        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            result = _find_briefing_file(d)
            assert result is not None
            assert result.name == fname

    def test_file_missing(self, tmp_path):
        """Should return None when the file doesn't exist."""
        d = date(2026, 1, 1)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            result = _find_briefing_file(d)
            assert result is None

    def test_defaults_to_today(self, tmp_path):
        """Should use today's date when no date is given."""
        today = date.today()
        fname = f"daily-stocks-briefing-{today.isoformat()}.md"
        (tmp_path / fname).write_text("# today")

        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            result = _find_briefing_file()
            assert result is not None
            assert result.name == fname


# ─────────────────────────────────────────────────────────────────────────────
# _TABLE_ROW_RE — table row regex
# ─────────────────────────────────────────────────────────────────────────────

class TestTableRowRegex:
    """Test the markdown table row regex extracts symbol and last-column alt text."""

    @staticmethod
    def _extract_alt(line):
        """Helper: match regex and extract last cell (same as parse_strategy_table)."""
        m = _TABLE_ROW_RE.match(line)
        if not m:
            return None, None
        symbol = m.group(1).upper()
        remaining = [c.strip() for c in m.group(2).split("|") if c.strip()]
        alt = remaining[-1] if remaining else ""
        return symbol, alt

    def test_standard_row_5_cols(self):
        line = "| 3 | **NVDA** | $198K | Beat, dip | Put Credit Spread (PCS) | CCS — sell calls above $260 |"
        sym, alt = self._extract_alt(line)
        assert sym == "NVDA"
        assert alt == "CCS — sell calls above $260"

    def test_standard_row_4_cols(self):
        line = "| 1 | INTU | 20% crash, 17% layoffs | Put Debit Spread ($310/$280) | PCS -- sell puts below $280 |"
        sym, alt = self._extract_alt(line)
        assert sym == "INTU"
        assert alt == "PCS -- sell puts below $280"

    def test_row_without_bold(self):
        line = "| 1 | AAPL | $95K | Earnings | PCS | PCS — sell puts below $170 |"
        sym, alt = self._extract_alt(line)
        assert sym == "AAPL"
        assert alt == "PCS — sell puts below $170"

    def test_separator_row_does_not_match(self):
        line = "|---|------|-------|-----------|------------|-----|"
        m = _TABLE_ROW_RE.match(line)
        assert m is None

    def test_header_row_does_not_match(self):
        line = "| # | Symbol | Value | Signal | Strategy | Alt |"
        m = _TABLE_ROW_RE.match(line)
        assert m is None


# ─────────────────────────────────────────────────────────────────────────────
# parse_strategy_table — full pipeline
# ─────────────────────────────────────────────────────────────────────────────

MOCK_BRIEFING = textwrap.dedent("""\
    # Daily Stocks Briefing — 2026-05-20

    Some intro text here.

    ## Summary Strategy Table

    | # | Symbol | ~Value | Event Signal | Primary Strategy | Alt (PCS or CCS) |
    |---|--------|--------|-------------|-----------------|-------------------|
    | 1 | **AAPL** | $95K | Beat, gap up | Covered Call | PCS — sell puts below $170 |
    | 2 | **NVDA** | $198K | Beat, dip | PCS | CCS — sell calls above $260 |
    | 3 | **TSLA** | $50K | Miss | Hold | Hold / no alt |
    | 4 | **MSFT** | $120K | Beat | CC | PCS — sell puts below $400 |

    ## Next Section
    Other content.
""")


class TestParseStrategyTable:
    """Test the main parse_strategy_table() function."""

    def _write_briefing(self, tmp_path, d: date, content: str):
        fname = f"daily-stocks-briefing-{d.isoformat()}.md"
        (tmp_path / fname).write_text(content)

    def test_parses_all_pcs_ccs(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        assert len(recs) == 3
        symbols = [r["symbol"] for r in recs]
        assert "AAPL" in symbols
        assert "NVDA" in symbols
        assert "MSFT" in symbols
        # TSLA should be skipped (no PCS/CCS)
        assert "TSLA" not in symbols

    def test_pcs_fields(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        aapl = next(r for r in recs if r["symbol"] == "AAPL")
        assert aapl["spread_type"] == "PCS"
        assert aapl["action"] == "sell puts below"
        assert aapl["strike"] == 170.0
        assert "raw_text" in aapl

    def test_ccs_fields(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        nvda = next(r for r in recs if r["symbol"] == "NVDA")
        assert nvda["spread_type"] == "CCS"
        assert nvda["action"] == "sell calls above"
        assert nvda["strike"] == 260.0

    def test_filter_by_symbol(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, filter_sym="AAPL", use_llm_fallback=False)
        assert len(recs) == 1
        assert recs[0]["symbol"] == "AAPL"

    def test_filter_case_insensitive(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, filter_sym="nvda", use_llm_fallback=False)
        assert len(recs) == 1
        assert recs[0]["symbol"] == "NVDA"

    def test_filter_no_match(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, filter_sym="XYZ", use_llm_fallback=False)
        assert recs == []

    def test_missing_file_returns_empty(self, tmp_path):
        d = date(2026, 1, 1)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d)
        assert recs == []

    def test_no_strategy_table_section(self, tmp_path):
        d = date(2026, 5, 20)
        content = "# Briefing\n\nNo table here.\n"
        self._write_briefing(tmp_path, d, content)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d)
        assert recs == []

    def test_four_column_format(self, tmp_path):
        """Handles briefings with 4 columns after # (no ~Value column)."""
        d = date(2026, 5, 24)
        content = textwrap.dedent("""\
            ## Summary Strategy Table

            | # | Ticker | Event Summary | Primary Strategy | Alternate Strategy |
            |---|--------|---------------|------------------|--------------------|
            | 1 | INTU | 20% crash | Put Debit Spread ($310/$280) | PCS -- sell puts below $280 |
            | 2 | NVDA | Blowout Q1 | CCS -- sell calls above $290 | CCS -- sell calls above $300 |
        """)
        self._write_briefing(tmp_path, d, content)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        assert len(recs) == 2
        intu = next(r for r in recs if r["symbol"] == "INTU")
        assert intu["spread_type"] == "PCS"
        assert intu["strike"] == 280.0
        nvda = next(r for r in recs if r["symbol"] == "NVDA")
        assert nvda["spread_type"] == "CCS"
        assert nvda["strike"] == 300.0

    def test_skips_non_pcs_ccs_rows(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        # TSLA "Hold / no alt" should not appear
        assert all(r["symbol"] != "TSLA" for r in recs)

    def test_case_insensitive_section_header(self, tmp_path):
        """Header like 'SUMMARY STRATEGY TABLE — extra text' should be found."""
        d = date(2026, 5, 20)
        content = textwrap.dedent("""\
            # Briefing

            ## SUMMARY STRATEGY TABLE — Strategy Recommendations

            | # | Ticker | Holding | Event | Primary Strategy | Alternate (PCS/CCS) |
            |---|--------|---------|-------|-----------------|---------------------|
            | 1 | NVDA | $198K | Beat | CCS — sell calls above $260 | CCS — sell calls above $260 |
        """)
        self._write_briefing(tmp_path, d, content)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        assert len(recs) == 1
        assert recs[0]["symbol"] == "NVDA"
        assert recs[0]["spread_type"] == "CCS"


# ─────────────────────────────────────────────────────────────────────────────
# _parse_alt_with_llm — Claude API fallback
# ─────────────────────────────────────────────────────────────────────────────

class TestParseAltWithLLM:
    """Test the LLM fallback parser."""

    def test_returns_none_without_api_key(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = _parse_alt_with_llm("some ambiguous text", "AAPL")
            assert result is None

    def _make_mock_anthropic(self, response_text):
        """Create a mock anthropic module with a mocked client."""
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text=response_text)]
        mock_module = MagicMock()
        mock_module.Anthropic.return_value.messages.create.return_value = mock_response
        return mock_module

    def test_parses_pcs_response(self):
        mock_mod = self._make_mock_anthropic("PCS|sell puts below|290.0")

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch.dict("sys.modules", {"anthropic": mock_mod}):
                result = _parse_alt_with_llm("sell 290P credit spread", "TSLA")

        assert result is not None
        assert result["spread_type"] == "PCS"
        assert result["action"] == "sell puts below"
        assert result["strike"] == 290.0

    def test_parses_ccs_response(self):
        mock_mod = self._make_mock_anthropic("CCS|sell calls above|260.0")

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch.dict("sys.modules", {"anthropic": mock_mod}):
                result = _parse_alt_with_llm("bear call spread above 260", "NVDA")

        assert result is not None
        assert result["spread_type"] == "CCS"
        assert result["strike"] == 260.0

    def test_skip_response(self):
        mock_mod = self._make_mock_anthropic("SKIP")

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch.dict("sys.modules", {"anthropic": mock_mod}):
                result = _parse_alt_with_llm("Iron Condor", "SPY")

        assert result is None

    def test_handles_api_exception(self):
        mock_mod = MagicMock()
        mock_mod.Anthropic.side_effect = Exception("API error")

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "test-key"}, clear=False):
            with patch.dict("sys.modules", {"anthropic": mock_mod}):
                result = _parse_alt_with_llm("some text", "AAPL")

        assert result is None


# ─────────────────────────────────────────────────────────────────────────────
# LLM fallback integration in parse_strategy_table
# ─────────────────────────────────────────────────────────────────────────────

class TestLLMFallbackIntegration:
    """Test that parse_strategy_table invokes LLM fallback for unrecognized patterns."""

    def test_llm_fallback_called_for_ambiguous(self, tmp_path):
        d = date(2026, 5, 20)
        content = textwrap.dedent("""\
            ## Summary Strategy Table

            | # | Symbol | ~Value | Signal | Strategy | Alt (PCS or CCS) |
            |---|--------|--------|--------|----------|-------------------|
            | 1 | **META** | $80K | Beat | CC | sell 290P credit spread |
        """)
        fname = f"daily-stocks-briefing-{d.isoformat()}.md"
        (tmp_path / fname).write_text(content)

        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            with patch("strategy._parse_alt_with_llm", return_value={
                "spread_type": "PCS",
                "action": "sell puts below",
                "strike": 290.0,
            }) as mock_llm:
                recs = parse_strategy_table(target_date=d, use_llm_fallback=True)

        mock_llm.assert_called_once_with("sell 290P credit spread", "META")
        assert len(recs) == 1
        assert recs[0]["symbol"] == "META"
        assert recs[0]["spread_type"] == "PCS"

    def test_llm_fallback_disabled(self, tmp_path):
        d = date(2026, 5, 20)
        content = textwrap.dedent("""\
            ## Summary Strategy Table

            | # | Symbol | ~Value | Signal | Strategy | Alt (PCS or CCS) |
            |---|--------|--------|--------|----------|-------------------|
            | 1 | **META** | $80K | Beat | CC | sell 290P credit spread |
        """)
        fname = f"daily-stocks-briefing-{d.isoformat()}.md"
        (tmp_path / fname).write_text(content)

        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            with patch("strategy._parse_alt_with_llm") as mock_llm:
                recs = parse_strategy_table(target_date=d, use_llm_fallback=False)

        mock_llm.assert_not_called()
        assert recs == []


# ─────────────────────────────────────────────────────────────────────────────
# scan_strategy_recommendations — scanner integration
# ─────────────────────────────────────────────────────────────────────────────

class TestScanStrategyRecommendations:
    """Test that scan_strategy_recommendations calls the right scanner per spread_type."""

    _ACTIONS = {
        "CCS": "sell calls above",
        "PCS": "sell puts below",
        "PDS": "buy puts below",
        "CDS": "buy calls above",
    }

    def _parsed_hint(self, symbol="NVDA", spread_type="CCS", strike=260.0):
        return {
            "symbol": symbol,
            "spread_type": spread_type,
            "action": self._ACTIONS.get(spread_type, "unknown"),
            "strike": strike,
            "raw_text": f"{spread_type} — test hint",
        }

    def test_ccs_hint_calls_scan_ccs(self):
        mock_rec = _make_scanner_rec("NVDA", "CCS")
        with patch("spread_scanner.scan_ccs", return_value=(mock_rec, 50)) as m_ccs, \
             patch("spread_scanner.scan_pcs") as m_pcs:
            results = scan_strategy_recommendations([self._parsed_hint("NVDA", "CCS")])
        m_ccs.assert_called_once()
        m_pcs.assert_not_called()
        found = [r for r in results if not r.get("no_contract")]
        assert len(found) == 1
        assert found[0]["symbol"] == "NVDA"
        assert found[0]["type"] == "CCS"
        assert found[0]["strategy_hint"] == "CCS — test hint"

    def test_pcs_hint_calls_scan_pcs(self):
        mock_rec = _make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)
        with patch("spread_scanner.scan_ccs") as m_ccs, \
             patch("spread_scanner.scan_pcs", return_value=(mock_rec, 30)) as m_pcs:
            results = scan_strategy_recommendations([self._parsed_hint("AAPL", "PCS")])
        m_pcs.assert_called_once()
        m_ccs.assert_not_called()
        found = [r for r in results if not r.get("no_contract")]
        assert len(found) == 1
        assert found[0]["symbol"] == "AAPL"
        assert found[0]["type"] == "PCS"

    def test_no_qualifying_contract_returns_stub(self):
        """Scanner returns None — result includes a no_contract stub."""
        with patch("spread_scanner.scan_ccs", return_value=(None, 100)):
            results = scan_strategy_recommendations([self._parsed_hint("XYZ", "CCS")])
        assert len(results) == 1
        assert results[0]["no_contract"] is True
        assert results[0]["symbol"] == "XYZ"
        assert results[0]["type"] == "CCS"

    def test_multiple_hints(self):
        """Multiple hints scan independently."""
        ccs_rec = _make_scanner_rec("NVDA", "CCS")
        pcs_rec = _make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)
        with patch("spread_scanner.scan_ccs", return_value=(ccs_rec, 50)), \
             patch("spread_scanner.scan_pcs", return_value=(pcs_rec, 30)):
            results = scan_strategy_recommendations([
                self._parsed_hint("NVDA", "CCS"),
                self._parsed_hint("AAPL", "PCS"),
            ])
        found = [r for r in results if not r.get("no_contract")]
        assert len(found) == 2
        assert {r["symbol"] for r in found} == {"NVDA", "AAPL"}

    def test_config_params_forwarded(self):
        """Config spread parameters are forwarded to scanner."""
        config = {
            "spread_dte_min": "21",
            "spread_dte_max": "56",
            "spread_short_otm_pct": "8.0",
            "spread_min_open_interest": "5",
            "spread_size_min_pct": "2.0",
            "spread_size_max_pct": "12.0",
            "spread_min_premium_pct": "1.5",
        }
        with patch("spread_scanner.scan_ccs", return_value=(None, 0)) as m_ccs:
            scan_strategy_recommendations([self._parsed_hint("NVDA", "CCS")], config)
        call_kwargs = m_ccs.call_args[1]
        assert call_kwargs["dte_min"] == 21
        assert call_kwargs["dte_max"] == 56
        assert call_kwargs["short_otm_pct"] == 8.0
        assert call_kwargs["min_open_interest"] == 5
        assert call_kwargs["spread_size_min_pct"] == 2.0
        assert call_kwargs["spread_size_max_pct"] == 12.0
        assert call_kwargs["min_premium_pct"] == 1.5

    def test_ccs_hint_passes_strike_min(self):
        """CCS 'above $X' hint passes short_strike_min_hint to scanner."""
        hint = self._parsed_hint("NVDA", "CCS")
        hint["action"] = "sell calls above"
        hint["strike"] = 280.0
        with patch("spread_scanner.scan_ccs", return_value=(None, 0)) as m_ccs:
            scan_strategy_recommendations([hint])
        call_kwargs = m_ccs.call_args[1]
        assert call_kwargs["short_strike_min_hint"] == 280.0

    def test_pcs_hint_passes_strike_max(self):
        """PCS 'below $X' hint passes short_strike_max_hint to scanner."""
        hint = self._parsed_hint("AMD", "PCS")
        hint["action"] = "sell puts below"
        hint["strike"] = 400.0
        with patch("spread_scanner.scan_pcs", return_value=(None, 0)) as m_pcs:
            scan_strategy_recommendations([hint])
        call_kwargs = m_pcs.call_args[1]
        assert call_kwargs["short_strike_max_hint"] == 400.0

    def test_pds_hint_calls_scan_pds(self):
        mock_rec = _make_debit_rec("TSLA", "PDS", 400.0, 380.0, 350.0, 3.50)
        with patch("spread_scanner.scan_pds", return_value=(mock_rec, 40)) as m_pds, \
             patch("spread_scanner.scan_cds") as m_cds, \
             patch("spread_scanner.scan_ccs") as m_ccs, \
             patch("spread_scanner.scan_pcs") as m_pcs:
            results = scan_strategy_recommendations([self._parsed_hint("TSLA", "PDS", 380.0)])
        m_pds.assert_called_once()
        m_cds.assert_not_called()
        m_ccs.assert_not_called()
        m_pcs.assert_not_called()
        found = [r for r in results if not r.get("no_contract")]
        assert len(found) == 1
        assert found[0]["symbol"] == "TSLA"
        assert found[0]["type"] == "PDS"

    def test_cds_hint_calls_scan_cds(self):
        mock_rec = _make_debit_rec("MSFT", "CDS", 420.0, 420.0, 450.0, 2.80)
        with patch("spread_scanner.scan_cds", return_value=(mock_rec, 25)) as m_cds, \
             patch("spread_scanner.scan_pds") as m_pds, \
             patch("spread_scanner.scan_ccs") as m_ccs, \
             patch("spread_scanner.scan_pcs") as m_pcs:
            results = scan_strategy_recommendations([self._parsed_hint("MSFT", "CDS", 420.0)])
        m_cds.assert_called_once()
        m_pds.assert_not_called()
        found = [r for r in results if not r.get("no_contract")]
        assert len(found) == 1
        assert found[0]["type"] == "CDS"

    def test_pds_hint_passes_long_strike_max(self):
        """PDS ceiling hint passes long_strike_max_hint to scan_pds."""
        with patch("spread_scanner.scan_pds", return_value=(None, 0)) as m_pds:
            scan_strategy_recommendations([self._parsed_hint("TSLA", "PDS", 380.0)])
        call_kwargs = m_pds.call_args[1]
        assert call_kwargs["long_strike_max_hint"] == 380.0

    def test_cds_hint_passes_long_strike_min(self):
        """CDS floor hint passes long_strike_min_hint to scan_cds."""
        with patch("spread_scanner.scan_cds", return_value=(None, 0)) as m_cds:
            scan_strategy_recommendations([self._parsed_hint("MSFT", "CDS", 420.0)])
        call_kwargs = m_cds.call_args[1]
        assert call_kwargs["long_strike_min_hint"] == 420.0

    def test_debit_config_params_forwarded(self):
        """Config debit parameters are forwarded to PDS scanner."""
        config = {
            "debit_dte_min": "35",
            "debit_dte_max": "70",
            "debit_min_open_interest": "3",
            "debit_spread_size_min_pct": "6.0",
            "debit_spread_size_max_pct": "20.0",
            "debit_max_debit_pct": "30.0",
            "debit_long_leg_offset_pct": "4.0",
            "debit_max_dpd_pct": "8.0",
        }
        with patch("spread_scanner.scan_pds", return_value=(None, 0)) as m_pds:
            scan_strategy_recommendations([self._parsed_hint("TSLA", "PDS", 380.0)], config)
        kw = m_pds.call_args[1]
        assert kw["dte_min"] == 35
        assert kw["dte_max"] == 70
        assert kw["min_open_interest"] == 3
        assert kw["spread_size_min_pct"] == 6.0
        assert kw["spread_size_max_pct"] == 20.0
        assert kw["max_debit_pct"] == 0.30
        assert kw["long_leg_offset"] == 0.04
        assert kw["max_dpd_pct"] == 0.08

    def test_mixed_credit_and_debit_hints(self):
        """Mix of PCS, CCS, PDS, CDS hints all scan correctly."""
        ccs_rec = _make_scanner_rec("NVDA", "CCS")
        pds_rec = _make_debit_rec("TSLA", "PDS")
        with patch("spread_scanner.scan_ccs", return_value=(ccs_rec, 50)), \
             patch("spread_scanner.scan_pcs", return_value=(None, 10)), \
             patch("spread_scanner.scan_pds", return_value=(pds_rec, 40)), \
             patch("spread_scanner.scan_cds", return_value=(None, 5)):
            results = scan_strategy_recommendations([
                self._parsed_hint("NVDA", "CCS"),
                self._parsed_hint("AAPL", "PCS"),
                self._parsed_hint("TSLA", "PDS", 380.0),
                self._parsed_hint("MSFT", "CDS", 420.0),
            ])
        found = [r for r in results if not r.get("no_contract")]
        stubs = [r for r in results if r.get("no_contract")]
        assert len(found) == 2
        assert len(stubs) == 2
        assert {r["type"] for r in found} == {"CCS", "PDS"}

    def test_unknown_spread_type_skipped(self):
        """Unknown spread_type is logged and skipped."""
        hint = self._parsed_hint("SPY", "CCS")
        hint["spread_type"] = "IRON_CONDOR"
        with patch("spread_scanner.scan_ccs") as m_ccs, \
             patch("spread_scanner.scan_pcs") as m_pcs:
            results = scan_strategy_recommendations([hint])
        m_ccs.assert_not_called()
        m_pcs.assert_not_called()
        assert results == []

    def test_strategy_hint_preserved(self):
        """The original raw_text from the parsed hint is added to the scanner result."""
        mock_rec = _make_scanner_rec("TSLA", "PCS")
        # Remove strategy_hint from the mock so scan_strategy_recommendations adds it
        del mock_rec["strategy_hint"]
        hint = self._parsed_hint("TSLA", "PCS")
        hint["raw_text"] = "PCS — sell puts below $220"
        with patch("spread_scanner.scan_pcs", return_value=(mock_rec, 40)):
            results = scan_strategy_recommendations([hint])
        found = [r for r in results if not r.get("no_contract")]
        assert found[0]["strategy_hint"] == "PCS — sell puts below $220"


# ─────────────────────────────────────────────────────────────────────────────
# CLI --strategy flag
# ─────────────────────────────────────────────────────────────────────────────

class TestCLIStrategy:
    """Test the cmd_strategy CLI function dispatches correctly."""

    def test_cmd_strategy_shows_credit_spread(self, capsys):
        """--strategy with CSV source parses hints, scans contracts, and displays PCS details."""
        parsed = [
            {"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0},
        ]
        scanned = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20,
                                     "PCS — sell puts below $170")]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_purchase_csv", return_value=parsed), \
             patch("strategy.scan_strategy_recommendations", return_value=scanned), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "AAPL" in output
        assert "PCS" in output
        assert "Net credit" in output
        assert "Source: CSV" in output

    def test_cmd_strategy_shows_debit_spread(self, capsys):
        """--strategy displays PDS debit spread details."""
        parsed = [
            {"symbol": "TSLA", "spread_type": "PDS", "action": "buy puts below", "strike": 380.0},
        ]
        scanned = [_make_debit_rec("TSLA", "PDS", 400.0, 380.0, 350.0, 3.50)]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_purchase_csv", return_value=parsed), \
             patch("strategy.scan_strategy_recommendations", return_value=scanned), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "TSLA" in output
        assert "PDS" in output
        assert "Net debit" in output
        assert "DPD" in output

    def test_cmd_strategy_falls_back_to_markdown(self, capsys):
        """When CSV is empty, falls back to markdown and shows source."""
        parsed = [
            {"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0},
        ]
        scanned = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_purchase_csv", return_value=[]), \
             patch("strategy.parse_strategy_table", return_value=parsed), \
             patch("strategy.scan_strategy_recommendations", return_value=scanned), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "markdown (fallback)" in output

    def test_cmd_strategy_no_recs(self, capsys):
        """When no strategy hints found, shows helpful message."""
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_purchase_csv", return_value=[]), \
             patch("strategy.parse_strategy_table", return_value=[]), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol="XYZ")
        output = capsys.readouterr().out
        assert "No strategy found for XYZ" in output

    def test_cmd_strategy_no_contracts(self, capsys):
        """When hints exist but scanner finds no contracts, shows hint with no-match."""
        parsed = [
            {"symbol": "XYZ", "spread_type": "CCS", "action": "sell calls above", "strike": 100.0},
        ]
        no_match = [{"symbol": "XYZ", "type": "CCS", "strategy_hint": "CCS — test",
                      "no_contract": True, "scenarios": 50}]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_purchase_csv", return_value=parsed), \
             patch("strategy.scan_strategy_recommendations", return_value=no_match), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "no qualifying contracts found" in output
        assert "XYZ" in output


# ─────────────────────────────────────────────────────────────────────────────
# Email template — strategy_recs rendering
# ─────────────────────────────────────────────────────────────────────────────

class TestEmailStrategyRecs:
    """Test that strategy_recs (full scanner dicts) render correctly in the Jinja2 email template."""

    MOCK_META = {
        "run_date": "2026-05-20",
        "duration_sec": 5,
        "pur_pct": 42.0,
        "pur_open": 3,
        "pur_max": 10,
        "portfolio_ypd": 1.25,
    }

    def _render(self, strategy_recs, strategy_source=None):
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("email.html")
        return template.render(
            recommendations=[],
            meta=self.MOCK_META,
            roll_candidates=[],
            btc_candidates=[],
            optimize_results=[],
            panic_results=[],
            rescue_results=[],
            safety_results=[],
            spread_optimize_results=[],
            spread_safety_results=[],
            spread_rescue_results=[],
            spread_panic_results=[],
            strategy_recs=strategy_recs,
            strategy_source=strategy_source,
        )

    def test_strategy_recs_rendered_with_contract_details(self):
        recs = [
            _make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20,
                              "PCS — sell puts below $170"),
            _make_scanner_rec("NVDA", "CCS", 260.0, 290.0, 300.0, 1.50,
                              "CCS — sell calls above $260"),
        ]
        html = self._render(recs)
        assert "Strategy Recommendations" in html
        assert "AAPL" in html
        assert "NVDA" in html
        # Contract details rendered
        assert "Short Put" in html
        assert "Short Call" in html
        assert "Long Put" in html
        assert "Long Call" in html
        assert "$170.00" in html  # short put strike
        assert "$290.00" in html  # short call strike
        # Net credit
        assert "Net credit" not in html or "$120.00" in html  # net_credit_total for AAPL
        # YPD
        assert "YPD" in html
        # Strategy hint preserved
        assert "sell puts below" in html
        assert "sell calls above" in html

    def test_pcs_green_background(self):
        recs = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)]
        html = self._render(recs)
        assert "#f0fdf4" in html  # green background for PCS rows
        assert "#14532d" in html  # green header for PCS

    def test_ccs_blue_background(self):
        recs = [_make_scanner_rec("NVDA", "CCS", 260.0, 290.0, 300.0, 1.50)]
        html = self._render(recs)
        assert "#eff6ff" in html  # blue background for CCS rows
        assert "#1e1b4b" in html  # indigo header for CCS

    def test_max_loss_and_spread_shown(self):
        recs = [_make_scanner_rec("TSLA", "PCS", 250.0, 220.0, 210.0, 2.00)]
        html = self._render(recs)
        assert "Max loss" in html
        assert "Spread" in html
        assert "C/L" in html

    def test_no_strategy_section_when_empty(self):
        html = self._render([])
        assert "Strategy Recommendations" not in html

    def test_no_contract_stub_renders_warning(self):
        """no_contract stubs render a yellow warning banner with hint text."""
        recs = [{
            "symbol": "XYZ",
            "type": "CCS",
            "strategy_hint": "CCS — sell calls above $100",
            "no_contract": True,
            "scenarios": 50,
        }]
        html = self._render(recs)
        assert "Strategy Recommendations" in html
        assert "XYZ" in html
        # Yellow warning banner background
        assert "#fef3c7" in html
        # "no qualifying contracts found" message
        assert "no qualifying contracts found" in html
        # Hint text shown
        assert "sell calls above" in html
        # Should NOT have contract detail fields
        assert "Short Call" not in html
        assert "Long Call" not in html

    def test_mixed_contracts_and_stubs(self):
        """Mix of full contracts and no_contract stubs render both styles."""
        recs = [
            _make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20,
                              "PCS — sell puts below $170"),
            {
                "symbol": "XYZ",
                "type": "CCS",
                "strategy_hint": "CCS — sell calls above $100",
                "no_contract": True,
                "scenarios": 50,
            },
        ]
        html = self._render(recs)
        # Full contract for AAPL
        assert "AAPL" in html
        assert "Short Put" in html
        assert "$170.00" in html
        # Warning banner for XYZ
        assert "XYZ" in html
        assert "no qualifying contracts found" in html
        assert "sell calls above" in html

    def test_no_strategy_section_when_none(self):
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("email.html")
        html = template.render(
            recommendations=[],
            meta=self.MOCK_META,
            roll_candidates=[],
            btc_candidates=[],
            optimize_results=[],
            panic_results=[],
            rescue_results=[],
            safety_results=[],
            spread_optimize_results=[],
            spread_safety_results=[],
            spread_rescue_results=[],
            spread_panic_results=[],
        )
        assert "Strategy Recommendations" not in html

    def test_pds_debit_details_rendered(self):
        """PDS strategy recs render debit spread details."""
        recs = [_make_debit_rec("TSLA", "PDS", 400.0, 380.0, 350.0, 3.50, "PDS ceiling $380")]
        html = self._render(recs)
        assert "Strategy Recommendations" in html
        assert "TSLA" in html
        assert "Long Put" in html
        assert "Short Put" in html
        assert "$380.00" in html
        assert "$350.00" in html
        assert "Net Debit" in html
        assert "DPD" in html
        assert "Debit/Width" in html
        assert "#059669" in html  # PDS green header

    def test_cds_debit_details_rendered(self):
        """CDS strategy recs render debit spread details."""
        recs = [_make_debit_rec("MSFT", "CDS", 420.0, 420.0, 450.0, 2.80, "CDS floor $420")]
        html = self._render(recs)
        assert "MSFT" in html
        assert "Long Call" in html
        assert "Short Call" in html
        assert "#7c3aed" in html  # CDS purple header

    def test_pds_no_contract_stub_renders(self):
        """PDS no-contract stub shows correct badge color."""
        recs = [{"symbol": "TSLA", "type": "PDS",
                 "strategy_hint": "PDS ceiling $380", "no_contract": True, "scenarios": 20}]
        html = self._render(recs)
        assert "TSLA" in html
        assert "#059669" in html  # PDS badge color
        assert "no qualifying contracts found" in html

    def test_markdown_fallback_indicator_shown(self):
        """When strategy_source is 'markdown', the fallback warning badge is shown."""
        recs = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)]
        html = self._render(recs, strategy_source="markdown")
        assert "Fallback: markdown briefing" in html

    def test_csv_source_no_fallback_indicator(self):
        """When strategy_source is 'csv', no fallback warning is shown."""
        recs = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20)]
        html = self._render(recs, strategy_source="csv")
        assert "Fallback" not in html

    def test_mixed_credit_and_debit_recs(self):
        """Mix of PCS and PDS recs renders both credit and debit tables."""
        recs = [
            _make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20, "PCS ceiling $170"),
            _make_debit_rec("TSLA", "PDS", 400.0, 380.0, 350.0, 3.50, "PDS ceiling $380"),
        ]
        html = self._render(recs)
        assert "AAPL" in html
        assert "TSLA" in html
        assert "Net Credit" in html
        assert "Net Debit" in html
        assert "Short Put" in html
        assert "Long Put" in html
