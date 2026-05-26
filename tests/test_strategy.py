"""
test_strategy.py — Tests for strategy.py
=========================================
Tests cover:
  - _find_briefing_file()      file discovery
  - _parse_alt_recommendation() regex-based parsing
  - _parse_alt_with_llm()       Claude API fallback
  - parse_strategy_table()      full table parse + symbol filter
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
    _parse_alt_recommendation,
    _parse_alt_with_llm,
    parse_strategy_table,
    scan_strategy_recommendations,
    _TABLE_ROW_RE,
    _ALT_RE,
    BRIEFINGS_DIR,
)


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

    def _parsed_hint(self, symbol="NVDA", spread_type="CCS"):
        return {
            "symbol": symbol,
            "spread_type": spread_type,
            "action": "sell calls above" if spread_type == "CCS" else "sell puts below",
            "strike": 260.0,
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

    def test_cmd_strategy_shows_contracts(self, capsys):
        """--strategy parses hints, scans contracts, and displays details."""
        parsed = [
            {"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0},
        ]
        scanned = [_make_scanner_rec("AAPL", "PCS", 195.0, 170.0, 160.0, 1.20,
                                     "PCS — sell puts below $170")]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_strategy_table", return_value=parsed), \
             patch("strategy.scan_strategy_recommendations", return_value=scanned), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "AAPL" in output
        assert "PCS" in output
        assert "Net credit" in output
        assert "YPD" in output

    def test_cmd_strategy_no_recs(self, capsys):
        """When no strategy hints found, shows helpful message."""
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_strategy_table", return_value=[]), \
             patch("utils.load_config", return_value={}):
            from main import cmd_strategy
            cmd_strategy(symbol="XYZ")
        output = capsys.readouterr().out
        assert "No PCS/CCS strategy found for XYZ" in output

    def test_cmd_strategy_no_contracts(self, capsys):
        """When hints exist but scanner finds no contracts, shows hint with no-match."""
        parsed = [
            {"symbol": "XYZ", "spread_type": "CCS", "action": "sell calls above", "strike": 100.0},
        ]
        no_match = [{"symbol": "XYZ", "type": "CCS", "strategy_hint": "CCS — test",
                      "no_contract": True, "scenarios": 50}]
        with patch("main.check_env"), \
             patch("main.setup_logging", create=True), \
             patch("strategy.parse_strategy_table", return_value=parsed), \
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

    def _render(self, strategy_recs):
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
            spread_safety_results=[],
            spread_rescue_results=[],
            spread_panic_results=[],
            strategy_recs=strategy_recs,
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
            spread_safety_results=[],
            spread_rescue_results=[],
            spread_panic_results=[],
        )
        assert "Strategy Recommendations" not in html
