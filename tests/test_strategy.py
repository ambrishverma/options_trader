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
    _TABLE_ROW_RE,
    _ALT_RE,
    BRIEFINGS_DIR,
)


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
    """Test the markdown table row regex extracts symbol and alt text."""

    def test_standard_row(self):
        line = "| 3 | **NVDA** | $198K | Beat, dip | Put Credit Spread (PCS) | CCS — sell calls above $260 |"
        m = _TABLE_ROW_RE.match(line)
        assert m is not None
        assert m.group(1) == "NVDA"
        assert "CCS" in m.group(2)

    def test_row_without_bold(self):
        line = "| 1 | AAPL | $95K | Earnings | PCS | PCS — sell puts below $170 |"
        m = _TABLE_ROW_RE.match(line)
        assert m is not None
        assert m.group(1) == "AAPL"

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

    def test_skips_non_pcs_ccs_rows(self, tmp_path):
        d = date(2026, 5, 20)
        self._write_briefing(tmp_path, d, MOCK_BRIEFING)
        with patch("strategy.BRIEFINGS_DIR", tmp_path):
            recs = parse_strategy_table(target_date=d, use_llm_fallback=False)
        # TSLA "Hold / no alt" should not appear
        assert all(r["symbol"] != "TSLA" for r in recs)


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
# CLI --strategy flag
# ─────────────────────────────────────────────────────────────────────────────

class TestCLIStrategy:
    """Test the cmd_strategy CLI function dispatches correctly."""

    def test_cmd_strategy_all_symbols(self, capsys):
        """--strategy with no symbol shows all recs."""
        recs = [
            {"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0},
        ]
        with patch("strategy.parse_strategy_table", return_value=recs):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "AAPL" in output
        assert "PCS" in output
        assert "170" in output

    def test_cmd_strategy_filtered(self, capsys):
        """--strategy NVDA shows only NVDA recs."""
        recs = [
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above", "strike": 260.0},
        ]
        with patch("strategy.parse_strategy_table", return_value=recs):
            from main import cmd_strategy
            cmd_strategy(symbol="NVDA")
        output = capsys.readouterr().out
        assert "NVDA" in output
        assert "CCS" in output

    def test_cmd_strategy_no_recs(self, capsys):
        """When no strategy recs found, shows helpful message."""
        with patch("strategy.parse_strategy_table", return_value=[]):
            from main import cmd_strategy
            cmd_strategy(symbol="XYZ")
        output = capsys.readouterr().out
        assert "No PCS/CCS strategy found for XYZ" in output

    def test_cmd_strategy_no_recs_all(self, capsys):
        """When no strategy recs found for all, shows message."""
        with patch("strategy.parse_strategy_table", return_value=[]):
            from main import cmd_strategy
            cmd_strategy(symbol=None)
        output = capsys.readouterr().out
        assert "No PCS/CCS strategies found" in output


# ─────────────────────────────────────────────────────────────────────────────
# Email template — strategy_recs rendering
# ─────────────────────────────────────────────────────────────────────────────

class TestEmailStrategyRecs:
    """Test that strategy_recs renders correctly in the Jinja2 email template."""

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
            spread_optimize_results=[],
            spread_rescue_results=[],
            spread_panic_results=[],
            strategy_recs=strategy_recs,
        )

    def test_strategy_recs_rendered(self):
        recs = [
            {"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0},
            {"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above", "strike": 260.0},
        ]
        html = self._render(recs)
        assert "Strategy Recommendations" in html
        assert "AAPL" in html
        assert "NVDA" in html
        assert "PCS" in html
        assert "CCS" in html
        assert "$170" in html
        assert "$260" in html
        assert "sell puts below" in html
        assert "sell calls above" in html

    def test_pcs_green_background(self):
        recs = [{"symbol": "AAPL", "spread_type": "PCS", "action": "sell puts below", "strike": 170.0}]
        html = self._render(recs)
        assert "#f0fdf4" in html  # green background for PCS rows

    def test_ccs_blue_background(self):
        recs = [{"symbol": "NVDA", "spread_type": "CCS", "action": "sell calls above", "strike": 260.0}]
        html = self._render(recs)
        assert "#eff6ff" in html  # blue background for CCS rows

    def test_no_strategy_section_when_empty(self):
        html = self._render([])
        assert "Strategy Recommendations" not in html

    def test_no_strategy_section_when_none(self):
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        templates_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
        env = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template("email.html")
        # Pass no strategy_recs at all (not in context)
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
            spread_rescue_results=[],
            spread_panic_results=[],
        )
        assert "Strategy Recommendations" not in html
