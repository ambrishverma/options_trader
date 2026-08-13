"""Tests for the trade report section in the daily pipeline email."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from emailer import _render_html


def _make_rec(symbol="AAPL"):
    yo = {"symbol": symbol, "name": "Corp", "strike": 200.0,
          "expiration": "2026-04-01", "dte": 5, "mid": 1.0,
          "bid": 0.95, "ask": 1.05,
          "otm_pct": 8.0, "annualized_yield": 40.0, "open_interest": 50, "contracts": 2}
    so = {"symbol": symbol, "name": "Corp", "strike": 220.0,
          "expiration": "2026-04-01", "dte": 10, "mid": 0.6,
          "bid": 0.55, "ask": 0.65,
          "otm_pct": 16.0, "annualized_yield": 20.0, "open_interest": 30, "contracts": 2}
    return {
        "symbol": symbol, "name": "Corp", "rank": 1,
        "contracts_total": 2,
        "combined_premium_total": (1.0 + 0.6) * 100,
        "combined_ann_yield": 30.0,
        "combined_ypd": 26.0,
        "earnings_flag": None, "earnings_warning": None,
        "yield_leg":  {"option": yo, "contracts": 1, "rationale": "r", "ypd": 20.0},
        "safety_leg": {"option": so, "contracts": 1, "rationale": "r", "ypd": 6.0},
    }


META = {
    "run_date": "2026-08-13",
    "duration_sec": 45.0,
    "recipient_email": "test@test.com",
    "pur_pct": 50.0, "pur_open": 7, "pur_max": 14,
    "portfolio_ypd": 26.0,
}


SAMPLE_REPORT = {
    "start_date": "2026-08-13",
    "end_date": "2026-08-13",
    "orders": [
        {
            "date": "2026-08-13",
            "symbol": "NVDA",
            "type": "call",
            "side": "sell",
            "strike": 140.0,
            "expiration": "2026-08-22",
            "quantity": 2,
            "price": 3.15,
            "premium": 630.0,
            "direction": "credit",
        },
        {
            "date": "2026-08-13",
            "symbol": "AMD",
            "type": "put",
            "side": "buy",
            "strike": 155.0,
            "expiration": "2026-08-22",
            "quantity": 1,
            "price": 1.20,
            "premium": 120.0,
            "direction": "debit",
        },
    ],
    "total_credit": 630.0,
    "total_debit": 120.0,
    "net_gain": 510.0,
    "order_count": 2,
}


class TestTradeReportSection:

    def test_section_renders_with_orders(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert "Today&#39;s Transactions" in html or "Today's Transactions" in html
        assert "NVDA" in html
        assert "AMD" in html
        assert "630.00" in html
        assert "120.00" in html
        assert "510.00" in html

    def test_section_shows_order_count(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert ">2<" in html or ">2\n" in html

    def test_section_renders_credit_direction(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert "▲" in html

    def test_section_renders_debit_direction(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert "▼" in html

    def test_section_renders_sell_side(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert "SELL" in html

    def test_section_renders_buy_side(self):
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        assert "BUY" in html

    def test_section_hidden_when_no_report(self):
        html = _render_html([_make_rec()], META, trade_report=None)
        assert "Today&#39;s Transactions" not in html
        assert "Today's Transactions" not in html

    def test_section_hidden_when_empty_dict(self):
        html = _render_html([_make_rec()], META, trade_report={})
        assert "Today&#39;s Transactions" not in html
        assert "Today's Transactions" not in html

    def test_section_renders_empty_orders(self):
        """Report with zero orders should show the section with an empty-state message."""
        empty_report = {
            "start_date": "2026-08-13",
            "end_date": "2026-08-13",
            "orders": [],
            "total_credit": 0.0,
            "total_debit": 0.0,
            "net_gain": 0.0,
            "order_count": 0,
        }
        html = _render_html([_make_rec()], META, trade_report=empty_report)
        assert "Today&#39;s Transactions" in html or "Today's Transactions" in html
        assert "No filled options orders today" in html

    def test_net_loss_renders_correctly(self):
        """When net_gain is negative, should show 'Net Loss' with the absolute value."""
        loss_report = {
            "start_date": "2026-08-13",
            "end_date": "2026-08-13",
            "orders": [
                {
                    "date": "2026-08-13",
                    "symbol": "TSLA",
                    "type": "put",
                    "side": "buy",
                    "strike": 250.0,
                    "expiration": "2026-08-22",
                    "quantity": 1,
                    "price": 5.00,
                    "premium": 500.0,
                    "direction": "debit",
                },
            ],
            "total_credit": 0.0,
            "total_debit": 500.0,
            "net_gain": -500.0,
            "order_count": 1,
        }
        html = _render_html([_make_rec()], META, trade_report=loss_report)
        assert "Net Loss" in html
        assert "500.00" in html

    def test_section_appears_before_footer(self):
        """The transaction report section should appear before the disclaimer footer."""
        html = _render_html([_make_rec()], META, trade_report=SAMPLE_REPORT)
        txn_pos = html.find("Today")
        footer_pos = html.find("Disclaimer")
        assert txn_pos < footer_pos, "Transaction report should appear before the footer"
