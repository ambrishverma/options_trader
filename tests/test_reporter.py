"""
test_reporter.py — Tests for reporter.py

Covers:
  - _parse_date_range(): today default, single-date, date-range, bad inputs
  - _execution_date_local(): UTC timestamp → local date, fallback to created_at
  - _get_order_date(): execution preference over updated_at
  - _process_orders(): filters state/date, computes credit/debit/net
  - Premium calculation: from "premium" field vs. price*qty*100
  - Edge cases: no matching orders, all-debit, mixed credit+debit
"""

import sys
import os
import pytest
from datetime import date, datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from reporter import (
    _parse_date_range,
    _execution_date_local,
    _get_order_date,
    _extract_leg_info,
)


# ─────────────────────────────────────────────────────────────────────────────
# _parse_date_range
# ─────────────────────────────────────────────────────────────────────────────

class TestParseDateRange:

    def test_none_returns_today(self):
        start, end = _parse_date_range(None)
        today = date.today()
        assert start == today
        assert end == today

    def test_single_date_mm_dd(self):
        start, end = _parse_date_range("04/09")
        today = date.today()
        assert start == date(today.year, 4, 9)
        assert end == start

    def test_range_mm_dd_mm_dd(self):
        start, end = _parse_date_range("04/01-04/09")
        today = date.today()
        assert start == date(today.year, 4, 1)
        assert end   == date(today.year, 4, 9)

    def test_range_with_spaces_around_dash(self):
        start, end = _parse_date_range("04/01 - 04/09")
        today = date.today()
        assert start == date(today.year, 4, 1)
        assert end   == date(today.year, 4, 9)

    def test_single_digit_month_and_day(self):
        start, end = _parse_date_range("1/5")
        today = date.today()
        assert start == date(today.year, 1, 5)

    def test_invalid_format_raises_valueerror(self):
        with pytest.raises(ValueError):
            _parse_date_range("not-a-date")

    def test_start_after_end_raises_valueerror(self):
        with pytest.raises(ValueError, match="after end"):
            _parse_date_range("04/09-04/01")


# ─────────────────────────────────────────────────────────────────────────────
# _execution_date_local
# ─────────────────────────────────────────────────────────────────────────────

class TestExecutionDateLocal:

    def _make_order(self, timestamp=None, created_at=None, updated_at=None):
        """Build a minimal fake order dict."""
        leg = {}
        if timestamp:
            leg = {"executions": [{"timestamp": timestamp}]}
        return {
            "legs":       [leg] if leg else [{}],
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def test_utc_timestamp_converts_to_local_date(self):
        # 2026-04-09T18:00:00Z → 11:00 AM PT (same day)
        order = self._make_order(timestamp="2026-04-09T18:00:00Z")
        d = _execution_date_local(order)
        assert d == date(2026, 4, 9)

    def test_midnight_utc_maps_to_previous_local_date(self):
        # 2026-04-10T00:30:00Z → 5:30 PM PT on Apr 9 (UTC−7 in summer)
        order = self._make_order(timestamp="2026-04-10T00:30:00Z")
        d = _execution_date_local(order)
        # 00:30 UTC → 17:30 PT (Apr 9) — should be Apr 9, not Apr 10
        assert d == date(2026, 4, 9)

    def test_fallback_to_created_at_when_no_execution(self):
        order = self._make_order(created_at="2026-04-08T17:00:00Z")
        d = _execution_date_local(order)
        assert d == date(2026, 4, 8)

    def test_returns_none_when_no_timestamps(self):
        order = {"legs": [{}]}
        d = _execution_date_local(order)
        assert d is None

    def test_malformed_timestamp_is_skipped(self):
        order = {
            "legs": [{"executions": [{"timestamp": "not-a-timestamp"}]}],
            "created_at": "2026-04-07T12:00:00Z",
        }
        d = _execution_date_local(order)
        # Falls back to created_at
        assert d == date(2026, 4, 7)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: minimal fake Robinhood order
# ─────────────────────────────────────────────────────────────────────────────

def _make_rh_order(
    state="filled",
    direction="credit",
    chain_symbol="TSLA",
    price="1.50",
    quantity="1",
    premium="150.00",
    execution_ts="2026-04-09T18:00:00Z",
    strike="300.00",
    expiration="2026-05-16",
    option_type="call",
    side="sell",
    order_id="ord-001",
):
    order = {
        "id":           order_id,
        "state":        state,
        "direction":    direction,
        "chain_symbol": chain_symbol,
        "price":        price,
        "quantity":     quantity,
        "created_at":   execution_ts,
        "updated_at":   execution_ts,
        "legs": [
            {
                "side":            side,
                "option_type":     option_type,
                "strike_price":    strike,
                "expiration_date": expiration,
                "executions": [
                    {"timestamp": execution_ts}
                ],
            }
        ],
    }
    if premium is not None:
        order["premium"] = premium
    return order


# ─────────────────────────────────────────────────────────────────────────────
# Pure processing logic extracted from build_options_report (no auth needed)
# ─────────────────────────────────────────────────────────────────────────────

def _process_orders(raw_orders: list, date_arg=None) -> dict:
    """
    Re-implements the filtering/aggregation half of build_options_report()
    without any Robinhood auth, so tests can exercise it in isolation.
    """
    start_date, end_date = _parse_date_range(date_arg)

    matched = []
    for order in raw_orders:
        state = (order.get("state") or "").lower()
        if state != "filled":
            continue

        order_date = _get_order_date(order)
        if order_date is None:
            continue

        if not (start_date <= order_date <= end_date):
            continue

        leg_info  = _extract_leg_info(order)
        quantity  = int(float(order.get("quantity") or 0))
        price     = float(order.get("price") or 0)
        premium_raw = order.get("premium")
        if premium_raw is not None:
            premium = abs(float(premium_raw))
        else:
            premium = round(price * quantity * 100, 2)

        direction = (order.get("direction") or "").lower()

        matched.append({
            "date":       str(order_date),
            "symbol":     (order.get("chain_symbol") or "").upper(),
            "type":       leg_info.get("option_type", ""),
            "side":       leg_info.get("side", ""),
            "strike":     leg_info.get("strike", 0.0),
            "expiration": leg_info.get("expiration", ""),
            "quantity":   quantity,
            "price":      round(price, 2),
            "premium":    round(premium, 2),
            "direction":  direction,
            "order_id":   order.get("id", ""),
        })

    matched.sort(key=lambda o: (o["date"], o["symbol"]))
    total_credit = sum(o["premium"] for o in matched if o["direction"] == "credit")
    total_debit  = sum(o["premium"] for o in matched if o["direction"] == "debit")

    return {
        "start_date":   str(start_date),
        "end_date":     str(end_date),
        "orders":       matched,
        "total_credit": round(total_credit, 2),
        "total_debit":  round(total_debit, 2),
        "net_gain":     round(total_credit - total_debit, 2),
        "order_count":  len(matched),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Processing logic tests
# ─────────────────────────────────────────────────────────────────────────────

class TestOrderProcessing:

    # ── Filter: state ────────────────────────────────────────────────────────

    def test_only_filled_orders_included(self):
        orders = [
            _make_rh_order(state="filled",    direction="credit", premium="150.00"),
            _make_rh_order(state="cancelled", direction="credit", premium="200.00", order_id="ord-002"),
            _make_rh_order(state="pending",   direction="credit", premium="100.00", order_id="ord-003"),
        ]
        report = _process_orders(orders, date_arg="04/09")
        assert report["order_count"] == 1
        assert report["total_credit"] == 150.00

    # ── Filter: date window ──────────────────────────────────────────────────

    def test_orders_outside_date_range_excluded(self):
        in_range  = _make_rh_order(execution_ts="2026-04-09T18:00:00Z", premium="150.00", order_id="in")
        out_range = _make_rh_order(execution_ts="2026-04-08T18:00:00Z", premium="200.00", order_id="out")
        report = _process_orders([in_range, out_range], date_arg="04/09")
        assert report["order_count"] == 1
        assert report["orders"][0]["order_id"] == "in"

    def test_full_date_range_captures_multiple_days(self):
        day1 = _make_rh_order(execution_ts="2026-04-07T18:00:00Z", premium="100.00", order_id="d1")
        day2 = _make_rh_order(execution_ts="2026-04-08T18:00:00Z", premium="100.00", order_id="d2")
        day3 = _make_rh_order(execution_ts="2026-04-09T18:00:00Z", premium="100.00", order_id="d3")
        outside = _make_rh_order(execution_ts="2026-04-10T18:00:00Z", premium="100.00", order_id="d4")
        report = _process_orders([day1, day2, day3, outside], date_arg="04/07-04/09")
        assert report["order_count"] == 3

    # ── Credit / debit totals ────────────────────────────────────────────────

    def test_total_credit_sum(self):
        orders = [
            _make_rh_order(direction="credit", premium="150.00", order_id="c1"),
            _make_rh_order(direction="credit", premium="200.00", order_id="c2"),
        ]
        report = _process_orders(orders, date_arg="04/09")
        assert report["total_credit"] == 350.00
        assert report["total_debit"]  == 0.00
        assert report["net_gain"]     == 350.00

    def test_total_debit_sum(self):
        orders = [
            _make_rh_order(direction="debit", side="buy", premium="50.00", order_id="d1"),
            _make_rh_order(direction="debit", side="buy", premium="80.00", order_id="d2"),
        ]
        report = _process_orders(orders, date_arg="04/09")
        assert report["total_debit"]  == 130.00
        assert report["total_credit"] == 0.00
        assert report["net_gain"]     == -130.00

    def test_mixed_credit_and_debit_net_gain(self):
        orders = [
            _make_rh_order(direction="credit", premium="300.00", order_id="c1"),
            _make_rh_order(direction="debit",  premium="100.00", order_id="d1", side="buy"),
        ]
        report = _process_orders(orders, date_arg="04/09")
        assert report["total_credit"] == 300.00
        assert report["total_debit"]  == 100.00
        assert report["net_gain"]     == 200.00

    # ── Premium calculation ──────────────────────────────────────────────────

    def test_premium_taken_from_premium_field_when_present(self):
        order = _make_rh_order(price="1.50", quantity="1", premium="175.00")
        report = _process_orders([order], date_arg="04/09")
        assert report["orders"][0]["premium"] == 175.00

    def test_premium_computed_from_price_qty_when_no_premium_field(self):
        order = _make_rh_order(price="2.00", quantity="2", premium=None)
        report = _process_orders([order], date_arg="04/09")
        # 2.00 × 2 × 100 = 400
        assert report["orders"][0]["premium"] == 400.00

    # ── Field extraction ─────────────────────────────────────────────────────

    def test_order_fields_extracted_correctly(self):
        order = _make_rh_order(
            chain_symbol="NVDA",
            strike="900.00",
            expiration="2026-06-20",
            option_type="call",
            side="sell",
            direction="credit",
            price="3.50",
            quantity="2",
            premium="700.00",
            order_id="nvda-001",
        )
        report = _process_orders([order], date_arg="04/09")
        o = report["orders"][0]
        assert o["symbol"]     == "NVDA"
        assert o["type"]       == "CALL"
        assert o["side"]       == "sell"
        assert o["strike"]     == 900.00
        assert o["expiration"] == "2026-06-20"
        assert o["quantity"]   == 2
        assert o["price"]      == 3.50
        assert o["premium"]    == 700.00
        assert o["direction"]  == "credit"
        assert o["order_id"]   == "nvda-001"

    def test_put_option_type_extracted(self):
        order = _make_rh_order(option_type="put", side="buy", direction="debit")
        report = _process_orders([order], date_arg="04/09")
        assert report["orders"][0]["type"] == "PUT"
        assert report["orders"][0]["side"] == "buy"

    # ── Empty result ─────────────────────────────────────────────────────────

    def test_no_matching_orders_returns_zero_totals(self):
        report = _process_orders([], date_arg="04/09")
        assert report["order_count"]  == 0
        assert report["total_credit"] == 0.00
        assert report["total_debit"]  == 0.00
        assert report["net_gain"]     == 0.00
        assert report["orders"]       == []

    # ── Date range metadata ──────────────────────────────────────────────────

    def test_single_date_start_equals_end(self):
        report = _process_orders([], date_arg="04/09")
        today_year = date.today().year
        assert report["start_date"] == f"{today_year}-04-09"
        assert report["end_date"]   == f"{today_year}-04-09"

    def test_range_dates_stored_correctly(self):
        report = _process_orders([], date_arg="04/01-04/09")
        today_year = date.today().year
        assert report["start_date"] == f"{today_year}-04-01"
        assert report["end_date"]   == f"{today_year}-04-09"

    # ── Sorting ──────────────────────────────────────────────────────────────

    def test_orders_sorted_by_date_then_symbol(self):
        orders = [
            _make_rh_order(chain_symbol="TSLA", execution_ts="2026-04-09T18:00:00Z", order_id="t"),
            _make_rh_order(chain_symbol="AAPL", execution_ts="2026-04-09T18:00:00Z", order_id="a"),
            _make_rh_order(chain_symbol="MSFT", execution_ts="2026-04-08T18:00:00Z", order_id="m"),
        ]
        report = _process_orders(orders, date_arg="04/08-04/09")
        symbols = [o["symbol"] for o in report["orders"]]
        assert symbols == ["MSFT", "AAPL", "TSLA"]


# ─────────────────────────────────────────────────────────────────────────────
# build_options_report integration (with mocked Robinhood auth)
# ─────────────────────────────────────────────────────────────────────────────

class TestBuildOptionsReportIntegration:
    """
    Tests build_options_report() end-to-end by patching the Robinhood
    functions that are called inside the function body.
    """

    def _run(self, orders, date_arg=None):
        import reporter as reporter_mod

        with patch("robin_stocks.robinhood.orders.get_all_option_orders",
                   return_value=orders), \
             patch("auth.login",  return_value=True), \
             patch("auth.logout"):
            report = reporter_mod.build_options_report(date_arg)

        return report

    def test_integration_credit_order(self):
        orders = [_make_rh_order(direction="credit", premium="150.00")]
        report = self._run(orders, date_arg="04/09")
        assert report["total_credit"] == 150.00
        assert report["net_gain"]     == 150.00

    def test_integration_empty_orders(self):
        report = self._run([], date_arg="04/09")
        assert report["order_count"] == 0
        assert report["net_gain"]    == 0.00

    def test_integration_filters_non_filled(self):
        orders = [
            _make_rh_order(state="filled",    premium="100.00"),
            _make_rh_order(state="cancelled", premium="999.00", order_id="x"),
        ]
        report = self._run(orders, date_arg="04/09")
        assert report["order_count"] == 1
        assert report["total_credit"] == 100.00
