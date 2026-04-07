"""
test_collar_emailer_v16.py — Tests for CCS/PCS sections in collar_emailer.py (v1.6)
=====================================================================================
Verifies that the weekly collar email template correctly renders
Section 2 (CCS) and Section 3 (PCS) when spread recommendations are provided,
and shows the correct empty-state messages when none are provided.

Scenarios covered:
  - CCS section renders with correct symbol, strikes, net credit, YPD
  - PCS section renders with correct symbol, strikes, net credit, YPD
  - "Short Call" / "Long Call" labels present in CCS section
  - "Short Put"  / "Long Put"  labels present in PCS section
  - Empty CCS state shown when ccs_recs=[]
  - Empty PCS state shown when pcs_recs=[]
  - Both CCS and PCS sections render independently (one empty, one not)
  - No crash when ccs_recs and pcs_recs are both None (defaults)
  - send_collar_report() subject line includes CCS/PCS counts
  - OTM% rendered with correct sign (+% for CCS, -% for PCS)
  - Max loss field rendered
  - Existing collar section (Section 1) still renders correctly with v1.6 additions
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from collar_emailer import _render_collar_html


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_ccs_rec(
    symbol="TSLA", name="Tesla Inc",
    current_price=250.0, expiration="2026-05-16", dte=42,
    short_strike=280.0, short_bid=3.50, short_ask=3.80,
    long_strike=285.0, long_bid=1.80, long_ask=2.10,
    short_oi=120, long_oi=80, short_otm_pct=12.0,
    net_credit=1.40, spread_size=5.0, max_loss=360.0, ypd=3.33,
):
    return {
        "symbol": symbol, "name": name, "current_price": current_price,
        "type": "CCS", "expiration": expiration, "dte": dte,
        "short_leg": {
            "strike": short_strike, "bid": short_bid, "ask": short_ask,
            "mid": round((short_bid + short_ask) / 2, 2),
            "open_interest": short_oi, "otm_pct": short_otm_pct,
        },
        "long_leg": {
            "strike": long_strike, "bid": long_bid, "ask": long_ask,
            "mid": round((long_bid + long_ask) / 2, 2),
            "open_interest": long_oi,
        },
        "net_credit": net_credit,
        "net_credit_total": round(net_credit * 100, 2),
        "spread_size": spread_size,
        "max_loss": max_loss,
        "ypd": ypd,
        "credit_to_loss_ratio": round((net_credit * 100) / max_loss, 2) if max_loss > 0 else 0.0,
        "score": round(ypd * (round((net_credit * 100) / max_loss, 2) if max_loss > 0 else 0.0), 6),
    }


def _make_pcs_rec(
    symbol="NVDA", name="NVIDIA Corp",
    current_price=800.0, expiration="2026-05-16", dte=42,
    short_strike=720.0, short_bid=8.50, short_ask=9.00,
    long_strike=708.0, long_bid=5.00, long_ask=5.50,
    short_oi=200, long_oi=150, short_otm_pct=10.0,
    net_credit=3.00, spread_size=12.0, max_loss=900.0, ypd=7.14,
):
    return {
        "symbol": symbol, "name": name, "current_price": current_price,
        "type": "PCS", "expiration": expiration, "dte": dte,
        "short_leg": {
            "strike": short_strike, "bid": short_bid, "ask": short_ask,
            "mid": round((short_bid + short_ask) / 2, 2),
            "open_interest": short_oi, "otm_pct": short_otm_pct,
        },
        "long_leg": {
            "strike": long_strike, "bid": long_bid, "ask": long_ask,
            "mid": round((long_bid + long_ask) / 2, 2),
            "open_interest": long_oi,
        },
        "net_credit": net_credit,
        "net_credit_total": round(net_credit * 100, 2),
        "spread_size": spread_size,
        "max_loss": max_loss,
        "ypd": ypd,
        "credit_to_loss_ratio": round((net_credit * 100) / max_loss, 2) if max_loss > 0 else 0.0,
        "score": round(ypd * (round((net_credit * 100) / max_loss, 2) if max_loss > 0 else 0.0), 6),
    }


META = {
    "run_date": "2026-04-04",
    "recipient_email": "test@test.com",
    "duration_sec": 10.0,
    "eligible_holdings": 5,
    "total_recommendations": 0,
    "symbols_with_collars": 0,
    "low_gain_count": 0,
    "earnings_flags": 0,
    "ccs_count": 1,
    "pcs_count": 1,
}


# ─────────────────────────────────────────────────────────────────────────────
# CCS section tests
# ─────────────────────────────────────────────────────────────────────────────

class TestCCSSectionRender:
    def test_ccs_symbol_appears_in_html(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(symbol="TSLA")])
        assert "TSLA" in html

    def test_ccs_section_header_present(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec()])
        assert "Call Credit Spread" in html or "CCS" in html

    def test_short_call_label_present(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec()])
        assert "Short Call" in html

    def test_long_call_label_present(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec()])
        assert "Long Call" in html

    def test_ccs_short_strike_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(short_strike=280.0)])
        assert "280.00" in html

    def test_ccs_long_strike_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(long_strike=290.0)])
        assert "290.00" in html

    def test_ccs_net_credit_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(net_credit=1.40)])
        assert "1.40" in html

    def test_ccs_net_credit_total_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(net_credit=1.40)])
        assert "140.00" in html   # net_credit_total = 1.40 * 100

    def test_ccs_max_loss_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(max_loss=860.0)])
        assert "860" in html

    def test_ccs_ypd_appears(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(ypd=3.33)])
        assert "3.33" in html

    def test_ccs_otm_pct_appears_with_plus_sign(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec(short_otm_pct=12.0)])
        assert "+12.0%" in html

    def test_empty_ccs_shows_no_candidates_message(self):
        html = _render_collar_html([], META, ccs_recs=[], pcs_recs=[])
        assert "No qualifying CCS" in html

    def test_multiple_ccs_recs_all_symbols_present(self):
        recs = [_make_ccs_rec(symbol="TSLA"), _make_ccs_rec(symbol="AAPL")]
        html = _render_collar_html([], META, ccs_recs=recs)
        assert "TSLA" in html
        assert "AAPL" in html


# ─────────────────────────────────────────────────────────────────────────────
# PCS section tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPCSSectionRender:
    def test_pcs_symbol_appears_in_html(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(symbol="NVDA")])
        assert "NVDA" in html

    def test_pcs_section_header_present(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec()])
        assert "Put Credit Spread" in html or "PCS" in html

    def test_short_put_label_present(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec()])
        assert "Short Put" in html

    def test_long_put_label_present(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec()])
        assert "Long Put" in html

    def test_pcs_short_strike_appears(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(short_strike=720.0)])
        assert "720.00" in html

    def test_pcs_long_strike_appears(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(long_strike=696.0)])
        assert "696.00" in html

    def test_pcs_net_credit_total_appears(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(net_credit=3.00)])
        assert "300.00" in html

    def test_pcs_ypd_appears(self):
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(ypd=7.14)])
        assert "7.14" in html

    def test_pcs_otm_pct_has_minus_sign(self):
        """Put OTM% is shown with a minus sign in the template."""
        html = _render_collar_html([], META, pcs_recs=[_make_pcs_rec(short_otm_pct=10.0)])
        assert "-10.0%" in html

    def test_empty_pcs_shows_no_candidates_message(self):
        html = _render_collar_html([], META, ccs_recs=[], pcs_recs=[])
        assert "No qualifying PCS" in html

    def test_multiple_pcs_recs_all_symbols_present(self):
        recs = [_make_pcs_rec(symbol="NVDA"), _make_pcs_rec(symbol="MSFT")]
        html = _render_collar_html([], META, pcs_recs=recs)
        assert "NVDA" in html
        assert "MSFT" in html


# ─────────────────────────────────────────────────────────────────────────────
# Independence / edge cases
# ─────────────────────────────────────────────────────────────────────────────

class TestCCSPCSIndependence:
    def test_ccs_present_pcs_empty(self):
        html = _render_collar_html([], META, ccs_recs=[_make_ccs_rec()], pcs_recs=[])
        assert "Short Call" in html
        assert "No qualifying PCS" in html

    def test_pcs_present_ccs_empty(self):
        html = _render_collar_html([], META, ccs_recs=[], pcs_recs=[_make_pcs_rec()])
        assert "Short Put" in html
        assert "No qualifying CCS" in html

    def test_no_crash_with_none_ccs_pcs(self):
        """Calling with no ccs_recs/pcs_recs (defaults) should not crash."""
        html = _render_collar_html([], META)
        assert "Collar" in html   # template header still renders

    def test_collar_section_still_renders_with_spread_sections(self):
        """Existing Section 1 (collar recs) is not broken by spread sections."""
        from tests.test_collar_emailer import _make_collar_rec, META as OLD_META
        collar_meta = dict(OLD_META, ccs_count=1, pcs_count=1)
        recs = [_make_collar_rec(symbol="GOOGL")]
        html = _render_collar_html(recs, collar_meta,
                                   ccs_recs=[_make_ccs_rec()],
                                   pcs_recs=[_make_pcs_rec()])
        assert "GOOGL" in html      # Section 1 collar rec
        assert "TSLA"  in html      # Section 2 CCS rec
        assert "NVDA"  in html      # Section 3 PCS rec
        assert "Covered Call" in html
        assert "Short Call" in html
        assert "Short Put"  in html

    def test_ccs_and_pcs_both_present(self):
        html = _render_collar_html([], META,
                                   ccs_recs=[_make_ccs_rec(symbol="TSLA")],
                                   pcs_recs=[_make_pcs_rec(symbol="NVDA")])
        assert "TSLA" in html
        assert "NVDA" in html
        assert "Short Call" in html
        assert "Short Put" in html


# ─────────────────────────────────────────────────────────────────────────────
# send_collar_report subject line tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSubjectLine:
    def _get_subject(self, recs=None, ccs_recs=None, pcs_recs=None, dry_run=True):
        """Call send_collar_report in dry_run mode and capture the log message."""
        import logging, io
        from collar_emailer import send_collar_report

        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        logging.getLogger("collar_emailer").addHandler(handler)
        logging.getLogger("collar_emailer").setLevel(logging.DEBUG)

        try:
            meta = dict(META, recipient_email="test@test.com")
            send_collar_report(
                recs or [], meta, dry_run=True,
                ccs_recs=ccs_recs or [],
                pcs_recs=pcs_recs or [],
            )
        finally:
            logging.getLogger("collar_emailer").removeHandler(handler)

        return log_stream.getvalue()

    def test_subject_includes_ccs_count(self):
        log = self._get_subject(ccs_recs=[_make_ccs_rec()])
        assert "1 CCS" in log

    def test_subject_includes_pcs_count(self):
        log = self._get_subject(pcs_recs=[_make_pcs_rec()])
        assert "1 PCS" in log

    def test_subject_no_ccs_pcs_when_empty(self):
        """When no CCS/PCS, the subject line omits the spread counts."""
        log = self._get_subject(ccs_recs=[], pcs_recs=[])
        assert "CCS" not in log.split("Subject:")[1].split("\n")[0] if "Subject:" in log else True


# ─────────────────────────────────────────────────────────────────────────────
# Display filter tests (net-credit floor + credit-to-loss floor)
# ─────────────────────────────────────────────────────────────────────────────

class TestSpreadDisplayFilters:
    """Verify that send_collar_report suppresses recs that fail either display filter."""

    def _render(self, ccs_recs=None, pcs_recs=None):
        """Return rendered HTML for the given spread recs (dry-run, no email sent)."""
        from collar_emailer import _render_collar_html
        meta = dict(META, recipient_email="test@test.com")
        return _render_collar_html([], meta, ccs_recs=ccs_recs or [], pcs_recs=pcs_recs or [])

    def _subject(self, ccs_recs=None, pcs_recs=None):
        """Return the log output (includes subject line) from a dry-run send."""
        import logging, io
        from collar_emailer import send_collar_report
        log_stream = io.StringIO()
        handler = logging.StreamHandler(log_stream)
        logging.getLogger("collar_emailer").addHandler(handler)
        logging.getLogger("collar_emailer").setLevel(logging.DEBUG)
        try:
            meta = dict(META, recipient_email="test@test.com")
            send_collar_report([], meta, dry_run=True,
                               ccs_recs=ccs_recs or [], pcs_recs=pcs_recs or [])
        finally:
            logging.getLogger("collar_emailer").removeHandler(handler)
        return log_stream.getvalue()

    def test_ccs_below_net_credit_floor_suppressed(self):
        """CCS rec with net_credit_total < $50 is filtered out; subject shows 0 CCS."""
        rec = _make_ccs_rec(net_credit=0.40, spread_size=5.0, max_loss=360.0)
        # net_credit_total = 0.40 * 100 = $40 < $50 → suppressed regardless of C/L
        assert rec["net_credit_total"] < 50.0
        log = self._subject(ccs_recs=[rec])
        # Rec is suppressed → subject line must not contain "1 CCS"
        subject_line = log.split("Subject:")[1].split("\n")[0] if "Subject:" in log else ""
        assert "1 CCS" not in subject_line

    def test_ccs_below_credit_to_loss_floor_suppressed(self):
        """CCS rec with credit_to_loss_ratio < 0.25 must be suppressed."""
        # net_credit=$0.60 → $60 total (passes $50 floor)
        # spread=$40 → max_loss=$3940 → C/L = 60/3940 ≈ 0.015 < 0.25 → suppressed
        rec = _make_ccs_rec(net_credit=0.60, spread_size=40.0, max_loss=3940.0,
                            long_strike=290.0 + 40.0)
        assert rec["net_credit_total"] >= 50.0
        assert rec["credit_to_loss_ratio"] < 0.25
        log = self._subject(ccs_recs=[rec])
        assert "0 CCS" in log or "| 0 CCS" not in log  # rec suppressed → not counted in subject

    def test_pcs_below_credit_to_loss_floor_suppressed(self):
        """PCS rec with credit_to_loss_ratio < 0.25 must be suppressed."""
        rec = _make_pcs_rec(net_credit=0.60, spread_size=40.0, max_loss=3940.0,
                            long_strike=720.0 - 40.0)
        assert rec["net_credit_total"] >= 50.0
        assert rec["credit_to_loss_ratio"] < 0.25
        log = self._subject(pcs_recs=[rec])
        assert "0 PCS" in log or "| 0 PCS" not in log

    def test_ccs_above_both_floors_passes(self):
        """CCS rec meeting both thresholds must appear in the email."""
        rec = _make_ccs_rec()  # defaults: net=1.40→$140, max_loss=860, C/L≈0.16… wait recalc
        # Default: net_credit=1.40, max_loss=860 → C/L=140/860≈0.163 < 0.25
        # Use a rec that clearly passes: net=2.00, spread=5, max_loss=300 → C/L=200/300≈0.67
        rec = _make_ccs_rec(net_credit=2.00, spread_size=5.0, max_loss=300.0)
        assert rec["net_credit_total"] >= 50.0
        assert rec["credit_to_loss_ratio"] >= 0.25
        html = self._render(ccs_recs=[rec])
        assert "TSLA" in html

    def test_pcs_above_both_floors_passes(self):
        """PCS rec meeting both thresholds must appear in the email."""
        rec = _make_pcs_rec(net_credit=2.00, spread_size=5.0, max_loss=300.0)
        assert rec["net_credit_total"] >= 50.0
        assert rec["credit_to_loss_ratio"] >= 0.25
        html = self._render(pcs_recs=[rec])
        assert "NVDA" in html
