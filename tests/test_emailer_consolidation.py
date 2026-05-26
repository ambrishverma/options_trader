"""
tests/test_emailer_consolidation.py — Verify emailer.py collar/CCS/PCS consolidation
=====================================================================================
Tests that _render_html() and send_recommendations() accept the new collar/CCS/PCS
parameters, that subject lines include the right counts, and that quality filters
suppress low-quality spread recs.
"""

import sys, os, logging

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from emailer import _render_html, send_recommendations, _build_spread_meta

# ── Test helpers ──────────────────────────────────────────────────────────────

META = {
    "run_date": "2026-05-26",
    "duration_sec": 10.0,
    "recipient_email": "test@test.com",
    "pur_pct": 50.0,
    "pur_open": 7,
    "pur_max": 14,
    "portfolio_ypd": 30.0,
}


def _make_cc_rec(symbol="AAPL", **overrides):
    """Return a minimal covered-call rec dict for emailer tests."""
    yo = {
        "symbol": symbol, "name": "Corp", "strike": 200.0,
        "expiration": "2026-06-01", "dte": 5, "mid": 1.0,
        "bid": 0.95, "ask": 1.05, "otm_pct": 8.0,
        "annualized_yield": 40.0, "open_interest": 50, "contracts": 2,
    }
    rec = {
        "symbol": symbol, "name": "Corp", "rank": 1,
        "contracts_total": 2,
        "combined_premium_total": 200.0,
        "combined_ann_yield": 30.0,
        "combined_ypd": 20.0,
        "earnings_flag": None, "earnings_warning": None,
        "yield_leg": {"option": yo, "contracts": 1, "rationale": "r", "ypd": 20.0},
        "safety_leg": None,
    }
    rec.update(overrides)
    return rec


def _make_collar_rec(symbol="MSFT", **overrides):
    """Return a minimal collar rec dict."""
    rec = {
        "symbol": symbol,
        "name": "Test Corp",
        "current_price": 100.0,
        "market_value": 30000.0,
        "contracts": 3,
        "expiration": "2026-06-20",
        "dte": 25,
        "cc_expiration": "2026-06-20",
        "cc_dte": 25,
        "lp_expiration": "2026-06-20",
        "lp_dte": 25,
        "call_leg": {"strike": 115.0, "bid": 1.45, "ask": 1.55, "mid": 1.50,
                     "open_interest": 20, "otm_pct": 15.0, "annualized_yield": 21.9},
        "put_leg": {"strike": 93.0, "bid": 1.15, "ask": 1.25, "mid": 1.20,
                    "open_interest": 15, "protection_pct": -7.0},
        "net_gain_per_share": 0.30,
        "net_gain_total": 90.0,
        "upside_cap_pct": 15.0,
        "downside_floor_pct": -7.0,
        "low_gain": False,
        "earnings_date": None,
        "earnings_warning": None,
    }
    rec.update(overrides)
    return rec


def _make_spread_rec(spread_type="PCS", **overrides):
    """Return a minimal CCS/PCS rec dict."""
    rec = {
        "symbol": "NVDA",
        "spread_type": spread_type,
        "short_strike": 120.0,
        "long_strike": 115.0,
        "spread_width": 5.0,
        "expiration": "2026-06-20",
        "dte": 25,
        "net_credit_per_share": 1.50,
        "net_credit_total": 150.0,
        "max_loss": 350.0,
        "credit_to_loss_ratio": 0.43,
        "ypd": 6.0,
        "contracts": 1,
    }
    rec.update(overrides)
    return rec


def _capture_subject(dry_run_kwargs) -> str:
    """
    Run send_recommendations(dry_run=True) and capture the Subject: line from logs.
    Returns the subject string.
    """
    import logging.handlers

    handler = logging.handlers.MemoryHandler(capacity=100)
    handler.setFormatter(logging.Formatter("%(message)s"))

    emailer_logger = logging.getLogger("emailer")
    emailer_logger.addHandler(handler)
    original_level = emailer_logger.level
    emailer_logger.setLevel(logging.DEBUG)

    try:
        send_recommendations(**dry_run_kwargs)
        handler.flush()

        for record in handler.buffer:
            msg = handler.formatter.format(record)
            if "Subject:" in msg:
                return msg.split("Subject:", 1)[1].strip()
        return ""
    finally:
        emailer_logger.removeHandler(handler)
        emailer_logger.setLevel(original_level)


# ── A. _render_html() accepts collar/CCS/PCS params ─────────────────────────

def test_render_html_accepts_collar_params():
    """_render_html() should not error when collar params are provided."""
    html = _render_html(
        [_make_cc_rec()], META,
        collar_recs=[_make_collar_rec()],
        collar_meta={"eligible_holdings": 4, "symbols_with_collars": 1},
    )
    assert html  # Non-empty HTML returned


def test_render_html_accepts_ccs_pcs_params():
    """_render_html() should not error when CCS/PCS params are provided."""
    html = _render_html(
        [_make_cc_rec()], META,
        ccs_recs=[_make_spread_rec("CCS")],
        pcs_recs=[_make_spread_rec("PCS")],
        ccs_meta={"count": 1, "total_net_credit": 150.0},
        pcs_meta={"count": 1, "total_net_credit": 150.0},
    )
    assert html  # Non-empty HTML returned


def test_render_html_accepts_all_new_params_together():
    """_render_html() should accept all collar+CCS+PCS params simultaneously."""
    html = _render_html(
        [], META,
        collar_recs=[_make_collar_rec()],
        collar_meta={"eligible_holdings": 4},
        ccs_recs=[_make_spread_rec("CCS")],
        pcs_recs=[_make_spread_rec("PCS")],
        ccs_meta={"count": 1},
        pcs_meta={"count": 1},
    )
    assert html


# ── B. send_recommendations() accepts collar/CCS/PCS params ─────────────────

def test_send_recs_dry_run_with_collar_params():
    """send_recommendations(dry_run=True) should not error with collar params."""
    result = send_recommendations(
        recommendations=[_make_cc_rec()],
        run_meta=META,
        dry_run=True,
        collar_recs=[_make_collar_rec()],
        collar_meta={"eligible_holdings": 4},
    )
    assert result is True


def test_send_recs_dry_run_with_ccs_pcs_params():
    """send_recommendations(dry_run=True) should not error with CCS/PCS params."""
    result = send_recommendations(
        recommendations=[_make_cc_rec()],
        run_meta=META,
        dry_run=True,
        ccs_recs=[_make_spread_rec("CCS")],
        pcs_recs=[_make_spread_rec("PCS")],
        ccs_scenarios=500,
        pcs_scenarios=600,
    )
    assert result is True


# ── C. Subject line includes collar counts ───────────────────────────────────

def test_subject_includes_collar_count():
    """Subject should contain 'collar' when collar_recs are provided."""
    subject = _capture_subject({
        "recommendations": [_make_cc_rec()],
        "run_meta": META,
        "dry_run": True,
        "collar_recs": [_make_collar_rec(), _make_collar_rec(symbol="GOOG")],
        "collar_meta": {},
    })
    assert "2 collars" in subject, f"Expected '2 collars' in: {subject}"


def test_subject_omits_collar_segment_when_zero():
    """Subject should NOT mention 'collar' when collar_recs is empty."""
    subject = _capture_subject({
        "recommendations": [_make_cc_rec()],
        "run_meta": META,
        "dry_run": True,
        "collar_recs": [],
        "collar_meta": {},
    })
    assert "collar" not in subject.lower(), f"Unexpected 'collar' in: {subject}"


# ── D. Subject line includes CCS/PCS counts ─────────────────────────────────

def test_subject_includes_ccs_pcs_counts():
    """Subject should contain CCS/PCS counts when spread recs are provided."""
    subject = _capture_subject({
        "recommendations": [_make_cc_rec()],
        "run_meta": META,
        "dry_run": True,
        "ccs_recs": [_make_spread_rec("CCS")],
        "pcs_recs": [_make_spread_rec("PCS"), _make_spread_rec("PCS", symbol="AMD")],
        "ccs_scenarios": 100,
        "pcs_scenarios": 200,
    })
    assert "1 CCS" in subject, f"Expected '1 CCS' in: {subject}"
    assert "2 PCS" in subject, f"Expected '2 PCS' in: {subject}"


def test_subject_omits_ccs_pcs_when_zero():
    """Subject should NOT mention CCS/PCS when none are provided."""
    subject = _capture_subject({
        "recommendations": [_make_cc_rec()],
        "run_meta": META,
        "dry_run": True,
    })
    assert "CCS" not in subject, f"Unexpected 'CCS' in: {subject}"
    assert "PCS" not in subject, f"Unexpected 'PCS' in: {subject}"


# ── E. Subject line unified format ──────────────────────────────────────────

def test_subject_unified_format_cc_only():
    """Subject should use 'Daily Options' and 'CC recs' format."""
    subject = _capture_subject({
        "recommendations": [_make_cc_rec(), _make_cc_rec(symbol="GOOG")],
        "run_meta": META,
        "dry_run": True,
    })
    assert "Daily Options" in subject, f"Expected 'Daily Options' in: {subject}"
    assert "2 CC recs" in subject, f"Expected '2 CC recs' in: {subject}"


def test_subject_unified_format_no_recs():
    """Subject should show 'No new recommendations' when all rec lists are empty."""
    subject = _capture_subject({
        "recommendations": [],
        "run_meta": META,
        "dry_run": True,
        "collar_recs": [],
        "ccs_recs": [],
        "pcs_recs": [],
    })
    assert "No new recommendations" in subject, f"Expected 'No new recommendations' in: {subject}"


# ── F. Quality filter suppresses low-quality spread recs ─────────────────────

def test_quality_filter_suppresses_low_credit():
    """Spread recs with net_credit_total < $50 should be filtered out."""
    low_credit = _make_spread_rec("PCS", net_credit_total=30.0, credit_to_loss_ratio=0.50)
    good_credit = _make_spread_rec("PCS", net_credit_total=150.0, credit_to_loss_ratio=0.43)

    subject = _capture_subject({
        "recommendations": [],
        "run_meta": META,
        "dry_run": True,
        "pcs_recs": [low_credit, good_credit],
        "pcs_scenarios": 100,
    })
    # Only 1 PCS should survive the filter (the low-credit one gets suppressed)
    assert "1 PCS" in subject, f"Expected '1 PCS' (filter suppressed low credit) in: {subject}"


def test_quality_filter_suppresses_low_cl_ratio():
    """Spread recs with credit_to_loss_ratio < 0.25 should be filtered out."""
    low_ratio = _make_spread_rec("CCS", net_credit_total=100.0, credit_to_loss_ratio=0.10)
    good_ratio = _make_spread_rec("CCS", net_credit_total=100.0, credit_to_loss_ratio=0.35)

    subject = _capture_subject({
        "recommendations": [],
        "run_meta": META,
        "dry_run": True,
        "ccs_recs": [low_ratio, good_ratio],
        "ccs_scenarios": 200,
    })
    # Only 1 CCS should survive the filter
    assert "1 CCS" in subject, f"Expected '1 CCS' (filter suppressed low C/L) in: {subject}"


def test_quality_filter_suppresses_all_low_quality():
    """When all spread recs are below threshold, CCS/PCS counts should be 0."""
    low1 = _make_spread_rec("PCS", net_credit_total=20.0, credit_to_loss_ratio=0.50)
    low2 = _make_spread_rec("PCS", net_credit_total=100.0, credit_to_loss_ratio=0.10)

    subject = _capture_subject({
        "recommendations": [_make_cc_rec()],
        "run_meta": META,
        "dry_run": True,
        "pcs_recs": [low1, low2],
        "pcs_scenarios": 100,
    })
    # Both filtered out -> no PCS segment in subject
    assert "PCS" not in subject, f"Expected no PCS (all filtered) in: {subject}"


# ── G. _build_spread_meta helper ────────────────────────────────────────────

def test_build_spread_meta_basic():
    """_build_spread_meta should compute correct aggregate metrics."""
    recs = [
        _make_spread_rec("PCS", symbol="AAPL", net_credit_total=150.0, ypd=6.0),
        _make_spread_rec("PCS", symbol="NVDA", net_credit_total=200.0, ypd=8.0),
    ]
    meta = _build_spread_meta(recs, scenarios=500, qualified_before_filter=5)
    assert meta["scenarios_evaluated"] == 500
    assert meta["qualified_opportunities"] == 5
    assert meta["symbols_recommended"] == 2
    assert meta["total_net_credit"] == 350.0
    assert meta["total_ypd"] == 14.0
    assert meta["count"] == 2


def test_build_spread_meta_empty():
    """_build_spread_meta with empty list should return zeros."""
    meta = _build_spread_meta([], scenarios=100, qualified_before_filter=0)
    assert meta["count"] == 0
    assert meta["symbols_recommended"] == 0
    assert meta["total_net_credit"] == 0
    assert meta["total_ypd"] == 0
