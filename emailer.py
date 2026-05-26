"""
emailer.py — Resend Email Delivery
======================================
Renders and sends the daily covered-call recommendation email via Resend.

Email structure:
  - Header: date, market status, portfolio stats
  - Recommendation cards: one per symbol, yield leg + safety leg
  - Earnings warnings: highlighted in red/yellow
  - Footer: disclaimer, settings summary

Uses the HTML template at ./templates/email.html (Jinja2-style).
Falls back to a plain-text version if template not found.
"""

import os
import logging
from datetime import date
from pathlib import Path
from typing import Optional

import resend
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR   = Path(__file__).parent
TEMPLATE_PATH = BASE_DIR / "templates" / "email.html"

# CCS/PCS quality filter thresholds (moved from collar_emailer.py)
_MIN_SPREAD_NET_CREDIT_TOTAL: float = 50.0
_MIN_SPREAD_CREDIT_TO_LOSS_RATIO: float = 0.25


def _build_spread_meta(recs: list, scenarios: int, qualified_before_filter: int) -> dict:
    """Build aggregate metrics dict for a CCS or PCS section header."""
    return {
        "scenarios_evaluated":    scenarios,
        "qualified_opportunities": qualified_before_filter,
        "symbols_recommended":    len(set(r["symbol"] for r in recs)),
        "total_net_credit":       round(sum(r.get("net_credit_total", 0) for r in recs), 2),
        "total_ypd":              round(sum(r.get("ypd", 0) for r in recs), 2),
        "count":                  len(recs),
    }


# ─────────────────────────────────────────────────────────────────────────────
# HTML renderer
# ─────────────────────────────────────────────────────────────────────────────

def _render_html(
    recommendations: list,
    run_meta: dict,
    roll_candidates: list = None,
    btc_candidates: list = None,
    optimize_results: list = None,
    panic_results: list = None,
    rescue_results: list = None,
    safety_results: list = None,
    spread_safety_results: list = None,
    spread_rescue_results: list = None,
    spread_panic_results: list = None,
    strategy_recs: list = None,
    collar_recs: list = None,
    collar_meta: dict = None,
    ccs_recs: list = None,
    pcs_recs: list = None,
    ccs_meta: dict = None,
    pcs_meta: dict = None,
) -> str:
    """
    Render the full HTML email body from recommendations.
    Tries Jinja2 template first; falls back to inline HTML generation.
    """
    roll_candidates  = roll_candidates  or []
    btc_candidates   = btc_candidates   or []
    optimize_results = optimize_results or []
    panic_results    = panic_results    or []
    rescue_results   = rescue_results   or []
    safety_results   = safety_results   or []
    spread_safety_results = spread_safety_results or []
    spread_rescue_results = spread_rescue_results or []
    spread_panic_results  = spread_panic_results  or []
    strategy_recs           = strategy_recs           or []
    collar_recs  = collar_recs  or []
    collar_meta  = collar_meta  or {}
    ccs_recs     = ccs_recs     or []
    pcs_recs     = pcs_recs     or []
    ccs_meta     = ccs_meta     or {}
    pcs_meta     = pcs_meta     or {}
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_PATH.parent)),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template(TEMPLATE_PATH.name)
        return template.render(
            recommendations=recommendations,
            meta=run_meta,
            roll_candidates=roll_candidates,
            btc_candidates=btc_candidates,
            optimize_results=optimize_results,
            panic_results=panic_results,
            rescue_results=rescue_results,
            safety_results=safety_results,
            spread_safety_results=spread_safety_results,
            spread_rescue_results=spread_rescue_results,
            spread_panic_results=spread_panic_results,
            strategy_recs=strategy_recs,
            collar_recs=collar_recs,
            collar_meta=collar_meta,
            ccs_recs=ccs_recs,
            pcs_recs=pcs_recs,
            ccs_meta=ccs_meta,
            pcs_meta=pcs_meta,
        )
    except Exception as e:
        logger.debug(f"Jinja2 template render failed ({e}) — using inline renderer")
        return _render_inline(recommendations, run_meta)


def _pct(val: float) -> str:
    return f"{val:.1f}%"


def _dollar(val: float) -> str:
    return f"${val:,.2f}"


def _flag_badge(flag: Optional[str]) -> str:
    if flag == "red":
        return '<span style="background:#dc2626;color:white;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:bold;">⚠ EARNINGS IN WINDOW</span>'
    if flag == "yellow":
        return '<span style="background:#d97706;color:white;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:bold;">🔔 EARNINGS NEAR EXPIRY</span>'
    return ""


def _render_inline(recommendations: list, meta: dict) -> str:
    """Inline HTML generator — no template dependency."""
    today_str = meta.get("run_date", str(date.today()))
    total_premium = sum(r.get("combined_premium_total", 0) for r in recommendations)
    n = len(recommendations)

    cards_html = ""
    for rec in recommendations:
        sym   = rec["symbol"]
        name  = rec["name"]
        rank  = rec.get("rank", "")
        ann_y = _pct(rec["combined_ann_yield"])
        prem  = _dollar(rec["combined_premium_total"])
        flag  = rec.get("earnings_flag")
        earn_warn = rec.get("earnings_warning", "")
        yl = rec["yield_leg"]
        sl = rec.get("safety_leg")

        y_opt = yl["option"]
        y_detail = (
            f"Strike {_dollar(y_opt['strike'])} | "
            f"Exp {y_opt['expiration']} ({y_opt['dte']}d) | "
            f"Bid/Ask {_dollar(y_opt['bid'])}/{_dollar(y_opt['ask'])} | "
            f"Mid {_dollar(y_opt['mid'])} | "
            f"OTM {_pct(y_opt['otm_pct'])} | "
            f"Ann. Yield {_pct(y_opt['annualized_yield'])} | "
            f"OI {y_opt['open_interest']}"
        )

        safety_section = ""
        if sl:
            s_opt = sl["option"]
            s_detail = (
                f"Strike {_dollar(s_opt['strike'])} | "
                f"Exp {s_opt['expiration']} ({s_opt['dte']}d) | "
                f"Bid/Ask {_dollar(s_opt['bid'])}/{_dollar(s_opt['ask'])} | "
                f"Mid {_dollar(s_opt['mid'])} | "
                f"OTM {_pct(s_opt['otm_pct'])} | "
                f"Ann. Yield {_pct(s_opt['annualized_yield'])} | "
                f"OI {s_opt['open_interest']}"
            )
            safety_section = f"""
            <tr>
              <td style="padding:8px 12px;background:#f0fdf4;border-bottom:1px solid #e2e8f0;">
                <b style="color:#15803d;">🛡 Safety Leg</b> — {sl['contracts']} contract(s)<br>
                <span style="color:#475569;font-size:13px;">{s_detail}</span><br>
                <span style="color:#64748b;font-size:12px;font-style:italic;">{sl['rationale']}</span>
              </td>
            </tr>"""

        warn_section = ""
        if earn_warn:
            bg = "#fef2f2" if flag == "red" else "#fffbeb"
            warn_section = f"""
            <tr>
              <td style="padding:8px 12px;background:{bg};border-bottom:1px solid #e2e8f0;">
                <span style="font-size:13px;">{earn_warn}</span>
              </td>
            </tr>"""

        cards_html += f"""
        <div style="margin-bottom:24px;border:1px solid #e2e8f0;border-radius:8px;overflow:hidden;font-family:Arial,sans-serif;">
          <div style="background:#1e3a5f;color:white;padding:12px 16px;display:flex;justify-content:space-between;align-items:center;">
            <span style="font-size:18px;font-weight:bold;">#{rank} {sym}</span>
            <span style="font-size:13px;opacity:0.85;">{name}</span>
            <span style="background:#22c55e;color:white;padding:4px 10px;border-radius:20px;font-size:13px;font-weight:bold;">{ann_y} ann. yield</span>
          </div>
          <table style="width:100%;border-collapse:collapse;">
            <tr>
              <td style="padding:8px 12px;background:#f8fafc;border-bottom:1px solid #e2e8f0;">
                <span style="color:#64748b;font-size:12px;">TOTAL ESTIMATED PREMIUM</span>&nbsp;
                <span style="font-size:16px;font-weight:bold;color:#1e3a5f;">{prem}</span>
                &nbsp;({rec['contracts_total']} contract(s))
                &nbsp;{_flag_badge(flag)}
              </td>
            </tr>
            <tr>
              <td style="padding:8px 12px;background:#eff6ff;border-bottom:1px solid #e2e8f0;">
                <b style="color:#1d4ed8;">📈 Yield Leg</b> — {yl['contracts']} contract(s)<br>
                <span style="color:#475569;font-size:13px;">{y_detail}</span><br>
                <span style="color:#64748b;font-size:12px;font-style:italic;">{yl['rationale']}</span>
              </td>
            </tr>
            {safety_section}
            {warn_section}
          </table>
        </div>"""

    pur_pct  = meta.get("pur_pct", 0.0)
    pur_open = meta.get("pur_open", 0)
    pur_max  = meta.get("pur_max", 0)
    port_ypd = meta.get("portfolio_ypd", 0.0)

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><title>Covered Call Recommendations — {today_str}</title></head>
<body style="font-family:Arial,sans-serif;background:#f1f5f9;margin:0;padding:20px;">
  <div style="max-width:720px;margin:0 auto;background:white;border-radius:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.08);">

    <!-- Header -->
    <div style="background:#1e3a5f;color:white;padding:24px 32px;">
      <h1 style="margin:0;font-size:22px;">📊 Covered Call Recommendations</h1>
      <p style="margin:8px 0 0;opacity:0.75;font-size:14px;">{today_str} &nbsp;·&nbsp; Safe Mode (≥7% OTM) &nbsp;·&nbsp; {n} symbol(s)</p>
    </div>

    <!-- Summary bar -->
    <div style="background:#1e293b;color:white;padding:12px 32px;font-size:14px;display:flex;gap:24px;flex-wrap:wrap;align-items:center;">
      <span><b>Est. Premium:</b>
        <span style="color:#22c55e;font-size:18px;font-weight:bold;margin-left:6px;">{_dollar(total_premium)}</span>
      </span>
      <span><b>Portfolio Utilization:</b>
        <span style="color:#f59e0b;font-size:18px;font-weight:bold;margin-left:6px;">{pur_pct:.1f}%</span>
        <span style="font-size:12px;color:#94a3b8;margin-left:4px;">({pur_open}/{pur_max} contracts)</span>
      </span>
      <span><b>Est. Total YPD:</b>
        <span style="color:#22c55e;font-size:18px;font-weight:bold;margin-left:6px;">${port_ypd:,.2f}/day</span>
      </span>
    </div>

    <!-- Cards -->
    <div style="padding:24px 32px;">
      {cards_html}
    </div>

    <!-- Disclaimer -->
    <div style="background:#f8fafc;padding:16px 32px;font-size:11px;color:#94a3b8;border-top:1px solid #e2e8f0;">
      <b>Disclaimer:</b> This is an automated analysis tool, not financial advice.
      Options trading involves significant risk. Verify all data before placing trades.
      Premium estimates are based on mid-price and may differ from actual fill prices.
      Always confirm earnings dates independently before selling covered calls.
    </div>

  </div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Plain-text fallback
# ─────────────────────────────────────────────────────────────────────────────

def _render_text(recommendations: list, meta: dict) -> str:
    today_str = meta.get("run_date", str(date.today()))
    lines = [
        f"COVERED CALL RECOMMENDATIONS — {today_str}",
        f"Mode: Safe (≥7% OTM) | {len(recommendations)} symbol(s)",
        "=" * 60,
    ]
    for rec in recommendations:
        yl = rec["yield_leg"]
        yo = yl["option"]
        lines.append(
            f"\n#{rec.get('rank')} {rec['symbol']} ({rec['name']})\n"
            f"  Combined Ann. Yield: {rec['combined_ann_yield']:.1f}%  |  "
            f"Est. Premium: ${rec['combined_premium_total']:.0f}\n"
            f"  Yield Leg  ({yl['contracts']}x): "
            f"Strike ${yo['strike']} | Exp {yo['expiration']} ({yo['dte']}d) | "
            f"Mid ${yo['mid']} | OTM {yo['otm_pct']:.1f}% | Yield {yo['annualized_yield']:.1f}%"
        )
        if rec.get("safety_leg"):
            sl = rec["safety_leg"]
            so = sl["option"]
            lines.append(
                f"  Safety Leg ({sl['contracts']}x): "
                f"Strike ${so['strike']} | Exp {so['expiration']} ({so['dte']}d) | "
                f"Mid ${so['mid']} | OTM {so['otm_pct']:.1f}%"
            )
        if rec.get("earnings_warning"):
            lines.append(f"  {rec['earnings_warning']}")

    lines.append("\nThis is automated analysis, not financial advice.")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Resend sender
# ─────────────────────────────────────────────────────────────────────────────

def send_recommendations(
    recommendations: list,
    run_meta: dict,
    dry_run: bool = False,
    roll_candidates: list = None,
    btc_candidates: list = None,
    optimize_results: list = None,
    panic_results: list = None,
    rescue_results: list = None,
    safety_results: list = None,
    spread_safety_results: list = None,
    spread_rescue_results: list = None,
    spread_panic_results: list = None,
    strategy_recs: list = None,
    collar_recs: list = None,
    collar_meta: dict = None,
    ccs_recs: list = None,
    pcs_recs: list = None,
    ccs_scenarios: int = 0,
    pcs_scenarios: int = 0,
) -> bool:
    """
    Send the daily covered-call email via Resend.

    Args:
        recommendations:  Output from diversifier.build_recommendations() + earnings warnings
        run_meta:         Dict with run context (run_date, duration_sec, etc.)
        dry_run:          If True, renders email but does not send
        optimize_results: List of optimize-roll result dicts from execute_optimize_rolls()
        panic_results:    List of panic-roll result dicts from execute_panic_rolls()
        rescue_results:   List of rescue-roll result dicts from execute_rescue_rolls()
        safety_results:   List of safety BTC result dicts from execute_safety_btc_orders()

    Returns:
        True on success (or dry_run), False on failure.
    """
    api_key       = os.getenv("RESEND_API_KEY", "").strip()
    sender        = os.getenv("RESEND_FROM", "").strip()
    recipient     = run_meta.get("recipient_email", "")

    if not recipient:
        logger.error("recipient_email not set in config.yaml")
        return False

    today_str = run_meta.get("run_date", str(date.today()))
    n = len(recommendations)
    flagged = sum(1 for r in recommendations if r.get("earnings_flag"))

    # ── Collar / CCS / PCS prep ──────────────────────────────────────────
    collar_recs = collar_recs or []
    ccs_recs_raw = ccs_recs or []
    pcs_recs_raw = pcs_recs or []
    collar_meta = collar_meta or {}

    # Quality filter: suppress recs below $50 net credit or 0.25 C/L ratio
    ccs_qualified = len(ccs_recs_raw)
    pcs_qualified = len(pcs_recs_raw)
    ccs_recs_filtered = [
        r for r in ccs_recs_raw
        if r.get("net_credit_total", 0) >= _MIN_SPREAD_NET_CREDIT_TOTAL
        and r.get("credit_to_loss_ratio", 0) >= _MIN_SPREAD_CREDIT_TO_LOSS_RATIO
    ]
    pcs_recs_filtered = [
        r for r in pcs_recs_raw
        if r.get("net_credit_total", 0) >= _MIN_SPREAD_NET_CREDIT_TOTAL
        and r.get("credit_to_loss_ratio", 0) >= _MIN_SPREAD_CREDIT_TO_LOSS_RATIO
    ]

    ccs_meta_built = _build_spread_meta(ccs_recs_filtered, ccs_scenarios, ccs_qualified)
    pcs_meta_built = _build_spread_meta(pcs_recs_filtered, pcs_scenarios, pcs_qualified)

    # ── Unified subject line ─────────────────────────────────────────────
    collar_n = len(collar_recs)
    ccs_n = len(ccs_recs_filtered)
    pcs_n = len(pcs_recs_filtered)

    subject = (
        f"📊 Daily Options — {today_str} — ⚪ No new recommendations"
        if n == 0 and collar_n == 0 and ccs_n == 0 and pcs_n == 0
        else f"📊 Daily Options — {today_str} — {n} CC recs"
    )
    if collar_n:
        subject += f" | {collar_n} collars"
    if ccs_n or pcs_n:
        subject += f" | {ccs_n} CCS, {pcs_n} PCS"
    if flagged:
        subject += f" | ⚠️ {flagged} earnings warning(s)"
    optimize_acted  = [o for o in (optimize_results or []) if not o.get("skipped")]
    optimize_ok     = sum(1 for o in optimize_acted if o.get("success"))
    optimize_fail   = len(optimize_acted) - optimize_ok
    if optimize_ok:
        subject += f" | 🚀 {optimize_ok} optimize roll(s)"
    if optimize_fail:
        subject += f" | ⚠️ {optimize_fail} OPTIMIZE ROLL FAILED"
    panic_failures = sum(1 for p in (panic_results or []) if not p.get("success"))
    panic_ok       = sum(1 for p in (panic_results or []) if p.get("success"))
    if panic_ok:
        subject += f" | ⚡ {panic_ok} panic roll(s)"
    if panic_failures:
        subject += f" | 🚨 {panic_failures} PANIC ROLL FAILED"
    rescue_acted  = [g for g in (rescue_results or []) if not g.get("skipped")]
    rescue_ok     = sum(1 for g in rescue_acted if g.get("success"))
    rescue_fail   = len(rescue_acted) - rescue_ok
    if rescue_ok:
        subject += f" | 🎯 {rescue_ok} rescue roll(s)"
    if rescue_fail:
        subject += f" | ⚠️ {rescue_fail} RESCUE ROLL FAILED"
    safety_failures = sum(1 for s in (safety_results or []) if not s.get("success"))
    safety_ok       = sum(1 for s in (safety_results or []) if s.get("success"))
    if safety_ok:
        subject += f" | 🛡 {safety_ok} safety BTC(s)"
    if safety_failures:
        subject += f" | ⚠️ {safety_failures} safety BTC failed"

    # Spread management subject indicators
    n_sp_saf = len(spread_safety_results or [])
    n_sp_res = len(spread_rescue_results or [])
    n_sp_pan = len(spread_panic_results or [])
    if n_sp_saf:
        subject += f" | 📐 {n_sp_saf} spread safety(s)"
    if n_sp_res:
        subject += f" | 📐 {n_sp_res} spread rescue(s)"
    if n_sp_pan:
        subject += f" | 📐 {n_sp_pan} spread panic(s)"

    html_body = _render_html(recommendations, run_meta,
                             roll_candidates=roll_candidates or [],
                             btc_candidates=btc_candidates or [],
                             optimize_results=optimize_results or [],
                             panic_results=panic_results or [],
                             rescue_results=rescue_results or [],
                             safety_results=safety_results or [],
                             spread_safety_results=spread_safety_results or [],
                             spread_rescue_results=spread_rescue_results or [],
                             spread_panic_results=spread_panic_results or [],
                             strategy_recs=strategy_recs or [],
                             collar_recs=collar_recs,
                             collar_meta=collar_meta,
                             ccs_recs=ccs_recs_filtered,
                             pcs_recs=pcs_recs_filtered,
                             ccs_meta=ccs_meta_built,
                             pcs_meta=pcs_meta_built)
    text_body = _render_text(recommendations, run_meta)

    if dry_run:
        logger.info(f"[DRY RUN] Email would be sent to {recipient}")
        logger.info(f"  Subject: {subject}")
        logger.info(f"  {n} recommendations, {flagged} earnings warnings")
        # Save HTML preview
        preview_path = BASE_DIR / "logs" / f"email_preview_{today_str}.html"
        preview_path.parent.mkdir(exist_ok=True)
        preview_path.write_text(html_body)
        logger.info(f"  HTML preview saved: {preview_path}")
        return True

    if not api_key or not sender:
        logger.error("RESEND_API_KEY or RESEND_FROM missing from .env")
        return False

    resend.api_key = api_key
    params: dict = {
        "from":    sender,
        "to":      [recipient],
        "subject": subject,
        "html":    html_body,
        "text":    text_body,
    }

    import time as _time
    for attempt in range(1, 4):   # up to 3 attempts
        try:
            response = resend.Emails.send(params)
            msg_id = response.get("id") if isinstance(response, dict) else None
            if msg_id:
                logger.info(f"✅  Email sent to {recipient} (Resend id {msg_id})")
                return True
            logger.error(
                f"Resend error (attempt {attempt}/3): unexpected response {response!r}"
            )
        except Exception as e:
            logger.warning(f"Email send failed (attempt {attempt}/3): {e}")
        if attempt < 3:
            _time.sleep(30)

    logger.error("Email send failed after 3 attempts")
    return False
