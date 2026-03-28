"""
collar_emailer.py — Collar Report Email Delivery
==================================================
Renders and sends the weekly collar recommendation email via SendGrid.
Mirrors emailer.py structure but for collar recs.
"""

import os
import logging
from datetime import date
from pathlib import Path
from typing import List

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).parent
TEMPLATE_PATH = BASE_DIR / "templates" / "collar_email.html"


def _render_collar_html(recommendations: List[dict], meta: dict) -> str:
    """Render collar email HTML via Jinja2 template."""
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_PATH.parent)),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template(TEMPLATE_PATH.name)
        return template.render(recommendations=recommendations, meta=meta)
    except Exception as e:
        logger.warning(f"Collar template render failed ({e}) — using fallback")
        return _render_collar_fallback(recommendations, meta)


def _render_collar_fallback(recommendations: List[dict], meta: dict) -> str:
    """Minimal plain-HTML fallback when template is unavailable."""
    today = meta.get("run_date", str(date.today()))
    n = len(recommendations)
    rows = ""
    for rec in recommendations:
        low_style = ' style="background:#fff1f2"' if rec.get("low_gain") else ""
        rows += f"""
        <tr{low_style}>
          <td><b>{rec['symbol']}</b></td>
          <td>{rec['expiration']} ({rec['dte']}d)</td>
          <td>CC ${rec['call_leg']['strike']} / LP ${rec['put_leg']['strike']}</td>
          <td>${rec['net_gain_per_share']:.2f}/share &middot; ${rec['net_gain_total']:.0f} total</td>
          <td>{rec['contracts']} contracts</td>
        </tr>"""
        if rec.get("earnings_warning"):
            rows += f'<tr><td colspan="5" style="background:#fef9c3">{rec["earnings_warning"]}</td></tr>'
        if rec.get("low_gain"):
            rows += '<tr><td colspan="5" style="background:#fee2e2">Best available &mdash; below $0.10/share threshold</td></tr>'

    return f"""<!DOCTYPE html><html><body style="font-family:Arial,sans-serif">
    <h2>Collar Recommendations &mdash; {today}</h2>
    <p>{n} recommendation(s) &middot; {meta.get('symbols_with_collars', 0)} symbol(s)</p>
    <table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;width:100%">
    <tr style="background:#1e3a5f;color:white">
      <th>Symbol</th><th>Expiration</th><th>Strikes</th><th>Net Gain</th><th>Contracts</th>
    </tr>
    {rows}
    </table>
    <p style="font-size:11px;color:#888">Automated analysis &mdash; not financial advice.</p>
    </body></html>"""


def _render_collar_text(recommendations: List[dict], meta: dict) -> str:
    today = meta.get("run_date", str(date.today()))
    lines = [f"COLLAR RECOMMENDATIONS — {today}", "=" * 60]
    for rec in recommendations:
        low = " [BELOW THRESHOLD]" if rec.get("low_gain") else ""
        lines.append(
            f"\n{rec['symbol']} ({rec['name']}){low}\n"
            f"  Exp: {rec['expiration']} ({rec['dte']}d)  |  "
            f"CC Strike: ${rec['call_leg']['strike']}  |  LP Strike: ${rec['put_leg']['strike']}\n"
            f"  Net: ${rec['net_gain_per_share']:.2f}/share  |  "
            f"Total: ${rec['net_gain_total']:.0f}  |  "
            f"Cap: +{rec['upside_cap_pct']:.1f}%  |  Floor: {rec['downside_floor_pct']:.1f}%"
        )
        if rec.get("earnings_warning"):
            lines.append(f"  {rec['earnings_warning']}")
    lines.append("\nAutomated analysis — not financial advice.")
    return "\n".join(lines)


def send_collar_report(
    recommendations: List[dict],
    collar_meta: dict,
    dry_run: bool = False,
) -> bool:
    """
    Send the weekly collar report via SendGrid.

    Args:
        recommendations: output from run_collar_pipeline() + earnings annotations
        collar_meta:     run context dict (run_date, eligible_holdings, etc.)
        dry_run:         if True, saves HTML preview but does not send

    Returns:
        True on success (or dry_run), False on send failure.
    """
    api_key   = os.getenv("SENDGRID_API_KEY", "").strip()
    sender    = os.getenv("SENDGRID_SENDER", "").strip()
    recipient = collar_meta.get("recipient_email", "")

    if not recipient:
        logger.error("recipient_email not set in config.yaml")
        return False

    run_date  = collar_meta.get("run_date", str(date.today()))
    n         = len(recommendations)
    low_count = sum(1 for r in recommendations if r.get("low_gain"))
    flags     = sum(1 for r in recommendations if r.get("earnings_date"))

    subject = f"Collar Report — {run_date} — {n} rec(s)"
    if low_count:
        subject += f" | {low_count} below threshold"
    if flags:
        subject += f" | {flags} earnings flag(s)"

    html_body = _render_collar_html(recommendations, collar_meta)
    text_body = _render_collar_text(recommendations, collar_meta)

    if dry_run:
        logger.info(f"[DRY RUN] Collar email would be sent to {recipient}")
        logger.info(f"  Subject: {subject}")
        logger.info(f"  {n} recs, {low_count} below threshold, {flags} earnings flags")
        preview_path = BASE_DIR / "logs" / f"collar_preview_{run_date}.html"
        preview_path.parent.mkdir(exist_ok=True)
        preview_path.write_text(html_body)
        logger.info(f"  HTML preview saved: {preview_path}")
        return True

    if not api_key or not sender:
        logger.error("SENDGRID_API_KEY or SENDGRID_SENDER missing from .env")
        return False

    try:
        message = Mail(
            from_email=sender,
            to_emails=recipient,
            subject=subject,
            html_content=html_body,
            plain_text_content=text_body,
        )
        sg = SendGridAPIClient(api_key)
        response = sg.send(message)
        if response.status_code in (200, 201, 202):
            logger.info(f"Collar email sent to {recipient} (HTTP {response.status_code})")
            return True
        else:
            logger.error(f"SendGrid error: HTTP {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Collar email send failed: {e}", exc_info=True)
        return False
