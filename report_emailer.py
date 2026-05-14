"""
report_emailer.py — Options Trade Report Email Delivery
=========================================================
Renders and sends the daily options trade report email via SendGrid.
Mirrors collar_emailer.py structure but for the trade report.
"""

import os
import logging
from datetime import date
from pathlib import Path

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_DIR      = Path(__file__).parent
TEMPLATE_PATH = BASE_DIR / "templates" / "report_email.html"


def _render_report_html(report: dict) -> str:
    """Render report email HTML via Jinja2 template."""
    try:
        from jinja2 import Environment, FileSystemLoader, select_autoescape
        env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_PATH.parent)),
            autoescape=select_autoescape(["html"]),
        )
        template = env.get_template(TEMPLATE_PATH.name)
        return template.render(report=report)
    except Exception as e:
        logger.warning(f"Report template render failed ({e}) — using fallback")
        return _render_report_fallback(report)


def _render_report_fallback(report: dict) -> str:
    """Minimal plain-HTML fallback when template is unavailable."""
    start = report.get("start_date", "")
    end   = report.get("end_date", "")
    date_label = start if start == end else f"{start} – {end}"
    credit = report.get("total_credit", 0)
    debit  = report.get("total_debit", 0)
    net    = report.get("net_gain", 0)
    orders = report.get("orders", [])

    net_color  = "#16a34a" if net >= 0 else "#dc2626"
    net_prefix = "+" if net >= 0 else "-"
    net_term   = "Net Gain" if net >= 0 else "Net Loss"

    rows_html = ""
    for o in orders:
        dir_color = "#16a34a" if o["direction"] == "credit" else "#dc2626"
        dir_label = "CREDIT" if o["direction"] == "credit" else "DEBIT"
        rows_html += f"""
        <tr>
          <td style="padding:6px 10px">{o['date']}</td>
          <td style="padding:6px 10px"><b>{o['symbol']}</b></td>
          <td style="padding:6px 10px">{o['type']}</td>
          <td style="padding:6px 10px">{o['side'].upper()}</td>
          <td style="padding:6px 10px">${o['strike']:.2f}</td>
          <td style="padding:6px 10px">{o['expiration']}</td>
          <td style="padding:6px 10px">{o['quantity']}</td>
          <td style="padding:6px 10px">${o['price']:.2f}</td>
          <td style="padding:6px 10px; color:{dir_color}; font-weight:600">
            ${o['premium']:.2f} {dir_label}
          </td>
        </tr>"""

    no_orders_row = ""
    if not orders:
        no_orders_row = """
        <tr>
          <td colspan="9" style="padding:16px; text-align:center; color:#6b7280">
            No filled options orders in this period.
          </td>
        </tr>"""

    return f"""
    <html><body style="font-family:sans-serif;color:#111">
      <h2 style="color:#1e293b">📋 Options Trade Report — {date_label}</h2>
      <table style="border-collapse:collapse;margin-bottom:16px;background:#f8fafc;padding:12px;border-radius:8px">
        <tr>
          <td style="padding:8px 20px;text-align:center">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase">Total Credit</div>
            <div style="font-size:22px;font-weight:700;color:#16a34a">${credit:,.2f}</div>
          </td>
          <td style="padding:8px 20px;text-align:center">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase">Total Debit</div>
            <div style="font-size:22px;font-weight:700;color:#dc2626">${debit:,.2f}</div>
          </td>
          <td style="padding:8px 20px;text-align:center">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase">{net_term}</div>
            <div style="font-size:22px;font-weight:700;color:{net_color}">{net_prefix}${abs(net):,.2f}</div>
          </td>
          <td style="padding:8px 20px;text-align:center">
            <div style="font-size:11px;color:#64748b;text-transform:uppercase">Orders</div>
            <div style="font-size:22px;font-weight:700">{len(orders)}</div>
          </td>
        </tr>
      </table>
      <table style="border-collapse:collapse;width:100%;font-size:13px">
        <thead>
          <tr style="background:#1e293b;color:white">
            <th style="padding:8px 10px;text-align:left">Date</th>
            <th style="padding:8px 10px;text-align:left">Symbol</th>
            <th style="padding:8px 10px;text-align:left">Type</th>
            <th style="padding:8px 10px;text-align:left">Side</th>
            <th style="padding:8px 10px;text-align:right">Strike</th>
            <th style="padding:8px 10px;text-align:left">Expiration</th>
            <th style="padding:8px 10px;text-align:right">Qty</th>
            <th style="padding:8px 10px;text-align:right">Price/sh</th>
            <th style="padding:8px 10px;text-align:right">Premium</th>
          </tr>
        </thead>
        <tbody>
          {rows_html or no_orders_row}
        </tbody>
      </table>
      <p style="color:#94a3b8;font-size:11px;margin-top:20px">
        Options Trader — auto-generated report. Not financial advice.
      </p>
    </body></html>
    """


def send_options_report_email(
    report: dict,
    recipient_email: str,
    dry_run: bool = False,
) -> bool:
    """
    Render and send the options trade report email.

    Parameters
    ----------
    report : dict
        Output of reporter.build_options_report().
    recipient_email : str
        Destination email address.
    dry_run : bool
        If True, save HTML locally and skip sending.

    Returns
    -------
    bool — True on success, False on failure.
    """
    api_key  = os.getenv("SENDGRID_API_KEY", "").strip()
    sender   = os.getenv("SENDGRID_SENDER", "").strip()
    recipient = recipient_email

    if not recipient:
        logger.error("recipient_email not set in config.yaml")
        return False

    start = report.get("start_date", "")
    end   = report.get("end_date", "")
    date_label = start if start == end else f"{start}–{end}"
    n_orders = report.get("order_count", 0)
    net      = report.get("net_gain", 0)
    if net >= 0:
        net_label = f"Net Gain +${abs(net):,.2f}"
    else:
        net_label = f"Net Loss -${abs(net):,.2f}"

    subject = f"📋 Options Report — {date_label} — {n_orders} order(s) | {net_label}"

    html_body = _render_report_html(report)

    if dry_run:
        today_str = str(date.today())
        preview_path = BASE_DIR / "logs" / f"report_preview_{today_str}.html"
        preview_path.parent.mkdir(exist_ok=True)
        preview_path.write_text(html_body)
        logger.info(f"[DRY RUN] Report email would be sent to {recipient}")
        logger.info(f"  Subject: {subject}")
        logger.info(f"  HTML preview saved: {preview_path}")
        return True

    if not api_key or not sender:
        logger.error("SENDGRID_API_KEY or SENDGRID_SENDER missing from .env")
        return False

    import time as _time

    message = Mail(
        from_email=sender,
        to_emails=recipient,
        subject=subject,
        html_content=html_body,
    )
    sg = SendGridAPIClient(api_key)

    for attempt in range(1, 4):   # up to 3 attempts
        try:
            response = sg.send(message)
            if response.status_code in (200, 201, 202):
                logger.info(f"✅  Report email sent to {recipient} (HTTP {response.status_code})")
                return True
            else:
                logger.error(
                    f"SendGrid error (attempt {attempt}/3): "
                    f"HTTP {response.status_code} — {response.body}"
                )
        except Exception as e:
            logger.warning(f"Report email send failed (attempt {attempt}/3): {e}")
        if attempt < 3:
            _time.sleep(30)

    logger.error("Report email send failed after 3 attempts")
    return False
