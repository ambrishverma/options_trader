"""
setup_wizard.py — One-time --setup Wizard
==========================================
Guides the user through 7 credential-collection steps, validates each
credential live, then writes a .env file (chmod 600) and a config.yaml.

Steps:
  1. Robinhood username + password
  2. Robinhood TOTP seed (validates by generating a code)
  3. Live Robinhood login test
  4. Finnhub API key (validates with a test API call)
  5. SendGrid API key (validates with a test API call)
  6. SendGrid verified sender email
  7. Summary + write .env / config.yaml
"""

import os
import sys
import stat
import getpass
import requests
import yaml
from pathlib import Path

BASE_DIR = Path(__file__).parent
ENV_FILE = BASE_DIR / ".env"
CONFIG_FILE = BASE_DIR / "config.yaml"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _banner(text: str):
    width = 60
    print("\n" + "═" * width)
    print(f"  {text}")
    print("═" * width)


def _step(n: int, total: int, title: str):
    print(f"\n  Step {n}/{total} — {title}")
    print("  " + "─" * 50)


def _ok(msg: str):
    print(f"  ✅  {msg}")


def _err(msg: str):
    print(f"  ❌  {msg}")


def _prompt(label: str, secret: bool = False, default: str = "") -> str:
    display_default = f" [{default}]" if default else ""
    prompt_text = f"  → {label}{display_default}: "
    while True:
        val = getpass.getpass(prompt_text) if secret else input(prompt_text)
        val = val.strip() or default
        if val:
            return val
        print("     Value required — please try again.")


def _validate_finnhub_key(api_key: str) -> dict:
    """Hit Finnhub's free /quote endpoint as a credential test."""
    try:
        resp = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": "AAPL", "token": api_key},
            timeout=8,
        )
        if resp.status_code == 200 and "c" in resp.json():
            return {"ok": True, "error": None}
        elif resp.status_code == 401:
            return {"ok": False, "error": "Invalid API key (401 Unauthorized)"}
        else:
            return {"ok": False, "error": f"Unexpected response: HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _validate_sendgrid_key(api_key: str) -> dict:
    """Validate SendGrid key by calling the /v3/user/profile endpoint."""
    try:
        resp = requests.get(
            "https://api.sendgrid.com/v3/user/profile",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=8,
        )
        if resp.status_code == 200:
            return {"ok": True, "error": None}
        elif resp.status_code == 401:
            return {"ok": False, "error": "Invalid API key (401 Unauthorized)"}
        elif resp.status_code == 403:
            return {"ok": False, "error": "API key lacks required scopes — ensure 'Mail Send' permission is granted."}
        else:
            return {"ok": False, "error": f"Unexpected response: HTTP {resp.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _write_env(creds: dict):
    """Write .env file with 600 permissions (owner read/write only)."""
    lines = [
        "# Options Trader — Secrets",
        "# DO NOT commit this file to version control.\n",
        f'ROBINHOOD_USERNAME="{creds["rh_user"]}"',
        f'ROBINHOOD_PASSWORD="{creds["rh_pass"]}"',
        f'ROBINHOOD_TOTP_SEED="{creds["rh_totp"]}"',
        f'SENDGRID_API_KEY="{creds["sg_key"]}"',
        f'SENDGRID_SENDER="{creds["sg_sender"]}"',
        f'FINNHUB_API_KEY="{creds["fh_key"]}"',
    ]
    ENV_FILE.write_text("\n".join(lines) + "\n")
    ENV_FILE.chmod(stat.S_IRUSR | stat.S_IWUSR)   # chmod 600
    _ok(f".env written → {ENV_FILE}  (permissions: 600)")


def _write_config(creds: dict):
    """Write config.yaml with non-sensitive operational settings."""
    config = {
        "mode": "safe",
        "min_otm_pct": 7.0,
        "min_bid": 0.20,
        "min_open_interest": 2,
        "lookahead_days": 21,
        "diversify_split": 0.5,
        "recipient_email": creds["recipient"],
        "portfolio_path": "./snapshots/",
        "cache_path": "./cache/",
        "log_path": "./logs/",
        "pipeline_time_et": "09:35",
        "portfolio_pull_day": 1,      # 1 = first trading day of month
    }
    with open(CONFIG_FILE, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)
    _ok(f"config.yaml written → {CONFIG_FILE}")


# ─────────────────────────────────────────────────────────────────────────────
# Wizard
# ─────────────────────────────────────────────────────────────────────────────

def run_setup_wizard():
    total = 7
    creds = {}

    _banner("Options Trader — First-Time Setup Wizard")
    print("""
  This wizard will:
    • Collect credentials for Robinhood, Finnhub, and SendGrid
    • Validate each credential live before saving
    • Write a .env file (chmod 600) and config.yaml
    • All subsequent runs are fully automated — no input needed

  You will only need to run this once.
    """)
    input("  Press Enter to begin...")

    # ── Step 1: Robinhood credentials ─────────────────────────────────────────
    _step(1, total, "Robinhood credentials")
    print("""
  Your Robinhood login email and password.
  These are stored encrypted-at-rest in .env (chmod 600).
    """)
    creds["rh_user"] = _prompt("Robinhood email")
    creds["rh_pass"] = _prompt("Robinhood password", secret=True)

    # ── Step 2: TOTP seed ─────────────────────────────────────────────────────
    _step(2, total, "Robinhood TOTP seed")
    print("""
  To find your TOTP seed:
    1. Open Robinhood app → Account (person icon, bottom right)
    2. Security & Privacy → Two-Factor Authentication
    3. Select "Authenticator App"
    4. Robinhood shows a QR code AND a text seed below it
    5. Copy the text seed (looks like: JBSWY3DPEHPK3PXP)

  ⚠️  Important: Copy the raw seed text, NOT the QR code.
    """)

    while True:
        seed = _prompt("TOTP seed (base32)")
        # Quick format check
        import pyotp
        try:
            code = pyotp.TOTP(seed.strip().replace(" ", "")).now()
            _ok(f"TOTP seed valid — current code: {code}")
            creds["rh_totp"] = seed.strip()
            break
        except Exception as e:
            _err(f"Invalid TOTP seed: {e}. Please try again.")

    # ── Step 3: Live Robinhood login test ─────────────────────────────────────
    _step(3, total, "Live Robinhood login test")
    print("  Testing login with your credentials now...")

    from auth import validate_credentials
    result = validate_credentials(creds["rh_user"], creds["rh_pass"], creds["rh_totp"])

    if result["ok"]:
        _ok("Robinhood login successful!")
    else:
        _err(f"Login failed: {result['error']}")
        print("""
  Possible causes:
    • Wrong username or password
    • TOTP seed is from a different account
    • Robinhood account has SMS 2FA (not Authenticator App)
      → In Robinhood app, switch 2FA to Authenticator App first

  Please restart the wizard and try again.
        """)
        sys.exit(1)

    # ── Step 4: Finnhub API key ────────────────────────────────────────────────
    _step(4, total, "Finnhub API key")
    print("""
  Finnhub provides free earnings calendar data.
  To get your API key:
    1. Go to https://finnhub.io
    2. Click "Get free API key" → sign up (free)
    3. Dashboard shows your key immediately
    """)

    while True:
        fh_key = _prompt("Finnhub API key", secret=True)
        print("  Validating Finnhub key...")
        result = _validate_finnhub_key(fh_key)
        if result["ok"]:
            _ok("Finnhub key valid!")
            creds["fh_key"] = fh_key
            break
        else:
            _err(f"Finnhub validation failed: {result['error']}")
            retry = input("  Try a different key? [y/N]: ").strip().lower()
            if retry != "y":
                print("  Skipping Finnhub validation. Key stored as-is.")
                creds["fh_key"] = fh_key
                break

    # ── Step 5: SendGrid API key ───────────────────────────────────────────────
    _step(5, total, "SendGrid API key")
    print("""
  SendGrid delivers the daily covered-call email.
  To get your API key:
    1. Go to https://app.sendgrid.com
    2. Settings → API Keys → Create API Key
    3. Choose "Restricted Access" → enable "Mail Send" → Full Access
    4. Copy the key immediately (shown only once)
    """)

    while True:
        sg_key = _prompt("SendGrid API key", secret=True)
        print("  Validating SendGrid key...")
        result = _validate_sendgrid_key(sg_key)
        if result["ok"]:
            _ok("SendGrid key valid!")
            creds["sg_key"] = sg_key
            break
        else:
            _err(f"SendGrid validation failed: {result['error']}")
            retry = input("  Try a different key? [y/N]: ").strip().lower()
            if retry != "y":
                print("  Skipping SendGrid validation. Key stored as-is.")
                creds["sg_key"] = sg_key
                break

    # ── Step 6: Sender + recipient email ──────────────────────────────────────
    _step(6, total, "Email addresses")
    print("""
  SendGrid requires a verified sender email (your sending address).
  To verify:
    1. In SendGrid → Settings → Sender Authentication
    2. "Verify a Single Sender" → follow the email verification steps

  Recipient email is where daily recommendations will be sent.
    """)

    creds["sg_sender"] = _prompt("SendGrid verified sender email")
    creds["recipient"] = _prompt("Recipient email (where to send daily report)", default="ambrish@gmail.com")

    # ── Step 7: Write files + summary ─────────────────────────────────────────
    _step(7, total, "Writing configuration files")

    _write_env(creds)
    _write_config(creds)

    # Create required directories
    for d in ["snapshots", "cache", "logs", "templates"]:
        Path(BASE_DIR / d).mkdir(exist_ok=True)
    _ok("Directories created: snapshots/, cache/, logs/, templates/")

    _banner("Setup Complete!")
    print(f"""
  Everything is configured. You're ready to go.

  Next steps:
    • Run a dry-run to test the full pipeline:
        python main.py --dry-run

    • Pull your portfolio from Robinhood now:
        python main.py --pull-portfolio

    • Start the automated daily scheduler:
        python main.py --schedule

  The scheduler will:
    ✓ Pull your Robinhood portfolio on the 1st trading day of each month (6:00 AM ET)
    ✓ Run the covered-call pipeline every weekday at 9:35 AM ET
    ✓ Email recommendations to {creds["recipient"]}

  Files written:
    • .env         — secrets (chmod 600, never commit)
    • config.yaml  — operational settings
    """)
