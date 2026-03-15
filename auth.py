"""
auth.py — Robinhood TOTP Authentication
========================================
Uses pyotp to generate 6-digit TOTP codes from a stored base32 seed,
enabling fully unattended Robinhood login without SMS interaction.

Environment variables required (loaded from .env):
  ROBINHOOD_USERNAME   — Robinhood account email
  ROBINHOOD_PASSWORD   — Robinhood account password
  ROBINHOOD_TOTP_SEED  — Base32 TOTP seed from Robinhood Authenticator App setup

Session caching: robin_stocks stores a pickle session at
~/.tokens/robinhood.pickle after first login, so subsequent logins
within the token TTL (~24h) skip the TOTP step entirely.
"""

import os
import logging
import pyotp
import robin_stocks.robinhood as rh
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_totp_code() -> str:
    """Generate current 6-digit TOTP code from stored seed."""
    seed = os.getenv("ROBINHOOD_TOTP_SEED", "").strip()
    if not seed:
        raise ValueError("ROBINHOOD_TOTP_SEED is missing from .env")
    # pyotp accepts base32 seeds; strips spaces if user copied with spaces
    return pyotp.TOTP(seed.replace(" ", "")).now()


def login(force_fresh: bool = False) -> bool:
    """
    Log in to Robinhood using TOTP.

    Args:
        force_fresh: If True, bypass cached session and re-authenticate.

    Returns:
        True on success, raises on failure.
    """
    username = os.getenv("ROBINHOOD_USERNAME", "").strip()
    password = os.getenv("ROBINHOOD_PASSWORD", "").strip()

    if not username or not password:
        raise ValueError("ROBINHOOD_USERNAME or ROBINHOOD_PASSWORD missing from .env")

    totp_code = get_totp_code()
    logger.info(f"Logging in as {username} (TOTP: {totp_code})")

    try:
        rh.login(
            username=username,
            password=password,
            mfa_code=totp_code,
            store_session=True,       # cache token to ~/.tokens/robinhood.pickle
            expiresIn=86400,          # 24h token TTL
        )
        logger.info("✅  Robinhood login successful")
        return True

    except Exception as e:
        logger.error(f"❌  Robinhood login failed: {e}")
        raise


def logout():
    """Gracefully log out and clear session."""
    try:
        rh.logout()
        logger.info("Robinhood session closed.")
    except Exception as e:
        logger.warning(f"Logout warning (non-fatal): {e}")


def validate_totp_seed(seed: str) -> bool:
    """
    Validate that a TOTP seed is well-formed and generates codes.
    Used during --setup wizard.
    """
    try:
        seed_clean = seed.strip().replace(" ", "")
        code = pyotp.TOTP(seed_clean).now()
        # code must be a 6-digit numeric string
        return code.isdigit() and len(code) == 6
    except Exception:
        return False


def validate_credentials(username: str, password: str, seed: str) -> dict:
    """
    Live-validate Robinhood credentials during --setup.
    Returns {"ok": bool, "error": str | None}.
    """
    if not validate_totp_seed(seed):
        return {"ok": False, "error": "TOTP seed is invalid — must be a base32 string."}

    # Temporarily set env vars for this test
    os.environ["ROBINHOOD_USERNAME"] = username
    os.environ["ROBINHOOD_PASSWORD"] = password
    os.environ["ROBINHOOD_TOTP_SEED"] = seed

    try:
        login(force_fresh=True)
        logout()
        return {"ok": True, "error": None}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        # Clear from environment — will be written to .env by wizard
        for key in ("ROBINHOOD_USERNAME", "ROBINHOOD_PASSWORD", "ROBINHOOD_TOTP_SEED"):
            os.environ.pop(key, None)
