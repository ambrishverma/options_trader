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

Verification-workflow handling
-------------------------------
When the cached session expires, Robinhood's new Sheriff/Pathfinder security
system may issue a "prompt" challenge — the user receives a push notification
on their Robinhood app and must tap Approve.  After they do, robin_stocks
reattempts the login with the *original* (now stale) TOTP code, which
Robinhood rejects.  The net result: rh.login() returns "successfully" but
the session is inactive (LOGGED_IN is still False).

Our login() handles this by:
  1. Calling rh.login() and then checking LOGGED_IN directly.
  2. On silent failure, deleting the stale pickle before retrying — this
     forces a fresh authentication without reloading the expired token,
     preventing another verification workflow from being triggered (the
     device was just verified, so Robinhood won't re-challenge it).
  3. Waiting _RETRY_SLEEP_SECS (45 s) between attempts so the TOTP code
     rotates (30 s window) and Robinhood's push-status endpoint cools down.
  4. On a 429 Too Many Requests error, waiting _RATE_LIMIT_SLEEP_SECS (90 s)
     before retrying — the push-prompts endpoint has a strict rate limit and
     firing a second attempt within seconds reliably produces a 429.
  5. Treating a NoneType / subscript error from robin_stocks (malformed push
     response) as a retry-able soft failure rather than a hard crash.
  6. Retrying up to MAX_LOGIN_ATTEMPTS times with a fresh TOTP each time.
"""

import os
import pickle
import logging
import time
import pyotp
import robin_stocks.robinhood as rh
import robin_stocks.robinhood.helper as _rh_helper
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_PICKLE_PATH = Path.home() / ".tokens" / "robinhood.pickle"
MAX_LOGIN_ATTEMPTS = 3
_RETRY_SLEEP_SECS = 45       # wait between silent-failure retries; ensures TOTP rotates
_RATE_LIMIT_SLEEP_SECS = 90  # longer wait after a 429 Too Many Requests


def get_totp_code() -> str:
    """Generate current 6-digit TOTP code from stored seed."""
    seed = os.getenv("ROBINHOOD_TOTP_SEED", "").strip()
    if not seed:
        raise ValueError("ROBINHOOD_TOTP_SEED is missing from .env")
    # pyotp accepts base32 seeds; strips spaces if user copied with spaces
    return pyotp.TOTP(seed.replace(" ", "")).now()


def _clear_stale_pickle() -> None:
    """Delete the cached session pickle.

    Called when rh.login() returns silently without activating the session
    (LOGGED_IN is False).  The pickle contains the expired access_token that
    causes the token-validity check to fail and triggers a new verification
    workflow on every retry.  Removing it ensures the next attempt performs
    a clean authentication against Robinhood's API.

    The device_token embedded in the pickle is tied to the robin_stocks
    device UUID that was accepted by Robinhood during the verification
    workflow.  Deleting the pickle means robin_stocks generates a fresh
    random device_token for the retry — but since the device was just
    verified in this same session, Robinhood recognises the account and
    does not issue a second challenge.
    """
    if _PICKLE_PATH.exists():
        try:
            _PICKLE_PATH.unlink()
            logger.info("  Cleared stale session pickle for clean retry.")
        except OSError as e:
            logger.warning(f"  Could not remove pickle: {e}")


def _classify_login_exception(e: Exception) -> str:
    """
    Return a short tag classifying a login exception for retry logic.

    Tags:
      "rate_limit"  — 429 Too Many Requests on Robinhood push-status endpoint
      "none_type"   — NoneType / subscript error from a malformed push response
      "other"       — anything else (network error, bad credentials, …)
    """
    err = str(e)
    if "429" in err or "Too Many Requests" in err:
        return "rate_limit"
    if ("NoneType" in err and "subscriptable" in err) or "'NoneType'" in err:
        return "none_type"
    return "other"


def login(force_fresh: bool = False) -> bool:
    """
    Log in to Robinhood using TOTP.

    Retries up to MAX_LOGIN_ATTEMPTS times.  Between each retry the function
    sleeps long enough for the TOTP code to rotate (codes are valid for ~30 s)
    and for Robinhood's push-status endpoint rate limit to reset.

    Specific error handling:
      - 429 Too Many Requests: wait _RATE_LIMIT_SLEEP_SECS (90 s) before retry
      - NoneType / subscript error: soft-failure; wait _RETRY_SLEEP_SECS (45 s)
      - Silent failure (LOGGED_IN=False after rh.login returns): wait
        _RETRY_SLEEP_SECS (45 s) so prior push session cools down

    Args:
        force_fresh: If True, delete any cached pickle before the first attempt.

    Returns:
        True on success, raises RuntimeError on all-attempts failure.
    """
    username = os.getenv("ROBINHOOD_USERNAME", "").strip()
    password = os.getenv("ROBINHOOD_PASSWORD", "").strip()

    if not username or not password:
        raise ValueError("ROBINHOOD_USERNAME or ROBINHOOD_PASSWORD missing from .env")

    if force_fresh:
        _clear_stale_pickle()

    for attempt in range(1, MAX_LOGIN_ATTEMPTS + 1):
        totp_code = get_totp_code()   # fresh code each attempt
        logger.info(f"Logging in as {username} (TOTP: {totp_code}"
                    + (f", attempt {attempt}/{MAX_LOGIN_ATTEMPTS}" if attempt > 1 else "") + ")")

        try:
            rh.login(
                username=username,
                password=password,
                mfa_code=totp_code,
                store_session=True,       # cache token to ~/.tokens/robinhood.pickle
                expiresIn=86400,          # 24h token TTL
            )
        except Exception as e:
            tag = _classify_login_exception(e)

            if attempt >= MAX_LOGIN_ATTEMPTS:
                logger.error(f"❌  Robinhood login failed after {MAX_LOGIN_ATTEMPTS} attempts: {e}")
                raise

            # Decide sleep duration based on error type
            if tag == "rate_limit":
                logger.warning(
                    f"  Rate-limited (429) on attempt {attempt} — "
                    f"waiting {_RATE_LIMIT_SLEEP_SECS}s before retry..."
                )
                _clear_stale_pickle()
                time.sleep(_RATE_LIMIT_SLEEP_SECS)
            elif tag == "none_type":
                # robin_stocks got a malformed/empty push-status response — treat
                # as transient; the prior push session may still be settling.
                logger.warning(
                    f"  Push verification returned empty response on attempt {attempt} "
                    f"— waiting {_RETRY_SLEEP_SECS}s before retry..."
                )
                _clear_stale_pickle()
                time.sleep(_RETRY_SLEEP_SECS)
            else:
                logger.warning(
                    f"  Login exception on attempt {attempt}: {e} "
                    f"— waiting {_RETRY_SLEEP_SECS}s before retry..."
                )
                _clear_stale_pickle()
                time.sleep(_RETRY_SLEEP_SECS)
            continue

        # rh.login() can return without raising even when the session is not active
        # (silent failure after Sheriff/verification workflow with stale TOTP reattempt).
        # Check the module-level flag directly.
        if _rh_helper.LOGGED_IN:
            logger.info("✅  Robinhood login successful")
            return True

        # Silent failure — session not active despite "successful" return.
        # The device-verification push just fired; retrying immediately would
        # either reuse the same (still-valid) TOTP code or hit the 429 rate
        # limit on the push-status endpoint.  Wait long enough for both to clear.
        logger.warning(
            f"  Login attempt {attempt} returned without activating session "
            f"(LOGGED_IN=False). Device-verification may be pending or the "
            f"reattempt used a stale TOTP."
        )
        if attempt < MAX_LOGIN_ATTEMPTS:
            logger.info(
                f"  Clearing stale pickle and waiting {_RETRY_SLEEP_SECS}s "
                f"before retry with fresh TOTP..."
            )
            _clear_stale_pickle()
            time.sleep(_RETRY_SLEEP_SECS)
        else:
            raise RuntimeError(
                f"Robinhood login failed after {MAX_LOGIN_ATTEMPTS} attempts: "
                "session never activated. Check Robinhood app for pending approval."
            )

    raise RuntimeError("Robinhood login failed")   # unreachable, satisfies type checkers


def logout():
    """Gracefully log out and clear in-memory session (pickle is preserved)."""
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
