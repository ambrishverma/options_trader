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
When the cached session expires, Robinhood's Sheriff/Pathfinder security
system issues a "prompt" challenge — the user receives a push notification
on their Robinhood app and must tap Approve.  robin_stocks then polls
get_prompts_status to detect the approval.

The get_prompts_status endpoint has a strict rate limit.  If multiple
login attempts fire in quick succession (or a prior session already
polled), the endpoint returns 429 Too Many Requests.  robin_stocks
then gets a NoneType error parsing the empty response and reports
"Login failed" — even though the user already approved on their phone.

Our login() handles this by:
  1. Calling rh.login() and checking LOGGED_IN directly.
  2. On ANY failure (429, NoneType, silent LOGGED_IN=False), clearing the
     stale pickle so the next attempt starts a clean session.  The old
     pickle's expired token is what triggers the verification flow; without
     it, the fresh login often skips verification entirely.
  3. Using progressive backoff (_RATE_LIMIT_SLEEP_SECS + 30s per attempt)
     so the rate limit on get_prompts_status fully resets.
  4. Retrying up to MAX_LOGIN_ATTEMPTS (5) times with a fresh TOTP each time.
"""

import os
import functools
import logging
import sys
import time
import pyotp
import robin_stocks.robinhood as rh
import robin_stocks.robinhood.authentication as _rh_auth
import robin_stocks.robinhood.helper as _rh_helper
from robin_stocks.robinhood.helper import request_get, request_post
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


# Side-channel flag: set inside the monkey-patch when SMS/email verification is
# required but no TTY is available.  login() checks this after rh.login() returns
# because the upstream `except Exception` swallows any exception we raise.
_INTERACTIVE_AUTH_NEEDED = False

# Reference-counted session: allows a single RH session to be shared across
# nested login/logout pairs.  The first login() authenticates; subsequent calls
# just increment the counter.  logout() only disconnects when the counter
# reaches zero (i.e. the outermost caller logs out).
_session_refcount = 0

# ---------------------------------------------------------------------------
# Monkey-patch robin_stocks' _validate_sherrif_id
# ---------------------------------------------------------------------------
# The upstream function has an unguarded `prompt_challenge_status["challenge_status"]`
# that crashes with TypeError when get_prompts_status returns None (429 rate limit).
# This patched version adds None-safe polling with backoff so the verification
# can survive transient 429s and actually read the approval.
# ---------------------------------------------------------------------------
def _patched_validate_sherrif_id(device_token: str, workflow_id: str):
    logger.info("Starting verification process (patched)...")
    pathfinder_url = "https://api.robinhood.com/pathfinder/user_machine/"
    machine_payload = {
        "device_id": device_token,
        "flow": "suv",
        "input": {"workflow_id": workflow_id},
    }
    machine_data = request_post(url=pathfinder_url, payload=machine_payload, json=True)
    if not machine_data:
        logger.warning("  Pathfinder POST returned None (rate-limited?), skipping verification.")
        return
    machine_id = _rh_auth._get_sherrif_id(machine_data)
    inquiries_url = f"https://api.robinhood.com/pathfinder/inquiries/{machine_id}/user_view/"

    start_time = time.time()

    while time.time() - start_time < 180:
        time.sleep(5)
        inquiries_response = request_get(inquiries_url)
        if not inquiries_response:
            logger.warning("  No response from inquiries endpoint, retrying...")
            continue

        ctx = inquiries_response.get("context", {})
        challenge = ctx.get("sheriff_challenge")
        if not challenge:
            continue

        challenge_type = challenge.get("type")
        challenge_status = challenge.get("status")
        challenge_id = challenge.get("id")

        if challenge_type == "prompt":
            logger.info("  Waiting for device approval in Robinhood app...")
            prompt_url = f"https://api.robinhood.com/push/{challenge_id}/get_prompts_status/"
            poll_start = time.time()
            consecutive_429s = 0

            def _poll_backoff():
                return min(10 + consecutive_429s * 10, 60)

            while time.time() - poll_start < 180:
                time.sleep(_poll_backoff())
                try:
                    status = request_get(url=prompt_url)
                except Exception as poll_err:
                    status = None
                    logger.warning(f"  get_prompts_status error: {poll_err}")
                if status and status.get("challenge_status") == "validated":
                    logger.info("  Device approval confirmed via get_prompts_status")
                    break
                if status is None:
                    consecutive_429s += 1
                    logger.warning(f"  get_prompts_status returned None (likely 429) "
                                   f"— next backoff {_poll_backoff()}s (attempt {consecutive_429s})")
                else:
                    consecutive_429s = 0
            break

        if challenge_status == "validated":
            logger.info("  Verification successful!")
            break

        if challenge_type in ("sms", "email") and challenge_status == "issued":
            if not sys.stdin.isatty():
                global _INTERACTIVE_AUTH_NEEDED
                _INTERACTIVE_AUTH_NEEDED = True
                logger.warning(
                    f"  Robinhood requires {challenge_type} verification but no TTY is attached."
                )
                return
            user_code = input(f"Enter the {challenge_type} verification code: ")
            challenge_url = f"https://api.robinhood.com/challenge/{challenge_id}/respond/"
            resp = request_post(url=challenge_url, payload={"response": user_code})
            if resp and resp.get("status") == "validated":
                break

    # Poll workflow status to confirm final approval (own 60s budget so it
    # always runs even if the prompt-poll consumed the original start_time window).
    workflow_deadline = time.time() + 60
    retry_attempts = 5
    while time.time() < workflow_deadline:
        try:
            payload = {"sequence": 0, "user_input": {"status": "continue"}}
            resp = request_post(url=inquiries_url, payload=payload, json=True)
            if not resp:
                time.sleep(5)
                retry_attempts -= 1
                if retry_attempts <= 0:
                    break
                continue
            approved = (
                resp.get("type_context", {}).get("result") == "workflow_status_approved"
                or resp.get("verification_workflow", {}).get("workflow_status") == "workflow_status_approved"
            )
            if approved:
                logger.info("  Workflow approved!")
                return
            time.sleep(5)
        except Exception as e:
            time.sleep(5)
            logger.warning(f"  Workflow status poll error: {e}")
            retry_attempts -= 1
            if retry_attempts <= 0:
                break

    logger.warning("  Verification timeout — assuming approved and proceeding.")


_rh_auth._validate_sherrif_id = _patched_validate_sherrif_id


# ---------------------------------------------------------------------------
# Default socket timeouts on the robin_stocks session
# ---------------------------------------------------------------------------
# robin_stocks routes every call through a module-level requests.Session and
# never passes a timeout, so a read blocks forever.  That turned two ordinary
# network interruptions into multi-hour outages: the laptop slept mid-request,
# the TCP connection died with the network, the read never returned, and the
# job sat there holding the scheduler until _Watchdog killed the process.  One
# of those took 8.5 hours to fire, because a threading.Timer does not count
# down while the machine is asleep — so the watchdog was defeated by the same
# condition that caused the hang.
#
# A read timeout turns that into a normal exception the callers already handle,
# and demotes the watchdog from primary defence back to the backstop it was
# meant to be.
#
# requests' read timeout is the gap allowed BETWEEN bytes, not a budget for the
# whole response, so 30s is generous for an API that normally answers in under
# a second — a slow paginated fetch is many quick reads, not one long one.
_DEFAULT_CONNECT_TIMEOUT_SECS = 10.0
_DEFAULT_READ_TIMEOUT_SECS = 30.0


def _timeout_from_env(name: str, default: float) -> float:
    """Read a positive float from the environment, falling back on anything else.

    Parsed defensively and never at the cost of startup.  This runs at import,
    so an uncaught ValueError here would take down every entry point that
    imports auth — the scheduler daemon included, which launchd would then
    crash-loop on its 30s ThrottleInterval.  A typo in a tuning knob must not
    stop live trading.  Same reasoning as pipeline_csv_wait_mins in scheduler.py.

    Non-positive values are rejected rather than honoured: requests treats a 0
    timeout as immediate expiry, so `RH_READ_TIMEOUT_SECS=0` would fail every
    Robinhood call instantly and look like a network outage rather than config.
    """
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning(f"{name} is not a number ({raw!r}) — using {default}s.")
        return default
    if value <= 0:
        logger.warning(f"{name} must be > 0 (got {value}) — using {default}s.")
        return default
    return value


_CONNECT_TIMEOUT_SECS = _timeout_from_env(
    "RH_CONNECT_TIMEOUT_SECS", _DEFAULT_CONNECT_TIMEOUT_SECS
)
_READ_TIMEOUT_SECS = _timeout_from_env(
    "RH_READ_TIMEOUT_SECS", _DEFAULT_READ_TIMEOUT_SECS
)


def _install_default_timeouts(session, connect: float, read: float) -> bool:
    """Give every request on `session` a default timeout.  Idempotent.

    Wraps Session.request rather than patching each robin_stocks call site:
    .get()/.post() both delegate to self.request, so one wrapper covers the
    whole library, including endpoints added by future upstream versions.

    An explicit timeout from a caller always wins; only None is filled in.
    Returns True if the wrapper was installed, False if it was already there.
    """
    if getattr(session, "_trader_timeouts_installed", False):
        return False

    original = session.request

    @functools.wraps(original)
    def _request_with_timeout(*args, **kwargs):
        # *args, not (method, url): Session.request also takes params, data and
        # headers positionally, so naming only the first two would turn a
        # positional third argument into a TypeError.  Nothing upstream passes
        # timeout positionally — it is the 9th parameter — so inspecting kwargs
        # alone is sufficient, and this keeps the wrapper signature-compatible
        # with the method functools.wraps advertises it to be.
        #
        # `is None` rather than `not in kwargs`: requests treats an explicit
        # timeout=None as "block forever", which is the bug being fixed here.
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (connect, read)
        return original(*args, **kwargs)

    session.request = _request_with_timeout
    session._trader_timeouts_installed = True
    return True


# Installed at import, not inside login(): other entry points (--report,
# --pull-portfolio) reach robin_stocks through this module without logging in
# first, and an untimed read hangs them exactly the same way.  helper.SESSION is
# created once at import and only ever mutated, never reassigned, so this holds
# across logins, logouts and pickle reloads.
_install_default_timeouts(
    _rh_helper.SESSION, _CONNECT_TIMEOUT_SECS, _READ_TIMEOUT_SECS
)

_PICKLE_PATH = Path.home() / ".tokens" / "robinhood.pickle"
MAX_LOGIN_ATTEMPTS = 5
_RETRY_SLEEP_SECS = 45       # base wait for non-rate-limited retries (TOTP rotation)
_RATE_LIMIT_SLEEP_SECS = 90  # base wait after a 429; grows by _BACKOFF_STEP_SECS per attempt
_BACKOFF_STEP_SECS = 30      # added to sleep for each successive attempt
_NO_TTY_MSG = ("Robinhood requires SMS/email verification but no TTY is attached "
               "(running in scheduler/automated mode).")


def _retry_wait(attempt: int, rate_limited: bool = True) -> int:
    base = _RATE_LIMIT_SLEEP_SECS if rate_limited else _RETRY_SLEEP_SECS
    return base + (attempt - 1) * _BACKOFF_STEP_SECS


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

    Reference-counted: if a session is already active, increments the
    refcount and returns immediately.  Only the first call (refcount 0→1)
    actually authenticates.  See logout() for the decrement side.

    Retries up to MAX_LOGIN_ATTEMPTS times with progressive backoff
    (90s + 30s per attempt) so the get_prompts_status rate limit resets.
    Every failure clears the stale pickle so the next attempt starts a
    clean session instead of re-triggering verification.
    """
    global _session_refcount

    if _session_refcount > 0 and _rh_helper.LOGGED_IN and not force_fresh:
        _session_refcount += 1
        logger.debug(f"Robinhood session reused (refcount={_session_refcount})")
        return True

    username = os.getenv("ROBINHOOD_USERNAME", "").strip()
    password = os.getenv("ROBINHOOD_PASSWORD", "").strip()

    if not username or not password:
        raise ValueError("ROBINHOOD_USERNAME or ROBINHOOD_PASSWORD missing from .env")

    if force_fresh:
        _clear_stale_pickle()

    for attempt in range(1, MAX_LOGIN_ATTEMPTS + 1):
        totp_code = get_totp_code()   # fresh code each attempt
        logger.info(f"Logging in as {username} (TOTP: {totp_code[:2]}****"
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
            global _INTERACTIVE_AUTH_NEEDED
            if _INTERACTIVE_AUTH_NEEDED:
                _INTERACTIVE_AUTH_NEEDED = False
                raise RuntimeError(_NO_TTY_MSG) from None

            tag = _classify_login_exception(e)

            if attempt >= MAX_LOGIN_ATTEMPTS:
                logger.error(f"❌  Robinhood login failed after {MAX_LOGIN_ATTEMPTS} attempts: {e}")
                raise

            is_rate_limited = tag in ("rate_limit", "none_type")
            wait = _retry_wait(attempt, rate_limited=is_rate_limited)
            _clear_stale_pickle()

            _TAG_DESC = {
                "rate_limit": "Rate-limited (429)",
                "none_type": "Push verification returned empty response (429 on get_prompts_status)",
            }
            desc = _TAG_DESC.get(tag, f"Login exception: {e}")
            logger.warning(
                f"  {desc} on attempt {attempt} — clearing pickle, waiting {wait}s before retry..."
            )
            time.sleep(wait)
            continue

        # Check side-channel flag: SMS/email verification required but no TTY.
        if _INTERACTIVE_AUTH_NEEDED:
            _INTERACTIVE_AUTH_NEEDED = False
            raise RuntimeError(_NO_TTY_MSG)

        if _rh_helper.LOGGED_IN:
            _session_refcount += 1
            logger.info("✅  Robinhood login successful")
            return True

        # Silent failure — session not active despite "successful" return.
        # robin_stocks hit a 429 on get_prompts_status internally, couldn't
        # read the approval, and returned without setting LOGGED_IN.  Clear
        # the stale pickle so the next attempt starts a fresh session instead
        # of re-triggering verification with the expired token.
        wait = _retry_wait(attempt, rate_limited=True)
        logger.warning(
            f"  Login attempt {attempt} returned without activating session "
            f"(LOGGED_IN=False). Likely 429 on get_prompts_status."
        )
        if attempt < MAX_LOGIN_ATTEMPTS:
            _clear_stale_pickle()
            logger.info(
                f"  Cleared pickle, waiting {wait}s for rate limit to reset..."
            )
            time.sleep(wait)
        else:
            raise RuntimeError(
                f"Robinhood login failed after {MAX_LOGIN_ATTEMPTS} attempts: "
                "session never activated. Check Robinhood app for pending approval."
            )

    raise RuntimeError("Robinhood login failed")   # unreachable, satisfies type checkers


def logout():
    """
    Decrement the session refcount.  Only actually disconnects when the
    count reaches zero (i.e. the outermost caller logs out).
    """
    global _session_refcount

    if _session_refcount > 1:
        _session_refcount -= 1
        logger.debug(f"Robinhood logout deferred (refcount={_session_refcount})")
        return

    _session_refcount = 0
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
