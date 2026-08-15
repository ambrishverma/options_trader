"""
utils.py — Shared Utilities
==============================
Logging setup, config loader, run log writer, status display.
"""

import json
import logging
import logging.handlers
import os
import sys
import yaml
from datetime import datetime, date
from pathlib import Path
from typing import Optional

# BASE_DIR is where the *code* lives; DATA_DIR is where mutable state lives.
#
# They are the same directory in a normal checkout, and differ in the container
# where TRADER_DATA_DIR=/data points at the mounted persistent disk.  Keeping
# them separate matters: without it every state path resolves inside the image
# layer, so an image rebuild silently discards logs/run_<date>.json and the
# action ledger — and the next market check then fires a second *live* pipeline
# on a day that already ran, with the dedupe that would have caught it gone too.
BASE_DIR = Path(__file__).parent
DATA_DIR = Path(os.getenv("TRADER_DATA_DIR") or BASE_DIR)


def is_container() -> bool:
    """True when this process is running inside the container image.

    Single source of truth.  This predicate was written out by hand at seven
    call sites and two of them disagreed: `os.getenv(...)` truthiness reads
    `ENV TRADER_DATA_DIR=` (an empty value, which a Dockerfile can perfectly
    well set) as "not a container", while the live-trading gate correctly used
    `is not None`.  The gate is the site where being wrong means an
    unauthoritative instance places real orders, so `is not None` is the
    behaviour kept here and everywhere else.

    Note DATA_DIR above deliberately keeps `or`, not this helper: an empty
    string is a container marker but not a usable path, so it must still fall
    back to BASE_DIR.
    """
    return os.getenv("TRADER_DATA_DIR") is not None


LOG_DIR   = DATA_DIR / "logs"
RECS_DIR  = DATA_DIR / "recommendations"
CONFIG_FILE = BASE_DIR / "config.yaml"      # ships with the code, not the data

# Credentials with no fallback.  Single source of truth: main.check_env() and
# print_status() both read this, so they cannot drift into disagreeing about
# whether the system is configured.
REQUIRED_ENV_VARS = (
    "ROBINHOOD_USERNAME",
    "ROBINHOOD_PASSWORD",
    "ROBINHOOD_TOTP_SEED",
    "RESEND_API_KEY",
    "RESEND_FROM",
)


def _ensure_dir(path: Path) -> None:
    """Create a state directory, tolerating a read-only or full volume.

    These run at import, so raising here kills `import main` outright — taking
    --help and --setup with it, and firing *before* _acquire_pid_lock, whose
    OSError branch exists precisely to handle a read-only mount and would
    otherwise be unreachable for that case.  The first real write then fails
    with a specific error instead of an import traceback.
    """
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        logging.getLogger(__name__).warning(
            "Cannot create %s (%s) — writes to it will fail", path, exc
        )


_ensure_dir(LOG_DIR)
_ensure_dir(RECS_DIR)


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def setup_logging(level: str = "INFO"):
    """Configure root logger: console + rotating file."""
    log_path = LOG_DIR / "options_trader.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.DEBUG)

    # Rotating file handler (5 MB, keep 7 files)
    file_handler = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=7
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    root.addHandler(console)
    root.addHandler(file_handler)

    # Quiet noisy third-party loggers
    for lib in ("urllib3", "yfinance", "peewee", "chardet"):
        logging.getLogger(lib).setLevel(logging.WARNING)


# ─────────────────────────────────────────────────────────────────────────────
# Yahoo Finance helpers
# ─────────────────────────────────────────────────────────────────────────────

def yahoo_symbol(symbol: str) -> str:
    """Convert broker symbol to Yahoo Finance format (e.g. BRK.B → BRK-B)."""
    return symbol.replace(".", "-")


# ─────────────────────────────────────────────────────────────────────────────
# yfinance cache recovery
# ─────────────────────────────────────────────────────────────────────────────

def _yf_cache_dir() -> Path:
    """Where yfinance keeps its SQLite cache, per platform.

    Was hardcoded to the macOS path, so the corruption-recovery below silently
    did nothing on Linux — including in the container.  yfinance uses
    appdirs/platformdirs semantics: XDG_CACHE_HOME (or ~/.cache) on Linux.
    """
    if sys.platform == "darwin":
        return Path.home() / "Library" / "Caches" / "py-yfinance"
    xdg = os.getenv("XDG_CACHE_HOME")
    return (Path(xdg) if xdg else Path.home() / ".cache") / "py-yfinance"


_YF_CACHE_DIR = _yf_cache_dir()
_yf_logger = logging.getLogger("utils.yf_cache")


def close_yfinance_dbs():
    """Close yfinance SQLite cache connections that poison the process lock table.

    Each close_db() is in its own try/except so a failure on one DB
    doesn't skip closing the other.
    """
    import gc
    try:
        from yfinance.cache import _TzDBManager
        _TzDBManager.close_db()
    except Exception:
        pass
    try:
        from yfinance.cache import _CookieDBManager
        _CookieDBManager.close_db()
    except Exception:
        pass
    gc.collect()


def nuke_yfinance_cache():
    """Delete all yfinance SQLite cache files and close open connections."""
    close_yfinance_dbs()
    if _YF_CACHE_DIR.exists():
        for f in _YF_CACHE_DIR.iterdir():
            try:
                f.unlink()
                _yf_logger.info(f"Deleted corrupt cache: {f.name}")
            except Exception:
                pass


def _is_cache_corruption(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(s in msg for s in ("no such table", "_cookieschema", "_tz_kv",
                                   "database disk image is malformed",
                                   "database is locked"))


def yf_retry(fn, *args, **kwargs):
    """Call fn; on yfinance SQLite cache corruption, nuke cache and retry once."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        if _is_cache_corruption(e):
            _yf_logger.warning(f"yfinance cache corrupt ({e}), clearing and retrying")
            nuke_yfinance_cache()
            return fn(*args, **kwargs)
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Config loader
# ─────────────────────────────────────────────────────────────────────────────

_config_cache: dict = {}


def load_config(reload: bool = False) -> dict:
    """
    Load config.yaml. Results are cached after first load.
    Args:
        reload: Force re-read from disk.
    """
    global _config_cache
    if _config_cache and not reload:
        return _config_cache

    if not CONFIG_FILE.exists():
        raise FileNotFoundError(
            f"config.yaml not found at {CONFIG_FILE}. "
            "Run: python main.py --setup"
        )

    with open(CONFIG_FILE) as f:
        _config_cache = yaml.safe_load(f) or {}

    return _config_cache


# ─────────────────────────────────────────────────────────────────────────────
# Run log writer
# ─────────────────────────────────────────────────────────────────────────────

def write_run_log(results: dict):
    """
    Append a pipeline run result to ./logs/run_log.jsonl (one JSON per line).
    Also writes a dated JSON file for easy debugging.
    """
    results["logged_at"] = datetime.now().isoformat()

    # Append to JSONL log
    jsonl_path = LOG_DIR / "run_log.jsonl"
    with open(jsonl_path, "a") as f:
        f.write(json.dumps(results) + "\n")

    # Write dated JSON for quick inspection
    run_date = results.get("run_date", date.today().strftime("%Y-%m-%d"))
    dated_path = LOG_DIR / f"run_{run_date}.json"
    with open(dated_path, "w") as f:
        json.dump(results, f, indent=2)

    logging.getLogger(__name__).debug(f"Run log written: {dated_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Recommendations history log
# ─────────────────────────────────────────────────────────────────────────────

def write_recommendations_log(recommendations: list, run_date: str, dry_run: bool = False):
    """
    Persist today's recommendations to ./recommendations/recommendations_YYYY-MM-DD.json.

    Each file contains a self-contained snapshot:
      {
        "run_date":  "2026-03-16",
        "dry_run":   false,
        "logged_at": "<iso timestamp>",
        "count":     24,
        "recommendations": [ ... full rec dicts ... ]
      }

    Files are never overwritten — if the pipeline runs twice on the same day
    (e.g., a manual re-run after a dry run), the second write appends a
    timestamp suffix so history is never lost.
    """
    base_name = f"recommendations_{run_date}.json"
    dest = RECS_DIR / base_name

    # If a file already exists for today, suffix with HH-MM-SS to avoid overwrite
    if dest.exists():
        ts_suffix = datetime.now().strftime("%H%M%S")
        dest = RECS_DIR / f"recommendations_{run_date}_{ts_suffix}.json"

    payload = {
        "run_date":        run_date,
        "dry_run":         dry_run,
        "logged_at":       datetime.now().isoformat(),
        "count":           len(recommendations),
        "recommendations": recommendations,
    }

    with open(dest, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    logging.getLogger(__name__).info(
        f"Recommendations log saved: {dest} ({len(recommendations)} recs)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Strategy recs snapshot  (persisted during --run, consumed by --income-generator)
# ─────────────────────────────────────────────────────────────────────────────

SNAPSHOTS_DIR = DATA_DIR / "snapshots"
_ensure_dir(SNAPSHOTS_DIR)


def write_strategy_recs_snapshot(
    strategy_recs: list,
    run_date: str,
    dry_run: bool = False,
) -> Path:
    """
    Persist scanned strategy recommendations to
    ``./snapshots/strategy_recs_YYYY-MM-DD.json``.

    The income generator reads this file instead of re-scanning.
    Overwrites any previous snapshot for the same date (the pipeline
    only runs once per day; a re-run should refresh the data).
    """
    dest = SNAPSHOTS_DIR / f"strategy_recs_{run_date}.json"

    if dry_run and dest.exists():
        logging.getLogger(__name__).info(
            f"Strategy recs snapshot skipped (dry-run, live snapshot exists): {dest}"
        )
        return dest

    payload = {
        "run_date":       run_date,
        "dry_run":        dry_run,
        "saved_at":       datetime.now().isoformat(),
        "count":          len(strategy_recs),
        "strategy_recs":  strategy_recs,
    }

    with open(dest, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    logging.getLogger(__name__).info(
        f"Strategy recs snapshot saved: {dest} ({len(strategy_recs)} recs)"
    )
    return dest


def load_strategy_recs_snapshot(
    target_date: Optional[str] = None,
) -> list:
    """
    Load persisted strategy recommendations for *target_date* (YYYY-MM-DD).

    Falls back to today if no date is given.  Returns an empty list when
    no snapshot file exists (e.g. before the first --run of the day).
    """
    d = target_date or date.today().strftime("%Y-%m-%d")
    path = SNAPSHOTS_DIR / f"strategy_recs_{d}.json"

    if not path.exists():
        logging.getLogger(__name__).warning(
            f"No strategy recs snapshot for {d} — run --run first"
        )
        return []

    with open(path) as f:
        data = json.load(f)

    recs = data.get("strategy_recs", [])
    logging.getLogger(__name__).info(
        f"Loaded {len(recs)} strategy rec(s) from {path.name}"
    )
    return recs


def write_spread_recs_snapshot(
    spread_recs: list,
    run_date: str,
    dry_run: bool = False,
) -> Path:
    """
    Persist daily CCS/PCS scanner recommendations to
    ``./snapshots/spread_recs_YYYY-MM-DD.json``.

    The income generator Pass-3 reads this file to purchase non-strategy
    spreads when the daily income goal is not met after Pass-1 and Pass-2.
    """
    dest = SNAPSHOTS_DIR / f"spread_recs_{run_date}.json"

    if dry_run and dest.exists():
        logging.getLogger(__name__).info(
            f"Spread recs snapshot skipped (dry-run, live snapshot exists): {dest}"
        )
        return dest

    payload = {
        "run_date":      run_date,
        "dry_run":       dry_run,
        "saved_at":      datetime.now().isoformat(),
        "count":         len(spread_recs),
        "spread_recs":   spread_recs,
    }

    with open(dest, "w") as f:
        json.dump(payload, f, indent=2, default=str)

    logging.getLogger(__name__).info(
        f"Spread recs snapshot saved: {dest} ({len(spread_recs)} recs)"
    )
    return dest


def load_spread_recs_snapshot(
    target_date: Optional[str] = None,
) -> list:
    """
    Load persisted CCS/PCS scanner recommendations for *target_date*.

    Falls back to today if no date is given.  Returns an empty list when
    no snapshot file exists.
    """
    d = target_date or date.today().strftime("%Y-%m-%d")
    path = SNAPSHOTS_DIR / f"spread_recs_{d}.json"

    if not path.exists():
        logging.getLogger(__name__).info(
            f"No spread recs snapshot for {d}"
        )
        return []

    with open(path) as f:
        data = json.load(f)

    recs = data.get("spread_recs", [])
    logging.getLogger(__name__).info(
        f"Loaded {len(recs)} spread rec(s) from {path.name}"
    )
    return recs


# ─────────────────────────────────────────────────────────────────────────────
# Status display
# ─────────────────────────────────────────────────────────────────────────────

def print_status():
    """Print system health and last run summary to stdout."""
    print("\n" + "═" * 60)
    print("  Options Trader — System Status")
    print("═" * 60)

    # Config
    config_ok = CONFIG_FILE.exists()
    # Credentials arrive from a .env file OR from the environment — the container
    # image deliberately has no .env.  Checking only the file made --status report
    # "run --setup" inside the container, and following that instruction writes
    # plaintext credentials into the ephemeral image layer.
    env_file_ok = (BASE_DIR / ".env").exists()
    # Load the .env first so a file-based install is judged on its VALUES, not
    # its existence — an empty or half-filled .env used to report OK here while
    # the very next command exited 1 from check_env.
    if env_file_ok:
        try:
            from dotenv import load_dotenv
            load_dotenv(BASE_DIR / ".env")
        except ImportError:
            pass
    # No "incomplete credentials" branch here.  print_status() is reached only
    # through main.cmd_status(), which runs check_env() first — and check_env
    # exits 1 naming exactly which REQUIRED_ENV_VARS are missing.  A branch for
    # that state could never fire, and reading as though it could implied a
    # diagnostic this function does not provide.
    secrets_msg = (".env found" if env_file_ok else "credentials from environment")
    import scheduler as _sched
    if _sched._force_dry_run:
        print("\n  ⚠️   NOT AUTHORITATIVE — scheduled jobs skip and on-demand")
        print("      order commands are refused. Set TRADER_ALLOW_LIVE=1 to enable.")
    print(f"\n  Config:     {'✅  config.yaml found' if config_ok else '❌  config.yaml missing'}")
    print(f"  Secrets:    ✅  {secrets_msg}")

    if config_ok:
        try:
            config = load_config()
            print(f"  Mode:       {config.get('mode', 'unknown')}")
            print(f"  OTM min:    {config.get('min_otm_pct', '?')}%")
            print(f"  Lookahead:  {config.get('lookahead_days', '?')} days")
            print(f"  Recipient:  {config.get('recipient_email', '?')}")
            print(f"  Pipeline:   {config.get('pipeline_time_et', '?')} ET daily")
        except Exception as e:
            print(f"  Config read error: {e}")

    # Snapshots
    import glob
    snaps = sorted(glob.glob(str(DATA_DIR / "snapshots" / "portfolio_*.json")), reverse=True)
    if snaps:
        latest = Path(snaps[0])
        try:
            with open(latest) as f:
                snap_data = json.load(f)
            pulled = snap_data.get("pulled_at", "unknown")
            n_holdings = len(snap_data.get("holdings", []))
            eligible   = sum(1 for h in snap_data.get("holdings", []) if h.get("eligible"))
            print(f"\n  Portfolio:  {n_holdings} holdings, {eligible} eligible (pulled {pulled[:10]})")
        except Exception:
            print(f"\n  Portfolio:  snapshot found but unreadable")
    else:
        print(f"\n  Portfolio:  ❌  No snapshot — run --pull-portfolio")

    # Last run
    jsonl_path = LOG_DIR / "run_log.jsonl"
    if jsonl_path.exists():
        try:
            lines = jsonl_path.read_text().strip().split("\n")
            last = json.loads(lines[-1])
            outcome = last.get("outcome", "?")
            icon = "✅" if outcome == "success" else "⚠️" if outcome == "no_eligible_holdings" else "❌"
            print(f"\n  Last run:   {icon}  {last.get('run_date', '?')} — {outcome}")
            print(f"  Duration:   {last.get('duration_sec', '?')}s")
            print(f"  Holdings:   {last.get('holdings_eligible', '?')} eligible")
            print(f"  Options:    {last.get('options_raw', '?')} raw → {last.get('options_passing', '?')} passing")
            print(f"  Recs:       {last.get('recommendations', '?')}")
            print(f"  Earnings:   {last.get('earnings_flagged', '?')} warning(s)")
            print(f"  Email:      {'sent ✅' if last.get('email_sent') else 'not sent'}")
        except Exception:
            print(f"\n  Last run:   log file found but unreadable")
    else:
        print(f"\n  Last run:   No runs recorded yet")

    print("\n" + "═" * 60 + "\n")
