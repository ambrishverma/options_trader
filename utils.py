"""
utils.py — Shared Utilities
==============================
Logging setup, config loader, run log writer, status display.
"""

import json
import logging
import logging.handlers
import yaml
from datetime import datetime, date
from pathlib import Path

BASE_DIR  = Path(__file__).parent
LOG_DIR   = BASE_DIR / "logs"
CONFIG_FILE = BASE_DIR / "config.yaml"
LOG_DIR.mkdir(exist_ok=True)


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
# Status display
# ─────────────────────────────────────────────────────────────────────────────

def print_status():
    """Print system health and last run summary to stdout."""
    print("\n" + "═" * 60)
    print("  Options Trader — System Status")
    print("═" * 60)

    # Config
    config_ok = CONFIG_FILE.exists()
    env_ok = (BASE_DIR / ".env").exists()
    print(f"\n  Config:     {'✅  config.yaml found' if config_ok else '❌  config.yaml missing'}")
    print(f"  Secrets:    {'✅  .env found' if env_ok else '❌  .env missing — run --setup'}")

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
    snaps = sorted(glob.glob(str(BASE_DIR / "snapshots" / "portfolio_*.json")), reverse=True)
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
