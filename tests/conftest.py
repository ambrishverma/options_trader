"""Suite-wide guards against tests mutating real repository state.

TestSuiteHasNoStateSideEffects in test_container_hardening.py guards one
specific leak (utils.write_run_log). This guards the general case, because that
narrow check did not stop a test in this very PR from rewriting config.yaml's
min_otm_pct from 10.0 to 7.5 — a live trading parameter — simply by calling
cmd_config() without redirecting utils.CONFIG_FILE first. Nothing failed; the
change was only noticed in `git diff`.

Tests that legitimately exercise a write must point it at tmp_path.
"""

import hashlib
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

# Files that carry real operating state and must survive the suite byte-identical.
# config.yaml holds the live-order master switches (ig_enabled, auto_income,
# auto_defense) and the strategy thresholds; .env holds credentials.
#
# KNOWN GAP — this list is not everything the suite touches.  On a developer
# host DATA_DIR == BASE_DIR, so runs also rewrite logs/email_preview_<date>.html
# (clobbering a saved operator preview) and append to logs/options_trader.log
# (interleaving test noise into the real rotating app log, and able to trigger
# rotation).  Those are NOT listed here because they would fail the suite today
# rather than fix it.
#
# The obvious remedy — setting TRADER_DATA_DIR to a tmp dir in this file, before
# utils resolves DATA_DIR at import — must NOT be used: it would make
# utils.is_container() true for the whole session, flipping scheduler's
# _force_dry_run and invalidating every live-trading-gate test. Redirecting the
# log destination specifically is the way in, not redirecting DATA_DIR.
_PROTECTED = ("config.yaml", ".env")


def _digest(path: Path):
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="session", autouse=True)
def _repo_state_is_not_mutated():
    before = {name: _digest(REPO / name) for name in _PROTECTED}
    yield
    changed = [
        name for name in _PROTECTED
        if _digest(REPO / name) != before[name]
    ]
    if changed:
        raise AssertionError(
            f"the test suite modified real repository state: {changed}. "
            "A test wrote to the checkout instead of tmp_path — redirect the "
            "write (e.g. mock.patch.object(utils, 'CONFIG_FILE', tmp_path/...)) "
            "before calling anything that persists config."
        )
