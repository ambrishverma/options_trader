"""Tests for check_env() credential pre-flight.

Credentials reach the process two different ways:
  - locally, from a .env file on disk
  - in the container, as environment variables injected by compose/Secret
    Manager (the image deliberately contains no .env)

check_env() must accept either, and must catch a .env that exists but is
missing values — the old file-existence check passed an empty file.
"""

import sys
import os
from unittest import mock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import main


@pytest.fixture(autouse=True)
def _reset_warned():
    """check_env memoises its optional-credential warning on the function object.

    Without a reset, whether the warning fires depends on test collection
    order — the once-per-process memo makes these tests order-dependent.
    """
    main.check_env.__dict__.pop("_warned", None)
    yield
    main.check_env.__dict__.pop("_warned", None)


REQUIRED = {
    "ROBINHOOD_USERNAME": "u@example.com",
    "ROBINHOOD_PASSWORD": "pw",
    "ROBINHOOD_TOTP_SEED": "JBSWY3DPEHPK3PXP",
    "RESEND_API_KEY": "re_x",
    "RESEND_FROM": "Trader <t@example.com>",
    "FINNHUB_API_KEY": "fh_x",
}


class TestCheckEnv:

    def test_passes_when_all_vars_present_without_env_file(self):
        """The container case: no .env on disk, everything injected as env vars."""
        with mock.patch.dict(os.environ, REQUIRED, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            main.check_env()   # must not raise

    def test_exits_when_a_required_var_is_missing(self):
        partial = dict(REQUIRED)
        del partial["ROBINHOOD_TOTP_SEED"]
        with mock.patch.dict(os.environ, partial, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            with pytest.raises(SystemExit) as exc:
                main.check_env()
        assert exc.value.code == 1

    def test_does_not_exit_when_only_an_optional_var_is_missing(self, capsys):
        """FINNHUB_API_KEY has a documented fallback in earnings.py.

        Requiring it locked every command — including read-only --status —
        behind a credential the module gating it treats as optional.
        """
        without_optional = {k: v for k, v in REQUIRED.items() if k != "FINNHUB_API_KEY"}
        with mock.patch.dict(os.environ, without_optional, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            main.check_env()          # must not raise
        assert "FINNHUB_API_KEY" in capsys.readouterr().out, "should warn, not fail"

    def test_error_names_the_missing_variables(self, capsys):
        partial = dict(REQUIRED)
        del partial["RESEND_API_KEY"]
        del partial["ROBINHOOD_TOTP_SEED"]
        with mock.patch.dict(os.environ, partial, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            with pytest.raises(SystemExit):
                main.check_env()
        out = capsys.readouterr().out
        assert "RESEND_API_KEY" in out
        assert "ROBINHOOD_TOTP_SEED" in out

    def test_empty_string_counts_as_missing(self):
        """A var set to '' is not a credential."""
        blanked = dict(REQUIRED, ROBINHOOD_PASSWORD="")
        with mock.patch.dict(os.environ, blanked, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            with pytest.raises(SystemExit):
                main.check_env()

    def test_suggests_setup_when_nothing_is_configured(self, capsys):
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch("main.Path") as mock_path:
            mock_path.return_value.parent.__truediv__.return_value.exists.return_value = False
            with pytest.raises(SystemExit):
                main.check_env()
        assert "--setup" in capsys.readouterr().out
