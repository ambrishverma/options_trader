"""Tests for reference-counted login/logout in auth.py."""

import sys
import os
from unittest import mock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import auth


@pytest.fixture(autouse=True)
def reset_refcount():
    """Reset the refcount before and after each test."""
    auth._session_refcount = 0
    yield
    auth._session_refcount = 0


class TestRefCountedLogin:
    """login() should authenticate once and reuse the session for nested calls."""

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.login")
    def test_first_login_authenticates(self, mock_rh_login, mock_totp):
        """First login() call should perform actual RH authentication."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            result = auth.login()

        assert result is True
        assert auth._session_refcount == 1
        mock_rh_login.assert_called_once()

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.login")
    def test_nested_login_skips_auth(self, mock_rh_login, mock_totp):
        """Second login() call should skip authentication and bump refcount."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            auth.login()
            assert auth._session_refcount == 1

            result = auth.login()
            assert result is True
            assert auth._session_refcount == 2

        mock_rh_login.assert_called_once()

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.login")
    def test_triple_nested_login(self, mock_rh_login, mock_totp):
        """Multiple nested login() calls should each increment refcount."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            auth.login()
            auth.login()
            auth.login()

        assert auth._session_refcount == 3
        mock_rh_login.assert_called_once()

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.login")
    def test_force_fresh_bypasses_refcount(self, mock_rh_login, mock_totp):
        """login(force_fresh=True) should re-authenticate even when refcount > 0."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            auth.login()
            assert auth._session_refcount == 1
            assert mock_rh_login.call_count == 1

            auth.login(force_fresh=True)
            assert auth._session_refcount == 2
            assert mock_rh_login.call_count == 2


class TestRefCountedLogout:
    """logout() should only disconnect when refcount reaches zero."""

    @mock.patch("auth.rh.logout")
    def test_logout_at_refcount_1_disconnects(self, mock_rh_logout):
        """logout() at refcount=1 should perform actual RH logout."""
        auth._session_refcount = 1
        auth.logout()

        assert auth._session_refcount == 0
        mock_rh_logout.assert_called_once()

    @mock.patch("auth.rh.logout")
    def test_logout_at_refcount_2_defers(self, mock_rh_logout):
        """logout() at refcount=2 should decrement but not disconnect."""
        auth._session_refcount = 2
        auth.logout()

        assert auth._session_refcount == 1
        mock_rh_logout.assert_not_called()

    @mock.patch("auth.rh.logout")
    def test_logout_at_refcount_0_still_safe(self, mock_rh_logout):
        """logout() at refcount=0 should call rh.logout (cleanup) and not go negative."""
        auth._session_refcount = 0
        auth.logout()

        assert auth._session_refcount == 0
        mock_rh_logout.assert_called_once()


class TestRefCountedRoundTrip:
    """Full nested login/logout round-trips should work correctly."""

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.logout")
    @mock.patch("auth.rh.login")
    def test_nested_round_trip(self, mock_rh_login, mock_rh_logout, mock_totp):
        """Simulate pipeline pattern: outer login, inner login+logout, outer logout."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            # Outer caller (pipeline) logs in
            auth.login()
            assert auth._session_refcount == 1

            # Inner caller (e.g. execute_short_optimize) logs in
            auth.login()
            assert auth._session_refcount == 2

            # Inner caller logs out — should NOT disconnect
            auth.logout()
            assert auth._session_refcount == 1
            mock_rh_logout.assert_not_called()

            # Another inner caller logs in and out
            auth.login()
            assert auth._session_refcount == 2
            auth.logout()
            assert auth._session_refcount == 1
            mock_rh_logout.assert_not_called()

            # Outer caller logs out — SHOULD disconnect
            auth.logout()
            assert auth._session_refcount == 0
            mock_rh_logout.assert_called_once()

        mock_rh_login.assert_called_once()

    @mock.patch.dict(os.environ, {
        "ROBINHOOD_USERNAME": "test@test.com",
        "ROBINHOOD_PASSWORD": "pw",
    })
    @mock.patch("auth.get_totp_code", return_value="123456")
    @mock.patch("auth.rh.logout")
    @mock.patch("auth.rh.login")
    def test_standalone_login_logout_still_works(self, mock_rh_login, mock_rh_logout, mock_totp):
        """Without a pipeline wrapper, a single login/logout should still authenticate and disconnect."""
        with mock.patch.object(auth._rh_helper, "LOGGED_IN", True):
            auth.login()
            assert auth._session_refcount == 1

        auth.logout()
        assert auth._session_refcount == 0
        mock_rh_login.assert_called_once()
        mock_rh_logout.assert_called_once()
