"""Tests for the /healthz liveness probe.

/healthz is the one unauthenticated endpoint — Cloud Monitoring's uptime check
cannot send a bearer token.  It must therefore reveal nothing beyond liveness.
"""

import sys
import os
from unittest import mock

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from api import app

client = TestClient(app)


class TestHealthz:

    def test_returns_200_when_scheduler_alive(self):
        with mock.patch("api.scheduler_alive", return_value=True):
            r = client.get("/healthz")
        assert r.status_code == 200

    def test_returns_503_when_scheduler_dead(self):
        with mock.patch("api.scheduler_alive", return_value=False):
            r = client.get("/healthz")
        assert r.status_code == 503

    def test_body_is_empty(self):
        """Unauthenticated endpoint must leak nothing — status code only."""
        with mock.patch("api.scheduler_alive", return_value=True):
            r = client.get("/healthz")
        assert r.content == b""

    def test_requires_no_auth_header(self):
        """Cloud Monitoring cannot send a bearer token."""
        with mock.patch("api.scheduler_alive", return_value=True):
            r = client.get("/healthz")
        assert r.status_code == 200


class TestNoIncidentalSurface:
    """Phase 1 ships exactly one endpoint; docs routes would leak the rest later."""

    def test_openapi_schema_is_disabled(self):
        assert client.get("/openapi.json").status_code == 404

    def test_interactive_docs_are_disabled(self):
        assert client.get("/docs").status_code == 404
