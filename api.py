"""
api.py — HTTP interface to the options-trader daemon
=====================================================
Runs on uvicorn's main thread while the scheduler polls on a background
thread (see `main.py --serve`).

Phase 1 exposes only /healthz.  Every endpoint added later requires a bearer
token; /healthz stays open because Cloud Monitoring's uptime check cannot send
one, and it returns a bare status code so an unauthenticated caller learns
nothing about trading schedules or system state.

Interactive docs and the OpenAPI schema are disabled: this is a private
single-user API, and publishing a route inventory to anonymous callers is
surface with no upside.
"""

import logging

from fastapi import FastAPI, Response

from scheduler import scheduler_alive

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Options Trader",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/healthz")
def healthz() -> Response:
    """Liveness probe.  200 if the scheduler loop is ticking, 503 if not.

    Deliberately returns no body — this is the one unauthenticated endpoint.
    A 503 here means the scheduler thread died while uvicorn kept serving,
    which is the failure this endpoint exists to surface.
    """
    return Response(status_code=200 if scheduler_alive() else 503)
