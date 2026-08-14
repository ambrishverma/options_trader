# Cloud Migration Phase 1 — Containerize Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Package the existing scheduler and a minimal web API into a Docker container that starts, serves a liveness probe, and shuts down gracefully — proving the runtime architecture before any GCP resources exist.

**Architecture:** A single Python process runs uvicorn on the main thread and the existing `schedule` polling loop on a background thread. A module-level `threading.Event` coordinates shutdown so SIGTERM from `docker stop` drains the current job instead of killing it mid-order. The scheduler stamps a heartbeat each tick; `/healthz` reports 503 if that heartbeat goes stale, making a dead scheduler thread visible rather than silent. Caddy runs as a separate container for TLS.

**Tech Stack:** Python 3.13, FastAPI + uvicorn, Docker + docker-compose (via Colima), Caddy 2.

## Global Constraints

- **Python 3.13** everywhere — container base image is `python:3.13-slim`, matching the laptop's 3.13.9. Do not use 3.12.
- **No new runtime dependencies** beyond `fastapi` and `uvicorn[standard]`. No `psutil`, no `slowapi`, no process supervisor.
- **`INSTANCE_ID` is never defaulted in code.** Phase 1 does not implement the control flag, but do not introduce a default that Phase 2 would have to remove.
- **Existing 785 tests must continue to pass.** Run the full suite before every commit.
- **Do not modify pipeline/trading logic.** Phase 1 changes process lifecycle only.
- **Branch:** all work on `feat/cloud-phase1-containerize`. Never commit to `main`.

---

## Prerequisites (do once, before Task 1)

Install Colima and the Docker CLI. Neither needs admin rights.

```bash
brew install colima docker docker-compose
colima start --cpu 2 --memory 4
docker info | head -5
```

Expected: `docker info` prints server details without error. If it says "Cannot connect to the Docker daemon", run `colima status` and `colima start`.

Create the working branch:

```bash
git checkout main && git pull && git checkout -b feat/cloud-phase1-containerize
```

---

## File Structure

| File | Responsibility |
|---|---|
| `requirements.lock` | **Create.** Exact pinned versions of every transitive dependency, generated from the working environment. Makes image builds reproducible. |
| `scheduler.py` | **Modify.** Add shutdown event, signal handlers, heartbeat, and a non-blocking start mode. Lifecycle only — no job logic changes. |
| `api.py` | **Create.** FastAPI app. Phase 1 holds only `/healthz`. Phase 2 adds the remaining eleven endpoints here. |
| `main.py` | **Modify.** Add the `--serve` flag and its dispatch. |
| `Dockerfile` | **Create.** Python 3.13 base, gcloud SDK for `gsutil`, tini as PID 1. |
| `docker-compose.yaml` | **Create.** Caddy + app services, volumes, memory limit, grace period. |
| `deploy/Caddyfile` | **Create.** Three-line reverse proxy config. |
| `tests/test_scheduler_lifecycle.py` | **Create.** Tests for shutdown and heartbeat. |
| `tests/test_api_health.py` | **Create.** Tests for `/healthz`. |

---

## Task 1: Pin dependencies

The PRD flags dependency drift as a watch item: `requirements.txt` pins only `robin-stocks==3.4.0` and everything else floats. The installed environment has already drifted several major versions (`pandas 3.0.1` vs `>=2.2.0`, `yfinance 1.2.0` vs `>=0.2.40`). Pin what actually works today so the container builds reproducibly.

**Files:**
- Create: `requirements.lock`
- Modify: `requirements.txt` (add a pointer comment only)

**Interfaces:**
- Produces: `requirements.lock` — consumed by `Dockerfile` in Task 5.

> **Corrected during execution.** The original plan said to run `pip freeze` on the host. That does not work here: the scheduler runs from a shared miniconda base environment, so `pip freeze` emitted 183 lines of which **62 were `@ file:///` conda build paths** — including genuine runtime dependencies (`requests`, `urllib3`, `certifi`, `python-dotenv`) that carry no installable version — plus an editable install of an unrelated project (`-e /Users/ambrish/Code/Agents/RAG-Agent`). None of it installs in a container.
>
> The lock is therefore resolved **inside `python:3.13-slim`**, which is also more correct: the lock should describe the container's environment, not the developer's laptop. Host-installed versions still seed the pins, since those are the versions the 785 tests pass against.

- [ ] **Step 1: Read the known-good versions of the direct dependencies**

```bash
/Users/ambrish/miniconda3/bin/python3 -c "
import importlib.metadata as m
deps = ['robin-stocks','pyotp','yfinance','scipy','finnhub-python','exchange-calendars',
        'resend','Jinja2','pandas','openpyxl','requests','python-dotenv','PyYAML',
        'schedule','tzdata']
for d in deps:
    try: print(f'{d}=={m.version(d)}')
    except Exception: print(f'# {d}: NOT FOUND')
"
```

Expected: fifteen `name==version` lines, no `NOT FOUND`. These are the versions the existing suite passes against.

- [ ] **Step 2: Pin those versions in requirements.txt**

Rewrite `requirements.txt` replacing every `>=` with the exact `==` version from Step 1, keeping the existing section comments. Append the two web dependencies:

```
# Web API (container --serve mode)
fastapi==0.115.6
uvicorn[standard]==0.34.0
```

Add a header comment recording how the lock is regenerated:

```
# Local install:      pip install -r requirements.txt
# Container build:    uses requirements.lock (exact transitive closure)
#
# To change a dependency: edit the pin here, then regenerate the lock:
#   docker run --rm -v "$PWD:/w" -w /w python:3.13-slim sh -c \
#     'pip install -q -r requirements.txt && pip freeze' > requirements.lock
```

- [ ] **Step 3: Resolve the transitive closure inside a clean container**

```bash
docker run --rm -v "$PWD:/w" -w /w python:3.13-slim sh -c \
  'pip install -q -r requirements.txt && pip freeze' > /tmp/lock.raw
wc -l /tmp/lock.raw
```

Expected: ~54 lines.

If this fails with `error getting credentials - err: exec: "docker-credential-desktop"`, a stale Docker Desktop credential helper is configured. Check `~/.docker/config.json`; if `auths` is empty there is nothing to lose — back the file up and remove the `credsStore` key, then retry. Public images need no auth.

- [ ] **Step 4: Verify the lock is clean**

```bash
grep -c "@ file://\|^-e " /tmp/lock.raw        # expect 0
grep -vcE "^[A-Za-z0-9_.-]+==" /tmp/lock.raw   # expect 0 (everything pinned)
```

Both must be `0`. A non-zero first count means the resolve leaked host state; a non-zero second means something is unpinned and the build is not reproducible.

- [ ] **Step 5: Write requirements.lock with a regeneration header**

```bash
{ echo "# Options Trader — exact transitive dependency closure"
  echo "#"
  echo "# GENERATED FILE — do not edit by hand."
  echo "# Resolved inside python:3.13-slim so it describes the container environment,"
  echo "# not the developer's macOS/conda environment (a host 'pip freeze' picks up"
  echo "# conda file:// paths and unrelated editable installs, and cannot install)."
  echo "#"
  echo "# Regenerate after any requirements.txt change:"
  echo "#   docker run --rm -v \"\$PWD:/w\" -w /w python:3.13-slim sh -c \\"
  echo "#     'pip install -q -r requirements.txt && pip freeze' > requirements.lock"
  echo "#   (then re-add this header)"
  echo
  cat /tmp/lock.raw
} > requirements.lock
```

- [ ] **Step 6: Validate the lock by running the full suite against it in a container**

This is the real proof — not just that the lock installs, but that the pinned versions are functionally correct on the target platform.

```bash
docker run --rm -v "$PWD:/w" -w /w python:3.13-slim sh -c \
  'pip install -q -r requirements.lock && pip install -q pytest httpx && python -m pytest tests/ -q 2>&1 | tail -3'
```

Expected: `785 passed`. A failure here means a pinned version is wrong for Linux/3.13 — fix the pin in `requirements.txt` and regenerate before continuing. (`pytest` and `httpx` are test-only and deliberately absent from the lock.)

- [ ] **Step 7: Commit**

```bash
git add requirements.lock requirements.txt
git commit -m "build: pin exact dependency versions in requirements.lock

Container builds need reproducible dependency resolution. requirements.txt
pinned only robin-stocks; everything else floated and had already drifted
several major versions (pandas 3.0.1 vs >=2.2.0, yfinance 1.2.0 vs >=0.2.40).

The lock is resolved inside python:3.13-slim rather than on the host: the
scheduler runs from a shared miniconda base env whose pip freeze emits 62
conda file:// paths and an unrelated editable install, none of which can be
installed in a container. Resolving in-image also makes the lock describe the
environment it actually targets.

Validated by running the full 785-test suite against the locked set inside a
clean Linux 3.13 container.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 2: Scheduler graceful shutdown and liveness heartbeat

Two lifecycle changes to the same polling loop, reviewed together.

**Shutdown:** `docker stop` sends SIGTERM. Today the loop is `while True: schedule.run_pending(); time.sleep(30)` — SIGTERM's default action terminates the process immediately, potentially mid-order. Replace the sleep with an `Event.wait()` so SIGTERM wakes the loop instantly and it exits between jobs.

**Heartbeat:** In `--serve` mode the scheduler is a background thread. If it dies, uvicorn keeps serving and nothing notices. Stamp a monotonic timestamp each tick so `/healthz` can detect a dead thread.

**Files:**
- Modify: `scheduler.py:2801-2806` (the polling loop) and the module header
- Test: `tests/test_scheduler_lifecycle.py`

**Interfaces:**
- Produces:
  - `request_shutdown() -> None` — sets the shutdown event
  - `install_signal_handlers() -> None` — registers SIGTERM/SIGINT; no-op off the main thread
  - `scheduler_alive(max_age_secs: float = 90.0) -> bool` — True if the loop ticked recently
  - `start_scheduler(block: bool = True)` — `block=False` registers jobs and returns without entering the loop
  - `run_loop() -> None` — the polling loop, callable on any thread
- Consumed by: `api.py` (Task 3) and `main.py` (Task 4)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_scheduler_lifecycle.py`:

```python
"""Tests for scheduler process lifecycle: graceful shutdown and liveness heartbeat."""

import sys
import os
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import scheduler


@pytest.fixture(autouse=True)
def reset_lifecycle_state():
    """Each test starts with a clear shutdown event and no heartbeat."""
    scheduler._shutdown.clear()
    scheduler._last_tick = None
    yield
    scheduler._shutdown.set()      # release any loop still running
    scheduler._shutdown.clear()
    scheduler._last_tick = None


class TestGracefulShutdown:

    def test_request_shutdown_sets_event(self):
        assert not scheduler._shutdown.is_set()
        scheduler.request_shutdown()
        assert scheduler._shutdown.is_set()

    def test_run_loop_exits_when_shutdown_requested(self):
        """run_loop must return promptly after request_shutdown, not after the 30s tick."""
        thread = threading.Thread(target=scheduler.run_loop, daemon=True)
        thread.start()
        time.sleep(0.2)
        assert thread.is_alive(), "loop should still be running before shutdown"

        scheduler.request_shutdown()
        thread.join(timeout=5)
        assert not thread.is_alive(), "loop did not exit within 5s of shutdown request"

    def test_run_loop_returns_immediately_if_already_shut_down(self):
        scheduler.request_shutdown()
        start = time.monotonic()
        scheduler.run_loop()
        assert time.monotonic() - start < 1.0

    def test_install_signal_handlers_is_noop_off_main_thread(self):
        """signal.signal() raises ValueError off the main thread; we must not."""
        error = {}

        def target():
            try:
                scheduler.install_signal_handlers()
            except Exception as e:
                error["e"] = e

        t = threading.Thread(target=target)
        t.start()
        t.join(timeout=5)
        assert "e" not in error, f"install_signal_handlers raised off-thread: {error.get('e')}"


class TestLivenessHeartbeat:

    def test_scheduler_alive_false_before_any_tick(self):
        assert scheduler.scheduler_alive() is False

    def test_run_loop_stamps_heartbeat(self):
        thread = threading.Thread(target=scheduler.run_loop, daemon=True)
        thread.start()
        time.sleep(0.2)
        scheduler.request_shutdown()
        thread.join(timeout=5)

        assert scheduler._last_tick is not None
        assert scheduler.scheduler_alive() is True

    def test_scheduler_alive_false_when_heartbeat_stale(self):
        scheduler._last_tick = time.monotonic() - 120.0
        assert scheduler.scheduler_alive(max_age_secs=90.0) is False

    def test_scheduler_alive_true_within_window(self):
        scheduler._last_tick = time.monotonic() - 10.0
        assert scheduler.scheduler_alive(max_age_secs=90.0) is True
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
python -m pytest tests/test_scheduler_lifecycle.py -q
```

Expected: FAIL with `AttributeError: module 'scheduler' has no attribute '_shutdown'`

- [ ] **Step 3: Add the lifecycle primitives to scheduler.py**

Insert immediately after the `_PID_FILE = BASE_DIR / "scheduler.pid"` line (currently `scheduler.py:2700`):

```python
# ---------------------------------------------------------------------------
# Process lifecycle: graceful shutdown + liveness heartbeat
# ---------------------------------------------------------------------------
# `docker stop` sends SIGTERM then SIGKILLs after the grace period.  The loop
# below waits on an Event rather than sleeping, so SIGTERM wakes it instantly
# and it exits between jobs instead of dying mid-order.
#
# _last_tick is stamped every iteration so /healthz can tell a live scheduler
# thread from a dead one — in --serve mode uvicorn keeps answering even if the
# scheduler thread has died, and that silence is exactly what we must catch.
# ---------------------------------------------------------------------------

_shutdown = threading.Event()
_last_tick: float | None = None
_TICK_SECONDS = 30


def request_shutdown() -> None:
    """Ask the scheduler loop to exit after the current job completes."""
    _shutdown.set()


def install_signal_handlers() -> None:
    """Route SIGTERM/SIGINT to a graceful shutdown.

    The signal module only permits handler registration on the main thread.
    In --serve mode the scheduler runs on a background thread and uvicorn owns
    the main thread's handlers, so this is a deliberate no-op there.
    """
    if threading.current_thread() is not threading.main_thread():
        logger.debug("Not the main thread — skipping signal handler registration")
        return
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda _sig, _frm: request_shutdown())
    logger.info("Signal handlers installed (SIGTERM/SIGINT → graceful shutdown)")


def scheduler_alive(max_age_secs: float = 90.0) -> bool:
    """True if the polling loop ticked within max_age_secs.

    Default is 3x the tick interval, so one slow iteration does not read as a
    failure but a genuinely dead thread is caught within ~90 seconds.
    """
    if _last_tick is None:
        return False
    return (time.monotonic() - _last_tick) < max_age_secs


def run_loop() -> None:
    """Poll the schedule until shutdown is requested. Safe on any thread."""
    global _last_tick
    while not _shutdown.is_set():
        _last_tick = time.monotonic()
        try:
            schedule.run_pending()
        except Exception as exc:
            # A job that escapes its own error handling must not kill the loop.
            logger.exception(f"Unhandled exception in scheduled job: {exc}")
        _shutdown.wait(timeout=_TICK_SECONDS)
    logger.info("Scheduler loop exited cleanly.")
```

- [ ] **Step 4: Add the required imports**

Confirm `scheduler.py` imports `signal` and `threading`. Check first:

```bash
grep -nE '^import (signal|threading)' scheduler.py
```

For each one that is missing, add it to the import block at the top of the file alongside the existing `import os` / `import sys` lines.

- [ ] **Step 5: Replace the old polling loop**

Replace `scheduler.py:2801-2806` — currently:

```python
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)   # check every 30 seconds
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
```

with:

```python
    if block:
        install_signal_handlers()
        try:
            run_loop()
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user.")
```

- [ ] **Step 6: Make start_scheduler support non-blocking start**

Change the signature at `scheduler.py:2728` from `def start_scheduler():` to:

```python
def start_scheduler(block: bool = True):
```

And add to its docstring, after the existing job list:

```
    Args:
        block: when True (default, used by --schedule) install signal handlers
            and run the polling loop until shutdown.  When False (used by
            --serve) register jobs and return, leaving the caller to run
            run_loop() on a thread of its choosing.
```

- [ ] **Step 7: Run the lifecycle tests to verify they pass**

```bash
python -m pytest tests/test_scheduler_lifecycle.py -q
```

Expected: `8 passed`

- [ ] **Step 8: Run the full suite to verify no regression**

```bash
python -m pytest tests/ -q
```

Expected: `793 passed` (785 existing + 8 new). Any failure here is a regression — fix before committing.

- [ ] **Step 9: Commit**

```bash
git add scheduler.py tests/test_scheduler_lifecycle.py
git commit -m "feat: graceful shutdown and liveness heartbeat for scheduler

docker stop sends SIGTERM; the old 'while True / time.sleep(30)' loop would
die mid-job. Wait on an Event instead so SIGTERM drains between jobs.

Stamp a heartbeat each tick so /healthz can distinguish a live scheduler
thread from a dead one — in --serve mode uvicorn keeps answering even after
the scheduler thread dies.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 3: Minimal API with /healthz

Phase 1 ships exactly one endpoint. It proves the FastAPI app loads and that liveness reflects real scheduler state. Phase 2 adds the other eleven.

**Files:**
- Create: `api.py`
- Test: `tests/test_api_health.py`

**Interfaces:**
- Consumes: `scheduler.scheduler_alive()` from Task 2
- Produces: `api.app` — the ASGI application, consumed by `main.py` in Task 4

- [ ] **Step 1: Write the failing tests**

Create `tests/test_api_health.py`:

```python
"""Tests for the /healthz liveness probe."""

import sys
import os
from unittest import mock

import pytest
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
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
python -m pytest tests/test_api_health.py -q
```

Expected: FAIL with `ModuleNotFoundError: No module named 'api'`

- [ ] **Step 3: Create api.py**

```python
"""
api.py — HTTP interface to the options-trader daemon
=====================================================
Runs on uvicorn's main thread while the scheduler polls on a background
thread (see main.py --serve).

Phase 1 exposes only /healthz.  Every endpoint added later requires a bearer
token; /healthz stays open because Cloud Monitoring's uptime check cannot
send one, and it returns a bare status code so an unauthenticated caller
learns nothing about trading schedules or system state.
"""

import logging

from fastapi import FastAPI, Response

from scheduler import scheduler_alive

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Options Trader",
    docs_url=None,      # no interactive docs — this is a private single-user API
    redoc_url=None,
    openapi_url=None,
)


@app.get("/healthz")
def healthz() -> Response:
    """Liveness probe. 200 if the scheduler loop is ticking, 503 if not.

    Deliberately returns no body: this is the one unauthenticated endpoint.
    """
    return Response(status_code=200 if scheduler_alive() else 503)
```

- [ ] **Step 4: Install the web dependencies locally**

```bash
/Users/ambrish/miniconda3/bin/python3 -m pip install -q fastapi "uvicorn[standard]" httpx
```

`httpx` is required by `fastapi.testclient`. It is a test-only dependency and does not belong in `requirements.lock`.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
python -m pytest tests/test_api_health.py -q
```

Expected: `4 passed`

- [ ] **Step 6: Run the full suite**

```bash
python -m pytest tests/ -q
```

Expected: `797 passed`

- [ ] **Step 7: Commit**

```bash
git add api.py tests/test_api_health.py
git commit -m "feat: add minimal FastAPI app with /healthz liveness probe

Returns 200 when the scheduler loop is ticking, 503 when it is not, with an
empty body — this is the only unauthenticated endpoint and must reveal
nothing about trading schedules or system state.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 4: Wire the --serve flag

**Files:**
- Modify: `main.py` — new `cmd_serve()` near `cmd_schedule()` (line ~1368), new argument (line ~1551), `primary_flags` entry (line ~1723), dispatch branch (line ~2030)
- Test: `tests/test_serve_flag.py`

**Interfaces:**
- Consumes: `scheduler.start_scheduler(block=False)`, `scheduler.run_loop`, `scheduler.install_signal_handlers`, `api.app` from Tasks 2 and 3
- Produces: `python main.py --serve` — the container entrypoint used by `Dockerfile` in Task 5

- [ ] **Step 1: Write the failing test**

Create `tests/test_serve_flag.py`:

```python
"""Tests for the --serve entrypoint wiring."""

import sys
import os
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import main


class TestServeFlag:

    def test_serve_flag_is_accepted(self):
        parser = main.build_parser()
        args = parser.parse_args(["--serve"])
        assert args.serve is True

    def test_cmd_serve_starts_scheduler_non_blocking_then_serves(self):
        """Jobs must be registered before uvicorn takes the main thread."""
        calls = []

        with mock.patch("main.check_env"), \
             mock.patch("scheduler.start_scheduler",
                        side_effect=lambda block: calls.append(("start", block))), \
             mock.patch("scheduler.install_signal_handlers",
                        side_effect=lambda: calls.append(("signals",))), \
             mock.patch("threading.Thread") as mock_thread, \
             mock.patch("uvicorn.run", side_effect=lambda *a, **k: calls.append(("uvicorn",))):
            main.cmd_serve()

        assert ("start", False) in calls, "scheduler must be started with block=False"
        assert ("signals",) in calls, "signal handlers must be installed on the main thread"
        assert ("uvicorn",) in calls, "uvicorn must be started"
        assert calls.index(("start", False)) < calls.index(("uvicorn",)), \
            "jobs must be registered before uvicorn blocks the main thread"
        mock_thread.assert_called_once()
        assert mock_thread.call_args.kwargs.get("daemon") is True, \
            "scheduler thread must be a daemon so it cannot block process exit"
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
python -m pytest tests/test_serve_flag.py -q
```

Expected: FAIL with `AttributeError: module 'main' has no attribute 'build_parser'`

- [ ] **Step 3: Extract the parser so it is testable**

`main.py` currently builds its parser inline inside `main()` at line ~1375. Extract it: change the line `def main():` to:

```python
def build_parser() -> argparse.ArgumentParser:
```

Then at the end of the parser-construction section — immediately before the line that reads `args = parser.parse_args()` — insert:

```python
    return parser


def main():
    parser = build_parser()
```

The body from `args = parser.parse_args()` onward stays in `main()` unchanged.

- [ ] **Step 4: Add the --serve argument**

After the `--schedule` line (`main.py:1551`):

```python
    group.add_argument("--serve",          action="store_true",  help="Start scheduler + HTTP API (container entrypoint)")
```

- [ ] **Step 5: Register --serve as a primary command**

In the `primary_flags` list (`main.py:1723`), change:

```python
        args.pull_portfolio, args.status, args.schedule,
```

to:

```python
        args.pull_portfolio, args.status, args.schedule, args.serve,
```

- [ ] **Step 6: Add cmd_serve()**

Immediately after `cmd_schedule()` (`main.py:1371`):

```python
def cmd_serve():
    """Container entrypoint: scheduler on a background thread, uvicorn on the main thread.

    One process, two responsibilities.  Signal handlers are installed here on
    the main thread (the signal module requires it) so SIGTERM from
    `docker stop` reaches the scheduler loop and drains the current job.
    """
    import threading
    import uvicorn
    import scheduler

    check_env()

    # Register jobs without entering the polling loop.
    scheduler.start_scheduler(block=False)

    # Main thread owns signals; the loop only reads the shutdown Event.
    scheduler.install_signal_handlers()

    threading.Thread(target=scheduler.run_loop, name="scheduler", daemon=True).start()

    from api import app
    uvicorn.run(app, host="0.0.0.0", port=8080, log_config=None)
```

`log_config=None` keeps uvicorn from replacing the application's logging setup.

- [ ] **Step 7: Add the dispatch branch**

After the `elif args.schedule:` branch (`main.py:2030-2031`):

```python
    elif args.serve:
        cmd_serve()
```

- [ ] **Step 8: Run the tests to verify they pass**

```bash
python -m pytest tests/test_serve_flag.py -q
```

Expected: `2 passed`

- [ ] **Step 9: Verify the server actually starts**

```bash
python main.py --serve &
sleep 8
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8080/healthz
kill %1
```

Expected: `200`. A `503` means the scheduler thread did not tick — check the logs. Connection refused means uvicorn failed to bind; read the traceback.

- [ ] **Step 10: Run the full suite and commit**

```bash
python -m pytest tests/ -q
```

Expected: `799 passed`

```bash
git add main.py tests/test_serve_flag.py
git commit -m "feat: add --serve flag running scheduler and API in one process

Container entrypoint. Scheduler jobs register first, then the loop runs on a
daemon thread while uvicorn takes the main thread. Signal handlers install on
the main thread because the signal module requires it.

Extracts build_parser() from main() so argument parsing is testable.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 5: Dockerfile

**Files:**
- Create: `Dockerfile`, `.dockerignore`

**Interfaces:**
- Consumes: `requirements.lock` (Task 1), `python main.py --serve` (Task 4)
- Produces: a local image tagged `options-trader:dev`, consumed by `docker-compose.yaml` in Task 6

- [ ] **Step 1: Create .dockerignore**

Keeps secrets and local state out of the build context — `.env` in an image layer would be a credential leak.

```
.git
.gitignore
.env
.env.*
*.pyc
__pycache__/
.pytest_cache/
venv/
.venv/
logs/
snapshots/
cache/
recommendations/
scheduler.pid
docs/
.claude/
```

- [ ] **Step 2: Create the Dockerfile**

```dockerfile
# Options Trader — application container
#
# Python 3.13 matches the laptop (3.13.9) so the fixture-replay parity test in
# Phase 5 compares like with like.
FROM python:3.13-slim

# tini reaps zombies from gsutil subprocess calls; without it a long-running
# container accumulates defunct processes.
# google-cloud-cli provides gsutil for strategy fetch and GCS sync (Phase 2).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg tini \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
        | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
        > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies before source so code edits do not invalidate the dependency layer.
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

COPY . .

# /data is the persistent disk mount (snapshots, logs, cache, recommendations, tokens)
ENV PYTHONUNBUFFERED=1
EXPOSE 8080
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "main.py", "--serve"]
```

`PYTHONUNBUFFERED=1` matters: without it Python buffers stdout and Cloud Logging receives logs in delayed chunks, which is painful mid-incident.

- [ ] **Step 3: Build the image**

```bash
docker build -t options-trader:dev .
```

Expected: `Successfully tagged options-trader:dev`. Build takes 3–5 minutes on first run (the gcloud SDK is large).

If a package fails to compile, note which one — Task 1 Step 4 should have caught it. Remove any conda-only line the lock still carries and rebuild.

- [ ] **Step 4: Verify the image's Python version and imports**

```bash
docker run --rm options-trader:dev python --version
docker run --rm options-trader:dev python -c "import scipy, pandas, yfinance, robin_stocks, fastapi; print('IMPORTS OK')"
docker run --rm options-trader:dev gsutil version | head -1
```

Expected: `Python 3.13.x`, then `IMPORTS OK`, then a gsutil version line.

- [ ] **Step 5: Verify no secrets landed in the image**

```bash
docker run --rm options-trader:dev sh -c 'ls -la /app/.env 2>&1 | head -1'
```

Expected: `No such file or directory`. If `.env` is present, `.dockerignore` is not being applied — fix it and rebuild before continuing.

- [ ] **Step 6: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "feat: add Dockerfile for Python 3.13 app container

Matches the laptop's 3.13.9 so Phase 5 replay parity compares like with like.
tini as PID 1 reaps gsutil zombies; gcloud SDK included for Phase 2 GCS sync.
.dockerignore keeps .env and local state out of the build context.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 6: Compose stack and end-to-end local verification

Proves the full runtime shape — two containers, TLS termination, graceful shutdown — before any GCP resource exists.

**Files:**
- Create: `docker-compose.yaml`, `deploy/Caddyfile`, `.env.example` additions

**Interfaces:**
- Consumes: `options-trader:dev` image (Task 5)
- Produces: a running local stack; the compose file is reused unchanged on the VM in Phase 4 apart from the `image:` reference

- [ ] **Step 1: Create deploy/Caddyfile**

```
# Local development: no TLS, no domain — plain proxy on :80.
# Phase 4 replaces this with the production Caddyfile:
#     trader.piventure.com {
#         reverse_proxy app:8080
#     }
:80 {
    reverse_proxy app:8080
}
```

Phase 1 runs Caddy without TLS because Let's Encrypt cannot issue a certificate for `localhost`. The production three-line config lands in Phase 4 when DNS exists.

- [ ] **Step 2: Create docker-compose.yaml**

```yaml
services:
  caddy:
    image: caddy:2-alpine
    ports: ["8000:80"]
    volumes:
      - ./deploy/Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy_certs:/data
    restart: unless-stopped
    depends_on: [app]

  app:
    image: options-trader:dev
    build: .
    expose: ["8080"]
    volumes:
      - ./local-data:/data
    environment:
      TZ: America/New_York
      XDG_CACHE_HOME: /data/cache
      INSTANCE_ID: local-dev      # Phase 2 control-flag identity; never defaulted in code
    env_file:
      - .env
    restart: unless-stopped
    stop_grace_period: 120s
    mem_limit: 1536m

volumes:
  caddy_certs:
```

Local differences from production, all revisited in Phase 4: `build: .` with a local image tag instead of Artifact Registry, host port 8000 instead of 443, `./local-data` instead of `/srv/trader-data`, and `.env` instead of `/srv/trader-secrets/.env`.

- [ ] **Step 3: Prepare the local data directory**

```bash
mkdir -p local-data/{snapshots,logs,cache,recommendations,tokens}
echo "local-data/" >> .gitignore
```

- [ ] **Step 4: Start the stack**

```bash
docker compose up -d
sleep 15
docker compose ps
```

Expected: both `caddy` and `app` show `Up`. If `app` is restarting, run `docker compose logs app` — a missing env var from `.env` is the most likely cause.

- [ ] **Step 5: Verify liveness through Caddy**

```bash
curl -s -o /dev/null -w 'through caddy: %{http_code}\n' http://localhost:8000/healthz
```

Expected: `through caddy: 200`. This proves the full path — Caddy reverse-proxies to the app container, the app's scheduler thread is ticking, and the heartbeat is fresh.

- [ ] **Step 6: Verify the scheduler registered its jobs**

```bash
docker compose logs app | grep -E "Scheduler running|Portfolio pull|Market check" | head -5
```

Expected: lines showing registered jobs and their times, matching what the laptop logs at startup.

- [ ] **Step 7: Verify graceful shutdown**

This is the behaviour Task 2 exists to deliver — confirm it end to end.

```bash
time docker compose stop app
docker compose logs app | tail -3
```

Expected: `docker compose stop` returns in **under 10 seconds** (not the full 120s grace period), and the logs end with `Scheduler loop exited cleanly.`

If it takes the full 120 seconds, SIGTERM is not reaching the loop — verify `install_signal_handlers()` is called on the main thread in `cmd_serve()` and that tini is PID 1 (`docker compose exec app ps -p 1`).

- [ ] **Step 8: Tear down and commit**

```bash
docker compose down
git add docker-compose.yaml deploy/Caddyfile .gitignore
git commit -m "feat: add docker-compose stack with Caddy sidecar

Two containers: app (scheduler + API, one process) and Caddy (TLS termination).
Local config uses plain HTTP on :8000 since Let's Encrypt cannot issue for
localhost; Phase 4 swaps in the production Caddyfile and Artifact Registry image.

Verified end to end: liveness through Caddy returns 200, jobs register, and
SIGTERM drains in under 10s rather than hitting the 120s grace period.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 7: Open the pull request

- [ ] **Step 1: Run the full suite one final time**

```bash
python -m pytest tests/ -q
```

Expected: `799 passed`

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/cloud-phase1-containerize
gh pr create --title "Cloud Phase 1: containerize scheduler + minimal API" --body "$(cat <<'EOF'
## What

Phase 1 of the cloud hosting migration — packages the existing scheduler and a
minimal web API into a Docker container. No GCP resources involved; this proves
the runtime architecture locally first.

- `requirements.lock` — exact pins generated from the working environment
  (closes the PRD's dependency-drift watch item; pandas had already drifted
  from `>=2.2.0` to 3.0.1)
- `scheduler.py` — graceful shutdown via `threading.Event` and a liveness
  heartbeat
- `api.py` — FastAPI app with `/healthz` only; the other eleven endpoints
  land in Phase 2
- `main.py` — `--serve` flag; `build_parser()` extracted so parsing is testable
- `Dockerfile` — Python 3.13 (matches the laptop's 3.13.9), tini as PID 1
- `docker-compose.yaml` + `deploy/Caddyfile` — app + Caddy sidecar

## Why Python 3.13

The scheduler already runs on 3.13.9 with all 785 tests passing. Matching it in
the container keeps Phase 5's fixture-replay parity test meaningful — it cannot
prove environment equivalence across different interpreter minor versions.

## Test plan

- 14 new tests (8 lifecycle, 4 health, 2 serve wiring); full suite 799 passed
- `docker build` succeeds; image runs Python 3.13 and imports all deps
- `.env` confirmed absent from the image
- Liveness through Caddy returns 200
- `docker compose stop` drains in under 10s and logs a clean loop exit

## Not in this phase

Control flag, GCS sync, remaining API endpoints, and all GCP provisioning.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Definition of Done

Phase 1 is complete when all of these hold:

1. `python -m pytest tests/ -q` reports 799 passed
2. `docker build -t options-trader:dev .` succeeds
3. `docker run --rm options-trader:dev python --version` reports 3.13.x
4. `.env` is absent from the image
5. `curl http://localhost:8000/healthz` returns 200 with the stack up
6. `docker compose stop app` completes in under 10 seconds with a clean-exit log line
7. PR opened and green

## Explicitly not in Phase 1

`resolve_mode()` and the control flag (Phase 2 — needs GCS), the remaining eleven API endpoints (Phase 2), GCS strategy fetch and periodic sync (Phase 2), `deploy/entrypoint.sh` (Phase 3 — it pulls from Secret Manager, which does not exist yet; local runs read `.env` directly), all GCP resources (Phase 3), the production Caddyfile with TLS (Phase 4), and the replay fixture (Phase 5).

### Deliberate scope deviation from the PRD

PRD Phase 1 says "verify locally: scheduler fires, **emails send**, SIGTERM shuts down cleanly." This plan verifies the first and third but **not** email delivery, because doing so requires authenticating to Robinhood from the container.

That first authentication from a new environment triggers Robinhood's device-approval flow — the PRD's own §5b calls this "the one manual step in the whole system" and schedules it for Phase 4 on the VM. Burning a device approval on a laptop container proves nothing about the VM and risks a live pipeline run against the real account before any dry-run guard is in place.

Email delivery is therefore verified in **Phase 4**, where the PRD already schedules the Robinhood login and an end-to-end dry-run. Phase 1's container test stays limited to process lifecycle: it starts, serves, registers jobs, and drains cleanly — none of which touches the brokerage.
