"""The robin_stocks session must never issue an untimed request.

robin_stocks routes every call through a module-level requests.Session and
passes no timeout, so a read blocks forever.  Twice in four days that turned an
ordinary network interruption into a multi-hour scheduler outage: the laptop
slept mid-request, the connection died with the network, and the read never
returned.  The watchdog meant to catch it is a threading.Timer, which does not
count down while the machine is asleep — so it was defeated by the same
condition that caused the hang, once taking 8.5 hours to fire.
"""
import requests

import auth


class _Recorder:
    """Stands in for Session.request and records the kwargs it was handed."""

    def __init__(self):
        self.calls = []

    def __call__(self, method, url, **kwargs):
        self.calls.append(kwargs)
        return "sentinel"


def _session_with(recorder, connect=10.0, read=30.0):
    s = requests.Session()
    s.request = recorder
    auth._install_default_timeouts(s, connect, read)
    return s


class TestDefaultTimeoutIsApplied:
    def test_untimed_call_gets_the_default(self):
        rec = _Recorder()
        _session_with(rec).get("https://api.robinhood.com/positions/")
        assert rec.calls[0]["timeout"] == (10.0, 30.0)

    def test_post_is_covered_too(self):
        """get() and post() both delegate to request(), so one wrapper covers both."""
        rec = _Recorder()
        _session_with(rec).post("https://api.robinhood.com/orders/", data={})
        assert rec.calls[0]["timeout"] == (10.0, 30.0)

    def test_explicit_timeout_from_caller_wins(self):
        rec = _Recorder()
        _session_with(rec).get("https://x/", timeout=5)
        assert rec.calls[0]["timeout"] == 5

    def test_explicit_none_is_replaced(self):
        """requests reads timeout=None as 'block forever' — the exact bug."""
        rec = _Recorder()
        _session_with(rec).get("https://x/", timeout=None)
        assert rec.calls[0]["timeout"] == (10.0, 30.0)

    def test_return_value_is_passed_through(self):
        rec = _Recorder()
        assert _session_with(rec).get("https://x/") == "sentinel"


class TestInstallIsIdempotent:
    def test_second_install_is_a_noop(self):
        """login() may re-enter, and the module can be imported more than once.

        Double-wrapping would still work but nests a closure per call; the flag
        makes re-installation observably a no-op.
        """
        rec = _Recorder()
        s = _session_with(rec)
        wrapper = s.request
        assert auth._install_default_timeouts(s, 1.0, 2.0) is False
        assert s.request is wrapper

    def test_first_install_reports_true(self):
        s = requests.Session()
        s.request = _Recorder()
        assert auth._install_default_timeouts(s, 1.0, 2.0) is True


class TestPositionalArgsArePassedThrough:
    def test_third_positional_arg_does_not_raise(self):
        """Session.request takes params/data/headers positionally too.

        Naming only (method, url) would turn a positional third argument into a
        TypeError — and functools.wraps sets __wrapped__, so inspect.signature()
        would still advertise the original wide signature.
        """
        rec = _Recorder()
        s = requests.Session()
        s.request = lambda *a, **k: (a, k)
        auth._install_default_timeouts(s, 10.0, 30.0)
        args, kwargs = s.request("GET", "https://x/", {"cursor": 1})
        assert args == ("GET", "https://x/", {"cursor": 1})
        assert kwargs["timeout"] == (10.0, 30.0)


class TestTimeoutFromEnv:
    """Parsed defensively: this runs at import, so a bad value must not raise.

    An uncaught ValueError here takes down every entry point that imports auth,
    the scheduler daemon included, which launchd then crash-loops.
    """

    def test_absent_uses_default(self, monkeypatch):
        monkeypatch.delenv("RH_TEST_TIMEOUT", raising=False)
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 30.0

    def test_empty_uses_default(self, monkeypatch):
        monkeypatch.setenv("RH_TEST_TIMEOUT", "   ")
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 30.0

    def test_valid_value_is_honoured(self, monkeypatch):
        monkeypatch.setenv("RH_TEST_TIMEOUT", "45.5")
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 45.5

    def test_garbage_falls_back_instead_of_raising(self, monkeypatch):
        monkeypatch.setenv("RH_TEST_TIMEOUT", "30s")
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 30.0

    def test_zero_is_rejected(self, monkeypatch):
        """requests reads 0 as immediate expiry — every call would fail instantly."""
        monkeypatch.setenv("RH_TEST_TIMEOUT", "0")
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 30.0

    def test_negative_is_rejected(self, monkeypatch):
        monkeypatch.setenv("RH_TEST_TIMEOUT", "-5")
        assert auth._timeout_from_env("RH_TEST_TIMEOUT", 30.0) == 30.0


class TestLiveSessionIsPatchedAtImport:
    def test_robin_stocks_session_has_the_wrapper(self):
        """Installed at import, so --report and --pull-portfolio are covered too."""
        import robin_stocks.robinhood.helper as helper

        assert getattr(helper.SESSION, "_trader_timeouts_installed", False) is True

    def test_module_defaults_are_sane(self):
        """Asserts the hardcoded fallbacks, not the env-derived values.

        Checking _READ_TIMEOUT_SECS would fail wherever RH_READ_TIMEOUT_SECS is
        legitimately set — a documented feature of this change — training people
        to ignore the suite.
        """
        assert 0 < auth._DEFAULT_CONNECT_TIMEOUT_SECS <= 60
        assert 0 < auth._DEFAULT_READ_TIMEOUT_SECS <= 300
