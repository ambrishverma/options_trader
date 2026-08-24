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


class TestLiveSessionIsPatchedAtImport:
    def test_robin_stocks_session_has_the_wrapper(self):
        """Installed at import, so --report and --pull-portfolio are covered too."""
        import robin_stocks.robinhood.helper as helper

        assert getattr(helper.SESSION, "_trader_timeouts_installed", False) is True

    def test_defaults_are_sane(self):
        assert 0 < auth._CONNECT_TIMEOUT_SECS <= 60
        assert 0 < auth._READ_TIMEOUT_SECS <= 300
