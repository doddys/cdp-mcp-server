"""Unit tests for clients/http_fallback.py — the shared HTTPS→HTTP port
fallback helper. Exercises the fallback decision tree in isolation with
respx: HTTPS succeeds (no HTTP attempt), HTTPS ConnectError → HTTP succeeds,
both fail → enriched ServiceUnavailable, 401/404 on HTTPS → no fallback."""

from __future__ import annotations

import sys

import httpx
import pytest
import respx
import structlog

from cdp_mcp.clients.http_fallback import fetch_with_http_fallback

HTTPS = "https://nn.example.com:9871"
HTTP = "http://nn.example.com:9870"


@pytest.fixture(autouse=True)
def _restore_structlog():
    """The log-assertion tests below call ``structlog.configure`` to route
    output to capsys's stderr. That mutates global state and leaks into
    later tests (the cm_pool tests' YarnClient retries hang/break under a
    reconfigured filter). Snapshot and restore structlog's config around
    every test in this module."""
    import copy

    snapshot = copy.deepcopy(structlog.get_config())
    yield
    structlog.configure(**snapshot)


class _SvcUnavailable(Exception):
    pass


def _retry_dec():
    # Real tenacity decorator — one attempt so connection errors surface
    # immediately for the fallback decision (matches the clients' reraise=True
    # behaviour without a 3-attempt wait in tests).
    from tenacity import retry, stop_after_attempt

    return retry(stop=stop_after_attempt(1), reraise=True)


def _map_ok(resp: httpx.Response, path: str, base_url: str) -> dict:
    if resp.status_code == 401:
        if "negotiate" in resp.headers.get("www-authenticate", "").lower():
            raise PermissionError("SPNEGO required")
        raise PermissionError("auth failed")
    if resp.status_code >= 400:
        raise ValueError(f"HTTP {resp.status_code}")
    return resp.json()


@respx.mock
async def test_https_succeeds_no_http_attempt():
    """Happy path: HTTPS answers → HTTP fallback URL is never hit."""
    respx.get(f"{HTTPS}/jmx").mock(return_value=httpx.Response(200, json={"ok": True}))
    http_route = respx.get(f"{HTTP}/jmx").mock(return_value=httpx.Response(200, json={"wrong": True}))
    result = await fetch_with_http_fallback(
        primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
        auth=None, timeout=5, retry_dec=_retry_dec,
        map_response=_map_ok, service_unavailable=_SvcUnavailable,
        service_label="NameNode JMX",
    )
    assert result == {"ok": True}
    assert http_route.calls.call_count == 0


@respx.mock
async def test_https_connect_error_falls_back_to_http():
    """HTTPS port has no listener (ConnectError) → fall back to HTTP, succeed."""
    respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ConnectError("connection refused"))
    respx.get(f"{HTTP}/jmx").mock(return_value=httpx.Response(200, json={"ok": True}))
    result = await fetch_with_http_fallback(
        primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
        auth=None, timeout=5, retry_dec=_retry_dec,
        map_response=_map_ok, service_unavailable=_SvcUnavailable,
        service_label="NameNode JMX",
    )
    assert result == {"ok": True}


@respx.mock
async def test_both_fail_enriched_message_names_both_urls():
    """HTTPS and HTTP both ConnectError → ServiceUnavailable naming both."""
    respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ConnectError("no route"))
    respx.get(f"{HTTP}/jmx").mock(side_effect=httpx.ConnectError("no route"))
    with pytest.raises(_SvcUnavailable) as exc:
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    msg = str(exc.value)
    assert "NameNode JMX unreachable" in msg
    assert HTTPS in msg and HTTP in msg
    assert "ConnectError" in msg


@respx.mock
async def test_http_401_on_https_no_fallback():
    """401 means the port answered (auth issue) — must NOT fall back to HTTP."""
    http_route = respx.get(f"{HTTP}/jmx").mock(return_value=httpx.Response(200, json={"wrong": True}))
    respx.get(f"{HTTPS}/jmx").mock(
        return_value=httpx.Response(401, headers={"www-authenticate": "Negotiate"})
    )
    with pytest.raises(PermissionError, match="SPNEGO required"):
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    assert http_route.calls.call_count == 0


@respx.mock
async def test_http_404_on_https_no_fallback():
    """404 means the port answered — must NOT fall back."""
    http_route = respx.get(f"{HTTP}/jmx").mock(return_value=httpx.Response(200, json={"wrong": True}))
    respx.get(f"{HTTPS}/jmx").mock(return_value=httpx.Response(404))
    with pytest.raises(ValueError, match="HTTP 404"):
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    assert http_route.calls.call_count == 0


@respx.mock
async def test_no_fallback_url_single_scheme():
    """fallback_url=None → connection error re-raised as-is (no fallback)."""
    route = respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(httpx.ConnectError):
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=None, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    # Only the primary attempt — no fallback to try.
    assert route.calls.call_count == 1


@respx.mock
async def test_fallback_equals_primary_no_double_attempt():
    """When the primary is already HTTP (HTTPS not configured), the fallback
    URL equals the primary — don't retry the same URL; re-raise the original
    rather than producing a 'tried X twice' message."""
    route = respx.get(f"{HTTP}/jmx").mock(side_effect=httpx.ConnectError("refused"))
    with pytest.raises(httpx.ConnectError):
        await fetch_with_http_fallback(
            primary_url=HTTP, fallback_url=HTTP, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    # Only the one primary attempt — no duplicate fallback call.
    assert route.calls.call_count == 1


@respx.mock
async def test_read_timeout_triggers_fallback():
    """ReadTimeout (half-open connection, no bytes) is in the fallback family."""
    respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ReadTimeout("stalled"))
    respx.get(f"{HTTP}/jmx").mock(return_value=httpx.Response(200, json={"ok": True}))
    result = await fetch_with_http_fallback(
        primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
        auth=None, timeout=5, retry_dec=_retry_dec,
        map_response=_map_ok, service_unavailable=_SvcUnavailable,
        service_label="NameNode JMX",
    )
    assert result == {"ok": True}


# ── debug logging: the "identify the cause on the next run" surface ────────────
# structlog's PrintLoggerFactory writes to stderr directly (bypassing stdlib
# logging), so pytest's `caplog` doesn't capture it — we assert on stderr via
# capsys instead.


@respx.mock
async def test_no_fallback_failure_emits_warning_log(capsys):
    """A primary failure with no fallback (override / single-scheme) emits a
    WARNING naming the URL + error + reason — visible at default log level so
    the operator can see why the ConnectError surfaced without DEBUG."""
    import logging

    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )
    respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ConnectError("connection refused"))
    with pytest.raises(httpx.ConnectError):
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=None, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    err = capsys.readouterr().err
    assert "downstream.fetch_failed_no_fallback" in err
    assert HTTPS in err
    assert "no_fallback_configured" in err


@respx.mock
async def test_both_fail_emits_warning_log(capsys):
    """Both URLs failing emits a WARNING naming both URLs + errors (in addition
    to the enriched ServiceUnavailable message)."""
    import logging

    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )
    respx.get(f"{HTTPS}/jmx").mock(side_effect=httpx.ConnectError("no route"))
    respx.get(f"{HTTP}/jmx").mock(side_effect=httpx.ConnectError("no route"))
    with pytest.raises(_SvcUnavailable):
        await fetch_with_http_fallback(
            primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
            auth=None, timeout=5, retry_dec=_retry_dec,
            map_response=_map_ok, service_unavailable=_SvcUnavailable,
            service_label="NameNode JMX",
        )
    err = capsys.readouterr().err
    assert "downstream.fetch_failed_both_urls" in err
    assert HTTPS in err and HTTP in err


@respx.mock
async def test_attempt_emits_debug_log(capsys):
    """Each attempt emits a DEBUG log naming the URL + path + whether a
    fallback is available — surfaces with LOG_LEVEL=DEBUG."""
    import logging

    import structlog

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )
    respx.get(f"{HTTPS}/jmx").mock(return_value=httpx.Response(200, json={"ok": True}))
    await fetch_with_http_fallback(
        primary_url=HTTPS, fallback_url=HTTP, path="/jmx", params=None,
        auth=None, timeout=5, retry_dec=_retry_dec,
        map_response=_map_ok, service_unavailable=_SvcUnavailable,
        service_label="NameNode JMX",
    )
    err = capsys.readouterr().err
    assert "downstream.fetch_attempt" in err
    assert HTTPS in err
    assert "fallback_available=True" in err
    assert "downstream.fetch_ok" in err