"""
http_fallback.py — shared HTTPS→HTTP port-fallback fetch helper for the
downstream service clients (HDFS NameNode JMX, YARN RM, Spark HS, Oozie).

Some CDP clusters disable HTTPS on the downstream service web UIs — only the
HTTP port is open — while CM *config* still reports the HTTPS port. CM
discovery reads that config and builds an ``https://host:<https_port>`` URL,
so every fetch hits a dead TLS listener and surfaces as "All connection
attempts failed". This helper tries the HTTPS endpoint first and, on a
connection/handshake error (the port never answered or the TLS handshake
never completed), falls back to the HTTP port on the same host.

Crucially, an HTTP *status* response (401/403/404/500/503/…) does **not**
trigger the fallback — a status means the port answered, so the issue is
auth/not-found/server, not SSL; falling back there would mask a real
Kerberos/SPNEGO misconfig with a spurious HTTP retry. Only the
connection-error family (ConnectError/ConnectTimeout/ReadTimeout) falls
back — these are "the connection/handshake never completed".

The helper is client-agnostic: each client supplies a ``map_response``
callable that maps a non-success ``httpx.Response`` to its own typed
exception (401→SpnegoRequiredError/AuthError, 404→NotFoundError, 5xx→
ServiceUnavailable, ≥400→ClientError, non-JSON→ClientError) and returns the
parsed dict on success, plus a ``service_unavailable`` factory that builds
the client's typed ``*ServiceUnavailable`` for the "both URLs failed" case
(so the enriched reason — naming both URLs and their errors — flows through
the existing ``str(exc)`` path into ``not_available`` records).

``fallback_url is None`` disables the fallback entirely (single-scheme
client) — preserves the exact prior behaviour for direct-constructed
clients and unit tests that pass only one base URL.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import structlog

log = structlog.get_logger(__name__)

# The connection/handshake-never-completed family. httpx raises an
# ``httpx.ConnectError`` (or a subclass such as ``httpx.ConnectTimeout``) for
# a refused/dropped connection or a failed TLS handshake; ``ReadTimeout`` is
# a half-open connection that stalled waiting for bytes. These are the only
# errors that mean "the endpoint didn't answer" — the fallback's trigger.
# ``httpx.RemoteProtocolError`` ("server sent a malformed response") is NOT
# here: the port answered, so the issue isn't SSL. Status responses (401/404/
# 5xx) are raised by ``map_response`` before this family is seen.
_CONNECTION_ERRORS: tuple[type[Exception], ...] = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
)


async def fetch_with_http_fallback(
    *,
    primary_url: str,
    fallback_url: str | None,
    path: str,
    params: dict[str, Any] | None,
    auth: Any,
    timeout: int,
    retry_dec: Callable[[], Any],
    map_response: Callable[[httpx.Response, str, str], dict],
    service_unavailable: Callable[[str], Exception],
    service_label: str,
) -> dict:
    """Fetch ``path`` from ``primary_url`` (HTTPS), falling back to
    ``fallback_url`` (HTTP) only on a connection/handshake error.

    ``retry_dec`` is the client's tenacity decorator builder (retries
    ``TransportError``/``*ServiceUnavailable``); it wraps each per-URL
    attempt so the existing retry semantics are preserved on the happy and
    auth-error paths. ``map_response(resp, path, base_url)`` maps a
    received response to a typed exception or returns the parsed dict.
    ``service_unavailable(msg)`` builds the client's typed
    ``*ServiceUnavailable`` carrying the enriched "both failed" message.
    """
    # Type narrowing for mypy: tenacity's retry() returns a decorator whose
    # use site is a sync callable, so we treat it as Any.
    retry_decorator = retry_dec()

    async def _attempt(base_url: str) -> dict:
        @retry_decorator  # type: ignore[misc]
        async def _execute() -> dict:
            async with httpx.AsyncClient(
                base_url=base_url,
                auth=auth,
                timeout=timeout,
                verify=False,  # internal services often self-signed
                follow_redirects=True,
            ) as client:
                try:
                    resp = await client.get(path, params=params)
                except httpx.TransportError:
                    raise  # → retry (if configured) then surface for fallback
                return map_response(resp, path, base_url)

        return await _execute()

    try:
        return await _attempt(primary_url)
    except _CONNECTION_ERRORS as primary_exc:
        # No fallback if there's no distinct HTTP URL: either the caller
        # passed none (single-scheme client) or HTTPS wasn't configured and
        # the primary is already the HTTP URL (retrying it would just produce
        # a confusing "tried X twice" message).
        if fallback_url is None or fallback_url == primary_url:
            raise
        log.info(
            "downstream.https_failed_falling_back",
            service=service_label,
            primary=primary_url,
            error=f"{type(primary_exc).__name__}: {primary_exc}",
            fallback=fallback_url,
        )
        try:
            return await _attempt(fallback_url)
        except _CONNECTION_ERRORS as fallback_exc:
            raise service_unavailable(
                f"{service_label} unreachable: tried {primary_url} "
                f"({type(primary_exc).__name__}: {primary_exc}) and "
                f"{fallback_url} ({type(fallback_exc).__name__}: {fallback_exc})"
            ) from fallback_exc