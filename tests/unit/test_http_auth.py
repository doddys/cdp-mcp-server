"""Unit tests for the shared-secret bearer-token auth middleware
(server._BearerAuthMiddleware) that gates the streamable-http endpoint when
MCP_AUTH_TOKEN is set.

These test the ASGI middleware directly (no uvicorn/server) — `run()` itself
blocks (runs uvicorn) and isn't exercised here.
"""
from __future__ import annotations

import pytest

from cdp_mcp.server import _BearerAuthMiddleware


def _http_scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict:
    return {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": headers or [],
    }


async def _no_receive() -> dict:
    return {"type": "http.request", "body": b"", "more_body": False}


def _make_inner() -> tuple[list, object]:
    """Return (calls, asgi_app) where asgi_app records invocations and 200-responds."""
    calls: list[dict] = []

    async def app(scope, receive, send):
        calls.append(scope)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    return calls, app


def _status(messages: list[dict]) -> int | None:
    for m in messages:
        if m.get("type") == "http.response.start":
            return m.get("status")
    return None


def _header(messages: list[dict], name: bytes) -> bytes | None:
    for m in messages:
        if m.get("type") == "http.response.start":
            for k, v in m.get("headers", []):
                if k.lower() == name:
                    return v
    return None


async def _run(middleware, scope):
    messages: list[dict] = []

    async def send(m):
        messages.append(m)

    await middleware(scope, _no_receive, send)
    return messages


@pytest.mark.asyncio
async def test_no_authorization_header_returns_401():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([]))
    assert calls == []                       # inner app NOT called
    assert _status(messages) == 401
    assert _header(messages, b"www-authenticate") == b"Bearer"


@pytest.mark.asyncio
async def test_wrong_token_returns_401():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"authorization", b"Bearer wrong")]))
    assert calls == []
    assert _status(messages) == 401


@pytest.mark.asyncio
async def test_correct_token_passes_through_to_inner():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"authorization", b"Bearer secret")]))
    assert len(calls) == 1                   # inner app called once
    assert _status(messages) == 200


@pytest.mark.asyncio
async def test_authorization_header_is_case_insensitive():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"Authorization", b"Bearer secret")]))
    assert len(calls) == 1
    assert _status(messages) == 200


@pytest.mark.asyncio
async def test_bearer_prefix_is_case_insensitive():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"authorization", b"bearer secret")]))
    assert len(calls) == 1
    assert _status(messages) == 200


@pytest.mark.asyncio
async def test_non_bearer_scheme_returns_401():
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"authorization", b"Basic dXNlcjpwYXNz")]))
    assert calls == []
    assert _status(messages) == 401


@pytest.mark.asyncio
async def test_non_http_scope_passes_through_unconditionally():
    """lifespan startup/shutdown must pass through even with no auth header."""
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, {"type": "lifespan", "headers": []})
    assert len(calls) == 1
    # inner's 200 response is http-shaped; the point is it was CALLED (not 401-gated)
    assert _status(messages) == 200


@pytest.mark.asyncio
async def test_token_compared_in_constant_time():
    """Smoke test that a near-miss token is still rejected (compare_digest)."""
    calls, inner = _make_inner()
    mw = _BearerAuthMiddleware(inner, "secret")
    messages = await _run(mw, _http_scope([(b"authorization", b"Bearer secre")]))
    assert calls == []
    assert _status(messages) == 401