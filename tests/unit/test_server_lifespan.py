"""Unit tests for cdp_mcp.server._lifespan session ref-counting.

Regression coverage for a production crash: the mcp library's
StreamableHTTPSessionManager invokes the FastMCP lifespan once per *session*,
not once per process, so concurrent sessions must share one CMPool/registry
instead of racing to build/tear it down.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

import cdp_mcp.server as server


@pytest.fixture(autouse=True)
def _reset_lifespan_globals():
    server._pool = None
    server._registry = None
    server._active_sessions = 0
    yield
    server._pool = None
    server._registry = None
    server._active_sessions = 0


def _install_fakes(monkeypatch):
    fake_registry = MagicMock()
    fake_registry.get_all.return_value = []

    fake_pool = MagicMock()
    fake_pool.start = AsyncMock()
    fake_pool.stop = AsyncMock()

    monkeypatch.setattr(server, "build_registry", MagicMock(return_value=fake_registry))
    monkeypatch.setattr(server, "CMPool", MagicMock(return_value=fake_pool))
    return fake_registry, fake_pool


async def _run_session(entered: asyncio.Event, release: asyncio.Event):
    async with server._lifespan(None):
        entered.set()
        await release.wait()


@pytest.mark.asyncio
async def test_concurrent_sessions_share_one_pool_and_stop_only_when_last_exits(monkeypatch):
    fake_registry, fake_pool = _install_fakes(monkeypatch)

    entered_a, entered_b = asyncio.Event(), asyncio.Event()
    release_a, release_b = asyncio.Event(), asyncio.Event()

    task_a = asyncio.create_task(_run_session(entered_a, release_a))
    await entered_a.wait()

    task_b = asyncio.create_task(_run_session(entered_b, release_b))
    await entered_b.wait()

    # Pool/registry built exactly once, shared by both overlapping sessions.
    server.build_registry.assert_called_once()
    server.CMPool.assert_called_once()
    fake_pool.start.assert_awaited_once()
    assert server._active_sessions == 2
    fake_pool.stop.assert_not_awaited()

    release_a.set()
    await task_a

    # One session is still active: the shared pool must stay up.
    assert server._active_sessions == 1
    fake_pool.stop.assert_not_awaited()

    release_b.set()
    await task_b

    fake_pool.stop.assert_awaited_once()
    fake_registry.stop.assert_called_once()
    assert server._active_sessions == 0
    assert server._pool is None
    assert server._registry is None


@pytest.mark.asyncio
async def test_failed_startup_does_not_poison_pool_for_next_session(monkeypatch):
    fake_registry = MagicMock()
    fake_registry.get_all.return_value = []

    failing_pool = MagicMock()
    failing_pool.start = AsyncMock(side_effect=RuntimeError("cm unreachable"))

    monkeypatch.setattr(server, "build_registry", MagicMock(return_value=fake_registry))
    monkeypatch.setattr(server, "CMPool", MagicMock(return_value=failing_pool))

    with pytest.raises(RuntimeError, match="cm unreachable"):
        async with server._lifespan(None):
            pass

    # Globals must be reset, not left pointing at the broken instances,
    # otherwise every later session would silently reuse a dead pool
    # instead of retrying startup.
    assert server._pool is None
    assert server._registry is None
    assert server._active_sessions == 0

    # A subsequent session must retry startup from scratch (and can succeed).
    working_pool = MagicMock()
    working_pool.start = AsyncMock()
    working_pool.stop = AsyncMock()
    monkeypatch.setattr(server, "CMPool", MagicMock(return_value=working_pool))

    async with server._lifespan(None):
        working_pool.start.assert_awaited_once()

    working_pool.stop.assert_awaited_once()
