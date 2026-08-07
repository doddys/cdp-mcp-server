"""Unit tests for the MCP transport selection (server._resolve_transport_settings).

These cover only the env-var resolution/dispatch logic — `run()` itself blocks
(runs the FastMCP server) and is not exercised here.
"""
from __future__ import annotations

import pytest

from cdp_mcp.server import _resolve_transport_settings


def test_defaults_to_stdio():
    transport, host, port = _resolve_transport_settings({})
    assert transport == "stdio"
    assert host == "127.0.0.1"
    assert port == 8000


def test_streamable_http_with_host_port():
    transport, host, port = _resolve_transport_settings(
        {"MCP_TRANSPORT": "streamable-http", "MCP_HOST": "0.0.0.0", "MCP_PORT": "9000"}
    )
    assert transport == "streamable-http"
    assert host == "0.0.0.0"
    assert port == 9000


def test_transport_is_case_insensitive_and_trimmed():
    transport, _, _ = _resolve_transport_settings({"MCP_TRANSPORT": "  STREAMABLE-HTTP  "})
    assert transport == "streamable-http"


def test_sse_transport_accepted():
    transport, _, _ = _resolve_transport_settings({"MCP_TRANSPORT": "sse"})
    assert transport == "sse"


def test_empty_transport_falls_back_to_stdio():
    transport, _, _ = _resolve_transport_settings({"MCP_TRANSPORT": ""})
    assert transport == "stdio"


def test_invalid_transport_raises():
    with pytest.raises(RuntimeError, match="Unsupported MCP_TRANSPORT"):
        _resolve_transport_settings({"MCP_TRANSPORT": "websocket"})


def test_invalid_port_raises():
    with pytest.raises(RuntimeError, match="Invalid MCP_PORT"):
        _resolve_transport_settings({"MCP_TRANSPORT": "streamable-http", "MCP_PORT": "not-an-int"})


def test_empty_host_falls_back_to_loopback():
    _, host, _ = _resolve_transport_settings({"MCP_HOST": "   "})
    assert host == "127.0.0.1"


def test_reads_live_environment(monkeypatch):
    """The helper reads os.environ when no explicit env is passed."""
    monkeypatch.setenv("MCP_TRANSPORT", "streamable-http")
    monkeypatch.setenv("MCP_HOST", "10.0.0.1")
    monkeypatch.setenv("MCP_PORT", "8080")
    transport, host, port = _resolve_transport_settings()
    assert (transport, host, port) == ("streamable-http", "10.0.0.1", 8080)