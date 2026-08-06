"""Unit tests for the SPNEGO auth factory (clients/spnego.py) and the CMPool
downstream-client factories.

These tests are written to be CI-safe: the base install has no `httpx-gssapi`,
so the "missing extra" path is the realistic default. The path that builds a
real HTTPSPNEGOAuth is guarded by importorskip so it only runs where the
`[kerberos]` extra is installed.
"""
from __future__ import annotations

import sys

import pytest

from cdp_mcp.clients import spnego
from cdp_mcp.clients.errors import SpnegoConfigError
from cdp_mcp.cm_pool import CMPool, ServiceEndpoints
from cdp_mcp.config import ServerSettings


@pytest.fixture(autouse=True)
def _reset_spnego_cache():
    spnego.reset_cache()
    yield
    spnego.reset_cache()


def _hide_httpx_gssapi(monkeypatch):
    """Make `from httpx_gssapi import ...` raise ImportError."""
    monkeypatch.setitem(sys.modules, "httpx_gssapi", None)


# ── build_spnego_auth ─────────────────────────────────────────────────────────


def test_build_spnego_auth_disabled_returns_none():
    assert spnego.build_spnego_auth(kerberos=False) is None


def test_build_spnego_auth_enabled_but_extra_missing_raises_typed(monkeypatch):
    _hide_httpx_gssapi(monkeypatch)
    with pytest.raises(SpnegoConfigError) as exc_info:
        spnego.build_spnego_auth(kerberos=True)
    assert "httpx-gssapi" in str(exc_info.value)
    assert "kerberos" in str(exc_info.value).lower()


def test_build_spnego_auth_enabled_with_extra_builds_auth():
    pytest.importorskip("httpx_gssapi")
    auth = spnego.build_spnego_auth(kerberos=True)
    assert auth is not None
    # Cached: second call returns the same instance.
    assert spnego.build_spnego_auth(kerberos=True) is auth


def test_build_spnego_auth_caches_built_auth():
    """When the extra is present, the auth is built once and cached."""
    pytest.importorskip("httpx_gssapi")
    auth = spnego.build_spnego_auth(kerberos=True)
    assert spnego.build_spnego_auth(kerberos=True) is auth


# ── CMPool downstream client factories ────────────────────────────────────────


def _make_pool() -> CMPool:
    pool = CMPool([], ServerSettings())
    return pool


def test_factory_returns_none_when_no_endpoint():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints()  # no hdfs_nn_url
    assert pool.get_hdfs_client("C") is None
    assert pool.get_yarn_client("C") is None
    assert pool.get_spark_client("C") is None
    assert pool.get_oozie_client("C") is None


def test_factory_kerberos_off_builds_client_without_auth():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        hdfs_nn_url="http://nn:9870",
        yarn_rm_url="http://rm:8088",
        spark_hs_url="http://hs:18088",
        oozie_url="http://oz:11000",
        kerberos=False,
    )
    assert pool.get_hdfs_client("C")._auth is None
    assert pool.get_yarn_client("C")._auth is None
    assert pool.get_spark_client("C")._auth is None
    assert pool.get_oozie_client("C")._auth is None


def test_factory_kerberos_on_without_extra_raises_spnego_config(monkeypatch):
    _hide_httpx_gssapi(monkeypatch)
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(hdfs_nn_url="http://nn:9870", kerberos=True)
    with pytest.raises(SpnegoConfigError):
        pool.get_hdfs_client("C")


def test_factory_kerberos_on_with_extra_builds_authed_client():
    pytest.importorskip("httpx_gssapi")
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(hdfs_nn_url="http://nn:9870", kerberos=True)
    client = pool.get_hdfs_client("C")
    assert client is not None
    assert client._auth is not None


def test_clients_accept_auth_kwarg():
    """The four downstream clients store the auth object passed in."""
    from cdp_mcp.clients.hdfs_client import HdfsClient
    from cdp_mcp.clients.oozie_client import OozieClient
    from cdp_mcp.clients.spark_client import SparkClient
    from cdp_mcp.clients.yarn_client import YarnClient

    sentinel = object()
    assert HdfsClient("http://x", auth=sentinel)._auth is sentinel
    assert SparkClient("http://x", auth=sentinel)._auth is sentinel
    assert OozieClient("http://x", auth=sentinel)._auth is sentinel
    # YarnClient: auth takes precedence over basic username/password.
    assert YarnClient("http://x", username="u", password="p", auth=sentinel)._auth is sentinel
    assert YarnClient("http://x", username="u", password="p")._auth == ("u", "p")
    assert YarnClient("http://x")._auth is None