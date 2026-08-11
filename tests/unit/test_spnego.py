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


# ── build_spnego_auth — in-process keytab mode ────────────────────────────────
# Keytab mode acquires a TGT from a keytab via gssapi.Credentials(store=...) and
# hands it to HTTPSPNEGOAuth(creds=...), instead of reading the default ccache.
# The validation guards (missing principal / missing file) run before any gssapi
# import, so they are testable on the base install. The acquisition path is
# monkeypatched (no real KDC/keytab) and guarded by importorskip.


def test_keytab_mode_requires_principal():
    """keytab set without principal → typed error, before any gssapi import."""
    with pytest.raises(SpnegoConfigError) as exc_info:
        spnego.build_spnego_auth(kerberos=True, keytab="/etc/k.keytab", principal=None)
    assert "principal" in str(exc_info.value).lower()


def test_keytab_mode_missing_file_raises_typed(tmp_path):
    """keytab path that doesn't exist → typed error mentioning the path."""
    with pytest.raises(SpnegoConfigError) as exc_info:
        spnego.build_spnego_auth(
            kerberos=True,
            keytab=str(tmp_path / "does-not-exist.keytab"),
            principal="mcp-svc@REALM",
        )
    assert "keytab" in str(exc_info.value).lower()


def _patch_gssapi(monkeypatch, captured):
    """Replace gssapi + httpx_gssapi with recording fakes (extra must be installed)."""
    import gssapi
    import httpx_gssapi

    class FakeName:
        def __init__(self, principal, name_type):
            captured["principal"] = principal
            captured["name_type"] = name_type

    monkeypatch.setattr(gssapi, "Name", FakeName)
    monkeypatch.setattr(
        gssapi,
        "NameType",
        type("FakeNameType", (), {"kerberos_principal": "KRB_PRINC"}),
    )

    class FakeCreds:
        def __init__(self, **kwargs):
            captured["creds_kwargs"] = kwargs

    monkeypatch.setattr(gssapi, "Credentials", FakeCreds)

    class FakeAuth:
        def __init__(self, creds=None, **_kwargs):
            captured["auth_creds"] = creds
            self.creds = creds

    monkeypatch.setattr(httpx_gssapi, "HTTPSPNEGOAuth", FakeAuth)


def test_keytab_mode_acquires_credentials_from_keytab(monkeypatch, tmp_path):
    pytest.importorskip("httpx_gssapi")
    kt = tmp_path / "svc.keytab"
    kt.write_bytes(b"fake-keytab-bytes")

    captured: dict = {}
    _patch_gssapi(monkeypatch, captured)

    auth = spnego.build_spnego_auth(
        kerberos=True, keytab=str(kt), principal="mcp-svc@REALM"
    )

    # The auth wraps the credentials acquired from the keytab.
    assert captured["principal"] == "mcp-svc@REALM"
    assert captured["name_type"] == "KRB_PRINC"
    assert captured["creds_kwargs"]["usage"] == "initiate"
    assert captured["creds_kwargs"]["store"] == {
        "client_keytab": str(kt),
        "ccache": "MEMORY:cdp-mcp",
    }
    assert isinstance(captured["creds_kwargs"]["name"], object)  # FakeName
    assert captured["auth_creds"] is auth.creds


def test_keytab_mode_does_not_cache_across_calls(monkeypatch, tmp_path):
    """Keytab mode re-acquires fresh credentials each call (auto-renewal)."""
    pytest.importorskip("httpx_gssapi")
    kt = tmp_path / "svc.keytab"
    kt.write_bytes(b"fake")

    created: list = []

    import gssapi
    import httpx_gssapi

    monkeypatch.setattr(gssapi, "Name", lambda p, t: object())
    monkeypatch.setattr(
        gssapi,
        "NameType",
        type("FakeNameType", (), {"kerberos_principal": "KRB_PRINC"}),
    )
    monkeypatch.setattr(gssapi, "Credentials", lambda **_kw: object())

    class FakeAuth:
        def __init__(self, creds=None, **_kw):
            created.append(self)

    monkeypatch.setattr(httpx_gssapi, "HTTPSPNEGOAuth", FakeAuth)

    a1 = spnego.build_spnego_auth(kerberos=True, keytab=str(kt), principal="p@R")
    a2 = spnego.build_spnego_auth(kerberos=True, keytab=str(kt), principal="p@R")
    assert a1 is not a2
    assert len(created) == 2


def test_keytab_mode_propagates_gssapi_failure_as_typed(monkeypatch, tmp_path):
    """A gssapi acquisition failure surfaces as SpnegoConfigError, not a bare exception."""
    pytest.importorskip("httpx_gssapi")
    kt = tmp_path / "svc.keytab"
    kt.write_bytes(b"fake")

    import gssapi
    import httpx_gssapi

    monkeypatch.setattr(gssapi, "Name", lambda p, t: object())
    monkeypatch.setattr(
        gssapi,
        "NameType",
        type("FakeNameType", (), {"kerberos_principal": "KRB_PRINC"}),
    )

    def _boom(**_kw):
        raise RuntimeError("KDC unreachable")

    monkeypatch.setattr(gssapi, "Credentials", _boom)
    monkeypatch.setattr(httpx_gssapi, "HTTPSPNEGOAuth", lambda **_kw: object())

    with pytest.raises(SpnegoConfigError) as exc_info:
        spnego.build_spnego_auth(kerberos=True, keytab=str(kt), principal="p@R")
    assert "keytab" in str(exc_info.value).lower()
    assert "KDC unreachable" in str(exc_info.value)


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


def test_factory_threads_keytab_config_into_spnego(monkeypatch):
    """kerberos_keytab/kerberos_principal on ServiceEndpoints reach build_spnego_auth."""
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        hdfs_nn_url="http://nn:9870",
        kerberos=True,
        kerberos_keytab="/etc/cdp-mcp/mcp.keytab",
        kerberos_principal="mcp-svc@REALM",
    )

    captured: dict = {}

    def fake_build(kerberos, keytab=None, principal=None):
        captured.update(kerberos=kerberos, keytab=keytab, principal=principal)
        return object()  # truthy non-None auth

    monkeypatch.setattr("cdp_mcp.clients.spnego.build_spnego_auth", fake_build)

    pool.get_hdfs_client("C")
    assert captured == {
        "kerberos": True,
        "keytab": "/etc/cdp-mcp/mcp.keytab",
        "principal": "mcp-svc@REALM",
    }


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