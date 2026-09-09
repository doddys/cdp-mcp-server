"""Unit tests for CMPool service-endpoint discovery — verifies that the
HTTPS→HTTP port fallback URLs are populated alongside the primary URLs, and
that the client factories thread ``http_url``/``http_candidates`` through.

Discovery reads CM role configs (not live probes), so a dead HTTPS listener
isn't detectable at discovery time — but discovery must still hand the HTTP
fallback URL to the client so the request-time fallback (tested in
test_http_fallback.py / test_*_client.py) can use it.
"""
from __future__ import annotations

import pytest

from cdp_mcp.cm_pool import CMPool, ServiceEndpoints
from cdp_mcp.config import ServerSettings


def _make_pool() -> CMPool:
    return CMPool([], ServerSettings())


class _FakeCMClient:
    """Minimal async CM client stub: returns canned JSON by URL path.

    ``responses`` is a list of (matcher, json). A matcher is either a string
    (substring match) or a callable(path) -> bool. First match wins, so put
    more specific paths (role configs, which contain "/config") before the
    roles list (which ends with "/roles").
    """

    def __init__(self, responses: list[tuple[object, dict]]):
        self._responses = responses
        self.calls: list[str] = []

    async def _get(self, path: str, params: dict | None = None) -> dict:
        self.calls.append(path)
        for matcher, payload in self._responses:
            if callable(matcher):
                if matcher(path):
                    return payload
            elif matcher in path:
                return payload
        raise AssertionError(f"unexpected CM GET: {path}")


# ── Discovery populates both HTTPS and HTTP URLs ──────────────────────────────


async def _run_discovery(service: str, roles_payload: dict, config_payloads: dict[str, dict]):
    """Drive a single _discover_* method with stubbed CM responses and return eps."""
    pool = _make_pool()
    eps = ServiceEndpoints()
    # Config endpoints contain "/config" — match by their unique role suffix.
    # Roles list ends with "/roles" — match with endswith so it doesn't shadow
    # the config calls (which contain "/roles/..."). Config entries first.
    responses: list[tuple[object, dict]] = []
    for suffix, payload in config_payloads.items():
        responses.append((suffix, payload))
    responses.append((lambda p: p.endswith("/roles"), roles_payload))

    client = _FakeCMClient(responses)
    method = {
        "YARN": pool._discover_yarn,
        "SPARK": pool._discover_spark,
        "HDFS": pool._discover_hdfs,
        "OOZIE": pool._discover_oozie,
    }[service]
    service_map = {
        "YARN": "yarn-service",
        "SPARK_ON_YARN": "spark-service",
        "SPARK": "spark-service",
        "HDFS": "hdfs-service",
        "OOZIE": "oozie-service",
    }
    await method("cluster1", client, service_map, eps)
    return eps


@pytest.mark.asyncio
async def test_discover_yarn_populates_https_and_http_urls():
    """YARN RM with HTTPS port configured → primary is HTTPS, http_url is HTTP."""
    roles = {
        "items": [
            {"type": "RESOURCEMANAGER", "name": "yarn-RM-1", "hostRef": {"hostname": "rm.example.com"}}
        ]
    }
    # Config endpoint for yarn-RM-1: https_port set to 8090, http port default.
    rm_config = {
        "items": [
            {"name": "resourcemanager_webserver_https_port", "value": "8090"},
            {"name": "yarn.resourcemanager.webapp.address", "value": "0.0.0.0:8088"},
        ]
    }
    eps = await _run_discovery(
        "YARN", roles,
        config_payloads={"/roles/yarn-RM-1/config": rm_config},
    )
    assert eps.yarn_rm_url == "https://rm.example.com:8090"
    assert eps.yarn_rm_http_url == "http://rm.example.com:8088"


@pytest.mark.asyncio
async def test_discover_yarn_https_unconfigured_primary_is_http():
    """No HTTPS port configured → primary URL is HTTP, http_url mirrors it."""
    roles = {
        "items": [
            {"type": "RESOURCEMANAGER", "name": "yarn-RM-1", "hostRef": {"hostname": "rm.example.com"}}
        ]
    }
    rm_config = {
        "items": [
            {"name": "yarn.resourcemanager.webapp.address", "value": "0.0.0.0:8088"},
        ]
    }
    eps = await _run_discovery(
        "YARN", roles,
        config_payloads={"/roles/yarn-RM-1/config": rm_config},
    )
    assert eps.yarn_rm_url == "http://rm.example.com:8088"
    # http_url still set (equals primary when HTTPS not configured); the helper
    # treats fallback == primary as "no fallback" so behaviour is unchanged.
    assert eps.yarn_rm_http_url == "http://rm.example.com:8088"


@pytest.mark.asyncio
async def test_discover_hdfs_populates_https_and_http_candidates():
    """HA HDFS with two NNs, both HTTPS-configured → candidates are HTTPS,
    http_candidates are HTTP, paired 1:1 by index."""
    roles = {
        "items": [
            {"type": "NAMENODE", "name": "nn-1", "hostRef": {"hostname": "nn1.example.com"}, "healthSummary": "GOOD"},
            {"type": "NAMENODE", "name": "nn-2", "hostRef": {"hostname": "nn2.example.com"}, "healthSummary": "GOOD"},
        ]
    }
    nn1_config = {"items": [{"name": "dfs_https_port", "value": "9871"},
                            {"name": "dfs.namenode.http-address", "value": "0.0.0.0:9870"}]}
    nn2_config = {"items": [{"name": "dfs_https_port", "value": "9871"},
                            {"name": "dfs.namenode.http-address", "value": "0.0.0.0:9870"}]}
    eps = await _run_discovery(
        "HDFS", roles,
        config_payloads={"/roles/nn-1/config": nn1_config, "/roles/nn-2/config": nn2_config},
    )
    assert eps.hdfs_nn_url == "https://nn1.example.com:9871"
    assert eps.hdfs_nn_candidates == [
        "https://nn1.example.com:9871",
        "https://nn2.example.com:9871",
    ]
    assert eps.hdfs_nn_http_url == "http://nn1.example.com:9870"
    assert eps.hdfs_nn_http_candidates == [
        "http://nn1.example.com:9870",
        "http://nn2.example.com:9870",
    ]


@pytest.mark.asyncio
async def test_discover_hdfs_skips_bad_namenode():
    """A BAD-health NN is skipped — its HTTP/HTTPS URLs are not collected."""
    roles = {
        "items": [
            {"type": "NAMENODE", "name": "nn-1", "hostRef": {"hostname": "nn1.example.com"}, "healthSummary": "BAD"},
            {"type": "NAMENODE", "name": "nn-2", "hostRef": {"hostname": "nn2.example.com"}, "healthSummary": "GOOD"},
        ]
    }
    nn2_config = {"items": [{"name": "dfs_https_port", "value": "9871"},
                            {"name": "dfs.namenode.http-address", "value": "0.0.0.0:9870"}]}
    eps = await _run_discovery(
        "HDFS", roles,
        config_payloads={"/roles/nn-1/config": {"items": []}, "/roles/nn-2/config": nn2_config},
    )
    assert eps.hdfs_nn_candidates == ["https://nn2.example.com:9871"]
    assert eps.hdfs_nn_http_candidates == ["http://nn2.example.com:9870"]


@pytest.mark.asyncio
async def test_discover_spark_populates_https_and_http_urls():
    roles = {
        "items": [
            {"type": "SPARK_YARN_HISTORY_SERVER", "name": "shs-1", "hostRef": {"hostname": "hs.example.com"}}
        ]
    }
    shs_config = {
        "items": [
            {"name": "ssl_server_port", "value": "18481"},
            {"name": "history.port", "value": "18080"},
        ]
    }
    eps = await _run_discovery(
        "SPARK", roles,
        config_payloads={"/roles/shs-1/config": shs_config},
    )
    assert eps.spark_hs_url == "https://hs.example.com:18481"
    assert eps.spark_hs_http_url == "http://hs.example.com:18080"


@pytest.mark.asyncio
async def test_discover_oozie_populates_https_and_http_urls():
    roles = {
        "items": [
            {"type": "OOZIE_SERVER", "name": "ooz-1", "hostRef": {"hostname": "oz.example.com"}}
        ]
    }
    ooz_config = {
        "items": [
            {"name": "oozie_https_port", "value": "11443"},
            {"name": "oozie_http_port", "value": "11000"},
        ]
    }
    eps = await _run_discovery(
        "OOZIE", roles,
        config_payloads={"/roles/ooz-1/config": ooz_config},
    )
    assert eps.oozie_url == "https://oz.example.com:11443"
    assert eps.oozie_http_url == "http://oz.example.com:11000"


# ── Factories thread http_url / http_candidates into the clients ──────────────


def test_get_hdfs_client_threads_http_candidates():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        hdfs_nn_url="https://nn:9871",
        hdfs_nn_candidates=["https://nn:9871"],
        hdfs_nn_http_url="http://nn:9870",
        hdfs_nn_http_candidates=["http://nn:9870"],
    )
    client = pool.get_hdfs_client("C")
    assert client is not None
    assert client._http_url == "http://nn:9870"
    assert client._http_candidates == ["http://nn:9870"]


def test_get_yarn_client_threads_http_url():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        yarn_rm_url="https://rm:8090",
        yarn_rm_http_url="http://rm:8088",
    )
    client = pool.get_yarn_client("C")
    assert client is not None
    assert client._http_url == "http://rm:8088"


def test_get_spark_client_threads_http_url():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        spark_hs_url="https://hs:18481",
        spark_hs_http_url="http://hs:18080",
    )
    client = pool.get_spark_client("C")
    assert client is not None
    assert client._http_url == "http://hs:18080"


def test_get_oozie_client_threads_http_url():
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        oozie_url="https://oz:11443",
        oozie_http_url="http://oz:11000",
    )
    client = pool.get_oozie_client("C")
    assert client is not None
    assert client._http_url == "http://oz:11000"


def test_factory_passes_none_when_no_http_url():
    """When discovery found no HTTP URL (e.g. older CM with no http config key),
    the factory passes None — the client's _http_url is None, no fallback."""
    pool = _make_pool()
    pool._endpoints["c"] = ServiceEndpoints(
        yarn_rm_url="http://rm:8088",  # already HTTP, no separate http_url
    )
    client = pool.get_yarn_client("C")
    assert client is not None
    assert client._http_url is None