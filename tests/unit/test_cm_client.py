"""Unit tests for ClouderaManagerClient (cm_client.py)."""
from __future__ import annotations

import httpx
import pytest
import respx

from cdp_mcp.cm_client import ClouderaManagerClient
from cdp_mcp.config import ClouderaManagerSettings, ServerSettings

BASE = "http://cm.example.com:7183/api/v51"


@pytest.fixture
async def client():
    settings = ClouderaManagerSettings(
        host="cm.example.com",
        port=7183,
        username="admin",
        password="admin",
        use_tls=False,
        api_version="v51",
    )
    c = ClouderaManagerClient(settings, ServerSettings())
    await c.connect()
    yield c
    await c.close()


@respx.mock
@pytest.mark.asyncio
async def test_get_cluster_security_info(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/isTlsEnabled").mock(
        return_value=httpx.Response(200, json={"tlsEnabled": True})
    )
    respx.get(f"{BASE}/clusters/My%20Cluster/kerberosInfo").mock(
        return_value=httpx.Response(200, json={"kerberized": True})
    )
    result = await client.get_cluster_security_info("My Cluster")
    assert result["tls"]["tlsEnabled"] is True
    assert result["kerberos"]["kerberized"] is True


@respx.mock
@pytest.mark.asyncio
async def test_get_host_metrics(client):
    route = respx.post(f"{BASE}/timeseries").mock(
        return_value=httpx.Response(200, json={"items": [{"metric": "cpu_percent"}]})
    )
    result = await client.get_host_metrics("host1.example.com", ["cpu_percent"])
    assert result == [{"metric": "cpu_percent"}]
    sent_body = route.calls[0].request.content
    assert b"host1.example.com" in sent_body


@respx.mock
@pytest.mark.asyncio
async def test_list_available_metrics_no_filter(client):
    respx.get(f"{BASE}/timeseries/schema").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {"name": "cpu_percent", "displayName": "CPU Percent"},
                    {"name": "mem_rss", "displayName": "Resident Memory"},
                ]
            },
        )
    )
    result = await client.list_available_metrics()
    assert len(result) == 2


@respx.mock
@pytest.mark.asyncio
async def test_list_available_metrics_filtered(client):
    respx.get(f"{BASE}/timeseries/schema").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {"name": "cpu_percent", "displayName": "CPU Percent"},
                    {"name": "mem_rss", "displayName": "Resident Memory"},
                ]
            },
        )
    )
    result = await client.list_available_metrics(name_contains="cpu")
    assert len(result) == 1
    assert result[0]["name"] == "cpu_percent"


@respx.mock
@pytest.mark.asyncio
async def test_list_roles(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(
            200,
            json={"items": [{"name": "namenode1", "healthSummary": "GOOD"}]},
        )
    )
    result = await client.list_roles("My Cluster", "hdfs")
    assert result == [{"name": "namenode1", "healthSummary": "GOOD"}]


@respx.mock
@pytest.mark.asyncio
async def test_get_role(client):
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/namenode1"
    ).mock(
        return_value=httpx.Response(
            200, json={"name": "namenode1", "roleState": "STARTED"}
        )
    )
    result = await client.get_role("My Cluster", "hdfs", "namenode1")
    assert result["roleState"] == "STARTED"


@respx.mock
@pytest.mark.asyncio
async def test_list_cluster_commands_applies_client_side_limit(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/commands").mock(
        return_value=httpx.Response(
            200,
            json={"items": [{"id": i, "name": "Start"} for i in range(5)]},
        )
    )
    result = await client.list_cluster_commands("My Cluster", limit=2)
    assert len(result) == 2
    assert result[0]["id"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_get_impala_queries(client):
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/impala/impalaQueries"
    ).mock(
        return_value=httpx.Response(
            200, json={"queries": [{"queryId": "abc123", "user": "root"}]}
        )
    )
    result = await client.get_impala_queries(
        "My Cluster", "impala", filter_str="user=root"
    )
    assert result == [{"queryId": "abc123", "user": "root"}]
    sent_params = route.calls[0].request.url.params
    assert sent_params["filter"] == "user=root"


@respx.mock
@pytest.mark.asyncio
async def test_get_cluster_utilization(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/utilization").mock(
        return_value=httpx.Response(200, json={"clusterUtilization": []})
    )
    result = await client.get_cluster_utilization("My Cluster")
    assert result == {"clusterUtilization": []}


@respx.mock
@pytest.mark.asyncio
async def test_list_replication_schedules(client):
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications"
    ).mock(
        return_value=httpx.Response(200, json={"items": [{"id": 1}]})
    )
    result = await client.list_replication_schedules("My Cluster", "hdfs")
    assert result == [{"id": 1}]


@respx.mock
@pytest.mark.asyncio
async def test_get_replication_history(client):
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications/1/history"
    ).mock(
        return_value=httpx.Response(200, json={"items": [{"id": 100, "success": True}]})
    )
    result = await client.get_replication_history("My Cluster", "hdfs", 1)
    assert result == [{"id": 100, "success": True}]


@respx.mock
@pytest.mark.asyncio
async def test_list_parcels(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/parcels").mock(
        return_value=httpx.Response(
            200, json={"items": [{"product": "CDH", "stage": "ACTIVATED"}]}
        )
    )
    result = await client.list_parcels("My Cluster")
    assert result == [{"product": "CDH", "stage": "ACTIVATED"}]


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_skips_gateway_and_caps_roles(client):
    roles = (
        [{"name": f"dn{i}", "type": "DATANODE"} for i in range(15)]
        + [{"name": "gw1", "type": "GATEWAY"}]
    )
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    for i in range(10):
        respx.get(
            f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/dn{i}/logs/full"
        ).mock(return_value=httpx.Response(200, text="log line"))

    result = await client.get_service_logs("My Cluster", "hdfs", max_lines=10)

    assert "gw1" not in result
    fetched = [k for k in result if k != "_truncated"]
    assert len(fetched) == 10
    assert "_truncated" in result


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_role_name_bypasses_cap(client):
    roles = [{"name": f"dn{i}", "type": "DATANODE"} for i in range(15)]
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/dn5/logs/full"
    ).mock(return_value=httpx.Response(200, text="log line"))

    result = await client.get_service_logs(
        "My Cluster", "hdfs", max_lines=10, role_name="dn5"
    )

    assert list(result.keys()) == ["dn5"]


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_truncates_client_side(client):
    """CM's /logs/full ignores any query params and always returns the
    complete file -- confirmed empirically against a live cluster (28,040
    lines returned regardless of a "lines" param). max_lines must be
    enforced by slicing the response, not by trusting the server."""
    roles = [{"name": "nn1", "type": "NAMENODE"}]
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    full_log = "\n".join(f"line {i}" for i in range(1000))
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/nn1/logs/full"
    ).mock(return_value=httpx.Response(200, text=full_log))

    result = await client.get_service_logs("My Cluster", "hdfs", max_lines=5)

    assert len(result["nn1"]) == 5
    assert result["nn1"] == [f"line {i}" for i in range(995, 1000)]
    # no "lines" (or any) query param sent -- it doesn't do anything server-side
    assert route.calls[0].request.url.params == httpx.QueryParams()
