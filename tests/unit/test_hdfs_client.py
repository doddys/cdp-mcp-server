"""Unit tests for HdfsClient."""
from __future__ import annotations

import pytest
import respx
import httpx

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.hdfs_client import HdfsClient


BASE = "http://nn.example.com:9870"


@pytest.fixture
def client():
    return HdfsClient(BASE, timeout=5)


def _jmx_response(fs_state: dict, nn_state: dict) -> dict:
    return {"beans": [{**fs_state, **nn_state}]}


FS_HEALTHY = {
    "FSState": "Operational",
    "UnderReplicatedBlocks": 0,
    "CorruptBlocks": 0,
    "MissingBlocks": 0,
    "CapacityTotal": 10 * (1024 ** 3),   # 10 GB
    "CapacityUsed": 4 * (1024 ** 3),     # 4 GB
    "CapacityRemaining": 6 * (1024 ** 3),  # 6 GB
    "FilesTotal": 1000,
    "Directories": 200,
}

NN_STATUS = {
    "HostAndPort": "nn.example.com:8020",
    "State": "active",
}


@respx.mock
@pytest.mark.asyncio
async def test_healthy_namenode(client):
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [FS_HEALTHY]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "HEALTHY"
    assert result["safe_mode"] is False
    assert result["corrupt_blocks"] == 0
    assert result["missing_blocks"] == 0
    assert result["capacity_total_gb"] == 10.0
    assert result["capacity_used_gb"] == 4.0
    assert result["capacity_used_pct"] == 40.0


@respx.mock
@pytest.mark.asyncio
async def test_degraded_namenode_under_replicated(client):
    fs = {**FS_HEALTHY, "UnderReplicatedBlocks": 5}
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [fs]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "DEGRADED"
    assert result["under_replicated_blocks"] == 5


@respx.mock
@pytest.mark.asyncio
async def test_critical_namenode_corrupt_blocks(client):
    fs = {**FS_HEALTHY, "CorruptBlocks": 3}
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [fs]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "CRITICAL"
    assert result["corrupt_blocks"] == 3


@respx.mock
@pytest.mark.asyncio
async def test_critical_namenode_safe_mode(client):
    fs = {**FS_HEALTHY, "FSState": "safeMode"}
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [fs]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "CRITICAL"
    assert result["safe_mode"] is True


@respx.mock
@pytest.mark.asyncio
async def test_capacity_used_pct_zero_when_total_zero(client):
    fs = {**FS_HEALTHY, "CapacityTotal": 0, "CapacityUsed": 0}
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [fs]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["capacity_used_pct"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_get_namenode_status_follows_https_redirect(client):
    https_base = "https://nn.example.com:9871"
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(
            302, headers={"Location": f"{https_base}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState"}
        )
    )
    respx.get(f"{https_base}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [FS_HEALTHY]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(
            302, headers={"Location": f"{https_base}/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus"}
        )
    )
    respx.get(f"{https_base}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "HEALTHY"


@respx.mock
@pytest.mark.asyncio
async def test_get_namenode_status_spnego_challenge_raises(client):
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"})
    )
    with pytest.raises(SpnegoRequiredError):
        await client.get_namenode_status()
