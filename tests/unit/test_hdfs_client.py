"""Unit tests for HdfsClient."""
from __future__ import annotations

import httpx
import pytest
import respx

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.hdfs_client import HdfsClient, HdfsClientError

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


@respx.mock
@pytest.mark.asyncio
async def test_none_block_counters_coerced_to_zero(client):
    """JMX may return a counter key with value None (observed live on some
    clusters). This must not crash the `> 0` health math with TypeError."""
    fs = {
        **FS_HEALTHY,
        "UnderReplicatedBlocks": None,
        "CorruptBlocks": None,
        "MissingBlocks": None,
    }
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=FSNamesystemState"}).mock(
        return_value=httpx.Response(200, json={"beans": [fs]})
    )
    respx.get(f"{BASE}/jmx", params={"qry": "Hadoop:service=NameNode,name=NameNodeStatus"}).mock(
        return_value=httpx.Response(200, json={"beans": [NN_STATUS]})
    )
    result = await client.get_namenode_status()
    assert result["health_summary"] == "HEALTHY"
    assert result["under_replicated_blocks"] == 0
    assert result["corrupt_blocks"] == 0
    assert result["missing_blocks"] == 0


# ── get_directory_snapshots (WebHDFS <path>/.snapshot?op=LISTSTATUS) ────────


def _liststatus_response(entries: list) -> dict:
    return {"FileStatuses": {"FileStatus": entries}}


SNAP_ENTRY_1 = {
    "pathSuffix": "snap20240101",
    "modificationTime": 1704067200000,  # 2024-01-01T00:00:00Z
    "owner": "hdfs",
    "group": "hadoop",
    "permission": "755",
    "type": "DIRECTORY",
    "childrenNum": 12,
    "length": 0,
}
SNAP_ENTRY_2 = {
    "pathSuffix": "snap20240201",
    "modificationTime": 1706745600000,  # 2024-02-01T00:00:00Z
    "owner": "hdfs",
    "group": "hadoop",
    "permission": "755",
    "type": "DIRECTORY",
    "childrenNum": 3,
    "length": 0,
}


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_lists_snapshots(client):
    respx.get(
        f"{BASE}/webhdfs/v1/data/warehouse/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(200, json=_liststatus_response([SNAP_ENTRY_1, SNAP_ENTRY_2])))
    result = await client.get_directory_snapshots("/data/warehouse")
    assert result["path"] == "/data/warehouse"
    assert result["count"] == 2
    assert result["truncated"] is False
    assert result["snapshots"][0]["snapshot_name"] == "snap20240101"
    assert result["snapshots"][0]["creation_time"].startswith("2024-01-01")
    assert result["snapshots"][1]["snapshot_name"] == "snap20240201"
    assert result["snapshots"][1]["owner"] == "hdfs"
    assert result["snapshots"][1]["children_num"] == 3


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_snapshottable_but_empty(client):
    respx.get(
        f"{BASE}/webhdfs/v1/data/empty/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(200, json=_liststatus_response([])))
    result = await client.get_directory_snapshots("/data/empty")
    assert result["count"] == 0
    assert result["snapshots"] == []
    assert result["truncated"] is False


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_not_snapshottable_raises(client):
    body = {
        "RemoteException": {
            "message": "Directory is not a snapshottable directory",
            "exception": "SnapshotException",
        }
    }
    respx.get(
        f"{BASE}/webhdfs/v1/data/nope/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(404, json=body))
    with pytest.raises(HdfsClientError) as exc_info:
        await client.get_directory_snapshots("/data/nope")
    assert "404" in str(exc_info.value)
    assert "not a snapshottable" in str(exc_info.value)


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_spnego_challenge_raises(client):
    respx.get(
        f"{BASE}/webhdfs/v1/data/sec/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"}))
    with pytest.raises(SpnegoRequiredError):
        await client.get_directory_snapshots("/data/sec")


# ── HA failover: standby NameNode → active NameNode ─────────────────────────


BASE2 = "http://nn2.example.com:9870"

STANDBY_BODY = {
    "RemoteException": {
        "exception": "StandbyException",
        "javaClassName": "org.apache.hadoop.ipc.StandbyException",
        "message": "Operation category READ is not supported in state standby",
    }
}


@pytest.fixture
def ha_client():
    return HdfsClient(BASE, timeout=5, candidates=[BASE, BASE2])


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_ha_failover_to_active(ha_client):
    """Discovery may hand us the standby NN; WebHDFS reads must fail over to the
    active one instead of returning the standby error."""
    respx.get(
        f"{BASE}/webhdfs/v1/data/warehouse/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(403, json=STANDBY_BODY))
    respx.get(
        f"{BASE2}/webhdfs/v1/data/warehouse/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(
        return_value=httpx.Response(200, json=_liststatus_response([SNAP_ENTRY_1]))
    )
    result = await ha_client.get_directory_snapshots("/data/warehouse")
    assert result["count"] == 1
    assert result["snapshots"][0]["snapshot_name"] == "snap20240101"


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_ha_all_standby_raises(ha_client):
    """Every candidate standby → clear error (no active NN reachable)."""
    respx.get(
        f"{BASE}/webhdfs/v1/data/warehouse/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(403, json=STANDBY_BODY))
    respx.get(
        f"{BASE2}/webhdfs/v1/data/warehouse/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(403, json=STANDBY_BODY))
    with pytest.raises(HdfsClientError) as exc_info:
        await ha_client.get_directory_snapshots("/data/warehouse")
    assert "All HDFS NameNodes" in str(exc_info.value)
    assert "standby" in str(exc_info.value).lower()


@respx.mock
@pytest.mark.asyncio
async def test_get_directory_snapshots_ha_path_error_does_not_failover(ha_client):
    """A real path error (not snapshottable) on the first candidate must NOT
    fail over — it's a path problem, not an NN problem."""
    body = {"RemoteException": {"message": "Directory is not a snapshottable directory"}}
    respx.get(
        f"{BASE}/webhdfs/v1/data/nope/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(404, json=body))
    # Second candidate should never be hit — give it a different (wrong) response
    # so the test would fail loudly if failover occurred.
    respx.get(
        f"{BASE2}/webhdfs/v1/data/nope/.snapshot", params={"op": "LISTSTATUS"}
    ).mock(return_value=httpx.Response(200, json=_liststatus_response([SNAP_ENTRY_1])))
    with pytest.raises(HdfsClientError) as exc_info:
        await ha_client.get_directory_snapshots("/data/nope")
    assert "not a snapshottable" in str(exc_info.value)
