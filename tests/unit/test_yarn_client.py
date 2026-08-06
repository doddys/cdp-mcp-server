"""Unit tests for YarnClient using respx to mock httpx."""
from __future__ import annotations

import pytest
import respx
import httpx

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.yarn_client import YarnClient, YarnNotFoundError


BASE = "http://rm.example.com:8088"


@pytest.fixture
def client():
    return YarnClient(BASE, timeout=5)


# ── get_app ───────────────────────────────────────────────────────────────────

@respx.mock
@pytest.mark.asyncio
async def test_get_app_running(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_001").mock(
        return_value=httpx.Response(
            200,
            json={
                "app": {
                    "id": "application_001",
                    "name": "MySparkJob",
                    "user": "alice",
                    "queue": "default",
                    "state": "RUNNING",
                    "finalStatus": "UNDEFINED",
                    "progress": 42.5,
                    "trackingUrl": "http://rm:8088/proxy/application_001",
                    "diagnostics": "",
                    "elapsedTime": 12000,
                    "memorySeconds": 90000,
                    "vcoreSeconds": 10,
                    "startedTime": 1700000000000,
                    "finishedTime": 0,
                    "clusterId": 12345,
                }
            },
        )
    )
    result = await client.get_app("application_001")
    assert result["app_id"] == "application_001"
    assert result["state"] == "RUNNING"
    assert result["elapsed_time_secs"] == 12.0
    assert result["user"] == "alice"


@respx.mock
@pytest.mark.asyncio
async def test_get_app_failed_no_diagnostics_adds_note(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_002").mock(
        return_value=httpx.Response(
            200,
            json={
                "app": {
                    "id": "application_002",
                    "name": "FailedJob",
                    "user": "bob",
                    "queue": "default",
                    "state": "FINISHED",
                    "finalStatus": "FAILED",
                    "progress": 0.0,
                    "trackingUrl": "",
                    "diagnostics": "",
                    "elapsedTime": 5000,
                    "memorySeconds": 0,
                    "vcoreSeconds": 0,
                    "startedTime": 0,
                    "finishedTime": 0,
                    "clusterId": 12345,
                }
            },
        )
    )
    result = await client.get_app("application_002")
    assert result["final_status"] == "FAILED"
    assert "get_service_logs" in result["diagnostics"]


@respx.mock
@pytest.mark.asyncio
async def test_get_app_failed_with_diagnostics(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_003").mock(
        return_value=httpx.Response(
            200,
            json={
                "app": {
                    "id": "application_003",
                    "name": "CrashJob",
                    "user": "carol",
                    "queue": "root.engineering",
                    "state": "FINISHED",
                    "finalStatus": "FAILED",
                    "progress": 0.0,
                    "trackingUrl": "",
                    "diagnostics": "Container exited with a non-zero exit code 1. Error: OOM killed.",
                    "elapsedTime": 3000,
                    "memorySeconds": 0,
                    "vcoreSeconds": 0,
                    "startedTime": 0,
                    "finishedTime": 0,
                    "clusterId": 12345,
                }
            },
        )
    )
    result = await client.get_app("application_003")
    assert result["final_status"] == "FAILED"
    assert "OOM killed" in result["diagnostics"]


@respx.mock
@pytest.mark.asyncio
async def test_get_app_long_diagnostics_truncated(client):
    long_diag = "x" * 1000
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_004").mock(
        return_value=httpx.Response(
            200,
            json={
                "app": {
                    "id": "application_004",
                    "name": "LongDiag",
                    "user": "u",
                    "queue": "q",
                    "state": "FINISHED",
                    "finalStatus": "FAILED",
                    "progress": 0,
                    "trackingUrl": "",
                    "diagnostics": long_diag,
                    "elapsedTime": 0,
                    "memorySeconds": 0,
                    "vcoreSeconds": 0,
                    "startedTime": 0,
                    "finishedTime": 0,
                    "clusterId": 0,
                }
            },
        )
    )
    result = await client.get_app("application_004")
    assert len(result["diagnostics"]) <= 503  # 500 + "..."
    assert result["diagnostics"].endswith("...")


@respx.mock
@pytest.mark.asyncio
async def test_get_app_not_found_raises(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_999").mock(
        return_value=httpx.Response(404)
    )
    with pytest.raises(YarnNotFoundError):
        await client.get_app("application_999")


# ── list_apps ─────────────────────────────────────────────────────────────────

@respx.mock
@pytest.mark.asyncio
async def test_list_apps_returns_sorted_by_start_desc(client):
    apps = [
        {"id": "app_001", "name": "A", "user": "u", "queue": "q", "state": "FINISHED",
         "finalStatus": "SUCCEEDED", "progress": 100, "elapsedTime": 1000, "startedTime": 1000},
        {"id": "app_002", "name": "B", "user": "u", "queue": "q", "state": "RUNNING",
         "finalStatus": "UNDEFINED", "progress": 50, "elapsedTime": 2000, "startedTime": 2000},
    ]
    respx.get(f"{BASE}/ws/v1/cluster/apps").mock(
        return_value=httpx.Response(200, json={"apps": {"app": apps}})
    )
    result = await client.list_apps()
    assert result[0]["app_id"] == "app_002"  # more recent first
    assert result[1]["app_id"] == "app_001"


@respx.mock
@pytest.mark.asyncio
async def test_list_apps_respects_limit(client):
    apps = [
        {"id": f"app_{i:03d}", "name": f"App{i}", "user": "u", "queue": "q",
         "state": "FINISHED", "finalStatus": "SUCCEEDED", "progress": 100,
         "elapsedTime": 0, "startedTime": i}
        for i in range(30)
    ]
    respx.get(f"{BASE}/ws/v1/cluster/apps").mock(
        return_value=httpx.Response(200, json={"apps": {"app": apps}})
    )
    result = await client.list_apps(limit=5)
    assert len(result) == 5


@respx.mock
@pytest.mark.asyncio
async def test_list_apps_no_diagnostics_in_result(client):
    apps = [
        {"id": "app_001", "name": "A", "user": "u", "queue": "q", "state": "FINISHED",
         "finalStatus": "FAILED", "progress": 0, "elapsedTime": 0, "startedTime": 0,
         "diagnostics": "Some error"}
    ]
    respx.get(f"{BASE}/ws/v1/cluster/apps").mock(
        return_value=httpx.Response(200, json={"apps": {"app": apps}})
    )
    result = await client.list_apps()
    assert "diagnostics" not in result[0]


@respx.mock
@pytest.mark.asyncio
async def test_list_apps_empty_returns_empty_list(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps").mock(
        return_value=httpx.Response(200, json={"apps": None})
    )
    result = await client.list_apps()
    assert result == []


# ── get_queue ─────────────────────────────────────────────────────────────────

SCHEDULER_RESPONSE = {
    "scheduler": {
        "schedulerInfo": {
            "queueName": "root",
            "capacity": 100.0,
            "usedCapacity": 45.0,
            "absoluteCapacity": 100.0,
            "absoluteUsedCapacity": 45.0,
            "numPendingApplications": 2,
            "numActiveApplications": 5,
            "numContainersPending": 10,
            "queues": {
                "queue": [
                    {
                        "queueName": "engineering",
                        "capacity": 60.0,
                        "usedCapacity": 50.0,
                        "absoluteCapacity": 60.0,
                        "absoluteUsedCapacity": 30.0,
                        "numPendingApplications": 1,
                        "numActiveApplications": 3,
                        "numContainersPending": 5,
                    }
                ]
            },
        }
    }
}


@respx.mock
@pytest.mark.asyncio
async def test_get_queue_root(client):
    respx.get(f"{BASE}/ws/v1/cluster/scheduler").mock(
        return_value=httpx.Response(200, json=SCHEDULER_RESPONSE)
    )
    result = await client.get_queue()
    assert result["name"] == "root"
    assert result["capacity"] == 100.0
    assert result["num_active_applications"] == 5


@respx.mock
@pytest.mark.asyncio
async def test_get_queue_named(client):
    respx.get(f"{BASE}/ws/v1/cluster/scheduler").mock(
        return_value=httpx.Response(200, json=SCHEDULER_RESPONSE)
    )
    result = await client.get_queue("engineering")
    assert result["name"] == "engineering"
    assert result["capacity"] == 60.0


@respx.mock
@pytest.mark.asyncio
async def test_get_queue_not_found(client):
    respx.get(f"{BASE}/ws/v1/cluster/scheduler").mock(
        return_value=httpx.Response(200, json=SCHEDULER_RESPONSE)
    )
    result = await client.get_queue("nonexistent")
    assert "error" in result


@respx.mock
@pytest.mark.asyncio
async def test_get_app_follows_https_redirect(client):
    https_base = "https://rm.example.com:8090"
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_001").mock(
        return_value=httpx.Response(
            302, headers={"Location": f"{https_base}/ws/v1/cluster/apps/application_001"}
        )
    )
    respx.get(f"{https_base}/ws/v1/cluster/apps/application_001").mock(
        return_value=httpx.Response(200, json={"app": {"id": "application_001"}})
    )
    result = await client.get_app("application_001")
    assert result["app_id"] == "application_001"


@respx.mock
@pytest.mark.asyncio
async def test_get_app_spnego_challenge_raises(client):
    respx.get(f"{BASE}/ws/v1/cluster/apps/application_001").mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"})
    )
    with pytest.raises(SpnegoRequiredError):
        await client.get_app("application_001")
