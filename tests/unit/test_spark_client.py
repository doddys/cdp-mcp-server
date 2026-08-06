"""Unit tests for SparkClient."""
from __future__ import annotations

import pytest
import respx
import httpx

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.spark_client import SparkClient, SparkNotFoundError


BASE = "http://hs.example.com:18088"


@pytest.fixture
def client():
    return SparkClient(BASE, timeout=5)


SPARK_APP = {
    "id": "application_1234567890_0001",
    "name": "MySparkApp",
    "sparkUser": "alice",
    "attempts": [
        {
            "startTime": "2024-01-01T00:00:00.000GMT",
            "endTime": "2024-01-01T01:00:00.000GMT",
            "duration": 3600000,
            "completed": True,
            "executorRunTime": 120000,
        }
    ],
}


@respx.mock
@pytest.mark.asyncio
async def test_get_app(client):
    app_id = "application_1234567890_0001"
    respx.get(f"{BASE}/api/v1/applications/{app_id}").mock(
        return_value=httpx.Response(200, json=SPARK_APP)
    )
    result = await client.get_app(app_id)
    assert result["id"] == app_id
    assert result["name"] == "MySparkApp"
    assert result["completed"] is True
    assert result["duration_ms"] == 3600000


@respx.mock
@pytest.mark.asyncio
async def test_get_app_not_found_raises(client):
    app_id = "application_nonexistent"
    respx.get(f"{BASE}/api/v1/applications/{app_id}").mock(
        return_value=httpx.Response(404)
    )
    # Also mock the alternative id attempt (replace "application_" with "spark_")
    alt_id = app_id.replace("application_", "spark_")  # spark_nonexistent
    respx.get(f"{BASE}/api/v1/applications/{alt_id}").mock(
        return_value=httpx.Response(404)
    )
    with pytest.raises(SparkNotFoundError):
        await client.get_app(app_id)


@respx.mock
@pytest.mark.asyncio
async def test_get_stages(client):
    app_id = "application_1234567890_0001"
    stages = [
        {
            "stageId": 0,
            "name": "map",
            "status": "COMPLETE",
            "numTasks": 10,
            "numFailedTasks": 0,
            "inputBytes": 1024,
            "outputBytes": 512,
            "shuffleReadBytes": 0,
            "shuffleWriteBytes": 0,
            "executorRunTime": 5000,
            "failureReason": None,
        },
        {
            "stageId": 1,
            "name": "reduce",
            "status": "FAILED",
            "numTasks": 5,
            "numFailedTasks": 2,
            "inputBytes": 512,
            "outputBytes": 0,
            "shuffleReadBytes": 256,
            "shuffleWriteBytes": 0,
            "executorRunTime": 2000,
            "failureReason": "Task failed due to OOM",
        },
    ]
    respx.get(f"{BASE}/api/v1/applications/{app_id}/stages").mock(
        return_value=httpx.Response(200, json=stages)
    )
    result = await client.get_stages(app_id)
    assert len(result) == 2
    assert result[0]["stage_id"] == 0
    assert result[1]["status"] == "FAILED"
    assert "OOM" in result[1]["failure_reason"]


@respx.mock
@pytest.mark.asyncio
async def test_get_stages_long_failure_truncated(client):
    app_id = "application_1234567890_0001"
    long_reason = "Error: " + "x" * 500
    stages = [
        {
            "stageId": 0, "name": "stage", "status": "FAILED",
            "numTasks": 1, "numFailedTasks": 1,
            "inputBytes": 0, "outputBytes": 0,
            "shuffleReadBytes": 0, "shuffleWriteBytes": 0,
            "executorRunTime": 0, "failureReason": long_reason,
        }
    ]
    respx.get(f"{BASE}/api/v1/applications/{app_id}/stages").mock(
        return_value=httpx.Response(200, json=stages)
    )
    result = await client.get_stages(app_id)
    assert len(result[0]["failure_reason"]) <= 303
    assert result[0]["failure_reason"].endswith("...")


@respx.mock
@pytest.mark.asyncio
async def test_list_apps(client):
    apps = [SPARK_APP, {**SPARK_APP, "id": "application_0002", "name": "OtherApp"}]
    respx.get(f"{BASE}/api/v1/applications").mock(
        return_value=httpx.Response(200, json=apps)
    )
    result = await client.list_apps()
    assert len(result) == 2
    assert result[0]["id"] == "application_1234567890_0001"
    # No executorRunTime in compact list
    assert "total_executor_run_time_ms" not in result[0]


@respx.mock
@pytest.mark.asyncio
async def test_list_apps_follows_https_redirect(client):
    https_base = "https://hs.example.com:18488"
    respx.get(f"{BASE}/api/v1/applications").mock(
        return_value=httpx.Response(
            302, headers={"Location": f"{https_base}/api/v1/applications"}
        )
    )
    respx.get(f"{https_base}/api/v1/applications").mock(
        return_value=httpx.Response(200, json=[SPARK_APP])
    )
    result = await client.list_apps()
    assert len(result) == 1


@respx.mock
@pytest.mark.asyncio
async def test_list_apps_spnego_challenge_raises(client):
    respx.get(f"{BASE}/api/v1/applications").mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"})
    )
    with pytest.raises(SpnegoRequiredError):
        await client.list_apps()
