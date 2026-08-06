"""Unit tests for OozieClient."""
from __future__ import annotations

import httpx
import pytest
import respx

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.oozie_client import OozieClient, OozieNotFoundError

BASE = "http://oozie.example.com:11000"


@pytest.fixture
def client():
    return OozieClient(BASE, timeout=5)


@respx.mock
@pytest.mark.asyncio
async def test_get_job_workflow(client):
    job_id = "0000001-240101120000000-oozie-oozi-W"
    respx.get(f"{BASE}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        return_value=httpx.Response(
            200,
            json={
                "id": job_id,
                "appName": "my-workflow",
                "type": "wf",
                "status": "SUCCEEDED",
                "actions": [],
            },
        )
    )
    result = await client.get_job(job_id)
    assert result["type"] == "workflow"
    assert result["status"] == "SUCCEEDED"


@respx.mock
@pytest.mark.asyncio
async def test_get_job_not_found_raises(client):
    job_id = "nonexistent-W"
    respx.get(f"{BASE}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        return_value=httpx.Response(404)
    )
    with pytest.raises(OozieNotFoundError):
        await client.get_job(job_id)


@respx.mock
@pytest.mark.asyncio
async def test_list_jobs(client):
    respx.get(f"{BASE}/oozie/v2/jobs").mock(
        return_value=httpx.Response(
            200,
            json={
                "workflows": [
                    {"id": "1-W", "appName": "wf1", "status": "RUNNING", "user": "alice"}
                ]
            },
        )
    )
    result = await client.list_jobs()
    assert len(result) == 1
    assert result[0]["app_name"] == "wf1"


@respx.mock
@pytest.mark.asyncio
async def test_list_jobs_follows_https_redirect(client):
    https_base = "https://oozie.example.com:11443"
    respx.get(f"{BASE}/oozie/v2/jobs").mock(
        return_value=httpx.Response(
            302, headers={"Location": f"{https_base}/oozie/v2/jobs"}
        )
    )
    respx.get(f"{https_base}/oozie/v2/jobs").mock(
        return_value=httpx.Response(200, json={"workflows": []})
    )
    result = await client.list_jobs()
    assert result == []


@respx.mock
@pytest.mark.asyncio
async def test_list_jobs_spnego_challenge_raises(client):
    respx.get(f"{BASE}/oozie/v2/jobs").mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"})
    )
    with pytest.raises(SpnegoRequiredError):
        await client.list_jobs()
