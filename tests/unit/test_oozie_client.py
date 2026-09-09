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


# ── HTTPS→HTTP port fallback ──────────────────────────────────────────────────

HTTPS_BASE = "https://oozie.example.com:11443"
HTTP_FALLBACK = "http://oozie.example.com:11000"


@pytest.fixture
def fallback_client():
    """Client whose discovered primary URL is HTTPS but the HTTPS port has no
    listener — must fall back to the HTTP port."""
    return OozieClient(HTTPS_BASE, timeout=5, http_url=HTTP_FALLBACK)


@respx.mock
@pytest.mark.asyncio
async def test_get_job_https_connect_error_falls_back_to_http(fallback_client):
    """HTTPS Oozie port refused → fall back to HTTP port, return real job data."""
    job_id = "0000001-240101120000000-oozie-oozi-W"
    respx.get(f"{HTTPS_BASE}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    respx.get(f"{HTTP_FALLBACK}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        return_value=httpx.Response(
            200,
            json={"id": job_id, "appName": "my-workflow", "type": "wf",
                  "status": "SUCCEEDED", "actions": []},
        )
    )
    result = await fallback_client.get_job(job_id)
    assert result["type"] == "workflow"
    assert result["status"] == "SUCCEEDED"


@respx.mock
@pytest.mark.asyncio
async def test_list_jobs_https_connect_error_falls_back_to_http(fallback_client):
    """list_jobs also falls back to HTTP when HTTPS is unreachable."""
    respx.get(f"{HTTPS_BASE}/oozie/v2/jobs").mock(
        side_effect=httpx.ConnectError("connection refused")
    )
    respx.get(f"{HTTP_FALLBACK}/oozie/v2/jobs").mock(
        return_value=httpx.Response(
            200, json={"workflows": [{"id": "1-W", "appName": "wf1", "status": "RUNNING", "user": "alice"}]}
        )
    )
    result = await fallback_client.list_jobs()
    assert len(result) == 1
    assert result[0]["app_name"] == "wf1"


@respx.mock
@pytest.mark.asyncio
async def test_get_job_both_ports_unavailable_names_both(fallback_client):
    """HTTPS and HTTP both unreachable → OozieServiceUnavailable naming both."""
    from cdp_mcp.clients.oozie_client import OozieServiceUnavailable
    job_id = "0000001-240101120000000-oozie-oozi-W"
    respx.get(f"{HTTPS_BASE}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        side_effect=httpx.ConnectError("no route")
    )
    respx.get(f"{HTTP_FALLBACK}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        side_effect=httpx.ConnectError("no route")
    )
    with pytest.raises(OozieServiceUnavailable) as exc_info:
        await fallback_client.get_job(job_id)
    msg = str(exc_info.value)
    assert HTTPS_BASE in msg and HTTP_FALLBACK in msg
    assert "Oozie unreachable" in msg


@respx.mock
@pytest.mark.asyncio
async def test_get_job_spnego_on_https_no_http_fallback(fallback_client):
    """401 on HTTPS means the port answered — do NOT fall back to HTTP."""
    job_id = "0000001-240101120000000-oozie-oozi-W"
    http_route = respx.get(f"{HTTP_FALLBACK}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        return_value=httpx.Response(200, json={"id": job_id, "appName": "wf", "type": "wf", "status": "SUCCEEDED", "actions": []})
    )
    respx.get(f"{HTTPS_BASE}/oozie/v2/job/{job_id}", params={"show": "info"}).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": "Negotiate"})
    )
    with pytest.raises(SpnegoRequiredError):
        await fallback_client.get_job(job_id)
    assert http_route.calls.call_count == 0
