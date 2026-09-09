"""
oozie_client.py — Async client for Oozie REST API.
"""
from __future__ import annotations

from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.http_fallback import fetch_with_http_fallback

log = structlog.get_logger(__name__)

_SENSITIVE_KEYS = {
    "password", "secret", "token", "credential", "passwd", "pwd", "key"
}


def _scrub(d: dict) -> dict:
    """Remove sensitive keys from a conf dict."""
    return {
        k: v
        for k, v in d.items()
        if not any(s in k.lower() for s in _SENSITIVE_KEYS)
    }


# ── Exceptions ────────────────────────────────────────────────────────────────

class OozieClientError(Exception):
    """Base exception for Oozie client errors."""


class OozieNotFoundError(OozieClientError):
    """Job or resource not found."""


class OozieServiceUnavailable(OozieClientError):
    """Oozie server temporarily unavailable."""


# ── Client ────────────────────────────────────────────────────────────────────

class OozieClient:
    def __init__(self, base_url: str, timeout: int = 30, auth: Any = None,
                 http_url: str | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._http_url = http_url.rstrip("/") if http_url else None
        self._timeout = timeout
        self._auth = auth

    def _retry_dec(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, OozieServiceUnavailable)
            ),
            reraise=True,
        )

    def _map_response(self, resp: httpx.Response, path: str, base_url: str) -> dict:
        if resp.status_code == 401:
            if "negotiate" in resp.headers.get("www-authenticate", "").lower():
                raise SpnegoRequiredError(f"SPNEGO required for {base_url}")
        if resp.status_code == 404:
            raise OozieNotFoundError(f"Not found: {path}")
        if resp.status_code in (503, 504):
            raise OozieServiceUnavailable(f"Oozie unavailable: {resp.status_code}")
        if resp.status_code >= 400:
            raise OozieClientError(f"Oozie HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            return resp.json()
        except ValueError as exc:
            log.warning(
                "oozie_client.non_json_response",
                status=resp.status_code,
                content_type=resp.headers.get("content-type"),
                body=resp.text[:300],
            )
            raise OozieClientError(
                f"Oozie returned non-JSON response (HTTP {resp.status_code}, "
                f"content-type={resp.headers.get('content-type')!r}): "
                f"{resp.text[:300]!r}"
            ) from exc

    async def _get(self, path: str, params: dict | None = None) -> dict:
        return await fetch_with_http_fallback(
            primary_url=self._base_url,
            fallback_url=self._http_url,
            path=path,
            params=params,
            auth=self._auth,
            timeout=self._timeout,
            retry_dec=self._retry_dec,
            map_response=self._map_response,
            service_unavailable=OozieServiceUnavailable,
            service_label="Oozie",
        )

    async def get_job(self, job_id: str) -> dict:
        """Get Oozie workflow or coordinator job details."""
        data = await self._get(
            f"/oozie/v2/job/{job_id}", params={"show": "info"}
        )
        job_type = data.get("type", "wf").lower()

        if "coordinator" in job_type or job_id.endswith("-C"):
            return {
                "id": data.get("coordJobId"),
                "app_name": data.get("coordJobName"),
                "type": "coordinator",
                "status": data.get("status"),
                "frequency": data.get("frequency"),
                "time_unit": data.get("timeUnit"),
                "next_materialize": data.get("nextMaterializedTime"),
                "pause_time": data.get("pauseTime"),
                "total_actions": data.get("total"),
                "done_actions": data.get("doneActions"),
                "failed_actions": data.get("failedActions"),
            }

        # Workflow
        actions = []
        for action in data.get("actions", []):
            ext_id = action.get("externalId", "")
            actions.append(
                {
                    "name": action.get("name"),
                    "type": action.get("type"),
                    "status": action.get("status"),
                    "start_time": action.get("startTime"),
                    "end_time": action.get("endTime"),
                    "error_message": action.get("errorMessage"),
                    "external_id": ext_id,  # YARN app_id if it's a YARN action
                }
            )

        return {
            "id": data.get("id"),
            "app_name": data.get("appName"),
            "type": "workflow",
            "status": data.get("status"),
            "start_time": data.get("startTime"),
            "end_time": data.get("endTime"),
            "created_time": data.get("createdTime"),
            "user": data.get("user"),
            "group": data.get("group"),
            "actions": actions,
        }

    async def list_jobs(
        self,
        status: str | None = None,
        jobtype: str = "wf",
        user: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """List Oozie jobs (compact)."""
        filter_parts = []
        if status:
            filter_parts.append(f"status={status}")
        if user:
            filter_parts.append(f"user={user}")
        filter_str = ";".join(filter_parts)

        params: dict = {"jobtype": jobtype, "len": limit}
        if filter_str:
            params["filter"] = filter_str

        data = await self._get("/oozie/v2/jobs", params=params)
        jobs = data.get("workflows", data.get("coordinatorjobs", []))
        return [
            {
                "id": j.get("id"),
                "app_name": j.get("appName", j.get("coordJobName")),
                "status": j.get("status"),
                "start_time": j.get("startTime", j.get("nextMaterializedTime")),
                "user": j.get("user"),
            }
            for j in jobs
        ]
