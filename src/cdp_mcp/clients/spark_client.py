"""
spark_client.py — Async client for Spark History Server REST API.
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

log = structlog.get_logger(__name__)


# ── Exceptions ────────────────────────────────────────────────────────────────

class SparkClientError(Exception):
    """Base exception for Spark History Server client errors."""


class SparkNotFoundError(SparkClientError):
    """Application or resource not found."""


class SparkServiceUnavailable(SparkClientError):
    """Spark History Server temporarily unavailable."""


# ── Client ────────────────────────────────────────────────────────────────────

class SparkClient:
    def __init__(self, base_url: str, timeout: int = 30, auth: Any = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth

    def _retry_dec(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, SparkServiceUnavailable)
            ),
            reraise=True,
        )

    async def _get(self, path: str, params: dict | None = None) -> dict:
        @self._retry_dec()
        async def _execute() -> dict:
            async with httpx.AsyncClient(
                base_url=self._base_url,
                auth=self._auth,
                timeout=self._timeout,
                verify=False,
                follow_redirects=True,
            ) as client:
                try:
                    resp = await client.get(path, params=params)
                except httpx.TransportError:
                    raise
                if resp.status_code == 401:
                    if "negotiate" in resp.headers.get("www-authenticate", "").lower():
                        raise SpnegoRequiredError(f"SPNEGO required for {self._base_url}")
                if resp.status_code == 404:
                    raise SparkNotFoundError(f"Not found: {path}")
                if resp.status_code in (503, 504):
                    raise SparkServiceUnavailable(
                        f"Spark HS unavailable: {resp.status_code}"
                    )
                if resp.status_code >= 400:
                    raise SparkClientError(f"Spark HS HTTP {resp.status_code}")
                try:
                    return resp.json()
                except ValueError as exc:
                    log.warning(
                        "spark_client.non_json_response",
                        status=resp.status_code,
                        content_type=resp.headers.get("content-type"),
                        body=resp.text[:300],
                    )
                    raise SparkClientError(
                        f"Spark HS returned non-JSON response (HTTP {resp.status_code}, "
                        f"content-type={resp.headers.get('content-type')!r}): "
                        f"{resp.text[:300]!r}"
                    ) from exc

        return await _execute()

    async def get_app(self, app_id: str) -> dict:
        """
        Get Spark application details.
        Tries both YARN app_id and Spark app_id formats.
        """
        try:
            data = await self._get(f"/api/v1/applications/{app_id}")
        except SparkNotFoundError:
            # Try alternate ID format
            if "application_" in app_id:
                alt_id = app_id.replace("application_", "spark_")
            else:
                alt_id = f"application_{app_id}"
            try:
                data = await self._get(f"/api/v1/applications/{alt_id}")
            except SparkNotFoundError:
                raise SparkNotFoundError(
                    f"Spark app '{app_id}' not found "
                    "(tried both YARN and Spark IDs)."
                )

        last_attempt = (data.get("attempts") or [{}])[-1]
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "spark_user": data.get("sparkUser"),
            "completed": last_attempt.get("completed"),
            "start_time": last_attempt.get("startTime"),
            "end_time": last_attempt.get("endTime"),
            "duration_ms": last_attempt.get("duration"),
            "total_executor_run_time_ms": last_attempt.get("executorRunTime"),
        }

    async def get_stages(
        self, app_id: str, status: str | None = None
    ) -> list[dict]:
        """Get stages for a Spark application."""
        params = {"status": status} if status else None
        try:
            data = await self._get(
                f"/api/v1/applications/{app_id}/stages", params=params
            )
        except SparkNotFoundError:
            return [{"error": f"Spark app '{app_id}' not found."}]

        result = []
        for stage in data if isinstance(data, list) else []:
            failure_reason = stage.get("failureReason", "") or ""
            if len(failure_reason) > 300:
                failure_reason = failure_reason[:300] + "..."
            result.append(
                {
                    "stage_id": stage.get("stageId"),
                    "name": stage.get("name"),
                    "status": stage.get("status"),
                    "num_tasks": stage.get("numTasks"),
                    "num_failed_tasks": stage.get("numFailedTasks"),
                    "input_bytes": stage.get("inputBytes"),
                    "output_bytes": stage.get("outputBytes"),
                    "shuffle_read_bytes": stage.get("shuffleReadBytes"),
                    "shuffle_write_bytes": stage.get("shuffleWriteBytes"),
                    "executor_run_time_ms": stage.get("executorRunTime"),
                    "failure_reason": failure_reason,
                }
            )
        return result

    async def list_apps(
        self, status: str | None = None, limit: int = 20
    ) -> list[dict]:
        """List Spark applications (compact)."""
        params: dict = {"limit": limit}
        if status:
            params["status"] = status
        data = await self._get("/api/v1/applications", params=params)
        apps = data if isinstance(data, list) else []
        return [
            {
                "id": a.get("id"),
                "name": a.get("name"),
                "spark_user": a.get("sparkUser"),
                "completed": (a.get("attempts") or [{}])[-1].get("completed"),
                "start_time": (a.get("attempts") or [{}])[-1].get("startTime"),
                "duration_ms": (a.get("attempts") or [{}])[-1].get("duration"),
            }
            for a in apps
        ]
