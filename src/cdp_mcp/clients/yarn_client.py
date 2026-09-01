"""
yarn_client.py — Async client for YARN ResourceManager REST API.
"""
from __future__ import annotations

from datetime import datetime
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

class YarnClientError(Exception):
    """Base exception for YARN client errors."""


class YarnAuthError(YarnClientError):
    """Authentication failure."""


class YarnNotFoundError(YarnClientError):
    """Resource not found."""


class YarnServiceUnavailable(YarnClientError):
    """YARN ResourceManager temporarily unavailable."""


# ── Client ────────────────────────────────────────────────────────────────────

class YarnClient:
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        username: str | None = None,
        password: str | None = None,
        auth: Any = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        # `auth` (an httpx Auth, e.g. SPNEGO) takes precedence over basic
        # username/password when both are supplied.
        if auth is not None:
            self._auth = auth
        elif username and password:
            self._auth = (username, password)
        else:
            self._auth = None

    def _retry_dec(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, YarnServiceUnavailable)
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
                verify=False,  # internal services often self-signed
                follow_redirects=True,
            ) as client:
                try:
                    resp = await client.get(path, params=params)
                except httpx.TransportError:
                    raise
                if resp.status_code == 401:
                    if "negotiate" in resp.headers.get("www-authenticate", "").lower():
                        raise SpnegoRequiredError(f"SPNEGO required for {self._base_url}")
                    raise YarnAuthError(f"YARN auth failed: {self._base_url}")
                if resp.status_code == 404:
                    raise YarnNotFoundError(f"YARN resource not found: {path}")
                if resp.status_code in (503, 504):
                    raise YarnServiceUnavailable(
                        f"YARN unavailable: {resp.status_code}"
                    )
                if resp.status_code >= 400:
                    raise YarnClientError(
                        f"YARN HTTP {resp.status_code}: {resp.text[:200]}"
                    )
                try:
                    return resp.json()
                except ValueError as exc:
                    log.warning(
                        "yarn_client.non_json_response",
                        status=resp.status_code,
                        content_type=resp.headers.get("content-type"),
                        body=resp.text[:300],
                    )
                    raise YarnClientError(
                        f"YARN returned non-JSON response (HTTP {resp.status_code}, "
                        f"content-type={resp.headers.get('content-type')!r}): "
                        f"{resp.text[:300]!r}"
                    ) from exc

        return await _execute()

    async def get_app(self, app_id: str) -> dict:
        """
        Get details of a YARN application.
        Returns state, final_status, diagnostics, resource usage and timing info.
        """
        data = await self._get(f"/ws/v1/cluster/apps/{app_id}")
        app = data.get("app", data)
        diagnostics = app.get("diagnostics", "") or ""
        if len(diagnostics) > 500:
            diagnostics = diagnostics[:500] + "..."
        result = {
            "app_id": app.get("id"),
            "name": app.get("name"),
            "user": app.get("user"),
            "queue": app.get("queue"),
            "state": app.get("state"),
            "final_status": app.get("finalStatus"),
            "progress": app.get("progress"),
            "tracking_url": app.get("trackingUrl"),
            "diagnostics": diagnostics,
            "elapsed_time_secs": round(app.get("elapsedTime", 0) / 1000, 1),
            "memory_seconds": app.get("memorySeconds"),
            "vcore_seconds": app.get("vcoreSeconds"),
            "started_time": app.get("startedTime"),
            "finished_time": app.get("finishedTime"),
            "cluster_id": app.get("clusterId"),
        }
        if app.get("finalStatus") == "FAILED" and not diagnostics:
            result["diagnostics"] = (
                "No diagnostics available. "
                "Check logs with get_service_logs(service_name='YARN', ...)."
            )
        return result

    @staticmethod
    def _to_millis(iso_time: str) -> int:
        dt = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)

    async def list_apps(
        self,
        state: str | None = None,
        queue: str | None = None,
        user: str | None = None,
        started_after: str | None = None,
        started_before: str | None = None,
        finished_after: str | None = None,
        finished_before: str | None = None,
        min_duration_secs: int | None = None,
        max_duration_secs: int | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """List YARN applications (compact, no diagnostics)."""
        params: dict = {}
        if state:
            params["states"] = state
        if queue:
            params["queues"] = queue
        if user:
            params["user"] = user
        started_after_ms = self._to_millis(started_after) if started_after else None
        started_before_ms = self._to_millis(started_before) if started_before else None
        finished_after_ms = self._to_millis(finished_after) if finished_after else None
        finished_before_ms = self._to_millis(finished_before) if finished_before else None
        if started_after_ms is not None:
            params["startedTimeBegin"] = started_after_ms
        if started_before_ms is not None:
            params["startedTimeEnd"] = started_before_ms
        if finished_after_ms is not None:
            params["finishedTimeBegin"] = finished_after_ms
        if finished_before_ms is not None:
            params["finishedTimeEnd"] = finished_before_ms

        # Sent server-side as a hint, but not trusted: some RM versions/configs
        # silently ignore startedTimeBegin/End and finishedTimeBegin/End and
        # return their whole cached app list regardless (confirmed live --
        # requests for non-overlapping weeks returned identical results
        # spanning the full multi-month range). So time bounds are always
        # re-applied client-side below, same as duration (which YARN has no
        # server-side filter for at all). Sending `limit` here would truncate
        # the result set before that filtering has a chance to run, so it's
        # only sent to YARN when no client-side filter is in play.
        client_side_filter = (
            min_duration_secs is not None
            or max_duration_secs is not None
            or started_after_ms is not None
            or started_before_ms is not None
            or finished_after_ms is not None
            or finished_before_ms is not None
        )
        if not client_side_filter:
            params["limit"] = limit

        data = await self._get("/ws/v1/cluster/apps", params=params)
        apps = (data.get("apps") or {}).get("app", []) or []

        if started_after_ms is not None:
            apps = [a for a in apps if a.get("startedTime", 0) >= started_after_ms]
        if started_before_ms is not None:
            apps = [a for a in apps if a.get("startedTime", 0) <= started_before_ms]
        if finished_after_ms is not None:
            apps = [
                a
                for a in apps
                if a.get("finishedTime", 0) > 0
                and a.get("finishedTime", 0) >= finished_after_ms
            ]
        if finished_before_ms is not None:
            apps = [
                a
                for a in apps
                if a.get("finishedTime", 0) > 0
                and a.get("finishedTime", 0) <= finished_before_ms
            ]
        if min_duration_secs is not None:
            apps = [
                a for a in apps if a.get("elapsedTime", 0) / 1000 >= min_duration_secs
            ]
        if max_duration_secs is not None:
            apps = [
                a for a in apps if a.get("elapsedTime", 0) / 1000 <= max_duration_secs
            ]

        apps_sorted = sorted(
            apps, key=lambda a: a.get("startedTime", 0), reverse=True
        )
        return [
            {
                "app_id": a.get("id"),
                "name": a.get("name"),
                "user": a.get("user"),
                "queue": a.get("queue"),
                "state": a.get("state"),
                "final_status": a.get("finalStatus"),
                "progress": a.get("progress"),
                "elapsed_time_secs": round(a.get("elapsedTime", 0) / 1000, 1),
                "started_time": a.get("startedTime"),
            }
            for a in apps_sorted[:limit]
        ]

    async def get_queue(self, queue_name: str | None = None) -> dict:
        """Get YARN scheduler queue info."""
        data = await self._get("/ws/v1/cluster/scheduler")
        scheduler = data.get("scheduler", {}).get("schedulerInfo", {})
        if not queue_name:
            return self._extract_queue_summary(scheduler)
        return (
            self._find_queue(scheduler, queue_name.lower())
            or {"error": f"Queue '{queue_name}' not found."}
        )

    def _extract_queue_summary(self, q: dict) -> dict:
        return {
            "name": q.get("queueName", q.get("type", "root")),
            "capacity": q.get("capacity"),
            "used_capacity": q.get("usedCapacity"),
            "absolute_capacity": q.get("absoluteCapacity"),
            "absolute_used_capacity": q.get("absoluteUsedCapacity"),
            "num_pending_applications": q.get("numPendingApplications", 0),
            "num_active_applications": q.get("numActiveApplications", 0),
            "num_containers_pending": q.get("numContainersPending", 0),
        }

    def _find_queue(self, node: dict, name: str) -> dict | None:
        if node.get("queueName", "").lower() == name:
            return self._extract_queue_summary(node)
        for child in (node.get("queues", {}).get("queue", []) or []):
            found = self._find_queue(child, name)
            if found:
                return found
        return None
