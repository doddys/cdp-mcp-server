"""
hdfs_client.py — Async client for HDFS NameNode JMX API.
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

class HdfsClientError(Exception):
    """Base exception for HDFS client errors."""


class HdfsServiceUnavailable(HdfsClientError):
    """HDFS NameNode temporarily unavailable."""


# ── Client ────────────────────────────────────────────────────────────────────

class HdfsClient:
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        auth: Any = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth

    def _retry_dec(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, HdfsServiceUnavailable)
            ),
            reraise=True,
        )

    async def _jmx(self, qry: str) -> dict:
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
                    resp = await client.get("/jmx", params={"qry": qry})
                except httpx.TransportError:
                    raise
                if resp.status_code == 401:
                    if "negotiate" in resp.headers.get("www-authenticate", "").lower():
                        raise SpnegoRequiredError(f"SPNEGO required for {self._base_url}")
                if resp.status_code in (503, 504):
                    raise HdfsServiceUnavailable(
                        f"HDFS NN unavailable: {resp.status_code}"
                    )
                if resp.status_code >= 400:
                    raise HdfsClientError(f"HDFS JMX HTTP {resp.status_code}")
                try:
                    return resp.json()
                except ValueError as exc:
                    log.warning(
                        "hdfs_client.non_json_response",
                        status=resp.status_code,
                        content_type=resp.headers.get("content-type"),
                        body=resp.text[:300],
                    )
                    raise HdfsClientError(
                        f"HDFS NN returned non-JSON response (HTTP {resp.status_code}, "
                        f"content-type={resp.headers.get('content-type')!r}): "
                        f"{resp.text[:300]!r}"
                    ) from exc

        return await _execute()

    async def get_namenode_status(self) -> dict:
        """Get HDFS NameNode status from JMX."""
        fs_data = await self._jmx(
            "Hadoop:service=NameNode,name=FSNamesystemState"
        )
        nn_data = await self._jmx(
            "Hadoop:service=NameNode,name=NameNodeStatus"
        )

        fs = (fs_data.get("beans") or [{}])[0]
        nn = (nn_data.get("beans") or [{}])[0]

        # JMX may return a counter key with value None (observed live for
        # CorruptBlocks/MissingBlocks on some clusters). `.get(k, 0)` returns
        # None in that case, which would crash the `> 0` health math below with
        # TypeError — so coerce None → 0 explicitly with `or 0`.
        capacity_total = fs.get("CapacityTotal") or 0
        capacity_used = fs.get("CapacityUsed") or 0
        capacity_remaining = fs.get("CapacityRemaining") or 0
        capacity_used_pct = (
            round(capacity_used / capacity_total * 100, 2) if capacity_total else 0
        )

        under_replicated = fs.get("UnderReplicatedBlocks") or 0
        corrupt_blocks = fs.get("CorruptBlocks") or 0
        missing_blocks = fs.get("MissingBlocks") or 0
        safe_mode = bool(fs.get("FSState", "Operational") != "Operational")

        if corrupt_blocks > 0 or missing_blocks > 0 or safe_mode:
            health_summary = "CRITICAL"
        elif under_replicated > 0:
            health_summary = "DEGRADED"
        else:
            health_summary = "HEALTHY"

        return {
            "health_summary": health_summary,
            "safe_mode": safe_mode,
            "under_replicated_blocks": under_replicated,
            "corrupt_blocks": corrupt_blocks,
            "missing_blocks": missing_blocks,
            "capacity_total_gb": round(capacity_total / (1024**3), 2),
            "capacity_used_gb": round(capacity_used / (1024**3), 2),
            "capacity_remaining_gb": round(capacity_remaining / (1024**3), 2),
            "capacity_used_pct": capacity_used_pct,
            "total_files_and_dirs": (
                (fs.get("FilesTotal") or 0) + (fs.get("Directories") or 0)
            ),
            "active_namenode": nn.get("HostAndPort"),
            "ha_state": nn.get("State"),
        }
