"""
hdfs_client.py — Async client for HDFS NameNode JMX + WebHDFS APIs.
"""
from __future__ import annotations

import urllib.parse
from datetime import datetime, timezone
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
        candidates: list[str] | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._auth = auth
        # All NameNode HTTP URLs (HA clusters have ≥2). JMX is served by every
        # NN; WebHDFS reads are served only by the active NN, so the client
        # fails over across candidates on a StandbyException. Defaults to the
        # single base_url for non-HA clusters.
        self._candidates = [c.rstrip("/") for c in candidates] if candidates else [
            self._base_url
        ]

    def _retry_dec(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, HdfsServiceUnavailable)
            ),
            reraise=True,
        )

    async def _get_json(self, url_path: str, params: dict, base_url: str | None = None) -> dict:
        """GET a JSON endpoint on the NameNode HTTP server (JMX or WebHDFS)
        with retry, SPNEGO, and the shared error mapping. Returns parsed JSON.
        ``base_url`` overrides the configured NN (used to try each HA candidate).
        """
        base = (base_url or self._base_url).rstrip("/")

        @self._retry_dec()
        async def _execute() -> dict:
            async with httpx.AsyncClient(
                base_url=base,
                auth=self._auth,
                timeout=self._timeout,
                verify=False,
                follow_redirects=True,
            ) as client:
                try:
                    resp = await client.get(url_path, params=params)
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
                    raise HdfsClientError(
                        f"HDFS HTTP {resp.status_code}: {resp.text[:300]}"
                    )
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

    async def _jmx(self, qry: str) -> dict:
        return await self._get_json("/jmx", {"qry": qry})

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

    async def get_directory_snapshots(self, path: str) -> dict:
        """List the snapshots of an HDFS directory via WebHDFS.

        Hits the NameNode WebHDFS API (same host:port as JMX, same auth) at
        ``/webhdfs/v1{path}/.snapshot?op=LISTSTATUS``. Each snapshot appears as a
        ``FileStatus`` entry — ``pathSuffix`` is the snapshot name and
        ``modificationTime`` (ms epoch) is its creation time. This op works on
        every Hadoop version that supports snapshots (the newer
        ``GETSNAPSHOTLIST`` adds snapshotID/deletionStatus but is Hadoop 3.4+).

        Args:
            path: HDFS directory path (must be snapshottable), e.g. ``/data/warehouse``.

        Returns:
            A bounded envelope ``{path, count, snapshots, truncated}`` where each
            snapshot is ``{snapshot_name, creation_time, owner, group,
            permission, type, children_num, size}``.

        Raises:
            HdfsClientError: if the path isn't snapshottable / doesn't exist /
                WebHDFS is disabled (the WebHDFS ``RemoteException`` message is
                carried in the error text), or if every candidate NameNode is
                standby/unavailable (HA — the active NN couldn't be reached).
            SpnegoRequiredError: if the endpoint challenges with SPNEGO.
            HdfsServiceUnavailable: only if every candidate NN returns 503/504.
        """
        norm = path.rstrip("/")
        url_path = "/webhdfs/v1" + urllib.parse.quote(norm, safe="/") + "/.snapshot"

        # WebHDFS reads are served ONLY by the active NameNode. On an HA cluster
        # a standby NN rejects with StandbyException (HTTP 403, "...state
        # standby"); it does not redirect. Try each candidate, failing over on a
        # standby rejection (and on a unavailable NN). A non-standby error — e.g.
        # the path isn't snapshottable — is raised immediately, since failover
        # won't help a path problem. SpnegoRequiredError propagates: the whole
        # cluster needs SPNEGO, so the next candidate would reject the same way.
        data: dict | None = None
        last_error: Exception | None = None
        for base in self._candidates:
            try:
                data = await self._get_json(
                    url_path, {"op": "LISTSTATUS"}, base_url=base
                )
                break
            except HdfsServiceUnavailable as exc:
                last_error = exc
                continue  # NN down/unavailable → try the next candidate
            except HdfsClientError as exc:
                msg = str(exc).lower()
                if "standbyexception" in msg or "state standby" in msg:
                    last_error = exc
                    continue  # standby → fail over to the active NN
                raise  # path error (not snapshottable / not found) — don't fail over

        if data is None:
            raise HdfsClientError(
                "All HDFS NameNodes rejected the WebHDFS read (standby or "
                f"unavailable). Last error: {last_error}"
            )

        statuses = (data.get("FileStatuses") or {}).get("FileStatus") or []

        snapshots = []
        for s in statuses:
            ms = s.get("modificationTime") or 0
            snapshots.append(
                {
                    "snapshot_name": s.get("pathSuffix"),
                    "creation_time": (
                        datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()  # noqa: UP017 -- 3.8-compatible (collector bundle)
                        if ms
                        else None
                    ),
                    "owner": s.get("owner"),
                    "group": s.get("group"),
                    "permission": s.get("permission"),
                    "type": s.get("type"),
                    "children_num": s.get("childrenNum"),
                    "size": s.get("length"),
                }
            )

        return {
            "path": path,
            "count": len(snapshots),
            "snapshots": snapshots,
            "truncated": False,
        }
