"""
cm_client.py — Async HTTP client for the Cloudera Manager REST API.
(Original code by dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from cdp_mcp.config import ClouderaManagerSettings, ServerSettings


def _to_num(v: Any) -> int | float:
    """Coerce a CM counter value to a number, treating None/str as 0."""
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int, float)):
        return v
    return 0

log = structlog.get_logger(__name__)


# ── Exception hierarchy ───────────────────────────────────────────────────────

class CMClientError(Exception):
    """Base exception for all CM client errors."""


class CMAuthError(CMClientError):
    """Authentication or authorisation failure."""


class CMNotFoundError(CMClientError):
    """Resource not found (HTTP 404)."""


class CMServiceUnavailable(CMClientError):
    """CM is temporarily unavailable (HTTP 503/504 or transport error)."""


class CMCommandFailed(CMClientError):
    """An asynchronous CM command finished with success=False."""


# ── Client ────────────────────────────────────────────────────────────────────

class ClouderaManagerClient:
    """Async HTTP client wrapping the Cloudera Manager REST API."""

    def __init__(
        self,
        settings: ClouderaManagerSettings,
        server_cfg: ServerSettings,
    ) -> None:
        self.cfg = settings
        self._server_cfg = server_cfg
        self._http: httpx.AsyncClient | None = None

    # ── Async context manager ─────────────────────────────────────────────────

    async def __aenter__(self) -> ClouderaManagerClient:
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=self.cfg.base_url,
            auth=(self.cfg.username, self.cfg.password),
            verify=self.cfg.effective_verify_ssl,
            timeout=httpx.Timeout(self.cfg.timeout_seconds),
            limits=httpx.Limits(
                max_connections=self._server_cfg.max_concurrent_requests,
                max_keepalive_connections=5,
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        log.debug("cm_client.connected", host=self.cfg.effective_host)

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()
            self._http = None
            log.debug("cm_client.closed", host=self.cfg.effective_host)

    # ── Retry / request helpers ───────────────────────────────────────────────

    def _retry_decorator(self):
        return retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=8),
            retry=retry_if_exception_type(
                (httpx.TransportError, CMServiceUnavailable)
            ),
            reraise=True,
        )

    async def _request(
        self,
        method: str,
        path: str,
        params: dict | None = None,
        json: Any | None = None,
        headers: dict | None = None,
    ) -> Any:
        assert self._http, "Client not initialised. Call connect() or use async with."

        @self._retry_decorator()
        async def _execute() -> Any:
            try:
                response = await self._http.request(
                    method, path, params=params, json=json, headers=headers
                )
            except httpx.TransportError as exc:
                log.warning("cm_client.transport_error", path=path, error=str(exc))
                raise

            if response.status_code == 401:
                raise CMAuthError(
                    f"Authentication failed for {self.cfg.host}. "
                    "Check username and password."
                )
            if response.status_code == 403:
                raise CMAuthError(f"Permission denied: {path}")
            if response.status_code == 404:
                raise CMNotFoundError(f"Resource not found: {path}")
            if response.status_code in (503, 504):
                log.warning(
                    "cm_client.server_unavailable",
                    status=response.status_code,
                    path=path,
                )
                raise CMServiceUnavailable(
                    f"CM unavailable (HTTP {response.status_code}): {self.cfg.host}"
                )
            if response.status_code >= 400:
                raise CMClientError(
                    f"CM returned HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
            return response.json() if response.content else {}

        return await _execute()

    async def _get(self, path: str, params: dict | None = None) -> Any:
        log.debug("cm_client.get", path=path)
        return await self._request("GET", path, params=params)

    async def _put(self, path: str, json: Any = None) -> Any:
        log.debug("cm_client.put", path=path)
        return await self._request("PUT", path, json=json)

    async def _post(self, path: str, json: Any = None) -> Any:
        log.debug("cm_client.post", path=path)
        return await self._request("POST", path, json=json)

    async def _get_text(self, path: str, params: dict | None = None) -> str:
        assert self._http, "Client not initialised."
        log.debug("cm_client.get_text", path=path)
        # The client default Accept: application/json is rejected with 406 by
        # text-producing endpoints (e.g. role log fetch); override it here.
        response = await self._http.get(
            path, params=params, headers={"Accept": "text/plain, */*"}
        )
        if response.status_code == 404:
            raise CMNotFoundError(f"Resource not found: {path}")
        if response.status_code >= 400:
            raise CMClientError(
                f"CM returned HTTP {response.status_code}: {response.text[:200]}"
            )
        return response.text

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    @staticmethod
    def _validate_time_range(
        start_time: str | None,
        end_time: str | None,
    ) -> tuple[str, str]:
        from datetime import timedelta
        now = datetime.now(UTC)
        if end_time is None:
            end_time = now.isoformat()
        if start_time is None:
            start_time = (now - timedelta(hours=1)).isoformat()
        return start_time, end_time

    # ── Cluster / service ─────────────────────────────────────────────────────

    async def list_clusters(self) -> list[dict]:
        data = await self._get("/clusters")
        return data.get("items", [])

    async def list_services(self, cluster_name: str) -> list[dict]:
        data = await self._get(f"/clusters/{cluster_name}/services")
        return data.get("items", [])

    async def get_service(self, cluster_name: str, service_name: str) -> dict:
        return await self._get(f"/clusters/{cluster_name}/services/{service_name}")

    async def list_roles(self, cluster_name: str, service_name: str) -> list[dict]:
        """Lightweight role status listing (healthSummary/roleState/commissionState)."""
        data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/roles"
        )
        return data.get("items", [])

    async def get_role(
        self, cluster_name: str, service_name: str, role_name: str
    ) -> dict:
        return await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/roles/{role_name}"
        )

    async def get_cluster_utilization(
        self,
        cluster_name: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> dict[str, Any]:
        """
        CM enforces a hard, undocumented-in-our-code-until-now limit: the
        'to'/'from' duration must be under 30 days (confirmed live -- a
        36-day request fails with HTTP 400: "the duration between 'to' and
        'from' must be less than 30 days"). Requests wider than 29 days are
        split into <=29-day chunks and merged (avg* fields: duration-weighted
        average; max* fields: true max across chunks, carrying the matching
        *TimestampMs; everything else: taken from the most recent chunk) so
        callers -- including monthly/quarterly reporting workflows -- don't
        need to know about or implement this CM quirk themselves.
        """
        from datetime import timedelta

        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)

        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        chunk_days = 29

        chunks: list[tuple[str, str]] = []
        if (end_dt - start_dt) <= timedelta(days=chunk_days):
            chunks = [(start_time, end_time)]
        else:
            cursor = start_dt
            while cursor < end_dt:
                chunk_end = min(cursor + timedelta(days=chunk_days), end_dt)
                chunks.append((cursor.isoformat(), chunk_end.isoformat()))
                cursor = chunk_end

        responses: list[tuple[str, str, dict]] = []
        for chunk_start, chunk_end in chunks:
            params: dict[str, Any] = {"from": chunk_start, "to": chunk_end}
            data = await self._get(
                f"/clusters/{cluster_name}/utilization", params=params
            )
            responses.append((chunk_start, chunk_end, data))

        merged = (
            dict(responses[0][2])
            if len(responses) == 1
            else self._merge_utilization_chunks(responses)
        )
        merged["time_range_defaulted"] = time_range_defaulted
        merged["effective_range"] = {"start": start_time, "end": end_time}
        merged["chunked"] = len(responses) > 1
        merged["num_chunks"] = len(responses)
        return merged

    @staticmethod
    def _merge_utilization_chunks(
        responses: list[tuple[str, str, dict]],
    ) -> dict[str, Any]:
        def _duration_days(s: str, e: str) -> float:
            sd = datetime.fromisoformat(s.replace("Z", "+00:00"))
            ed = datetime.fromisoformat(e.replace("Z", "+00:00"))
            return max((ed - sd).total_seconds() / 86400, 1e-9)

        weights = [_duration_days(s, e) for s, e, _ in responses]
        total_weight = sum(weights)
        all_keys: set[str] = set()
        for _, _, data in responses:
            all_keys.update(data.keys())

        merged: dict[str, Any] = {}
        for key in all_keys:
            if key.endswith("TimestampMs"):
                continue  # handled alongside its paired max* key below
            values = [data.get(key) for _, _, data in responses]
            if key.startswith("avg"):
                numeric = [
                    (w, v) for w, v in zip(weights, values, strict=True)
                    if isinstance(v, int | float)
                ]
                merged[key] = (
                    sum(w * v for w, v in numeric) / total_weight if numeric else None
                )
            elif key.startswith("max"):
                numeric = [
                    (v, i) for i, v in enumerate(values) if isinstance(v, int | float)
                ]
                if numeric:
                    best_val, best_idx = max(numeric, key=lambda t: t[0])
                    merged[key] = best_val
                    ts_key = f"{key}TimestampMs"
                    if ts_key in all_keys:
                        merged[ts_key] = responses[best_idx][2].get(ts_key)
                else:
                    merged[key] = values[-1]
            else:
                merged[key] = values[-1]
        return merged

    async def list_replication_schedules(
        self,
        cluster_name: str,
        service_name: str,
        max_schedules: int | None = None,
        max_commands: int = 1,
        view: str = "summary",
    ) -> dict[str, Any]:
        """
        List replication schedules for a service -- use this for discovery
        (schedule ids/names/type), not for full run history.

        CM defaults to maxCommands=0 (unlimited embedded recent history), which
        pushes HDFS/Hive responses past the MCP tool-result cap on services with
        many schedules (confirmed live). We cap maxCommands=1 by default so the
        response stays small; call get_replication_history() or
        get_replication_metrics() for actual run history over a time window.

        Server-side caps (CM v32+; confirmed live on v51): maxSchedules,
        maxCommands, view (summary|full).
        """
        params: dict[str, Any] = {"view": view, "maxCommands": max_commands}
        if max_schedules is not None:
            params["maxSchedules"] = max_schedules
        data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/replications",
            params=params,
        )
        items = data.get("items", [])
        # Heuristic: a schedule cap that returned exactly the cap likely has more.
        truncated = max_schedules is not None and len(items) == max_schedules
        return {
            "items": items,
            "count": len(items),
            "truncated": truncated,
            "applied_limits": {
                "maxSchedules": max_schedules,
                "maxCommands": max_commands,
                "view": view,
            },
        }

    async def get_replication_history(
        self,
        cluster_name: str,
        service_name: str,
        schedule_id: int,
        limit: int = 50,
        start_time: str | None = None,
        end_time: str | None = None,
        view: str = "summary",
        max_scan: int = 10000,
    ) -> dict[str, Any]:
        """
        Get run history for one replication schedule, paginated.

        CM's /replications/{id}/history has NO server-side time filter (only
        limit/offset/view -- confirmed against the v51 docs), so a wide window
        is covered by paging by offset with a client-side cutoff on each run's
        startTime (history is returned newest-first). This paginates internally
        up to `limit` in-range runs or max_scan raw runs, like get_alerts().

        view=summary already carries all scalar counters (numBytesCopied,
        tableCount, errorCount, etc.) at ~10-40x smaller than full (confirmed
        live); use view=full only for per-failure detail (failedFiles, errors,
        tables).
        """
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))

        page_size = 100
        offset = 0
        raw_items: list[dict] = []
        truncated = False
        while True:
            params: dict[str, Any] = {
                "limit": page_size,
                "offset": offset,
                "view": view,
            }
            data = await self._get(
                f"/clusters/{cluster_name}/services/{service_name}"
                f"/replications/{schedule_id}/history",
                params=params,
            )
            page = data.get("items", [])
            if not page:
                break
            raw_items.extend(page)
            # History is newest-first: once the oldest item in a page is older
            # than start_time we have left the window -- stop.
            oldest = page[-1].get("startTime")
            if oldest:
                oldest_dt = datetime.fromisoformat(oldest.replace("Z", "+00:00"))
                if oldest_dt < start_dt:
                    break
            if len(page) < page_size:
                break
            if len(raw_items) >= max_scan:
                truncated = True
                break
            offset += page_size

        in_range = []
        for it in raw_items:
            st = it.get("startTime")
            if not st:
                continue
            sdt = datetime.fromisoformat(st.replace("Z", "+00:00"))
            if start_dt <= sdt < end_dt:
                in_range.append(it)
        in_range.sort(key=lambda it: it.get("startTime", ""), reverse=True)
        return {
            "items": in_range[:limit],
            "count": min(len(in_range), limit),
            "total_in_range": len(in_range),
            "truncated": truncated,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
        }

    async def get_replication_metrics(
        self,
        cluster_name: str,
        service_name: str,
        start_time: str | None = None,
        end_time: str | None = None,
        schedule_id: int | None = None,
        max_schedules: int = 100,
        max_runs_per_schedule: int = 1000,
        include_failures: bool = True,
        max_failures: int = 20,
    ) -> dict[str, Any]:
        """
        Aggregated replication execution metrics over a time window -- the
        right tool for a monthly report. Paginates each schedule's history
        (via get_replication_history, view=summary) and accumulates the
        per-run counters into per-schedule totals, returning a compact
        summary that stays well under the tool-result cap even for an hourly
        schedule over a month (~720 runs).

        If schedule_id is None, iterates all schedules on the service (capped
        by max_schedules via list_replication_schedules); otherwise reports
        just that one schedule.
        """
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)

        if schedule_id is not None:
            schedules = [{"id": schedule_id, "displayName": str(schedule_id)}]
            schedule_truncated = False
        else:
            disc = await self.list_replication_schedules(
                cluster_name,
                service_name,
                max_schedules=max_schedules,
                max_commands=1,
            )
            schedules = disc["items"]
            schedule_truncated = disc["truncated"]

        per_schedule: list[dict[str, Any]] = []
        total_scanned = 0
        for sch in schedules:
            sid = sch.get("id")
            name = sch.get("displayName") or sch.get("name") or str(sid)
            hist = await self.get_replication_history(
                cluster_name,
                service_name,
                sid,
                limit=max_runs_per_schedule,
                start_time=start_time,
                end_time=end_time,
                view="summary",
                max_scan=max(max_runs_per_schedule, 10000),
            )
            runs = hist["items"]
            agg = self._aggregate_replication_runs(name, sch, runs)
            agg["schedule_id"] = sid
            agg["runs"] = len(runs)
            agg["total_in_range"] = hist["total_in_range"]
            agg["truncated"] = hist["truncated"] or hist["total_in_range"] > len(runs)
            if include_failures:
                agg["failures"] = [
                    {
                        "id": r.get("id"),
                        "start_time": r.get("startTime"),
                        "end_time": r.get("endTime"),
                        "result_message": (r.get("resultMessage") or "")[:300],
                    }
                    for r in runs
                    if r.get("success") is False
                ][:max_failures]
            else:
                agg["failures"] = []
            per_schedule.append(agg)
            total_scanned += len(runs)

        return {
            "schedules": per_schedule,
            "schedule_count": len(per_schedule),
            "schedule_truncated": schedule_truncated,
            "scanned_runs": total_scanned,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
        }

    @staticmethod
    def _aggregate_replication_runs(
        name: str, schedule: dict, runs: list[dict]
    ) -> dict[str, Any]:
        """Sum per-run replication counters (HDFS/Ozone and Hive result objects)
        into a compact per-schedule summary. Generically handles other result
        types by name only -- extend _COUNTER_PATHS to aggregate new types."""
        # Service type from the schedule's arguments block (no explicit type
        # field on ApiReplicationSchedule); fall back to the run's result key.
        stype = (
            "HIVE" if schedule.get("hiveArguments")
            else "HDFS" if schedule.get("hdfsArguments")
            else "UNKNOWN"
        )
        if stype == "UNKNOWN" and runs:
            for r in runs:
                for key, name in (
                    ("hdfsResult", "HDFS"),
                    ("hiveResult", "HIVE"),
                    ("ozoneResult", "OZONE"),
                    ("icebergResult", "ICEBERG"),
                    ("rangerResult", "RANGER"),
                    ("hiveOnTezResult", "HIVE_ON_TEZ"),
                ):
                    if r.get(key):
                        stype = name
                        break
                if stype != "UNKNOWN":
                    break
        agg: dict[str, Any] = {
            "name": name,
            "type": stype,
            "succeeded": 0,
            "failed": 0,
            "total_bytes_copied": 0,
            "total_files_copied": 0,
            "total_files_failed": 0,
            "total_files_skipped": 0,
            "total_files_deleted": 0,
            "total_tables_processed": 0,
            "total_errors": 0,
            "durations_seconds": [],
            "first_run": None,
            "last_run": None,
        }
        for run in runs:
            if run.get("success") is True:
                agg["succeeded"] += 1
            elif run.get("success") is False:
                agg["failed"] += 1
            st = run.get("startTime")
            et = run.get("endTime")
            if st:
                if agg["first_run"] is None or st < agg["first_run"]:
                    agg["first_run"] = st
                if agg["last_run"] is None or st > agg["last_run"]:
                    agg["last_run"] = st
            if st and et:
                try:
                    s = datetime.fromisoformat(st.replace("Z", "+00:00"))
                    e = datetime.fromisoformat(et.replace("Z", "+00:00"))
                    agg["durations_seconds"].append((e - s).total_seconds())
                except ValueError:
                    pass
            # HDFS/Ozone counters, or Hive's embedded HDFS-style result.
            res = run.get("hdfsResult") or (
                run.get("hiveResult") or {}
            ).get("dataReplicationResult")
            if res:
                agg["total_bytes_copied"] += _to_num(res.get("numBytesCopied"))
                agg["total_files_copied"] += _to_num(res.get("numFilesCopied"))
                agg["total_files_failed"] += _to_num(res.get("numFilesCopyFailed"))
                agg["total_files_skipped"] += _to_num(res.get("numFilesSkipped"))
                agg["total_files_deleted"] += _to_num(res.get("numFilesDeleted"))
            hr = run.get("hiveResult")
            if hr:
                agg["total_tables_processed"] += _to_num(hr.get("tableProcessed"))
                agg["total_errors"] += _to_num(hr.get("errorCount"))
        durations = agg.pop("durations_seconds")
        agg["avg_duration_seconds"] = (
            round(sum(durations) / len(durations), 1) if durations else 0
        )
        return agg

    async def list_parcels(self, cluster_name: str) -> list[dict]:
        data = await self._get(f"/clusters/{cluster_name}/parcels")
        return data.get("items", [])

    async def list_cluster_commands(
        self, cluster_name: str, limit: int = 20
    ) -> list[dict]:
        """No server-side pagination on this CM endpoint; limit is applied client-side."""
        data = await self._get(
            f"/clusters/{cluster_name}/commands", params={"view": "full"}
        )
        items = data.get("items", [])
        return items[:limit]

    # ── Impala ────────────────────────────────────────────────────────────────

    async def get_impala_queries(
        self,
        cluster_name: str,
        service_name: str,
        filter_str: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        params: dict[str, Any] = {
            "from": start_time,
            "to": end_time,
            "limit": limit,
        }
        if filter_str:
            params["filter"] = filter_str
        data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/impalaQueries",
            params=params,
        )
        return {
            "items": data.get("queries", []),
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
        }

    async def get_cluster_security_info(self, cluster_name: str) -> dict:
        """Combined TLS and Kerberos status for a cluster."""
        tls = await self._get(f"/clusters/{cluster_name}/isTlsEnabled")
        kerberos = await self._get(f"/clusters/{cluster_name}/kerberosInfo")
        return {"tls": tls, "kerberos": kerberos}

    # ── Logs ──────────────────────────────────────────────────────────────────

    async def get_service_logs(
        self,
        cluster_name: str,
        service_name: str,
        max_lines: int = 500,
        role_name: str | None = None,
        max_roles: int = 10,
    ) -> dict[str, list[str]]:
        """
        Retrieve logs for role(s) of a service in parallel (max 5 concurrent).
        Returns a dict of {role_name: [log_lines]}.

        CM's /logs/full has no documented or functional server-side line
        limit (verified empirically -- it returns the complete log file,
        which can be tens of MB, no matter what query params are sent), so
        each role fetch always transfers the full log over the network;
        max_lines only bounds what's kept after the fact, not the transfer
        itself. Without role_name, this fetches every role of the service --
        on a service with many roles (e.g. HDFS with dozens of DataNodes)
        that's dozens of full-log transfers and can make a single call take
        minutes. GATEWAY roles are skipped by default since they have no
        daemon/log file (confirmed: CM returns 404 for them). When no
        role_name is given, the (post-GATEWAY-filter) role list is capped at
        max_roles; a "_truncated" marker is added to the result so the
        caller knows to narrow with role_name or raise max_roles explicitly.
        """
        roles_data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/roles"
        )
        roles = roles_data.get("items", [])

        if role_name:
            roles = [r for r in roles if r.get("name") == role_name]
        else:
            roles = [r for r in roles if r.get("type") != "GATEWAY"]

        truncated = False
        if not role_name and len(roles) > max_roles:
            truncated = True
            roles = roles[:max_roles]

        log.info(
            "cm_client.get_service_logs",
            cluster=cluster_name,
            service=service_name,
            num_roles=len(roles),
            truncated=truncated,
        )

        semaphore = asyncio.Semaphore(5)
        result: dict[str, list[str]] = {}

        async def _fetch_role_log(role: dict) -> None:
            role_name = role.get("name", "unknown")
            async with semaphore:
                try:
                    # /logs/full has no documented (or functional -- verified
                    # empirically) server-side line limit; it always returns the
                    # complete log file regardless of any query param. Fetch it
                    # in full and truncate to the last max_lines client-side.
                    text = await self._get_text(
                        f"/clusters/{cluster_name}/services/{service_name}"
                        f"/roles/{role_name}/logs/full"
                    )
                    result[role_name] = text.splitlines()[-max_lines:]
                except CMNotFoundError:
                    log.debug(
                        "cm_client.role_log_not_found",
                        role=role_name,
                    )
                    result[role_name] = []
                except Exception as exc:
                    log.warning(
                        "cm_client.role_log_error",
                        role=role_name,
                        error=str(exc),
                    )
                    result[role_name] = [f"[Error fetching log: {exc}]"]

        await asyncio.gather(*[_fetch_role_log(r) for r in roles])
        if truncated:
            result["_truncated"] = [
                f"Only the first {max_roles} roles were fetched (GATEWAY roles "
                "excluded). Pass role_name to target a specific role, or "
                "max_roles to raise the cap."
            ]
        return result

    # ── Alerts / events ───────────────────────────────────────────────────────

    async def get_alerts(
        self,
        cluster_name: str,
        category: str | None = None,
        severity: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        limit: int = 50,
        max_scan: int = 10000,
    ) -> dict[str, Any]:
        """
        CM has no /clusters/{name}/events resource; events are only queryable
        via the global /events endpoint. Cluster association lives in the
        free-form attributes list (undocumented key), so cluster_name is
        applied client-side after fetching a larger buffer server-side.

        On an active cluster, a single maxResults-capped request can cover
        only a small fraction of a wide [start_time, end_time] window --
        confirmed live: a 5-week request returned only ~1.3 hours of actual
        coverage. This paginates via resultOffset until the requested range
        is fully scanned or max_scan raw events have been fetched (whichever
        comes first), and reports which happened via "truncated" so a caller
        never silently gets a partial answer that looks complete.
        """
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        filters = [
            f"timeReceived=ge={start_time}",
            f"timeReceived=lt={end_time}",
        ]
        if category:
            filters.append(f"category=={category}")
        if severity:
            filters.append(f"severity=={severity}")
        query = ";".join(filters)

        page_size = 100
        offset = 0
        raw_items: list[dict] = []
        truncated = False
        while True:
            params: dict[str, Any] = {
                "query": query,
                "maxResults": page_size,
                "resultOffset": offset,
            }
            data = await self._get("/events", params=params)
            page = data.get("items", [])
            raw_items.extend(page)
            if len(page) < page_size:
                break
            if len(raw_items) >= max_scan:
                truncated = True
                break
            offset += page_size

        matched = [
            item
            for item in raw_items
            if any(
                cluster_name in attr.get("values", [])
                for attr in item.get("attributes", [])
            )
        ]
        matched.sort(key=lambda item: item.get("timeOccurred", ""), reverse=True)
        return {
            "items": matched[:limit],
            "total_matched_in_range": len(matched),
            "truncated": truncated,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
        }

    # ── Metrics ───────────────────────────────────────────────────────────────

    # CM's /timeseries has no server-side point cap, and a wide range times
    # many metric_names can return a huge number of raw {timestamp, value}
    # points per series -- confirmed live: an uncapped request produced a
    # ~17MB response that took long enough for a downstream MCP client to
    # give up and drop the connection entirely. Each series exceeding
    # MAX_TIMESERIES_POINTS is capped client-side, in one of two caller-
    # selectable ways (sample_mode): "even" downsamples to an even spread
    # across the full requested range (first/last sample kept, preserves
    # trend shape end-to-end at reduced resolution -- the default, best for
    # capacity/trend queries); "recent" keeps the most recent points at full
    # native resolution, dropping everything older (best for "what's this
    # host doing right now" incident response). Same envelope style as
    # get_alerts/get_replication_*.
    MAX_TIMESERIES_POINTS = 2000
    SAMPLE_MODES = ("even", "recent")

    # CM embeds a full aggregateStatistics block (min/max/mean/stdDev/count/
    # sampleTime/minTime/maxTime) in every point at some rollup granularities
    # -- confirmed live: ~330 bytes/point vs ~65 for timestamp+value+type, the
    # dominant size driver in real report-generator traffic (4-8MB responses
    # that never even hit MAX_TIMESERIES_POINTS, because the point *count*
    # was fine -- the point *payload* wasn't).
    #
    # Stripped by default (include_aggregate_stats=False) for the common
    # trend/troubleshooting case, but callers that need min/max/mean/stdDev
    # (e.g. a report chart) can opt in per call with include_aggregate_stats
    # =True to get the full block back -- don't silently drop data a caller
    # explicitly asked for. When stripped, its absence is NOT a signal of
    # CM's rollup level (RAW/TEN_MINUTELY/HOURLY/SIX_HOURLY/DAILY) -- CM's
    # own rollup aging as data ages is a real, separate effect on point
    # spacing/count; check `timeSeries[].metadata.rollupUsed` in the
    # response for actual resolution, never the presence of
    # aggregateStatistics (an earlier version of this comment conflated the
    # two after a same-call comparison that turned out to straddle this
    # code's own deploy boundary rather than reflect CM's rollup aging).
    _POINT_FIELDS = ("timestamp", "value", "type")

    @classmethod
    def _slim_point(cls, point: dict) -> dict:
        return {k: v for k, v in point.items() if k in cls._POINT_FIELDS}

    @staticmethod
    def _downsample_evenly(points: list, max_points: int) -> list:
        n = len(points)
        if n <= max_points or max_points <= 1:
            return points[:max_points] if max_points >= 1 else points
        step = (n - 1) / (max_points - 1)
        seen: set[int] = set()
        result = []
        for k in range(max_points):
            idx = round(k * step)
            if idx not in seen:
                seen.add(idx)
                result.append(points[idx])
        return result

    @classmethod
    def _cap_timeseries_items(
        cls,
        items: list[dict],
        sample_mode: str = "even",
        include_aggregate_stats: bool = False,
    ) -> tuple[list[dict], bool]:
        if sample_mode not in cls.SAMPLE_MODES:
            raise ValueError(f"sample_mode must be one of {cls.SAMPLE_MODES}, got {sample_mode!r}")
        truncated = False
        capped_items = []
        for item in items:
            capped_series = []
            for ts in item.get("timeSeries") or []:
                raw_points = ts.get("data") or []
                points = (
                    list(raw_points)
                    if include_aggregate_stats
                    else [cls._slim_point(p) for p in raw_points]
                )
                if len(points) > cls.MAX_TIMESERIES_POINTS:
                    truncated = True
                    if sample_mode == "recent":
                        capped_points = points[-cls.MAX_TIMESERIES_POINTS :]
                    else:
                        capped_points = cls._downsample_evenly(points, cls.MAX_TIMESERIES_POINTS)
                    ts = {
                        **ts,
                        "data": capped_points,
                        "data_downsampled": sample_mode == "even",
                        "data_truncated": sample_mode == "recent",
                        "data_points_available": len(points),
                    }
                else:
                    ts = {**ts, "data": points}
                capped_series.append(ts)
            capped_items.append({**item, "timeSeries": capped_series})
        return capped_items, truncated

    @classmethod
    def _truncation_note(cls, sample_mode: str) -> str:
        if sample_mode == "recent":
            return (
                f"One or more series exceeded {cls.MAX_TIMESERIES_POINTS} points and "
                "were capped to the most recent points at full resolution -- pass "
                "sample_mode='even' for a full-range trend, or narrow start_time/"
                "end_time / reduce metric_names for full-resolution recent data."
            )
        return (
            f"One or more series exceeded {cls.MAX_TIMESERIES_POINTS} points and "
            "were downsampled to an even spread across the range (first/last "
            "sample kept) -- pass sample_mode='recent' for full-resolution recent "
            "data, or narrow start_time/end_time / reduce metric_names for full-"
            "resolution data."
        )

    async def get_service_metrics(
        self,
        cluster_name: str,
        service_name: str,
        metric_names: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
        sample_mode: str = "even",
        include_aggregate_stats: bool = False,
    ) -> dict[str, Any]:
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        metric_selector = ", ".join(metric_names)
        tsquery = (
            f"SELECT {metric_selector} "
            f"WHERE clusterName = {cluster_name!r} "
            f"AND serviceName = {service_name!r}"
        )
        body = {
            "query": tsquery,
            "from": start_time,
            "to": end_time,
        }
        data = await self._post("/timeseries", json=body)
        items, truncated = self._cap_timeseries_items(
            data.get("items", []), sample_mode, include_aggregate_stats
        )
        result: dict[str, Any] = {
            "items": items,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
            "truncated": truncated,
        }
        if truncated:
            result["_truncated"] = [self._truncation_note(sample_mode)]
        return result

    async def get_host_metrics(
        self,
        hostname: str,
        metric_names: list[str],
        start_time: str | None = None,
        end_time: str | None = None,
        sample_mode: str = "even",
        include_aggregate_stats: bool = False,
    ) -> dict[str, Any]:
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        metric_selector = ", ".join(metric_names)
        tsquery = f"SELECT {metric_selector} WHERE hostname = {hostname!r}"
        body = {
            "query": tsquery,
            "from": start_time,
            "to": end_time,
        }
        data = await self._post("/timeseries", json=body)
        items, truncated = self._cap_timeseries_items(
            data.get("items", []), sample_mode, include_aggregate_stats
        )
        result: dict[str, Any] = {
            "items": items,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
            "truncated": truncated,
        }
        if truncated:
            result["_truncated"] = [self._truncation_note(sample_mode)]
        return result

    async def list_available_metrics(self, name_contains: str | None = None) -> list[dict]:
        """
        Discover metric names for use with get_service_metrics/get_host_metrics.
        ApiMetricSchema has no entity-type field to filter on server-side, so
        name_contains does a simple client-side substring match on name/displayName.
        """
        data = await self._get("/timeseries/schema")
        items = data.get("items", [])
        if name_contains:
            needle = name_contains.lower()
            items = [
                item
                for item in items
                if needle in item.get("name", "").lower()
                or needle in item.get("displayName", "").lower()
            ]
        return items

    # ── Config ────────────────────────────────────────────────────────────────

    async def get_config(
        self,
        cluster_name: str,
        service_name: str,
        view: str = "full",
    ) -> list[dict]:
        data = await self._get(
            f"/clusters/{cluster_name}/services/{service_name}/config",
            params={"view": view},
        )
        return data.get("items", [])

    async def update_config(
        self,
        cluster_name: str,
        service_name: str,
        configs: list[dict],
    ) -> dict:
        """
        Update service config items.
        configs: list of {"name": str, "value": str} dicts.
        """
        body = {"items": configs}
        return await self._put(
            f"/clusters/{cluster_name}/services/{service_name}/config",
            json=body,
        )

    # ── Commands ──────────────────────────────────────────────────────────────

    async def run_service_command(
        self,
        cluster_name: str,
        service_name: str,
        command: str,
    ) -> dict:
        data = await self._post(
            f"/clusters/{cluster_name}/services/{service_name}/commands/{command}"
        )
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "active": data.get("active"),
            "success": data.get("success"),
            "resultMessage": data.get("resultMessage"),
        }

    async def get_command_status(self, command_id: int) -> dict:
        data = await self._get(f"/commands/{command_id}")
        return {
            "id": data.get("id"),
            "name": data.get("name"),
            "active": data.get("active"),
            "success": data.get("success"),
            "resultMessage": data.get("resultMessage"),
        }

    # ── Hosts ─────────────────────────────────────────────────────────────────

    async def get_host_status(
        self,
        cluster_name: str | None = None,
        host_filter: str | None = None,
    ) -> list[dict]:
        if cluster_name:
            data = await self._get(f"/clusters/{cluster_name}/hosts")
        else:
            params: dict[str, Any] = {}
            if host_filter:
                params["filter"] = host_filter
            data = await self._get("/hosts", params=params or None)

        hosts = data.get("items", [])
        result = []
        for host in hosts:
            result.append(
                {
                    "hostname": host.get("hostname"),
                    "hostId": host.get("hostId"),
                    "ipAddress": host.get("ipAddress"),
                    "healthSummary": host.get("healthSummary"),
                    "entityStatus": host.get("entityStatus"),
                    "numCores": host.get("numCores"),
                    "totalPhysMemBytes": host.get("totalPhysMemBytes"),
                    "roleRefs": host.get("roleRefs", []),
                }
            )
        return result

    # ── Audit events ──────────────────────────────────────────────────────────

    async def get_audit_events(
        self,
        cluster_name: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        service_name: str | None = None,
        user_name: str | None = None,
        limit: int = 50,
        max_scan: int = 10000,
    ) -> dict[str, Any]:
        """
        CM has no /clusters/{name}/audits resource -- audits are only
        queryable via the global /audits endpoint, and ApiAudit carries no
        cluster reference at all (only a service name). cluster_name is used
        upstream to pick the right CM client; it cannot filter server-side
        here, so it's intentionally not applied to the request.

        Same maxResults-vs-range problem as get_alerts (confirmed live: a
        default 1h call returned 4 events, a 5-week-range call returned 50
        events but only spanning ~12h) -- paginates via resultOffset until
        the requested range is fully scanned or max_scan events are fetched,
        reporting which happened via "truncated".
        """
        time_range_defaulted = start_time is None and end_time is None
        start_time, end_time = self._validate_time_range(start_time, end_time)
        filters = []
        if service_name:
            filters.append(f"service=={service_name}")
        if user_name:
            filters.append(f"username=={user_name}")
        query = ";".join(filters) if filters else None

        page_size = 100
        offset = 0
        items: list[dict] = []
        truncated = False
        while True:
            params: dict[str, Any] = {
                "startTime": start_time,
                "endTime": end_time,
                "maxResults": page_size,
                "resultOffset": offset,
            }
            if query:
                params["query"] = query
            data = await self._get("/audits", params=params)
            page = data.get("items", [])
            items.extend(page)
            if len(page) < page_size:
                break
            if len(items) >= max_scan:
                truncated = True
                break
            offset += page_size

        items.sort(key=lambda item: item.get("timestamp", ""), reverse=True)
        return {
            "items": items[:limit],
            "total_matched_in_range": len(items),
            "truncated": truncated,
            "time_range_defaulted": time_range_defaulted,
            "effective_range": {"start": start_time, "end": end_time},
        }

    # ── Service / role management ─────────────────────────────────────────────

    async def delete_service(self, cluster_name: str, service_name: str) -> dict:
        """Delete a service from a cluster. Returns the deleted service object."""
        return await self._request(
            "DELETE",
            f"/clusters/{cluster_name}/services/{service_name}",
        )

    # ── Role management ───────────────────────────────────────────────────────

    async def delete_role(
        self,
        cluster_name: str,
        service_name: str,
        role_name: str,
    ) -> dict:
        """Delete a role instance from a service. Returns the deleted role object."""
        return await self._request(
            "DELETE",
            f"/clusters/{cluster_name}/services/{service_name}/roles/{role_name}",
        )

    # ── Management Service ────────────────────────────────────────────────────

    async def get_mgmt_service(self) -> dict:
        """Return CM Management Service state and health summary."""
        return await self._get("/cm/service")

    async def get_mgmt_service_roles(self) -> list[dict]:
        """Return all role instances of the CM Management Service."""
        data = await self._get("/cm/service/roles")
        return data.get("items", [])

    # ── DataHubs ──────────────────────────────────────────────────────────────

    async def list_datahubs(self) -> list[dict]:
        """
        List available DataHub clusters.
        For Knox proxy environments this returns the configured cluster name.
        For direct CM connections this is equivalent to list_clusters().
        """
        if self.cfg.use_knox and self.cfg.cluster_name:
            return [
                {
                    "name": self.cfg.cluster_name,
                    "displayName": self.cfg.cluster_name,
                    "fullVersion": "unknown",
                    "clusterType": "DATAHUB",
                    "environment": self.cfg.environment_name,
                }
            ]
        return await self.list_clusters()
