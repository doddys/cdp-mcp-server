"""
server.py — FastMCP server entry point for cdp-mcp.
(Based on dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

import asyncio
import hmac
import json
import os
import sys
from contextlib import asynccontextmanager
from typing import Any

import structlog
from mcp.server.fastmcp import FastMCP

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.clients.oozie_client import OozieNotFoundError
from cdp_mcp.clients.spark_client import SparkNotFoundError
from cdp_mcp.clients.yarn_client import YarnNotFoundError
from cdp_mcp.cm_pool import CMPool
from cdp_mcp.config import ServerSettings, build_registry

log = structlog.get_logger(__name__)

server_cfg = ServerSettings()
_registry = None
_pool: CMPool | None = None
_lifespan_lock = asyncio.Lock()
_active_sessions = 0


# ── Lifespan ──────────────────────────────────────────────────────────────────
# NOTE: under the streamable-http/sse transports, the mcp library's
# StreamableHTTPSessionManager invokes this lifespan once per *session*, not
# once per process (each concurrent client connection gets its own
# `Server.run()` call). _registry/_pool are process-wide singletons shared by
# every tool call via module-level globals, so treating them as per-session
# state and tearing them down on every session exit races: one session's
# stop() can close the httpx clients a different, still-active session is
# using mid-request ("Client not initialised" AssertionError). Reference-count
# sessions instead: the first session in starts the shared pool, the last
# session out stops it. For stdio there is exactly one session per process,
# so this behaves exactly as a plain start/stop.
@asynccontextmanager
async def _lifespan(server):
    global _registry, _pool, _active_sessions
    async with _lifespan_lock:
        if _pool is None:
            try:
                _registry = build_registry(server_cfg)
                _registry.start()
                instances = _registry.get_all()
                _pool = CMPool(instances, server_cfg)
                await _pool.start()
                log.info("cdp_mcp.ready", instances=len(instances))
            except Exception:
                # Startup failed partway through: don't leave a half-built
                # pool/registry sitting in the globals, or every subsequent
                # session will see `_pool is not None` and silently reuse
                # the broken instance instead of retrying.
                _pool = None
                _registry = None
                raise
        _active_sessions += 1
    try:
        yield
    finally:
        async with _lifespan_lock:
            _active_sessions -= 1
            if _active_sessions == 0:
                try:
                    await _pool.stop()
                    _registry.stop()
                finally:
                    _pool = None
                    _registry = None


# ── Transport selection ──────────────────────────────────────────────────────
# FastMCP (mcp 1.x) defaults to the stdio transport: it reads JSON-RPC from stdin
# and writes to stdout, so it is meant to be spawned by an MCP client (Claude
# Desktop / CLI) as a child process. Under a standalone systemd service stdin is
# /dev/null → the server reads EOF and exits cleanly (exit 0) immediately after
# startup. To run cdp-mcp as a long-lived network daemon instead, set
# MCP_TRANSPORT=streamable-http (or sse) plus MCP_HOST/MCP_PORT. host/port are
# bound at FastMCP construction (run() reads them from self.settings); the
# transport is chosen at run() time. stdio stays the default so existing
# Claude Desktop configs are unchanged.
_VALID_TRANSPORTS = ("stdio", "sse", "streamable-http")


def _resolve_transport_settings(
    env: dict[str, str] | None = None,
) -> tuple[str, str, int]:
    """Read MCP_TRANSPORT / MCP_HOST / MCP_PORT from the environment.

    Returns ``(transport, host, port)``. ``transport`` is one of
    ``stdio`` / ``sse`` / ``streamable-http``. Defaults: stdio, 127.0.0.1, 8000.
    """
    e = env if env is not None else os.environ
    transport = (e.get("MCP_TRANSPORT") or "stdio").strip().lower() or "stdio"
    if transport not in _VALID_TRANSPORTS:
        raise RuntimeError(
            f"Unsupported MCP_TRANSPORT={transport!r}; expected one of "
            f"{', '.join(_VALID_TRANSPORTS)}."
        )
    host = (e.get("MCP_HOST") or "127.0.0.1").strip() or "127.0.0.1"
    port_raw = e.get("MCP_PORT") or "8000"
    try:
        port = int(port_raw)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Invalid MCP_PORT={port_raw!r}; expected an integer."
        ) from exc
    return transport, host, port


_transport, _host, _port = _resolve_transport_settings()
# FastMCP reads host/port from self.settings (set at construction), so bind them
# here from the environment; they are ignored under the stdio transport.
mcp = FastMCP("cdp-mcp", lifespan=_lifespan, host=_host, port=_port)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _dump(data: Any) -> str:
    return json.dumps(data, indent=2, default=str)


def _spnego_error(service: str) -> str:
    return _dump(
        {
            "error": (
                f"SPNEGO required for {service}; the endpoint is Kerberized. "
                "To use SPNEGO, enable Kerberos for this CM instance "
                "(kerberos: true / CM_KERBEROS=true) and obtain a TGT (kinit) "
                "or load a keytab into the default credentials cache. The "
                "optional httpx-gssapi package must be installed "
                "(`uv pip install -e '.[kerberos]'`). Set disable_on_spnego=false "
                "to retry each call instead of skipping after the first challenge."
            ),
            "spnego_required": True,
        }
    )


def _no_client(cluster_name: str) -> str:
    return _dump(
        {
            "error": (
                f"No Cloudera Manager found for cluster '{cluster_name}'. "
                "Use list_clusters() to see available clusters."
            )
        }
    )


# ── Original CM tools ─────────────────────────────────────────────────────────

@mcp.tool()
async def list_clusters() -> str:
    """
    List all CDP / Cloudera clusters managed by the configured CM instances.
    Returns cluster name, version, status and associated services.
    Use this as the starting point to discover available clusters.
    """
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client is None:
            continue
        try:
            clusters = await client.list_clusters()
            results.extend(clusters)
        except Exception as exc:
            log.error("tool.list_clusters.error", env=env_name, error=str(exc))
            results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


@mcp.tool()
async def list_services(cluster_name: str) -> str:
    """
    List all services running on a cluster.
    Returns service name, type, state and health status.

    Args:
      cluster_name: Cluster name as returned by list_clusters().
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.list_services(cluster_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_service(cluster_name: str, service_name: str) -> str:
    """
    Get detailed status for a single service: healthSummary, healthChecks,
    serviceState and config staleness. Lighter-weight than list_services when
    you already know which service to inspect.

    Args:
      cluster_name: Cluster name.
      service_name: Service name (e.g. YARN, SPARK_ON_YARN, HDFS, OOZIE).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.get_service(cluster_name, service_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_roles(cluster_name: str, service_name: str) -> str:
    """
    List role instances of a service with their status (healthSummary,
    roleState, commissionState, haStatus) — lighter-weight than
    get_service_logs when triaging before deciding to pull log content.

    Args:
      cluster_name: Cluster name.
      service_name: Service name (e.g. YARN, SPARK_ON_YARN, HDFS, OOZIE).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.list_roles(cluster_name, service_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_role_status(cluster_name: str, service_name: str, role_name: str) -> str:
    """
    Get detailed status for a single role instance.

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      role_name:    Role instance name, as returned by list_roles().
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.get_role(cluster_name, service_name, role_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_service_logs(
    cluster_name: str,
    service_name: str,
    max_lines: int = 500,
    role_name: str | None = None,
    max_roles: int = 10,
) -> str:
    """
    Retrieve recent log lines for role(s) of a service.
    Returns a dict mapping role_name → list of log lines.
    Useful for diagnosing service failures.

    CM has no server-side way to limit log size -- each role fetch always
    transfers the complete log file (can be tens of MB); max_lines only
    trims what's returned, not what's downloaded. Without role_name, this
    fetches every role of the service -- on services with many roles (e.g.
    HDFS with many DataNodes) that can take minutes. Prefer passing
    role_name (from list_roles()) once you know which host/role is
    relevant. Unfiltered calls are capped at max_roles (default 10);
    GATEWAY roles are always skipped since they have no logs.

    Args:
      cluster_name: Cluster name.
      service_name: Service name (e.g. YARN, SPARK_ON_YARN, HDFS, OOZIE).
      max_lines:    Maximum log lines per role (default 500).
      role_name:    Fetch logs for just this one role (from list_roles()).
      max_roles:    Cap on roles fetched when role_name is not given (default 10).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_service_logs(
                cluster_name, service_name, max_lines,
                role_name=role_name, max_roles=max_roles,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_alerts(
    cluster_name: str,
    category: str | None = None,
    severity: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 50,
    max_scan: int = 10000,
) -> str:
    """
    Get cluster alert events from Cloudera Manager.

    Paginates internally to cover the full [start_time, end_time] window
    rather than silently stopping at `limit` (on an active cluster, a single
    capped request can cover only a small fraction of a wide range). Check
    the response's "truncated" field: true means max_scan raw events were
    fetched before the full range was covered, so the result may be missing
    older in-range events -- raise max_scan or narrow the range if so.
    "time_range_defaulted": true means start_time/end_time were omitted and
    defaulted to the last hour -- check "effective_range" for what was
    actually queried.

    Args:
      cluster_name: Cluster name.
      category:     Event category filter (e.g. HEALTH_CHECK, LOG_MESSAGE).
      severity:     Severity filter (e.g. CRITICAL, WARNING, INFORMATIONAL).
      start_time:   ISO 8601 start time (default: 1 hour ago).
      end_time:     ISO 8601 end time (default: now).
      limit:        Maximum number of events to return (default 50).
      max_scan:     Safety cap on raw events scanned while paginating (default 10000).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_alerts(
                cluster_name,
                category=category,
                severity=severity,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
                max_scan=max_scan,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_service_metrics(
    cluster_name: str,
    service_name: str,
    metric_names: list[str],
    start_time: str | None = None,
    end_time: str | None = None,
) -> str:
    """
    Query time-series metrics for a service via the CM tsquery API.
    Response: {"items": [...], "time_range_defaulted": bool, "effective_range": {...}}.
    "time_range_defaulted": true means start_time/end_time were omitted and
    silently defaulted to the last hour -- check "effective_range" for what
    was actually queried before assuming this covers a longer period.

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      metric_names: List of metric names (e.g. ["cpu_user_rate", "mem_rss"]).
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_service_metrics(
                cluster_name,
                service_name,
                metric_names,
                start_time=start_time,
                end_time=end_time,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_host_metrics(
    cluster_name: str,
    hostname: str,
    metric_names: list[str],
    start_time: str | None = None,
    end_time: str | None = None,
) -> str:
    """
    Query time-series metrics for a single host (CPU, memory, disk, network)
    via the CM tsquery API. Use list_available_metrics() to discover metric
    names first if unsure what to pass.
    Response: {"items": [...], "time_range_defaulted": bool, "effective_range": {...}}.
    "time_range_defaulted": true means start_time/end_time were omitted and
    silently defaulted to the last hour.

    Args:
      cluster_name: Cluster the host belongs to (used to pick the right CM instance).
      hostname:     Host to query metrics for.
      metric_names: List of metric names (e.g. ["cpu_percent", "physical_memory_used"]).
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_host_metrics(
                hostname,
                metric_names,
                start_time=start_time,
                end_time=end_time,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_available_metrics(
    name_contains: str | None = None,
    cluster_name: str | None = None,
) -> str:
    """
    Discover available CM metric names/descriptions for use with
    get_service_metrics() and get_host_metrics(). Metric schema is
    CM-instance-wide, not per-cluster -- but in a registry with multiple CM
    instances/environments, different instances can expose different schemas
    (different CM/CDH versions, different installed services). Pass
    cluster_name to scope this to the CM instance that actually manages the
    cluster you're querying metrics for; without it, this silently queries
    whichever CM environment happens to be registered first, which may not
    match your target cluster in a multi-instance deployment.

    Args:
      name_contains: Optional case-insensitive substring filter (e.g. "cpu").
      cluster_name:  If set, scope to the CM instance managing this cluster.
    """
    if cluster_name:
        client = _pool.get_client_for_cluster(cluster_name)
        if client is None:
            return _no_client(cluster_name)
        try:
            return _dump(await client.list_available_metrics(name_contains=name_contains))
        except Exception as exc:
            return _dump({"error": str(exc)})

    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client is None:
            continue
        try:
            return _dump(await client.list_available_metrics(name_contains=name_contains))
        except Exception as exc:
            return _dump({"error": str(exc)})
    return _dump({"error": "No CM clients available."})


@mcp.tool()
async def list_impala_queries(
    cluster_name: str,
    service_name: str,
    filter_str: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    limit: int = 50,
) -> str:
    """
    List Impala queries via Cloudera Manager's own query monitoring (no
    SPNEGO required, unlike the direct Impala/YARN service UIs). Useful for
    finding slow or stuck queries.
    Response: {"items": [...], "time_range_defaulted": bool, "effective_range": {...}}.
    service_name is required (this is service-scoped, not cluster-wide) --
    get it from list_services().

    Args:
      cluster_name: Cluster name.
      service_name: Impala service name (required -- see list_services()).
      filter_str:   CM filter expression, e.g. "user=root" or
                     "query_duration > 5s and (user=root or user=alice)".
      start_time:   ISO 8601 start time (default: 1 hour before end_time).
      end_time:     ISO 8601 end time (default: now).
      limit:        Maximum queries to return (default 50).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_impala_queries(
                cluster_name,
                service_name,
                filter_str=filter_str,
                start_time=start_time,
                end_time=end_time,
                limit=limit,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_config(
    cluster_name: str,
    service_name: str,
    view: str = "full",
) -> str:
    """
    Get the configuration of a service (all parameters with current values and defaults).

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      view:         "full" (all params) or "summary" (only explicitly set params).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.get_config(cluster_name, service_name, view=view))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def update_config(
    cluster_name: str,
    service_name: str,
    configs: list[dict],
) -> str:
    """
    Update one or more configuration parameters for a service.
    Each item in configs must have 'name' and 'value' keys.

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      configs:      List of {"name": str, "value": str} dicts.

    WARNING: This is a write operation. Changes may require a service restart to take effect.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.update_config(cluster_name, service_name, configs))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def run_service_command(
    cluster_name: str,
    service_name: str,
    command: str,
) -> str:
    """
    Execute a service-level command (e.g. restart, start, stop, refresh).
    Returns the command ID which can be polled with get_command_status().

    Args:
      cluster_name: Cluster name.
      service_name: Service name.
      command:      Command name (e.g. "restart", "start", "stop", "refresh").

    WARNING: This is a write operation that affects a running service.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.run_service_command(cluster_name, service_name, command)
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_command_status(command_id: int) -> str:
    """
    Check the status of an asynchronous CM command.
    Poll this after run_service_command() to know when it completes.

    Args:
      command_id: Command ID as returned by run_service_command().
    """
    # Use any available client for command status (commands are global)
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client:
            try:
                return _dump(await client.get_command_status(command_id))
            except Exception as exc:
                return _dump({"error": str(exc)})
    return _dump({"error": "No CM clients available."})


@mcp.tool()
async def list_cluster_commands(cluster_name: str, limit: int = 20) -> str:
    """
    List recent commands executed against a cluster (start/stop/restart,
    config deploys, upgrades, etc.) with success/failure status. Use this
    to discover a command_id for get_command_status(), or to see what ran
    recently without already knowing an ID.

    Args:
      cluster_name: Cluster name.
      limit:        Maximum commands to return (default 20).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.list_cluster_commands(cluster_name, limit=limit))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_host_status(
    cluster_name: str | None = None,
    host_filter: str | None = None,
) -> str:
    """
    Get health and role information for cluster hosts.

    Args:
      cluster_name: If set, return only hosts in this cluster.
      host_filter:  Optional CM filter expression (e.g. "hostname = myhost").
    """
    if cluster_name:
        client = _pool.get_client_for_cluster(cluster_name)
        if client is None:
            return _no_client(cluster_name)
        try:
            return _dump(
                await client.get_host_status(
                    cluster_name=cluster_name,
                    host_filter=host_filter,
                )
            )
        except Exception as exc:
            return _dump({"error": str(exc)})

    # No cluster specified: query all environments
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client:
            try:
                results.extend(
                    await client.get_host_status(host_filter=host_filter)
                )
            except Exception as exc:
                results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


@mcp.tool()
async def get_cluster_security_info(cluster_name: str) -> str:
    """
    Get TLS and Kerberos status for a cluster. Useful to confirm upfront
    whether downstream service UIs (YARN RM, Spark HS, HDFS NN, Oozie) will
    require SPNEGO authentication before calling their tools.

    Args:
      cluster_name: Cluster name.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.get_cluster_security_info(cluster_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_cluster_utilization(
    cluster_name: str,
    start_time: str | None = None,
    end_time: str | None = None,
) -> str:
    """
    Get aggregated CPU/memory utilization for a cluster (capacity planning).

    Ranges over 29 days are automatically split into multiple CM calls and
    merged (CM itself hard-rejects any single request wider than 30 days) --
    check "chunked"/"num_chunks" in the response if you need to know this
    happened. "time_range_defaulted": true means start_time/end_time were
    omitted and silently defaulted to the last hour.

    Args:
      cluster_name: Cluster name.
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time (default: now).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_cluster_utilization(
                cluster_name, start_time=start_time, end_time=end_time
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_replication_schedules(
    cluster_name: str,
    service_name: str,
    max_schedules: int | None = None,
    max_commands: int = 1,
    view: str = "summary",
) -> str:
    """
    List replication schedules for a service (HDFS/Hive/etc replication jobs).

    Use this for discovery (schedule ids/names/type/last-run status), NOT for
    full run history. It caps embedded recent history at max_commands=1 by
    default so the response stays small; call get_replication_history() (raw
    per-run detail) or get_replication_metrics() (aggregated over a window) for
    actual execution history.

    service_name is required (service-scoped despite the name -- get it from
    list_services()); there is no single call that returns replication schedules
    across all services on a cluster.

    Check the response's "truncated" field: true means max_schedules was hit and
    more schedules exist -- raise max_schedules if so.

    Args:
      cluster_name:  Cluster name.
      service_name:  Service name (required -- e.g. HDFS, HIVE; see list_services()).
      max_schedules: Optional cap on number of schedules returned (default: all).
      max_commands:  Embedded recent-history runs per schedule (default 1; CM
                     default 0 = unlimited, which can exceed the tool-result cap).
      view:          "summary" (default) or "full" for more per-run detail.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.list_replication_schedules(
                cluster_name,
                service_name,
                max_schedules=max_schedules,
                max_commands=max_commands,
                view=view,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_replication_history(
    cluster_name: str,
    service_name: str,
    schedule_id: int,
    limit: int = 50,
    start_time: str | None = None,
    end_time: str | None = None,
    view: str = "summary",
    max_scan: int = 10000,
) -> str:
    """
    Get run history for one replication schedule, paginated over a time window.

    Use list_replication_schedules() first to get the schedule_id. CM has no
    server-side time filter on this endpoint, so the window is applied
    client-side while paging by offset (history is returned newest-first).

    The response paginates internally up to `limit` in-range runs (or max_scan
    raw runs, whichever comes first). "truncated": true means max_scan was hit
    before the full window was covered -- raise max_scan or narrow the range.
    "total_in_range" tells you how many runs matched regardless of `limit`.
    For an aggregated 1-month report across many schedules, prefer
    get_replication_metrics() over paging this raw detail.

    view=summary (default) carries the per-run counters (bytes/files copied,
    tableCount, errorCount) at ~10-40x smaller than full; use view=full only
    when you need per-failure detail (failedFiles, errors, tables).

    Args:
      cluster_name: Cluster name.
      service_name: Service name (e.g. HDFS, HIVE).
      schedule_id:  Replication schedule ID (from list_replication_schedules()).
      limit:        Maximum in-range runs to return (default 50).
      start_time:   ISO 8601 start time (default: 1 hour ago).
      end_time:     ISO 8601 end time (default: now).
      view:         "summary" (default) or "full".
      max_scan:     Safety cap on raw runs scanned while paginating (default 10000).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_replication_history(
                cluster_name,
                service_name,
                schedule_id,
                limit=limit,
                start_time=start_time,
                end_time=end_time,
                view=view,
                max_scan=max_scan,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_replication_metrics(
    cluster_name: str,
    service_name: str,
    start_time: str | None = None,
    end_time: str | None = None,
    schedule_id: int | None = None,
    max_schedules: int = 100,
    max_runs_per_schedule: int = 1000,
    include_failures: bool = True,
    max_failures: int = 20,
) -> str:
    """
    Aggregated replication execution metrics over a time window -- the right
    tool for a monthly report.

    Paginates each schedule's history (view=summary) and accumulates the
    per-run counters into per-schedule totals: runs, succeeded, failed, total
    bytes/files copied, files failed, tables processed (Hive), errors, average
    duration, first/last run, plus a capped list of failed runs. The summary
    stays well under the tool-result cap even for an hourly schedule over a
    month (~720 runs), unlike paging raw history.

    If schedule_id is None, iterates all schedules on the service (capped by
    max_schedules); otherwise reports just that one. Per-schedule "truncated":
    true means max_runs_per_schedule was hit before the full window was
    aggregated -- raise it or narrow the range. "schedule_truncated": true
    means max_schedules was hit -- more schedules exist.

    For per-run raw detail (full counters, failedFiles, errors, tables), use
    get_replication_history() with view=full on a specific schedule.

    Args:
      cluster_name:           Cluster name.
      service_name:           Service name (e.g. HDFS, HIVE).
      start_time:             ISO 8601 start time (default: 1 hour ago).
      end_time:               ISO 8601 end time (default: now).
      schedule_id:            Optional single schedule ID (default: all on service).
      max_schedules:          Cap on schedules scanned when schedule_id is None (default 100).
      max_runs_per_schedule:  Safety cap on runs aggregated per schedule (default 1000).
      include_failures:       Include the capped failed-run list (default true).
      max_failures:           Max failed runs listed per schedule (default 20).
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(
            await client.get_replication_metrics(
                cluster_name,
                service_name,
                start_time=start_time,
                end_time=end_time,
                schedule_id=schedule_id,
                max_schedules=max_schedules,
                max_runs_per_schedule=max_runs_per_schedule,
                include_failures=include_failures,
                max_failures=max_failures,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_parcels(cluster_name: str) -> str:
    """
    List parcels (CDH/runtime distribution packages) available to a
    cluster, with version and activation/distribution stage per host.
    Useful after upgrades or when suspecting a version mismatch.

    Args:
      cluster_name: Cluster name.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        return _dump(await client.list_parcels(cluster_name))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def get_audit_events(
    cluster_name: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    service_name: str | None = None,
    user_name: str | None = None,
    limit: int = 50,
    max_scan: int = 10000,
) -> str:
    """
    Retrieve CM audit events (login, config changes, command executions).

    Paginates internally to cover the full [start_time, end_time] window
    rather than silently stopping at `limit`. Check the response's
    "truncated" field: true means max_scan events were fetched before the
    full range was covered. "time_range_defaulted": true means
    start_time/end_time were omitted and defaulted to the last hour -- check
    "effective_range" for what was actually queried.

    Args:
      cluster_name: If set, scope to this cluster.
      start_time:   ISO 8601 start time.
      end_time:     ISO 8601 end time.
      service_name: Filter by service name.
      user_name:    Filter by user who performed the action.
      limit:        Maximum events to return (default 50).
      max_scan:     Safety cap on events scanned while paginating (default 10000).
    """
    if cluster_name:
        client = _pool.get_client_for_cluster(cluster_name)
        if client is None:
            return _no_client(cluster_name)
    else:
        # Pick first available client
        client = None
        for env_name in _pool.list_environments():
            client = _pool.get_client_for_environment(env_name)
            if client:
                break
        if client is None:
            return _dump({"error": "No CM clients available."})

    try:
        return _dump(
            await client.get_audit_events(
                cluster_name=cluster_name,
                start_time=start_time,
                end_time=end_time,
                service_name=service_name,
                user_name=user_name,
                limit=limit,
                max_scan=max_scan,
            )
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def list_datahubs() -> str:
    """
    List all DataHub clusters across all configured CM environments.
    Returns cluster name, type, version and environment.
    """
    results = []
    for env_name in _pool.list_environments():
        client = _pool.get_client_for_environment(env_name)
        if client is None:
            continue
        try:
            hubs = await client.list_datahubs()
            for hub in hubs:
                hub.setdefault("environment", env_name)
            results.extend(hubs)
        except Exception as exc:
            log.error("tool.list_datahubs.error", env=env_name, error=str(exc))
            results.append({"error": str(exc), "environment": env_name})
    return _dump(results)


# ── Role management tools ─────────────────────────────────────────────────────

@mcp.tool()
async def delete_service(cluster_name: str, service_name: str) -> str:
    """
    Delete a service from a cluster in Cloudera Manager.
    Use this to remove stub, orphaned or decommissioned services
    (e.g. STUB_DFS, standalone Tez).

    The service must be stopped before deletion — CM will reject the request otherwise.

    Args:
      cluster_name: Cluster name (from list_clusters).
      service_name: Service name as shown in CM (e.g. STUB_DFS-4555, tez).

    WARNING: This is a destructive, irreversible operation.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        result = await client.delete_service(cluster_name, service_name)
        return _dump({"deleted": service_name, "result": result})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def delete_role(
    cluster_name: str,
    service_name: str,
    role_name: str,
) -> str:
    """
    Delete a role instance from a service in Cloudera Manager.
    Use this to remove stale, decommissioned or erroneously added role instances.

    The role must be stopped before deletion — CM will reject the request otherwise.
    Use run_service_command() or stop the role individually before calling this.

    Args:
      cluster_name: Cluster name (from list_clusters).
      service_name: Service name (e.g. HIVE, YARN, HDFS).
      role_name:    Full role name (e.g. hive-HIVESERVER2-abc123def456).

    WARNING: This is a destructive, irreversible operation.
    """
    client = _pool.get_client_for_cluster(cluster_name)
    if client is None:
        return _no_client(cluster_name)
    try:
        result = await client.delete_role(cluster_name, service_name, role_name)
        return _dump({"deleted": role_name, "result": result})
    except Exception as exc:
        return _dump({"error": str(exc)})


# ── CM Management Service tools ───────────────────────────────────────────────

@mcp.tool()
async def get_mgmt_service(environment_name: str | None = None) -> str:
    """
    Get the health and role status of the Cloudera Manager Management Service.
    This covers internal CM roles: Host Monitor, Service Monitor, Alert Publisher,
    Reports Manager, Event Server, Activity Monitor.
    These roles are NOT listed by list_services() — they live under /cm/service.

    Args:
      environment_name: CM environment to query (default: first available).
                        Use registry_list() to see environment names.
    """
    envs = _pool.list_environments()
    if environment_name:
        targets = [environment_name] if environment_name in envs else []
    else:
        targets = envs

    if not targets:
        return _dump({"error": "No CM environments available."})

    results = []
    for env in targets:
        client = _pool.get_client_for_environment(env)
        if client is None:
            continue
        try:
            svc = await client.get_mgmt_service()
            roles = await client.get_mgmt_service_roles()
            results.append({
                "environment": env,
                "name": svc.get("name"),
                "type": svc.get("type"),
                "serviceState": svc.get("serviceState"),
                "healthSummary": svc.get("healthSummary"),
                "configStalenessStatus": svc.get("configStalenessStatus"),
                "roles": [
                    {
                        "name": r.get("name"),
                        "type": r.get("type"),
                        "hostRef": r.get("hostRef", {}).get("hostname"),
                        "roleState": r.get("roleState"),
                        "healthSummary": r.get("healthSummary"),
                        "configStalenessStatus": r.get("configStalenessStatus"),
                    }
                    for r in roles
                ],
            })
        except Exception as exc:
            results.append({"environment": env, "error": str(exc)})

    return _dump(results if len(results) != 1 else results[0])


# ── Registry management tools ─────────────────────────────────────────────────

@mcp.tool()
async def refresh_cluster_map() -> str:
    """
    Rebuild the cluster → CM mapping and re-discover service endpoints.
    Call this after adding a new cluster or after CM failover.
    """
    try:
        await _pool.refresh_cluster_map()
        return _dump(
            {
                "status": "ok",
                "clusters": _pool.list_known_clusters(),
                "environments": _pool.list_environments(),
            }
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_list(include_inactive: bool = False) -> str:
    """
    List all registered CM instances (passwords excluded).

    Args:
      include_inactive: If True, include deactivated instances (default False).
    """
    try:
        return _dump(await _registry.async_list_raw(include_inactive=include_inactive))
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_stats() -> str:
    """
    Return registry statistics: total instances, active/inactive count,
    breakdown by environment.
    """
    try:
        return _dump(await _registry.async_get_stats())
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_add(
    host: str,
    port: int = 7183,
    username: str = "admin",
    password: str = "",
    environment_name: str = "default",
    use_tls: bool = True,
    verify_ssl: bool = True,
    api_version: str = "v51",
    timeout_seconds: int = 30,
) -> str:
    """
    Register a new Cloudera Manager instance.
    Not available with EnvRegistry (read-only backend).

    Args:
      host:             CM hostname or IP.
      port:             CM API port (default 7183).
      username:         CM username (default "admin").
      password:         CM password.
      environment_name: Logical environment label.
      use_tls:          Use HTTPS (default True).
      verify_ssl:       Verify TLS certificate (default True).
      api_version:      CM API version (default "v51").
      timeout_seconds:  Request timeout (default 30).
    """
    try:
        result = await _registry.async_register(
            host=host,
            port=port,
            username=username,
            password=password,
            environment_name=environment_name,
            use_tls=use_tls,
            verify_ssl=verify_ssl,
            api_version=api_version,
            timeout_seconds=timeout_seconds,
        )
        return _dump({"registered": result})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_deactivate(host: str) -> str:
    """
    Deactivate (soft-delete) a CM instance by hostname.
    The instance will no longer be used but remains in the registry.

    Args:
      host: CM hostname to deactivate.
    """
    try:
        await _registry.async_deactivate(host)
        return _dump({"deactivated": host})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_update_field(host: str, field: str, value: str) -> str:
    """
    Update a single field on a registered CM instance.
    Not available with EnvRegistry.

    Args:
      host:  CM hostname.
      field: Field name to update (e.g. "password", "port", "api_version").
      value: New value (always a string; will be coerced to the correct type).
    """
    try:
        await _registry.async_update_field(host, field, value)
        return _dump({"updated": host, "field": field})
    except NotImplementedError as exc:
        return _dump({"error": str(exc)})
    except Exception as exc:
        return _dump({"error": str(exc)})


@mcp.tool()
async def registry_reload() -> str:
    """
    Reload registry from the backend (re-read YAML file or Iceberg table)
    and reconnect all CM clients.
    Use after manually editing cm_instances.yaml.
    """
    try:
        instances = await _registry.async_load()
        await _pool.reload(instances)
        return _dump(
            {
                "status": "ok",
                "instances": len(instances),
                "clusters": _pool.list_known_clusters(),
            }
        )
    except Exception as exc:
        return _dump({"error": str(exc)})


# ── YARN tools ────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_yarn_app(cluster_name: str, app_id: str) -> str:
    """
    Get the status and details of a YARN application by ID.
    Returns state, final_status, diagnostics (error message if failed),
    tracking_url, resource usage and timing information.
    Use this to diagnose why a Spark / MapReduce / Oozie job failed.

    Args:
      cluster_name: DataHub cluster name (use list_clusters to discover).
      app_id:       YARN application ID (e.g. application_1234567890_0001).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the YARN service is running and reachable."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "yarn" in endpoints.spnego_required:
        return _spnego_error("YARN")
    try:
        client = _pool.get_yarn_client(cluster_name)
        return _dump(await client.get_app(app_id))
    except YarnNotFoundError as exc:
        return _dump({"error": str(exc)})
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "yarn")
        return _spnego_error("YARN")
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


@mcp.tool()
async def list_yarn_apps(
    cluster_name: str,
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
) -> str:
    """
    List recent YARN applications on a cluster.

    Args:
      cluster_name:       Cluster name.
      state:              Filter by state (e.g. RUNNING, FINISHED, FAILED, KILLED).
      queue:              Filter by queue name.
      user:               Filter by submitting user.
      started_after:      ISO 8601 time -- only apps started at/after this time.
      started_before:     ISO 8601 time -- only apps started at/before this time.
      finished_after:     ISO 8601 time -- only apps finished at/after this time.
      finished_before:    ISO 8601 time -- only apps finished at/before this time.
      min_duration_secs:  Only apps whose elapsed time is >= this many seconds.
      max_duration_secs:  Only apps whose elapsed time is <= this many seconds.
      limit:              Maximum applications to return (default 20).

    Time-range and duration filters are enforced client-side regardless of
    whether the RM honors them server-side (some RM versions/configs
    silently ignore startedTimeBegin/End and finishedTimeBegin/End and
    return their full cached app list) -- so results are always correctly
    bounded, though a wide/unbounded range can still only surface whatever
    YARN currently retains in its completed-applications cache.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "yarn" in endpoints.spnego_required:
        return _spnego_error("YARN")
    try:
        client = _pool.get_yarn_client(cluster_name)
        return _dump(
            await client.list_apps(
                state=state,
                queue=queue,
                user=user,
                started_after=started_after,
                started_before=started_before,
                finished_after=finished_after,
                finished_before=finished_before,
                min_duration_secs=min_duration_secs,
                max_duration_secs=max_duration_secs,
                limit=limit,
            )
        )
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "yarn")
        return _spnego_error("YARN")
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


@mcp.tool()
async def get_yarn_queue(
    cluster_name: str,
    queue_name: str | None = None,
) -> str:
    """
    Get YARN scheduler queue capacity and utilisation.
    If queue_name is omitted, returns the root queue summary.

    Args:
      cluster_name: Cluster name.
      queue_name:   Queue name to inspect (e.g. "default", "root.production").
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.yarn_rm_url:
        return _dump(
            {
                "error": (
                    f"YARN ResourceManager endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "yarn" in endpoints.spnego_required:
        return _spnego_error("YARN")
    try:
        client = _pool.get_yarn_client(cluster_name)
        return _dump(await client.get_queue(queue_name=queue_name))
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "yarn")
        return _spnego_error("YARN")
    except Exception as exc:
        return _dump({"error": f"YARN error: {exc}"})


# ── Spark tools ───────────────────────────────────────────────────────────────

@mcp.tool()
async def get_spark_app(cluster_name: str, app_id: str) -> str:
    """
    Get Spark application details from the Spark History Server.
    Accepts both YARN application IDs and Spark application IDs.

    Args:
      cluster_name: Cluster name.
      app_id:       YARN application ID or Spark application ID.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the SPARK_ON_YARN service is running."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "spark" in endpoints.spnego_required:
        return _spnego_error("Spark History Server")
    try:
        client = _pool.get_spark_client(cluster_name)
        return _dump(await client.get_app(app_id))
    except SparkNotFoundError as exc:
        return _dump({"error": str(exc)})
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "spark")
        return _spnego_error("Spark History Server")
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


@mcp.tool()
async def get_spark_stages(
    cluster_name: str,
    app_id: str,
    status: str | None = None,
) -> str:
    """
    Get stage-level details for a Spark application.
    Useful to identify slow or failed stages.

    Args:
      cluster_name: Cluster name.
      app_id:       Spark or YARN application ID.
      status:       Filter by stage status (e.g. FAILED, ACTIVE, COMPLETE).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "spark" in endpoints.spnego_required:
        return _spnego_error("Spark History Server")
    try:
        client = _pool.get_spark_client(cluster_name)
        return _dump(await client.get_stages(app_id, status=status))
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "spark")
        return _spnego_error("Spark History Server")
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


@mcp.tool()
async def list_spark_apps(
    cluster_name: str,
    status: str | None = None,
    limit: int = 20,
) -> str:
    """
    List recent Spark applications from the Spark History Server.

    Args:
      cluster_name: Cluster name.
      status:       Filter by status (e.g. completed, running).
      limit:        Maximum applications to return (default 20).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.spark_hs_url:
        return _dump(
            {
                "error": (
                    f"Spark History Server endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "spark" in endpoints.spnego_required:
        return _spnego_error("Spark History Server")
    try:
        client = _pool.get_spark_client(cluster_name)
        return _dump(await client.list_apps(status=status, limit=limit))
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "spark")
        return _spnego_error("Spark History Server")
    except Exception as exc:
        return _dump({"error": f"Spark error: {exc}"})


# ── HDFS tools ────────────────────────────────────────────────────────────────

@mcp.tool()
async def get_namenode_status(cluster_name: str) -> str:
    """
    Get HDFS NameNode health status, capacity usage and block health.
    Returns health_summary (HEALTHY / DEGRADED / CRITICAL), under-replicated
    blocks, corrupt blocks, disk usage, and HA state.

    Args:
      cluster_name: Cluster name.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.hdfs_nn_url:
        return _dump(
            {
                "error": (
                    f"HDFS NameNode endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the HDFS service is running."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "hdfs" in endpoints.spnego_required:
        return _spnego_error("HDFS NameNode")
    try:
        client = _pool.get_hdfs_client(cluster_name)
        return _dump(await client.get_namenode_status())
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "hdfs")
        return _spnego_error("HDFS NameNode")
    except Exception as exc:
        return _dump({"error": f"HDFS error: {exc}"})


@mcp.tool()
async def get_hdfs_snapshots(cluster_name: str, path: str) -> str:
    """
    List the HDFS snapshots of a directory (snapshot name + creation time, owner,
    group, permission) via the NameNode WebHDFS API at `<path>/.snapshot`. The
    directory must be snapshottable (enable with `hdfs dfs -allowSnapshot <path>`);
    if it isn't, or has no snapshots, the result reflects that — it does not
    create or modify snapshots (read-only).

    Args:
      cluster_name: Cluster name.
      path: HDFS directory path, e.g. `/data/warehouse`.
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.hdfs_nn_url:
        return _dump(
            {
                "error": (
                    f"HDFS NameNode endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the HDFS service is running."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "hdfs" in endpoints.spnego_required:
        return _spnego_error("HDFS NameNode")
    try:
        client = _pool.get_hdfs_client(cluster_name)
        return _dump(await client.get_directory_snapshots(path))
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "hdfs")
        return _spnego_error("HDFS NameNode")
    except Exception as exc:
        return _dump(
            {
                "error": f"HDFS error: {exc}",
                "hint": (
                    "ensure the path is snapshottable (hdfs dfs -allowSnapshot <path>) "
                    "and exists, and that WebHDFS is enabled (dfs.webhdfs.enabled=true)"
                ),
            }
        )


# ── Oozie tools ───────────────────────────────────────────────────────────────

@mcp.tool()
async def get_oozie_job(cluster_name: str, job_id: str) -> str:
    """
    Get details of an Oozie workflow or coordinator job.
    For workflows, returns all action statuses including the YARN app_id of
    each action, which can be passed to get_yarn_app() for deeper diagnosis.

    Args:
      cluster_name: Cluster name.
      job_id:       Oozie job ID (e.g. 0000001-240101120000000-oozie-oozi-W).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.oozie_url:
        return _dump(
            {
                "error": (
                    f"Oozie endpoint not found for cluster '{cluster_name}'. "
                    "Ensure the OOZIE service is running."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "oozie" in endpoints.spnego_required:
        return _spnego_error("Oozie")
    try:
        client = _pool.get_oozie_client(cluster_name)
        return _dump(await client.get_job(job_id))
    except OozieNotFoundError as exc:
        return _dump({"error": str(exc)})
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "oozie")
        return _spnego_error("Oozie")
    except Exception as exc:
        return _dump({"error": f"Oozie error: {exc}"})


@mcp.tool()
async def list_oozie_jobs(
    cluster_name: str,
    status: str | None = None,
    jobtype: str = "wf",
    user: str | None = None,
    limit: int = 20,
) -> str:
    """
    List recent Oozie jobs.

    Args:
      cluster_name: Cluster name.
      status:       Filter by status (e.g. RUNNING, FAILED, SUCCEEDED, KILLED).
      jobtype:      Job type: "wf" (workflow) or "coordinator" (default "wf").
      user:         Filter by submitting user.
      limit:        Maximum jobs to return (default 20).
    """
    endpoints = _pool.get_endpoints(cluster_name)
    if not endpoints.oozie_url:
        return _dump(
            {
                "error": (
                    f"Oozie endpoint not found for cluster '{cluster_name}'."
                )
            }
        )
    if not endpoints.kerberos and endpoints.disable_on_spnego and "oozie" in endpoints.spnego_required:
        return _spnego_error("Oozie")
    try:
        client = _pool.get_oozie_client(cluster_name)
        return _dump(
            await client.list_jobs(
                status=status, jobtype=jobtype, user=user, limit=limit
            )
        )
    except SpnegoRequiredError:
        _pool.mark_spnego_required(cluster_name, "oozie")
        return _spnego_error("Oozie")
    except Exception as exc:
        return _dump({"error": f"Oozie error: {exc}"})


# ── Entry point ───────────────────────────────────────────────────────────────

class _BearerAuthMiddleware:
    """ASGI middleware gating HTTP requests with a shared-secret bearer token.

    Rejects (401) any http/websocket request whose ``Authorization`` header is
    missing or doesn't match ``Authorization: Bearer <expected>``. Non-HTTP scopes
    (lifespan startup/shutdown) pass through. This is a lightweight shared-secret
    gate (NOT OAuth) — the secret is configured via ``MCP_AUTH_TOKEN``. A reverse
    proxy in front may set/forward the ``Authorization`` header.

    Constant-time comparison (``hmac.compare_digest``) so the gate doesn't leak
    how close a wrong token was.
    """

    def __init__(self, app: Any, expected_token: str) -> None:
        self._app = app
        self._expected = expected_token

    async def __call__(self, scope: dict, receive: Any, send: Any) -> None:
        if scope.get("type") not in ("http", "websocket"):
            # lifespan and other non-HTTP scopes pass through unconditionally.
            await self._app(scope, receive, send)
            return

        token: str | None = None
        for name, value in scope.get("headers", ()):
            if name.lower() == b"authorization":
                try:
                    header = value.decode("latin-1")
                except UnicodeDecodeError:
                    header = ""
                if header.lower().startswith("bearer "):
                    token = header[7:].strip()
                break

        if token is not None and hmac.compare_digest(token, self._expected):
            await self._app(scope, receive, send)
            return
        await _send_unauthorized(send)


async def _send_unauthorized(send: Any) -> None:
    """ASGI 401 response with a WWW-Authenticate: Bearer challenge."""
    body = json.dumps(
        {"error": "unauthorized", "hint": "Authorization: Bearer <MCP_AUTH_TOKEN> required"}
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
                (b"www-authenticate", b"Bearer"),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def _run_http_server() -> None:
    """Run the streamable-http (or sse) Starlette app under uvicorn, optionally
    wrapped in the bearer-token auth middleware when MCP_AUTH_TOKEN is set.

    We wrap ``mcp.streamable_http_app()`` ourselves (instead of
    ``mcp.run(transport="streamable-http")``) so we can layer the shared-secret
    middleware without pulling in FastMCP's OAuth machinery — which standard MCP
    clients would try to negotiate and which is the wrong fit for a static token.
    """
    import anyio

    async def _serve() -> None:
        import uvicorn

        app = mcp.streamable_http_app()
        auth_token = os.environ.get("MCP_AUTH_TOKEN") or None
        if auth_token:
            app = _BearerAuthMiddleware(app, auth_token)
            log.info("cdp_mcp.auth_enabled", scheme="bearer", header="Authorization")
        config = uvicorn.Config(
            app,
            host=_host,
            port=_port,
            log_level=server_cfg.log_level.lower(),
        )
        await uvicorn.Server(config).serve()

    anyio.run(_serve)


def run() -> None:
    """Entry point invoked by the cdp-mcp console script."""
    import structlog

    # MCP stdio transport uses stdout for JSON-RPC messages.
    # Logs MUST go to stderr to avoid corrupting the protocol.
    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(__import__("logging"), server_cfg.log_level.upper(), 20)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
    )
    # stdio (default): spawned by an MCP client over stdin/stdout. streamable-http
    # / sse: long-lived network daemon (e.g. under systemd); clients connect over
    # HTTP. Host/port were bound at FastMCP construction from MCP_HOST/MCP_PORT.
    log.info(
        "cdp_mcp.transport",
        transport=_transport,
        host=_host,
        port=_port,
    )
    if _transport == "streamable-http":
        # streamable-http goes through our runner so MCP_AUTH_TOKEN (if set)
        # gates the endpoint with the bearer-token middleware.
        _run_http_server()
    else:
        # stdio (default) / sse: FastMCP handles them directly (no HTTP auth).
        mcp.run(transport=_transport)


if __name__ == "__main__":
    run()
