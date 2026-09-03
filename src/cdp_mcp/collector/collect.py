"""
collect.py — standalone, offline data collector for one CDP cluster.

Entry point for the client-site deployment described in
scripts/build_collector_bundle.sh. Run inside the client's network, where a
cdp-mcp/Claude Code session cannot reach the cluster at all; its output
directory is what gets carried out afterwards (SFTP, encrypted USB, whatever
egress channel the client approves) for analysis on a separate,
internet-connected machine.

No LLM, no FastMCP/MCP transport: builds the same CMPool/registry server.py's
lifespan builds, then calls ClouderaManagerClient methods directly -- notably
get_service_metrics_raw/get_host_metrics_raw, which skip the
MAX_TIMESERIES_POINTS cap that exists specifically to keep MCP tool results
under the transport's ~1MB result cap (see cm_client.py). That cap doesn't
apply here: output goes straight to local files, so metrics collection is
full-resolution for the whole requested period in one call per entity.

Output file naming (NN_<name>.json, flat in --out) deliberately mirrors the
raw/ directory cdp-report's cdp-report-export skill already produces when run
interactively against a network-reachable cluster (confirmed against a real
run: outputs/astra_daas_prd_reports/deepseek/2026-08/raw/) -- same filenames,
same per-entity content shape, same weekly/severity chunking for the entities
that need it. That means cdp-report's Phase 1.5+ (curate/render) tooling can
run against this collector's --out directory with little to no adaptation,
instead of collector output needing its own bespoke downstream pipeline. Not
reproduced here: cdp-report's own bookkeeping files (_manifest.json,
_metric_coverage.json, _hostnames.json, _cluster_resolution.json) -- those
are cheap to derive from the NN_*.json files below on the analysis side, and
this collector keeps its own manifest.json (checksums + resumability, a
different job) instead of trying to imitate that format.

Also collects YARN/Spark/HDFS/Oozie data via the same downstream clients
server.py's tools use (cm_pool.py's get_{yarn,spark,hdfs,oozie}_client) --
Basic auth to CM itself is unaffected; these four attach SPNEGO when the
cluster's registry entry has kerberos=true. On a Kerberized cluster, `kinit`
(or a valid ticket already in the default credentials cache) before running
this -- see cm_instances.yaml.example's kerberos_keytab/kerberos_principal
for the unattended alternative. Unlike CM's /timeseries, these four REST
APIs are mostly NOT period-bounded: YARN's list_apps() does accept a time
range (client-side filtered per cm_client.py's own note about the RM
sometimes ignoring it server-side), but Spark's list_apps() and Oozie's
list_jobs() have no time filter at all -- they're "most recent N as of
collection time" snapshots, recorded as such in the manifest, not a period-
complete export.

get_alerts/get_audit_events/list_impala_queries all cap what a single call
can return (matched[:limit] for alerts/audit, a hard server-side limit for
impala queries) with NO further pagination available beyond that -- verified
against the real run above: a single week's IMPORTANT-severity alerts alone
matched ~10,000 events, far past any single call's practical limit. So, same
as cdp-report's proven approach: alerts are chunked by week AND severity
(CRITICAL, IMPORTANT), audit events and impala's >15min-query listing by
week. Each chunk still isn't guaranteed exhaustive on a very active
cluster/week -- see the collect.limit_truncated / collect.possible_floor log
warnings this module emits when a chunk's returned count suggests more data
existed than was returned.

Usage:
    cdp-collect --cluster my-cluster \\
        --period-start 2026-08-01T00:00:00Z --period-end 2026-09-01T00:00:00Z \\
        --out output/my-cluster_202608/

    cdp-collect --list-clusters   # discover cluster names known to the registry
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import structlog

from cdp_mcp.clients.errors import SpnegoRequiredError
from cdp_mcp.cm_client import ClouderaManagerClient
from cdp_mcp.cm_pool import CMPool
from cdp_mcp.collector import metrics_catalog
from cdp_mcp.collector.manifest import (
    MANIFEST_VERSION,
    FileRecord,
    Manifest,
    load_manifest,
    save_manifest,
    write_json,
)
from cdp_mcp.config import ServerSettings, build_registry

log = structlog.get_logger(__name__)

# Concurrent get_host_metrics_raw calls -- bounds load on CM's /timeseries
# endpoint on a large cluster; raise via --concurrency for a beefier CM, lower
# it if collection is timing out under load.
DEFAULT_CONCURRENCY = 4

# get_alerts/get_audit_events page internally up to max_scan and report
# `matched[:limit]` -- confirmed live against a real cluster that a single
# week of IMPORTANT-severity alerts alone can match ~10,000 events, so these
# are generous-but-still-bounded collector-context defaults (local disk, not
# an MCP result), not a guarantee of completeness. _write_entity logs
# collect.limit_truncated whenever total_matched_in_range exceeds what was
# actually written.
ALERTS_LIMIT = 2000
ALERTS_MAX_SCAN = 100000
ALERT_SEVERITIES = ("CRITICAL", "IMPORTANT")

# YARN/Spark/Oozie listing calls -- generous since output is local disk, not
# an MCP result. Actual coverage is still bounded by what each service
# retains (RM's completed-app cache, Spark HS's/Oozie's own history limits).
DOWNSTREAM_LIST_LIMIT = 5000

# list_impala_queries has a hard server-side limit with no total-count signal
# at all (unlike alerts/audit) -- cdp-report's proven convention: only
# long-running queries matter for a report, weekly chunks, and a chunk that
# returns exactly the limit is flagged as a floor, not paginated further (the
# CM API's own docs call offset paging on executing queries non-deterministic).
IMPALA_SLOW_QUERY_FILTER = "query_duration > 15m"
IMPALA_QUERY_LIMIT = 200

REPLICATION_SERVICE_TYPES = {"HDFS", "HIVE"}


def _cdp_mcp_version() -> str:
    try:
        return version("cdp-mcp")
    except PackageNotFoundError:
        return "unknown"


async def build_pool() -> CMPool:
    """Same start sequence as server.py's _lifespan, minus the MCP session
    ref-counting -- one process, one run, nothing to share across sessions."""
    settings = ServerSettings()
    registry = build_registry(settings)
    registry.start()
    try:
        instances = registry.get_all()
        pool = CMPool(instances, settings)
        await pool.start()
        return pool
    finally:
        registry.stop()


def _weekly_ranges(start: str, end: str, days: int = 7) -> list[tuple[str, str]]:
    """Split an ISO 8601 [start, end) period into <=`days`-day sub-ranges.
    Needed because get_alerts/get_audit_events/list_impala_queries all cap
    what a single call returns with no further pagination available --
    narrowing the range is the only way to get more complete coverage."""
    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    step = timedelta(days=days)
    ranges = []
    cur = start_dt
    while cur < end_dt:
        nxt = min(cur + step, end_dt)
        ranges.append((cur.isoformat().replace("+00:00", "Z"), nxt.isoformat().replace("+00:00", "Z")))
        cur = nxt
    return ranges


def _count_points(data: dict) -> int:
    return sum(
        len(ts.get("data", []))
        for item in data.get("items", [])
        for ts in item.get("timeSeries", [])
    )


def _count_items(data: Any) -> int:
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return len(data.get("items", []))
    return 0


async def _write_entity(
    rel: str,
    entity_type: str,
    entity_name: str,
    out_dir: Path,
    manifest: Manifest,
    fetch: Callable[[], Awaitable[Any]],
    *,
    count_fn: Callable[[Any], int] = _count_items,
    metric_names: list[str] | None = None,
    downstream_service: str | None = None,
    pool: CMPool | None = None,
    cluster: str | None = None,
    floor_limit: int | None = None,
) -> None:
    """Fetch one entity and record it in the manifest, or log-and-skip on
    failure -- a single service/host/downstream call failing must not abort
    an otherwise multi-hour unattended collection run (per CLAUDE.md rule 5:
    clean fallbacks, never a crash). downstream_service/pool/cluster are only
    needed to mark_spnego_required() on a SPNEGO challenge, mirroring
    server.py's tool-level short-circuit for later calls in the same run.
    floor_limit flags entities with a hard server-side cap and no
    total-count signal (list_impala_queries, YARN/Spark/Oozie listings) --
    a returned count >= floor_limit means "at least this many", not
    "exactly this many"."""
    if manifest.has(rel):
        log.info("collect.skip_resumed", path=rel)
        return
    try:
        data = await fetch()
    except SpnegoRequiredError:
        log.warning("collect.spnego_required", path=rel, service=downstream_service)
        if pool is not None and cluster is not None and downstream_service is not None:
            pool.mark_spnego_required(cluster, downstream_service)
        return
    except Exception as exc:
        log.warning("collect.entity_failed", path=rel, error=str(exc))
        return

    n = count_fn(data)
    if isinstance(data, dict) and "total_matched_in_range" in data:
        total = data["total_matched_in_range"]
        if n < total:
            log.warning(
                "collect.limit_truncated",
                path=rel,
                returned=n,
                total_matched_in_range=total,
                note="server-side matched[:limit] cut this short -- narrow the sub-range for fuller coverage",
            )
    elif floor_limit is not None and n >= floor_limit:
        log.warning(
            "collect.possible_floor",
            path=rel,
            returned=n,
            floor_limit=floor_limit,
            note="hit a hard server-side cap with no total-count signal -- this count may be a floor, not exhaustive",
        )
    if isinstance(data, dict) and data.get("truncated"):
        log.warning(
            "collect.range_truncated",
            path=rel,
            note="underlying tool reported truncated=true (scan budget exhausted before covering the full range)",
        )

    sha, size = write_json(out_dir / rel, data)
    manifest.add(
        FileRecord(
            path=rel,
            sha256=sha,
            bytes=size,
            entity_type=entity_type,
            entity_name=entity_name,
            count=n,
            metric_names=metric_names or [],
        )
    )
    save_manifest(out_dir / "manifest.json", manifest)
    log.info("collect.done", path=rel, count=n)


async def _fetch_this_cluster(client: ClouderaManagerClient, cluster: str) -> dict:
    clusters = await client.list_clusters()
    match = next((c for c in clusters if c.get("name") == cluster), None)
    if match is None:
        raise ValueError(f"cluster {cluster!r} not found in list_clusters()")
    return match


async def collect_bootstrap(
    client: ClouderaManagerClient, cluster: str, out_dir: Path, manifest: Manifest
) -> None:
    """01_*.json -- cluster-wide inventory/security snapshot, no time range."""
    await _write_entity(
        "01_cluster.json", "cluster_info", cluster, out_dir, manifest,
        lambda: _fetch_this_cluster(client, cluster), count_fn=lambda _d: 1,
    )
    await _write_entity(
        "01_security_info.json", "security_info", cluster, out_dir, manifest,
        lambda: client.get_cluster_security_info(cluster), count_fn=lambda _d: 1,
    )
    await _write_entity(
        "01_parcels.json", "parcels", cluster, out_dir, manifest,
        lambda: client.list_parcels(cluster),
    )
    await _write_entity(
        "01_host_status.json", "host_status", cluster, out_dir, manifest,
        lambda: client.get_host_status(cluster),
    )


async def collect_services_and_roles(
    client: ClouderaManagerClient,
    cluster: str,
    services: list[dict],
    out_dir: Path,
    manifest: Manifest,
) -> set[str]:
    """02_services.json + 02_roles_<service>.json per service. Also returns
    the host set (from each role's hostRef.hostname) that collect_host
    iterates -- 01_host_status.json's own roleRefs field is unpopulated on
    real CM instances (confirmed against a real run), so per-service role
    listings are the only source of the host<->role association, same as
    cdp-report's build_host_role_map.py already assumes."""
    await _write_entity(
        "02_services.json", "services", cluster, out_dir, manifest,
        lambda: client.list_services(cluster),
    )
    hosts: set[str] = set()
    for svc in services:
        name = svc["name"]
        roles = await client.list_roles(cluster, name)
        for role in roles:
            hostname = (role.get("hostRef") or {}).get("hostname")
            if hostname:
                hosts.add(hostname)
        rel = f"02_roles_{name}.json"
        if manifest.has(rel):
            log.info("collect.skip_resumed", path=rel)
            continue
        sha, size = write_json(out_dir / rel, roles)
        manifest.add(
            FileRecord(
                path=rel, sha256=sha, bytes=size, entity_type="roles",
                entity_name=name, count=len(roles),
            )
        )
        save_manifest(out_dir / "manifest.json", manifest)
        log.info("collect.done", path=rel, count=len(roles))
    return hosts


async def resolve_service_metric_names(
    client: ClouderaManagerClient, service_type: str
) -> list[str]:
    if metrics_catalog.has_curated(service_type):
        schema = await client.list_available_metrics()
        known = {item["name"] for item in schema if item.get("name")}
        resolved, missing = metrics_catalog.resolve_curated(service_type, known)
        if missing:
            log.warning(
                "collect.curated_metrics_missing", service_type=service_type, missing=missing
            )
        return resolved

    hint = metrics_catalog.discovery_hint(service_type)
    if not hint:
        log.warning("collect.no_catalog_entry", service_type=service_type)
        return []
    schema = await client.list_available_metrics(name_contains=hint)
    names = [item["name"] for item in schema if item.get("name")]
    names, truncated = metrics_catalog.cap_discovered(names)
    if truncated:
        log.warning("collect.discovery_truncated", service_type=service_type, kept=len(names))
    if not names:
        log.warning("collect.discovery_empty", service_type=service_type, hint=hint)
    return names


async def collect_service_metrics(
    client: ClouderaManagerClient,
    cluster: str,
    service: dict,
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
) -> None:
    name, svc_type = service["name"], service.get("type", "")
    metric_names = await resolve_service_metric_names(client, svc_type)
    if not metric_names:
        log.warning("collect.service_no_metrics", service=name, type=svc_type)
        return
    await _write_entity(
        f"03_service_metrics_{name}.json",
        "service_metrics",
        name,
        out_dir,
        manifest,
        lambda: client.get_service_metrics_raw(cluster, name, metric_names, start, end),
        count_fn=_count_points,
        metric_names=metric_names,
    )


async def collect_host_metrics(
    client: ClouderaManagerClient,
    hostname: str,
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
    sem: asyncio.Semaphore,
) -> None:
    async with sem:
        await _write_entity(
            f"03_host_metrics_{hostname}.json",
            "host_metrics",
            hostname,
            out_dir,
            manifest,
            lambda: client.get_host_metrics_raw(
                hostname, metrics_catalog.DEFAULT_HOST_METRICS, start, end
            ),
            count_fn=_count_points,
            metric_names=metrics_catalog.DEFAULT_HOST_METRICS,
        )


async def collect_cluster_utilization(
    client: ClouderaManagerClient, cluster: str, start: str, end: str, out_dir: Path, manifest: Manifest
) -> None:
    await _write_entity(
        "03_cluster_utilization.json", "cluster_utilization", cluster, out_dir, manifest,
        lambda: client.get_cluster_utilization(cluster, start_time=start, end_time=end),
        count_fn=lambda _d: 1,
    )


def _bound_audit_fetch(
    client: ClouderaManagerClient, cluster: str, start: str, end: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.get_audit_events(
        cluster, start_time=start, end_time=end, limit=ALERTS_LIMIT, max_scan=ALERTS_MAX_SCAN
    )


async def collect_audit(
    client: ClouderaManagerClient, cluster: str, start: str, end: str, out_dir: Path, manifest: Manifest
) -> None:
    for i, (wstart, wend) in enumerate(_weekly_ranges(start, end), 1):
        await _write_entity(
            f"04_audit_events_w{i}.json", "audit", cluster, out_dir, manifest,
            _bound_audit_fetch(client, cluster, wstart, wend),
        )


async def collect_cluster_commands(
    client: ClouderaManagerClient, cluster: str, out_dir: Path, manifest: Manifest
) -> None:
    await _write_entity(
        "05_cluster_commands.json", "cluster_commands", cluster, out_dir, manifest,
        lambda: client.list_cluster_commands(cluster, limit=200),
        floor_limit=200,
    )


def _bound_repl_schedules_fetch(
    client: ClouderaManagerClient, cluster: str, name: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.list_replication_schedules(cluster, name)


def _bound_repl_metrics_fetch(
    client: ClouderaManagerClient, cluster: str, name: str, start: str, end: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.get_replication_metrics(cluster, name, start_time=start, end_time=end)


async def collect_replication(
    client: ClouderaManagerClient,
    cluster: str,
    services: list[dict],
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
) -> None:
    """HDFS/Hive are the two natively supported CM replication service
    types -- other service types simply have no /replications resource, so
    this only attempts services whose type matches."""
    for svc in services:
        if svc.get("type") not in REPLICATION_SERVICE_TYPES:
            continue
        name = svc["name"]
        await _write_entity(
            f"05_repl_schedules_{name}.json", "repl_schedules", name, out_dir, manifest,
            _bound_repl_schedules_fetch(client, cluster, name),
        )
        await _write_entity(
            f"05_repl_metrics_{name}.json", "repl_metrics", name, out_dir, manifest,
            _bound_repl_metrics_fetch(client, cluster, name, start, end),
        )


def _bound_alerts_fetch(
    client: ClouderaManagerClient, cluster: str, severity: str, start: str, end: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.get_alerts(
        cluster, severity=severity, start_time=start, end_time=end,
        limit=ALERTS_LIMIT, max_scan=ALERTS_MAX_SCAN,
    )


async def collect_alerts(
    client: ClouderaManagerClient, cluster: str, start: str, end: str, out_dir: Path, manifest: Manifest
) -> None:
    for severity in ALERT_SEVERITIES:
        for i, (wstart, wend) in enumerate(_weekly_ranges(start, end), 1):
            await _write_entity(
                f"06_alerts_{severity}_w{i}.json", "alerts", cluster, out_dir, manifest,
                _bound_alerts_fetch(client, cluster, severity, wstart, wend),
            )


def _bound_impala_fetch(
    client: ClouderaManagerClient, cluster: str, svc_name: str, start: str, end: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.get_impala_queries(
        cluster, svc_name, filter_str=IMPALA_SLOW_QUERY_FILTER,
        start_time=start, end_time=end, limit=IMPALA_QUERY_LIMIT,
    )


async def collect_impala_queries(
    client: ClouderaManagerClient,
    cluster: str,
    services: list[dict],
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
) -> None:
    """Long-running (>15min) queries only, weekly chunks -- see module
    docstring. NOTE: requires the CM user (cm_instances.yaml `username`) to
    hold Cluster Administrator privileges (or the Impala "view all queries"
    grant); otherwise this returns an empty result with HTTP 200, not a
    permission error, which reads identically to "no slow queries this
    period" -- if every week comes back empty, verify the CM user's role
    before concluding the cluster is healthy."""
    impala_services = [s["name"] for s in services if s.get("type") == "IMPALA"]
    multi = len(impala_services) > 1
    for svc_name in impala_services:
        for i, (wstart, wend) in enumerate(_weekly_ranges(start, end), 1):
            rel = (
                f"07_impala_queries_{svc_name}_w{i}.json" if multi else f"07_impala_queries_w{i}.json"
            )
            await _write_entity(
                rel, "impala_queries", svc_name, out_dir, manifest,
                _bound_impala_fetch(client, cluster, svc_name, wstart, wend),
                floor_limit=IMPALA_QUERY_LIMIT,
            )


async def _yarn_apps_wrapped(yarn: Any, start: str, end: str) -> dict:
    """yarn_client.list_apps() returns a bare array with no effective_range/
    truncated wrapper of its own (verified against yarn_client.py) --
    constructed here so a consumer can tell what period this covers and
    whether the count looks like a floor, matching cdp-report's documented
    workaround (a from-scratch run that wrote the bare array lost the
    ability to verify range coverage entirely)."""
    apps = await yarn.list_apps(started_after=start, started_before=end, limit=DOWNSTREAM_LIST_LIMIT)
    return {
        "apps": apps,
        "effective_range": {"start": start, "end": end},
        "truncated": len(apps) >= DOWNSTREAM_LIST_LIMIT,
    }


async def collect_downstream(
    pool: CMPool,
    cluster: str,
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
) -> None:
    """YARN/Spark/HDFS/Oozie, via the same downstream clients server.py's
    tools use. Each is independently optional: an undiscovered endpoint
    (service not installed, or not yet auto-discovered) is logged and
    skipped, not an error -- same as server.py's per-tool endpoint checks."""
    endpoints = pool.get_endpoints(cluster)

    if endpoints.yarn_rm_url:
        yarn = pool.get_yarn_client(cluster)
        await _write_entity(
            "07_list_yarn_apps.json", "yarn_apps", cluster, out_dir, manifest,
            lambda: _yarn_apps_wrapped(yarn, start, end),
            count_fn=lambda d: len(d.get("apps", [])),
            downstream_service="yarn", pool=pool, cluster=cluster,
        )
        await _write_entity(
            "07_get_yarn_queue.json", "yarn_queue", cluster, out_dir, manifest,
            lambda: yarn.get_queue(), count_fn=lambda _d: 1,
            downstream_service="yarn", pool=pool, cluster=cluster,
        )
    else:
        log.info("collect.endpoint_not_discovered", service="yarn")

    if endpoints.spark_hs_url:
        spark = pool.get_spark_client(cluster)
        # No time filter on Spark HS's /applications -- "most recent N as of
        # collection time" snapshot, not period-bounded.
        await _write_entity(
            "07_list_spark_apps.json", "spark_apps_snapshot", cluster, out_dir, manifest,
            lambda: spark.list_apps(limit=DOWNSTREAM_LIST_LIMIT),
            downstream_service="spark", pool=pool, cluster=cluster,
            floor_limit=DOWNSTREAM_LIST_LIMIT,
        )
    else:
        log.info("collect.endpoint_not_discovered", service="spark")

    if endpoints.hdfs_nn_url:
        hdfs = pool.get_hdfs_client(cluster)
        await _write_entity(
            "07_get_namenode_status.json", "hdfs_namenode_status", cluster, out_dir, manifest,
            lambda: hdfs.get_namenode_status(), count_fn=lambda _d: 1,
            downstream_service="hdfs", pool=pool, cluster=cluster,
        )
    else:
        log.info("collect.endpoint_not_discovered", service="hdfs")

    if endpoints.oozie_url:
        oozie = pool.get_oozie_client(cluster)
        # No time filter on Oozie's /jobs either -- same snapshot caveat.
        # "wf" matches cdp-report's 07_list_oozie_jobs.json convention;
        # coordinator jobs are an addition (separate file, non-breaking).
        await _write_entity(
            "07_list_oozie_jobs.json", "oozie_jobs_snapshot", cluster, out_dir, manifest,
            lambda: oozie.list_jobs(jobtype="wf", limit=DOWNSTREAM_LIST_LIMIT),
            downstream_service="oozie", pool=pool, cluster=cluster,
            floor_limit=DOWNSTREAM_LIST_LIMIT,
        )
        await _write_entity(
            "07_list_oozie_jobs_coordinator.json", "oozie_jobs_snapshot", cluster, out_dir, manifest,
            lambda: oozie.list_jobs(jobtype="coordinator", limit=DOWNSTREAM_LIST_LIMIT),
            downstream_service="oozie", pool=pool, cluster=cluster,
            floor_limit=DOWNSTREAM_LIST_LIMIT,
        )
    else:
        log.info("collect.endpoint_not_discovered", service="oozie")


async def collect_cluster(
    pool: CMPool,
    cluster: str,
    start: str,
    end: str,
    out_dir: Path,
    concurrency: int,
    service_filter: set[str] | None,
    skip_downstream: bool = False,
) -> Manifest:
    client = pool.get_client_for_cluster(cluster)
    if client is None:
        raise SystemExit(
            f"Unknown cluster {cluster!r}. Known clusters: {pool.list_known_clusters()}"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(out_dir / "manifest.json") or Manifest(
        version=MANIFEST_VERSION,
        cluster=cluster,
        period_start=start,
        period_end=end,
        generated_at=datetime.now(UTC).isoformat(),
        cdp_mcp_version=_cdp_mcp_version(),
    )

    await collect_bootstrap(client, cluster, out_dir, manifest)

    all_services = await client.list_services(cluster)
    services = (
        [s for s in all_services if s["name"] in service_filter] if service_filter else all_services
    )

    hosts = await collect_services_and_roles(client, cluster, services, out_dir, manifest)

    for svc in services:
        await collect_service_metrics(client, cluster, svc, start, end, out_dir, manifest)
    await collect_cluster_utilization(client, cluster, start, end, out_dir, manifest)

    sem = asyncio.Semaphore(concurrency)
    await asyncio.gather(
        *(
            collect_host_metrics(client, hostname, start, end, out_dir, manifest, sem)
            for hostname in sorted(hosts)
        )
    )

    await collect_audit(client, cluster, start, end, out_dir, manifest)
    await collect_alerts(client, cluster, start, end, out_dir, manifest)
    await collect_cluster_commands(client, cluster, out_dir, manifest)
    await collect_replication(client, cluster, services, start, end, out_dir, manifest)
    await collect_impala_queries(client, cluster, services, start, end, out_dir, manifest)

    if not skip_downstream:
        await collect_downstream(pool, cluster, start, end, out_dir, manifest)

    manifest.generated_at = datetime.now(UTC).isoformat()
    save_manifest(out_dir / "manifest.json", manifest)
    return manifest


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cluster", help="Cluster name as known to the CM registry")
    p.add_argument("--period-start", help="ISO 8601 start time (e.g. 2026-08-01T00:00:00Z)")
    p.add_argument("--period-end", help="ISO 8601 end time (e.g. 2026-09-01T00:00:00Z)")
    p.add_argument("--out", type=Path, help="Output directory for the collected bundle")
    p.add_argument(
        "--services",
        help="Comma-separated service names to restrict collection to (default: all)",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Max concurrent get_host_metrics_raw calls (default: {DEFAULT_CONCURRENCY})",
    )
    p.add_argument(
        "--skip-downstream",
        action="store_true",
        help=(
            "Skip YARN/Spark/HDFS/Oozie collection (CM metrics/alerts/audit/roles "
            "only). Use this if those services aren't installed, or Kerberos "
            "credentials (kinit) aren't set up yet on this host."
        ),
    )
    p.add_argument(
        "--list-clusters",
        action="store_true",
        help="Print cluster names known to the registry and exit",
    )
    return p.parse_args(argv)


async def _async_main(argv: list[str]) -> int:
    args = _parse_args(argv)
    pool = await build_pool()
    try:
        if args.list_clusters:
            for name in pool.list_known_clusters():
                print(name)
            return 0

        missing = [
            flag
            for flag, val in (
                ("--cluster", args.cluster),
                ("--period-start", args.period_start),
                ("--period-end", args.period_end),
                ("--out", args.out),
            )
            if not val
        ]
        if missing:
            print(f"Missing required arguments: {', '.join(missing)}", file=sys.stderr)
            return 2

        service_filter = set(args.services.split(",")) if args.services else None
        manifest = await collect_cluster(
            pool,
            args.cluster,
            args.period_start,
            args.period_end,
            args.out,
            args.concurrency,
            service_filter,
            args.skip_downstream,
        )
        print(f"Collected {len(manifest.files)} files to {args.out}")
        return 0
    finally:
        await pool.stop()


def main() -> None:
    sys.exit(asyncio.run(_async_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
