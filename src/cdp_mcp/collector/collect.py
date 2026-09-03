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
apply here: output goes straight to local files.

Output file naming (NN_<name>.json, flat in --out) and _manifest.json's
shape both mirror the raw/ directory cdp-report's cdp-report-export skill
produces interactively -- confirmed against a real run and against
cdp-report-curate/render/score_export_run.py's actual field expectations,
not just filename convention. That means cdp-report's Phase 1.5+ tooling can
run against this collector's --out directory directly.

Also collects YARN/Spark/HDFS/Oozie data via the same downstream clients
server.py's tools use (cm_pool.py's get_{yarn,spark,hdfs,oozie}_client) --
Basic auth to CM itself is unaffected; these four attach SPNEGO when the
cluster's registry entry has kerberos=true. On a Kerberized cluster, `kinit`
(or a valid ticket already in the default credentials cache) before running
this -- see cm_instances.yaml.example's kerberos_keytab/kerberos_principal
for the unattended alternative.

Host metrics are fetched in <=14-day sub-ranges and merged into one file per
host, rather than one full-period call -- confirmed live: a single
full-month call comes back at CM's SIX_HOURLY rollup (124 points for 31
days, exactly 6h spacing), while cdp-report's proven convention of <=14-day
chunks keeps each chunk's own rollup selection at HOURLY. Service metrics
are NOT chunked the same way -- cdp-report's own tooling documents that a
finer range does not yield finer service-level rollup once the period has
aged, so one full-period call is both correct and sufficient there.

get_alerts/get_audit_events/list_impala_queries/list_yarn_apps(long) all cap
what a single call can return with no further pagination available --
confirmed live: a single week's IMPORTANT-severity alerts alone matched
~10,000 events. Alerts/audit are chunked by week (alerts additionally by
severity); impala's >15min-query listing and YARN's >15min-app listing are
chunked by week AND merged into one combined file (deduped by queryId /
app_id respectively), since cdp-report-curate reads the combined file, not
the per-week chunks, to compute its downstream summaries.

Usage:
    cdp-collect --cluster my-cluster \\
        --period-start 2026-08-01T00:00:00Z --period-end 2026-09-01T00:00:00Z \\
        --out output/my-cluster_202608/

    cdp-collect --list-clusters   # discover cluster names known to the registry
"""
from __future__ import annotations

import argparse
import asyncio
import json
import re
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
    MANIFEST_FILENAME,
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

# YARN/Spark/Oozie snapshot listing calls -- generous since output is local
# disk, not an MCP result. Actual coverage is still bounded by what each
# service retains (RM's completed-app cache, Spark HS's/Oozie's own history).
DOWNSTREAM_LIST_LIMIT = 5000

# list_impala_queries has a hard server-side limit with no total-count signal
# at all (unlike alerts/audit) -- cdp-report's proven convention: only
# long-running queries matter for a report, weekly chunks, and a chunk that
# returns exactly the limit is flagged as a floor, not paginated further (the
# CM API's own docs call offset paging on executing queries non-deterministic).
IMPALA_SLOW_QUERY_FILTER = "query_duration > 15m"
IMPALA_QUERY_LIMIT = 200

# Same floor/no-pagination situation for YARN's own long-running-app listing
# (list_yarn_apps with min_duration_secs) -- same weekly-chunk-then-merge
# treatment as impala queries.
YARN_LONG_APP_MIN_DURATION_SECS = 900
YARN_LONG_APP_LIMIT = 200

REPLICATION_SERVICE_TYPES = {"HDFS", "HIVE"}

# get_host_metrics_raw has no point cap of its own (see cm_client.py), but a
# full-period call still gets coarsened by CM's own server-side rollup
# selection -- confirmed live: a 31-day single call came back at SIX_HOURLY.
# <=14-day chunks (each gets its own rollup decision) keep it at HOURLY;
# cdp-report's own tooling uses the same chunk size for the same reason.
# Service metrics deliberately do NOT get this treatment -- see module
# docstring.
HOST_METRICS_CHUNK_DAYS = 14


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
    """Split an ISO 8601 [start, end) period into <=`days`-day sub-ranges."""
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


def _default_period_label(start: str) -> str:
    dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
    return dt.strftime("%B %Y")


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug or "cluster"


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


async def _as_coro(value: Any) -> Any:
    return value


def _const_fetch(value: Any) -> Callable[[], Awaitable[Any]]:
    return lambda: _as_coro(value)


def _write_not_available(
    rel: str, tool: str, out_dir: Path, manifest: Manifest, reason: str
) -> None:
    data = {"status": "not_available", "reason": reason}
    sha, size = write_json(out_dir / rel, data)
    manifest.add(
        FileRecord(
            file=rel, tool=tool, sha256=sha, bytes=size, item_count=0, status="not_available"
        )
    )
    save_manifest(out_dir / MANIFEST_FILENAME, manifest)
    log.info("collect.not_available", path=rel, reason=reason)


async def _write_entity(
    rel: str,
    tool: str,
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
) -> Any | None:
    """Fetch (or, on resume, read back) one entity, record it in
    _manifest.json, and return its data -- callers that need to merge
    several entities (impala/yarn-long weekly chunks, host-metrics
    sub-ranges, _hostnames.json) get a value back whether this run actually
    fetched it or a prior run already did. Returns None on failure/skip
    (SPNEGO, transient error) -- a single entity failing must not abort an
    otherwise multi-hour unattended run (CLAUDE.md rule 5: clean fallbacks,
    never a crash). downstream_service/pool/cluster are only needed to
    mark_spnego_required() on a SPNEGO challenge, mirroring server.py's
    tool-level short-circuit for later calls in the same run. floor_limit
    flags entities with a hard server-side cap and no total-count signal
    (list_impala_queries, YARN long-app/Spark/Oozie listings) -- a returned
    count >= floor_limit means "at least this many, not exhaustive", which
    also gets embedded into the written JSON's own `truncated` field so a
    later merge step (see collect_impala_queries/collect_yarn_long_apps)
    can combine it across chunks without re-deriving the heuristic.

    A failed call still writes a file -- {"status": "not_available",
    "reason": ...} -- rather than nothing at all, so cdp-report-curate's
    Prerequisites check (which needs "attempted and failed" distinguishable
    from "never collected", and both distinguishable from a genuine
    zero-result success) always finds the file it expects. A record with
    status="not_available" is NOT treated as done on resume -- it's retried,
    since the underlying cause (a permission grant, a kinit, a transient
    network blip) may have been fixed since the failed attempt."""
    path = out_dir / rel
    existing = manifest.get(rel)
    if existing is not None and existing.status != "not_available":
        log.info("collect.skip_resumed", path=rel)
        try:
            return json.loads(path.read_text())
        except Exception:
            log.warning("collect.resume_read_failed", path=rel)
            return None
    if existing is not None:
        log.info("collect.retry_previously_failed", path=rel, reason="status was not_available")
    try:
        data = await fetch()
    except SpnegoRequiredError:
        reason = f"SPNEGO/Kerberos auth required for {downstream_service or 'this'} service"
        log.warning("collect.spnego_required", path=rel, service=downstream_service)
        if pool is not None and cluster is not None and downstream_service is not None:
            pool.mark_spnego_required(cluster, downstream_service)
        _write_not_available(rel, tool, out_dir, manifest, reason)
        return None
    except Exception as exc:
        log.warning("collect.entity_failed", path=rel, error=str(exc))
        _write_not_available(rel, tool, out_dir, manifest, str(exc))
        return None

    n = count_fn(data)
    total_matched = data.get("total_matched_in_range") if isinstance(data, dict) else None
    truncated = data.get("truncated") if isinstance(data, dict) else None
    if (
        floor_limit is not None
        and isinstance(data, dict)
        and "items" in data
        and total_matched is None
    ):
        truncated = n >= floor_limit
        data["truncated"] = truncated

    if total_matched is not None and n < total_matched:
        log.warning(
            "collect.limit_truncated",
            path=rel,
            returned=n,
            total_matched_in_range=total_matched,
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
    if truncated:
        log.warning(
            "collect.range_truncated",
            path=rel,
            note="underlying tool reported truncated=true",
        )

    sha, size = write_json(path, data)
    manifest.add(
        FileRecord(
            file=rel,
            tool=tool,
            sha256=sha,
            bytes=size,
            item_count=n,
            truncated=truncated,
            total_matched_in_range=total_matched,
            metric_names=metric_names or [],
        )
    )
    save_manifest(out_dir / MANIFEST_FILENAME, manifest)
    log.info("collect.done", path=rel, count=n)
    return data


async def _fetch_this_cluster(client: ClouderaManagerClient, cluster: str) -> dict:
    clusters = await client.list_clusters()
    match = next((c for c in clusters if c.get("name") == cluster), None)
    if match is None:
        raise ValueError(f"cluster {cluster!r} not found in list_clusters()")
    return match


async def collect_bootstrap(
    client: ClouderaManagerClient, cluster: str, out_dir: Path, manifest: Manifest
) -> None:
    """01_*.json (inventory/security snapshot, no time range) plus the
    derived _hostnames.json render_charts.py's per-host appendix needs."""
    await _write_entity(
        "01_cluster.json", "list_clusters", out_dir, manifest,
        lambda: _fetch_this_cluster(client, cluster), count_fn=lambda _d: 1,
    )
    await _write_entity(
        "01_security_info.json", "get_cluster_security_info", out_dir, manifest,
        lambda: client.get_cluster_security_info(cluster), count_fn=lambda _d: 1,
    )
    await _write_entity(
        "01_parcels.json", "list_parcels", out_dir, manifest,
        lambda: client.list_parcels(cluster),
    )
    host_status = await _write_entity(
        "01_host_status.json", "get_host_status", out_dir, manifest,
        lambda: client.get_host_status(cluster),
    )
    if host_status:
        hostnames = {
            "hosts": [
                {
                    "hostname": h.get("hostname"),
                    "numCores": h.get("numCores"),
                    "totalPhysMemBytes": h.get("totalPhysMemBytes"),
                }
                for h in host_status
            ]
        }
        await _write_entity(
            "_hostnames.json", "get_host_status", out_dir, manifest,
            _const_fetch(hostnames), count_fn=lambda d: len(d.get("hosts", [])),
        )
    else:
        log.warning("collect.hostnames_skipped", reason="01_host_status.json unavailable")


def _bound_list_roles_fetch(
    client: ClouderaManagerClient, cluster: str, name: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.list_roles(cluster, name)


async def collect_services_and_roles(
    client: ClouderaManagerClient,
    cluster: str,
    all_services: list[dict],
    services: list[dict],
    out_dir: Path,
    manifest: Manifest,
) -> set[str]:
    """02_services.json (full, unfiltered inventory) + 02_roles_<service>.json
    per service actually being collected (respects --services). Returns the
    host set (from each role's hostRef.hostname) collect_host_metrics
    iterates -- 01_host_status.json's own roleRefs field is unpopulated on
    real CM instances (confirmed against a real run), so per-service role
    listings are the only source of the host<->role association, same as
    cdp-report's build_host_role_map.py already assumes."""
    await _write_entity(
        "02_services.json", "list_services", out_dir, manifest,
        _const_fetch(all_services),
    )
    hosts: set[str] = set()
    for svc in services:
        name = svc["name"]
        roles = await _write_entity(
            f"02_roles_{name}.json", "list_roles", out_dir, manifest,
            _bound_list_roles_fetch(client, cluster, name),
        )
        for role in roles or []:
            hostname = (role.get("hostRef") or {}).get("hostname")
            if hostname:
                hosts.add(hostname)
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
    """One full-period call -- see module docstring for why service metrics
    are NOT chunked the way host metrics are."""
    name, svc_type = service["name"], service.get("type", "")
    metric_names = await resolve_service_metric_names(client, svc_type)
    if not metric_names:
        log.warning("collect.service_no_metrics", service=name, type=svc_type)
        return
    await _write_entity(
        f"03_service_metrics_{name}.json", "get_service_metrics", out_dir, manifest,
        lambda: client.get_service_metrics_raw(cluster, name, metric_names, start, end),
        count_fn=_count_points, metric_names=metric_names,
    )


def _merge_timeseries_chunks(chunks: list[dict]) -> dict:
    """Combine multiple get_host_metrics_raw chunk responses (each fetched
    over a <=HOST_METRICS_CHUNK_DAYS sub-range) into one response shaped like
    a single full-period call would have produced, had CM's rollup selection
    not coarsened it. Series are matched across chunks by
    (metricName, entityName) -- CM's own key for "the same series": a
    HOST-category series shares metricName+hostname, and a ROLE-category
    series's entityName already includes the role, so no separate category
    key is needed (confirmed against real host-metrics output)."""
    series_by_key: dict[tuple, dict] = {}
    order: list[tuple] = []
    warnings: list = []
    for chunk in chunks:
        for item in chunk.get("items", []):
            warnings.extend(item.get("warnings") or [])
            for ts in item.get("timeSeries", []):
                md = ts.get("metadata") or {}
                key = (md.get("metricName"), md.get("entityName"))
                if key not in series_by_key:
                    # metadata (including rollupUsed) is taken from the
                    # FIRST chunk this key appears in, not recomputed across
                    # chunks -- confirmed live that later chunks can carry a
                    # finer rollup than earlier ones (CM hadn't aged the most
                    # recent chunk into as coarse a rollup yet at collection
                    # time), so a merged series' own metadata.rollupUsed can
                    # under-state its actual resolution. Never over-states it
                    # in the misleading direction, so left as a known
                    # imprecision rather than tracked per-chunk.
                    series_by_key[key] = {**ts, "data": list(ts.get("data", []))}
                    order.append(key)
                else:
                    series_by_key[key]["data"].extend(ts.get("data", []))
    for ts in series_by_key.values():
        ts["data"].sort(key=lambda p: p.get("timestamp", ""))
    merged_series = [series_by_key[k] for k in order]
    items = [{"timeSeries": merged_series, "warnings": warnings}] if merged_series else []
    start = chunks[0]["effective_range"]["start"] if chunks and chunks[0].get("effective_range") else None
    end = chunks[-1]["effective_range"]["end"] if chunks and chunks[-1].get("effective_range") else None
    return {"items": items, "effective_range": {"start": start, "end": end}}


async def collect_host_metrics(
    client: ClouderaManagerClient,
    hostname: str,
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
    sem: asyncio.Semaphore,
) -> None:
    async def _fetch_merged() -> dict:
        chunks = []
        for c_start, c_end in _weekly_ranges(start, end, days=HOST_METRICS_CHUNK_DAYS):
            chunks.append(
                await client.get_host_metrics_raw(
                    hostname, metrics_catalog.DEFAULT_HOST_METRICS, c_start, c_end
                )
            )
        return _merge_timeseries_chunks(chunks)

    async with sem:
        await _write_entity(
            f"03_host_metrics_{hostname}.json", "get_host_metrics", out_dir, manifest,
            _fetch_merged, count_fn=_count_points,
            metric_names=metrics_catalog.DEFAULT_HOST_METRICS,
        )


async def collect_cluster_utilization(
    client: ClouderaManagerClient, cluster: str, start: str, end: str, out_dir: Path, manifest: Manifest
) -> None:
    await _write_entity(
        "03_cluster_utilization.json", "get_cluster_utilization", out_dir, manifest,
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
            f"04_audit_events_w{i}.json", "get_audit_events", out_dir, manifest,
            _bound_audit_fetch(client, cluster, wstart, wend),
        )


async def collect_cluster_commands(
    client: ClouderaManagerClient, cluster: str, out_dir: Path, manifest: Manifest
) -> None:
    await _write_entity(
        "05_cluster_commands.json", "list_cluster_commands", out_dir, manifest,
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
            f"05_repl_schedules_{name}.json", "list_replication_schedules", out_dir, manifest,
            _bound_repl_schedules_fetch(client, cluster, name),
        )
        await _write_entity(
            f"05_repl_metrics_{name}.json", "get_replication_metrics", out_dir, manifest,
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
                f"06_alerts_{severity}_w{i}.json", "get_alerts", out_dir, manifest,
                _bound_alerts_fetch(client, cluster, severity, wstart, wend),
            )


def _bound_impala_fetch(
    client: ClouderaManagerClient, cluster: str, svc_name: str, start: str, end: str
) -> Callable[[], Awaitable[Any]]:
    return lambda: client.get_impala_queries(
        cluster, svc_name, filter_str=IMPALA_SLOW_QUERY_FILTER,
        start_time=start, end_time=end, limit=IMPALA_QUERY_LIMIT,
    )


def _merge_impala_weeks(weekly: list[dict], start: str, end: str) -> dict:
    seen: dict[str, dict] = {}
    order: list[str] = []
    any_truncated = False
    for wk in weekly:
        if wk.get("truncated"):
            any_truncated = True
        for q in wk.get("items", []):
            qid = q.get("queryId")
            if qid is not None and qid not in seen:
                seen[qid] = q
                order.append(qid)
    return {
        "items": [seen[k] for k in order],
        "effective_range": {"start": start, "end": end},
        "truncated": any_truncated,
    }


async def collect_impala_queries(
    client: ClouderaManagerClient,
    cluster: str,
    services: list[dict],
    start: str,
    end: str,
    out_dir: Path,
    manifest: Manifest,
) -> None:
    """Long-running (>15min) queries only, weekly chunks merged into one
    combined file (deduped by queryId) -- cdp-report-curate reads the
    combined file, not the per-week chunks, to compute its by-user/by-detail
    downstream summary. NOTE: requires the CM user (cm_instances.yaml
    `username`) to hold Cluster Administrator privileges (or the Impala
    "view all queries" grant); otherwise this returns an empty result with
    HTTP 200, not a permission error, which reads identically to "no slow
    queries this period" -- if every week comes back empty, verify the CM
    user's role before concluding the cluster is healthy."""
    impala_services = [s["name"] for s in services if s.get("type") == "IMPALA"]
    multi = len(impala_services) > 1
    for svc_name in impala_services:
        weekly_results = []
        for i, (wstart, wend) in enumerate(_weekly_ranges(start, end), 1):
            rel = (
                f"07_impala_queries_{svc_name}_w{i}.json" if multi else f"07_impala_queries_w{i}.json"
            )
            data = await _write_entity(
                rel, "list_impala_queries", out_dir, manifest,
                _bound_impala_fetch(client, cluster, svc_name, wstart, wend),
                floor_limit=IMPALA_QUERY_LIMIT,
            )
            if data:
                weekly_results.append(data)
        combined_rel = f"07_impala_queries_{svc_name}.json" if multi else "07_impala_queries.json"
        await _write_entity(
            combined_rel, "list_impala_queries", out_dir, manifest,
            _const_fetch(_merge_impala_weeks(weekly_results, start, end)),
        )


def _bound_yarn_long_apps_fetch(yarn: Any, start: str, end: str) -> Callable[[], Awaitable[Any]]:
    return lambda: _yarn_long_apps_wrapped(yarn, start, end)


async def _yarn_long_apps_wrapped(yarn: Any, start: str, end: str) -> dict:
    apps = await yarn.list_apps(
        started_after=start, started_before=end,
        min_duration_secs=YARN_LONG_APP_MIN_DURATION_SECS, limit=YARN_LONG_APP_LIMIT,
    )
    return {
        "apps": apps,
        "effective_range": {"start": start, "end": end},
        "truncated": len(apps) >= YARN_LONG_APP_LIMIT,
    }


def _merge_yarn_long_apps(weekly: list[dict], start: str, end: str) -> dict:
    seen: dict[str, dict] = {}
    order: list[str] = []
    any_truncated = False
    for wk in weekly:
        if wk.get("truncated"):
            any_truncated = True
        for app in wk.get("apps", []):
            app_id = app.get("app_id")
            if app_id is not None and app_id not in seen:
                seen[app_id] = app
                order.append(app_id)
    return {
        "apps": [seen[k] for k in order],
        "effective_range": {"start": start, "end": end},
        "truncated": any_truncated,
    }


async def collect_yarn_long_apps(
    pool: CMPool, cluster: str, start: str, end: str, out_dir: Path, manifest: Manifest
) -> None:
    """Apps running >=15min, weekly chunks merged into one combined file
    (deduped by app_id) -- same floor/no-pagination situation and the same
    weekly-chunk-then-merge treatment as collect_impala_queries."""
    endpoints = pool.get_endpoints(cluster)
    if not endpoints.yarn_rm_url:
        log.info("collect.endpoint_not_discovered", service="yarn_long_apps")
        return
    yarn = pool.get_yarn_client(cluster)
    weekly_results = []
    for i, (wstart, wend) in enumerate(_weekly_ranges(start, end), 1):
        data = await _write_entity(
            f"07_yarn_long_apps_w{i}.json", "list_yarn_apps", out_dir, manifest,
            _bound_yarn_long_apps_fetch(yarn, wstart, wend),
            count_fn=lambda d: len(d.get("apps", [])),
            downstream_service="yarn", pool=pool, cluster=cluster,
        )
        if data:
            weekly_results.append(data)
    await _write_entity(
        "07_yarn_long_apps.json", "list_yarn_apps", out_dir, manifest,
        _const_fetch(_merge_yarn_long_apps(weekly_results, start, end)),
        count_fn=lambda d: len(d.get("apps", [])),
    )


async def _yarn_apps_snapshot_wrapped(yarn: Any, start: str, end: str) -> dict:
    """yarn_client.list_apps() returns a bare array with no effective_range/
    truncated wrapper of its own (verified against yarn_client.py) --
    constructed here so a consumer can tell what period this covers and
    whether the count looks like a floor."""
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
            "07_list_yarn_apps.json", "list_yarn_apps", out_dir, manifest,
            lambda: _yarn_apps_snapshot_wrapped(yarn, start, end),
            count_fn=lambda d: len(d.get("apps", [])),
            downstream_service="yarn", pool=pool, cluster=cluster,
        )
        await _write_entity(
            "07_get_yarn_queue.json", "get_yarn_queue", out_dir, manifest,
            lambda: yarn.get_queue(), count_fn=lambda _d: 1,
            downstream_service="yarn", pool=pool, cluster=cluster,
        )
        await collect_yarn_long_apps(pool, cluster, start, end, out_dir, manifest)
    else:
        log.info("collect.endpoint_not_discovered", service="yarn")

    if endpoints.spark_hs_url:
        spark = pool.get_spark_client(cluster)
        # No time filter on Spark HS's /applications -- "most recent N as of
        # collection time" snapshot, not period-bounded.
        await _write_entity(
            "07_list_spark_apps.json", "list_spark_apps", out_dir, manifest,
            lambda: spark.list_apps(limit=DOWNSTREAM_LIST_LIMIT),
            downstream_service="spark", pool=pool, cluster=cluster,
            floor_limit=DOWNSTREAM_LIST_LIMIT,
        )
    else:
        log.info("collect.endpoint_not_discovered", service="spark")

    if endpoints.hdfs_nn_url:
        hdfs = pool.get_hdfs_client(cluster)
        await _write_entity(
            "07_get_namenode_status.json", "get_namenode_status", out_dir, manifest,
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
            "07_list_oozie_jobs.json", "list_oozie_jobs", out_dir, manifest,
            lambda: oozie.list_jobs(jobtype="wf", limit=DOWNSTREAM_LIST_LIMIT),
            downstream_service="oozie", pool=pool, cluster=cluster,
            floor_limit=DOWNSTREAM_LIST_LIMIT,
        )
        await _write_entity(
            "07_list_oozie_jobs_coordinator.json", "list_oozie_jobs", out_dir, manifest,
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
    period_label: str | None = None,
    cluster_hint: str | None = None,
) -> Manifest:
    client = pool.get_client_for_cluster(cluster)
    if client is None:
        raise SystemExit(
            f"Unknown cluster {cluster!r}. Known clusters: {pool.list_known_clusters()}"
        )

    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = load_manifest(out_dir / MANIFEST_FILENAME)
    if manifest is not None and (
        manifest.period.get("start") != start or manifest.period.get("end") != end
    ):
        # Resuming reuses whatever period the existing _manifest.json already
        # has -- silently, since load_manifest() has no way to know this run
        # asked for something different. Loading old files under a new
        # period's label would be worse than refusing: every entity in this
        # directory would look like it covers [start, end) when it actually
        # covers the manifest's original range.
        raise SystemExit(
            f"{out_dir} already has _manifest.json for period "
            f"{manifest.period.get('start')} -> {manifest.period.get('end')}, "
            f"but this run requested {start} -> {end}. Use a different --out "
            "directory for a different period, or delete the existing one to start over."
        )
    if manifest is None:
        manifest = Manifest(
            period={"label": period_label or _default_period_label(start), "start": start, "end": end},
            cluster={"hint": cluster_hint or _slugify(cluster), "resolved_name": cluster},
            cdp_mcp_version=_cdp_mcp_version(),
            generated_at=datetime.now(UTC).isoformat(),
        )

    await collect_bootstrap(client, cluster, out_dir, manifest)

    all_services = await client.list_services(cluster)
    services = (
        [s for s in all_services if s["name"] in service_filter] if service_filter else all_services
    )

    hosts = await collect_services_and_roles(client, cluster, all_services, services, out_dir, manifest)

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
    save_manifest(out_dir / MANIFEST_FILENAME, manifest)
    return manifest


def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cluster", help="Cluster name as known to the CM registry")
    p.add_argument("--period-start", help="ISO 8601 start time (e.g. 2026-08-01T00:00:00Z)")
    p.add_argument("--period-end", help="ISO 8601 end time (e.g. 2026-09-01T00:00:00Z)")
    p.add_argument("--out", type=Path, help="Output directory for the collected bundle")
    p.add_argument(
        "--period-label",
        help='Human-readable period label for _manifest.json, e.g. "August 2026" '
        "(default: derived from --period-start)",
    )
    p.add_argument(
        "--cluster-hint",
        help="Short slug for _manifest.json, matching cdp-report's CLUSTER_NAME_HINT "
        'convention (e.g. "astra_daas_drc"). Default: derived from --cluster.',
    )
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
            args.period_label,
            args.cluster_hint,
        )
        print(f"Collected {len(manifest.files)} files to {args.out}")
        return 0
    finally:
        await pool.stop()


def main() -> None:
    sys.exit(asyncio.run(_async_main(sys.argv[1:])))


if __name__ == "__main__":
    main()
