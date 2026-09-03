"""
metrics_catalog.py — per-service-type default metric names for collect.py.

Two tiers, resolved against a specific cluster's live CM metric schema
(`ClouderaManagerClient.list_available_metrics`) rather than trusted blindly:

1. CURATED_SERVICE_METRICS — metric names already validated live against a
   real CDP cluster, carried over from cdp-report's export skill
   (cdp-report/.claude/skills/cdp-report-export/SKILL.md §3, which fetches
   these same names via get_service_metrics for hdfs/yarn/impala/
   hive_on_tez/hue/zookeeper/solr monthly reporting runs).
2. DISCOVERY_HINTS — service types with no curated entry yet, including
   kafka/nifi/hbase/phoenix: resolved purely from
   list_available_metrics(name_contains=HINT) at collection time, since no
   metric names for these have been confirmed against a live cluster here.
   Once a client site with one of these services has been run through the
   collector, promote the confirmed names from a run's manifest into
   CURATED_SERVICE_METRICS above instead of re-discovering every time.

This module is pure logic (no I/O, no client) so it stays unit-testable
without mocking httpx -- collect.py owns fetching the schema and calling
these functions with the results.
"""
from __future__ import annotations

CURATED_SERVICE_METRICS: dict[str, list[str]] = {
    "HDFS": [
        "total_dfs_capacity_used_across_datanodes",
        "total_capacity_remaining_across_namenodes",
        "total_bytes_read_rate_across_datanodes",
        "total_bytes_written_rate_across_datanodes",
    ],
    "YARN": [
        "total_max_capacity_vcores_across_yarn_pools",
        "total_allocated_vcores_across_yarn_pools",
        "total_available_vcores_across_yarn_pools",
        "total_allocated_memory_mb_across_yarn_pools",
        "total_available_memory_mb_across_yarn_pools",
        "apps_running_cumulative",
        "apps_failed_cumulative_rate",
        "apps_killed_cumulative_rate",
        "total_containers_running_across_nodemanagers",
        "pending_containers_cumulative",
        "total_containers_failed_rate_across_nodemanagers",
    ],
    "IMPALA": [
        "total_queries_ingested_rate_across_impala_pools",
        "queries_oom_rate_across_impala_pools",
        "queries_rejected_rate_across_impala_pools",
    ],
    "HIVE_ON_TEZ": [
        "total_hive_on_tez_waiting_compile_ops_across_hive_on_tez_hiveserver2s",
        "total_hive_on_tez_api_compile_avg_across_hive_on_tez_hiveserver2s",
    ],
    "HUE": [
        "hue_users_active",
        "hue_requests_active",
        "hue_requests_exceptions_rate",
        "hue_requests_response_time_rate",
    ],
    "ZOOKEEPER": ["canary_duration"],
    "SOLR": [
        "total_index_size_across_solr_replicas",
        "index_size_across_solr_replicas",
    ],
}

# service.type strings (as returned by ClouderaManagerClient.list_services)
# with no curated entry -- resolved purely via discovery. HINT is a
# case-insensitive substring passed to list_available_metrics(name_contains=),
# not a metric name itself.
DISCOVERY_HINTS: dict[str, str] = {
    "KAFKA": "kafka",
    "NIFI": "nifi",
    "NIFIREGISTRY": "nifi",
    "HBASE": "hbase",
    "PHOENIX": "phoenix",
}

# Validated live (cdp-report SKILL.md §3). NOTE: cpu_user_rate/cpu_system_rate
# return one HOST-level series *plus* a per-role breakdown series per role
# running on that host, under the same metric name -- consumers must filter
# to attributes.category == "HOST" for the host aggregate, or they'll
# silently pick up a near-zero per-role series instead. cpu_percent and
# physical_memory_used do not have this issue (single HOST series only).
DEFAULT_HOST_METRICS: list[str] = [
    "cpu_percent",
    "physical_memory_used",
    "cpu_user_rate",
    "cpu_system_rate",
]

# Safety cap on a pure-discovery metric list. CM's /timeseries has no
# server-side point cap (see cm_client.py get_*_metrics_raw), so an
# unrestrained list_available_metrics(name_contains=hint) result feeding
# straight into one tsquery SELECT list risks a slow/oversized response for
# a busy, metric-heavy service. Collection still succeeds past this cap --
# it just logs a warning and truncates. Hand-curate
# CURATED_SERVICE_METRICS for that service type if this bites in practice.
MAX_DISCOVERED_METRICS = 25


def has_curated(service_type: str) -> bool:
    return service_type.upper() in CURATED_SERVICE_METRICS


def discovery_hint(service_type: str) -> str | None:
    return DISCOVERY_HINTS.get(service_type.upper())


def resolve_curated(
    service_type: str, known_metric_names: set[str]
) -> tuple[list[str], list[str]]:
    """Cross-check this service type's curated metric names against the
    cluster's live schema. Returns (resolved, missing) -- a curated name
    that doesn't exist on this CM instance (older/newer version, service
    variant) is dropped rather than sent, since a bad name in a tsquery
    SELECT list doesn't error, it just silently returns no series for that
    name, which reads as "no data" instead of "unknown metric"."""
    curated = CURATED_SERVICE_METRICS.get(service_type.upper(), [])
    resolved = [name for name in curated if name in known_metric_names]
    missing = [name for name in curated if name not in known_metric_names]
    return resolved, missing


def cap_discovered(
    names: list[str], limit: int = MAX_DISCOVERED_METRICS
) -> tuple[list[str], bool]:
    """Returns (possibly-truncated names, was_truncated)."""
    if len(names) > limit:
        return names[:limit], True
    return names, False
