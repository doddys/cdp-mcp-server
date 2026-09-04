"""
metrics_catalog.py — per-service-type default metric names for collect.py.

Two tiers, resolved against a specific cluster's live CM metric schema
(`ClouderaManagerClient.list_available_metrics`) rather than trusted blindly:

1. CURATED_SERVICE_METRICS — metric names confirmed to exist. Two grades:
   - hdfs/yarn/impala/hive_on_tez/hue/zookeeper/solr: validated live against
     a real CDP cluster's actual *data*, carried over from cdp-report's
     export skill (cdp-report/.claude/skills/cdp-report-export/SKILL.md §3,
     which fetches these same names via get_service_metrics for monthly
     reporting runs) -- these are known to return real, populated series.
   - kafka/hbase/phoenix: confirmed only against CM's *metric schema*
     (list_available_metrics against a CM v51/CDH 7.1.9 instance -- the
     schema is instance-wide, not deployment-dependent, so these names exist
     in the schema regardless of whether the service is actually installed
     on any managed cluster) -- NOT yet confirmed to return populated data
     on a live-collecting instance of these services, since none was
     available to test against. Note kafka/hbase's core per-role metrics
     (e.g. kafka's `kafka_offline_partitions_across_kafka_brokers`, hbase's
     `compaction_queue_size_across_regionservers`) do NOT contain the
     service name as a substring, the same way host metrics like
     `cpu_percent` don't say "host" -- name_contains="kafka"/"hbase" alone
     misses them; finding these required searching by JMX vocabulary
     (compaction/memstore/requests_/isr_shrinks/etc.) and filtering by
     `sources` containing the actual role type (KAFKA/REGIONSERVER/HBASE/
     MASTER), not by service-name substring. phoenix's CM-native monitoring
     is limited to generic Query Server process/health metrics (cpu/mem/fd/
     health/events) -- no query-level or throughput metrics were found in
     the schema at all, unlike Impala's list_impala_queries which has its
     own dedicated CM API. Once a real site with one of these services runs
     through the collector, re-confirm which of these actually return
     non-empty series and prune/extend accordingly.
2. DISCOVERY_HINTS — service types with no curated entry, resolved purely
   from list_available_metrics(name_contains=HINT) at collection time.
   NIFI genuinely has zero metric definitions in the CM instance checked
   above (0 matches for name_contains="nifi") -- CFM/NiFi is commonly run
   outside Cloudera Manager entirely, so this CM instance had no NiFi
   CSD/parcel registered to contribute metric descriptors. Stays
   discovery-only rather than curated-empty, since a *different* CM
   instance that does have NiFi installed may well have real definitions.

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
    "KAFKA": [
        # Health/availability -- exactly one active controller is expected
        # cluster-wide; 0 or >1 indicates a controller election problem.
        "kafka_active_controller_across_kafka_brokers",
        "kafka_offline_partitions_across_kafka_brokers",
        # Durability risk -- partitions running below their replication
        # factor / minimum in-sync replica count.
        "kafka_under_replicated_partitions_across_kafka_brokers",
        "kafka_under_min_isr_partition_count_across_kafka_brokers",
        # Stability trend -- frequent ISR churn or unclean leader elections
        # indicate broker instability or network issues, not just noise.
        "kafka_isr_shrinks_rate_across_kafka_brokers",
        "kafka_unclean_leader_elections_rate_across_kafka_brokers",
        # Throughput.
        "kafka_produce_requests_rate_across_kafka_brokers",
        "kafka_fetch_consumer_requests_rate_across_kafka_brokers",
        # Saturation -- request-handler thread pool idle percentage nearing
        # zero is the standard Kafka broker capacity-headroom signal.
        "kafka_request_handler_avg_idle_rate_across_kafka_brokers",
        # Capacity/inventory context for topic/partition growth trends.
        "kafka_global_topic_count_across_kafka_brokers",
        "kafka_global_partition_count_across_kafka_brokers",
        # Disk headroom -- a full log directory crashes the broker.
        "kafka_log_directory_disk_free_space_across_kafka_broker_log_directories",
    ],
    "HBASE": [
        # Throughput.
        "requests_rate_across_regionservers",
        "read_requests_rate_across_regionservers",
        "write_requests_rate_across_regionservers",
        # Saturation/health -- a growing compaction or flush queue is the
        # standard RegionServer write-pressure early-warning signal.
        "compaction_queue_size_across_regionservers",
        "flush_queue_size_across_regionservers",
        # Memory pressure -- memstore approaching its flush threshold.
        "memstore_size_across_regionservers",
        # Read performance -- block cache hit ratio.
        "total_block_cache_express_hit_ratio_across_regionservers",
        # Region-level health.
        "regions_with_errors_across_htables",
        "regions_healthy_across_htables",
    ],
    "PHOENIX": [
        # Phoenix Query Server exposes only generic process/health metrics
        # to CM -- no query-level or throughput metrics exist in the schema
        # (unlike Impala, which has its own dedicated list_impala_queries
        # API). This set mirrors HUE's generic request/health pattern.
        "health_bad_rate_across_phoenix_query_servers",
        "health_disabled_rate_across_phoenix_query_servers",
        "mem_rss_across_phoenix_query_servers",
        "events_critical_rate_across_phoenixs",
    ],
}

# service.type strings (as returned by ClouderaManagerClient.list_services)
# with no curated entry -- resolved purely via discovery. HINT is a
# case-insensitive substring passed to list_available_metrics(name_contains=),
# not a metric name itself.
DISCOVERY_HINTS: dict[str, str] = {
    "NIFI": "nifi",
    "NIFIREGISTRY": "nifi",
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
