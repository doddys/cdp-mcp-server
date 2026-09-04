"""Unit tests for cdp_mcp.collector.metrics_catalog (pure logic, no I/O)."""
from cdp_mcp.collector import metrics_catalog


def test_has_curated_true_for_known_service():
    assert metrics_catalog.has_curated("hdfs")
    assert metrics_catalog.has_curated("HDFS")


def test_has_curated_true_for_kafka_hbase_phoenix():
    # Confirmed against a real CM v51/CDH 7.1.9 metric schema (see
    # metrics_catalog.py's module docstring) -- schema-verified, not yet
    # confirmed against a live-collecting instance of these services.
    assert metrics_catalog.has_curated("kafka")
    assert metrics_catalog.has_curated("hbase")
    assert metrics_catalog.has_curated("PHOENIX")


def test_has_curated_false_for_nifi():
    # NiFi has zero metric definitions in the CM instance checked -- no
    # NiFi CSD/parcel was registered there to contribute descriptors.
    # Discovery-only, since a different CM instance may have real ones.
    assert not metrics_catalog.has_curated("nifi")


def test_discovery_hint_covers_nifi_only():
    assert metrics_catalog.discovery_hint("NIFI") == "nifi"
    assert metrics_catalog.discovery_hint("nifiregistry") == "nifi"
    # kafka/hbase/phoenix are curated now, not discovery-hinted
    assert metrics_catalog.discovery_hint("kafka") is None
    assert metrics_catalog.discovery_hint("hbase") is None
    assert metrics_catalog.discovery_hint("phoenix") is None


def test_discovery_hint_none_for_unknown_type():
    assert metrics_catalog.discovery_hint("not_a_real_service") is None


def test_resolve_curated_drops_unknown_names():
    known = {"total_dfs_capacity_used_across_datanodes", "total_capacity_remaining_across_namenodes"}
    resolved, missing = metrics_catalog.resolve_curated("hdfs", known)
    assert resolved == [
        "total_dfs_capacity_used_across_datanodes",
        "total_capacity_remaining_across_namenodes",
    ]
    assert "total_bytes_read_rate_across_datanodes" in missing
    assert "total_bytes_written_rate_across_datanodes" in missing


def test_resolve_curated_empty_for_unknown_service_type():
    resolved, missing = metrics_catalog.resolve_curated("nifi", {"anything"})
    assert resolved == []
    assert missing == []


def test_resolve_curated_kafka_names_present():
    known = {
        "kafka_active_controller_across_kafka_brokers",
        "kafka_offline_partitions_across_kafka_brokers",
    }
    resolved, missing = metrics_catalog.resolve_curated("kafka", known)
    assert "kafka_active_controller_across_kafka_brokers" in resolved
    assert "kafka_offline_partitions_across_kafka_brokers" in resolved
    assert "kafka_under_replicated_partitions_across_kafka_brokers" in missing


def test_kafka_curated_list_includes_reviewed_additions():
    kafka_names = set(metrics_catalog.CURATED_SERVICE_METRICS["KAFKA"])
    added = {
        "kafka_network_processor_avg_idle_across_kafka_brokers",
        "kafka_groups_preparing_rebalance_across_kafka_brokers",
        "kafka_groups_completing_rebalance_across_kafka_brokers",
        "kafka_max_replication_lag_across_kafka_brokers",
        "kafka_offline_replica_count_across_kafka_brokers",
    }
    assert added <= kafka_names


def test_resolve_curated_hbase_and_phoenix_names_present():
    hbase_resolved, _ = metrics_catalog.resolve_curated(
        "hbase", {"compaction_queue_size_across_regionservers"}
    )
    assert "compaction_queue_size_across_regionservers" in hbase_resolved

    phoenix_resolved, _ = metrics_catalog.resolve_curated(
        "phoenix", {"health_bad_rate_across_phoenix_query_servers"}
    )
    assert "health_bad_rate_across_phoenix_query_servers" in phoenix_resolved


def test_cap_discovered_truncates_and_flags():
    names = [f"metric_{i}" for i in range(30)]
    capped, truncated = metrics_catalog.cap_discovered(names, limit=25)
    assert len(capped) == 25
    assert truncated is True


def test_cap_discovered_under_limit_unflagged():
    names = ["a", "b", "c"]
    capped, truncated = metrics_catalog.cap_discovered(names, limit=25)
    assert capped == names
    assert truncated is False
