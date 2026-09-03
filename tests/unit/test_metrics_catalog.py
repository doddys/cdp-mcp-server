"""Unit tests for cdp_mcp.collector.metrics_catalog (pure logic, no I/O)."""
from cdp_mcp.collector import metrics_catalog


def test_has_curated_true_for_known_service():
    assert metrics_catalog.has_curated("hdfs")
    assert metrics_catalog.has_curated("HDFS")


def test_has_curated_false_for_kafka():
    # Kafka has no live-validated metric names yet -- discovery-only.
    assert not metrics_catalog.has_curated("kafka")


def test_discovery_hint_covers_new_services():
    assert metrics_catalog.discovery_hint("kafka") == "kafka"
    assert metrics_catalog.discovery_hint("NIFI") == "nifi"
    assert metrics_catalog.discovery_hint("hbase") == "hbase"
    assert metrics_catalog.discovery_hint("phoenix") == "phoenix"


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
    resolved, missing = metrics_catalog.resolve_curated("kafka", {"anything"})
    assert resolved == []
    assert missing == []


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
