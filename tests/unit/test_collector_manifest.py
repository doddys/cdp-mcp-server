"""Unit tests for cdp_mcp.collector.manifest."""
import json

from cdp_mcp.collector.manifest import (
    FileRecord,
    Manifest,
    load_manifest,
    save_manifest,
    write_json,
)


def test_write_json_hashes_actual_bytes_on_disk(tmp_path):
    path = tmp_path / "services" / "hdfs.json"
    sha, size = write_json(path, {"items": [1, 2, 3]})
    assert path.exists()
    assert size == path.stat().st_size
    import hashlib

    assert sha == hashlib.sha256(path.read_bytes()).hexdigest()


def test_manifest_has_and_add():
    manifest = Manifest(
        version=1,
        cluster="c1",
        period_start="2026-08-01",
        period_end="2026-09-01",
        generated_at="2026-09-01T00:00:00Z",
        cdp_mcp_version="0.1.0",
    )
    assert not manifest.has("services/hdfs.json")
    manifest.add(
        FileRecord(
            path="services/hdfs.json",
            sha256="abc",
            bytes=10,
            entity_type="service_metrics",
            entity_name="hdfs",
            count=5,
        )
    )
    assert manifest.has("services/hdfs.json")
    assert len(manifest.files) == 1


def test_manifest_add_replaces_existing_record_for_same_path():
    manifest = Manifest(
        version=1,
        cluster="c1",
        period_start="2026-08-01",
        period_end="2026-09-01",
        generated_at="2026-09-01T00:00:00Z",
        cdp_mcp_version="0.1.0",
    )
    manifest.add(
        FileRecord(
            path="services/hdfs.json", sha256="old", bytes=1, entity_type="service_metrics",
            entity_name="hdfs", count=1,
        )
    )
    manifest.add(
        FileRecord(
            path="services/hdfs.json", sha256="new", bytes=2, entity_type="service_metrics",
            entity_name="hdfs", count=2,
        )
    )
    assert len(manifest.files) == 1
    assert manifest.files[0].sha256 == "new"


def test_save_and_load_manifest_roundtrip(tmp_path):
    manifest = Manifest(
        version=1,
        cluster="c1",
        period_start="2026-08-01",
        period_end="2026-09-01",
        generated_at="2026-09-01T00:00:00Z",
        cdp_mcp_version="0.1.0",
    )
    manifest.add(
        FileRecord(
            path="alerts.json", sha256="xyz", bytes=42, entity_type="alerts",
            entity_name="c1", count=7, metric_names=[],
        )
    )
    path = tmp_path / "manifest.json"
    save_manifest(path, manifest)

    loaded = load_manifest(path)
    assert loaded is not None
    assert loaded.cluster == "c1"
    assert loaded.has("alerts.json")
    assert loaded.files[0].sha256 == "xyz"


def test_load_manifest_missing_file_returns_none(tmp_path):
    assert load_manifest(tmp_path / "does_not_exist.json") is None


def test_load_manifest_corrupt_file_returns_none(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("not valid json {{{")
    assert load_manifest(path) is None


def test_manifest_json_is_stable_and_readable(tmp_path):
    manifest = Manifest(
        version=1, cluster="c1", period_start="s", period_end="e",
        generated_at="now", cdp_mcp_version="0.1.0",
    )
    path = tmp_path / "manifest.json"
    save_manifest(path, manifest)
    raw = json.loads(path.read_text())
    assert raw["cluster"] == "c1"
    assert raw["files"] == []
