"""Unit tests for cdp_mcp.collector.manifest."""

from __future__ import annotations
import json

from cdp_mcp.collector.manifest import (
    FileRecord,
    Manifest,
    load_manifest,
    save_manifest,
    write_json,
)


def _manifest(**overrides) -> Manifest:
    defaults = dict(
        period={"label": "August 2026", "start": "2026-08-01T00:00:00Z", "end": "2026-09-01T00:00:00Z"},
        cluster={"hint": "astra_daas_drc", "resolved_name": "Astra DaaS Data Lake DRC Cluster"},
        cdp_mcp_version="0.1.0",
        generated_at="2026-09-01T00:00:00Z",
    )
    defaults.update(overrides)
    return Manifest(**defaults)


def test_write_json_hashes_actual_bytes_on_disk(tmp_path):
    path = tmp_path / "03_service_metrics_hdfs.json"
    sha, size = write_json(path, {"items": [1, 2, 3]})
    assert path.exists()
    assert size == path.stat().st_size
    import hashlib

    assert sha == hashlib.sha256(path.read_bytes()).hexdigest()


def test_manifest_has_and_add():
    manifest = _manifest()
    assert not manifest.has("03_service_metrics_hdfs.json")
    manifest.add(
        FileRecord(
            file="03_service_metrics_hdfs.json",
            tool="get_service_metrics_raw",
            sha256="abc",
            bytes=10,
            item_count=5,
        )
    )
    assert manifest.has("03_service_metrics_hdfs.json")
    assert len(manifest.files) == 1


def test_manifest_add_replaces_existing_record_for_same_file():
    manifest = _manifest()
    manifest.add(
        FileRecord(file="03_service_metrics_hdfs.json", tool="get_service_metrics_raw",
                   sha256="old", bytes=1, item_count=1)
    )
    manifest.add(
        FileRecord(file="03_service_metrics_hdfs.json", tool="get_service_metrics_raw",
                   sha256="new", bytes=2, item_count=2)
    )
    assert len(manifest.files) == 1
    assert manifest.files[0].sha256 == "new"


def test_file_record_truncation_fields_default_to_none():
    record = FileRecord(file="a.json", tool="get_alerts", sha256="x", bytes=1, item_count=1)
    assert record.truncated is None
    assert record.total_matched_in_range is None


def test_save_and_load_manifest_roundtrip(tmp_path):
    manifest = _manifest()
    manifest.add(
        FileRecord(
            file="06_alerts_IMPORTANT_w1.json", tool="get_alerts", sha256="xyz", bytes=42,
            item_count=7, truncated=False, total_matched_in_range=7,
        )
    )
    path = tmp_path / "_manifest.json"
    save_manifest(path, manifest)

    loaded = load_manifest(path)
    assert loaded is not None
    assert loaded.cluster["hint"] == "astra_daas_drc"
    assert loaded.period["label"] == "August 2026"
    assert loaded.has("06_alerts_IMPORTANT_w1.json")
    assert loaded.files[0].sha256 == "xyz"
    assert loaded.files[0].total_matched_in_range == 7


def test_load_manifest_missing_file_returns_none(tmp_path):
    assert load_manifest(tmp_path / "does_not_exist.json") is None


def test_load_manifest_corrupt_file_returns_none(tmp_path):
    path = tmp_path / "_manifest.json"
    path.write_text("not valid json {{{")
    assert load_manifest(path) is None


def test_manifest_json_uses_expected_field_names(tmp_path):
    """The leading underscore and field names aren't cosmetic -- cdp-report's
    curate/render/score_export_run.py all read _manifest.json by this exact
    shape (file/tool/item_count/truncated/total_matched_in_range)."""
    manifest = _manifest()
    manifest.add(
        FileRecord(
            file="01_host_status.json", tool="get_host_status", sha256="s", bytes=1, item_count=8,
        )
    )
    path = tmp_path / "_manifest.json"
    save_manifest(path, manifest)
    raw = json.loads(path.read_text())
    assert raw["period"]["start"] == "2026-08-01T00:00:00Z"
    assert raw["cluster"]["resolved_name"] == "Astra DaaS Data Lake DRC Cluster"
    f = raw["files"][0]
    assert f["file"] == "01_host_status.json"
    assert f["tool"] == "get_host_status"
    assert f["item_count"] == 8
