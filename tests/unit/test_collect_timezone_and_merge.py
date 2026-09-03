"""Unit tests for:
- _to_utc_z / _default_period_label ordering (Jakarta offset handling)
- _merge_timeseries_chunks deduping duplicate points at chunk seams
"""
from cdp_mcp.collector.collect import (
    _default_period_label,
    _merge_timeseries_chunks,
    _to_utc_z,
)


def test_to_utc_z_converts_jakarta_offset_to_equivalent_utc_instant():
    # Jakarta midnight Aug 1 == UTC 17:00 the previous day.
    assert _to_utc_z("2026-08-01T00:00:00+07:00") == "2026-07-31T17:00:00Z"


def test_to_utc_z_is_a_no_op_for_already_utc_input():
    assert _to_utc_z("2026-08-01T00:00:00Z") == "2026-08-01T00:00:00Z"


def test_to_utc_z_treats_naive_input_as_already_utc():
    assert _to_utc_z("2026-08-01T00:00:00") == "2026-08-01T00:00:00Z"


def test_default_period_label_uses_wall_clock_month_not_utc_shifted_month():
    """The whole reason _default_period_label must run BEFORE _to_utc_z:
    Jakarta midnight Aug 1 shifts to July 31 in UTC -- labeling from the
    UTC-converted instant would misname the period "July"."""
    start = "2026-08-01T00:00:00+07:00"
    assert _default_period_label(start) == "August 2026"
    # confirm the UTC-shifted instant really would land in July, proving the
    # ordering requirement is real and not just documentation
    assert _to_utc_z(start).startswith("2026-07-31")


def _chunk(points: list[tuple[str, float]], metric="cpu_percent", entity="h1") -> dict:
    return {
        "items": [{
            "timeSeries": [{
                "data": [{"timestamp": t, "value": v, "type": "SAMPLE"} for t, v in points],
                "metadata": {"metricName": metric, "entityName": entity, "rollupUsed": "HOURLY"},
            }],
            "warnings": [],
        }],
        "effective_range": {"start": points[0][0] if points else None, "end": points[-1][0] if points else None},
    }


def test_merge_dedupes_shared_boundary_timestamp_between_chunks():
    # chunk 1: ..., 14:00 (the seam) -- chunk 2: 14:00 (same seam), 15:00, ...
    chunk1 = _chunk([("t12", 1.0), ("t13", 2.0), ("t14", 3.0)])
    chunk2 = _chunk([("t14", 3.0), ("t15", 4.0)])

    merged = _merge_timeseries_chunks([chunk1, chunk2])

    series = merged["items"][0]["timeSeries"][0]
    timestamps = [p["timestamp"] for p in series["data"]]
    assert timestamps == ["t12", "t13", "t14", "t15"], "seam timestamp must appear exactly once"


def test_merge_preserves_chronological_order_across_chunks():
    chunk1 = _chunk([("t2", 2.0), ("t1", 1.0)])  # out of order on purpose
    chunk2 = _chunk([("t4", 4.0), ("t3", 3.0)])

    merged = _merge_timeseries_chunks([chunk1, chunk2])
    timestamps = [p["timestamp"] for p in merged["items"][0]["timeSeries"][0]["data"]]
    assert timestamps == ["t1", "t2", "t3", "t4"]


def test_merge_keeps_series_separate_by_metric_and_entity():
    chunk1 = _chunk([("t1", 1.0)], metric="cpu_percent", entity="h1")
    chunk2 = _chunk([("t1", 9.0)], metric="physical_memory_used", entity="h1")

    merged = _merge_timeseries_chunks([chunk1, chunk2])
    series = merged["items"][0]["timeSeries"]
    assert len(series) == 2
    metrics = {s["metadata"]["metricName"] for s in series}
    assert metrics == {"cpu_percent", "physical_memory_used"}
