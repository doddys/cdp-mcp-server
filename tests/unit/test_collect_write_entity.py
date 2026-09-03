"""Unit tests for cdp_mcp.collector.collect._write_entity -- the shared
fetch/record/resume helper every collection step goes through."""
import json

import pytest

from cdp_mcp.collector.collect import _write_entity
from cdp_mcp.collector.manifest import Manifest


def _manifest() -> Manifest:
    return Manifest(
        period={"label": "August 2026", "start": "s", "end": "e"},
        cluster={"hint": "c", "resolved_name": "Cluster"},
    )


async def test_failed_call_writes_not_available_wrapper_not_nothing(tmp_path):
    manifest = _manifest()

    async def failing_fetch():
        raise PermissionError("403: user lacks Cluster Administrator")

    result = await _write_entity("x.json", "some_tool", tmp_path, manifest, failing_fetch)

    assert result is None
    on_disk = json.loads((tmp_path / "x.json").read_text())
    assert on_disk == {"status": "not_available", "reason": "403: user lacks Cluster Administrator"}
    record = manifest.get("x.json")
    assert record is not None
    assert record.status == "not_available"
    assert record.item_count == 0


async def test_resumed_run_retries_a_previously_failed_entity(tmp_path):
    manifest = _manifest()
    calls = {"n": 0}

    async def flaky_fetch():
        calls["n"] += 1
        if calls["n"] == 1:
            raise PermissionError("denied")
        return {"items": [{"a": 1}]}

    await _write_entity("x.json", "some_tool", tmp_path, manifest, flaky_fetch)
    assert calls["n"] == 1

    result = await _write_entity("x.json", "some_tool", tmp_path, manifest, flaky_fetch)

    assert calls["n"] == 2, "a previously-failed entity must be retried, not skipped"
    assert result == {"items": [{"a": 1}]}
    record = manifest.get("x.json")
    assert record.status is None


async def test_resumed_run_skips_a_previously_successful_entity(tmp_path):
    manifest = _manifest()
    calls = {"n": 0}

    async def fetch():
        calls["n"] += 1
        return {"items": [{"a": 1}]}

    await _write_entity("x.json", "some_tool", tmp_path, manifest, fetch)
    assert calls["n"] == 1

    async def should_not_be_called():
        pytest.fail("a genuinely successful prior entity must not be re-fetched")

    result = await _write_entity("x.json", "some_tool", tmp_path, manifest, should_not_be_called)

    assert calls["n"] == 1
    assert result == {"items": [{"a": 1}]}


async def test_successful_empty_result_stays_bare_not_wrapped(tmp_path):
    manifest = _manifest()

    async def fetch():
        return []

    result = await _write_entity("x.json", "some_tool", tmp_path, manifest, fetch)

    assert result == []
    on_disk = json.loads((tmp_path / "x.json").read_text())
    assert on_disk == []
    record = manifest.get("x.json")
    assert record.status is None
    assert record.item_count == 0
