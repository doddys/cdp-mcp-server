"""Unit tests for cdp_mcp.collector.jobs — the in-process job registry that
backs the trigger_collection / get_collection_status MCP tools.

collect_cluster is patched so no CM/network/tunnel access is needed; the
tarball + manifest paths are exercised against tmp_path."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from cdp_mcp.collector import jobs
from cdp_mcp.collector.manifest import MANIFEST_FILENAME, Manifest


@pytest.fixture(autouse=True)
def _reset_job_registry():
    """Each test starts with an empty _jobs dict and a fresh semaphore."""
    jobs._jobs.clear()
    # Re-create the semaphore so a prior test's acquire (if any leaked) is
    # reset. Semaphore(1) starts unlocked.
    jobs._concurrency = asyncio.Semaphore(1)
    yield
    jobs._jobs.clear()


def _fake_manifest(out_dir: Path) -> Manifest:
    m = Manifest(
        period={"label": "August 2026", "start": "2026-08-01T00:00:00Z", "end": "2026-08-31T23:59:59Z"},
        cluster={"hint": "drc", "resolved_name": "DRC"},
    )
    (out_dir / MANIFEST_FILENAME).write_text(json.dumps(m.to_dict()))
    return m


async def test_start_collection_returns_running_then_done(tmp_path, monkeypatch):
    collect_root = tmp_path / "collect"
    exports_dir = tmp_path / "exports"
    out_dir_holder: dict[str, Path] = {}

    async def fake_collect_cluster(pool, cluster, start, end, out_dir, **kw):
        out_dir.mkdir(parents=True, exist_ok=True)
        _fake_manifest(out_dir)
        out_dir_holder["dir"] = out_dir
        return Manifest(
            period={"label": "August 2026", "start": start, "end": end},
            cluster={"hint": "drc", "resolved_name": cluster},
            files=[],
        )

    monkeypatch.setattr(jobs, "collect_cluster", fake_collect_cluster)

    job = await jobs.start_collection(
        pool=None,
        cluster="DRC",
        period_start="2026-08-01T00:00:00Z",
        period_end="2026-08-31T23:59:59Z",
        collect_root=collect_root,
        exports_dir=exports_dir,
        public_base_url="https://gateway.example.com",
    )

    assert job.state == "running"
    assert job.job_id in jobs._jobs
    assert job.out_dir.startswith(str(collect_root))

    # Wait for the background task to finish by polling until not running.
    for _ in range(200):
        if jobs.get_job(job.job_id).state != "running":
            break
        await asyncio.sleep(0.01)

    done = jobs.get_job(job.job_id)
    assert done.state == "done"
    assert done.file_count == 0
    assert done.download_url == f"https://gateway.example.com/exports/{job.job_id}.tar.gz"
    assert done.tarball_path is not None
    assert Path(done.tarball_path).exists()
    assert done.manifest_summary is not None
    assert done.manifest_summary["period"]["label"] == "August 2026"
    assert done.finished_at is not None


async def test_second_trigger_while_running_returns_busy(tmp_path, monkeypatch):
    collect_root = tmp_path / "collect"
    exports_dir = tmp_path / "exports"

    started = asyncio.Event()

    async def slow_collect(pool, cluster, start, end, out_dir, **kw):
        out_dir.mkdir(parents=True, exist_ok=True)
        started.set()
        await asyncio.sleep(0.2)  # hold the semaphore
        _fake_manifest(out_dir)
        return Manifest(
            period={"label": "August 2026", "start": start, "end": end},
            cluster={"hint": "drc", "resolved_name": cluster},
        )

    monkeypatch.setattr(jobs, "collect_cluster", slow_collect)

    first = await jobs.start_collection(
        pool=None, cluster="DRC",
        period_start="2026-08-01T00:00:00Z", period_end="2026-08-31T23:59:59Z",
        collect_root=collect_root, exports_dir=exports_dir,
        public_base_url="https://x",
    )
    await started.wait()  # ensure the first job has the semaphore

    second = await jobs.start_collection(
        pool=None, cluster="PRD",
        period_start="2026-08-01T00:00:00Z", period_end="2026-08-31T23:59:59Z",
        collect_root=collect_root, exports_dir=exports_dir,
        public_base_url="https://x",
    )

    assert second.state == "busy"
    assert second.job_id not in jobs._jobs  # rejected job not retained
    assert "already running" in second.error
    assert first.job_id in second.error  # points at the running job

    # let the first finish so the task doesn't warn on teardown
    for _ in range(200):
        if jobs.get_job(first.job_id).state != "running":
            break
        await asyncio.sleep(0.01)


async def test_collect_cluster_systemexit_marks_failed_not_running(tmp_path, monkeypatch):
    collect_root = tmp_path / "collect"
    exports_dir = tmp_path / "exports"

    async def rejecting_collect(pool, cluster, start, end, out_dir, **kw):
        raise SystemExit(
            f"{out_dir} already has _manifest.json for a different period."
        )

    monkeypatch.setattr(jobs, "collect_cluster", rejecting_collect)

    job = await jobs.start_collection(
        pool=None, cluster="DRC",
        period_start="2026-07-01T00:00:00Z", period_end="2026-07-31T23:59:59Z",
        collect_root=collect_root, exports_dir=exports_dir,
        public_base_url="https://x",
    )

    for _ in range(200):
        if jobs.get_job(job.job_id).state != "running":
            break
        await asyncio.sleep(0.01)

    failed = jobs.get_job(job.job_id)
    assert failed.state == "failed"
    assert "Rejected" in failed.error
    assert failed.download_url is None
    assert failed.tarball_path is None


async def test_get_job_unknown_returns_none():
    assert jobs.get_job("nonexistent") is None


def test_list_jobs_returns_registry_values(tmp_path):
    j = jobs.CollectionJob(
        job_id="abc", cluster="DRC",
        period_start="s", period_end="e",
        state="running", started_at="now",
    )
    jobs._jobs["abc"] = j
    listed = jobs.list_jobs()
    assert len(listed) == 1
    assert listed[0].job_id == "abc"


def test_to_dict_roundtrips_all_fields():
    j = jobs.CollectionJob(
        job_id="x", cluster="C", period_start="s", period_end="e",
        state="done", started_at="t0", finished_at="t1",
        out_dir="/o", tarball_path="/o.tar.gz",
        download_url="https://h/e/x.tar.gz", error=None,
        file_count=5, manifest_summary={"period": {}},
    )
    d = j.to_dict()
    assert d["job_id"] == "x"
    assert d["state"] == "done"
    assert d["file_count"] == 5
    assert d["download_url"].endswith("x.tar.gz")