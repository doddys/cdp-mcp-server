"""
jobs.py — in-process job registry for triggering cdp-collect runs via MCP.

The collector is invoked as a library (``collect_cluster``) on the MCP server's
event loop, reusing the live ``CMPool`` rather than building a second one. This
module owns the fire-and-forget task + the job-state dict that the
``trigger_collection`` / ``get_collection_status`` tools in ``server.py`` read.

Design notes (see CLAUDE.md "Triggering via MCP"):

- **One collector at a time.** A ``Semaphore(1)`` guards concurrent runs — a
  second ``start_collection`` while one is running returns a "busy" sentinel
  instead of queueing silently, so the caller can poll the running job and
  decide. The collector's own internal semaphore bounds per-host concurrency;
  this one bounds whole-collection concurrency against the shared pool/tunnel.
- **In-memory only.** ``_jobs`` is a module-level dict; a daemon restart kills
  any running task and orphans its entry. The out dir + tarball survive on
  disk, so job metadata could be rehydrated from ``exports/`` later, but that
  rehydration is out of scope for the first cut. ``get_job`` for a pre-restart
  id returns None — honest "not found", not a stale "running".
- **Downloads bypass MCP.** The tarball is written to ``exports_dir`` (served
  by nginx, gated by ``X-Gateway-Token``); the status tool returns its
  ``https://`` URL. The tarball is binary and tens of MB — it must not flow
  through an MCP tool result (capped at ~1MB) nor through the mask proxy
  (JSON-RPC-aware, mangles non-JSON).
- **Dependency direction stays server → collector.** This module imports
  ``collect_cluster`` / ``Manifest`` only; it never imports ``server.py`` or
  ``mcp``. ``server.py`` imports this module.
"""
from __future__ import annotations

import asyncio
import tarfile
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog

from cdp_mcp.cm_pool import CMPool
from cdp_mcp.collector.collect import collect_cluster
from cdp_mcp.collector.manifest import MANIFEST_FILENAME, load_manifest

# datetime.UTC is 3.11+; the collector targets 3.8+ (see build_collector_bundle.sh
# TARGET_PYTHON). Alias the timeless timezone.utc spelling instead -- UP017's
# datetime.UTC preference reflects the MCP server's py311 floor, not the
# collector's, so silence it for just this line.
UTC = timezone.utc  # noqa: UP017 -- deliberate 3.8-compatible spelling

log = structlog.get_logger(__name__)

# Whole-collection concurrency: one collector at a time against the shared
# pool/tunnel. Non-blocking acquire so a second trigger reports "busy" rather
# than queuing behind a multi-minute run.
_concurrency = asyncio.Semaphore(1)


@dataclass
class CollectionJob:
    job_id: str
    cluster: str
    period_start: str
    period_end: str
    state: str  # "running" | "done" | "failed"
    started_at: str
    finished_at: str | None = None
    out_dir: str = ""
    tarball_path: str | None = None
    download_url: str | None = None
    error: str | None = None
    file_count: int | None = None
    manifest_summary: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# job_id -> job. Process-wide, like server.py's _pool. Not persisted.
_jobs: dict[str, CollectionJob] = {}


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _manifest_summary(out_dir: Path) -> dict[str, Any] | None:
    """Read the just-written _manifest.json for a compact summary the status
    tool can return without re-reading every file."""
    m = load_manifest(out_dir / MANIFEST_FILENAME)
    if m is None:
        return None
    truncated = [f.file for f in m.files if f.truncated]
    not_available = [f.file for f in m.files if f.status == "not_available"]
    return {
        "period": m.period,
        "cluster": m.cluster,
        "cdp_mcp_version": m.cdp_mcp_version,
        "generated_at": m.generated_at,
        "file_count": len(m.files),
        "truncated_files": truncated,
        "not_available_files": not_available,
    }


def _tar_dir(out_dir: Path, tarball_path: Path) -> int:
    """Tar the out dir (the NN_<name>.json files + _manifest.json) into a
    gzip tarball. Returns the tarball size in bytes. The archive root is the
    out dir's basename so extracting it produces a single directory, matching
    how cdp-report expects a raw/ tree to be laid out."""
    tarball_path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(tarball_path, "w:gz") as tar:
        tar.add(out_dir, arcname=out_dir.name)
    return tarball_path.stat().st_size


async def start_collection(
    pool: CMPool,
    cluster: str,
    period_start: str,
    period_end: str,
    *,
    exports_dir: Path,
    public_base_url: str,
    services: set[str] | None = None,
    skip_downstream: bool = False,
    period_label: str | None = None,
    cluster_hint: str | None = None,
    collect_root: Path,
) -> CollectionJob:
    """Start a background collection. Returns immediately with state="running",
    or with state="busy" if a collection is already running (the busy job
    carries the running job's id so the caller can poll it). Never blocks."""
    job_id = uuid.uuid4().hex
    out_dir = collect_root / job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    job = CollectionJob(
        job_id=job_id,
        cluster=cluster,
        period_start=period_start,
        period_end=period_end,
        state="running",
        started_at=_utc_now(),
        out_dir=str(out_dir),
    )
    _jobs[job_id] = job

    # Non-blocking: if a collection is already running, don't queue — report
    # busy so the caller can poll the existing job and decide.
    if _concurrency.locked():
        running = next(
            (j for j in _jobs.values() if j.state == "running"), None
        )
        running_id = running.job_id if running is not None else "unknown"
        job.state = "busy"
        job.error = (
            f"A collection is already running (job_id={running_id}). "
            "One collector at a time against the shared CM pool/tunnel. "
            "Poll get_collection_status for the running job."
        )
        # Don't keep the rejected job in the registry — it didn't run.
        del _jobs[job_id]
        return job

    asyncio.create_task(
        _run_collection(
            job,
            pool,
            cluster,
            period_start,
            period_end,
            out_dir,
            services=services,
            skip_downstream=skip_downstream,
            period_label=period_label,
            cluster_hint=cluster_hint,
            exports_dir=exports_dir,
            public_base_url=public_base_url,
        )
    )
    return job


async def _run_collection(
    job: CollectionJob,
    pool: CMPool,
    cluster: str,
    period_start: str,
    period_end: str,
    out_dir: Path,
    *,
    services: set[str] | None,
    skip_downstream: bool,
    period_label: str | None,
    cluster_hint: str | None,
    exports_dir: Path,
    public_base_url: str,
) -> None:
    async with _concurrency:
        try:
            manifest = await collect_cluster(
                pool,
                cluster,
                period_start,
                period_end,
                out_dir,
                concurrency=5,
                service_filter=services,
                skip_downstream=skip_downstream,
                period_label=period_label,
                cluster_hint=cluster_hint,
            )
            job.file_count = len(manifest.files)
            job.manifest_summary = _manifest_summary(out_dir)
            tarball_path = exports_dir / f"{job.job_id}.tar.gz"
            _tar_dir(out_dir, tarball_path)
            job.tarball_path = str(tarball_path)
            base = public_base_url.rstrip("/")
            job.download_url = f"{base}/exports/{job.job_id}.tar.gz"
            job.state = "done"
            log.info(
                "collector.job.done",
                job_id=job.job_id,
                cluster=cluster,
                files=job.file_count,
                tarball_bytes=tarball_path.stat().st_size,
            )
        except SystemExit as exc:
            # collect_cluster raises SystemExit for the period-drift guard
            # (existing dir's manifest period != requested) and for unknown
            # cluster — a rejection, not a mid-run failure.
            job.state = "failed"
            job.error = f"Rejected: {exc}"
            log.warning("collector.job.rejected", job_id=job.job_id, error=str(exc))
        except Exception as exc:
            job.state = "failed"
            job.error = f"{type(exc).__name__}: {exc}"
            log.warning("collector.job.failed", job_id=job.job_id, error=str(exc))
        finally:
            job.finished_at = _utc_now()


def get_job(job_id: str) -> CollectionJob | None:
    return _jobs.get(job_id)


def list_jobs() -> list[CollectionJob]:
    return list(_jobs.values())