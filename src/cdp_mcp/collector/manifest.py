"""
manifest.py — _manifest.json schema shared with cdp-report's raw/ directory
convention, plus checksums so a bundle can be verified after it crosses an
air-gap boundary and a re-run can resume instead of re-fetching entities it
already has.

The leading underscore and field names (file/tool/item_count/truncated/
total_matched_in_range) are not cosmetic -- cdp-report-curate's
Prerequisites check, cdp-report-render's validation cross-checks, and
scripts/score_export_run.py all read _manifest.json by that exact name and
shape. sha256/bytes/metric_names are additive fields those consumers don't
read but this collector needs for its own integrity/resumability job.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

MANIFEST_FILENAME = "_manifest.json"


@dataclass
class FileRecord:
    file: str  # relative to the bundle root, e.g. "03_host_metrics_<host>.json"
    tool: str  # source API/method name, e.g. "get_host_metrics_raw"
    sha256: str
    bytes: int
    item_count: int  # points, rows, or hosts -- meaning depends on the file
    truncated: bool | None = None  # only set for entities with truncation semantics
    total_matched_in_range: int | None = None  # only set when the source call reports one
    metric_names: list[str] = field(default_factory=list)
    # "not_available" when the underlying call failed/was denied -- the file
    # still exists (see collect.py's _write_not_available) with
    # {"status": "not_available", "reason": ...} as its content, so a bare
    # empty []/{} can never be mistaken for "we don't know" (cdp-report-curate
    # needs this distinction for its Prerequisites check). None otherwise --
    # not forced to "ok", since a present "not_available" is the meaningful
    # signal and its absence already reads as fine.
    status: str | None = None


@dataclass
class Manifest:
    period: dict  # {"label": "August 2026", "start": "...", "end": "..."}
    cluster: dict  # {"hint": "astra_daas_drc", "resolved_name": "Astra DaaS ..."}
    files: list[FileRecord] = field(default_factory=list)
    cdp_mcp_version: str = "unknown"
    generated_at: str = ""

    def has(self, file: str) -> bool:
        return any(f.file == file for f in self.files)

    def get(self, file: str) -> FileRecord | None:
        return next((f for f in self.files if f.file == file), None)

    def add(self, record: FileRecord) -> None:
        self.files = [f for f in self.files if f.file != record.file]
        self.files.append(record)

    def to_dict(self) -> dict:
        return asdict(self)


def hash_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def write_json(path: Path, data: dict | list) -> tuple[str, int]:
    """Write data as JSON, then hash the file that actually landed on disk --
    avoids a separately-computed in-memory hash silently diverging from the
    real bytes written (encoding edge cases, partial writes)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))
    return hash_file(path)


def load_manifest(path: Path) -> Manifest | None:
    """Load an existing manifest for resumability. Returns None if absent or
    unparsable -- always treated as "start fresh", never a fatal error, so a
    corrupt manifest from an interrupted prior run doesn't block a re-run."""
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text())
        files = [FileRecord(**f) for f in raw.pop("files", [])]
        return Manifest(files=files, **raw)
    except Exception:
        return None


def save_manifest(path: Path, manifest: Manifest) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(manifest.to_dict(), indent=2, sort_keys=True))
