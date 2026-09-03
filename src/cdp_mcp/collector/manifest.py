"""
manifest.py — content-addressed record of everything collect.py wrote to
disk, so a bundle can be verified (checksums, counts) after it crosses an
air-gap boundary, and so a re-run can resume instead of re-fetching entities
it already has.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

MANIFEST_VERSION = 1


@dataclass
class FileRecord:
    path: str  # relative to the bundle root
    sha256: str
    bytes: int
    entity_type: str  # "service_metrics" | "host_metrics" | "alerts" | "audit" | "role_map"
    entity_name: str
    count: int  # points, rows, or hosts -- meaning depends on entity_type
    metric_names: list[str] = field(default_factory=list)


@dataclass
class Manifest:
    version: int
    cluster: str
    period_start: str
    period_end: str
    generated_at: str
    cdp_mcp_version: str
    files: list[FileRecord] = field(default_factory=list)

    def has(self, path: str) -> bool:
        return any(f.path == path for f in self.files)

    def add(self, record: FileRecord) -> None:
        self.files = [f for f in self.files if f.path != record.path]
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
