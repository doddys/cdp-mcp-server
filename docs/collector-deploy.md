# Offline collector (`cdp-collect`) — build, deploy, collect

End-to-end runbook for the standalone offline collector: build a bundle on
an internet-connected machine, carry it into a restricted network, collect a
month of cluster data against a CM the LLM client can't reach, and carry the
output back out for cdp-report's curate/render phases.

The collector lives in `src/cdp_mcp/collector/` and never imports
`server.py` — no LLM, no MCP transport, no `mcp` package at runtime. It
calls Cloudera Manager's REST API directly (plus YARN/Spark/HDFS/Oozie
where installed) and writes full-resolution JSON to local files. It skips
the `MAX_TIMESERIES_POINTS` cap that exists only to keep MCP tool results
under the transport's ~1 MB limit — irrelevant when writing to disk.

Output filenames (`NN_<name>.json`, flat) and `_manifest.json`'s shape
mirror cdp-report's interactive export exactly, so cdp-report's Phase 1.5+
tooling (curate/render, `score_export_run.py`) can run against the
collector's `--out` directory directly, with no conversion step.

---

## 1. Build the bundle (internet-connected machine)

`scripts/build_collector_bundle.sh` builds an offline-installable tarball.
Run it from the repo root on a machine with internet and `uv` installed:

```bash
scripts/build_collector_bundle.sh [output_dir]   # default: dist/collector-bundle/
```

Output: `dist/collector-bundle/cdp-collector-<version>-<platform>.tar.gz`,
plus a SHA-256 checksum on stdout — record it, you'll verify after
transfer.

### Build variables

| Variable | Default | Meaning |
|----------|---------|---------|
| `TARGET_PLATFORM` | `x86_64-manylinux2014` | Wheel platform of the *client site's* machines. Use `aarch64-manylinux2014` for arm64 Linux. |
| `TARGET_PYTHON` | `3.12` | Python minor version at the client site — the bundle requires a system `python3` matching it. Supported: `3.8`–`3.12` (the collector subpackage is 3.8-compatible even though the MCP server requires 3.11+; the build rewrites the wheel's `Requires-Python` accordingly and adds `eval_type_backport` for pydantic on <3.11). |
| `WITH_KERBEROS` | `false` | Vendor `httpx-gssapi` for SPNEGO to the four downstream services (YARN RM, Spark HS, HDFS NameNode, Oozie). See the guard below. |

Python 3.8 + Kerberos note: `httpx-gssapi` 0.4 dropped 3.8, so those builds
pin `httpx-gssapi==0.3.1` (last 3.8-capable release) with `httpx<0.28`.
Both support the `HTTPSPNEGOAuth(creds=...)` keytab path `spnego.py` uses.
**If the client's KDC is Active Directory, prefer the py3.12 bundle** —
0.3.1 predates httpx-gssapi 0.5's switch to SPNEGO-by-default and
negotiates with the plain krb5 mechanism, which has historically caused
interop friction against AD (the 3.8 bundle's README carries this caveat
too). Bundle filenames embed the Python version
(`…-py3.8-kerberos.tar.gz` vs `…-py3.12-kerberos.tar.gz`) so the two
can't be mistaken for each other.

### What's inside

- `vendor/` — the `cdp_mcp` wheel (installed `--no-deps`) plus only the
  dependencies the collector actually imports: `httpx[socks]`, `tenacity`,
  `pydantic`, `pydantic-settings`, `structlog`, `python-dotenv`,
  `python-dateutil`, `pyyaml`. Deliberately **excludes** `mcp` (server-only)
  and `impyla`/`thrift*`/`pure-sasl` (IcebergRegistry-only) — a plain
  `pip install` of the wheel would pull all of those in regardless.
- `run_collect.sh` — entry point; sets `PYTHONPATH` to the vendored deps
  and `REGISTRY_BACKEND=file`/`REGISTRY_FILE_PATH` to the bundle's own
  `cm_instances.yaml`, then execs
  `python3 -m cdp_mcp.collector.collect`.
- `cm_instances.yaml.example` — config template.
- `README.md` — the client-site quick start (same content as § 2–3 below).

Client-site prerequisites are just: a system `python3` matching
`TARGET_PYTHON`, `tar`, and network reachability to CM. No pip, no
internet, no compilers.

### Kerberos bundles must be built on a matching platform

`WITH_KERBEROS=true` requires the build host to match `TARGET_PLATFORM`
(Linux + same arch). The `gssapi` C extension has no prebuilt PyPI wheel;
`uv pip install --python-platform` only steers wheel *selection*, and when
no wheel matches it silently falls back to compiling with the **build**
machine's toolchain. Confirmed live: building the Kerberos extra for
x86_64 Linux from a macOS arm64 host produced Mach-O arm64 `.so` files
vendored inside a Linux-labeled bundle — an import failure waiting at the
client site, with no build-time warning. The script guards this and
refuses; build inside a matching Linux container instead. On Apple Silicon,
`--platform linux/amd64` matters (Docker Desktop defaults to arm64):

```bash
docker run --rm --platform linux/amd64 \
    -v "$PWD":/repo -w /repo \
    -e TARGET_PLATFORM=x86_64-manylinux2014 -e TARGET_PYTHON=3.8 \
    -e WITH_KERBEROS=true \
    python:3.8-slim bash -c '
        apt-get update && \
        apt-get install -y --no-install-recommends libkrb5-dev gcc pkg-config curl ca-certificates && \
        curl -LsSf https://astral.sh/uv/install.sh | sh && \
        export PATH="$HOME/.local/bin:$PATH" && \
        scripts/build_collector_bundle.sh
    '
```

A Kerberos bundle additionally needs MIT krb5 runtime libraries at the
client site (e.g. `libkrb5-3`/`krb5-libs` — normally already present on
any host that can `kinit`).

### Verify the build (optional but cheap)

```bash
tar -tzf dist/collector-bundle/cdp-collector-*.tar.gz | head
shasum -a 256 dist/collector-bundle/cdp-collector-*.tar.gz
```

---

## 2. Deploy at the client site

Transfer the tarball through the client's approved ingress channel, then:

```bash
sha256sum cdp-collector-<version>-<platform>.tar.gz   # must match build-time checksum
mkdir -p /opt/cdp-collector && tar -xzf cdp-collector-*.tar.gz -C /opt/cdp-collector
cd /opt/cdp-collector/cdp-collector-<version>-<platform>/
```

### Configure

```bash
cp cm_instances.yaml.example cm_instances.yaml
```

Fill in the site's CM host/credentials. Keys that matter most for the
collector:

- `host`, `port`, `username`, `password`, `use_tls`, `verify_ssl`,
  `api_version` — CM API access (Basic auth; Kerberos never applies to CM
  itself).
- `kerberos: true` — if the cluster's YARN/Spark/HDFS/Oozie UIs require
  SPNEGO. Then either `kinit` before running, or set `kerberos_keytab` +
  `kerberos_principal` for unattended runs (the TGT is re-acquired from the
  keytab on every call — no renewal cron needed).
- `disable_on_spnego` — with `kerberos: false`, skip a downstream service
  after its first SPNEGO challenge instead of failing every call.

To reach CM through a jump host / SOCKS tunnel, set `ALL_PROXY` (always
`socks5h://`, never `socks5://` — remote DNS) when invoking
`run_collect.sh`; see [Kerberos tunneling](kerberos-tunneling.md) for the
full ssh `-D` recipe. `httpx` honours the standard proxy env vars.

`cm_instances.yaml` holds credentials — never commit it, never carry it
back out with the output.

### Smoke test

```bash
./run_collect.sh --list-clusters
```

Prints the cluster names this CM knows. If this works, CM connectivity and
config are good.

---

## 3. Collect

```bash
./run_collect.sh --cluster <name> \
    --period-start 2026-08-01T00:00:00+07:00 \
    --period-end   2026-08-31T23:59:59+07:00 \
    --out output/<name>_202608/
```

Useful flags:

| Flag | Meaning |
|------|---------|
| `--period-label` | Override the manifest's label (default: derived from `--period-start`, e.g. "August 2026"). |
| `--cluster-hint` | Short slug for the manifest, matching cdp-report's `CLUSTER_NAME_HINT` convention. |
| `--services svc1,svc2` | Restrict service-metrics collection to named services (default: all). |
| `--concurrency N` | Max parallel host-metrics calls (default 4; raise for a beefier CM, lower if CM times out under load). |
| `--skip-downstream` | CM data only — skip YARN/Spark/HDFS/Oozie (not installed, or Kerberos not set up yet). |
| `--list-clusters` | Discover cluster names and exit. |

### Period conventions

- **`--period-end` is inclusive**: pass the last day's `23:59:59`, not the
  next day's `00:00:00` — matching cdp-report's own convention.
- **Non-UTC offsets are fine** — timestamps are normalized to UTC once at
  the top of collection; the period *label* is derived from the original
  string, so Jakarta midnight Aug 1 still labels "August 2026" even though
  the UTC instant is Jul 31 17:00.

### Resume behavior

The run is resumable and idempotent per entity. Re-running the identical
command after an interruption skips everything already recorded in
`_manifest.json` (matched by exact relative path). A file written as
`{"status": "not_available", "reason": ...}` after a failed call is
**retried** on resume — the cause (permission grant, `kinit`, network
blip) may be fixed by then. Resuming into a directory whose recorded
period disagrees with the requested one **refuses** rather than silently
mixing ranges — use a fresh `--out` per period.

### Runtime characteristics (from the live validation run)

- Host metrics are fetched in ≤14-day sub-ranges and merged (seams deduped)
  — a single full-month call gets coarsened to CM's `SIX_HOURLY` rollup;
  ≤14-day chunks keep `HOURLY`. Service metrics are deliberately not
  chunked (finer ranges don't yield finer service rollup on an aged
  period).
- Alerts/audit are collected per severity per week; impala long queries
  (`> 15m`) and YARN long apps (`> 15m`) are chunked weekly **and** merged
  into one combined deduped file each, which is what cdp-report-curate
  reads.
- Downstream services are individually optional — an undiscovered endpoint
  is logged and skipped, not an error.

---

## 4. Egress and handoff to cdp-report

The `--out` directory is self-contained and what physically leaves the
network:

```
output/<name>_202608/
├── _manifest.json          # period, cluster, per-file sha256 + counts
├── 01_host_status.json
├── 02_roles_hdfs.json
├── 03_service_metrics_yarn.json
├── 06_alerts_CRITICAL_w2.json
└── ...
```

Move the whole directory out through the client's approved egress channel
(SFTP, encrypted USB, file-transfer portal). On the internet-connected
machine, point cdp-report's Phase 1.5+ tooling (curate/render) at the
directory directly — no conversion step; the manifest's sha256s let you
verify integrity after transfer.

**The output is not masked.** Nothing in this bundle redacts
hostnames/IPs before the data leaves the network — treat the output
directory as sensitive, control who/what can access it in transit, and
follow the client's data-handling policy for what may leave.

---

## Troubleshooting

| Symptom | Meaning / fix |
|---------|---------------|
| `--list-clusters` fails | CM host/credentials/TLS in `cm_instances.yaml`, or `ALL_PROXY` tunnel down. |
| Files with `status: not_available` | That call failed (SPNEGO, permission, transient). Fix the cause and re-run the same command — `not_available` records are retried on resume. |
| `already has _manifest.json for period ... but this run requested ...` | Different `--period-*` than the directory was created with. Use a new `--out`, or delete it to start over. |
| SPNEGO challenges on every downstream call | Cluster is Kerberized: set `kerberos: true` (+ `kinit` or keytab fields), or use `--skip-downstream` for CM-only collection. |
| Import error on `gssapi` at client site | Kerberos bundle was built on a mismatched platform — rebuild via the Docker recipe in § 1. |
| `truncated: true` in an alerts/audit file | That chunk matched more events than the collector's per-file cap; the manifest's `total_matched_in_range` records how many. |
| Metrics coarse (6h spacing) | Expected if host metrics were fetched as one full-period call — the collector avoids this via ≤14-day chunks; check you're on a current bundle. |
