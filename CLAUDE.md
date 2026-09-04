# CLAUDE.md — cdp-mcp-server

Python MCP server for Cloudera/CDP cluster administration and troubleshooting.
Fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — Apache 2.0.

---

## General directives

### Python and tooling
- Use **uv** for virtualenv, dependency, and lockfile management. `uv.lock` is committed; `poetry.lock` is not used.
- The virtualenv lives in `.venv/` (in-project). Create it with `uv venv --python 3.12`, then `uv sync --extra dev` to install.
- The build backend is `poetry-core` (PEP 517) for the src-layout package; this is independent of the dev tool. Plain `pip install -e ".[dev]"` also works (and is what CI uses).
- To run commands in the venv: `.venv/bin/python`, `.venv/bin/pytest`, `.venv/bin/ruff`, etc. (or prefix with `uv run`).

### Git and GitHub
- Public personal repo on **github.com** (NOT github.dxc.com, which is corporate).
- Always use `GH_HOST=github.com gh ...` for all `gh` commands in this project.
- Never commit credentials or files with real data.
- Config files with credentials → always commit a `*.example`, and put the real file in `.gitignore`.
- Do not add references to real environments or hosts in the repository.

### Work sessions
- Save sessions in `./.claude/sessions/` (project-local path, NOT in ~/.claude).
- Use the `save-session` skill at the end of a session and `load-session` at the start.

### License
Apache License 2.0 — same as the original dvergari repo.

---

## Quick setup

```bash
# Clone
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server

# Python 3.12 virtualenv (uv)
uv venv --python 3.12
uv sync --extra dev

# Config
cp .env.example .env
cp cm_instances.yaml.example cm_instances.yaml
# Edit cm_instances.yaml with your test cluster credentials

# Start (FileRegistry)
REGISTRY_BACKEND=file cdp-mcp

# Quick start (EnvRegistry — single CM)
REGISTRY_BACKEND=env CM_HOST=cm.example.com CM_USERNAME=admin CM_PASSWORD=changeme CM_USE_TLS=true cdp-mcp
```

### Transports (stdio vs network daemon)
`server.py:run()` calls `mcp.run(transport=...)`, read from `MCP_TRANSPORT`
(default `stdio`). stdio is the default — the server is spawned by an MCP client
(Claude Desktop/CLI) over stdin/stdout. To run cdp-mcp as a **long-lived
network daemon** (e.g. under systemd), set `MCP_TRANSPORT=streamable-http` (or
`sse`) plus `MCP_HOST`/`MCP_PORT` (bound by FastMCP at construction; default
`127.0.0.1:8000`). Do **not** run a stdio server as a standalone systemd service
— stdin is `/dev/null`, so it reads EOF and exits cleanly (exit 0) right after
startup. Use the HTTP transport for a systemd daemon; clients connect to
`http://<host>:<port>/mcp`. **Security:** the HTTP transport has no built-in
auth — keep the bind on 127.0.0.1 (SSH-tunnel in) or put auth/reverse-proxy in
front; never expose `MCP_HOST=0.0.0.0` on a public VPS unprotected.

**Shared-secret bearer gate (optional):** set `MCP_AUTH_TOKEN` to gate the
streamable-http/sse endpoint — requests must then send
`Authorization: Bearer <MCP_AUTH_TOKEN>` or they get 401. Implemented as a small
ASGI middleware (`server._BearerAuthMiddleware`) that wraps
`mcp.streamable_http_app()` (run via `server._run_http_server`), NOT FastMCP's
OAuth `token_verifier` — a static shared secret is the wrong fit for the OAuth
flow standard MCP clients negotiate. Constant-time compare (`hmac.compare_digest`);
non-HTTP scopes (lifespan) pass through; stdio is unaffected. Unset = open
(loopback + SSH tunnel only). A reverse proxy in front may set/forward the
`Authorization` header.

**Concurrent sessions share one CMPool (ref-counted singleton):** under
streamable-http/sse, the `mcp` library's `StreamableHTTPSessionManager` invokes
`server._lifespan()` once per **session** (each client connection gets its own
`Server.run()` call), not once per process. A single MCP client naturally
produces overlapping sessions on a long-lived daemon — reconnect after a
network blip, client restart, multiple editor windows/tabs pointed at the same
endpoint — so `_lifespan` must not treat `_registry`/`_pool` as per-session
state. It's reference-counted instead: the first session in builds the shared
`CMPool`/registry and the last session out tears it down (`_lifespan_lock` +
`_active_sessions` in `server.py`); a failed startup resets the globals so a
later session retries rather than reusing a broken pool. Getting this wrong
previously crashed sessions in production — one session's teardown closed the
httpx clients a different, still-active session was using mid-request
(`AssertionError: Client not initialised`). For stdio there is exactly one
session per process, so this is a plain start/stop either way. See
`tests/unit/test_server_lifespan.py` for the regression coverage.

---

## Code structure

```
src/cdp_mcp/
├── server.py            ← FastMCP entry point, @mcp.tool() definitions
├── config.py            ← Pydantic settings + build_registry() factory
├── cm_client.py         ← httpx async client for CM API (do not modify without reason)
├── cm_pool.py           ← multi-CM connection pool + auto-discovery endpoint
├── registry/
│   ├── base.py          ← BaseRegistry ABC: start/stop/get_all/register/deactivate
│   ├── iceberg.py       ← IcebergRegistry (original dvergari code)
│   ├── file_registry.py ← FileRegistry (YAML with env interpolation)
│   └── env_registry.py ← EnvRegistry (single CM from CM_HOST/CM_PORT/etc.)
├── clients/
│   ├── yarn_client.py   ← YARN ResourceManager REST API (:8088)
│   ├── spark_client.py  ← Spark History Server REST API (:18088)
│   ├── hdfs_client.py   ← HDFS NameNode JMX (:9870)
│   ├── oozie_client.py  ← Oozie REST API (:11000)
│   ├── errors.py        ← shared SpnegoRequiredError / SpnegoConfigError
│   └── spnego.py        ← lazy httpx-gssapi SPNEGO auth factory (optional [kerberos] extra)
└── collector/            ← standalone offline collector (cdp-collect) — see § below
    ├── collect.py        ← orchestration + CLI; never imports server.py/mcp
    ├── manifest.py        ← _manifest.json schema + checksums
    └── metrics_catalog.py ← curated/discovery metric names per service type

scripts/
└── build_collector_bundle.sh  ← packages collector/ + deps as an offline-installable tarball

tests/
├── unit/                ← Unit tests (httpx mocked with respx, no external dependencies)
└── integration/         ← Integration tests (WireMock via docker-compose)
    ├── docker-compose.yml
    ├── wiremock/        ← Stub definitions for CM, YARN, HDFS, Spark
    └── cm_instances.yaml  ← Gitignored — local config for tests
```

---

## Architectural rules

### 1. Do not modify cm_client.py without reason
`cm_client.py` is dvergari's original code, tested and working. Modify only for bug fixes or strictly necessary extensions.

### 2. New clients follow the same pattern as cm_client.py
Every client in `clients/` must:
- Use `httpx.AsyncClient` with an explicit timeout
- Have retries via `tenacity` for `TransportError` and 503/504
- Have the same exception hierarchy: `XxxClientError → XxxAuthError → XxxNotFoundError → XxxServiceUnavailable`
- Log with `structlog`
- Never throw untyped exceptions toward `server.py`

### 3. Auto-discovery, not manual configuration
YARN/Spark/Oozie/HDFS endpoints are discovered from CM at startup in `cm_pool.py → _discover_service_endpoints()`. Do not add env vars for these URLs. Acceptable override: `endpoints_override` in `cm_instances.yaml`.

### 4. Tools in server.py are read-mostly
New troubleshooting tools (YARN, Spark, HDFS, Oozie) are all **read-only**. Mutating tools require explicit discussion.

### 5. Clean fallbacks on application tools
If an endpoint is not discovered → return a structured JSON message, never a traceback.

### 6. Registry backend
| Backend | Use case | Requires |
|---------|----------|---------|
| `file` | development, small teams | `cm_instances.yaml` |
| `env` | single CM, smoke test | env vars `CM_HOST`, `CM_USERNAME`, `CM_PASSWORD` |
| `iceberg` | CDP with Impala/Iceberg | Impala/HiveServer2 + Iceberg table |

Default: `iceberg` (dvergari backward compatibility). For development use `file` or `env`.

### 7. Bound large tool responses
A tool that returns an unbounded CM payload can exceed the MCP ~1 MB
tool-result cap and return nothing usable (confirmed live: HDFS/Hive
replication schedules with unbounded embedded history). Every tool whose
response can grow large must bound itself, in this order of preference:

1. **Server-side caps first.** Prefer documented CM params (`maxSchedules`,
   `maxCommands`, `maxResults`/`resultOffset`, `view=summary`, `limit`) over
   fetching everything and trimming in Python — less bandwidth, correct
   semantics. `view=summary` often carries the scalar counters at ~10–40×
   smaller than `full`; use `full` only for per-record detail. Don't trust a
   documented server-side filter blindly, though — `list_yarn_apps`'
   `startedTimeBegin`/`startedTimeEnd` are real YARN RM API params, but were
   observed live returning the RM's full cached app list regardless of the
   requested bounds; time-range and duration filters there are always
   re-applied client-side as a correctness backstop, not just for bounding.
2. **Client-side pagination where the server has no time filter.** When CM
   has no `from`/`to` (e.g. `/replications/{id}/history`), page by offset with
   a client-side cutoff on each record's timestamp (history is newest-first).
3. **Aggregation for wide windows.** For a report spanning a wide time range
   (e.g. one month), aggregate counters page-by-page and return totals, never
   accumulate raw records — `get_replication_metrics` is the model.
4. **Client-side downsampling for continuous/point-series data.** Some
   payloads (CM `/timeseries`) aren't record lists that can be paginated or
   aggregated into totals — they're continuous series where the caller wants
   the *shape*, not a subset. `get_service_metrics`/`get_host_metrics` cap
   each series to `ClouderaManagerClient.MAX_TIMESERIES_POINTS` (2000) and
   offer a `sample_mode` so the caller picks the trade-off: `"even"` merges
   the full range into 2000 contiguous buckets (trend queries), `"recent"`
   keeps full resolution for only the most recent slice (incident response).
   Confirmed live: an uncapped `get_host_metrics` call returned ~17MB, slow
   enough end-to-end (through the deployment's masking proxy) that the
   downstream MCP client gave up and dropped the connection.

   `"even"` mode **merges** buckets rather than picking one representative
   point and discarding the rest — decimation would silently drop every
   spike/dip between kept samples, which is an accuracy problem for a report,
   not just a size one. Each bucket's `value` is a mean over its underlying
   points, computed via `_bucket_merge_series`/`_merge_stat_groups`: an exact
   pooled-variance merge (min-of-mins, max-of-maxes, summed counts, combined
   mean/stdDev via the standard parallel-variance identity) that's correct
   whether or not CM's own points already carried `aggregateStatistics` —
   fresh/RAW-granularity points never do (nothing to aggregate over yet), so
   `_point_stat_group` treats those as trivial one-sample subgroups; the math
   is uniform either way, not an approximation.

   Every point is also slimmed to `timestamp`/`value`/`type` by default,
   stripping CM's verbose per-point `aggregateStatistics` block (min/max/
   mean/stdDev/count/sampleTime/minTime/maxTime, ~5x the bytes of a bare
   point) — the dominant size driver even *within* the point cap on real
   report-generator traffic. This is opt-out, not silent data loss: pass
   `include_aggregate_stats=True` when a caller specifically needs min/max/
   mean (e.g. a report chart) — under `"even"` mode this returns the
   *synthesized* merged stats above, flagged on the series with
   `aggregate_stats_synthesized: true` so it's never mistaken for a value CM
   itself returned.
   `aggregateStatistics` presence/absence is never a signal of CM's own
   rollup level (RAW/TEN_MINUTELY/HOURLY/SIX_HOURLY/DAILY, which CM ages a
   series through independent of this parameter) — a same-nominal-window
   comparison that looked like "fresh=HOURLY+agg vs aged=SIX_HOURLY+no-agg"
   turned out to straddle this code's own deploy boundary (one fetch predated
   the stripping fix, one postdated it) rather than reflecting CM's rollup
   aging — confirmed by comparing fetch timestamps against the deploy
   restart time. CM's rollup aging is still real and still affects point
   spacing/count; check `timeSeries[].metadata.rollupUsed` in the response
   for actual resolution, never the presence of `aggregateStatistics`.

Return a structured envelope (`{items, count, truncated, ...,
effective_range}`) consistent with `get_alerts`/`get_audit_events`/
`get_service_logs`/`get_replication_*`/`get_service_metrics`/
`get_host_metrics`, so the caller can tell "nothing matched" from "we
stopped looking". Never return a bare unbounded list.

---

## Useful commands

```bash
# Run in development (stdio)
REGISTRY_BACKEND=file cdp-mcp

# Unit tests (no external dependencies)
.venv/bin/pytest tests/unit/ -v

# Integration tests (requires docker compose up)
docker compose -f tests/integration/docker-compose.yml up -d
REGISTRY_BACKEND=file REGISTRY_FILE_PATH=tests/integration/cm_instances.yaml \
  .venv/bin/pytest tests/integration/ -v
docker compose -f tests/integration/docker-compose.yml down

# Manual smoke test (single tool via stdin)
echo '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"list_clusters","arguments":{}}}' \
  | REGISTRY_BACKEND=env CM_HOST=fake CM_USERNAME=x CM_PASSWORD=x cdp-mcp

# Lint
.venv/bin/ruff check src/
.venv/bin/mypy src/ --ignore-missing-imports

# Offline collector: build the deployable bundle (see § below)
scripts/build_collector_bundle.sh
TARGET_PLATFORM=aarch64-manylinux2014 TARGET_PYTHON=3.12 scripts/build_collector_bundle.sh
TARGET_PLATFORM=x86_64-manylinux2014 TARGET_PYTHON=3.8 WITH_KERBEROS=true \
  scripts/build_collector_bundle.sh   # 3.8 supported; Kerberos needs a matching-platform Linux host/container
WITH_KERBEROS=true scripts/build_collector_bundle.sh   # only on a host matching TARGET_PLATFORM

# Offline collector: run directly from this checkout (no bundle needed for local testing)
REGISTRY_BACKEND=file REGISTRY_FILE_PATH=cm_instances.yaml .venv/bin/cdp-collect --list-clusters
REGISTRY_BACKEND=file REGISTRY_FILE_PATH=cm_instances.yaml .venv/bin/cdp-collect \
  --cluster "my-cluster" --period-start 2026-08-01T00:00:00+07:00 \
  --period-end 2026-08-31T23:59:59+07:00 --out output/my-cluster_202608/
```

---

## Claude Desktop configuration

### FileRegistry (recommended for development)
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "file",
        "REGISTRY_FILE_PATH": "/path/to/cm_instances.yaml"
      }
    }
  }
}
```

### EnvRegistry (single CM, minimal setup)
```json
{
  "mcpServers": {
    "cdp": {
      "command": "/path/to/cdp-mcp-server/.venv/bin/cdp-mcp",
      "env": {
        "REGISTRY_BACKEND": "env",
        "CM_HOST": "cm.example.com",
        "CM_USERNAME": "admin",
        "CM_PASSWORD": "changeme",
        "CM_USE_TLS": "true"
      }
    }
  }
}
```

---

## GitHub Pages documentation
- Documentation goes in `docs/gh-pages/`.
- When a first stable release is ready, generate the documentation.
- Dedicated `gh-pages` branch for deployment.

## Kerberos / SPNEGO (implemented)

`cm_client.py` uses Basic auth against CM and is never touched by Kerberos.
The four downstream clients (YARN RM, Spark HS, HDFS NameNode JMX, Oozie) attach
SPNEGO auth when a CM instance has `kerberos=true` (`CM_KERBEROS=true` env /
`kerberos:` yaml key).

- Library: `httpx-gssapi` (`HTTPSPNEGOAuth`, implements `httpx.Auth`), attached
  **only** to the four downstream clients — never to `cm_client.py`. Declared as
  the optional `[kerberos]` extra in `pyproject.toml`; the import in
  `clients/spnego.py` is lazy so the base install stays krb5-free for
  non-Kerberized users.
- Credentials: two sources, picked per CM instance:
  - **Default Kerberos credentials cache** (a `kinit` TGT, or a keytab loaded
    into the ccache) — the original path, used when no keytab is configured.
    `build_spnego_auth(kerberos)` builds/caches one `HTTPSPNEGOAuth` per process
    and raises a typed `SpnegoConfigError` (actionable message, no traceback) if
    the extra is missing or no TGT is available.
  - **In-process keytab acquisition** (unattended production) — set
    `kerberos_keytab` + `kerberos_principal` on the instance (`CM_KERBEROS_KEYTAB`
    / `CM_KERBEROS_PRINCIPAL` env). `build_spnego_auth` acquires a TGT directly
    from the keytab via `gssapi.Credentials(usage='initiate', name=..., store=
    {'client_keytab': ...})` and hands it to `HTTPSPNEGOAuth(creds=...)`. The downstream
    client factories rebuild the auth per tool invocation, so the TGT is
    re-acquired from the keytab on each call — automatic renewal, no external
    `kinit`/cron needed. Validation failures (missing principal, missing/unreadable
    keytab, gssapi acquisition error) raise `SpnegoConfigError`.
- Injection point: `cm_pool.py` factory methods `get_{yarn,spark,hdfs,oozie}_client`
  (the single place that constructs downstream clients and attaches auth).
  `server.py` calls these instead of constructing clients inline.
- Short-circuit: the per-cluster `spnego_required` set + `disable_on_spnego`
  apply **only when `kerberos=false`** (the unconfigured fallback). With
  `kerberos=true` the clients always attempt SPNEGO.
- Outbound proxying (e.g. `socks5h://` for internal-only hostnames) is via httpx
  `trust_env` — set `ALL_PROXY`/`HTTPS_PROXY`. No proxy config field.
- Trade-off: system library dependency (MIT `libkrb5-dev`/`krb5-libs`) → heavier
  image, only when the `[kerberos]` extra is installed.

### In-process keytab acquisition (implemented)
The server supports two SPNEGO credential sources (see above). The default-ccache
path still relies on a TGT already in the ccache, so a long-running process using
*that* path needs TGT renewal (cron `kinit -R`, or a keytab loaded into the ccache
by an external mechanism). The **in-process keytab path** (`kerberos_keytab` +
`kerberos_principal`) is the unattended-production path: gssapi acquires the TGT
from the keytab on each downstream tool call, so no external renewer is required.
The spike script `scripts/spnego_spike.py` is the starting point for manual
verification.

## Offline collector (`cdp-collect`, implemented)

For CDP clusters an LLM client cannot reach directly (air-gapped/restricted
client networks). No LLM, no FastMCP/MCP transport: `src/cdp_mcp/collector/`
imports only `cm_pool`/`clients`/`cm_client`/`registry`/`config` — **never**
`server.py` — so the `mcp` package is never loaded at runtime (confirmed:
`import cdp_mcp.collector.collect` leaves `sys.modules` with no `mcp.*`
entries). Entry point: `cdp-collect` (`cdp_mcp.collector.collect:main`).
Runs inside the client's network (or a jump host with a tunnel to CM), and
its `--out` directory is what physically crosses the air-gap afterwards.

- **Full resolution, not the MCP-capped tools.** Uses
  `get_service_metrics_raw`/`get_host_metrics_raw` (see `cm_client.py`),
  which skip `MAX_TIMESERIES_POINTS` — that cap exists only to keep MCP tool
  results under the transport's ~1MB limit, irrelevant when writing straight
  to local files.
- **Output matches cdp-report's raw/ directory exactly** — same
  `NN_<name>.json` flat file naming, same `_manifest.json` shape (`period`/
  `cluster`/`files: [{file, tool, item_count, truncated,
  total_matched_in_range, ...}]`) — confirmed against cdp-report-curate's
  Prerequisites check, cdp-report-render's validation cross-checks, and
  `scripts/score_export_run.py`'s actual field reads, not just filenames.
  cdp-report's Phase 1.5+ tooling can point at a collector `--out` directory
  directly.
- **Resumable, and honest about failures.** `_manifest.json` tracks
  per-file completion; re-running the identical command skips entities
  already recorded. A failed call (SPNEGO challenge, permission denied,
  transient error) writes `{"status": "not_available", "reason": ...}`
  instead of nothing at all — cdp-report-curate needs "attempted and
  failed" distinguishable from "never collected" and from a genuine empty
  success (`[]`/`{}`). A `not_available` record is **not** treated as done
  on resume — it's retried, since the cause (a permission grant, a
  `kinit`, a network blip) may be fixed by the next run.
- **Timestamps normalized to UTC once, at the top of `collect_cluster`**
  (`_to_utc_z()`), before anything else uses `start`/`end` — CM's API and
  every internal chunk-boundary calculation have only been verified against
  `Z` timestamps, never trust a non-`Z` offset to be interpreted correctly
  server-side. The period label (e.g. `"August 2026"`) is computed from the
  **original**, pre-conversion string — order matters: Jakarta midnight
  Aug 1 is UTC Jul 31 17:00, so labeling from the converted instant would
  misname the period "July". Pass `--period-end` as the same day's
  `23:59:59` (inclusive), not the next day's `00:00:00`, matching
  cdp-report's own convention.
- **Resuming into a directory whose period doesn't match refuses**, rather
  than silently reusing stale bounds under a new label — `collect_cluster`
  raises `SystemExit` if `--period-start`/`--period-end` disagree with an
  existing `_manifest.json`'s `period`.
- **Host metrics are chunked, service metrics are not.** Hosts: `<=14`-day
  sub-range calls, merged (deduped by exact timestamp at chunk seams — CM's
  `/timeseries` `from`/`to` are inclusive on both ends, so adjacent chunks
  share a boundary point) into one file per host. Confirmed live: a single
  full-month call gets coarsened to CM's `SIX_HOURLY` rollup (124 points/31
  days); `<=14`-day chunks (each gets its own rollup decision) keep it at
  `HOURLY`. Services deliberately do **not** get this treatment — a finer
  range doesn't yield finer service-level rollup once the period has aged,
  matching cdp-report's own documented finding.
- **Alerts/audit/impala-queries/YARN-long-apps are chunked weekly** — a
  single call's `matched[:limit]` (alerts/audit) or hard server cap
  (impala/YARN) can't cover a full month on a busy cluster: confirmed live,
  one week of IMPORTANT-severity alerts alone matched ~10,000 events.
  Impala and YARN-long-apps additionally get one **merged** file (deduped
  by `queryId`/`app_id`) across all weeks, since cdp-report-curate reads the
  combined file, not the per-week chunks.
- **Downstream (YARN/Spark/HDFS/Oozie)** via the same
  `cm_pool.get_{yarn,spark,hdfs,oozie}_client` factories server.py's tools
  use — SPNEGO-aware, each independently optional (endpoint not discovered
  → logged and skipped, not an error).
- **`metrics_catalog.py`** — curated (data-verified for hdfs/yarn/impala/
  hive_on_tez/hue/zookeeper/solr; schema-verified only for kafka/hbase/
  phoenix — confirmed present in a real CM v51/CDH 7.1.9 metric schema and
  round-tripped through a live `/timeseries` query with zero errors, but
  not yet confirmed against a live-collecting instance of those three
  services) vs. discovery-only (nifi — this CM instance has zero metric
  definitions for it, confirmed empirically). Substring matching
  (`name_contains=`) misses a service's own core per-role metrics the same
  way host metrics like `cpu_percent` don't say "host" — finding
  kafka/hbase's real names required searching by JMX vocabulary and
  filtering by `sources` containing the actual role type, not by
  service-name substring.

**Deployment:** `scripts/build_collector_bundle.sh` packages `collector/`
plus only the dependencies it actually imports (excludes `mcp` and the
iceberg-only `impyla`/`thrift*`) as an offline-installable tarball.
`WITH_KERBEROS=true` adds `httpx-gssapi` for the downstream clients' SPNEGO
— must be built on a host matching `TARGET_PLATFORM`, since `gssapi` has no
PyPI wheel and a cross-platform build silently compiles it for the *build*
machine instead (confirmed: building for Linux from macOS produced Mach-O
binaries vendored into a Linux-labeled bundle with no warning); the script
refuses a mismatch and prints a Docker recipe instead of doing this.
Full build → deploy → collect → handoff runbook:
`docs/collector-deploy.md`.
**Python 3.8–3.10 targets are supported** (`TARGET_PYTHON=3.8` etc.) even
though the wheel's metadata says `requires-python >=3.11` (that floor is
the MCP server's, not the collector's): the build rewrites the wheel's
`Requires-Python`, vendors `eval_type_backport` for pydantic's `X | Y`
annotations on old interpreters, and — for 3.8 + Kerberos — pins
`httpx-gssapi==0.3.1` (0.4 dropped 3.8) with `httpx<0.28`. **AD-KDC
client sites should use the py3.12 Kerberos bundle** — 0.3.1 predates
httpx-gssapi 0.5's SPNEGO-mechanism-by-default and negotiates plain krb5,
which has historically misbehaved against Active Directory. Bundle names
embed the target Python (`-py3.8-`/`-py3.12-`) so versions aren't
confused at the client site. When building a
Kerberos bundle inside the Docker recipe, the gssapi C extension must
compile against a `TARGET_PYTHON` interpreter, so the container's
`python:<TARGET_PYTHON>-slim` system python is used for that step (the
script auto-detects a matching system python3; a 3.12-compiled gssapi
gets tagged cp312 and rejected).

Validated end-to-end against a real production cluster (Astra DaaS DRC,
Kerberized, reached via a SOCKS5 tunnel + in-process keytab acquisition) —
not just mocked.

## TO-DO: incremental collector
The collector is currently a **full-period re-collect** per run: every
entity in the period is fetched from scratch, and resume (`_manifest.json`)
only skips entities already recorded as a successful *full* result — it does
not reuse prior data to fetch only what's new. Re-running the same month
re-pulls the entire month; re-running an adjacent month re-pulls both in
full. On a cluster with months of history, this is bandwidth- and
time-wasteful for repeat/adjacent runs, and means a period's data can never
be "topped up" with the latest days without redoing the whole range.

Implement an **incremental mode** so a run against a period that already has
an `_manifest.json` fetches only the delta since the prior run's
`generated_at`, rather than the whole period again. Sketch of the design
(think through carefully before implementing — this is non-trivial and the
resume/period-drift guards interact with it):

- **Distinguish entity classes by how they age.** Time-series metrics
  (host/service) and record-streams (alerts/audit/impala-queries/YARN-long
  apps) are append-only in CM and naturally incremental — a `from`/`to` of
  `[last_run_generated_at, period_end]` re-fetches just the new points/
  records, then the new slice is merged into the existing file (deduped by
  timestamp / `queryId` / `app_id`, reusing the seam-dedup and impala/YARN
  merge logic already in `collect.py`). Snapshot entities (host status,
  roles, services, parcels, security info, namenode status, oozie jobs,
  replication schedules) are *current-state* reads with no time dimension —
  these can't be incrementally merged; a repeat run should just re-fetch the
  snapshot (cheap) or skip it if unchanged, but they're small so re-fetch is
  fine.
- **Manifest gains a `generated_at` + per-entity `last_collected_at`** so an
  incremental run knows the watermark without a separate state file. The
  existing `generated_at` at the top level is a start; per-entity timestamps
  let one entity be re-fetched without forcing all others to be.
- **Period drift still refuses, but narrower.** Today a different
  `--period-*` against an existing dir is a hard error. Incremental mode
  relaxes this for the *same* period (a top-up) but must still reject a
  *different* period — don't let a "July top-up" silently write August data.
  Consider a `--incremental`/`--top-up` flag to make the intent explicit
  rather than inferring it from the manifest's existence (full re-collect
  into an existing dir is a legitimate operation today and shouldn't become
  implicit).
- **Chunk-boundary interaction.** Host metrics are fetched in ≤14-day
  sub-ranges and merged; an incremental slice near `period_end` is typically
  one small chunk — fine — but a top-up spanning a chunk seam must dedupe
  against the existing merged file's exact timestamps, same as today's
  inter-chunk seam dedup. Alerts/audit are weekly-chunked; an incremental
  slice that falls mid-week needs to re-fetch that whole week's bucket
  (CM has no sub-week `from`/`to` for the `matched[:limit]` paging path)
  and dedupe — the weekly granularity is a server-side constraint, not a
  choice.
- **`not_available` records still retried.** A prior failed entity is still
  retried on the next run regardless of incremental mode — the cause may be
  fixed by then (existing behaviour, keep it).
- **Reporting caveat.** A period that's been topped up incrementally has a
  `generated_at` newer than some of its data; cdp-report-curate/render and
  `score_export_run.py` read counts/period, not freshness, so verify they
  don't key off `generated_at` as a "this is the whole period" signal before
  shipping.

Not yet scoped or designed in detail — this is a placeholder for the work.
The full-period path stays the default; incremental is an opt-in mode.

## Future language note
The PoC is Python + FastMCP. If validated, consider rewriting in Go for distribution as a static binary.