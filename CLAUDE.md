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
└── clients/
    ├── yarn_client.py   ← YARN ResourceManager REST API (:8088)
    ├── spark_client.py  ← Spark History Server REST API (:18088)
    ├── hdfs_client.py   ← HDFS NameNode JMX (:9870)
    ├── oozie_client.py  ← Oozie REST API (:11000)
    ├── errors.py        ← shared SpnegoRequiredError / SpnegoConfigError
    └── spnego.py        ← lazy httpx-gssapi SPNEGO auth factory (optional [kerberos] extra)

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
   offer a `sample_mode` so the caller picks the trade-off: `"even"` spreads
   samples across the full requested range (trend queries), `"recent"` keeps
   full resolution for only the most recent slice (incident response).
   Confirmed live: an uncapped `get_host_metrics` call returned ~17MB, slow
   enough end-to-end (through the deployment's masking proxy) that the
   downstream MCP client gave up and dropped the connection. Every point is
   also slimmed to `timestamp`/`value`/`type` by default, stripping CM's
   verbose per-point `aggregateStatistics` block (min/max/mean/stdDev/count/
   sampleTime/minTime/maxTime, ~5x the bytes of a bare point) — the dominant
   size driver even *within* the point cap on real report-generator traffic.
   This is opt-out, not silent data loss: pass `include_aggregate_stats=True`
   when a caller specifically needs min/max/mean (e.g. a report chart).
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

## Future language note
The PoC is Python + FastMCP. If validated, consider rewriting in Go for distribution as a static binary.