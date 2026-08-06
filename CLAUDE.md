# CLAUDE.md — cdp-mcp-server

Python MCP server for Cloudera/CDP cluster administration and troubleshooting.
Fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — Apache 2.0.

---

## General directives

### Python and tooling
- Use **poetry** for virtualenv and dependency management (NOT uv, NOT direct pip).
- The virtualenv lives in `.venv/` (in-project). Create it with `python3.12 -m venv .venv` then `poetry env use .venv/bin/python`.
- To install: `.venv/bin/pip install -e ".[dev]"` (pyproject.toml uses poetry-core as the build backend).
- To run commands in the venv: `.venv/bin/python`, `.venv/bin/pytest`, `.venv/bin/ruff`, etc.

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

# Python 3.12 virtualenv
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# Config
cp .env.example .env
cp cm_instances.yaml.example cm_instances.yaml
# Edit cm_instances.yaml with your test cluster credentials

# Start (FileRegistry)
REGISTRY_BACKEND=file cdp-mcp

# Quick start (EnvRegistry — single CM)
REGISTRY_BACKEND=env CM_HOST=cm.example.com CM_USERNAME=admin CM_PASSWORD=changeme CM_USE_TLS=false cdp-mcp
```

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
    └── oozie_client.py  ← Oozie REST API (:11000)

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
   smaller than `full`; use `full` only for per-record detail.
2. **Client-side pagination where the server has no time filter.** When CM
   has no `from`/`to` (e.g. `/replications/{id}/history`), page by offset with
   a client-side cutoff on each record's timestamp (history is newest-first).
3. **Aggregation for wide windows.** For a report spanning a wide time range
   (e.g. one month), aggregate counters page-by-page and return totals, never
   accumulate raw records — `get_replication_metrics` is the model.

Return a structured envelope (`{items, count, truncated, ...,
effective_range}`) consistent with `get_alerts`/`get_audit_events`/
`get_service_logs`/`get_replication_*`, so the caller can tell "nothing
matched" from "we stopped looking". Never return a bare unbounded list.

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
        "CM_USE_TLS": "false"
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

## Kerberos (out of scope for MVP)
For CDP clusters with Kerberos/SPNEGO use `httpx-gssapi`. To be implemented as a `CM_KERBEROS=true` option.

### TODO — SPNEGO for downstream clients
`cm_client.py` uses Basic auth against CM and works. The clients in `clients/` (YARN RM,
Spark HS, HDFS NameNode JMX, Oozie) receive no credentials: on Kerberized clusters
unauthenticated requests return a non-JSON response (an HTML negotiation page or an
empty body), which today surfaces as a generic JSON parse error.

- Library: `httpx-gssapi` (implements `httpx.Auth`), to be attached only to the
  four downstream clients — not to `cm_client.py`.
- Credentials from a keytab, not interactive `kinit` (long-running server process):
  `KRB5_CLIENT_KTNAME=/path/to/svc.keytab` + a dedicated principal, so gssapi
  auto-renews the ticket.
- Main trade-off: system library dependency (MIT `libkrb5-dev` at build time,
  `krb5-libs` at runtime) → heavier Docker image.
- Injection point: `cm_pool.py` (where `YarnClient`, `SparkClient`, `HdfsClient`,
  `OozieClient` are instantiated) + new `CM_KERBEROS=true` flag in `config.py`.
- Do not implement without explicit discussion (see architectural rule #4).

## Future language note
The PoC is Python + FastMCP. If validated, consider rewriting in Go for distribution as a static binary.