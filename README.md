# cdp-mcp-server

MCP (Model Context Protocol) server for Cloudera Manager / CDP cluster administration and troubleshooting.

Based on [Davide Isoardi's cdp-mcp-server](https://github.com/disoardi/cdp-mcp-server), itself a fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — Apache 2.0. Doddy Sebastianus is extending the functionality.

The codebase builds on the original fork with pluggable registry backends (File, Env, Iceberg), additional service clients (YARN, Spark History Server, HDFS NameNode, Oozie), role/metric/security/replication/Impala monitoring tools, SOCKS5 proxy support, and clean SPNEGO detection with per-cluster short-circuiting.

## Quick Start (general purpose — no Iceberg required)

### Option 1: FileRegistry (recommended for development)

```bash
# Install
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .

# Configure
cp cm_instances.yaml.example cm_instances.yaml
# Edit cm_instances.yaml with your CM credentials

# Run
REGISTRY_BACKEND=file cdp-mcp
```

### Option 2: EnvRegistry (single CM, zero config)

```bash
REGISTRY_BACKEND=env \
  CM_HOST=cm.example.com \
  CM_USERNAME=admin \
  CM_PASSWORD=changeme \
  CM_USE_TLS=false \
  cdp-mcp
```

Additional env vars (all optional): `CM_ENVIRONMENT`, `CM_VERIFY_SSL`, `CM_API_VERSION`,
`CM_TIMEOUT_SECONDS` (CM core API calls, default 30), `CM_DOWNSTREAM_TIMEOUT_SECONDS`
(YARN/Spark/HDFS/Oozie calls, default 30), `CM_DISABLE_ON_SPNEGO` (default `true` —
see [Kerberos / SPNEGO](#kerberos--spnego) below). The `file` backend exposes the same
fields per-instance in `cm_instances.yaml` (see `cm_instances.yaml.example`).

### Connecting through a SOCKS5 proxy / SSH tunnel

If your CM instance is only reachable through a jump host, set `ALL_PROXY` (or
`HTTPS_PROXY`) before starting the server — every HTTP client in cdp-mcp honors it
automatically (`httpx`'s `trust_env` is on by default):

```bash
ALL_PROXY=socks5h://127.0.0.1:7890 REGISTRY_BACKEND=file cdp-mcp
```

`socks5h://` resolves DNS through the proxy, which is usually what you want for
internal-only hostnames. This requires the `httpx[socks]` extra (already a declared
dependency — `pip install -e .` / `poetry install` pulls in `socksio` automatically).

## Claude Desktop Configuration

### FileRegistry
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

### EnvRegistry (single CM)
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

## Available Tools

Full descriptions and diagnostic-workflow recipes (e.g. "job failed, why?", "SPNEGO
error, what now?") are in [docs/tools.md](docs/tools.md). Summary below.

### Cloudera Manager
- `list_clusters` — List all managed DataHub clusters
- `list_services` — List services on a cluster
- `get_service` — Single-service detail (health, config staleness)
- `list_roles` / `get_role_status` — Role-level status for a service, or one role in detail
- `get_service_logs` — Extract recent log lines for one role or a service's roles (see note below)
- `get_alerts` — Get cluster alerts and events
- `get_service_metrics` / `get_host_metrics` — Time-series metrics via tsquery, per-service or per-host
- `list_available_metrics` — Discover metric names for the two tools above
- `get_config` / `update_config` — Read and write service configuration
- `list_impala_queries` — Impala query monitoring via CM (no SPNEGO required)
- `run_service_command` / `get_command_status` — Execute and poll async CM commands
- `list_cluster_commands` — Recent command history for a cluster
- `get_host_status` — Host health and role inventory
- `get_cluster_security_info` — TLS/Kerberos status for a cluster
- `get_cluster_utilization` — Aggregated CPU/memory utilization report
- `list_replication_schedules` / `get_replication_history` — Replication job status and run history
- `list_parcels` — Parcel (CDH/runtime distribution) version and activation status
- `get_audit_events` — CM audit log
- `list_datahubs` — Enumerate DataHub clusters
- `refresh_cluster_map` — Rebuild cluster→CM mapping
- `get_mgmt_service` — CM Management Service health (Host Monitor, Service Monitor, etc.)
- `delete_service` / `delete_role` — Remove a stale service/role — **irreversible**

> **`get_service_logs` note:** the CM `/logs/full` endpoint has no server-side line
> limit — it always returns the complete log file (can be tens of MB), which cdp-mcp
> then truncates client-side to `max_lines`. On services with many roles (e.g. HDFS
> with many DataNodes), an unfiltered call is capped at `max_roles` (default 10) to
> avoid a slow, multi-role full-log sweep. Pass `role_name` (from `list_roles()`) to
> target one role directly — the fast, predictable path.

### Registry Management
- `registry_list` — List registered CM instances
- `registry_stats` — Registry statistics
- `registry_add` — Register a new CM instance
- `registry_deactivate` — Deactivate a CM instance
- `registry_update_field` — Update a registry field
- `registry_reload` — Hot-reload registry

### YARN ResourceManager
- `get_yarn_app` — Get YARN application details and diagnostics
- `list_yarn_apps` — List YARN applications with filters
- `get_yarn_queue` — Get YARN scheduler queue capacity/usage

### Spark History Server
- `get_spark_app` — Get Spark application summary
- `list_spark_apps` — List Spark applications
- `get_spark_stages` — Get stage details (useful for debugging slow jobs)

### HDFS NameNode
- `get_namenode_status` — NameNode health, capacity, corrupt/missing blocks

### Oozie
- `get_oozie_job` — Get workflow or coordinator job details
- `list_oozie_jobs` — List Oozie jobs with filters

YARN/Spark/HDFS/Oozie endpoints are auto-discovered from CM at startup and connected
to over HTTPS automatically when the role's config reports a TLS port (see
`cm_pool.py`); no manual scheme/port configuration is needed for TLS-enabled clusters.

## Registry Backends

| Backend | Use case | Requires |
|---------|----------|---------|
| `file` | Development, small teams | `cm_instances.yaml` |
| `env` | Single CM, quick tests | env vars `CM_HOST`, `CM_USERNAME`, `CM_PASSWORD` |
| `iceberg` | Production CDP environments | Impala/HiveServer2 + Iceberg table |

## Kerberos / SPNEGO

Full Kerberos/SPNEGO auth for the YARN/Spark/HDFS/Oozie service UIs is **not yet
implemented** (tracked in `CLAUDE.md`). On a Kerberized cluster, calling one of those
tools will hit a `401 Negotiate` challenge; cdp-mcp detects this cleanly and returns a
structured `spnego_required` error instead of a cryptic parse failure.

By default (`disable_on_spnego: true` / `CM_DISABLE_ON_SPNEGO=true`), once a
service is found to require SPNEGO on a given cluster, cdp-mcp remembers this and
short-circuits further calls to that tool immediately — no repeated network
round-trips or timeouts. Set it to `false` to always attempt the call fresh (useful
while testing a Kerberos setup).

Run `get_cluster_security_info` first on an unfamiliar cluster to check its TLS/Kerberos
status upfront. CM-API-based tools (`list_impala_queries`, `get_service_metrics`,
`get_service_logs`, etc.) work regardless, since they authenticate against CM itself,
not the downstream service UIs.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

- Original work © Davide Vergari ([dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server))
- Fork & extensions © Davide Isoardi ([disoardi/cdp-mcp-server](https://github.com/disoardi/cdp-mcp-server))
- Further extensions © Doddy Sebastianus
