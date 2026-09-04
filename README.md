# cdp-mcp-server

MCP (Model Context Protocol) server for Cloudera Manager / CDP cluster administration and troubleshooting.

Based on [Davide Isoardi's cdp-mcp-server](https://github.com/disoardi/cdp-mcp-server), itself a fork of [dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server) — Apache 2.0. Doddy Sebastianus is extending the functionality.

The codebase builds on the original fork with pluggable registry backends (File, Env, Iceberg), additional service clients (YARN, Spark History Server, HDFS NameNode, Oozie), role/metric/security/replication/Impala monitoring tools, SOCKS5 proxy support, optional Kerberos/SPNEGO auth for the downstream service UIs, and clean SPNEGO detection with per-cluster short-circuiting.

## Quick Start (general purpose — no Iceberg required)

### Option 1: FileRegistry (recommended for development)

```bash
# Install
git clone https://github.com/doddys/cdp-mcp-server.git
cd cdp-mcp-server
uv venv --python 3.12
uv sync --extra dev

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
  CM_USE_TLS=true \
  cdp-mcp
```

Additional env vars (all optional): `CM_ENVIRONMENT`, `CM_VERIFY_SSL`, `CM_API_VERSION`,
`CM_TIMEOUT_SECONDS` (CM core API calls, default 30), `CM_DOWNSTREAM_TIMEOUT_SECONDS`
(YARN/Spark/HDFS/Oozie calls, default 30), `CM_DISABLE_ON_SPNEGO` (default `true`),
`CM_KERBEROS` (default `false` — enable SPNEGO for the downstream clients; see
[Kerberos / SPNEGO](#kerberos--spnego) below). The `file` backend exposes the same
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
dependency — `uv sync --extra dev` pulls in `socksio` automatically).

For the full jump-host + Kerberos setup (SSH dynamic port forward, `kinit`/keytab,
TGT renewal, end-to-end recipe), see
[docs/kerberos-tunneling.md](docs/kerberos-tunneling.md).

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
        "CM_USE_TLS": "true"
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
- `list_roles` / `get_role_status` — Role-level status listing for a service, or detailed status for one role
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
- `list_replication_schedules` / `get_replication_history` / `get_replication_metrics` — Replication discovery, paginated run history, and aggregated monthly metrics (bounded to stay under the tool-result cap; see [docs/tools.md](docs/tools.md))
- `list_parcels` — Parcel (CDH/runtime distribution) version and activation status
- `get_audit_events` — CM audit log
- `list_datahubs` — Enumerate DataHub clusters
- `refresh_cluster_map` — Rebuild cluster→CM mapping
- `get_mgmt_service` — CM Management Service health (Host Monitor, Service Monitor, etc.)
- `delete_service` / `delete_role` — Remove a stale service/role — **irreversible**
- `get_hdfs_snapshots` — List snapshots of an HDFS directory via WebHDFS (read-only; HA failover to the active NameNode)

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

## Offline collector (`cdp-collect`)

For clusters an AI assistant **cannot reach** directly — air-gapped networks,
restricted client networks, or CM behind a jump host with no MCP route out —
`cdp-collect` is a standalone, LLM-free collector that calls Cloudera
Manager's REST API directly (plus YARN/Spark/HDFS/Oozie where installed) and
writes **full-resolution** JSON to local files. It skips the
`MAX_TIMESERIES_POINTS` cap that exists only to keep MCP tool results under
the transport's ~1 MB limit — irrelevant when writing to disk. The output
matches cdp-report's interactive export exactly (`NN_<name>.json` files +
`_manifest.json` with per-file sha256), so cdp-report's curate/render phases
can run against the `--out` directory directly, with no conversion step.

It's a separate entry point (`cdp-collect`, imports only `cm_pool`/`clients`/
`registry`/`config` — never `server.py`/`mcp`), resumable per entity, and
honest about failures: a failed call writes `{"status": "not_available"}`
(retried on resume) rather than nothing at all.

```bash
# Run directly from a checkout (dev/smoke)
REGISTRY_BACKEND=file .venv/bin/cdp-collect --cluster "my-cluster" \
  --period-start 2026-08-01T00:00:00+07:00 \
  --period-end   2026-08-31T23:59:59+07:00 --out output/my-cluster_202608/
```

For a client site with no internet/PyPI, `scripts/build_collector_bundle.sh`
packages the collector plus only its runtime deps (excludes `mcp` and the
Iceberg-only `impyla`/`thrift*`) as an offline-installable tarball — only a
matching system `python3` is needed at the site. Bundle names embed the
target Python (`-py3.8-` / `-py3.12-`) so versions aren't confused; Kerberos
bundles (`WITH_KERBEROS=true`) vendor `httpx-gssapi` for SPNEGO and must be
built on a host matching `TARGET_PLATFORM` (gssapi has no prebuilt wheel).

Full build → deploy → collect → handoff runbook: **[docs/collector-deploy.md](docs/collector-deploy.md)**.

## Kerberos / SPNEGO

On Kerberized clusters the YARN/Spark/HDFS/Oozie service UIs challenge with
SPNEGO. cdp-mcp can attach SPNEGO auth to those four downstream clients so the tools
work against Kerberized clusters. It is **opt-in** and reads credentials from one of
two sources: the **default Kerberos credentials cache** (a `kinit` TGT, or a keytab
you've loaded into the ccache), or **in-process keytab acquisition** (`kerberos_keytab`
+ `kerberos_principal`), where gssapi acquires the TGT from the keytab on each call so
no external `kinit`/renewer is needed.

> CM itself always uses Basic auth — `kerberos` affects only the four downstream
> service clients, never the CM API client.

### Enabling SPNEGO

1. Install the optional extra (needs MIT krb5 at build/runtime — on macOS:
   `brew install krb5` then build with
   `PKG_CONFIG_PATH=/opt/homebrew/opt/krb5/lib/pkgconfig`). Use `uv sync` so it
   reads the kerberos extra from `uv.lock` (a lockless `uv pip install` can
   resolve to an incompatible major `mcp`):
   ```bash
   uv sync --extra kerberos   # pulls in httpx-gssapi
   ```
2. Obtain credentials — either populate the default ccache:
   ```bash
   kinit <user>@<REALM>
   ```
   or, for an unattended/daemon deployment, set `kerberos_keytab` +
   `kerberos_principal` on the instance (`CM_KERBEROS_KEYTAB` /
   `CM_KERBEROS_PRINCIPAL` env) so gssapi acquires the TGT from the keytab
   in-process (no `kinit`, no renewal cron).
3. Enable the flag per CM instance — env (`CM_KERBEROS=true`) or yaml (`kerberos: true`):
   ```bash
   ALL_PROXY=socks5h://127.0.0.1:7890 \
   REGISTRY_BACKEND=env CM_HOST=... CM_USERNAME=... CM_PASSWORD=... \
     CM_KERBEROS=true cdp-mcp
   ```

When `kerberos=true`, the downstream clients are built with an `HTTPSPNEGOAuth`
(via `clients/spnego.py`, lazy-imported so the base install stays krb5-free). If the
extra is missing or no TGT is in the cache, the tool returns a structured
`spnego_config_error` with an actionable message instead of a traceback.

### When SPNEGO is NOT enabled

If you leave `kerberos=false` on a Kerberized cluster, calling a downstream tool hits
a `401 Negotiate` challenge; cdp-mcp detects this cleanly and returns a structured
`spnego_required` error instead of a cryptic parse failure. By default
(`disable_on_spnego: true` / `CM_DISABLE_ON_SPNEGO=true`), once a service is found to
require SPNEGO on a cluster, cdp-mcp remembers this and short-circuits further calls
to that tool immediately — no repeated round-trips or timeouts. Set it to `false` to
always attempt the call fresh (useful while testing a Kerberos setup).

### Notes
- Run `get_cluster_security_info` first on an unfamiliar cluster to check its
  TLS/Kerberos status upfront.
- **Full tunneling setup** (SOCKS via SSH dynamic port forward + `kinit`/keytab
  credentials + TGT renewal) is documented in
  [docs/kerberos-tunneling.md](docs/kerberos-tunneling.md).
- CM-API-based tools (`list_impala_queries`, `get_service_metrics`, `get_service_logs`,
  etc.) work regardless of Kerberos, since they authenticate against CM itself, not the
  downstream service UIs.
- **Long-running server caveat:** when using the default ccache, a daemon process
  needs TGT renewal (cron `kinit -R`, or a keytab loaded into the ccache externally).
  To avoid that entirely, use in-process keytab acquisition (`kerberos_keytab` +
  `kerberos_principal`) — gssapi re-acquires the TGT from the keytab on each downstream
  call, so no external renewer is required. See [docs/kerberos-tunneling.md](docs/kerberos-tunneling.md).

## License

Apache License 2.0 — see [LICENSE](LICENSE).

- Original work © Davide Vergari ([dvergari/cloudera-mcp-server](https://github.com/dvergari/cloudera-mcp-server))
- Fork & extensions © Davide Isoardi ([disoardi/cdp-mcp-server](https://github.com/disoardi/cdp-mcp-server))
- Further extensions © Doddy Sebastianus
