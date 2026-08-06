# Available Tools

## Cloudera Manager Tools

| Tool | Description |
|---|---|
| `list_clusters` | List all managed clusters (name, version, health) |
| `list_services` | List services on a cluster |
| `get_service` | Single-service detail: healthSummary, healthChecks, serviceState, config staleness |
| `list_roles` | Lightweight role status listing for a service (healthSummary, roleState, commissionState) |
| `get_role_status` | Detailed status for a single role instance |
| `get_service_logs` | Extract service logs with time range filtering |
| `get_alerts` | Get cluster alerts and events — paginates internally to cover the full requested time range, see note below |
| `get_service_metrics` | Time-series metrics via tsquery |
| `get_host_metrics` | Time-series metrics for a single host (CPU, memory, disk, network) |
| `list_available_metrics` | Discover metric names/descriptions for use with `get_service_metrics`/`get_host_metrics` — pass `cluster_name` in multi-CM registries to scope to the right instance |
| `get_config` | Read service configuration |
| `update_config` | Write service configuration |
| `list_impala_queries` | List Impala queries via CM's own query monitoring (no SPNEGO required) — `service_name` required, see `list_services()` |
| `run_service_command` | Execute async CM command (restart, start, stop, deploy config, etc.) |
| `get_command_status` | Poll async command status |
| `list_cluster_commands` | List recent commands executed against a cluster with success/failure status |
| `get_host_status` | Host health, roles, rack info |
| `get_cluster_security_info` | TLS and Kerberos status for a cluster — check before calling YARN/Spark/HDFS/Oozie tools |
| `get_cluster_utilization` | Aggregated CPU/memory utilization report (capacity planning) — auto-chunks ranges over 29 days |
| `list_replication_schedules` | Replication schedules for a service with last-run status — `service_name` required; embeds recent `history` inline |
| `get_replication_history` | Run history for a replication schedule |
| `list_parcels` | Parcel (CDH/runtime distribution) version and activation status per host |
| `get_audit_events` | CM audit log (login, config changes, command executions) — paginates internally, see note below |
| `list_datahubs` | Enumerate DataHub clusters |
| `refresh_cluster_map` | Rebuild cluster→CM mapping after failover or new cluster |
| `get_mgmt_service` | CM Management Service health and role status (Host Monitor, Service Monitor, Alert Publisher, etc.) |
| `delete_service` | Delete a stale/orphaned service from a cluster — **irreversible** |
| `delete_role` | Delete a stale/decommissioned role instance — **irreversible** |

!!! warning "Destructive operations"
    `delete_service` and `delete_role` call `DELETE` on the CM API and cannot be undone.
    The target must be in **stopped** state before deletion — CM will return an error otherwise.
    Use `run_service_command` with `command="stop"` first if needed.

!!! note "Time-range response metadata"
    `get_alerts`, `get_audit_events`, `get_service_metrics`, `get_host_metrics`,
    `get_cluster_utilization`, and `list_impala_queries` all return an object (not a
    bare list) with metadata alongside `items`:

    - `time_range_defaulted` — `true` if `start_time`/`end_time` were omitted; they
      silently default to the last hour, so check `effective_range` before assuming a
      call covered more than that.
    - `truncated` (`get_alerts`/`get_audit_events` only) — `true` means `max_scan`
      raw events were fetched before the full requested range was actually scanned.
      On an active cluster this is common with wide ranges: `limit` caps the
      *returned* count, but the tool now pages internally (via `resultOffset`) until
      either the range is fully covered or `max_scan` is hit — `truncated` tells you
      which one happened, since a non-empty result otherwise looks identical either way.
    - `chunked`/`num_chunks` (`get_cluster_utilization` only) — CM rejects any single
      request spanning more than 30 days; ranges over 29 days are split into multiple
      calls and merged automatically. This field just tells you it happened.

## Registry Tools

| Tool | Description |
|---|---|
| `registry_list` | List registered CM instances (passwords excluded) |
| `registry_stats` | Statistics: total, active/inactive count, by environment |
| `registry_add` | Register a new CM instance at runtime |
| `registry_deactivate` | Soft-delete a CM instance (keeps it in registry) |
| `registry_update_field` | Update a single field (e.g. password, port) |
| `registry_reload` | Hot-reload registry from YAML/Iceberg without restart |

## YARN Tools

| Tool | Description |
|---|---|
| `get_yarn_app` | Application details, diagnostics, resource usage and timing |
| `list_yarn_apps` | List applications filtered by state / queue / user |
| `get_yarn_queue` | Scheduler queue capacity and active/pending applications |

## Spark History Server Tools

| Tool | Description |
|---|---|
| `get_spark_app` | Spark application summary (duration, executor time, attempt count) |
| `get_spark_stages` | Stage details including failure reason (truncated to 300 chars) |
| `list_spark_apps` | List Spark applications filtered by status |

## HDFS Tools

| Tool | Description |
|---|---|
| `get_namenode_status` | NameNode health (HEALTHY / DEGRADED / CRITICAL), capacity, corrupt/missing blocks, HA state |

## Oozie Tools

| Tool | Description |
|---|---|
| `get_oozie_job` | Workflow or coordinator job details with action list and YARN app IDs |
| `list_oozie_jobs` | List jobs filtered by status / type / user |

---

## Diagnostic Workflows

### Job failed — why?

1. `list_yarn_apps` with `state=FAILED` → find the `app_id`
2. `get_yarn_app` → read `diagnostics` field
3. `get_spark_stages` with `status=FAILED` → find `failureReason`
4. `get_service_logs` → deep dive into YARN / Spark logs

### HDFS issues?

1. `get_namenode_status` → check `health_summary`, `corrupt_blocks`, `missing_blocks`
2. `get_alerts` with `severity=CRITICAL` → related alerts
3. `get_service_logs` for HDFS → NameNode log details

### Resource contention?

1. `get_yarn_queue` → check `used_capacity` vs `capacity`
2. `list_yarn_apps` with `state=RUNNING` → who is consuming resources
3. `get_service_metrics` → trend over time

### YARN/Spark/HDFS/Oozie tool returns a non-JSON / parse error?

1. `get_cluster_security_info` → check `kerberos.kerberized`
2. If `true`, the cluster requires SPNEGO for these service UIs, which is not
   yet implemented (see `CLAUDE.md` roadmap) — use `list_impala_queries` /
   `get_service_metrics` / `get_service_logs` (all CM-API-based, no SPNEGO
   needed) for equivalent visibility where possible.

### CM internal health?

1. `get_mgmt_service` → Host Monitor, Service Monitor, Alert Publisher status
2. `get_alerts` → any unacknowledged critical alerts
3. `get_host_status` → per-host health and role assignment

### Cleanup — remove stale service or role?

1. `list_services` → confirm the service name
2. `run_service_command` with `command="stop"` → stop the service first
3. `delete_service` → remove it from CM

For a single stale role (e.g. orphaned HiveServer2):

1. `list_services` → identify the service
2. Stop the specific role via `run_service_command` or CM UI
3. `delete_role` with the full role name (e.g. `hive-HIVESERVER2-abc123def456`)
