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
| `get_service_metrics` | Time-series metrics via tsquery — series over 2000 points are capped client-side, see `sample_mode` note below |
| `get_host_metrics` | Time-series metrics for a single host (CPU, memory, disk, network) — series over 2000 points are capped client-side, see `sample_mode` note below |
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
| `list_replication_schedules` | Replication schedules for a service — `service_name` required; **lightweight discovery** (capped to `maxCommands=1` + `view=summary`) so it stays under the tool-result cap; returns `{items, count, truncated, applied_limits}` |
| `get_replication_history` | Paginated run history for one schedule over a time window — CM has no server-side time filter, so the window is applied client-side while paging by offset; `view=summary` carries the per-run counters at ~10–40× smaller than `full` |
| `get_replication_metrics` | **Aggregated** replication metrics over a time window (the monthly-report tool) — discovers schedules and accumulates per-run counters (bytes/files copied, files failed, tables processed, errors, avg duration) into per-schedule totals + a capped failed-run list; stays well under the cap even for an hourly schedule over a month |
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
    `get_cluster_utilization`, `list_impala_queries`, `get_replication_history`,
    and `get_replication_metrics` all return an object (not a bare list) with
    metadata alongside `items`:

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
    - `truncated` (replication) — on `get_replication_history`, `true` means `max_scan`
      was hit before the window was fully scanned; on `get_replication_metrics`,
      per-schedule `truncated` means `max_runs_per_schedule` was hit mid-window, and
      top-level `schedule_truncated` means `max_schedules` was hit (more schedules
      exist). Raise the cap or narrow the range. `get_replication_history` also
      returns `total_in_range` (true match count, independent of `limit`).

!!! note "`get_service_metrics` / `get_host_metrics` — point cap and `sample_mode`"
    CM's `/timeseries` has no server-side point cap — a wide range × several
    `metric_names` can return tens of thousands of raw points and a multi-MB
    response large enough to break the MCP connection. Every series is capped
    client-side to **2000 points**, in one of two caller-selectable ways via
    `sample_mode`:

    - `"even"` (default) — the full range is partitioned into 2000 contiguous
      buckets and each is **merged**, not decimated: every point's value is a
      mean over its bucket (never an arbitrary raw sample that happened to
      land on a kept index — picking one point and discarding the rest would
      silently lose every spike/dip in between, which is a real accuracy
      problem for a report, not just a size one). Use this for trend/capacity
      queries over a wide window (e.g. a monthly report) — resolution drops,
      but the whole period is represented and every value is a genuine
      summary of its window. For 2000 buckets over 30 days, expect roughly
      one bucket every ~22 minutes regardless of the metric's native
      sampling rate.
    - `"recent"` — full native-resolution samples for only the **most
      recent** slice; older data is dropped entirely. Use this for "what's
      happening right now" incident response, not for period-spanning
      reports.

    The cap is **per series**, not per response — requesting many
    `metric_names` (or querying a service with many role instances) multiplies
    total series count, so group related metrics into a few calls rather than
    one call with a huge `metric_names` list.

    Check before treating a series as complete: top-level `truncated` (bool)
    and `_truncated` (human-readable note) are set when any series was capped;
    per-series, capped entries carry `data_downsampled` (true under `"even"`),
    `data_truncated` (true under `"recent"`), and `data_points_available` (the
    original, pre-cap point count).

    Every point is also stripped to `timestamp`/`value`/`type` by default,
    dropping CM's per-point `aggregateStatistics` block (min/max/mean/stdDev/
    count/sampleTime/minTime/maxTime) — confirmed live as ~5x the bytes per
    point and the dominant driver of oversized responses even *within* the
    point cap. Pass `include_aggregate_stats=True` to keep it for calls that
    specifically need min/max/mean (e.g. a report chart) — don't assume it's
    simply unavailable; it's opt-in, not gone.

    When `"even"`-mode bucketing merges multiple points together,
    `include_aggregate_stats=True` gets a **synthesized** `aggregateStatistics`
    block per bucket (min/max/mean/count/stdDev/minTime/maxTime over every
    point the bucket collapsed) — computed via an exact pooled-variance merge
    that's correct whether or not CM's own points already carried their own
    `aggregateStatistics` (fresh/RAW-granularity points never do, since a raw
    sample has nothing to aggregate over; the merge treats those as trivial
    one-sample subgroups). The series carries `aggregate_stats_synthesized:
    true` whenever this happened, so it's never mistaken for a value CM
    itself returned. `aggregateStatistics` presence/absence — synthesized or
    native — is never a signal of CM's own rollup granularity (RAW/
    TEN_MINUTELY/HOURLY/SIX_HOURLY/DAILY, which CM ages a series through
    independent of anything this parameter does) — check
    `timeSeries[].metadata.rollupUsed` in the response for that.

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
| `list_yarn_apps` | List applications filtered by state / queue / user / started / finished time / duration — time-range and duration filters are always re-applied client-side, since the ResourceManager doesn't reliably honor `startedTimeBegin`/`startedTimeEnd`/`finishedTimeBegin`/`finishedTimeEnd` on every RM version (confirmed live: non-overlapping week-long requests returned identical, unfiltered results) |
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
| `get_hdfs_snapshots` | List snapshots of a directory (name + creation time, owner, group, permission) via WebHDFS `<path>/.snapshot` — read-only; the directory must be snapshottable (`hdfs dfs -allowSnapshot <path>`). On HA clusters it fails over across the discovered NameNodes to the active one (a standby rejects WebHDFS reads with `StandbyException`) |

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
2. If `true`, the cluster requires SPNEGO for these service UIs. Enable it per
   CM instance — env (`CM_KERBEROS=true`) or yaml (`kerberos: true`) — and obtain
   credentials (a `kinit` TGT, or in-process keytab via `kerberos_keytab` +
   `kerberos_principal`). See [docs/kerberos-tunneling.md](kerberos-tunneling.md)
   for the full SOCKS + credentials setup.
3. If you can't enable SPNEGO yet, use `list_impala_queries` /
   `get_service_metrics` / `get_service_logs` (all CM-API-based, no SPNEGO
   needed) for equivalent visibility where possible.

### Metrics — monthly report / trend over a period?

1. `get_service_metrics` / `get_host_metrics` with `start_time`/`end_time`
   spanning the full period and **`sample_mode="even"`** (pass it explicitly
   — don't rely on the default in case it changes) → each returned point is
   a merged bucket mean spanning the whole range, suitable for a trend chart.
   Add `include_aggregate_stats=True` if the report needs min/max (e.g. a
   band around the trend line, or "peak CPU this week") — those are exact
   merged values, not raw CM samples, and the series carries
   `aggregate_stats_synthesized: true` when so.
2. Check `truncated`/`data_points_available` on the response — if truncated,
   footnote the chart with the effective resolution (2000 buckets over the
   period) rather than presenting it as native-resolution data.
3. To zoom into a specific incident found in the overview, issue a second,
   narrow-range call for just that window (a day or a few hours) — native
   resolution is unlikely to exceed the cap at that scale, so either
   `sample_mode` works and no merging happens.

### Replication — monthly report / which jobs failed this month?

1. `get_replication_metrics` with `start_time` ~30 days ago (no `schedule_id`)
   → per-schedule totals: `runs`, `succeeded`/`failed`, `total_bytes_copied`,
   `total_files_failed`, `total_errors`, `avg_duration_seconds`, plus the
   `failures[]` list. This is the right call for a monthly report — it stays
   well under the tool-result cap even for an hourly schedule over a month.
2. For any schedule with `failed > 0`, drill in with `get_replication_history`
   (`schedule_id`, `view=full`) to read the full `resultMessage`, `failedFiles`,
   or Hive `errors[]`/`tables[]` for the failing runs.
3. `list_replication_schedules` only if you need schedule metadata (cron,
   paused, nextRun) — it is a lightweight discovery call, not for history.

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
