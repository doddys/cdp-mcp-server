# Monthly cluster reporting prompt — tool validation

**Date:** 2026-08-06
**Scope:** Static + live validation of every `cdp-mcp` tool call/parameter assumed by
a monthly cluster reporting prompt, against the codebase as of commit including the
P0–P2 pagination/chunking/scoping fixes (get_alerts/get_audit_events pagination,
get_cluster_utilization auto-chunking, list_available_metrics cluster scoping).
**Cluster used for live checks:** "Astra DaaS Data Lake DRC Cluster"
(`astra_daas_drc`), reached via a local SOCKS5 proxy.

---

## Part 1 — Static validation

| Tool | Exists (Y/N) | Params match report call (Y/N + notes) | Time-range capable (Y/N/N-A) | SPNEGO-gated (Y/N) |
|---|---|---|---|---|
| `get_cluster_security_info(cluster_name)` | Y | Y | N-A | N |
| `list_parcels(cluster_name)` | Y | Y | N-A | N |
| `get_host_status(cluster_name)` | Y | Y | N-A | N |
| `list_services(cluster_name)` | Y | Y | N-A | N |
| `list_roles(cluster_name, service_name)` | Y | Y | N-A | N |
| `get_cluster_utilization(cluster_name)` | Y | Y — `start_time`/`end_time` unused by report | **Y — improved**: auto-chunks ranges >29 days and merges results (`chunked`/`num_chunks`); previously a >30-day request hard-failed with a CM 400. Still defaults to last 1h if no range passed | N |
| `get_service_metrics(cluster_name, service_name, metric_names)` | Y | Y — `start_time`/`end_time` unused | Y, but unused → defaults to last 1h. **Response shape changed**: now `{"items": [...], "time_range_defaulted": bool, "effective_range": {...}}`, not a bare list | N |
| `list_available_metrics(...)` | Y | **Improved**: now accepts optional `cluster_name` to scope to the right CM instance in multi-CM registries (previously always queried "the first environment") | N-A | N |
| `get_audit_events(cluster_name, limit=50)` | Y | Y — `start_time`/`end_time`/filters unused | Y, and **now genuinely honored**: paginates via `resultOffset` until the range is covered or `max_scan` (10000) is hit. **Response shape changed**: `{"items": [...], "truncated": bool, "total_matched_in_range": int, "time_range_defaulted": bool, "effective_range": {...}}`. Still no `allowed`-filter param | N |
| `list_cluster_commands(cluster_name)` | Y | Y | N — only `limit`, no time bound | N |
| `list_replication_schedules(cluster_name)` | Y | **N — still missing required `service_name`** | N-A | N |
| `get_replication_history(...)` | Y | Requires `cluster_name`+`service_name`+`schedule_id` | N | N |
| `get_alerts(cluster_name, limit=50)` | Y | Y — `start_time`/`end_time` unused | Y, and **now genuinely honored** — same pagination/truncation upgrade as `get_audit_events` | N |
| `list_impala_queries(cluster_name)` | Y | **N — still missing required `service_name`** | Y, response also carries `time_range_defaulted`/`effective_range` | N |
| `get_namenode_status(cluster_name)` | Y | Y | N-A | **Y** |
| `get_yarn_queue(cluster_name)` | Y | Y | N-A | **Y** |
| `list_yarn_apps(cluster_name)` | Y | Y | N | **Y** |
| `list_spark_apps(cluster_name)` | Y | Y | N | **Y** (code-level; see live table) |
| `list_oozie_jobs(cluster_name)` | Y | Y | N | **Y** |

### Actionable edits — report prompt

1. `list_replication_schedules(cluster_name)` and `list_impala_queries(cluster_name)` fail immediately — both require `service_name`. Loop over real service names from `list_services()` rather than calling cluster-wide.
2. `get_alerts`, `get_audit_events`, `get_service_metrics`, `get_cluster_utilization`, `list_impala_queries` still silently default to the last hour if `start_time`/`end_time` are omitted. The tools now *correctly honor* a wide range once given one, but won't infer one — explicitly pass the report's period boundaries to all five.
3. Once a real range is passed, check `truncated` on `get_alerts`/`get_audit_events` — `true` means the `max_scan` safety cap (10,000) was hit before the full period was scanned, i.e. a genuine partial answer, not silently wrong. Raise `max_scan` if seen.

---

## Part 2 — Live verification

| Tool | Call succeeded | Response matched static prediction | Range param honored | Valid metric names confirmed |
|---|---|---|---|---|
| `get_cluster_security_info` | Y | Y | N-A | — |
| `list_parcels` | Y | Y | N-A | — |
| `get_host_status` | Y | Y | N-A | — |
| `list_services` | Y | Y | N-A | — |
| `list_roles` | Y | Y | N-A | — |
| `get_cluster_utilization` (default) | Y | Y | N-A | — |
| `get_cluster_utilization` (31-day range) | Y | **Y, confirmed fixed**: `chunked: true, num_chunks: 2`, no CM 400 (this exact call errored in a prior live session) | Y | — |
| `get_service_metrics` (`capacity_used`) | Y | Y — real HDFS NameNode timeseries returned | N-A (unranged) | **valid** |
| `get_service_metrics` (`capacity_used_gb`) | Y (tool ran; CM errored, correctly surfaced) | Y | N-A | **invalid** — `HTTP 500: Invalid metric 'capacity_used_gb'`, absent from schema |
| `list_available_metrics` (scoped) | Y | Y — 84 capacity-related metrics, same set as unscoped (single-CM deployment) | N-A | — |
| `get_audit_events` (default) | Y | Y | Y — 1h window | — |
| `get_audit_events` (30-day range) | Y | Y | **Y, confirmed**: `truncated: false`, `total_matched_in_range: 1605`, returned 1000 — fully covered the range, under `max_scan` | — |
| `list_cluster_commands` | Y | Y | N-A | — |
| `list_replication_schedules` (correct call, `service_name="hdfs"`) | Y | Y — 1 schedule, embedded `history` present | N-A | — |
| `get_alerts` (default) | Y | Y | Y — 1h window | — |
| `get_alerts` (30-day range) | Y | Y | **Y, confirmed**: `truncated: true`, `total_matched_in_range: 9675` — hit `max_scan` on this busy cluster, correctly flagged | — |
| `list_impala_queries` (called exactly as report lists it) | **N** | Y — same Pydantic "Field required: service_name" error as before | N-A | — |
| `list_impala_queries` (correct call, `service_name="impala"`) | Y | Y — clean `{"items": [], "time_range_defaulted": true, "effective_range": {...}}` | N-A (unranged) | — |
| `get_namenode_status` | spnego-expected | Y | N-A | — |
| `get_yarn_queue` | spnego-expected | Y | N-A | — |
| `list_yarn_apps` | spnego-expected | Y | N-A | — |
| `list_spark_apps` | Y | **N, drift**: code-level SPNEGO gap exists but this cluster's Spark HS doesn't enforce it — `200 OK` / `[]` | N-A | — |
| `list_oozie_jobs` | spnego-expected | Y | N-A | — |

**Latency:** everything 0.03–0.19s except `list_available_metrics` (2.03s) and, once a
real wide range is passed, `get_alerts` (9.6s) and `get_audit_events` (18.6s) —
the direct cost of the pagination fix (previously instant because silently
wrong/partial).

### Actionable edits — driven specifically by live behavior

1. The `get_cluster_utilization` 30-day hard-limit failure from a prior review no longer exists — remove any report-side chunking workaround; it's handled inside the tool now.
2. Budget real wait time once ranges are added to `get_alerts`/`get_audit_events`/`get_cluster_utilization`/`get_service_metrics`/`list_impala_queries` — a 31-day `get_alerts` call took 9.6s here, `get_audit_events` 18.6s. Across 6+ clusters run back-to-back this adds real minutes; not a bug, plan around it (generous timeout, off critical path).
3. Add an explicit check on `truncated` after any ranged `get_alerts`/`get_audit_events` call and surface it in the report itself (e.g. "alerts data covers only the most recent N of a busier period") rather than treating `truncated: true` as a complete result.
4. `capacity_used` is valid; `capacity_used_gb`/`capacity_total` are not and don't exist in the schema — use `capacity_used` and plain `capacity`.
