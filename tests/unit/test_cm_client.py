"""Unit tests for ClouderaManagerClient (cm_client.py)."""
from __future__ import annotations

import httpx
import pytest
import respx

from cdp_mcp.cm_client import ClouderaManagerClient
from cdp_mcp.config import ClouderaManagerSettings, ServerSettings

BASE = "http://cm.example.com:7183/api/v51"


@pytest.fixture
async def client():
    settings = ClouderaManagerSettings(
        host="cm.example.com",
        port=7183,
        username="admin",
        password="admin",
        use_tls=False,
        api_version="v51",
    )
    c = ClouderaManagerClient(settings, ServerSettings())
    await c.connect()
    yield c
    await c.close()


@respx.mock
@pytest.mark.asyncio
async def test_get_cluster_security_info(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/isTlsEnabled").mock(
        return_value=httpx.Response(200, json={"tlsEnabled": True})
    )
    respx.get(f"{BASE}/clusters/My%20Cluster/kerberosInfo").mock(
        return_value=httpx.Response(200, json={"kerberized": True})
    )
    result = await client.get_cluster_security_info("My Cluster")
    assert result["tls"]["tlsEnabled"] is True
    assert result["kerberos"]["kerberized"] is True


@respx.mock
@pytest.mark.asyncio
async def test_get_host_metrics(client):
    route = respx.post(f"{BASE}/timeseries").mock(
        return_value=httpx.Response(200, json={"items": [{"metric": "cpu_percent"}]})
    )
    result = await client.get_host_metrics("host1.example.com", ["cpu_percent"])
    assert result["items"] == [{"metric": "cpu_percent"}]
    assert result["time_range_defaulted"] is True
    sent_body = route.calls[0].request.content
    assert b"host1.example.com" in sent_body


@respx.mock
@pytest.mark.asyncio
async def test_list_available_metrics_no_filter(client):
    respx.get(f"{BASE}/timeseries/schema").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {"name": "cpu_percent", "displayName": "CPU Percent"},
                    {"name": "mem_rss", "displayName": "Resident Memory"},
                ]
            },
        )
    )
    result = await client.list_available_metrics()
    assert len(result) == 2


@respx.mock
@pytest.mark.asyncio
async def test_list_available_metrics_filtered(client):
    respx.get(f"{BASE}/timeseries/schema").mock(
        return_value=httpx.Response(
            200,
            json={
                "items": [
                    {"name": "cpu_percent", "displayName": "CPU Percent"},
                    {"name": "mem_rss", "displayName": "Resident Memory"},
                ]
            },
        )
    )
    result = await client.list_available_metrics(name_contains="cpu")
    assert len(result) == 1
    assert result[0]["name"] == "cpu_percent"


@respx.mock
@pytest.mark.asyncio
async def test_list_roles(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(
            200,
            json={"items": [{"name": "namenode1", "healthSummary": "GOOD"}]},
        )
    )
    result = await client.list_roles("My Cluster", "hdfs")
    assert result == [{"name": "namenode1", "healthSummary": "GOOD"}]


@respx.mock
@pytest.mark.asyncio
async def test_get_role(client):
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/namenode1"
    ).mock(
        return_value=httpx.Response(
            200, json={"name": "namenode1", "roleState": "STARTED"}
        )
    )
    result = await client.get_role("My Cluster", "hdfs", "namenode1")
    assert result["roleState"] == "STARTED"


@respx.mock
@pytest.mark.asyncio
async def test_list_cluster_commands_applies_client_side_limit(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/commands").mock(
        return_value=httpx.Response(
            200,
            json={"items": [{"id": i, "name": "Start"} for i in range(5)]},
        )
    )
    result = await client.list_cluster_commands("My Cluster", limit=2)
    assert len(result) == 2
    assert result[0]["id"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_get_impala_queries(client):
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/impala/impalaQueries"
    ).mock(
        return_value=httpx.Response(
            200, json={"queries": [{"queryId": "abc123", "user": "root"}]}
        )
    )
    result = await client.get_impala_queries(
        "My Cluster", "impala", filter_str="user=root"
    )
    assert result["items"] == [{"queryId": "abc123", "user": "root"}]
    assert result["time_range_defaulted"] is True
    sent_params = route.calls[0].request.url.params
    assert sent_params["filter"] == "user=root"


@respx.mock
@pytest.mark.asyncio
async def test_get_cluster_utilization(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/utilization").mock(
        return_value=httpx.Response(200, json={"clusterUtilization": []})
    )
    result = await client.get_cluster_utilization("My Cluster")
    assert result["clusterUtilization"] == []
    assert result["chunked"] is False
    assert result["num_chunks"] == 1
    assert result["time_range_defaulted"] is True


@respx.mock
@pytest.mark.asyncio
async def test_get_cluster_utilization_chunks_over_29_days(client):
    route = respx.get(f"{BASE}/clusters/My%20Cluster/utilization")
    route.side_effect = [
        httpx.Response(
            200,
            json={
                "totalCpuCores": 100,
                "avgCpuUtilization": 10.0,
                "maxCpuUtilization": 50.0,
                "maxCpuUtilizationTimestampMs": 111,
            },
        ),
        httpx.Response(
            200,
            json={
                "totalCpuCores": 120,
                "avgCpuUtilization": 20.0,
                "maxCpuUtilization": 90.0,
                "maxCpuUtilizationTimestampMs": 222,
            },
        ),
    ]
    result = await client.get_cluster_utilization(
        "My Cluster", start_time="2026-01-01T00:00:00Z", end_time="2026-02-05T00:00:00Z"
    )
    assert route.call_count == 2
    assert result["chunked"] is True
    assert result["num_chunks"] == 2
    # max* takes the true max across chunks, carrying its own timestamp
    assert result["maxCpuUtilization"] == 90.0
    assert result["maxCpuUtilizationTimestampMs"] == 222
    # avg* is duration-weighted, not a naive mean of the two chunk averages
    assert 10.0 < result["avgCpuUtilization"] < 20.0
    # non-avg/max fields fall back to the most recent chunk
    assert result["totalCpuCores"] == 120
    assert result["time_range_defaulted"] is False


@respx.mock
@pytest.mark.asyncio
async def test_list_replication_schedules(client):
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications"
    ).mock(
        return_value=httpx.Response(200, json={"items": [{"id": 1}, {"id": 2}]})
    )
    result = await client.list_replication_schedules("My Cluster", "hdfs")
    # Defaults: view=summary, maxCommands=1 (capped to keep response small).
    assert route.calls.last.request.url.params["view"] == "summary"
    assert route.calls.last.request.url.params["maxCommands"] == "1"
    assert "maxSchedules" not in route.calls.last.request.url.params
    assert result["items"] == [{"id": 1}, {"id": 2}]
    assert result["count"] == 2
    assert result["truncated"] is False
    assert result["applied_limits"] == {
        "maxSchedules": None,
        "maxCommands": 1,
        "view": "summary",
    }


@respx.mock
@pytest.mark.asyncio
async def test_list_replication_schedules_truncated(client):
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications"
    ).mock(
        return_value=httpx.Response(
            200, json={"items": [{"id": i} for i in range(5)]}
        )
    )
    result = await client.list_replication_schedules(
        "My Cluster", "hdfs", max_schedules=5
    )
    assert route.calls.last.request.url.params["maxSchedules"] == "5"
    # Returned exactly the cap -> more may exist.
    assert result["count"] == 5
    assert result["truncated"] is True


@respx.mock
@pytest.mark.asyncio
async def test_get_replication_history(client):
    # newest-first page: two runs in-window, one older (out of window),
    # one without startTime (filtered out).
    in1 = {"id": 100, "success": True,
           "startTime": "2026-08-06T11:30:00.000Z", "endTime": "2026-08-06T11:31:00.000Z"}
    in2 = {"id": 101, "success": False,
           "startTime": "2026-08-06T11:10:00.000Z", "endTime": "2026-08-06T11:11:00.000Z"}
    old = {"id": 99, "success": True, "startTime": "2026-08-06T09:00:00.000Z"}
    no_time = {"id": 98, "success": True}
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications/1/history"
    ).mock(
        return_value=httpx.Response(200, json={"items": [in1, in2, old, no_time]})
    )
    result = await client.get_replication_history(
        "My Cluster", "hdfs", 1,
        start_time="2026-08-06T11:00:00.000Z",
        end_time="2026-08-06T12:00:00.000Z",
    )
    assert route.calls.last.request.url.params["view"] == "summary"
    assert route.calls.last.request.url.params["offset"] == "0"
    assert result["items"] == [in1, in2]  # newest-first, in-window only
    assert result["count"] == 2
    assert result["total_in_range"] == 2
    assert result["truncated"] is False
    assert result["effective_range"] == {
        "start": "2026-08-06T11:00:00.000Z",
        "end": "2026-08-06T12:00:00.000Z",
    }


@respx.mock
@pytest.mark.asyncio
async def test_get_replication_history_limit_and_truncate(client):
    runs = [
        {"id": i, "success": True,
         "startTime": f"2026-08-06T11:{59 - i:02d}:00.000Z"}
        for i in range(10)
    ]
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications/1/history"
    ).mock(return_value=httpx.Response(200, json={"items": runs}))
    result = await client.get_replication_history(
        "My Cluster", "hdfs", 1,
        start_time="2026-08-06T11:00:00.000Z",
        end_time="2026-08-06T12:00:00.000Z",
        limit=3,
    )
    assert result["count"] == 3            # capped to limit
    assert result["total_in_range"] == 10  # but full match count reported
    assert result["truncated"] is False
    # newest-first: ids 0 (11:59) .. 9 (11:50)
    assert [r["id"] for r in result["items"]] == [0, 1, 2]


@respx.mock
@pytest.mark.asyncio
async def test_get_replication_metrics_single_schedule(client):
    # HDFS schedule: 3 in-window runs (one failed), one older out-of-window.
    runs = [
        {"id": 1, "success": True,
         "startTime": "2026-08-06T11:30:00.000Z", "endTime": "2026-08-06T11:31:00.000Z",
         "hdfsResult": {"numBytesCopied": 1000, "numFilesCopied": 10,
                        "numFilesCopyFailed": 0, "numFilesSkipped": 2}},
        {"id": 2, "success": False,
         "startTime": "2026-08-06T11:20:00.000Z", "endTime": "2026-08-06T11:20:30.000Z",
         "hdfsResult": {"numBytesCopied": 500, "numFilesCopied": 5,
                        "numFilesCopyFailed": 3},
         "resultMessage": "copy failed"},
        {"id": 3, "success": True,
         "startTime": "2026-08-06T11:10:00.000Z", "endTime": "2026-08-06T11:10:30.000Z",
         "hdfsResult": {"numBytesCopied": 2000, "numFilesCopied": 20}},
        {"id": 0, "success": True, "startTime": "2026-08-06T09:00:00.000Z",
         "hdfsResult": {"numBytesCopied": 99999}},  # out of window
    ]
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/replications/42/history"
    ).mock(return_value=httpx.Response(200, json={"items": runs}))
    result = await client.get_replication_metrics(
        "My Cluster", "hdfs",
        start_time="2026-08-06T11:00:00.000Z",
        end_time="2026-08-06T12:00:00.000Z",
        schedule_id=42,
    )
    assert result["schedule_count"] == 1
    sch = result["schedules"][0]
    assert sch["schedule_id"] == 42
    assert sch["type"] == "HDFS"  # schedule had no hdfsArguments -> UNKNOWN actually
    assert sch["runs"] == 3            # out-of-window run excluded
    assert sch["succeeded"] == 2
    assert sch["failed"] == 1
    assert sch["total_bytes_copied"] == 3500   # 1000 + 500 + 2000
    assert sch["total_files_copied"] == 35     # 10 + 5 + 20
    assert sch["total_files_failed"] == 3
    assert sch["total_files_skipped"] == 2
    assert sch["avg_duration_seconds"] == 40.0  # (60+30+30)/3
    assert sch["first_run"] == "2026-08-06T11:10:00.000Z"
    assert sch["last_run"] == "2026-08-06T11:30:00.000Z"
    assert sch["truncated"] is False
    assert len(sch["failures"]) == 1
    assert sch["failures"][0]["id"] == 2
    assert sch["failures"][0]["result_message"] == "copy failed"
    assert result["scanned_runs"] == 3


@respx.mock
@pytest.mark.asyncio
async def test_get_replication_metrics_all_schedules(client):
    # Discovery returns 2 HIVE schedules; each paginated separately.
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hive/replications"
    ).mock(return_value=httpx.Response(
        200, json={"items": [
            {"id": 1, "displayName": "hive-sync", "hiveArguments": {}},
            {"id": 2, "displayName": "hive-sync-2", "hiveArguments": {}},
        ]}
    ))
    def history_handler(request):
        sid = request.url.path.split("/")[-2]
        # schedule 1: one in-window run with table metrics
        if sid == "1":
            items = [{"id": 10, "success": True,
                      "startTime": "2026-08-06T11:30:00.000Z",
                      "endTime": "2026-08-06T11:31:00.000Z",
                      "hiveResult": {"tableProcessed": 736, "errorCount": 0,
                                     "dataReplicationResult": {"numBytesCopied": 500}}}]
        else:
            items = [{"id": 20, "success": False,
                      "startTime": "2026-08-06T11:30:00.000Z",
                      "endTime": "2026-08-06T11:31:00.000Z",
                      "hiveResult": {"tableProcessed": 100, "errorCount": 4,
                                     "dataReplicationResult": {"numBytesCopied": 0}},
                      "resultMessage": "boom"}]
        return httpx.Response(200, json={"items": items})
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hive/replications/1/history"
    ).mock(side_effect=history_handler)
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hive/replications/2/history"
    ).mock(side_effect=history_handler)
    result = await client.get_replication_metrics(
        "My Cluster", "hive",
        start_time="2026-08-06T11:00:00.000Z",
        end_time="2026-08-06T12:00:00.000Z",
    )
    assert result["schedule_count"] == 2
    assert result["schedule_truncated"] is False
    by_id = {s["schedule_id"]: s for s in result["schedules"]}
    assert by_id[1]["type"] == "HIVE"
    assert by_id[1]["total_tables_processed"] == 736
    assert by_id[1]["total_errors"] == 0
    assert by_id[1]["total_bytes_copied"] == 500  # via dataReplicationResult
    assert by_id[1]["succeeded"] == 1 and by_id[1]["failed"] == 0
    assert by_id[2]["failed"] == 1
    assert by_id[2]["total_errors"] == 4
    assert by_id[2]["failures"][0]["result_message"] == "boom"


@respx.mock
@pytest.mark.asyncio
async def test_list_parcels(client):
    respx.get(f"{BASE}/clusters/My%20Cluster/parcels").mock(
        return_value=httpx.Response(
            200, json={"items": [{"product": "CDH", "stage": "ACTIVATED"}]}
        )
    )
    result = await client.list_parcels("My Cluster")
    assert result == [{"product": "CDH", "stage": "ACTIVATED"}]


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_skips_gateway_and_caps_roles(client):
    roles = (
        [{"name": f"dn{i}", "type": "DATANODE"} for i in range(15)]
        + [{"name": "gw1", "type": "GATEWAY"}]
    )
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    for i in range(10):
        respx.get(
            f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/dn{i}/logs/full"
        ).mock(return_value=httpx.Response(200, text="log line"))

    result = await client.get_service_logs("My Cluster", "hdfs", max_lines=10)

    assert "gw1" not in result
    fetched = [k for k in result if k != "_truncated"]
    assert len(fetched) == 10
    assert "_truncated" in result


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_role_name_bypasses_cap(client):
    roles = [{"name": f"dn{i}", "type": "DATANODE"} for i in range(15)]
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/dn5/logs/full"
    ).mock(return_value=httpx.Response(200, text="log line"))

    result = await client.get_service_logs(
        "My Cluster", "hdfs", max_lines=10, role_name="dn5"
    )

    assert list(result.keys()) == ["dn5"]


@respx.mock
@pytest.mark.asyncio
async def test_get_service_logs_truncates_client_side(client):
    """CM's /logs/full ignores any query params and always returns the
    complete file -- confirmed empirically against a live cluster (28,040
    lines returned regardless of a "lines" param). max_lines must be
    enforced by slicing the response, not by trusting the server."""
    roles = [{"name": "nn1", "type": "NAMENODE"}]
    respx.get(f"{BASE}/clusters/My%20Cluster/services/hdfs/roles").mock(
        return_value=httpx.Response(200, json={"items": roles})
    )
    full_log = "\n".join(f"line {i}" for i in range(1000))
    route = respx.get(
        f"{BASE}/clusters/My%20Cluster/services/hdfs/roles/nn1/logs/full"
    ).mock(return_value=httpx.Response(200, text=full_log))

    result = await client.get_service_logs("My Cluster", "hdfs", max_lines=5)

    assert len(result["nn1"]) == 5
    assert result["nn1"] == [f"line {i}" for i in range(995, 1000)]
    # no "lines" (or any) query param sent -- it doesn't do anything server-side
    assert route.calls[0].request.url.params == httpx.QueryParams()


def _event(i: int, cluster: str = "My Cluster") -> dict:
    return {
        "timeOccurred": f"2026-01-01T00:{i:02d}:00Z",
        "attributes": [{"name": "CLUSTER_DISPLAY_NAME", "values": [cluster]}],
    }


@respx.mock
@pytest.mark.asyncio
async def test_get_alerts_paginates_until_partial_page(client):
    page1 = [_event(i) for i in range(100)]
    page2 = [_event(i) for i in range(30)]
    route = respx.get(f"{BASE}/events")
    route.side_effect = [
        httpx.Response(200, json={"items": page1}),
        httpx.Response(200, json={"items": page2}),
    ]
    result = await client.get_alerts("My Cluster", start_time="2026-01-01T00:00:00Z")
    assert route.call_count == 2
    assert result["total_matched_in_range"] == 130
    assert result["truncated"] is False
    assert result["time_range_defaulted"] is False
    assert len(result["items"]) == 50  # default limit


@respx.mock
@pytest.mark.asyncio
async def test_get_alerts_truncated_at_max_scan(client):
    page1 = [_event(i) for i in range(100)]
    route = respx.get(f"{BASE}/events").mock(
        return_value=httpx.Response(200, json={"items": page1})
    )
    result = await client.get_alerts("My Cluster", limit=10, max_scan=100)
    assert route.call_count == 1  # stopped at max_scan, never fetched a 2nd page
    assert result["truncated"] is True
    assert result["total_matched_in_range"] == 100
    assert len(result["items"]) == 10


@respx.mock
@pytest.mark.asyncio
async def test_get_alerts_time_range_defaulted_flag(client):
    respx.get(f"{BASE}/events").mock(
        return_value=httpx.Response(200, json={"items": []})
    )
    result = await client.get_alerts("My Cluster")
    assert result["time_range_defaulted"] is True


def _audit(i: int) -> dict:
    return {"timestamp": f"2026-01-01T00:{i:02d}:00Z", "username": "u"}


@respx.mock
@pytest.mark.asyncio
async def test_get_audit_events_paginates_until_partial_page(client):
    page1 = [_audit(i) for i in range(100)]
    page2 = [_audit(i) for i in range(5)]
    route = respx.get(f"{BASE}/audits")
    route.side_effect = [
        httpx.Response(200, json={"items": page1}),
        httpx.Response(200, json={"items": page2}),
    ]
    result = await client.get_audit_events(
        "My Cluster", start_time="2026-01-01T00:00:00Z", limit=1000
    )
    assert route.call_count == 2
    assert result["total_matched_in_range"] == 105
    assert result["truncated"] is False
    assert len(result["items"]) == 105
