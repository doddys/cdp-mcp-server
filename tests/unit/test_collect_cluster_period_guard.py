"""Unit test for collect_cluster's period-mismatch guard: resuming into an
--out directory whose _manifest.json already covers a *different* period
must refuse, not silently resume under the stale period's data."""
from unittest.mock import AsyncMock, MagicMock

import pytest

from cdp_mcp.collector.collect import collect_cluster


def _client() -> AsyncMock:
    client = AsyncMock()
    client.list_clusters.return_value = [{"name": "c1"}]
    client.get_cluster_security_info.return_value = {}
    client.list_parcels.return_value = []
    client.get_host_status.return_value = []
    client.list_services.return_value = []
    client.get_cluster_utilization.return_value = {}
    client.get_audit_events.return_value = {"items": []}
    client.get_alerts.return_value = {"items": []}
    client.list_cluster_commands.return_value = []
    return client


def _pool(client: AsyncMock) -> MagicMock:
    pool = MagicMock()
    pool.get_client_for_cluster.return_value = client
    pool.get_endpoints.return_value = MagicMock(
        yarn_rm_url=None, spark_hs_url=None, hdfs_nn_url=None, oozie_url=None
    )
    return pool


async def test_same_period_resumes_without_error(tmp_path):
    client = _client()
    pool = _pool(client)
    await collect_cluster(
        pool, "c1", "2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z", tmp_path, 2, None, True
    )
    # second call, identical period -- must not raise
    await collect_cluster(
        pool, "c1", "2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z", tmp_path, 2, None, True
    )


async def test_different_period_against_same_out_dir_raises(tmp_path):
    client = _client()
    pool = _pool(client)
    await collect_cluster(
        pool, "c1", "2026-08-01T00:00:00Z", "2026-09-01T00:00:00Z", tmp_path, 2, None, True
    )
    with pytest.raises(SystemExit, match="already has _manifest.json for period"):
        await collect_cluster(
            pool, "c1", "2026-08-01T00:00:00+07:00", "2026-09-01T00:00:00+07:00",
            tmp_path, 2, None, True,
        )
