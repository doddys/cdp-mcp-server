"""Regression test for a bug where two active CM instances sharing an
environment_name (including both defaulting to "default" when omitted)
caused the second connect() to silently overwrite the first in CMPool's
_clients dict -- dropping the first instance's clusters from
list_known_clusters() with no error at all. Reported as: a registry with
several active CM instances only ever surfaces the last one's clusters.
"""
from __future__ import annotations

import pytest

import cdp_mcp.cm_pool as cm_pool_module
from cdp_mcp.cm_pool import CMPool
from cdp_mcp.config import ClouderaManagerSettings, ServerSettings


class _FakeClient:
    def __init__(self, cfg, server_cfg):
        self.cfg = cfg
        self.connected = False
        self.closed = False

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.closed = True

    async def list_clusters(self) -> list[dict]:
        return [{"name": f"cluster-{self.cfg.environment_name}"}]

    async def list_services(self, cluster_name: str) -> list[dict]:
        return []


@pytest.fixture(autouse=True)
def _patch_client(monkeypatch):
    monkeypatch.setattr(cm_pool_module, "ClouderaManagerClient", _FakeClient)


def _settings(**overrides) -> ClouderaManagerSettings:
    base = {"host": "cm.example.com"}
    base.update(overrides)
    return ClouderaManagerSettings(**base)


@pytest.mark.asyncio
async def test_duplicate_environment_name_raises_before_connecting():
    instances = [
        _settings(host="cm-a.example.com", environment_name="prod"),
        _settings(host="cm-b.example.com", environment_name="prod"),
    ]
    pool = CMPool(instances, ServerSettings())

    with pytest.raises(ValueError, match="Duplicate environment_name"):
        await pool.start()

    # Upfront check means neither client should have been connected.
    assert pool.list_environments() == []


@pytest.mark.asyncio
async def test_both_omitting_environment_name_defaults_collide():
    """environment_name defaults to "default" -- two instances that both
    omit it collide exactly like an explicit duplicate."""
    instances = [
        _settings(host="cm-a.example.com"),
        _settings(host="cm-b.example.com"),
    ]
    pool = CMPool(instances, ServerSettings())

    with pytest.raises(ValueError, match="Duplicate environment_name"):
        await pool.start()


@pytest.mark.asyncio
async def test_distinct_environment_names_both_survive():
    instances = [
        _settings(host="cm-a.example.com", environment_name="drc"),
        _settings(host="cm-b.example.com", environment_name="prd"),
    ]
    pool = CMPool(instances, ServerSettings())
    await pool.start()

    assert sorted(pool.list_environments()) == ["drc", "prd"]
    assert sorted(pool.list_known_clusters()) == ["cluster-drc", "cluster-prd"]
