"""Unit tests for FileRegistry."""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from cdp_mcp.registry.file_registry import FileRegistry, _interpolate, _interpolate_dict


# ── Helper ────────────────────────────────────────────────────────────────────

def _make_yaml(instances: list[dict]) -> str:
    return yaml.dump({"instances": instances})


def _write_temp_yaml(instances: list[dict]) -> tempfile.NamedTemporaryFile:
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    yaml.safe_dump({"instances": instances}, f)
    f.flush()
    return f


# ── _interpolate ──────────────────────────────────────────────────────────────

def test_interpolate_replaces_env_var(monkeypatch):
    monkeypatch.setenv("MY_SECRET", "hunter2")
    assert _interpolate("${MY_SECRET}") == "hunter2"


def test_interpolate_missing_var_returns_empty(monkeypatch):
    monkeypatch.delenv("UNDEFINED_VAR", raising=False)
    assert _interpolate("${UNDEFINED_VAR}") == ""


def test_interpolate_no_vars():
    assert _interpolate("plain-value") == "plain-value"


def test_interpolate_dict_nested():
    os.environ["PASS"] = "secret"
    result = _interpolate_dict({"password": "${PASS}", "host": "cm.example.com"})
    assert result["password"] == "secret"
    assert result["host"] == "cm.example.com"


# ── FileRegistry ──────────────────────────────────────────────────────────────

@pytest.fixture()
def simple_yaml(tmp_path):
    """A YAML file with one active instance."""
    data = {
        "instances": [
            {
                "host": "cm.example.com",
                "port": 7183,
                "username": "admin",
                "password": "secret",
                "environment_name": "dev",
                "use_tls": False,
                "verify_ssl": False,
                "api_version": "v51",
            }
        ]
    }
    p = tmp_path / "cm_instances.yaml"
    p.write_text(yaml.safe_dump(data))
    return str(p)


def test_load_returns_one_instance(simple_yaml):
    reg = FileRegistry(simple_yaml)
    instances = reg.load()
    assert len(instances) == 1
    assert instances[0].host == "cm.example.com"
    assert instances[0].port == 7183
    assert instances[0].environment_name == "dev"


def test_load_reads_keytab_config(tmp_path):
    """kerberos_keytab/kerberos_principal yaml keys map onto the settings fields."""
    data = {
        "instances": [
            {
                "host": "cm.example.com",
                "port": 7183,
                "username": "admin",
                "password": "secret",
                "kerberos": True,
                "kerberos_keytab": "/etc/cdp-mcp/mcp.keytab",
                "kerberos_principal": "mcp-svc@REALM",
            }
        ]
    }
    p = tmp_path / "cm_instances.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    inst = reg.load()[0]
    assert inst.kerberos is True
    assert inst.kerberos_keytab == "/etc/cdp-mcp/mcp.keytab"
    assert inst.kerberos_principal == "mcp-svc@REALM"


def test_get_all_after_start(simple_yaml):
    reg = FileRegistry(simple_yaml)
    reg.start()
    all_inst = reg.get_all()
    assert len(all_inst) == 1


def test_inactive_instance_excluded(tmp_path):
    data = {
        "instances": [
            {"host": "active.cm", "port": 7183, "username": "a", "password": "p", "active": True},
            {"host": "inactive.cm", "port": 7183, "username": "a", "password": "p", "active": False},
        ]
    }
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    instances = reg.load()
    assert len(instances) == 1
    assert instances[0].host == "active.cm"


def test_env_var_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv("CM_PASS", "env_password")
    data = {
        "instances": [
            {"host": "cm.example.com", "port": 7183, "username": "admin", "password": "${CM_PASS}"}
        ]
    }
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    instances = reg.load()
    assert instances[0].password == "env_password"


def test_list_raw_excludes_password(simple_yaml):
    reg = FileRegistry(simple_yaml)
    reg.start()
    raw = reg.list_raw()
    assert len(raw) == 1
    assert "password" not in raw[0]
    assert raw[0]["host"] == "cm.example.com"


def test_get_stats(simple_yaml):
    reg = FileRegistry(simple_yaml)
    reg.start()
    stats = reg.get_stats()
    assert stats["backend"] == "file"
    assert stats["total"] == 1
    assert stats["active"] == 1
    assert stats["inactive"] == 0
    assert "dev" in stats["by_environment"]


def test_register_new_host(tmp_path):
    data = {"instances": [{"host": "existing.cm", "port": 7183, "username": "a", "password": "p"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    reg.register(host="new.cm", port=7183, username="admin", password="pass")

    instances = reg.get_all()
    hosts = [i.host for i in instances]
    assert "new.cm" in hosts
    assert "existing.cm" in hosts


def test_register_duplicate_raises(tmp_path):
    data = {"instances": [{"host": "cm.example.com", "port": 7183, "username": "a", "password": "p"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    with pytest.raises(ValueError, match="already registered"):
        reg.register(host="cm.example.com", port=7183, username="a", password="p")


def test_deactivate(tmp_path):
    data = {"instances": [{"host": "cm.example.com", "port": 7183, "username": "a", "password": "p"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    reg.deactivate("cm.example.com")
    assert reg.get_all() == []

    # Should still appear in list_raw with include_inactive
    raw = reg.list_raw(include_inactive=True)
    assert len(raw) == 1
    assert raw[0]["host"] == "cm.example.com"


def test_deactivate_unknown_host_raises(tmp_path):
    data = {"instances": [{"host": "cm.example.com", "port": 7183, "username": "a", "password": "p"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    with pytest.raises(ValueError, match="not found"):
        reg.deactivate("unknown.host")


def test_update_field(tmp_path):
    data = {"instances": [{"host": "cm.example.com", "port": 7183, "username": "admin", "password": "old"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    reg.update_field("cm.example.com", "password", "new_pass")
    instances = reg.get_all()
    assert instances[0].password == "new_pass"


def test_update_field_readonly_raises(tmp_path):
    data = {"instances": [{"host": "cm.example.com", "port": 7183, "username": "a", "password": "p"}]}
    p = tmp_path / "cm.yaml"
    p.write_text(yaml.safe_dump(data))
    reg = FileRegistry(str(p))
    reg.start()

    with pytest.raises(ValueError, match="read-only"):
        reg.update_field("cm.example.com", "host", "newhost")


def test_file_not_found_returns_empty():
    reg = FileRegistry("/nonexistent/path/cm.yaml")
    instances = reg.load()
    assert instances == []


def test_stop_is_noop(simple_yaml):
    reg = FileRegistry(simple_yaml)
    reg.start()
    reg.stop()  # Should not raise
