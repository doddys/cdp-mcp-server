"""
config.py — Pydantic settings for cdp-mcp.
(Based on dvergari/cloudera-mcp-server, Apache 2.0)
"""
from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic_settings import BaseSettings

# ── ClouderaManagerSettings ───────────────────────────────────────────────────

@pydantic_dataclass
class ClouderaManagerSettings:
    host: str
    port: int = 7183
    username: str = "admin"
    password: str = ""
    use_tls: bool = True
    verify_ssl: bool = True
    api_version: str = "v51"
    timeout_seconds: int = 30
    downstream_timeout_seconds: int = 30
    disable_on_spnego: bool = True
    # Kerberos / SPNEGO for the four downstream clients (YARN/Spark/HDFS/Oozie).
    # When True, the clients attach an HTTPSPNEGOAuth. cm_client.py is unaffected
    # (Basic auth). Outbound proxying (e.g. socks5h://) is handled by httpx
    # trust_env, which honours ALL_PROXY / HTTPS_PROXY / HTTP_PROXY env vars
    # with no config here.
    kerberos: bool = False
    # In-process keytab acquisition (unattended production path). When
    # `kerberos_keytab` is set, build_spnego_auth acquires a TGT directly from
    # the keytab via gssapi.Credentials(store={'client_keytab': ...}) instead of
    # reading the default credentials cache — so no external `kinit`/renewer is
    # needed. `kerberos_principal` is required with it (the principal to
    # acquire, e.g. "mcp-svc@REALM"). When both are unset, the auth falls back
    # to the default ccache (a `kinit` TGT / keytab loaded into the ccache).
    kerberos_keytab: str | None = None
    kerberos_principal: str | None = None
    environment_name: str = "default"
    active: bool = True
    use_knox: bool = False
    lb_host: str | None = None
    lb_port: int = 443
    cluster_name: str | None = None
    endpoints_override: dict | None = None

    @property
    def effective_host(self) -> str:
        return self.lb_host if self.use_knox else self.host

    @property
    def effective_port(self) -> int:
        return self.lb_port if self.use_knox else self.port

    @property
    def effective_verify_ssl(self) -> bool:
        return True if self.use_knox else self.verify_ssl

    @property
    def base_url(self) -> str:
        if self.use_knox:
            return (
                f"https://{self.lb_host}:{self.lb_port}"
                f"/{self.cluster_name}/cdp-proxy-api/cm-api"
                f"/{self.api_version}"
            )
        scheme = "https" if self.use_tls else "http"
        return f"{scheme}://{self.host}:{self.port}/api/{self.api_version}"

    def __repr__(self) -> str:
        if self.use_knox:
            return (
                f"ClouderaManagerSettings(lb={self.lb_host!r}, "
                f"cluster={self.cluster_name!r}, env={self.environment_name!r})"
            )
        return (
            f"ClouderaManagerSettings(host={self.host!r}, "
            f"env={self.environment_name!r}, api={self.api_version}, tls={self.use_tls})"
        )


# ── ImpalaSettings ────────────────────────────────────────────────────────────

class ImpalaSettings(BaseSettings):
    model_config = {"env_prefix": "IMPALA_", "env_file": ".env", "extra": "ignore"}

    host: str = "localhost"
    port: int = 21050
    username: str = "impala"
    password: str = ""
    use_ssl: bool = False
    verify_ssl: bool = True
    timeout_seconds: int = 30
    database: str = "default"
    table: str = "cm_instances"
    auth_mechanism: str = "PLAIN"


# ── ServerSettings ────────────────────────────────────────────────────────────

class ServerSettings(BaseSettings):
    model_config = {
        "env_file": ".env",
        "env_prefix": "MCP_",
        "extra": "ignore",
        "populate_by_name": True,
    }

    server_name: str = "cdp-mcp"
    server_version: str = "0.1.0"
    log_level: str = "INFO"
    max_concurrent_requests: int = 10
    log_lines_per_role: int = 500

    # Registry backend — read from REGISTRY_BACKEND (no MCP_ prefix)
    registry_backend: Literal["iceberg", "file", "env"] = Field(
        "iceberg", alias="REGISTRY_BACKEND"
    )
    registry_file_path: str = Field(
        "cm_instances.yaml", alias="REGISTRY_FILE_PATH"
    )

    @model_validator(mode="before")
    @classmethod
    def _remap_registry_env(cls, values: dict) -> dict:
        """
        Allow REGISTRY_BACKEND / REGISTRY_FILE_PATH to be read from the
        environment even when pydantic-settings uses MCP_ prefix for everything
        else.  This validator fires before field assignment so the aliases work
        even without a MCP_ prefix.
        """
        import os
        if "REGISTRY_BACKEND" not in values and "REGISTRY_BACKEND" in os.environ:
            values["REGISTRY_BACKEND"] = os.environ["REGISTRY_BACKEND"]
        if "REGISTRY_FILE_PATH" not in values and "REGISTRY_FILE_PATH" in os.environ:
            values["REGISTRY_FILE_PATH"] = os.environ["REGISTRY_FILE_PATH"]
        return values


# ── build_registry factory ────────────────────────────────────────────────────

def build_registry(settings: ServerSettings):
    """Factory: creates the appropriate registry backend based on settings."""
    from cdp_mcp.registry.env_registry import EnvRegistry
    from cdp_mcp.registry.file_registry import FileRegistry
    from cdp_mcp.registry.iceberg import IcebergRegistry

    match settings.registry_backend:
        case "iceberg":
            return IcebergRegistry(ImpalaSettings())
        case "file":
            return FileRegistry(settings.registry_file_path)
        case "env":
            return EnvRegistry()
        case _:
            raise ValueError(f"Unknown REGISTRY_BACKEND: {settings.registry_backend!r}")
